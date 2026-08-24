use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

use bytes::Bytes;
use rdkafka::client::{DefaultClientContext, OAuthToken};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer as RdkafkaConsumer, ConsumerContext, Rebalance,
    StreamConsumer,
};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Header, Headers, Message, OwnedHeaders};
use rdkafka::{ClientConfig, ClientContext, Offset, Statistics, TopicPartitionList};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::consumer::validate_message_size;
use crate::consumer_supervisor::{SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::Result;
use crate::handler::{BatchMessageHandler, MessageHandler};
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::routing::{
    PoisonedKeys, RetryDecision, decide_retry, handler_timeout_outcome, hold_index,
};
use crate::topic::{NotSequenced, SequencedTopic, Topic};
use crate::topology::QueueTopology;
use crate::{DEFAULT_HANDLER_TIMEOUT, DEFAULT_MAX_MESSAGE_SIZE, HoldQueue, Kafka, ShoveError};

#[cfg(feature = "kafka-msk-iam")]
use super::msk_iam::MskIamContext;

#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::decode::{RegistryDecode, registry_decode};
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::default_subject;
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::{SchemaEnforcement, SchemaRegistry, WireFormat};

use super::client::KafkaClient;
use super::constants::{
    DEATH_COUNT_HEADER, DEATH_REASON_HEADER, FETCH_MIN_BYTES, FETCH_WAIT_MAX_MS,
    MAX_POLL_INTERVAL_MS, MAX_PUBLISH_ATTEMPTS, MESSAGE_ID_HEADER, ORIGINAL_QUEUE_HEADER,
    RETRY_COUNT_HEADER, SESSION_TIMEOUT_MS,
};
use super::consumer_group::KafkaAutoOffsetReset;

// ---------------------------------------------------------------------------
// Offset tracking for concurrent consumption
// ---------------------------------------------------------------------------

/// Consecutive quiet drains (see `PartitionTracker::drain_committable`) after
/// which a re-offered commit is assumed to have landed, resolving the
/// partition's dirty streak.
///
/// Must be >= 2: at 1 this degenerates into "clear on the first quiet tick",
/// which resets a genuinely wedged partition's streak on every iteration and
/// stops it ever reaching `COMMIT_FENCE_TIMEOUT`. At 2, the tolerated window
/// is 2 × `HOUSEKEEPING_INTERVAL` — comfortably longer than a commit round
/// trip, and far inside the fence threshold, so a wedged partition still
/// trips it on schedule.
const QUIET_DRAINS_TO_RESOLVE: u32 = 2;

struct PartitionTracker {
    /// Next offset to commit (exclusive — Kafka convention).
    next_to_commit: i64,
    /// Offsets that have been processed but not yet committable
    /// (because earlier offsets are still in-flight).
    completed: BTreeSet<i64>,
    /// Set when an async commit that included this partition was rejected
    /// (e.g. during a rebalance). Makes the next `drain_committable` re-offer
    /// the current `next_to_commit` even without new completions, so the
    /// failed commit is retried instead of silently lost.
    dirty: bool,
    /// When a *rejected commit* first raised `dirty` in the current
    /// unresolved streak. Only `mark_dirty` sets it; a rebalance-induced
    /// re-offer goes through `mark_dirty_resolved`, which clears it.
    ///
    /// `commit_callback` only fires on failure (see its doc comment), so
    /// there is no positive ack that a re-offered commit landed. Any
    /// rebalance clears the streak instead — for a partition that moves, by
    /// dropping this tracker (`OffsetTracker::remove`); for one this member
    /// keeps, via `mark_dirty_resolved`. So a `dirty_since` that keeps aging
    /// across repeated re-offers with no intervening rebalance is exactly the
    /// "commits rejected, group never recovers" signature:
    /// `PartitionTracker::stuck_for`.
    dirty_since: Option<Instant>,
    /// Consecutive `drain_committable` calls entered with `dirty == false`.
    /// See `QUIET_DRAINS_TO_RESOLVE` — this is what lets a *recovered*
    /// partition clear `dirty_since`, which a successful commit cannot do on
    /// its own because it is silent.
    quiet_drains: u32,
    /// Terminal discards awaiting the commit that retires them, keyed by the
    /// offset each depends on.
    ///
    /// `messages_discarded_total` promises every increment is a message that
    /// no longer exists, so a discard cannot be counted while its offset is
    /// merely queued for commit. Entries leave this map in exactly two ways:
    /// covered by a landed commit (`confirm`), or dropped with the tracker
    /// when the partition is revoked (`OffsetTracker::remove`), which means
    /// the message is redelivered to whoever takes the partition over — an
    /// undercount, the safe direction.
    pending_discards: BTreeMap<i64, TerminalDiscard>,
}

impl PartitionTracker {
    fn new(first_offset: i64) -> Self {
        Self {
            next_to_commit: first_offset,
            completed: BTreeSet::new(),
            dirty: false,
            dirty_since: None,
            quiet_drains: 0,
            pending_discards: BTreeMap::new(),
        }
    }

    fn mark_complete(&mut self, offset: i64, discard: Option<TerminalDiscard>) {
        // Completions below the seed are stale: after a partition is removed
        // on rebalance and re-seeded by the next delivery, completions of
        // messages in flight from the previous assignment epoch would
        // otherwise pile up in `completed` forever (never contiguous with
        // `next_to_commit`).
        if offset < self.next_to_commit {
            // This epoch will never commit that offset, so any retirement
            // riding on it is not ours to claim.
            if let Some(discard) = discard {
                discard.survived();
            }
            return;
        }
        if let Some(discard) = discard {
            self.pending_discards.insert(offset, discard);
        }
        self.completed.insert(offset);
    }

    /// Flags this partition dirty (see the `dirty` field) because a commit
    /// was *rejected*, recording `now` as the start of the unresolved streak
    /// if one isn't already in progress.
    fn mark_dirty(&mut self, now: Instant) {
        self.dirty = true;
        self.quiet_drains = 0;
        self.dirty_since.get_or_insert(now);
    }

    /// Flags this partition dirty because a rebalance may have dropped an
    /// in-flight commit, and clears any unresolved streak.
    ///
    /// A rebalance is the *resolving* event whose absence defines the fenced
    /// signature (see `OffsetTracker::fenced`), so it must never start or
    /// extend a streak. `OffsetTracker::remove` already clears the streak for
    /// partitions that move, by dropping the tracker — but a partition this
    /// member *keeps* across the rebalance holds on to its tracker, so
    /// clearing has to happen here for it to happen at all.
    fn mark_dirty_resolved(&mut self) {
        self.dirty = true;
        self.quiet_drains = 0;
        self.dirty_since = None;
    }

    /// Returns the offset to commit if progress was made (or a failed commit
    /// needs to be retried — see `dirty`), or None.
    ///
    /// Deliberately does **not** clear `dirty_since` on the *first* drain
    /// that finds nothing to retry: `commit_callback` only fires on failure
    /// (see its doc comment), so an async commit's round trip can easily
    /// outlast one loop iteration — clearing on the first quiet tick would
    /// let a genuinely wedged partition's streak get reset just before each
    /// new failure lands, so it would never reach the fenced threshold.
    /// `QUIET_DRAINS_TO_RESOLVE` consecutive quiet drains, on the other
    /// hand, are long enough that a re-offered commit must have landed, and
    /// clearing there is what stops a *recovered* partition from aging into
    /// a false fence (a successful commit is silent, so it cannot clear the
    /// streak itself). A resolving rebalance also clears it, by dropping and
    /// recreating this tracker — see `OffsetTracker::remove`.
    /// The returned discards are the ones this commit position retires: their
    /// offsets are strictly below the (exclusive) commit offset. They are
    /// handed to the caller unsettled, because only the commit's result says
    /// whether the retirement actually happened.
    fn drain_committable(&mut self) -> Option<(i64, Vec<TerminalDiscard>)> {
        let mut next = self.next_to_commit;
        while self.completed.remove(&next) {
            next += 1;
        }
        let progressed = next > self.next_to_commit;
        let retry = self.dirty;
        self.dirty = false;
        if retry {
            self.quiet_drains = 0;
        } else {
            self.quiet_drains = self.quiet_drains.saturating_add(1);
            if self.quiet_drains >= QUIET_DRAINS_TO_RESOLVE {
                self.dirty_since = None;
            }
        }
        if progressed {
            self.next_to_commit = next;
        }
        if progressed || retry {
            // `next` is exclusive, so everything strictly below it is covered.
            let remainder = self.pending_discards.split_off(&next);
            let covered = std::mem::replace(&mut self.pending_discards, remainder);
            Some((next, covered.into_values().collect()))
        } else {
            None
        }
    }

    /// How long this partition has been continuously dirty, or `None` if
    /// it's currently clean.
    fn stuck_for(&self, now: Instant) -> Option<Duration> {
        self.dirty_since
            .map(|since| now.saturating_duration_since(since))
    }
}

struct OffsetTracker {
    topic: String,
    partitions: HashMap<i32, PartitionTracker>,
}

impl OffsetTracker {
    fn new(topic: String) -> Self {
        Self {
            topic,
            partitions: HashMap::new(),
        }
    }

    fn track_received(&mut self, partition: i32, offset: i64) {
        self.partitions
            .entry(partition)
            .or_insert_with(|| PartitionTracker::new(offset));
    }

    fn mark_complete(&mut self, completion: Completion) {
        let Completion {
            partition,
            offset,
            discard,
        } = completion;
        match self.partitions.get_mut(&partition) {
            Some(tracker) => tracker.mark_complete(offset, discard),
            // The partition was revoked while this delivery was in flight, so
            // this member will never commit the offset and whoever owns the
            // partition now will redeliver the message.
            None => {
                if let Some(discard) = discard {
                    discard.survived();
                }
            }
        }
    }

    /// Drops per-partition state when the partition's ownership changes.
    ///
    /// Called for BOTH revoke and assign events: on revoke so this member
    /// stops committing offsets for a partition it no longer owns; on assign
    /// so a reassigned partition re-seeds `next_to_commit` from the first
    /// offset actually delivered under the new assignment. The broker's
    /// committed offset decides where delivery resumes, so seeding from
    /// delivery is correct — while a stale seed (left from before the
    /// partition moved away and another member committed on it) would make
    /// `drain_committable` wait for a contiguous run that never arrives,
    /// stalling commits on the partition forever.
    fn remove(&mut self, partition: i32) {
        self.partitions.remove(&partition);
    }

    /// Flags a partition to re-offer its current commit position on the next
    /// drain, because an async commit that covered it was rejected. No-ops if
    /// the partition's tracker is gone (revoked meanwhile) — a commit must
    /// never be retried for a partition this member no longer owns.
    fn mark_dirty(&mut self, partition: i32, now: Instant) {
        if let Some(tracker) = self.partitions.get_mut(&partition) {
            tracker.mark_dirty(now);
        }
    }

    /// Applies all queued rebalance/commit-failure events from librdkafka's
    /// callbacks. Cheap when the channel is empty (a single failed
    /// `try_recv`), so callers run it every loop iteration.
    fn apply_rebalance_events(&mut self, rx: &std_mpsc::Receiver<RebalanceEvent>, now: Instant) {
        while let Ok(event) = rx.try_recv() {
            match event {
                RebalanceEvent::Assign(partitions) | RebalanceEvent::Revoke(partitions) => {
                    for partition in partitions {
                        self.remove(partition);
                    }
                    // Async commits in flight while the group rebalances can
                    // be dropped by librdkafka without surfacing an error
                    // (observed against a real broker: commits submitted
                    // between the revoke and assign phases of a cooperative
                    // rebalance vanish — no commit_callback ever fires).
                    // Re-offer every retained partition's position once the
                    // dust settles; re-committing an already-committed offset
                    // is a broker-side no-op. The rebalance always ends with
                    // an assign round, so the last re-offer lands after the
                    // group is stable again.
                    //
                    // `mark_dirty_resolved`, not `mark_dirty`: a rebalance is
                    // evidence the member is still talking to the coordinator,
                    // which is the opposite of the fenced signature, so it must
                    // clear the streak rather than start one.
                    for tracker in self.partitions.values_mut() {
                        tracker.mark_dirty_resolved();
                    }
                }
                RebalanceEvent::CommitFailed(partitions) => {
                    for partition in partitions {
                        self.mark_dirty(partition, now);
                    }
                }
            }
        }
    }

    /// Returns the first partition that has been continuously dirty for at
    /// least `threshold`, or `None` if every partition is either clean or
    /// still within a normal rebalance's timing budget.
    ///
    /// This is the fenced-member signature from the 2026-07 staging
    /// incident: offset commits kept getting rejected ("Specified group
    /// generation id is not valid") with no resolving rebalance ever
    /// arriving, so the consumer sat wedged — silently, since the receive
    /// loop never errored and the task never finished, so it kept reporting
    /// as an active group member. `threshold` should comfortably exceed the
    /// broker's own rebalance protocol timing (`SESSION_TIMEOUT_MS`) so this
    /// never fires during an ordinary, self-resolving rebalance.
    fn fenced(&self, now: Instant, threshold: Duration) -> Option<i32> {
        self.partitions.iter().find_map(|(&partition, tracker)| {
            tracker
                .stuck_for(now)
                .filter(|&stuck| stuck >= threshold)
                .map(|_| partition)
        })
    }

    /// Returns the partitions that have new contiguous-from-start offsets to
    /// commit, or `None` if nothing has advanced since the last call.
    ///
    /// perf-K-16: the previous impl allocated a fresh `TopicPartitionList` on
    /// every receive-loop iteration even when no partition had progress to
    /// commit (the common case). Returning `Option` skips the C-heap (librdkafka
    /// FFI) allocation when there's nothing to do.
    ///
    /// The second element carries the terminal discards this commit would
    /// retire. A non-empty vec is what makes the caller commit with
    /// `CommitMode::Sync`: the accounting needs a broker-confirmed commit, and
    /// paying for one only when a terminal offset is in the batch keeps the
    /// ordinary Ack-only path asynchronous.
    fn drain_committable(&mut self) -> Option<(TopicPartitionList, Vec<TerminalDiscard>)> {
        let mut tpl: Option<TopicPartitionList> = None;
        let mut discards = Vec::new();
        for (&partition, tracker) in &mut self.partitions {
            if let Some((commit_offset, covered)) = tracker.drain_committable() {
                discards.extend(covered);
                tpl.get_or_insert_with(TopicPartitionList::new)
                    .add_partition_offset(&self.topic, partition, Offset::Offset(commit_offset))
                    .ok();
            }
        }
        tpl.map(|tpl| (tpl, discards))
    }
}

// ---------------------------------------------------------------------------
// Metadata extraction functions
// ---------------------------------------------------------------------------

fn extract_string_headers(msg: &BorrowedMessage<'_>) -> Arc<HashMap<String, String>> {
    let mut out = HashMap::new();
    if let Some(headers) = msg.headers() {
        for idx in 0..headers.count() {
            let header = headers.get(idx);
            if let Some(value) = header.value
                && let Ok(s) = std::str::from_utf8(value)
            {
                out.insert(header.key.to_string(), s.to_string());
            }
        }
    }
    Arc::new(out)
}

fn get_retry_count(headers: &HashMap<String, String>) -> u32 {
    headers
        .get(RETRY_COUNT_HEADER)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
}

fn build_message_metadata(
    headers: &Arc<HashMap<String, String>>,
    redelivered: bool,
) -> MessageMetadata {
    let retry_count = get_retry_count(headers);
    let delivery_id = headers.get(MESSAGE_ID_HEADER).cloned().unwrap_or_default();
    MessageMetadata {
        retry_count,
        delivery_id,
        redelivered,
        // Kafka delivery is offset-based: the broker keeps no per-message
        // attempt counter, and a redelivery after a rebalance or restart is
        // indistinguishable from a first read. Reporting `retry_count + 1` here
        // would silently under-count exactly those cases, so report "unknown".
        delivery_count: None,
        headers: Arc::clone(headers),
    }
}

fn build_dead_metadata(headers: &Arc<HashMap<String, String>>) -> DeadMessageMetadata {
    let message = build_message_metadata(headers, false);
    let reason = headers.get(DEATH_REASON_HEADER).cloned();
    let original_queue = headers.get(ORIGINAL_QUEUE_HEADER).cloned();
    let death_count = headers
        .get(DEATH_COUNT_HEADER)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);

    DeadMessageMetadata {
        message,
        reason,
        original_queue,
        death_count,
    }
}

// ---------------------------------------------------------------------------
// Header building helpers
// ---------------------------------------------------------------------------

fn headers_with_retry_count(
    original: &HashMap<String, String>,
    retry_count: u32,
    message_id_suffix: &str,
) -> OwnedHeaders {
    // perf-K-8: original.len() bounds the carried-over headers; +2 for the
    // RETRY_COUNT_HEADER and MESSAGE_ID_HEADER we always re-insert.
    let mut headers = OwnedHeaders::new_with_capacity(original.len() + 2);
    for (k, v) in original {
        if k == RETRY_COUNT_HEADER || k == MESSAGE_ID_HEADER {
            continue;
        }
        headers = headers.insert(Header {
            key: k.as_str(),
            value: Some(v.as_bytes()),
        });
    }
    headers = headers.insert(Header {
        key: RETRY_COUNT_HEADER,
        value: Some(retry_count.to_string().as_bytes()),
    });

    let original_id = original.get(MESSAGE_ID_HEADER).cloned().unwrap_or_default();
    let new_id = format!("{original_id}{message_id_suffix}");
    headers = headers.insert(Header {
        key: MESSAGE_ID_HEADER,
        value: Some(new_id.as_bytes()),
    });
    headers
}

fn headers_for_dlq(
    original: &HashMap<String, String>,
    reason: &str,
    original_queue: &str,
) -> OwnedHeaders {
    // perf-K-8: original.len() bounds the carried-over headers; +4 for the
    // DEATH_REASON / ORIGINAL_QUEUE / DEATH_COUNT / MESSAGE_ID we re-insert.
    let mut headers = OwnedHeaders::new_with_capacity(original.len() + 4);
    for (k, v) in original {
        if k == DEATH_REASON_HEADER
            || k == ORIGINAL_QUEUE_HEADER
            || k == DEATH_COUNT_HEADER
            || k == MESSAGE_ID_HEADER
        {
            continue;
        }
        headers = headers.insert(Header {
            key: k.as_str(),
            value: Some(v.as_bytes()),
        });
    }
    headers = headers.insert(Header {
        key: DEATH_REASON_HEADER,
        value: Some(reason.as_bytes()),
    });
    headers = headers.insert(Header {
        key: ORIGINAL_QUEUE_HEADER,
        value: Some(original_queue.as_bytes()),
    });

    let current_death_count = original
        .get(DEATH_COUNT_HEADER)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    headers = headers.insert(Header {
        key: DEATH_COUNT_HEADER,
        value: Some((current_death_count + 1).to_string().as_bytes()),
    });

    let original_id = original.get(MESSAGE_ID_HEADER).cloned().unwrap_or_default();
    headers = headers.insert(Header {
        key: MESSAGE_ID_HEADER,
        value: Some(format!("{original_id}-dlq").as_bytes()),
    });
    headers
}

// ---------------------------------------------------------------------------
// Outcome routing functions
// ---------------------------------------------------------------------------

/// Record a `FailAll` poisoning, logging only the first transition per key.
/// A no-op under `SequenceFailure::Skip` and for unkeyed messages.
fn poison_key(poisoned: &PoisonedKeys, key: &str, queue: &str) {
    if poisoned.poison(key) {
        tracing::info!(queue, sequence_key = %key, "poisoning sequence key (FailAll)");
    }
}

fn adjust_outcome_for_fifo(outcome: Outcome) -> Outcome {
    match outcome {
        Outcome::Defer => {
            tracing::warn!("Defer is not supported on sequenced consumers — treating as Retry");
            Outcome::Retry
        }
        other => other,
    }
}

async fn publish_to_dlq(
    client: &KafkaClient,
    topology: &QueueTopology,
    payload: &[u8],
    key: Option<&[u8]>,
    headers: &HashMap<String, String>,
    reason: &str,
) -> Result<()> {
    let dlq_topic = match topology.dlq() {
        Some(dlq) => dlq.to_string(),
        None => {
            tracing::warn!(
                queue = topology.queue(),
                "no DLQ configured, message will be discarded"
            );
            return Ok(());
        }
    };

    let dlq_headers = headers_for_dlq(headers, reason, topology.queue());
    client
        .publish_with_retry(
            &dlq_topic,
            key,
            dlq_headers,
            payload,
            MAX_PUBLISH_ATTEMPTS,
            "DLQ publish",
        )
        .await
}

/// Completion handle for the concurrent (non-FIFO) consumer path.
///
/// Threaded into [`route_outcome`] so the function can signal offset-commit
/// readiness exactly once per message — synchronously for terminal outcomes
/// (Ack, DLQ-terminal Retry/Reject), or from inside the delayed-republish
/// spawn for Retry/Defer **after** the republish has actually landed. This
/// closes the at-least-once gap that existed when the outer task signaled
/// completion before the delayed publish had been attempted.
///
/// `None` selects the FIFO path: no async signaling, [`route_outcome`]
/// instead awaits the republish inline and returns whether the caller may
/// proceed with `consumer.commit_message`.
type CompletionHandle = Option<(mpsc::Sender<Completion>, i32, i64)>;

/// A finished delivery handed back to the offset tracker.
struct Completion {
    partition: i32,
    offset: i64,
    /// Discard accounting that must not be counted until this offset is
    /// actually committed, so it rides along with the offset it depends on.
    /// See [`PartitionTracker::pending_discards`].
    discard: Option<TerminalDiscard>,
}

impl Completion {
    /// A completion with no terminal accounting riding on it — an `Ack`, a
    /// landed republish, or a pre-handler path that routed the message
    /// somewhere it still exists.
    fn plain(partition: i32, offset: i64) -> Self {
        Self {
            partition,
            offset,
            discard: None,
        }
    }
}

/// A terminal discard waiting on the commit that retires its message.
///
/// Which settle method applies is decided where the outcome is routed, not
/// where the commit lands, so the choice travels with the pending record.
enum TerminalDiscard {
    /// Dead-lettered, or terminal on a topic with no DLQ. Counts only when no
    /// DLQ exists — with one, the message is still in it.
    Retired(metrics::PendingDiscard),
    /// A DLQ was configured and the publish to it failed. Nothing holds a
    /// copy, so this counts regardless of the topology.
    Lost(metrics::PendingDiscard),
}

impl TerminalDiscard {
    /// The commit landed: the message is genuinely gone.
    fn confirm(self) {
        match self {
            Self::Retired(pending) => pending.confirm(),
            Self::Lost(pending) => pending.confirm_lost(),
        }
    }

    /// The commit did not land, so the message will be redelivered.
    fn survived(self) {
        match self {
            Self::Retired(pending) | Self::Lost(pending) => pending.survived(),
        }
    }
}

/// How a terminally-rejected message's discard must be settled.
///
/// Shared by the single-message and batch reject paths so the two cannot drift:
/// the decision depends only on whether the topology declares a DLQ and whether
/// this message's publish to it landed, never on which path asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RejectSettlement {
    /// It is in the DLQ, so it exists whatever the commit does. Settle now and
    /// keep the ordinary dead-letter path off the commit round trip.
    InDlq,
    /// No DLQ declared: retiring this message drops it. Counts if the commit
    /// lands.
    Retired,
    /// A DLQ was declared and did not receive it. Nothing holds a copy, so this
    /// is data loss whatever the topology says. Counts if the commit lands.
    Lost,
}

/// Classify a rejected message for [`RejectSettlement`].
///
/// `reached_dlq` is meaningful only when `has_dlq`; with no DLQ declared there
/// was no publish to succeed or fail, and the message is simply dropped.
fn reject_settlement(has_dlq: bool, reached_dlq: bool) -> RejectSettlement {
    match (has_dlq, reached_dlq) {
        (false, _) => RejectSettlement::Retired,
        (true, true) => RejectSettlement::InDlq,
        (true, false) => RejectSettlement::Lost,
    }
}

/// Hand this delivery's offset to the tracker so it can be committed.
///
/// Returns whether the offset was actually handed over. A full channel means
/// it is never committed, so the message is redelivered from the last
/// committed position on the next rebalance or restart — callers about to
/// assert the message is gone must not do so when this returns `false`.
///
/// **`true` is not a broker acknowledgement**, and terminal accounting is
/// therefore *not* settled here. It says only that the offset entered the
/// in-process tracker; the commit can still fail, or be dropped outright by
/// librdkafka mid-rebalance without any callback (see
/// `OffsetTracker::apply_rebalance_events`), and a redelivered message is one
/// `messages_discarded_total` promised no longer exists.
///
/// So a `discard` handed in here travels with its offset into the tracker and
/// is settled by the commit that covers it, which the receive loop issues with
/// `CommitMode::Sync` precisely because a terminal offset is in the batch —
/// rust-rdkafka's `Sync` waits for the broker to finish the commit, which is
/// the positive signal hand-off cannot give. A commit that fails, or a
/// partition revoked before one lands, settles as `survived`: undercounting an
/// ambiguous retirement is safe, claiming one that did not happen is not.
fn signal_completion(
    handle: CompletionHandle,
    queue: &str,
    discard: Option<TerminalDiscard>,
) -> bool {
    // No tracker attached (the FIFO path commits inline): nothing to signal,
    // and nothing holding the delivery back either. FIFO settles its own
    // accounting on the synchronous commit, so it never hands one in here.
    let Some((tx, partition, offset)) = handle else {
        debug_assert!(
            discard.is_none(),
            "FIFO settles terminal accounting on its own commit"
        );
        if let Some(discard) = discard {
            discard.survived();
        }
        return true;
    };
    if let Err(e) = tx.try_send(Completion {
        partition,
        offset,
        discard,
    }) {
        tracing::error!(
            queue,
            partition,
            offset,
            "completion channel full — logic bug in offset tracker"
        );
        // The offset never reaches the tracker, so it is never committed and
        // the message is redelivered — the opposite of retired.
        if let Some(discard) = e.into_inner().discard {
            discard.survived();
        }
        return false;
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn route_outcome(
    client: &KafkaClient,
    topic: &str,
    // Optional consumer-group label propagated to `metrics::record_failed`
    // on DLQ-terminal outcomes (max_retries_exceeded, Rejected). Matches the
    // shape `invoke_handler` already uses.
    group: Option<&str>,
    payload: &[u8],
    // perf-K-9: take key as Option<Bytes> by value. Each match arm uses it
    // once, so we move it instead of cloning. The receive loop's Bytes
    // refcount machinery makes any further sharing a refcount bump.
    key: Option<Bytes>,
    headers: &HashMap<String, String>,
    outcome: Outcome,
    topology: &'static QueueTopology,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[HoldQueue],
    // sec-K-8: retry/defer arms move this permit into the delayed-republish
    // spawn so the prefetch semaphore stays bounded across delayed work.
    // None on the FIFO path (no semaphore in play there) and on the
    // outer-task's Ack/Reject arms (permit drops at end of scope).
    retry_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    completion: CompletionHandle,
    // Threaded into the Retry/Defer republish spawn so a graceful shutdown
    // can short-circuit the (potentially minute-long) hold-queue delay
    // instead of stalling `acquire_many(prefetch)` until every delayed
    // permit-holder finishes naturally.
    shutdown: CancellationToken,
) -> (bool, Option<metrics::PendingDiscard>) {
    match decide_retry(&outcome, retry_count, max_retries) {
        RetryDecision::Ack => {
            let _ = signal_completion(completion, topic, None);
            (true, None)
        }
        RetryDecision::Dlq { reason } => {
            let fail_reason = match reason {
                "rejected" => metrics::FailReason::Rejected,
                _ => metrics::FailReason::MaxRetriesExceeded,
            };
            let pending =
                metrics::record_terminal(topic, group, fail_reason, topology.dlq().is_some());
            // Neither path settles the accounting here. FIFO commits
            // synchronously in the caller; the concurrent path hands the
            // pending record to the tracker, which settles it on the
            // `CommitMode::Sync` commit that covers the offset. Both wait for
            // the only thing Kafka offers as a real acknowledgement that a
            // message is retired.
            let fifo = completion.is_none();
            let dlq_ok =
                publish_to_dlq(client, topology, payload, key.as_deref(), headers, reason).await;
            match dlq_ok {
                Ok(()) => {
                    // `publish_to_dlq` is `Ok(())` both when it dead-lettered
                    // and when no DLQ is configured; `confirm` distinguishes
                    // them from the topology.
                    if fifo {
                        return (true, Some(pending));
                    }
                    // Commit even though this is terminal: the message has
                    // exhausted retries (or was rejected) and looping it
                    // forever produces a poison hot-spot.
                    match reject_settlement(topology.dlq().is_some(), true) {
                        RejectSettlement::InDlq => {
                            // Settling now keeps the ordinary dead-letter path
                            // off the synchronous commit a pending record would
                            // force.
                            pending.survived();
                            signal_completion(completion, topic, None);
                        }
                        settled => {
                            debug_assert_eq!(settled, RejectSettlement::Retired);
                            signal_completion(
                                completion,
                                topic,
                                Some(TerminalDiscard::Retired(pending)),
                            );
                        }
                    }
                    (true, None)
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to publish to DLQ");
                    // A DLQ was configured and did not receive the message. If
                    // the offset still advances, nothing holds a copy — that is
                    // data loss even though the topology declares a DLQ, so it
                    // is counted rather than excused by `has_dlq`. Whether the
                    // offset advances is now decided by the commit, so the
                    // distinction rides along as `Lost` rather than being
                    // resolved here.
                    //
                    // FIFO returns `false`, so its caller skips the commit
                    // entirely and the message stays put — no loss to record.
                    if fifo {
                        pending.survived();
                    } else {
                        // `publish_to_dlq` is `Ok(())` when no DLQ is declared,
                        // so reaching this arm at all means one was.
                        debug_assert_eq!(
                            reject_settlement(topology.dlq().is_some(), false),
                            RejectSettlement::Lost
                        );
                        signal_completion(completion, topic, Some(TerminalDiscard::Lost(pending)));
                    }
                    (false, None)
                }
            }
        }
        RetryDecision::Hold { increment: true } => {
            let new_count = retry_count + 1;
            let delay = if hold_queues.is_empty() {
                Duration::from_secs(1)
            } else {
                let idx = hold_index(retry_count, hold_queues.len());
                hold_queues[idx].delay()
            };

            let retry_headers =
                headers_with_retry_count(headers, new_count, &format!("-r{new_count}"));

            (
                run_delayed_republish(
                    client.clone(),
                    topic.to_string(),
                    key,
                    retry_headers,
                    payload.to_vec(),
                    delay,
                    retry_permit,
                    completion,
                    shutdown,
                    "retry republish",
                )
                .await,
                None,
            )
        }
        RetryDecision::Hold { increment: false } => {
            let delay = if hold_queues.is_empty() {
                Duration::from_secs(1)
            } else {
                hold_queues[0].delay()
            };

            // Defer does NOT increment retry count.
            let defer_headers = headers_with_retry_count(
                headers,
                retry_count,
                &format!("-d{}", uuid::Uuid::new_v4()),
            );

            (
                run_delayed_republish(
                    client.clone(),
                    topic.to_string(),
                    key,
                    defer_headers,
                    payload.to_vec(),
                    delay,
                    retry_permit,
                    completion,
                    shutdown,
                    "defer republish",
                )
                .await,
                None,
            )
        }
    }
}

/// Drive the Retry/Defer delayed republish.
///
/// **Concurrent path** (`completion: Some`): spawns the work and returns
/// immediately. The spawn races `sleep(delay)` against the shutdown token —
/// if shutdown wins, the spawn drops the permit without publishing or
/// signaling, so the message will be redelivered on next start. If sleep
/// wins, it publishes; on success it signals completion (offset gets
/// committed), on failure it logs and drops without signaling so the
/// message is redelivered.
///
/// **FIFO path** (`completion: None`): awaits the republish inline. Returns
/// `true` iff publish succeeded and the caller may proceed with
/// `consumer.commit_message`. On shutdown or publish failure returns
/// `false` — the FIFO loop will see the same shutdown via its own polling
/// path.
#[allow(clippy::too_many_arguments)]
async fn run_delayed_republish(
    client: KafkaClient,
    topic: String,
    key: Option<Bytes>,
    headers: OwnedHeaders,
    payload: Vec<u8>,
    delay: Duration,
    retry_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    completion: CompletionHandle,
    shutdown: CancellationToken,
    label: &'static str,
) -> bool {
    match completion {
        Some(_) => {
            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.cancelled() => {
                        tracing::debug!(
                            queue = %topic,
                            label,
                            "shutdown fired before delayed republish; dropping permit — \
                             offset stays uncommitted, message will be redelivered on restart"
                        );
                        drop(retry_permit);
                        return;
                    }
                }
                match client
                    .publish_with_retry(
                        &topic,
                        key.as_deref(),
                        headers,
                        &payload,
                        MAX_PUBLISH_ATTEMPTS,
                        label,
                    )
                    .await
                {
                    Ok(()) => {
                        let _ = signal_completion(completion, &topic, None);
                    }
                    Err(e) => {
                        // Don't signal — leaving the offset uncommitted is the
                        // only thing preserving at-least-once delivery if the
                        // republish itself fails. The next poll/restart will
                        // redeliver the original message.
                        tracing::error!(
                            error = %e,
                            label,
                            "delayed republish failed — leaving offset uncommitted for redelivery"
                        );
                    }
                }
                // sec-K-8: permit lifetime = full processing including
                // delayed republish, so prefetch bounds inflight work.
                drop(retry_permit);
            });
            true
        }
        None => {
            // FIFO path: serialize inline so the per-partition ordering
            // contract is preserved (a spawn would let the next message
            // run before the current republish lands).
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.cancelled() => {
                    tracing::debug!(
                        queue = %topic,
                        label,
                        "shutdown fired before FIFO republish; skipping — \
                         message will be redelivered on restart"
                    );
                    drop(retry_permit);
                    return false;
                }
            }
            let ok = match client
                .publish_with_retry(
                    &topic,
                    key.as_deref(),
                    headers,
                    &payload,
                    MAX_PUBLISH_ATTEMPTS,
                    label,
                )
                .await
            {
                Ok(()) => true,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        label,
                        "FIFO delayed republish failed — leaving offset uncommitted for redelivery"
                    );
                    false
                }
            };
            drop(retry_permit);
            ok
        }
    }
}

// ---------------------------------------------------------------------------
// Handler invocation
// ---------------------------------------------------------------------------

/// Invoke the handler future with an optional timeout, emitting inflight /
/// consumed / duration metrics. Returns `Outcome::Retry` on timeout or panic.
///
/// Awaits the handler future with timeout + panic isolation, recording
/// per-outcome metrics. A panic inside the user's handler is caught via
/// `AssertUnwindSafe(...).catch_unwind()` and surfaced as `Outcome::Retry`.
///
/// perf-K-7: this previously spawned an inner `tokio::spawn` to catch panics
/// via JoinError. The spawn allocated a task struct + scheduler enqueue per
/// message — combined with the wrapper spawn (now removed) and outer outcome
/// spawn, that was 3 spawns per message. catch_unwind achieves the same
/// panic-isolation outcome without the task alloc.
async fn invoke_handler<F>(
    fut: F,
    timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    topic: &str,
    group: Option<&str>,
) -> Outcome
where
    F: std::future::Future<Output = Outcome> + Send,
{
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    let _inflight = metrics::InflightGuard::from_refs(topic, group);
    let start = std::time::Instant::now();
    let safe_fut = AssertUnwindSafe(fut).catch_unwind();
    let outcome = match timeout {
        Some(duration) => match tokio::time::timeout(duration, safe_fut).await {
            Ok(Ok(o)) => o,
            Ok(Err(_panic)) => {
                tracing::warn!("handler panicked, retrying message");
                Outcome::Retry
            }
            Err(_) => {
                let resolved = handler_timeout_outcome(timeout_outcome);
                tracing::warn!(outcome = ?resolved, "handler timed out after {duration:?}");
                metrics::record_failed(topic, group, metrics::FailReason::Timeout);
                resolved
            }
        },
        None => match safe_fut.await {
            Ok(o) => o,
            Err(_panic) => {
                tracing::warn!("handler panicked, retrying message");
                Outcome::Retry
            }
        },
    };
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_consumed(topic, group, &outcome);
    metrics::record_processing_duration(topic, group, &outcome, elapsed);
    outcome
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Maps a rdkafka `KafkaError` to the appropriate `ShoveError` variant.
/// Permanent errors (bad config, fatal consumption, cancelled) become
/// `Topology`; transient errors (broker down, network) become `Connection`.
fn map_kafka_error(context: &str, e: KafkaError) -> ShoveError {
    let is_permanent = matches!(
        &e,
        KafkaError::ClientConfig(..)
            | KafkaError::ClientCreation(_)
            | KafkaError::MessageConsumptionFatal(_)
            | KafkaError::Canceled
            | KafkaError::Nul(_)
    );
    if is_permanent {
        ShoveError::Topology(format!("{context}: {e}"))
    } else {
        ShoveError::Connection(format!("{context}: {e}"))
    }
}

// ---------------------------------------------------------------------------
// Rebalance plumbing
// ---------------------------------------------------------------------------

/// Consumer-group event forwarded from librdkafka's callbacks to the receive
/// loop that owns the `OffsetTracker`.
enum RebalanceEvent {
    Assign(Vec<i32>),
    Revoke(Vec<i32>),
    /// An async offset commit was rejected — typically
    /// `REBALANCE_IN_PROGRESS` or a stale generation while the group was
    /// re-forming. `drain_committable` only yields on *new* completions, so
    /// without an explicit retry the failed commit would never be resubmitted
    /// and the partition's committed offset would stay stale until (if ever)
    /// new traffic arrived. The listed partitions re-offer their current
    /// commit position on the next drain.
    CommitFailed(Vec<i32>),
}

/// `ConsumerContext` that forwards partition assign/revoke deltas to the
/// receive loop over a channel, wrapping whichever inner `ClientContext`
/// matches the broker's auth mode (default or MSK IAM).
///
/// The callback is synchronous (librdkafka invokes it during a poll inside
/// `StreamConsumer::recv`), so a `std::sync::mpsc` channel is used and the
/// loop drains it non-blocking with `try_recv`. Unbounded is safe: rebalances
/// are rare and each event is a small partition list.
pub(super) struct RebalanceContext<C: ClientContext> {
    inner: C,
    /// Topic this consumer subscribes to; TPL entries for other topics are
    /// filtered out (each shove consumer subscribes to exactly one topic).
    topic: String,
    /// The consumer's unique `client.id` — identifies which group member an
    /// assignment change happened on in the logs.
    client_id: String,
    tx: std_mpsc::Sender<RebalanceEvent>,
}

impl<C: ClientContext> RebalanceContext<C> {
    fn partitions_for_topic(&self, tpl: &TopicPartitionList) -> Vec<i32> {
        tpl.elements()
            .iter()
            .filter(|e| e.topic() == self.topic)
            .map(|e| e.partition())
            .collect()
    }
}

/// Load-bearing delegation: every method and associated const the inner
/// context overrides MUST be forwarded here, or the override silently stops
/// working once the context is wrapped. `MskIamContext` overrides
/// `ENABLE_REFRESH_OAUTH_TOKEN` and `generate_oauth_token` (OAUTHBEARER token
/// refresh — MSK auth breaks without them); `log`/`stats`/`stats_raw`/`error`
/// are forwarded too so any future inner override keeps working.
impl<C: ClientContext> ClientContext for RebalanceContext<C> {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = C::ENABLE_REFRESH_OAUTH_TOKEN;

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.inner.log(level, fac, log_message);
    }

    fn stats(&self, statistics: Statistics) {
        self.inner.stats(statistics);
    }

    fn stats_raw(&self, statistics: &[u8]) {
        self.inner.stats_raw(statistics);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.inner.error(error, reason);
    }

    fn generate_oauth_token(
        &self,
        oauthbearer_config: Option<&str>,
    ) -> std::result::Result<OAuthToken, Box<dyn std::error::Error>> {
        self.inner.generate_oauth_token(oauthbearer_config)
    }
}

impl<C: ClientContext> ConsumerContext for RebalanceContext<C> {
    /// `pre_rebalance` (not `post_`) for both directions: pre and post
    /// receive the identical TPL delta (rdkafka's default `rebalance` builds
    /// the `Rebalance` value once and passes it to both), and pre runs
    /// *before* librdkafka applies the incremental (un)assignment — so an
    /// Assign event is guaranteed to be queued before any message from the
    /// new assignment can be delivered, which is the ordering the tracker
    /// reset relies on.
    ///
    /// The inner context's own `ConsumerContext` methods cannot be delegated
    /// (they take `&BaseConsumer<C>`, not `&BaseConsumer<Self>`); both
    /// wrapped contexts use the empty defaults, so nothing is lost.
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let event = match rebalance {
            Rebalance::Assign(tpl) => {
                let partitions = self.partitions_for_topic(tpl);
                tracing::debug!(
                    topic = %self.topic,
                    client_id = %self.client_id,
                    ?partitions,
                    "rebalance: partitions assigned"
                );
                RebalanceEvent::Assign(partitions)
            }
            Rebalance::Revoke(tpl) => {
                let partitions = self.partitions_for_topic(tpl);
                tracing::debug!(
                    topic = %self.topic,
                    client_id = %self.client_id,
                    ?partitions,
                    "rebalance: partitions revoked"
                );
                RebalanceEvent::Revoke(partitions)
            }
            Rebalance::Error(e) => {
                tracing::warn!(topic = %self.topic, error = %e, "rebalance error");
                return;
            }
        };
        // A closed channel means the receive loop is gone (shutdown or
        // reconnect teardown, or a consumer that keeps no tracker) — nothing
        // to notify.
        let _ = self.tx.send(event);
    }

    /// Async commit results surface here (the default impl is silent). A
    /// commit rejected during a rebalance (stale generation,
    /// `REBALANCE_IN_PROGRESS`) would otherwise be lost for good: the
    /// tracker's `next_to_commit` has already advanced past the offsets in
    /// the failed request, so no future drain re-offers them. Report the
    /// failure to the loop so the affected partitions re-offer their current
    /// position.
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        if let Err(e) = result {
            let partitions = self.partitions_for_topic(offsets);
            tracing::warn!(
                topic = %self.topic,
                client_id = %self.client_id,
                error = %e,
                ?partitions,
                "async offset commit failed; scheduling re-commit"
            );
            let _ = self.tx.send(RebalanceEvent::CommitFailed(partitions));
        }
    }
}

// ---------------------------------------------------------------------------
// KafkaStreamConsumer — context-agnostic wrapper
// ---------------------------------------------------------------------------

pub(super) enum KafkaStreamConsumer {
    Default(StreamConsumer<RebalanceContext<DefaultClientContext>>),
    #[cfg(feature = "kafka-msk-iam")]
    MskIam(StreamConsumer<RebalanceContext<MskIamContext>>),
}

impl KafkaStreamConsumer {
    pub(super) fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        match self {
            Self::Default(c) => c.subscribe(topics),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.subscribe(topics),
        }
    }

    pub(super) async fn recv(&self) -> KafkaResult<BorrowedMessage<'_>> {
        match self {
            Self::Default(c) => c.recv().await,
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.recv().await,
        }
    }

    pub(super) fn commit(&self, tpl: &TopicPartitionList, mode: CommitMode) -> KafkaResult<()> {
        match self {
            Self::Default(c) => c.commit(tpl, mode),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.commit(tpl, mode),
        }
    }

    pub(super) fn commit_message(
        &self,
        msg: &BorrowedMessage<'_>,
        mode: CommitMode,
    ) -> KafkaResult<()> {
        match self {
            Self::Default(c) => c.commit_message(msg, mode),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.commit_message(msg, mode),
        }
    }

    /// Rewinds the given partitions to the offsets in `tpl` so the next
    /// `recv()` re-delivers from there. Used by the batch consumer to
    /// redeliver an entire un-acked batch instead of silently skipping past
    /// it (see `run_batch`).
    pub(super) fn seek_partitions(
        &self,
        tpl: TopicPartitionList,
        timeout: Duration,
    ) -> KafkaResult<TopicPartitionList> {
        match self {
            Self::Default(c) => c.seek_partitions(tpl, timeout),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.seek_partitions(tpl, timeout),
        }
    }

    /// This member's current partition assignment. Used to tell a seek that
    /// failed because the partition was revoked (expected) from one that failed
    /// on a partition still held (message loss) — see `rewind_after_rebalance`.
    pub(super) fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        match self {
            Self::Default(c) => c.assignment(),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.assignment(),
        }
    }
}

// Consumer helper
// ---------------------------------------------------------------------------

fn create_stream_consumer(
    mut base: ClientConfig,
    group_id: &str,
    auto_offset_reset: KafkaAutoOffsetReset,
    topic: &str,
    rebalance_tx: std_mpsc::Sender<RebalanceEvent>,
    #[cfg(feature = "kafka-msk-iam")] msk_context: Option<MskIamContext>,
) -> Result<KafkaStreamConsumer> {
    // Each consumer task within a group gets a distinct `client.id` so
    // librdkafka treats them as separate members. Without this, group
    // rebalances across repeated join attempts can produce stale
    // "group generation id is not valid" commit errors.
    let client_id = format!("shove-{}", uuid::Uuid::new_v4().simple());
    base.set("group.id", group_id)
        .set("client.id", &client_id)
        // Cooperative-sticky assignment performs incremental rebalance so that
        // adding/removing a consumer only reassigns the delta — without this,
        // every join triggers an eager (stop-the-world) rebalance that
        // freezes the entire group for the heartbeat window.
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", auto_offset_reset.as_rdkafka_str())
        .set("session.timeout.ms", SESSION_TIMEOUT_MS.to_string())
        .set("max.poll.interval.ms", MAX_POLL_INTERVAL_MS.to_string())
        // Minimise fetch-latency so small-payload workloads aren't bottlenecked
        // by the default 500 ms broker dwell. `FETCH_MIN_BYTES=1` returns as
        // soon as any data is available; `FETCH_WAIT_MAX_MS=50` caps the
        // blocking dwell so the broker doesn't hold the connection open.
        .set("fetch.min.bytes", FETCH_MIN_BYTES.to_string())
        .set("fetch.wait.max.ms", FETCH_WAIT_MAX_MS.to_string());

    #[cfg(feature = "kafka-msk-iam")]
    if let Some(ctx) = msk_context {
        let ctx = RebalanceContext {
            inner: ctx,
            topic: topic.to_string(),
            client_id,
            tx: rebalance_tx,
        };
        let consumer: StreamConsumer<RebalanceContext<MskIamContext>> = base
            .create_with_context(ctx)
            .map_err(|e| map_kafka_error("failed to create MSK consumer", e))?;
        return Ok(KafkaStreamConsumer::MskIam(consumer));
    }

    let ctx = RebalanceContext {
        inner: DefaultClientContext,
        topic: topic.to_string(),
        client_id,
        tx: rebalance_tx,
    };
    let consumer: StreamConsumer<RebalanceContext<DefaultClientContext>> = base
        .create_with_context(ctx)
        .map_err(|e| map_kafka_error("failed to create consumer", e))?;
    Ok(KafkaStreamConsumer::Default(consumer))
}

// ---------------------------------------------------------------------------
// Reconnect loop
// ---------------------------------------------------------------------------

/// A consumer that stayed up at least this long before erroring is considered
/// to have had a healthy connection: the reconnect budget and backoff reset,
/// so `max_reconnect_attempts` bounds *consecutive* failures, not lifetime.
const RECONNECT_RESET_AFTER: Duration = Duration::from_secs(60);

/// How often the concurrent receive loop wakes to drain rebalance events and
/// retry commits when no messages or completions arrive to wake it.
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(5);

/// How long a partition may sit with offset commits continuously rejected,
/// no resolving rebalance ever arriving, before the receive loop treats
/// itself as fenced from the group and forces a clean reconnect (see
/// `OffsetTracker::fenced`). Well above `SESSION_TIMEOUT_MS` so an ordinary
/// rebalance — which by protocol resolves inside that window — never trips
/// it; well below the "silent multi-hour wedge" this guards against.
const COMMIT_FENCE_TIMEOUT: Duration = Duration::from_secs(60);

/// Timeout for `seek_partitions` when redelivering an un-acked batch.
///
/// **Must stay non-zero.** `rd_kafka_seek_partitions` only waits for the
/// per-partition seeks — and so only fills in their real results — when it is
/// given a timeout; with zero it returns immediately, leaving every partition
/// marked `__IN_PROGRESS`. [`seek_errors`] would then read every seek as
/// failed and every redelivery would escalate to a reconnect.
const SEEK_TIMEOUT: Duration = Duration::from_secs(5);

/// First delay after redelivering an un-acked batch, escalating to
/// [`BATCH_REDELIVERY_BACKOFF_MAX`] — see `flush_batch`'s non-Ack arm.
const BATCH_REDELIVERY_BACKOFF_INITIAL: Duration = Duration::from_secs(1);

/// Ceiling on the redelivery delay for a handler that keeps returning non-Ack.
const BATCH_REDELIVERY_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Redelivery backoff schedule for a batch the handler did not Ack. Escalates
/// across *consecutive* non-Ack flushes and is reset on the first Ack, so a
/// wedged handler backs off instead of spinning the seek-then-recv cycle while
/// a handler that merely hit one bad batch pays the delay once.
fn batch_redelivery_backoff() -> Backoff {
    Backoff::new(
        BATCH_REDELIVERY_BACKOFF_INITIAL,
        BATCH_REDELIVERY_BACKOFF_MAX,
    )
}

async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    label: &str,
    max_reconnect_attempts: Option<u32>,
    mut f: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut backoff = Backoff::default();
    let mut attempts = 0u32;
    loop {
        let started = tokio::time::Instant::now();
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if started.elapsed() >= RECONNECT_RESET_AFTER {
                    attempts = 0;
                    backoff = Backoff::default();
                }
                if !e.is_retryable() {
                    return Err(e);
                }
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                attempts += 1;
                if let Some(max) = max_reconnect_attempts
                    && attempts >= max
                {
                    tracing::error!(
                        label,
                        attempts,
                        error = %e,
                        "max reconnect attempts reached, giving up"
                    );
                    return Err(ShoveError::Connection(format!(
                        "consumer on '{label}' exhausted {max} reconnect attempt(s): {e}"
                    )));
                }
                let delay = backoff.next().expect("backoff is infinite");
                tracing::warn!(
                    label,
                    attempt = attempts,
                    ?max_reconnect_attempts,
                    error = %e,
                    delay_ms = delay.as_millis() as u64,
                    "consumer error, reconnecting"
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.cancelled() => return Ok(()),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Batch consumer
// ---------------------------------------------------------------------------

/// Options for [`KafkaConsumer::run_batch`].
pub struct BatchConsumerOptions {
    max_batch_size: usize,
    max_batch_age: Duration,
    max_reconnect_attempts: Option<u32>,
    max_message_size: Option<usize>,
    handler_timeout: Option<Duration>,
    handler_timeout_outcome: Option<Outcome>,
    consumer_group: Option<Arc<str>>,
    kafka_group_id: Option<Arc<str>>,
    kafka_auto_offset_reset: Option<KafkaAutoOffsetReset>,
    shutdown: CancellationToken,
    #[cfg(feature = "kafka-schema-registry")]
    schema_registry: Option<Arc<SchemaRegistry>>,
    #[cfg(feature = "kafka-schema-registry")]
    schema_enforcement: SchemaEnforcement,
    #[cfg(feature = "kafka-schema-registry")]
    schema_accepted_subjects: Option<Vec<Arc<str>>>,
}

impl Default for BatchConsumerOptions {
    fn default() -> Self {
        Self {
            max_batch_size: 500,
            max_batch_age: Duration::from_millis(250),
            max_reconnect_attempts: None,
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            // Same default as `ConsumerOptions`. A batch flush is one DB
            // transaction rather than one row, so 30 s is a different amount of
            // headroom than it is on the single-message path — a sink whose
            // flush legitimately takes longer should raise it deliberately
            // rather than discover the default by having batches retried.
            handler_timeout: Some(DEFAULT_HANDLER_TIMEOUT),
            handler_timeout_outcome: None,
            consumer_group: None,
            kafka_group_id: None,
            kafka_auto_offset_reset: None,
            shutdown: CancellationToken::new(),
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: None,
            // Matches `ConsumerOptions`: enforcement is opt-out, not opt-in.
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: SchemaEnforcement::Enforce,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted_subjects: None,
        }
    }
}

impl BatchConsumerOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Flush once the batch reaches this many messages. Default 500.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0`.
    pub fn with_max_batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "max_batch_size must be > 0");
        self.max_batch_size = n;
        self
    }

    /// Flush once this long has elapsed since the first message in the
    /// current batch, even if `max_batch_size` hasn't been reached.
    /// Default 250ms.
    ///
    /// # Panics
    ///
    /// Panics if `d` is zero.
    pub fn with_max_batch_age(mut self, d: Duration) -> Self {
        assert!(!d.is_zero(), "max_batch_age must be positive");
        self.max_batch_age = d;
        self
    }

    pub fn with_max_reconnect_attempts(mut self, n: u32) -> Self {
        self.max_reconnect_attempts = Some(n);
        self
    }

    pub fn with_max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    /// Abandon a `handle_batch` call that runs longer than this and treat the
    /// batch as [`Outcome::Retry`]. Default [`DEFAULT_HANDLER_TIMEOUT`] (30 s).
    ///
    /// Same semantics as [`ConsumerOptions::with_handler_timeout`], and it
    /// matters more here: the batch loop is a single task, so a flush that
    /// never returns also stops offset commits, rebalance handling and
    /// shutdown for as long as it hangs.
    ///
    /// [`ConsumerOptions::with_handler_timeout`]: crate::ConsumerOptions::with_handler_timeout
    ///
    /// # Panics
    ///
    /// Panics if `timeout` is zero.
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "handler_timeout must be positive");
        self.handler_timeout = Some(timeout);
        self
    }

    /// Let `handle_batch` run for as long as it likes.
    ///
    /// For sinks whose flush has no meaningful upper bound. The cost is that a
    /// genuinely hung flush wedges the consumer with no recovery — including
    /// `shutdown.cancel()`, which cannot interrupt an in-flight flush.
    pub fn without_handler_timeout(mut self) -> Self {
        self.handler_timeout = None;
        self
    }

    /// What a batch handler timeout resolves to, instead of the default
    /// [`Outcome::Retry`].
    ///
    /// The same motivation as
    /// [`ConsumerOptions::with_handler_timeout_outcome`] — a slow flush is
    /// usually backpressure, not a poison batch — but **not** the same
    /// mechanics, because the outcome applies batch-wide and `run_batch` has
    /// no per-message retry counter:
    ///
    /// | Outcome | Effect on the whole batch |
    /// |---|---|
    /// | `Retry` (default) | Seek back and redeliver after an escalating delay. Forever, if the handler keeps timing out. |
    /// | `Defer` | **Identical to `Retry` here.** Same seek-back arm, same backoff. |
    /// | `Ack` | Commit every offset in the batch. The messages are gone, unprocessed, with no DLQ copy. |
    /// | `Reject` | Terminal: dead-letter every message (or discard it, with no DLQ declared) and commit. |
    ///
    /// The `Retry`/`Defer` distinction that matters on the single-message path
    /// — spending the retry budget versus not — has no meaning here. There is
    /// no budget to spend: a batch is never dead-lettered for exhausting one,
    /// so a timeout can never turn into the silent budget-exhaustion discard
    /// this option exists to avoid elsewhere. Setting `Defer` over the default
    /// is therefore a no-op, kept legal only so the two option types read the
    /// same.
    ///
    /// That leaves the real choice as *redeliver forever* (`Retry`/`Defer`)
    /// versus *give up on this batch* (`Ack`/`Reject`). Both of the latter
    /// retire messages the handler never finished, so reach for them only when
    /// a stalled flush genuinely means the payloads are no longer worth
    /// processing — and prefer `Reject` to `Ack` unless the topology has no
    /// DLQ, since `Reject` at least preserves them.
    ///
    /// [`ConsumerOptions::with_handler_timeout_outcome`]: crate::ConsumerOptions::with_handler_timeout_outcome
    pub fn with_handler_timeout_outcome(mut self, outcome: Outcome) -> Self {
        self.handler_timeout_outcome = Some(outcome);
        self
    }

    /// Tag this consumer with a group name for metrics labelling, exactly as
    /// [`ConsumerOptions::with_consumer_group`] does on the single-message
    /// path. Left unset it surfaces as `consumer_group="default"`.
    ///
    /// This is **not** [`with_group_id`](Self::with_group_id). The two are
    /// deliberately independent: `group_id` is the Kafka `group.id` the broker
    /// coordinates partition assignment with, while this is the logical group
    /// name the `consumer_group` metric label reports — the same value every
    /// other backend reports, so one dashboard query spans all of them. Setting
    /// only `with_group_id` moves the partitions without moving the label, and
    /// setting only this one moves the label without moving the partitions.
    ///
    /// [`ConsumerOptions::with_consumer_group`]: crate::ConsumerOptions::with_consumer_group
    pub fn with_consumer_group(mut self, name: impl Into<Arc<str>>) -> Self {
        self.consumer_group = Some(name.into());
        self
    }

    pub fn with_group_id(mut self, group_id: impl Into<Arc<str>>) -> Self {
        self.kafka_group_id = Some(group_id.into());
        self
    }

    pub fn with_auto_offset_reset(mut self, reset: KafkaAutoOffsetReset) -> Self {
        self.kafka_auto_offset_reset = Some(reset);
        self
    }

    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Decode batch messages through the Confluent Schema Registry: strip the
    /// wire frame, gate the resolved subject, then decode the inner payload
    /// with `T::Codec`.
    ///
    /// Same semantics as [`ConsumerOptions::with_schema_registry`] on the
    /// single-message path, including DLQ routing for frame/subject/decode
    /// failures. Without a registry the payload is decoded directly by
    /// `T::Codec`, unchanged.
    ///
    /// [`ConsumerOptions::with_schema_registry`]: crate::ConsumerOptions::with_schema_registry
    #[cfg(feature = "kafka-schema-registry")]
    pub fn with_schema_registry(mut self, registry: Arc<SchemaRegistry>) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    /// Whether a message whose schema subject is not accepted is routed to the
    /// DLQ (`Enforce`, the default) or decoded anyway with a warning
    /// (`Permissive`).
    #[cfg(feature = "kafka-schema-registry")]
    pub fn with_schema_enforcement(mut self, enforcement: SchemaEnforcement) -> Self {
        self.schema_enforcement = enforcement;
        self
    }

    /// Subjects this consumer accepts. Defaults to `{queue}-value`.
    #[cfg(feature = "kafka-schema-registry")]
    pub fn accept_schema_subjects<I, S>(mut self, subjects: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.schema_accepted_subjects = Some(subjects.into_iter().map(Into::into).collect());
        self
    }
}

/// The parts of a batch flush that don't change between flushes.
struct BatchFlushCtx<'a> {
    consumer: &'a Arc<KafkaStreamConsumer>,
    client: &'a KafkaClient,
    topology: &'a QueueTopology,
    queue: &'a str,
    topic: &'a str,
    group: Option<&'a str>,
    /// Cuts the redelivery backoff short so a wedged handler cannot hold up
    /// shutdown for the whole (escalated) delay.
    shutdown: &'a CancellationToken,
    /// How long one `handle_batch` call may run before it is abandoned.
    /// `None` waits forever — see
    /// [`BatchConsumerOptions::without_handler_timeout`].
    handler_timeout: Option<Duration>,
    /// What that abandonment resolves to. `None` keeps the historical
    /// `Outcome::Retry` — see
    /// [`BatchConsumerOptions::with_handler_timeout_outcome`].
    handler_timeout_outcome: Option<Outcome>,
}

/// Commits each partition's exclusive end offset in one batched sync commit.
///
/// The commit runs on a blocking thread: `CommitMode::Sync` is a blocking FFI
/// call into `rd_kafka_commit(…, 0)` that waits a full broker round trip, and
/// at the default 250 ms `max_batch_age` this fires ~4×/s — enough to park a
/// runtime worker often enough to starve co-scheduled tasks. The single-message
/// path can use `CommitMode::Async` because it has the `commit_callback`
/// machinery to pick up failures; a batch commit has to report its error to the
/// caller, so it stays synchronous and moves off the runtime instead.
async fn commit_batch_end(
    consumer: &Arc<KafkaStreamConsumer>,
    queue: &str,
    batch_end: &HashMap<i32, i64>,
) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    for (&partition, &end_offset) in batch_end {
        // Not `.ok()`: a partition silently missing from the list is a
        // partition whose offsets never commit, replaying the whole span on
        // the next restart. Fail the flush instead and let the reconnect
        // resume from the last committed offset.
        tpl.add_partition_offset(queue, partition, Offset::Offset(end_offset))
            .map_err(|e| map_kafka_error("batch commit offset list rejected a partition", e))?;
    }
    let consumer = consumer.clone();
    tokio::task::spawn_blocking(move || consumer.commit(&tpl, CommitMode::Sync))
        .await
        .map_err(|e| ShoveError::Connection(format!("batch commit task failed: {e}")))?
        .map_err(|e| map_kafka_error("batch commit failed", e))
}

/// The parts of a batch decode that don't change between messages.
struct BatchDecodeCtx<'a> {
    queue: &'a str,
    topic: &'a str,
    group: Option<&'a str>,
    #[cfg(feature = "kafka-schema-registry")]
    schema_registry: Option<&'a Arc<SchemaRegistry>>,
    #[cfg(feature = "kafka-schema-registry")]
    schema_enforcement: SchemaEnforcement,
    #[cfg(feature = "kafka-schema-registry")]
    schema_accepted: &'a [Arc<str>],
}

/// Outcome of decoding one message on its way into a batch.
enum BatchDecode<M> {
    Decoded(M),
    /// Undecodable: park the wire bytes for the DLQ under this reason, to be
    /// published once the batch's offsets commit.
    Dlq(&'static str),
}

/// Decodes one message on its way into a batch, routing anything undecodable to
/// the DLQ.
///
/// This is the batch path's equivalent of the decode stage in `run_with_inner`,
/// and deliberately matches it: with a registry configured the frame-strip +
/// subject gate + inner-codec decode runs, otherwise `T::Codec` decodes the
/// payload directly, and either way a failure ends up *in the DLQ* rather than
/// silently discarded. Batching changes when offsets are committed, not what
/// happens to a poison message.
///
/// The failure metric is emitted here, at decode time; the DLQ publish itself
/// is deferred to the commit — see [`BatchBuffer::pending_dlq`]. The caller has
/// already extended the batch's offset span, so a dropped message is still
/// committed past.
async fn decode_batch_message<T: Topic>(
    dec: &BatchDecodeCtx<'_>,
    payload_slice: &[u8],
) -> BatchDecode<T::Message> {
    #[cfg(feature = "kafka-schema-registry")]
    if let Some(registry) = dec.schema_registry {
        let codec_name = <T::Codec as crate::Codec<T::Message>>::NAME;
        let registry_result = match WireFormat::from_codec_name(codec_name) {
            Some(fmt) => {
                registry_decode::<T::Message, T::Codec>(
                    registry,
                    fmt,
                    dec.schema_enforcement,
                    dec.schema_accepted,
                    payload_slice,
                )
                .await
            }
            None => {
                tracing::error!(
                    codec = codec_name,
                    queue = dec.queue,
                    "codec has no Confluent wire format; routing to DLQ"
                );
                Ok(RegistryDecode::Dlq("schema_unsupported_codec"))
            }
        };
        return match registry_result {
            Ok(RegistryDecode::Decoded(m)) => BatchDecode::Decoded(m),
            Ok(RegistryDecode::Dlq(reason)) => {
                metrics::record_failed(
                    dec.topic,
                    dec.group,
                    metrics::FailReason::for_schema_reason(reason),
                );
                BatchDecode::Dlq(reason)
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    queue = dec.queue,
                    "failed to deserialize batch message, sending to DLQ"
                );
                metrics::record_failed(dec.topic, dec.group, metrics::FailReason::Deserialize);
                BatchDecode::Dlq("deserialization_error")
            }
        };
    }

    match <T::Codec as crate::Codec<T::Message>>::decode(payload_slice) {
        Ok(m) => BatchDecode::Decoded(m),
        Err(e) => {
            tracing::error!(
                error = %e,
                queue = dec.queue,
                "failed to deserialize batch message, sending to DLQ"
            );
            metrics::record_failed(dec.topic, dec.group, metrics::FailReason::Deserialize);
            BatchDecode::Dlq("deserialization_error")
        }
    }
}

/// The original wire bytes of one batched message.
///
/// Kept alongside the decoded message only when the topology declares a DLQ:
/// the sole consumer is the `Outcome::Reject` arm of [`flush_batch`], which
/// parks the batch in the DLQ byte-for-byte. With no DLQ configured there is
/// nowhere for those bytes to go, so nothing is retained and a batch costs
/// exactly what its decoded messages cost.
#[derive(Clone)]
struct RawMessage {
    payload: Bytes,
    key: Option<Bytes>,
    headers: Arc<HashMap<String, String>>,
}

/// A message parked for the DLQ, published once the batch it belongs to
/// commits — see [`BatchBuffer::pending_dlq`].
struct PendingDlq {
    raw: RawMessage,
    reason: String,
}

/// The in-flight batch: the decoded messages destined for the handler, the
/// offset span they cover, and the side buffers that make the flush's DLQ
/// routing correct.
struct BatchBuffer<T: Topic> {
    messages: Vec<(T::Message, MessageMetadata)>,
    /// Wire bytes, index-parallel to `messages`; empty when the topology has
    /// no DLQ. See [`RawMessage`].
    raw: Vec<RawMessage>,
    /// Poison dropped before the handler (oversize / undecodable), held until
    /// the batch's offsets actually commit.
    ///
    /// Publishing at drop time re-published the same payload on every
    /// redelivery: a handler returning `Retry` seeks back over the poison,
    /// which fails to decode again, so a sink that is down for ten minutes
    /// turns one bad message into dozens of identical DLQ copies and the
    /// dead-letter alert stops mapping to distinct bad messages. Parking until
    /// the commit makes it exactly one — and the publish still precedes the
    /// commit, so the payload is never lost.
    pending_dlq: Vec<PendingDlq>,
    /// Messages dropped pre-handler since the last flush, counted whether or
    /// not their bytes were parked in `pending_dlq`. See [`Self::flush_len`].
    dropped: usize,
    /// First offset seen per partition — where a non-`Ack` outcome seeks back
    /// to. Always has the same key set as `end`.
    start: HashMap<i32, i64>,
    /// Exclusive end offset per partition — what an `Ack` commits.
    end: HashMap<i32, i64>,
}

impl<T: Topic> BatchBuffer<T> {
    fn new(capacity: usize) -> Self {
        Self {
            messages: Vec::with_capacity(capacity),
            raw: Vec::new(),
            pending_dlq: Vec::new(),
            dropped: 0,
            start: HashMap::new(),
            end: HashMap::new(),
        }
    }

    /// Nothing to flush: no decoded messages *and* no offset span to commit
    /// past. `end` can be non-empty while `messages` is empty when every
    /// message in the span was dropped pre-handler.
    fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.end.is_empty()
    }

    /// How many messages this batch has consumed — what `max_batch_size` caps.
    ///
    /// Deliberately *not* `messages.len()`: a message dropped pre-handler still
    /// costs a parked DLQ payload and an offset in the span, and counting only
    /// the decoded ones means a run of poison never trips the size trigger at
    /// all. The batch would then grow until `max_batch_age` alone ended it —
    /// with `max_batch_age` set high (the whole point of a DB-sink batch) a
    /// poison flood holds every payload in memory for the full window, which is
    /// exactly the unboundedness `max_batch_size` exists to prevent.
    fn flush_len(&self) -> usize {
        self.messages.len() + self.dropped
    }

    /// Extends the offset span to cover `offset` on `partition`.
    fn extend_span(&mut self, partition: i32, offset: i64) {
        self.start.entry(partition).or_insert(offset);
        self.end.insert(partition, offset + 1);
    }

    fn push(&mut self, message: T::Message, metadata: MessageMetadata, raw: Option<RawMessage>) {
        self.messages.push((message, metadata));
        if let Some(raw) = raw {
            self.raw.push(raw);
        }
    }

    /// Records a message dropped pre-handler, parking its bytes for the DLQ
    /// only when there is a DLQ to publish them to.
    ///
    /// `raw` is `None` for a topology with no DLQ: `publish_to_dlq` would log
    /// "no DLQ configured" and discard, so copying and holding every poison
    /// payload until the flush buys nothing. The drop is still counted, so the
    /// span is still committed past and the size trigger still sees it.
    fn drop_message(&mut self, raw: Option<RawMessage>, reason: String) {
        self.dropped += 1;
        if let Some(raw) = raw {
            self.pending_dlq.push(PendingDlq { raw, reason });
        }
    }

    fn clear(&mut self) {
        self.messages.clear();
        self.raw.clear();
        self.pending_dlq.clear();
        self.dropped = 0;
        self.start.clear();
        self.end.clear();
    }
}

/// Publishes a batch message to the DLQ, logging a failure to do so.
///
/// A DLQ publish failure is not propagated: the offset span is committed either
/// way, so failing the whole batch here would stall forward progress on a
/// message that is by definition unprocessable (or explicitly rejected).
///
/// Returns whether the message reached the DLQ. Committing past a message the
/// DLQ never received is data loss even on a topology that declares one, so the
/// terminal-accounting caller needs the distinction — `false` is what makes it
/// settle with [`metrics::PendingDiscard::confirm_lost`] rather than the
/// topology-derived `confirm`.
async fn dlq_batch_message(
    client: &KafkaClient,
    topology: &QueueTopology,
    queue: &str,
    raw: &RawMessage,
    reason: &str,
) -> bool {
    if let Err(dlq_err) = publish_to_dlq(
        client,
        topology,
        &raw.payload,
        raw.key.as_deref(),
        &raw.headers,
        reason,
    )
    .await
    {
        tracing::error!(
            error = %dlq_err,
            queue,
            reason,
            "failed to publish bad batch message to DLQ"
        );
        return false;
    }
    true
}

/// Publishes everything parked for the DLQ during this batch, immediately
/// before its offsets commit.
async fn publish_pending_dlq(flush: &BatchFlushCtx<'_>, pending: &[PendingDlq]) {
    for item in pending {
        // The publish result is logged inside and deliberately not acted on
        // here: pre-handler drops are not part of the terminal-accounting
        // contract on this backend (they never reach `record_terminal`), so
        // there is no pending discard to settle either way.
        let _reached_dlq = dlq_batch_message(
            flush.client,
            flush.topology,
            flush.queue,
            &item.raw,
            &item.reason,
        )
        .await;
    }
}

/// Hands the buffered batch to the handler and applies the single returned
/// `Outcome`:
///
/// - `Ack` commits every partition's end offset in one batched commit.
/// - `Reject` is terminal, exactly as it is on every other consumer path: the
///   batch is published to the DLQ and its offsets commit.
/// - `Retry` / `Defer` seek every partition back to the batch's start offset so
///   the whole batch is re-delivered on the next `recv()` instead of being
///   silently skipped (offsets were never committed, but this consumer keeps
///   polling forward without a seek).
///
/// The buffer's messages may be empty while its offset span is not: every
/// message in the span was dropped before the handler (oversize /
/// undeserializable). There is nothing to hand over, but those offsets still
/// get committed — see the empty-batch arm.
///
/// Anything parked for the DLQ during the batch is published immediately before
/// the commit, and dropped un-published when the batch is redelivered instead
/// (it will be re-parked on the way back through). `redelivery_backoff`
/// escalates across consecutive `Retry`/`Defer` flushes and is reset on `Ack`.
///
/// The buffer is left empty on every path; the caller only has to disarm the
/// deadline.
/// [`invoke_handler`] for a whole batch: same panic containment, same timeout,
/// same instrumentation — in message units, with one duration observation per
/// flush rather than per message.
///
/// Without this, `run_batch` was the one handler-invoking path in the crate
/// with neither guard. A panicking flush unwound the batch task itself, so the
/// consumer died and stayed dead until an external supervisor noticed; the
/// single-message path turns the same panic into a redelivery. And because the
/// batch loop is one task, a flush future that never resolves froze offset
/// commits, rebalance handling and `shutdown.cancel()` along with it.
///
/// Both failures map to [`Outcome::Retry`], which is honest: the batch was not
/// acked, so its offsets are not committed, and the caller seeks back and
/// redelivers it. The messages themselves were moved into the abandoned
/// future — they come back from the broker, not from memory.
///
/// The handler future is built by `make_fut` *inside* the guard rather than
/// passed in ready-made. `BatchMessageHandler::handle_batch` is an ordinary
/// `fn -> impl Future`, so an implementation may panic while assembling its
/// future — before any of it is awaited. Constructing at the call site put
/// that panic outside `catch_unwind`, where it unwound `flush_batch` and
/// killed `run_batch` instead of resolving to `Retry` as the contract above
/// promises. This mirrors the single-message path, which never materializes
/// the handler future outside its own guard.
async fn invoke_batch_handler<F, Fut>(
    make_fut: F,
    timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    topic: &str,
    group: Option<&str>,
    batch_size: u64,
) -> Outcome
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Outcome>,
{
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    let _inflight = metrics::InflightGuard::from_refs_n(topic, group, batch_size);
    let started = std::time::Instant::now();
    let safe_fut = AssertUnwindSafe(async move { make_fut().await }).catch_unwind();
    let outcome = match timeout {
        Some(duration) => match tokio::time::timeout(duration, safe_fut).await {
            Ok(Ok(o)) => o,
            Ok(Err(_panic)) => {
                // No `record_failed_n` here: the single-message path emits no
                // fail metric for a panic either, and `outcome="retry"`
                // climbing without a matching `outcome="ack"` is the same
                // wedged-handler signal. Keeping the two paths identical
                // matters more than the extra label.
                tracing::warn!(topic, batch_size, "batch handler panicked, redelivering");
                Outcome::Retry
            }
            Err(_) => {
                let resolved = handler_timeout_outcome(timeout_outcome);
                tracing::warn!(
                    topic,
                    batch_size,
                    outcome = ?resolved,
                    "batch handler timed out after {duration:?}"
                );
                metrics::record_failed_n(topic, group, metrics::FailReason::Timeout, batch_size);
                resolved
            }
        },
        None => match safe_fut.await {
            Ok(o) => o,
            Err(_panic) => {
                tracing::warn!(topic, batch_size, "batch handler panicked, redelivering");
                Outcome::Retry
            }
        },
    };
    metrics::record_processing_duration(topic, group, &outcome, started.elapsed().as_secs_f64());
    metrics::record_consumed_n(topic, group, &outcome, batch_size);
    outcome
}

async fn flush_batch<T, H>(
    flush: &BatchFlushCtx<'_>,
    handler: &H,
    ctx: &H::Context,
    buffer: &mut BatchBuffer<T>,
    redelivery_backoff: &mut Backoff,
) -> Result<()>
where
    T: Topic,
    H: BatchMessageHandler<T>,
{
    let BatchFlushCtx {
        consumer,
        client,
        topology,
        queue,
        topic,
        group,
        shutdown,
        handler_timeout,
        // Not `Copy` like the rest, so bind it by reference and leave the
        // by-value destructure of the other fields intact.
        ref handler_timeout_outcome,
    } = *flush;
    let batch_size = buffer.messages.len();

    // Every message in this span was dropped pre-handler. The offsets must
    // still be committed: leaving them uncommitted replays the same poison on
    // every restart, and a poll window that is *entirely* poison would never
    // commit anything at all — the consumer would make no forward progress
    // across restarts, forever.
    if batch_size == 0 {
        if buffer.end.is_empty() {
            return Ok(());
        }
        publish_pending_dlq(flush, &buffer.pending_dlq).await;
        commit_batch_end(consumer, queue, &buffer.end).await?;
        tracing::debug!(queue, "committed offsets past a fully-dropped batch");
        buffer.clear();
        return Ok(());
    }

    let messages = std::mem::take(&mut buffer.messages);
    // Same instrumentation the single-message path gets from `invoke_handler`:
    // without it, migrating a sink from `run` to `run_batch` silently loses the
    // handler-latency histogram and the in-flight gauge — and for a batch sink,
    // flush duration is precisely the signal that says whether `max_batch_age`
    // is achievable. The duration is one observation per *flush* (the whole
    // batch), not per message.
    let outcome = invoke_batch_handler(
        // Closure, not a ready-made future: `handle_batch` itself may panic
        // while building it, and that has to happen inside the guard.
        || handler.handle_batch(messages, ctx),
        handler_timeout,
        handler_timeout_outcome.clone(),
        topic,
        group,
        batch_size as u64,
    )
    .await;
    match outcome {
        Outcome::Ack => {
            publish_pending_dlq(flush, &buffer.pending_dlq).await;
            commit_batch_end(consumer, queue, &buffer.end).await?;
            *redelivery_backoff = batch_redelivery_backoff();
            tracing::debug!(queue, batch_size, "batch committed");
            buffer.clear();
        }
        // `Reject` means terminal-do-not-retry everywhere else in shove, and
        // it has to mean that here too: routing it through the seek-back arm
        // pins the partition forever (seek, sleep, redeliver, reject, …) with
        // the payloads never reaching a DLQ and the offsets never advancing.
        Outcome::Reject => {
            // Same terminal contract the single-message path applies in
            // `route_outcome`, in batch units. The failure is counted now
            // because it already happened; the *discard* is a claim that the
            // message no longer exists, and nothing has retired these messages
            // until the commit below lands. Recording it up front would fire a
            // data-loss alert during the broker outage that made the commit
            // fail — precisely when the batch is about to be redelivered.
            let has_dlq = topology.dlq().is_some();
            // Settled only by the commit's result, so they outlive this block.
            let mut unsettled: Vec<TerminalDiscard> = Vec::new();
            // Driven off `batch_size`, not `buffer.raw.len()`. With no DLQ the
            // buffer keeps no wire bytes at all (`retain_raw` in the receive
            // loop is the same `topology.dlq().is_some()`), so `raw` is empty
            // — but there are still `batch_size` messages about to be dropped
            // on the floor, which is exactly what the discard counter exists
            // to report. Iterating `raw` would count none of them.
            //
            // With a DLQ, `push` supplies `Some(raw)` for every buffered
            // message and is the only writer, so the two stay index-parallel.
            // The `get` below is nevertheless a `match` rather than an index:
            // if that invariant ever broke, the fallback is `reached_dlq =
            // false`, which is *honest* — no bytes means no dead-letter
            // publish happened, so the message really is gone with no copy,
            // and `Lost` counts it. A panic or a silent `InDlq` would both be
            // worse. The assert catches the programming error in tests without
            // making release behaviour depend on it.
            debug_assert!(
                !has_dlq || buffer.raw.len() == batch_size,
                "`raw` is index-parallel to `messages` whenever a DLQ is declared"
            );
            for i in 0..batch_size {
                let pending =
                    metrics::record_terminal(topic, group, metrics::FailReason::Rejected, has_dlq);
                let reached_dlq = match buffer.raw.get(i) {
                    Some(raw) => dlq_batch_message(client, topology, queue, raw, "rejected").await,
                    None => false,
                };
                match reject_settlement(has_dlq, reached_dlq) {
                    RejectSettlement::InDlq => pending.survived(),
                    RejectSettlement::Retired => {
                        unsettled.push(TerminalDiscard::Retired(pending));
                    }
                    RejectSettlement::Lost => unsettled.push(TerminalDiscard::Lost(pending)),
                }
            }
            if has_dlq {
                tracing::warn!(queue, batch_size, "batch rejected, routed to the DLQ");
            } else {
                tracing::warn!(
                    queue,
                    batch_size,
                    "batch rejected but no DLQ is configured, the messages are discarded"
                );
            }
            publish_pending_dlq(flush, &buffer.pending_dlq).await;
            let committed = commit_batch_end(consumer, queue, &buffer.end).await;
            match committed {
                // The offsets advanced: these messages are genuinely gone.
                Ok(()) => unsettled.into_iter().for_each(TerminalDiscard::confirm),
                // The commit did not land, so the offsets did not advance and
                // the whole batch is redelivered. Nothing was lost.
                Err(_) => unsettled.into_iter().for_each(TerminalDiscard::survived),
            }
            committed?;
            *redelivery_backoff = batch_redelivery_backoff();
            buffer.clear();
        }
        other => {
            let delay = redelivery_backoff.next().expect("backoff is infinite");
            tracing::warn!(
                queue,
                batch_size,
                outcome = ?other,
                delay_ms = delay.as_millis() as u64,
                "batch handler returned a non-Ack outcome, redelivering the whole batch"
            );
            let mut tpl = TopicPartitionList::new();
            for (&partition, &start_offset) in &buffer.start {
                // Not `.ok()`: a partition missing from the seek list is never
                // rewound, so its consumed-but-uncommitted messages are skipped
                // permanently. Failing the flush costs a reconnect and resumes
                // from the last committed offset instead.
                tpl.add_partition_offset(queue, partition, Offset::Offset(start_offset))
                    .map_err(|e| {
                        map_kafka_error("batch seek offset list rejected a partition", e)
                    })?;
            }
            let sought = consumer
                .seek_partitions(tpl, SEEK_TIMEOUT)
                .map_err(|e| map_kafka_error("batch seek-for-redelivery failed", e))?;
            // `Ok` only means the *call* was well-formed: rdkafka reports each
            // partition's seek result on that partition's element. A partition
            // that silently failed here keeps its advanced fetch position, so
            // clearing the buffer and polling on would skip its uncommitted
            // messages — and the next `Ack` would commit past them, making the
            // loss permanent. Fail the batch instead: the reconnect rejoins at
            // the last committed offset and everything is redelivered.
            let failed = seek_errors(&sought);
            if !failed.is_empty() {
                let partitions: Vec<i32> = failed.iter().map(|(p, _)| *p).collect();
                tracing::error!(
                    queue,
                    ?partitions,
                    "batch seek-for-redelivery failed per-partition, reconnecting so the \
                     un-acked messages are redelivered instead of skipped"
                );
                buffer.clear();
                return Err(ShoveError::Connection(format!(
                    "batch seek-for-redelivery failed on {queue} partitions {partitions:?}"
                )));
            }
            // There is no per-batch retry counter at this level (see the
            // type's doc comment), so a handler stuck returning `Retry`
            // redelivers the same batch forever. The escalating delay bounds
            // the spin; `messages_consumed_total{outcome="retry"}` climbing
            // with no matching `outcome="ack"` is the alertable signal that it
            // is wedged. Shutdown cuts the delay short — otherwise a wedged
            // handler would add up to `BATCH_REDELIVERY_BACKOFF_MAX` to every
            // stop.
            buffer.clear();
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = shutdown.cancelled() => {}
            }
        }
    }
    Ok(())
}

/// Drains librdkafka's rebalance events, returning the partitions of this
/// consumer's topic whose ownership changed.
///
/// Only a partition the in-flight batch actually spans invalidates it, so the
/// caller intersects this set with the batch (see
/// [`rebalance_affects_batch`]) instead of abandoning on any event at all.
/// Under `cooperative-sticky` the callback fires on *every* member each round —
/// including members that gain a partition and lose nothing, and with an empty
/// delta — so treating a bare "something happened" as invalidation throws away
/// batches that were never at risk, redelivering them as duplicate writes.
///
/// `CommitFailed` is deliberately excluded: it is not a rebalance, and the
/// batch path commits synchronously (see [`commit_batch_end`]) so it surfaces
/// its own commit errors to the caller rather than through this channel.
fn drain_batch_rebalance_events(rx: &std_mpsc::Receiver<RebalanceEvent>) -> BTreeSet<i32> {
    let mut affected = BTreeSet::new();
    while let Ok(event) = rx.try_recv() {
        match event {
            RebalanceEvent::Assign(partitions) | RebalanceEvent::Revoke(partitions) => {
                affected.extend(partitions);
            }
            RebalanceEvent::CommitFailed(_) => {}
        }
    }
    affected
}

/// Whether a rebalance that moved `affected` invalidates a batch spanning
/// `batch_start`'s partitions.
///
/// A revoked partition takes its slice of the batch with it, and a (re-)assigned
/// one has had its fetch position reset out from under the batch; either way the
/// batch is no longer a coherent unit and is abandoned. A rebalance that touches
/// only partitions the batch never read is not this consumer's problem — its own
/// partitions keep their positions, and the commit that follows carries the new
/// generation.
fn rebalance_affects_batch(affected: &BTreeSet<i32>, batch_start: &HashMap<i32, i64>) -> bool {
    affected.iter().any(|p| batch_start.contains_key(p))
}

/// Rewinds every partition of an abandoned in-flight batch to `batch_start`,
/// best-effort and independently per partition.
///
/// This consumer runs `partition.assignment.strategy=cooperative-sticky`, so a
/// rebalance revokes only *some* partitions and leaves the rest assigned with
/// their fetch position untouched. Dropping the batch without rewinding would
/// therefore **lose** the retained partitions' consumed-but-uncommitted
/// messages: nothing committed them, the position has already advanced past
/// them, and no other member will ever be assigned them. Seeking back makes
/// them redeliver.
///
/// The seek fails for partitions that *were* revoked; that is expected, and
/// logged at debug because the member taking them over resumes from the last
/// committed offset on its own. Each partition is sought separately so one
/// revoked partition's failure cannot stop a retained partition from rewinding.
///
/// A failure on a partition this member **still holds** is a different thing
/// entirely: those messages are consumed, uncommitted, and the fetch position
/// has already moved past them, so nothing will ever redeliver them — that is
/// silent message loss, not a tolerable best-effort miss. It returns `Err` so
/// the caller tears the consumer down and reconnects; rejoining resumes every
/// partition from its last committed offset, which is at or before
/// `batch_start`, so the batch comes back instead of vanishing.
///
/// If the assignment cannot be read at all we cannot tell the two cases apart,
/// so a failure is treated as the losing one. Reconnecting when the partition
/// was in fact revoked costs a rejoin; guessing the other way costs data.
fn rewind_after_rebalance(
    consumer: &KafkaStreamConsumer,
    queue: &str,
    batch_start: &HashMap<i32, i64>,
) -> Result<()> {
    let still_assigned = assigned_partitions(consumer, queue);
    // `None` = librdkafka would not report the assignment.
    let may_be_held = |partition: i32| {
        still_assigned
            .as_ref()
            .is_none_or(|a| a.contains(&partition))
    };
    let mut lost = Vec::new();

    for (&partition, &start_offset) in batch_start {
        let mut tpl = TopicPartitionList::new();
        // A partition missing from the list is one that never gets sought, so
        // this counts as a failed rewind exactly like a failed seek does.
        if let Err(e) = tpl.add_partition_offset(queue, partition, Offset::Offset(start_offset)) {
            tracing::warn!(
                error = %e,
                queue,
                partition,
                start_offset,
                "rewind offset list rejected a partition"
            );
            lost.push(partition);
            continue;
        }

        let failed = match consumer.seek_partitions(tpl, SEEK_TIMEOUT) {
            // rdkafka only surfaces argument-level problems through the outer
            // `Result`; a per-partition seek that failed comes back as `Ok`
            // with the error set on that partition's element. Checking only
            // the outer result reads "the partition was rewound" off a
            // response that says the opposite.
            Ok(result) => {
                let elem_errors = seek_errors(&result);
                for (p, e) in &elem_errors {
                    tracing::debug!(error = %e, queue, partition = p, "rewind element reported an error");
                }
                !elem_errors.is_empty()
            }
            Err(e) => {
                tracing::debug!(error = %e, queue, partition, start_offset, "rewind seek failed");
                true
            }
        };

        if failed {
            if may_be_held(partition) {
                lost.push(partition);
            } else {
                tracing::debug!(
                    queue,
                    partition,
                    start_offset,
                    "rewind after rebalance failed on a revoked partition; \
                     the member taking it over resumes from the last committed offset"
                );
            }
        }
    }

    if lost.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        queue,
        partitions = ?lost,
        assignment_known = still_assigned.is_some(),
        "rewind after rebalance failed on partitions this member may still hold; \
         reconnecting so their consumed-but-uncommitted messages are redelivered"
    );
    Err(ShoveError::Connection(format!(
        "batch rewind after rebalance failed on {queue} partitions {lost:?}; \
         reconnecting to avoid skipping their uncommitted messages"
    )))
}

/// Per-partition errors hiding inside a successful `seek_partitions` response.
///
/// `rd_kafka_seek_partitions` reports each partition's outcome in that
/// element's `err` field and only returns a top-level error for argument-level
/// problems, so an `Ok(_)` here does **not** mean every partition was sought.
///
/// Relies on the caller passing a non-zero timeout — see [`SEEK_TIMEOUT`].
fn seek_errors(result: &TopicPartitionList) -> Vec<(i32, KafkaError)> {
    result
        .elements()
        .iter()
        .filter_map(|e| e.error().err().map(|err| (e.partition(), err)))
        .collect()
}

/// This member's currently-assigned partitions of `queue`, or `None` if
/// librdkafka would not report the assignment.
fn assigned_partitions(consumer: &KafkaStreamConsumer, queue: &str) -> Option<BTreeSet<i32>> {
    let tpl = consumer.assignment().ok()?;
    Some(
        tpl.elements()
            .iter()
            .filter(|e| e.topic() == queue)
            .map(|e| e.partition())
            .collect(),
    )
}

/// Drains librdkafka's rebalance events and abandons the in-flight batch if any
/// of them touched a partition it spans.
///
/// **Must run before the batch absorbs a freshly-received message.** The
/// rebalance callback fires *inside* `consumer.recv()`'s poll (see
/// [`RebalanceContext::pre_rebalance`]), so the `Assign` event for a
/// partition is already queued when that partition's first post-assignment
/// message comes back from the very same poll. Draining only at the top of the
/// loop reads the event one iteration too late: the message has already seeded
/// `batch_start` for its partition, so the drain finds the new partition
/// *inside* the batch's span, concludes the batch is invalid, and rewinds
/// thousands of rows belonging to partitions that never moved. That is the
/// duplicate-write bug partition-precision was meant to fix, reached by a
/// different route — and under `cooperative-sticky`, where the callback fires
/// on every member every round, gaining a partition is the common case.
///
/// Returns `Ok(true)` if the batch was abandoned, in which case the caller must
/// also disarm the age deadline. An `Err` means the rewind could not put a
/// still-held partition back and the caller must reconnect rather than keep
/// polling — see [`rewind_after_rebalance`].
fn apply_batch_rebalance<T: Topic>(
    rx: &std_mpsc::Receiver<RebalanceEvent>,
    consumer: &KafkaStreamConsumer,
    queue: &str,
    buffer: &mut BatchBuffer<T>,
) -> Result<bool> {
    let moved = drain_batch_rebalance_events(rx);
    if moved.is_empty() || buffer.is_empty() {
        return Ok(false);
    }
    if !rebalance_affects_batch(&moved, &buffer.start) {
        tracing::debug!(
            queue,
            partitions = ?moved,
            "group rebalanced on partitions the in-flight batch does not span, keeping it"
        );
        return Ok(false);
    }
    tracing::warn!(
        queue,
        batch_size = buffer.messages.len(),
        partitions = ?moved,
        "group rebalanced mid-batch, abandoning the in-flight batch uncommitted"
    );
    // Rewind before clearing: under cooperative-sticky the partitions this
    // member *keeps* would otherwise have their consumed-but-uncommitted
    // messages skipped.
    let rewound = rewind_after_rebalance(consumer, queue, &buffer.start);
    buffer.clear();
    rewound.map(|()| true)
}

// ---------------------------------------------------------------------------
// KafkaConsumer
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct KafkaConsumer {
    client: KafkaClient,
}

impl KafkaConsumer {
    pub fn new(client: KafkaClient) -> Self {
        Self { client }
    }
}

impl KafkaConsumer {
    pub async fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Kafka>,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        self.run_with_inner::<T, H>(handler, ctx, options.into_inner())
            .await
    }

    pub(crate) async fn run_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let queue = topology.queue();
        // Precedence: explicit `with_group_id` > the topology's fan-out group
        // (`{queue}-{group}-consumer`) > the topic default `{queue}-consumer`.
        let group_id = options
            .kafka_group_id
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| {
                super::constants::consumer_group_id_scoped(queue, topology.consumer_group())
            });
        let auto_offset_reset = options
            .kafka_auto_offset_reset
            .unwrap_or(KafkaAutoOffsetReset::Earliest);

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let prefetch_count = options.prefetch_count;
        let handler_timeout = options.handler_timeout;
        let handler_timeout_outcome_cfg = options.handler_timeout_outcome.clone();
        let max_message_size = options.max_message_size;
        let hold_queues = topology.hold_queues();

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();

        tracing::info!(
            queue,
            group_id,
            prefetch_count,
            max_retries,
            "Kafka consumer started"
        );

        let semaphore = Arc::new(Semaphore::new(prefetch_count as usize));
        let topic: Arc<str> = Arc::from(queue);
        let group: Option<Arc<str>> = options.consumer_group.clone();

        #[cfg(feature = "kafka-schema-registry")]
        let schema_registry = options.schema_registry.clone();
        #[cfg(feature = "kafka-schema-registry")]
        let schema_enforcement = options.schema_enforcement;
        #[cfg(feature = "kafka-schema-registry")]
        let schema_accepted: Arc<[Arc<str>]> = options
            .schema_accepted_subjects
            .clone()
            .map(Arc::from)
            .unwrap_or_else(|| Arc::from(vec![default_subject(queue)]));

        run_with_reconnect(&shutdown, queue, options.max_reconnect_attempts, || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client = client.clone();
            let processing = processing.clone();
            let shutdown = shutdown.clone();
            let group_id = group_id.clone();
            let semaphore = semaphore.clone();
            let topic = topic.clone();
            let group = group.clone();
            let handler_timeout_outcome_cfg = handler_timeout_outcome_cfg.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_registry = schema_registry.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_accepted = schema_accepted.clone();
            async move {
                // Fresh channel per (re)connect, matching the fresh
                // OffsetTracker below: rebalance events from a torn-down
                // consumer must not leak into the next connection's tracker.
                let (rebalance_tx, rebalance_rx) = std_mpsc::channel::<RebalanceEvent>();
                let consumer = create_stream_consumer(
                    client.base_config(),
                    &group_id,
                    auto_offset_reset,
                    queue,
                    rebalance_tx,
                    #[cfg(feature = "kafka-msk-iam")]
                    client.msk_context(),
                )?;
                consumer
                    .subscribe(&[queue])
                    .map_err(|e| map_kafka_error("failed to subscribe", e))?;

                let queue_owned = queue.to_string();
                // perf-K-6: OffsetTracker is touched only by this receive loop; handler
                // completions arrive via completion_tx/_rx. Drop the Mutex so the loop
                // owns the tracker directly — saves two async-lock acquisitions per
                // message (drain at top + track_received in the message branch).
                let mut tracker = OffsetTracker::new(queue_owned.clone());
                let consumer = Arc::new(consumer);
                // Bounded to prefetch_count: the semaphore already limits in-flight
                // handler tasks to this count, so the channel can never grow beyond
                // it under correct operation. An Err from try_send would indicate a
                // logic bug (handler completing without holding a permit) and is
                // surfaced immediately rather than silently accumulating (sec-K-4).
                let (completion_tx, mut completion_rx) =
                    mpsc::channel::<Completion>(prefetch_count as usize);

                // Periodic wake so rebalance events and commit retries are
                // drained even when no messages or completions arrive: the
                // callbacks push onto a channel that only the loop body
                // reads, and the loop body only runs when a select arm
                // completes. A no-op when nothing is pending.
                let mut housekeeping = tokio::time::interval(HOUSEKEEPING_INTERVAL);

                loop {
                    // Drain completed offsets, then apply any partition
                    // assignment changes BEFORE committing: a revoked
                    // partition's tracker (and any completions queued for it)
                    // is dropped so this member never commits offsets for a
                    // partition it no longer owns.
                    while let Ok(completion) = completion_rx.try_recv() {
                        tracker.mark_complete(completion);
                    }
                    let now = Instant::now();
                    tracker.apply_rebalance_events(&rebalance_rx, now);
                    if let Some(partition) = tracker.fenced(now, COMMIT_FENCE_TIMEOUT) {
                        metrics::record_backend_error(
                            metrics::BackendLabel::Kafka,
                            metrics::BackendErrorKind::Connection,
                        );
                        tracing::error!(
                            queue,
                            group_id,
                            partition,
                            stuck_for = ?COMMIT_FENCE_TIMEOUT,
                            "consumer appears fenced from its group (offset commits rejected \
                             with no resolving rebalance); forcing a clean reconnect"
                        );
                        return Err(ShoveError::Connection(format!(
                            "consumer on '{queue}' appears fenced from group '{group_id}': \
                             partition {partition} has had offset commits rejected for over \
                             {COMMIT_FENCE_TIMEOUT:?} with no resolving rebalance"
                        )));
                    }
                    if let Some((tpl, discards)) = tracker.drain_committable() {
                        if discards.is_empty() {
                            consumer
                                .commit(&tpl, CommitMode::Async)
                                .map_err(|e| map_kafka_error("commit failed", e))?;
                        } else {
                            // This batch retires messages that a no-DLQ topic
                            // will never see again, and `messages_discarded_total`
                            // may only move once that is true. `Sync` waits for
                            // the broker to finish the commit, which is the
                            // positive signal the async path cannot give.
                            let committed = consumer.commit(&tpl, CommitMode::Sync);
                            match &committed {
                                Ok(()) => {
                                    for discard in discards {
                                        discard.confirm();
                                    }
                                }
                                Err(_) => {
                                    // Ambiguous: the message may or may not be
                                    // retired, so do not claim it was.
                                    for discard in discards {
                                        discard.survived();
                                    }
                                }
                            }
                            committed.map_err(|e| map_kafka_error("commit failed", e))?;
                        }
                    }

                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(queue, "shutdown signal received, draining in-flight tasks");
                            let _ = semaphore.acquire_many(prefetch_count as u32).await;
                            // Final commit
                            while let Ok(completion) = completion_rx.try_recv() {
                                tracker.mark_complete(completion);
                            }
                            tracker.apply_rebalance_events(&rebalance_rx, Instant::now());
                            if let Some((tpl, discards)) = tracker.drain_committable() {
                                match consumer.commit(&tpl, CommitMode::Sync) {
                                    Ok(()) => {
                                        for discard in discards {
                                            discard.confirm();
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(queue, error = %e, "final offset commit failed during shutdown; batch may be redelivered");
                                        for discard in discards {
                                            discard.survived();
                                        }
                                    }
                                }
                            }
                            return Ok(());
                        }
                        // Falls through to the top-of-loop drain — see the
                        // comment on `housekeeping` above.
                        _ = housekeeping.tick() => {}
                        // Handler completions must wake the loop even when no new
                        // message arrives: with only the recv() arm, the offsets of
                        // the last in-flight batch sat uncommitted until the *next*
                        // message (or shutdown), so a crash or rebalance on an idle
                        // topic redelivered an already-processed batch. The drain at
                        // the top of the loop picks up any further completions and
                        // commits in one pass.
                        completion = completion_rx.recv() => {
                            if let Some(completion) = completion {
                                tracker.mark_complete(completion);
                            }
                        }
                        msg_result = consumer.recv() => {
                            let msg = match msg_result {
                                Ok(msg) => msg,
                                Err(e) => {
                                    tracing::error!(error = %e, queue, "consumer recv error");
                                    return Err(map_kafka_error(
                                        &format!("consumer recv error on {queue}"),
                                        e,
                                    ));
                                }
                            };

                            // perf-K-5: defer Vec<u8> allocation until after decode succeeds.
                            // Oversize and decode-fail paths use msg.payload() directly for
                            // their DLQ publish (no copy). The happy path owns the bytes
                            // only because the handler runs in a spawned task that outlives
                            // this loop iteration.
                            let payload_slice = msg.payload().unwrap_or_default();
                            let headers = extract_string_headers(&msg);
                            let partition = msg.partition();
                            let offset = msg.offset();
                            // perf-K-9: store key as bytes::Bytes — cloning into spawned
                            // delay tasks becomes a refcount bump instead of a memcpy.
                            let key = msg.key().map(Bytes::copy_from_slice);

                            // The rebalance callback runs inside consumer.recv()'s
                            // poll, so an Assign event for this partition may
                            // already be queued when its first post-reassignment
                            // message arrives from the same poll. Apply pending
                            // events BEFORE tracking — otherwise the next
                            // iteration's drain would wipe the tracker entry this
                            // message is about to seed.
                            tracker.apply_rebalance_events(&rebalance_rx, Instant::now());
                            tracker.track_received(partition, offset);

                            metrics::record_message_size(&topic, group.as_deref(), payload_slice.len());

                            // Reject oversized messages before deserialization
                            if let Err(e) = validate_message_size(payload_slice.len(), max_message_size) {
                                tracing::warn!(
                                    error = %e,
                                    queue,
                                    "rejecting oversized message to DLQ"
                                );
                                metrics::record_failed(
                                    &topic,
                                    group.as_deref(),
                                    metrics::FailReason::Oversize,
                                );
                                if let Err(dlq_err) = publish_to_dlq(
                                    &client,
                                    topology,
                                    payload_slice,
                                    key.as_deref(),
                                    &headers,
                                    &e.to_string(),
                                ).await {
                                    tracing::error!(
                                        error = %dlq_err,
                                        "failed to publish oversized message to DLQ"
                                    );
                                }
                                if completion_tx.try_send(Completion::plain(partition, offset)).is_err() {
                                    tracing::error!(partition, offset, "completion channel full — logic bug in offset tracker");
                                }
                                continue;
                            }

                            // Deserialize payload; reject to DLQ on failure.
                            // With a schema registry configured, the registry decode
                            // stage (frame-strip + subject gate + inner codec decode)
                            // runs in place of the direct codec decode. Without one,
                            // the direct decode path is byte-for-byte the same as before.
                            #[cfg(feature = "kafka-schema-registry")]
                            let payload: T::Message = if let Some(registry) = schema_registry.as_ref() {
                                let codec_name = <T::Codec as crate::Codec<T::Message>>::NAME;
                                let registry_result = match WireFormat::from_codec_name(codec_name) {
                                    Some(fmt) => registry_decode::<T::Message, T::Codec>(
                                        registry,
                                        fmt,
                                        schema_enforcement,
                                        &schema_accepted,
                                        payload_slice,
                                    ).await,
                                    None => {
                                        tracing::error!(
                                            codec = codec_name,
                                            queue,
                                            "codec has no Confluent wire format; routing to DLQ"
                                        );
                                        Ok(RegistryDecode::Dlq("schema_unsupported_codec"))
                                    }
                                };
                                match registry_result {
                                    Ok(RegistryDecode::Decoded(m)) => m,
                                    Ok(RegistryDecode::Dlq(reason)) => {
                                        metrics::record_failed(
                                            &topic,
                                            group.as_deref(),
                                            metrics::FailReason::for_schema_reason(reason),
                                        );
                                        if let Err(dlq_err) = publish_to_dlq(
                                            &client,
                                            topology,
                                            payload_slice,
                                            key.as_deref(),
                                            &headers,
                                            reason,
                                        ).await {
                                            tracing::error!(error = %dlq_err, "failed to publish bad message to DLQ");
                                        }
                                        if completion_tx.try_send(Completion::plain(partition, offset)).is_err() {
                                            tracing::error!(partition, offset, "completion channel full — logic bug in offset tracker");
                                        }
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            error = %e,
                                            queue,
                                            "failed to deserialize message, sending to DLQ"
                                        );
                                        metrics::record_failed(
                                            &topic,
                                            group.as_deref(),
                                            metrics::FailReason::Deserialize,
                                        );
                                        if let Err(dlq_err) = publish_to_dlq(
                                            &client,
                                            topology,
                                            payload_slice,
                                            key.as_deref(),
                                            &headers,
                                            "deserialization_error",
                                        ).await {
                                            tracing::error!(error = %dlq_err, "failed to publish bad message to DLQ");
                                        }
                                        if completion_tx.try_send(Completion::plain(partition, offset)).is_err() {
                                            tracing::error!(partition, offset, "completion channel full — logic bug in offset tracker");
                                        }
                                        continue;
                                    }
                                }
                            } else {
                                match <T::Codec as crate::Codec<T::Message>>::decode(payload_slice) {
                                    Ok(m) => m,
                                    Err(e) => {
                                        tracing::error!(
                                            error = %e,
                                            queue,
                                            "failed to deserialize message, sending to DLQ"
                                        );
                                        metrics::record_failed(
                                            &topic,
                                            group.as_deref(),
                                            metrics::FailReason::Deserialize,
                                        );
                                        if let Err(dlq_err) = publish_to_dlq(
                                            &client,
                                            topology,
                                            payload_slice,
                                            key.as_deref(),
                                            &headers,
                                            "deserialization_error",
                                        ).await {
                                            tracing::error!(error = %dlq_err, "failed to publish bad message to DLQ");
                                        }
                                        if completion_tx.try_send(Completion::plain(partition, offset)).is_err() {
                                            tracing::error!(partition, offset, "completion channel full — logic bug in offset tracker");
                                        }
                                        continue;
                                    }
                                }
                            };

                            #[cfg(not(feature = "kafka-schema-registry"))]
                            let payload: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(payload_slice) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        queue,
                                        "failed to deserialize message, sending to DLQ"
                                    );
                                    metrics::record_failed(
                                        &topic,
                                        group.as_deref(),
                                        metrics::FailReason::Deserialize,
                                    );
                                    if let Err(dlq_err) = publish_to_dlq(
                                        &client,
                                        topology,
                                        payload_slice,
                                        key.as_deref(),
                                        &headers,
                                        // sec-K-5: do NOT append the codec error message to
                                        // the DLQ death-reason header — serde_json errors can
                                        // carry fragments of attacker-controlled payload bytes.
                                        // The full error is recorded via tracing above.
                                        "deserialization_error",
                                    ).await {
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish bad message to DLQ"
                                        );
                                    }
                                    if completion_tx.try_send(Completion::plain(partition, offset)).is_err() {
                                        tracing::error!(partition, offset, "completion channel full — logic bug in offset tracker");
                                    }
                                    continue;
                                }
                            };

                            // Decode succeeded — copy bytes for the spawned task's
                            // route_outcome (msg goes out of scope after this loop iteration).
                            let payload_bytes = payload_slice.to_vec();

                            let metadata = build_message_metadata(&headers, false);
                            let retry_count = metadata.retry_count;

                            let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                                ShoveError::Connection("semaphore closed".to_string())
                            })?;

                            let task_client = client.clone();
                            let task_processing = processing.clone();
                            let task_semaphore = semaphore.clone();
                            let task_prefetch = prefetch_count;
                            let task_tx = completion_tx.clone();
                            let task_topic = topic.clone();
                            let task_handler = handler.clone();
                            let task_ctx = ctx.clone();
                            let task_group = group.clone();
                            let task_shutdown = shutdown.clone();
                            let task_timeout_outcome = handler_timeout_outcome_cfg.clone();

                            // perf-K-7: single spawn per message (was three).
                            // invoke_handler awaits the handler with catch_unwind +
                            // timeout in-place, then route_outcome runs in the same
                            // task — no inner spawn, no oneshot relay.
                            tokio::spawn(async move {
                                task_processing.store(true, Ordering::Release);

                                let outcome = invoke_handler(
                                    async move {
                                        task_handler
                                            .handle(payload, metadata, task_ctx.as_ref())
                                            .await
                                    },
                                    handler_timeout,
                                    task_timeout_outcome,
                                    &task_topic,
                                    task_group.as_deref(),
                                )
                                .await;

                                // sec-K-8: hand the prefetch permit to route_outcome
                                // so Retry/Defer's delayed republish spawn stays
                                // bounded by the prefetch limit instead of running
                                // outside the cap.
                                //
                                // Completion signaling now lives inside route_outcome:
                                // terminal outcomes signal sync; delayed republish
                                // signals from the spawn only on successful publish.
                                // Without this gating the offset would commit before
                                // the republish landed, silently dropping the message
                                // on republish failure.
                                let routed = route_outcome(
                                    &task_client,
                                    &task_topic,
                                    task_group.as_deref(),
                                    &payload_bytes,
                                    key,
                                    &headers,
                                    outcome,
                                    topology,
                                    retry_count,
                                    max_retries,
                                    hold_queues,
                                    Some(permit),
                                    Some((task_tx, partition, offset)),
                                    task_shutdown,
                                )
                                .await;
                                // Concurrent path: the pending discard is
                                // always settled inside `route_outcome` (see
                                // `signal_completion`), so nothing is handed
                                // back here.
                                debug_assert!(routed.1.is_none());

                                if task_semaphore.available_permits() == task_prefetch as usize {
                                    task_processing.store(false, Ordering::Release);
                                }
                            });
                        }
                    }
                }
            }
        })
        .await
    }

    /// Consume `T`'s main queue in bounded batches, flushing to `handler`
    /// once a batch reaches `max_batch_size` messages or `max_batch_age`
    /// has elapsed since the first message in it — whichever comes first.
    ///
    /// `max_batch_size` counts every message consumed, including those dropped
    /// before the handler (see *Poison handling*), so it bounds the batch's
    /// memory whatever the payloads turn out to be. The handler therefore
    /// receives *at most* `max_batch_size` messages, and fewer when some of
    /// the window was poison.
    ///
    /// This is the primitive DB-sink consumers otherwise hand-roll around
    /// the single-message API: push decoded messages into a buffer, flush
    /// on size-or-age, ack-after-flush. See [`BatchMessageHandler`] for the
    /// outcome semantics.
    ///
    /// # Outcomes
    ///
    /// - `Ack` commits the whole batch's offsets in one batched commit.
    /// - `Reject` is terminal: the batch is published to the DLQ (if the
    ///   topology declares one) and its offsets commit, matching what `Reject`
    ///   means on every other consumer path.
    /// - `Retry` / `Defer` redeliver the **entire batch** after an escalating
    ///   delay. There is no per-batch retry counter, so a handler that never
    ///   acks redelivers forever — that is the intended back-pressure for a
    ///   sink that is down, and the escalating delay bounds the spin.
    ///
    /// # Poison handling
    ///
    /// A message that fails to deserialize (including the schema-registry
    /// frame-strip and subject gate under `kafka-schema-registry`) or exceeds
    /// `options.max_message_size` is dropped from the batch and published to
    /// the DLQ byte-for-byte, and its offset is committed with the batch. The
    /// DLQ publish is deferred until the batch actually commits, so a batch
    /// that gets redelivered does not multiply its poison in the DLQ. With no
    /// DLQ declared the payload is discarded at the drop rather than buffered
    /// to the flush, but the offset is committed past either way.
    ///
    /// # Handler failures
    ///
    /// A flush that **panics** — including a panic raised while `handle_batch`
    /// is still building its future, before any of it runs — is treated as
    /// `Retry`: nothing is committed and the whole batch is redelivered. Same
    /// containment the single-message path gets, and it carries more weight
    /// here, because the batch loop is a single task: an unguarded panic would
    /// kill offset commits, rebalance handling and shutdown along with it.
    ///
    /// A flush that outruns `options.handler_timeout` (default
    /// [`DEFAULT_HANDLER_TIMEOUT`], 30 s) resolves to `Retry` too, but that is
    /// a *default*, not a rule:
    /// [`BatchConsumerOptions::with_handler_timeout_outcome`] can make it any
    /// `Outcome`, and `Ack`/`Reject` commit or dead-letter the entire batch
    /// rather than redelivering it. See that method for the batch-wide outcome
    /// table — the mapping differs from the single-message path, and `Defer`
    /// in particular is indistinguishable from `Retry` here.
    /// [`BatchConsumerOptions::without_handler_timeout`] opts out of the
    /// deadline entirely, at the cost of making a hung flush unrecoverable.
    ///
    /// # Metrics
    ///
    /// `messages_consumed_total`, `messages_failed_total` and
    /// `messages_inflight` count *messages*, comparable with the
    /// single-message consumers. `message_processing_duration_seconds` is the
    /// exception: it records one observation per **flush** (the whole batch),
    /// which is the signal that says whether `max_batch_age` is achievable.
    ///
    /// # Current limitations
    ///
    /// - A rebalance that moves a partition the in-flight batch spans abandons
    ///   the whole batch uncommitted and rewinds the partitions this member
    ///   keeps, so every message in it is redelivered — to this consumer for
    ///   retained partitions, to the new owner for revoked ones. Expect
    ///   duplicates across a rebalance, never loss. If a partition this member
    ///   still holds cannot be rewound, the consumer reconnects rather than
    ///   poll on past its uncommitted messages.
    /// - No FIFO/sequenced variant, by design. `T` is bound by
    ///   [`NotSequenced`], so a topic from `define_sequenced_topic!` is a
    ///   compile error here — use [`run_fifo`](Self::run_fifo) instead. See
    ///   `docs/design/batch-and-sequencing.md`.
    pub async fn run_batch<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptions,
    ) -> Result<()>
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>,
    {
        let topology = T::topology();
        let queue = topology.queue();
        // Mirror of the `run_fifo` guard. `NotSequenced` is the primary gate,
        // but it is a hand-implementable marker: a topic can claim it while
        // still carrying sequencing config in its topology. Consuming that in
        // batches would bypass ordering silently, so fail closed instead.
        if topology.sequencing().is_some() {
            return Err(ShoveError::Topology(format!(
                "run_batch called on {queue}, which declares sequencing config; \
                 batching and sequencing are mutually exclusive — use run_fifo"
            )));
        }
        // Same precedence as `run_with_inner`: explicit override, then the
        // topology's fan-out group, then the topic default.
        let group_id = options
            .kafka_group_id
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| {
                super::constants::consumer_group_id_scoped(queue, topology.consumer_group())
            });
        let auto_offset_reset = options
            .kafka_auto_offset_reset
            .unwrap_or(KafkaAutoOffsetReset::Earliest);
        let shutdown = options.shutdown.clone();
        let max_message_size = options.max_message_size;
        let max_batch_size = options.max_batch_size;
        let max_batch_age = options.max_batch_age;
        let handler_timeout = options.handler_timeout;
        let handler_timeout_outcome = options.handler_timeout_outcome.clone();
        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();
        let topic: Arc<str> = Arc::from(queue);
        // The `consumer_group` metric label is the shove group name, never
        // `group_id` — same as `run`/`run_fifo` above and as every other
        // backend, so one `sum by (consumer_group)` covers a topic however it
        // is consumed. The Kafka `group.id` is a backend detail; it stays in
        // the `Kafka batch consumer started` line below and in the broker's own
        // tooling.
        let group: Option<Arc<str>> = options.consumer_group.clone();

        #[cfg(feature = "kafka-schema-registry")]
        let schema_registry = options.schema_registry.clone();
        #[cfg(feature = "kafka-schema-registry")]
        let schema_enforcement = options.schema_enforcement;
        #[cfg(feature = "kafka-schema-registry")]
        let schema_accepted: Arc<[Arc<str>]> = options
            .schema_accepted_subjects
            .clone()
            .map(Arc::from)
            .unwrap_or_else(|| Arc::from(vec![default_subject(queue)]));

        tracing::info!(
            queue,
            group_id,
            max_batch_size,
            ?max_batch_age,
            "Kafka batch consumer started"
        );

        run_with_reconnect(&shutdown, queue, options.max_reconnect_attempts, || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client = client.clone();
            let shutdown = shutdown.clone();
            let group_id = group_id.clone();
            let topic = topic.clone();
            let group = group.clone();
            let handler_timeout_outcome = handler_timeout_outcome.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_registry = schema_registry.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_accepted = schema_accepted.clone();
            async move {
                let (rebalance_tx, rebalance_rx) = std_mpsc::channel::<RebalanceEvent>();
                // Arc so `commit_batch_end` can hand the consumer to
                // `spawn_blocking` for the duration of the sync commit.
                let consumer = Arc::new(create_stream_consumer(
                    client.base_config(),
                    &group_id,
                    auto_offset_reset,
                    queue,
                    rebalance_tx,
                    #[cfg(feature = "kafka-msk-iam")]
                    client.msk_context(),
                )?);
                consumer
                    .subscribe(&[queue])
                    .map_err(|e| map_kafka_error("failed to subscribe", e))?;

                let flush_ctx = BatchFlushCtx {
                    consumer: &consumer,
                    client: &client,
                    topology,
                    queue,
                    topic: topic.as_ref(),
                    group: group.as_deref(),
                    shutdown: &shutdown,
                    handler_timeout,
                    handler_timeout_outcome,
                };

                let decode_ctx = BatchDecodeCtx {
                    queue,
                    topic: topic.as_ref(),
                    group: group.as_deref(),
                    #[cfg(feature = "kafka-schema-registry")]
                    schema_registry: schema_registry.as_ref(),
                    #[cfg(feature = "kafka-schema-registry")]
                    schema_enforcement,
                    #[cfg(feature = "kafka-schema-registry")]
                    schema_accepted: schema_accepted.as_ref(),
                };

                // Retaining each message's wire bytes only pays for itself if
                // there is a DLQ to put them in on `Outcome::Reject`.
                let retain_raw = topology.dlq().is_some();
                let mut buffer: BatchBuffer<T> = BatchBuffer::new(max_batch_size);
                let mut deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
                let mut redelivery_backoff = batch_redelivery_backoff();

                // Wakes the loop when neither a message nor the batch deadline
                // will, so a rebalance that arrives while the topic is idle is
                // still noticed promptly.
                let mut housekeeping = tokio::time::interval(HOUSEKEEPING_INTERVAL);
                housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    // Catches a rebalance that lands while the topic is idle —
                    // the housekeeping tick exists to reach this line. The
                    // drain that matters for a *busy* topic is the one inside
                    // the recv() arm; see `apply_batch_rebalance`.
                    if apply_batch_rebalance(&rebalance_rx, &consumer, queue, &mut buffer)? {
                        deadline = None;
                    }

                    let sleep_until_deadline = async {
                        match deadline.as_mut() {
                            Some(d) => d.await,
                            None => std::future::pending().await,
                        }
                    };

                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            if !buffer.is_empty() {
                                flush_batch(
                                    &flush_ctx,
                                    handler.as_ref(),
                                    ctx.as_ref(),
                                    &mut buffer,
                                    &mut redelivery_backoff,
                                )
                                .await?;
                            }
                            tracing::info!(queue, "shutdown signal received, batch consumer stopped");
                            return Ok(());
                        }
                        _ = housekeeping.tick() => {
                            // Nothing to do — the drain at the top of the next
                            // iteration is the point of the wake-up.
                        }
                        () = sleep_until_deadline => {
                            flush_batch(
                                &flush_ctx,
                                handler.as_ref(),
                                ctx.as_ref(),
                                &mut buffer,
                                &mut redelivery_backoff,
                            )
                            .await?;
                            deadline = None;
                        }
                        msg_result = consumer.recv() => {
                            let msg = match msg_result {
                                Ok(msg) => msg,
                                Err(e) => {
                                    tracing::error!(error = %e, queue, "consumer recv error");
                                    return Err(map_kafka_error(
                                        &format!("consumer recv error on {queue}"),
                                        e,
                                    ));
                                }
                            };

                            let partition = msg.partition();
                            let offset = msg.offset();
                            let payload_slice = msg.payload().unwrap_or_default();

                            // Before `extend_span`, not after: the rebalance
                            // callback runs inside the poll that produced this
                            // message, so an Assign for `partition` may already
                            // be queued. Applying it first means the batch is
                            // judged against the partitions it read *before*
                            // this message, and a newly-assigned partition
                            // cannot make its own first message invalidate a
                            // batch it was never part of.
                            if apply_batch_rebalance(&rebalance_rx, &consumer, queue, &mut buffer)? {
                                deadline = None;
                            }

                            metrics::record_message_size(&topic, group.as_deref(), payload_slice.len());

                            // Extend the offset span *before* the drop checks
                            // below. A message dropped pre-handler still has to
                            // be committed past: leaving its offset out of
                            // `batch_end` replays it after every restart, and a
                            // window of nothing but poison would never commit
                            // at all. Arming the deadline here — rather than
                            // only once a message survives — is what guarantees
                            // such an all-dropped window still reaches
                            // `flush_batch` to have its offsets committed.
                            buffer.extend_span(partition, offset);
                            if deadline.is_none() {
                                deadline = Some(Box::pin(tokio::time::sleep(max_batch_age)));
                            }

                            let key = msg.key().map(Bytes::copy_from_slice);
                            let headers = extract_string_headers(&msg);

                            if let Err(e) = validate_message_size(payload_slice.len(), max_message_size) {
                                tracing::warn!(error = %e, queue, partition, offset, dlq = retain_raw, "oversized message, dropped before the handler");
                                metrics::record_failed(&topic, group.as_deref(), metrics::FailReason::Oversize);
                                buffer.drop_message(
                                    retain_raw.then(|| RawMessage {
                                        payload: Bytes::copy_from_slice(payload_slice),
                                        key,
                                        headers,
                                    }),
                                    e.to_string(),
                                );
                            } else {
                                match decode_batch_message::<T>(&decode_ctx, payload_slice).await {
                                    BatchDecode::Dlq(reason) => {
                                        buffer.drop_message(
                                            retain_raw.then(|| RawMessage {
                                                payload: Bytes::copy_from_slice(payload_slice),
                                                key,
                                                headers,
                                            }),
                                            reason.to_string(),
                                        );
                                    }
                                    BatchDecode::Decoded(decoded) => {
                                        let metadata = build_message_metadata(&headers, false);
                                        let raw = retain_raw.then(|| RawMessage {
                                            payload: Bytes::copy_from_slice(payload_slice),
                                            key,
                                            headers: headers.clone(),
                                        });
                                        buffer.push(decoded, metadata, raw);
                                    }
                                }
                            }

                            // `flush_len`, not `messages.len()`: a batch of
                            // nothing but poison has to hit the size cap too,
                            // or it grows for the whole `max_batch_age` window.
                            if buffer.flush_len() >= max_batch_size {
                                flush_batch(
                                    &flush_ctx,
                                    handler.as_ref(),
                                    ctx.as_ref(),
                                    &mut buffer,
                                    &mut redelivery_backoff,
                                )
                                .await?;
                                deadline = None;
                            }
                        }
                    }
                }
            }
        })
        .await
    }

    pub async fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Kafka>,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        self.run_fifo_with_inner::<T, H>(handler, ctx, options.into_inner())
            .await
    }

    pub(crate) async fn run_fifo_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let handles = self.spawn_fifo_shards::<T, H>(handler, ctx, options)?;
        // Kafka has exactly one FIFO task per call (single consumer, partition ordering).
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::error!("Kafka FIFO consumer task failed: {e}"),
                Err(e) => tracing::error!("Kafka FIFO consumer task panicked: {e}"),
            }
        }
        Ok(())
    }

    /// Spawn the Kafka FIFO consumer task and return its join handle.
    ///
    /// Kafka relies on partition-level ordering, so a single consumer task is
    /// sufficient — `routing_shards` is a no-op for Kafka FIFO. The returned
    /// `Vec` always contains exactly one element.
    ///
    /// `pub(crate)` visibility is required for Phase 2 (Task 16), which calls
    /// this from the consumer-group module.
    pub(crate) fn spawn_fifo_shards<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let queue = topology.queue().to_string();
        // sec-K-9: T is bound by SequencedTopic at the trait level, so this
        // is unreachable under correct callers. Returning an error instead
        // of expect()-panicking keeps misuse (e.g. from a future caller
        // path) recoverable.
        let seq_config = topology.sequencing().ok_or_else(|| {
            ShoveError::Topology(format!(
                "run_fifo called on {queue} without sequencing config"
            ))
        })?;
        // Kafka has a single FIFO task covering every assigned partition, so
        // one poison set covers every key this consumer sees. It lives outside
        // the reconnect wrapper below: a broker blip must not un-poison a key.
        //
        // Scope is this consumer task, as documented on `SequenceFailure`. A
        // partition reassigned to a sibling consumer starts with that
        // consumer's own (empty) set — the same per-process scope RabbitMQ has.
        let poisoned = PoisonedKeys::new(seq_config.on_failure());

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let handler_timeout = options.handler_timeout;
        let handler_timeout_outcome_cfg = options.handler_timeout_outcome.clone();
        let max_message_size = options.max_message_size;
        let hold_queues = topology.hold_queues();

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();

        // Kafka naturally provides per-partition ordering. A single consumer
        // processing one message at a time guarantees FIFO per key (all
        // messages for the same key land in the same partition).
        //
        // Honor the `group.id` override (registry: set by `spawn_one` from
        // `KafkaConsumerGroupConfig::with_group_id`; direct: set by
        // `ConsumerOptions::<Kafka>::with_group_id`) by rebasing onto it as
        // `{group}-fifo`. `None` falls back to the topology's fan-out group
        // (`{queue}-{group}-fifo`), then to the default `{queue}-fifo`.
        let group_id = match options.kafka_group_id.as_deref() {
            Some(base) => super::constants::fifo_group_id_from_base(base),
            None => {
                super::constants::consumer_group_id_fifo_scoped(&queue, topology.consumer_group())
            }
        };
        let auto_offset_reset = options
            .kafka_auto_offset_reset
            .unwrap_or(KafkaAutoOffsetReset::Earliest);
        let topic: Arc<str> = Arc::from(queue.as_str());
        let group: Option<Arc<str>> = options.consumer_group.clone();

        #[cfg(feature = "kafka-schema-registry")]
        let schema_registry = options.schema_registry.clone();
        #[cfg(feature = "kafka-schema-registry")]
        let schema_enforcement = options.schema_enforcement;
        #[cfg(feature = "kafka-schema-registry")]
        let schema_accepted: Arc<[Arc<str>]> = options
            .schema_accepted_subjects
            .clone()
            .map(Arc::from)
            .unwrap_or_else(|| Arc::from(vec![default_subject(&queue)]));

        tracing::info!(queue, group_id, max_retries, "Kafka FIFO consumer started");

        let shard_task = tokio::spawn(async move {
            run_with_reconnect(&shutdown, &queue, options.max_reconnect_attempts, || {
                let handler = handler.clone();
                let ctx = ctx.clone();
                let client = client.clone();
                let shutdown = shutdown.clone();
                let processing = processing.clone();
                let group_id = group_id.clone();
                let queue = queue.clone();
                let topic = topic.clone();
                let group = group.clone();
                let handler_timeout_outcome_cfg = handler_timeout_outcome_cfg.clone();
                #[cfg(feature = "kafka-schema-registry")]
                let schema_registry = schema_registry.clone();
                #[cfg(feature = "kafka-schema-registry")]
                let schema_accepted = schema_accepted.clone();
                let poisoned = poisoned.clone();
                async move {
                    // FIFO commits per message via commit_message and keeps no
                    // offset tracker, so rebalance events are irrelevant — the
                    // receiver is dropped deliberately.
                    let (rebalance_tx, _) = std_mpsc::channel::<RebalanceEvent>();
                    let consumer = create_stream_consumer(
                        client.base_config(),
                        &group_id,
                        auto_offset_reset,
                        queue.as_str(),
                        rebalance_tx,
                        #[cfg(feature = "kafka-msk-iam")]
                        client.msk_context(),
                    )?;
                    consumer
                        .subscribe(&[queue.as_str()])
                        .map_err(|e| map_kafka_error("failed to subscribe", e))?;

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                tracing::info!(queue, "shutdown signal received, stopping FIFO consumer");
                                return Ok(());
                            }
                            msg_result = consumer.recv() => {
                                let msg = match msg_result {
                                    Ok(msg) => msg,
                                    Err(e) => {
                                        tracing::error!(error = %e, queue, "FIFO consumer recv error");
                                        return Err(map_kafka_error(
                                            &format!("FIFO consumer recv error on {queue}"),
                                            e,
                                        ));
                                    }
                                };

                                // perf-K-5: FIFO is sequential — msg lives through this whole
                                // iteration (commit_message at the end), so use msg.payload()
                                // directly instead of allocating a Vec<u8> copy.
                                let payload_bytes = msg.payload().unwrap_or_default();
                                let headers = extract_string_headers(&msg);
                                // perf-K-9: Bytes for cheap refcount-clone semantics.
                                let key = msg.key().map(Bytes::copy_from_slice);
                                // The Kafka message key *is* the sequence key —
                                // the publisher sets it from `SEQUENCE_KEY_FN`.
                                let seq_key = key
                                    .as_deref()
                                    .map(|k| String::from_utf8_lossy(k).into_owned())
                                    .unwrap_or_default();

                                metrics::record_message_size(&topic, group.as_deref(), payload_bytes.len());

                                // ── FailAll: skip poisoned keys ──
                                // Inert unless this topic is configured
                                // `SequenceFailure::FailAll`.
                                if poisoned.is_poisoned(&seq_key) {
                                    tracing::warn!(
                                        queue,
                                        sequence_key = %seq_key,
                                        "sequence key poisoned (FailAll) — sending to DLQ without invoking handler"
                                    );
                                    // Collateral of an already-counted failure,
                                    // so the failure half is deliberately not
                                    // counted again — see `metrics::FailReason`.
                                    // The discard half still applies: a
                                    // cascaded message dropped with no DLQ is
                                    // just as gone as any other.
                                    let pending = metrics::pending_discard(
                                        &topic,
                                        group.as_deref(),
                                        metrics::FailReason::Rejected,
                                        topology.dlq().is_some(),
                                    );
                                    if let Err(dlq_err) = publish_to_dlq(
                                        &client,
                                        topology,
                                        payload_bytes,
                                        key.as_deref(),
                                        &headers,
                                        "rejected",
                                    ).await {
                                        // Leave the offset uncommitted so the
                                        // message is redelivered rather than
                                        // silently dropped — the same
                                        // at-least-once rule the routing path
                                        // below follows.
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish poisoned-key message to DLQ"
                                        );
                                        pending.survived();
                                        continue;
                                    }
                                    if topology.dlq().is_some() {
                                        // The message is in the DLQ, so it
                                        // exists whatever the commit does and
                                        // `confirm` could never count it.
                                        // Settling now keeps the dead-lettered
                                        // cascade off the synchronous commit a
                                        // live pending record would force —
                                        // the same short-circuit the routing
                                        // path below takes, and it matters more
                                        // here: a poisoned key drains its whole
                                        // backlog through this branch, so a
                                        // per-message round trip is a far
                                        // heavier tax than on an ordinary
                                        // terminal outcome.
                                        pending.survived();
                                        consumer.commit_message(&msg, CommitMode::Async).ok();
                                        continue;
                                    }
                                    // No DLQ: this commit is what actually
                                    // drops the message, so it decides the
                                    // discard accounting. `Async` only queues
                                    // the request and reports nothing, so
                                    // commit synchronously and settle on the
                                    // broker's answer.
                                    match consumer.commit_message(&msg, CommitMode::Sync) {
                                        Ok(()) => pending.confirm(),
                                        Err(e) => {
                                            tracing::warn!(
                                                queue,
                                                error = %e,
                                                "offset commit failed after a poisoned-key \
                                                 cascade; the message stays committed at its \
                                                 previous offset and is redelivered"
                                            );
                                            pending.survived();
                                        }
                                    }
                                    continue;
                                }

                                // Reject oversized messages before deserialization
                                if let Err(e) = validate_message_size(payload_bytes.len(), max_message_size) {
                                    tracing::warn!(
                                        error = %e,
                                        queue,
                                        "rejecting oversized FIFO message to DLQ"
                                    );
                                    metrics::record_failed(
                                        &topic,
                                        group.as_deref(),
                                        metrics::FailReason::Oversize,
                                    );
                                    poison_key(&poisoned, &seq_key, &queue);
                                    if let Err(dlq_err) = publish_to_dlq(
                                        &client,
                                        topology,
                                        payload_bytes,
                                        key.as_deref(),
                                        &headers,
                                        &e.to_string(),
                                    ).await {
                                        tracing::error!(
                                            error = %dlq_err,
                                            "failed to publish oversized message to DLQ"
                                        );
                                    }
                                    consumer.commit_message(&msg, CommitMode::Async).ok();
                                    continue;
                                }

                                // Deserialize payload; reject to DLQ on failure.
                                // With a schema registry configured, the registry decode
                                // stage runs in place of the direct codec decode; without
                                // one the direct decode path is unchanged.
                                #[cfg(feature = "kafka-schema-registry")]
                                let payload: T::Message = if let Some(registry) = schema_registry.as_ref() {
                                    let codec_name = <T::Codec as crate::Codec<T::Message>>::NAME;
                                    let registry_result = match WireFormat::from_codec_name(codec_name) {
                                        Some(fmt) => registry_decode::<T::Message, T::Codec>(
                                            registry,
                                            fmt,
                                            schema_enforcement,
                                            &schema_accepted,
                                            payload_bytes,
                                        ).await,
                                        None => {
                                            tracing::error!(
                                                codec = codec_name,
                                                queue,
                                                "codec has no Confluent wire format; routing to DLQ"
                                            );
                                            Ok(RegistryDecode::Dlq("schema_unsupported_codec"))
                                        }
                                    };
                                    match registry_result {
                                        Ok(RegistryDecode::Decoded(m)) => m,
                                        Ok(RegistryDecode::Dlq(reason)) => {
                                            metrics::record_failed(
                                                &topic,
                                                group.as_deref(),
                                                metrics::FailReason::for_schema_reason(reason),
                                            );
                                            poison_key(&poisoned, &seq_key, &queue);
                                            if let Err(dlq_err) = publish_to_dlq(
                                                &client,
                                                topology,
                                                payload_bytes,
                                                key.as_deref(),
                                                &headers,
                                                reason,
                                            ).await {
                                                tracing::error!(error = %dlq_err, "failed to publish bad message to DLQ");
                                            }
                                            consumer.commit_message(&msg, CommitMode::Async).ok();
                                            continue;
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                error = %e,
                                                queue,
                                                "failed to deserialize FIFO message, sending to DLQ"
                                            );
                                            metrics::record_failed(
                                                &topic,
                                                group.as_deref(),
                                                metrics::FailReason::Deserialize,
                                            );
                                            poison_key(&poisoned, &seq_key, &queue);
                                            if let Err(dlq_err) = publish_to_dlq(
                                                &client,
                                                topology,
                                                payload_bytes,
                                                key.as_deref(),
                                                &headers,
                                                "deserialization_error",
                                            ).await {
                                                tracing::error!(error = %dlq_err, "failed to publish bad message to DLQ");
                                            }
                                            consumer.commit_message(&msg, CommitMode::Async).ok();
                                            continue;
                                        }
                                    }
                                } else {
                                    match <T::Codec as crate::Codec<T::Message>>::decode(payload_bytes) {
                                        Ok(m) => m,
                                        Err(e) => {
                                            tracing::error!(
                                                error = %e,
                                                queue,
                                                "failed to deserialize FIFO message, sending to DLQ"
                                            );
                                            metrics::record_failed(
                                                &topic,
                                                group.as_deref(),
                                                metrics::FailReason::Deserialize,
                                            );
                                            poison_key(&poisoned, &seq_key, &queue);
                                            if let Err(dlq_err) = publish_to_dlq(
                                                &client,
                                                topology,
                                                payload_bytes,
                                                key.as_deref(),
                                                &headers,
                                                "deserialization_error",
                                            ).await {
                                                tracing::error!(error = %dlq_err, "failed to publish bad message to DLQ");
                                            }
                                            consumer.commit_message(&msg, CommitMode::Async).ok();
                                            continue;
                                        }
                                    }
                                };

                                #[cfg(not(feature = "kafka-schema-registry"))]
                                let payload: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(payload_bytes) {
                                    Ok(m) => m,
                                    Err(e) => {
                                        tracing::error!(
                                            error = %e,
                                            queue,
                                            "failed to deserialize FIFO message, sending to DLQ"
                                        );
                                        metrics::record_failed(
                                            &topic,
                                            group.as_deref(),
                                            metrics::FailReason::Deserialize,
                                        );
                                        poison_key(&poisoned, &seq_key, &queue);
                                        if let Err(dlq_err) = publish_to_dlq(
                                            &client,
                                            topology,
                                            payload_bytes,
                                            key.as_deref(),
                                            &headers,
                                            // sec-K-5: do NOT append the codec error message to
                                        // the DLQ death-reason header — serde_json errors can
                                        // carry fragments of attacker-controlled payload bytes.
                                        // The full error is recorded via tracing above.
                                        "deserialization_error",
                                        ).await {
                                            tracing::error!(
                                                error = %dlq_err,
                                                "failed to publish bad message to DLQ"
                                            );
                                        }
                                        consumer.commit_message(&msg, CommitMode::Async).ok();
                                        continue;
                                    }
                                };

                                let metadata = build_message_metadata(&headers, false);
                                let retry_count = metadata.retry_count;

                                processing.store(true, Ordering::Release);

                                // perf-K-7: call invoke_handler directly (no inner spawn).
                                // FIFO awaits the outcome inline anyway, so no task alloc
                                // is needed for panic isolation — catch_unwind covers it.
                                let handler_clone = handler.clone();
                                let ctx_clone = ctx.clone();
                                let outcome = invoke_handler(
                                    async move {
                                        handler_clone
                                            .handle(payload, metadata, ctx_clone.as_ref())
                                            .await
                                    },
                                    handler_timeout,
                                    handler_timeout_outcome_cfg.clone(),
                                    &topic,
                                    group.as_deref(),
                                )
                                .await;
                                let outcome = adjust_outcome_for_fifo(outcome);

                                                                // FailAll: a DLQ-terminal outcome poisons the
                                // key, so every later message for it is
                                // dead-lettered instead of handled.
                                if matches!(
                                    decide_retry(&outcome, retry_count, max_retries),
                                    RetryDecision::Dlq { .. }
                                ) {
                                    poison_key(&poisoned, &seq_key, &queue);
                                }

                                let (route_ok, pending) = route_outcome(
                                    &client,
                                    &queue,
                                    group.as_deref(),
                                    payload_bytes,
                                    key,
                                    &headers,
                                    outcome,
                                    topology,
                                    retry_count,
                                    max_retries,
                                    hold_queues,
                                    // FIFO is sequential — no prefetch semaphore in play.
                                    None,
                                    // No async completion: route_outcome awaits
                                    // the Retry/Defer republish inline and reports
                                    // via the bool return whether the message has
                                    // been retired (Ack / DLQ / republished OK).
                                    None,
                                    shutdown.clone(),
                                )
                                .await;

                                // Only commit when the message has been retired —
                                // skipping the commit on republish failure or
                                // shutdown is how at-least-once delivery survives
                                // a missed delayed publish on the FIFO path.
                                if route_ok {
                                    match pending {
                                        // A terminal outcome: this commit is
                                        // what actually drops the message, so
                                        // it decides the discard accounting.
                                        // `Async` only queues the request and
                                        // reports nothing, so commit
                                        // synchronously here and settle on the
                                        // broker's answer. Terminal outcomes
                                        // are rare (rejected, or retries
                                        // exhausted), so the extra round trip
                                        // stays off the hot path.
                                        Some(pending) => {
                                            match consumer.commit_message(&msg, CommitMode::Sync) {
                                                Ok(()) => pending.confirm(),
                                                Err(e) => {
                                                    tracing::warn!(
                                                        queue,
                                                        error = %e,
                                                        "offset commit failed after a terminal \
                                                         outcome; the message stays committed at \
                                                         its previous offset and is redelivered"
                                                    );
                                                    pending.survived();
                                                }
                                            }
                                        }
                                        None => {
                                            consumer.commit_message(&msg, CommitMode::Async).ok();
                                        }
                                    }
                                }
                                processing.store(false, Ordering::Release);
                            }
                        }
                    }
                }
            })
            .await
        });

        Ok(vec![shard_task])
    }

    /// Drain a Kafka FIFO consumer with a timeout, mirroring
    /// [`ConsumerSupervisor::run_until_timeout`] for sequenced topics.
    ///
    /// Spawns a single FIFO task (Kafka uses partition ordering rather than
    /// routing shards). Races `signal` against the task exiting on its own.
    /// When `signal` resolves, cancels `options.shutdown` and waits up to
    /// `drain_timeout` for the task to finish; a surviving task is aborted
    /// and reflected in `timed_out`.
    pub async fn run_fifo_until_timeout<T, H, S>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Kafka>,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
        S: Future<Output = ()> + Send + 'static,
    {
        self.run_fifo_until_timeout_with_inner::<T, H, S>(
            handler,
            ctx,
            options.into_inner(),
            signal,
            drain_timeout,
        )
        .await
    }

    pub(crate) async fn run_fifo_until_timeout_with_inner<T, H, S>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
        S: Future<Output = ()> + Send + 'static,
    {
        let shutdown = options.shutdown.clone();
        let handles = match self.spawn_fifo_shards::<T, H>(handler, ctx, options) {
            Ok(h) => h,
            Err(e) => {
                tracing::error!(error = %e, "run_fifo_until_timeout: shard spawn failed");
                return SupervisorOutcome {
                    errors: 1,
                    panics: 0,
                    timed_out: false,
                };
            }
        };
        drive_fifo_until_timeout(handles, shutdown, signal, drain_timeout).await
    }

    /// Public DLQ entrypoint with default options. Equivalent to
    /// [`run_dlq_with_options`](Self::run_dlq_with_options) with
    /// `ConsumerOptions::new()`; kept for callers who don't need to thread
    /// per-consumer options into the DLQ loop.
    pub async fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        self.run_dlq_with_options::<T, H>(handler, ctx, crate::ConsumerOptions::<Kafka>::new())
            .await
    }

    /// Public DLQ entrypoint that threads [`ConsumerOptions`] into the DLQ
    /// loop. Use [`ConsumerOptions::<Kafka>::with_group_id`] here to drain the
    /// DLQ under a custom group (`{group}-dlq`) so two independent services
    /// draining the same DLQ topic do not compete for its partitions.
    ///
    /// [`ConsumerOptions`]: crate::ConsumerOptions
    /// [`ConsumerOptions::<Kafka>::with_group_id`]: crate::ConsumerOptions::with_group_id
    pub async fn run_dlq_with_options<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: crate::ConsumerOptions<Kafka>,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        self.run_dlq_with_inner::<T, H>(handler, ctx, options.into_inner())
            .await
    }

    pub(crate) async fn run_dlq_with_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let dlq = topology.dlq().ok_or_else(|| {
            ShoveError::Topology("run_dlq requires a DLQ to be configured".into())
        })?;

        // Honor the `group.id` override (set via
        // `ConsumerOptions::<Kafka>::with_group_id`) by rebasing the DLQ group
        // onto it as `{group}-dlq`, so a custom group does not re-collide on
        // the default `{dlq}-consumer`. `None` keeps that default.
        let dlq_group_id = match options.kafka_group_id.as_deref() {
            Some(base) => super::constants::dlq_group_id_from_base(base),
            None => super::constants::dlq_consumer_group_id(dlq),
        };
        let shutdown = self.client.shutdown_token();
        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();
        // sec-K-7: respect the same max_message_size the main consumer uses
        // rather than the DEFAULT_MAX_MESSAGE_SIZE constant.
        let max_message_size = options.max_message_size;
        // `shove_message_size_bytes` labels a DLQ drain with the SOURCE topic,
        // not the DLQ name, and with whatever `consumer_group` the main loop
        // for this topic would carry — never the internal `{dlq}-consumer`
        // group id below. Two reasons the label must not switch to `dlq`:
        // Redis already drains its DLQ through `run_stream_loop`, which
        // labels every metric `topology.queue()` whichever stream it is
        // reading, so a DLQ name here would make `topic` mean two different
        // things depending on the backend; and a per-topic size profile is
        // only summable across the main and DLQ paths if both carry the same
        // label. The DLQ drain stays distinguishable through the metrics that
        // exist to name it (`shove_messages_discarded_total`, the DLQ backlog
        // gauge) rather than by overloading `topic` here.
        let topic: Arc<str> = Arc::from(topology.queue());
        let group: Option<Arc<str>> = options.consumer_group.clone();

        #[cfg(feature = "kafka-schema-registry")]
        let schema_registry = options.schema_registry.clone();
        #[cfg(feature = "kafka-schema-registry")]
        let schema_enforcement = options.schema_enforcement;
        #[cfg(feature = "kafka-schema-registry")]
        let schema_accepted: Arc<[Arc<str>]> = options
            .schema_accepted_subjects
            .clone()
            .map(Arc::from)
            .unwrap_or_else(|| Arc::from(vec![default_subject(dlq)]));

        tracing::info!(dlq, group_id = dlq_group_id, "Kafka DLQ consumer started");

        run_with_reconnect(&shutdown, dlq, None, || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client_clone = client.clone();
            let shutdown = shutdown.clone();
            let dlq_group_id = dlq_group_id.clone();
            let topic = topic.clone();
            let group = group.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_registry = schema_registry.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_accepted = schema_accepted.clone();
            async move {
                // DLQ consumers always drain from the earliest available
                // offset — skipping dead messages on a tail-only join would
                // silently lose audit data the operator explicitly opted in
                // to. Keep the policy fixed regardless of the user's main
                // consumer `auto_offset_reset` override.
                //
                // The DLQ loop commits per message via commit_message and
                // keeps no offset tracker, so rebalance events are irrelevant
                // — the receiver is dropped deliberately.
                let (rebalance_tx, _) = std_mpsc::channel::<RebalanceEvent>();
                let consumer = create_stream_consumer(
                    client_clone.base_config(),
                    &dlq_group_id,
                    KafkaAutoOffsetReset::Earliest,
                    dlq,
                    rebalance_tx,
                    #[cfg(feature = "kafka-msk-iam")]
                    client_clone.msk_context(),
                )?;
                consumer
                    .subscribe(&[dlq])
                    .map_err(|e| map_kafka_error("failed to subscribe to DLQ", e))?;

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(dlq, "shutdown signal received, stopping DLQ consumer");
                            return Ok(());
                        }
                        msg_result = consumer.recv() => {
                            let msg = match msg_result {
                                Ok(msg) => msg,
                                Err(e) => {
                                    tracing::error!(error = %e, dlq, "DLQ consumer recv error");
                                    return Err(map_kafka_error(
                                        &format!("DLQ consumer recv error on {dlq}"),
                                        e,
                                    ));
                                }
                            };

                            // perf-K-5: msg lives through commit_message at the end of this
                            // iteration and we never spawn — decode from msg.payload() directly
                            // instead of allocating a Vec<u8> copy.
                            let payload_bytes = msg.payload().unwrap_or_default();
                            let headers = extract_string_headers(&msg);

                            // Before the size gate, exactly as on the main loop: the
                            // histogram describes what arrived on the wire, so the
                            // payload the gate is about to discard is precisely the
                            // sample an operator sizing `max_message_size` needs.
                            metrics::record_message_size(&topic, group.as_deref(), payload_bytes.len());

                            // sec-K-7: honor options.max_message_size (same as the main
                            // consumer) instead of the DEFAULT_MAX_MESSAGE_SIZE constant.
                            // None means no limit.
                            if let Some(max) = max_message_size
                                && payload_bytes.len() > max
                            {
                                tracing::warn!(
                                    bytes = payload_bytes.len(),
                                    max,
                                    dlq,
                                    "oversized DLQ message — discarding"
                                );
                                consumer.commit_message(&msg, CommitMode::Async).ok();
                                continue;
                            }

                            // Deserialize payload; on failure, log and ack anyway.
                            // With a schema registry configured, the registry decode
                            // stage runs in place of the direct codec decode. The DLQ
                            // consumer never re-publishes — an undecodable dead message
                            // is logged and acked, exactly as before. Without a registry
                            // the direct decode path is unchanged.
                            #[cfg(feature = "kafka-schema-registry")]
                            let payload: T::Message = if let Some(registry) = schema_registry.as_ref() {
                                let codec_name = <T::Codec as crate::Codec<T::Message>>::NAME;
                                let registry_result = match WireFormat::from_codec_name(codec_name) {
                                    Some(fmt) => registry_decode::<T::Message, T::Codec>(
                                        registry,
                                        fmt,
                                        schema_enforcement,
                                        &schema_accepted,
                                        payload_bytes,
                                    ).await,
                                    None => {
                                        tracing::error!(
                                            codec = codec_name,
                                            dlq,
                                            "codec has no Confluent wire format; acking dead message anyway"
                                        );
                                        Ok(RegistryDecode::Dlq("schema_unsupported_codec"))
                                    }
                                };
                                match registry_result {
                                    Ok(RegistryDecode::Decoded(m)) => m,
                                    Ok(RegistryDecode::Dlq(reason)) => {
                                        tracing::error!(
                                            reason,
                                            dlq,
                                            "schema decode rejected dead message, acking anyway"
                                        );
                                        consumer.commit_message(&msg, CommitMode::Async).ok();
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            error = %e,
                                            dlq,
                                            "failed to deserialize DLQ message, acking anyway"
                                        );
                                        consumer.commit_message(&msg, CommitMode::Async).ok();
                                        continue;
                                    }
                                }
                            } else {
                                match <T::Codec as crate::Codec<T::Message>>::decode(payload_bytes) {
                                    Ok(m) => m,
                                    Err(e) => {
                                        tracing::error!(
                                            error = %e,
                                            dlq,
                                            "failed to deserialize DLQ message, acking anyway"
                                        );
                                        consumer.commit_message(&msg, CommitMode::Async).ok();
                                        continue;
                                    }
                                }
                            };

                            #[cfg(not(feature = "kafka-schema-registry"))]
                            let payload: T::Message = match <T::Codec as crate::Codec<T::Message>>::decode(payload_bytes) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        dlq,
                                        "failed to deserialize DLQ message, acking anyway"
                                    );
                                    consumer.commit_message(&msg, CommitMode::Async).ok();
                                    continue;
                                }
                            };

                            let metadata = build_dead_metadata(&headers);
                            handler.handle_dead(payload, metadata, ctx.as_ref()).await;

                            if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                                tracing::error!(error = %e, dlq, "failed to commit DLQ message");
                            }
                        }
                    }
                }
            }
        })
        .await
    }
}

#[cfg(test)]
mod offset_tracker_tests {
    use super::*;

    fn committed_offset(tpl: &TopicPartitionList, partition: i32) -> Option<i64> {
        tpl.elements()
            .iter()
            .find(|e| e.partition() == partition)
            .and_then(|e| match e.offset() {
                Offset::Offset(o) => Some(o),
                _ => None,
            })
    }

    /// `drain_committable` for the offset-sequencing tests, which attach no
    /// terminal discards — asserting the side channel stays empty keeps them
    /// honest about that.
    fn drain_tpl(tracker: &mut OffsetTracker) -> Option<TopicPartitionList> {
        tracker.drain_committable().map(|(tpl, discards)| {
            assert!(
                discards.is_empty(),
                "no terminal discards were attached in this test"
            );
            tpl
        })
    }

    /// Regression: the normal contiguous drain still works — out-of-order
    /// completions commit only up to the first gap, then advance once the
    /// gap fills.
    #[test]
    fn contiguous_drain_advances_past_gaps_only_when_filled() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(Completion::plain(0, 2));
        tracker.mark_complete(Completion::plain(0, 0));

        let tpl = drain_tpl(&mut tracker).expect("offset 0 is committable");
        assert_eq!(committed_offset(&tpl, 0), Some(1), "gap at 1 blocks 2");

        tracker.mark_complete(Completion::plain(0, 1));
        let tpl = drain_tpl(&mut tracker).expect("gap filled");
        assert_eq!(committed_offset(&tpl, 0), Some(3));
    }

    /// After remove + re-track (a partition revoked and reassigned), the
    /// tracker re-seeds `next_to_commit` from the newly delivered offset
    /// instead of stalling on the stale pre-revocation seed.
    #[test]
    fn remove_then_track_reseeds_next_to_commit() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 5);
        tracker.mark_complete(Completion::plain(0, 5));
        let tpl = drain_tpl(&mut tracker).expect("initial commit");
        assert_eq!(committed_offset(&tpl, 0), Some(6));

        // Partition moves away (another member commits 6..99), then returns.
        tracker.remove(0);
        tracker.track_received(0, 100);
        tracker.mark_complete(Completion::plain(0, 100));
        let tpl = drain_tpl(&mut tracker)
            .expect("re-seeded partition must commit without waiting for 6..100");
        assert_eq!(committed_offset(&tpl, 0), Some(101));
    }

    /// Completions that arrive after the partition's tracker was removed
    /// (late completions from the previous assignment epoch) are dropped and
    /// never produce a commit.
    #[test]
    fn completions_after_remove_are_dropped() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 5);
        tracker.remove(0);
        tracker.mark_complete(Completion::plain(0, 5));
        assert!(
            drain_tpl(&mut tracker).is_none(),
            "removed partition must not commit"
        );
    }

    // -- terminal discard accounting (CAF-35) --
    //
    // `messages_discarded_total` promises every increment is a message that
    // no longer exists. On the concurrent path the offset is committed by
    // this tracker, so the discard cannot be settled at hand-off — it has to
    // ride the offset and surface only on the drain whose commit covers it.

    fn terminal(offset: i64) -> Completion {
        Completion {
            partition: 0,
            offset,
            discard: Some(TerminalDiscard::Retired(metrics::record_terminal(
                "q",
                None,
                metrics::FailReason::Rejected,
                false,
            ))),
        }
    }

    /// The discard surfaces on the drain that commits past its offset, and
    /// not before — a gap holding the commit back also holds the accounting.
    #[test]
    fn terminal_discard_surfaces_only_once_its_offset_is_committable() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(terminal(1));

        assert!(
            tracker.drain_committable().is_none(),
            "offset 0 is still in flight, so nothing commits and nothing is counted"
        );

        tracker.mark_complete(Completion::plain(0, 0));
        let (tpl, discards) = tracker
            .drain_committable()
            .expect("the gap filled, so 0 and 1 commit together");
        assert_eq!(committed_offset(&tpl, 0), Some(2));
        assert_eq!(
            discards.len(),
            1,
            "the discard riding offset 1 is now covered by the commit"
        );
        for discard in discards {
            discard.survived();
        }
    }

    /// An Ack-only batch reports no discards, which is what lets the receive
    /// loop keep committing asynchronously in the common case.
    #[test]
    fn a_batch_without_terminal_outcomes_reports_no_discards() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(Completion::plain(0, 0));
        let (_, discards) = tracker.drain_committable().expect("offset 0 commits");
        assert!(discards.is_empty());
    }

    /// A partition revoked before its commit lands takes the pending discard
    /// with it: whoever owns the partition now will redeliver the message, so
    /// claiming it was retired would be a lie. Undercounting is the safe
    /// direction.
    #[test]
    fn revoking_a_partition_drops_its_uncommitted_discards() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(terminal(0));
        tracker.remove(0);
        assert!(
            tracker.drain_committable().is_none(),
            "a revoked partition neither commits nor counts"
        );
    }

    /// A completion arriving for a partition this member no longer tracks is
    /// likewise never counted.
    #[test]
    fn a_completion_for_an_untracked_partition_is_not_counted() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.mark_complete(terminal(0));
        assert!(tracker.drain_committable().is_none());
    }

    /// A completion below the seed (stale offset from before reassignment)
    /// is ignored — it must neither commit nor linger in `completed`.
    #[test]
    fn mark_complete_below_seed_is_ignored() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 10);
        tracker.mark_complete(Completion::plain(0, 5));
        assert!(
            drain_tpl(&mut tracker).is_none(),
            "stale completion must not commit"
        );

        tracker.mark_complete(Completion::plain(0, 10));
        let tpl = drain_tpl(&mut tracker).expect("seed offset completes");
        assert_eq!(
            committed_offset(&tpl, 0),
            Some(11),
            "stale offset 5 must not have corrupted the contiguous run"
        );
    }

    /// A failed async commit (CommitFailed event) makes the partition
    /// re-offer its current commit position on the next drain even though no
    /// new completions arrived — and only once (the flag clears).
    #[test]
    fn commit_failed_re_offers_current_position_once() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(Completion::plain(0, 0));
        let tpl = drain_tpl(&mut tracker).expect("initial commit");
        assert_eq!(committed_offset(&tpl, 0), Some(1));

        // The async commit of offset 1 was rejected mid-rebalance.
        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, Instant::now());
        let tpl = drain_tpl(&mut tracker)
            .expect("failed commit must be re-offered without new completions");
        assert_eq!(committed_offset(&tpl, 0), Some(1));

        assert!(
            drain_tpl(&mut tracker).is_none(),
            "retry flag must clear after one re-offer"
        );
    }

    /// Any rebalance event re-offers every retained partition's current
    /// position: async commits in flight during the rebalance can be dropped
    /// by librdkafka without an error, so positions are re-asserted once the
    /// group settles.
    #[test]
    fn rebalance_event_re_offers_retained_partition_positions() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(4, 0);
        tracker.mark_complete(Completion::plain(4, 0));
        let tpl = drain_tpl(&mut tracker).expect("initial commit");
        assert_eq!(committed_offset(&tpl, 4), Some(1));

        // Another member joins: partitions 0-3 move away; the commit of
        // (4, 1) submitted during the rebalance may have been dropped.
        tx.send(RebalanceEvent::Revoke(vec![0, 1, 2, 3])).unwrap();
        tracker.apply_rebalance_events(&rx, Instant::now());
        let tpl = drain_tpl(&mut tracker).expect("retained partition must re-offer its position");
        assert_eq!(committed_offset(&tpl, 4), Some(1));

        assert!(
            drain_tpl(&mut tracker).is_none(),
            "re-offer happens once per rebalance event"
        );
    }

    /// A commit failure for a partition whose tracker was removed (revoked
    /// meanwhile) must NOT resurrect a commit — this member no longer owns
    /// the partition.
    #[test]
    fn commit_failed_after_revoke_is_dropped() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(Completion::plain(0, 0));
        let _ = drain_tpl(&mut tracker).expect("initial commit");

        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, Instant::now());
        assert!(
            drain_tpl(&mut tracker).is_none(),
            "no retry for a revoked partition"
        );
    }

    /// Assign and Revoke events drained from the channel both remove the
    /// affected partitions' state; other partitions are untouched.
    #[test]
    fn apply_rebalance_events_removes_only_listed_partitions() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 5);
        tracker.track_received(1, 7);
        tracker.track_received(2, 9);

        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        tx.send(RebalanceEvent::Assign(vec![1])).unwrap();
        tracker.apply_rebalance_events(&rx, Instant::now());

        tracker.mark_complete(Completion::plain(0, 5));
        tracker.mark_complete(Completion::plain(1, 7));
        tracker.mark_complete(Completion::plain(2, 9));
        let tpl = drain_tpl(&mut tracker).expect("partition 2 commits");
        assert_eq!(committed_offset(&tpl, 0), None, "revoked: removed");
        assert_eq!(committed_offset(&tpl, 1), None, "reassigned: removed");
        assert_eq!(committed_offset(&tpl, 2), Some(10), "untouched partition");
    }

    // -- fenced-member detection (CAF-25 / uc04) --

    /// A clean tracker (never dirty) is never fenced, regardless of `now`.
    #[test]
    fn fenced_is_none_for_a_clean_tracker() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let now = Instant::now();
        assert_eq!(tracker.fenced(now, Duration::from_secs(60)), None);
        assert_eq!(
            tracker.fenced(now + Duration::from_secs(1_000), Duration::from_secs(60)),
            None
        );
    }

    /// A single rejected commit is well within a normal rebalance's timing
    /// budget — must not trip the fenced check.
    #[test]
    fn fenced_is_none_within_threshold() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();
        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t0);

        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(59), Duration::from_secs(60)),
            None
        );
    }

    /// Offset commits rejected continuously (re-offered every drain, failing
    /// again every time) past the threshold with no resolving rebalance
    /// trips the fenced check on the affected partition.
    #[test]
    fn fenced_fires_after_sustained_unresolved_commit_failures() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t0);

        // Every re-offer fails again immediately — no gap wide enough to
        // resolve the streak, and no revoke/assign ever arrives.
        let t1 = t0 + Duration::from_secs(30);
        let _ = drain_tpl(&mut tracker);
        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t1);

        let t2 = t0 + Duration::from_secs(61);
        assert_eq!(
            tracker.fenced(t2, Duration::from_secs(60)),
            Some(0),
            "partition 0 has been continuously dirty since t0"
        );
    }

    /// A *single* quiet drain does not resolve the streak: `commit_callback`
    /// only fires on failure, so silence one iteration after a re-offer is
    /// not yet evidence of success — a genuinely wedged partition looks
    /// identical at this point, and its next failure callback may not land
    /// for another iteration.
    #[test]
    fn fenced_streak_survives_a_single_quiet_drain() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t0);
        let _ = drain_tpl(&mut tracker); // re-offers the failed commit
        let _ = drain_tpl(&mut tracker); // 1st quiet drain: not yet resolved

        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(61), Duration::from_secs(60)),
            Some(0),
            "one quiet drain is within a commit round trip; streak must keep aging"
        );
    }

    /// A partition whose re-offered commit actually landed must NOT age into
    /// a fence. Success is silent (`commit_callback` fires only on failure),
    /// so `QUIET_DRAINS_TO_RESOLVE` consecutive quiet drains are what stand
    /// in for the missing positive ack.
    ///
    /// Regression: `dirty_since` was previously only ever cleared by a
    /// resolving rebalance, so a single transient rejection on an otherwise
    /// healthy consumer forced a spurious reconnect exactly
    /// `COMMIT_FENCE_TIMEOUT` later — rebalances are rare in a stable group,
    /// so nothing intervened.
    #[test]
    fn fenced_does_not_fire_after_a_transient_failure_recovers() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t0);
        let _ = drain_tpl(&mut tracker); // re-offers; this one succeeds

        // Two consecutive quiet drains: the re-offer must have landed.
        for _ in 0..QUIET_DRAINS_TO_RESOLVE {
            let _ = drain_tpl(&mut tracker);
        }

        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(61), Duration::from_secs(60)),
            None,
            "a recovered partition must not be reported as fenced"
        );
        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(86_400), Duration::from_secs(60)),
            None,
            "and must not age into one later either"
        );
    }

    /// After a recovery, a later unrelated failure starts a *fresh* streak
    /// rather than inheriting the resolved one's age — otherwise the first
    /// rejection after a long healthy period would fence the consumer
    /// instantly.
    #[test]
    fn fenced_measures_a_later_failure_from_its_own_start() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t0);
        let _ = drain_tpl(&mut tracker);
        for _ in 0..QUIET_DRAINS_TO_RESOLVE {
            let _ = drain_tpl(&mut tracker);
        }

        // An unrelated rejection an hour later.
        let t1 = t0 + Duration::from_secs(3_600);
        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t1);

        assert_eq!(
            tracker.fenced(t1 + Duration::from_secs(30), Duration::from_secs(60)),
            None,
            "the new streak is 30s old, not an hour"
        );
        assert_eq!(
            tracker.fenced(t1 + Duration::from_secs(61), Duration::from_secs(60)),
            Some(0),
            "and still fences once the new streak itself crosses the threshold"
        );
    }

    /// A resolving rebalance (revoke + reassign) drops and recreates the
    /// tracker, which clears any in-progress streak even if the old one had
    /// already crossed the threshold.
    #[test]
    fn fenced_clears_on_partition_revoke_and_reassign() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();
        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t0);

        let t1 = t0 + Duration::from_secs(90);
        assert_eq!(tracker.fenced(t1, Duration::from_secs(60)), Some(0));

        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t1);
        tracker.track_received(0, 42);

        assert_eq!(
            tracker.fenced(t1, Duration::from_secs(60)),
            None,
            "reassignment recreated the tracker; the old streak is gone"
        );
    }

    /// A rebalance that *retains* a partition must also clear its streak.
    ///
    /// Regression: the retained-partition re-offer went through `mark_dirty`,
    /// which starts a streak. A group rebalancing faster than
    /// `QUIET_DRAINS_TO_RESOLVE` drains — an autoscaler adding and removing
    /// consumers, or a rolling deploy — therefore kept every retained
    /// partition permanently dirty and fenced the consumer after
    /// `COMMIT_FENCE_TIMEOUT` even though not one commit had been rejected.
    /// The forced reconnect then rejoins the group, provoking another
    /// rebalance: the detector fed the churn it mistook for a wedge.
    #[test]
    fn fenced_does_not_fire_on_sustained_rebalancing_of_a_retained_partition() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.track_received(1, 0);
        let t0 = Instant::now();

        // Partition 1 churns in and out for five minutes while partition 0
        // stays put. No commit is ever rejected.
        for i in 0..60 {
            let at = t0 + Duration::from_secs(5 * i);
            tx.send(RebalanceEvent::Revoke(vec![1])).unwrap();
            tracker.apply_rebalance_events(&rx, at);
            tx.send(RebalanceEvent::Assign(vec![])).unwrap();
            tracker.apply_rebalance_events(&rx, at);
            let _ = tracker.drain_committable();
            tracker.track_received(1, 0);
        }

        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(300), Duration::from_secs(60)),
            None,
            "rebalances are proof of coordinator contact, not of a fenced member"
        );
    }

    /// The clearing above must not blunt real detection: a rejected commit
    /// that keeps being rejected still fences, and a rebalance partway
    /// through only restarts the clock.
    #[test]
    fn fenced_still_fires_when_commits_fail_after_a_rebalance() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t0);

        // A rebalance at t0+30s retains partition 0 and resolves the streak.
        let t1 = t0 + Duration::from_secs(30);
        tx.send(RebalanceEvent::Assign(vec![])).unwrap();
        tracker.apply_rebalance_events(&rx, t1);
        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(61), Duration::from_secs(60)),
            None,
            "the rebalance resolved the pre-existing streak"
        );

        // Commits start being rejected again right after.
        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, t1);
        assert_eq!(
            tracker.fenced(t1 + Duration::from_secs(61), Duration::from_secs(60)),
            Some(0),
            "a genuinely wedged partition still trips the fence, timed from t1"
        );
    }
}

/// The batch path's rebalance handling: which events invalidate an in-flight
/// batch, and which are noise that would otherwise throw away — and duplicate —
/// thousands of buffered rows.
#[cfg(test)]
mod batch_rebalance_tests {
    use super::*;

    fn batch_over(partitions: &[i32]) -> HashMap<i32, i64> {
        partitions.iter().map(|&p| (p, 0)).collect()
    }

    #[test]
    fn drain_reports_assigned_and_revoked_partitions() {
        let (tx, rx) = std_mpsc::channel();
        tx.send(RebalanceEvent::Revoke(vec![1, 2])).unwrap();
        tx.send(RebalanceEvent::Assign(vec![5])).unwrap();

        assert_eq!(
            drain_batch_rebalance_events(&rx),
            BTreeSet::from([1, 2, 5]),
            "every partition whose ownership moved should be reported"
        );
    }

    #[test]
    fn drain_is_empty_when_nothing_happened() {
        let (_tx, rx) = std_mpsc::channel::<RebalanceEvent>();
        assert!(drain_batch_rebalance_events(&rx).is_empty());
    }

    /// `CommitFailed` is not a rebalance. The batch path commits
    /// synchronously and surfaces its own errors, so letting this event
    /// abandon the batch would discard it for no reason at all.
    #[test]
    fn commit_failed_is_not_a_rebalance() {
        let (tx, rx) = std_mpsc::channel();
        tx.send(RebalanceEvent::CommitFailed(vec![0, 1])).unwrap();

        assert!(
            drain_batch_rebalance_events(&rx).is_empty(),
            "a failed commit says nothing about partition ownership"
        );
    }

    #[test]
    fn a_revoked_batch_partition_invalidates_the_batch() {
        let moved = BTreeSet::from([1]);
        assert!(rebalance_affects_batch(&moved, &batch_over(&[0, 1])));
    }

    /// The regression this exists for: another member leaves, this one is
    /// *assigned* a partition and loses nothing. Abandoning here rewinds
    /// partitions {0,1} and redelivers the whole in-flight batch as duplicate
    /// writes, for a rebalance that never touched it.
    #[test]
    fn an_assignment_elsewhere_leaves_the_batch_alone() {
        let moved = BTreeSet::from([5]);
        assert!(!rebalance_affects_batch(&moved, &batch_over(&[0, 1])));
    }

    /// `cooperative-sticky` invokes the rebalance callback on every member
    /// each round, so an empty delta is routine — and must not cost a batch.
    #[test]
    fn an_empty_delta_leaves_the_batch_alone() {
        assert!(!rebalance_affects_batch(
            &BTreeSet::new(),
            &batch_over(&[0, 1])
        ));
    }

    /// An eager (non-cooperative) rebalance revokes everything first; the
    /// batch's own partitions are in that list, so it is abandoned.
    #[test]
    fn an_eager_revoke_all_invalidates_the_batch() {
        let moved = BTreeSet::from([0, 1, 2, 3]);
        assert!(rebalance_affects_batch(&moved, &batch_over(&[1])));
    }

    #[test]
    fn a_reassignment_of_a_batch_partition_invalidates_the_batch() {
        let moved = BTreeSet::from([0]);
        assert!(
            rebalance_affects_batch(&moved, &batch_over(&[0])),
            "the fetch position was reset out from under the batch"
        );
    }
}

/// [`BatchBuffer`]'s accounting: what counts toward `max_batch_size`, and when
/// a dropped message's wire bytes are worth holding on to.
#[cfg(test)]
mod batch_buffer_tests {
    use super::*;
    use crate::topology::TopologyBuilder;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct BufMessage {
        value: u32,
    }

    struct BufTopic;
    impl Topic for BufTopic {
        type Message = BufMessage;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("batch-buffer-test").build())
        }
    }

    fn buffer() -> BatchBuffer<BufTopic> {
        BatchBuffer::new(8)
    }

    fn raw() -> RawMessage {
        RawMessage {
            payload: Bytes::from_static(b"poison"),
            key: None,
            headers: Arc::new(HashMap::new()),
        }
    }

    fn metadata() -> MessageMetadata {
        build_message_metadata(&Arc::new(HashMap::new()), false)
    }

    /// The regression: with only decoded messages counted, a poll window of
    /// nothing but poison never reaches `max_batch_size`, so the batch grows
    /// for the whole `max_batch_age` window holding every payload in memory.
    #[test]
    fn dropped_messages_count_toward_the_size_trigger() {
        let mut buf = buffer();
        for _ in 0..5 {
            buf.drop_message(Some(raw()), "deserialization_error".into());
        }

        assert_eq!(
            buf.flush_len(),
            5,
            "five poison messages are five messages consumed"
        );
        assert!(
            buf.messages.is_empty(),
            "none of them reach the handler, which is why messages.len() cannot be the trigger"
        );
    }

    #[test]
    fn flush_len_sums_decoded_and_dropped() {
        let mut buf = buffer();
        buf.push(BufMessage { value: 1 }, metadata(), None);
        buf.push(BufMessage { value: 2 }, metadata(), None);
        buf.drop_message(Some(raw()), "oversize".into());

        assert_eq!(buf.flush_len(), 3);
    }

    /// With no DLQ declared, `publish_to_dlq` logs and discards, so copying and
    /// holding each poison payload until the flush buys nothing — but the drop
    /// still has to be counted, or its offset never gets committed past.
    #[test]
    fn a_drop_without_a_dlq_parks_no_payload_but_still_counts() {
        let mut buf = buffer();
        buf.drop_message(None, "deserialization_error".into());

        assert!(buf.pending_dlq.is_empty(), "nowhere to publish it to");
        assert_eq!(buf.flush_len(), 1, "the offset still has to be committed");
    }

    #[test]
    fn clear_resets_the_drop_count() {
        let mut buf = buffer();
        buf.drop_message(Some(raw()), "oversize".into());
        buf.clear();

        assert_eq!(buf.flush_len(), 0);
        assert!(buf.is_empty());
    }

    /// A span with no decoded messages is *not* empty: those offsets still
    /// have to be committed past, or the poison replays on every restart.
    #[test]
    fn an_all_dropped_span_is_not_empty() {
        let mut buf = buffer();
        buf.extend_span(0, 7);
        buf.drop_message(None, "oversize".into());

        assert!(!buf.is_empty());
        assert_eq!(buf.end.get(&0), Some(&8));
        assert_eq!(buf.start.get(&0), Some(&7));
    }
}

/// [`reject_settlement`]: the terminal-discard decision for a rejected
/// message.
///
/// `messages_discarded_total` promises every increment is a message that no
/// longer exists, so getting this matrix wrong is either a false data-loss
/// alert or the silent loss this PR exists to surface. Both the single-message
/// `route_outcome` and the batch `flush_batch` reject arm route through it, so
/// the two paths cannot drift the way earlier cross-backend fixes did.
#[cfg(test)]
mod reject_settlement_tests {
    use super::*;

    /// With no DLQ there is nothing to publish to, so a reject drops the
    /// message. This is the bare-topology shape from CAF-35: the discard has to
    /// be counted, which is precisely what the batch path used to skip.
    #[test]
    fn no_dlq_is_always_a_plain_discard() {
        assert_eq!(
            reject_settlement(false, false),
            RejectSettlement::Retired,
            "no DLQ declared: the message is dropped and must be counted"
        );
    }

    /// `reached_dlq` is meaningless without a DLQ — there was no publish. A
    /// caller that passes `true` anyway (a batch with no `raw` bytes buffered,
    /// say) must still get a countable discard rather than a silent `InDlq`.
    #[test]
    fn no_dlq_ignores_the_publish_flag() {
        assert_eq!(reject_settlement(false, true), RejectSettlement::Retired);
    }

    /// The message exists in the DLQ, so no discard is owed however the commit
    /// resolves. Counting here would be a false data-loss alert on the
    /// perfectly ordinary dead-letter path.
    #[test]
    fn a_landed_dlq_publish_owes_no_discard() {
        assert_eq!(reject_settlement(true, true), RejectSettlement::InDlq);
    }

    /// The case `has_dlq` alone gets wrong: a DLQ was declared, the publish
    /// failed, and the offsets commit anyway. Nothing holds a copy, so this is
    /// data loss even though the topology says otherwise — it must settle with
    /// `confirm_lost`, which counts regardless of `has_dlq`.
    #[test]
    fn a_failed_dlq_publish_is_loss_despite_the_topology() {
        assert_eq!(reject_settlement(true, false), RejectSettlement::Lost);
    }

    /// Only `InDlq` may skip the commit round trip. The other two are claims
    /// that a message is gone, and nothing has retired it until the offsets
    /// actually advance — settling them early is the false-alert-during-an-
    /// outage failure `PendingDiscard` was introduced to prevent.
    #[test]
    fn everything_but_in_dlq_waits_for_the_commit() {
        for (has_dlq, reached_dlq) in [(false, false), (false, true), (true, false)] {
            assert_ne!(
                reject_settlement(has_dlq, reached_dlq),
                RejectSettlement::InDlq,
                "has_dlq={has_dlq} reached_dlq={reached_dlq} must wait for the commit"
            );
        }
    }
}

/// [`invoke_batch_handler`]: a batch flush must not be able to take the
/// consumer task down with it, and must not be able to hang it forever.
#[cfg(test)]
mod batch_handler_isolation_tests {
    use super::*;

    /// A panicking flush used to unwind `run_batch` itself, killing the
    /// consumer until something outside the process restarted it. It has to
    /// become a redelivery, exactly like the single-message path's.
    #[tokio::test]
    async fn a_panicking_batch_becomes_retry() {
        let outcome = invoke_batch_handler(
            || async { panic!("flush blew up") },
            Some(Duration::from_secs(5)),
            None,
            "topic",
            None,
            3,
        )
        .await;

        assert!(matches!(outcome, Outcome::Retry));
    }

    /// The batch loop is a single task, so a flush that never resolves also
    /// stops commits, rebalance handling and shutdown. The timeout is what
    /// bounds all three.
    #[tokio::test(start_paused = true)]
    async fn a_hung_batch_times_out_into_retry() {
        let outcome = invoke_batch_handler(
            || async {
                tokio::time::sleep(Duration::from_secs(600)).await;
                Outcome::Ack
            },
            Some(Duration::from_secs(30)),
            None,
            "topic",
            None,
            3,
        )
        .await;

        assert!(matches!(outcome, Outcome::Retry));
    }

    /// Panic containment must not depend on the timeout being set: opting out
    /// of the timeout is a statement about how long a flush may take, not a
    /// request to let a panic kill the consumer.
    #[tokio::test]
    async fn panics_are_caught_with_no_timeout_configured() {
        let outcome = invoke_batch_handler(
            || async { panic!("flush blew up") },
            None,
            None,
            "topic",
            None,
            1,
        )
        .await;

        assert!(matches!(outcome, Outcome::Retry));
    }

    /// `BatchMessageHandler::handle_batch` is an ordinary `fn -> impl Future`,
    /// so an implementation may panic while *building* its future — validating
    /// an argument, indexing a config map — before returning anything to await.
    ///
    /// That panic used to escape containment: the call was evaluated at the
    /// `flush_batch` call site, outside the `catch_unwind`, so it unwound the
    /// batch task and killed `run_batch` instead of becoming a redelivery. The
    /// closure is what moves construction inside the guard, and this test is
    /// what stops the ready-made-future form coming back — with a plain
    /// `Future` parameter it does not even compile.
    #[tokio::test]
    async fn a_panic_while_building_the_future_is_contained() {
        fn build_future() -> impl std::future::Future<Output = Outcome> {
            panic!("handle_batch blew up before returning a future");
            #[allow(unreachable_code)]
            async {
                unreachable!()
            }
        }

        let outcome = invoke_batch_handler(
            build_future,
            Some(Duration::from_secs(5)),
            None,
            "topic",
            None,
            3,
        )
        .await;

        assert!(matches!(outcome, Outcome::Retry));
    }

    /// The same, with the deadline disabled — the no-timeout arm has its own
    /// `catch_unwind` and has to construct inside it too.
    #[tokio::test]
    async fn a_panic_while_building_the_future_is_contained_with_no_timeout() {
        fn build_future() -> impl std::future::Future<Output = Outcome> {
            panic!("handle_batch blew up before returning a future");
            #[allow(unreachable_code)]
            async {
                unreachable!()
            }
        }

        let outcome = invoke_batch_handler(build_future, None, None, "topic", None, 1).await;

        assert!(matches!(outcome, Outcome::Retry));
    }

    #[tokio::test]
    async fn a_normal_outcome_passes_through_untouched() {
        let ack = invoke_batch_handler(
            || async { Outcome::Ack },
            Some(Duration::from_secs(5)),
            None,
            "topic",
            None,
            2,
        )
        .await;
        assert!(matches!(ack, Outcome::Ack));

        let reject = invoke_batch_handler(
            || async { Outcome::Reject },
            Some(Duration::from_secs(5)),
            None,
            "topic",
            None,
            2,
        )
        .await;
        assert!(matches!(reject, Outcome::Reject));
    }

    /// The batch path has its own timeout arm, so it needs its own proof that
    /// `handler_timeout_outcome` reaches it. Without this the setter would be
    /// silently inert for `run_batch` — and a slow flush would keep burning the
    /// retry budget of every message in the batch.
    #[tokio::test(start_paused = true)]
    async fn a_hung_batch_honours_the_configured_timeout_outcome() {
        let outcome = invoke_batch_handler(
            || async {
                tokio::time::sleep(Duration::from_secs(600)).await;
                Outcome::Ack
            },
            Some(Duration::from_secs(30)),
            Some(Outcome::Defer),
            "topic",
            None,
            3,
        )
        .await;

        assert!(matches!(outcome, Outcome::Defer));
    }
}

/// [`seek_errors`]: the guard against reading "rewound" off a response that
/// says otherwise.
#[cfg(test)]
mod seek_error_tests {
    use super::*;

    /// The false-positive direction matters as much as the false-negative one:
    /// every `Retry` flush seeks, so a clean seek reporting an error here would
    /// turn each retry into a full reconnect.
    #[test]
    fn a_clean_seek_result_reports_no_errors() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("queue", 0, Offset::Offset(10))
            .unwrap();
        tpl.add_partition_offset("queue", 1, Offset::Offset(20))
            .unwrap();

        assert!(seek_errors(&tpl).is_empty());
    }

    #[test]
    fn an_empty_seek_result_reports_no_errors() {
        assert!(seek_errors(&TopicPartitionList::new()).is_empty());
    }
}

#[cfg(test)]
mod batch_sequencing_guard_tests {
    use super::*;
    use crate::topology::{SequenceFailure, TopologyBuilder};

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Entry {
        account: String,
    }

    /// The case the `NotSequenced` bound cannot catch. The marker is
    /// hand-implementable, so a topic can claim it while its topology still
    /// declares sequencing — as this one deliberately does. Without the
    /// runtime guard, `run_batch` would consume the unsharded main queue with
    /// no shard permits and no `FailAll` poison set, and the caller would
    /// still believe ordering held.
    struct LiesAboutSequencing;
    impl Topic for LiesAboutSequencing {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("guard-test-ledger")
                    .sequenced(SequenceFailure::FailAll)
                    .hold_queue(Duration::from_secs(5))
                    .dlq()
                    .build()
            })
        }
    }
    impl NotSequenced for LiesAboutSequencing {}

    struct NoopHandler;
    impl BatchMessageHandler<LiesAboutSequencing> for NoopHandler {
        type Context = ();
        async fn handle_batch(
            &self,
            _messages: Vec<(Entry, MessageMetadata)>,
            _ctx: &(),
        ) -> Outcome {
            Outcome::Ack
        }
    }

    #[tokio::test]
    async fn run_batch_rejects_a_topic_that_declares_sequencing() {
        // Port 1 is never listening; the guard returns before any I/O, so the
        // test neither connects nor blocks.
        let client = KafkaClient::connect(&super::super::client::KafkaConfig::new("127.0.0.1:1"))
            .await
            .expect("client construction is lazy");
        let consumer = KafkaConsumer::new(client);

        let err = consumer
            .run_batch::<LiesAboutSequencing, _>(
                NoopHandler,
                (),
                BatchConsumerOptions::new().with_shutdown(CancellationToken::new()),
            )
            .await
            .expect_err("a sequenced topology must be refused");

        match err {
            ShoveError::Topology(msg) => {
                assert!(
                    msg.contains("guard-test-ledger"),
                    "the error must name the offending topic, got: {msg}"
                );
                assert!(
                    msg.contains("run_fifo"),
                    "the error must point at the supported alternative, got: {msg}"
                );
            }
            other => panic!("expected ShoveError::Topology, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod batch_consumer_options_tests {
    use super::*;

    #[test]
    fn defaults_match_documented_values() {
        let opts = BatchConsumerOptions::default();
        assert_eq!(opts.max_batch_size, 500);
        assert_eq!(opts.max_batch_age, Duration::from_millis(250));
        assert_eq!(opts.max_reconnect_attempts, None);
        assert_eq!(opts.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
        assert_eq!(opts.kafka_group_id, None);
        assert_eq!(opts.kafka_auto_offset_reset, None);
    }

    /// Parity with `ConsumerOptions`: the timeout is opt-*out*, not opt-in.
    /// Defaulting it to `None` would leave every batch sink one hung flush away
    /// from a consumer that never commits, rebalances or shuts down again.
    #[test]
    fn handler_timeout_defaults_to_the_shared_default() {
        assert_eq!(
            BatchConsumerOptions::default().handler_timeout,
            Some(DEFAULT_HANDLER_TIMEOUT)
        );
    }

    #[test]
    fn with_handler_timeout_sets_value() {
        let opts = BatchConsumerOptions::new().with_handler_timeout(Duration::from_secs(90));
        assert_eq!(opts.handler_timeout, Some(Duration::from_secs(90)));
    }

    #[test]
    fn without_handler_timeout_clears_it() {
        let opts = BatchConsumerOptions::new().without_handler_timeout();
        assert_eq!(opts.handler_timeout, None);
    }

    /// `None` by default so an existing batch sink keeps resolving a timeout to
    /// `Retry`; opting in is what changes the behaviour.
    #[test]
    fn handler_timeout_outcome_is_opt_in() {
        assert!(
            BatchConsumerOptions::default()
                .handler_timeout_outcome
                .is_none()
        );
        let opts = BatchConsumerOptions::new().with_handler_timeout_outcome(Outcome::Defer);
        assert!(matches!(opts.handler_timeout_outcome, Some(Outcome::Defer)));
    }

    #[test]
    #[should_panic(expected = "handler_timeout must be positive")]
    fn zero_handler_timeout_panics() {
        let _ = BatchConsumerOptions::new().with_handler_timeout(Duration::ZERO);
    }

    #[test]
    fn new_is_default() {
        let opts = BatchConsumerOptions::new();
        assert_eq!(
            opts.max_batch_size,
            BatchConsumerOptions::default().max_batch_size
        );
    }

    #[test]
    fn with_max_batch_size_sets_value() {
        let opts = BatchConsumerOptions::new().with_max_batch_size(1000);
        assert_eq!(opts.max_batch_size, 1000);
    }

    #[test]
    #[should_panic(expected = "max_batch_size must be > 0")]
    fn with_max_batch_size_zero_panics() {
        let _ = BatchConsumerOptions::new().with_max_batch_size(0);
    }

    #[test]
    fn with_max_batch_age_sets_value() {
        let opts = BatchConsumerOptions::new().with_max_batch_age(Duration::from_secs(2));
        assert_eq!(opts.max_batch_age, Duration::from_secs(2));
    }

    #[test]
    #[should_panic(expected = "max_batch_age must be positive")]
    fn with_max_batch_age_zero_panics() {
        let _ = BatchConsumerOptions::new().with_max_batch_age(Duration::ZERO);
    }

    #[test]
    fn with_max_reconnect_attempts_sets_value() {
        let opts = BatchConsumerOptions::new().with_max_reconnect_attempts(5);
        assert_eq!(opts.max_reconnect_attempts, Some(5));
    }

    #[test]
    fn with_max_message_size_sets_value() {
        let opts = BatchConsumerOptions::new().with_max_message_size(1024);
        assert_eq!(opts.max_message_size, Some(1024));
    }

    #[test]
    fn with_group_id_sets_value() {
        let opts = BatchConsumerOptions::new().with_group_id("custom-group");
        assert_eq!(opts.kafka_group_id.as_deref(), Some("custom-group"));
    }

    #[test]
    fn with_consumer_group_sets_value() {
        let opts = BatchConsumerOptions::new().with_consumer_group("orders-workers");
        assert_eq!(opts.consumer_group.as_deref(), Some("orders-workers"));
    }

    /// The metrics group name and the Kafka `group.id` are separate knobs, and
    /// setting one must never imply the other. That conflation is what the
    /// batch path shipped with: it labelled metrics from the derived
    /// `group.id`, so `with_group_id` silently renamed the consumer group on
    /// every dashboard. `metrics_kafka_consumer_group_label` pins the emitted
    /// label end to end; this pins the options that feed it.
    #[test]
    fn group_id_and_consumer_group_are_independent() {
        let neither = BatchConsumerOptions::new();
        assert_eq!(neither.consumer_group, None);
        assert_eq!(neither.kafka_group_id, None);

        let only_gid = BatchConsumerOptions::new().with_group_id("orders-consumer-v2");
        assert_eq!(
            only_gid.consumer_group, None,
            "a `group.id` override must not become the metrics group name — \
             unset still means `consumer_group=\"default\"`"
        );

        let only_name = BatchConsumerOptions::new().with_consumer_group("orders-workers");
        assert_eq!(
            only_name.kafka_group_id, None,
            "naming the group for metrics must not repartition the consumer"
        );

        let both = BatchConsumerOptions::new()
            .with_consumer_group("orders-workers")
            .with_group_id("orders-consumer-v2");
        assert_eq!(both.consumer_group.as_deref(), Some("orders-workers"));
        assert_eq!(both.kafka_group_id.as_deref(), Some("orders-consumer-v2"));
    }

    #[test]
    fn with_auto_offset_reset_sets_value() {
        let opts = BatchConsumerOptions::new().with_auto_offset_reset(KafkaAutoOffsetReset::Latest);
        assert_eq!(
            opts.kafka_auto_offset_reset,
            Some(KafkaAutoOffsetReset::Latest)
        );
    }

    #[test]
    fn with_shutdown_sets_token() {
        let token = CancellationToken::new();
        let opts = BatchConsumerOptions::new().with_shutdown(token.clone());
        token.cancel();
        assert!(opts.shutdown.is_cancelled());
    }
}

#[cfg(test)]
mod reconnect_tests {
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    use super::*;

    /// A closure that keeps a connection "up" for at least
    /// `RECONNECT_RESET_AFTER` before failing must have its reconnect budget
    /// reset each time, so `max_reconnect_attempts` never trips even though
    /// the closure fails more times than the configured max.
    #[tokio::test(start_paused = true)]
    async fn resets_budget_after_healthy_run() {
        let shutdown = CancellationToken::new();
        let calls = AtomicU32::new(0);
        let result = run_with_reconnect(&shutdown, "test", Some(2), || {
            let n = calls.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            async move {
                if n <= 5 {
                    tokio::time::advance(RECONNECT_RESET_AFTER + Duration::from_secs(1)).await;
                    Err(ShoveError::Connection("boom".to_string()))
                } else {
                    Ok(())
                }
            }
        })
        .await;

        assert!(result.is_ok(), "expected success, got {result:?}");
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 6);
    }

    /// Without an intervening healthy period, consecutive fast failures must
    /// still exhaust the configured reconnect budget.
    #[tokio::test(start_paused = true)]
    async fn exhausts_budget_on_consecutive_fast_failures() {
        let shutdown = CancellationToken::new();
        let calls = AtomicU32::new(0);
        let result = run_with_reconnect(&shutdown, "test", Some(2), || {
            calls.fetch_add(1, AtomicOrdering::SeqCst);
            async move { Err(ShoveError::Connection("boom".to_string())) }
        })
        .await;

        assert!(result.is_err(), "expected exhaustion error, got {result:?}");
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
    }
}
