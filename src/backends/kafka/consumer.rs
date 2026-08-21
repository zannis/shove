use std::collections::{BTreeSet, HashMap};
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
use crate::routing::{PoisonedKeys, RetryDecision, decide_retry, hold_index};
use crate::topic::{SequencedTopic, Topic};
use crate::topology::QueueTopology;
use crate::{DEFAULT_MAX_MESSAGE_SIZE, HoldQueue, Kafka, ShoveError};

#[cfg(feature = "kafka-msk-iam")]
use super::msk_iam::MskIamContext;

#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::WireFormat;
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::decode::{RegistryDecode, registry_decode};
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::default_subject;

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
}

impl PartitionTracker {
    fn new(first_offset: i64) -> Self {
        Self {
            next_to_commit: first_offset,
            completed: BTreeSet::new(),
            dirty: false,
            dirty_since: None,
            quiet_drains: 0,
        }
    }

    fn mark_complete(&mut self, offset: i64) {
        // Completions below the seed are stale: after a partition is removed
        // on rebalance and re-seeded by the next delivery, completions of
        // messages in flight from the previous assignment epoch would
        // otherwise pile up in `completed` forever (never contiguous with
        // `next_to_commit`).
        if offset < self.next_to_commit {
            return;
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
    fn drain_committable(&mut self) -> Option<i64> {
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
            Some(next)
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

    fn mark_complete(&mut self, partition: i32, offset: i64) {
        if let Some(tracker) = self.partitions.get_mut(&partition) {
            tracker.mark_complete(offset);
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
    fn drain_committable(&mut self) -> Option<TopicPartitionList> {
        let mut tpl: Option<TopicPartitionList> = None;
        for (&partition, tracker) in &mut self.partitions {
            if let Some(commit_offset) = tracker.drain_committable() {
                tpl.get_or_insert_with(TopicPartitionList::new)
                    .add_partition_offset(&self.topic, partition, Offset::Offset(commit_offset))
                    .ok();
            }
        }
        tpl
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
type CompletionHandle = Option<(mpsc::Sender<(i32, i64)>, i32, i64)>;

fn signal_completion(handle: CompletionHandle, queue: &str) {
    if let Some((tx, partition, offset)) = handle
        && tx.try_send((partition, offset)).is_err()
    {
        tracing::error!(
            queue,
            partition,
            offset,
            "completion channel full — logic bug in offset tracker"
        );
    }
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
) -> bool {
    match decide_retry(&outcome, retry_count, max_retries) {
        RetryDecision::Ack => {
            signal_completion(completion, topic);
            true
        }
        RetryDecision::Dlq { reason } => {
            // Emit before the DLQ publish so the metric fires regardless
            // of DLQ outcome — silent loss on DLQ failure is what the
            // counter has to surface to alerting.
            let fail_reason = match reason {
                "rejected" => metrics::FailReason::Rejected,
                _ => metrics::FailReason::MaxRetriesExceeded,
            };
            metrics::record_failed(topic, group, fail_reason);
            let dlq_ok =
                publish_to_dlq(client, topology, payload, key.as_deref(), headers, reason).await;
            // Commit even if the DLQ publish failed: the message has
            // exhausted retries (or was rejected) and looping it forever
            // produces a poison hot-spot. The error trace + metric give
            // operators what they need to investigate. Matches the existing
            // pre-refactor semantic (the outer task committed regardless).
            signal_completion(completion, topic);
            match dlq_ok {
                Ok(()) => true,
                Err(e) => {
                    tracing::error!(error = %e, "failed to publish to DLQ");
                    false
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
            .await
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
            .await
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
                        signal_completion(completion, &topic);
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
                tracing::warn!("handler timed out after {duration:?}, retrying");
                metrics::record_failed(topic, group, metrics::FailReason::Timeout);
                Outcome::Retry
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
    kafka_group_id: Option<Arc<str>>,
    kafka_auto_offset_reset: Option<KafkaAutoOffsetReset>,
    shutdown: CancellationToken,
}

impl Default for BatchConsumerOptions {
    fn default() -> Self {
        Self {
            max_batch_size: 500,
            max_batch_age: Duration::from_millis(250),
            max_reconnect_attempts: None,
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            kafka_group_id: None,
            kafka_auto_offset_reset: None,
            shutdown: CancellationToken::new(),
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
}

/// The parts of a batch flush that don't change between flushes.
struct BatchFlushCtx<'a> {
    consumer: &'a KafkaStreamConsumer,
    queue: &'a str,
    topic: &'a str,
    group: Option<&'a str>,
    /// Cuts the redelivery backoff short so a wedged handler cannot hold up
    /// shutdown for the whole (escalated) delay.
    shutdown: &'a CancellationToken,
}

/// Commits each partition's exclusive end offset in one batched sync commit.
fn commit_batch_end(
    consumer: &KafkaStreamConsumer,
    queue: &str,
    batch_end: &HashMap<i32, i64>,
) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    for (&partition, &end_offset) in batch_end {
        tpl.add_partition_offset(queue, partition, Offset::Offset(end_offset))
            .ok();
    }
    consumer
        .commit(&tpl, CommitMode::Sync)
        .map_err(|e| map_kafka_error("batch commit failed", e))
}

/// Hands `batch` to the handler and applies the single returned `Outcome`:
/// `Ack` commits every partition's end offset in `batch_end` in one batched
/// commit; anything else seeks every partition back to `batch_start` so the
/// whole batch is re-delivered on the next `recv()` instead of being
/// silently skipped (offsets were never committed, but this consumer keeps
/// polling forward without a seek).
///
/// `batch` may be empty while `batch_end` is not: every message in the offset
/// span was dropped before the handler (oversize / undeserializable). There is
/// nothing to hand over, but those offsets still get committed — see the
/// empty-batch arm.
///
/// `redelivery_backoff` escalates across consecutive non-Ack flushes and is
/// reset here on Ack.
async fn flush_batch<T, H>(
    flush: &BatchFlushCtx<'_>,
    handler: &H,
    ctx: &H::Context,
    batch: Vec<(T::Message, MessageMetadata)>,
    batch_start: &HashMap<i32, i64>,
    batch_end: &HashMap<i32, i64>,
    redelivery_backoff: &mut Backoff,
) -> Result<()>
where
    T: Topic,
    H: BatchMessageHandler<T>,
{
    let BatchFlushCtx {
        consumer,
        queue,
        topic,
        group,
        shutdown,
    } = *flush;
    let batch_size = batch.len();

    // Every message in this span was dropped pre-handler. The offsets must
    // still be committed: leaving them uncommitted replays the same poison on
    // every restart, and a poll window that is *entirely* poison would never
    // commit anything at all — the consumer would make no forward progress
    // across restarts, forever.
    if batch_size == 0 {
        if batch_end.is_empty() {
            return Ok(());
        }
        commit_batch_end(consumer, queue, batch_end)?;
        tracing::debug!(queue, "committed offsets past a fully-dropped batch");
        return Ok(());
    }

    let outcome = handler.handle_batch(batch, ctx).await;
    metrics::record_consumed_n(topic, group, &outcome, batch_size as u64);
    match outcome {
        Outcome::Ack => {
            commit_batch_end(consumer, queue, batch_end)?;
            *redelivery_backoff = batch_redelivery_backoff();
            tracing::debug!(queue, batch_size, "batch committed");
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
            for (&partition, &start_offset) in batch_start {
                tpl.add_partition_offset(queue, partition, Offset::Offset(start_offset))
                    .ok();
            }
            consumer
                .seek_partitions(tpl, SEEK_TIMEOUT)
                .map_err(|e| map_kafka_error("batch seek-for-redelivery failed", e))?;
            // There is no per-batch retry counter or DLQ route at this level
            // (see the type's doc comment), so a handler stuck returning
            // non-Ack redelivers the same batch forever. The escalating delay
            // bounds the spin; `messages_consumed_total{outcome="retry"}`
            // climbing with no matching `outcome="ack"` is the alertable
            // signal that it is wedged. Shutdown cuts the delay short —
            // otherwise a wedged handler would add up to
            // `BATCH_REDELIVERY_BACKOFF_MAX` to every stop.
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = shutdown.cancelled() => {}
            }
        }
    }
    Ok(())
}

/// Drains librdkafka's rebalance / commit-failure events, returning whether
/// any arrived.
///
/// The callbacks already log the assignment change itself, so this only
/// reports *that* ownership moved. Any such event invalidates the in-flight
/// batch: a commit would be rejected for a stale generation, and part of the
/// offset span may belong to a partition this member no longer owns. Callers
/// abandon the whole batch rather than the revoked partitions' slice of it —
/// see [`rewind_after_rebalance`] for why that is still lossless.
fn drain_batch_rebalance_events(rx: &std_mpsc::Receiver<RebalanceEvent>) -> bool {
    let mut seen = false;
    while rx.try_recv().is_ok() {
        seen = true;
    }
    seen
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
/// The seek fails for partitions that *were* revoked; that is expected and only
/// logged, because the member taking them over resumes from the last committed
/// offset on its own. Each partition is sought separately so one revoked
/// partition's failure cannot stop a retained partition from rewinding.
fn rewind_after_rebalance(
    consumer: &KafkaStreamConsumer,
    queue: &str,
    batch_start: &HashMap<i32, i64>,
) {
    for (&partition, &start_offset) in batch_start {
        let mut tpl = TopicPartitionList::new();
        if tpl
            .add_partition_offset(queue, partition, Offset::Offset(start_offset))
            .is_err()
        {
            continue;
        }
        if let Err(e) = consumer.seek_partitions(tpl, SEEK_TIMEOUT) {
            tracing::debug!(
                error = %e,
                queue,
                partition,
                start_offset,
                "rewind after rebalance failed; the partition was most likely revoked"
            );
        }
    }
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
        let group_id = options
            .kafka_group_id
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| super::constants::consumer_group_id(queue));
        let auto_offset_reset = options
            .kafka_auto_offset_reset
            .unwrap_or(KafkaAutoOffsetReset::Earliest);

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let prefetch_count = options.prefetch_count;
        let handler_timeout = options.handler_timeout;
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
                    mpsc::channel::<(i32, i64)>(prefetch_count as usize);

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
                    while let Ok((partition, offset)) = completion_rx.try_recv() {
                        tracker.mark_complete(partition, offset);
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
                    if let Some(tpl) = tracker.drain_committable() {
                        consumer
                            .commit(&tpl, CommitMode::Async)
                            .map_err(|e| map_kafka_error("commit failed", e))?;
                    }

                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            tracing::info!(queue, "shutdown signal received, draining in-flight tasks");
                            let _ = semaphore.acquire_many(prefetch_count as u32).await;
                            // Final commit
                            while let Ok((partition, offset)) = completion_rx.try_recv() {
                                tracker.mark_complete(partition, offset);
                            }
                            tracker.apply_rebalance_events(&rebalance_rx, Instant::now());
                            if let Some(tpl) = tracker.drain_committable()
                                && let Err(e) = consumer.commit(&tpl, CommitMode::Sync)
                            {
                                tracing::warn!(queue, error = %e, "final offset commit failed during shutdown; batch may be redelivered");
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
                            if let Some((partition, offset)) = completion {
                                tracker.mark_complete(partition, offset);
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
                                if completion_tx.try_send((partition, offset)).is_err() {
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
                                        if completion_tx.try_send((partition, offset)).is_err() {
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
                                        if completion_tx.try_send((partition, offset)).is_err() {
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
                                        if completion_tx.try_send((partition, offset)).is_err() {
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
                                    if completion_tx.try_send((partition, offset)).is_err() {
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
                                route_outcome(
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
    /// This is the primitive DB-sink consumers otherwise hand-roll around
    /// the single-message API: push decoded messages into a buffer, flush
    /// on size-or-age, ack-after-flush. See [`BatchMessageHandler`] for the
    /// outcome semantics — in particular, only `Outcome::Ack` commits;
    /// every other outcome redelivers the **entire batch** (no per-message
    /// retry-count tracking or DLQ routing at this level yet).
    ///
    /// # Current limitations
    ///
    /// - No Confluent Schema Registry integration (even under
    ///   `kafka-schema-registry`) — messages are decoded directly via
    ///   `T::Codec`.
    /// - A message that fails to deserialize or exceeds
    ///   `options.max_message_size` is dropped from the batch (logged and
    ///   counted via `metrics::FailReason`) rather than DLQ'd individually.
    ///   Its offset is still committed with the batch, so poison does not
    ///   replay forever — but the payload is gone, not parked in a DLQ.
    /// - A rebalance abandons the whole in-flight batch uncommitted and rewinds
    ///   the partitions this member keeps, so every message is redelivered —
    ///   to this consumer for retained partitions, to the new owner for revoked
    ///   ones. Expect duplicates across a rebalance, never loss.
    /// - No FIFO/sequenced variant.
    pub async fn run_batch<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        H: BatchMessageHandler<T>,
    {
        let topology = T::topology();
        let queue = topology.queue();
        let group_id = options
            .kafka_group_id
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| super::constants::consumer_group_id(queue));
        let auto_offset_reset = options
            .kafka_auto_offset_reset
            .unwrap_or(KafkaAutoOffsetReset::Earliest);
        let shutdown = options.shutdown.clone();
        let max_message_size = options.max_message_size;
        let max_batch_size = options.max_batch_size;
        let max_batch_age = options.max_batch_age;
        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();
        let topic: Arc<str> = Arc::from(queue);
        let group: Option<Arc<str>> = Some(Arc::from(group_id.as_str()));

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
            async move {
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

                let flush_ctx = BatchFlushCtx {
                    consumer: &consumer,
                    queue,
                    topic: topic.as_ref(),
                    group: group.as_deref(),
                    shutdown: &shutdown,
                };

                let mut batch: Vec<(T::Message, MessageMetadata)> = Vec::with_capacity(max_batch_size);
                // Exclusive end offset to commit per partition once the
                // batch acks, and the same batch's start offset per
                // partition to seek back to if it doesn't — see
                // `flush_batch`.
                let mut batch_end: HashMap<i32, i64> = HashMap::new();
                let mut batch_start: HashMap<i32, i64> = HashMap::new();
                let mut deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
                let mut redelivery_backoff = batch_redelivery_backoff();

                // Wakes the loop when neither a message nor the batch deadline
                // will, so a rebalance that arrives while the topic is idle is
                // still noticed promptly.
                let mut housekeeping = tokio::time::interval(HOUSEKEEPING_INTERVAL);
                housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    if drain_batch_rebalance_events(&rebalance_rx)
                        && (!batch.is_empty() || !batch_end.is_empty())
                    {
                        tracing::warn!(
                            queue,
                            batch_size = batch.len(),
                            "group rebalanced mid-batch, abandoning the in-flight batch uncommitted"
                        );
                        // Rewind before clearing: under cooperative-sticky the
                        // partitions this member *keeps* would otherwise have
                        // their consumed-but-uncommitted messages skipped.
                        rewind_after_rebalance(&consumer, queue, &batch_start);
                        batch.clear();
                        batch_start.clear();
                        batch_end.clear();
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
                            if !batch.is_empty() || !batch_end.is_empty() {
                                flush_batch(
                                    &flush_ctx,
                                    handler.as_ref(),
                                    ctx.as_ref(),
                                    std::mem::take(&mut batch),
                                    &batch_start,
                                    &batch_end,
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
                                std::mem::take(&mut batch),
                                &batch_start,
                                &batch_end,
                                &mut redelivery_backoff,
                            )
                            .await?;
                            batch_start.clear();
                            batch_end.clear();
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
                            batch_start.entry(partition).or_insert(offset);
                            batch_end.insert(partition, offset + 1);
                            if deadline.is_none() {
                                deadline = Some(Box::pin(tokio::time::sleep(max_batch_age)));
                            }

                            if let Err(e) = validate_message_size(payload_slice.len(), max_message_size) {
                                tracing::warn!(error = %e, queue, partition, offset, "dropping oversized message from batch");
                                metrics::record_failed(&topic, group.as_deref(), metrics::FailReason::Oversize);
                                continue;
                            }

                            let decoded = match <T::Codec as crate::Codec<T::Message>>::decode(payload_slice) {
                                Ok(m) => m,
                                Err(e) => {
                                    tracing::warn!(error = %e, queue, partition, offset, "dropping undeserializable message from batch");
                                    metrics::record_failed(&topic, group.as_deref(), metrics::FailReason::Deserialize);
                                    continue;
                                }
                            };

                            let headers = extract_string_headers(&msg);
                            let metadata = build_message_metadata(&headers, false);
                            batch.push((decoded, metadata));

                            if batch.len() >= max_batch_size {
                                flush_batch(
                                    &flush_ctx,
                                    handler.as_ref(),
                                    ctx.as_ref(),
                                    std::mem::take(&mut batch),
                                    &batch_start,
                                    &batch_end,
                                    &mut redelivery_backoff,
                                )
                                .await?;
                                batch_start.clear();
                                batch_end.clear();
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
        // `{group}-fifo`. `None` keeps the default `{queue}-fifo`.
        let group_id = match options.kafka_group_id.as_deref() {
            Some(base) => super::constants::fifo_group_id_from_base(base),
            None => super::constants::consumer_group_id_fifo(&queue),
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
                                    metrics::record_failed(
                                        &topic,
                                        group.as_deref(),
                                        metrics::FailReason::Rejected,
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
                                        continue;
                                    }
                                    consumer.commit_message(&msg, CommitMode::Async).ok();
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

                                let route_ok = route_outcome(
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
                                    consumer.commit_message(&msg, CommitMode::Async).ok();
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

    /// Regression: the normal contiguous drain still works — out-of-order
    /// completions commit only up to the first gap, then advance once the
    /// gap fills.
    #[test]
    fn contiguous_drain_advances_past_gaps_only_when_filled() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(0, 2);
        tracker.mark_complete(0, 0);

        let tpl = tracker
            .drain_committable()
            .expect("offset 0 is committable");
        assert_eq!(committed_offset(&tpl, 0), Some(1), "gap at 1 blocks 2");

        tracker.mark_complete(0, 1);
        let tpl = tracker.drain_committable().expect("gap filled");
        assert_eq!(committed_offset(&tpl, 0), Some(3));
    }

    /// After remove + re-track (a partition revoked and reassigned), the
    /// tracker re-seeds `next_to_commit` from the newly delivered offset
    /// instead of stalling on the stale pre-revocation seed.
    #[test]
    fn remove_then_track_reseeds_next_to_commit() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 5);
        tracker.mark_complete(0, 5);
        let tpl = tracker.drain_committable().expect("initial commit");
        assert_eq!(committed_offset(&tpl, 0), Some(6));

        // Partition moves away (another member commits 6..99), then returns.
        tracker.remove(0);
        tracker.track_received(0, 100);
        tracker.mark_complete(0, 100);
        let tpl = tracker
            .drain_committable()
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
        tracker.mark_complete(0, 5);
        assert!(
            tracker.drain_committable().is_none(),
            "removed partition must not commit"
        );
    }

    /// A completion below the seed (stale offset from before reassignment)
    /// is ignored — it must neither commit nor linger in `completed`.
    #[test]
    fn mark_complete_below_seed_is_ignored() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 10);
        tracker.mark_complete(0, 5);
        assert!(
            tracker.drain_committable().is_none(),
            "stale completion must not commit"
        );

        tracker.mark_complete(0, 10);
        let tpl = tracker.drain_committable().expect("seed offset completes");
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
        tracker.mark_complete(0, 0);
        let tpl = tracker.drain_committable().expect("initial commit");
        assert_eq!(committed_offset(&tpl, 0), Some(1));

        // The async commit of offset 1 was rejected mid-rebalance.
        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, Instant::now());
        let tpl = tracker
            .drain_committable()
            .expect("failed commit must be re-offered without new completions");
        assert_eq!(committed_offset(&tpl, 0), Some(1));

        assert!(
            tracker.drain_committable().is_none(),
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
        tracker.mark_complete(4, 0);
        let tpl = tracker.drain_committable().expect("initial commit");
        assert_eq!(committed_offset(&tpl, 4), Some(1));

        // Another member joins: partitions 0-3 move away; the commit of
        // (4, 1) submitted during the rebalance may have been dropped.
        tx.send(RebalanceEvent::Revoke(vec![0, 1, 2, 3])).unwrap();
        tracker.apply_rebalance_events(&rx, Instant::now());
        let tpl = tracker
            .drain_committable()
            .expect("retained partition must re-offer its position");
        assert_eq!(committed_offset(&tpl, 4), Some(1));

        assert!(
            tracker.drain_committable().is_none(),
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
        tracker.mark_complete(0, 0);
        let _ = tracker.drain_committable().expect("initial commit");

        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        tx.send(RebalanceEvent::CommitFailed(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx, Instant::now());
        assert!(
            tracker.drain_committable().is_none(),
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

        tracker.mark_complete(0, 5);
        tracker.mark_complete(1, 7);
        tracker.mark_complete(2, 9);
        let tpl = tracker.drain_committable().expect("partition 2 commits");
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
        let _ = tracker.drain_committable();
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
        let _ = tracker.drain_committable(); // re-offers the failed commit
        let _ = tracker.drain_committable(); // 1st quiet drain: not yet resolved

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
        let _ = tracker.drain_committable(); // re-offers; this one succeeds

        // Two consecutive quiet drains: the re-offer must have landed.
        for _ in 0..QUIET_DRAINS_TO_RESOLVE {
            let _ = tracker.drain_committable();
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
        let _ = tracker.drain_committable();
        for _ in 0..QUIET_DRAINS_TO_RESOLVE {
            let _ = tracker.drain_committable();
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
