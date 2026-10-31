use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::CStr;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

use bytes::Bytes;
use rdkafka::bindings::{
    rd_kafka_header_cnt, rd_kafka_header_get_all, rd_kafka_header_remove, rd_kafka_message_headers,
};
use rdkafka::client::{DefaultClientContext, OAuthToken};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer as RdkafkaConsumer, ConsumerContext, Rebalance,
    StreamConsumer,
};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Header, Message, OwnedHeaders};
use rdkafka::metadata::Metadata;
use rdkafka::types::{RDKafkaErrorCode, RDKafkaHeaders, RDKafkaRespErr};
use rdkafka::{ClientConfig, ClientContext, Offset, Statistics, TopicPartitionList};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::backend::batch_consumer::settling::{
    PREALLOC_CAP, RejectSettlement, TerminalDiscard, batch_redelivery_backoff,
    invoke_batch_handler, next_redelivery_delay, reject_settlement,
};
use crate::backend::batch_consumer::{
    BatchConsumerOptionsInner, BatchSettlement, settle_batch_outcome, validate_batch_topic,
};
use crate::backend::broadcast::{BROADCAST_DEFER_DELAY, BroadcastAction, settle_broadcast_outcome};
use crate::batch_consumer::BatchConsumerOptions as GenericBatchConsumerOptions;
use crate::broadcast::BroadcastStart;
use crate::consumer::{RetryStrategy, validate_message_size};
use crate::consumer_supervisor::{AbortOnDrop, SupervisorOutcome, drive_fifo_until_timeout};
use crate::error::{CommitFailure, FailedCommit, Result};
use crate::handler::{BatchMessageHandler, MessageHandler};
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::retry::Backoff;
use crate::routing::{PoisonedKeys, RetryDecision, decide_retry, handler_timeout_outcome};
use crate::topic::{NotSequenced, SequencedTopic, Topic};
use crate::topology::QueueTopology;
use crate::{HoldQueue, Kafka, ShoveError};
// These four are only read back by `batch_consumer_options_tests` below, to
// pin that `BatchConsumerOptions` (now a `pub type` alias of the generic
// `BatchConsumerOptions<Kafka>`) still defaults to the same shared constants
// the type used to hardcode directly.
#[cfg(test)]
use crate::{
    DEFAULT_HANDLER_TIMEOUT, DEFAULT_KAFKA_MAX_BATCH_AGE, DEFAULT_KAFKA_MAX_BATCH_SIZE,
    DEFAULT_MAX_MESSAGE_SIZE,
};

#[cfg(feature = "kafka-msk-iam")]
use super::msk_iam::MskIamContext;

#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::decode::{RegistryDecode, registry_decode};
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::default_subject;
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::{
    SchemaEnforcement, SchemaId, SchemaRegistry, SchemaRegistryError, WireFormat,
};

use super::client::KafkaClient;
#[cfg(feature = "kafka-schema-registry")]
use super::constants::REGISTRY_RETRY_DELAY;
use super::constants::{
    BASE_MESSAGE_ID_HEADER, BASE_MESSAGE_ID_KEY, DEATH_COUNT_HEADER, DEATH_COUNT_KEY,
    DEATH_REASON_HEADER, DEATH_REASON_KEY, FETCH_MIN_BYTES, FETCH_WAIT_MAX_MS,
    FINAL_COMMIT_RETRY_POLL, FINAL_COMMIT_RETRY_RESERVE, MAX_POLL_INTERVAL_MS,
    MAX_PUBLISH_ATTEMPTS, MESSAGE_ID_HEADER, MESSAGE_ID_KEY, ORIGINAL_QUEUE_HEADER,
    ORIGINAL_QUEUE_KEY, PENDING_COMMIT_BUDGET, RETRY_COUNT_HEADER, RETRY_COUNT_KEY,
    SESSION_TIMEOUT_MS, SHUTDOWN_COMMIT_DEADLINE,
};
use super::consumer_group::{CommitPolicy, KafkaAutoOffsetReset};
use super::fetch_queue::FetchQueueTuning;
use super::offset_reset::target_from_timestamp_lookup;
use super::startup;

// ---------------------------------------------------------------------------
// Offset tracking for concurrent consumption
// ---------------------------------------------------------------------------

/// Consecutive quiet drains (see `PartitionTracker::drain_committable`) after
/// which a re-offered commit is assumed to have landed, resolving the
/// partition's dirty streak. An accepted commit resolves it at once (see
/// `PartitionTracker::resolve`); quiet drains are a second, slower way.
///
/// Must be >= 2: at 1 this degenerates into "clear on the first quiet tick",
/// which resets a genuinely wedged partition's streak on every iteration and
/// stops it ever reaching `COMMIT_FENCE_TIMEOUT`. At 2, the tolerated window
/// is at least `ASYNC_COMMIT_INTERVAL` while traffic flows (the re-offering
/// drain always issues a commit and so marks the gate, holding the *first*
/// quiet drain back a full interval; the second may follow one iteration
/// later) and 2 × `HOUSEKEEPING_INTERVAL` on an idle topic. Both windows
/// comfortably exceed a healthy commit round trip (sub-ms here), so a wedged
/// partition still trips the fence on schedule. A loop that commits through
/// a `CommitSlot` drains nothing while a commit is in flight, so a rejection
/// slower than the interval cannot out-age the streak there.
const QUIET_DRAINS_TO_RESOLVE: u32 = 2;

/// Minimum spacing between offset commits issued by the concurrent receive
/// loop.
///
/// The loop used to fire one fire-and-forget `commit(Async)` on every
/// iteration that advanced any offset. At high consume rates that issues
/// commits an order of magnitude faster than the group coordinator answers
/// them, so tens of thousands pile up in flight — and
/// `rd_kafka_consumer_close` (which runs inside `Drop`, blocking, and cannot
/// be aborted) waits for every one of them before terminating: measured
/// 5.5–20.2 s after a fast 150k-message corpus, intermittently blowing a 30 s
/// shutdown budget. Spacing commits well above a coordinator round trip keeps
/// the in-flight count O(1), so close stays prompt. Two costs, both accepted:
/// a wider crash-redelivery window (which at-least-once semantics already
/// promise), matching the spirit of Kafka's own 5 s auto-commit default while
/// staying 10x tighter; and a wider gap between the committed offset and the
/// consumed position. That gap costs no memory: the tracker's `in_flight` set
/// holds each delivered offset only from `track_received` until
/// `mark_complete` removes it, so it never holds more than `prefetch_count`
/// entries whatever the interval, and the offsets completed since the last
/// drain are summed up by `position()` alone. A longer interval widens what a
/// crash redelivers (about 6k offsets at 12k msg/s), nothing else.
const ASYNC_COMMIT_INTERVAL: Duration = Duration::from_millis(500);

/// The window a commit policy spaces commits by: the interval itself, or
/// under `PerRecord` the default interval, which there spaces only the
/// re-offers of a rejected commit (see `AsyncCommitGate`) and keeps the
/// fence threshold at its floor.
fn commit_window(policy: CommitPolicy) -> Duration {
    match policy {
        CommitPolicy::Interval(interval) => interval,
        CommitPolicy::PerRecord => ASYNC_COMMIT_INTERVAL,
    }
}

/// Rate gate for the receive loop's offset commits: `due` says whether the
/// commit drain may run now, `mark` records that a commit was issued.
///
/// Only *issued* commits consume the window — a drain that found nothing to
/// commit leaves the gate due, so the next completion is committed as soon
/// as the interval since the last real commit has passed.
///
/// Under `CommitPolicy::PerRecord` there is no window: the gate is due
/// however recent the last confirmed commit, so every completion is
/// committed as it lands. A *rejected* commit closes it for the default
/// interval, though. The loop learns of the rejection before its next pass,
/// and an immediate re-offer would be issued without a `recv()` in between,
/// so a rebalance behind a `REBALANCE_IN_PROGRESS` could never be served and
/// every re-offer would meet the same answer until the fence. Waiting out
/// the interval lets the loop poll between re-offers, as it does under
/// `Interval`.
struct AsyncCommitGate {
    interval: Duration,
    per_record: bool,
    last: Option<Instant>,
    /// The last commit issued was rejected, or went unanswered. Only read
    /// under `PerRecord`, where it is what closes the gate.
    rejected: bool,
}

impl AsyncCommitGate {
    fn new(policy: CommitPolicy) -> Self {
        Self {
            interval: commit_window(policy),
            per_record: matches!(policy, CommitPolicy::PerRecord),
            last: None,
            rejected: false,
        }
    }

    /// True when no commit has been issued yet, or the last one is at least
    /// `interval` old; under `PerRecord`, also whenever the last commit was
    /// confirmed. Read-only: asking does not advance the window.
    fn due(&self, now: Instant) -> bool {
        if self.per_record && !self.rejected {
            return true;
        }
        match self.last {
            None => true,
            Some(last) => now.saturating_duration_since(last) >= self.interval,
        }
    }

    /// Records a commit issued at `now`, opening a fresh window. Under
    /// `PerRecord` the commit was confirmed, and the gate is due again.
    fn mark(&mut self, now: Instant) {
        self.last = Some(now);
        self.rejected = false;
    }

    /// Records a commit issued at `now` that the coordinator rejected, or
    /// that went unanswered. Under `Interval` this is `mark`: the window
    /// is the pacing either way. Under `PerRecord` it closes the gate for
    /// the default interval, see the type's doc.
    fn mark_rejected(&mut self, now: Instant) {
        self.last = Some(now);
        self.rejected = true;
    }

    /// When the gate next becomes due: `last + interval`, which may already
    /// be past. The receive loop's wake arm sleeps until this, so gated
    /// commit work never waits on an unrelated wake.
    ///
    /// The `None` branch returns `now + interval` rather than `now`,
    /// deliberately: with no commit ever issued the gate is due, so the
    /// loop's top-of-iteration drain handles the work before the wake arm
    /// matters — the branch is unreachable while anything is committable.
    /// If a future refactor ever breaks that invariant (skipping the drain,
    /// resetting the gate), a stale-`now` deadline would make the wake arm
    /// ready on construction forever — a hot spin. `now + interval` degrades
    /// to a bounded wake instead.
    ///
    /// Checked: a sum `Instant` cannot represent is treated as due now. The
    /// setters bound the interval to `MAX_COMMIT_INTERVAL`, and `run_with_inner`
    /// checks the field again before it builds the gate, so this is a second
    /// line, not the first; the gate must not be the place a bad interval
    /// first panics.
    fn deadline(&self, now: Instant) -> Instant {
        let anchor = self.last.unwrap_or(now);
        anchor.checked_add(self.interval).unwrap_or(now)
    }
}

struct PartitionTracker {
    /// The epoch of the assignment this tracker was seeded under, see
    /// `OffsetTracker::epochs`. A commit's answer settles on this tracker
    /// only when the commit was drained under the same epoch.
    epoch: u64,
    /// Next offset to commit (exclusive - Kafka convention): every offset
    /// delivered under this assignment below it has completed.
    next_to_commit: i64,
    /// Offsets delivered under this assignment whose handler has not finished
    /// yet. Bounded by `prefetch_count`, since every entry holds a permit.
    ///
    /// The commit position is derived from what was *delivered*, not from a
    /// run of consecutive integers: log compaction removes records without
    /// renumbering the survivors, and a transactional producer leaves a
    /// control record at the end of every transaction that a consumer never
    /// receives. Either leaves a hole in the offset sequence, and a tracker
    /// that waited for the hole to fill stalled the partition for the life of
    /// the assignment.
    in_flight: BTreeSet<i64>,
    /// Highest offset delivered under this assignment. With nothing in flight
    /// the commit position is one past it, which steps over any hole the
    /// broker never delivered.
    highest_delivered: i64,
    /// Set when a commit that included this partition was not accepted
    /// (e.g. during a rebalance), or when a rebalance may have cost one.
    /// Makes the next `drain_committable` re-offer the current
    /// `next_to_commit` even without new completions, so the failed commit
    /// is retried instead of silently lost.
    dirty: bool,
    /// When a *rejected commit* first raised `dirty` in the current
    /// unresolved streak. Only `mark_dirty` sets it; a rebalance-induced
    /// re-offer goes through `mark_dirty_resolved`, which clears it.
    ///
    /// An accepted commit of the partition clears it (`resolve`), and so
    /// does any rebalance: for a partition that moves, by dropping this
    /// tracker (`OffsetTracker::remove`); for one this member keeps, via
    /// `mark_dirty_resolved`. So a `dirty_since` that keeps aging across
    /// repeated re-offers, none accepted and no rebalance between them, is
    /// exactly the "commits rejected, group never recovers" signature:
    /// `PartitionTracker::stuck_for`.
    dirty_since: Option<Instant>,
    /// Consecutive `drain_committable` calls entered with `dirty == false`.
    /// See `QUIET_DRAINS_TO_RESOLVE`: a second way a *recovered* partition
    /// clears `dirty_since`, beside the accepted commit that clears it at
    /// once.
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
    /// Seeds the tracker from the first offset delivered under the
    /// assignment `epoch` names, which is also the first in-flight offset.
    fn new(first_offset: i64, epoch: u64) -> Self {
        Self {
            epoch,
            next_to_commit: first_offset,
            in_flight: BTreeSet::from([first_offset]),
            highest_delivered: first_offset,
            dirty: false,
            dirty_since: None,
            quiet_drains: 0,
            pending_discards: BTreeMap::new(),
        }
    }

    /// Records a delivery under this assignment.
    ///
    /// An offset below `next_to_commit` is not tracked. Within one
    /// assignment librdkafka delivers a partition in offset order, with one
    /// exception: when offset validation finds the log truncated by a leader
    /// change, it seeks the partition back to the new leader's end offset for
    /// the old epoch (`rd_kafka_offset_validate` in the bundled source, under
    /// every reset policy but `error`), and offsets this assignment already
    /// committed past arrive again. The handler still sees them. Their
    /// offsets stay out of `in_flight`, because `position()` is its lowest
    /// entry and would otherwise drop below what is committed, and because a
    /// completion for such an offset would meet the stale check below with
    /// the entry still in the set.
    fn track_received(&mut self, offset: i64) {
        if offset < self.next_to_commit {
            return;
        }
        self.in_flight.insert(offset);
        self.highest_delivered = self.highest_delivered.max(offset);
    }

    fn mark_complete(&mut self, offset: i64, discard: Option<TerminalDiscard>) {
        // Taken out first, whatever the stale check decides: an entry left
        // behind would pin `position()` at its offset for the rest of the
        // assignment. `track_received` keeps such an entry from existing, and
        // this keeps one from surviving if it ever did.
        let known = self.in_flight.remove(&offset);
        // A completion this assignment never delivered is stale: after a
        // partition is removed on rebalance and re-seeded by the next
        // delivery, completions of messages in flight from the previous
        // assignment epoch still arrive. They sit either below the new seed
        // or above it but outside `in_flight`; both are the old epoch's, and
        // the new epoch redelivers those offsets itself. A completion below
        // `next_to_commit` from a post-truncation redelivery lands here too.
        if offset < self.next_to_commit || !known {
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
    }

    /// Completes every offset in flight in `[start, end)`: a batch flush
    /// settling its span on this partition. Every offset the partition
    /// delivered in that range belongs to the span, or to a span abandoned
    /// and rewound before it, whose records the broker delivers again into
    /// this one; the records after `end` stay in flight.
    fn complete_span(&mut self, start: i64, end: i64) {
        let mut span = self.in_flight.split_off(&start);
        let mut after = span.split_off(&end);
        self.in_flight.append(&mut after);
    }

    /// The flush that retired the span up to `end` committed it itself and
    /// the coordinator accepted it: `end` counts as offered, so the next
    /// drain offers a position only past it, and the partition's streak
    /// ends, as an accepted commit's does, see [`resolve`](Self::resolve).
    fn committed_through(&mut self, end: i64) {
        self.next_to_commit = self.next_to_commit.max(end);
        self.resolve();
    }

    /// The exclusive commit position this assignment can offer right now:
    /// the lowest offset still in flight, or one past the highest delivered
    /// offset once nothing is in flight. Never consults offsets the broker
    /// did not deliver, so a compacted or transactional hole does not hold
    /// the position back.
    ///
    /// Saturating, like every other successor on the commit path: at
    /// `i64::MAX` the exclusive position does not exist, and an unchecked
    /// `+ 1` would wrap to a negative offset in a release build.
    fn position(&self) -> i64 {
        self.in_flight
            .first()
            .copied()
            .unwrap_or_else(|| self.highest_delivered.saturating_add(1))
    }

    /// Flags this partition dirty (see the `dirty` field) because a commit
    /// was *rejected*, recording `now` as the start of the unresolved streak
    /// if one isn't already in progress.
    fn mark_dirty(&mut self, now: Instant) {
        self.dirty = true;
        self.quiet_drains = 0;
        self.dirty_since.get_or_insert(now);
    }

    /// Ends the unresolved streak, because the coordinator accepted a
    /// commit of this partition: a member whose commits land is not fenced.
    /// A re-offer already flagged stays flagged.
    fn resolve(&mut self) {
        self.dirty_since = None;
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
    /// that finds nothing to retry: the answer to a re-offer can outlast one
    /// loop iteration, and clearing on the first quiet tick would let a
    /// genuinely wedged partition's streak get reset just before each new
    /// rejection lands, so it would never reach the fenced threshold.
    /// `QUIET_DRAINS_TO_RESOLVE` consecutive quiet drains, on the other
    /// hand, are long enough that a re-offered commit must have landed. The
    /// accepted commit itself clears the streak at once (`resolve`), and a
    /// resolving rebalance clears it by dropping and recreating this
    /// tracker — see `OffsetTracker::remove`.
    /// The returned discards are the ones this commit position retires: their
    /// offsets are strictly below the (exclusive) commit offset, and each is
    /// returned with the offset it rides on, so a commit that is not
    /// accepted can hand it back through `re_pend`. They are handed to the
    /// caller unsettled, because only the commit's result says whether the
    /// retirement actually happened.
    fn drain_committable(&mut self) -> Option<(i64, Vec<(i64, TerminalDiscard)>)> {
        let next = self.position();
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
            Some((next, covered.into_iter().collect()))
        } else {
            None
        }
    }

    /// The shutdown drain: the current commit position and the discards it
    /// retires, whether or not an earlier drain offered the same position.
    ///
    /// `drain_committable` offers a position only on new progress or after
    /// a rejected commit, which is right for the running cadence: a
    /// position offered once is in flight, and offering it again would only
    /// add a request. At shutdown that rule left a hole. The commit the stop
    /// found in flight may have been rejected or gone unanswered, and with
    /// no new completion the final `Sync` commit carried nothing.
    /// Re-offering every position makes that commit carry every safe
    /// position, for the broker to confirm. A repeated commit
    /// keeps the same logical position, though the coordinator still
    /// appends the request to `__consumer_offsets`, as the rebalance
    /// re-offer in `OffsetTracker::apply_rebalance_events` relies on; and
    /// the position never lowers, see `position`.
    fn drain_all(&mut self) -> (i64, Vec<(i64, TerminalDiscard)>) {
        let next = self.position();
        self.next_to_commit = self.next_to_commit.max(next);
        self.dirty = false;
        // `next` is exclusive, so everything strictly below it is covered.
        let remainder = self.pending_discards.split_off(&next);
        let covered = std::mem::replace(&mut self.pending_discards, remainder);
        (next, covered.into_iter().collect())
    }

    /// How long this partition has been continuously dirty, or `None` if
    /// it's currently clean.
    fn stuck_for(&self, now: Instant) -> Option<Duration> {
        self.dirty_since
            .map(|since| now.saturating_duration_since(since))
    }

    /// Whether a drain right now would offer a commit: a rejected commit is
    /// waiting to be re-offered, or the next offset to commit has completed.
    ///
    /// Read-only twin of `drain_committable`'s `progressed || retry`
    /// condition — the receive loop's wake arm uses it to decide whether the
    /// commit gate's deadline is worth waking for. Completions behind an
    /// offset that is still in flight do not count, exactly as they would
    /// not commit.
    fn has_committable(&self) -> bool {
        self.dirty || self.position() > self.next_to_commit
    }
}

/// The revoke token of every partition the receive loop's tracker has seeded
/// under its current assignment, shared between the tracker and the
/// rebalance callback. `OffsetTracker::track_received` creates the token when
/// it seeds the partition and hands a clone to the handler task, whose
/// in-place wait or delayed republish selects on it.
/// `RebalanceContext::pre_rebalance` cancels and removes the token on
/// `Revoke`, synchronously, inside the `recv()` poll that reported the
/// revoke: with one permit, held by such a wait, the loop is parked in
/// `acquire_permit_while_polling` on a paused assignment and drains the
/// rebalance events only on its housekeeping ticks, under `Interval`, or not
/// at all until that wait ends, so the callback is the one place a revoke
/// reaches the wait from at once. `OffsetTracker::remove` cancels as a
/// backstop when the loop drains the event. The batch loop takes a token
/// only for a record its schema registry stall holds, see
/// `OffsetTracker::assignment_token`. The mutex is held for a map operation
/// only, never across an await. Loops that keep no tracker pass
/// `create_stream_consumer` a map nothing reads.
type AssignmentTokens = Arc<std::sync::Mutex<HashMap<i32, CancellationToken>>>;

/// Cancel and forget the tokens of `partitions`, so every in-place wait on
/// them ends with `InPlaceEnd::Revoked`. The lock is released before the
/// tokens are cancelled.
fn cancel_assignments(assignments: &AssignmentTokens, partitions: &[i32]) {
    let revoked: Vec<CancellationToken> = {
        let mut map = assignments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        partitions
            .iter()
            .filter_map(|partition| map.remove(partition))
            .collect()
    };
    for token in revoked {
        token.cancel();
    }
}

/// The assignment a delivery was tracked under: the epoch its completion
/// carries, which `OffsetTracker::mark_complete` checks against the
/// partition's current one, and the token a revoke of the partition cancels.
#[derive(Clone)]
struct Assignment {
    epoch: u64,
    revoked: CancellationToken,
}

struct OffsetTracker {
    topic: String,
    partitions: HashMap<i32, PartitionTracker>,
    /// How many assignments of each partition this tracker has seeded, which
    /// is the epoch of the partition's current one. Only equality between a
    /// completion's epoch and the current one matters, so the count wraps.
    /// Never pruned: the count has to outlive the partition's tracker entry,
    /// or a revoke and a reassign would mint the epoch the old delivery's
    /// completion still carries.
    epochs: HashMap<i32, u64>,
    assignments: AssignmentTokens,
}

impl OffsetTracker {
    /// A tracker with a token map of its own, which nothing cancels in. The
    /// receive loop builds its tracker with `with_assignments`; this is for
    /// the unit tests below.
    #[cfg_attr(not(test), allow(dead_code))]
    fn new(topic: String) -> Self {
        Self::with_assignments(topic, AssignmentTokens::default())
    }

    /// A tracker whose revoke tokens live in `assignments`, the map the
    /// consumer's rebalance callback cancels in; see `AssignmentTokens`.
    fn with_assignments(topic: String, assignments: AssignmentTokens) -> Self {
        Self {
            topic,
            partitions: HashMap::new(),
            epochs: HashMap::new(),
            assignments,
        }
    }

    /// Records a delivery: seeds the partition's tracker on its first offset
    /// under this assignment, and marks every later offset in flight so the
    /// commit position follows what was actually delivered. An offset below
    /// the partition's committed position is not marked, see
    /// `PartitionTracker::track_received`. Returns the assignment the
    /// delivery belongs to: the seed begins a new epoch for the partition
    /// and gives it a fresh revoke token.
    fn track_received(&mut self, partition: i32, offset: i64) -> Assignment {
        let epoch = self.track_epoch(partition, offset);
        let revoked = self.assignment_token(partition);
        Assignment { epoch, revoked }
    }

    /// The revoke token of `partition`'s current assignment, created on the
    /// first ask, see `AssignmentTokens`. A cancelled token leaves the map,
    /// so a wait that must see a revoke keeps the token it took instead of
    /// asking again.
    fn assignment_token(&self, partition: i32) -> CancellationToken {
        self.assignments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(partition)
            .or_default()
            .clone()
    }

    /// [`track_received`](Self::track_received) with no revoke token: the
    /// delivery's epoch alone, for the batch loop, whose only wait a revoke
    /// must reach is a schema registry stall, which takes the token itself.
    fn track_epoch(&mut self, partition: i32, offset: i64) -> u64 {
        match self.partitions.get_mut(&partition) {
            Some(tracker) => {
                tracker.track_received(offset);
                tracker.epoch
            }
            None => {
                let seeded = self.epochs.entry(partition).or_insert(0);
                *seeded = seeded.wrapping_add(1);
                let epoch = *seeded;
                self.partitions
                    .insert(partition, PartitionTracker::new(offset, epoch));
                epoch
            }
        }
    }

    fn mark_complete(&mut self, completion: Completion) {
        let Completion {
            partition,
            offset,
            epoch,
            discard,
        } = completion;
        match self.partitions.get_mut(&partition) {
            // A completion from the partition's current assignment, the one
            // its delivery was tracked under.
            Some(tracker) if tracker.epoch == epoch => tracker.mark_complete(offset, discard),
            // The partition was revoked while this delivery was in flight, so
            // this member will never commit the offset and whoever owns the
            // partition now will redeliver the message. Or the partition came
            // back since and this completion is an earlier assignment's: the
            // current assignment's delivery of the same offset is in flight
            // and stays so until its own handler finishes.
            _ => {
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
        // The backstop for the rebalance callback's cancel: an in-place wait
        // on an assignment that is over ends here at the latest.
        cancel_assignments(&self.assignments, &[partition]);
    }

    /// Settles a commit the coordinator accepted: every partition it carried
    /// that is still under the assignment the commit was drained under ends
    /// its unresolved streak, see `PartitionTracker::resolve`. The caller
    /// confirms the discards the commit retired.
    fn commit_accepted(&mut self, positions: &[CommitPosition]) {
        for position in positions {
            if let Some(tracker) = self.tracker_under(position) {
                tracker.resolve();
            }
        }
    }

    /// Settles a commit that did not land: the coordinator refused it,
    /// librdkafka gave it up, or no thread could carry it. Every partition it
    /// carried re-offers its current position on the next drain, and the
    /// discards it would have retired ride that re-offer, so the commit that
    /// finally lands retires them exactly once.
    ///
    /// Only on a partition still under the assignment the commit was drained
    /// under. One revoked meanwhile is no longer this member's to commit, and
    /// one revoked and handed back since is redelivered from its committed
    /// position under the new assignment, which commits it on its own
    /// account: the old commit's rejection starts no streak there, and its
    /// discards settle as survived, the rule `remove` applies.
    fn commit_rejected(
        &mut self,
        positions: &[CommitPosition],
        discards: Vec<CoveredDiscard>,
        now: Instant,
    ) {
        for position in positions {
            if let Some(tracker) = self.tracker_under(position) {
                tracker.mark_dirty(now);
            }
        }
        self.re_pend(discards);
    }

    /// The tracker of `position`'s partition, while the partition is still
    /// under the assignment the position was drained under.
    fn tracker_under(&mut self, position: &CommitPosition) -> Option<&mut PartitionTracker> {
        self.partitions
            .get_mut(&position.partition)
            .filter(|tracker| tracker.epoch == position.epoch)
    }

    /// Whether any partition has commit work pending (see
    /// `PartitionTracker::has_committable`).
    fn has_committable(&self) -> bool {
        self.partitions
            .values()
            .any(PartitionTracker::has_committable)
    }

    /// Applies all queued rebalance events from librdkafka's callbacks.
    /// Cheap when the channel is empty (a single failed `try_recv`), so
    /// callers run it every loop iteration. Returns whether an assign event
    /// was among them: a partition this member is handed, or handed back,
    /// keeps the pause flag librdkafka last gave it, so the loop must
    /// re-apply its intent then, see [`reconcile_pause_after_assign`].
    fn apply_rebalance_events(&mut self, rx: &std_mpsc::Receiver<RebalanceEvent>) -> bool {
        let mut assigned = false;
        while let Ok(event) = rx.try_recv() {
            assigned |= matches!(event, RebalanceEvent::Assign(_));
            let (RebalanceEvent::Assign(partitions) | RebalanceEvent::Revoke(partitions)) = event;
            self.rebalanced(partitions);
        }
        assigned
    }

    /// Applies one rebalance: every partition in `moved` is dropped, see
    /// [`remove`](Self::remove), and every one this member keeps re-offers
    /// its position. The batch loop drains the events itself, because it
    /// also judges its in-flight batch by them, and hands the partitions
    /// they moved over here in one call, which leaves the tracker as the
    /// events one at a time would.
    fn rebalanced(&mut self, moved: impl IntoIterator<Item = i32>) {
        for partition in moved {
            self.remove(partition);
        }
        // A commit made while the group rebalances can be refused with a
        // rebalance in progress or a stale generation. Re-offer every
        // retained partition's position once the dust settles;
        // re-committing an already-committed offset keeps the same
        // logical position, at the cost of one more write to
        // __consumer_offsets. The rebalance always ends with an assign
        // round, so the last re-offer lands after the group is stable
        // again.
        //
        // `mark_dirty_resolved`, not `mark_dirty`: a rebalance is
        // evidence the member is still talking to the coordinator,
        // which is the opposite of the fenced signature, so it must
        // clear the streak rather than start one.
        for tracker in self.partitions.values_mut() {
            tracker.mark_dirty_resolved();
        }
    }

    /// Retires the span a batch flush settled on `partition`: every offset
    /// in `[start, end)` delivered under `epoch` is complete, see
    /// `PartitionTracker::complete_span`. `committed` says the flush
    /// committed `end` itself and the coordinator accepted it, so the
    /// tracker takes `end` as offered and ends the partition's streak.
    /// A partition revoked since, or revoked and handed back, is no longer
    /// under `epoch`, and nothing is retired on it.
    fn retire_span(&mut self, partition: i32, epoch: u64, start: i64, end: i64, committed: bool) {
        let Some(tracker) = self
            .partitions
            .get_mut(&partition)
            .filter(|tracker| tracker.epoch == epoch)
        else {
            return;
        };
        tracker.complete_span(start, end);
        if committed {
            tracker.committed_through(end);
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
    /// The commit carries, beside the list, each partition's position with
    /// the epoch it was drained under and the terminal discards those
    /// positions retire, so its answer settles on the assignment it was made
    /// for, see `commit_accepted` and `commit_rejected`.
    fn drain_committable(&mut self) -> Option<DrainedCommit> {
        let mut drained: Option<DrainedCommit> = None;
        for (&partition, tracker) in &mut self.partitions {
            let Some((offset, covered)) = tracker.drain_committable() else {
                continue;
            };
            let epoch = tracker.epoch;
            let commit = drained.get_or_insert_with(DrainedCommit::default);
            if let Err(e) =
                commit
                    .tpl
                    .add_partition_offset(&self.topic, partition, Offset::Offset(offset))
            {
                // As in `drain_all`: a position the commit cannot carry is
                // never confirmed by it, so its discards are not retired.
                tracing::error!(
                    queue = %self.topic,
                    partition,
                    offset,
                    error = %e,
                    "could not add the partition to the offset commit; its position is not committed"
                );
                for (_, discard) in covered {
                    discard.survived();
                }
                continue;
            }
            commit.positions.push(CommitPosition {
                partition,
                offset,
                epoch,
            });
            commit
                .discards
                .extend(covered.into_iter().map(|(offset, discard)| CoveredDiscard {
                    partition,
                    offset,
                    epoch,
                    discard,
                }));
        }
        drained.filter(|commit| !commit.positions.is_empty())
    }

    /// Every partition's current position for the final commit at shutdown,
    /// see `PartitionTracker::drain_all`, with the discards those positions
    /// retire, each with the coordinates it rides on, so a commit refused
    /// while the group rebalances can hand them back for its next try, see
    /// [`re_pend`](Self::re_pend). `None` when no partition is tracked: the
    /// member has nothing to commit.
    fn drain_all(&mut self) -> Option<(TopicPartitionList, Vec<CoveredDiscard>)> {
        let mut tpl: Option<TopicPartitionList> = None;
        let mut discards = Vec::new();
        for (&partition, tracker) in &mut self.partitions {
            let (commit_offset, covered) = tracker.drain_all();
            let epoch = tracker.epoch;
            match tpl
                .get_or_insert_with(TopicPartitionList::new)
                .add_partition_offset(&self.topic, partition, Offset::Offset(commit_offset))
            {
                Ok(()) => {
                    discards.extend(covered.into_iter().map(|(offset, discard)| CoveredDiscard {
                        partition,
                        offset,
                        epoch,
                        discard,
                    }))
                }
                Err(e) => {
                    // A partition left out here is a position the final
                    // commit never carries, which is what that commit's
                    // result exists to surface; say so instead of dropping
                    // it in silence. The entry stays in the list with an
                    // invalid offset, which librdkafka leaves out of the
                    // request. A commit that does not carry the position
                    // cannot confirm what it would have retired, so those
                    // discards are settled as survived here instead of
                    // riding the commit.
                    tracing::error!(
                        queue = %self.topic,
                        partition,
                        offset = commit_offset,
                        error = %e,
                        "could not add the partition to the final commit; its position is not committed"
                    );
                    for (_, discard) in covered {
                        discard.survived();
                    }
                }
            }
        }
        tpl.map(|tpl| (tpl, discards))
    }

    /// Hands the discards of a commit that was not accepted back to their
    /// partitions, so the re-offer of the position carries them and the
    /// commit that finally lands retires them exactly once. A discard whose
    /// partition was revoked meanwhile, or revoked and handed back since, is
    /// settled as survived: the partition's owner redelivers the message, the
    /// rule `remove` applies.
    fn re_pend(&mut self, discards: Vec<CoveredDiscard>) {
        for covered in discards {
            match self.partitions.get_mut(&covered.partition) {
                Some(tracker) if tracker.epoch == covered.epoch => {
                    tracker
                        .pending_discards
                        .insert(covered.offset, covered.discard);
                }
                _ => covered.discard.survived(),
            }
        }
    }
}

/// The `(partition, offset)` pairs a commit carries, in partition order, for
/// `FailedCommit::offsets` and the shutdown diagnostics. Positions only:
/// nothing of the records behind them.
fn committed_offsets(tpl: &TopicPartitionList) -> Vec<(i32, i64)> {
    let mut offsets: Vec<(i32, i64)> = tpl
        .elements()
        .iter()
        .filter_map(|e| match e.offset() {
            Offset::Offset(offset) => Some((e.partition(), offset)),
            _ => None,
        })
        .collect();
    offsets.sort_unstable();
    offsets
}

/// One partition's position in a drained commit, with the epoch of the
/// assignment it was drained under, see `OffsetTracker::track_received`.
#[derive(Debug, Clone, Copy)]
struct CommitPosition {
    partition: i32,
    offset: i64,
    epoch: u64,
}

/// The `(partition, offset)` pairs `positions` carry, in partition order:
/// what `committed_offsets` reads off a list, read off a drained commit.
fn position_offsets(positions: &[CommitPosition]) -> Vec<(i32, i64)> {
    let mut offsets: Vec<(i32, i64)> = positions
        .iter()
        .map(|position| (position.partition, position.offset))
        .collect();
    offsets.sort_unstable();
    offsets
}

/// What one drain of the tracker hands the commit it builds: the list to
/// commit, the positions it carries, and the discards those positions
/// retire. See `OffsetTracker::drain_committable`.
#[derive(Default)]
struct DrainedCommit {
    tpl: TopicPartitionList,
    positions: Vec<CommitPosition>,
    discards: Vec<CoveredDiscard>,
}

/// A terminal discard a drained commit position covers, with the
/// coordinates it rides on and the epoch of the assignment it was drained
/// under, so a commit the coordinator does not accept can hand it back to
/// the tracker (`OffsetTracker::re_pend`) instead of settling it. Settling
/// delegates to the discard itself.
struct CoveredDiscard {
    partition: i32,
    offset: i64,
    epoch: u64,
    discard: TerminalDiscard,
}

impl CoveredDiscard {
    /// The commit landed: the message is genuinely gone.
    fn confirm(self) {
        self.discard.confirm();
    }

    /// The commit did not land, so the message will be redelivered.
    fn survived(self) {
        self.discard.survived();
    }
}

// ---------------------------------------------------------------------------
// Metadata extraction functions
// ---------------------------------------------------------------------------

/// The record's headers whose key and value are both valid UTF-8, a repeated
/// key keeping its last value: the map `MessageMetadata::headers` hands a
/// handler.
///
/// Read through librdkafka's C API from the message itself: rdkafka's
/// `Headers` accessors unwrap every key as UTF-8, and the protocol lets a
/// producer send any bytes there. librdkafka reports a key up to its first
/// NUL byte. A header block librdkafka cannot parse reads as no headers, as
/// rdkafka's own `headers()` reads it.
fn extract_string_headers(msg: &BorrowedMessage<'_>) -> Arc<HashMap<String, String>> {
    let mut out = HashMap::new();
    let mut list = std::ptr::null_mut();
    // SAFETY: `msg.ptr()` is the live message `msg` borrows; on success
    // librdkafka points `list` at that message's own header list, which lives
    // as long as `msg` and is only read here.
    let err = unsafe { rd_kafka_message_headers(msg.ptr(), &mut list) };
    if err == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR && !list.is_null() {
        // SAFETY: as above.
        let count = unsafe { rd_kafka_header_cnt(list) };
        for idx in 0..count {
            let mut name = std::ptr::null();
            let mut value = std::ptr::null();
            let mut size = 0;
            // SAFETY: as above, with `idx` below the list's count.
            let err =
                unsafe { rd_kafka_header_get_all(list, idx, &mut name, &mut value, &mut size) };
            if err != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR
                || name.is_null()
                || value.is_null()
            {
                continue;
            }
            // SAFETY: librdkafka keeps every name NUL-terminated and a value
            // as `size` bytes, both inside the list `msg` owns.
            let (key, value) = unsafe {
                (
                    CStr::from_ptr(name),
                    std::slice::from_raw_parts(value.cast::<u8>(), size),
                )
            };
            if let (Ok(key), Ok(value)) = (key.to_str(), std::str::from_utf8(value)) {
                out.insert(key.to_owned(), value.to_owned());
            }
        }
    }
    Arc::new(out)
}

/// A record's headers, read once at receive.
///
/// `strings` is the map a handler sees, see [`extract_string_headers`].
/// `wire` is the record's own header list, copied out of librdkafka byte for
/// byte: a republished or dead-lettered copy is built from it, so the copy
/// keeps every header the record carried, in order, binary and null values,
/// repeated keys and keys that are not UTF-8 included. `None` when the record
/// carries no headers. `partition` and `offset` name a record that carries
/// no id, see [`base_message_id`](Self::base_message_id).
struct RecordHeaders {
    strings: Arc<HashMap<String, String>>,
    wire: Option<OwnedHeaders>,
    partition: i32,
    offset: i64,
}

impl RecordHeaders {
    fn read(msg: &BorrowedMessage<'_>) -> Self {
        Self::with_strings(msg, extract_string_headers(msg))
    }

    /// For a caller that has read `strings` off `msg` already.
    fn with_strings(msg: &BorrowedMessage<'_>, strings: Arc<HashMap<String, String>>) -> Self {
        Self {
            strings,
            wire: msg.headers().map(BorrowedHeaders::detach),
            partition: msg.partition(),
            offset: msg.offset(),
        }
    }

    /// The id every copy of the record on `topic` extends: the base a copy
    /// carries in `Shove-Base-Message-Id`, else the record's own
    /// `Shove-Message-Id`, else the record's position,
    /// `{topic}-{partition}-{offset}`, so two records that carry no id never
    /// share one. An empty header counts as absent, and nothing is read from
    /// the shape of an id.
    fn base_message_id(&self, topic: &str) -> Cow<'_, str> {
        [BASE_MESSAGE_ID_HEADER, MESSAGE_ID_HEADER]
            .into_iter()
            .find_map(|key| self.strings.get(key).filter(|id| !id.is_empty()))
            .map_or_else(
                || Cow::Owned(format!("{topic}-{}-{}", self.partition, self.offset)),
                |id| Cow::Borrowed(id.as_str()),
            )
    }

    /// Every header of the record minus each `rewritten` key: the headers of
    /// a copy shove publishes on the record's behalf, before the copy sets
    /// those keys afresh.
    ///
    /// Writes to the copy through librdkafka. `OwnedHeaders` has no method
    /// that removes a header, so this reaches the list behind it the way
    /// rdkafka's own accessors do, through the address of the zero-sized
    /// `BorrowedHeaders` that `as_borrowed` hands out.
    fn copy_without(&self, rewritten: &[&CStr]) -> OwnedHeaders {
        let copy = self.wire.clone().unwrap_or_default();
        let list = std::ptr::from_ref(copy.as_borrowed())
            .cast_mut()
            .cast::<RDKafkaHeaders>();
        for key in rewritten {
            // SAFETY: `list` is the list `copy` owns, which this call made and
            // nothing else reads while librdkafka edits it, and `key` is
            // NUL-terminated. A key the list lacks answers `__NOENT`: there is
            // nothing to drop.
            unsafe { rd_kafka_header_remove(list, key.as_ptr()) };
        }
        copy
    }
}

// `copy_without` relies on this: a reference to a zero-sized type is only an address.
const _: () = assert!(size_of::<BorrowedHeaders>() == 0);

fn get_retry_count(headers: &HashMap<String, String>) -> u32 {
    headers
        .get(RETRY_COUNT_HEADER)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
}

/// Where a record sits in its topic: the partition, the offset inside it and
/// the broker timestamp when the record carries one. Read once off the
/// `BorrowedMessage` and carried to every `MessageMetadata` built for the
/// delivery, including a deferred broadcast redelivery, which keeps the
/// original record's coordinates rather than inventing new ones.
#[derive(Debug, Clone, Copy)]
struct RecordCoordinates {
    partition: i32,
    offset: i64,
    timestamp_ms: Option<i64>,
}

impl RecordCoordinates {
    fn of(msg: &BorrowedMessage<'_>) -> Self {
        Self {
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp_ms: msg.timestamp().to_millis(),
        }
    }
}

fn build_message_metadata(
    headers: &Arc<HashMap<String, String>>,
    redelivered: bool,
    coordinates: RecordCoordinates,
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
        // What Kafka does have is a position in the log, which is exactly
        // what an audit trail or a replay request needs.
        partition: Some(coordinates.partition),
        offset: Some(coordinates.offset),
        timestamp_ms: coordinates.timestamp_ms,
        headers: Arc::clone(headers),
    }
}

fn build_dead_metadata(
    headers: &Arc<HashMap<String, String>>,
    coordinates: RecordCoordinates,
) -> DeadMessageMetadata {
    let message = build_message_metadata(headers, false, coordinates);
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

/// The hop a copy of a record makes, which names the suffix of the copy's
/// `Shove-Message-Id`.
enum Hop {
    /// A `Retry` republish as this attempt.
    Retry(u32),
    /// A `Defer` republish.
    Defer,
    /// A dead letter with this death count.
    DeadLetter(u32),
}

/// A copy's `Shove-Message-Id`: its record's base and one suffix for the
/// hop. A retry's id is the same for the same attempt, so a republish
/// repeated for one attempt carries one id; a defer spends no attempt, so a
/// fresh UUID tells its hops apart; a dead letter's carries its death count,
/// so each death of a record has its own. The base comes from the record, so
/// a later copy replaces the suffix and the id stays one suffix longer than
/// the base however many hops the record makes.
fn copy_message_id(base: &str, hop: Hop) -> String {
    match hop {
        Hop::Retry(attempt) => format!("{base}-r{attempt}"),
        Hop::Defer => {
            let mut buf = Uuid::encode_buffer();
            let uuid = Uuid::new_v4().hyphenated().encode_lower(&mut buf);
            format!("{base}-d{uuid}")
        }
        Hop::DeadLetter(death_count) => format!("{base}-dlq{death_count}"),
    }
}

/// The headers of a `Retry` or `Defer` republish of `original` into
/// `topic`: every header the record carried, byte for byte and in order,
/// with shove's retry count, base id and message id rewritten.
fn headers_with_retry_count(
    original: &RecordHeaders,
    topic: &str,
    retry_count: u32,
    hop: Hop,
) -> OwnedHeaders {
    let base = original.base_message_id(topic);
    original
        .copy_without(&[RETRY_COUNT_KEY, BASE_MESSAGE_ID_KEY, MESSAGE_ID_KEY])
        .insert(Header {
            key: RETRY_COUNT_HEADER,
            value: Some(retry_count.to_string().as_bytes()),
        })
        .insert(Header {
            key: BASE_MESSAGE_ID_HEADER,
            value: Some(base.as_bytes()),
        })
        .insert(Header {
            key: MESSAGE_ID_HEADER,
            value: Some(copy_message_id(&base, hop).as_bytes()),
        })
}

/// The headers a dead-lettered record carries: every header of the
/// original, byte for byte and in order, with shove's own rewritten.
///
/// `retry_count` is the number of retries the record had used when it died.
/// On a shove-owned topic every retry republished the record with the count
/// in `RETRY_COUNT_HEADER`, so it equals the header's value; on an external
/// topic the retries happened in place and the count lived only in memory,
/// so writing it here is what lets a DLQ consumer's `retry_count` report the
/// attempts that were actually made.
fn headers_for_dlq(
    original: &RecordHeaders,
    reason: &str,
    original_queue: &str,
    retry_count: u32,
) -> OwnedHeaders {
    let death_count = original
        .strings
        .get(DEATH_COUNT_HEADER)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
        .saturating_add(1);
    let base = original.base_message_id(original_queue);
    original
        .copy_without(&[
            DEATH_REASON_KEY,
            ORIGINAL_QUEUE_KEY,
            DEATH_COUNT_KEY,
            BASE_MESSAGE_ID_KEY,
            MESSAGE_ID_KEY,
            RETRY_COUNT_KEY,
        ])
        .insert(Header {
            key: RETRY_COUNT_HEADER,
            value: Some(retry_count.to_string().as_bytes()),
        })
        .insert(Header {
            key: DEATH_REASON_HEADER,
            value: Some(reason.as_bytes()),
        })
        .insert(Header {
            key: ORIGINAL_QUEUE_HEADER,
            value: Some(original_queue.as_bytes()),
        })
        .insert(Header {
            key: DEATH_COUNT_HEADER,
            value: Some(death_count.to_string().as_bytes()),
        })
        .insert(Header {
            key: BASE_MESSAGE_ID_HEADER,
            value: Some(base.as_bytes()),
        })
        .insert(Header {
            key: MESSAGE_ID_HEADER,
            value: Some(copy_message_id(&base, Hop::DeadLetter(death_count)).as_bytes()),
        })
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

/// Publishes `payload` to the topology's DLQ with [`headers_for_dlq`]
/// headers; `retry_count` is that function's. A no-op with a warning when
/// the topology has no DLQ.
async fn publish_to_dlq(
    client: &KafkaClient,
    topology: &QueueTopology,
    payload: &[u8],
    key: Option<&[u8]>,
    headers: &RecordHeaders,
    reason: &str,
    retry_count: u32,
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

    #[cfg(feature = "test-support")]
    if dlq_publish_probe::refused() {
        return Err(ShoveError::Topology(format!(
            "DLQ publish to '{dlq_topic}' refused by the test seam"
        )));
    }

    let dlq_headers = headers_for_dlq(headers, reason, topology.queue(), retry_count);
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

/// Test-only switch (see the `test-support` feature) on [`publish_to_dlq`]:
/// it refuses every dead letter with the `ShoveError::Topology` a client
/// whose producer cannot be built answers with, so a test can drive a
/// publish failure that no retry gets past. nextest runs each test in its own
/// process, so the switch belongs to that test's consumers alone.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod dlq_publish_probe {
    use std::sync::atomic::{AtomicBool, Ordering};

    static REFUSE: AtomicBool = AtomicBool::new(false);

    /// Refuse, or with `false` allow again, every dead-letter publish.
    pub fn refuse(refuse: bool) {
        REFUSE.store(refuse, Ordering::SeqCst);
    }

    pub(super) fn refused() -> bool {
        REFUSE.load(Ordering::SeqCst)
    }
}

/// Whether a failed publish can never succeed: `publish_with_retry` answers
/// a record the producer or the broker refuses as too large or as invalid
/// with `ShoveError::Validation`, and sends it no more.
fn is_unpublishable(e: &ShoveError) -> bool {
    matches!(e, ShoveError::Validation(_))
}

/// Completion handle for the concurrent consumer path.
///
/// Threaded into [`route_outcome`] so the function can signal offset-commit
/// readiness exactly once per message — synchronously for terminal outcomes
/// (Ack, DLQ-terminal Retry/Reject), or from inside the delayed-republish
/// spawn for Retry/Defer **after** the republish has actually landed. This
/// closes the at-least-once gap that existed when the outer task signaled
/// completion before the delayed publish had been attempted.
type CompletionHandle = (mpsc::Sender<Completion>, i32, i64, u64);

/// A finished delivery handed back to the offset tracker.
struct Completion {
    partition: i32,
    offset: i64,
    /// The epoch of the assignment the delivery was tracked under, see
    /// `OffsetTracker::track_received`. A completion from an earlier
    /// assignment of the partition is ignored, so a handler that finishes
    /// after its partition moved away and came back cannot complete the
    /// current assignment's delivery of the same offset.
    epoch: u64,
    /// Discard accounting that must not be counted until this offset is
    /// actually committed, so it rides along with the offset it depends on.
    /// See [`PartitionTracker::pending_discards`].
    discard: Option<TerminalDiscard>,
}

impl Completion {
    /// A completion with no terminal accounting riding on it — an `Ack`, a
    /// landed republish, or a pre-handler path that routed the message
    /// somewhere it still exists.
    ///
    /// Production call sites go through [`signal_completion`], which builds
    /// its own `Completion`; this constructor is now exercised only by the
    /// `OffsetTracker`/`PartitionTracker` unit tests below, which need a
    /// completion carrying no discard without reaching for `signal_completion`
    /// itself.
    ///
    /// The epoch is the one a fresh tracker gives a partition's first
    /// assignment, 1, which is what those tests seed; `under` names another.
    #[cfg_attr(not(test), allow(dead_code))]
    fn plain(partition: i32, offset: i64) -> Self {
        Self::under(1, partition, offset)
    }

    /// `plain` under an explicit assignment epoch.
    #[cfg_attr(not(test), allow(dead_code))]
    fn under(epoch: u64, partition: i32, offset: i64) -> Self {
        Self {
            partition,
            offset,
            epoch,
            discard: None,
        }
    }
}

/// Hand this delivery's offset to the tracker so it can be committed.
///
/// Returns whether the offset was actually handed over. When it was not, it
/// is never committed, so the message is redelivered from the last
/// committed position on the next rebalance or restart — callers about to
/// assert the message is gone must not do so when this returns `false`.
/// The channel refuses it in two ways. Closed, the receive loop has ended,
/// which a fault does with sibling handlers still running, and the
/// reconnect or the next owner redelivers the message; that is logged at
/// debug. Full, the channel's bound is wrong, a logic bug logged at error.
///
/// **`true` is not a broker acknowledgement**, and terminal accounting is
/// therefore *not* settled here. It says only that the offset entered the
/// in-process tracker; the commit can still fail, and a redelivered message
/// is one `messages_discarded_total` promised no longer exists.
///
/// So a `discard` handed in here travels with its offset into the tracker and
/// is settled by the commit that covers it, which the receive loop makes
/// with `CommitMode::Sync` on a thread of its own and reads the answer of,
/// see [`CommitSlot`]: rust-rdkafka's `Sync` waits for the broker to finish
/// the commit, which is the positive signal hand-off cannot give. A commit
/// that fails, or a partition revoked before one lands, settles as
/// `survived`: undercounting an ambiguous retirement is safe, claiming one
/// that did not happen is not.
fn signal_completion(
    handle: CompletionHandle,
    queue: &str,
    discard: Option<TerminalDiscard>,
) -> bool {
    let (tx, partition, offset, epoch) = handle;
    let refused = match tx.try_send(Completion {
        partition,
        offset,
        epoch,
        discard,
    }) {
        Ok(()) => {
            #[cfg(feature = "test-support")]
            completion_probe::QUEUED.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        Err(mpsc::error::TrySendError::Closed(completion)) => {
            tracing::debug!(
                queue,
                partition,
                offset,
                "completion arrived after its receive loop ended; the record is redelivered"
            );
            completion
        }
        Err(mpsc::error::TrySendError::Full(completion)) => {
            #[cfg(feature = "test-support")]
            completion_probe::REFUSED.fetch_add(1, Ordering::SeqCst);
            tracing::error!(
                queue,
                partition,
                offset,
                "completion channel full — logic bug in offset tracker"
            );
            completion
        }
    };
    // The offset never reaches the tracker, so it is never committed and
    // the message is redelivered — the opposite of retired.
    if let Some(discard) = refused.discard {
        discard.survived();
    }
    false
}

/// Test-only probe (see the `test-support` feature) on the completion
/// channel: how many completions `signal_completion` queued and how many it
/// refused for a full channel, process-wide. The channel's sender is internal
/// to the receive loop, so its occupancy cannot be read from a test any other
/// way. nextest runs each test in its own process, so both start at zero.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod completion_probe {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub static QUEUED: AtomicUsize = AtomicUsize::new(0);
    pub static REFUSED: AtomicUsize = AtomicUsize::new(0);

    /// Completions `try_send` accepted so far.
    pub fn queued() -> usize {
        QUEUED.load(Ordering::SeqCst)
    }

    /// Completions `try_send` refused for a full channel so far.
    pub fn refused() -> usize {
        REFUSED.load(Ordering::SeqCst)
    }
}

/// Test-only counters (see the `test-support` feature) on the four `put_back`
/// call sites, one per site, so a test can prove that a record went back to
/// the broker on the path it drives instead of inferring it from timing.
/// nextest runs each test in its own process, so every counter starts at
/// zero.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod put_back_probe {
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A record received while `RegistryStall::wait` polled through a stall.
    pub static REGISTRY_STALL: AtomicUsize = AtomicUsize::new(0);
    /// A record received while `acquire_permit_while_polling` waited for
    /// prefetch permits: for the record in hand, or in the shutdown drain.
    pub static PERMIT_WAIT: AtomicUsize = AtomicUsize::new(0);
    /// A record the receive arm put back because the assignment was, or had
    /// to be, paused for waiting handlers.
    pub static PAUSED_RECEIVE: AtomicUsize = AtomicUsize::new(0);
    /// A record on another partition put back behind a rewound batch span.
    pub static BATCH_REWIND: AtomicUsize = AtomicUsize::new(0);
    /// A record received while `answer_while_polling` waited for a commit's
    /// answer.
    pub static COMMIT_WAIT: AtomicUsize = AtomicUsize::new(0);

    pub fn registry_stall() -> usize {
        REGISTRY_STALL.load(Ordering::SeqCst)
    }

    pub fn permit_wait() -> usize {
        PERMIT_WAIT.load(Ordering::SeqCst)
    }

    pub fn paused_receive() -> usize {
        PAUSED_RECEIVE.load(Ordering::SeqCst)
    }

    pub fn batch_rewind() -> usize {
        BATCH_REWIND.load(Ordering::SeqCst)
    }

    pub fn commit_wait() -> usize {
        COMMIT_WAIT.load(Ordering::SeqCst)
    }
}

/// Test-only probe (see the `test-support` feature) on the two waits for a
/// permit: `acquire_permit_while_polling` on the receive loop, and the slot
/// wait of the broadcast loop. Two counters count the records each loop has
/// held while it waited, the ones handed a free permit at once included, so
/// a test that must have a record in that wait before it goes on, as the
/// stop tests do, reads them instead of sleeping. One hook runs once, in the
/// instant either loop has a permit in hand and before it reads the stop and
/// the record's assignment again (see `after_wait`): the one way a test can
/// land a stop inside that window on cue, which no order of broker events
/// reaches on its own. nextest runs each test in its own process, so the
/// counters start at zero and the hook starts empty.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod permit_wait_probe {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub static ENTERED: AtomicUsize = AtomicUsize::new(0);
    pub static BROADCAST_ENTERED: AtomicUsize = AtomicUsize::new(0);
    static ON_ACQUIRED: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

    /// Records the receive loop held for a permit so far.
    pub fn entered() -> usize {
        ENTERED.load(Ordering::SeqCst)
    }

    /// Records the broadcast loop held for its slot so far.
    pub fn broadcast_entered() -> usize {
        BROADCAST_ENTERED.load(Ordering::SeqCst)
    }

    /// Install `hook` to run once, the next time a loop has a permit in
    /// hand for a record and has not yet read the stop again.
    pub fn on_acquired(hook: impl FnOnce() + Send + 'static) {
        *ON_ACQUIRED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(hook));
    }

    /// Runs the installed hook, if any, and uninstalls it.
    pub(super) fn acquired() {
        let hook = ON_ACQUIRED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

/// Runs a decode until it answers or `shutdown` fires, whichever comes
/// first. `None` means shutdown fired.
///
/// A registry lookup can take the client's timeout and retries to fail,
/// about twenty seconds with the defaults, and a shutdown that landed
/// meanwhile used to wait all of it out. Dropping the request future here
/// takes this waiter off the registry client's single-flight entry (see
/// `SchemaRegistry::resolve`), and a fetch nobody waits on any more is
/// dropped with it. Each caller decides what its loop does next; every one
/// leaves the record in hand uncommitted.
async fn decode_or_shutdown<F, T>(decode: F, shutdown: &CancellationToken) -> Option<T>
where
    F: Future<Output = T>,
{
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => None,
        out = decode => Some(out),
    }
}

/// The wait a consume loop performs while the schema registry cannot answer
/// for the record in hand.
///
/// Such a record is neither poison nor processed: discarding it would turn a
/// registry outage into data loss, and skipping it would break at-least-once.
/// So the loop keeps the record, pauses its assignment the way the
/// external-topic loop does while every permit is held by a waiting handler,
/// and decodes the same bytes again every `REGISTRY_RETRY_DELAY` until the
/// registry answers or shutdown fires. Each wait counts one
/// `messages_failed_total{reason="schema_unavailable"}` and warns with the
/// schema id; nothing terminal is recorded, so `messages_discarded_total`
/// does not move.
///
/// The wait keeps polling `recv()`, so the member stays in its group for
/// the whole outage instead of being evicted at `max.poll.interval.ms`. A
/// paused assignment yields nothing; the one thing `recv()` can still hand
/// over is a record from a partition the group assigned after the pause
/// took effect. That record is put back (see [`put_back`]) and the pause is
/// widened to the new assignment, so it is delivered again, in order, once
/// the wait ends.
#[cfg(feature = "kafka-schema-registry")]
#[derive(Default)]
struct RegistryStall {
    /// Whether a wait ran, so `end` has the loop's pause intent to re-apply.
    waited: bool,
    /// Whether the loop holds a pause of its own over the stalled record,
    /// as the concurrent loop does under `CommitPolicy::PerRecord` and a
    /// sequenced consumer does through every wait. The stall then pauses
    /// nothing in `wait`, and `end` re-applies that pause; see
    /// `inside_pause`.
    loop_holds_pause: bool,
    /// Whether the consumer never joins a group, as the broadcast
    /// subscription does not. Its inert group id still provokes a
    /// `GroupAuthorizationFailed` on every coordinator lookup under a
    /// restrictive ACL; the receive arm tolerates that answer, and so must
    /// the wait, or an outage would turn it into a reconnect.
    groupless: bool,
}

#[cfg(feature = "kafka-schema-registry")]
impl RegistryStall {
    /// A stall for the groupless broadcast subscription, see `groupless`.
    fn groupless() -> Self {
        Self {
            waited: false,
            loop_holds_pause: false,
            groupless: true,
        }
    }

    /// A stall in a loop that may already hold a pause of its own: under
    /// `CommitPolicy::PerRecord` the concurrent loop pauses the assignment
    /// the moment a record is taken, before its decode, and a sequenced
    /// consumer pauses it for every wait on the record in hand. A stall
    /// that starts inside that pause pauses nothing in `wait`, and `end`
    /// pauses the whole assignment again, which covers a partition the
    /// group assigned during the stall; the loop's own resume ends the
    /// pause, see the resume at the top of the concurrent loop's pass. A
    /// stall that resumed on its own there handed librdkafka a fetch for the
    /// next record while the loop's `paused` still held, and the receive
    /// arm put that record back and paused again: a fetch, a seek and a
    /// put-back per stall, and a pause state the broker and the loop no
    /// longer agreed on.
    fn inside_pause(loop_holds_pause: bool) -> Self {
        Self {
            waited: false,
            loop_holds_pause,
            groupless: false,
        }
    }

    /// Pause on the first wait, count and warn, then wait out
    /// `REGISTRY_RETRY_DELAY` while still polling. `Ok(false)` means shutdown
    /// fired during the wait. A fault a handler task reports over `fault_rx`
    /// meanwhile ends the wait with that error: the loop's own fault arm sits
    /// in a `select!` this wait is nested inside, and an outage has no time
    /// bound, so the wait must read the channel itself. The retry is read
    /// last: a rebalance event librdkafka has queued is applied by the
    /// `recv()` poll, inside which a revoke of the stalled record's partition
    /// cancels the record's assignment token, and the concurrent loop reads
    /// that token before it retries the decode, see `after_wait`. An
    /// unbiased pick could take the retry first and leave the event for a
    /// later poll.
    #[allow(clippy::too_many_arguments)]
    async fn wait(
        &mut self,
        consumer: &KafkaStreamConsumer,
        shutdown: &CancellationToken,
        fault_rx: Option<&mut mpsc::Receiver<ShoveError>>,
        topic: &str,
        group: Option<&str>,
        queue: &str,
        id: SchemaId,
        error: &SchemaRegistryError,
    ) -> Result<bool> {
        if !self.waited {
            if !self.loop_holds_pause {
                consumer
                    .pause_assignment()
                    .map_err(|e| map_kafka_error("pause failed", e))?;
            }
            self.waited = true;
        }
        tracing::warn!(
            queue,
            schema_id = %id,
            error = %error,
            retry_after = ?REGISTRY_RETRY_DELAY,
            "schema registry unavailable; keeping the record and retrying"
        );
        metrics::record_failed(topic, group, metrics::FailReason::SchemaUnavailable);
        let retry = tokio::time::sleep(REGISTRY_RETRY_DELAY);
        tokio::pin!(retry);
        let fault = handler_fault(fault_rx);
        tokio::pin!(fault);
        loop {
            tokio::select! {
                // Read in this order, the retry last; see the doc above.
                biased;
                _ = shutdown.cancelled() => return Ok(false),
                e = &mut fault => return Err(e),
                received = consumer.recv() => {
                    let msg = match received {
                        Ok(msg) => msg,
                        Err(e) if self.groupless && is_ignorable_groupless_error(&e) => continue,
                        Err(e) if recv_error_is_transient(consumer.transport_gate(), &e) => {
                            log_transient_recv_error(queue, &e);
                            continue;
                        }
                        Err(e) => {
                            return Err(map_kafka_error(
                                &format!("consumer recv error on {queue} during a schema registry wait"),
                                e,
                            ));
                        }
                    };
                    #[cfg(feature = "test-support")]
                    put_back_probe::REGISTRY_STALL.fetch_add(1, Ordering::SeqCst);
                    put_back(consumer, queue, msg.partition(), msg.offset())?;
                    consumer
                        .pause_assignment()
                        .map_err(|e| map_kafka_error("pause failed", e))?;
                    tracing::debug!(
                        queue,
                        partition = msg.partition(),
                        offset = msg.offset(),
                        "record from a partition assigned during a schema registry wait put back; pause widened"
                    );
                }
                _ = &mut retry => return Ok(true),
            }
        }
    }

    /// Re-applies the loop's pause intent over its whole assignment once
    /// the registry has answered, if a wait ran: resumed, or paused again
    /// when the loop holds a pause of its own, see [`apply_pause_intent`].
    /// Either way a partition the group assigned or handed back during the
    /// stall follows the loop.
    fn end(&mut self, consumer: &KafkaStreamConsumer) -> Result<()> {
        if !std::mem::take(&mut self.waited) {
            return Ok(());
        }
        apply_pause_intent(consumer, self.loop_holds_pause)
    }
}

/// What the decode stage left a consume loop with, once any registry wait is
/// over: the concurrent loop's registry decode, every decode of the
/// sequenced loop, and the DLQ drain's.
enum Staged<M> {
    /// Decoded: hand it to the handler.
    Ready(M),
    /// A pre-handler drop under this reason pair.
    Drop(metrics::FailReason, &'static str),
    /// Shutdown fired before the decode or during a registry wait. The
    /// record is untouched and uncommitted; the loop's stop takes over.
    Stop,
    /// The record's partition was revoked during a registry wait, as the
    /// record's assignment token tells. The record is untouched and left to
    /// the partition's next owner; the loop's next pass drains the revoke.
    #[cfg_attr(not(feature = "kafka-schema-registry"), allow(dead_code))]
    Revoked,
}

/// Dead-letter a record of the concurrent path under `reason`, with
/// `retry_count` as the dead letter's count, and complete it once it is
/// finished with: in the DLQ, dropped when no DLQ is declared, or lost to a
/// dead letter that can never be published. `pending` is the record's
/// discard. It settles at once when the dead letter is in the DLQ, and
/// otherwise rides the offset into the tracker, settled by the commit that
/// covers it, as lost when the dead letter could not be published.
///
/// `Err` is a dead letter that failed otherwise, the fault [`publish_fault`]
/// makes of it. Nothing is completed, so the record stays in flight and no
/// commit passes it, and `pending` settles as survived. The caller ends the
/// member with the error: the reconnect a retryable one leads to resumes
/// from the committed position and redelivers the record, and any other
/// error ends the member with nothing committed.
#[allow(clippy::too_many_arguments)]
async fn dead_letter_and_complete(
    client: &KafkaClient,
    topology: &'static QueueTopology,
    topic: &str,
    payload: &[u8],
    key: Option<&[u8]>,
    headers: &RecordHeaders,
    reason: &str,
    retry_count: u32,
    pending: metrics::PendingDiscard,
    completion: CompletionHandle,
) -> Result<()> {
    let has_dlq = topology.dlq().is_some();
    let published =
        publish_to_dlq(client, topology, payload, key, headers, reason, retry_count).await;
    let discard = match published {
        Ok(()) => match reject_settlement(has_dlq, true) {
            RejectSettlement::InDlq => {
                // Settling now keeps the ordinary dead-letter path off the
                // synchronous commit a pending record would force.
                pending.survived();
                None
            }
            _ => Some(TerminalDiscard::Retired(pending)),
        },
        Err(e) if is_unpublishable(&e) => {
            tracing::error!(
                error = %e,
                queue = topic,
                reason,
                "the dead letter can never be published; the record is discarded"
            );
            // A DLQ was declared, since `publish_to_dlq` is `Ok(())` without
            // one, and nothing holds a copy.
            Some(TerminalDiscard::Lost(pending))
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                queue = topic,
                reason,
                retryable = e.is_retryable(),
                "DLQ publish failed; the record stays uncommitted"
            );
            pending.survived();
            return Err(publish_fault("DLQ publish", topic, e));
        }
    };
    signal_completion(completion, topic, discard);
    Ok(())
}

/// The member's fault for a republish or a dead letter that failed with `e`,
/// one `is_unpublishable` does not cover. A retryable error becomes a
/// `Connection` fault: the member reconnects, resumes from the committed
/// position and is handed the record again. Any other error is the fault as
/// it came, so the member ends with it, as `run_with_reconnect` ends on
/// every error it cannot retry.
fn publish_fault(what: &str, topic: &str, e: ShoveError) -> ShoveError {
    if e.is_retryable() {
        ShoveError::Connection(format!(
            "{what} on '{topic}' failed: {e}; the record stays uncommitted and the member \
             reconnects to redeliver it"
        ))
    } else {
        e
    }
}

/// Count and settle a message dropped before the handler on the concurrent
/// path: the failure is recorded now, and the record is dead-lettered and
/// completed by [`dead_letter_and_complete`], the transport the post-handler
/// terminal path uses too. `Err` is that function's fault, which the caller
/// reports as the member's: a dead letter that may land later, as the
/// `Connection` fault the member reconnects on, or one that failed with an
/// error no retry gets past, which ends the member.
#[allow(clippy::too_many_arguments)]
async fn discard_pre_handler(
    client: &KafkaClient,
    topology: &'static QueueTopology,
    topic: &str,
    group: Option<&str>,
    fail_reason: metrics::FailReason,
    dlq_reason: &str,
    payload: &[u8],
    key: Option<&[u8]>,
    headers: &RecordHeaders,
    completion_tx: &mpsc::Sender<Completion>,
    partition: i32,
    offset: i64,
    epoch: u64,
) -> Result<()> {
    let pending = metrics::record_terminal(topic, group, fail_reason, topology.dlq().is_some());
    let retry_count = get_retry_count(&headers.strings);
    dead_letter_and_complete(
        client,
        topology,
        topic,
        payload,
        key,
        headers,
        dlq_reason,
        retry_count,
        pending,
        (completion_tx.clone(), partition, offset, epoch),
    )
    .await
}

/// Settle the outcome of a record of the concurrent path: complete an
/// `Ack`, dead-letter a `Reject` or a spent budget, and republish a `Retry`
/// or `Defer` once its hold delay has passed, completing the record when the
/// copy landed. The caller holds the record's permit until this returns, so
/// prefetch bounds the records waiting out a hold.
///
/// A dead letter or a republish that failed and may succeed later completes
/// nothing: the record stays in flight, no commit passes it, and the error
/// is reported over `fault`, which ends the member under either commit
/// policy. The reconnect resumes from the committed position and the record
/// is redelivered. An error no retry gets past is reported as it came, so
/// the member ends with it instead of reconnecting, see [`publish_fault`]. A
/// copy that can never be published goes to the DLQ under the death reason
/// `republish_failed`.
#[allow(clippy::too_many_arguments)]
async fn route_outcome(
    client: &KafkaClient,
    topic: &str,
    // Optional consumer-group label propagated to `metrics::record_failed`
    // on DLQ-terminal outcomes (max_retries_exceeded, Rejected). Matches the
    // shape `invoke_handler` already uses.
    group: Option<&str>,
    payload: &[u8],
    key: Option<&[u8]>,
    headers: &RecordHeaders,
    outcome: Outcome,
    topology: &'static QueueTopology,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[HoldQueue],
    completion: CompletionHandle,
    // A hold ends early on a graceful shutdown, instead of stalling
    // `acquire_many(prefetch)` until every delayed permit-holder finishes,
    // and on a revoke of the record's partition; see `run_delayed_republish`.
    shutdown: &CancellationToken,
    revoked: &CancellationToken,
    fault: &mpsc::Sender<ShoveError>,
) {
    match decide_retry(&outcome, retry_count, max_retries) {
        RetryDecision::Ack => {
            let _ = signal_completion(completion, topic, None);
        }
        RetryDecision::Dlq { reason } => {
            let pending = metrics::record_terminal(
                topic,
                group,
                terminal_fail_reason(reason),
                topology.dlq().is_some(),
            );
            if let Err(e) = dead_letter_and_complete(
                client,
                topology,
                topic,
                payload,
                key,
                headers,
                reason,
                retry_count,
                pending,
                completion,
            )
            .await
            {
                // A full channel means a sibling already reported a fault.
                let _ = fault.try_send(e);
            }
        }
        RetryDecision::Hold { increment } => {
            let (count, hop, label) = if increment {
                let attempt = retry_count.saturating_add(1);
                (attempt, Hop::Retry(attempt), "retry republish")
            } else {
                // Defer does NOT increment retry count.
                (retry_count, Hop::Defer, "defer republish")
            };
            let copy = headers_with_retry_count(headers, topic, count, hop);
            let delay = hold_delay(hold_queues, retry_count, increment);
            match run_delayed_republish(
                client, topic, key, copy, payload, delay, shutdown, revoked, label,
            )
            .await
            {
                Republished::Landed => {
                    let _ = signal_completion(completion, topic, None);
                }
                Republished::Abandoned => {}
                Republished::Failed(e) if is_unpublishable(&e) => {
                    tracing::error!(
                        error = %e,
                        queue = topic,
                        label,
                        "the copy can never be published; dead-lettering the record"
                    );
                    let pending = metrics::record_terminal(
                        topic,
                        group,
                        metrics::FailReason::RepublishFailed,
                        topology.dlq().is_some(),
                    );
                    if let Err(e) = dead_letter_and_complete(
                        client,
                        topology,
                        topic,
                        payload,
                        key,
                        headers,
                        "republish_failed",
                        retry_count,
                        pending,
                        completion,
                    )
                    .await
                    {
                        let _ = fault.try_send(e);
                    }
                }
                Republished::Failed(e) => {
                    tracing::error!(
                        error = %e,
                        queue = topic,
                        label,
                        retryable = e.is_retryable(),
                        "delayed republish failed; the record stays uncommitted"
                    );
                    let _ = fault.try_send(publish_fault(label, topic, e));
                }
            }
        }
    }
}

/// The failure reason a DLQ-terminal decision counts under: an explicit
/// `Reject`, or a retry budget spent.
fn terminal_fail_reason(reason: &str) -> metrics::FailReason {
    match reason {
        "rejected" => metrics::FailReason::Rejected,
        _ => metrics::FailReason::MaxRetriesExceeded,
    }
}

// ---------------------------------------------------------------------------
// Broadcast — one groupless, assign-only subscription per call
// ---------------------------------------------------------------------------

/// How long `assign_all_partitions_at`'s metadata fetch may block.
///
/// Matches the timeout the topology declarer and the offset-reset path already
/// use for their own metadata round trips.
const BROADCAST_ASSIGN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often an assign-only broadcast subscriber checks for partitions added
/// after it connected. Manual assignments do not participate in rebalances, so
/// metadata growth has to be reflected explicitly.
const BROADCAST_ASSIGN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Whether a `recv` error on the groupless broadcast path is the one error
/// its inert `group.id` can provoke and nothing else.
///
/// librdkafka looks up the coordinator of the configured group even for a
/// consumer that only ever calls `assign()`. A cluster ACL that grants group
/// Describe on a prefix the inert id does not match answers that lookup with
/// `GroupAuthorizationFailed`. Fetching does not go through the coordinator,
/// so the subscription is unaffected, and a consumer that never joins has no
/// use for the permission. Every other error keeps its meaning: it ends the
/// connection and the reconnect loop decides.
fn is_ignorable_groupless_error(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::MessageConsumption(RDKafkaErrorCode::GroupAuthorizationFailed)
    )
}

/// Count a broadcast message that is being dropped before or instead of
/// handling.
///
/// `record_terminal` with `has_dlq: false` — a broadcast topology cannot
/// declare one — which resolves straight to `messages_discarded_total`. There
/// is no commit to wait on: nothing was ever going to hold a copy, so the
/// discard is true the moment it is decided.
fn discard_broadcast(topic: &str, group: Option<&str>, reason: metrics::FailReason) {
    metrics::record_terminal(topic, group, reason, false).confirm();
}

/// How an in-place redelivery loop on an infra-owned topic ended.
enum InPlaceEnd {
    /// The handler settled on an outcome `decide_retry` calls terminal: `Ack`,
    /// or a `Reject` or exhausted `Retry` bound for the DLQ. `attempts` is the
    /// retry count the terminal decision was made at.
    Terminal { outcome: Outcome, attempts: u32 },
    /// The retained bytes no longer decode, so the record cannot be handed
    /// back to the handler; the caller settles it as a pre-handler drop.
    /// `attempts` is the in-memory retry count at that point, which the dead
    /// letter carries as its `Shove-Retry-Count` in place of the count the
    /// record arrived with, as an exhausted in-place retry does.
    Undecodable {
        reason: &'static str,
        fail: metrics::FailReason,
        attempts: u32,
    },
    /// Shutdown fired during a wait. Nothing was completed, so the record is
    /// redelivered on restart.
    Cancelled,
    /// The record's partition was revoked during a wait. Nothing was
    /// completed: the partition's next owner, or this member under a fresh
    /// assignment, is handed the record again from the committed offset.
    Revoked,
    /// The schema registry answered with a deployment fault while the record
    /// was being decoded again. Nothing is completed: the record stays
    /// uncommitted, and the task reports the fault over the receive loop's
    /// fault channel, so the loop ends with it at once rather than on some
    /// later record that may never come.
    #[cfg(feature = "kafka-schema-registry")]
    Fatal(ShoveError),
}

/// The hold delay of a `Retry` or `Defer`: the tier at the attempt count for
/// a `Retry`, the last tier once the count is past them, the first tier for
/// a `Defer`, and one second without hold queues. The republishing arms of
/// `route_outcome`, an in-place redelivery and a sequenced retry all wait
/// it.
fn hold_delay(hold_queues: &[HoldQueue], attempts: u32, increment: bool) -> Duration {
    let tier = if increment {
        usize::try_from(attempts).unwrap_or(usize::MAX)
    } else {
        0
    };
    hold_queues
        .get(tier)
        .or(hold_queues.last())
        .map_or(Duration::from_secs(1), HoldQueue::delay)
}

/// Counts one handler task as waiting out an in-place delay for as long as
/// the guard lives, so the receive loop can tell a permit held by a waiting
/// handler from one held by a running handler. Dropping the guard uncounts
/// it on every exit path, a shutdown mid-delay included.
struct InPlaceWait<'a>(&'a AtomicUsize);

impl<'a> InPlaceWait<'a> {
    fn begin(waiters: &'a AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::AcqRel);
        Self(waiters)
    }
}

impl Drop for InPlaceWait<'_> {
    fn drop(&mut self) {
        // Underflow-checking: every count has exactly one uncount, so a zero
        // here is a broken invariant. It is reported, not wrapped round to
        // `usize::MAX`, which would read as waiting handlers for good and
        // pause the assignment. Reported only: this runs on every in-place
        // exit path, and a runtime path never panics, in a debug build too.
        if self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_err()
        {
            tracing::error!("in-place waiter count underflow: a wait ended that was never counted");
        }
    }
}

/// Retry and defer a record read off an infra-owned topic without producing
/// into it.
///
/// An external topic is read-only for shove, so the republish the Hold arms
/// of `route_outcome` perform is not available there. The wait happens here
/// instead, inside the handler's own task and holding its prefetch permit,
/// and the record is handed back to the handler decoded afresh from the
/// retained bytes: `MessageHandler::handle` takes the message by value and
/// `Topic` does not require `Clone`, so the bytes, not the value, are what
/// the task keeps. A `Retry` counts its attempt in memory instead of in a
/// header, and the redelivered metadata carries that count. Only a terminal
/// decision leaves this loop, so `route_outcome` then runs its Ack or DLQ arm
/// and never a Hold arm. Every wait selects on `shutdown`, as
/// `run_delayed_republish` does: a cancelled wait completes nothing and the
/// record is redelivered on restart. Every wait selects on `revoked` too, the
/// token of the record's partition assignment (see `AssignmentTokens`): a
/// revoke ends the wait as `InPlaceEnd::Revoked`, nothing completed, and the
/// partition's next owner is handed the record from the committed offset.
/// Without it the old owner held its permit for the whole delay and then
/// handled the record again, beside the new owner.
#[allow(clippy::too_many_arguments)]
async fn redeliver_in_place<T, H>(
    handler: &Arc<H>,
    ctx: &Arc<H::Context>,
    decode: &BatchDecodeCtx<'_>,
    payload: &[u8],
    headers: &Arc<HashMap<String, String>>,
    coordinates: RecordCoordinates,
    first_outcome: Outcome,
    retry_count: u32,
    max_retries: u32,
    hold_queues: &[HoldQueue],
    handler_timeout: Option<Duration>,
    timeout_outcome: Option<Outcome>,
    topic: &str,
    group: Option<&str>,
    shutdown: &CancellationToken,
    revoked: &CancellationToken,
    waiters: &AtomicUsize,
) -> InPlaceEnd
where
    T: Topic,
    H: MessageHandler<T>,
{
    let mut outcome = first_outcome;
    let mut attempts = retry_count;
    loop {
        let increment = match decide_retry(&outcome, attempts, max_retries) {
            RetryDecision::Ack | RetryDecision::Dlq { .. } => {
                return InPlaceEnd::Terminal { outcome, attempts };
            }
            RetryDecision::Hold { increment } => increment,
        };
        let delay = hold_delay(hold_queues, attempts, increment);
        // Counted from the delay through the decode and any registry wait,
        // and uncounted before the handler runs: the receive loop pauses its
        // assignment for permits held by *waiting* handlers, not by running
        // ones, and a task waiting on the registry is waiting, not running.
        // The guard drops on every exit path, a shutdown mid-wait included.
        let message = {
            let _waiting = InPlaceWait::begin(waiters);
            tokio::select! {
                // The stop and the revoke are read before the sleep: a
                // sleep that won a tie cost one more pass before the next
                // check read the token.
                biased;
                _ = shutdown.cancelled() => {
                    tracing::debug!(
                        queue = topic,
                        "shutdown fired during an in-place wait; the record stays uncommitted \
                         and is redelivered on restart"
                    );
                    return InPlaceEnd::Cancelled;
                }
                _ = revoked.cancelled() => {
                    tracing::debug!(
                        queue = topic,
                        partition = coordinates.partition,
                        offset = coordinates.offset,
                        "partition revoked during an in-place wait; the record is left to its next owner"
                    );
                    return InPlaceEnd::Revoked;
                }
                _ = tokio::time::sleep(delay) => {}
            }
            if increment {
                attempts = attempts.saturating_add(1);
            }
            // Loops only through the registry wait arm, which the
            // `kafka-schema-registry` feature adds.
            #[allow(clippy::never_loop)]
            loop {
                let decoded = tokio::select! {
                    biased;
                    _ = revoked.cancelled() => return InPlaceEnd::Revoked,
                    decoded = decode_or_shutdown(
                        decode_batch_message::<T>(decode, payload),
                        shutdown,
                    ) => decoded,
                };
                let Some(decoded) = decoded else {
                    return InPlaceEnd::Cancelled;
                };
                match decoded {
                    BatchDecode::Decoded(m) => break m,
                    BatchDecode::Dlq { reason, fail } => {
                        // The decode's own poll can complete in the instant a
                        // revoke lands, after the select around it read the
                        // token. Read it again before the bytes are condemned:
                        // on `Undecodable` the caller publishes a dead letter
                        // from this member, and the partition's next owner
                        // would publish a second one for the same bytes.
                        if revoked.is_cancelled() {
                            tracing::debug!(
                                queue = topic,
                                partition = coordinates.partition,
                                offset = coordinates.offset,
                                "partition revoked during an in-place redelivery's decode; \
                                 the record is left to its next owner"
                            );
                            return InPlaceEnd::Revoked;
                        }
                        return InPlaceEnd::Undecodable {
                            reason,
                            fail,
                            attempts,
                        };
                    }
                    // An outage during the redelivery: keep waiting on the same
                    // bytes, as the receive loop would.
                    #[cfg(feature = "kafka-schema-registry")]
                    BatchDecode::Unavailable { id, error } => {
                        tracing::warn!(
                            queue = topic,
                            schema_id = %id,
                            error = %error,
                            retry_after = ?REGISTRY_RETRY_DELAY,
                            "schema registry unavailable during an in-place redelivery; keeping the record"
                        );
                        metrics::record_failed(
                            topic,
                            group,
                            metrics::FailReason::SchemaUnavailable,
                        );
                        tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => return InPlaceEnd::Cancelled,
                            _ = revoked.cancelled() => return InPlaceEnd::Revoked,
                            _ = tokio::time::sleep(REGISTRY_RETRY_DELAY) => {}
                        }
                    }
                    #[cfg(feature = "kafka-schema-registry")]
                    BatchDecode::Fatal(e) => return InPlaceEnd::Fatal(e),
                }
            }
        };
        // The decode's own poll can complete in the instant a revoke lands,
        // after the select around it read the token: read it once more
        // before the handler runs, or the record would be handled on the
        // old owner beside the new one. The `Dlq` arm above makes the same
        // read before the bytes are dead-lettered.
        if revoked.is_cancelled() {
            return InPlaceEnd::Revoked;
        }
        let mut metadata = build_message_metadata(headers, true, coordinates);
        metadata.retry_count = attempts;
        let handler = Arc::clone(handler);
        let ctx = Arc::clone(ctx);
        outcome = invoke_handler(
            async move { handler.handle(message, metadata, ctx.as_ref()).await },
            handler_timeout,
            timeout_outcome.clone(),
            topic,
            group,
        )
        .await;
    }
}

/// How a delayed republish ended, see [`run_delayed_republish`].
enum Republished {
    /// The copy landed: the record is done with.
    Landed,
    /// Shutdown fired, or the record's partition was revoked, before the
    /// delay ran out. Nothing was published: the record is redelivered on
    /// restart, or handed to the partition's next owner from the committed
    /// offset.
    Abandoned,
    /// The publish failed, with `publish_with_retry`'s error.
    Failed(ShoveError),
}

/// Wait out the hold delay of a `Retry` or `Defer`, then publish the
/// record's copy into its topic.
///
/// The stop and the revoke of the record's partition, `revoked` (see
/// `AssignmentTokens`), are read before the delay, and either one ends the
/// wait with nothing published. A copy the old owner published after a
/// revoke would run beside the record the partition's next owner is handed
/// from the committed offset, two live copies each with a retry budget of
/// its own.
#[allow(clippy::too_many_arguments)]
async fn run_delayed_republish(
    client: &KafkaClient,
    topic: &str,
    key: Option<&[u8]>,
    copy: OwnedHeaders,
    payload: &[u8],
    delay: Duration,
    shutdown: &CancellationToken,
    revoked: &CancellationToken,
    label: &'static str,
) -> Republished {
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => {
            tracing::debug!(
                queue = topic,
                label,
                "shutdown fired before delayed republish; the offset stays uncommitted and the \
                 record is redelivered on restart"
            );
            return Republished::Abandoned;
        }
        _ = revoked.cancelled() => {
            tracing::debug!(
                queue = topic,
                label,
                "partition revoked before delayed republish; nothing is published and the \
                 record is left to its next owner"
            );
            return Republished::Abandoned;
        }
        _ = tokio::time::sleep(delay) => {}
    }
    match client
        .publish_with_retry(topic, key, copy, payload, MAX_PUBLISH_ATTEMPTS, label)
        .await
    {
        Ok(()) => Republished::Landed,
        Err(e) => Republished::Failed(e),
    }
}

// ---------------------------------------------------------------------------
// Handler invocation
// ---------------------------------------------------------------------------

/// How a [`guarded`] call ended.
enum Guarded<T> {
    Done(T),
    Panicked,
    /// The call outlasted this limit and was dropped.
    TimedOut(Duration),
}

/// Awaits `fut` on the task that calls it, with panic isolation and an
/// optional timeout, and records no metric. A panic inside `fut` is caught
/// via `AssertUnwindSafe(...).catch_unwind()`, and a call that outlasts
/// `timeout` is dropped at its next await point.
async fn guarded<F: Future>(fut: F, timeout: Option<Duration>) -> Guarded<F::Output> {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    let safe_fut = AssertUnwindSafe(fut).catch_unwind();
    match timeout {
        Some(limit) => match tokio::time::timeout(limit, safe_fut).await {
            Ok(Ok(output)) => Guarded::Done(output),
            Ok(Err(_panic)) => Guarded::Panicked,
            Err(_elapsed) => Guarded::TimedOut(limit),
        },
        None => match safe_fut.await {
            Ok(output) => Guarded::Done(output),
            Err(_panic) => Guarded::Panicked,
        },
    }
}

/// Invoke the handler future with an optional timeout, emitting inflight /
/// consumed / duration metrics. Returns `Outcome::Retry` on timeout or panic.
///
/// Awaits the handler future under [`guarded`], recording per-outcome
/// metrics. A panic inside the user's handler is surfaced as
/// `Outcome::Retry`, and a timeout as the configured timeout outcome.
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
    let _inflight = metrics::InflightGuard::from_refs(topic, group);
    let start = std::time::Instant::now();
    let outcome = match guarded(fut, timeout).await {
        Guarded::Done(outcome) => outcome,
        Guarded::Panicked => {
            tracing::warn!("handler panicked, retrying message");
            Outcome::Retry
        }
        Guarded::TimedOut(duration) => {
            let resolved = handler_timeout_outcome(timeout_outcome);
            tracing::warn!(outcome = ?resolved, "handler timed out after {duration:?}");
            metrics::record_failed(topic, group, metrics::FailReason::Timeout);
            resolved
        }
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
/// A receive loop never ends on a `recv()` error it rides out, so it maps
/// none of those, see [`recv_error_is_transient`].
///
/// `AutoOffsetReset` is permanent too: librdkafka raises it when a
/// partition has no committed offset (or the committed one is out of range)
/// and the reset policy is `KafkaAutoOffsetReset::None` (`error` on the
/// wire). Reconnecting rejoins the same group under the same policy and meets
/// the same answer, so the consumer ends instead and names the fault; the
/// operator either commits a starting position (see
/// `reset_consumer_group_offsets`) or picks another `KafkaAutoOffsetReset`.
/// Ending the consumer ends one group member, not the group: the group's
/// spawner counts the error, and with autoscaling on its respawn supervision
/// replaces the member, which meets the same answer again (see the `None`
/// variant's doc for the schedule).
fn map_kafka_error(context: &str, e: KafkaError) -> ShoveError {
    let is_permanent = matches!(
        &e,
        KafkaError::ClientConfig(..)
            | KafkaError::ClientCreation(_)
            | KafkaError::MessageConsumptionFatal(_)
            | KafkaError::MessageConsumption(RDKafkaErrorCode::AutoOffsetReset)
            | KafkaError::Canceled
            | KafkaError::Nul(_)
    );
    if is_permanent {
        ShoveError::Topology(format!("{context}: {e}"))
    } else {
        ShoveError::Connection(format!("{context}: {e}"))
    }
}

/// Whether an error a consume loop's `recv()` returned is ridden out. Such
/// an error is logged, see [`log_transient_recv_error`], and the loop goes
/// on polling on the same client. Every other error ends the loop with the
/// error [`map_kafka_error`] makes of it. Every `recv()` arm reads this one
/// table: each consume loop's, and each wait's that polls. `gate` is the
/// gate of the consumer the arm polls, or `None` in the stop's drain, see
/// below.
///
/// The codes ridden out, with the bundled librdkafka 2.12.1 source behind
/// each:
///
/// - `BrokerTransportFailure` (`__TRANSPORT`): a broker connection could not
///   be made, or failed. `rd_kafka_broker_fail` tears it down and
///   reconnects under `reconnect.backoff.ms` (`rdkafka_broker.c:616-625`).
///   One that `gate` finds soon after an assignment change ends the loop
///   instead, see below.
/// - `Resolve` (`__RESOLVE`): a broker's name did not resolve. The same
///   `rd_kafka_broker_fail`, and the next attempt resolves the name again
///   (`rdkafka_broker.c:1127-1139`). `gate` reads it as it reads a
///   `__TRANSPORT`.
/// - `AllBrokersDown` (`__ALL_BROKERS_DOWN`): every broker is down at once.
///   It is raised once per outage, while librdkafka re-bootstraps and every
///   broker keeps reconnecting (`rdkafka_broker.c:369-388`).
///
/// A broker failure reaches the application only at error level, and an
/// identical one at most once every 30 s per broker
/// (`rdkafka_broker.c:555-612`), so a bounce logs a handful of lines and the
/// warning needs no rate limit of its own.
///
/// A committed-offset fetch can report `__TRANSPORT` or `__RESOLVE` as well:
/// a broker that fails its connection, or cannot resolve its name, fails the
/// requests queued for it with that code (`rdkafka_broker.c:698-707`,
/// `:1136-1138`). librdkafka retries the fetch through such failures, with
/// no deadline, for as long as the assignment it was sent for stands
/// (`rdkafka_assignment.c:261-262`, `:588`, `rdkafka_request.c:1595`), so
/// that error reaches `recv()` only once an assign or revoke has changed the
/// assignment under the fetch. Its partitions then stay unfetched, and so
/// does every partition assigned after them, until they are revoked
/// (`rdkafka_assignment.c:283-298`, `:459-461`); the member keeps
/// heartbeating, so the group never moves them. The code alone cannot tell
/// that error from a broker failure, so [`TransportGate`] ends the loop on
/// a `__TRANSPORT` or `__RESOLVE` that may be one: one within
/// `socket.timeout.ms` of an assign or revoke that itself landed within
/// `socket.timeout.ms` of an earlier assign, or of a ridden-out
/// `__TRANSPORT` or `__RESOLVE`, because a fetch may have been pending when
/// that change landed. A revoke of an assignment librdkafka reports lost is
/// never such a change, see [`AssignmentChange::LostRevoke`]. The reconnect
/// fetches the committed offsets again. The stop's drain passes no gate, see
/// [`acquire_permit_while_polling`], and rides out every `__TRANSPORT` and
/// `__RESOLVE`: the gate's one remedy is that reconnect, which a stop never
/// makes, so ending the drain on one would only cost the stop its final
/// commit.
/// The gate is blind to a coordinator that holds a fetch unanswered for
/// longer than `socket.timeout.ms` while the application is handed no
/// error: an assignment change after that bound, and a connection the
/// coordinator drops after it, strand the fetch's partitions with the error
/// ridden out.
///
/// Every other code ends the loop. That covers a fatal error, whether
/// `MessageConsumptionFatal` or a `__FATAL` left untranslated
/// (`rdkafka_event.h:77-91`), and `AutoOffsetReset`, see
/// [`map_kafka_error`]. It covers `PollExceeded`, raised once librdkafka has
/// left the group for a member that did not poll in time
/// (`rdkafka_cgrp.c:5162-5210`). It covers what a committed-offset fetch
/// passes to the application, `__TIMED_OUT` and `NotCoordinator` among
/// them: the fetch's partitions stay unfetched until the assignment
/// changes, and librdkafka does not fetch them again on its own
/// (`rdkafka_assignment.c:198-232`), so the reconnect that ends the loop
/// fetches them. It covers an authorization a coordinator lookup or a fetch
/// was refused, and a subscribed topic the cluster does not know.
/// librdkafka reports each of those once and goes on retrying in silence
/// (`rdkafka_fetcher.c:295-307`, `:344`, `rdkafka_cgrp.c:794-808`,
/// `:5232-5262`), and only an operator clears one, with an ACL or the
/// topic, so the loop ends where the error shows and every reconnect
/// reports it again. And it covers the join and fetch errors librdkafka
/// reports on every retry of a cause that persists, a refused join among
/// them (`rdkafka_cgrp.c:2434-2441`, `rdkafka_fetcher.c:327-338`).
fn recv_error_is_transient<'a>(
    gate: impl Into<Option<&'a TransportGate>>,
    error: &KafkaError,
) -> bool {
    match error {
        KafkaError::MessageConsumption(
            RDKafkaErrorCode::BrokerTransportFailure | RDKafkaErrorCode::Resolve,
        ) => {
            let Some(gate) = gate.into() else {
                return true;
            };
            let rides_out = gate.rides_out(std::time::Instant::now());
            if !rides_out {
                tracing::warn!(
                    error = %error,
                    bound = ?gate.bound,
                    "broker connection or name resolution failure within socket.timeout.ms \
                     of an assignment change, which may have stranded a committed-offset \
                     fetch; ending the loop so the reconnect fetches the committed offsets \
                     again"
                );
            }
            rides_out
        }
        KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown) => true,
        _ => false,
    }
}

/// Logs an error [`recv_error_is_transient`] rides out.
fn log_transient_recv_error(queue: &str, error: &KafkaError) {
    tracing::warn!(
        queue,
        error = %error,
        "broker connection error ridden out; librdkafka reconnects and polling continues \
         on the same client. A failed broker connection or name resolution within \
         socket.timeout.ms of an assignment change ends the loop instead, so the reconnect \
         fetches the committed offsets again; a partition that stays unfetched after an \
         outage needs the member restarted"
    );
}

/// Whether a `__TRANSPORT` or `__RESOLVE` a receive loop met may be the one
/// trace of a committed-offset fetch an assignment change stranded, see
/// [`recv_error_is_transient`].
///
/// The consumer's rebalance callback stamps every assign and revoke here,
/// inside the poll that serves it: the stranded fetch's error can reach
/// `recv()` before the loop drains the event. A change that lands within
/// `bound` of the last assign, whose fetch may still be pending, or of the
/// last `__TRANSPORT` or `__RESOLVE` ridden out, which may have left a fetch
/// retrying through a failing broker, arms the gate. While it is armed, a
/// `__TRANSPORT` or `__RESOLVE` within `bound` of the change that armed it
/// ends the loop. A revoke starts no fetch, so it opens no window for the
/// change behind it. A revoke of an assignment librdkafka reports lost arms
/// nothing, see [`AssignmentChange::LostRevoke`].
struct TransportGate {
    /// The consumer's `socket.timeout.ms`, see [`socket_timeout`].
    bound: Duration,
    stamps: std::sync::Mutex<GateStamps>,
}

/// When each event [`TransportGate`] reads last happened.
#[derive(Default)]
struct GateStamps {
    /// An assign the rebalance callback served.
    assign: Option<std::time::Instant>,
    /// A `__TRANSPORT` or `__RESOLVE` a receive loop rode out.
    ridden: Option<std::time::Instant>,
    /// The assign or revoke that armed the gate.
    armed: Option<std::time::Instant>,
}

/// An assignment change the rebalance callback serves, as
/// [`TransportGate::changed`] reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AssignmentChange {
    Assign,
    Revoke,
    /// A revoke of an assignment librdkafka reports lost: the member's
    /// session timed out, its membership or generation was refused, it
    /// outlasted `max.poll.interval.ms`, or its topic was deleted (bundled
    /// librdkafka 2.12.1, `rdkafka_cgrp.c:5949-5990`, `:3459-3471`,
    /// `:5200-5210`, `:6971-6988`). Each revokes every partition of the
    /// topic (`rdkafka_cgrp.c:5104-5125`), and the unassign takes the ones
    /// whose committed offsets are being fetched off that list too
    /// (`rdkafka_assignment.c:884-887`), so no fetch is left to strand.
    LostRevoke,
}

impl TransportGate {
    fn new(bound: Duration) -> Self {
        Self {
            bound,
            stamps: std::sync::Mutex::new(GateStamps::default()),
        }
    }

    /// Stamps the assign or revoke the rebalance callback serves at `now`,
    /// arming the gate if it lands within the bound of the last assign or of
    /// the last ridden-out `__TRANSPORT` or `__RESOLVE`. A lost revoke
    /// stamps nothing.
    fn changed(&self, change: AssignmentChange, now: std::time::Instant) {
        if change == AssignmentChange::LostRevoke {
            return;
        }
        let mut stamps = self.stamps();
        if self.within(stamps.assign, now) || self.within(stamps.ridden, now) {
            stamps.armed = Some(now);
        }
        if change == AssignmentChange::Assign {
            stamps.assign = Some(now);
        }
    }

    /// Whether a `__TRANSPORT` or `__RESOLVE` a receive loop met at `now` is
    /// ridden out: it is, and is stamped, unless `now` falls within the
    /// bound of the change that armed the gate.
    fn rides_out(&self, now: std::time::Instant) -> bool {
        let mut stamps = self.stamps();
        if self.within(stamps.armed, now) {
            return false;
        }
        stamps.ridden = Some(now);
        true
    }

    fn within(&self, stamp: Option<std::time::Instant>, now: std::time::Instant) -> bool {
        stamp.is_some_and(|at| now.saturating_duration_since(at) <= self.bound)
    }

    fn stamps(&self) -> std::sync::MutexGuard<'_, GateStamps> {
        // Plain stamps, valid whatever a panicking holder left behind.
        self.stamps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The `socket.timeout.ms` a client built from `config` runs with: the
/// value `config` sets, or librdkafka's default, 60 s (bundled librdkafka
/// 2.12.1, `rdkafka_conf.c:558-567`).
fn socket_timeout(config: &ClientConfig) -> Result<Duration> {
    let value = config
        .create_native_config()
        .and_then(|native| native.get("socket.timeout.ms"))
        .map_err(|e| map_kafka_error("failed to read socket.timeout.ms", e))?;
    let ms = value.parse::<u64>().map_err(|e| {
        ShoveError::Topology(format!(
            "socket.timeout.ms is {value:?}, not a count of milliseconds: {e}"
        ))
    })?;
    Ok(Duration::from_millis(ms))
}

// ---------------------------------------------------------------------------
// Rebalance plumbing
// ---------------------------------------------------------------------------

/// Consumer-group event forwarded from librdkafka's rebalance callback to
/// the receive loop that owns the `OffsetTracker`.
enum RebalanceEvent {
    Assign(Vec<i32>),
    Revoke(Vec<i32>),
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
    /// The revoke tokens of the receive loop's tracker, cancelled on
    /// `Revoke`; see `AssignmentTokens`.
    assignments: AssignmentTokens,
    /// Stamped on every assign and revoke, for the receive loops' table to
    /// read; see [`TransportGate`].
    transport_gate: TransportGate,
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

/// `commit_callback` keeps rdkafka's empty default. It never fires for a
/// commit shove makes: rdkafka 0.39 sets no `offset_commit_cb`, and a manual
/// commit carries no callback of its own, so librdkafka posts its result
/// only to the reply queue of a synchronous commit (bundled librdkafka
/// 2.12.1, `rdkafka_offset.c:387-411`, `rdkafka_cgrp.c:3871-3908`). The
/// `EVENT_OFFSET_COMMIT` rust-rdkafka enables reaches only the commits
/// librdkafka makes on its own account (`rdkafka_cgrp.c:4269-4278`). Every
/// loop that commits therefore commits synchronously, on a thread of its own,
/// and reads the answer back, see [`CommitSlot`].
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
    ///
    /// `pre_` is also where a revoke's `assignment_lost()` still holds: the
    /// incremental unassign rust-rdkafka makes after this callback clears
    /// it (bundled librdkafka 2.12.1, `rdkafka_cgrp.c:4406-4407`).
    fn pre_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let (event, change) = match rebalance {
            Rebalance::Assign(tpl) => {
                let partitions = self.partitions_for_topic(tpl);
                tracing::debug!(
                    topic = %self.topic,
                    client_id = %self.client_id,
                    ?partitions,
                    "rebalance: partitions assigned"
                );
                #[cfg(feature = "test-support")]
                per_record_probe::ASSIGNS_QUEUED.fetch_add(1, Ordering::SeqCst);
                (RebalanceEvent::Assign(partitions), AssignmentChange::Assign)
            }
            Rebalance::Revoke(tpl) => {
                let partitions = self.partitions_for_topic(tpl);
                tracing::debug!(
                    topic = %self.topic,
                    client_id = %self.client_id,
                    ?partitions,
                    "rebalance: partitions revoked"
                );
                // Ends every in-place wait and delayed republish on these
                // partitions now, inside the poll that reported the revoke,
                // rather than when the receive loop next drains its events,
                // which can be a housekeeping tick away. See
                // `AssignmentTokens`.
                cancel_assignments(&self.assignments, &partitions);
                let change = if base_consumer.assignment_lost() {
                    AssignmentChange::LostRevoke
                } else {
                    AssignmentChange::Revoke
                };
                (RebalanceEvent::Revoke(partitions), change)
            }
            Rebalance::Error(e) => {
                tracing::warn!(topic = %self.topic, error = %e, "rebalance error");
                return;
            }
        };
        self.transport_gate
            .changed(change, std::time::Instant::now());
        // A closed channel means the receive loop is gone (shutdown or
        // reconnect teardown, or a consumer that keeps no tracker) — nothing
        // to notify.
        let _ = self.tx.send(event);
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

    /// The gate this consumer's rebalance callback stamps, which every
    /// `recv()` arm but the stop's drain hands [`recv_error_is_transient`].
    fn transport_gate(&self) -> &TransportGate {
        match self {
            Self::Default(c) => &c.context().transport_gate,
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => &c.context().transport_gate,
        }
    }

    pub(super) fn commit(&self, tpl: &TopicPartitionList, mode: CommitMode) -> KafkaResult<()> {
        match self {
            Self::Default(c) => c.commit(tpl, mode),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.commit(tpl, mode),
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

    fn position(&self) -> KafkaResult<TopicPartitionList> {
        match self {
            Self::Default(c) => c.position(),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.position(),
        }
    }

    /// Whether librdkafka has given this member's assignment up as lost,
    /// see [`FinalCommit::assignment_lost`].
    fn assignment_lost(&self) -> bool {
        match self {
            Self::Default(c) => c.assignment_lost(),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.assignment_lost(),
        }
    }

    /// Stop fetching from every partition this member currently holds.
    ///
    /// Used by the receive loop of an infra-owned topic while every prefetch
    /// permit is held by a handler waiting out a retry or a defer in place.
    /// Paused, `recv()` yields nothing but keeps polling, so the member stays
    /// inside `max.poll.interval.ms`; a park on the semaphore instead would
    /// stop the polling and have the member evicted. librdkafka drops the
    /// records it had already fetched for a paused partition and refetches
    /// them from the consumed position on resume, so nothing is skipped.
    pub(super) fn pause_assignment(&self) -> KafkaResult<()> {
        let assignment = self.assignment()?;
        match self {
            Self::Default(c) => c.pause(&assignment),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.pause(&assignment),
        }
    }

    /// Undo [`pause_assignment`](Self::pause_assignment).
    pub(super) fn resume_assignment(&self) -> KafkaResult<()> {
        let assignment = self.assignment()?;
        match self {
            Self::Default(c) => c.resume(&assignment),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.resume(&assignment),
        }
    }

    /// Assign every partition of `topic` at `start`.
    ///
    /// The groupless half of a broadcast subscription. `assign()` instead of
    /// `subscribe()` means librdkafka never sends JoinGroup, so no group is
    /// created broker-side and no rebalance is paid at boot. `None` and
    /// `Some(Tail)` assign at `Offset::End`, so deliver-new falls out of the
    /// assignment rather than out of a stored offset - byte for byte the
    /// behaviour before a start could be configured. `Head` assigns at
    /// `Offset::Beginning`, and `Timestamp` resolves each partition through
    /// the same lookup `reset_consumer_group_offsets` uses; see
    /// [`broadcast_start_positions`](Self::broadcast_start_positions).
    ///
    /// **Blocking** — `fetch_metadata` is a synchronous librdkafka call. Callers
    /// on an async runtime must wrap this in `spawn_blocking`.
    ///
    /// Returns the number of partitions assigned.
    pub(super) fn assign_all_partitions_at(
        &self,
        topic: &str,
        start: Option<BroadcastStart>,
        timeout: Duration,
    ) -> Result<usize> {
        let metadata = self
            .fetch_metadata(Some(topic), timeout)
            .map_err(|e| map_kafka_error(&format!("failed to fetch metadata for {topic}"), e))?;
        // `Connection`, not `Topology`, and deliberately: both of these mean
        // "the topic is not there *yet*", which for a subscriber is a wait, not
        // a misconfiguration. A `subscribe()`-based consumer gets that for free
        // — librdkafka keeps refreshing metadata until the topic appears — and
        // an assign-based one has to ask for it, because `Topology` is not
        // retryable and would kill a subscriber that merely started before the
        // publisher declared the topic. The reconnect loop bounds the wait via
        // `max_reconnect_attempts`, so a genuinely absent topic still gives up.
        let partitions: Vec<i32> = metadata
            .topics()
            .first()
            .map(|t| {
                let mut ids: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
                ids.sort_unstable();
                ids
            })
            .unwrap_or_default();
        if partitions.is_empty() {
            return Err(ShoveError::Connection(format!(
                "topic '{topic}' has no partitions yet — a broadcast subscription cannot \
                 assign until it exists; waiting for it to be declared"
            )));
        }

        let tpl = self.broadcast_start_positions(topic, &partitions, start, timeout)?;
        self.assign(&tpl)
            .map_err(|e| map_kafka_error(&format!("failed to assign partitions of {topic}"), e))?;
        Ok(partitions.len())
    }

    /// The offset each of `partitions` is assigned at for `start`.
    ///
    /// `None` and `Tail` are `Offset::End`, `Head` is `Offset::Beginning`:
    /// lazy sentinels librdkafka resolves when the fetcher starts, which is
    /// what a subscription with no stored position wants. `Timestamp` needs
    /// the broker: the watermarks are fetched **first**, then
    /// `offsets_for_times`, and each partition resolves through
    /// [`target_from_timestamp_lookup`] - the order `run_reset` uses, because
    /// a record landing between the lookup and a *later* watermark fetch would
    /// otherwise be skipped by the tail fallback. A partition the lookup did
    /// not resolve fails the whole assignment rather than silently starting at
    /// the tail.
    ///
    /// One exhaustive match over the start, so adding a variant is a compile
    /// error here rather than a runtime `unreachable!`.
    ///
    /// **Blocking** on the `Timestamp` path; callers must use `spawn_blocking`.
    fn broadcast_start_positions(
        &self,
        topic: &str,
        partitions: &[i32],
        start: Option<BroadcastStart>,
        timeout: Duration,
    ) -> Result<TopicPartitionList> {
        let ts_ms = match start {
            None | Some(BroadcastStart::Tail) => {
                return lazy_positions(topic, partitions, Offset::End);
            }
            Some(BroadcastStart::Head) => {
                return lazy_positions(topic, partitions, Offset::Beginning);
            }
            Some(BroadcastStart::Timestamp(ts_ms)) => ts_ms,
        };

        let mut tpl = TopicPartitionList::new();
        let mut watermarks = HashMap::with_capacity(partitions.len());
        for pid in partitions {
            let (low, high) = self.fetch_watermarks(topic, *pid, timeout).map_err(|e| {
                map_kafka_error(&format!("failed to fetch watermarks for {topic}[{pid}]"), e)
            })?;
            watermarks.insert(*pid, (low, high));
        }
        let mut query = TopicPartitionList::new();
        for pid in partitions {
            query
                .add_partition_offset(topic, *pid, Offset::Offset(ts_ms))
                .map_err(|e| {
                    map_kafka_error(
                        &format!("failed to build the timestamp query for {topic}[{pid}]"),
                        e,
                    )
                })?;
        }
        let resolved = self.offsets_for_times(query, timeout).map_err(|e| {
            map_kafka_error(&format!("offset lookup by timestamp failed for {topic}"), e)
        })?;
        for elem in resolved.elements_for_topic(topic) {
            let pid = elem.partition();
            elem.error().map_err(|e| {
                ShoveError::Connection(format!(
                    "offset lookup by timestamp failed for {topic}[{pid}]: {e}"
                ))
            })?;
            let (low, high) = watermarks.get(&pid).copied().ok_or_else(|| {
                ShoveError::Connection(format!(
                    "offset lookup by timestamp answered for {topic}[{pid}], which the \
                     metadata did not list"
                ))
            })?;
            let target = target_from_timestamp_lookup(elem.offset(), low, high, topic, pid)?;
            tpl.add_partition_offset(topic, pid, Offset::Offset(target))
                .map_err(|e| {
                    map_kafka_error(&format!("failed to target {target} of {topic}[{pid}]"), e)
                })?;
        }
        Ok(tpl)
    }

    /// Extend a manual broadcast assignment with partitions added since the
    /// previous metadata snapshot.
    ///
    /// Existing partitions are re-assigned at their current consumer position;
    /// only newly discovered partitions are resolved, at the same `start` the
    /// subscription began with, so a `Head` subscription does not skip a new
    /// partition's first records and a `Tail` one still joins it at the tail.
    /// This avoids both replaying already-delivered records and moving an
    /// existing partition.
    ///
    /// **Blocking** for the metadata fetch; callers must use `spawn_blocking`.
    fn refresh_broadcast_partitions(
        &self,
        topic: &str,
        start: Option<BroadcastStart>,
        timeout: Duration,
    ) -> Result<usize> {
        let metadata = self
            .fetch_metadata(Some(topic), timeout)
            .map_err(|e| map_kafka_error(&format!("failed to refresh metadata for {topic}"), e))?;
        let mut positions = self.position().map_err(|e| {
            map_kafka_error(
                &format!("failed to read broadcast positions for {topic}"),
                e,
            )
        })?;
        let new_partitions: Vec<i32> = metadata
            .topics()
            .iter()
            .find(|candidate| candidate.name() == topic)
            .map(|metadata_topic| {
                metadata_topic
                    .partitions()
                    .iter()
                    .map(|partition| partition.id())
                    .filter(|partition| positions.find_partition(topic, *partition).is_none())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if new_partitions.is_empty() {
            return Ok(0);
        }

        let additions = self.broadcast_start_positions(topic, &new_partitions, start, timeout)?;
        for elem in additions.elements_for_topic(topic) {
            positions
                .add_partition_offset(topic, elem.partition(), elem.offset())
                .map_err(|e| {
                    map_kafka_error(
                        &format!(
                            "failed to extend the assignment with {topic}[{}]",
                            elem.partition()
                        ),
                        e,
                    )
                })?;
        }
        self.assign(&positions).map_err(|e| {
            map_kafka_error(
                &format!("failed to extend broadcast assignment for {topic}"),
                e,
            )
        })?;
        Ok(new_partitions.len())
    }

    fn fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> KafkaResult<(i64, i64)> {
        match self {
            Self::Default(c) => c.fetch_watermarks(topic, partition, timeout),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.fetch_watermarks(topic, partition, timeout),
        }
    }

    fn offsets_for_times(
        &self,
        timestamps: TopicPartitionList,
        timeout: Duration,
    ) -> KafkaResult<TopicPartitionList> {
        match self {
            Self::Default(c) => c.offsets_for_times(timestamps, timeout),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.offsets_for_times(timestamps, timeout),
        }
    }

    fn fetch_metadata(&self, topic: Option<&str>, timeout: Duration) -> KafkaResult<Metadata> {
        match self {
            Self::Default(c) => c.fetch_metadata(topic, timeout),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.fetch_metadata(topic, timeout),
        }
    }

    fn assign(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        match self {
            Self::Default(c) => c.assign(tpl),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.assign(tpl),
        }
    }
}

// Consumer helper
// ---------------------------------------------------------------------------

/// The `max.poll.interval.ms` a group member is created with: the pinned
/// `MAX_POLL_INTERVAL_MS`, or under `test-support` the value a test injected,
/// `kafka_max_poll_interval` on `ConsumerOptionsInner` or on
/// `BatchConsumerOptionsInner`.
fn max_poll_interval_ms(#[cfg(feature = "test-support")] injected: Option<Duration>) -> u32 {
    #[cfg(feature = "test-support")]
    if let Some(interval) = injected {
        return u32::try_from(interval.as_millis()).unwrap_or(u32::MAX);
    }
    MAX_POLL_INTERVAL_MS
}

/// How often the batch loop wakes with nothing else to wake it: the pinned
/// `HOUSEKEEPING_INTERVAL`, or under `test-support` the value a test
/// injected through `BatchConsumerOptionsInner::kafka_housekeeping_interval`.
fn batch_housekeeping_interval(options: &BatchConsumerOptionsInner) -> Duration {
    #[cfg(feature = "test-support")]
    if let Some(interval) = options.kafka_housekeeping_interval {
        return interval;
    }
    #[cfg(not(feature = "test-support"))]
    let _ = options;
    HOUSEKEEPING_INTERVAL
}

/// The bound on one schema registry lookup made from a consume loop: a third
/// of the member's `max.poll.interval.ms`, and at least a second.
///
/// The lookup runs in the receive arm, where nothing polls until it answers,
/// so an unbounded one against a registry that accepts and never answers
/// would let the group evict the member; `RegistryStall::wait` polls while
/// it waits, so a lookup that becomes `Unavailable` at the bound keeps the
/// member alive and retries every `REGISTRY_RETRY_DELAY`. A third leaves
/// room for the wait and the next attempt inside one interval.
#[cfg(feature = "kafka-schema-registry")]
fn registry_lookup_bound(max_poll_interval_ms: u32) -> Duration {
    Duration::from_millis(u64::from(max_poll_interval_ms) / 3).max(Duration::from_secs(1))
}

/// The client config every shove consumer is created with: the connection
/// settings of `base`, the group and client identity, the pinned protocol and
/// fetch settings, and the fetch queue settings of `fetch_queue`.
fn consumer_config(
    mut base: ClientConfig,
    group_id: &str,
    client_id: &str,
    auto_offset_reset: KafkaAutoOffsetReset,
    max_poll_interval_ms: u32,
    fetch_queue: &FetchQueueTuning,
    topic: &str,
) -> Result<ClientConfig> {
    base.set("group.id", group_id)
        .set("client.id", client_id)
        // Cooperative-sticky assignment performs incremental rebalance so that
        // adding/removing a consumer only reassigns the delta — without this,
        // every join triggers an eager (stop-the-world) rebalance that
        // freezes the entire group for the heartbeat window.
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", auto_offset_reset.as_rdkafka_str())
        .set("session.timeout.ms", SESSION_TIMEOUT_MS.to_string())
        .set("max.poll.interval.ms", max_poll_interval_ms.to_string())
        // Minimise fetch-latency so small-payload workloads aren't bottlenecked
        // by the default 500 ms broker dwell. `FETCH_MIN_BYTES=1` returns as
        // soon as any data is available; `FETCH_WAIT_MAX_MS=50` caps the
        // blocking dwell so the broker doesn't hold the connection open.
        .set("fetch.min.bytes", FETCH_MIN_BYTES.to_string())
        .set("fetch.wait.max.ms", FETCH_WAIT_MAX_MS.to_string());
    fetch_queue.apply(&mut base, topic)?;
    Ok(base)
}

#[allow(clippy::too_many_arguments)]
fn create_stream_consumer(
    base: ClientConfig,
    group_id: &str,
    auto_offset_reset: KafkaAutoOffsetReset,
    max_poll_interval_ms: u32,
    fetch_queue: &FetchQueueTuning,
    topic: &str,
    rebalance_tx: std_mpsc::Sender<RebalanceEvent>,
    assignments: AssignmentTokens,
    #[cfg(feature = "kafka-msk-iam")] msk_context: Option<MskIamContext>,
) -> Result<KafkaStreamConsumer> {
    // Each consumer task within a group gets a distinct `client.id` so
    // librdkafka treats them as separate members. Without this, group
    // rebalances across repeated join attempts can produce stale
    // "group generation id is not valid" commit errors.
    let client_id = format!("shove-{}", uuid::Uuid::new_v4().simple());
    #[cfg(feature = "test-support")]
    client_id_probe::record(&client_id);
    let base = consumer_config(
        base,
        group_id,
        &client_id,
        auto_offset_reset,
        max_poll_interval_ms,
        fetch_queue,
        topic,
    )?;
    #[cfg(feature = "test-support")]
    fetch_queue_probe::record(&base);
    let transport_gate = TransportGate::new(socket_timeout(&base)?);
    #[cfg(feature = "kafka-msk-iam")]
    if let Some(ctx) = msk_context {
        let ctx = RebalanceContext {
            inner: ctx,
            topic: topic.to_string(),
            client_id,
            tx: rebalance_tx,
            assignments,
            transport_gate,
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
        assignments,
        transport_gate,
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
pub(super) const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(5);

/// How long a partition may sit with offset commits continuously rejected,
/// no resolving rebalance ever arriving, before the receive loop treats
/// itself as fenced from the group and forces a clean reconnect (see
/// `OffsetTracker::fenced`). Well above `SESSION_TIMEOUT_MS` so an ordinary
/// rebalance — which by protocol resolves inside that window — never trips
/// it; well below the "silent multi-hour wedge" this guards against.
///
/// This is the floor. The receive loops use [`fence_threshold_over`], which
/// grows with a configured commit interval, because a rejected position is
/// re-offered only once per interval, and the streak ends on the first
/// re-offer the coordinator accepts.
const COMMIT_FENCE_TIMEOUT: Duration = Duration::from_secs(60);

/// Test-only probe (see the `test-support` feature): the fence threshold the
/// last receive loop started with, concurrent or sequenced, so a test can
/// prove that a raised commit interval raised the threshold the loop judges
/// commits by, and not only the helper the loop calls, and how many times a
/// loop has found itself fenced. nextest runs each test in its own process,
/// so the values belong to that test's consumers alone.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod fence_probe {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::Duration;

    static LAST_MS: AtomicU64 = AtomicU64::new(0);
    static FIRED: AtomicUsize = AtomicUsize::new(0);

    pub(super) fn record(threshold: Duration) {
        LAST_MS.store(
            u64::try_from(threshold.as_millis()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
    }

    pub(super) fn record_fired() {
        FIRED.fetch_add(1, Ordering::SeqCst);
    }

    /// The threshold the last receive loop started with, `None` before any.
    pub fn last_threshold() -> Option<Duration> {
        match LAST_MS.load(Ordering::SeqCst) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }

    /// How many times a receive loop ended because it found itself fenced.
    pub fn fired() -> usize {
        FIRED.load(Ordering::SeqCst)
    }
}

/// The fence threshold for a given commit interval, over `floor`: the
/// pinned `COMMIT_FENCE_TIMEOUT`, or under `test-support` the floor a test
/// injected through `ConsumerOptionsInner::kafka_fence_floor`.
///
/// A rejected commit is re-offered one interval after its rejection, and
/// the first re-offer the coordinator accepts ends the streak at once, see
/// `PartitionTracker::resolve`. So the threshold bounds how many re-offers
/// a recovery may take. Four intervals leave room for three whatever the
/// interval, where the 60 s floor alone would leave fewer than three once
/// the interval reaches 20 s. The default 500 ms interval keeps the floor
/// exactly as before.
fn fence_threshold_over(floor: Duration, commit_interval: Duration) -> Duration {
    floor.max(commit_interval.saturating_mul(4))
}

/// The floor the receive loop judges rejected commits by: the pinned
/// `COMMIT_FENCE_TIMEOUT`, or under `test-support` the value a test injected
/// through `ConsumerOptionsInner::kafka_fence_floor`.
fn fence_floor(options: &ConsumerOptions) -> Duration {
    #[cfg(feature = "test-support")]
    if let Some(floor) = options.kafka_fence_floor {
        return floor;
    }
    #[cfg(not(feature = "test-support"))]
    let _ = options;
    COMMIT_FENCE_TIMEOUT
}

/// The retry strategy a consumer runs with, from its options and the
/// topology's ownership.
///
/// An external topology implies `InPlace` and refuses `Republish`: the
/// republish would write a retried record into a topic infra owns, the one
/// write external ownership rules out. A shove-owned topology defaults to
/// `Republish`, the historical behaviour, and may opt into `InPlace`.
pub(super) fn resolve_retry_strategy(
    topology: &QueueTopology,
    requested: Option<RetryStrategy>,
    queue: &str,
    entry_point: &str,
) -> Result<RetryStrategy> {
    match (topology.external(), requested) {
        (true, Some(RetryStrategy::Republish)) => Err(ShoveError::Topology(format!(
            "topic '{queue}' is an external topology; `RetryStrategy::Republish` would \
             republish a retried record into a topic infra owns, so `{entry_point}` refuses \
             it. Drop `with_retry_strategy(..)`: an external topology implies \
             `RetryStrategy::InPlace`."
        ))),
        (true, _) => Ok(RetryStrategy::InPlace),
        (false, requested) => Ok(requested.unwrap_or(RetryStrategy::Republish)),
    }
}

/// The error the broadcast and DLQ entry points return for options that set
/// a retry strategy: neither loop retries through a strategy, so the setting
/// would be read by nothing.
fn reject_retry_strategy_unread(queue: &str, entry_point: &str) -> ShoveError {
    ShoveError::Topology(format!(
        "topic '{queue}': `with_retry_strategy` applies to the standard consumer only; \
         `{entry_point}` never reads it. Drop the call."
    ))
}

/// Every partition of `topic` at one lazy sentinel offset, the assignment a
/// `Tail` or `Head` broadcast start resolves to without asking the broker.
fn lazy_positions(topic: &str, partitions: &[i32], offset: Offset) -> Result<TopicPartitionList> {
    let mut tpl = TopicPartitionList::new();
    for pid in partitions {
        tpl.add_partition_offset(topic, *pid, offset).map_err(|e| {
            map_kafka_error(&format!("failed to target {offset:?} of {topic}[{pid}]"), e)
        })?;
    }
    Ok(tpl)
}

/// The FIFO entry points reach the spawn without the registry's own check,
/// so the refusals live here: every FIFO entry point then fails the same way
/// on a setting the FIFO loop would never read. A retry strategy is not
/// refused: the FIFO loop retries in place whichever one is set. A fetch queue
/// setting outside librdkafka's range is refused here too.
fn check_fifo_options(queue: &str, options: &ConsumerOptions) -> Result<()> {
    options.refuse_broadcast_start(queue, "KafkaConsumer::run_fifo")?;
    if options.kafka_commit_policy.is_some() {
        return Err(reject_fifo_commit_policy(queue));
    }
    options.kafka_fetch_queue.validate(queue)
}

/// The error every FIFO entry point returns for options that set a commit
/// policy, through `with_commit_policy` or its shorthand
/// `with_commit_interval`: a FIFO consumer commits on the fixed default
/// interval (see `FifoState::commit_due`), so neither an interval nor a
/// per-record policy would be read by anything, and a setting that changes
/// nothing is refused rather than silently dropped. `spawn_fifo_shards`
/// applies it, so the direct, supervisor and registry paths all refuse
/// alike; the registry also checks it before it spawns.
pub(super) fn reject_fifo_commit_policy(queue: &str) -> ShoveError {
    ShoveError::Topology(format!(
        "topic '{queue}' is sequenced; `with_commit_policy` and `with_commit_interval` do not \
         apply to a FIFO consumer, which commits on the fixed default interval of 500 ms. Drop \
         the call or use `register` for unsequenced topics."
    ))
}

/// The one constraint `CommitPolicy::PerRecord` carries: a single prefetch
/// permit. The commit a completion triggers carries the partition's safe
/// position, the lowest offset still in flight, never the completed
/// record's own offset. With more than one permit a completion above an
/// unfinished lower offset moves that position nowhere, so the commit
/// confirms nothing and a commit per record is not exact. Refused with the
/// entry point named, at the fail-fast point of the other option refusals:
/// `KafkaConsumer::run` for the direct, supervisor and group members, and
/// `KafkaConsumerGroupRegistry::register` before a member is spawned.
/// `prefetch_count` is the effective count, after the clamp concurrent
/// processing off applies.
/// The commit interval a consumer runs with, checked where the loop reads
/// it. Both `with_commit_interval` setters refuse these values already, with
/// a panic at configuration time, but `ConsumerOptions::kafka_commit_interval`
/// is a public field, and a value written past the setter reaches the loop.
/// A run path does not panic, so the loop refuses it with a `Topology`
/// error instead. Zero would make every commit due at once, and an interval
/// past `MAX_COMMIT_INTERVAL` is refused for the reasons on that constant.
pub(super) fn check_commit_policy(policy: CommitPolicy, queue: &str) -> Result<()> {
    let interval = match policy {
        CommitPolicy::Interval(interval) => interval,
        _ => return Ok(()),
    };
    let max = super::constants::MAX_COMMIT_INTERVAL;
    if interval.is_zero() {
        return Err(ShoveError::Topology(format!(
            "topic '{queue}': `kafka_commit_interval` must be positive, and this consumer has \
             {interval:?} written to the field. A zero interval would make every commit due at \
             once. `KafkaConsumer::run` refuses it. Set `with_commit_interval` to a positive \
             interval of at most {max:?}, or leave the field unset for the 500 ms default."
        )));
    }
    if interval > max {
        return Err(ShoveError::Topology(format!(
            "topic '{queue}': `kafka_commit_interval` must be at most {max:?}, and this consumer \
             has {interval:?} written to the field. The commit gate could not turn it into a \
             deadline. `KafkaConsumer::run` refuses it. Set `with_commit_interval` to a positive \
             interval of at most {max:?}, or leave the field unset for the 500 ms default."
        )));
    }
    Ok(())
}

pub(super) fn check_per_record_prefetch(
    prefetch_count: u16,
    queue: &str,
    entry_point: &str,
) -> Result<()> {
    if prefetch_count <= 1 {
        return Ok(());
    }
    Err(ShoveError::Topology(format!(
        "topic '{queue}': `CommitPolicy::PerRecord` needs one prefetch permit, and this \
         consumer has prefetch_count {prefetch_count} with concurrent processing on; with more \
         than one permit a completion above an unfinished lower offset confirms nothing, so a \
         commit per record would not be exact. `{entry_point}` refuses it. Set \
         `with_prefetch_count(1)`, turn concurrent processing off, or use \
         `CommitPolicy::Interval`."
    )))
}

/// Run the receive loop's final `CommitMode::Sync` commit, and the consumer's
/// close, on a dedicated thread that owns the consumer, waiting at most
/// `deadline` for both.
///
/// `consumer` must be the loop's last handle on the consumer: the thread
/// drops it after the commit, which is where `rd_kafka_consumer_close` runs,
/// and reports the commit's result and the close separately, each waited
/// for inside `deadline`, so a close that outlives the deadline is logged
/// and never read as a commit that missed it. The one other holder there can
/// be is a commit thread whose answer the loop did not wait for: one a stop
/// did not wait out (see [`stop_with_final_commit`]), or one an `Err` exit
/// left in flight (see [`close_without_commit`]). Then this thread's drop
/// closes nothing, the thread says so, the loop logs that the close follows
/// that commit, and the close runs when that thread drops its clone, off the
/// runtime either way. `None` for `tpl` means there is nothing to commit and
/// the thread only closes. The thread is
/// spawned before it is handed the consumer, and a spawn failure disposes of
/// the consumer off the runtime too; see [`final_commit_on_thread`] and
/// [`close_off_runtime_or_leak`].
///
/// The `Err` the result ends with is the [`CommitFailure`] the shutdown arm
/// reports in `ShoveError::Commit`: the broker's answer, the deadline, or no
/// thread. A refusal while the group rebalances goes to `hand_back`, see
/// [`HandBack`].
async fn final_commit_off_runtime<B: Send + 'static>(
    consumer: LoopConsumer,
    tpl: Option<TopicPartitionList>,
    queue: &str,
    deadline: Duration,
    hand_back: HandBack<LoopConsumer, B>,
) -> FinalCommitted<B> {
    final_commit_on_thread(
        consumer,
        tpl,
        queue,
        deadline,
        hand_back,
        &mut spawn_named_thread,
    )
    .await
}

/// What a final commit's thread does with the consumer when the coordinator
/// refused the commit while the group rebalances, see
/// [`is_rebalance_refusal`]: hand it back unclosed as `B`, or give it back
/// with the refusal to be closed as after any other answer. A commit that
/// is never handed back has [`Infallible`](std::convert::Infallible) for
/// `B`, see [`keep_to_close`].
type HandBack<C, B> = fn(C, KafkaError) -> std::result::Result<B, (C, KafkaError)>;

/// The unclosed consumer a refused final commit hands back, with the
/// refusal, for the stop to poll and commit again.
struct Refused<C> {
    consumer: C,
    error: KafkaError,
}

/// A final commit the stop may try again: the consumer comes back.
fn hand_back<C>(
    consumer: C,
    error: KafkaError,
) -> std::result::Result<Refused<C>, (C, KafkaError)> {
    Ok(Refused { consumer, error })
}

/// A final commit the stop does not try again, and a close with nothing to
/// commit: the consumer is closed whatever the answer.
fn keep_to_close<C>(
    consumer: C,
    error: KafkaError,
) -> std::result::Result<std::convert::Infallible, (C, KafkaError)> {
    Err((consumer, error))
}

/// How a final commit on its thread ended, see [`final_commit_on_thread`].
enum FinalCommitted<B> {
    /// The commit's result, the consumer closed after it on the thread, or
    /// closing there once its commit returns.
    Ended(std::result::Result<(), CommitFailure>),
    /// The coordinator refused the commit while the group rebalances, and
    /// the thread handed the consumer back, see [`HandBack`].
    Rebalancing(B),
}

impl FinalCommitted<std::convert::Infallible> {
    /// The result of a final commit that is never handed back.
    fn ended(self) -> std::result::Result<(), CommitFailure> {
        match self {
            Self::Ended(result) => result,
            Self::Rebalancing(never) => match never {},
        }
    }
}

/// What the final-commit thread reports once its commit is answered: the
/// result, after which it closes the consumer, or what it handed back, see
/// [`FinalCommitted::Rebalancing`].
enum Committed<B> {
    Result(KafkaResult<()>),
    HandedBack(B),
}

/// Whether the coordinator refused a commit because the group is
/// rebalancing: with a rebalance in progress, or for a generation the
/// rebalance has made stale. A commit made while another member's leave
/// rebalances the group meets one of the two.
fn is_rebalance_refusal(error: &KafkaError) -> bool {
    matches!(
        error.rdkafka_error_code(),
        Some(RDKafkaErrorCode::RebalanceInProgress | RDKafkaErrorCode::IllegalGeneration)
    )
}

/// Close the consumer on a thread of its own with nothing committed, and
/// wait at most `deadline` for the close: the end of a consume loop that
/// makes no final commit, an `Err` exit or a stop whose commit in flight
/// had no answer in time. It is [`final_commit_off_runtime`] with nothing
/// to commit.
///
/// The close sends the member's LeaveGroup and waits for its answer, so a
/// loop that waits for it has left its group when it returns, and a process
/// that exits on the loop's error does not cut the leave short and leave
/// the group to wait out `session.timeout.ms` for the member. A close that
/// outlives `deadline` is logged and finishes on its thread. With a commit
/// still in flight on another thread, that thread holds a handle too, and
/// the close runs once that commit returns.
async fn close_without_commit(consumer: LoopConsumer, queue: &str, deadline: Duration) {
    if let Err(e) = final_commit_off_runtime(consumer, None, queue, deadline, keep_to_close)
        .await
        .ended()
    {
        tracing::warn!(
            queue,
            error = %e,
            "no thread could be spawned to wait for the consumer's close; the close is not awaited"
        );
    }
}

/// Spawn a detached OS thread named `name` running `body`, the spawner the
/// final commit, every other commit a loop makes (see [`spawn_commit`]) and
/// a consumer's close hand their work to. Under `test-support` it refuses
/// while `final_commit_spawn_probe` says so, the way an exhausted host does.
fn spawn_named_thread(name: String, body: Box<dyn FnOnce() + Send>) -> std::io::Result<()> {
    #[cfg(feature = "test-support")]
    if final_commit_spawn_probe::refused() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "thread spawn refused by final_commit_spawn_probe",
        ));
    }
    std::thread::Builder::new()
        .name(name)
        .spawn(body)
        .map(|_detached| ())
}

/// Test-only counters (see the `test-support` feature) for what a
/// concurrent loop does that nothing else observes: a commit attempted
/// while no thread can carry it leaves no request at the broker and no
/// record at the handler, so a test that holds the next record behind it
/// needs this to know the attempt was made, and the drain that met an
/// assign is invisible but for the pause it re-applies. nextest runs each
/// test in its own process, so the counters belong to that test's
/// consumers alone.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod per_record_probe {
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Entries into `commit_confirmed`: every commit a loop attempted and
    /// waited for, a per-record commit or a batch span's, carried or not.
    pub static COMMIT_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    /// Assign events the rebalance callback queued for the loop, from any
    /// consumer in the process.
    pub static ASSIGNS_QUEUED: AtomicUsize = AtomicUsize::new(0);
    /// Assigns a concurrent loop reconciled from the drain at the top of a
    /// pass, under either commit policy.
    pub static RECONCILED_AT_TOP_OF_PASS: AtomicUsize = AtomicUsize::new(0);
    /// Assigns a concurrent loop's receive arm drained, and reconciled,
    /// ahead of the record that brought them, under either commit policy.
    pub static ASSIGNS_DRAINED_AT_RECEIVE_ARM: AtomicUsize = AtomicUsize::new(0);

    pub fn commit_attempts() -> usize {
        COMMIT_ATTEMPTS.load(Ordering::SeqCst)
    }

    pub fn assigns_queued() -> usize {
        ASSIGNS_QUEUED.load(Ordering::SeqCst)
    }

    pub fn reconciled_at_top_of_pass() -> usize {
        RECONCILED_AT_TOP_OF_PASS.load(Ordering::SeqCst)
    }

    pub fn assigns_drained_at_receive_arm() -> usize {
        ASSIGNS_DRAINED_AT_RECEIVE_ARM.load(Ordering::SeqCst)
    }
}

/// Test-only record (see the `test-support` feature) of the `client.id`
/// every consumer this process created was given, in creation order. A
/// reconnect creates a consumer, and with it a fresh id, so a test that must
/// prove a member rode out a fault without reconnecting reads this. nextest
/// runs each test in its own process, so the record belongs to that test's
/// consumers alone.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod client_id_probe {
    use std::sync::Mutex;

    static CREATED: Mutex<Vec<String>> = Mutex::new(Vec::new());

    /// The ids, in creation order.
    pub fn created() -> Vec<String> {
        CREATED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn record(client_id: &str) {
        CREATED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(client_id.to_owned());
    }
}

/// Test-only record (see the `test-support` feature) of the fetch queue
/// settings every consumer this process created was handed, in creation
/// order, as the strings librdkafka was given. A setting nobody set reads
/// `None`. nextest runs each test in its own process, so the record belongs to
/// that test's consumers alone.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod fetch_queue_probe {
    use std::sync::Mutex;

    use rdkafka::ClientConfig;

    /// The fetch queue settings of one consumer.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Created {
        pub fetch_queue_backoff_ms: Option<String>,
        pub queued_min_messages: Option<String>,
        pub queued_max_messages_kbytes: Option<String>,
    }

    static CREATED: Mutex<Vec<Created>> = Mutex::new(Vec::new());

    /// The settings of each consumer, in creation order.
    pub fn created() -> Vec<Created> {
        CREATED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn record(config: &ClientConfig) {
        let read = |key: &str| config.get(key).map(str::to_owned);
        CREATED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Created {
                fetch_queue_backoff_ms: read("fetch.queue.backoff.ms"),
                queued_min_messages: read("queued.min.messages"),
                queued_max_messages_kbytes: read("queued.max.messages.kbytes"),
            });
    }
}

/// Test-only switch (see the `test-support` feature): refuses every thread
/// the shutdown path asks for, and every commit thread (see
/// [`spawn_commit`]), the way an exhausted host does, so an integration
/// test can drive the last-resort leak against a real broker and watch what
/// the leaked member does, or prove that a commit no thread could carry is
/// never taken as confirmed. nextest runs each test in its own process, so
/// the switch belongs to that test's consumers alone.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod final_commit_spawn_probe {
    use std::sync::atomic::{AtomicBool, Ordering};

    static REFUSE: AtomicBool = AtomicBool::new(false);

    /// Refuse, or with `false` allow again, the spawn of the final-commit
    /// thread and of the close-only thread that follows a failed one.
    pub fn refuse_threads(refuse: bool) {
        REFUSE.store(refuse, Ordering::SeqCst);
    }

    pub(super) fn refused() -> bool {
        REFUSE.load(Ordering::SeqCst)
    }
}

/// What the final-commit thread does with what it owns: the synchronous
/// commit, then the drop, which for a consumer is where
/// `rd_kafka_consumer_close` runs. A trait over the concrete handle so the
/// thread choreography is testable with a drop probe instead of a broker.
trait FinalCommit: Send + 'static {
    fn commit_sync(&self, tpl: &TopicPartitionList) -> KafkaResult<()>;

    /// Whether dropping this handle runs the consumer's close: true unless
    /// another holder remains, see [`final_commit_off_runtime`].
    fn is_last_handle(&self) -> bool {
        true
    }

    /// Whether librdkafka has given the member's assignment up as lost. A
    /// commit refused for a stale generation outside a rebalance does that
    /// before its answer comes back (bundled librdkafka 2.12.1,
    /// `rdkafka_cgrp.c:4007-4018`): nothing is left to commit under the
    /// assignment, so the refusal is not tried again.
    fn assignment_lost(&self) -> bool {
        false
    }
}

impl FinalCommit for LoopConsumer {
    fn commit_sync(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        self.commit(tpl, CommitMode::Sync)
    }

    fn is_last_handle(&self) -> bool {
        Arc::strong_count(self) == 1
    }

    fn assignment_lost(&self) -> bool {
        self.consumer.assignment_lost()
    }
}

/// A consume loop's own handle on its consumer: the standard consumer's, a
/// sequenced consumer's, the batch consumer's and the DLQ drain's, each the
/// last holder in the ordinary case.
///
/// Dropping the last handle runs `rd_kafka_consumer_close`, which blocks
/// until librdkafka has left the group: rust-rdkafka 0.39 polls until the
/// consumer reports closed (`base_consumer.rs:766-780`), and a coordinator
/// that does not answer the LeaveGroup holds it there. Every exit a loop
/// takes itself closes on a thread of its own and waits for the close: a
/// stop through the final commit, an `Err` exit through
/// [`close_without_commit`]. An exit the loop does not take itself, its
/// task aborted where it waits or a panic unwinding through it, drops this
/// handle on a runtime thread instead, and the handle then hands the
/// consumer to a thread of its own, see [`close_off_runtime_or_leak`];
/// nothing waits for that close. Dropped on any other thread, the final
/// commit's included, it drops the consumer there.
struct LoopConsumer {
    consumer: ManuallyDrop<Arc<KafkaStreamConsumer>>,
    queue: String,
}

impl LoopConsumer {
    fn new(consumer: KafkaStreamConsumer, queue: &str) -> Self {
        Self {
            consumer: ManuallyDrop::new(Arc::new(consumer)),
            queue: queue.to_string(),
        }
    }
}

impl std::ops::Deref for LoopConsumer {
    type Target = Arc<KafkaStreamConsumer>;

    fn deref(&self) -> &Self::Target {
        &self.consumer
    }
}

impl Drop for LoopConsumer {
    fn drop(&mut self) {
        // SAFETY: `drop` runs once, and nothing reads the field after it.
        let consumer = unsafe { ManuallyDrop::take(&mut self.consumer) };
        if tokio::runtime::Handle::try_current().is_ok() {
            close_off_runtime_or_leak(consumer, &self.queue, &mut spawn_named_thread);
        }
    }
}

/// `final_commit_off_runtime` over any [`FinalCommit`] and any thread
/// spawner, which takes the thread's name and its body and answers as
/// `std::thread::Builder::spawn` does.
///
/// The thread is spawned first and handed the consumer only once it exists.
/// A closure that owned the consumer would be dropped inside a failed
/// `spawn`, on this thread, and `rd_kafka_consumer_close` would run right
/// here, which is the one thing this function exists to prevent. When no
/// thread can be spawned nothing commits: the failure is reported as
/// [`CommitFailure::NoThread`], so the caller settles its discards as
/// survived, and the consumer goes to [`close_off_runtime_or_leak`], never
/// to a drop on this thread.
///
/// The other two kinds: the broker's answer to the commit is
/// [`CommitFailure::Rejected`] with librdkafka's text for it, and a commit
/// with no answer within `deadline`, or whose thread ended without
/// answering, is [`CommitFailure::Deadline`] with the time waited, its
/// result unknown.
///
/// The commit's result and the close are reported on two channels. The
/// result goes out before the drop, so a landed commit followed by a slow
/// LeaveGroup is a landed commit and not a missed deadline; the close is
/// then waited for inside what is left of `deadline`, and a close that
/// outlives it is logged, never returned. Past the deadline the thread
/// finishes on its own, close included.
///
/// A commit the coordinator refuses while the group rebalances goes to
/// `hand_back`, see [`HandBack`]: handed back, it closes nothing, so the
/// stop can poll for the rebalance and commit again, and a hand-back nobody
/// waits for any more, past the deadline, closes the consumer on the thread
/// after all. A refusal that left the assignment lost, see
/// [`FinalCommit::assignment_lost`], never reaches `hand_back`: there is
/// nothing to commit again, and it ends as any other refusal does.
async fn final_commit_on_thread<C, S, B>(
    consumer: C,
    tpl: Option<TopicPartitionList>,
    queue: &str,
    deadline: Duration,
    hand_back: HandBack<C, B>,
    spawn: &mut S,
) -> FinalCommitted<B>
where
    C: FinalCommit,
    B: Send + 'static,
    S: FnMut(String, Box<dyn FnOnce() + Send>) -> std::io::Result<()>,
{
    let started = Instant::now();
    let (committed_tx, committed_rx) = oneshot::channel::<Committed<B>>();
    let (closed_tx, closed_rx) = oneshot::channel::<bool>();
    let handed = hand_to_new_thread(
        spawn,
        format!("shove-kafka-final-commit {queue}"),
        (consumer, tpl),
        move |(consumer, tpl)| {
            let result = match tpl {
                Some(tpl) => consumer.commit_sync(&tpl),
                None => Ok(()),
            };
            let (consumer, result) = match result {
                Err(error) if is_rebalance_refusal(&error) && !consumer.assignment_lost() => {
                    match hand_back(consumer, error) {
                        Ok(back) => {
                            // Unsent, the hand-back drops here, which closes
                            // the consumer on this thread.
                            drop(committed_tx.send(Committed::HandedBack(back)));
                            return;
                        }
                        Err((consumer, error)) => (consumer, Err(error)),
                    }
                }
                result => (consumer, result),
            };
            // The commit's result goes out before the close, so a slow
            // close is never read as a slow commit. Nobody may be listening
            // any more; that is the deadline case.
            let _ = committed_tx.send(Committed::Result(result));
            // The last `Arc`: `rd_kafka_consumer_close` runs here, off the
            // runtime, however long the broker takes to answer. With another
            // holder left, this drop closes nothing, and the signal says so.
            let closes = consumer.is_last_handle();
            drop(consumer);
            let _ = closed_tx.send(closes);
        },
    );
    if let Err(((consumer, _tpl), e)) = handed {
        tracing::error!(
            queue,
            error = %e,
            "could not spawn the final commit thread; this commit is never made, and redelivery \
             starts from the last position the broker accepted"
        );
        close_off_runtime_or_leak(consumer, queue, spawn);
        return FinalCommitted::Ended(Err(CommitFailure::NoThread));
    }
    let mut answered = true;
    let committed = match tokio::time::timeout(deadline, committed_rx).await {
        Ok(Ok(Committed::HandedBack(back))) => return FinalCommitted::Rebalancing(back),
        // librdkafka's text for the broker's answer: the error code and its
        // description, and nothing of the records behind the positions.
        Ok(Ok(Committed::Result(result))) => {
            result.map_err(|e| CommitFailure::Rejected(e.to_string()))
        }
        // The thread died before it could answer: not the broker's verdict,
        // so not a rejection. The result is unknown, like a missed deadline.
        Ok(Err(_recv)) => {
            tracing::warn!(
                queue,
                "final commit thread ended without a result; the result is unknown"
            );
            Err(CommitFailure::Deadline(started.elapsed()))
        }
        Err(_elapsed) => {
            tracing::warn!(
                queue,
                ?deadline,
                "final offset commit did not finish within the shutdown deadline; \
                 giving up on its result, redelivery may start from the last position the \
                 broker accepted"
            );
            answered = false;
            Err(CommitFailure::Deadline(deadline))
        }
    };
    if !answered {
        // The thread is still inside the commit, so the close has not begun
        // and a close deadline here would report nothing true: the close
        // follows the commit when librdkafka answers it.
        tracing::warn!(
            queue,
            "the consumer's close follows the commit the thread is still waiting on; the member \
             leaves its group once that commit returns"
        );
        return FinalCommitted::Ended(committed);
    }
    // The close, inside what is left of the deadline. It is reported apart
    // from the commit: a LeaveGroup the broker is slow to answer is logged
    // here and changes nothing about `committed`.
    let remaining = deadline.saturating_sub(started.elapsed());
    match tokio::time::timeout(remaining, closed_rx).await {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => {
            tracing::warn!(
                queue,
                "the consumer handle was dropped but another holder remains, a commit thread \
                 still waiting on its answer; the close runs when that commit returns"
            );
        }
        Ok(Err(_recv)) => {
            tracing::warn!(
                queue,
                "final commit thread ended before it reported the consumer's close"
            );
        }
        Err(_elapsed) => {
            tracing::warn!(
                queue,
                ?deadline,
                "the consumer's close did not finish within the shutdown deadline; the thread \
                 finishes it on its own"
            );
        }
    }
    FinalCommitted::Ended(committed)
}

/// Spawn a thread that waits for one `value`, then hand `value` to it.
///
/// The thread is created before anything is moved toward it, so a failed
/// spawn leaves `value` with the caller, returned in the `Err`, rather than
/// dropped inside `spawn` on the calling thread, which is what happens to
/// the captures of a closure `Builder::spawn` could not start. `run`
/// receives `value` on the new thread and drops it there.
fn hand_to_new_thread<T, S>(
    spawn: &mut S,
    name: String,
    value: T,
    run: impl FnOnce(T) + Send + 'static,
) -> std::result::Result<(), (T, std::io::Error)>
where
    T: Send + 'static,
    S: FnMut(String, Box<dyn FnOnce() + Send>) -> std::io::Result<()>,
{
    let (tx, rx) = std_mpsc::channel::<T>();
    let body = move || {
        // The sender is dropped without sending only when the caller never
        // got to send; then there is nothing to own here.
        if let Ok(value) = rx.recv() {
            run(value);
        }
    };
    if let Err(e) = spawn(name, Box::new(body)) {
        return Err((value, e));
    }
    // The thread blocks on `recv` until this send, so the send fails only
    // if the thread died first; even then `value` comes back to the caller
    // rather than being dropped here.
    tx.send(value).map_err(|std_mpsc::SendError(value)| {
        (
            value,
            std::io::Error::other("the thread ended before it received its work"),
        )
    })
}

/// Where a consumer goes when its final-commit thread could not be spawned,
/// and where a loop's own handle goes when it is dropped on a runtime
/// thread (see [`LoopConsumer`]).
///
/// The close blocks for as long as librdkafka takes to leave the group, so
/// it must not run on the runtime thread, which is the one thing
/// `final_commit_off_runtime` exists to prevent. A close-only thread takes
/// it; after a failed final-commit thread that is a second try, since a
/// failed spawn is often a momentary `EAGAIN`. If the spawn fails the handle
/// is leaked, deliberately, and logged at error level: the process is
/// failing to create threads and is degraded already, and the alternative
/// is a runtime thread blocked for as long as a frozen coordinator keeps
/// librdkafka retrying.
///
/// What the leak costs. The leaked instance keeps heartbeating, so while the
/// process lives the member holds its partitions until `max.poll.interval.ms`
/// (five minutes) passes without a poll and librdkafka leaves the group on
/// its own; the session timeout does not apply to a member that still
/// heartbeats. Once the process exits the broker drops the member after the
/// session timeout, as after a crash. At-least-once delivery covers both.
fn close_off_runtime_or_leak<C, S>(consumer: C, queue: &str, spawn: &mut S)
where
    C: Send + 'static,
    S: FnMut(String, Box<dyn FnOnce() + Send>) -> std::io::Result<()>,
{
    match hand_to_new_thread(
        spawn,
        format!("shove-kafka-consumer-close {queue}"),
        consumer,
        drop,
    ) {
        Ok(()) => tracing::warn!(
            queue,
            "the consumer closes on a dedicated thread, without a final commit"
        ),
        Err((consumer, e)) => {
            tracing::error!(
                queue,
                error = %e,
                "no thread for the consumer's close; leaking the handle rather than closing \
                 it on the runtime thread. The leaked consumer keeps heartbeating: its partitions \
                 stay assigned until max.poll.interval.ms passes without a poll, or until the \
                 process exits and the session timeout drops the member"
            );
            std::mem::forget(consumer);
        }
    }
}

/// Re-applies a loop's pause intent over its whole assignment: `paused`
/// pauses every partition the member holds, and otherwise every one is
/// resumed.
///
/// A loop pauses and resumes its assignment as a whole, while librdkafka
/// keeps a pause flag per partition and keeps it across a revoke. A
/// partition revoked while the loop was paused and handed back later
/// therefore arrives paused, and one assigned fresh arrives unpaused,
/// whatever the loop's intent is by then: the revoke and the start that
/// follows a re-assign clear only the library's own pause flag (bundled
/// librdkafka 2.12.1, `rdkafka_assignment.c:371-375` and `:501-505`). Each
/// loop calls this with the intent its own state holds, see
/// [`reconcile_pause_after_assign`] and `RegistryStall::end`.
fn apply_pause_intent(consumer: &KafkaStreamConsumer, paused: bool) -> Result<()> {
    if paused {
        consumer
            .pause_assignment()
            .map_err(|e| map_kafka_error("pause failed", e))
    } else {
        consumer
            .resume_assignment()
            .map_err(|e| map_kafka_error("resume failed", e))
    }
}

/// Re-applies the loop's pause intent, see [`apply_pause_intent`], when the
/// rebalance events a loop just drained held an assign, and does nothing
/// otherwise. Every loop that keeps a group assignment calls it after every
/// drain of its events, under every commit policy: the concurrent loop at
/// the top of a pass, in its receive arm and in its permit wait, a
/// sequenced consumer on every pass and wait, the batch loop and the DLQ
/// drain. A drain that met an assign and left the intent unapplied would
/// leave a partition handed back paused stalled for as long as the loop
/// stays unpaused: under `CommitPolicy::Interval`, on a sequenced consumer
/// between waits, and in the batch and DLQ loops between registry stalls,
/// nothing else resumes it.
fn reconcile_pause_after_assign(
    consumer: &KafkaStreamConsumer,
    paused: bool,
    assigned: bool,
) -> Result<()> {
    if assigned {
        apply_pause_intent(consumer, paused)
    } else {
        Ok(())
    }
}

/// What a wait for a confirmed commit ends with; see [`commit_confirmed`].
enum CommitAnswer {
    /// The coordinator accepted the commit.
    Confirmed,
    /// The coordinator refused it, librdkafka gave it up, or no thread could
    /// be spawned for it. The position is not known to have landed.
    Rejected(KafkaError),
    /// Shutdown fired during the wait. The commit is still in flight on its
    /// thread; its answer arrives on the receiver, which the stop waits for
    /// before the final commit, see [`PendingCommit`].
    Cancelled(oneshot::Receiver<KafkaResult<()>>),
}

/// A commit on its thread whose answer the loop has not read yet: the
/// answer, the positions the commit carries, and the discards it retires.
/// It is the one commit a loop has in flight, see [`CommitSlot`], or a
/// commit shutdown cut short: a per-record commit, or a batch span's, see
/// [`commit_terminal_span`]. The stop waits for it, for at most
/// `PENDING_COMMIT_BUDGET`, before it drains the final position, so the
/// final commit does not race the thread that carries the one in flight. An
/// answer that does not come in time ends the stop with
/// `ShoveError::Commit` of the `Deadline` kind carrying that budget, see
/// [`stop_with_final_commit`].
struct PendingCommit {
    answer: oneshot::Receiver<KafkaResult<()>>,
    positions: Vec<CommitPosition>,
    discards: Vec<CoveredDiscard>,
}

/// Commit `tpl` synchronously on a thread of its own, and return at once
/// with the receiver its answer arrives on.
///
/// Why a thread, and why synchronous. An asynchronous commit reports nothing
/// back: rust-rdkafka 0.39 sets no `offset_commit_cb`, so the result of a
/// manual asynchronous commit reaches no callback and no queue (bundled
/// librdkafka 2.12.1, `rdkafka_offset.c:387-403`, `rdkafka_cgrp.c:3871-3908`;
/// see the doc on `RebalanceContext`'s `ConsumerContext` impl). A
/// synchronous commit returns the coordinator's answer. It blocks for the
/// round trip, so it does not run on the runtime thread, and it is not a
/// `spawn_blocking` task either: one of those holds the runtime's shutdown
/// for as long as librdkafka waits, which is the reason the final commit
/// has a thread of its own too. The spawn costs microseconds against a
/// coordinator round trip of milliseconds, and a loop has at most one such
/// commit in flight.
///
/// The answer is librdkafka's result for the whole commit operation, which
/// it waits for without a timeout of its own (`rdkafka_offset.c:403-406`):
/// a request unanswered for `socket.timeout.ms` is retried up to two times
/// (`rdkafka_buf.c:161-162`, `rdkafka_proto.h:47`,
/// `rdkafka_request.c:1784-1787`), and a commit with no coordinator waits
/// for one up to `session.timeout.ms` (`rdkafka_cgrp.c:3744-3746`), so the
/// answer can take several socket timeouts to come back as a rejection.
///
/// The thread drops its clone of the handle before it answers, so once the
/// answer is in the caller's handle is the last again. A spawn failure
/// commits nothing and is the `Err`; the clone is dropped here then, which
/// closes nothing while the caller holds its own.
fn spawn_commit(
    consumer: &Arc<KafkaStreamConsumer>,
    tpl: TopicPartitionList,
    queue: &str,
) -> std::io::Result<oneshot::Receiver<KafkaResult<()>>> {
    let (done_tx, done_rx) = oneshot::channel::<KafkaResult<()>>();
    hand_to_new_thread(
        &mut spawn_named_thread,
        format!("shove-kafka-commit {queue}"),
        (Arc::clone(consumer), tpl),
        move |(consumer, tpl)| {
            let result = consumer.commit(&tpl, CommitMode::Sync);
            drop(tpl);
            drop(consumer);
            // Nobody may be listening any more; that is the fault case.
            let _ = done_tx.send(result);
        },
    )
    .map(|()| done_rx)
    .map_err(|(_consumer_and_tpl, e)| e)
}

/// The commits a receive loop makes under `CommitPolicy::Interval`, a
/// sequenced consumer's, the batch consumer's and the DLQ drain's: at most one
/// in flight, made with [`spawn_commit`] while the loop goes on, and spaced by
/// the gate.
///
/// The loop reads the answer back, as it reads a completion: a select arm
/// wakes on it, see [`commit_answer`], and the top of every pass, and of
/// every pass of a wait that commits, takes one that came meanwhile. An
/// accepted commit confirms the discards it retires and ends the streak of
/// every partition it carried. A rejected one, or one no thread could
/// carry, hands its discards back and marks its partitions dirty, so the
/// next drain re-offers their positions, and the fence judges the streak;
/// see `OffsetTracker::commit_accepted` and `OffsetTracker::commit_rejected`.
/// No drain runs while a commit is in flight, which keeps the in-flight
/// count at one however fast the loop completes records.
///
/// `rd_kafka_commit_queue` would deliver an asynchronous commit's answer to
/// a queue the loop drains, but rust-rdkafka 0.39 wraps neither that call
/// nor a queue of the application's own (its `NativeQueue` is crate-private),
/// so it would take raw FFI for the queue's creation, polling, wake-up and
/// teardown order against the client; a commit on a thread takes none.
struct CommitSlot {
    gate: AsyncCommitGate,
    in_flight: Option<PendingCommit>,
}

impl CommitSlot {
    fn new(policy: CommitPolicy) -> Self {
        Self {
            gate: AsyncCommitGate::new(policy),
            in_flight: None,
        }
    }

    /// Drain the tracker and commit what it offers, when the gate is due and
    /// no commit is in flight. The gate is marked once the commit is on its
    /// thread.
    fn commit_due(
        &mut self,
        consumer: &Arc<KafkaStreamConsumer>,
        tracker: &mut OffsetTracker,
        queue: &str,
        now: Instant,
    ) {
        if self.in_flight.is_some() || !self.gate.due(now) {
            return;
        }
        let Some(DrainedCommit {
            tpl,
            positions,
            discards,
        }) = tracker.drain_committable()
        else {
            return;
        };
        match spawn_commit(consumer, tpl, queue) {
            Ok(answer) => {
                self.gate.mark(Instant::now());
                self.in_flight = Some(PendingCommit {
                    answer,
                    positions,
                    discards,
                });
            }
            Err(e) => {
                tracing::error!(
                    queue,
                    error = %e,
                    "could not spawn the commit thread; nothing commits, the position is re-offered"
                );
                let now = Instant::now();
                tracker.commit_rejected(&positions, discards, now);
                self.gate.mark_rejected(now);
            }
        }
    }

    /// Settle the answer of the commit in flight, if it has come.
    fn settle_arrived(&mut self, tracker: &mut OffsetTracker, queue: &str) {
        let answer = match self
            .in_flight
            .as_mut()
            .map(|pending| pending.answer.try_recv())
        {
            None | Some(Err(oneshot::error::TryRecvError::Empty)) => return,
            Some(Ok(result)) => Some(result),
            Some(Err(oneshot::error::TryRecvError::Closed)) => None,
        };
        self.settle(answer, tracker, queue);
    }

    /// Settle the commit in flight on `answer`: its result, or `None` when
    /// its thread ended without one, which is taken as a rejection since the
    /// position is not known to have landed.
    fn settle(
        &mut self,
        answer: Option<KafkaResult<()>>,
        tracker: &mut OffsetTracker,
        queue: &str,
    ) {
        let Some(PendingCommit {
            positions,
            discards,
            ..
        }) = self.in_flight.take()
        else {
            return;
        };
        let error = match answer {
            Some(Ok(())) => {
                tracker.commit_accepted(&positions);
                for discard in discards {
                    discard.confirm();
                }
                return;
            }
            Some(Err(e)) => e.to_string(),
            None => "the commit thread ended without a result".to_string(),
        };
        tracing::warn!(
            queue,
            error = %error,
            offsets = ?position_offsets(&positions),
            "offset commit rejected; re-offering the position"
        );
        let now = Instant::now();
        tracker.commit_rejected(&positions, discards, now);
        self.gate.mark_rejected(now);
    }

    /// The commit step of a pass, the same in every loop that commits
    /// through a slot: the concurrent receive loop, its permit wait, a
    /// sequenced consumer's pass and waits, the batch loop's pass and schema
    /// registry stall, and the DLQ drain's pass and registry waits. It
    /// settles the answer of the
    /// commit in flight if it came, applies the rebalance events, ends the
    /// member with [`fenced_error`] once a partition's commits have been
    /// rejected for `fence.threshold` with nothing resolving them (see
    /// `OffsetTracker::fenced`), and then makes the commit the gate allows.
    /// The events are applied before the commit, so a partition revoked
    /// meanwhile is dropped with its completions and never committed by this
    /// member.
    ///
    /// Under `CommitPolicy::PerRecord` the step makes no commit: that loop
    /// commits through [`commit_confirmed`] and waits for the answer. The
    /// `Ok` says whether the events held an assign, see
    /// `OffsetTracker::apply_rebalance_events`.
    fn pass(
        &mut self,
        consumer: &Arc<KafkaStreamConsumer>,
        tracker: &mut OffsetTracker,
        rebalance_rx: &std_mpsc::Receiver<RebalanceEvent>,
        fence: Fence<'_>,
        queue: &str,
    ) -> Result<bool> {
        self.settle_arrived(tracker, queue);
        let assigned = tracker.apply_rebalance_events(rebalance_rx);
        let now = Instant::now();
        if let Some(partition) = tracker.fenced(now, fence.threshold) {
            return Err(fenced_error(
                queue,
                fence.group_id,
                partition,
                fence.threshold,
            ));
        }
        if !self.gate.per_record {
            self.commit_due(consumer, tracker, queue, now);
        }
        Ok(assigned)
    }
}

/// What the fenced consumer detector judges a loop's commits by, see
/// [`CommitSlot::pass`]: the threshold, from [`fence_threshold_over`], and
/// the group the member's error names.
#[derive(Clone, Copy)]
struct Fence<'a> {
    threshold: Duration,
    group_id: &'a str,
}

/// The answer of `in_flight`, or a future that never resolves when nothing
/// is in flight: the select arm a loop that commits through a
/// [`CommitSlot`] wakes on. The arm's handler settles what it reads at once,
/// since a oneshot receiver polled again after it answered panics.
async fn commit_answer(in_flight: Option<&mut PendingCommit>) -> Option<KafkaResult<()>> {
    match in_flight {
        Some(pending) => (&mut pending.answer).await.ok(),
        None => std::future::pending().await,
    }
}

/// Commit `tpl` synchronously on a thread of its own (see [`spawn_commit`])
/// and wait for the coordinator's answer while the loop keeps polling, see
/// [`answer_while_polling`]: the commit every completion triggers under
/// `CommitPolicy::PerRecord`, and the commit of a batch whose span carries
/// dead letters or discards, see [`commit_terminal_span`]. `grace` is how
/// long the wait goes before it polls. A spawn failure is a rejection:
/// nothing commits, and the position is re-offered.
async fn commit_confirmed(
    consumer: &Arc<KafkaStreamConsumer>,
    tpl: TopicPartitionList,
    shutdown: &CancellationToken,
    fault_rx: Option<&mut mpsc::Receiver<ShoveError>>,
    queue: &str,
    paused: &mut bool,
    grace: Duration,
) -> Result<CommitAnswer> {
    #[cfg(feature = "test-support")]
    per_record_probe::COMMIT_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    let mut answer = match spawn_commit(consumer, tpl, queue) {
        Ok(answer) => answer,
        Err(e) => {
            tracing::error!(
                queue,
                error = %e,
                "could not spawn the commit thread; nothing commits, the position is re-offered"
            );
            return Ok(CommitAnswer::Rejected(KafkaError::ConsumerCommit(
                RDKafkaErrorCode::Fail,
            )));
        }
    };
    Ok(
        match answer_while_polling(
            consumer,
            &mut answer,
            shutdown,
            fault_rx,
            queue,
            paused,
            grace,
        )
        .await?
        {
            Answered::Came(Some(Ok(()))) => CommitAnswer::Confirmed,
            Answered::Came(Some(Err(e))) => CommitAnswer::Rejected(e),
            Answered::Came(None) => {
                tracing::warn!(queue, "commit thread ended without a result");
                CommitAnswer::Rejected(KafkaError::ConsumerCommit(RDKafkaErrorCode::Fail))
            }
            Answered::Stopped => CommitAnswer::Cancelled(answer),
        },
    )
}

/// How a wait for the answer of a commit on its thread ended, see
/// [`answer_while_polling`].
enum Answered {
    /// The answer came: the commit's result, or `None` when its thread ended
    /// without one, as [`commit_answer`] reads it.
    Came(Option<KafkaResult<()>>),
    /// Shutdown fired first. The commit is still in flight on its thread,
    /// and its answer is still to come on the receiver.
    Stopped,
}

/// Wait for `answer`, the answer of a commit on its thread (see
/// [`spawn_commit`]), while the loop keeps polling.
///
/// The wait is a select that keeps calling `recv()` once `grace` has run
/// out, as the permit wait does, so it serves the rebalance callbacks and
/// the poll budget however long the coordinator takes. Under
/// `CommitPolicy::PerRecord` the assignment is paused while a commit is
/// unconfirmed (see the receive loop), so `recv()` yields nothing of it, and
/// the wait polls at once. On the batch loop nothing is paused, so a
/// `recv()` would be handed a fetched record straight away, and the wait
/// gives the commit [`PERMIT_WAIT_GRACE`] first: a commit answered inside
/// it, the usual case, puts nothing back. A record `recv()` does yield is
/// put back with the assignment paused, and `paused` says so. The wait ends
/// on the answer, on shutdown, or on a fault a handler task reports over
/// `fault_rx`. Shutdown is read first, so a stop leaves the receiver unread
/// for the stop to wait on.
async fn answer_while_polling(
    consumer: &Arc<KafkaStreamConsumer>,
    answer: &mut oneshot::Receiver<KafkaResult<()>>,
    shutdown: &CancellationToken,
    fault_rx: Option<&mut mpsc::Receiver<ShoveError>>,
    queue: &str,
    paused: &mut bool,
    grace: Duration,
) -> Result<Answered> {
    let fault = handler_fault(fault_rx);
    tokio::pin!(fault);
    let mut polling = grace.is_zero();
    let grace = tokio::time::sleep(grace);
    tokio::pin!(grace);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(Answered::Stopped),
            e = &mut fault => return Err(e),
            result = &mut *answer => return Ok(Answered::Came(result.ok())),
            () = &mut grace, if !polling => polling = true,
            received = consumer.recv(), if polling => {
                let msg = match received {
                    Ok(msg) => msg,
                    Err(e) if recv_error_is_transient(consumer.transport_gate(), &e) => {
                        log_transient_recv_error(queue, &e);
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, queue, "consumer recv error");
                        return Err(map_kafka_error(
                            &format!("consumer recv error on {queue} while waiting for a commit"),
                            e,
                        ));
                    }
                };
                #[cfg(feature = "test-support")]
                put_back_probe::COMMIT_WAIT.fetch_add(1, Ordering::SeqCst);
                put_back(consumer, queue, msg.partition(), msg.offset())?;
                consumer
                    .pause_assignment()
                    .map_err(|e| map_kafka_error("pause failed", e))?;
                *paused = true;
                tracing::debug!(
                    queue,
                    partition = msg.partition(),
                    offset = msg.offset(),
                    "record delivered while a commit awaits its answer put back; pause widened to the current assignment"
                );
            }
        }
    }
}

/// The end of a consume loop on a stop, once nothing more can complete:
/// settle the commit the stop found in flight, then commit every partition's
/// position synchronously and close the consumer, both on a thread of their
/// own, see [`final_commit_off_runtime`]. The standard consumer calls it
/// once its handler tasks have drained, the sequenced consumer once its
/// record in hand is left behind, the batch consumer once its stop has
/// flushed the batch in hand, and the DLQ drain once its dead letter in hand
/// is finished with or left behind.
///
/// A final commit the coordinator refuses while the group rebalances, see
/// [`is_rebalance_refusal`], is tried again while more than
/// `FINAL_COMMIT_RETRY_RESERVE` of the deadline is left, checked when the
/// try starts and again when its refusal comes back. The reserve is room for
/// the poll, the last try and the close, so the tries end inside the
/// deadline, and a refusal that comes back with less left stands. The
/// consumer comes back from the thread unclosed, polls for
/// `FINAL_COMMIT_RETRY_POLL` with
/// its assignment paused, which serves the rebalance callbacks the
/// rebalance waits on, and drains the rebalance events, so a partition the
/// rebalance revoked leaves the next try. Another member's stop, or the
/// autoscaler retiring one, starts such a rebalance, so members that stop
/// together do not routinely end with a refused commit. A stale generation
/// outside a rebalance is the exception: librdkafka gives the assignment up
/// as lost with that refusal, so it is not tried again, and the stop ends
/// with it as `ShoveError::Commit`, see [`FinalCommit::assignment_lost`].
///
/// `Err` is `ShoveError::Commit`: a final commit the broker did not confirm,
/// or a commit in flight that had no answer in time. `Commit` is not
/// retryable, so `run_with_reconnect` returns it, and a group's spawner
/// counts it under `errors`.
async fn stop_with_final_commit(
    consumer: LoopConsumer,
    tracker: &mut OffsetTracker,
    // `&mut`, because the receiver is not `Sync` and a shared borrow held
    // across this future's awaits would make the future `!Send`.
    rebalance_rx: &mut std_mpsc::Receiver<RebalanceEvent>,
    pending_commit: Option<PendingCommit>,
    queue: &str,
) -> Result<()> {
    // One budget for everything the shutdown waits on: a commit found in
    // flight gets at most `PENDING_COMMIT_BUDGET` of it, and the final commit,
    // its tries again and the close get the rest, so the close is never left
    // without a slice. With nothing in flight the final commit has the whole
    // deadline, as in 0.15.
    let mut final_deadline = SHUTDOWN_COMMIT_DEADLINE;
    // The commit the stop found in flight is settled first, so the final
    // commit does not race the thread that carries it. An accepted answer
    // confirms what rode on it, and the final commit then carries the
    // position again, as every final commit does; a rejection hands what
    // rode on it back to the final commit; and no answer inside the share
    // ends the stop here: the commit is still in flight on its thread, so a
    // second commit would race it and the close cannot run until that thread
    // drops its clone of the handle. This loop's handle goes to a thread of
    // its own, as always, and the stop is reported as a `Commit` of the
    // `Deadline` kind carrying the share, which says the member leaves its
    // group once that commit returns.
    if let Some(pending) = pending_commit {
        let started = Instant::now();
        match tokio::time::timeout(PENDING_COMMIT_BUDGET, pending.answer).await {
            Ok(Ok(Ok(()))) => {
                for discard in pending.discards {
                    discard.confirm();
                }
            }
            Ok(answer) => {
                tracing::warn!(
                    queue,
                    ?answer,
                    "the commit in flight at shutdown was not accepted; re-offering its position in the final commit"
                );
                tracker.commit_rejected(&pending.positions, pending.discards, Instant::now());
            }
            Err(_elapsed) => {
                let offsets = position_offsets(&pending.positions);
                tracing::warn!(
                    queue,
                    ?offsets,
                    budget = ?PENDING_COMMIT_BUDGET,
                    "the commit in flight at shutdown had no answer within its share of the deadline; stopping without a second commit, the member leaves the group once it returns"
                );
                for discard in pending.discards {
                    discard.survived();
                }
                close_without_commit(
                    consumer,
                    queue,
                    SHUTDOWN_COMMIT_DEADLINE.saturating_sub(started.elapsed()),
                )
                .await;
                return Err(ShoveError::Commit(Box::new(FailedCommit {
                    topic: queue.to_string(),
                    offsets,
                    kind: CommitFailure::Deadline(PENDING_COMMIT_BUDGET),
                })));
            }
        }
        final_deadline = SHUTDOWN_COMMIT_DEADLINE.saturating_sub(started.elapsed());
    }
    // A try again gets what is left of `final_deadline` once the tries before
    // it and their polls are over.
    let tries_started = Instant::now();
    let mut deadline = final_deadline;
    let mut consumer = consumer;
    let mut polled = true;
    loop {
        tracker.apply_rebalance_events(rebalance_rx);
        // Every partition's current position, whether or not an earlier
        // drain offered it: the final `Sync` commit offers every safe
        // position, including one a rejected commit left to re-offer, for
        // the broker to confirm. See `PartitionTracker::drain_all`.
        let (tpl, discards) = match tracker.drain_all() {
            Some((tpl, discards)) => (Some(tpl), discards),
            None => (None, Vec::new()),
        };
        let offsets = tpl.as_ref().map(committed_offsets).unwrap_or_default();
        let may_retry = polled && deadline > FINAL_COMMIT_RETRY_RESERVE;
        // The commit and the consumer's close both block: `CommitMode::Sync`
        // waits for the coordinator, and `Drop` runs
        // `rd_kafka_consumer_close`. A frozen broker holds either for as long
        // as librdkafka retries - minutes - and a blocking task cannot be
        // aborted once started, so `spawn_blocking` would only move the wait
        // onto a thread the runtime's shutdown then waits for. A dedicated
        // thread that owns the last `Arc` of the consumer holds neither the
        // runtime nor the process: `deadline`, what is left of
        // `SHUTDOWN_COMMIT_DEADLINE` once a commit in flight and any earlier
        // try are over, bounds how long this loop waits for its result, and
        // past it the thread finishes on its own.
        let committed = if may_retry {
            final_commit_off_runtime(consumer, tpl, queue, deadline, hand_back).await
        } else {
            FinalCommitted::Ended(
                final_commit_off_runtime(consumer, tpl, queue, deadline, keep_to_close)
                    .await
                    .ended(),
            )
        };
        let kind = match committed {
            FinalCommitted::Ended(Ok(())) => {
                for discard in discards {
                    discard.confirm();
                }
                return Ok(());
            }
            FinalCommitted::Ended(Err(kind)) => {
                for discard in discards {
                    discard.survived();
                }
                if offsets.is_empty() {
                    // Nothing was offered, so no commit failed: only the
                    // close could not run on the commit thread, and
                    // `close_off_runtime_or_leak` has dealt with that.
                    tracing::warn!(
                        queue,
                        error = %kind,
                        "no final offset commit to make; the consumer ends clean"
                    );
                    return Ok(());
                }
                kind
            }
            FinalCommitted::Rebalancing(Refused {
                consumer: unclosed,
                error,
            }) => {
                let left = final_deadline.saturating_sub(tries_started.elapsed());
                if left <= FINAL_COMMIT_RETRY_RESERVE {
                    tracing::warn!(
                        queue,
                        %error,
                        ?offsets,
                        ?left,
                        "the final offset commit was refused while the group rebalances, with too \
                         little of the shutdown deadline left to poll and commit again"
                    );
                    for discard in discards {
                        discard.survived();
                    }
                    close_without_commit(unclosed, queue, left).await;
                    CommitFailure::Rejected(error.to_string())
                } else {
                    tracing::warn!(
                        queue,
                        %error,
                        ?offsets,
                        "the final offset commit was refused while the group rebalances; polling \
                         with the assignment paused, then committing again"
                    );
                    consumer = unclosed;
                    tracker.re_pend(discards);
                    if let Err(e) = poll_paused(&consumer, queue, FINAL_COMMIT_RETRY_POLL).await {
                        tracing::error!(
                            queue,
                            error = %e,
                            "polling for the rebalance at shutdown failed; the next final commit \
                             is the last"
                        );
                        polled = false;
                    }
                    deadline = final_deadline.saturating_sub(tries_started.elapsed());
                    continue;
                }
            }
        };
        // The member ends with the failure instead of a clean exit.
        tracing::error!(
            queue,
            error = %kind,
            ?offsets,
            "final offset commit failed during shutdown; redelivery starts \
             from the last position the broker accepted, and the consumer \
             ends with ShoveError::Commit"
        );
        return Err(ShoveError::Commit(Box::new(FailedCommit {
            topic: queue.to_string(),
            offsets,
            kind,
        })));
    }
}

/// Polls `consumer` for `period` with its assignment paused: the wait a
/// stop makes before it tries again a final commit refused while the group
/// rebalances, see [`stop_with_final_commit`]. The polls serve the
/// rebalance callbacks, and a record one hands over, from a partition
/// assigned meanwhile, is put back with the pause widened, as in every
/// polling wait. A stop never reconnects, so the wait reads no gate and
/// rides out every `__TRANSPORT` and `__RESOLVE`, see
/// [`recv_error_is_transient`].
async fn poll_paused(consumer: &KafkaStreamConsumer, queue: &str, period: Duration) -> Result<()> {
    apply_pause_intent(consumer, true)?;
    let polled = tokio::time::sleep(period);
    tokio::pin!(polled);
    loop {
        tokio::select! {
            biased;
            () = &mut polled => return Ok(()),
            received = consumer.recv() => match received {
                Ok(msg) => {
                    put_back(consumer, queue, msg.partition(), msg.offset())?;
                    apply_pause_intent(consumer, true)?;
                }
                Err(e) if recv_error_is_transient(None, &e) => log_transient_recv_error(queue, &e),
                Err(e) => {
                    return Err(map_kafka_error(
                        &format!("consumer recv error on {queue} while polling for a rebalance at shutdown"),
                        e,
                    ));
                }
            },
        }
    }
}

/// Timeout for `seek_partitions` when redelivering an un-acked batch.
///
/// **Must stay non-zero.** `rd_kafka_seek_partitions` only waits for the
/// per-partition seeks — and so only fills in their real results — when it is
/// given a timeout; with zero it returns immediately, leaving every partition
/// marked `__IN_PROGRESS`. [`seek_errors`] would then read every seek as
/// failed and every redelivery would escalate to a reconnect.
const SEEK_TIMEOUT: Duration = Duration::from_secs(5);

/// Raised by a consume loop whose own stop failed after the stop had fired:
/// the standard consumer's wait for its handlers at a stop polls, and a
/// receive error that wait does not ride out, or a put-back or pause
/// librdkafka refuses, ends it with no final commit. [`run_with_reconnect`]
/// then returns the error that ended the attempt instead of reading it as
/// the stop.
#[derive(Default)]
pub(super) struct StopFailed(AtomicBool);

impl StopFailed {
    fn raise(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn raised(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Hands on the result of the stop's wait for its handlers. A wait that
    /// failed is logged at error level and raises the flag, so the run ends
    /// with its error.
    fn raise_if_failed<T>(&self, waited: Result<T>, queue: &str) -> Result<T> {
        waited.inspect_err(|e| {
            tracing::error!(
                queue,
                error = %e,
                "the stop's wait for its handlers failed; closing without the final commit, \
                 so every record completed since the last accepted commit is redelivered, \
                 and the run ends with this error"
            );
            self.raise();
        })
    }
}

/// Runs `f`, the attempt that connects a consumer and runs its loop, and
/// runs it again after a retryable error, under backoff and
/// `max_reconnect_attempts`. An `Ok` ends the run, and so does an error that
/// is not retryable, which is returned. Once `shutdown` has fired, a
/// retryable error ends the run with `Ok(())`, unless the attempt raised
/// `stop_failed`: the error is then the stop's own failure, and it is
/// returned.
pub(super) async fn run_with_reconnect<F, Fut>(
    shutdown: &CancellationToken,
    label: &str,
    max_reconnect_attempts: Option<u32>,
    stop_failed: Option<&StopFailed>,
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
                    if stop_failed.is_some_and(StopFailed::raised) {
                        return Err(e);
                    }
                    return Ok(());
                }
                attempts = attempts.saturating_add(1);
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
                let Some(delay) = backoff.next() else {
                    return Err(e);
                };
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
///
/// A `pub type` alias of the generic
/// [`BatchConsumerOptions<Kafka>`](crate::batch_consumer::BatchConsumerOptions),
/// not a distinct type: every builder this backend needs already exists on
/// the generic type, split across two homes. The backend-agnostic ones
/// (`with_max_batch_size`, `with_max_batch_age`, `with_handler_timeout`,
/// `with_consumer_group`, `with_shutdown`, ...) live directly on
/// `BatchConsumerOptions<B>` in `src/batch_consumer.rs`. The Kafka-only ones —
/// `with_group_id`, `with_auto_offset_reset`, and (under
/// `kafka-schema-registry`) `with_schema_registry`,
/// `with_schema_enforcement`, `accept_schema_subjects` — live in that same
/// file's `#[cfg(feature = "kafka")] impl BatchConsumerOptions<Kafka>` blocks,
/// gated the same way this type alias only exists under `kafka`.
pub type BatchConsumerOptions = GenericBatchConsumerOptions<Kafka>;

/// The parts of a batch flush that don't change between flushes.
struct BatchFlushCtx<'a> {
    consumer: &'a Arc<KafkaStreamConsumer>,
    client: &'a KafkaClient,
    /// `'static`, because a dead letter is published on a task of its own,
    /// see [`dead_letter_span`].
    topology: &'static QueueTopology,
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

/// What one connection of the batch consumer keeps beside its buffer: the
/// tracker its commits are drawn from, the rebalance events, the commit in
/// flight, and the pause a polling wait left behind.
///
/// Every record that joins a span is tracked, see
/// `OffsetTracker::track_epoch`, and a flush retires its span there once
/// it is done with the records, see `OffsetTracker::retire_span`. An acked
/// span commits the way the standard consumer commits under
/// `CommitPolicy::Interval`: through [`CommitSlot`], one commit at a time on
/// a thread of its own while the loop goes on, re-offered when the
/// coordinator rejects it, and judged by the fence, which ends the member
/// once its commits keep being rejected with no rebalance resolving them. A
/// span with dead letters or discards on it commits synchronously instead,
/// and the flush waits for the answer while it polls, see
/// [`commit_terminal_span`]; a rejection of that commit leaves the position
/// to the slot to re-offer as well.
struct BatchState {
    tracker: OffsetTracker,
    rebalance_rx: std_mpsc::Receiver<RebalanceEvent>,
    commits: CommitSlot,
    /// The fenced consumer detector's threshold, see [`fence_threshold_over`].
    fence_threshold: Duration,
    /// The member's group, which the fence's error names.
    group_id: String,
    /// Whether a polling wait left the assignment paused, see
    /// [`acquire_permit_while_polling`]. The batch loop holds no pause of
    /// its own, so the flush that waited lifts it, see
    /// [`resume`](Self::resume).
    paused: bool,
}

impl BatchState {
    /// The commit step of a pass, see [`CommitSlot::pass`]. The loop has
    /// drained the rebalance events itself just before, see
    /// [`apply_batch_rebalance`], so the step finds none of its own; an
    /// assign among them would re-apply the loop's pause intent all the same.
    fn pass(&mut self, consumer: &Arc<KafkaStreamConsumer>, queue: &str) -> Result<()> {
        self.commit_step(consumer, queue, self.paused)
    }

    /// The commit step a schema registry stall runs between its retries, as
    /// a sequenced consumer's stall does, so the positions the flush ahead
    /// of the stalled record retired, and the ones a rebalance or a rejected
    /// commit re-offers meanwhile, are committed however long the outage
    /// runs. No commit passes the stalled record, which is tracked only once
    /// it decodes. The step drains the rebalance events the stall's polls
    /// served. The buffer is empty through the stall, so they abandon no
    /// batch, and the stall reads a revoke of its record's partition off the
    /// record's token. The stall holds the whole assignment paused, so an
    /// assign among the events pauses it again; `RegistryStall::end` lifts
    /// that pause.
    #[cfg(feature = "kafka-schema-registry")]
    fn stall_pass(&mut self, consumer: &Arc<KafkaStreamConsumer>, queue: &str) -> Result<()> {
        self.commit_step(consumer, queue, true)
    }

    /// [`CommitSlot::pass`], then `paused`, the pause intent the caller
    /// holds, re-applied when the events held an assign, see
    /// [`reconcile_pause_after_assign`].
    fn commit_step(
        &mut self,
        consumer: &Arc<KafkaStreamConsumer>,
        queue: &str,
        paused: bool,
    ) -> Result<()> {
        let fence = Fence {
            threshold: self.fence_threshold,
            group_id: &self.group_id,
        };
        let assigned = self.commits.pass(
            consumer,
            &mut self.tracker,
            &self.rebalance_rx,
            fence,
            queue,
        )?;
        reconcile_pause_after_assign(consumer, paused, assigned)
    }

    /// Resumes the assignment a polling wait paused, if it did.
    fn resume(&mut self, consumer: &KafkaStreamConsumer) -> Result<()> {
        if std::mem::take(&mut self.paused) {
            apply_pause_intent(consumer, false)?;
        }
        Ok(())
    }
}

/// Commits a span with dead letters or discards on it, at the positions its
/// flush settled it at, see [`settle_terminal_span`], and settles the
/// discards that ride on the commit. `positions` is each partition's
/// position under the assignment the span tracked it under, and `discards`
/// are the discards those positions retire. `Ok(true)` says the coordinator
/// accepted the commit.
///
/// One commit is in flight at a time, so the slot's own commit is answered
/// first. Both waits poll once [`PERMIT_WAIT_GRACE`] has run out, see
/// [`answer_while_polling`], and both end on a stop, which then waits for
/// the commit in flight for its share of the shutdown deadline before its
/// final commit, see [`stop_with_final_commit`]: a commit the stop cut short
/// goes to the slot as that commit, and one the stop kept from being made at
/// all leaves its discards to the final commit, which carries its position.
/// A revoke the slot's wait serves comes after the drain that followed the
/// dead letters, so it is not judged against the span, which the generation
/// check makes safe: the span's commit lands before the partition's next
/// owner is assigned, or is refused and re-offered without the partition.
///
/// A commit the coordinator rejects, or that no thread or partition list
/// could carry, hands its discards back to the tracker and marks its
/// partitions dirty, so the slot re-offers the position as it re-offers an
/// acked span's, and the fence judges the streak, see
/// `OffsetTracker::commit_rejected`. The span is settled by then: its dead
/// letters have landed, so nothing of it is handed over again.
async fn commit_terminal_span(
    flush: &BatchFlushCtx<'_>,
    state: &mut BatchState,
    positions: Vec<CommitPosition>,
    discards: Vec<CoveredDiscard>,
) -> Result<bool> {
    let queue = flush.queue;
    if let Some(pending) = state.commits.in_flight.as_mut() {
        match answer_while_polling(
            flush.consumer,
            &mut pending.answer,
            flush.shutdown,
            None,
            queue,
            &mut state.paused,
            PERMIT_WAIT_GRACE,
        )
        .await?
        {
            Answered::Came(answer) => state.commits.settle(answer, &mut state.tracker, queue),
            Answered::Stopped => {
                state.tracker.re_pend(discards);
                return Ok(false);
            }
        }
    }
    let mut tpl = TopicPartitionList::new();
    for position in &positions {
        if let Err(e) =
            tpl.add_partition_offset(queue, position.partition, Offset::Offset(position.offset))
        {
            tracing::error!(
                queue,
                partition = position.partition,
                offset = position.offset,
                error = %e,
                "could not add the partition to the batch's commit; nothing is committed, and the \
                 position is re-offered"
            );
            let now = Instant::now();
            state.tracker.commit_rejected(&positions, discards, now);
            state.commits.gate.mark_rejected(now);
            return Ok(false);
        }
    }
    match commit_confirmed(
        flush.consumer,
        tpl,
        flush.shutdown,
        None,
        queue,
        &mut state.paused,
        PERMIT_WAIT_GRACE,
    )
    .await?
    {
        CommitAnswer::Confirmed => {
            state.tracker.commit_accepted(&positions);
            discards.into_iter().for_each(CoveredDiscard::confirm);
            state.commits.gate.mark(Instant::now());
            Ok(true)
        }
        CommitAnswer::Rejected(e) => {
            tracing::warn!(
                queue,
                error = %e,
                offsets = ?position_offsets(&positions),
                "the commit of a batch with dead letters or discards on it was rejected; the \
                 batch is settled, and its position is re-offered"
            );
            let now = Instant::now();
            state.tracker.commit_rejected(&positions, discards, now);
            state.commits.gate.mark_rejected(now);
            Ok(false)
        }
        CommitAnswer::Cancelled(answer) => {
            state.commits.in_flight = Some(PendingCommit {
                answer,
                positions,
                discards,
            });
            Ok(false)
        }
    }
}

/// The parts of a batch decode that don't change between messages.
struct BatchDecodeCtx<'a> {
    queue: &'a str,
    #[cfg(feature = "kafka-schema-registry")]
    schema_registry: Option<&'a Arc<SchemaRegistry>>,
    #[cfg(feature = "kafka-schema-registry")]
    schema_enforcement: SchemaEnforcement,
    #[cfg(feature = "kafka-schema-registry")]
    schema_accepted: &'a [Arc<str>],
    #[cfg(feature = "kafka-schema-registry")]
    schema_message_index: Option<&'a [i32]>,
    /// See `registry_lookup_bound`.
    #[cfg(feature = "kafka-schema-registry")]
    registry_lookup_bound: Duration,
}

/// What the batch loop's decode stage leaves it with once any registry wait
/// is over: a record to buffer, or a pre-handler drop to park. The wait loop
/// resolves `Unavailable` by waiting and `Fatal` by returning, so neither
/// reaches the buffering step, and this type says so where an `unreachable!`
/// arm used to. A wait that sees its record's partition revoked ends with no
/// stage at all, and the record is left to the partition's next owner.
enum BatchStaged<M> {
    Decoded(M),
    Dlq {
        reason: &'static str,
        fail: metrics::FailReason,
    },
}

/// Outcome of decoding one message on its way into a batch. Also the decode
/// stage of an in-place redelivery and of a broadcast redelivery, which
/// decode the retained bytes again through the same context, of every
/// decode of the sequenced loop, and of the DLQ drain's registry decode.
enum BatchDecode<M> {
    Decoded(M),
    /// Undecodable: park the wire bytes for the DLQ under this reason, to be
    /// published once the batch's offsets commit. `fail` travels with it so
    /// the drop site can pair the right `FailReason` with `record_terminal`
    /// without re-deriving it from `reason`.
    Dlq {
        reason: &'static str,
        fail: metrics::FailReason,
    },
    /// The schema registry could not answer for `id` right now; the caller
    /// keeps the bytes and decodes them again later.
    #[cfg(feature = "kafka-schema-registry")]
    Unavailable {
        id: SchemaId,
        error: SchemaRegistryError,
    },
    /// The schema registry answered with a deployment fault; the consumer
    /// stops rather than waiting on it.
    #[cfg(feature = "kafka-schema-registry")]
    Fatal(ShoveError),
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
/// The failure metric is *not* emitted here: the drop site now counts it via
/// `metrics::record_terminal`, which pairs `messages_failed_total` with the
/// pending discard in one call rather than splitting the two across this
/// function and its caller. The DLQ publish itself is deferred to the commit
/// — see [`BatchBuffer::dropped`]. The caller has already extended the
/// batch's offset span, so a dropped message is still committed past.
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
                    dec.schema_message_index,
                    dec.registry_lookup_bound,
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
            Ok(RegistryDecode::Dlq(reason)) => BatchDecode::Dlq {
                reason,
                fail: metrics::FailReason::for_schema_reason(reason),
            },
            Ok(RegistryDecode::Unavailable { id, error }) => BatchDecode::Unavailable { id, error },
            // A deployment fault, not an outage: stop instead of waiting.
            Err(e @ ShoveError::Topology(_)) => BatchDecode::Fatal(e),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    queue = dec.queue,
                    "failed to deserialize message, sending to DLQ"
                );
                BatchDecode::Dlq {
                    reason: "deserialization_error",
                    fail: metrics::FailReason::Deserialize,
                }
            }
        };
    }

    match <T::Codec as crate::Codec<T::Message>>::decode(payload_slice) {
        Ok(m) => BatchDecode::Decoded(m),
        Err(e) => {
            tracing::error!(
                error = %e,
                queue = dec.queue,
                "failed to deserialize message, sending to DLQ"
            );
            BatchDecode::Dlq {
                reason: "deserialization_error",
                fail: metrics::FailReason::Deserialize,
            }
        }
    }
}

/// The original wire bytes of one batched message.
///
/// Kept alongside the decoded message only when the topology declares a
/// DLQ: a `Reject` dead-letters the batch byte-for-byte, see
/// [`settle_terminal_span`]. With no DLQ configured there is nowhere for
/// those bytes to go, so nothing is retained and a batch costs exactly what
/// its decoded messages cost. The headers carry the record's partition and
/// offset, which a dead letter that fails keeps the commit below.
struct RawMessage {
    payload: Bytes,
    key: Option<Bytes>,
    headers: RecordHeaders,
}

/// A message parked for the DLQ, published when the batch it belongs to
/// settles — see [`DroppedMessage`].
struct PendingDlq {
    raw: RawMessage,
    reason: String,
}

/// A message dropped before the handler (oversize / undecodable), carrying
/// its partition and offset, its parked DLQ payload (when the topology
/// declares one) and the discard accounting that the flush settles against
/// the publish + commit results.
///
/// `dlq` is `None` exactly when the topology has no DLQ (see
/// `BatchBuffer::retain_raw`): `publish_to_dlq` would log "no DLQ
/// configured" and discard, so copying and holding every poison payload
/// until the flush buys nothing.
struct DroppedMessage {
    coordinates: (i32, i64),
    dlq: Option<PendingDlq>,
    pending: metrics::PendingDiscard,
}

/// The in-flight batch: the decoded messages destined for the handler, the
/// offset span they cover, and the side buffers that make the flush's DLQ
/// routing correct.
struct BatchBuffer<T: Topic> {
    messages: Vec<(T::Message, MessageMetadata)>,
    /// What `messages` reserves again each time a flush hands it to the
    /// handler, so a batch does not regrow its buffer from nothing.
    capacity: usize,
    /// Whether the topology declares a DLQ, which is when wire bytes are
    /// kept, see [`RawMessage`].
    retain_raw: bool,
    /// Wire bytes, index-parallel to `messages` when `retain_raw` and empty
    /// otherwise: [`push`](Self::push) writes both.
    raw: Vec<RawMessage>,
    /// Each message's partition and offset, index-parallel to `messages`
    /// when the topology declares no DLQ and empty otherwise, where `raw`'s
    /// headers carry them: the record a `Reject`'s discard rides on.
    coordinates: Vec<(i32, i64)>,
    /// Messages dropped pre-handler since the last flush, held until the
    /// batch settles.
    ///
    /// Publishing at drop time re-published the same payload on every
    /// redelivery: a handler returning `Retry` seeks back over the poison,
    /// which fails to decode again, so a sink that is down for ten minutes
    /// turns one bad message into dozens of identical DLQ copies and the
    /// dead-letter alert stops mapping to distinct bad messages. Parking
    /// until the batch settles makes it exactly one — and the publish still
    /// precedes the commit, so the payload is never lost. See
    /// [`Self::flush_len`].
    dropped: Vec<DroppedMessage>,
    /// Lowest offset seen per partition, where a non-`Ack` outcome seeks
    /// back to. Always has the same key set as `end`.
    start: HashMap<i32, i64>,
    /// Exclusive end offset per partition, one past the highest offset seen,
    /// which is what an `Ack` commits.
    end: HashMap<i32, i64>,
    /// The assignment each partition of the span was tracked under, see
    /// `OffsetTracker::track_epoch`. A rebalance that moves a partition
    /// the span holds abandons the span, so one epoch covers each.
    epochs: HashMap<i32, u64>,
}

impl<T: Topic> BatchBuffer<T> {
    fn new(max_batch_size: usize, retain_raw: bool) -> Self {
        // Clamped: `max_batch_size` may be set to `usize::MAX` — see
        // `PREALLOC_CAP`'s doc.
        let capacity = max_batch_size.min(PREALLOC_CAP);
        Self {
            messages: Vec::with_capacity(capacity),
            capacity,
            retain_raw,
            raw: Vec::new(),
            coordinates: Vec::new(),
            dropped: Vec::new(),
            start: HashMap::new(),
            end: HashMap::new(),
            epochs: HashMap::new(),
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
        self.messages.len().saturating_add(self.dropped.len())
    }

    /// Extends the offset span to cover `offset` on `partition`, a record
    /// tracked under the assignment `epoch` names.
    ///
    /// Both bounds are taken against what the span already holds rather
    /// than assumed from arrival order: `start` only ever moves down and
    /// `end` only ever moves up. A record that reaches the buffer out of
    /// offset order can then neither raise the seek target above records
    /// the handler never acked nor pull the commit position below records
    /// it did. The exclusive end saturates at `i64::MAX`, as
    /// `PartitionTracker::position` does, so the batch commit path carries
    /// no unchecked arithmetic.
    fn extend_span(&mut self, partition: i32, offset: i64, epoch: u64) {
        let end = offset.saturating_add(1);
        self.start
            .entry(partition)
            .and_modify(|start| *start = (*start).min(offset))
            .or_insert(offset);
        self.end
            .entry(partition)
            .and_modify(|current| *current = (*current).max(end))
            .or_insert(end);
        self.epochs.entry(partition).or_insert(epoch);
    }

    /// Buffers a decoded message, and its wire bytes when the topology
    /// declares a DLQ, which `raw` builds only then, or its `coordinates`
    /// when it declares none.
    fn push(
        &mut self,
        message: T::Message,
        metadata: MessageMetadata,
        coordinates: (i32, i64),
        raw: impl FnOnce() -> RawMessage,
    ) {
        self.messages.push((message, metadata));
        if self.retain_raw {
            self.raw.push(raw());
        } else {
            self.coordinates.push(coordinates);
        }
    }

    /// Records a message dropped pre-handler at `coordinates`, parking its
    /// bytes for the DLQ only when there is a DLQ to publish them to, and
    /// carrying the discard accounting the flush settles against the
    /// publish + commit results. `raw` builds the bytes only then — see
    /// [`DroppedMessage`].
    fn drop_message(
        &mut self,
        coordinates: (i32, i64),
        raw: impl FnOnce() -> RawMessage,
        reason: String,
        pending: metrics::PendingDiscard,
    ) {
        let dlq = self.retain_raw.then(|| PendingDlq { raw: raw(), reason });
        self.dropped.push(DroppedMessage {
            coordinates,
            dlq,
            pending,
        });
    }

    /// The decoded messages, for the handler. `messages` starts over with
    /// its full reservation.
    fn take_messages(&mut self) -> Vec<(T::Message, MessageMetadata)> {
        std::mem::replace(&mut self.messages, Vec::with_capacity(self.capacity))
    }

    /// Retires the span in `tracker`, each partition from its start up to
    /// its offset in `through`, see `OffsetTracker::retire_span`.
    fn retire(&self, tracker: &mut OffsetTracker, through: &HashMap<i32, i64>, committed: bool) {
        for (&partition, &start) in &self.start {
            if let (Some(&end), Some(&epoch)) =
                (through.get(&partition), self.epochs.get(&partition))
            {
                tracker.retire_span(partition, epoch, start, end, committed);
            }
        }
    }

    /// Drains every field, settling any not-yet-settled dropped message as
    /// `survived` — the batch is being abandoned (redelivered, or the
    /// consumer is shutting down mid-batch), so nothing was actually lost.
    /// This makes every abandon path (a `Retry` or `Defer`, a rebalance via
    /// `apply_batch_rebalance`, a seek failure) correct with zero call-site
    /// changes; [`settle_terminal_span`] empties this vec first, settling
    /// each drop itself, so this drain finds nothing left to do.
    fn clear(&mut self) {
        self.messages.clear();
        self.raw.clear();
        self.coordinates.clear();
        for dropped in self.dropped.drain(..) {
            dropped.pending.survived();
        }
        self.start.clear();
        self.end.clear();
        self.epochs.clear();
    }
}

/// The most dead letters one flush has in flight at once, see
/// [`dead_letter_span`].
const BATCH_DLQ_CONCURRENCY: u32 = 16;

/// One record a flush dead-letters: its wire bytes, which name its
/// partition and offset, and the death reason.
struct DeadLetter {
    raw: RawMessage,
    reason: Cow<'static, str>,
}

/// How one dead letter of a flush ended, see [`dead_letter_span`].
#[derive(Debug)]
enum DeadLettered {
    /// It is in the DLQ.
    Landed,
    /// It can never be published, refused as too large or as invalid: the
    /// record is discarded as lost.
    Unpublishable,
    /// It failed with an error no retry gets past; the member ends with it.
    Fatal(ShoveError),
    /// It failed and may land later, was never tried because the member
    /// ends or a dead letter before it on its partition failed so, or its
    /// task ended without an answer: nothing is committed past the record.
    Kept,
}

/// What a dead letter's publish answered, logged and read as a
/// [`DeadLettered`].
fn dead_lettered(
    queue: &str,
    (partition, offset): (i32, i64),
    published: Result<()>,
) -> DeadLettered {
    let e = match published {
        Ok(()) => return DeadLettered::Landed,
        Err(e) => e,
    };
    if is_unpublishable(&e) {
        tracing::error!(
            error = %e,
            queue,
            partition,
            offset,
            "the dead letter can never be published; the record is discarded"
        );
        return DeadLettered::Unpublishable;
    }
    if !e.is_retryable() {
        tracing::error!(
            error = %e,
            queue,
            partition,
            offset,
            "dead-letter publish failed with an error no retry gets past; the record stays \
             uncommitted and the consumer ends"
        );
        return DeadLettered::Fatal(e);
    }
    tracing::error!(
        error = %e,
        queue,
        partition,
        offset,
        "dead-letter publish failed; nothing is committed past the record, and its partition \
         is sought back to it"
    );
    DeadLettered::Kept
}

/// Waits for `permits` permits of a flush's dead-letter semaphore through
/// [`acquire_permit_while_polling`], which polls once its grace has run out.
/// A stop does not end the wait: a flush that has begun is seen through, so
/// once the stop has fired the wait goes on as the drain at a stop does,
/// riding out every transport failure.
async fn dead_letter_permits(
    flush: &BatchFlushCtx<'_>,
    state: &mut BatchState,
    semaphore: &Arc<Semaphore>,
    permits: u32,
) -> Result<tokio::sync::OwnedSemaphorePermit> {
    loop {
        let stop = (!flush.shutdown.is_cancelled()).then_some(flush.shutdown);
        let waited = acquire_permit_while_polling(
            semaphore,
            permits,
            flush.consumer,
            stop,
            None,
            flush.queue,
            &mut state.paused,
            None,
        )
        .await?;
        if let Some(permit) = waited {
            return Ok(permit);
        }
    }
}

/// Publishes `letters` to the DLQ, at most [`BATCH_DLQ_CONCURRENCY`] at a
/// time, and returns how each one ended, in order.
///
/// Each publish runs on a task of its own that holds one of the
/// semaphore's permits, and the loop waits for a free permit, and once every
/// letter is out for all of them, through [`dead_letter_permits`]. A slow
/// DLQ therefore costs one publish's latency per round of letters, and the
/// waits poll, so dead letters that outlast `max.poll.interval.ms` do not
/// have the member evicted. A record a wait is handed is put back with the
/// assignment paused, which the flush lifts once the letters are done, see
/// [`BatchState::resume`]; the rebalance events the polls serve are drained
/// then too, see [`settle_terminal_span`].
///
/// A dead letter that fails and may land later keeps its record. The letters
/// behind it on its partition are not tried, since nothing is committed past
/// the first, and those already out are seen through. One that fails with an
/// error no retry gets past launches no more.
async fn dead_letter_span(
    flush: &BatchFlushCtx<'_>,
    state: &mut BatchState,
    letters: Vec<DeadLetter>,
) -> Result<Vec<DeadLettered>> {
    let mut run = DeadLetterRun::new(&letters);
    let semaphore = Arc::new(Semaphore::new(BATCH_DLQ_CONCURRENCY as usize));
    let mut publishes: tokio::task::JoinSet<(usize, Result<()>)> = tokio::task::JoinSet::new();
    for (index, letter) in letters.into_iter().enumerate() {
        let partition = letter.raw.headers.partition;
        if run.skips(partition) {
            continue;
        }
        let permit = dead_letter_permits(flush, state, &semaphore, 1).await?;
        while let Some(done) = publishes.try_join_next() {
            run.record(flush.queue, done);
        }
        if run.skips(partition) {
            continue;
        }
        let client = flush.client.clone();
        let topology = flush.topology;
        publishes.spawn(async move {
            let DeadLetter { raw, reason } = letter;
            let published = publish_to_dlq(
                &client,
                topology,
                &raw.payload,
                raw.key.as_deref(),
                &raw.headers,
                &reason,
                get_retry_count(&raw.headers.strings),
            )
            .await;
            drop(permit);
            (index, published)
        });
    }
    // Every permit back is every publish over.
    drop(dead_letter_permits(flush, state, &semaphore, BATCH_DLQ_CONCURRENCY).await?);
    while let Some(done) = publishes.join_next().await {
        run.record(flush.queue, done);
    }
    Ok(run.outcomes)
}

/// The dead letters of one flush as their publishes come back, see
/// [`dead_letter_span`].
struct DeadLetterRun {
    /// Each letter's partition and offset, index-parallel to `outcomes`.
    coordinates: Vec<(i32, i64)>,
    /// `Kept` until the letter's publish answers.
    outcomes: Vec<DeadLettered>,
    /// Partitions with a letter kept: nothing behind it is tried.
    kept: BTreeSet<i32>,
    /// A letter failed with an error no retry gets past: nothing more is
    /// tried.
    ending: bool,
}

impl DeadLetterRun {
    fn new(letters: &[DeadLetter]) -> Self {
        Self {
            coordinates: letters
                .iter()
                .map(|letter| (letter.raw.headers.partition, letter.raw.headers.offset))
                .collect(),
            outcomes: std::iter::repeat_with(|| DeadLettered::Kept)
                .take(letters.len())
                .collect(),
            kept: BTreeSet::new(),
            ending: false,
        }
    }

    /// Whether a letter on `partition` is left untried.
    fn skips(&self, partition: i32) -> bool {
        self.ending || self.kept.contains(&partition)
    }

    /// Reads one finished publish. A task that ended without an answer
    /// leaves its letter `Kept`.
    fn record(
        &mut self,
        queue: &str,
        done: std::result::Result<(usize, Result<()>), tokio::task::JoinError>,
    ) {
        let (index, published) = match done {
            Ok(done) => done,
            Err(e) => {
                tracing::error!(
                    queue,
                    error = %e,
                    "a dead-letter task ended without an answer; its record stays uncommitted"
                );
                return;
            }
        };
        let (Some(&coordinate), Some(slot)) =
            (self.coordinates.get(index), self.outcomes.get_mut(index))
        else {
            return;
        };
        let outcome = dead_lettered(queue, coordinate, published);
        match &outcome {
            DeadLettered::Kept => {
                self.kept.insert(coordinate.0);
            }
            DeadLettered::Fatal(_) => self.ending = true,
            DeadLettered::Landed | DeadLettered::Unpublishable => {}
        }
        *slot = outcome;
    }
}

/// The discard a terminally settled record owes, see [`reject_settlement`]:
/// `None` when it is in the DLQ, settled at once, and otherwise the
/// discard that rides the commit retiring it.
fn terminal_discard(
    has_dlq: bool,
    reached_dlq: bool,
    pending: metrics::PendingDiscard,
) -> Option<TerminalDiscard> {
    match reject_settlement(has_dlq, reached_dlq) {
        RejectSettlement::InDlq => {
            pending.survived();
            None
        }
        RejectSettlement::Retired => Some(TerminalDiscard::Retired(pending)),
        RejectSettlement::Lost => Some(TerminalDiscard::Lost(pending)),
    }
}

/// `discard`, riding the commit of the span whose partitions `epochs`
/// names at `offset` on `partition`, see [`CoveredDiscard`]. A partition the
/// span does not hold has no commit to ride: its discard settles as
/// survived, and nothing is counted.
fn ride(
    epochs: &HashMap<i32, u64>,
    (partition, offset): (i32, i64),
    discard: TerminalDiscard,
) -> Option<CoveredDiscard> {
    match epochs.get(&partition) {
        Some(&epoch) => Some(CoveredDiscard {
            partition,
            offset,
            epoch,
            discard,
        }),
        None => {
            discard.survived();
            None
        }
    }
}

/// Settles a span whose records leave through the DLQ or are discarded:
/// the `rejected` messages a `Reject` dead-letters, none for a span the
/// handler acked, and every drop parked during the batch.
///
/// The failure of each rejected message is counted now, because it already
/// happened. Its *discard* is a claim that the message no longer exists, and
/// nothing has retired these messages until the commit below lands:
/// recording it up front would fire a data-loss alert during the broker
/// outage that made the commit fail, precisely when the batch is about to be
/// redelivered. With no DLQ the buffer keeps no wire bytes at all, but there
/// are still `rejected` messages about to be dropped on the floor, which is
/// exactly what the discard counter exists to report: each discard rides
/// the commit at its own record, see [`CoveredDiscard`].
///
/// The dead letters run concurrently and the loop keeps polling, see
/// [`dead_letter_span`]. A rebalance their polls served that moved a
/// partition of the span abandons it uncommitted, as a rebalance between
/// flushes does. Otherwise the span is settled and commits synchronously,
/// see [`commit_terminal_span`], each partition up to its first record whose
/// dead letter failed and may land later, or to its end; a dead letter that
/// can never be published is discarded as lost and committed past. The
/// discards riding the commit count once the coordinator accepts it. A
/// partition the commit stops short of is sought back to the record that
/// failed, so the batch is handed over again from it after the escalating
/// redelivery delay, and its dead letter tried again; no discard is counted
/// for a record kept so. A dead letter that failed with an error no retry
/// gets past commits nothing and ends the member with that error.
///
/// A commit the coordinator rejects, such as one made while the group
/// rebalances, neither ends the member nor hands the span over again. The
/// span is settled, so it is retired as an acked span is, its discards go
/// back to the tracker, and the slot re-offers its position: a refusal
/// costs a re-offer, with no redelivery and no second dead letter. The
/// rejection feeds the fence, see `OffsetTracker::commit_rejected`, so a
/// member whose commits keep being rejected with no rebalance resolving
/// them reconnects through it.
async fn settle_terminal_span<T: Topic>(
    flush: &BatchFlushCtx<'_>,
    buffer: &mut BatchBuffer<T>,
    state: &mut BatchState,
    redelivery_backoff: &mut Backoff,
    rejected: usize,
) -> Result<Flushed> {
    let queue = flush.queue;
    let has_dlq = buffer.retain_raw;
    let mut letters: Vec<DeadLetter> = Vec::new();
    let mut letter_pendings: Vec<metrics::PendingDiscard> = Vec::new();
    let mut riding: Vec<CoveredDiscard> = Vec::new();
    if rejected > 0 {
        if has_dlq {
            for raw in buffer.raw.drain(..) {
                letter_pendings.push(metrics::record_terminal(
                    flush.topic,
                    flush.group,
                    metrics::FailReason::Rejected,
                    true,
                ));
                letters.push(DeadLetter {
                    raw,
                    reason: Cow::Borrowed("rejected"),
                });
            }
            tracing::warn!(
                queue,
                batch_size = rejected,
                "batch rejected, routed to the DLQ"
            );
        } else {
            for &coordinates in &buffer.coordinates {
                let pending = metrics::record_terminal(
                    flush.topic,
                    flush.group,
                    metrics::FailReason::Rejected,
                    false,
                );
                if let Some(discard) = terminal_discard(false, false, pending) {
                    riding.extend(ride(&buffer.epochs, coordinates, discard));
                }
            }
            tracing::warn!(
                queue,
                batch_size = rejected,
                "batch rejected but no DLQ is configured, the messages are discarded"
            );
        }
    }
    // Pre-handler drops parked in this same batch settle on the same commit
    // as the rejected messages above — one commit, one decision for both
    // populations.
    for dropped in buffer.dropped.drain(..) {
        match dropped.dlq {
            Some(PendingDlq { raw, reason }) => {
                letter_pendings.push(dropped.pending);
                letters.push(DeadLetter {
                    raw,
                    reason: Cow::Owned(reason),
                });
            }
            None => {
                if let Some(discard) = terminal_discard(false, false, dropped.pending) {
                    riding.extend(ride(&buffer.epochs, dropped.coordinates, discard));
                }
            }
        }
    }

    let coordinates: Vec<(i32, i64)> = letters
        .iter()
        .map(|letter| (letter.raw.headers.partition, letter.raw.headers.offset))
        .collect();
    let outcomes = if letters.is_empty() {
        Vec::new()
    } else {
        dead_letter_span(flush, state, letters).await?
    };
    state.resume(flush.consumer)?;

    // Each partition commits up to its first record kept, or to its end.
    let mut commit_to = buffer.end.clone();
    let mut fatal: Option<ShoveError> = None;
    for ((coordinates, pending), outcome) in
        coordinates.into_iter().zip(letter_pendings).zip(outcomes)
    {
        match outcome {
            DeadLettered::Landed => {
                if let Some(discard) = terminal_discard(true, true, pending) {
                    riding.extend(ride(&buffer.epochs, coordinates, discard));
                }
            }
            DeadLettered::Unpublishable => {
                if let Some(discard) = terminal_discard(true, false, pending) {
                    riding.extend(ride(&buffer.epochs, coordinates, discard));
                }
            }
            DeadLettered::Fatal(e) => {
                pending.survived();
                fatal.get_or_insert(e);
            }
            DeadLettered::Kept => {
                pending.survived();
                let (partition, offset) = coordinates;
                if let Some(end) = commit_to.get_mut(&partition) {
                    *end = (*end).min(offset);
                }
            }
        }
    }
    if let Some(e) = fatal {
        riding.into_iter().for_each(CoveredDiscard::survived);
        buffer.clear();
        return Err(e);
    }
    if let Some(rewound) = apply_batch_rebalance(flush.consumer, queue, buffer, state)? {
        riding.into_iter().for_each(CoveredDiscard::survived);
        return Ok(Flushed::Redelivered(rewound));
    }

    // The span is settled: its commit carries each partition's position and
    // the discards below it, and a discard at or past a kept record waits
    // for that record's redelivery.
    let positions: Vec<CommitPosition> = commit_to
        .iter()
        .filter_map(|(&partition, &offset)| {
            buffer.epochs.get(&partition).map(|&epoch| CommitPosition {
                partition,
                offset,
                epoch,
            })
        })
        .collect();
    let mut covered = Vec::with_capacity(riding.len());
    for discard in riding {
        if commit_to
            .get(&discard.partition)
            .is_some_and(|&end| discard.offset < end)
        {
            covered.push(discard);
        } else {
            discard.survived();
        }
    }
    let accepted = commit_terminal_span(flush, state, positions, covered).await?;
    state.resume(flush.consumer)?;
    buffer.retire(&mut state.tracker, &commit_to, accepted);
    let kept: HashMap<i32, i64> = commit_to
        .iter()
        .filter(|&(partition, &to)| buffer.end.get(partition).is_some_and(|&end| to < end))
        .map(|(&partition, &to)| (partition, to))
        .collect();
    if kept.is_empty() {
        *redelivery_backoff = batch_redelivery_backoff();
        if accepted {
            tracing::debug!(queue, "batch settled and its offsets committed");
        } else {
            tracing::debug!(
                queue,
                "batch settled; its offsets go out with the next commit"
            );
        }
        buffer.clear();
        return Ok(Flushed::Committed);
    }
    let delay = next_redelivery_delay(redelivery_backoff);
    let rewound: BTreeSet<i32> = kept.keys().copied().collect();
    tracing::warn!(
        queue,
        partitions = ?rewound,
        ?delay,
        "a dead letter of the batch failed; nothing is committed past it, and its partition is \
         sought back to it and handed over again"
    );
    let sought = rewind_span(flush.consumer, queue, &kept);
    buffer.clear();
    sought?;
    tokio::select! {
        () = tokio::time::sleep(delay) => {}
        () = flush.shutdown.cancelled() => {}
    }
    Ok(Flushed::Redelivered(rewound))
}

/// Hands the buffered batch to the handler and applies the single returned
/// `Outcome`, via the shared [`settle_batch_outcome`] classifier:
///
/// - `Ack` (`BatchSettlement::Commit`) retires every partition's span in the
///   tracker, and the commit the loop makes through [`CommitSlot`] carries
///   it, see [`BatchState`]. A span with drops parked in it settles as
///   [`settle_terminal_span`] settles it instead.
/// - `Reject` (`BatchSettlement::DeadLetter`) is terminal, exactly as it is on
///   every other consumer path: the batch is published to the DLQ and its
///   offsets commit, see [`settle_terminal_span`].
/// - `Retry` / `Defer` (`BatchSettlement::Redeliver`) seek every partition
///   back to the batch's start offset so the whole batch is re-delivered on
///   the next `recv()` instead of being silently skipped (offsets were never
///   committed, but this consumer keeps polling forward without a seek).
///
/// The rebalance events are drained first, on every arm that flushes: a
/// revoke the loop has not read yet abandons the batch before its records
/// reach the handler, see [`apply_batch_rebalance`].
///
/// The buffer's messages may be empty while its offset span is not: every
/// message in the span was dropped before the handler (oversize /
/// undeserializable). There is nothing to hand over, but those offsets still
/// get committed: leaving them uncommitted replays the same poison on every
/// restart, and a poll window that is *entirely* poison would never commit
/// anything at all.
///
/// Anything parked for the DLQ during the batch is published when the batch
/// settles, and dropped un-published when the batch is redelivered instead
/// (it will be re-parked on the way back through). `redelivery_backoff`
/// escalates across consecutive redeliveries and is reset once a batch
/// settles.
///
/// The buffer is left empty on every path; the caller only has to disarm the
/// deadline. The returned [`Flushed`] says which partitions were rewound,
/// because a caller holding a record that arrived after the span must not
/// buffer it ahead of a rewound span.
///
/// The handler itself is invoked through [`invoke_batch_handler`] —
/// shared batching machinery, not Kafka-specific — which supplies the
/// panic containment, timeout and
/// instrumentation this flush relies on. See that function's doc for why the
/// guard exists and why the handler future is built *inside* it.
async fn flush_batch<T, H>(
    flush: &BatchFlushCtx<'_>,
    handler: &H,
    ctx: &H::Context,
    buffer: &mut BatchBuffer<T>,
    redelivery_backoff: &mut Backoff,
    state: &mut BatchState,
) -> Result<Flushed>
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
        handler_timeout,
        // Not `Copy` like the rest, so bind it by reference and leave the
        // by-value destructure of the other fields intact.
        ref handler_timeout_outcome,
        ..
    } = *flush;
    if let Some(rewound) = apply_batch_rebalance(consumer, queue, buffer, state)? {
        return Ok(Flushed::Redelivered(rewound));
    }
    if buffer.is_empty() {
        return Ok(Flushed::Committed);
    }
    let batch_size = buffer.messages.len();
    if batch_size == 0 {
        return settle_terminal_span(flush, buffer, state, redelivery_backoff, 0).await;
    }

    let messages = buffer.take_messages();
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
    // The *selection* of which arm runs is the shared classifier; the
    // mechanics inside each arm (offset commits, seeks, DLQ publishes) are
    // Kafka's own.
    match settle_batch_outcome(&outcome) {
        BatchSettlement::Commit if buffer.dropped.is_empty() => {
            buffer.retire(&mut state.tracker, &buffer.end, false);
            *redelivery_backoff = batch_redelivery_backoff();
            tracing::debug!(
                queue,
                batch_size,
                "batch acked; its offsets go out with the next commit"
            );
            buffer.clear();
            Ok(Flushed::Committed)
        }
        BatchSettlement::Commit => {
            settle_terminal_span(flush, buffer, state, redelivery_backoff, 0).await
        }
        // `Reject` means terminal-do-not-retry everywhere else in shove, and
        // it has to mean that here too: routing it through the seek-back arm
        // pins the partition forever (seek, sleep, redeliver, reject, …) with
        // the payloads never reaching a DLQ and the offsets never advancing.
        BatchSettlement::DeadLetter => {
            settle_terminal_span(flush, buffer, state, redelivery_backoff, batch_size).await
        }
        BatchSettlement::Redeliver => {
            let delay = next_redelivery_delay(redelivery_backoff);
            tracing::warn!(
                queue,
                batch_size,
                ?outcome,
                ?delay,
                "batch handler returned a non-Ack outcome, redelivering the whole batch"
            );
            let rewound: BTreeSet<i32> = buffer.start.keys().copied().collect();
            let sought = rewind_span(consumer, queue, &buffer.start);
            buffer.clear();
            sought?;
            // There is no per-batch retry counter at this level (see the
            // type's doc comment), so a handler stuck returning `Retry`
            // redelivers the same batch forever. The escalating delay bounds
            // the spin; `messages_consumed_total{outcome="retry"}` climbing
            // with no matching `outcome="ack"` is the alertable signal that it
            // is wedged. Shutdown cuts the delay short — otherwise a wedged
            // handler would add up to `BATCH_REDELIVERY_BACKOFF_MAX` to every
            // stop.
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = shutdown.cancelled() => {}
            }
            Ok(Flushed::Redelivered(rewound))
        }
    }
}

/// How [`flush_batch`] left the span it flushed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Flushed {
    /// The span's records are retired: its offsets were committed with the
    /// flush, or go out with the loop's next commit, as an acked span's do
    /// and as a refused commit's re-offer does.
    Committed,
    /// These partitions of the span were sought back, to the span's start or
    /// to a record whose dead letter failed, or the span was abandoned to a
    /// rebalance that moved one of them. Their records are delivered again,
    /// in offset order, on the next polls, or to the partition's next owner.
    Redelivered(BTreeSet<i32>),
}

/// What one drain of the batch loop's rebalance events found, see
/// [`drain_batch_rebalance_events`].
#[derive(Debug, Default, PartialEq, Eq)]
struct BatchRebalance {
    /// The partitions of this consumer's topic whose ownership changed.
    moved: BTreeSet<i32>,
    /// Whether an assign was among the events, see
    /// [`reconcile_pause_after_assign`].
    assigned: bool,
}

/// Drains librdkafka's rebalance events, returning the partitions of this
/// consumer's topic whose ownership changed, and whether an assign was among
/// them.
///
/// Only a partition the in-flight batch actually spans invalidates it, so the
/// caller intersects this set with the batch (see
/// [`rebalance_affects_batch`]) instead of abandoning on any event at all.
/// Under `cooperative-sticky` the callback fires on *every* member each round —
/// including members that gain a partition and lose nothing, and with an empty
/// delta — so treating a bare "something happened" as invalidation throws away
/// batches that were never at risk, redelivering them as duplicate writes.
fn drain_batch_rebalance_events(rx: &std_mpsc::Receiver<RebalanceEvent>) -> BatchRebalance {
    let mut drained = BatchRebalance::default();
    while let Ok(event) = rx.try_recv() {
        let partitions = match event {
            RebalanceEvent::Assign(partitions) => {
                drained.assigned = true;
                partitions
            }
            RebalanceEvent::Revoke(partitions) => partitions,
        };
        drained.moved.extend(partitions);
    }
    drained
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

/// Seeks every partition in `targets` back to its offset there, best-effort
/// and independently per partition: the rewind of a span abandoned to a
/// rebalance, of a span a `Retry` or `Defer` redelivers, and of a partition
/// whose dead letter failed, see [`settle_terminal_span`].
///
/// This consumer runs `partition.assignment.strategy=cooperative-sticky`, so a
/// rebalance revokes only *some* partitions and leaves the rest assigned with
/// their fetch position untouched. Dropping a batch without rewinding would
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
/// partition from its last committed offset, which is at or before its
/// target, so the batch comes back instead of vanishing. Which failures are
/// losses is [`seek_failure_is_loss`]'s call.
fn rewind_span(
    consumer: &KafkaStreamConsumer,
    queue: &str,
    targets: &HashMap<i32, i64>,
) -> Result<()> {
    let still_assigned = assigned_partitions(consumer, queue);
    let may_be_held = |partition: i32| seek_failure_is_loss(still_assigned.as_ref(), partition);
    let mut lost = Vec::new();

    for (&partition, &offset) in targets {
        let mut tpl = TopicPartitionList::new();
        // A partition missing from the list is one that never gets sought, so
        // this counts as a failed rewind exactly like a failed seek does.
        if let Err(e) = tpl.add_partition_offset(queue, partition, Offset::Offset(offset)) {
            tracing::warn!(
                error = %e,
                queue,
                partition,
                offset,
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
                tracing::debug!(error = %e, queue, partition, offset, "rewind seek failed");
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
                    offset,
                    "rewind failed on a revoked partition; the member taking it over resumes \
                     from the last committed offset"
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
        "rewind failed on partitions this member may still hold; reconnecting so their \
         consumed-but-uncommitted messages are redelivered"
    );
    Err(ShoveError::Connection(format!(
        "batch rewind failed on {queue} partitions {lost:?}; reconnecting to avoid skipping \
         their uncommitted messages"
    )))
}

/// Whether a failed seek on `partition` may have lost this member's position.
///
/// A seek fails harmlessly on a partition the group revoked in the same
/// poll: the member taking it over resumes from the last committed offset,
/// which is where a put-back or a rewind would have pointed anyway. It fails
/// harmfully on a partition this member still holds, whose consumed position
/// then stays past the record. `None` means librdkafka would not report the
/// assignment, and the two cases cannot be told apart, so the failure counts
/// as a loss: a needless reconnect costs a rejoin, guessing the other way
/// costs data.
fn seek_failure_is_loss(still_assigned: Option<&BTreeSet<i32>>, partition: i32) -> bool {
    still_assigned.is_none_or(|assigned| assigned.contains(&partition))
}

/// Settle the result of a put-back seek on `partition` at `offset`.
///
/// `errors` are the per-partition errors of the seek response. An empty list
/// is a clean seek. An error on a partition that `seek_failure_is_loss` calls
/// a loss keeps the `Connection` error: the loop reconnects and resumes from
/// the last committed offset, which redelivers the record. An error on a
/// partition revoked in the same poll is settled as `Ok(())`, because the
/// member taking the partition over resumes from that same committed offset
/// and a reconnect here would only cost this member a rejoin.
fn settle_put_back_seek(
    queue: &str,
    partition: i32,
    offset: i64,
    errors: Vec<(i32, KafkaError)>,
    still_assigned: Option<&BTreeSet<i32>>,
) -> Result<()> {
    let Some((_, e)) = errors.into_iter().next() else {
        return Ok(());
    };
    if !seek_failure_is_loss(still_assigned, partition) {
        tracing::debug!(
            error = %e,
            queue,
            partition,
            offset,
            "put-back seek failed on a partition revoked in the same poll; the member taking \
             it over resumes from the last committed offset"
        );
        return Ok(());
    }
    Err(ShoveError::Connection(format!(
        "put-back seek failed on {queue} partition {partition} at offset {offset}: {e}"
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

/// Hands a delivered but unprocessed record back to the broker side: seeks
/// its partition to the record's own offset, so the next fetch delivers it,
/// and everything the partition holds after it, again in order.
///
/// This is how a loop declines a record it cannot take right now without
/// skipping it: the record was never tracked or buffered, and the seek
/// resets librdkafka's consumed position (see `rd_kafka_toppar_seek`), so a
/// pause that follows keeps the rewound position rather than the one past
/// the record. A seek that does not take is an error, not a warning: the
/// partition would otherwise keep its advanced position and the record
/// would be skipped for good. The `Connection` error makes the loop
/// reconnect and resume from the last committed offset, which redelivers it.
fn put_back(
    consumer: &KafkaStreamConsumer,
    queue: &str,
    partition: i32,
    offset: i64,
) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(queue, partition, Offset::Offset(offset))
        .map_err(|e| map_kafka_error("put-back offset list rejected the partition", e))?;
    let sought = consumer
        .seek_partitions(tpl, SEEK_TIMEOUT)
        .map_err(|e| map_kafka_error("put-back seek failed", e))?;
    let errors = seek_errors(&sought);
    if errors.is_empty() {
        return Ok(());
    }
    let still_assigned = assigned_partitions(consumer, queue);
    settle_put_back_seek(queue, partition, offset, errors, still_assigned.as_ref())
}

/// What the receive loop does with a record it holds once a wait it polled
/// `recv()` through has ended: the wait for a prefetch permit, or a schema
/// registry stall. The stop and the record's assignment are read again then,
/// with the wait over, because either can have moved during it and the
/// record must not reach a handler after that. A stop from another thread
/// can land between the wait's poll of the token and its poll of the permit,
/// and the permit it then frees, from a cancelled in-place wait, is the one
/// just taken. A revoke of the record's partition is applied inside the
/// `recv()` poll, which cancels the token, and the record is the next
/// owner's. `revoked` is `None` on the broadcast loop, which has no
/// assignment to lose.
#[derive(Debug, PartialEq, Eq)]
enum AfterWait {
    /// Neither moved: the record goes on to its handler.
    Dispatch,
    /// Shutdown fired. The record is left uncommitted for the restart, which
    /// delivers it behind the record that was waiting.
    Stop,
    /// The record's partition was revoked. The record is left to its next
    /// owner, which is handed it from the committed offset.
    Revoked,
}

fn after_wait(shutdown: &CancellationToken, revoked: Option<&CancellationToken>) -> AfterWait {
    if shutdown.is_cancelled() {
        AfterWait::Stop
    } else if revoked.is_some_and(CancellationToken::is_cancelled) {
        AfterWait::Revoked
    } else {
        AfterWait::Dispatch
    }
}

/// The fault a handler task reported over the receive loop's channel, or a
/// future that never resolves: a loop without such a channel passes `None`,
/// and a channel whose senders are all gone has nothing more to report.
async fn handler_fault(rx: Option<&mut mpsc::Receiver<ShoveError>>) -> ShoveError {
    match rx {
        Some(rx) => match rx.recv().await {
            Some(e) => e,
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
}

/// The error that ends a member whose offset commits keep being rejected,
/// or keep finding no thread to carry them, with no rebalance resolving
/// them, see `OffsetTracker::fenced`. It is counted and logged here,
/// because the reconnect it forces is the only other trace of it.
fn fenced_error(
    queue: &str,
    group_id: &str,
    partition: i32,
    fence_timeout: Duration,
) -> ShoveError {
    #[cfg(feature = "test-support")]
    fence_probe::record_fired();
    metrics::record_backend_error(
        metrics::BackendLabel::Kafka,
        metrics::BackendErrorKind::Connection,
    );
    tracing::error!(
        queue,
        group_id,
        partition,
        stuck_for = ?fence_timeout,
        "consumer appears fenced from its group (offset commits rejected or not issued, \
         with no resolving rebalance); forcing a clean reconnect"
    );
    ShoveError::Connection(format!(
        "consumer on '{queue}' appears fenced from group '{group_id}': \
         partition {partition} has had offset commits rejected or not issued for \
         over {fence_timeout:?} with no resolving rebalance"
    ))
}

/// What the receive loop's shutdown arm hands the tail once its handler
/// tasks have drained: what [`stop_with_final_commit`] reads.
struct Stopped {
    tracker: OffsetTracker,
    rebalance_rx: std_mpsc::Receiver<RebalanceEvent>,
    pending_commit: Option<PendingCommit>,
}

/// The receive loop's offset bookkeeping under `CommitPolicy::Interval`,
/// lent to [`acquire_permit_while_polling`] so the wait does on every pass
/// what a pass of the loop does: it takes in the completions, then runs the
/// commit step, see [`CommitSlot::pass`].
struct IntervalCommits<'a> {
    tracker: &'a mut OffsetTracker,
    completion_rx: &'a mut mpsc::Receiver<Completion>,
    // `&mut`, because the receiver is not `Sync` and a shared borrow held
    // across the wait's awaits would make the future `!Send`.
    rebalance_rx: &'a mut std_mpsc::Receiver<RebalanceEvent>,
    slot: &'a mut CommitSlot,
    fence: Fence<'a>,
}

impl IntervalCommits<'_> {
    /// The `Ok` says whether the commit step's rebalance events held an
    /// assign, see [`CommitSlot::pass`].
    fn settle(&mut self, consumer: &Arc<KafkaStreamConsumer>, queue: &str) -> Result<bool> {
        while let Ok(completion) = self.completion_rx.try_recv() {
            self.tracker.mark_complete(completion);
        }
        self.slot
            .pass(consumer, self.tracker, self.rebalance_rx, self.fence, queue)
    }
}

/// How long a record waits for a prefetch permit before
/// [`acquire_permit_while_polling`] starts calling `recv()`, and how long a
/// batch flush waits for a commit's answer before it does, see
/// [`answer_while_polling`].
///
/// A polled `recv()` hands over the next fetched record at once, and putting
/// that record back costs a seek, a pause and a refetch that librdkafka
/// backs off before. A member on one permit draining a backlog would pay it
/// for every second record, which throttles it to a few dozen records a
/// second. Inside the grace a permit that frees takes the record in hand
/// with nothing put back. A wait that outlasts it polls from then on, far
/// inside the 10 s floor of `max.poll.interval.ms`.
const PERMIT_WAIT_GRACE: Duration = Duration::from_secs(1);

/// Wait for `permits` prefetch permits while the receive loop keeps polling.
///
/// The receive loop waits here for one permit when a record is in hand,
/// decoded and tracked, and every permit is held: by running handlers, by
/// handlers waiting out an in-place delay, or by republishes waiting out
/// their hold. Its shutdown arm waits here for every permit, the proof that
/// every handler task has ended, see the drain there. A batch flush waits
/// here for the permits its dead letters hold, see [`dead_letter_span`],
/// and makes no commit while it waits: a position acked before the flush,
/// and a re-offer of a rejected one, wait for the flush to end. librdkafka
/// counts the member as polling only when a `recv()` actually polls its queue
/// (`rd_kafka_app_polled`, bundled librdkafka 2.12.1, `rdkafka_int.h:1219`;
/// rust-rdkafka 0.39 polls it with a zero timeout,
/// `stream_consumer.rs:125-131`). A bare `acquire_owned().await` here would
/// poll nothing, so a hold, or a running holder that turns into an in-place
/// wait afterwards, would park the loop for the whole delay, and a park past
/// `max.poll.interval.ms` has the member evicted; a park in the drain would
/// also leave a rebalance that needs this member's revoke unfinished until
/// the eviction. So the permits are acquired inside a `select!` whose
/// `recv()` arm is armed once [`PERMIT_WAIT_GRACE`] has run out. From then
/// on the member stays inside the interval because that arm is polled again
/// on every wake of the wait: the housekeeping tick, the commit gate's
/// deadline and the answer of the commit in flight under
/// `CommitPolicy::Interval`, and the wake rust-rdkafka sends a pending
/// `recv()` at half the poll interval (`stream_consumer.rs:238-263`). Those
/// polls serve the rebalance callbacks. A record `recv()` yields meanwhile
/// comes from a partition that is not paused; it is put back (see
/// [`put_back`]) and the assignment is paused, so it arrives again, in
/// order, once a permit frees and the loop resumes, or, after a stop, once
/// the group hands it out again from the committed offset. A `recv()` error
/// the table rides out is logged and the wait goes on, see
/// [`recv_error_is_transient`].
///
/// `commits`, under `CommitPolicy::Interval`, has the wait commit as the
/// loop does: positions completed before the wait, and the ones a rebalance
/// or a rejected commit re-offers during it, are committed however long the
/// wait runs, and the record in hand stays in flight, so no commit passes
/// it. The wait wakes for that on the gate's deadline, on the answer of the
/// commit in flight and on the housekeeping tick, as the loop does, and an
/// assign among the events that step drains re-applies the loop's pause
/// intent, see [`reconcile_pause_after_assign`]. Under
/// `CommitPolicy::PerRecord` the loop holds its permit whenever it takes a
/// record, so the record wait only ever finds a free one, and commits
/// nothing. The drain commits nothing either: the stop commits what the
/// handlers completed once the last permit is in.
///
/// `Ok(None)` means `shutdown` fired first: it is read before the permits,
/// and the caller reads it once more with the permit in hand, see the
/// receive arm. A fault a handler task reports over `fault_rx` meanwhile
/// ends the wait with that error, for the reason `RegistryStall::wait`
/// gives: the loop's fault arm is not polled while this wait runs, and a
/// permit may never free if the holder is the task that faulted. The drain
/// passes neither: it runs once the stop has fired, and a fault reported
/// during it leaves its record uncommitted, as the stop leaves every record
/// whose handler did not complete. A stop never reconnects, so the drain
/// reads no gate and rides out every `__TRANSPORT` and `__RESOLVE`, see
/// [`recv_error_is_transient`].
#[allow(clippy::too_many_arguments)]
async fn acquire_permit_while_polling(
    semaphore: &Arc<tokio::sync::Semaphore>,
    permits: u32,
    consumer: &Arc<KafkaStreamConsumer>,
    shutdown: Option<&CancellationToken>,
    fault_rx: Option<&mut mpsc::Receiver<ShoveError>>,
    queue: &str,
    paused: &mut bool,
    mut commits: Option<IntervalCommits<'_>>,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>> {
    // No token means the drain, whose stop has fired.
    let gate = shutdown.map(|_| consumer.transport_gate());
    let stop = stop_requested(shutdown);
    tokio::pin!(stop);
    let fault = handler_fault(fault_rx);
    tokio::pin!(fault);
    let grace = tokio::time::sleep(PERMIT_WAIT_GRACE);
    tokio::pin!(grace);
    let mut polling = false;
    let mut housekeeping = tokio::time::interval(HOUSEKEEPING_INTERVAL);
    housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if let Some(commits) = commits.as_mut() {
            let assigned = commits.settle(consumer, queue)?;
            reconcile_pause_after_assign(consumer, *paused, assigned)?;
        }
        let now = Instant::now();
        let commit_deadline = commits.as_ref().map_or(now, |c| c.slot.gate.deadline(now));
        let in_flight = commits.as_ref().is_some_and(|c| c.slot.in_flight.is_some());
        let committable = !in_flight
            && commits
                .as_ref()
                .is_some_and(|c| c.tracker.has_committable());
        tokio::select! {
            biased;
            // Shutdown and the fault are read before the permit. An in-place
            // wait that either one ends frees its permit in the same
            // instant, and taking that permit first would hand the record in
            // hand to a handler on a consumer that is about to return:
            // handled ahead of the record that was waiting, and handled
            // again after the restart.
            () = &mut stop => return Ok(None),
            e = &mut fault => return Err(e),
            permit = semaphore.clone().acquire_many_owned(permits) => {
                let permit = permit.map_err(|_| {
                    ShoveError::Connection("semaphore closed".to_string())
                })?;
                return Ok(Some(permit));
            }
            () = &mut grace, if !polling => polling = true,
            _ = housekeeping.tick(), if commits.is_some() => {}
            _ = tokio::time::sleep_until(commit_deadline), if committable => {}
            answer = commit_answer(
                commits.as_mut().and_then(|c| c.slot.in_flight.as_mut()),
            ), if in_flight => {
                if let Some(commits) = commits.as_mut() {
                    commits.slot.settle(answer, commits.tracker, queue);
                }
            }
            received = consumer.recv(), if polling => {
                let msg = match received {
                    Ok(msg) => msg,
                    Err(e) if recv_error_is_transient(gate, &e) => {
                        log_transient_recv_error(queue, &e);
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, queue, "consumer recv error");
                        return Err(map_kafka_error(
                            &format!("consumer recv error on {queue} while waiting for a permit"),
                            e,
                        ));
                    }
                };
                #[cfg(feature = "test-support")]
                put_back_probe::PERMIT_WAIT.fetch_add(1, Ordering::SeqCst);
                put_back(consumer, queue, msg.partition(), msg.offset())?;
                consumer
                    .pause_assignment()
                    .map_err(|e| map_kafka_error("pause failed", e))?;
                if !*paused {
                    *paused = true;
                    // A transition, at info: a member can sit paused for
                    // minutes, and this is the only trace of it in
                    // production. Each further record is put back at debug.
                    tracing::info!(
                        queue,
                        "a record arrived while every prefetch permit is held; assignment paused"
                    );
                }
                tracing::debug!(
                    queue,
                    partition = msg.partition(),
                    offset = msg.offset(),
                    "record delivered during a permit wait put back; pause widened to the current assignment"
                );
            }
        }
    }
}

/// Resolves once `shutdown` fires, and never when there is none to read.
async fn stop_requested(shutdown: Option<&CancellationToken>) {
    match shutdown {
        Some(shutdown) => shutdown.cancelled().await,
        None => std::future::pending().await,
    }
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
/// The events reach the tracker as well, see `OffsetTracker::rebalanced`:
/// a moved partition is dropped with everything it had in flight, and every
/// partition this member keeps re-offers its position.
///
/// An assign among the events re-applies the loop's pause intent over the
/// whole assignment, see [`reconcile_pause_after_assign`]: the batch loop
/// holds no pause of its own, and this drain runs outside the polling
/// waits. A registry stall pauses the assignment for its own wait only: its
/// pass drains the events there and keeps that pause on an assign, see
/// `BatchState::stall_pass`, and `RegistryStall::end` lifts it. A partition
/// revoked during a stall and handed back after it would otherwise come back
/// paused and stay so.
///
/// Returns the partitions of the abandoned batch, every one of them sought
/// back to the span's start, or left to its next owner when it was revoked;
/// the caller disarms the age deadline then, and drops a record in hand on
/// one of those partitions, which the broker delivers again behind the
/// rewound ones. `None` means the batch stands. An `Err` means the rewind
/// could not put a still-held partition back and the caller must reconnect
/// rather than keep polling — see [`rewind_span`].
fn apply_batch_rebalance<T: Topic>(
    consumer: &KafkaStreamConsumer,
    queue: &str,
    buffer: &mut BatchBuffer<T>,
    state: &mut BatchState,
) -> Result<Option<BTreeSet<i32>>> {
    let BatchRebalance { moved, assigned } = drain_batch_rebalance_events(&state.rebalance_rx);
    if assigned || !moved.is_empty() {
        state.tracker.rebalanced(moved.iter().copied());
    }
    reconcile_pause_after_assign(consumer, state.paused, assigned)?;
    if moved.is_empty() || buffer.is_empty() {
        return Ok(None);
    }
    if !rebalance_affects_batch(&moved, &buffer.start) {
        tracing::debug!(
            queue,
            partitions = ?moved,
            "group rebalanced on partitions the in-flight batch does not span, keeping it"
        );
        return Ok(None);
    }
    let spanned: BTreeSet<i32> = buffer.start.keys().copied().collect();
    tracing::warn!(
        queue,
        partitions = ?moved,
        ?spanned,
        "group rebalanced mid-batch, abandoning the in-flight batch uncommitted"
    );
    // Rewind before clearing: under cooperative-sticky the partitions this
    // member *keeps* would otherwise have their consumed-but-uncommitted
    // messages skipped.
    let rewound = rewind_span(consumer, queue, &buffer.start);
    buffer.clear();
    rewound.map(|()| Some(spanned))
}

// ---------------------------------------------------------------------------
// Sequenced (FIFO) consumption
// ---------------------------------------------------------------------------

/// The first wait before a sequenced consumer tries a failed dead-letter
/// publish again. Each further failure doubles it, up to
/// [`DEAD_LETTER_RETRY_MAX`].
const DEAD_LETTER_RETRY_INITIAL: Duration = Duration::from_secs(1);

/// The longest wait between two tries of one dead-letter publish.
const DEAD_LETTER_RETRY_MAX: Duration = Duration::from_secs(30);

/// What every connection of one sequenced consumer reads: the handler, the
/// client and the settings `spawn_fifo_shards` resolved once.
struct FifoLoop<T: Topic, H: MessageHandler<T>> {
    handler: H,
    ctx: H::Context,
    client: KafkaClient,
    topology: &'static QueueTopology,
    queue: String,
    topic: Arc<str>,
    group: Option<Arc<str>>,
    group_id: String,
    auto_offset_reset: KafkaAutoOffsetReset,
    max_poll_interval_ms: u32,
    fetch_queue: FetchQueueTuning,
    /// The fenced consumer detector's threshold, see [`fence_threshold_over`],
    /// for the fixed default interval the loop commits on.
    fence_timeout: Duration,
    shutdown: CancellationToken,
    processing: Arc<AtomicBool>,
    max_retries: u32,
    handler_timeout: Option<Duration>,
    handler_timeout_outcome: Option<Outcome>,
    max_message_size: Option<usize>,
    poisoned: PoisonedKeys,
    #[cfg(feature = "kafka-schema-registry")]
    schema_registry: Option<Arc<SchemaRegistry>>,
    #[cfg(feature = "kafka-schema-registry")]
    schema_enforcement: SchemaEnforcement,
    #[cfg(feature = "kafka-schema-registry")]
    schema_accepted: Arc<[Arc<str>]>,
    #[cfg(feature = "kafka-schema-registry")]
    schema_message_index: Option<Arc<[i32]>>,
    #[cfg(feature = "kafka-schema-registry")]
    lookup_bound: Duration,
    _topic: PhantomData<fn() -> T>,
}

/// What one connection of a [`SerialLoop`] keeps between records: a
/// sequenced consumer's, and a DLQ drain's.
///
/// The tracker is the standard consumer's. A record is in flight from the
/// moment it is taken until it is finished with: acked or dead-lettered by a
/// sequenced consumer, handed to `handle_dead` or dropped by a drain. The
/// commit position therefore never passes a record that is waiting out a
/// hold, a failed dead-letter publish or a registry outage, and a revoke
/// drops the partition's entry with nothing committed for it.
struct FifoState {
    tracker: OffsetTracker,
    rebalance_rx: std_mpsc::Receiver<RebalanceEvent>,
    /// The commit gate and the one commit in flight, see [`CommitSlot`].
    commits: CommitSlot,
    /// The fenced consumer detector's threshold, see [`fence_threshold_over`].
    fence_timeout: Duration,
    /// The member's group, which the fence's error names.
    group_id: String,
    /// Whether the loop holds its assignment paused: a wait on the record in
    /// hand pauses it. The next pass resumes it.
    paused: bool,
}

/// Where the record a [`SerialLoop`] took sits in its tracker, see
/// [`FifoState::take`].
struct Tracked {
    partition: i32,
    offset: i64,
    /// The epoch of the assignment the record was taken under, see
    /// `OffsetTracker::track_received`.
    epoch: u64,
    /// Cancelled by a revoke of the record's partition, see
    /// `AssignmentTokens`.
    revoked: CancellationToken,
}

/// The record a sequenced consumer has in hand, read once off its message.
struct FifoRecord<'m> {
    tracked: Tracked,
    payload: &'m [u8],
    key: Option<&'m [u8]>,
    headers: RecordHeaders,
    coordinates: RecordCoordinates,
}

/// Whether a [`SerialLoop`] goes on to the next record or stops.
enum FifoPass {
    Next,
    Stop,
    /// The record in hand could not be dead-lettered, with an error no retry
    /// gets past. The loop stops as on a stop, its final commit included, and
    /// the member then ends with the error.
    End(ShoveError),
}

/// How a wait on the record in hand ended.
enum FifoWait {
    /// The delay ran out: the record goes on.
    Elapsed,
    /// Shutdown fired. The record stays uncommitted for the restart.
    Stop,
    /// The record's partition was revoked. The record is left to the
    /// partition's next owner, which is handed it from the committed offset.
    Revoked,
}

/// Holds the consumer's busy flag up while a record is with its handler,
/// its in-place retries and its dead letter included, and drops it on every
/// exit.
struct Busy<'a>(&'a AtomicBool);

impl<'a> Busy<'a> {
    fn begin(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::Release);
        Self(flag)
    }
}

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Pauses the assignment for a wait on the record in hand, once.
fn pause_for_wait(consumer: &KafkaStreamConsumer, paused: &mut bool) -> Result<()> {
    if !*paused {
        consumer
            .pause_assignment()
            .map_err(|e| map_kafka_error("pause failed", e))?;
        *paused = true;
    }
    Ok(())
}

/// Waits out `delay` on the record in hand while the member keeps polling:
/// the hold before an in-place retry, and the backoff before a failed
/// dead-letter publish is tried again. A park that stopped polling would let
/// the group evict the member once the wait passed `max.poll.interval.ms`.
///
/// The assignment is paused for the wait, so `recv()` yields nothing it
/// held. A record it does yield comes from a partition assigned since; it is
/// put back (see [`put_back`]) with the pause widened, and arrives again, in
/// order, once the next pass resumes. The stop and the revoke of the
/// record's partition are read first and the delay last, and both are read
/// once more when the delay runs out: a revoke reaches the token inside a
/// `recv()` poll, which can be the poll right before the delay's, see
/// `after_wait`.
///
/// Every pass of the wait makes the commit the gate allows, as a pass of the
/// receive loop does, and the wait wakes for it as the loop does: on the
/// gate's deadline, on the answer of the commit in flight and on the
/// housekeeping tick. The positions acked before the wait, and the ones a
/// rebalance or a rejected commit re-offers during it, are then committed
/// however long the wait runs; the record in hand stays in flight, so no
/// commit passes it.
async fn fifo_wait(
    consumer: &Arc<KafkaStreamConsumer>,
    queue: &str,
    delay: Duration,
    shutdown: &CancellationToken,
    revoked: &CancellationToken,
    state: &mut FifoState,
) -> Result<FifoWait> {
    pause_for_wait(consumer, &mut state.paused)?;
    let elapsed = tokio::time::sleep(delay);
    tokio::pin!(elapsed);
    let mut housekeeping = tokio::time::interval(HOUSEKEEPING_INTERVAL);
    housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        state.commit_due(consumer, queue)?;
        let now = Instant::now();
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(FifoWait::Stop),
            _ = revoked.cancelled() => return Ok(FifoWait::Revoked),
            _ = housekeeping.tick() => {}
            _ = tokio::time::sleep_until(state.commits.gate.deadline(now)),
                if state.commits.in_flight.is_none() && state.tracker.has_committable() => {}
            answer = commit_answer(state.commits.in_flight.as_mut()),
                if state.commits.in_flight.is_some() => {
                state.commits.settle(answer, &mut state.tracker, queue);
            }
            received = consumer.recv() => {
                let msg = match received {
                    Ok(msg) => msg,
                    Err(e) if recv_error_is_transient(consumer.transport_gate(), &e) => {
                        log_transient_recv_error(queue, &e);
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, queue, "FIFO consumer recv error");
                        return Err(map_kafka_error(
                            &format!("FIFO consumer recv error on {queue} during a wait"),
                            e,
                        ));
                    }
                };
                put_back(consumer, queue, msg.partition(), msg.offset())?;
                consumer
                    .pause_assignment()
                    .map_err(|e| map_kafka_error("pause failed", e))?;
                tracing::debug!(
                    queue,
                    partition = msg.partition(),
                    offset = msg.offset(),
                    "record delivered during a FIFO wait put back; pause widened to the current assignment"
                );
            }
            () = &mut elapsed => {
                return Ok(match after_wait(shutdown, Some(revoked)) {
                    AfterWait::Dispatch => FifoWait::Elapsed,
                    AfterWait::Stop => FifoWait::Stop,
                    AfterWait::Revoked => FifoWait::Revoked,
                });
            }
        }
    }
}

impl FifoState {
    /// Applies the rebalance events librdkafka queued, then re-applies the
    /// loop's pause intent if an assign was among them, see
    /// [`reconcile_pause_after_assign`]: a partition revoked during a wait
    /// and handed back once the wait is over arrives paused, and nothing
    /// else would resume it, and one handed out during a wait is paused
    /// with the rest.
    fn apply_rebalance_events(&mut self, consumer: &KafkaStreamConsumer) -> Result<()> {
        let assigned = self.tracker.apply_rebalance_events(&self.rebalance_rx);
        reconcile_pause_after_assign(consumer, self.paused, assigned)
    }

    /// Tracks the record `msg` carries as in flight. The poll that delivered
    /// it may have queued the assign of its partition, so the rebalance
    /// events are applied first, or the next pass would drop the record's
    /// tracker entry.
    fn take(
        &mut self,
        consumer: &KafkaStreamConsumer,
        msg: &BorrowedMessage<'_>,
    ) -> Result<Tracked> {
        self.apply_rebalance_events(consumer)?;
        let (partition, offset) = (msg.partition(), msg.offset());
        let Assignment { epoch, revoked } = self.tracker.track_received(partition, offset);
        Ok(Tracked {
            partition,
            offset,
            epoch,
            revoked,
        })
    }

    /// The top of every pass: the rebalance events, the commit the gate
    /// allows, and the end of the pause the last record's wait left behind.
    fn settle(&mut self, consumer: &Arc<KafkaStreamConsumer>, queue: &str) -> Result<()> {
        self.commit_due(consumer, queue)?;
        if self.paused {
            consumer
                .resume_assignment()
                .map_err(|e| map_kafka_error("resume failed", e))?;
            self.paused = false;
        }
        Ok(())
    }

    /// The commit step the standard consumer runs, see [`CommitSlot::pass`],
    /// then the pause intent an assign among its rebalance events calls for,
    /// as in [`apply_rebalance_events`](Self::apply_rebalance_events): at
    /// the top of every pass, and on every pass of a wait on the record in
    /// hand, so a position acked before the wait and one a rebalance or a
    /// rejected commit re-offers during it are committed however long the
    /// wait runs.
    ///
    /// Commits are spaced as the standard consumer spaces them, at most once
    /// per `ASYNC_COMMIT_INTERVAL`, and made as it makes them under
    /// `CommitPolicy::Interval`: one at a time, synchronously on a thread of
    /// its own while the loop goes on, see [`CommitSlot`]. A discard rides
    /// the commit that retires its record and counts once the broker
    /// accepted it. A rejected commit is re-offered with its discards on a
    /// later pass, and a streak of rejections nothing resolves fences the
    /// member as it fences the standard consumer: the connection ends with
    /// the error, and `run_with_reconnect` reconnects. The commit in flight
    /// when the loop stops is the stop's to settle.
    fn commit_due(&mut self, consumer: &Arc<KafkaStreamConsumer>, queue: &str) -> Result<()> {
        let fence = Fence {
            threshold: self.fence_timeout,
            group_id: &self.group_id,
        };
        let assigned = self.commits.pass(
            consumer,
            &mut self.tracker,
            &self.rebalance_rx,
            fence,
            queue,
        )?;
        reconcile_pause_after_assign(consumer, self.paused, assigned)
    }

    /// The record in hand is finished with, `discard` riding the commit that
    /// retires it.
    fn complete(&mut self, tracked: &Tracked, discard: Option<TerminalDiscard>) {
        self.tracker.mark_complete(Completion {
            partition: tracked.partition,
            offset: tracked.offset,
            epoch: tracked.epoch,
            discard,
        });
    }

    /// Decodes the record in hand with `decode`, which answers `None` when a
    /// stop fired first, see [`decode_or_shutdown`]. A schema registry that
    /// cannot answer stalls the record with the assignment paused, see
    /// `RegistryStall`, and `decode` is tried again after each wait; a stop
    /// or a revoke of the record's partition, which cancels `revoked`, ends
    /// the stall.
    #[cfg_attr(not(feature = "kafka-schema-registry"), allow(unused_variables))]
    #[allow(clippy::too_many_arguments)]
    async fn decode_in_hand<M, F, Fut>(
        &mut self,
        consumer: &Arc<KafkaStreamConsumer>,
        queue: &str,
        shutdown: &CancellationToken,
        revoked: &CancellationToken,
        topic: &str,
        group: Option<&str>,
        mut decode: F,
    ) -> Result<Staged<M>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Option<BatchDecode<M>>>,
    {
        // The loop holds the pause through a stall itself, see
        // `RegistryStall::inside_pause`.
        #[cfg(feature = "kafka-schema-registry")]
        let mut stall = RegistryStall::inside_pause(true);
        // Loops only through the registry wait arm, which the
        // `kafka-schema-registry` feature adds.
        #[allow(clippy::never_loop)]
        let staged = loop {
            let Some(decoded) = decode().await else {
                break Staged::Stop;
            };
            match decoded {
                BatchDecode::Decoded(message) => break Staged::Ready(message),
                BatchDecode::Dlq { reason, fail } => break Staged::Drop(fail, reason),
                #[cfg(feature = "kafka-schema-registry")]
                BatchDecode::Unavailable { id, error } => {
                    pause_for_wait(consumer, &mut self.paused)?;
                    if !stall
                        .wait(consumer, shutdown, None, topic, group, queue, id, &error)
                        .await?
                    {
                        break Staged::Stop;
                    }
                    // The wait polled `recv()`, inside which a revoke of the
                    // record's partition cancels its token: read before the
                    // decode is retried.
                    match after_wait(shutdown, Some(revoked)) {
                        AfterWait::Dispatch => {}
                        AfterWait::Stop => break Staged::Stop,
                        AfterWait::Revoked => break Staged::Revoked,
                    }
                    // An outage can last: commit between its retries, as a
                    // hold does, see `fifo_wait`.
                    self.commit_due(consumer, queue)?;
                }
                // A deployment fault, not an outage: stop instead of waiting
                // on it.
                #[cfg(feature = "kafka-schema-registry")]
                BatchDecode::Fatal(e) => return Err(e),
            }
        };
        #[cfg(feature = "kafka-schema-registry")]
        stall.end(consumer)?;
        Ok(staged)
    }
}

/// A consume loop that takes one record at a time and keeps a [`FifoState`]
/// between records: the sequenced consumer and the DLQ drain. Each
/// connection of either runs through [`run_serial_connection`], so both
/// subscribe, receive, commit and stop alike, and only the record step is a
/// loop's own.
trait SerialLoop: Sync {
    /// What a connection of the loop is made with.
    fn connection(&self) -> SerialConnection<'_>;

    /// Takes one record and carries it to its end, see [`FifoPass`].
    fn record(
        &self,
        consumer: &Arc<KafkaStreamConsumer>,
        state: &mut FifoState,
        msg: &BorrowedMessage<'_>,
    ) -> impl Future<Output = Result<FifoPass>> + Send;
}

/// What a connection of a [`SerialLoop`] is made with.
struct SerialConnection<'a> {
    client: &'a KafkaClient,
    /// The topic the loop reads and commits on.
    queue: &'a str,
    group_id: &'a str,
    auto_offset_reset: KafkaAutoOffsetReset,
    max_poll_interval_ms: u32,
    fetch_queue: FetchQueueTuning,
    /// The fenced consumer detector's threshold, see [`fence_threshold_over`].
    fence_timeout: Duration,
    shutdown: &'a CancellationToken,
    /// The loop's name in its log lines and in the error a receive ends on.
    label: &'static str,
    /// The context of the error a refused subscribe ends on.
    subscribe_failed: &'static str,
    /// The dead-letter topic a drain reads, logged under `dlq`; `None` for a
    /// sequenced consumer.
    dlq: Option<&'a str>,
}

/// One connection of a [`SerialLoop`]: subscribe and consume until a stop
/// or a fault. A stop ends with the final commit and the consumer's close on
/// a thread of their own; a fault closes the consumer on a thread of its own
/// too, with nothing committed, and waits for the close before it returns
/// the error, see [`close_without_commit`]. An abort of the loop's task
/// leaves the close to [`LoopConsumer`]. A sequenced consumer's dead letter
/// that failed with an error no retry gets past, see [`FifoPass::End`], ends
/// as a stop does, the final commit carrying what was acked before it, and
/// then returns that error, so `run_with_reconnect` ends the member on it.
async fn run_serial_connection<L: SerialLoop>(serial: &L) -> Result<()> {
    let connection = serial.connection();
    let queue = connection.queue;
    let (rebalance_tx, rebalance_rx) = std_mpsc::channel::<RebalanceEvent>();
    // The map the rebalance callback cancels revoked partitions' tokens
    // in; the tracker below shares it. See `AssignmentTokens`.
    let assignments = AssignmentTokens::default();
    let consumer = LoopConsumer::new(
        create_stream_consumer(
            connection.client.base_config(),
            connection.group_id,
            connection.auto_offset_reset,
            connection.max_poll_interval_ms,
            &connection.fetch_queue,
            queue,
            rebalance_tx,
            assignments.clone(),
            #[cfg(feature = "kafka-msk-iam")]
            connection.client.msk_context(),
        )?,
        queue,
    );
    let mut state = FifoState {
        tracker: OffsetTracker::with_assignments(queue.to_string(), assignments),
        rebalance_rx,
        commits: CommitSlot::new(CommitPolicy::Interval(ASYNC_COMMIT_INTERVAL)),
        fence_timeout: connection.fence_timeout,
        group_id: connection.group_id.to_string(),
        paused: false,
    };
    let ended = match consumer.subscribe(&[queue]) {
        Ok(()) => receive_serially(serial, &connection, &consumer, &mut state).await,
        Err(e) => Err(map_kafka_error(connection.subscribe_failed, e)),
    };
    let fault = match ended {
        Ok(fault) => fault,
        Err(e) => {
            close_without_commit(consumer, queue, SHUTDOWN_COMMIT_DEADLINE).await;
            return Err(e);
        }
    };
    let pending_commit = state.commits.in_flight.take();
    let stopped = stop_with_final_commit(
        consumer,
        &mut state.tracker,
        &mut state.rebalance_rx,
        pending_commit,
        queue,
    )
    .await;
    // The fault outranks the stop's result; the stop logs its own failure.
    match fault {
        Some(e) => Err(e),
        None => stopped,
    }
}

/// The receive loop of [`run_serial_connection`]. `Ok` is a stop: whatever
/// record was in hand is left uncommitted, and nothing behind it was handed
/// to the handler. It carries the error of a dead letter no retry gets past
/// when that is what stopped the loop, see [`FifoPass::End`].
async fn receive_serially<L: SerialLoop>(
    serial: &L,
    connection: &SerialConnection<'_>,
    consumer: &Arc<KafkaStreamConsumer>,
    state: &mut FifoState,
) -> Result<Option<ShoveError>> {
    let queue = connection.queue;
    let label = connection.label;
    // Wakes an idle loop, so rebalance events are applied and the positions
    // they re-offer are committed without a record to wait for.
    let mut housekeeping = tokio::time::interval(HOUSEKEEPING_INTERVAL);
    housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        state.settle(consumer, queue)?;
        let now = Instant::now();
        tokio::select! {
            // The stop first: a stop that landed during the last record's
            // wait is read before the next record is taken.
            biased;
            _ = connection.shutdown.cancelled() => {
                tracing::info!(queue, dlq = connection.dlq, "shutdown signal received, stopping {label}");
                return Ok(None);
            }
            _ = housekeeping.tick() => {}
            // Wakes the loop when the commit gate's window expires with a
            // position to commit, so it lands on a topic gone idle.
            _ = tokio::time::sleep_until(state.commits.gate.deadline(now)),
                if state.commits.in_flight.is_none() && state.tracker.has_committable() => {}
            answer = commit_answer(state.commits.in_flight.as_mut()),
                if state.commits.in_flight.is_some() => {
                state.commits.settle(answer, &mut state.tracker, queue);
            }
            received = consumer.recv() => {
                let msg = match received {
                    Ok(msg) => msg,
                    Err(e) if recv_error_is_transient(consumer.transport_gate(), &e) => {
                        log_transient_recv_error(queue, &e);
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, queue, dlq = connection.dlq, "{label} recv error");
                        return Err(map_kafka_error(
                            &format!("{label} recv error on {queue}"),
                            e,
                        ));
                    }
                };
                match serial.record(consumer, state, &msg).await? {
                    FifoPass::Next => {}
                    FifoPass::Stop => return Ok(None),
                    FifoPass::End(e) => return Ok(Some(e)),
                }
            }
        }
    }
}

impl<T: Topic, H: MessageHandler<T>> SerialLoop for FifoLoop<T, H> {
    fn connection(&self) -> SerialConnection<'_> {
        SerialConnection {
            client: &self.client,
            queue: &self.queue,
            group_id: &self.group_id,
            auto_offset_reset: self.auto_offset_reset,
            max_poll_interval_ms: self.max_poll_interval_ms,
            fetch_queue: self.fetch_queue,
            fence_timeout: self.fence_timeout,
            shutdown: &self.shutdown,
            label: "FIFO consumer",
            subscribe_failed: "failed to subscribe",
            dlq: None,
        }
    }

    /// Takes one record and carries it to its end: acked, dead-lettered,
    /// left to its partition's next owner, or kept uncommitted for a stop or
    /// for an error that ends the member.
    async fn record(
        &self,
        consumer: &Arc<KafkaStreamConsumer>,
        state: &mut FifoState,
        msg: &BorrowedMessage<'_>,
    ) -> Result<FifoPass> {
        let queue = self.queue.as_str();
        let record = FifoRecord {
            tracked: state.take(consumer, msg)?,
            payload: msg.payload().unwrap_or_default(),
            key: msg.key(),
            headers: RecordHeaders::read(msg),
            coordinates: RecordCoordinates::of(msg),
        };
        // The Kafka message key *is* the sequence key — the publisher sets it
        // from `SEQUENCE_KEY_FN`.
        let seq_key = record
            .key
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .unwrap_or_default();
        let group = self.group.as_deref();
        let has_dlq = self.topology.dlq().is_some();
        metrics::record_message_size(&self.topic, group, record.payload.len());

        // ── FailAll: skip poisoned keys ──
        // Inert unless this topic is configured `SequenceFailure::FailAll`.
        if self.poisoned.is_poisoned(&seq_key) {
            tracing::warn!(
                queue,
                sequence_key = %seq_key,
                "sequence key poisoned (FailAll) — sending to DLQ without invoking handler"
            );
            // Collateral of an already-counted failure, so the failure half
            // is deliberately not counted again — see `metrics::FailReason`.
            // The discard half still applies: a cascaded message dropped
            // with no DLQ is just as gone as any other.
            let pending = metrics::pending_discard(
                &self.topic,
                group,
                metrics::FailReason::Rejected,
                has_dlq,
            );
            let retry_count = get_retry_count(&record.headers.strings);
            return self
                .dead_letter(consumer, state, &record, "rejected", retry_count, pending)
                .await;
        }

        if let Err(e) = validate_message_size(record.payload.len(), self.max_message_size) {
            tracing::warn!(error = %e, queue, "rejecting oversized FIFO message to DLQ");
            poison_key(&self.poisoned, &seq_key, queue);
            let pending = metrics::record_terminal(
                &self.topic,
                group,
                metrics::FailReason::Oversize,
                has_dlq,
            );
            let retry_count = get_retry_count(&record.headers.strings);
            return self
                .dead_letter(
                    consumer,
                    state,
                    &record,
                    &e.to_string(),
                    retry_count,
                    pending,
                )
                .await;
        }

        let message = match self.decode(consumer, state, &record).await? {
            Staged::Ready(message) => message,
            Staged::Drop(fail, reason) => {
                poison_key(&self.poisoned, &seq_key, queue);
                let pending = metrics::record_terminal(&self.topic, group, fail, has_dlq);
                let retry_count = get_retry_count(&record.headers.strings);
                return self
                    .dead_letter(consumer, state, &record, reason, retry_count, pending)
                    .await;
            }
            Staged::Stop => return Ok(FifoPass::Stop),
            Staged::Revoked => return Ok(FifoPass::Next),
        };
        self.handle(consumer, state, &record, message, &seq_key)
            .await
    }
}

impl<T: Topic, H: MessageHandler<T>> FifoLoop<T, H> {
    /// Decodes the record in hand from its bytes: before its first handler
    /// call, and again before every in-place retry, because
    /// `MessageHandler::handle` takes the message by value and `Topic` does
    /// not require `Clone`. A schema registry that cannot answer stalls the
    /// record, see [`FifoState::decode_in_hand`].
    async fn decode(
        &self,
        consumer: &Arc<KafkaStreamConsumer>,
        state: &mut FifoState,
        record: &FifoRecord<'_>,
    ) -> Result<Staged<T::Message>> {
        let decode = BatchDecodeCtx {
            queue: &self.queue,
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: self.schema_registry.as_ref(),
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: self.schema_enforcement,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted: &self.schema_accepted,
            #[cfg(feature = "kafka-schema-registry")]
            schema_message_index: self.schema_message_index.as_deref(),
            #[cfg(feature = "kafka-schema-registry")]
            registry_lookup_bound: self.lookup_bound,
        };
        state
            .decode_in_hand(
                consumer,
                &self.queue,
                &self.shutdown,
                &record.tracked.revoked,
                &self.topic,
                self.group.as_deref(),
                || {
                    decode_or_shutdown(
                        decode_batch_message::<T>(&decode, record.payload),
                        &self.shutdown,
                    )
                },
            )
            .await
    }

    /// Hands the record to the handler until it settles on a terminal
    /// outcome, retrying in place.
    ///
    /// A `Retry`, a `Defer` turned into one, a handler timeout that resolves
    /// to either, and a panic keep the record where it is. The loop waits the
    /// hold delay for the attempt, 1 s with no hold queues, see
    /// [`hold_delay`], with the assignment paused and still polling, then
    /// hands the same record back as a redelivery, decoded afresh. The count
    /// lives in memory, read from the record's `Shove-Retry-Count` on every
    /// delivery, and a dead letter carries it. A `Reject` or a spent budget
    /// dead-letters the record and poisons its key under `FailAll`.
    async fn handle(
        &self,
        consumer: &Arc<KafkaStreamConsumer>,
        state: &mut FifoState,
        record: &FifoRecord<'_>,
        first: T::Message,
        seq_key: &str,
    ) -> Result<FifoPass> {
        let queue = self.queue.as_str();
        let group = self.group.as_deref();
        let _busy = Busy::begin(&self.processing);
        let mut message = Some(first);
        let mut attempts = get_retry_count(&record.headers.strings);
        let mut redelivered = false;
        loop {
            let payload = match message.take() {
                Some(payload) => payload,
                None => match self.decode(consumer, state, record).await? {
                    Staged::Ready(payload) => payload,
                    Staged::Drop(fail, reason) => {
                        tracing::error!(
                            queue,
                            reason,
                            "record no longer decodes for its in-place retry; dropping it as a pre-handler discard"
                        );
                        poison_key(&self.poisoned, seq_key, queue);
                        let pending = metrics::record_terminal(
                            &self.topic,
                            group,
                            fail,
                            self.topology.dlq().is_some(),
                        );
                        return self
                            .dead_letter(consumer, state, record, reason, attempts, pending)
                            .await;
                    }
                    Staged::Stop => return Ok(FifoPass::Stop),
                    Staged::Revoked => return Ok(FifoPass::Next),
                },
            };
            let mut metadata =
                build_message_metadata(&record.headers.strings, redelivered, record.coordinates);
            metadata.retry_count = attempts;
            let outcome = adjust_outcome_for_fifo(
                invoke_handler(
                    self.handler.handle(payload, metadata, &self.ctx),
                    self.handler_timeout,
                    self.handler_timeout_outcome.clone(),
                    &self.topic,
                    group,
                )
                .await,
            );
            match decide_retry(&outcome, attempts, self.max_retries) {
                RetryDecision::Ack => {
                    state.complete(&record.tracked, None);
                    return Ok(FifoPass::Next);
                }
                RetryDecision::Dlq { reason } => {
                    // FailAll: a DLQ-terminal outcome poisons the key, so
                    // every later message for it is dead-lettered instead of
                    // handled.
                    poison_key(&self.poisoned, seq_key, queue);
                    let pending = metrics::record_terminal(
                        &self.topic,
                        group,
                        terminal_fail_reason(reason),
                        self.topology.dlq().is_some(),
                    );
                    return self
                        .dead_letter(consumer, state, record, reason, attempts, pending)
                        .await;
                }
                RetryDecision::Hold { increment } => {
                    let delay = hold_delay(self.topology.hold_queues(), attempts, increment);
                    match fifo_wait(
                        consumer,
                        queue,
                        delay,
                        &self.shutdown,
                        &record.tracked.revoked,
                        state,
                    )
                    .await?
                    {
                        FifoWait::Elapsed => {}
                        FifoWait::Stop => return Ok(FifoPass::Stop),
                        FifoWait::Revoked => return Ok(FifoPass::Next),
                    }
                    if increment {
                        attempts = attempts.saturating_add(1);
                    }
                    redelivered = true;
                }
            }
        }
    }

    /// Dead-letters the record in hand under `reason`, with `retry_count` as
    /// the dead letter's count, and completes the record once the dead
    /// letter landed.
    ///
    /// A publish that fails is settled by its error:
    /// - `ShoveError::Validation`: the producer or the broker refused the dead
    ///   letter as too large or as invalid, so it can never be published, and
    ///   trying it again would block the partition for good. The record is
    ///   completed as lost, an error is logged, and the loop goes on to the
    ///   next.
    /// - Any other error no retry gets past ([`ShoveError::is_retryable`] is
    ///   false), such as the `Topology` error of a producer that cannot be
    ///   built, ends the loop with [`FifoPass::End`]. The record stays
    ///   uncommitted, `pending` settles as survived, and the member ends with
    ///   the error once the stop's final commit has carried what was acked
    ///   before it.
    /// - A retryable error keeps the record: the publish is tried again after
    ///   [`DEAD_LETTER_RETRY_INITIAL`], doubling up to
    ///   [`DEAD_LETTER_RETRY_MAX`], with the assignment paused and the member
    ///   polling, and nothing is committed past it meanwhile.
    ///
    /// `pending` is the record's discard: settled at once when the dead letter
    /// is in the DLQ, and on the commit that retires the record when no DLQ is
    /// declared or the dead letter can never be published. A stop or a revoke
    /// while the publish is retried settles it as survived, because the
    /// record is redelivered.
    async fn dead_letter(
        &self,
        consumer: &Arc<KafkaStreamConsumer>,
        state: &mut FifoState,
        record: &FifoRecord<'_>,
        reason: &str,
        retry_count: u32,
        pending: metrics::PendingDiscard,
    ) -> Result<FifoPass> {
        let queue = self.queue.as_str();
        let mut backoff = Backoff::new(DEAD_LETTER_RETRY_INITIAL, DEAD_LETTER_RETRY_MAX);
        loop {
            let published = tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    pending.survived();
                    return Ok(FifoPass::Stop);
                }
                published = publish_to_dlq(
                    &self.client,
                    self.topology,
                    record.payload,
                    record.key,
                    &record.headers,
                    reason,
                    retry_count,
                ) => published,
            };
            let Err(e) = published else { break };
            if is_unpublishable(&e) {
                tracing::error!(
                    error = %e,
                    queue,
                    partition = record.tracked.partition,
                    offset = record.tracked.offset,
                    "the dead letter can never be published; the record is discarded"
                );
                // `publish_to_dlq` is `Ok(())` when no DLQ is declared, so a
                // DLQ was, and nothing holds a copy.
                state.complete(&record.tracked, Some(TerminalDiscard::Lost(pending)));
                return Ok(FifoPass::Next);
            }
            if !e.is_retryable() {
                tracing::error!(
                    error = %e,
                    queue,
                    partition = record.tracked.partition,
                    offset = record.tracked.offset,
                    "dead-letter publish failed with an error no retry gets past; the record stays \
                     uncommitted and the consumer ends"
                );
                pending.survived();
                return Ok(FifoPass::End(e));
            }
            let delay = backoff.next().unwrap_or(DEAD_LETTER_RETRY_MAX);
            tracing::error!(
                error = %e,
                queue,
                partition = record.tracked.partition,
                offset = record.tracked.offset,
                retry_in = ?delay,
                "dead-letter publish failed; the record stays uncommitted and the publish is tried again"
            );
            match fifo_wait(
                consumer,
                queue,
                delay,
                &self.shutdown,
                &record.tracked.revoked,
                state,
            )
            .await?
            {
                FifoWait::Elapsed => {}
                FifoWait::Stop => {
                    pending.survived();
                    return Ok(FifoPass::Stop);
                }
                FifoWait::Revoked => {
                    pending.survived();
                    return Ok(FifoPass::Next);
                }
            }
        }
        let discard = match reject_settlement(self.topology.dlq().is_some(), true) {
            RejectSettlement::InDlq => {
                pending.survived();
                None
            }
            _ => Some(TerminalDiscard::Retired(pending)),
        };
        state.complete(&record.tracked, discard);
        Ok(FifoPass::Next)
    }
}

// ---------------------------------------------------------------------------
// DLQ drain
// ---------------------------------------------------------------------------

/// A token that fires when `first` or `second` does. It is a child of `first`,
/// so a stop through `first` reaches it in the same step, and a task forwards
/// `second` to it, at once when `second` has fired already. The guard ends that
/// task, so hold it as long as the token is in use.
fn cancelled_by_either(
    first: &CancellationToken,
    second: &CancellationToken,
) -> (CancellationToken, AbortOnDrop) {
    let merged = first.child_token();
    if second.is_cancelled() {
        merged.cancel();
    }
    let forward = tokio::spawn({
        let (second, merged) = (second.clone(), merged.clone());
        async move {
            second.cancelled().await;
            merged.cancel();
        }
    });
    (merged, AbortOnDrop(forward.abort_handle()))
}

/// What every connection of one DLQ drain reads: the handler, the client and
/// the settings `KafkaConsumer::run_dlq_with_inner` resolved once.
struct DlqDrain<T: Topic, H: MessageHandler<T>> {
    handler: H,
    ctx: H::Context,
    client: KafkaClient,
    /// The dead-letter topic the drain reads, and commits on.
    dlq: &'static str,
    /// The drain's group, `{dlq}-consumer` or `{group}-dlq`.
    group_id: String,
    /// The source topic and the group its consumers carry, which label the
    /// drain's metrics: the size sample of every dead letter and a registry
    /// wait.
    topic: Arc<str>,
    group: Option<Arc<str>>,
    /// Fires on the token of the options or on the client's, see
    /// [`cancelled_by_either`].
    shutdown: CancellationToken,
    max_message_size: Option<usize>,
    /// The bound on one `handle_dead` call, none when the options disable the
    /// handler timeout.
    handler_timeout: Option<Duration>,
    fetch_queue: FetchQueueTuning,
    /// The fenced consumer detector's threshold, see [`fence_threshold_over`].
    fence_timeout: Duration,
    #[cfg(feature = "kafka-schema-registry")]
    schema_registry: Option<Arc<SchemaRegistry>>,
    #[cfg(feature = "kafka-schema-registry")]
    schema_enforcement: SchemaEnforcement,
    #[cfg(feature = "kafka-schema-registry")]
    schema_accepted: Vec<Arc<str>>,
    #[cfg(feature = "kafka-schema-registry")]
    schema_message_index: Option<Vec<i32>>,
    #[cfg(feature = "kafka-schema-registry")]
    lookup_bound: Duration,
    _topic: PhantomData<fn() -> T>,
}

/// The dead letter a drain has in hand, read once off its message.
struct DrainRecord<'m> {
    tracked: Tracked,
    payload: &'m [u8],
    headers: Arc<HashMap<String, String>>,
    coordinates: RecordCoordinates,
}

impl<T: Topic, H: MessageHandler<T>> SerialLoop for DlqDrain<T, H> {
    fn connection(&self) -> SerialConnection<'_> {
        SerialConnection {
            client: &self.client,
            queue: self.dlq,
            group_id: &self.group_id,
            // The drain hard-codes `earliest` for its group, and that group id
            // is stable across restarts. librdkafka consults the policy only
            // for a group with no usable committed offset, so a fresh drain
            // starts at the earliest retained offset and a tail-only join can
            // never skip a dead letter the operator opted in to keep, while a
            // restarted drain resumes from its commit. The main consumer's
            // `auto_offset_reset` override never reaches here:
            // `check_dlq_options` refuses it for the drain.
            auto_offset_reset: KafkaAutoOffsetReset::Earliest,
            max_poll_interval_ms: MAX_POLL_INTERVAL_MS,
            fetch_queue: self.fetch_queue,
            fence_timeout: self.fence_timeout,
            shutdown: &self.shutdown,
            label: "DLQ consumer",
            subscribe_failed: "failed to subscribe to DLQ",
            dlq: Some(self.dlq),
        }
    }

    /// Takes one dead letter and carries it to its end: handed to
    /// `handle_dead` and finished with whatever the call came to, dropped
    /// before the handler, left to its partition's next owner, or kept
    /// uncommitted for a stop. The drain keeps no discard accounting, so no
    /// discard rides the commit that retires a dead letter.
    async fn record(
        &self,
        consumer: &Arc<KafkaStreamConsumer>,
        state: &mut FifoState,
        msg: &BorrowedMessage<'_>,
    ) -> Result<FifoPass> {
        let dlq = self.dlq;
        let record = DrainRecord {
            tracked: state.take(consumer, msg)?,
            payload: msg.payload().unwrap_or_default(),
            headers: extract_string_headers(msg),
            coordinates: RecordCoordinates::of(msg),
        };

        // Before the size gate, exactly as on the main loop: the histogram
        // describes what arrived on the wire, so the payload the gate is about
        // to discard is precisely the sample an operator sizing
        // `max_message_size` needs.
        metrics::record_message_size(&self.topic, self.group.as_deref(), record.payload.len());

        if let Some(max) = self.max_message_size
            && record.payload.len() > max
        {
            tracing::warn!(
                bytes = record.payload.len(),
                max,
                dlq,
                "oversized DLQ message — discarding"
            );
            state.complete(&record.tracked, None);
            return Ok(FifoPass::Next);
        }

        // Deserialize payload; on failure, log and commit anyway. With a
        // schema registry configured, the registry decode stage runs in place
        // of the direct codec decode, and a registry outage stalls the dead
        // letter as it stalls a sequenced consumer's record. The DLQ consumer
        // never re-publishes: an undecodable dead message is logged and
        // committed past.
        #[cfg(feature = "kafka-schema-registry")]
        let staged = match self.schema_registry.as_ref() {
            Some(registry) => {
                let payload = record.payload;
                state
                    .decode_in_hand(
                        consumer,
                        dlq,
                        &self.shutdown,
                        &record.tracked.revoked,
                        &self.topic,
                        self.group.as_deref(),
                        || self.decode_registered(registry, payload),
                    )
                    .await?
            }
            None => self.decode_direct(record.payload),
        };
        #[cfg(not(feature = "kafka-schema-registry"))]
        let staged = self.decode_direct(record.payload);
        let message = match staged {
            Staged::Ready(message) => message,
            Staged::Drop(..) => {
                state.complete(&record.tracked, None);
                return Ok(FifoPass::Next);
            }
            Staged::Stop => return Ok(FifoPass::Stop),
            Staged::Revoked => return Ok(FifoPass::Next),
        };

        let metadata = build_dead_metadata(&record.headers, record.coordinates);
        let called = guarded(
            self.handler.handle_dead(message, metadata, &self.ctx),
            self.handler_timeout,
        )
        .await;
        match called {
            Guarded::Done(()) => {}
            Guarded::Panicked => tracing::warn!(
                dlq,
                partition = record.tracked.partition,
                offset = record.tracked.offset,
                "handle_dead panicked; committing the dead letter anyway"
            ),
            Guarded::TimedOut(limit) => tracing::warn!(
                dlq,
                partition = record.tracked.partition,
                offset = record.tracked.offset,
                ?limit,
                "handle_dead timed out; committing the dead letter anyway"
            ),
        }
        state.complete(&record.tracked, None);
        Ok(FifoPass::Next)
    }
}

impl<T: Topic, H: MessageHandler<T>> DlqDrain<T, H> {
    /// Decodes a dead letter's payload with the topic's codec.
    fn decode_direct(&self, payload: &[u8]) -> Staged<T::Message> {
        match <T::Codec as crate::Codec<T::Message>>::decode(payload) {
            Ok(message) => Staged::Ready(message),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    dlq = self.dlq,
                    "failed to deserialize DLQ message, acking anyway"
                );
                Staged::Drop(metrics::FailReason::Deserialize, "deserialization_error")
            }
        }
    }

    /// Decodes a dead letter's payload through the schema registry once, for
    /// [`FifoState::decode_in_hand`] to try again through an outage. `None`
    /// means a stop fired during the lookup. A dead letter the registry
    /// rejects, or that does not decode, is logged and dropped, and the drain
    /// commits past it.
    #[cfg(feature = "kafka-schema-registry")]
    async fn decode_registered(
        &self,
        registry: &Arc<SchemaRegistry>,
        payload: &[u8],
    ) -> Option<BatchDecode<T::Message>> {
        let dlq = self.dlq;
        let codec_name = <T::Codec as crate::Codec<T::Message>>::NAME;
        let decoded = match WireFormat::from_codec_name(codec_name) {
            Some(format) => {
                decode_or_shutdown(
                    registry_decode::<T::Message, T::Codec>(
                        registry,
                        format,
                        self.schema_enforcement,
                        &self.schema_accepted,
                        self.schema_message_index.as_deref(),
                        self.lookup_bound,
                        payload,
                    ),
                    &self.shutdown,
                )
                .await?
            }
            None => {
                tracing::error!(
                    codec = codec_name,
                    dlq,
                    "codec has no Confluent wire format; acking dead message anyway"
                );
                Ok(RegistryDecode::Dlq("schema_unsupported_codec"))
            }
        };
        Some(match decoded {
            Ok(RegistryDecode::Decoded(message)) => BatchDecode::Decoded(message),
            Ok(RegistryDecode::Dlq(reason)) => {
                tracing::error!(
                    reason,
                    dlq,
                    "schema decode rejected dead message, acking anyway"
                );
                BatchDecode::Dlq {
                    reason,
                    fail: metrics::FailReason::for_schema_reason(reason),
                }
            }
            Ok(RegistryDecode::Unavailable { id, error }) => BatchDecode::Unavailable { id, error },
            // A deployment fault, not an outage: stop instead of waiting on it.
            Err(e @ ShoveError::Topology(_)) => BatchDecode::Fatal(e),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    dlq,
                    "failed to deserialize DLQ message, acking anyway"
                );
                BatchDecode::Dlq {
                    reason: "deserialization_error",
                    fail: metrics::FailReason::Deserialize,
                }
            }
        })
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
        self.run_consumer::<T, H>(handler, ctx, options, true).await
    }

    /// [`run_with_inner`](Self::run_with_inner) for a member of a registry
    /// group, which declared its topology before it spawned the member and
    /// so skips the startup topic check.
    pub(crate) async fn run_declared<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        self.run_consumer::<T, H>(handler, ctx, options, false)
            .await
    }

    async fn run_consumer<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptions,
        check_topics: bool,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let queue = topology.queue();
        // The direct and supervisor paths reach this function without the
        // subscriber's check, so a knob only the broadcast loop reads is
        // refused here rather than dropped.
        options.refuse_broadcast_start(queue, "KafkaConsumer::run")?;
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
        // The commit policy, the gate's window, and the fence threshold
        // that has to grow with the window - see `fence_threshold_over`. The
        // interval is checked again here although both setters already
        // check it: `ConsumerOptions::kafka_commit_interval` is a public
        // field, and a value written past the setter would reach the gate,
        // where a deadline the `Instant` cannot represent reads as due now
        // while `due()` still says no, so the wake arm would fire on every
        // pass with commit work pending. This is the one place the receive
        // loop reads the policy, so it is the one place to refuse it, with
        // a `Topology` error and never a panic, because this is a run path
        // and the setters have refused the same values at configuration
        // time already. It is also the one place every path passes through
        // for the constraint a per-record policy carries; see
        // `check_per_record_prefetch`.
        let commit_policy = options
            .kafka_commit_policy
            .unwrap_or(CommitPolicy::Interval(ASYNC_COMMIT_INTERVAL));
        check_commit_policy(commit_policy, queue)?;
        let per_record = matches!(commit_policy, CommitPolicy::PerRecord);
        if per_record {
            check_per_record_prefetch(options.prefetch_count, queue, "KafkaConsumer::run")?;
        }
        let fetch_queue = options.kafka_fetch_queue;
        fetch_queue.validate(queue)?;
        let fence_timeout =
            fence_threshold_over(fence_floor(&options), commit_window(commit_policy));
        #[cfg(feature = "test-support")]
        fence_probe::record(fence_timeout);
        let max_poll_interval_ms = max_poll_interval_ms(
            #[cfg(feature = "test-support")]
            options.kafka_max_poll_interval,
        );
        #[cfg(feature = "kafka-schema-registry")]
        let lookup_bound = registry_lookup_bound(max_poll_interval_ms);

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let max_retries = options.max_retries;
        let prefetch_count = options.prefetch_count.max(1);
        let handler_timeout = options.handler_timeout;
        let handler_timeout_outcome_cfg = options.handler_timeout_outcome.clone();
        let max_message_size = options.max_message_size;
        let hold_queues = topology.hold_queues();
        // How a `Retry` or a `Defer` is carried out here. Ownership implies
        // it: an infra-owned topic is never written into when an outcome is
        // settled, so it retries in place and refuses the republish, see
        // `redeliver_in_place`. A shove-owned topic republishes by default
        // and may opt into the in-place shape. Publishing is outside that
        // guarantee; see the Kafka page.
        let retry_strategy = resolve_retry_strategy(
            topology,
            options.retry_strategy,
            queue,
            "KafkaConsumer::run",
        )?;
        let in_place = retry_strategy == RetryStrategy::InPlace;

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();

        if check_topics {
            startup::verify(
                &client,
                topology,
                startup::Path::Publishing,
                &shutdown,
                options.max_reconnect_attempts,
            )
            .await?;
        }

        tracing::info!(
            queue,
            group_id,
            prefetch_count,
            max_retries,
            retry_strategy = ?retry_strategy,
            "Kafka consumer started"
        );

        let semaphore = Arc::new(Semaphore::new(prefetch_count as usize));
        // How many permits are held by handlers waiting out an in-place
        // delay right now, as opposed to running. Only external mode moves
        // it; see `InPlaceWait` and the pause below.
        let in_place_waiters = Arc::new(AtomicUsize::new(0));
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
        #[cfg(feature = "kafka-schema-registry")]
        let schema_message_index: Option<Arc<[i32]>> =
            options.schema_message_index.clone().map(Arc::from);

        let stop_failed = StopFailed::default();
        let stop_failed = &stop_failed;
        let max_attempts = options.max_reconnect_attempts;
        run_with_reconnect(&shutdown, queue, max_attempts, Some(stop_failed), || {
            let handler = handler.clone();
            let ctx = ctx.clone();
            let client = client.clone();
            let processing = processing.clone();
            let shutdown = shutdown.clone();
            let group_id = group_id.clone();
            let semaphore = semaphore.clone();
            let in_place_waiters = in_place_waiters.clone();
            let topic = topic.clone();
            let group = group.clone();
            let handler_timeout_outcome_cfg = handler_timeout_outcome_cfg.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_registry = schema_registry.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_accepted = schema_accepted.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_message_index = schema_message_index.clone();
            async move {
                // Fresh channel per (re)connect, matching the fresh
                // OffsetTracker below: rebalance events from a torn-down
                // consumer must not leak into the next connection's tracker.
                let (rebalance_tx, mut rebalance_rx) = std_mpsc::channel::<RebalanceEvent>();
                // The map the rebalance callback cancels revoked partitions'
                // tokens in; the tracker below shares it. See `AssignmentTokens`.
                let assignments = AssignmentTokens::default();
                // Closed on a thread of its own on every exit, and waited
                // for: through the final commit on a stop, through
                // `close_without_commit` on an `Err`, a failed subscribe
                // included. An abort leaves it to `LoopConsumer`'s drop.
                let consumer = LoopConsumer::new(
                    create_stream_consumer(
                        client.base_config(),
                        &group_id,
                        auto_offset_reset,
                        max_poll_interval_ms,
                        &fetch_queue,
                        queue,
                        rebalance_tx,
                        assignments.clone(),
                        #[cfg(feature = "kafka-msk-iam")]
                        client.msk_context(),
                    )?,
                    queue,
                );
                if let Err(e) = consumer.subscribe(&[queue]) {
                    let e = map_kafka_error("failed to subscribe", e);
                    close_without_commit(consumer, queue, SHUTDOWN_COMMIT_DEADLINE).await;
                    return Err(e);
                }

                let queue_owned = queue.to_string();
                // perf-K-6: OffsetTracker is touched only by this receive loop; handler
                // completions arrive via completion_tx/_rx. Drop the Mutex so the loop
                // owns the tracker directly — saves two async-lock acquisitions per
                // message (drain at top + track_received in the message branch).
                let mut tracker = OffsetTracker::with_assignments(queue_owned.clone(), assignments);
                // One slot per sender that can hold a completion while this
                // loop is not draining. The `prefetch_count` permit holders
                // are `prefetch_count` of them. The loop itself is one more:
                // a pre-handler discard signals through `signal_completion`
                // without a permit, and the loop sends at most one such
                // completion per iteration, so with every handler completion
                // already queued the loop's own send still fits. The longest
                // drain-free window is a registry stall, which holds the loop
                // for the whole outage without draining; a handler that
                // completed during it filled a `prefetch_count`-sized channel,
                // and the stalled record's own verdict was refused and its
                // offset lost. The bound stays, and so does `try_send`: the
                // loop is the only receiver, so a `send().await` from it would
                // deadlock, and a refusal past this count is a logic bug
                // surfaced at once rather than silently accumulating (sec-K-4).
                let (completion_tx, mut completion_rx) =
                    mpsc::channel::<Completion>(usize::from(prefetch_count).saturating_add(1));
                // A fault a handler task meets ends this loop the way the
                // loop ends when it meets the fault itself: at once, with
                // the error and the record uncommitted rather than pinned in
                // a consumer that keeps polling. Two kinds of fault report
                // here: a republish or a dead letter that failed and may
                // succeed later, under either commit policy, see
                // `route_outcome`, whose reconnect redelivers the record;
                // and a registry deployment fault met inside an in-place
                // redelivery, under `kafka-schema-registry`. The broadcast
                // loop owns the same channel. The sender stays alive here for
                // the loop's lifetime so the arm below stays pending.
                let (fault_tx, mut fault_rx) = mpsc::channel::<ShoveError>(1);
                // Handler tasks wait on this child rather than on `shutdown`
                // itself. A shutdown still reaches them through the parent,
                // and a fault lets the loop end every sibling's in-place wait
                // without cancelling the token the group or supervisor owns.
                let task_cancel = shutdown.child_token();

                // Periodic wake so rebalance events and commit retries are
                // drained even when no messages or completions arrive: the
                // callbacks push onto a channel that only the loop body
                // reads, and the loop body only runs when a select arm
                // completes. A no-op when nothing is pending.
                let mut housekeeping = tokio::time::interval(HOUSEKEEPING_INTERVAL);
                // The commit gate, and the commit in flight: under `Interval`
                // the one the loop has on its thread, see `CommitSlot`; under
                // `PerRecord` only the one shutdown cut short, set on the
                // pass that then takes the shutdown arm.
                let mut commits = CommitSlot::new(commit_policy);
                // External-topic pause discipline, see the check below the
                // commit gate: whether the assignment is paused, and a permit
                // acquired to end the pause, kept for the next record.
                let mut paused = false;
                let mut spare_permit: Option<tokio::sync::OwnedSemaphorePermit> = None;

                // The receive loop runs inside one block so that every exit
                // it takes lands on the tail below it: a clean shutdown
                // returns `Ok` from its own arm after its drain, with what
                // the final commit reads, and every other exit is fatal and
                // returns `Err`. The block owns the loop's state, the spare
                // permit included, so that state is dropped before the tail
                // runs. It borrows the consumer, which the tail closes; the
                // tail keeps its own handles.
                let tail_cancel = task_cancel.clone();
                let tail_semaphore = semaphore.clone();
                let tail_processing = processing.clone();
                let loop_consumer = &consumer;
                let ended: Result<Stopped> = async move {
                let consumer = loop_consumer;
                let fence = Fence {
                    threshold: fence_timeout,
                    group_id: &group_id,
                };
                loop {
                    // Completed offsets first, then the commit step, whose
                    // rebalance events drop a revoked partition's tracker,
                    // and the completions queued for it, before anything is
                    // committed; under `Interval` the step makes the commit
                    // too, see `CommitSlot::pass`.
                    while let Ok(completion) = completion_rx.try_recv() {
                        tracker.mark_complete(completion);
                    }
                    let assigned =
                        commits.pass(consumer, &mut tracker, &rebalance_rx, fence, queue)?;
                    let now = Instant::now();
                    #[cfg(feature = "test-support")]
                    if assigned {
                        per_record_probe::RECONCILED_AT_TOP_OF_PASS.fetch_add(1, Ordering::SeqCst);
                    }
                    reconcile_pause_after_assign(consumer, paused, assigned)?;
                    // The gate spaces commits out (see `ASYNC_COMMIT_INTERVAL`):
                    // ungated, this drain committed on nearly every iteration —
                    // roughly once per message at speed — piling up in-flight
                    // commits that `rd_kafka_consumer_close` later drains one
                    // reply at a time (tens of seconds after a large fast
                    // corpus). `drain_committable` is only *called* when due and
                    // with no commit in flight, so its side effects
                    // (dirty/quiet-drain accounting, pending discards) stay tied
                    // to actual commit offers. Only a drain that offered a
                    // commit consumes the window.
                    if per_record
                        && commits.in_flight.is_none()
                        && commits.gate.due(now)
                        && let Some(DrainedCommit {
                            tpl,
                            positions,
                            discards,
                        }) = tracker.drain_committable()
                    {
                        // Every completion is committed before the next
                        // record is handed out, and only a confirmed
                        // commit counts: the position is committed
                        // synchronously on a thread of its own and the
                        // loop waits for the coordinator's answer while
                        // it keeps polling, see `commit_confirmed`. The
                        // assignment has been paused since the record
                        // was handed out, so nothing is taken meanwhile.
                        // A rejected commit is re-offered as under
                        // `Interval`, through the dirty flag, with its
                        // discards handed back so the commit that lands
                        // retires them; the fence judges the streak, and
                        // the gate spaces the re-offers so the loop polls
                        // between them. The assignment stays paused until
                        // a commit is accepted, see the resume below.
                        match commit_confirmed(
                            consumer,
                            tpl,
                            &shutdown,
                            Some(&mut fault_rx),
                            queue,
                            &mut paused,
                            Duration::ZERO,
                        )
                        .await?
                        {
                            CommitAnswer::Confirmed => {
                                tracker.commit_accepted(&positions);
                                for discard in discards {
                                    discard.confirm();
                                }
                                commits.gate.mark(Instant::now());
                            }
                            CommitAnswer::Rejected(e) => {
                                tracing::warn!(
                                    queue,
                                    error = %e,
                                    offsets = ?position_offsets(&positions),
                                    "offset commit rejected; re-offering the position"
                                );
                                let now = Instant::now();
                                tracker.commit_rejected(&positions, discards, now);
                                commits.gate.mark_rejected(now);
                                // A rejected position holds the next
                                // record back whatever brought the loop
                                // here. A record taken paused the
                                // assignment already; a position
                                // re-offered after a rebalance was
                                // committed with nothing paused, and its
                                // rejection pauses the same way, until a
                                // re-offer is accepted.
                                if !paused {
                                    consumer
                                        .pause_assignment()
                                        .map_err(|e| map_kafka_error("pause failed", e))?;
                                    paused = true;
                                    tracing::info!(
                                        queue,
                                        "offset commit rejected; assignment paused until a re-offer is accepted"
                                    );
                                }
                            }
                            CommitAnswer::Cancelled(answer) => {
                                // Shutdown fired during the wait. The
                                // commit is still in flight on its
                                // thread; the shutdown arm, which this
                                // pass reaches next, waits for its answer
                                // and settles what rides on it before
                                // the final commit.
                                commits.in_flight = Some(PendingCommit {
                                    answer,
                                    positions,
                                    discards,
                                });
                            }
                        }
                    }

                    // Under `PerRecord` the assignment is paused from the
                    // moment a record is taken until the commit of its
                    // completion, or of its drop, is accepted, see the pause
                    // in the receive arm: librdkafka hands the next record to any
                    // `recv()`, and the loop must keep calling `recv()` to
                    // serve rebalance callbacks and its poll budget, so the
                    // one way to poll without taking a record is to pause.
                    // It resumes here, once nothing is left to commit on any
                    // partition and the permit is back in hand; a rejected
                    // commit keeps `has_committable` true until its re-offer
                    // lands, so no record is taken behind an unconfirmed
                    // position. A commit shutdown cut short is in flight, not
                    // committable, and nothing resumes behind it either: the
                    // shutdown arm wins the next select, and the resume would
                    // only hand librdkafka a fetch to purge. A pause purges
                    // the fetch queue and a resume refetches from the
                    // position, so every record costs a fetch round trip
                    // beside its commit, the price of the policy.
                    if per_record
                        && paused
                        && spare_permit.is_some()
                        && commits.in_flight.is_none()
                        && !tracker.has_committable()
                    {
                        consumer
                            .resume_assignment()
                            .map_err(|e| map_kafka_error("resume failed", e))?;
                        paused = false;
                        tracing::debug!(queue, "commit confirmed; assignment resumed");
                    }

                    // On an infra-owned topic a waiting handler holds its
                    // permit for the whole delay. Parking this loop on the
                    // semaphore would stop it polling, and a park past
                    // `max.poll.interval.ms` has the member evicted. So while
                    // no permit is free and at least one is held by a handler
                    // waiting out a delay, the assignment is paused and the
                    // loop keeps calling `recv()`, which yields nothing but
                    // keeps the member alive; the first permit to free resumes
                    // it and is kept for the next record. Permits held only by
                    // running handlers do not pause by themselves: a pause
                    // purges the fetch queue and refetches on resume, a cost
                    // worth paying for a delay of seconds but not for every
                    // busy moment. A record that arrives while every permit is
                    // held by a running handler is decoded and waits for a
                    // permit while the loop keeps polling, and a further
                    // record during that wait is put back and the assignment
                    // paused: see `acquire_permit_while_polling`.
                    if in_place
                        && !paused
                        && spare_permit.is_none()
                        && semaphore.available_permits() == 0
                        && in_place_waiters.load(Ordering::Acquire) > 0
                    {
                        consumer
                            .pause_assignment()
                            .map_err(|e| map_kafka_error("pause failed", e))?;
                        paused = true;
                        tracing::info!(
                            queue,
                            "every prefetch permit is held by a waiting handler; assignment paused"
                        );
                    }

                    // `biased`, shutdown first: a tie between ready arms is
                    // broken in the order written rather than at random.
                    // Under `PerRecord` a shutdown that cuts a confirmation
                    // wait short leaves the shutdown arm and the permit arm
                    // ready together, and the shutdown arm must win that
                    // tie, or the pass that follows would take the permit
                    // and reach `recv()` with the token cancelled.
                    tokio::select! {
                        // Read in this order, the stop first. A permit the
                        // stop itself freed, from a cancelled in-place wait,
                        // readies the resume arm below in the same instant,
                        // and an unbiased pick could take it, and on the
                        // next pass the record behind the waiting one, before
                        // the stop is seen. A record `recv()` has ready is
                        // read last for the same reason.
                        biased;
                        _ = shutdown.cancelled() => {
                            tracing::info!(queue, "shutdown signal received, draining in-flight tasks");
                            // A spare permit would otherwise deadlock the drain below.
                            drop(spare_permit.take());
                            // Polls throughout, so a long handler costs no eviction.
                            let waited = acquire_permit_while_polling(
                                &semaphore,
                                u32::from(prefetch_count),
                                consumer,
                                None,
                                None,
                                queue,
                                &mut paused,
                                None,
                            )
                            .await;
                            drop(stop_failed.raise_if_failed(waited, queue)?);
                            // Every task has exited. A task that ended on a
                            // cancelled wait may not have cleared the busy
                            // flag itself: a permit it releases goes to the
                            // queued wait above first, so its own
                            // availability check need not see the full count.
                            processing.store(false, Ordering::Release);
                            while let Ok(completion) = completion_rx.try_recv() {
                                tracker.mark_complete(completion);
                            }
                            return Ok(Stopped {
                                tracker,
                                rebalance_rx,
                                pending_commit: commits.in_flight.take(),
                            });
                        }
                        fault = fault_rx.recv() => {
                            // `None` cannot happen while `fault_tx` lives in
                            // this scope; the arm is total anyway.
                            let Some(e) = fault else { continue };
                            tracing::error!(
                                error = %e,
                                queue,
                                "fault reported by a handler task; ending the consumer"
                            );
                            // The tail below the loop ends every sibling
                            // task and drains the permits before the error
                            // is returned.
                            return Err(e);
                        }
                        // Falls through to the top-of-loop drain — see the
                        // comment on `housekeeping` above.
                        _ = housekeeping.tick() => {}
                        // Wakes the loop when the commit gate's window expires
                        // with commit work pending, so a gated commit lands
                        // within ~ASYNC_COMMIT_INTERVAL even on a topic gone
                        // idle — without this arm it would wait for the next
                        // housekeeping tick, regressing the idle-topic
                        // redelivery bound (see the completion arm below) from
                        // the gate interval to HOUSEKEEPING_INTERVAL. Disabled
                        // when nothing is committable, so an idle consumer does
                        // not wake every interval, and while a commit is in
                        // flight, whose answer arm below wakes the loop
                        // instead; no busy-spin when past due, because the
                        // top-of-loop drain then commits and `mark` pushes the
                        // deadline forward. Cost note:
                        // select! evaluates this expression (a `Sleep`
                        // construct-and-drop plus the `has_committable` scan)
                        // on every pass, even when the precondition disables
                        // the arm; the timer only registers when the arm is
                        // enabled, and the whole thing displaced a per-message
                        // commit FFI call — the e2e cell's throughput went UP
                        // ~26k -> ~30.5k msg/s with the gate in place.
                        _ = tokio::time::sleep_until(commits.gate.deadline(now)),
                            if commits.in_flight.is_none() && tracker.has_committable() => {}
                        // The answer of the commit in flight, settled at once:
                        // a rejection marks its partitions to re-offer, so the
                        // wake above is due once the gate allows it.
                        answer = commit_answer(commits.in_flight.as_mut()),
                            if commits.in_flight.is_some() => {
                            commits.settle(answer, &mut tracker, queue);
                        }
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
                        // The end of a pause the permit wait or a waiting
                        // in-place handler left: the first permit to free
                        // ends it, and is kept for the next record. Not under
                        // `PerRecord`, whose pause ends only once a commit is
                        // accepted, see the resume at the top of the pass.
                        permit = semaphore.clone().acquire_owned(), if paused && !per_record => {
                            let permit = permit.map_err(|_| {
                                ShoveError::Connection("semaphore closed".to_string())
                            })?;
                            consumer
                                .resume_assignment()
                                .map_err(|e| map_kafka_error("resume failed", e))?;
                            paused = false;
                            spare_permit = Some(permit);
                            tracing::info!(queue, "a prefetch permit freed; assignment resumed");
                        }
                        // Under `PerRecord` the one permit is taken back
                        // here as soon as its handler frees it, and kept for
                        // the next record. The handler sends its completion
                        // before it frees the permit, so the pass that
                        // follows drains that completion and commits it; the
                        // resume at the top of a pass needs the permit in
                        // hand and nothing left to commit.
                        permit = semaphore.clone().acquire_owned(),
                            if per_record && spare_permit.is_none() => {
                            let permit = permit.map_err(|_| {
                                ShoveError::Connection("semaphore closed".to_string())
                            })?;
                            spare_permit = Some(permit);
                        }
                        msg_result = consumer.recv() => {
                            let msg = match msg_result {
                                Ok(msg) => msg,
                                Err(e) if recv_error_is_transient(consumer.transport_gate(), &e) => {
                                    log_transient_recv_error(queue, &e);
                                    continue;
                                }
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
                            let headers = RecordHeaders::read(&msg);
                            let partition = msg.partition();
                            let offset = msg.offset();
                            let key = msg.key().map(Bytes::copy_from_slice);

                            // The rebalance callback runs inside consumer.recv()'s
                            // poll, so an Assign event for this partition may
                            // already be queued when its first post-reassignment
                            // message arrives from the same poll. Apply pending
                            // events BEFORE tracking — otherwise the next
                            // iteration's drain would wipe the tracker entry this
                            // message is about to seed.
                            let assigned =
                                tracker.apply_rebalance_events(&rebalance_rx);
                            #[cfg(feature = "test-support")]
                            if assigned {
                                per_record_probe::ASSIGNS_DRAINED_AT_RECEIVE_ARM
                                    .fetch_add(1, Ordering::SeqCst);
                            }
                            reconcile_pause_after_assign(consumer, paused, assigned)?;

                            // A paused assignment delivers nothing it held when
                            // the pause took effect, so a record that arrives
                            // while `paused` comes from a partition the group
                            // assigned afterwards. Taking it would hold this
                            // loop in a wait for a permit, the very wait the
                            // pause exists to avoid. It is put back instead and
                            // the pause is widened to the new assignment;
                            // resuming delivers it again. On an infra-owned
                            // topic the same holds when the pause is due but
                            // not yet applied: every permit is held and one
                            // holder started waiting after the check at the
                            // top of this pass.
                            if paused
                                || (in_place
                                    && spare_permit.is_none()
                                    && semaphore.available_permits() == 0
                                    && in_place_waiters.load(Ordering::Acquire) > 0)
                            {
                                #[cfg(feature = "test-support")]
                                put_back_probe::PAUSED_RECEIVE.fetch_add(1, Ordering::SeqCst);
                                put_back(consumer, queue, partition, offset)?;
                                consumer
                                    .pause_assignment()
                                    .map_err(|e| map_kafka_error("pause failed", e))?;
                                if !paused {
                                    paused = true;
                                    tracing::info!(
                                        queue,
                                        "every prefetch permit is held by a waiting handler; assignment paused"
                                    );
                                }
                                tracing::debug!(
                                    queue,
                                    partition,
                                    offset,
                                    "record delivered during the pause put back; pause widened to the current assignment"
                                );
                                continue;
                            }
                            let assignment = tracker.track_received(partition, offset);

                            // Under `PerRecord` nothing may be taken until
                            // the commit of this record's completion is
                            // accepted, and the loop keeps polling meanwhile,
                            // so the assignment is paused here, the moment
                            // the record is taken and before anything decides
                            // its fate. A record dropped before the handler,
                            // oversize or undecodable, commits its position
                            // like a completion and must hold the next record
                            // back the same way, so the pause cannot wait for
                            // the handler spawn below. Resumed at the top of
                            // the pass that finds nothing left to commit; see
                            // the resume for the reasoning.
                            if per_record && !paused {
                                consumer
                                    .pause_assignment()
                                    .map_err(|e| map_kafka_error("pause failed", e))?;
                                paused = true;
                                tracing::debug!(
                                    queue,
                                    partition,
                                    offset,
                                    "record taken; assignment paused until its commit is accepted"
                                );
                            }

                            metrics::record_message_size(&topic, group.as_deref(), payload_slice.len());

                            // Reject oversized messages before deserialization
                            if let Err(e) = validate_message_size(payload_slice.len(), max_message_size) {
                                tracing::warn!(
                                    error = %e,
                                    queue,
                                    "rejecting oversized message to DLQ"
                                );
                                discard_pre_handler(
                                    &client,
                                    topology,
                                    &topic,
                                    group.as_deref(),
                                    metrics::FailReason::Oversize,
                                    &e.to_string(),
                                    payload_slice,
                                    key.as_deref(),
                                    &headers,
                                    &completion_tx,
                                    partition,
                                    offset,
                                    assignment.epoch,
                                )
                                .await?;
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
                                let staged = match WireFormat::from_codec_name(codec_name) {
                                    Some(fmt) => {
                                        // An unavailable registry stalls this
                                        // record instead of discarding it; see
                                        // `RegistryStall`. Under `PerRecord`
                                        // the take above paused the assignment
                                        // already, and the stall leaves that
                                        // pause, and its resume, to the commit.
                                        let mut stall = RegistryStall::inside_pause(paused);
                                        let staged = loop {
                                            let Some(result) = decode_or_shutdown(
                                                registry_decode::<T::Message, T::Codec>(
                                                    registry,
                                                    fmt,
                                                    schema_enforcement,
                                                    &schema_accepted,
                                                    schema_message_index.as_deref(),
                                                    lookup_bound,
                                                    payload_slice,
                                                ),
                                                &shutdown,
                                            )
                                            .await
                                            else {
                                                break Staged::Stop;
                                            };
                                            match result {
                                                Ok(RegistryDecode::Decoded(m)) => break Staged::Ready(m),
                                                Ok(RegistryDecode::Dlq(reason)) => break Staged::Drop(
                                                    metrics::FailReason::for_schema_reason(reason),
                                                    reason,
                                                ),
                                                Ok(RegistryDecode::Unavailable { id, error }) => {
                                                    if !stall
                                                        .wait(consumer, &shutdown, Some(&mut fault_rx), &topic, group.as_deref(), queue, id, &error)
                                                        .await?
                                                    {
                                                        break Staged::Stop;
                                                    }
                                                    // The wait polled `recv()`, inside which a
                                                    // revoke of this record's partition cancels
                                                    // its token: read before the decode is
                                                    // retried, or the record would reach the
                                                    // handler as the old owner's.
                                                    match after_wait(&shutdown, Some(&assignment.revoked)) {
                                                        AfterWait::Dispatch => {}
                                                        AfterWait::Stop => break Staged::Stop,
                                                        AfterWait::Revoked => break Staged::Revoked,
                                                    }
                                                }
                                                // A deployment fault, not an outage: stop
                                                // instead of waiting on it.
                                                Err(e @ ShoveError::Topology(_)) => return Err(e),
                                                Err(e) => {
                                                    tracing::error!(
                                                        error = %e,
                                                        queue,
                                                        "failed to deserialize message, sending to DLQ"
                                                    );
                                                    break Staged::Drop(
                                                        metrics::FailReason::Deserialize,
                                                        "deserialization_error",
                                                    );
                                                }
                                            }
                                        };
                                        stall.end(consumer)?;
                                        staged
                                    }
                                    None => {
                                        tracing::error!(
                                            codec = codec_name,
                                            queue,
                                            "codec has no Confluent wire format; routing to DLQ"
                                        );
                                        Staged::Drop(
                                            metrics::FailReason::for_schema_reason("schema_unsupported_codec"),
                                            "schema_unsupported_codec",
                                        )
                                    }
                                };
                                match staged {
                                    Staged::Ready(m) => m,
                                    Staged::Drop(fail, reason) => {
                                        discard_pre_handler(
                                            &client,
                                            topology,
                                            &topic,
                                            group.as_deref(),
                                            fail,
                                            reason,
                                            payload_slice,
                                            key.as_deref(),
                                            &headers,
                                            &completion_tx,
                                            partition,
                                            offset,
                                            assignment.epoch,
                                        )
                                        .await?;
                                        continue;
                                    }
                                    // The record is untouched; the receive loop's
                                    // `shutdown.cancelled()` arm drains and commits
                                    // the rest on its next pass.
                                    Staged::Stop => continue,
                                    // Untouched here too: the partition's next
                                    // owner is handed the record, and the next
                                    // pass drains the revoke.
                                    Staged::Revoked => {
                                        tracing::debug!(
                                            queue,
                                            partition,
                                            offset,
                                            "partition revoked during a schema registry wait; the record is left to its next owner"
                                        );
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
                                        discard_pre_handler(
                                            &client,
                                            topology,
                                            &topic,
                                            group.as_deref(),
                                            metrics::FailReason::Deserialize,
                                            "deserialization_error",
                                            payload_slice,
                                            key.as_deref(),
                                            &headers,
                                            &completion_tx,
                                            partition,
                                            offset,
                                            assignment.epoch,
                                        )
                                        .await?;
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
                                    discard_pre_handler(
                                        &client,
                                        topology,
                                        &topic,
                                        group.as_deref(),
                                        metrics::FailReason::Deserialize,
                                        // sec-K-5: do NOT append the codec error message to
                                        // the DLQ death-reason header — serde_json errors can
                                        // carry fragments of attacker-controlled payload bytes.
                                        // The full error is recorded via tracing above.
                                        "deserialization_error",
                                        payload_slice,
                                        key.as_deref(),
                                        &headers,
                                        &completion_tx,
                                        partition,
                                        offset,
                                        assignment.epoch,
                                    )
                                    .await?;
                                    continue;
                                }
                            };

                            // Decode succeeded — copy bytes for the spawned task's
                            // route_outcome (msg goes out of scope after this loop iteration).
                            let payload_bytes = payload_slice.to_vec();

                            let coordinates = RecordCoordinates::of(&msg);
                            let metadata = build_message_metadata(&headers.strings, false, coordinates);
                            let retry_count = metadata.retry_count;

                            let permit = match spare_permit.take() {
                                Some(permit) => permit,
                                // Every permit may be held, by running
                                // handlers, by in-place waits or by republishes
                                // waiting out their hold, for as long as a hold
                                // lasts, so the wait keeps polling, and commits
                                // under `Interval`; see the helper.
                                None => {
                                    let commits = (!per_record).then_some(IntervalCommits {
                                        tracker: &mut tracker,
                                        completion_rx: &mut completion_rx,
                                        rebalance_rx: &mut rebalance_rx,
                                        slot: &mut commits,
                                        fence,
                                    });
                                    #[cfg(feature = "test-support")]
                                    permit_wait_probe::ENTERED.fetch_add(1, Ordering::SeqCst);
                                    match acquire_permit_while_polling(
                                        &semaphore,
                                        1,
                                        consumer,
                                        Some(&shutdown),
                                        Some(&mut fault_rx),
                                        queue,
                                        &mut paused,
                                        commits,
                                    )
                                    .await?
                                    {
                                        Some(permit) => permit,
                                        // Shutdown fired first. The record is
                                        // tracked and never completed, so it
                                        // stays uncommitted; the shutdown arm
                                        // takes over on the next pass.
                                        None => continue,
                                    }
                                }
                            };
                            #[cfg(feature = "test-support")]
                            permit_wait_probe::acquired();
                            // Read again with the permit in hand, see
                            // `after_wait`. A record left here is tracked and
                            // never completed, so the position stays below it:
                            // a restart delivers it behind the record that
                            // was waiting, and the partition's next owner is
                            // handed it from the committed offset.
                            match after_wait(&shutdown, Some(&assignment.revoked)) {
                                AfterWait::Dispatch => {}
                                AfterWait::Stop => {
                                    drop(permit);
                                    tracing::debug!(
                                        queue,
                                        partition,
                                        offset,
                                        "shutdown fired while this record waited for a permit; left uncommitted for the restart"
                                    );
                                    continue;
                                }
                                // The tracker entry goes with the queued
                                // revoke event on the next pass. The busy flag
                                // is cleared here when no task holds a permit:
                                // the ended wait's task cleared it only if its
                                // permit was still free when it looked.
                                AfterWait::Revoked => {
                                    drop(permit);
                                    if semaphore.available_permits() == prefetch_count as usize {
                                        processing.store(false, Ordering::Release);
                                    }
                                    tracing::debug!(
                                        queue,
                                        partition,
                                        offset,
                                        "partition revoked while this record waited for a permit; left to its next owner"
                                    );
                                    continue;
                                }
                            }

                            let task_client = client.clone();
                            let task_processing = processing.clone();
                            let task_semaphore = semaphore.clone();
                            let task_prefetch = prefetch_count;
                            let task_tx = completion_tx.clone();
                            let task_topic = topic.clone();
                            let task_handler = handler.clone();
                            let task_ctx = ctx.clone();
                            let task_group = group.clone();
                            let task_shutdown = task_cancel.clone();
                            let task_waiters = in_place_waiters.clone();
                            let task_timeout_outcome = handler_timeout_outcome_cfg.clone();
                            let task_fault = fault_tx.clone();

                            #[cfg(feature = "kafka-schema-registry")]
                            let task_schema_registry = schema_registry.clone();
                            #[cfg(feature = "kafka-schema-registry")]
                            let task_schema_accepted = schema_accepted.clone();
                            #[cfg(feature = "kafka-schema-registry")]
                            let task_schema_message_index = schema_message_index.clone();
                            #[cfg(feature = "kafka-schema-registry")]
                            let task_lookup_bound = lookup_bound;

                            // perf-K-7: single spawn per message (was three).
                            // invoke_handler awaits the handler with catch_unwind +
                            // timeout in-place, then route_outcome runs in the same
                            // task — no inner spawn, no oneshot relay.
                            tokio::spawn(async move {
                                task_processing.store(true, Ordering::Release);

                                let outcome = {
                                    let handler = Arc::clone(&task_handler);
                                    let ctx = Arc::clone(&task_ctx);
                                    invoke_handler(
                                        async move { handler.handle(payload, metadata, ctx.as_ref()).await },
                                        handler_timeout,
                                        task_timeout_outcome.clone(),
                                        &task_topic,
                                        task_group.as_deref(),
                                    )
                                    .await
                                };

                                // Under `RetryStrategy::InPlace` the Hold arms
                                // of `route_outcome`, which republish, are not
                                // taken: `Retry` and `Defer` are resolved here
                                // first, in place and holding the permit, and
                                // only a terminal outcome goes on to
                                // `route_outcome`.
                                let (outcome, retry_count) = if in_place {
                                    let decode = BatchDecodeCtx {
                                        queue: &task_topic,
                                        #[cfg(feature = "kafka-schema-registry")]
                                        schema_registry: task_schema_registry.as_ref(),
                                        #[cfg(feature = "kafka-schema-registry")]
                                        schema_enforcement,
                                        #[cfg(feature = "kafka-schema-registry")]
                                        schema_accepted: task_schema_accepted.as_ref(),
                                        #[cfg(feature = "kafka-schema-registry")]
                                        schema_message_index: task_schema_message_index.as_deref(),
                                        #[cfg(feature = "kafka-schema-registry")]
                                        registry_lookup_bound: task_lookup_bound,
                                    };
                                    match redeliver_in_place::<T, H>(
                                        &task_handler,
                                        &task_ctx,
                                        &decode,
                                        &payload_bytes,
                                        &headers.strings,
                                        coordinates,
                                        outcome,
                                        retry_count,
                                        max_retries,
                                        hold_queues,
                                        handler_timeout,
                                        task_timeout_outcome,
                                        &task_topic,
                                        task_group.as_deref(),
                                        &task_shutdown,
                                        &assignment.revoked,
                                        &task_waiters,
                                    )
                                    .await
                                    {
                                        InPlaceEnd::Terminal { outcome, attempts } => (outcome, attempts),
                                        InPlaceEnd::Undecodable { reason, fail, attempts } => {
                                            tracing::error!(
                                                queue = %task_topic,
                                                reason,
                                                "record no longer decodes for its in-place redelivery; dropping it as a pre-handler discard"
                                            );
                                            // The dead letter carries the attempts made in memory, as an
                                            // exhausted in-place retry does, not the count the record
                                            // arrived with.
                                            let mut dead_headers = headers;
                                            Arc::make_mut(&mut dead_headers.strings)
                                                .insert(RETRY_COUNT_HEADER.to_string(), attempts.to_string());
                                            if let Err(e) = discard_pre_handler(
                                                &task_client,
                                                topology,
                                                &task_topic,
                                                task_group.as_deref(),
                                                fail,
                                                reason,
                                                &payload_bytes,
                                                key.as_deref(),
                                                &dead_headers,
                                                &task_tx,
                                                partition,
                                                offset,
                                                assignment.epoch,
                                            )
                                            .await
                                            {
                                                // A full channel means a sibling
                                                // already reported a fault.
                                                let _ = task_fault.try_send(e);
                                            }
                                            drop(permit);
                                            if task_semaphore.available_permits() == task_prefetch as usize {
                                                task_processing.store(false, Ordering::Release);
                                            }
                                            return;
                                        }
                                        InPlaceEnd::Cancelled => {
                                            // Nothing is completed: the offset stays
                                            // uncommitted and the loop's shutdown or
                                            // fault arm collects this permit, then
                                            // clears the busy flag itself once the
                                            // drain proves every task has ended.
                                            drop(permit);
                                            return;
                                        }
                                        InPlaceEnd::Revoked => {
                                            // Nothing is completed: the partition's
                                            // next owner is handed the record from
                                            // the committed offset. The loop runs on,
                                            // so the busy flag is cleared here when no
                                            // other task holds a permit.
                                            drop(permit);
                                            if task_semaphore.available_permits() == task_prefetch as usize {
                                                task_processing.store(false, Ordering::Release);
                                            }
                                            return;
                                        }
                                        #[cfg(feature = "kafka-schema-registry")]
                                        InPlaceEnd::Fatal(e) => {
                                            tracing::error!(
                                                queue = %task_topic,
                                                error = %e,
                                                "schema registry deployment fault during an in-place redelivery; ending the consumer"
                                            );
                                            // Cleanup before the report: the loop's fault
                                            // arm drains every permit, and a permit still
                                            // held here would hold that drain up.
                                            drop(permit);
                                            if task_semaphore.available_permits() == task_prefetch as usize {
                                                task_processing.store(false, Ordering::Release);
                                            }
                                            // A full channel means a sibling already
                                            // reported the same fault.
                                            let _ = task_fault.try_send(e);
                                            return;
                                        }
                                    }
                                } else {
                                    (outcome, retry_count)
                                };

                                // sec-K-8: the permit is held until
                                // route_outcome returns, a republish's hold
                                // delay included, so delayed work stays bounded
                                // by the prefetch limit.
                                //
                                // Completion signaling lives inside route_outcome:
                                // terminal outcomes signal at once; a republish
                                // signals only once the copy landed. Without this
                                // gating the offset would commit before the
                                // republish landed, silently dropping the message
                                // on republish failure.
                                route_outcome(
                                    &task_client,
                                    &task_topic,
                                    task_group.as_deref(),
                                    &payload_bytes,
                                    key.as_deref(),
                                    &headers,
                                    outcome,
                                    topology,
                                    retry_count,
                                    max_retries,
                                    hold_queues,
                                    (task_tx, partition, offset, assignment.epoch),
                                    &task_shutdown,
                                    &assignment.revoked,
                                    &task_fault,
                                )
                                .await;
                                drop(permit);

                                if task_semaphore.available_permits() == task_prefetch as usize {
                                    task_processing.store(false, Ordering::Release);
                                }
                            });
                        }
                    }
                }
                }
                .await;
                // A stop ends with the final commit, made with what the
                // shutdown arm handed back. Every fatal exit, whether the
                // fault arm took it, the loop met a registry deployment fault
                // itself, a seek or pause failed, or the connection was lost,
                // closes the consumer first, on a thread of its own and
                // waited for, so the member has left its group when the
                // error is returned, see `close_without_commit`. Then the
                // child token ends every sibling task's in-place wait, the
                // permit drain is the only aggregate proof that detached
                // tasks have released their permits, and only then is the
                // busy flag cleared. Nothing is committed on that path: every
                // task's record stays uncommitted and is redelivered on
                // restart, or by the reconnect a retryable error leads to.
                // The spare permit, if one was held, went with the block's
                // state, so the drain cannot wait on it.
                let Stopped {
                    mut tracker,
                    mut rebalance_rx,
                    pending_commit,
                } = match ended {
                    Ok(stopped) => stopped,
                    Err(e) => {
                        close_without_commit(consumer, queue, SHUTDOWN_COMMIT_DEADLINE).await;
                        tail_cancel.cancel();
                        let _ = tail_semaphore.acquire_many(prefetch_count as u32).await;
                        tail_processing.store(false, Ordering::Release);
                        return Err(e);
                    }
                };
                stop_with_final_commit(
                    consumer,
                    &mut tracker,
                    &mut rebalance_rx,
                    pending_commit,
                    queue,
                )
                .await
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
    /// - `Ack` retires the whole batch, and its offsets go out with the
    ///   consumer's next commit. Commits are made as the standard consumer
    ///   makes them under the default `CommitPolicy::Interval`: at most every
    ///   500 ms, one at a time on a thread of their own while the consumer
    ///   goes on, and a commit the coordinator rejects, such as one made while
    ///   the group rebalances, is re-offered.
    /// - `Reject` is terminal: the batch is published to the DLQ (if the
    ///   topology declares one) and its offsets commit, matching what `Reject`
    ///   means on every other consumer path. That commit is synchronous, and
    ///   the flush waits for its answer while the consumer keeps polling,
    ///   because the discards ride on it.
    /// - `Retry` / `Defer` redeliver the **entire batch** after an escalating
    ///   delay. There is no per-batch retry counter, so a handler that never
    ///   acks redelivers forever — that is the intended back-pressure for a
    ///   sink that is down, and the escalating delay bounds the spin.
    ///
    /// A rejected batch's dead letters are published concurrently, at most 16
    /// at a time, and the consumer keeps polling while they run, so a slow
    /// DLQ does not have it evicted at `max.poll.interval.ms`. Running
    /// concurrently, a partition's dead letters do not reach the DLQ in
    /// offset order. A stop that lands meanwhile waits for them. A dead
    /// letter that fails and may land later retires nothing: nothing is
    /// committed past its record, the record's partition is sought back to
    /// it, and the batch is handed over again from there after the same
    /// escalating delay, with no discard counted. A dead letter that can
    /// never be published, refused as too large or as invalid, is discarded
    /// as lost, counted and logged. One that fails with an error no retry
    /// gets past, such as a producer that cannot be built, commits nothing of
    /// the batch and ends the consumer with that error.
    ///
    /// Dead letters are at-least-once. A rebalance while they run, a dead
    /// letter that fails, and a refused commit whose re-offer has not landed
    /// when the partition moves can each hand the batch over again, and its
    /// letters that had landed are then published again.
    ///
    /// A synchronous commit the coordinator rejects does not end the
    /// consumer, and the batch is not handed over again: once its dead
    /// letters have landed the batch is settled, so its position is
    /// re-offered as an acked batch's is, with its discards riding on the
    /// re-offer. The consumer keeps polling, so a rebalance behind the
    /// rejection can complete. A consumer whose commits keep being rejected,
    /// none accepted and no rebalance resolving them, is treated as fenced
    /// and reconnects, as the standard consumer does.
    ///
    /// # Poison handling
    ///
    /// A message that fails to deserialize (including the schema-registry
    /// frame-strip and subject gate under `kafka-schema-registry`) or exceeds
    /// `options.max_message_size` is dropped from the batch and published to
    /// the DLQ byte-for-byte, and its offset is committed with the batch. The
    /// DLQ publish is deferred until the batch settles, so a batch that gets
    /// redelivered does not multiply its poison in the DLQ, and a batch with a
    /// drop in it commits synchronously, as a rejected one does. With no DLQ
    /// declared the payload is discarded at the drop rather than buffered to
    /// the flush, but the offset is committed past either way.
    ///
    /// # Stopping
    ///
    /// A stop flushes the batch in hand first. That flush is not inside the
    /// shutdown deadline: it waits for the handler and for the batch's dead
    /// letters. The commits that follow are bounded as the standard
    /// consumer's are. A commit in flight when the stop lands, the flushed
    /// batch's own included, is waited for at most 15 s, and every retired
    /// position is then committed synchronously and the consumer closed,
    /// both on a thread of their own, within what is left of the 20 s
    /// shutdown deadline. A final commit refused while the group
    /// rebalances, as one made during another member's stop can be, is
    /// tried again inside that deadline once the consumer has polled. A
    /// commit in flight with no answer in time, and a final commit the broker
    /// does not confirm, end the run with `ShoveError::Commit`. Any other
    /// error closes the consumer on a thread of its own with nothing
    /// committed, and the error is returned once the close has run or the
    /// same deadline has passed.
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
    /// [`DEFAULT_HANDLER_TIMEOUT`](crate::DEFAULT_HANDLER_TIMEOUT), 30 s) resolves to `Retry` too, but that is
    /// a *default*, not a rule:
    /// [`BatchConsumerOptions::with_handler_timeout_outcome`] can make it any
    /// `Outcome`, and `Ack`/`Reject` commit or dead-letter the entire batch
    /// rather than redelivering it. See that method for the batch-wide outcome
    /// table — the mapping differs from the single-message path, and `Defer`
    /// in particular is indistinguishable from `Retry` here.
    /// [`BatchConsumerOptions::without_handler_timeout`] opts out of the
    /// deadline entirely, at the cost of making a hung flush unrecoverable.
    ///
    /// Nothing polls while the handler runs, so the handler call must finish
    /// inside `max.poll.interval.ms`, five minutes; the flush's dead-letter
    /// and commit waits poll, so they are not held to it. `handler_timeout`
    /// is the only bound on the handler call: a timeout above that interval,
    /// or none at all through `without_handler_timeout`, lets a slow handler
    /// call outlast it, and the member is then evicted from its group, its
    /// partitions handed to another member, which is handed the batch again.
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
    ///   retained partitions, to the new owner for revoked ones — once each,
    ///   in offset order. A batch abandoned while its dead letters were
    ///   landing was handed to the handler already, so its messages are
    ///   handled twice. Every flush reads the rebalance first, the one a
    ///   batch's age or a stop triggers included. Expect duplicates across a
    ///   rebalance when an acked batch's commit had not landed yet, never
    ///   loss. If a partition this member still holds cannot be sought back,
    ///   after a rebalance or for a redelivery, the consumer reconnects rather
    ///   than poll on past its uncommitted messages; a seek that fails on a
    ///   partition the group revoked in the same poll is left to its next
    ///   owner.
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
        self.run_batch_inner::<T, H>(handler, ctx, options.into_inner())
            .await
    }

    /// The body of [`run_batch`](Self::run_batch), taking the un-generic
    /// [`BatchConsumerOptionsInner`] so it can also serve as this backend's
    /// [`BatchConsumerImpl::run_batch`](crate::backend::BatchConsumerImpl::run_batch)
    /// implementation. See `run_batch` for the full contract — this method's
    /// behaviour is unchanged from before the split, only its options type.
    pub(crate) async fn run_batch_inner<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptionsInner,
    ) -> Result<()>
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>,
    {
        let topology = T::topology();
        let queue = topology.queue();
        // The public `run_batch` delegate above does not pass through
        // `BatchConsumer::run` (the generic wrapper), so this is its only
        // guard — not a redundant second check of one `BatchConsumer::run`
        // already performed. One definition (`validate_batch_topic`), two
        // call sites; see the trait doc for why both are load-bearing.
        validate_batch_topic::<T>()?;
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
        let max_poll_interval_ms = max_poll_interval_ms(
            #[cfg(feature = "test-support")]
            options.kafka_max_poll_interval,
        );
        let housekeeping_interval = batch_housekeeping_interval(&options);
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
        #[cfg(feature = "kafka-schema-registry")]
        let schema_message_index: Option<Arc<[i32]>> =
            options.schema_message_index.clone().map(Arc::from);

        startup::verify(
            &client,
            topology,
            startup::Path::Publishing,
            &shutdown,
            options.max_reconnect_attempts,
        )
        .await?;

        tracing::info!(
            queue,
            group_id,
            max_batch_size,
            ?max_batch_age,
            "Kafka batch consumer started"
        );

        run_with_reconnect(&shutdown, queue, options.max_reconnect_attempts, None, || {
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
            #[cfg(feature = "kafka-schema-registry")]
            let schema_message_index = schema_message_index.clone();
            async move {
                let (rebalance_tx, rebalance_rx) = std_mpsc::channel::<RebalanceEvent>();
                let assignments = AssignmentTokens::default();
                let consumer = LoopConsumer::new(
                    create_stream_consumer(
                        client.base_config(),
                        &group_id,
                        auto_offset_reset,
                        max_poll_interval_ms,
                        &FetchQueueTuning::default(),
                        queue,
                        rebalance_tx,
                        assignments.clone(),
                        #[cfg(feature = "kafka-msk-iam")]
                        client.msk_context(),
                    )?,
                    queue,
                );
                if let Err(e) = consumer.subscribe(&[queue]) {
                    let e = map_kafka_error("failed to subscribe", e);
                    close_without_commit(consumer, queue, SHUTDOWN_COMMIT_DEADLINE).await;
                    return Err(e);
                }
                let mut state = BatchState {
                    tracker: OffsetTracker::with_assignments(queue.to_string(), assignments),
                    rebalance_rx,
                    commits: CommitSlot::new(CommitPolicy::Interval(ASYNC_COMMIT_INTERVAL)),
                    fence_threshold: fence_threshold_over(
                        COMMIT_FENCE_TIMEOUT,
                        ASYNC_COMMIT_INTERVAL,
                    ),
                    group_id: group_id.clone(),
                    paused: false,
                };

                // `Ok` is a stop, which ends with the final commit below.
                let ended: Result<()> = async {
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
                        #[cfg(feature = "kafka-schema-registry")]
                        schema_registry: schema_registry.as_ref(),
                        #[cfg(feature = "kafka-schema-registry")]
                        schema_enforcement,
                        #[cfg(feature = "kafka-schema-registry")]
                        schema_accepted: schema_accepted.as_ref(),
                        #[cfg(feature = "kafka-schema-registry")]
                        schema_message_index: schema_message_index.as_deref(),
                        #[cfg(feature = "kafka-schema-registry")]
                        registry_lookup_bound: registry_lookup_bound(max_poll_interval_ms),
                    };

                    // Retaining each message's wire bytes only pays for itself if
                    // there is a DLQ to put them in on `Outcome::Reject`.
                    let retain_raw = topology.dlq().is_some();
                    let mut buffer: BatchBuffer<T> = BatchBuffer::new(max_batch_size, retain_raw);
                    let mut deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
                    let mut redelivery_backoff = batch_redelivery_backoff();

                    // Wakes the loop when neither a message nor the batch deadline
                    // will, so a rebalance that arrives while the topic is idle is
                    // still noticed promptly.
                    let mut housekeeping = tokio::time::interval(housekeeping_interval);
                    housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                    loop {
                        // Catches a rebalance that lands while the topic is idle —
                        // the housekeeping tick exists to reach this line. The
                        // drain that matters for a *busy* topic is the one inside
                        // the recv() arm, and every flush drains first too; see
                        // `apply_batch_rebalance`.
                        if apply_batch_rebalance(&consumer, queue, &mut buffer, &mut state)?
                            .is_some()
                        {
                            deadline = None;
                        }
                        state.pass(&consumer, queue)?;

                        let sleep_until_deadline = async {
                            match deadline.as_mut() {
                                Some(d) => d.await,
                                None => std::future::pending().await,
                            }
                        };
                        let now = Instant::now();
                        let commit_deadline = state.commits.gate.deadline(now);
                        let in_flight = state.commits.in_flight.is_some();
                        let committable = !in_flight && state.tracker.has_committable();

                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                if !buffer.is_empty() {
                                    flush_batch(
                                        &flush_ctx,
                                        handler.as_ref(),
                                        ctx.as_ref(),
                                        &mut buffer,
                                        &mut redelivery_backoff,
                                        &mut state,
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
                            // Wakes the loop when the commit gate's window
                            // expires with a position to commit, so it lands on
                            // a topic gone idle.
                            _ = tokio::time::sleep_until(commit_deadline), if committable => {}
                            answer = commit_answer(state.commits.in_flight.as_mut()), if in_flight => {
                                state.commits.settle(answer, &mut state.tracker, queue);
                            }
                            () = sleep_until_deadline => {
                                flush_batch(
                                    &flush_ctx,
                                    handler.as_ref(),
                                    ctx.as_ref(),
                                    &mut buffer,
                                    &mut redelivery_backoff,
                                    &mut state,
                                )
                                .await?;
                                deadline = None;
                            }
                            msg_result = consumer.recv() => {
                                let msg = match msg_result {
                                    Ok(msg) => msg,
                                    Err(e) if recv_error_is_transient(consumer.transport_gate(), &e) => {
                                        log_transient_recv_error(queue, &e);
                                        continue;
                                    }
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
                                if let Some(rewound) =
                                    apply_batch_rebalance(&consumer, queue, &mut buffer, &mut state)?
                                {
                                    deadline = None;
                                    // Rewound below this record: the broker delivers it again.
                                    if rewound.contains(&partition) {
                                        continue;
                                    }
                                }

                                metrics::record_message_size(&topic, group.as_deref(), payload_slice.len());

                                let key = msg.key().map(Bytes::copy_from_slice);
                                let headers = extract_string_headers(&msg);

                                // The record's fate is decided before its offset
                                // joins the span, because the span is what the
                                // next flush commits: a record the schema registry
                                // cannot resolve right now must stay out of it
                                // until it decodes. Every other outcome extends the
                                // span, a pre-handler drop included: leaving a
                                // dropped offset out of the span replays it after
                                // every restart, and a window of nothing but
                                // poison would never commit at all. Arming the
                                // deadline with the span is what guarantees such an
                                // all-dropped window still reaches `flush_batch`.
                                if let Err(e) = validate_message_size(payload_slice.len(), max_message_size) {
                                    let epoch = state.tracker.track_epoch(partition, offset);
                                    buffer.extend_span(partition, offset, epoch);
                                    if deadline.is_none() {
                                        deadline = Some(Box::pin(tokio::time::sleep(max_batch_age)));
                                    }
                                    tracing::warn!(error = %e, queue, partition, offset, dlq = retain_raw, "oversized message, dropped before the handler");
                                    let pending = metrics::record_terminal(
                                        &topic,
                                        group.as_deref(),
                                        metrics::FailReason::Oversize,
                                        retain_raw,
                                    );
                                    buffer.drop_message(
                                        (partition, offset),
                                        || RawMessage {
                                            payload: Bytes::copy_from_slice(payload_slice),
                                            key,
                                            headers: RecordHeaders::with_strings(&msg, headers),
                                        },
                                        e.to_string(),
                                        pending,
                                    );
                                } else {
                                    #[cfg(feature = "kafka-schema-registry")]
                                    let mut stall = RegistryStall::default();
                                    // Set when the flush ahead of a stalled record
                                    // rewound the span. The broker then delivers
                                    // this record again behind the rewound ones,
                                    // and the copy in hand must not be buffered
                                    // ahead of them: that would hand the handler a
                                    // batch out of offset order and, on a further
                                    // `Retry`, seek to a start above records it
                                    // never acked.
                                    #[cfg(feature = "kafka-schema-registry")]
                                    let mut rewound = false;
                                    #[cfg(feature = "kafka-schema-registry")]
                                    let mut revoke_token: Option<CancellationToken> = None;
                                    // Loops only through the registry wait arm, which
                                    // the `kafka-schema-registry` feature adds.
                                    #[allow(clippy::never_loop)]
                                    let decoded = loop {
                                        let Some(decoded) = decode_or_shutdown(
                                            decode_batch_message::<T>(&decode_ctx, payload_slice),
                                            &shutdown,
                                        )
                                        .await
                                        else {
                                            // Shutdown fired mid-lookup. The record in
                                            // hand stays uncommitted, and what came
                                            // before it is flushed as the shutdown arm
                                            // would flush it.
                                            if !buffer.is_empty() {
                                                flush_batch(
                                                    &flush_ctx,
                                                    handler.as_ref(),
                                                    ctx.as_ref(),
                                                    &mut buffer,
                                                    &mut redelivery_backoff,
                                                    &mut state,
                                                )
                                                .await?;
                                            }
                                            tracing::info!(queue, "shutdown signal received during a schema registry lookup, batch consumer stopped");
                                            return Ok(());
                                        };
                                        match decoded {
                                            // The span must end before this record:
                                            // flush what came before it, then wait
                                            // for the registry. Nothing after it is
                                            // read while the assignment is paused,
                                            // so partition order holds.
                                            #[cfg(feature = "kafka-schema-registry")]
                                            BatchDecode::Unavailable { id, error } => {
                                                // Once, and before the flush, whose polls can serve a revoke.
                                                let revoked = revoke_token
                                                    .get_or_insert_with(|| state.tracker.assignment_token(partition))
                                                    .clone();
                                                if !buffer.is_empty() {
                                                    let flushed = flush_batch(
                                                        &flush_ctx,
                                                        handler.as_ref(),
                                                        ctx.as_ref(),
                                                        &mut buffer,
                                                        &mut redelivery_backoff,
                                                        &mut state,
                                                    )
                                                    .await?;
                                                    deadline = None;
                                                    if let Flushed::Redelivered(sought) = flushed {
                                                        // The flush sought partitions
                                                        // of the span back. This record
                                                        // sits after that span, so it
                                                        // goes back too: its partition
                                                        // is either rewound below it
                                                        // already, or sought to it here.
                                                        // The rewound records and this
                                                        // one then arrive in offset
                                                        // order once the wait ends.
                                                        if !sought.contains(&partition) {
                                                            #[cfg(feature = "test-support")]
                                                            put_back_probe::BATCH_REWIND.fetch_add(1, Ordering::SeqCst);
                                                            put_back(&consumer, queue, partition, offset)?;
                                                        }
                                                        rewound = true;
                                                    }
                                                }
                                                if !stall
                                                    .wait(&consumer, &shutdown, None, &topic, group.as_deref(), queue, id, &error)
                                                    .await?
                                                {
                                                    // The buffer is empty and this record
                                                    // is uncommitted: nothing left to flush.
                                                    tracing::info!(queue, "shutdown signal received during a schema registry wait, batch consumer stopped");
                                                    return Ok(());
                                                }
                                                // An outage can last: commit between its retries.
                                                state.stall_pass(&consumer, queue)?;
                                                // Read after the pass, whose drain backs up the callback's cancel.
                                                if revoked.is_cancelled() {
                                                    tracing::debug!(
                                                        queue,
                                                        partition,
                                                        offset,
                                                        "partition revoked during a schema registry wait; the record is left to its next owner"
                                                    );
                                                    break None;
                                                }
                                            }
                                            #[cfg(feature = "kafka-schema-registry")]
                                            BatchDecode::Fatal(e) => return Err(e),
                                            BatchDecode::Decoded(m) => break Some(BatchStaged::Decoded(m)),
                                            BatchDecode::Dlq { reason, fail } => {
                                                break Some(BatchStaged::Dlq { reason, fail });
                                            }
                                        }
                                    };
                                    #[cfg(feature = "kafka-schema-registry")]
                                    stall.end(&consumer)?;
                                    // `None`: the stall saw its record's partition revoked.
                                    let Some(decoded) = decoded else {
                                        continue;
                                    };
                                    #[cfg(feature = "kafka-schema-registry")]
                                    if rewound {
                                        // The registry answers again and the
                                        // assignment is resumed: the broker
                                        // delivers this record behind the rewound
                                        // span, and that copy is the one that gets
                                        // buffered.
                                        drop(decoded);
                                        continue;
                                    }
                                    let epoch = state.tracker.track_epoch(partition, offset);
                                    buffer.extend_span(partition, offset, epoch);
                                    if deadline.is_none() {
                                        deadline = Some(Box::pin(tokio::time::sleep(max_batch_age)));
                                    }
                                    match decoded {
                                        BatchStaged::Dlq { reason, fail } => {
                                            let pending = metrics::record_terminal(
                                                &topic,
                                                group.as_deref(),
                                                fail,
                                                retain_raw,
                                            );
                                            buffer.drop_message(
                                                (partition, offset),
                                                || RawMessage {
                                                    payload: Bytes::copy_from_slice(payload_slice),
                                                    key,
                                                    headers: RecordHeaders::with_strings(&msg, headers),
                                                },
                                                reason.to_string(),
                                                pending,
                                            );
                                        }
                                        BatchStaged::Decoded(decoded) => {
                                            let metadata = build_message_metadata(&headers, false, RecordCoordinates::of(&msg));
                                            buffer.push(decoded, metadata, (partition, offset), || RawMessage {
                                                payload: Bytes::copy_from_slice(payload_slice),
                                                key,
                                                headers: RecordHeaders::with_strings(&msg, headers),
                                            });
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
                                        &mut state,
                                    )
                                    .await?;
                                    deadline = None;
                                }
                            }
                        }
                    }
                }
                .await;

                match ended {
                    Ok(()) => {
                        let pending_commit = state.commits.in_flight.take();
                        stop_with_final_commit(
                            consumer,
                            &mut state.tracker,
                            &mut state.rebalance_rx,
                            pending_commit,
                            queue,
                        )
                        .await
                    }
                    Err(e) => {
                        close_without_commit(consumer, queue, SHUTDOWN_COMMIT_DEADLINE).await;
                        Err(e)
                    }
                }
            }
        })
        .await
    }

    /// Consume the sequenced topic `T` one record at a time, in partition
    /// order, across every partition this member is assigned.
    ///
    /// A `Retry` keeps the record where it is, and so do a `Defer`, which a
    /// sequenced consumer turns into a `Retry`, a handler timeout and a
    /// handler panic. The consumer waits the hold delay for the attempt, 1 s
    /// with no hold queues, with its assignment paused and still polling,
    /// then hands the same record back to the handler: nothing behind it on
    /// the member is handled first, and nothing is republished. The attempt
    /// count lives in memory, read from the record's `Shove-Retry-Count` on
    /// every delivery, so a restart or a rebalance starts it again. Once the
    /// budget is spent, or on a `Reject`, the record is dead-lettered under
    /// its `SequenceFailure`. A dead letter that fails and may land later
    /// keeps the record uncommitted and is tried again after a backoff,
    /// polling, until it lands. A stop during any of these waits returns at
    /// once and leaves the record for the restart. A dead letter that can
    /// never be published, too large or invalid, is discarded and counted.
    /// One that fails with an error no retry gets past, such as a producer
    /// that cannot be built, ends the member once its final commit has run,
    /// with the record left uncommitted; `run_fifo` logs the error and
    /// returns `Ok(())`.
    ///
    /// Either `RetryStrategy` is accepted, and both retry in place. Offsets
    /// are committed at most every 500 ms, one commit at a time on a thread
    /// of its own while the consumer goes on, and once more at the stop. A
    /// commit the coordinator rejects is re-offered, and a member whose
    /// commits keep being rejected with no rebalance resolving them is
    /// treated as fenced and reconnects, as the standard consumer does.
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
        let handles = self
            .spawn_fifo_shards_checked::<T, H>(handler, ctx, options)
            .await?;
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

    /// [`spawn_fifo_shards`](Self::spawn_fifo_shards) after the startup topic
    /// check, which needs the broker and so cannot run in the sync spawn.
    ///
    /// Run here and not in the spawned task: a task's error is logged by
    /// `run_fifo_with_inner` and not returned, so a check there would end the
    /// start without telling its caller. The options are refused first, as
    /// the spawn refuses them, so a bad option never waits on the broker.
    pub(crate) async fn spawn_fifo_shards_checked<T, H>(
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
        check_fifo_options(topology.queue(), &options)?;
        startup::verify(
            &self.client,
            topology,
            startup::Path::Publishing,
            &options.shutdown,
            options.max_reconnect_attempts,
        )
        .await?;
        self.spawn_fifo_shards::<T, H>(handler, ctx, options)
    }

    /// Spawn the Kafka FIFO consumer task and return its join handle.
    ///
    /// Kafka relies on partition-level ordering, so a single consumer task is
    /// sufficient — `routing_shards` is a no-op for Kafka FIFO. The returned
    /// `Vec` always contains exactly one element. `pub(crate)` because the
    /// consumer-group module spawns its FIFO member through it.
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
        // `T` is bound by `SequencedTopic`, so a topology without sequencing
        // means a caller that bypassed the bound: refused, never panicked on.
        let seq_config = topology.sequencing().ok_or_else(|| {
            ShoveError::Topology(format!(
                "run_fifo called on {queue} without sequencing config"
            ))
        })?;
        check_fifo_options(&queue, &options)?;
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
        let max_poll_interval_ms = max_poll_interval_ms(
            #[cfg(feature = "test-support")]
            options.kafka_max_poll_interval,
        );
        let fence_timeout = fence_threshold_over(fence_floor(&options), ASYNC_COMMIT_INTERVAL);
        #[cfg(feature = "test-support")]
        fence_probe::record(fence_timeout);
        #[cfg(feature = "kafka-schema-registry")]
        let schema_accepted: Arc<[Arc<str>]> = options
            .schema_accepted_subjects
            .clone()
            .map(Arc::from)
            .unwrap_or_else(|| Arc::from(vec![default_subject(&queue)]));
        let fifo = FifoLoop {
            handler,
            ctx,
            client: self.client.clone(),
            topology,
            topic: Arc::from(queue.as_str()),
            group: options.consumer_group.clone(),
            group_id,
            auto_offset_reset: options
                .kafka_auto_offset_reset
                .unwrap_or(KafkaAutoOffsetReset::Earliest),
            max_poll_interval_ms,
            fetch_queue: options.kafka_fetch_queue,
            fence_timeout,
            shutdown: options.shutdown.clone(),
            processing: options.processing.clone(),
            max_retries: options.max_retries,
            handler_timeout: options.handler_timeout,
            handler_timeout_outcome: options.handler_timeout_outcome.clone(),
            max_message_size: options.max_message_size,
            // Kafka has a single FIFO task covering every assigned partition,
            // so one poison set covers every key this consumer sees. It
            // outlives every connection: a broker blip must not un-poison a
            // key.
            //
            // Scope is this consumer task, as documented on
            // `SequenceFailure`. A partition reassigned to a sibling consumer
            // starts with that consumer's own (empty) set — the same
            // per-process scope RabbitMQ has.
            poisoned: PoisonedKeys::new(seq_config.on_failure()),
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: options.schema_registry.clone(),
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: options.schema_enforcement,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted,
            #[cfg(feature = "kafka-schema-registry")]
            schema_message_index: options.schema_message_index.clone().map(Arc::from),
            #[cfg(feature = "kafka-schema-registry")]
            lookup_bound: registry_lookup_bound(max_poll_interval_ms),
            queue,
            _topic: PhantomData,
        };
        tracing::info!(
            queue = %fifo.queue,
            group_id = %fifo.group_id,
            max_retries = fifo.max_retries,
            "Kafka FIFO consumer started"
        );
        let max_reconnect_attempts = options.max_reconnect_attempts;
        let shard_task = tokio::spawn(async move {
            run_with_reconnect(
                &fifo.shutdown,
                &fifo.queue,
                max_reconnect_attempts,
                None,
                || run_serial_connection(&fifo),
            )
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
        let handles = match self
            .spawn_fifo_shards_checked::<T, H>(handler, ctx, options)
            .await
        {
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

    /// What the DLQ drain refuses, after the DLQ check and before it
    /// subscribes: the settings its loop never reads, two group knobs and the
    /// outcome of a handler timeout.
    ///
    /// The drain hard-codes `earliest` for its group, and that policy applies
    /// only while the group has no usable committed offset: a fresh drain can
    /// never skip a dead letter the operator opted in to keep, and a restarted
    /// drain resumes from its commit. It commits on the fixed default
    /// interval, as the FIFO consumer does, so there is no commit policy to
    /// set: neither an interval nor a per-record policy. `handle_dead` returns
    /// no outcome, so a call that times out has none to resolve to, and its
    /// dead letter is committed. None of `with_auto_offset_reset`,
    /// `with_commit_policy` (or its shorthand `with_commit_interval`) and
    /// `with_handler_timeout_outcome` would change anything, and a setting
    /// that changes nothing is refused rather than dropped: the FIFO
    /// consumer's policy, applied on every path. The message names the entry
    /// point, as `refuse_broadcast_start` does.
    fn check_dlq_options(queue: &str, dlq: &str, options: &ConsumerOptions) -> Result<()> {
        if options.kafka_commit_policy.is_some() {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}': `with_commit_policy` and `with_commit_interval` do not apply \
                 to the dead-letter drain of '{dlq}', which commits on the fixed default \
                 interval; `KafkaConsumer::run_dlq` never reads them. Drop the call."
            )));
        }
        if options.kafka_auto_offset_reset.is_some() {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}': `with_auto_offset_reset` does not apply to the dead-letter \
                 drain of '{dlq}', which hard-codes `earliest` for a group with no committed \
                 offset so a fresh drain never skips a dead letter; `KafkaConsumer::run_dlq` \
                 never reads it. Drop `with_auto_offset_reset(..)`."
            )));
        }
        if options.handler_timeout_outcome.is_some() {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}': `with_handler_timeout_outcome` does not apply to the \
                 dead-letter drain of '{dlq}', where `handle_dead` returns no outcome and a \
                 call that times out has its dead letter committed; `KafkaConsumer::run_dlq` \
                 never reads it. Drop `with_handler_timeout_outcome(..)`."
            )));
        }
        Ok(())
    }

    /// The Kafka half of `BroadcastImpl::check_options`: what a broadcast
    /// subscription refuses at `subscribe()`, before its loop is spawned.
    ///
    /// Every [`BroadcastStart`] is honoured here, so none is refused. The
    /// two group knobs are: the loop commits nothing, so a commit policy,
    /// interval or per-record, would be read by nothing, and it assigns
    /// every partition at an explicit offset, so librdkafka never consults
    /// `auto.offset.reset`. The FIFO consumer already refuses a commit
    /// policy on the same ground, and a setting that changes nothing is
    /// refused rather than dropped. A retry strategy is refused on that
    /// ground too: the loop settles every outcome without one, so
    /// `with_retry_strategy` would be read by nothing. The loop does read the
    /// fetch queue settings, and one outside librdkafka's range is refused.
    pub(crate) fn check_broadcast_options(queue: &str, options: &ConsumerOptions) -> Result<()> {
        if options.kafka_commit_policy.is_some() {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}' is a broadcast topology; `with_commit_policy` and \
                 `with_commit_interval` do not apply to a broadcast subscription, which \
                 commits nothing. Drop the call."
            )));
        }
        if options.kafka_auto_offset_reset.is_some() {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}' is a broadcast topology; `with_auto_offset_reset` does not \
                 apply to a broadcast subscription, which assigns every partition at an \
                 explicit offset and never consults `auto.offset.reset`. Drop \
                 `with_auto_offset_reset(..)` and set the start with `with_broadcast_start`."
            )));
        }
        if options.retry_strategy.is_some() {
            return Err(reject_retry_strategy_unread(
                queue,
                "BroadcastSubscriber::subscribe",
            ));
        }
        options.kafka_fetch_queue.validate(queue)
    }

    /// Run this process's own groupless subscription to `T` until shutdown.
    ///
    /// Reached only through
    /// [`BroadcastSubscriber`](crate::broadcast::BroadcastSubscriber), which
    /// already refuses a non-broadcast topology; the guard here is the
    /// backend-side half of that check.
    ///
    /// # What makes it ephemeral
    ///
    /// The subscription is `assign()` of every partition at the configured
    /// start - the tail unless `with_broadcast_start` says otherwise - with no
    /// `subscribe()` and no commit anywhere in the loop. Nothing is written to
    /// the broker at any point in its life, so there is nothing to tear down
    /// when it ends and nothing to reap when the process dies without ending it
    /// — which is why the teardown here is "drop the consumer handle" and not a
    /// cleanup path that has to be reached. See
    /// [`broadcast_group_id`](super::constants::broadcast_group_id) for why a
    /// `group.id` string is nonetheless configured, why it is a fixed one, and
    /// why `with_group_id` may replace it.
    ///
    /// A reconnect re-assigns at the configured start as it stands *then*: at
    /// the then-current tail by default, so messages published while the
    /// connection was down are not replayed - deliver-new applied to the
    /// reconnect window, the same best-effort contract the subscription has
    /// everywhere else.
    pub(crate) async fn run_broadcast_with_inner<T, H>(
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
        if !topology.broadcast() {
            return Err(ShoveError::Topology(format!(
                "topic '{}' is not a broadcast topology; a groupless subscription to it \
                 would compete with the topic's consumer group rather than fan out",
                topology.queue()
            )));
        }
        let queue = topology.queue();
        // An explicit `with_group_id` is honoured verbatim, as on the standard
        // path. It is inert either way; the override exists for a cluster ACL
        // that grants group Describe on one prefix only.
        let group_id = options
            .kafka_group_id
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| super::constants::broadcast_group_id(queue));
        let start = options.broadcast_start;
        let fetch_queue = options.kafka_fetch_queue;
        // One warning per subscription, not per poll: the error repeats on
        // every coordinator lookup for the life of the handle.
        let warned_group_acl = Arc::new(AtomicBool::new(false));

        let shutdown = options.shutdown.clone();
        let processing = options.processing.clone();
        let prefetch_count = options.prefetch_count.max(1);
        let handler_timeout = options.handler_timeout;
        let handler_timeout_outcome_cfg = options.handler_timeout_outcome.clone();
        let max_message_size = options.max_message_size;

        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let client = self.client.clone();
        let semaphore = Arc::new(Semaphore::new(prefetch_count as usize));
        let topic: Arc<str> = Arc::from(queue);
        // Always `None`: `BroadcastSubscriber` sets no consumer-group label,
        // because there is no group to label. Threaded through anyway so the
        // metric call sites are identical to the shared path's.
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
        #[cfg(feature = "kafka-schema-registry")]
        let schema_message_index: Option<Arc<[i32]>> =
            options.schema_message_index.clone().map(Arc::from);

        startup::verify(
            &client,
            topology,
            startup::Path::Broadcast,
            &shutdown,
            options.max_reconnect_attempts,
        )
        .await?;

        tracing::info!(
            queue,
            group_id,
            prefetch_count,
            "Kafka broadcast subscription starting"
        );

        run_with_reconnect(&shutdown, queue, options.max_reconnect_attempts, None, || {
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
            let warned_group_acl = warned_group_acl.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_registry = schema_registry.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_accepted = schema_accepted.clone();
            #[cfg(feature = "kafka-schema-registry")]
            let schema_message_index = schema_message_index.clone();
            async move {
                // A groupless consumer never joins, so the rebalance callbacks
                // wired into the shared context never fire; the receiver is
                // dropped deliberately, as in the DLQ loop.
                let (rebalance_tx, _) = std_mpsc::channel::<RebalanceEvent>();
                let consumer = Arc::new(create_stream_consumer(
                    client.base_config(),
                    &group_id,
                    // Inert: every partition is assigned at an explicit offset
                    // below, so librdkafka never consults the reset policy.
                    // `Latest` states the intent anyway.
                    KafkaAutoOffsetReset::Latest,
                    MAX_POLL_INTERVAL_MS,
                    &fetch_queue,
                    queue,
                    rebalance_tx,
                    AssignmentTokens::default(),
                    #[cfg(feature = "kafka-msk-iam")]
                    client.msk_context(),
                )?);

                // A schema registry deployment fault met inside a handler's
                // task (a deferred redelivery decodes there) ends the
                // subscription the way the receive loop ends it when it meets
                // the same fault: at once, with the record undelivered rather
                // than counted as a discard. The sender stays alive here for
                // the loop's lifetime so the arm below stays pending; it is
                // cloned into tasks only under `kafka-schema-registry`, the
                // one build with such a fault to report.
                let (fault_tx, mut fault_rx) = mpsc::channel::<ShoveError>(1);
                #[cfg(not(feature = "kafka-schema-registry"))]
                let _ = &fault_tx;
                // Handler tasks wait on this child rather than on `shutdown`
                // itself, so a fatal exit of the loop below can end their
                // in-place redelivery waits without cancelling the token the
                // subscriber owns.
                let task_cancel = shutdown.child_token();

                // `fetch_metadata` blocks the calling thread, so the assignment
                // runs off the runtime's worker threads. `create_stream_consumer`
                // configures `cooperative-sticky`, which would refuse a
                // non-incremental `assign()` — but only once an assignor has
                // been selected, which happens during JoinGroup. This consumer
                // never subscribes, so it has no assignor and librdkafka reports
                // rebalance protocol NONE, under which the eager `assign()` is
                // accepted. Pinned by `kafka_broadcast_leaves_no_consumer_group`.
                let assign_consumer = consumer.clone();
                let assign_topic = queue.to_string();
                let partitions = tokio::task::spawn_blocking(move || {
                    assign_consumer.assign_all_partitions_at(
                        &assign_topic,
                        start,
                        BROADCAST_ASSIGN_TIMEOUT,
                    )
                })
                .await
                .map_err(|e| {
                    ShoveError::Connection(format!("broadcast assign task panicked: {e}"))
                })??;

                tracing::info!(
                    queue,
                    partitions,
                    start = ?start,
                    "broadcast subscription assigned"
                );

                let mut assignment_refresh =
                    tokio::time::interval(BROADCAST_ASSIGN_REFRESH_INTERVAL);
                assignment_refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // `interval`'s first tick is immediate. The initial assignment
                // above is already the authoritative metadata snapshot.
                assignment_refresh.tick().await;

                let tail_cancel = task_cancel.clone();
                let tail_semaphore = semaphore.clone();
                let tail_processing = processing.clone();
                let ended: Result<()> = async move {
                loop {
                    let (payload, headers, coordinates) = tokio::select! {
                        biased;

                        _ = shutdown.cancelled() => {
                            tracing::info!(queue, "shutdown signal received, draining in-flight handlers");
                            // Nothing to commit and nothing to delete: the
                            // subscription exists only in this consumer handle,
                            // which is dropped when this future resolves.
                            let _ = semaphore.acquire_many(prefetch_count as u32).await;
                            return Ok(());
                        }

                        fault = fault_rx.recv() => {
                            // `None` cannot happen while `fault_tx` lives in
                            // this scope; the arm is total anyway.
                            let Some(e) = fault else { continue };
                            tracing::error!(error = %e, queue, "schema registry deployment fault reported by a handler task; ending the broadcast subscription");
                            return Err(e);
                        }

                        refresh = async {
                            assignment_refresh.tick().await;
                            let refresh_consumer = consumer.clone();
                            let refresh_topic = queue.to_string();
                            tokio::task::spawn_blocking(move || {
                                refresh_consumer.refresh_broadcast_partitions(
                                    &refresh_topic,
                                    start,
                                    BROADCAST_ASSIGN_TIMEOUT,
                                )
                            })
                            .await
                        } => {
                            match refresh {
                                Ok(Ok(added)) if added > 0 => {
                                    tracing::info!(
                                        queue,
                                        added,
                                        "broadcast subscription extended its partition assignment"
                                    );
                                }
                                Ok(Ok(_)) => {}
                                Ok(Err(e)) => {
                                    // The existing assignment remains valid.
                                    // Retry on the next tick instead of
                                    // reconnecting at the tail and creating an
                                    // avoidable delivery gap on every partition.
                                    tracing::warn!(
                                        error = %e,
                                        queue,
                                        "broadcast assignment refresh failed; keeping the current assignment"
                                    );
                                }
                                Err(e) => {
                                    return Err(ShoveError::Connection(format!(
                                        "broadcast assignment refresh task panicked: {e}"
                                    )));
                                }
                            }
                            continue;
                        }

                        msg_result = consumer.recv() => {
                            let msg = match msg_result {
                                Ok(msg) => msg,
                                Err(e) if is_ignorable_groupless_error(&e) => {
                                    // librdkafka looks up the inert group's
                                    // coordinator anyway. A cluster ACL that
                                    // does not grant Describe on it answers
                                    // this on every lookup; fetching is
                                    // unaffected, and a consumer that never
                                    // joins needs no group permission.
                                    if !warned_group_acl.swap(true, Ordering::Relaxed) {
                                        tracing::warn!(
                                            error = %e,
                                            queue,
                                            group_id,
                                            "coordinator lookup for the inert broadcast \
                                             group id is not authorized; fetching continues \
                                             (grant Describe on it, or pick one with \
                                             `with_group_id`, to silence this)"
                                        );
                                    }
                                    continue;
                                }
                                Err(e) if recv_error_is_transient(consumer.transport_gate(), &e) => {
                                    log_transient_recv_error(queue, &e);
                                    continue;
                                }
                                Err(e) => {
                                    tracing::error!(error = %e, queue, "broadcast consumer recv error");
                                    return Err(map_kafka_error(
                                        &format!("broadcast consumer recv error on {queue}"),
                                        e,
                                    ));
                                }
                            };
                            // Unlike the shared loop this copies before decoding
                            // rather than after. There is no DLQ arm to feed
                            // from the borrowed payload, and a deferred
                            // redelivery decodes the same owned bytes again
                            // inside the handler's task, so one representation
                            // is worth more here than one saved allocation on
                            // the reject path.
                            (
                                msg.payload().unwrap_or_default().to_vec(),
                                extract_string_headers(&msg),
                                RecordCoordinates::of(&msg),
                            )
                        }
                    };

                    metrics::record_message_size(&topic, group.as_deref(), payload.len());

                    if let Err(e) = validate_message_size(payload.len(), max_message_size) {
                        tracing::warn!(
                            error = %e,
                            queue,
                            "oversized message on a broadcast subscription — discarding"
                        );
                        discard_broadcast(&topic, group.as_deref(), metrics::FailReason::Oversize);
                        continue;
                    }

                    #[cfg(feature = "kafka-schema-registry")]
                    let message: T::Message = {
                        let staged = if let Some(registry) = schema_registry.as_ref() {
                            let codec_name = <T::Codec as crate::Codec<T::Message>>::NAME;
                            match WireFormat::from_codec_name(codec_name) {
                                Some(fmt) => {
                                    // An unavailable registry stalls this record
                                    // instead of discarding it; see `RegistryStall`.
                                    // Groupless: the wait tolerates the inert
                                    // group id's coordinator answer as this loop's
                                    // `consumer.recv()` arm does.
                                    let mut stall = RegistryStall::groupless();
                                    let staged = loop {
                                        let Some(result) = decode_or_shutdown(
                                            registry_decode::<T::Message, T::Codec>(
                                                registry,
                                                fmt,
                                                schema_enforcement,
                                                &schema_accepted,
                                                schema_message_index.as_deref(),
                                                registry_lookup_bound(MAX_POLL_INTERVAL_MS),
                                                &payload,
                                            ),
                                            &shutdown,
                                        )
                                        .await
                                        else {
                                            break Staged::Stop;
                                        };
                                        match result {
                                            Ok(RegistryDecode::Decoded(m)) => break Staged::Ready(m),
                                            Ok(RegistryDecode::Dlq(reason)) => {
                                                break Staged::Drop(
                                                    metrics::FailReason::for_schema_reason(reason),
                                                    reason,
                                                );
                                            }
                                            Ok(RegistryDecode::Unavailable { id, error }) => {
                                                if !stall
                                                    .wait(&consumer, &shutdown, Some(&mut fault_rx), &topic, group.as_deref(), queue, id, &error)
                                                    .await?
                                                {
                                                    break Staged::Stop;
                                                }
                                            }
                                            Err(e @ ShoveError::Topology(_)) => return Err(e),
                                            Err(e) => {
                                                tracing::error!(error = %e, queue, "failed to deserialize broadcast message; discarding");
                                                break Staged::Drop(
                                                    metrics::FailReason::Deserialize,
                                                    "deserialization_error",
                                                );
                                            }
                                        }
                                    };
                                    stall.end(&consumer)?;
                                    staged
                                }
                                None => {
                                    tracing::error!(
                                        codec = codec_name,
                                        queue,
                                        "codec has no Confluent wire format; discarding"
                                    );
                                    Staged::Drop(
                                        metrics::FailReason::for_schema_reason("schema_unsupported_codec"),
                                        "schema_unsupported_codec",
                                    )
                                }
                            }
                        } else {
                            match <T::Codec as crate::Codec<T::Message>>::decode(&payload) {
                                Ok(m) => Staged::Ready(m),
                                Err(e) => {
                                    tracing::error!(error = %e, queue, "failed to deserialize broadcast message; discarding");
                                    Staged::Drop(metrics::FailReason::Deserialize, "deserialization_error")
                                }
                            }
                        };
                        match staged {
                            Staged::Ready(m) => m,
                            Staged::Drop(reason, _) => {
                                discard_broadcast(&topic, group.as_deref(), reason);
                                continue;
                            }
                            // Shutdown fired during a registry wait; the loop's
                            // `shutdown.cancelled()` arm ends it on its next pass.
                            Staged::Stop | Staged::Revoked => continue,
                        }
                    };

                    #[cfg(not(feature = "kafka-schema-registry"))]
                    let message: T::Message =
                        match <T::Codec as crate::Codec<T::Message>>::decode(&payload) {
                            Ok(m) => m,
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    queue,
                                    "failed to deserialize broadcast message — discarding"
                                );
                                discard_broadcast(
                                    &topic,
                                    group.as_deref(),
                                    metrics::FailReason::Deserialize,
                                );
                                continue;
                            }
                        };

                    let metadata = build_message_metadata(&headers, false, coordinates);

                    // The stop is read before the slot, and once more with
                    // the slot in hand. A deferred redelivery the stop cancels
                    // frees the slot in the same instant, and taking that slot
                    // would hand this record to a handler during the drain,
                    // ahead of the record that was waiting. The record is
                    // dropped instead, and the loop's own shutdown arm drains
                    // on the next pass.
                    #[cfg(feature = "test-support")]
                    permit_wait_probe::BROADCAST_ENTERED.fetch_add(1, Ordering::SeqCst);
                    let permit = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            tracing::debug!(
                                queue,
                                partition = coordinates.partition,
                                offset = coordinates.offset,
                                "shutdown fired while this record waited for a slot; dropped"
                            );
                            continue;
                        }
                        permit = semaphore.clone().acquire_owned() => permit
                            .map_err(|_| ShoveError::Connection("semaphore closed".to_string()))?,
                    };
                    #[cfg(feature = "test-support")]
                    permit_wait_probe::acquired();
                    // Read again with the slot in hand, see `after_wait`.
                    if after_wait(&shutdown, None) == AfterWait::Stop {
                        drop(permit);
                        tracing::debug!(
                            queue,
                            partition = coordinates.partition,
                            offset = coordinates.offset,
                            "shutdown fired while this record waited for a slot; dropped"
                        );
                        continue;
                    }

                    let task_processing = processing.clone();
                    let task_topic = topic.clone();
                    let task_group = group.clone();
                    let task_handler = handler.clone();
                    let task_ctx = ctx.clone();
                    let task_timeout_outcome = handler_timeout_outcome_cfg.clone();
                    let task_shutdown = task_cancel.clone();
                    #[cfg(feature = "kafka-schema-registry")]
                    let task_fault = fault_tx.clone();
                    #[cfg(feature = "kafka-schema-registry")]
                    let task_schema_registry = schema_registry.clone();
                    #[cfg(feature = "kafka-schema-registry")]
                    let task_schema_accepted = schema_accepted.clone();
                    #[cfg(feature = "kafka-schema-registry")]
                    let task_schema_message_index = schema_message_index.clone();
                    #[cfg(feature = "kafka-schema-registry")]
                    let task_lookup_bound = registry_lookup_bound(MAX_POLL_INTERVAL_MS);

                    // Outcome routing for a broadcast subscription. The
                    // decision is `settle_broadcast_outcome`, shared with NATS
                    // and Redis so the terminal accounting cannot drift
                    // between the three loops that settle in-process: `Ack`
                    // retires the message by doing nothing (there is no
                    // offset to advance), and `Retry` and `Reject` are both
                    // terminal discards at the pinned `max_retries = 0`.
                    //
                    // The `Redeliver` half is Kafka-specific. Kafka's ordinary
                    // defer republishes to the topic, and on a broadcast topic
                    // that would fan the message back out to *every*
                    // subscriber instead of redelivering it to the one that
                    // deferred. So the wait happens right here, holding the
                    // subscription's single permit, and the same bytes are
                    // decoded again and handed back: the deferred record is
                    // redelivered before any record received after it, which
                    // is the order a single delivery loop promises.
                    tokio::spawn(async move {
                        task_processing.store(true, Ordering::Release);

                        let decode = BatchDecodeCtx {
                            queue: &task_topic,
                            #[cfg(feature = "kafka-schema-registry")]
                            schema_registry: task_schema_registry.as_ref(),
                            #[cfg(feature = "kafka-schema-registry")]
                            schema_enforcement,
                            #[cfg(feature = "kafka-schema-registry")]
                            schema_accepted: task_schema_accepted.as_ref(),
                            #[cfg(feature = "kafka-schema-registry")]
                            schema_message_index: task_schema_message_index.as_deref(),
                            #[cfg(feature = "kafka-schema-registry")]
                            registry_lookup_bound: task_lookup_bound,
                        };
                        let mut message = message;
                        let mut metadata = metadata;
                        loop {
                            let outcome = {
                                let handler = Arc::clone(&task_handler);
                                let ctx = Arc::clone(&task_ctx);
                                invoke_handler(
                                    async move { handler.handle(message, metadata, ctx.as_ref()).await },
                                    handler_timeout,
                                    task_timeout_outcome.clone(),
                                    &task_topic,
                                    task_group.as_deref(),
                                )
                                .await
                            };
                            match settle_broadcast_outcome(&outcome, &task_topic, task_group.as_deref()) {
                                BroadcastAction::Done => break,
                                BroadcastAction::Redeliver => {}
                            }
                            tokio::select! {
                                // The stop is read before the sleep, so a
                                // tie never costs a redelivery.
                                biased;
                                // A deferral outliving shutdown would hold the
                                // drain open for the length of the delay to
                                // redeliver a message the loop is about to stop
                                // reading. Drop it instead; broadcast is
                                // best-effort and the process is going away.
                                _ = task_shutdown.cancelled() => {
                                    tracing::debug!(
                                        topic = %task_topic,
                                        "shutdown cancelled a deferred broadcast redelivery"
                                    );
                                    break;
                                }
                                _ = tokio::time::sleep(BROADCAST_DEFER_DELAY) => {}
                            }
                            // Loops only through the registry wait arm, which
                            // the `kafka-schema-registry` feature adds.
                            #[allow(clippy::never_loop)]
                            let redelivered = loop {
                                let Some(decoded) = decode_or_shutdown(
                                    decode_batch_message::<T>(&decode, &payload),
                                    &task_shutdown,
                                )
                                .await
                                else {
                                    break None;
                                };
                                match decoded {
                                    BatchDecode::Decoded(m) => break Some(m),
                                    BatchDecode::Dlq { fail, .. } => {
                                        discard_broadcast(&task_topic, task_group.as_deref(), fail);
                                        break None;
                                    }
                                    #[cfg(feature = "kafka-schema-registry")]
                                    BatchDecode::Unavailable { id, error } => {
                                        tracing::warn!(
                                            topic = %task_topic,
                                            schema_id = %id,
                                            error = %error,
                                            retry_after = ?REGISTRY_RETRY_DELAY,
                                            "schema registry unavailable during a deferred redelivery; keeping the record"
                                        );
                                        metrics::record_failed(&task_topic, task_group.as_deref(), metrics::FailReason::SchemaUnavailable);
                                        tokio::select! {
                                            _ = tokio::time::sleep(REGISTRY_RETRY_DELAY) => {}
                                            _ = task_shutdown.cancelled() => break None,
                                        }
                                    }
                                    #[cfg(feature = "kafka-schema-registry")]
                                    BatchDecode::Fatal(e) => {
                                        // A deployment fault, not poison: the
                                        // record is not counted as a discard.
                                        // The receive loop ends the subscription
                                        // with this error, as it does when it
                                        // meets the fault itself. A full channel
                                        // means another task already reported.
                                        tracing::error!(topic = %task_topic, error = %e, "schema registry deployment fault during a deferred redelivery; ending the subscription");
                                        let _ = task_fault.try_send(e);
                                        break None;
                                    }
                                }
                            };
                            let Some(m) = redelivered else { break };
                            message = m;
                            metadata = build_message_metadata(&headers, true, coordinates);
                        }

                        task_processing.store(false, Ordering::Release);
                        // Held until the outcome, including a `Defer`'s delay
                        // and redelivery, is fully resolved, so deferrals stay
                        // inside the prefetch cap instead of running beside it.
                        drop(permit);
                    });
                }
                }
                .await;
                // Every fatal exit passes through here: the child token ends
                // the handler task's redelivery wait, the permit drain proves
                // it has released its permit, and only then is the busy flag
                // cleared. Nothing is committed on this path at any time.
                let Err(e) = ended else { return Ok(()) };
                tail_cancel.cancel();
                let _ = tail_semaphore.acquire_many(prefetch_count as u32).await;
                tail_processing.store(false, Ordering::Release);
                Err(e)
            }
        })
        .await
    }

    /// Public DLQ entrypoint with default options. Equivalent to
    /// [`run_dlq_with_options`](Self::run_dlq_with_options) with
    /// `ConsumerOptions::new()`; kept for callers who don't need to thread
    /// per-consumer options into the DLQ loop. It runs until the client is
    /// closed.
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
    /// The drain reads these options:
    ///
    /// - `with_shutdown` ends the drain once the dead letter in hand has
    ///   settled, and so does closing the client, whatever the token holds.
    /// - `with_handler_timeout` bounds each `handle_dead` call, 30 s by default
    ///   ([`DEFAULT_HANDLER_TIMEOUT`](crate::DEFAULT_HANDLER_TIMEOUT)), and
    ///   `without_handler_timeout` leaves the call unbounded.
    /// - `with_max_message_size` gates the dead letters. One over the limit is
    ///   logged and committed without reaching `handle_dead`.
    /// - `with_group_id` rebases the drain's group, `{group}-dlq`.
    /// - `with_consumer_group` labels the size sample of the drain.
    /// - `with_queued_min_messages`, `with_queued_max_messages_kbytes` and
    ///   `with_fetch_queue_backoff_ms` set the fetch queue of the drain's
    ///   consumer. One outside librdkafka's range is refused.
    /// - The schema registry options decode the dead letters through the
    ///   registry.
    ///
    /// # Handler calls
    ///
    /// `handle_dead` runs on the drain's own task, one dead letter at a time,
    /// under panic isolation and the handler timeout. A call that panics or
    /// outlasts the timeout is logged at warn level and does not stop the
    /// drain, and its dead letter is committed all the same, as it is after a
    /// call that returns. With no timeout, a call that never returns holds the
    /// drain, its stop included, for good. Nothing polls while the call runs,
    /// so keep the call, and the timeout, below `max.poll.interval.ms`, five
    /// minutes. `handle_dead` returns no outcome, so
    /// `with_handler_timeout_outcome` is refused. A `handle_dead` call moves
    /// no consumed, in-flight, duration or failed metric under the source
    /// topic. The series the drain does record are
    /// `shove_message_size_bytes` for each dead letter, and
    /// `shove_messages_failed_total{reason="schema_unavailable"}` for each
    /// registry wait.
    ///
    /// # Commits
    ///
    /// The drain commits as the sequenced consumer does: at most once per
    /// 500 ms, one commit at a time on a thread of its own while the drain
    /// goes on, and offered again when the coordinator rejects it. Its
    /// commits are fixed, so `with_commit_interval`, `with_commit_policy` and
    /// `with_auto_offset_reset` are refused. A stop makes one synchronous
    /// final commit of every position, and closes the consumer, both on a
    /// thread of its own.
    ///
    /// # Errors
    ///
    /// A topology problem is returned before any dead letter is read: a DLQ
    /// topic that does not exist on an external topology, or an option the
    /// drain refuses. A stop whose final commit the broker does not confirm
    /// returns `ShoveError::Commit`, as it does for every consume loop. A fault
    /// the drain cannot ride out closes its consumer, waits for the close, and
    /// reconnects; an error no reconnect gets past, such as a schema registry
    /// deployment fault, ends the drain with that error.
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
        options.refuse_broadcast_start(topology.queue(), "KafkaConsumer::run_dlq")?;
        Self::check_dlq_options(topology.queue(), dlq, &options)?;
        if options.retry_strategy.is_some() {
            return Err(reject_retry_strategy_unread(
                topology.queue(),
                "KafkaConsumer::run_dlq",
            ));
        }
        options.kafka_fetch_queue.validate(topology.queue())?;

        // Honor the `group.id` override (set via
        // `ConsumerOptions::<Kafka>::with_group_id`) by rebasing the DLQ group
        // onto it as `{group}-dlq`, so a custom group does not re-collide on
        // the default `{dlq}-consumer`. `None` keeps that default.
        let group_id = match options.kafka_group_id.as_deref() {
            Some(base) => super::constants::dlq_group_id_from_base(base),
            None => super::constants::dlq_consumer_group_id(dlq),
        };
        // The drain stops on the token of its options or on the client's, so
        // closing the client ends a drain whose token nothing cancels. The
        // token of the options goes first: a stop through it reaches the
        // drain at once.
        let (shutdown, _forwarder) =
            cancelled_by_either(&options.shutdown, &self.client.shutdown_token());
        let drain = DlqDrain::<T, H> {
            handler,
            ctx,
            client: self.client.clone(),
            dlq,
            group_id,
            // `shove_message_size_bytes` labels a DLQ drain with the SOURCE
            // topic, not the DLQ name, and with whatever `consumer_group` the
            // main loop for this topic would carry — never the internal
            // `{dlq}-consumer` group id above. Two reasons the label must not
            // switch to `dlq`: Redis already drains its DLQ through
            // `run_stream_loop`, which labels every metric `topology.queue()`
            // whichever stream it is reading, so a DLQ name here would make
            // `topic` mean two different things depending on the backend; and
            // a per-topic size profile is only summable across the main and
            // DLQ paths if both carry the same label. The DLQ drain stays
            // distinguishable through the metrics that exist to name it
            // (`shove_messages_discarded_total`, the DLQ backlog gauge) rather
            // than by overloading `topic` here.
            topic: Arc::from(topology.queue()),
            group: options.consumer_group.clone(),
            shutdown,
            max_message_size: options.max_message_size,
            handler_timeout: options.handler_timeout,
            fetch_queue: options.kafka_fetch_queue,
            fence_timeout: fence_threshold_over(fence_floor(&options), ASYNC_COMMIT_INTERVAL),
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: options.schema_registry.clone(),
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: options.schema_enforcement,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted: options
                .schema_accepted_subjects
                .clone()
                .unwrap_or_else(|| vec![default_subject(dlq)]),
            #[cfg(feature = "kafka-schema-registry")]
            schema_message_index: options.schema_message_index.clone(),
            #[cfg(feature = "kafka-schema-registry")]
            lookup_bound: registry_lookup_bound(MAX_POLL_INTERVAL_MS),
            _topic: PhantomData,
        };

        startup::verify(
            &drain.client,
            topology,
            startup::Path::DeadLetterDrain,
            &drain.shutdown,
            None,
        )
        .await?;

        tracing::info!(
            dlq,
            group_id = drain.group_id.as_str(),
            "Kafka DLQ consumer started"
        );

        run_with_reconnect(&drain.shutdown, dlq, None, None, || {
            run_serial_connection(&drain)
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
        tracker.drain_committable().map(|drained| {
            assert!(
                drained.discards.is_empty(),
                "no terminal discards were attached in this test"
            );
            drained.tpl
        })
    }

    /// What a rejected commit of `partition` does to the tracker: the
    /// commit's answer as `CommitSlot::settle` hands it over, for a commit
    /// drained under the partition's current assignment.
    fn reject(tracker: &mut OffsetTracker, partition: i32, now: Instant) {
        let position = position_of(tracker, partition);
        tracker.commit_rejected(&[position], Vec::new(), now);
    }

    /// `partition`'s current assignment as a drained commit names it; the
    /// offset plays no part in settling an answer.
    fn position_of(tracker: &OffsetTracker, partition: i32) -> CommitPosition {
        CommitPosition {
            partition,
            offset: 0,
            epoch: tracker
                .partitions
                .get(&partition)
                .map(|tracker| tracker.epoch)
                .expect("the partition is tracked"),
        }
    }

    /// An offset the broker never delivered - a compacted record, or a
    /// transaction's control record - must not hold the commit position
    /// back. Delivered 0, 1 and 3, completed all three, the position is 4.
    #[test]
    fn undelivered_gaps_do_not_block_the_commit_position() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.track_received(0, 1);
        tracker.track_received(0, 3);
        tracker.mark_complete(Completion::plain(0, 3));
        tracker.mark_complete(Completion::plain(0, 0));
        tracker.mark_complete(Completion::plain(0, 1));

        let tpl = drain_tpl(&mut tracker).expect("everything delivered has completed");
        assert_eq!(
            committed_offset(&tpl, 0),
            Some(4),
            "the hole at 2 was never delivered, so it is not waited for"
        );
    }

    /// `position()` is one past the highest delivered offset once nothing
    /// is in flight. At `i64::MAX` that successor does not exist: an
    /// unchecked `+ 1` panics in a debug build and wraps to `i64::MIN` in a
    /// release build, which would offer a negative commit position.
    /// Saturating keeps the commit path free of unchecked arithmetic.
    #[test]
    fn position_saturates_at_the_maximum_offset() {
        let mut tracker = PartitionTracker::new(i64::MAX, 1);
        tracker.mark_complete(i64::MAX, None);
        assert_eq!(tracker.position(), i64::MAX);
    }

    /// A delivered offset whose handler is still running is a real gap:
    /// out-of-order completions commit only up to it, then advance once it
    /// completes.
    #[test]
    fn a_delivered_but_unfinished_offset_still_blocks() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.track_received(0, 1);
        tracker.track_received(0, 2);
        tracker.mark_complete(Completion::plain(0, 2));
        tracker.mark_complete(Completion::plain(0, 0));

        let tpl = drain_tpl(&mut tracker).expect("offset 0 is committable");
        assert_eq!(
            committed_offset(&tpl, 0),
            Some(1),
            "1 is in flight and blocks 2"
        );

        tracker.mark_complete(Completion::plain(0, 1));
        let tpl = drain_tpl(&mut tracker).expect("gap filled");
        assert_eq!(committed_offset(&tpl, 0), Some(3));
    }

    /// With nothing in flight the position is one past the highest delivered
    /// offset, whatever holes the sequence had.
    #[test]
    fn nothing_in_flight_commits_highest_delivered_plus_one() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 10);
        tracker.track_received(0, 14);
        tracker.mark_complete(Completion::plain(0, 10));
        tracker.mark_complete(Completion::plain(0, 14));

        let tpl = drain_tpl(&mut tracker).expect("nothing is in flight");
        assert_eq!(committed_offset(&tpl, 0), Some(15));
        assert!(
            drain_tpl(&mut tracker).is_none(),
            "no new delivery, no new commit"
        );
    }

    /// A replayed lower offset - the same record delivered again after the
    /// partition briefly left and returned - never lowers what the next
    /// drain commits. Delivered and completed 1 to 4, then 1 once more: the
    /// position stays at 5 and exactly one commit is offered.
    #[test]
    fn a_replayed_lower_offset_never_lowers_the_committed_position() {
        let mut tracker = OffsetTracker::new("q".to_string());
        for offset in 1..=4 {
            tracker.track_received(0, offset);
        }
        for offset in 1..=4 {
            tracker.mark_complete(Completion::plain(0, offset));
        }
        tracker.mark_complete(Completion::plain(0, 1));

        let tpl = drain_tpl(&mut tracker).expect("1 to 4 have completed");
        assert_eq!(committed_offset(&tpl, 0), Some(5));
        assert!(
            drain_tpl(&mut tracker).is_none(),
            "the replayed 1 offered nothing, and nothing moved backwards"
        );
    }

    /// `PerRecord` has no interval to scale the threshold by, so it judges
    /// rejected commits by the 60 s floor, like the default interval does.
    #[test]
    fn per_record_commits_keep_the_fence_floor() {
        assert_eq!(
            fence_threshold_over(COMMIT_FENCE_TIMEOUT, commit_window(CommitPolicy::PerRecord)),
            COMMIT_FENCE_TIMEOUT
        );
        assert_eq!(
            fence_threshold_over(
                COMMIT_FENCE_TIMEOUT,
                commit_window(CommitPolicy::Interval(Duration::from_secs(20)))
            ),
            Duration::from_secs(80)
        );
    }

    /// The fence threshold is the 60 s floor for the default interval and
    /// four intervals once the interval is long enough that a recovery
    /// (three drains) would otherwise outlast the floor.
    #[test]
    fn fence_threshold_grows_with_the_commit_interval() {
        assert_eq!(
            fence_threshold_over(COMMIT_FENCE_TIMEOUT, ASYNC_COMMIT_INTERVAL),
            COMMIT_FENCE_TIMEOUT
        );
        assert_eq!(
            fence_threshold_over(COMMIT_FENCE_TIMEOUT, Duration::from_secs(15)),
            COMMIT_FENCE_TIMEOUT,
            "four intervals of 15 s is exactly the floor"
        );
        assert_eq!(
            fence_threshold_over(COMMIT_FENCE_TIMEOUT, Duration::from_secs(30)),
            Duration::from_secs(120),
            "a 30 s interval needs 120 s so three recovery drains fit inside"
        );
    }

    // -- broadcast start position --

    /// The groupless path ignores exactly one error of its own, the
    /// coordinator lookup its inert group id provokes under a restrictive
    /// ACL; every other code is left to the table every receive loop reads,
    /// `recv_error_is_transient`.
    #[test]
    fn only_group_authorization_is_ignorable_on_the_groupless_path() {
        assert!(is_ignorable_groupless_error(
            &KafkaError::MessageConsumption(RDKafkaErrorCode::GroupAuthorizationFailed)
        ));
        for other in [
            KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure),
            KafkaError::MessageConsumption(RDKafkaErrorCode::TopicAuthorizationFailed),
            KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition),
            KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::Fatal),
            KafkaError::Global(RDKafkaErrorCode::GroupAuthorizationFailed),
        ] {
            assert!(
                !is_ignorable_groupless_error(&other),
                "{other:?} is no answer to the inert group id"
            );
        }
    }

    /// A completion for an offset this assignment never delivered belongs to
    /// a previous epoch and must not become a commit position.
    #[test]
    fn a_completion_this_assignment_never_delivered_is_stale() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 105);
        // The old epoch delivered 108 before the partition moved; its handler
        // finishes after the re-seed.
        tracker.mark_complete(Completion::plain(0, 108));
        assert!(
            drain_tpl(&mut tracker).is_none(),
            "105 is still in flight, and 108 was not delivered in this epoch"
        );

        tracker.mark_complete(Completion::plain(0, 105));
        let tpl = drain_tpl(&mut tracker).expect("105 completes");
        assert_eq!(
            committed_offset(&tpl, 0),
            Some(106),
            "the stale 108 did not lift the position past what this epoch handled"
        );
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
        // The re-seed begins a new assignment epoch, which the completion
        // of a record delivered under it carries.
        tracker.remove(0);
        let reseeded = tracker.track_received(0, 100);
        tracker.mark_complete(Completion::under(reseeded.epoch, 0, 100));
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
            epoch: 1,
            discard: Some(TerminalDiscard::Retired(metrics::record_terminal(
                "q",
                None,
                metrics::FailReason::Rejected,
                false,
            ))),
        }
    }

    /// The shutdown drain re-offers the position a running drain has already
    /// offered: the commit that carried it may have been rejected at the
    /// stop or gone unanswered, and with no new completion
    /// `drain_committable` would give the final `Sync` commit nothing to
    /// confirm.
    #[test]
    fn the_shutdown_drain_re_offers_a_position_an_earlier_drain_offered() {
        let mut tracker = OffsetTracker::new("q".to_string());
        for offset in 0..5 {
            tracker.track_received(0, offset);
        }
        for offset in 0..5 {
            tracker.mark_complete(Completion::plain(0, offset));
        }
        let offered = drain_tpl(&mut tracker).expect("progress to 5 is offered");
        assert_eq!(committed_offset(&offered, 0), Some(5));

        assert!(
            tracker.drain_committable().is_none(),
            "nothing new completed, so the running drain offers nothing"
        );

        let (again, discards) = tracker
            .drain_all()
            .expect("the shutdown drain offers every partition");
        assert_eq!(committed_offset(&again, 0), Some(5));
        assert!(discards.is_empty());
    }

    /// The shutdown drain covers every partition the member holds, each at
    /// its own position, including one with nothing completed under this
    /// assignment, whose position is the offset it started at.
    #[test]
    fn the_shutdown_drain_offers_every_partitions_current_position() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 10);
        tracker.mark_complete(Completion::plain(0, 10));
        // Delivered and still in flight: the position is the offset itself.
        tracker.track_received(1, 7);
        // 4 completed behind 3, which is still in flight and holds the position.
        tracker.track_received(2, 3);
        tracker.track_received(2, 4);
        tracker.mark_complete(Completion::plain(2, 4));

        let (tpl, discards) = tracker.drain_all().expect("three partitions");
        assert_eq!(committed_offset(&tpl, 0), Some(11));
        assert_eq!(committed_offset(&tpl, 1), Some(7));
        assert_eq!(committed_offset(&tpl, 2), Some(3));
        assert!(discards.is_empty());
    }

    /// The discards below the offered position ride the shutdown drain, so
    /// the final commit's result settles them as a running commit's would.
    #[test]
    fn the_shutdown_drain_hands_out_the_discards_its_positions_retire() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.track_received(0, 1);
        tracker.mark_complete(terminal(0));

        let (tpl, discards) = tracker.drain_all().expect("partition 0");
        assert_eq!(committed_offset(&tpl, 0), Some(1), "1 is still in flight");
        assert_eq!(discards.len(), 1, "the discard riding offset 0 is covered");
        for discard in discards {
            discard.survived();
        }
    }

    /// A partition revoked while its work is pending leaves the final
    /// commit: its tracker is dropped with the partition, so the member
    /// commits nothing it no longer owns and the new owner redelivers. The
    /// partition the member keeps commits its own position.
    #[test]
    fn a_partition_revoked_while_work_is_pending_is_not_in_the_final_commit() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 5);
        tracker.track_received(0, 6);
        tracker.mark_complete(Completion::plain(0, 5));
        tracker.track_received(1, 2);
        tracker.mark_complete(Completion::plain(1, 2));

        // Partition 0 moves to another member with offset 6 still in flight,
        // and its late completion arrives after the move.
        tracker.remove(0);
        tracker.mark_complete(Completion::plain(0, 6));

        let (tpl, discards) = tracker.drain_all().expect("partition 1 remains");
        assert_eq!(
            committed_offset(&tpl, 0),
            None,
            "a revoked partition is not in the final commit"
        );
        assert_eq!(committed_offset(&tpl, 1), Some(3));
        assert!(discards.is_empty());
    }

    /// The one arm of `drain_all` a position the tracker computes cannot
    /// reach: `add_partition_offset` refuses an offset below zero, so a
    /// tracker seeded below zero drives it. The partition's position is not
    /// carried, the refusal is logged at error level with the partition and
    /// the position, and the discards that position would have retired are
    /// settled as survived instead of riding a commit that could confirm
    /// them without having carried them. The other partition rides as
    /// usual.
    #[test]
    fn a_partition_the_commit_cannot_carry_keeps_its_discards_out_of_the_commit() {
        use std::sync::{Arc, Mutex};
        use tracing::field::{Field, Visit};
        use tracing::subscriber::with_default;
        use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

        /// The fields of one event: `partition` and `offset` as recorded,
        /// and the message text.
        #[derive(Debug, Default)]
        struct Fields {
            partition: Option<i64>,
            offset: Option<i64>,
            message: String,
        }

        impl Visit for Fields {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.message = format!("{value:?}");
                }
            }
            fn record_i64(&mut self, field: &Field, value: i64) {
                match field.name() {
                    "partition" => self.partition = Some(value),
                    "offset" => self.offset = Some(value),
                    _ => {}
                }
            }
        }

        /// Collects the fields of every ERROR event.
        #[derive(Clone, Default)]
        struct Errors(Arc<Mutex<Vec<Fields>>>);

        impl<S: tracing::Subscriber> Layer<S> for Errors {
            fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
                if *event.metadata().level() == tracing::Level::ERROR {
                    let mut fields = Fields::default();
                    event.record(&mut fields);
                    self.0.lock().unwrap().push(fields);
                }
            }
        }

        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, -2);
        tracker.track_received(0, -1);
        tracker.mark_complete(terminal(-2));
        tracker.track_received(1, 4);
        tracker.mark_complete(Completion::plain(1, 4));

        let errors = Errors::default();
        let drained = with_default(tracing_subscriber::registry().with(errors.clone()), || {
            tracker.drain_all()
        });
        let (tpl, discards) = drained.expect("two partitions");
        assert_eq!(
            committed_offset(&tpl, 0),
            None,
            "a position below zero is not carried"
        );
        assert_eq!(
            committed_offset(&tpl, 1),
            Some(5),
            "the other partition rides"
        );
        assert!(
            discards.is_empty(),
            "the discard of the partition left out was settled, not handed to the commit"
        );
        let logged = errors.0.lock().unwrap();
        assert_eq!(
            logged.len(),
            1,
            "the refusal is logged once at error level: {logged:?}"
        );
        let refusal = &logged[0];
        assert_eq!(refusal.partition, Some(0), "the partition left out");
        assert_eq!(
            refusal.offset,
            Some(-1),
            "the position the commit cannot carry"
        );
        assert!(
            refusal
                .message
                .contains("could not add the partition to the final commit"),
            "the log names the refusal: {}",
            refusal.message
        );
    }

    /// A member that never held a partition has nothing to commit, so the
    /// shutdown arm makes no commit and ends clean.
    #[test]
    fn the_shutdown_drain_offers_nothing_with_no_partition() {
        let mut tracker = OffsetTracker::new("q".to_string());
        assert!(tracker.drain_all().is_none());
    }

    /// The discard surfaces on the drain that commits past its offset, and
    /// not before — a gap holding the commit back also holds the accounting.
    #[test]
    fn terminal_discard_surfaces_only_once_its_offset_is_committable() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.track_received(0, 1);
        tracker.mark_complete(terminal(1));

        assert!(
            tracker.drain_committable().is_none(),
            "offset 0 is still in flight, so nothing commits and nothing is counted"
        );

        tracker.mark_complete(Completion::plain(0, 0));
        let DrainedCommit { tpl, discards, .. } = tracker
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

    /// An Ack-only batch reports no discards: nothing rides its commit.
    #[test]
    fn a_batch_without_terminal_outcomes_reports_no_discards() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(Completion::plain(0, 0));
        let drained = tracker.drain_committable().expect("offset 0 commits");
        assert!(drained.discards.is_empty());
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

    /// A commit the coordinator did not accept hands its discards back:
    /// the re-offer of the position carries them again, so the commit that
    /// lands retires them, once. The discard rides its own offset, so it is
    /// covered by the same position it was drained under.
    #[test]
    fn a_re_pended_discard_rides_the_re_offer() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(terminal(0));
        let drained = tracker.drain_committable().expect("offset 0 commits");
        assert_eq!(drained.discards.len(), 1);
        assert_eq!(
            (drained.discards[0].partition, drained.discards[0].offset),
            (0, 0)
        );

        // The commit was rejected: hand the discard back and re-offer.
        tracker.commit_rejected(&drained.positions, drained.discards, Instant::now());
        let DrainedCommit { tpl, discards, .. } =
            tracker.drain_committable().expect("the re-offer");
        assert_eq!(committed_offset(&tpl, 0), Some(1), "the same position");
        assert_eq!(discards.len(), 1, "the discard rides the re-offer");

        // Accepted this time: nothing is left to carry.
        for discard in discards {
            discard.confirm();
        }
        assert!(tracker.drain_committable().is_none());
    }

    /// A discard handed back for a partition revoked meanwhile is settled as
    /// survived, the rule `remove` applies, and never re-pended on a tracker
    /// that no longer exists.
    #[test]
    fn a_re_pended_discard_of_a_revoked_partition_is_settled_survived() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(terminal(0));
        let drained = tracker.drain_committable().expect("offset 0 commits");
        tracker.remove(0);
        tracker.re_pend(drained.discards);
        assert!(
            tracker.drain_committable().is_none(),
            "nothing is tracked for the revoked partition"
        );
    }

    /// The answer to a commit drained under an earlier assignment settles
    /// nothing on the current one. The partition moves away and comes back
    /// while the commit is in flight, and its first record is delivered
    /// again, from the position nothing committed past. The old commit's
    /// rejection then re-offers nothing for the new assignment and starts no
    /// streak on it, and its discard is settled as survived instead of
    /// riding the new assignment's commits: the record it rode on is being
    /// handled again.
    #[test]
    fn an_answer_drained_under_an_earlier_assignment_settles_nothing_on_the_current_one() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(terminal(0));
        let drained = tracker.drain_committable().expect("offset 0 commits");

        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        tx.send(RebalanceEvent::Assign(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx);
        let again = tracker.track_received(0, 0);
        let now = Instant::now();
        tracker.commit_rejected(&drained.positions, drained.discards, now);

        assert!(
            tracker.drain_committable().is_none(),
            "the old assignment's rejection re-offers nothing for the new one"
        );
        assert_eq!(
            tracker.fenced(now + Duration::from_secs(120), Duration::from_secs(60)),
            None,
            "and starts no streak on it"
        );
        tracker.mark_complete(Completion::under(again.epoch, 0, 0));
        let redelivered = tracker
            .drain_committable()
            .expect("the new assignment commits its own delivery");
        assert!(
            redelivered.discards.is_empty(),
            "the old discard was settled as survived and left off the new assignment"
        );
    }

    /// An accepted commit ends its partitions' streak at once: a member
    /// whose commits land is not fenced, whatever came before.
    #[test]
    fn an_accepted_commit_ends_the_streak() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();
        reject(&mut tracker, 0, t0);
        let drained = tracker.drain_committable().expect("the re-offer");
        tracker.commit_accepted(&drained.positions);
        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(61), Duration::from_secs(60)),
            None,
            "the accepted re-offer resolved the streak"
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

    /// librdkafka's seek after a detected log truncation delivers offsets
    /// below the committed position inside the same assignment, with no
    /// revoke or assign in between. The position must not drop below what
    /// is committed, and the lower offset must leave no entry behind that
    /// would pin the position once higher offsets complete.
    #[test]
    fn a_lower_offset_inside_one_assignment_never_lowers_the_position() {
        let mut tracker = OffsetTracker::new("q".to_string());
        for offset in 10..=12 {
            tracker.track_received(0, offset);
        }
        tracker.mark_complete(Completion::plain(0, 10));
        tracker.mark_complete(Completion::plain(0, 11));
        let tpl = drain_tpl(&mut tracker).expect("10 and 11 completed");
        assert_eq!(committed_offset(&tpl, 0), Some(12));

        // The truncation seek: offsets 5 and 6 arrive after 12 was delivered.
        tracker.track_received(0, 5);
        tracker.track_received(0, 6);
        let partition = &tracker.partitions[&0];
        assert_eq!(partition.position(), 12, "the position never lowers");
        assert_eq!(
            partition.in_flight.iter().copied().collect::<Vec<_>>(),
            vec![12],
            "offsets below the committed position are not tracked"
        );
        assert!(
            drain_tpl(&mut tracker).is_none(),
            "nothing below the committed position is ever offered"
        );

        // Their completions are stale for this epoch and leave nothing behind.
        tracker.mark_complete(Completion::plain(0, 5));
        tracker.mark_complete(Completion::plain(0, 6));
        assert!(drain_tpl(&mut tracker).is_none());
        let partition = &tracker.partitions[&0];
        assert_eq!(partition.position(), 12);
        assert_eq!(partition.in_flight.len(), 1, "no entry leaked");

        tracker.mark_complete(Completion::plain(0, 12));
        let tpl = drain_tpl(&mut tracker).expect("12 completed");
        assert_eq!(
            committed_offset(&tpl, 0),
            Some(13),
            "the higher offsets commit as if the lower ones never came"
        );
        assert!(tracker.partitions[&0].in_flight.is_empty());
    }

    /// The belt under the braces: even if an offset below the committed
    /// position sat in `in_flight`, its completion removes it instead of
    /// short-circuiting past the removal and pinning the position there.
    #[test]
    fn a_stale_completion_removes_its_in_flight_entry() {
        let mut partition = PartitionTracker::new(10, 1);
        partition.mark_complete(10, None);
        assert_eq!(
            partition.drain_committable().map(|(next, _)| next),
            Some(11)
        );
        partition.track_received(12);
        partition.in_flight.insert(5);
        assert_eq!(
            partition.position(),
            5,
            "the leaked entry is what the fix removes"
        );

        partition.mark_complete(5, None);
        assert!(!partition.in_flight.contains(&5), "the stale entry is gone");
        assert_eq!(
            partition.position(),
            12,
            "the position is the real in-flight offset again"
        );
        assert_eq!(
            partition.drain_committable().map(|(next, _)| next),
            Some(12),
            "the position the leaked entry held back is offered, and nothing lower"
        );
    }

    /// A rejected commit makes the partition re-offer its current commit
    /// position on the next drain even though no new completions arrived —
    /// and only once (the flag clears).
    #[test]
    fn commit_failed_re_offers_current_position_once() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        tracker.mark_complete(Completion::plain(0, 0));
        let tpl = drain_tpl(&mut tracker).expect("initial commit");
        assert_eq!(committed_offset(&tpl, 0), Some(1));

        // The commit of offset 1 was rejected mid-rebalance.
        reject(&mut tracker, 0, Instant::now());
        let tpl = drain_tpl(&mut tracker)
            .expect("failed commit must be re-offered without new completions");
        assert_eq!(committed_offset(&tpl, 0), Some(1));

        assert!(
            drain_tpl(&mut tracker).is_none(),
            "retry flag must clear after one re-offer"
        );
    }

    /// The drain reports an assign event and nothing else: a revoke is not
    /// what makes a per-record loop re-apply its pause, a partition handed
    /// to this member is.
    #[test]
    fn apply_rebalance_events_reports_only_an_assign() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        assert!(!tracker.apply_rebalance_events(&rx));
        tx.send(RebalanceEvent::Assign(vec![0])).unwrap();
        assert!(tracker.apply_rebalance_events(&rx));
        assert!(
            !tracker.apply_rebalance_events(&rx),
            "an empty channel reports nothing"
        );
    }

    /// Any rebalance event re-offers every retained partition's current
    /// position: a commit made during the rebalance can be refused with a
    /// rebalance in progress, so positions are re-asserted once the group
    /// settles.
    #[test]
    fn rebalance_event_re_offers_retained_partition_positions() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(4, 0);
        tracker.mark_complete(Completion::plain(4, 0));
        let tpl = drain_tpl(&mut tracker).expect("initial commit");
        assert_eq!(committed_offset(&tpl, 4), Some(1));

        // Another member joins: partitions 0-3 move away; the commit of
        // (4, 1) submitted during the rebalance may have been refused.
        tx.send(RebalanceEvent::Revoke(vec![0, 1, 2, 3])).unwrap();
        tracker.apply_rebalance_events(&rx);
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
        let drained = tracker.drain_committable().expect("initial commit");

        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx);
        tracker.commit_rejected(&drained.positions, drained.discards, Instant::now());
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
        tracker.apply_rebalance_events(&rx);

        tracker.mark_complete(Completion::plain(0, 5));
        tracker.mark_complete(Completion::plain(1, 7));
        tracker.mark_complete(Completion::plain(2, 9));
        let tpl = drain_tpl(&mut tracker).expect("partition 2 commits");
        assert_eq!(committed_offset(&tpl, 0), None, "revoked: removed");
        assert_eq!(committed_offset(&tpl, 1), None, "reassigned: removed");
        assert_eq!(committed_offset(&tpl, 2), Some(10), "untouched partition");
    }

    // -- assignment epochs and revoke tokens --

    /// A completion carries the epoch of the assignment its delivery was
    /// tracked under, and one from an earlier assignment of the partition
    /// cannot complete the current assignment's delivery of the same offset.
    /// Proved with an injected completion: the broker-backed shape needs the
    /// old handler's completion to land after the partition came back and
    /// its first record was tracked again, and before that record's own
    /// handler finished, an interleaving a real group cannot be made to
    /// stage on cue.
    #[test]
    fn a_completion_from_an_earlier_assignment_cannot_complete_a_later_delivery() {
        let (tx, rx) = std_mpsc::channel();
        let mut tracker = OffsetTracker::new("q".to_string());
        let first = tracker.track_received(0, 7);

        // The partition moves away with offset 7 in flight and comes back;
        // the first record under the new assignment is offset 7 again.
        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        tx.send(RebalanceEvent::Assign(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx);
        let second = tracker.track_received(0, 7);
        assert_ne!(first.epoch, second.epoch, "a reassign begins a new epoch");

        tracker.mark_complete(Completion::under(first.epoch, 0, 7));
        assert!(
            drain_tpl(&mut tracker).is_none(),
            "the old handler's completion must not move the commit past a record whose handler still runs"
        );

        tracker.mark_complete(Completion::under(second.epoch, 0, 7));
        let tpl = drain_tpl(&mut tracker).expect("the current delivery's own completion commits");
        assert_eq!(committed_offset(&tpl, 0), Some(8));
    }

    /// A revoke cancels the token every in-place wait on the partition
    /// selects on, through the map the rebalance callback cancels in and
    /// through the tracker's own removal as the backstop. A retained
    /// partition keeps its token, and a reassign gets a fresh one under a
    /// new epoch.
    #[test]
    fn a_revoke_cancels_the_assignment_token_and_a_reassign_gets_a_fresh_one() {
        let (tx, rx) = std_mpsc::channel();
        let assignments = AssignmentTokens::default();
        let mut tracker = OffsetTracker::with_assignments("q".to_string(), assignments.clone());
        let first = tracker.track_received(0, 0);
        let other = tracker.track_received(1, 0);
        assert!(!first.revoked.is_cancelled());

        // The callback's path, inside the poll that reported the revoke.
        cancel_assignments(&assignments, &[0]);
        assert!(
            first.revoked.is_cancelled(),
            "the revoked partition's token is cancelled"
        );
        assert!(
            !other.revoked.is_cancelled(),
            "the retained partition's token is not"
        );

        // The loop drains the same revoke next, and the reassign seeds the
        // partition again.
        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        tx.send(RebalanceEvent::Assign(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx);
        let again = tracker.track_received(0, 0);
        assert!(
            !again.revoked.is_cancelled(),
            "the reassign gets a fresh token"
        );
        assert_ne!(again.epoch, first.epoch, "under a new epoch");

        // The tracker's own path, the backstop, when it drains a revoke.
        tx.send(RebalanceEvent::Revoke(vec![1])).unwrap();
        tracker.apply_rebalance_events(&rx);
        assert!(
            other.revoked.is_cancelled(),
            "the tracker cancels as the backstop"
        );
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
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();
        reject(&mut tracker, 0, t0);

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
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        reject(&mut tracker, 0, t0);

        // Every re-offer fails again immediately — no gap wide enough to
        // resolve the streak, and no revoke/assign ever arrives.
        let t1 = t0 + Duration::from_secs(30);
        let _ = drain_tpl(&mut tracker);
        reject(&mut tracker, 0, t1);

        let t2 = t0 + Duration::from_secs(61);
        assert_eq!(
            tracker.fenced(t2, Duration::from_secs(60)),
            Some(0),
            "partition 0 has been continuously dirty since t0"
        );
    }

    /// A configured commit interval raises the threshold the loop asks
    /// `fenced` about (see `fence_threshold`), and the tracker honours the
    /// raised value: the streak that trips the 60 s floor is still within
    /// bounds under a 120 s threshold, and trips it only once 120 s have
    /// passed.
    #[test]
    fn fenced_honours_a_raised_threshold() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        reject(&mut tracker, 0, t0);
        let _ = drain_tpl(&mut tracker);
        reject(&mut tracker, 0, t0 + Duration::from_secs(30));

        let raised = fence_threshold_over(COMMIT_FENCE_TIMEOUT, Duration::from_secs(30));
        assert_eq!(raised, Duration::from_secs(120));
        let past_floor = t0 + Duration::from_secs(61);
        assert_eq!(
            tracker.fenced(past_floor, COMMIT_FENCE_TIMEOUT),
            Some(0),
            "the floor alone would fence this streak"
        );
        assert_eq!(
            tracker.fenced(past_floor, raised),
            None,
            "under the raised threshold the streak is still within bounds"
        );
        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(121), raised),
            Some(0),
            "past four intervals the raised threshold fences it"
        );
    }

    /// A *single* quiet drain does not resolve the streak: the answer to a
    /// re-offer can outlast one iteration, so silence one iteration after a
    /// re-offer is not yet evidence of success — a genuinely wedged
    /// partition looks identical at this point, and its next rejection may
    /// not land for another iteration.
    #[test]
    fn fenced_streak_survives_a_single_quiet_drain() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        reject(&mut tracker, 0, t0);
        let _ = drain_tpl(&mut tracker); // re-offers the failed commit
        let _ = drain_tpl(&mut tracker); // 1st quiet drain: not yet resolved

        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(61), Duration::from_secs(60)),
            Some(0),
            "one quiet drain is within a commit round trip; streak must keep aging"
        );
    }

    /// A partition whose re-offered commit actually landed must NOT age into
    /// a fence. The accepted answer resolves the streak at once (see
    /// `an_accepted_commit_ends_the_streak`); `QUIET_DRAINS_TO_RESOLVE`
    /// consecutive quiet drains resolve it too, without one.
    ///
    /// Regression: `dirty_since` was previously only ever cleared by a
    /// resolving rebalance, so a single transient rejection on an otherwise
    /// healthy consumer forced a spurious reconnect exactly
    /// `COMMIT_FENCE_TIMEOUT` later — rebalances are rare in a stable group,
    /// so nothing intervened.
    #[test]
    fn fenced_does_not_fire_after_a_transient_failure_recovers() {
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        reject(&mut tracker, 0, t0);
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
        let mut tracker = OffsetTracker::new("q".to_string());
        tracker.track_received(0, 0);
        let t0 = Instant::now();

        reject(&mut tracker, 0, t0);
        let _ = drain_tpl(&mut tracker);
        for _ in 0..QUIET_DRAINS_TO_RESOLVE {
            let _ = drain_tpl(&mut tracker);
        }

        // An unrelated rejection an hour later.
        let t1 = t0 + Duration::from_secs(3_600);
        reject(&mut tracker, 0, t1);

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
        reject(&mut tracker, 0, t0);

        let t1 = t0 + Duration::from_secs(90);
        assert_eq!(tracker.fenced(t1, Duration::from_secs(60)), Some(0));

        tx.send(RebalanceEvent::Revoke(vec![0])).unwrap();
        tracker.apply_rebalance_events(&rx);
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
        for _ in 0..60 {
            tx.send(RebalanceEvent::Revoke(vec![1])).unwrap();
            tracker.apply_rebalance_events(&rx);
            tx.send(RebalanceEvent::Assign(vec![])).unwrap();
            tracker.apply_rebalance_events(&rx);
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

        reject(&mut tracker, 0, t0);

        // A rebalance at t0+30s retains partition 0 and resolves the streak.
        let t1 = t0 + Duration::from_secs(30);
        tx.send(RebalanceEvent::Assign(vec![])).unwrap();
        tracker.apply_rebalance_events(&rx);
        assert_eq!(
            tracker.fenced(t0 + Duration::from_secs(61), Duration::from_secs(60)),
            None,
            "the rebalance resolved the pre-existing streak"
        );

        // Commits start being rejected again right after.
        reject(&mut tracker, 0, t1);
        assert_eq!(
            tracker.fenced(t1 + Duration::from_secs(61), Duration::from_secs(60)),
            Some(0),
            "a genuinely wedged partition still trips the fence, timed from t1"
        );
    }

    /// The wake-arm predicate mirrors `drain_committable`'s
    /// `progressed || retry`: nothing pending on a fresh tracker, pending
    /// when the next offset to commit completed, NOT pending when only a
    /// gapped completion exists (exactly as it would not commit), pending
    /// again while a rejected commit awaits re-offer.
    #[test]
    fn has_committable_mirrors_drain_conditions() {
        let mut tracker = OffsetTracker::new("q".to_string());
        assert!(!tracker.has_committable(), "empty tracker");

        tracker.track_received(0, 0);
        assert!(!tracker.has_committable(), "received, nothing completed");

        tracker.mark_complete(Completion::plain(0, 1));
        assert!(
            !tracker.has_committable(),
            "only a gapped completion (offset 0 still in flight)"
        );

        tracker.mark_complete(Completion::plain(0, 0));
        assert!(tracker.has_committable(), "contiguous completion pending");

        drain_tpl(&mut tracker).expect("gap filled, commit offered");
        assert!(!tracker.has_committable(), "drained clean");

        reject(&mut tracker, 0, Instant::now());
        assert!(tracker.has_committable(), "rejected commit awaits re-offer");
    }

    /// A batch flush retiring a span completes every tracked offset in it,
    /// and the records a rewound span left in flight past it stay so: the
    /// position stops at the first of them.
    #[test]
    fn a_retired_span_leaves_the_records_after_it_in_flight() {
        let mut tracker = OffsetTracker::new("q".to_string());
        let epoch = tracker.track_received(0, 100).epoch;
        for offset in 101..150 {
            tracker.track_received(0, offset);
        }
        // A rewind redelivered 100..=119, and that span was acked.
        tracker.retire_span(0, epoch, 100, 120, false);
        assert_eq!(
            committed_offset(&drain_tpl(&mut tracker).expect("committable"), 0),
            Some(120)
        );

        tracker.retire_span(0, epoch, 120, 150, false);
        assert_eq!(
            committed_offset(&drain_tpl(&mut tracker).expect("committable"), 0),
            Some(150)
        );
    }

    /// A span the flush committed itself, and the coordinator accepted, is
    /// not offered again, and it ends the partition's rejected streak.
    #[test]
    fn a_span_committed_by_its_flush_is_not_offered_again() {
        let mut tracker = OffsetTracker::new("q".to_string());
        let epoch = tracker.track_received(0, 0).epoch;
        tracker.track_received(0, 1);
        let now = Instant::now();
        reject(&mut tracker, 0, now);
        assert!(
            tracker
                .fenced(now + Duration::from_secs(61), Duration::from_secs(60))
                .is_some()
        );

        tracker.retire_span(0, epoch, 0, 2, true);
        assert!(
            tracker
                .fenced(now + Duration::from_secs(61), Duration::from_secs(60))
                .is_none(),
            "an accepted commit ends the streak"
        );
        // The rejection's re-offer still goes out once, with the position the
        // flush committed; nothing past it is new.
        assert_eq!(
            committed_offset(&drain_tpl(&mut tracker).expect("re-offer"), 0),
            Some(2)
        );
        assert!(drain_tpl(&mut tracker).is_none(), "nothing new to offer");
    }

    /// A span retired under an assignment the partition has left settles
    /// nothing on the one that followed.
    #[test]
    fn a_span_of_an_earlier_assignment_retires_nothing() {
        let mut tracker = OffsetTracker::new("q".to_string());
        let earlier = tracker.track_received(0, 0).epoch;
        tracker.remove(0);
        tracker.track_received(0, 0);
        tracker.retire_span(0, earlier, 0, 1, false);
        assert!(
            drain_tpl(&mut tracker).is_none(),
            "the current delivery is still in flight"
        );
    }
}

/// The receive loop's commit rate gate. What these pin down: an ungated loop
/// committed once per iteration that advanced any offset — roughly once per
/// message at speed — and consumer close then drained tens of thousands of
/// in-flight commits for tens of seconds (CAF: post-cancel drain timeouts).
#[cfg(test)]
mod async_commit_gate_tests {
    use super::*;

    const INTERVAL: Duration = Duration::from_millis(500);

    #[test]
    fn due_immediately_when_nothing_ever_committed() {
        let gate = AsyncCommitGate::new(CommitPolicy::Interval(INTERVAL));
        let now = Instant::now();
        assert!(gate.due(now), "first commit must not wait out an interval");
        assert_eq!(
            gate.deadline(now),
            now + INTERVAL,
            "the None branch is unreachable from the wake arm (a due gate \
             means the drain already ran), so it pins a bounded future wake \
             rather than a ready-now deadline that could hot-spin if a \
             refactor ever skips the drain"
        );
    }

    #[test]
    fn mark_closes_the_window_and_the_interval_reopens_it() {
        let mut gate = AsyncCommitGate::new(CommitPolicy::Interval(INTERVAL));
        let t0 = Instant::now();
        gate.mark(t0);
        assert!(!gate.due(t0));
        assert!(!gate.due(t0 + INTERVAL / 2));
        assert!(gate.due(t0 + INTERVAL));
        assert!(gate.due(t0 + INTERVAL * 3), "an old window stays open");
    }

    /// A denied `due` must not push the window back: it is anchored to the
    /// last *issued* commit, however often the loop polls it.
    #[test]
    fn asking_does_not_advance_the_window() {
        let mut gate = AsyncCommitGate::new(CommitPolicy::Interval(INTERVAL));
        let t0 = Instant::now();
        gate.mark(t0);
        for tenths in 1..=9u32 {
            assert!(!gate.due(t0 + INTERVAL * tenths / 10));
        }
        assert!(gate.due(t0 + INTERVAL));
    }

    #[test]
    fn deadline_is_anchored_to_the_last_commit() {
        let mut gate = AsyncCommitGate::new(CommitPolicy::Interval(INTERVAL));
        let t0 = Instant::now();
        gate.mark(t0);
        assert_eq!(gate.deadline(t0), t0 + INTERVAL);
        assert_eq!(
            gate.deadline(t0 + INTERVAL * 2),
            t0 + INTERVAL,
            "past due, the deadline stays anchored rather than chasing `now`"
        );
    }

    /// An interval the public setters no longer admit, but the gate must not
    /// panic on either: `last + interval` overflows `Instant`, and the checked
    /// form treats an unrepresentable deadline as already due.
    #[test]
    fn commit_gate_deadline_treats_overflow_as_due() {
        let mut gate = AsyncCommitGate::new(CommitPolicy::Interval(Duration::MAX));
        let now = Instant::now();
        assert_eq!(gate.deadline(now), now, "no commit yet: due now");
        gate.mark(now);
        assert_eq!(gate.deadline(now), now, "after a commit: due now");
    }

    /// The gate is built from the configured interval, so a consumer with
    /// `with_commit_interval(5 s)` commits at most every 5 s - not every
    /// `ASYNC_COMMIT_INTERVAL`.
    #[test]
    fn a_configured_interval_replaces_the_default_window() {
        let configured = Duration::from_secs(5);
        let mut gate = AsyncCommitGate::new(CommitPolicy::Interval(configured));
        let t0 = Instant::now();
        gate.mark(t0);
        assert!(
            !gate.due(t0 + ASYNC_COMMIT_INTERVAL * 4),
            "the default window has no bearing on a configured gate"
        );
        assert!(!gate.due(t0 + configured - Duration::from_millis(1)));
        assert!(gate.due(t0 + configured));
        assert_eq!(gate.deadline(t0), t0 + configured);
    }

    /// `PerRecord` has no window: the gate is due with nothing committed
    /// and stays due right after a confirmed commit, however recent, so
    /// every completion is committed as it lands.
    #[test]
    fn per_record_is_due_however_recent_the_last_confirmed_commit() {
        let mut gate = AsyncCommitGate::new(CommitPolicy::PerRecord);
        let t0 = Instant::now();
        assert!(gate.due(t0), "nothing committed yet");
        gate.mark(t0);
        assert!(gate.due(t0), "a confirmed commit opens no window");
        assert!(gate.due(t0 + Duration::from_micros(1)));
    }

    /// A rejected commit under `PerRecord` closes the gate for the default
    /// interval: the re-offer waits it out, so the loop polls in between
    /// and a rebalance behind the rejection can complete. The wake arm's
    /// deadline is the end of that window, and the next confirmed commit
    /// reopens the gate.
    #[test]
    fn per_record_waits_out_the_default_interval_after_a_rejected_commit() {
        let mut gate = AsyncCommitGate::new(CommitPolicy::PerRecord);
        let t0 = Instant::now();
        gate.mark_rejected(t0);
        assert!(!gate.due(t0));
        assert!(!gate.due(t0 + ASYNC_COMMIT_INTERVAL / 2));
        assert_eq!(gate.deadline(t0), t0 + ASYNC_COMMIT_INTERVAL);
        assert!(gate.due(t0 + ASYNC_COMMIT_INTERVAL));
        gate.mark(t0 + ASYNC_COMMIT_INTERVAL);
        assert!(
            gate.due(t0 + ASYNC_COMMIT_INTERVAL),
            "a confirmed re-offer reopens the gate at once"
        );
    }

    /// Under `Interval` a rejected commit consumes the window exactly as an
    /// issued one does, as in 0.15: the policy's pacing is the window.
    #[test]
    fn interval_treats_a_rejected_commit_as_an_issued_one() {
        let mut gate = AsyncCommitGate::new(CommitPolicy::Interval(INTERVAL));
        let t0 = Instant::now();
        gate.mark_rejected(t0);
        assert!(!gate.due(t0 + INTERVAL / 2));
        assert!(gate.due(t0 + INTERVAL));
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
            BatchRebalance {
                moved: BTreeSet::from([1, 2, 5]),
                assigned: true,
            },
            "every partition whose ownership moved should be reported, and the assign"
        );
    }

    /// An assign is reported even when it hands over nothing, as the
    /// tracker's drain reports it, and a revoke alone is no assign.
    #[test]
    fn drain_reports_an_assign_only_when_one_came() {
        let (tx, rx) = std_mpsc::channel();
        tx.send(RebalanceEvent::Revoke(vec![3])).unwrap();
        assert_eq!(
            drain_batch_rebalance_events(&rx),
            BatchRebalance {
                moved: BTreeSet::from([3]),
                assigned: false,
            }
        );
        tx.send(RebalanceEvent::Assign(Vec::new())).unwrap();
        assert_eq!(
            drain_batch_rebalance_events(&rx),
            BatchRebalance {
                moved: BTreeSet::new(),
                assigned: true,
            }
        );
    }

    #[test]
    fn drain_is_empty_when_nothing_happened() {
        let (_tx, rx) = std_mpsc::channel::<RebalanceEvent>();
        assert_eq!(drain_batch_rebalance_events(&rx), BatchRebalance::default());
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

    /// A buffer for a topology with a DLQ, which keeps wire bytes.
    fn buffer() -> BatchBuffer<BufTopic> {
        BatchBuffer::new(8, true)
    }

    /// A buffer for a topology with no DLQ, which keeps none.
    fn bare_buffer() -> BatchBuffer<BufTopic> {
        BatchBuffer::new(8, false)
    }

    fn raw_at(partition: i32, offset: i64) -> RawMessage {
        RawMessage {
            payload: Bytes::from_static(b"poison"),
            key: None,
            headers: RecordHeaders {
                strings: Arc::new(HashMap::new()),
                wire: None,
                partition,
                offset,
            },
        }
    }

    fn raw() -> RawMessage {
        raw_at(0, 0)
    }

    fn metadata() -> MessageMetadata {
        build_message_metadata(
            &Arc::new(HashMap::new()),
            false,
            RecordCoordinates {
                partition: 0,
                offset: 0,
                timestamp_ms: None,
            },
        )
    }

    /// A fresh discard-accounting record, same shape `record_terminal` hands
    /// the receive loop's drop sites — built via `pending_discard` directly so
    /// these tests don't need a `metrics` feature check to also bump
    /// `messages_failed_total`.
    fn pending() -> metrics::PendingDiscard {
        metrics::pending_discard(
            "batch-buffer-test",
            None,
            metrics::FailReason::Oversize,
            false,
        )
    }

    /// The regression: with only decoded messages counted, a poll window of
    /// nothing but poison never reaches `max_batch_size`, so the batch grows
    /// for the whole `max_batch_age` window holding every payload in memory.
    #[test]
    fn dropped_messages_count_toward_the_size_trigger() {
        let mut buf = buffer();
        for _ in 0..5 {
            buf.drop_message((0, 0), raw, "deserialization_error".into(), pending());
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
        buf.push(BufMessage { value: 1 }, metadata(), (0, 0), raw);
        buf.push(BufMessage { value: 2 }, metadata(), (0, 0), raw);
        buf.drop_message((0, 0), raw, "oversize".into(), pending());

        assert_eq!(buf.flush_len(), 3);
    }

    /// With no DLQ declared, `publish_to_dlq` logs and discards, so copying and
    /// holding each poison payload until the flush buys nothing — but the drop
    /// still has to be counted, or its offset never gets committed past.
    #[test]
    fn a_drop_without_a_dlq_parks_no_payload_but_still_counts() {
        let mut buf = bare_buffer();
        buf.drop_message(
            (0, 7),
            || unreachable!("no wire bytes are built without a DLQ"),
            "deserialization_error".into(),
            pending(),
        );

        assert!(
            buf.dropped.iter().all(|d| d.dlq.is_none()),
            "nowhere to publish it to"
        );
        assert_eq!(buf.flush_len(), 1, "the offset still has to be committed");
    }

    /// The wire bytes a `Reject` dead-letters stay index-parallel to the
    /// messages handed to the handler whenever a DLQ is declared, and are
    /// never built without one, where the records' coordinates are kept
    /// instead.
    #[test]
    fn wire_bytes_are_kept_for_every_message_only_with_a_dlq() {
        let mut buf = buffer();
        buf.push(BufMessage { value: 1 }, metadata(), (0, 1), || raw_at(0, 1));
        buf.push(BufMessage { value: 2 }, metadata(), (0, 2), || raw_at(0, 2));
        assert_eq!(buf.raw.len(), buf.messages.len());

        assert!(buf.coordinates.is_empty(), "the wire bytes carry them");

        let mut bare = bare_buffer();
        bare.push(BufMessage { value: 1 }, metadata(), (0, 1), || {
            unreachable!("no wire bytes are built without a DLQ")
        });
        assert!(bare.raw.is_empty());
        assert_eq!(bare.messages.len(), 1);
        assert_eq!(
            bare.coordinates,
            [(0, 1)],
            "without wire bytes the record's coordinates are kept, for the discard a Reject owes"
        );
    }

    /// The messages go to the handler by value, and the buffer starts the
    /// next batch with its full reservation rather than an empty `Vec` that
    /// regrows record by record.
    #[test]
    fn handing_the_messages_over_keeps_the_reservation() {
        let mut buf = buffer();
        buf.push(BufMessage { value: 1 }, metadata(), (0, 0), raw);
        let handed = buf.take_messages();
        assert_eq!(handed.len(), 1);
        assert!(buf.messages.is_empty());
        assert!(
            buf.messages.capacity() >= 8,
            "the next batch starts with the reservation, got {}",
            buf.messages.capacity()
        );
    }

    #[test]
    fn clear_resets_the_drop_count() {
        let mut buf = buffer();
        buf.drop_message((0, 0), raw, "oversize".into(), pending());
        buf.extend_span(0, 3, 1);
        buf.clear();

        assert_eq!(buf.flush_len(), 0);
        assert!(buf.is_empty());
        assert!(buf.epochs.is_empty());
    }

    /// A span with no decoded messages is *not* empty: those offsets still
    /// have to be committed past, or the poison replays on every restart.
    #[test]
    fn an_all_dropped_span_is_not_empty() {
        let mut buf = bare_buffer();
        buf.extend_span(0, 7, 1);
        buf.drop_message(
            (0, 7),
            || unreachable!("no wire bytes are built without a DLQ"),
            "oversize".into(),
            pending(),
        );

        assert!(!buf.is_empty());
        assert_eq!(buf.end.get(&0), Some(&8));
        assert_eq!(buf.start.get(&0), Some(&7));
    }

    /// The exclusive end of a span is `offset + 1`, which does not exist at
    /// `i64::MAX`. The same saturating rule as `PartitionTracker::position`
    /// keeps the batch commit path free of unchecked arithmetic.
    #[test]
    fn extend_span_saturates_at_the_maximum_offset() {
        let mut buf = buffer();
        buf.extend_span(1, i64::MAX, 1);
        assert_eq!(buf.start.get(&1), Some(&i64::MAX));
        assert_eq!(buf.end.get(&1), Some(&i64::MAX));
    }

    /// A span keeps the epoch its partition's first record was tracked
    /// under; the records after it share it.
    #[test]
    fn the_span_keeps_the_epoch_of_its_partitions_first_record() {
        let mut buf = buffer();
        buf.extend_span(0, 3, 2);
        buf.extend_span(0, 4, 2);
        buf.extend_span(1, 9, 5);
        assert_eq!(buf.epochs.get(&0), Some(&2));
        assert_eq!(buf.epochs.get(&1), Some(&5));
    }

    /// Retiring an acked span completes its offsets in the tracker, so the
    /// tracker offers the span's end at its next drain.
    #[test]
    fn retiring_a_span_moves_the_tracked_position_to_its_end() {
        let mut tracker = OffsetTracker::new("batch-buffer-test".into());
        let mut buf = buffer();
        for offset in 10..13 {
            let epoch = tracker.track_epoch(0, offset);
            buf.extend_span(0, offset, epoch);
        }
        buf.retire(&mut tracker, &buf.end, false);
        let drained = tracker
            .drain_committable()
            .expect("the span is committable");
        assert_eq!(position_offsets(&drained.positions), [(0, 13)]);
    }

    fn letters(at: &[(i32, i64)]) -> Vec<DeadLetter> {
        at.iter()
            .map(|&(partition, offset)| DeadLetter {
                raw: raw_at(partition, offset),
                reason: Cow::Borrowed("rejected"),
            })
            .collect()
    }

    /// A dead letter that fails and may land later keeps its partition's
    /// letters behind it untried, and leaves other partitions alone.
    #[test]
    fn a_kept_dead_letter_stops_its_partition_only() {
        let mut run = DeadLetterRun::new(&letters(&[(0, 5), (1, 7), (0, 6)]));
        run.record(
            "batch-buffer-test",
            Ok((0, Err(ShoveError::Connection("refused".into())))),
        );
        assert!(run.skips(0));
        assert!(!run.skips(1));
        run.record("batch-buffer-test", Ok((1, Ok(()))));
        assert!(matches!(run.outcomes.first(), Some(DeadLettered::Kept)));
        assert!(matches!(run.outcomes.get(1), Some(DeadLettered::Landed)));
        assert!(
            matches!(run.outcomes.get(2), Some(DeadLettered::Kept)),
            "a letter never tried stays kept"
        );
    }

    /// One that can never be published is not kept: it is discarded and
    /// committed past. One that fails with an error no retry gets past ends
    /// the run on every partition.
    #[test]
    fn an_unpublishable_letter_is_discarded_and_a_fatal_one_ends_the_run() {
        let mut run = DeadLetterRun::new(&letters(&[(0, 5), (1, 7)]));
        run.record(
            "batch-buffer-test",
            Ok((0, Err(ShoveError::Validation("too large".into())))),
        );
        assert!(matches!(
            run.outcomes.first(),
            Some(DeadLettered::Unpublishable)
        ));
        assert!(!run.skips(0), "an unpublishable letter keeps nothing back");
        run.record(
            "batch-buffer-test",
            Ok((1, Err(ShoveError::Topology("no producer".into())))),
        );
        assert!(matches!(run.outcomes.get(1), Some(DeadLettered::Fatal(_))));
        assert!(run.skips(0) && run.skips(1), "nothing more is tried");
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

    /// The three assignment shapes: a partition still held is a loss, one no
    /// longer held is not, and an unreadable assignment counts as a loss
    /// because the two cannot be told apart.
    #[test]
    fn seek_failure_on_a_revoked_partition_is_not_a_loss() {
        let holding_zero: BTreeSet<i32> = BTreeSet::from([0]);
        assert!(seek_failure_is_loss(Some(&holding_zero), 0), "still held");
        assert!(!seek_failure_is_loss(Some(&holding_zero), 1), "revoked");
        assert!(
            seek_failure_is_loss(None, 1),
            "unknown assignment is the losing case"
        );
    }

    /// A put-back whose seek failed on a partition revoked in the same poll
    /// settles cleanly: the member taking it over resumes from the committed
    /// offset. The same error on a held partition, or with an unreadable
    /// assignment, keeps forcing the reconnect that redelivers the record.
    #[test]
    fn a_put_back_seek_error_on_a_revoked_partition_is_settled_without_a_reconnect() {
        let error = || vec![(1, KafkaError::Seek("Local: Erroneous state".into()))];
        let holding_zero: BTreeSet<i32> = BTreeSet::from([0]);
        let holding_one: BTreeSet<i32> = BTreeSet::from([1]);

        settle_put_back_seek("q", 1, 7, error(), Some(&holding_zero))
            .expect("a revoked partition's failed seek is not a loss");
        assert!(matches!(
            settle_put_back_seek("q", 1, 7, error(), Some(&holding_one)),
            Err(ShoveError::Connection(_))
        ));
        assert!(matches!(
            settle_put_back_seek("q", 1, 7, error(), None),
            Err(ShoveError::Connection(_))
        ));
        settle_put_back_seek("q", 1, 7, Vec::new(), None).expect("a clean seek settles");
    }
}

#[cfg(all(test, feature = "kafka-schema-registry"))]
mod in_place_wait_accounting_tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::schema_registry::SchemaRegistry;
    use crate::topology::TopologyBuilder;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Note {
        id: u32,
    }

    struct Notes;
    impl Topic for Notes {
        type Message = Note;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("in-place-wait-accounting").build())
        }
    }

    struct Noop;
    impl MessageHandler<Notes> for Noop {
        type Context = ();
        async fn handle(&self, _: Note, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    /// A registry that answers 503 to everything and counts the requests.
    async fn unavailable_registry() -> (Arc<SchemaRegistry>, Arc<AtomicUsize>) {
        use axum::extract::State;
        use axum::http::StatusCode;
        use axum::{Router, routing::get};

        async fn unavailable(State(hits): State<Arc<AtomicUsize>>) -> StatusCode {
            hits.fetch_add(1, Ordering::SeqCst);
            StatusCode::SERVICE_UNAVAILABLE
        }
        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/schemas/ids/{id}/versions", get(unavailable))
            .route("/schemas/ids/{id}", get(unavailable))
            .with_state(hits.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock registry");
        let addr = listener.local_addr().expect("mock registry addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock registry serve");
        });
        let registry = SchemaRegistry::builder(format!("http://{addr}"))
            .max_retries(1)
            .build();
        (registry, hits)
    }

    /// The receive loop pauses its assignment for permits held by *waiting*
    /// handlers, and a redelivery that waits on the registry is waiting, not
    /// running: it counts in `in_place_waiters` from its delay through the
    /// decode and the registry retry wait, and uncounts on every exit.
    #[tokio::test]
    async fn redeliver_in_place_counts_the_registry_retry_wait() {
        let (registry, hits) = unavailable_registry().await;
        let waiters = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();

        let task = {
            let registry = registry.clone();
            let waiters = waiters.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let hold = [HoldQueue {
                    name: "in-place-wait-accounting-hold-10ms".into(),
                    delay: Duration::from_millis(10),
                }];
                // A Confluent frame naming a schema id the mock never answers for.
                let mut payload = vec![0u8];
                payload.extend_from_slice(&9u32.to_be_bytes());
                payload.extend_from_slice(br#"{"id":1}"#);
                let accepted: [Arc<str>; 1] = [Arc::from("in-place-wait-accounting-value")];
                let decode = BatchDecodeCtx {
                    queue: "in-place-wait-accounting",
                    schema_registry: Some(&registry),
                    schema_enforcement: SchemaEnforcement::Enforce,
                    schema_accepted: &accepted,
                    schema_message_index: None,
                    registry_lookup_bound: Duration::from_secs(60),
                };
                let handler = Arc::new(Noop);
                let ctx = Arc::new(());
                let headers = Arc::new(HashMap::new());
                let never_revoked = CancellationToken::new();
                let end = redeliver_in_place::<Notes, Noop>(
                    &handler,
                    &ctx,
                    &decode,
                    &payload,
                    &headers,
                    RecordCoordinates {
                        partition: 0,
                        offset: 0,
                        timestamp_ms: None,
                    },
                    Outcome::Defer,
                    0,
                    10,
                    &hold,
                    None,
                    None,
                    "in-place-wait-accounting",
                    None,
                    &shutdown,
                    &never_revoked,
                    &waiters,
                )
                .await;
                matches!(end, InPlaceEnd::Cancelled)
            })
        };

        // The registry has been asked, so the task is past its delay and
        // either mid-lookup or inside the registry retry wait.
        let deadline = Instant::now() + Duration::from_secs(10);
        while hits.load(Ordering::SeqCst) == 0 {
            assert!(
                Instant::now() < deadline,
                "the mock registry was never asked"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            waiters.load(Ordering::SeqCst),
            1,
            "a redelivery waiting on the registry counts as a waiting handler"
        );

        shutdown.cancel();
        assert!(
            task.await.expect("redelivery task panicked"),
            "a shutdown during the registry wait ends the redelivery as Cancelled"
        );
        assert_eq!(
            waiters.load(Ordering::SeqCst),
            0,
            "the count is released on exit"
        );
    }

    /// A revoke during the registry retry wait of a redelivery ends it as
    /// `Revoked` at once: that wait selects on the record's assignment token
    /// too. The mock has answered the lookup and its one retry once it has
    /// been asked twice, so the task is in the retry wait, or about to enter
    /// it from a lookup the token ends the same way. The exit is bounded by
    /// half the retry delay: a wait that selected on the sleep alone would
    /// end only when the delay ran out and the next check read the token,
    /// about a second later, while the revoke arm ends it within
    /// milliseconds. The waiter is uncounted.
    #[tokio::test]
    async fn a_revoke_during_the_registry_retry_wait_ends_the_redelivery() {
        let (registry, hits) = unavailable_registry().await;
        let waiters = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let revoked = CancellationToken::new();

        let task = {
            let registry = registry.clone();
            let waiters = waiters.clone();
            let shutdown = shutdown.clone();
            let revoked = revoked.clone();
            tokio::spawn(async move {
                let hold = [HoldQueue {
                    name: "in-place-wait-accounting-hold-10ms".into(),
                    delay: Duration::from_millis(10),
                }];
                let mut payload = vec![0u8];
                payload.extend_from_slice(&9u32.to_be_bytes());
                payload.extend_from_slice(br#"{"id":1}"#);
                let accepted: [Arc<str>; 1] = [Arc::from("in-place-wait-accounting-value")];
                let decode = BatchDecodeCtx {
                    queue: "in-place-wait-accounting",
                    schema_registry: Some(&registry),
                    schema_enforcement: SchemaEnforcement::Enforce,
                    schema_accepted: &accepted,
                    schema_message_index: None,
                    registry_lookup_bound: Duration::from_secs(60),
                };
                let handler = Arc::new(Noop);
                let ctx = Arc::new(());
                let headers = Arc::new(HashMap::new());
                let end = redeliver_in_place::<Notes, Noop>(
                    &handler,
                    &ctx,
                    &decode,
                    &payload,
                    &headers,
                    RecordCoordinates {
                        partition: 0,
                        offset: 0,
                        timestamp_ms: None,
                    },
                    Outcome::Defer,
                    0,
                    10,
                    &hold,
                    None,
                    None,
                    "in-place-wait-accounting",
                    None,
                    &shutdown,
                    &revoked,
                    &waiters,
                )
                .await;
                matches!(end, InPlaceEnd::Revoked)
            })
        };

        let deadline = Instant::now() + Duration::from_secs(10);
        while hits.load(Ordering::SeqCst) < 2 {
            assert!(
                Instant::now() < deadline,
                "the mock registry was not asked twice"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(waiters.load(Ordering::SeqCst), 1);

        let cancelled_at = Instant::now();
        revoked.cancel();
        assert!(
            task.await.expect("redelivery task panicked"),
            "a revoke during the registry retry wait ends the redelivery as Revoked"
        );
        let took = cancelled_at.elapsed();
        assert!(
            took < REGISTRY_RETRY_DELAY / 2,
            "the revoke ends the retry wait at once, not when the delay runs out; took {took:?}"
        );
        assert_eq!(waiters.load(Ordering::SeqCst), 0);
        assert!(!shutdown.is_cancelled());
    }
}

#[cfg(test)]
mod in_place_wait_guard_tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// A guard counts one waiter for as long as it lives, and the drop
    /// uncounts it. A drop that finds nothing to uncount is a broken
    /// invariant: it is reported and the count stays at zero, neither
    /// wrapped round to `usize::MAX`, which would pause the assignment for
    /// good, nor turned into a panic, which a drop on a runtime path never
    /// raises, in a debug build included.
    #[test]
    fn a_drop_without_a_count_is_reported_and_leaves_the_count_at_zero() {
        let waiters = AtomicUsize::new(0);
        {
            let _waiting = InPlaceWait::begin(&waiters);
            assert_eq!(waiters.load(Ordering::SeqCst), 1);
        }
        assert_eq!(
            waiters.load(Ordering::SeqCst),
            0,
            "the drop uncounts the wait"
        );

        drop(InPlaceWait(&waiters));
        assert_eq!(
            waiters.load(Ordering::SeqCst),
            0,
            "an uncount with nothing to uncount leaves zero in place"
        );
    }
}

#[cfg(test)]
mod in_place_revoke_tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::topology::TopologyBuilder;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Note {
        id: u32,
    }

    struct Notes;
    impl Topic for Notes {
        type Message = Note;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("in-place-revoke").build())
        }
    }

    /// A JSON codec that cancels the token left for it as it decodes, so a
    /// test can land a revoke inside a successful decode.
    struct RevokingCodec;

    static REVOKE_ON_DECODE: std::sync::Mutex<Option<CancellationToken>> =
        std::sync::Mutex::new(None);

    impl crate::Codec<Note> for RevokingCodec {
        const NAME: &'static str = "revoking-json";

        fn encode(value: &Note) -> Result<Vec<u8>> {
            Ok(serde_json::to_vec(value)?)
        }

        fn decode(bytes: &[u8]) -> Result<Note> {
            if let Some(token) = REVOKE_ON_DECODE.lock().unwrap().take() {
                token.cancel();
            }
            Ok(serde_json::from_slice(bytes)?)
        }
    }

    struct RevokedNotes;
    impl Topic for RevokedNotes {
        type Message = Note;
        type Codec = RevokingCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("in-place-revoke-decode").build())
        }
    }

    /// A codec that cancels the token left for it and then fails to decode,
    /// so a test can land a revoke inside a decode that condemns the bytes.
    /// Its own token slot, so the test it serves and the one above can run
    /// in one process side by side.
    struct RevokingUndecodableCodec;

    static REVOKE_ON_UNDECODABLE: std::sync::Mutex<Option<CancellationToken>> =
        std::sync::Mutex::new(None);

    impl crate::Codec<Note> for RevokingUndecodableCodec {
        const NAME: &'static str = "revoking-undecodable-json";

        fn encode(value: &Note) -> Result<Vec<u8>> {
            Ok(serde_json::to_vec(value)?)
        }

        fn decode(bytes: &[u8]) -> Result<Note> {
            if let Some(token) = REVOKE_ON_UNDECODABLE.lock().unwrap().take() {
                token.cancel();
            }
            Ok(serde_json::from_slice(bytes)?)
        }
    }

    struct RevokedUndecodableNotes;
    impl Topic for RevokedUndecodableNotes {
        type Message = Note;
        type Codec = RevokingUndecodableCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("in-place-revoke-undecodable").build())
        }
    }

    /// Counts its calls and acks.
    struct Counting(AtomicUsize);
    impl MessageHandler<Notes> for Counting {
        type Context = ();
        async fn handle(&self, _: Note, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Ack
        }
    }
    impl MessageHandler<RevokedNotes> for Counting {
        type Context = ();
        async fn handle(&self, _: Note, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Ack
        }
    }
    impl MessageHandler<RevokedUndecodableNotes> for Counting {
        type Context = ();
        async fn handle(&self, _: Note, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Ack
        }
    }

    /// A revoke that lands inside the decode of the redelivery, after the
    /// select around the decode read the token and before the decode's own
    /// poll completed, ends the wait as `Revoked` before the handler runs.
    /// The codec cancels the token as it decodes; the handler must not see
    /// the record, and the waiter is uncounted.
    #[tokio::test]
    async fn a_revoke_inside_the_decode_ends_the_wait_before_the_handler() {
        let waiters = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let revoked = CancellationToken::new();
        *REVOKE_ON_DECODE.lock().unwrap() = Some(revoked.clone());
        let handler = Arc::new(Counting(AtomicUsize::new(0)));
        let hold = [HoldQueue {
            name: "in-place-revoke-decode-hold-10ms".into(),
            delay: Duration::from_millis(10),
        }];
        let decode = BatchDecodeCtx {
            queue: "in-place-revoke-decode",
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: SchemaEnforcement::Enforce,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted: &[],
            #[cfg(feature = "kafka-schema-registry")]
            schema_message_index: None,
            #[cfg(feature = "kafka-schema-registry")]
            registry_lookup_bound: Duration::from_secs(1),
        };
        let ctx = Arc::new(());
        let headers = Arc::new(HashMap::new());
        let end = redeliver_in_place::<RevokedNotes, Counting>(
            &handler,
            &ctx,
            &decode,
            br#"{"id":1}"#,
            &headers,
            RecordCoordinates {
                partition: 3,
                offset: 9,
                timestamp_ms: None,
            },
            Outcome::Defer,
            0,
            10,
            &hold,
            None,
            None,
            "in-place-revoke-decode",
            None,
            &shutdown,
            &revoked,
            &waiters,
        )
        .await;
        assert!(
            revoked.is_cancelled(),
            "the codec cancelled the token during the decode"
        );
        assert!(
            matches!(end, InPlaceEnd::Revoked),
            "a revoke inside the decode ends the redelivery as Revoked"
        );
        assert_eq!(
            handler.0.load(Ordering::SeqCst),
            0,
            "the record was not handed to the handler"
        );
        assert_eq!(waiters.load(Ordering::SeqCst), 0);
    }

    /// A revoke that lands inside a decode which then condemns the bytes
    /// ends the wait as `Revoked`, not `Undecodable`. `Undecodable` is the
    /// end the caller dead-letters on, from this member; the partition's
    /// next owner dead-letters the same bytes once it is handed them from
    /// the committed offset. One dead letter for the record, not two. The
    /// codec cancels the token and then fails on the bytes; the handler
    /// must not see the record, and the waiter is uncounted.
    #[tokio::test]
    async fn a_revoke_inside_an_undecodable_decode_leaves_the_dead_letter_to_the_next_owner() {
        let waiters = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let revoked = CancellationToken::new();
        *REVOKE_ON_UNDECODABLE.lock().unwrap() = Some(revoked.clone());
        let handler = Arc::new(Counting(AtomicUsize::new(0)));
        let hold = [HoldQueue {
            name: "in-place-revoke-undecodable-hold-10ms".into(),
            delay: Duration::from_millis(10),
        }];
        let decode = BatchDecodeCtx {
            queue: "in-place-revoke-undecodable",
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: SchemaEnforcement::Enforce,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted: &[],
            #[cfg(feature = "kafka-schema-registry")]
            schema_message_index: None,
            #[cfg(feature = "kafka-schema-registry")]
            registry_lookup_bound: Duration::from_secs(1),
        };
        let ctx = Arc::new(());
        let headers = Arc::new(HashMap::new());
        let end = redeliver_in_place::<RevokedUndecodableNotes, Counting>(
            &handler,
            &ctx,
            &decode,
            b"not json",
            &headers,
            RecordCoordinates {
                partition: 3,
                offset: 9,
                timestamp_ms: None,
            },
            Outcome::Defer,
            0,
            10,
            &hold,
            None,
            None,
            "in-place-revoke-undecodable",
            None,
            &shutdown,
            &revoked,
            &waiters,
        )
        .await;
        assert!(
            revoked.is_cancelled(),
            "the codec cancelled the token during the decode"
        );
        assert!(
            !matches!(end, InPlaceEnd::Undecodable { .. }),
            "a revoke inside the decode must not end the redelivery as Undecodable, \
             the end the caller dead-letters on"
        );
        assert!(
            matches!(end, InPlaceEnd::Revoked),
            "a revoke inside an undecodable decode ends the redelivery as Revoked"
        );
        assert_eq!(
            handler.0.load(Ordering::SeqCst),
            0,
            "the record was not handed to the handler"
        );
        assert_eq!(waiters.load(Ordering::SeqCst), 0);
        assert!(!shutdown.is_cancelled());
    }

    /// A revoke of the record's partition during the delay ends the wait as
    /// `Revoked`: the handler is not called again, the waiter is uncounted,
    /// and shutdown is untouched. The delay is a minute, so only the revoke
    /// can end the wait within the test.
    #[tokio::test]
    async fn a_revoke_during_the_delay_ends_the_wait_without_a_redelivery() {
        let waiters = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let revoked = CancellationToken::new();
        let handler = Arc::new(Counting(AtomicUsize::new(0)));
        let task = {
            let waiters = waiters.clone();
            let shutdown = shutdown.clone();
            let revoked = revoked.clone();
            let handler = handler.clone();
            tokio::spawn(async move {
                let hold = [HoldQueue {
                    name: "in-place-revoke-hold-60s".into(),
                    delay: Duration::from_secs(60),
                }];
                let decode = BatchDecodeCtx {
                    queue: "in-place-revoke",
                    #[cfg(feature = "kafka-schema-registry")]
                    schema_registry: None,
                    #[cfg(feature = "kafka-schema-registry")]
                    schema_enforcement: SchemaEnforcement::Enforce,
                    #[cfg(feature = "kafka-schema-registry")]
                    schema_accepted: &[],
                    #[cfg(feature = "kafka-schema-registry")]
                    schema_message_index: None,
                    #[cfg(feature = "kafka-schema-registry")]
                    registry_lookup_bound: Duration::from_secs(1),
                };
                let ctx = Arc::new(());
                let headers = Arc::new(HashMap::new());
                let end = redeliver_in_place::<Notes, Counting>(
                    &handler,
                    &ctx,
                    &decode,
                    br#"{"id":1}"#,
                    &headers,
                    RecordCoordinates {
                        partition: 3,
                        offset: 9,
                        timestamp_ms: None,
                    },
                    Outcome::Defer,
                    0,
                    10,
                    &hold,
                    None,
                    None,
                    "in-place-revoke",
                    None,
                    &shutdown,
                    &revoked,
                    &waiters,
                )
                .await;
                matches!(end, InPlaceEnd::Revoked)
            })
        };

        let deadline = Instant::now() + Duration::from_secs(10);
        while waiters.load(Ordering::SeqCst) == 0 {
            assert!(
                Instant::now() < deadline,
                "the redelivery never began its wait"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        revoked.cancel();
        assert!(
            task.await.expect("redelivery task panicked"),
            "a revoke during the delay ends the redelivery as Revoked"
        );
        assert_eq!(
            handler.0.load(Ordering::SeqCst),
            0,
            "the record was not handed back to the handler"
        );
        assert_eq!(
            waiters.load(Ordering::SeqCst),
            0,
            "the waiter is uncounted on exit"
        );
        assert!(!shutdown.is_cancelled());
    }
}

#[cfg(test)]
mod after_wait_tests {
    use super::*;

    /// The cases of the check the receive loop makes once a wait it polled
    /// through has ended, pinned here because the broker tests reach the
    /// stop case only through a race the stop usually wins earlier, and the
    /// revoke case only when the coordinator moves the partition whose
    /// record is in the loop's hand.
    #[test]
    fn the_stop_is_read_first_then_the_assignment() {
        let live = CancellationToken::new();
        let stopped = CancellationToken::new();
        stopped.cancel();
        let revoked = CancellationToken::new();
        revoked.cancel();

        assert_eq!(after_wait(&live, Some(&live)), AfterWait::Dispatch);
        assert_eq!(
            after_wait(&live, None),
            AfterWait::Dispatch,
            "the broadcast loop has no assignment to lose"
        );
        assert_eq!(
            after_wait(&stopped, Some(&live)),
            AfterWait::Stop,
            "a stop that landed after the wait's own poll of the token"
        );
        assert_eq!(after_wait(&stopped, None), AfterWait::Stop);
        assert_eq!(
            after_wait(&live, Some(&revoked)),
            AfterWait::Revoked,
            "a revoke applied inside the wait's recv poll"
        );
        assert_eq!(
            after_wait(&stopped, Some(&revoked)),
            AfterWait::Stop,
            "the stop is read first"
        );
    }
}

#[cfg(test)]
mod error_classifier_tests {
    use super::*;

    /// librdkafka raises `AutoOffsetReset` when a partition has no committed
    /// offset and the policy is `error`. A reconnect rejoins the same group
    /// under the same policy and meets the same answer, so the classifier
    /// must end the consumer (`Topology`) rather than loop on `Connection`.
    #[test]
    fn a_missing_offset_under_the_error_policy_is_permanent() {
        let e = KafkaError::MessageConsumption(RDKafkaErrorCode::AutoOffsetReset);
        assert!(matches!(
            map_kafka_error("recv", e),
            ShoveError::Topology(msg) if msg.starts_with("recv: ")
        ));
    }

    /// The transient direction stays transient: a broker that is merely
    /// unreachable maps to a `Connection` error, so a call that does end
    /// on it, such as a failed subscribe, reconnects.
    #[test]
    fn an_unreachable_broker_stays_transient() {
        let e = KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure);
        assert!(matches!(
            map_kafka_error("recv", e),
            ShoveError::Connection(_)
        ));
    }

    // -- the receive loops' table, `recv_error_is_transient` --

    fn consumption(code: RDKafkaErrorCode) -> KafkaError {
        KafkaError::MessageConsumption(code)
    }

    /// The table, read through a gate no assignment change has armed.
    fn transient(error: KafkaError) -> bool {
        recv_error_is_transient(&TransportGate::new(GATE_BOUND), &error)
    }

    /// A broker connection that failed or could not be made: librdkafka
    /// reconnects on its own (`rdkafka_broker.c:616-625`).
    #[test]
    fn a_broker_transport_failure_is_ridden_out() {
        assert!(transient(consumption(
            RDKafkaErrorCode::BrokerTransportFailure
        )));
    }

    /// A broker name that did not resolve: the next connect attempt
    /// resolves it again (`rdkafka_broker.c:1127-1139`).
    #[test]
    fn a_name_that_did_not_resolve_is_ridden_out() {
        assert!(transient(consumption(RDKafkaErrorCode::Resolve)));
    }

    /// Every broker down at once: librdkafka re-bootstraps and its brokers
    /// keep reconnecting (`rdkafka_broker.c:369-388`).
    #[test]
    fn every_broker_down_is_ridden_out() {
        assert!(transient(consumption(RDKafkaErrorCode::AllBrokersDown)));
    }

    /// A fatal error ends the loop, whatever code it carries.
    #[test]
    fn a_fatal_error_ends_the_loop() {
        assert!(!transient(KafkaError::MessageConsumptionFatal(
            RDKafkaErrorCode::FencedInstanceId
        )));
        assert!(!transient(KafkaError::MessageConsumptionFatal(
            RDKafkaErrorCode::BrokerTransportFailure
        )));
    }

    /// A `__FATAL` librdkafka did not translate to its underlying code
    /// (`rdkafka_event.h:77-91`) ends the loop too.
    #[test]
    fn an_untranslated_fatal_error_ends_the_loop() {
        assert!(!transient(consumption(RDKafkaErrorCode::Fatal)));
    }

    /// No committed offset under the `error` reset policy: the reconnect
    /// meets the same answer, see `map_kafka_error`.
    #[test]
    fn a_missing_offset_under_the_error_policy_ends_the_loop() {
        assert!(!transient(consumption(RDKafkaErrorCode::AutoOffsetReset)));
    }

    /// librdkafka raises it once it has left the group for a member that
    /// did not poll in time (`rdkafka_cgrp.c:5162-5210`).
    #[test]
    fn an_exceeded_poll_interval_ends_the_loop() {
        assert!(!transient(consumption(RDKafkaErrorCode::PollExceeded)));
    }

    /// A committed-offset fetch that timed out leaves its partitions
    /// unfetched (`rdkafka_assignment.c:281-298`).
    #[test]
    fn a_timed_out_offset_fetch_ends_the_loop() {
        assert!(!transient(consumption(RDKafkaErrorCode::OperationTimedOut)));
    }

    /// A committed-offset fetch the coordinator refused leaves its
    /// partitions unfetched until the assignment changes
    /// (`rdkafka_assignment.c:198-232`, `:301-326`).
    #[test]
    fn a_moved_coordinator_ends_the_loop() {
        assert!(!transient(consumption(RDKafkaErrorCode::NotCoordinator)));
    }

    /// The same, for a coordinator that is not available yet.
    #[test]
    fn an_unavailable_coordinator_ends_the_loop() {
        assert!(!transient(consumption(
            RDKafkaErrorCode::CoordinatorNotAvailable
        )));
    }

    /// The group's coordinator lookup or join was refused. librdkafka
    /// reports a refused lookup once and retries it in silence, and a
    /// refused join on every attempt; either way every retry meets the same
    /// refusal until an operator grants the ACL (`rdkafka_cgrp.c:794-808`,
    /// `:2434-2441`).
    #[test]
    fn a_group_authorization_failure_ends_the_loop() {
        assert!(!transient(consumption(
            RDKafkaErrorCode::GroupAuthorizationFailed
        )));
    }

    /// A fetch the topic's ACL refused. librdkafka reports it once per
    /// partition and goes on fetching in silence after a backoff, so only an
    /// operator's ACL clears it (`rdkafka_fetcher.c:295-307`, `:344`).
    #[test]
    fn a_topic_authorization_failure_ends_the_loop() {
        assert!(!transient(consumption(
            RDKafkaErrorCode::TopicAuthorizationFailed
        )));
    }

    /// A subscribed topic the cluster does not know. librdkafka reports it
    /// once and keeps the subscription, so only an operator creating the
    /// topic clears it (`rdkafka_cgrp.c:5232-5262`).
    #[test]
    fn an_unknown_topic_ends_the_loop() {
        assert!(!transient(consumption(
            RDKafkaErrorCode::UnknownTopicOrPartition
        )));
    }

    /// A TLS handshake that failed for any reason but a disconnect, which
    /// librdkafka reports as `__TRANSPORT` (`rdkafka_ssl.c:644-686`).
    #[test]
    fn a_tls_failure_ends_the_loop() {
        assert!(!transient(consumption(RDKafkaErrorCode::SSL)));
    }

    /// A SASL authentication the broker refused
    /// (`rdkafka_request.c:3425-3433`).
    #[test]
    fn an_authentication_failure_ends_the_loop() {
        assert!(!transient(consumption(RDKafkaErrorCode::Authentication)));
    }

    /// A broker response past `receive.message.max.bytes`: librdkafka
    /// reconnects, and the response the broker sends again is as large
    /// (`rdkafka_broker.c:2130-2141`, `:2174`).
    #[test]
    fn an_oversized_broker_response_ends_the_loop() {
        assert!(!transient(consumption(RDKafkaErrorCode::BadMessage)));
    }

    /// librdkafka out of a system resource: a broker thread it could not
    /// create, which leaves it without that broker
    /// (`rdkafka_broker.c:5050-5059`), or the buffer a decompression needs
    /// (`rdkafka_msgset_reader.c:379`).
    #[test]
    fn an_exhausted_system_resource_ends_the_loop() {
        assert!(!transient(consumption(
            RDKafkaErrorCode::CriticalSystemResource
        )));
    }

    /// A fetch error librdkafka passes on and retries without end
    /// (`rdkafka_fetcher.c:327-342`).
    #[test]
    fn a_fetch_error_ends_the_loop() {
        assert!(!transient(consumption(
            RDKafkaErrorCode::MessageSizeTooLarge
        )));
    }

    /// The end of a partition, which shove never asks librdkafka to report
    /// (`enable.partition.eof` keeps its default, off).
    #[test]
    fn a_partition_end_ends_the_loop() {
        assert!(!transient(KafkaError::PartitionEOF(0)));
    }

    /// The table matches the consumption errors `recv()` returns
    /// (rust-rdkafka 0.39, `base_consumer.rs:241-257`), not a code on its
    /// own.
    #[test]
    fn a_transient_code_outside_a_consumption_error_ends_the_loop() {
        assert!(!transient(KafkaError::Global(
            RDKafkaErrorCode::BrokerTransportFailure
        )));
    }

    // -- the gate the table reads a `__TRANSPORT` through, `TransportGate` --

    /// librdkafka's default `socket.timeout.ms`.
    const GATE_BOUND: Duration = Duration::from_secs(60);

    fn after(origin: std::time::Instant, secs: u64) -> std::time::Instant {
        origin + Duration::from_secs(secs)
    }

    /// A member assigned once, with no change since, rides out every
    /// `__TRANSPORT`, however soon after the assign: a bounce that lands on
    /// a settled member.
    #[test]
    fn a_lone_assign_leaves_the_gate_unarmed() {
        let gate = TransportGate::new(GATE_BOUND);
        let t0 = std::time::Instant::now();
        gate.changed(AssignmentChange::Assign, t0);
        assert!(gate.rides_out(after(t0, 1)));
        assert!(gate.rides_out(after(t0, 2)));
    }

    /// A revoke within the bound of an assign arms the gate, because the
    /// assign's fetch may still have been pending, and the next `__TRANSPORT`
    /// within the bound of the revoke ends the loop.
    #[test]
    fn a_change_within_the_bound_of_an_assign_arms_the_gate() {
        let gate = TransportGate::new(GATE_BOUND);
        let t0 = std::time::Instant::now();
        gate.changed(AssignmentChange::Assign, t0);
        gate.changed(AssignmentChange::Revoke, after(t0, 5));
        assert!(!gate.rides_out(after(t0, 6)));
    }

    /// An assign within the bound of a ridden-out `__TRANSPORT` arms the
    /// gate however long ago the last assign was: that assign's fetch may be
    /// retrying through the connection that failed.
    #[test]
    fn a_change_within_the_bound_of_a_ridden_out_failure_arms_the_gate() {
        let gate = TransportGate::new(GATE_BOUND);
        let t0 = std::time::Instant::now();
        gate.changed(AssignmentChange::Assign, t0);
        assert!(
            gate.rides_out(after(t0, 100)),
            "long after the assign, nothing has armed the gate"
        );
        gate.changed(AssignmentChange::Assign, after(t0, 130));
        assert!(!gate.rides_out(after(t0, 131)));
    }

    /// A revoke and the assign behind it, both past the bound of the last
    /// assign and of any ridden-out `__TRANSPORT`, arm nothing: the rebalance
    /// of a settled member. A revoke starts no fetch, so it opens no window
    /// for the assign behind it.
    #[test]
    fn a_rebalance_long_after_the_last_assign_leaves_the_gate_unarmed() {
        let gate = TransportGate::new(GATE_BOUND);
        let t0 = std::time::Instant::now();
        gate.changed(AssignmentChange::Assign, t0);
        gate.changed(AssignmentChange::Revoke, after(t0, 61));
        gate.changed(AssignmentChange::Assign, after(t0, 62));
        assert!(gate.rides_out(after(t0, 63)));
    }

    /// A revoke of an assignment librdkafka reports lost, as at a session
    /// timeout during a broker outage, arms nothing, however soon after an
    /// assign and a ridden-out `__TRANSPORT` it lands: it takes every
    /// partition, the ones a fetch was querying included, so it strands
    /// none, and the rest of the outage is ridden out.
    #[test]
    fn a_lost_revoke_leaves_the_gate_unarmed() {
        let gate = TransportGate::new(GATE_BOUND);
        let t0 = std::time::Instant::now();
        gate.changed(AssignmentChange::Assign, t0);
        assert!(gate.rides_out(after(t0, 1)), "the outage's first failure");
        gate.changed(AssignmentChange::LostRevoke, after(t0, 10));
        assert!(gate.rides_out(after(t0, 11)));
        assert!(gate.rides_out(after(t0, 40)));
    }

    /// The gate stays armed for the bound after the change that armed it,
    /// that instant included, and rides out every `__TRANSPORT` after it.
    #[test]
    fn an_armed_gate_expires_with_the_bound() {
        let gate = TransportGate::new(GATE_BOUND);
        let t0 = std::time::Instant::now();
        gate.changed(AssignmentChange::Assign, t0);
        gate.changed(AssignmentChange::Revoke, after(t0, 5));
        assert!(!gate.rides_out(after(t0, 65)), "at the bound");
        assert!(gate.rides_out(after(t0, 66)), "past it");
    }

    /// The table reads the gate for a `__TRANSPORT` and a `__RESOLVE`, the
    /// two codes a failing broker fails a queued fetch with: an armed gate
    /// ends the loop on either, and every broker being down stays ridden out.
    #[test]
    fn an_armed_gate_ends_the_loop_on_a_failed_connection_or_resolve() {
        let gate = TransportGate::new(GATE_BOUND);
        let now = std::time::Instant::now();
        gate.changed(AssignmentChange::Assign, now);
        gate.changed(AssignmentChange::Revoke, now);
        assert!(!recv_error_is_transient(
            &gate,
            &consumption(RDKafkaErrorCode::BrokerTransportFailure)
        ));
        assert!(!recv_error_is_transient(
            &gate,
            &consumption(RDKafkaErrorCode::Resolve)
        ));
        assert!(recv_error_is_transient(
            &gate,
            &consumption(RDKafkaErrorCode::AllBrokersDown)
        ));
    }

    /// A `__RESOLVE` the table rides out is stamped like a `__TRANSPORT`: the
    /// first assign after it arms the gate, with no earlier assign to open a
    /// window, and the next `__RESOLVE` ends the loop. Every broker being
    /// down stamps nothing.
    #[test]
    fn a_ridden_out_resolve_failure_arms_the_gate_like_a_transport_failure() {
        let after_resolve = TransportGate::new(GATE_BOUND);
        assert!(recv_error_is_transient(
            &after_resolve,
            &consumption(RDKafkaErrorCode::Resolve)
        ));
        after_resolve.changed(AssignmentChange::Assign, std::time::Instant::now());
        assert!(!recv_error_is_transient(
            &after_resolve,
            &consumption(RDKafkaErrorCode::Resolve)
        ));

        let after_all_down = TransportGate::new(GATE_BOUND);
        assert!(recv_error_is_transient(
            &after_all_down,
            &consumption(RDKafkaErrorCode::AllBrokersDown)
        ));
        after_all_down.changed(AssignmentChange::Assign, std::time::Instant::now());
        assert!(
            recv_error_is_transient(&after_all_down, &consumption(RDKafkaErrorCode::Resolve)),
            "nothing armed the gate"
        );
    }

    /// The stop's drain reads no gate: it rides out a `__TRANSPORT` and a
    /// `__RESOLVE` whatever change came before them, because a stop never
    /// reconnects, and the rest of the table reads as it does everywhere.
    #[test]
    fn the_stops_drain_rides_out_a_failed_connection_or_resolve() {
        let drain = None::<&TransportGate>;
        for code in [
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::Resolve,
            RDKafkaErrorCode::AllBrokersDown,
        ] {
            assert!(
                recv_error_is_transient(drain, &consumption(code)),
                "{code:?} is ridden out"
            );
        }
        assert!(!recv_error_is_transient(
            drain,
            &consumption(RDKafkaErrorCode::GroupAuthorizationFailed)
        ));
    }

    /// The bound is the consumer's `socket.timeout.ms`: the value its config
    /// sets, or librdkafka's default.
    #[test]
    fn the_gate_bound_is_the_socket_timeout() {
        assert_eq!(
            socket_timeout(&ClientConfig::new()).expect("librdkafka's default"),
            GATE_BOUND
        );
        let mut config = ClientConfig::new();
        config.set("socket.timeout.ms", "1500");
        assert_eq!(
            socket_timeout(&config).expect("the configured value"),
            Duration::from_millis(1500)
        );
    }
}

/// The stop's drain, `acquire_permit_while_polling` with no shutdown token,
/// on a real consumer.
#[cfg(test)]
mod stop_drain_tests {
    use super::*;
    use tracing::field::{Field, Visit};
    use tracing::subscriber::set_default;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    /// The fields of one event, read for the warning a ridden-out receive
    /// error logs, see [`log_transient_recv_error`].
    #[derive(Default)]
    struct Fields {
        ridden_out: bool,
        error: Option<String>,
    }

    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            match field.name() {
                "message" => {
                    self.ridden_out =
                        format!("{value:?}").starts_with("broker connection error ridden out");
                }
                "error" => self.error = Some(format!("{value:?}")),
                _ => {}
            }
        }
    }

    /// Collects the error of every ridden-out receive error the current
    /// thread logs.
    #[derive(Clone, Default)]
    struct RiddenOut(Arc<std::sync::Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> Layer<S> for RiddenOut {
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            let mut fields = Fields::default();
            event.record(&mut fields);
            if let (true, Some(error)) = (fields.ridden_out, fields.error) {
                self.0.lock().unwrap().push(error);
            }
        }
    }

    impl RiddenOut {
        fn any(&self, code: &str) -> bool {
            self.0
                .lock()
                .unwrap()
                .iter()
                .any(|error| error.contains(code))
        }
    }

    /// The drain rides out a failed broker connection while the consumer's
    /// gate is armed, and takes the last permit once the handler holding it
    /// ends: a stop never reconnects, so ending the drain on the failure
    /// would only cost the stop its final commit. The consumer points at a
    /// port nothing listens on, so librdkafka's first connection is refused
    /// at once, and the handler ends only once the drain has ridden that
    /// failure out.
    #[tokio::test]
    async fn the_drain_rides_out_a_failed_connection_through_an_armed_gate() {
        let ridden_out = RiddenOut::default();
        let _subscriber = set_default(tracing_subscriber::registry().with(ridden_out.clone()));
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", "127.0.0.1:1");
        let (rebalance_tx, _rebalance_rx) = std_mpsc::channel();
        let consumer = Arc::new(
            create_stream_consumer(
                config,
                "stop-drain",
                KafkaAutoOffsetReset::Earliest,
                MAX_POLL_INTERVAL_MS,
                &FetchQueueTuning::default(),
                "stop-drain",
                rebalance_tx,
                AssignmentTokens::default(),
                #[cfg(feature = "kafka-msk-iam")]
                None,
            )
            .expect("creating a consumer needs no broker"),
        );
        consumer
            .subscribe(&["stop-drain"])
            .expect("subscribing needs no broker");
        let now = std::time::Instant::now();
        consumer
            .transport_gate()
            .changed(AssignmentChange::Assign, now);
        consumer
            .transport_gate()
            .changed(AssignmentChange::Revoke, now);

        let semaphore = Arc::new(Semaphore::new(1));
        let handler = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("a fresh semaphore has its permit");
        let handler_ends = tokio::spawn({
            let ridden_out = ridden_out.clone();
            async move {
                while !ridden_out.any("BrokerTransportFailure") {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                drop(handler);
            }
        });
        let mut paused = false;
        let drained = tokio::time::timeout(
            Duration::from_secs(30),
            acquire_permit_while_polling(
                &semaphore,
                1,
                &consumer,
                None,
                None,
                "stop-drain",
                &mut paused,
                None,
            ),
        )
        .await
        .expect("the drain ends once the handler does");

        assert!(
            matches!(&drained, Ok(Some(_))),
            "the drain rides the failure out and takes the permit: {drained:?}"
        );
        handler_ends.await.expect("the handler ends");
    }
}

/// [`guarded`]: the call guard under every handler invocation, the DLQ
/// drain's `handle_dead` included.
#[cfg(test)]
mod guarded_tests {
    use super::*;

    async fn panics() -> u32 {
        panic!("a handler that panics");
    }

    #[tokio::test]
    async fn a_call_that_returns_is_done() {
        assert!(matches!(guarded(async { 7 }, None).await, Guarded::Done(7)));
        assert!(matches!(
            guarded(async { 7 }, Some(Duration::from_secs(5))).await,
            Guarded::Done(7)
        ));
    }

    #[tokio::test]
    async fn a_panic_is_caught_on_the_task_that_calls() {
        assert!(matches!(guarded(panics(), None).await, Guarded::Panicked));
        assert!(matches!(
            guarded(panics(), Some(Duration::from_secs(5))).await,
            Guarded::Panicked
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_that_outlasts_the_limit_is_dropped() {
        let limit = Duration::from_secs(30);
        let call = guarded(std::future::pending::<()>(), Some(limit)).await;
        assert!(matches!(call, Guarded::TimedOut(reported) if reported == limit));
    }

    #[tokio::test(start_paused = true)]
    async fn no_limit_waits_as_long_as_the_call_runs() {
        let call = guarded(
            async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                7
            },
            None,
        )
        .await;
        assert!(matches!(call, Guarded::Done(7)));
    }
}

/// [`cancelled_by_either`]: the token the DLQ drain stops on.
#[cfg(test)]
mod either_token_tests {
    use super::*;

    async fn fires(token: &CancellationToken) -> bool {
        tokio::time::timeout(Duration::from_secs(1), token.cancelled())
            .await
            .is_ok()
    }

    /// The first token reaches the merged one in the same step, so a stop
    /// requested through it takes effect before anything else runs.
    #[tokio::test(start_paused = true)]
    async fn the_first_token_fires_it_at_once() {
        let (first, second) = (CancellationToken::new(), CancellationToken::new());
        let (merged, _guard) = cancelled_by_either(&first, &second);
        assert!(!fires(&merged).await, "nothing has fired yet");
        first.cancel();
        assert!(merged.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn the_second_token_fires_it_through_the_forwarder() {
        let (first, second) = (CancellationToken::new(), CancellationToken::new());
        let (merged, _guard) = cancelled_by_either(&first, &second);
        second.cancel();
        assert!(fires(&merged).await);
        assert!(!first.is_cancelled(), "the first token is left alone");
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_token_that_has_fired_already_fires_it_at_once() {
        let (first, second) = (CancellationToken::new(), CancellationToken::new());
        second.cancel();
        let (merged, _guard) = cancelled_by_either(&first, &second);
        assert!(merged.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_guard_ends_only_the_forwarding() {
        let (first, second) = (CancellationToken::new(), CancellationToken::new());
        let (merged, guard) = cancelled_by_either(&first, &second);
        drop(guard);
        tokio::task::yield_now().await;
        second.cancel();
        assert!(!fires(&merged).await, "no task is left to forward it");
        first.cancel();
        assert!(merged.is_cancelled(), "the first token needs no task");
    }
}

#[cfg(test)]
mod retry_strategy_guard_tests {
    use super::*;
    use crate::topology::{SequenceFailure, TopologyBuilder};

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Entry {
        account: String,
    }

    struct Owned;
    impl Topic for Owned {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("retry-strategy-guard-owned").build())
        }
    }

    struct External;
    impl Topic for External {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("retry-strategy-guard-external")
                    .external()
                    .dlq()
                    .build()
            })
        }
    }

    struct Ledger;
    impl Topic for Ledger {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("retry-strategy-guard-ledger")
                    .sequenced(SequenceFailure::FailAll)
                    .hold_queue(Duration::from_secs(5))
                    .dlq()
                    .build()
            })
        }
    }
    impl SequencedTopic for Ledger {
        fn sequence_key(msg: &Entry) -> String {
            msg.account.clone()
        }
    }

    /// A sequenced topology with no DLQ, so the startup check has no topic to
    /// probe on the unreachable broker.
    struct LooseLedger;
    impl Topic for LooseLedger {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("retry-strategy-guard-loose-ledger")
                    .sequenced(SequenceFailure::Skip)
                    .hold_queue(Duration::from_secs(5))
                    .allow_message_loss()
                    .build()
            })
        }
    }
    impl SequencedTopic for LooseLedger {
        fn sequence_key(msg: &Entry) -> String {
            msg.account.clone()
        }
    }

    struct NoopHandler;
    impl<T: Topic<Message = Entry>> MessageHandler<T> for NoopHandler {
        type Context = ();
        async fn handle(&self, _: Entry, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    async fn consumer() -> KafkaConsumer {
        // Port 1 is never listening; every guard returns before any I/O.
        let client = KafkaClient::connect(&super::super::client::KafkaConfig::new("127.0.0.1:1"))
            .await
            .expect("client construction is lazy");
        KafkaConsumer::new(client)
    }

    fn topology_message(err: ShoveError) -> String {
        match err {
            ShoveError::Topology(msg) => msg,
            other => panic!("expected ShoveError::Topology, got {other:?}"),
        }
    }

    /// Ownership decides when nothing is asked, an explicit choice stands on
    /// a shove-owned topology, and an external topology refuses the one
    /// choice that would write into the topic.
    #[test]
    fn resolve_retry_strategy_lets_ownership_decide() {
        let owned = Owned::topology();
        let external = External::topology();
        let resolve = |t, requested| resolve_retry_strategy(t, requested, t.queue(), "test");

        assert_eq!(resolve(owned, None).unwrap(), RetryStrategy::Republish);
        assert_eq!(
            resolve(owned, Some(RetryStrategy::InPlace)).unwrap(),
            RetryStrategy::InPlace,
            "a shove-owned topology opts in"
        );
        assert_eq!(
            resolve(owned, Some(RetryStrategy::Republish)).unwrap(),
            RetryStrategy::Republish
        );
        assert_eq!(
            resolve(external, None).unwrap(),
            RetryStrategy::InPlace,
            "implied"
        );
        assert_eq!(
            resolve(external, Some(RetryStrategy::InPlace)).unwrap(),
            RetryStrategy::InPlace
        );
        let msg = topology_message(
            resolve(external, Some(RetryStrategy::Republish))
                .expect_err("a republish into an infra-owned topic is refused"),
        );
        assert!(msg.contains("retry-strategy-guard-external"), "{msg}");
        assert!(msg.contains("RetryStrategy::Republish"), "{msg}");
        assert!(msg.contains("RetryStrategy::InPlace"), "{msg}");
    }

    /// The direct path refuses the republish on an external topology before
    /// any broker request, naming the entry point.
    #[tokio::test]
    async fn run_rejects_republish_on_an_external_topology() {
        let msg = topology_message(
            consumer()
                .await
                .run::<External, _>(
                    NoopHandler,
                    (),
                    crate::ConsumerOptions::<Kafka>::new()
                        .with_retry_strategy(RetryStrategy::Republish)
                        .with_shutdown(CancellationToken::new()),
                )
                .await
                .expect_err("Republish on an external topology must be refused"),
        );
        assert!(msg.contains("KafkaConsumer::run"), "{msg}");
        assert!(msg.contains("RetryStrategy::Republish"), "{msg}");
    }

    /// The FIFO consumer always retries in place, so it accepts either
    /// strategy: the entry point runs on with either one and ends on its
    /// cancelled token.
    #[tokio::test]
    async fn run_fifo_accepts_either_retry_strategy() {
        for strategy in [RetryStrategy::InPlace, RetryStrategy::Republish] {
            let shutdown = CancellationToken::new();
            shutdown.cancel();
            consumer()
                .await
                .run_fifo::<LooseLedger, _>(
                    NoopHandler,
                    (),
                    crate::ConsumerOptions::<Kafka>::new()
                        .with_retry_strategy(strategy)
                        .with_shutdown(shutdown),
                )
                .await
                .unwrap_or_else(|e| {
                    panic!("{strategy:?} must be accepted on a FIFO consumer: {e}")
                });
        }
    }

    /// The FIFO consumer commits on the fixed default interval, so a
    /// per-record commit policy would change nothing there and is refused
    /// as the interval is, from `spawn_fifo_shards` so every FIFO entry
    /// point fails alike.
    #[tokio::test]
    async fn run_fifo_rejects_a_per_record_commit_policy() {
        let msg = topology_message(
            consumer()
                .await
                .run_fifo::<Ledger, _>(
                    NoopHandler,
                    (),
                    crate::ConsumerOptions::<Kafka>::new()
                        .with_commit_policy(CommitPolicy::PerRecord)
                        .with_shutdown(CancellationToken::new()),
                )
                .await
                .expect_err("a per-record commit policy on a FIFO consumer must be refused"),
        );
        assert!(msg.contains("retry-strategy-guard-ledger"), "{msg}");
        assert!(msg.contains("is sequenced"), "{msg}");
        assert!(msg.contains("with_commit_policy"), "{msg}");
        assert!(
            msg.contains("commits on the fixed default interval"),
            "{msg}"
        );
    }

    /// The DLQ drain never retries, so a strategy set on its options is a
    /// setting it would never read, and is refused.
    #[tokio::test]
    async fn run_dlq_rejects_a_retry_strategy() {
        let msg = topology_message(
            consumer()
                .await
                .run_dlq_with_options::<External, _>(
                    NoopHandler,
                    (),
                    crate::ConsumerOptions::<Kafka>::new()
                        .with_retry_strategy(RetryStrategy::InPlace)
                        .with_shutdown(CancellationToken::new()),
                )
                .await
                .expect_err("a strategy the DLQ drain never reads must be refused"),
        );
        assert!(msg.contains("KafkaConsumer::run_dlq"), "{msg}");
        assert!(msg.contains("with_retry_strategy"), "{msg}");
    }
}

#[cfg(test)]
mod broadcast_option_guard_tests {
    use super::*;
    use crate::broadcast::BroadcastStart;
    use crate::topology::{SequenceFailure, TopologyBuilder};

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Entry {
        account: String,
    }

    struct Plain;
    impl Topic for Plain {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("broadcast-option-guard-plain").build())
        }
    }
    impl NotSequenced for Plain {}

    struct WithDlq;
    impl Topic for WithDlq {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("broadcast-option-guard-dlq")
                    .dlq()
                    .build()
            })
        }
    }

    struct Ledger;
    impl Topic for Ledger {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("broadcast-option-guard-ledger")
                    .sequenced(SequenceFailure::FailAll)
                    .hold_queue(Duration::from_secs(5))
                    .dlq()
                    .build()
            })
        }
    }
    impl SequencedTopic for Ledger {
        fn sequence_key(msg: &Entry) -> String {
            msg.account.clone()
        }
    }

    struct NoopHandler;
    impl<T: Topic<Message = Entry>> MessageHandler<T> for NoopHandler {
        type Context = ();
        async fn handle(&self, _: Entry, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    async fn consumer() -> KafkaConsumer {
        // Port 1 is never listening; every guard returns before any I/O, and
        // the reconnect budget of one bounds the unfixed path to one refused
        // connection instead of an endless reconnect loop.
        let client = KafkaClient::connect(&super::super::client::KafkaConfig::new("127.0.0.1:1"))
            .await
            .expect("client construction is lazy");
        KafkaConsumer::new(client)
    }

    fn topology_message(err: ShoveError) -> String {
        match err {
            ShoveError::Topology(msg) => msg,
            other => panic!("expected ShoveError::Topology, got {other:?}"),
        }
    }

    /// `with_broadcast_start` is read by `BroadcastSubscriber::subscribe`
    /// alone. The direct path refuses it, at the same fail-fast point where
    /// the FIFO path refuses a commit interval, rather than starting a group
    /// consumer at its committed offset with the setting silently dropped.
    #[tokio::test]
    async fn run_rejects_broadcast_start() {
        let err = consumer()
            .await
            .run::<Plain, _>(
                NoopHandler,
                (),
                crate::ConsumerOptions::<Kafka>::new()
                    .with_broadcast_start(BroadcastStart::Head)
                    .with_max_reconnect_attempts(1)
                    .with_shutdown(CancellationToken::new()),
            )
            .await
            .expect_err("a set broadcast start must be refused on the direct path");
        let msg = topology_message(err);
        assert!(msg.contains("broadcast-option-guard-plain"), "{msg}");
        assert!(msg.contains("with_broadcast_start(Head)"), "{msg}");
        assert!(msg.contains("KafkaConsumer::run"), "{msg}");
    }

    /// The FIFO path refuses it too, from `spawn_fifo_shards`, so the direct,
    /// supervisor and registry FIFO entry points all fail alike.
    #[tokio::test]
    async fn run_fifo_rejects_broadcast_start() {
        let err = consumer()
            .await
            .run_fifo::<Ledger, _>(
                NoopHandler,
                (),
                crate::ConsumerOptions::<Kafka>::new()
                    .with_broadcast_start(BroadcastStart::Tail)
                    .with_max_reconnect_attempts(1)
                    .with_shutdown(CancellationToken::new()),
            )
            .await
            .expect_err("a set broadcast start must be refused on the FIFO path");
        let msg = topology_message(err);
        assert!(msg.contains("broadcast-option-guard-ledger"), "{msg}");
        assert!(msg.contains("with_broadcast_start(Tail)"), "{msg}");
        assert!(msg.contains("KafkaConsumer::run_fifo"), "{msg}");
    }

    /// The DLQ drain refuses it too, after the DLQ check and before it
    /// subscribes, so a start set on a drain's options is not dropped.
    #[tokio::test]
    async fn run_dlq_rejects_broadcast_start() {
        let err = consumer()
            .await
            .run_dlq_with_options::<WithDlq, _>(
                NoopHandler,
                (),
                crate::ConsumerOptions::<Kafka>::new()
                    .with_broadcast_start(BroadcastStart::Timestamp(1_700_000_000_000))
                    .with_max_reconnect_attempts(1)
                    .with_shutdown(CancellationToken::new()),
            )
            .await
            .expect_err("a set broadcast start must be refused on the DLQ drain");
        let msg = topology_message(err);
        assert!(msg.contains("broadcast-option-guard-dlq"), "{msg}");
        assert!(
            msg.contains("with_broadcast_start(Timestamp(1700000000000))"),
            "{msg}"
        );
        assert!(msg.contains("KafkaConsumer::run_dlq"), "{msg}");
    }

    /// The DLQ drain commits on the fixed default interval, so a commit
    /// interval would be read by nothing: refused, the FIFO policy. The
    /// client's token is cancelled first so a regression fails fast instead
    /// of reconnecting forever, because the drain's loop passes no attempt
    /// cap to `run_with_reconnect`.
    #[tokio::test]
    async fn run_dlq_rejects_commit_interval() {
        let consumer = consumer().await;
        consumer.client.shutdown_token().cancel();
        let err = consumer
            .run_dlq_with_options::<WithDlq, _>(
                NoopHandler,
                (),
                crate::ConsumerOptions::<Kafka>::new()
                    .with_commit_interval(Duration::from_secs(5))
                    .with_shutdown(CancellationToken::new()),
            )
            .await
            .expect_err("a commit interval must be refused on the DLQ drain");
        let msg = topology_message(err);
        assert!(msg.contains("broadcast-option-guard-dlq"), "{msg}");
        assert!(msg.contains("with_commit_interval"), "{msg}");
        assert!(msg.contains("KafkaConsumer::run_dlq"), "{msg}");
    }

    /// A per-record policy is refused on the same ground as the interval:
    /// the drain commits on the fixed default interval, so neither policy
    /// would change anything there.
    #[tokio::test]
    async fn run_dlq_rejects_a_per_record_commit_policy() {
        let consumer = consumer().await;
        consumer.client.shutdown_token().cancel();
        let err = consumer
            .run_dlq_with_options::<WithDlq, _>(
                NoopHandler,
                (),
                crate::ConsumerOptions::<Kafka>::new()
                    .with_commit_policy(CommitPolicy::PerRecord)
                    .with_shutdown(CancellationToken::new()),
            )
            .await
            .expect_err("a per-record commit policy must be refused on the DLQ drain");
        let msg = topology_message(err);
        assert!(msg.contains("broadcast-option-guard-dlq"), "{msg}");
        assert!(msg.contains("with_commit_policy"), "{msg}");
        assert!(msg.contains("KafkaConsumer::run_dlq"), "{msg}");
    }

    /// The DLQ drain hard-codes `earliest`, which applies only while its
    /// group has no usable committed offset, so a fresh drain never skips a
    /// dead letter: `with_auto_offset_reset` is refused rather than silently
    /// overridden.
    #[tokio::test]
    async fn run_dlq_rejects_auto_offset_reset() {
        let consumer = consumer().await;
        consumer.client.shutdown_token().cancel();
        let err = consumer
            .run_dlq_with_options::<WithDlq, _>(
                NoopHandler,
                (),
                crate::ConsumerOptions::<Kafka>::new()
                    .with_auto_offset_reset(KafkaAutoOffsetReset::Latest)
                    .with_shutdown(CancellationToken::new()),
            )
            .await
            .expect_err("auto.offset.reset must be refused on the DLQ drain");
        let msg = topology_message(err);
        assert!(msg.contains("broadcast-option-guard-dlq"), "{msg}");
        assert!(msg.contains("with_auto_offset_reset"), "{msg}");
        assert!(msg.contains("KafkaConsumer::run_dlq"), "{msg}");
    }

    /// `handle_dead` returns no outcome, so a call that times out has none to
    /// resolve to and its dead letter is committed: a timeout outcome would be
    /// read by nothing on the drain, and is refused.
    #[tokio::test]
    async fn run_dlq_rejects_a_handler_timeout_outcome() {
        let consumer = consumer().await;
        consumer.client.shutdown_token().cancel();
        let err = consumer
            .run_dlq_with_options::<WithDlq, _>(
                NoopHandler,
                (),
                crate::ConsumerOptions::<Kafka>::new()
                    .with_handler_timeout_outcome(Outcome::Reject)
                    .with_shutdown(CancellationToken::new()),
            )
            .await
            .expect_err("a handler timeout outcome must be refused on the DLQ drain");
        let msg = topology_message(err);
        assert!(msg.contains("broadcast-option-guard-dlq"), "{msg}");
        assert!(msg.contains("with_handler_timeout_outcome"), "{msg}");
        assert!(msg.contains("KafkaConsumer::run_dlq"), "{msg}");
    }

    /// Negative control: the options the drain does read pass its guards,
    /// so the refusals above are conditional on the knob and not on the
    /// entry point. With the client's token already cancelled the drain
    /// returns from its first `select!`, before the missing broker matters.
    #[tokio::test]
    async fn run_dlq_admits_the_options_it_reads() {
        let consumer = consumer().await;
        consumer.client.shutdown_token().cancel();
        consumer
            .run_dlq_with_options::<WithDlq, _>(
                NoopHandler,
                (),
                crate::ConsumerOptions::<Kafka>::new()
                    .with_group_id("settlement-audit")
                    .with_max_message_size(64 * 1024)
                    .with_shutdown(CancellationToken::new()),
            )
            .await
            .expect("the drain reads the group id and the size cap, so both pass its guards");
    }

    /// The token of the options stops the drain as the client's does: with
    /// only the options' token fired, the drain returns from its first
    /// `select!`, before the missing broker matters. The bound makes a
    /// regression fail instead of reconnecting forever.
    #[tokio::test]
    async fn run_dlq_stops_on_the_token_of_its_options() {
        let consumer = consumer().await;
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        tokio::time::timeout(
            Duration::from_secs(30),
            consumer.run_dlq_with_options::<WithDlq, _>(
                NoopHandler,
                (),
                crate::ConsumerOptions::<Kafka>::new().with_shutdown(shutdown),
            ),
        )
        .await
        .expect("a fired options token stops the drain")
        .expect("the stop is clean");
        assert!(
            !consumer.client.shutdown_token().is_cancelled(),
            "the client's token was never fired"
        );
    }

    /// A broadcast subscription commits nothing, so a commit interval would
    /// be read by nothing: refused at `subscribe()`, the FIFO policy.
    #[test]
    fn broadcast_subscribe_rejects_commit_interval() {
        let inner = crate::ConsumerOptions::<Kafka>::new()
            .with_commit_interval(Duration::from_secs(5))
            .into_inner();
        let msg = topology_message(
            KafkaConsumer::check_broadcast_options("cache-invalidations", &inner)
                .expect_err("a commit interval must be refused on a broadcast subscription"),
        );
        assert!(msg.contains("cache-invalidations"), "{msg}");
        assert!(msg.contains("with_commit_interval"), "{msg}");
        assert!(msg.contains("broadcast"), "{msg}");
    }

    /// A per-record policy is refused on the same ground: the subscription
    /// commits nothing, so no commit policy would be read.
    #[test]
    fn broadcast_subscribe_rejects_a_per_record_commit_policy() {
        let inner = crate::ConsumerOptions::<Kafka>::new()
            .with_commit_policy(CommitPolicy::PerRecord)
            .into_inner();
        let msg = topology_message(
            KafkaConsumer::check_broadcast_options("cache-invalidations", &inner).expect_err(
                "a per-record commit policy must be refused on a broadcast subscription",
            ),
        );
        assert!(msg.contains("cache-invalidations"), "{msg}");
        assert!(msg.contains("with_commit_policy"), "{msg}");
        assert!(msg.contains("broadcast"), "{msg}");
    }

    /// Every partition is assigned at an explicit offset, so librdkafka never
    /// consults `auto.offset.reset`: the setting is refused, and the error
    /// points at the knob that does decide where the subscription starts.
    #[test]
    fn broadcast_subscribe_rejects_auto_offset_reset() {
        let inner = crate::ConsumerOptions::<Kafka>::new()
            .with_auto_offset_reset(KafkaAutoOffsetReset::Latest)
            .into_inner();
        let msg = topology_message(
            KafkaConsumer::check_broadcast_options("cache-invalidations", &inner)
                .expect_err("auto.offset.reset must be refused on a broadcast subscription"),
        );
        assert!(msg.contains("cache-invalidations"), "{msg}");
        assert!(msg.contains("with_auto_offset_reset"), "{msg}");
        assert!(msg.contains("with_broadcast_start"), "{msg}");
    }

    /// The broadcast loop settles every outcome without a retry strategy, so
    /// `with_retry_strategy` would be read by nothing: refused at
    /// `subscribe()`, next to the other knobs the loop never reads.
    #[test]
    fn broadcast_subscribe_rejects_a_retry_strategy() {
        let inner = crate::ConsumerOptions::<Kafka>::new()
            .with_retry_strategy(RetryStrategy::InPlace)
            .into_inner();
        let msg = topology_message(
            KafkaConsumer::check_broadcast_options("cache-invalidations", &inner)
                .expect_err("a retry strategy must be refused on a broadcast subscription"),
        );
        assert!(msg.contains("cache-invalidations"), "{msg}");
        assert!(msg.contains("with_retry_strategy"), "{msg}");
        assert!(msg.contains("BroadcastSubscriber::subscribe"), "{msg}");
    }

    /// Negative control: the options a broadcast subscription does read pass,
    /// every start included, so the refusals of the commit interval and of
    /// the reset policy are conditional on the knob and not on the check
    /// itself.
    #[test]
    fn broadcast_subscribe_admits_every_start_and_the_group_id() {
        for start in [
            BroadcastStart::Tail,
            BroadcastStart::Head,
            BroadcastStart::Timestamp(1_700_000_000_000),
        ] {
            let inner = crate::ConsumerOptions::<Kafka>::new()
                .with_broadcast_start(start)
                .with_group_id("cache-invalidations-broadcast")
                .into_inner();
            KafkaConsumer::check_broadcast_options("cache-invalidations", &inner)
                .expect("the start and the inert group id are read by the broadcast loop");
        }
    }
}

#[cfg(test)]
mod final_commit_thread_tests {
    use super::*;
    use std::thread::ThreadId;
    use tracing::field::{Field, Visit};
    use tracing::subscriber::set_default;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    type Spawner = Box<dyn FnMut(String, Box<dyn FnOnce() + Send>) -> std::io::Result<()>>;

    /// What the probe saw, in the order it happened: each commit with the
    /// offsets it was given, and the drop, each with the thread that ran it.
    /// For the real handle the drop is where `rd_kafka_consumer_close` runs.
    #[derive(Debug, PartialEq)]
    enum Event {
        Commit {
            thread: ThreadId,
            offsets: Vec<(String, i32, Offset)>,
        },
        Drop {
            thread: ThreadId,
            name: Option<String>,
        },
    }

    /// Stands in for the consumer: a commit answers as `answer` says and is
    /// recorded with its offsets, and the drop reports the thread it ran on.
    /// With `close_release` set the drop first blocks until it is signalled
    /// or dropped, like a close waiting on a slow LeaveGroup.
    struct DropProbe {
        events: std_mpsc::Sender<Event>,
        /// `None` for a commit that lands; the broker's error otherwise.
        answer: Option<KafkaError>,
        close_release: Option<std_mpsc::Receiver<()>>,
        /// What `assignment_lost` answers once the commit has run.
        lost: bool,
    }

    impl FinalCommit for DropProbe {
        fn assignment_lost(&self) -> bool {
            self.lost
        }

        fn commit_sync(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
            let offsets = tpl
                .elements()
                .iter()
                .map(|e| (e.topic().to_owned(), e.partition(), e.offset()))
                .collect();
            let _ = self.events.send(Event::Commit {
                thread: std::thread::current().id(),
                offsets,
            });
            match &self.answer {
                Some(error) => Err(error.clone()),
                None => Ok(()),
            }
        }
    }

    /// Stands in for a consumer whose commit thread dies: `commit_sync`
    /// panics, so the thread ends without reporting a result.
    struct PanickingProbe;

    impl FinalCommit for PanickingProbe {
        fn commit_sync(&self, _tpl: &TopicPartitionList) -> KafkaResult<()> {
            panic!("the commit thread dies before it can answer");
        }
    }

    /// Stands in for a consumer whose commit does not answer, like a commit
    /// to a frozen coordinator: `commit_sync` blocks until `release` is
    /// signalled or dropped.
    struct BlockingProbe {
        release: std_mpsc::Receiver<()>,
    }

    impl FinalCommit for BlockingProbe {
        fn commit_sync(&self, _tpl: &TopicPartitionList) -> KafkaResult<()> {
            let _ = self.release.recv();
            Ok(())
        }
    }

    /// The message of one WARN event.
    struct Message(String);

    impl Visit for Message {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = format!("{value:?}");
            }
        }
    }

    /// Collects the messages of every WARN event the current thread emits.
    #[derive(Clone, Default)]
    struct Warnings(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> Layer<S> for Warnings {
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            if *event.metadata().level() == tracing::Level::WARN {
                let mut message = Message(String::new());
                event.record(&mut message);
                self.0.lock().unwrap().push(message.0);
            }
        }
    }

    impl Warnings {
        fn messages(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    /// Stands in for a handle another thread still holds: the commit lands
    /// and the drop closes nothing.
    struct SharedProbe;

    impl FinalCommit for SharedProbe {
        fn commit_sync(&self, _tpl: &TopicPartitionList) -> KafkaResult<()> {
            Ok(())
        }

        fn is_last_handle(&self) -> bool {
            false
        }
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            if let Some(release) = &self.close_release {
                let _ = release.recv();
            }
            let thread = std::thread::current();
            let _ = self.events.send(Event::Drop {
                thread: thread.id(),
                name: thread.name().map(str::to_owned),
            });
        }
    }

    fn probe() -> (DropProbe, std_mpsc::Receiver<Event>) {
        probe_answering(None)
    }

    fn probe_answering(answer: Option<KafkaError>) -> (DropProbe, std_mpsc::Receiver<Event>) {
        let (events, seen) = std_mpsc::channel();
        (
            DropProbe {
                events,
                answer,
                close_release: None,
                lost: false,
            },
            seen,
        )
    }

    /// A probe whose commit is refused with `error` and leaves the
    /// assignment lost, as a stale generation outside a rebalance does.
    fn probe_losing_its_assignment(error: KafkaError) -> (DropProbe, std_mpsc::Receiver<Event>) {
        let (events, seen) = std_mpsc::channel();
        (
            DropProbe {
                events,
                answer: Some(error),
                close_release: None,
                lost: true,
            },
            seen,
        )
    }

    /// A probe whose commit lands and whose close blocks until `release` is
    /// signalled.
    fn probe_with_slow_close(
        release: std_mpsc::Receiver<()>,
    ) -> (DropProbe, std_mpsc::Receiver<Event>) {
        let (events, seen) = std_mpsc::channel();
        (
            DropProbe {
                events,
                answer: None,
                close_release: Some(release),
                lost: false,
            },
            seen,
        )
    }

    /// `std::thread::Builder`, as production uses it.
    fn real_spawner() -> Spawner {
        Box::new(|name, body| {
            std::thread::Builder::new()
                .name(name)
                .spawn(body)
                .map(|_detached| ())
        })
    }

    /// Fails its first `failures` spawns the way an exhausted host does,
    /// then behaves. `Builder::spawn` drops the body it was given when it
    /// fails, and so does this, which is the ownership the fix is about.
    fn failing_spawner(failures: usize) -> Spawner {
        let mut calls = 0usize;
        Box::new(move |name, body| {
            calls += 1;
            if calls <= failures {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "Resource temporarily unavailable",
                ));
            }
            std::thread::Builder::new()
                .name(name)
                .spawn(body)
                .map(|_detached| ())
        })
    }

    /// [`final_commit_on_thread`] for a commit that is never handed back,
    /// which always ends with the commit's result and closes on the thread.
    async fn final_commit<C, S>(
        consumer: C,
        tpl: Option<TopicPartitionList>,
        queue: &str,
        deadline: Duration,
        spawn: &mut S,
    ) -> std::result::Result<(), CommitFailure>
    where
        C: FinalCommit,
        S: FnMut(String, Box<dyn FnOnce() + Send>) -> std::io::Result<()>,
    {
        final_commit_on_thread(consumer, tpl, queue, deadline, keep_to_close, spawn)
            .await
            .ended()
    }

    fn one_offset() -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("orders", 0, Offset::Offset(7))
            .expect("a literal partition offset");
        tpl
    }

    /// Every event up to and including the drop, in order. The drop is the
    /// probe's last act, so once it has arrived nothing else is pending.
    fn events_until_the_drop(seen: &std_mpsc::Receiver<Event>) -> Vec<Event> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut events = Vec::new();
        loop {
            let event = seen
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                .expect("the consumer is dropped, on some thread, within the deadline");
            let is_drop = matches!(event, Event::Drop { .. });
            events.push(event);
            if is_drop {
                return events;
            }
        }
    }

    /// The shape step 4 promised: exactly one commit, of the offsets the
    /// loop drained, and then the close, both on the dedicated thread and
    /// never on the runtime thread that awaits them.
    #[tokio::test]
    async fn the_final_commit_thread_commits_and_then_owns_the_close() {
        let (probe, seen) = probe();
        let result = final_commit(
            probe,
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            &mut real_spawner(),
        )
        .await;
        assert!(result.is_ok(), "{result:?}");
        // The close is waited for inside the deadline, so both events are
        // already queued when the result arrives.
        let events: Vec<Event> = seen.try_iter().collect();
        let [
            Event::Commit {
                thread: committed_on,
                offsets,
            },
            Event::Drop {
                thread: dropped_on,
                name,
            },
        ] = events.as_slice()
        else {
            panic!(
                "one commit, then the drop, both done before the result arrives, and nothing \
                 else: {events:?}"
            );
        };
        assert_eq!(*offsets, vec![("orders".to_owned(), 0, Offset::Offset(7))]);
        assert_eq!(
            committed_on, dropped_on,
            "the commit and the close run on the same thread"
        );
        assert_ne!(
            *dropped_on,
            std::thread::current().id(),
            "the close must not run on the runtime thread"
        );
        assert_eq!(name.as_deref(), Some("shove-kafka-final-commit orders"));
    }

    /// N1: when the thread cannot be spawned, nothing commits, and the
    /// consumer still closes off the runtime, on a second, close-only
    /// thread. Before the fix the closure owned the consumer, `spawn`
    /// dropped it on failure, and the close ran right here.
    #[tokio::test]
    async fn a_failed_spawn_commits_nothing_and_closes_the_consumer_on_a_second_thread() {
        let (probe, seen) = probe();
        let result = final_commit(
            probe,
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            &mut failing_spawner(1),
        )
        .await;
        assert_eq!(result, Err(CommitFailure::NoThread));
        let events = events_until_the_drop(&seen);
        let [Event::Drop { thread, name }] = events.as_slice() else {
            panic!("the drop and no commit: {events:?}");
        };
        assert_ne!(
            *thread,
            std::thread::current().id(),
            "a failed spawn must not drop the consumer on the runtime thread"
        );
        assert_eq!(name.as_deref(), Some("shove-kafka-consumer-close orders"));
    }

    /// N1, the last resort: with no thread to be had at all, nothing commits
    /// and the handle is leaked rather than closed on the runtime thread.
    /// `forget` is synchronous, so once the call has returned a drop that
    /// has not happened never will.
    #[tokio::test]
    async fn with_no_thread_at_all_the_consumer_is_leaked_and_never_closed_here() {
        let (probe, seen) = probe();
        let result = final_commit(
            probe,
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            &mut failing_spawner(usize::MAX),
        )
        .await;
        assert_eq!(result, Err(CommitFailure::NoThread));
        assert_eq!(
            seen.try_recv().ok(),
            None,
            "no commit and no close: the handle must be leaked, never closed on the runtime thread"
        );
    }

    /// A commit the coordinator rejects is reported as `Rejected` with
    /// librdkafka's text for the broker's answer, and the consumer still
    /// closes on the thread after it. This is the failure the shutdown arm
    /// turns into `ShoveError::Commit` instead of a clean exit.
    #[tokio::test]
    async fn a_rejected_commit_is_reported_with_the_brokers_text_and_the_close_still_runs() {
        let (probe, seen) = probe_answering(Some(KafkaError::ConsumerCommit(
            RDKafkaErrorCode::GroupAuthorizationFailed,
        )));
        let result = final_commit(
            probe,
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            &mut real_spawner(),
        )
        .await;
        let Err(CommitFailure::Rejected(text)) = result else {
            panic!("a rejected commit is reported as Rejected: {result:?}");
        };
        assert!(
            text.contains("Group authorization failed"),
            "the broker's text is carried: {text}"
        );
        let events = events_until_the_drop(&seen);
        assert!(
            matches!(
                events.as_slice(),
                [Event::Commit { .. }, Event::Drop { .. }]
            ),
            "the commit, then the close: {events:?}"
        );
    }

    /// A commit that lands, followed by a close that outlives the deadline,
    /// is a landed commit: the result comes back `Ok` at the deadline while
    /// the close is still running, and the thread finishes the close on its
    /// own. Before the commit and the close were reported apart, the close
    /// held the result back and a slow LeaveGroup read as a commit that
    /// missed its deadline.
    #[tokio::test]
    async fn a_landed_commit_followed_by_a_slow_close_is_not_a_failed_commit() {
        const DEADLINE: Duration = Duration::from_millis(500);
        let (release_tx, release) = std_mpsc::channel::<()>();
        let (probe, seen) = probe_with_slow_close(release);
        let started = Instant::now();
        let result = final_commit(
            probe,
            Some(one_offset()),
            "orders",
            DEADLINE,
            &mut real_spawner(),
        )
        .await;
        assert_eq!(result, Ok(()), "the commit landed, whatever the close does");
        assert!(
            started.elapsed() >= DEADLINE,
            "the close is waited for inside the deadline, returned after {:?}",
            started.elapsed()
        );
        let events: Vec<Event> = seen.try_iter().collect();
        assert!(
            matches!(events.as_slice(), [Event::Commit { .. }]),
            "the commit has run and the close has not finished: {events:?}"
        );
        // Let the close finish; it runs on the thread, late, and reports to
        // nobody.
        let _ = release_tx.send(());
        let events = events_until_the_drop(&seen);
        assert!(
            matches!(events.as_slice(), [Event::Drop { .. }]),
            "the close finishes on its own thread: {events:?}"
        );
    }

    /// With another holder of the handle left, a commit thread a stop did
    /// not wait out, the final thread's drop closes nothing: the
    /// commit's result is still the result, and the loop logs that the close
    /// follows that holder instead of claiming it ran.
    #[tokio::test]
    async fn a_drop_with_another_holder_left_reports_the_close_as_following_that_holder() {
        let warnings = Warnings::default();
        let _guard = set_default(tracing_subscriber::registry().with(warnings.clone()));
        let result = final_commit(
            SharedProbe,
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            &mut real_spawner(),
        )
        .await;
        assert_eq!(result, Ok(()), "the commit landed, whoever closes");
        let logged = warnings.messages();
        assert_eq!(
            logged.len(),
            1,
            "one warning, that the close follows the other holder: {logged:?}"
        );
        assert!(
            logged[0].contains("another holder remains"),
            "the warning names the other holder: {}",
            logged[0]
        );
    }

    /// A commit with no answer within `SHUTDOWN_COMMIT_DEADLINE` is reported
    /// as `Deadline` carrying that deadline, while the thread keeps waiting
    /// for the answer. Paused time: the deadline elapses as soon as nothing
    /// else can make progress, which is at once, so the test does not wait
    /// the real 20 s.
    #[tokio::test(start_paused = true)]
    async fn a_commit_past_the_deadline_is_reported_as_the_deadline() {
        let warnings = Warnings::default();
        let _guard = set_default(tracing_subscriber::registry().with(warnings.clone()));
        let (release_tx, release) = std_mpsc::channel::<()>();
        let result = final_commit(
            BlockingProbe { release },
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            &mut real_spawner(),
        )
        .await;
        assert_eq!(
            result,
            Err(CommitFailure::Deadline(SHUTDOWN_COMMIT_DEADLINE))
        );
        // The thread is still inside the commit, so no close deadline is
        // claimed: the loop says the close follows the commit instead.
        let logged = warnings.messages();
        assert!(
            logged.iter().any(|m| m.contains("follows the commit")),
            "the close is reported as following the commit: {logged:?}"
        );
        assert!(
            !logged
                .iter()
                .any(|m| m.contains("close did not finish within the shutdown deadline")),
            "no close deadline is claimed while the commit is still waiting: {logged:?}"
        );
        // Let the thread finish; its late answer has nobody listening.
        let _ = release_tx.send(());
    }

    /// A commit thread that dies without answering is not the broker's
    /// verdict: the result is unknown, and is reported as `Deadline` with
    /// the time waited, never as `Rejected`.
    #[tokio::test]
    async fn a_commit_thread_that_dies_without_answering_is_reported_as_unknown() {
        let result = final_commit(
            PanickingProbe,
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            &mut real_spawner(),
        )
        .await;
        assert!(
            matches!(result, Err(CommitFailure::Deadline(_))),
            "a lost result is unknown, not rejected: {result:?}"
        );
    }

    /// With a try again asked for, a commit the coordinator refuses while
    /// the group rebalances closes nothing: the thread hands the consumer
    /// back, unclosed, with the refusal, for the stop to poll and commit
    /// again. A rebalance in progress and a stale generation both do.
    #[tokio::test]
    async fn a_commit_refused_while_the_group_rebalances_hands_the_consumer_back_unclosed() {
        for code in [
            RDKafkaErrorCode::RebalanceInProgress,
            RDKafkaErrorCode::IllegalGeneration,
        ] {
            let (probe, seen) = probe_answering(Some(KafkaError::ConsumerCommit(code)));
            let committed = final_commit_on_thread(
                probe,
                Some(one_offset()),
                "orders",
                SHUTDOWN_COMMIT_DEADLINE,
                hand_back,
                &mut real_spawner(),
            )
            .await;
            let FinalCommitted::Rebalancing(Refused { consumer, error }) = committed else {
                panic!("a refusal with {code:?} hands the consumer back");
            };
            assert_eq!(error.rdkafka_error_code(), Some(code));
            let events: Vec<Event> = seen.try_iter().collect();
            assert!(
                matches!(events.as_slice(), [Event::Commit { .. }]),
                "the commit ran and nothing closed: {events:?}"
            );
            drop(consumer);
            let events: Vec<Event> = seen.try_iter().collect();
            assert!(
                matches!(events.as_slice(), [Event::Drop { .. }]),
                "the handle came back whole, and closes where it is dropped: {events:?}"
            );
        }
    }

    /// A refusal for a stale generation that left the assignment lost is not
    /// handed back, even with a try again asked for: nothing is left to
    /// commit under it, so the thread closes the consumer after the commit
    /// and the refusal is reported as `Rejected`.
    #[tokio::test]
    async fn a_refusal_that_left_the_assignment_lost_is_not_handed_back() {
        let (probe, seen) = probe_losing_its_assignment(KafkaError::ConsumerCommit(
            RDKafkaErrorCode::IllegalGeneration,
        ));
        let committed = final_commit_on_thread(
            probe,
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            hand_back,
            &mut real_spawner(),
        )
        .await;
        assert!(
            matches!(
                committed,
                FinalCommitted::Ended(Err(CommitFailure::Rejected(_)))
            ),
            "a refusal that lost the assignment is reported as Rejected"
        );
        let events = events_until_the_drop(&seen);
        assert!(
            matches!(
                events.as_slice(),
                [Event::Commit { .. }, Event::Drop { .. }]
            ),
            "the commit, then the close on the thread: {events:?}"
        );
    }

    /// The commit and then the close, both on the same thread: what every
    /// final commit that is not handed back leaves behind.
    fn assert_the_commit_then_the_close_on_its_thread(seen: &std_mpsc::Receiver<Event>) {
        let events = events_until_the_drop(seen);
        let [
            Event::Commit {
                thread: committed_on,
                ..
            },
            Event::Drop {
                thread: dropped_on, ..
            },
        ] = events.as_slice()
        else {
            panic!("the commit, then the close: {events:?}");
        };
        assert_eq!(
            committed_on, dropped_on,
            "the close runs on the commit's thread"
        );
    }

    /// A refusal while the group rebalances on a commit that is never
    /// handed back, and a refusal of any other kind on one that may be,
    /// close as every final commit does: the commit and then the close,
    /// both on the thread, with the refusal reported as `Rejected`.
    #[tokio::test]
    async fn a_refusal_with_no_try_again_or_of_another_kind_closes_on_the_thread() {
        let (probe, seen) = probe_answering(Some(KafkaError::ConsumerCommit(
            RDKafkaErrorCode::RebalanceInProgress,
        )));
        let result = final_commit(
            probe,
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            &mut real_spawner(),
        )
        .await;
        assert!(
            matches!(result, Err(CommitFailure::Rejected(_))),
            "a refusal with no try again is reported as Rejected: {result:?}"
        );
        assert_the_commit_then_the_close_on_its_thread(&seen);

        let (probe, seen) = probe_answering(Some(KafkaError::ConsumerCommit(
            RDKafkaErrorCode::GroupAuthorizationFailed,
        )));
        let committed = final_commit_on_thread(
            probe,
            Some(one_offset()),
            "orders",
            SHUTDOWN_COMMIT_DEADLINE,
            hand_back,
            &mut real_spawner(),
        )
        .await;
        assert!(
            matches!(
                committed,
                FinalCommitted::Ended(Err(CommitFailure::Rejected(_)))
            ),
            "a refusal of another kind is reported as Rejected"
        );
        assert_the_commit_then_the_close_on_its_thread(&seen);
    }

    /// The offsets `ShoveError::Commit` carries are the commit's positions in
    /// partition order, whatever order the partition list holds them in.
    #[test]
    fn committed_offsets_lists_the_positions_in_partition_order() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("orders", 3, Offset::Offset(12))
            .expect("a literal partition offset");
        tpl.add_partition_offset("orders", 0, Offset::Offset(8))
            .expect("a literal partition offset");
        assert_eq!(committed_offsets(&tpl), vec![(0, 8), (3, 12)]);
    }
}

#[cfg(test)]
mod commit_interval_funnel_tests {
    use super::super::constants::MAX_COMMIT_INTERVAL;
    use super::*;
    use crate::topology::TopologyBuilder;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Entry;

    struct Plain;
    impl Topic for Plain {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("commit-interval-funnel").build())
        }
    }
    impl NotSequenced for Plain {}

    struct NoopHandler;
    impl MessageHandler<Plain> for NoopHandler {
        type Context = ();
        async fn handle(&self, _: Entry, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    /// Runs the direct path with `interval` written to the public
    /// `kafka_commit_interval` field, past the setter and its check. The
    /// shutdown token is cancelled first: with the funnel's check in place
    /// the run never gets that far, and without it the receive loop returns
    /// from its first `select!` instead of reconnecting against the
    /// unreachable broker on port 1.
    async fn run_with_field_interval(interval: Duration) -> Result<()> {
        let mut opts = crate::ConsumerOptions::<Kafka>::new();
        opts.kafka_commit_interval = Some(interval);
        run_cancelled(opts).await
    }

    /// The `Topology` error the loop refuses a field-written interval with,
    /// naming the topic, the field and the entry point.
    fn refused_interval(result: Result<()>) -> String {
        let err = result.expect_err("an interval the setter refuses must be refused by the loop");
        let ShoveError::Topology(msg) = err else {
            panic!("expected ShoveError::Topology, got {err:?}");
        };
        assert!(
            msg.contains("commit-interval-funnel")
                && msg.contains("kafka_commit_interval")
                && msg.contains("KafkaConsumer::run"),
            "message must name the topic, the field and the entry point: {msg}"
        );
        msg
    }

    /// Zero would make the gate always due. The setter refuses it with a
    /// panic at configuration time, and the one place the receive loop
    /// reads the field refuses it with a `Topology` error, never a panic:
    /// an interval written straight into the public field skips the setter
    /// and reaches a run path.
    #[tokio::test]
    async fn run_rejects_a_zero_interval_written_to_the_field() {
        let msg = refused_interval(run_with_field_interval(Duration::ZERO).await);
        assert!(msg.contains("must be positive"), "{msg}");
    }

    /// `Duration::MAX` makes `deadline` overflow to `now` while `due` stays
    /// false, so the wake arm would fire on every pass with commit work
    /// pending: the hot spin the gate's own doc warns about. Refused before
    /// the gate is built, with the same error, as is anything past
    /// `MAX_COMMIT_INTERVAL`.
    #[tokio::test]
    async fn run_rejects_an_unrepresentable_interval_written_to_the_field() {
        let msg = refused_interval(run_with_field_interval(Duration::MAX).await);
        assert!(msg.contains("must be at most"), "{msg}");
        let over = MAX_COMMIT_INTERVAL.saturating_add(Duration::from_secs(1));
        refused_interval(run_with_field_interval(over).await);
    }

    /// Control: an interval the setter admits passes the funnel's check too,
    /// the bound itself included, so the funnel refuses exactly what the
    /// setter refuses.
    #[tokio::test]
    async fn run_admits_a_bounded_interval_written_to_the_field() {
        for interval in [Duration::from_secs(5), MAX_COMMIT_INTERVAL] {
            run_with_field_interval(interval)
                .await
                .expect("an interval the setter admits passes the check");
        }
    }

    /// Runs the direct path with `opts`, its shutdown token cancelled first
    /// so a run that is not refused returns from its first `select!`
    /// instead of reconnecting against the unreachable broker on port 1.
    async fn run_cancelled(opts: crate::ConsumerOptions<Kafka>) -> Result<()> {
        let client = KafkaClient::connect(&super::super::client::KafkaConfig::new("127.0.0.1:1"))
            .await
            .expect("client construction is lazy");
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        KafkaConsumer::new(client)
            .run::<Plain, _>(NoopHandler, (), opts.with_shutdown(shutdown))
            .await
    }

    /// `CommitPolicy::PerRecord` with two permits is refused where the loop
    /// reads the policy, before any consumer is created, with a `Topology`
    /// error that names the topic, the policy and the entry point: with
    /// more than one permit a completion above an unfinished lower offset
    /// confirms nothing, so a commit per record would not be exact.
    #[tokio::test]
    async fn run_rejects_per_record_commits_with_two_permits() {
        let err = run_cancelled(
            crate::ConsumerOptions::<Kafka>::new()
                .with_concurrent_processing(true)
                .with_prefetch_count(2)
                .with_commit_policy(CommitPolicy::PerRecord),
        )
        .await
        .expect_err("two permits under PerRecord must be refused");
        let ShoveError::Topology(msg) = err else {
            panic!("expected ShoveError::Topology, got {err:?}");
        };
        assert!(
            msg.contains("commit-interval-funnel")
                && msg.contains("CommitPolicy::PerRecord")
                && msg.contains("prefetch_count")
                && msg.contains("KafkaConsumer::run"),
            "message must name the topic, the policy and the entry point: {msg}"
        );
    }

    /// Control: one permit is admitted, whether as `prefetch_count(1)` with
    /// concurrent processing on or as the clamp concurrent processing off
    /// applies on the way through `into_inner`.
    #[tokio::test]
    async fn run_admits_per_record_commits_with_one_permit() {
        for opts in [
            crate::ConsumerOptions::<Kafka>::new()
                .with_concurrent_processing(true)
                .with_prefetch_count(1)
                .with_commit_policy(CommitPolicy::PerRecord),
            crate::ConsumerOptions::<Kafka>::new()
                .with_concurrent_processing(false)
                .with_prefetch_count(2)
                .with_commit_policy(CommitPolicy::PerRecord),
        ] {
            run_cancelled(opts)
                .await
                .expect("one permit under PerRecord passes the check");
        }
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
        // The shared constants are the same values — they are what external
        // tooling (the stress harness) references instead of restating 500/250.
        assert_eq!(opts.max_batch_size, DEFAULT_KAFKA_MAX_BATCH_SIZE);
        assert_eq!(opts.max_batch_age, DEFAULT_KAFKA_MAX_BATCH_AGE);
        assert_eq!(opts.max_reconnect_attempts, None);
        assert_eq!(opts.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
        assert_eq!(opts.kafka_group_id, None);
        assert_eq!(opts.kafka_auto_offset_reset, None);
    }

    /// The getters exist so code outside this module (the stress harness's
    /// tests) can read back what the builders set; a transposed pair here
    /// would let a benchmark stamp rows with knobs the run never used.
    #[test]
    fn getters_read_back_what_the_builders_set() {
        let opts = BatchConsumerOptions::new()
            .with_max_batch_size(7)
            .with_max_batch_age(Duration::from_millis(9));
        assert_eq!(opts.max_batch_size(), 7);
        assert_eq!(opts.max_batch_age(), Duration::from_millis(9));
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

    #[cfg(feature = "test-support")]
    #[test]
    #[should_panic(expected = "housekeeping interval must be positive")]
    fn with_housekeeping_interval_for_test_zero_panics() {
        let _ = BatchConsumerOptions::new().with_housekeeping_interval_for_test(Duration::ZERO);
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
        let result = run_with_reconnect(&shutdown, "test", Some(2), None, || {
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
        let result = run_with_reconnect(&shutdown, "test", Some(2), None, || {
            calls.fetch_add(1, AtomicOrdering::SeqCst);
            async move { Err(ShoveError::Connection("boom".to_string())) }
        })
        .await;

        assert!(result.is_err(), "expected exhaustion error, got {result:?}");
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
    }

    /// An attempt whose own stop failed, as the standard consumer's wait for
    /// its handlers can at a stop, ends the run with that error although it
    /// is retryable and the stop has fired.
    #[tokio::test(start_paused = true)]
    async fn a_failed_stop_ends_the_run_with_its_error() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let stop_failed = StopFailed::default();
        let result = run_with_reconnect(&shutdown, "test", None, Some(&stop_failed), || {
            stop_failed.raise();
            async { Err(ShoveError::Connection("put-back seek failed".to_string())) }
        })
        .await;

        assert!(
            matches!(&result, Err(ShoveError::Connection(msg)) if msg == "put-back seek failed"),
            "the stop's failure is returned: {result:?}"
        );
    }

    /// The same error with nothing raised, from an attempt that ended on an
    /// error the stop raced, still ends the run with `Ok(())`.
    #[tokio::test(start_paused = true)]
    async fn a_retryable_error_once_the_stop_has_fired_ends_the_run_clean() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let stop_failed = StopFailed::default();
        let result = run_with_reconnect(&shutdown, "test", None, Some(&stop_failed), || async {
            Err(ShoveError::Connection("boom".to_string()))
        })
        .await;

        assert!(result.is_ok(), "expected a clean end, got {result:?}");
    }

    /// The stop's wait for its handlers that fails raises the flag and hands
    /// its error on, so the run ends with it; one that ends clean raises
    /// nothing.
    #[test]
    fn a_failed_wait_for_the_handlers_raises_the_flag() {
        let failed = StopFailed::default();
        let result = failed.raise_if_failed::<()>(
            Err(ShoveError::Connection("put-back seek failed".to_string())),
            "test",
        );
        assert!(
            matches!(&result, Err(ShoveError::Connection(msg)) if msg == "put-back seek failed"),
            "the wait's error is handed on: {result:?}"
        );
        assert!(failed.raised(), "a failed wait raises the flag");

        let clean = StopFailed::default();
        assert!(clean.raise_if_failed(Ok(()), "test").is_ok());
        assert!(!clean.raised(), "a clean wait raises nothing");
    }
}

/// A copy's `Shove-Message-Id`: its record's base and one suffix for the hop.
#[cfg(test)]
mod message_id_tests {
    use super::*;

    fn record(headers: &[(&str, &str)]) -> RecordHeaders {
        RecordHeaders {
            strings: Arc::new(
                headers
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            ),
            wire: None,
            partition: 3,
            offset: 17,
        }
    }

    #[test]
    fn the_base_is_the_carried_base_then_the_record_id_then_the_position() {
        let carried = record(&[
            (BASE_MESSAGE_ID_HEADER, "inv-r7"),
            (MESSAGE_ID_HEADER, "inv-r7-r1"),
        ]);
        assert_eq!(carried.base_message_id("orders"), "inv-r7");
        let original = record(&[(MESSAGE_ID_HEADER, "inv-r7")]);
        assert_eq!(original.base_message_id("orders"), "inv-r7");
        assert_eq!(record(&[]).base_message_id("orders"), "orders-3-17");
        let empty = record(&[(BASE_MESSAGE_ID_HEADER, ""), (MESSAGE_ID_HEADER, "")]);
        assert_eq!(empty.base_message_id("orders"), "orders-3-17");
    }

    #[test]
    fn a_copy_id_is_its_base_and_one_suffix_for_the_hop() {
        assert_eq!(copy_message_id("inv-r7", Hop::Retry(1)), "inv-r7-r1");
        assert_eq!(copy_message_id("a-dlq", Hop::DeadLetter(1)), "a-dlq-dlq1");
        assert_eq!(copy_message_id("a", Hop::DeadLetter(2)), "a-dlq2");
        let deferred = copy_message_id("a", Hop::Defer);
        assert!(
            deferred
                .strip_prefix("a-d")
                .is_some_and(|suffix| Uuid::try_parse(suffix).is_ok()),
            "a Defer copy's id is the base, `-d` and a UUID"
        );
        assert_ne!(deferred, copy_message_id("a", Hop::Defer));
    }
}

/// [`consumer_config`]: the librdkafka settings every shove consumer is
/// created with, the fetch queue's among them.
#[cfg(test)]
mod consumer_config_tests {
    use super::*;

    fn config_of(fetch_queue: &FetchQueueTuning) -> Result<ClientConfig> {
        consumer_config(
            ClientConfig::new(),
            "orders-consumer",
            "shove-test",
            KafkaAutoOffsetReset::Earliest,
            MAX_POLL_INTERVAL_MS,
            fetch_queue,
            "orders",
        )
    }

    #[test]
    fn every_consumer_is_created_with_the_default_fetch_queue_backoff() {
        let config = config_of(&FetchQueueTuning::default()).expect("the default tuning is valid");

        assert_eq!(config.get("fetch.queue.backoff.ms"), Some("25"));
        assert_eq!(config.get("queued.min.messages"), None);
        assert_eq!(config.get("queued.max.messages.kbytes"), None);
    }

    #[test]
    fn the_overrides_reach_the_config_and_the_pinned_settings_stay() {
        let config = config_of(&FetchQueueTuning {
            queued_min_messages: Some(5_000),
            queued_max_messages_kbytes: Some(2_048),
            fetch_queue_backoff_ms: Some(250),
        })
        .expect("an in-range tuning is valid");

        assert_eq!(config.get("queued.min.messages"), Some("5000"));
        assert_eq!(config.get("queued.max.messages.kbytes"), Some("2048"));
        assert_eq!(config.get("fetch.queue.backoff.ms"), Some("250"));
        assert_eq!(config.get("group.id"), Some("orders-consumer"));
        assert_eq!(config.get("client.id"), Some("shove-test"));
        assert_eq!(config.get("enable.auto.commit"), Some("false"));
        assert_eq!(config.get("fetch.min.bytes"), Some("1"));
        assert_eq!(config.get("fetch.wait.max.ms"), Some("50"));
    }

    /// What librdkafka itself reads back: the default backoff, and its own
    /// defaults for the two thresholds nobody set.
    #[test]
    fn librdkafka_reads_back_the_default_backoff_and_keeps_its_threshold_defaults() {
        let native = config_of(&FetchQueueTuning::default())
            .expect("the default tuning is valid")
            .create_native_config()
            .expect("librdkafka takes the consumer config");

        assert_eq!(native.get("fetch.queue.backoff.ms").as_deref(), Ok("25"));
        assert_eq!(native.get("queued.min.messages").as_deref(), Ok("100000"));
        assert_eq!(
            native.get("queued.max.messages.kbytes").as_deref(),
            Ok("65536")
        );
    }

    #[test]
    fn an_out_of_range_setting_builds_no_config() {
        let err = config_of(&FetchQueueTuning {
            queued_min_messages: Some(0),
            ..FetchQueueTuning::default()
        })
        .expect_err("an out-of-range setting is refused");

        assert!(
            matches!(&err, ShoveError::Topology(msg)
                if msg.contains("orders") && msg.contains("with_queued_min_messages")),
            "{err:?}"
        );
    }
}

/// A fetch queue setting outside librdkafka's range is refused where each
/// consume path starts, before any broker request: the broker the client
/// points at is never listening.
#[cfg(test)]
mod fetch_queue_guard_tests {
    use super::*;
    use crate::topology::{SequenceFailure, TopologyBuilder};

    /// Far above a refusal that makes no request, far below a hang.
    const REFUSAL_BOUND: Duration = Duration::from_secs(10);

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Entry {
        account: String,
    }

    struct Plain;
    impl Topic for Plain {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("fetch-queue-guard-plain").build())
        }
    }
    impl NotSequenced for Plain {}

    struct WithDlq;
    impl Topic for WithDlq {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("fetch-queue-guard-dlq").dlq().build())
        }
    }

    struct Ledger;
    impl Topic for Ledger {
        type Message = Entry;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("fetch-queue-guard-ledger")
                    .sequenced(SequenceFailure::FailAll)
                    .hold_queue(Duration::from_secs(5))
                    .dlq()
                    .build()
            })
        }
    }
    impl SequencedTopic for Ledger {
        fn sequence_key(msg: &Entry) -> String {
            msg.account.clone()
        }
    }

    struct NoopHandler;
    impl<T: Topic<Message = Entry>> MessageHandler<T> for NoopHandler {
        type Context = ();
        async fn handle(&self, _: Entry, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    async fn consumer() -> KafkaConsumer {
        let client = KafkaClient::connect(&super::super::client::KafkaConfig::new("127.0.0.1:1"))
            .await
            .expect("client construction is lazy");
        KafkaConsumer::new(client)
    }

    /// The reconnect budget of one bounds a path that does not refuse to one
    /// refused connection.
    fn out_of_range() -> crate::ConsumerOptions<Kafka> {
        crate::ConsumerOptions::<Kafka>::new()
            .with_queued_min_messages(0)
            .with_max_reconnect_attempts(1)
            .with_shutdown(CancellationToken::new())
    }

    fn refusal(err: ShoveError) -> String {
        match err {
            ShoveError::Topology(msg) => msg,
            other => panic!("expected ShoveError::Topology, got {other:?}"),
        }
    }

    fn assert_names(msg: &str, queue: &str) {
        assert!(msg.contains(queue), "the message names the topic: {msg}");
        assert!(
            msg.contains("with_queued_min_messages") && msg.contains("1..=10000000"),
            "the message names the setter and the range: {msg}"
        );
    }

    #[tokio::test]
    async fn run_refuses_an_out_of_range_setting_before_any_broker_request() {
        let err = tokio::time::timeout(
            REFUSAL_BOUND,
            consumer()
                .await
                .run::<Plain, _>(NoopHandler, (), out_of_range()),
        )
        .await
        .expect("the refusal makes no broker request")
        .expect_err("an out-of-range setting must be refused");

        assert_names(&refusal(err), "fetch-queue-guard-plain");
    }

    #[tokio::test]
    async fn run_fifo_refuses_an_out_of_range_setting_before_any_broker_request() {
        let err = tokio::time::timeout(
            REFUSAL_BOUND,
            consumer()
                .await
                .run_fifo::<Ledger, _>(NoopHandler, (), out_of_range()),
        )
        .await
        .expect("the refusal makes no broker request")
        .expect_err("an out-of-range setting must be refused");

        assert_names(&refusal(err), "fetch-queue-guard-ledger");
    }

    #[tokio::test]
    async fn run_dlq_refuses_an_out_of_range_setting_before_any_broker_request() {
        let err = tokio::time::timeout(
            REFUSAL_BOUND,
            consumer()
                .await
                .run_dlq_with_options::<WithDlq, _>(NoopHandler, (), out_of_range()),
        )
        .await
        .expect("the refusal makes no broker request")
        .expect_err("an out-of-range setting must be refused");

        assert_names(&refusal(err), "fetch-queue-guard-dlq");
    }

    /// A broadcast subscription takes the same settings, and `subscribe()`
    /// refuses a bad one synchronously, before it spawns the loop.
    #[test]
    fn a_broadcast_subscription_refuses_an_out_of_range_setting() {
        let err = KafkaConsumer::check_broadcast_options(
            "fetch-queue-guard-plain",
            &out_of_range().into_inner(),
        )
        .expect_err("an out-of-range setting must be refused");

        assert_names(&refusal(err), "fetch-queue-guard-plain");
    }

    /// Negative control: the same options with the setting inside its range
    /// pass the broadcast check.
    #[test]
    fn a_broadcast_subscription_admits_a_setting_inside_its_range() {
        let options = crate::ConsumerOptions::<Kafka>::new()
            .with_queued_min_messages(1)
            .with_queued_max_messages_kbytes(2_097_151)
            .with_fetch_queue_backoff_ms(0)
            .into_inner();

        KafkaConsumer::check_broadcast_options("fetch-queue-guard-plain", &options)
            .expect("settings inside the ranges are admitted");
    }
}
