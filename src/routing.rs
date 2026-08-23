//! Backend-agnostic retry/DLQ routing decisions, shared across consumer
//! backends so the boundary logic lives (and is tested) in exactly one place.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::Outcome;
use crate::topology::SequenceFailure;

/// Hold-queue tier for a given retry count, clamped to the last tier.
/// Caller guarantees `hold_queue_count > 0`.
pub(crate) fn hold_index(retry_count: u32, hold_queue_count: usize) -> usize {
    debug_assert!(
        hold_queue_count > 0,
        "hold_index called with no hold queues"
    );
    (retry_count as usize).min(hold_queue_count - 1)
}

/// The backend-agnostic decision for what to do with a message after the
/// handler returns `outcome`. Execution (ack/commit/publish/DLQ) and the
/// empty-hold-queue fallback are intentionally left to each backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    /// Handler succeeded — ack/commit the message.
    Ack,
    /// Terminal failure — route to the DLQ with this death reason, then
    /// ack/commit. `reason` is one of "rejected" or "max_retries_exceeded".
    Dlq { reason: &'static str },
    /// Hold-and-redeliver. `increment` is true for `Retry` (consumes retry
    /// budget) and false for `Defer` (does not).
    Hold { increment: bool },
}

/// Whether the retry budget is exhausted. `max_retries = N` permits N retries,
/// so a message is terminal once `retry_count >= max_retries`. Single source of
/// truth for the boundary shared by `decide_retry` and pre-handler gates.
pub(crate) fn retries_exhausted(retry_count: u32, max_retries: u32) -> bool {
    retry_count >= max_retries
}

/// Decide the routing for `outcome`. The retry-budget boundary lives here:
/// `max_retries = N` permits 1 initial attempt + N retries, so the message
/// goes to the DLQ once `retry_count >= max_retries`.
pub(crate) fn decide_retry(outcome: &Outcome, retry_count: u32, max_retries: u32) -> RetryDecision {
    match outcome {
        Outcome::Ack => RetryDecision::Ack,
        Outcome::Reject => RetryDecision::Dlq { reason: "rejected" },
        Outcome::Retry => {
            if retries_exhausted(retry_count, max_retries) {
                RetryDecision::Dlq {
                    reason: "max_retries_exceeded",
                }
            } else {
                RetryDecision::Hold { increment: true }
            }
        }
        Outcome::Defer => RetryDecision::Hold { increment: false },
    }
}

/// The set of sequence keys poisoned by [`SequenceFailure::FailAll`].
///
/// Every sequenced consumer holds one of these. The semantics are identical on
/// all six backends — see `docs/design/sequence-failure-parity.md`:
///
/// * Under [`SequenceFailure::Skip`] the tracker is **inert**: [`poison`] does
///   nothing and [`is_poisoned`] is always `false`, so the `Skip` path never
///   allocates and never takes a lock.
/// * A key is poisoned by any DLQ-terminal event for one of its messages —
///   `Outcome::Reject`, an exhausted retry budget, or a pre-handler rejection
///   (oversize / undeserializable payload).
/// * A poisoned key stays poisoned for the lifetime of the consumer task, as
///   documented on [`SequenceFailure::FailAll`].
/// * The empty key is never poisoned. A message with no sequence key carries
///   no ordering relationship, and poisoning `""` would dead-letter every
///   other unkeyed message that shares the shard.
///
/// Cloning shares the underlying set. That matters because NATS, Kafka and
/// Redis rebuild their inner consume loop through a reconnect wrapper: without
/// shared state a broker blip would silently un-poison every key.
///
/// [`poison`]: PoisonedKeys::poison
/// [`is_poisoned`]: PoisonedKeys::is_poisoned
#[derive(Clone, Default)]
pub(crate) struct PoisonedKeys(Option<Arc<Mutex<HashSet<String>>>>);

impl PoisonedKeys {
    /// Build a tracker for `on_failure`. `Skip` yields an inert tracker.
    pub(crate) fn new(on_failure: SequenceFailure) -> Self {
        match on_failure {
            SequenceFailure::Skip => Self(None),
            SequenceFailure::FailAll => Self(Some(Arc::new(Mutex::new(HashSet::new())))),
        }
    }

    /// Whether messages for `key` must bypass the handler and be dead-lettered.
    pub(crate) fn is_poisoned(&self, key: &str) -> bool {
        match &self.0 {
            None => false,
            Some(set) => !key.is_empty() && Self::lock(set).contains(key),
        }
    }

    /// Poison `key` after a DLQ-terminal event. Returns whether this call
    /// changed anything, so callers can log the transition exactly once.
    pub(crate) fn poison(&self, key: &str) -> bool {
        match &self.0 {
            None => false,
            Some(set) => !key.is_empty() && Self::lock(set).insert(key.to_owned()),
        }
    }

    /// A poisoned `Mutex` only means some other consumer task panicked while
    /// holding the set; the set itself is still a valid `HashSet`, and losing
    /// the poison record would be worse than reusing it.
    fn lock(set: &Mutex<HashSet<String>>) -> std::sync::MutexGuard<'_, HashSet<String>> {
        set.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hold_index_clamps_to_last_tier() {
        assert_eq!(hold_index(0, 2), 0);
        assert_eq!(hold_index(1, 2), 1);
        assert_eq!(hold_index(5, 2), 1); // clamped
        assert_eq!(hold_index(0, 1), 0);
    }

    #[test]
    fn retries_exhausted_at_boundary() {
        assert!(!retries_exhausted(2, 3));
        assert!(retries_exhausted(3, 3));
        assert!(retries_exhausted(0, 0));
    }

    #[test]
    fn ack_decides_ack() {
        assert_eq!(decide_retry(&Outcome::Ack, 0, 3), RetryDecision::Ack);
    }

    #[test]
    fn reject_always_dlqs_rejected() {
        assert_eq!(
            decide_retry(&Outcome::Reject, 0, 3),
            RetryDecision::Dlq { reason: "rejected" }
        );
        assert_eq!(
            decide_retry(&Outcome::Reject, 99, 0),
            RetryDecision::Dlq { reason: "rejected" }
        );
    }

    #[test]
    fn retry_below_budget_holds_and_increments() {
        assert_eq!(
            decide_retry(&Outcome::Retry, 0, 3),
            RetryDecision::Hold { increment: true }
        );
    }

    #[test]
    fn retry_at_budget_boundary_dlqs() {
        // retry_count == max_retries → DLQ (the boundary).
        assert_eq!(
            decide_retry(&Outcome::Retry, 3, 3),
            RetryDecision::Dlq {
                reason: "max_retries_exceeded"
            }
        );
    }

    #[test]
    fn retry_last_allowed_holds() {
        // retry_count == max_retries - 1 → the last permitted retry.
        assert_eq!(
            decide_retry(&Outcome::Retry, 2, 3),
            RetryDecision::Hold { increment: true }
        );
    }

    #[test]
    fn retry_with_zero_budget_dlqs_immediately() {
        assert_eq!(
            decide_retry(&Outcome::Retry, 0, 0),
            RetryDecision::Dlq {
                reason: "max_retries_exceeded"
            }
        );
    }

    #[test]
    fn defer_always_holds_without_increment() {
        assert_eq!(
            decide_retry(&Outcome::Defer, 0, 3),
            RetryDecision::Hold { increment: false }
        );
        assert_eq!(
            decide_retry(&Outcome::Defer, 99, 0),
            RetryDecision::Hold { increment: false }
        );
    }

    // ── PoisonedKeys ──

    #[test]
    fn skip_policy_never_poisons() {
        let poisoned = PoisonedKeys::new(SequenceFailure::Skip);
        assert!(!poisoned.poison("acct-1"));
        assert!(!poisoned.is_poisoned("acct-1"));
    }

    #[test]
    fn fail_all_poisons_only_the_failing_key() {
        let poisoned = PoisonedKeys::new(SequenceFailure::FailAll);
        assert!(poisoned.poison("acct-A"));
        assert!(poisoned.is_poisoned("acct-A"));
        assert!(!poisoned.is_poisoned("acct-B"));
    }

    #[test]
    fn poison_reports_only_the_first_transition() {
        let poisoned = PoisonedKeys::new(SequenceFailure::FailAll);
        assert!(poisoned.poison("acct-A"));
        assert!(!poisoned.poison("acct-A"));
    }

    #[test]
    fn empty_key_is_never_poisoned() {
        // An unkeyed message has no sequence to fail; poisoning "" would
        // dead-letter every other unkeyed message on the shard.
        let poisoned = PoisonedKeys::new(SequenceFailure::FailAll);
        assert!(!poisoned.poison(""));
        assert!(!poisoned.is_poisoned(""));
    }

    #[test]
    fn clones_share_the_same_set() {
        // NATS/Kafka/Redis rebuild their consume loop on reconnect; the clone
        // handed to the new attempt must still see keys poisoned before it.
        let poisoned = PoisonedKeys::new(SequenceFailure::FailAll);
        let reconnected = poisoned.clone();
        poisoned.poison("acct-A");
        assert!(reconnected.is_poisoned("acct-A"));
    }
}
