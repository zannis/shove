//! Backend-agnostic retry/DLQ routing decisions, shared across consumer
//! backends so the boundary logic lives (and is tested) in exactly one place.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::Outcome;
use crate::topology::SequenceFailure;

/// Hold-queue tier for a given retry count, clamped to the last tier.
/// Caller guarantees `hold_queue_count > 0`.
#[allow(dead_code)] // unused in an aws-sns-sqs-only build; see the note on `decide_retry`
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
#[allow(dead_code)] // unused in an aws-sns-sqs-only build; see the note on `decide_retry`
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

/// The outcome a handler timeout resolves to, given the consumer's optional
/// `handler_timeout_outcome` override. `None` means "keep the historical
/// default", which is [`Outcome::Retry`] on every backend that maps a timeout
/// onto an outcome at all. Redis Streams does not — it leaves the entry in the
/// PEL for `XAUTOCLAIM` — and only reaches this helper once an override is set.
///
/// Single source of truth so the five backends that hand-roll their own
/// `invoke_handler` cannot drift apart on the default.
#[allow(dead_code)] // every consumer is feature-gated; dead under --no-default-features
pub(crate) fn handler_timeout_outcome(configured: Option<Outcome>) -> Outcome {
    configured.unwrap_or(Outcome::Retry)
}

/// Extra time a shutdown drain waits on top of the handler's own deadline.
///
/// A drain waits on a channel fed by a handler that is *already* bounded by
/// `handler_timeout`, so the two deadlines are racing over the same work. Give
/// the inner one room to win: it knows the real outcome, including a
/// configured `handler_timeout_outcome`, whereas the drain can only guess.
///
/// Without this grace the outer timer can fire first — the inner deadline does
/// not start until the spawned task is first polled, so a shutdown that begins
/// before that poll sets an *earlier* outer deadline, and even when the task
/// did start, delivering the outcome through the channel can lose a photo
/// finish on a saturated runtime at shutdown.
#[allow(dead_code)] // only the backends with a spawned-handler drain use this
pub(crate) const DRAIN_GRACE: Duration = Duration::from_secs(5);

/// How long a shutdown drain should wait for one in-flight handler's outcome.
///
/// Single source of truth so the backends that drain spawned handlers
/// (RabbitMQ, SQS) cannot drift apart on the margin.
#[allow(dead_code)] // only the backends with a spawned-handler drain use this
pub(crate) fn shutdown_drain_timeout(handler_timeout: Option<Duration>) -> Duration {
    handler_timeout
        .unwrap_or(crate::DEFAULT_HANDLER_TIMEOUT)
        .saturating_add(DRAIN_GRACE)
}

/// What the shutdown drain's own expiry resolves to.
///
/// `handler_timeout_outcome` answers "what does a *handler timeout* mean here",
/// so honouring it in the drain is only correct when a handler deadline exists
/// and has therefore already fired: the drain waits `handler_timeout +
/// DRAIN_GRACE`, so its expiry implies the inner deadline expired first and the
/// handler is no longer running.
///
/// With `without_handler_timeout()` the handler is unbounded — the drain's
/// expiry is a *shutdown* backstop firing against a task that is still working.
/// Retiring the delivery there would lose in-flight work: `Ack` deletes it and
/// `Reject` sends it to the DLQ (or drops it, with no DLQ configured), while the
/// handler runs on and may still succeed. So the backstop stays at `Retry` and
/// the broker redelivers, regardless of the configured timeout outcome.
#[allow(dead_code)] // only the backends with a spawned-handler drain use this
pub(crate) fn drain_timeout_outcome(
    handler_timeout: Option<Duration>,
    configured: Option<Outcome>,
) -> Outcome {
    match handler_timeout {
        Some(_) => handler_timeout_outcome(configured),
        None => Outcome::Retry,
    }
}

/// Whether the retry budget is exhausted. `max_retries = N` permits N retries,
/// so a message is terminal once `retry_count >= max_retries`. Single source of
/// truth for the boundary shared by `decide_retry` and pre-handler gates.
#[allow(dead_code)] // unused in an aws-sns-sqs-only build; see the note on `decide_retry`
pub(crate) fn retries_exhausted(retry_count: u32, max_retries: u32) -> bool {
    retry_count >= max_retries
}

/// Decide the routing for `outcome`. The retry-budget boundary lives here:
/// `max_retries = N` permits 1 initial attempt + N retries, so the message
/// goes to the DLQ once `retry_count >= max_retries`.
///
/// This module is compiled for any backend feature because SNS/SQS needs
/// [`handler_timeout_outcome`], but SNS/SQS hand-rolls its own retry-budget
/// checks and reaches none of the rest — so in a build with `aws-sns-sqs` and
/// no other backend, everything here except that one helper is genuinely
/// unused. Hence the `dead_code` allows, rather than a narrower `cfg`, which
/// would have to enumerate the other five backends on every item.
#[allow(dead_code)]
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

    #[test]
    fn handler_timeout_outcome_defaults_to_retry() {
        assert!(matches!(handler_timeout_outcome(None), Outcome::Retry));
    }

    #[test]
    fn handler_timeout_outcome_honours_override() {
        assert!(matches!(
            handler_timeout_outcome(Some(Outcome::Defer)),
            Outcome::Defer
        ));
        assert!(matches!(
            handler_timeout_outcome(Some(Outcome::Reject)),
            Outcome::Reject
        ));
        assert!(matches!(
            handler_timeout_outcome(Some(Outcome::Ack)),
            Outcome::Ack
        ));
    }

    #[test]
    fn shutdown_drain_outlasts_the_handler_deadline() {
        // The point of the grace: the handler's own bounded wait must be able
        // to win the race and report the real outcome. An equal deadline —
        // what both drains used to use — is what let the drain overwrite a
        // configured timeout outcome with Retry.
        let handler = Duration::from_secs(30);
        assert!(shutdown_drain_timeout(Some(handler)) > handler);
        assert_eq!(shutdown_drain_timeout(Some(handler)), handler + DRAIN_GRACE);
    }

    #[test]
    fn shutdown_drain_falls_back_to_the_default_handler_timeout() {
        assert_eq!(
            shutdown_drain_timeout(None),
            crate::DEFAULT_HANDLER_TIMEOUT + DRAIN_GRACE
        );
    }

    #[test]
    fn drain_expiry_honours_the_override_when_a_deadline_exists() {
        let handler = Some(Duration::from_secs(30));
        assert!(matches!(
            drain_timeout_outcome(handler, Some(Outcome::Ack)),
            Outcome::Ack
        ));
        assert!(matches!(
            drain_timeout_outcome(handler, Some(Outcome::Reject)),
            Outcome::Reject
        ));
        assert!(matches!(
            drain_timeout_outcome(handler, None),
            Outcome::Retry
        ));
    }

    #[test]
    fn drain_expiry_stays_retry_when_handler_deadlines_are_disabled() {
        // `without_handler_timeout()` leaves the handler unbounded, so the
        // drain's expiry fires while it is still running. Retiring the delivery
        // there — Ack deletes it, Reject DLQs or drops it — would lose in-flight
        // work, so the configured timeout outcome must not apply.
        for configured in [Outcome::Ack, Outcome::Reject, Outcome::Defer] {
            assert!(
                matches!(
                    drain_timeout_outcome(None, Some(configured.clone())),
                    Outcome::Retry
                ),
                "drain backstop retired a still-running handler for {configured:?}"
            );
        }
        assert!(matches!(drain_timeout_outcome(None, None), Outcome::Retry));
    }

    #[test]
    fn shutdown_drain_saturates_instead_of_overflowing() {
        // A caller is free to pass Duration::MAX; adding the grace must not
        // panic in a release build's debug-assert-free arithmetic.
        assert_eq!(shutdown_drain_timeout(Some(Duration::MAX)), Duration::MAX);
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
