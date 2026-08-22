//! Backend-agnostic retry/DLQ routing decisions, shared across consumer
//! backends so the boundary logic lives (and is tested) in exactly one place.

use crate::Outcome;

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
}
