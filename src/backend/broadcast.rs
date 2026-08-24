//! Internal `BroadcastImpl` trait. Backends with an ephemeral per-instance
//! subscription primitive implement this; the public
//! [`BroadcastSubscriber<B>`](crate::broadcast::BroadcastSubscriber) delegates
//! here.
//!
//! Kept separate from [`ConsumerImpl`](crate::backend::ConsumerImpl) so that a
//! backend can be a full consumer without being able to broadcast — which is
//! exactly SQS's situation, and the reason
//! [`HasBroadcast`](crate::backend::capability::HasBroadcast) exists.

use std::future::Future;

use crate::backend::ConsumerOptionsInner;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::Topic;

// Anchored by the InMemory port's `_anchor_broadcast_impl` helper in
// `backend::mod`; genuinely uncalled under `--no-default-features`, where no
// backend is compiled at all.
#[allow(dead_code)]
pub(crate) trait BroadcastImpl: Send + Sync {
    /// Create this process's own ephemeral subscription to `T`'s topic and run
    /// `handler` against it until `options.shutdown` fires.
    ///
    /// # Contract
    ///
    /// - **Deliver-new.** Only messages published after this future has
    ///   subscribed are delivered. Nothing published earlier is replayed.
    /// - **Nothing survives.** Every piece of broker-side state the
    ///   subscription creates is torn down before the returned future
    ///   resolves — no consumer group, no durable consumer, no leftover queue.
    ///   The teardown also has to survive the task being dropped mid-run,
    ///   which is what a drain-timeout abort does.
    /// - **One consumer.** Exactly one delivery loop per call. Broadcast has no
    ///   autoscaling: a second consumer in the same process would split this
    ///   subscription rather than duplicate it.
    fn run_broadcast<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>;
}

// Gated to the backends that actually call it, for two reasons. `routing` is
// itself compiled only when some backend is, so an ungated body fails to
// resolve `crate::routing` under `--no-default-features`. And InMemory is
// deliberately *not* in the list: its broadcast path reuses its own
// `route_outcome` over a private buffer, so including it would leave this
// re-export unused under `--features inmemory` alone — which `-D warnings`
// rejects, and which CI's per-feature clippy legs would catch.
#[cfg(any(feature = "nats", feature = "redis-streams"))]
mod settling {
    use std::time::Duration;

    use crate::metrics;
    use crate::outcome::Outcome;
    use crate::routing::{RetryDecision, decide_retry};

    /// How long a `Defer` waits before the same message is handed back to the
    /// handler.
    ///
    /// A broadcast topology cannot declare hold queues (`build()` rejects the
    /// pair), so there is no configured backoff to read. One second matches the
    /// fallback every backend's `route_outcome` already uses when `hold_queues` is
    /// empty, so a `Defer` on a broadcast subscription is paced like a `Defer`
    /// anywhere else.
    #[allow(dead_code)] // Callers gated behind backend features.
    pub(crate) const BROADCAST_DEFER_DELAY: Duration = Duration::from_secs(1);

    /// What the delivery loop should do with a message after the handler has run.
    #[allow(dead_code)] // Callers gated behind backend features.
    pub(crate) enum BroadcastAction {
        /// The message is finished with — acked, or discarded and already counted.
        Done,
        /// `Outcome::Defer`: hand the *same* message back to the handler after
        /// [`BROADCAST_DEFER_DELAY`], within this subscription only.
        Redeliver,
    }

    /// Settle a handler outcome on an ephemeral broadcast subscription.
    ///
    /// Shared by every backend's broadcast loop so the terminal accounting cannot
    /// drift between them — the recurring cross-backend defect this crate keeps
    /// paying for when each `route_outcome` restates the same rule.
    ///
    /// The decision itself still goes through [`decide_retry`], with the retry
    /// budget pinned to zero exactly as
    /// [`BroadcastSubscriber::subscribe`](crate::broadcast::BroadcastSubscriber::subscribe)
    /// pins it. That is deliberate: a `Retry` must land on the *existing* no-DLQ
    /// terminal arm and report `reason="max_retries_exceeded"`, so an operator's
    /// existing `shove_messages_discarded_total` alert covers broadcast without
    /// being taught about a second, quieter discard path.
    #[allow(dead_code)] // Callers gated behind backend features.
    pub(crate) fn settle_broadcast_outcome(
        outcome: &Outcome,
        topic: &str,
        group: Option<&str>,
    ) -> BroadcastAction {
        match decide_retry(outcome, 0, 0) {
            RetryDecision::Ack => BroadcastAction::Done,
            RetryDecision::Dlq { reason } => {
                let fail_reason = match reason {
                    "rejected" => metrics::FailReason::Rejected,
                    _ => metrics::FailReason::MaxRetriesExceeded,
                };
                tracing::warn!(
                    topic,
                    reason,
                    "broadcast subscription has no DLQ — discarding message"
                );
                // Confirmed immediately, unlike every other backend's terminal
                // path, and that is correct rather than sloppy: an ephemeral
                // broadcast subscription has no retirement operation that can fail.
                // NATS reads it with `AckPolicy::None` and Redis with a bare
                // `XREAD` (no group, so no PEL and no `XACK`), so nothing on either
                // broker is still holding the delivery and no redelivery can
                // resurrect it. The message is already gone at this point.
                metrics::record_terminal(topic, group, fail_reason, false).confirm();
                BroadcastAction::Done
            }
            // `Hold { increment: false }` is `Outcome::Defer`. `increment: true`
            // (a `Retry` with budget left) is unreachable here because the budget
            // is pinned to zero, but redelivering is the safe reading of it either
            // way: it keeps the message alive rather than dropping it uncounted.
            RetryDecision::Hold { .. } => BroadcastAction::Redeliver,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn settles_done(outcome: Outcome) -> bool {
            matches!(
                settle_broadcast_outcome(&outcome, "t", None),
                BroadcastAction::Done
            )
        }

        /// The load-bearing case, and the one that would be silently wrong if
        /// the budget boundary were `>` instead of `>=`: with `max_retries`
        /// pinned to zero, the *first* `Retry` must be terminal. If it resolved
        /// to `Redeliver` instead, a failing handler would be handed the same
        /// message forever — no DLQ, no counter moving, and nothing in the
        /// delivery loop to stop it.
        #[test]
        fn retry_is_terminal_on_the_first_attempt() {
            assert!(
                settles_done(Outcome::Retry),
                "a broadcast Retry must discard immediately, not redeliver"
            );
        }

        #[test]
        fn reject_is_terminal() {
            assert!(settles_done(Outcome::Reject));
        }

        #[test]
        fn ack_is_terminal() {
            assert!(settles_done(Outcome::Ack));
        }

        /// `Defer` is the only outcome that comes back for another attempt —
        /// the "redelivered within this subscription only" half of the contract.
        #[test]
        fn defer_redelivers() {
            assert!(
                matches!(
                    settle_broadcast_outcome(&Outcome::Defer, "t", None),
                    BroadcastAction::Redeliver
                ),
                "a broadcast Defer must redeliver within the subscription"
            );
        }

        /// Pins the reason strings the discard is attributed to. These are not
        /// cosmetic: `docs/pages/concepts/broadcast.mdx` promises that an
        /// existing `shove_messages_discarded_total` alert covers broadcast
        /// unchanged, which only holds while broadcast reports the same reasons
        /// a no-DLQ topology already does.
        #[test]
        fn terminal_reasons_match_the_existing_no_dlq_arm() {
            assert_eq!(
                decide_retry(&Outcome::Retry, 0, 0),
                RetryDecision::Dlq {
                    reason: "max_retries_exceeded"
                }
            );
            assert_eq!(
                decide_retry(&Outcome::Reject, 0, 0),
                RetryDecision::Dlq { reason: "rejected" }
            );
        }
    }
}

#[cfg(any(feature = "nats", feature = "redis-streams"))]
pub(crate) use settling::{BROADCAST_DEFER_DELAY, BroadcastAction, settle_broadcast_outcome};
