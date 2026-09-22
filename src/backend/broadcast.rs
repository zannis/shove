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
    ///   A caller may opt out of this per subscription with
    ///   `ConsumerOptions::with_broadcast_start`, on a backend whose
    ///   [`check_options`](Self::check_options) admits the start; the default
    ///   is the tail everywhere, and Kafka is the one backend that honours
    ///   the head and a timestamp on this version.
    /// - **Nothing survives.** Every piece of broker-side state the
    ///   subscription creates is torn down before the returned future
    ///   resolves — no consumer group, no durable consumer, no leftover queue.
    ///   The teardown also has to survive the task being dropped mid-run,
    ///   which is what a drain-timeout abort does.
    ///
    ///   Where a backend's teardown is a request that can be refused, that
    ///   holds for the path where the request succeeds, and *eventual* cleanup
    ///   is the guarantee otherwise. NATS is the case in point: an ephemeral
    ///   consumer is deleted by an awaited request, retried a bounded number
    ///   of times, then left to a drop guard's detached attempt and finally to
    ///   the server's `inactive_threshold`. So a future can resolve with the
    ///   consumer still present for up to that interval after a delete the
    ///   server would not honour. Redis and Kafka have nothing to refuse —
    ///   neither creates broker-side state to begin with — and RabbitMQ's
    ///   exclusive auto-delete queue is removed by the broker on disconnect.
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

    /// Refuse, synchronously and before [`run_broadcast`](Self::run_broadcast)
    /// is spawned, every option this backend's subscription would not read.
    ///
    /// `BroadcastSubscriber::subscribe` calls this and returns the error to
    /// the caller, so a setting that changes nothing is a `Topology` error at
    /// the call site rather than a silent no-op. Two things are checked here:
    /// a [`BroadcastStart`] the backend cannot honour (every backend but Kafka
    /// starts at the tail only on this version, so it refuses `Head` and
    /// `Timestamp` through [`unsupported_broadcast_start`]), and a
    /// backend-specific knob whose only readers are the group paths, such as
    /// Kafka's commit interval and `auto.offset.reset`.
    fn check_options(queue: &str, options: &ConsumerOptionsInner) -> Result<()>;
}

// Gated to the tail-only backends, which are the only callers; Kafka honours
// every start and never builds this error.
#[cfg(any(
    feature = "inmemory",
    feature = "nats",
    feature = "rabbitmq",
    feature = "redis-streams"
))]
mod tail_only {
    use crate::backend::ConsumerOptionsInner;
    use crate::broadcast::BroadcastStart;
    use crate::error::{Result, ShoveError};

    /// The error a backend returns from `BroadcastImpl::check_options` for a
    /// start it cannot honour. `backend` is the display name, `why` one
    /// clause that names the primitive that fixes the start at the tail.
    fn unsupported_broadcast_start(
        backend: &str,
        queue: &str,
        start: BroadcastStart,
        why: &str,
    ) -> ShoveError {
        ShoveError::Topology(format!(
            "topic '{queue}': `with_broadcast_start({start:?})` is not supported on {backend}, \
             whose broadcast subscription starts at the tail only ({why}). Drop the call or \
             pass `BroadcastStart::Tail`."
        ))
    }

    /// The `check_options` body every tail-only backend shares: `None` and
    /// `Tail` change nothing and pass, `Head` and `Timestamp` are refused.
    pub(crate) fn refuse_start_other_than_tail(
        backend: &str,
        queue: &str,
        options: &ConsumerOptionsInner,
        why: &str,
    ) -> Result<()> {
        match options.broadcast_start {
            None | Some(BroadcastStart::Tail) => Ok(()),
            Some(start) => Err(unsupported_broadcast_start(backend, queue, start, why)),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use tokio_util::sync::CancellationToken;

        fn options(start: Option<BroadcastStart>) -> ConsumerOptionsInner {
            let mut options =
                ConsumerOptionsInner::defaults_with_shutdown(CancellationToken::new());
            options.broadcast_start = start;
            options
        }

        /// A start that changes nothing passes; a start the backend cannot
        /// honour is a `Topology` error that names the topic, the variant,
        /// the backend and the way out.
        #[test]
        fn a_tail_only_backend_refuses_head_and_timestamp() {
            for start in [None, Some(BroadcastStart::Tail)] {
                refuse_start_other_than_tail("Example", "q", &options(start), "why")
                    .expect("the tail changes nothing and passes");
            }
            for start in [
                BroadcastStart::Head,
                BroadcastStart::Timestamp(1_700_000_000_000),
            ] {
                let err =
                    refuse_start_other_than_tail("Example", "q", &options(Some(start)), "why")
                        .expect_err("a start the backend cannot honour is refused");
                let ShoveError::Topology(msg) = err else {
                    panic!("expected ShoveError::Topology, got {err:?}");
                };
                assert!(msg.contains("topic 'q'"), "{msg}");
                assert!(
                    msg.contains(&format!("with_broadcast_start({start:?})")),
                    "{msg}"
                );
                assert!(msg.contains("Example") && msg.contains("(why)"), "{msg}");
                assert!(msg.contains("BroadcastStart::Tail"), "{msg}");
            }
        }
    }
}

#[cfg(any(
    feature = "inmemory",
    feature = "nats",
    feature = "rabbitmq",
    feature = "redis-streams"
))]
pub(crate) use tail_only::refuse_start_other_than_tail;

// Gated to the backends that actually call into it, for two reasons.
// `routing` is itself compiled only when some backend is, so an ungated body
// fails to resolve `crate::routing` under `--no-default-features`. And neither
// InMemory nor RabbitMQ calls `settle_broadcast_outcome`: InMemory's broadcast
// path reuses its own `route_outcome` over a private buffer, and RabbitMQ
// settles through `router::route_reject` / `nack_requeue` because an AMQP
// delivery has to be nacked on the channel it arrived on — the router already
// records the terminal metric there, so going through this helper too would
// count it twice. InMemory *is* in the cfg list, but only for
// `BROADCAST_DEFER_DELAY`: its in-place broadcast `Defer` paces on the same
// constant, so the test substrate redelivers on the schedule production does.
// The re-exports below are split to match — a name re-exported under a feature
// that never uses it is an unused import, which `-D warnings` rejects and
// CI's per-feature clippy legs would catch.
#[cfg(any(
    feature = "inmemory",
    feature = "kafka",
    feature = "nats",
    feature = "redis-streams"
))]
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
    /// Shared by every broadcast loop that settles in-process — Kafka, NATS and
    /// Redis — so the terminal accounting cannot drift between them, the
    /// recurring cross-backend defect this crate keeps paying for when each
    /// `route_outcome` restates the same rule. RabbitMQ is the exception, and
    /// only because its terminal arm has to nack a live AMQP delivery: see the
    /// gate above.
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
                // NATS reads it with `AckPolicy::None`, Redis with a bare `XREAD`
                // (no group, so no PEL and no `XACK`), and Kafka with an
                // assign-only handle that never commits — so nothing on any of the
                // three brokers is still holding the delivery and no redelivery can
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

#[cfg(any(
    feature = "inmemory",
    feature = "kafka",
    feature = "nats",
    feature = "redis-streams"
))]
pub(crate) use settling::BROADCAST_DEFER_DELAY;
#[cfg(any(feature = "kafka", feature = "nats", feature = "redis-streams"))]
pub(crate) use settling::{BroadcastAction, settle_broadcast_outcome};
