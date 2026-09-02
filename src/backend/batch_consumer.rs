//! Shared internal batch-consumption machinery.
//!
//! Backends with a batch-consumption primitive (buffering up to N messages
//! before handing them to the handler as one call) implement
//! [`BatchConsumerImpl`]; the public
//! [`BatchConsumer<B>`](crate::batch_consumer::BatchConsumer) delegates here.
//! Named `batch_consumer` rather than `batch` because [`crate::batch`]
//! already exists, for the publish-side partial-batch report
//! ([`BatchFailure`](crate::batch::BatchFailure)) — this module is the
//! consume side.
//!
//! This module also houses the terminal-settlement machinery Kafka's
//! *single-message* async-commit path depends on — `TerminalDiscard`,
//! `RejectSettlement` and `reject_settlement` — not only its batch path. A
//! later cleanup must not narrow the gate on those three below `kafka`
//! without checking that single-message caller first.
//!
//! # Gating
//!
//! [`BatchConsumerOptionsInner`], [`BatchConsumerImpl`],
//! [`validate_batch_topic`], [`BatchSettlement`] and [`settle_batch_outcome`]
//! are ungated, so their unit tests run under `cargo nextest run
//! --no-default-features` with no backend compiled at all — the same
//! `#[allow(dead_code)]`-over-losing-coverage trade `ConsumerOptionsInner`
//! and `settle_broadcast_outcome` already make.
//!
//! Everything else here is gated `#[cfg(any(feature = "kafka", feature =
//! "inmemory"))]`: both backends now have a batch-consumption implementation
//! and share this settlement machinery. The gate widens per-backend as
//! T2–T5 land in turn — NATS, RabbitMQ, Redis, SQS each add their own
//! feature to the list the moment their `BatchConsumerImpl` exists, exactly
//! as `broadcast.rs`'s gate widened backend by backend. `kafka` itself must
//! never leave the list while it stands, per the single-message dependency
//! noted above.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

#[cfg(feature = "kafka")]
use crate::backends::kafka::KafkaAutoOffsetReset;
use crate::error::{Result, ShoveError};
use crate::handler::BatchMessageHandler;
use crate::outcome::Outcome;
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::{SchemaEnforcement, SchemaRegistry};
use crate::topic::{NotSequenced, Topic};

/// Un-generic lowering of
/// [`BatchConsumerOptions<B>`](crate::batch_consumer::BatchConsumerOptions)
/// passed across the [`BatchConsumerImpl`] trait boundary, mirroring how
/// [`ConsumerOptionsInner`](crate::backend::ConsumerOptionsInner) does the
/// same job on the single-message path.
#[allow(dead_code)] // Fields are read by backend consumers behind feature gates.
pub(crate) struct BatchConsumerOptionsInner {
    pub max_batch_size: usize,
    pub max_batch_age: Duration,
    pub handler_timeout: Option<Duration>,
    /// What a handler timeout resolves to. `None` keeps each backend's
    /// historical default (`Outcome::Retry`). See
    /// [`BatchSettlement`]/[`settle_batch_outcome`] for the batch-wide
    /// outcome table this feeds into.
    pub handler_timeout_outcome: Option<Outcome>,
    pub shutdown: CancellationToken,
    /// Consumer-group name for metrics labels. `None` is treated as `"default"`.
    pub consumer_group: Option<Arc<str>>,
    pub max_message_size: Option<usize>,
    pub max_reconnect_attempts: Option<u32>,

    /// Kafka-only: explicit `group.id` to pass to rdkafka instead of the
    /// default `"{queue}-consumer"`. See
    /// `ConsumerOptionsInner::kafka_group_id` for the single-message
    /// counterpart this mirrors.
    #[cfg(feature = "kafka")]
    pub kafka_group_id: Option<Arc<str>>,
    /// Kafka-only: rdkafka `auto.offset.reset` override. `None` falls back to
    /// the library default of `earliest`.
    #[cfg(feature = "kafka")]
    pub kafka_auto_offset_reset: Option<KafkaAutoOffsetReset>,

    /// Kafka-only: Schema Registry client for decoding Confluent wire-framed
    /// batch messages. `None` disables registry-based decoding.
    #[cfg(feature = "kafka-schema-registry")]
    pub schema_registry: Option<Arc<SchemaRegistry>>,
    /// Kafka-only: how subject mismatches are handled. Default `Enforce`.
    #[cfg(feature = "kafka-schema-registry")]
    pub schema_enforcement: SchemaEnforcement,
    /// Kafka-only: accepted subject set. `None` derives `["{queue}-value"]` at
    /// decode time.
    #[cfg(feature = "kafka-schema-registry")]
    pub schema_accepted_subjects: Option<Vec<Arc<str>>>,
}

/// Internal batch-consumption trait. Backend-specific consumer structs with a
/// batch-consumption primitive implement this; the public
/// [`BatchConsumer<B>`](crate::batch_consumer::BatchConsumer) delegates here.
///
/// Anchored by each backend's own `impl BatchConsumerImpl` under its feature.
/// Under `--no-default-features`, and under any feature set naming no backend
/// with a batch implementation yet, the trait genuinely has no call site —
/// `dead_code` is expected there and the per-trait allow avoids polluting
/// those builds with warnings, exactly as [`ConsumerImpl`](crate::backend::ConsumerImpl)
/// does for the single-message trait.
#[allow(dead_code)]
pub(crate) trait BatchConsumerImpl: Send + Sync {
    fn run_batch<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>;
}

/// Runtime guard mirroring the compile-time [`NotSequenced`] bound.
///
/// `NotSequenced` is a hand-implementable marker: a topic can claim it while
/// its topology still carries sequencing config. Consuming that in batches
/// would bypass ordering silently, so this fails closed instead — moved
/// unchanged from Kafka's `run_batch`, which is where the sequencing-guard
/// unit test (`backends::kafka::consumer::batch_sequencing_guard_tests`)
/// still pins this exact error text through the public delegate.
pub(crate) fn validate_batch_topic<T: Topic>() -> Result<()> {
    let topology = T::topology();
    let queue = topology.queue();
    if topology.sequencing().is_some() {
        return Err(ShoveError::Topology(format!(
            "run_batch called on {queue}, which declares sequencing config; \
             batching and sequencing are mutually exclusive — use run_fifo"
        )));
    }
    Ok(())
}

/// What a batch-wide [`Outcome`] resolves to. The one definition every
/// backend's batch flush routes through, so the mapping cannot drift between
/// them the way per-backend `route_outcome` restatements have drifted before.
///
/// | `Outcome` (whole batch) | `BatchSettlement` | Effect |
/// |---|---|---|
/// | `Ack` | `Commit` | Every message in the batch retires; offsets/positions advance. |
/// | `Reject` | `DeadLetter` | Terminal: every message is dead-lettered (or discarded, with no DLQ configured) and retires. |
/// | `Retry` | `Redeliver` | The whole batch is redelivered — a seek-back / re-buffer, not a republish. There is no per-batch retry budget, so a handler stuck returning `Retry` redelivers forever. |
/// | `Defer` | `Redeliver` | **Identical to `Retry` here.** A batch-wide outcome carries no sequence key, so the `Retry`/`Defer` distinction that matters on the single-message path — spending the retry budget versus not — has no meaning: there is no per-batch budget to spend either way. |
///
/// See [`settle_broadcast_outcome`](crate::backend::broadcast::settle_broadcast_outcome)
/// for the sibling classifier on an ephemeral broadcast subscription: it
/// settles differently, because a broadcast subscription has no redelivery
/// buffer to hold a batch in — a `Retry`/`Reject` there is terminal on the
/// first attempt instead of redelivering.
#[allow(dead_code)] // Callers are backend consumers behind feature gates.
pub(crate) enum BatchSettlement {
    Commit,
    DeadLetter,
    Redeliver,
}

#[allow(dead_code)] // Callers are backend consumers behind feature gates.
pub(crate) fn settle_batch_outcome(outcome: &Outcome) -> BatchSettlement {
    match outcome {
        Outcome::Ack => BatchSettlement::Commit,
        Outcome::Reject => BatchSettlement::DeadLetter,
        Outcome::Retry | Outcome::Defer => BatchSettlement::Redeliver,
    }
}

#[cfg(test)]
mod settle_batch_outcome_tests {
    use super::*;

    #[test]
    fn ack_commits() {
        assert!(matches!(
            settle_batch_outcome(&Outcome::Ack),
            BatchSettlement::Commit
        ));
    }

    #[test]
    fn reject_dead_letters() {
        assert!(matches!(
            settle_batch_outcome(&Outcome::Reject),
            BatchSettlement::DeadLetter
        ));
    }

    #[test]
    fn retry_redelivers() {
        assert!(matches!(
            settle_batch_outcome(&Outcome::Retry),
            BatchSettlement::Redeliver
        ));
    }

    /// The load-bearing case: `Defer` must settle exactly like `Retry`
    /// because a batch-wide outcome has no per-message retry budget for the
    /// two to disagree over.
    #[test]
    fn defer_and_retry_produce_the_same_settlement() {
        assert!(matches!(
            settle_batch_outcome(&Outcome::Defer),
            BatchSettlement::Redeliver
        ));
        assert!(matches!(
            settle_batch_outcome(&Outcome::Retry),
            BatchSettlement::Redeliver
        ));
    }
}

// Gated `any(kafka, inmemory)` — see the module doc's "Gating" section. Named
// `settling` rather than after either backend: it houses the settlement
// classifier + panic/timeout invariant surface both backends' batch loops
// route through, plus the terminal-discard machinery Kafka's single-message
// path also depends on (see the module doc's `kafka` note).
#[cfg(any(feature = "kafka", feature = "inmemory"))]
mod settling {
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::time::{Duration, Instant};

    use futures_util::FutureExt;

    use crate::metrics;
    use crate::outcome::Outcome;
    use crate::retry::Backoff;
    use crate::routing::handler_timeout_outcome;

    /// A terminal discard waiting on the commit that retires its message.
    ///
    /// Which settle method applies is decided where the outcome is routed,
    /// not where the commit lands, so the choice travels with the pending
    /// record.
    pub(crate) enum TerminalDiscard {
        /// Dead-lettered, or terminal on a topic with no DLQ. Counts only
        /// when no DLQ exists — with one, the message is still in it.
        Retired(metrics::PendingDiscard),
        /// A DLQ was configured and the publish to it failed. Nothing holds a
        /// copy, so this counts regardless of the topology.
        Lost(metrics::PendingDiscard),
    }

    impl TerminalDiscard {
        /// The commit landed: the message is genuinely gone.
        pub(crate) fn confirm(self) {
            match self {
                Self::Retired(pending) => pending.confirm(),
                Self::Lost(pending) => pending.confirm_lost(),
            }
        }

        /// The commit did not land, so the message will be redelivered.
        ///
        /// Kafka-only in practice: its offset commit is the one settlement
        /// point in either backend's batch path that can fail after the
        /// terminal decision is made. InMemory retires a rejected message the
        /// instant its DLQ hand-off resolves — there is no later commit that
        /// could still fail — so under `inmemory` alone this has no caller.
        #[allow(dead_code)]
        pub(crate) fn survived(self) {
            match self {
                Self::Retired(pending) | Self::Lost(pending) => pending.survived(),
            }
        }
    }

    /// How a terminally-rejected message's discard must be settled.
    ///
    /// Shared by the single-message and batch reject paths so the two cannot
    /// drift: the decision depends only on whether the topology declares a
    /// DLQ and whether this message's publish to it landed, never on which
    /// path asked.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum RejectSettlement {
        /// It is in the DLQ, so it exists whatever the commit does. Settle
        /// now and keep the ordinary dead-letter path off the commit round
        /// trip.
        InDlq,
        /// No DLQ declared: retiring this message drops it. Counts if the
        /// commit lands.
        Retired,
        /// A DLQ was declared and did not receive it. Nothing holds a copy,
        /// so this is data loss whatever the topology says. Counts if the
        /// commit lands.
        Lost,
    }

    /// Classify a rejected message for [`RejectSettlement`].
    ///
    /// `reached_dlq` is meaningful only when `has_dlq`; with no DLQ declared
    /// there was no publish to succeed or fail, and the message is simply
    /// dropped.
    pub(crate) fn reject_settlement(has_dlq: bool, reached_dlq: bool) -> RejectSettlement {
        match (has_dlq, reached_dlq) {
            (false, _) => RejectSettlement::Retired,
            (true, true) => RejectSettlement::InDlq,
            (true, false) => RejectSettlement::Lost,
        }
    }

    /// First delay after redelivering an un-acked batch, escalating to
    /// [`BATCH_REDELIVERY_BACKOFF_MAX`] — see `flush_batch`'s non-Ack arm.
    const BATCH_REDELIVERY_BACKOFF_INITIAL: Duration = Duration::from_secs(1);

    /// Ceiling on the redelivery delay for a handler that keeps returning
    /// non-Ack.
    const BATCH_REDELIVERY_BACKOFF_MAX: Duration = Duration::from_secs(30);

    /// Redelivery backoff schedule for a batch the handler did not Ack.
    /// Escalates across *consecutive* non-Ack flushes and is reset on the
    /// first Ack, so a wedged handler backs off instead of spinning the
    /// seek-then-recv cycle while a handler that merely hit one bad batch
    /// pays the delay once.
    pub(crate) fn batch_redelivery_backoff() -> Backoff {
        Backoff::new(
            BATCH_REDELIVERY_BACKOFF_INITIAL,
            BATCH_REDELIVERY_BACKOFF_MAX,
        )
    }

    /// [`invoke_handler`] for a whole batch: same panic containment, same
    /// timeout, same instrumentation — in message units, with one duration
    /// observation per flush rather than per message.
    ///
    /// Without this, `run_batch` was the one handler-invoking path in the
    /// crate with neither guard. A panicking flush unwound the batch task
    /// itself, so the consumer died and stayed dead until an external
    /// supervisor noticed; the single-message path turns the same panic into
    /// a redelivery. And because the batch loop is one task, a flush future
    /// that never resolves froze offset commits, rebalance handling and
    /// `shutdown.cancel()` along with it.
    ///
    /// Both failures map to [`Outcome::Retry`], which is honest: the batch
    /// was not acked, so its offsets are not committed, and the caller seeks
    /// back and redelivers it. The messages themselves were moved into the
    /// abandoned future — they come back from the broker, not from memory.
    ///
    /// The handler future is built by `make_fut` *inside* the guard rather
    /// than passed in ready-made. `BatchMessageHandler::handle_batch` is an
    /// ordinary `fn -> impl Future`, so an implementation may panic while
    /// assembling its future — before any of it is awaited. Constructing at
    /// the call site put that panic outside `catch_unwind`, where it unwound
    /// `flush_batch` and killed `run_batch` instead of resolving to `Retry`
    /// as the contract above promises. This mirrors the single-message path,
    /// which never materializes the handler future outside its own guard.
    pub(crate) async fn invoke_batch_handler<F, Fut>(
        make_fut: F,
        timeout: Option<Duration>,
        timeout_outcome: Option<Outcome>,
        topic: &str,
        group: Option<&str>,
        batch_size: u64,
    ) -> Outcome
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Outcome>,
    {
        let _inflight = metrics::InflightGuard::from_refs_n(topic, group, batch_size);
        let started = Instant::now();
        let safe_fut = AssertUnwindSafe(async move { make_fut().await }).catch_unwind();
        let outcome = match timeout {
            Some(duration) => match tokio::time::timeout(duration, safe_fut).await {
                Ok(Ok(o)) => o,
                Ok(Err(_panic)) => {
                    // No `record_failed_n` here: the single-message path
                    // emits no fail metric for a panic either, and
                    // `outcome="retry"` climbing without a matching
                    // `outcome="ack"` is the same wedged-handler signal.
                    // Keeping the two paths identical matters more than the
                    // extra label.
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
                    metrics::record_failed_n(
                        topic,
                        group,
                        metrics::FailReason::Timeout,
                        batch_size,
                    );
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
        metrics::record_processing_duration(
            topic,
            group,
            &outcome,
            started.elapsed().as_secs_f64(),
        );
        metrics::record_consumed_n(topic, group, &outcome, batch_size);
        outcome
    }

    /// [`reject_settlement`]: the terminal-discard decision for a rejected
    /// message.
    ///
    /// `messages_discarded_total` promises every increment is a message that
    /// no longer exists, so getting this matrix wrong is either a false
    /// data-loss alert or the silent loss this PR exists to surface. Both the
    /// single-message `route_outcome` and the batch `flush_batch` reject arm
    /// route through it, so the two paths cannot drift the way earlier
    /// cross-backend fixes did.
    #[cfg(test)]
    mod reject_settlement_tests {
        use super::*;

        /// With no DLQ there is nothing to publish to, so a reject drops the
        /// message. This is the bare-topology shape from CAF-35: the discard
        /// has to be counted, which is precisely what the batch path used to
        /// skip.
        #[test]
        fn no_dlq_is_always_a_plain_discard() {
            assert_eq!(
                reject_settlement(false, false),
                RejectSettlement::Retired,
                "no DLQ declared: the message is dropped and must be counted"
            );
        }

        /// `reached_dlq` is meaningless without a DLQ — there was no publish.
        /// A caller that passes `true` anyway (a batch with no `raw` bytes
        /// buffered, say) must still get a countable discard rather than a
        /// silent `InDlq`.
        #[test]
        fn no_dlq_ignores_the_publish_flag() {
            assert_eq!(reject_settlement(false, true), RejectSettlement::Retired);
        }

        /// The message exists in the DLQ, so no discard is owed however the
        /// commit resolves. Counting here would be a false data-loss alert on
        /// the perfectly ordinary dead-letter path.
        #[test]
        fn a_landed_dlq_publish_owes_no_discard() {
            assert_eq!(reject_settlement(true, true), RejectSettlement::InDlq);
        }

        /// The case `has_dlq` alone gets wrong: a DLQ was declared, the
        /// publish failed, and the offsets commit anyway. Nothing holds a
        /// copy, so this is data loss even though the topology says
        /// otherwise — it must settle with `confirm_lost`, which counts
        /// regardless of `has_dlq`.
        #[test]
        fn a_failed_dlq_publish_is_loss_despite_the_topology() {
            assert_eq!(reject_settlement(true, false), RejectSettlement::Lost);
        }

        /// Only `InDlq` may skip the commit round trip. The other two are
        /// claims that a message is gone, and nothing has retired it until
        /// the offsets actually advance — settling them early is the
        /// false-alert-during-an-outage failure `PendingDiscard` was
        /// introduced to prevent.
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
        /// consumer until something outside the process restarted it. It has
        /// to become a redelivery, exactly like the single-message path's.
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

        /// The batch loop is a single task, so a flush that never resolves
        /// also stops commits, rebalance handling and shutdown. The timeout
        /// is what bounds all three.
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

        /// Panic containment must not depend on the timeout being set:
        /// opting out of the timeout is a statement about how long a flush
        /// may take, not a request to let a panic kill the consumer.
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

        /// `BatchMessageHandler::handle_batch` is an ordinary `fn -> impl
        /// Future`, so an implementation may panic while *building* its
        /// future — validating an argument, indexing a config map — before
        /// returning anything to await.
        ///
        /// That panic used to escape containment: the call was evaluated at
        /// the `flush_batch` call site, outside the `catch_unwind`, so it
        /// unwound the batch task and killed `run_batch` instead of becoming
        /// a redelivery. The closure is what moves construction inside the
        /// guard, and this test is what stops the ready-made-future form
        /// coming back — with a plain `Future` parameter it does not even
        /// compile.
        #[tokio::test]
        async fn a_panic_while_building_the_future_is_contained() {
            fn build_future() -> impl Future<Output = Outcome> {
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

        /// The same, with the deadline disabled — the no-timeout arm has its
        /// own `catch_unwind` and has to construct inside it too.
        #[tokio::test]
        async fn a_panic_while_building_the_future_is_contained_with_no_timeout() {
            fn build_future() -> impl Future<Output = Outcome> {
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

        /// The batch path has its own timeout arm, so it needs its own proof
        /// that `handler_timeout_outcome` reaches it. Without this the
        /// setter would be silently inert for `run_batch` — and a slow flush
        /// would keep burning the retry budget of every message in the
        /// batch.
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
}

#[cfg(any(feature = "kafka", feature = "inmemory"))]
pub(crate) use settling::{
    RejectSettlement, TerminalDiscard, batch_redelivery_backoff, invoke_batch_handler,
    reject_settlement,
};
