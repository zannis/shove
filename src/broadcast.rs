//! Public `BroadcastSubscriber<B, Ctx>` — the entry point for ephemeral
//! per-instance fan-out. Gated on `B: HasBroadcast`.

use std::collections::HashSet;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::backend::BroadcastImpl;
use crate::backend::capability::HasBroadcast;
use crate::consumer::ConsumerOptions;
use crate::consumer_supervisor::{SupervisorOutcome, tally_join_result};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::Topic;

/// Where an ephemeral broadcast subscription starts reading.
///
/// Backend-neutral: the same three positions exist on every log-shaped
/// broker, as a Kafka offset, a JetStream deliver policy or a Redis stream
/// id. Set with
/// [`ConsumerOptions::with_broadcast_start`](crate::ConsumerOptions::with_broadcast_start),
/// which is only available on a backend that implements [`HasBroadcast`], and
/// read only by [`BroadcastSubscriber::subscribe`]; the competing-consumer
/// entry points
/// refuse an options value that sets it.
///
/// Kafka honours all three variants. Every other backend starts at the tail
/// only on this version, and refuses [`Head`](Self::Head) and
/// [`Timestamp`](Self::Timestamp) at `subscribe()` with a
/// [`ShoveError::Topology`], rather than silently subscribing at the tail:
/// a start that changes nothing must not be accepted.
///
/// `#[non_exhaustive]`: a later position, such as an absolute sequence or
/// offset, is a new variant and not a breaking change, so match with a
/// wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BroadcastStart {
    /// Only messages published after the subscription exists: deliver-new,
    /// the broadcast contract, and the default on every backend.
    Tail,
    /// Every message the broker still retains, then the tail. The right
    /// start for an instance that rebuilds an in-memory view from the topic
    /// instead of from a separate store.
    Head,
    /// The first message at or after this instant, in **milliseconds since
    /// the Unix epoch**. A partition or stream with nothing at or after the
    /// instant starts at its tail.
    Timestamp(i64),
}

impl BroadcastStart {
    /// The check every backend shares, run by
    /// [`BroadcastSubscriber::subscribe`] before the backend's own: a
    /// timestamp is milliseconds since the Unix epoch, so a negative one
    /// names no instant and is refused here rather than handed to a broker
    /// as a sentinel it may read as something else.
    pub(crate) fn validate(self, queue: &str) -> Result<()> {
        match self {
            BroadcastStart::Timestamp(ms) if ms < 0 => Err(ShoveError::Topology(format!(
                "topic '{queue}': `with_broadcast_start(Timestamp({ms}))` is before the Unix \
                 epoch; a timestamp start is milliseconds since 1970-01-01T00:00:00Z and \
                 cannot be negative."
            ))),
            _ => Ok(()),
        }
    }
}

/// Reject a `.broadcast()` topology arriving at a competing-consumer entry
/// point.
///
/// Broadcast's guarantees are enforced by *where* it runs, not by the topology
/// alone: `BroadcastSubscriber` is the only path with no autoscaling knob, and
/// the only one that creates an ephemeral per-instance subscription. Running a
/// broadcast topology through `ConsumerGroup` or `ConsumerSupervisor` instead
/// would attach a scalable, competing consumer to a shared queue that
/// publishers never write to — every instance receiving nothing, quietly. So
/// the mistake is refused at registration rather than diagnosed in production.
pub(crate) fn reject_broadcast<T: Topic>(entry_point: &str) -> Result<()> {
    if T::topology().broadcast() {
        return Err(ShoveError::Topology(format!(
            "topic '{}' declares `.broadcast()`; `{entry_point}` would register a \
             competing consumer on a shared queue that broadcast publishes never reach. \
             Use `broker.broadcast_subscriber()` instead.",
            T::topology().queue()
        )));
    }
    Ok(())
}

/// Runs this process's own ephemeral subscriptions: every instance of the
/// service receives every message, rather than competing for one queue.
///
/// Obtained from
/// [`Broker::broadcast_subscriber`](crate::Broker::broadcast_subscriber), and
/// only for backends implementing
/// [`HasBroadcast`](crate::backend::capability::HasBroadcast).
///
/// ```rust,no_run
/// # #[cfg(feature = "inmemory")]
/// # mod example {
/// # use std::time::Duration;
/// # use serde::{Deserialize, Serialize};
/// # use shove::{Broker, ConsumerOptions, InMemory, MessageHandler, MessageMetadata, Outcome};
/// # use shove::inmemory::InMemoryConfig;
/// # use shove::topology::TopologyBuilder;
/// # #[derive(Debug, Clone, Serialize, Deserialize)]
/// # pub struct InvalidateKey { pub key: String }
/// shove::define_topic!(pub CacheInvalidations, InvalidateKey,
///     TopologyBuilder::new("cache-invalidations").broadcast().build()
/// );
/// # pub struct Evict;
/// # impl MessageHandler<CacheInvalidations> for Evict {
/// #     type Context = ();
/// #     async fn handle(&self, _m: InvalidateKey, _meta: MessageMetadata, _ctx: &()) -> Outcome {
/// #         Outcome::Ack
/// #     }
/// # }
/// # pub async fn run() -> shove::error::Result<()> {
/// let broker = Broker::<InMemory>::new(InMemoryConfig::default()).await?;
/// let mut subscriber = broker.broadcast_subscriber();
/// subscriber.subscribe::<CacheInvalidations, _>(Evict, ConsumerOptions::new())?;
/// subscriber
///     .run_until_timeout(async { /* shutdown signal */ }, Duration::from_secs(5))
///     .await;
/// # Ok(())
/// # }
/// # }
/// ```
///
/// # Why there is no autoscaling knob
///
/// A [`ConsumerGroup`](crate::ConsumerGroup) scales consumers because they
/// share one queue: more consumers means the same messages are processed
/// faster. A broadcast subscription is the opposite — a second consumer on it
/// would *split* the fan-out this instance is supposed to receive whole. So
/// there is exactly one delivery loop per [`subscribe`](Self::subscribe) call,
/// and no method here to change that; asking for autoscaling is a compile
/// error, not a silently ignored setting.
///
/// # Best-effort delivery
///
/// [`Outcome::Retry`](crate::Outcome::Retry) and
/// [`Outcome::Reject`](crate::Outcome::Reject) from a handler **discard the
/// message** with a warning: a broadcast topology cannot declare a DLQ or hold
/// queues (`build()` rejects both), so there is nowhere for a failed message to
/// go. [`Outcome::Defer`](crate::Outcome::Defer) redelivers within this
/// subscription only.
pub struct BroadcastSubscriber<B: HasBroadcast, Ctx: Clone + Send + Sync + 'static = ()> {
    broadcast: B::BroadcastImpl,
    ctx: Ctx,
    shutdown: CancellationToken,
    tasks: JoinSet<Result<()>>,
    /// Queue names already subscribed on this handle. A second subscription to
    /// the same topic in one process would split that topic's fan-out between
    /// two loops instead of duplicating it — the exact failure mode broadcast
    /// exists to avoid — so it is rejected rather than spawned.
    registered: HashSet<&'static str>,
}

impl<B: HasBroadcast> BroadcastSubscriber<B, ()> {
    pub(crate) fn new(client: &B::Client) -> Self {
        Self {
            broadcast: B::make_broadcast(client),
            ctx: (),
            shutdown: CancellationToken::new(),
            tasks: JoinSet::new(),
            registered: HashSet::new(),
        }
    }

    pub fn with_context<Ctx: Clone + Send + Sync + 'static>(
        self,
        ctx: Ctx,
    ) -> BroadcastSubscriber<B, Ctx> {
        BroadcastSubscriber {
            broadcast: self.broadcast,
            ctx,
            shutdown: self.shutdown,
            tasks: self.tasks,
            registered: self.registered,
        }
    }
}

impl<B: HasBroadcast, Ctx: Clone + Send + Sync + 'static> BroadcastSubscriber<B, Ctx> {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Subscribe this instance to `T` and spawn its delivery loop.
    ///
    /// The subscription is created by the spawned task, so messages published
    /// before it is established are not delivered — deliver-new is the
    /// contract, not a timing accident.
    /// [`ConsumerOptions::with_broadcast_start`](crate::ConsumerOptions::with_broadcast_start)
    /// opts a subscription into starting at the head or at a timestamp
    /// instead, on a backend that can honour it (see [`BroadcastStart`]).
    ///
    /// Returns an error if:
    /// - `T`'s topology does not declare
    ///   [`broadcast()`](crate::topology::TopologyBuilder::broadcast) — a
    ///   shared-queue topology run through here would be competing consumption
    ///   wearing a broadcast label.
    /// - `T` is already subscribed on this handle.
    /// - `options` sets something this backend's subscription cannot honour:
    ///   a [`BroadcastStart`] other than `Tail` on a backend that starts at
    ///   the tail only, or a Kafka knob the broadcast loop never reads. The
    ///   refusal is synchronous, here, and not a task error surfaced at
    ///   shutdown.
    ///
    /// [`ConsumerOptions::with_max_retries`](crate::ConsumerOptions::with_max_retries)
    /// is ignored: broadcast has no retry chain, so the retry budget is pinned
    /// to `0` and the first `Retry` discards through the same no-DLQ path a
    /// `Reject` takes.
    ///
    /// [`ConsumerOptions::with_prefetch_count`](crate::ConsumerOptions::with_prefetch_count)
    /// and
    /// [`ConsumerOptions::with_concurrent_processing`](crate::ConsumerOptions::with_concurrent_processing)
    /// are ignored too: a broadcast subscription is a single delivery loop by
    /// contract, so the effective prefetch is pinned to `1`. A second
    /// concurrent handler inside one subscription would reorder the fan-out
    /// this instance is supposed to observe whole — the same surprise
    /// [`reject_broadcast`] refuses from the outside — and NATS and Redis
    /// already assume the single loop structurally, so honouring the option
    /// here would make ordering backend-dependent.
    pub fn subscribe<T, H>(&mut self, handler: H, options: ConsumerOptions<B>) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = Ctx>,
    {
        let queue = T::topology().queue();
        if !T::topology().broadcast() {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}' is not a broadcast topology; \
                 `BroadcastSubscriber::subscribe` would consume it as one instance of a \
                 competing-consumer group, not as a fan-out. Add `.broadcast()` to its \
                 topology, or use `broker.consumer_group()` / `broker.consumer_supervisor()`."
            )));
        }
        if !self.registered.insert(queue) {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}' is already subscribed on this broadcast subscriber; \
                 a second subscription would split this instance's fan-out across two \
                 consumers instead of duplicating it"
            )));
        }

        let mut inner = options.with_shutdown(self.shutdown.clone()).into_inner();
        // The neutral check first: a start no backend can honour is refused
        // with the same error everywhere, before the backend names its own
        // reasons.
        if let Some(start) = inner.broadcast_start
            && let Err(e) = start.validate(queue)
        {
            self.registered.remove(queue);
            return Err(e);
        }
        // Each backend refuses, before anything is spawned, the options its
        // subscription cannot honour: the FIFO consumer already refuses a
        // commit interval it would never read, and a start position or a
        // group knob that changes nothing here must not be accepted silently
        // either. Checked before the `registered` insert is relied on, so a
        // refused subscribe leaves the handle free to retry with fixed options.
        if let Err(e) = B::BroadcastImpl::check_options(queue, &inner) {
            self.registered.remove(queue);
            return Err(e);
        }
        // An external topology is refused here too, on the backends whose
        // loop would republish into it, so a subscription that can never run
        // is refused at `subscribe()` and not in its task.
        if let Err(e) = B::BroadcastImpl::refuse_external(T::topology()) {
            self.registered.remove(queue);
            return Err(e);
        }
        let broadcast = self.broadcast.clone();
        let ctx = self.ctx.clone();
        // A broadcast topology has neither a DLQ nor hold queues, so a retry
        // budget above zero would mean re-enqueuing to this subscription until
        // it is spent before discarding — redelivery to one subscriber of a
        // fan-out, which is what the design rules out. Pinning it to zero makes
        // the first `Retry` land on the existing terminal (no-DLQ) arm instead
        // of introducing a second discard path.
        inner.max_retries = 0;
        // A broadcast subscription is a single delivery loop by contract:
        // NATS and Redis assume it structurally (inline delivery; neither
        // reads the prefetch), so any backend that does honour prefetch —
        // InMemory, Kafka, RabbitMQ — must be pinned here or ordering
        // becomes backend-dependent. Pinned after `into_inner()` so
        // `with_concurrent_processing(true)` cannot re-raise it either.
        inner.prefetch_count = 1;

        self.tasks
            .spawn(async move { broadcast.run_broadcast::<T, H>(handler, ctx, inner).await });
        Ok(())
    }

    /// Wait for `signal`, then cancel every subscription and give the delivery
    /// loops up to `drain_timeout` to tear down.
    ///
    /// Teardown is the point: each backend's subscription is removed before its
    /// loop resolves, so a clean drain leaves nothing broker-side behind. On
    /// timeout the surviving tasks are aborted and
    /// [`SupervisorOutcome::timed_out`] is set — backends release their
    /// subscription from a drop guard, so the abort path reaps too.
    pub async fn run_until_timeout<S>(
        mut self,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        tokio::select! {
            _ = signal => { self.shutdown.cancel(); }
            _ = self.shutdown.cancelled() => {}
        }

        let mut errors = 0usize;
        let mut panics = 0usize;

        let drain = {
            let tasks = &mut self.tasks;
            let errors = &mut errors;
            let panics = &mut panics;
            async move {
                while let Some(res) = tasks.join_next().await {
                    tally_join_result(res, errors, panics);
                }
            }
        };

        match tokio::time::timeout(drain_timeout, drain).await {
            Ok(()) => SupervisorOutcome {
                errors,
                panics,
                timed_out: false,
            },
            Err(_) => {
                tracing::warn!(
                    timeout_ms = drain_timeout.as_millis() as u64,
                    "broadcast subscriber drain timed out; aborting surviving subscriptions"
                );
                self.tasks.abort_all();
                while let Some(res) = self.tasks.join_next().await {
                    tally_join_result(res, &mut errors, &mut panics);
                }
                SupervisorOutcome {
                    errors,
                    panics,
                    timed_out: true,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The neutral check names the topic, the value and the unit, so the
    /// error reads the same on every backend.
    #[test]
    fn a_negative_timestamp_start_is_refused_with_the_unit_in_the_error() {
        let err = BroadcastStart::Timestamp(-1)
            .validate("q")
            .expect_err("a negative timestamp names no instant");
        let ShoveError::Topology(msg) = err else {
            panic!("expected ShoveError::Topology, got {err:?}");
        };
        assert!(msg.contains("topic 'q'"), "{msg}");
        assert!(msg.contains("Timestamp(-1)"), "{msg}");
        assert!(msg.contains("Unix epoch"), "{msg}");
    }

    /// Negative control: every other start passes, the epoch itself
    /// included, so the refusal is conditional on the sign alone.
    #[test]
    fn every_other_start_passes_the_neutral_check() {
        for start in [
            BroadcastStart::Tail,
            BroadcastStart::Head,
            BroadcastStart::Timestamp(0),
            BroadcastStart::Timestamp(1_700_000_000_000),
        ] {
            start
                .validate("q")
                .expect("a start with an instant passes the neutral check");
        }
    }
}
