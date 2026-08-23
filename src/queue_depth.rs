//! Queue-depth gauges that do not depend on the autoscaler.
//!
//! Backlog and broker-side in-flight depth are the two numbers every operator
//! wants, and until now `shove` only published them as a side effect of
//! [`Autoscaler::poll_and_scale`]: turn autoscaling off — a fixed consumer
//! pool, a Deployment scaled by something else — and the `shove_autoscaler_*`
//! gauges go silent along with it. Services then rebuild the same poller
//! themselves on top of [`Broker::queue_stats_provider`], once per service.
//!
//! [`QueueDepthSampler`] is that poller, owned by the library: it reads the
//! same per-backend snapshot the autoscaler reads and publishes it as
//! `shove_queue_backlog` / `shove_queue_inflight`, keyed by `topic` so the
//! series join against the rest of the consumer metrics.
//!
//! [`Autoscaler::poll_and_scale`]: crate::autoscaler::Autoscaler::poll_and_scale
//! [`Broker::queue_stats_provider`]: crate::broker::Broker::queue_stats_provider

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::backend::{Backend, QueueStatsProviderImpl};
use crate::metrics;
use crate::topic::Topic;

/// Default gap between polls. Matches [`AutoscalerConfig::poll_interval`], so
/// running both produces gauges that move at the same rate.
///
/// [`AutoscalerConfig::poll_interval`]: crate::autoscaler::AutoscalerConfig::poll_interval
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Polls the broker for queue depth and publishes it as gauges.
///
/// Build one with [`Broker::queue_depth_sampler`], name the queues to watch,
/// and drive it with [`run`](Self::run):
///
/// ```rust,no_run
/// # use std::time::Duration;
/// # use tokio_util::sync::CancellationToken;
/// # async fn example<B: shove::Backend>(broker: &shove::Broker<B>, shutdown: CancellationToken) {
/// let sampler = broker
///     .queue_depth_sampler()
///     .watch("orders")
///     .with_poll_interval(Duration::from_secs(15));
/// tokio::spawn(sampler.run(shutdown));
/// # }
/// ```
///
/// # Scope
///
/// - **Nothing is emitted until you run it.** `shove` does not open sockets or
///   spawn tasks a caller did not ask for, and every poll is a real broker
///   round trip (RabbitMQ Management HTTP, SQS `GetQueueAttributes`, Kafka
///   watermark fetches). The gauges are unconditional with respect to
///   *autoscaling*, not with respect to the caller.
/// - **The watch set is explicit.** No backend can enumerate "the queues this
///   service cares about" — SQS sees every queue in the account, Redis every
///   stream in the keyspace — so guessing would either miss queues or bill a
///   caller for polling queues they never declared. Name them, or use
///   [`watch_topic`](Self::watch_topic) to take the name off the topology.
/// - **A failed poll leaves the previous value in place.** See
///   [`sample_once`](Self::sample_once).
///
/// [`Broker::queue_depth_sampler`]: crate::broker::Broker::queue_depth_sampler
pub struct QueueDepthSampler<B: Backend> {
    provider: B::QueueStatsImpl,
    queues: Vec<String>,
    poll_interval: Duration,
}

impl<B: Backend> QueueDepthSampler<B> {
    pub(crate) fn new(provider: B::QueueStatsImpl) -> Self {
        Self {
            provider,
            queues: Vec::new(),
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Add a queue to the watch set, by name.
    ///
    /// Duplicates are ignored: the same queue named twice would issue two
    /// round trips per poll to `set` one gauge twice.
    #[must_use]
    pub fn watch(mut self, queue: impl Into<String>) -> Self {
        let queue = queue.into();
        if !self.queues.contains(&queue) {
            self.queues.push(queue);
        }
        self
    }

    /// Add a queue to the watch set, taking its name from `T`'s topology.
    ///
    /// Prefer this over [`watch`](Self::watch): it cannot drift from the name
    /// the publisher and consumer actually use, so the `topic` label matches
    /// the one on `shove_messages_consumed_total` by construction.
    #[must_use]
    pub fn watch_topic<T: Topic>(self) -> Self {
        self.watch(T::topology().queue())
    }

    /// Override the gap between polls. Default: [`DEFAULT_POLL_INTERVAL`].
    #[must_use]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// The queues this sampler polls, in the order they were added.
    #[must_use]
    pub fn queues(&self) -> &[String] {
        &self.queues
    }

    /// Poll every watched queue once and publish the gauges.
    ///
    /// A queue whose snapshot fails is logged and skipped, leaving its gauge
    /// at the last successfully-read value. The alternative — clearing it, or
    /// setting it to zero — would turn a broker outage into a "backlog
    /// drained to nothing" reading at exactly the moment an operator is
    /// looking at the dashboard to find out what broke. A stale gauge is
    /// visibly stale next to `shove_backend_errors_total`; a fabricated zero
    /// is not visibly anything.
    ///
    /// One failing queue does not stop the others.
    pub async fn sample_once(&self) {
        for queue in &self.queues {
            match self.provider.snapshot(queue).await {
                Ok(sample) => metrics::record_queue_depth(queue, &sample),
                Err(e) => tracing::warn!(
                    queue = %queue,
                    error = %e,
                    "queue-depth snapshot failed; leaving the gauges at their last value",
                ),
            }
        }
    }

    /// Poll on `poll_interval` until `shutdown` is cancelled.
    ///
    /// Cancellation is checked before each sleep and races the poll itself, so
    /// a shutdown that arrives mid-poll drops the in-flight request rather
    /// than waiting out a broker that has stopped answering.
    ///
    /// Returns without emitting anything if the watch set is empty — that is a
    /// misconfiguration, so it is logged rather than spun on.
    pub async fn run(self, shutdown: CancellationToken) {
        if self.queues.is_empty() {
            tracing::warn!(
                "queue-depth sampler started with no queues to watch; \
                 call `watch`/`watch_topic` before `run`"
            );
            return;
        }

        tracing::info!(
            queues = ?self.queues,
            poll_interval_secs = self.poll_interval.as_secs_f64(),
            "queue-depth sampler started",
        );

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    tracing::info!("queue-depth sampler shutting down");
                    return;
                }
                () = async {
                    tokio::time::sleep(self.poll_interval).await;
                    self.sample_once().await;
                } => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "inmemory")]
    use crate::inmemory::InMemoryBroker;
    #[cfg(feature = "inmemory")]
    use crate::markers::InMemory;
    use crate::topology::{QueueTopology, TopologyBuilder};

    struct Orders;
    impl Topic for Orders {
        type Message = ();
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("orders").build())
        }
    }

    /// The builder is generic over `B` but none of these assertions need a
    /// live broker, so borrow the in-memory backend's provider type.
    #[cfg(feature = "inmemory")]
    fn sampler() -> QueueDepthSampler<InMemory> {
        let client = InMemoryBroker::new();
        QueueDepthSampler::new(<InMemory as Backend>::make_stats_provider(&client))
    }

    #[cfg(feature = "inmemory")]
    #[test]
    fn watch_dedupes_and_preserves_order() {
        let s = sampler().watch("b").watch("a").watch("b");
        assert_eq!(s.queues(), ["b", "a"]);
    }

    #[cfg(feature = "inmemory")]
    #[test]
    fn watch_topic_takes_the_name_from_the_topology() {
        let s = sampler().watch_topic::<Orders>();
        assert_eq!(s.queues(), ["orders"]);
        // …and dedupes against the same name added by hand, so a caller
        // mixing both forms does not double-poll.
        assert_eq!(s.watch("orders").queues(), ["orders"]);
    }

    #[cfg(feature = "inmemory")]
    #[test]
    fn poll_interval_defaults_and_overrides() {
        assert_eq!(sampler().poll_interval, DEFAULT_POLL_INTERVAL);
        let s = sampler().with_poll_interval(Duration::from_millis(250));
        assert_eq!(s.poll_interval, Duration::from_millis(250));
    }

    /// An empty watch set must return, not spin: a sampler that never had a
    /// `watch` call is a misconfiguration, and a loop that wakes every 5 s to
    /// do nothing hides it.
    #[cfg(feature = "inmemory")]
    #[tokio::test]
    async fn run_returns_immediately_with_no_queues() {
        let token = CancellationToken::new();
        // No timeout needed: if this ever starts looping, the test hangs and
        // the harness reports it — which is the failure we want to see.
        sampler().run(token).await;
    }

    /// A queue that does not exist makes `snapshot` fail. The sampler must
    /// swallow it and keep going rather than propagate or panic.
    #[cfg(feature = "inmemory")]
    #[tokio::test]
    async fn sample_once_survives_a_failing_snapshot() {
        sampler().watch("no-such-queue").sample_once().await;
    }
}
