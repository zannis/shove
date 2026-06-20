//! Public `Broker<B>` hub.

use std::time::Duration;

use crate::backend::Backend;
use crate::backend::capability::HasCoordinatedGroups;
use crate::consumer_group::ConsumerGroup;
use crate::consumer_supervisor::ConsumerSupervisor;
use crate::error::Result;
use crate::publisher::Publisher;
use crate::topology_declarer::TopologyDeclarer;

/// Default deadline for `Broker::ping`. Matches the rest of shove's 5 s
/// timeout constants (kafka `PRODUCE_TIMEOUT`, autoscaler metadata fetch,
/// `MESSAGE_TIMEOUT_MS`). Override via [`Broker::ping_with_timeout`].
pub const DEFAULT_PING_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Broker<B: Backend> {
    client: B::Client,
}

impl<B: Backend> Broker<B> {
    pub async fn new(config: B::Config) -> Result<Self> {
        Ok(Self {
            client: B::connect(config).await?,
        })
    }

    pub fn from_client(client: B::Client) -> Self {
        Self { client }
    }

    pub async fn publisher(&self) -> Result<Publisher<B>> {
        Ok(Publisher::new(B::make_publisher(&self.client).await?))
    }

    /// Return a [`ConsumerSupervisor`] for spawning fixed-concurrency consumers.
    ///
    /// # SQS autoscaling note
    ///
    /// For the SQS backend, consumers started through this supervisor are **not**
    /// registered in `SqsConsumerGroupRegistry`. Pairing this path with
    /// [`autoscaler`](Self::autoscaler) will produce an autoscaler that always
    /// observes zero groups. Use [`SqsConsumerGroupRegistry`] directly when
    /// autoscaling is required.
    ///
    /// [`SqsConsumerGroupRegistry`]: crate::backends::sns::registry::SqsConsumerGroupRegistry
    pub fn consumer_supervisor(&self) -> ConsumerSupervisor<B> {
        ConsumerSupervisor::new(&self.client)
    }

    pub fn topology(&self) -> TopologyDeclarer<B> {
        TopologyDeclarer::new(B::make_declarer(&self.client))
    }

    pub async fn close(&self) {
        B::close(&self.client).await
    }

    /// Verify the broker is reachable. Issues a single bounded RPC against
    /// the cluster and returns `Ok(())` iff it completes within
    /// [`DEFAULT_PING_TIMEOUT`].
    ///
    /// Designed for liveness / readiness probes:
    ///
    /// - **No retries** — a failed probe is returned to the caller as-is.
    ///   Probe policy (retry counts, failure thresholds) belongs to the caller
    ///   (k8s `failureThreshold`, an HTTP middleware, etc.).
    /// - **No metrics emitted** — probes are called frequently; recording a
    ///   metric per call would drown out failure signal.
    /// - **Post-`close()` semantics are backend-specific.** Backends with a
    ///   meaningful close (Kafka, RabbitMQ, NATS, SQS, InMemory) check a
    ///   shutdown token and return `Err(ShoveError::Connection)` before any
    ///   I/O. The Redis backend's `close` is a no-op — its connections drop
    ///   on last `Arc` release — so ping continues to function until the
    ///   broker itself becomes unreachable.
    /// - **Backends may transparently recover stale internal state.**
    ///   For example, the RabbitMQ backend dials a fresh AMQP connection if
    ///   the cached one died, librdkafka maintains its own broker connection
    ///   pool, and async-nats heartbeats keep the underlying connection
    ///   healthy. A probe that succeeds after such recovery is reported as
    ///   `Ok(())` — the broker is reachable now, which is what liveness asks.
    pub async fn ping(&self) -> Result<()> {
        self.ping_with_timeout(DEFAULT_PING_TIMEOUT).await
    }

    /// Same as [`ping`](Self::ping), with a caller-supplied deadline. Exceeding
    /// `timeout` returns `Err(ShoveError::Connection)`.
    pub async fn ping_with_timeout(&self, timeout: Duration) -> Result<()> {
        B::ping(&self.client, timeout).await
    }

    /// Return a [`QueueStatsImpl`](crate::backend::Backend::QueueStatsImpl) for
    /// reading queue depth from the underlying broker.
    pub fn queue_stats_provider(&self) -> B::QueueStatsImpl {
        B::make_stats_provider(&self.client)
    }

    /// Return a [`Backend::AutoscalerImpl`](crate::backend::Backend::AutoscalerImpl)
    /// for driving generic autoscaling through the
    /// [`AutoscalerBackend`](crate::autoscaler::AutoscalerBackend) interface.
    ///
    /// The returned value implements [`AutoscalerBackend`](crate::autoscaler::AutoscalerBackend)
    /// and can be passed directly to
    /// [`Autoscaler::new`](crate::autoscaler::Autoscaler::new).
    ///
    /// # SQS autoscaling note
    ///
    /// For the SQS backend, the returned autoscaler queries a
    /// `SqsConsumerGroupRegistry`. Groups must be registered through
    /// [`SqsConsumerGroupRegistry::register`] — consumers spawned via
    /// [`consumer_supervisor`](Self::consumer_supervisor) are **not** visible
    /// to the autoscaler and it will always observe zero groups.
    ///
    /// [`SqsConsumerGroupRegistry::register`]: crate::backends::sns::registry::SqsConsumerGroupRegistry::register
    pub fn autoscaler(&self) -> B::AutoscalerImpl {
        B::make_autoscaler(&self.client)
    }
}

impl<B: HasCoordinatedGroups> Broker<B> {
    pub fn consumer_group(&self) -> ConsumerGroup<B> {
        ConsumerGroup::new(B::make_registry(&self.client), self.client.clone())
    }
}

#[cfg(feature = "kafka")]
use crate::backends::kafka::{KafkaPublisher, KafkaPublisherConfig};
#[cfg(feature = "kafka")]
use crate::markers::Kafka;

#[cfg(feature = "kafka")]
impl Broker<Kafka> {
    /// Build a [`Publisher`] from a [`KafkaPublisherConfig`].
    ///
    /// The Kafka-specific counterpart to [`publisher`](Self::publisher): the
    /// config carries optional producer-side Schema Registry settings used to
    /// Confluent-frame outgoing payloads, symmetric with how
    /// [`KafkaConsumerGroupConfig`] carries SR settings on the consume side.
    /// [`publisher`](Self::publisher) remains framing-free.
    ///
    /// [`KafkaConsumerGroupConfig`]: crate::backends::kafka::KafkaConsumerGroupConfig
    pub async fn publisher_with(&self, config: KafkaPublisherConfig) -> Result<Publisher<Kafka>> {
        let inner = KafkaPublisher::with_config(self.client.clone(), config).await?;
        Ok(Publisher::new(inner))
    }
}
