//! Public `Broker<B>` hub.

use std::time::Duration;

use crate::backend::Backend;
use crate::backend::capability::{HasBatchConsumption, HasBroadcast, HasCoordinatedGroups};
use crate::batch_consumer::BatchConsumer;
use crate::broadcast::BroadcastSubscriber;
use crate::consumer_group::ConsumerGroup;
use crate::consumer_supervisor::ConsumerSupervisor;
use crate::error::Result;
use crate::publisher::Publisher;
use crate::queue_depth::QueueDepthSampler;
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

    /// Return a [`QueueDepthSampler`] that publishes backlog and broker-side
    /// in-flight depth as gauges, whether or not this service autoscales.
    ///
    /// Name the queues to watch, then drive it — nothing is emitted until you
    /// do:
    ///
    /// ```rust,no_run
    /// # use tokio_util::sync::CancellationToken;
    /// # async fn example<B: shove::Backend>(broker: &shove::Broker<B>, shutdown: CancellationToken) {
    /// tokio::spawn(broker.queue_depth_sampler().watch("orders").run(shutdown));
    /// # }
    /// ```
    ///
    /// Reads the same per-backend snapshot the autoscaler reads, so the two
    /// agree. See [`queue_depth`](crate::queue_depth) for how the resulting
    /// series relate to the `shove_autoscaler_*` gauges.
    pub fn queue_depth_sampler(&self) -> QueueDepthSampler<B> {
        QueueDepthSampler::new(self.queue_stats_provider())
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

impl<B: HasBroadcast> Broker<B> {
    /// Return a [`BroadcastSubscriber`] for ephemeral per-instance fan-out:
    /// every instance of the service receives every message, as its own
    /// short-lived subscriber.
    ///
    /// The counterpart to [`consumer_group`](Self::consumer_group), which
    /// splits a topic across instances. Only topologies declaring
    /// [`.broadcast()`](crate::topology::TopologyBuilder::broadcast) can be
    /// subscribed through it.
    ///
    /// Gated on [`HasBroadcast`], which not every backend implements — see that
    /// trait for the authoritative list. `Broker<Sqs>` is excluded permanently
    /// (SQS has no subscription shove can create and destroy per process
    /// without leaking a queue); the backends still awaiting an implementation
    /// are excluded by the same gate until it lands.
    pub fn broadcast_subscriber(&self) -> BroadcastSubscriber<B> {
        BroadcastSubscriber::new(&self.client)
    }
}

impl<B: HasBatchConsumption> Broker<B> {
    /// Return a [`BatchConsumer`] for handler amortisation: buffering up to
    /// N messages before invoking the handler once with the whole batch,
    /// instead of once per message.
    ///
    /// Gated on [`HasBatchConsumption`] — see that trait for the
    /// authoritative per-backend list and the caps that apply to each
    /// (SQS's is a hard 10 messages).
    pub fn batch_consumer(&self) -> BatchConsumer<B> {
        BatchConsumer::new(&self.client)
    }
}

#[cfg(feature = "kafka")]
use crate::backends::kafka::{
    KafkaConsumerGroupConfig, KafkaOffsetReset, KafkaOffsetResetReport, KafkaPublisher,
    KafkaPublisherConfig, reset_group_offsets, resolved_reset_group_id,
};
#[cfg(feature = "kafka")]
use crate::markers::Kafka;
#[cfg(feature = "kafka")]
use crate::topic::Topic;

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

    /// Re-anchor `T`'s consumer group at a new position without minting a new
    /// group ID.
    ///
    /// [`KafkaConsumerGroupConfig::with_auto_offset_reset`] only decides where
    /// a group starts when it has **no** usable committed offset. Once the
    /// group has committed, the only way to move it is to rewrite those
    /// offsets — otherwise the sole way to seek to the tail is to join under a
    /// throwaway group ID (`my-group-v2`, `my-group-20260812`, …), which
    /// strands the old group's offsets and its lag metrics forever.
    ///
    /// This is the library-side equivalent of
    /// `kafka-consumer-groups.sh --reset-offsets --execute`. The group ID is
    /// resolved from `config` and `T`'s topology exactly as
    /// [`ConsumerGroup::register`] would resolve it — a [`with_group_id`]
    /// override, otherwise the topology's
    /// [`for_consumer_group`](crate::TopologyBuilder::for_consumer_group)
    /// fan-out group, otherwise the `{queue}-consumer` default, plus the
    /// `-fifo` suffix for a sequenced topic — so the offsets rewritten here are
    /// the ones the consumers will actually read.
    ///
    /// # The group must be inactive
    ///
    /// Kafka only accepts an offset reset while the group has no live members.
    /// Call this **before** starting the group's consumers (or after stopping
    /// them); with consumers running it returns
    /// [`ShoveError::Validation`](crate::ShoveError::Validation) naming the
    /// active member count. The broker enforces the same rule independently,
    /// so a member that joins mid-reset fails the commit rather than silently
    /// losing it.
    ///
    /// A group does not become inactive the instant its consumers stop: the
    /// coordinator drops each member as its `LeaveGroup` lands. A reset issued
    /// immediately after
    /// [`run_until_timeout`](crate::ConsumerGroup::run_until_timeout) returns
    /// may therefore need a brief retry. Re-anchoring at process start —
    /// before the group is registered — avoids the race entirely and is the
    /// intended shape.
    ///
    /// # Example
    ///
    /// A tailing sink that re-anchors on demand, under a stable group ID:
    ///
    /// ```no_run
    /// # use shove::kafka::{Kafka, KafkaConsumerGroupConfig, KafkaOffsetReset};
    /// # use shove::{Broker, define_topic, TopologyBuilder};
    /// # define_topic!(Prices, String, TopologyBuilder::new("prices").build());
    /// # async fn run(broker: &Broker<Kafka>) -> Result<(), shove::ShoveError> {
    /// let config = KafkaConsumerGroupConfig::new(1..=4);
    ///
    /// if std::env::var("PRICES_SEEK_TO_TAIL").is_ok() {
    ///     let report = broker
    ///         .reset_consumer_group_offsets::<Prices>(&config, KafkaOffsetReset::Latest)
    ///         .await?;
    ///     tracing::warn!(?report, "re-anchored prices consumer group at the tail");
    /// }
    ///
    /// let mut group = broker.consumer_group();
    /// // group.register::<Prices, _>(config.into(), ...).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Not applicable to a broadcast topology
    ///
    /// A [`.broadcast()`](crate::topology::TopologyBuilder::broadcast) topic has
    /// no consumer group to re-anchor: its subscribers assign partitions
    /// manually and never commit, so there are no offsets and nothing reads the
    /// `{queue}-consumer` group this method would otherwise resolve. Rewriting
    /// it would move a group nothing has ever joined, so the call returns
    /// [`ShoveError::Validation`](crate::ShoveError::Validation) instead of
    /// succeeding at nothing on an ops-facing method.
    ///
    /// [`with_group_id`]: KafkaConsumerGroupConfig::with_group_id
    /// [`ConsumerGroup::register`]: crate::ConsumerGroup::register
    pub async fn reset_consumer_group_offsets<T: Topic>(
        &self,
        config: &KafkaConsumerGroupConfig,
        to: KafkaOffsetReset,
    ) -> Result<KafkaOffsetResetReport> {
        let topology = T::topology();
        if topology.broadcast() {
            return Err(crate::ShoveError::Validation(format!(
                "topic '{}' declares `.broadcast()`, so it has no consumer group to \
                 re-anchor: its subscribers assign partitions manually and never commit an \
                 offset. A broadcast subscription always starts at the tail; there is no \
                 stored position to reset.",
                topology.queue()
            )));
        }
        let group_id = resolved_reset_group_id(config, topology);
        reset_group_offsets(&self.client, topology.queue(), &group_id, to).await
    }
}
