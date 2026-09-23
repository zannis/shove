//! Backend / impl-trait registrations for the Redis Streams backend.
//!
//! Binds the Redis marker (`crate::markers::Redis`) to the concrete types in
//! this module via `impl Backend` and `impl HasCoordinatedGroups`, plus the
//! impl-trait bodies that carry the real work.
//!
//! `ConsumerImpl` is already implemented directly in `consumer.rs`; it is NOT
//! re-implemented here.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::autoscaler::AutoscalerConfig;
use crate::backend::broadcast::refuse_start_other_than_tail;
use crate::backend::{
    AutoscalerBackendImpl, Backend, BatchConsumerImpl, BatchConsumerOptionsInner, BroadcastImpl,
    ConsumerOptionsInner, QueueStatsProviderImpl, RegistryImpl, TopologyImpl,
    capability::{HasBatchConsumption, HasBroadcast, HasCoordinatedGroups},
    sealed,
};
use crate::consumer_supervisor::{ShutdownTally, SupervisorOutcome};
use crate::error::Result;
use crate::handler::{BatchMessageHandler, MessageHandler};
use crate::markers::Redis;
use crate::topic::{NotSequenced, SequencedTopic, Topic};
use crate::topology::QueueTopology;

use super::autoscaler::{RedisAutoscalerBackend, XlenStatsProvider};
use super::client::{RedisClient, RedisConfig};
use super::consumer::RedisConsumer;
use super::consumer_group::{RedisConsumerGroupConfig, RedisConsumerGroupRegistry};
use super::publisher::RedisPublisher;
use super::topology::RedisTopologyDeclarer;

// ---------------------------------------------------------------------------
// Marker bindings
// ---------------------------------------------------------------------------

impl sealed::Sealed for Redis {}

impl Backend for Redis {
    type Config = RedisConfig;
    type Client = RedisClient;

    type PublisherImpl = RedisPublisher;
    type ConsumerImpl = RedisConsumer;
    type TopologyImpl = RedisTopologyDeclarer;
    type AutoscalerImpl = RedisAutoscalerBackend<XlenStatsProvider>;
    type QueueStatsImpl = XlenStatsProvider;

    async fn connect(config: Self::Config) -> Result<Self::Client> {
        RedisClient::connect(config).await
    }

    async fn make_publisher(client: &Self::Client) -> Result<Self::PublisherImpl> {
        Ok(RedisPublisher::new(client.clone()))
    }

    fn make_consumer(client: &Self::Client) -> Self::ConsumerImpl {
        RedisConsumer::new(client.clone())
    }

    fn make_declarer(client: &Self::Client) -> Self::TopologyImpl {
        RedisTopologyDeclarer::new(client.clone())
    }

    fn make_autoscaler(client: &Self::Client) -> Self::AutoscalerImpl {
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let registry = Arc::new(Mutex::new(RedisConsumerGroupRegistry::new(client.clone())));
        RedisAutoscalerBackend::new(client.clone(), registry)
    }

    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl {
        XlenStatsProvider::new(client.clone())
    }

    async fn close(_client: &Self::Client) {
        // Redis connections are closed when the last Arc<ClientInner> drops.
        // Nothing to do here.
    }

    async fn ping(client: &Self::Client, timeout: std::time::Duration) -> Result<()> {
        client.ping(timeout).await
    }
}

impl HasBroadcast for Redis {
    // Same type as the competing-consumer path: a broadcast subscription is
    // another delivery loop over the same stream, differing in the command it
    // issues (`XREAD` from `$` rather than `XREADGROUP`) rather than in what it
    // needs from the client.
    type BroadcastImpl = RedisConsumer;

    fn make_broadcast(client: &Self::Client) -> Self::BroadcastImpl {
        RedisConsumer::new(client.clone())
    }
}

impl BroadcastImpl for RedisConsumer {
    async fn run_broadcast<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        RedisConsumer::run_broadcast_with_inner::<T, H>(self, handler, ctx, options).await
    }

    fn check_options(queue: &str, options: &ConsumerOptionsInner) -> Result<()> {
        refuse_start_other_than_tail(
            "Redis Streams",
            queue,
            options,
            "the subscription reads from `$` on this version",
        )
    }

    fn refuse_external(topology: &QueueTopology) -> Result<()> {
        super::consumer::refuse_external(topology)
    }
}

impl HasBatchConsumption for Redis {
    // Same consumer type as every other Redis path — the batch loop needs no
    // state the group/broadcast loops don't already carry.
    type BatchConsumerImpl = RedisConsumer;

    fn make_batch_consumer(client: &Self::Client) -> Self::BatchConsumerImpl {
        RedisConsumer::new(client.clone())
    }
}

impl BatchConsumerImpl for RedisConsumer {
    async fn run_batch<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: BatchConsumerOptionsInner,
    ) -> Result<()>
    where
        T: NotSequenced,
        H: BatchMessageHandler<T>,
    {
        RedisConsumer::run_batch_with_inner::<T, H>(self, handler, ctx, options).await
    }
}

impl HasCoordinatedGroups for Redis {
    type ConsumerGroupConfig = RedisConsumerGroupConfig;
    type RegistryImpl = RedisConsumerGroupRegistry;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl {
        RedisConsumerGroupRegistry::new(client.clone())
    }

    fn spawn_autoscaler(
        client: &Self::Client,
        registry: Arc<Mutex<Self::RegistryImpl>>,
        config: AutoscalerConfig,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let mut autoscaler = RedisAutoscalerBackend::autoscaler(client.clone(), registry, config);
        tokio::spawn(async move { autoscaler.run(shutdown).await })
    }
}

// ---------------------------------------------------------------------------
// TopologyImpl — delegate to the inherent `declare` method
// ---------------------------------------------------------------------------

impl TopologyImpl for RedisTopologyDeclarer {
    async fn declare<T: Topic>(&self) -> Result<()> {
        RedisTopologyDeclarer::declare(self, T::topology()).await
    }
}

// ---------------------------------------------------------------------------
// RegistryImpl
// ---------------------------------------------------------------------------

impl RegistryImpl for RedisConsumerGroupRegistry {
    type GroupConfig = RedisConsumerGroupConfig;

    async fn register<T, H>(
        &mut self,
        config: Self::GroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        RedisConsumerGroupRegistry::register::<T, H>(self, config, factory, ctx).await
    }

    async fn register_fifo<T, H>(
        &mut self,
        config: Self::GroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        RedisConsumerGroupRegistry::register_fifo::<T, H>(self, config, factory, ctx).await
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.broker_shutdown_token()
    }

    fn set_default_handler_timeout(&mut self, timeout: std::time::Duration) {
        self.default_handler_timeout = Some(timeout);
    }

    fn start_all(&mut self) {
        RedisConsumerGroupRegistry::start_all(self);
    }

    async fn drain_until_timeout(mut self, drain_timeout: Duration) -> SupervisorOutcome {
        let mut tally = ShutdownTally::default();
        match tokio::time::timeout(drain_timeout, self.drain_all_into(&mut tally)).await {
            Ok(()) => SupervisorOutcome {
                errors: tally.errors,
                panics: tally.panics,
                timed_out: false,
            },
            Err(_) => {
                tracing::warn!(
                    timeout_ms = drain_timeout.as_millis() as u64,
                    "drain timeout elapsed; aborting surviving consumer tasks"
                );
                self.abort_all_remaining_into(&mut tally).await;
                SupervisorOutcome {
                    errors: tally.errors,
                    panics: tally.panics,
                    timed_out: true,
                }
            }
        }
    }

    async fn run_until_timeout<S>(mut self, signal: S, drain_timeout: Duration) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        self.start_all();

        let shutdown = self.broker_shutdown_token();
        let signal_handle = tokio::spawn(signal);
        tokio::select! {
            _ = shutdown.cancelled() => {}
            res = signal_handle => {
                let _ = res;
                shutdown.cancel();
            }
        }

        self.drain_until_timeout(drain_timeout).await
    }
}

#[cfg(test)]
mod broadcast_start_guard_tests {
    use super::*;
    use crate::backend::{BroadcastImpl, ConsumerOptionsInner};
    use crate::broadcast::BroadcastStart;
    use crate::error::ShoveError;
    use tokio_util::sync::CancellationToken;

    fn options(start: Option<BroadcastStart>) -> ConsumerOptionsInner {
        let mut options = ConsumerOptionsInner::defaults_with_shutdown(CancellationToken::new());
        options.broadcast_start = start;
        options
    }

    /// The subscription starts at the tail only on this version, so
    /// `subscribe()` refuses `Head` and `Timestamp` with this backend's name
    /// and its reason, and admits an unset or `Tail` start.
    #[test]
    fn broadcast_subscribe_refuses_head_and_timestamp() {
        for start in [None, Some(BroadcastStart::Tail)] {
            <RedisConsumer as BroadcastImpl>::check_options("cache-invalidations", &options(start))
                .expect("the tail changes nothing and passes");
        }
        for start in [
            BroadcastStart::Head,
            BroadcastStart::Timestamp(1_700_000_000_000),
        ] {
            let err = <RedisConsumer as BroadcastImpl>::check_options(
                "cache-invalidations",
                &options(Some(start)),
            )
            .expect_err("a start this backend cannot honour is refused at subscribe()");
            let ShoveError::Topology(msg) = err else {
                panic!("expected ShoveError::Topology, got {err:?}");
            };
            assert!(msg.contains("cache-invalidations"), "{msg}");
            assert!(
                msg.contains(&format!("with_broadcast_start({start:?})")),
                "{msg}"
            );
            assert!(msg.contains("Redis Streams"), "{msg}");
            assert!(msg.contains("reads from `$`"), "{msg}");
            assert!(msg.contains("BroadcastStart::Tail"), "{msg}");
        }
    }
}
