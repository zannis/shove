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
use crate::backend::{
    AutoscalerBackendImpl, Backend, QueueStatsProviderImpl, RegistryImpl, TopologyImpl,
    capability::HasCoordinatedGroups, sealed,
};
use crate::consumer_supervisor::{ShutdownTally, SupervisorOutcome};
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::markers::Redis;
use crate::topic::{SequencedTopic, Topic};

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

impl HasCoordinatedGroups for Redis {
    type ConsumerGroupConfig = RedisConsumerGroupConfig;
    type RegistryImpl = RedisConsumerGroupRegistry;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl {
        RedisConsumerGroupRegistry::new(client.clone())
    }

    fn spawn_autoscaler(
        _client: &Self::Client,
        _registry: Arc<Mutex<Self::RegistryImpl>>,
        _config: AutoscalerConfig,
        _shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        todo!("implemented in task-5")
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

    async fn run_until_timeout<S>(self, signal: S, drain_timeout: Duration) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        RedisConsumerGroupRegistry::run_until_timeout(self, signal, drain_timeout).await
    }
}
