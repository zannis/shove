//! Backend / impl-trait registrations for the Redis Streams backend.
//!
//! Binds the Redis marker (`crate::markers::Redis`) to the concrete types in
//! this module via `impl Backend` and `impl HasCoordinatedGroups`, plus the
//! impl-trait bodies that carry the real work.
//!
//! `ConsumerImpl` is already implemented directly in `consumer.rs`; it is NOT
//! re-implemented here.

use std::future::Future;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::backend::{
    AutoscalerBackendImpl, Backend, QueueStatsProviderImpl, RegistryImpl, TopologyImpl,
    capability::HasCoordinatedGroups, sealed,
};
use crate::consumer_supervisor::SupervisorOutcome;
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
    type AutoscalerImpl = RedisAutoscalerBackend;
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
        RedisAutoscalerBackend::new(client.clone())
    }

    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl {
        XlenStatsProvider::new(client.clone())
    }

    async fn close(_client: &Self::Client) {
        // Redis connections are closed when the last Arc<ClientInner> drops.
        // Nothing to do here.
    }
}

impl HasCoordinatedGroups for Redis {
    type ConsumerGroupConfig = RedisConsumerGroupConfig;
    type RegistryImpl = RedisConsumerGroupRegistry;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl {
        RedisConsumerGroupRegistry::new(client.clone())
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

    async fn run_until_timeout<S>(self, signal: S, drain_timeout: Duration) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        RedisConsumerGroupRegistry::run_until_timeout(self, signal, drain_timeout).await
    }
}
