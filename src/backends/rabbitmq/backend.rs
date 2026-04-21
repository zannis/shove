//! Backend / impl-trait registrations for the RabbitMQ backend.
//!
//! Binds the RabbitMq marker (`crate::markers::RabbitMq`) to the concrete
//! types in this module via `impl Backend` and `impl HasCoordinatedGroups`,
//! plus the six `pub(crate)` impl-trait bodies that carry the real work.
//!
//! See DESIGN_V2.md §4 and §4.1.
//!
//! The whole `crate::backends::rabbitmq` module is already gated on the
//! `rabbitmq` feature at the parent (`crate::backends`); no per-file cfg
//! is needed here.

use std::future::Future;
use std::time::Duration;

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::backend::{
    AutoscalerBackendImpl, Backend, ConsumerImpl, ConsumerOptionsInner, QueueStatsProviderImpl,
    RegistryImpl, TopologyImpl, capability::HasCoordinatedGroups, sealed,
};
use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::markers::RabbitMq;
use crate::topic::{SequencedTopic, Topic};

use super::autoscaler::RabbitMqAutoscalerBackend;
use super::client::{RabbitMqClient, RabbitMqConfig};
use super::consumer::RabbitMqConsumer;
use super::consumer_group::ConsumerGroupConfig;
use super::publisher::RabbitMqPublisher;
use super::registry::ConsumerGroupRegistry;
use super::topology::RabbitMqTopologyDeclarer;

// ---------------------------------------------------------------------------
// Topology wrapper — `RabbitMqTopologyDeclarer::new` takes a `Channel`,
// but `Backend::make_declarer` is synchronous. This wrapper stores the
// client and opens a fresh channel on each `declare` call.
// ---------------------------------------------------------------------------

pub struct LazyRabbitMqTopologyDeclarer {
    client: RabbitMqClient,
}

impl LazyRabbitMqTopologyDeclarer {
    fn new(client: RabbitMqClient) -> Self {
        Self { client }
    }
}

// ---------------------------------------------------------------------------
// Stats-provider placeholder — `RabbitMqAutoscalerBackend::new` requires
// `ManagementConfig` which is not available from `RabbitMqClient` alone.
// This no-op provider satisfies the `Backend` associated type; users who
// need real queue stats should construct a `ManagementClient` directly.
// ---------------------------------------------------------------------------

pub struct NoopQueueStatsProvider;

// ---------------------------------------------------------------------------
// Marker bindings
// ---------------------------------------------------------------------------

impl sealed::Sealed for RabbitMq {}

impl Backend for RabbitMq {
    type Config = RabbitMqConfig;
    type Client = RabbitMqClient;

    type PublisherImpl = RabbitMqPublisher;
    type ConsumerImpl = RabbitMqConsumer;
    type TopologyImpl = LazyRabbitMqTopologyDeclarer;
    type AutoscalerImpl = RabbitMqAutoscalerBackend<NoopQueueStatsProvider>;
    type QueueStatsImpl = NoopQueueStatsProvider;

    async fn connect(config: Self::Config) -> Result<Self::Client> {
        RabbitMqClient::connect(&config).await
    }

    async fn make_publisher(client: &Self::Client) -> Result<Self::PublisherImpl> {
        RabbitMqPublisher::new(client.clone()).await
    }

    fn make_consumer(client: &Self::Client) -> Self::ConsumerImpl {
        RabbitMqConsumer::new(client.clone())
    }

    fn make_declarer(client: &Self::Client) -> Self::TopologyImpl {
        LazyRabbitMqTopologyDeclarer::new(client.clone())
    }

    fn make_autoscaler(client: &Self::Client) -> Self::AutoscalerImpl {
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let registry = Arc::new(Mutex::new(ConsumerGroupRegistry::new(client.clone())));
        RabbitMqAutoscalerBackend::with_stats_provider(NoopQueueStatsProvider, registry)
    }

    fn make_stats_provider(_client: &Self::Client) -> Self::QueueStatsImpl {
        NoopQueueStatsProvider
    }

    async fn close(client: &Self::Client) {
        client.shutdown().await;
    }
}

impl HasCoordinatedGroups for RabbitMq {
    type ConsumerGroupConfig = ConsumerGroupConfig;
    type RegistryImpl = ConsumerGroupRegistry;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl {
        ConsumerGroupRegistry::new(client.clone())
    }
}

// ---------------------------------------------------------------------------
// ConsumerImpl — delegate through the existing `Consumer` trait (Context = ())
// ---------------------------------------------------------------------------

impl ConsumerImpl for RabbitMqConsumer {
    async fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        RabbitMqConsumer::run_with_inner::<T, H>(self, handler, ctx, options).await
    }

    async fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        RabbitMqConsumer::run_fifo_with_inner::<T, H>(self, handler, ctx, options).await
    }

    async fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        RabbitMqConsumer::run_dlq::<T, H>(self, handler, ctx).await
    }
}

// ---------------------------------------------------------------------------
// TopologyImpl — lazy channel creation on each `declare` call
// ---------------------------------------------------------------------------

impl TopologyImpl for LazyRabbitMqTopologyDeclarer {
    async fn declare<T: Topic>(&self) -> Result<()> {
        let channel = self.client.create_channel().await?;
        let declarer = RabbitMqTopologyDeclarer::new(channel);
        RabbitMqTopologyDeclarer::declare(&declarer, T::topology()).await
    }
}

// ---------------------------------------------------------------------------
// AutoscalerBackendImpl — trait has no methods in Phase 4
// ---------------------------------------------------------------------------

impl AutoscalerBackendImpl for RabbitMqAutoscalerBackend<NoopQueueStatsProvider> {}

// ---------------------------------------------------------------------------
// NoopQueueStatsProvider — implements the management.rs trait so it can
// parameterise `RabbitMqAutoscalerBackend<S>`, and also implements the
// `QueueStatsProviderImpl` internal trait for the `Backend` binding.
// ---------------------------------------------------------------------------

impl super::management::QueueStatsProvider for NoopQueueStatsProvider {
    async fn get_queue_stats(&self, _queue: &str) -> Result<super::management::QueueStats> {
        Err(ShoveError::Topology(
            "QueueStatsProvider requires ManagementConfig; \
             construct a ManagementClient directly for real queue stats"
                .into(),
        ))
    }
}

impl QueueStatsProviderImpl for NoopQueueStatsProvider {
    async fn snapshot(&self, _queue: &str) -> Result<AutoscaleMetrics> {
        Err(ShoveError::Topology(
            "QueueStatsProvider requires ManagementConfig; \
             construct a ManagementClient directly for real queue stats"
                .into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// RegistryImpl — thin forward over existing inherent `register`, plus
// the new `cancellation_token` and `run_until_timeout` methods.
// ---------------------------------------------------------------------------

impl RegistryImpl for ConsumerGroupRegistry {
    type GroupConfig = ConsumerGroupConfig;

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
        ConsumerGroupRegistry::register::<T, H>(self, config, factory, ctx).await
    }

    fn cancellation_token(&self) -> tokio_util::sync::CancellationToken {
        self.client_shutdown_token()
    }

    async fn run_until_timeout<S>(mut self, signal: S, drain_timeout: Duration) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        self.start_all();

        let broker_token = self.client_shutdown_token();
        let signal_handle = tokio::spawn(signal);
        tokio::select! {
            _ = broker_token.cancelled() => {}
            res = signal_handle => {
                let _ = res;
                broker_token.cancel();
            }
        }

        let drain = self.shutdown_all_with_tally();
        match tokio::time::timeout(drain_timeout, drain).await {
            Ok(tally) => SupervisorOutcome {
                errors: tally.errors,
                panics: tally.panics,
                timed_out: false,
            },
            Err(_) => SupervisorOutcome {
                errors: 0,
                panics: 0,
                timed_out: true,
            },
        }
    }
}
