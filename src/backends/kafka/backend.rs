//! Backend / impl-trait registrations for the Kafka backend.
//!
//! Binds the Kafka marker (`crate::markers::Kafka`) to the concrete
//! types in this module via `impl Backend` and `impl HasCoordinatedGroups`,
//! plus the six `pub(crate)` impl-trait bodies that carry the real work.
//!
//! See DESIGN_V2.md §4 and §4.1.
//!
//! The whole `crate::backends::kafka` module is already gated on the
//! `kafka` feature at the parent (`crate::backends`); no per-file cfg
//! is needed here.

use std::future::Future;
use std::time::Duration;

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::backend::{
    AutoscalerBackendImpl, Backend, ConsumerImpl, ConsumerOptionsInner, QueueStatsProviderImpl,
    RegistryImpl, TopologyImpl, capability::HasCoordinatedGroups, sealed,
};
use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::markers::Kafka;
use crate::topic::{SequencedTopic, Topic};

use super::autoscaler::{KafkaAutoscalerBackend, KafkaLagStatsProvider, KafkaQueueStatsProvider};
use super::client::{KafkaClient, KafkaConfig};
use super::consumer::KafkaConsumer;
use super::consumer_group::{KafkaConsumerGroupConfig, KafkaConsumerGroupRegistry};
use super::publisher::KafkaPublisher;
use super::topology::KafkaTopologyDeclarer;

// ---------------------------------------------------------------------------
// Marker bindings
// ---------------------------------------------------------------------------

impl sealed::Sealed for Kafka {}

impl Backend for Kafka {
    type Config = KafkaConfig;
    type Client = KafkaClient;

    type PublisherImpl = KafkaPublisher;
    type ConsumerImpl = KafkaConsumer;
    type TopologyImpl = KafkaTopologyDeclarer;
    // Bound to the default stats-provider parameterisation — autoscaler impls
    // built with a custom `KafkaQueueStatsProvider` go through a different
    // constructor and don't flow through this trait.
    type AutoscalerImpl = KafkaAutoscalerBackend<KafkaLagStatsProvider>;
    type QueueStatsImpl = KafkaLagStatsProvider;

    async fn connect(config: Self::Config) -> Result<Self::Client> {
        KafkaClient::connect(&config).await
    }

    async fn make_publisher(client: &Self::Client) -> Result<Self::PublisherImpl> {
        KafkaPublisher::new(client.clone()).await
    }

    fn make_consumer(client: &Self::Client) -> Self::ConsumerImpl {
        KafkaConsumer::new(client.clone())
    }

    fn make_declarer(client: &Self::Client) -> Self::TopologyImpl {
        KafkaTopologyDeclarer::new(client.clone())
    }

    fn make_autoscaler(client: &Self::Client) -> Self::AutoscalerImpl {
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let registry = Arc::new(Mutex::new(KafkaConsumerGroupRegistry::new(client.clone())));
        KafkaAutoscalerBackend::new(client.clone(), registry)
    }

    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl {
        KafkaLagStatsProvider::new(client.clone())
    }

    async fn close(client: &Self::Client) {
        client.shutdown().await;
    }
}

impl HasCoordinatedGroups for Kafka {
    type ConsumerGroupConfig = KafkaConsumerGroupConfig;
    type RegistryImpl = KafkaConsumerGroupRegistry;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl {
        KafkaConsumerGroupRegistry::new(client.clone())
    }
}

// ---------------------------------------------------------------------------
// ConsumerImpl — delegate through the existing `Consumer` trait (Context = ())
// ---------------------------------------------------------------------------

impl ConsumerImpl for KafkaConsumer {
    async fn run<T, H>(
        &self,
        handler: H,
        _ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>,
    {
        KafkaConsumer::run_with_inner::<T>(self, handler, options).await
    }

    async fn run_fifo<T, H>(
        &self,
        handler: H,
        _ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T, Context = ()>,
    {
        KafkaConsumer::run_fifo_with_inner::<T>(self, handler, options).await
    }

    async fn run_dlq<T, H>(&self, handler: H, _ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>,
    {
        KafkaConsumer::run_dlq::<T>(self, handler).await
    }
}

// ---------------------------------------------------------------------------
// TopologyImpl — delegate through the existing `TopologyDeclarer` impl
// ---------------------------------------------------------------------------

impl TopologyImpl for KafkaTopologyDeclarer {
    async fn declare<T: Topic>(&self) -> Result<()> {
        KafkaTopologyDeclarer::declare(self, T::topology()).await
    }
}

// ---------------------------------------------------------------------------
// AutoscalerBackendImpl — trait has no methods in Phase 4
// ---------------------------------------------------------------------------

impl AutoscalerBackendImpl for KafkaAutoscalerBackend<KafkaLagStatsProvider> {}

// ---------------------------------------------------------------------------
// QueueStatsProviderImpl — map the stats to AutoscaleMetrics
// ---------------------------------------------------------------------------

impl QueueStatsProviderImpl for KafkaLagStatsProvider {
    async fn snapshot(&self, queue: &str) -> Result<AutoscaleMetrics> {
        let stats = self.get_queue_stats(queue).await?;
        Ok(AutoscaleMetrics {
            backlog: Some(stats.messages_pending),
            inflight: Some(stats.messages_in_flight),
            throughput_per_sec: None,
            processing_latency: None,
        })
    }
}

// ---------------------------------------------------------------------------
// RegistryImpl — thin forward over existing inherent `register`, plus
// the new `cancellation_token` and `run_until_timeout` methods.
// ---------------------------------------------------------------------------

impl RegistryImpl for KafkaConsumerGroupRegistry {
    type GroupConfig = KafkaConsumerGroupConfig;

    async fn register<T, H>(
        &mut self,
        config: Self::GroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
        _ctx: H::Context,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>,
    {
        KafkaConsumerGroupRegistry::register::<T, H>(self, config, factory).await
    }

    fn cancellation_token(&self) -> tokio_util::sync::CancellationToken {
        // Kafka consumer group registry doesn't expose a single
        // `broker_shutdown_token()` like InMemory. The canonical shutdown
        // token lives on the `KafkaClient` that was used to construct the
        // registry. Extract it from one of the registered groups, or fall
        // back to the client stored on the registry.
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
