//! Backend / impl-trait registrations for the NATS backend.
//!
//! Binds the Nats marker (`crate::markers::Nats`) to the concrete
//! types in this module via `impl Backend` and `impl HasCoordinatedGroups`,
//! plus the six `pub(crate)` impl-trait bodies that carry the real work.
//!
//! See DESIGN_V2.md §4 and §4.1.
//!
//! The whole `crate::backends::nats` module is already gated on the
//! `nats` feature at the parent (`crate::backends`); no per-file cfg
//! is needed here.

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::backend::{
    AutoscalerBackendImpl, Backend, ConsumerImpl, ConsumerOptionsInner, QueueStatsProviderImpl,
    RegistryImpl, TopologyImpl, capability::HasCoordinatedGroups, sealed,
};
use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::markers::Nats;
use crate::topic::{SequencedTopic, Topic};
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::autoscaler::{JetStreamStatsProvider, NatsAutoscalerBackend, NatsQueueStatsProvider};
use super::client::{NatsClient, NatsConfig};
use super::consumer::NatsConsumer;
use super::consumer_group::{NatsConsumerGroupConfig, NatsConsumerGroupRegistry};
use super::publisher::NatsPublisher;
use super::topology::NatsTopologyDeclarer;

// ---------------------------------------------------------------------------
// Marker bindings
// ---------------------------------------------------------------------------

impl sealed::Sealed for Nats {}

impl Backend for Nats {
    type Config = NatsConfig;
    type Client = NatsClient;

    type PublisherImpl = NatsPublisher;
    type ConsumerImpl = NatsConsumer;
    type TopologyImpl = NatsTopologyDeclarer;
    type AutoscalerImpl = NatsAutoscalerBackend<JetStreamStatsProvider>;
    type QueueStatsImpl = JetStreamStatsProvider;

    async fn connect(config: Self::Config) -> Result<Self::Client> {
        NatsClient::connect(&config).await
    }

    async fn make_publisher(client: &Self::Client) -> Result<Self::PublisherImpl> {
        NatsPublisher::new(client.clone()).await
    }

    fn make_consumer(client: &Self::Client) -> Self::ConsumerImpl {
        NatsConsumer::new(client.clone())
    }

    fn make_declarer(client: &Self::Client) -> Self::TopologyImpl {
        NatsTopologyDeclarer::new(client.clone())
    }

    fn make_autoscaler(client: &Self::Client) -> Self::AutoscalerImpl {
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let registry = Arc::new(Mutex::new(NatsConsumerGroupRegistry::new(client.clone())));
        NatsAutoscalerBackend::new(client.clone(), registry)
    }

    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl {
        JetStreamStatsProvider::new(client.clone())
    }

    async fn close(client: &Self::Client) {
        client.shutdown().await;
    }
}

impl HasCoordinatedGroups for Nats {
    type ConsumerGroupConfig = NatsConsumerGroupConfig;
    type RegistryImpl = NatsConsumerGroupRegistry;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl {
        NatsConsumerGroupRegistry::new(client.clone())
    }
}

// ---------------------------------------------------------------------------
// ConsumerImpl — delegate through the existing `Consumer` trait (Context = ())
// ---------------------------------------------------------------------------

impl ConsumerImpl for NatsConsumer {
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
        NatsConsumer::run_with_inner::<T, H>(self, handler, ctx, options).await
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
        NatsConsumer::run_fifo_with_inner::<T, H>(self, handler, ctx, options).await
    }

    async fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        NatsConsumer::run_dlq::<T, H>(self, handler, ctx).await
    }

    async fn spawn_fifo_shards<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        NatsConsumer::spawn_fifo_shards::<T, H>(self, handler, ctx, options).await
    }
}

// ---------------------------------------------------------------------------
// TopologyImpl — delegate through the existing `TopologyDeclarer` impl
// ---------------------------------------------------------------------------

impl TopologyImpl for NatsTopologyDeclarer {
    async fn declare<T: Topic>(&self) -> Result<()> {
        NatsTopologyDeclarer::declare(self, T::topology()).await
    }
}

// ---------------------------------------------------------------------------
// AutoscalerBackendImpl — trait has no methods in Phase 4
// ---------------------------------------------------------------------------

impl AutoscalerBackendImpl for NatsAutoscalerBackend<JetStreamStatsProvider> {}

// ---------------------------------------------------------------------------
// QueueStatsProviderImpl — map the JetStream stats to AutoscaleMetrics
// ---------------------------------------------------------------------------

impl QueueStatsProviderImpl for JetStreamStatsProvider {
    async fn snapshot(&self, queue: &str) -> Result<AutoscaleMetrics> {
        let stats = self.get_queue_stats(queue).await?;
        Ok(AutoscaleMetrics {
            backlog: Some(stats.messages_pending),
            inflight: Some(stats.messages_ack_pending),
            throughput_per_sec: None,
            processing_latency: None,
        })
    }
}

// ---------------------------------------------------------------------------
// RegistryImpl — thin forward over existing inherent `register`, plus
// the new `cancellation_token` and `run_until_timeout` methods.
// ---------------------------------------------------------------------------

impl RegistryImpl for NatsConsumerGroupRegistry {
    type GroupConfig = NatsConsumerGroupConfig;

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
        NatsConsumerGroupRegistry::register::<T, H>(self, config, factory, ctx).await
    }

    fn cancellation_token(&self) -> CancellationToken {
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
