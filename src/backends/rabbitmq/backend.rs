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
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

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
// RabbitMqStatsBridge — adapts the optional Management API client into the
// `QueueStatsProviderImpl` shape that `Backend::QueueStatsImpl` requires.
//
// `make_stats_provider` is an infallible factory, so the bridge defers the
// "management not configured" error to the first `snapshot` call — the
// caller sees a clear `Topology` error pointing at `RabbitMqConfig::with_management`.
// ---------------------------------------------------------------------------

use super::management::ManagementClient;

pub struct RabbitMqStatsBridge {
    inner: Option<ManagementClient>,
}

impl RabbitMqStatsBridge {
    fn from_config(cfg: Option<super::management::ManagementConfig>) -> Self {
        Self {
            inner: cfg.map(ManagementClient::new),
        }
    }
}

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
    type AutoscalerImpl = RabbitMqAutoscalerBackend<ManagementClient>;
    type QueueStatsImpl = RabbitMqStatsBridge;

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

    /// # Panics
    ///
    /// Panics if `RabbitMqConfig::with_management(...)` was not set. The
    /// autoscaler is useless without management — loud failure at
    /// construction is preferred over silent no-op decisions at scale time.
    fn make_autoscaler(client: &Self::Client) -> Self::AutoscalerImpl {
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let mc = ManagementClient::new(client.management_config().expect(
            "Broker<RabbitMq>::autoscaler() requires \
             RabbitMqConfig::with_management(...) to be set",
        ));
        let registry = Arc::new(Mutex::new(ConsumerGroupRegistry::new(client.clone())));
        RabbitMqAutoscalerBackend::with_stats_provider(mc, registry)
    }

    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl {
        RabbitMqStatsBridge::from_config(client.management_config())
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
        RabbitMqConsumer::spawn_fifo_shards::<T, H>(self, handler, ctx, options)
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

impl AutoscalerBackendImpl for RabbitMqAutoscalerBackend<ManagementClient> {}

// ---------------------------------------------------------------------------
// RabbitMqStatsBridge — maps Option<ManagementClient> into the
// QueueStatsProviderImpl::snapshot contract. When management is not
// configured, snapshot returns a Topology error directing the caller at
// RabbitMqConfig::with_management.
// ---------------------------------------------------------------------------

impl QueueStatsProviderImpl for RabbitMqStatsBridge {
    async fn snapshot(&self, queue: &str) -> Result<AutoscaleMetrics> {
        let mc = self.inner.as_ref().ok_or_else(|| {
            ShoveError::Topology(
                "Broker<RabbitMq>::queue_stats_provider().snapshot(...) requires \
                 RabbitMqConfig::with_management(...) to be set"
                    .into(),
            )
        })?;
        let stats =
            <ManagementClient as super::management::QueueStatsProvider>::get_queue_stats(mc, queue)
                .await?;
        Ok(AutoscaleMetrics {
            backlog: Some(stats.messages_ready),
            inflight: Some(stats.messages_unacknowledged),
            throughput_per_sec: None,
            processing_latency: None,
        })
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
        ConsumerGroupRegistry::register_fifo::<T, H>(self, config, factory, ctx).await
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.client_shutdown_token()
    }

    fn set_default_handler_timeout(&mut self, timeout: std::time::Duration) {
        self.default_handler_timeout = Some(timeout);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stats_bridge_without_management_errors() {
        let bridge = RabbitMqStatsBridge { inner: None };
        let err = <RabbitMqStatsBridge as QueueStatsProviderImpl>::snapshot(&bridge, "whatever")
            .await
            .expect_err("bridge with no management must refuse to produce metrics");
        assert!(matches!(err, ShoveError::Topology(_)));
        assert!(
            err.to_string().contains("with_management"),
            "error must direct caller at RabbitMqConfig::with_management; got: {err}"
        );
    }

    #[test]
    fn stats_bridge_from_config_none_yields_unconfigured_bridge() {
        let bridge = RabbitMqStatsBridge::from_config(None);
        assert!(bridge.inner.is_none());
    }

    #[test]
    fn stats_bridge_from_config_some_yields_configured_bridge() {
        let cfg = super::super::management::ManagementConfig::new(
            "http://localhost:15672",
            "guest",
            "guest",
        );
        let bridge = RabbitMqStatsBridge::from_config(Some(cfg));
        assert!(bridge.inner.is_some());
    }
}
