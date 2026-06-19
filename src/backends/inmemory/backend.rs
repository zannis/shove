//! Backend / impl-trait registrations for the in-memory backend.
//!
//! Binds the InMemory marker (`crate::markers::InMemory`) to the concrete
//! types in this module via `impl Backend` and `impl HasCoordinatedGroups`,
//! plus the six `pub(crate)` impl-trait bodies that carry the real work.
//!
//! The whole `crate::backends::inmemory` module is already gated on the
//! `inmemory` feature at the parent (`crate::backends`); no per-file cfg
//! is needed here.

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::backend::{
    AutoscalerBackendImpl, Backend, ConsumerImpl, ConsumerOptionsInner, QueueStatsProviderImpl,
    RegistryImpl, TopologyImpl, capability::HasCoordinatedGroups, sealed,
};
use crate::consumer_supervisor::{ShutdownTally, SupervisorOutcome};
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::markers::InMemory;
use crate::topic::{SequencedTopic, Topic};
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// `InMemoryQueueStatsProvider` is imported only for its trait-method-in-scope
// effect: the `BrokerStatsProvider::get_queue_stats` inherent name is the
// trait method, not an inherent method, so the trait must be in scope for
// `snapshot` to resolve it.
use super::autoscaler::{
    BrokerStatsProvider, InMemoryAutoscalerBackend, InMemoryQueueStatsProvider,
};
use super::client::{InMemoryBroker, InMemoryConfig};
use super::consumer::InMemoryConsumer;
use super::consumer_group::{InMemoryConsumerGroupConfig, InMemoryConsumerGroupRegistry};
use super::publisher::InMemoryPublisher;
use super::topology::InMemoryTopologyDeclarer;

// ---------------------------------------------------------------------------
// Marker bindings
// ---------------------------------------------------------------------------

impl sealed::Sealed for InMemory {}

impl Backend for InMemory {
    type Config = InMemoryConfig;
    type Client = InMemoryBroker;

    type PublisherImpl = InMemoryPublisher;
    type ConsumerImpl = InMemoryConsumer;
    type TopologyImpl = InMemoryTopologyDeclarer;
    // Bound to the default stats-provider parameterisation — autoscaler impls
    // built with a custom `InMemoryQueueStatsProvider` go through a different
    // constructor and don't flow through this trait.
    type AutoscalerImpl = InMemoryAutoscalerBackend<BrokerStatsProvider>;
    type QueueStatsImpl = BrokerStatsProvider;

    async fn connect(config: Self::Config) -> Result<Self::Client> {
        Ok(InMemoryBroker::with_config(config))
    }

    async fn make_publisher(client: &Self::Client) -> Result<Self::PublisherImpl> {
        Ok(InMemoryPublisher::new(client.clone()))
    }

    fn make_consumer(client: &Self::Client) -> Self::ConsumerImpl {
        InMemoryConsumer::new(client.clone())
    }

    fn make_declarer(client: &Self::Client) -> Self::TopologyImpl {
        InMemoryTopologyDeclarer::new(client.clone())
    }

    fn make_autoscaler(client: &Self::Client) -> Self::AutoscalerImpl {
        // Creates a fresh registry separate from the one used by
        // `ConsumerGroup<InMemory>`. Consumer groups registered through
        // `Broker::consumer_group()` are not visible to this autoscaler;
        // wire them via `InMemoryAutoscalerBackend::new` with the shared
        // registry if cross-visibility is required.
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let registry = Arc::new(Mutex::new(InMemoryConsumerGroupRegistry::new(
            client.clone(),
        )));
        InMemoryAutoscalerBackend::new(client.clone(), registry)
    }

    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl {
        BrokerStatsProvider::new(client.clone())
    }

    async fn close(client: &Self::Client) {
        client.shutdown();
    }

    async fn ping(client: &Self::Client, timeout: std::time::Duration) -> Result<()> {
        client.ping(timeout).await
    }
}

impl HasCoordinatedGroups for InMemory {
    type ConsumerGroupConfig = InMemoryConsumerGroupConfig;
    type RegistryImpl = InMemoryConsumerGroupRegistry;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl {
        InMemoryConsumerGroupRegistry::new(client.clone())
    }
}

// ---------------------------------------------------------------------------
// ConsumerImpl — delegate through the existing `Consumer` trait (Context = ())
// ---------------------------------------------------------------------------

impl ConsumerImpl for InMemoryConsumer {
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
        InMemoryConsumer::run_with_inner::<T, H>(self, handler, ctx, options).await
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
        InMemoryConsumer::run_fifo_with_inner::<T, H>(self, handler, ctx, options).await
    }

    async fn run_dlq<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        _options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        // In-memory DLQ has no size validation (messages never leave the
        // process); the options arg is accepted for trait conformance.
        InMemoryConsumer::run_dlq::<T, H>(self, handler, ctx).await
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
        InMemoryConsumer::spawn_fifo_shards_inner::<T, H>(self, handler, ctx, options)
    }
}

// ---------------------------------------------------------------------------
// TopologyImpl — delegate through the existing `TopologyDeclarer` impl
// ---------------------------------------------------------------------------

impl TopologyImpl for InMemoryTopologyDeclarer {
    async fn declare<T: Topic>(&self) -> Result<()> {
        InMemoryTopologyDeclarer::declare(self, T::topology()).await
    }
}

// ---------------------------------------------------------------------------
// AutoscalerBackendImpl — trait has no methods in Phase 4
// ---------------------------------------------------------------------------

impl AutoscalerBackendImpl for InMemoryAutoscalerBackend<BrokerStatsProvider> {}

// ---------------------------------------------------------------------------
// QueueStatsProviderImpl — map the stats to AutoscaleMetrics
// ---------------------------------------------------------------------------

impl QueueStatsProviderImpl for BrokerStatsProvider {
    async fn snapshot(&self, queue: &str) -> Result<AutoscaleMetrics> {
        let stats = self.get_queue_stats(queue).await?;
        Ok(AutoscaleMetrics {
            backlog: Some(stats.messages_ready),
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

impl RegistryImpl for InMemoryConsumerGroupRegistry {
    type GroupConfig = InMemoryConsumerGroupConfig;

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
        // Inherent methods take precedence over trait methods at call sites
        // where both are in scope, so `self.register` resolves to the inherent
        // method here — `<Self as _>::` disambiguation would be infinite
        // recursion.
        InMemoryConsumerGroupRegistry::register::<T, H>(self, config, factory, ctx).await
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
        InMemoryConsumerGroupRegistry::register_fifo::<T, H>(self, config, factory, ctx).await
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.broker_shutdown_token()
    }

    fn set_default_handler_timeout(&mut self, timeout: std::time::Duration) {
        self.default_handler_timeout = Some(timeout);
    }

    fn start_all(&mut self) {
        InMemoryConsumerGroupRegistry::start_all(self);
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
        // Start every registered group, then wait for either the caller's
        // shutdown signal or the broker's own shutdown token. Once either
        // fires, ask each group to drain; fall back to an abort after the
        // timeout.
        RegistryImpl::start_all(&mut self);

        let broker_token = self.broker_shutdown_token();
        let signal_handle = tokio::spawn(signal);
        tokio::select! {
            _ = broker_token.cancelled() => {}
            res = signal_handle => {
                // Spawned signal completed (or its task panicked — propagate
                // cancellation either way so groups shut down deterministically).
                let _ = res;
                broker_token.cancel();
            }
        }

        self.drain_until_timeout(drain_timeout).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{PublisherImpl, QueueStatsProviderImpl};
    use crate::topic::Topic;
    use crate::topology::{QueueTopology, TopologyBuilder};

    struct BS;
    impl Topic for BS {
        type Message = String;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("backend-stats").build())
        }
    }

    #[tokio::test]
    async fn broker_backend_connect_and_close_are_symmetric() {
        let client = <InMemory as Backend>::connect(InMemoryConfig::default())
            .await
            .expect("connect InMemory");
        assert!(!client.shutdown_token().is_cancelled());
        <InMemory as Backend>::close(&client).await;
        assert!(client.shutdown_token().is_cancelled());
    }

    #[tokio::test]
    async fn stats_provider_snapshot_reflects_queue_state() {
        let client = <InMemory as Backend>::connect(InMemoryConfig::default())
            .await
            .expect("connect InMemory");

        let declarer = <InMemory as Backend>::make_declarer(&client);
        <InMemoryTopologyDeclarer as TopologyImpl>::declare::<BS>(&declarer)
            .await
            .expect("declare");

        let publisher = <InMemory as Backend>::make_publisher(&client)
            .await
            .expect("publisher");
        for i in 0..4u32 {
            <InMemoryPublisher as PublisherImpl>::publish::<BS>(&publisher, &format!("m-{i}"))
                .await
                .expect("publish");
        }

        let stats = <InMemory as Backend>::make_stats_provider(&client);
        let metrics =
            <BrokerStatsProvider as QueueStatsProviderImpl>::snapshot(&stats, "backend-stats")
                .await
                .expect("snapshot");
        assert_eq!(metrics.backlog, Some(4));
        assert_eq!(metrics.inflight, Some(0));
        assert!(metrics.throughput_per_sec.is_none());
        assert!(metrics.processing_latency.is_none());

        // Missing queue propagates an error instead of returning defaulted metrics.
        let err =
            <BrokerStatsProvider as QueueStatsProviderImpl>::snapshot(&stats, "missing-queue")
                .await
                .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("queue"));

        <InMemory as Backend>::close(&client).await;
    }

    #[tokio::test]
    async fn make_autoscaler_produces_fresh_registry_handle() {
        let client = <InMemory as Backend>::connect(InMemoryConfig::default())
            .await
            .expect("connect InMemory");
        // `make_autoscaler` must not panic and must construct the backend with
        // an independent fresh registry (the doc comment on the impl).
        let _autoscaler = <InMemory as Backend>::make_autoscaler(&client);
        <InMemory as Backend>::close(&client).await;
    }

    #[tokio::test]
    async fn make_registry_reuses_client_shutdown_token() {
        let client = <InMemory as Backend>::connect(InMemoryConfig::default())
            .await
            .expect("connect InMemory");
        let registry = <InMemory as HasCoordinatedGroups>::make_registry(&client);
        let token = <InMemoryConsumerGroupRegistry as RegistryImpl>::cancellation_token(&registry);
        assert!(!token.is_cancelled());
        <InMemory as Backend>::close(&client).await;
        assert!(
            token.is_cancelled(),
            "client close must trip registry token"
        );
    }

    #[tokio::test]
    async fn drain_until_timeout_drains_started_groups_cleanly() {
        use crate::backend::RegistryImpl;
        let broker = InMemoryBroker::new();
        let mut registry = InMemoryConsumerGroupRegistry::new(broker);
        // No groups registered: start + drain must be a clean no-op.
        RegistryImpl::start_all(&mut registry);
        let outcome = RegistryImpl::drain_until_timeout(registry, Duration::from_millis(100)).await;
        assert!(outcome.is_clean(), "unexpected: {outcome:?}");
    }
}
