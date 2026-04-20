//! Backend / impl-trait registrations for the in-memory backend.
//!
//! Binds the InMemory marker (`crate::markers::InMemory`) to the concrete
//! types in this module via `impl Backend` and `impl HasCoordinatedGroups`,
//! plus the six `pub(crate)` impl-trait bodies that carry the real work.
//!
//! See DESIGN_V2.md §4 and §4.1.
//!
//! The whole `crate::backends::inmemory` module is already gated on the
//! `inmemory` feature at the parent (`crate::backends`); no per-file cfg
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
use crate::markers::InMemory;
use crate::topic::{SequencedTopic, Topic};

// `InMemoryQueueStatsProvider` is imported only for its trait-method-in-scope
// effect: the `BrokerStatsProvider::get_queue_stats` inherent name is the
// trait method, not an inherent method, so the trait must be in scope for
// `snapshot` to resolve it.
use super::autoscaler::{BrokerStatsProvider, InMemoryAutoscalerBackend, InMemoryQueueStatsProvider};
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
        // `InMemoryAutoscalerBackend::new` needs a shared registry handle too;
        // the public `Broker::autoscaler()` path (Phase 5+) supplies one. For
        // the trait-dispatch factory we build a fresh empty registry: callers
        // that want to observe scaling decisions go through the typed
        // `InMemoryAutoscalerBackend::autoscaler` constructor which is the
        // real wiring point until Phase 8's `Autoscaler<B>` harness lands.
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let registry = Arc::new(Mutex::new(InMemoryConsumerGroupRegistry::new(client.clone())));
        InMemoryAutoscalerBackend::new(client.clone(), registry)
    }

    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl {
        BrokerStatsProvider::new(client.clone())
    }

    async fn close(client: &Self::Client) {
        client.shutdown();
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
        _ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>,
    {
        let options = options.into_consumer_options();
        InMemoryConsumer::run::<T>(self, handler, options).await
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
        let options = options.into_consumer_options();
        InMemoryConsumer::run_fifo::<T>(self, handler, options).await
    }

    async fn run_dlq<T, H>(&self, handler: H, _ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>,
    {
        InMemoryConsumer::run_dlq::<T>(self, handler).await
    }
}

// ---------------------------------------------------------------------------
// TopologyImpl — delegate through the existing `TopologyDeclarer` impl
// ---------------------------------------------------------------------------

impl TopologyImpl for InMemoryTopologyDeclarer {
    async fn declare<T: Topic>(&self) -> Result<()> {
        <Self as crate::topology::TopologyDeclarer>::declare(self, T::topology()).await
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
        _ctx: H::Context,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>,
    {
        // `Context = ()` on the bound makes the generic `T: Topic` + `H: MessageHandler<T, Context = ()>`
        // the right shape for the existing inherent `register`. Inherent
        // methods take precedence over trait methods at call sites where
        // both are in scope, so `self.register` resolves to the inherent
        // method here — the `<Self as _>::` disambiguation would be an
        // infinite recursion.
        InMemoryConsumerGroupRegistry::register::<T, H>(self, config, factory).await
    }

    fn cancellation_token(&self) -> tokio_util::sync::CancellationToken {
        self.broker_shutdown_token()
    }

    async fn run_until_timeout<S>(mut self, signal: S, drain_timeout: Duration) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        // Start every registered group, then wait for either the caller's
        // shutdown signal or the broker's own shutdown token. Once either
        // fires, ask each group to drain; fall back to an abort after the
        // timeout.
        self.start_all();

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

        // The existing `shutdown_all` performs per-group cancellation and
        // awaits each consumer task's `JoinHandle` without surfacing
        // per-task panic counts. Phase 11 rebuilds the registry around a
        // single `JoinSet` so errors/panics can be tallied here; for
        // Phase 4 the best-effort outcome records only drain-timeout.
        let drain = self.shutdown_all();
        match tokio::time::timeout(drain_timeout, drain).await {
            Ok(()) => SupervisorOutcome::default(),
            Err(_) => SupervisorOutcome {
                errors: 0,
                panics: 0,
                timed_out: true,
            },
        }
    }
}

