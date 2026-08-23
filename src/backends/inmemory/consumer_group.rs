use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::backend::ConsumerOptionsInner;
use crate::consumer::{
    DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY, HandlerTimeoutConfig,
    resolve_handler_timeout,
};
use crate::consumer_supervisor::{AbortOnDrop, ShutdownTally};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metrics;
use crate::topic::{SequencedTopic, Topic};

use super::client::InMemoryBroker;
use super::consumer::InMemoryConsumer;
use super::topology::InMemoryTopologyDeclarer;

pub(crate) type Spawner = Arc<dyn Fn(ConsumerOptionsInner) -> JoinHandle<()> + Send + Sync>;

/// Configuration for an [`InMemoryConsumerGroup`].
#[derive(Clone)]
pub struct InMemoryConsumerGroupConfig {
    prefetch_count: u16,
    min_consumers: u16,
    max_consumers: u16,
    max_retries: u32,
    pub(crate) handler_timeout: HandlerTimeoutConfig,
    /// What a handler timeout resolves to. `None` keeps the backend's
    /// historical default.
    pub(crate) handler_timeout_outcome: Option<crate::Outcome>,
    max_pending_per_key: Option<usize>,
    max_message_size: Option<usize>,
}

impl Default for InMemoryConsumerGroupConfig {
    /// A single consumer, default tuning. Matches the defaults baked into
    /// `InMemoryConsumerGroupConfig::new(1..=1)`. Mirrors the
    /// `HasCoordinatedGroups::ConsumerGroupConfig: Default` bound.
    fn default() -> Self {
        Self::new(1..=1)
    }
}

impl InMemoryConsumerGroupConfig {
    /// Create a new config with the given consumer-count range.
    ///
    /// # Panics
    ///
    /// Panics when `*range.start() > *range.end()`.
    pub fn new(range: RangeInclusive<u16>) -> Self {
        let min = *range.start();
        let max = *range.end();
        assert!(
            min <= max,
            "min_consumers ({min}) must be <= max_consumers ({max})"
        );
        Self {
            prefetch_count: 10,
            min_consumers: min,
            max_consumers: max,
            max_retries: 10,
            handler_timeout: HandlerTimeoutConfig::Inherit,
            handler_timeout_outcome: None,
            max_pending_per_key: Some(DEFAULT_MAX_PENDING_PER_KEY),
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
        }
    }

    pub fn with_prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "handler_timeout must be positive");
        self.handler_timeout = HandlerTimeoutConfig::Set(timeout);
        self
    }

    /// Choose what a handler timeout resolves to for consumers in this group,
    /// instead of the backend default. See
    /// [`ConsumerOptions::with_handler_timeout_outcome`](crate::ConsumerOptions::with_handler_timeout_outcome)
    /// for the semantics of each outcome; leaving it unset preserves current
    /// behaviour exactly.
    pub fn with_handler_timeout_outcome(mut self, outcome: crate::Outcome) -> Self {
        self.handler_timeout_outcome = Some(outcome);
        self
    }

    pub fn with_max_message_size(mut self, max: usize) -> Self {
        self.max_message_size = Some(max);
        self
    }

    pub fn without_message_size_limit(mut self) -> Self {
        self.max_message_size = None;
        self
    }

    pub fn with_max_pending_per_key(mut self, limit: usize) -> Self {
        self.max_pending_per_key = Some(limit);
        self
    }

    pub fn without_max_pending_per_key(mut self) -> Self {
        self.max_pending_per_key = None;
        self
    }

    pub fn prefetch_count(&self) -> u16 {
        self.prefetch_count
    }

    pub fn min_consumers(&self) -> u16 {
        self.min_consumers
    }

    pub fn max_consumers(&self) -> u16 {
        self.max_consumers
    }

    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Returns the configured handler timeout. A freshly-constructed
    /// config reports `Some(DEFAULT_HANDLER_TIMEOUT)`; a registry-level
    /// default set via `ConsumerGroup::with_default_handler_timeout`
    /// is not reflected here because the config does not know about
    /// its registry.
    pub fn handler_timeout(&self) -> Option<Duration> {
        Some(resolve_handler_timeout(self.handler_timeout, None))
    }

    pub fn max_pending_per_key(&self) -> Option<usize> {
        self.max_pending_per_key
    }

    pub fn max_message_size(&self) -> Option<usize> {
        self.max_message_size
    }
}

/// A group of in-memory consumers sharing a single queue's load.
pub struct InMemoryConsumerGroup {
    pub(crate) queue: String,
    pub(crate) config: InMemoryConsumerGroupConfig,
    pub(crate) spawner: Spawner,
    pub(crate) consumers: Vec<(CancellationToken, Arc<AtomicBool>, JoinHandle<()>)>,
    pub(crate) group_token: CancellationToken,
    /// Error count incremented by each spawned task when its inner
    /// `run_with_inner` returns `Err`. Drained by
    /// [`InMemoryConsumerGroup::shutdown_with_tally`].
    pub(crate) error_count: Arc<AtomicUsize>,
    /// Panic count incremented by the FIFO spawner wrapper when a shard
    /// task exits with a `JoinError` that is not a cancellation. Drained by
    /// [`InMemoryConsumerGroup::shutdown_with_tally`].
    pub(crate) panic_count: Arc<AtomicUsize>,
    /// Handles of consumers removed by `scale_down`. Their tokens are already
    /// cancelled; retained here so the final drain awaits/aborts and tallies
    /// them instead of detaching the task.
    pub(crate) retiring: Vec<JoinHandle<()>>,
}

impl InMemoryConsumerGroup {
    pub fn new<T, H>(
        queue: impl Into<String>,
        config: InMemoryConsumerGroupConfig,
        broker: InMemoryBroker,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Self
    where
        T: Topic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let error_count = Arc::new(AtomicUsize::new(0));
        let ec_for_spawner = error_count.clone();
        let spawner: Spawner = Arc::new(move |options: ConsumerOptionsInner| {
            let handler = handler_factory();
            let consumer = InMemoryConsumer::new(broker.clone());
            let ec = ec_for_spawner.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                if let Err(e) = consumer.run_with_inner::<T, H>(handler, ctx, options).await {
                    ec.fetch_add(1, Ordering::Relaxed);
                    tracing::error!("in-memory consumer task exited with error: {e}");
                }
            })
        });

        Self {
            queue: queue.into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
            error_count,
            panic_count: Arc::new(AtomicUsize::new(0)),
            retiring: Vec::new(),
        }
    }

    /// Construct a FIFO consumer group for a `SequencedTopic`.
    ///
    /// FIFO replica count is fixed at 1 — concurrency comes from shards,
    /// not from multiple replicas of the shard set.
    pub fn new_fifo<T, H>(
        queue: impl Into<String>,
        broker: InMemoryBroker,
        mut config: InMemoryConsumerGroupConfig,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Self
    where
        T: SequencedTopic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let error_count = Arc::new(AtomicUsize::new(0));
        let panic_count = Arc::new(AtomicUsize::new(0));
        let ec_for_spawner = error_count.clone();
        let pc_for_spawner = panic_count.clone();

        // FIFO replica count is fixed at 1 — FIFO concurrency is per-shard, not per-replica.
        // Override min/max consumers from the user-provided config since FIFO is always single-replica.
        config.min_consumers = 1;
        config.max_consumers = 1;

        let spawner: Spawner = Arc::new(move |options: ConsumerOptionsInner| {
            let handler = handler_factory();
            let consumer = InMemoryConsumer::new(broker.clone());
            let ec = ec_for_spawner.clone();
            let pc = pc_for_spawner.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let handles = match consumer.spawn_fifo_shards_inner::<T, H>(handler, ctx, options)
                {
                    Ok(h) => h,
                    Err(e) => {
                        ec.fetch_add(1, Ordering::Relaxed);
                        tracing::error!("FIFO registration failed: {e}");
                        return;
                    }
                };
                for handle in handles {
                    match handle.await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            ec.fetch_add(1, Ordering::Relaxed);
                            tracing::error!("sequenced shard exited with error: {e}");
                        }
                        Err(e) if e.is_cancelled() => {}
                        Err(e) => {
                            pc.fetch_add(1, Ordering::Relaxed);
                            tracing::error!("sequenced shard panicked: {e}");
                        }
                    }
                }
            })
        });

        Self {
            queue: queue.into(),
            consumers: Vec::with_capacity(1),
            config,
            spawner,
            group_token,
            error_count,
            panic_count,
            retiring: Vec::new(),
        }
    }

    /// Spawn `min_consumers` consumers.
    pub fn start(&mut self) {
        let target = self.config.min_consumers as usize;
        info!(
            group = %self.queue,
            queue = %self.queue,
            initial_consumers = target,
            "starting in-memory consumer group"
        );
        for _ in 0..target {
            self.spawn_one();
        }
    }

    /// Spawn one additional consumer. Returns `false` at max capacity.
    pub fn scale_up(&mut self) -> bool {
        if self.consumers.len() >= self.config.max_consumers as usize {
            debug!(group = %self.queue, max = self.config.max_consumers, "scale_up rejected: at max capacity");
            return false;
        }
        self.spawn_one();
        info!(
            group = %self.queue,
            consumers = self.consumers.len(),
            "scaled up: spawned new consumer"
        );
        true
    }

    /// Cancel an idle consumer. Returns `false` at min capacity or when every
    /// consumer is busy.
    pub fn scale_down(&mut self) -> bool {
        if self.consumers.len() <= self.config.min_consumers as usize {
            debug!(group = %self.queue, min = self.config.min_consumers, "scale_down rejected: at min capacity");
            return false;
        }

        let idle_index = self
            .consumers
            .iter()
            .rposition(|(_, processing, _)| !processing.load(Ordering::Relaxed));

        let Some(index) = idle_index else {
            warn!(group = %self.queue, "scale_down rejected: all consumers are busy");
            return false;
        };

        let (token, _, handle) = self.consumers.swap_remove(index);
        token.cancel();
        self.retiring.push(handle);

        info!(
            group = %self.queue,
            consumers = self.consumers.len(),
            "scaled down: cancelled an idle consumer"
        );
        true
    }

    pub fn active_consumers(&self) -> usize {
        self.consumers.len()
    }

    #[cfg(test)]
    pub(crate) fn retiring_is_empty(&self) -> bool {
        self.retiring.is_empty()
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn config(&self) -> &InMemoryConsumerGroupConfig {
        &self.config
    }

    /// Cancel every consumer and wait for all tasks to finish.
    pub async fn shutdown(&mut self) {
        let _ = self.shutdown_with_tally().await;
    }

    /// Same as [`shutdown`] but returns a tally of how many consumer tasks
    /// exited with a non-retryable error or panicked. Used by
    /// `RegistryImpl::run_until_timeout` to surface failures through
    /// [`SupervisorOutcome`](crate::consumer_supervisor::SupervisorOutcome).
    pub(crate) async fn shutdown_with_tally(&mut self) -> ShutdownTally {
        let mut tally = ShutdownTally::default();
        self.drain_into(&mut tally).await;
        debug!(
            group = %self.queue,
            errors = tally.errors,
            panics = tally.panics,
            "in-memory consumer group shutdown complete"
        );
        tally
    }

    /// Cancel the group token and await every consumer handle, accumulating
    /// errors and panics into the caller-owned `tally`.
    ///
    /// Atomic counts are swapped into `tally` **before** any handle is
    /// awaited, so a caller that races this against a timeout (see
    /// `RegistryImpl::run_until_timeout`) preserves pre-cancel state even if
    /// the future is dropped mid-await. The consumer list is drained via
    /// `pop()` so dropped futures leave unawaited handles in place for a
    /// subsequent escalation via [`Self::abort_remaining_into`].
    pub(crate) async fn drain_into(&mut self, tally: &mut ShutdownTally) {
        info!(
            group = %self.queue,
            consumers = self.consumers.len(),
            "shutting down in-memory consumer group"
        );
        self.group_token.cancel();

        tally.errors += self.error_count.swap(0, Ordering::Relaxed);
        tally.panics += self.panic_count.swap(0, Ordering::Relaxed);

        while let Some((_token, _processing, handle)) = self.consumers.pop() {
            let _abort_guard = AbortOnDrop(handle.abort_handle());
            match handle.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => {
                    tracing::error!(error = %e, group = %self.queue, "consumer task panicked");
                    tally.panics += 1;
                }
            }
        }

        while let Some(handle) = self.retiring.pop() {
            let _abort_guard = AbortOnDrop(handle.abort_handle());
            match handle.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => {
                    tracing::error!(error = %e, group = %self.queue, "retired consumer task panicked");
                    tally.panics += 1;
                }
            }
        }

        tally.errors += self.error_count.swap(0, Ordering::Relaxed);
        tally.panics += self.panic_count.swap(0, Ordering::Relaxed);
    }

    /// Abort surviving consumer handles after a drain timeout, accumulating
    /// any results into `tally`.
    pub(crate) async fn abort_remaining_into(&mut self, tally: &mut ShutdownTally) {
        self.group_token.cancel();
        for (_token, _processing, handle) in &self.consumers {
            handle.abort();
        }
        for handle in &self.retiring {
            handle.abort();
        }
        while let Some((_token, _processing, handle)) = self.consumers.pop() {
            match handle.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        group = %self.queue,
                        "consumer task panicked during abort escalation"
                    );
                    tally.panics += 1;
                }
            }
        }
        while let Some(handle) = self.retiring.pop() {
            match handle.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => {
                    tracing::error!(error = %e, group = %self.queue, "retired consumer task panicked");
                    tally.panics += 1;
                }
            }
        }
        tally.errors += self.error_count.swap(0, Ordering::Relaxed);
        tally.panics += self.panic_count.swap(0, Ordering::Relaxed);
    }

    fn spawn_one(&mut self) {
        let child_token = self.group_token.child_token();
        let processing = Arc::new(AtomicBool::new(false));
        let mut options = ConsumerOptionsInner::defaults_with_shutdown(child_token.clone());
        options.max_retries = self.config.max_retries;
        options.prefetch_count = self.config.prefetch_count;
        options.handler_timeout = Some(resolve_handler_timeout(self.config.handler_timeout, None));
        options.handler_timeout_outcome = self.config.handler_timeout_outcome.clone();
        options.max_message_size = self.config.max_message_size;
        options.max_pending_per_key = self.config.max_pending_per_key;
        options.processing = processing.clone();
        options.consumer_group = Some(Arc::from(self.queue.as_str()));

        let handle = (self.spawner)(options);
        self.consumers.push((child_token, processing, handle));
        debug!(group = %self.queue, consumer_index = self.consumers.len() - 1, "spawned consumer");
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

pub struct InMemoryConsumerGroupRegistry {
    pub(crate) groups: HashMap<String, InMemoryConsumerGroup>,
    broker: Option<InMemoryBroker>,
    pub(super) default_handler_timeout: Option<Duration>,
}

impl InMemoryConsumerGroupRegistry {
    pub fn new(broker: InMemoryBroker) -> Self {
        Self {
            groups: HashMap::new(),
            broker: Some(broker),
            default_handler_timeout: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_groups(groups: HashMap<String, InMemoryConsumerGroup>) -> Self {
        Self {
            groups,
            broker: None,
            default_handler_timeout: None,
        }
    }

    /// Set the registry-level default handler timeout. Applies to every
    /// group whose `InMemoryConsumerGroupConfig` did not explicitly call
    /// `with_handler_timeout`.
    pub fn with_default_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "default_handler_timeout must be positive"
        );
        self.default_handler_timeout = Some(timeout);
        self
    }

    pub async fn register<T, H>(
        &mut self,
        config: InMemoryConsumerGroupConfig,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: Topic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let mut config = config;
        config.handler_timeout = HandlerTimeoutConfig::Set(resolve_handler_timeout(
            config.handler_timeout,
            self.default_handler_timeout,
        ));

        let topology = T::topology();
        let name = topology.queue().to_string();

        if self.groups.contains_key(&name) {
            metrics::record_backend_error(
                metrics::BackendLabel::InMemory,
                metrics::BackendErrorKind::Topology,
            );
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' is already registered"
            )));
        }

        let broker = self.broker.as_ref().ok_or_else(|| {
            ShoveError::Topology("registry has no broker (test-only registry)".into())
        })?;

        let declarer = InMemoryTopologyDeclarer::new(broker.clone());
        declarer.declare(topology).await?;

        info!(group = %name, "registering in-memory consumer group");
        let group_token = broker.shutdown_token().child_token();
        let group = InMemoryConsumerGroup::new::<T, H>(
            name.clone(),
            config,
            broker.clone(),
            group_token,
            handler_factory,
            ctx,
        );
        self.groups.insert(name, group);
        Ok(())
    }

    pub async fn register_fifo<T, H>(
        &mut self,
        config: InMemoryConsumerGroupConfig,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: SequencedTopic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let mut config = config;
        config.handler_timeout = HandlerTimeoutConfig::Set(resolve_handler_timeout(
            config.handler_timeout,
            self.default_handler_timeout,
        ));

        let topology = T::topology();
        let name = topology.queue().to_string();

        if self.groups.contains_key(&name) {
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' is already registered"
            )));
        }
        let queue = name.clone();

        let broker = self
            .broker
            .as_ref()
            .ok_or_else(|| ShoveError::Topology("registry not initialized".into()))?
            .clone();

        let declarer = InMemoryTopologyDeclarer::new(broker.clone());
        declarer.declare(topology).await?;

        let group_token = broker.shutdown_token().child_token();
        let group = InMemoryConsumerGroup::new_fifo::<T, H>(
            queue.clone(),
            broker,
            config,
            group_token,
            handler_factory,
            ctx,
        );
        self.groups.insert(queue, group);
        Ok(())
    }

    pub fn start_all(&mut self) {
        info!(
            count = self.groups.len(),
            "starting all in-memory consumer groups"
        );
        for group in self.groups.values_mut() {
            group.start();
        }
    }

    /// Broker-wide shutdown token. Used by the `RegistryImpl::run_until_timeout`
    /// adapter in `backend.rs` to propagate cancellation deterministically
    /// when the caller-supplied shutdown signal fires. Returns a fresh
    /// `CancellationToken` for test-only registries that have no broker.
    pub(crate) fn broker_shutdown_token(&self) -> CancellationToken {
        self.broker
            .as_ref()
            .map(|b| b.shutdown_token().clone())
            .unwrap_or_default()
    }

    pub fn groups(&self) -> &HashMap<String, InMemoryConsumerGroup> {
        &self.groups
    }

    pub fn groups_mut(&mut self) -> &mut HashMap<String, InMemoryConsumerGroup> {
        &mut self.groups
    }

    pub async fn shutdown_all(&mut self) {
        let _ = self.shutdown_all_with_tally().await;
    }

    /// Same as [`shutdown_all`] but aggregates a per-group tally of task
    /// errors and panics. Used by `RegistryImpl::run_until_timeout` to
    /// populate [`SupervisorOutcome`](crate::consumer_supervisor::SupervisorOutcome).
    pub(crate) async fn shutdown_all_with_tally(&mut self) -> ShutdownTally {
        let mut tally = ShutdownTally::default();
        self.drain_all_into(&mut tally).await;
        tally
    }

    /// Drain every consumer group, accumulating errors/panics into `tally`.
    pub(crate) async fn drain_all_into(&mut self, tally: &mut ShutdownTally) {
        info!(
            count = self.groups.len(),
            "shutting down all in-memory consumer groups"
        );
        for group in self.groups.values_mut() {
            group.drain_into(tally).await;
        }
        debug!(
            errors = tally.errors,
            panics = tally.panics,
            "all in-memory consumer groups shut down"
        );
    }

    /// Abort surviving consumers across every group after a drain timeout.
    pub(crate) async fn abort_all_remaining_into(&mut self, tally: &mut ShutdownTally) {
        for group in self.groups.values_mut() {
            group.abort_remaining_into(tally).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::DEFAULT_HANDLER_TIMEOUT;

    #[test]
    fn inherit_config_uses_library_default_with_no_registry_default() {
        let cfg = InMemoryConsumerGroupConfig::new(1..=1);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, None),
            DEFAULT_HANDLER_TIMEOUT,
        );
    }

    #[test]
    fn inherit_config_uses_registry_default_when_set() {
        let cfg = InMemoryConsumerGroupConfig::new(1..=1);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(45),
        );
    }

    #[test]
    fn with_handler_timeout_beats_registry_default() {
        let cfg =
            InMemoryConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::from_secs(5));
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(5),
        );
    }

    #[test]
    #[should_panic(expected = "handler_timeout must be positive")]
    fn with_handler_timeout_zero_panics() {
        let _ = InMemoryConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "default_handler_timeout must be positive")]
    fn with_default_handler_timeout_zero_panics() {
        let registry = InMemoryConsumerGroupRegistry::from_groups(HashMap::new());
        let _ = registry.with_default_handler_timeout(Duration::ZERO);
    }

    fn test_group(config: InMemoryConsumerGroupConfig) -> InMemoryConsumerGroup {
        let group_token = CancellationToken::new();
        let spawner: Spawner = Arc::new(|options: ConsumerOptionsInner| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });

        InMemoryConsumerGroup {
            queue: "test-queue".into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
            error_count: Arc::new(AtomicUsize::new(0)),
            panic_count: Arc::new(AtomicUsize::new(0)),
            retiring: Vec::new(),
        }
    }

    #[tokio::test]
    async fn start_spawns_min_consumers() {
        let mut group = test_group(InMemoryConsumerGroupConfig::new(3..=5));
        group.start();
        assert_eq!(group.active_consumers(), 3);
        group.shutdown().await;
    }

    #[tokio::test]
    async fn scale_up_adds_one_consumer() {
        let mut group = test_group(InMemoryConsumerGroupConfig::new(1..=4));
        group.start();
        assert!(group.scale_up());
        assert_eq!(group.active_consumers(), 2);
        group.shutdown().await;
    }

    #[tokio::test]
    async fn scale_up_rejected_at_max() {
        let mut group = test_group(InMemoryConsumerGroupConfig::new(2..=2));
        group.start();
        assert!(!group.scale_up());
        assert_eq!(group.active_consumers(), 2);
        group.shutdown().await;
    }

    #[tokio::test]
    async fn scale_down_removes_idle() {
        let mut group = test_group(InMemoryConsumerGroupConfig::new(1..=5));
        group.start();
        group.scale_up();
        group.scale_up();
        assert_eq!(group.active_consumers(), 3);
        assert!(group.scale_down());
        assert_eq!(group.active_consumers(), 2);
        group.shutdown().await;
    }

    #[tokio::test]
    async fn scale_down_rejected_at_min() {
        let mut group = test_group(InMemoryConsumerGroupConfig::new(1..=4));
        group.start();
        assert!(!group.scale_down());
        assert_eq!(group.active_consumers(), 1);
        group.shutdown().await;
    }

    // --- shutdown_with_tally regression tests for review #1 ---
    // Prior versions always returned 0/0 regardless of consumer-loop
    // failures; `RegistryImpl::run_until_timeout` therefore reported
    // `SupervisorOutcome::default()` even after errors/panics.

    fn panicking_group() -> InMemoryConsumerGroup {
        let config = InMemoryConsumerGroupConfig::new(2..=2);
        let group_token = CancellationToken::new();
        let spawner: Spawner = Arc::new(|_: ConsumerOptionsInner| {
            tokio::spawn(async move {
                panic!("simulated consumer-loop panic");
            })
        });
        InMemoryConsumerGroup {
            queue: "panicky".into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
            error_count: Arc::new(AtomicUsize::new(0)),
            panic_count: Arc::new(AtomicUsize::new(0)),
            retiring: Vec::new(),
        }
    }

    #[tokio::test]
    async fn shutdown_with_tally_counts_panicked_tasks() {
        let mut group = panicking_group();
        group.start();
        // Give the spawned tasks a moment to panic before draining.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let tally = group.shutdown_with_tally().await;
        assert_eq!(tally.panics, 2, "expected both spawned tasks to panic");
        assert_eq!(tally.errors, 0);
    }

    #[tokio::test]
    async fn shutdown_with_tally_counts_error_flag() {
        // Simulate consumer-loop errors by incrementing the group's error
        // counter directly — this is exactly what the spawner closure in
        // `InMemoryConsumerGroup::new` does on `Err` from `run_with_inner`.
        let group = test_group(InMemoryConsumerGroupConfig::new(1..=1));
        group.error_count.fetch_add(3, Ordering::Relaxed);
        let mut group = group;
        let tally = group.shutdown_with_tally().await;
        assert_eq!(tally.errors, 3);
        assert_eq!(tally.panics, 0);
    }

    fn hanging_test_group(config: InMemoryConsumerGroupConfig) -> InMemoryConsumerGroup {
        let mut group = test_group(config);
        group.spawner = Arc::new(|_options: ConsumerOptionsInner| {
            tokio::spawn(async {
                std::future::pending::<()>().await;
            })
        });
        group
    }

    #[tokio::test]
    async fn drain_into_timeout_preserves_atomics_in_tally() {
        let mut group = hanging_test_group(InMemoryConsumerGroupConfig::new(2..=2));
        group.start();

        group.error_count.store(7, Ordering::Relaxed);
        group.panic_count.store(2, Ordering::Relaxed);

        let mut tally = ShutdownTally::default();
        let result =
            tokio::time::timeout(Duration::from_millis(50), group.drain_into(&mut tally)).await;
        assert!(result.is_err(), "drain must time out on hanging consumers");

        assert_eq!(tally.errors, 7);
        assert_eq!(tally.panics, 2);
    }

    #[tokio::test]
    async fn abort_remaining_into_kills_hanging_consumers_and_keeps_tally() {
        let mut group = hanging_test_group(InMemoryConsumerGroupConfig::new(2..=2));
        group.start();

        group.error_count.store(5, Ordering::Relaxed);
        group.panic_count.store(1, Ordering::Relaxed);

        let mut tally = ShutdownTally::default();
        let _ = tokio::time::timeout(Duration::from_millis(50), group.drain_into(&mut tally)).await;
        group.abort_remaining_into(&mut tally).await;

        assert_eq!(group.active_consumers(), 0);
        assert_eq!(tally.errors, 5);
        assert_eq!(tally.panics, 1);
    }

    #[tokio::test]
    async fn scale_down_handle_is_drained_not_detached() {
        // A scaled-down consumer that hangs must still be aborted and counted,
        // not silently detached. Use a hanging spawner so the retiring handle
        // would leak under the old drop-the-handle behaviour.
        let mut group = hanging_test_group(InMemoryConsumerGroupConfig::new(1..=3));
        group.start();
        group.scale_up();
        group.scale_up();
        assert_eq!(group.active_consumers(), 3);
        // Force scale_down of an idle (hanging-but-not-processing) consumer.
        assert!(group.scale_down());
        assert_eq!(group.active_consumers(), 2);

        let mut tally = ShutdownTally::default();
        let _ = tokio::time::timeout(Duration::from_millis(50), group.drain_into(&mut tally)).await;
        group.abort_remaining_into(&mut tally).await;
        // All handles, including the retired one, are now resolved.
        assert_eq!(group.active_consumers(), 0);
        assert!(group.retiring_is_empty());
    }

    #[tokio::test]
    async fn registry_shutdown_all_with_tally_aggregates() {
        let mut registry = InMemoryConsumerGroupRegistry::from_groups(HashMap::new());
        registry.groups.insert("a".into(), panicking_group());
        registry.groups.insert("b".into(), panicking_group());
        for g in registry.groups.values_mut() {
            g.start();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let tally = registry.shutdown_all_with_tally().await;
        // 2 groups × 2 panicking consumers = 4 panics.
        assert_eq!(tally.panics, 4);
    }

    /// Verify that a handle popped from `consumers` but not yet awaited is
    /// ABORTED (not detached) when the outer drain timeout fires.
    ///
    /// The sentinel's `Drop` fires only when the task future is dropped, which
    /// happens on abort. If the handle is detached instead the sentinel never
    /// drops within the test window and the assertion fails.
    #[tokio::test]
    async fn drain_into_aborts_inflight_handle_on_timeout() {
        use std::sync::atomic::AtomicBool;

        struct AbortSentinel(Arc<AtomicBool>);
        impl Drop for AbortSentinel {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let aborted = Arc::new(AtomicBool::new(false));
        let aborted_clone = Arc::clone(&aborted);

        let mut group = test_group(InMemoryConsumerGroupConfig::new(1..=1));
        // Replace spawner with one that holds a sentinel and then hangs.
        group.spawner = Arc::new(move |_options: ConsumerOptionsInner| {
            let flag = Arc::clone(&aborted_clone);
            tokio::spawn(async move {
                let _sentinel = AbortSentinel(flag);
                std::future::pending::<()>().await;
            })
        });
        group.start();

        let mut tally = ShutdownTally::default();
        // Mirror how drain_until_timeout drives drain_into.
        let _ = tokio::time::timeout(Duration::from_millis(50), group.drain_into(&mut tally)).await;

        // Give the runtime a tick to deliver the abort signal and drop the future.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(
            aborted.load(Ordering::Acquire),
            "AbortSentinel must have been dropped: the in-flight handle was detached, not aborted"
        );
    }
}
