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
    DEFAULT_HANDLER_TIMEOUT, DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY,
};
use crate::consumer_supervisor::ShutdownTally;
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
    handler_timeout: Option<Duration>,
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
            handler_timeout: Some(DEFAULT_HANDLER_TIMEOUT),
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
        self.handler_timeout = Some(timeout);
        self
    }

    pub fn without_handler_timeout(mut self) -> Self {
        self.handler_timeout = None;
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

    pub fn handler_timeout(&self) -> Option<Duration> {
        self.handler_timeout
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
        }
    }

    /// Construct a FIFO consumer group for a `SequencedTopic`.
    ///
    /// FIFO replica count is fixed at 1 — concurrency comes from shards,
    /// not from multiple replicas of the shard set.
    pub fn new_fifo<T, H>(
        queue: impl Into<String>,
        broker: InMemoryBroker,
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
        let fifo_config = InMemoryConsumerGroupConfig::new(1..=1);

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
            config: fifo_config,
            spawner,
            group_token,
            error_count,
            panic_count,
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

        let (token, _, _handle) = self.consumers.swap_remove(index);
        token.cancel();

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
        info!(group = %self.queue, consumers = self.consumers.len(), "shutting down in-memory consumer group");
        self.group_token.cancel();
        let mut panics = 0usize;
        for (_token, _processing, handle) in self.consumers.drain(..) {
            match handle.await {
                Ok(()) => {}
                // Defensive: shutdown is cooperative via `group_token`; no
                // code path currently calls `JoinHandle::abort()` on these
                // handles, so this arm is unreachable today. It mirrors the
                // `ConsumerSupervisor` drain (src/consumer_supervisor.rs)
                // and keeps parity if a future timeout escalation adds
                // `abort_all`. Cancelled tasks do not count as panics.
                Err(e) if e.is_cancelled() => {}
                Err(e) => {
                    tracing::error!(error = %e, group = %self.queue, "consumer task panicked");
                    panics += 1;
                }
            }
        }
        let errors = self.error_count.swap(0, Ordering::Relaxed);
        let panics = panics + self.panic_count.swap(0, Ordering::Relaxed);
        debug!(group = %self.queue, errors, panics, "in-memory consumer group shutdown complete");
        ShutdownTally { errors, panics }
    }

    fn spawn_one(&mut self) {
        let child_token = self.group_token.child_token();
        let processing = Arc::new(AtomicBool::new(false));
        let mut options = ConsumerOptionsInner::defaults_with_shutdown(child_token.clone());
        options.max_retries = self.config.max_retries;
        options.prefetch_count = self.config.prefetch_count;
        options.handler_timeout = self.config.handler_timeout;
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
}

impl InMemoryConsumerGroupRegistry {
    pub fn new(broker: InMemoryBroker) -> Self {
        Self {
            groups: HashMap::new(),
            broker: Some(broker),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_groups(groups: HashMap<String, InMemoryConsumerGroup>) -> Self {
        Self {
            groups,
            broker: None,
        }
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
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: SequencedTopic + 'static,
        H: MessageHandler<T> + 'static,
    {
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

        let group_token = broker.shutdown_token().child_token();
        let group = InMemoryConsumerGroup::new_fifo::<T, H>(
            queue.clone(),
            broker,
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
        info!(
            count = self.groups.len(),
            "shutting down all in-memory consumer groups"
        );
        let mut tally = ShutdownTally::default();
        for group in self.groups.values_mut() {
            tally.add(group.shutdown_with_tally().await);
        }
        debug!(
            errors = tally.errors,
            panics = tally.panics,
            "all in-memory consumer groups shut down"
        );
        tally
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
}
