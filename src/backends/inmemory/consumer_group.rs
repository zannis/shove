use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::backend::ConsumerOptionsInner;
use crate::consumer::{
    DEFAULT_HANDLER_TIMEOUT, DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY,
};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::Topic;

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
}

impl InMemoryConsumerGroup {
    pub fn new<T, H>(
        queue: impl Into<String>,
        config: InMemoryConsumerGroupConfig,
        broker: InMemoryBroker,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
    ) -> Self
    where
        T: Topic + 'static,
        H: MessageHandler<T, Context = ()> + 'static,
    {
        let spawner: Spawner = Arc::new(move |options: ConsumerOptionsInner| {
            let handler = handler_factory();
            let consumer = InMemoryConsumer::new(broker.clone());
            tokio::spawn(async move {
                if let Err(e) = consumer.run_with_inner::<T>(handler, options).await {
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
        info!(group = %self.queue, consumers = self.consumers.len(), "shutting down in-memory consumer group");
        self.group_token.cancel();
        for (_token, _processing, handle) in self.consumers.drain(..) {
            let _ = handle.await;
        }
        debug!(group = %self.queue, "in-memory consumer group shutdown complete");
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
    ) -> Result<()>
    where
        T: Topic + 'static,
        H: MessageHandler<T, Context = ()> + 'static,
    {
        let topology = T::topology();
        let name = topology.queue().to_string();

        if self.groups.contains_key(&name) {
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
        );
        self.groups.insert(name, group);
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
        info!(
            count = self.groups.len(),
            "shutting down all in-memory consumer groups"
        );
        for group in self.groups.values_mut() {
            group.shutdown().await;
        }
        debug!("all in-memory consumer groups shut down");
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
}
