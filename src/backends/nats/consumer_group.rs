use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::backends::nats::client::NatsClient;
use crate::backends::nats::consumer::NatsConsumer;
use crate::backends::nats::topology::NatsTopologyDeclarer;
use crate::consumer::{Consumer, ConsumerOptions};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::Topic;
use crate::topology::TopologyDeclarer;

/// Type-erased factory that spawns a single consumer task.
pub(crate) type Spawner = Arc<dyn Fn(ConsumerOptions) -> JoinHandle<()> + Send + Sync>;

// ---------------------------------------------------------------------------
// NatsConsumerGroupConfig
// ---------------------------------------------------------------------------

pub struct NatsConsumerGroupConfig {
    pub(crate) prefetch_count: u16,
    pub(crate) min_consumers: u16,
    pub(crate) max_consumers: u16,
    pub(crate) max_retries: u32,
    pub(crate) handler_timeout: Option<Duration>,
    pub(crate) concurrent_processing: bool,
    pub(crate) max_pending_per_key: Option<usize>,
}

impl NatsConsumerGroupConfig {
    /// Create a new config with the given consumer count range.
    ///
    /// # Panics
    ///
    /// Panics if `*range.start() > *range.end()`.
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
            handler_timeout: None,
            concurrent_processing: false,
            max_pending_per_key: None,
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

    pub fn with_concurrent_processing(mut self, concurrent: bool) -> Self {
        self.concurrent_processing = concurrent;
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

    pub fn concurrent_processing(&self) -> bool {
        self.concurrent_processing
    }

    pub fn max_pending_per_key(&self) -> Option<usize> {
        self.max_pending_per_key
    }
}

// ---------------------------------------------------------------------------
// NatsConsumerGroup
// ---------------------------------------------------------------------------

pub struct NatsConsumerGroup {
    pub(crate) name: String,
    pub(crate) queue: String,
    pub(crate) config: NatsConsumerGroupConfig,
    pub(crate) spawner: Spawner,
    pub(crate) consumers: Vec<(CancellationToken, Arc<AtomicBool>, JoinHandle<()>)>,
    pub(crate) group_token: CancellationToken,
}

impl NatsConsumerGroup {
    pub fn new<T, H>(
        name: impl Into<String>,
        queue: impl Into<String>,
        config: NatsConsumerGroupConfig,
        client: NatsClient,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
    ) -> Self
    where
        T: Topic + 'static,
        H: MessageHandler<T> + Clone + 'static,
    {
        let concurrent = config.concurrent_processing;
        let spawner: Spawner = Arc::new(move |options: ConsumerOptions| {
            let handler = handler_factory();
            let consumer = NatsConsumer::new(client.clone());
            let options = if concurrent {
                options
            } else {
                ConsumerOptions {
                    prefetch_count: 1,
                    ..options
                }
            };

            tokio::spawn(async move {
                let result = consumer.run::<T>(handler, options).await;
                if let Err(e) = result {
                    tracing::error!("consumer task exited with error: {e}");
                }
            })
        });

        Self {
            name: name.into(),
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
            group = %self.name,
            queue = %self.queue,
            initial_consumers = target,
            "starting consumer group"
        );
        for _ in 0..target {
            self.spawn_one();
        }
    }

    /// Spawn one additional consumer. Returns false at max capacity.
    pub fn scale_up(&mut self) -> bool {
        if self.consumers.len() >= self.config.max_consumers as usize {
            debug!(group = %self.name, max = self.config.max_consumers, "scale_up rejected: at max capacity");
            return false;
        }
        self.spawn_one();
        info!(
            group = %self.name,
            consumers = self.consumers.len(),
            "scaled up: spawned new consumer"
        );
        true
    }

    /// Cancel an idle consumer. Returns false at min capacity or all busy.
    pub fn scale_down(&mut self) -> bool {
        if self.consumers.len() <= self.config.min_consumers as usize {
            debug!(group = %self.name, min = self.config.min_consumers, "scale_down rejected: at min capacity");
            return false;
        }

        let idle_index = self
            .consumers
            .iter()
            .rposition(|(_, processing, _)| !processing.load(Ordering::Relaxed));

        let Some(index) = idle_index else {
            warn!(group = %self.name, "scale_down rejected: all consumers are busy");
            return false;
        };

        let (token, _, _handle) = self.consumers.swap_remove(index);
        token.cancel();

        info!(
            group = %self.name,
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

    pub fn config(&self) -> &NatsConsumerGroupConfig {
        &self.config
    }

    pub async fn shutdown(&mut self) {
        info!(group = %self.name, consumers = self.consumers.len(), "shutting down consumer group");
        self.group_token.cancel();
        for (_token, _processing, handle) in self.consumers.drain(..) {
            let _ = handle.await;
        }
        debug!(group = %self.name, "consumer group shutdown complete");
    }

    fn spawn_one(&mut self) {
        let child_token = self.group_token.child_token();
        let processing = Arc::new(AtomicBool::new(false));
        let options = ConsumerOptions {
            max_retries: self.config.max_retries,
            prefetch_count: self.config.prefetch_count,
            shutdown: child_token.clone(),
            processing: processing.clone(),
            handler_timeout: self.config.handler_timeout,
            max_pending_per_key: self.config.max_pending_per_key,
            exactly_once: false,
            receive_batch_size: 0,
        };
        let handle = (self.spawner)(options);
        self.consumers.push((child_token, processing, handle));
        debug!(group = %self.name, consumer_index = self.consumers.len() - 1, "spawned consumer");
    }
}

// ---------------------------------------------------------------------------
// NatsConsumerGroupRegistry
// ---------------------------------------------------------------------------

pub struct NatsConsumerGroupRegistry {
    pub(crate) groups: HashMap<String, NatsConsumerGroup>,
    client: Option<NatsClient>,
}

impl NatsConsumerGroupRegistry {
    pub fn new(client: NatsClient) -> Self {
        Self {
            groups: HashMap::new(),
            client: Some(client),
        }
    }

    /// Create a registry from a pre-populated map of groups (for testing).
    /// The resulting registry cannot be used to call `register()`.
    #[cfg(test)]
    pub(crate) fn from_groups(groups: HashMap<String, NatsConsumerGroup>) -> Self {
        Self {
            groups,
            client: None,
        }
    }

    pub async fn register<T, H>(
        &mut self,
        config: NatsConsumerGroupConfig,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
    ) -> Result<()>
    where
        T: Topic + 'static,
        H: MessageHandler<T> + Clone + 'static,
    {
        let topology = T::topology();
        let name = topology.queue().to_string();

        if self.groups.contains_key(&name) {
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' is already registered"
            )));
        }

        let client = self.client.as_ref().ok_or_else(|| {
            ShoveError::Connection("registry has no client (test-only registry)".into())
        })?;

        let declarer = NatsTopologyDeclarer::new(client.clone());
        declarer.declare(topology).await?;

        info!(group = %name, "registering consumer group");
        let group_token = client.shutdown_token().child_token();
        let group = NatsConsumerGroup::new::<T, H>(
            name.clone(),
            name.clone(),
            config,
            client.clone(),
            group_token,
            handler_factory,
        );
        self.groups.insert(name, group);
        Ok(())
    }

    pub fn start_all(&mut self) {
        info!(count = self.groups.len(), "starting all consumer groups");
        for group in self.groups.values_mut() {
            group.start();
        }
    }

    pub fn groups(&self) -> &HashMap<String, NatsConsumerGroup> {
        &self.groups
    }

    pub fn groups_mut(&mut self) -> &mut HashMap<String, NatsConsumerGroup> {
        &mut self.groups
    }

    pub async fn shutdown_all(&mut self) {
        info!(
            count = self.groups.len(),
            "shutting down all consumer groups"
        );
        for group in self.groups.values_mut() {
            group.shutdown().await;
        }
        debug!("all consumer groups shut down");
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_group(config: NatsConsumerGroupConfig) -> NatsConsumerGroup {
        let group_token = CancellationToken::new();
        let spawner: Spawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });

        NatsConsumerGroup {
            name: "test-group".into(),
            queue: "test-queue".into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
        }
    }

    fn default_config() -> NatsConsumerGroupConfig {
        NatsConsumerGroupConfig::new(1..=4)
    }

    // -- start --

    #[test]
    fn start_spawns_min_consumers() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(NatsConsumerGroupConfig::new(3..=5));
            group.start();
            assert_eq!(group.active_consumers(), 3);
            group.shutdown().await;
        });
    }

    #[test]
    fn start_with_zero_min_spawns_nothing() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(NatsConsumerGroupConfig::new(0..=4));
            group.start();
            assert_eq!(group.active_consumers(), 0);
            group.shutdown().await;
        });
    }

    // -- scale_up --

    #[test]
    fn scale_up_adds_one_consumer() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(default_config());
            group.start();
            assert_eq!(group.active_consumers(), 1);
            assert!(group.scale_up());
            assert_eq!(group.active_consumers(), 2);
            group.shutdown().await;
        });
    }

    #[test]
    fn scale_up_rejected_at_max() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(NatsConsumerGroupConfig::new(2..=2));
            group.start();
            assert!(!group.scale_up());
            assert_eq!(group.active_consumers(), 2);
            group.shutdown().await;
        });
    }

    // -- scale_down --

    #[test]
    fn scale_down_removes_one_consumer() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(default_config());
            group.start();
            group.scale_up();
            assert_eq!(group.active_consumers(), 2);
            assert!(group.scale_down());
            assert_eq!(group.active_consumers(), 1);
            group.shutdown().await;
        });
    }

    #[test]
    fn scale_down_rejected_at_min() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(default_config());
            group.start();
            assert!(!group.scale_down());
            assert_eq!(group.active_consumers(), 1);
            group.shutdown().await;
        });
    }

    #[test]
    fn scale_down_skips_busy_consumers() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(NatsConsumerGroupConfig::new(0..=3));
            group.scale_up();
            group.scale_up();
            group.scale_up();

            for (_, processing, _) in &group.consumers {
                processing.store(true, Ordering::Release);
            }

            assert!(!group.scale_down());
            assert_eq!(group.active_consumers(), 3);
            group.shutdown().await;
        });
    }

    #[test]
    fn scale_down_cancels_token() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(NatsConsumerGroupConfig::new(0..=2));
            group.scale_up();
            group.scale_up();

            let doomed_token = group.consumers[1].0.clone();
            assert!(!doomed_token.is_cancelled());

            group.scale_down();
            assert!(doomed_token.is_cancelled());
            group.shutdown().await;
        });
    }

    // -- shutdown --

    #[tokio::test]
    async fn shutdown_cancels_group_token() {
        let mut group = test_group(default_config());
        let group_token = group.group_token.clone();
        group.start();
        group.scale_up();

        assert!(!group_token.is_cancelled());
        group.shutdown().await;
        assert!(group_token.is_cancelled());
        assert_eq!(group.active_consumers(), 0);
    }

    // -- accessors --

    #[test]
    fn queue_returns_configured_queue() {
        let group = test_group(default_config());
        assert_eq!(group.queue(), "test-queue");
    }

    #[test]
    fn config_returns_reference() {
        let group = test_group(
            NatsConsumerGroupConfig::new(2..=8)
                .with_prefetch_count(5)
                .with_max_retries(3)
                .with_handler_timeout(Duration::from_secs(30)),
        );
        let config = group.config();
        assert_eq!(config.min_consumers(), 2);
        assert_eq!(config.max_consumers(), 8);
        assert_eq!(config.prefetch_count(), 5);
        assert_eq!(config.max_retries(), 3);
        assert_eq!(config.handler_timeout(), Some(Duration::from_secs(30)));
    }

    // -- config validation --

    #[test]
    #[should_panic]
    #[allow(clippy::reversed_empty_ranges)]
    fn new_panics_if_min_greater_than_max() {
        let _ = NatsConsumerGroupConfig::new(5..=2);
    }
}
