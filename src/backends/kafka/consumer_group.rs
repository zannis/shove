use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::backends::kafka::client::KafkaClient;
use crate::backends::kafka::consumer::KafkaConsumer;
use crate::backends::kafka::topology::KafkaTopologyDeclarer;
use crate::consumer_supervisor::ShutdownTally;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::Topic;
use crate::{DEFAULT_HANDLER_TIMEOUT, DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY};

/// Type-erased factory that spawns a single consumer task.
pub(crate) type Spawner = Arc<dyn Fn(ConsumerOptions) -> JoinHandle<()> + Send + Sync>;

// ---------------------------------------------------------------------------
// KafkaConsumerGroupConfig
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct KafkaConsumerGroupConfig {
    prefetch_count: u16,
    min_consumers: u16,
    max_consumers: u16,
    max_retries: u32,
    handler_timeout: Option<Duration>,
    concurrent_processing: bool,
    max_pending_per_key: Option<usize>,
    max_message_size: Option<usize>,
}

impl Default for KafkaConsumerGroupConfig {
    /// A single consumer, default tuning. Matches the defaults baked into
    /// `KafkaConsumerGroupConfig::new(1..=1)`. Mirrors the
    /// `HasCoordinatedGroups::ConsumerGroupConfig: Default` bound.
    fn default() -> Self {
        Self::new(1..=1)
    }
}

impl KafkaConsumerGroupConfig {
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
            handler_timeout: Some(DEFAULT_HANDLER_TIMEOUT),
            concurrent_processing: false,
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
// KafkaConsumerGroup
// ---------------------------------------------------------------------------

pub struct KafkaConsumerGroup {
    pub(crate) queue: String,
    pub(crate) config: KafkaConsumerGroupConfig,
    pub(crate) spawner: Spawner,
    pub(crate) consumers: Vec<(CancellationToken, Arc<AtomicBool>, JoinHandle<()>)>,
    pub(crate) group_token: CancellationToken,
    pub(crate) error_count: Arc<AtomicUsize>,
}

impl KafkaConsumerGroup {
    pub fn new<T, H>(
        queue: impl Into<String>,
        config: KafkaConsumerGroupConfig,
        client: KafkaClient,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Self
    where
        T: Topic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let concurrent = config.concurrent_processing;
        let error_count = Arc::new(AtomicUsize::new(0));
        let ec_for_spawner = error_count.clone();
        let spawner: Spawner = Arc::new(move |options: ConsumerOptions| {
            let handler = handler_factory();
            let consumer = KafkaConsumer::new(client.clone());
            let options = if concurrent {
                options
            } else {
                ConsumerOptions {
                    prefetch_count: 1,
                    ..options
                }
            };
            let ec = ec_for_spawner.clone();
            let ctx = ctx.clone();

            tokio::spawn(async move {
                let result = consumer.run_with_inner::<T, H>(handler, ctx, options).await;
                if let Err(e) = result {
                    ec.fetch_add(1, Ordering::Relaxed);
                    tracing::error!("consumer task exited with error: {e}");
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
        }
    }

    /// Spawn `min_consumers` consumers.
    pub fn start(&mut self) {
        let target = self.config.min_consumers as usize;
        info!(
            group = %self.queue,
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

    /// Cancel an idle consumer. Returns false at min capacity or all busy.
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

    pub fn config(&self) -> &KafkaConsumerGroupConfig {
        &self.config
    }

    pub async fn shutdown(&mut self) {
        let _ = self.shutdown_with_tally().await;
    }

    pub(crate) async fn shutdown_with_tally(&mut self) -> ShutdownTally {
        info!(group = %self.queue, consumers = self.consumers.len(), "shutting down consumer group");
        self.group_token.cancel();
        let mut panics = 0usize;
        for (_token, _processing, handle) in self.consumers.drain(..) {
            match handle.await {
                Ok(()) => {}
                // Defensive: shutdown is cooperative via `group_token`; no
                // code path currently calls `JoinHandle::abort()` on these
                // handles, so this arm is unreachable today. Mirrors the
                // `ConsumerSupervisor` drain and keeps parity if a future
                // timeout escalation adds `abort_all`.
                Err(e) if e.is_cancelled() => {}
                Err(e) => {
                    tracing::error!(error = %e, group = %self.queue, "consumer task panicked");
                    panics += 1;
                }
            }
        }
        let errors = self.error_count.swap(0, Ordering::Relaxed);
        debug!(group = %self.queue, errors, panics, "consumer group shutdown complete");
        ShutdownTally { errors, panics }
    }

    fn spawn_one(&mut self) {
        let child_token = self.group_token.child_token();
        let processing = Arc::new(AtomicBool::new(false));
        let mut options = ConsumerOptions::defaults_with_shutdown(child_token.clone());
        options.max_retries = self.config.max_retries;
        options.prefetch_count = self.config.prefetch_count;
        options.processing = processing.clone();
        if let Some(timeout) = self.config.handler_timeout {
            options.handler_timeout = Some(timeout);
        }
        if let Some(limit) = self.config.max_pending_per_key {
            options.max_pending_per_key = Some(limit);
        }
        options.max_message_size = self.config.max_message_size;
        options.consumer_group = Some(Arc::from(self.queue.as_str()));
        let handle = (self.spawner)(options);
        self.consumers.push((child_token, processing, handle));
        debug!(group = %self.queue, consumer_index = self.consumers.len() - 1, "spawned consumer");
    }
}

// ---------------------------------------------------------------------------
// KafkaConsumerGroupRegistry
// ---------------------------------------------------------------------------

pub struct KafkaConsumerGroupRegistry {
    pub(crate) groups: HashMap<String, KafkaConsumerGroup>,
    client: Option<KafkaClient>,
}

impl KafkaConsumerGroupRegistry {
    pub fn new(client: KafkaClient) -> Self {
        Self {
            groups: HashMap::new(),
            client: Some(client),
        }
    }

    /// Create a registry from a pre-populated map of groups (for testing).
    #[cfg(test)]
    pub(crate) fn from_groups(groups: HashMap<String, KafkaConsumerGroup>) -> Self {
        Self {
            groups,
            client: None,
        }
    }

    /// Return the client's shutdown token.
    ///
    /// Used by `RegistryImpl::cancellation_token` and `run_until_timeout`
    /// to coordinate graceful shutdown with the broker's lifecycle.
    pub(crate) fn client_shutdown_token(&self) -> CancellationToken {
        self.client
            .as_ref()
            .map(|c| c.shutdown_token())
            .unwrap_or_default()
    }

    pub async fn register<T, H>(
        &mut self,
        config: KafkaConsumerGroupConfig,
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
            crate::metrics::record_backend_error(
                crate::metrics::BackendLabel::Kafka,
                crate::metrics::BackendErrorKind::Topology,
            );
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' is already registered"
            )));
        }

        let client = self.client.as_ref().ok_or_else(|| {
            ShoveError::Topology("registry has no client (test-only registry)".into())
        })?;

        let declarer = KafkaTopologyDeclarer::new(client.clone())
            .with_min_partitions(config.max_consumers as i32);
        declarer.declare(topology).await?;

        info!(group = %name, "registering consumer group");
        let group_token = client.shutdown_token().child_token();
        let group = KafkaConsumerGroup::new::<T, H>(
            name.clone(),
            config,
            client.clone(),
            group_token,
            handler_factory,
            ctx,
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

    pub fn groups(&self) -> &HashMap<String, KafkaConsumerGroup> {
        &self.groups
    }

    pub fn groups_mut(&mut self) -> &mut HashMap<String, KafkaConsumerGroup> {
        &mut self.groups
    }

    pub async fn shutdown_all(&mut self) {
        let _ = self.shutdown_all_with_tally().await;
    }

    pub(crate) async fn shutdown_all_with_tally(&mut self) -> ShutdownTally {
        info!(
            count = self.groups.len(),
            "shutting down all consumer groups"
        );
        let mut tally = ShutdownTally::default();
        for group in self.groups.values_mut() {
            tally.add(group.shutdown_with_tally().await);
        }
        debug!(
            errors = tally.errors,
            panics = tally.panics,
            "all consumer groups shut down"
        );
        tally
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_group(config: KafkaConsumerGroupConfig) -> KafkaConsumerGroup {
        let group_token = CancellationToken::new();
        let spawner: Spawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });

        KafkaConsumerGroup {
            queue: "test-queue".into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
            error_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn default_config() -> KafkaConsumerGroupConfig {
        KafkaConsumerGroupConfig::new(1..=4)
    }

    // -- start --

    #[test]
    fn start_spawns_min_consumers() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(KafkaConsumerGroupConfig::new(3..=5));
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
            let mut group = test_group(KafkaConsumerGroupConfig::new(0..=4));
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
            let mut group = test_group(KafkaConsumerGroupConfig::new(2..=2));
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
            let mut group = test_group(KafkaConsumerGroupConfig::new(0..=3));
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
            let mut group = test_group(KafkaConsumerGroupConfig::new(0..=2));
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
            KafkaConsumerGroupConfig::new(2..=8)
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
        let _ = KafkaConsumerGroupConfig::new(5..=2);
    }
}
