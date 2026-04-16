#![cfg(feature = "rabbitmq")]

use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use tracing::{debug, info, warn};

use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::consumer::RabbitMqConsumer;
use crate::consumer::{Consumer, ConsumerOptions};
use crate::handler::MessageHandler;
use crate::topic::Topic;

/// Type-erased factory that spawns a single consumer task.
///
/// The closure captures the client and receives fully-configured consumer
/// options, returning the `JoinHandle` of the spawned task.
type Spawner = Arc<dyn Fn(ConsumerOptions) -> JoinHandle<()> + Send + Sync>;

/// Configuration that governs the behaviour of a [`ConsumerGroup`].
pub struct ConsumerGroupConfig {
    pub(crate) prefetch_count: u16,
    pub(crate) min_consumers: u16,
    pub(crate) max_consumers: u16,
    pub(crate) max_retries: u32,
    /// Maximum time a handler may spend processing a single message.
    /// If exceeded the message is retried. `None` means no limit.
    pub(crate) handler_timeout: Option<Duration>,
    /// When `true`, each consumer in the group processes up to `prefetch_count`
    /// messages concurrently while preserving in-order acknowledgement.
    pub(crate) concurrent_processing: bool,
    /// Maximum locally buffered messages per sequence key (sequenced consumers).
    pub(crate) max_pending_per_key: Option<usize>,
    /// Maximum allowed message payload size in bytes.
    pub(crate) max_message_size: Option<usize>,
}

impl ConsumerGroupConfig {
    /// Create a new config with the given consumer count range.
    ///
    /// `range` sets `min_consumers..=max_consumers`.
    /// Defaults: `prefetch_count=10`, `max_retries=10`, `handler_timeout=None`.
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
            max_message_size: Some(crate::consumer::DEFAULT_MAX_MESSAGE_SIZE),
        }
    }

    /// Set the prefetch count (number of unacknowledged messages per consumer).
    pub fn with_prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }

    /// Set the maximum number of retries before a message is dead-lettered.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the maximum time a handler may spend processing a single message.
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        self.handler_timeout = Some(timeout);
        self
    }

    /// Returns the configured prefetch count.
    pub fn prefetch_count(&self) -> u16 {
        self.prefetch_count
    }

    /// Returns the minimum number of consumers.
    pub fn min_consumers(&self) -> u16 {
        self.min_consumers
    }

    /// Returns the maximum number of consumers.
    pub fn max_consumers(&self) -> u16 {
        self.max_consumers
    }

    /// Returns the maximum number of retries.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Returns the handler timeout, if any.
    pub fn handler_timeout(&self) -> Option<Duration> {
        self.handler_timeout
    }

    /// Enable concurrent message processing within each consumer.
    ///
    /// When enabled, each consumer processes up to `prefetch_count` messages
    /// concurrently while preserving in-order acknowledgement.
    ///
    /// Not available for sequenced consumers — consumer groups always use
    /// non-sequenced consumption.
    pub fn with_concurrent_processing(mut self, concurrent: bool) -> Self {
        self.concurrent_processing = concurrent;
        self
    }

    /// Returns whether concurrent processing is enabled.
    pub fn concurrent_processing(&self) -> bool {
        self.concurrent_processing
    }
}

/// A named group of identical consumers all reading from the same queue.
///
/// The group owns the concrete consumers and is responsible for scaling them
/// up and down.  It keeps a [`CancellationToken`] per consumer so that
/// individual consumers can be stopped without affecting the rest of the group.
pub struct ConsumerGroup {
    name: String,
    /// The queue that every consumer in this group reads from (derived from
    /// `T::topology()` at construction time and stored for stats lookups).
    queue: String,
    config: ConsumerGroupConfig,
    spawner: Spawner,
    /// One entry per active consumer: (per-consumer token, processing flag, task handle).
    consumers: Vec<(CancellationToken, Arc<AtomicBool>, JoinHandle<()>)>,
    /// Cancelling this token stops every consumer in the group at once.
    group_token: CancellationToken,
}

impl ConsumerGroup {
    /// Create a new consumer group.
    ///
    /// `handler_factory` is called once per consumer spawn to produce a fresh
    /// handler instance.  The factory is stored inside a type-erased closure
    /// so that the rest of the codebase does not have to carry `T`/`H` type
    /// parameters.
    ///
    /// `queue` must match `T::topology().queue()` — it is stored separately
    /// so the autoscaler can look up queue statistics without the `T` type
    /// parameter.
    pub fn new<T, H>(
        name: impl Into<String>,
        queue: impl Into<String>,
        config: ConsumerGroupConfig,
        client: RabbitMqClient,
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
            let consumer = RabbitMqConsumer::new(client.clone());
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

    /// Spawn `min_consumers` consumers to get the group to its minimum size.
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

    /// Spawn one additional consumer, respecting `max_consumers`.
    ///
    /// Returns `false` when the group is already at maximum capacity.
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

    /// Cancel an idle consumer, respecting `min_consumers`.
    ///
    /// Returns `false` when the group is already at minimum capacity or all
    /// consumers are currently processing a message.
    pub fn scale_down(&mut self) -> bool {
        if self.consumers.len() <= self.config.min_consumers as usize {
            debug!(group = %self.name, min = self.config.min_consumers, "scale_down rejected: at min capacity");
            return false;
        }

        // Find the last idle consumer (prefer removing recently-spawned ones).
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

    /// Number of currently active (spawned) consumers.
    pub fn active_consumers(&self) -> usize {
        self.consumers.len()
    }

    /// The queue name this group reads from.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Access the group's configuration.
    pub fn config(&self) -> &ConsumerGroupConfig {
        &self.config
    }

    /// Cancel every consumer in the group and wait for all tasks to finish.
    pub async fn shutdown(&mut self) {
        info!(group = %self.name, consumers = self.consumers.len(), "shutting down consumer group");
        self.group_token.cancel();
        for (_token, _processing, handle) in self.consumers.drain(..) {
            let _ = handle.await;
        }
        debug!(group = %self.name, "consumer group shutdown complete");
    }

    // ---- private helpers ----

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
            max_message_size: self.config.max_message_size,
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: false,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: 0,
            #[cfg(feature = "nats")]
            max_ack_pending: None,
        };
        let handle = (self.spawner)(options);
        self.consumers.push((child_token, processing, handle));
        debug!(group = %self.name, consumer_index = self.consumers.len() - 1, "spawned consumer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `ConsumerGroup` with a test spawner that simply waits on the
    /// cancellation token (no RabbitMQ connection needed).
    fn test_group(config: ConsumerGroupConfig) -> ConsumerGroup {
        let group_token = CancellationToken::new();
        let spawner: Spawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });

        ConsumerGroup {
            name: "test-group".into(),
            queue: "test-queue".into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
        }
    }

    fn default_config() -> ConsumerGroupConfig {
        ConsumerGroupConfig::new(1..=4)
    }

    // -- start --

    #[test]
    fn start_spawns_min_consumers() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(ConsumerGroupConfig::new(3..=5));
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
            let mut group = test_group(ConsumerGroupConfig::new(0..=4));
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
            let mut group = test_group(ConsumerGroupConfig::new(2..=2));
            group.start();
            assert_eq!(group.active_consumers(), 2);

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
            assert_eq!(group.active_consumers(), 1);

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
            let mut group = test_group(ConsumerGroupConfig::new(0..=3));
            group.scale_up();
            group.scale_up();
            group.scale_up();
            assert_eq!(group.active_consumers(), 3);

            // Mark all consumers as busy.
            for (_, processing, _) in &group.consumers {
                processing.store(true, Ordering::Release);
            }

            assert!(!group.scale_down());
            assert_eq!(group.active_consumers(), 3);
            group.shutdown().await;
        });
    }

    #[test]
    fn scale_down_picks_idle_when_some_busy() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(ConsumerGroupConfig::new(0..=3));
            group.scale_up();
            group.scale_up();
            group.scale_up();
            assert_eq!(group.active_consumers(), 3);

            // Mark first and last as busy, middle one stays idle.
            group.consumers[0].1.store(true, Ordering::Release);
            group.consumers[2].1.store(true, Ordering::Release);

            // Capture the idle consumer's token pointer to verify it was the one removed.
            let idle_token_ptr = Arc::as_ptr(&group.consumers[1].1);

            assert!(group.scale_down());
            assert_eq!(group.active_consumers(), 2);

            // The idle consumer (index 1) should have been removed.
            for (_, processing, _) in &group.consumers {
                assert_ne!(Arc::as_ptr(processing), idle_token_ptr);
            }
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
            let mut group = test_group(ConsumerGroupConfig::new(0..=2));
            group.scale_up();
            group.scale_up();

            // Grab the token of the consumer that will be removed (last idle = index 1).
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
            ConsumerGroupConfig::new(2..=8)
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

    // -- ConsumerGroupConfig constructor validation --

    #[test]
    fn new_with_valid_range() {
        let config = ConsumerGroupConfig::new(2..=8);
        assert_eq!(config.min_consumers(), 2);
        assert_eq!(config.max_consumers(), 8);
    }

    #[test]
    fn new_sets_defaults() {
        let config = ConsumerGroupConfig::new(1..=4);
        assert_eq!(config.prefetch_count(), 10);
        assert_eq!(config.max_retries(), 10);
        assert!(config.handler_timeout().is_none());
    }

    #[test]
    fn new_with_equal_min_max() {
        let config = ConsumerGroupConfig::new(3..=3);
        assert_eq!(config.min_consumers(), 3);
        assert_eq!(config.max_consumers(), 3);
    }

    #[test]
    #[should_panic]
    #[allow(clippy::reversed_empty_ranges)]
    fn new_panics_if_min_greater_than_max() {
        let _ = ConsumerGroupConfig::new(5..=2);
    }

    // -- ConsumerGroupConfig builder methods --

    #[test]
    fn with_prefetch_count_sets_value() {
        let config = ConsumerGroupConfig::new(1..=4).with_prefetch_count(25);
        assert_eq!(config.prefetch_count(), 25);
    }

    #[test]
    fn with_max_retries_sets_value() {
        let config = ConsumerGroupConfig::new(1..=4).with_max_retries(5);
        assert_eq!(config.max_retries(), 5);
    }

    #[test]
    fn with_handler_timeout_sets_value() {
        let config = ConsumerGroupConfig::new(1..=4).with_handler_timeout(Duration::from_secs(60));
        assert_eq!(config.handler_timeout(), Some(Duration::from_secs(60)));
    }

    #[test]
    fn builder_chaining_sets_all_values() {
        let config = ConsumerGroupConfig::new(1..=5)
            .with_prefetch_count(20)
            .with_max_retries(3)
            .with_handler_timeout(Duration::from_secs(30));
        assert_eq!(config.min_consumers(), 1);
        assert_eq!(config.max_consumers(), 5);
        assert_eq!(config.prefetch_count(), 20);
        assert_eq!(config.max_retries(), 3);
        assert_eq!(config.handler_timeout(), Some(Duration::from_secs(30)));
    }

    // -- concurrent_processing --

    #[test]
    fn concurrent_processing_defaults_to_false() {
        let config = ConsumerGroupConfig::new(1..=4);
        assert!(!config.concurrent_processing());
    }

    #[test]
    fn with_concurrent_processing_sets_value() {
        let config = ConsumerGroupConfig::new(1..=4).with_concurrent_processing(true);
        assert!(config.concurrent_processing());
    }

    #[test]
    fn with_concurrent_processing_false_explicit() {
        let config = ConsumerGroupConfig::new(1..=4)
            .with_concurrent_processing(true)
            .with_concurrent_processing(false);
        assert!(!config.concurrent_processing());
    }

    #[test]
    fn builder_chaining_with_concurrent_processing() {
        let config = ConsumerGroupConfig::new(1..=8)
            .with_prefetch_count(20)
            .with_max_retries(3)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true);
        assert_eq!(config.min_consumers(), 1);
        assert_eq!(config.max_consumers(), 8);
        assert_eq!(config.prefetch_count(), 20);
        assert_eq!(config.max_retries(), 3);
        assert_eq!(config.handler_timeout(), Some(Duration::from_secs(30)));
        assert!(config.concurrent_processing());
    }

    // -- spawn_one wiring --

    #[test]
    fn spawned_consumers_start_idle() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(default_config());
            group.scale_up();

            let (_, processing, _) = &group.consumers[0];
            assert!(!processing.load(Ordering::Acquire));
            group.shutdown().await;
        });
    }
}
