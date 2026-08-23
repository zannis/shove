#![cfg(feature = "rabbitmq")]

use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use tracing::{debug, info, warn};

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::backends::rabbitmq::client::RabbitMqClient;
use crate::backends::rabbitmq::consumer::RabbitMqConsumer;
use crate::consumer::{HandlerTimeoutConfig, resolve_handler_timeout};
use crate::consumer_supervisor::{AbortOnDrop, ShutdownTally};
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};
use crate::{DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY};

/// Type-erased factory that spawns a single consumer task.
///
/// The closure captures the client and receives fully-configured consumer
/// options, returning the `JoinHandle` of the spawned task.
type Spawner = Arc<dyn Fn(ConsumerOptions) -> JoinHandle<()> + Send + Sync>;

/// Configuration that governs the behaviour of a [`RabbitMqConsumerGroup`].
#[derive(Clone)]
pub struct RabbitMqConsumerGroupConfig {
    pub(crate) prefetch_count: u16,
    pub(crate) min_consumers: u16,
    pub(crate) max_consumers: u16,
    pub(crate) max_retries: u32,
    /// Maximum time a handler may spend processing a single message.
    /// If exceeded the message is retried. `None` means no limit.
    pub(crate) handler_timeout: HandlerTimeoutConfig,
    /// What a handler timeout resolves to. `None` keeps the backend's
    /// historical default.
    pub(crate) handler_timeout_outcome: Option<crate::Outcome>,
    /// When `true`, each consumer in the group processes up to `prefetch_count`
    /// messages concurrently while preserving in-order acknowledgement.
    pub(crate) concurrent_processing: bool,
    /// Maximum locally buffered messages per sequence key (sequenced consumers).
    pub(crate) max_pending_per_key: Option<usize>,
    /// Maximum allowed message payload size in bytes.
    pub(crate) max_message_size: Option<usize>,
}

impl RabbitMqConsumerGroupConfig {
    /// Create a new config with the given consumer count range.
    ///
    /// `range` sets `min_consumers..=max_consumers`.
    /// Defaults: `prefetch_count=10`, `max_retries=10`, `handler_timeout=30s`.
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
            handler_timeout: HandlerTimeoutConfig::Inherit,
            handler_timeout_outcome: None,
            concurrent_processing: false,
            max_pending_per_key: Some(DEFAULT_MAX_PENDING_PER_KEY),
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
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

    /// Returns the configured handler timeout. A freshly-constructed
    /// config reports `Some(DEFAULT_HANDLER_TIMEOUT)`; a registry-level
    /// default set via `RabbitMqConsumerGroup::with_default_handler_timeout`
    /// is not reflected here because the config does not know about
    /// its registry.
    pub fn handler_timeout(&self) -> Option<Duration> {
        Some(resolve_handler_timeout(self.handler_timeout, None))
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

impl Default for RabbitMqConsumerGroupConfig {
    fn default() -> Self {
        Self::new(1..=4)
    }
}

/// Number of AMQP connections to dial for a consumer group sized
/// `max_consumers`.
///
/// One connection per 50 workers, rounded up, never less than 1.
/// Lapin parses every inbound delivery on every channel through a single
/// reader task; past ~50 channels per connection the socket becomes a
/// serialisation point, so we fan out to multiple connections.
fn pool_size_for(max_consumers: u16) -> usize {
    const CHANNELS_PER_CONN: usize = 50;
    let max = (max_consumers as usize).max(1);
    max.div_ceil(CHANNELS_PER_CONN)
}

/// A named group of identical consumers all reading from the same queue.
///
/// The group owns the concrete consumers and is responsible for scaling them
/// up and down.  It keeps a [`CancellationToken`] per consumer so that
/// individual consumers can be stopped without affecting the rest of the group.
pub struct RabbitMqConsumerGroup {
    name: String,
    /// The queue that every consumer in this group reads from (derived from
    /// `T::topology()` at construction time and stored for stats lookups).
    queue: String,
    config: RabbitMqConsumerGroupConfig,
    spawner: Spawner,
    /// One entry per active consumer: (per-consumer token, processing flag, task handle).
    consumers: Vec<(CancellationToken, Arc<AtomicBool>, JoinHandle<()>)>,
    /// Cancelling this token stops every consumer in the group at once.
    group_token: CancellationToken,
    /// Non-retryable error count incremented by each spawned task when its
    /// inner `run_with_inner` returns `Err`. Drained by
    /// [`RabbitMqConsumerGroup::shutdown_with_tally`].
    error_count: Arc<AtomicUsize>,
    /// Panic count incremented by the FIFO spawner wrapper when a shard task
    /// exits with a `JoinError` that is not a cancellation. Drained by
    /// [`RabbitMqConsumerGroup::shutdown_with_tally`].
    panic_count: Arc<AtomicUsize>,
    /// AMQP connection pool shared across all consumers in this group.
    /// Sized at construction time from `ceil(max_consumers / 50)`.
    pool: Arc<Vec<RabbitMqClient>>,
    /// Handles of consumers removed by `scale_down`. Their tokens are already
    /// cancelled; retained here so the final drain awaits/aborts and tallies
    /// them instead of detaching the task.
    pub(crate) retiring: Vec<JoinHandle<()>>,
}

impl RabbitMqConsumerGroup {
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
    pub async fn new<T, H>(
        name: impl Into<String>,
        queue: impl Into<String>,
        config: RabbitMqConsumerGroupConfig,
        client: RabbitMqClient,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<Self>
    where
        T: Topic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let concurrent = config.concurrent_processing;
        let error_count = Arc::new(AtomicUsize::new(0));
        let ec_for_spawner = error_count.clone();

        // Build the connection pool. Slot 0 is the caller-supplied client;
        // slots 1..pool_size are freshly dialed siblings.
        //
        // Partial-failure semantics: if `dial_sibling()` errors on iteration K
        // (1..pool_size), `pool_vec` (holding K successfully dialed clients)
        // is dropped via `?` returning Err. Lapin's `Connection::Drop` closes
        // the underlying TCP socket — that's a hard close (TCP RST), not a
        // graceful AMQP `Connection.Close` handshake. Acceptable: this only
        // happens on a fatal registration error where the broker is already
        // misbehaving, and the broker recovers via TCP timeout on its side.
        let pool_size = pool_size_for(config.max_consumers);
        let mut pool_vec: Vec<RabbitMqClient> = Vec::with_capacity(pool_size);
        pool_vec.push(client);
        for _ in 1..pool_size {
            pool_vec.push(pool_vec[0].dial_sibling().await?);
        }
        let pool: Arc<Vec<RabbitMqClient>> = Arc::new(pool_vec);
        let next: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        let pool_for_spawner = pool.clone();
        let spawner: Spawner = Arc::new(move |options: ConsumerOptions| {
            let idx = next.fetch_add(1, Ordering::Relaxed) % pool_for_spawner.len();
            let handler = handler_factory();
            let consumer = RabbitMqConsumer::new(pool_for_spawner[idx].clone());
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

        Ok(Self {
            name: name.into(),
            queue: queue.into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
            error_count,
            panic_count: Arc::new(AtomicUsize::new(0)),
            pool,
            retiring: Vec::new(),
        })
    }

    /// Construct a FIFO consumer group for a [`SequencedTopic`].
    ///
    /// FIFO replica count is fixed at 1 — concurrency comes from shards,
    /// not from multiple replicas of the shard set.
    pub async fn new_fifo<T, H>(
        queue: impl Into<String>,
        client: RabbitMqClient,
        mut config: RabbitMqConsumerGroupConfig,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<Self>
    where
        T: SequencedTopic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let error_count = Arc::new(AtomicUsize::new(0));
        let panic_count = Arc::new(AtomicUsize::new(0));
        let ec_for_spawner = error_count.clone();
        let pc_for_spawner = panic_count.clone();

        // FIFO replica count is fixed at 1 — FIFO concurrency is per-shard,
        // not per-replica.
        config.min_consumers = 1;
        config.max_consumers = 1;

        // FIFO groups always sit at max_consumers=1, so pool_size is 1 and no
        // siblings are dialed. The pool plumbing is still applied uniformly
        // so the spawner code path stays identical to the non-FIFO group.
        let pool_size = pool_size_for(config.max_consumers);
        let mut pool_vec: Vec<RabbitMqClient> = Vec::with_capacity(pool_size);
        pool_vec.push(client);
        for _ in 1..pool_size {
            pool_vec.push(pool_vec[0].dial_sibling().await?);
        }
        let pool: Arc<Vec<RabbitMqClient>> = Arc::new(pool_vec);
        let next: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        let pool_for_spawner = pool.clone();
        let spawner: Spawner = Arc::new(move |options: ConsumerOptions| {
            let idx = next.fetch_add(1, Ordering::Relaxed) % pool_for_spawner.len();
            let handler = handler_factory();
            let consumer = RabbitMqConsumer::new(pool_for_spawner[idx].clone());
            let ec = ec_for_spawner.clone();
            let pc = pc_for_spawner.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let handles = match consumer.spawn_fifo_shards::<T, H>(handler, ctx, options) {
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

        let queue_str: String = queue.into();
        Ok(Self {
            name: queue_str.clone(),
            queue: queue_str,
            consumers: Vec::with_capacity(1),
            config,
            spawner,
            group_token,
            error_count,
            panic_count,
            pool,
            retiring: Vec::new(),
        })
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

        let (token, _, handle) = self.consumers.swap_remove(index);
        token.cancel();
        self.retiring.push(handle);

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

    #[cfg(test)]
    pub(crate) fn retiring_is_empty(&self) -> bool {
        self.retiring.is_empty()
    }

    /// The queue name this group reads from.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Access the group's configuration.
    pub fn config(&self) -> &RabbitMqConsumerGroupConfig {
        &self.config
    }

    /// Number of AMQP connections backing this group's consumer pool.
    ///
    /// Sized at construction time from `ceil(max_consumers / 50)`. Exposed
    /// publicly so integration tests can verify the pool spans multiple
    /// connections at high consumer counts. Production code should not
    /// depend on this value.
    #[doc(hidden)]
    pub fn pool_len(&self) -> usize {
        self.pool.len()
    }

    /// Cancel every consumer in the group and wait for all tasks to finish.
    pub async fn shutdown(&mut self) {
        let _ = self.shutdown_with_tally().await;
    }

    pub(crate) async fn shutdown_with_tally(&mut self) -> ShutdownTally {
        let mut tally = ShutdownTally::default();
        self.drain_into(&mut tally).await;
        debug!(
            group = %self.name,
            errors = tally.errors,
            panics = tally.panics,
            "consumer group shutdown complete"
        );
        tally
    }

    /// Cancel the group token and await every consumer handle, accumulating
    /// errors and panics into the caller-owned `tally`.
    ///
    /// Critically, the error/panic atomics are swapped into `tally` **before**
    /// any handle is awaited, so a caller that races this future against a
    /// timeout (see `RegistryImpl::run_until_timeout`) still observes the
    /// pre-cancel tally even if the drain future is dropped mid-await.
    ///
    /// The consumer list is drained via `pop()` rather than `drain(..)` so
    /// that dropping this future leaves any unawaited handles in place — the
    /// caller can then escalate via [`Self::abort_remaining_into`].
    pub(crate) async fn drain_into(&mut self, tally: &mut ShutdownTally) {
        info!(
            group = %self.name,
            consumers = self.consumers.len(),
            "shutting down consumer group"
        );
        self.group_token.cancel();

        // Capture accumulated counts up front so a dropped future preserves
        // them. A trailing swap below picks up anything that increments
        // between this point and full task termination.
        tally.errors += self.error_count.swap(0, Ordering::Relaxed);
        tally.panics += self.panic_count.swap(0, Ordering::Relaxed);

        while let Some((_token, _processing, handle)) = self.consumers.pop() {
            let _abort_guard = AbortOnDrop(handle.abort_handle());
            match handle.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => {
                    tracing::error!(error = %e, group = %self.name, "consumer task panicked");
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
                    tracing::error!(error = %e, group = %self.name, "retired consumer task panicked");
                    tally.panics += 1;
                }
            }
        }

        tally.errors += self.error_count.swap(0, Ordering::Relaxed);
        tally.panics += self.panic_count.swap(0, Ordering::Relaxed);
    }

    /// Abort every surviving consumer handle and tally the result.
    ///
    /// Used by `RegistryImpl::run_until_timeout` after a cooperative drain
    /// times out. Mirrors `ConsumerSupervisor::run_until_timeout`'s
    /// `abort_all` + drain escalation so the returned `SupervisorOutcome`
    /// reflects errors and panics counted before the deadline.
    pub(crate) async fn abort_remaining_into(&mut self, tally: &mut ShutdownTally) {
        // Idempotent: token may already be cancelled by `drain_into`.
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
                        group = %self.name,
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
                    tracing::error!(error = %e, group = %self.name, "retired consumer task panicked");
                    tally.panics += 1;
                }
            }
        }
        tally.errors += self.error_count.swap(0, Ordering::Relaxed);
        tally.panics += self.panic_count.swap(0, Ordering::Relaxed);
    }

    // ---- private helpers ----

    fn spawn_one(&mut self) {
        let child_token = self.group_token.child_token();
        let processing = Arc::new(AtomicBool::new(false));
        let mut options = ConsumerOptions::defaults_with_shutdown(child_token.clone());
        options.max_retries = self.config.max_retries;
        options.prefetch_count = self.config.prefetch_count;
        options.processing = processing.clone();
        options.handler_timeout = Some(resolve_handler_timeout(self.config.handler_timeout, None));
        options.handler_timeout_outcome = self.config.handler_timeout_outcome.clone();
        options.max_pending_per_key = self.config.max_pending_per_key;
        options.max_message_size = self.config.max_message_size;
        options.consumer_group = Some(Arc::from(self.name.as_str()));
        let handle = (self.spawner)(options);
        self.consumers.push((child_token, processing, handle));
        debug!(group = %self.name, consumer_index = self.consumers.len() - 1, "spawned consumer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::DEFAULT_HANDLER_TIMEOUT;

    /// Build a `RabbitMqConsumerGroup` with a test spawner that simply waits on the
    /// cancellation token (no RabbitMQ connection needed).
    fn test_group(config: RabbitMqConsumerGroupConfig) -> RabbitMqConsumerGroup {
        let group_token = CancellationToken::new();
        let spawner: Spawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });

        RabbitMqConsumerGroup {
            name: "test-group".into(),
            queue: "test-queue".into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
            error_count: Arc::new(AtomicUsize::new(0)),
            panic_count: Arc::new(AtomicUsize::new(0)),
            pool: Arc::new(Vec::new()),
            retiring: Vec::new(),
        }
    }

    fn default_config() -> RabbitMqConsumerGroupConfig {
        RabbitMqConsumerGroupConfig::new(1..=4)
    }

    // -- start --

    #[test]
    fn start_spawns_min_consumers() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut group = test_group(RabbitMqConsumerGroupConfig::new(3..=5));
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
            let mut group = test_group(RabbitMqConsumerGroupConfig::new(0..=4));
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
            let mut group = test_group(RabbitMqConsumerGroupConfig::new(2..=2));
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
            let mut group = test_group(RabbitMqConsumerGroupConfig::new(0..=3));
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
            let mut group = test_group(RabbitMqConsumerGroupConfig::new(0..=3));
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
            let mut group = test_group(RabbitMqConsumerGroupConfig::new(0..=2));
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

    #[tokio::test]
    async fn scale_down_handle_is_drained_not_detached() {
        // A scaled-down consumer that hangs must still be aborted and counted,
        // not silently detached. Use a hanging spawner so the retiring handle
        // would leak under the old drop-the-handle behaviour.
        let mut group = hanging_test_group(RabbitMqConsumerGroupConfig::new(1..=3));
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
        assert_eq!(tally.panics, 0);
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

    /// Build a `RabbitMqConsumerGroup` whose spawned consumers ignore cancellation,
    /// so `drain_into` can only progress via the abort escalation path.
    fn hanging_test_group(config: RabbitMqConsumerGroupConfig) -> RabbitMqConsumerGroup {
        let mut group = test_group(config);
        group.spawner = Arc::new(|_options: ConsumerOptions| {
            tokio::spawn(async {
                std::future::pending::<()>().await;
            })
        });
        group
    }

    #[tokio::test]
    async fn drain_into_clean_shutdown_captures_atomics_into_tally() {
        let mut group = test_group(default_config());
        group.start();
        group.scale_up();

        group.error_count.store(3, Ordering::Relaxed);
        group.panic_count.store(1, Ordering::Relaxed);

        let mut tally = ShutdownTally::default();
        group.drain_into(&mut tally).await;

        assert_eq!(group.active_consumers(), 0);
        assert_eq!(tally.errors, 3);
        assert_eq!(tally.panics, 1);
    }

    #[tokio::test]
    async fn drain_into_timeout_preserves_atomics_in_tally() {
        // Regression: previously, a drain timeout discarded any tally
        // accumulated by `shutdown_all_with_tally` because the future was
        // simply dropped. The fix lifts the tally outside the timeout so
        // pre-cancel error / panic counts survive even when consumers hang.
        let mut group = hanging_test_group(RabbitMqConsumerGroupConfig::new(2..=2));
        group.start();
        assert_eq!(group.active_consumers(), 2);

        group.error_count.store(7, Ordering::Relaxed);
        group.panic_count.store(2, Ordering::Relaxed);

        let mut tally = ShutdownTally::default();
        let result =
            tokio::time::timeout(Duration::from_millis(50), group.drain_into(&mut tally)).await;
        assert!(result.is_err(), "drain must time out on hanging consumers");

        assert_eq!(
            tally.errors, 7,
            "drain_into must capture error_count into the tally before awaiting handles"
        );
        assert_eq!(
            tally.panics, 2,
            "drain_into must capture panic_count into the tally before awaiting handles"
        );
    }

    #[tokio::test]
    async fn abort_remaining_into_kills_hanging_consumers_and_keeps_tally() {
        let mut group = hanging_test_group(RabbitMqConsumerGroupConfig::new(2..=2));
        group.start();
        assert_eq!(group.active_consumers(), 2);

        group.error_count.store(5, Ordering::Relaxed);
        group.panic_count.store(1, Ordering::Relaxed);

        let mut tally = ShutdownTally::default();
        let _ = tokio::time::timeout(Duration::from_millis(50), group.drain_into(&mut tally)).await;

        // Tally is captured already; abort the survivors and confirm the
        // tally is preserved (not zeroed) and the Vec is drained.
        group.abort_remaining_into(&mut tally).await;

        assert_eq!(group.active_consumers(), 0);
        assert_eq!(tally.errors, 5);
        assert_eq!(tally.panics, 1);
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
            RabbitMqConsumerGroupConfig::new(2..=8)
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

    // -- RabbitMqConsumerGroupConfig constructor validation --

    #[test]
    fn new_with_valid_range() {
        let config = RabbitMqConsumerGroupConfig::new(2..=8);
        assert_eq!(config.min_consumers(), 2);
        assert_eq!(config.max_consumers(), 8);
    }

    #[test]
    fn new_sets_defaults() {
        let config = RabbitMqConsumerGroupConfig::new(1..=4);
        assert_eq!(config.prefetch_count(), 10);
        assert_eq!(config.max_retries(), 10);
        assert_eq!(config.handler_timeout(), Some(DEFAULT_HANDLER_TIMEOUT));
    }

    #[test]
    fn new_with_equal_min_max() {
        let config = RabbitMqConsumerGroupConfig::new(3..=3);
        assert_eq!(config.min_consumers(), 3);
        assert_eq!(config.max_consumers(), 3);
    }

    #[test]
    #[should_panic]
    #[allow(clippy::reversed_empty_ranges)]
    fn new_panics_if_min_greater_than_max() {
        let _ = RabbitMqConsumerGroupConfig::new(5..=2);
    }

    // -- RabbitMqConsumerGroupConfig builder methods --

    #[test]
    fn with_prefetch_count_sets_value() {
        let config = RabbitMqConsumerGroupConfig::new(1..=4).with_prefetch_count(25);
        assert_eq!(config.prefetch_count(), 25);
    }

    #[test]
    fn with_max_retries_sets_value() {
        let config = RabbitMqConsumerGroupConfig::new(1..=4).with_max_retries(5);
        assert_eq!(config.max_retries(), 5);
    }

    #[test]
    fn with_handler_timeout_sets_value() {
        let config =
            RabbitMqConsumerGroupConfig::new(1..=4).with_handler_timeout(Duration::from_secs(60));
        assert_eq!(config.handler_timeout(), Some(Duration::from_secs(60)));
    }

    #[test]
    fn builder_chaining_sets_all_values() {
        let config = RabbitMqConsumerGroupConfig::new(1..=5)
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
        let config = RabbitMqConsumerGroupConfig::new(1..=4);
        assert!(!config.concurrent_processing());
    }

    #[test]
    fn with_concurrent_processing_sets_value() {
        let config = RabbitMqConsumerGroupConfig::new(1..=4).with_concurrent_processing(true);
        assert!(config.concurrent_processing());
    }

    #[test]
    fn with_concurrent_processing_false_explicit() {
        let config = RabbitMqConsumerGroupConfig::new(1..=4)
            .with_concurrent_processing(true)
            .with_concurrent_processing(false);
        assert!(!config.concurrent_processing());
    }

    #[test]
    fn builder_chaining_with_concurrent_processing() {
        let config = RabbitMqConsumerGroupConfig::new(1..=8)
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

    // -- HandlerTimeoutConfig resolution --

    #[test]
    fn inherit_config_uses_library_default_with_no_registry_default() {
        let cfg = RabbitMqConsumerGroupConfig::new(1..=4);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, None),
            DEFAULT_HANDLER_TIMEOUT,
        );
    }

    #[test]
    fn inherit_config_uses_registry_default_when_set() {
        let cfg = RabbitMqConsumerGroupConfig::new(1..=4);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(45),
        );
    }

    #[test]
    fn with_handler_timeout_beats_registry_default() {
        let cfg =
            RabbitMqConsumerGroupConfig::new(1..=4).with_handler_timeout(Duration::from_secs(5));
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(5),
        );
    }

    #[test]
    #[should_panic(expected = "handler_timeout must be positive")]
    fn with_handler_timeout_zero_panics() {
        let _ = RabbitMqConsumerGroupConfig::new(1..=4).with_handler_timeout(Duration::ZERO);
    }

    // -- pool_size_for --

    #[test]
    fn pool_size_for_clamps_below_to_one() {
        assert_eq!(pool_size_for(0), 1);
        assert_eq!(pool_size_for(1), 1);
        assert_eq!(pool_size_for(50), 1);
    }

    #[test]
    fn pool_size_for_rolls_to_two_at_fifty_one() {
        assert_eq!(pool_size_for(51), 2);
        assert_eq!(pool_size_for(100), 2);
    }

    #[test]
    fn pool_size_for_scales_linearly() {
        assert_eq!(pool_size_for(101), 3);
        assert_eq!(pool_size_for(200), 4);
        assert_eq!(pool_size_for(800), 16);
    }

    #[test]
    fn pool_size_for_does_not_overflow_at_u16_max() {
        // u16::MAX = 65_535; ceil(65535 / 50) = 1311
        assert_eq!(pool_size_for(u16::MAX), 1311);
    }
}
