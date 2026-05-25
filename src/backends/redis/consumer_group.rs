//! Redis Streams consumer-group registry.
//!
//! [`RedisConsumerGroupRegistry`] owns a collection of lazily-constructed task
//! factories (closures returning boxed futures). Calling [`start_all`] spawns
//! every registered consumer into a [`JoinSet`], and [`run_until_timeout`]
//! drives the set to completion or a configurable drain deadline.

use std::future::Future;
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
use crate::consumer_supervisor::{ShutdownTally, SupervisorOutcome};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

use crate::backend::consumer::ConsumerImpl;

use super::client::RedisClient;
use super::consumer::RedisConsumer;
use super::reaper::spawn_reaper;
use super::topology::RedisTopologyDeclarer;

// ---------------------------------------------------------------------------
// Spawner type alias — captures `T`, `H`, the client, and the handler factory
// so `RedisConsumerGroup::scale_up` can launch a fresh consumer on demand.
// ---------------------------------------------------------------------------

pub(crate) type Spawner = Arc<dyn Fn(ConsumerOptionsInner) -> JoinHandle<()> + Send + Sync>;

// ---------------------------------------------------------------------------
// RedisConsumerGroupConfig
// ---------------------------------------------------------------------------

/// Configuration for a [`RedisConsumerGroupRegistry`] registration.
///
/// The consumer count range matches the other coordinated-group backends.
/// The registry spawns `min_consumers` consumer tasks at start; the
/// `max_consumers` ceiling will be honoured by a future autoscaler (Redis
/// has no scaling loop wired today, so `min_consumers` is the effective
/// in-flight count). Users who want a fixed `N` consumers pass
/// `N..=N`. FIFO topics always spawn one task per shard regardless of
/// this setting.
#[derive(Debug, Clone)]
pub struct RedisConsumerGroupConfig {
    prefetch_count: u16,
    max_retries: u32,
    pub(crate) min_consumers: u16,
    pub(crate) max_consumers: u16,
    concurrent_processing: bool,
    pub(crate) handler_timeout: HandlerTimeoutConfig,
    /// Maximum locally buffered messages per sequence key (sequenced consumers).
    max_pending_per_key: Option<usize>,
    /// Maximum allowed message payload size in bytes.
    max_message_size: Option<usize>,
}

impl RedisConsumerGroupConfig {
    /// Create a new config with the given consumer-count range.
    ///
    /// `range` sets `min_consumers..=max_consumers`. Both ends are clamped to
    /// at least 1.
    ///
    /// # Panics
    ///
    /// Panics if `*range.start() > *range.end()`.
    pub fn new(range: RangeInclusive<u16>) -> Self {
        let min = (*range.start()).max(1);
        let max = (*range.end()).max(1);
        assert!(
            min <= max,
            "min_consumers ({min}) must be <= max_consumers ({max})"
        );
        Self {
            prefetch_count: 10,
            max_retries: 10,
            min_consumers: min,
            max_consumers: max,
            concurrent_processing: false,
            handler_timeout: HandlerTimeoutConfig::Inherit,
            max_pending_per_key: Some(DEFAULT_MAX_PENDING_PER_KEY),
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
        }
    }

    /// Set the prefetch count (the `COUNT` argument to XREADGROUP). With
    /// `concurrent_processing(true)` this is the upper bound on in-flight
    /// handlers per consumer task; with `concurrent_processing(false)` the
    /// effective prefetch is clamped to 1 so handlers serialize regardless.
    pub fn with_prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }

    /// Set the maximum number of retries before a message is dead-lettered.
    /// Defaults to `10` (matches [`crate::ConsumerOptions::new`]).
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Enable concurrent message processing within each consumer.
    ///
    /// When enabled, each consumer processes up to `prefetch_count` messages
    /// concurrently. Handlers are dispatched as independent tokio tasks and
    /// each routes its own outcome (XACK / hold-queue / DLQ).
    ///
    /// Not available for sequenced topics — [`register_fifo`] rejects configs
    /// with this flag set to preserve FIFO ordering within a shard.
    ///
    /// [`register_fifo`]: RedisConsumerGroupRegistry::register_fifo
    pub fn with_concurrent_processing(mut self, concurrent: bool) -> Self {
        self.concurrent_processing = concurrent;
        self
    }

    /// Set the maximum time a handler may spend processing a single
    /// message. If exceeded, the message is left in the PEL for XAUTOCLAIM.
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "handler_timeout must be positive");
        self.handler_timeout = HandlerTimeoutConfig::Set(timeout);
        self
    }

    /// Set the maximum number of locally buffered messages per sequence key.
    /// When exceeded, new deliveries for that key are rejected to the DLQ.
    pub fn with_max_pending_per_key(mut self, limit: usize) -> Self {
        self.max_pending_per_key = Some(limit);
        self
    }

    /// Set the maximum allowed message payload size in bytes.
    /// Messages exceeding this limit are rejected before deserialization.
    pub fn with_max_message_size(mut self, max: usize) -> Self {
        self.max_message_size = Some(max);
        self
    }

    /// Returns the configured prefetch count.
    pub fn prefetch_count(&self) -> u16 {
        self.prefetch_count
    }

    /// Returns the configured max retries before dead-lettering.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Returns the minimum number of consumers.
    pub fn min_consumers(&self) -> u16 {
        self.min_consumers
    }

    /// Returns the maximum number of consumers. Currently used only as the
    /// upper bound for a future autoscaler — the registry spawns
    /// `min_consumers` tasks at start.
    pub fn max_consumers(&self) -> u16 {
        self.max_consumers
    }

    /// Returns whether concurrent processing is enabled.
    pub fn concurrent_processing(&self) -> bool {
        self.concurrent_processing
    }

    /// Returns the configured handler timeout. A freshly-constructed
    /// config reports `Some(DEFAULT_HANDLER_TIMEOUT)`; a registry-level
    /// default is not reflected here because the config does not know
    /// about its registry.
    pub fn handler_timeout(&self) -> Option<Duration> {
        Some(resolve_handler_timeout(self.handler_timeout, None))
    }

    /// Returns the configured per-key pending buffer limit.
    pub fn max_pending_per_key(&self) -> Option<usize> {
        self.max_pending_per_key
    }

    /// Returns the configured maximum message payload size in bytes.
    pub fn max_message_size(&self) -> Option<usize> {
        self.max_message_size
    }
}

impl Default for RedisConsumerGroupConfig {
    fn default() -> Self {
        Self::new(1..=4)
    }
}

// ---------------------------------------------------------------------------
// RedisConsumerGroup
// ---------------------------------------------------------------------------

/// A single named consumer group inside the Redis registry. Holds one
/// `Spawner` closure (capturing the topic, handler factory, and client) plus
/// the live consumer tasks. `scale_up`/`scale_down` invoke the spawner or
/// cancel the chosen consumer's child token.
pub struct RedisConsumerGroup {
    pub(crate) queue: String,
    pub(crate) config: RedisConsumerGroupConfig,
    pub(crate) spawner: Spawner,
    pub(crate) consumers: Vec<(CancellationToken, Arc<AtomicBool>, JoinHandle<()>)>,
    pub(crate) group_token: CancellationToken,
    pub(crate) error_count: Arc<AtomicUsize>,
    pub(crate) panic_count: Arc<AtomicUsize>,
    /// Factory for the per-group XAUTOCLAIM sidecar. Built once in `new` /
    /// `new_fifo` so `start` can spawn the reaper without re-capturing
    /// topology data.
    pub(crate) reaper_factory: ReaperFactory,
    /// Handle to the reaper task spawned on `start`. `None` until `start` is
    /// called, taken back to `None` by `shutdown_with_tally`.
    pub(crate) reaper_handle: Option<JoinHandle<()>>,
}

/// Type-erased "spawn me a reaper" closure. Captures the streams + group +
/// timing knobs needed to call [`super::reaper::spawn_reaper`].
pub(crate) type ReaperFactory = Arc<dyn Fn() -> JoinHandle<()> + Send + Sync>;

/// Build a [`ReaperFactory`] for the given streams + group. Used by both
/// `RedisConsumerGroup::new` (single stream) and `::new_fifo` (one stream
/// per shard).
///
/// The reaper interval derives from the group's resolved `handler_timeout`,
/// floored at 30 s — matches what the per-consumer autoclaim loop used to
/// pick. `min_idle_ms` (the XAUTOCLAIM idle threshold) is set to the same
/// value, so an entry must have been pending at least one handler-timeout
/// before the reaper reclaims it.
fn build_reaper_factory(
    client: &RedisClient,
    streams: Vec<String>,
    config: &RedisConsumerGroupConfig,
    group_token: &CancellationToken,
) -> ReaperFactory {
    let group = client.group().to_string();
    let handler_timeout = resolve_handler_timeout(config.handler_timeout, None);
    let min_idle_ms = handler_timeout.as_millis() as u64;
    let interval = Duration::from_millis(min_idle_ms.max(30_000));
    let client = client.clone();
    let token = group_token.clone();
    Arc::new(move || {
        spawn_reaper(
            client.clone(),
            streams.clone(),
            group.clone(),
            interval,
            min_idle_ms,
            token.clone(),
        )
    })
}

impl RedisConsumerGroup {
    /// Construct a non-FIFO consumer group. Captures `T`, `H`, and the client
    /// in a `Spawner` closure so `start`/`scale_up` can launch fresh consumers
    /// on demand.
    pub fn new<T, H>(
        queue: impl Into<String>,
        client: RedisClient,
        config: RedisConsumerGroupConfig,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Self
    where
        T: Topic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let concurrent = config.concurrent_processing();
        // Non-concurrent groups clamp prefetch to 1 so each XREADGROUP fetches
        // at most one message, matching the other backends' "concurrent off"
        // semantics. Concurrent groups use the configured value as the
        // in-flight cap (semaphore size).
        let effective_prefetch = if concurrent {
            config.prefetch_count().max(1)
        } else {
            1
        };

        let error_count = Arc::new(AtomicUsize::new(0));
        let ec_for_spawner = error_count.clone();
        let client_for_spawner = client.clone();
        let ctx_for_spawner = ctx;
        let spawner: Spawner = Arc::new(move |mut options: ConsumerOptionsInner| {
            let handler = handler_factory();
            let consumer = RedisConsumer::new(client_for_spawner.clone());
            let ctx = ctx_for_spawner.clone();
            let ec = ec_for_spawner.clone();
            // Pin prefetch to the group-derived value regardless of caller defaults.
            options.prefetch_count = effective_prefetch;
            tokio::spawn(async move {
                let result = if concurrent {
                    consumer.run_concurrent::<T, H>(handler, ctx, options).await
                } else {
                    <RedisConsumer as ConsumerImpl>::run::<T, H>(&consumer, handler, ctx, options)
                        .await
                };
                if let Err(e) = result {
                    ec.fetch_add(1, Ordering::Relaxed);
                    tracing::error!("consumer task exited with error: {e}");
                }
            })
        });

        let reaper_factory = build_reaper_factory(
            &client,
            vec![T::topology().queue().to_string()],
            &config,
            &group_token,
        );

        Self {
            queue: queue.into(),
            consumers: Vec::with_capacity(config.max_consumers() as usize),
            config,
            spawner,
            group_token,
            error_count,
            panic_count: Arc::new(AtomicUsize::new(0)),
            reaper_factory,
            reaper_handle: None,
        }
    }

    /// Construct a FIFO consumer group for a [`SequencedTopic`].
    ///
    /// FIFO replica count is pinned to 1 — FIFO concurrency comes from
    /// shards, not from multiple replicas of the shard set.
    pub fn new_fifo<T, H>(
        queue: impl Into<String>,
        client: RedisClient,
        mut config: RedisConsumerGroupConfig,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Self
    where
        T: SequencedTopic + 'static,
        H: MessageHandler<T> + 'static,
    {
        config.min_consumers = 1;
        config.max_consumers = 1;

        let prefetch = config.prefetch_count().max(1);
        let error_count = Arc::new(AtomicUsize::new(0));
        let panic_count = Arc::new(AtomicUsize::new(0));
        let ec_for_spawner = error_count.clone();
        let pc_for_spawner = panic_count.clone();
        let client_for_spawner = client.clone();
        let ctx_for_spawner = ctx;

        let spawner: Spawner = Arc::new(move |mut options: ConsumerOptionsInner| {
            let handler = handler_factory();
            let consumer = RedisConsumer::new(client_for_spawner.clone());
            let ctx = ctx_for_spawner.clone();
            let ec = ec_for_spawner.clone();
            let pc = pc_for_spawner.clone();
            options.prefetch_count = prefetch;
            tokio::spawn(async move {
                let handles = match <RedisConsumer as ConsumerImpl>::spawn_fifo_shards::<T, H>(
                    &consumer, handler, ctx, options,
                )
                .await
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

        // FIFO topics own one stream per shard — pass all shard names so the
        // reaper sweeps them as a group.
        let topology = T::topology();
        let n_shards = topology
            .sequencing()
            .expect("new_fifo requires sequencing config")
            .routing_shards();
        let reaper_streams: Vec<String> = (0..n_shards)
            .map(|idx| RedisTopologyDeclarer::shard_stream_name(topology.queue(), idx))
            .collect();
        let reaper_factory = build_reaper_factory(&client, reaper_streams, &config, &group_token);

        Self {
            queue: queue.into(),
            consumers: Vec::with_capacity(1),
            config,
            spawner,
            group_token,
            error_count,
            panic_count,
            reaper_factory,
            reaper_handle: None,
        }
    }

    /// Spawn `min_consumers` consumer tasks and the per-group reaper sidecar.
    pub fn start(&mut self) {
        let target = self.config.min_consumers() as usize;
        info!(
            group = %self.queue,
            queue = %self.queue,
            initial_consumers = target,
            "starting consumer group"
        );
        for _ in 0..target {
            self.spawn_one();
        }
        if self.reaper_handle.is_none() {
            self.reaper_handle = Some((self.reaper_factory)());
        }
    }

    /// Spawn one additional consumer. Returns `false` at max capacity.
    pub fn scale_up(&mut self) -> bool {
        if self.consumers.len() >= self.config.max_consumers() as usize {
            debug!(group = %self.queue, max = self.config.max_consumers(), "scale_up rejected: at max capacity");
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

    /// Cancel an idle consumer. Returns `false` at min capacity or if every
    /// consumer is currently processing a message.
    pub fn scale_down(&mut self) -> bool {
        if self.consumers.len() <= self.config.min_consumers() as usize {
            debug!(group = %self.queue, min = self.config.min_consumers(), "scale_down rejected: at min capacity");
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

    pub fn config(&self) -> &RedisConsumerGroupConfig {
        &self.config
    }

    pub async fn shutdown(&mut self) {
        let _ = self.shutdown_with_tally().await;
    }

    pub(crate) async fn shutdown_with_tally(&mut self) -> ShutdownTally {
        let mut tally = ShutdownTally::default();
        self.drain_into(&mut tally).await;
        debug!(
            group = %self.queue,
            errors = tally.errors,
            panics = tally.panics,
            "consumer group shutdown complete"
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
            "shutting down consumer group"
        );
        self.group_token.cancel();

        tally.errors += self.error_count.swap(0, Ordering::Relaxed);
        tally.panics += self.panic_count.swap(0, Ordering::Relaxed);

        while let Some((_token, _processing, handle)) = self.consumers.pop() {
            match handle.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => {
                    tracing::error!(error = %e, group = %self.queue, "consumer task panicked");
                    tally.panics += 1;
                }
            }
        }
        // The reaper shares `group_token`, so the cancel above already signals
        // it to exit. Await its JoinHandle so we don't return before the
        // sidecar is gone.
        if let Some(h) = self.reaper_handle.take() {
            let _ = h.await;
        }

        tally.errors += self.error_count.swap(0, Ordering::Relaxed);
        tally.panics += self.panic_count.swap(0, Ordering::Relaxed);
    }

    /// Abort surviving consumer handles (and the reaper) after a drain
    /// timeout, accumulating any results into `tally`.
    pub(crate) async fn abort_remaining_into(&mut self, tally: &mut ShutdownTally) {
        self.group_token.cancel();
        for (_token, _processing, handle) in &self.consumers {
            handle.abort();
        }
        if let Some(h) = &self.reaper_handle {
            h.abort();
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
        if let Some(h) = self.reaper_handle.take() {
            let _ = h.await;
        }
        tally.errors += self.error_count.swap(0, Ordering::Relaxed);
        tally.panics += self.panic_count.swap(0, Ordering::Relaxed);
    }

    fn spawn_one(&mut self) {
        let child_token = self.group_token.child_token();
        let processing = Arc::new(AtomicBool::new(false));
        let mut options = ConsumerOptionsInner::defaults_with_shutdown(child_token.clone());
        options.prefetch_count = self.config.prefetch_count();
        options.max_retries = self.config.max_retries();
        options.handler_timeout = Some(resolve_handler_timeout(self.config.handler_timeout, None));
        options.processing = processing.clone();
        options.consumer_group = Some(Arc::from(self.queue.as_str()));
        options.max_pending_per_key = self.config.max_pending_per_key();
        options.max_message_size = self.config.max_message_size();

        let handle = (self.spawner)(options);
        self.consumers.push((child_token, processing, handle));
        debug!(group = %self.queue, consumer_index = self.consumers.len() - 1, "spawned consumer");
    }
}

// ---------------------------------------------------------------------------
// RedisConsumerGroupRegistry
// ---------------------------------------------------------------------------

/// Registry that owns one [`RedisConsumerGroup`] per registered topic.
///
/// Typical lifecycle:
/// 1. `let mut reg = RedisConsumerGroupRegistry::new(client);`
/// 2. `reg.register::<T, H>(...).await?;` — one call per topic
/// 3. `reg.run_until_timeout(signal, drain_timeout).await`
pub struct RedisConsumerGroupRegistry {
    pub(crate) groups: std::collections::HashMap<String, RedisConsumerGroup>,
    client: Option<RedisClient>,
    shutdown: CancellationToken,
    pub(super) default_handler_timeout: Option<Duration>,
}

impl RedisConsumerGroupRegistry {
    /// Create a new registry backed by the given Redis client.
    pub fn new(client: RedisClient) -> Self {
        Self {
            groups: std::collections::HashMap::new(),
            client: Some(client),
            shutdown: CancellationToken::new(),
            default_handler_timeout: None,
        }
    }

    /// Create a registry from a pre-populated map of groups (for testing).
    /// The resulting registry cannot be used to call `register()`.
    #[cfg(test)]
    pub(crate) fn from_groups(
        groups: std::collections::HashMap<String, RedisConsumerGroup>,
    ) -> Self {
        Self {
            groups,
            client: None,
            shutdown: CancellationToken::new(),
            default_handler_timeout: None,
        }
    }

    /// Set the registry-level default handler timeout. Applies to every
    /// group whose `RedisConsumerGroupConfig` did not explicitly call
    /// `with_handler_timeout`. Per-group explicit settings always win.
    pub fn with_default_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "default_handler_timeout must be positive"
        );
        self.default_handler_timeout = Some(timeout);
        self
    }

    /// Return the broker-wide shutdown token.
    ///
    /// Cancelling this token propagates shutdown to every consumer task
    /// spawned by this registry.
    pub fn broker_shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Register a non-FIFO topic handler.
    ///
    /// Builds a [`RedisConsumerGroup`] keyed on the topic's queue name. The
    /// group's `min_consumers` tasks are spawned on the next call to
    /// [`start_all`]; the `max_consumers` ceiling is honoured by the Redis
    /// autoscaler ([`crate::redis::RedisAutoscalerBackend`]).
    ///
    /// Topology structures (stream + consumer group) are declared before
    /// returning.
    pub async fn register<T, H>(
        &mut self,
        config: RedisConsumerGroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
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
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' is already registered"
            )));
        }
        let client = self.client.as_ref().ok_or_else(|| {
            ShoveError::Topology("registry has no client (test-only registry)".into())
        })?;
        let declarer = RedisTopologyDeclarer::new(client.clone());
        declarer.declare(topology).await?;

        let group_token = self.shutdown.child_token();
        let group = RedisConsumerGroup::new::<T, H>(
            name.clone(),
            client.clone(),
            config,
            group_token,
            factory,
            ctx,
        );
        self.groups.insert(name, group);
        Ok(())
    }

    /// Register a FIFO (sequenced) topic handler.
    ///
    /// Rejects configs with `concurrent_processing(true)` — concurrent
    /// dispatch within a sequenced shard would break FIFO ordering.
    /// FIFO concurrency comes from shards, not replicas: the group's
    /// replica count is pinned to 1 regardless of the config range.
    pub async fn register_fifo<T, H>(
        &mut self,
        config: RedisConsumerGroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: SequencedTopic + 'static,
        H: MessageHandler<T> + 'static,
    {
        if config.concurrent_processing() {
            return Err(ShoveError::Topology(format!(
                "topic '{}' is sequenced; `concurrent_processing` on a FIFO consumer would \
                 break per-key ordering. Drop `with_concurrent_processing(true)` or use \
                 `register` for unsequenced topics.",
                T::topology().queue(),
            )));
        }

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
        let client = self.client.as_ref().ok_or_else(|| {
            ShoveError::Topology("registry has no client (test-only registry)".into())
        })?;
        let declarer = RedisTopologyDeclarer::new(client.clone());
        declarer.declare(topology).await?;

        let group_token = self.shutdown.child_token();
        let group = RedisConsumerGroup::new_fifo::<T, H>(
            name.clone(),
            client.clone(),
            config,
            group_token,
            factory,
            ctx,
        );
        self.groups.insert(name, group);
        Ok(())
    }

    /// Spawn the initial `min_consumers` per registered group.
    pub fn start_all(&mut self) {
        info!(count = self.groups.len(), "starting all consumer groups");
        for group in self.groups.values_mut() {
            group.start();
        }
    }

    /// Borrow the registered groups by name. Used by
    /// [`crate::redis::RedisAutoscalerBackend`] for `list_groups`/`fetch_metrics`.
    pub fn groups(&self) -> &std::collections::HashMap<String, RedisConsumerGroup> {
        &self.groups
    }

    /// Mutably borrow the registered groups by name. Used by the autoscaler
    /// to invoke `scale_up`/`scale_down`.
    pub fn groups_mut(&mut self) -> &mut std::collections::HashMap<String, RedisConsumerGroup> {
        &mut self.groups
    }

    /// Shut every group down, then drain panics+errors as a tally.
    pub async fn shutdown_all(&mut self) {
        let _ = self.shutdown_all_with_tally().await;
    }

    pub(crate) async fn shutdown_all_with_tally(&mut self) -> ShutdownTally {
        let mut tally = ShutdownTally::default();
        self.drain_all_into(&mut tally).await;
        tally
    }

    /// Drain every consumer group, accumulating errors/panics into `tally`.
    pub(crate) async fn drain_all_into(&mut self, tally: &mut ShutdownTally) {
        info!(
            count = self.groups.len(),
            "shutting down all consumer groups"
        );
        for group in self.groups.values_mut() {
            group.drain_into(tally).await;
        }
        debug!(
            errors = tally.errors,
            panics = tally.panics,
            "all consumer groups shut down"
        );
    }

    /// Abort surviving consumers across every group after a drain timeout.
    pub(crate) async fn abort_all_remaining_into(&mut self, tally: &mut ShutdownTally) {
        for group in self.groups.values_mut() {
            group.abort_remaining_into(tally).await;
        }
    }

    /// Start every registered group, wait for `signal` or the internal
    /// shutdown token, then drain with `drain_timeout`.
    ///
    /// Returns a [`SupervisorOutcome`] summarising errors, panics, and
    /// whether the drain timed out.
    pub async fn run_until_timeout<S>(
        mut self,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        self.start_all();

        let shutdown = self.shutdown.clone();
        let signal_handle = tokio::spawn(signal);
        tokio::select! {
            _ = shutdown.cancelled() => {}
            res = signal_handle => {
                let _ = res;
                shutdown.cancel();
            }
        }

        // Mirror the supervisor pattern in `ConsumerSupervisor::run_until_timeout`:
        // accumulate the tally outside the timeout so a drain-timeout
        // escalation can abort survivors and finish tallying instead of
        // discarding what was already counted.
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::DEFAULT_HANDLER_TIMEOUT;

    #[test]
    fn config_default_handler_timeout_is_library_default() {
        let cfg = RedisConsumerGroupConfig::new(1..=1);
        assert_eq!(cfg.handler_timeout(), Some(DEFAULT_HANDLER_TIMEOUT));
    }

    #[test]
    fn with_handler_timeout_round_trips() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::from_secs(7));
        assert_eq!(cfg.handler_timeout(), Some(Duration::from_secs(7)));
    }

    #[test]
    fn config_inherit_resolves_to_registry_default_when_set() {
        let cfg = RedisConsumerGroupConfig::new(1..=1);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(45),
        );
    }

    #[test]
    fn with_handler_timeout_beats_registry_default() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::from_secs(5));
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(5),
        );
    }

    #[test]
    #[should_panic(expected = "handler_timeout must be positive")]
    fn with_handler_timeout_zero_panics() {
        let _ = RedisConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::ZERO);
    }

    #[test]
    fn config_consumer_range() {
        let cfg = RedisConsumerGroupConfig::new(2..=4);
        assert_eq!(cfg.min_consumers(), 2);
        assert_eq!(cfg.max_consumers(), 4);
    }

    #[test]
    fn config_default_range_is_one_to_four() {
        let cfg = RedisConsumerGroupConfig::default();
        assert_eq!(cfg.min_consumers(), 1);
        assert_eq!(cfg.max_consumers(), 4);
    }

    #[test]
    fn config_zero_clamped_to_one() {
        let cfg = RedisConsumerGroupConfig::new(0..=0);
        assert_eq!(cfg.min_consumers(), 1);
        assert_eq!(cfg.max_consumers(), 1);
    }

    #[test]
    fn config_large_consumer_count() {
        let cfg = RedisConsumerGroupConfig::new(u16::MAX..=u16::MAX);
        assert_eq!(cfg.max_consumers(), u16::MAX);
    }

    #[test]
    fn config_default_prefetch_count_is_ten() {
        let cfg = RedisConsumerGroupConfig::default();
        assert_eq!(cfg.prefetch_count(), 10);
    }

    #[test]
    fn config_default_concurrent_processing_is_false() {
        let cfg = RedisConsumerGroupConfig::default();
        assert!(!cfg.concurrent_processing());
    }

    #[test]
    fn with_prefetch_count_round_trips() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_prefetch_count(64);
        assert_eq!(cfg.prefetch_count(), 64);
    }

    #[test]
    fn default_max_retries_is_ten() {
        let cfg = RedisConsumerGroupConfig::default();
        assert_eq!(cfg.max_retries(), 10);
    }

    #[test]
    fn with_max_retries_round_trips() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_max_retries(7);
        assert_eq!(cfg.max_retries(), 7);
    }

    #[test]
    fn with_concurrent_processing_round_trips() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_concurrent_processing(true);
        assert!(cfg.concurrent_processing());
    }

    #[test]
    fn config_default_max_pending_per_key_is_library_default() {
        use crate::consumer::DEFAULT_MAX_PENDING_PER_KEY;
        let cfg = RedisConsumerGroupConfig::new(1..=1);
        assert_eq!(cfg.max_pending_per_key(), Some(DEFAULT_MAX_PENDING_PER_KEY));
    }

    #[test]
    fn config_default_max_message_size_is_library_default() {
        use crate::consumer::DEFAULT_MAX_MESSAGE_SIZE;
        let cfg = RedisConsumerGroupConfig::new(1..=1);
        assert_eq!(cfg.max_message_size(), Some(DEFAULT_MAX_MESSAGE_SIZE));
    }

    #[test]
    fn with_max_pending_per_key_round_trips() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_max_pending_per_key(500);
        assert_eq!(cfg.max_pending_per_key(), Some(500));
    }

    #[test]
    fn with_max_message_size_round_trips() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_max_message_size(1024);
        assert_eq!(cfg.max_message_size(), Some(1024));
    }

    #[test]
    fn builder_chain_preserves_all_fields() {
        use crate::consumer::{DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY};
        let cfg = RedisConsumerGroupConfig::new(2..=8)
            .with_prefetch_count(32)
            .with_concurrent_processing(true)
            .with_handler_timeout(Duration::from_secs(3))
            .with_max_pending_per_key(200)
            .with_max_message_size(4096);
        assert_eq!(cfg.min_consumers(), 2);
        assert_eq!(cfg.max_consumers(), 8);
        assert_eq!(cfg.prefetch_count(), 32);
        assert!(cfg.concurrent_processing());
        assert_eq!(cfg.handler_timeout(), Some(Duration::from_secs(3)));
        assert_eq!(cfg.max_pending_per_key(), Some(200));
        assert_eq!(cfg.max_message_size(), Some(4096));
        // Verify defaults are distinct from the values set above.
        assert_ne!(Some(200), Some(DEFAULT_MAX_PENDING_PER_KEY));
        assert_ne!(Some(4096usize), Some(DEFAULT_MAX_MESSAGE_SIZE));
    }

    #[test]
    #[should_panic(expected = "min_consumers")]
    #[allow(clippy::reversed_empty_ranges)]
    fn config_min_greater_than_max_panics() {
        // Intentionally inverted: the constructor must assert that min <= max.
        let _ = RedisConsumerGroupConfig::new(5..=2);
    }

    mod group {
        use super::*;
        use crate::backend::ConsumerOptionsInner;

        fn default_config() -> RedisConsumerGroupConfig {
            RedisConsumerGroupConfig::new(1..=4)
        }

        fn test_group(config: RedisConsumerGroupConfig) -> RedisConsumerGroup {
            let group_token = CancellationToken::new();
            let spawner: Spawner = Arc::new(|options: ConsumerOptionsInner| {
                tokio::spawn(async move {
                    options.shutdown.cancelled().await;
                })
            });
            let reaper_factory: ReaperFactory = Arc::new(|| tokio::spawn(async {}));
            RedisConsumerGroup {
                queue: "test-queue".into(),
                consumers: Vec::with_capacity(config.max_consumers() as usize),
                config,
                spawner,
                group_token,
                error_count: Arc::new(AtomicUsize::new(0)),
                panic_count: Arc::new(AtomicUsize::new(0)),
                reaper_factory,
                reaper_handle: None,
            }
        }

        #[tokio::test]
        async fn spawn_one_threads_max_pending_per_key_to_options() {
            use std::sync::Mutex;
            let captured: Arc<Mutex<Option<Option<usize>>>> = Arc::new(Mutex::new(None));
            let cap = captured.clone();
            let config = RedisConsumerGroupConfig::new(1..=1).with_max_pending_per_key(777);
            let mut group = test_group(config);
            group.spawner = Arc::new(move |options: ConsumerOptionsInner| {
                *cap.lock().unwrap() = Some(options.max_pending_per_key);
                tokio::spawn(async move { options.shutdown.cancelled().await })
            });
            group.start();
            assert_eq!(*captured.lock().unwrap(), Some(Some(777)));
            group.shutdown().await;
        }

        #[tokio::test]
        async fn spawn_one_threads_max_message_size_to_options() {
            use std::sync::Mutex;
            let captured: Arc<Mutex<Option<Option<usize>>>> = Arc::new(Mutex::new(None));
            let cap = captured.clone();
            let config = RedisConsumerGroupConfig::new(1..=1).with_max_message_size(2048);
            let mut group = test_group(config);
            group.spawner = Arc::new(move |options: ConsumerOptionsInner| {
                *cap.lock().unwrap() = Some(options.max_message_size);
                tokio::spawn(async move { options.shutdown.cancelled().await })
            });
            group.start();
            assert_eq!(*captured.lock().unwrap(), Some(Some(2048)));
            group.shutdown().await;
        }

        #[tokio::test]
        async fn start_spawns_min_consumers() {
            let mut group = test_group(RedisConsumerGroupConfig::new(3..=5));
            group.start();
            assert_eq!(group.active_consumers(), 3);
            group.shutdown().await;
        }

        #[tokio::test]
        async fn scale_up_adds_one_consumer() {
            let mut group = test_group(default_config());
            group.start();
            assert_eq!(group.active_consumers(), 1);
            assert!(group.scale_up());
            assert_eq!(group.active_consumers(), 2);
            group.shutdown().await;
        }

        #[tokio::test]
        async fn scale_up_rejected_at_max() {
            let mut group = test_group(RedisConsumerGroupConfig::new(2..=2));
            group.start();
            assert_eq!(group.active_consumers(), 2);
            assert!(!group.scale_up());
            assert_eq!(group.active_consumers(), 2);
            group.shutdown().await;
        }

        #[tokio::test]
        async fn scale_down_removes_one_consumer() {
            let mut group = test_group(RedisConsumerGroupConfig::new(1..=4));
            group.start();
            assert!(group.scale_up());
            assert_eq!(group.active_consumers(), 2);
            assert!(group.scale_down());
            assert_eq!(group.active_consumers(), 1);
            group.shutdown().await;
        }

        #[tokio::test]
        async fn scale_down_rejected_at_min() {
            let mut group = test_group(RedisConsumerGroupConfig::new(1..=4));
            group.start();
            assert!(!group.scale_down());
            assert_eq!(group.active_consumers(), 1);
            group.shutdown().await;
        }

        #[tokio::test]
        async fn reaper_is_spawned_exactly_once_per_group() {
            // `start()` is idempotent re: the reaper — calling it multiple
            // times (e.g. after a no-op scale event) must not double-spawn.
            // Verified by counting factory invocations.
            let spawn_count = Arc::new(AtomicUsize::new(0));
            let counter = spawn_count.clone();
            let reaper_factory: ReaperFactory = Arc::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async {})
            });
            let mut group = test_group(RedisConsumerGroupConfig::new(1..=4));
            group.reaper_factory = reaper_factory;
            assert_eq!(spawn_count.load(Ordering::Relaxed), 0);
            group.start();
            assert_eq!(spawn_count.load(Ordering::Relaxed), 1);
            group.start();
            assert_eq!(
                spawn_count.load(Ordering::Relaxed),
                1,
                "second start() must not respawn the reaper"
            );
            group.shutdown().await;
        }

        #[tokio::test]
        async fn shutdown_joins_the_reaper_handle() {
            // After `shutdown`, the group's reaper_handle should be cleared
            // — `shutdown_with_tally` takes() it and awaits.
            let reaper_factory: ReaperFactory = Arc::new(|| tokio::spawn(async {}));
            let mut group = test_group(RedisConsumerGroupConfig::new(1..=4));
            group.reaper_factory = reaper_factory;
            group.start();
            assert!(group.reaper_handle.is_some());
            group.shutdown().await;
            assert!(
                group.reaper_handle.is_none(),
                "shutdown_with_tally must take() the reaper handle"
            );
        }

        fn hanging_test_group(config: RedisConsumerGroupConfig) -> RedisConsumerGroup {
            let mut group = test_group(config);
            group.spawner = Arc::new(|_options: ConsumerOptionsInner| {
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                })
            });
            // Reaper that also never exits, exercising the abort-on-escalation path.
            group.reaper_factory = Arc::new(|| {
                tokio::spawn(async {
                    std::future::pending::<()>().await;
                })
            });
            group
        }

        #[tokio::test]
        async fn drain_into_timeout_preserves_atomics_in_tally() {
            let mut group = hanging_test_group(RedisConsumerGroupConfig::new(2..=2));
            group.start();
            assert_eq!(group.active_consumers(), 2);

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
        async fn abort_remaining_into_kills_hanging_consumers_and_reaper() {
            let mut group = hanging_test_group(RedisConsumerGroupConfig::new(2..=2));
            group.start();
            assert!(group.reaper_handle.is_some());

            group.error_count.store(5, Ordering::Relaxed);
            group.panic_count.store(1, Ordering::Relaxed);

            let mut tally = ShutdownTally::default();
            let _ =
                tokio::time::timeout(Duration::from_millis(50), group.drain_into(&mut tally)).await;
            group.abort_remaining_into(&mut tally).await;

            assert_eq!(group.active_consumers(), 0);
            assert!(
                group.reaper_handle.is_none(),
                "abort_remaining_into must take() the reaper handle"
            );
            assert_eq!(tally.errors, 5);
            assert_eq!(tally.panics, 1);
        }

        #[tokio::test]
        async fn scale_down_skips_busy_consumers() {
            let mut group = test_group(RedisConsumerGroupConfig::new(1..=4));
            // Replace spawner with one whose `processing` flag we control.
            let busy_flag = Arc::new(AtomicBool::new(true));
            let flag = busy_flag.clone();
            let spawner: Spawner = Arc::new(move |options: ConsumerOptionsInner| {
                // Mirror the real consumer's contract: set `options.processing`
                // before awaiting shutdown so `scale_down` sees the task as busy.
                options
                    .processing
                    .store(flag.load(Ordering::Relaxed), Ordering::Relaxed);
                tokio::spawn(async move {
                    options.shutdown.cancelled().await;
                })
            });
            group.spawner = spawner;
            group.start();
            group.scale_up();
            assert!(!group.scale_down(), "all consumers reporting busy");
            busy_flag.store(false, Ordering::Relaxed);
            // Spawn a fresh idle one and verify scale-down picks it.
            group.scale_up();
            assert!(group.scale_down());
            group.shutdown().await;
        }
    }
}
