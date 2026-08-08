use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::backend::ConsumerOptionsInner as ConsumerOptions;
use crate::backends::nats::client::NatsClient;
use crate::backends::nats::consumer::{NatsConsumer, derive_ack_wait};
use crate::backends::nats::topology::NatsTopologyDeclarer;
use crate::consumer::{HandlerTimeoutConfig, resolve_handler_timeout};
use crate::consumer_supervisor::{AbortOnDrop, ShutdownTally};
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metrics;
use crate::supervision::RespawnSupervisor;
use crate::topic::{SequencedTopic, Topic};
use crate::{DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY};

/// Type-erased factory that spawns a single consumer task.
pub(crate) type Spawner = Arc<dyn Fn(ConsumerOptions) -> JoinHandle<()> + Send + Sync>;

/// Returns `true` if the handle is finished, harvesting its result first: a
/// panic (non-cancelled `JoinError`) is counted into `panic_count` so it
/// still reaches the shutdown tally even though the pruned handle will never
/// be awaited by the final drain. Polling a finished `JoinHandle` with a
/// no-op waker completes immediately — this never blocks.
fn harvest_if_finished(
    handle: &mut JoinHandle<()>,
    panic_count: &AtomicUsize,
    queue: &str,
) -> bool {
    if !handle.is_finished() {
        return false;
    }
    let mut cx = Context::from_waker(Waker::noop());
    if let Poll::Ready(Err(e)) = Pin::new(handle).poll(&mut cx)
        && !e.is_cancelled()
    {
        tracing::error!(error = %e, group = %queue, "finished consumer task had panicked");
        panic_count.fetch_add(1, Ordering::Relaxed);
    }
    true
}

// ---------------------------------------------------------------------------
// NatsConsumerGroupConfig
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct NatsConsumerGroupConfig {
    prefetch_count: u16,
    min_consumers: u16,
    max_consumers: u16,
    max_retries: u32,
    pub(crate) handler_timeout: HandlerTimeoutConfig,
    concurrent_processing: bool,
    max_pending_per_key: Option<usize>,
    max_message_size: Option<usize>,
}

impl Default for NatsConsumerGroupConfig {
    fn default() -> Self {
        Self::new(1..=4)
    }
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
            handler_timeout: HandlerTimeoutConfig::Inherit,
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
        assert!(!timeout.is_zero(), "handler_timeout must be positive");
        self.handler_timeout = HandlerTimeoutConfig::Set(timeout);
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

    /// Returns the configured handler timeout. A freshly-constructed
    /// config reports `Some(DEFAULT_HANDLER_TIMEOUT)`; a registry-level
    /// default set via `ConsumerGroup::with_default_handler_timeout`
    /// is not reflected here because the config does not know about
    /// its registry.
    pub fn handler_timeout(&self) -> Option<Duration> {
        Some(resolve_handler_timeout(self.handler_timeout, None))
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
    pub(crate) queue: String,
    pub(crate) config: NatsConsumerGroupConfig,
    pub(crate) spawner: Spawner,
    pub(crate) consumers: Vec<(CancellationToken, Arc<AtomicBool>, JoinHandle<()>)>,
    pub(crate) group_token: CancellationToken,
    pub(crate) error_count: Arc<AtomicUsize>,
    /// Panic count incremented by the FIFO spawner wrapper when a shard task
    /// exits with a `JoinError` that is not a cancellation. Drained by
    /// [`NatsConsumerGroup::shutdown_with_tally`].
    pub(crate) panic_count: Arc<AtomicUsize>,
    /// Handles of consumers removed by `scale_down`. Their tokens are already
    /// cancelled; retained here so the final drain awaits/aborts and tallies
    /// them instead of detaching the task.
    pub(crate) retiring: Vec<JoinHandle<()>>,
    /// Backoff + circuit-breaker state driving [`Self::ensure_min`].
    pub(crate) respawn: RespawnSupervisor,
}

impl NatsConsumerGroup {
    pub fn new<T, H>(
        queue: impl Into<String>,
        config: NatsConsumerGroupConfig,
        client: NatsClient,
        group_token: CancellationToken,
        handler_factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Self
    where
        T: Topic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let concurrent = config.concurrent_processing;
        let max_ack_pending = aggregate_max_ack_pending(&config);
        let error_count = Arc::new(AtomicUsize::new(0));
        let ec_for_spawner = error_count.clone();
        let spawner: Spawner = Arc::new(move |options: ConsumerOptions| {
            let handler = handler_factory();
            let consumer = NatsConsumer::new(client.clone());
            let mut options = if concurrent {
                options
            } else {
                ConsumerOptions {
                    prefetch_count: 1,
                    ..options
                }
            };
            options.max_ack_pending = Some(max_ack_pending);
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
            panic_count: Arc::new(AtomicUsize::new(0)),
            retiring: Vec::new(),
            respawn: RespawnSupervisor::default(),
        }
    }

    /// Construct a FIFO consumer group for a [`SequencedTopic`].
    ///
    /// FIFO replica count is fixed at 1 — concurrency comes from shards,
    /// not from multiple replicas of the shard set.
    pub fn new_fifo<T, H>(
        queue: impl Into<String>,
        client: NatsClient,
        mut config: NatsConsumerGroupConfig,
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
        config.min_consumers = 1;
        config.max_consumers = 1;

        let spawner: Spawner = Arc::new(move |options: ConsumerOptions| {
            let handler = handler_factory();
            let consumer = NatsConsumer::new(client.clone());
            let ec = ec_for_spawner.clone();
            let pc = pc_for_spawner.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                // `spawn_fifo_shards` is async for NATS — `.await` is required.
                let handles = match consumer
                    .spawn_fifo_shards::<T, H>(handler, ctx, options)
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

        let queue_str: String = queue.into();
        Self {
            queue: queue_str.clone(),
            consumers: Vec::with_capacity(1),
            config,
            spawner,
            group_token,
            error_count,
            panic_count,
            retiring: Vec::new(),
            respawn: RespawnSupervisor::default(),
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

    /// Removes finished handles from the active and retiring lists so
    /// capacity gates and the idle-pick operate on live members only. A
    /// consumer that exited with a non-retryable error (or exhausted its
    /// reconnect budget) would otherwise occupy a slot forever: `scale_up`
    /// would refuse at "max capacity" with zero consumers running, and
    /// `scale_down` could "cancel" an already-dead member while live ones
    /// keep working. Panics of pruned handles are harvested into
    /// `panic_count` so the shutdown tally stays accurate.
    fn prune_finished(&mut self) {
        let panic_count = &self.panic_count;
        let queue = &self.queue;
        self.consumers
            .retain_mut(|(_, _, handle)| !harvest_if_finished(handle, panic_count, queue));
        self.retiring
            .retain_mut(|handle| !harvest_if_finished(handle, panic_count, queue));
    }

    /// Spawn one additional consumer. Returns false at max capacity.
    pub fn scale_up(&mut self) -> bool {
        self.prune_finished();
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
        self.prune_finished();
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

    /// Number of consumer tasks that are actually alive. Counts (rather than
    /// relying on prior pruning) because the autoscaler reads this through
    /// immutable registry access — it must be truthful with `&self`.
    pub fn active_consumers(&self) -> usize {
        self.consumers
            .iter()
            .filter(|(_, _, handle)| !handle.is_finished())
            .count()
    }

    /// Top the group back up to `min_consumers`, respecting the respawn
    /// backoff and circuit-breaker. Driven by the autoscaler tick; returns how
    /// many members were spawned.
    pub(crate) fn ensure_min(&mut self) -> usize {
        // Never respawn into a group that is shutting down: the autoscaler can
        // still tick between cancellation and the final drain, and a member
        // spawned there would be born cancelled.
        if self.group_token.is_cancelled() {
            return 0;
        }
        self.prune_finished();
        let live = self.active_consumers();
        let min = self.config.min_consumers as usize;
        let Some(round) = self.respawn.plan(live, min, &self.queue) else {
            return 0;
        };
        for _ in 0..round.count {
            self.spawn_one();
        }
        self.respawn.commit(live, round);
        info!(
            group = %self.queue,
            spawned = round.count,
            live_before = live,
            min,
            probe = round.probe,
            "respawned consumers to restore min capacity"
        );
        round.count
    }

    #[cfg(test)]
    pub(crate) fn retiring_is_empty(&self) -> bool {
        self.retiring.is_empty()
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn config(&self) -> &NatsConsumerGroupConfig {
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
            group = %self.queue,
            // Deliberately the raw slot count (not active_consumers): this is
            // how many handles the drain below will await, live or not.
            consumers = self.consumers.len(),
            "shutting down consumer group"
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

    /// Abort every surviving consumer handle and tally the result.
    ///
    /// Used by `RegistryImpl::run_until_timeout` after a cooperative drain
    /// times out. Mirrors `ConsumerSupervisor::run_until_timeout`'s
    /// `abort_all` + drain escalation so the returned `SupervisorOutcome`
    /// reflects errors and panics counted before the deadline.
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
        let mut options = ConsumerOptions::defaults_with_shutdown(child_token.clone());
        options.max_retries = self.config.max_retries;
        options.prefetch_count = self.config.prefetch_count;
        options.processing = processing.clone();
        options.handler_timeout = Some(resolve_handler_timeout(self.config.handler_timeout, None));
        options.max_pending_per_key = self.config.max_pending_per_key;
        options.max_message_size = self.config.max_message_size;
        options.consumer_group = Some(Arc::from(self.queue.as_str()));
        let handle = (self.spawner)(options);
        self.consumers.push((child_token, processing, handle));
        // Raw vec position of the new entry, not a liveness count.
        debug!(group = %self.queue, consumer_index = self.consumers.len() - 1, "spawned consumer");
    }
}

// ---------------------------------------------------------------------------
// NatsConsumerGroupRegistry
// ---------------------------------------------------------------------------

pub struct NatsConsumerGroupRegistry {
    pub(crate) groups: HashMap<String, NatsConsumerGroup>,
    client: Option<NatsClient>,
    pub(super) default_handler_timeout: Option<Duration>,
}

impl NatsConsumerGroupRegistry {
    pub fn new(client: NatsClient) -> Self {
        Self {
            groups: HashMap::new(),
            client: Some(client),
            default_handler_timeout: None,
        }
    }

    /// Create a registry from a pre-populated map of groups (for testing).
    /// The resulting registry cannot be used to call `register()`.
    #[cfg(test)]
    pub(crate) fn from_groups(groups: HashMap<String, NatsConsumerGroup>) -> Self {
        Self {
            groups,
            client: None,
            default_handler_timeout: None,
        }
    }

    /// Set the registry-level default handler timeout. Applies to every
    /// group whose `NatsConsumerGroupConfig` did not explicitly call
    /// `with_handler_timeout`. Per-group explicit settings always win.
    pub fn with_default_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "default_handler_timeout must be positive"
        );
        self.default_handler_timeout = Some(timeout);
        self
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
        config: NatsConsumerGroupConfig,
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
                metrics::BackendLabel::Nats,
                metrics::BackendErrorKind::Topology,
            );
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' is already registered"
            )));
        }

        let client = self.client.as_ref().ok_or_else(|| {
            ShoveError::Topology("registry has no client (test-only registry)".into())
        })?;

        let declarer = NatsTopologyDeclarer::new(client.clone());
        declarer.declare(topology).await?;

        // Pre-establish the durable pull consumer with the aggregate
        // ack budget — eliminates N-way `CONSUMER.CREATE` storms on
        // reconnect (one per consumer task). Per-consumer code now
        // uses `get_consumer` (read-only) to attach.
        //
        // `config.handler_timeout` was folded to `Set(resolved)` above, so
        // this resolves to exactly the effective timeout the group's
        // consumers will enforce.
        let max_ack_pending = aggregate_max_ack_pending(&config);
        let ack_wait = derive_ack_wait(resolve_handler_timeout(config.handler_timeout, None));
        let consumer_name = super::constants::consumer_name(topology.queue());
        let filter_subjects = topology.nats_stream_subjects().unwrap_or(&[]);
        declarer
            .declare_pull_consumer(
                topology.queue(),
                &consumer_name,
                max_ack_pending,
                ack_wait,
                filter_subjects,
            )
            .await?;

        info!(group = %name, "registering consumer group");
        let group_token = client.shutdown_token().child_token();
        let group = NatsConsumerGroup::new::<T, H>(
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

    /// Register a new FIFO consumer group for a [`SequencedTopic`].
    ///
    /// Declares the topology for `T` before creating the group. The group is
    /// **not** started — call [`start_all`] separately.
    ///
    /// [`start_all`]: Self::start_all
    pub async fn register_fifo<T, H>(
        &mut self,
        config: NatsConsumerGroupConfig,
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

        let client = self.client.as_ref().ok_or_else(|| {
            ShoveError::Topology("registry has no client (test-only registry)".into())
        })?;

        let declarer = NatsTopologyDeclarer::new(client.clone());
        declarer.declare(topology).await?;

        // Note: unlike `register` above, FIFO does **not** call
        // `declare_pull_consumer` here. FIFO consumer groups create
        // one shard task per routing shard via `spawn_fifo_shards`,
        // and each shard uses a distinct, shard-specific consumer
        // name (`<group>-shard-<n>`) via `get_or_create_consumer`.
        // The fan-out is therefore bounded by `routing_shards()`
        // (typically 8-32), not by an arbitrary consumer count, so
        // there is no thundering herd to consolidate.

        info!(group = %name, "registering FIFO consumer group");
        let group_token = client.shutdown_token().child_token();
        let group = NatsConsumerGroup::new_fifo::<T, H>(
            name.clone(),
            client.clone(),
            config,
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

    pub fn groups(&self) -> &HashMap<String, NatsConsumerGroup> {
        &self.groups
    }

    pub fn groups_mut(&mut self) -> &mut HashMap<String, NatsConsumerGroup> {
        &mut self.groups
    }

    pub async fn shutdown_all(&mut self) {
        let _ = self.shutdown_all_with_tally().await;
    }

    pub(crate) async fn shutdown_all_with_tally(&mut self) -> ShutdownTally {
        let mut tally = ShutdownTally::default();
        self.drain_all_into(&mut tally).await;
        tally
    }

    /// Drain every consumer group, accumulating errors/panics into `tally`.
    ///
    /// Caller may race this against a timeout: each per-group `drain_into`
    /// captures atomic counts before awaiting handles, so the tally retains
    /// pre-cancel state even if the outer future is dropped mid-iteration.
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
}

/// Aggregate `max_ack_pending` budget for a consumer-group config.
///
/// In concurrent mode every consumer holds up to `prefetch_count` messages
/// in flight, so the group as a whole can have `prefetch_count × max_consumers`
/// pending acks. In sequential mode each consumer holds at most 1, so the
/// budget is `max_consumers`. Used both by the topology declarer (to size
/// the durable JetStream consumer once) and by `NatsConsumerGroup::new` (to
/// communicate the same value to spawned consumer tasks, even though they
/// only `get_consumer` now and don't re-write the config).
pub(crate) fn aggregate_max_ack_pending(config: &NatsConsumerGroupConfig) -> i64 {
    if config.concurrent_processing {
        config.prefetch_count as i64 * config.max_consumers as i64
    } else {
        config.max_consumers as i64
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::DEFAULT_HANDLER_TIMEOUT;
    use crate::supervision::{
        RESPAWN_BACKOFF_MAX, RESPAWN_CIRCUIT_COOLDOWN, RESPAWN_CIRCUIT_LIMIT,
    };

    /// Spawner whose tasks all exit immediately: a group that can never stay up.
    fn always_dies_spawner() -> Spawner {
        Arc::new(|_options: ConsumerOptions| tokio::spawn(async {}))
    }

    /// Spawner whose first `n` tasks exit immediately; later ones stay alive
    /// until their shutdown token fires.
    fn dies_n_times_spawner(n: usize) -> Spawner {
        let calls = Arc::new(AtomicUsize::new(0));
        Arc::new(move |options: ConsumerOptions| {
            let i = calls.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if i >= n {
                    options.shutdown.cancelled().await;
                }
            })
        })
    }

    /// Let immediately-exiting consumer tasks reach completion.
    async fn settle() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ensure_min_respawns_dead_consumers() {
        let mut group =
            test_group_with_spawner(NatsConsumerGroupConfig::new(2..=4), dies_n_times_spawner(2));
        group.start();
        settle().await;
        assert_eq!(group.active_consumers(), 0, "both members must have died");

        assert_eq!(group.ensure_min(), 2, "must respawn the full shortfall");
        assert_eq!(group.active_consumers(), 2);
        group.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn ensure_min_does_not_respawn_during_shutdown() {
        let mut group =
            test_group_with_spawner(NatsConsumerGroupConfig::new(2..=4), always_dies_spawner());
        group.start();
        settle().await;
        group.group_token.cancel();

        assert_eq!(
            group.ensure_min(),
            0,
            "a cancelled group must not respawn members into the drain"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn ensure_min_is_a_noop_for_a_healthy_group() {
        let mut group = test_group(NatsConsumerGroupConfig::new(2..=4));
        group.start();
        assert_eq!(group.ensure_min(), 0, "a group at min must not respawn");
        assert_eq!(group.active_consumers(), 2);
        group.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn ensure_min_circuit_breaks_then_probes_and_recovers() {
        let mut group =
            test_group_with_spawner(NatsConsumerGroupConfig::new(3..=4), always_dies_spawner());
        group.start();
        settle().await;

        for _ in 0..RESPAWN_CIRCUIT_LIMIT {
            group.ensure_min();
            settle().await;
            tokio::time::advance(RESPAWN_BACKOFF_MAX).await;
        }

        assert_eq!(
            group.ensure_min(),
            0,
            "an open circuit must not spawn before its cooldown"
        );

        tokio::time::advance(RESPAWN_CIRCUIT_COOLDOWN).await;
        assert_eq!(
            group.ensure_min(),
            1,
            "an open circuit must half-open with a single probe"
        );
        group.shutdown().await;
    }

    fn test_group(config: NatsConsumerGroupConfig) -> NatsConsumerGroup {
        let spawner: Spawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });
        test_group_with_spawner(config, spawner)
    }

    fn test_group_with_spawner(
        config: NatsConsumerGroupConfig,
        spawner: Spawner,
    ) -> NatsConsumerGroup {
        NatsConsumerGroup {
            queue: "test-queue".into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token: CancellationToken::new(),
            error_count: Arc::new(AtomicUsize::new(0)),
            panic_count: Arc::new(AtomicUsize::new(0)),
            retiring: Vec::new(),
            respawn: RespawnSupervisor::default(),
        }
    }

    /// Spawner whose first task exits immediately (a "dead" consumer) and
    /// whose subsequent tasks stay alive until their shutdown token fires.
    fn first_dies_spawner() -> Spawner {
        let calls = Arc::new(AtomicUsize::new(0));
        Arc::new(move |options: ConsumerOptions| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if n > 0 {
                    options.shutdown.cancelled().await;
                }
            })
        })
    }

    /// Polls until the group's consumer at `index` has finished (bounded).
    async fn wait_finished(group: &NatsConsumerGroup, index: usize) {
        for _ in 0..200 {
            if group.consumers[index].2.is_finished() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("consumer task {index} did not finish within the wait budget");
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

    #[tokio::test]
    async fn scale_down_handle_is_drained_not_detached() {
        // A scaled-down consumer that hangs must still be aborted and counted,
        // not silently detached. Use a hanging spawner so the retiring handle
        // would leak under the old drop-the-handle behaviour.
        let mut group = hanging_test_group(NatsConsumerGroupConfig::new(1..=3));
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

    // -- liveness accounting --

    #[tokio::test]
    async fn dead_consumer_is_not_counted_as_active() {
        let spawner: Spawner = Arc::new(|_options: ConsumerOptions| tokio::spawn(async {}));
        let mut group = test_group_with_spawner(NatsConsumerGroupConfig::new(1..=4), spawner);
        group.start();
        wait_finished(&group, 0).await;
        assert_eq!(
            group.active_consumers(),
            0,
            "a finished consumer task must not be reported as active"
        );
        group.shutdown().await;
    }

    #[tokio::test]
    async fn scale_up_replaces_dead_consumer_at_max_capacity() {
        let mut group =
            test_group_with_spawner(NatsConsumerGroupConfig::new(0..=1), first_dies_spawner());
        assert!(group.scale_up());
        wait_finished(&group, 0).await;
        assert_eq!(group.active_consumers(), 0);

        // len == max, but the only member is dead: pruning must free the slot.
        assert!(
            group.scale_up(),
            "scale_up must not refuse at max capacity when the slot holder is dead"
        );
        assert_eq!(group.active_consumers(), 1);
        group.shutdown().await;
    }

    #[tokio::test]
    async fn scale_down_prunes_dead_member_instead_of_cancelling_live_one() {
        let mut group =
            test_group_with_spawner(NatsConsumerGroupConfig::new(1..=2), first_dies_spawner());
        assert!(group.scale_up()); // dies immediately
        assert!(group.scale_up()); // stays alive
        wait_finished(&group, 0).await;

        let live_token = group.consumers[1].0.clone();
        // min+1 slots, one dead: pruning brings the live count to min, so the
        // scale-down is rejected and no live member is cancelled.
        assert!(!group.scale_down());
        assert!(
            !live_token.is_cancelled(),
            "the live member must not be cancelled while a dead one occupied a slot"
        );
        assert_eq!(group.active_consumers(), 1);
        assert!(group.retiring_is_empty());
        group.shutdown().await;
    }

    #[tokio::test]
    async fn pruned_panicked_consumer_still_lands_in_shutdown_tally() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let spawner: Spawner = Arc::new(move |options: ConsumerOptions| {
            let n = c.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if n == 0 {
                    panic!("consumer blew up");
                }
                options.shutdown.cancelled().await;
            })
        });
        let mut group = test_group_with_spawner(NatsConsumerGroupConfig::new(0..=2), spawner);
        assert!(group.scale_up()); // panics immediately
        wait_finished(&group, 0).await;
        assert!(group.scale_up()); // prunes (and harvests) the panicked handle

        let tally = group.shutdown_with_tally().await;
        assert_eq!(
            tally.panics, 1,
            "a panic harvested at prune time must still reach the shutdown tally"
        );
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

    /// Build a `NatsConsumerGroup` whose spawned consumers ignore cancellation,
    /// so `drain_into` can only progress via the abort escalation path.
    fn hanging_test_group(config: NatsConsumerGroupConfig) -> NatsConsumerGroup {
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
        // Regression: previously, a drain timeout in
        // `RegistryImpl::run_until_timeout` discarded the tally because the
        // future was simply dropped. The fix lifts the tally outside the
        // timeout so pre-cancel counts survive even when consumers hang.
        let mut group = hanging_test_group(NatsConsumerGroupConfig::new(2..=2));
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
        let mut group = hanging_test_group(NatsConsumerGroupConfig::new(2..=2));
        group.start();
        assert_eq!(group.active_consumers(), 2);

        group.error_count.store(5, Ordering::Relaxed);
        group.panic_count.store(1, Ordering::Relaxed);

        let mut tally = ShutdownTally::default();
        let _ = tokio::time::timeout(Duration::from_millis(50), group.drain_into(&mut tally)).await;

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

    // -- handler timeout tri-state --

    #[test]
    fn inherit_config_uses_library_default_with_no_registry_default() {
        let cfg = NatsConsumerGroupConfig::new(1..=4);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, None),
            DEFAULT_HANDLER_TIMEOUT,
        );
    }

    #[test]
    fn inherit_config_uses_registry_default_when_set() {
        let cfg = NatsConsumerGroupConfig::new(1..=4);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(45),
        );
    }

    #[test]
    fn with_handler_timeout_beats_registry_default() {
        let cfg = NatsConsumerGroupConfig::new(1..=4).with_handler_timeout(Duration::from_secs(5));
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(5),
        );
    }

    #[test]
    #[should_panic(expected = "handler_timeout must be positive")]
    fn with_handler_timeout_zero_panics() {
        let _ = NatsConsumerGroupConfig::new(1..=4).with_handler_timeout(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "default_handler_timeout must be positive")]
    fn with_default_handler_timeout_zero_panics() {
        let registry = NatsConsumerGroupRegistry::from_groups(HashMap::new());
        let _ = registry.with_default_handler_timeout(Duration::ZERO);
    }
}
