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
use crate::consumer::{HandlerTimeoutConfig, resolve_handler_timeout};
use crate::consumer_supervisor::ShutdownTally;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metrics;
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::{SchemaEnforcement, SchemaRegistry};
use crate::topic::{SequencedTopic, Topic};
use crate::{DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY};

/// Type-erased factory that spawns a single consumer task.
pub(crate) type Spawner = Arc<dyn Fn(ConsumerOptions) -> JoinHandle<()> + Send + Sync>;

/// `auto.offset.reset` policy applied when a consumer group joins a topic
/// for the first time (no committed offsets yet) or after offsets have
/// expired. Maps to the rdkafka client config value of the same name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaAutoOffsetReset {
    /// Replay from the earliest available record on the topic. Suitable for
    /// at-least-once pipelines starting from a clean slate or for DLQ
    /// drains where the goal is to consume the full history.
    Earliest,
    /// Resume from the latest record at the moment the group joins —
    /// historical messages on the topic are skipped. Use this for high-
    /// volume metrics / telemetry streams or blue-green deploys where
    /// crawling backlog is wasteful.
    Latest,
    /// Refuse to start without a committed offset; the consumer raises an
    /// error if no offset exists. Use this when accidentally replaying or
    /// skipping history would be a correctness bug.
    None,
}

impl KafkaAutoOffsetReset {
    pub(crate) fn as_rdkafka_str(self) -> &'static str {
        match self {
            KafkaAutoOffsetReset::Earliest => "earliest",
            KafkaAutoOffsetReset::Latest => "latest",
            KafkaAutoOffsetReset::None => "none",
        }
    }
}

// ---------------------------------------------------------------------------
// KafkaConsumerGroupConfig
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct KafkaConsumerGroupConfig {
    prefetch_count: u16,
    min_consumers: u16,
    max_consumers: u16,
    max_retries: u32,
    pub(crate) handler_timeout: HandlerTimeoutConfig,
    concurrent_processing: bool,
    max_pending_per_key: Option<usize>,
    max_message_size: Option<usize>,
    /// Optional Kafka consumer group ID override.
    ///
    /// When `None` (the default), the group ID is derived from the topic name as
    /// `"{queue}-consumer"`. Set this when two independent services must each
    /// receive every message from the same topic (fan-out) — without separate
    /// group IDs they share the same consumer group and compete for partitions.
    group_id: Option<String>,
    /// Optional `auto.offset.reset` policy. `None` keeps the library default
    /// of `Earliest` (replay history). Override to `Latest` for tail-only
    /// consumers or to `None` to refuse silent replay/skip on a fresh group.
    auto_offset_reset: Option<KafkaAutoOffsetReset>,

    /// Schema Registry client shared across every consumer spawned by this
    /// group. `None` disables registry-based decoding for the group.
    #[cfg(feature = "kafka-schema-registry")]
    pub(crate) schema_registry: Option<Arc<SchemaRegistry>>,
    /// How subject mismatches are handled for this group. Default `Enforce`.
    #[cfg(feature = "kafka-schema-registry")]
    pub(crate) schema_enforcement: SchemaEnforcement,
    /// Accepted subject set for this group. `None` derives `["{queue}-value"]`
    /// at decode time.
    #[cfg(feature = "kafka-schema-registry")]
    pub(crate) schema_accepted_subjects: Option<Vec<Arc<str>>>,
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
            handler_timeout: HandlerTimeoutConfig::Inherit,
            concurrent_processing: false,
            max_pending_per_key: Some(DEFAULT_MAX_PENDING_PER_KEY),
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            group_id: None,
            auto_offset_reset: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: SchemaEnforcement::Enforce,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted_subjects: None,
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

    /// Override the Kafka consumer group ID for this consumer group.
    ///
    /// By default the group ID is `"{queue}-consumer"`. Call this when two
    /// independent services consume the same topic and each must receive all
    /// messages (fan-out): they must use different group IDs so they are
    /// assigned independent partition sets rather than competing for the same
    /// partitions.
    ///
    /// This overrides only the rdkafka `group.id`; the group is still
    /// identified within the registry by the topic name.
    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    /// Returns the explicitly configured consumer group ID, or `None` if the
    /// default (`"{queue}-consumer"`) should be used.
    pub fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }

    /// Resolves the broker-side consumer group ID for `queue`: the explicit
    /// override if set via [`with_group_id`], otherwise the default
    /// `"{queue}-consumer"`. The autoscaler and broker-side consumer must
    /// resolve the same value — keeping this in one place is the regression
    /// guard for the arch-K-1-style "phantom backlog" footgun.
    ///
    /// [`with_group_id`]: Self::with_group_id
    pub(crate) fn resolved_group_id(&self, queue: &str) -> String {
        self.group_id
            .clone()
            .unwrap_or_else(|| super::constants::consumer_group_id(queue))
    }

    /// Override the rdkafka `auto.offset.reset` policy for this consumer
    /// group. Defaults to [`KafkaAutoOffsetReset::Earliest`] (replay history)
    /// when unset.
    pub fn with_auto_offset_reset(mut self, policy: KafkaAutoOffsetReset) -> Self {
        self.auto_offset_reset = Some(policy);
        self
    }

    /// Returns the explicitly configured `auto.offset.reset` policy, or
    /// `None` if the library default ([`KafkaAutoOffsetReset::Earliest`])
    /// should apply.
    pub fn auto_offset_reset(&self) -> Option<KafkaAutoOffsetReset> {
        self.auto_offset_reset
    }

    /// Set the Schema Registry client for this consumer group.
    ///
    /// Every consumer spawned by the group shares the same `Arc<SchemaRegistry>`,
    /// so the in-memory schema cache is shared across the whole group rather than
    /// duplicated per consumer.
    #[cfg(feature = "kafka-schema-registry")]
    pub fn with_schema_registry(mut self, registry: Arc<SchemaRegistry>) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    /// Override the schema-enforcement policy for this consumer group.
    ///
    /// Defaults to [`SchemaEnforcement::Enforce`] (subject mismatches route
    /// messages to the DLQ). Set to [`SchemaEnforcement::Permissive`] to log
    /// and count mismatches without rejecting.
    #[cfg(feature = "kafka-schema-registry")]
    pub fn with_schema_enforcement(mut self, enforcement: SchemaEnforcement) -> Self {
        self.schema_enforcement = enforcement;
        self
    }

    /// Restrict accepted Confluent schema subjects for this consumer group.
    ///
    /// When set, only messages whose registered subject matches one of the
    /// supplied values are accepted; mismatches are handled according to the
    /// configured [`SchemaEnforcement`] policy. When unset (the default), the
    /// subject `"{queue}-value"` is derived at decode time.
    #[cfg(feature = "kafka-schema-registry")]
    pub fn accept_schema_subjects<I, S>(mut self, subjects: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.schema_accepted_subjects = Some(subjects.into_iter().map(Into::into).collect());
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

    /// Returns the explicitly-set per-group handler timeout if any, otherwise
    /// the library default [`DEFAULT_HANDLER_TIMEOUT`].
    ///
    /// **Does not reflect the registry-level default** (set via
    /// [`KafkaConsumerGroupRegistry::with_default_handler_timeout`]).
    /// The registry-level default is resolved at consumer-registration time
    /// and is not visible from this config object.
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
// KafkaConsumerGroup
// ---------------------------------------------------------------------------

pub struct KafkaConsumerGroup {
    pub(crate) queue: String,
    /// The Kafka consumer group ID actually used by broker-side consumers.
    /// Standard consumers: `"{queue}-consumer"`. FIFO consumers: `"{queue}-fifo"`.
    /// Stored here so the autoscaler can query committed offsets under the
    /// correct group without re-deriving it.
    pub(crate) group_id: String,
    pub(crate) config: KafkaConsumerGroupConfig,
    pub(crate) spawner: Spawner,
    pub(crate) consumers: Vec<(CancellationToken, Arc<AtomicBool>, JoinHandle<()>)>,
    pub(crate) group_token: CancellationToken,
    pub(crate) error_count: Arc<AtomicUsize>,
    /// Panic count incremented by the FIFO spawner wrapper when a shard task
    /// exits with a `JoinError` that is not a cancellation. Drained by
    /// [`KafkaConsumerGroup::shutdown_with_tally`].
    pub(crate) panic_count: Arc<AtomicUsize>,
    /// Handles of consumers removed by `scale_down`. Their tokens are already
    /// cancelled; retained here so the final drain awaits/aborts and tallies
    /// them instead of detaching the task.
    pub(crate) retiring: Vec<JoinHandle<()>>,
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
        let queue_str: String = queue.into();
        let concurrent = config.concurrent_processing;
        // arch-K-8: surface the silent override of prefetch_count when the
        // caller configured >1 but left concurrent_processing=false. Logged
        // once at construction (vs. inside the spawner, which would fire on
        // every scale-up).
        if !concurrent && config.prefetch_count > 1 {
            tracing::warn!(
                queue = %queue_str,
                configured_prefetch_count = config.prefetch_count,
                "prefetch_count > 1 with concurrent_processing=false is being clamped to 1 — \
                 call .with_concurrent_processing(true) to honor the configured value"
            );
        }
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

        // arch-K-1 follow-up: honor the per-group `with_group_id` override here
        // so the autoscaler queries committed offsets under the same group the
        // broker-side consumer actually joined. Falling back to the default
        // `"{queue}-consumer"` keeps existing callers unchanged.
        let group_id = config.resolved_group_id(&queue_str);
        Self {
            queue: queue_str,
            group_id,
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
            error_count,
            panic_count: Arc::new(AtomicUsize::new(0)),
            retiring: Vec::new(),
        }
    }

    /// Construct a FIFO consumer group for a [`SequencedTopic`].
    ///
    /// FIFO replica count is fixed at 1 — Kafka uses partition-level ordering,
    /// so a single consumer task is sufficient.
    pub fn new_fifo<T, H>(
        queue: impl Into<String>,
        client: KafkaClient,
        mut config: KafkaConsumerGroupConfig,
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
            let consumer = KafkaConsumer::new(client.clone());
            let ec = ec_for_spawner.clone();
            let pc = pc_for_spawner.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                // `spawn_fifo_shards` is sync for Kafka — no `.await` needed.
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
        let group_id = super::constants::consumer_group_id_fifo(&queue_str);
        Self {
            queue: queue_str,
            group_id,
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

    /// The Kafka consumer group ID used by this group's broker-side consumers.
    ///
    /// Standard consumers return `"{queue}-consumer"` (or the override supplied
    /// via [`KafkaConsumerGroupConfig::with_group_id`]); FIFO consumers return
    /// `"{queue}-fifo"`. The autoscaler uses this to query committed offsets
    /// under the correct group — never re-derive it from the queue name.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn config(&self) -> &KafkaConsumerGroupConfig {
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
    /// Error and panic counts are snapshotted at the start and again after all
    /// awaits complete. Both the active-consumer and retiring drain loops sit
    /// between the two snapshots, preserving cancel-safety across both. The
    /// consumer list is drained via `pop()` so dropped futures leave unawaited
    /// handles in place for a subsequent escalation via
    /// [`Self::abort_remaining_into`].
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
        let mut options = ConsumerOptions::defaults_with_shutdown(child_token.clone());
        options.max_retries = self.config.max_retries;
        options.prefetch_count = self.config.prefetch_count;
        options.processing = processing.clone();
        options.handler_timeout = Some(resolve_handler_timeout(self.config.handler_timeout, None));
        if let Some(limit) = self.config.max_pending_per_key {
            options.max_pending_per_key = Some(limit);
        }
        options.max_message_size = self.config.max_message_size;
        options.consumer_group = Some(Arc::from(self.queue.as_str()));
        if let Some(ref gid) = self.config.group_id {
            options.kafka_group_id = Some(Arc::from(gid.as_str()));
        }
        options.kafka_auto_offset_reset = self.config.auto_offset_reset;
        #[cfg(feature = "kafka-schema-registry")]
        {
            options.schema_registry = self.config.schema_registry.clone();
            options.schema_enforcement = self.config.schema_enforcement;
            options.schema_accepted_subjects = self.config.schema_accepted_subjects.clone();
        }
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
    pub(super) default_handler_timeout: Option<Duration>,
    /// Replication factor applied to every auto-created topic (main + DLQ)
    /// declared via `register` / `register_fifo`. `None` keeps the topology
    /// declarer default (`1`) — suitable for single-broker dev clusters.
    /// Production clusters must set this via
    /// [`with_default_replication_factor`].
    ///
    /// [`with_default_replication_factor`]: Self::with_default_replication_factor
    default_replication_factor: Option<i32>,
}

impl KafkaConsumerGroupRegistry {
    pub fn new(client: KafkaClient) -> Self {
        Self {
            groups: HashMap::new(),
            client: Some(client),
            default_handler_timeout: None,
            default_replication_factor: None,
        }
    }

    /// Create a registry from a pre-populated map of groups (for testing).
    #[cfg(test)]
    pub(crate) fn from_groups(groups: HashMap<String, KafkaConsumerGroup>) -> Self {
        Self {
            groups,
            client: None,
            default_handler_timeout: None,
            default_replication_factor: None,
        }
    }

    /// Set the registry-level default handler timeout. Applies to every
    /// group whose `KafkaConsumerGroupConfig` did not explicitly call
    /// `with_handler_timeout`. Per-group explicit settings always win.
    pub fn with_default_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "default_handler_timeout must be positive"
        );
        self.default_handler_timeout = Some(timeout);
        self
    }

    /// Replication factor applied to every auto-created topic when groups
    /// register. Defaults to `1` (single-broker dev) when unset — production
    /// clusters should call this with `≥ 3` (or whatever the cluster's
    /// quorum sizing demands) or pre-create topics out-of-band via
    /// Terraform / MSK console. `create_topic` is idempotent and will not
    /// alter the replication of an already-existing topic.
    ///
    /// # Panics
    ///
    /// Panics if `n < 1`.
    pub fn with_default_replication_factor(mut self, n: i32) -> Self {
        assert!(n >= 1, "default_replication_factor must be >= 1 (got {n})");
        self.default_replication_factor = Some(n);
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
        config: KafkaConsumerGroupConfig,
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
                metrics::BackendLabel::Kafka,
                metrics::BackendErrorKind::Topology,
            );
            return Err(ShoveError::Topology(format!(
                "consumer group '{name}' is already registered"
            )));
        }

        let client = self.client.as_ref().ok_or_else(|| {
            ShoveError::Topology("registry has no client (test-only registry)".into())
        })?;

        let mut declarer = KafkaTopologyDeclarer::new(client.clone())
            .with_min_partitions(config.max_consumers as i32);
        if let Some(rf) = self.default_replication_factor {
            declarer = declarer.with_replication_factor(rf);
        }
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

    /// Register a new FIFO consumer group for a [`SequencedTopic`].
    ///
    /// Declares the topology for `T` before creating the group. The group is
    /// **not** started — call [`start_all`] separately.
    ///
    /// [`start_all`]: Self::start_all
    pub async fn register_fifo<T, H>(
        &mut self,
        config: KafkaConsumerGroupConfig,
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

        let mut declarer = KafkaTopologyDeclarer::new(client.clone()).with_min_partitions(1);
        if let Some(rf) = self.default_replication_factor {
            declarer = declarer.with_replication_factor(rf);
        }
        declarer.declare(topology).await?;

        info!(group = %name, "registering FIFO consumer group");
        let group_token = client.shutdown_token().child_token();
        let group = KafkaConsumerGroup::new_fifo::<T, H>(
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
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::DEFAULT_HANDLER_TIMEOUT;

    fn test_group(config: KafkaConsumerGroupConfig) -> KafkaConsumerGroup {
        let group_token = CancellationToken::new();
        let spawner: Spawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });

        KafkaConsumerGroup {
            queue: "test-queue".into(),
            group_id: "test-queue-consumer".into(),
            consumers: Vec::with_capacity(config.max_consumers as usize),
            config,
            spawner,
            group_token,
            error_count: Arc::new(AtomicUsize::new(0)),
            panic_count: Arc::new(AtomicUsize::new(0)),
            retiring: Vec::new(),
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

    fn hanging_test_group(config: KafkaConsumerGroupConfig) -> KafkaConsumerGroup {
        let mut group = test_group(config);
        group.spawner = Arc::new(|_options: ConsumerOptions| {
            tokio::spawn(async {
                std::future::pending::<()>().await;
            })
        });
        group
    }

    #[tokio::test]
    async fn scale_down_handle_is_drained_not_detached() {
        // A scaled-down consumer that hangs must still be aborted and counted,
        // not silently detached. Use a hanging spawner so the retiring handle
        // would leak under the old drop-the-handle behaviour.
        let mut group = hanging_test_group(KafkaConsumerGroupConfig::new(1..=3));
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
        assert_eq!(tally.panics, 0, "no spurious panics from cancelled handles");
    }

    #[tokio::test]
    async fn drain_into_timeout_preserves_atomics_in_tally() {
        let mut group = hanging_test_group(KafkaConsumerGroupConfig::new(2..=2));
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
    async fn abort_remaining_into_kills_hanging_consumers_and_keeps_tally() {
        let mut group = hanging_test_group(KafkaConsumerGroupConfig::new(2..=2));
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

    // -- handler timeout tri-state --

    #[test]
    fn inherit_config_uses_library_default_with_no_registry_default() {
        let cfg = KafkaConsumerGroupConfig::new(1..=4);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, None),
            DEFAULT_HANDLER_TIMEOUT,
        );
    }

    #[test]
    fn inherit_config_uses_registry_default_when_set() {
        let cfg = KafkaConsumerGroupConfig::new(1..=4);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(45),
        );
    }

    #[test]
    fn with_handler_timeout_beats_registry_default() {
        let cfg = KafkaConsumerGroupConfig::new(1..=4).with_handler_timeout(Duration::from_secs(5));
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(5),
        );
    }

    #[test]
    #[should_panic(expected = "handler_timeout must be positive")]
    fn with_handler_timeout_zero_panics() {
        let _ = KafkaConsumerGroupConfig::new(1..=4).with_handler_timeout(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "default_handler_timeout must be positive")]
    fn with_default_handler_timeout_zero_panics() {
        let registry = KafkaConsumerGroupRegistry::from_groups(HashMap::new());
        let _ = registry.with_default_handler_timeout(Duration::ZERO);
    }

    // -- configurable group_id (arch-K-3) --

    #[test]
    fn with_group_id_overrides_default() {
        // with_group_id() must exist and store the override
        let cfg = KafkaConsumerGroupConfig::new(1..=1).with_group_id("my-service");
        assert_eq!(cfg.group_id(), Some("my-service"));
    }

    #[test]
    fn without_group_id_returns_none() {
        let cfg = KafkaConsumerGroupConfig::new(1..=1);
        assert_eq!(cfg.group_id(), None);
    }

    // Regression test for the `with_group_id` propagation bug: the autoscaler
    // and the broker-side consumer must resolve the same group ID. Before the
    // fix, `KafkaConsumerGroup::new` stored `"{queue}-consumer"` regardless of
    // the override, while the broker-side consumer honored it — the autoscaler
    // then queried committed offsets under the wrong group and reported the
    // full partition watermark as backlog (phantom backlog → runaway scale-up).
    // Same class of bug as arch-K-1 on the FIFO path.
    #[test]
    fn resolved_group_id_returns_override_when_set() {
        let cfg = KafkaConsumerGroupConfig::new(1..=1).with_group_id("my-service");
        assert_eq!(cfg.resolved_group_id("orders"), "my-service");
    }

    #[test]
    fn resolved_group_id_falls_back_to_default_derivation() {
        let cfg = KafkaConsumerGroupConfig::new(1..=1);
        assert_eq!(cfg.resolved_group_id("orders"), "orders-consumer");
    }

    // -- auto.offset.reset --

    #[test]
    fn auto_offset_reset_defaults_to_none() {
        let cfg = KafkaConsumerGroupConfig::new(1..=1);
        assert_eq!(cfg.auto_offset_reset(), None);
    }

    #[test]
    fn with_auto_offset_reset_stores_override() {
        let cfg = KafkaConsumerGroupConfig::new(1..=1)
            .with_auto_offset_reset(KafkaAutoOffsetReset::Latest);
        assert_eq!(cfg.auto_offset_reset(), Some(KafkaAutoOffsetReset::Latest));
    }

    #[test]
    fn auto_offset_reset_rdkafka_strings_are_canonical() {
        assert_eq!(KafkaAutoOffsetReset::Earliest.as_rdkafka_str(), "earliest");
        assert_eq!(KafkaAutoOffsetReset::Latest.as_rdkafka_str(), "latest");
        assert_eq!(KafkaAutoOffsetReset::None.as_rdkafka_str(), "none");
    }

    // -- group_id (arch-K-1) --

    #[test]
    fn standard_group_id_is_queue_consumer() {
        let group = test_group(default_config());
        // test_group sets group_id = "test-queue-consumer" directly
        assert_eq!(group.group_id(), "test-queue-consumer");
    }

    #[test]
    fn fifo_group_id_must_not_match_standard_consumer_group_id() {
        // FIFO consumers MUST use "{queue}-fifo", never "{queue}-consumer".
        // If group_id() returns "{queue}-consumer" for a FIFO group, the
        // autoscaler will read committed offsets from the wrong group and
        // report phantom backlog (arch-K-1).
        //
        // We cannot call `new_fifo` from a unit test without a full KafkaClient,
        // so we construct the group struct directly with the FIFO group_id that
        // `new_fifo` sets via `consumer_group_id_fifo`.
        let fifo_group_id = super::super::constants::consumer_group_id_fifo("orders");
        assert_eq!(fifo_group_id, "orders-fifo");
        assert_ne!(
            fifo_group_id,
            super::super::constants::consumer_group_id("orders")
        );
    }

    // -- schema-registry group config (Task 8) --

    #[cfg(feature = "kafka-schema-registry")]
    mod schema_registry_group_config {
        use super::*;
        use crate::schema_registry::SchemaEnforcement;

        #[test]
        fn schema_enforcement_default_is_enforce() {
            let cfg = KafkaConsumerGroupConfig::new(1..=1);
            assert_eq!(cfg.schema_enforcement, SchemaEnforcement::Enforce);
            assert!(cfg.schema_registry.is_none());
            assert!(cfg.schema_accepted_subjects.is_none());
        }

        #[test]
        fn with_schema_enforcement_permissive_stores_permissive() {
            let cfg = KafkaConsumerGroupConfig::new(1..=1)
                .with_schema_enforcement(SchemaEnforcement::Permissive);
            assert_eq!(cfg.schema_enforcement, SchemaEnforcement::Permissive);
        }

        #[test]
        fn accept_schema_subjects_stores_subjects() {
            let cfg = KafkaConsumerGroupConfig::new(1..=1)
                .accept_schema_subjects(["orders-value", "orders-dlq-value"]);
            let subjects = cfg.schema_accepted_subjects.as_deref().unwrap();
            assert_eq!(subjects.len(), 2);
            assert!(subjects.iter().any(|s| s.as_ref() == "orders-value"));
            assert!(subjects.iter().any(|s| s.as_ref() == "orders-dlq-value"));
        }
    }
}
