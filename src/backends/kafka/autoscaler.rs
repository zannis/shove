use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer as RdkafkaConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::metadata::Metadata;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::ShoveError;
use crate::autoscaler::{
    Autoscaler, AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics, Stabilized,
    ThresholdStrategy,
};
use crate::error::Result;

use super::client::KafkaClient;
use super::consumer_group::{KafkaAutoOffsetReset, KafkaConsumerGroupRegistry};
#[cfg(feature = "kafka-msk-iam")]
use super::msk_iam::MskIamContext;
#[cfg(feature = "kafka-msk-iam")]
use tokio_util::sync::CancellationToken;

/// Queue statistics fetched from Kafka consumer lag.
#[derive(Debug, Clone, Default)]
pub struct KafkaQueueStats {
    pub messages_pending: u64,
    pub messages_in_flight: u64,
}

/// Abstraction over Kafka consumer lag for fetching queue stats.
pub trait KafkaQueueStatsProvider: Send + Sync {
    /// Fetch lag statistics for `queue` under the given `group_id`.
    ///
    /// `queue` names the topic, `group_id` is the Kafka consumer group whose
    /// committed offsets are queried. Callers must pass the actual group ID
    /// (e.g. `"{queue}-fifo"` for FIFO groups) rather than re-deriving it from
    /// the queue name — re-derivation is what caused the bug fixed by arch-K-1.
    ///
    /// `reset` is the group's `auto.offset.reset` policy. It determines the
    /// effective start position — and therefore the lag — for partitions where
    /// the group has no usable committed offset, either because it never
    /// committed or because its commit falls outside the partition's retained
    /// range: `earliest` starts at the low watermark, `latest` at the high
    /// watermark (zero lag).
    fn get_queue_stats(
        &self,
        queue: &str,
        group_id: &str,
        reset: KafkaAutoOffsetReset,
    ) -> impl Future<Output = Result<KafkaQueueStats>> + Send;
}

/// Messages between `from` and the high watermark, clamped to `0..=u64::MAX`.
/// Widened to `i128` because `high - from` can exceed `i64` at the watermark
/// extremes.
fn messages_until(high: i64, from: i64) -> u64 {
    i128::from(high)
        .saturating_sub(i128::from(from))
        .clamp(0, i128::from(u64::MAX)) as u64
}

/// Lag for one partition given the group's committed offset and the
/// partition watermarks. When the group has no usable committed offset, the
/// effective start position depends on `auto.offset.reset`: `earliest` starts
/// at the low watermark, `latest` at the high watermark (zero lag).
fn partition_lag(committed: Option<i64>, low: i64, high: i64, reset: KafkaAutoOffsetReset) -> u64 {
    // Kafka discards a commit outside `[low, high]` — truncated by retention
    // below, or stranded past the log end by an unclean truncation above — and
    // applies `auto.offset.reset`, so lag is measured from the reset position
    // rather than from the stale offset.
    match committed.filter(|&offset| offset >= low && offset <= high) {
        Some(offset) => messages_until(high, offset),
        None => match reset {
            KafkaAutoOffsetReset::Latest => 0,
            // `None` refuses to start without a usable offset; report the
            // same backlog as `earliest` so the pathology is visible.
            KafkaAutoOffsetReset::Earliest | KafkaAutoOffsetReset::None => {
                messages_until(high, low)
            }
        },
    }
}

/// Total lag across a topic's partitions, each given as
/// `(committed, low, high)`. Saturates so one extreme partition cannot wrap the
/// total.
fn aggregate_lag(
    per_partition: impl IntoIterator<Item = (Option<i64>, i64, i64)>,
    reset: KafkaAutoOffsetReset,
) -> u64 {
    per_partition
        .into_iter()
        .fold(0u64, |total, (committed, low, high)| {
            total.saturating_add(partition_lag(committed, low, high, reset))
        })
}

/// Stats-only `BaseConsumer` that carries whichever `ClientContext` matches the
/// broker's auth mode. Under MSK IAM the consumer must hold an
/// [`MskIamContext`] so librdkafka's OAUTHBEARER refresh callback can mint
/// tokens; without it the metadata/committed-offset RPCs below fail
/// authentication. Mirrors the context selection used by the producer, stream
/// consumer, admin client, and metadata consumer.
enum StatsConsumer {
    Default(BaseConsumer),
    #[cfg(feature = "kafka-msk-iam")]
    MskIam(BaseConsumer<MskIamContext>),
}

impl StatsConsumer {
    fn fetch_metadata(&self, topic: Option<&str>, timeout: Duration) -> KafkaResult<Metadata> {
        match self {
            Self::Default(c) => c.fetch_metadata(topic, timeout),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.fetch_metadata(topic, timeout),
        }
    }

    fn committed_offsets(
        &self,
        tpl: TopicPartitionList,
        timeout: Duration,
    ) -> KafkaResult<TopicPartitionList> {
        match self {
            Self::Default(c) => c.committed_offsets(tpl, timeout),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.committed_offsets(tpl, timeout),
        }
    }

    fn fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> KafkaResult<(i64, i64)> {
        match self {
            Self::Default(c) => c.fetch_watermarks(topic, partition, timeout),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.fetch_watermarks(topic, partition, timeout),
        }
    }

    /// Service the client's event queue once. This is what fires librdkafka's
    /// OAUTHBEARER token-refresh callback: the blocking metadata/offset RPCs
    /// above never poll the queue themselves, so an MSK IAM consumer that is
    /// only ever used for stats would otherwise never acquire or refresh a
    /// token. The returned message (always `None` here — this consumer never
    /// subscribes) is dropped immediately.
    #[cfg(feature = "kafka-msk-iam")]
    fn serve_events(&self, timeout: Duration) {
        match self {
            Self::Default(c) => drop(c.poll(timeout)),
            Self::MskIam(c) => drop(c.poll(timeout)),
        }
    }
}

/// Default stats provider that queries Kafka consumer lag.
///
/// perf-K-10: caches one consumer per group_id so each autoscaler poll
/// reuses the connection + metadata cache instead of doing a fresh broker
/// handshake. Typical deployments have one or two distinct group_ids
/// (`{queue}-consumer` for standard groups, `{queue}-fifo` for FIFO).
pub struct KafkaLagStatsProvider {
    client: KafkaClient,
    consumers: StdMutex<HashMap<String, Arc<StatsConsumer>>>,
    /// Child of the client shutdown token: cancelled when the client shuts
    /// down *or* when this provider is dropped, whichever comes first. Bounds
    /// the OAUTHBEARER refresh threads (below) to the provider's lifetime so
    /// none outlive it.
    #[cfg(feature = "kafka-msk-iam")]
    refresh_shutdown: CancellationToken,
    #[cfg(feature = "kafka-msk-iam")]
    refresh_threads: StdMutex<Vec<std::thread::JoinHandle<()>>>,
}

impl KafkaLagStatsProvider {
    pub fn new(client: KafkaClient) -> Self {
        #[cfg(feature = "kafka-msk-iam")]
        let refresh_shutdown = client.shutdown_token().child_token();
        Self {
            #[cfg(feature = "kafka-msk-iam")]
            refresh_shutdown,
            #[cfg(feature = "kafka-msk-iam")]
            refresh_threads: StdMutex::new(Vec::new()),
            client,
            consumers: StdMutex::new(HashMap::new()),
        }
    }

    fn get_or_create_consumer(&self, group_id: &str) -> Result<Arc<StatsConsumer>> {
        let mut guard = self
            .consumers
            .lock()
            .map_err(|_| ShoveError::Topology("stats consumer cache poisoned".into()))?;
        if let Some(c) = guard.get(group_id) {
            return Ok(Arc::clone(c));
        }
        let mut cfg = self.client.base_config();
        cfg.set("group.id", group_id);

        #[cfg(feature = "kafka-msk-iam")]
        let arc = if let Some(ctx) = self.client.msk_context() {
            let consumer: BaseConsumer<MskIamContext> =
                cfg.create_with_context(ctx).map_err(|e| {
                    ShoveError::Topology(format!("failed to create MSK stats consumer: {e}"))
                })?;
            let arc = Arc::new(StatsConsumer::MskIam(consumer));
            // A stats-only consumer never polls, so nothing would service the
            // OAUTHBEARER token-refresh event. Pump its event queue on a
            // dedicated thread, scoped to this provider via refresh_shutdown.
            let handle =
                Self::spawn_token_refresh_loop(Arc::clone(&arc), self.refresh_shutdown.clone());
            if let Ok(mut threads) = self.refresh_threads.lock() {
                threads.push(handle);
            }
            arc
        } else {
            Arc::new(StatsConsumer::Default(cfg.create().map_err(|e| {
                ShoveError::Topology(format!("failed to create stats consumer: {e}"))
            })?))
        };

        #[cfg(not(feature = "kafka-msk-iam"))]
        let arc = Arc::new(StatsConsumer::Default(cfg.create().map_err(|e| {
            ShoveError::Topology(format!("failed to create stats consumer: {e}"))
        })?));

        guard.insert(group_id.to_string(), Arc::clone(&arc));
        Ok(arc)
    }

    /// Continuously serve the MSK IAM stats consumer's event queue so
    /// librdkafka can deliver the initial OAUTHBEARER token and the periodic
    /// (~every few minutes) refreshes. Runs on its own OS thread because
    /// `serve_events` blocks inside librdkafka; exits promptly once the client
    /// shutdown token is cancelled.
    #[cfg(feature = "kafka-msk-iam")]
    fn spawn_token_refresh_loop(
        consumer: Arc<StatsConsumer>,
        shutdown: CancellationToken,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            while !shutdown.is_cancelled() {
                consumer.serve_events(Duration::from_millis(250));
            }
        })
    }
}

#[cfg(feature = "kafka-msk-iam")]
impl Drop for KafkaLagStatsProvider {
    fn drop(&mut self) {
        // Stop the OAUTHBEARER refresh threads and wait for them so no
        // background work outlives the provider.
        self.refresh_shutdown.cancel();
        if let Ok(mut threads) = self.refresh_threads.lock() {
            for handle in threads.drain(..) {
                let _ = handle.join();
            }
        }
    }
}

impl KafkaQueueStatsProvider for KafkaLagStatsProvider {
    async fn get_queue_stats(
        &self,
        queue: &str,
        group_id: &str,
        reset: KafkaAutoOffsetReset,
    ) -> Result<KafkaQueueStats> {
        // perf-K-10: reuse a cached BaseConsumer keyed by group_id.
        let consumer = self.get_or_create_consumer(group_id)?;
        let queue = queue.to_string();

        // Fetch metadata to enumerate partitions.
        let partitions: Vec<i32> = {
            let c = Arc::clone(&consumer);
            let q = queue.clone();
            tokio::task::spawn_blocking(move || -> Result<Vec<i32>> {
                let metadata = c
                    .fetch_metadata(Some(&q), Duration::from_secs(5))
                    .map_err(|e| {
                        ShoveError::Connection(format!("failed to fetch metadata for {q}: {e}"))
                    })?;
                let topic_metadata = metadata
                    .topics()
                    .first()
                    .ok_or_else(|| ShoveError::Topology(format!("no metadata for topic {q}")))?;
                Ok(topic_metadata.partitions().iter().map(|p| p.id()).collect())
            })
            .await
            .map_err(|e| ShoveError::Topology(format!("metadata task failed: {e}")))??
        };

        // Build a single TopicPartitionList for all partitions so we
        // fetch committed offsets in one RPC instead of N.
        let mut tpl = TopicPartitionList::new();
        for &pid in &partitions {
            tpl.add_partition(&queue, pid);
        }

        // perf-K-11: committed_offsets can fail with transient errors (e.g.
        // NotCoordinator) when the group coordinator hasn't been elected.
        // Drive the retry loop with tokio::time::sleep between attempts so
        // we don't hold a spawn_blocking worker across the backoff (the old
        // code used std::thread::sleep, which could pin a worker for up to
        // 5 seconds under coordinator-election races).
        let committed = {
            let mut last_err = None;
            let mut result = None;
            for attempt in 0..5u32 {
                let c = Arc::clone(&consumer);
                let tpl_clone = tpl.clone();
                let r = tokio::task::spawn_blocking(move || {
                    c.committed_offsets(tpl_clone, Duration::from_secs(5))
                })
                .await
                .map_err(|e| ShoveError::Topology(format!("committed task failed: {e}")))?;
                match r {
                    Ok(c) => {
                        result = Some(c);
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        if attempt < 4 {
                            tokio::time::sleep(Duration::from_millis(500 * (attempt as u64 + 1)))
                                .await;
                        }
                    }
                }
            }
            result.ok_or_else(|| {
                ShoveError::Connection(format!(
                    "failed to get committed offsets for {queue}: {}",
                    last_err.unwrap()
                ))
            })?
        };

        // Per-partition watermark fetch is still serial (perf-K-12 — rdkafka
        // doesn't expose a batched end-watermarks API; staying serial here).
        let total_lag: u64 = {
            let c = Arc::clone(&consumer);
            let q = queue.clone();
            tokio::task::spawn_blocking(move || -> Result<u64> {
                let mut per_partition = Vec::with_capacity(partitions.len());
                for pid in partitions {
                    let (low, high) = c
                        .fetch_watermarks(&q, pid, Duration::from_secs(5))
                        .map_err(|e| {
                            ShoveError::Connection(format!(
                                "failed to fetch watermarks for {q}/{pid}: {e}"
                            ))
                        })?;
                    // Both a partition absent from the committed TPL and an
                    // `Offset::Invalid` entry mean "the group has never
                    // committed here" — map both to `None`.
                    let committed_offset =
                        committed
                            .find_partition(&q, pid)
                            .and_then(|elem| match elem.offset() {
                                rdkafka::Offset::Offset(o) => Some(o),
                                _ => None,
                            });
                    per_partition.push((committed_offset, low, high));
                }
                Ok(aggregate_lag(per_partition, reset))
            })
            .await
            .map_err(|e| ShoveError::Topology(format!("watermarks task failed: {e}")))??
        };

        Ok(KafkaQueueStats {
            messages_pending: total_lag,
            messages_in_flight: 0, // Kafka doesn't expose in-flight count easily
        })
    }
}

/// Backend that adapts a [`KafkaConsumerGroupRegistry`] to the generic [`AutoscalerBackend`] trait.
pub struct KafkaAutoscalerBackend<S: KafkaQueueStatsProvider = KafkaLagStatsProvider> {
    stats_provider: S,
    registry: Arc<Mutex<KafkaConsumerGroupRegistry>>,
}

impl KafkaAutoscalerBackend<KafkaLagStatsProvider> {
    /// Create a backend that talks to Kafka for queue stats.
    pub fn new(client: KafkaClient, registry: Arc<Mutex<KafkaConsumerGroupRegistry>>) -> Self {
        Self {
            stats_provider: KafkaLagStatsProvider::new(client),
            registry,
        }
    }

    /// Convenience constructor that wires up a fully-configured autoscaler with
    /// [`Stabilized<ThresholdStrategy>`] from a single [`AutoscalerConfig`].
    pub fn autoscaler(
        client: KafkaClient,
        registry: Arc<Mutex<KafkaConsumerGroupRegistry>>,
        config: AutoscalerConfig,
    ) -> Autoscaler<Self, Stabilized<ThresholdStrategy>> {
        let strategy = Stabilized::new(
            ThresholdStrategy {
                scale_up_multiplier: config.scale_up_multiplier,
                scale_down_multiplier: config.scale_down_multiplier,
            },
            config.hysteresis_duration,
            config.cooldown_duration,
        );
        let backend = Self::new(client, registry);
        Autoscaler::new(backend, strategy, config.poll_interval)
    }
}

impl<S: KafkaQueueStatsProvider> KafkaAutoscalerBackend<S> {
    /// Create a backend with an explicit stats provider (useful for testing).
    pub fn with_stats_provider(
        stats_provider: S,
        registry: Arc<Mutex<KafkaConsumerGroupRegistry>>,
    ) -> Self {
        Self {
            stats_provider,
            registry,
        }
    }
}

impl<S: KafkaQueueStatsProvider> AutoscalerBackend for KafkaAutoscalerBackend<S> {
    type GroupId = String;

    async fn list_groups(&self) -> Result<Vec<Self::GroupId>> {
        let reg = self.registry.lock().await;
        Ok(reg.groups().keys().cloned().collect())
    }

    async fn fetch_metrics(&self, group: &Self::GroupId) -> Result<ScalingMetrics> {
        let (queue, group_id, prefetch, active, reset) = {
            let mut reg = self.registry.lock().await;
            let g = reg
                .groups_mut()
                .get_mut(group)
                .ok_or_else(|| ShoveError::Topology(format!("group not found: {group}")))?;
            // Supervision piggybacks the poll cadence: respawn dead members
            // before reading counts so the metrics reflect the topped-up group.
            g.ensure_min();
            (
                g.queue().to_owned(),
                g.group_id().to_owned(),
                g.config().prefetch_count(),
                g.active_consumers(),
                g.config()
                    .auto_offset_reset()
                    .unwrap_or(KafkaAutoOffsetReset::Earliest),
            )
        };

        let stats = self
            .stats_provider
            .get_queue_stats(&queue, &group_id, reset)
            .await?;

        debug!(
            group = %group,
            queue = %queue,
            messages_pending = stats.messages_pending,
            messages_in_flight = stats.messages_in_flight,
            active_consumers = active,
            "fetched Kafka metrics"
        );

        Ok(ScalingMetrics::new(
            stats.messages_pending,
            stats.messages_in_flight,
            active as u16,
            prefetch,
        ))
    }

    async fn scale(&self, group: &Self::GroupId, decision: ScalingDecision) -> Result<()> {
        let mut reg = self.registry.lock().await;
        let g = reg
            .groups_mut()
            .get_mut(group)
            .ok_or_else(|| ShoveError::Topology(format!("group not found: {group}")))?;

        match decision {
            ScalingDecision::ScaleUp(n) => {
                let mut changed = false;
                for _ in 0..n {
                    if !g.scale_up() {
                        warn!(group = %group, "scale-up requested but already at max consumers");
                        break;
                    }
                    changed = true;
                }
                if changed {
                    info!(group = %group, consumers = g.active_consumers(), "Kafka scaled up");
                }
            }
            ScalingDecision::ScaleDown(n) => {
                let mut changed = false;
                for _ in 0..n {
                    if !g.scale_down() {
                        debug!(group = %group, "scale-down requested but already at min consumers");
                        break;
                    }
                    changed = true;
                }
                if changed {
                    info!(group = %group, consumers = g.active_consumers(), "Kafka scaled down");
                }
            }
            ScalingDecision::Hold => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autoscaler::{Autoscaler, Stabilized, ThresholdStrategy};
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::backend::ConsumerOptionsInner as ConsumerOptions;
    use crate::backends::kafka::constants::consumer_group_id;
    use crate::backends::kafka::consumer_group::{KafkaConsumerGroup, KafkaConsumerGroupConfig};
    use crate::supervision::RespawnSupervisor;
    use tokio_util::sync::CancellationToken;

    struct MockKafkaStatsProvider {
        stats: HashMap<String, KafkaQueueStats>,
    }

    impl MockKafkaStatsProvider {
        fn new() -> Self {
            Self {
                stats: HashMap::new(),
            }
        }
    }

    impl KafkaQueueStatsProvider for MockKafkaStatsProvider {
        async fn get_queue_stats(
            &self,
            queue: &str,
            _group_id: &str,
            _reset: KafkaAutoOffsetReset,
        ) -> Result<KafkaQueueStats> {
            self.stats
                .get(queue)
                .cloned()
                .ok_or_else(|| ShoveError::Topology(format!("not found: {queue}")))
        }
    }

    #[test]
    fn partition_lag_committed_normal_case() {
        assert_eq!(
            partition_lag(Some(40), 0, 100, KafkaAutoOffsetReset::Earliest),
            60
        );
        assert_eq!(
            partition_lag(Some(40), 0, 100, KafkaAutoOffsetReset::Latest),
            60
        );
    }

    #[test]
    fn partition_lag_committed_beyond_high_resets_per_policy() {
        // Past the high watermark is out of range too — Kafka discards the
        // commit and applies the reset policy.
        assert_eq!(
            partition_lag(Some(120), 0, 100, KafkaAutoOffsetReset::Earliest),
            100
        );
        assert_eq!(
            partition_lag(Some(120), 0, 100, KafkaAutoOffsetReset::Latest),
            0
        );
    }

    #[test]
    fn partition_lag_committed_at_high_is_in_range() {
        // A fully caught-up group sits exactly at the high watermark.
        assert_eq!(
            partition_lag(Some(100), 0, 100, KafkaAutoOffsetReset::Earliest),
            0
        );
    }

    #[test]
    fn partition_lag_never_committed_earliest_uses_low_watermark() {
        // Retention has truncated the log: low > 0. Lag is high - low, not
        // the full high watermark.
        assert_eq!(
            partition_lag(None, 30, 100, KafkaAutoOffsetReset::Earliest),
            70
        );
    }

    #[test]
    fn partition_lag_never_committed_latest_is_zero() {
        assert_eq!(
            partition_lag(None, 30, 100, KafkaAutoOffsetReset::Latest),
            0
        );
    }

    #[test]
    fn partition_lag_committed_below_low_earliest_resets_to_low_watermark() {
        // Retention advanced past the group's commit: Kafka rejects the offset
        // as out of range and applies `earliest`, so lag is high - low.
        assert_eq!(
            partition_lag(Some(5), 30, 100, KafkaAutoOffsetReset::Earliest),
            70
        );
    }

    #[test]
    fn partition_lag_committed_below_low_latest_is_zero() {
        assert_eq!(
            partition_lag(Some(5), 30, 100, KafkaAutoOffsetReset::Latest),
            0
        );
    }

    #[test]
    fn partition_lag_committed_below_low_none_reports_earliest_backlog() {
        assert_eq!(
            partition_lag(Some(5), 30, 100, KafkaAutoOffsetReset::None),
            70
        );
    }

    #[test]
    fn partition_lag_committed_at_low_is_in_range() {
        // The low watermark itself is a valid offset — no reset applies.
        assert_eq!(
            partition_lag(Some(30), 30, 100, KafkaAutoOffsetReset::Latest),
            70
        );
    }

    #[test]
    fn partition_lag_saturates_on_extreme_offsets() {
        assert_eq!(
            partition_lag(
                Some(i64::MIN),
                i64::MIN,
                i64::MAX,
                KafkaAutoOffsetReset::Latest
            ),
            u64::MAX
        );
        assert_eq!(
            partition_lag(None, i64::MIN, i64::MAX, KafkaAutoOffsetReset::Earliest),
            u64::MAX
        );
    }

    #[test]
    fn aggregate_lag_sums_partitions() {
        let parts = vec![(Some(40), 0, 100), (None, 30, 100), (Some(5), 30, 100)];
        assert_eq!(
            aggregate_lag(parts, KafkaAutoOffsetReset::Earliest),
            60 + 70 + 70
        );
    }

    #[test]
    fn aggregate_lag_saturates_instead_of_wrapping() {
        // Two partitions each at the full offset span: the sum exceeds u64.
        let parts = vec![
            (Some(i64::MIN), i64::MIN, i64::MAX),
            (Some(i64::MIN), i64::MIN, i64::MAX),
        ];
        assert_eq!(aggregate_lag(parts, KafkaAutoOffsetReset::Latest), u64::MAX);
    }

    #[test]
    fn aggregate_lag_of_no_partitions_is_zero() {
        assert_eq!(aggregate_lag(vec![], KafkaAutoOffsetReset::Earliest), 0);
    }

    type TestSpawner = Arc<dyn Fn(ConsumerOptions) -> tokio::task::JoinHandle<()> + Send + Sync>;

    fn make_test_group(
        queue: &str,
        config: KafkaConsumerGroupConfig,
        started: bool,
    ) -> KafkaConsumerGroup {
        let group_token = CancellationToken::new();
        let spawner: TestSpawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });

        let queue_str: String = queue.into();
        let group_id = consumer_group_id(&queue_str);
        let mut group = KafkaConsumerGroup {
            queue: queue_str,
            group_id,
            consumers: Vec::with_capacity(config.max_consumers() as usize),
            config,
            spawner,
            group_token,
            error_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            panic_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            retiring: Vec::new(),
            respawn: RespawnSupervisor::default(),
        };
        if started {
            group.start();
        }
        group
    }

    fn make_single_group_registry(
        min: u16,
        max: u16,
        prefetch: u16,
        started: bool,
    ) -> Arc<Mutex<KafkaConsumerGroupRegistry>> {
        let config = KafkaConsumerGroupConfig::new(min..=max).with_prefetch_count(prefetch);
        let group = make_test_group("test-queue", config, started);

        let mut groups = HashMap::new();
        groups.insert("test-group".to_string(), group);

        Arc::new(Mutex::new(KafkaConsumerGroupRegistry::from_groups(groups)))
    }

    #[tokio::test]
    async fn kafka_backend_list_groups() {
        let registry = make_single_group_registry(1, 5, 10, false);
        let backend =
            KafkaAutoscalerBackend::with_stats_provider(MockKafkaStatsProvider::new(), registry);
        let groups = backend.list_groups().await.unwrap();
        assert_eq!(groups, vec!["test-group".to_string()]);
    }

    #[tokio::test]
    async fn kafka_backend_fetch_metrics() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let mut stats_provider = MockKafkaStatsProvider::new();
        stats_provider.stats.insert(
            "test-queue".into(),
            KafkaQueueStats {
                messages_pending: 42,
                messages_in_flight: 7,
            },
        );

        let backend = KafkaAutoscalerBackend::with_stats_provider(stats_provider, registry);
        let metrics = backend
            .fetch_metrics(&"test-group".to_string())
            .await
            .unwrap();

        assert_eq!(metrics.messages_ready, 42);
        assert_eq!(metrics.messages_in_flight, 7);
        assert_eq!(metrics.active_consumers, 1);
        assert_eq!(metrics.prefetch_count, 10);
    }

    #[tokio::test]
    async fn kafka_backend_scale_up() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend = KafkaAutoscalerBackend::with_stats_provider(
            MockKafkaStatsProvider::new(),
            registry.clone(),
        );

        backend
            .scale(&"test-group".to_string(), ScalingDecision::ScaleUp(1))
            .await
            .unwrap();

        let count = registry
            .lock()
            .await
            .groups()
            .get("test-group")
            .unwrap()
            .active_consumers();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn kafka_backend_scale_down() {
        let registry = make_single_group_registry(1, 5, 10, true);
        {
            let mut reg = registry.lock().await;
            reg.groups_mut().get_mut("test-group").unwrap().scale_up();
        }
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .get("test-group")
                .unwrap()
                .active_consumers(),
            2
        );

        let backend = KafkaAutoscalerBackend::with_stats_provider(
            MockKafkaStatsProvider::new(),
            registry.clone(),
        );
        backend
            .scale(&"test-group".to_string(), ScalingDecision::ScaleDown(1))
            .await
            .unwrap();

        let count = registry
            .lock()
            .await
            .groups()
            .get("test-group")
            .unwrap()
            .active_consumers();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn kafka_backend_scale_up_clamped_at_max() {
        let registry = make_single_group_registry(1, 2, 10, true);
        let backend = KafkaAutoscalerBackend::with_stats_provider(
            MockKafkaStatsProvider::new(),
            registry.clone(),
        );

        backend
            .scale(&"test-group".to_string(), ScalingDecision::ScaleUp(10))
            .await
            .unwrap();

        let count = registry
            .lock()
            .await
            .groups()
            .get("test-group")
            .unwrap()
            .active_consumers();
        assert_eq!(count, 2, "should be clamped at max=2");
    }

    #[tokio::test]
    async fn kafka_backend_full_autoscaler_round_trip() {
        let registry = make_single_group_registry(1, 5, 10, true);

        let mut stats_provider = MockKafkaStatsProvider::new();
        stats_provider.stats.insert(
            "test-queue".into(),
            KafkaQueueStats {
                messages_pending: 100,
                messages_in_flight: 0,
            },
        );

        let config = AutoscalerConfig {
            hysteresis_duration: Duration::ZERO,
            cooldown_duration: Duration::ZERO,
            ..AutoscalerConfig::default()
        };

        let mut autoscaler = Autoscaler::new(
            KafkaAutoscalerBackend::with_stats_provider(stats_provider, registry.clone()),
            Stabilized::new(
                ThresholdStrategy {
                    scale_up_multiplier: config.scale_up_multiplier,
                    scale_down_multiplier: config.scale_down_multiplier,
                },
                config.hysteresis_duration,
                config.cooldown_duration,
            ),
            config.poll_interval,
        );

        let before = registry
            .lock()
            .await
            .groups()
            .get("test-group")
            .unwrap()
            .active_consumers();
        assert_eq!(before, 1);

        autoscaler.poll_and_scale().await;

        let after = registry
            .lock()
            .await
            .groups()
            .get("test-group")
            .unwrap()
            .active_consumers();
        assert_eq!(after, 2, "expected scale-up after poll_and_scale");
    }

    #[tokio::test]
    async fn kafka_backend_scale_hold_is_noop() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend = KafkaAutoscalerBackend::with_stats_provider(
            MockKafkaStatsProvider::new(),
            registry.clone(),
        );

        backend
            .scale(&"test-group".to_string(), ScalingDecision::Hold)
            .await
            .unwrap();

        let count = registry
            .lock()
            .await
            .groups()
            .get("test-group")
            .unwrap()
            .active_consumers();
        assert_eq!(count, 1, "Hold should not change consumer count");
    }

    #[tokio::test]
    async fn kafka_backend_fetch_metrics_unknown_group_fails() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend =
            KafkaAutoscalerBackend::with_stats_provider(MockKafkaStatsProvider::new(), registry);

        let result = backend
            .fetch_metrics(&"nonexistent-group".to_string())
            .await;
        assert!(
            result.is_err(),
            "fetch_metrics for unknown group should fail"
        );
    }

    #[tokio::test]
    async fn kafka_backend_scale_unknown_group_fails() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend =
            KafkaAutoscalerBackend::with_stats_provider(MockKafkaStatsProvider::new(), registry);

        let result = backend
            .scale(
                &"nonexistent-group".to_string(),
                ScalingDecision::ScaleUp(1),
            )
            .await;
        assert!(result.is_err(), "scale for unknown group should fail");
    }

    #[tokio::test]
    async fn kafka_backend_scale_down_clamped_at_min() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend = KafkaAutoscalerBackend::with_stats_provider(
            MockKafkaStatsProvider::new(),
            registry.clone(),
        );

        backend
            .scale(&"test-group".to_string(), ScalingDecision::ScaleDown(5))
            .await
            .unwrap();

        let count = registry
            .lock()
            .await
            .groups()
            .get("test-group")
            .unwrap()
            .active_consumers();
        assert_eq!(count, 1, "should stay at min=1");
    }

    // -- arch-K-1: autoscaler uses the group's actual consumer group ID --

    fn make_group_registry_with_group_id(
        queue: &str,
        group_id: &str,
    ) -> Arc<Mutex<KafkaConsumerGroupRegistry>> {
        let config = KafkaConsumerGroupConfig::new(1..=5).with_prefetch_count(10);
        let group_token = CancellationToken::new();
        type TestSpawner =
            Arc<dyn Fn(ConsumerOptions) -> tokio::task::JoinHandle<()> + Send + Sync>;
        let spawner: TestSpawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });
        let group = KafkaConsumerGroup {
            queue: queue.into(),
            group_id: group_id.into(),
            consumers: Vec::with_capacity(config.max_consumers() as usize),
            config,
            spawner,
            group_token,
            error_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            panic_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            retiring: Vec::new(),
            respawn: RespawnSupervisor::default(),
        };

        let mut groups = HashMap::new();
        groups.insert("test-group".to_string(), group);
        Arc::new(Mutex::new(KafkaConsumerGroupRegistry::from_groups(groups)))
    }

    #[tokio::test]
    async fn fetch_metrics_passes_stored_group_id_to_stats_provider() {
        // Standard group: group_id defaults to "{queue}-consumer". Regression
        // test for arch-K-1 on the standard path — without it the FIFO test
        // alone left the default-derivation case uncovered.
        let queue = "orders";
        let expected_group_id = format!("{queue}-consumer");
        let registry = make_group_registry_with_group_id(queue, &expected_group_id);

        struct AssertGroupIdProvider {
            expected_group_id: String,
            stats: KafkaQueueStats,
        }
        impl KafkaQueueStatsProvider for AssertGroupIdProvider {
            async fn get_queue_stats(
                &self,
                _queue: &str,
                group_id: &str,
                _reset: KafkaAutoOffsetReset,
            ) -> Result<KafkaQueueStats> {
                assert_eq!(
                    group_id, self.expected_group_id,
                    "autoscaler must pass the stored group_id to the stats provider"
                );
                Ok(self.stats.clone())
            }
        }

        let backend = KafkaAutoscalerBackend::with_stats_provider(
            AssertGroupIdProvider {
                expected_group_id,
                stats: KafkaQueueStats {
                    messages_pending: 10,
                    messages_in_flight: 0,
                },
            },
            registry,
        );

        let metrics = backend
            .fetch_metrics(&"test-group".to_string())
            .await
            .unwrap();
        assert_eq!(metrics.messages_ready, 10);
    }

    #[tokio::test]
    async fn fetch_metrics_uses_fifo_group_id_for_fifo_groups() {
        // FIFO group: group_id = "{queue}-fifo", NOT "{queue}-consumer".
        // Before the arch-K-1 fix, the autoscaler always derived group_id as
        // "{queue}-consumer", so committed offsets for the FIFO group were never
        // found and the fallback path reported the full partition watermark as lag.
        let queue = "orders";
        let fifo_group_id = format!("{queue}-fifo");
        let registry = make_group_registry_with_group_id(queue, &fifo_group_id);

        // Provider keyed by queue name; we verify the group_id forwarded to it.
        let mut stats = HashMap::new();
        stats.insert(
            queue.to_string(),
            KafkaQueueStats {
                messages_pending: 5,
                messages_in_flight: 0,
            },
        );

        // Use a provider that only answers when called with the FIFO group_id —
        // if the autoscaler passes "{queue}-consumer" instead, the call succeeds
        // because MockKafkaStatsProvider ignores group_id; so we use a dedicated
        // mock that enforces the group_id.
        struct AssertGroupIdProvider {
            expected_group_id: String,
            stats: KafkaQueueStats,
        }
        impl KafkaQueueStatsProvider for AssertGroupIdProvider {
            async fn get_queue_stats(
                &self,
                _queue: &str,
                group_id: &str,
                _reset: KafkaAutoOffsetReset,
            ) -> Result<KafkaQueueStats> {
                assert_eq!(
                    group_id, self.expected_group_id,
                    "autoscaler must pass the FIFO group_id to the stats provider"
                );
                Ok(self.stats.clone())
            }
        }

        let backend = KafkaAutoscalerBackend::with_stats_provider(
            AssertGroupIdProvider {
                expected_group_id: fifo_group_id,
                stats: KafkaQueueStats {
                    messages_pending: 5,
                    messages_in_flight: 0,
                },
            },
            registry,
        );

        let metrics = backend
            .fetch_metrics(&"test-group".to_string())
            .await
            .unwrap();
        assert_eq!(metrics.messages_ready, 5);
    }
}
