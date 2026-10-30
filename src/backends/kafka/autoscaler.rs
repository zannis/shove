use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "kafka-msk-iam")]
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer as RdkafkaConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::metadata::Metadata;
use rdkafka::types::RDKafkaRespErr;
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
    ///
    /// # Errors
    ///
    /// Returns an error when the lag cannot be read. An implementation must not
    /// answer a failed read with a lag of zero: the autoscaler reads zero lag as
    /// an empty queue and shrinks the group.
    fn get_queue_stats(
        &self,
        queue: &str,
        group_id: &str,
        reset: KafkaAutoOffsetReset,
    ) -> impl Future<Output = Result<KafkaQueueStats>> + Send;
}

/// Attempts at reading the group's committed offsets: the coordinator can
/// answer `NotCoordinator` while it is still being elected.
const COMMITTED_OFFSETS_ATTEMPTS: u32 = 5;

/// The wait after the n-th failed attempt is n times this.
const COMMITTED_OFFSETS_BACKOFF: Duration = Duration::from_millis(500);

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

/// One topic of a metadata answer, reduced to what the lag read needs.
struct TopicAnswer<'a> {
    name: &'a str,
    error: Option<RDKafkaRespErr>,
    partitions: Vec<i32>,
}

/// The partitions of `queue` in a metadata answer.
///
/// A topic the broker does not have, or one it could not describe, comes back
/// with an error and no partitions, and a broker can list partitions beside a
/// topic error too. Neither says anything about lag, so each is an error here:
/// an empty partition list would sum to a lag of zero.
fn queue_partitions<'a>(
    queue: &str,
    answer: impl IntoIterator<Item = TopicAnswer<'a>>,
) -> Result<Vec<i32>> {
    let topic = answer
        .into_iter()
        .find(|topic| topic.name == queue)
        .ok_or_else(|| {
            ShoveError::Topology(format!("the metadata answer has no topic named {queue}"))
        })?;
    if let Some(code) = topic.error {
        return Err(ShoveError::Topology(format!(
            "metadata for topic {queue} returned {code:?}"
        )));
    }
    if topic.partitions.is_empty() {
        return Err(ShoveError::Topology(format!(
            "topic {queue} has no partitions"
        )));
    }
    Ok(topic.partitions)
}

/// The committed offset of each of `partitions`, in the same order, from the
/// group's OffsetFetch answer: `None` for a partition the group has never
/// committed to, which the broker reports as an invalid offset, and for one the
/// answer leaves out.
///
/// A partition the broker answered with an error carries an invalid offset
/// too, but says nothing about the group's position. Read as a gap it would
/// count the whole retained log as lag under `earliest` and none of it under
/// `latest`, so it is an error here.
fn committed_by_partition(
    committed: &TopicPartitionList,
    topic: &str,
    partitions: &[i32],
) -> Result<Vec<Option<i64>>> {
    partitions
        .iter()
        .map(|&partition| {
            let Some(element) = committed.find_partition(topic, partition) else {
                return Ok(None);
            };
            element.error().map_err(|e| {
                ShoveError::Connection(format!(
                    "failed to get the committed offset of {topic}/{partition}: {e}"
                ))
            })?;
            Ok(match element.offset() {
                Offset::Offset(offset) => Some(offset),
                _ => None,
            })
        })
        .collect()
}

/// The offsets the broker reports for `partitions` at `at`, one per partition
/// in the same order: [`Offset::Beginning`] asks for the start of each log,
/// [`Offset::End`] for its end. One `ListOffsets` request goes to each
/// partition leader whatever the partition count.
fn list_offsets(
    consumer: &StatsConsumer,
    topic: &str,
    partitions: &[i32],
    at: Offset,
    timeout: Duration,
) -> Result<Vec<i64>> {
    let mut query = TopicPartitionList::new();
    for &partition in partitions {
        query
            .add_partition_offset(topic, partition, at)
            .map_err(|e| {
                ShoveError::Topology(format!("cannot ask for {topic}/{partition} offsets: {e}"))
            })?;
    }
    let answer = consumer.offsets_for_times(query, timeout).map_err(|e| {
        ShoveError::Connection(format!("failed to fetch watermarks for {topic}: {e}"))
    })?;
    partitions
        .iter()
        .map(|&partition| {
            let unanswered = || {
                ShoveError::Connection(format!(
                    "the broker gave no {at:?} offset for {topic}/{partition}"
                ))
            };
            let element = answer
                .find_partition(topic, partition)
                .ok_or_else(unanswered)?;
            element.error().map_err(|e| {
                ShoveError::Connection(format!(
                    "failed to fetch watermarks for {topic}/{partition}: {e}"
                ))
            })?;
            let raw = element.offset().to_raw().ok_or_else(unanswered)?;
            // An unanswered partition still holds the marker that was asked.
            if Some(raw) == at.to_raw() {
                return Err(unanswered());
            }
            Ok(raw)
        })
        .collect()
}

/// `(low, high)` from the start and end offsets of one partition, as
/// librdkafka's `rd_kafka_query_watermark_offsets` settles them: the smaller
/// is the low, and a partition whose start offset is missing (a negative
/// answer) takes the end offset as its low.
fn settle_watermarks(start: i64, end: i64) -> (i64, i64) {
    let (low, high) = if start < end {
        (start, end)
    } else {
        (end, start)
    };
    if low < 0 && high >= 0 {
        (high, high)
    } else {
        (low, high)
    }
}

/// `(low, high)` watermarks of each of `partitions`, in the same order, from
/// two batched rounds: the start offsets of every partition, then their end
/// offsets. Each pair equals what librdkafka's `fetch_watermarks` returns for
/// that partition, which asks the same two questions of one partition per call
/// and so costs a pair of requests per partition on a serial path.
fn batched_watermarks(
    consumer: &StatsConsumer,
    topic: &str,
    partitions: &[i32],
    timeout: Duration,
) -> Result<Vec<(i64, i64)>> {
    let starts = list_offsets(consumer, topic, partitions, Offset::Beginning, timeout)?;
    let ends = list_offsets(consumer, topic, partitions, Offset::End, timeout)?;
    Ok(starts
        .into_iter()
        .zip(ends)
        .map(|(start, end)| settle_watermarks(start, end))
        .collect())
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
    /// librdkafka's constructor may block on TLS material, so the caller runs
    /// this on a blocking thread.
    fn create(
        cfg: ClientConfig,
        #[cfg(feature = "kafka-msk-iam")] msk_context: Option<MskIamContext>,
    ) -> Result<Self> {
        #[cfg(feature = "kafka-msk-iam")]
        if let Some(ctx) = msk_context {
            let consumer: BaseConsumer<MskIamContext> =
                cfg.create_with_context(ctx).map_err(|e| {
                    ShoveError::Topology(format!("failed to create MSK stats consumer: {e}"))
                })?;
            return Ok(Self::MskIam(consumer));
        }
        let consumer: BaseConsumer = cfg
            .create()
            .map_err(|e| ShoveError::Topology(format!("failed to create stats consumer: {e}")))?;
        Ok(Self::Default(consumer))
    }

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

    /// Despite the name, an `Offset::Beginning` or `Offset::End` marker in
    /// place of a timestamp asks for the start or end offset of the partition:
    /// librdkafka sends it as the `ListOffsets` earliest or latest timestamp.
    fn offsets_for_times(
        &self,
        timestamps: TopicPartitionList,
        timeout: Duration,
    ) -> KafkaResult<TopicPartitionList> {
        match self {
            Self::Default(c) => c.offsets_for_times(timestamps, timeout),
            #[cfg(feature = "kafka-msk-iam")]
            Self::MskIam(c) => c.offsets_for_times(timestamps, timeout),
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
    /// Held across a consumer's creation, which runs on a blocking thread, so
    /// two polls of one group create one consumer.
    consumers: Mutex<HashMap<String, Arc<StatsConsumer>>>,
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
            consumers: Mutex::new(HashMap::new()),
        }
    }

    async fn get_or_create_consumer(&self, group_id: &str) -> Result<Arc<StatsConsumer>> {
        let mut consumers = self.consumers.lock().await;
        if let Some(c) = consumers.get(group_id) {
            return Ok(Arc::clone(c));
        }
        let mut cfg = self.client.base_config();
        cfg.set("group.id", group_id);
        #[cfg(feature = "kafka-msk-iam")]
        let msk_context = self.client.msk_context();

        let consumer = tokio::task::spawn_blocking(move || {
            StatsConsumer::create(
                cfg,
                #[cfg(feature = "kafka-msk-iam")]
                msk_context,
            )
        })
        .await
        .map_err(|e| ShoveError::Topology(format!("stats consumer task failed: {e}")))??;
        let arc = Arc::new(consumer);

        // A stats-only consumer never polls, so nothing would service the
        // OAUTHBEARER token-refresh event. Pump its event queue on a
        // dedicated thread, scoped to this provider via refresh_shutdown.
        #[cfg(feature = "kafka-msk-iam")]
        if matches!(*arc, StatsConsumer::MskIam(_)) {
            let handle =
                Self::spawn_token_refresh_loop(Arc::clone(&arc), self.refresh_shutdown.clone());
            if let Ok(mut threads) = self.refresh_threads.lock() {
                threads.push(handle);
            }
        }

        consumers.insert(group_id.to_string(), Arc::clone(&arc));
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
    /// # Errors
    ///
    /// Fails with [`ShoveError::Topology`] when the broker has no topic
    /// `queue`, describes it with an error such as `TOPIC_AUTHORIZATION_FAILED`,
    /// or lists no partitions for it, and with [`ShoveError::Connection`] when
    /// an offset query fails or the broker answers a partition's committed
    /// offset with an error. None of those answers yields a lag of zero.
    async fn get_queue_stats(
        &self,
        queue: &str,
        group_id: &str,
        reset: KafkaAutoOffsetReset,
    ) -> Result<KafkaQueueStats> {
        // perf-K-10: reuse a cached BaseConsumer keyed by group_id.
        let consumer = self.get_or_create_consumer(group_id).await?;
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
                queue_partitions(
                    &q,
                    metadata.topics().iter().map(|topic| TopicAnswer {
                        name: topic.name(),
                        error: topic.error(),
                        partitions: topic.partitions().iter().map(|p| p.id()).collect(),
                    }),
                )
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
            let mut attempt = 1u32;
            loop {
                let c = Arc::clone(&consumer);
                let tpl_clone = tpl.clone();
                let result = tokio::task::spawn_blocking(move || {
                    c.committed_offsets(tpl_clone, Duration::from_secs(5))
                })
                .await
                .map_err(|e| ShoveError::Topology(format!("committed task failed: {e}")))?;
                match result {
                    Ok(committed) => break committed,
                    Err(e) if attempt >= COMMITTED_OFFSETS_ATTEMPTS => {
                        return Err(ShoveError::Connection(format!(
                            "failed to get committed offsets for {queue}: {e}"
                        )));
                    }
                    Err(_) => {
                        tokio::time::sleep(COMMITTED_OFFSETS_BACKOFF.saturating_mul(attempt)).await;
                        attempt = attempt.saturating_add(1);
                    }
                }
            }
        };

        let committed_offsets = committed_by_partition(&committed, &queue, &partitions)?;

        let total_lag: u64 = {
            let c = Arc::clone(&consumer);
            let q = queue.clone();
            tokio::task::spawn_blocking(move || -> Result<u64> {
                let watermarks = batched_watermarks(&c, &q, &partitions, Duration::from_secs(5))?;
                let per_partition = committed_offsets
                    .into_iter()
                    .zip(watermarks)
                    .map(|(committed, (low, high))| (committed, low, high));
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
        let (queue, group_id, concurrent, prefetch, active, reset) = {
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
                g.config().concurrent_processing(),
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

        Ok(ScalingMetrics::for_group(
            stats.messages_pending,
            stats.messages_in_flight,
            active,
            concurrent,
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
    use crate::backends::kafka::client::KafkaConfig;
    use crate::backends::kafka::constants::consumer_group_id;
    use crate::backends::kafka::consumer_group::{KafkaConsumerGroup, KafkaConsumerGroupConfig};
    use crate::supervision::RespawnSupervisor;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{DefaultProducerContext, FutureProducer, FutureRecord};
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

    // -- the metadata answer for the queue --

    fn answer<'a>(
        name: &'a str,
        error: Option<RDKafkaRespErr>,
        partitions: &[i32],
    ) -> TopicAnswer<'a> {
        TopicAnswer {
            name,
            error,
            partitions: partitions.to_vec(),
        }
    }

    #[test]
    fn queue_partitions_lists_the_partitions_of_the_named_topic() {
        let partitions = queue_partitions(
            "orders",
            [
                answer("payments", None, &[0]),
                answer("orders", None, &[0, 1, 2]),
            ],
        )
        .expect("a healthy topic names its partitions");
        assert_eq!(partitions, vec![0, 1, 2]);
    }

    #[test]
    fn queue_partitions_refuses_an_answer_about_another_topic() {
        let err = queue_partitions("orders", [answer("payments", None, &[0, 1])])
            .expect_err("an answer for another topic says nothing about this one");
        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
        assert!(err.to_string().contains("orders"), "{err}");
    }

    #[test]
    fn queue_partitions_refuses_an_answer_without_topics() {
        let err = queue_partitions("orders", [])
            .expect_err("an empty answer says nothing about the topic");
        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
    }

    #[test]
    fn queue_partitions_refuses_a_topic_error_even_beside_partitions() {
        let err = queue_partitions(
            "orders",
            [answer(
                "orders",
                Some(RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED),
                &[0, 1],
            )],
        )
        .expect_err("an errored topic answers nothing about its partitions");
        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
        assert!(
            err.to_string().contains("TOPIC_AUTHORIZATION_FAILED"),
            "{err}"
        );
    }

    #[test]
    fn queue_partitions_refuses_a_topic_without_partitions() {
        let err = queue_partitions("orders", [answer("orders", None, &[])])
            .expect_err("a topic with no partitions has no lag to sum");
        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
    }

    // -- start and end offsets settle into watermarks as librdkafka does --

    #[test]
    fn settle_watermarks_keeps_a_healthy_pair() {
        assert_eq!(settle_watermarks(10, 40), (10, 40));
        assert_eq!(settle_watermarks(5, 5), (5, 5));
    }

    #[test]
    fn settle_watermarks_orders_a_swapped_pair() {
        assert_eq!(settle_watermarks(40, 10), (10, 40));
    }

    #[test]
    fn settle_watermarks_takes_the_end_for_a_missing_start() {
        assert_eq!(settle_watermarks(-1, 40), (40, 40));
        assert_eq!(settle_watermarks(40, -1), (40, 40));
    }

    // -- lag reads against a mock cluster --

    const MOCK_TIMEOUT: Duration = Duration::from_secs(10);

    /// A mock cluster holding `topic` with `counts.len()` partitions, the
    /// partition at index `i` holding `counts[i]` records.
    async fn mock_holding(
        topic: &str,
        counts: &[usize],
    ) -> MockCluster<'static, DefaultProducerContext> {
        let mock = MockCluster::new(1).expect("mock cluster");
        let partitions = i32::try_from(counts.len()).expect("partition count");
        mock.create_topic(topic, partitions, 1).expect("mock topic");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", mock.bootstrap_servers())
            .set("enable.idempotence", "false")
            .create()
            .expect("mock producer");
        for (partition, &count) in (0..).zip(counts) {
            for n in 0..count {
                producer
                    .send(
                        FutureRecord::to(topic)
                            .partition(partition)
                            .key("k")
                            .payload(&format!("record-{n}")),
                        MOCK_TIMEOUT,
                    )
                    .await
                    .map_err(|(e, _)| e)
                    .expect("produce to the mock cluster");
            }
        }
        mock
    }

    async fn lag_provider_on(
        mock: &MockCluster<'static, DefaultProducerContext>,
    ) -> KafkaLagStatsProvider {
        let client = KafkaClient::connect(&KafkaConfig::new(mock.bootstrap_servers()))
            .await
            .expect("connect to the mock cluster");
        KafkaLagStatsProvider::new(client)
    }

    #[tokio::test]
    async fn unknown_topic_stats_are_an_error() {
        let mock = MockCluster::new(1).expect("mock cluster");
        let provider = lag_provider_on(&mock).await;

        let err = provider
            .get_queue_stats(
                "no-such-topic",
                "no-such-topic-consumer",
                KafkaAutoOffsetReset::Earliest,
            )
            .await
            .expect_err("a topic the broker does not have has no lag to report");

        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
        assert!(err.to_string().contains("no-such-topic"), "{err}");
    }

    /// The mock lists the four partitions beside the topic error, as a real
    /// broker can for a topic the principal may not describe; trusting them
    /// would read an invalid snapshot as a lag of zero.
    #[tokio::test]
    async fn a_topic_level_metadata_error_refuses_the_lag_read() {
        let mock = MockCluster::new(1).expect("mock cluster");
        mock.create_topic("errored", 4, 1).expect("mock topic");
        mock.topic_error(
            "errored",
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
        )
        .expect("inject the topic error");
        let provider = lag_provider_on(&mock).await;

        let err = provider
            .get_queue_stats(
                "errored",
                "errored-consumer",
                KafkaAutoOffsetReset::Earliest,
            )
            .await
            .expect_err("an errored topic has no lag to report");

        assert!(matches!(err, ShoveError::Topology(_)), "got {err:?}");
        assert!(
            err.to_string().contains("TOPIC_AUTHORIZATION_FAILED"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn lag_sums_every_partition_of_a_mock_topic() {
        let counts = [3usize, 0, 5, 1, 0, 2];
        let mock = mock_holding("lag-sum", &counts).await;
        let provider = lag_provider_on(&mock).await;
        let total: u64 = counts.iter().map(|&c| c as u64).sum();

        let from_start = provider
            .get_queue_stats(
                "lag-sum",
                "lag-sum-consumer",
                KafkaAutoOffsetReset::Earliest,
            )
            .await
            .expect("lag from the start of the log");
        let from_tail = provider
            .get_queue_stats("lag-sum", "lag-sum-consumer", KafkaAutoOffsetReset::Latest)
            .await
            .expect("lag from the tail of the log");

        assert_eq!(from_start.messages_pending, total);
        assert_eq!(from_tail.messages_pending, 0);
    }

    fn stats_consumer_on(bootstrap: String) -> StatsConsumer {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap)
            .set("group.id", "watermark-group");
        StatsConsumer::create(
            cfg,
            #[cfg(feature = "kafka-msk-iam")]
            None,
        )
        .expect("stats consumer")
    }

    /// [`mock_holding`] for a test whose broker calls block, so it cannot run
    /// inside the test's own runtime.
    fn blocking_mock_holding(
        topic: &str,
        counts: &[usize],
    ) -> MockCluster<'static, DefaultProducerContext> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(mock_holding(topic, counts))
    }

    #[test]
    fn batched_watermarks_match_per_partition_watermarks() {
        let counts = [0usize, 3, 1, 0, 7, 2, 0, 5, 4, 0, 9, 1];
        let mock = blocking_mock_holding("watermarks", &counts);
        let partitions: Vec<i32> = (0..).take(counts.len()).collect();

        let batched = batched_watermarks(
            &stats_consumer_on(mock.bootstrap_servers()),
            "watermarks",
            &partitions,
            MOCK_TIMEOUT,
        )
        .expect("batched watermarks");

        let reference: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", mock.bootstrap_servers())
            .set("group.id", "watermark-reference")
            .create()
            .expect("reference consumer");
        let per_partition: Vec<(i64, i64)> = partitions
            .iter()
            .map(|&p| {
                reference
                    .fetch_watermarks("watermarks", p, MOCK_TIMEOUT)
                    .expect("per-partition watermarks")
            })
            .collect();

        assert_eq!(batched, per_partition);
        let highs: Vec<i64> = batched.iter().map(|&(_, high)| high).collect();
        let produced: Vec<i64> = counts.iter().map(|&c| c as i64).collect();
        assert_eq!(highs, produced, "the high watermark is the record count");
    }

    // -- the group's committed offsets --

    #[test]
    fn committed_by_partition_reads_commits_and_gaps() {
        let mut answer = TopicPartitionList::new();
        answer
            .add_partition_offset("orders", 0, Offset::Offset(5))
            .expect("a committed offset");
        answer
            .add_partition_offset("orders", 1, Offset::Invalid)
            .expect("the marker of a partition never committed to");

        let committed = committed_by_partition(&answer, "orders", &[0, 1, 2])
            .expect("an answer without errors");

        assert_eq!(committed, vec![Some(5), None, None]);
    }

    /// A broker answers a partition it will not report on, here one the topic
    /// does not have, with an error inside an otherwise healthy OffsetFetch
    /// answer, beside an invalid offset. Read as a gap, that partition would
    /// count the whole retained log as lag under `earliest` and none of it
    /// under `latest`.
    #[test]
    fn a_partition_the_broker_errored_on_refuses_the_committed_offsets() {
        let mock = blocking_mock_holding("committed", &[2, 3]);
        let consumer = stats_consumer_on(mock.bootstrap_servers());
        let mut asked = TopicPartitionList::new();
        for partition in [0, 1, 7] {
            asked.add_partition("committed", partition);
        }
        let answer = consumer
            .committed_offsets(asked, MOCK_TIMEOUT)
            .expect("the request itself succeeds");

        let healthy = committed_by_partition(&answer, "committed", &[0, 1])
            .expect("partitions the broker answers cleanly");
        assert_eq!(healthy, vec![None, None]);

        let err = committed_by_partition(&answer, "committed", &[0, 1, 7])
            .expect_err("an errored partition has no committed offset to read");
        assert!(matches!(err, ShoveError::Connection(_)), "got {err:?}");
        assert!(err.to_string().contains("committed/7"), "{err}");
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

    fn make_registry_with_config(
        config: KafkaConsumerGroupConfig,
        started: bool,
    ) -> Arc<Mutex<KafkaConsumerGroupRegistry>> {
        let group = make_test_group("test-queue", config, started);

        let mut groups = HashMap::new();
        groups.insert("test-group".to_string(), group);

        Arc::new(Mutex::new(KafkaConsumerGroupRegistry::from_groups(groups)))
    }

    fn make_single_group_registry(
        min: u16,
        max: u16,
        prefetch: u16,
        started: bool,
    ) -> Arc<Mutex<KafkaConsumerGroupRegistry>> {
        make_registry_with_config(
            KafkaConsumerGroupConfig::new(min..=max).with_prefetch_count(prefetch),
            started,
        )
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
        let registry = make_registry_with_config(
            KafkaConsumerGroupConfig::new(1..=5)
                .with_prefetch_count(10)
                .with_concurrent_processing(true),
            true,
        );
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

    /// What the autoscaler reads as the per-member capacity of a group
    /// registered with `config`, with lag from a real stats provider on a mock
    /// cluster.
    async fn reported_member_permits(config: KafkaConsumerGroupConfig) -> u16 {
        let mock = mock_holding("test-queue", &[0; 4]).await;
        let backend = KafkaAutoscalerBackend::with_stats_provider(
            lag_provider_on(&mock).await,
            make_registry_with_config(config, true),
        );
        let metrics = backend
            .fetch_metrics(&"test-group".to_string())
            .await
            .expect("fetch_metrics");
        assert_eq!(metrics.active_consumers, 1);
        metrics.prefetch_count
    }

    #[tokio::test]
    async fn sequential_group_capacity_is_one_per_member() {
        let config = KafkaConsumerGroupConfig::new(1..=5).with_prefetch_count(10);
        assert_eq!(reported_member_permits(config).await, 1);
    }

    #[tokio::test]
    async fn concurrent_group_capacity_is_the_prefetch_per_member() {
        let config = KafkaConsumerGroupConfig::new(1..=5)
            .with_prefetch_count(10)
            .with_concurrent_processing(true);
        assert_eq!(reported_member_permits(config).await, 10);
    }

    #[tokio::test]
    async fn zero_prefetch_group_capacity_is_one_per_member() {
        let config = KafkaConsumerGroupConfig::new(1..=5)
            .with_prefetch_count(0)
            .with_concurrent_processing(true);
        assert_eq!(reported_member_permits(config).await, 1);
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
