use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer as RdkafkaConsumer};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::ShoveError;
use crate::autoscaler::{
    Autoscaler, AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics, Stabilized,
    ThresholdStrategy,
};
use crate::error::Result;

use super::client::KafkaClient;
use super::consumer_group::KafkaConsumerGroupRegistry;

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
    /// Both parameters are required: `queue` names the topic, `group_id` is the
    /// Kafka consumer group whose committed offsets are queried. Callers must pass
    /// the actual group ID (e.g. `"{queue}-fifo"` for FIFO groups) rather than
    /// re-deriving it from the queue name — re-derivation is what caused the bug
    /// fixed by arch-K-1.
    fn get_queue_stats(
        &self,
        queue: &str,
        group_id: &str,
    ) -> impl Future<Output = Result<KafkaQueueStats>> + Send;
}

/// Default stats provider that queries Kafka consumer lag.
///
/// perf-K-10: caches one BaseConsumer per group_id so each autoscaler poll
/// reuses the connection + metadata cache instead of doing a fresh broker
/// handshake. Typical deployments have one or two distinct group_ids
/// (`{queue}-consumer` for standard groups, `{queue}-fifo` for FIFO).
pub struct KafkaLagStatsProvider {
    client: KafkaClient,
    consumers: StdMutex<HashMap<String, Arc<BaseConsumer>>>,
}

impl KafkaLagStatsProvider {
    pub fn new(client: KafkaClient) -> Self {
        Self {
            client,
            consumers: StdMutex::new(HashMap::new()),
        }
    }

    fn get_or_create_consumer(&self, group_id: &str) -> Result<Arc<BaseConsumer>> {
        let mut guard = self
            .consumers
            .lock()
            .map_err(|_| ShoveError::Topology("stats consumer cache poisoned".into()))?;
        if let Some(c) = guard.get(group_id) {
            return Ok(Arc::clone(c));
        }
        let consumer: BaseConsumer = self
            .client
            .base_config()
            .set("group.id", group_id)
            .create()
            .map_err(|e| ShoveError::Topology(format!("failed to create stats consumer: {e}")))?;
        let arc = Arc::new(consumer);
        guard.insert(group_id.to_string(), Arc::clone(&arc));
        Ok(arc)
    }
}

impl KafkaQueueStatsProvider for KafkaLagStatsProvider {
    async fn get_queue_stats(&self, queue: &str, group_id: &str) -> Result<KafkaQueueStats> {
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
                let mut total: u64 = 0;
                for pid in partitions {
                    let (_low, high) = c
                        .fetch_watermarks(&q, pid, Duration::from_secs(5))
                        .map_err(|e| {
                            ShoveError::Connection(format!(
                                "failed to fetch watermarks for {q}/{pid}: {e}"
                            ))
                        })?;
                    if let Some(elem) = committed.find_partition(&q, pid) {
                        let committed_offset = match elem.offset() {
                            rdkafka::Offset::Offset(o) => o,
                            _ => 0,
                        };
                        if high > committed_offset {
                            total += (high - committed_offset) as u64;
                        }
                    } else {
                        total += high as u64;
                    }
                }
                Ok(total)
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
        let (queue, group_id, prefetch, active) = {
            let reg = self.registry.lock().await;
            let g = reg
                .groups()
                .get(group)
                .ok_or_else(|| ShoveError::Topology(format!("group not found: {group}")))?;
            (
                g.queue().to_owned(),
                g.group_id().to_owned(),
                g.config().prefetch_count(),
                g.active_consumers(),
            )
        };

        let stats = self
            .stats_provider
            .get_queue_stats(&queue, &group_id)
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
                for _ in 0..n {
                    if !g.scale_up() {
                        warn!(group = %group, "scale-up requested but already at max consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "Kafka scaled up");
            }
            ScalingDecision::ScaleDown(n) => {
                for _ in 0..n {
                    if !g.scale_down() {
                        debug!(group = %group, "scale-down requested but already at min consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "Kafka scaled down");
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
        async fn get_queue_stats(&self, queue: &str, _group_id: &str) -> Result<KafkaQueueStats> {
            self.stats
                .get(queue)
                .cloned()
                .ok_or_else(|| ShoveError::Topology(format!("not found: {queue}")))
        }
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
