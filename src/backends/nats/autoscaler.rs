use async_nats::jetstream::consumer::pull::Config;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::ShoveError;
use crate::autoscaler::{
    Autoscaler, AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics, Stabilized,
    ThresholdStrategy,
};
use crate::error::Result;

use super::client::NatsClient;
use super::consumer_group::NatsConsumerGroupRegistry;

/// Queue statistics fetched from a NATS JetStream consumer.
#[derive(Debug, Clone, Default)]
pub struct NatsQueueStats {
    pub messages_pending: u64,
    pub messages_ack_pending: u64,
}

/// Abstraction over JetStream consumer info for fetching queue stats.
pub trait NatsQueueStatsProvider: Send + Sync {
    fn get_queue_stats(&self, queue: &str) -> impl Future<Output = Result<NatsQueueStats>> + Send;
}

/// Default stats provider that queries JetStream consumer info.
pub struct JetStreamStatsProvider {
    client: NatsClient,
}

impl JetStreamStatsProvider {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }
}

impl NatsQueueStatsProvider for JetStreamStatsProvider {
    async fn get_queue_stats(&self, queue: &str) -> Result<NatsQueueStats> {
        let mut stream = self
            .client
            .jetstream()
            .get_stream(queue)
            .await
            .map_err(|e| ShoveError::Topology(format!("failed to get stream {queue}: {e}")))?;

        let consumer_name = super::constants::consumer_name(queue);
        let consumer_result = stream.get_consumer::<Config>(&consumer_name).await;

        match consumer_result {
            Ok(mut consumer) => {
                let info = consumer.info().await.map_err(|e| {
                    ShoveError::Connection(format!(
                        "failed to get consumer info for {consumer_name}: {e}"
                    ))
                })?;

                Ok(NatsQueueStats {
                    messages_pending: info.num_pending,
                    messages_ack_pending: info.num_ack_pending as u64,
                })
            }
            Err(_) => {
                // Consumer hasn't been created yet — all stream messages are pending.
                let info = stream.info().await.map_err(|e| {
                    ShoveError::Connection(format!("failed to get stream info for {queue}: {e}"))
                })?;

                Ok(NatsQueueStats {
                    messages_pending: info.state.messages,
                    messages_ack_pending: 0,
                })
            }
        }
    }
}

/// Backend that adapts a [`NatsConsumerGroupRegistry`] to the generic [`AutoscalerBackend`] trait.
///
/// Generic over the queue-stats provider so that tests can inject a mock.
pub struct NatsAutoscalerBackend<S: NatsQueueStatsProvider = JetStreamStatsProvider> {
    stats_provider: S,
    registry: Arc<Mutex<NatsConsumerGroupRegistry>>,
}

impl NatsAutoscalerBackend<JetStreamStatsProvider> {
    /// Create a backend that talks to JetStream for queue stats.
    pub fn new(client: NatsClient, registry: Arc<Mutex<NatsConsumerGroupRegistry>>) -> Self {
        Self {
            stats_provider: JetStreamStatsProvider::new(client),
            registry,
        }
    }

    /// Convenience constructor that wires up a fully-configured autoscaler with
    /// [`Stabilized<ThresholdStrategy>`] from a single [`AutoscalerConfig`].
    pub fn autoscaler(
        client: NatsClient,
        registry: Arc<Mutex<NatsConsumerGroupRegistry>>,
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

impl<S: NatsQueueStatsProvider> NatsAutoscalerBackend<S> {
    /// Create a backend with an explicit stats provider (useful for testing).
    pub fn with_stats_provider(
        stats_provider: S,
        registry: Arc<Mutex<NatsConsumerGroupRegistry>>,
    ) -> Self {
        Self {
            stats_provider,
            registry,
        }
    }
}

impl<S: NatsQueueStatsProvider> AutoscalerBackend for NatsAutoscalerBackend<S> {
    type GroupId = String;

    async fn list_groups(&self) -> Result<Vec<Self::GroupId>> {
        let reg = self.registry.lock().await;
        Ok(reg.groups().keys().cloned().collect())
    }

    async fn fetch_metrics(&self, group: &Self::GroupId) -> Result<ScalingMetrics> {
        let (queue, prefetch, active) = {
            let reg = self.registry.lock().await;
            let g = reg
                .groups()
                .get(group)
                .ok_or_else(|| ShoveError::Topology(format!("group not found: {group}")))?;
            (
                g.queue().to_owned(),
                g.config().prefetch_count(),
                g.active_consumers(),
            )
        };

        let stats = self.stats_provider.get_queue_stats(&queue).await?;

        debug!(
            group = %group,
            queue = %queue,
            messages_pending = stats.messages_pending,
            messages_ack_pending = stats.messages_ack_pending,
            active_consumers = active,
            "fetched NATS metrics"
        );

        Ok(ScalingMetrics::new(
            stats.messages_pending,
            stats.messages_ack_pending,
            active as u16,
            prefetch,
        ))
    }

    async fn scale(&self, group: &Self::GroupId, decision: ScalingDecision) -> Result<()> {
        let mut reg = self.registry.lock().await;
        let g = reg
            .groups_mut()
            .get_mut(group)
            .ok_or_else(|| ShoveError::Connection(format!("group not found: {group}")))?;

        match decision {
            ScalingDecision::ScaleUp(n) => {
                for _ in 0..n {
                    if !g.scale_up() {
                        warn!(group = %group, "scale-up requested but already at max consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "NATS scaled up");
            }
            ScalingDecision::ScaleDown(n) => {
                for _ in 0..n {
                    if !g.scale_down() {
                        debug!(group = %group, "scale-down requested but already at min consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "NATS scaled down");
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
    use crate::backends::nats::consumer_group::{NatsConsumerGroup, NatsConsumerGroupConfig};
    use tokio_util::sync::CancellationToken;

    struct MockNatsStatsProvider {
        stats: HashMap<String, NatsQueueStats>,
    }

    impl MockNatsStatsProvider {
        fn new() -> Self {
            Self {
                stats: HashMap::new(),
            }
        }
    }

    impl NatsQueueStatsProvider for MockNatsStatsProvider {
        async fn get_queue_stats(&self, queue: &str) -> Result<NatsQueueStats> {
            self.stats
                .get(queue)
                .cloned()
                .ok_or_else(|| ShoveError::Topology(format!("not found: {queue}")))
        }
    }

    type Spawner = Arc<dyn Fn(ConsumerOptions) -> tokio::task::JoinHandle<()> + Send + Sync>;

    fn make_test_group(
        queue: &str,
        config: NatsConsumerGroupConfig,
        started: bool,
    ) -> NatsConsumerGroup {
        let group_token = CancellationToken::new();
        let spawner: Spawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });

        let mut group = NatsConsumerGroup {
            queue: queue.into(),
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
    ) -> Arc<Mutex<NatsConsumerGroupRegistry>> {
        let config = NatsConsumerGroupConfig::new(min..=max).with_prefetch_count(prefetch);
        let group = make_test_group("test-queue", config, started);

        let mut groups = HashMap::new();
        groups.insert("test-group".to_string(), group);

        Arc::new(Mutex::new(NatsConsumerGroupRegistry::from_groups(groups)))
    }

    #[tokio::test]
    async fn nats_backend_list_groups() {
        let registry = make_single_group_registry(1, 5, 10, false);
        let backend =
            NatsAutoscalerBackend::with_stats_provider(MockNatsStatsProvider::new(), registry);
        let groups = backend.list_groups().await.unwrap();
        assert_eq!(groups, vec!["test-group".to_string()]);
    }

    #[tokio::test]
    async fn nats_backend_fetch_metrics() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let mut stats_provider = MockNatsStatsProvider::new();
        stats_provider.stats.insert(
            "test-queue".into(),
            NatsQueueStats {
                messages_pending: 42,
                messages_ack_pending: 7,
            },
        );

        let backend = NatsAutoscalerBackend::with_stats_provider(stats_provider, registry);
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
    async fn nats_backend_scale_up() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend = NatsAutoscalerBackend::with_stats_provider(
            MockNatsStatsProvider::new(),
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
    async fn nats_backend_scale_down() {
        let registry = make_single_group_registry(1, 5, 10, true);
        // Scale up first so we have room to scale down
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

        let backend = NatsAutoscalerBackend::with_stats_provider(
            MockNatsStatsProvider::new(),
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
    async fn nats_backend_scale_up_clamped_at_max() {
        // max=2, start at 1, request 10 scale-ups -> should stop at 2
        let registry = make_single_group_registry(1, 2, 10, true);
        let backend = NatsAutoscalerBackend::with_stats_provider(
            MockNatsStatsProvider::new(),
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
    async fn nats_backend_full_autoscaler_round_trip() {
        let registry = make_single_group_registry(1, 5, 10, true);

        let mut stats_provider = MockNatsStatsProvider::new();
        // High load: 100 pending, capacity=1*10=10, threshold=20 -> ScaleUp
        stats_provider.stats.insert(
            "test-queue".into(),
            NatsQueueStats {
                messages_pending: 100,
                messages_ack_pending: 0,
            },
        );

        let config = AutoscalerConfig {
            hysteresis_duration: Duration::ZERO,
            cooldown_duration: Duration::ZERO,
            ..AutoscalerConfig::default()
        };

        let mut autoscaler = Autoscaler::new(
            NatsAutoscalerBackend::with_stats_provider(stats_provider, registry.clone()),
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
    async fn nats_backend_scale_hold_is_noop() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend = NatsAutoscalerBackend::with_stats_provider(
            MockNatsStatsProvider::new(),
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
    async fn nats_backend_fetch_metrics_unknown_group_fails() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend =
            NatsAutoscalerBackend::with_stats_provider(MockNatsStatsProvider::new(), registry);

        let result = backend
            .fetch_metrics(&"nonexistent-group".to_string())
            .await;
        assert!(
            result.is_err(),
            "fetch_metrics for unknown group should fail"
        );
    }

    #[tokio::test]
    async fn nats_backend_scale_unknown_group_fails() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend =
            NatsAutoscalerBackend::with_stats_provider(MockNatsStatsProvider::new(), registry);

        let result = backend
            .scale(
                &"nonexistent-group".to_string(),
                ScalingDecision::ScaleUp(1),
            )
            .await;
        assert!(result.is_err(), "scale for unknown group should fail");
    }

    #[tokio::test]
    async fn nats_backend_scale_down_clamped_at_min() {
        let registry = make_single_group_registry(1, 5, 10, true);
        let backend = NatsAutoscalerBackend::with_stats_provider(
            MockNatsStatsProvider::new(),
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
}
