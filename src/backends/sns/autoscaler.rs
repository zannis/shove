use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::autoscaler::{
    AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics, Stabilized,
    ThresholdStrategy,
};
use crate::backends::sns::registry::SqsConsumerGroupRegistry;
use crate::backends::sns::stats::{SqsQueueStatsProvider, SqsQueueStatsProviderTrait};

/// Backend that adapts a [`SqsConsumerGroupRegistry`] to the generic [`AutoscalerBackend`] trait.
///
/// Generic over the queue-stats provider so that tests can inject a mock.
pub struct SqsAutoscalerBackend<S: SqsQueueStatsProviderTrait = SqsQueueStatsProvider> {
    stats_provider: S,
    registry: Arc<Mutex<SqsConsumerGroupRegistry>>,
}

impl SqsAutoscalerBackend<SqsQueueStatsProvider> {
    /// Create a backend that talks to SQS for queue stats.
    pub fn new(
        stats_provider: SqsQueueStatsProvider,
        registry: Arc<Mutex<SqsConsumerGroupRegistry>>,
    ) -> Self {
        Self {
            stats_provider,
            registry,
        }
    }

    /// Convenience constructor that wires up a fully-configured autoscaler with
    /// [`Stabilized<ThresholdStrategy>`] from a single [`AutoscalerConfig`].
    pub fn autoscaler(
        stats_provider: SqsQueueStatsProvider,
        registry: Arc<Mutex<SqsConsumerGroupRegistry>>,
        config: AutoscalerConfig,
    ) -> crate::autoscaler::Autoscaler<Self, Stabilized<ThresholdStrategy>> {
        let strategy = Stabilized::new(
            ThresholdStrategy {
                scale_up_multiplier: config.scale_up_multiplier,
                scale_down_multiplier: config.scale_down_multiplier,
            },
            config.hysteresis_duration,
            config.cooldown_duration,
        );
        let backend = Self::new(stats_provider, registry);
        crate::autoscaler::Autoscaler::new(backend, strategy, config.poll_interval)
    }
}

impl<S: SqsQueueStatsProviderTrait> SqsAutoscalerBackend<S> {
    /// Create a backend with an explicit stats provider (useful for testing).
    pub fn with_stats_provider(
        stats_provider: S,
        registry: Arc<Mutex<SqsConsumerGroupRegistry>>,
    ) -> Self {
        Self {
            stats_provider,
            registry,
        }
    }
}

impl<S: SqsQueueStatsProviderTrait> AutoscalerBackend for SqsAutoscalerBackend<S> {
    type GroupId = String;

    async fn list_groups(&self) -> crate::error::Result<Vec<Self::GroupId>> {
        let reg = self.registry.lock().await;
        Ok(reg.groups().keys().cloned().collect())
    }

    async fn fetch_metrics(&self, group: &Self::GroupId) -> crate::error::Result<ScalingMetrics> {
        let (queue, prefetch, active) = {
            let reg = self.registry.lock().await;
            let g = reg.groups().get(group).ok_or_else(|| {
                crate::error::ShoveError::Topology(format!("group not found: {group}"))
            })?;
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
            messages_ready = stats.messages_ready,
            messages_not_visible = stats.messages_not_visible,
            active_consumers = active,
            "fetched SQS metrics"
        );

        Ok(ScalingMetrics::new(
            stats.messages_ready,
            stats.messages_not_visible,
            active as u16,
            prefetch,
        ))
    }

    async fn scale(
        &self,
        group: &Self::GroupId,
        decision: ScalingDecision,
    ) -> crate::error::Result<()> {
        let mut reg = self.registry.lock().await;
        let g = reg.groups_mut().get_mut(group).ok_or_else(|| {
            crate::error::ShoveError::Connection(format!("group not found: {group}"))
        })?;

        match decision {
            ScalingDecision::ScaleUp(n) => {
                for _ in 0..n {
                    if !g.scale_up() {
                        warn!(group = %group, "scale-up requested but already at max consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "SQS scaled up");
            }
            ScalingDecision::ScaleDown(n) => {
                for _ in 0..n {
                    if !g.scale_down() {
                        debug!(group = %group, "scale-down requested but already at min consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "SQS scaled down");
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
    use crate::backends::sns::stats::SqsQueueStats;
    use crate::error::ShoveError;
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use std::time::Duration;

    struct MockSqsStatsProvider {
        stats: HashMap<String, SqsQueueStats>,
    }

    impl MockSqsStatsProvider {
        fn new() -> Self {
            Self {
                stats: HashMap::new(),
            }
        }
    }

    impl SqsQueueStatsProviderTrait for MockSqsStatsProvider {
        async fn get_queue_stats(&self, queue_name: &str) -> crate::error::Result<SqsQueueStats> {
            self.stats
                .get(queue_name)
                .cloned()
                .ok_or_else(|| ShoveError::Topology(format!("not found: {queue_name}")))
        }
    }

    async fn make_single_group_registry(
        min: u16,
        max: u16,
        prefetch: u16,
        started: bool,
    ) -> Arc<Mutex<SqsConsumerGroupRegistry>> {
        use crate::backends::sns::client::SnsClient;
        use crate::backends::sns::consumer_group::SqsConsumerGroupConfig;
        use crate::backends::sns::topology::{QueueRegistry, TopicRegistry};
        use crate::handler::MessageHandler;
        use crate::topic::Topic;
        use crate::topology::QueueTopology;

        let client = SnsClient::mock();
        let topic_reg = Arc::new(TopicRegistry::new());
        let queue_reg = Arc::new(QueueRegistry::new());
        let mut registry =
            SqsConsumerGroupRegistry::new(client.clone(), topic_reg, queue_reg.clone());

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct Msg;
        struct T;
        impl Topic for T {
            type Message = Msg;
            fn topology() -> &'static QueueTopology {
                static TOPO: OnceLock<QueueTopology> = OnceLock::new();
                TOPO.get_or_init(|| crate::topology::TopologyBuilder::new("test-queue").build())
            }
        }
        #[derive(Clone)]
        struct H;
        impl MessageHandler<T> for H {
            type Context = ();
            async fn handle(
                &self,
                _: Msg,
                _: crate::metadata::MessageMetadata,
                _: &(),
            ) -> crate::outcome::Outcome {
                crate::outcome::Outcome::Ack
            }
        }

        let token = client.shutdown_token().child_token();
        let mut group = crate::backends::sns::consumer_group::SqsConsumerGroup::new::<T, H>(
            "test-group",
            "test-queue",
            SqsConsumerGroupConfig::new(min..=max).with_prefetch_count(prefetch),
            client.clone(),
            Arc::new(QueueRegistry::new()),
            token.clone(),
            || H,
            (),
        );
        if started {
            group.start();
        }
        registry.groups_mut().insert("test-group".into(), group);
        Arc::new(Mutex::new(registry))
    }

    #[tokio::test]
    async fn sqs_backend_list_groups() {
        let registry = make_single_group_registry(1, 5, 10, false).await;
        let backend =
            SqsAutoscalerBackend::with_stats_provider(MockSqsStatsProvider::new(), registry);
        let groups = backend.list_groups().await.unwrap();
        assert_eq!(groups, vec!["test-group".to_string()]);
    }

    #[tokio::test]
    async fn sqs_backend_fetch_metrics() {
        let registry = make_single_group_registry(1, 5, 10, true).await;
        let mut stats_provider = MockSqsStatsProvider::new();
        stats_provider.stats.insert(
            "test-queue".into(),
            SqsQueueStats {
                messages_ready: 42,
                messages_not_visible: 7,
            },
        );

        let backend = SqsAutoscalerBackend::with_stats_provider(stats_provider, registry);
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
    async fn sqs_backend_scale_up() {
        let registry = make_single_group_registry(1, 5, 10, true).await;
        let backend = SqsAutoscalerBackend::with_stats_provider(
            MockSqsStatsProvider::new(),
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
    async fn sqs_backend_scale_down() {
        let registry = make_single_group_registry(1, 5, 10, true).await;
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

        let backend = SqsAutoscalerBackend::with_stats_provider(
            MockSqsStatsProvider::new(),
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
    async fn sqs_backend_scale_up_clamped_at_max() {
        // max=2, start at 1, request 10 scale-ups → should stop at 2
        let registry = make_single_group_registry(1, 2, 10, true).await;
        let backend = SqsAutoscalerBackend::with_stats_provider(
            MockSqsStatsProvider::new(),
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
    async fn sqs_backend_full_autoscaler_round_trip() {
        let registry = make_single_group_registry(1, 5, 10, true).await;

        let mut stats_provider = MockSqsStatsProvider::new();
        // High load: 100 ready, capacity=1*10=10, threshold=20 → ScaleUp
        stats_provider.stats.insert(
            "test-queue".into(),
            SqsQueueStats {
                messages_ready: 100,
                messages_not_visible: 0,
            },
        );

        let config = AutoscalerConfig {
            hysteresis_duration: Duration::ZERO,
            cooldown_duration: Duration::ZERO,
            ..AutoscalerConfig::default()
        };

        let mut autoscaler = Autoscaler::new(
            SqsAutoscalerBackend::with_stats_provider(stats_provider, registry.clone()),
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
}
