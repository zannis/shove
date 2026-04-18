use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::autoscaler::{
    Autoscaler, AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics, Stabilized,
    ThresholdStrategy,
};
use crate::error::{Result, ShoveError};

use super::client::InMemoryBroker;
use super::consumer_group::InMemoryConsumerGroupRegistry;

/// Queue statistics for the in-memory broker.
#[derive(Debug, Clone, Default)]
pub struct InMemoryQueueStats {
    pub messages_ready: u64,
    pub messages_in_flight: u64,
}

/// Abstraction over queue depth lookup (mockable for tests).
pub trait InMemoryQueueStatsProvider: Send + Sync {
    fn get_queue_stats(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<InMemoryQueueStats>> + Send;
}

/// Default stats provider that reads from a broker's queue state.
pub struct BrokerStatsProvider {
    broker: InMemoryBroker,
}

impl BrokerStatsProvider {
    pub fn new(broker: InMemoryBroker) -> Self {
        Self { broker }
    }
}

impl InMemoryQueueStatsProvider for BrokerStatsProvider {
    async fn get_queue_stats(&self, queue: &str) -> Result<InMemoryQueueStats> {
        let q = self.broker.lookup(queue)?;
        let messages_ready = q.buffer.lock().await.len() as u64;
        let messages_in_flight = q.in_flight.load(Ordering::Acquire);
        Ok(InMemoryQueueStats {
            messages_ready,
            messages_in_flight,
        })
    }
}

/// Backend that adapts an [`InMemoryConsumerGroupRegistry`] to the generic
/// [`AutoscalerBackend`] trait.
pub struct InMemoryAutoscalerBackend<S: InMemoryQueueStatsProvider = BrokerStatsProvider> {
    stats_provider: S,
    registry: Arc<Mutex<InMemoryConsumerGroupRegistry>>,
}

impl InMemoryAutoscalerBackend<BrokerStatsProvider> {
    pub fn new(
        broker: InMemoryBroker,
        registry: Arc<Mutex<InMemoryConsumerGroupRegistry>>,
    ) -> Self {
        Self {
            stats_provider: BrokerStatsProvider::new(broker),
            registry,
        }
    }

    /// Convenience constructor that wires up a fully-configured autoscaler with
    /// [`Stabilized<ThresholdStrategy>`] from a single [`AutoscalerConfig`].
    pub fn autoscaler(
        broker: InMemoryBroker,
        registry: Arc<Mutex<InMemoryConsumerGroupRegistry>>,
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
        let backend = Self::new(broker, registry);
        Autoscaler::new(backend, strategy, config.poll_interval)
    }
}

impl<S: InMemoryQueueStatsProvider> InMemoryAutoscalerBackend<S> {
    pub fn with_stats_provider(
        stats_provider: S,
        registry: Arc<Mutex<InMemoryConsumerGroupRegistry>>,
    ) -> Self {
        Self {
            stats_provider,
            registry,
        }
    }
}

impl<S: InMemoryQueueStatsProvider> AutoscalerBackend for InMemoryAutoscalerBackend<S> {
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
            messages_ready = stats.messages_ready,
            messages_in_flight = stats.messages_in_flight,
            active_consumers = active,
            "fetched in-memory metrics"
        );

        Ok(ScalingMetrics::new(
            stats.messages_ready,
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
            .ok_or_else(|| ShoveError::Connection(format!("group not found: {group}")))?;

        match decision {
            ScalingDecision::ScaleUp(n) => {
                for _ in 0..n {
                    if !g.scale_up() {
                        warn!(group = %group, "scale-up requested but already at max consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "in-memory scaled up");
            }
            ScalingDecision::ScaleDown(n) => {
                for _ in 0..n {
                    if !g.scale_down() {
                        debug!(group = %group, "scale-down requested but already at min consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "in-memory scaled down");
            }
            ScalingDecision::Hold => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::backends::inmemory::consumer_group::{
        InMemoryConsumerGroup, InMemoryConsumerGroupConfig, Spawner,
    };
    use crate::consumer::ConsumerOptions;
    use tokio_util::sync::CancellationToken;

    struct MockStats {
        stats: HashMap<String, InMemoryQueueStats>,
    }

    impl InMemoryQueueStatsProvider for MockStats {
        async fn get_queue_stats(&self, queue: &str) -> Result<InMemoryQueueStats> {
            self.stats
                .get(queue)
                .cloned()
                .ok_or_else(|| ShoveError::Topology(format!("not found: {queue}")))
        }
    }

    fn make_test_group(
        queue: &str,
        config: InMemoryConsumerGroupConfig,
        started: bool,
    ) -> InMemoryConsumerGroup {
        let group_token = CancellationToken::new();
        let spawner: Spawner = Arc::new(|options: ConsumerOptions| {
            tokio::spawn(async move {
                options.shutdown.cancelled().await;
            })
        });
        let mut group = InMemoryConsumerGroup {
            queue: queue.into(),
            consumers: Vec::with_capacity(config.max_consumers() as usize),
            config,
            spawner,
            group_token,
        };
        if started {
            group.start();
        }
        group
    }

    fn make_registry(
        min: u16,
        max: u16,
        prefetch: u16,
        started: bool,
    ) -> Arc<Mutex<InMemoryConsumerGroupRegistry>> {
        let config = InMemoryConsumerGroupConfig::new(min..=max).with_prefetch_count(prefetch);
        let group = make_test_group("test-queue", config, started);
        let mut groups = HashMap::new();
        groups.insert("test-group".to_string(), group);
        Arc::new(Mutex::new(InMemoryConsumerGroupRegistry::from_groups(
            groups,
        )))
    }

    #[tokio::test]
    async fn list_groups_returns_registered() {
        let registry = make_registry(1, 4, 10, false);
        let backend = InMemoryAutoscalerBackend::with_stats_provider(
            MockStats {
                stats: HashMap::new(),
            },
            registry,
        );
        let groups = backend.list_groups().await.unwrap();
        assert_eq!(groups, vec!["test-group".to_string()]);
    }

    #[tokio::test]
    async fn fetch_metrics_uses_stats_provider() {
        let registry = make_registry(1, 4, 10, true);
        let mut stats = HashMap::new();
        stats.insert(
            "test-queue".into(),
            InMemoryQueueStats {
                messages_ready: 42,
                messages_in_flight: 7,
            },
        );

        let backend = InMemoryAutoscalerBackend::with_stats_provider(MockStats { stats }, registry);
        let m = backend
            .fetch_metrics(&"test-group".to_string())
            .await
            .unwrap();
        assert_eq!(m.messages_ready, 42);
        assert_eq!(m.messages_in_flight, 7);
        assert_eq!(m.active_consumers, 1);
        assert_eq!(m.prefetch_count, 10);
    }

    #[tokio::test]
    async fn scale_up_invokes_group_scale_up() {
        let registry = make_registry(1, 4, 10, true);
        let backend = InMemoryAutoscalerBackend::with_stats_provider(
            MockStats {
                stats: HashMap::new(),
            },
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
}
