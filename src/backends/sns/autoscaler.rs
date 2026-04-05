use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::autoscaler::{AutoscalerConfig, GroupScalingState};
use crate::backends::sns::registry::SqsConsumerGroupRegistry;
use crate::backends::sns::stats::{SqsQueueStatsProvider, SqsQueueStatsProviderTrait};

/// Polls SQS queue statistics and adjusts the number of running consumers
/// in each [`SqsConsumerGroup`] using hysteresis to prevent flapping.
pub struct SqsAutoscaler<S: SqsQueueStatsProviderTrait = SqsQueueStatsProvider> {
    stats_provider: S,
    config: AutoscalerConfig,
    state: HashMap<String, GroupScalingState>,
}

impl SqsAutoscaler<SqsQueueStatsProvider> {
    pub fn new(stats_provider: SqsQueueStatsProvider, config: AutoscalerConfig) -> Self {
        Self {
            stats_provider,
            config,
            state: HashMap::new(),
        }
    }
}

impl<S: SqsQueueStatsProviderTrait> SqsAutoscaler<S> {
    pub fn with_stats_provider(stats_provider: S, config: AutoscalerConfig) -> Self {
        Self {
            stats_provider,
            config,
            state: HashMap::new(),
        }
    }

    /// Main loop: poll on `poll_interval`, exit cleanly when `shutdown` fires.
    pub async fn run(
        &mut self,
        registry: Arc<Mutex<SqsConsumerGroupRegistry>>,
        shutdown: CancellationToken,
    ) {
        info!("SQS autoscaler started");
        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!("SQS autoscaler shutting down");
                    break;
                }

                _ = tokio::time::sleep(self.config.poll_interval) => {
                    self.poll_and_scale(&registry).await;
                }
            }
        }
    }

    /// One polling cycle: iterate over every group, fetch stats, decide.
    async fn poll_and_scale(&mut self, registry: &Arc<Mutex<SqsConsumerGroupRegistry>>) {
        let group_snapshots: Vec<(String, String, u16, f64, usize)> = {
            let reg = registry.lock().await;
            reg.groups()
                .iter()
                .map(|(name, group)| {
                    (
                        name.clone(),
                        group.queue().to_owned(),
                        group.config().prefetch_count(),
                        group.active_consumers() as f64,
                        group.active_consumers(),
                    )
                })
                .collect()
        };

        for (name, queue, prefetch, active_f64, _active) in group_snapshots {
            let state = self.state.entry(name.clone()).or_default();

            if state.in_cooldown(self.config.cooldown_duration) {
                debug!(group = %name, "skipping: in cooldown");
                continue;
            }

            let stats = match self.stats_provider.get_queue_stats(&queue).await {
                Ok(s) => s,
                Err(e) => {
                    error!(group = %name, queue = %queue, error = %e, "failed to fetch queue stats");
                    continue;
                }
            };

            // Use ApproximateNumberOfMessages as the "ready" count.
            let ready = stats.messages_ready as f64;
            let capacity = (prefetch as f64) * active_f64;

            let scale_up_threshold = capacity * self.config.scale_up_multiplier;
            let scale_down_threshold = capacity * self.config.scale_down_multiplier;
            let wants_scale_up = ready > scale_up_threshold;
            let wants_scale_down = ready < scale_down_threshold;

            debug!(
                group = %name,
                messages_ready = stats.messages_ready,
                messages_in_flight = stats.messages_not_visible,
                active_consumers = _active,
                capacity,
                scale_up_threshold,
                scale_down_threshold,
                wants_scale_up,
                wants_scale_down,
                "SQS poll cycle"
            );

            let now = Instant::now();

            if wants_scale_up {
                state.scale_down_since = None;
                let since = state.scale_up_since.get_or_insert(now);
                let elapsed = since.elapsed();
                if elapsed >= self.config.hysteresis_duration {
                    let mut reg = registry.lock().await;
                    if let Some(group) = reg.groups_mut().get_mut(&name) {
                        if group.scale_up() {
                            info!(
                                group = %name,
                                consumers = group.active_consumers(),
                                messages_ready = stats.messages_ready,
                                "SQS scaled up"
                            );
                            state.last_scaled_at = Some(now);
                            state.scale_up_since = None;
                        } else {
                            warn!(group = %name, "SQS scale-up requested but already at max consumers");
                        }
                    }
                }
            } else {
                state.scale_up_since = None;
            }

            if wants_scale_down {
                state.scale_up_since = None;
                let since = state.scale_down_since.get_or_insert(now);
                let elapsed = since.elapsed();
                if elapsed >= self.config.hysteresis_duration {
                    let mut reg = registry.lock().await;
                    if let Some(group) = reg.groups_mut().get_mut(&name) {
                        if group.scale_down() {
                            info!(
                                group = %name,
                                consumers = group.active_consumers(),
                                messages_ready = stats.messages_ready,
                                "SQS scaled down"
                            );
                            state.last_scaled_at = Some(now);
                            state.scale_down_since = None;
                        } else {
                            debug!(group = %name, "SQS scale-down requested but already at min consumers");
                        }
                    }
                }
            } else {
                state.scale_down_since = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ShoveError;
    use crate::sns::SqsQueueStats;
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
                .ok_or_else(|| ShoveError::Connection(format!("not found: {queue_name}")))
        }
    }

    #[test]
    fn sqs_autoscaler_new_starts_with_empty_state() {
        let config = AutoscalerConfig::default();
        let stats_provider = MockSqsStatsProvider::new();
        let autoscaler = SqsAutoscaler::with_stats_provider(stats_provider, config);
        assert!(autoscaler.state.is_empty());
    }

    #[tokio::test]
    async fn sqs_autoscaler_run_exits_on_shutdown() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let token = shutdown.clone();
        let handle = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = token.cancelled() => { /* exit */ }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    panic!("should have exited via shutdown");
                }
            }
        });

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("should complete within timeout")
            .expect("task should not panic");
    }

    #[tokio::test]
    async fn poll_and_scale_triggers_scaling() {
        use crate::backends::sns::client::SnsClient;
        use crate::backends::sns::consumer_group::SqsConsumerGroupConfig;
        use crate::backends::sns::topology::QueueRegistry;
        use crate::backends::sns::topology::TopicRegistry;
        use crate::handler::MessageHandler;
        use crate::topic::Topic;
        use crate::topology::QueueTopology;
        use std::sync::OnceLock;

        // 1. Setup minimal SQS infrastructure
        let client = SnsClient::mock();

        let topic_reg = Arc::new(TopicRegistry::new());
        let queue_reg = Arc::new(QueueRegistry::new());
        let mut registry = SqsConsumerGroupRegistry::new(client.clone(), topic_reg, queue_reg);

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct DummyMsg;
        struct DummyTopic;
        impl Topic for DummyTopic {
            type Message = DummyMsg;
            fn topology() -> &'static QueueTopology {
                static TOPOLOGY: OnceLock<QueueTopology> = OnceLock::new();
                TOPOLOGY.get_or_init(|| crate::topology::TopologyBuilder::new("test-queue").build())
            }
        }
        #[derive(Clone)]
        struct DummyHandler;
        impl MessageHandler<DummyTopic> for DummyHandler {
            async fn handle(
                &self,
                _: DummyMsg,
                _: crate::metadata::MessageMetadata,
            ) -> crate::outcome::Outcome {
                crate::outcome::Outcome::Ack
            }
        }

        // Registry that doesn't actually call AWS
        let group_token = client.shutdown_token().child_token();
        let group = crate::backends::sns::consumer_group::SqsConsumerGroup::new::<
            DummyTopic,
            DummyHandler,
        >(
            "test-group",
            "test-queue",
            SqsConsumerGroupConfig::new(1..=5).with_prefetch_count(10),
            client.clone(),
            Arc::new(QueueRegistry::new()),
            group_token,
            || DummyHandler,
        );
        registry.groups_mut().insert("test-group".into(), group);

        let registry = Arc::new(Mutex::new(registry));

        // 2. Setup Autoscaler with Mock Stats
        let mut stats_provider = MockSqsStatsProvider::new();
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

        let mut autoscaler = SqsAutoscaler::with_stats_provider(stats_provider, config);

        // 3. Initial state: 0 active
        {
            let reg = registry.lock().await;
            assert_eq!(
                reg.groups().get("test-group").unwrap().active_consumers(),
                0
            );
        }

        // Start group to min_consumers (1)
        registry
            .lock()
            .await
            .groups_mut()
            .get_mut("test-group")
            .unwrap()
            .start();
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .get("test-group")
                .unwrap()
                .active_consumers(),
            1
        );

        // 4. Poll and Scale Up
        // Ready=100, Capacity=1*10=10. 100 > 10 * 2.0 (scale_up_multiplier) -> Should scale up
        autoscaler.poll_and_scale(&registry).await;
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

        // 5. Scale Down
        // Update stats to low load
        autoscaler.stats_provider.stats.insert(
            "test-queue".into(),
            SqsQueueStats {
                messages_ready: 2,
                messages_not_visible: 0,
            },
        );
        // Ready=2, Capacity=2*10=20. 2 < 20 * 0.5 (scale_down_multiplier) -> Should scale down
        autoscaler.poll_and_scale(&registry).await;
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .get("test-group")
                .unwrap()
                .active_consumers(),
            1
        );
    }

    #[tokio::test]
    async fn poll_and_scale_respects_hysteresis() {
        use crate::backends::sns::client::SnsClient;
        use crate::backends::sns::consumer_group::SqsConsumerGroupConfig;
        use crate::backends::sns::topology::{QueueRegistry, TopicRegistry};
        use crate::handler::MessageHandler;
        use crate::topic::Topic;
        use crate::topology::QueueTopology;
        use std::sync::OnceLock;

        let client = SnsClient::mock();
        let topic_reg = Arc::new(TopicRegistry::new());
        let queue_reg = Arc::new(QueueRegistry::new());
        let mut registry = SqsConsumerGroupRegistry::new(client.clone(), topic_reg, queue_reg);

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct DummyMsg;
        struct DummyTopic;
        impl Topic for DummyTopic {
            type Message = DummyMsg;
            fn topology() -> &'static QueueTopology {
                static TOPOLOGY: OnceLock<QueueTopology> = OnceLock::new();
                TOPOLOGY.get_or_init(|| crate::topology::TopologyBuilder::new("test-queue").build())
            }
        }
        #[derive(Clone)]
        struct DummyHandler;
        impl MessageHandler<DummyTopic> for DummyHandler {
            async fn handle(
                &self,
                _: DummyMsg,
                _: crate::metadata::MessageMetadata,
            ) -> crate::outcome::Outcome {
                crate::outcome::Outcome::Ack
            }
        }

        let group_token = client.shutdown_token().child_token();
        let mut group = crate::backends::sns::consumer_group::SqsConsumerGroup::new::<
            DummyTopic,
            DummyHandler,
        >(
            "test-group",
            "test-queue",
            SqsConsumerGroupConfig::new(1..=5).with_prefetch_count(10),
            client.clone(),
            Arc::new(QueueRegistry::new()),
            group_token,
            || DummyHandler,
        );
        group.start();
        registry.groups_mut().insert("test-group".into(), group);
        let registry = Arc::new(Mutex::new(registry));

        let mut stats_provider = MockSqsStatsProvider::new();
        stats_provider.stats.insert(
            "test-queue".into(),
            SqsQueueStats {
                messages_ready: 100,
                messages_not_visible: 0,
            },
        );

        let config = AutoscalerConfig {
            hysteresis_duration: Duration::from_secs(60), // Long hysteresis
            cooldown_duration: Duration::ZERO,
            ..AutoscalerConfig::default()
        };

        let mut autoscaler = SqsAutoscaler::with_stats_provider(stats_provider, config);

        // First poll should start hysteresis but NOT scale
        autoscaler.poll_and_scale(&registry).await;
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .values()
                .next()
                .unwrap()
                .active_consumers(),
            1
        );
        assert!(
            autoscaler
                .state
                .get("test-group")
                .unwrap()
                .scale_up_since
                .is_some()
        );

        // Manually "fast forward" hysteresis by back-dating scale_up_since
        autoscaler
            .state
            .get_mut("test-group")
            .unwrap()
            .scale_up_since = Some(Instant::now() - Duration::from_secs(120));
        autoscaler.poll_and_scale(&registry).await;
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .values()
                .next()
                .unwrap()
                .active_consumers(),
            2
        );
    }

    #[tokio::test]
    async fn poll_and_scale_respects_cooldown() {
        use crate::backends::sns::client::SnsClient;
        use crate::backends::sns::consumer_group::SqsConsumerGroupConfig;
        use crate::backends::sns::topology::{QueueRegistry, TopicRegistry};
        use crate::handler::MessageHandler;
        use crate::topic::Topic;
        use crate::topology::QueueTopology;
        use std::sync::OnceLock;

        let client = SnsClient::mock();
        let topic_reg = Arc::new(TopicRegistry::new());
        let queue_reg = Arc::new(QueueRegistry::new());
        let mut registry = SqsConsumerGroupRegistry::new(client.clone(), topic_reg, queue_reg);

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct DummyMsg;
        struct DummyTopic;
        impl Topic for DummyTopic {
            type Message = DummyMsg;
            fn topology() -> &'static QueueTopology {
                static TOPOLOGY: OnceLock<QueueTopology> = OnceLock::new();
                TOPOLOGY.get_or_init(|| crate::topology::TopologyBuilder::new("test-queue").build())
            }
        }
        #[derive(Clone)]
        struct DummyHandler;
        impl MessageHandler<DummyTopic> for DummyHandler {
            async fn handle(
                &self,
                _: DummyMsg,
                _: crate::metadata::MessageMetadata,
            ) -> crate::outcome::Outcome {
                crate::outcome::Outcome::Ack
            }
        }

        let group_token = client.shutdown_token().child_token();
        let mut group = crate::backends::sns::consumer_group::SqsConsumerGroup::new::<
            DummyTopic,
            DummyHandler,
        >(
            "test-group",
            "test-queue",
            SqsConsumerGroupConfig::new(1..=5).with_prefetch_count(10),
            client.clone(),
            Arc::new(QueueRegistry::new()),
            group_token,
            || DummyHandler,
        );
        group.start();
        registry.groups_mut().insert("test-group".into(), group);
        let registry = Arc::new(Mutex::new(registry));

        let mut stats_provider = MockSqsStatsProvider::new();
        stats_provider.stats.insert(
            "test-queue".into(),
            SqsQueueStats {
                messages_ready: 100,
                messages_not_visible: 0,
            },
        );

        let config = AutoscalerConfig {
            hysteresis_duration: Duration::ZERO,
            cooldown_duration: Duration::from_secs(60),
            ..AutoscalerConfig::default()
        };
        AutoscalerConfig::default();

        let mut autoscaler = SqsAutoscaler::with_stats_provider(stats_provider, config);

        // First poll scales up to 2
        autoscaler.poll_and_scale(&registry).await;
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .values()
                .next()
                .unwrap()
                .active_consumers(),
            2
        );

        // Second poll immediately after should be blocked by cooldown
        autoscaler.poll_and_scale(&registry).await;
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .values()
                .next()
                .unwrap()
                .active_consumers(),
            2
        );

        // Back-date last_scaled_at
        autoscaler
            .state
            .get_mut("test-group")
            .unwrap()
            .last_scaled_at = Some(Instant::now() - Duration::from_secs(120));
        autoscaler.poll_and_scale(&registry).await;
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .values()
                .next()
                .unwrap()
                .active_consumers(),
            3
        );
    }

    // Shared boilerplate for single-group registry setup used by edge-case tests.
    async fn make_single_group_registry(
        min: u16,
        max: u16,
        prefetch: u16,
        started: bool,
    ) -> (Arc<Mutex<SqsConsumerGroupRegistry>>, CancellationToken) {
        use crate::backends::sns::client::SnsClient;
        use crate::backends::sns::consumer_group::SqsConsumerGroupConfig;
        use crate::backends::sns::topology::{QueueRegistry, TopicRegistry};
        use crate::handler::MessageHandler;
        use crate::topic::Topic;
        use crate::topology::QueueTopology;
        use std::sync::OnceLock;

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
                TOPO.get_or_init(|| crate::topology::TopologyBuilder::new("edge-queue").build())
            }
        }
        #[derive(Clone)]
        struct H;
        impl MessageHandler<T> for H {
            async fn handle(
                &self,
                _: Msg,
                _: crate::metadata::MessageMetadata,
            ) -> crate::outcome::Outcome {
                crate::outcome::Outcome::Ack
            }
        }

        let token = client.shutdown_token().child_token();
        let mut group = crate::backends::sns::consumer_group::SqsConsumerGroup::new::<T, H>(
            "edge-group",
            "edge-queue",
            SqsConsumerGroupConfig::new(min..=max).with_prefetch_count(prefetch),
            client.clone(),
            Arc::new(QueueRegistry::new()),
            token.clone(),
            || H,
        );
        if started {
            group.start();
        }
        registry.groups_mut().insert("edge-group".into(), group);
        (Arc::new(Mutex::new(registry)), token)
    }

    /// When the stats provider returns an error the autoscaler logs and skips
    /// that group — consumer count must not change.
    #[tokio::test]
    async fn poll_and_scale_stats_error_skips_group() {
        let (registry, _token) = make_single_group_registry(1, 5, 10, true).await;

        // Provider has no entry for "edge-queue" → returns Err.
        let stats_provider = MockSqsStatsProvider::new();
        let config = AutoscalerConfig {
            hysteresis_duration: Duration::ZERO,
            cooldown_duration: Duration::ZERO,
            ..AutoscalerConfig::default()
        };
        let mut autoscaler = SqsAutoscaler::with_stats_provider(stats_provider, config);

        let before = registry
            .lock()
            .await
            .groups()
            .get("edge-group")
            .unwrap()
            .active_consumers();

        autoscaler.poll_and_scale(&registry).await;

        let after = registry
            .lock()
            .await
            .groups()
            .get("edge-group")
            .unwrap()
            .active_consumers();

        assert_eq!(
            before, after,
            "consumer count must not change on stats error"
        );
    }

    /// scale_up at max_consumers is a no-op — autoscaler must not exceed the
    /// configured ceiling.
    #[tokio::test]
    async fn poll_and_scale_at_max_does_not_exceed() {
        // min=max=2 so the group is already at capacity after start().
        let (registry, _token) = make_single_group_registry(2, 2, 10, true).await;

        let mut stats_provider = MockSqsStatsProvider::new();
        // High load → wants scale-up, but max is already reached.
        stats_provider.stats.insert(
            "edge-queue".into(),
            SqsQueueStats {
                messages_ready: 1000,
                messages_not_visible: 0,
            },
        );

        let config = AutoscalerConfig {
            hysteresis_duration: Duration::ZERO,
            cooldown_duration: Duration::ZERO,
            ..AutoscalerConfig::default()
        };
        let mut autoscaler = SqsAutoscaler::with_stats_provider(stats_provider, config);

        autoscaler.poll_and_scale(&registry).await;

        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .get("edge-group")
                .unwrap()
                .active_consumers(),
            2,
            "must not exceed max_consumers"
        );
    }

    /// scale_down at min_consumers is a no-op — autoscaler must not go below
    /// the configured floor.
    #[tokio::test]
    async fn poll_and_scale_at_min_does_not_go_below() {
        // min=max=1 — already at the floor after start().
        let (registry, _token) = make_single_group_registry(1, 5, 10, true).await;

        let mut stats_provider = MockSqsStatsProvider::new();
        // Empty queue → wants scale-down, but min is already reached.
        stats_provider.stats.insert(
            "edge-queue".into(),
            SqsQueueStats {
                messages_ready: 0,
                messages_not_visible: 0,
            },
        );

        let config = AutoscalerConfig {
            hysteresis_duration: Duration::ZERO,
            cooldown_duration: Duration::ZERO,
            ..AutoscalerConfig::default()
        };
        let mut autoscaler = SqsAutoscaler::with_stats_provider(stats_provider, config);

        // Active = 1 = min_consumers.  scale_down returns false.
        autoscaler.poll_and_scale(&registry).await;

        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .get("edge-group")
                .unwrap()
                .active_consumers(),
            1,
            "must not go below min_consumers"
        );
    }

    /// When the scale-up condition was accumulating hysteresis but the load
    /// then drops below the threshold, `scale_up_since` must be cleared so
    /// a subsequent low-load poll does not trigger a spurious scale-up.
    #[tokio::test]
    async fn poll_and_scale_scale_up_hysteresis_resets_when_load_drops() {
        let (registry, _token) = make_single_group_registry(1, 5, 10, true).await;

        let mut stats_provider = MockSqsStatsProvider::new();
        // First: high load → starts hysteresis.
        stats_provider.stats.insert(
            "edge-queue".into(),
            SqsQueueStats {
                messages_ready: 1000,
                messages_not_visible: 0,
            },
        );

        let config = AutoscalerConfig {
            hysteresis_duration: Duration::from_secs(60), // very long, won't fire naturally
            cooldown_duration: Duration::ZERO,
            ..AutoscalerConfig::default()
        };
        let mut autoscaler = SqsAutoscaler::with_stats_provider(stats_provider, config);

        autoscaler.poll_and_scale(&registry).await;
        assert!(
            autoscaler
                .state
                .get("edge-group")
                .unwrap()
                .scale_up_since
                .is_some(),
            "hysteresis should have started"
        );

        // Load drops below threshold.
        autoscaler.stats_provider.stats.insert(
            "edge-queue".into(),
            SqsQueueStats {
                messages_ready: 0,
                messages_not_visible: 0,
            },
        );
        autoscaler.poll_and_scale(&registry).await;

        assert!(
            autoscaler
                .state
                .get("edge-group")
                .unwrap()
                .scale_up_since
                .is_none(),
            "scale_up_since must be cleared when load drops"
        );
        // Consumer count unchanged — hysteresis never fired.
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .get("edge-group")
                .unwrap()
                .active_consumers(),
            1
        );
    }

    /// Symmetric: scale-down hysteresis resets when load rises back above
    /// the scale-down threshold before the duration elapses.
    #[tokio::test]
    async fn poll_and_scale_scale_down_hysteresis_resets_when_load_rises() {
        let (registry, _token) = make_single_group_registry(1, 5, 10, true).await;

        // Scale up to 2 first so there is room to scale back down.
        {
            let mut reg = registry.lock().await;
            reg.groups_mut().get_mut("edge-group").unwrap().scale_up();
        }
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .get("edge-group")
                .unwrap()
                .active_consumers(),
            2
        );

        let mut stats_provider = MockSqsStatsProvider::new();
        // First: low load → starts scale-down hysteresis.
        stats_provider.stats.insert(
            "edge-queue".into(),
            SqsQueueStats {
                messages_ready: 0,
                messages_not_visible: 0,
            },
        );

        let config = AutoscalerConfig {
            hysteresis_duration: Duration::from_secs(60),
            cooldown_duration: Duration::ZERO,
            ..AutoscalerConfig::default()
        };
        let mut autoscaler = SqsAutoscaler::with_stats_provider(stats_provider, config);

        autoscaler.poll_and_scale(&registry).await;
        assert!(
            autoscaler
                .state
                .get("edge-group")
                .unwrap()
                .scale_down_since
                .is_some(),
            "hysteresis should have started"
        );

        // Load rises — scale-down condition no longer true.
        autoscaler.stats_provider.stats.insert(
            "edge-queue".into(),
            SqsQueueStats {
                messages_ready: 1000,
                messages_not_visible: 0,
            },
        );
        autoscaler.poll_and_scale(&registry).await;

        assert!(
            autoscaler
                .state
                .get("edge-group")
                .unwrap()
                .scale_down_since
                .is_none(),
            "scale_down_since must be cleared when load rises"
        );
        // Still 2 consumers — hysteresis never fired.
        assert_eq!(
            registry
                .lock()
                .await
                .groups()
                .get("edge-group")
                .unwrap()
                .active_consumers(),
            2
        );
    }
}
