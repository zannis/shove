use crate::error::Result;
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::autoscaler::{
    AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics, Stabilized,
    ThresholdStrategy,
};
use crate::backends::rabbitmq::management::{
    ManagementClient, ManagementConfig, QueueStatsProvider,
};
use crate::backends::rabbitmq::registry::ConsumerGroupRegistry;
use crate::{Autoscaler, ShoveError};

/// Backend that adapts a [`ConsumerGroupRegistry`] to the generic [`AutoscalerBackend`] trait.
///
/// Generic over the queue-stats provider so that tests can inject a mock.
pub struct RabbitMqAutoscalerBackend<S: QueueStatsProvider = ManagementClient> {
    stats_provider: S,
    registry: Arc<Mutex<ConsumerGroupRegistry>>,
}

impl RabbitMqAutoscalerBackend<ManagementClient> {
    /// Create a backend that talks to the RabbitMQ Management HTTP API.
    pub fn new(
        mgmt_config: &ManagementConfig,
        registry: Arc<Mutex<ConsumerGroupRegistry>>,
    ) -> Self {
        Self {
            stats_provider: ManagementClient::new(mgmt_config.clone()),
            registry,
        }
    }

    /// Convenience constructor that wires up a fully-configured autoscaler with
    /// [`Stabilized<ThresholdStrategy>`] from a single [`AutoscalerConfig`].
    pub fn autoscaler(
        mgmt_config: &ManagementConfig,
        registry: Arc<Mutex<ConsumerGroupRegistry>>,
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
        let backend = Self::new(mgmt_config, registry);
        Autoscaler::new(backend, strategy, config.poll_interval)
    }
}

impl<S: QueueStatsProvider> RabbitMqAutoscalerBackend<S> {
    /// Create a backend with an explicit stats provider (useful for testing).
    pub fn with_stats_provider(
        stats_provider: S,
        registry: Arc<Mutex<ConsumerGroupRegistry>>,
    ) -> Self {
        Self {
            stats_provider,
            registry,
        }
    }
}

impl<S: QueueStatsProvider> AutoscalerBackend for RabbitMqAutoscalerBackend<S> {
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
            messages_unacknowledged = stats.messages_unacknowledged,
            active_consumers = active,
            "fetched metrics"
        );

        Ok(ScalingMetrics::new(
            stats.messages_ready,
            stats.messages_unacknowledged,
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
                info!(group = %group, consumers = g.active_consumers(), "scaled up");
            }
            ScalingDecision::ScaleDown(n) => {
                for _ in 0..n {
                    if !g.scale_down() {
                        debug!(group = %group, "scale-down requested but already at min consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "scaled down");
            }
            ScalingDecision::Hold => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use crate::autoscaler::GroupScalingState;

    // -- AutoscalerConfig --

    #[test]
    fn config_defaults() {
        let config = AutoscalerConfig::default();
        assert_eq!(config.poll_interval, Duration::from_secs(5));
        assert_eq!(config.scale_up_multiplier, 2.0);
        assert_eq!(config.scale_down_multiplier, 0.5);
        assert_eq!(config.hysteresis_duration, Duration::from_secs(10));
        assert_eq!(config.cooldown_duration, Duration::from_secs(30));
    }

    // -- GroupScalingState --

    #[test]
    fn state_new_is_empty() {
        let state = GroupScalingState::default();
        assert!(state.scale_up_since.is_none());
        assert!(state.scale_down_since.is_none());
        assert!(state.last_scaled_at.is_none());
    }

    #[test]
    fn in_cooldown_false_initially() {
        let state = GroupScalingState::default();
        assert!(!state.in_cooldown(Duration::from_secs(30)));
    }

    #[test]
    fn in_cooldown_true_during_cooldown() {
        let state = GroupScalingState {
            last_scaled_at: Some(Instant::now()),
            ..GroupScalingState::default()
        };
        assert!(state.in_cooldown(Duration::from_secs(30)));
    }

    #[test]
    fn in_cooldown_false_after_expiry() {
        let state = GroupScalingState {
            last_scaled_at: Some(Instant::now() - Duration::from_secs(60)),
            ..GroupScalingState::default()
        };
        assert!(!state.in_cooldown(Duration::from_secs(30)));
    }

    #[test]
    fn in_cooldown_zero_duration_always_expired() {
        let state = GroupScalingState {
            last_scaled_at: Some(Instant::now()),
            ..GroupScalingState::default()
        };
        assert!(!state.in_cooldown(Duration::ZERO));
    }
}
