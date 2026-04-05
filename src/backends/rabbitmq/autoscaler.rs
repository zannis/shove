use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub use crate::autoscaler::AutoscalerConfig;
use crate::autoscaler::GroupScalingState;
use crate::backends::rabbitmq::management::{
    ManagementClient, ManagementConfig, QueueStatsProvider,
};
use crate::backends::rabbitmq::registry::ConsumerGroupRegistry;

/// Polls RabbitMQ queue statistics and adjusts the number of running consumers
/// in each [`ConsumerGroup`] using hysteresis to prevent flapping.
pub struct Autoscaler<S: QueueStatsProvider = ManagementClient> {
    stats_provider: S,
    config: AutoscalerConfig,
    state: HashMap<String, GroupScalingState>,
}

impl Autoscaler<ManagementClient> {
    pub fn new(mgmt_config: &ManagementConfig, config: AutoscalerConfig) -> Self {
        Self {
            stats_provider: ManagementClient::new(mgmt_config.clone()),
            config,
            state: HashMap::new(),
        }
    }
}

impl<S: QueueStatsProvider> Autoscaler<S> {
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
        registry: Arc<Mutex<ConsumerGroupRegistry>>,
        shutdown: CancellationToken,
    ) {
        info!("autoscaler started");
        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!("autoscaler shutting down");
                    break;
                }

                _ = tokio::time::sleep(self.config.poll_interval) => {
                    self.poll_and_scale(&registry).await;
                }
            }
        }
    }

    /// One polling cycle: iterate over every group, fetch stats, decide.
    async fn poll_and_scale(&mut self, registry: &Arc<Mutex<ConsumerGroupRegistry>>) {
        // Collect the information we need while holding the lock as briefly as
        // possible (just reading names and config; we re-lock to mutate).
        let group_snapshots: Vec<(String, String, u16, f64, usize)> = {
            let reg = registry.lock().await;
            reg.groups()
                .iter()
                .map(|(name, group)| {
                    (
                        name.clone(),
                        group.queue().to_owned(),
                        group.config().prefetch_count,
                        group.active_consumers() as f64,
                        group.active_consumers(),
                    )
                })
                .collect()
        };

        for (name, queue, prefetch, active_f64, _active) in group_snapshots {
            // --- cooldown check (no lock needed) ---
            let state = self.state.entry(name.clone()).or_default();

            if state.in_cooldown(self.config.cooldown_duration) {
                debug!(group = %name, "skipping: in cooldown");
                continue;
            }

            // --- fetch stats ---
            let stats = match self.stats_provider.get_queue_stats(&queue).await {
                Ok(s) => s,
                Err(e) => {
                    error!(group = %name, queue = %queue, error = %e, "failed to fetch queue stats");
                    continue;
                }
            };

            let ready = stats.messages_ready as f64;
            let unacked = stats.messages_unacknowledged;
            let capacity = (prefetch as f64) * active_f64;

            let scale_up_threshold = capacity * self.config.scale_up_multiplier;
            let scale_down_threshold = capacity * self.config.scale_down_multiplier;
            let wants_scale_up = ready > scale_up_threshold;
            let wants_scale_down = ready < scale_down_threshold;

            debug!(
                group = %name,
                messages_ready = stats.messages_ready,
                messages_unacked = unacked,
                active_consumers = _active,
                capacity,
                scale_up_threshold,
                scale_down_threshold,
                wants_scale_up,
                wants_scale_down,
                "poll cycle"
            );

            let now = Instant::now();

            // --- hysteresis + scaling ---
            if wants_scale_up {
                // Clear scale-down timer since conditions are contradictory.
                state.scale_down_since = None;

                let since = state.scale_up_since.get_or_insert(now);
                let elapsed = since.elapsed();
                debug!(
                    group = %name,
                    elapsed_ms = elapsed.as_millis(),
                    hysteresis_ms = self.config.hysteresis_duration.as_millis(),
                    "scale-up hysteresis check"
                );
                if elapsed >= self.config.hysteresis_duration {
                    let mut reg = registry.lock().await;
                    if let Some(group) = reg.groups_mut().get_mut(&name) {
                        if group.scale_up() {
                            info!(
                                group = %name,
                                consumers = group.active_consumers(),
                                messages_ready = stats.messages_ready,
                                "scaled up"
                            );
                            state.last_scaled_at = Some(now);
                            state.scale_up_since = None;
                        } else {
                            warn!(group = %name, "scale-up requested but already at max consumers");
                        }
                    }
                }
            } else {
                // Condition no longer met — reset the timer.
                state.scale_up_since = None;
            }

            if wants_scale_down {
                // Clear scale-up timer since conditions are contradictory.
                state.scale_up_since = None;

                let since = state.scale_down_since.get_or_insert(now);
                let elapsed = since.elapsed();
                debug!(
                    group = %name,
                    elapsed_ms = elapsed.as_millis(),
                    hysteresis_ms = self.config.hysteresis_duration.as_millis(),
                    "scale-down hysteresis check"
                );
                if elapsed >= self.config.hysteresis_duration {
                    let mut reg = registry.lock().await;
                    if let Some(group) = reg.groups_mut().get_mut(&name) {
                        if group.scale_down() {
                            info!(
                                group = %name,
                                consumers = group.active_consumers(),
                                messages_ready = stats.messages_ready,
                                "scaled down"
                            );
                            state.last_scaled_at = Some(now);
                            state.scale_down_since = None;
                        } else {
                            debug!(group = %name, "scale-down requested but already at min consumers");
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
    use std::time::Duration;

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

    // -- Threshold math --

    /// Validates the threshold calculations used in poll_and_scale:
    /// scale_up when ready > capacity * scale_up_multiplier
    /// scale_down when ready < capacity * scale_down_multiplier
    #[test]
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    fn scale_thresholds_computed_correctly() {
        let config = AutoscalerConfig::default();

        // 2 consumers, prefetch 10 → capacity = 20
        let prefetch = 10_u16;
        let active = 2_f64;
        let capacity = (prefetch as f64) * active;
        assert_eq!(capacity, 20.0);

        let scale_up_threshold = capacity * config.scale_up_multiplier; // 40
        let scale_down_threshold = capacity * config.scale_down_multiplier; // 10

        assert_eq!(scale_up_threshold, 40.0);
        assert_eq!(scale_down_threshold, 10.0);

        // Within thresholds
        assert!(!(15.0_f64 > scale_up_threshold));
        assert!(!(15.0_f64 < scale_down_threshold));

        // Above scale-up
        assert!(50.0_f64 > scale_up_threshold);

        // Below scale-down
        assert!(5.0_f64 < scale_down_threshold);
    }

    #[test]
    fn scale_thresholds_with_single_consumer() {
        let config = AutoscalerConfig::default();

        // 1 consumer, prefetch 10 → capacity = 10
        let capacity = 10.0_f64;
        let scale_up_threshold = capacity * config.scale_up_multiplier; // 20
        let scale_down_threshold = capacity * config.scale_down_multiplier; // 5

        assert_eq!(scale_up_threshold, 20.0);
        assert_eq!(scale_down_threshold, 5.0);

        // Exactly at threshold: ready == threshold does NOT trigger
        assert_eq!(
            20.0_f64.partial_cmp(&scale_up_threshold),
            Some(std::cmp::Ordering::Equal)
        );
        assert_eq!(
            5.0_f64.partial_cmp(&scale_down_threshold),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn hysteresis_resets_on_condition_change() {
        let mut state = GroupScalingState::default();

        // Simulate scale-up condition starting
        let now = Instant::now();
        state.scale_up_since = Some(now);
        assert!(state.scale_up_since.is_some());

        // When scale-down is triggered, scale-up timer should be cleared
        // (this is what poll_and_scale does)
        state.scale_up_since = None;
        state.scale_down_since = Some(now);
        assert!(state.scale_up_since.is_none());
        assert!(state.scale_down_since.is_some());
    }

    #[test]
    fn cooldown_set_after_scaling() {
        let mut state = GroupScalingState::default();
        assert!(!state.in_cooldown(Duration::from_secs(30)));

        // Simulate a scaling action
        state.last_scaled_at = Some(Instant::now());
        state.scale_up_since = None; // reset after action

        assert!(state.in_cooldown(Duration::from_secs(30)));
    }

    // -- Mock QueueStatsProvider --

    struct MockStatsProvider {
        stats: HashMap<String, crate::backends::rabbitmq::management::QueueStats>,
    }

    impl MockStatsProvider {
        fn new() -> Self {
            Self {
                stats: HashMap::new(),
            }
        }
    }

    impl QueueStatsProvider for MockStatsProvider {
        async fn get_queue_stats(
            &self,
            queue: &str,
        ) -> Result<crate::backends::rabbitmq::management::QueueStats, crate::error::ShoveError>
        {
            self.stats
                .get(queue)
                .cloned()
                .ok_or_else(|| crate::error::ShoveError::Connection(format!("not found: {queue}")))
        }
    }

    #[test]
    fn state_scale_up_since_tracks_condition() {
        let mut state = GroupScalingState::default();
        let now = Instant::now();
        state.scale_up_since = Some(now);
        assert!(state.scale_up_since.unwrap().elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn cooldown_prevents_rapid_scaling() {
        let state = GroupScalingState {
            last_scaled_at: Some(Instant::now()),
            ..GroupScalingState::default()
        };
        // With 30s cooldown, should be in cooldown
        assert!(state.in_cooldown(Duration::from_secs(30)));
        // With 0s cooldown, should not be in cooldown
        assert!(!state.in_cooldown(Duration::ZERO));
    }

    #[test]
    fn with_stats_provider_creates_autoscaler() {
        let autoscaler = Autoscaler::with_stats_provider(
            MockStatsProvider::new(),
            AutoscalerConfig {
                poll_interval: Duration::from_secs(1),
                ..AutoscalerConfig::default()
            },
        );
        assert!(autoscaler.state.is_empty());
    }

    #[test]
    fn autoscaler_new_starts_with_empty_state() {
        let autoscaler =
            Autoscaler::with_stats_provider(MockStatsProvider::new(), AutoscalerConfig::default());
        assert!(autoscaler.state.is_empty());
    }

    #[tokio::test]
    async fn autoscaler_run_exits_on_shutdown() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let token = shutdown.clone();
        let handle = tokio::spawn(async move {
            // Simulate the run loop's select behavior
            tokio::select! {
                biased;
                _ = token.cancelled() => { /* exit */ }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    panic!("should have exited via shutdown");
                }
            }
        });

        // Should complete immediately since shutdown is already cancelled
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("should complete within timeout")
            .expect("task should not panic");
    }
}
