use std::time::{Duration, Instant};

/// Tuning knobs for the autoscaler's polling and scaling decisions.
#[derive(Debug, Clone)]
pub struct AutoscalerConfig {
    /// How often the autoscaler checks queue depths. Default: 5 s.
    pub poll_interval: Duration,
    /// Trigger a scale-up when `messages_ready > capacity × scale_up_multiplier`.
    /// Default: 2.0
    pub scale_up_multiplier: f64,
    /// Trigger a scale-down when `messages_ready < capacity × scale_down_multiplier`.
    /// Default: 0.5
    pub scale_down_multiplier: f64,
    /// A scaling condition must be sustained for this long before action is
    /// taken, preventing flapping. Default: 10 s.
    pub hysteresis_duration: Duration,
    /// Minimum time between two scaling actions for the same group.
    /// Default: 30 s.
    pub cooldown_duration: Duration,
}

impl Default for AutoscalerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            scale_up_multiplier: 2.0,
            scale_down_multiplier: 0.5,
            hysteresis_duration: Duration::from_secs(10),
            cooldown_duration: Duration::from_secs(30),
        }
    }
}

/// Per-group mutable state tracked between polling iterations.
#[derive(Debug, Clone, Default)]
pub(crate) struct GroupScalingState {
    /// When the scale-up condition first became true (reset when it becomes false).
    pub scale_up_since: Option<Instant>,
    /// When the scale-down condition first became true (reset when it becomes false).
    pub scale_down_since: Option<Instant>,
    /// When the last actual scaling action was taken (used for cooldown).
    pub last_scaled_at: Option<Instant>,
}

impl GroupScalingState {
    /// Returns `true` when the group is still within the cooldown window.
    pub fn in_cooldown(&self, cooldown: Duration) -> bool {
        self.last_scaled_at
            .map(|t| t.elapsed() < cooldown)
            .unwrap_or(false)
    }
}

/// A snapshot of queue and consumer metrics used to drive scaling decisions.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ScalingMetrics {
    pub messages_ready: u64,
    pub messages_in_flight: u64,
    pub active_consumers: u16,
    pub prefetch_count: u16,
}

impl ScalingMetrics {
    pub fn new(
        messages_ready: u64,
        messages_in_flight: u64,
        active_consumers: u16,
        prefetch_count: u16,
    ) -> Self {
        Self {
            messages_ready,
            messages_in_flight,
            active_consumers,
            prefetch_count,
        }
    }

    /// Total message capacity across all active consumers.
    pub fn capacity(&self) -> u64 {
        self.active_consumers as u64 * self.prefetch_count as u64
    }
}

/// The outcome of a single scaling evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingDecision {
    ScaleUp(u16),
    ScaleDown(u16),
    Hold,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaling_metrics_new() {
        let m = ScalingMetrics::new(100, 20, 4, 10);
        assert_eq!(m.messages_ready, 100);
        assert_eq!(m.messages_in_flight, 20);
        assert_eq!(m.active_consumers, 4);
        assert_eq!(m.prefetch_count, 10);
    }

    #[test]
    fn scaling_metrics_capacity() {
        let m = ScalingMetrics::new(0, 0, 4, 10);
        assert_eq!(m.capacity(), 40);
    }

    #[test]
    fn scaling_metrics_capacity_zero_consumers() {
        let m = ScalingMetrics::new(0, 0, 0, 10);
        assert_eq!(m.capacity(), 0);
    }

    #[test]
    fn scaling_decision_hold_is_default() {
        let d = ScalingDecision::Hold;
        assert_eq!(d, ScalingDecision::Hold);
    }

    #[test]
    fn scaling_decision_scale_up_carries_magnitude() {
        let d = ScalingDecision::ScaleUp(3);
        assert_eq!(d, ScalingDecision::ScaleUp(3));
    }

    #[test]
    fn scaling_decision_scale_down_carries_magnitude() {
        let d = ScalingDecision::ScaleDown(2);
        assert_eq!(d, ScalingDecision::ScaleDown(2));
    }

    #[test]
    fn scaling_decision_equality() {
        assert_eq!(ScalingDecision::ScaleUp(1), ScalingDecision::ScaleUp(1));
        assert_ne!(ScalingDecision::ScaleUp(1), ScalingDecision::ScaleDown(1));
        assert_ne!(ScalingDecision::ScaleUp(1), ScalingDecision::Hold);
    }
}
