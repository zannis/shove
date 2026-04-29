use crate::error::Result;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

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

/// A pluggable strategy for deciding how to scale a consumer group.
///
/// `&mut self` allows strategies to maintain internal state (e.g. hysteresis
/// counters). The `group` parameter lets a single strategy instance be shared
/// across groups while tracking per-group state.
pub trait ScalingStrategy: Send + Sync {
    fn evaluate(&mut self, group: &str, metrics: &ScalingMetrics) -> ScalingDecision;
}

/// A simple threshold-based scaling strategy.
///
/// Scales up when `messages_ready > capacity × scale_up_multiplier` and
/// scales down when `messages_ready < capacity × scale_down_multiplier`.
pub struct ThresholdStrategy {
    pub scale_up_multiplier: f64,
    pub scale_down_multiplier: f64,
}

impl Default for ThresholdStrategy {
    fn default() -> Self {
        Self {
            scale_up_multiplier: 2.0,
            scale_down_multiplier: 0.5,
        }
    }
}

impl ScalingStrategy for ThresholdStrategy {
    fn evaluate(&mut self, _group: &str, metrics: &ScalingMetrics) -> ScalingDecision {
        let capacity = metrics.capacity() as f64;
        let ready = metrics.messages_ready as f64;
        if ready > capacity * self.scale_up_multiplier {
            ScalingDecision::ScaleUp(1)
        } else if ready < capacity * self.scale_down_multiplier {
            ScalingDecision::ScaleDown(1)
        } else {
            ScalingDecision::Hold
        }
    }
}

/// A composable decorator that wraps any `ScalingStrategy` and adds
/// per-group hysteresis (a condition must be sustained before acting)
/// and cooldown (minimum time between consecutive scaling actions).
pub struct Stabilized<S: ScalingStrategy> {
    inner: S,
    hysteresis_duration: Duration,
    cooldown_duration: Duration,
    pub(crate) state: HashMap<String, GroupScalingState>,
}

impl<S: ScalingStrategy> Stabilized<S> {
    pub fn new(inner: S, hysteresis_duration: Duration, cooldown_duration: Duration) -> Self {
        Self {
            inner,
            hysteresis_duration,
            cooldown_duration,
            state: HashMap::new(),
        }
    }
}

impl<S: ScalingStrategy> ScalingStrategy for Stabilized<S> {
    fn evaluate(&mut self, group: &str, metrics: &ScalingMetrics) -> ScalingDecision {
        let raw = self.inner.evaluate(group, metrics);

        let state = self.state.entry(group.to_string()).or_default();

        if state.in_cooldown(self.cooldown_duration) {
            return ScalingDecision::Hold;
        }

        match raw {
            ScalingDecision::ScaleUp(n) => {
                state.scale_down_since = None;
                let since = state.scale_up_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= self.hysteresis_duration {
                    state.last_scaled_at = Some(Instant::now());
                    ScalingDecision::ScaleUp(n)
                } else {
                    ScalingDecision::Hold
                }
            }
            ScalingDecision::ScaleDown(n) => {
                state.scale_up_since = None;
                let since = state.scale_down_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= self.hysteresis_duration {
                    state.last_scaled_at = Some(Instant::now());
                    ScalingDecision::ScaleDown(n)
                } else {
                    ScalingDecision::Hold
                }
            }
            ScalingDecision::Hold => {
                state.scale_up_since = None;
                state.scale_down_since = None;
                ScalingDecision::Hold
            }
        }
    }
}

/// A backend that provides group discovery, metric fetching, and scaling
/// operations for the generic `Autoscaler`.
pub trait AutoscalerBackend: Send + Sync {
    type GroupId: Clone + Eq + Hash + Display + Send + Sync;

    fn list_groups(&self) -> impl Future<Output = Result<Vec<Self::GroupId>>> + Send;
    fn fetch_metrics(
        &self,
        group: &Self::GroupId,
    ) -> impl Future<Output = Result<ScalingMetrics>> + Send;
    fn scale(
        &self,
        group: &Self::GroupId,
        decision: ScalingDecision,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// A generic autoscaler that polls a backend and applies a scaling strategy.
pub struct Autoscaler<B: AutoscalerBackend, S: ScalingStrategy> {
    backend: B,
    strategy: S,
    poll_interval: Duration,
}

impl<B: AutoscalerBackend, S: ScalingStrategy> Autoscaler<B, S> {
    pub fn new(backend: B, strategy: S, poll_interval: Duration) -> Self {
        Self {
            backend,
            strategy,
            poll_interval,
        }
    }

    /// Run the autoscaler loop until the `shutdown` token is cancelled.
    pub async fn run(&mut self, shutdown: CancellationToken) {
        tracing::info!("autoscaler started");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::info!("autoscaler shutting down");
                    return;
                }
                _ = tokio::time::sleep(self.poll_interval) => {
                    self.poll_and_scale().await;
                }
            }
        }
    }

    /// Fetch groups and metrics, evaluate strategy, and issue scaling commands.
    pub async fn poll_and_scale(&mut self) {
        let groups = match self.backend.list_groups().await {
            Ok(g) => g,
            Err(e) => {
                tracing::error!("failed to list groups: {e}");
                return;
            }
        };

        for group in groups {
            let metrics = match self.backend.fetch_metrics(&group).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!("failed to fetch metrics for {group}: {e}");
                    continue;
                }
            };

            let group_str = group.to_string();
            let decision = self.strategy.evaluate(&group_str, &metrics);

            let (direction, target_workers): (&'static str, Option<u16>) = match &decision {
                ScalingDecision::ScaleUp(n) => ("up", Some(*n)),
                ScalingDecision::ScaleDown(n) => ("down", Some(*n)),
                ScalingDecision::Hold => ("hold", None),
            };
            crate::metrics::record_autoscaler_decision(&group_str, direction);

            if decision == ScalingDecision::Hold {
                continue;
            }

            match self.backend.scale(&group, decision).await {
                Ok(()) => {
                    if let Some(target) = target_workers {
                        crate::metrics::set_consumer_workers(&group_str, target as i64);
                    }
                }
                Err(e) => tracing::error!("failed to scale {group}: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ShoveError;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    struct MockBackend {
        groups: Vec<String>,
        metrics: HashMap<String, ScalingMetrics>,
        scale_log: Arc<Mutex<Vec<(String, ScalingDecision)>>>,
    }

    impl AutoscalerBackend for MockBackend {
        type GroupId = String;

        async fn list_groups(&self) -> Result<Vec<Self::GroupId>> {
            Ok(self.groups.clone())
        }

        async fn fetch_metrics(&self, group: &Self::GroupId) -> Result<ScalingMetrics> {
            self.metrics
                .get(group)
                .cloned()
                .ok_or_else(|| ShoveError::Topology(format!("no metrics for {group}")))
        }

        async fn scale(&self, group: &Self::GroupId, decision: ScalingDecision) -> Result<()> {
            self.scale_log.lock().await.push((group.clone(), decision));
            Ok(())
        }
    }

    #[tokio::test]
    async fn autoscaler_calls_strategy_for_each_group() {
        // group-a: 100 ready, cap=10 (1 consumer, 10 prefetch) → ScaleUp
        // group-b: 1 ready, cap=20 (2 consumers, 10 prefetch) → ScaleDown
        let mut metrics = HashMap::new();
        metrics.insert("group-a".into(), ScalingMetrics::new(100, 0, 1, 10));
        metrics.insert("group-b".into(), ScalingMetrics::new(1, 0, 2, 10));

        let scale_log = Arc::new(Mutex::new(vec![]));
        let backend = MockBackend {
            groups: vec!["group-a".into(), "group-b".into()],
            metrics,
            scale_log: scale_log.clone(),
        };
        let strategy = Stabilized::new(
            ThresholdStrategy::default(),
            Duration::from_secs(0),
            Duration::from_secs(0),
        );
        let mut autoscaler = Autoscaler::new(backend, strategy, Duration::from_secs(60));
        autoscaler.poll_and_scale().await;

        let log = scale_log.lock().await;
        assert_eq!(log.len(), 2);
        let has_a = log
            .iter()
            .any(|(g, d)| g == "group-a" && matches!(d, ScalingDecision::ScaleUp(_)));
        let has_b = log
            .iter()
            .any(|(g, d)| g == "group-b" && matches!(d, ScalingDecision::ScaleDown(_)));
        assert!(has_a, "expected ScaleUp for group-a, log: {log:?}");
        assert!(has_b, "expected ScaleDown for group-b, log: {log:?}");
    }

    #[tokio::test]
    async fn autoscaler_skips_hold_decisions() {
        // cap = 2*10 = 20, ready = 15 → within [10, 40] → Hold
        let mut metrics = HashMap::new();
        metrics.insert("group-a".into(), ScalingMetrics::new(15, 0, 2, 10));

        let scale_log = Arc::new(Mutex::new(vec![]));
        let backend = MockBackend {
            groups: vec!["group-a".into()],
            metrics,
            scale_log: scale_log.clone(),
        };
        let strategy = Stabilized::new(
            ThresholdStrategy::default(),
            Duration::from_secs(0),
            Duration::from_secs(0),
        );
        let mut autoscaler = Autoscaler::new(backend, strategy, Duration::from_secs(60));
        autoscaler.poll_and_scale().await;

        let log = scale_log.lock().await;
        assert!(log.is_empty(), "expected no scaling actions, log: {log:?}");
    }

    #[tokio::test]
    async fn autoscaler_continues_on_metrics_error() {
        // group-a has no metrics (will error), group-b should still be scaled
        let mut metrics = HashMap::new();
        metrics.insert("group-b".into(), ScalingMetrics::new(100, 0, 1, 10));

        let scale_log = Arc::new(Mutex::new(vec![]));
        let backend = MockBackend {
            groups: vec!["group-a".into(), "group-b".into()],
            metrics,
            scale_log: scale_log.clone(),
        };
        let strategy = Stabilized::new(
            ThresholdStrategy::default(),
            Duration::from_secs(0),
            Duration::from_secs(0),
        );
        let mut autoscaler = Autoscaler::new(backend, strategy, Duration::from_secs(60));
        autoscaler.poll_and_scale().await;

        let log = scale_log.lock().await;
        assert_eq!(
            log.len(),
            1,
            "expected only group-b to be scaled, log: {log:?}"
        );
        assert_eq!(log[0].0, "group-b");
        assert!(matches!(log[0].1, ScalingDecision::ScaleUp(_)));
    }

    #[tokio::test]
    async fn autoscaler_run_exits_on_shutdown() {
        let backend = MockBackend {
            groups: vec![],
            metrics: HashMap::new(),
            scale_log: Arc::new(Mutex::new(vec![])),
        };
        let strategy = Stabilized::new(
            ThresholdStrategy::default(),
            Duration::from_secs(0),
            Duration::from_secs(0),
        );
        let token = CancellationToken::new();
        token.cancel();

        let mut autoscaler = Autoscaler::new(backend, strategy, Duration::from_secs(60));
        tokio::time::timeout(Duration::from_secs(1), autoscaler.run(token))
            .await
            .expect("run() should return promptly after shutdown token is cancelled");
    }

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

    // --- ThresholdStrategy tests ---

    #[test]
    fn threshold_default_values() {
        let s = ThresholdStrategy::default();
        assert_eq!(s.scale_up_multiplier, 2.0);
        assert_eq!(s.scale_down_multiplier, 0.5);
    }

    #[test]
    fn threshold_scale_up_when_ready_exceeds_capacity_times_multiplier() {
        // cap = 2 consumers * 10 prefetch = 20, threshold = 40, ready = 50 → ScaleUp(1)
        let mut s = ThresholdStrategy::default();
        let m = ScalingMetrics::new(50, 0, 2, 10);
        assert_eq!(s.evaluate("group", &m), ScalingDecision::ScaleUp(1));
    }

    #[test]
    fn threshold_scale_down_when_ready_below_capacity_times_multiplier() {
        // cap = 2 consumers * 10 prefetch = 20, threshold = 10, ready = 5 → ScaleDown(1)
        let mut s = ThresholdStrategy::default();
        let m = ScalingMetrics::new(5, 0, 2, 10);
        assert_eq!(s.evaluate("group", &m), ScalingDecision::ScaleDown(1));
    }

    #[test]
    fn threshold_hold_when_within_thresholds() {
        // cap = 20, up threshold = 40, down threshold = 10, ready = 15 → Hold
        let mut s = ThresholdStrategy::default();
        let m = ScalingMetrics::new(15, 0, 2, 10);
        assert_eq!(s.evaluate("group", &m), ScalingDecision::Hold);
    }

    #[test]
    fn threshold_hold_at_exact_up_threshold() {
        // cap = 1 * 10 = 10, up threshold = 20, ready = 20 → Hold (requires >)
        let mut s = ThresholdStrategy::default();
        let m = ScalingMetrics::new(20, 0, 1, 10);
        assert_eq!(s.evaluate("group", &m), ScalingDecision::Hold);
    }

    #[test]
    fn threshold_hold_at_exact_down_threshold() {
        // cap = 2 * 10 = 20, down threshold = 10, ready = 10 → Hold (requires <)
        let mut s = ThresholdStrategy::default();
        let m = ScalingMetrics::new(10, 0, 2, 10);
        assert_eq!(s.evaluate("group", &m), ScalingDecision::Hold);
    }

    #[test]
    fn threshold_custom_multipliers() {
        // up=1.5, down=0.3, cap=10, up threshold=15, ready=16 → ScaleUp(1)
        let mut s = ThresholdStrategy {
            scale_up_multiplier: 1.5,
            scale_down_multiplier: 0.3,
        };
        let m = ScalingMetrics::new(16, 0, 1, 10);
        assert_eq!(s.evaluate("group", &m), ScalingDecision::ScaleUp(1));
    }

    #[test]
    fn threshold_zero_consumers_scale_up() {
        // cap = 0, up threshold = 0.0, ready = 100 → 100 > 0 → ScaleUp(1)
        let mut s = ThresholdStrategy::default();
        let m = ScalingMetrics::new(100, 0, 0, 10);
        assert_eq!(s.evaluate("group", &m), ScalingDecision::ScaleUp(1));
    }

    #[test]
    fn threshold_ignores_group_name() {
        let mut s = ThresholdStrategy::default();
        let m = ScalingMetrics::new(50, 0, 2, 10);
        let r1 = s.evaluate("group-a", &m);
        let r2 = s.evaluate("group-b", &m);
        assert_eq!(r1, r2);
    }

    // --- Stabilized tests ---

    struct FixedStrategy(ScalingDecision);
    impl ScalingStrategy for FixedStrategy {
        fn evaluate(&mut self, _group: &str, _metrics: &ScalingMetrics) -> ScalingDecision {
            self.0.clone()
        }
    }

    struct SequentialStrategy(Vec<ScalingDecision>);
    impl ScalingStrategy for SequentialStrategy {
        fn evaluate(&mut self, _group: &str, _metrics: &ScalingMetrics) -> ScalingDecision {
            self.0.remove(0)
        }
    }

    fn test_metrics() -> ScalingMetrics {
        ScalingMetrics::new(100, 0, 2, 10)
    }

    #[test]
    fn stabilized_passes_through_hold() {
        let mut s = Stabilized::new(
            FixedStrategy(ScalingDecision::Hold),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        assert_eq!(s.evaluate("g", &test_metrics()), ScalingDecision::Hold);
    }

    #[test]
    fn stabilized_blocks_during_hysteresis() {
        let mut s = Stabilized::new(
            FixedStrategy(ScalingDecision::ScaleUp(1)),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        // hysteresis not elapsed yet → Hold
        assert_eq!(s.evaluate("g", &test_metrics()), ScalingDecision::Hold);
        // timer should have been set
        assert!(s.state["g"].scale_up_since.is_some());
    }

    #[test]
    fn stabilized_fires_after_hysteresis() {
        let mut s = Stabilized::new(
            FixedStrategy(ScalingDecision::ScaleUp(1)),
            Duration::from_secs(0),
            Duration::from_secs(60),
        );
        // hysteresis = 0 → passes through immediately
        assert_eq!(
            s.evaluate("g", &test_metrics()),
            ScalingDecision::ScaleUp(1)
        );
    }

    #[test]
    fn stabilized_blocks_during_cooldown() {
        let mut s = Stabilized::new(
            FixedStrategy(ScalingDecision::ScaleUp(1)),
            Duration::from_secs(0),
            Duration::from_secs(60),
        );
        // First call fires (hysteresis=0)
        assert_eq!(
            s.evaluate("g", &test_metrics()),
            ScalingDecision::ScaleUp(1)
        );
        // Second call is in cooldown
        assert_eq!(s.evaluate("g", &test_metrics()), ScalingDecision::Hold);
    }

    #[test]
    fn stabilized_resets_hysteresis_on_hold() {
        // ScaleUp → Hold → ScaleUp: the second ScaleUp should start a fresh timer
        let mut s = Stabilized::new(
            SequentialStrategy(vec![
                ScalingDecision::ScaleUp(1),
                ScalingDecision::Hold,
                ScalingDecision::ScaleUp(1),
            ]),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        // First ScaleUp: starts timer, returns Hold (not elapsed)
        assert_eq!(s.evaluate("g", &test_metrics()), ScalingDecision::Hold);
        let first_timer = s.state["g"].scale_up_since;
        assert!(first_timer.is_some());

        // Hold: clears timer
        assert_eq!(s.evaluate("g", &test_metrics()), ScalingDecision::Hold);
        assert!(s.state["g"].scale_up_since.is_none());

        // ScaleUp again: fresh timer started, returns Hold (60s not elapsed)
        assert_eq!(s.evaluate("g", &test_metrics()), ScalingDecision::Hold);
        assert!(s.state["g"].scale_up_since.is_some());
    }

    #[test]
    fn stabilized_per_group_isolation() {
        let mut s = Stabilized::new(
            FixedStrategy(ScalingDecision::ScaleUp(1)),
            Duration::from_secs(0),
            Duration::from_secs(60),
        );
        // group "a" fires
        assert_eq!(
            s.evaluate("a", &test_metrics()),
            ScalingDecision::ScaleUp(1)
        );
        // group "a" is in cooldown
        assert_eq!(s.evaluate("a", &test_metrics()), ScalingDecision::Hold);
        // group "b" is independent and fires
        assert_eq!(
            s.evaluate("b", &test_metrics()),
            ScalingDecision::ScaleUp(1)
        );
    }

    #[test]
    fn stabilized_scale_down_hysteresis() {
        let mut s = Stabilized::new(
            FixedStrategy(ScalingDecision::ScaleDown(1)),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        // hysteresis not elapsed → Hold, but timer is set
        assert_eq!(s.evaluate("g", &test_metrics()), ScalingDecision::Hold);
        assert!(s.state["g"].scale_down_since.is_some());
    }

    #[test]
    fn stabilized_clears_opposite_timer_on_scale_up() {
        // ScaleDown starts scale_down_since, then ScaleUp clears it and sets scale_up_since
        let mut s = Stabilized::new(
            SequentialStrategy(vec![
                ScalingDecision::ScaleDown(1),
                ScalingDecision::ScaleUp(1),
            ]),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        // ScaleDown: starts scale_down_since, returns Hold
        assert_eq!(s.evaluate("g", &test_metrics()), ScalingDecision::Hold);
        assert!(s.state["g"].scale_down_since.is_some());
        assert!(s.state["g"].scale_up_since.is_none());

        // ScaleUp: clears scale_down_since, starts scale_up_since, returns Hold
        assert_eq!(s.evaluate("g", &test_metrics()), ScalingDecision::Hold);
        assert!(s.state["g"].scale_down_since.is_none());
        assert!(s.state["g"].scale_up_since.is_some());
    }
}
