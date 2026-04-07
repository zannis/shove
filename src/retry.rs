use std::time::Duration;

/// Default randomization factor (±50% jitter around the current interval).
const RANDOMIZATION_FACTOR: f64 = 0.5;

/// Exponential backoff iterator with full-jitter: each call to `next` returns a duration
/// randomly sampled from `[(1 - factor) * base, (1 + factor) * base]`, where `base` doubles
/// on every step (capped at `max`).
///
/// This mirrors the algorithm used by the `backoff` crate's `ExponentialBackoff`.
pub(crate) struct Backoff {
    /// The base interval that grows exponentially.
    current: Duration,
    /// The initial interval (used by `reset`).
    initial: Duration,
    /// The upper bound on the base interval.
    max: Duration,
    /// Randomization factor in `[0, 1)`. `0` means no jitter.
    randomization_factor: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(30))
    }
}

impl Backoff {
    /// Create a new `Backoff` with the given initial delay, maximum delay, and the default
    /// randomization factor of 0.5.
    pub(crate) fn new(initial: Duration, max: Duration) -> Self {
        Self {
            current: initial,
            initial,
            max,
            randomization_factor: RANDOMIZATION_FACTOR,
        }
    }

    /// Reset the iterator to the initial delay (call after a successful operation).
    #[allow(dead_code)]
    pub(crate) fn reset(&mut self) {
        self.current = self.initial;
    }

    /// Compute a jittered delay from the current base interval.
    ///
    /// Mirrors `ExponentialBackoff::get_random_value_from_interval` in the `backoff` crate:
    /// picks a random value in `[base - factor*base, base + factor*base]`.
    fn jittered(&self) -> Duration {
        if self.randomization_factor == 0.0 {
            return self.current;
        }
        let base_nanos = duration_to_nanos(self.current);
        let delta = self.randomization_factor * base_nanos;
        let min = base_nanos - delta;
        let max = base_nanos + delta;
        let random: f64 = rand::random();
        let nanos = min + random * (max - min + 1.0);
        nanos_to_duration(nanos)
    }
}

fn duration_to_nanos(d: Duration) -> f64 {
    d.as_secs() as f64 * 1_000_000_000.0 + f64::from(d.subsec_nanos())
}

fn nanos_to_duration(nanos: f64) -> Duration {
    let secs = (nanos / 1_000_000_000.0) as u64;
    let sub_nanos = (nanos as u64) % 1_000_000_000;
    Duration::new(secs, sub_nanos as u32)
}

impl Iterator for Backoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        let delay = self.jittered();
        self.current = (self.current * 2).min(self.max);
        Some(delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_sequence_within_bounds() {
        // With jitter the exact values vary, but each delay must be within ±50% of the base.
        let bases = [1u64, 2, 4, 8, 16, 30, 30];
        let delays: Vec<Duration> = Backoff::default().take(7).collect();
        for (delay, &base_secs) in delays.iter().zip(bases.iter()) {
            let base = Duration::from_secs(base_secs);
            let lo = base / 2; // base * 0.5
            let hi = base + base / 2; // base * 1.5
            assert!(
                *delay >= lo && *delay <= hi,
                "delay {delay:?} not in [{lo:?}, {hi:?}] for base {base:?}"
            );
        }
    }

    #[test]
    fn backoff_reset() {
        let mut b = Backoff::default();
        b.next(); // advances to 2s base
        b.next(); // advances to 4s base
        b.reset();
        // After reset the base is back to 1s, so jittered value must be in [0.5s, 1.5s].
        let delay = b.next().unwrap();
        assert!(
            delay >= Duration::from_millis(500) && delay <= Duration::from_millis(1500),
            "after reset, delay {delay:?} should be in [500ms, 1500ms]"
        );
    }

    #[test]
    fn backoff_custom_initial_and_max() {
        // initial=100ms, max=500ms → bases: 100, 200, 400, 500, 500
        let bases_ms = [100u64, 200, 400, 500, 500];
        let delays: Vec<Duration> =
            Backoff::new(Duration::from_millis(100), Duration::from_millis(500))
                .take(5)
                .collect();
        for (delay, &base_ms) in delays.iter().zip(bases_ms.iter()) {
            let base = Duration::from_millis(base_ms);
            let lo = base / 2;
            let hi = base + base / 2;
            assert!(
                *delay >= lo && *delay <= hi,
                "delay {delay:?} not in [{lo:?}, {hi:?}] for base {base:?}"
            );
        }
    }

    #[test]
    fn backoff_no_jitter() {
        let mut b = Backoff {
            current: Duration::from_secs(1),
            initial: Duration::from_secs(1),
            max: Duration::from_secs(30),
            randomization_factor: 0.0,
        };
        let delays: Vec<Duration> = (&mut b).take(7).collect();
        assert_eq!(
            delays,
            vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(16),
                Duration::from_secs(30),
                Duration::from_secs(30),
            ]
        );
    }
}
