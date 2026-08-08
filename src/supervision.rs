//! Respawn supervision for coordinated consumer groups.
//!
//! A consumer task that exits (non-retryable error, exhausted reconnect
//! budget) leaves the group below its configured `min_consumers`. Lag-driven
//! autoscaling only replaces that capacity when there is lag, so an idle group
//! would sit under-provisioned indefinitely. [`RespawnSupervisor`] tops the
//! group back up on the autoscaler tick, with exponential backoff between
//! rounds and a circuit-breaker so a persistent failure (bad credentials) does
//! not become a crash-loop.
//!
//! The circuit is deliberately **not** terminal: after
//! [`RESPAWN_CIRCUIT_COOLDOWN`] it half-opens and spawns a single probe, and a
//! probe still alive after [`RESPAWN_HEALTHY_AFTER`] closes it again. Without
//! that, a group killed by a transient outage would stay dead for the lifetime
//! of the process.
//!
//! The state machine is shared rather than duplicated per backend: it carries
//! no backend-specific behavior, and this crate has a history of delivery
//! semantics drifting between backends when the same logic is written twice.

use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, error, info, warn};

/// First backoff step after a respawn round whose members died.
pub(crate) const RESPAWN_BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Ceiling for the exponential backoff between respawn rounds.
pub(crate) const RESPAWN_BACKOFF_MAX: Duration = Duration::from_secs(60);
/// Consecutive failed respawn rounds that open the circuit.
pub(crate) const RESPAWN_CIRCUIT_LIMIT: u32 = 5;
/// How long a respawn round must stand before it counts as healthy and
/// resets the failure streak.
pub(crate) const RESPAWN_HEALTHY_AFTER: Duration = Duration::from_secs(60);
/// How long an open circuit waits between half-open probes. Must exceed
/// [`RESPAWN_HEALTHY_AFTER`], otherwise a probe is re-gated before it can ever
/// be judged healthy and the circuit becomes terminal.
pub(crate) const RESPAWN_CIRCUIT_COOLDOWN: Duration = Duration::from_secs(300);

/// Largest exponent applied to [`RESPAWN_BACKOFF_BASE`]. `2^6 = 64` already
/// exceeds [`RESPAWN_BACKOFF_MAX`] at a 1s base; capping keeps the shift far
/// inside `u32` regardless of how the constants are retuned.
const MAX_BACKOFF_EXPONENT: u32 = 6;

/// A respawn round the caller should carry out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RespawnRound {
    /// How many members to spawn.
    pub(crate) count: usize,
    /// Whether this is a half-open circuit probe rather than a top-up.
    pub(crate) probe: bool,
}

/// Backoff + circuit-breaker state for topping a group back up to
/// `min_consumers`. Drive it once per supervision tick: [`plan`] decides the
/// round, then [`commit`] records what was actually spawned.
///
/// [`plan`]: RespawnSupervisor::plan
/// [`commit`]: RespawnSupervisor::commit
#[derive(Debug, Default)]
pub(crate) struct RespawnSupervisor {
    /// Consecutive respawn rounds that did not hold. Saturates at
    /// [`RESPAWN_CIRCUIT_LIMIT`]; the circuit is open at or above it.
    consecutive_failures: u32,
    /// Earliest instant the next round may run (backoff or circuit cooldown).
    not_before: Option<Instant>,
    /// When the last round ran, used to age it against
    /// [`RESPAWN_HEALTHY_AFTER`].
    last_respawn_at: Option<Instant>,
    /// Live count the last round reached. Still being at or above it later is
    /// what certifies the round as healthy — a probe targets 1 member, not
    /// `min`, so "are we at min" cannot certify one.
    watermark: usize,
}

impl RespawnSupervisor {
    /// Decide this tick's round, or `None` to spawn nothing.
    ///
    /// The health check runs before every gate, which is what lets an open
    /// circuit recover: its reset must be reachable from states that are, by
    /// definition, below `min`.
    pub(crate) fn plan(&mut self, live: usize, min: usize, queue: &str) -> Option<RespawnRound> {
        self.check_health(live, queue);

        if live >= min {
            return None;
        }

        if let Some(gate) = self.not_before
            && Instant::now() < gate
        {
            debug!(
                group = %queue,
                live,
                min,
                "respawn gated: backoff or circuit cooldown still open"
            );
            return None;
        }

        if self.circuit_open() {
            warn!(
                group = %queue,
                live,
                min,
                "respawn circuit half-open: probing with a single consumer"
            );
            return Some(RespawnRound {
                count: 1,
                probe: true,
            });
        }

        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures == RESPAWN_CIRCUIT_LIMIT {
            error!(
                group = %queue,
                consecutive_failures = self.consecutive_failures,
                cooldown_secs = RESPAWN_CIRCUIT_COOLDOWN.as_secs(),
                "respawn circuit opened: consecutive rounds died immediately; \
                 backing off to a single probe per cooldown"
            );
        }

        Some(RespawnRound {
            count: min.saturating_sub(live),
            probe: false,
        })
    }

    /// Record the round returned by [`plan`](RespawnSupervisor::plan).
    /// `live_before` is the live count `plan` was given.
    pub(crate) fn commit(&mut self, live_before: usize, round: RespawnRound) {
        let now = Instant::now();
        // Keyed on circuit state, not on the round kind: the round that *opens*
        // the circuit must also rest for the cooldown, otherwise the breaker
        // trips and immediately probes one backoff step later.
        let delay = if self.circuit_open() {
            RESPAWN_CIRCUIT_COOLDOWN
        } else {
            let exponent = self.consecutive_failures.min(MAX_BACKOFF_EXPONENT);
            RESPAWN_BACKOFF_BASE
                .saturating_mul(2u32.saturating_pow(exponent))
                .min(RESPAWN_BACKOFF_MAX)
        };

        self.last_respawn_at = Some(now);
        self.watermark = live_before.saturating_add(round.count);
        // `None` on overflow degrades to "no gate", which only ever permits an
        // earlier respawn — the safe direction.
        self.not_before = now.checked_add(delay);
    }

    /// Whether the breaker is open (spawning is limited to probes).
    fn circuit_open(&self) -> bool {
        self.consecutive_failures >= RESPAWN_CIRCUIT_LIMIT
    }

    /// Reset the failure streak if the last round is still standing after
    /// [`RESPAWN_HEALTHY_AFTER`].
    fn check_health(&mut self, live: usize, queue: &str) {
        let Some(at) = self.last_respawn_at else {
            return;
        };
        if at.elapsed() < RESPAWN_HEALTHY_AFTER || live < self.watermark {
            return;
        }

        if self.circuit_open() {
            info!(
                group = %queue,
                live,
                "respawn circuit closed: probe stayed healthy"
            );
        }
        self.consecutive_failures = 0;
        self.not_before = None;
        self.last_respawn_at = None;
        self.watermark = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUEUE: &str = "test-queue";

    /// Drive one tick that spawns nothing but keeps the group dead.
    fn tick_dead(sup: &mut RespawnSupervisor, min: usize) -> usize {
        match sup.plan(0, min, QUEUE) {
            Some(round) => {
                sup.commit(0, round);
                round.count
            }
            None => 0,
        }
    }

    /// Open the circuit by burning through the failure limit, then wait out
    /// the cooldown so the caller sits at the half-open boundary. Returns the
    /// total members "spawned" into the void.
    async fn open_circuit(sup: &mut RespawnSupervisor, min: usize) -> usize {
        let mut spawned = 0;
        for _ in 0..RESPAWN_CIRCUIT_LIMIT {
            spawned += tick_dead(sup, min);
            tokio::time::advance(RESPAWN_BACKOFF_MAX).await;
        }
        tokio::time::advance(RESPAWN_CIRCUIT_COOLDOWN).await;
        spawned
    }

    #[test]
    fn cooldown_exceeds_healthy_window() {
        assert!(
            RESPAWN_CIRCUIT_COOLDOWN > RESPAWN_HEALTHY_AFTER,
            "a probe must be judgeable as healthy before the next cooldown gate, \
             otherwise the circuit is terminal"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tops_up_to_min_when_all_members_are_dead() {
        let mut sup = RespawnSupervisor::default();
        let round = sup.plan(0, 3, QUEUE).expect("must plan a round");
        assert_eq!(round.count, 3);
        assert!(!round.probe);
    }

    #[tokio::test(start_paused = true)]
    async fn spawns_only_the_shortfall() {
        let mut sup = RespawnSupervisor::default();
        let round = sup.plan(1, 3, QUEUE).expect("must plan a round");
        assert_eq!(round.count, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn plans_nothing_at_or_above_min() {
        let mut sup = RespawnSupervisor::default();
        assert_eq!(sup.plan(3, 3, QUEUE), None);
        assert_eq!(sup.plan(4, 3, QUEUE), None);
    }

    #[tokio::test(start_paused = true)]
    async fn second_round_is_gated_by_backoff() {
        let mut sup = RespawnSupervisor::default();
        let round = sup.plan(0, 2, QUEUE).expect("first round");
        sup.commit(0, round);

        assert_eq!(sup.plan(0, 2, QUEUE), None, "backoff must gate the retry");

        tokio::time::advance(RESPAWN_BACKOFF_BASE * 2).await;
        assert!(
            sup.plan(0, 2, QUEUE).is_some(),
            "round must be allowed once backoff elapses"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_grows_and_is_capped() {
        let mut sup = RespawnSupervisor::default();
        let mut previous = Duration::ZERO;
        // Stop before the round that opens the circuit: that one switches to
        // the cooldown, which is deliberately larger than the backoff cap.
        for _ in 0..RESPAWN_CIRCUIT_LIMIT.saturating_sub(1) {
            let before = Instant::now();
            let round = sup.plan(0, 2, QUEUE).expect("round");
            sup.commit(0, round);
            let delay = sup.not_before.expect("gate must be set") - before;
            assert!(delay >= previous, "backoff must not shrink");
            assert!(
                delay <= RESPAWN_BACKOFF_MAX,
                "backoff {delay:?} exceeded the cap"
            );
            previous = delay;
            tokio::time::advance(RESPAWN_BACKOFF_MAX).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn circuit_opens_after_the_limit() {
        let mut sup = RespawnSupervisor::default();
        open_circuit(&mut sup, 3).await;

        let round = sup.plan(0, 3, QUEUE).expect("half-open probe");
        assert_eq!(round.count, 1, "an open circuit must spawn only a probe");
        assert!(round.probe);
    }

    #[tokio::test(start_paused = true)]
    async fn opening_round_rests_for_the_cooldown() {
        let mut sup = RespawnSupervisor::default();
        for _ in 0..RESPAWN_CIRCUIT_LIMIT {
            tick_dead(&mut sup, 3);
            tokio::time::advance(RESPAWN_BACKOFF_MAX).await;
        }
        assert_eq!(
            sup.plan(0, 3, QUEUE),
            None,
            "the round that opens the circuit must rest for the cooldown, \
             not merely one backoff step"
        );

        tokio::time::advance(RESPAWN_CIRCUIT_COOLDOWN).await;
        assert!(
            sup.plan(0, 3, QUEUE).is_some_and(|r| r.probe),
            "probe must follow once the cooldown elapses"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn open_circuit_gates_probes_by_cooldown() {
        let mut sup = RespawnSupervisor::default();
        open_circuit(&mut sup, 3).await;

        let probe = sup.plan(0, 3, QUEUE).expect("first probe");
        sup.commit(0, probe);

        tokio::time::advance(RESPAWN_CIRCUIT_COOLDOWN - Duration::from_secs(1)).await;
        assert_eq!(
            sup.plan(0, 3, QUEUE),
            None,
            "probe must wait out the cooldown"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        let next = sup.plan(0, 3, QUEUE).expect("probe after cooldown");
        assert_eq!(next.count, 1);
        assert!(next.probe);
    }

    /// Regression test for the terminal-circuit bug: recovery was reachable
    /// only from `live >= min`, which an open circuit can never reach.
    #[tokio::test(start_paused = true)]
    async fn healthy_probe_closes_the_circuit_and_restores_min() {
        let mut sup = RespawnSupervisor::default();
        open_circuit(&mut sup, 3).await;

        let probe = sup.plan(0, 3, QUEUE).expect("probe");
        assert!(probe.probe);
        sup.commit(0, probe);

        // The probe survives: one live member, aged past the healthy window.
        tokio::time::advance(RESPAWN_HEALTHY_AFTER + Duration::from_secs(1)).await;

        let round = sup
            .plan(1, 3, QUEUE)
            .expect("a closed circuit must top back up to min");
        assert_eq!(round.count, 2, "must restore the full shortfall, not probe");
        assert!(!round.probe, "circuit must be closed after a healthy probe");
    }

    #[tokio::test(start_paused = true)]
    async fn dying_probes_never_escalate_beyond_one_member() {
        let mut sup = RespawnSupervisor::default();
        open_circuit(&mut sup, 5).await;

        for _ in 0..10 {
            if let Some(round) = sup.plan(0, 5, QUEUE) {
                assert_eq!(round.count, 1, "a persistent failure must only ever probe");
                assert!(round.probe);
                sup.commit(0, round);
            }
            tokio::time::advance(RESPAWN_CIRCUIT_COOLDOWN).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn healthy_round_resets_the_streak() {
        let mut sup = RespawnSupervisor::default();
        let round = sup.plan(0, 2, QUEUE).expect("round");
        sup.commit(0, round);
        assert_eq!(sup.consecutive_failures, 1);

        tokio::time::advance(RESPAWN_HEALTHY_AFTER + Duration::from_secs(1)).await;
        assert_eq!(sup.plan(2, 2, QUEUE), None, "at min, nothing to do");
        assert_eq!(
            sup.consecutive_failures, 0,
            "healthy round resets the streak"
        );
        assert_eq!(sup.not_before, None, "healthy round clears the gate");
    }

    #[tokio::test(start_paused = true)]
    async fn short_lived_round_does_not_reset_the_streak() {
        let mut sup = RespawnSupervisor::default();
        let round = sup.plan(0, 2, QUEUE).expect("round");
        sup.commit(0, round);

        tokio::time::advance(RESPAWN_HEALTHY_AFTER + Duration::from_secs(1)).await;
        // Still below the watermark: the round did not hold.
        let next = sup.plan(0, 2, QUEUE).expect("round");
        sup.commit(0, next);
        assert_eq!(sup.consecutive_failures, 2);
    }
}
