# Plan 015: Respawn dead consumers to min_consumers with backoff and circuit-break

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before moving on. On any STOP condition, stop and
> report. Your reviewer maintains `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- src/backends/kafka/consumer_group.rs src/backends/nats/consumer_group.rs src/backends/kafka/autoscaler.rs src/backends/nats/autoscaler.rs src/autoscaler.rs`
> REQUIRED drift: plan 008's liveness accounting must already be present in
> both consumer_group files (`active_consumers` filters on `is_finished`).
> If it is not, STOP — this plan builds on it.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED (a supervision loop that respawns; the circuit-breaker is the
  guard against crash-loops)
- **Depends on**: **008 (hard dependency — verify DONE in plans/README.md)**
- **Category**: bug/ops
- **Planned at**: commit `e902d7c` (main), 2026-07-02
- **Maintainer decision**: respawn to `min_consumers` with exponential backoff
  between respawn rounds, and stop respawning (circuit-break) after N
  consecutive immediate deaths so a persistent failure (e.g. bad credentials)
  does not become an infinite crash-loop.
- **Amended 2026-08-08** (review finding, P1): the open circuit must **not** be
  terminal. The original step 1 reset the failure streak only on the
  "live >= min" path, which an open circuit can never reach — a group killed by
  a transient outage (broker down, credentials rotated and restored) would stay
  dead for the process lifetime. The circuit now has a cooldown and a
  **half-open single-member probe**; a probe that survives
  `RESPAWN_HEALTHY_AFTER` closes the circuit. See step 1.

## Why this matters

After plan 008, a group whose consumer tasks died reports truthful counts and
lag-driven autoscaling replaces capacity **when there is lag**. Two gaps
remain: an idle group sits below `min_consumers` indefinitely (the config's
contract says at least N members), and a group without traffic never heals at
all. Supervision policy chosen by the maintainer: top groups back up to
`min_consumers` on the autoscaler tick, with backoff and a circuit-breaker.

## Current state

- Post-008 `src/backends/kafka/consumer_group.rs` / `src/backends/nats/consumer_group.rs`:
  `active_consumers()` counts unfinished handles; `prune_finished()` exists
  and runs at the top of `scale_up`/`scale_down`; `spawn_one()` spawns a
  member (kafka: search `fn spawn_one`); `min_consumers`/`max_consumers` on
  the config; `error_count: Arc<AtomicUsize>` incremented in the spawner
  wrapper when a task exits with error (kafka consumer_group.rs ~:374-380,
  nats ~:183-186).
- The supervision tick site: each backend's `AutoscalerBackend::fetch_metrics`
  (`src/backends/kafka/autoscaler.rs:385-420`, and the NATS sibling in
  `src/backends/nats/autoscaler.rs` — same trait) runs on every poll interval
  for every group and locks the registry. The generic loop
  (`src/autoscaler.rs::poll_and_scale`) calls `fetch_metrics` then `scale`
  per decision.
- Design constraint: groups are driven externally; the autoscaler tick is the
  only recurring hook. Groups not covered by an autoscaler get NO respawn —
  documented limitation (see Maintenance notes).
- Conventions: Conventional Commits, no `Co-Authored-By`; clippy `-D warnings`
  both feature sets; `absolute-paths = "deny"`; `cargo nextest run` (never
  `-q`).

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Lint | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Lint (min) | `cargo clippy -q --no-default-features -- -D warnings` | exit 0 |
| Kafka unit | `cargo nextest run --features kafka --lib` | all pass |
| NATS unit | `cargo nextest run --features nats --lib` | all pass |
| Kafka integration (Docker) | `cargo nextest run --features kafka --test kafka_integration` | all pass |
| NATS integration (Docker) | `cargo nextest run --features nats --test nats_integration` | all pass |

## Scope

**In scope**:
- `src/backends/kafka/consumer_group.rs`, `src/backends/nats/consumer_group.rs`
  (the `ensure_min` supervision method + state)
- `src/backends/kafka/autoscaler.rs`, `src/backends/nats/autoscaler.rs`
  (invoke it on the tick)
- `src/metrics.rs` ONLY IF a respawn counter metric fits the existing
  pattern cheaply (optional; skip if it drags in churn)

**Out of scope**:
- `src/autoscaler.rs` (generic layer stays supervision-agnostic).
- Respawn for groups without an autoscaler (documented limitation).
- Restarting FIFO shard tasks (different lifecycle; separate finding if ever).
- Other backends.

## Steps

### Step 1: Supervision state + ensure_min (both consumer_group files)

Add to the group struct (same shape in both backends):

```rust
/// Supervision state for respawn-to-min: consecutive respawn rounds whose
/// members died again quickly, the earliest time the next respawn round is
/// allowed, when the last round ran, and the live count it reached (the
/// round "held" if we are still at or above that count later).
respawn_consecutive_failures: u32,
respawn_not_before: Option<tokio::time::Instant>,
last_respawn_at: Option<tokio::time::Instant>,
respawn_watermark: usize,
```

Constants (per file): `RESPAWN_BACKOFF_BASE: Duration = 1s`,
`RESPAWN_BACKOFF_MAX: Duration = 60s`, `RESPAWN_CIRCUIT_LIMIT: u32 = 5`,
`RESPAWN_HEALTHY_AFTER: Duration = 60s` (a respawn round still standing after
this long resets the failure streak), `RESPAWN_CIRCUIT_COOLDOWN: Duration =
5min` (how long an open circuit waits before it probes again).

**Invariant**: `RESPAWN_CIRCUIT_COOLDOWN > RESPAWN_HEALTHY_AFTER`, otherwise a
probe is re-gated before it can ever be judged healthy and the circuit is
terminal again. Assert this in a unit test (`const` comparison), not at
runtime.

Method:

```rust
/// Top the group back up to min_consumers, respecting backoff and the
/// circuit-breaker. Called from the autoscaler tick. Returns how many
/// members were spawned.
pub(crate) fn ensure_min(&mut self) -> usize
```

Logic, in this order — the health check runs **unconditionally, before any
gate**, which is what makes recovery possible from every state:

1. `prune_finished()`; `let live = self.active_consumers()`.
2. **Health check**: if `last_respawn_at` is `Some(at)` with
   `at.elapsed() >= RESPAWN_HEALTHY_AFTER` **and** `live >= respawn_watermark`
   → the last round held: set `respawn_consecutive_failures = 0`,
   `respawn_not_before = None`, `last_respawn_at = None`. Log
   `tracing::info!` if the circuit was open (it just closed).
3. If `live >= min` → return 0.
4. **Gate**: if `respawn_not_before` is `Some(t)` with `t` in the future →
   `tracing::debug!` and return 0. (One gate for both backoff and the circuit
   cooldown, so there is exactly one place that can block a spawn.)
5. **Decide the round size**:
   - circuit open (`respawn_consecutive_failures >= RESPAWN_CIRCUIT_LIMIT`) →
     **half-open probe**: spawn exactly 1 member, `tracing::warn!`
     "circuit half-open: probing with a single consumer". Do **not** increment
     the streak (it is already saturated at the limit).
   - otherwise → spawn `min.saturating_sub(live)` members via `spawn_one()`;
     `respawn_consecutive_failures = respawn_consecutive_failures
     .saturating_add(1)`; if it just reached `RESPAWN_CIRCUIT_LIMIT`, log
     `tracing::error!` **once, on that transition only** (never per tick).
6. Record the round: `last_respawn_at = Some(Instant::now())`,
   `respawn_watermark = live.saturating_add(spawned)`,
   `respawn_not_before = now.checked_add(delay)` (`Instant + Duration` panics
   on overflow; `checked_add` yielding `None` degrades to "no gate", which is
   the safe direction — it only ever allows a respawn sooner), where

   ```rust
   let delay = if circuit_open {
       RESPAWN_CIRCUIT_COOLDOWN
   } else {
       let exp = self.respawn_consecutive_failures.min(6);
       RESPAWN_BACKOFF_BASE
           .saturating_mul(2u32.saturating_pow(exp))
           .min(RESPAWN_BACKOFF_MAX)
   };
   ```

   No bare arithmetic anywhere on this path (`saturating_*` throughout, and
   the `exp` cap keeps the shift well inside `u32`).
7. Return `spawned`.

Why the watermark instead of "are we at min": a probe round targets 1 member,
not `min`, so "at/above min" can never certify it. `respawn_watermark` is the
live count the round actually reached, so the same rule judges both a full
top-up and a probe. It also correctly treats a member that dies *after* a long
healthy period as the start of a fresh streak rather than a continuation.

### Step 2: Tick wiring

In each backend's `fetch_metrics` (it already takes the registry lock and a
`&mut` path exists via `groups_mut()` — confirmed at
`src/backends/kafka/autoscaler.rs:456` and `src/backends/nats/autoscaler.rs:173`):
call `g.ensure_min()` before reading counts, and `tracing::info!` when it
spawned members. This makes supervision piggyback the existing poll cadence
with no new tasks.

Note the cadence coupling: the half-open probe cannot fire sooner than the
next poll interval after `RESPAWN_CIRCUIT_COOLDOWN` elapses. That is fine for
the default cadence, but it is why the cooldown is a flat constant rather than
another doubling step — recovery latency is already bounded below by the poll
interval.

### Step 3: Unit tests (both files)

Using the same test seams plan 008 established, all with
`#[tokio::test(start_paused = true)]` + `tokio::time::advance`:
- group at min with all members dead → `ensure_min` spawns to min;
- immediately-dying members → after `RESPAWN_CIRCUIT_LIMIT` rounds,
  `ensure_min` stops spawning (circuit open);
- backoff: second round is not allowed before `respawn_not_before`;
- healthy respawn (round stays alive past `RESPAWN_HEALTHY_AFTER`) resets
  the streak;
- **circuit half-opens**: with the circuit open, advancing past
  `RESPAWN_CIRCUIT_COOLDOWN` makes `ensure_min` return exactly 1 (the probe),
  and advancing by less than the cooldown returns 0;
- **circuit closes on a healthy probe**: probe survives
  `RESPAWN_HEALTHY_AFTER` → streak resets to 0 and the next tick tops the
  group all the way back to `min` (this is the regression test for the
  terminal-circuit bug — it must fail against the pre-amendment logic);
- **circuit stays shut against a persistent failure**: probes that die
  immediately do not escalate spawning — each cooldown yields at most one
  member, never a `min`-sized round.

**Verify**: kafka + nats `--lib` suites pass; integration suites pass
(regression: `autoscaling_scales_up_and_drains_clean` in both).

## Done criteria

- [ ] `cargo fmt -- --check`, both clippy commands exit 0
- [ ] Kafka + NATS unit suites pass incl. 7+ new supervision tests each
- [ ] Kafka + NATS integration suites pass
- [ ] Circuit-breaker proven by a test (spawning stops, error logged)
- [ ] Circuit **recovery** proven by a test: open circuit → cooldown → single
      probe → probe stays healthy → streak resets → group returns to `min`
- [ ] No path through `ensure_min` can leave the group permanently below
      `min` while the process is healthy (the open circuit is time-bounded,
      not terminal)
- [ ] `ensure_min` is invoked from both autoscaler `fetch_metrics` paths
      (grep evidence)
- [ ] No files outside scope modified

## STOP conditions

- Plan 008's pruning/liveness code is absent or differs from what step 1
  builds on.
- The NATS autoscaler backend's `fetch_metrics` has no mutable registry
  access path (report its actual shape).
- Unit-testing requires new public API on the groups.

## Maintenance notes

- Documented limitation: groups without an autoscaler get no respawn; if a
  standalone supervision task is ever wanted, `ensure_min` is the reusable
  core.
- Plan 014 (error taxonomy) composes here: once `Auth` errors exist, a future
  refinement can open the circuit immediately on non-retryable death causes
  instead of counting rounds.
- Reviewer scrutiny: the registry lock is held across `ensure_min` (spawning
  is synchronous `tokio::spawn` — cheap); the circuit-breaker log must not
  spam every tick (error on the open transition, warn per probe, debug for a
  gated tick); and the open circuit must be provably escapable — check that
  every `return 0` before a spawn is guarded by a deadline that time alone
  clears.
- The half-open probe deliberately spawns 1, not `min`: against a persistent
  failure (bad credentials) the steady-state cost is one dying task per
  `RESPAWN_CIRCUIT_COOLDOWN`, which is the price of automatic recovery from a
  transient one. Plan 014's `Auth` variant later lets us skip probing for
  causes known to be non-transient.
