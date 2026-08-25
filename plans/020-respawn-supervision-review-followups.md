# Plan 020: Respawn supervision — review follow-ups

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before moving on. On any STOP condition, stop and
> report. Your reviewer maintains `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat 82d1817..HEAD -- src/supervision.rs src/backends/kafka/consumer_group.rs src/backends/nats/consumer_group.rs src/backends/kafka/autoscaler.rs src/backends/nats/autoscaler.rs`
> REQUIRED baseline: `src/supervision.rs` exists with `RespawnSupervisor`
> (`plan` / `commit` / `check_health`), and both backends have
> `ensure_min()` called from `fetch_metrics`
> (`src/backends/kafka/autoscaler.rs:422`, `src/backends/nats/autoscaler.rs:146`).
> If any of that is absent, STOP — this plan is a follow-up to 015 as landed.

## Status

- **Priority**: P1 (step 1) / P2 (steps 2–3) / P3 (steps 4–5)
- **Effort**: S–M
- **Risk**: LOW-MED (step 1 changes when lag-driven scale-up is allowed to
  spawn; the rest is tests, logging, and a constant)
- **Depends on**: 015 (landed as `82d1817`)
- **Category**: bug/ops/test
- **Planned at**: commit `82d1817` (`feat/respawn-supervision`), 2026-08-08
- **Origin**: review of the respawn-supervision branch against `ffa2cd0`.
  Every finding below was re-verified against the working tree at `82d1817`.

## Why this matters

Plan 015 bought one guarantee: against a persistent failure (bad credentials,
a broker that refuses the group), the process spawns **at most one member per
`RESPAWN_CIRCUIT_COOLDOWN`**. Step 1 is the hole in that guarantee — the
autoscaler's ordinary scale-up path never consults the breaker, so the exact
scenario the breaker exists for (dead consumers, therefore growing backlog)
routes around it. Steps 2–5 are the difference between a circuit that is
*tested* to recover and one that is only *asserted* to.

## Current state (verified)

- `src/supervision.rs:82` `plan()` — health check, `live >= min` early return,
  `not_before` gate, half-open probe, else full top-up.
- `src/supervision.rs:114` — `consecutive_failures` is incremented **at plan
  time**, before the caller has spawned anything, and `:115` logs
  `error!("respawn circuit opened…")` on `== RESPAWN_CIRCUIT_LIMIT`.
- `src/supervision.rs:141` — `exponent = consecutive_failures.min(6)`, read
  *after* that increment, so the first round's delay is `1s * 2^1 = 2s`, while
  `:27` documents `RESPAWN_BACKOFF_BASE` as the "first backoff step" of 1s.
- `src/backends/kafka/consumer_group.rs:599` / `src/backends/nats/consumer_group.rs:397`
  `ensure_min()` — shutdown guard, prune, plan, spawn, commit, then one
  `info!` at `kafka:622` / `nats:420` reading
  `"respawned consumers to restore min capacity"` for probe and top-up alike.
- `src/backends/kafka/autoscaler.rs:464` / `src/backends/nats/autoscaler.rs:181`
  — `scale()`'s `ScalingDecision::ScaleUp(n)` arm loops `g.scale_up()` with no
  reference to the supervisor.
- `src/backends/kafka/consumer_group.rs:1633` / `src/backends/nats/consumer_group.rs:898`
  — `ensure_min_circuit_breaks_then_probes_and_recovers`, despite the name,
  uses `always_dies_spawner()` and stops at "the probe was created". Nothing
  proves a healthy probe closes the circuit at the backend level.
- Test seams available: `always_dies_spawner()`
  (`kafka:1568` / `nats:835`), `dies_n_times_spawner(n)`
  (`kafka:1574` / `nats:841`), `test_group_with_spawner`
  (`kafka:1042` / `nats:934`), `settle()`, `#[tokio::test(start_paused = true)]`.
- `scale_up()` is **public API** (`kafka/consumer_group.rs`,
  `nats/consumer_group.rs`, and the same shape on SNS/SQS), called directly by
  users and by many existing tests.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Format | `cargo fmt -- --check` | exit 0 |
| Lint | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Lint (min) | `cargo clippy -q --no-default-features -- -D warnings` | exit 0 |
| Supervision unit | `cargo nextest run --lib supervision` | all pass |
| Kafka unit | `cargo nextest run --features kafka --lib` | all pass |
| NATS unit | `cargo nextest run --features nats --lib` | all pass |
| Kafka integration (Docker) | `cargo nextest run --features kafka --test kafka_integration` | all pass |
| NATS integration (Docker) | `cargo nextest run --features nats --test nats_integration` | all pass |

## Scope

**In scope**: `src/supervision.rs`, `src/backends/kafka/consumer_group.rs`,
`src/backends/nats/consumer_group.rs`, `src/backends/kafka/autoscaler.rs`,
`src/backends/nats/autoscaler.rs`, and this file's amendment note in
`plans/015-consumer-respawn-supervision.md`.

**Out of scope**: `src/autoscaler.rs` (the generic layer stays
supervision-agnostic — the gate is enforced in each backend's `scale()`),
other backends, the public `scale_up()` signature and its semantics for direct
callers.

## Steps

### Step 1 (P1): the breaker must gate lag-driven scale-up

**Problem**: with a group whose members are dying and a backlog building, the
autoscaler decides `ScaleUp(n)` and `scale()` spawns `n` members every poll
interval (default cadence: 30s) straight through the five-minute cooldown. The
"at most one member per cooldown" guarantee holds only for `ensure_min`, which
is not where most spawns come from in exactly this scenario.

**Do not** gate inside `scale_up()` — it is public API with existing direct
callers and tests; changing it silently changes user-visible behavior.

1. In `src/supervision.rs`, add a non-mutating query:

   ```rust
   /// Whether the breaker currently forbids spawning. Lets other spawn paths
   /// (lag-driven scale-up) honor the same gate without disturbing the
   /// backoff state machine.
   pub(crate) fn is_gated(&self) -> bool
   ```

   True when `not_before` is `Some(t)` and `Instant::now() < t`. Read-only:
   it must not touch `consecutive_failures`, `not_before`, or `watermark`, and
   must not run the health check (`plan` owns those transitions — two writers
   on this state is how the recovery path gets subtle).

2. Expose it per backend as `pub(crate) fn respawn_gated(&self) -> bool` on
   `KafkaConsumerGroup` / `NatsConsumerGroup`, delegating to `self.respawn`.

3. In both `scale()` implementations
   (`src/backends/kafka/autoscaler.rs:464`, `src/backends/nats/autoscaler.rs:181`),
   before the `ScaleUp` loop:

   ```rust
   if g.respawn_gated() {
       warn!(group = %group, "scale-up suppressed: respawn backoff or circuit cooldown is open");
       return Ok(());
   }
   ```

   Keep it to the `ScaleUp` arm — `ScaleDown` and `Hold` must stay reachable
   (a gated group that is over-provisioned should still shrink).

4. Log discipline: the gate is re-evaluated every poll interval, so this
   `warn!` repeats for the life of the cooldown. Emit it at `debug!` instead
   if the reviewer objects to per-tick warns; the `error!` on the open
   transition (step 3) is the alerting signal, this one is diagnostics.

**Tests** (both backends, unit, `start_paused = true`): with the supervisor
gated after a failed round, `scale()` with `ScaleUp(2)` must add zero members
and leave `active_consumers()` unchanged; once the gate elapses, the same call
must scale up normally; `ScaleDown` must work while gated.

**Verify**: kafka + nats `--lib`; kafka + nats integration (regression:
`autoscaling_scales_up_and_drains_clean` in both — a healthy group has no gate
set, so it must be unaffected).

### Step 2 (P2): prove backend-level recovery, don't just name it

`ensure_min_circuit_breaks_then_probes_and_recovers`
(`kafka:1633`, `nats:898`) never recovers anything — `always_dies_spawner()`
guarantees the probe dies, and the test's last assertion is that a probe was
created.

Rename the existing test to
`ensure_min_circuit_breaks_and_probes_after_cooldown` (it is a good test of
that), and add a genuine recovery test in both backends:

- build the group with a spawner that dies for the first
  `RESPAWN_CIRCUIT_LIMIT * min` spawns and then stays alive — extend
  `dies_n_times_spawner(n)`, which already has this shape, rather than adding
  a new helper;
- burn the streak to open the circuit (as the existing test does);
- advance `RESPAWN_CIRCUIT_COOLDOWN`, call `ensure_min()` → 1 (probe), and
  assert `active_consumers() == 1` after `settle()` (the probe is genuinely
  alive — this is the assertion the current test cannot make);
- advance `RESPAWN_HEALTHY_AFTER + 1s`;
- call `ensure_min()` → `min - 1`, and assert `active_consumers() == min`.

That last assertion is the one that matters: it proves the circuit closed and
the **real group**, not just the supervisor's counters, came back to `min`.
`src/supervision.rs`'s `healthy_probe_closes_the_circuit_and_restores_min`
already covers the state machine in isolation; this covers the wiring.

**Verify**: kafka + nats `--lib`.

### Step 3 (P2): don't announce an open circuit before the round runs

`src/supervision.rs:114` increments the streak and `:115` fires
`error!("respawn circuit opened")` when it hits `RESPAWN_CIRCUIT_LIMIT` — but
that is the *fifth round being planned*, which has not spawned yet and may
well hold. Operators get a paged-worthy error for a round that recovers.

Move the announcement to the first tick that actually **acts** as an open
circuit. Implementation with the smallest blast radius: keep the increment
where it is (the backoff math and `circuit_open()` both depend on it), drop
the `error!` at `:115`, and emit it at the top of the `circuit_open()` branch
(`:101`) the first time that branch is taken — gated by a
`bool` field (e.g. `open_announced`) that `check_health` clears alongside the
rest of the reset. The existing per-probe `warn!` stays.

Net effect: `error!` fires once, when the breaker first refuses a full
top-up — which is a true statement — and never again until it closes and
re-opens.

**Tests**: extend `src/supervision.rs`'s unit tests to assert the streak
reaches the limit without the circuit having *acted* yet (the round that hits
the limit still returns a full-shortfall round, `probe == false`), and that a
healthy fifth round resets the streak with no open-circuit behavior ever
observed.

**Verify**: `cargo nextest run --lib supervision`.

### Step 4 (P3): probe-specific log wording

`kafka:622` / `nats:420` log `"respawned consumers to restore min capacity"`
for both round kinds. A one-member probe usually leaves the group below `min`,
so the message is false exactly when an operator is reading logs to find out
whether capacity came back.

Branch on `round.probe`:

- probe → `"respawn probe spawned: testing whether consumers can survive"`
- top-up → keep the existing message

Keep the same structured fields (`group`, `spawned`, `live_before`, `min`,
`probe`) so log queries do not break. Apply identically in both backends.

### Step 5 (P3): make the first backoff step match its documentation

`src/supervision.rs:27` documents `RESPAWN_BACKOFF_BASE` as the "first backoff
step after a respawn round whose members died", but `:141` computes
`exponent = consecutive_failures.min(MAX_BACKOFF_EXPONENT)` *after* the
increment at `:114`, so the first delay is 2s.

Pick one and make the code say it:

- **Preferred**: `let exponent = self.consecutive_failures.saturating_sub(1).min(MAX_BACKOFF_EXPONENT);`
  so the sequence is 1s, 2s, 4s, 8s, 16s, capped at 60s — matching the doc.
- Alternative: reword `:27` to "base for the exponential backoff; the first
  step is `2 × base`". Weaker — the constant's name then lies about its role.

Take the preferred fix and adjust `backoff_grows_and_is_capped` (`:260`) to
assert the first gate is exactly `RESPAWN_BACKOFF_BASE`, which is a real
regression test rather than the current monotonicity-only check.

**Verify**: `cargo nextest run --lib supervision`.

### Step 6: reconcile plan 015

`src/supervision.rs` — a shared state machine rather than per-backend
duplication — is outside the "In scope" list of plan 015, which named only the
two `consumer_group.rs` files. The shared module is the better call given this
repo's history of cross-backend drift (see CLAUDE.md, "Cross-backend
consistency"). Do **not** revert it. Append an amendment note to
`plans/015-consumer-respawn-supervision.md` recording that the supervision
state machine was extracted to `src/supervision.rs` and why, so the plan and
the tree agree. Plans are not committed (global git rule) — this is for the
next reader of the plan directory.

## Done criteria

- [ ] `cargo fmt -- --check` and both clippy commands exit 0
- [ ] Lag-driven `ScaleUp` is suppressed while the respawn gate is open, in
      **both** backends, proven by unit tests; `ScaleDown` still works gated
- [ ] No spawn path in the Kafka or NATS autoscaler can exceed one member per
      `RESPAWN_CIRCUIT_COOLDOWN` while the circuit is open (grep every
      `spawn_one()` / `scale_up()` caller and state the evidence)
- [ ] A backend-level test proves a healthy probe closes the circuit **and**
      `active_consumers()` returns to `min`, in both backends
- [ ] `error!` for an opened circuit fires only when the breaker actually
      refuses a top-up, once per open episode
- [ ] Probe rounds log probe-specific wording
- [ ] First backoff step equals `RESPAWN_BACKOFF_BASE`, asserted by a test
- [ ] Kafka + NATS unit and integration suites pass
- [ ] Plan 015 carries the `src/supervision.rs` amendment note
- [ ] No files outside scope modified

## STOP conditions

- `scale()`'s `ScaleUp` arm turns out to have callers that depend on spawning
  during a respawn cooldown (report them rather than working around it).
- Gating `ScaleUp` breaks `autoscaling_scales_up_and_drains_clean` in either
  backend — that would mean healthy groups carry a gate, which is a bug in
  step 1's condition, not a reason to weaken the test.
- Step 3's re-siting of the `error!` cannot be done without a second mutable
  entry point into the supervisor state.

## Maintenance notes

- Steps 1 and 3 are the two places where "the counter says X" and "the system
  did X" had drifted apart. Any future change to `plan`/`commit` should keep
  the rule that state transitions happen in exactly one writer.
- Once plan 014's error taxonomy lands, `Auth`-class deaths should open the
  circuit immediately and skip probing altogether; step 1's `is_gated()` is
  the hook that also makes lag-driven scale-up respect that.
- The gate deliberately suppresses scale-up rather than deferring it: a group
  that cannot keep its members alive will not drain a backlog by being handed
  more members.
