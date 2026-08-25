# Plan 008: Truthful consumer-group liveness accounting (Kafka + NATS)

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving on. If a
> STOP condition occurs, stop and report. Your reviewer maintains
> `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat dacfe5c..HEAD -- src/backends/kafka/consumer_group.rs src/backends/nats/consumer_group.rs`
> Expected: no changes to these two files from plans 006/007. Any drift: STOP.

## Status

- **Priority**: P1
- **Effort**: S–M
- **Risk**: LOW-MED
- **Depends on**: 006 (same branch; plan 006 step 4 changed
  `fetch_metrics` in `src/backends/kafka/autoscaler.rs` — this plan does not
  touch that file, but its behavior composes with it)
- **Category**: bug
- **Planned at**: commit `dacfe5c` (main), 2026-07-02

## Why this matters

A consumer task that exits with a non-retryable error (or exhausts
`max_reconnect_attempts`) is only counted (`error_count += 1`) and logged; its
`JoinHandle` stays in the group's `consumers` vec forever. `active_consumers()`
returns `consumers.len()`, so a group whose members have all died still reports
full strength: the autoscaler computes capacity from a fiction and won't scale
up to compensate, `scale_up` may refuse at "max capacity" while zero consumers
run, and `scale_down` can "cancel" an already-dead consumer while live ones
keep working. Fix: count only live handles, and prune finished handles at the
mutation points. With truthful counts, the existing lag-driven autoscaler
self-heals groups under load (capacity shrinks → lag exceeds threshold →
scale-up spawns replacements). Respawn-to-min for *idle* groups is a
supervision-policy decision explicitly deferred (see Maintenance notes).

## Current state

Both files have the same structure (Kafka is the reference; NATS mirrors it).

- `src/backends/kafka/consumer_group.rs`:
  - Spawner (~lines 360-381): `tokio::spawn` wraps `consumer.run_with_inner`;
    on `Err` it does `ec.fetch_add(1, ..)` + `tracing::error!` — handle retained.
  - `consumers: Vec<(CancellationToken, Arc<AtomicBool>, JoinHandle<()>)>`
    (the AtomicBool is the "processing" flag used by scale_down).
  - `scale_up` (~493): gate `self.consumers.len() >= max_consumers`.
  - `scale_down` (~508): gate `self.consumers.len() <= min_consumers`; picks an
    idle member by `rposition(|(_, processing, _)| !processing.load(..))`;
    `swap_remove` → `token.cancel()` → pushes handle onto `self.retiring`.
  - `active_consumers` (~536): `self.consumers.len()`.
  - There is a `retiring: Vec<JoinHandle<()>>` and a test helper
    `retiring_is_empty()`; look for where `retiring` is drained (search
    `retiring` in the file) and keep that behavior intact.
- `src/backends/nats/consumer_group.rs`: same pattern — spawner error-count at
  ~183-186, `active_consumers` = len at ~334-336, scale_up/scale_down siblings
  (search `fn scale_up` / `fn scale_down`).
- `tokio::task::JoinHandle::is_finished(&self) -> bool` is the liveness probe
  (non-blocking, `&self`).
- The autoscaler reads counts via
  `src/backends/kafka/autoscaler.rs::fetch_metrics` → `g.active_consumers()`
  (immutable registry access) — which is why `active_consumers()` must be
  accurate with `&self`, not rely on prior pruning.
- Conventions: Conventional Commits, no `Co-Authored-By`; clippy `-D warnings`
  both feature sets; `absolute-paths = "deny"`; `cargo nextest run` only
  (never `-q` to nextest).

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
- `src/backends/kafka/consumer_group.rs`
- `src/backends/nats/consumer_group.rs`

**Out of scope**:
- `src/backends/kafka/autoscaler.rs`, `src/backends/nats/autoscaler.rs`
  (plan 006 touched the former; truthful counts flow through unchanged APIs).
- Respawn/supervision logic of any kind (deferred — see Maintenance notes).
- Other backends' consumer groups (same disease, separate decision).
- `error_count` / `panic_count` semantics.

## Git workflow

- Continue on branch `advisor/kafka-nats-prod-fixes`.
- Commit: `fix(consumer-group): count only live consumer tasks (kafka, nats)`.
  No `Co-Authored-By`.

## Steps

### Step 1: Kafka — live counting + pruning

In `src/backends/kafka/consumer_group.rs`:

1. `active_consumers(&self)` → count live handles:

   ```rust
   pub fn active_consumers(&self) -> usize {
       self.consumers.iter().filter(|(_, _, h)| !h.is_finished()).count()
   }
   ```

2. Add a private `fn prune_finished(&mut self)` that (a) `retain`s
   `self.consumers` where the handle is not finished, and (b) removes finished
   handles from `self.retiring`. Call it as the **first line** of both
   `scale_up` and `scale_down`, so their min/max gates and the idle-pick
   operate on live members only. Preserve any existing `retiring`-draining
   logic you find — if `retiring` is already swept elsewhere, keep that and
   only add the `consumers` retain (note what you found in NOTES).
3. Check every other read of `self.consumers.len()` in the file (search
   `consumers.len()`) and decide per site whether it means "live members"
   (switch to `active_consumers()`/prune) or "slots ever spawned" (leave;
   add a one-line comment). List the sites and your calls in NOTES.

### Step 2: NATS — mirror the change

Apply the identical treatment to `src/backends/nats/consumer_group.rs`
(its consumers vec may have a slightly different tuple shape — adapt the
pattern, not the letter).

### Step 3: Unit tests (both files)

In each file's `#[cfg(test)]` module, add tests that construct the group (use
whatever existing test constructors/tests already build one — search
`#[cfg(test)]` and `mod tests` in each file for the pattern; kafka's
autoscaler tests at `src/backends/kafka/autoscaler.rs:475+` also show how
groups are built with mock spawners if the group file itself lacks a seam):

- a group whose consumer task exits immediately reports `active_consumers() == 0`
  after the task completes (spawn, `tokio::task::yield_now().await` /
  small sleep until `is_finished()`, assert);
- `scale_up` succeeds when len == max but a member is dead (pruning frees the slot);
- `scale_down` at min+1 with one dead member prunes rather than cancelling a
  live one (assert the live member's token is not cancelled).

If constructing a group with a controllable spawner requires a new test-only
seam, prefer a `#[cfg(test)]` constructor over changing public API.

**Verify**: `cargo nextest run --features kafka --lib` and
`cargo nextest run --features nats --lib` → all pass.

## Test plan

Unit tests above are the core. The Docker integration suites
(`kafka_integration`, `nats_integration`) must stay green — they exercise
scale-up/down paths end-to-end (`autoscaling_scales_up_and_drains_clean` in
both files).

## Done criteria

- [ ] `cargo fmt -- --check` exits 0; both clippy commands exit 0
- [ ] `cargo nextest run --features kafka --lib` all pass (incl. new tests)
- [ ] `cargo nextest run --features nats --lib` all pass (incl. new tests)
- [ ] `cargo nextest run --features kafka --test kafka_integration` all pass
- [ ] `cargo nextest run --features nats --test nats_integration` all pass
- [ ] `grep -n "fn active_consumers" src/backends/kafka/consumer_group.rs src/backends/nats/consumer_group.rs`
      → both filter on `is_finished()`
- [ ] No files outside scope modified

## STOP conditions

- The consumers-vec tuple shape or scale methods differ materially from the
  excerpts (drift).
- Making the unit tests possible requires public API changes.
- Any existing integration test fails and the cause traces to pruning
  semantics rather than test flake (report the failing test and analysis;
  known flake: `redis_ping`/autoscaler tests can flake at container setup —
  a 0.2s connect failure is infra, not you).

## Maintenance notes

- **Deferred decision (surface in review)**: whether dead consumers should be
  *respawned* to honor `min_consumers` even when there is no lag (idle groups
  now sit below min until traffic arrives and lag-driven scale-up heals them),
  and whether repeated non-retryable exits (e.g. auth failure) should
  circuit-break instead of respawn. That is supervision policy — the
  maintainer is planning it interactively; this plan only makes reporting
  truthful.
- Reviewer scrutiny: no double-counting between `consumers` and `retiring`;
  `scale_down` must never cancel a live member while a dead one occupies a slot.
