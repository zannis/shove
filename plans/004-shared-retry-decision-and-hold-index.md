# Plan 004: Extract a shared, unit-tested retry decision + hold-queue index helper

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan in
> `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat 106bb05..HEAD -- src/backends/kafka/consumer.rs src/backends/nats/consumer.rs src/backends/rabbitmq/router.rs src/backends/inmemory/consumer.rs src/backends/redis/consumer.rs src/lib.rs`
> If any of these changed, compare the "Current state" excerpts against the live
> code before proceeding; on a mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: none (but 005 depends on this)
- **Category**: tech-debt / tests
- **Planned at**: commit `106bb05`, 2026-06-20

## Why this matters

The retry/DLQ/defer decision — "if `retry_count >= max_retries` go to the DLQ,
else hold-and-republish; defer never increments the count; pick the hold tier
`min(retry_count, len-1)`" — is reimplemented inline in every backend's
`route_outcome`. The git history shows recurring "cross-backend consistency"
fix commits because a fix to this decision in one backend has to be hand-copied
to the others. The exact boundary (`retry_count >= max_retries` vs `>`) is the
kind of off-by-one that has been fixed before and is currently pinned only by
heavy, Docker-requiring integration tests — there is no fast, Docker-free unit
test of the decision table.

This plan extracts the **unambiguous, backend-agnostic** part of that decision
into one place, unit-tests it exhaustively (Docker-free), and wires it into the
in-memory backend as the reference consumer. It deliberately does NOT touch the
five remaining backends or the backend-specific *execution* (ack/publish/DLQ
mechanics, and the divergent empty-hold-queue fallback) — that is plan 005,
which depends on this one. Landing this first gives the safety net that makes
005 safe.

## Current state

### The duplicated decision (read these to confirm the shared logic)

- **Kafka** `src/backends/kafka/consumer.rs:374-466` (`route_outcome`): `Ack` →
  commit; `Retry` → `if retry_count >= max_retries { DLQ("max_retries_exceeded") }
  else { delay = empty? 1s : hold_queues[min(retry_count, len-1)].delay();
  republish with retry_count+1 }`; `Reject` → DLQ("rejected"); `Defer` →
  `delay = empty? 1s : hold_queues[0].delay()`, republish WITHOUT incrementing.
- **NATS** `src/backends/nats/consumer.rs:299-357`: same decision, different
  execution (`msg.ack()` / `hold_then_republish`). Empty-hold fallback = 1s.
- **RabbitMQ** `src/backends/rabbitmq/router.rs:26-37` (`route_retry`): hold-tier
  index `(retry_count as usize).min(hold_queues.len() - 1)` at line 36; when
  `hold_queues.is_empty()` it does NOT republish — it nack-requeues (different
  empty-case behavior).
- **In-memory** `src/backends/inmemory/consumer.rs:700-724` (`route_outcome`):
  ```rust
  match outcome {
      Outcome::Ack => {}
      Outcome::Retry => {
          let retry_count = get_retry_count(&env.headers);
          if retry_count >= options.max_retries {
              route_reject(broker, topology, env).await;        // its DLQ path
          } else {
              schedule_redelivery(broker, topology, env, true); // increment=true
          }
      }
      Outcome::Defer => { schedule_redelivery(broker, topology, env, false); }
      Outcome::Reject => { route_reject(broker, topology, env).await; }
  }
  ```
  Its `schedule_redelivery` (lines 726-746) computes the hold delay with the same
  `min(retry_count, len-1)` index at line 743, and uses `Duration::ZERO` (not 1s)
  when hold queues are empty.

### The hold-tier index duplication (finding to dedupe — pure, behavior-preserving)

The identical expression `(retry_count as usize).min(hold_queues.len() - 1)`
appears at:
- `src/backends/kafka/consumer.rs:416`
- `src/backends/nats/consumer.rs:326`
- `src/backends/rabbitmq/router.rs:36`
- `src/backends/inmemory/consumer.rs:743`

And Redis already has a helper that does exactly this (handling the empty case):
`src/backends/redis/consumer.rs:1506-1512`:
```rust
pub(super) fn hold_level<T>(retry_count: u32, hold_queues: &[T]) -> Option<usize> {
    if hold_queues.is_empty() { None }
    else { Some((retry_count as usize).min(hold_queues.len() - 1)) }
}
```
with tests at `src/backends/redis/consumer.rs:1522-1534`.

### Module/feature facts

- `Outcome` (Ack/Retry/Reject/Defer) is defined in `src/outcome.rs` and is
  always compiled.
- `src/retry.rs` is `pub(crate) mod retry` gated to `any(rabbitmq, nats, kafka,
  pub-aws-sns, aws-sns-sqs, redis-streams)` (`src/lib.rs:174-182`) — note it does
  NOT include `inmemory`, so the in-memory backend cannot use helpers placed
  there. The new module in this plan must be compiled for `inmemory` too.
- CI denies warnings on both `--all-features` and `--no-default-features`
  (`ci.yml:48-49`), so the new module must not be dead code under
  `--no-default-features` (no backend features) — gate it on the union of
  backend features that use it.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build the new module + inmemory | `cargo build --features inmemory` | exit 0 |
| Unit-test new helper + inmemory | `cargo nextest run --features inmemory` | all pass, incl. new tests |
| Docker-free integration tests | `cargo nextest run --features inmemory,metrics --test inmemory_integration` | all pass (no behavior change) |
| Build other backends (compile check only) | `cargo build --features kafka,nats,rabbitmq,redis-streams` | exit 0 |
| Lint, all features | `cargo clippy --all-features -- -D warnings` | exit 0 |
| Lint, no features (dead-code gate) | `cargo clippy --no-default-features -- -D warnings` | exit 0 |
| Format | `cargo fmt -- --check` | exit 0 |

## Scope

**In scope** (create/modify only):
- `src/routing.rs` (new) — the shared helper + its unit tests.
- `src/lib.rs` — register the new module (one gated `mod routing;` line).
- `src/backends/kafka/consumer.rs` — replace the inline index at line 416 with
  the shared helper. (Decision branching UNCHANGED.)
- `src/backends/nats/consumer.rs` — same, line 326.
- `src/backends/rabbitmq/router.rs` — same, line 36.
- `src/backends/inmemory/consumer.rs` — replace inline index at line 743 AND
  wire `route_outcome` (700-724) to call the shared `decide_retry`.
- `src/backends/redis/consumer.rs` — rewrite `hold_level` (1506-1512) to delegate
  to the shared index helper; keep its signature and tests.

**Out of scope** (do NOT touch):
- The backend-specific *execution* in Kafka/NATS/RabbitMQ/Redis `route_outcome`
  (the ack/commit/publish/DLQ mechanics) — only Kafka/NATS/RabbitMQ/Redis index
  lookups change here; their decision branching stays as-is. Migrating their
  decision branching to `decide_retry` is **plan 005**.
- The empty-hold-queue fallback values (1s / ZERO / nack-requeue). They diverge
  by backend on purpose-or-by-accident; reconciling them is a decision for plan
  005. Do not change any of them.
- The public API surface — `routing` is a private `pub(crate)` module.
- SQS/SNS (`src/backends/sns/`) — it has no hold-queue retry path of this shape.

## Git workflow

- Branch: `advisor/004-shared-retry-decision`
- Commit per logical unit (helper+tests; then per-backend index swaps; then
  inmemory wiring). Conventional Commits, e.g.
  `refactor(retry): extract shared hold-queue index and retry decision helper`.
- Do NOT push or open a PR unless instructed. No `Co-Authored-By` trailer.

## Steps

### Step 1: Create `src/routing.rs` with the helper and exhaustive unit tests

Create `src/routing.rs`. Gate every item so it is only compiled when a backend
that uses it is enabled (prevents dead-code warnings under
`--no-default-features`). Use this module-level attribute on the `mod routing;`
declaration in lib.rs (Step 2) rather than per-item gates.

Contents:

```rust
//! Backend-agnostic retry/DLQ routing decisions, shared across consumer
//! backends so the boundary logic lives (and is tested) in exactly one place.

use crate::Outcome;

/// Hold-queue tier for a given retry count, clamped to the last tier.
/// Caller guarantees `hold_queue_count > 0`.
pub(crate) fn hold_index(retry_count: u32, hold_queue_count: usize) -> usize {
    debug_assert!(hold_queue_count > 0, "hold_index called with no hold queues");
    (retry_count as usize).min(hold_queue_count - 1)
}

/// The backend-agnostic decision for what to do with a message after the
/// handler returns `outcome`. Execution (ack/commit/publish/DLQ) and the
/// empty-hold-queue fallback are intentionally left to each backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    /// Handler succeeded — ack/commit the message.
    Ack,
    /// Terminal failure — route to the DLQ with this death reason, then
    /// ack/commit. `reason` is one of "rejected" or "max_retries_exceeded".
    Dlq { reason: &'static str },
    /// Hold-and-redeliver. `increment` is true for `Retry` (consumes retry
    /// budget) and false for `Defer` (does not).
    Hold { increment: bool },
}

/// Decide the routing for `outcome`. The retry-budget boundary lives here:
/// `max_retries = N` permits 1 initial attempt + N retries, so the message
/// goes to the DLQ once `retry_count >= max_retries`.
pub(crate) fn decide_retry(
    outcome: &Outcome,
    retry_count: u32,
    max_retries: u32,
) -> RetryDecision {
    match outcome {
        Outcome::Ack => RetryDecision::Ack,
        Outcome::Reject => RetryDecision::Dlq { reason: "rejected" },
        Outcome::Retry => {
            if retry_count >= max_retries {
                RetryDecision::Dlq { reason: "max_retries_exceeded" }
            } else {
                RetryDecision::Hold { increment: true }
            }
        }
        Outcome::Defer => RetryDecision::Hold { increment: false },
    }
}
```

Then add a `#[cfg(test)] mod tests` covering, at minimum:
- `hold_index`: `(0, 2)=0`, `(1, 2)=1`, `(5, 2)=1` (clamped), `(0, 1)=0`.
- `decide_retry`:
  - `Ack` → `Ack`.
  - `Reject` → `Dlq { reason: "rejected" }` regardless of counts.
  - `Retry` with `retry_count < max_retries` → `Hold { increment: true }`.
  - `Retry` with `retry_count == max_retries` → `Dlq { reason:
    "max_retries_exceeded" }` (the boundary — assert this exact case).
  - `Retry` with `retry_count == max_retries - 1` → `Hold { increment: true }`
    (the last allowed retry).
  - `Retry` with `max_retries == 0` and `retry_count == 0` → `Dlq` (no retries
    allowed).
  - `Defer` → `Hold { increment: false }` for any counts (never DLQs).

**Verify**: `cargo nextest run --features inmemory routing` → the new tests pass.

### Step 2: Register the module in `src/lib.rs`

Add, near the other `mod` declarations (e.g. by the gated `pub(crate) mod retry;`
at `src/lib.rs:173-182`):

```rust
#[cfg(any(
    feature = "inmemory",
    feature = "rabbitmq",
    feature = "nats",
    feature = "kafka",
    feature = "redis-streams"
))]
pub(crate) mod routing;
```

**Verify**: `cargo build --features inmemory` → exit 0; `cargo build --no-default-features` → exit 0 (module not compiled, no warning).

### Step 3: Replace the inline hold-index expression in Kafka, NATS, RabbitMQ

In each of these, replace `(retry_count as usize).min(hold_queues.len() - 1)`
with `crate::routing::hold_index(retry_count, hold_queues.len())`. These sites
are all already inside an `if !hold_queues.is_empty()` / `else` branch, so the
`debug_assert` precondition holds and behavior is identical:
- `src/backends/kafka/consumer.rs:416`
- `src/backends/nats/consumer.rs:326`
- `src/backends/rabbitmq/router.rs:36`

Do not change anything else in those functions.

**Verify**: `cargo build --features kafka,nats,rabbitmq` → exit 0;
`grep -rn "min(hold_queues.len() - 1)" src/backends/kafka src/backends/nats src/backends/rabbitmq` → no matches.

### Step 4: Delegate Redis's `hold_level` to the shared helper

In `src/backends/redis/consumer.rs:1506-1512`, keep the `hold_level` signature
and its tests, but rewrite the body to delegate:

```rust
pub(super) fn hold_level<T>(retry_count: u32, hold_queues: &[T]) -> Option<usize> {
    if hold_queues.is_empty() {
        None
    } else {
        Some(crate::routing::hold_index(retry_count, hold_queues.len()))
    }
}
```

**Verify**: `cargo nextest run --features redis-streams hold_level` → the existing
Redis `hold_level` tests still pass.

### Step 5: Replace the inline index in inmemory `schedule_redelivery`

In `src/backends/inmemory/consumer.rs:743`, replace
`hold_queues[(retry_count as usize).min(hold_queues.len() - 1)]` with
`hold_queues[crate::routing::hold_index(retry_count, hold_queues.len())]`. This
site is in the `else if increment` branch (hold queues known non-empty).
Leave the `Duration::ZERO` empty-case branch and the Defer branch unchanged.

**Verify**: `cargo build --features inmemory` → exit 0.

### Step 6: Wire inmemory `route_outcome` to `decide_retry` (reference consumer)

Rewrite `src/backends/inmemory/consumer.rs:700-724` `route_outcome` to derive the
branch from `decide_retry`, preserving exact behavior:

```rust
async fn route_outcome(
    broker: &InMemoryBroker,
    topology: &'static QueueTopology,
    env: Envelope,
    outcome: Outcome,
    options: &ConsumerOptionsInner,
) {
    let retry_count = get_retry_count(&env.headers);
    match crate::routing::decide_retry(&outcome, retry_count, options.max_retries) {
        crate::routing::RetryDecision::Ack => {}
        crate::routing::RetryDecision::Dlq { .. } => {
            route_reject(broker, topology, env).await;
        }
        crate::routing::RetryDecision::Hold { increment } => {
            schedule_redelivery(broker, topology, env, increment);
        }
    }
}
```

Notes:
- In-memory's DLQ path is `route_reject` for both `Reject` and
  `max_retries_exceeded` (it does not differentiate the reason), so the `Dlq {
  reason }` is matched with `..`. This preserves current behavior exactly.
- `Hold { increment: true }` maps to `schedule_redelivery(.., true)` (Retry,
  increments), `increment: false` to `schedule_redelivery(.., false)` (Defer).
  Identical to the original branching.
- `get_retry_count` is now called once up front (it was previously called inside
  the `Retry` arm and again inside `schedule_redelivery`; calling it once here is
  harmless — `schedule_redelivery` still recomputes it internally).

**Verify**: `cargo nextest run --features inmemory,metrics --test inmemory_integration`
→ all in-memory integration tests pass unchanged. This is the regression net: the
in-memory suite (`tests/inmemory_integration.rs`) covers `retry_then_ack`,
`max_retries_exceeded_goes_to_dlq`, `defer_*`, oversized→DLQ, and timeout→retry
paths, all Docker-free.

### Step 7: Full lint + format

**Verify**:
- `cargo clippy --all-features -- -D warnings` → exit 0.
- `cargo clippy --no-default-features -- -D warnings` → exit 0.
- `cargo fmt -- --check` → exit 0.

## Test plan

- New unit tests in `src/routing.rs` (Step 1) pin the decision table and the
  index clamping — Docker-free, fast. Model their structure on the existing
  `hold_level` tests at `src/backends/redis/consumer.rs:1522-1534`.
- Existing Redis `hold_level` tests must still pass (Step 4).
- Existing in-memory integration tests are the behavior-preservation net for the
  Step 6 wiring; they must pass unchanged (no new ones required here).
- Kafka/NATS/RabbitMQ/Redis integration tests (Docker-required) are unaffected
  because only their index lookups changed; CI covers them. If Docker is
  available, run e.g. `cargo nextest run --features kafka,kafka-ssl,audit --test
  kafka_integration` to confirm; otherwise note that CI is the gate.

## Done criteria

ALL must hold:

- [ ] `src/routing.rs` exists with `hold_index`, `RetryDecision`, `decide_retry`,
      and unit tests including the `retry_count == max_retries` boundary case.
- [ ] `cargo nextest run --features inmemory,metrics` passes, including the new
      `routing` tests and the unchanged `inmemory_integration` suite.
- [ ] `grep -rn "min(hold_queues.len() - 1)" src/backends` returns matches ONLY
      where intentionally left (there should be none in kafka/nats/rabbitmq/
      inmemory/redis after this plan — Redis's was replaced; confirm zero).
- [ ] `cargo clippy --all-features -- -D warnings` and
      `cargo clippy --no-default-features -- -D warnings` both exit 0.
- [ ] `cargo fmt -- --check` exits 0.
- [ ] Only the in-scope files are modified (`git status`).
- [ ] `plans/README.md` status row for 004 updated.

## STOP conditions

Stop and report back (do not improvise) if:

- Any backend's `route_outcome` no longer matches the excerpts above (drift since
  `106bb05`).
- The in-memory integration suite fails after Step 6 — that means the wiring
  changed behavior; revert Step 6 and report which test failed (do NOT adjust the
  test to make it pass).
- You find that an inline index site is NOT guarded by a non-empty check (so the
  `debug_assert` could fire / a subtraction could underflow) — report it; it may
  be a latent bug to handle separately.
- You are tempted to change an empty-hold-queue fallback value to make things
  consistent — that is plan 005's decision, not this plan's. Stop.

## Maintenance notes

- After this lands, the retry-budget boundary lives in exactly one place
  (`decide_retry`). Plan 005 migrates the other five backends' decision branching
  to call it; until then, Kafka/NATS/RabbitMQ/Redis still inline their own
  branching (only their index lookups are shared).
- A reviewer should verify the in-memory wiring preserved the
  `Reject`/`max_retries` → `route_reject` mapping and the Defer-no-increment
  behavior, and that `routing.rs` stays `pub(crate)` (not part of the public API).
- The empty-hold-queue divergence (1s vs ZERO vs nack-requeue) is documented in
  plan 005 as an open decision; do not treat it as a bug here.
