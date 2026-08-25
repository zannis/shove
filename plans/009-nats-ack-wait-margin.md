# Plan 009: Give NATS consumers an ack_wait margin above the handler timeout

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving on. If a
> STOP condition occurs, stop and report. Your reviewer maintains
> `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat dacfe5c..HEAD -- src/backends/nats/topology.rs src/backends/nats/consumer.rs src/backends/nats/consumer_group.rs`
> Expected drift on the advisor branch: plan 006 touched
> `src/backends/nats/consumer.rs` (reconnect-reset in `run_with_reconnect`)
> and plan 008 touched `src/backends/nats/consumer_group.rs`
> (`active_consumers`/pruning). Neither overlaps the excerpts below. Any other
> drift: STOP.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: LOW (additive config; behavior becomes strictly more conservative)
- **Depends on**: 006, 008 (same branch ordering only)
- **Category**: bug
- **Planned at**: commit `dacfe5c` (main), 2026-07-02

## Why this matters

Shove creates JetStream pull consumers without setting `ack_wait`, leaving the
server default of **30 seconds** — exactly equal to shove's default handler
timeout (`DEFAULT_HANDLER_TIMEOUT = 30s`, `src/consumer.rs:25`). There is zero
safety margin: a handler that legitimately runs near its timeout, or a message
that sits behind a saturated prefetch buffer before its handler even starts
(the `ack_wait` clock ticks from delivery, and delivered messages wait on
`semaphore.acquire_owned()` before processing), exceeds `ack_wait` and is
**redelivered while still in flight** — duplicate processing under perfectly
normal slow-handler load. Progress acks exist only inside the retry-hold path
(`hold_then_republish`), not during handler execution. And an operator who
raises `with_handler_timeout(60s)` silently makes every such handler a
guaranteed duplicate. Fix: derive `ack_wait = max(3 × effective handler
timeout, 30s)` at every consumer-creation site.

## Current state

- `src/backends/nats/topology.rs:151-190` — `declare_pull_consumer(stream,
  consumer_name, max_ack_pending, filter_subjects)` creates the group durable:

  ```rust
  stream.create_consumer(PullConsumerConfig {
      durable_name: Some(consumer_name.to_string()),
      ack_policy: AckPolicy::Explicit,
      max_ack_pending,
      filter_subject,
      filter_subjects,
      ..Default::default()          // <- ack_wait left at server default (30s)
  })
  ```

  `create_consumer` is an **upsert** — existing durables get updated config.
- Its caller: `src/backends/nats/consumer_group.rs` (~lines 163-180) — the
  group register path; the same config object resolves the handler timeout:
  `NatsConsumerGroupConfig::handler_timeout()` at ~115-116 returns
  `Some(resolve_handler_timeout(self.handler_timeout, None))`, and ~466 sets
  `options.handler_timeout = Some(resolve_handler_timeout(self.config.handler_timeout, None))`.
  Registry-level defaults are folded into `config.handler_timeout` at add-time
  (~line 540, `with_default_handler_timeout` resolution) — verify by reading
  that block; the resolved per-group value is what consumers actually enforce.
- `src/backends/nats/consumer.rs:581-621` — concurrent-path bootstrap: fast
  path `get_consumer` (read-only); on `NotFound`, a fallback one-shot
  `create_consumer` with the same `..Default::default()` gap. `ConsumerOptions`
  (`src/consumer.rs:117`) carries `handler_timeout: Option<Duration>` and is in
  scope here.
- `src/backends/nats/consumer.rs:954-963` — FIFO shard path:
  `get_or_create_consumer(&consumer_name, PullConsumerConfig { .., max_ack_pending: 1, ..Default::default() })`.
  NOTE: `get_or_create_consumer` returns an existing consumer **verbatim**
  (does not update config) — only newly created shard durables get the new
  `ack_wait`. Acceptable; call it out in NOTES.
- Consumers already *read* the effective value at runtime:
  `let ack_wait = pull_consumer.cached_info().config.ack_wait;`
  (`consumer.rs:972`, similar in the concurrent path — search `ack_wait` in the
  file) and thread it into `route_outcome`/`hold_then_republish` heartbeats —
  so the hold-heartbeat logic adapts automatically. Do not change it.
- `DEFAULT_HANDLER_TIMEOUT`: `src/consumer.rs:25` (`30s`);
  `resolve_handler_timeout`: `src/consumer.rs:47`.
- Conventions: Conventional Commits, no `Co-Authored-By`; clippy `-D warnings`
  both feature sets; `absolute-paths = "deny"`; `cargo nextest run` only
  (never `-q` to nextest). NATS integration tests each start their own
  container — copy a nearby test's setup in `tests/nats_integration.rs`
  (e.g. `topology_managed_config_reconciles_existing_stream` shows how to
  inspect JetStream state after registration).

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Lint | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Lint (min) | `cargo clippy -q --no-default-features -- -D warnings` | exit 0 |
| NATS unit | `cargo nextest run --features nats --lib` | all pass |
| NATS integration (Docker) | `cargo nextest run --features nats --test nats_integration` | all pass |

## Scope

**In scope**:
- `src/backends/nats/topology.rs` (add `ack_wait` param to `declare_pull_consumer`)
- `src/backends/nats/consumer_group.rs` (compute + pass it)
- `src/backends/nats/consumer.rs` (fallback + FIFO creation sites; the
  derivation helper)
- `tests/nats_integration.rs` (new assertions)

**Out of scope**:
- Any change to `hold_then_republish` heartbeats or `route_outcome`.
- Progress-acks *during handler execution* (with a 3× margin the handler
  timeout always fires first; deferred).
- A user-facing `with_ack_wait` knob (belongs to the NatsConfig-ergonomics
  plan being designed separately).
- `max_deliver`, stream configs, other backends.

## Steps

### Step 1: Derivation helper

In `src/backends/nats/consumer.rs` (importable by `consumer_group.rs` /
`topology.rs` via `pub(super)` or a small shared location in the nats module):

```rust
/// JetStream redelivers any message not acked within `ack_wait`. Derive it
/// from the handler timeout with a 3x margin so a handler running to its
/// limit — plus queue-wait behind a full prefetch buffer — never has its
/// message redelivered mid-flight. Floor at the JetStream default (30s) so
/// short handler timeouts don't tighten redelivery below the server default.
pub(super) fn derive_ack_wait(handler_timeout: Duration) -> Duration {
    (handler_timeout * 3).max(Duration::from_secs(30))
}
```

Unit tests: 30s→90s, 10s→30s (floor), 2m→6m.

### Step 2: Thread it through the three creation sites

1. `declare_pull_consumer` gains `ack_wait: Duration` and sets it in the
   `PullConsumerConfig`. Update its caller in `consumer_group.rs` to pass
   `derive_ack_wait(<the group's resolved handler timeout>)` — use the same
   resolved value the group threads into `ConsumerOptions` (read the ~466
   block; if the registry-default fold-in at ~540 means `self.config.handler_timeout`
   is already resolved, use exactly what options get).
2. The `NotFound` fallback `create_consumer` in `consumer.rs` (~607-620):
   set `ack_wait: derive_ack_wait(options.handler_timeout.unwrap_or(DEFAULT_HANDLER_TIMEOUT))`
   from the `ConsumerOptions` in scope.
3. The FIFO `get_or_create_consumer` (~954-963): same derivation from the
   FIFO path's options.

All three sites must use the same helper — grep afterwards:
`grep -n "ack_wait" src/backends/nats/*.rs` and confirm no remaining
consumer-creation site leaves it defaulted.

### Step 3: Integration assertions

In `tests/nats_integration.rs` add a test (pattern: any group-registration
test + JetStream introspection like the stream-reconcile test):

- register a consumer group with `with_handler_timeout(Duration::from_secs(20))`,
  then fetch the durable's consumer info via the jetstream context and assert
  `config.ack_wait == Duration::from_secs(60)`;
- register a group with the default (no explicit timeout) and assert
  `ack_wait == Duration::from_secs(90)` (3 × 30s);
- a FIFO/sequenced registration creates shard consumers with the derived
  `ack_wait` (assert on one shard durable's info).

Also confirm existing redelivery-dependent tests still pass — none should
depend on the 30s default (retry/defer tests use explicit Nak delays), but if
one does, that's a STOP, not a test edit.

**Verify**: `cargo nextest run --features nats --test nats_integration` → all pass.

## Done criteria

- [ ] `cargo fmt -- --check` exits 0; both clippy commands exit 0
- [ ] `cargo nextest run --features nats --lib` all pass (incl. derive tests)
- [ ] `cargo nextest run --features nats --test nats_integration` all pass,
      including the 3 new ack_wait assertions
- [ ] `grep -rn "\.\.Default::default()" src/backends/nats/topology.rs src/backends/nats/consumer.rs`
      shows no `PullConsumerConfig` literal that omits `ack_wait`
- [ ] No files outside scope modified

## STOP conditions

- The resolved-handler-timeout plumbing in `consumer_group.rs` doesn't match
  the description (e.g. the value passed to consumers differs from what the
  register path can see) — report what you found.
- An existing integration test depends on the 30s default redelivery and
  fails — report it; do not weaken the test.
- async-nats rejects `ack_wait` updates on the existing durable in
  `declare_pull_consumer`'s upsert (consumer-update error) — report the exact
  error.

## Maintenance notes

- The 3× factor covers queue-wait behind a full prefetch buffer only up to
  2× the handler timeout; extremely large `prefetch_count` with uniformly slow
  handlers can still exceed it. If that surfaces, the fix is progress-acks
  while waiting for the semaphore — deferred deliberately.
- The planned `NatsConfig`/topology ergonomics work may add an explicit
  `with_ack_wait` override; it should route through `derive_ack_wait` as the
  default and document the invariant `ack_wait > handler_timeout`.
- FIFO shard durables created before this change keep 30s until recreated
  (`get_or_create_consumer` doesn't update config) — release notes material.
