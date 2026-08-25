# Plan 017: Integration coverage — NATS consumer-deletion recovery + autoscaler scale-down on real stats

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before moving on. On any STOP condition, stop and
> report. Your reviewer maintains `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- tests/kafka_integration.rs tests/nats_integration.rs src/backends/nats/consumer.rs`
> Earlier plans may have appended tests to these files and edited the
> consumer; that is expected. Verify the cited recovery-branch code below
> still exists (`grep -n "NotFound" src/backends/nats/consumer.rs`).

## Status

- **Priority**: P3
- **Effort**: M
- **Risk**: LOW (test-only)
- **Depends on**: 008 + 015 if landed (scale-down asserts must match the
  respawn behavior — see step 2 note); otherwise standalone
- **Category**: tests
- **Planned at**: commit `e902d7c` (main), 2026-07-02

## Why this matters

Two production-critical paths run with zero coverage:

1. **NATS durable-consumer deletion**: `src/backends/nats/consumer.rs:581-621`
   has a `ConsumerInfoErrorKind::NotFound` fallback that re-creates the
   consumer when it vanished server-side (operator action, server storage
   loss). If the recreated consumer bootstraps with wrong config or the
   branch regresses, consumption silently stalls or mass-redelivers.
2. **Autoscaler scale-down on real stats**: both backends' only autoscaling
   integration test (`autoscaling_scales_up_and_drains_clean`,
   `tests/kafka_integration.rs:~2717` and `tests/nats_integration.rs:~2644`)
   covers scale-UP then drain. Scale-down when idle — driven by the same live
   lag/pending queries (`KafkaQueueStats` from committed-vs-high-watermark,
   NATS `num_pending`) — is exercised only against mocked stats in unit
   tests. A regression leaves consumers running forever (cost) or tears them
   down while lag remains (stall).

## Current state

- The NATS recovery branch: concurrent path bootstrap does `get_consumer`
  (read-only fast path); on NotFound it one-shot `create_consumer`s
  (nats/consumer.rs:581-621, sibling logic ~:949-955). The durable's name
  derivation and the group registration flow live in
  `src/backends/nats/consumer_group.rs` (declare via
  `declare_pull_consumer`).
- Deleting a consumer server-side from a test: the async-nats jetstream
  context (available in tests — see how existing tests build a raw
  `jetstream` context to inspect stream state, e.g. the config-reconcile
  test around nats_integration.rs:622) exposes stream handles with a
  `delete_consumer(name)` API — verify the exact method on 0.49.
- The scale-up tests to extend: read both `autoscaling_scales_up_and_drains_clean`
  implementations first; they configure a group `min..=max`, generate lag,
  assert scale-up, then drain. The autoscaler poll interval / hysteresis /
  cooldown knobs used there (`src/autoscaler.rs::Stabilized` — hysteresis +
  cooldown gates) determine how long an idle period must last before a
  ScaleDown decision fires; reuse the same short intervals those tests set.
- Conventions: struct MessageHandlers only; `cargo nextest run` (never `-q`);
  Conventional Commits; no `Co-Authored-By`. Known flake: 0.2s connect
  failures at container setup are infra races.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| NATS integration (Docker) | `cargo nextest run --features nats --test nats_integration` | all pass |
| Kafka integration (Docker) | `cargo nextest run --features kafka --test kafka_integration` | all pass |
| Lint | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |

## Scope

**In scope**:
- `tests/nats_integration.rs` (two new tests)
- `tests/kafka_integration.rs` (one new/extended test)

**Out of scope**:
- ANY `src/` change — bugs found are findings to report, not fixes.

## Steps

### Step 1: NATS consumer-deletion recovery test

In `tests/nats_integration.rs`: register a consumer group, process a first
batch, then delete the durable server-side via the jetstream context
(`get_stream(queue)` → `delete_consumer(<derived durable name>)` — derive the
name the same way the group does; find it by listing consumers on the stream
if derivation is awkward). Publish a second batch and assert it is processed
within a generous budget (the recovery branch re-creates the durable).
Tolerate redelivery of batch-1 messages (at-least-once); assert set-coverage
of batch 2. If recovery does NOT happen (test hangs), that is a STOP-and-
report finding with the observed behavior.

**Verify**: the new test passes twice consecutively.

### Step 2: Scale-down assertions (both backends)

Extend each `autoscaling_scales_up_and_drains_clean` (or add a sibling test
reusing its harness) so that after the drain completes, the queue is held
idle past the strategy's hysteresis + cooldown, and assert
`active_consumers()` returns to the configured minimum. Note if plan 015
landed: respawn-to-min runs on the same tick — the assertion target is
exactly `min_consumers`, which both features agree on. Use the group handle
the test already holds; poll with a deadline rather than a fixed sleep.

**Verify**: both integration suites pass twice consecutively.

## Done criteria

- [ ] Three new/extended tests pass twice consecutively
- [ ] Full kafka + nats integration suites pass
- [ ] Clippy exits 0
- [ ] No `src/` files modified

## STOP conditions

- The deletion-recovery test reveals the consumer does not recover (finding).
- async-nats 0.49 has no consumer-deletion API reachable from tests.
- Scale-down cannot be made deterministic within the existing tests' knob
  ranges after 3 attempts (report the timing analysis).

## Maintenance notes

- The scale-down assertions become the guard for the `KafkaQueueStats` lag
  arithmetic (plan 006 step 4) and NATS pending computation — note that in
  the test comments.
- If plan 015's supervision changes group counts on ticks, these tests are
  where a conflict would first show.
