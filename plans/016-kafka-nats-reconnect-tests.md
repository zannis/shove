# Plan 016: Broker-restart reconnect tests for Kafka and NATS

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before moving on. On any STOP condition, stop and
> report. Your reviewer maintains `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- tests/ src/backends/kafka/consumer.rs src/backends/nats/consumer.rs`
> Expected drift: plans 006/007/009 edits to the consumer files (this plan is
> test-only and reads them for behavior, not excerpts). New test files from
> plan 007 (`tests/kafka_rebalance.rs`) may exist. Anything else surprising =
> STOP.

## Status

- **Priority**: P2
- **Effort**: L (timing-sensitive container orchestration, two backends)
- **Risk**: LOW (test-only; flake risk is the real cost — budget generous
  timeouts)
- **Depends on**: 006 (tests also lock in its reconnect-budget reset)
- **Category**: tests
- **Planned at**: commit `e902d7c` (main), 2026-07-02

## Why this matters

The single most production-critical event these backends must survive — a
broker rolling restart — has zero test coverage for Kafka and NATS (RabbitMQ
has `tests/rabbitmq_reconnect.rs`). The consumer's only resilience mechanism
is `run_with_reconnect`, which re-enters only when the error maps to a
retryable `ShoveError` — nothing verifies that a real broker drop takes that
path rather than killing the consumer. A regression here ships silently and
manifests as "consumers died during the Tuesday broker upgrade."

## Current state

- The pattern to mirror: `tests/rabbitmq_reconnect.rs` —
  `client_recovers_after_broker_app_restart` starts the container, verifies a
  baseline operation, stops the broker application **inside the container**
  via `container.exec(ExecCommand::new([...]))` (so the mapped port survives —
  do NOT stop/start the container itself, which can remap ports), asserts
  operations fail while down, restarts the app, and polls until recovery
  within `RECOVERY_BUDGET = 30s`.
- Kafka equivalent of `rabbitmqctl stop_app`: the container images used by
  the existing tests (see how `tests/kafka_integration.rs` starts Kafka —
  read its container setup first; testcontainers-modules' kafka/redpanda
  module) — options: `container.exec` with the broker's stop script, SIGSTOP/
  SIGCONT the broker process (`kill -STOP 1`), or `docker pause`-equivalent
  via exec. **Investigate which is expressible with the testcontainers API
  in this repo's version and reliably drops client connections**; SIGSTOP of
  PID 1 via exec is usually the most portable (connections hang → librdkafka
  timeouts → retryable errors) but produces timeout-flavored rather than
  connection-closed errors — either is fine, both must classify retryable.
- NATS equivalent: the nats-server process is PID 1 in the standard image;
  `nats-server --signal` needs a pid file, so SIGSTOP/SIGCONT via exec is the
  simplest. async-nats also auto-reconnects at the client layer — the test
  must still prove shove's consume loop keeps delivering after the blip.
- What to assert (per backend):
  1. consumer group processing works (baseline batch A fully handled);
  2. broker goes away; publisher `publish` fails or blocks-and-recovers
     (assert an error surfaces within a bounded window OR the publish
     eventually succeeds after recovery — do not assert a specific error
     string);
  3. broker returns; batch B (published after recovery) is fully processed
     by the SAME consumer group registration (no re-register) within
     `RECOVERY_BUDGET`;
  4. no message from batch A is lost (received set ⊇ batch A ∪ batch B;
     duplicates are acceptable — at-least-once).
- Handler/collection pattern: copy whatever `tests/kafka_integration.rs` /
  `tests/nats_integration.rs` use for collecting received messages
  (Arc<Mutex<Vec>> or channel + a struct MessageHandler — struct handlers
  only; do NOT introduce closure-based handler impls).
- Conventions: `cargo nextest run` (never `-q` to nextest); Conventional
  Commits; no `Co-Authored-By`. Known infra flake: a 0.2s connect failure at
  container setup is an environment race — retry the run before diagnosing.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Kafka reconnect (Docker) | `cargo nextest run --features kafka --test kafka_reconnect` | pass |
| NATS reconnect (Docker) | `cargo nextest run --features nats --test nats_reconnect` | pass |
| Lint | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Regression | `cargo nextest run --features kafka --test kafka_integration` and `--features nats --test nats_integration` | all pass |

## Scope

**In scope**:
- `tests/kafka_reconnect.rs` (create)
- `tests/nats_reconnect.rs` (create)

**Out of scope**:
- ANY `src/` change. If the tests reveal a real reconnect bug, STOP and
  report it as a finding — do not fix it in this plan.
- The existing integration files.

## Steps

### Step 1: NATS first (simpler)

Write `tests/nats_reconnect.rs` per the assertion list above, modeled
structurally on `tests/rabbitmq_reconnect.rs` (tracing init, container start,
budget constants) and on `tests/nats_integration.rs` for broker setup +
consumer-group registration. Freeze the broker with
`container.exec(ExecCommand::new(["kill", "-STOP", "1"]))`, sleep past the
client ping interval, `-CONT` to resume. If exec-signaling proves unreliable
in 2 attempts, try the module's documented stop mechanism and record what
worked in NOTES.

**Verify**: `cargo nextest run --features nats --test nats_reconnect` → pass,
twice in a row (flake check).

### Step 2: Kafka

Same shape in `tests/kafka_reconnect.rs`. Kafka's client timeouts are longer —
size `RECOVERY_BUDGET` accordingly (60-90s) and keep the frozen window shorter
than `session.timeout.ms`-driven group eviction only if you want to avoid a
rebalance in the test; a rebalance occurring is FINE (it is realistic) as long
as batch-B delivery is asserted.

**Verify**: `cargo nextest run --features kafka --test kafka_reconnect` →
pass, twice in a row.

## Done criteria

- [ ] Both new test files pass twice consecutively
- [ ] Both regression suites still pass
- [ ] Tests assert batch-B processing on the ORIGINAL registration (grep: no
      second `register` call after the blip)
- [ ] `cargo clippy -q --all-features --all-targets -- -D warnings` exit 0
- [ ] No `src/` files modified

## STOP conditions

- A real reconnect bug surfaces (consumer never recovers) — report with the
  failure evidence; that is a finding, not a test problem.
- Neither SIGSTOP nor a module-native stop mechanism produces a client-visible
  outage in 3 attempts.
- The test cannot be de-flaked to 2 consecutive passes.

## Maintenance notes

- These tests are the regression harness for plan 006's reconnect-budget
  reset and any future `run_with_reconnect` change.
- If CI runtime becomes a concern, these files are candidates for a separate
  slower CI job — flag rather than delete.
