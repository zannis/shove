# Plan 014: Error taxonomy — Auth/NotFound/Timeout variants with correct retryability

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before moving on. On any STOP condition, stop and
> report. Your reviewer maintains `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- src/error.rs src/backends/nats/client.rs src/backends/kafka/client.rs src/backends/kafka/consumer.rs src/backends/nats/consumer.rs`
> Earlier plans (006/007/009/011/012) touch the client/consumer files in
> regions unrelated to error construction; `src/error.rs` should be untouched.
> Compare excerpts on any drift; mismatch in the error-mapping regions = STOP.

## Status

- **Priority**: P2
- **Effort**: M–L
- **Risk**: MED (reclassification changes retry-loop behavior — the point,
  but every reclassified site needs justification)
- **Depends on**: none logically; execute after 006-012 to avoid churn
- **Category**: dx + ops
- **Planned at**: commit `e902d7c` (main), 2026-07-02
- **Maintainer decision**: add variants + mapping (option between "minimal
  auth-only fix" and "full structured redesign"). `#[non_exhaustive]` makes
  this non-breaking.

## Why this matters

`ShoveError` has six variants and `is_retryable()` returns true only for
`Connection(_)` (`src/error.rs:42-44`). Backends stringify everything into
`Connection`/`Topology`: NATS collapses **every** connect failure — including
credential rejection — into `Connection(e.to_string())`
(`src/backends/nats/client.rs:148-151`), so `connect_with_retry` and the
consumer reconnect loops retry a bad password on backoff instead of failing
fast. Kafka wraps admin/topology error codes as
`Topology(format!("...{code:?}"))` (`src/backends/kafka/client.rs:700-702`).
Operators cannot programmatically distinguish "auth rejected" (page a human)
from "broker unreachable" (wait) from "topic missing" (declare it); alerting
has to regex display strings.

## Current state

- `src/error.rs` (102 lines, read fully): `ShoveError` is
  `#[derive(Debug, thiserror::Error)] #[non_exhaustive]` with variants
  `Serialization(#[from] serde_json::Error)`, `Connection(String)`,
  `Topology(String)`, `Validation(String)`, `Codec { codec, source }`,
  `Unknown(String)`. `is_retryable()` = `matches!(self, Connection(_))`.
  Existing tests in-file assert Display strings and retryability.
- Retryability consumers (behavior changes ripple here — enumerate all with
  `grep -rn "is_retryable" src/`):
  `run_with_reconnect` in `src/backends/kafka/consumer.rs` (~:777) and
  `src/backends/nats/consumer.rs` (~:445), plus any others grep finds.
- Error construction sites to remap (the two priority families):
  - NATS connect: `src/backends/nats/client.rs:148-151`
    (`opts.connect(&config.url)` error → `Connection`). async-nats 0.49's
    `ConnectError` exposes a `kind()` (`ConnectErrorKind`) — check the
    vendored crate for the auth-ish kinds (e.g. `AuthorizationViolation`,
    `Authentication`-flavored variants, TLS failures) and map those to the
    new `Auth` variant; genuine I/O/timeout kinds stay `Connection`.
  - Kafka: `map_kafka_error` in `src/backends/kafka/consumer.rs` (search
    `fn map_kafka_error`) and the client/admin wrap sites in
    `src/backends/kafka/client.rs`. rdkafka's `KafkaError` exposes
    `RDKafkaErrorCode`; map `SaslAuthenticationFailed`/`Authentication`
    codes → `Auth`, `UnknownTopicOrPartition` → `NotFound`,
    request-timeout codes → `Timeout`. Everything else keeps its current
    variant.
- Conventions: Conventional Commits, no `Co-Authored-By`; clippy `-D warnings`
  both feature sets; `absolute-paths = "deny"`; `cargo nextest run` (never
  `-q`); thiserror is already a dependency — no new deps.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Lint | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Lint (min) | `cargo clippy -q --no-default-features -- -D warnings` | exit 0 |
| Full unit | `cargo nextest run --no-default-features` | all pass |
| Kafka unit | `cargo nextest run --features kafka --lib` | all pass |
| NATS unit | `cargo nextest run --features nats --lib` | all pass |
| Kafka integration (Docker) | `cargo nextest run --features kafka --test kafka_integration` | all pass |
| NATS integration (Docker) | `cargo nextest run --features nats --test nats_integration` | all pass |

## Scope

**In scope**:
- `src/error.rs` (variants, `is_retryable`, tests)
- `src/backends/nats/client.rs`, `src/backends/kafka/client.rs`,
  `src/backends/kafka/consumer.rs`, `src/backends/nats/consumer.rs`
  (error-construction sites only)
- `tests/nats_integration.rs` (one auth-failure classification test)

**Out of scope**:
- Other backends' error mapping (rabbitmq/redis/sns/inmemory) — same
  treatment later, separate decision; do NOT partially remap them.
- Any change to retry/DLQ routing semantics (`route_outcome`, `src/routing.rs`).
- Renaming/removing existing variants (would break matches downstream).

## Steps

### Step 1: Variants

Add to `ShoveError` (non-exhaustive, so additive is safe):

```rust
/// Authentication or authorization was rejected by the broker. Not
/// retryable: retrying the same credentials cannot succeed.
#[error("auth error: {0}")]
Auth(String),

/// A referenced entity (topic, stream, consumer) does not exist.
/// Not retryable by the connection loop; callers typically declare topology.
#[error("not found: {0}")]
NotFound(String),

/// An operation exceeded its deadline. Retryable.
#[error("timeout: {0}")]
Timeout(String),
```

`is_retryable()` → `matches!(self, Connection(_) | Timeout(_))`. Update the
rustdoc on `is_retryable` accordingly. In-file tests: `Auth`/`NotFound` not
retryable, `Timeout` retryable, Display prefixes.

### Step 2: NATS mapping

At `nats/client.rs` connect error: inspect `ConnectError::kind()` and map
auth-flavored kinds → `Auth`, else `Connection` (preserve the message text).
Check `connect_with_retry` (nats/client.rs:162-188): it retries on any error
today — add an early `return Err(e)` when `!e.is_retryable()` so a bad
credential fails fast (this is the user-visible fix). Mirror the same guard
in Kafka's `connect_with_retry` (kafka/client.rs:488-514).

Audit other NATS `Connection(...)` constructions in the consumer for
messages that are actually auth/not-found (`map_get_stream_error` at
nats/consumer.rs:~415 already isolates NotFound-ish kinds as retryable
`Connection` — leave its behavior IDENTICAL unless the get-stream error kind
is genuinely `NotFound` of a stream, which today deliberately stays retryable
because topology may be declared concurrently; do not change it, note why in
a comment).

### Step 3: Kafka mapping

In `map_kafka_error` (kafka/consumer.rs) and the connect/admin wrap sites in
kafka/client.rs: where a `KafkaError` carries an `RDKafkaErrorCode`, map
`SaslAuthenticationFailed` (and sibling auth codes you find on the enum) →
`Auth`; `UnknownTopicOrPartition` → `NotFound`; `RequestTimedOut`/
`OperationTimedOut` → `Timeout`. Preserve existing behavior for every other
code. CAUTION on `Timeout` in the consumer reconnect path: `Timeout` is
retryable, same as today's `Connection` mapping, so `run_with_reconnect`
behavior is unchanged there — state this invariant in a test or comment.

### Step 4: Classification tests

- Unit: feed representative rdkafka error codes through `map_kafka_error`
  and assert variants (construct `KafkaError::AdminOp(code)` or the cheapest
  constructible carrier — check what the existing tests build).
- Integration (`tests/nats_integration.rs`): start the NATS container with
  auth enabled (testcontainers module supports a config file or command
  args — if enabling auth on the container is not cheaply expressible, run
  `connect` against a user/password on the default unauthenticated container
  and assert whatever error the server returns; only assert `Auth` if the
  server actually rejects credentials. If neither is achievable, drop to a
  unit-level mapping test on `ConnectErrorKind` and record it in NOTES).
  Assert `connect_with_retry(cfg, 5)` with bad credentials returns quickly
  (single attempt) with `ShoveError::Auth`.

**Verify**: all table commands pass.

## Done criteria

- [ ] `cargo fmt -- --check`, both clippy commands exit 0
- [ ] All five test commands pass; new classification tests present
- [ ] `is_retryable()` truth table covered by unit tests (all 9 variants)
- [ ] `connect_with_retry` in both backends bails on non-retryable errors
      (test or code-reading evidence cited in NOTES)
- [ ] `map_get_stream_error` behavior unchanged (existing tests still pass)
- [ ] No files outside scope modified

## STOP conditions

- async-nats 0.49's `ConnectError` does not expose a usable kind for auth
  (report the actual API; the fallback is string-matching, which is a
  design smell worth reviewing before shipping).
- Reclassifying any error breaks an existing integration test that depended
  on retry-until-topology-appears semantics (report which test; likely the
  concurrent-declare pattern — that is exactly the class of regression this
  plan must not cause).
- The rdkafka error carrier types make unit-testing `map_kafka_error`
  impossible without a broker.

## Maintenance notes

- Follow-up (not planned): apply the same mapping to rabbitmq/redis/sns —
  the cross-backend consistency convention says delivery-semantics changes
  eventually need all backends; retryability classification is adjacent.
- Reviewer scrutiny: every reclassified construction site, one by one — each
  changes a retry loop's behavior somewhere.
- `Unknown` remains the "surface immediately" bucket; new SDK error kinds
  should be mapped deliberately, not defaulted to `Connection`.
