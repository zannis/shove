# Plan 006: Batch of six small Kafka/NATS production fixes

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. Your reviewer maintains `plans/README.md`; do
> not update it.
>
> **Drift check (run first)**:
> `git diff --stat dacfe5c..HEAD -- src/backends/kafka/consumer.rs src/backends/nats/consumer.rs src/backends/nats/publisher.rs src/backends/kafka/autoscaler.rs src/backends/kafka/client.rs src/backends/nats/client.rs src/topology.rs src/topology_declarer.rs src/backends/kafka/consumer_group.rs docs/pages/backends/kafka.mdx docs/pages/backends/kafka/examples/basic.mdx`
> You will be working on a branch created from `dacfe5c` (main), so this should
> report no changes. If it does, compare the "Current state" excerpts against
> the live code; on a mismatch, STOP.

## Status

- **Priority**: P1
- **Effort**: M (six S-sized independent fixes)
- **Risk**: LOW
- **Depends on**: none
- **Category**: bug + dx + docs
- **Planned at**: commit `dacfe5c` (main), 2026-07-02

## Why this matters

Six independently-verified sharp edges in the Kafka and NATS backends:
(a) the final offset commit on graceful shutdown is async-and-dropped, so every
clean deploy can redeliver the last drained batch; (b) the reconnect-attempt
budget never resets after a healthy period, so a finite `max_reconnect_attempts`
is a lifetime budget that eventually kills a healthy consumer; (c) NATS
`publish_batch` abandons already-submitted acks on a mid-batch error and reports
`succeeded=0`, so callers that re-drive the batch produce duplicates; (d) the
Kafka autoscaler counts the full high-watermark as lag for groups that have
never committed, which for `latest`-reset groups causes a spurious scale-to-max
at startup; (e) the builder types lack `#[must_use]`, so
`config.with_tls(tls);` (result dropped) silently ships without TLS; (f) the
docs reference a nonexistent `KafkaConfig::with_ssl` and tell users to tune
config keys that are not settable.

## Current state

- `src/backends/kafka/consumer.rs` — Kafka consumer; the shutdown branch of the
  receive loop (lines ~953-964):

  ```rust
  _ = shutdown.cancelled() => {
      tracing::info!(queue, "shutdown signal received, draining in-flight tasks");
      let _ = semaphore.acquire_many(prefetch_count as u32).await;
      // Final commit
      while let Ok((partition, offset)) = completion_rx.try_recv() {
          tracker.mark_complete(partition, offset);
      }
      if let Some(tpl) = tracker.drain_committable() {
          consumer.commit(&tpl, CommitMode::Async).ok();
      }
      return Ok(());
  }
  ```

- `src/backends/kafka/consumer.rs:761-813` and
  `src/backends/nats/consumer.rs:429-...` — two structurally identical private
  `run_with_reconnect` loops. Shape (identical in both files):

  ```rust
  let mut backoff = Backoff::default();
  let mut attempts = 0u32;
  loop {
      match f().await {
          Ok(()) => return Ok(()),
          Err(e) => {
              if !e.is_retryable() { return Err(e); }
              if shutdown.is_cancelled() { return Ok(()); }
              attempts += 1;
              if let Some(max) = max_reconnect_attempts && attempts >= max { ... return Err(...); }
              let delay = backoff.next().expect("backoff is infinite");
              ...
          }
      }
  }
  ```

  `attempts` and `backoff` are never reset, even if `f()` ran healthily for
  hours between failures.

- `src/backends/nats/publisher.rs:148-211` — `publish_batch`. On a submission
  error the loop `break`s with `first_err` set; the ack loop is gated on
  `if first_err.is_none()`, so acks already collected in `ack_futures` are
  never awaited and `succeeded` returns 0. The comment above the loop says the
  intent is to "attribute partial-failure counters to what NATS actually
  accepted" — the code fails that intent on this path.

- `src/backends/kafka/autoscaler.rs:291-320` — per-partition lag arithmetic in
  `KafkaLagStatsProvider::get_queue_stats`:

  ```rust
  let (_low, high) = c.fetch_watermarks(&q, pid, Duration::from_secs(5))...;
  if let Some(elem) = committed.find_partition(&q, pid) {
      let committed_offset = match elem.offset() {
          rdkafka::Offset::Offset(o) => o,
          _ => 0,
      };
      if high > committed_offset {
          total += (high - committed_offset) as u64;
      }
  } else {
      total += high as u64;
  }
  ```

  A group that has never committed yields `Offset::Invalid` → treated as 0 →
  lag = full `high`. Correct-ish for `earliest` groups only when `low == 0`
  (wrong once retention truncates the log), and flatly wrong for `latest`
  groups (real lag ≈ 0).

- The trait being extended, `src/backends/kafka/autoscaler.rs:34-46`:

  ```rust
  pub trait KafkaQueueStatsProvider: Send + Sync {
      fn get_queue_stats(&self, queue: &str, group_id: &str)
          -> impl Future<Output = Result<KafkaQueueStats>> + Send;
  }
  ```

  Its caller `KafkaAutoscalerBackend::fetch_metrics`
  (`src/backends/kafka/autoscaler.rs:385-420`) already locks the registry and
  reads the group's config; `KafkaConsumerGroupConfig` exposes
  `auto_offset_reset() -> Option<KafkaAutoOffsetReset>`
  (`src/backends/kafka/consumer_group.rs:224-225`), and the consumer resolves
  `None` to `KafkaAutoOffsetReset::Earliest`
  (`src/backends/kafka/consumer.rs:863-864`). A `MockKafkaStatsProvider`
  implements the trait in the same file's tests (~line 475).

- `#[must_use]` exists on only 2 items in the whole crate. Builder types with
  consuming `-> Self` methods: `KafkaConfig` / `KafkaTls` / `KafkaSasl`
  (`src/backends/kafka/client.rs:50, 99, 222`), `NatsConfig`
  (`src/backends/nats/client.rs:14`), `TopologyBuilder` (`src/topology.rs`),
  `TopologyDeclarer` (`src/topology_declarer.rs`).

- `docs/pages/backends/kafka/examples/basic.mdx:42` and `:56` both say
  `KafkaConfig::with_ssl`; the real method is `with_tls`
  (`src/backends/kafka/client.rs:253`). `docs/pages/backends/kafka.mdx:322`
  (the "Gotchas" bullet on consumer-group rebalances) says "Tune
  `session.timeout.ms` and `max.poll.interval.ms` in `KafkaConfig`" — neither
  is settable; they are fixed constants (`SESSION_TIMEOUT_MS = 10_000`,
  `MAX_POLL_INTERVAL_MS = 300_000` in `src/backends/kafka/constants.rs`).

- Conventions: Conventional Commits; **never** add `Co-Authored-By` trailers.
  Clippy runs with `-D warnings` on both `--all-features` and
  `--no-default-features`. `[lints.clippy] absolute-paths = "deny"` — use
  `use` imports, not inline `crate::...` paths. Tests run with
  `cargo nextest run` (never plain `cargo test`; never pass `-q` to nextest).

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Format | `cargo fmt` then `cargo fmt -- --check` | exit 0 |
| Lint (all) | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Lint (min) | `cargo clippy -q --no-default-features -- -D warnings` | exit 0 |
| Unit tests | `cargo nextest run --no-default-features` | all pass |
| Kafka unit | `cargo nextest run --features kafka --lib` | all pass |
| Kafka integration (Docker) | `cargo nextest run --features kafka --test kafka_integration` | all pass |
| NATS integration (Docker) | `cargo nextest run --features nats --test nats_integration` | all pass |

## Scope

**In scope** (the only files you should modify):
- `src/backends/kafka/consumer.rs` (fixes a, b)
- `src/backends/nats/consumer.rs` (fix b)
- `src/backends/nats/publisher.rs` (fix c)
- `src/backends/kafka/autoscaler.rs` (fix d)
- `src/backends/kafka/client.rs`, `src/backends/nats/client.rs`,
  `src/topology.rs`, `src/topology_declarer.rs` (fix e — attribute lines only)
- `docs/pages/backends/kafka/examples/basic.mdx`,
  `docs/pages/backends/kafka.mdx` (fix f)
- `tests/nats_integration.rs` (fix c test, only if a new integration test is
  needed; prefer a unit test in publisher.rs if the ack-drain logic can be
  factored to be testable without a broker — do NOT force it; an integration
  test in the existing file is fine)

**Out of scope** (do NOT touch):
- `src/backends/kafka/consumer_group.rs` beyond *reading* config getters
  (plan 008 owns its changes). If fix (d) needs a getter that doesn't exist,
  STOP.
- Any change to `route_outcome`, retry/DLQ semantics, or `src/routing.rs`.
- `src/backends/kafka/constants.rs` values (do not retune timeouts).
- Any other backend (rabbitmq/redis/sns/inmemory) even though `run_with_reconnect`
  has siblings there — cross-backend parity is a separate decision.

## Git workflow

- You are in a fresh worktree. First: `git checkout -b advisor/kafka-nats-prod-fixes dacfe5c`
- One commit per fix (six commits), Conventional Commits style, e.g.:
  - `fix(kafka): commit final offsets synchronously on graceful shutdown`
  - `fix(consumer): reset reconnect budget after a healthy run (kafka, nats)`
  - `fix(nats): await already-submitted acks in publish_batch partial failure`
  - `fix(kafka): make autoscaler lag respect auto.offset.reset for uncommitted groups`
  - `feat(api): add #[must_use] to config and topology builder types`
  - `docs(kafka): fix with_ssl -> with_tls and remove dead tuning advice`
- **No `Co-Authored-By` trailers.** Do not push; the reviewer handles the PR.

## Steps

### Step 1: Kafka — synchronous final commit on shutdown

In `src/backends/kafka/consumer.rs`, in the shutdown branch shown above,
replace `consumer.commit(&tpl, CommitMode::Async).ok();` with a
`CommitMode::Sync` commit whose error is logged (not returned — shutdown must
proceed):

```rust
if let Err(e) = consumer.commit(&tpl, CommitMode::Sync) {
    tracing::warn!(queue, error = %e, "final offset commit failed during shutdown; batch may be redelivered");
}
```

There is exactly one such shutdown-branch commit in the concurrent path. Do NOT
change the steady-state `CommitMode::Async` commit at the top of the loop
(~line 949) — the next iteration re-commits there, so async is correct.
Check whether the FIFO path (`spawn_fifo_shards`, lines ~1309-1655) has an
equivalent final-commit-then-return on shutdown; FIFO commits use
`commit_message` per message, so there is likely nothing to change — if you
find an async final commit there too, apply the same treatment.

**Verify**: `cargo clippy -q --features kafka --all-targets -- -D warnings` → exit 0.

### Step 2: Reset reconnect budget after a healthy run (both backends)

In both `run_with_reconnect` functions (`src/backends/kafka/consumer.rs:761`,
`src/backends/nats/consumer.rs:429`): measure how long each `f()` invocation
ran. If it ran longer than a threshold before failing, the connection was
healthy — reset `attempts = 0` and `backoff = Backoff::default()` before
handling the error. Add a module-level constant (in each file, near the
function):

```rust
/// A consumer that stayed up at least this long before erroring is considered
/// to have had a healthy connection: the reconnect budget and backoff reset,
/// so `max_reconnect_attempts` bounds *consecutive* failures, not lifetime.
const RECONNECT_RESET_AFTER: Duration = Duration::from_secs(60);
```

Shape:

```rust
loop {
    let started = tokio::time::Instant::now();
    match f().await {
        Ok(()) => return Ok(()),
        Err(e) => {
            if started.elapsed() >= RECONNECT_RESET_AFTER {
                attempts = 0;
                backoff = Backoff::default();
            }
            ...unchanged...
```

Add a tokio paused-time unit test in each file's `#[cfg(test)]` module proving
that with `max_reconnect_attempts = Some(2)`, a closure that runs longer than
the threshold (use `tokio::time::advance`) before erroring retryably can fail
more than 2 times total without exhausting the budget, while consecutive fast
failures still exhaust it. Use `#[tokio::test(start_paused = true)]`;
`ShoveError::Connection` is retryable. Keep the tests deterministic — no real
sleeps.

**Verify**: `cargo nextest run --features kafka --lib` and
`cargo nextest run --features nats --lib` → new tests pass.

### Step 3: NATS publish_batch — always drain submitted acks

In `src/backends/nats/publisher.rs`, restructure the tail of `publish_batch`
so the collected `ack_futures` are awaited even when submission broke early:
count each `Ok` ack into `succeeded`; on an ack error record it into
`first_err` **only if `first_err` is empty** (the submission error takes
precedence) and keep draining the remaining acks (they were submitted; their
outcome must be counted). Return `(succeeded, Err(first_err))` as before.
Preserve the existing metrics calls (one `record_backend_error` per failure).

Add a test proving the count: publish a batch where submission succeeds for
N messages (this path is easiest to exercise against a real broker — add to
`tests/nats_integration.rs` following the file's existing container-setup
pattern only if a unit test is impractical). Minimum bar: a test that the
happy path returns `(N, Ok(()))` and — if you can injection-fail a subject
(e.g. a subject that no stream captures returns an error PubAck) — that
`succeeded` reflects genuinely-stored messages. If fault injection proves
impractical, say so in NOTES rather than shipping a fake test.

**Verify**: `cargo nextest run --features nats --test nats_integration` → all pass.

### Step 4: Kafka autoscaler lag honors auto.offset.reset

1. Extract the per-partition arithmetic into a pure function in
   `src/backends/kafka/autoscaler.rs`:

   ```rust
   /// Lag for one partition given the group's committed offset and the
   /// partition watermarks. When the group has never committed, the effective
   /// start position depends on `auto.offset.reset`: `earliest` starts at the
   /// low watermark, `latest` at the high watermark (zero lag).
   fn partition_lag(
       committed: Option<i64>,
       low: i64,
       high: i64,
       reset: KafkaAutoOffsetReset,
   ) -> u64
   ```

   Semantics: `committed = Some(o)` → `max(high - o, 0)`; `None` + `Earliest`
   → `max(high - low, 0)`; `None` + `Latest` → `0`. Map both `Offset::Invalid`
   and "partition absent from the committed TPL" to `None` at the call site.
   Use the existing `(_low, high)` tuple — rename `_low` to `low`.

2. Add a `reset: KafkaAutoOffsetReset` parameter to
   `KafkaQueueStatsProvider::get_queue_stats` (breaking change to a pub trait —
   acceptable, pre-1.0). Thread it from `fetch_metrics`:
   `g.config().auto_offset_reset().unwrap_or(KafkaAutoOffsetReset::Earliest)`
   (mirrors `src/backends/kafka/consumer.rs:863-864`). Update
   `MockKafkaStatsProvider` in the same file's tests and any other impl
   (search: `grep -rn "get_queue_stats" src/ tests/ benches/ examples/`).

3. Unit tests for `partition_lag`: committed normal case, committed > high
   (returns 0), never-committed earliest with `low > 0` (returns high-low,
   not high), never-committed latest (returns 0).

`KafkaAutoOffsetReset` lives in `src/backends/kafka/consumer_group.rs:32` and
is `Copy`. It needs to be importable from autoscaler.rs — it is already `pub`.

**Verify**: `cargo nextest run --features kafka --lib` → new tests pass;
`cargo clippy -q --all-features --all-targets -- -D warnings` → exit 0.

### Step 5: #[must_use] on builder types

Add `#[must_use]` directly above these type declarations (the attribute on the
*type* makes any unused function return of that type warn, covering all current
and future builder methods):

- `pub struct KafkaConfig` and, under `#[cfg(feature = "kafka-ssl")]`,
  `pub struct KafkaTls`, `pub enum KafkaSasl` (`src/backends/kafka/client.rs`)
- `pub struct NatsConfig` (`src/backends/nats/client.rs`)
- `pub struct TopologyBuilder` (`src/topology.rs`)
- `pub struct TopologyDeclarer` (`src/topology_declarer.rs`)

If clippy then flags any existing internal call site for an unused result,
fix that call site (it is a latent bug of exactly the kind this prevents) and
mention it in NOTES.

**Verify**: both clippy commands → exit 0;
`cargo nextest run --no-default-features` → all pass.

### Step 6: Docs — with_ssl and dead tuning advice

- `docs/pages/backends/kafka/examples/basic.mdx:42` and `:56`: replace
  `with_ssl` with `with_tls` (keep surrounding prose).
- `docs/pages/backends/kafka.mdx:322` (rebalance Gotcha bullet): replace the
  claim "Tune `session.timeout.ms` and `max.poll.interval.ms` in `KafkaConfig`"
  with accurate text: shove currently pins `session.timeout.ms` to 10s and
  `max.poll.interval.ms` to 5 minutes (constants in
  `src/backends/kafka/constants.rs`); slow handlers should raise the handler
  timeout via `with_handler_timeout` / `with_default_handler_timeout` instead.

**Verify**: `grep -rn "with_ssl" docs/pages/` → no matches.

## Test plan

Covered per step: paused-time reconnect-budget tests (step 2), `partition_lag`
unit tests (step 4), batch-publish count test (step 3). Full gates in Done
criteria. Model integration additions on the existing tests in
`tests/nats_integration.rs` (each test starts its own container via the file's
helper — copy the pattern of a nearby test).

## Done criteria

ALL must hold (run in the worktree):

- [ ] `cargo fmt -- --check` exits 0
- [ ] `cargo clippy -q --all-features --all-targets -- -D warnings` exits 0
- [ ] `cargo clippy -q --no-default-features -- -D warnings` exits 0
- [ ] `cargo nextest run --no-default-features` all pass
- [ ] `cargo nextest run --features kafka --test kafka_integration` all pass
- [ ] `cargo nextest run --features nats --test nats_integration` all pass
- [ ] `grep -rn "with_ssl" docs/pages/` → no matches
- [ ] `grep -n "CommitMode::Async" src/backends/kafka/consumer.rs` shows no
      match inside the shutdown branch (steady-state loop commit still Async)
- [ ] `git status` shows no modified files outside the in-scope list

## STOP conditions

- The code at any "Current state" location doesn't match the excerpt.
- Changing `get_queue_stats`'s signature turns out to require touching files
  other than `src/backends/kafka/autoscaler.rs` plus trait-impl call sites —
  list them and stop if any is out of scope.
- `#[must_use]` on a type triggers more than ~5 clippy violations (that would
  mean real API-use churn worth reviewing first).
- A step's verification fails twice after a reasonable fix attempt.
- Docker is unavailable for the integration suites.

## Maintenance notes

- Step 2's 60s threshold is a heuristic; if a backend gains an explicit
  "connection established" signal, prefer resetting on that.
- Step 4's trait change is breaking for external `KafkaQueueStatsProvider`
  impls — release notes should mention it.
- Plan 008 will modify `active_consumers()` used by `fetch_metrics`; these
  compose but will merge-conflict textually if reordered.
