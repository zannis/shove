# Plan 007: Make Kafka offset tracking rebalance-safe

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. Your reviewer maintains `plans/README.md`; do
> not update it.
>
> **Drift check (run first)**:
> `git diff --stat dacfe5c..HEAD -- src/backends/kafka/consumer.rs src/backends/kafka/msk_iam.rs`
> Expected on the advisor branch: only plan-006 commits touching
> `src/backends/kafka/consumer.rs` (shutdown-commit + reconnect-reset). The
> excerpts below predate those, but none of them overlap plan 006's edits.
> Any other drift: STOP.

## Status

- **Priority**: P1
- **Effort**: M–L
- **Risk**: MED (touches commit timing; mitigated by a new rebalance integration test)
- **Depends on**: 006 (same branch; no logical dependency)
- **Category**: bug
- **Planned at**: commit `dacfe5c` (main), 2026-07-02

## Why this matters

The Kafka consumer uses manual commits (`enable.auto.commit=false`) with a
per-partition `OffsetTracker`, but installs **no rebalance callback** and never
resets tracker state when partitions move. Two concrete failures:

1. **Commit stall**: partition P is revoked from consumer A (e.g. consumer B
   joins — the autoscaler makes this routine), B processes and commits on P,
   then P is reassigned to A. A's stale `PartitionTracker.next_to_commit` is
   still the old low offset; `drain_committable` only advances on a contiguous
   run from that stale value, which will never arrive. **P stops committing for
   the life of the connection**, and any restart redelivers everything since
   the stale offset.
2. **Stale-partition commits**: completions for messages in flight when P was
   revoked still enqueue offsets, and the loop commits them even though this
   member no longer owns P.

Fix: install a `ConsumerContext` whose `pre_rebalance` forwards
assigned/revoked partition lists to the receive loop over a channel; the loop
drops tracker entries for those partitions, so a reassigned partition re-seeds
`next_to_commit` from the first actually-delivered offset.

## Current state

All in `src/backends/kafka/consumer.rs` unless noted.

- Tracker (lines ~52-130): `PartitionTracker { next_to_commit, completed: BTreeSet<i64> }`;
  `OffsetTracker::track_received` does
  `self.partitions.entry(partition).or_insert_with(|| PartitionTracker::new(offset))`
  — insert-once, never reset. `OffsetTracker::mark_complete` no-ops if the
  partition has no tracker (safe for late completions after removal).
- Consumer construction (lines ~714-755): `create_stream_consumer` builds a
  `StreamConsumer` (default context) or `StreamConsumer<MskIamContext>`
  (feature `kafka-msk-iam`), with `partition.assignment.strategy =
  cooperative-sticky` and `enable.auto.commit = false`. No
  `ConsumerContext` methods are overridden anywhere:
  `src/backends/kafka/msk_iam.rs:76` is `impl ConsumerContext for MskIamContext {}`.
- The consumer enum (search `enum KafkaStreamConsumer`) wraps the two variants
  and delegates `recv/commit/commit_message/subscribe/...`.
- Receive loop (lines ~901-1000, inside `run_with_reconnect`): owns
  `let mut tracker = OffsetTracker::new(...)` (created once per (re)connect);
  drains a `completion_rx: mpsc::Receiver<(i32, i64)>` and commits
  `tracker.drain_committable()` each iteration; `tokio::select!` over shutdown /
  completions / `consumer.recv()`.
- rdkafka 0.39 API you will use: `trait ConsumerContext` with
  `fn pre_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance)`
  (or the `post_rebalance` sibling), and
  `enum Rebalance<'a> { Assign(&'a TopicPartitionList), Revoke(&'a TopicPartitionList), Error(..) }`.
  Check the exact signatures in the vendored crate
  (`cargo doc -p rdkafka --no-deps` or the source in `~/.cargo/registry`) —
  match what compiles, and note the version's exact shape in NOTES.
  With cooperative-sticky, Assign/Revoke lists are **incremental deltas**, which
  is exactly what the tracker-removal logic wants.
- Conventions: Conventional Commits, no `Co-Authored-By`; clippy `-D warnings`
  on `--all-features` and `--no-default-features`;
  `[lints.clippy] absolute-paths = "deny"` (use `use` imports); tests via
  `cargo nextest run` (never `-q` to nextest). Integration tests start their own
  Kafka testcontainer — copy the setup pattern from `tests/kafka_integration.rs`
  (see its top-of-file helpers and any small consumer-group test, e.g.
  `consumer_group_processes_messages`).

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Lint (all) | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Lint (min) | `cargo clippy -q --no-default-features -- -D warnings` | exit 0 |
| MSK-IAM compile | `cargo clippy -q --features kafka-msk-iam --all-targets -- -D warnings` | exit 0 |
| Kafka unit | `cargo nextest run --features kafka --lib` | all pass |
| Kafka integration (Docker) | `cargo nextest run --features kafka --test kafka_integration` | all pass |
| New test (Docker) | `cargo nextest run --features kafka --test kafka_rebalance` | all pass |

## Scope

**In scope**:
- `src/backends/kafka/consumer.rs`
- `src/backends/kafka/msk_iam.rs` (only if the context wrapper needs a trait
  bound adjusted there)
- `tests/kafka_rebalance.rs` (create)

**Out of scope**:
- `src/backends/kafka/consumer_group.rs`, `autoscaler.rs`, `client.rs`
  (the producer/admin clients don't rebalance).
- Any change to commit *cadence* or `route_outcome`/retry semantics.
- The FIFO path (`spawn_fifo_shards`): FIFO consumers use per-message
  `commit_message` and a dedicated group; leave untouched.
- Other backends.

## Git workflow

- Continue on branch `advisor/kafka-nats-prod-fixes` (created by plan 006).
- Commits: `fix(kafka): reset offset tracking on partition rebalance` and
  `test(kafka): rebalance integration coverage` (or one combined commit).
  No `Co-Authored-By`.

## Steps

### Step 1: Rebalance-event plumbing

In `src/backends/kafka/consumer.rs` add:

```rust
/// Partition-assignment change forwarded from librdkafka's rebalance callback
/// to the receive loop that owns the OffsetTracker.
enum RebalanceEvent {
    Assign(Vec<i32>),
    Revoke(Vec<i32>),
}
```

and a context wrapper generic over the inner client context:

```rust
struct RebalanceContext<C: ClientContext> {
    inner: C,
    topic: String,
    tx: std::sync::mpsc::Sender<RebalanceEvent>, // std channel: callback is sync, loop drains non-blocking
}
```

Implement `ClientContext` for `RebalanceContext<C>` by **delegating every
method the inner context relies on**. For the MSK-IAM build this MUST include
`generate_oauth_token` and `ENABLE_REFRESH_OAUTH_TOKEN` (check
`src/backends/kafka/msk_iam.rs` for exactly what `MskIamContext` implements
and forward each overridden item). Getting this wrong breaks MSK auth
silently — treat the delegation list as load-bearing and enumerate it in NOTES.

Implement `ConsumerContext` for `RebalanceContext<C>`: in `pre_rebalance`
(or `post_rebalance` for Assign if that ordering is cleaner — decide by what
guarantees the TPL contents; document the choice), extract partitions for
`self.topic` from the `Rebalance::Assign`/`Revoke` TPL and send the event;
ignore send errors (loop gone = shutdown).

### Step 2: Wire the wrapper into consumer construction

Change `create_stream_consumer` to build
`StreamConsumer<RebalanceContext<DefaultClientContext>>` /
`StreamConsumer<RebalanceContext<MskIamContext>>` and accept the
`std::sync::mpsc::Sender<RebalanceEvent>` + topic name as parameters. Update
the `KafkaStreamConsumer` enum variants and its delegation impl accordingly.
Callers create the channel right before `create_stream_consumer` (inside the
`run_with_reconnect` closure, so each (re)connect gets a fresh channel —
matching the fresh `OffsetTracker`).

The DLQ/base consumers created via the same helper get the same wrapper; a
channel nobody drains is fine (bounded memory: rebalances are rare — use an
unbounded std channel and note it, or drain defensively; prefer draining in
every loop that owns a tracker). If `create_stream_consumer` has callers that
have no tracker/loop (search them all: `grep -n "create_stream_consumer" src/backends/kafka/`),
pass a sender whose receiver is dropped and add a one-line comment.

### Step 3: Drain rebalance events in the receive loop

In the concurrent receive loop (~line 942), at the top of each iteration
(next to the `completion_rx.try_recv()` drain), drain the rebalance receiver:

```rust
while let Ok(event) = rebalance_rx.try_recv() {
    match event {
        RebalanceEvent::Assign(parts) | RebalanceEvent::Revoke(parts) => {
            for p in parts { tracker.remove(p); }
        }
    }
}
```

Add `OffsetTracker::remove(&mut self, partition: i32)`. Removing on **Revoke**
prevents commits for unowned partitions; removing on **Assign** re-seeds
`next_to_commit` from the first delivered offset after reassignment (the
actual stall fix — the broker's committed offset determines where delivery
resumes, so seeding from delivery is correct).

Also guard `PartitionTracker::mark_complete` against late completions below
the seed: ignore `offset < next_to_commit` (prevents unbounded stale entries
in `completed` after a reassignment).

### Step 4: Unit tests for the tracker

In the file's `#[cfg(test)]` module (pure logic, no broker):
- remove-then-track re-seeds `next_to_commit` at the new offset;
- completions arriving after `remove` are dropped (no commit produced);
- `mark_complete` below `next_to_commit` is ignored;
- normal contiguous drain still works (regression).

**Verify**: `cargo nextest run --features kafka --lib` → all pass.

### Step 5: Rebalance integration test

Create `tests/kafka_rebalance.rs` (testcontainer harness copied from
`tests/kafka_integration.rs`). Scenario (generous timeouts, poll-until —
model timing style on `committed_offsets_advance_while_consumer_is_idle` in
`tests/kafka_integration.rs`):

1. Topic with ≥4 partitions; consumer group with `min=1`.
2. Consumer A runs; publish batch 1 across all partitions (distinct keys);
   wait until processed and committed (poll committed offsets == high
   watermark per partition).
3. Scale up to add consumer B (use the group's scale-up path or a second
   registered member — whatever `tests/kafka_integration.rs` already does for
   multi-consumer groups); publish batch 2; wait until processed.
4. Scale B down (its partitions return to A). Publish batch 3.
5. Assert: every batch-3 message is processed AND committed offsets on **all**
   partitions converge to the high watermark within the timeout.

Without the fix, step 5 hangs on the partitions that B committed on (stale
tracker in A) — run the test against the pre-fix code once to confirm it
fails, and say so in NOTES (this is the point of the test).

**Verify**: `cargo nextest run --features kafka --test kafka_rebalance` → pass.

## Done criteria

- [ ] All three clippy commands exit 0 (`--all-features`,
      `--no-default-features`, `--features kafka-msk-iam`)
- [ ] `cargo fmt -- --check` exits 0
- [ ] `cargo nextest run --features kafka --lib` all pass (incl. 4+ new tracker tests)
- [ ] `cargo nextest run --features kafka --test kafka_integration` all pass (no regression)
- [ ] `cargo nextest run --features kafka --test kafka_rebalance` passes
- [ ] NOTES records: the delegated ClientContext method list, the
      pre/post_rebalance choice and why, and confirmation the new test fails
      on pre-fix code
- [ ] No files outside scope modified

## STOP conditions

- rdkafka 0.39's `ConsumerContext`/`Rebalance` API doesn't match the shape
  above in a way that changes the design (e.g. no per-topic TPL access, or
  callbacks unavailable on `StreamConsumer`).
- Delegating `MskIamContext`'s `ClientContext` impl requires changes beyond
  `msk_iam.rs` trait bounds.
- The integration test cannot be made to pass reliably in 3 attempts
  (rebalance timing) — report what you observed instead of loosening asserts.
- Any "Current state" excerpt mismatch beyond plan-006's documented edits.

## Maintenance notes

- Reviewer scrutiny: the ClientContext delegation for MSK IAM (auth breaks
  silently if a method is missed) and that tracker removal happens for BOTH
  Assign and Revoke.
- Future: if eager (non-cooperative) assignment is ever made configurable,
  Assign events become full sets, and removal-on-assign must clear all
  partitions not in the set.
- Deferred: committing best-effort *inside* pre_rebalance before revoke
  (reduces redeliveries further but adds sync-commit-in-callback complexity);
  the at-least-once contract makes plain removal correct without it.
