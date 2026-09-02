# Design decision: batching and sequencing are mutually exclusive

**Status:** decided
**Issue:** CAF-63 (follow-up from CAF-22)
**Scope:** the batch consumption API (`BatchMessageHandler` / `run_batch`) introduced by CAF-22

## Decision

`run_batch` does **not** get a `SequencedTopic` variant. Batch consumption and
sequenced (FIFO) consumption are mutually exclusive, and that exclusion is
enforced in the type system rather than documented and hoped for.

This is a design decision, not deferred work. A per-key batch trait is sketched
in the last section so the door is not nailed shut, but it should not be built
without a caller.

**2026-09-02:** the batch-consumption entry point is now generic —
`Broker::batch_consumer()`, gated on `HasBatchConsumption` (see that trait's
own doc comment in `src/backend/capability.rs` for the current per-backend
list, which is the authoritative copy — not restated here) — rather than
Kafka-only `KafkaConsumer::run_batch`. The exclusion this document argues for
is unchanged and applies identically on the generic path: `NotSequenced` is
the compile-time bound, backed by the same runtime guard, on every backend.

## Why

### 1. A batch-wide `Outcome` cannot express the per-key poison set

`SequenceFailure::FailAll` is not just a label — it is implemented as a per-key
poison set. On `Reject`, the failing key is inserted into a `HashSet<String>`
(`src/backends/rabbitmq/consumer.rs:402`,
`route_reject_sequenced` at `src/backends/inmemory/consumer.rs:632`), and every
subsequent message for that key bypasses the handler and goes straight to the
DLQ.

An `Outcome` carries no key. So a batch-wide `Reject` on a batch spanning keys
`A..Z` has exactly two possible lowerings, and both are wrong:

- **poison every key in the batch** — fails keys whose messages succeeded, which
  is precisely the guarantee a sequenced topic exists to provide; or
- **poison no key** — silently drops the `FailAll` semantics the topic asked
  for, downgrading it to `Skip` without telling anyone.

There is no third option. This is the core argument: the batch API's defining
simplification (one `Outcome` per batch) is not merely awkward under sequencing,
it is *unrepresentable*.

### 2. Batch-wide `Retry` re-runs work that already committed

`Retry` lowers to a hold-queue republish, selected by
`hold_queues[min(retry_count, len - 1)]` for escalating backoff
(`src/consumer.rs:84-91`). Redelivering a batch redelivers *every* message in it.

On an unsequenced DB sink this is safe, and that safety is the entire premise of
the batch API: the flush is one transaction, so replaying it is idempotent. A
sequenced topic's handler is typically a stateful per-key reducer — applying a
prefix twice is not idempotent by default. The batch API would be exporting its
central safety assumption into a context that does not satisfy it.

### 3. Shard boundaries either break ordering or erase the throughput win

The two FIFO topologies in this crate fail differently, and neither is good:

- **Sharded backends** (InMemory, RabbitMQ, Redis, NATS): `run_fifo` shards by
  key into `routing_shards()` separate queues, one task per shard, each holding
  a single-active-consumer permit (`src/backends/inmemory/consumer.rs:350`,
  `:460`). A batch can only be assembled from within one shard's queue, so peak
  batch size is bounded by a single shard's arrival rate — with the default 8
  shards (`src/topology.rs:495`), roughly 1/8 of the intended win. Assembling
  across shards would require a cross-shard buffer, reintroducing exactly the
  ordering coupling that sharding exists to eliminate.
- **Kafka**: `routing_shards` is a no-op; ordering comes from partition
  assignment and there is a single consumer task
  (`src/backends/kafka/consumer.rs:1708-1710`). A batch here *can* legally span
  keys within the assigned partitions, so the throughput win is real.

That asymmetry is itself disqualifying. `shove`'s premise is one consistent API
across every backend. A trait whose throughput characteristics differ by nearly
an order of magnitude depending on the marker type is a portability trap: code
benchmarked on Kafka would quietly fall off a cliff when pointed at RabbitMQ.

### 4. Kafka's per-message commit is load-bearing

FIFO commits per message, and only once the message has been retired:

```rust
// src/backends/kafka/consumer.rs:2051-2055
if route_ok {
    consumer.commit_message(&msg, CommitMode::Async).ok();
}
```

That conditional is how at-least-once delivery survives a failed hold-queue
republish. Batch commit would widen the redelivery window from one message to a
whole batch and would need re-verification against the fenced-consumer /
reconnect path (commit `2f03ad9`). That is real risk taken on for no known
caller.

### 5. There is no caller

Confirmed in CAF-63: the source repo (clob-data-services) uses batch consumption
for DB sinks on non-sequenced topics. No consumer wants sequenced + batched.

## Precedent

This codebase already narrows the `Outcome` vocabulary under sequencing.
`Outcome::Defer` is unsupported on sequenced consumers and is silently
downgraded to `Retry` with a warning:

```rust
// src/backends/kafka/consumer.rs:444-452
fn adjust_outcome_for_fifo(outcome: Outcome) -> Outcome {
    match outcome {
        Outcome::Defer => {
            tracing::warn!("Defer is not supported on sequenced consumers — treating as Retry");
            Outcome::Retry
        }
        other => other,
    }
}
```

So "sequencing restricts what an outcome may mean" is established. Excluding
batch applies the same principle, enforced better — at compile time instead of
via a runtime log line nobody reads.

## How to enforce it — a warning for CAF-22

**Implementing `run_batch` only for `Topic` does not exclude sequenced topics.**

This is not hypothetical: as shipped on PR #60 (`feat/kafka-batch-consumer`),
`run_batch` is bound `where T: Topic` with no sequencing check
(`src/backends/kafka/consumer.rs:2737`), and the exclusion exists only as a
rustdoc bullet ("No FIFO/sequenced variant"). A `define_sequenced_topic!` type
satisfies that bound today.

`SequencedTopic: Topic` (`src/topic.rs`), so a sequenced topic *is* a `Topic`. A
`run_batch` bound on `T: Topic` accepts a `SequencedTopic` without complaint and
consumes the unsharded main queue — bypassing shard queues, the SAC permits, and
the poison set entirely, while the caller believes ordering is intact. This is
the worst available outcome and it is the default one. It must be closed
explicitly.

Two mechanisms, and CAF-22 should ship both:

**(a) Runtime guard — baseline.** Have `run_batch` return
`Err(ShoveError::Topology(..))` when `T::topology().sequencing().is_some()`.
This is the exact mirror of the guard `run_fifo` already carries
(`src/backends/kafka/consumer.rs:1729-1734`), which errors when a topic *lacks*
sequencing config. Cheap, and it catches any future internal caller path.

**(b) Compile-time capability gate — the real fix.** Add a marker trait
(`NotSequenced` or similar) implemented by `define_topic!` and deliberately not
by `define_sequenced_topic!` (`src/macros.rs:88`), and bound `run_batch` on it.
Attach a `#[diagnostic::on_unimplemented]` message pointing the user at
`run_fifo`, following `HasCoordinatedGroups` (`src/backend/capability.rs:20-23`).

Pin the property with a `compile_fail` doctest, exactly as `Sqs` pins the absence
of `consumer_group` (`src/markers.rs:24-38`). That doctest is what stops the
exclusion from silently regressing:

```rust
/// ```compile_fail
/// // define_sequenced_topic!(OrdersTopic, ...);
/// // error: `OrdersTopic` is sequenced; batch consumption is unavailable.
/// broker.consumer().run_batch::<OrdersTopic, _>(handler, ctx, opts);
/// ```
```

This is capability gating by marker trait, which is already the crate's idiom
for "this combination is not merely unsupported, it is incoherent."

## If a caller ever appears

Do not widen `BatchMessageHandler`. The correct shape is a separate trait, because
the return type differs:

```rust
pub trait KeyedBatchMessageHandler<T: SequencedTopic> {
    type Context: Clone + Send + Sync + 'static;

    /// Batches are assembled per shard, never across shards.
    /// Every key present in the input MUST appear in the output map;
    /// a missing key is an error, not an implicit Ack.
    fn handle_keyed_batch(
        &self,
        batch: BTreeMap<String, Vec<(T::Message, MessageMetadata)>>,
        ctx: &Self::Context,
    ) -> impl Future<Output = HashMap<String, Outcome>> + Send;
}
```

Requirements it would impose, all of which are real work:

1. Per-shard batch assembly, with batch size bounded by the shard — accept the
   reduced win rather than papering over it.
2. Per-key outcome routing: each key's `Outcome` lowered independently against
   that key's hold queue and poison-set entry.
3. Partial-batch commit on Kafka: commit up to the highest offset whose key
   acked, hold the rest. Non-trivial against the reconnect path.
4. A stated rule for keys that appear in the batch but not the outcome map —
   recommend hard error, since silent `Ack` loses messages.

Estimated cost is comparable to the original batch feature, for a caller that
does not exist. Revisit only when one does.

## Related finding (out of scope, filed separately — now fixed)

While grounding this decision: `SequenceFailure::FailAll` poisoning was
implemented only in the InMemory and RabbitMQ FIFO paths. The Kafka, Redis, and
NATS FIFO paths contained no poison set, so a topic configured `FailAll` behaved
as `Skip` on those three backends. Per `CLAUDE.md`, delivery-semantics gaps are
expected to be fixed across all backends. This is independent of the batch
question and was tracked on its own issue (CAF-84).

Resolved: `FailAll` is now honoured on all six backends, with the semantics
single-sourced in `PoisonedKeys` (`src/routing.rs`). See
[`sequence-failure-parity.md`](./sequence-failure-parity.md). Note that this
does **not** weaken argument 1 above — a batch-wide `Outcome` still cannot
express a per-key poison set, on any backend.
