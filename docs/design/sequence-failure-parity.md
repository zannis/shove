# Design decision: `SequenceFailure::FailAll` is implemented on every backend

**Status:** decided, implemented
**Issue:** CAF-84 (found during the CAF-61/CAF-63 audit, see
[`batch-and-sequencing.md`](./batch-and-sequencing.md) — "Related finding")
**Scope:** the `FailAll` poison-key path on all six backends

## The bug

`TopologyBuilder::sequenced(SequenceFailure::FailAll)` promises that a
permanent failure halts the whole sequence key. Only InMemory, RabbitMQ and SQS
implemented it. NATS, Kafka and Redis read `seq.routing_shards()` and never read
`seq.on_failure()`, so on those three backends a `FailAll` topology behaved
exactly like `Skip` — silently, with no warning at declare time or at runtime.

Ordering guarantees are the entire reason to use a sequenced topic. A silent
downgrade to `Skip` is the worst failure mode available: the caller believes
message 4 was withheld because message 3 failed, and it was not.

## Decision

**Implement `FailAll` on NATS, Kafka and Redis.** Not a declare-time rejection,
and not a type-system capability gate.

### Why not reject the topology at declare time (option 2)

It converts a silent wrong answer into a loud one, which is better — but it also
permanently removes the feature from three backends, including Kafka, the one
backend whose ordering story is strongest. `FailAll` is wanted precisely where
messages are causally dependent (ledger entries, state-machine transitions), and
those are the workloads most likely to be on Kafka. Rejecting is only the right
call when honouring the config is infeasible. It is not: see "Cost" below.

### Why not gate it in the type system (option 3)

The issue proposed this as the most consistent option, on the `HasCoordinatedGroups`
precedent. On inspection it does not fit.

`HasCoordinatedGroups` gates a **method on a backend marker** — `Broker<Sqs>` has
no `consumer_group()`, and that is decidable from `B` alone. `FailAll` is not a
method and it is not a property of `B`. It is a runtime field inside a
`&'static QueueTopology` built by a builder chain:

```rust
TopologyBuilder::new("orders").sequenced(SequenceFailure::FailAll).build()
```

The topic type produced by `define_sequenced_topic!` is backend-independent by
design — the same topic is meant to work against `Broker<Kafka>` and
`Broker<InMemory>`. To gate on `on_failure` the type system would have to see the
policy, which means either lifting it into the topic's type parameters (a
breaking change to the macro and to every sequenced topic declaration) or
splitting `SequencedTopic` into two traits. Either way the gate is on a
(topic × backend) *pair*, which `HasCoordinatedGroups` never has to express.

That is a large, breaking change to encode a restriction we have now decided not
to impose.

### Cost of implementing (why option 1 was cheap)

The poison set is a per-consumer `HashSet<String>`, checked before the handler
and inserted on DLQ-terminal events. Two of the three backends already carried
everything needed:

| Backend | Sequence key available on consume? |
|---|---|
| Kafka | yes — the Kafka message key *is* the sequence key (`resolve_topic_and_key`) |
| Redis | yes — the `x-sequence-key` stream field |
| NATS | **no** — only the shard was encoded, in the subject `{queue}.shard.{n}` |

So the only real work was NATS, which needed the key on the wire. See
"NATS wire change" below.

## Semantics (identical on all six backends)

Single-sourced in `PoisonedKeys` (`src/routing.rs`), alongside `decide_retry` —
the same "boundary logic lives in exactly one place" rule this crate already
applies to the retry budget.

1. **Inert under `Skip`.** `PoisonedKeys::new(SequenceFailure::Skip)` allocates
   nothing and takes no lock; `is_poisoned` is a constant `false`. The `Skip`
   path pays nothing for a feature it does not use.
2. **A key is poisoned by any DLQ-terminal event** for one of its messages:
   `Outcome::Reject`, an exhausted retry budget, or a pre-handler rejection
   (oversize payload, failed deserialization, and on Kafka a schema-registry
   rejection). This matches the reference implementations — RabbitMQ poisons on
   deserialization failure at `src/backends/rabbitmq/consumer.rs:774`, and
   InMemory routes the same failures through `route_reject_sequenced`.
3. **A poisoned key is dead-lettered without invoking the handler**, with death
   reason `"rejected"` — the same reason a direct `Outcome::Reject` produces, so
   the DLQ vocabulary does not grow a backend-specific value.
4. **Poisoning lasts for the lifetime of the consumer task**, as the rustdoc on
   `SequenceFailure::FailAll` already states. It survives a broker reconnect:
   NATS, Kafka and Redis rebuild their inner consume loop through a reconnect
   wrapper, and `PoisonedKeys` is cloned into each attempt rather than rebuilt,
   so a blip cannot un-poison a key.
5. **The empty key is never poisoned.** A message with no sequence key carries
   no ordering relationship, and poisoning `""` would dead-letter every other
   unkeyed message sharing the shard. This mirrors the `!key.is_empty()` guard
   InMemory already had (`src/backends/inmemory/consumer.rs:632`).

### Scope of the poison set

One set per **consumer task**, which means per shard on the sharded backends
(InMemory, RabbitMQ, Redis, NATS) and one for the whole assignment on Kafka.
Per-shard is complete, not partial: a sequence key always hashes to the same
shard, so a shard's set sees every message that key will ever produce on this
consumer.

The set is *not* shared between processes. Two consumers of the same topic each
keep their own. This is pre-existing RabbitMQ behaviour and is inherent to a
process-local set — sharing it would need a distributed store and a poison-key
lifecycle (who un-poisons, and when) that no caller has asked for. It is
documented on `SequenceFailure::FailAll` rather than hidden.

Consequences worth stating plainly:

- **Kafka**: a partition reassigned to a sibling consumer by a rebalance arrives
  with that consumer's own (empty) set. Ordering is preserved by the partition
  assignment; the poison record is not carried across.
- **Redis/NATS consumer groups**: several tasks may read the same shard, and
  only the one that observed the failure holds the poison.

## NATS wire change

`Shove-Sequence-Key` is now set by `NatsPublisher::build_headers` on sequenced
topics. The subject already identified the shard, but a shard holds many keys,
so poisoning without the key would have taken out the whole shard — strictly
worse than the `Skip` behaviour it replaced.

The header is written **after** user-supplied headers so a caller cannot
overwrite it. (`publisher_internal::validate_headers` reserves the `x-`-prefixed
internal names shared with the other backends, but not the `Shove-` names this
backend uses.)

Backwards compatibility: a message published by an older version carries no
key, `get_sequence_key` returns `""`, and rule 5 applies — it is never poisoned.
Such a message degrades to `Skip` behaviour rather than mis-poisoning. Messages
already in flight during an upgrade are therefore safe.

## Per-backend behaviour table

| Backend | `FailAll` honoured | Key source | Poison scope |
|---|---|---|---|
| InMemory | yes | `x-sequence-key` header | per shard task¹ |
| RabbitMQ | yes | delivery routing key | per shard consumer |
| SQS | yes | message group ID | per consumer |
| NATS | yes (new) | `Shove-Sequence-Key` header | per shard task |
| Kafka | yes (new) | Kafka message key | per consumer task² |
| Redis | yes (new) | `x-sequence-key` stream field | per shard task |

¹ InMemory additionally clears its poison set whenever the shard buffer drains
(`src/backends/inmemory/consumer.rs:593`). That is a deliberate bounded-drain
heuristic, but it is narrower than the documented "lifetime of the consumer"
contract, and it is only expressible because the queue is in-process. Left as
is — changing it is a behaviour change to a tested backend and is out of scope
for this issue — but it is the one remaining deviation in the table.

² Covers every partition currently assigned; see the rebalance note above.

## Testing

- `PoisonedKeys` semantics (inert under `Skip`, per-key isolation, empty-key
  guard, shared-across-clones) are unit-tested in `src/routing.rs` — no broker
  required, so the rules cannot regress silently on a backend whose integration
  suite needs Docker.
- End-to-end coverage for each newly-fixed backend, in the existing Docker
  integration suites (CI-only — Docker is not available in the dev sandbox):
  - `tests/nats_integration.rs::sequenced_failall_poisons_same_key_after_reject`
  - `tests/kafka_integration.rs::sequenced_failall_poisons_same_key_after_reject`
  - `tests/redis_integration.rs::fifo_failall_poisons_same_key_after_reject`

  All three assert the same three properties, so a divergence shows up as one
  backend's test failing rather than as a silent behaviour difference: the
  failing key's later messages never reach the handler, exactly three messages
  land in the DLQ (the rejected one plus the two poisoned ones), and a second
  key on the same topic is handled normally throughout.
