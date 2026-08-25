# Plan: extend configurable Kafka `group.id` to DLQ + FIFO, add fan-out test, release 0.11.9

## Context correction (read first)

The original handoff (`/tmp/shove-configurable-group-id-handoff.md`) assumed `shove` is an
external crate where this change still needs to be made from scratch. That premise is stale.

This repo (`github.com/zannis/shove`) is the canonical source, and the core feature **already
ships in v0.11.8** — the exact version downstream pulls. Verified at the `v0.11.8` tag (HEAD):

- `KafkaConsumerGroupConfig::with_group_id(...)` / `group_id()` / `resolved_group_id()` — `consumer_group.rs:170,177,188`
- Override threaded to the **main consumer** (`consumer_group.rs:652` → `consumer.rs:887`)
- **Autoscaler** reads the same stored `group_id`, no phantom-backlog drift (`autoscaler.rs:259`); committed as breaking fix `b989eac`
- Unit + autoscaler regression tests already present

So the DENG-293 forcing case (two independent main consumers on `subscriptions.price-changes`)
is **already solved** by calling `.with_group_id("...")` against `shove = "0.11.8"`. No change is
required for that path.

This plan covers the two genuine gaps the user opted to close (scope: "Close DLQ/FIFO gap + test +
release"), plus the missing regression test and a release.

## Semantics (the contract this plan implements)

Let `G` be the value passed to `with_group_id(G)` (registry) or the new `ConsumerOptions` builder
(direct path). When `G` is set the three broker-side groups become:

| Consumer | override set | override unset (unchanged) |
|---|---|---|
| standard | `G` | `{queue}-consumer` |
| FIFO | `{G}-fifo` | `{queue}-fifo` |
| DLQ | `{G}-dlq` | `{dlq}-consumer` |

Backward compatibility is mandatory: with no override every derived id is byte-for-byte what
0.11.8 produces today, so existing deployments do not silently reset offsets.

## Task 1 — FIFO honors the override (registry path)

Single-source-of-truth approach to avoid re-introducing the arch-K-1 footgun (autoscaler and
consumer deriving the group id independently and disagreeing).

1. Add to `KafkaConsumerGroupConfig` (`consumer_group.rs`):
   ```rust
   pub(crate) fn resolved_fifo_group_id(&self, queue: &str) -> String {
       match self.group_id.as_deref() {
           Some(base) => format!("{base}-fifo"),
           None => super::constants::consumer_group_id_fifo(queue),
       }
   }
   ```
2. `new_fifo` (`consumer_group.rs:438`): store `config.resolved_fifo_group_id(&queue_str)` in
   place of `super::constants::consumer_group_id_fifo(&queue_str)`. This is what the autoscaler
   queries.
3. `spawn_one` (`consumer_group.rs:652`): set `options.kafka_group_id = Some(self.group_id...)`
   (the group's already-resolved id) instead of only forwarding the raw override. Because
   `self.group_id` is resolved correctly per group type at construction, the spawned consumer
   joins exactly the group the autoscaler tracks. For the standard path this turns `None` into
   `Some("{queue}-consumer")`, an identical value, so behavior is unchanged.
4. `spawn_fifo_shards` (`consumer.rs:1375`): replace `let group_id = format!("{queue}-fifo")`
   with use of `options.kafka_group_id`, falling back to `consumer_group_id_fifo(&queue)` when
   `None` (preserves the direct `run_fifo` path that does not set it).

After this, both the FIFO consumer's joined group and the autoscaler's stored group resolve to the
same value by construction.

## Task 2 — DLQ honors the override (one additive builder)

The registry never spawns DLQ consumers; DLQ draining is the app-initiated `Consumer::run_dlq`
API. The public `run_dlq(handler, ctx)` takes no options and hardcodes `ConsumerOptions::new()`
(`kafka_group_id = None`), so the registry's `with_group_id` has no channel to reach the DLQ.
Decision: add **one** additive (non-breaking) builder so the override has a channel.

1. Add a builder on the Kafka-specific block `impl ConsumerOptions<Kafka>` (`consumer.rs:498`,
   alongside `with_schema_registry`):
   ```rust
   /// Override the base Kafka consumer `group.id` for this consumer. The
   /// standard consumer joins this value verbatim; a DLQ drain joins
   /// `{group}-dlq`; a FIFO consumer joins `{group}-fifo`. `None` (the
   /// default) keeps the topic-derived ids (`{queue}-consumer`,
   /// `{dlq}-consumer`, `{queue}-fifo`).
   pub fn with_group_id(mut self, group_id: impl Into<Arc<str>>) -> Self {
       self.inner.kafka_group_id = Some(group_id.into());
       self
   }
   ```
   (Confirm the wrapper field path — `self.inner.kafka_group_id` — against the `ConsumerOptions`
   struct before writing.)
2. `run_dlq_with_inner` (`consumer.rs:1767`): derive
   ```rust
   let dlq_group_id = match options.kafka_group_id.as_deref() {
       Some(base) => format!("{base}-dlq"),
       None => super::constants::dlq_consumer_group_id(dlq),
   };
   ```
3. Free bonus, backward compatible: `run_with_inner` (direct `Consumer::run` path) already reads
   `options.kafka_group_id` verbatim, so this builder also gives the non-registry standard and
   FIFO direct paths a group-id override with no extra wiring. With no override every id is the
   0.11.8 default.

## Task 3 — tests (TDD: write first)

Unit (`consumer_group.rs` tests + `consumer.rs`/options tests):
- `resolved_fifo_group_id`: override → `{G}-fifo`; none → `{queue}-fifo`.
- DLQ derivation: override → `{G}-dlq`; none → `{dlq}-consumer`.
- `ConsumerOptions::with_kafka_group_id` stores the value.
- FIFO autoscaler agreement: extend the existing
  `fetch_metrics_uses_fifo_group_id_for_fifo_groups` so a group built with an override resolves
  the stored group to `{G}-fifo`.

Integration (`tests/kafka_integration.rs`, Redpanda testcontainer, mirror existing setup) — the
property DENG-293 depends on:
- Register two consumer groups on the **same topic** with **different** `with_group_id`, produce
  N messages, assert **both** groups receive **all N** (independent consumption / fan-out).
- Optional: FIFO override test (two sequenced consumers, distinct `{G}-fifo` groups, both receive
  all N) and a DLQ override drain test.

Run via `dotenvx run -- cargo nextest run` (testcontainers / real broker). Do not pass `-q` to
nextest. Expect the redis_ping/autoscaler connect flake noted in memory — a 0.2s setup-connect
failure is infra race, not regression.

Also run the full existing suite to prove the default path is unchanged.

## Task 3b — broker hub ergonomics (follow-up request)

Surface the Kafka topology/replication knobs on the `Broker` hub instead of only
on the low-level declarer/registry:

- `TopologyDeclarer<Kafka>` (returned by `broker.topology()`) gains
  `with_replication_factor(n)` and `with_min_partitions(n)`, delegating to the
  inner `KafkaTopologyDeclarer`. So
  `broker.topology().with_replication_factor(3).with_min_partitions(8).declare::<T>()`.
- `ConsumerGroup<Kafka, Ctx>` (returned by `broker.consumer_group()`) gains
  `with_default_replication_factor(n)`, delegating to the inner
  `KafkaConsumerGroupRegistry`, mirroring the existing `with_default_handler_timeout`.
  So `broker.consumer_group().with_default_replication_factor(3).register::<T,H>(..)`.

Both are thin delegations to already-tested logic. Covered by two end-to-end
smoke tests in `kafka_integration.rs` (RF=1 on the single-broker container).

## Task 4 — release 0.11.9

- Additive public API (new builders, no signature changes) → minor bump within 0.x: `0.11.8`
  → `0.11.9`. Update `version` in `Cargo.toml`.
- Commit `release: v0.11.9` (the repo's release convention; no CHANGELOG.md, no spec/plan docs
  committed, no `Co-Authored-By` trailer).
- Tag + publish per the existing release flow (confirm how prior releases were cut before pushing).

## Task 5 — downstream (out of scope for this repo, noted for handoff)

1. Bump `shove` to `0.11.9` in `clob-data-services` and the DENG-293 sink.
2. DENG-293 sink calls `.with_group_id("subscriptions.price-changes.market-prices-latest-consumer")`
   (or chosen id) so it does not collide with `subscriptions.price-changes-consumer`.
3. Existing consumers that set no override are unaffected.

## Sequencing / review

1. Task 1 (FIFO) and Task 2 (DLQ) are independent — either order, or parallel.
2. Write unit tests before each (TDD). Integration test after both land.
3. Run full suite; confirm default-path bytes unchanged.
4. `superpowers:requesting-code-review` before release — shared crate consumed by multiple
   services.
5. Release last.
