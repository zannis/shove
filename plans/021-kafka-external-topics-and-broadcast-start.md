# Plan 021: Kafka consumers of externally owned topics, broadcast start position, and a configurable commit interval

> **Executor instructions**: Follow this plan step by step.
> Run every verification command before moving on.
> On any STOP condition, stop and report.
> Your reviewer maintains `plans/README.md`, so do not update it.
>
> **Drift check (run first)**:
> `git diff --stat 5bc4979..HEAD -- Cargo.toml src/consumer.rs src/batch_consumer.rs src/backend/options_inner.rs src/backend/batch_consumer.rs src/backend/mod.rs src/metadata.rs src/metrics.rs src/handler.rs src/audit.rs src/topology.rs src/broker.rs src/broadcast.rs src/backend/broadcast.rs src/backend/capability.rs src/consumer_group.rs src/backends/kafka/ src/schema_registry/decode.rs src/schema_registry/wire.rs docs/pages/backends/kafka.mdx docs/pages/concepts/broadcast.mdx docs/pages/concepts/handlers.mdx`
> You branch from `5bc4979`, so this should report no changes.
> If it reports changes, compare every "Current state" excerpt against the live code, and STOP on a mismatch.

## Status

- **Priority**: P1
- **Effort**: L, as eleven steps sized S, S, M, M, M, M, S, S, S, L, M
- **Risk**: LOW for steps 1, 2 and 5 to 9, which are one-token or additive with unchanged defaults.
  MED for steps 3 and 4, which change the commit position and the shutdown commit on the default path.
  HIGH for steps 10 and 11, which change what an outcome does on a Kafka consumer and on a NATS consumer of an external stream.
- **Depends on**: none, plan 011 is not required
- **Category**: bug + feature + dx
- **Planned at**: commit `5bc4979` (main), 2026-09-17
- **Maintainer decision**: reviewed on the three pull requests below on 2026-09-22, and answered the four open questions the same evening.
  The review asked for backend-neutral shapes in four places, and steps 5, 6, 7 and 10 now carry them.
  They are `BroadcastStart` on the broadcast options, `external()` on the topology builder, per-field availability on `MessageMetadata`, and an explicit retry strategy on the consumer options.
  The answers moved step 6 next to step 10, so external ownership and in-place retry land together.
  The second pull request therefore ships on its own.
  They bound NATS to the same invariant and stated the SQS ownership row.
  They also asked for the producer's `allow.auto.create.topics` as a pull request of its own in the same release.
  The maintainer proposed to merge the stack and ship it as the next minor release.
  The plan 021 row of `plans/README.md` is the maintainer's to add, as the executor instructions above say.
- **Delivery**: four pull requests by risk class, one minor release after the fourth.
  - https://github.com/zannis/shove/pull/210 delivers steps 1 and 2.
    Step 1 maps `KafkaAutoOffsetReset::None` to the `error` token and classifies librdkafka's `AutoOffsetReset` error as permanent in `map_kafka_error`.
    Step 2 splits Cyrus SASL out of `kafka-ssl` into `kafka-gssapi`.
  - https://github.com/zannis/shove/pull/211 delivers steps 3, 4, 5, 7, 8 and 9, and ships on its own.
    Step 3 commits past undelivered offsets.
    Step 4 makes the commit interval configurable and bounds the shutdown commit.
    Step 5 adds the broadcast start position.
    Step 7 exposes partition, offset and timestamp on deliveries.
    Step 8 exposes `auto.offset.reset` on the direct consumer options, and step 9 adds the protobuf message-index check.
    The DLQ-name guard in `TopologyBuilder::build` landed in the same pull request as a fix found on the way.
  - https://github.com/zannis/shove/pull/212 delivers steps 6, 10 and 11.
    Step 6 adds the external topology binding.
    Step 10 retries and defers in place instead of republishing into the topology, on Kafka and on an external NATS stream.
    Step 11 waits through a Schema Registry outage instead of discarding the record.
  - A fourth pull request, from `feat/kafka-producer-no-auto-create`, pins `allow.auto.create.topics=false` on the Kafka producer.
    A publish then fails on a topic nobody declared instead of creating it, a breaking change of its own in the same minor release.

## Why this matters

shove's Kafka consumer assumes shove owns the topic.
A service that reads a topic another team provisions, and locks down with read-only ACLs, hits six sharp edges.
`ConsumerGroup::register` declares the topology.
Under a read-only ACL the registration fails, and under a write ACL shove expands partitions on a topic it does not own.
`Outcome::Retry` and `Outcome::Defer` produce a copy of the record back into the consumed topic, which needs Write and duplicates the record for every other reader.
A Schema Registry outage discards every framed record it cannot resolve, because a transport error is treated like an unknown schema id.
A broadcast subscriber can only start at the tail, so a fresh instance cannot replay retained history or seek to a point in time.
Handlers never learn the partition, the offset or the broker timestamp of a delivery.
A dedup window therefore cannot anchor to append time, and no time-lag gauge is possible.
The direct consumer paths pin `auto.offset.reset` to `earliest` with no knob.
Four more edges are independent of topic ownership.
The offset tracker only advances through consecutive integers, so a compacted topic or a transactional producer's control records stall the commit position.
`KafkaAutoOffsetReset::None` sends librdkafka the token `none`, which it rejects, so that variant has never worked.
The protobuf frame parser ignores the message index, so a codec can silently decode the wrong message type from a multi-message schema.
Every authenticated cluster pays for cyrus SASL because `kafka-ssl` enables `rdkafka/sasl`, although librdkafka implements SCRAM and OAUTHBEARER natively and shove exposes no GSSAPI mechanism.
Finally, the offset commit cadence is a fixed 500 ms, and the final synchronous commit at shutdown runs on a runtime task with no deadline.

## Current state

All line numbers are at `5bc4979`.

- `src/backends/kafka/consumer_group.rs:77-83` `as_rdkafka_str` maps `None` to `"none"`.
  The unit test at `:1623` asserts that string.
- `Cargo.toml:96-99` `kafka-ssl = ["kafka", "rdkafka?/ssl", "rdkafka?/sasl"]`.
  `:104-109` `kafka-msk-iam` implies `kafka-ssl`.
  `:217-223` enables `cmake-build` on Windows only.
  `src/backends/kafka/client.rs:50-51`, `:96-128`, `:258-266`: `KafkaTls`, `KafkaSasl` and the `KafkaConfig` fields behind `kafka-ssl`.
  `:436-506`: the TLS and SASL wiring in `connect`.
- `rdkafka-sys-4.10.0+2.12.1/Cargo.toml`: `sasl = ["gssapi"]`, `gssapi = ["ssl", "sasl2-sys"]`.
  Its `build.rs:107` selects the `configure` build unless `cmake-build` is set, and `librdkafka/configure.self:143-148` turns on SCRAM and OAUTHBEARER from SSL.
  Its `build.rs:271-275` does the same for cmake, and `:280-282` turns on cyrus `WITH_SASL` from `gssapi` only.
- `.github/workflows/ci.yml:53`, `:86`, `:130`, `:314`: `libsasl2-dev` installed in `check`, `feature-lint`, `msrv` and the Kafka coverage entries.
  `:58-73`: the `check` job's commands.
  `:110-121`: the single-backend lint sets.
  `:273` and `:286`: the two Kafka coverage sets.
- `src/backends/kafka/consumer.rs:170-335` `PartitionTracker`: `next_to_commit`, a `completed` set, the dirty streak and the pending discards.
  `:224-238` `mark_complete` ignores an offset below `next_to_commit`.
  `:287-313` `drain_committable` advances through consecutive integers only.
  `:331` `has_committable`.
  `:349-353` `track_received` seeds a partition from the first offset it sees and records nothing afterwards.
  `:355-372` `OffsetTracker::mark_complete`, `:385-387` `remove`, `:401` `has_committable`, `:410-450` `apply_rebalance_events` with `remove` at `:413-416`, `:479-491` `drain_committable`.
  `:5142` the unit test `contiguous_drain_advances_past_gaps_only_when_filled` pins the consecutive-integer rule.
- `:115` `ASYNC_COMMIT_INTERVAL` 500 ms, `:123-160` `AsyncCommitGate`, `:3028` the gate is built from the constant.
  `:3073-3079` gated `CommitMode::Async` commit, `:3086` `Sync` when a batch retires discards.
  `:3107-3129` the shutdown branch waits for every permit at `:3109`, then commits `Sync` on the runtime task with no deadline.
  `:1633-1652` `commit_callback` re-offers a failed asynchronous commit.
- `:79-95` `QUIET_DRAINS_TO_RESOLVE` = 2 with its timing argument, `:1940-1946` `COMMIT_FENCE_TIMEOUT` 60 s, `:3041` the fence check.
- `:2066-2085` `commit_batch_end`: a `Sync` commit on `spawn_blocking`.
  Tokio cannot abort a started blocking task, and runtime shutdown waits for it, so this pattern bounds nothing when the broker is frozen.
- `:1749-1790` `assign_all_partitions_at_end`: every partition at `Offset::End` (`:1783`).
  `:1802-1848` `refresh_broadcast_partitions`: new partitions at `Offset::End` (`:1829`).
- `:4486-4600` `run_broadcast_with_inner`: fixed inert group id from `constants::broadcast_group_id` (`:4504-4505`), `Latest` reset passed as inert (`:4565-4568`), assign at `:4587`.
  Any `recv` error returns `Err` and triggers a reconnect (`:4684-4688`).
  The permit is acquired after a record arrived (`:4793-4797`), and a deferring task queues itself before releasing its permit (`:1266-1300`, `:4824-4838`).
- `src/broker.rs:280-295` `reset_consumer_group_offsets`: refuses a broadcast topology with "always starts at the tail" (`:286-293`).
- `src/backends/kafka/offset_reset.rs:46-60` `KafkaOffsetReset` with `Earliest`, `Latest`, `Timestamp(i64)`.
  Private helpers `target_from_watermarks` (`:186`) and `target_from_timestamp_lookup` (`:208-225`).
  `run_reset` fetches watermarks (`:380`) before the timestamp lookup (`:401`).
- `src/broadcast.rs:168-211` `BroadcastSubscriber::subscribe`: `into_inner` at `:192`, pins `max_retries = 0` (`:199`) and `prefetch_count = 1` (`:206`).
  Its doc says deliver-new is the contract (`:142-144`).
- `src/backend/broadcast.rs:22-58` `BroadcastImpl` contract, "Deliver-new" at `:28-30`.
  `src/backend/capability.rs:75` lists Kafka as "groupless `assign()` at the latest offset".
- `src/backends/kafka/consumer_group.rs:926-931` and `:993-997`: `register` and `register_fifo` declare unconditionally.
  `:784-807` `spawn_one` copies `group_id`, `auto_offset_reset` and the registry settings into the options.
  `:538-548` `start` spawns `min_consumers`, and `:1013-1017` `start_all` calls it for every group.
- `src/backends/kafka/topology.rs:175-191` `declare_standard`, `:193-221` `declare_sequenced`, `:224-246` `declare`.
  `src/backends/kafka/client.rs:739-801` `create_topic`, with `ensure_partitions` (`:807`) and `ensure_topic_configs` (`:877`) on `TopicAlreadyExists` (`:786-789`).
  `:1000-1013` builds a metadata consumer with a `group.id`.
- `src/topology.rs:291-294` and `:632-648`: `nats_external_stream` field and builder.
  `:923-935`: the `build()` guards that refuse it with `nats_stream_config`, `nats_subjects` and `sequenced`.
  `:685-744`: the six Kafka topic-config methods, `with_topic_config`, `with_retention`, `with_retention_forever`, `with_retention_bytes`, `with_cleanup_policy`, `with_max_message_bytes`.
  `src/backends/nats/topology.rs:116-131`: the declarer verifies the stream and fails fast, and `:152-154` still creates the DLQ.
- `src/metadata.rs:47-150` `MessageMetadata`, `#[non_exhaustive]`, five fields, builder at `:194-249`.
  `docs/pages/concepts/handlers.mdx:160-170` lists the fields.
- `src/backends/kafka/consumer.rs:498-536` `extract_string_headers` and `build_message_metadata(headers, redelivered)`.
  Call sites at `:3363` (concurrent), `:3807` (batch), `:4295` (FIFO), `:4791` (broadcast), `:5951` (test).
  `:1226-1229` `DeferredDelivery { payload, headers }`.
- `src/handler.rs:20-25` `MessageHandler::handle` takes `T::Message` by value, and `src/topic.rs:13-24` does not require `Clone` on it.
- `src/consumer.rs:265-271` `kafka_group_id` field, `:531-560` `into_inner` with `kafka_auto_offset_reset: None` at `:553`.
  `:680-702` the `impl ConsumerOptions<Kafka>` block with `with_group_id`, `:703-728` the schema-registry block with `accept_schema_subjects` at `:720`.
  `src/backend/options_inner.rs:45-56` the two Kafka fields on the inner struct, `:84` `defaults_with_shutdown`.
  Other `ConsumerOptionsInner` literals: `src/backend/mod.rs:205-234` (a test anchor).
  `src/batch_consumer.rs:222-240` `into_inner` and `:275-295` the schema-registry block, `src/backend/batch_consumer.rs:92-129` the inner struct.
- `src/backends/kafka/consumer.rs:2925-2936`, `:3565-3574`, `:3935-3944`: group id and reset resolution on the concurrent, batch and FIFO paths.
  Each defaults the reset to `Earliest`.
- `:1032-1207` `route_outcome`.
  `:1145-1173` the `Retry` arm and `:1174-1203` the `Defer` arm both call `run_delayed_republish` (`:1320-1420`).
  That helper publishes to the consumed topic (`:1349-1356` concurrent, `:1398-1405` FIFO), and selects on cancellation before the sleep ends (`:1334-1345`).
- `:3365-3367`: the receive loop acquires a prefetch permit inside the `recv` arm, after a record arrived, and keeps the raw bytes as `payload_bytes`.
  A loop with every permit held therefore stops polling.
- `constants.rs:165` `SESSION_TIMEOUT_MS` 10 s, `:172` `MAX_POLL_INTERVAL_MS` 5 min, both pinned.
- `src/schema_registry/wire.rs:44` `FrameResult::Framed { id, payload }`, `:107-129` `parse_frame`, `:132-143` `skip_message_indexes` discards the index array, `:200-209` the test that pins an explicit index being stripped.
- `src/schema_registry/decode.rs:13-18` `RegistryDecode`, `:39-45` every `registry.resolve` error becomes `Dlq("schema_resolve_failed")`.
  `src/schema_registry/error.rs:5-21` the error enum, `:23-33` `is_retriable`.
  `src/schema_registry/client.rs:323-378`: 404 is `NotFound`, redirects and unexpected statuses are non-retriable `Transport`, 5xx and send errors are retriable `Transport` after the client's own retries.
  Call sites that discard the record: `consumer.rs:3255-3271` (concurrent), `:3787-3800` (batch), `:4757-4762` (broadcast), plus the FIFO path.
  The batch path extends its commit span before decode at `:3753-3764`.
- `src/metrics.rs:174-228` `FailReason`, `:238-243` `for_schema_reason` routes `schema_frame_invalid` and `schema_unsupported_codec` to `SchemaFrame` and everything else to `SchemaValidation`, `:347` `record_failed`, `:450-458` `record_terminal`.
- `tests/kafka_schema_registry.rs:17-47`: an in-process axum mock with one fixed status per spawn and no consumer.
- `testcontainers-0.27.3/src/core/containers/async_container.rs:161-168`: `ContainerAsync::pause` and `unpause`.
- Conventions: Conventional Commits with a scope, no `Co-Authored-By`, `absolute-paths = "deny"` (`Cargo.toml:54-55`), `cargo nextest run` and never `-q`.
  Clippy runs with `-D warnings` on `--all-features`, on `--no-default-features` and on every single-backend set.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Format | `cargo fmt -- --check` | exit 0 |
| Lint (all) | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Lint (min) | `cargo clippy -q --no-default-features --all-targets -- -D warnings` | exit 0 |
| Lint (kafka set) | `cargo clippy -q --lib --no-default-features --features kafka,kafka-ssl,kafka-msk-iam,test-support,audit,metrics,sbe,env-config -- -D warnings` | exit 0 |
| Lint (registry set) | `cargo clippy -q --lib --no-default-features --features kafka,kafka-schema-registry,protobuf -- -D warnings` | exit 0 |
| No cyrus on TLS only | `cargo tree -e features --no-default-features --features kafka-ssl -i sasl2-sys` | exit code not 0, package absent |
| Docs | `cargo doc --no-deps --all-features` | exit 0 |
| No-Docker tests | `cargo nextest run --no-default-features` | all pass |
| Kafka unit | `cargo nextest run --features kafka,kafka-schema-registry --lib` | all pass |
| Registry mock | `cargo nextest run --all-features --test kafka_schema_registry` | all pass |
| Kafka integration (Docker) | `cargo nextest run --features kafka,kafka-schema-registry,metrics --test kafka_integration` | all pass |
| Registry outage (Docker) | `cargo nextest run --features kafka,kafka-schema-registry,metrics --test kafka_schema_registry_outage` | all pass |
| Broadcast integration (Docker) | `cargo nextest run --features kafka --test kafka_broadcast_integration` | all pass |
| Batch integration (Docker) | `cargo nextest run --features kafka,kafka-schema-registry --test kafka_batch_integration` | all pass |
| Offset reset integration (Docker) | `cargo nextest run --features kafka --test kafka_offset_reset_integration` | all pass |
| Docs site | `pnpm install --frozen-lockfile && pnpm build` | exit 0 |

## Scope

**In scope**:

- `Cargo.toml` features, `.github/workflows/ci.yml`, `CONTRIBUTING.md`, `README.md`, `src/lib.rs` feature table.
- `src/consumer.rs`, `src/batch_consumer.rs`, `src/backend/options_inner.rs`, `src/backend/batch_consumer.rs`, `src/backend/mod.rs` (the options literal in its test anchor).
- `src/metadata.rs`, `src/metrics.rs`, `src/handler.rs` and `src/audit.rs` (metadata literals in tests), `src/topology.rs`, `src/broker.rs`, `src/broadcast.rs`, `src/backend/broadcast.rs`, `src/backend/capability.rs` docs, `src/consumer_group.rs` docs.
- `src/backends/kafka/consumer.rs`, `consumer_group.rs`, `topology.rs`, `client.rs`, `constants.rs`, `offset_reset.rs`, `mod.rs`.
- `src/schema_registry/decode.rs`, `src/schema_registry/wire.rs`.
- Every other backend's `MessageMetadata` construction site, for the three new fields only: `src/backends/sns/consumer.rs`, `src/backends/redis/consumer.rs`, `src/backends/redis/broadcast.rs`, `src/backends/rabbitmq/headers.rs`, `src/backends/inmemory/consumer.rs`, `src/backends/nats/consumer.rs`.
- `tests/kafka_integration.rs`, `tests/kafka_broadcast_integration.rs`, `tests/kafka_batch_integration.rs`, `tests/kafka_schema_registry.rs`, and two new files, `tests/kafka_schema_registry_outage.rs` and `tests/metrics_kafka_external_topic_discard.rs`.
- `docs/pages/backends/kafka.mdx`, `docs/pages/concepts/broadcast.mdx`, `docs/pages/concepts/handlers.mdx`, `docs/pages/concepts/topics.mdx`, `docs/pages/concepts/outcomes.mdx`, `docs/pages/ops/backends.mdx`, and the metric reference in `docs/pages/guides/observability.mdx`.
- `plans/021-kafka-external-topics-and-broadcast-start.md`, this plan document, committed on the first branch.

**Out of scope**:

- Plan 011's raw client-property passthrough.
- Changing any pinned constant value, including `ASYNC_COMMIT_INTERVAL`, `SESSION_TIMEOUT_MS` and `MAX_POLL_INTERVAL_MS`.
- Committing inside `pre_rebalance` before a revoke, which plan 007 deferred and the Maintenance notes discuss.
- A start position for NATS, Redis or RabbitMQ broadcast subscriptions.
- Filling the new metadata fields on SQS and RabbitMQ.
  NATS fills `offset` and `timestamp_ms` and Redis fills `timestamp_ms` in step 7.
  The Maintenance notes list what the other two could fill.
- The FIFO commit cadence, which commits per message.
- Moving `commit_batch_end` off `spawn_blocking`, which the Maintenance notes discuss.
- A partition-count accessor on the public API.
- Any change to `decide_retry`, `settle_broadcast_outcome` or the batch settling module.

## Steps

Each step is one commit.
The commit subjects are given per step.
Steps 1 and 2 form the first PR, steps 3 to 9 the second, steps 10 and 11 the third.

### Step 1: `KafkaAutoOffsetReset::None` sends librdkafka the token it accepts

Commit: `fix(kafka): map KafkaAutoOffsetReset::None to librdkafka's "error" token`.

librdkafka's `auto.offset.reset` accepts `smallest`, `earliest`, `beginning`, `largest`, `latest`, `end` and `error`.
`as_rdkafka_str` at `consumer_group.rs:77-83` returns `"none"` for the `None` variant, so a consumer configured with it fails at creation.

- Change the mapping to `"error"`.
  Keep the variant name, because the rustdoc semantics at `:70-73` are right and only the wire token is wrong.
- Classify `RDKafkaErrorCode::AutoOffsetReset` as permanent in `map_kafka_error` (`consumer.rs`), beside `ClientConfig` and `MessageConsumptionFatal`.
  librdkafka raises it when a partition has no committed offset, or the committed one is out of range, and the policy is `error`.
  A reconnect rejoins the same group under the same policy and meets the same answer, so the consumer ends with `ShoveError::Topology` and names the fault.
  The operator either commits a starting position with `reset_consumer_group_offsets` or picks another `KafkaAutoOffsetReset`.
  Without this classification the error is `Connection` and the consumer reconnects in a loop.
- Rewrite the unit test at `:1623` to assert `"error"`.
- Add a native-config unit test: for each variant, set `auto.offset.reset` on an `rdkafka::ClientConfig` and call `create_native_config`, which must succeed.
  This is the test that would have caught the bug, and it needs no broker.
- Add a classifier unit test: `map_kafka_error` turns `KafkaError::MessageConsumption(RDKafkaErrorCode::AutoOffsetReset)` into `ShoveError::Topology`.

Docs: `docs/pages/backends/kafka.mdx:137` describes `None` as "refuse silent replay/skip on a fresh group", which stays true.

Migration note: a group configured with `None` starts working.
No caller can have depended on the failure.

**Verify**: Kafka unit suite passes.

### Step 2: `kafka-ssl` enables TLS and native SASL only, `kafka-gssapi` carries cyrus

Commit: `feat(kafka)!: split cyrus SASL out of kafka-ssl into kafka-gssapi`.

`Cargo.toml`:

```toml
# TLS plus the SASL mechanisms librdkafka implements itself: PLAIN, SCRAM-SHA-256,
# SCRAM-SHA-512 and OAUTHBEARER. Needs OpenSSL and nothing else.
kafka-ssl = ["kafka", "rdkafka?/ssl"]

# GSSAPI/Kerberos through cyrus SASL. Links libsasl2 into every build and image.
# Only needed when a broker requires the GSSAPI mechanism.
kafka-gssapi = ["kafka-ssl", "rdkafka?/gssapi"]
```

`kafka-msk-iam` keeps implying `kafka-ssl`, because OAUTHBEARER is native.
No Rust code changes: `KafkaSasl` has no GSSAPI variant, so nothing in `src/` is gated on the new feature.
The evidence that SCRAM needs no cyrus is the same on both build paths.
Unix builds through `configure`, where `configure.self:143-148` enables SCRAM and OAUTHBEARER from SSL.
Windows builds through cmake, where `build.rs:271-275` does the same.

CI:

- Keep `libsasl2-dev` in the four `apt-get` lines, because `--all-features` now includes `kafka-gssapi`.
- Add one step to the `check` job that proves the split: `! cargo tree -e features --no-default-features --features kafka-ssl -i sasl2-sys`.
- Add `kafka-gssapi` to the Kafka coverage set at `ci.yml:273` so the feature compiles and links in a test job too.

Docs, all five places that name the feature:

- `src/lib.rs:51-52`: split the `kafka-ssl` row and add a `kafka-gssapi` row.
- `docs/pages/backends/kafka.mdx:15-31` install section and `:424-425` gotchas: `libsasl2-dev` is needed with `kafka-gssapi` only.
- `docs/pages/ops/backends.mdx:79-84`, `CONTRIBUTING.md:76-77`, `README.md:95`.

Migration note: a downstream that enabled `kafka-ssl` and used raw `rdkafka` with GSSAPI relied on shove's transitive edge, and must add `kafka-gssapi`.
Nothing that goes through shove's own API can be affected, which is why the `!` marker is defensible but light.

**Verify**: `check`, `feature-lint` and `msrv` jobs green, the `cargo tree` step prints no `sasl2-sys`.

### Step 3: The tracker commits past offsets the broker never delivered

Commit: `fix(kafka): commit past compacted and transactional gaps instead of stalling on them`.

Kafka log compaction removes records without renumbering the survivors.
A transactional producer leaves a control record at the end of every transaction, and consumers never receive it.
`PartitionTracker` seeds `next_to_commit` from the first delivered offset (`:349-353`) and then advances only while `completed` holds every consecutive integer (`:287-291`).
An undelivered offset therefore blocks the commit position until the assignment changes.

Design, in `src/backends/kafka/consumer.rs`:

- Replace `completed: BTreeSet<i64>` with `in_flight: BTreeSet<i64>` and `highest_delivered: i64`.
  `track_received` inserts every delivered offset into `in_flight` and raises `highest_delivered`, on every delivery and not only the first.
  `mark_complete` removes the offset from `in_flight`.
  An offset below `next_to_commit`, or one that `in_flight` never held, is stale from an earlier assignment and settles its discard as `survived()`, exactly as today.
- The commit position is the smallest offset still in flight, or `highest_delivered + 1` when nothing is in flight.
  `drain_committable` reports progress when that position exceeds `next_to_commit`, and never moves backwards.
  `has_committable` mirrors the same condition.
- The dirty streak, `pending_discards` and the fence logic are unchanged.
  A discard is covered when its offset is below the new position, as before.
- `OffsetTracker::remove` on assign and revoke (`:385-387`, `:413-416`) is what makes the position per assignment, and it stays.

Tests:

- Unit, tracker tests near `:5128`: `undelivered_gaps_do_not_block_the_commit_position` delivers 0, 1 and 3, completes them, and drains one commit at 4.
  `a_delivered_but_unfinished_offset_still_blocks` delivers 0, 1 and 2, completes 0 and 2, drains at 1, completes 1, and drains at 3.
  `nothing_in_flight_commits_highest_delivered_plus_one`.
  Rewrite `contiguous_drain_advances_past_gaps_only_when_filled` (`:5142`) into the first two, because its name states the rule this step removes.
- Integration, `tests/kafka_integration.rs`: `transactional_gaps_do_not_stall_commits` produces through an rdkafka transactional producer in several transactions, consumes everything, and asserts the committed offset reaches `highest_delivered + 1` on every partition.
  A second consumer under the same group then receives nothing, which is the property the tracker exists for.
  The committed offset sits one below the high watermark after the last control record, so the test compares against delivered offsets, not the watermark.
- Regression: `committed_offsets_advance_while_consumer_is_idle` (`tests/kafka_integration.rs:2468`) must pass unchanged, because a plain producer leaves no gaps.

Docs: `docs/pages/backends/kafka.mdx:254-260` gains one sentence that compaction and transactions do not stall commits.

Migration note: none.
A topic without gaps commits the same positions as before.

**Verify**: Kafka unit and integration suites pass, including the rewritten tracker tests.

### Step 4: A configurable commit interval and a final commit on its own thread

Commit: `feat(kafka): make the offset commit interval configurable and bound the shutdown commit`.

Public API, fields on the structs and setters in their `impl` blocks:

```rust
// src/backends/kafka/consumer_group.rs, struct KafkaConsumerGroupConfig
commit_interval: Option<Duration>,

// src/backends/kafka/consumer_group.rs, impl KafkaConsumerGroupConfig, next to with_auto_offset_reset
/// How often the consumer commits the offsets its handlers completed.
/// Unset keeps the 500 ms default. Panics on a zero interval.
pub fn with_commit_interval(mut self, interval: Duration) -> Self
pub fn commit_interval(&self) -> Option<Duration>

// src/consumer.rs, struct ConsumerOptions<B>
#[cfg(feature = "kafka")]
pub kafka_commit_interval: Option<Duration>,

// src/consumer.rs, impl ConsumerOptions<Kafka>
pub fn with_commit_interval(mut self, interval: Duration) -> Self
```

The builders assert `!interval.is_zero()`, copying `RedisConfig::with_trim_interval` at `src/backends/redis/client.rs:151-153`.
Any nonzero interval up to one hour is accepted, above or below 500 ms.
`MAX_COMMIT_INTERVAL` in `constants.rs` is the upper bound.
The gate adds the interval to an `Instant`, and the fence threshold grows with four intervals.
Both setters panic past an hour, at configuration time and never in the receive loop.
`ConsumerOptionsInner` gains `kafka_commit_interval`, and every literal of it sets the field: `src/consumer.rs:537`, `src/backend/options_inner.rs:84`, `src/backend/mod.rs:205`.
`spawn_one` copies the group value into the options like `auto_offset_reset` at `consumer_group.rs:801`.
The receive loop builds the gate from `options.kafka_commit_interval.unwrap_or(ASYNC_COMMIT_INTERVAL)` at `consumer.rs:3028`.

The fence threshold scales with the interval:

- The fenced-consumer detector clears a dirty streak after `QUIET_DRAINS_TO_RESOLVE` quiet drains, and drains run once per interval.
  Recovery from one rejected commit therefore takes about three intervals.
  With a 60 s `COMMIT_FENCE_TIMEOUT` an interval above 20 s would fence a healthy consumer.
- Compute the fence threshold as `COMMIT_FENCE_TIMEOUT.max(4 * interval)` at the fence check (`:3041`), and update the docs at `:79-95` and `:1940-1946`.

The final commit moves onto a dedicated thread that owns the consumer:

- Tokio cannot abort a blocking task once it started, and runtime shutdown waits for every started blocking task.
  `spawn_blocking` under a timeout therefore bounds nothing, and `commit_batch_end` at `:2066-2085` has that exposure today.
  Dropping the consumer also blocks, because rdkafka polls the consumer close inside `Drop`.
- In the shutdown branch at `:3107-3129`, after the permits are collected, move the `Arc<KafkaStreamConsumer>` into a `std::thread::Builder` thread named `shove-kafka-final-commit`.
  The thread runs the `Sync` commit, sends the result on a `oneshot`, and then drops the consumer, so the close also runs off the runtime.
  The loop awaits the `oneshot` under `tokio::time::timeout(SHUTDOWN_COMMIT_DEADLINE)`.
- `SHUTDOWN_COMMIT_DEADLINE` is a new 20 s constant in `constants.rs`, chosen against the 30 s termination grace Kubernetes gives a Pod by default.
- On a result within the deadline, settle the pending discards as today.
  On timeout, log the existing "batch may be redelivered" warning with the deadline, mark every pending discard `survived()`, and return.
  The thread finishes on its own, and a thread that is still running never blocks runtime shutdown or process exit.
- The receive loop must hold the only clone of the consumer `Arc` at that point.
  The concurrent loop does today, because handler tasks receive the client and not the consumer.

What this step deliberately keeps:

- Steady-state commits stay `CommitMode::Async` with `commit_callback` re-offers and the fenced detector.
  A synchronous commit per interval would serialize a broker round trip into the receive loop.
  At-least-once already covers the one exposure, a retried async commit landing after the final one, and the docs name it.
- The per-assignment replay guard is already there by construction after step 3.
  `mark_complete` ignores an offset below `next_to_commit`, and `remove` resets the tracker on assign and revoke (`:385-387`).
  This step adds the unit test that pins it, so a future refactor cannot lose it silently.

Tests:

- Unit, tracker tests near `:5128`: `a_replayed_lower_offset_never_lowers_the_committed_position` completes offsets 1 to 4, then 1 again, and drains exactly one commit at 5.
- Unit, gate tests near `:5777`: the gate honours a configured interval.
- Unit: the fence threshold scales with the interval.
- Integration, `tests/kafka_integration.rs`: `commit_interval_bounds_how_far_committed_offsets_lag`, modelled on `committed_offsets_advance_while_consumer_is_idle`.
  With a 5 s interval and every commit accepted, the committed offset stays behind the processed count for about one interval and then catches up.
- Integration, `tests/kafka_integration.rs`: `shutdown_exits_the_process_while_the_broker_is_frozen`, a parent test that drives a child process.
  The child is an `#[ignore]` test in the same binary, `child_consumes_then_shuts_down_on_stdin`.
  The parent re-invokes the binary through `std::env::current_exe()` with `--ignored --exact` and passes the bootstrap address and the topic in environment variables.
  The child runs a consumer group and prints a ready line after it handled one record.
  It resolves its shutdown signal when a line arrives on stdin.
  The whole child process therefore exits when `run_until_timeout` returns.
  The parent produces one record, waits for the ready line, pauses the container with `ContainerAsync::pause` (`testcontainers-0.27.3/src/core/containers/async_container.rs:161`), and only then writes the stdin line.
  It asserts the child exits with status 0 within `SHUTDOWN_COMMIT_DEADLINE` plus 10 s while the broker stays paused.
  It also asserts that the child's stderr carries the deadline warning.
  The parent calls `unpause` after the exit, never before it.
  A detached thread does not keep a Rust process alive.
  An exit inside the window therefore proves the commit thread held neither the runtime nor the process.

Docs:

- `docs/pages/backends/kafka.mdx:254-260`, "Offset commit semantics": rewrite around the gate, the knob, the shutdown deadline, and the replay windows.
  After an accepted commit, and with every earlier offset on the partition complete, a crash replays the records completed since that commit, about one interval.
  A commit the coordinator rejected is re-offered on a later drain, and an unfinished earlier offset holds the position.
  The window can therefore exceed one interval.
  A moved partition replays from its last accepted commit.
  A clean shutdown that overlapped a coordinator hiccup can replay a few records.
- `kafka.mdx:122-141`, group configuration: one bullet for `with_commit_interval`.
- `docs/pages/ops/backends.mdx:88`, the asynchronous commit paragraph.

Migration note: the default interval stays 500 ms and the shutdown commit is still synchronous.
It now runs on a dedicated thread and gives up after 20 s, where it previously waited without bound and held the runtime.

**Verify**: Kafka unit and integration suites pass.

### Step 5: A start position and a configurable inert group id for the Kafka broadcast subscriber

Commit: `feat(kafka): let a broadcast subscription start at the head, the tail or a timestamp`.

Where an ephemeral subscription starts is not a Kafka idea: NATS has `DeliverPolicy::{New, All, ByStartTime}` and a Redis stream id carries its millisecond.
The maintainer's review of 2026-09-22 therefore asked for a backend-neutral shape in place of the Kafka-typed knob this step first shipped.
The shape below is the one that landed.

Public API, the type, the field and the setter, scoped to `HasBroadcast` like the rest of the broadcast surface:

```rust
// src/broadcast.rs, re-exported at the crate root
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastStart {
    /// Deliver-new, the default on every backend.
    Tail,
    /// Every retained message, then the tail.
    Head,
    /// The first message at or after this instant, in milliseconds since the Unix epoch.
    Timestamp(i64),
}

// src/consumer.rs, struct ConsumerOptions<B>
pub broadcast_start: Option<BroadcastStart>,

// src/consumer.rs, impl<B: HasBroadcast> ConsumerOptions<B>
pub fn with_broadcast_start(mut self, start: BroadcastStart) -> Self

// src/backend/broadcast.rs, trait BroadcastImpl
fn check_options(queue: &str, options: &ConsumerOptionsInner) -> Result<()>;
```

`ConsumerOptionsInner` gains `broadcast_start`, and the three literals named in step 4 set it.
Neither field is feature-gated, because the concept is not.
`BroadcastSubscriber::subscribe` calls `check_options` before it spawns the loop.
A backend therefore refuses what it cannot honour synchronously, at the call site, with `ShoveError::Topology`.
A refused subscribe leaves the handle free to retry.
Kafka honours all three variants.
NATS, Redis, RabbitMQ and the in-process broker start at the tail only on this version.
`None` and `Tail` pass, and `Head` and `Timestamp` are refused through one shared helper, `refuse_start_other_than_tail`.
Its message names the topic, the variant, the backend and the way out.
Mapping `Head` and `Timestamp` onto `DeliverPolicy::All`, `DeliverPolicy::ByStartTime` and a Redis id is a later step, and needs a checked conversion for the NATS `OffsetDateTime`.
The timestamp unit is Unix epoch milliseconds on every backend, the unit `KafkaOffsetReset::Timestamp` already documents.
`KafkaOffsetReset` stays the type of `reset_consumer_group_offsets` and no longer names the broadcast start.
The refusal policy is the FIFO consumer's, applied to every knob an entry point never reads.
The Kafka broadcast subscription refuses `with_commit_interval` and `with_auto_offset_reset` in `check_options`, because it commits nothing and assigns every partition at an explicit offset.
Every competing-consumer entry point refuses a set `broadcast_start` through `ConsumerOptionsInner::refuse_broadcast_start`.
`ConsumerSupervisor::register` and `register_fifo` do so generically, and each backend's direct, FIFO and DLQ paths do so themselves.
A caller therefore meets the same `Topology` error whichever way it starts a consumer.

Kafka backend:

- Rename `assign_all_partitions_at_end` to `assign_all_partitions_at(topic, start: Option<BroadcastStart>, timeout)`.
- `None` and `Some(Tail)` add every partition at `Offset::End`, byte for byte the current behaviour.
- `Some(Head)` adds every partition at `Offset::Beginning`.
- `broadcast_start_positions` is one exhaustive match over the start.
  A new variant is a compile error there, and no `unreachable!` arm sits on the runtime path.
- `Some(Timestamp(ms))` fetches every partition's watermarks first, then calls `offsets_for_times`.
  It then resolves each partition through `target_from_timestamp_lookup`, made `pub(super)`.
  The order matters, because a record that lands between the lookup and a later watermark fetch would be skipped.
  `run_reset`'s order at `offset_reset.rs:380` and `:401` is the one to copy.
- `refresh_broadcast_partitions` resolves a newly discovered partition with the same start.
  A `Head` subscription then does not skip a new partition's first records.
- `run_broadcast_with_inner` uses `options.kafka_group_id` verbatim as the inert `group.id` when set, else `broadcast_group_id(queue)`.
  The value is inert either way.
  The doc on `broadcast_group_id` says why a caller may want to pick it: a cluster ACL that grants group Describe on one prefix only.
- In the broadcast `recv` arm, a `KafkaError::MessageConsumption(RDKafkaErrorCode::GroupAuthorizationFailed)` is logged once at `warn` and ignored.
  A consumer that never joins needs no group permission, and fetching is unaffected.
  Every other error keeps the current `return Err` at `:4684-4688`.
  Put the classification in a small pure function so a unit test can pin it.
- `reset_consumer_group_offsets` keeps refusing a broadcast topology.
  Its message changes from "always starts at the tail" to "has no committed offset, set the start with `with_broadcast_start`".

Tests:

- Unit, `offset_reset.rs`: a `Timestamp` start resolves through the group reset's lookup helper.
- Unit, `src/backend/broadcast.rs`: the tail-only helper passes `None` and `Tail` and refuses `Head` and `Timestamp`.
  Its message names the topic, the variant and the way out.
- Unit, `consumer.rs`: the error classifier ignores `GroupAuthorizationFailed` and passes every other code through.
- Unit, `src/consumer.rs`: `with_broadcast_start` propagates through `into_inner`, modelled on `kafka_with_group_id_propagates_through_into_inner` (`:1044-1050`).
- Integration, `tests/inmemory_broadcast.rs`: `broadcast_start_is_refused_by_a_backend_that_cannot_honour_it` asserts the synchronous `Topology` error from `subscribe()` for `Head` and `Timestamp`, and that `Tail` is then accepted on the same handle.
- Unit, `kafka/consumer.rs`: `run_rejects_broadcast_start` and `run_fifo_rejects_broadcast_start` against a client that never connects.
  `broadcast_subscribe_rejects_commit_interval` and `broadcast_subscribe_rejects_auto_offset_reset` cover `check_broadcast_options`.
  A negative control admits every start and the inert group id.
- Unit, `src/consumer_supervisor.rs`: `supervisor_register_rejects_broadcast_start` and `supervisor_register_fifo_rejects_broadcast_start` on the in-process broker, each proving the topic stays registrable after the refusal.
- Integration, `tests/kafka_broadcast_integration.rs`, three new tests.
  `broadcast_starts_from_the_head_when_asked` publishes before subscribing, subscribes with `Head`, and receives everything.
  `broadcast_starts_at_a_timestamp` publishes two batches around a captured timestamp, subscribes with `Timestamp`, and receives only the second.
  `broadcast_uses_the_configured_inert_group_id` asserts the group list still shows nothing under the configured name, reusing the control in `broadcast_leaves_no_consumer_group`.
- The existing `broadcast_fans_out_to_every_instance_from_the_tail` must pass unchanged, which is the byte-for-byte proof for the default.

Docs:

- `docs/pages/concepts/broadcast.mdx:33`: "Deliver-new only" gains the neutral `BroadcastStart`.
  Kafka honours all three starts, every other backend refuses `Head` and `Timestamp` at `subscribe()`, and deliver-new stays the default.
- `broadcast.mdx:116-124`, the Kafka section: describe the three starts and the configurable inert `group.id`.
- `docs/pages/backends/kafka.mdx`: a new `### Starting a broadcast subscription elsewhere than the tail` after the re-anchoring section (`:164-199`).
  The "See also" broadcast line at `:439` is updated.
- `src/backend/broadcast.rs:28-30`, `src/backend/capability.rs:75`, `src/broadcast.rs:142-144`: each names the neutral start and which backends honour it.

Migration note for existing users: none, every default is unchanged.

**Verify**: broadcast integration suite and Kafka unit suite pass.

### Step 6: `kafka_external_topic()`, a topology that shove reads but never creates or alters

Commit: `feat(topology): kafka_external_topic() binds to an infra-owned topic without declaring it`.

Public API:

```rust
// src/topology.rs, struct TopologyBuilder and struct QueueTopology
#[cfg(feature = "kafka")]
kafka_external_topic: bool,

// src/topology.rs, impl TopologyBuilder
#[cfg(feature = "kafka")]
pub fn kafka_external_topic(mut self) -> Self

// src/topology.rs, impl QueueTopology
#[cfg(feature = "kafka")]
pub fn kafka_external_topic(&self) -> bool
```

`build()` panics when the flag is combined with `sequenced()` or with any of the six Kafka topic-config methods at `src/topology.rs:685-744`.
Those are `with_topic_config`, `with_retention`, `with_retention_forever`, `with_retention_bytes`, `with_cleanup_policy` and `with_max_message_bytes`.
The wording follows the NATS guards at `:923-935`.
`dlq()`, `dlq_named()`, `hold_queue()`, `for_consumer_group()` and `broadcast()` stay allowed.
Sequenced topics are refused for now to mirror NATS and keep the test matrix small, see Maintenance notes.

Kafka backend:

- `KafkaTopologyDeclarer::declare_standard` branches on the flag.
  In external mode it verifies the topic exists with one metadata fetch.
  When the topic is absent it returns `ShoveError::Topology` naming the topic, with the fail-fast wording of `src/backends/nats/topology.rs:127-129`.
  It never calls `create_topic`, `ensure_partitions` or `ensure_topic_configs` for the main topic.
- The verification fetch uses a one-shot consumer-type metadata client with `allow.auto.create.topics=false` set explicitly and no `group.id`, `probe_external_topic_blocking` in `client.rs`.
  The producer's own client, the one `KafkaClient::ping` uses, pins that flag to `false` as well, in `producer_config`.
  That pin is delivered by `feat/kafka-producer-no-auto-create` in the same release, and the probe does not rely on it.
  Without the pin, librdkafka's producer default of true would create the very topic `external()` promises never to create.
  Without a `group.id` there is no coordinator lookup, so the probe needs no group permission under a group-scoped ACL.
  Do not reuse `fetch_topic_partition_count_blocking`, which sets one.
- The DLQ, when declared, is still created, because shove owns its dead-letter topic in both modes.
  NATS does the same at `nats/topology.rs:152-154`.
- `register` and `register_fifo` need no change, because the flag travels inside the topology they hand to the declarer.
  Their `with_min_partitions(max_consumers)` has no effect in external mode.
  The rustdoc says that a `max_consumers` above the partition count leaves members idle.

Tests:

- Unit, `src/topology.rs`: the flag round-trips, and each of the seven forbidden combinations panics with its message.
- Integration, `tests/kafka_integration.rs`, two new tests.
  `external_topic_is_never_created_or_expanded` pre-creates a three-partition topic through rdkafka's admin client.
  It then registers a group with `max_consumers` 8 on an external topology, consumes, and asserts the partition count is still three.
  `external_topic_missing_fails_fast_at_declare` asserts `Topology` from `declare` on a topic nobody created.

Docs:

- `docs/pages/backends/kafka.mdx:77-83`, "Declare topology": add `### Bind to an infra-owned topic`, mirroring `docs/pages/backends/nats.mdx:126-146`.
  State the ACLs a reader then needs: Describe and Read on the topic, Describe and Read on the group.
- `docs/pages/concepts/topics.mdx:158`, "Topology declaration": one sentence pointing at both external flags.
- `src/consumer_group.rs:123-128` and `:160-165`: the "automatically declares" rustdoc gains the two exceptions.

Migration note: none, the flag is opt-in.

**Verify**: Kafka integration suite passes, including the two new tests.

### Step 7: Partition, offset and broker timestamp on `MessageMetadata`

Commit: `feat(metadata): expose partition, offset and broker timestamp on Kafka deliveries`.

Public API, in `src/metadata.rs`, on the struct:

```rust
/// Partition the record was read from. `None` where the backend has no partitions.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub partition: Option<i32>,
/// Offset of the record inside its partition. `None` where the backend has no log offset.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub offset: Option<i64>,
/// The record's broker timestamp in milliseconds since the Unix epoch.
/// `LogAppendTime` or `CreateTime`, as the topic is configured. `None` when absent.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub timestamp_ms: Option<i64>,
```

Plus three builder setters, `partition`, `offset` and `timestamp_ms`, each `impl Into<Option<_>>` like `delivery_count` at `:223-226`.
Each field carries its own per-backend availability table in the rustdoc, modelled on the `delivery_count` table at `:79-88`.
Each is filled where the backend has the data.
The maintainer's review of 2026-09-22 asked for this in place of the first shape, which documented the three fields as Kafka-only.
`partition` is the one Kafka-shaped field.
`offset` is the offset inside the partition on Kafka and the stream sequence on NATS, a log position with no partition.
The NATS sequence is converted with `i64::try_from`, so a sequence past `i64::MAX` reads as `None`.
`timestamp_ms` is the broker timestamp on Kafka and the time component of the entry id on Redis.
On NATS it is the server's receive time, `Info.published`, as Unix milliseconds through a checked conversion.
The Redis value is worded as the id's time component and never as a publish time.
Redis fills it with the instance clock only for an id it generated, and a publisher may supply an explicit id.
After a clock rollback Redis reuses the top entry's time and increments the sequence part.
SQS (`SentTimestamp`) and RabbitMQ (the optional AMQP `timestamp` property) stay `None` on this version and follow later.
The struct is `#[non_exhaustive]` since PR 71, so downstream code that reads fields or uses the builder is unaffected.

Construction sites that gain the three fields, filled where the backend has the data and `None` elsewhere:

- `src/metadata.rs:241-249` builder `build`.
- `src/backends/sns/consumer.rs:101`, `src/backends/redis/consumer.rs:901`, `:1335`, `:2592`, `src/backends/redis/broadcast.rs:292`.
- `src/backends/rabbitmq/headers.rs:50`, `src/backends/inmemory/consumer.rs:2545`, `src/backends/nats/consumer.rs:137`.
- Test fixtures at `src/handler.rs:191`, `src/audit.rs:310`, `:406`, `:459`.
- NATS fills `offset` and `timestamp_ms` from the message's stream metadata through two checked helpers, `sequence_to_offset` and `published_to_millis`.
- The four Redis sites fill `timestamp_ms` through `stream_id::time_component_ms`, which reuses the existing id parser.
  A malformed id or a time past `i64::MAX` reads as `None`.

Kafka backend:

- Introduce a small `RecordCoordinates { partition: i32, offset: i64, timestamp_ms: Option<i64> }` read from the `BorrowedMessage`.
  The accessors are `partition()`, `offset()` and `timestamp().to_millis()`.
- `build_message_metadata` takes it as a third parameter and fills the three fields.
  Call sites: `:3363`, `:3807`, `:4295`, `:4791`, and `build_dead_metadata` at `:539-556` for the DLQ drain.
- `DeferredDelivery` (`:1226-1229`) carries the coordinates so a deferred broadcast redelivery keeps the original ones.
- `DeadMessageMetadata::message` documents that on a DLQ drain the coordinates are the dead letter's own, on the DLQ topic.
  `retry_count` and `delivery_id` come from the original's headers.
  Source coordinates in the DLQ headers are a later change.

Tests:

- Unit, `src/metadata.rs`: defaults are `None`, setters work, serde round-trips and a JSON without the fields deserializes.
- Unit, `src/backends/nats/consumer.rs`: the two checked conversions pass the representable range and read the rest as `None`.
- Unit, `src/backends/redis/stream_id.rs`: the time component is the id's first field, checked.
- Integration, `tests/nats_integration.rs`: `nats_delivery_fills_offset_and_timestamp_from_stream_info` sees sequence 1 on a fresh stream and a publish time inside the test's wall-clock window.
- Integration, `tests/redis_integration.rs`: `redis_delivery_fills_timestamp_from_the_entry_id` sees the id's time inside the publish window and no partition or offset.
- Integration, `tests/kafka_integration.rs`: `handler_sees_partition_offset_and_timestamp`.
  It asserts all three are `Some`, offsets increase per partition, and the timestamp lies inside the test's wall-clock window.
  One assertion each goes into the broadcast suite and into `tests/kafka_batch_integration.rs`.

Docs: `docs/pages/concepts/handlers.mdx:160-170` lists the three fields with one availability table per backend and field, and the dead-letter provenance sentence.

Migration note: none for readers and builder users.
A downstream test that destructures without `..` already fails to compile since PR 71.

**Verify**: `cargo nextest run --no-default-features` and the three Kafka suites pass.

### Step 8: `auto.offset.reset` on `ConsumerOptions<Kafka>`

Commit: `feat(kafka): expose with_auto_offset_reset on ConsumerOptions for the direct and supervisor paths`.

Public API, the field on the struct and the setter in the Kafka `impl` block at `src/consumer.rs:680-702`:

```rust
// src/consumer.rs, struct ConsumerOptions<B>
#[cfg(feature = "kafka")]
pub kafka_auto_offset_reset: Option<KafkaAutoOffsetReset>,

// src/consumer.rs, impl ConsumerOptions<Kafka>
pub fn with_auto_offset_reset(mut self, reset: KafkaAutoOffsetReset) -> Self
```

`into_inner` copies the field instead of the literal `None` at `:553`.
The rustdoc at `src/backend/options_inner.rs:53-56` names both sources, the group config and this builder.
The three resolution sites (`consumer.rs:2934-2936`, `:3572-3574`, `:3942-3944`) already read the inner field and need no change.
The registry path is unchanged.
`spawn_one` still copies the group config's value (`consumer_group.rs:801`), so a group config wins on that path exactly as `with_group_id` does.

Tests: a unit test twin of `kafka_with_group_id_propagates_through_into_inner`, and one integration test.
The integration test runs a supervisor consumer with `Latest` on a topic with history and receives only new records.

Docs: `docs/pages/backends/kafka.mdx:137`, the `with_auto_offset_reset` bullet, names the options-level twin.

Migration note: none.

**Verify**: Kafka unit and integration suites pass.

### Step 9: An opt-in check on the Confluent protobuf message index

Commit: `feat(kafka): let a registry consumer require one protobuf message index`.

A Confluent protobuf frame carries a message-index array that names which message of the schema file the bytes encode.
`skip_message_indexes` reads and discards it (`src/schema_registry/wire.rs:132-143`), so a `ProtobufCodec<M>` decodes any message of the file as `M`.

Public API, on the three homes that already carry `accept_schema_subjects`:

```rust
// src/consumer.rs, struct ConsumerOptions<B>; src/batch_consumer.rs, struct BatchConsumerOptions<B>;
// src/backends/kafka/consumer_group.rs, struct KafkaConsumerGroupConfig
#[cfg(feature = "kafka-schema-registry")]
schema_message_index: Option<Vec<i32>>,

// the matching impl blocks, next to accept_schema_subjects
/// Accept only frames whose protobuf message index equals `index`, the path of
/// `M` inside its schema file. `[0]` is the first message. Unset keeps the
/// current behaviour of accepting any index.
pub fn require_schema_message_index(mut self, index: impl Into<Vec<i32>>) -> Self
```

`ConsumerOptionsInner` and `BatchConsumerOptionsInner` gain the field, and every literal sets it.
`spawn_one` copies it like the other registry settings (`consumer_group.rs:784-807`).

Decode stage:

- `FrameResult::Framed` gains `message_index: Option<Vec<i32>>`, filled for `WireFormat::Protobuf` and `None` for JSON.
  The single-zero-byte optimization at `:135-138` yields `Some(vec![0])`.
- `registry_decode` takes the required index and returns `Dlq("schema_message_index_rejected")` on a mismatch.
  `for_schema_reason` routes that string to `FailReason::SchemaFrame`, so the metric label is `schema_frame`.
- The check runs before the registry lookup, because a rejected frame needs no network round trip.

Tests:

- Unit, `wire.rs`: the parser returns `[0]` for the single byte, the explicit array for `[2, 1, 3]`, and `None` for JSON.
- Unit, `decode.rs`: a mismatch is a DLQ reason and a match decodes, both without a registry call.
- Registry mock, `tests/kafka_schema_registry.rs`: a frame with index `[1]` under `require_schema_message_index([0])` is counted as `schema_frame` and never resolved.

Docs: `docs/pages/backends/kafka.mdx:262-403`, the Schema Registry section, one paragraph, and the metrics list at `:404-416` names the new death reason.

Migration note: none, unset keeps today's behaviour.

**Verify**: registry mock suite and Kafka unit suite pass.

### Step 10: On an external topic, Retry and Defer never produce into the topic

Commit: `feat(kafka)!: retry and defer in place on an external topic instead of republishing into it`.

Rule: an external topic is read-only for shove.
A shove-owned DLQ is still a legal publish target, because `dlq()` stays allowed on an external topology.

Behaviour when `topology.kafka_external_topic()` is true, on the concurrent path and the FIFO path:

- `Outcome::Defer` waits `hold_queues[0].delay()`, or 1 s without hold queues, inside the same task and holding its permit.
  It then decodes the retained raw bytes again and calls the handler with the fresh value.
  `MessageHandler::handle` takes `T::Message` by value and `Topic` does not require `Clone`, so the bytes, not the value, are what the task keeps.
  The offset is not completed until the handler returns a terminal outcome.
- `Outcome::Retry` does the same with the tier delay from `hold_index`, and counts the attempt in memory instead of in a header.
  When `decide_retry` says `Dlq`, the existing terminal path runs: publish to the DLQ when declared, else discard with `max_retries_exceeded`.
- `Outcome::Reject` and every pre-handler discard are unchanged, because they never republished.
- Every wait selects on `shutdown.cancelled()`, exactly as `run_delayed_republish` does at `:1334-1345`.
  A cancelled wait drops the permit, completes nothing, and returns.
  The shutdown branch at `:3107-3109` then collects every permit, and the record is redelivered on restart.
- While every prefetch permit is held by waiting handlers, the receive loop must keep polling.
  Today it parks on `acquire_owned` inside the `recv` arm (`:3365-3367`), and a park longer than `MAX_POLL_INTERVAL_MS` evicts the member.
  In external mode the loop pauses its assignment when no permit is free and keeps calling `recv`.
  It resumes when a permit frees.
  rdkafka's `Consumer::pause` and `resume` on `assignment()` are the primitives.
- With `concurrent_processing(false)` the member runs one record at a time, so a deferred record stays ahead of every later record on its partitions.
  With concurrency above one, later records on the same partition may run first, which is already the contract of concurrent processing.

The broadcast path gets the same in-place shape:

- `route_broadcast_outcome` (`:1266-1300`) today hands a deferred delivery to a channel and releases its permit afterwards.
  A record received meanwhile is already waiting on that permit (`:4793-4797`), so it runs before the redelivery and the observed order is `[1, 2, 1]`.
- Replace the channel with the in-task wait above, holding the pinned single permit across the delay, so the order becomes `[1, 1, 2]`.
  `DeferredDelivery` and the `defer` channel go away.

When the flag is false, nothing changes on the group paths, so every existing Retry and Defer test stays green.
Kafka is the only backend where this arises.
It simulates hold queues by republishing into the consumed topic (`src/backends/kafka/topology.rs:225-236`), while NATS publishes retries into shove-owned hold streams.

Tests:

- Integration, `tests/kafka_integration.rs`, four new tests.
  `external_topic_defer_redelivers_in_place_without_producing` returns `Defer` once then `Ack` with `concurrent_processing(false)`, asserts the order `[1, 1, 2]`, and asserts the high watermark did not move.
  `external_topic_retry_exhausts_without_producing` sets `max_retries` 2, always returns `Retry`, and asserts three handler calls, no new records, and a committed offset.
  `external_topic_defer_works_for_a_message_type_without_clone` uses a topic whose message type derives only `Deserialize`.
  `external_topic_shutdown_during_a_wait_leaves_the_record_uncommitted` cancels during a `Defer` wait, asserts a prompt return, and asserts a restart redelivers the record.
- Integration, `tests/kafka_integration.rs`: `external_topic_waiting_handlers_keep_the_member_in_the_group` defers past a shortened test wait and asserts no rebalance in the logs.
  If the pinned 5 min cannot be shortened in a test, record that in NOTES.
- Integration, `tests/kafka_broadcast_integration.rs`: `defer_redelivers_in_place_before_later_records` asserts `[1, 1, 2]` on a broadcast subscription.
- Metrics: a `tests/metrics_kafka_external_topic_discard.rs` twin of `metrics_kafka_failall_no_dlq.rs`.
  It asserts `shove_messages_discarded_total{reason="max_retries_exceeded"}` moves once per exhausted record.

Docs:

- `docs/pages/backends/kafka.mdx`, the new external-topic subsection from step 6: a table of the four outcomes in external mode.
- `docs/pages/concepts/outcomes.mdx:56-60`: one sentence that Kafka external topics retry in place.
- `docs/pages/concepts/broadcast.mdx:116-124`: the Kafka section states that a deferred record is redelivered before later records.

Migration note: none for existing group topologies.
The `!` marks two things.
A handler on an external topology now blocks its slot while deferring, the same contract broadcast already has.
A Kafka broadcast subscription now delivers a deferred record before later records, which is what `broadcast.mdx` already promised.

**Verify**: full Kafka integration suite, the broadcast suite, the new metrics test, and every existing Retry and Defer test unchanged.

### Step 11: A Schema Registry outage waits instead of discarding

Commit: `fix(kafka): wait for an unavailable schema registry instead of discarding the record`.

`registry_decode` returns a third variant:

```rust
pub(crate) enum RegistryDecode<M> {
    Decoded(M),
    Dlq(&'static str),
    /// The registry could not answer right now. Retry the same bytes later.
    Unavailable(SchemaRegistryError),
}
```

Mapping of `registry.resolve` errors at `decode.rs:39-45`:

- `NotFound` stays `Dlq("schema_resolve_failed")`, because the registry definitely does not know the id.
- An error whose `is_retriable()` is true becomes `Unavailable`.
  Today that is a `Transport` from a send failure or a 5xx after the client's own retries (`client.rs:356-378`).
- Every other error is returned as `Err(ShoveError::Topology(..))`: a redirect, a 401 or 403, another unexpected status, or a `Decode` of the registry's response.
  These are deployment faults, and waiting on them forever would hide them.
  The receive loop returns that error, and `run_with_reconnect` stops on a non-retryable error.
  The group's respawn supervision then restarts the member with backoff and the circuit breaker (plan 015).

Consumer paths, on `Unavailable`:

- Concurrent and FIFO (`:3255-3271` and the FIFO twin): keep the raw bytes and the coordinates, and pause the assignment as in step 10.
  Then wait `REGISTRY_RETRY_DELAY`, a new 1 s constant.
  The wait selects on `shutdown.cancelled()`, and a cancelled wait completes nothing and returns.
  After the wait, decode the same bytes again.
  Log at `warn` with the schema id, and call `record_failed` with `FailReason::SchemaUnavailable`, a new non-terminal reason with label `schema_unavailable`, once per wait.
  It never calls `record_terminal`, so `messages_discarded_total` does not move.
- Batch (`:3787-3800`): the batch path extends the commit span before decode (`:3753-3764`), and the span must not cover an unavailable record.
  Do not call `extend_span` for it.
  Flush the buffer as it stands, whose span on that partition ends before the record, and commit that span.
  Park the record's raw bytes and coordinates in a single `Option`, pause the assignment, and retry the decode every `REGISTRY_RETRY_DELAY` with the same cancellation select.
  On success, call `extend_span` for it, push it into the fresh buffer, resume, and continue.
  Nothing after the parked record is read while the assignment is paused, so partition order holds.
- Broadcast (`:4757-4762`): wait and retry in place through the step 10 in-task wait.

Tests:

- New Docker test file `tests/kafka_schema_registry_outage.rs`, whose name matches the `binary(/schema_registry/)` filter of the `kafka-schema-registry` coverage entry (`ci.yml:286`).
  It runs a Kafka container and an in-process axum registry whose status is an `Arc<AtomicU16>` the test flips, extending the mock at `tests/kafka_schema_registry.rs:17-47`.
  `an_unavailable_registry_stalls_the_record_and_resumes` starts at 503, asserts no commit and one `schema_unavailable` increment per wait, flips to 200, and asserts delivery from the same offset.
  `a_batch_flushes_before_the_parked_record_and_resumes` does the same on the batch path and asserts the committed span ends before the parked record until it decodes.
  `shutdown_during_a_registry_wait_leaves_the_record_uncommitted` cancels mid-wait and asserts a restart redelivers the record.
  `an_authentication_failure_ends_the_consumer` flips to 401 and asserts the consumer task ends with `Topology` and the record stays uncommitted.
- Unit, `decode.rs`: the error mapping table, one case per variant.

Docs: `docs/pages/backends/kafka.mdx:262-403`, the Schema Registry section, gains one paragraph on outages and one on authentication faults.
The metrics reason list at `:404-416` and the reason table in `docs/pages/guides/observability.mdx:83-97` gain `schema_unavailable`.

Migration note: a consumer that relied on a registry outage draining records into the DLQ now stalls instead.
One that hit a 401 now ends its member instead of dead-lettering.
That is the at-least-once contract the rest of the crate keeps, and the PR body says so.

**Verify**: registry mock suite, the new outage suite, Kafka integration suite and both schema-registry coverage entries pass.

## Known patterns and alternatives

The maintainer's review of 2026-09-22 asked for backend-neutral shapes in four places.
This section records, per changed step, the patterns weighed and the one chosen, so the choice stays legible without the review thread.

### Step 7, the metadata fields

- Kafka-only fields with one combined table was the first shape.
  It hid different capabilities behind one `None` row per backend.
- Filling each field where the backend has the data is the chosen shape.
  NATS supplies the stream sequence and the publish time, and Redis supplies the id's time component.
  Every fill is a checked conversion, because `offset` is `i64` while the NATS sequence is `u64`.
- Per-field rows with the fill deferred was the alternative, and it keeps the non-Kafka fields empty.
- The Redis wording is the constraint that survived verification.
  An entry id carries the instance's millisecond only when Redis generated it.
  The field therefore exposes the id's time component, never an exact publish time.
- A namespaced attribute map, the shape of the OpenTelemetry messaging conventions (`messaging.kafka.offset`, `messaging.kafka.partition`), was weighed too.
  It carries any backend's coordinates without new fields.
  The price is typed access: a string lookup and a parse where a field gives an `Option<i64>`.
  The typed fields are kept because a handler reads them on every delivery and the set is small.
  The attribute map stays the shape for an export layer, not for the handler's argument.

### Step 5, the broadcast start

- A Kafka-typed knob, `with_broadcast_start(KafkaOffsetReset)` on `ConsumerOptions<Kafka>`, was the first shape.
  It bakes Kafka into a concept every log-shaped broker has, and a later NATS or Redis start would have needed a second knob.
- A backend-neutral enum on the `HasBroadcast`-scoped options is the chosen shape.
  NATS has `DeliverPolicy::{All, New, ByStartTime}` and a Redis stream id starts with a millisecond, so head, tail and timestamp map onto both.
  Each backend refuses at `subscribe()` what it cannot honour, the rule the FIFO consumer already applies to the commit interval.
- Implementing the NATS and Redis mappings in the same step was set aside for scope.
  The type and the refusal land now, and the mappings land later, each with its own tests.
- An absolute position is the pattern the brokers offer as well: async-nats has `DeliverPolicy::ByStartSequence` and RabbitMQ Streams take a numeric offset.
  It is deferred because a sequence or an offset names a position on one backend's log and means nothing on another.
  It would be the first backend-specific variant on a neutral enum, and `#[non_exhaustive]` leaves the room for it.

## Done criteria

- [ ] `cargo fmt -- --check` and the four clippy commands exit 0.
- [ ] `cargo doc --no-deps --all-features` exits 0 and the new rustdoc tables render.
- [ ] `cargo tree -e features --no-default-features --features kafka-ssl` prints no `sasl2-sys`, and the same command with `kafka-gssapi` prints it.
- [ ] `cargo nextest run --no-default-features` passes, including the new metadata and topology unit tests.
- [ ] The Kafka unit suite passes, including the native-config test for every reset variant and the rewritten tracker tests.
- [ ] The Kafka unit suite also covers the replay guard, the gate interval, the fence scaling, the error classifier and the registry mapping.
- [ ] The Kafka, broadcast, batch, offset reset, registry mock and registry outage suites pass, including every new test named above.
- [ ] Every pre-existing Retry, Defer, broadcast and commit test passes unchanged, which is the byte-for-byte proof for the defaults.
- [ ] `grep -rn "assign_all_partitions_at_end" src/` prints nothing.
- [ ] `grep -n "rdkafka?/sasl" Cargo.toml` prints nothing.
- [ ] `grep -rn '"none"' src/backends/kafka/consumer_group.rs` prints nothing.
- [ ] Every construction site of `MessageMetadata`, `ConsumerOptionsInner` and `BatchConsumerOptionsInner` sets the new fields.
- [ ] The five feature documentation places name `kafka-gssapi`.
- [ ] `pnpm build` succeeds for the docs site.
- [ ] No file outside Scope is modified.

## STOP conditions

- A "Current state" excerpt does not match the live code.
- `create_native_config` rejects `error` for `auto.offset.reset` on the pinned rdkafka.
  Report the librdkafka version and the message.
- `offsets_for_times` on rdkafka 0.39 returns a sentinel that `target_from_timestamp_lookup` refuses in a case the existing offset-reset suite accepts.
  Report the shape instead of relaxing the helper.
- Honouring `kafka_group_id` on the broadcast path changes the group list in `broadcast_leaves_no_consumer_group`.
  That would mean the id is not inert, and the step is wrong, not the test.
- The new tracker changes the committed position in any existing Kafka integration test that uses a plain producer.
  Report the test, because a gap-free topic must commit exactly as before.
- Making `Retry` or `Defer` in-place on an external topic requires a change to `decide_retry`, `settle_broadcast_outcome` or the batch settling module.
  Those are cross-backend surfaces and need their own review.
- The pause-and-poll discipline in step 10 cannot keep a member inside `MAX_POLL_INTERVAL_MS` in a test, or requires unpinning that constant.
- Parking an unavailable record on the batch path cannot be done without changing `BatchFlushCtx` semantics or the span accounting of pre-handler drops.
- Scaling the fence threshold makes `kafka_rebalance` or any fenced-consumer test flaky across three runs.
  Report the observed timings.
- `ContainerAsync::pause` does not freeze the broker enough for the child process to observe a blocked commit, or the test binary cannot re-invoke itself under nextest.
  Record it and ship the unit tests only.
- Any step needs a file outside Scope.

## Maintenance notes

- Committing inside `pre_rebalance` before a revoke would shrink the rebalance replay window from one interval to zero.
  Plan 007 deferred it because the tracker lives in the receive loop, not in the context, and a lock across the callback undoes perf-K-6.
  Revisit if a user runs intervals above a few seconds.
- `commit_batch_end` still runs its `Sync` commit on `spawn_blocking`, which runtime shutdown waits for.
  The dedicated-thread shape of step 4 applies there too, as a follow-up.
- The three metadata fields are filled where a backend has the data.
  Kafka fills all three, and NATS fills `offset` from the stream sequence and `timestamp_ms` from the message info.
  Redis fills `timestamp_ms` from the entry id's time component.
  SQS could fill `timestamp_ms` from `SentTimestamp` and RabbitMQ from the AMQP `timestamp` property.
  Do it per backend with its own availability row, as `delivery_count` did.
- `kafka_external_topic()` refuses `sequenced()` for now.
  Nothing in the Kafka FIFO consumer depends on shove having created the topic, so lifting the guard is additive once a user needs it.
- `KafkaOffsetReset` describes a position for `reset_consumer_group_offsets` alone.
  The broadcast start is the backend-neutral `BroadcastStart`, and `KafkaAutoOffsetReset` is the group's librdkafka policy.
  The three types share the timestamp unit and nothing else, so a variant added to one does not touch the others.
- Every new `.set(` on the Kafka client config still has to be checked against plan 011's reserved-key list when that plan lands.
- `FailReason::SchemaUnavailable` is the first reason that `record_failed` counts and `record_terminal` never does.
  `record_terminal`'s doc in `src/metrics.rs` is the authoritative completeness statement and must stay accurate.
- The committed position after a transaction sits one below the high watermark until the next data record, because the control record is never delivered.
  A lag gauge built on the difference reads 1 in that state, which the autoscaler's lag math tolerates.
