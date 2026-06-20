# CLAUDE.md — agent orientation for `shove`

This file orients an agent (or a new contributor) to the `shove` crate. For the
human build/test/lint workflow see [`CONTRIBUTING.md`](./CONTRIBUTING.md).

## What this is

`shove` is a type-safe, async pub/sub library over six message backends. The
entire public surface hangs off a single generic `Broker<B>`: you parameterize
it with a backend marker type `B`, and one consistent API (publish, consume,
consumer groups, topology, autoscaling) works across every backend.

## Architecture map

- **Generic layer** lives directly in `src/`: `broker.rs`, `publisher.rs`,
  `consumer.rs`, `consumer_group.rs`, `consumer_supervisor.rs`, `autoscaler.rs`,
  `topology.rs`, `topology_declarer.rs`. These are backend-agnostic wrappers
  around `Broker<B>`.
- **Per-backend implementations** live in `src/backends/<name>/`. Each backend
  has the same file layout: `backend.rs`, `client.rs`, `consumer.rs`,
  `consumer_group.rs`, `publisher.rs`, `topology.rs`, `autoscaler.rs`, plus
  `constants.rs`/`headers.rs`.
- The **sealed `Backend` trait** (`src/backend/mod.rs`) binds, for each marker
  type, that backend's client / publisher / consumer / topology / registry
  types. Being sealed means external crates cannot add backends.

### Backends, feature flags, marker types

| Backend | Feature flag | Marker type |
|---|---|---|
| RabbitMQ | `rabbitmq` | `RabbitMq` |
| AWS SNS+SQS | `aws-sns-sqs` (publisher-only: `pub-aws-sns`) | `Sqs` |
| NATS JetStream | `nats` | `Nats` |
| Apache Kafka | `kafka` (+ `kafka-ssl`, `kafka-msk-iam`, `kafka-schema-registry`) | `Kafka` |
| Redis/Valkey Streams | `redis-streams` | `Redis` |
| In-process | `inmemory` | `InMemory` |

Other add-on features: `audit`, `metrics`, `protobuf`, `rabbitmq-transactional`.

## Capability gating

- Kafka, RabbitMQ, NATS, InMemory, and Redis implement `HasCoordinatedGroups`
  and expose `Broker::consumer_group`.
- SQS does **not** implement `HasCoordinatedGroups` — calling `consumer_group()`
  on `Broker<Sqs>` is a **compile error**. SQS uses `ConsumerSupervisor`
  (`src/consumer_supervisor.rs`) instead.

## Where things live

- **Typed-topic macros** `define_topic!` / `define_sequenced_topic!` — in
  `src/macros.rs` (re-exported from `src/lib.rs`).
- **Outcome / retry semantics** — `src/outcome.rs` (the `Outcome` enum:
  `Ack` / `Retry` / `Reject` / `Defer`) and `src/retry.rs`.
- **Per-backend retry/DLQ routing** — each backend's
  `src/backends/<name>/consumer.rs::route_outcome` (RabbitMQ also has
  `src/backends/rabbitmq/router.rs`). This is where a delivery-semantics
  decision is turned into ack/commit/publish/DLQ mechanics.

## Build / test / lint commands

- **Fast path** (no Docker, no secrets): `cargo nextest run --no-default-features`
  runs unit + in-memory tests. (CI uses `cargo test --no-default-features`; this
  repo's convention is `cargo nextest run`.)
- **Integration tests** require Docker (real brokers via `testcontainers`). The
  AWS/MSK tests additionally require `LOCALSTACK_AUTH_TOKEN`, supplied via
  `dotenvx run -- cargo nextest run --features <set>`. See `CONTRIBUTING.md`
  for per-backend feature sets.
- **Lint gates CI enforces**:
  - `cargo fmt -- --check`
  - `cargo clippy --all-features -- -D warnings`
  - `cargo clippy --no-default-features -- -D warnings`
- **Never** run tests with plain `cargo test` if you can use `cargo nextest run`;
  do not pass `-q`/`--quiet` to `nextest` (it rejects it).

## Conventions for changes

- **Cross-backend consistency**: a delivery-semantics bug (retry boundary, DLQ
  routing, defer handling) usually must be fixed in **all** backends'
  `route_outcome`, not just one. The git history shows recurring
  "cross-backend consistency" commits because a fix landed in one backend and
  was missed in the others. When you touch retry/DLQ logic, check every backend.
- **Conventional Commits** for commit messages. No CHANGELOG — releases are
  `release: vX.Y.Z` commits.
- Do **not** add `Co-Authored-By` trailers to commits.
