# CLAUDE.md — agent orientation for `shove`

This file orients an agent (or a new contributor) to the `shove` crate. For the
human build/test/lint workflow see [`CONTRIBUTING.md`](./CONTRIBUTING.md).

## What this is

`shove` is a type-safe, async pub/sub library over six message backends. The
entire public surface hangs off a single generic `Broker<B>`: you parameterize
it with a backend marker type `B`, and one consistent API (publish, consume,
consumer groups, broadcast, topology, autoscaling) works across every backend.

## Architecture map

- **Generic layer** lives directly in `src/`: `broker.rs`, `publisher.rs`,
  `consumer.rs`, `consumer_group.rs`, `consumer_supervisor.rs`, `broadcast.rs`,
  `batch_consumer.rs`, `autoscaler.rs`, `topology.rs`, `topology_declarer.rs`.
  These are backend-agnostic wrappers around `Broker<B>`.
- **Per-backend implementations** live in `src/backends/<name>/`. Each backend
  has the same file layout: `backend.rs`, `client.rs`, `consumer.rs`,
  `consumer_group.rs`, `publisher.rs`, `topology.rs`, `autoscaler.rs`, plus
  `constants.rs`/`headers.rs`. NATS and Redis put their ephemeral broadcast
  loop in a `broadcast.rs`; Kafka, RabbitMQ and InMemory keep theirs in
  `consumer.rs`.
- The **sealed `Backend` trait** (`src/backend/mod.rs`) binds, for each marker
  type, that backend's client / publisher / consumer / topology / registry
  types. Being sealed means external crates cannot add backends.

### Backends, feature flags, marker types

| Backend | Feature flag | Marker type | Directory under `src/backends/` |
|---|---|---|---|
| RabbitMQ | `rabbitmq` | `RabbitMq` | `rabbitmq` |
| AWS SNS+SQS | `aws-sns-sqs` (publisher-only: `pub-aws-sns`) | `Sqs` | `sns` |
| NATS JetStream | `nats` | `Nats` | `nats` |
| Apache Kafka | `kafka` (+ `kafka-ssl`, `kafka-msk-iam`, `kafka-schema-registry`) | `Kafka` | `kafka` |
| Redis/Valkey Streams | `redis-streams` | `Redis` | `redis` |
| In-process | `inmemory` | `InMemory` | `inmemory` |

The directory is not always the marker type lowercased: the `Sqs` backend lives in
`src/backends/sns/`, because the crate models SNS-publish plus SQS-consume as one
backend. There is no `src/backends/sqs/`.

Other add-on features: `audit`, `metrics`, `protobuf`, `rabbitmq-transactional`.

## Capability gating

Three traits in `src/backend/capability.rs` gate a public entry point to the
backends that have the underlying broker primitive. SQS implements only
`HasBatchConsumption`; the other two are compile errors on `Broker<Sqs>`
rather than runtime surprises.

- `HasCoordinatedGroups` gates `Broker::consumer_group`. Kafka, RabbitMQ, NATS,
  InMemory and Redis implement it; SQS instead uses `ConsumerSupervisor`
  (`src/consumer_supervisor.rs`), which is N parallel independent pollers.
- `HasBroadcast` gates `Broker::broadcast_subscriber` — each process gets its
  own ephemeral subscription, so every instance receives every message. Kafka,
  RabbitMQ, NATS, InMemory and Redis implement it. SQS is excluded
  **permanently**, not pending: per-process fan-out there needs a real queue
  plus an SNS subscription whose lifecycle shove does not manage, and a leaked
  queue costs money forever.
- `HasBatchConsumption` gates `Broker::batch_consumer` /
  `BatchConsumer<B>::run`. Kafka, InMemory, Redis, RabbitMQ and SQS implement
  it today (SQS with a hard 10-message cap — `max_batch_size > 10` is
  rejected at consumer startup); every other backend is pending, not excluded
  — each gets the capability the moment its own `BatchConsumerImpl` lands. The
  primitive exists for **handler amortisation** (one flush per N messages
  instead of one call per message), nothing else.

The trait's own doc comment is the authoritative per-backend list — update it
there rather than restating the table in a third place.

## Where things live

- **Typed-topic macros** `define_topic!` / `define_sequenced_topic!` — in
  `src/macros.rs` (re-exported from `src/lib.rs`).
- **Outcome / retry semantics** — `src/outcome.rs` (the `Outcome` enum:
  `Ack` / `Retry` / `Reject` / `Defer`) and `src/retry.rs`.
- **Per-backend retry/DLQ routing** — each backend's
  `src/backends/<name>/consumer.rs::route_outcome`. This is where a
  delivery-semantics decision is turned into ack/commit/publish/DLQ mechanics.
  Two backends split the mechanics out into a helper module the routing calls
  into — `src/backends/rabbitmq/router.rs` and `src/backends/sns/router.rs` —
  so a routing change there means both files, not just `consumer.rs`.
- **Broadcast outcome settling** — `src/backend/broadcast.rs::settle_broadcast_outcome`,
  shared by the Kafka, NATS and Redis broadcast loops. It does **not** go
  through `route_outcome`, so a retry/discard fix applied to every backend's
  `route_outcome` still misses broadcast. RabbitMQ settles broadcast through
  its own router (an AMQP delivery must be nacked on the channel it arrived
  on), and InMemory reuses its own `route_outcome` over a private buffer.
- **Batch consumption / settlement** — `src/backend/batch_consumer.rs`: the
  classifier every backend's batch flush routes an `Outcome` through, plus the
  panic/timeout invariant surface (`invoke_batch_handler`) shared by every
  `HasBatchConsumption` backend. Kafka's *single-message* async-commit reject
  path also imports this module's deferred-settlement machinery
  (`TerminalDiscard`, `RejectSettlement`, `reject_settlement`) — not only its
  batch path — so a cleanup narrowing that trio's gate must check the
  single-message caller first, per the module's own doc comment.

## Build / test / lint commands

- **Fast path** (no Docker, no secrets): `cargo nextest run --no-default-features`
  runs the lib unit tests plus the feature-free integration targets, among
  them `tests/chartgen.rs` (the benchmark chart generator, including the
  byte-compare of the committed SVGs against the committed results document).
  `default = []` in `Cargo.toml`, and every in-memory test file is gated on
  `feature = "inmemory"`, so under this flag the backend test binaries compile
  empty and report green having run nothing. (CI's `check` job uses
  `cargo test --no-default-features` — the same set; this repo's convention is
  `cargo nextest run`.)
- **Benchmarks**: `scripts/bench.sh <backend>` runs the pinned matrix for one
  backend into `benches/results/bench-results.json`, and `scripts/bench.sh
  charts` regenerates the SVGs and runs the byte-compare test. The runbook is
  `benches/README.md`; the matrix lives only in the script's `MATRIX` array.
  The consume flows are measured two ways, and every row and failure of
  those flows records which (`method`): a **drain** (`--drain-messages`), the
  corpus published before any consumer starts and the rate measured from the
  readiness barrier to nine tenths consumed with no producer in the window,
  which is the throughput ceiling the charts publish; and the **offered-load
  ladder** (`--load-rates`), a paced producer holding a rate while the
  consumers run, which is where the dispatch-latency percentiles come from.
  A drain row carries a `drain` account, a rung a `load` account, and on
  neither is `throughput_msg_per_sec` a corpus over a whole drain.
- **In-memory tests, still Docker-free**:
  `cargo nextest run --features inmemory,metrics,sbe,env-config` — the CI
  `coverage` matrix's `inmemory` feature set (authoritative copy in
  `.github/workflows/ci.yml`). `metrics` and `env-config` are load-bearing:
  the `tests/metrics_inmemory_*.rs` and env-schema tests are gated on them
  and compile to zero tests without them.
- **Integration tests** require Docker (real brokers via `testcontainers`). The
  AWS/MSK tests additionally require `LOCALSTACK_AUTH_TOKEN`, supplied via
  `dotenvx run -- cargo nextest run --features <set>`. See `CONTRIBUTING.md`
  for per-backend feature sets.
- **Lint gates CI enforces**:
  - `cargo fmt -- --check`
  - `cargo clippy --all-features --all-targets -- -D warnings`
  - `cargo clippy --no-default-features --all-targets -- -D warnings`
  - `cargo clippy --lib --no-default-features --features <set> -- -D warnings`
    for each single-backend feature set (the `feature-lint` job). The two gates
    above can both be clean while one of these is not: an item whose callers
    are all behind a feature gate that set does not enable is dead code there
    and nowhere else.
- **Never** run tests with plain `cargo test` if you can use `cargo nextest run`;
  do not pass `-q`/`--quiet` to `nextest` (it rejects it).

## Conventions for changes

- **Cross-backend consistency**: a delivery-semantics bug (retry boundary, DLQ
  routing, defer handling) usually must be fixed in **all** backends'
  `route_outcome`, not just one. The git history shows recurring
  "cross-backend consistency" commits because a fix landed in one backend and
  was missed in the others. When you touch retry/DLQ logic, check every backend
  — and the two settling paths above that bypass `route_outcome`: broadcast
  (`settle_broadcast_outcome`) and batch (`settle_batch_outcome` plus each
  backend's own batch-flush arms).
- **Pre-handler drops count toward `messages_discarded_total`**: a message
  dropped before the handler (oversize, undecodable, batch-accumulation drop)
  settles a discard under the same rules as a post-handler terminal outcome —
  via `metrics::record_terminal` / `pending_discard`, confirmed only against a
  broker-acknowledged retirement, counted iff the message is truly gone (no
  DLQ, or the DLQ publish failed). A batch path parks its drops and settles
  them on the flush's commit result. New consume paths (including the batch
  ports) wire this from day one; the per-backend completeness statement in
  `src/metrics.rs` (`record_terminal`'s doc) is authoritative and must stay
  accurate.
- **Conventional Commits** for commit messages. No CHANGELOG — releases are
  `release: vX.Y.Z` commits.
- Do **not** add `Co-Authored-By` trailers to commits.
