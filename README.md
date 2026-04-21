# shove

[![ci](https://github.com/zannis/shove/actions/workflows/ci.yml/badge.svg)](https://github.com/zannis/shove/actions/workflows/ci.yml)
[![Latest Version](https://img.shields.io/crates/v/shove.svg)](https://crates.io/crates/shove)
[![Docs](https://docs.rs/shove/badge.svg)](https://docs.rs/shove)
[![License:MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage](https://codecov.io/gh/zannis/shove/branch/main/graph/badge.svg)](https://codecov.io/gh/zannis/shove)

Type-safe async pub/sub for Rust on top of RabbitMQ, AWS SNS/SQS, NATS JetStream, Apache Kafka, or an in-process broker.

`shove` is for workloads where "just use the broker client" stops being enough: retries with backoff, DLQs, ordered delivery, consumer groups, autoscaling, and auditability. You define a topic once as a Rust type and the crate derives the messaging topology and runtime behavior from it. Pick the transport that fits your stack — RabbitMQ, SNS/SQS, NATS JetStream, Kafka, or the zero-dependency in-process backend for tests and single-process apps — and get the same high-level API everywhere.

## Why shove

- **Typed topics** — define a topic once as a Rust type; queue names, DLQs, and hold queues all derive from it.
- **Retry topologies without glue code** — escalating backoff through hold queues, DLQ routing, retry budgets, handler timeouts.
- **Strict per-key ordering** — `SequencedTopic` with pluggable failure policies (`Skip` or `FailAll`), enforced by the broker.
- **Consumer groups + autoscaling** — min/max bounds driven by queue depth (or consumer lag on Kafka), with optional structured audit trails.
- **One API across five backends** — swap the transport without changing topic definitions or handlers.

If you have one queue, one consumer, and little retry logic, use `lapin`, the AWS SDK, `async-nats`, or `rdkafka` directly. `shove` is the layer for multi-service event flows that need operational discipline.

## The `Broker<B>` pattern

Every backend is reached through the same generic entry point:

```text
Broker<B>
   ├─ .topology()             → TopologyDeclarer<B>
   ├─ .publisher().await      → Publisher<B>
   ├─ .consumer_supervisor()  → ConsumerSupervisor<B>        (all backends)
   └─ .consumer_group()       → ConsumerGroup<B>             (RabbitMQ, NATS, Kafka, InMemory)
```

`B` is a zero-sized marker (`RabbitMq`, `Sqs`, `Nats`, `Kafka`, `InMemory`) that binds a backend's client/publisher/consumer/topology types together. Swap `B` — the topic definitions, handlers, and call sites stay identical.

SQS has no broker-level coordinated-group primitive; `Broker<Sqs>` exposes `consumer_supervisor()` only. The other four backends expose both: `consumer_supervisor()` for manual per-topic fan-out, `consumer_group()` for min/max-bounded coordinated groups with autoscaling.

## 30-second tour

No Docker, no credentials, no config — this runs against the in-process backend:

```rust,no_run
use serde::{Deserialize, Serialize};
use shove::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
use shove::{
    Broker, ConsumerGroupConfig, InMemory, MessageHandler, MessageMetadata, Outcome,
    TopologyBuilder, define_topic,
};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderPaid { order_id: String }

define_topic!(Orders, OrderPaid,
    TopologyBuilder::new("orders")
        .hold_queue(Duration::from_secs(5))  // retry with backoff
        .dlq()                               // dead-letter on permanent failure
        .build());

struct Handler;
impl MessageHandler<Orders> for Handler {
    type Context = ();
    async fn handle(&self, msg: OrderPaid, _: MessageMetadata, _: &()) -> Outcome {
        println!("paid: {}", msg.order_id);
        Outcome::Ack
    }
}

# async fn run() -> Result<(), shove::ShoveError> {
let broker = Broker::<InMemory>::new(InMemoryConfig::default()).await?;
broker.topology().declare::<Orders>().await?;

let publisher = broker.publisher().await?;
publisher.publish::<Orders>(&OrderPaid { order_id: "ORD-1".into() }).await?;

let mut group = broker.consumer_group();
group
    .register::<Orders, _>(
        ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=1)),
        || Handler,
    )
    .await?;

let outcome = group
    .run_until_timeout(tokio::signal::ctrl_c().map(drop), Duration::from_secs(5))
    .await;
std::process::exit(outcome.exit_code());
# }
# use futures::FutureExt as _;
```

Swap `InMemory` for `RabbitMq`, `Sqs`, `Nats`, or `Kafka` — the topic definition and handler stay identical.

## Pick your backend

| Backend        | Feature flag    | Marker      | Durability         | Ordering primitive                    | Autoscale signal       | Ops burden            |
|----------------|-----------------|-------------|--------------------|---------------------------------------|------------------------|-----------------------|
| RabbitMQ       | `rabbitmq`      | `RabbitMq`  | Disk               | Consistent-hash exchange + SAC shards | Queue depth (mgmt API) | Broker + mgmt plugin  |
| AWS SNS/SQS    | `aws-sns-sqs`   | `Sqs`       | Managed (AWS)      | FIFO topic + `MessageGroupId`         | Queue depth            | Managed — no infra    |
| NATS JetStream | `nats`          | `Nats`      | Disk (JetStream)   | Subject shard + `max_ack_pending=1`   | Pending messages       | NATS server           |
| Apache Kafka   | `kafka`         | `Kafka`     | Disk (log)         | Partition key                         | Consumer lag           | Kafka cluster         |
| In-process     | `inmemory`      | `InMemory`  | None (process RAM) | Per-key FIFO shards                   | Queue depth (in-proc)  | None                  |

Rules of thumb:

- **Prototyping, tests, single-process apps** → `inmemory`
- **Already on AWS, don't want to run infra** → `aws-sns-sqs`
- **Low-latency streaming, high throughput, replay** → `kafka`
- **Complex routing + retry topologies, existing RabbitMQ** → `rabbitmq`
- **Lightweight edge deployments, JetStream already in-stack** → `nats`

## Features

No features are enabled by default.

```sh
# RabbitMQ publisher + consumer + consumer groups + autoscaling
cargo add shove --features rabbitmq

# Transactional RabbitMQ subscribers (exactly-once routing, slower)
cargo add shove --features rabbitmq-transactional

# SNS publisher only
cargo add shove --features pub-aws-sns

# Full AWS SNS + SQS backend
cargo add shove --features aws-sns-sqs

# NATS JetStream
cargo add shove --features nats

# Apache Kafka
cargo add shove --features kafka

# In-memory broker
cargo add shove --features inmemory

# Optional built-in audit publisher
cargo add shove --features rabbitmq,audit
```

| Feature | What it enables                                                                        |
|---|----------------------------------------------------------------------------------------|
| `rabbitmq` | RabbitMQ publisher, consumer, topology declaration, consumer groups, autoscaling       |
| `rabbitmq-transactional` | RabbitMQ exactly-once routing via AMQP transactions                                    |
| `pub-aws-sns` | SNS publisher and topology declaration                                                 |
| `aws-sns-sqs` | Full SNS + SQS publisher/consumer stack, consumer groups, autoscaling                  |
| `nats` | NATS JetStream publisher, consumer, topology declaration, consumer groups, autoscaling |
| `kafka` | Kafka publisher, consumer, topology declaration, consumer groups, autoscaling          |
| `inmemory` | In-memory broker — publisher, consumer, topology, consumer groups, autoscaling         |
| `audit` | Built-in `ShoveAuditHandler` and `AuditLog` topic                                      |

## Quick start

Every non-`inmemory` example spins up its broker on demand via [testcontainers](https://crates.io/crates/testcontainers) — you just need a running Docker daemon. The SQS/SNS examples additionally need a LocalStack Pro auth token (`LOCALSTACK_AUTH_TOKEN`). The `inmemory` backend needs nothing at all.

Define the topic and handler once — this is identical across every backend:

```rust,no_run
use serde::{Deserialize, Serialize};
use shove::{MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SettlementEvent {
    order_id: String,
    amount_cents: u64,
}

define_topic!(
    OrderSettlement,
    SettlementEvent,
    TopologyBuilder::new("order-settlement")
        .hold_queue(Duration::from_secs(5))
        .hold_queue(Duration::from_secs(30))
        .hold_queue(Duration::from_secs(120))
        .dlq()
        .build()
);

struct SettlementHandler;

impl MessageHandler<OrderSettlement> for SettlementHandler {
    type Context = ();
    async fn handle(&self, msg: SettlementEvent, _meta: MessageMetadata, _: &()) -> Outcome {
        // ... your business logic
        Outcome::Ack
    }
}
```

Then pick the transport. Every snippet below uses the same `Broker<B>` pattern.

<details>
<summary><strong>RabbitMQ</strong></summary>

```rust,no_run
use shove::rabbitmq::RabbitMqConfig;
use shove::{Broker, ConsumerGroupConfig, RabbitMq};
use shove::rabbitmq::ConsumerGroupConfig as RabbitMqGroupConfig;
use std::time::Duration;

# async fn run() -> Result<(), shove::ShoveError> {
let broker = Broker::<RabbitMq>::new(RabbitMqConfig::new("amqp://guest:guest@localhost:5672")).await?;

broker.topology().declare::<OrderSettlement>().await?;

let publisher = broker.publisher().await?;
publisher.publish::<OrderSettlement>(&event).await?;

let mut group = broker.consumer_group();
group
    .register::<OrderSettlement, _>(
        ConsumerGroupConfig::new(RabbitMqGroupConfig::new(1..=4).with_prefetch_count(20)),
        || SettlementHandler,
    )
    .await?;

let outcome = group
    .run_until_timeout(tokio::signal::ctrl_c().map(drop), Duration::from_secs(30))
    .await;
# Ok(()) }
# use futures::FutureExt as _;
# use shove::{MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct SettlementEvent { order_id: String, amount_cents: u64 }
# define_topic!(OrderSettlement, SettlementEvent, TopologyBuilder::new("x").build());
# struct SettlementHandler;
# impl MessageHandler<OrderSettlement> for SettlementHandler {
#     type Context = ();
#     async fn handle(&self, _: SettlementEvent, _: MessageMetadata, _: &()) -> Outcome { Outcome::Ack }
# }
# let event = SettlementEvent { order_id: "x".into(), amount_cents: 0 };
```

</details>

<details>
<summary><strong>AWS SNS + SQS</strong></summary>

SQS runs N independent pollers on one queue — there is no broker-level coordinated group. Use `consumer_supervisor()` instead of `consumer_group()`.

```rust,no_run
use shove::sns::SnsConfig;
use shove::{Broker, ConsumerOptions, Sqs};
use std::time::Duration;

# async fn run() -> Result<(), shove::ShoveError> {
let broker = Broker::<Sqs>::new(SnsConfig {
    region: Some("us-east-1".into()),
    endpoint_url: Some("http://localhost:4566".into()), // omit for real AWS
}).await?;

broker.topology().declare::<OrderSettlement>().await?;

let publisher = broker.publisher().await?;
publisher.publish::<OrderSettlement>(&event).await?;

let mut supervisor = broker.consumer_supervisor();
supervisor.register::<OrderSettlement, _>(
    SettlementHandler,
    ConsumerOptions::<Sqs>::preset(10),
)?;

let outcome = supervisor
    .run_until_timeout(tokio::signal::ctrl_c().map(drop), Duration::from_secs(30))
    .await;
# Ok(()) }
# use futures::FutureExt as _;
# use shove::{MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct SettlementEvent { order_id: String, amount_cents: u64 }
# define_topic!(OrderSettlement, SettlementEvent, TopologyBuilder::new("x").build());
# struct SettlementHandler;
# impl MessageHandler<OrderSettlement> for SettlementHandler {
#     type Context = ();
#     async fn handle(&self, _: SettlementEvent, _: MessageMetadata, _: &()) -> Outcome { Outcome::Ack }
# }
# let event = SettlementEvent { order_id: "x".into(), amount_cents: 0 };
```

</details>

<details>
<summary><strong>NATS JetStream</strong></summary>

```rust,no_run
use shove::nats::{NatsConfig, NatsConsumerGroupConfig};
use shove::{Broker, ConsumerGroupConfig, Nats};
use std::time::Duration;

# async fn run() -> Result<(), shove::ShoveError> {
let broker = Broker::<Nats>::new(NatsConfig::new("nats://localhost:4222")).await?;

broker.topology().declare::<OrderSettlement>().await?;

let publisher = broker.publisher().await?;
publisher.publish::<OrderSettlement>(&event).await?;

let mut group = broker.consumer_group();
group
    .register::<OrderSettlement, _>(
        ConsumerGroupConfig::new(NatsConsumerGroupConfig::new(1..=4)),
        || SettlementHandler,
    )
    .await?;

let outcome = group
    .run_until_timeout(tokio::signal::ctrl_c().map(drop), Duration::from_secs(30))
    .await;
# Ok(()) }
# use futures::FutureExt as _;
# use shove::{MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct SettlementEvent { order_id: String, amount_cents: u64 }
# define_topic!(OrderSettlement, SettlementEvent, TopologyBuilder::new("x").build());
# struct SettlementHandler;
# impl shove::MessageHandler<OrderSettlement> for SettlementHandler {
#     type Context = ();
#     async fn handle(&self, _: SettlementEvent, _: shove::MessageMetadata, _: &()) -> shove::Outcome { shove::Outcome::Ack }
# }
# let event = SettlementEvent { order_id: "x".into(), amount_cents: 0 };
```

</details>

<details>
<summary><strong>Apache Kafka</strong></summary>

```rust,no_run
use shove::kafka::{KafkaConfig, KafkaConsumerGroupConfig};
use shove::{Broker, ConsumerGroupConfig, Kafka};
use std::time::Duration;

# async fn run() -> Result<(), shove::ShoveError> {
let broker = Broker::<Kafka>::new(KafkaConfig::new("localhost:9092")).await?;

broker.topology().declare::<OrderSettlement>().await?;

let publisher = broker.publisher().await?;
publisher.publish::<OrderSettlement>(&event).await?;

let mut group = broker.consumer_group();
group
    .register::<OrderSettlement, _>(
        ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=4)),
        || SettlementHandler,
    )
    .await?;

let outcome = group
    .run_until_timeout(tokio::signal::ctrl_c().map(drop), Duration::from_secs(30))
    .await;
# Ok(()) }
# use futures::FutureExt as _;
# use shove::{MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct SettlementEvent { order_id: String, amount_cents: u64 }
# define_topic!(OrderSettlement, SettlementEvent, TopologyBuilder::new("x").build());
# struct SettlementHandler;
# impl shove::MessageHandler<OrderSettlement> for SettlementHandler {
#     type Context = ();
#     async fn handle(&self, _: SettlementEvent, _: shove::MessageMetadata, _: &()) -> shove::Outcome { shove::Outcome::Ack }
# }
# let event = SettlementEvent { order_id: "x".into(), amount_cents: 0 };
```

</details>

<details>
<summary><strong>In-process (no broker, no Docker)</strong></summary>

```rust,no_run
use shove::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
use shove::{Broker, ConsumerGroupConfig, InMemory};
use std::time::Duration;

# async fn run() -> Result<(), shove::ShoveError> {
let broker = Broker::<InMemory>::new(InMemoryConfig::default()).await?;

broker.topology().declare::<OrderSettlement>().await?;

let publisher = broker.publisher().await?;
publisher.publish::<OrderSettlement>(&event).await?;

let mut group = broker.consumer_group();
group
    .register::<OrderSettlement, _>(
        ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=1)),
        || SettlementHandler,
    )
    .await?;

let outcome = group
    .run_until_timeout(tokio::signal::ctrl_c().map(drop), Duration::from_secs(5))
    .await;
# Ok(()) }
# use futures::FutureExt as _;
# use shove::{MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct SettlementEvent { order_id: String, amount_cents: u64 }
# define_topic!(OrderSettlement, SettlementEvent, TopologyBuilder::new("x").build());
# struct SettlementHandler;
# impl shove::MessageHandler<OrderSettlement> for SettlementHandler {
#     type Context = ();
#     async fn handle(&self, _: SettlementEvent, _: shove::MessageMetadata, _: &()) -> shove::Outcome { shove::Outcome::Ack }
# }
# let event = SettlementEvent { order_id: "x".into(), amount_cents: 0 };
```

Messages live only in the broker process and are dropped on shutdown — use a durable backend for anything production.

</details>

## Ergonomics

A handful of helpers keep call sites short:

- **`MessageHandlerExt::audited`** — fluent audit wrapping. Write `SettlementHandler.audited(my_audit)` instead of `Audited::new(SettlementHandler, my_audit)`. The wrapped handler is a drop-in replacement that accepts the same `ConsumerGroup`/`ConsumerSupervisor` registration path.
- **`broker.topology().declare_all::<(A, B, C)>()`** — declare multiple topics in one call via tuple arities 1 through 16, instead of three separate `declare::<_>()` awaits.
- **`ConsumerOptions::<B>::preset(prefetch)`** — shorthand for `ConsumerOptions::<B>::new().with_prefetch_count(prefetch)`. Composes with the other `with_*` builders.
- **`SupervisorOutcome::exit_code()`** — canonical process exit code from a consumer group or supervisor: `0` clean, `1` any handler error, `2` any task panic, `3` drain timeout. Call `std::process::exit(outcome.exit_code())` for a uniform shutdown contract across long-running consumer binaries.

```rust,no_run
use shove::MessageHandlerExt;
# use shove::{MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic,
#             AuditHandler, AuditRecord};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct SettlementEvent { order_id: String }
# define_topic!(OrderSettlement, SettlementEvent, TopologyBuilder::new("x").build());
# struct SettlementHandler;
# impl MessageHandler<OrderSettlement> for SettlementHandler {
#     type Context = ();
#     async fn handle(&self, _: SettlementEvent, _: MessageMetadata, _: &()) -> Outcome { Outcome::Ack }
# }
# struct MyAudit;
# impl AuditHandler<OrderSettlement> for MyAudit {
#     async fn audit(&self, _: &AuditRecord<SettlementEvent>) -> Result<(), shove::ShoveError> { Ok(()) }
# }

// `SettlementHandler.audited(MyAudit)` == `Audited::new(SettlementHandler, MyAudit)`.
let handler = SettlementHandler.audited(MyAudit);
```

## Delivery model

`shove` is at-least-once by default. That means handlers should be idempotent.

- `Outcome::Ack`: success
- `Outcome::Retry`: delayed retry through hold queues, with escalating backoff
- `Outcome::Reject`: dead-letter immediately
- `Outcome::Defer`: delay without increasing retry count

Additional behavior:

- Handler timeouts automatically convert to retry
- DLQ consumers receive `DeadMessageMetadata`
- RabbitMQ publishes a stable `x-message-id` header for deduplication
- RabbitMQ can opt into transactional exactly-once routing with `rabbitmq-transactional`
- NATS uses `Nats-Msg-Id` for deduplication within a 120-second window
- Kafka uses partition-based ordering; retry and DLQ routing is handled via hold/DLQ topics

Exactly-once mode removes the publish-then-ack race in RabbitMQ by wrapping routing decisions in AMQP transactions. It is materially slower and should be reserved for handlers with irreversible side effects.

## Sequenced topics

Use `define_sequenced_topic!` when messages for the same entity must be processed in order.

```rust,no_run
use shove::{SequenceFailure, TopologyBuilder, define_sequenced_topic};
use std::time::Duration;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct LedgerEntry { account_id: String }

define_sequenced_topic!(
    AccountLedger,
    LedgerEntry,
    |msg| msg.account_id.clone(),
    TopologyBuilder::new("account-ledger")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(16)
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);
```

Failure policies:

- `SequenceFailure::Skip`: dead-letter the failed message and continue processing the rest of the sequence
- `SequenceFailure::FailAll`: poison the key and dead-letter all remaining messages for that key

Given messages `[1, 2, 3, 4, 5]` for the same key where message 3 is permanently rejected:

| Policy    | Acked   | DLQed |
|-----------|---------|-------|
| `Skip`    | 1,2,4,5 | 3     |
| `FailAll` | 1,2     | 3,4,5 |

Use `Skip` when messages are independently valid (e.g. audit entries). Use `FailAll` when later messages are causally dependent on earlier ones (e.g. financial ledger entries, state-machine transitions).

Messages for other sequence keys are unaffected by either policy.

RabbitMQ uses consistent-hash routing for this. SNS/SQS uses FIFO topics and queues. NATS uses subject-based shard routing with `max_ack_pending: 1` per shard to enforce strict ordering. Kafka uses partition-key routing — messages with the same sequence key land in the same partition, and the consumer processes one message at a time to guarantee order.

## Consumer groups and autoscaling

All backends with a coordinated-group primitive (RabbitMQ, NATS, Kafka, InMemory) expose `broker.consumer_group()`. SQS uses `broker.consumer_supervisor()` with N parallel pollers instead. Both share the same `run_until_timeout` / `SupervisorOutcome` shutdown contract.

```rust,no_run
use shove::rabbitmq::{ConsumerGroupConfig as RabbitMqGroupConfig, ManagementConfig, RabbitMqConfig};
use shove::autoscaler::AutoscalerConfig;
use shove::{Broker, ConsumerGroupConfig, RabbitMq};
use std::time::Duration;

# async fn run() -> Result<(), shove::ShoveError> {
# use shove::{MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct Task;
# define_topic!(WorkQueue, Task, TopologyBuilder::new("work").build());
# struct TaskHandler;
# impl MessageHandler<WorkQueue> for TaskHandler {
#     type Context = ();
#     async fn handle(&self, _: Task, _: MessageMetadata, _: &()) -> Outcome { Outcome::Ack }
# }

let broker = Broker::<RabbitMq>::new(RabbitMqConfig::default()).await?;
broker.topology().declare::<WorkQueue>().await?;

let mut group = broker.consumer_group();
group
    .register::<WorkQueue, _>(
        ConsumerGroupConfig::new(
            RabbitMqGroupConfig::new(1..=8)
                .with_prefetch_count(10)
                .with_max_retries(5)
                .with_handler_timeout(Duration::from_secs(30))
                .with_concurrent_processing(true),
        ),
        || TaskHandler,
    )
    .await?;

# Ok(()) }
```

The autoscaler harness is wired the same shape on every backend — see `examples/<backend>/consumer_groups.rs` for a full runnable example with `AutoscalerConfig`.

## Audit logging

Wrap any handler with `MessageHandlerExt::audited` (or `Audited::new`) to persist a structured `AuditRecord` for each delivery attempt. Records include the trace id, topic, payload, message metadata, outcome, duration, and timestamp.

Implement `AuditHandler<T>` for your persistence backend:

```rust,no_run
use shove::{AuditHandler, AuditRecord, MessageHandlerExt, ShoveError};
# use shove::{MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct SettlementEvent { order_id: String }
# define_topic!(OrderSettlement, SettlementEvent, TopologyBuilder::new("x").build());
# struct SettlementHandler;
# impl MessageHandler<OrderSettlement> for SettlementHandler {
#     type Context = ();
#     async fn handle(&self, _: SettlementEvent, _: MessageMetadata, _: &()) -> Outcome { Outcome::Ack }
# }

struct MyAuditSink;

impl AuditHandler<OrderSettlement> for MyAuditSink {
    async fn audit(&self, record: &AuditRecord<SettlementEvent>) -> Result<(), ShoveError> {
        println!("{}", serde_json::to_string(record).unwrap());
        Ok(())
    }
}

let handler = SettlementHandler.audited(MyAuditSink);
// Drop-in — consumer groups, supervisors, and FIFO consumers all accept it.
```

If the audit handler returns `Err`, the message is retried — audit failure is never silently dropped.

With the `audit` feature enabled, `ShoveAuditHandler<B>` publishes those records back into the dedicated `shove-audit-log` topic using any broker's `Publisher<B>`:

```rust,no_run
use shove::{MessageHandlerExt, ShoveAuditHandler};
# use shove::{Broker, RabbitMq, MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic};
# use shove::rabbitmq::RabbitMqConfig;
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Clone, Serialize, Deserialize)]
# struct SettlementEvent { order_id: String }
# define_topic!(OrderSettlement, SettlementEvent, TopologyBuilder::new("x").build());
# struct SettlementHandler;
# impl MessageHandler<OrderSettlement> for SettlementHandler {
#     type Context = ();
#     async fn handle(&self, _: SettlementEvent, _: MessageMetadata, _: &()) -> Outcome { Outcome::Ack }
# }
# async fn run() -> Result<(), shove::ShoveError> {
let broker = Broker::<RabbitMq>::new(RabbitMqConfig::default()).await?;
let publisher = broker.publisher().await?;
let audit = ShoveAuditHandler::for_publisher(&publisher);
let handler = SettlementHandler.audited(audit);
# Ok(()) }
```

## Performance

Measured on a MacBook Pro M4 Max, single RabbitMQ node via Docker, Rust 1.91. Reproducible via `cargo run -q --example rabbitmq_stress --features rabbitmq`.

| Handler         | 1 worker, prefetch=1 | 1 worker, prefetch=20 | 8 workers, prefetch=20 | 32 workers, prefetch=40 |
|-----------------|----------------------|-----------------------|------------------------|-------------------------|
| Fast (1–5 ms)   | 179 msg/s            | 2,866 msg/s           | 19,669 msg/s           | 29,207 msg/s            |
| Slow (50–300 ms)| 6 msg/s              | 75 msg/s              | 544 msg/s              | 4,076 msg/s             |
| Heavy (1–5 s)   | 0.4 msg/s            | 5 msg/s               | 21 msg/s               | 199 msg/s               |

`prefetch_count` is the primary throughput lever for I/O-bound handlers. Adding workers scales linearly when the handler is the bottleneck. Results will vary by hardware and broker configuration.

## Examples

Run any with `cargo run --example <name> --features <flag>`:

```sh
cargo run --example inmemory_basic --features inmemory
cargo run --example rabbitmq_exactly_once --features rabbitmq-transactional
```

| Backend         | Feature flag              | Examples |
|-----------------|---------------------------|----------|
| RabbitMQ        | `rabbitmq`                | `rabbitmq_basic_pubsub`, `rabbitmq_concurrent_pubsub`, `rabbitmq_sequenced_pubsub`, `rabbitmq_consumer_groups`, `rabbitmq_audited_consumer`, `rabbitmq_stress` |
| RabbitMQ (tx)   | `rabbitmq-transactional`  | `rabbitmq_exactly_once` |
| AWS SNS/SQS     | `aws-sns-sqs`             | `sqs_basic_pubsub`, `sqs_concurrent_pubsub`, `sqs_sequenced_pubsub`, `sqs_consumer_groups`, `sqs_audited_consumer`, `sqs_autoscaler`, `sqs_stress` |
| NATS JetStream  | `nats`                    | `nats_basic`, `nats_sequenced`, `nats_audited_consumer`, `nats_stress` |
| Apache Kafka    | `kafka`                   | `kafka_basic`, `kafka_sequenced`, `kafka_audited_consumer`, `kafka_stress` |
| In-process      | `inmemory`                | `inmemory_basic`, `inmemory_sequenced`, `inmemory_consumer_groups`, `inmemory_audited_consumer`, `inmemory_stress` |

The `audit` feature is required on top of the backend flag for any `*_audited_consumer` example.

## API reference

Full rustdoc is on [docs.rs/shove](https://docs.rs/shove):

- [`Broker`](https://docs.rs/shove/latest/shove/struct.Broker.html), [`Publisher`](https://docs.rs/shove/latest/shove/struct.Publisher.html), [`TopologyDeclarer`](https://docs.rs/shove/latest/shove/struct.TopologyDeclarer.html)
- [`ConsumerSupervisor`](https://docs.rs/shove/latest/shove/struct.ConsumerSupervisor.html), [`ConsumerGroup`](https://docs.rs/shove/latest/shove/struct.ConsumerGroup.html), [`SupervisorOutcome`](https://docs.rs/shove/latest/shove/struct.SupervisorOutcome.html)
- [`Outcome`](https://docs.rs/shove/latest/shove/enum.Outcome.html) — what a handler returns (`Ack`, `Retry`, `Reject`, `Defer`)
- [`TopologyBuilder`](https://docs.rs/shove/latest/shove/struct.TopologyBuilder.html) — `.hold_queue`, `.sequenced`, `.routing_shards`, `.dlq`
- [`ConsumerOptions`](https://docs.rs/shove/latest/shove/struct.ConsumerOptions.html), [`MessageHandler`](https://docs.rs/shove/latest/shove/trait.MessageHandler.html), [`MessageHandlerExt`](https://docs.rs/shove/latest/shove/trait.MessageHandlerExt.html)
- Per-backend modules: [`rabbitmq`](https://docs.rs/shove/latest/shove/rabbitmq/), [`sns`](https://docs.rs/shove/latest/shove/sns/), [`nats`](https://docs.rs/shove/latest/shove/nats/), [`kafka`](https://docs.rs/shove/latest/shove/kafka/), [`inmemory`](https://docs.rs/shove/latest/shove/inmemory/)

Backend-specific sequenced-delivery mapping: RabbitMQ uses a consistent-hash exchange with SAC shards. SNS/SQS uses FIFO topics + `MessageGroupId`. NATS uses subject-based shard routing with `max_ack_pending: 1`. Kafka uses partition-key routing. In-process uses per-key FIFO shards (ordering enforced in-memory, no persistence, no cross-process delivery).

## Requirements

`shove` requires Rust 1.85 or newer (edition 2024).

## Background

`shove` came out of production event-processing systems that needed more than a broker client but less than a platform rewrite. The crate focuses on the hard parts around message handling correctness and operational behavior, while leaving transport and persistence to RabbitMQ, SNS/SQS, NATS JetStream, or Kafka.

The API is still evolving.

## License

[MIT](LICENSE)
