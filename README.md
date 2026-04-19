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

## 30-second tour

No Docker, no credentials, no config — this runs against the in-process backend:

```rust,no_run
use serde::{Deserialize, Serialize};
use shove::inmemory::*;
use shove::*;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderPaid { order_id: String }

define_topic!(Orders, OrderPaid,
    TopologyBuilder::new("orders")
        .hold_queue(Duration::from_secs(5))  // retry with backoff
        .dlq()                               // dead-letter on permanent failure
        .build());

struct Handler;
impl MessageHandler<Orders> for Handler {
    async fn handle(&self, msg: OrderPaid, _: MessageMetadata) -> Outcome {
        println!("paid: {}", msg.order_id);
        Outcome::Ack
    }
}

# async fn run() -> Result<(), ShoveError> {
let broker = InMemoryBroker::new();
InMemoryTopologyDeclarer::new(broker.clone())
    .declare(Orders::topology()).await?;

InMemoryPublisher::new(broker.clone())
    .publish::<Orders>(&OrderPaid { order_id: "ORD-1".into() }).await?;

InMemoryConsumer::new(broker)
    .run::<Orders>(Handler, ConsumerOptions::new(CancellationToken::new())).await?;
# Ok(()) }
```

Swap `InMemory*` for `RabbitMq*`, `Sns*`, `Nats*`, or `Kafka*` — the topic definition and handler stay identical.

## Pick your backend

| Backend        | Feature flag    | Durability        | Ordering primitive                   | Autoscale signal       | Ops burden            |
|----------------|-----------------|-------------------|--------------------------------------|------------------------|-----------------------|
| RabbitMQ       | `rabbitmq`      | Disk              | Consistent-hash exchange + SAC shards | Queue depth (mgmt API) | Broker + mgmt plugin  |
| AWS SNS/SQS    | `aws-sns-sqs`   | Managed (AWS)     | FIFO topic + `MessageGroupId`         | Queue depth            | Managed — no infra    |
| NATS JetStream | `nats`          | Disk (JetStream)  | Subject shard + `max_ack_pending=1`   | Pending messages       | NATS server           |
| Apache Kafka   | `kafka`         | Disk (log)        | Partition key                        | Consumer lag           | Kafka cluster         |
| In-process     | `inmemory`      | None (process RAM) | Per-key FIFO shards                  | Queue depth (in-proc)  | None                  |

Rules of thumb:

- **Prototyping, tests, single-process apps** → `inmemory`
- **Already on AWS, don't want to run infra** → `aws-sns-sqs`
- **Low-latency streaming, high throughput, replay** → `kafka`
- **Complex routing + retry topologies, existing RabbitMQ** → `rabbitmq`
- **Lightweight edge deployments, JetStream already in-stack** → `nats`

All backends support typed topics, retry/DLQ, sequenced delivery, consumer groups, autoscaling, and audit — the trait surface is identical.

## Features

No features are enabled by default.

```sh
# RabbitMQ publisher + consumer + consumer groups + autoscaling
cargo add shove --features rabbitmq

# Transactional RabbitMQ subscribers with exactly-once delivery guarantees (use only when absolutely necessary)
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

The repo's `docker-compose.yml` starts every broker needed below: RabbitMQ with the consistent-hash plugin, LocalStack (SNS + SQS on `http://localhost:4566`), NATS JetStream on `localhost:4222`, and Kafka on `localhost:9092`. The `inmemory` backend needs nothing.

```sh
docker compose up -d
```

Define the topic and handler once — this is identical across every backend:

```rust,no_run
use serde::{Deserialize, Serialize};
use shove::*;
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
    async fn handle(&self, msg: SettlementEvent, _metadata: MessageMetadata) -> Outcome {
        match process(&msg).await {
            Ok(()) => Outcome::Ack,
            Err(e) if e.is_transient() => Outcome::Retry,
            Err(_) => Outcome::Reject,
        }
    }
}
```

Then pick the transport:

<details>
<summary><strong>RabbitMQ</strong></summary>

```rust,no_run
use shove::rabbitmq::*;
use tokio_util::sync::CancellationToken;

let client = RabbitMqClient::connect(&RabbitMqConfig::new("amqp://localhost")).await?;

// Publish
let publisher = RabbitMqPublisher::new(client.clone()).await?;
publisher.publish::<OrderSettlement>(&event).await?;

// Consume
let shutdown = CancellationToken::new();
let consumer = RabbitMqConsumer::new(client);
let options = ConsumerOptions::new(shutdown).with_prefetch_count(20);
consumer.run::<OrderSettlement>(SettlementHandler, options).await?;
```

</details>

<details>
<summary><strong>AWS SNS + SQS</strong></summary>

```rust,no_run
use shove::sns::*;
use std::sync::Arc;

let client = SnsClient::new(&SnsConfig {
    region: Some("us-east-1".into()),
    endpoint_url: Some("http://localhost:4566".into()), // omit for real AWS
}).await?;

let topic_registry = Arc::new(TopicRegistry::new());
let queue_registry = Arc::new(QueueRegistry::new());

let declarer = SnsTopologyDeclarer::new(client.clone(), topic_registry.clone())
    .with_queue_registry(queue_registry.clone());
declare_topic::<OrderSettlement>(&declarer).await?;

// Publish
let publisher = SnsPublisher::new(client.clone(), topic_registry);
publisher.publish::<OrderSettlement>(&event).await?;

// Consume
let consumer = SqsConsumer::new(client, queue_registry);
consumer.run::<OrderSettlement>(SettlementHandler, ConsumerOptions::new(shutdown)).await?;
```

</details>

<details>
<summary><strong>NATS JetStream</strong></summary>

```rust,no_run
use shove::nats::*;

let client = NatsClient::connect(&NatsConfig::new("nats://localhost:4222")).await?;
NatsTopologyDeclarer::new(client.clone())
    .declare(OrderSettlement::topology()).await?;

// Publish
NatsPublisher::new(client.clone()).await?
    .publish::<OrderSettlement>(&event).await?;

// Consume
NatsConsumer::new(client)
    .run::<OrderSettlement>(SettlementHandler, ConsumerOptions::new(shutdown)).await?;
```

</details>

<details>
<summary><strong>Apache Kafka</strong></summary>

```rust,no_run
use shove::kafka::*;

let client = KafkaClient::connect(&KafkaConfig::new("localhost:9092")).await?;
KafkaTopologyDeclarer::new(client.clone())
    .declare(OrderSettlement::topology()).await?;

// Publish
KafkaPublisher::new(client.clone()).await?
    .publish::<OrderSettlement>(&event).await?;

// Consume
KafkaConsumer::new(client)
    .run::<OrderSettlement>(SettlementHandler, ConsumerOptions::new(shutdown)).await?;
```

</details>

<details>
<summary><strong>In-process (no broker, no Docker)</strong></summary>

```rust,no_run
use shove::inmemory::*;

let broker = InMemoryBroker::new();
InMemoryTopologyDeclarer::new(broker.clone())
    .declare(OrderSettlement::topology()).await?;

// Publish
InMemoryPublisher::new(broker.clone())
    .publish::<OrderSettlement>(&event).await?;

// Consume
InMemoryConsumer::new(broker)
    .run::<OrderSettlement>(SettlementHandler, ConsumerOptions::new(shutdown)).await?;
```

Messages live only in the broker process and are dropped on shutdown — use a durable backend for anything production.

</details>

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
use shove::*;
use std::time::Duration;

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

consumer.run_fifo::<AccountLedger>(handler, options).await?;
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

All backends support named consumer groups with min/max bounds, per-consumer prefetch, retry budget, handler timeouts, and optional concurrent processing inside each consumer.

RabbitMQ:

```rust,no_run
use shove::rabbitmq::*;
use shove::autoscaler::AutoscalerConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

let mut registry = ConsumerGroupRegistry::new(client.clone());

registry
    .register::<WorkQueue, TaskHandler>(
        ConsumerGroupConfig::new(1..=8)
            .with_prefetch_count(10)
            .with_max_retries(5)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true),
        || TaskHandler,
    )
    .await?;

registry.start_all();

let registry = Arc::new(Mutex::new(registry));

let mgmt = ManagementConfig::new("http://localhost:15672", "guest", "guest");
let mut autoscaler = RabbitMqAutoscalerBackend::autoscaler(
    &mgmt,
    registry.clone(),
    AutoscalerConfig {
        poll_interval: Duration::from_secs(2),
        scale_up_multiplier: 1.5,
        scale_down_multiplier: 0.3,
        hysteresis_duration: Duration::from_secs(4),
        cooldown_duration: Duration::from_secs(8),
    },
);
autoscaler.run(shutdown_token).await;
```

Every other backend has an identical shape — swap `ConsumerGroupRegistry` / `RabbitMqAutoscalerBackend` for `SqsConsumerGroupRegistry` / `SqsAutoscalerBackend`, `NatsConsumerGroupRegistry` / `NatsAutoscalerBackend`, `KafkaConsumerGroupRegistry` / `KafkaAutoscalerBackend`, or `InMemoryConsumerGroupRegistry` / `InMemoryAutoscalerBackend`. Config constructors (`NatsConsumerGroupConfig::new`, `KafkaConsumerGroupConfig::new`, …) carry the same builder methods.

## Audit logging

Wrap any handler with `Audited<H, A>` to persist a structured `AuditRecord` for each delivery attempt. Records include the trace id, topic, payload, message metadata, outcome, duration, and timestamp.

Implement `AuditHandler<T>` for your persistence backend:

```rust,no_run
use shove::*;

struct MyAuditSink;

impl AuditHandler<OrderSettlement> for MyAuditSink {
    async fn audit(&self, record: &AuditRecord<SettlementEvent>) -> Result<(), ShoveError> {
        println!("{}", serde_json::to_string(record).unwrap());
        Ok(())
    }
}

let handler = Audited::new(SettlementHandler, MyAuditSink);

// Drop-in replacement — consumers, consumer groups, and FIFO consumers all accept it.
consumer.run::<OrderSettlement>(handler, ConsumerOptions::new(shutdown)).await?;
```

If the audit handler returns `Err`, the message is retried — audit failure is never silently dropped.

If you enable `audit`, `ShoveAuditHandler` can publish those records back into a dedicated `shove-audit-log` topic using the same broker:

```rust,no_run
use shove::rabbitmq::*;
use shove::*;

let audit = ShoveAuditHandler::new(publisher.clone());
let handler = Audited::new(SettlementHandler, audit);
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

- [`Outcome`](https://docs.rs/shove/latest/shove/enum.Outcome.html) — what a handler returns (`Ack`, `Retry`, `Reject`, `Defer`)
- [`TopologyBuilder`](https://docs.rs/shove/latest/shove/struct.TopologyBuilder.html) — `.hold_queue`, `.sequenced`, `.routing_shards`, `.dlq`
- [`ConsumerOptions`](https://docs.rs/shove/latest/shove/struct.ConsumerOptions.html), [`MessageHandler`](https://docs.rs/shove/latest/shove/trait.MessageHandler.html), [`Publisher`](https://docs.rs/shove/latest/shove/trait.Publisher.html)
- Per-backend modules: [`rabbitmq`](https://docs.rs/shove/latest/shove/rabbitmq/), [`sns`](https://docs.rs/shove/latest/shove/sns/), [`nats`](https://docs.rs/shove/latest/shove/nats/), [`kafka`](https://docs.rs/shove/latest/shove/kafka/), [`inmemory`](https://docs.rs/shove/latest/shove/inmemory/)

Backend-specific sequenced-delivery mapping: RabbitMQ uses a consistent-hash exchange with SAC shards. SNS/SQS uses FIFO topics + `MessageGroupId`. NATS uses subject-based shard routing with `max_ack_pending: 1`. Kafka uses partition-key routing. In-process uses per-key FIFO shards (ordering enforced in-memory, no persistence, no cross-process delivery).

## Requirements

`shove` requires Rust 1.85 or newer (edition 2024).

## Background

`shove` came out of production event-processing systems that needed more than a broker client but less than a platform rewrite. The crate focuses on the hard parts around message handling correctness and operational behavior, while leaving transport and persistence to RabbitMQ, SNS/SQS, NATS JetStream, or Kafka.

The API is still evolving.

## License

[MIT](LICENSE)
