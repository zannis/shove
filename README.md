# shove

[![ci](https://github.com/zannis/shove/actions/workflows/ci.yml/badge.svg)](https://github.com/zannis/shove/actions/workflows/ci.yml)
[![Latest Version](https://img.shields.io/crates/v/shove.svg)](https://crates.io/crates/shove)
[![Docs](https://docs.rs/shove/badge.svg)](https://docs.rs/shove)
[![License:MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage](https://codecov.io/gh/zannis/shove/branch/main/graph/badge.svg)](https://codecov.io/gh/zannis/shove)

Type-safe async pub/sub for Rust on top of RabbitMQ, AWS SNS/SQS, NATS JetStream, or Apache Kafka.

`shove` is for workloads where "just use the broker client" stops being enough: retries with backoff, DLQs, ordered delivery, consumer groups, autoscaling, and auditability. You define a topic once as a Rust type and the crate derives the messaging topology and runtime behavior from it. Pick the transport that fits your stack — RabbitMQ, SNS/SQS, NATS JetStream, or Kafka — and get the same high-level API everywhere.

## Why shove

- Typed topics instead of stringly-typed queue names
- Built-in retry topologies with delayed hold queues and DLQs
- Concurrent consumers with in-order acknowledgements
- Strict per-key ordering for sequenced topics
- Consumer groups with queue-depth autoscaling
- Structured audit trails for every delivery attempt
- RabbitMQ at-least-once by default, with opt-in transactional exactly-once routing
- SNS publisher-only mode or full SNS + SQS consumption stack
- NATS JetStream with per-key shard ordering and queue-depth autoscaling
- Kafka with partition-based ordering, consumer lag autoscaling, and reconnect resilience

If you have one queue, one consumer, and little retry logic, use `lapin`, the AWS SDK, `async-nats`, or `rdkafka` directly. `shove` is the layer for multi-service event flows that need operational discipline.

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

# Optional built-in audit publisher
cargo add shove --features rabbitmq,audit
```

| Feature | What it enables |
|---|---|
| `rabbitmq` | RabbitMQ publisher, consumer, topology declaration, consumer groups, autoscaling |
| `rabbitmq-transactional` | RabbitMQ exactly-once routing via AMQP transactions |
| `pub-aws-sns` | SNS publisher and topology declaration |
| `aws-sns-sqs` | Full SNS + SQS publisher/consumer stack, consumer groups, autoscaling |
| `nats` | NATS JetStream publisher, consumer, topology declaration, consumer groups, autoscaling |
| `kafka` | Kafka publisher, consumer, topology declaration, consumer groups, autoscaling |
| `audit` | Built-in `ShoveAuditHandler` and `AuditLog` topic |

## Quick start

For RabbitMQ, the repo's `docker-compose.yml` starts a local broker with the consistent-hash exchange plugin enabled. For the AWS path, it starts LocalStack with SNS and SQS on `http://localhost:4566`. For NATS, it starts a JetStream-enabled server on `localhost:4222`. For Kafka, it starts a broker on `localhost:9092`.

```sh
docker compose up -d
```

Define a topic once:

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
```

Publish:

```rust,no_run
use shove::rabbitmq::*;

let client = RabbitMqClient::connect(&RabbitMqConfig::new("amqp://localhost")).await?;
let publisher = RabbitMqPublisher::new(client.clone()).await?;

publisher
    .publish::<OrderSettlement>(&SettlementEvent {
        order_id: "ORD-1".into(),
        amount_cents: 5000,
    })
    .await?;
```

Consume:

```rust,no_run
use shove::rabbitmq::*;
use shove::*;
use tokio_util::sync::CancellationToken;

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

let shutdown = CancellationToken::new();
let consumer = RabbitMqConsumer::new(client);
let options = ConsumerOptions::new(shutdown).with_prefetch_count(20);

consumer.run::<OrderSettlement>(SettlementHandler, options).await?;
```

Publish (SNS):

```rust,no_run
use shove::sns::*;
use std::sync::Arc;

let config = SnsConfig {
    region: Some("us-east-1".into()),
    endpoint_url: Some("http://localhost:4566".into()), // omit for real AWS
};
let client = SnsClient::new(&config).await?;

let topic_registry = Arc::new(TopicRegistry::new());
let queue_registry = Arc::new(QueueRegistry::new());

let declarer = SnsTopologyDeclarer::new(client.clone(), topic_registry.clone())
    .with_queue_registry(queue_registry.clone());
declare_topic::<OrderSettlement>(&declarer).await?;

let publisher = SnsPublisher::new(client.clone(), topic_registry.clone());
publisher
    .publish::<OrderSettlement>(&SettlementEvent {
        order_id: "ORD-1".into(),
        amount_cents: 5000,
    })
    .await?;
```

Consume (SQS):

```rust,no_run
use shove::sns::*;

let consumer = SqsConsumer::new(client.clone(), queue_registry.clone());
let options = ConsumerOptions::new(shutdown).with_prefetch_count(20);
consumer.run::<OrderSettlement>(SettlementHandler, options).await?;
```

Publish (NATS):

```rust,no_run
use shove::nats::*;
use shove::publisher::Publisher;
use shove::topology::TopologyDeclarer;

let client = NatsClient::connect(&NatsConfig::new("nats://localhost:4222")).await?;

let declarer = NatsTopologyDeclarer::new(client.clone());
declarer.declare(OrderSettlement::topology()).await?;

let publisher = NatsPublisher::new(client.clone()).await?;
publisher
    .publish::<OrderSettlement>(&SettlementEvent {
        order_id: "ORD-1".into(),
        amount_cents: 5000,
    })
    .await?;
```

Consume (NATS):

```rust,no_run
use shove::nats::*;
use shove::consumer::{Consumer, ConsumerOptions};

let consumer = NatsConsumer::new(client.clone());
let options = ConsumerOptions::new(shutdown).with_prefetch_count(20);
consumer.run::<OrderSettlement>(SettlementHandler, options).await?;
```

Publish (Kafka):

```rust,no_run
use shove::kafka::*;
use shove::publisher::Publisher;
use shove::topology::TopologyDeclarer;

let client = KafkaClient::connect(&KafkaConfig::new("localhost:9092")).await?;

let declarer = KafkaTopologyDeclarer::new(client.clone());
declarer.declare(OrderSettlement::topology()).await?;

let publisher = KafkaPublisher::new(client.clone()).await?;
publisher
    .publish::<OrderSettlement>(&SettlementEvent {
        order_id: "ORD-1".into(),
        amount_cents: 5000,
    })
    .await?;
```

Consume (Kafka):

```rust,no_run
use shove::kafka::*;
use shove::consumer::{Consumer, ConsumerOptions};

let consumer = KafkaConsumer::new(client.clone());
let options = ConsumerOptions::new(shutdown).with_prefetch_count(20);
consumer.run::<OrderSettlement>(SettlementHandler, options).await?;
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

SNS/SQS uses `SqsConsumerGroupRegistry` and `SqsAutoscaler` with the same configuration shape.

NATS:

```rust,no_run
use shove::nats::*;
use shove::autoscaler::AutoscalerConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

let client = NatsClient::connect(&NatsConfig::new("nats://localhost:4222")).await?;
let mut registry = NatsConsumerGroupRegistry::new(client.clone());

registry
    .register::<WorkQueue, TaskHandler>(
        NatsConsumerGroupConfig::new(1..=8)
            .with_prefetch_count(10)
            .with_max_retries(5)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true),
        || TaskHandler,
    )
    .await?;

registry.start_all();

let registry = Arc::new(Mutex::new(registry));

let mut autoscaler = NatsAutoscalerBackend::autoscaler(
    client.clone(),
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

Kafka:

```rust,no_run
use shove::kafka::*;
use shove::autoscaler::AutoscalerConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

let client = KafkaClient::connect(&KafkaConfig::new("localhost:9092")).await?;
let mut registry = KafkaConsumerGroupRegistry::new(client.clone());

registry
    .register::<WorkQueue, TaskHandler>(
        KafkaConsumerGroupConfig::new(1..=8)
            .with_prefetch_count(10)
            .with_max_retries(5)
            .with_handler_timeout(Duration::from_secs(30))
            .with_concurrent_processing(true),
        || TaskHandler,
    )
    .await?;

registry.start_all();

let registry = Arc::new(Mutex::new(registry));

let mut autoscaler = KafkaAutoscalerBackend::autoscaler(
    client.clone(),
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

RabbitMQ examples:

- `rabbitmq_basic_pubsub`
- `rabbitmq_concurrent_pubsub`
- `rabbitmq_sequenced_pubsub`
- `rabbitmq_consumer_groups`
- `rabbitmq_audited_consumer`
- `rabbitmq_exactly_once`
- `rabbitmq_stress`

SNS/SQS examples:

- `sqs_basic_pubsub`
- `sqs_concurrent_pubsub`
- `sqs_sequenced_pubsub`
- `sqs_consumer_groups`
- `sqs_audited_consumer`
- `sqs_autoscaler`
- `sqs_stress`

NATS examples:

- `nats_basic`
- `nats_sequenced`
- `nats_audited_consumer`
- `nats_stress`

Kafka examples:

- `kafka_basic`
- `kafka_sequenced`
- `kafka_audited_consumer`
- `kafka_stress`

Run them with:

```sh
cargo run --example rabbitmq_basic_pubsub --features rabbitmq
cargo run --example rabbitmq_exactly_once --features rabbitmq-transactional
cargo run --example sqs_basic_pubsub --features aws-sns-sqs
cargo run --example sqs_autoscaler --features aws-sns-sqs
cargo run --example nats_basic --features nats
cargo run --example nats_sequenced --features nats
cargo run --example kafka_basic --features kafka
cargo run --example kafka_sequenced --features kafka
```

## Reference

### Outcome variants

| Variant        | Behavior |
|----------------|----------|
| `Outcome::Ack`    | Success — remove from queue |
| `Outcome::Retry`  | Transient failure — route to hold queue with escalating backoff, increment retry counter. Also triggered when `handler_timeout` is exceeded |
| `Outcome::Reject` | Permanent failure — route to DLQ (or discard if no DLQ configured) |
| `Outcome::Defer`  | Re-deliver via hold queue without incrementing retry counter |

### TopologyBuilder

| Method | Effect |
|--------|--------|
| `.hold_queue(duration)` | Adds a hold queue for delayed retry (`{name}-hold-{secs}s`) |
| `.sequenced(policy)` | Enables strict per-key ordering via consistent-hash exchange |
| `.routing_shards(n)` | Sets the number of sub-queues for sequenced delivery (default: 8) |
| `.dlq()` | Adds a dead-letter queue (`{name}-dlq`) |

### Backends

| Backend | Feature flag | Status |
|---------|-------------|--------|
| RabbitMQ | `rabbitmq` | Stable |
| RabbitMQ (exactly-once routing) | `rabbitmq-transactional` | Stable |
| AWS SNS (publisher only) | `pub-aws-sns` | Stable |
| AWS SNS + SQS | `aws-sns-sqs` | Stable |
| NATS JetStream | `nats` | Stable |
| Apache Kafka | `kafka` | Stable |

`aws-sns-sqs` implies `pub-aws-sns`. Sequenced topics use SNS FIFO topics with `MessageGroupId` and sharded FIFO SQS queues. NATS uses subject-based shard routing with per-shard `max_ack_pending: 1`. Kafka uses partition-key routing for sequenced delivery and consumer lag for autoscaling.

## Requirements

`shove` requires Rust 1.85 or newer (edition 2024).

## Background

`shove` came out of production event-processing systems that needed more than a broker client but less than a platform rewrite. The crate focuses on the hard parts around message handling correctness and operational behavior, while leaving transport and persistence to RabbitMQ, SNS/SQS, NATS JetStream, or Kafka.

The API is still evolving.

## License

[MIT](LICENSE)
