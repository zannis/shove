# shove

[![ci](https://github.com/zannis/shove/actions/workflows/ci.yml/badge.svg)](https://github.com/zannis/shove/actions/workflows/ci.yml)
[![Latest Version](https://img.shields.io/crates/v/shove.svg)](https://crates.io/crates/shove)
[![Docs](https://docs.rs/shove/badge.svg)](https://docs.rs/shove)
[![License:MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage](https://codecov.io/gh/zannis/shove/branch/main/graph/badge.svg)](https://codecov.io/gh/zannis/shove)

Type-safe async pub/sub for Rust, backed by RabbitMQ.

Define a topic once — shove handles the plumbing: queue topology, retries, DLQ, ordered delivery, auditing and autoscaling consumer groups. A single consumer handles 10k+ msg/s out of the box.

## Quick start

Requires a running RabbitMQ instance with the consistent-hash exchange plugin enabled. Start one with the included docker-compose:

```sh
docker compose up -d
```

Add to your project:

```sh
cargo add shove
```

### Define a topic

```rust,no_run
use shove::*;
use serde::{Serialize, Deserialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct SettlementEvent {
    order_id: String,
    amount_cents: u64,
}

define_topic!(OrderSettlement, SettlementEvent,
    TopologyBuilder::new("order-settlement")
        .hold_queue(Duration::from_secs(5))
        .hold_queue(Duration::from_secs(30))
        .hold_queue(Duration::from_secs(120))
        .dlq()
        .build()
);
```

### Publish

```rust,no_run
use shove::rabbitmq::*;

// Connect to local RabbitMQ (docker compose up -d)
let client = RabbitMqClient::connect(&RabbitMqConfig::new("amqp://localhost")).await?;
let publisher = RabbitMqPublisher::new(client.clone()).await?;

let event = SettlementEvent { order_id: "ORD-1".into(), amount_cents: 5000 };

// Type-safe: compiler ensures SettlementEvent matches OrderSettlement::Message
publisher.publish::<OrderSettlement>(&event).await?;
```

### Consume

```rust,no_run
use shove::rabbitmq::*;
use tokio_util::sync::CancellationToken;

struct SettlementHandler;

impl MessageHandler<OrderSettlement> for SettlementHandler {
    async fn handle(&self, msg: SettlementEvent, metadata: MessageMetadata) -> Outcome {
        match process(&msg).await {  // your business logic here
            Ok(()) => Outcome::Ack,
            Err(e) if e.is_transient() => Outcome::Retry,
            Err(_) => Outcome::Reject,
        }
    }
}

let shutdown_token = CancellationToken::new();
let consumer = RabbitMqConsumer::new(client);
let options = ConsumerOptions::new(shutdown_token)
    .with_prefetch_count(20);

// Processes up to 20 messages concurrently, acks always in delivery order
consumer
    .run_concurrent::<OrderSettlement>(SettlementHandler, options)
    .await?;
```

## Why not just use lapin?

lapin gives you raw AMQP primitives — channels, queues, exchanges, bindings. If you have one service with one queue, that's all you need.

shove is for when you have dozens of topics across multiple services and you don't want to hand-roll retry topologies, DLQ routing, consumer groups, autoscaling, and audit trails for each one. It's a higher-level abstraction: topics are Rust types, the topology is derived from them, and the framework manages the lifecycle.

## Performance

Measured on a MacBook Pro M4 Max, single RabbitMQ node via Docker, Rust 1.91. Results will vary by hardware and broker configuration. Reproducible via `cargo run -q --example stress --features rabbitmq`.

Throughput is controlled by two knobs: **prefetch count** (how many messages each worker processes concurrently) and **worker count** (how many consumers in the group).

| Handler          | 1 worker, prefetch=1 | 1 worker, prefetch=20 | 8 workers, prefetch=20 | 32 workers, prefetch=40 |
|------------------|----------------------|-----------------------|------------------------|-------------------------|
| Fast (1-5ms)     | 179 msg/s            | 2,866 msg/s           | 19,669 msg/s           | 29,207 msg/s            |
| Slow (50-300ms)  | 6 msg/s              | 75 msg/s              | 544 msg/s              | 4,076 msg/s             |
| Heavy (1-5s)     | 0.4 msg/s            | 5 msg/s               | 21 msg/s               | 199 msg/s               |

Prefetch count is the single biggest throughput lever for I/O-bound handlers. Adding workers scales linearly when the handler is the bottleneck.

The framework itself adds minimal overhead: sub-millisecond dispatch latency per message, ~4 KB RSS per consumer, and zero idle CPU — tested up to 4096 consumers on a single connection.

## Features

- **Compile-time topic binding** — each topic is a unit struct that associates a message type (`Serialize + DeserializeOwned`) with a queue topology. No stringly-typed queue names at call sites.
- **Concurrent consumption** — process up to `prefetch_count` messages concurrently within a single consumer via `run_concurrent`, while always acknowledging in delivery order. This should be your default.
- **Escalating retry backoff** — configure multiple hold queues with increasing delays. The consumer picks the right one automatically based on retry count.
- **Dead-letter queues** — opt-in per topic. Messages that exceed max retries or are explicitly rejected route to DLQ with full death metadata.
- **Sequenced delivery** — strict per-key ordering via `SequencedTopic`. Messages sharing a sequence key are delivered in publish order. Different keys are processed concurrently within each shard. Two failure policies: `Skip` (continue the sequence) or `FailAll` (terminate it).
- **Consumer groups & autoscaling** — dynamically scale consumers up and down based on queue depth, with hysteresis and cooldown to prevent flapping.
- **Audit logging** — wrap any handler with `Audited<H, A>` to capture every delivery attempt as a structured `AuditRecord`. Implement the `AuditHandler` trait for your persistence backend, or enable the `audit` feature for a built-in handler that publishes records to a dedicated `shove-audit-log` topic.
- **Handler timeout** — set a per-consumer `handler_timeout` in `ConsumerOptions` or `ConsumerGroupConfig`. Messages that exceed the deadline are automatically retried.
- **Backend-agnostic core** — traits for `Publisher`, `Consumer`, `TopologyDeclarer`, and `MessageHandler` live in the core crate. Backends are feature-gated.

## Concurrent consumption

By default, `run_concurrent` processes up to `prefetch_count` messages concurrently within a single consumer. Messages are dispatched to handler tasks as they arrive, but **acknowledgements are always sent in delivery order** — if messages 1, 2, 3 are in-flight and message 3 finishes first, it waits for 1 and 2 to complete before any are acked.

Consumer groups support this via `ConsumerGroupConfig::with_concurrent_processing(true)`.

The only reason to use sequential consumption (`run`) is if your handler has process-level side effects that cannot tolerate concurrency (e.g. writing to a shared file or holding a global lock). If your handler is `async` and talks to external services, use `run_concurrent`.

## Sequenced topics

Use sequenced topics when message ordering is absolutely required for correctness. The canonical example is financial transactions: if account `ACC-123` receives a deposit, then a withdrawal, processing them out of order produces an incorrect balance. Other cases include state-machine transitions, inventory adjustments, and any domain where events are causally dependent.

Within each shard, different sequence keys are processed concurrently — only messages sharing the same key are serialized. This means you get the throughput benefits of concurrent processing across independent entities (different accounts, different users) while maintaining strict ordering where it matters:

```rust,no_run
define_sequenced_topic!(AccountLedger, LedgerEntry, |msg| msg.account_id.clone(),
    TopologyBuilder::new("account-ledger")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(16)
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// Compiler enforces AccountLedger: SequencedTopic
consumer
    .run_sequenced::<AccountLedger>(handler, options)
    .await?;
```

### Sequence failure policies

When a sequenced message fails permanently (exceeds max retries or returns `Outcome::Reject`), the failure policy controls what happens to the rest of the sequence:

**`SequenceFailure::Skip`** — Dead-letter the failed message and continue processing subsequent messages for the same key. Use this when messages are independently valid but need ordered delivery (e.g. audit log entries, analytics events).

**`SequenceFailure::FailAll`** — Dead-letter the failed message *and* automatically dead-letter all remaining messages for the same key. The key is "poisoned" for the lifetime of the consumer process. Use this when messages are causally dependent — processing later messages after an earlier failure would produce an inconsistent state (e.g. financial ledger entries, state-machine transitions).

Messages for *other* sequence keys are unaffected by either policy.

**Example:** given messages `[1, 2, 3, 4, 5]` for key `ACC-A` where message 3 is rejected:

| Policy    | Ack'd   | DLQ'd |
|-----------|---------|-------|
| `Skip`    | 1,2,4,5 | 3     |
| `FailAll` | 1,2     | 3,4,5 |

## Consumer groups & autoscaling

```rust,no_run
use shove::rabbitmq::*;

let mut registry = ConsumerGroupRegistry::new(client.clone());

registry.register::<OrderSettlement, SettlementHandler>(
    ConsumerGroupConfig::new(1..=8)
        .with_prefetch_count(10)
        .with_max_retries(5)
        .with_handler_timeout(Duration::from_secs(30))
        .with_concurrent_processing(true),
    || SettlementHandler,
).await?;

registry.start_all();

// Autoscaler adjusts consumer count based on queue depth
let mgmt = ManagementClient::new(ManagementConfig::new("http://localhost:15672", "guest", "guest"));
let mut autoscaler = Autoscaler::new(mgmt, AutoscalerConfig::default());
autoscaler.run(Arc::new(Mutex::new(registry)), shutdown_token).await;
```

## Audit logging

Wrap any handler with `Audited` to record every delivery attempt as a structured `AuditRecord`:

```rust,no_run
use shove::*;

struct MyAuditSink;

impl<T: Topic> AuditHandler<T> for MyAuditSink
where
    T::Message: Clone + serde::Serialize,
{
    async fn audit(&self, record: &AuditRecord<T::Message>) -> error::Result<()> {
        println!("{}", serde_json::to_string(record).unwrap());
        Ok(())
    }
}

let handler = Audited::new(SettlementHandler, MyAuditSink);

// Use it anywhere a normal handler is accepted — consumers, consumer groups, etc.
consumer
    .run::<OrderSettlement>(handler, ConsumerOptions::new(shutdown_token))
    .await?;
```

Each `AuditRecord` contains: `trace_id`, `topic`, `payload`, `metadata`, `outcome`, `duration_ms`, and `timestamp`.

If the audit handler returns `Err`, the message is retried — audit failure is never silently dropped.

### Built-in `ShoveAuditHandler`

Enable the `audit` feature (on by default) for a handler that publishes records back to a dedicated `shove-audit-log` topic:

```rust,no_run
use shove::*;
use shove::rabbitmq::*;

let audit = ShoveAuditHandler::new(publisher.clone());
let handler = Audited::new(SettlementHandler, audit);
```

This creates a self-contained audit trail inside your broker. The audit topic itself is not audited.

## Reference

### Outcome variants

| Variant  | Behavior                                                                                                                                    |
|----------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `Ack`    | Message processed successfully, remove from queue                                                                                           |
| `Retry`  | Transient failure — route to hold queue with escalating backoff, increment retry counter. Also triggered automatically when `handler_timeout` is exceeded |
| `Reject` | Permanent failure — route to DLQ (or discard if no DLQ)                                                                                     |
| `Defer`  | Re-deliver via hold queue without incrementing retry counter (e.g. scheduled messages)                                                      |

### Topology configurations

| Configuration          | What it does                                                          |
|------------------------|-----------------------------------------------------------------------|
| `.dlq()`               | Adds a dead-letter queue (`{name}-dlq`)                               |
| `.hold_queue(duration)` | Adds a hold queue for delayed retry (`{name}-hold-{secs}s`)          |
| `.sequenced(policy)`   | Enables strict per-key ordering via consistent-hash exchange          |
| `.routing_shards(n)`   | Sets the number of sub-queues for sequenced delivery (default: 8)     |

### Backends

| Backend     | Feature flag | Status  |
|-------------|-------------|---------|
| RabbitMQ    | `rabbitmq`  | Stable  |
| AWS SNS/SQS | `sns`       | Planned |

Both `rabbitmq` and `audit` are default features. To opt out of the built-in audit backend:

```toml
shove = { version = "0.5", default-features = false, features = ["rabbitmq"] }
```

### Minimum Rust version

shove uses `edition = "2024"`, which requires **Rust 1.85 or later**.

## Examples

See the [`examples/`](examples/) directory:

- **[`basic_pubsub`](examples/basic_pubsub.rs)** — publish/consume lifecycle, DLQ handling, all outcome variants
- **[`concurrent_pubsub`](examples/concurrent_pubsub.rs)** — concurrent consumption with in-order acking, sequential vs concurrent comparison
- **[`sequenced_pubsub`](examples/sequenced_pubsub.rs)** — ordered delivery with `Skip` and `FailAll` policies
- **[`consumer_groups`](examples/consumer_groups.rs)** — dynamic scaling with the autoscaler
- **[`audited_consumer`](examples/audited_consumer.rs)** — custom `AuditHandler` that logs every delivery attempt to stdout
- **[`stress`](examples/stress.rs)** — tiered stress benchmarks measuring throughput, latency percentiles, scaling efficiency, and resource usage

```sh
cargo run --example basic_pubsub --features rabbitmq
cargo run --example concurrent_pubsub --features rabbitmq
cargo run --example sequenced_pubsub --features rabbitmq
cargo run --example consumer_groups --features rabbitmq
cargo run --example audited_consumer --features rabbitmq
```

### Stress tests

The stress suite runs tiered benchmarks against a RabbitMQ container (started automatically via testcontainers):

```sh
cargo run -q --example stress --features rabbitmq                                          # all tiers, all handlers
cargo run -q --example stress --features rabbitmq -- --tier moderate                       # moderate tier only
cargo run -q --example stress --features rabbitmq -- --handler fast                        # fast handler only
cargo run -q --example stress --features rabbitmq -- --tier high --handler zero --output json
```

The bench suite also includes consumer-overhead analysis:

```sh
cargo bench -q --features rabbitmq --bench consumer_overhead                             # consumer scaling overhead (128..4096)
cargo bench -q --features rabbitmq --bench consumer_overhead -- --output json
```

## Roadmap

- [ ] **SNS/SQS backend** — publish via SNS, consume via SQS FIFO queues with message group ID for sequenced delivery
- [ ] **Observability** — built-in OpenTelemetry metrics (publish latency, consume rate, retry/DLQ counts)
- [ ] **Multi-backend topology declaration** — declare topics across backends in a single call

## Background

The first version of this crate was built for [Lens](https://lens.xyz) to handle millions of async events — media ingestion, cross-chain migrations, backfills. It was a self-contained pubsub system including a bespoke message broker, and it worked well, but lacked auditing and autoscaling.

shove is the do-over. RabbitMQ handles storage and routing. shove handles the rest: type-safe topics, retry topologies, ordered delivery, consumer groups that scale themselves.

We needed a name for "throw a job at something and stop thinking about it." Push was taken. Yeet was considered. shove stuck — it's what you do with messages: shove them in, let the broker deal with it.

> **Note:** shove is under active development. The API is subject to breaking changes.

## Disclaimer

The architecture and design of this crate are human-made. The implementation is mostly written by [Claude](https://claude.ai).

## License

[MIT](LICENSE)