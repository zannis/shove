# shove

[![ci](https://github.com/zannis/shove/actions/workflows/ci.yml/badge.svg)](https://github.com/zannis/shove/actions/workflows/ci.yml)
[![Latest Version](https://img.shields.io/crates/v/shove.svg)](https://crates.io/crates/shove)
[![Docs](https://docs.rs/shove/badge.svg)](https://docs.rs/shove)
[![License:MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage](https://codecov.io/gh/zannis/shove/branch/main/graph/badge.svg)](https://codecov.io/gh/zannis/shove)

High-performance, topic-typed pub/sub for Rust.

**shove** is an async message publishing and consuming library built for high throughput and low overhead. It binds message types to their broker destinations at compile time — the type system ensures publishers and consumers agree on message shape, queue names, retry policy, and ordering guarantees. Consumer groups scale linearly with near-zero framework overhead, handling 28k+ msg/s at 128 consumers on a single broker node.

> ⚠️ **Warning:** shove is under active development and is not considered production-ready. The API is subject to breaking changes and comes with no guarantees of stability, correctness, or backwards compatibility. 🚧

## Background

The first version of this crate was built for [Lens](https://lens.xyz) to handle millions of async events —  media ingestion, cross-chain migrations, backfills. It was a self-contained pubsub system including a bespoke message broker, and it worked well, but lacked auditing and autoscaling.

shove is the do-over. RabbitMQ handles storage and routing. shove handles the rest: type-safe topics, retry topologies, ordered delivery, consumer groups that scale themselves.

## Who is it for

You have a distributed, event-driven application. You need to move messages between services reliably, at high throughput, without burning through resources. You want the compiler to catch mismatches between publishers and consumers before they hit production. And when load spikes, you want consumers to scale themselves instead of paging someone at 3 AM.

If the above sounds relatable, then shove is for you.

## Why "shove"

We needed a name for "throw a job at something and stop thinking about it." Push was taken. Yeet was considered. shove stuck — it's what you do with messages: shove them in, let the broker deal with it.

## Features

- **Compile-time topic binding** — each topic is a unit struct that associates a message type (`Serialize + DeserializeOwned`) with a queue topology. No stringly-typed queue names at call sites.
- **Escalating retry backoff** — configure multiple hold queues with increasing delays. The consumer picks the right one automatically based on retry count.
- **Dead-letter queues** — opt-in per topic. Messages that exceed max retries or are explicitly rejected route to DLQ with full death metadata.
- **Sequenced delivery** — strict per-key ordering via `SequencedTopic`. Messages sharing a sequence key are delivered in publish order. Two failure policies: `Skip` (continue the sequence) or `FailAll` (terminate it).
- **Consumer groups & autoscaling** — dynamically scale consumers up and down based on queue depth, with hysteresis and cooldown to prevent flapping.
- **Audit logging** — wrap any handler with `Audited<H, A>` to capture every delivery attempt as a structured `AuditRecord`. Implement the `AuditHandler` trait for your persistence backend, or enable the `audit` feature for a built-in handler that publishes records to a dedicated `shove-audit-log` topic.
- **Concurrent consumption** — process up to `prefetch_count` messages concurrently within a single consumer via `run_concurrent`, while always acknowledging in delivery order. Not available for sequenced topics (enforced at compile time).
- **Handler timeout** — set a per-consumer `handler_timeout` in `ConsumerOptions` or `ConsumerGroupConfig`. Messages that exceed the deadline are automatically retried.
- **Backend-agnostic core** — traits for `Publisher`, `Consumer`, `TopologyDeclarer`, and `MessageHandler` live in the core crate. Backends are feature-gated.

## Backends

| Backend | Feature flag | Status |
|---------|-------------|--------|
| RabbitMQ | `rabbitmq` | Stable |
| AWS SNS/SQS | `sns` | Planned |

Both `rabbitmq` and `audit` are default features. To opt out of the built-in audit backend:

```toml
shove = { version = "0.5", default-features = false, features = ["rabbitmq"] }
```

## Quick start

Add shove to your `Cargo.toml`:

```toml
[dependencies]
shove = { version = "0.5", features = ["rabbitmq"] }
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

let client = RabbitMqClient::connect(&RabbitMqConfig::new("amqp://localhost")).await?;
let publisher = RabbitMqPublisher::new(client.clone()).await?;

let event = SettlementEvent { order_id: "ORD-1".into(), amount_cents: 5000 };

// Type-safe: compiler ensures SettlementEvent matches OrderSettlement::Message
publisher.publish::<OrderSettlement>(&event).await?;
```

### Consume

```rust,no_run
use shove::rabbitmq::*;

struct SettlementHandler;

impl MessageHandler<OrderSettlement> for SettlementHandler {
    async fn handle(&self, msg: SettlementEvent, metadata: MessageMetadata) -> Outcome {
        match process(&msg).await {
            Ok(()) => Outcome::Ack,
            Err(e) if e.is_transient() => Outcome::Retry,
            Err(_) => Outcome::Reject,
        }
    }
}

let consumer = RabbitMqConsumer::new(client);
consumer
    .run::<OrderSettlement>(SettlementHandler, ConsumerOptions::new(shutdown_token))
    .await?;
```

### Concurrent consumption

By default, each consumer processes one message at a time. For handlers with I/O latency (HTTP calls, database queries, external APIs), you can enable concurrent processing within a single consumer using `run_concurrent`:

```rust,no_run
let options = ConsumerOptions::new(shutdown_token)
    .with_prefetch_count(20);

// Processes up to 20 messages concurrently, acks always in delivery order
consumer
    .run_concurrent::<OrderSettlement>(SettlementHandler, options)
    .await?;
```

Messages are dispatched to handler tasks as they arrive (up to `prefetch_count` in-flight), but **acknowledgements are always sent in delivery order** — if messages 1, 2, 3 are in-flight and message 3 finishes first, it waits for 1 and 2 to complete before any are acked. This preserves broker-level ordering guarantees while allowing handlers to overlap.

Consumer groups support this via `ConsumerGroupConfig::with_concurrent_processing(true)`.

`run_concurrent` is **not available for sequenced topics** — `run_sequenced` always processes one message at a time to guarantee strict per-key ordering.

### Sequenced topics

For messages that must be processed in strict order per key:

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

| Policy   | Ack'd   | DLQ'd     |
|----------|---------|-----------|
| `Skip`   | 1,2,4,5 | 3         |
| `FailAll`| 1,2     | 3,4,5     |

## Topology configurations

| Configuration | What it does |
|---------------|-------------|
| `.dlq()` | Adds a dead-letter queue (`{name}-dlq`) |
| `.hold_queue(duration)` | Adds a hold queue for delayed retry (`{name}-hold-{secs}s`) |
| `.sequenced(policy)` | Enables strict per-key ordering via consistent-hash exchange |
| `.routing_shards(n)` | Sets the number of sub-queues for sequenced delivery (default: 8) |

## Outcome variants

| Variant | Behavior |
|---------|----------|
| `Ack` | Message processed successfully, remove from queue |
| `Retry` | Transient failure — route to hold queue with escalating backoff, increment retry counter. Also triggered automatically when `handler_timeout` is exceeded |
| `Reject` | Permanent failure — route to DLQ (or discard if no DLQ) |
| `Defer` | Re-deliver via hold queue without incrementing retry counter (e.g. scheduled messages) |

## Consumer groups & autoscaling

```rust,no_run
use shove::rabbitmq::*;

let mut registry = ConsumerGroupRegistry::new(client.clone());

registry.register::<MyTopic, MyHandler>(
    "my-group",
    MyTopic::topology().queue(),
    ConsumerGroupConfig {
        prefetch_count: 10,
        min_consumers: 1,
        max_consumers: 8,
        max_retries: 5,
        handler_timeout: Some(Duration::from_secs(30)),
    },
    || MyHandler::new(),
);

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

## Examples

See the [`examples/`](examples/) directory:

- **[`basic_pubsub`](examples/basic_pubsub.rs)** — all non-sequenced configurations, publish/consume lifecycle, DLQ handling, all outcome variants
- **[`sequenced_pubsub`](examples/sequenced_pubsub.rs)** — ordered delivery with `Skip` and `FailAll` policies
- **[`concurrent_pubsub`](examples/concurrent_pubsub.rs)** — concurrent consumption with in-order acking, sequential vs concurrent comparison
- **[`consumer_groups`](examples/consumer_groups.rs)** — dynamic scaling with the autoscaler
- **[`audited_consumer`](examples/audited_consumer.rs)** — custom `AuditHandler` that logs every delivery attempt to stdout
- **[`stress`](examples/stress.rs)** — tiered stress benchmarks measuring throughput, latency percentiles, scaling efficiency, and resource usage

Start RabbitMQ with the included docker-compose (enables the management and consistent-hash exchange plugins):

```sh
docker compose up -d
```

Then run any example:

```sh
cargo run --example basic_pubsub --features rabbitmq
cargo run --example sequenced_pubsub --features rabbitmq
cargo run --example concurrent_pubsub --features rabbitmq
cargo run --example consumer_groups --features rabbitmq
cargo run --example audited_consumer --features rabbitmq
```

e### Benchmarks

The bench suite runs tiered stress tests and consumer-overhead analysis against a RabbitMQ container (started automatically via testcontainers):

```sh
cargo bench --features rabbitmq --bench stress                          # all tiers, all handlers
cargo bench --features rabbitmq --bench stress -- --tier moderate       # moderate tier only
cargo bench --features rabbitmq --bench stress -- --handler fast        # fast handler only
cargo bench --features rabbitmq --bench stress -- --tier high --handler zero --output json
cargo bench --features rabbitmq --bench consumer_overhead               # consumer scaling overhead (128..4096)
cargo bench --features rabbitmq --bench consumer_overhead -- --handler slow --concurrent
```

## Performance

Measured on a MacBook Pro M4 Max, single RabbitMQ node via Docker, Rust 1.91. Four handler profiles simulate different workloads: **zero** (no-op), **fast** (1-5ms), **slow** (50-300ms), and **heavy** (1-5s). Full results are reproducible via `cargo bench --features rabbitmq --bench stress`.

### Broker ceiling

With no handler latency, throughput plateaus at **~40k msg/s** — the single-node RabbitMQ dispatch ceiling. Adding consumers beyond 8 provides no benefit. This is the upper bound for any workload on a single broker.

### Sequential consumer scaling

Consumer groups scale linearly when the handler is the bottleneck:

| Handler | 1 consumer | 8 consumers | 32 consumers | 64 consumers | 128 consumers |
|---------|-----------|-------------|--------------|--------------|---------------|
| Fast (1-5ms) | 222 msg/s | 1,885 msg/s | 7,543 msg/s | 14,663 msg/s | 28,706 msg/s |
| Slow (50-300ms) | 6 msg/s | 45 msg/s | 177 msg/s | 323 msg/s | 679 msg/s |
| Heavy (1-5s) | 0.3 msg/s | 2 msg/s | 7 msg/s | 14 msg/s | — |

Scaling is near-perfect linear across all profiles. At 256 consumers, broker coordination overhead causes a slight regression for fast handlers.

### Concurrent processing

Each consumer can process up to `prefetch_count` messages concurrently within a single task while preserving in-order acking. This is the single biggest throughput lever for I/O-bound handlers:

| Handler | 1c sequential | 1c concurrent | Speedup | 8c concurrent |
|---------|--------------|---------------|---------|---------------|
| Fast (1-5ms) | 222 msg/s | 10,616 msg/s | **48x** | 40,582 msg/s |
| Slow (50-300ms) | 6 msg/s | 331 msg/s | **55x** | 2,479 msg/s |
| Heavy (1-5s) | 0.3 msg/s | 4 msg/s | **13x** | 15 msg/s |

One concurrent consumer with a slow handler (331 msg/s) outperforms **64 sequential consumers** (323 msg/s). For fast handlers, 8 concurrent consumers saturate the broker ceiling.

### Key takeaways

- **Use concurrent mode** — it's always faster for I/O-bound handlers, with 13-55x speedup per consumer
- **Broker ceiling** of ~40k msg/s on a single RabbitMQ node, reachable with 8 concurrent consumers
- **Linear scaling** when the handler is the bottleneck — add consumers and throughput scales proportionally
- **Memory** stays under 10 MB even at 256 consumers

## Roadmap

- [ ] **SNS/SQS backend** — publish via SNS, consume via SQS FIFO queues with message group ID for sequenced delivery
- [ ] **Batch consume** — process messages in batches for throughput-sensitive workloads
- [ ] **Observability** — built-in OpenTelemetry metrics (publish latency, consume rate, retry/DLQ counts)
- [ ] **Multi-backend topology declaration** — declare topics across backends in a single call

## Disclaimer

The architecture and design of this crate are human-made. The implementation is mostly written by [Claude](https://claude.ai).

## License

[MIT](LICENSE)
