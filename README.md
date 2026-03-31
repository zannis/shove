# shove

[![Build Status](https://github.com/zannis/shove/workflows/Build/badge.svg)](https://github.com/zannis/shove/actions)
[![Latest Version](https://img.shields.io/crates/v/shove.svg)](https://crates.io/crates/shove)
[![Docs](https://docs.rs/shove/badge.svg)](https://docs.rs/shove)
[![License:MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage](https://codecov.io/gh/zannis/shove/branch/main/graph/badge.svg)](https://codecov.io/gh/zannis/shove)

Topic-typed pub/sub for Rust.

**shove** is an async message publishing and consuming library that binds message types to their broker destinations at compile time. Define a topic once — the type system ensures publishers and consumers agree on message shape, queue names, retry policy, and ordering guarantees.

## Background

The first version of this was built for [Lens](https://lens.xyz) to handle millions of async events — ingestion, cross-chain migrations, backfills. It was a custom broker, and it worked, but lacked auditing and autoscaling.

shove is the do-over. RabbitMQ handles storage and routing. shove handles the rest: type-safe topics, retry topologies, ordered delivery, consumer groups that scale themselves.

## Who is it for

You have a distributed, event-driven application. You need to move messages between services reliably, at high throughput, without burning through resources. You want the compiler to catch mismatches between publishers and consumers before they hit production. And when load spikes, you want consumers to scale themselves instead of paging someone at 3 AM.

If all of the above sounds relatable, then shove is for you.

## Why "shove"

We needed a name for "throw a job at something and stop thinking about it." Push was taken. Yeet was considered. shove stuck — it's what you do with messages: shove them in, let the broker deal with it.

## Features

- **Compile-time topic binding** — each topic is a unit struct that associates a message type (`Serialize + DeserializeOwned`) with a queue topology. No stringly-typed queue names at call sites.
- **Escalating retry backoff** — configure multiple hold queues with increasing delays. The consumer picks the right one automatically based on retry count.
- **Dead-letter queues** — opt-in per topic. Messages that exceed max retries or are explicitly rejected route to DLQ with full death metadata.
- **Sequenced delivery** — strict per-key ordering via `SequencedTopic`. Messages sharing a sequence key are delivered in publish order. Two failure policies: `Skip` (continue the sequence) or `FailAll` (terminate it).
- **Consumer groups & autoscaling** — dynamically scale consumers up and down based on queue depth, with hysteresis and cooldown to prevent flapping.
- **Backend-agnostic core** — traits for `Publisher`, `Consumer`, `TopologyDeclarer`, and `MessageHandler` live in the core crate. Backends are feature-gated.

## Backends

| Backend | Feature flag | Status |
|---------|-------------|--------|
| RabbitMQ | `rabbitmq` | Stable |
| AWS SNS/SQS | `sns` | Planned |

## Quick start

Add shove to your `Cargo.toml`:

```toml
[dependencies]
shove = { version = "0.1", features = ["rabbitmq"] }
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

## Examples

See the [`examples/`](examples/) directory:

- **[`basic_pubsub`](examples/basic_pubsub.rs)** — all non-sequenced configurations, publish/consume lifecycle, DLQ handling, all outcome variants
- **[`sequenced_pubsub`](examples/sequenced_pubsub.rs)** — ordered delivery with `Skip` and `FailAll` policies
- **[`consumer_groups`](examples/consumer_groups.rs)** — dynamic scaling with the autoscaler
- **[`audited_consumer`](examples/audited_consumer.rs)** — custom `AuditHandler` that logs every delivery attempt to stdout

Start RabbitMQ with the included docker-compose (enables the management and consistent-hash exchange plugins):

```sh
docker compose up -d
```

Then run any example:

```sh
cargo run --example basic_pubsub --features rabbitmq
cargo run --example sequenced_pubsub --features rabbitmq
cargo run --example consumer_groups --features rabbitmq
cargo run --example audited_consumer --features rabbitmq
```

## Roadmap

- [ ] **SNS/SQS backend** — publish via SNS, consume via SQS FIFO queues with message group ID for sequenced delivery
- [ ] **Batch consume** — process messages in batches for throughput-sensitive workloads
- [ ] **Observability** — built-in metrics (publish latency, consume rate, retry/DLQ counts)
- [ ] **Schema evolution** — pluggable serialization with versioned message envelopes
- [ ] **Multi-backend topology declaration** — declare topics across backends in a single call

## Disclaimer

The architecture and design of this crate are human-made. The implementation is mostly written by [Claude](https://claude.ai).

## License

[MIT](LICENSE)
