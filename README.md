# shove

Topic-typed pub/sub for Rust.

**shove** is an async message publishing and consuming library that binds message types to their broker destinations at compile time. Define a topic once — the type system ensures publishers and consumers agree on message shape, queue names, retry policy, and ordering guarantees.

## Background

The first version of this was built for [Lens](https://lens.xyz) to handle millions of async events — ingestion, cross-chain migrations, backfills. It was a custom broker, and it worked, but lacked auditing and autoscaling.

shove is the do-over. RabbitMQ handles storage and routing. shove handles the rest: type-safe topics, retry topologies, ordered delivery, consumer groups that scale themselves.

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
| `Retry` | Transient failure — route to hold queue with escalating backoff, increment retry counter |
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

Start RabbitMQ with the included docker-compose (enables the management and consistent-hash exchange plugins):

```sh
docker compose up -d
```

Then run any example:

```sh
cargo run --example basic_pubsub --features rabbitmq
cargo run --example sequenced_pubsub --features rabbitmq
cargo run --example consumer_groups --features rabbitmq
```

## Roadmap

- [ ] **SNS/SQS backend** — publish via SNS, consume via SQS FIFO queues with message group ID for sequenced delivery
- [ ] **Batch consume** — process messages in batches for throughput-sensitive workloads
- [ ] **Observability** — built-in metrics (publish latency, consume rate, retry/DLQ counts)
- [ ] **Schema evolution** — pluggable serialization with versioned message envelopes
- [ ] **Multi-backend topology declaration** — declare topics across backends in a single call

## License

[MIT](LICENSE)
