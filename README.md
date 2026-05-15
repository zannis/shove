# shove

[![ci](https://github.com/zannis/shove/actions/workflows/ci.yml/badge.svg)](https://github.com/zannis/shove/actions/workflows/ci.yml)
[![Latest Version](https://img.shields.io/crates/v/shove.svg)](https://crates.io/crates/shove)
[![Docs](https://docs.rs/shove/badge.svg)](https://docs.rs/shove)
[![License:MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage](https://codecov.io/gh/zannis/shove/branch/main/graph/badge.svg)](https://codecov.io/gh/zannis/shove)

Type-safe async pub/sub for Rust. One API across RabbitMQ, AWS SNS+SQS, NATS JetStream, Apache Kafka, Redis/Valkey Streams, and an in-process backend.

**Guides, examples, and the full walkthrough live at [shove.rs](https://shove.rs).** Rustdoc on [docs.rs/shove](https://docs.rs/shove).

## Why shove

- **Typed topics** — define a topic once as a Rust type; queue names, DLQs, and hold queues all derive from it.
- **Retry topologies without glue code** — escalating backoff through hold queues, DLQ routing, retry budgets, handler timeouts.
- **Strict per-key ordering** — `SequencedTopic` with pluggable failure policies (`Skip` or `FailAll`), enforced by the broker.
- **Consumer groups + autoscaling** — min/max bounds driven by queue depth (or consumer lag on Kafka), with optional structured audit trails.
- **One API across six backends** — swap the transport without changing topic definitions or handlers.

If you have one queue, one consumer, and little retry logic, use `lapin`, the AWS SDK, `async-nats`, or `rdkafka` directly. `shove` is the layer for multi-service event flows that need operational discipline.

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

#[tokio::main]
async fn main() -> Result<(), shove::ShoveError> {
    use futures::FutureExt as _;

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
}
```

Swap `InMemory` for `RabbitMq`, `Sqs`, `Nats`, `Kafka`, or `Redis` — the topic and handler stay identical. Per-backend setup: [Getting Started](https://shove.rs/getting-started).

## Backends

| Backend              | Feature flag    | Marker     | Ordering primitive                    | Autoscale signal       |
|----------------------|-----------------|------------|---------------------------------------|------------------------|
| RabbitMQ             | `rabbitmq`      | `RabbitMq` | Consistent-hash exchange + SAC shards | Queue depth            |
| AWS SNS+SQS          | `aws-sns-sqs`   | `Sqs`      | FIFO topic + `MessageGroupId`         | Queue depth            |
| NATS JetStream       | `nats`          | `Nats`     | Subject shard + `max_ack_pending=1`   | Pending messages       |
| Apache Kafka         | `kafka`         | `Kafka`    | Partition key                         | Consumer lag           |
| Redis/Valkey Streams | `redis-streams` | `Redis`    | FNV-1a shard streams                  | XLEN + XPENDING        |
| In-process           | `inmemory`      | `InMemory` | Per-key FIFO shards                   | Queue depth (in-proc)  |

> **Redis/Valkey requirement:** Redis 6.2+ (or an equivalent Valkey release) is required. shove uses `ZRANGE … BYSCORE` for hold-queue polling, which was introduced in Redis 6.2. The version is validated at connection time and an error is returned if the server is older.

`cargo add shove --features <flag>`. No features are enabled by default. Decision guide: [Choosing a backend](https://shove.rs/backends/choosing).

Optional add-ons: `audit` (built-in `ShoveAuditHandler` + `AuditLog` topic), `metrics` (Prometheus/StatsD/OTel via the [`metrics`](https://docs.rs/metrics) facade), `kafka-ssl` (TLS + SASL), `rabbitmq-transactional` (exactly-once routing).

## Delivery

`shove` is at-least-once by default — handlers must be idempotent. A handler returns one of:

- `Ack` — success
- `Retry` — delayed retry through hold queues with escalating backoff
- `Reject` — dead-letter immediately
- `Defer` — delay without consuming a retry budget

Handler timeouts convert to `Retry`. Full semantics: [Outcomes & Delivery](https://shove.rs/concepts/outcomes).

## Performance

MacBook Pro M4 Max, single RabbitMQ node via Docker, Rust 1.91. Reproducible via `cargo run -q --example rabbitmq_stress --features rabbitmq`.

| Handler          | 1 worker, prefetch=1 | 1 worker, prefetch=20 | 8 workers, prefetch=20 | 32 workers, prefetch=40 |
|------------------|----------------------|-----------------------|------------------------|-------------------------|
| Fast (1–5 ms)    | 179 msg/s            | 2,866 msg/s           | 19,669 msg/s           | 29,207 msg/s            |
| Slow (50–300 ms) | 6 msg/s              | 75 msg/s              | 544 msg/s              | 4,076 msg/s             |
| Heavy (1–5 s)    | 0.4 msg/s            | 5 msg/s               | 21 msg/s               | 199 msg/s               |

`prefetch_count` is the primary throughput lever for I/O-bound handlers. Tuning notes: [Performance](https://shove.rs/ops/performance).

## Learn more

- [Getting Started](https://shove.rs/getting-started) — install, declare your first topic, publish and consume on every backend
- [Core concepts](https://shove.rs/concepts/topics) — topics & topology, outcomes, handlers & context, the `Broker<B>` pattern
- [Guides](https://shove.rs/guides/retries) — retries, sequenced delivery, consumer groups, audit, observability, exactly-once, shutdown
- [Backends](https://shove.rs/backends/choosing) — per-backend overviews and runnable examples
- [docs.rs/shove](https://docs.rs/shove) — full rustdoc

## Requirements

- Rust 1.85 or newer (edition 2024).
- Redis 6.2+ or Valkey (any release) when using the `redis-streams` backend.

## License

[MIT](LICENSE)