# shove

[![ci](https://github.com/zannis/shove/actions/workflows/ci.yml/badge.svg)](https://github.com/zannis/shove/actions/workflows/ci.yml)
[![Latest Version](https://img.shields.io/crates/v/shove.svg)](https://crates.io/crates/shove)
[![Docs](https://docs.rs/shove/badge.svg)](https://docs.rs/shove)
[![License:MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage](https://codecov.io/gh/zannis/shove/branch/main/graph/badge.svg)](https://codecov.io/gh/zannis/shove)
[![Dependencies](https://deps.rs/repo/github/zannis/shove/status.svg)](https://deps.rs/repo/github/zannis/shove)

Type-safe async pub/sub for Rust. One API across RabbitMQ, AWS SNS+SQS, NATS JetStream, Apache Kafka, Redis/Valkey Streams, and an in-process backend.

**Guides, examples, and the full walkthrough live at [shove.rs](https://shove.rs).** Rustdoc on [docs.rs/shove](https://docs.rs/shove).

## What you get

- **Define a topic once, use it everywhere.** Queue names, DLQs, and retries all derive from a single Rust type.
- **Retries and DLQs included.** Escalating backoff, dead-letter routing, retry budgets, handler timeouts — no glue code.
- **Strict per-key ordering** when you need it, with pluggable failure policies.
- **Autoscaling consumer groups** driven by queue depth or consumer lag.
- **Switch backends without changing your code.** Same topic, same handler, six transports.
- **Pluggable message codecs.** JSON by default; Protobuf, zero-copy SBE, raw bytes, or your own.
- **Confluent Schema Registry** for Kafka — opt-in encode on publish and decode on consume (Confluent and Redpanda), with subject enforcement.

## 30-second tour

In-process, no Docker, no credentials:

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

Swap `InMemory` for `RabbitMq`, `Sqs`, `Nats`, `Kafka`, or `Redis` and the topic and handler stay identical. Per-backend setup: [Getting Started](https://shove.rs/getting-started).

## Backends

| Backend              | Feature flag    | Marker     |
| -------------------- | --------------- | ---------- |
| RabbitMQ             | `rabbitmq`      | `RabbitMq` |
| AWS SNS+SQS          | `aws-sns-sqs`   | `Sqs`      |
| NATS JetStream       | `nats`          | `Nats`     |
| Apache Kafka         | `kafka`         | `Kafka`    |
| Redis/Valkey Streams | `redis-streams` | `Redis`    |
| In-process           | `inmemory`      | `InMemory` |

`cargo add shove --features <flag>`. Need help choosing? [Choosing a backend](https://shove.rs/backends/choosing).

Optional add-ons: `audit`, `metrics`, `kafka-ssl`, `rabbitmq-transactional`, `protobuf`, `sbe`, `env-config`. Codec details, including the zero-copy SBE path: [Codecs](https://shove.rs/concepts/codecs). Configuring the tuning knobs from environment variables: [Environment Configuration](https://shove.rs/ops/env-config).

## Delivery

At-least-once by default. Handlers return one of:

- `Ack` — success
- `Retry` — delayed retry through hold queues with escalating backoff
- `Reject` — dead-letter immediately
- `Defer` — delay without consuming a retry budget

Full semantics: [Outcomes & Delivery](https://shove.rs/concepts/outcomes).

## Performance

Benchmarked across all six backends. The charts are generated from a committed
results document — never hand-copied — and each one carries its own provenance
(shove version, generation date, hardware) in the caption.

![Throughput vs consumer count, per backend](https://raw.githubusercontent.com/zannis/shove/main/docs/public/bench/throughput-vs-consumers-dark.svg)

![Framework overhead per flow, nanoseconds per message](https://raw.githubusercontent.com/zannis/shove/main/docs/public/bench/framework-overhead-dark.svg)

Throughput vs payload size, the cost of sequenced ordering, and dispatch latency
percentiles: [Performance](https://shove.rs/ops/performance). Reproduce with
`cargo run --release --example <backend>_stress --features <flag>` using the
feature flag from the table above; the exact published matrix and the chart
regeneration command are in
[Measurement methodology](https://shove.rs/ops/performance#measurement-methodology).

## Learn more

- [Getting Started](https://shove.rs/getting-started)
- [Core concepts](https://shove.rs/concepts/topics)
- [Guides](https://shove.rs/guides/retries) — retries, sequenced delivery, consumer groups, audit, observability, exactly-once, shutdown, liveness
- [Backends](https://shove.rs/backends/choosing)
- [docs.rs/shove](https://docs.rs/shove)

## Requirements

- Rust 1.91+ (edition 2024)
- Redis 6.2+ or Valkey (when using `redis-streams`)

## Maintainer

Built and maintained by [Zannis Kalampoukis](https://zannis.xyz) at
[Synchronicity Labs](https://synchronicitylabs.io).

## License

[MIT](LICENSE)
