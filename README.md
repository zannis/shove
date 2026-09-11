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

The charts are generated from a committed results document — never hand-copied —
and each one carries its own provenance (shove version, generation date,
hardware) in the caption. The harness runs against every backend; the currently
published document measures all six backends, and a plotted series appears only
for a backend the document actually contains. The SQS series measures LocalStack
rather than the AWS service and runs a smaller, recorded corpus. See
[Measurement methodology](https://shove.rs/ops/performance#measurement-methodology)
for what is measured and how.

![Throughput vs consumer count, per backend](https://raw.githubusercontent.com/zannis/shove/main/docs/public/bench/throughput-vs-consumers-dark.svg)

![Framework overhead per flow, nanoseconds per message](https://raw.githubusercontent.com/zannis/shove/main/docs/public/bench/framework-overhead-dark.svg)

Throughput vs payload size, the cost of sequenced ordering, and dispatch latency
percentiles: [Performance](https://shove.rs/ops/performance). Every published row
comes from one pinned matrix, so a run is reproduced with
`scripts/bench.sh <backend>` rather than by invoking an example directly; that
script and the chart regeneration are described under
[Measurement methodology](https://shove.rs/ops/performance#measurement-methodology).

**Batch consumption.** `BatchConsumer` hands the handler up to `max_batch_size`
messages per call, so whatever the handler costs per invocation is paid once per
flush rather than once per message. The table reads the batch flow's drain
ceiling off the same results document: the best cell the charts publish for
each backend and payload, its consumer count, and the ratio to the plain
parallel consumer at that same cell — on SQS, which has no `consume_parallel`
rows, the comparator is `supervisor`. Batches are up to 500 messages or 250 ms,
except on SQS, whose API caps a batch at 10. Batching leads most where messages
are small and per-message framework work dominates; at 64 KiB the batch flow is
at most 1.5x the parallel one on every backend, because the cost there is
moving bytes.

| Backend | 64 B (msg/s) | 1 KiB (msg/s) | 64 KiB (msg/s) |
|---|---|---|---|
| In-process | 4.33M (8c), 9.6x parallel | 1.09M (4c), 3.0x | 80k (8c), 1.1x |
| Kafka | 963k (8c), 1.2x | 1.50M (8c), 2.9x | 42k (2c), 1.0x |
| NATS | 101k (8c), 1.1x | 81k (2c), 1.0x | 12k (1c), 1.1x |
| RabbitMQ | 35k (2c), 1.6x | 36k (4c), 1.2x | 31k (4c), 1.3x |
| Redis | 1.29M (8c), 2.1x | 770k (8c), 1.5x | 41k (4c), 1.0x |
| SQS (LocalStack) | 3.2k (8c), 1.1x | 3.3k (8c), 2.5x | 1.9k (8c), 1.1x |

**What batching costs.** A batch flushes when it fills or when `max_batch_age`
expires, whichever comes first, so the rates above are bought with dispatch
latency — and the batch flow is on no latency chart, because the
dispatch-latency family plots the parallel flow only. Over the 120 paired
offered-load rungs of the same document (`consume_batch` against the plain
parallel consumer at the same backend, payload, consumer count and offered rate,
both arms sustaining the rate), batch dispatch p99 runs 6.1-262 ms against
0.14-340 ms for the comparator, and batch is the slower arm in 115 of them; the
five exceptions are all in-process rungs where the parallel consumer was itself
the slower one. What sets the cost is `max_batch_age`, not batching: at the
5,000 msg/s rung a 500-message batch does not fill inside 250 ms, so p99 lands
on the age bound in 60 of 60 rungs, 100-262 ms; by 100,000 msg/s the size bound
fires first and batch p99 falls as low as 6.1 ms. Lower `max_batch_age` to buy
the latency back, at the cost of the amortisation the table measures. SQS
contributes no rung — its batch consumers never kept pace with the paced
producer, so its percentiles there measure backlog rather than dispatch. Every
bound is rounded outward, so no rung falls outside a range stated here.

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
