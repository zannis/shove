//! Basic pub/sub examples covering all non-sequenced topology configurations.
//!
//! Demonstrates: `define_topic!`, manual `Topic` impl, all `Outcome` variants,
//! `publish`, `publish_with_headers`, `publish_batch`, `run`, `run_dlq`,
//! and `handle_dead`.
//!
//! Requires a running RabbitMQ instance (see docker-compose.yml):
//!
//!     docker compose up -d
//!     cargo run --example basic_pubsub --features rabbitmq

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::rabbitmq::*;
use shove::*;
use tokio_util::sync::CancellationToken;

// ─── Message type ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderEvent {
    order_id: String,
    amount_cents: u64,
}

// ─── Topic definitions ──────────────────────────────────────────────────────

// 1. Minimal: just a queue. No DLQ, no hold queues.
//    Rejected messages are discarded with a warning log.
define_topic!(
    MinimalOrder,
    OrderEvent,
    TopologyBuilder::new("ex-minimal-orders").build()
);

// 2. With DLQ: rejected messages land in a dead-letter queue for inspection.
define_topic!(
    DlqOrder,
    OrderEvent,
    TopologyBuilder::new("ex-dlq-orders").dlq().build()
);

// 3. With hold queues + DLQ: escalating retry backoff (5 s → 30 s → 120 s).
//    Messages that exceed max_retries are sent to DLQ.
define_topic!(
    RetryOrder,
    OrderEvent,
    TopologyBuilder::new("ex-retry-orders")
        .hold_queue(Duration::from_secs(5))
        .hold_queue(Duration::from_secs(30))
        .hold_queue(Duration::from_secs(120))
        .dlq()
        .build()
);

// 4. Manual `Topic` impl — for topics that use `Defer` (hold without retry
//    counter increment). `OnceLock` pattern shown explicitly.
struct ScheduledOrder;

impl Topic for ScheduledOrder {
    type Message = OrderEvent;

    fn topology() -> &'static QueueTopology {
        static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
        TOPOLOGY.get_or_init(|| {
            TopologyBuilder::new("ex-scheduled-orders")
                .hold_queue(Duration::from_secs(10))
                .dlq()
                .build()
        })
    }
}

// ─── Handlers ───────────────────────────────────────────────────────────────

// Acks every message.
struct AckHandler;

impl MessageHandler<MinimalOrder> for AckHandler {
    async fn handle(&self, msg: OrderEvent, metadata: MessageMetadata) -> Outcome {
        println!(
            "[minimal] order={} amount=${:.2} attempt={}",
            msg.order_id,
            msg.amount_cents as f64 / 100.0,
            metadata.retry_count + 1,
        );
        Outcome::Ack
    }
}

// Rejects every message. Also implements handle_dead for DLQ processing.
struct RejectHandler;

impl MessageHandler<DlqOrder> for RejectHandler {
    async fn handle(&self, msg: OrderEvent, _metadata: MessageMetadata) -> Outcome {
        println!("[dlq] rejecting order={} → DLQ", msg.order_id);
        Outcome::Reject
    }

    async fn handle_dead(&self, msg: OrderEvent, metadata: DeadMessageMetadata) {
        println!(
            "[dlq] dead-letter: order={} reason={} deaths={}",
            msg.order_id,
            metadata.reason.as_deref().unwrap_or("unknown"),
            metadata.death_count,
        );
    }
}

// Retries once (simulates transient failure), then acks.
struct RetryHandler;

impl MessageHandler<RetryOrder> for RetryHandler {
    async fn handle(&self, msg: OrderEvent, metadata: MessageMetadata) -> Outcome {
        println!(
            "[retry] order={} attempt={}",
            msg.order_id,
            metadata.retry_count + 1,
        );
        if metadata.retry_count == 0 {
            println!("[retry]   → transient failure, will Retry");
            Outcome::Retry
        } else {
            println!("[retry]   → success on retry");
            Outcome::Ack
        }
    }
}

// Defers the message on first delivery (re-delivers via hold queue without
// incrementing the retry counter), then acks.
struct DeferHandler;

impl MessageHandler<ScheduledOrder> for DeferHandler {
    async fn handle(&self, msg: OrderEvent, metadata: MessageMetadata) -> Outcome {
        println!(
            "[defer] order={} attempt={} redelivered={}",
            msg.order_id,
            metadata.retry_count + 1,
            metadata.redelivered,
        );
        if metadata.retry_count == 0 && !metadata.redelivered {
            println!("[defer]   → not ready yet, deferring to hold queue");
            Outcome::Defer
        } else {
            println!("[defer]   → processing now");
            Outcome::Ack
        }
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn require_rabbitmq() {
    let output = std::process::Command::new("docker")
        .args(["compose", "ps", "--services", "--filter", "status=running"])
        .output();
    match output {
        Ok(o) if String::from_utf8_lossy(&o.stdout).contains("rabbitmq") => {}
        _ => {
            eprintln!("RabbitMQ is not running. Start it with:\n\n    docker compose up -d\n");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), ShoveError> {
    require_rabbitmq();
    let config = RabbitMqConfig::new("amqp://guest:guest@localhost:5673/%2f");
    let client = RabbitMqClient::connect(&config).await?;

    // ── Declare all topologies ──
    let channel = client.create_channel().await?;
    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declare_topic::<MinimalOrder>(&declarer).await?;
    declare_topic::<DlqOrder>(&declarer).await?;
    declare_topic::<RetryOrder>(&declarer).await?;
    declare_topic::<ScheduledOrder>(&declarer).await?;
    println!("topologies declared\n");

    // ── Publish ──
    let publisher = RabbitMqPublisher::new(client.clone()).await?;

    // Single publish
    let order = OrderEvent {
        order_id: "ORD-001".into(),
        amount_cents: 5000,
    };
    publisher.publish::<MinimalOrder>(&order).await?;
    publisher.publish::<DlqOrder>(&order).await?;
    publisher.publish::<RetryOrder>(&order).await?;
    publisher.publish::<ScheduledOrder>(&order).await?;

    // Publish with custom headers
    let mut headers = HashMap::new();
    headers.insert("x-source".into(), "example".into());
    headers.insert("x-priority".into(), "high".into());
    let order2 = OrderEvent {
        order_id: "ORD-002".into(),
        amount_cents: 9900,
    };
    publisher
        .publish_with_headers::<MinimalOrder>(&order2, headers)
        .await?;

    // Batch publish
    let batch = vec![
        OrderEvent {
            order_id: "ORD-003".into(),
            amount_cents: 1000,
        },
        OrderEvent {
            order_id: "ORD-004".into(),
            amount_cents: 2500,
        },
        OrderEvent {
            order_id: "ORD-005".into(),
            amount_cents: 7777,
        },
    ];
    publisher.publish_batch::<MinimalOrder>(&batch).await?;
    println!("messages published\n");

    // ── Start consumers ──
    let shutdown = CancellationToken::new();

    let s = shutdown.clone();
    let c = client.clone();
    let minimal_task = tokio::spawn(async move {
        RabbitMqConsumer::new(c)
            .run::<MinimalOrder>(AckHandler, ConsumerOptions::new(s))
            .await
    });

    let s = shutdown.clone();
    let c = client.clone();
    let dlq_main_task = tokio::spawn(async move {
        RabbitMqConsumer::new(c)
            .run::<DlqOrder>(RejectHandler, ConsumerOptions::new(s))
            .await
    });

    // DLQ consumer — calls handle_dead for each dead-lettered message
    let c = client.clone();
    let dlq_task = tokio::spawn(async move {
        RabbitMqConsumer::new(c)
            .run_dlq::<DlqOrder>(RejectHandler)
            .await
    });

    let s = shutdown.clone();
    let c = client.clone();
    let retry_task = tokio::spawn(async move {
        RabbitMqConsumer::new(c)
            .run::<RetryOrder>(RetryHandler, ConsumerOptions::new(s).with_max_retries(3))
            .await
    });

    let s = shutdown.clone();
    let c = client.clone();
    let defer_task = tokio::spawn(async move {
        RabbitMqConsumer::new(c)
            .run::<ScheduledOrder>(DeferHandler, ConsumerOptions::new(s))
            .await
    });

    // ── Let everything run, then shut down ──
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("\nshutting down...");
    shutdown.cancel();
    client.shutdown().await;

    let _ = tokio::join!(
        minimal_task,
        dlq_main_task,
        dlq_task,
        retry_task,
        defer_task
    );
    println!("done");

    Ok(())
}
