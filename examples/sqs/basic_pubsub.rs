//! Basic pub/sub examples covering all non-sequenced topology configurations (SQS backend).
//!
//! Demonstrates: `define_topic!`, all `Outcome` variants (`Ack`, `Retry`, `Reject`),
//! `publish`, `publish_with_headers`, `publish_batch`, `run`, `run_dlq`,
//! and `handle_dead`.
//!
//! Requires a running LocalStack instance (see docker-compose.yml):
//!
//!     docker compose up -d
//!     cargo run --example sqs_basic_pubsub --features aws-sns-sqs

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::sns::*;
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
    TopologyBuilder::new("sqs-minimal-orders").build()
);

// 2. With DLQ: rejected messages land in a dead-letter queue for inspection.
define_topic!(
    DlqOrder,
    OrderEvent,
    TopologyBuilder::new("sqs-dlq-orders").dlq().build()
);

// 3. With hold queues + DLQ: escalating retry backoff (5 s → 30 s → 120 s).
//    Messages that exceed max_retries are sent to DLQ.
define_topic!(
    RetryOrder,
    OrderEvent,
    TopologyBuilder::new("sqs-retry-orders")
        .hold_queue(Duration::from_secs(5))
        .hold_queue(Duration::from_secs(30))
        .hold_queue(Duration::from_secs(120))
        .dlq()
        .build()
);

// ─── Handlers ───────────────────────────────────────────────────────────────

// Acks every message.
struct AckHandler;

impl MessageHandler<MinimalOrder> for AckHandler {
    type Context = ();
    async fn handle(&self, msg: OrderEvent, metadata: MessageMetadata, _: &()) -> Outcome {
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
    type Context = ();
    async fn handle(&self, msg: OrderEvent, _metadata: MessageMetadata, _: &()) -> Outcome {
        println!("[dlq] rejecting order={} → DLQ", msg.order_id);
        Outcome::Reject
    }

    async fn handle_dead(&self, msg: OrderEvent, metadata: DeadMessageMetadata, _: &()) {
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
    type Context = ();
    async fn handle(&self, msg: OrderEvent, metadata: MessageMetadata, _: &()) -> Outcome {
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

// ─── Main ───────────────────────────────────────────────────────────────────

fn require_localstack() {
    let output = std::process::Command::new("docker")
        .args(["compose", "ps", "--services", "--filter", "status=running"])
        .output();
    match output {
        Ok(o) if String::from_utf8_lossy(&o.stdout).contains("localstack") => {}
        _ => {
            eprintln!(
                "LocalStack is not running. Start it with:\n\n    docker compose up -d\n\n\
                 Also ensure AWS credentials are set:\n\
                 export AWS_ACCESS_KEY_ID=test\n\
                 export AWS_SECRET_ACCESS_KEY=test\n"
            );
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), ShoveError> {
    require_localstack();

    // Set dummy credentials for LocalStack.
    // SAFETY: called before any concurrent env access in this process.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    }

    let config = SnsConfig {
        region: Some("us-east-1".into()),
        endpoint_url: Some("http://localhost:4566".into()),
    };
    let client = SnsClient::new(&config).await?;

    // ── Registries ──
    let topic_registry = Arc::new(TopicRegistry::new());
    let queue_registry = Arc::new(QueueRegistry::new());

    // ── Declare all topologies ──
    let declarer = SnsTopologyDeclarer::new(client.clone(), topic_registry.clone())
        .with_queue_registry(queue_registry.clone());
    declare_topic::<MinimalOrder>(&declarer).await?;
    declare_topic::<DlqOrder>(&declarer).await?;
    declare_topic::<RetryOrder>(&declarer).await?;
    println!("topologies declared\n");

    // ── Publish ──
    let publisher = SnsPublisher::new(client.clone(), topic_registry.clone());

    // Single publish
    let order = OrderEvent {
        order_id: "ORD-001".into(),
        amount_cents: 5000,
    };
    publisher.publish::<MinimalOrder>(&order).await?;
    publisher.publish::<DlqOrder>(&order).await?;
    publisher.publish::<RetryOrder>(&order).await?;

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
    let qr = queue_registry.clone();
    let minimal_task = tokio::spawn(async move {
        SqsConsumer::new(c, qr)
            .run::<MinimalOrder>(AckHandler, ConsumerOptions::new(s))
            .await
    });

    let s = shutdown.clone();
    let c = client.clone();
    let qr = queue_registry.clone();
    let dlq_main_task = tokio::spawn(async move {
        SqsConsumer::new(c, qr)
            .run::<DlqOrder>(RejectHandler, ConsumerOptions::new(s))
            .await
    });

    // DLQ consumer — calls handle_dead for each dead-lettered message.
    // Uses the client's own shutdown token (cancelled when client.shutdown() is called).
    let c = client.clone();
    let qr = queue_registry.clone();
    let dlq_task = tokio::spawn(async move {
        SqsConsumer::new(c, qr)
            .run_dlq::<DlqOrder>(RejectHandler)
            .await
    });

    let s = shutdown.clone();
    let c = client.clone();
    let qr = queue_registry.clone();
    let retry_task = tokio::spawn(async move {
        SqsConsumer::new(c, qr)
            .run::<RetryOrder>(RetryHandler, ConsumerOptions::new(s).with_max_retries(3))
            .await
    });

    // ── Let everything run, then shut down ──
    tokio::time::sleep(Duration::from_secs(10)).await;
    println!("\nshutting down...");
    shutdown.cancel();
    client.shutdown().await;

    let _ = tokio::join!(minimal_task, dlq_main_task, dlq_task, retry_task);
    println!("done");

    Ok(())
}
