//! Audited consumer example — custom `AuditHandler` that writes to stdout (SQS backend).
//!
//! Demonstrates: `Audited` wrapper, custom `AuditHandler` implementation,
//! trace ID propagation across retries.
//!
//! Requires a running floci instance (see docker-compose.yml):
//!
//!     docker compose up -d
//!     cargo run --example sqs_audited_consumer --features aws-sns-sqs,audit

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::sns::*;
use shove::*;
use tokio_util::sync::CancellationToken;

// ─── Message type ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentEvent {
    payment_id: String,
    amount_cents: u64,
}

// ─── Topic ──────────────────────────────────────────────────────────────────

define_topic!(
    Payments,
    PaymentEvent,
    TopologyBuilder::new("sqs-audited-payments")
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// ─── Handler ────────────────────────────────────────────────────────────────

struct PaymentHandler;

impl MessageHandler<Payments> for PaymentHandler {
    async fn handle(&self, msg: PaymentEvent, metadata: MessageMetadata) -> Outcome {
        println!(
            "[handler] payment={} amount=${:.2} attempt={}",
            msg.payment_id,
            msg.amount_cents as f64 / 100.0,
            metadata.retry_count + 1,
        );
        // Simulate a transient failure on first attempt to show trace ID
        // persisting across retries.
        if metadata.retry_count == 0 {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    }
}

// ─── Custom audit handler ───────────────────────────────────────────────────

/// Prints every audit record to stdout as JSON.
struct StdoutAuditHandler;

impl AuditHandler<Payments> for StdoutAuditHandler {
    async fn audit(&self, record: &AuditRecord<PaymentEvent>) -> Result<(), ShoveError> {
        let json = serde_json::to_string_pretty(record).map_err(ShoveError::Serialization)?;
        println!("[audit] {json}");
        Ok(())
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn require_floci() {
    let output = std::process::Command::new("docker")
        .args(["compose", "ps", "--services", "--filter", "status=running"])
        .output();
    match output {
        Ok(o) if String::from_utf8_lossy(&o.stdout).contains("floci") => {}
        _ => {
            eprintln!("floci is not running. Start it with:\n\n    docker compose up -d\n");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), ShoveError> {
    require_floci();

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

    let topic_registry = Arc::new(TopicRegistry::new());
    let queue_registry = Arc::new(QueueRegistry::new());

    // ── Declare topology ──
    let declarer = SnsTopologyDeclarer::new(client.clone(), topic_registry.clone())
        .with_queue_registry(queue_registry.clone());
    declare_topic::<Payments>(&declarer).await?;
    println!("topology declared\n");

    // ── Publish a payment ──
    let publisher = SnsPublisher::new(client.clone(), topic_registry.clone());
    let event = PaymentEvent {
        payment_id: "PAY-001".into(),
        amount_cents: 4999,
    };
    publisher.publish::<Payments>(&event).await?;
    println!("published payment\n");

    // ── Start audited consumer ──
    //
    // Wrap the handler with `Audited` — the consumer sees a normal
    // `MessageHandler<Payments>`, no API changes needed.
    let audited_handler = Audited::new(PaymentHandler, StdoutAuditHandler);
    let shutdown = CancellationToken::new();

    let s = shutdown.clone();
    let c = client.clone();
    let qr = queue_registry.clone();
    let consumer_task = tokio::spawn(async move {
        SqsConsumer::new(c, qr)
            .run::<Payments>(audited_handler, ConsumerOptions::new(s).with_max_retries(3))
            .await
    });

    // ── Let it process (first attempt retries, second acks) ──
    tokio::time::sleep(Duration::from_secs(15)).await;

    println!("\nshutting down...");
    shutdown.cancel();
    client.shutdown().await;
    let _ = consumer_task.await;
    println!("done");

    Ok(())
}
