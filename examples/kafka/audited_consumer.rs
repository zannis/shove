//! Audited consumer example — custom `AuditHandler` that writes to stdout (Kafka backend).
//!
//! Demonstrates: `Audited` wrapper, custom `AuditHandler` implementation,
//! trace ID propagation across retries.
//!
//! Requires a running Kafka broker:
//!
//!     docker compose up -d
//!     cargo run --example kafka_audited_consumer --features kafka,audit

use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::kafka::*;
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
    TopologyBuilder::new("kafka-audited-payments")
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// ─── Handler ────────────────────────────────────────────────────────────────

struct PaymentHandler;

impl MessageHandler<Payments> for PaymentHandler {
    type Context = ();
    async fn handle(&self, msg: PaymentEvent, metadata: MessageMetadata, _: &()) -> Outcome {
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

#[tokio::main]
async fn main() -> Result<(), ShoveError> {
    let config = KafkaConfig::new("localhost:9092");
    let client = KafkaClient::connect(&config).await?;

    // ── Declare topology ──
    let declarer = KafkaTopologyDeclarer::new(client.clone());
    declarer.declare(Payments::topology()).await?;
    println!("topology declared\n");

    // ── Publish a payment ──
    let publisher = KafkaPublisher::new(client.clone()).await?;
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
    let consumer_task = tokio::spawn(async move {
        KafkaConsumer::new(client.clone())
            .run::<Payments>(audited_handler, ConsumerOptions::new(s).with_max_retries(3))
            .await
    });

    // ── Let it process (first attempt retries, second acks) ──
    tokio::time::sleep(Duration::from_secs(10)).await;

    println!("\nshutting down...");
    shutdown.cancel();
    let _ = consumer_task.await;
    println!("done");

    Ok(())
}
