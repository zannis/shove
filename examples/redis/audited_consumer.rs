//! Audited consumer example — custom `AuditHandler` that writes to stdout (Redis backend).
//!
//! Demonstrates: `MessageHandlerExt::audited` wrapper, custom `AuditHandler`
//! implementation, trace ID propagation across retries.
//!
//! Run with:
//!     docker run --rm -p 6379:6379 redis:7-alpine
//!     cargo run --example redis_audited_consumer --features redis-streams,audit

use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode};
use shove::*;

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
    TopologyBuilder::new("redis-audited-payments")
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
        // Simulate a transient failure on first attempt so the audit trail
        // shows a Retry record followed by an Ack — with the same trace ID.
        if metadata.retry_count == 0 {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    }
}

// ─── Custom audit handler ───────────────────────────────────────────────────

/// Prints every audit record to stdout as JSON. Clone-able so a fresh
/// instance can be handed to each spawned consumer.
#[derive(Clone, Default)]
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());

    let broker = Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone { url },
        group: Some("redis-audited-grp".into()),
        ..Default::default()
    })
    .await?;
    broker.topology().declare::<Payments>().await?;
    println!("topology declared\n");

    // ── Publish a payment ──
    let publisher = broker.publisher().await?;
    let event = PaymentEvent {
        payment_id: "PAY-001".into(),
        amount_cents: 4999,
    };
    publisher.publish::<Payments>(&event).await?;
    println!("published payment\n");

    // ── Start audited consumer via a consumer group ──
    //
    // `audited(audit)` wraps the handler in the `Audited` decorator — the
    // group sees a normal `MessageHandler<Payments>`, no API changes needed.
    let mut group = broker.consumer_group();
    group
        .register::<Payments, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::new(1..=1)),
            || PaymentHandler.audited(StdoutAuditHandler),
        )
        .await?;

    // Let it process (first attempt retries, second acks), then shut down.
    let outcome = group
        .run_until_timeout(
            async {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            },
            Duration::from_secs(10),
        )
        .await;

    println!("done");
    std::process::exit(outcome.exit_code());
}
