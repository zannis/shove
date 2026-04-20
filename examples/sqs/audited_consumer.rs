//! Audited consumer example — custom `AuditHandler` that writes to stdout (SQS backend).
//!
//! Demonstrates: `MessageHandlerExt::audited` wrapper, custom `AuditHandler`
//! implementation, trace ID propagation across retries.
//!
//! Requires a running LocalStack instance (see docker-compose.yml):
//!
//!     docker compose up -d
//!     cargo run --example sqs_audited_consumer --features aws-sns-sqs,audit

use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::sns::SnsConfig;
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
    TopologyBuilder::new("sqs-audited-payments")
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

fn require_localstack() {
    let output = std::process::Command::new("docker")
        .args(["compose", "ps", "--services", "--filter", "status=running"])
        .output();
    match output {
        Ok(o) if String::from_utf8_lossy(&o.stdout).contains("localstack") => {}
        _ => {
            eprintln!("LocalStack is not running. Start it with:\n\n    docker compose up -d\n");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), ShoveError> {
    require_localstack();

    // SAFETY: called before any concurrent env access in this process.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    }

    let broker = Broker::<Sqs>::new(SnsConfig {
        region: Some("us-east-1".into()),
        endpoint_url: Some("http://localhost:4566".into()),
    })
    .await?;

    // ── Declare topology ──
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

    // ── Start audited consumer ──
    //
    // `audited(audit)` wraps the handler in the `Audited` decorator — the
    // supervisor sees a normal `MessageHandler<Payments>`, no API changes needed.
    let mut supervisor = broker.consumer_supervisor();
    supervisor.register::<Payments, _>(
        PaymentHandler.audited(StdoutAuditHandler),
        ConsumerOptions::<Sqs>::new().with_max_retries(3),
    )?;

    // Let it process (first attempt retries, second acks), then shut down.
    let outcome = supervisor
        .run_until_timeout(
            async {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            },
            Duration::from_secs(5),
        )
        .await;

    println!("done");
    std::process::exit(outcome.exit_code());
}
