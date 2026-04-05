//! Sequenced topic examples demonstrating strict per-key ordering (SQS backend).
//!
//! Demonstrates: `define_sequenced_topic!`, `SequenceFailure::Skip` vs
//! `SequenceFailure::FailAll`, `run_fifo`, and custom `routing_shards`.
//!
//! Each handler sums the `amount_cents` of successfully processed messages per
//! account. After shutdown the totals are compared against expected values to
//! verify that the failure policies work correctly.
//!
//! Published data:
//!   ACC-A  seq 1..5  amounts 100, 200, 300, 400, 500  (seq 3 is poison)
//!   ACC-B  seq 1..3  amounts  50, 100, 150             (no poison)
//!
//! Expected totals:
//!   Skip:    ACC-A = 100+200+400+500 = 1200  (seq 3 DLQ'd, rest continues)
//!            ACC-B = 50+100+150      = 300
//!   FailAll: ACC-A = 100+200         = 300   (seq 3 + subsequent DLQ'd)
//!            ACC-B = 50+100+150      = 300   (independent key, unaffected)
//!
//! Requires a running LocalStack instance (see docker-compose.yml):
//!
//!     docker compose up -d
//!     cargo run --example sqs_sequenced_pubsub --features aws-sns-sqs

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use shove::sns::*;
use shove::*;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

// ─── Message type ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerEntry {
    account_id: String,
    sequence_num: u64,
    amount_cents: i64,
}

// ─── Topic definitions ──────────────────────────────────────────────────────

// Skip policy: if one entry fails, DLQ it and continue the sequence.
// Good for independent events that happen to need ordering (e.g. audit logs).
define_sequenced_topic!(
    SkipLedger,
    LedgerEntry,
    |msg| msg.account_id.clone(),
    TopologyBuilder::new("sqs-skip-ledger")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(2)
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// FailAll policy: if one entry fails, DLQ it AND all subsequent entries for
// the same key. Good for causally dependent sequences (e.g. financial ledger).
define_sequenced_topic!(
    StrictLedger,
    LedgerEntry,
    |msg| msg.account_id.clone(),
    TopologyBuilder::new("sqs-strict-ledger")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(2)
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// ─── Handlers ───────────────────────────────────────────────────────────────

type Totals = Arc<Mutex<HashMap<String, i64>>>;

// Rejects ACC-A seq 3 to demonstrate failure policies.
// Sums amount_cents per account for successfully acked messages.
struct LedgerHandler {
    label: &'static str,
    totals: Totals,
}

impl MessageHandler<SkipLedger> for LedgerHandler {
    async fn handle(&self, msg: LedgerEntry, metadata: MessageMetadata) -> Outcome {
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!(
            "[{}] account={} seq={} amount={} attempt={}",
            self.label,
            msg.account_id,
            msg.sequence_num,
            msg.amount_cents,
            metadata.retry_count + 1,
        );
        if msg.account_id == "ACC-A" && msg.sequence_num == 3 {
            println!("[{}]   → Reject (seq=3 is poison for ACC-A)", self.label);
            Outcome::Reject
        } else {
            *self.totals.lock().await.entry(msg.account_id).or_default() += msg.amount_cents;
            Outcome::Ack
        }
    }
}

impl MessageHandler<StrictLedger> for LedgerHandler {
    async fn handle(&self, msg: LedgerEntry, metadata: MessageMetadata) -> Outcome {
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!(
            "[{}] account={} seq={} amount={} attempt={}",
            self.label,
            msg.account_id,
            msg.sequence_num,
            msg.amount_cents,
            metadata.retry_count + 1,
        );
        if msg.account_id == "ACC-A" && msg.sequence_num == 3 {
            println!(
                "[{}]   → Reject (FailAll: seq=3 + all subsequent for ACC-A will DLQ)",
                self.label,
            );
            Outcome::Reject
        } else {
            *self.totals.lock().await.entry(msg.account_id).or_default() += msg.amount_cents;
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

    let config = SnsConfig {
        region: Some("us-east-1".into()),
        endpoint_url: Some("http://localhost:4566".into()),
    };
    let client = SnsClient::new(&config).await?;

    // ── Registries ──
    let topic_registry = Arc::new(TopicRegistry::new());
    let queue_registry = Arc::new(QueueRegistry::new());

    // ── Declare topologies ──
    let declarer = SnsTopologyDeclarer::new(client.clone(), topic_registry.clone())
        .with_queue_registry(queue_registry.clone());
    declare_topic::<SkipLedger>(&declarer).await?;
    declare_topic::<StrictLedger>(&declarer).await?;
    println!("sequenced topologies declared\n");

    // ── Publish ordered entries for account ACC-A ──
    let publisher = SnsPublisher::new(client.clone(), topic_registry.clone());

    for seq in 1..=5 {
        let entry = LedgerEntry {
            account_id: "ACC-A".into(),
            sequence_num: seq,
            amount_cents: (seq as i64) * 100,
        };
        publisher.publish::<SkipLedger>(&entry).await?;
        publisher.publish::<StrictLedger>(&entry).await?;
    }
    println!("published 5 entries per topic for ACC-A");

    // Also publish for an independent account to show cross-key concurrency.
    for seq in 1..=3 {
        let entry = LedgerEntry {
            account_id: "ACC-B".into(),
            sequence_num: seq,
            amount_cents: (seq as i64) * 50,
        };
        publisher.publish::<SkipLedger>(&entry).await?;
        publisher.publish::<StrictLedger>(&entry).await?;
    }
    println!("published 3 entries per topic for ACC-B\n");

    // ── Start sequenced consumers ──
    let skip_totals: Totals = Arc::new(Mutex::new(HashMap::new()));
    let strict_totals: Totals = Arc::new(Mutex::new(HashMap::new()));
    let shutdown = CancellationToken::new();
    let start = Instant::now();

    let s = shutdown.clone();
    let c = client.clone();
    let qr = queue_registry.clone();
    let t = skip_totals.clone();
    let skip_task = tokio::spawn(async move {
        let handler = LedgerHandler {
            label: "skip error and continue",
            totals: t,
        };
        SqsConsumer::new(c, qr)
            .run_fifo::<SkipLedger>(
                handler,
                ConsumerOptions::new(s)
                    .with_max_retries(2)
                    .with_prefetch_count(8),
            )
            .await
    });

    let s = shutdown.clone();
    let c = client.clone();
    let qr = queue_registry.clone();
    let t = strict_totals.clone();
    let strict_task = tokio::spawn(async move {
        let handler = LedgerHandler {
            label: "fail all after error",
            totals: t,
        };
        SqsConsumer::new(c, qr)
            .run_fifo::<StrictLedger>(
                handler,
                ConsumerOptions::new(s)
                    .with_max_retries(2)
                    .with_prefetch_count(8),
            )
            .await
    });

    // ── Let consumers process, then shut down ──
    tokio::time::sleep(Duration::from_secs(15)).await;
    println!("\nshutting down...");
    shutdown.cancel();
    client.shutdown().await;

    let _ = tokio::join!(skip_task, strict_task);

    // ── Verify totals ──
    let skip = skip_totals.lock().await;
    let strict = strict_totals.lock().await;

    println!(
        "\n── Results (elapsed: {:.1}s) ──",
        start.elapsed().as_secs_f64()
    );
    println!(
        "skip   ACC-A = {} (expected 1200)",
        skip.get("ACC-A").copied().unwrap_or(0)
    );
    println!(
        "skip   ACC-B = {} (expected 300)",
        skip.get("ACC-B").copied().unwrap_or(0)
    );
    println!(
        "strict ACC-A = {} (expected 300)",
        strict.get("ACC-A").copied().unwrap_or(0)
    );
    println!(
        "strict ACC-B = {} (expected 300)",
        strict.get("ACC-B").copied().unwrap_or(0)
    );

    assert_eq!(skip.get("ACC-A").copied().unwrap_or(0), 1200, "skip ACC-A");
    assert_eq!(skip.get("ACC-B").copied().unwrap_or(0), 300, "skip ACC-B");
    assert_eq!(
        strict.get("ACC-A").copied().unwrap_or(0),
        300,
        "strict ACC-A"
    );
    assert_eq!(
        strict.get("ACC-B").copied().unwrap_or(0),
        300,
        "strict ACC-B"
    );

    Ok(())
}
