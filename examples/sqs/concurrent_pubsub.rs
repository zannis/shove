//! Concurrent consumption example (SQS backend).
//!
//! Demonstrates `run` for non-blocking message processing with in-order
//! acknowledgement. Compares sequential vs concurrent throughput with a
//! slow handler.
//!
//! Requires a running LocalStack instance (see docker-compose.yml):
//!
//!     docker compose up -d
//!     cargo run --example sqs_concurrent_pubsub --features aws-sns-sqs

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use shove::sns::*;
use shove::*;
use tokio_util::sync::CancellationToken;

// ─── Message type ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Task {
    id: u32,
    payload: String,
}

// ─── Topic ──────────────────────────────────────────────────────────────────

define_topic!(
    SlowTasks,
    Task,
    TopologyBuilder::new("sqs-concurrent-tasks").dlq().build()
);

// ─── Handler ────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct SlowTaskHandler {
    processed: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
    delay: Duration,
}

impl SlowTaskHandler {
    fn new(delay: Duration) -> Self {
        Self {
            processed: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
            delay,
        }
    }

    fn count(&self) -> u32 {
        self.processed.load(Ordering::Relaxed)
    }

    async fn wait_for(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.count() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.count() >= target;
                }
            }
        }
    }
}

impl MessageHandler<SlowTasks> for SlowTaskHandler {
    type Context = ();
    async fn handle(&self, task: Task, _meta: MessageMetadata, _: &()) -> Outcome {
        // Simulate slow I/O (HTTP call, database query, etc.)
        tokio::time::sleep(self.delay).await;
        eprintln!("  processed task {}", task.id);
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

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

async fn setup() -> (SnsClient, SnsPublisher, Arc<QueueRegistry>) {
    // SAFETY: called before any concurrent env access in this process.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    }

    let config = SnsConfig {
        region: Some("us-east-1".into()),
        endpoint_url: Some("http://localhost:4566".into()),
    };
    let client = SnsClient::new(&config)
        .await
        .expect("failed to connect to LocalStack");

    let topic_registry = Arc::new(TopicRegistry::new());
    let queue_registry = Arc::new(QueueRegistry::new());

    let declarer = SnsTopologyDeclarer::new(client.clone(), topic_registry.clone())
        .with_queue_registry(queue_registry.clone());
    declarer
        .declare(SlowTasks::topology())
        .await
        .expect("topology declaration failed");

    let publisher = SnsPublisher::new(client.clone(), topic_registry);
    (client, publisher, queue_registry)
}

async fn publish_tasks(publisher: &SnsPublisher, count: u32) {
    let messages: Vec<Task> = (0..count)
        .map(|id| Task {
            id,
            payload: format!("task-{id}"),
        })
        .collect();
    publisher
        .publish_batch::<SlowTasks>(&messages)
        .await
        .expect("publish failed");
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    require_localstack();
    let msg_count = 20u32;
    let handler_delay = Duration::from_millis(100);
    let prefetch = 20u16;

    let (client, publisher, queue_registry) = setup().await;

    // ── Sequential run ──────────────────────────────────────────────────

    eprintln!("--- Sequential (prefetch_count=1) ---");
    eprintln!("  {msg_count} messages, {handler_delay:?} handler delay, prefetch=1");

    publish_tasks(&publisher, msg_count).await;

    let handler = SlowTaskHandler::new(handler_delay);
    let shutdown = CancellationToken::new();
    let h = handler.clone();
    let s = shutdown.clone();
    let c = client.clone();
    let qr = queue_registry.clone();

    let start = Instant::now();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s).with_prefetch_count(1);
        SqsConsumer::new(c, qr).run::<SlowTasks>(h, opts).await
    });

    handler.wait_for(msg_count, Duration::from_secs(120)).await;
    let sequential_dur = start.elapsed();
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let seq_throughput = msg_count as f64 / sequential_dur.as_secs_f64();
    eprintln!(
        "  done: {:.1}s ({:.0} msg/s)\n",
        sequential_dur.as_secs_f64(),
        seq_throughput
    );

    // ── Concurrent run ──────────────────────────────────────────────────
    // The sequential run drained all messages, so we publish a fresh batch.

    eprintln!("--- Concurrent (prefetch_count={prefetch}) ---");
    eprintln!("  {msg_count} messages, {handler_delay:?} handler delay, prefetch={prefetch}");

    publish_tasks(&publisher, msg_count).await;

    let handler = SlowTaskHandler::new(handler_delay);
    let shutdown = CancellationToken::new();
    let h = handler.clone();
    let s = shutdown.clone();
    let c = client.clone();
    let qr = queue_registry.clone();

    let start = Instant::now();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s).with_prefetch_count(prefetch);
        SqsConsumer::new(c, qr).run::<SlowTasks>(h, opts).await
    });

    handler.wait_for(msg_count, Duration::from_secs(120)).await;
    let concurrent_dur = start.elapsed();
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let conc_throughput = msg_count as f64 / concurrent_dur.as_secs_f64();
    eprintln!(
        "  done: {:.1}s ({:.0} msg/s)\n",
        concurrent_dur.as_secs_f64(),
        conc_throughput
    );

    // ── Summary ─────────────────────────────────────────────────────────

    let speedup = sequential_dur.as_secs_f64() / concurrent_dur.as_secs_f64();
    eprintln!("--- Summary ---");
    eprintln!(
        "  sequential:  {:.1}s ({:.0} msg/s)",
        sequential_dur.as_secs_f64(),
        seq_throughput
    );
    eprintln!(
        "  concurrent:  {:.1}s ({:.0} msg/s)",
        concurrent_dur.as_secs_f64(),
        conc_throughput
    );
    eprintln!("  speedup:     {:.1}x", speedup);
    eprintln!();
    eprintln!("  Messages are always acked in delivery order.");
    eprintln!("  Concurrent mode overlaps handler I/O within a single consumer.");

    client.shutdown().await;
}
