//! Concurrent consumption example.
//!
//! Demonstrates `run` for non-blocking message processing with
//! in-order acknowledgement. Compares sequential vs concurrent throughput
//! with a slow handler.
//!
//! Requires a running RabbitMQ instance (see docker-compose.yml):
//!
//!     docker compose up -d
//!     cargo run --example concurrent_pubsub --features rabbitmq

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use shove::rabbitmq::*;
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
    TopologyBuilder::new("ex-concurrent-tasks")
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
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
    async fn handle(&self, task: Task, _meta: MessageMetadata) -> Outcome {
        // Simulate slow I/O (HTTP call, database query, etc.)
        tokio::time::sleep(self.delay).await;
        eprintln!("  processed task {}", task.id);
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

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

async fn connect() -> (RabbitMqClient, RabbitMqPublisher) {
    let config = RabbitMqConfig::new("amqp://guest:guest@localhost:5673/%2f");
    let client = RabbitMqClient::connect(&config)
        .await
        .expect("failed to connect to RabbitMQ");

    let channel = client.create_channel().await.unwrap();
    RabbitMqTopologyDeclarer::new(channel)
        .declare(SlowTasks::topology())
        .await
        .unwrap();

    let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
    (client, publisher)
}

async fn publish_tasks(publisher: &RabbitMqPublisher, count: u32) {
    let messages: Vec<Task> = (0..count)
        .map(|id| Task {
            id,
            payload: format!("task-{id}"),
        })
        .collect();
    publisher
        .publish_batch::<SlowTasks>(&messages)
        .await
        .unwrap();
}

async fn purge_queue(client: &RabbitMqClient) {
    // Purge via management API to start clean.
    let http = reqwest::Client::new();
    let _ = http
        .delete(format!(
            "http://localhost:15673/api/queues/%2F/{}/contents",
            SlowTasks::topology().queue()
        ))
        .basic_auth("guest", Some("guest"))
        .send()
        .await;
    // Also purge the hold queue.
    let _ = http
        .delete(format!(
            "http://localhost:15673/api/queues/%2F/{}-hold-5s/contents",
            SlowTasks::topology().queue()
        ))
        .basic_auth("guest", Some("guest"))
        .send()
        .await;
    // Brief pause for the purge to take effect.
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Ensure no leftover consumer connections from prior runs.
    drop(client.create_channel().await);
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    require_rabbitmq();
    let msg_count = 20u32;
    let handler_delay = Duration::from_millis(100);
    let prefetch = 20u16;

    let (client, publisher) = connect().await;

    // ── Sequential run ──────────────────────────────────────────────────

    eprintln!("--- Sequential (run) ---");
    eprintln!("  {msg_count} messages, {handler_delay:?} handler delay, prefetch={prefetch}");

    purge_queue(&client).await;
    publish_tasks(&publisher, msg_count).await;

    let handler = SlowTaskHandler::new(handler_delay);
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();

    let start = Instant::now();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s).with_prefetch_count(prefetch);
        consumer.run::<SlowTasks>(h, opts).await
    });

    handler.wait_for(msg_count, Duration::from_secs(60)).await;
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

    eprintln!("--- Concurrent (run) ---");
    eprintln!("  {msg_count} messages, {handler_delay:?} handler delay, prefetch={prefetch}");

    purge_queue(&client).await;
    publish_tasks(&publisher, msg_count).await;

    let handler = SlowTaskHandler::new(handler_delay);
    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let h = handler.clone();
    let s = shutdown.clone();

    let start = Instant::now();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(s).with_prefetch_count(prefetch);
        consumer.run::<SlowTasks>(h, opts).await
    });

    handler.wait_for(msg_count, Duration::from_secs(60)).await;
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
