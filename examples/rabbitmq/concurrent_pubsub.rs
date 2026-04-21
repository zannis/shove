//! Concurrent consumption example.
//!
//! Demonstrates `ConsumerOptions::with_concurrent_processing` for non-blocking
//! message processing with in-order acknowledgement. Compares sequential vs
//! concurrent throughput with a slow handler, both driven through the generic
//! `Broker<RabbitMq>::consumer_supervisor()` path.
//!
//! Spins up a RabbitMQ testcontainer automatically (requires a running
//! Docker daemon):
//!
//!     cargo run --example rabbitmq_concurrent_pubsub --features rabbitmq

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use shove::rabbitmq::RabbitMqConfig;
use shove::{
    Broker, ConsumerOptions, MessageHandler, MessageMetadata, Outcome, Publisher, RabbitMq, Topic,
    TopologyBuilder, define_topic,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;

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

async fn connect(amqp_port: u16) -> (Broker<RabbitMq>, Publisher<RabbitMq>) {
    let uri = format!("amqp://guest:guest@localhost:{amqp_port}/%2f");
    let broker = Broker::<RabbitMq>::new(RabbitMqConfig::new(&uri))
        .await
        .expect("failed to connect to RabbitMQ");
    broker
        .topology()
        .declare::<SlowTasks>()
        .await
        .expect("failed to declare topology");
    let publisher = broker
        .publisher()
        .await
        .expect("failed to create publisher");
    (broker, publisher)
}

async fn publish_tasks(publisher: &Publisher<RabbitMq>, count: u32) {
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

async fn purge_queue(mgmt_port: u16) {
    // Purge via management API to start clean.
    let http = reqwest::Client::new();
    let _ = http
        .delete(format!(
            "http://localhost:{mgmt_port}/api/queues/%2F/{}/contents",
            SlowTasks::topology().queue()
        ))
        .basic_auth("guest", Some("guest"))
        .send()
        .await;
    // Also purge the hold queue.
    let _ = http
        .delete(format!(
            "http://localhost:{mgmt_port}/api/queues/%2F/{}-hold-5s/contents",
            SlowTasks::topology().queue()
        ))
        .basic_auth("guest", Some("guest"))
        .send()
        .await;
    // Brief pause for the purge to take effect.
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn run_round(
    broker: &Broker<RabbitMq>,
    handler: SlowTaskHandler,
    prefetch: u16,
    concurrent: bool,
    msg_count: u32,
) -> Duration {
    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<SlowTasks, _>(
            handler.clone(),
            ConsumerOptions::<RabbitMq>::new()
                .with_prefetch_count(prefetch)
                .with_concurrent_processing(concurrent),
        )
        .expect("register");

    let start = Instant::now();

    // Run the supervisor until every expected message has been processed, then
    // drain (bounded).
    let signal_handler = handler.clone();
    let outcome = supervisor
        .run_until_timeout(
            async move {
                signal_handler
                    .wait_for(msg_count, Duration::from_secs(60))
                    .await;
            },
            Duration::from_secs(10),
        )
        .await;
    let duration = start.elapsed();
    assert!(
        outcome.is_clean(),
        "supervisor reported errors: {outcome:?}"
    );
    duration
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let container = RabbitMqImage::default()
        .start()
        .await
        .expect("failed to start RabbitMQ container");
    let amqp_port = container
        .get_host_port_ipv4(5672)
        .await
        .expect("failed to read AMQP port");
    let mgmt_port = container
        .get_host_port_ipv4(15672)
        .await
        .expect("failed to read management port");

    let msg_count = 20u32;
    let handler_delay = Duration::from_millis(100);
    let prefetch = 20u16;

    let (broker, publisher) = connect(amqp_port).await;

    // ── Sequential run ──────────────────────────────────────────────────

    eprintln!("--- Sequential (prefetch_count=1) ---");
    eprintln!("  {msg_count} messages, {handler_delay:?} handler delay, prefetch=1");

    purge_queue(mgmt_port).await;
    publish_tasks(&publisher, msg_count).await;

    let handler = SlowTaskHandler::new(handler_delay);
    let sequential_dur = run_round(&broker, handler, 1, false, msg_count).await;

    let seq_throughput = msg_count as f64 / sequential_dur.as_secs_f64();
    eprintln!(
        "  done: {:.1}s ({:.0} msg/s)\n",
        sequential_dur.as_secs_f64(),
        seq_throughput
    );

    // ── Concurrent run ──────────────────────────────────────────────────

    eprintln!("--- Concurrent (prefetch_count={prefetch}) ---");
    eprintln!("  {msg_count} messages, {handler_delay:?} handler delay, prefetch={prefetch}");

    purge_queue(mgmt_port).await;
    publish_tasks(&publisher, msg_count).await;

    let handler = SlowTaskHandler::new(handler_delay);
    let concurrent_dur = run_round(&broker, handler, prefetch, true, msg_count).await;

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
    eprintln!("  speedup:     {speedup:.1}x");
    eprintln!();
    eprintln!("  Messages are always acked in delivery order.");
    eprintln!("  Concurrent mode overlaps handler I/O within a single consumer.");

    broker.close().await;
    drop(container);
}
