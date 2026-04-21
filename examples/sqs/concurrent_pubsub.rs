//! Concurrent consumption example (SQS backend).
//!
//! Demonstrates `ConsumerOptions::with_concurrent_processing` for non-blocking
//! message processing with in-order acknowledgement. Compares sequential vs
//! concurrent throughput with a slow handler, both driven through the generic
//! `Broker<Sqs>::consumer_supervisor()` path.
//!
//! Spins up a LocalStack testcontainer automatically. Requires a running
//! Docker daemon and the `LOCALSTACK_AUTH_TOKEN` environment variable:
//!
//!     LOCALSTACK_AUTH_TOKEN=... cargo run --example sqs_concurrent_pubsub --features aws-sns-sqs

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use shove::sns::SnsConfig;
use shove::{
    Broker, ConsumerOptions, MessageHandler, MessageMetadata, Outcome, Publisher, Sqs,
    TopologyBuilder, define_topic,
};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

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

async fn setup(
    endpoint: String,
) -> Result<(Broker<Sqs>, Publisher<Sqs>), Box<dyn std::error::Error>> {
    let broker = Broker::<Sqs>::new(SnsConfig {
        region: Some("us-east-1".into()),
        endpoint_url: Some(endpoint),
    })
    .await?;
    broker.topology().declare::<SlowTasks>().await?;
    let publisher = broker.publisher().await?;
    Ok((broker, publisher))
}

async fn publish_tasks(
    publisher: &Publisher<Sqs>,
    count: u32,
) -> Result<(), shove::ShoveError> {
    let messages: Vec<Task> = (0..count)
        .map(|id| Task {
            id,
            payload: format!("task-{id}"),
        })
        .collect();
    publisher.publish_batch::<SlowTasks>(&messages).await
}

async fn run_round(
    broker: &Broker<Sqs>,
    handler: SlowTaskHandler,
    prefetch: u16,
    concurrent: bool,
    msg_count: u32,
) -> Duration {
    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<SlowTasks, _>(
            handler.clone(),
            ConsumerOptions::<Sqs>::new()
                .with_prefetch_count(prefetch)
                .with_concurrent_processing(concurrent),
        )
        .expect("register");

    let start = Instant::now();

    let signal_handler = handler.clone();
    let outcome = supervisor
        .run_until_timeout(
            async move {
                signal_handler
                    .wait_for(msg_count, Duration::from_secs(120))
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let auth_token = match std::env::var("LOCALSTACK_AUTH_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!(
                "LOCALSTACK_AUTH_TOKEN is not set. This example requires a LocalStack Pro auth \
                 token:\n\n    export LOCALSTACK_AUTH_TOKEN=...\n"
            );
            std::process::exit(1);
        }
    };

    // SAFETY: called before any concurrent env access in this process.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
    }

    let container = LocalStack::default()
        .with_env_var("LOCALSTACK_AUTH_TOKEN", auth_token)
        .start()
        .await?;
    let port = container.get_host_port_ipv4(4566).await?;
    let endpoint = format!("http://localhost:{port}");

    let msg_count = 20u32;
    let handler_delay = Duration::from_millis(100);
    let prefetch = 20u16;

    let (broker, publisher) = setup(endpoint).await?;

    // ── Sequential run ──────────────────────────────────────────────────

    eprintln!("--- Sequential (prefetch_count=1) ---");
    eprintln!("  {msg_count} messages, {handler_delay:?} handler delay, prefetch=1");

    publish_tasks(&publisher, msg_count).await?;

    let handler = SlowTaskHandler::new(handler_delay);
    let sequential_dur = run_round(&broker, handler, 1, false, msg_count).await;

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

    publish_tasks(&publisher, msg_count).await?;

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
    Ok(())
}
