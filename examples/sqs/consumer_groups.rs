//! Consumer group example (SQS backend).
//!
//! Demonstrates: `SqsConsumerGroupRegistry`, `SqsConsumerGroupConfig`,
//! `SqsQueueStatsProvider`, and dynamic queue depth monitoring.
//!
//! Note: SQS has no broker-level coordinated-group primitive — the SQS
//! registry spawns independent poll workers. The generic `Broker<Sqs>`
//! deliberately exposes only a supervisor (see `Sqs`'s doctest), so this
//! example stays on the backend-specific `SqsConsumerGroupRegistry` path.
//!
//! Requires a running LocalStack instance (see docker-compose.yml):
//!
//!     docker compose up -d
//!     cargo run --example sqs_consumer_groups --features aws-sns-sqs

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::sns::*;
use shove::*;
use tokio::sync::Mutex;

// ─── Message type ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskEvent {
    task_id: String,
    payload: String,
}

// ─── Topic ──────────────────────────────────────────────────────────────────

define_topic!(
    WorkQueue,
    TaskEvent,
    TopologyBuilder::new("sqs-work-queue").dlq().build()
);

// ─── Handler ────────────────────────────────────────────────────────────────

// Handler must be Clone for SqsConsumerGroup (each spawned consumer gets a clone).
#[derive(Clone)]
struct TaskHandler;

impl MessageHandler<WorkQueue> for TaskHandler {
    type Context = ();
    async fn handle(&self, msg: TaskEvent, metadata: MessageMetadata, _: &()) -> Outcome {
        println!(
            "[worker] task={} attempt={}",
            msg.task_id,
            metadata.retry_count + 1,
        );
        // Simulate work
        tokio::time::sleep(Duration::from_millis(200)).await;
        Outcome::Ack
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shove=debug,sqs_consumer_groups=debug".parse().unwrap()),
        )
        .init();

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

    // ── Publish an initial burst of tasks ──
    //
    // We declare topology manually here so the publisher can resolve the SNS ARN.
    let declarer = SnsTopologyDeclarer::new(client.clone(), topic_registry.clone())
        .with_queue_registry(queue_registry.clone());
    declarer.declare(WorkQueue::topology()).await?;

    let publisher = SnsPublisher::new(client.clone(), topic_registry.clone());
    let burst_size = 50;
    for i in 0..burst_size {
        let event = TaskEvent {
            task_id: format!("TASK-{i:03}"),
            payload: format!("work item {i}"),
        };
        publisher.publish::<WorkQueue>(&event).await?;
    }
    println!("published {burst_size} tasks\n");

    // ── Set up consumer group registry ──
    //
    // SqsConsumerGroupRegistry manages named groups of identical consumers.
    // It automatically declares the topology and starts consumers at their
    // minimum count. Each group reads from a single SQS queue and can be
    // scaled up/down manually or via a custom autoscaler.
    let mut registry = SqsConsumerGroupRegistry::new(
        client.clone(),
        topic_registry.clone(),
        queue_registry.clone(),
    );

    registry
        .register::<WorkQueue, TaskHandler>(
            SqsConsumerGroupConfig::new(1..=5) // min..=max consumers
                .with_prefetch_count(10) // messages per consumer
                .with_max_retries(3),
            || TaskHandler, // factory — called once per spawned consumer
        )
        .await?;

    // Start all groups at their minimum consumer count.
    registry.start_all();
    println!("consumer group started (min_consumers=1)\n");

    let registry = Arc::new(Mutex::new(registry));

    // ── Monitor queue depth using SqsQueueStatsProvider ──
    //
    // Poll queue attributes to observe the backlog draining.
    let stats_provider = SqsQueueStatsProvider::new(client.clone(), queue_registry.clone());

    println!("monitoring queue depth — watching backlog drain\n");

    for _ in 0..15 {
        tokio::time::sleep(Duration::from_secs(2)).await;

        match stats_provider
            .get_queue_stats(WorkQueue::topology().queue())
            .await
        {
            Ok(stats) => println!(
                "[monitor] messages_ready={} in_flight={}",
                stats.messages_ready, stats.messages_not_visible,
            ),
            Err(e) => eprintln!("[monitor] failed to fetch stats: {e}"),
        }
    }

    // ── Shutdown ──
    println!("\nshutting down...");
    registry.lock().await.shutdown_all().await;
    client.shutdown().await;
    println!("done");

    Ok(())
}
