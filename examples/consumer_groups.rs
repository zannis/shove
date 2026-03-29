//! Consumer group and autoscaler example.
//!
//! Demonstrates: `ConsumerGroup`, `ConsumerGroupConfig`, `ConsumerGroupRegistry`,
//! `Autoscaler`, `AutoscalerConfig`, `ManagementClient`, and dynamic scaling
//! based on queue depth.
//!
//! Requires a running RabbitMQ instance with the management plugin enabled:
//!
//!     rabbitmq-plugins enable rabbitmq_management
//!     cargo run --example consumer_groups --features rabbitmq

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::rabbitmq::*;
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
    TopologyBuilder::new("ex-work-queue")
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// ─── Handler ────────────────────────────────────────────────────────────────

// Handler must be Clone for ConsumerGroup (each spawned consumer gets a clone).
#[derive(Clone)]
struct TaskHandler;

impl MessageHandler<WorkQueue> for TaskHandler {
    async fn handle(&self, msg: TaskEvent, metadata: MessageMetadata) -> Outcome {
        println!(
            "[worker] task={} attempt={}",
            msg.task_id,
            metadata.retry_count + 1,
        );
        // Simulate work
        tokio::time::sleep(Duration::from_millis(100)).await;
        Outcome::Ack
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), ShoveError> {
    let config = RabbitMqConfig::new("amqp://guest:guest@localhost:5672/%2f");
    let client = RabbitMqClient::connect(&config).await?;

    // ── Declare topology ──
    let channel = client.create_channel().await?;
    let declarer = RabbitMqTopologyDeclarer::new(channel);
    declare_topic::<WorkQueue>(&declarer).await?;
    println!("topology declared\n");

    // ── Publish a burst of tasks ──
    let publisher = RabbitMqPublisher::new(client.clone()).await?;
    for i in 0..50 {
        let event = TaskEvent {
            task_id: format!("TASK-{i:03}"),
            payload: format!("work item {i}"),
        };
        publisher.publish::<WorkQueue>(&event).await?;
    }
    println!("published 50 tasks\n");

    // ── Set up consumer group registry ──
    //
    // A ConsumerGroupRegistry manages named groups of identical consumers.
    // Each group reads from a single queue and can be scaled up/down.
    let mut registry = ConsumerGroupRegistry::new(client.clone());

    registry.register::<WorkQueue, TaskHandler>(
        "work-queue-group",             // group name
        WorkQueue::topology().queue(),  // queue to read from
        ConsumerGroupConfig {
            prefetch_count: 10,         // messages per consumer
            min_consumers: 1,           // floor
            max_consumers: 5,           // ceiling
            max_retries: 3,
        },
        || TaskHandler,                 // factory — called once per spawned consumer
    );

    // Start all groups at their minimum consumer count.
    registry.start_all();
    println!("consumer group started (min_consumers=1)\n");

    let registry = Arc::new(Mutex::new(registry));

    // ── Set up autoscaler ──
    //
    // The autoscaler polls the RabbitMQ Management API for queue statistics
    // and scales consumer groups up/down based on queue depth relative to
    // capacity (consumers × prefetch_count).
    let mgmt_config = ManagementConfig::new("http://localhost:15672", "guest", "guest");
    let mgmt_client = ManagementClient::new(mgmt_config);

    let mut autoscaler = Autoscaler::new(
        mgmt_client,
        AutoscalerConfig {
            poll_interval: Duration::from_secs(2),
            // Scale up when messages_ready > capacity × 1.5
            scale_up_multiplier: 1.5,
            // Scale down when messages_ready < capacity × 0.3
            scale_down_multiplier: 0.3,
            // Condition must hold for 4 s before acting (prevents flapping).
            hysteresis_duration: Duration::from_secs(4),
            // At least 10 s between consecutive scaling actions per group.
            cooldown_duration: Duration::from_secs(10),
        },
    );

    let shutdown = client.shutdown_token();
    let r = registry.clone();
    let s = shutdown.clone();
    let autoscaler_task = tokio::spawn(async move {
        autoscaler.run(r, s).await;
    });

    // ── Let the system process and scale ──
    //
    // With 50 queued messages, prefetch=10, and 1 consumer:
    //   capacity = 10, messages_ready = 50 → 50 > 10 × 1.5 → scale up
    // The autoscaler will add consumers until the queue drains or max is hit.
    println!("autoscaler running — watch consumer count change\n");

    for _ in 0..6 {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let reg = registry.lock().await;
        if let Some(group) = reg.groups().get("work-queue-group") {
            println!(
                "[monitor] active_consumers={} queue={}",
                group.active_consumers(),
                group.queue(),
            );
        }
    }

    // ── Shutdown ──
    println!("\nshutting down...");
    registry.lock().await.shutdown_all().await;
    client.shutdown().await;
    let _ = autoscaler_task.await;
    println!("done");

    Ok(())
}
