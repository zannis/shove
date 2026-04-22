//! Consumer group and autoscaler example.
//!
//! Demonstrates: `ConsumerGroup`, `ConsumerGroupConfig`, `ConsumerGroupRegistry`,
//! `Autoscaler`, `AutoscalerConfig`, `ManagementClient`, and dynamic scaling
//! based on queue depth.
//!
//! Note: the autoscaler machinery isn't yet surfaced on the generic
//! `Broker<B>` wrapper, so this example stays on the backend-specific
//! `ConsumerGroupRegistry` / `RabbitMqAutoscalerBackend` path (same as
//! `sqs_autoscaler`).
//!
//! Spins up a RabbitMQ testcontainer with the management plugin enabled
//! automatically (requires a running Docker daemon):
//!
//!     cargo run --example rabbitmq_consumer_groups --features rabbitmq

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::rabbitmq::{
    ConsumerGroupConfig, ConsumerGroupRegistry, ManagementConfig, RabbitMqAutoscalerBackend,
    RabbitMqClient, RabbitMqConfig, RabbitMqPublisher,
};
use shove::{
    AutoscalerConfig, MessageHandler, MessageMetadata, Outcome, Topic, TopologyBuilder,
    define_topic,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;
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
    type Context = ();
    async fn handle(&self, msg: TaskEvent, metadata: MessageMetadata, _: &()) -> Outcome {
        println!(
            "[worker] task={} attempt={}",
            msg.task_id,
            metadata.retry_count + 1,
        );
        // Simulate work
        tokio::time::sleep(Duration::from_millis(500)).await;
        Outcome::Ack
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shove=debug,consumer_groups=debug".parse().unwrap()),
        )
        .init();

    // Spin up a RabbitMQ testcontainer (management plugin is enabled by default
    // in the `management` tag — the default image already exposes the API).
    let container = RabbitMqImage::default().start().await?;
    let amqp_port = container.get_host_port_ipv4(5672).await?;
    let mgmt_port = container.get_host_port_ipv4(15672).await?;

    let uri = format!("amqp://guest:guest@localhost:{amqp_port}/%2f");
    let config = RabbitMqConfig::new(&uri);
    let client = RabbitMqClient::connect(&config).await?;

    // ── Publish an initial burst of tasks ──
    let publisher = RabbitMqPublisher::new(client.clone()).await?;
    let burst_size = 100;
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
    // A ConsumerGroupRegistry manages named groups of identical consumers.
    // Each group reads from a single queue and can be scaled up/down.
    let mut registry = ConsumerGroupRegistry::new(client.clone());

    registry
        .register::<WorkQueue, TaskHandler>(
            ConsumerGroupConfig::new(1..=5) // min..=max consumers
                .with_prefetch_count(10) // messages per consumer
                .with_max_retries(3),
            || TaskHandler, // factory — called once per spawned consumer
            (),             // handler context (unit for this example)
        )
        .await?;

    // Start all groups at their minimum consumer count.
    registry.start_all();
    println!("consumer group started (min_consumers=1)\n");

    let registry = Arc::new(Mutex::new(registry));

    // ── Set up autoscaler ──
    //
    // The autoscaler polls the RabbitMQ Management API for queue statistics
    // and scales consumer groups up/down based on queue depth relative to
    // capacity (consumers × prefetch_count).
    let mgmt_config =
        ManagementConfig::new(format!("http://localhost:{mgmt_port}"), "guest", "guest");

    let mut autoscaler = RabbitMqAutoscalerBackend::autoscaler(
        &mgmt_config,
        registry.clone(),
        AutoscalerConfig {
            poll_interval: Duration::from_secs(2),
            // Scale up when messages_ready > capacity × 1.5
            scale_up_multiplier: 1.5,
            // Scale down when messages_ready < capacity × 0.3
            scale_down_multiplier: 0.3,
            // Condition must hold for 4 s before acting (prevents flapping).
            hysteresis_duration: Duration::from_secs(4),
            // At least 8 s between consecutive scaling actions per group.
            cooldown_duration: Duration::from_secs(8),
        },
    );

    let shutdown = client.shutdown_token();
    let s = shutdown.clone();
    let autoscaler_task = tokio::spawn(async move {
        autoscaler.run(s).await;
    });

    // ── Let the system process and scale ──
    //
    // With 100 queued messages at 500 ms each, prefetch=10, and 1 consumer:
    //   capacity = 10, messages_ready = 100 → 100 > 10 × 1.5 → scale up
    //
    // Phase 1 – burst processing: the autoscaler adds consumers as the queue
    //   stays deep, up to max_consumers.
    // Phase 2 – drain & settle: once the queue empties, messages_ready drops
    //   below capacity × 0.3, triggering scale-down back to min_consumers.
    println!("autoscaler running — watch consumer count change\n");

    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let reg = registry.lock().await;
        // Group name is derived from the queue name in T::topology()
        if let Some(group) = reg.groups().get(WorkQueue::topology().queue()) {
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

    drop(container);
    Ok(())
}
