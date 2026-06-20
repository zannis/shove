//! RabbitMQ consumer group + autoscaler example.
//!
//! Demonstrates the one-line autoscaling API: `ConsumerGroup::enable_autoscaling`.
//! The consumer group is built through `broker.consumer_group()`, autoscaling is
//! switched on with a single call, and `run_until_timeout` owns the whole
//! lifecycle — it starts the consumers, spawns the autoscaler (which polls the
//! RabbitMQ Management API for queue depth and scales the group up past
//! `messages_ready > capacity × scale_up_multiplier`, then back down as the
//! queue drains), and drains both cleanly on shutdown.
//!
//! The autoscaler reads its management credentials from the client, so the
//! `RabbitMqConfig` is built with `.with_management(...)`.
//!
//! Spins up a RabbitMQ testcontainer with the management plugin enabled
//! automatically (requires a running Docker daemon):
//!
//!     cargo run --example rabbitmq_consumer_groups --features rabbitmq

use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::rabbitmq::{ManagementConfig, RabbitMqConfig, RabbitMqConsumerGroupConfig};
use shove::{
    AutoscalerConfig, Broker, ConsumerGroupConfig, MessageHandler, MessageMetadata, Outcome,
    RabbitMq, TopologyBuilder, define_topic,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;

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

// ─── Handler ──────────────────────────────────────────────────────────────

// Slow on purpose so backlog accumulates and triggers scale-up.
struct TaskHandler;

impl MessageHandler<WorkQueue> for TaskHandler {
    type Context = ();
    async fn handle(&self, msg: TaskEvent, metadata: MessageMetadata, _: &()) -> Outcome {
        println!(
            "[worker] task={} attempt={}",
            msg.task_id,
            metadata.retry_count + 1,
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        Outcome::Ack
    }
}

/// Shutdown trigger for `run_until_timeout`. This demo runs for a fixed window
/// so the backlog has time to accumulate, scale the group up, and drain, then
/// begins a graceful shutdown. A real service would instead
/// `tokio::signal::ctrl_c().await.ok();` here.
async fn shutdown_signal() {
    tokio::time::sleep(Duration::from_secs(60)).await;
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shove=info,consumer_groups=info".parse().unwrap()),
        )
        .init();

    // Spin up a RabbitMQ testcontainer (the default image enables the
    // management plugin, so the HTTP API the autoscaler polls is available).
    let container = RabbitMqImage::default().start().await?;
    let amqp_port = container.get_host_port_ipv4(5672).await?;
    let mgmt_port = container.get_host_port_ipv4(15672).await?;

    let uri = format!("amqp://guest:guest@localhost:{amqp_port}/%2f");
    let config = RabbitMqConfig::new(&uri).with_management(ManagementConfig::new(
        format!("http://localhost:{mgmt_port}"),
        "guest",
        "guest",
    ));

    let broker = Broker::<RabbitMq>::new(config).await?;
    broker.topology().declare::<WorkQueue>().await?;

    // Publish a burst of work — enough to push backlog past the scale-up
    // threshold for the initial capacity (1 consumer × prefetch 10 = 10).
    let publisher = broker.publisher().await?;
    let burst_size = 100;
    for i in 0..burst_size {
        publisher
            .publish::<WorkQueue>(&TaskEvent {
                task_id: format!("TASK-{i:03}"),
                payload: format!("work item {i}"),
            })
            .await?;
    }
    println!("published {burst_size} tasks; starting group with autoscaling (min=1, max=5)\n");

    // Build the consumer group through the harness. min=1, max=5: autoscaling
    // moves the live consumer count within this range based on queue depth.
    let mut group = broker.consumer_group();
    group
        .register::<WorkQueue, TaskHandler>(
            ConsumerGroupConfig::new(
                RabbitMqConsumerGroupConfig::new(1..=5)
                    .with_prefetch_count(10)
                    .with_max_retries(3),
            ),
            || TaskHandler,
        )
        .await?;

    // Tight thresholds so this example reacts quickly. Tune for production.
    let auto = AutoscalerConfig {
        poll_interval: Duration::from_secs(2),
        scale_up_multiplier: 1.5,
        scale_down_multiplier: 0.3,
        hysteresis_duration: Duration::from_secs(4),
        cooldown_duration: Duration::from_secs(8),
    };

    // One line turns on autoscaling; `run_until_timeout` starts the consumers,
    // spawns the autoscaler against this group's own registry, and drains both
    // when `shutdown_signal` fires.
    let outcome = group
        .enable_autoscaling(auto)
        .run_until_timeout(shutdown_signal(), Duration::from_secs(30))
        .await;

    println!("\nshutdown complete: {outcome:?}");
    drop(container);
    std::process::exit(outcome.exit_code());
}
