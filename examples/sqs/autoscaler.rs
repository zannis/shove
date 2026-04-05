//! SQS Autoscaler example.
//!
//! Demonstrates: `SqsAutoscaler`, `AutoscalerConfig`, `SqsConsumerGroupRegistry`,
//! and dynamic scaling of SQS consumer groups based on queue depth.
//!
//! The autoscaler polls SQS queue attributes on a configurable interval and
//! scales consumer groups up or down using hysteresis (condition must be
//! sustained for `hysteresis_duration`) and cooldown (minimum gap between
//! consecutive scaling actions) to prevent flapping.
//!
//! Requires a running LocalStack instance (see docker-compose.yml):
//!
//!     docker compose up -d
//!     cargo run --example sqs_autoscaler --features aws-sns-sqs

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::sns::*;
use shove::*;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

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
    TopologyBuilder::new("sqs-autoscale-work").dlq().build()
);

// ─── Handler ────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct TaskHandler;

impl MessageHandler<WorkQueue> for TaskHandler {
    async fn handle(&self, msg: TaskEvent, meta: MessageMetadata) -> Outcome {
        println!(
            "[worker] task={} attempt={}",
            msg.task_id,
            meta.retry_count + 1,
        );
        // Simulate slow work so messages queue up and trigger scale-up.
        tokio::time::sleep(Duration::from_millis(500)).await;
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

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), ShoveError> {
    require_localstack();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shove=debug,sqs_autoscaler=debug".parse().unwrap()),
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

    // ── Declare topology and publish a burst ──
    let declarer = SnsTopologyDeclarer::new(client.clone(), topic_registry.clone())
        .with_queue_registry(queue_registry.clone());
    declare_topic::<WorkQueue>(&declarer).await?;

    let publisher = SnsPublisher::new(client.clone(), topic_registry.clone());
    let burst_size = 60usize;
    for i in 0..burst_size {
        publisher
            .publish::<WorkQueue>(&TaskEvent {
                task_id: format!("TASK-{i:03}"),
                payload: format!("work item {i}"),
            })
            .await?;
    }
    println!("published {burst_size} tasks\n");

    // ── Set up consumer group registry ──
    //
    // Consumer groups manage identical consumers reading from the same queue.
    // `register` declares the topology and prepares the group; `start_all`
    // starts consumers at their minimum count (1 here).
    let mut registry = SqsConsumerGroupRegistry::new(
        client.clone(),
        topic_registry.clone(),
        queue_registry.clone(),
    );

    registry
        .register::<WorkQueue, TaskHandler>(
            SqsConsumerGroupConfig::new(1..=5) // scale between 1 and 5 consumers
                .with_prefetch_count(5) // each consumer holds up to 5 in-flight messages
                .with_max_retries(3),
            || TaskHandler,
        )
        .await?;

    registry.start_all();
    println!("consumer group started (min=1, max=5)\n");

    let registry = Arc::new(Mutex::new(registry));

    // ── Set up SqsAutoscaler ──
    //
    // The autoscaler polls SQS `GetQueueAttributes` and computes:
    //   capacity = active_consumers × prefetch_count
    //   scale up   when messages_ready > capacity × scale_up_multiplier
    //   scale down when messages_ready < capacity × scale_down_multiplier
    //
    // Hysteresis prevents flapping: the condition must be sustained for
    // `hysteresis_duration` before action is taken.
    // Cooldown prevents back-to-back scaling: at least `cooldown_duration`
    // must elapse between two scaling actions for the same group.
    let stats_provider = SqsQueueStatsProvider::new(client.clone(), queue_registry.clone());

    let auto_config = AutoscalerConfig {
        poll_interval: Duration::from_secs(2),
        // Scale up when messages_ready > capacity × 1.5
        scale_up_multiplier: 1.5,
        // Scale down when messages_ready < capacity × 0.3
        scale_down_multiplier: 0.3,
        // Condition must hold for 4 s before acting
        hysteresis_duration: Duration::from_secs(4),
        // At least 8 s between two scaling actions
        cooldown_duration: Duration::from_secs(8),
    };

    let mut autoscaler = SqsAutoscaler::new(stats_provider, auto_config);
    let shutdown = CancellationToken::new();

    let r = registry.clone();
    let s = shutdown.clone();
    let autoscaler_task = tokio::spawn(async move {
        autoscaler.run(r, s).await;
    });

    // ── Monitor — watch consumer count change as load spikes then drains ──
    //
    // Phase 1: queue is deep → autoscaler scales up toward max_consumers.
    // Phase 2: queue drains  → autoscaler scales back down to min_consumers.
    println!("autoscaler running — watching consumer count change\n");

    let monitor_stats = SqsQueueStatsProvider::new(client.clone(), queue_registry.clone());
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(3)).await;

        let stats = monitor_stats
            .get_queue_stats(WorkQueue::topology().queue())
            .await;
        let active = {
            let reg = registry.lock().await;
            reg.groups()
                .values()
                .next()
                .map(|g| g.active_consumers())
                .unwrap_or(0)
        };

        match stats {
            Ok(s) => println!(
                "[monitor] consumers={active} messages_ready={} in_flight={}",
                s.messages_ready, s.messages_not_visible,
            ),
            Err(e) => eprintln!("[monitor] failed to fetch stats: {e}"),
        }
    }

    // ── Shutdown ──
    println!("\nshutting down…");
    shutdown.cancel();
    let _ = autoscaler_task.await;
    registry.lock().await.shutdown_all().await;
    client.shutdown().await;
    println!("done");

    Ok(())
}
