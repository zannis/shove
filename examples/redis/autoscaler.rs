//! Redis Streams autoscaler example.
//!
//! Demonstrates `RedisAutoscalerBackend::autoscaler(...)` — polls XLEN +
//! XPENDING on a tight interval, scales the consumer group up when the
//! backlog grows past `messages_ready > capacity × scale_up_multiplier`, and
//! scales back down once the queue drains.
//!
//! Run with:
//!     docker run --rm -p 6379:6379 redis:7-alpine
//!     cargo run --example redis_autoscaler --features redis-streams

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use shove::redis::{
    RedisAutoscalerBackend, RedisConfig, RedisConsumerGroupConfig, RedisConsumerGroupRegistry,
    RedisMode, RedisQueueStatsProvider, XlenStatsProvider,
};
use shove::{
    AutoscalerConfig, Backend, Broker, JsonCodec, MessageHandler, MessageMetadata, Outcome,
    QueueTopology, Redis, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Message + topic
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkItem {
    id: u64,
}

struct WorkQueue;
impl Topic for WorkQueue {
    type Message = WorkItem;
    type Codec = JsonCodec;
    fn topology() -> &'static QueueTopology {
        static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("autoscaler-work").build())
    }
}

// ---------------------------------------------------------------------------
// Handler — slow on purpose so backlog accumulates and triggers scale-up
// ---------------------------------------------------------------------------

struct SlowHandler;
impl MessageHandler<WorkQueue> for SlowHandler {
    type Context = ();
    async fn handle(&self, msg: WorkItem, _meta: MessageMetadata, _: &()) -> Outcome {
        println!("[worker] handling id={}", msg.id);
        tokio::time::sleep(Duration::from_millis(500)).await;
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "shove=info,redis_autoscaler=info".parse().unwrap()),
        )
        .init();

    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let cfg = RedisConfig {
        mode: RedisMode::Standalone { url },
        group: Some("autoscaler-grp".into()),
    };

    // Build the underlying client + Broker wrapper. We need both: the
    // registry/autoscaler talk directly to the client, while the broker is
    // used for the topology declarer + publisher.
    let client = <Redis as Backend>::connect(cfg).await?;
    let broker = Broker::<Redis>::from_client(client.clone());
    broker.topology().declare::<WorkQueue>().await?;

    // Build a registry + register one slow handler. min=1, max=5.
    let mut registry = RedisConsumerGroupRegistry::new(client.clone());
    registry
        .register::<WorkQueue, SlowHandler>(
            RedisConsumerGroupConfig::new(1..=5).with_prefetch_count(1),
            || SlowHandler,
            (),
        )
        .await?;
    registry.start_all();
    println!("consumer group started (min=1, max=5)");

    // Publish a burst of work — enough to push backlog past the scale-up
    // threshold for the active capacity.
    let publisher = broker.publisher().await?;
    for i in 0..40u64 {
        publisher.publish::<WorkQueue>(&WorkItem { id: i }).await?;
    }
    println!("published 40 work items\n");

    let registry = Arc::new(Mutex::new(registry));

    // Tight thresholds so this example finishes quickly. Tune for production.
    let auto = AutoscalerConfig {
        poll_interval: Duration::from_secs(1),
        scale_up_multiplier: 1.5,
        scale_down_multiplier: 0.3,
        hysteresis_duration: Duration::from_secs(2),
        cooldown_duration: Duration::from_secs(3),
    };

    let mut autoscaler =
        RedisAutoscalerBackend::autoscaler(client.clone(), registry.clone(), auto);
    let shutdown = CancellationToken::new();
    let shutdown_for_task = shutdown.clone();
    let autoscaler_task = tokio::spawn(async move { autoscaler.run(shutdown_for_task).await });

    // Monitor — print active consumer count and queue depth every 2 s.
    let stats = XlenStatsProvider::new(client.clone());
    for _ in 0..15 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let active = registry
            .lock()
            .await
            .groups()
            .get(WorkQueue::topology().queue())
            .map(|g| g.active_consumers())
            .unwrap_or(0);
        match stats
            .get_queue_stats(WorkQueue::topology().queue())
            .await
        {
            Ok(s) => println!(
                "[monitor] consumers={active} ready={} in_flight={}",
                s.messages_ready, s.messages_in_flight
            ),
            Err(e) => eprintln!("[monitor] stats fetch failed: {e}"),
        }
    }

    println!("\nshutting down…");
    shutdown.cancel();
    let _ = autoscaler_task.await;
    registry.lock().await.shutdown_all().await;
    Ok(())
}
