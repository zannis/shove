//! Redis Streams autoscaler example.
//!
//! Demonstrates the one-line autoscaling API: `ConsumerGroup::enable_autoscaling`.
//! The consumer group is built through `broker.consumer_group()`, autoscaling is
//! switched on with a single call, and `run_until_timeout` owns the whole
//! lifecycle — it starts the consumers, spawns the autoscaler (which polls
//! XINFO GROUPS lag/pending and scales the group up past
//! `messages_ready > capacity × scale_up_multiplier`, then back down as the
//! queue drains), and drains both cleanly on shutdown.
//!
//! Run with:
//!     docker run --rm -p 6379:6379 redis:7-alpine
//!     cargo run --example redis_autoscaler --features redis-streams

use std::time::Duration;

use serde::{Deserialize, Serialize};

use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode};
use shove::{
    AutoscalerConfig, Backend, Broker, ConsumerGroupConfig, JsonCodec, MessageHandler,
    MessageMetadata, Outcome, QueueTopology, Redis, Topic, TopologyBuilder,
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

/// Shutdown trigger for `run_until_timeout`. This demo runs for a fixed window
/// so the backlog has time to accumulate, scale the group up, and drain, then
/// begins a graceful shutdown. A real service would instead
/// `tokio::signal::ctrl_c().await.ok();` here.
async fn shutdown_signal() {
    tokio::time::sleep(Duration::from_secs(30)).await;
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
    let cfg = RedisConfig::new(RedisMode::Standalone { url }).with_group("autoscaler-grp");

    let broker = Broker::<Redis>::from_client(<Redis as Backend>::connect(cfg).await?);
    broker.topology().declare::<WorkQueue>().await?;

    // Build the consumer group through the harness and register one slow
    // handler. min=1, max=5: autoscaling moves the live consumer count within
    // this range based on backlog.
    let mut group = broker.consumer_group();
    group
        .register::<WorkQueue, SlowHandler>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::new(1..=5).with_prefetch_count(1)),
            || SlowHandler,
        )
        .await?;

    // Publish a burst of work — enough to push backlog past the scale-up
    // threshold for the initial capacity. The messages queue in Redis until
    // `run_until_timeout` starts the consumers below.
    let publisher = broker.publisher().await?;
    for i in 0..40u64 {
        publisher.publish::<WorkQueue>(&WorkItem { id: i }).await?;
    }
    println!("published 40 work items; starting group with autoscaling (min=1, max=5)\n");

    // Tight thresholds so this example reacts quickly. Tune for production.
    let auto = AutoscalerConfig {
        poll_interval: Duration::from_secs(1),
        scale_up_multiplier: 1.5,
        scale_down_multiplier: 0.3,
        hysteresis_duration: Duration::from_secs(2),
        cooldown_duration: Duration::from_secs(3),
    };

    // One line turns on autoscaling; `run_until_timeout` starts the consumers,
    // spawns the autoscaler against this group's own registry, and drains both
    // when `shutdown_signal` fires.
    let outcome = group
        .enable_autoscaling(auto)
        .run_until_timeout(shutdown_signal(), Duration::from_secs(30))
        .await;

    println!("\nshutdown complete: {outcome:?}");
    std::process::exit(outcome.exit_code());
}
