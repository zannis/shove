//! Redis Streams consumer-group example.
//!
//! Registers a single consumer group with `min_consumers=2, max_consumers=4`,
//! starts it, publishes a batch of work, and lets the group drain on Ctrl-C
//! (or a 30 s timeout).
//!
//! Run with:
//!     docker run --rm -p 6379:6379 redis:7-alpine
//!     cargo run --example redis_consumer_groups --features redis-streams

use std::time::Duration;

use serde::{Deserialize, Serialize};

use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode};
use shove::{
    Broker, ConsumerGroupConfig, JsonCodec, MessageHandler, MessageMetadata, Outcome, Redis, Topic,
    TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Message + topic
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkItem {
    id: u64,
    payload: String,
}

struct WorkQueue;
impl Topic for WorkQueue {
    type Message = WorkItem;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("work-queue")
                .hold_queue(Duration::from_secs(5))
                .dlq()
                .build()
        })
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct WorkHandler;
impl MessageHandler<WorkQueue> for WorkHandler {
    type Context = ();
    async fn handle(&self, msg: WorkItem, meta: MessageMetadata, _: &()) -> Outcome {
        println!(
            "[worker] id={} payload={} attempt={}",
            msg.id,
            msg.payload,
            meta.retry_count + 1,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), shove::ShoveError> {
    tracing_subscriber::fmt::init();

    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());

    let broker = Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone { url },
        group: Some("worker-group".into()),
    })
    .await?;

    broker.topology().declare::<WorkQueue>().await?;

    // Publish a batch first so the group has something to chew on at startup.
    let publisher = broker.publisher().await?;
    for i in 1..=20u64 {
        publisher
            .publish::<WorkQueue>(&WorkItem {
                id: i,
                payload: format!("job-{i:03}"),
            })
            .await?;
    }
    println!("published 20 work items");

    // Register one group with min=2, max=4. The registry starts `min_consumers`
    // tasks at run_until_timeout; the autoscaler can grow up to max.
    let mut group = broker.consumer_group();
    group
        .register::<WorkQueue, _>(
            ConsumerGroupConfig::new(
                RedisConsumerGroupConfig::new(2..=4).with_prefetch_count(4),
            ),
            || WorkHandler,
        )
        .await?;

    println!("consuming with 2 initial consumers — Ctrl-C to stop\n");
    let outcome = group
        .run_until_timeout(
            async {
                tokio::signal::ctrl_c().await.ok();
            },
            Duration::from_secs(30),
        )
        .await;

    println!(
        "shutdown: errors={} panics={} timed_out={}",
        outcome.errors, outcome.panics, outcome.timed_out
    );
    std::process::exit(outcome.exit_code());
}
