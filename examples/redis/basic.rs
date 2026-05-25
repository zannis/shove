//! Basic Redis Streams publish/consume round-trip.
//!
//! Publishes three `OrderPaid` messages, then consumes them until Ctrl-C.
//!
//! Run with:
//!   docker run --rm -p 6379:6379 redis:7-alpine
//!   cargo run --example redis_basic --features redis-streams

use std::time::Duration;

use serde::{Deserialize, Serialize};

use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode};
use shove::{
    Broker, ConsumerGroupConfig, JsonCodec, MessageHandler, MessageMetadata, Outcome, Redis, Topic,
    TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Message type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderPaid {
    order_id: String,
}

// ---------------------------------------------------------------------------
// Topic definition
// ---------------------------------------------------------------------------

struct Orders;
impl Topic for Orders {
    type Message = OrderPaid;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("orders")
                .hold_queue(Duration::from_secs(5))
                .dlq()
                .build()
        })
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct Handler;
impl MessageHandler<Orders> for Handler {
    type Context = ();
    async fn handle(&self, msg: OrderPaid, meta: MessageMetadata, _: &()) -> Outcome {
        println!(
            "received order: {} (retry={})",
            msg.order_id, meta.retry_count
        );
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

    let broker = Broker::<Redis>::new(RedisConfig::new(RedisMode::Standalone { url })).await?;

    broker.topology().declare::<Orders>().await?;

    // Publish a few orders.
    let publisher = broker.publisher().await?;
    for i in 1..=3u32 {
        publisher
            .publish::<Orders>(&OrderPaid {
                order_id: format!("ORD-{i:04}"),
            })
            .await?;
        println!("published ORD-{i:04}");
    }

    // Consume until Ctrl-C.
    let mut group = broker.consumer_group();
    group
        .register::<Orders, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::new(1..=1)),
            || Handler,
        )
        .await?;

    println!("consuming — press Ctrl-C to stop");
    let outcome = group
        .run_until_timeout(
            async {
                tokio::signal::ctrl_c().await.ok();
            },
            Duration::from_secs(5),
        )
        .await;

    std::process::exit(outcome.exit_code());
}
