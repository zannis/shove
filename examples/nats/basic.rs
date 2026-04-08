//! Basic NATS JetStream publish/consume example.
//!
//! Run: cargo run -q --example nats_basic --features nats
//! Requires: NATS server with JetStream enabled at localhost:4222

use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::consumer::{Consumer, ConsumerOptions};
use shove::handler::MessageHandler;
use shove::metadata::MessageMetadata;
use shove::nats::{NatsClient, NatsConfig, NatsConsumer, NatsPublisher, NatsTopologyDeclarer};
use shove::outcome::Outcome;
use shove::publisher::Publisher;
use shove::topology::{TopologyBuilder, TopologyDeclarer};
use shove::Topic;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderCreated {
    order_id: String,
    amount: f64,
}

shove::define_topic!(
    OrderTopic,
    OrderCreated,
    TopologyBuilder::new("orders")
        .hold_queue(Duration::from_secs(1))
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

struct OrderHandler;

impl MessageHandler<OrderTopic> for OrderHandler {
    async fn handle(&self, message: OrderCreated, metadata: MessageMetadata) -> Outcome {
        println!(
            "Processing order {} (${:.2}) [retry={}]",
            message.order_id, message.amount, metadata.retry_count
        );
        Outcome::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = NatsConfig::new("nats://localhost:4222");
    let client = NatsClient::connect(&config).await?;

    // Declare topology
    let declarer = NatsTopologyDeclarer::new(client.clone());
    declarer.declare(OrderTopic::topology()).await?;

    // Publish
    let publisher = NatsPublisher::new(client.clone()).await?;
    for i in 0..3 {
        publisher
            .publish::<OrderTopic>(&OrderCreated {
                order_id: format!("ORD-{i}"),
                amount: 99.99 + i as f64,
            })
            .await?;
        println!("Published order ORD-{i}");
    }

    // Consume
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        shutdown_clone.cancel();
    });

    let consumer = NatsConsumer::new(client.clone());
    let options = ConsumerOptions::new(shutdown);
    consumer.run::<OrderTopic>(OrderHandler, options).await?;

    client.shutdown().await;
    println!("Done.");
    Ok(())
}