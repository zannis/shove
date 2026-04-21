//! Sequenced Kafka example — per-key ordering.
//!
//! Run: cargo run -q --example kafka_sequenced --features kafka
//! Requires: Kafka broker at localhost:9092
//!
//! Note: per-key FIFO consumption (`run_fifo`) isn't yet surfaced on the
//! generic `Broker<B>` / `ConsumerSupervisor<B>` / `ConsumerGroup<B>`
//! wrappers — this example therefore keeps using the backend-specific
//! `KafkaConsumer::run_fifo` directly.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::kafka::{
    KafkaClient, KafkaConfig, KafkaConsumer, KafkaPublisher, KafkaTopologyDeclarer,
};
use shove::{
    ConsumerOptions, Kafka, MessageHandler, MessageMetadata, Outcome, SequenceFailure,
    SequencedTopic, Topic, TopologyBuilder,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    action: String,
    seq: u32,
}

shove::define_sequenced_topic!(
    UserEventTopic,
    UserEvent,
    |msg: &UserEvent| msg.user_id.clone(),
    TopologyBuilder::new("kafka-user-events")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(4)
        .hold_queue(Duration::from_secs(1))
        .dlq()
        .build()
);

struct UserEventHandler;

impl MessageHandler<UserEventTopic> for UserEventHandler {
    type Context = ();
    async fn handle(&self, message: UserEvent, metadata: MessageMetadata, _: &()) -> Outcome {
        println!(
            "[user={}] action={} seq={} (retry={})",
            message.user_id, message.action, message.seq, metadata.retry_count
        );
        Outcome::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = KafkaConfig::new("localhost:9092");
    let client = KafkaClient::connect(&config).await?;

    // Declare topology
    let declarer = KafkaTopologyDeclarer::new(client.clone());
    declarer.declare(UserEventTopic::topology()).await?;

    // Publish events for two users
    let publisher = KafkaPublisher::new(client.clone()).await?;
    for seq in 0..5u32 {
        for user in &["alice", "bob"] {
            publisher
                .publish::<UserEventTopic>(&UserEvent {
                    user_id: user.to_string(),
                    action: format!("action-{seq}"),
                    seq,
                })
                .await?;
        }
    }
    println!("Published 10 events (5 per user)");

    // Consume with FIFO ordering
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        shutdown_clone.cancel();
    });

    let consumer = KafkaConsumer::new(client.clone());
    let options = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown);
    consumer
        .run_fifo::<UserEventTopic, _>(UserEventHandler, (), options)
        .await?;

    client.shutdown().await;
    println!("Done.");
    Ok(())
}
