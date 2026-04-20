//! Sequenced NATS JetStream example — per-key ordering.
//!
//! Run: cargo run -q --example nats_sequenced --features nats
//! Requires: NATS server with JetStream enabled at localhost:4222
//!
//! Note: per-key FIFO consumption (`run_fifo`) isn't yet surfaced on the
//! generic `Broker<B>` / `ConsumerSupervisor<B>` / `ConsumerGroup<B>`
//! wrappers — this example therefore keeps using the backend-specific
//! `NatsConsumer::run_fifo` directly.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::nats::{NatsClient, NatsConfig, NatsConsumer, NatsPublisher, NatsTopologyDeclarer};
use shove::{
    ConsumerOptions, MessageHandler, MessageMetadata, Nats, Outcome, SequenceFailure,
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
    TopologyBuilder::new("user-events")
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

    let config = NatsConfig::new("nats://localhost:4222");
    let client = NatsClient::connect(&config).await?;

    // Declare topology
    let declarer = NatsTopologyDeclarer::new(client.clone());
    declarer.declare(UserEventTopic::topology()).await?;

    // Publish events for two users
    let publisher = NatsPublisher::new(client.clone()).await?;
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

    let consumer = NatsConsumer::new(client.clone());
    let options = ConsumerOptions::<Nats>::new().with_shutdown(shutdown);
    consumer
        .run_fifo::<UserEventTopic>(UserEventHandler, options)
        .await?;

    client.shutdown().await;
    println!("Done.");
    Ok(())
}
