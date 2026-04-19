//! Basic in-memory publish/consume round-trip.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use shove::inmemory::{
    InMemoryBroker, InMemoryConsumer, InMemoryPublisher, InMemoryTopologyDeclarer,
};
use shove::{
    Consumer, ConsumerOptions, MessageHandler, MessageMetadata, Outcome, Publisher, Topic,
    TopologyBuilder, declare_topic,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ping {
    id: u32,
    note: String,
}

struct PingTopic;
impl Topic for PingTopic {
    type Message = Ping;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("ping").dlq().build())
    }
}

#[derive(Clone)]
struct PingHandler {
    count: Arc<AtomicUsize>,
}
impl MessageHandler<PingTopic> for PingHandler {
    type Context = ();
    async fn handle(&self, msg: Ping, _: MessageMetadata, _: &()) -> Outcome {
        println!("received #{}: {}", msg.id, msg.note);
        self.count.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<PingTopic>(&declarer).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let handler = PingHandler {
        count: count.clone(),
    };

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(broker.clone());
    let shutdown_for_task = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_for_task).with_prefetch_count(4);
        consumer.run::<PingTopic>(handler, opts).await
    });

    let publisher = InMemoryPublisher::new(broker.clone());
    for i in 0..5 {
        publisher
            .publish::<PingTopic>(&Ping {
                id: i,
                note: format!("hello {i}"),
            })
            .await
            .unwrap();
    }

    while count.load(Ordering::Relaxed) < 5 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown.cancel();
    let _ = consume_handle.await;
}
