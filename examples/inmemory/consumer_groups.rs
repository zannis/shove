//! Consumer group with N workers sharing a single queue's load.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use shove::inmemory::{
    InMemoryBroker, InMemoryConsumerGroupConfig, InMemoryConsumerGroupRegistry, InMemoryPublisher,
};
use shove::{MessageHandler, MessageMetadata, Outcome, Publisher, Topic, TopologyBuilder};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Work {
    id: u32,
}

struct WorkTopic;
impl Topic for WorkTopic {
    type Message = Work;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("work").dlq().build())
    }
}

#[derive(Clone)]
struct Worker {
    count: Arc<AtomicUsize>,
}
impl MessageHandler<WorkTopic> for Worker {
    type Context = ();
    async fn handle(&self, msg: Work, _: MessageMetadata, _: &()) -> Outcome {
        // Simulate some I/O.
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.count.fetch_add(1, Ordering::Relaxed);
        println!("processed work id={}", msg.id);
        Outcome::Ack
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let broker = InMemoryBroker::new();
    let registry = Arc::new(Mutex::new(InMemoryConsumerGroupRegistry::new(
        broker.clone(),
    )));

    let count = Arc::new(AtomicUsize::new(0));
    {
        let count = count.clone();
        let factory = move || Worker {
            count: count.clone(),
        };
        let mut reg = registry.lock().await;
        reg.register::<WorkTopic, _>(
            InMemoryConsumerGroupConfig::new(3..=6).with_prefetch_count(4),
            factory,
        )
        .await
        .unwrap();
        reg.start_all();
    }

    let publisher = InMemoryPublisher::new(broker.clone());
    for i in 0..100u32 {
        publisher
            .publish::<WorkTopic>(&Work { id: i })
            .await
            .unwrap();
    }

    while count.load(Ordering::Relaxed) < 100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    registry.lock().await.shutdown_all().await;
    println!("processed {} messages", count.load(Ordering::Relaxed));
}
