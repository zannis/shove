//! Consumer group with N workers sharing a single queue's load.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use shove::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
use shove::{
    Broker, ConsumerGroupConfig, InMemory, MessageHandler, MessageMetadata, Outcome, Topic,
    TopologyBuilder,
};

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

    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect InMemory");
    broker
        .topology()
        .declare::<WorkTopic>()
        .await
        .expect("declare");

    let count = Arc::new(AtomicUsize::new(0));

    let mut group = broker.consumer_group();
    let c = count.clone();
    group
        .register::<WorkTopic, _>(
            ConsumerGroupConfig::new(
                InMemoryConsumerGroupConfig::new(3..=6).with_prefetch_count(4),
            ),
            move || Worker { count: c.clone() },
        )
        .await
        .expect("register");

    let publisher = broker.publisher().await.expect("publisher");
    for i in 0..100u32 {
        publisher
            .publish::<WorkTopic>(&Work { id: i })
            .await
            .expect("publish");
    }

    // Stop when all 100 messages have been processed, or after a deadline.
    let stop = CancellationToken::new();
    let waiter_stop = stop.clone();
    let waiter_count = count.clone();
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while waiter_count.load(Ordering::Relaxed) < 100 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        waiter_stop.cancel();
    });

    let signal_stop = stop.clone();
    let outcome = group
        .run_until_timeout(
            async move { signal_stop.cancelled().await },
            Duration::from_secs(5),
        )
        .await;

    println!("processed {} messages", count.load(Ordering::Relaxed));
    std::process::exit(outcome.exit_code());
}
