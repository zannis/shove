//! Basic in-memory publish/consume round-trip.

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

// [!region topic]
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
// [!endregion topic]

// [!region handler]
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
// [!endregion handler]

#[tokio::main]
async fn main() {
    // [!region main]
    tracing_subscriber::fmt::init();

    // [!region connect]
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect InMemory");
    // [!endregion connect]
    // [!region declare]
    broker
        .topology()
        .declare::<PingTopic>()
        .await
        .expect("declare");
    // [!endregion declare]

    let count = Arc::new(AtomicUsize::new(0));

    // [!region consume]
    let mut group = broker.consumer_group();
    let c = count.clone();
    group
        .register::<PingTopic, _>(
            ConsumerGroupConfig::new(
                InMemoryConsumerGroupConfig::new(1..=1).with_prefetch_count(4),
            ),
            move || PingHandler { count: c.clone() },
        )
        .await
        .expect("register");

    // [!region publish]
    let publisher = broker.publisher().await.expect("publisher");
    for i in 0..5 {
        publisher
            .publish::<PingTopic>(&Ping {
                id: i,
                note: format!("hello {i}"),
            })
            .await
            .expect("publish");
    }
    // [!endregion publish]

    // Stop when all five messages have been processed (or after 5 s).
    let stop = CancellationToken::new();
    let waiter_stop = stop.clone();
    let waiter_count = count.clone();
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while waiter_count.load(Ordering::Relaxed) < 5 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
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
    // [!endregion consume]

    std::process::exit(outcome.exit_code());
    // [!endregion main]
}
