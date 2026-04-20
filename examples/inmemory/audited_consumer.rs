//! In-memory consumer wrapped in the `Audited` decorator. The audit handler
//! prints each record to stdout; swap in `ShoveAuditHandler` to publish to a
//! dedicated audit topic.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use shove::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
use shove::{
    AuditHandler, AuditRecord, Broker, ConsumerGroupConfig, InMemory, MessageHandler,
    MessageHandlerExt, MessageMetadata, Outcome, Topic, TopologyBuilder,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    id: u32,
}

struct EventTopic;
impl Topic for EventTopic {
    type Message = Event;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("audited-demo").dlq().build())
    }
}

#[derive(Clone)]
struct Inner {
    count: Arc<AtomicUsize>,
}
impl MessageHandler<EventTopic> for Inner {
    type Context = ();
    async fn handle(&self, msg: Event, _: MessageMetadata, _: &()) -> Outcome {
        self.count.fetch_add(1, Ordering::Relaxed);
        println!("handled event {}", msg.id);
        Outcome::Ack
    }
}

#[derive(Clone, Default)]
struct StdoutAudit;
impl AuditHandler<EventTopic> for StdoutAudit {
    async fn audit(&self, record: &AuditRecord<Event>) -> shove::error::Result<()> {
        println!(
            "audit trace_id={} outcome={:?} duration_ms={}",
            record.trace_id, record.outcome, record.duration_ms
        );
        Ok(())
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
        .declare::<EventTopic>()
        .await
        .expect("declare");

    let count = Arc::new(AtomicUsize::new(0));

    let mut group = broker.consumer_group();
    let c = count.clone();
    group
        .register::<EventTopic, _>(
            ConsumerGroupConfig::new(
                InMemoryConsumerGroupConfig::new(1..=1).with_prefetch_count(1),
            ),
            move || Inner { count: c.clone() }.audited(StdoutAudit),
        )
        .await
        .expect("register");

    let publisher = broker.publisher().await.expect("publisher");
    for id in 0..3 {
        publisher
            .publish::<EventTopic>(&Event { id })
            .await
            .expect("publish");
    }

    // Stop when all three events have been processed (or after a deadline).
    let stop = CancellationToken::new();
    let waiter_stop = stop.clone();
    let waiter_count = count.clone();
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while waiter_count.load(Ordering::Relaxed) < 3 && tokio::time::Instant::now() < deadline {
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

    std::process::exit(outcome.exit_code());
}
