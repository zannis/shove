//! In-memory consumer wrapped in the `Audited` decorator. The audit handler
//! prints each record to stdout; swap in `ShoveAuditHandler` to publish to a
//! dedicated audit topic.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use shove::inmemory::{
    InMemoryBroker, InMemoryConsumer, InMemoryPublisher, InMemoryTopologyDeclarer,
};
use shove::{
    AuditHandler, AuditRecord, Audited, Consumer, ConsumerOptions, MessageHandler, MessageMetadata,
    Outcome, Publisher, Topic, TopologyBuilder, declare_topic,
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

    let broker = InMemoryBroker::new();
    let declarer = InMemoryTopologyDeclarer::new(broker.clone());
    declare_topic::<EventTopic>(&declarer).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let audited = Audited::new(
        Inner {
            count: count.clone(),
        },
        StdoutAudit,
    );

    let shutdown = CancellationToken::new();
    let consumer = InMemoryConsumer::new(broker.clone());
    let shutdown_for_task = shutdown.clone();
    let handle = tokio::spawn(async move {
        let opts = ConsumerOptions::new(shutdown_for_task).with_prefetch_count(1);
        consumer.run::<EventTopic>(audited, opts).await
    });

    let publisher = InMemoryPublisher::new(broker.clone());
    for id in 0..3 {
        publisher
            .publish::<EventTopic>(&Event { id })
            .await
            .unwrap();
    }

    while count.load(Ordering::Relaxed) < 3 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown.cancel();
    let _ = handle.await;
}
