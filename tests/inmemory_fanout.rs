//! Fan-out: two independent readers of one topic, each with its own retry/DLQ
//! chain (`TopologyBuilder::for_consumer_group`).
//!
//! Before this existed, the second reader of a shared topic had to declare a
//! *bare* topology — no `.dlq()`, no `.hold_queue()` — because both readers
//! would otherwise derive the same `{queue}-dlq` and `{queue}-hold-*` names and
//! drain each other's held and dead messages. A bare topology cannot use
//! `Outcome::Retry` or `Outcome::Reject` at all: both silently discard without
//! a DLQ. These tests pin the escape from that trade-off.
//!
//! The in-memory backend has no consumer-group notion, so both topics here
//! share the one queue. What is asserted is the part that is backend-agnostic
//! and was actually broken: the *failure-handling chain* is disjoint.

#![cfg(feature = "inmemory")]

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::inmemory::{InMemoryBroker, InMemoryConsumer};
use shove::markers::InMemory;
use shove::{
    ConsumerOptions, DeadMessageMetadata, JsonCodec, MessageHandler, MessageMetadata, Outcome,
    QueueTopology, Topic, TopologyBuilder,
};

const SHARED_QUEUE: &str = "prices-fanout-int";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Price {
    id: u64,
}

/// Reader 1 — the incumbent, declared exactly as it was before fan-out existed.
struct PricesBook;
impl Topic for PricesBook {
    type Message = Price;
    type Codec = JsonCodec;
    fn topology() -> &'static QueueTopology {
        static T: OnceLock<QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new(SHARED_QUEUE)
                .hold_queue(Duration::from_millis(20))
                .dlq()
                .build()
        })
    }
}

/// Reader 2 — same topic, its own chain. Full `Retry`/`Reject` semantics.
struct PricesLatest;
impl Topic for PricesLatest {
    type Message = Price;
    type Codec = JsonCodec;
    fn topology() -> &'static QueueTopology {
        static T: OnceLock<QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new(SHARED_QUEUE)
                .for_consumer_group("latest")
                .hold_queue(Duration::from_millis(20))
                .dlq()
                .build()
        })
    }
}

#[derive(Clone)]
struct AlwaysRetry;
impl MessageHandler<PricesLatest> for AlwaysRetry {
    type Context = ();
    async fn handle(&self, _: Price, _: MessageMetadata, _: &()) -> Outcome {
        Outcome::Retry
    }
}

#[derive(Clone)]
struct CountDead(Arc<AtomicUsize>);
impl MessageHandler<PricesLatest> for CountDead {
    type Context = ();
    async fn handle(&self, _: Price, _: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _: Price, _: DeadMessageMetadata, _: &()) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct CountDeadBook(Arc<AtomicUsize>);
impl MessageHandler<PricesBook> for CountDeadBook {
    type Context = ();
    async fn handle(&self, _: Price, _: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _: Price, _: DeadMessageMetadata, _: &()) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

async fn poll_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

#[test]
fn the_two_readers_share_the_topic_and_split_the_retry_chain() {
    let book = PricesBook::topology();
    let latest = PricesLatest::topology();

    assert_eq!(book.queue(), latest.queue());
    assert_eq!(book.dlq(), Some("prices-fanout-int-dlq"));
    assert_eq!(latest.dlq(), Some("prices-fanout-int-latest-dlq"));
    assert_eq!(book.hold_queues()[0].name(), "prices-fanout-int-hold-20ms");
    assert_eq!(
        latest.hold_queues()[0].name(),
        "prices-fanout-int-latest-hold-20ms"
    );
}

/// The message the second reader gives up on must land in *its* DLQ, and the
/// incumbent's DLQ must stay empty — otherwise the incumbent's dead-letter
/// drain starts handling failures that belong to another service.
///
/// The incumbent's drain runs *first and alone* so the check is a decision, not
/// a race: were both readers still deriving `{queue}-dlq`, it is the only
/// consumer of that DLQ and would necessarily take the message.
#[tokio::test]
async fn a_dead_message_lands_only_in_its_own_groups_dlq() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker.topology().declare::<PricesBook>().await.unwrap();
    broker.topology().declare::<PricesLatest>().await.unwrap();

    broker
        .publisher()
        .await
        .unwrap()
        .publish::<PricesLatest>(&Price { id: 7 })
        .await
        .unwrap();

    let own_dlq = Arc::new(AtomicUsize::new(0));
    let other_dlq = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();

    let main = {
        let consumer = InMemoryConsumer::new(client.clone());
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(shutdown.clone())
            .with_prefetch_count(1)
            .with_max_retries(1);
        tokio::spawn(async move { consumer.run::<PricesLatest, _>(AlwaysRetry, (), opts).await })
    };

    let other_drain = {
        let consumer = InMemoryConsumer::new(client.clone());
        let handler = CountDeadBook(other_dlq.clone());
        tokio::spawn(async move { consumer.run_dlq::<PricesBook, _>(handler, ()).await })
    };

    // One retry over a 20ms hold queue, then the DLQ write — 500ms is well past
    // it, so a message the incumbent's drain was going to steal is gone by now.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        other_dlq.load(Ordering::Relaxed),
        0,
        "the incumbent reader's DLQ drained a message belonging to the fan-out group"
    );

    let own_drain = {
        let consumer = InMemoryConsumer::new(client.clone());
        let handler = CountDead(own_dlq.clone());
        tokio::spawn(async move { consumer.run_dlq::<PricesLatest, _>(handler, ()).await })
    };

    let probe = own_dlq.clone();
    assert!(
        poll_until(
            move || probe.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5),
        )
        .await,
        "the retry-exhausted message never reached the fan-out group's own DLQ"
    );

    shutdown.cancel();
    client.shutdown();
    let _ = main.await;
    let _ = own_drain.await;
    let _ = other_drain.await;
}
