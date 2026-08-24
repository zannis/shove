#![cfg(all(feature = "inmemory", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: the in-memory **DLQ-drain** loop records
//! `shove_message_size_bytes`.
//!
//! # What is being asserted
//!
//! `record_message_size` is placed on each backend's main consume path. The
//! dedicated DLQ-drain loops are a second delivery path that the main loop's
//! call site does not cover: `run_dlq_impl` in
//! `src/backends/inmemory/consumer.rs` pops from the DLQ and applies its own
//! `validate_payload_message_size` gate without going through
//! `prepare_message`, so it applied the size limit while never sampling the
//! size. The same held for the Kafka, NATS and RabbitMQ DLQ loops; each has
//! its own `metrics_*_dlq_message_size` binary asserting the same rule.
//!
//! Three things are pinned here, and the third is the reason this is worth a
//! test rather than a code comment:
//!
//! 1. **A sample exists at all** for a message that only ever arrived through
//!    the DLQ drain. Deleting the call in `run_dlq_impl` reddens this.
//! 2. **Its value is the exact encoded payload length** — computed through the
//!    topic's own codec rather than hard-coded, since the histogram is only
//!    useful for sizing `max_message_size` if it reports the bytes that limit
//!    is compared against.
//! 3. **Its labels are the SOURCE topic and no consumer group**, never the DLQ
//!    queue name. Redis already drains its DLQ through `run_stream_loop`,
//!    which labels every metric `topology.queue()` whichever stream it reads;
//!    if this path used the DLQ name instead, `topic` would mean two different
//!    things depending on the backend and a per-topic size profile would stop
//!    summing across the main and DLQ paths. The main consumer here runs under
//!    an explicit `consumer_group`, so its own sample lands on a separate
//!    series and cannot be mistaken for the DLQ one.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot and whose `snapshot()` drains what it reads. Hence its own
//! integration binary, a single `#[test]`, and exactly one snapshot taken
//! after both consumers have stopped — progress is waited on through handler
//! counters, never by peeking at the metrics.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::codec::Codec;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::inmemory::{InMemoryBroker, InMemoryConsumer};
use shove::markers::InMemory;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::{TopologyBuilder, define_topic};

/// Source topic — and therefore the expected `topic` label on *both* the main
/// and the DLQ-drain samples.
const QUEUE: &str = "inmemory-dlq-size";
/// The DLQ this topic dead-letters into. Asserted to be absent from the
/// `topic` label of every `shove_message_size_bytes` series.
const DLQ: &str = "inmemory-dlq-size_dlq";
/// The main consumer's group label, which keeps its sample on its own series.
const MAIN_GROUP: &str = "inmemory-dlq-size-main";
/// What `run_dlq` reports: it builds default options, so no group at all,
/// which `metrics::group_label` renders as `default`.
const DLQ_GROUP: &str = "default";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    /// Present only to give the payload a length distinctive enough that a
    /// sample from some other series could not coincidentally match it.
    reference: String,
}

define_topic!(
    Orders,
    Order,
    TopologyBuilder::new(QUEUE).dlq_named(DLQ).build()
);

/// Rejects, which is terminal — the message goes straight to the DLQ with no
/// retry, so the main path contributes exactly one size sample.
#[derive(Clone)]
struct RejectHandler;

impl MessageHandler<Orders> for RejectHandler {
    type Context = Arc<AtomicU32>;
    async fn handle(&self, _msg: Order, _meta: MessageMetadata, ctx: &Arc<AtomicU32>) -> Outcome {
        ctx.fetch_add(1, Ordering::SeqCst);
        Outcome::Reject
    }
}

/// Counts `handle_dead` calls so the test can wait on the drain rather than
/// sleeping against it.
#[derive(Clone)]
struct DlqHandler(Arc<AtomicU32>);

impl MessageHandler<Orders> for DlqHandler {
    type Context = ();
    async fn handle(&self, _msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: Order, _meta: DeadMessageMetadata, _: &()) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

type Snapshot = HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

/// Every `shove_message_size_bytes` sample recorded for `topic` under `group`.
fn size_samples(snapshot: &Snapshot, topic: &str, group: &str) -> Vec<f64> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_message_size_bytes")
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "topic" && l.value() == topic)
        })
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "consumer_group" && l.value() == group)
        })
        .flat_map(|(_, (_, _, value))| match value {
            DebugValue::Histogram(samples) => {
                samples.iter().copied().map(f64::from).collect::<Vec<f64>>()
            }
            other => panic!("shove_message_size_bytes is not a histogram: {other:?}"),
        })
        .collect()
}

/// Every distinct `topic` label value carrying a `shove_message_size_bytes`
/// series.
fn size_topics(snapshot: &Snapshot) -> Vec<String> {
    let mut topics: Vec<String> = snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_message_size_bytes")
        .filter_map(|(k, _)| {
            k.key()
                .labels()
                .find(|l| l.key() == "topic")
                .map(|l| l.value().to_string())
        })
        .collect();
    topics.sort();
    topics.dedup();
    topics
}

/// Every `shove_message_size_bytes` series, labels and all — so a failing
/// assertion says what *was* recorded, not merely that what it wanted is
/// missing.
fn size_series(snapshot: &Snapshot) -> Vec<String> {
    let mut series: Vec<String> = snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_message_size_bytes")
        .map(|(k, (_, _, value))| {
            let labels: Vec<String> = k
                .key()
                .labels()
                .map(|l| format!("{}={}", l.key(), l.value()))
                .collect();
            format!("{{{}}} => {value:?}", labels.join(","))
        })
        .collect();
    series.sort();
    series
}

/// The byte length the consumer should report for `msg` — the topic's own
/// encoding, so the assertion survives a codec change.
fn encoded_len<T: Topic>(msg: &T::Message) -> f64 {
    let bytes = <T::Codec as Codec<T::Message>>::encode(msg).expect("encode");
    bytes.len() as f64
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

#[tokio::test(flavor = "multi_thread")]
async fn dlq_drain_records_message_size_under_the_source_topic() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<Orders>()
        .await
        .expect("declare topology");

    let msg = Order {
        reference: "order-reference-0001".into(),
    };
    let expected = encoded_len::<Orders>(&msg);

    let publisher = broker.publisher().await.expect("publisher");
    publisher.publish::<Orders>(&msg).await.expect("publish");

    let handled = Arc::new(AtomicU32::new(0));
    let drained = Arc::new(AtomicU32::new(0));

    let shutdown = CancellationToken::new();
    let main_consumer = InMemoryConsumer::new(client.clone());
    let main_shutdown = shutdown.clone();
    let handler_ctx = handled.clone();
    let main_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<InMemory>::new()
            .with_shutdown(main_shutdown)
            .with_consumer_group(MAIN_GROUP);
        main_consumer
            .run::<Orders, _>(RejectHandler, handler_ctx, opts)
            .await
    });

    let dlq_consumer = InMemoryConsumer::new(client.clone());
    let dlq_handler = DlqHandler(drained.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<Orders, _>(dlq_handler, ()).await });

    let drained_probe = drained.clone();
    assert!(
        poll_until(
            move || drained_probe.load(Ordering::SeqCst) >= 1,
            Duration::from_secs(10),
        )
        .await,
        "timed out waiting for the DLQ drain: handled={} drained={}",
        handled.load(Ordering::SeqCst),
        drained.load(Ordering::SeqCst),
    );

    shutdown.cancel();
    client.shutdown();
    let _ = main_handle.await;
    let _ = dlq_handle.await;

    // Single, draining snapshot — taken only once both consumers have stopped,
    // so nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();
    let observed = size_series(&snapshot);

    assert_eq!(
        handled.load(Ordering::SeqCst),
        1,
        "the main consumer must have handled (and rejected) exactly one message"
    );
    assert_eq!(
        drained.load(Ordering::SeqCst),
        1,
        "the DLQ consumer must have drained exactly one message"
    );

    assert_eq!(
        size_samples(&snapshot, QUEUE, DLQ_GROUP),
        vec![expected],
        "the in-memory DLQ drain must record one shove_message_size_bytes \
         sample, carrying the encoded payload length, labelled with the source \
         topic and no consumer group; observed series: {observed:?}"
    );
    assert_eq!(
        size_samples(&snapshot, QUEUE, MAIN_GROUP),
        vec![expected],
        "the main loop's own sample must still be recorded, on its own \
         consumer_group series — otherwise the assertion above could be \
         satisfied by the main loop alone; observed series: {observed:?}"
    );
    assert_eq!(
        size_topics(&snapshot),
        vec![QUEUE.to_string()],
        "no shove_message_size_bytes series may be labelled with the DLQ queue \
         name {DLQ}: the drain reports the source topic, so main-path and \
         DLQ-path sizes stay summable under one label; observed series: \
         {observed:?}"
    );
}
