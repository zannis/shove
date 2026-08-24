#![cfg(all(feature = "nats", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: the NATS **DLQ-drain** loop records
//! `shove_message_size_bytes`.
//!
//! # What is being asserted
//!
//! `record_message_size` is placed on each backend's main consume path. The
//! dedicated DLQ-drain loops are a second delivery path that call site does
//! not cover: `NatsConsumer::run_dlq` in `src/backends/nats/consumer.rs` pulls
//! from the DLQ stream and applies its own `DEFAULT_MAX_MESSAGE_SIZE` gate
//! without touching the main loop, so it applied a size limit while never
//! sampling the size. The same held for the Kafka, RabbitMQ and in-memory DLQ
//! loops; each has its own `metrics_*_dlq_message_size` binary asserting the
//! same rule.
//!
//! Three things are pinned here, and the third is the reason this is worth a
//! test rather than a code comment:
//!
//! 1. **A sample exists at all** for a message that only ever arrived through
//!    the DLQ drain. Deleting the call in `run_dlq` reddens this.
//! 2. **Its value is the exact encoded payload length** — computed through the
//!    topic's own codec rather than hard-coded, since the histogram is only
//!    useful for sizing `max_message_size` if it reports the bytes that limit
//!    is compared against.
//! 3. **Its labels are the SOURCE topic and no consumer group**, never the DLQ
//!    stream name. Redis already drains its DLQ through `run_stream_loop`,
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
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::codec::Codec;
use shove::consumer::ConsumerOptions;
use shove::define_topic;
use shove::handler::MessageHandler;
use shove::markers::Nats;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::nats::{NatsClient, NatsConfig, NatsConsumer};
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::TopologyBuilder;

/// Source topic — and therefore the expected `topic` label on *both* the main
/// and the DLQ-drain samples.
const QUEUE: &str = "nats-dlq-size";
/// The DLQ stream this topic dead-letters into. Asserted to be absent from the
/// `topic` label of every `shove_message_size_bytes` series.
const DLQ: &str = "nats-dlq-size-dead";
/// The main consumer's group label, which keeps its sample on its own series.
const MAIN_GROUP: &str = "nats-dlq-size-main";
/// What `run_dlq` reports: it takes no `ConsumerOptions`, so no group at all,
/// which `metrics::group_label` renders as `default`.
const DLQ_GROUP: &str = "default";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A counter a test can await on rather than sleeping against.
#[derive(Clone)]
struct WaitableCounter {
    count: Arc<AtomicU32>,
    signal: Arc<Notify>,
}

impl WaitableCounter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(Notify::new()),
        }
    }

    fn increment(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.signal.notify_waiters();
    }

    fn get(&self) -> u32 {
        self.count.load(Ordering::SeqCst)
    }

    async fn wait_for(&self, target: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.get() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => return self.get() >= target,
            }
        }
    }
}

struct TestBroker {
    _container: testcontainers::ContainerAsync<NatsContainer>,
    client: NatsClient,
}

impl TestBroker {
    async fn start() -> Self {
        let cmd = NatsServerCmd::default().with_jetstream();
        let container = NatsContainer::default()
            .with_cmd(&cmd)
            .start()
            .await
            .expect("failed to start NATS container");
        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(4222)
            .await
            .expect("failed to get NATS port");
        let nats_url = format!("nats://{host}:{port}");

        let client = NatsClient::connect_with_retry(&NatsConfig::new(&nats_url), 10)
            .await
            .expect("failed to connect to NATS");

        Self {
            _container: container,
            client,
        }
    }

    fn broker(&self) -> Broker<Nats> {
        Broker::<Nats>::from_client(self.client.clone())
    }

    fn client(&self) -> NatsClient {
        self.client.clone()
    }
}

// ---------------------------------------------------------------------------
// Topic and handlers
// ---------------------------------------------------------------------------

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
struct RejectHandler(WaitableCounter);

impl MessageHandler<Orders> for RejectHandler {
    type Context = ();
    async fn handle(&self, _msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.0.increment();
        Outcome::Reject
    }
}

/// Counts `handle_dead` calls so the test can wait on the drain rather than
/// sleeping against it.
#[derive(Clone)]
struct DlqHandler(WaitableCounter);

impl MessageHandler<Orders> for DlqHandler {
    type Context = ();
    async fn handle(&self, _msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: Order, _meta: DeadMessageMetadata, _: &()) {
        self.0.increment();
    }
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dlq_drain_records_message_size_under_the_source_topic() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
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

    let handled = WaitableCounter::new();
    let drained = WaitableCounter::new();

    let shutdown = CancellationToken::new();
    let main_consumer = NatsConsumer::new(client.clone());
    let main_shutdown = shutdown.clone();
    let handler = RejectHandler(handled.clone());
    let main_handle = tokio::spawn(async move {
        main_consumer
            .run::<Orders, _>(
                handler,
                (),
                ConsumerOptions::<Nats>::new()
                    .with_shutdown(main_shutdown)
                    .with_prefetch_count(1)
                    .with_consumer_group(MAIN_GROUP),
            )
            .await
    });

    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handler = DlqHandler(drained.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<Orders, _>(dlq_handler, ()).await });

    assert!(
        drained.wait_for(1, Duration::from_secs(30)).await,
        "timed out waiting for the DLQ drain: handled={} drained={}",
        handled.get(),
        drained.get(),
    );

    shutdown.cancel();
    // `run_dlq` has no shutdown token of its own — it stops when the client's
    // token is cancelled, which `close()` does. Without this the awaits below
    // hang until the job times out.
    broker.close().await;
    main_handle.await.expect("main consumer task panicked").ok();
    dlq_handle.await.expect("DLQ consumer task panicked").ok();

    // Single, draining snapshot — taken only once both consumers have stopped,
    // so nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();
    let observed = size_series(&snapshot);

    assert_eq!(
        handled.get(),
        1,
        "the main consumer must have handled (and rejected) exactly one message"
    );
    assert_eq!(
        drained.get(),
        1,
        "the DLQ consumer must have drained exactly one message"
    );

    assert_eq!(
        size_samples(&snapshot, QUEUE, DLQ_GROUP),
        vec![expected],
        "the NATS DLQ drain must record one shove_message_size_bytes sample, \
         carrying the encoded payload length, labelled with the source topic \
         and no consumer group; observed series: {observed:?}"
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
        "no shove_message_size_bytes series may be labelled with the DLQ stream \
         name {DLQ}: the drain reports the source topic, so main-path and \
         DLQ-path sizes stay summable under one label; observed series: \
         {observed:?}"
    );
}
