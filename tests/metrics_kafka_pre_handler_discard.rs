#![cfg(all(feature = "kafka", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: Kafka's **concurrent** (non-FIFO) consumer must count a
//! message dropped *before the handler* — oversize, or undecodable — in
//! `shove_messages_discarded_total`, with the same DLQ-aware rules the
//! post-handler terminal path already gets.
//!
//! Before this change, the five pre-handler drop sites in
//! `KafkaConsumer::run`'s receive loop called `metrics::record_failed` and
//! retired the offset with no discard accounting at all: a bare topology that
//! drops an oversized or undecodable message loses it silently, with nothing
//! but a `WARN` line to show for it. That is the same silent-loss shape
//! `metrics_kafka_failall_no_dlq.rs` pins for the FIFO cascade path and
//! `metrics_kafka_batch_reject.rs` pins for the batch `Reject` arm — this is
//! the concurrent path's turn.
//!
//! # Shape
//!
//! Two scenarios, one Kafka container, one `#[test]`:
//!
//! 1. **No DLQ.** An oversized and an undecodable message are dropped before
//!    the handler; both `discarded_total` and `failed_total` must move once
//!    per reason. A trailing *valid* message is the barrier — see below for
//!    why a single partition makes it trustworthy.
//! 2. **DLQ declared and reachable.** The same two poison messages must be
//!    dead-lettered, `failed_total` still moves, but `discarded_total` stays
//!    at zero: the messages are readable off the DLQ, so nothing is lost.
//!
//! # Why the no-DLQ barrier needs a single partition
//!
//! There is no DLQ delivery to wait on in scenario 1, and
//! `Snapshotter::snapshot()` *drains* every counter it reads, so the metric
//! itself cannot be polled for progress either. The only observable signal
//! left is a message that reaches the handler — but the concurrent consumer
//! decodes messages inline in its receive loop (see the five drop sites in
//! `src/backends/kafka/consumer.rs`) in whatever order librdkafka happens to
//! deliver across partitions, so a trailing valid message on a *default*
//! multi-partition topic could arrive and get handled before the poison
//! messages sitting on other partitions are even fetched.
//!
//! `KafkaTopologyDeclarer::with_min_partitions` only ever *raises* a topic's
//! partition count (`declare`'s own doc), so the usual `broker.topology()`
//! path cannot pin scenario 1's topic to one partition — hence the raw
//! `rdkafka::admin::AdminClient` topic creation below, which runs before
//! anything is published, so `create_topic`'s "raise, never lower" behaviour
//! never comes into play.
//!
//! With exactly one partition, the receive loop's single sequential
//! `consumer.recv()` delivers in publish order, so the valid message reaching
//! the handler proves the two poison messages ahead of it were already
//! decoded and dropped. It does not by itself prove their *discard* settled
//! (that rides the offset into the tracker and is confirmed by a later
//! `CommitMode::Sync` commit — see `signal_completion`'s doc) — but shutting
//! the consumer down and awaiting its task handle does: the receive loop's
//! shutdown arm drains and synchronously commits every pending offset,
//! discards included, before the task returns.
//!
//! Scenario 2 does not need any of this: the DLQ delivery itself is the
//! barrier, however the partitions happen to be laid out.
//!
//! # Oversize mechanics
//!
//! The broker's default `message.max.bytes` (~1 MiB) makes a genuinely
//! oversized publish impractical, so the *consumer* is configured with a tiny
//! `ConsumerOptions::with_max_message_size` instead — same trick
//! `kafka_batch_integration.rs`'s `oversized_message_is_dropped_from_the_batch`
//! uses.
//!
//! Scenario 2 reads the DLQ back with a raw `StreamConsumer` rather than a
//! typed `run_dlq`: the undecodable message fails to decode as `Order` in the
//! DLQ drain exactly as it did on the main path, so a typed drain would ack it
//! without ever calling `handle_dead` — see `drain_raw`'s doc, and
//! `kafka_batch_integration.rs`'s own `drain_raw` for the same reasoning on
//! the batch path.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot. Hence its own integration binary, a single `#[test]`, and
//! exactly one snapshot taken after every consumer has stopped.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;

const TIMEOUT: Duration = Duration::from_secs(60);

/// Comfortably above a bare `Order`, far below the padded oversize payload.
const MAX_MESSAGE_SIZE: usize = 512;

const NO_DLQ_TOPIC: &str = "kafka-metrics-prehandler-concurrent-no-dlq";
const DLQ_TOPIC: &str = "kafka-metrics-prehandler-concurrent-dlq";

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
    _container: testcontainers::ContainerAsync<KafkaContainer>,
    brokers: String,
}

impl TestBroker {
    async fn start() -> Self {
        let container = KafkaContainer::default()
            .start()
            .await
            .expect("failed to start Kafka container");
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("failed to get Kafka port");
        let brokers = format!("127.0.0.1:{port}");
        Self {
            _container: container,
            brokers,
        }
    }

    async fn client(&self) -> KafkaClient {
        KafkaClient::connect_with_retry(&KafkaConfig::new(&self.brokers), 10)
            .await
            .expect("failed to connect to Kafka")
    }
}

/// Creates `topic` with exactly one partition, out-of-band, before anything
/// touches shove's own topology declarer.
///
/// `KafkaTopologyDeclarer::declare` always requests
/// `DEFAULT_PARTITIONS` (8) for a non-sequenced topic and only ever *raises* a
/// partition count on an existing topic (`ensure_partitions`), never lowers
/// it — so declaring through shove after this would undo the pin. Scenario 1
/// therefore never calls `broker.topology().declare`, and consumers do not
/// need it either: `run`/`run_dlq` just `subscribe`.
async fn create_single_partition_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create admin client");
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .expect("create_topics RPC failed")
        .into_iter()
        .for_each(|r| {
            r.expect("topic creation failed");
        });
}

/// Publish a payload straight through rdkafka, bypassing shove's codec — the
/// only way to land a body that `T::Codec` cannot decode.
async fn publish_raw(brokers: &str, topic: &str, payload: &[u8]) {
    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create raw producer");
    producer
        .send(
            FutureRecord::<(), [u8]>::to(topic).payload(payload),
            Duration::from_secs(10),
        )
        .await
        .expect("raw publish should succeed");
}

/// Reads up to `expected` raw payloads off `topic`, giving up after `TIMEOUT`.
///
/// A typed `run_dlq` cannot be the barrier here: the undecodable message
/// fails to decode as `Order` in the DLQ drain exactly as it did on the main
/// path, so the drain acks it without ever calling `handle_dead` (see
/// `kafka_batch_integration.rs`'s `drain_raw` for the same reasoning on the
/// batch path). Reading the bytes back directly is the only way to observe
/// both poison messages landing in the DLQ.
async fn drain_raw(brokers: &str, topic: &str, expected: usize) -> Vec<Vec<u8>> {
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::Message;

    let consumer: StreamConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("{topic}-raw-drain"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("failed to create raw consumer");
    consumer.subscribe(&[topic]).expect("subscribe should work");

    let mut out = Vec::new();
    let _ = tokio::time::timeout(TIMEOUT, async {
        while out.len() < expected {
            if let Ok(msg) = consumer.recv().await {
                out.push(msg.payload().unwrap_or_default().to_vec());
            }
        }
    })
    .await;
    out
}

// ---------------------------------------------------------------------------
// Topic and handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: u32,
    /// Padded well past `MAX_MESSAGE_SIZE` to trip the oversize gate.
    padding: String,
}

shove::define_topic!(
    NoDlqTopic,
    Order,
    TopologyBuilder::new(NO_DLQ_TOPIC).build()
);

shove::define_topic!(
    WithDlqTopic,
    Order,
    TopologyBuilder::new(DLQ_TOPIC).dlq().build()
);

/// Acks everything; signals whenever the marker id is handled.
#[derive(Clone)]
struct BarrierHandler {
    marker_id: u32,
    handled_marker: WaitableCounter,
    handled_total: Arc<AtomicU32>,
}

impl BarrierHandler {
    fn new(marker_id: u32) -> Self {
        Self {
            marker_id,
            handled_marker: WaitableCounter::new(),
            handled_total: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl MessageHandler<NoDlqTopic> for BarrierHandler {
    type Context = ();
    async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.handled_total.fetch_add(1, Ordering::SeqCst);
        if msg.id == self.marker_id {
            self.handled_marker.increment();
        }
        Outcome::Ack
    }
}

impl MessageHandler<WithDlqTopic> for BarrierHandler {
    type Context = ();
    async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.handled_total.fetch_add(1, Ordering::SeqCst);
        if msg.id == self.marker_id {
            self.handled_marker.increment();
        }
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

type Snapshot = std::collections::HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

fn counter_total(snapshot: &Snapshot, name: &str, topic: &str, reason: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .filter(|(k, _)| {
            let mut has_topic = false;
            let mut has_reason = false;
            for label in k.key().labels() {
                match label.key() {
                    "topic" => has_topic = label.value() == topic,
                    "reason" => has_reason = label.value() == reason,
                    _ => {}
                }
            }
            has_topic && has_reason
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("{name} is not a counter: {other:?}"),
        })
        .sum()
}

fn failed_total(snapshot: &Snapshot, topic: &str, reason: &str) -> u64 {
    counter_total(snapshot, "shove_messages_failed_total", topic, reason)
}

fn discarded_total(snapshot: &Snapshot, topic: &str, reason: &str) -> u64 {
    counter_total(snapshot, "shove_messages_discarded_total", topic, reason)
}

/// Every `shove_messages_consumed_total` series for `topic`, as
/// `(outcome, count)` pairs — so a wrong reading names the outcome label
/// rather than leaving the assertion to a bare number mismatch.
fn consumed_series(snapshot: &Snapshot, topic: &str) -> Vec<(String, u64)> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_consumed_total")
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "topic" && l.value() == topic)
        })
        .map(|(k, (_, _, value))| {
            let outcome = k
                .key()
                .labels()
                .find(|l| l.key() == "outcome")
                .map_or_else(|| "<unlabelled>".to_string(), |l| l.value().to_string());
            match value {
                DebugValue::Counter(n) => (outcome, *n),
                other => panic!("shove_messages_consumed_total is not a counter: {other:?}"),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_pre_handler_drops_count_as_discarded_only_with_no_dlq() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;

    // -- Scenario 1: no DLQ --------------------------------------------------
    {
        create_single_partition_topic(&tb.brokers, NO_DLQ_TOPIC).await;
        let client = tb.client().await;
        let broker = Broker::<Kafka>::from_client(client.clone());
        let publisher = broker.publisher().await.expect("publisher");

        let oversized = Order {
            id: 0,
            padding: "x".repeat(4096),
        };
        publisher
            .publish::<NoDlqTopic>(&oversized)
            .await
            .expect("publish oversized");
        publish_raw(&tb.brokers, NO_DLQ_TOPIC, b"{not valid json").await;
        let marker_id = 42;
        publisher
            .publish::<NoDlqTopic>(&Order {
                id: marker_id,
                padding: String::new(),
            })
            .await
            .expect("publish marker");

        let handler = BarrierHandler::new(marker_id);
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(client);
        let handle = tokio::spawn(async move {
            consumer
                .run::<NoDlqTopic, _>(
                    h,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_max_message_size(MAX_MESSAGE_SIZE),
                )
                .await
        });

        assert!(
            handler.handled_marker.wait_for(1, TIMEOUT).await,
            "the marker message must reach the handler after the two poison \
             messages ahead of it on the single partition"
        );
        shutdown.cancel();
        handle.await.expect("consumer task panicked").ok();

        assert_eq!(
            handler.handled_total.load(Ordering::SeqCst),
            1,
            "only the marker message should ever reach the handler"
        );
    }

    // -- Scenario 2: DLQ declared and reachable ------------------------------
    {
        let client = tb.client().await;
        let broker = Broker::<Kafka>::from_client(client.clone());
        broker
            .topology()
            .declare::<WithDlqTopic>()
            .await
            .expect("declare");
        let publisher = broker.publisher().await.expect("publisher");

        let oversized = Order {
            id: 0,
            padding: "x".repeat(4096),
        };
        publisher
            .publish::<WithDlqTopic>(&oversized)
            .await
            .expect("publish oversized");
        const POISON: &[u8] = b"{not valid json";
        publish_raw(&tb.brokers, DLQ_TOPIC, POISON).await;

        let handler = BarrierHandler::new(u32::MAX);
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let main_consumer = KafkaConsumer::new(client.clone());
        let main_handle = tokio::spawn(async move {
            main_consumer
                .run::<WithDlqTopic, _>(
                    h,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_max_message_size(MAX_MESSAGE_SIZE),
                )
                .await
        });

        // A typed `run_dlq` cannot be the barrier — see `drain_raw`'s doc.
        let mut dead = drain_raw(&tb.brokers, &format!("{DLQ_TOPIC}-dlq"), 2).await;
        dead.sort();
        let mut expected = vec![serde_json::to_vec(&oversized).unwrap(), POISON.to_vec()];
        expected.sort();
        assert_eq!(
            dead, expected,
            "both poison payloads should be preserved in the DLQ"
        );

        shutdown.cancel();
        broker.close().await;
        main_handle.await.expect("main consumer task panicked").ok();
    }

    // Single, draining snapshot, taken only once every consumer above has
    // stopped so nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    // -- Scenario 1 assertions -----------------------------------------------
    assert_eq!(
        failed_total(&snapshot, NO_DLQ_TOPIC, "oversize"),
        1,
        "the oversized message must count exactly one failure"
    );
    assert_eq!(
        failed_total(&snapshot, NO_DLQ_TOPIC, "deserialize"),
        1,
        "the undecodable message must count exactly one failure"
    );
    assert_eq!(
        discarded_total(&snapshot, NO_DLQ_TOPIC, "oversize"),
        1,
        "with no DLQ declared, the oversized message is dropped on the floor \
         once its offset commits — that is exactly what \
         `shove_messages_discarded_total` promises to report"
    );
    assert_eq!(
        discarded_total(&snapshot, NO_DLQ_TOPIC, "deserialize"),
        1,
        "with no DLQ declared, the undecodable message is dropped on the \
         floor once its offset commits"
    );
    assert_eq!(
        consumed_series(&snapshot, NO_DLQ_TOPIC),
        vec![("ack".to_string(), 1)],
        "pre-handler drops must never touch messages_consumed_total — only \
         the marker message reaches the handler and is acked"
    );

    // -- Scenario 2 assertions -----------------------------------------------
    assert_eq!(
        failed_total(&snapshot, DLQ_TOPIC, "oversize"),
        1,
        "a DLQ does not make the oversize rejection stop being a failure"
    );
    assert_eq!(
        failed_total(&snapshot, DLQ_TOPIC, "deserialize"),
        1,
        "a DLQ does not make the deserialize rejection stop being a failure"
    );
    assert_eq!(
        discarded_total(&snapshot, DLQ_TOPIC, "oversize"),
        0,
        "the oversized message was observed arriving in the DLQ above, so \
         nothing was discarded; counting it here would fire a false \
         data-loss alert"
    );
    assert_eq!(
        discarded_total(&snapshot, DLQ_TOPIC, "deserialize"),
        0,
        "the undecodable message was observed arriving in the DLQ above, so \
         nothing was discarded"
    );
}
