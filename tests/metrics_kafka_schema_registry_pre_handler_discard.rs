#![cfg(all(
    feature = "kafka",
    feature = "kafka-schema-registry",
    feature = "metrics"
))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: with a schema registry configured, a message rejected by
//! the **registry decode stage** — an unframed payload (`schema_frame_invalid`
//! → reason `schema_frame`) or a subject the gate refuses
//! (`schema_validation_failed` → reason `schema_validation`) — must count in
//! `shove_messages_discarded_total` under exactly the same DLQ-aware rules as
//! the plain-codec pre-handler drops, on **all three** group-consume paths.
//!
//! The sibling fixtures (`metrics_kafka_pre_handler_discard.rs`,
//! `metrics_kafka_fifo_pre_handler_discard.rs`,
//! `metrics_kafka_batch_pre_handler_discard.rs`) cover the oversize and
//! plain-`Deserialize` branches only — none of them configures a registry, so
//! the `RegistryDecode::Dlq` accounting branch in each consumer
//! (`FailReason::for_schema_reason` pairing included) had no behavioural
//! coverage: a regression that dropped or mis-tagged accounting on that branch
//! alone would fail no test. This binary closes that gap.
//!
//! # Shape
//!
//! Four scenarios, one Kafka container, one `#[test]`:
//!
//! 1. **Concurrent, no DLQ.** An unframed and a rejected-subject message are
//!    dropped before the handler; `failed_total` and `discarded_total` must
//!    move once per schema reason. A trailing valid *framed* message is the
//!    barrier (single partition — see below).
//! 2. **FIFO, no DLQ.** Same two poisons, same assertions, on the sequenced
//!    consumer (`SequenceFailure::Skip`, so `poison_key` at the drop sites is
//!    inert and the barrier's key cannot be swept up).
//! 3. **Batch, no DLQ.** Same two poisons plus the marker in one flush window
//!    (`max_batch_size` = messages published), exercising
//!    `BatchDecode::Dlq { fail: FailReason::for_schema_reason(..) }`.
//! 4. **Concurrent, DLQ declared and reachable.** The same two poisons must be
//!    dead-lettered with their payloads preserved; `failed_total` still moves,
//!    `discarded_total` stays at zero.
//!
//! # Why no real registry is needed
//!
//! The frame parse runs before any registry round trip, so the unframed
//! poison never touches the registry at all. The rejected-subject poison and
//! the marker resolve their schema ids against a tiny axum mock (the same
//! shape `kafka_schema_registry.rs` uses): `ACCEPTED_SCHEMA_ID` resolves to
//! the topic's default accepted subject (`{queue}-value`), any other id to a
//! subject the default `SchemaEnforcement::Enforce` gate refuses.
//!
//! # Barriers
//!
//! Scenario 1 pins its topic to a single partition (raw `AdminClient`
//! creation, before anything is published) so the framed marker reaching the
//! handler proves the two poisons ahead of it were decoded and dropped —
//! identical reasoning to `metrics_kafka_pre_handler_discard.rs`, which also
//! documents why the shutdown drain then settles the pending discards.
//! Scenario 2's FIFO loop commits each drop synchronously before `recv()`-ing
//! the next message, so the marker reaching the handler proves both discards
//! settled. Scenario 3's flush containing the marker is its barrier, and
//! scenario 4's is the DLQ delivery itself.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot. Hence its own integration binary, a single `#[test]`, and
//! exactly one snapshot taken after every consumer has stopped.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{Json, Router, extract::Path, extract::State, routing::get};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use serde::{Deserialize, Serialize};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::SequencedTopic as _;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::kafka::{BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::schema_registry::SchemaRegistry;
use shove::topology::{SequenceFailure, TopologyBuilder};

const TIMEOUT: Duration = Duration::from_secs(60);

/// Resolves to the scenario topic's accepted subject on the mock registry.
const ACCEPTED_SCHEMA_ID: u32 = 1;
/// Resolves to a subject the `Enforce` gate refuses.
const REJECTED_SCHEMA_ID: u32 = 99;

const CONCURRENT_TOPIC: &str = "kafka-metrics-schema-prehandler-concurrent";
const FIFO_TOPIC: &str = "kafka-metrics-schema-prehandler-fifo";
const BATCH_TOPIC: &str = "kafka-metrics-schema-prehandler-batch";
const DLQ_TOPIC: &str = "kafka-metrics-schema-prehandler-dlq";

// ---------------------------------------------------------------------------
// Mock schema registry
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockState {
    /// The subject `ACCEPTED_SCHEMA_ID` resolves to — each scenario passes its
    /// topic's default accepted subject (`{queue}-value`).
    accepted_subject: &'static str,
}

async fn versions(State(s): State<MockState>, Path(id): Path<u32>) -> Json<serde_json::Value> {
    let subject = if id == ACCEPTED_SCHEMA_ID {
        s.accepted_subject
    } else {
        "subject-nobody-accepts"
    };
    Json(serde_json::json!([{ "subject": subject, "version": 1 }]))
}

async fn schema(State(_): State<MockState>, Path(_id): Path<u32>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "schema": "{}", "schemaType": "JSON" }))
}

/// Spawn a mock registry whose `ACCEPTED_SCHEMA_ID` resolves to
/// `accepted_subject`, and return a client pointed at it.
async fn mock_registry(accepted_subject: &'static str) -> Arc<SchemaRegistry> {
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(versions))
        .route("/schemas/ids/{id}", get(schema))
        .with_state(MockState { accepted_subject });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock registry");
    let addr = listener.local_addr().expect("mock registry addr");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock registry serve");
    });
    SchemaRegistry::builder(format!("http://{addr}")).build()
}

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
/// touches shove's own topology declarer — same reasoning as
/// `metrics_kafka_pre_handler_discard.rs`: the concurrent receive loop decodes
/// inline in delivery order only within a partition, so the marker-as-barrier
/// argument needs publish order to be consume order.
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

/// Build a Confluent JSON wire frame: `0x00` magic + 4-byte BE schema id +
/// JSON payload — same helper shape as `kafka_schema_registry_integration.rs`.
fn frame_json(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + payload.len());
    bytes.push(0x00);
    bytes.extend_from_slice(&schema_id.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

/// Publish a payload straight through rdkafka. Everything in this binary goes
/// through here: the poisons *must* bypass shove's publisher, and publishing
/// the framed marker the same way keeps the wire bytes explicit.
async fn publish_raw(brokers: &str, topic: &str, key: &str, payload: &[u8]) {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create raw producer");
    producer
        .send(
            FutureRecord::to(topic).key(key).payload(payload),
            Duration::from_secs(10),
        )
        .await
        .expect("raw publish should succeed");
}

/// Reads up to `expected` raw payloads off `topic`, giving up after `TIMEOUT`.
/// A typed `run_dlq` cannot be the barrier: both poisons fail the registry
/// decode in the DLQ drain exactly as they did on the main path.
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
// Topics and handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderEvent {
    id: u32,
}

shove::define_topic!(
    ConcurrentTopic,
    OrderEvent,
    TopologyBuilder::new(CONCURRENT_TOPIC).build()
);

shove::define_sequenced_topic!(
    FifoTopic,
    OrderEvent,
    |msg: &OrderEvent| msg.id.to_string(),
    TopologyBuilder::new(FIFO_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .allow_message_loss()
        .build()
);

shove::define_topic!(
    BatchTopic,
    OrderEvent,
    TopologyBuilder::new(BATCH_TOPIC).build()
);

shove::define_topic!(
    WithDlqTopic,
    OrderEvent,
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

    fn record(&self, msg: &OrderEvent) -> Outcome {
        self.handled_total.fetch_add(1, Ordering::SeqCst);
        if msg.id == self.marker_id {
            self.handled_marker.increment();
        }
        Outcome::Ack
    }
}

macro_rules! impl_barrier_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl MessageHandler<$topic> for BarrierHandler {
                type Context = ();
                async fn handle(&self, msg: OrderEvent, _meta: MessageMetadata, _: &()) -> Outcome {
                    self.record(&msg)
                }
            }
        )*
    };
}

impl_barrier_for!(ConcurrentTopic, FifoTopic, WithDlqTopic);

/// Records every batch and signals on each flush.
#[derive(Clone)]
struct RecordingBatchHandler {
    batches: Arc<Mutex<Vec<Vec<u32>>>>,
    signal: Arc<Notify>,
}

impl RecordingBatchHandler {
    fn new() -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            signal: Arc::new(Notify::new()),
        }
    }

    fn seen(&self) -> Vec<u32> {
        self.batches
            .lock()
            .expect("batches lock poisoned")
            .iter()
            .flatten()
            .copied()
            .collect()
    }

    async fn wait_for_id(&self, id: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.seen().contains(&id) {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => return self.seen().contains(&id),
            }
        }
    }
}

impl BatchMessageHandler<BatchTopic> for RecordingBatchHandler {
    type Context = ();
    async fn handle_batch(&self, messages: Vec<(OrderEvent, MessageMetadata)>, _: &()) -> Outcome {
        self.batches
            .lock()
            .expect("batches lock poisoned")
            .push(messages.iter().map(|(m, _)| m.id).collect());
        self.signal.notify_waiters();
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

/// Assert the no-DLQ contract for one scenario topic: one failure and one
/// discard per schema reason.
fn assert_no_dlq_schema_counts(snapshot: &Snapshot, topic: &str, path: &str) {
    assert_eq!(
        failed_total(snapshot, topic, "schema_frame"),
        1,
        "{path}: the unframed message must count exactly one schema_frame failure"
    );
    assert_eq!(
        failed_total(snapshot, topic, "schema_validation"),
        1,
        "{path}: the rejected-subject message must count exactly one \
         schema_validation failure"
    );
    assert_eq!(
        discarded_total(snapshot, topic, "schema_frame"),
        1,
        "{path}: with no DLQ declared, the unframed message is dropped on the \
         floor once its offset commits — the registry-decode branch must \
         settle a discard exactly like the plain-codec branch"
    );
    assert_eq!(
        discarded_total(snapshot, topic, "schema_validation"),
        1,
        "{path}: with no DLQ declared, the rejected-subject message is \
         dropped on the floor once its offset commits"
    );
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schema_registry_pre_handler_drops_count_as_discarded_only_with_no_dlq() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    const MARKER: u32 = 42;
    let valid_json = serde_json::to_vec(&OrderEvent { id: MARKER }).expect("serialize marker");

    // -- Scenario 1: concurrent, no DLQ ---------------------------------------
    {
        create_single_partition_topic(&tb.brokers, CONCURRENT_TOPIC).await;
        let registry = mock_registry("kafka-metrics-schema-prehandler-concurrent-value").await;
        publish_raw(&tb.brokers, CONCURRENT_TOPIC, "k-frame", b"{not framed").await;
        publish_raw(
            &tb.brokers,
            CONCURRENT_TOPIC,
            "k-subject",
            &frame_json(REJECTED_SCHEMA_ID, &valid_json),
        )
        .await;
        publish_raw(
            &tb.brokers,
            CONCURRENT_TOPIC,
            "k-marker",
            &frame_json(ACCEPTED_SCHEMA_ID, &valid_json),
        )
        .await;

        let handler = BarrierHandler::new(MARKER);
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(tb.client().await);
        let handle = tokio::spawn(async move {
            consumer
                .run::<ConcurrentTopic, _>(
                    h,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_schema_registry(registry),
                )
                .await
        });

        assert!(
            handler.handled_marker.wait_for(1, TIMEOUT).await,
            "the framed marker must reach the handler after the two schema \
             poisons ahead of it on the single partition"
        );
        shutdown.cancel();
        handle.await.expect("consumer task panicked").ok();

        assert_eq!(
            handler.handled_total.load(Ordering::SeqCst),
            1,
            "only the framed marker should ever reach the handler"
        );
    }

    // -- Scenario 2: FIFO, no DLQ ---------------------------------------------
    {
        let client = tb.client().await;
        let broker = Broker::<Kafka>::from_client(client.clone());
        broker
            .topology()
            .declare::<FifoTopic>()
            .await
            .expect("declare fifo topic");
        let registry = mock_registry("kafka-metrics-schema-prehandler-fifo-value").await;
        publish_raw(&tb.brokers, FIFO_TOPIC, "k-frame", b"{not framed").await;
        publish_raw(
            &tb.brokers,
            FIFO_TOPIC,
            "k-subject",
            &frame_json(REJECTED_SCHEMA_ID, &valid_json),
        )
        .await;
        publish_raw(
            &tb.brokers,
            FIFO_TOPIC,
            "k-marker",
            &frame_json(ACCEPTED_SCHEMA_ID, &valid_json),
        )
        .await;

        let handler = BarrierHandler::new(MARKER);
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(client);
        let handle = tokio::spawn(async move {
            consumer
                .run_fifo::<FifoTopic, _>(
                    h,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_max_retries(0)
                        .with_schema_registry(registry),
                )
                .await
        });

        assert!(
            handler.handled_marker.wait_for(1, TIMEOUT).await,
            "the framed marker must reach the FIFO handler after the two \
             schema poisons ahead of it on the single partition"
        );
        shutdown.cancel();
        handle.await.expect("fifo consumer task panicked").ok();
        broker.close().await;

        assert_eq!(
            handler.handled_total.load(Ordering::SeqCst),
            1,
            "only the framed marker should ever reach the FIFO handler"
        );
    }

    // -- Scenario 3: batch, no DLQ ---------------------------------------------
    {
        let client = tb.client().await;
        let broker = Broker::<Kafka>::from_client(client.clone());
        broker
            .topology()
            .declare::<BatchTopic>()
            .await
            .expect("declare batch topic");
        let registry = mock_registry("kafka-metrics-schema-prehandler-batch-value").await;
        publish_raw(&tb.brokers, BATCH_TOPIC, "k-frame", b"{not framed").await;
        publish_raw(
            &tb.brokers,
            BATCH_TOPIC,
            "k-subject",
            &frame_json(REJECTED_SCHEMA_ID, &valid_json),
        )
        .await;
        publish_raw(
            &tb.brokers,
            BATCH_TOPIC,
            "k-marker",
            &frame_json(ACCEPTED_SCHEMA_ID, &valid_json),
        )
        .await;

        let handler = RecordingBatchHandler::new();
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(client);
        let handle = tokio::spawn(async move {
            consumer
                .run_batch::<BatchTopic, _>(
                    h,
                    (),
                    BatchConsumerOptions::new()
                        // Exactly the number of messages published, so the
                        // flush triggers only once all three have arrived —
                        // dropped messages count toward the trigger.
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(10))
                        .with_schema_registry(registry)
                        .with_shutdown(sc),
                )
                .await
        });

        assert!(
            handler.wait_for_id(MARKER, TIMEOUT).await,
            "the framed marker must reach the batch handler"
        );
        shutdown.cancel();
        handle.await.expect("batch consumer task panicked").ok();
        broker.close().await;

        assert_eq!(
            handler.seen(),
            vec![MARKER],
            "only the framed marker should ever reach the batch handler"
        );
    }

    // -- Scenario 4: concurrent, DLQ declared and reachable --------------------
    {
        let client = tb.client().await;
        let broker = Broker::<Kafka>::from_client(client.clone());
        broker
            .topology()
            .declare::<WithDlqTopic>()
            .await
            .expect("declare dlq topic");
        let registry = mock_registry("kafka-metrics-schema-prehandler-dlq-value").await;
        const UNFRAMED: &[u8] = b"{not framed";
        let rejected = frame_json(REJECTED_SCHEMA_ID, &valid_json);
        publish_raw(&tb.brokers, DLQ_TOPIC, "k-frame", UNFRAMED).await;
        publish_raw(&tb.brokers, DLQ_TOPIC, "k-subject", &rejected).await;

        let handler = BarrierHandler::new(u32::MAX);
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(client.clone());
        let handle = tokio::spawn(async move {
            consumer
                .run::<WithDlqTopic, _>(
                    h,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_schema_registry(registry),
                )
                .await
        });

        let mut dead = drain_raw(&tb.brokers, &format!("{DLQ_TOPIC}-dlq"), 2).await;
        dead.sort();
        let mut expected = vec![UNFRAMED.to_vec(), rejected.clone()];
        expected.sort();
        assert_eq!(
            dead, expected,
            "both schema-rejected payloads should be preserved in the DLQ"
        );

        shutdown.cancel();
        broker.close().await;
        handle.await.expect("dlq consumer task panicked").ok();
    }

    // Single, draining snapshot, taken only once every consumer above has
    // stopped so nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    assert_no_dlq_schema_counts(&snapshot, CONCURRENT_TOPIC, "concurrent");
    assert_no_dlq_schema_counts(&snapshot, FIFO_TOPIC, "fifo");
    assert_no_dlq_schema_counts(&snapshot, BATCH_TOPIC, "batch");

    // -- Scenario 4 assertions -------------------------------------------------
    assert_eq!(
        failed_total(&snapshot, DLQ_TOPIC, "schema_frame"),
        1,
        "a DLQ does not make the frame rejection stop being a failure"
    );
    assert_eq!(
        failed_total(&snapshot, DLQ_TOPIC, "schema_validation"),
        1,
        "a DLQ does not make the subject rejection stop being a failure"
    );
    assert_eq!(
        discarded_total(&snapshot, DLQ_TOPIC, "schema_frame"),
        0,
        "the unframed message was observed arriving in the DLQ above, so \
         nothing was discarded; counting it here would fire a false data-loss \
         alert"
    );
    assert_eq!(
        discarded_total(&snapshot, DLQ_TOPIC, "schema_validation"),
        0,
        "the rejected-subject message was observed arriving in the DLQ above, \
         so nothing was discarded"
    );
}
