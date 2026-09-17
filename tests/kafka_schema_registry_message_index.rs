#![cfg(all(
    feature = "kafka",
    feature = "kafka-schema-registry",
    feature = "protobuf",
    feature = "metrics"
))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test for `require_schema_message_index`.
//!
//! A Confluent protobuf frame names which message of its schema file the
//! bytes encode. With the requirement set, a frame carrying another index must
//! be dropped **before** the handler and **before** the registry is asked
//! anything: it counts as `schema_frame`, it settles a discard when the topic
//! has no DLQ, and the mock registry sees no extra resolve for it. Frames with
//! the required index, in both encodings Confluent uses for `[0]`, still
//! decode.
//!
//! Single partition, so the third frame reaching the handler proves the
//! rejected second frame ahead of it was decoded and dropped. The snapshot is
//! taken after the consumer has stopped, once the shutdown drain has settled
//! the pending discard. `DebuggingRecorder` takes the global recorder slot,
//! hence a binary of its own. The single-message test owns that slot; the
//! batch case installs no recorder and asserts on the handler and the mock
//! alone, so both tests can share one process.
//!
//! Run with:
//! `cargo nextest run --features kafka,kafka-schema-registry,protobuf,metrics --test kafka_schema_registry_message_index`

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{Json, Router, extract::Path, extract::State, routing::get};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::consumer::ConsumerOptions;
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::kafka::{BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::schema_registry::SchemaRegistry;
use shove::topology::TopologyBuilder;

const TIMEOUT: Duration = Duration::from_secs(60);
const TOPIC: &str = "kafka-schema-message-index";
const SCHEMA_ID: u32 = 7;

// ---------------------------------------------------------------------------
// Mock schema registry, counting resolves
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockState {
    resolves: Arc<AtomicUsize>,
}

async fn versions(State(s): State<MockState>, Path(_id): Path<u32>) -> Json<serde_json::Value> {
    s.resolves.fetch_add(1, Ordering::SeqCst);
    Json(serde_json::json!([{ "subject": format!("{TOPIC}-value"), "version": 1 }]))
}

async fn schema(State(_): State<MockState>, Path(_id): Path<u32>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "schema": "syntax = \"proto3\"; message Tick { uint32 seq = 1; }",
        "schemaType": "PROTOBUF"
    }))
}

/// Spawn a mock registry resolving every id to the topic's default subject,
/// returning a client pointed at it and the count of resolve requests.
async fn mock_registry() -> (Arc<SchemaRegistry>, Arc<AtomicUsize>) {
    let resolves = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(versions))
        .route("/schemas/ids/{id}", get(schema))
        .with_state(MockState {
            resolves: resolves.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock registry");
    let addr = listener.local_addr().expect("mock registry addr");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock registry serve");
    });
    (
        SchemaRegistry::builder(format!("http://{addr}")).build(),
        resolves,
    )
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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
            let mut notified = std::pin::pin!(self.signal.notified());
            notified.as_mut().enable();
            if self.get() >= target {
                return true;
            }
            tokio::select! {
                _ = &mut notified => {}
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
        Self {
            _container: container,
            brokers: format!("127.0.0.1:{port}"),
        }
    }

    async fn client(&self) -> KafkaClient {
        KafkaClient::connect_with_retry(&KafkaConfig::new(&self.brokers), 10)
            .await
            .expect("failed to connect to Kafka")
    }
}

/// One partition, so publish order is consume order and the last frame is a
/// barrier for the ones ahead of it.
async fn create_single_partition_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create admin client");
    admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create_topics RPC failed")
        .into_iter()
        .for_each(|r| {
            r.expect("topic creation failed");
        });
}

/// Confluent protobuf frame: magic byte, big-endian schema id, the
/// message-index bytes exactly as given, then the proto bytes.
fn frame_protobuf(schema_id: u32, message_index_bytes: &[u8], proto_bytes: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + message_index_bytes.len() + proto_bytes.len());
    bytes.push(0x00);
    bytes.extend_from_slice(&schema_id.to_be_bytes());
    bytes.extend_from_slice(message_index_bytes);
    bytes.extend_from_slice(proto_bytes);
    bytes
}

async fn publish_raw(brokers: &str, topic: &str, payload: &[u8]) {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create raw producer");
    producer
        .send(
            FutureRecord::to(topic).key("k").payload(payload),
            Duration::from_secs(10),
        )
        .await
        .expect("raw publish should succeed");
}

// ---------------------------------------------------------------------------
// Topic and handler
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
struct Tick {
    #[prost(uint32, tag = "1")]
    seq: u32,
}

shove::define_topic!(
    TickTopic,
    Tick,
    TopologyBuilder::new("kafka-schema-message-index").build(),
    codec = shove::ProtobufCodec
);

#[derive(Clone)]
struct Recorder {
    seen: Arc<Mutex<Vec<u32>>>,
    counter: WaitableCounter,
}

impl Recorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }
}

impl MessageHandler<TickTopic> for Recorder {
    type Context = ();
    async fn handle(&self, msg: Tick, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().unwrap().push(msg.seq);
        self.counter.increment();
        Outcome::Ack
    }
}

/// Records the `seq` of every message a batch flush carried, in order.
#[derive(Clone)]
struct BatchRecorder {
    seen: Arc<Mutex<Vec<u32>>>,
    counter: WaitableCounter,
}

impl BatchRecorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }
}

impl BatchMessageHandler<TickTopic> for BatchRecorder {
    type Context = ();
    async fn handle_batch(&self, messages: Vec<(Tick, MessageMetadata)>, _: &()) -> Outcome {
        let mut seen = self.seen.lock().unwrap();
        for (msg, _) in &messages {
            seen.push(msg.seq);
            self.counter.increment();
        }
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Metric helpers
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

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_frame_with_another_message_index_is_dropped_before_the_registry() {
    use prost::Message as _;

    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, resolves) = mock_registry().await;

    let tick = |seq: u32| Tick { seq }.encode_to_vec();
    // Index [0] in the single-byte shorthand: accepted.
    publish_raw(
        &tb.brokers,
        TOPIC,
        &frame_protobuf(SCHEMA_ID, &[0x00], &tick(1)),
    )
    .await;
    // Index [1], as a Confluent serializer writes it (count 1 and index 1 as
    // zigzag varints): rejected before the registry is consulted.
    publish_raw(
        &tb.brokers,
        TOPIC,
        &frame_protobuf(SCHEMA_ID, &[0x02, 0x02], &tick(2)),
    )
    .await;
    // Index [0] in the explicit encoding: accepted, and the barrier.
    publish_raw(
        &tb.brokers,
        TOPIC,
        &frame_protobuf(SCHEMA_ID, &[0x02, 0x00], &tick(3)),
    )
    .await;

    let handler = Recorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client().await);
    let handle = tokio::spawn(async move {
        consumer
            .run::<TickTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_schema_registry(registry)
                    .require_schema_message_index([0]),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "both [0] frames must reach the handler, the barrier included"
    );
    shutdown.cancel();
    handle.await.expect("consumer task panicked").ok();

    assert_eq!(
        handler.seen.lock().unwrap().clone(),
        vec![1, 3],
        "only the frames carrying the required index reach the handler"
    );
    assert_eq!(
        resolves.load(Ordering::SeqCst),
        1,
        "the schema id resolves once for the first accepted frame and is then \
         cached; the rejected frame must not add a resolve"
    );

    let snapshot = snapshotter.snapshot().into_hashmap();
    assert_eq!(
        counter_total(
            &snapshot,
            "shove_messages_failed_total",
            TOPIC,
            "schema_frame"
        ),
        1,
        "the rejected index counts exactly one schema_frame failure"
    );
    assert_eq!(
        counter_total(
            &snapshot,
            "shove_messages_discarded_total",
            TOPIC,
            "schema_frame"
        ),
        1,
        "with no DLQ declared the rejected frame is discarded once its offset commits"
    );
}

/// The batch path applies the same requirement: the frame with another index
/// is dropped before the registry is asked, and the two `[0]` frames reach the
/// handler in offset order.
#[tokio::test]
async fn a_batch_consumer_drops_a_frame_with_another_message_index_before_the_registry() {
    use prost::Message as _;

    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, resolves) = mock_registry().await;

    let tick = |seq: u32| Tick { seq }.encode_to_vec();
    publish_raw(
        &tb.brokers,
        TOPIC,
        &frame_protobuf(SCHEMA_ID, &[0x00], &tick(1)),
    )
    .await;
    publish_raw(
        &tb.brokers,
        TOPIC,
        &frame_protobuf(SCHEMA_ID, &[0x02, 0x02], &tick(2)),
    )
    .await;
    publish_raw(
        &tb.brokers,
        TOPIC,
        &frame_protobuf(SCHEMA_ID, &[0x02, 0x00], &tick(3)),
    )
    .await;

    let handler = BatchRecorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client().await);
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<TickTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(10)
                    .with_max_batch_age(Duration::from_secs(2))
                    .with_schema_registry(registry)
                    .require_schema_message_index([0])
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "both [0] frames must reach the batch handler"
    );
    shutdown.cancel();
    handle.await.expect("consumer task panicked").ok();

    assert_eq!(
        handler.seen.lock().unwrap().clone(),
        vec![1, 3],
        "only the frames carrying the required index reach the handler"
    );
    assert_eq!(
        resolves.load(Ordering::SeqCst),
        1,
        "the rejected frame must not add a resolve on the batch path either"
    );
}
