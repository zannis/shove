#![cfg(all(
    feature = "kafka",
    feature = "kafka-schema-registry",
    feature = "metrics"
))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration tests: a schema registry that cannot answer **stalls** the
//! record instead of discarding it, and a registry that answers with a
//! deployment fault **ends** the consumer instead of dead-lettering.
//!
//! The registry here is an in-process axum mock whose answer for one schema
//! id is a status the test flips at runtime (`503`, `401`, or a real answer).
//! Every other id resolves normally, so a batch can hold healthy records
//! ahead of the stalled one.
//!
//! Run with:
//! `cargo nextest run --features kafka,kafka-schema-registry,metrics --test kafka_schema_registry_outage`

use std::sync::atomic::{AtomicU16, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
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

use shove::BroadcastStart;
use shove::SequencedTopic as _;
use shove::ShoveError;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::kafka::{
    BatchConsumerOptions, KafkaAutoOffsetReset, KafkaClient, KafkaConfig, KafkaConsumer,
    KafkaLagStatsProvider, KafkaQueueStats, KafkaQueueStatsProvider,
};
use shove::markers::Kafka;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
#[cfg(feature = "test-support")]
use shove::schema_registry::SchemaId;
use shove::schema_registry::SchemaRegistry;
use shove::topology::{SequenceFailure, TopologyBuilder};

const TIMEOUT: Duration = Duration::from_secs(60);
/// Resolves normally on every mock.
const HEALTHY_ID: u32 = 1;
/// Answers with whatever status the test has set.
const FLAKY_ID: u32 = 9;

// ---------------------------------------------------------------------------
// Mock schema registry with a flippable answer for one id
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockState {
    accepted_subject: &'static str,
    /// The HTTP status returned for `FLAKY_ID`; 200 means "answer normally".
    flaky_status: Arc<AtomicU16>,
    hits: Arc<AtomicUsize>,
}

fn flaky_response(s: &MockState, id: u32, body: serde_json::Value) -> Response {
    s.hits.fetch_add(1, Ordering::SeqCst);
    let status = s.flaky_status.load(Ordering::SeqCst);
    if id == FLAKY_ID && status != 200 {
        return StatusCode::from_u16(status)
            .expect("valid status")
            .into_response();
    }
    Json(body).into_response()
}

async fn versions(State(s): State<MockState>, Path(id): Path<u32>) -> Response {
    let body = serde_json::json!([{ "subject": s.accepted_subject, "version": 1 }]);
    flaky_response(&s, id, body)
}

async fn schema(State(s): State<MockState>, Path(id): Path<u32>) -> Response {
    let body = serde_json::json!({ "schema": "{}", "schemaType": "JSON" });
    flaky_response(&s, id, body)
}

/// A registry client over a mock whose answer for `FLAKY_ID` starts as
/// `initial_status`, plus the handle that flips it.
async fn mock_registry(
    accepted_subject: &'static str,
    initial_status: u16,
) -> (Arc<SchemaRegistry>, Arc<AtomicU16>) {
    let flaky_status = Arc::new(AtomicU16::new(initial_status));
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(versions))
        .route("/schemas/ids/{id}", get(schema))
        .with_state(MockState {
            accepted_subject,
            flaky_status: flaky_status.clone(),
            hits: Arc::new(AtomicUsize::new(0)),
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
    // Fewer client retries keep each wait cycle short.
    let registry = SchemaRegistry::builder(format!("http://{addr}"))
        .max_retries(1)
        .build();
    (registry, flaky_status)
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

/// One partition, so publish order is consume order.
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

/// Confluent JSON wire frame: `0x00` magic + big-endian schema id + JSON.
fn frame_json(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + payload.len());
    bytes.push(0x00);
    bytes.extend_from_slice(&schema_id.to_be_bytes());
    bytes.extend_from_slice(payload);
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

/// The group's lag on `topic`: how many records sit past its committed
/// offset. The broker-side proof of what was, and was not, committed.
async fn lag(client: &KafkaClient, topic: &str, group: &str) -> u64 {
    let stats: KafkaQueueStats = KafkaLagStatsProvider::new(client.clone())
        .get_queue_stats(topic, group, KafkaAutoOffsetReset::Earliest)
        .await
        .expect("get_queue_stats should succeed");
    stats.messages_pending
}

async fn wait_for_lag(
    client: &KafkaClient,
    topic: &str,
    group: &str,
    want: u64,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let got = lag(client, topic, group).await;
        if got == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "lag on {topic} for {group} is {got}, wanted {want} within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// ---------------------------------------------------------------------------
// Topics and handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Event {
    id: u32,
}

shove::define_topic!(
    StallTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-stall").dlq().build()
);
shove::define_topic!(
    BatchStallTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-batch").dlq().build()
);
shove::define_topic!(
    BatchRetryStallTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-batch-retry")
        .dlq()
        .build()
);
shove::define_topic!(
    ShutdownTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-shutdown")
        .dlq()
        .build()
);
shove::define_topic!(
    AuthTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-auth").dlq().build()
);
shove::define_sequenced_topic!(
    FifoStallTopic,
    Event,
    |msg: &Event| msg.id.to_string(),
    TopologyBuilder::new("kafka-sr-outage-fifo")
        .sequenced(SequenceFailure::Skip)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);
shove::define_topic!(
    BroadcastStallTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-broadcast")
        .broadcast()
        .build()
);
shove::define_topic!(
    DrainStallTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-drain").dlq().build()
);
// An infra-owned topic, so a `Defer` waits in place and decodes the retained
// bytes again: the registry is asked a second time for the same record.
#[cfg(feature = "test-support")]
shove::define_topic!(
    InPlaceFaultTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-inplace-fault")
        .external()
        .hold_queue(Duration::from_millis(200))
        .allow_message_loss()
        .build()
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

    fn seen(&self) -> Vec<u32> {
        self.seen.lock().unwrap().clone()
    }
}

macro_rules! recorder_for {
    ($($topic:ty),+ $(,)?) => {$(
        impl MessageHandler<$topic> for Recorder {
            type Context = ();
            async fn handle(&self, msg: Event, _meta: MessageMetadata, _: &()) -> Outcome {
                self.seen.lock().unwrap().push(msg.id);
                self.counter.increment();
                Outcome::Ack
            }
        }
    )+};
}

recorder_for!(
    StallTopic,
    ShutdownTopic,
    AuthTopic,
    FifoStallTopic,
    BroadcastStallTopic
);

/// Records every delivery and returns `Defer` for the first one it sees,
/// then `Ack`.
#[cfg(feature = "test-support")]
#[derive(Clone)]
struct DeferOnceRecorder {
    seen: Arc<Mutex<Vec<u32>>>,
    counter: WaitableCounter,
}

#[cfg(feature = "test-support")]
impl MessageHandler<InPlaceFaultTopic> for DeferOnceRecorder {
    type Context = ();
    async fn handle(&self, msg: Event, _meta: MessageMetadata, _: &()) -> Outcome {
        let mut seen = self.seen.lock().unwrap();
        let first = seen.is_empty();
        seen.push(msg.id);
        drop(seen);
        self.counter.increment();
        if first { Outcome::Defer } else { Outcome::Ack }
    }
}

/// Records what the DLQ drain hands to `handle_dead`.
#[derive(Clone)]
struct DeadRecorder {
    dead: Arc<Mutex<Vec<u32>>>,
    counter: WaitableCounter,
}

impl DeadRecorder {
    fn new() -> Self {
        Self {
            dead: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }

    fn dead(&self) -> Vec<u32> {
        self.dead.lock().unwrap().clone()
    }
}

impl MessageHandler<DrainStallTopic> for DeadRecorder {
    type Context = ();
    async fn handle(&self, _msg: Event, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, msg: Event, _meta: DeadMessageMetadata, _: &()) {
        self.dead.lock().unwrap().push(msg.id);
        self.counter.increment();
    }
}

/// Records each flush as the ids it carried.
#[derive(Clone)]
struct BatchRecorder {
    batches: Arc<Mutex<Vec<Vec<u32>>>>,
    counter: WaitableCounter,
}

impl BatchRecorder {
    fn new() -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }

    fn batches(&self) -> Vec<Vec<u32>> {
        self.batches.lock().unwrap().clone()
    }
}

impl BatchMessageHandler<BatchStallTopic> for BatchRecorder {
    type Context = ();
    async fn handle_batch(&self, messages: Vec<(Event, MessageMetadata)>, _: &()) -> Outcome {
        self.batches
            .lock()
            .unwrap()
            .push(messages.iter().map(|(m, _)| m.id).collect());
        self.counter.increment();
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Metric helpers (used by the stall test only, which owns the recorder slot)
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

fn discarded_series(snapshot: &Snapshot, topic: &str) -> Vec<(String, u64)> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_discarded_total")
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "topic" && l.value() == topic)
        })
        .map(|(k, (_, _, value))| {
            let reason = k
                .key()
                .labels()
                .find(|l| l.key() == "reason")
                .map_or_else(|| "<unlabelled>".to_string(), |l| l.value().to_string());
            match value {
                DebugValue::Counter(n) => (reason, *n),
                other => panic!("shove_messages_discarded_total is not a counter: {other:?}"),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A 503 from the registry stalls the record: nothing reaches the handler,
/// nothing commits, `schema_unavailable` counts once per wait, and when the
/// registry answers the same record is delivered from the same offset with
/// no discard recorded.
#[tokio::test]
async fn an_unavailable_registry_stalls_the_record_and_resumes() {
    const TOPIC: &str = "kafka-sr-outage-stall";
    const GROUP: &str = "kafka-sr-outage-stall-consumer";
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status) = mock_registry("kafka-sr-outage-stall-value", 503).await;
    let client = tb.client().await;
    let body = serde_json::to_vec(&Event { id: 1 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(FLAKY_ID, &body)).await;

    let handler = Recorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<StallTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_concurrent_processing(false)
                    .with_schema_registry(registry)
                    .with_shutdown(sc),
            )
            .await
    });

    // Long enough for the group to form and for several waits to pass.
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(
        handler.seen(),
        Vec::<u32>::new(),
        "nothing may reach the handler"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        1,
        "the record must stay uncommitted"
    );

    status.store(200, Ordering::SeqCst);
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "delivered once the registry answers"
    );
    assert_eq!(handler.seen(), vec![1]);
    wait_for_lag(&client, TOPIC, GROUP, 0, TIMEOUT).await;

    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");

    let snapshot = snapshotter.snapshot().into_hashmap();
    assert!(
        counter_total(
            &snapshot,
            "shove_messages_failed_total",
            TOPIC,
            "schema_unavailable"
        ) >= 2,
        "one schema_unavailable per wait, and several waits passed"
    );
    assert_eq!(
        discarded_series(&snapshot, TOPIC),
        Vec::<(String, u64)>::new(),
        "a stalled record is never a discard"
    );
    assert_eq!(
        counter_total(
            &snapshot,
            "shove_messages_failed_total",
            TOPIC,
            "schema_validation"
        ),
        0,
        "an outage is not a validation failure"
    );
}

/// On the batch path the records ahead of the stalled one are flushed and
/// committed on their own, so the committed span ends before the stalled
/// record until it decodes, and then it flushes as a batch of its own.
#[tokio::test]
async fn a_batch_flushes_before_the_parked_record_and_resumes() {
    const TOPIC: &str = "kafka-sr-outage-batch";
    const GROUP: &str = "kafka-sr-outage-batch-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status) = mock_registry("kafka-sr-outage-batch-value", 503).await;
    let client = tb.client().await;
    for (schema_id, id) in [(HEALTHY_ID, 1u32), (HEALTHY_ID, 2), (FLAKY_ID, 3)] {
        let body = serde_json::to_vec(&Event { id }).unwrap();
        publish_raw(&tb.brokers, TOPIC, &frame_json(schema_id, &body)).await;
    }

    let handler = BatchRecorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<BatchStallTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(10)
                    // Long enough that the first flush below is the stall's
                    // doing, short enough that the parked record's batch of
                    // one does not hold the suite for half a minute.
                    .with_max_batch_age(Duration::from_secs(5))
                    .with_schema_registry(registry)
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the records ahead of the stalled one flush without waiting for the age trigger"
    );
    assert_eq!(handler.batches(), vec![vec![1, 2]]);
    wait_for_lag(&client, TOPIC, GROUP, 1, TIMEOUT).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        1,
        "the span must end before the stalled record"
    );
    assert_eq!(handler.batches().len(), 1);

    status.store(200, Ordering::SeqCst);
    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "the parked record flushes once it decodes"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    // A batch of one, ended by the age trigger before the shutdown below, so
    // the shutdown flush is not what delivered it.
    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");
    assert_eq!(handler.batches(), vec![vec![1, 2], vec![3]]);
    wait_for_lag(&client, TOPIC, GROUP, 0, TIMEOUT).await;
}

/// Returns `Retry` for its first two flushes and `Ack` afterwards, recording
/// what each flush carried and what was acked.
#[derive(Clone)]
struct RetryTwiceRecorder {
    batches: Arc<Mutex<Vec<Vec<u32>>>>,
    acked: Arc<Mutex<Vec<u32>>>,
    counter: WaitableCounter,
}

impl RetryTwiceRecorder {
    fn new() -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            acked: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }
}

impl BatchMessageHandler<BatchRetryStallTopic> for RetryTwiceRecorder {
    type Context = ();
    async fn handle_batch(&self, messages: Vec<(Event, MessageMetadata)>, _: &()) -> Outcome {
        let ids: Vec<u32> = messages.iter().map(|(m, _)| m.id).collect();
        let flushes = {
            let mut batches = self.batches.lock().unwrap();
            batches.push(ids.clone());
            batches.len()
        };
        self.counter.increment();
        if flushes <= 2 {
            Outcome::Retry
        } else {
            self.acked.lock().unwrap().extend(ids);
            Outcome::Ack
        }
    }
}

/// The flush ahead of a stalled record may itself come back `Retry`, which
/// seeks the span's partition back to its start. The stalled record then sits
/// after records the broker is about to deliver again, and the copy the loop
/// holds must not be buffered ahead of them: that handed the handler
/// `[3, 1, 2]` and, on the next `Retry`, sought to the parked record's
/// offset, so records 1 and 2 were committed past without ever being acked.
/// The record is put back instead and every batch arrives in offset order.
#[tokio::test]
async fn a_retry_before_a_registry_stall_redelivers_the_rewound_span_in_order() {
    const TOPIC: &str = "kafka-sr-outage-batch-retry";
    const GROUP: &str = "kafka-sr-outage-batch-retry-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status) = mock_registry("kafka-sr-outage-batch-retry-value", 503).await;
    let client = tb.client().await;
    for (schema_id, id) in [(HEALTHY_ID, 1u32), (HEALTHY_ID, 2), (FLAKY_ID, 3)] {
        let body = serde_json::to_vec(&Event { id }).unwrap();
        publish_raw(&tb.brokers, TOPIC, &frame_json(schema_id, &body)).await;
    }

    let handler = RetryTwiceRecorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<BatchRetryStallTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_secs(5))
                    .with_schema_registry(registry)
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the records ahead of the stalled one flush first"
    );
    assert_eq!(handler.batches.lock().unwrap().clone(), vec![vec![1, 2]]);
    // The `Retry` seeks back to offset 0 and the loop enters its registry wait.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(lag(&client, TOPIC, GROUP).await, 3, "nothing is committed");

    status.store(200, Ordering::SeqCst);
    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "the rewound span and the parked record are redelivered, twice"
    );
    wait_for_lag(&client, TOPIC, GROUP, 0, TIMEOUT).await;
    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");

    let batches = handler.batches.lock().unwrap().clone();
    assert_eq!(
        batches,
        vec![vec![1, 2], vec![1, 2, 3], vec![1, 2, 3]],
        "every redelivered batch is in offset order with the parked record last"
    );
    let acked = handler.acked.lock().unwrap().clone();
    assert_eq!(
        acked,
        vec![1, 2, 3],
        "the committed span covers only records the handler acked"
    );
}

/// Shutdown during a registry wait returns promptly, commits nothing for the
/// stalled record, and a restarted consumer is handed it again once the
/// registry answers.
#[tokio::test]
async fn shutdown_during_a_registry_wait_leaves_the_record_uncommitted() {
    const TOPIC: &str = "kafka-sr-outage-shutdown";
    const GROUP: &str = "kafka-sr-outage-shutdown-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status) = mock_registry("kafka-sr-outage-shutdown-value", 503).await;
    let client = tb.client().await;
    let body = serde_json::to_vec(&Event { id: 7 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(FLAKY_ID, &body)).await;

    let handler = Recorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let first_registry = registry.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run::<ShutdownTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_concurrent_processing(false)
                    .with_schema_registry(first_registry)
                    .with_shutdown(sc),
            )
            .await
    });
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(handler.seen(), Vec::<u32>::new());

    let cancelled_at = Instant::now();
    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");
    assert!(
        cancelled_at.elapsed() < Duration::from_secs(5),
        "shutdown must not wait out the registry"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        1,
        "the stalled record stays uncommitted"
    );

    // Registry back, fresh client and consumer: the record is redelivered.
    status.store(200, Ordering::SeqCst);
    let restarted = Recorder::new();
    let h = restarted.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client().await);
    let handle = tokio::spawn(async move {
        consumer
            .run::<ShutdownTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_concurrent_processing(false)
                    .with_schema_registry(registry)
                    .with_shutdown(sc),
            )
            .await
    });
    assert!(
        restarted.counter.wait_for(1, TIMEOUT).await,
        "redelivered after the restart"
    );
    assert_eq!(restarted.seen(), vec![7]);
    wait_for_lag(&client, TOPIC, GROUP, 0, TIMEOUT).await;
    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");
}

/// A 401 is a deployment fault, not an outage: the consumer ends with
/// `Topology` instead of waiting or dead-lettering, and the record stays
/// uncommitted for whoever runs next with working credentials.
#[tokio::test]
async fn an_authentication_failure_ends_the_consumer() {
    const TOPIC: &str = "kafka-sr-outage-auth";
    const GROUP: &str = "kafka-sr-outage-auth-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, _status) = mock_registry("kafka-sr-outage-auth-value", 401).await;
    let client = tb.client().await;
    let body = serde_json::to_vec(&Event { id: 4 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(FLAKY_ID, &body)).await;

    let handler = Recorder::new();
    let h = handler.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let outcome = tokio::time::timeout(
        TIMEOUT,
        consumer.run::<AuthTopic, _>(
            h,
            (),
            ConsumerOptions::<Kafka>::new()
                .with_concurrent_processing(false)
                .with_schema_registry(registry),
        ),
    )
    .await
    .expect("the consumer must end on its own, not wait");
    match outcome {
        Err(ShoveError::Topology(message)) => {
            assert!(
                message.contains("schema id 9") && message.contains("deployment fault"),
                "the error names the schema id and the fault: {message}"
            );
        }
        other => panic!("expected ShoveError::Topology, got {other:?}"),
    }
    assert_eq!(
        handler.seen(),
        Vec::<u32>::new(),
        "nothing reached the handler"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        1,
        "the record stays uncommitted"
    );
}

/// The FIFO loop stalls the same way as the concurrent one: nothing reaches
/// the handler and nothing commits while the first record's schema cannot be
/// resolved, and once the registry answers the stalled record is delivered
/// ahead of the record behind it.
#[tokio::test]
async fn a_fifo_consumer_stalls_and_resumes() {
    const TOPIC: &str = "kafka-sr-outage-fifo";
    const GROUP: &str = "kafka-sr-outage-fifo-fifo";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status) = mock_registry("kafka-sr-outage-fifo-value", 503).await;
    let client = tb.client().await;
    for (schema_id, id) in [(FLAKY_ID, 1u32), (HEALTHY_ID, 2)] {
        let body = serde_json::to_vec(&Event { id }).unwrap();
        publish_raw(&tb.brokers, TOPIC, &frame_json(schema_id, &body)).await;
    }

    let handler = Recorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<FifoStallTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_schema_registry(registry)
                    .with_shutdown(sc),
            )
            .await
    });

    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(
        !handle.is_finished(),
        "the FIFO consumer must still be running while it waits on the registry"
    );
    assert_eq!(
        handler.seen(),
        Vec::<u32>::new(),
        "nothing may reach the handler while the first record stalls"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        2,
        "nothing is committed while the first record stalls"
    );

    status.store(200, Ordering::SeqCst);
    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "both records are delivered once the registry answers"
    );
    assert_eq!(
        handler.seen(),
        vec![1, 2],
        "the stalled record first, then the record behind it"
    );
    wait_for_lag(&client, TOPIC, GROUP, 0, TIMEOUT).await;

    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");
}

/// A broadcast subscription stalls in place too: the record behind the
/// stalled one is held back, and both arrive in order once the registry
/// answers. The subscription starts at the head so the records published
/// before it exist are what it reads.
#[tokio::test]
async fn a_broadcast_subscription_stalls_and_resumes() {
    const TOPIC: &str = "kafka-sr-outage-broadcast";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status) = mock_registry("kafka-sr-outage-broadcast-value", 503).await;
    for (schema_id, id) in [(FLAKY_ID, 1u32), (HEALTHY_ID, 2)] {
        let body = serde_json::to_vec(&Event { id }).unwrap();
        publish_raw(&tb.brokers, TOPIC, &frame_json(schema_id, &body)).await;
    }

    let broker = Broker::<Kafka>::from_client(tb.client().await);
    let handler = Recorder::new();
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<BroadcastStallTopic, _>(
            handler.clone(),
            ConsumerOptions::new()
                .with_schema_registry(registry)
                .with_broadcast_start(BroadcastStart::Head),
        )
        .expect("failed to subscribe");

    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(
        handler.seen(),
        Vec::<u32>::new(),
        "nothing may reach the handler while the first record stalls"
    );

    status.store(200, Ordering::SeqCst);
    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "both records are delivered once the registry answers"
    );
    assert_eq!(
        handler.seen(),
        vec![1, 2],
        "the stalled record first, then the record behind it"
    );

    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending(), Duration::from_secs(5))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
}

/// The DLQ drain stalls instead of acking a dead message it cannot decode:
/// nothing reaches `handle_dead` and nothing commits while the first dead
/// message's schema cannot be resolved, and both dead messages are handled
/// in order once the registry answers.
#[tokio::test]
async fn a_dlq_drain_stalls_and_resumes() {
    const DLQ: &str = "kafka-sr-outage-drain-dlq";
    const GROUP: &str = "kafka-sr-outage-drain-dlq-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, DLQ).await;
    // The drain accepts the DLQ's own default subject.
    let (registry, status) = mock_registry("kafka-sr-outage-drain-dlq-value", 503).await;
    let client = tb.client().await;
    for (schema_id, id) in [(FLAKY_ID, 1u32), (HEALTHY_ID, 2)] {
        let body = serde_json::to_vec(&Event { id }).unwrap();
        publish_raw(&tb.brokers, DLQ, &frame_json(schema_id, &body)).await;
    }

    let handler = DeadRecorder::new();
    let h = handler.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_dlq_with_options::<DrainStallTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new().with_schema_registry(registry),
            )
            .await
    });

    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(
        !handle.is_finished(),
        "the drain must still be running while it waits on the registry"
    );
    assert_eq!(
        handler.dead(),
        Vec::<u32>::new(),
        "nothing may reach handle_dead while the first dead message stalls"
    );
    assert_eq!(
        lag(&client, DLQ, GROUP).await,
        2,
        "the drain commits nothing while the first dead message stalls"
    );

    status.store(200, Ordering::SeqCst);
    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "both dead messages are handled once the registry answers"
    );
    assert_eq!(
        handler.dead(),
        vec![1, 2],
        "the stalled dead message first, then the one behind it"
    );
    wait_for_lag(&client, DLQ, GROUP, 0, TIMEOUT).await;

    // The DLQ loop stops on its client's shutdown token.
    client.shutdown_token().cancel();
    handle
        .await
        .expect("drain task panicked")
        .expect("drain ended cleanly");
}

/// A registry deployment fault met during an in-place redelivery ends the
/// consumer with `Topology`, as the receive loop does when it meets the fault
/// itself: the handler task reports it over the loop's fault channel, every
/// sibling drains, the busy flag clears, and the record stays uncommitted
/// rather than pinned in a consumer that keeps polling.
///
/// The public client caches a resolved schema id for good, so the second
/// decode would never ask the registry again: the `test-support` seam evicts
/// the id between the first delivery and the redelivery.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_registry_deployment_fault_during_an_in_place_redelivery_ends_the_consumer() {
    const TOPIC: &str = "kafka-sr-outage-inplace-fault";
    const GROUP: &str = "kafka-sr-outage-inplace-fault-consumer";
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status) = mock_registry("kafka-sr-outage-inplace-fault-value", 200).await;
    let client = tb.client().await;
    let body = serde_json::to_vec(&Event { id: 5 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(FLAKY_ID, &body)).await;

    let handler = DeferOnceRecorder {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let h = handler.clone();
    let options = ConsumerOptions::<Kafka>::new()
        .with_concurrent_processing(false)
        .with_schema_registry(registry.clone())
        .with_shutdown(CancellationToken::new());
    let processing = options.processing_handle();
    let consumer = KafkaConsumer::new(client.clone());
    let running =
        tokio::spawn(async move { consumer.run::<InPlaceFaultTopic, _>(h, (), options).await });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the record reached the handler once"
    );
    // The redelivery decodes the retained bytes again; make that lookup meet
    // a fault instead of the cache.
    registry.evict_for_test(SchemaId(FLAKY_ID));
    status.store(401, Ordering::SeqCst);

    let outcome = tokio::time::timeout(TIMEOUT, running)
        .await
        .expect("the consumer must end on its own, not keep polling with the record pinned")
        .expect("consumer task panicked");
    match outcome {
        Err(ShoveError::Topology(message)) => {
            assert!(
                message.contains("schema id 9") && message.contains("deployment fault"),
                "the error names the schema id and the fault: {message}"
            );
        }
        other => panic!("expected ShoveError::Topology, got {other:?}"),
    }
    assert_eq!(
        handler.seen.lock().unwrap().clone(),
        vec![5],
        "one delivery, no redelivery"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        1,
        "the record stays uncommitted for whoever runs next"
    );
    assert!(
        !processing.load(Ordering::Acquire),
        "every handler task ended before the loop returned, so the member reads idle"
    );
    let snapshot = snapshotter.snapshot().into_hashmap();
    assert_eq!(
        discarded_series(&snapshot, TOPIC),
        Vec::<(String, u64)>::new(),
        "a deployment fault is not a discard"
    );
}
