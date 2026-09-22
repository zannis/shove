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
/// Answers with a second status the test can set on its own, 200 by default,
/// for a test that needs one record to stall while another meets a fault.
const SECOND_FLAKY_ID: u32 = 10;
/// A "status" of zero for either flaky id means: accept the request and never
/// answer it, the shape of a registry that is up but hung.
const HANG_STATUS: u16 = 0;
/// How long a shutdown that lands inside a hung lookup may take: far below
/// the registry client's own timeout and retries, which the loop must not
/// wait out.
const PROMPT: Duration = Duration::from_secs(3);

// ---------------------------------------------------------------------------
// Mock schema registry with a flippable answer for one id
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockState {
    accepted_subject: &'static str,
    /// The HTTP status returned for `FLAKY_ID`; 200 means "answer normally".
    flaky_status: Arc<AtomicU16>,
    /// The same for `SECOND_FLAKY_ID`.
    second_status: Arc<AtomicU16>,
    hits: Arc<AtomicUsize>,
}

fn status_for(s: &MockState, id: u32) -> u16 {
    match id {
        FLAKY_ID => s.flaky_status.load(Ordering::SeqCst),
        SECOND_FLAKY_ID => s.second_status.load(Ordering::SeqCst),
        _ => 200,
    }
}

/// Never resolves when the id's status is [`HANG_STATUS`]; the request still
/// counts as a hit, so a test can gate on the lookup being in flight.
async fn hang_if_silent(s: &MockState, id: u32) {
    if status_for(s, id) == HANG_STATUS {
        s.hits.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<()>().await;
    }
}

fn flaky_response(s: &MockState, id: u32, body: serde_json::Value) -> Response {
    s.hits.fetch_add(1, Ordering::SeqCst);
    let status = status_for(s, id);
    if status != 200 {
        return StatusCode::from_u16(status)
            .expect("valid status")
            .into_response();
    }
    Json(body).into_response()
}

async fn versions(State(s): State<MockState>, Path(id): Path<u32>) -> Response {
    hang_if_silent(&s, id).await;
    let body = serde_json::json!([{ "subject": s.accepted_subject, "version": 1 }]);
    flaky_response(&s, id, body)
}

async fn schema(State(s): State<MockState>, Path(id): Path<u32>) -> Response {
    hang_if_silent(&s, id).await;
    let body = serde_json::json!({ "schema": "{}", "schemaType": "JSON" });
    flaky_response(&s, id, body)
}

/// A registry client over a mock whose answer for `FLAKY_ID` starts as
/// `initial_status`, plus the handle that flips it and the mock's request
/// counter, so a test can gate on an observed registry request instead of a
/// sleep.
async fn mock_registry(
    accepted_subject: &'static str,
    initial_status: u16,
) -> (Arc<SchemaRegistry>, Arc<AtomicU16>, Arc<AtomicUsize>) {
    let (registry, flaky_status, _second, hits) =
        mock_registry_with_second_id(accepted_subject, initial_status).await;
    (registry, flaky_status, hits)
}

/// [`mock_registry`] plus the handle for `SECOND_FLAKY_ID`'s status, which
/// starts at 200.
async fn mock_registry_with_second_id(
    accepted_subject: &'static str,
    initial_status: u16,
) -> (
    Arc<SchemaRegistry>,
    Arc<AtomicU16>,
    Arc<AtomicU16>,
    Arc<AtomicUsize>,
) {
    let flaky_status = Arc::new(AtomicU16::new(initial_status));
    let second_status = Arc::new(AtomicU16::new(200));
    let hits = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(versions))
        .route("/schemas/ids/{id}", get(schema))
        .with_state(MockState {
            accepted_subject,
            flaky_status: flaky_status.clone(),
            second_status: second_status.clone(),
            hits: hits.clone(),
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
    (registry, flaky_status, second_status, hits)
}

/// Read up to `expected` raw payloads off `topic` with a throwaway group,
/// from the beginning. The dead-letter topic's records keep the Confluent
/// frame the original carried, so they are read raw rather than decoded.
#[cfg(feature = "test-support")]
async fn drain_raw(brokers: &str, topic: &str, expected: usize, timeout: Duration) -> Vec<Vec<u8>> {
    use rdkafka::Message as _;
    use rdkafka::consumer::{Consumer as _, StreamConsumer};

    let consumer: StreamConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("{topic}-raw-drain"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("failed to create raw drain consumer");
    consumer
        .subscribe(&[topic])
        .expect("failed to subscribe the raw drain");
    let deadline = Instant::now() + timeout;
    let mut out = Vec::new();
    while out.len() < expected {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => out.push(msg.payload().unwrap_or_default().to_vec()),
            Ok(Err(e)) => panic!("raw drain recv failed: {e}"),
            Err(_) => break,
        }
    }
    out
}

/// Poll `hits` until it exceeds `above`, the observable proof that the
/// registry was asked again.
async fn wait_for_hits_above(hits: &AtomicUsize, above: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if hits.load(Ordering::SeqCst) > above {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the registry was not asked again within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
    create_topic(brokers, topic, 1).await;
}

/// `topic` with `partitions` partitions, through a raw admin client.
async fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create admin client");
    admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create_topics RPC failed")
        .into_iter()
        .for_each(|r| {
            r.expect("topic creation failed");
        });
}

/// Wait until the broker reports `group` as `Stable` with exactly `members`
/// members, so a record published afterwards lands after the assignment the
/// test reasons about was taken.
///
/// A coordinator that is moving or loading while the member joins answers the
/// probe with an error that means "ask again"; those are retried within the
/// deadline, as `tests/kafka_offset_reset_integration.rs` retries them, and
/// the last one is reported if the deadline passes. Any other error is a
/// broken probe and fails at once.
async fn wait_for_group_members(brokers: &str, group: &str, members: usize, timeout: Duration) {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    fn is_transient(code: RDKafkaErrorCode) -> bool {
        matches!(
            code,
            RDKafkaErrorCode::NotCoordinator
                | RDKafkaErrorCode::CoordinatorNotAvailable
                | RDKafkaErrorCode::CoordinatorLoadInProgress
                | RDKafkaErrorCode::OperationTimedOut
        )
    }

    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create group probe");
    let deadline = Instant::now() + timeout;
    let mut last_error: Option<KafkaError> = None;
    loop {
        match probe.fetch_group_list(Some(group), Duration::from_secs(10)) {
            Ok(list) => {
                let ready = list.groups().iter().any(|g| {
                    g.name() == group && g.state() == "Stable" && g.members().len() == members
                });
                if ready {
                    return;
                }
            }
            Err(KafkaError::GroupListFetch(code)) if is_transient(code) => {
                last_error = Some(KafkaError::GroupListFetch(code));
            }
            Err(e) => panic!("failed to fetch group list: {e}"),
        }
        assert!(
            Instant::now() < deadline,
            "group {group} did not become stable with {members} member(s) within {timeout:?}; \
             last coordinator error: {last_error:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The group's committed offset on one partition, or `None` when it has
/// never committed there.
fn committed_offset(brokers: &str, group: &str, topic: &str, partition: i32) -> Option<i64> {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    use rdkafka::{Offset, TopicPartitionList};

    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .create()
        .expect("failed to create committed-offset probe");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);
    probe
        .committed_offsets(tpl, Duration::from_secs(10))
        .expect("committed_offsets")
        .find_partition(topic, partition)
        .and_then(|e| match e.offset() {
            Offset::Offset(o) => Some(o),
            _ => None,
        })
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

/// `publish_raw` onto one partition, for topics with more than one.
async fn publish_raw_to(brokers: &str, topic: &str, partition: i32, payload: &[u8]) {
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create raw producer");
    producer
        .send(
            FutureRecord::to(topic)
                .partition(partition)
                .key("k")
                .payload(payload),
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
#[cfg(feature = "test-support")]
shove::define_topic!(
    FreezeTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-freeze").dlq().build()
);
shove::define_topic!(
    SilentRegistryTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-silent").dlq().build()
);
// One topic per decode site a hung registry is driven through below.
shove::define_topic!(
    BatchSilentTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-batch-silent")
        .dlq()
        .build()
);
shove::define_sequenced_topic!(
    FifoSilentTopic,
    Event,
    |msg: &Event| msg.id.to_string(),
    TopologyBuilder::new("kafka-sr-outage-fifo-silent")
        .sequenced(SequenceFailure::Skip)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);
shove::define_topic!(
    BroadcastSilentTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-broadcast-silent")
        .broadcast()
        .build()
);
shove::define_topic!(
    DrainSilentTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-drain-silent")
        .dlq()
        .build()
);
#[cfg(feature = "test-support")]
shove::define_topic!(
    InPlaceSilentTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-inplace-silent")
        .external()
        .hold_queue(Duration::from_millis(200))
        .allow_message_loss()
        .build()
);
#[cfg(feature = "test-support")]
shove::define_topic!(
    BroadcastRedeliverySilentTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-broadcast-redelivery-silent")
        .broadcast()
        .build()
);
shove::define_topic!(
    AssignStallTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-assign").dlq().build()
);
shove::define_topic!(
    BatchRewindPutBackTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-batch-rewind-putback")
        .dlq()
        .build()
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

// An infra-owned topic for a fault reported while the receive loop waits out
// a registry stall on the record behind the one that faults.
#[cfg(feature = "test-support")]
shove::define_topic!(
    StallFaultTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-stall-fault")
        .external()
        .hold_queue(Duration::from_millis(200))
        .allow_message_loss()
        .build()
);
// An infra-owned topic whose handler never settles: every delivery waits in
// place, so a fault the receive loop meets finds handler tasks mid-wait.
shove::define_topic!(
    LoopFaultTopic,
    Event,
    TopologyBuilder::new("kafka-sr-outage-loop-fault")
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
    BroadcastStallTopic,
    SilentRegistryTopic,
    FifoSilentTopic,
    BroadcastSilentTopic
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
macro_rules! defer_once_for {
    ($($topic:ty),+ $(,)?) => {$(
        impl MessageHandler<$topic> for DeferOnceRecorder {
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
    )+};
}

#[cfg(feature = "test-support")]
defer_once_for!(
    InPlaceFaultTopic,
    StallFaultTopic,
    InPlaceSilentTopic,
    BroadcastRedeliverySilentTopic
);

/// Counts its calls and defers every one of them, so each record it is
/// handed cycles through the in-place wait for as long as its task lives.
#[derive(Clone)]
struct DeferForeverRecorder {
    calls: Arc<AtomicUsize>,
}

impl MessageHandler<LoopFaultTopic> for DeferForeverRecorder {
    type Context = ();
    async fn handle(&self, _msg: Event, _meta: MessageMetadata, _: &()) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Defer
    }
}

/// Holds its first delivery on a gate until the test releases it, so a
/// completion can be made to land while the receive loop is stalled.
#[cfg(feature = "test-support")]
#[derive(Clone)]
struct GatedRecorder {
    seen: Arc<Mutex<Vec<u32>>>,
    counter: WaitableCounter,
    gate: Arc<Notify>,
    released: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "test-support")]
impl GatedRecorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
            gate: Arc::new(Notify::new()),
            released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.gate.notify_waiters();
    }
}

#[cfg(feature = "test-support")]
impl MessageHandler<FreezeTopic> for GatedRecorder {
    type Context = ();
    async fn handle(&self, msg: Event, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().unwrap().push(msg.id);
        self.counter.increment();
        loop {
            let notified = self.gate.notified();
            if self.released.load(Ordering::SeqCst) {
                return Outcome::Ack;
            }
            notified.await;
        }
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

macro_rules! dead_recorder_for {
    ($($topic:ty),+ $(,)?) => {$(
        impl MessageHandler<$topic> for DeadRecorder {
            type Context = ();
            async fn handle(&self, _msg: Event, _meta: MessageMetadata, _: &()) -> Outcome {
                Outcome::Ack
            }
            async fn handle_dead(&self, msg: Event, _meta: DeadMessageMetadata, _: &()) {
                self.dead.lock().unwrap().push(msg.id);
                self.counter.increment();
            }
        }
    )+};
}

dead_recorder_for!(DrainStallTopic, DrainSilentTopic);

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

macro_rules! batch_recorder_for {
    ($($topic:ty),+ $(,)?) => {$(
        impl BatchMessageHandler<$topic> for BatchRecorder {
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
    )+};
}

batch_recorder_for!(BatchStallTopic, BatchSilentTopic);

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
    let (registry, status, _) = mock_registry("kafka-sr-outage-stall-value", 503).await;
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
    let (registry, status, _) = mock_registry("kafka-sr-outage-batch-value", 503).await;
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

macro_rules! retry_twice_for {
    ($($topic:ty),+ $(,)?) => {$(
        impl BatchMessageHandler<$topic> for RetryTwiceRecorder {
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
    )+};
}

retry_twice_for!(BatchRetryStallTopic, BatchRewindPutBackTopic);

/// Records every delivery as `(id, partition)`, so a test can tell which
/// member of a group holds which partition.
#[derive(Clone)]
struct PartitionRecorder {
    seen: Arc<Mutex<Vec<(u32, i32)>>>,
    counter: WaitableCounter,
}

impl PartitionRecorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            counter: WaitableCounter::new(),
        }
    }

    fn seen(&self) -> Vec<(u32, i32)> {
        self.seen.lock().unwrap().clone()
    }
}

impl MessageHandler<AssignStallTopic> for PartitionRecorder {
    type Context = ();
    async fn handle(&self, msg: Event, meta: MessageMetadata, _: &()) -> Outcome {
        self.seen
            .lock()
            .unwrap()
            .push((msg.id, meta.partition.expect("Kafka fills the partition")));
        self.counter.increment();
        Outcome::Ack
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
    let (registry, status, _) = mock_registry("kafka-sr-outage-batch-retry-value", 503).await;
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
    let (registry, status, _) = mock_registry("kafka-sr-outage-shutdown-value", 503).await;
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
    let (registry, _status, _) = mock_registry("kafka-sr-outage-auth-value", 401).await;
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
    let (registry, status, _) = mock_registry("kafka-sr-outage-fifo-value", 503).await;
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
    let (registry, status, _) = mock_registry("kafka-sr-outage-broadcast-value", 503).await;
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
    let (registry, status, _) = mock_registry("kafka-sr-outage-drain-dlq-value", 503).await;
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
    let (registry, status, _) = mock_registry("kafka-sr-outage-inplace-fault-value", 200).await;
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

/// A registry deployment fault the receive loop meets itself ends every
/// handler task before `run` returns, as a fault a task reports does: the
/// records ahead of the faulty one were handed to handlers that wait in
/// place, and those waits end with the consumer rather than cycling on in
/// tasks nobody owns. Before, the direct decode path returned the error and
/// skipped the fault arm's drain, so the handler kept being called after the
/// consumer had returned `Err(Topology)` and the busy flag never cleared.
#[tokio::test]
async fn a_fault_met_by_the_receive_loop_ends_every_handler_task() {
    const TOPIC: &str = "kafka-sr-outage-loop-fault";
    const GROUP: &str = "kafka-sr-outage-loop-fault-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, _status, _) = mock_registry("kafka-sr-outage-loop-fault-value", 401).await;
    let client = tb.client().await;
    // Two records that decode, then the one whose schema the registry
    // refuses with a deployment fault.
    for (schema_id, id) in [(HEALTHY_ID, 1u32), (HEALTHY_ID, 2), (FLAKY_ID, 3)] {
        let body = serde_json::to_vec(&Event { id }).unwrap();
        publish_raw(&tb.brokers, TOPIC, &frame_json(schema_id, &body)).await;
    }

    let handler = DeferForeverRecorder {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let h = handler.clone();
    let options = ConsumerOptions::<Kafka>::new()
        .with_prefetch_count(3)
        .with_schema_registry(registry)
        .with_shutdown(CancellationToken::new());
    let processing = options.processing_handle();
    let consumer = KafkaConsumer::new(client.clone());
    let running =
        tokio::spawn(async move { consumer.run::<LoopFaultTopic, _>(h, (), options).await });

    let outcome = tokio::time::timeout(TIMEOUT, running)
        .await
        .expect("the consumer must end on the fault it meets")
        .expect("consumer task panicked");
    assert!(
        matches!(outcome, Err(ShoveError::Topology(_))),
        "expected ShoveError::Topology, got {outcome:?}"
    );
    let at_return = handler.calls.load(Ordering::SeqCst);
    assert!(
        at_return >= 2,
        "both records ahead of the fault reached the handler before it, calls: {at_return}"
    );
    assert!(
        !processing.load(Ordering::Acquire),
        "every handler task ended before the loop returned, so the member reads idle"
    );
    // Ten in-place cycles' worth of time: a task that survived the return
    // would call the handler again within it.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        handler.calls.load(Ordering::SeqCst),
        at_return,
        "no handler task keeps redelivering after the consumer returned"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        3,
        "nothing is committed: every record stays for whoever runs next"
    );
}

/// A fault a handler task reports while the receive loop waits out a registry
/// stall ends the consumer at once. The loop's fault arm sits in a `select!`
/// the stall wait is nested inside, so a fault that landed during the wait
/// was read only when the outage ended, and an outage has no time bound:
/// the wait reads the channel itself now, as the permit wait does.
///
/// The public client caches a resolved schema id for good, so the
/// redelivery's lookup would never reach the registry: the `test-support`
/// seam evicts the id between the first delivery and the redelivery.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_fault_reported_during_a_registry_stall_ends_the_consumer() {
    const TOPIC: &str = "kafka-sr-outage-stall-fault";
    const GROUP: &str = "kafka-sr-outage-stall-fault-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    // `FLAKY_ID` stalls for the whole test; `SECOND_FLAKY_ID` answers until
    // the test flips it to a deployment fault.
    let (registry, _stalled, second, _) =
        mock_registry_with_second_id("kafka-sr-outage-stall-fault-value", 503).await;
    let client = tb.client().await;
    for (schema_id, id) in [(SECOND_FLAKY_ID, 1u32), (FLAKY_ID, 2)] {
        let body = serde_json::to_vec(&Event { id }).unwrap();
        publish_raw(&tb.brokers, TOPIC, &frame_json(schema_id, &body)).await;
    }

    let handler = DeferOnceRecorder {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let h = handler.clone();
    let options = ConsumerOptions::<Kafka>::new()
        .with_prefetch_count(2)
        .with_schema_registry(registry.clone())
        .with_shutdown(CancellationToken::new());
    let processing = options.processing_handle();
    let consumer = KafkaConsumer::new(client.clone());
    let running =
        tokio::spawn(async move { consumer.run::<StallFaultTopic, _>(h, (), options).await });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the first record reached the handler once"
    );
    // The receive loop is waiting out the second record's stall by now. Make
    // the first record's redelivery meet a fault instead of the cache.
    registry.evict_for_test(SchemaId(SECOND_FLAKY_ID));
    second.store(401, Ordering::SeqCst);

    let outcome = tokio::time::timeout(TIMEOUT, running)
        .await
        .expect("the consumer must end on the reported fault, not wait out an outage with no end")
        .expect("consumer task panicked");
    match outcome {
        Err(ShoveError::Topology(message)) => {
            assert!(
                message.contains("schema id 10") && message.contains("deployment fault"),
                "the error names the schema id and the fault: {message}"
            );
        }
        other => panic!("expected ShoveError::Topology, got {other:?}"),
    }
    assert_eq!(
        handler.seen.lock().unwrap().clone(),
        vec![1],
        "one delivery, no redelivery, and the stalled record never reached the handler"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        2,
        "both records stay uncommitted for whoever runs next"
    );
    assert!(
        !processing.load(Ordering::Acquire),
        "every handler task ended before the loop returned"
    );
}

/// The completion channel holds one slot per prefetch permit, and every
/// permit holder is a sender. The receive loop itself is a sender too: a
/// pre-handler discard signals without a permit, and `RegistryStall::wait`
/// never drains. With one permit and one slot, a handler completion that
/// landed during a stall filled the channel, and the stalled record's own
/// verdict was refused: its offset never reached the tracker and lag stayed
/// at one for good. The channel now holds one slot more than the permits.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_completion_during_a_registry_stall_does_not_freeze_the_committed_offset() {
    use shove::kafka::completion_probe;

    const TOPIC: &str = "kafka-sr-outage-freeze";
    const GROUP: &str = "kafka-sr-outage-freeze-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status, hits) = mock_registry("kafka-sr-outage-freeze-value", 503).await;
    let client = tb.client().await;
    for (schema_id, id) in [(HEALTHY_ID, 1u32), (FLAKY_ID, 2)] {
        let body = serde_json::to_vec(&Event { id }).unwrap();
        publish_raw(&tb.brokers, TOPIC, &frame_json(schema_id, &body)).await;
    }

    let handler = GatedRecorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<FreezeTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_concurrent_processing(false)
                    .with_schema_registry(registry)
                    .with_shutdown(sc),
            )
            .await
    });

    // Gate one: record 1 is in its handler, and the loop has asked the
    // registry about record 2 at least once more, so it is stalled.
    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "record 1 delivered"
    );
    let after_first = hits.load(Ordering::SeqCst);
    wait_for_hits_above(&hits, after_first, TIMEOUT).await;

    // Gate two: the handler's Ack completion is queued while the loop is
    // still stalled and cannot drain it.
    let queued_before = completion_probe::queued();
    handler.release();
    let deadline = Instant::now() + TIMEOUT;
    while completion_probe::queued() <= queued_before {
        assert!(
            Instant::now() < deadline,
            "record 1's completion was never queued"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The registry's verdict on record 2 arrives into a channel that already
    // holds record 1's completion.
    status.store(404, Ordering::SeqCst);
    wait_for_lag(&client, TOPIC, GROUP, 0, TIMEOUT).await;
    assert_eq!(
        completion_probe::refused(),
        0,
        "no completion may be refused for a full channel"
    );
    assert_eq!(
        handler.seen.lock().unwrap().clone(),
        vec![1],
        "record 2 never reached the handler"
    );

    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");

    // Record 2 went to the dead-letter topic, the registry's definite answer,
    // with the bytes it arrived with.
    let dead = drain_raw(&tb.brokers, "kafka-sr-outage-freeze-dlq", 1, TIMEOUT).await;
    assert_eq!(
        dead,
        vec![frame_json(
            FLAKY_ID,
            &serde_json::to_vec(&Event { id: 2 }).unwrap()
        )],
        "the dead letter is record 2, framed as it was published"
    );
}

/// A shutdown that lands while a registry lookup is in flight returns at
/// once: the request future is dropped, and the client forgets the fetch
/// nobody waits on any more. Before, the loop waited out the client's own
/// timeout and retries first, about ten seconds with one retry and twenty
/// with the defaults.
#[tokio::test]
async fn shutdown_during_a_registry_lookup_returns_promptly() {
    const TOPIC: &str = "kafka-sr-outage-silent";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;

    // A registry that accepts the connection and never answers.
    let accepted = Arc::new(Notify::new());
    let accepted_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind silent registry");
    let addr = listener.local_addr().expect("silent registry addr");
    {
        let accepted = accepted.clone();
        let accepted_once = accepted_once.clone();
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let (socket, _) = listener.accept().await.expect("accept");
                accepted_once.store(true, Ordering::SeqCst);
                accepted.notify_waiters();
                held.push(socket);
            }
        });
    }
    let registry = SchemaRegistry::builder(format!("http://{addr}"))
        .max_retries(1)
        .build();
    let client = tb.client().await;
    let body = serde_json::to_vec(&Event { id: 3 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(HEALTHY_ID, &body)).await;

    let handler = Recorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<SilentRegistryTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_concurrent_processing(false)
                    .with_schema_registry(registry)
                    .with_shutdown(sc),
            )
            .await
    });

    // Gate: the lookup is in flight. Without it the shutdown arm can win
    // before the decode starts, and the unfixed code passes.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let notified = accepted.notified();
        if accepted_once.load(Ordering::SeqCst) {
            break;
        }
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep_until(deadline) => panic!("the registry was never contacted"),
        }
    }

    let cancelled_at = Instant::now();
    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");
    let took = cancelled_at.elapsed();
    assert!(
        took < Duration::from_secs(3),
        "shutdown must not wait out the registry client's timeout and retries, took {took:?}"
    );
    assert_eq!(
        handler.seen(),
        Vec::<u32>::new(),
        "nothing reached the handler"
    );
}

/// The batch loop's lookup is dropped by a shutdown the same way: the batch
/// in hand is flushed as the shutdown arm would flush it, which is nothing
/// here, and the record whose schema never resolved stays uncommitted.
#[tokio::test]
async fn shutdown_during_a_batch_registry_lookup_returns_promptly() {
    const TOPIC: &str = "kafka-sr-outage-batch-silent";
    const GROUP: &str = "kafka-sr-outage-batch-silent-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, _status, hits) =
        mock_registry("kafka-sr-outage-batch-silent-value", HANG_STATUS).await;
    let client = tb.client().await;
    let body = serde_json::to_vec(&Event { id: 3 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(FLAKY_ID, &body)).await;

    let handler = BatchRecorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<BatchSilentTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(10)
                    .with_max_batch_age(Duration::from_secs(30))
                    .with_schema_registry(registry)
                    .with_shutdown(sc),
            )
            .await
    });

    // Gate: the lookup is in flight.
    wait_for_hits_above(&hits, 0, TIMEOUT).await;
    let cancelled_at = Instant::now();
    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");
    let took = cancelled_at.elapsed();
    assert!(
        took < PROMPT,
        "shutdown must not wait out the registry client's timeout and retries, took {took:?}"
    );
    assert_eq!(
        handler.batches(),
        Vec::<Vec<u32>>::new(),
        "nothing reached the batch handler"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        1,
        "the record whose schema never resolved stays uncommitted"
    );
}

/// The FIFO loop's lookup is dropped by a shutdown the same way.
#[tokio::test]
async fn shutdown_during_a_fifo_registry_lookup_returns_promptly() {
    const TOPIC: &str = "kafka-sr-outage-fifo-silent";
    const GROUP: &str = "kafka-sr-outage-fifo-silent-fifo";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, _status, hits) =
        mock_registry("kafka-sr-outage-fifo-silent-value", HANG_STATUS).await;
    let client = tb.client().await;
    let body = serde_json::to_vec(&Event { id: 3 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(FLAKY_ID, &body)).await;

    let handler = Recorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<FifoSilentTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_schema_registry(registry)
                    .with_shutdown(sc),
            )
            .await
    });

    wait_for_hits_above(&hits, 0, TIMEOUT).await;
    let cancelled_at = Instant::now();
    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");
    let took = cancelled_at.elapsed();
    assert!(
        took < PROMPT,
        "shutdown must not wait out the registry client's timeout and retries, took {took:?}"
    );
    assert_eq!(
        handler.seen(),
        Vec::<u32>::new(),
        "nothing reached the handler"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        1,
        "the record whose schema never resolved stays uncommitted"
    );
}

/// The broadcast loop's lookup is dropped by a shutdown the same way. The
/// subscription starts at the head so the record published before it exists
/// is what it reads.
#[tokio::test]
async fn shutdown_during_a_broadcast_registry_lookup_returns_promptly() {
    const TOPIC: &str = "kafka-sr-outage-broadcast-silent";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, _status, hits) =
        mock_registry("kafka-sr-outage-broadcast-silent-value", HANG_STATUS).await;
    let body = serde_json::to_vec(&Event { id: 3 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(FLAKY_ID, &body)).await;

    let broker = Broker::<Kafka>::from_client(tb.client().await);
    let handler = Recorder::new();
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<BroadcastSilentTopic, _>(
            handler.clone(),
            ConsumerOptions::new()
                .with_schema_registry(registry)
                .with_broadcast_start(BroadcastStart::Head),
        )
        .expect("failed to subscribe");

    wait_for_hits_above(&hits, 0, TIMEOUT).await;
    let cancelled_at = Instant::now();
    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending(), Duration::from_secs(10))
        .await;
    let took = cancelled_at.elapsed();
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(
        took < PROMPT,
        "shutdown must not wait out the registry client's timeout and retries, took {took:?}"
    );
    assert_eq!(
        handler.seen(),
        Vec::<u32>::new(),
        "nothing reached the handler"
    );
}

/// The DLQ drain's lookup is dropped by its client's shutdown the same way,
/// and the dead message it never decoded stays uncommitted.
#[tokio::test]
async fn shutdown_during_a_dlq_registry_lookup_returns_promptly() {
    const DLQ: &str = "kafka-sr-outage-drain-silent-dlq";
    const GROUP: &str = "kafka-sr-outage-drain-silent-dlq-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, DLQ).await;
    let (registry, _status, hits) =
        mock_registry("kafka-sr-outage-drain-silent-dlq-value", HANG_STATUS).await;
    let client = tb.client().await;
    let body = serde_json::to_vec(&Event { id: 3 }).unwrap();
    publish_raw(&tb.brokers, DLQ, &frame_json(FLAKY_ID, &body)).await;

    let handler = DeadRecorder::new();
    let h = handler.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_dlq_with_options::<DrainSilentTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new().with_schema_registry(registry),
            )
            .await
    });

    wait_for_hits_above(&hits, 0, TIMEOUT).await;
    let cancelled_at = Instant::now();
    // The DLQ loop stops on its client's shutdown token.
    client.shutdown_token().cancel();
    handle
        .await
        .expect("drain task panicked")
        .expect("drain ended cleanly");
    let took = cancelled_at.elapsed();
    assert!(
        took < PROMPT,
        "shutdown must not wait out the registry client's timeout and retries, took {took:?}"
    );
    assert_eq!(
        handler.dead(),
        Vec::<u32>::new(),
        "nothing reached handle_dead"
    );
    assert_eq!(
        committed_offset(&tb.brokers, GROUP, DLQ, 0),
        None,
        "the dead message whose schema never resolved stays uncommitted"
    );
}

/// An in-place redelivery decodes the retained bytes again inside the
/// handler's task; a shutdown that lands inside that lookup ends the task at
/// once, and the record stays uncommitted for a restart. The `test-support`
/// seam evicts the cached id so the redelivery reaches the registry, which
/// hangs by then.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn shutdown_during_an_in_place_redelivery_lookup_returns_promptly() {
    const TOPIC: &str = "kafka-sr-outage-inplace-silent";
    const GROUP: &str = "kafka-sr-outage-inplace-silent-consumer";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status, hits) = mock_registry("kafka-sr-outage-inplace-silent-value", 200).await;
    let client = tb.client().await;
    let body = serde_json::to_vec(&Event { id: 5 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(FLAKY_ID, &body)).await;

    let handler = DeferOnceRecorder {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let task_registry = registry.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<InPlaceSilentTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_schema_registry(task_registry)
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the record reached the handler once"
    );
    // The redelivery's lookup hangs: forget the id and silence the mock
    // before the in-place wait ends.
    let before = hits.load(Ordering::SeqCst);
    registry.evict_for_test(SchemaId(FLAKY_ID));
    status.store(HANG_STATUS, Ordering::SeqCst);
    wait_for_hits_above(&hits, before, TIMEOUT).await;

    let cancelled_at = Instant::now();
    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");
    let took = cancelled_at.elapsed();
    assert!(
        took < PROMPT,
        "shutdown must not wait out the registry client's timeout and retries, took {took:?}"
    );
    assert_eq!(
        handler.seen.lock().unwrap().clone(),
        vec![5],
        "one delivery, and the redelivery never reached the handler"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        1,
        "the record stays uncommitted for a restart"
    );
}

/// A broadcast subscription redelivers a deferred message inside its handler
/// task, decoding the retained bytes again; a shutdown inside that lookup
/// ends the task at once.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn shutdown_during_a_broadcast_redelivery_lookup_returns_promptly() {
    const TOPIC: &str = "kafka-sr-outage-broadcast-redelivery-silent";
    let tb = TestBroker::start().await;
    create_single_partition_topic(&tb.brokers, TOPIC).await;
    let (registry, status, hits) =
        mock_registry("kafka-sr-outage-broadcast-redelivery-silent-value", 200).await;
    let body = serde_json::to_vec(&Event { id: 5 }).unwrap();
    publish_raw(&tb.brokers, TOPIC, &frame_json(FLAKY_ID, &body)).await;

    let broker = Broker::<Kafka>::from_client(tb.client().await);
    let handler = DeferOnceRecorder {
        seen: Arc::new(Mutex::new(Vec::new())),
        counter: WaitableCounter::new(),
    };
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<BroadcastRedeliverySilentTopic, _>(
            handler.clone(),
            ConsumerOptions::new()
                .with_schema_registry(registry.clone())
                .with_broadcast_start(BroadcastStart::Head),
        )
        .expect("failed to subscribe");

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the message reached the handler once"
    );
    let before = hits.load(Ordering::SeqCst);
    registry.evict_for_test(SchemaId(FLAKY_ID));
    status.store(HANG_STATUS, Ordering::SeqCst);
    wait_for_hits_above(&hits, before, TIMEOUT).await;

    let cancelled_at = Instant::now();
    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending(), Duration::from_secs(10))
        .await;
    let took = cancelled_at.elapsed();
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(
        took < PROMPT,
        "shutdown must not wait out the registry client's timeout and retries, took {took:?}"
    );
    assert_eq!(
        handler.seen.lock().unwrap().clone(),
        vec![5],
        "one delivery, and the redelivery never reached the handler"
    );
}

/// A partition the group assigns to a member during a registry stall may
/// deliver a record into the stall's `recv()`. That record is put back and
/// the pause widened, so it arrives again, in order, once the wait ends;
/// without the put-back the consumed position has moved past it, a resume
/// does not refetch it, and the record is skipped for good.
#[tokio::test]
async fn a_partition_assigned_during_a_registry_stall_is_put_back_and_delivered_after_it() {
    const TOPIC: &str = "kafka-sr-outage-assign";
    const GROUP: &str = "kafka-sr-outage-assign-consumer";
    let tb = TestBroker::start().await;
    create_topic(&tb.brokers, TOPIC, 2).await;
    let (registry, status, hits) = mock_registry("kafka-sr-outage-assign-value", 200).await;
    let client = tb.client().await;
    let body = |id: u32| serde_json::to_vec(&Event { id }).unwrap();

    // Two members of one group, one partition each.
    let mut members = Vec::new();
    for _ in 0..2 {
        let recorder = PartitionRecorder::new();
        let shutdown = CancellationToken::new();
        let (h, sc, reg) = (recorder.clone(), shutdown.clone(), registry.clone());
        let consumer = KafkaConsumer::new(tb.client().await);
        let handle = tokio::spawn(async move {
            consumer
                .run::<AssignStallTopic, _>(
                    h,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_concurrent_processing(false)
                        .with_schema_registry(reg)
                        .with_shutdown(sc),
                )
                .await
        });
        members.push((recorder, shutdown, handle));
    }
    wait_for_group_members(&tb.brokers, GROUP, 2, TIMEOUT).await;

    // One healthy record per partition says which member holds partition 0.
    for partition in 0..2i32 {
        publish_raw_to(
            &tb.brokers,
            TOPIC,
            partition,
            &frame_json(HEALTHY_ID, &body(10 + partition as u32)),
        )
        .await;
    }
    let deadline = Instant::now() + TIMEOUT;
    while members
        .iter()
        .map(|(r, _, _)| r.seen().len())
        .sum::<usize>()
        < 2
    {
        assert!(
            Instant::now() < deadline,
            "both healthy records must be delivered"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let holds_zero = members
        .iter()
        .position(|(r, _, _)| r.seen().iter().any(|(_, p)| *p == 0))
        .expect("one member holds partition 0");
    let other = 1 - holds_zero;
    let after_healthy = hits.load(Ordering::SeqCst);

    // The member on partition 0 stalls on a record the registry cannot answer for.
    status.store(503, Ordering::SeqCst);
    publish_raw_to(&tb.brokers, TOPIC, 0, &frame_json(FLAKY_ID, &body(1))).await;
    wait_for_hits_above(&hits, after_healthy, TIMEOUT).await;

    // The other member leaves, so the group hands partition 1 to the stalled
    // member during its wait, and a record lands on it.
    members[other].1.cancel();
    (&mut members[other].2)
        .await
        .expect("member task panicked")
        .expect("the leaving member ends cleanly");
    wait_for_group_members(&tb.brokers, GROUP, 1, TIMEOUT).await;
    publish_raw_to(&tb.brokers, TOPIC, 1, &frame_json(HEALTHY_ID, &body(2))).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        members[holds_zero].0.seen().len(),
        1,
        "nothing new is handled while the member waits on the registry"
    );
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        2,
        "the stalled record and the put-back record stay uncommitted"
    );

    status.store(200, Ordering::SeqCst);
    assert!(
        members[holds_zero].0.counter.wait_for(3, TIMEOUT).await,
        "the stalled record and then the put-back record are delivered"
    );
    let seen = members[holds_zero].0.seen();
    assert_eq!(
        &seen[1..],
        &[(1, 0), (2, 1)],
        "the stalled record first, then the record from the partition assigned during the wait"
    );
    wait_for_lag(&client, TOPIC, GROUP, 0, TIMEOUT).await;

    members[holds_zero].1.cancel();
    (&mut members[holds_zero].2)
        .await
        .expect("member task panicked")
        .expect("the stalled member ends cleanly");
}

/// On the batch path a `Retry` ahead of a stall rewinds the flushed span.
/// A stalled record on a partition outside that span must be put back too:
/// otherwise the copy in hand is dropped at the rewound check and, with its
/// partition never sought back, the record is skipped for good.
#[tokio::test]
async fn a_stalled_record_on_another_partition_is_put_back_behind_a_rewound_span() {
    const TOPIC: &str = "kafka-sr-outage-batch-rewind-putback";
    const GROUP: &str = "kafka-sr-outage-batch-rewind-putback-consumer";
    let tb = TestBroker::start().await;
    create_topic(&tb.brokers, TOPIC, 2).await;
    let (registry, status, hits) =
        mock_registry("kafka-sr-outage-batch-rewind-putback-value", 503).await;
    let client = tb.client().await;
    let body = |id: u32| serde_json::to_vec(&Event { id }).unwrap();
    // Two healthy records under two schema ids on partition 0, so the barrier
    // below can count both resolutions.
    publish_raw_to(&tb.brokers, TOPIC, 0, &frame_json(HEALTHY_ID, &body(1))).await;
    publish_raw_to(&tb.brokers, TOPIC, 0, &frame_json(HEALTHY_ID + 1, &body(2))).await;

    let handler = RetryTwiceRecorder::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<BatchRewindPutBackTopic, _>(
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

    // Barrier: both healthy ids resolved, two requests each, so records 1
    // and 2 are decoded and buffered before the stalled record arrives.
    // Without it record 3 can arrive first into an empty buffer, and neither
    // the flush nor the put-back runs.
    wait_for_hits_above(&hits, 3, TIMEOUT).await;
    publish_raw_to(&tb.brokers, TOPIC, 1, &frame_json(FLAKY_ID, &body(3))).await;

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "the records ahead of the stalled one flush first"
    );
    assert_eq!(handler.batches.lock().unwrap().clone(), vec![vec![1, 2]]);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        lag(&client, TOPIC, GROUP).await,
        3,
        "nothing is committed during the stall"
    );

    status.store(200, Ordering::SeqCst);
    assert!(
        handler.counter.wait_for(3, TIMEOUT).await,
        "the rewound span and the put-back record are redelivered, twice"
    );
    wait_for_lag(&client, TOPIC, GROUP, 0, TIMEOUT).await;
    shutdown.cancel();
    handle
        .await
        .expect("consumer task panicked")
        .expect("consumer ended cleanly");

    let batches = handler.batches.lock().unwrap().clone();
    assert_eq!(
        batches[0],
        vec![1, 2],
        "the first flush is the span ahead of the stall"
    );
    assert_eq!(batches.len(), 3, "two redeliveries follow");
    for batch in &batches[1..] {
        let mut sorted = batch.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            vec![1, 2, 3],
            "every later batch holds the rewound records and the put-back record: {batches:?}"
        );
    }
    let mut acked = handler.acked.lock().unwrap().clone();
    acked.sort_unstable();
    assert_eq!(acked, vec![1, 2, 3]);
    assert_eq!(committed_offset(&tb.brokers, GROUP, TOPIC, 0), Some(2));
    assert_eq!(committed_offset(&tb.brokers, GROUP, TOPIC, 1), Some(1));
}
