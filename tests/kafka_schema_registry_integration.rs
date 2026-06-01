//! End-to-end Kafka schema-registry decode tests against REAL infrastructure.
//!
//! HYBRID SETUP (Option C): these tests run a REAL Kafka broker (the
//! `testcontainers_modules::kafka::apache` container) for produce/consume, and
//! an in-process axum mock for the two registry GET endpoints
//! (`/schemas/ids/{id}/versions` + `/schemas/ids/{id}`). Rationale: the
//! preferred Option A (a `redpanda` testcontainers module that bundles a
//! Confluent-compatible Schema Registry) is NOT available in
//! `testcontainers-modules` 0.15 — that crate ships no `redpanda` module — so a
//! single-container real-SR setup is not achievable here. The hybrid still
//! exercises the full real consumer decode path: Confluent frame parse ->
//! resolve over HTTP -> subject gate -> JsonCodec decode -> handler/DLQ. Only
//! the registry's two read endpoints are served by the in-process mock; nothing
//! else is mocked.

#![cfg(feature = "kafka-schema-registry")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use axum::{Json, Router, extract::Path, extract::State, routing::get};
use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::message::{Headers as _, OwnedHeaders};
use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::schema_registry::{SchemaEnforcement, SchemaRegistry};
use shove::topic::Topic as _;
use shove::topology::TopologyBuilder;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// WaitableCounter (mirrors tests/kafka_integration.rs)
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
        self.count.fetch_add(1, Ordering::Relaxed);
        self.signal.notify_waiters();
    }

    fn get(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    async fn wait_for(&self, target: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.get() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.get() >= target;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Message type + topic (JsonCodec, with a DLQ so enforcement can route to it)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderEvent {
    id: String,
    qty: u64,
}

shove::define_topic!(
    OrderTopic,
    OrderEvent,
    TopologyBuilder::new("kafka-sr-orders").dlq().build()
);

// ---------------------------------------------------------------------------
// Capturing handler
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CaptureHandler {
    counter: WaitableCounter,
    received: Arc<Mutex<Vec<OrderEvent>>>,
}

impl CaptureHandler {
    fn new() -> Self {
        Self {
            counter: WaitableCounter::new(),
            received: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn messages(&self) -> Vec<OrderEvent> {
        self.received.lock().await.clone()
    }
}

impl MessageHandler<OrderTopic> for CaptureHandler {
    type Context = ();
    async fn handle(&self, msg: OrderEvent, _meta: MessageMetadata, _: &()) -> Outcome {
        self.received.lock().await.push(msg);
        self.counter.increment();
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Real Kafka harness
// ---------------------------------------------------------------------------

struct TestBroker {
    _container: testcontainers::ContainerAsync<KafkaContainer>,
    client: KafkaClient,
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
        let bootstrap_servers = format!("127.0.0.1:{port}");

        let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap_servers), 10)
            .await
            .expect("failed to connect to Kafka");

        Self {
            _container: container,
            client,
        }
    }

    fn broker(&self) -> Broker<Kafka> {
        Broker::<Kafka>::from_client(self.client.clone())
    }

    fn client(&self) -> KafkaClient {
        self.client.clone()
    }
}

// ---------------------------------------------------------------------------
// In-process mock registry (parameterized subject)
// ---------------------------------------------------------------------------

/// Spawn a mock registry that resolves every id to `subject`. Returns base_url.
async fn spawn_mock(subject: &str) -> String {
    spawn_mock_with_type(subject, "JSON").await
}

/// Spawn a mock registry that resolves every id to `subject` and reports the
/// given `schema_type` string (e.g. `"JSON"` or `"PROTOBUF"`). Returns base_url.
async fn spawn_mock_with_type(subject: &str, schema_type: &'static str) -> String {
    #[derive(Clone)]
    struct TypedMockState {
        subject: Arc<str>,
        schema_type: &'static str,
    }

    async fn versions_typed(
        State(s): State<TypedMockState>,
        Path(_id): Path<u32>,
    ) -> Json<serde_json::Value> {
        Json(serde_json::json!([{ "subject": s.subject.as_ref(), "version": 1 }]))
    }

    async fn schema_typed(
        State(s): State<TypedMockState>,
        Path(_id): Path<u32>,
    ) -> Json<serde_json::Value> {
        Json(serde_json::json!({ "schema": "{}", "schemaType": s.schema_type }))
    }

    let state = TypedMockState {
        subject: Arc::from(subject),
        schema_type,
    };
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(versions_typed))
        .route("/schemas/ids/{id}", get(schema_typed))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

// ---------------------------------------------------------------------------
// Protobuf message type + topic (gated on `protobuf` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "protobuf")]
#[derive(Clone, PartialEq, ::prost::Message)]
struct OrderProto {
    #[prost(string, tag = "1")]
    id: String,
    #[prost(uint64, tag = "2")]
    qty: u64,
}

#[cfg(feature = "protobuf")]
shove::define_topic!(
    OrderProtoTopic,
    OrderProto,
    TopologyBuilder::new("kafka-sr-orders-proto").dlq().build(),
    codec = shove::ProtobufCodec
);

// ---------------------------------------------------------------------------
// Capturing handler for the proto topic
// ---------------------------------------------------------------------------

#[cfg(feature = "protobuf")]
#[derive(Clone)]
struct CaptureProtoHandler {
    counter: WaitableCounter,
    received: Arc<Mutex<Vec<OrderProto>>>,
}

#[cfg(feature = "protobuf")]
impl CaptureProtoHandler {
    fn new() -> Self {
        Self {
            counter: WaitableCounter::new(),
            received: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn messages(&self) -> Vec<OrderProto> {
        self.received.lock().await.clone()
    }
}

#[cfg(feature = "protobuf")]
impl MessageHandler<OrderProtoTopic> for CaptureProtoHandler {
    type Context = ();
    async fn handle(&self, msg: OrderProto, _meta: MessageMetadata, _: &()) -> Outcome {
        self.received.lock().await.push(msg);
        self.counter.increment();
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Protobuf framing helpers
// ---------------------------------------------------------------------------

/// Build a Confluent Protobuf wire frame:
/// `0x00` magic + 4-byte BE schema id + message-index bytes + proto payload.
#[cfg(feature = "protobuf")]
fn frame_protobuf(schema_id: u32, message_index_bytes: &[u8], proto_bytes: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + message_index_bytes.len() + proto_bytes.len());
    bytes.push(0x00);
    bytes.extend_from_slice(&schema_id.to_be_bytes());
    bytes.extend_from_slice(message_index_bytes);
    bytes.extend_from_slice(proto_bytes);
    bytes
}

/// Produce a registry-framed Protobuf message to `topic` on the real broker.
#[cfg(feature = "protobuf")]
async fn produce_framed_protobuf(
    client: &KafkaClient,
    topic: &str,
    schema_id: u32,
    message_index_bytes: &[u8],
    msg: &OrderProto,
) {
    use prost::Message as _;
    let proto_bytes = msg.encode_to_vec();
    let framed = frame_protobuf(schema_id, message_index_bytes, &proto_bytes);
    client
        .publish_with_retry(
            topic,
            None,
            OwnedHeaders::new(),
            &framed,
            5,
            "produce framed protobuf test message",
        )
        .await
        .expect("framed protobuf publish should succeed");
}

// ---------------------------------------------------------------------------
// Framing helper
// ---------------------------------------------------------------------------

/// Build a Confluent JSON wire frame: `0x00` magic + 4-byte BE schema id +
/// JSON payload.
fn frame_json(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + payload.len());
    bytes.push(0x00);
    bytes.extend_from_slice(&schema_id.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

/// Produce a registry-framed JSON message to `topic` on the real broker.
async fn produce_framed(client: &KafkaClient, topic: &str, schema_id: u32, event: &OrderEvent) {
    let payload = serde_json::to_vec(event).expect("serialize OrderEvent");
    let framed = frame_json(schema_id, &payload);
    client
        .publish_with_retry(
            topic,
            None,
            OwnedHeaders::new(),
            &framed,
            5,
            "produce framed test message",
        )
        .await
        .expect("framed publish should succeed");
}

/// Read the first message off the DLQ topic, returning (payload, death-reason).
/// Returns `None` if nothing arrives within the deadline.
async fn read_dlq(client: &KafkaClient, dlq_topic: &str) -> Option<(Vec<u8>, Option<String>)> {
    let brokers = client.brokers().to_string();
    let dlq_topic = dlq_topic.to_string();
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "test-sr-dlq-verify")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("DLQ verify consumer");
        consumer
            .subscribe(&[&dlq_topic])
            .expect("subscribe to DLQ topic");

        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if std::time::Instant::now() > deadline {
                return None;
            }
            if let Some(result) = consumer.poll(Duration::from_secs(1)) {
                let msg = result.expect("DLQ message");
                let payload = msg.payload().unwrap_or_default().to_vec();
                let reason = msg.headers().and_then(|hdrs| {
                    (0..hdrs.count()).find_map(|i| {
                        let h = hdrs.get(i);
                        if h.key == "Shove-Death-Reason" {
                            h.value.map(|v| String::from_utf8_lossy(v).into_owned())
                        } else {
                            None
                        }
                    })
                });
                return Some((payload, reason));
            }
        }
    })
    .await
    .expect("spawn_blocking join")
}

// ===========================================================================
// Tests
// ===========================================================================

/// Happy path: schema resolves to the topic's default accepted subject
/// (`{queue}-value`), the framed JSON is decoded, and the handler receives it.
#[tokio::test]
async fn consumer_decodes_registry_framed_json() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<OrderTopic>().await.unwrap();

    let queue = OrderTopic::topology().queue();
    let accepted_subject = format!("{queue}-value");
    let registry_url = spawn_mock(&accepted_subject).await;
    let registry = SchemaRegistry::builder(registry_url).build();

    let event = OrderEvent {
        id: "order-1".into(),
        qty: 7,
    };
    // Schema id 42 — the mock resolves any id to `accepted_subject`.
    produce_framed(&client, queue, 42, &event).await;

    let handler = CaptureHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<OrderTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1)
                    .with_schema_registry(registry),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "handler should receive the decoded message"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let received = handler.messages().await;
    assert_eq!(received, vec![event], "decoded payload must match");
    broker.close().await;
}

/// Enforce (default): the resolved subject is NOT in the accepted set, so the
/// message is routed to the DLQ with reason `schema_validation_failed` and the
/// handler is never called.
#[tokio::test]
async fn enforce_routes_wrong_subject_to_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<OrderTopic>().await.unwrap();

    let queue = OrderTopic::topology().queue();
    let accepted_subject = format!("{queue}-value");

    // Registry resolves the id to a DIFFERENT subject, not in the accepted set.
    let registry_url = spawn_mock("other-value").await;
    let registry = SchemaRegistry::builder(registry_url).build();

    let event = OrderEvent {
        id: "order-2".into(),
        qty: 13,
    };
    produce_framed(&client, queue, 99, &event).await;

    let handler = CaptureHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<OrderTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1)
                    .with_schema_registry(registry)
                    // Default enforcement is Enforce; set the accepted set explicitly.
                    .accept_schema_subjects([accepted_subject.clone()]),
            )
            .await
    });

    // The message must land on the DLQ with the schema-validation reason.
    let dlq_topic = OrderTopic::topology().dlq().expect("OrderTopic has a DLQ");
    let dlq = read_dlq(&client, dlq_topic).await;

    shutdown.cancel();
    broker.close().await;
    handle.await.unwrap().ok();

    let (payload, reason) = dlq.expect("rejected message should land in DLQ");
    // The gate rejects BEFORE decoding, so the DLQ carries the ORIGINAL framed
    // bytes (0x00 magic + 4-byte BE id + JSON), not the stripped inner payload.
    assert_eq!(
        payload.first(),
        Some(&0x00),
        "DLQ payload retains the frame"
    );
    assert_eq!(
        u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]),
        99,
        "DLQ frame retains the original schema id"
    );
    let decoded: OrderEvent = serde_json::from_slice(&payload[5..])
        .expect("inner payload (after the 5-byte frame) is the original JSON");
    assert_eq!(
        decoded, event,
        "DLQ should carry the original event payload"
    );
    assert_eq!(
        reason.as_deref(),
        Some("schema_validation_failed"),
        "DLQ death reason must be schema_validation_failed"
    );
    assert_eq!(
        handler.counter.get(),
        0,
        "handler must NOT receive a subject-rejected message under Enforce"
    );
}

/// Permissive: the resolved subject is NOT in the accepted set, but enforcement
/// is `Permissive`, so the message is decoded and delivered to the handler
/// (a warning is logged on this path; we assert delivery, not the log).
#[tokio::test]
async fn permissive_decodes_wrong_subject() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<OrderTopic>().await.unwrap();

    let queue = OrderTopic::topology().queue();
    let accepted_subject = format!("{queue}-value");

    // Same wrong-subject setup as the Enforce test.
    let registry_url = spawn_mock("other-value").await;
    let registry = SchemaRegistry::builder(registry_url).build();

    let event = OrderEvent {
        id: "order-3".into(),
        qty: 21,
    };
    produce_framed(&client, queue, 7, &event).await;

    let handler = CaptureHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<OrderTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(1)
                    .with_schema_registry(registry)
                    .with_schema_enforcement(SchemaEnforcement::Permissive)
                    .accept_schema_subjects([accepted_subject.clone()]),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(1, TIMEOUT).await,
        "Permissive must decode and deliver even a non-accepted subject"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let received = handler.messages().await;
    assert_eq!(
        received,
        vec![event],
        "Permissive should deliver the decoded payload, not DLQ it"
    );
    broker.close().await;
}

/// Happy path (Protobuf): two messages are produced — one with the single-byte
/// message-index optimisation `[0x00]` and one with the explicit encoding
/// `[0x01, 0x00]` (count=1, index=0). Both must be decoded to the expected
/// `OrderProto` field values. This locks the message-index skip at the
/// integration seam, complementing the unit tests in `wire.rs`.
#[cfg(feature = "protobuf")]
#[tokio::test]
async fn consumer_decodes_registry_framed_protobuf() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<OrderProtoTopic>()
        .await
        .unwrap();

    let queue = OrderProtoTopic::topology().queue();
    let accepted_subject = format!("{queue}-value");
    // Mock reports PROTOBUF for realism; the consumer derives the WireFormat
    // from the topic codec's NAME, not from the registry's schemaType.
    let registry_url = spawn_mock_with_type(&accepted_subject, "PROTOBUF").await;
    let registry = SchemaRegistry::builder(registry_url).build();

    let msg_a = OrderProto {
        id: "proto-1".into(),
        qty: 10,
    };
    let msg_b = OrderProto {
        id: "proto-2".into(),
        qty: 20,
    };

    // Message A: single-byte message-index optimisation [0x00] (count=0 means [0]).
    produce_framed_protobuf(&client, queue, 55, &[0x00], &msg_a).await;
    // Message B: explicit encoding [0x01, 0x00] (count=1, index=0).
    produce_framed_protobuf(&client, queue, 55, &[0x01, 0x00], &msg_b).await;

    let handler = CaptureProtoHandler::new();
    let hc = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();

    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<OrderProtoTopic, _>(
                hc,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(2)
                    .with_schema_registry(registry),
            )
            .await
    });

    assert!(
        handler.counter.wait_for(2, TIMEOUT).await,
        "handler should receive both decoded protobuf messages"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let received = handler.messages().await;
    assert_eq!(received.len(), 2, "exactly two messages must be decoded");
    assert_eq!(received[0].id, "proto-1", "first message id must match");
    assert_eq!(received[0].qty, 10, "first message qty must match");
    assert_eq!(received[1].id, "proto-2", "second message id must match");
    assert_eq!(received[1].qty, 20, "second message qty must match");
    broker.close().await;
}
