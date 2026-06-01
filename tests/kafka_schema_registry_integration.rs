//! End-to-end Kafka schema-registry decode tests against a REAL two-container
//! Confluent stack: `confluentinc/cp-kafka` (KRaft) + `confluentinc/cp-schema-registry`.
//!
//! Unlike the in-process axum mock in `kafka_schema_registry.rs` (which asserts
//! exact HTTP call counts for single-flight / negative-cache and therefore must
//! keep mocking the registry), this file exercises the full decode path against
//! the GENUINE Confluent Schema Registry: schemas are registered over the real
//! SR REST API, framed messages are produced to the real broker, and the shove
//! consumer resolves ids by calling the real SR, then runs the subject gate /
//! codec decode -> handler/DLQ.
//!
//! ## Two-container wiring
//!
//! `cp-schema-registry` persists schemas in a Kafka topic, so it must reach the
//! broker over a shared docker network, while the test client reaches the broker
//! over the host. We put both containers on a user-defined network (referenced
//! by name via `ImageExt::with_network`; testcontainers auto-creates it on first
//! use and reuses it for the second container) and give the broker a stable
//! container name so SR can reach it by DNS on that network.
//!
//! The broker runs in KRaft mode with DUAL data listeners. The INTERNAL
//! listener (`sr-cp-broker:9092`) is reachable by SR on the shared network. The
//! EXTERNAL listener (`127.0.0.1:29092`) is reachable from the host.
//!
//! The EXTERNAL listener BINDS on the same container port (29092) it is
//! advertised on, so the fixed host->container mapping `29092->29092` lands on
//! the EXTERNAL listener. rdkafka bootstraps to `127.0.0.1:29092`, reads
//! metadata, then reconnects to the advertised EXTERNAL address, which is thus
//! host-reachable. SR's HTTP 8081 is fixed-mapped to host 18082.
//!
//! ## Parallelism constraint
//!
//! The fixed host ports (Kafka 29092, SR 18082) mean only ONE cp stack can run
//! at a time. To stay safe under the default nextest runner WITHOUT requiring
//! `--test-threads=1`, all scenarios run sequentially inside a SINGLE test fn
//! against ONE shared stack (this also amortises the slow cp-SR JVM startup).
//! The fixed ports are also distinct from the Redpanda suite's (Kafka 19092 /
//! SR 18081) so the two suites don't collide if run concurrently.

#![cfg(feature = "kafka-schema-registry")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

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
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Confluent Platform image tags. Pinned to a current cp release with ARM64
/// support.
const CP_KAFKA_IMAGE: &str = "confluentinc/cp-kafka";
const CP_SCHEMA_REGISTRY_IMAGE: &str = "confluentinc/cp-schema-registry";
const CP_TAG: &str = "7.7.1";

/// Stable network alias / container name for the broker so SR can resolve it by
/// DNS on the shared network.
const BROKER_ALIAS: &str = "sr-cp-broker";

/// Fixed external Kafka listener port (advertised as `127.0.0.1:29092`),
/// distinct from the Redpanda suite (19092) so the two can't collide.
const KAFKA_EXTERNAL_PORT: u16 = 29092;
/// In-container Schema Registry HTTP port.
const SCHEMA_REGISTRY_PORT: u16 = 8081;
/// Fixed host port mapped to the in-container Schema Registry port, distinct
/// from the Redpanda suite (18081).
const SCHEMA_REGISTRY_HOST_PORT: u16 = 18082;

const TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// WaitableCounter (mirrors the other Kafka test files)
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
// Message type + topics (one topic per scenario so consumer groups don't share
// offsets across phases). JsonCodec, each with a DLQ for enforcement routing.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderEvent {
    id: String,
    qty: u64,
}

shove::define_topic!(
    HappyTopic,
    OrderEvent,
    TopologyBuilder::new("cp-sr-happy").dlq().build()
);

shove::define_topic!(
    EnforceTopic,
    OrderEvent,
    TopologyBuilder::new("cp-sr-enforce").dlq().build()
);

shove::define_topic!(
    PermissiveTopic,
    OrderEvent,
    TopologyBuilder::new("cp-sr-permissive").dlq().build()
);

// ---------------------------------------------------------------------------
// Capturing handler (generic over the JSON topics)
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

macro_rules! impl_capture_handler {
    ($topic:ty) => {
        impl MessageHandler<$topic> for CaptureHandler {
            type Context = ();
            async fn handle(&self, msg: OrderEvent, _meta: MessageMetadata, _: &()) -> Outcome {
                self.received.lock().await.push(msg);
                self.counter.increment();
                Outcome::Ack
            }
        }
    };
}

impl_capture_handler!(HappyTopic);
impl_capture_handler!(EnforceTopic);
impl_capture_handler!(PermissiveTopic);

// ---------------------------------------------------------------------------
// Framing + produce helpers
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
            .set("group.id", "test-cp-sr-dlq-verify")
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

// ---------------------------------------------------------------------------
// Real Schema Registry REST: register schemas, return assigned ids.
// ---------------------------------------------------------------------------

/// Register a permissive JSON schema under `subject` against the real cp Schema
/// Registry and return the assigned global schema id. cp-SR validates the
/// JSON-schema syntax, so the body must be a syntactically valid schema.
async fn register_json_schema(sr_base: &str, subject: &str) -> u32 {
    register_schema(
        sr_base,
        subject,
        r#"{"schema":"{\"type\":\"object\"}","schemaType":"JSON"}"#,
    )
    .await
}

/// Register a schema (raw request body) under `subject`, returning its id.
async fn register_schema(sr_base: &str, subject: &str, body: &'static str) -> u32 {
    let url = format!("{sr_base}/subjects/{subject}/versions");
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .body(body)
        .send()
        .await
        .expect("register schema request");
    assert!(
        resp.status().is_success(),
        "schema registration failed: {} {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = resp.json().await.expect("registration response json");
    body["id"].as_u64().expect("registration returns an id") as u32
}

/// Poll the Schema Registry until `GET /subjects` returns 200, or panic. The
/// cp-schema-registry JVM can take 15-30s to come up, so the deadline is
/// generous; on timeout the caller has already had a chance to inspect logs.
async fn wait_for_schema_registry(sr_base: &str) -> bool {
    let url = format!("{sr_base}/subjects");
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Ok(resp) = http.get(&url).send().await
            && resp.status().is_success()
        {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
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
    TopologyBuilder::new("cp-sr-orders-proto").dlq().build(),
    codec = shove::ProtobufCodec
);

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

// ===========================================================================
// The test: one cp-kafka + cp-schema-registry stack, sequential scenarios.
// ===========================================================================

/// Real Confluent end-to-end: register schemas via the real cp Schema Registry,
/// produce framed messages to the real cp-kafka broker, and assert the shove
/// consumer's resolve -> subject-gate -> decode path across all enforcement
/// outcomes (and, under the `protobuf` feature, both message-index encodings).
/// All phases share ONE stack (the fixed external Kafka + SR ports forbid
/// parallel stacks — see the module docs).
#[tokio::test]
async fn cp_real_schema_registry_e2e() {
    // Unique network name per run so repeated runs don't trip over a stale
    // network; testcontainers auto-creates it on first `with_network` use and
    // reuses it for the second container.
    let network = format!("cp-sr-net-{}", std::process::id());

    // -- Broker: cp-kafka in KRaft mode with dual data listeners. The EXTERNAL
    // listener BINDS on container port 29092, is advertised at 127.0.0.1:29092,
    // and is fixed-mapped host->container 29092 so rdkafka's metadata-driven
    // reconnect lands on a host-reachable address; the INTERNAL listener (9092)
    // is advertised under the broker's network alias so SR can reach it over the
    // shared docker network. --
    let _broker_container = GenericImage::new(CP_KAFKA_IMAGE, CP_TAG)
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_network(&network)
        .with_container_name(BROKER_ALIAS)
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!(
                "INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:{KAFKA_EXTERNAL_PORT},CONTROLLER://0.0.0.0:9093"
            ),
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("INTERNAL://{BROKER_ALIAS}:9092,EXTERNAL://127.0.0.1:{KAFKA_EXTERNAL_PORT}"),
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        )
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        // The EXTERNAL advertised port must resolve to a host-reachable port.
        .with_mapped_port(KAFKA_EXTERNAL_PORT, ContainerPort::Tcp(KAFKA_EXTERNAL_PORT))
        .start()
        .await
        .expect("failed to start cp-kafka container");

    // -- Connect from the host over the EXTERNAL listener before bringing up SR
    // (SR needs the broker reachable on the internal listener, which is up by
    // the same point). --
    let bootstrap = format!("127.0.0.1:{KAFKA_EXTERNAL_PORT}");
    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap), 30)
        .await
        .expect("failed to connect to cp-kafka over the external listener");
    let broker = Broker::<Kafka>::from_client(client.clone());

    // -- Schema Registry: same cp version, on the same network, pointed at the
    // broker's INTERNAL listener via its network alias. --
    let sr_container = GenericImage::new(CP_SCHEMA_REGISTRY_IMAGE, CP_TAG)
        .with_wait_for(WaitFor::message_on_stdout(
            "Server started, listening for requests",
        ))
        .with_network(&network)
        .with_env_var("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .with_env_var(
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
            format!("PLAINTEXT://{BROKER_ALIAS}:9092"),
        )
        .with_env_var("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .with_mapped_port(
            SCHEMA_REGISTRY_HOST_PORT,
            ContainerPort::Tcp(SCHEMA_REGISTRY_PORT),
        )
        .start()
        .await
        .expect("failed to start cp-schema-registry container");

    let sr_base = format!("http://127.0.0.1:{SCHEMA_REGISTRY_HOST_PORT}");

    // -- Confirm the SR HTTP surface is actually serving. On timeout, dump the
    // SR container logs to diagnose (likely a broker-connectivity misconfig). --
    if !wait_for_schema_registry(&sr_base).await {
        let stderr = sr_container
            .stderr_to_vec()
            .await
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default();
        let stdout = sr_container
            .stdout_to_vec()
            .await
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default();
        panic!(
            "cp-schema-registry at {sr_base} not ready within 90s.\n\
             ---- SR stdout ----\n{stdout}\n---- SR stderr ----\n{stderr}"
        );
    }

    // =======================================================================
    // Scenario 1 — happy path: schema registered under the topic's default
    // accepted subject `{queue}-value`; default Enforce decodes and delivers.
    // =======================================================================
    {
        broker.topology().declare::<HappyTopic>().await.unwrap();
        let queue = HappyTopic::topology().queue();
        let subject = format!("{queue}-value");
        let schema_id = register_json_schema(&sr_base, &subject).await;

        let event = OrderEvent {
            id: "order-1".into(),
            qty: 7,
        };
        produce_framed(&client, queue, schema_id, &event).await;

        let registry = SchemaRegistry::builder(sr_base.clone()).build();
        let handler = CaptureHandler::new();
        let hc = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();

        let consumer = KafkaConsumer::new(client.clone());
        let handle = tokio::spawn(async move {
            consumer
                .run::<HappyTopic, _>(
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
            "happy path: handler should receive the decoded message"
        );
        shutdown.cancel();
        handle.await.unwrap().ok();

        assert_eq!(
            handler.messages().await,
            vec![event],
            "happy path: decoded payload must match"
        );
    }

    // =======================================================================
    // Scenario 2 — Enforce + wrong subject: schema registered under
    // `other-value`, accepted set is `{queue}-value`; default Enforce routes
    // to DLQ with reason `schema_validation_failed`, handler stays at 0.
    // =======================================================================
    {
        broker.topology().declare::<EnforceTopic>().await.unwrap();
        let queue = EnforceTopic::topology().queue();
        let accepted_subject = format!("{queue}-value");
        // Register under a DIFFERENT subject not in the accepted set.
        let schema_id = register_json_schema(&sr_base, "other-value").await;

        let event = OrderEvent {
            id: "order-2".into(),
            qty: 13,
        };
        produce_framed(&client, queue, schema_id, &event).await;

        let registry = SchemaRegistry::builder(sr_base.clone()).build();
        let handler = CaptureHandler::new();
        let hc = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();

        let consumer = KafkaConsumer::new(client.clone());
        let handle = tokio::spawn(async move {
            consumer
                .run::<EnforceTopic, _>(
                    hc,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_prefetch_count(1)
                        .with_schema_registry(registry)
                        // Default enforcement is Enforce; set the accepted set.
                        .accept_schema_subjects([accepted_subject.clone()]),
                )
                .await
        });

        let dlq_topic = EnforceTopic::topology().dlq().expect("EnforceTopic DLQ");
        let dlq = read_dlq(&client, dlq_topic).await;

        shutdown.cancel();
        handle.await.unwrap().ok();

        let (payload, reason) = dlq.expect("rejected message should land in DLQ");
        // The gate rejects BEFORE decoding, so the DLQ carries the ORIGINAL
        // framed bytes (magic + id + json), not the stripped inner payload.
        assert_eq!(
            payload.first(),
            Some(&0x00),
            "enforce: DLQ payload retains the frame magic byte"
        );
        assert_eq!(
            u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]),
            schema_id,
            "enforce: DLQ frame retains the original schema id"
        );
        let decoded: OrderEvent = serde_json::from_slice(&payload[5..])
            .expect("enforce: inner payload (after the 5-byte frame) is the original JSON");
        assert_eq!(
            decoded, event,
            "enforce: DLQ should carry the original event payload"
        );
        assert_eq!(
            reason.as_deref(),
            Some("schema_validation_failed"),
            "enforce: DLQ death reason must be schema_validation_failed"
        );
        assert_eq!(
            handler.counter.get(),
            0,
            "enforce: handler must NOT receive a subject-rejected message"
        );
    }

    // =======================================================================
    // Scenario 3 — Permissive + wrong subject: same wrong-subject setup as
    // scenario 2, but Permissive enforcement decodes and delivers anyway.
    // =======================================================================
    {
        broker
            .topology()
            .declare::<PermissiveTopic>()
            .await
            .unwrap();
        let queue = PermissiveTopic::topology().queue();
        let accepted_subject = format!("{queue}-value");
        // `other-value` already exists (registered in scenario 2); registering
        // the same schema again is idempotent and returns the same id.
        let schema_id = register_json_schema(&sr_base, "other-value").await;

        let event = OrderEvent {
            id: "order-3".into(),
            qty: 21,
        };
        produce_framed(&client, queue, schema_id, &event).await;

        let registry = SchemaRegistry::builder(sr_base.clone()).build();
        let handler = CaptureHandler::new();
        let hc = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();

        let consumer = KafkaConsumer::new(client.clone());
        let handle = tokio::spawn(async move {
            consumer
                .run::<PermissiveTopic, _>(
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
            "permissive: must decode and deliver even a non-accepted subject"
        );
        shutdown.cancel();
        handle.await.unwrap().ok();

        assert_eq!(
            handler.messages().await,
            vec![event],
            "permissive: should deliver the decoded payload, not DLQ it"
        );
    }

    // =======================================================================
    // Scenario 4 (protobuf feature) — register a real PROTOBUF schema, produce
    // two framed `OrderProto` messages with BOTH message-index encodings
    // (`[0x00]` optimisation and explicit `[0x01,0x00]`), assert both decode.
    // cp-SR validates proto syntax, so the registered schema must be a valid
    // `.proto`; the consumer derives the wire format from the topic CODEC name
    // regardless of the registry's declared schemaType.
    // =======================================================================
    #[cfg(feature = "protobuf")]
    {
        broker
            .topology()
            .declare::<OrderProtoTopic>()
            .await
            .unwrap();
        let queue = OrderProtoTopic::topology().queue();
        let subject = format!("{queue}-value");
        let schema_id = register_schema(
            &sr_base,
            &subject,
            r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message OrderProto { string id = 1; uint64 qty = 2; }"}"#,
        )
        .await;

        let msg_a = OrderProto {
            id: "proto-1".into(),
            qty: 10,
        };
        let msg_b = OrderProto {
            id: "proto-2".into(),
            qty: 20,
        };

        // Message A: single-byte message-index optimisation [0x00].
        produce_framed_protobuf(&client, queue, schema_id, &[0x00], &msg_a).await;
        // Message B: explicit encoding [0x01, 0x00] (count=1, index=0).
        produce_framed_protobuf(&client, queue, schema_id, &[0x01, 0x00], &msg_b).await;

        let registry = SchemaRegistry::builder(sr_base.clone()).build();
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
            "protobuf: handler should receive both decoded protobuf messages"
        );
        shutdown.cancel();
        handle.await.unwrap().ok();

        let received = handler.messages().await;
        assert_eq!(received.len(), 2, "protobuf: exactly two messages decoded");
        assert_eq!(received[0].id, "proto-1", "protobuf: first id must match");
        assert_eq!(received[0].qty, 10, "protobuf: first qty must match");
        assert_eq!(received[1].id, "proto-2", "protobuf: second id must match");
        assert_eq!(received[1].qty, 20, "protobuf: second qty must match");
    }

    broker.close().await;
}
