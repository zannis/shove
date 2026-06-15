//! End-to-end Kafka schema-registry tests against a REAL Redpanda container.
//!
//! Unlike the hybrid setup in `kafka_schema_registry_integration.rs` (real
//! apache-Kafka + an in-process axum mock registry), this file runs ONE
//! Redpanda container that bundles BOTH a real Kafka broker AND a real
//! Confluent-compatible Schema Registry. The full decode path is exercised
//! against genuine infrastructure: schemas are registered over the real SR
//! REST API, framed messages are produced to the real broker, and the shove
//! consumer resolves ids by calling the real SR, then runs the subject gate /
//! `JsonCodec` decode -> handler/DLQ.
//!
//! `testcontainers-modules` 0.15 ships no `redpanda` module, so the container
//! is driven directly via `testcontainers::GenericImage` (+ `ImageExt`,
//! `runners::AsyncRunner`).
//!
//! ## Advertised-listener wiring (Approach 1: two listeners + fixed port)
//!
//! rdkafka bootstraps to `host:port`, reads broker metadata, then reconnects to
//! the broker's ADVERTISED address. That advertised address must be reachable
//! from the host, but with testcontainers' dynamic port mapping the host port
//! is unknown until after start. We sidestep this by giving Redpanda two Kafka
//! listeners — an `internal` one (container-local) and an `external` one
//! advertised as `127.0.0.1:19092` — and fixed-mapping host 19092 ->
//! container 19092. The Schema Registry is plain HTTP (no advertised-address
//! problem), so its 8081 port is mapped dynamically.
//!
//! ## Parallelism constraint
//!
//! The fixed host port 19092 means two of these containers cannot run at once.
//! nextest runs each test fn in its own process, so multiple `#[tokio::test]`
//! fns here would each spin up a container on the same fixed port and collide.
//! To stay safe under the default nextest runner WITHOUT requiring
//! `--test-threads=1`, all three scenarios run sequentially inside a SINGLE
//! test fn against ONE shared container.

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
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumer, KafkaPublisherConfig};
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

/// Image tag pinned to a known-good Redpanda release.
const REDPANDA_IMAGE: &str = "docker.redpanda.com/redpandadata/redpanda";
const REDPANDA_TAG: &str = "v24.2.7";
/// Fixed external Kafka listener port (advertised as `127.0.0.1:19092`).
const KAFKA_EXTERNAL_PORT: u16 = 19092;
/// In-container Schema Registry HTTP port.
const SCHEMA_REGISTRY_PORT: u16 = 8081;
/// Fixed host port mapped to the in-container Schema Registry port. Once any
/// `with_mapped_port` is set, testcontainers stops auto-publishing exposed
/// ports (it disables `publish_all_ports` in favour of explicit bindings), so
/// the SR port must be fixed-mapped too rather than relying on a dynamic map.
const SCHEMA_REGISTRY_HOST_PORT: u16 = 18081;

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
    TopologyBuilder::new("rp-sr-happy").dlq().build()
);

shove::define_topic!(
    EnforceTopic,
    OrderEvent,
    TopologyBuilder::new("rp-sr-enforce").dlq().build()
);

shove::define_topic!(
    PermissiveTopic,
    OrderEvent,
    TopologyBuilder::new("rp-sr-permissive").dlq().build()
);

shove::define_topic!(
    ProducerTopic,
    OrderEvent,
    TopologyBuilder::new("rp-sr-producer").dlq().build()
);

// ---------------------------------------------------------------------------
// Capturing handler (generic over the three topics)
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
impl_capture_handler!(ProducerTopic);

// ---------------------------------------------------------------------------
// Framing + produce helpers (mirror the hybrid test)
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
            .set("group.id", "test-rp-sr-dlq-verify")
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
// Real Schema Registry REST: register a permissive JSON schema, return its id.
// ---------------------------------------------------------------------------

/// Register a permissive JSON schema under `subject` against the real Redpanda
/// Schema Registry and return the assigned global schema id. Redpanda validates
/// the JSON-schema syntax, so the body must be a syntactically valid schema.
async fn register_json_schema(sr_base: &str, subject: &str) -> u32 {
    let url = format!("{sr_base}/subjects/{subject}/versions");
    let resp = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .body(r#"{"schema":"{\"type\":\"object\"}","schemaType":"JSON"}"#)
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

/// Poll the Schema Registry until `GET /subjects` returns 200, or panic.
async fn wait_for_schema_registry(sr_base: &str) {
    let url = format!("{sr_base}/subjects");
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(resp) = http.get(&url).send().await
            && resp.status().is_success()
        {
            return;
        }
        if Instant::now() > deadline {
            panic!("Schema Registry at {sr_base} not ready within 60s");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// ===========================================================================
// The test: one Redpanda container, three sequential scenarios.
// ===========================================================================

/// Real Redpanda end-to-end: register schemas via the real Schema Registry,
/// produce framed messages to the real broker, and assert the shove consumer's
/// resolve -> subject-gate -> decode path against all three enforcement
/// outcomes. All three phases share ONE container (the fixed external Kafka
/// port forbids parallel containers — see the module docs).
#[tokio::test]
async fn redpanda_real_schema_registry_e2e() {
    // -- Start Redpanda with two Kafka listeners (internal + advertised
    // external) and the Schema Registry enabled. --
    let cmd = [
        "redpanda",
        "start",
        "--mode",
        "dev-container",
        "--smp",
        "1",
        "--default-log-level=warn",
        "--kafka-addr",
        "internal://0.0.0.0:9092,external://0.0.0.0:19092",
        "--advertise-kafka-addr",
        "internal://127.0.0.1:9092,external://127.0.0.1:19092",
        "--schema-registry-addr",
        "0.0.0.0:8081",
    ];

    // Bound for its lifetime — dropping the handle stops the container, so it
    // must stay alive until the end of the test (all three phases).
    let _container = GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
        // `--default-log-level=warn` suppresses the INFO "Successfully started
        // Redpanda!" line, so gate on the Admin API listener warning, which is
        // logged at WARN and appears once the broker core is up. The explicit
        // Schema-Registry + Kafka readiness polling after `.start()` then
        // confirms both surfaces are actually accepting requests.
        .with_wait_for(WaitFor::message_on_stderr(
            "Insecure Admin API listener on 0.0.0.0:9644",
        ))
        .with_cmd(cmd)
        // Fixed-map the advertised external Kafka port so the address Redpanda
        // hands back to rdkafka (127.0.0.1:19092) is reachable from the host.
        .with_mapped_port(KAFKA_EXTERNAL_PORT, ContainerPort::Tcp(KAFKA_EXTERNAL_PORT))
        // Fixed-map the Schema Registry port too: with an explicit mapped port
        // present, testcontainers disables `publish_all_ports`, so a merely
        // `with_exposed_port` SR port would never receive a host binding.
        .with_mapped_port(
            SCHEMA_REGISTRY_HOST_PORT,
            ContainerPort::Tcp(SCHEMA_REGISTRY_PORT),
        )
        .start()
        .await
        .expect("failed to start Redpanda container");

    let sr_base = format!("http://127.0.0.1:{SCHEMA_REGISTRY_HOST_PORT}");

    // -- Wait for both surfaces to be ready. --
    wait_for_schema_registry(&sr_base).await;
    let bootstrap = format!("127.0.0.1:{KAFKA_EXTERNAL_PORT}");
    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap), 20)
        .await
        .expect("failed to connect to Redpanda Kafka API");
    let broker = Broker::<Kafka>::from_client(client.clone());

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
    // Scenario 4 — producer-side SR encode round-trip: shove's publisher frames
    // an SR-encoded message (subject `{queue}-value`, default Enforce-compatible)
    // and shove's own SR-decode consumer reads it back. Proves the producer
    // emits Confluent-compatible wire bytes (magic + BE id) AND that the full
    // encode -> broker -> decode path reconstructs the original payload.
    // =======================================================================
    {
        broker.topology().declare::<ProducerTopic>().await.unwrap();
        let queue = ProducerTopic::topology().queue();
        let subject = format!("{queue}-value");
        let schema_id = register_json_schema(&sr_base, &subject).await;

        let event = OrderEvent {
            id: "order-4".into(),
            qty: 99,
        };

        // Publish through shove's publisher with producer-side SR framing.
        let registry = SchemaRegistry::builder(sr_base.clone()).build();
        let publisher = broker
            .publisher_with(KafkaPublisherConfig::new().with_schema_registry(registry.clone()))
            .await
            .expect("build SR publisher");
        publisher
            .publish::<ProducerTopic>(&event)
            .await
            .expect("SR-framed publish should succeed");

        // Raw-read the produced bytes off the broker (independent group) and
        // assert they carry the Confluent frame: magic 0x00 + the registered id.
        let (raw, _reason) = read_dlq(&client, queue)
            .await
            .expect("produced message should be on the topic");
        assert_eq!(
            raw.first(),
            Some(&0x00),
            "producer: emitted payload must start with the Confluent magic byte"
        );
        assert_eq!(
            u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]),
            schema_id,
            "producer: emitted frame must carry the latest registered schema id"
        );

        // Decode it back through shove's consumer SR path.
        let handler = CaptureHandler::new();
        let hc = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(client.clone());
        let handle = tokio::spawn(async move {
            consumer
                .run::<ProducerTopic, _>(
                    hc,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_prefetch_count(1)
                        .with_schema_registry(registry)
                        .accept_schema_subjects([subject.clone()]),
                )
                .await
        });

        assert!(
            handler.counter.wait_for(1, TIMEOUT).await,
            "producer round-trip: consumer should decode the self-published message"
        );
        shutdown.cancel();
        handle.await.unwrap().ok();

        assert_eq!(
            handler.messages().await,
            vec![event],
            "producer round-trip: decoded payload must match what was published"
        );
    }

    broker.close().await;
}
