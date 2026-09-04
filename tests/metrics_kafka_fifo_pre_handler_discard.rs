#![cfg(all(feature = "kafka", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: Kafka's **sequenced (FIFO)** consumer must count a
//! message dropped *before the handler* — oversize, or undecodable — in
//! `shove_messages_discarded_total`, with the same DLQ-aware rules the
//! poisoned-key cascade already gets (see `metrics_kafka_failall_no_dlq.rs`
//! and `metrics_kafka_failall.rs`, the sibling tests for the *post-handler*
//! terminal path on this same consumer).
//!
//! Before this change, the four decode/oversize drop sites in
//! `spawn_fifo_shards`'s receive loop called `metrics::record_failed` and
//! committed the offset with no discard accounting — a bare sequenced
//! topology dropped a poison message with nothing but a `WARN` line to show
//! for it.
//!
//! # Shape
//!
//! Two scenarios, one Kafka container, one `#[test]`:
//!
//! 1. **No DLQ.** An oversized and an undecodable message are dropped before
//!    the handler; both `discarded_total` and `failed_total` must move once
//!    per reason. A trailing valid message is the barrier.
//! 2. **DLQ declared and reachable.** The same two poison messages must be
//!    dead-lettered; `failed_total` still moves, `discarded_total` stays at
//!    zero.
//!
//! # Why `SequenceFailure::Skip` and a single partition
//!
//! Every drop site also calls `poison_key`, which is inert under `Skip`
//! (`PoisonedKeys::new` only allocates the poison set under `FailAll` — see
//! `src/routing.rs`), so using `Skip` here sidesteps the FailAll cascade
//! entirely: there is no risk of the barrier's own key getting swept up in a
//! poisoning it has nothing to do with, unlike `metrics_kafka_failall*.rs`
//! where the cascade is the very thing under test.
//!
//! Kafka runs a single FIFO task over its whole partition assignment and
//! processes one message fully (including its synchronous commit) before
//! `recv()`-ing the next, so with `routing_shards(1)` pinning the topic to one
//! partition, publish order is consume order. The trailing valid message
//! reaching the handler therefore proves the two poison messages ahead of it
//! were already decoded, dropped, and — because each FIFO commit is
//! synchronous — fully settled, not merely dispatched.
//!
//! `.allow_message_loss()` is required on the no-DLQ topology: sequenced
//! topics otherwise refuse to `build()` without a DLQ or a hold queue.
//!
//! # Oversize mechanics
//!
//! The consumer is configured with a tiny `ConsumerOptions::with_max_message_size`
//! rather than relying on the broker's ~1 MiB default, matching every other
//! oversize fixture in this suite.
//!
//! Scenario 2 reads the DLQ back with a raw `StreamConsumer` rather than a
//! typed `run_dlq`: the undecodable message fails to decode as `OrderMessage`
//! in the DLQ drain exactly as it did on the main path, so a typed drain would
//! ack it without ever calling `handle_dead` — see `drain_raw`'s doc.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot. Hence its own integration binary, a single `#[test]`, and
//! exactly one snapshot taken after every consumer has stopped.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::SequencedTopic as _;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::{SequenceFailure, TopologyBuilder};

const TIMEOUT: Duration = Duration::from_secs(60);

/// Comfortably above a bare `OrderMessage`, far below the padded oversize one.
const MAX_MESSAGE_SIZE: usize = 512;

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
    client: KafkaClient,
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
        let bootstrap_servers = format!("127.0.0.1:{port}");

        let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap_servers), 10)
            .await
            .expect("failed to connect to Kafka");

        Self {
            _container: container,
            client,
            brokers: bootstrap_servers,
        }
    }

    fn broker(&self) -> Broker<Kafka> {
        Broker::<Kafka>::from_client(self.client.clone())
    }

    fn client(&self) -> KafkaClient {
        self.client.clone()
    }
}

/// Publish a payload straight through rdkafka, bypassing shove's codec — the
/// only way to land a body that `T::Codec` cannot decode. Keyed so the FIFO
/// consumer's sequence-key extraction (which reads the Kafka message key) has
/// something deterministic to log, though with a single partition it plays no
/// role in ordering.
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
///
/// A typed `run_dlq` cannot be the barrier here: the undecodable message
/// fails to decode as `OrderMessage` in the DLQ drain exactly as it did on the
/// main path, so the drain acks it without ever calling `handle_dead`.
/// Reading the bytes back directly is the only way to observe both poison
/// messages landing in the DLQ.
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
struct OrderMessage {
    order_id: String,
    /// Padded well past `MAX_MESSAGE_SIZE` to trip the oversize gate.
    padding: String,
}

const NO_DLQ_TOPIC: &str = "kafka-metrics-fifo-prehandler-no-dlq";
const DLQ_TOPIC: &str = "kafka-metrics-fifo-prehandler-dlq";

shove::define_sequenced_topic!(
    NoDlqFifoTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new(NO_DLQ_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .allow_message_loss()
        .build()
);

shove::define_sequenced_topic!(
    DlqFifoTopic,
    OrderMessage,
    |msg: &OrderMessage| msg.order_id.clone(),
    TopologyBuilder::new(DLQ_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

/// Acks everything; signals when the marker key is handled.
#[derive(Clone)]
struct BarrierHandler {
    marker_key: &'static str,
    handled_marker: WaitableCounter,
    handled_total: Arc<AtomicU32>,
}

impl BarrierHandler {
    fn new(marker_key: &'static str) -> Self {
        Self {
            marker_key,
            handled_marker: WaitableCounter::new(),
            handled_total: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl MessageHandler<NoDlqFifoTopic> for BarrierHandler {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.handled_total.fetch_add(1, Ordering::SeqCst);
        if msg.order_id == self.marker_key {
            self.handled_marker.increment();
        }
        Outcome::Ack
    }
}

impl MessageHandler<DlqFifoTopic> for BarrierHandler {
    type Context = ();
    async fn handle(&self, msg: OrderMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.handled_total.fetch_add(1, Ordering::SeqCst);
        if msg.order_id == self.marker_key {
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

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fifo_pre_handler_drops_count_as_discarded_only_with_no_dlq() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    let client = tb.client();

    // -- Scenario 1: no DLQ ---------------------------------------------------
    {
        let broker = tb.broker();
        broker
            .topology()
            .declare::<NoDlqFifoTopic>()
            .await
            .expect("declare");
        let publisher = broker.publisher().await.expect("publisher");

        publisher
            .publish::<NoDlqFifoTopic>(&OrderMessage {
                order_id: "key-oversize".into(),
                padding: "x".repeat(4096),
            })
            .await
            .expect("publish oversized");
        publish_raw(
            &tb.brokers,
            NO_DLQ_TOPIC,
            "key-undecodable",
            b"{not valid json",
        )
        .await;
        const MARKER: &str = "key-marker";
        publisher
            .publish::<NoDlqFifoTopic>(&OrderMessage {
                order_id: MARKER.into(),
                padding: String::new(),
            })
            .await
            .expect("publish marker");

        let handler = BarrierHandler::new(MARKER);
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(client.clone());
        let handle = tokio::spawn(async move {
            consumer
                .run_fifo::<NoDlqFifoTopic, _>(
                    h,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_max_retries(0)
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

    // -- Scenario 2: DLQ declared and reachable -------------------------------
    {
        let broker = tb.broker();
        broker
            .topology()
            .declare::<DlqFifoTopic>()
            .await
            .expect("declare");
        let publisher = broker.publisher().await.expect("publisher");

        let oversized = OrderMessage {
            order_id: "key-oversize".into(),
            padding: "x".repeat(4096),
        };
        publisher
            .publish::<DlqFifoTopic>(&oversized)
            .await
            .expect("publish oversized");
        const POISON: &[u8] = b"{not valid json";
        publish_raw(&tb.brokers, DLQ_TOPIC, "key-undecodable", POISON).await;

        let handler = BarrierHandler::new("unused");
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let main_consumer = KafkaConsumer::new(client.clone());
        let main_handle = tokio::spawn(async move {
            main_consumer
                .run_fifo::<DlqFifoTopic, _>(
                    h,
                    (),
                    ConsumerOptions::<Kafka>::new()
                        .with_shutdown(sc)
                        .with_max_retries(0)
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

    // -- Scenario 1 assertions -------------------------------------------------
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
         once its offset commits"
    );
    assert_eq!(
        discarded_total(&snapshot, NO_DLQ_TOPIC, "deserialize"),
        1,
        "with no DLQ declared, the undecodable message is dropped on the \
         floor once its offset commits"
    );

    // -- Scenario 2 assertions -------------------------------------------------
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
         nothing was discarded"
    );
    assert_eq!(
        discarded_total(&snapshot, DLQ_TOPIC, "deserialize"),
        0,
        "the undecodable message was observed arriving in the DLQ above, so \
         nothing was discarded"
    );
}
