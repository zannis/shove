#![cfg(all(feature = "kafka", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: Kafka's **batch** consumer must count a message dropped
//! *before the handler* — oversize, or undecodable — in
//! `shove_messages_discarded_total`, with the same DLQ-aware rules the batch
//! `Reject` arm already gets (see `metrics_kafka_batch_reject.rs`).
//!
//! Before this change, `decode_batch_message`'s `BatchDecode::Dlq` arm and the
//! batch receive loop's own oversize check both called `metrics::record_failed`
//! and parked the message for `publish_pending_dlq`, with no discard accounting
//! at all — a bare topology dropped every poison message on the floor with
//! nothing but a `WARN` line to show for it.
//!
//! # Shape
//!
//! Three scenarios, one Kafka container, one `#[test]`:
//!
//! 1. **No DLQ, mixed batch.** An oversized and an undecodable message share a
//!    flush window with one valid message. `max_batch_size` is set to exactly
//!    the number of messages published, so the flush triggers only once every
//!    message has been received — whichever arrives last — and the handler
//!    receiving the valid one is the barrier proving that flush (poison
//!    included) has run. Both `discarded_total` and `failed_total` must move
//!    once per reason.
//! 2. **No DLQ, all-poison window.** The batch's *own* offset span can be
//!    non-empty while `messages` is empty (every message in it was dropped
//!    pre-handler) — `flush_batch`'s `batch_size == 0` arm. There is no
//!    message left to hand the handler in that same run, so the barrier is a
//!    *second*, fresh consumer run on the same group: the all-poison window's
//!    commit must have landed for a later valid message to be handled at all,
//!    exactly like `kafka_batch_integration.rs`'s
//!    `a_batch_of_only_dropped_messages_still_commits`.
//! 3. **DLQ declared and reachable.** The same mixed-batch shape as scenario
//!    1, but `discarded_total` must stay at zero and both poison payloads must
//!    be readable off the DLQ byte-for-byte.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot — hence its own integration binary, a single `#[test]`, and
//! one snapshot taken after every consumer has stopped.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::handler::BatchMessageHandler;
use shove::kafka::{BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;

/// How long a wait may take before the scenario is considered failed.
const TIMEOUT: Duration = Duration::from_secs(60);

/// Comfortably above a bare `BatchMessage`, far below the padded oversize one.
const MAX_MESSAGE_SIZE: usize = 512;

const MIXED_TOPIC: &str = "kafka-metrics-batch-prehandler-mixed";
const ALLPOISON_TOPIC: &str = "kafka-metrics-batch-prehandler-allpoison";
const DLQ_TOPIC: &str = "kafka-metrics-batch-prehandler-dlq";

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BatchMessage {
    seq: u32,
    padding: String,
}

impl BatchMessage {
    fn new(seq: u32) -> Self {
        Self {
            seq,
            padding: String::new(),
        }
    }

    fn oversized(seq: u32) -> Self {
        Self {
            seq,
            padding: "x".repeat(4096),
        }
    }
}

shove::define_topic!(
    MixedTopic,
    BatchMessage,
    TopologyBuilder::new(MIXED_TOPIC).build()
);

shove::define_topic!(
    AllPoisonTopic,
    BatchMessage,
    TopologyBuilder::new(ALLPOISON_TOPIC).build()
);

shove::define_topic!(
    DlqBatchTopic,
    BatchMessage,
    TopologyBuilder::new(DLQ_TOPIC).dlq().build()
);

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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

/// Publish a payload straight through rdkafka, bypassing shove's codec — the
/// only way to land a body that `T::Codec` cannot decode.
async fn publish_raw(brokers: &str, topic: &str, payload: &[u8]) {
    use rdkafka::producer::{FutureProducer, FutureRecord};

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
// Handler
// ---------------------------------------------------------------------------

/// Records every batch it is handed and signals on each one so a scenario can
/// wait rather than sleep.
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

    fn record(&self, batch: &[(BatchMessage, MessageMetadata)]) -> Outcome {
        self.batches
            .lock()
            .expect("batches lock poisoned")
            .push(batch.iter().map(|(m, _)| m.seq).collect());
        self.signal.notify_waiters();
        Outcome::Ack
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

    fn batches(&self) -> Vec<Vec<u32>> {
        self.batches.lock().expect("batches lock poisoned").clone()
    }

    /// Wait until at least one flush has contained `seq`.
    async fn wait_for_seq(&self, seq: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.seen().contains(&seq) {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => return self.seen().contains(&seq),
            }
        }
    }
}

macro_rules! impl_recording_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl BatchMessageHandler<$topic> for RecordingBatchHandler {
                type Context = ();
                async fn handle_batch(
                    &self,
                    messages: Vec<(BatchMessage, MessageMetadata)>,
                    _: &(),
                ) -> Outcome {
                    self.record(&messages)
                }
            }
        )*
    };
}

impl_recording_for!(MixedTopic, AllPoisonTopic, DlqBatchTopic);

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
async fn batch_pre_handler_drops_count_as_discarded_only_with_no_dlq() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;

    // -- Scenario 1: no DLQ, mixed batch -------------------------------------
    let mixed_handler = {
        let client = tb.client().await;
        let broker = Broker::<Kafka>::from_client(client.clone());
        broker.topology().declare::<MixedTopic>().await.unwrap();
        let publisher = broker.publisher().await.unwrap();
        publisher
            .publish::<MixedTopic>(&BatchMessage::oversized(1))
            .await
            .unwrap();
        publish_raw(&tb.brokers, MIXED_TOPIC, b"{not valid json").await;
        const MARKER: u32 = 2;
        publisher
            .publish::<MixedTopic>(&BatchMessage::new(MARKER))
            .await
            .unwrap();

        let handler = RecordingBatchHandler::new();
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(client);
        let handle = tokio::spawn(async move {
            consumer
                .run_batch::<MixedTopic, _>(
                    h,
                    (),
                    BatchConsumerOptions::new()
                        // Exactly the number of messages published, so the
                        // flush triggers only once all three have arrived.
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(10))
                        .with_max_message_size(MAX_MESSAGE_SIZE)
                        .with_shutdown(sc),
                )
                .await
        });

        assert!(
            handler.wait_for_seq(MARKER, TIMEOUT).await,
            "the marker message must reach the handler; batches so far: {:?}",
            handler.batches()
        );
        shutdown.cancel();
        handle.await.unwrap().ok();
        broker.close().await;

        assert_eq!(
            handler.seen(),
            vec![MARKER],
            "only the marker message should ever reach the handler"
        );
        handler
    };

    // -- Scenario 2: no DLQ, all-poison window (empty-batch commit arm) -----
    let allpoison_handler = {
        const GROUP: &str = "batch-prehandler-allpoison-group";

        // Run 1: nothing but poison. The handler is never invoked, so there
        // is no batch to wait on — give the age-triggered flush time to fire
        // and commit, exactly like
        // `a_batch_of_only_dropped_messages_still_commits`.
        {
            let client = tb.client().await;
            let broker = Broker::<Kafka>::from_client(client.clone());
            broker.topology().declare::<AllPoisonTopic>().await.unwrap();
            let publisher = broker.publisher().await.unwrap();
            publisher
                .publish::<AllPoisonTopic>(&BatchMessage::oversized(101))
                .await
                .unwrap();
            publish_raw(&tb.brokers, ALLPOISON_TOPIC, b"{not valid json").await;

            let handler = RecordingBatchHandler::new();
            let h = handler.clone();
            let shutdown = CancellationToken::new();
            let sc = shutdown.clone();
            let consumer = KafkaConsumer::new(client);
            let handle = tokio::spawn(async move {
                consumer
                    .run_batch::<AllPoisonTopic, _>(
                        h,
                        (),
                        BatchConsumerOptions::new()
                            .with_max_batch_size(1000)
                            .with_max_batch_age(Duration::from_millis(300))
                            .with_max_message_size(MAX_MESSAGE_SIZE)
                            .with_group_id(GROUP)
                            .with_shutdown(sc),
                    )
                    .await
            });

            tokio::time::sleep(Duration::from_secs(3)).await;
            shutdown.cancel();
            handle.await.unwrap().ok();
            broker.close().await;

            assert!(
                handler.batches().is_empty(),
                "no batch should reach the handler, got {:?}",
                handler.batches()
            );
        }

        // Run 2: a fresh consumer, same group. The poison must not be
        // re-read, and the new valid message reaching the handler is the
        // proof that run 1's all-poison commit actually landed.
        let client = tb.client().await;
        let broker = Broker::<Kafka>::from_client(client.clone());
        let publisher = broker.publisher().await.unwrap();
        const MARKER: u32 = 102;
        publisher
            .publish::<AllPoisonTopic>(&BatchMessage::new(MARKER))
            .await
            .unwrap();

        let handler = RecordingBatchHandler::new();
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(client);
        let handle = tokio::spawn(async move {
            consumer
                .run_batch::<AllPoisonTopic, _>(
                    h,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(1000)
                        .with_max_batch_age(Duration::from_millis(300))
                        .with_max_message_size(MAX_MESSAGE_SIZE)
                        .with_group_id(GROUP)
                        .with_shutdown(sc),
                )
                .await
        });

        assert!(
            handler.wait_for_seq(MARKER, TIMEOUT).await,
            "the marker message must reach the handler on the fresh run"
        );
        shutdown.cancel();
        handle.await.unwrap().ok();
        broker.close().await;

        assert_eq!(
            handler.seen(),
            vec![MARKER],
            "the poison offsets must not have been replayed"
        );
        handler
    };

    // -- Scenario 3: DLQ declared and reachable ------------------------------
    let dlq_handler = {
        let client = tb.client().await;
        let broker = Broker::<Kafka>::from_client(client.clone());
        broker.topology().declare::<DlqBatchTopic>().await.unwrap();
        let publisher = broker.publisher().await.unwrap();
        let oversized = BatchMessage::oversized(201);
        publisher
            .publish::<DlqBatchTopic>(&oversized)
            .await
            .unwrap();
        const POISON: &[u8] = b"{not valid json";
        publish_raw(&tb.brokers, DLQ_TOPIC, POISON).await;
        const MARKER: u32 = 202;
        publisher
            .publish::<DlqBatchTopic>(&BatchMessage::new(MARKER))
            .await
            .unwrap();

        let handler = RecordingBatchHandler::new();
        let h = handler.clone();
        let shutdown = CancellationToken::new();
        let sc = shutdown.clone();
        let consumer = KafkaConsumer::new(client);
        let handle = tokio::spawn(async move {
            consumer
                .run_batch::<DlqBatchTopic, _>(
                    h,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(10))
                        .with_max_message_size(MAX_MESSAGE_SIZE)
                        .with_shutdown(sc),
                )
                .await
        });

        assert!(
            handler.wait_for_seq(MARKER, TIMEOUT).await,
            "the marker message must reach the handler; batches so far: {:?}",
            handler.batches()
        );
        shutdown.cancel();
        handle.await.unwrap().ok();

        let mut dead = drain_raw(&tb.brokers, &format!("{DLQ_TOPIC}-dlq"), 2).await;
        dead.sort();
        let mut expected = vec![serde_json::to_vec(&oversized).unwrap(), POISON.to_vec()];
        expected.sort();
        assert_eq!(
            dead, expected,
            "both poison payloads should be preserved in the DLQ"
        );

        broker.close().await;

        assert_eq!(
            handler.seen(),
            vec![MARKER],
            "only the marker message should ever reach the handler"
        );
        handler
    };

    // Single, draining snapshot, taken only once every consumer above has
    // stopped so nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    // -- Scenario 1 assertions -----------------------------------------------
    assert_eq!(
        failed_total(&snapshot, MIXED_TOPIC, "oversize"),
        1,
        "handler saw {:?}",
        mixed_handler.batches()
    );
    assert_eq!(
        failed_total(&snapshot, MIXED_TOPIC, "deserialize"),
        1,
        "handler saw {:?}",
        mixed_handler.batches()
    );
    assert_eq!(
        discarded_total(&snapshot, MIXED_TOPIC, "oversize"),
        1,
        "with no DLQ declared, the oversized message is dropped on the floor \
         once its offset commits"
    );
    assert_eq!(
        discarded_total(&snapshot, MIXED_TOPIC, "deserialize"),
        1,
        "with no DLQ declared, the undecodable message is dropped on the \
         floor once its offset commits"
    );

    // -- Scenario 2 assertions -----------------------------------------------
    assert_eq!(
        failed_total(&snapshot, ALLPOISON_TOPIC, "oversize"),
        1,
        "handler saw {:?}",
        allpoison_handler.batches()
    );
    assert_eq!(
        failed_total(&snapshot, ALLPOISON_TOPIC, "deserialize"),
        1,
        "handler saw {:?}",
        allpoison_handler.batches()
    );
    assert_eq!(
        discarded_total(&snapshot, ALLPOISON_TOPIC, "oversize"),
        1,
        "the empty-batch commit arm must settle the discard for the \
         oversized message exactly like the mixed-batch arm does"
    );
    assert_eq!(
        discarded_total(&snapshot, ALLPOISON_TOPIC, "deserialize"),
        1,
        "the empty-batch commit arm must settle the discard for the \
         undecodable message exactly like the mixed-batch arm does"
    );

    // -- Scenario 3 assertions -----------------------------------------------
    assert_eq!(
        failed_total(&snapshot, DLQ_TOPIC, "oversize"),
        1,
        "handler saw {:?}",
        dlq_handler.batches()
    );
    assert_eq!(
        failed_total(&snapshot, DLQ_TOPIC, "deserialize"),
        1,
        "handler saw {:?}",
        dlq_handler.batches()
    );
    assert_eq!(
        discarded_total(&snapshot, DLQ_TOPIC, "oversize"),
        0,
        "the oversized message is readable off the DLQ, so nothing was \
         discarded"
    );
    assert_eq!(
        discarded_total(&snapshot, DLQ_TOPIC, "deserialize"),
        0,
        "the undecodable message is readable off the DLQ, so nothing was \
         discarded"
    );
}
