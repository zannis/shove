#![cfg(all(feature = "kafka", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: on an infra-owned Kafka topic with **no DLQ**, a record
//! whose `Retry` budget runs out is retried in place, never republished, and
//! is then gone. `shove_messages_discarded_total{reason="max_retries_exceeded"}`
//! must move exactly once for it, and `shove_messages_failed_total` once,
//! while the topic's high watermark shows that no copy was ever produced.
//!
//! This is the external-topic twin of `metrics_kafka_failall_no_dlq.rs`. Both
//! pin the arm where the `CommitMode::Sync` commit that retires a record is
//! the thing that loses it, and nothing else would notice. The in-place path
//! reaches that arm through `route_outcome`'s DLQ decision after
//! `redeliver_in_place` has exhausted the budget in memory, a route the
//! republishing path never takes, so the discard accounting needs its own
//! end-to-end proof here.
//!
//! # Barrier
//!
//! One partition and concurrent processing off, so the consumer has one slot
//! and `marker` reaching the handler proves `poison` ahead of it was retried
//! out and settled. The snapshot is taken after the consumer has stopped, once the
//! shutdown drain has committed and confirmed the discard.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot. Hence its own integration binary, a single `#[test]`, and
//! exactly one snapshot taken after the consumer has stopped.
//!
//! Run with:
//! `cargo nextest run --features kafka,metrics --test metrics_kafka_external_topic_discard`

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use serde::{Deserialize, Serialize};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::{Mutex, Notify};
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
const TOPIC: &str = "kafka-metrics-external-discard";

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

/// Infra's side of the contract: the topic exists before shove sees it, with
/// one partition so publish order is consume order.
async fn provision_single_partition_topic(brokers: &str, topic: &str) {
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

fn high_watermark(brokers: &str, topic: &str) -> i64 {
    use rdkafka::consumer::{BaseConsumer, Consumer as _};

    let probe: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to create watermark probe");
    let (_, high) = probe
        .fetch_watermarks(topic, 0, Duration::from_secs(10))
        .expect("fetch_watermarks failed");
    high
}

// ---------------------------------------------------------------------------
// Topic and handler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Event {
    id: String,
}

// No DLQ, so an exhausted record is dropped on the floor once its offset
// commits; the short hold tier keeps the in-place waits quick.
shove::define_topic!(
    ExternalNoDlqTopic,
    Event,
    TopologyBuilder::new("kafka-metrics-external-discard")
        .external()
        .hold_queue(Duration::from_millis(100))
        .allow_message_loss()
        .build()
);

/// `Retry` for `poison` on every attempt, `Ack` for `marker`.
#[derive(Clone)]
struct PoisonHandler {
    seen: Arc<Mutex<Vec<(String, u32)>>>,
    marker_handled: WaitableCounter,
}

impl MessageHandler<ExternalNoDlqTopic> for PoisonHandler {
    type Context = ();
    async fn handle(&self, msg: Event, meta: MessageMetadata, _: &()) -> Outcome {
        self.seen
            .lock()
            .await
            .push((msg.id.clone(), meta.retry_count));
        if msg.id == "marker" {
            self.marker_handled.increment();
            Outcome::Ack
        } else {
            Outcome::Retry
        }
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

fn counter_total(snapshot: &Snapshot, name: &str, reason: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "reason" && l.value() == reason)
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("{name} is not a counter: {other:?}"),
        })
        .sum()
}

/// Every `shove_messages_discarded_total` series, as `(reason, count)`.
fn discarded_series(snapshot: &Snapshot) -> Vec<(String, u64)> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_discarded_total")
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
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exhausted_in_place_retries_on_an_external_topic_count_one_discard() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;
    provision_single_partition_topic(&tb.brokers, TOPIC).await;
    let client = tb.client().await;
    let broker = Broker::<Kafka>::from_client(client.clone());
    // Verifies the topic and creates nothing: there is no DLQ to create.
    broker
        .topology()
        .declare::<ExternalNoDlqTopic>()
        .await
        .expect("declare must succeed against the provisioned topic");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<ExternalNoDlqTopic>(&Event {
            id: "poison".into(),
        })
        .await
        .expect("publish poison");
    publisher
        .publish::<ExternalNoDlqTopic>(&Event {
            id: "marker".into(),
        })
        .await
        .expect("publish marker");

    let handler = PoisonHandler {
        seen: Arc::new(Mutex::new(Vec::new())),
        marker_handled: WaitableCounter::new(),
    };
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(client.clone());
    let handle = tokio::spawn(async move {
        consumer
            .run::<ExternalNoDlqTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_max_retries(2)
                    // One slot: the marker cannot overtake the poison.
                    .with_concurrent_processing(false)
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.marker_handled.wait_for(1, TIMEOUT).await,
        "the marker behind the poison must reach the handler once the poison \
         has been retried out in place"
    );
    shutdown.cancel();
    handle.await.expect("consumer task panicked").ok();

    let seen = handler.seen.lock().await.clone();
    assert_eq!(
        seen,
        vec![
            ("poison".to_string(), 0),
            ("poison".to_string(), 1),
            ("poison".to_string(), 2),
            ("marker".to_string(), 0),
        ],
        "one initial attempt, two in-place retries, then the marker"
    );
    assert_eq!(
        high_watermark(&tb.brokers, TOPIC),
        2,
        "no retry was republished into the external topic"
    );

    let snapshot = snapshotter.snapshot().into_hashmap();
    assert_eq!(
        counter_total(
            &snapshot,
            "shove_messages_failed_total",
            "max_retries_exceeded"
        ),
        1,
        "the exhausted budget counts exactly one failure"
    );
    let mut discarded = discarded_series(&snapshot);
    discarded.sort();
    assert_eq!(
        discarded,
        vec![("max_retries_exceeded".to_string(), 1)],
        "with no DLQ the exhausted record is gone once its offset commits, and \
         that is the only discard"
    );
    broker.close().await;
}
