#![cfg(all(feature = "kafka", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: the `consumer_group` metric label means the **same thing**
//! on every Kafka consume entrypoint — `run`, `run_fifo` and `run_batch`.
//!
//! It is the shove consumer-group name (`with_consumer_group`, `"default"` when
//! unset), never the derived Kafka `group.id`. That is what the other four
//! backends report, and what `docs/pages/guides/observability.mdx` documents.
//!
//! `run_batch` used to label with the `group.id` instead — `{queue}-consumer`,
//! or whatever `with_group_id` overrode it to. The same topic consumed with
//! `run` and with `run_batch`, configured identically, produced two different
//! label values, so a `sum by (consumer_group)` panel silently split one logical
//! group in two. Nothing caught it: the only batch metrics test in the repo
//! deliberately sums *across* the label ("whatever `consumer_group` label the
//! consumer happened to generate", `metrics_kafka_batch_reject.rs`).
//!
//! # Why the assertion is shaped this way
//!
//! Each scenario sets a Kafka `group.id` override that is deliberately nothing
//! like its shove group name. A label sourced from the `group.id` therefore
//! cannot coincidentally equal the expected value — it reads `gid-*-override`
//! (or, in the fourth scenario, `{queue}-consumer`) and the assertion names it.
//!
//! The check sweeps **every** `shove_*` series carrying the scenario's `topic`
//! label and collects the distinct `consumer_group` values, rather than probing
//! one counter. The batch path threads a single `group` binding into
//! `record_consumed_n`, `record_failed`, `record_message_size`,
//! `record_processing_duration` and the inflight gauge, so a per-call-site
//! assertion would pin one of them and let the rest drift.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot — hence its own integration binary and a single `#[test]`. The
//! one snapshot is taken after every consumer has stopped, and the scenarios are
//! told apart by their `topic` label.

use std::collections::BTreeSet;
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
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::kafka::{BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::{SequenceFailure, TopologyBuilder};

/// How long a wait may take before the scenario is considered failed.
const TIMEOUT: Duration = Duration::from_secs(60);

/// Messages per scenario. One would prove the label; three keeps the wait
/// insensitive to how the broker splits a batch across polls.
const MESSAGES: u32 = 3;

/// The shove consumer-group name every configured scenario sets. Nothing like
/// a derived Kafka `group.id`, so the two can never be confused in a failure
/// message.
const GROUP: &str = "orders-workers";

/// What the label must read when no group name is configured.
const DEFAULT_GROUP: &str = "default";

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Job {
    id: u32,
}

const RUN_TOPIC: &str = "kafka-metrics-group-label-run";
const FIFO_TOPIC: &str = "kafka-metrics-group-label-fifo";
const BATCH_TOPIC: &str = "kafka-metrics-group-label-batch";
const BATCH_DEFAULT_TOPIC: &str = "kafka-metrics-group-label-batch-default";

shove::define_topic!(RunTopic, Job, TopologyBuilder::new(RUN_TOPIC).build());

shove::define_sequenced_topic!(
    FifoTopic,
    Job,
    |_: &Job| "only-key".to_string(),
    TopologyBuilder::new(FIFO_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(2)
        .hold_queue(Duration::from_millis(200))
        .dlq()
        .build()
);

shove::define_topic!(BatchTopic, Job, TopologyBuilder::new(BATCH_TOPIC).build());

shove::define_topic!(
    BatchDefaultTopic,
    Job,
    TopologyBuilder::new(BATCH_DEFAULT_TOPIC).build()
);

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// One container for the whole test. Each scenario connects a *fresh*
/// `KafkaClient`: `broker.close()` cancels the client's shutdown token, and a
/// consumer started on an already-cancelled client stops before it consumes
/// anything.
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

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Acks everything and signals on each message, so a scenario can wait rather
/// than sleep. Only the labels the consumer emits are under test, so the
/// outcome never varies.
#[derive(Clone)]
struct AckingHandler {
    seen: Arc<AtomicU32>,
    signal: Arc<Notify>,
}

impl AckingHandler {
    fn new() -> Self {
        Self {
            seen: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(Notify::new()),
        }
    }

    fn record(&self, n: u32) {
        self.seen.fetch_add(n, Ordering::Relaxed);
        self.signal.notify_waiters();
    }

    fn seen(&self) -> u32 {
        self.seen.load(Ordering::Relaxed)
    }

    async fn wait_for_messages(&self, n: u32) -> bool {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if self.seen() >= n {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => return self.seen() >= n,
            }
        }
    }
}

macro_rules! impl_single_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl MessageHandler<$topic> for AckingHandler {
                type Context = ();
                async fn handle(&self, _: Job, _: MessageMetadata, _: &()) -> Outcome {
                    self.record(1);
                    Outcome::Ack
                }
            }
        )*
    };
}

macro_rules! impl_batch_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl BatchMessageHandler<$topic> for AckingHandler {
                type Context = ();
                async fn handle_batch(
                    &self,
                    messages: Vec<(Job, MessageMetadata)>,
                    _: &(),
                ) -> Outcome {
                    let handled = u32::try_from(messages.len()).expect("batch larger than u32");
                    self.record(handled);
                    Outcome::Ack
                }
            }
        )*
    };
}

impl_single_for!(RunTopic, FifoTopic);
impl_batch_for!(BatchTopic, BatchDefaultTopic);

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

/// Every series in the snapshot that carries `topic == topic`, as
/// `(metric name, consumer_group label)`. Series without a `consumer_group`
/// label (`shove_queue_backlog`) are not part of this contract and are skipped.
fn labelled_series(snapshot: &Snapshot, topic: &str) -> Vec<(String, String)> {
    snapshot
        .iter()
        .filter_map(|(k, _)| {
            let mut matches_topic = false;
            let mut group = None;
            for label in k.key().labels() {
                match label.key() {
                    "topic" => matches_topic = label.value() == topic,
                    "consumer_group" => group = Some(label.value().to_string()),
                    _ => {}
                }
            }
            match (matches_topic, group) {
                (true, Some(g)) => Some((k.key().name().to_string(), g)),
                _ => None,
            }
        })
        .collect()
}

/// The distinct `consumer_group` values every labelled series for `topic`
/// reports.
fn group_labels(snapshot: &Snapshot, topic: &str) -> BTreeSet<String> {
    labelled_series(snapshot, topic)
        .into_iter()
        .map(|(_, group)| group)
        .collect()
}

fn metric_names(snapshot: &Snapshot, topic: &str) -> BTreeSet<String> {
    labelled_series(snapshot, topic)
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// Assert the whole labelled surface of one entrypoint reports `expected`.
///
/// The emptiness check is what stops this passing vacuously: a consumer that
/// never ran emits no series at all, and an equality against an empty set would
/// hold for every expected value.
fn assert_group_label(snapshot: &Snapshot, topic: &str, expected: &str, entrypoint: &str) {
    let names = metric_names(snapshot, topic);
    assert!(
        names.contains("shove_messages_consumed_total"),
        "`{entrypoint}` must have emitted `shove_messages_consumed_total` for \
         `{topic}` — without it the label assertion below would pass on a \
         consumer that never consumed anything. Labelled series seen: {names:?}"
    );

    let labels = group_labels(snapshot, topic);
    assert_eq!(
        labels,
        BTreeSet::from([expected.to_string()]),
        "every `{entrypoint}` metric for `{topic}` must carry \
         `consumer_group=\"{expected}\"` — the shove consumer-group name, which \
         is what `run`, `run_fifo`, `run_batch` and the other four backends all \
         report. A Kafka `group.id` here (`gid-*-override`, `{topic}-consumer`, \
         …) is a backend implementation detail no other backend exposes, and it \
         splits one logical group across two label values in \
         `sum by (consumer_group)`. Series checked: {:?}",
        labelled_series(snapshot, topic)
    );
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

async fn publish_jobs<T>(broker: &Broker<Kafka>, count: u32)
where
    T: Topic<Message = Job>,
{
    let publisher = broker.publisher().await.expect("publisher");
    for id in 0..count {
        publisher
            .publish::<T>(&Job { id })
            .await
            .expect("publish should succeed");
    }
}

/// Publish `MESSAGES`, run `consume` against them until every one has reached
/// the handler, then stop.
async fn run_scenario<T, F, Fut>(tb: &TestBroker, consume: F)
where
    T: Topic<Message = Job> + 'static,
    F: FnOnce(KafkaConsumer, AckingHandler, CancellationToken) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let client = tb.client().await;
    let broker = Broker::<Kafka>::from_client(client.clone());
    broker.topology().declare::<T>().await.expect("declare");
    publish_jobs::<T>(&broker, MESSAGES).await;

    let handler = AckingHandler::new();
    let shutdown = CancellationToken::new();
    let task = consume(
        KafkaConsumer::new(client),
        handler.clone(),
        shutdown.clone(),
    );
    let handle = tokio::spawn(task);

    let all_seen = handler.wait_for_messages(MESSAGES).await;
    assert!(
        all_seen,
        "every published message must reach the handler; saw {} of {MESSAGES}",
        handler.seen()
    );

    shutdown.cancel();
    handle.await.expect("consumer task panicked");
    broker.close().await;
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn consumer_group_label_is_the_shove_group_name_on_every_entrypoint() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tb = TestBroker::start().await;

    // -- `run` --------------------------------------------------------------
    //
    // The reference behaviour: the shove group name, with a `group.id`
    // override set to prove the label does not follow it.
    run_scenario::<RunTopic, _, _>(&tb, |consumer, handler, shutdown| async move {
        consumer
            .run::<RunTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(shutdown)
                    .with_consumer_group(GROUP)
                    .with_group_id("gid-run-override"),
            )
            .await
            .ok();
    })
    .await;

    // -- `run_fifo` ---------------------------------------------------------
    run_scenario::<FifoTopic, _, _>(&tb, |consumer, handler, shutdown| async move {
        consumer
            .run_fifo::<FifoTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(shutdown)
                    .with_consumer_group(GROUP)
                    .with_group_id("gid-fifo-override"),
            )
            .await
            .ok();
    })
    .await;

    // -- `run_batch`, group name configured ---------------------------------
    //
    // The defect: this reported `gid-batch-override` while the two above
    // reported `orders-workers`, for the same configuration.
    run_scenario::<BatchTopic, _, _>(&tb, |consumer, handler, shutdown| async move {
        consumer
            .run_batch::<BatchTopic, _>(
                handler,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(MESSAGES as usize)
                    .with_max_batch_age(Duration::from_millis(500))
                    .with_shutdown(shutdown)
                    .with_consumer_group(GROUP)
                    .with_group_id("gid-batch-override"),
            )
            .await
            .ok();
    })
    .await;

    // -- `run_batch`, nothing configured ------------------------------------
    //
    // The branch a batch consumer hits without opting into anything, and the
    // one the defect showed up on with no override in play at all: it reported
    // `{queue}-consumer`, which is neither a group name nor `"default"`.
    run_scenario::<BatchDefaultTopic, _, _>(&tb, |consumer, handler, shutdown| async move {
        consumer
            .run_batch::<BatchDefaultTopic, _>(
                handler,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(MESSAGES as usize)
                    .with_max_batch_age(Duration::from_millis(500))
                    .with_shutdown(shutdown),
            )
            .await
            .ok();
    })
    .await;

    // Single snapshot, taken once every consumer has stopped so nothing can
    // emit into it while it is read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    assert_group_label(&snapshot, RUN_TOPIC, GROUP, "run");
    assert_group_label(&snapshot, FIFO_TOPIC, GROUP, "run_fifo");
    assert_group_label(&snapshot, BATCH_TOPIC, GROUP, "run_batch");
    assert_group_label(&snapshot, BATCH_DEFAULT_TOPIC, DEFAULT_GROUP, "run_batch");
}
