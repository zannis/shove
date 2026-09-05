#![cfg(all(feature = "aws-sns-sqs", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: the SQS batch consumer's metrics match the documented
//! batch-wide contract (`docs/pages/guides/observability.mdx`), the same
//! contract `tests/metrics_inmemory_batch.rs` pins for InMemory:
//!
//! - `shove_messages_consumed_total` is counted **per message**, under the
//!   batch's single outcome label — not once per flush.
//! - `shove_message_processing_duration_seconds` is observed **once per
//!   flush**, in message units it may cover many of.
//! - A rejected batch's `shove_messages_failed_total{reason="rejected"}`
//!   moves once per message, while `shove_messages_discarded_total` does
//!   **not** move at all — SQS never publishes to a DLQ itself; every
//!   `route_reject`/`route_reject_batch` call resets visibility and lets
//!   AWS-side redrive own any DLQ move, so the discard counter is opted out
//!   backend-wide (see `router::route_reject`'s doc).
//!
//! Split into its own binary — as `tests/metrics_inmemory_batch.rs` is, for
//! the same reason — because `metrics-util::debugging::DebuggingRecorder`
//! takes the *global* recorder slot; keeping it out of `tests/sns_sqs_batch.rs`
//! avoids any risk of two recorder installs racing inside one process (even
//! though `cargo-nextest` already runs each `#[test]` in its own process,
//! this mirrors the repo's established split rather than relying on that
//! alone).
//!
//! Self-contained LocalStack fixture, same deliberate deviation as
//! `tests/sns_sqs_batch.rs`: no `LOCALSTACK_AUTH_TOKEN` requirement, since
//! `testcontainers_modules::localstack::LocalStack` pins the Community
//! edition and SNS/SQS/redrive are core (non-Pro) services on it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::handler::BatchMessageHandler;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::publisher::Publisher;
use shove::sns::{SnsClient, SnsConfig};
use shove::topic::Topic;
use shove::topology::TopologyBuilder;
use shove::{BatchConsumerOptions, Sqs, define_topic};

use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

async fn wait_for_localstack_ready(endpoint_url: &str) {
    let aws_config = aws_config::from_env()
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .load()
        .await;
    let sns = aws_sdk_sns::Client::new(&aws_config);
    let sqs = aws_sdk_sqs::Client::new(&aws_config);

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if sns.list_topics().send().await.is_ok() && sqs.list_queues().send().await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "LocalStack services not ready within 30s at {endpoint_url}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

struct TestBroker {
    #[allow(dead_code)]
    container: testcontainers::ContainerAsync<LocalStack>,
    endpoint_url: String,
}

impl TestBroker {
    async fn start() -> Self {
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
        }

        let container = match std::env::var("LOCALSTACK_AUTH_TOKEN") {
            Ok(token) => {
                LocalStack::default()
                    .with_env_var("LOCALSTACK_AUTH_TOKEN", token)
                    .start()
                    .await
            }
            Err(_) => LocalStack::default().start().await,
        }
        .expect("failed to start LocalStack container");

        let port = container
            .get_host_port_ipv4(4566)
            .await
            .expect("failed to get LocalStack port");
        let endpoint_url = format!("http://localhost:{port}");
        wait_for_localstack_ready(&endpoint_url).await;

        Self {
            container,
            endpoint_url,
        }
    }

    fn sns_config(&self) -> SnsConfig {
        SnsConfig {
            region: Some("us-east-1".into()),
            endpoint_url: Some(self.endpoint_url.clone()),
        }
    }
}

struct TestSetup {
    broker: Broker<Sqs>,
    publisher: Publisher<Sqs>,
}

impl TestSetup {
    async fn new(broker: &TestBroker) -> Self {
        let sns_client = SnsClient::new(&broker.sns_config())
            .await
            .expect("failed to create SNS client");
        let broker = Broker::<Sqs>::from_client(sns_client);
        let publisher = broker.publisher().await.expect("publisher construction");
        Self { broker, publisher }
    }

    async fn declare<T: Topic>(&self) {
        self.broker
            .topology()
            .declare::<T>()
            .await
            .expect("topology declaration should succeed");
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct BatchMessage {
    seq: u32,
}

async fn publish_seqs<T>(publisher: &Publisher<Sqs>, seqs: impl IntoIterator<Item = u32>)
where
    T: Topic<Message = BatchMessage>,
{
    for seq in seqs {
        publisher
            .publish::<T>(&BatchMessage { seq })
            .await
            .expect("publish should succeed");
    }
}

type Snapshot = HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

fn counter(snapshot: &Snapshot, name: &str, extra: &[(&str, &str)]) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .filter(|(k, _)| {
            extra.iter().all(|(key, value)| {
                k.key()
                    .labels()
                    .any(|l| l.key() == *key && l.value() == *value)
            })
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(c) => *c,
            other => panic!("{name} is not a counter: {other:?}"),
        })
        .sum()
}

fn histogram_samples(snapshot: &Snapshot, name: &str) -> Vec<f64> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == name)
        .flat_map(|(_, (_, _, value))| match value {
            DebugValue::Histogram(samples) => {
                samples.iter().copied().map(f64::from).collect::<Vec<f64>>()
            }
            other => panic!("{name} is not a histogram: {other:?}"),
        })
        .collect()
}

const GROUP: &str = "metrics-sqs-batch-group";
const CONSUMED_QUEUE: &str = "sqs-metrics-batch-consumed";

define_topic!(
    ConsumedMetricsTopic,
    BatchMessage,
    TopologyBuilder::new(CONSUMED_QUEUE).build()
);

#[derive(Clone)]
struct AckAllHandler {
    calls: Arc<AtomicUsize>,
    signal: Arc<Notify>,
}

impl AckAllHandler {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            signal: Arc::new(Notify::new()),
        }
    }
}

impl BatchMessageHandler<ConsumedMetricsTopic> for AckAllHandler {
    type Context = ();
    async fn handle_batch(
        &self,
        _messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.signal.notify_waiters();
        Outcome::Ack
    }
}

/// Lean version of `metrics_inmemory_batch.rs`'s scope: this pins the SQS
/// wiring for the two facts every backend's batch flush must get right — the
/// per-message/per-flush split — without re-covering the pre-handler-drop
/// richness the InMemory test also carries (that mechanism does not exist on
/// SQS; see `src/backends/sns/consumer.rs`'s module doc).
#[tokio::test(flavor = "current_thread")]
async fn batch_metrics_match_the_documented_contract() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<ConsumedMetricsTopic>().await;

    publish_seqs::<ConsumedMetricsTopic>(&setup.publisher, 0..3).await;

    let handler = AckAllHandler::new();
    let calls = handler.calls.clone();
    let signal = handler.signal.clone();
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<ConsumedMetricsTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(10))
                        .with_consumer_group(GROUP)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    let deadline = Instant::now() + Duration::from_secs(30);
    while calls.load(Ordering::SeqCst) < 1 && Instant::now() < deadline {
        tokio::select! {
            _ = signal.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the flush must have happened exactly once"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let snapshot = snapshotter.snapshot().into_hashmap();

    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_consumed_total",
            &[
                ("topic", CONSUMED_QUEUE),
                ("consumer_group", GROUP),
                ("outcome", "ack"),
            ],
        ),
        3,
        "consumed must count messages, not flushes"
    );

    assert_eq!(
        histogram_samples(&snapshot, "shove_message_processing_duration_seconds").len(),
        1,
        "processing duration must be observed once per flush, not once per message"
    );
}

const REJECT_QUEUE: &str = "sqs-metrics-batch-reject";

define_topic!(
    RejectMetricsTopic,
    BatchMessage,
    TopologyBuilder::new(REJECT_QUEUE).dlq().build()
);

#[derive(Clone)]
struct RejectOnceHandler {
    calls: Arc<AtomicUsize>,
    signal: Arc<Notify>,
}

impl RejectOnceHandler {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            signal: Arc::new(Notify::new()),
        }
    }
}

impl BatchMessageHandler<RejectMetricsTopic> for RejectOnceHandler {
    type Context = ();
    async fn handle_batch(
        &self,
        _messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.signal.notify_waiters();
        Outcome::Reject
    }
}

/// The metrics half of `tests/sns_sqs_batch.rs`'s
/// `reject_mechanics_leave_messages_on_the_queue` acceptance criterion,
/// split out for `DebuggingRecorder` isolation (see module doc): a rejected
/// batch must move `messages_failed_total` once per message and must NOT
/// move `messages_discarded_total` at all, since SQS's reject path never
/// publishes to a DLQ itself.
#[tokio::test(flavor = "current_thread")]
async fn reject_records_failed_and_leaves_discard_untouched() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<RejectMetricsTopic>().await;

    publish_seqs::<RejectMetricsTopic>(&setup.publisher, 0..2).await;

    let handler = RejectOnceHandler::new();
    let calls = handler.calls.clone();
    let signal = handler.signal.clone();
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<RejectMetricsTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(10))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    let deadline = Instant::now() + Duration::from_secs(30);
    while calls.load(Ordering::SeqCst) < 1 && Instant::now() < deadline {
        tokio::select! {
            _ = signal.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Stop before the DeadLetter arm's backoff sleep elapses and the batch
    // is re-received — one rejected flush is all this needs.
    shutdown.cancel();
    handle.await.unwrap().ok();

    let snapshot = snapshotter.snapshot().into_hashmap();

    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_failed_total",
            &[("topic", REJECT_QUEUE), ("reason", "rejected")],
        ),
        2,
        "messages_failed_total must move once per message in the rejected batch"
    );
    assert_eq!(
        counter(
            &snapshot,
            "shove_messages_discarded_total",
            &[("topic", REJECT_QUEUE), ("reason", "rejected")],
        ),
        0,
        "SQS never publishes to a DLQ itself, so the discard counter must not move"
    );
}
