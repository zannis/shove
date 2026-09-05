#![cfg(feature = "aws-sns-sqs")]

//! Integration tests for the SQS batch consumer
//! (`Broker::<Sqs>::batch_consumer()`).
//!
//! Drives everything through the generic wrapper — `BatchConsumer<Sqs>` /
//! `BatchConsumerOptions<Sqs>` — the same public entry point every other
//! backend's batch tests use (see `tests/inmemory_batch.rs`), over SQS's own
//! mechanics: a poll-shaped `ReceiveMessage` loop, `DeleteMessageBatch` for
//! `Ack`, and `ChangeMessageVisibilityBatch` for `Retry`/`Defer`/`Reject`,
//! instead of a `VecDeque` or partition offsets. See
//! `src/backends/sns/consumer.rs`'s "Batch consumption" module doc for the
//! full contract this pins (the 10-message cap, the visibility-timing bound,
//! the `Redeliver`/`DeadLetter` pacing asymmetry).
//!
//! Self-contained per repo convention: the LocalStack container/fixture below
//! is its own copy, mirroring `tests/sns_sqs_integration.rs`'s pattern rather
//! than importing it (integration test binaries cannot share code without a
//! separate support crate this repo does not have).
//!
//! One deliberate deviation from `tests/sns_sqs_integration.rs`'s fixture:
//! that file's `TestBroker::start()` requires `LOCALSTACK_AUTH_TOKEN` and
//! panics without it. `testcontainers_modules::localstack::LocalStack`
//! actually pins the Community-edition image (`localstack/localstack:4.5`),
//! and SNS/SQS — including redrive policies and FIFO — are core (non-Pro)
//! services on it, so no token is required for anything this file exercises.
//! This fixture reads `LOCALSTACK_AUTH_TOKEN` opportunistically (forwarded to
//! the container if set, e.g. in CI) but does not require it, so these tests
//! also run in environments without a LocalStack Pro credential.
//!
//! Run with: `cargo nextest run --features pub-aws-sns,aws-sns-sqs,audit,metrics,sbe --test sns_sqs_batch`

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_sqs::types::MessageSystemAttributeName;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::error::ShoveError;
use shove::handler::BatchMessageHandler;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::publisher::Publisher;
use shove::sns::{SnsClient, SnsConfig};
use shove::topic::{NotSequenced, Topic};
use shove::topology::{QueueTopology, SequenceFailure, TopologyBuilder};
use shove::{BatchConsumerOptions, Sqs, define_topic};

use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;

const TIMEOUT: Duration = Duration::from_secs(20);

/// Poll `long_enough` until it reports true or `timeout` elapses, waking on
/// every `signal` notification in between rather than busy-polling. Mirrors
/// `tests/inmemory_batch.rs`'s helper of the same name.
async fn wait_for(
    signal: &Notify,
    timeout: Duration,
    mut long_enough: impl FnMut() -> bool,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if long_enough() {
            return true;
        }
        tokio::select! {
            _ = signal.notified() => {}
            _ = tokio::time::sleep_until(deadline) => {
                return long_enough();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test harness — self-contained LocalStack fixture (see module doc)
// ---------------------------------------------------------------------------

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

        // Forwarded if present (e.g. CI), but not required — see the module
        // doc's deviation note: SNS/SQS are core services on the pinned
        // Community-edition image.
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

    async fn sqs_client(&self) -> aws_sdk_sqs::Client {
        let aws_config = aws_config::from_env()
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(&self.endpoint_url)
            .load()
            .await;
        aws_sdk_sqs::Client::new(&aws_config)
    }
}

struct TestSetup {
    #[allow(dead_code)]
    sns_client: SnsClient,
    broker: Broker<Sqs>,
    publisher: Publisher<Sqs>,
}

impl TestSetup {
    async fn new(broker: &TestBroker) -> Self {
        let sns_client = SnsClient::new(&broker.sns_config())
            .await
            .expect("failed to create SNS client");
        let broker = Broker::<Sqs>::from_client(sns_client.clone());
        let publisher = broker.publisher().await.expect("publisher construction");
        Self {
            sns_client,
            broker,
            publisher,
        }
    }

    async fn declare<T: Topic>(&self) {
        self.broker
            .topology()
            .declare::<T>()
            .await
            .expect("topology declaration should succeed");
    }
}

// ---------------------------------------------------------------------------
// Message type + topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BatchMessage {
    seq: u32,
}

define_topic!(
    SizeTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-size").build()
);
define_topic!(
    AgeTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-age").build()
);
define_topic!(
    AckTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-ack").build()
);
define_topic!(
    RetryTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-retry").build()
);
define_topic!(
    DeferTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-defer").build()
);
define_topic!(
    RejectMechanicsTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-reject-mech").dlq().build()
);
define_topic!(
    RejectRedriveTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-reject-redrive")
        .dlq()
        .build()
);
define_topic!(
    PanicTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-panic").build()
);
define_topic!(
    TimeoutDefaultTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-timeout-default").build()
);
define_topic!(
    TimeoutRejectTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-timeout-reject")
        .dlq()
        .build()
);
define_topic!(
    CapRunsTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-cap-runs").build()
);
define_topic!(
    ShutdownTopic,
    BatchMessage,
    TopologyBuilder::new("sqs-batch-shutdown").build()
);

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

// ---------------------------------------------------------------------------
// Recording batch handler — records every flush, returns scripted outcomes
// ---------------------------------------------------------------------------

/// `(seq, delivery_count, redelivered, has_x_retry_count)` for one message in
/// a recorded batch. `has_x_retry_count` is read from `meta.headers` — batch
/// `Retry`/`Defer` redelivery is a visibility reset
/// (`ChangeMessageVisibilityBatch`), never the single-message path's
/// delete+re-send, so it must never appear.
type SeqDeliveryRedelivered = (u32, Option<u32>, bool, bool);

#[derive(Clone)]
struct RecordingBatchHandler {
    batches: Arc<Mutex<Vec<Vec<SeqDeliveryRedelivered>>>>,
    scripted: Arc<Mutex<VecDeque<Outcome>>>,
    signal: Arc<Notify>,
}

impl RecordingBatchHandler {
    fn new() -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            scripted: Arc::new(Mutex::new(VecDeque::new())),
            signal: Arc::new(Notify::new()),
        }
    }

    fn scripting(self, outcomes: impl IntoIterator<Item = Outcome>) -> Self {
        *self.scripted.lock().unwrap() = outcomes.into_iter().collect();
        self
    }

    fn record(&self, batch: &[(BatchMessage, MessageMetadata)]) -> Outcome {
        self.batches.lock().unwrap().push(
            batch
                .iter()
                .map(|(m, meta)| {
                    (
                        m.seq,
                        meta.delivery_count,
                        meta.redelivered,
                        meta.headers.contains_key("x-retry-count"),
                    )
                })
                .collect(),
        );
        let outcome = self
            .scripted
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Outcome::Ack);
        self.signal.notify_waiters();
        outcome
    }

    fn batches(&self) -> Vec<Vec<SeqDeliveryRedelivered>> {
        self.batches.lock().unwrap().clone()
    }

    fn seqs(&self) -> Vec<u32> {
        self.batches()
            .into_iter()
            .flatten()
            .map(|(seq, _, _, _)| seq)
            .collect()
    }

    async fn wait_for_batches(&self, n: usize, timeout: Duration) -> bool {
        wait_for(&self.signal, timeout, || {
            self.batches.lock().unwrap().len() >= n
        })
        .await
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

impl_recording_for!(
    SizeTopic,
    AgeTopic,
    AckTopic,
    RetryTopic,
    DeferTopic,
    RejectMechanicsTopic,
    RejectRedriveTopic,
    TimeoutRejectTopic,
    CapRunsTopic,
    ShutdownTopic,
);

// ---------------------------------------------------------------------------
// 1. Size-triggered flush
// ---------------------------------------------------------------------------

#[tokio::test]
async fn size_triggered_flush_produces_the_configured_chunks() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<SizeTopic>().await;

    publish_seqs::<SizeTopic>(&setup.publisher, 0..10).await;

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<SizeTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(4)
                        .with_max_batch_age(Duration::from_millis(500))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(
        handler.wait_for_batches(3, Duration::from_secs(30)).await,
        "expected 3 flushes (4/4/2), got {:?}",
        handler.batches()
    );

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let mut sizes: Vec<usize> = handler.batches().iter().map(Vec::len).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![2, 4, 4], "batch sizes should be 4/4/2");

    let mut seqs = handler.seqs();
    seqs.sort_unstable();
    assert_eq!(seqs, (0..10).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// 2. Age-triggered flush
// ---------------------------------------------------------------------------

#[tokio::test]
async fn age_triggered_flush_fires_before_size_cap() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<AgeTopic>().await;

    publish_seqs::<AgeTopic>(&setup.publisher, 0..3).await;

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<AgeTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(10)
                        .with_max_batch_age(Duration::from_millis(300))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(
        handler.wait_for_batches(1, Duration::from_secs(15)).await,
        "expected one age-triggered flush, got {:?}",
        handler.batches()
    );

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let batches = handler.batches();
    assert_eq!(batches.len(), 1, "batches: {batches:?}");
    let mut seqs = handler.seqs();
    seqs.sort_unstable();
    assert_eq!(seqs, vec![0, 1, 2]);
}

// ---------------------------------------------------------------------------
// 3. Ack settles the batch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ack_settles_the_batch() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<AckTopic>().await;

    publish_seqs::<AckTopic>(&setup.publisher, 0..3).await;

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<AckTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(5))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    // Give the ack a moment to land before checking the queue is drained.
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let mut seqs = handler.seqs();
    seqs.sort_unstable();
    assert_eq!(seqs, vec![0, 1, 2]);

    // Queue must be drained: nothing left to receive, even with a
    // visibility_timeout of 0 (would surface anything not truly deleted).
    let sqs = broker.sqs_client().await;
    let url = sqs_queue_url(&sqs, "sqs-batch-ack").await;
    let remaining = sqs
        .receive_message()
        .queue_url(&url)
        .max_number_of_messages(10)
        .wait_time_seconds(1)
        .visibility_timeout(0)
        .send()
        .await
        .expect("receive should succeed");
    assert!(
        remaining.messages().is_empty(),
        "queue should be drained after Ack, found {} messages",
        remaining.messages().len()
    );
}

/// Look up a queue URL by name via the raw SQS client — used by assertions
/// that need to peek at broker-side state the public API does not expose.
async fn sqs_queue_url(sqs: &aws_sdk_sqs::Client, name: &str) -> String {
    sqs.get_queue_url()
        .queue_name(name)
        .send()
        .await
        .unwrap_or_else(|e| panic!("get_queue_url({name}) failed: {e}"))
        .queue_url()
        .unwrap_or_else(|| panic!("get_queue_url({name}) returned no URL"))
        .to_string()
}

// ---------------------------------------------------------------------------
// 4. Retry redelivers the whole batch, then Ack settles it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retry_redelivers_the_whole_batch_then_acks() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<RetryTopic>().await;

    publish_seqs::<RetryTopic>(&setup.publisher, 0..2).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry]);
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<RetryTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(5))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(
        handler.wait_for_batches(2, Duration::from_secs(30)).await,
        "expected retry flush then ack flush, got {:?}",
        handler.batches()
    );
    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let batches = handler.batches();
    assert_eq!(batches.len(), 2, "batches: {batches:?}");

    // Round 1: first delivery — delivery_count 1 (SQS's ApproximateReceiveCount
    // counts the current receive), not marked redelivered, no x-retry-count.
    let mut round1 = batches[0].clone();
    round1.sort_unstable_by_key(|(seq, _, _, _)| *seq);
    for (_, delivery_count, redelivered, has_x_retry_count) in &round1 {
        assert_eq!(*delivery_count, Some(1));
        assert!(!redelivered);
        assert!(!has_x_retry_count);
    }

    // Round 2: the same messages come back with delivery_count incremented
    // and redelivered=true — a visibility reset, not a republish. The
    // republish-only `x-retry-count` message attribute must still be absent:
    // asserting that here (not just `redelivered`/`delivery_count`, which
    // `ApproximateReceiveCount` alone could satisfy) is what pins "this went
    // through ChangeMessageVisibilityBatch, never route_retry's delete+re-send".
    let mut round2 = batches[1].clone();
    round2.sort_unstable_by_key(|(seq, _, _, _)| *seq);
    for (_, delivery_count, redelivered, has_x_retry_count) in &round2 {
        assert_eq!(*delivery_count, Some(2));
        assert!(*redelivered);
        assert!(
            !has_x_retry_count,
            "batch redelivery must never set x-retry-count (that would mean a republish)"
        );
    }

    let seqs1: Vec<u32> = round1.iter().map(|(s, _, _, _)| *s).collect();
    let seqs2: Vec<u32> = round2.iter().map(|(s, _, _, _)| *s).collect();
    assert_eq!(seqs1, vec![0, 1]);
    assert_eq!(seqs2, vec![0, 1], "the SAME message set must redeliver");
}

// ---------------------------------------------------------------------------
// 5. Defer settles like Retry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn defer_redelivers_like_retry() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<DeferTopic>().await;

    publish_seqs::<DeferTopic>(&setup.publisher, 0..2).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Defer]);
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<DeferTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(5))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(
        handler.wait_for_batches(2, Duration::from_secs(30)).await,
        "expected defer flush then ack flush, got {:?}",
        handler.batches()
    );
    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let batches = handler.batches();
    assert_eq!(batches.len(), 2, "batches: {batches:?}");
    let mut round2 = batches[1].clone();
    round2.sort_unstable_by_key(|(seq, _, _, _)| *seq);
    for (_, delivery_count, redelivered, has_x_retry_count) in &round2 {
        assert_eq!(*delivery_count, Some(2));
        assert!(*redelivered);
        assert!(!has_x_retry_count);
    }
}

// ---------------------------------------------------------------------------
// 6. Reject — mechanics only (fast)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reject_mechanics_leave_messages_on_the_queue() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<RejectMechanicsTopic>().await;

    publish_seqs::<RejectMechanicsTopic>(&setup.publisher, 0..2).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Reject]);
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<RejectMechanicsTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(5))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    // Stop the moment the first (only scripted) flush has happened — before
    // the DeadLetter arm's backoff sleep elapses and the consumer re-receives
    // the same batch. `shutdown.cancel()` races that sleep and cuts it short
    // (see `flush_sqs_batch`), and the loop's top-of-iteration shutdown check
    // returns before any second receive.
    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let sqs = broker.sqs_client().await;
    let url = sqs_queue_url(&sqs, "sqs-batch-reject-mech").await;

    // Nothing was deleted: both messages are still receivable. Peeking with
    // visibility_timeout=0 also proves ApproximateReceiveCount moved past 1
    // (the batch consumer's own receive already counted once, and reject
    // does not delete).
    let peek = sqs
        .receive_message()
        .queue_url(&url)
        .max_number_of_messages(10)
        .wait_time_seconds(2)
        .visibility_timeout(0)
        .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
        .send()
        .await
        .expect("receive should succeed");
    let messages = peek.messages();
    assert_eq!(
        messages.len(),
        2,
        "both rejected messages should still be on the queue"
    );
    for msg in messages {
        let arc: u32 = msg
            .attributes()
            .and_then(|a| a.get(&MessageSystemAttributeName::ApproximateReceiveCount))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        assert!(
            arc >= 1,
            "ApproximateReceiveCount should have incremented, got {arc}"
        );
    }
}

// ---------------------------------------------------------------------------
// 7. Reject — end-to-end redrive (SLOW: ~90-270s, generous timeout)
// ---------------------------------------------------------------------------

/// A handler that always rejects, recording the wall-clock instant of every
/// invocation so the test can assert the redelivery backoff actually
/// escalates (rather than spinning at full API rate) via the growing gaps
/// between invocations — the count itself is a vacuous assertion once redrive
/// caps delivery at `maxReceiveCount`.
#[derive(Clone)]
struct AlwaysRejectTimestamped {
    invocations: Arc<Mutex<Vec<Instant>>>,
    signal: Arc<Notify>,
}

impl AlwaysRejectTimestamped {
    fn new() -> Self {
        Self {
            invocations: Arc::new(Mutex::new(Vec::new())),
            signal: Arc::new(Notify::new()),
        }
    }

    fn count(&self) -> usize {
        self.invocations.lock().unwrap().len()
    }

    fn gaps(&self) -> Vec<Duration> {
        let invocations = self.invocations.lock().unwrap();
        invocations
            .windows(2)
            .map(|w| w[1].saturating_duration_since(w[0]))
            .collect()
    }
}

impl BatchMessageHandler<RejectRedriveTopic> for AlwaysRejectTimestamped {
    type Context = ();
    async fn handle_batch(
        &self,
        _messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        self.invocations.lock().unwrap().push(Instant::now());
        self.signal.notify_waiters();
        Outcome::Reject
    }
}

#[tokio::test]
async fn reject_redrive_reaches_the_dlq_eventually() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<RejectRedriveTopic>().await;

    publish_seqs::<RejectRedriveTopic>(&setup.publisher, 0..2).await;

    let handler = AlwaysRejectTimestamped::new();
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<RejectRedriveTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(5))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    let sqs = broker.sqs_client().await;
    let dlq_url = sqs_queue_url(&sqs, "sqs-batch-reject-redrive-dlq").await;

    // The library's default DLQ redrive policy is `maxReceiveCount = 10`
    // (`DEFAULT_MAX_RECEIVE_COUNT`), and the un-reset backoff means ten
    // consecutive DeadLetter flushes sleep roughly 1+2+4+8+16+30*5 ≈ 181s
    // (90-270s with the shared backoff's ±50% jitter) before AWS redrive
    // moves both messages to the DLQ.
    let deadline = Instant::now() + Duration::from_secs(400);
    let dlq_messages = loop {
        let received = sqs
            .receive_message()
            .queue_url(&dlq_url)
            .max_number_of_messages(10)
            .wait_time_seconds(2)
            .send()
            .await
            .expect("DLQ receive should succeed");
        let messages = received.messages().to_vec();
        if messages.len() >= 2 {
            break messages;
        }
        assert!(
            Instant::now() < deadline,
            "messages did not reach the DLQ within the generous redrive budget; \
             handler invocations so far: {}",
            handler.count()
        );
    };
    assert_eq!(dlq_messages.len(), 2, "both messages should reach the DLQ");

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    // Pacing proof: the gaps between invocations must grow — a flat,
    // full-API-rate spin would show ~0s gaps throughout instead.
    let gaps = handler.gaps();
    assert!(
        gaps.len() >= 3,
        "expected several reject flushes before redrive, got {} gaps ({} invocations)",
        gaps.len(),
        handler.count()
    );
    let first = gaps[0];
    let last = *gaps.last().unwrap();
    assert!(
        last > first * 3,
        "backoff should escalate: first gap {first:?}, last gap {last:?}, all gaps {gaps:?}"
    );

    let snapshot = snapshotter.snapshot().into_hashmap();
    let failed: u64 = snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_failed_total")
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "topic" && l.value() == "sqs-batch-reject-redrive")
        })
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "reason" && l.value() == "rejected")
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(c) => *c,
            other => panic!("shove_messages_failed_total is not a counter: {other:?}"),
        })
        .sum();
    assert!(
        failed >= 2,
        "messages_failed_total{{reason=rejected}} should be at least 2, got {failed}"
    );
}

// ---------------------------------------------------------------------------
// 8. Panic in flush redelivers the whole batch, then Ack settles it
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct PanicOnceThenAck {
    calls: Arc<AtomicUsize>,
    signal: Arc<Notify>,
    seen: Arc<Mutex<Vec<Vec<u32>>>>,
}

impl PanicOnceThenAck {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            signal: Arc::new(Notify::new()),
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn wait_for_calls(&self, n: usize, timeout: Duration) -> bool {
        wait_for(&self.signal, timeout, || {
            self.calls.load(Ordering::SeqCst) >= n
        })
        .await
    }
}

impl BatchMessageHandler<PanicTopic> for PanicOnceThenAck {
    type Context = ();
    async fn handle_batch(
        &self,
        messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let mut seqs: Vec<u32> = messages.iter().map(|(m, _)| m.seq).collect();
        seqs.sort_unstable();
        self.seen.lock().unwrap().push(seqs);
        self.signal.notify_waiters();
        if call == 0 {
            panic!("flush blew up on purpose");
        }
        Outcome::Ack
    }
}

#[tokio::test]
async fn panic_in_flush_redelivers_the_whole_batch_then_acks() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<PanicTopic>().await;

    publish_seqs::<PanicTopic>(&setup.publisher, 0..2).await;

    let handler = PanicOnceThenAck::new();
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<PanicTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(5))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(
        handler.wait_for_calls(2, Duration::from_secs(30)).await,
        "expected a panicking call then an acking call"
    );
    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();

    let seen = handler.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], vec![0, 1]);
    assert_eq!(seen[1], vec![0, 1], "round 2 must see the SAME message set");
}

// ---------------------------------------------------------------------------
// 9. Handler timeout
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct HangingHandler {
    calls: Arc<AtomicUsize>,
    signal: Arc<Notify>,
}

impl HangingHandler {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            signal: Arc::new(Notify::new()),
        }
    }

    async fn wait_for_calls(&self, n: usize, timeout: Duration) -> bool {
        wait_for(&self.signal, timeout, || {
            self.calls.load(Ordering::SeqCst) >= n
        })
        .await
    }
}

impl BatchMessageHandler<TimeoutDefaultTopic> for HangingHandler {
    type Context = ();
    async fn handle_batch(
        &self,
        _messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.signal.notify_waiters();
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Outcome::Ack
    }
}

#[tokio::test]
async fn handler_timeout_defaults_to_retry_redelivery() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<TimeoutDefaultTopic>().await;

    publish_seqs::<TimeoutDefaultTopic>(&setup.publisher, 0..1).await;

    let handler = HangingHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<TimeoutDefaultTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(1)
                        .with_max_batch_age(Duration::from_secs(5))
                        .with_handler_timeout(Duration::from_millis(300))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    // The default timeout outcome (Retry) redelivers, so the handler is
    // invoked at least twice within the budget.
    assert!(
        handler.wait_for_calls(2, Duration::from_secs(30)).await,
        "expected at least 2 invocations from timeout-triggered redelivery, got {}",
        handler.calls.load(Ordering::SeqCst)
    );

    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();
}

// Reject-mechanics-only for a timeout outcome of Reject: proves the wiring
// reaches `BatchSettlement::DeadLetter`, without paying the full redrive
// budget a second time (see `reject_redrive_reaches_the_dlq_eventually`).
#[tokio::test]
async fn handler_timeout_outcome_reject_settles_as_dead_letter() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<TimeoutRejectTopic>().await;

    publish_seqs::<TimeoutRejectTopic>(&setup.publisher, 0..1).await;

    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<TimeoutRejectTopic, _>(
                    HangingReject,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(1)
                        .with_max_batch_age(Duration::from_secs(5))
                        .with_handler_timeout(Duration::from_millis(300))
                        .with_handler_timeout_outcome(Outcome::Reject)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    tokio::time::sleep(Duration::from_millis(800)).await;
    shutdown.cancel();
    handle
        .await
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly");

    let sqs = broker.sqs_client().await;
    let url = sqs_queue_url(&sqs, "sqs-batch-timeout-reject").await;
    let peek = sqs
        .receive_message()
        .queue_url(&url)
        .max_number_of_messages(10)
        .wait_time_seconds(1)
        .visibility_timeout(0)
        .send()
        .await
        .expect("receive should succeed");
    assert_eq!(
        peek.messages().len(),
        1,
        "the message should still be on the queue (rejected, not deleted)"
    );
}

struct HangingReject;
impl BatchMessageHandler<TimeoutRejectTopic> for HangingReject {
    type Context = ();
    async fn handle_batch(
        &self,
        _messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// 10. Cap rejection end to end
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cap_rejection_end_to_end() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<CapRunsTopic>().await;

    let consumer = setup.broker.batch_consumer();

    // Default options (max_batch_size = 500) must be rejected.
    let err = consumer
        .run::<CapRunsTopic, _>(
            RecordingBatchHandler::new(),
            (),
            BatchConsumerOptions::new().with_shutdown(CancellationToken::new()),
        )
        .await
        .expect_err("default options (500) exceed the SQS cap");
    match err {
        ShoveError::Validation(msg) => {
            assert!(msg.contains("500"), "message: {msg}");
            assert!(msg.contains("10"), "message: {msg}");
        }
        other => panic!("expected ShoveError::Validation, got {other:?}"),
    }

    // 11 is also rejected.
    let err = consumer
        .run::<CapRunsTopic, _>(
            RecordingBatchHandler::new(),
            (),
            BatchConsumerOptions::new()
                .with_max_batch_size(11)
                .with_shutdown(CancellationToken::new()),
        )
        .await
        .expect_err("11 exceeds the SQS cap");
    assert!(matches!(err, ShoveError::Validation(_)));

    // 10 runs normally.
    publish_seqs::<CapRunsTopic>(&setup.publisher, 0..1).await;
    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<CapRunsTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(10)
                        .with_max_batch_age(Duration::from_secs(5))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });
    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    shutdown.cancel();
    handle
        .await
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly");
}

// ---------------------------------------------------------------------------
// 11. Runtime sequencing guard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runtime_guard_rejects_a_topic_that_declares_sequencing() {
    struct LiesAboutSequencing;
    impl Topic for LiesAboutSequencing {
        type Message = BatchMessage;
        type Codec = shove::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("sqs-batch-guard-test")
                    .sequenced(SequenceFailure::FailAll)
                    .routing_shards(1)
                    .hold_queue(Duration::from_secs(5))
                    .dlq()
                    .build()
            })
        }
    }
    impl NotSequenced for LiesAboutSequencing {}

    struct NoopHandler;
    impl BatchMessageHandler<LiesAboutSequencing> for NoopHandler {
        type Context = ();
        async fn handle_batch(
            &self,
            _messages: Vec<(BatchMessage, MessageMetadata)>,
            _: &(),
        ) -> Outcome {
            Outcome::Ack
        }
    }

    // No `declare` needed: the guard fires before any queue lookup.
    let broker = TestBroker::start().await;
    let sns_client = SnsClient::new(&broker.sns_config())
        .await
        .expect("failed to create SNS client");
    let broker = Broker::<Sqs>::from_client(sns_client);
    let consumer = broker.batch_consumer();

    let err = consumer
        .run::<LiesAboutSequencing, _>(
            NoopHandler,
            (),
            BatchConsumerOptions::new().with_shutdown(CancellationToken::new()),
        )
        .await
        .expect_err("a sequenced topology must be refused");

    match err {
        ShoveError::Topology(msg) => {
            assert!(msg.contains("sqs-batch-guard-test"));
            assert!(msg.contains("run_fifo"));
        }
        other => panic!("expected ShoveError::Topology, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 12. Clean shutdown flushes the partial batch exactly once
// ---------------------------------------------------------------------------

#[tokio::test]
async fn clean_shutdown_flushes_the_pending_partial_batch() {
    let broker = TestBroker::start().await;
    let setup = TestSetup::new(&broker).await;
    setup.declare::<ShutdownTopic>().await;

    publish_seqs::<ShutdownTopic>(&setup.publisher, 0..2).await;

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = setup.broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<ShutdownTopic, _>(
                    handler,
                    (),
                    // Neither trigger can fire on its own.
                    BatchConsumerOptions::new()
                        .with_max_batch_size(10)
                        .with_max_batch_age(Duration::from_secs(3600))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    // Give the consumer time to have received and buffered both messages.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        handler.batches().is_empty(),
        "neither trigger should have fired before shutdown, got {:?}",
        handler.batches()
    );

    shutdown.cancel();
    handle
        .await
        .expect("consumer task should not panic")
        .expect("consumer should exit cleanly with Ok");

    let mut seqs = handler.seqs();
    seqs.sort_unstable();
    assert_eq!(
        seqs,
        vec![0, 1],
        "the partial batch must flush exactly once on shutdown"
    );
    assert_eq!(handler.batches().len(), 1);

    // The flush's default outcome is Ack (nothing scripted), so the queue
    // should drain rather than leaving the pair invisible-then-redelivered.
    let sqs = broker.sqs_client().await;
    let url = sqs_queue_url(&sqs, "sqs-batch-shutdown").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let remaining = sqs
        .receive_message()
        .queue_url(&url)
        .max_number_of_messages(10)
        .wait_time_seconds(1)
        .visibility_timeout(0)
        .send()
        .await
        .expect("receive should succeed");
    assert!(
        remaining.messages().is_empty(),
        "queue should drain after the shutdown-triggered Ack flush"
    );
}
