//! Integration tests for the Kafka batch consumer (`KafkaConsumer::run_batch`).
//!
//! These cover the parts of `run_batch`/`flush_batch` that only a real broker
//! exercises: the size- and age-triggered flush boundaries, the offset commit
//! on `Ack`, the seek-and-redeliver path on every other outcome, the
//! drop-from-batch arms for oversized/undeserializable payloads, and the
//! flush-on-shutdown drain.

#![cfg(feature = "kafka")]

use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::handler::BatchMessageHandler;
use shove::kafka::{BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::TopologyBuilder;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const TIMEOUT: Duration = Duration::from_secs(60);

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
}

shove::define_topic!(
    SizeTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-size").build()
);

shove::define_topic!(
    AgeTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-age").build()
);

shove::define_topic!(
    RedeliverTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-redeliver").build()
);

shove::define_topic!(
    CommitTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-commit").build()
);

shove::define_topic!(
    BadPayloadTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-bad-payload").build()
);

shove::define_topic!(
    OversizeTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-oversize").dlq().build()
);

shove::define_topic!(
    ShutdownTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-shutdown").build()
);

shove::define_topic!(
    PoisonOnlyTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-poison-only").build()
);

shove::define_topic!(
    BatchDlqTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-dlq").dlq().build()
);

shove::define_topic!(
    RejectTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-reject").dlq().build()
);

shove::define_topic!(
    PoisonRetryTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-poison-retry")
        .dlq()
        .build()
);

shove::define_topic!(
    PoisonFloodTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-poison-flood")
        .dlq()
        .build()
);

shove::define_topic!(
    PanicTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-panic").build()
);

shove::define_topic!(
    HandlerTimeoutTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-handler-timeout").build()
);

shove::define_topic!(
    HungShutdownTopic,
    BatchMessage,
    TopologyBuilder::new("kafka-batch-hung-shutdown").build()
);

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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
        let brokers = format!("127.0.0.1:{port}");
        let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&brokers), 10)
            .await
            .expect("failed to connect to Kafka");
        Self {
            _container: container,
            client,
            brokers,
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
///
/// The batch DLQ test can't use `run_dlq` for this: the whole point is that the
/// payload failed to decode as `T::Message`, so a typed DLQ consumer would fail
/// on it too. This reads the bytes back as bytes.
async fn drain_raw(brokers: &str, topic: &str, expected: usize) -> Vec<Vec<u8>> {
    drain_raw_within(brokers, topic, expected, TIMEOUT).await
}

/// `drain_raw` with an explicit deadline, for assertions about what *should
/// not* be there: waiting the full `TIMEOUT` to prove a second copy never
/// arrives would add a minute to the suite.
async fn drain_raw_within(
    brokers: &str,
    topic: &str,
    expected: usize,
    timeout: Duration,
) -> Vec<Vec<u8>> {
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
    let _ = tokio::time::timeout(timeout, async {
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
// Recording batch handler
// ---------------------------------------------------------------------------

/// Records the `seq` of every message in every batch it is handed, and
/// returns outcomes from a scripted queue (defaulting to `Ack` once the
/// script is exhausted).
#[derive(Clone)]
struct RecordingBatchHandler {
    batches: Arc<Mutex<Vec<Vec<u32>>>>,
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

    /// Outcomes returned for the first N flushes, in order.
    fn scripting(self, outcomes: impl IntoIterator<Item = Outcome>) -> Self {
        *self.scripted.lock().unwrap() = outcomes.into_iter().collect();
        self
    }

    fn record(&self, batch: &[(BatchMessage, MessageMetadata)]) -> Outcome {
        self.batches
            .lock()
            .unwrap()
            .push(batch.iter().map(|(m, _)| m.seq).collect());
        let outcome = self
            .scripted
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Outcome::Ack);
        self.signal.notify_waiters();
        outcome
    }

    fn batches(&self) -> Vec<Vec<u32>> {
        self.batches.lock().unwrap().clone()
    }

    /// All `seq` values seen across every batch, flattened.
    fn seen(&self) -> Vec<u32> {
        self.batches().into_iter().flatten().collect()
    }

    /// Wait until at least `n` batches have been flushed.
    async fn wait_for_batches(&self, n: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.batches.lock().unwrap().len() >= n {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.batches.lock().unwrap().len() >= n;
                }
            }
        }
    }
}

/// One `BatchMessageHandler` impl per topic — the trait is parameterized on
/// the topic, so a single blanket impl is not possible.
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
    RedeliverTopic,
    CommitTopic,
    BadPayloadTopic,
    OversizeTopic,
    ShutdownTopic,
    PoisonOnlyTopic,
    BatchDlqTopic,
    RejectTopic,
    PoisonRetryTopic,
    PoisonFloodTopic,
);

// ---------------------------------------------------------------------------
// Misbehaving batch handler
// ---------------------------------------------------------------------------

/// What the handler does *instead* of returning an outcome.
#[derive(Clone, Copy)]
enum Misbehaviour {
    /// Panic on the first flush, behave on every one after.
    PanicOnce,
    /// Outlast any sane handler timeout on the first flush, behave after.
    HangOnce,
    /// Never return, on any flush.
    HangForever,
}

/// A handler that panics or hangs, to prove `run_batch` survives both.
///
/// Neither is hypothetical for a DB sink: an `unwrap` on a poisoned lock or a
/// row that violates an assumption panics, and a driver waiting on a lost
/// connection hangs. Before batch invocation got `catch_unwind` + timeout, the
/// first killed the consumer task outright and the second wedged it — along
/// with offset commits, rebalance handling, and shutdown — forever.
#[derive(Clone)]
struct MisbehavingBatchHandler {
    mode: Misbehaviour,
    /// The `seq` list handed to each flush, in order. Recorded *before*
    /// misbehaving, so a panicking or hanging call still shows up.
    calls: Arc<Mutex<Vec<Vec<u32>>>>,
    signal: Arc<Notify>,
}

impl MisbehavingBatchHandler {
    fn new(mode: Misbehaviour) -> Self {
        Self {
            mode,
            calls: Arc::new(Mutex::new(Vec::new())),
            signal: Arc::new(Notify::new()),
        }
    }

    async fn act(&self, batch: &[(BatchMessage, MessageMetadata)]) -> Outcome {
        let nth = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(batch.iter().map(|(m, _)| m.seq).collect());
            calls.len()
        };
        self.signal.notify_waiters();

        let misbehave = match self.mode {
            Misbehaviour::HangForever => true,
            Misbehaviour::PanicOnce | Misbehaviour::HangOnce => nth == 1,
        };
        if !misbehave {
            return Outcome::Ack;
        }
        match self.mode {
            Misbehaviour::PanicOnce => panic!("batch handler panicked on flush {nth}"),
            // Longer than any timeout the tests configure and longer than
            // `TIMEOUT`, so a test that hangs fails on its own deadline rather
            // than on this sleep expiring.
            Misbehaviour::HangOnce | Misbehaviour::HangForever => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Outcome::Ack
            }
        }
    }

    fn calls(&self) -> Vec<Vec<u32>> {
        self.calls.lock().unwrap().clone()
    }

    async fn wait_for_calls(&self, n: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.calls.lock().unwrap().len() >= n {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.calls.lock().unwrap().len() >= n;
                }
            }
        }
    }
}

macro_rules! impl_misbehaving_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl BatchMessageHandler<$topic> for MisbehavingBatchHandler {
                type Context = ();
                async fn handle_batch(
                    &self,
                    messages: Vec<(BatchMessage, MessageMetadata)>,
                    _: &(),
                ) -> Outcome {
                    self.act(&messages).await
                }
            }
        )*
    };
}

impl_misbehaving_for!(PanicTopic, HandlerTimeoutTopic, HungShutdownTopic);

/// The test topics have 8 partitions, so a batch's *order* follows poll order
/// across partitions rather than publish order — compare batches as sets.
fn sorted(batch: &[u32]) -> Vec<u32> {
    let mut v = batch.to_vec();
    v.sort_unstable();
    v
}

async fn publish_seq<T>(broker: &Broker<Kafka>, range: std::ops::Range<u32>)
where
    T: Topic<Message = BatchMessage>,
{
    let publisher = broker.publisher().await.unwrap();
    for seq in range {
        publisher
            .publish::<T>(&BatchMessage::new(seq))
            .await
            .expect("publish should succeed");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A batch flushes as soon as it reaches `max_batch_size`, and no message is
/// lost or duplicated across the resulting batches.
#[tokio::test]
async fn batch_flushes_on_max_batch_size() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<SizeTopic>().await.unwrap();
    publish_seq::<SizeTopic>(&broker, 0..10).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<SizeTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(5)
                    // Long enough that only the size trigger can fire.
                    .with_max_batch_age(Duration::from_secs(30))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_batches(2, TIMEOUT).await,
        "expected two size-triggered batches, got {:?}",
        handler.batches()
    );
    shutdown.cancel();
    handle.await.unwrap().ok();

    let batches = handler.batches();
    assert!(
        batches.iter().all(|b| b.len() == 5),
        "every batch should hold exactly max_batch_size messages, got {batches:?}"
    );
    let mut seen = handler.seen();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen, (0..10).collect::<Vec<_>>());
    broker.close().await;
}

/// A batch below `max_batch_size` still flushes once `max_batch_age` elapses.
#[tokio::test]
async fn batch_flushes_on_max_batch_age() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<AgeTopic>().await.unwrap();
    publish_seq::<AgeTopic>(&broker, 0..3).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<AgeTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    // Far above the 3 published, so only the age trigger fires.
                    .with_max_batch_size(1000)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "age trigger should flush a partial batch"
    );
    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(seen, vec![0, 1, 2]);
    broker.close().await;
}

/// A non-`Ack` outcome seeks every partition back to the batch's start
/// offset, so the *whole* batch is redelivered rather than skipped.
#[tokio::test]
async fn non_ack_outcome_redelivers_the_whole_batch() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<RedeliverTopic>().await.unwrap();
    publish_seq::<RedeliverTopic>(&broker, 0..3).await;

    // First flush retries (seek back), second acks.
    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry]);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<RedeliverTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_secs(30))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_batches(2, TIMEOUT).await,
        "the retried batch should be redelivered, got {:?}",
        handler.batches()
    );
    shutdown.cancel();
    handle.await.unwrap().ok();

    let batches = handler.batches();
    assert_eq!(
        sorted(&batches[0]),
        sorted(&batches[1]),
        "redelivery should replay the identical batch, got {batches:?}"
    );
    assert_eq!(sorted(&batches[0]), vec![0, 1, 2]);
    broker.close().await;
}

/// A panicking flush is a redelivery, not the end of the consumer.
///
/// The regression: `flush_batch` awaited `handle_batch` directly, so the panic
/// unwound the `run_batch` task itself. The consumer stopped consuming and
/// stayed stopped — the single-message path has turned the same panic into
/// `Outcome::Retry` since forever. Here the batch must come back intact and
/// the second flush must be able to ack it.
#[tokio::test]
async fn a_panicking_flush_is_redelivered_and_the_consumer_survives() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<PanicTopic>().await.unwrap();
    publish_seq::<PanicTopic>(&broker, 0..3).await;

    let handler = MisbehavingBatchHandler::new(Misbehaviour::PanicOnce);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<PanicTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_secs(30))
                    .with_shutdown(sc),
            )
            .await
    });

    let got_two = handler.wait_for_calls(2, TIMEOUT).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let calls = handler.calls();
    assert!(
        got_two,
        "the panic must be caught and the batch redelivered; the handler was \
         called {} time(s): {calls:?}",
        calls.len()
    );
    assert_eq!(
        sorted(&calls[0]),
        vec![0, 1, 2],
        "first flush sees the whole batch, then panics"
    );
    assert_eq!(
        sorted(&calls[1]),
        vec![0, 1, 2],
        "the same batch comes back — the panic acked nothing"
    );
    broker.close().await;
}

/// A flush that outlasts `handler_timeout` is abandoned and redelivered.
///
/// Without the timeout the batch loop — a single task — sits inside the hung
/// future indefinitely: no commits, no rebalance handling, no shutdown. The
/// timeout has to end the flush *and* leave the batch un-acked so it returns.
#[tokio::test]
async fn a_flush_that_outlasts_the_handler_timeout_is_redelivered() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<HandlerTimeoutTopic>()
        .await
        .unwrap();
    publish_seq::<HandlerTimeoutTopic>(&broker, 0..3).await;

    let handler = MisbehavingBatchHandler::new(Misbehaviour::HangOnce);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<HandlerTimeoutTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_secs(30))
                    .with_handler_timeout(Duration::from_secs(2))
                    .with_shutdown(sc),
            )
            .await
    });

    let got_two = handler.wait_for_calls(2, TIMEOUT).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let calls = handler.calls();
    assert!(
        got_two,
        "the hung flush must time out and redeliver; the handler was called {} \
         time(s): {calls:?}",
        calls.len()
    );
    assert_eq!(
        sorted(&calls[1]),
        vec![0, 1, 2],
        "the timed-out batch is redelivered whole"
    );
    broker.close().await;
}

/// `shutdown.cancel()` must return even while a flush is hung.
///
/// The reason the timeout is opt-*out* rather than opt-in: with no timeout,
/// this test never finishes. The consumer only reaches its shutdown branch
/// between flushes, so the bound on stopping is the bound on one flush.
#[tokio::test]
async fn shutdown_completes_while_a_flush_is_hung() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<HungShutdownTopic>()
        .await
        .unwrap();
    publish_seq::<HungShutdownTopic>(&broker, 0..3).await;

    let handler = MisbehavingBatchHandler::new(Misbehaviour::HangForever);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<HungShutdownTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_secs(30))
                    .with_handler_timeout(Duration::from_secs(2))
                    .with_shutdown(sc),
            )
            .await
    });

    // Cancel from *inside* a hung flush — the case that used to never return.
    assert!(
        handler.wait_for_calls(1, TIMEOUT).await,
        "the handler must be entered before shutdown is signalled"
    );
    shutdown.cancel();

    let stopped = tokio::time::timeout(Duration::from_secs(30), handle).await;
    assert!(
        stopped.is_ok(),
        "shutdown must not wait on a flush that never returns"
    );
    broker.close().await;
}

/// `Ack` commits the batch's end offsets, so a second consumer in the same
/// group starts past them instead of replaying.
#[tokio::test]
async fn ack_commits_offsets_so_a_restart_does_not_replay() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<CommitTopic>().await.unwrap();
    publish_seq::<CommitTopic>(&broker, 0..4).await;

    let group = "batch-commit-group";

    // First run: consume and ack all 4.
    let first = RecordingBatchHandler::new();
    let h = first.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<CommitTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(4)
                    .with_max_batch_age(Duration::from_secs(30))
                    .with_group_id(group)
                    .with_shutdown(sc),
            )
            .await
    });
    assert!(first.wait_for_batches(1, TIMEOUT).await);
    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(first.seen().len(), 4);

    // Second run, same group: only the newly published message is delivered.
    publish_seq::<CommitTopic>(&broker, 100..101).await;

    let second = RecordingBatchHandler::new();
    let h = second.clone();
    let shutdown2 = CancellationToken::new();
    let sc2 = shutdown2.clone();
    let consumer2 = KafkaConsumer::new(tb.client());
    let handle2 = tokio::spawn(async move {
        consumer2
            .run_batch::<CommitTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(1000)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_group_id(group)
                    .with_shutdown(sc2),
            )
            .await
    });
    assert!(second.wait_for_batches(1, TIMEOUT).await);
    shutdown2.cancel();
    handle2.await.unwrap().ok();

    assert_eq!(
        second.seen(),
        vec![100],
        "the committed batch must not be replayed, got {:?}",
        second.batches()
    );
    broker.close().await;
}

/// A flush window containing *nothing but* poison still commits its offsets.
///
/// This is the forward-progress guarantee: the handler is never called (there
/// is no surviving message to hand it), so if the empty batch did not commit,
/// the same poison would be re-read after every restart and a partition whose
/// unread tail is all poison would never advance again.
#[tokio::test]
async fn a_batch_of_only_dropped_messages_still_commits() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<PoisonOnlyTopic>()
        .await
        .unwrap();

    // Nothing but undecodable payloads — every one is dropped pre-handler.
    for _ in 0..3 {
        publish_raw(&tb.brokers, "kafka-batch-poison-only", b"{not valid json").await;
    }

    let group = "batch-poison-only-group";

    let first = RecordingBatchHandler::new();
    let h = first.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<PoisonOnlyTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(1000)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_group_id(group)
                    .with_shutdown(sc),
            )
            .await
    });

    // The handler is never invoked here, so there is no batch to wait on —
    // just give the age-triggered flush time to fire and commit.
    tokio::time::sleep(Duration::from_secs(3)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();
    assert!(
        first.batches().is_empty(),
        "no batch should reach the handler, got {:?}",
        first.batches()
    );

    // Second run, same group: the poison must not be re-read. Only the
    // newly published good message should arrive.
    publish_seq::<PoisonOnlyTopic>(&broker, 7..8).await;

    let second = RecordingBatchHandler::new();
    let h = second.clone();
    let shutdown2 = CancellationToken::new();
    let sc2 = shutdown2.clone();
    let consumer2 = KafkaConsumer::new(tb.client());
    let handle2 = tokio::spawn(async move {
        consumer2
            .run_batch::<PoisonOnlyTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(1000)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_group_id(group)
                    .with_shutdown(sc2),
            )
            .await
    });

    assert!(second.wait_for_batches(1, TIMEOUT).await);
    shutdown2.cancel();
    handle2.await.unwrap().ok();

    assert_eq!(
        second.seen(),
        vec![7],
        "offsets past the all-poison window must have been committed"
    );
    broker.close().await;
}

/// `max_batch_size` has to bound a batch of nothing but poison too.
///
/// Dropped messages never reach `messages`, so counting only decoded ones left
/// the size trigger unreachable for an all-poison window: the batch grew until
/// `max_batch_age` alone ended it, holding every parked DLQ payload in memory
/// for the full window. `max_batch_age` here is 120s — far longer than the
/// test will wait — so the DLQ can only fill if the *size* trigger fired.
#[tokio::test]
async fn a_flood_of_poison_still_trips_the_size_trigger() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<PoisonFloodTopic>()
        .await
        .unwrap();

    const POISON: &[u8] = b"{not valid json";
    for _ in 0..3 {
        publish_raw(&tb.brokers, "kafka-batch-poison-flood", POISON).await;
    }

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<PoisonFloodTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_secs(120))
                    .with_shutdown(sc),
            )
            .await
    });

    let dead = drain_raw(&tb.brokers, "kafka-batch-poison-flood-dlq", 3).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    assert_eq!(
        dead.len(),
        3,
        "three poison messages fill a max_batch_size of 3 and must flush on size, \
         not wait out the 120s age window; got {dead:?}"
    );
    assert!(
        handler.batches().is_empty(),
        "none of them decode, so the handler is never called"
    );
    broker.close().await;
}

/// An undeserializable payload is dropped from the batch; its neighbours
/// still flush.
#[tokio::test]
async fn undeserializable_message_is_dropped_from_the_batch() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<BadPayloadTopic>()
        .await
        .unwrap();

    publish_seq::<BadPayloadTopic>(&broker, 0..1).await;
    publish_raw(&tb.brokers, "kafka-batch-bad-payload", b"{not valid json").await;
    publish_seq::<BadPayloadTopic>(&broker, 1..2).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<BadPayloadTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(1000)
                    .with_max_batch_age(Duration::from_millis(500))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    // Let any straggler flush land before asserting on the full set.
    tokio::time::sleep(Duration::from_millis(800)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![0, 1],
        "the good messages should survive the poison payload"
    );
    broker.close().await;
}

/// An undecodable message in a batch is preserved in the DLQ, not silently
/// discarded — parity with the single-message path.
#[tokio::test]
async fn undeserializable_batch_message_lands_in_dlq() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<BatchDlqTopic>().await.unwrap();

    const POISON: &[u8] = b"{not valid json";
    publish_seq::<BatchDlqTopic>(&broker, 0..1).await;
    publish_raw(&tb.brokers, "kafka-batch-dlq", POISON).await;
    publish_seq::<BatchDlqTopic>(&broker, 1..2).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<BatchDlqTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(1000)
                    .with_max_batch_age(Duration::from_millis(500))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(handler.wait_for_batches(1, TIMEOUT).await);

    // The poison payload must be recoverable from the DLQ, byte-for-byte.
    let dead = drain_raw(&tb.brokers, "kafka-batch-dlq-dlq", 1).await;
    assert_eq!(
        dead,
        vec![POISON.to_vec()],
        "the undecodable payload should be preserved in the DLQ"
    );

    tokio::time::sleep(Duration::from_millis(800)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(seen, vec![0, 1], "the good messages should still flush");
    broker.close().await;
}

/// A payload over `max_message_size` is dropped from the batch rather than
/// failing the consumer — and lands in the DLQ instead of being discarded.
#[tokio::test]
async fn oversized_message_is_dropped_from_the_batch() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<OversizeTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<OversizeTopic>(&BatchMessage::new(0))
        .await
        .unwrap();
    let oversized = BatchMessage {
        seq: 1,
        padding: "x".repeat(4096),
    };
    publisher
        .publish::<OversizeTopic>(&oversized)
        .await
        .unwrap();
    publisher
        .publish::<OversizeTopic>(&BatchMessage::new(2))
        .await
        .unwrap();

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<OversizeTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(1000)
                    .with_max_batch_age(Duration::from_millis(500))
                    // Comfortably above a bare BatchMessage, far below the
                    // 4 KiB-padded one.
                    .with_max_message_size(512)
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(handler.wait_for_batches(1, TIMEOUT).await);

    // Dropped from the batch is not the same as discarded: the payload must be
    // recoverable from the DLQ, byte-for-byte.
    let dead = drain_raw(&tb.brokers, "kafka-batch-oversize-dlq", 1).await;
    assert_eq!(
        dead,
        vec![serde_json::to_vec(&oversized).unwrap()],
        "the oversized payload should be preserved in the DLQ"
    );

    tokio::time::sleep(Duration::from_millis(800)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![0, 2],
        "the oversized message should be dropped, its neighbours kept"
    );
    broker.close().await;
}

/// `Outcome::Reject` is terminal on the batch path exactly as it is on the
/// single-message path: the batch goes to the DLQ and its offsets commit,
/// instead of being redelivered forever.
#[tokio::test]
async fn rejected_batch_lands_in_the_dlq_and_commits() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<RejectTopic>().await.unwrap();
    publish_seq::<RejectTopic>(&broker, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Reject]);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<RejectTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(500))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(handler.wait_for_batches(1, TIMEOUT).await);

    let mut dead = drain_raw(&tb.brokers, "kafka-batch-reject-dlq", 3).await;
    dead.sort();
    let mut expected: Vec<Vec<u8>> = (0..3)
        .map(|seq| serde_json::to_vec(&BatchMessage::new(seq)).unwrap())
        .collect();
    expected.sort();
    assert_eq!(dead, expected, "every rejected message belongs in the DLQ");

    // Give a redelivery every chance to show up: the whole point is that the
    // offsets committed, so the rejected batch never comes back.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        handler.batches().len(),
        1,
        "a rejected batch must not be redelivered, got {:?}",
        handler.batches()
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// Poison is DLQ'd once per message, not once per redelivery of the batch it
/// happened to sit in. The DLQ publish is deferred until the batch's offsets
/// actually commit, so a `Retry` that seeks back over the same poison does not
/// duplicate it.
#[tokio::test]
async fn poison_is_not_re_dlqd_on_every_batch_redelivery() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<PoisonRetryTopic>()
        .await
        .unwrap();

    const POISON: &[u8] = b"{not valid json";
    publish_seq::<PoisonRetryTopic>(&broker, 0..1).await;
    publish_raw(&tb.brokers, "kafka-batch-poison-retry", POISON).await;
    publish_seq::<PoisonRetryTopic>(&broker, 1..2).await;

    // First flush retries the whole batch — including a seek back over the
    // poison, which fails to decode a second time — then acks.
    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry]);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<PoisonRetryTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(1000)
                    .with_max_batch_age(Duration::from_millis(500))
                    .with_shutdown(sc),
            )
            .await
    });

    // Two flushes: the retried one and the acked redelivery.
    assert!(handler.wait_for_batches(2, TIMEOUT).await);
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let dead = drain_raw_within(
        &tb.brokers,
        "kafka-batch-poison-retry-dlq",
        2,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        dead,
        vec![POISON.to_vec()],
        "one bad message means one DLQ entry, however many times its batch was redelivered"
    );
    broker.close().await;
}

/// Cancelling the shutdown token drains a partially-filled batch to the
/// handler instead of discarding it.
#[tokio::test]
async fn shutdown_flushes_the_pending_partial_batch() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<ShutdownTopic>().await.unwrap();
    publish_seq::<ShutdownTopic>(&broker, 0..2).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = KafkaConsumer::new(tb.client());
    let handle = tokio::spawn(async move {
        consumer
            .run_batch::<ShutdownTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    // Neither trigger can fire on its own: the batch only
                    // reaches the handler via the shutdown drain.
                    .with_max_batch_size(1000)
                    .with_max_batch_age(Duration::from_secs(3600))
                    .with_shutdown(sc),
            )
            .await
    });

    // Give the consumer time to join the group and buffer both messages.
    // Nothing may flush in that window — both triggers are set out of reach,
    // so a batch arriving here would mean the drain is not what delivered it.
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(
        handler.batches().is_empty(),
        "neither trigger should have fired before shutdown, got {:?}",
        handler.batches()
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![0, 1],
        "shutdown should drain the pending batch, got {:?}",
        handler.batches()
    );
    broker.close().await;
}
