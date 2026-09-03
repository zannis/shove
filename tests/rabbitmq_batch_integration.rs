//! Integration tests for the RabbitMQ batch consumer
//! (`Broker::<RabbitMq>::batch_consumer()`).
//!
//! Drives everything through the generic wrapper — `BatchConsumer<RabbitMq>` /
//! `BatchConsumerOptions<RabbitMq>` — rather than any RabbitMQ-only inherent
//! method, so these tests exercise the same entry point a caller reaches for
//! on any backend. Mirrors `tests/inmemory_batch.rs`'s coverage (size/age
//! flush boundaries, `Ack`/`Reject`/`Retry`/`Defer` settlement, pre-handler
//! drops, panics, timeouts, the sequencing guard, shutdown drain) over
//! RabbitMQ's own mechanics: unacked deliveries on one channel settled with
//! `multiple: true` frames, broker-side DLX dead-lettering, and
//! `basic_nack(requeue: true)` redelivery instead of a seek or a
//! `requeue_front`.

#![cfg(feature = "rabbitmq")]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::codec::RawBytesCodec;
use shove::error::ShoveError;
use shove::handler::BatchMessageHandler;
use shove::markers::RabbitMq as RabbitMqMarker;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::rabbitmq::{RabbitMqClient, RabbitMqConfig};
use shove::topic::{NotSequenced, Topic};
use shove::topology::{QueueTopology, SequenceFailure, TopologyBuilder};
use shove::{BatchConsumerOptions, define_topic};

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

const TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Test context — shared-container mode (RABBITMQ_AMQP_URL, one vhost per
// test) or standalone testcontainers mode, same split as
// `tests/rabbitmq_integration.rs`.
// ---------------------------------------------------------------------------

struct TestContext {
    amqp_url: String,
    mgmt_url: String,
    vhost: String,
    _container: Option<testcontainers::ContainerAsync<RabbitMq>>,
}

impl TestContext {
    async fn new() -> Self {
        let http = reqwest::Client::new();

        if let Ok(base_amqp) = std::env::var("RABBITMQ_AMQP_URL") {
            let mgmt_url = std::env::var("RABBITMQ_MGMT_URL")
                .expect("RABBITMQ_MGMT_URL must be set when RABBITMQ_AMQP_URL is set");
            let vhost = format!("test-{}", uuid::Uuid::new_v4());

            let status = http
                .put(format!("{mgmt_url}/api/vhosts/{vhost}"))
                .basic_auth("guest", Some("guest"))
                .header("content-type", "application/json")
                .body("{}")
                .send()
                .await
                .expect("failed to create vhost")
                .status();
            assert!(status.is_success(), "create vhost returned {status}");

            let status = http
                .put(format!("{mgmt_url}/api/permissions/{vhost}/guest"))
                .basic_auth("guest", Some("guest"))
                .json(&std::collections::HashMap::from([
                    ("configure", ".*"),
                    ("write", ".*"),
                    ("read", ".*"),
                ]))
                .send()
                .await
                .expect("failed to set vhost permissions")
                .status();
            assert!(status.is_success(), "set permissions returned {status}");

            let amqp_url = format!("{base_amqp}/{vhost}");
            Self {
                amqp_url,
                mgmt_url,
                vhost,
                _container: None,
            }
        } else {
            let container = RabbitMq::default()
                .start()
                .await
                .expect("failed to start RabbitMQ container");
            let host = container.get_host().await.expect("failed to get host");
            let amqp_port = container
                .get_host_port_ipv4(5672)
                .await
                .expect("failed to get AMQP port");
            let mgmt_port = container
                .get_host_port_ipv4(15672)
                .await
                .expect("failed to get mgmt port");
            Self {
                amqp_url: format!("amqp://guest:guest@{host}:{amqp_port}"),
                mgmt_url: format!("http://{host}:{mgmt_port}"),
                vhost: "/".to_string(),
                _container: Some(container),
            }
        }
    }

    fn rmq_config(&self) -> RabbitMqConfig {
        RabbitMqConfig::new(self.amqp_url.clone())
    }

    fn broker_from(&self, client: RabbitMqClient) -> Broker<RabbitMqMarker> {
        Broker::<RabbitMqMarker>::from_client(client)
    }

    fn queue_api_url(&self, queue: &str) -> String {
        let vhost_path = if self.vhost == "/" {
            "%2F".to_string()
        } else {
            self.vhost.clone()
        };
        format!("{}/api/queues/{vhost_path}/{queue}", self.mgmt_url)
    }

    /// Poll the management API until `queue`'s total message count (ready +
    /// unacked) satisfies `pred`, or `timeout` elapses. Returns the last
    /// observed count. Management stats lag the broker, hence polling.
    async fn wait_for_queue_total(
        &self,
        queue: &str,
        timeout: Duration,
        pred: impl Fn(u64) -> bool,
    ) -> u64 {
        let http = reqwest::Client::new();
        let deadline = Instant::now() + timeout;
        let mut last = u64::MAX;
        loop {
            if let Ok(resp) = http
                .get(self.queue_api_url(queue))
                .basic_auth("guest", Some("guest"))
                .send()
                .await
                && resp.status().is_success()
                && let Ok(v) = resp.json::<serde_json::Value>().await
            {
                last = v.get("messages").and_then(|m| m.as_u64()).unwrap_or(0);
                if pred(last) {
                    return last;
                }
            }
            if Instant::now() >= deadline {
                return last;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn cleanup(self) {
        if self._container.is_none() {
            let http = reqwest::Client::new();
            let _ = http
                .delete(format!("{}/api/vhosts/{}", self.mgmt_url, self.vhost))
                .basic_auth("guest", Some("guest"))
                .send()
                .await;
        }
    }
}

/// Poll `long_enough` until it reports true or `timeout` elapses, waking on
/// every `signal` notification in between. Same shape as
/// `tests/inmemory_batch.rs::wait_for`.
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
// Test topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BatchMessage {
    seq: u32,
}

define_topic!(
    SizeTopic,
    BatchMessage,
    TopologyBuilder::new("rmq-batch-size").build()
);
define_topic!(
    AgeTopic,
    BatchMessage,
    TopologyBuilder::new("rmq-batch-age").build()
);
define_topic!(
    AckTopic,
    BatchMessage,
    TopologyBuilder::new("rmq-batch-ack").build()
);
define_topic!(
    RetryTopic,
    BatchMessage,
    TopologyBuilder::new("rmq-batch-retry").build()
);
define_topic!(
    DeferTopic,
    BatchMessage,
    TopologyBuilder::new("rmq-batch-defer").build()
);

const REJECT_QUEUE: &str = "rmq-batch-reject";
const REJECT_DLQ: &str = "rmq-batch-reject-dlq";
define_topic!(
    RejectDlqTopic,
    BatchMessage,
    TopologyBuilder::new(REJECT_QUEUE)
        .dlq_named(REJECT_DLQ)
        .build()
);
define_topic!(
    RejectNoDlqTopic,
    BatchMessage,
    TopologyBuilder::new("rmq-batch-reject-no-dlq").build()
);
define_topic!(
    PanicTopic,
    BatchMessage,
    TopologyBuilder::new("rmq-batch-panic").build()
);
define_topic!(
    TimeoutDefaultTopic,
    BatchMessage,
    TopologyBuilder::new("rmq-batch-timeout-default").build()
);

const TIMEOUT_REJECT_QUEUE: &str = "rmq-batch-timeout-reject";
const TIMEOUT_REJECT_DLQ: &str = "rmq-batch-timeout-reject-dlq";
define_topic!(
    TimeoutRejectTopic,
    BatchMessage,
    TopologyBuilder::new(TIMEOUT_REJECT_QUEUE)
        .dlq_named(TIMEOUT_REJECT_DLQ)
        .build()
);
define_topic!(
    ShutdownTopic,
    BatchMessage,
    TopologyBuilder::new("rmq-batch-shutdown").build()
);

// The pre-handler-drop pair: `DropTopic` is the batch consumer's own view
// (JSON-decoded `BatchMessage`); `DropRawTopic` publishes the exact same
// queue with `RawBytesCodec` so an invalid-JSON payload can be injected.
const DROP_QUEUE: &str = "rmq-batch-drop";
const DROP_DLQ: &str = "rmq-batch-drop-dlq";
define_topic!(
    DropTopic,
    BatchMessage,
    TopologyBuilder::new(DROP_QUEUE).dlq_named(DROP_DLQ).build()
);
define_topic!(
    DropRawTopic,
    Vec<u8>,
    TopologyBuilder::new(DROP_QUEUE).dlq_named(DROP_DLQ).build(),
    codec = RawBytesCodec
);

const ALL_POISON_QUEUE: &str = "rmq-batch-all-poison";
const ALL_POISON_DLQ: &str = "rmq-batch-all-poison-dlq";
define_topic!(
    AllPoisonTopic,
    BatchMessage,
    TopologyBuilder::new(ALL_POISON_QUEUE)
        .dlq_named(ALL_POISON_DLQ)
        .build()
);
define_topic!(
    AllPoisonRawTopic,
    Vec<u8>,
    TopologyBuilder::new(ALL_POISON_QUEUE)
        .dlq_named(ALL_POISON_DLQ)
        .build(),
    codec = RawBytesCodec
);

// ---------------------------------------------------------------------------
// Recording batch handler — records (seq, redelivered) per message. RabbitMQ
// carries no delivery counter (AMQP 0-9-1), so the redelivery-visibility
// assertion reads `metadata.redelivered` instead of `delivery_count`.
// ---------------------------------------------------------------------------

type SeqAndRedelivered = (u32, bool);

#[derive(Clone)]
struct RecordingBatchHandler {
    batches: Arc<Mutex<Vec<Vec<SeqAndRedelivered>>>>,
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
                .map(|(m, meta)| (m.seq, meta.redelivered))
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

    fn batches(&self) -> Vec<Vec<u32>> {
        self.batches_with_redelivered()
            .into_iter()
            .map(|batch| batch.into_iter().map(|(seq, _)| seq).collect())
            .collect()
    }

    fn batches_with_redelivered(&self) -> Vec<Vec<SeqAndRedelivered>> {
        self.batches.lock().unwrap().clone()
    }

    fn seen(&self) -> Vec<u32> {
        self.batches().into_iter().flatten().collect()
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
    RejectDlqTopic,
    RejectNoDlqTopic,
    ShutdownTopic,
    DropTopic,
    AllPoisonTopic,
);

// ---------------------------------------------------------------------------
// Misbehaving batch handler — panics or hangs on the first flush only.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Misbehaviour {
    PanicOnce,
    HangOnce,
    HangAlways,
}

#[derive(Clone)]
struct MisbehavingBatchHandler {
    mode: Misbehaviour,
    calls: Arc<Mutex<Vec<Vec<SeqAndRedelivered>>>>,
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
            calls.push(
                batch
                    .iter()
                    .map(|(m, meta)| (m.seq, meta.redelivered))
                    .collect(),
            );
            calls.len()
        };
        self.signal.notify_waiters();

        match (self.mode, nth) {
            (Misbehaviour::PanicOnce, 1) => panic!("batch handler panicked on flush {nth}"),
            (Misbehaviour::HangOnce, 1) | (Misbehaviour::HangAlways, _) => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Outcome::Ack
            }
            _ => Outcome::Ack,
        }
    }

    fn calls(&self) -> Vec<Vec<SeqAndRedelivered>> {
        self.calls.lock().unwrap().clone()
    }

    async fn wait_for_calls(&self, n: usize, timeout: Duration) -> bool {
        wait_for(&self.signal, timeout, || {
            self.calls.lock().unwrap().len() >= n
        })
        .await
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

impl_misbehaving_for!(PanicTopic, TimeoutDefaultTopic, TimeoutRejectTopic);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn publish_seq<T>(
    publisher: &shove::publisher::Publisher<RabbitMqMarker>,
    range: std::ops::Range<u32>,
) where
    T: Topic<Message = BatchMessage>,
{
    for seq in range {
        publisher
            .publish::<T>(&BatchMessage { seq })
            .await
            .expect("publish");
    }
}

/// Connect, declare `T`, publish `publish` messages, and run a batch
/// consumer over it with `options`. Returns everything the assertions need.
struct Rig {
    ctx: TestContext,
    client: RabbitMqClient,
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<shove::error::Result<()>>,
}

impl Rig {
    async fn finish(self) {
        self.shutdown.cancel();
        let _ = self.handle.await;
        self.client.shutdown().await;
        self.ctx.cleanup().await;
    }
}

async fn start_rig<T, H>(
    publish: std::ops::Range<u32>,
    handler: H,
    options_size: usize,
    options_age: Duration,
) -> Rig
where
    T: Topic<Message = BatchMessage> + NotSequenced,
    H: BatchMessageHandler<T, Context = ()> + Clone + 'static,
{
    let ctx = TestContext::new().await;
    let client = RabbitMqClient::connect(&ctx.rmq_config()).await.unwrap();
    let broker = ctx.broker_from(client.clone());
    broker.topology().declare::<T>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<T>(&publisher, publish).await;

    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<T, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(options_size)
                        .with_max_batch_age(options_age)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    Rig {
        ctx,
        client,
        shutdown,
        handle,
    }
}

// ---------------------------------------------------------------------------
// Flush triggers
// ---------------------------------------------------------------------------

/// `max_batch_size` messages flush without waiting for `max_batch_age`.
#[tokio::test]
async fn batch_flushes_on_max_batch_size() {
    let handler = RecordingBatchHandler::new();
    let rig = start_rig::<SizeTopic, _>(0..10, handler.clone(), 5, Duration::from_secs(30)).await;

    assert!(
        handler.wait_for_batches(2, TIMEOUT).await,
        "expected two size-triggered batches, got {:?}",
        handler.batches()
    );
    let batches = handler.batches();
    assert!(
        batches.iter().all(|b| b.len() == 5),
        "every batch should hold exactly max_batch_size messages, got {batches:?}"
    );
    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(seen, (0..10).collect::<Vec<_>>());
    rig.finish().await;
}

/// A batch below `max_batch_size` still flushes once `max_batch_age` elapses.
#[tokio::test]
async fn batch_flushes_on_max_batch_age() {
    let handler = RecordingBatchHandler::new();
    let rig =
        start_rig::<AgeTopic, _>(0..3, handler.clone(), 1000, Duration::from_millis(300)).await;

    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "age trigger should flush a partial batch"
    );
    let batches = handler.batches();
    assert_eq!(batches[0].len(), 3, "the partial batch holds all published");
    rig.finish().await;
}

// ---------------------------------------------------------------------------
// Outcome settlement — the four variants, each through the shared classifier
// ---------------------------------------------------------------------------

/// `Ack` retires the whole batch: the queue drains to zero (ready + unacked)
/// and nothing is ever redelivered.
#[tokio::test]
async fn ack_settles_the_whole_batch() {
    let handler = RecordingBatchHandler::new();
    let rig = start_rig::<AckTopic, _>(0..4, handler.clone(), 4, Duration::from_secs(30)).await;

    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    let total = rig
        .ctx
        .wait_for_queue_total("rmq-batch-ack", TIMEOUT, |n| n == 0)
        .await;
    assert_eq!(total, 0, "acked batch must leave nothing ready or unacked");
    assert_eq!(
        handler.batches().len(),
        1,
        "an acked batch must not be redelivered: {:?}",
        handler.batches()
    );
    assert!(
        handler.batches_with_redelivered()[0]
            .iter()
            .all(|&(_, redelivered)| !redelivered),
        "first delivery must not be flagged redelivered"
    );
    rig.finish().await;
}

/// `Reject` dead-letters the whole batch broker-side (DLX) and retires it.
#[tokio::test]
async fn reject_routes_the_whole_batch_to_the_dlq() {
    let handler = RecordingBatchHandler::new().scripting([Outcome::Reject]);
    let rig =
        start_rig::<RejectDlqTopic, _>(0..3, handler.clone(), 3, Duration::from_secs(30)).await;

    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    let dlq_total = rig
        .ctx
        .wait_for_queue_total(REJECT_DLQ, TIMEOUT, |n| n == 3)
        .await;
    assert_eq!(dlq_total, 3, "every rejected message lands in the DLQ");
    let main_total = rig
        .ctx
        .wait_for_queue_total(REJECT_QUEUE, TIMEOUT, |n| n == 0)
        .await;
    assert_eq!(main_total, 0, "rejected batch retires from the main queue");
    assert_eq!(
        handler.batches().len(),
        1,
        "a rejected batch must not be redelivered"
    );
    rig.finish().await;
}

/// `Reject` with no DLQ configured discards the batch (with the discard
/// counted — the metrics binary asserts that half) and still makes progress.
#[tokio::test]
async fn reject_without_dlq_discards_and_retires() {
    let handler = RecordingBatchHandler::new().scripting([Outcome::Reject]);
    let rig =
        start_rig::<RejectNoDlqTopic, _>(0..3, handler.clone(), 3, Duration::from_secs(30)).await;

    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    let total = rig
        .ctx
        .wait_for_queue_total("rmq-batch-reject-no-dlq", TIMEOUT, |n| n == 0)
        .await;
    assert_eq!(total, 0, "no-DLQ reject discards; nothing stays queued");
    assert_eq!(handler.batches().len(), 1);
    rig.finish().await;
}

/// `Retry` redelivers the whole batch — same messages, `redelivered: true` —
/// and the second flush's `Ack` retires them.
#[tokio::test]
async fn retry_redelivers_the_whole_batch() {
    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry, Outcome::Ack]);
    let rig = start_rig::<RetryTopic, _>(0..3, handler.clone(), 3, Duration::from_secs(30)).await;

    assert!(
        handler.wait_for_batches(2, TIMEOUT).await,
        "retry must produce a second flush, got {:?}",
        handler.batches()
    );
    let batches = handler.batches_with_redelivered();
    let first: Vec<u32> = batches[0].iter().map(|&(s, _)| s).collect();
    let mut second: Vec<u32> = batches[1].iter().map(|&(s, _)| s).collect();
    second.sort_unstable();
    assert_eq!(first, vec![0, 1, 2]);
    assert_eq!(second, vec![0, 1, 2], "the same batch comes back");
    assert!(
        batches[1].iter().all(|&(_, redelivered)| redelivered),
        "broker must flag the second delivery redelivered: {batches:?}"
    );
    let total = rig
        .ctx
        .wait_for_queue_total("rmq-batch-retry", TIMEOUT, |n| n == 0)
        .await;
    assert_eq!(total, 0, "acked retry batch retires");
    rig.finish().await;
}

/// `Defer` settles exactly like `Retry` on the batch path — the shared
/// classifier maps both to `Redeliver`, so there must be no divergent arm.
#[tokio::test]
async fn defer_settles_exactly_like_retry() {
    let handler = RecordingBatchHandler::new().scripting([Outcome::Defer, Outcome::Ack]);
    let rig = start_rig::<DeferTopic, _>(0..3, handler.clone(), 3, Duration::from_secs(30)).await;

    assert!(handler.wait_for_batches(2, TIMEOUT).await);
    let batches = handler.batches_with_redelivered();
    assert!(
        batches[1].iter().all(|&(_, redelivered)| redelivered),
        "deferred batch redelivers like a retried one: {batches:?}"
    );
    let total = rig
        .ctx
        .wait_for_queue_total("rmq-batch-defer", TIMEOUT, |n| n == 0)
        .await;
    assert_eq!(total, 0);
    rig.finish().await;
}

// ---------------------------------------------------------------------------
// Panic / timeout containment
// ---------------------------------------------------------------------------

/// A panic inside the flush becomes `Retry`: the whole batch is redelivered,
/// nothing was acked, and the loop survives to handle the redelivery.
#[tokio::test]
async fn panic_inside_a_flush_redelivers_the_batch() {
    let handler = MisbehavingBatchHandler::new(Misbehaviour::PanicOnce);
    let rig = start_rig::<PanicTopic, _>(0..3, handler.clone(), 3, Duration::from_secs(30)).await;

    assert!(
        handler.wait_for_calls(2, TIMEOUT).await,
        "the panicked batch must come back, got {:?}",
        handler.calls()
    );
    let calls = handler.calls();
    let mut second: Vec<u32> = calls[1].iter().map(|&(s, _)| s).collect();
    second.sort_unstable();
    assert_eq!(second, vec![0, 1, 2], "same batch after the panic");
    assert!(
        calls[1].iter().all(|&(_, redelivered)| redelivered),
        "nothing was acked, so every message redelivers: {calls:?}"
    );
    let total = rig
        .ctx
        .wait_for_queue_total("rmq-batch-panic", TIMEOUT, |n| n == 0)
        .await;
    assert_eq!(total, 0, "second (acked) flush retires the batch");
    rig.finish().await;
}

/// A hung flush resolves through the handler timeout into the default
/// `Retry`, and the batch redelivers.
#[tokio::test]
async fn handler_timeout_defaults_to_retry() {
    let ctx = TestContext::new().await;
    let client = RabbitMqClient::connect(&ctx.rmq_config()).await.unwrap();
    let broker = ctx.broker_from(client.clone());
    broker
        .topology()
        .declare::<TimeoutDefaultTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<TimeoutDefaultTopic>(&publisher, 0..2).await;

    let handler = MisbehavingBatchHandler::new(Misbehaviour::HangOnce);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<TimeoutDefaultTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_handler_timeout(Duration::from_millis(400))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(
        handler.wait_for_calls(2, TIMEOUT).await,
        "timed-out batch must redeliver, got {:?}",
        handler.calls()
    );
    let calls = handler.calls();
    assert!(
        calls[1].iter().all(|&(_, redelivered)| redelivered),
        "timeout means nothing was acked: {calls:?}"
    );
    shutdown.cancel();
    let _ = handle.await;
    client.shutdown().await;
    ctx.cleanup().await;
}

/// `with_handler_timeout_outcome(Reject)` routes a hung flush's batch to the
/// DLQ instead of redelivering it.
#[tokio::test]
async fn handler_timeout_honours_the_configured_outcome() {
    let ctx = TestContext::new().await;
    let client = RabbitMqClient::connect(&ctx.rmq_config()).await.unwrap();
    let broker = ctx.broker_from(client.clone());
    broker
        .topology()
        .declare::<TimeoutRejectTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<TimeoutRejectTopic>(&publisher, 0..2).await;

    let handler = MisbehavingBatchHandler::new(Misbehaviour::HangAlways);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<TimeoutRejectTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_handler_timeout(Duration::from_millis(400))
                        .with_handler_timeout_outcome(Outcome::Reject)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_calls(1, TIMEOUT).await);
    let dlq_total = ctx
        .wait_for_queue_total(TIMEOUT_REJECT_DLQ, TIMEOUT, |n| n == 2)
        .await;
    assert_eq!(
        dlq_total, 2,
        "a Reject timeout outcome dead-letters the whole batch"
    );
    shutdown.cancel();
    let _ = handle.await;
    client.shutdown().await;
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// Sequencing guard
// ---------------------------------------------------------------------------

/// `NotSequenced` is hand-implementable, so a topic can claim it while its
/// topology still declares sequencing. The runtime guard must reject that.
#[tokio::test]
async fn runtime_guard_rejects_a_topic_that_declares_sequencing() {
    struct LiesAboutSequencing;
    impl Topic for LiesAboutSequencing {
        type Message = BatchMessage;
        type Codec = shove::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("rmq-batch-guard-test")
                    .sequenced(SequenceFailure::FailAll)
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
            _ctx: &(),
        ) -> Outcome {
            Outcome::Ack
        }
    }

    // The guard fires before any channel is opened, so no declare is needed.
    let ctx = TestContext::new().await;
    let client = RabbitMqClient::connect(&ctx.rmq_config()).await.unwrap();
    let broker = ctx.broker_from(client.clone());
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
            assert!(msg.contains("rmq-batch-guard-test"));
            assert!(msg.contains("run_fifo"));
        }
        other => panic!("expected ShoveError::Topology, got {other:?}"),
    }
    client.shutdown().await;
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// Pre-handler drops
// ---------------------------------------------------------------------------

/// Oversize and undecodable messages are dropped before the handler and
/// dead-lettered at the flush; decodable siblings in the same window are
/// still handled.
#[tokio::test]
async fn pre_handler_drops_dead_letter_and_do_not_block_the_batch() {
    let ctx = TestContext::new().await;
    let client = RabbitMqClient::connect(&ctx.rmq_config()).await.unwrap();
    let broker = ctx.broker_from(client.clone());
    broker.topology().declare::<DropTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();

    // Two good, one undecodable (raw non-JSON bytes), one oversized.
    publish_seq::<DropTopic>(&publisher, 0..2).await;
    publisher
        .publish::<DropRawTopic>(&b"not json".to_vec())
        .await
        .unwrap();
    publisher
        .publish::<DropRawTopic>(&vec![b'x'; 64 * 1024])
        .await
        .unwrap();

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<DropTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(4)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_max_message_size(32 * 1024)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![0, 1],
        "only the decodable pair reaches the handler"
    );
    let dlq_total = ctx
        .wait_for_queue_total(DROP_DLQ, TIMEOUT, |n| n == 2)
        .await;
    assert_eq!(dlq_total, 2, "both pre-handler drops land in the DLQ");
    let main_total = ctx
        .wait_for_queue_total(DROP_QUEUE, TIMEOUT, |n| n == 0)
        .await;
    assert_eq!(main_total, 0);
    shutdown.cancel();
    let _ = handle.await;
    client.shutdown().await;
    ctx.cleanup().await;
}

/// A window of nothing but poison still makes forward progress: it flushes at
/// the size trigger with no handler call and dead-letters everything.
#[tokio::test]
async fn an_all_poison_window_still_flushes_and_makes_progress() {
    let ctx = TestContext::new().await;
    let client = RabbitMqClient::connect(&ctx.rmq_config()).await.unwrap();
    let broker = ctx.broker_from(client.clone());
    broker.topology().declare::<AllPoisonTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    for _ in 0..3 {
        publisher
            .publish::<AllPoisonRawTopic>(&b"not json".to_vec())
            .await
            .unwrap();
    }

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<AllPoisonTopic, _>(
                    handler,
                    (),
                    // Size 3 so the all-poison window trips the SIZE trigger —
                    // an age of 30s would time the test out if poison did not
                    // count toward the flush threshold.
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    let dlq_total = ctx
        .wait_for_queue_total(ALL_POISON_DLQ, TIMEOUT, |n| n == 3)
        .await;
    assert_eq!(
        dlq_total, 3,
        "all poison dead-letters without a handler call"
    );
    assert!(
        handler.batches().is_empty(),
        "the handler must never see an empty batch: {:?}",
        handler.batches()
    );
    let main_total = ctx
        .wait_for_queue_total(ALL_POISON_QUEUE, TIMEOUT, |n| n == 0)
        .await;
    assert_eq!(
        main_total, 0,
        "the poison window retires — forward progress"
    );
    shutdown.cancel();
    let _ = handle.await;
    client.shutdown().await;
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

/// Cancelling the per-consumer token flushes the pending partial batch
/// instead of stranding it, and the loop exits cleanly.
#[tokio::test]
async fn shutdown_flushes_the_pending_partial_batch() {
    let handler = RecordingBatchHandler::new();
    let rig =
        start_rig::<ShutdownTopic, _>(0..2, handler.clone(), 10, Duration::from_secs(300)).await;

    // Wait until both messages are held in the accumulator (unacked at the
    // broker), then cancel. The flush must happen on the way out.
    rig.ctx
        .wait_for_queue_total("rmq-batch-shutdown", TIMEOUT, |n| n == 2)
        .await;
    // Give the consumer a beat to actually ingest what the broker delivered.
    tokio::time::sleep(Duration::from_millis(500)).await;
    rig.shutdown.cancel();

    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "shutdown must flush the partial batch, got {:?}",
        handler.batches()
    );
    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(seen, vec![0, 1]);

    let handle_result = rig.handle.await;
    assert!(
        matches!(&handle_result, Ok(Ok(()))),
        "batch loop must exit Ok on shutdown, got {handle_result:?}"
    );
    let total = rig
        .ctx
        .wait_for_queue_total("rmq-batch-shutdown", TIMEOUT, |n| n == 0)
        .await;
    assert_eq!(total, 0, "the flushed batch must be settled, not stranded");
    rig.client.shutdown().await;
    rig.ctx.cleanup().await;
}
