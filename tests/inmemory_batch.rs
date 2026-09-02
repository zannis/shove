//! Integration tests for the InMemory batch consumer
//! (`Broker::<InMemory>::batch_consumer()`).
//!
//! Drives everything through the generic wrapper — `BatchConsumer<InMemory>` /
//! `BatchConsumerOptions<InMemory>` — rather than any InMemory-only inherent
//! method, so these tests exercise the same entry point a caller reaches for
//! on any backend. Mirrors `tests/kafka_batch_integration.rs`'s coverage
//! (size/age flush boundaries, `Ack`/`Reject`/`Retry`/`Defer` settlement,
//! pre-handler drops, panics, timeouts, shutdown drain) over InMemory's own
//! mechanics: a `VecDeque` buffer and `requeue_front` instead of partition
//! offsets and a seek.

#![cfg(feature = "inmemory")]

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::broker::Broker;
use shove::codec::RawBytesCodec;
use shove::consumer::ConsumerOptions;
use shove::error::ShoveError;
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::inmemory::InMemoryConfig;
use shove::markers::InMemory;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::publisher::Publisher;
use shove::topic::{NotSequenced, Topic};
use shove::topology::{QueueTopology, SequenceFailure, TopologyBuilder};
use shove::{BatchConsumerOptions, define_topic};

const TIMEOUT: Duration = Duration::from_secs(10);

/// Poll `long_enough` until it reports true or `timeout` elapses, waking on
/// every `signal` notification in between rather than busy-polling. Shared
/// tail for every recording handler's `wait_for_*` method below — each of
/// them differs only in *what* "long enough" means (a `Mutex<Vec<_>>`'s
/// length against a target count, or an `AtomicUsize`'s), not in how the wait
/// itself is driven.
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

// ---------------------------------------------------------------------------
// Test topics
// ---------------------------------------------------------------------------

define_topic!(
    SizeTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-size").build()
);
define_topic!(
    AgeTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-age").build()
);
define_topic!(
    AgeUnderLoadTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-age-under-load").build()
);
define_topic!(
    AckTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-ack").build()
);
define_topic!(
    RetryTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-retry").build()
);
define_topic!(
    DeferTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-defer").build()
);
define_topic!(
    RejectDlqTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-reject-dlq").dlq().build()
);
define_topic!(
    RejectNoDlqTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-reject-no-dlq").build()
);
define_topic!(
    PanicTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-panic").build()
);
define_topic!(
    PanicBuildTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-panic-build").build()
);
define_topic!(
    TimeoutDefaultTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-timeout-default").build()
);
define_topic!(
    TimeoutAckTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-timeout-ack").build()
);
define_topic!(
    ShutdownTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-shutdown").build()
);
define_topic!(
    InFlightTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-inflight").build()
);
define_topic!(
    BrokerCloseBackoffTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-broker-close-backoff").build()
);
const REDELIVERY_ORDER_QUEUE: &str = "inmem-batch-redelivery-order";
const REDELIVERY_ORDER_DLQ: &str = "inmem-batch-redelivery-order-dlq";

define_topic!(
    RedeliveryOrderTopic,
    BatchMessage,
    TopologyBuilder::new(REDELIVERY_ORDER_QUEUE)
        .dlq_named(REDELIVERY_ORDER_DLQ)
        .build()
);
define_topic!(
    DeliveryCountTopic,
    BatchMessage,
    TopologyBuilder::new("inmem-batch-delivery-count").build()
);

// The pre-handler-drop pair: `DropTopic` is the batch consumer's own view
// (JSON-decoded `BatchMessage`); `DropRawTopic` publishes the exact same
// queue name with `RawBytesCodec`, so an invalid-JSON payload can be
// injected — the JSON topic would fail to *publish* one, never mind consume
// it. Both must declare the same DLQ name so a shadow raw reader can drain
// it byte-for-byte (JSON-decoding a genuinely undecodable DLQ entry would
// just fail again).
const DROP_QUEUE: &str = "inmem-batch-drop";
const DROP_DLQ: &str = "inmem-batch-drop-dlq";

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
define_topic!(
    DropDlqRawTopic,
    Vec<u8>,
    TopologyBuilder::new(DROP_DLQ).build(),
    codec = RawBytesCodec
);

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
    AgeUnderLoadTopic,
    AckTopic,
    RetryTopic,
    DeferTopic,
    RejectDlqTopic,
    RejectNoDlqTopic,
    ShutdownTopic,
    InFlightTopic,
    DropTopic,
    BrokerCloseBackoffTopic,
    RedeliveryOrderTopic,
);

// ---------------------------------------------------------------------------
// Delivery-count recording batch handler — for F2 (redelivery marks every
// requeued envelope, batch-wide) coverage.
// ---------------------------------------------------------------------------

/// `(seq, delivery_count)` for one message.
type SeqAndDeliveryCount = (u32, Option<u32>);

/// Records `(seq, delivery_count)` for every message in every batch, so a
/// test can tell a first delivery from a redelivery. Scripts outcomes the
/// same way [`RecordingBatchHandler`] does.
#[derive(Clone)]
struct DeliveryCountRecordingHandler {
    batches: Arc<Mutex<Vec<Vec<SeqAndDeliveryCount>>>>,
    scripted: Arc<Mutex<VecDeque<Outcome>>>,
    signal: Arc<Notify>,
}

impl DeliveryCountRecordingHandler {
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

    fn batches(&self) -> Vec<Vec<SeqAndDeliveryCount>> {
        self.batches.lock().unwrap().clone()
    }

    async fn wait_for_batches(&self, n: usize, timeout: Duration) -> bool {
        wait_for(&self.signal, timeout, || {
            self.batches.lock().unwrap().len() >= n
        })
        .await
    }
}

impl BatchMessageHandler<DeliveryCountTopic> for DeliveryCountRecordingHandler {
    type Context = ();
    async fn handle_batch(
        &self,
        messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        self.batches.lock().unwrap().push(
            messages
                .iter()
                .map(|(m, meta)| (m.seq, meta.delivery_count))
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
}

// ---------------------------------------------------------------------------
// Misbehaving batch handler — panics or hangs
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Misbehaviour {
    PanicOnce,
    HangOnce,
}

#[derive(Clone)]
struct MisbehavingBatchHandler {
    mode: Misbehaviour,
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

        if nth != 1 {
            return Outcome::Ack;
        }
        match self.mode {
            Misbehaviour::PanicOnce => panic!("batch handler panicked on flush {nth}"),
            Misbehaviour::HangOnce => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Outcome::Ack
            }
        }
    }

    fn calls(&self) -> Vec<Vec<u32>> {
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

impl_misbehaving_for!(PanicTopic, TimeoutDefaultTopic, TimeoutAckTopic);

/// Panics while *building* its future — before anything is awaited — on the
/// first call, and acks thereafter. `handle_batch` is deliberately not an
/// `async fn`: it is a plain function returning `impl Future`, so the panic
/// happens inside the function body rather than inside the future it
/// produces. See `src/backend/batch_consumer.rs`'s
/// `a_panic_while_building_the_future_is_contained` for the unit-level
/// version of this shape; this is the same case through the full
/// `run_batch_impl` loop.
#[derive(Clone)]
struct FutureBuildPanicHandler {
    calls: Arc<AtomicUsize>,
    signal: Arc<Notify>,
}

impl FutureBuildPanicHandler {
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

impl BatchMessageHandler<PanicBuildTopic> for FutureBuildPanicHandler {
    type Context = ();

    fn handle_batch(
        &self,
        _messages: Vec<(BatchMessage, MessageMetadata)>,
        _ctx: &(),
    ) -> impl Future<Output = Outcome> + Send {
        let nth = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.signal.notify_waiters();
        if nth == 1 {
            panic!("handle_batch blew up before returning a future");
        }
        async { Outcome::Ack }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sorted(batch: &[u32]) -> Vec<u32> {
    let mut v = batch.to_vec();
    v.sort_unstable();
    v
}

async fn publish_seq<T>(publisher: &Publisher<InMemory>, range: std::ops::Range<u32>)
where
    T: Topic<Message = BatchMessage>,
{
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

/// A batch flushes as soon as it reaches `max_batch_size`, with every message
/// delivered exactly once across the resulting batches.
#[tokio::test]
async fn batch_flushes_on_max_batch_size() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<SizeTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<SizeTopic>(&publisher, 0..10).await;

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<SizeTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(5)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_shutdown(shutdown),
                )
                .await
        }
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
    assert_eq!(seen, (0..10).collect::<Vec<_>>());
    broker.close().await;
}

/// A batch below `max_batch_size` still flushes once `max_batch_age` elapses.
#[tokio::test]
async fn batch_flushes_on_max_batch_age() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<AgeTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<AgeTopic>(&publisher, 0..3).await;

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<AgeTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(1000)
                        .with_max_batch_age(Duration::from_millis(200))
                        .with_shutdown(shutdown),
                )
                .await
        }
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

/// The age trigger must not starve under a steady sub-cap trickle: even
/// though the buffer never truly goes idle, the deadline armed on the first
/// message of each batch still fires and flushes within roughly
/// `max_batch_age`, rather than being pushed back by every new arrival.
#[tokio::test]
async fn age_trigger_flushes_under_sustained_sub_cap_load() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<AgeUnderLoadTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<AgeUnderLoadTopic, _>(
                    handler,
                    (),
                    // Far above what the trickle below can reach in the test
                    // window, so only the age trigger can fire.
                    BatchConsumerOptions::new()
                        .with_max_batch_size(1000)
                        .with_max_batch_age(Duration::from_millis(150))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    // One message every 30ms — well under the 150ms deadline, so the buffer
    // is essentially never empty, but each individual batch stays far below
    // max_batch_size.
    for seq in 0..15u32 {
        publisher
            .publish::<AgeUnderLoadTopic>(&BatchMessage::new(seq))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    assert!(
        handler.wait_for_batches(2, TIMEOUT).await,
        "sustained sub-cap arrivals must not suppress the age trigger; got {:?}",
        handler.batches()
    );
    shutdown.cancel();
    handle.await.unwrap().ok();

    let batches = handler.batches();
    assert!(
        batches.iter().all(|b| b.len() < 15),
        "each age-triggered flush should be a fraction of the whole trickle, got {batches:?}"
    );
    broker.close().await;
}

/// `Ack` retires the batch — no redelivery.
#[tokio::test]
async fn ack_retires_the_batch() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<AckTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<AckTopic>(&publisher, 0..3).await;

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
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
                        .with_max_batch_age(Duration::from_millis(200))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    assert_eq!(
        handler.batches().len(),
        1,
        "an acked batch must not be redelivered, got {:?}",
        handler.batches()
    );
    broker.close().await;
}

/// `Retry` redelivers the whole batch, then `Ack` stops it.
#[tokio::test]
async fn retry_redelivers_the_whole_batch_then_ack_stops_it() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<RetryTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<RetryTopic>(&publisher, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<RetryTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_shutdown(shutdown),
                )
                .await
        }
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
    assert_eq!(
        handler.batches().len(),
        2,
        "ack on the redelivery must stop it, got {:?}",
        handler.batches()
    );
    broker.close().await;
}

/// `Defer` is indistinguishable from `Retry` on the batch path — both
/// redeliver the whole batch, since a batch-wide outcome has no per-message
/// retry budget for the two to disagree over.
#[tokio::test]
async fn defer_redelivers_like_retry() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<DeferTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<DeferTopic>(&publisher, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Defer]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<DeferTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(2, TIMEOUT).await);
    shutdown.cancel();
    handle.await.unwrap().ok();

    let batches = handler.batches();
    assert_eq!(sorted(&batches[0]), sorted(&batches[1]));
    broker.close().await;
}

/// `Reject` with a DLQ declared: every message in the batch is dead-lettered,
/// the offsets (conceptually) advance, and the loop keeps going.
#[tokio::test]
async fn rejected_batch_with_dlq_lands_every_message_and_continues() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<RejectDlqTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<RejectDlqTopic>(&publisher, 0..3).await;
    // A message published after the rejected batch proves the loop is still
    // alive rather than wedged on a redelivery.
    publish_seq::<RejectDlqTopic>(&publisher, 100..101).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Reject]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<RejectDlqTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_millis(200))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(2, TIMEOUT).await);
    shutdown.cancel();
    handle.await.unwrap().ok();

    assert_eq!(
        sorted(&handler.batches()[0]),
        vec![0, 1, 2],
        "rejected batch must reach the handler before dead-lettering"
    );
    assert_eq!(
        handler.batches()[1],
        vec![100],
        "the loop must keep consuming after a rejected batch, got {:?}",
        handler.batches()
    );

    // Every rejected message must actually be in the DLQ.
    let mut dlq = broker.consumer_supervisor();
    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    dlq.register::<DropDlqRawTopicAliasForReject, _>(
        RawRecorder {
            received: received.clone(),
        },
        ConsumerOptions::new(),
    )
    .expect("register dlq drain");
    tokio::time::sleep(Duration::from_millis(300)).await;
    dlq.cancellation_token().cancel();
    let _ = dlq
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;

    let mut dead: Vec<BatchMessage> = received
        .lock()
        .unwrap()
        .iter()
        .map(|bytes| serde_json::from_slice(bytes).unwrap())
        .collect();
    dead.sort_by_key(|m| m.seq);
    assert_eq!(
        dead,
        vec![
            BatchMessage::new(0),
            BatchMessage::new(1),
            BatchMessage::new(2)
        ],
        "every rejected message must land in the DLQ"
    );
    broker.close().await;
}

define_topic!(
    DropDlqRawTopicAliasForReject,
    Vec<u8>,
    TopologyBuilder::new("inmem-batch-reject-dlq-dlq").build(),
    codec = RawBytesCodec
);

#[derive(Clone)]
struct RawRecorder {
    received: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl MessageHandler<DropDlqRawTopicAliasForReject> for RawRecorder {
    type Context = ();
    async fn handle(&self, msg: Vec<u8>, _meta: MessageMetadata, _: &()) -> Outcome {
        self.received.lock().unwrap().push(msg);
        Outcome::Ack
    }
}

/// `Reject` with no DLQ declared: the batch is discarded (not redelivered)
/// and the loop keeps consuming.
#[tokio::test]
async fn rejected_batch_without_dlq_is_discarded_and_continues() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<RejectNoDlqTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<RejectNoDlqTopic>(&publisher, 0..3).await;
    publish_seq::<RejectNoDlqTopic>(&publisher, 100..101).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Reject]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<RejectNoDlqTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_millis(200))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(2, TIMEOUT).await);
    shutdown.cancel();
    handle.await.unwrap().ok();

    assert_eq!(handler.batches()[1], vec![100]);
    broker.close().await;
}

/// A panic inside the handler is caught and the batch is redelivered whole.
#[tokio::test]
async fn panic_in_handler_redelivers_the_batch() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<PanicTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<PanicTopic>(&publisher, 0..3).await;

    let handler = MisbehavingBatchHandler::new(Misbehaviour::PanicOnce);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<PanicTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    let got_two = handler.wait_for_calls(2, TIMEOUT).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let calls = handler.calls();
    assert!(
        got_two,
        "the panic must be caught and the batch redelivered; got {calls:?}"
    );
    assert_eq!(sorted(&calls[0]), vec![0, 1, 2]);
    assert_eq!(sorted(&calls[1]), vec![0, 1, 2]);
    broker.close().await;
}

/// A panic while *building* the handler future (before anything is awaited)
/// is contained exactly like one raised from inside the future.
#[tokio::test]
async fn panic_while_building_the_future_redelivers_the_batch() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<PanicBuildTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<PanicBuildTopic>(&publisher, 0..3).await;

    let handler = FutureBuildPanicHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<PanicBuildTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    let got_two = handler.wait_for_calls(2, TIMEOUT).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    assert!(
        got_two,
        "the batch must be redelivered after the build-time panic"
    );
    broker.close().await;
}

/// A flush that outlasts `handler_timeout` is abandoned and redelivered.
#[tokio::test]
async fn handler_timeout_defaults_to_retry_redelivery() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<TimeoutDefaultTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<TimeoutDefaultTopic>(&publisher, 0..3).await;

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
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_handler_timeout(Duration::from_millis(500))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    let got_two = handler.wait_for_calls(2, TIMEOUT).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    assert!(got_two, "the hung flush must time out and redeliver");
    assert_eq!(sorted(&handler.calls()[1]), vec![0, 1, 2]);
    broker.close().await;
}

/// `with_handler_timeout_outcome(Ack)` makes a timed-out batch gone instead
/// of redelivered.
#[tokio::test]
async fn handler_timeout_outcome_ack_makes_the_batch_gone() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<TimeoutAckTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<TimeoutAckTopic>(&publisher, 0..3).await;

    let handler = MisbehavingBatchHandler::new(Misbehaviour::HangOnce);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<TimeoutAckTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_handler_timeout(Duration::from_millis(500))
                        .with_handler_timeout_outcome(Outcome::Ack)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_calls(1, TIMEOUT).await);
    // Give a would-be redelivery every chance to show up.
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    assert_eq!(
        handler.calls().len(),
        1,
        "Ack on timeout must not redeliver, got {:?}",
        handler.calls()
    );
    broker.close().await;
}

/// An oversized and an undecodable message are both dropped before the
/// handler ever sees them, and each lands in the DLQ exactly once — only
/// after the surviving batch commits.
#[tokio::test]
async fn oversize_and_undecodable_never_reach_the_handler_and_dlq_once_each() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<DropTopic>().await.unwrap();

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<DropTopic>(&BatchMessage::new(0))
        .await
        .unwrap();
    let oversized = BatchMessage {
        seq: 1,
        padding: "x".repeat(4096),
    };
    publisher.publish::<DropTopic>(&oversized).await.unwrap();

    let raw_publisher = broker.publisher().await.unwrap();
    raw_publisher
        .publish::<DropRawTopic>(&b"{not valid json".to_vec())
        .await
        .unwrap();

    publisher
        .publish::<DropTopic>(&BatchMessage::new(2))
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
                        .with_max_batch_size(1000)
                        .with_max_batch_age(Duration::from_millis(300))
                        .with_max_message_size(512)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![0, 2],
        "the handler must never see the oversized or undecodable message"
    );

    let mut dlq = broker.consumer_supervisor();
    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    dlq.register::<DropDlqRawTopic, _>(
        RawDlqRecorder {
            received: received.clone(),
        },
        ConsumerOptions::new(),
    )
    .expect("register dlq drain");
    tokio::time::sleep(Duration::from_millis(300)).await;
    dlq.cancellation_token().cancel();
    let _ = dlq
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;

    let dead = received.lock().unwrap().clone();
    assert_eq!(
        dead.len(),
        2,
        "exactly one DLQ copy each for the oversized and undecodable message, got {dead:?}"
    );
    assert!(
        dead.contains(&serde_json::to_vec(&oversized).unwrap()),
        "the oversized payload must be preserved byte-for-byte in the DLQ"
    );
    assert!(
        dead.contains(&b"{not valid json".to_vec()),
        "the undecodable payload must be preserved byte-for-byte in the DLQ"
    );
    broker.close().await;
}

#[derive(Clone)]
struct RawDlqRecorder {
    received: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl MessageHandler<DropDlqRawTopic> for RawDlqRecorder {
    type Context = ();
    async fn handle(&self, msg: Vec<u8>, _meta: MessageMetadata, _: &()) -> Outcome {
        self.received.lock().unwrap().push(msg);
        Outcome::Ack
    }
}

/// The `NotSequenced` bound is compile-time only; a hand-implemented topic
/// can claim it while its topology still declares sequencing. The runtime
/// guard must reject that rather than silently batching out of order.
#[tokio::test]
async fn runtime_guard_rejects_a_topic_that_declares_sequencing() {
    struct LiesAboutSequencing;
    impl Topic for LiesAboutSequencing {
        type Message = BatchMessage;
        type Codec = shove::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("inmem-batch-guard-test")
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

    // No `declare` needed: the guard fires before any queue lookup.
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
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
            assert!(msg.contains("inmem-batch-guard-test"));
            assert!(msg.contains("run_fifo"));
        }
        other => panic!("expected ShoveError::Topology, got {other:?}"),
    }
    broker.close().await;
}

/// Cancelling the shutdown token flushes a partially-filled batch instead of
/// discarding it.
#[tokio::test]
async fn shutdown_flushes_the_pending_partial_batch() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<ShutdownTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<ShutdownTopic>(&publisher, 0..2).await;

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
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
                        .with_max_batch_size(1000)
                        .with_max_batch_age(Duration::from_secs(3600))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        handler.batches().is_empty(),
        "neither trigger should have fired before shutdown, got {:?}",
        handler.batches()
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(seen, vec![0, 1]);
    broker.close().await;
}

/// After a `Retry`-then-`Ack` sequence, both the queue's backlog and its
/// broker-side in-flight count must return to zero — proving every popped
/// envelope's `in_flight` increment is matched by exactly one decrement,
/// whichever settlement resolved it.
#[cfg(feature = "metrics")]
#[tokio::test(flavor = "current_thread")]
async fn in_flight_balance_after_retry_then_ack() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker.topology().declare::<InFlightTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<InFlightTopic>(&publisher, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<InFlightTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(2, TIMEOUT).await);
    shutdown.cancel();
    handle.await.unwrap().ok();

    let sampler = broker.queue_depth_sampler().watch("inmem-batch-inflight");
    sampler.sample_once().await;
    let snapshot = snapshotter.snapshot().into_hashmap();

    let gauge = |name: &str| -> Option<f64> {
        snapshot.iter().find_map(|(k, (_, _, value))| {
            let key = k.key();
            let matches = key.name() == name
                && key
                    .labels()
                    .any(|l| l.key() == "topic" && l.value() == "inmem-batch-inflight");
            match (matches, value) {
                (true, DebugValue::Gauge(v)) => Some(v.into_inner()),
                _ => None,
            }
        })
    };

    assert_eq!(
        gauge("shove_queue_backlog"),
        Some(0.0),
        "the queue must be fully drained after the ack"
    );
    assert_eq!(
        gauge("shove_queue_inflight"),
        Some(0.0),
        "in_flight must return to zero: every pop's increment must be matched \
         by exactly one decrement on whichever settlement resolved it"
    );
    broker.close().await;
}

/// `broker.close()` must cut the `Redeliver` arm's backoff sleep short, the
/// same way the per-consumer `shutdown` token already does — not stall the
/// drain for the (possibly escalated) remainder of the delay. Two
/// consecutive `Retry`s escalate the backoff from the 1s initial delay to 2s,
/// so "the run merely finished inside a generous timeout" cannot pass by
/// accident: only actually cutting the sleep short gets under the assertion
/// below.
#[tokio::test]
async fn broker_close_cuts_the_redelivery_backoff_short() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<BrokerCloseBackoffTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<BrokerCloseBackoffTopic>(&publisher, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry, Outcome::Retry]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<BrokerCloseBackoffTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    // First flush returns `Retry`: the batch is now sleeping the 1s initial
    // backoff, about to redeliver and flush a second time, escalating to 2s.
    assert!(handler.wait_for_batches(1, TIMEOUT).await);

    // Closing the broker here — never the per-consumer `shutdown` token —
    // must be enough on its own. Whichever backoff sleep is in flight when
    // this lands (1s or the escalated 2s), the consumer must stop promptly
    // rather than sleeping it out: the redelivery itself still happens
    // (unconditionally, after the race), only the delay is cut.
    let closed_at = Instant::now();
    broker.close().await;

    tokio::time::timeout(Duration::from_millis(500), handle)
        .await
        .expect(
            "broker close must cut the redelivery backoff short, not stall \
             the consumer for the rest of the (up to 2s) escalated delay",
        )
        .expect("consumer task must not panic")
        .ok();

    assert!(
        closed_at.elapsed() < Duration::from_millis(500),
        "broker close took {:?} to take effect",
        closed_at.elapsed()
    );
}

/// Every envelope requeued by `Redeliver` must be marked redelivered —
/// mirroring the single-message `Defer` nak-in-place convention — so
/// `MessageMetadata::delivery_count` actually climbs across a batch-wide
/// redelivery instead of reading `Some(1)` forever.
#[tokio::test]
async fn redelivery_increments_delivery_count_for_every_envelope() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<DeliveryCountTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<DeliveryCountTopic>(&publisher, 0..2).await;

    let handler = DeliveryCountRecordingHandler::new().scripting([Outcome::Retry]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<DeliveryCountTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(2, TIMEOUT).await);
    shutdown.cancel();
    handle.await.unwrap().ok();

    let batches = handler.batches();
    assert_eq!(batches.len(), 2, "got {batches:?}");

    let mut first = batches[0].clone();
    first.sort_by_key(|(seq, _)| *seq);
    assert_eq!(
        first,
        vec![(0, Some(1)), (1, Some(1))],
        "the first delivery must report delivery_count == 1"
    );

    let mut second = batches[1].clone();
    second.sort_by_key(|(seq, _)| *seq);
    assert_eq!(
        second,
        vec![(0, Some(2)), (1, Some(2))],
        "every envelope in a redelivered batch must have delivery_count \
         incremented, not stuck at 1"
    );
    broker.close().await;
}

/// A pre-handler drop with a DLQ configured is *parked*, not destroyed, and
/// must travel with the rest of the batch through a `Retry` in true arrival
/// order. The pre-fix redelivery order was `[handled..][parked..]`
/// unconditionally, so poison that arrived *first* would permanently drop
/// behind messages that arrived after it.
///
/// Detected by re-consuming the redelivered queue with a `max_batch_size`
/// small enough that where poison sits changes which handled message shares
/// its flush: with poison correctly in front, it consumes one of the two
/// size-cap slots in the very next flush, leaving only `seq=0` in it. With
/// poison wrongly shoved to the back, both handled messages fill that flush
/// together and poison is deferred to one of its own.
#[tokio::test]
async fn poison_keeps_its_arrival_position_across_a_retry() {
    let broker = Broker::<InMemory>::new(InMemoryConfig::default())
        .await
        .expect("connect inmemory");
    broker
        .topology()
        .declare::<RedeliveryOrderTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();

    // Poison arrives first — oversized under the `with_max_message_size`
    // both consumer phases below set, so it is parked (not decoded) on every
    // pop, forever.
    let poison = BatchMessage {
        seq: 999,
        padding: "x".repeat(4096),
    };
    publisher
        .publish::<RedeliveryOrderTopic>(&poison)
        .await
        .unwrap();
    publish_seq::<RedeliveryOrderTopic>(&publisher, 0..2).await;

    // Phase 1: gather all three (poison + seq 0 + seq 1) into one flush and
    // force exactly one whole-batch redelivery.
    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<RedeliveryOrderTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(3)
                        .with_max_batch_age(Duration::from_secs(30))
                        .with_max_message_size(512)
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    assert_eq!(sorted(&handler.batches()[0]), vec![0, 1]);
    // The redelivery always happens regardless of when `shutdown` fires (see
    // `flush_inmemory_batch`'s `Redeliver` arm: the requeue is unconditional
    // after the backoff race) — cancelling here stops this consumer before
    // it re-pops the just-requeued batch itself, so phase 2 below observes
    // the redelivered queue order fresh.
    shutdown.cancel();
    handle.await.unwrap().ok();

    // Phase 2: a fresh consumer with `max_batch_size(2)` re-reads the
    // redelivered queue. Correct order (poison, seq0, seq1) spends one slot
    // on poison, so the first flush the handler sees is `[0]` alone.
    let handler2 = RecordingBatchHandler::new();
    let shutdown2 = CancellationToken::new();
    let consumer2 = broker.batch_consumer();
    let handle2 = tokio::spawn({
        let handler2 = handler2.clone();
        let shutdown2 = shutdown2.clone();
        async move {
            consumer2
                .run::<RedeliveryOrderTopic, _>(
                    handler2,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(2)
                        .with_max_batch_age(Duration::from_millis(300))
                        .with_max_message_size(512)
                        .with_shutdown(shutdown2),
                )
                .await
        }
    });

    assert!(handler2.wait_for_batches(1, TIMEOUT).await);
    shutdown2.cancel();
    handle2.await.unwrap().ok();

    assert_eq!(
        handler2.batches()[0],
        vec![0],
        "poison must keep its front-of-queue arrival position across the \
         retry: the first post-redelivery flush should spend one of its two \
         size-cap slots on poison, leaving seq=0 alone in it, got {:?}",
        handler2.batches()
    );
    broker.close().await;
}
