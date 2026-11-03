//! Integration tests for the NATS JetStream batch consumer
//! (`Broker::<Nats>::batch_consumer()`).
//!
//! These cover what only a real server exercises: the size- and age-triggered
//! flush boundaries of the pull-batch request, per-message ack retirement on
//! `Ack`, whole-batch Nak redelivery on `Retry`/`Defer`, the per-message
//! DLQ-publish-then-ack path on `Reject`, the drop-from-batch arms for
//! oversized/undeserializable payloads, the runtime sequencing guard, the
//! `max_ack_pending` clamp, and the flush-on-shutdown drain. They also cover
//! the batch read ahead of a flush and its hand-back on a stop, the background
//! settlement of poison messages, the statuses a pull can end with, and the
//! pull a reconnect replaces.

#![cfg(feature = "nats")]

use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
use async_nats::jetstream::stream::Config as StreamConfig;
use serde::{Deserialize, Serialize};
use shove::BatchConsumerOptions;
use shove::broker::Broker;
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::markers::Nats;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::nats::{NatsClient, NatsConfig, NatsConsumer};
use shove::outcome::Outcome;
use shove::topic::{NotSequenced, Topic};
use shove::topology::{QueueTopology, SequenceFailure, TopologyBuilder};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::subscriber::{DefaultGuard, set_default};

const TIMEOUT: Duration = Duration::from_secs(15);

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
    TopologyBuilder::new("nats-batch-size").build()
);

shove::define_topic!(
    AgeTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-age").build()
);

shove::define_topic!(
    CommitTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-commit").build()
);

shove::define_topic!(
    RedeliverTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-redeliver").build()
);

shove::define_topic!(
    PanicTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-panic").build()
);

shove::define_topic!(
    DeferTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-defer").build()
);

shove::define_topic!(
    RejectDlqTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-reject-dlq").dlq().build()
);

shove::define_topic!(
    RejectNoDlqTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-reject-nodlq").build()
);

shove::define_topic!(
    TimeoutTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-timeout").build()
);

shove::define_topic!(
    TimeoutOutcomeTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-timeout-outcome").build()
);

shove::define_topic!(
    ShutdownFlushTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-shutdown-flush").build()
);

shove::define_topic!(
    HungShutdownTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-hung-shutdown").build()
);

shove::define_topic!(
    ClampTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-clamp").build()
);

shove::define_topic!(
    PoisonTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-poison").dlq().build()
);

shove::define_topic!(
    OversizeTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-oversize").dlq().build()
);

shove::define_topic!(
    IdleTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-idle").build()
);

shove::define_topic!(
    SupplyTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-supply").build()
);

shove::define_topic!(
    ReadAheadTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-readahead").build()
);

shove::define_topic!(
    HandBackTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-handback").build()
);

shove::define_topic!(
    RenewReadAheadTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-renew-read-ahead").build()
);

shove::define_topic!(
    StopRaceTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-stop-race").build()
);

shove::define_topic!(
    AbortFlushTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-abort-flush").build()
);

shove::define_topic!(
    PoisonStallTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-poison-stall")
        .dlq()
        .build()
);

shove::define_topic!(
    RefusedPullTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-refused").build()
);

shove::define_topic!(
    ReconnectTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-reconnect").build()
);

shove::define_topic!(
    RenewTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-renew").dlq().build()
);

shove::define_topic!(
    BudgetTopic,
    BatchMessage,
    TopologyBuilder::new("nats-batch-budget").build()
);

/// A message that is only an enum. A payload naming a variant it does not have
/// fails to decode, and serde quotes that variant in its error verbatim.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Shape {
    Circle,
    Square,
}

shove::define_topic!(
    ShapeTopic,
    Shape,
    TopologyBuilder::new("nats-batch-shape").dlq().build()
);

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct TestBroker {
    _container: testcontainers::ContainerAsync<NatsContainer>,
    client: NatsClient,
}

impl TestBroker {
    async fn start() -> Self {
        let cmd = NatsServerCmd::default().with_jetstream();
        let container = NatsContainer::default()
            .with_cmd(&cmd)
            .start()
            .await
            .expect("failed to start NATS container");
        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(4222)
            .await
            .expect("failed to get NATS port");
        let nats_url = format!("nats://{host}:{port}");

        let client = NatsClient::connect_with_retry(&NatsConfig::new(&nats_url), 10)
            .await
            .expect("failed to connect to NATS");

        Self {
            _container: container,
            client,
        }
    }

    fn broker(&self) -> Broker<Nats> {
        Broker::<Nats>::from_client(self.client.clone())
    }

    fn client(&self) -> NatsClient {
        self.client.clone()
    }
}

async fn publish_seq<T>(broker: &Broker<Nats>, range: std::ops::Range<u32>)
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

fn sorted(batch: &[u32]) -> Vec<u32> {
    let mut v = batch.to_vec();
    v.sort_unstable();
    v
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
    CommitTopic,
    RedeliverTopic,
    DeferTopic,
    RejectDlqTopic,
    RejectNoDlqTopic,
    ShutdownFlushTopic,
    ClampTopic,
    PoisonTopic,
    OversizeTopic,
    IdleTopic,
    SupplyTopic,
    HandBackTopic,
    PoisonStallTopic,
    RefusedPullTopic,
    ReconnectTopic,
    RenewTopic,
    BudgetTopic,
    StopRaceTopic,
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

/// A handler that panics or hangs, to prove the batch loop survives both.
#[derive(Clone)]
struct MisbehavingBatchHandler {
    mode: Misbehaviour,
    /// The `seq` list handed to each flush, in order. Recorded *before*
    /// misbehaving, so a panicking call still shows up.
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

impl_misbehaving_for!(
    PanicTopic,
    TimeoutTopic,
    TimeoutOutcomeTopic,
    HungShutdownTopic
);

// ---------------------------------------------------------------------------
// DLQ recording handler
// ---------------------------------------------------------------------------

/// Counts what `run_dlq` hands to `handle_dead`, for asserting a rejected
/// batch's messages actually landed in the DLQ.
#[derive(Clone)]
struct DlqRecordingHandler {
    seqs: Arc<Mutex<Vec<u32>>>,
    signal: Arc<Notify>,
}

impl DlqRecordingHandler {
    fn new() -> Self {
        Self {
            seqs: Arc::new(Mutex::new(Vec::new())),
            signal: Arc::new(Notify::new()),
        }
    }

    fn seqs(&self) -> Vec<u32> {
        self.seqs.lock().unwrap().clone()
    }

    async fn wait_for(&self, n: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.seqs.lock().unwrap().len() >= n {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return self.seqs.lock().unwrap().len() >= n;
                }
            }
        }
    }
}

impl MessageHandler<RejectDlqTopic> for DlqRecordingHandler {
    type Context = ();
    async fn handle(&self, _msg: BatchMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, msg: BatchMessage, _meta: DeadMessageMetadata, _: &()) {
        self.seqs.lock().unwrap().push(msg.seq);
        self.signal.notify_waiters();
    }
}

/// Poll a stream's message count until it reaches `expected` or the deadline
/// passes; returns the last observed count.
async fn wait_for_stream_count(
    client: &NatsClient,
    stream_name: &str,
    expected: u64,
    timeout: Duration,
) -> u64 {
    let deadline = Instant::now() + timeout;
    let mut last = 0;
    loop {
        if let Ok(mut stream) = client.jetstream().get_stream(stream_name).await
            && let Ok(info) = stream.info().await
        {
            last = info.state.messages;
            if last >= expected {
                return last;
            }
        }
        if Instant::now() >= deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A stream's current message count, 0 when it does not exist yet.
async fn stream_count(client: &NatsClient, stream_name: &str) -> u64 {
    match client.jetstream().get_stream(stream_name).await {
        Ok(mut stream) => stream
            .info()
            .await
            .map(|info| info.state.messages)
            .unwrap_or(0),
        Err(_) => 0,
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
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<SizeTopic, _>(
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
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<AgeTopic, _>(
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

/// Regression: with several workers attached to the bootstrap durable, a
/// pre-filled stream must drain at the speed of supply, not at
/// `max_batch_size / max_batch_age`.
///
/// JetStream serves every concurrent pull request on a durable from one shared
/// `max_ack_pending` budget. When the bootstrap sized that budget to a single
/// batch, two workers' overlapping pulls each received a *partial* fill, and a
/// partial pull cannot terminate by count — it stays pinned open until the
/// server-side `expires` (= `max_batch_age`) fires. Throughput then collapses
/// to `budget / age` no matter how many messages are waiting.
///
/// Two gates, deliberately: one on the mechanism and one on the clock.
///
/// The **mechanism** gate is the primary one, because it does not depend on
/// how fast the machine is. Under the bug every concurrent pull gets a partial
/// fill, so almost no flushed batch reaches `max_batch_size`; when supply is
/// what bounds the drain, nearly all of them do (only the tail is short). This
/// is asserted on the recorded batch sizes, with no clock in it — which
/// matters because the `nats` CI leg runs this suite under
/// `cargo llvm-cov nextest`, fully instrumented, on a shared runner.
///
/// The **clock** gate covers what the mechanism gate cannot: a regression that
/// fills its batches but still spends a window per round (no pipelining) would
/// pass the fill check. Full batches means `MESSAGES / BATCH_SIZE` flushes
/// split across the workers, so that shape costs
/// `(3000/100 / 2) × 0.5s = 7.5s`; the shared-budget defect costs the full
/// `(3000/100) × 0.5s = 15s`. The ceiling is 40% of that floor — 6s — which
/// sits below both. Measured on an idle host: 267ms supply-bound, 13.1s and
/// 15.2s under the defect. That is ~22x headroom for instrumentation
/// (`cargo llvm-cov` on the CI leg) and runner contention to eat into before a
/// correct run goes red. The clock starts after the fully-awaited publish
/// loop, so publish RTTs are outside the window, and the elapsed time is
/// printed either way.
///
/// The two-worker premise is asserted, not assumed: a silently dead second
/// worker would turn this into a single-worker drain, which is the one
/// configuration the bug never affected, and it would pass.
#[tokio::test]
async fn a_prefilled_stream_drains_bound_by_supply_not_age() {
    const MESSAGES: u32 = 3_000;
    const BATCH_SIZE: u32 = 100;
    const BATCH_AGE: Duration = Duration::from_millis(500);
    const WORKERS: usize = 2;

    // What the bug costs: one `max_batch_age` window per batch, whatever the
    // supply. Anything at or above this is the defect, not a slow runner.
    let expiry_bound_floor = BATCH_AGE * MESSAGES.div_euclid(BATCH_SIZE);
    let ceiling = expiry_bound_floor.mul_f64(0.4);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<SupplyTopic>().await.unwrap();
    publish_seq::<SupplyTopic>(&broker, 0..MESSAGES).await;

    // One handler per worker rather than a shared one: which worker flushed
    // what is exactly the evidence the two-worker premise needs.
    let handlers: Vec<RecordingBatchHandler> =
        (0..WORKERS).map(|_| RecordingBatchHandler::new()).collect();
    let shutdown = CancellationToken::new();
    let mut handles = Vec::new();
    for handler in &handlers {
        let h = handler.clone();
        let sc = shutdown.clone();
        let consumer = broker.batch_consumer();
        handles.push(tokio::spawn(async move {
            consumer
                .run::<SupplyTopic, _>(
                    h,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(BATCH_SIZE as usize)
                        .with_max_batch_age(BATCH_AGE)
                        .with_shutdown(sc),
                )
                .await
        }));
    }

    // Distinct `seq` values across every worker, so an at-least-once
    // redelivery cannot satisfy the wait early.
    let unique_seen = |handlers: &[RecordingBatchHandler]| {
        let mut seen: Vec<u32> = handlers
            .iter()
            .flat_map(RecordingBatchHandler::seen)
            .collect();
        seen.sort_unstable();
        seen.dedup();
        seen.len()
    };

    // A 10ms poll rather than the handlers' `Notify`: the drain is what this
    // test measures, and a poll cannot lose a wakeup and bill the wait to the
    // measurement. The 40s bound is far above the buggy floor so a regression
    // fails on the assertions below, not on a hung wait.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(40);
    while unique_seen(&handlers) < MESSAGES as usize && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let elapsed = started.elapsed();
    let seen = unique_seen(&handlers);
    shutdown.cancel();
    let outcomes: Vec<_> = {
        let mut v = Vec::new();
        for handle in handles {
            v.push(handle.await.unwrap());
        }
        v
    };

    let per_worker: Vec<usize> = handlers.iter().map(|h| h.batches().len()).collect();
    let sizes: Vec<usize> = handlers
        .iter()
        .flat_map(|h| h.batches())
        .map(|b| b.len())
        .collect();
    let full = sizes.iter().filter(|n| **n >= BATCH_SIZE as usize).count();
    eprintln!(
        "drained {seen}/{MESSAGES} in {elapsed:?} (ceiling {ceiling:?}, expiry-bound \
         floor {expiry_bound_floor:?}); {full}/{} batches reached {BATCH_SIZE}; \
         per-worker flushes {per_worker:?}",
        sizes.len()
    );

    for (worker, outcome) in outcomes.iter().enumerate() {
        assert!(
            outcome.is_ok(),
            "batch worker {worker} returned an error: {outcome:?}"
        );
    }
    assert!(
        per_worker.iter().all(|n| *n > 0),
        "every worker must have flushed at least one batch, or this is not the \
         multi-worker configuration the bug needed: {per_worker:?}"
    );
    assert_eq!(
        seen, MESSAGES as usize,
        "expected all {MESSAGES} messages to be consumed"
    );
    // The mechanism: a pull that cannot fill is the bug. Half is a wide
    // margin either way — under the defect essentially none fill, and when
    // supply bounds the drain only the tail batches are short.
    assert!(
        full * 2 > sizes.len(),
        "only {full} of {} flushed batches reached {BATCH_SIZE}: pulls are \
         being served partial fills and waiting out max_batch_age",
        sizes.len()
    );
    assert!(
        elapsed < ceiling,
        "drain took {elapsed:?}, over the {ceiling:?} ceiling: bound by \
         max_batch_age (expiry-bound floor {expiry_bound_floor:?}), not by supply"
    );
    broker.close().await;
}

/// An acked batch is retired: nothing is redelivered once the handler has
/// returned `Ack`, even though the consumer keeps polling well past several
/// ack-wait-sized windows.
#[tokio::test]
async fn an_acked_batch_is_retired_not_redelivered() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<CommitTopic>().await.unwrap();
    publish_seq::<CommitTopic>(&broker, 0..4).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<CommitTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(4)
                    .with_max_batch_age(Duration::from_millis(200))
                    // A short handler timeout keeps the derived ack_wait at its
                    // 30s floor; the redelivery probe below stays well under it
                    // but far over the flush cadence.
                    .with_handler_timeout(Duration::from_secs(1))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "the published batch should flush"
    );
    // Keep consuming across many empty windows: a redelivery would show up as
    // a second non-empty flush.
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![0, 1, 2, 3],
        "an acked batch must not be redelivered, got batches {:?}",
        handler.batches()
    );
    broker.close().await;
}

/// A non-`Ack` outcome redelivers the **whole batch**, not a subset. The
/// redelivery may split across pull windows, so the assertion accumulates
/// across flushes rather than expecting one identical second batch.
#[tokio::test]
async fn non_ack_outcome_redelivers_the_whole_batch() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<RedeliverTopic>().await.unwrap();
    publish_seq::<RedeliverTopic>(&broker, 0..3).await;

    // First flush retries (Nak), everything after acks.
    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry]);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<RedeliverTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_batches(2, TIMEOUT).await,
        "the retried batch should be redelivered, got {:?}",
        handler.batches()
    );
    // Wait until every message has been seen at least twice (once naked, once
    // redelivered), however the redelivery splits across windows.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let seen = handler.seen();
        let all_twice = (0..3u32).all(|s| seen.iter().filter(|&&x| x == s).count() >= 2);
        if all_twice || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown.cancel();
    handle.await.unwrap().ok();

    let batches = handler.batches();
    assert_eq!(
        sorted(&batches[0]),
        vec![0, 1, 2],
        "the first flush should carry the full batch, got {batches:?}"
    );
    let seen = handler.seen();
    for s in 0..3u32 {
        assert!(
            seen.iter().filter(|&&x| x == s).count() >= 2,
            "message {s} was not redelivered after Retry, batches {batches:?}"
        );
    }
    broker.close().await;
}

/// A panicking flush is a redelivery, not the end of the consumer: the batch
/// comes back and the second flush acks it.
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
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<PanicTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_calls(2, TIMEOUT).await,
        "the batch should be redelivered after the panic, got {:?}",
        handler.calls()
    );
    // The panicked flush acked nothing: every message must come back.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let seen: Vec<u32> = handler.calls().into_iter().flatten().collect();
        let all_twice = (0..3u32).all(|s| seen.iter().filter(|&&x| x == s).count() >= 2);
        if all_twice || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown.cancel();
    handle.await.unwrap().ok();

    let calls = handler.calls();
    assert_eq!(
        sorted(&calls[0]),
        vec![0, 1, 2],
        "the panicking flush should have received the full batch"
    );
    let seen: Vec<u32> = calls.iter().flatten().copied().collect();
    for s in 0..3u32 {
        assert!(
            seen.iter().filter(|&&x| x == s).count() >= 2,
            "message {s} was acked by a panicking flush, calls {calls:?}"
        );
    }
    broker.close().await;
}

/// `Defer` settles exactly like `Retry` on a batch: the whole batch is
/// redelivered, no retry budget is spent, no hold queue is involved.
#[tokio::test]
async fn defer_settles_like_retry() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<DeferTopic>().await.unwrap();
    publish_seq::<DeferTopic>(&broker, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Defer]);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<DeferTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_batches(2, TIMEOUT).await,
        "the deferred batch should be redelivered, got {:?}",
        handler.batches()
    );
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let seen = handler.seen();
        let all_twice = (0..3u32).all(|s| seen.iter().filter(|&&x| x == s).count() >= 2);
        if all_twice || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown.cancel();
    handle.await.unwrap().ok();

    let seen = handler.seen();
    for s in 0..3u32 {
        assert!(
            seen.iter().filter(|&&x| x == s).count() >= 2,
            "message {s} was not redelivered after Defer, batches {:?}",
            handler.batches()
        );
    }
    broker.close().await;
}

/// A rejected batch is terminal: every message lands in the DLQ and the
/// batch is never redelivered to the handler.
#[tokio::test]
async fn rejected_batch_lands_in_the_dlq_and_is_retired() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<RejectDlqTopic>().await.unwrap();
    publish_seq::<RejectDlqTopic>(&broker, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Reject]);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<RejectDlqTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    let dlq_handler = DlqRecordingHandler::new();
    let dhc = dlq_handler.clone();
    let dlq_consumer = NatsConsumer::new(client.clone());
    let dlq_handle =
        tokio::spawn(async move { dlq_consumer.run_dlq::<RejectDlqTopic, _>(dhc, ()).await });

    assert!(
        dlq_handler.wait_for(3, TIMEOUT).await,
        "the whole rejected batch should land in the DLQ, got {:?}",
        dlq_handler.seqs()
    );
    assert_eq!(sorted(&dlq_handler.seqs()), vec![0, 1, 2]);

    // Terminal means terminal: no second flush arrives.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        handler.batches().len(),
        1,
        "a rejected batch must not be redelivered, got {:?}",
        handler.batches()
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
    dlq_handle.await.unwrap().ok();
}

/// A rejected batch on a topic with no DLQ is discarded — retired from the
/// stream, never redelivered.
#[tokio::test]
async fn rejected_batch_without_a_dlq_is_discarded() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<RejectNoDlqTopic>()
        .await
        .unwrap();
    publish_seq::<RejectNoDlqTopic>(&broker, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Reject]);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<RejectNoDlqTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "the batch should flush once"
    );

    // The WorkQueue stream retires acked messages, so a fully-settled reject
    // leaves it empty.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut stream = client
            .jetstream()
            .get_stream("nats-batch-reject-nodlq")
            .await
            .expect("stream should exist");
        let messages = stream.info().await.expect("stream info").state.messages;
        if messages == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rejected messages should be retired from the stream, {messages} remain"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // And nothing comes back to the handler.
    tokio::time::sleep(Duration::from_secs(1)).await;
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

/// A flush that outlasts the handler timeout resolves to the default
/// `Retry`: the whole batch is redelivered and the consumer survives.
#[tokio::test]
async fn a_flush_outlasting_the_handler_timeout_is_redelivered() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<TimeoutTopic>().await.unwrap();
    publish_seq::<TimeoutTopic>(&broker, 0..3).await;

    let handler = MisbehavingBatchHandler::new(Misbehaviour::HangOnce);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<TimeoutTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_handler_timeout(Duration::from_millis(500))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_calls(2, TIMEOUT).await,
        "the timed-out batch should be redelivered, got {:?}",
        handler.calls()
    );
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let seen: Vec<u32> = handler.calls().into_iter().flatten().collect();
        let all_twice = (0..3u32).all(|s| seen.iter().filter(|&&x| x == s).count() >= 2);
        if all_twice || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown.cancel();
    handle.await.unwrap().ok();

    let seen: Vec<u32> = handler.calls().into_iter().flatten().collect();
    for s in 0..3u32 {
        assert!(
            seen.iter().filter(|&&x| x == s).count() >= 2,
            "message {s} was acked by a timed-out flush, calls {:?}",
            handler.calls()
        );
    }
    broker.close().await;
}

/// `with_handler_timeout_outcome(Ack)` makes a timed-out flush commit the
/// batch instead of redelivering it.
#[tokio::test]
async fn the_configured_timeout_outcome_is_honoured() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<TimeoutOutcomeTopic>()
        .await
        .unwrap();
    publish_seq::<TimeoutOutcomeTopic>(&broker, 0..3).await;

    let handler = MisbehavingBatchHandler::new(Misbehaviour::HangForever);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<TimeoutOutcomeTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_handler_timeout(Duration::from_millis(500))
                    .with_handler_timeout_outcome(Outcome::Ack)
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_calls(1, TIMEOUT).await,
        "the batch should flush once"
    );
    // Ack-on-timeout retires the batch: no second call ever arrives.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        handler.calls().len(),
        1,
        "a timeout resolved to Ack must not redeliver, got {:?}",
        handler.calls()
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// The runtime sequencing guard: a topic that hand-implements `NotSequenced`
/// while its topology declares sequencing config is rejected before any
/// consumption starts.
#[tokio::test]
async fn run_batch_rejects_a_topic_that_declares_sequencing() {
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Entry {
        account: String,
    }

    struct LiesAboutSequencing;
    impl Topic for LiesAboutSequencing {
        type Message = Entry;
        type Codec = shove::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("nats-batch-guard-ledger")
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
        async fn handle_batch(&self, _messages: Vec<(Entry, MessageMetadata)>, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    let tb = TestBroker::start().await;
    let broker = tb.broker();

    let err = broker
        .batch_consumer()
        .run::<LiesAboutSequencing, _>(NoopHandler, (), BatchConsumerOptions::new())
        .await
        .expect_err("a sequenced topology must be rejected");
    assert!(
        err.to_string().contains("mutually exclusive"),
        "unexpected error: {err}"
    );
    broker.close().await;
}

/// Shutdown flushes the in-flight partial batch instead of stranding it:
/// messages already pulled into the window reach the handler even though
/// neither the size nor the age trigger ever fired.
#[tokio::test]
async fn shutdown_flushes_the_in_flight_partial_batch() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<ShutdownFlushTopic>()
        .await
        .unwrap();
    publish_seq::<ShutdownFlushTopic>(&broker, 0..2).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<ShutdownFlushTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    // Neither trigger can fire on its own within the test.
                    .with_max_batch_size(10)
                    .with_max_batch_age(Duration::from_secs(30))
                    .with_shutdown(sc),
            )
            .await
    });

    // Let the pull window pick both messages up, then interrupt it.
    tokio::time::sleep(Duration::from_millis(600)).await;
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("shutdown should complete promptly, not wait out the window");
    result.unwrap().ok();

    assert_eq!(
        handler.batches(),
        vec![vec![0, 1]],
        "the partial batch should flush exactly once at shutdown"
    );
    broker.close().await;
}

/// Shutdown completes while a flush is hung: the handler timeout resolves the
/// flush, and the loop exits instead of waiting on the handler forever.
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
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<HungShutdownTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_handler_timeout(Duration::from_secs(1))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_calls(1, TIMEOUT).await,
        "the batch should reach the handler"
    );
    // Fire shutdown while the flush is hung inside the handler.
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("shutdown should complete once the handler timeout resolves the flush");
    result.unwrap().ok();
    broker.close().await;
}

/// A pre-declared durable with `max_ack_pending` below `max_batch_size`
/// clamps the per-pull size instead of deadlocking: everything is still
/// consumed, in batches no larger than the budget.
#[tokio::test]
async fn batch_size_is_clamped_to_max_ack_pending() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<ClampTopic>().await.unwrap();

    // Pre-declare the durable the way a registry would, with a tiny budget.
    let stream = client
        .jetstream()
        .get_stream("nats-batch-clamp")
        .await
        .expect("stream should exist");
    stream
        .create_consumer(PullConsumerConfig {
            durable_name: Some("nats-batch-clamp-consumer".to_string()),
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 2,
            ack_wait: Duration::from_secs(60),
            ..Default::default()
        })
        .await
        .expect("pre-declaring the durable should succeed");

    publish_seq::<ClampTopic>(&broker, 0..6).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<ClampTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(10)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    // All six messages arrive despite the budget being far below the batch
    // size — the clamp guarantees progress.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut seen = handler.seen();
        seen.sort_unstable();
        seen.dedup();
        if seen == (0..6).collect::<Vec<_>>() || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen, (0..6).collect::<Vec<_>>());
    assert!(
        handler.batches().iter().all(|b| b.len() <= 2),
        "no batch may exceed the max_ack_pending budget, got {:?}",
        handler.batches()
    );
    broker.close().await;
}

/// An undeserializable message is dropped from the batch to the DLQ and the
/// rest of the window still reaches the handler.
#[tokio::test]
async fn undeserializable_message_is_dropped_to_the_dlq_and_the_rest_flush() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<PoisonTopic>().await.unwrap();

    // Earlier dead letters persist on a shared long-lived server, so the DLQ
    // assertion below is a delta over this baseline.
    let dead_before = stream_count(&client, "nats-batch-poison-dlq").await;

    // Straight through JetStream, bypassing shove's codec — the only way to
    // land a body `T::Codec` cannot decode. Carries a unique message id the
    // way any real publisher does.
    let mut poison_headers = async_nats::HeaderMap::new();
    poison_headers.insert(
        NATS_MESSAGE_ID,
        format!(
            "poison-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        )
        .as_str(),
    );
    client
        .jetstream()
        .publish_with_headers(
            "nats-batch-poison",
            poison_headers,
            "not json at all".into(),
        )
        .await
        .expect("raw publish should succeed")
        .await
        .expect("raw publish should be acked");
    publish_seq::<PoisonTopic>(&broker, 0..2).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<PoisonTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "the surviving messages should flush"
    );
    let dlq_count =
        wait_for_stream_count(&client, "nats-batch-poison-dlq", dead_before + 1, TIMEOUT).await;
    assert_eq!(
        dlq_count,
        dead_before + 1,
        "the poison payload should land in the DLQ"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(
        seen,
        vec![0, 1],
        "the two decodable messages should reach the handler, got {:?}",
        handler.batches()
    );
    broker.close().await;
}

/// An oversized message is dropped from the batch to the DLQ and the rest of
/// the window still reaches the handler.
#[tokio::test]
async fn oversized_message_is_dropped_to_the_dlq_and_the_rest_flush() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<OversizeTopic>().await.unwrap();

    // Same delta treatment as the poison test: a shared server accumulates.
    let dead_before = stream_count(&client, "nats-batch-oversize-dlq").await;

    let publisher = broker.publisher().await.unwrap();
    publisher
        .publish::<OversizeTopic>(&BatchMessage {
            seq: 99,
            padding: "x".repeat(256),
        })
        .await
        .expect("publish should succeed");
    publish_seq::<OversizeTopic>(&broker, 0..2).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<OversizeTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_max_message_size(128)
                    .with_shutdown(sc),
            )
            .await
    });

    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "the surviving messages should flush"
    );
    let dlq_count =
        wait_for_stream_count(&client, "nats-batch-oversize-dlq", dead_before + 1, TIMEOUT).await;
    assert_eq!(
        dlq_count,
        dead_before + 1,
        "the oversized payload should land in the DLQ"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(
        seen,
        vec![0, 1],
        "the two in-size messages should reach the handler, got {:?}",
        handler.batches()
    );
    broker.close().await;
}

/// Keeps every `Shape` a flush is handed.
#[derive(Clone)]
struct ShapeBatchRecorder(Arc<Mutex<Vec<Shape>>>);

impl BatchMessageHandler<ShapeTopic> for ShapeBatchRecorder {
    type Context = ();
    async fn handle_batch(&self, messages: Vec<(Shape, MessageMetadata)>, _: &()) -> Outcome {
        self.0
            .lock()
            .unwrap()
            .extend(messages.into_iter().map(|(shape, _)| shape));
        Outcome::Ack
    }
}

/// serde quotes an unknown variant verbatim, so `"a\nb"` fails to decode with
/// a raw newline in the error text. The batch intake dead-letters it under the
/// fixed reason instead of that text, and the consumer flushes what follows.
#[tokio::test]
async fn decode_error_with_newline_dead_letters_without_panic() {
    use futures_util::StreamExt;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<ShapeTopic>().await.unwrap();
    client
        .jetstream()
        .publish("nats-batch-shape", br#""a\nb""#.as_slice().into())
        .await
        .expect("raw publish should succeed")
        .await
        .expect("raw publish should be acked");

    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = ShapeBatchRecorder(seen.clone());
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<ShapeTopic, _>(
                recorder,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    let reader = client
        .jetstream()
        .get_stream("nats-batch-shape-dlq")
        .await
        .expect("the DLQ stream exists")
        .get_or_create_consumer(
            "test-dead-letter-reader",
            PullConsumerConfig {
                durable_name: Some("test-dead-letter-reader".into()),
                ..Default::default()
            },
        )
        .await
        .expect("the DLQ reader is created");
    let mut dead_letters = reader.messages().await.expect("the DLQ reader pulls");
    let dead = tokio::time::timeout(TIMEOUT, dead_letters.next())
        .await
        .expect("the poison payload reaches the DLQ")
        .expect("the DLQ reader stream stays open")
        .expect("the dead letter is readable");
    let reason = dead
        .headers
        .as_ref()
        .and_then(|headers| headers.get("Shove-Death-Reason"))
        .map(|value| value.as_str().to_string());
    assert_eq!(reason.as_deref(), Some("deserialization_error"));

    broker
        .publisher()
        .await
        .unwrap()
        .publish::<ShapeTopic>(&Shape::Circle)
        .await
        .expect("publish should succeed");
    let deadline = Instant::now() + TIMEOUT;
    while seen.lock().unwrap().is_empty() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        *seen.lock().unwrap(),
        vec![Shape::Circle],
        "the consumer outlives the poison message and flushes the next one"
    );

    shutdown.cancel();
    handle
        .await
        .expect("the batch consumer task does not panic")
        .expect("the batch consumer stops cleanly");
    broker.close().await;
}

/// An idle topic churns empty pull windows without killing the consumer —
/// messages published after several empty windows still arrive.
#[tokio::test]
async fn an_idle_consumer_survives_empty_windows_then_consumes() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<IdleTopic>().await.unwrap();

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<IdleTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(3)
                    .with_max_batch_age(Duration::from_millis(200))
                    .with_shutdown(sc),
            )
            .await
    });

    // Ride out several guaranteed-empty windows first.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(handler.batches().is_empty(), "nothing to flush while idle");

    publish_seq::<IdleTopic>(&broker, 0..3).await;
    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "messages published after idle windows should still arrive"
    );
    shutdown.cancel();
    handle.await.unwrap().ok();

    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(seen, vec![0, 1, 2]);
    broker.close().await;
}

// ---------------------------------------------------------------------------
// Read-ahead, background poison settlement, pull statuses, reconnects
// ---------------------------------------------------------------------------

/// How the first flush of a [`FirstFlushHandler`] is held up.
#[derive(Clone)]
enum Hold {
    /// The handler blocks its thread for this long, without yielding.
    BlockThread(Duration),
    /// The handler waits for a permit, which the test adds.
    Gate(Arc<tokio::sync::Semaphore>),
}

/// Holds its first flush up as told, and acks every flush. The seqs of every
/// flush are recorded as the flush starts.
#[derive(Clone)]
struct FirstFlushHandler {
    batches: Arc<Mutex<Vec<Vec<u32>>>>,
    started: Arc<Notify>,
    hold: Hold,
    held: Arc<AtomicBool>,
}

impl FirstFlushHandler {
    fn new(hold: Hold) -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            started: Arc::new(Notify::new()),
            hold,
            held: Arc::new(AtomicBool::new(true)),
        }
    }

    fn seen(&self) -> Vec<u32> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .copied()
            .collect()
    }
}

macro_rules! impl_first_flush_for {
    ($($topic:ty),* $(,)?) => {
        $(
            impl BatchMessageHandler<$topic> for FirstFlushHandler {
                type Context = ();
                async fn handle_batch(
                    &self,
                    messages: Vec<(BatchMessage, MessageMetadata)>,
                    _: &(),
                ) -> Outcome {
                    self.batches
                        .lock()
                        .unwrap()
                        .push(messages.iter().map(|(m, _)| m.seq).collect());
                    self.started.notify_one();
                    if self.held.swap(false, Ordering::SeqCst) {
                        match &self.hold {
                            Hold::BlockThread(time) => std::thread::sleep(*time),
                            Hold::Gate(gate) => gate
                                .acquire()
                                .await
                                .expect("the gate is never closed")
                                .forget(),
                        }
                    }
                    Outcome::Ack
                }
            }
        )*
    };
}

impl_first_flush_for!(ReadAheadTopic, HandBackTopic);

/// How the durable `consumer` of `stream` stands: its un-acked deliveries, its
/// messages still waiting in the stream and its redeliveries.
async fn durable_state(client: &NatsClient, stream: &str, consumer: &str) -> (usize, u64, usize) {
    let info = client
        .jetstream()
        .get_stream(stream)
        .await
        .expect("the stream exists")
        .consumer_info(consumer)
        .await
        .expect("the durable exists");
    (info.num_ack_pending, info.num_pending, info.num_redelivered)
}

/// Waits until the durable has `expected` deliveries out un-acked, and returns
/// the last count it saw.
async fn wait_for_ack_pending(
    client: &NatsClient,
    stream: &str,
    consumer: &str,
    expected: usize,
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let pending = durable_state(client, stream, consumer).await.0;
        if pending == expected || Instant::now() >= deadline {
            return pending;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// While a flush runs, the next batch is already pulled: the consumer keeps
/// one pull outstanding. Nothing is lost or handled twice across the
/// hand-over from one batch to the one read ahead, not even when the flush
/// holds its thread for longer than the pull's window and the 5 s the client
/// allows past it, after which the client abandons a pull and what it
/// delivered. The batch read ahead is read while the flush is held, so no
/// message waits on a redelivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_read_ahead_loses_and_duplicates_nothing() {
    const MESSAGES: u32 = 600;
    const BATCH_SIZE: usize = 100;
    const STREAM: &str = "nats-batch-readahead";
    const DURABLE: &str = "nats-batch-readahead-consumer";
    const WINDOW: Duration = Duration::from_millis(500);
    const HOLD: Duration = Duration::from_millis(6_500);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<ReadAheadTopic>().await.unwrap();
    publish_seq::<ReadAheadTopic>(&broker, 0..MESSAGES).await;

    let handler = FirstFlushHandler::new(Hold::BlockThread(HOLD));
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<ReadAheadTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(BATCH_SIZE)
                    .with_max_batch_age(WINDOW)
                    .with_shutdown(sc),
            )
            .await
    });

    tokio::time::timeout(TIMEOUT, handler.started.notified())
        .await
        .expect("the first flush starts");
    let flush_started = Instant::now();

    // The first flush holds its batch, and the pull for the next is already
    // served: two batches are out on the server.
    assert_eq!(
        wait_for_ack_pending(&client, STREAM, DURABLE, 2 * BATCH_SIZE).await,
        2 * BATCH_SIZE,
        "the flush in progress and the batch read ahead are both delivered"
    );
    assert_eq!(
        handler.batches.lock().unwrap().len(),
        1,
        "only the first batch has reached the handler"
    );

    let deadline = flush_started + HOLD + Duration::from_secs(10);
    while handler.seen().len() < MESSAGES as usize && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        flush_started.elapsed() < HOLD + Duration::from_secs(10),
        "the batch read ahead waited on a redelivery: all {MESSAGES} reached the handler only after {:?}",
        flush_started.elapsed()
    );
    // A duplicate would surface within a moment of the last flush.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let batches = handler.batches.lock().unwrap().clone();
    assert!(
        batches.iter().all(|b| b.len() <= BATCH_SIZE),
        "no flush exceeds max_batch_size, got sizes {:?}",
        batches.iter().map(Vec::len).collect::<Vec<_>>()
    );
    let mut seen = handler.seen();
    seen.sort_unstable();
    assert_eq!(
        seen,
        (0..MESSAGES).collect::<Vec<_>>(),
        "every message reaches the handler exactly once"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(
        durable_state(&client, STREAM, DURABLE).await,
        (0, 0, 0),
        "everything is acked and nothing was redelivered"
    );
    broker.close().await;
}

/// Sets its flag when it is dropped, as a flush that is aborted drops the call
/// it holds.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A flush that never returns.
#[derive(Clone)]
struct EndlessFlush {
    started: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}

impl BatchMessageHandler<AbortFlushTopic> for EndlessFlush {
    type Context = ();
    async fn handle_batch(&self, _: Vec<(BatchMessage, MessageMetadata)>, _: &()) -> Outcome {
        let _flag = DropFlag(Arc::clone(&self.dropped));
        self.started.notify_one();
        std::future::pending().await
    }
}

/// The flush runs in a task of its own, and that task goes down with the
/// consumer: aborting the consumer drops the handler call it was awaiting, as
/// it does on every other consume path, so no handler outlives its consumer.
#[tokio::test]
async fn aborting_the_batch_consumer_aborts_its_flush() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<AbortFlushTopic>()
        .await
        .unwrap();
    publish_seq::<AbortFlushTopic>(&broker, 0..1).await;

    let handler = EndlessFlush {
        started: Arc::new(Notify::new()),
        dropped: Arc::new(AtomicBool::new(false)),
    };
    let h = handler.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<AbortFlushTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(1)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_handler_timeout(Duration::from_secs(3600)),
            )
            .await
    });
    tokio::time::timeout(TIMEOUT, handler.started.notified())
        .await
        .expect("the flush starts");
    assert!(!handler.dropped.load(Ordering::SeqCst));

    handle.abort();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !handler.dropped.load(Ordering::SeqCst) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        handler.dropped.load(Ordering::SeqCst),
        "the handler call outlived the consumer that was awaiting it"
    );
    broker.close().await;
}

/// A stop that lands during a flush finishes that flush and hands the batch
/// read ahead back to the server at once, instead of flushing it as well,
/// which would double the time the stop takes. Another consumer takes the
/// handed-back messages straight away.
#[tokio::test]
async fn a_stop_during_a_flush_hands_the_batch_read_ahead_back() {
    const BATCH_SIZE: usize = 100;
    const STREAM: &str = "nats-batch-handback";
    const DURABLE: &str = "nats-batch-handback-consumer";

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<HandBackTopic>().await.unwrap();
    publish_seq::<HandBackTopic>(&broker, 0..200).await;

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let handler = FirstFlushHandler::new(Hold::Gate(Arc::clone(&gate)));
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<HandBackTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(BATCH_SIZE)
                    .with_max_batch_age(Duration::from_secs(10))
                    .with_shutdown(sc),
            )
            .await
    });
    tokio::time::timeout(TIMEOUT, handler.started.notified())
        .await
        .expect("the first flush starts");
    assert_eq!(
        wait_for_ack_pending(&client, STREAM, DURABLE, 2 * BATCH_SIZE).await,
        2 * BATCH_SIZE,
        "the batch read ahead is delivered"
    );

    shutdown.cancel();

    // The flush is still held, and another consumer already takes the batch
    // that was read ahead: it was handed back with a nak, so it waits on no
    // ack_wait.
    let second = RecordingBatchHandler::new();
    let h = second.clone();
    let second_shutdown = CancellationToken::new();
    let sc = second_shutdown.clone();
    let consumer = broker.batch_consumer();
    let second_handle = tokio::spawn(async move {
        consumer
            .run::<HandBackTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(BATCH_SIZE)
                    .with_max_batch_age(Duration::from_secs(10))
                    .with_shutdown(sc),
            )
            .await
    });
    assert!(
        second.wait_for_batches(1, Duration::from_secs(5)).await,
        "the batch read ahead was not handed back while the flush was held"
    );

    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("the stop finishes with the flush in progress")
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");
    second_shutdown.cancel();
    second_handle.await.unwrap().ok();

    assert_eq!(
        handler.batches.lock().unwrap().len(),
        1,
        "the batch read ahead was not flushed by the consumer that stopped"
    );
    assert_eq!(handler.seen(), (0..100).collect::<Vec<_>>());
    let mut taken = second.seen();
    taken.sort_unstable();
    assert_eq!(taken, (100..200).collect::<Vec<_>>());
    broker.close().await;
}

/// Holds each flush for the next of its scripted times, yielding meanwhile,
/// and acks every flush. The seqs of every flush are recorded as it starts.
#[derive(Clone)]
struct PacedFlushHandler {
    batches: Arc<Mutex<Vec<Vec<u32>>>>,
    holds: Arc<Mutex<VecDeque<Duration>>>,
    started: Arc<Notify>,
}

impl PacedFlushHandler {
    fn new(holds: impl IntoIterator<Item = Duration>) -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            holds: Arc::new(Mutex::new(holds.into_iter().collect())),
            started: Arc::new(Notify::new()),
        }
    }

    fn batches(&self) -> Vec<Vec<u32>> {
        self.batches.lock().unwrap().clone()
    }
}

impl BatchMessageHandler<RenewReadAheadTopic> for PacedFlushHandler {
    type Context = ();
    async fn handle_batch(
        &self,
        messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        self.batches
            .lock()
            .unwrap()
            .push(messages.iter().map(|(m, _)| m.seq).collect());
        self.started.notify_one();
        let hold = self.holds.lock().unwrap().pop_front();
        if let Some(hold) = hold {
            tokio::time::sleep(hold).await;
        }
        Outcome::Ack
    }
}

/// The batch read ahead waits out the flush in progress before its own flush
/// starts, and its `ack_wait` is renewed while it waits, so a batch held for
/// longer than the durable's `ack_wait` is not redelivered. Against an
/// `ack_wait` of 9 s, the first flush takes 7 s and the second 4 s, so the
/// batch read ahead is held for 11 s.
#[tokio::test]
async fn a_batch_read_ahead_held_past_its_ack_wait_is_not_redelivered() {
    const STREAM: &str = "nats-batch-renew-read-ahead";
    const DURABLE: &str = "nats-batch-renew-read-ahead-consumer";
    const ACK_WAIT: Duration = Duration::from_secs(9);
    const FIRST_FLUSH: Duration = Duration::from_secs(7);
    const SECOND_FLUSH: Duration = Duration::from_secs(4);

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<RenewReadAheadTopic>()
        .await
        .unwrap();
    client
        .jetstream()
        .get_stream(STREAM)
        .await
        .unwrap()
        .create_consumer(PullConsumerConfig {
            durable_name: Some(DURABLE.to_string()),
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: -1,
            ack_wait: ACK_WAIT,
            ..Default::default()
        })
        .await
        .unwrap();
    publish_seq::<RenewReadAheadTopic>(&broker, 0..10).await;

    let handler = PacedFlushHandler::new([FIRST_FLUSH, SECOND_FLUSH]);
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<RenewReadAheadTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(5)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    tokio::time::timeout(TIMEOUT, handler.started.notified())
        .await
        .expect("the first flush starts");
    let flush_started = Instant::now();
    assert_eq!(
        wait_for_ack_pending(&client, STREAM, DURABLE, 10).await,
        10,
        "the batch read ahead is delivered while the first flush runs"
    );

    // A redelivery would land during the second flush and be flushed after it.
    tokio::time::sleep_until(flush_started + FIRST_FLUSH + SECOND_FLUSH + Duration::from_secs(2))
        .await;
    assert_eq!(
        handler.batches(),
        vec![(0..5).collect::<Vec<_>>(), (5..10).collect()],
        "each batch reaches the handler once"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    assert_eq!(
        durable_state(&client, STREAM, DURABLE).await,
        (0, 0, 0),
        "everything is acked and nothing was redelivered"
    );
    broker.close().await;
}

/// Stops its consumer as the last thing its first flush does, with no yield
/// between the stop and the end of the flush, and acks every flush. The seqs
/// of every flush are recorded as it starts.
#[derive(Clone)]
struct StopOnFirstFlush {
    batches: Arc<Mutex<Vec<Vec<u32>>>>,
    shutdown: CancellationToken,
}

impl BatchMessageHandler<StopRaceTopic> for StopOnFirstFlush {
    type Context = ();
    async fn handle_batch(
        &self,
        messages: Vec<(BatchMessage, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        let first = {
            let mut batches = self.batches.lock().unwrap();
            batches.push(messages.iter().map(|(m, _)| m.seq).collect());
            batches.len() == 1
        };
        if first {
            // Time for the consumer to read the whole batch ahead.
            tokio::time::sleep(Duration::from_secs(1)).await;
            self.shutdown.cancel();
        }
        Outcome::Ack
    }
}

/// A stop the consumer sees in the same wake-up as the end of a flush counts
/// as a stop during that flush, so the batch read ahead goes back to the
/// server with a nak and the stopping consumer leaves it unflushed. On the
/// test's single-threaded runtime the flush task runs to its end in the poll
/// that stops the consumer, so the consumer sees both at once.
#[tokio::test]
async fn a_stop_seen_with_the_end_of_a_flush_hands_the_batch_read_ahead_back() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker.topology().declare::<StopRaceTopic>().await.unwrap();
    publish_seq::<StopRaceTopic>(&broker, 0..10).await;

    let shutdown = CancellationToken::new();
    let handler = StopOnFirstFlush {
        batches: Arc::default(),
        shutdown: shutdown.clone(),
    };
    let h = handler.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<StopRaceTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(5)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(shutdown),
            )
            .await
    });
    tokio::time::timeout(TIMEOUT, handle)
        .await
        .expect("the consumer stops in time")
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");
    assert_eq!(
        handler.batches.lock().unwrap().clone(),
        vec![(0..5).collect::<Vec<_>>()],
        "the consumer that stopped flushed the batch read ahead"
    );

    // Handed back with a nak, so another consumer takes it at once.
    let second = RecordingBatchHandler::new();
    let h = second.clone();
    let second_shutdown = CancellationToken::new();
    let sc = second_shutdown.clone();
    let consumer = broker.batch_consumer();
    let second_handle = tokio::spawn(async move {
        consumer
            .run::<StopRaceTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(5)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });
    assert!(
        second.wait_for_batches(1, Duration::from_secs(5)).await,
        "the batch read ahead was not handed back"
    );
    assert_eq!(sorted(&second.seen()), (5..10).collect::<Vec<_>>());
    second_shutdown.cancel();
    second_handle.await.unwrap().ok();
    broker.close().await;
}

/// Replaces the dead-letter stream `dlq` with one whose publishes are never
/// acknowledged, so each attempt to dead-letter waits out the client's ack
/// timeout, 5 s, and the three attempts of a publish take over 15 s.
async fn silence_dlq(client: &NatsClient, dlq: &str) {
    let js = client.jetstream();
    js.delete_stream(dlq).await.expect("the DLQ stream exists");
    js.create_stream(StreamConfig {
        name: dlq.to_string(),
        subjects: vec![dlq.to_string()],
        no_ack: true,
        ..Default::default()
    })
    .await
    .expect("the silent DLQ stream is created");
}

/// Replaces the dead-letter stream `dlq` with one that acknowledges publishes.
async fn repair_dlq(client: &NatsClient, dlq: &str) {
    let js = client.jetstream();
    js.delete_stream(dlq).await.expect("the DLQ stream exists");
    js.create_stream(StreamConfig {
        name: dlq.to_string(),
        subjects: vec![dlq.to_string()],
        ..Default::default()
    })
    .await
    .expect("the working DLQ stream is created");
}

/// Publishes `payload` as it is, bypassing the codec, with an id of its own.
async fn raw_publish(client: &NatsClient, subject: &str, id: &str, payload: &'static str) {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(NATS_MESSAGE_ID, id);
    client
        .jetstream()
        .publish_with_headers(subject.to_string(), headers, payload.into())
        .await
        .expect("raw publish should succeed")
        .await
        .expect("raw publish should be acked");
}

/// Polls until the stream holds exactly `expected` messages, and returns the
/// last count it saw.
async fn wait_for_exact_stream_count(
    client: &NatsClient,
    stream_name: &str,
    expected: u64,
    timeout: Duration,
) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        let count = stream_count(client, stream_name).await;
        if count == expected || Instant::now() >= deadline {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A poison message whose dead letter cannot be published for a long while
/// does not hold the receive loop: the healthy messages behind it flush at
/// once. The poison message stays un-acked until its dead letter lands, and is
/// acked after it.
#[tokio::test]
async fn poison_settlement_does_not_stall_healthy_messages() {
    const MAIN: &str = "nats-batch-poison-stall";
    const DLQ: &str = "nats-batch-poison-stall-dlq";

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<PoisonStallTopic>()
        .await
        .unwrap();
    silence_dlq(&client, DLQ).await;

    raw_publish(&client, MAIN, "stalled-poison", "not json at all").await;
    publish_seq::<PoisonStallTopic>(&broker, 0..3).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<PoisonStallTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(10)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    // Inside the first 5 s attempt to dead-letter the poison message.
    assert!(
        handler.wait_for_batches(1, Duration::from_secs(4)).await,
        "the healthy messages wait on the poison message's dead letter"
    );
    assert_eq!(sorted(&handler.seen()), vec![0, 1, 2]);
    assert_eq!(
        wait_for_exact_stream_count(&client, MAIN, 1, Duration::from_secs(3)).await,
        1,
        "the healthy messages are acked and the poison message is not"
    );

    repair_dlq(&client, DLQ).await;
    assert_eq!(
        wait_for_exact_stream_count(&client, DLQ, 1, TIMEOUT).await,
        1,
        "the dead letter lands once the DLQ answers"
    );
    assert_eq!(
        wait_for_exact_stream_count(&client, MAIN, 0, TIMEOUT).await,
        0,
        "the poison message is acked after its dead letter"
    );

    shutdown.cancel();
    handle.await.unwrap().ok();
    broker.close().await;
}

/// A stop that lands while poison messages wait for a slot to settle in ends
/// the consumer within the bound it puts on the settlements in progress, and
/// leaves every message un-acked: none is retired without its dead letter,
/// the ones still waiting included.
#[tokio::test]
async fn a_stop_while_poison_waits_for_a_slot_loses_nothing() {
    const MAIN: &str = "nats-batch-poison-stall";
    const DLQ: &str = "nats-batch-poison-stall-dlq";
    // One more than the settlements the consumer runs at once, and then some.
    const POISON: usize = 20;

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<PoisonStallTopic>()
        .await
        .unwrap();
    silence_dlq(&client, DLQ).await;
    for n in 0..POISON {
        raw_publish(
            &client,
            MAIN,
            &format!("waiting-poison-{n}"),
            "not json at all",
        )
        .await;
    }

    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<PoisonStallTopic, _>(
                RecordingBatchHandler::new(),
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(50)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });

    // Every message is read, the first 16 are publishing their dead letters
    // and the rest wait for a slot.
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("the stop waits for the settlements in progress, and for no longer")
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");

    assert_eq!(
        stream_count(&client, MAIN).await,
        POISON as u64,
        "no poison message was acked without a dead letter that was confirmed"
    );
    broker.close().await;
}

/// What a `tracing` subscriber wrote, for asserting on a log line.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn contents(&self) -> String {
        let buffer = self.0.lock().expect("log buffer mutex poisoned");
        String::from_utf8_lossy(&buffer).into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Logs at `WARN` and above on this thread are written into the returned
/// buffer until the guard drops.
fn capture_warnings() -> (CapturedLogs, DefaultGuard) {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    (logs, set_default(subscriber))
}

/// A pull the server ends with a status, while the server still answers, is
/// not a connection error. The durable keeps one waiting pull and another
/// reader holds it, so the consumer's pull is refused with `409 Exceeded
/// MaxWaiting`. The consumer probes its connection, finds the server
/// answering, and goes on without spending the one reconnect attempt it is
/// allowed. It consumes once the holder lets go.
#[tokio::test]
async fn a_batch_pull_the_server_refuses_is_not_a_connection_error() {
    let (logs, _logging) = capture_warnings();

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker
        .topology()
        .declare::<RefusedPullTopic>()
        .await
        .unwrap();
    let durable = client
        .jetstream()
        .get_stream("nats-batch-refused")
        .await
        .unwrap()
        .create_consumer(PullConsumerConfig {
            durable_name: Some("nats-batch-refused-consumer".to_string()),
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: -1,
            max_waiting: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    // Takes the durable's one waiting slot, and is never read.
    let holder = durable
        .batch()
        .max_messages(1)
        .expires(Duration::from_secs(120))
        .messages()
        .await
        .unwrap();

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<RefusedPullTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(5)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_max_reconnect_attempts(1)
                    .with_shutdown(sc),
            )
            .await
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    let refused = logs.contents();
    assert!(
        refused.contains("Exceeded MaxWaiting"),
        "the pull was refused: {refused}"
    );
    assert!(
        !handle.is_finished(),
        "the refused pull ended the consumer: {:?}",
        handle.await
    );

    drop(holder);
    publish_seq::<RefusedPullTopic>(&broker, 0..3).await;
    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "the consumer goes on once the waiting slot is free"
    );
    assert_eq!(sorted(&handler.seen()), vec![0, 1, 2]);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("the consumer stops in time")
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");
    broker.close().await;
}

/// A free host port, for a container whose port must survive a restart.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener
        .local_addr()
        .expect("read the bound address")
        .port()
}

/// An idle consumer's pull is lost when the server is killed and restarts:
/// the restarted server knows nothing of it. Once the client has reconnected,
/// the consumer pulls again at once, so a message published after the restart
/// is handled well inside the 30 s the lost pull would otherwise hold the
/// consumer for.
#[tokio::test]
async fn a_batch_consumer_pulls_again_once_its_connection_comes_back() {
    use testcontainers::core::ContainerPort;

    let port = free_port();
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = NatsContainer::default()
        .with_cmd(&cmd)
        .with_mapped_port(port, ContainerPort::Tcp(4222))
        .start()
        .await
        .expect("failed to start NATS container");
    let host = container.get_host().await.expect("failed to get host");
    let client =
        NatsClient::connect_with_retry(&NatsConfig::new(format!("nats://{host}:{port}")), 10)
            .await
            .expect("failed to connect to NATS");
    let broker = Broker::<Nats>::from_client(client.clone());
    broker.topology().declare::<ReconnectTopic>().await.unwrap();

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<ReconnectTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    // One message fills a batch, so the flush follows the pull.
                    .with_max_batch_size(1)
                    // The lost pull's client-side backstop is 5 s past this.
                    .with_max_batch_age(Duration::from_secs(25))
                    .with_shutdown(sc),
            )
            .await
    });

    let stream = client
        .jetstream()
        .get_stream("nats-batch-reconnect")
        .await
        .expect("the stream exists");
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if let Ok(info) = stream.consumer_info("nats-batch-reconnect-consumer").await
            && info.num_waiting > 0
        {
            break;
        }
        assert!(Instant::now() < deadline, "no pull request was ever opened");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let opened = Instant::now();

    // SIGKILL with no SIGTERM first: a stop lets the server answer its
    // waiting pulls before it goes.
    let killed = tokio::process::Command::new("docker")
        .args(["kill", container.id()])
        .output()
        .await
        .expect("docker kill runs");
    assert!(
        killed.status.success(),
        "docker kill failed: {}",
        String::from_utf8_lossy(&killed.stderr)
    );
    container.start().await.expect("the server starts again");
    let deadline = Instant::now() + TIMEOUT;
    while !client.is_connected() {
        assert!(Instant::now() < deadline, "the client never reconnected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let reconnected = opened.elapsed();

    let publisher = broker.publisher().await.unwrap();
    let deadline = Instant::now() + TIMEOUT;
    while let Err(e) = publisher
        .publish::<ReconnectTopic>(&BatchMessage::new(7))
        .await
    {
        assert!(
            Instant::now() < deadline,
            "JetStream never took the publish: {e}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        handler.wait_for_batches(1, Duration::from_secs(10)).await,
        "the message published {reconnected:?} after the pull opened, once the client was \
         back, waited more than 10 s: the consumer kept its place on the lost pull"
    );
    assert_eq!(handler.seen(), vec![7]);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("the consumer stops in time")
        .expect("the consumer task does not panic")
        .expect("the consumer stops cleanly");
    broker.close().await;
}

/// Each ack sent for a durable: the stream sequence it is for, and its
/// payload (empty or `+ACK` for an ack, `+WPI` for a progress ack, ...).
type AckLog = Arc<Mutex<Vec<(u64, String)>>>;

/// Watches every ack sent for the durable `consumer` on `stream`, by
/// subscribing to the ack subjects the server routes them on,
/// `$JS.ACK.{stream}.{consumer}.{delivered}.{stream_seq}...`.
async fn watch_acks(client: &NatsClient, stream: &str, consumer: &str) -> AckLog {
    use futures_util::StreamExt;

    let raw = client.jetstream().client();
    let mut acks = raw
        .subscribe(format!("$JS.ACK.{stream}.{consumer}.>"))
        .await
        .expect("the ack subjects can be watched");
    raw.flush()
        .await
        .expect("the watch is in place before any ack");
    let log: AckLog = Arc::default();
    let sink = log.clone();
    tokio::spawn(async move {
        while let Some(ack) = acks.next().await {
            let sequence = ack
                .subject
                .split('.')
                .nth(5)
                .and_then(|token| token.parse().ok())
                .expect("an ack subject names the stream sequence");
            let payload = String::from_utf8_lossy(&ack.payload).into_owned();
            sink.lock().unwrap().push((sequence, payload));
        }
    });
    log
}

const RENEW_MAIN: &str = "nats-batch-renew";
const RENEW_DLQ: &str = "nats-batch-renew-dlq";
const RENEW_DURABLE: &str = "nats-batch-renew-consumer";

/// Declares [`RenewTopic`] with a dead-letter stream that never answers and a
/// durable whose `ack_wait` is 3 s, and watches the durable's acks.
async fn renew_setup(tb: &TestBroker) -> AckLog {
    let client = tb.client();
    tb.broker()
        .topology()
        .declare::<RenewTopic>()
        .await
        .unwrap();
    silence_dlq(&client, RENEW_DLQ).await;
    client
        .jetstream()
        .get_stream(RENEW_MAIN)
        .await
        .unwrap()
        .create_consumer(PullConsumerConfig {
            durable_name: Some(RENEW_DURABLE.to_string()),
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: -1,
            ack_wait: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .unwrap();
    watch_acks(&client, RENEW_MAIN, RENEW_DURABLE).await
}

/// How many progress acks the log holds for the message at `sequence`.
fn renewals(acks: &AckLog, sequence: u64) -> usize {
    acks.lock()
        .unwrap()
        .iter()
        .filter(|(seq, payload)| *seq == sequence && payload == "+WPI")
        .count()
}

/// A poison message gets the same renewals while its dead letter publishes.
#[tokio::test]
async fn a_poison_message_renews_its_ack_wait_while_its_dead_letter_publishes() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let acks = renew_setup(&tb).await;
    raw_publish(
        &tb.client(),
        RENEW_MAIN,
        "renewed-poison",
        "not json at all",
    )
    .await;

    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<RenewTopic, _>(
                RecordingBatchHandler::new(),
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(10)
                    .with_max_batch_age(Duration::from_millis(300)),
            )
            .await
    });

    // The 3 s of ack_wait pass once over while the dead letter waits.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let renewed = renewals(&acks, 1);
    assert!(
        renewed >= 2,
        "the poison message was renewed {renewed} times in 4 s of waiting on its dead letter"
    );

    handle.abort();
    broker.close().await;
}

/// A dead-letter publish that takes longer than the durable's `ack_wait` does
/// not get its message redelivered: each message of a rejected batch gets a
/// progress ack every third of the `ack_wait` while its publish runs.
#[tokio::test]
async fn a_rejected_batch_renews_its_ack_wait_while_its_dead_letters_publish() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let acks = renew_setup(&tb).await;
    publish_seq::<RenewTopic>(&broker, 0..2).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Reject]);
    let h = handler.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<RenewTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(10)
                    .with_max_batch_age(Duration::from_millis(300)),
            )
            .await
    });
    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "the batch reaches the handler"
    );

    // The 3 s of ack_wait pass once over while the dead letters wait.
    tokio::time::sleep(Duration::from_secs(4)).await;
    for sequence in 1..=2 {
        let renewed = renewals(&acks, sequence);
        assert!(
            renewed >= 2,
            "message {sequence} was renewed {renewed} times in 4 s of waiting on its dead letter"
        );
    }
    assert_eq!(
        handler.batches().len(),
        1,
        "nothing was redelivered meanwhile"
    );

    handle.abort();
    broker.close().await;
}

/// A durable whose `max_ack_pending` covers a single batch is named for what
/// it costs a consumer that reads ahead, and the warning gives the in-place
/// change that lifts it, which keeps the durable's position in the stream.
#[tokio::test]
async fn a_single_batch_budget_names_the_in_place_fix() {
    let (logs, _logging) = capture_warnings();

    let tb = TestBroker::start().await;
    let broker = tb.broker();
    let client = tb.client();
    broker.topology().declare::<BudgetTopic>().await.unwrap();
    client
        .jetstream()
        .get_stream("nats-batch-budget")
        .await
        .unwrap()
        .create_consumer(PullConsumerConfig {
            durable_name: Some("nats-batch-budget-consumer".to_string()),
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    publish_seq::<BudgetTopic>(&broker, 0..1).await;

    let handler = RecordingBatchHandler::new();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let sc = shutdown.clone();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn(async move {
        consumer
            .run::<BudgetTopic, _>(
                h,
                (),
                BatchConsumerOptions::new()
                    .with_max_batch_size(10)
                    .with_max_batch_age(Duration::from_millis(300))
                    .with_shutdown(sc),
            )
            .await
    });
    assert!(
        handler.wait_for_batches(1, TIMEOUT).await,
        "the batch reaches the handler"
    );
    shutdown.cancel();
    handle.await.unwrap().ok();

    let warned = logs.contents();
    assert!(
        warned.contains("max_ack_pending=10")
            && warned.contains(
                "nats consumer edit nats-batch-budget nats-batch-budget-consumer --max-pending=-1"
            ),
        "the budget in force and its in-place fix are named: {warned}"
    );
    assert!(
        !warned.contains("recreate"),
        "recreating the durable is not advised: {warned}"
    );
    broker.close().await;
}
