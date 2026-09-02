//! Integration tests for the NATS JetStream batch consumer
//! (`Broker::<Nats>::batch_consumer()`).
//!
//! These cover what only a real server exercises: the size- and age-triggered
//! flush boundaries of the pull-batch request, per-message ack retirement on
//! `Ack`, whole-batch Nak redelivery on `Retry`/`Defer`, the per-message
//! DLQ-publish-then-ack path on `Reject`, the drop-from-batch arms for
//! oversized/undeserializable payloads, the runtime sequencing guard, the
//! `max_ack_pending` clamp, and the flush-on-shutdown drain.

#![cfg(feature = "nats")]

use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull::Config as PullConsumerConfig;
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
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

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
    // way any real publisher does: `publish_to_dlq` derives the DLQ copy's id
    // from the original's, and an id-less original yields the constant id
    // `-dlq`, which JetStream's dedup window collapses across runs.
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
