//! Integration tests for the Redis Streams batch consumer
//! (`Broker::<Redis>::batch_consumer()`).
//!
//! Drives everything through the generic wrapper — `BatchConsumer<Redis>` /
//! `BatchConsumerOptions<Redis>` — rather than any Redis-only inherent
//! method (there is none: see `src/backend/batch_consumer.rs`'s "Who runs the
//! sequencing guard" doc). Mirrors `tests/inmemory_batch.rs`'s coverage
//! (size/age flush boundaries, `Ack`/`Reject`/`Retry`/`Defer` settlement,
//! pre-handler drops, panics, timeouts, shutdown drain) over Redis Streams'
//! own mechanics: `XREADGROUP COUNT`/`BLOCK` instead of a `VecDeque`, PEL
//! replay instead of a requeue, and immediate per-entry settlement (`XACK` /
//! `XADD` to a DLQ) instead of an in-process buffer.
//!
//! Every test uses a unique stream + consumer group name so the suite can run
//! concurrently against a shared container without entries from one test
//! leaking into another's PEL.

#![cfg(feature = "redis-streams")]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use futures_util::StreamExt;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use shove::codec::RawBytesCodec;
use shove::consumer::ConsumerOptions;
use shove::error::ShoveError;
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::markers::Redis;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::publisher::Publisher;
use shove::redis::{RedisConfig, RedisMode, spawn_reaper};
use shove::topic::{NotSequenced, Topic};
use shove::topology::{QueueTopology, SequenceFailure, TopologyBuilder};
use shove::{Backend, BatchConsumerOptions, Broker, define_topic};

// ---------------------------------------------------------------------------
// Shared Redis container (started once for the entire test binary) — copied
// verbatim (module docs included) from `tests/redis_integration.rs`'s
// scaffolding: this is a separate compiled binary, so it needs its own copy.
// ---------------------------------------------------------------------------

static REDIS_URL: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
static REDIS_CONTAINER: OnceLock<Mutex<Option<testcontainers::ContainerAsync<RedisContainer>>>> =
    OnceLock::new();
static ATEXIT_REGISTERED: std::sync::Once = std::sync::Once::new();

extern "C" fn cleanup_shared_redis_container() {
    let Some(slot) = REDIS_CONTAINER.get() else {
        return;
    };
    let Ok(mut guard) = slot.lock() else {
        return;
    };
    let Some(container) = guard.take() else {
        return;
    };
    let handle = std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(_) => return,
        };
        rt.block_on(async move {
            let _ = container.rm().await;
        });
    });
    let _ = handle.join();
}

async fn redis_url() -> &'static str {
    REDIS_URL
        .get_or_init(|| async {
            let container = RedisContainer::default()
                .with_tag("7.0")
                .start()
                .await
                .expect("failed to start Redis container");
            let host = container.get_host().await.expect("failed to get host");
            let port = container
                .get_host_port_ipv4(REDIS_PORT)
                .await
                .expect("failed to get Redis port");
            let url = format!("redis://{host}:{port}/");
            REDIS_CONTAINER.set(Mutex::new(Some(container))).ok();
            ATEXIT_REGISTERED.call_once(|| {
                // SAFETY: see `tests/redis_integration.rs` — same rationale.
                unsafe { libc::atexit(cleanup_shared_redis_container) };
            });
            url
        })
        .await
}

/// Connect to Redis with a bounded retry loop — testcontainers can return
/// before Redis is bound.
async fn connect_with_retry(url: &str, group: &str, budget: Duration) -> Broker<Redis> {
    let start = std::time::Instant::now();
    let mut last_err: Option<shove::ShoveError> = None;
    while start.elapsed() < budget {
        match Broker::<Redis>::new(
            RedisConfig::new(RedisMode::Standalone {
                url: url.to_owned(),
            })
            .with_group(group),
        )
        .await
        {
            Ok(b) => return b,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    panic!(
        "connect to Redis at {url} after {budget:?}: {}",
        last_err.expect("must have at least one error before timeout")
    );
}

async fn make_broker(group: &str) -> Broker<Redis> {
    let url = redis_url().await;
    connect_with_retry(url, group, Duration::from_secs(30)).await
}

/// Raw multiplexed connection for issuing arbitrary commands
/// (XPENDING / XREADGROUP / XGROUP) the typed API doesn't expose.
async fn raw_conn(url: &str) -> MultiplexedConnection {
    let client = redis::Client::open(url).expect("redis::Client::open");
    for attempt in 0u32..5 {
        match client.get_multiplexed_async_connection().await {
            Ok(c) => return c,
            Err(_) if attempt < 4 => {
                tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt + 1))).await;
            }
            Err(e) => panic!("multiplexed conn after retries: {e}"),
        }
    }
    unreachable!()
}

async fn connect_client_with_retry(url: &str, group: &str) -> shove::redis::RedisClient {
    for attempt in 0u32..5 {
        let cfg = RedisConfig::new(RedisMode::Standalone {
            url: url.to_owned(),
        })
        .with_group(group);
        match <Redis as Backend>::connect(cfg).await {
            Ok(c) => return c,
            Err(_) if attempt < 4 => {
                tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt + 1))).await;
            }
            Err(e) => panic!("connect RedisClient after retries: {e}"),
        }
    }
    unreachable!()
}

async fn xpending_count(conn: &mut MultiplexedConnection, stream: &str, group: &str) -> usize {
    let reply: redis::Value = redis::cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .query_async(conn)
        .await
        .expect("XPENDING");
    match &reply {
        redis::Value::Array(parts) => match parts.first() {
            Some(redis::Value::Int(n)) => *n as usize,
            _ => 0,
        },
        redis::Value::Nil => 0,
        other => panic!("unexpected XPENDING reply: {other:?}"),
    }
}

async fn poll_until<F: Fn() -> bool>(cond: F, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

const TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Recording batch handler — mirrors `tests/inmemory_batch.rs`'s, extended
// with the Redis stream entry id (`meta.delivery_id`) so redelivery tests can
// pin "same entry, re-buffered" against "a fresh entry, republished".
// ---------------------------------------------------------------------------

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

/// `(seq, delivery_id, retry_count)` for one message.
type Recorded = (u32, String, u32);

#[derive(Clone)]
struct RecordingBatchHandler {
    batches: Arc<Mutex<Vec<Vec<Recorded>>>>,
    scripted: Arc<Mutex<std::collections::VecDeque<Outcome>>>,
    signal: Arc<Notify>,
}

impl RecordingBatchHandler {
    fn new() -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
            scripted: Arc::new(Mutex::new(std::collections::VecDeque::new())),
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
                .map(|(m, meta)| (m.seq, meta.delivery_id.clone(), meta.retry_count))
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

    fn batches_full(&self) -> Vec<Vec<Recorded>> {
        self.batches.lock().unwrap().clone()
    }

    fn batches(&self) -> Vec<Vec<u32>> {
        self.batches_full()
            .into_iter()
            .map(|b| b.into_iter().map(|(seq, _, _)| seq).collect())
            .collect()
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

fn sorted(batch: &[u32]) -> Vec<u32> {
    let mut v = batch.to_vec();
    v.sort_unstable();
    v
}

async fn publish_seq<T>(publisher: &Publisher<Redis>, range: std::ops::Range<u32>)
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
// Test topics
// ---------------------------------------------------------------------------

define_topic!(
    SizeTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-size").build()
);
define_topic!(
    CountPinTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-count-pin").build()
);
define_topic!(
    AgeTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-age").build()
);
define_topic!(
    AgeUnderLoadTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-age-under-load").build()
);
define_topic!(
    AckTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-ack").build()
);
define_topic!(
    RetryTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-retry").build()
);
define_topic!(
    DeferTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-defer").build()
);
const REJECT_DLQ_QUEUE: &str = "redis-batch-reject-dlq";
const REJECT_DLQ_DLQ: &str = "redis-batch-reject-dlq-dlq";
define_topic!(
    RejectDlqTopic,
    BatchMessage,
    TopologyBuilder::new(REJECT_DLQ_QUEUE)
        .dlq_named(REJECT_DLQ_DLQ)
        .build()
);
define_topic!(
    RejectDlqDrainTopic,
    Vec<u8>,
    TopologyBuilder::new(REJECT_DLQ_DLQ).build(),
    codec = RawBytesCodec
);
define_topic!(
    RejectNoDlqTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-reject-no-dlq").build()
);
define_topic!(
    PanicTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-panic").build()
);
define_topic!(
    PanicBuildTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-panic-build").build()
);
define_topic!(
    TimeoutDefaultTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-timeout-default").build()
);
define_topic!(
    TimeoutAckTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-timeout-ack").build()
);
const DROP_QUEUE: &str = "redis-batch-drop";
const DROP_DLQ: &str = "redis-batch-drop-dlq";
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
define_topic!(
    ShutdownTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-shutdown").build()
);
define_topic!(
    ShutdownBackoffTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-shutdown-backoff").build()
);
define_topic!(
    ReaperTopic,
    BatchMessage,
    TopologyBuilder::new("redis-batch-reaper").build()
);

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
    CountPinTopic,
    AgeTopic,
    AgeUnderLoadTopic,
    AckTopic,
    RetryTopic,
    DeferTopic,
    RejectDlqTopic,
    RejectNoDlqTopic,
    DropTopic,
    ShutdownTopic,
    ShutdownBackoffTopic,
    ReaperTopic,
);

#[derive(Clone)]
struct RawRecorder {
    received: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl MessageHandler<RejectDlqDrainTopic> for RawRecorder {
    type Context = ();
    async fn handle(&self, msg: Vec<u8>, _meta: MessageMetadata, _: &()) -> Outcome {
        self.received.lock().unwrap().push(msg);
        Outcome::Ack
    }
}

impl MessageHandler<DropDlqRawTopic> for RawRecorder {
    type Context = ();
    async fn handle(&self, msg: Vec<u8>, _meta: MessageMetadata, _: &()) -> Outcome {
        self.received.lock().unwrap().push(msg);
        Outcome::Ack
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

/// Panics while *building* its future — before anything is awaited.
#[derive(Clone)]
struct FutureBuildPanicHandler {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    signal: Arc<Notify>,
}

impl FutureBuildPanicHandler {
    fn new() -> Self {
        Self {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            signal: Arc::new(Notify::new()),
        }
    }

    async fn wait_for_calls(&self, n: usize, timeout: Duration) -> bool {
        wait_for(&self.signal, timeout, || {
            self.calls.load(std::sync::atomic::Ordering::SeqCst) >= n
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
    ) -> impl std::future::Future<Output = Outcome> + Send {
        let nth = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        self.signal.notify_waiters();
        if nth == 1 {
            panic!("handle_batch blew up before returning a future");
        }
        async { Outcome::Ack }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A batch flushes as soon as it reaches `max_batch_size`, with every message
/// delivered exactly once across the resulting batches.
#[tokio::test]
async fn batch_flushes_on_max_batch_size() {
    let broker = make_broker("batch-size-grp").await;
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
}

/// The ticket's trap pin: the batch consumer must issue `XREADGROUP … COUNT
/// {max_batch_size}`, never the non-concurrent single-message path's `COUNT
/// 1` clamp. Captured via a raw `MONITOR` connection established before the
/// consumer starts.
#[tokio::test]
async fn xreadgroup_count_matches_max_batch_size_not_one() {
    let url = redis_url().await;
    let broker = connect_with_retry(url, "batch-count-pin-grp", Duration::from_secs(30)).await;
    broker.topology().declare::<CountPinTopic>().await.unwrap();

    let monitor_client = redis::Client::open(url).expect("open monitor client");
    let monitor = monitor_client
        .get_async_monitor()
        .await
        .expect("MONITOR connection");
    let mut lines = monitor.into_on_message::<String>();

    let publisher = broker.publisher().await.unwrap();
    publish_seq::<CountPinTopic>(&publisher, 0..2).await;

    let handler = RecordingBatchHandler::new();
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<CountPinTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_max_batch_size(5)
                        .with_max_batch_age(Duration::from_millis(500))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    // Find the FIRST XREADGROUP MONITOR line for this test's stream: the
    // buffer starts empty, so headroom == max_batch_size on that call. Later
    // reads legitimately shrink COUNT as the buffer fills — this pin is
    // scoped to the first call only.
    let expected = "\"COUNT\" \"5\"";
    let stream_needle = "\"redis-batch-count-pin\"";
    let found = tokio::time::timeout(TIMEOUT, async {
        loop {
            let Some(line) = lines.next().await else {
                return false;
            };
            if line.contains("XREADGROUP") && line.contains(stream_needle) {
                return line.contains(expected);
            }
        }
    })
    .await
    .unwrap_or(false);

    shutdown.cancel();
    handle.await.unwrap().ok();

    assert!(
        found,
        "expected the first XREADGROUP for {stream_needle} to carry {expected}"
    );
}

/// A batch below `max_batch_size` still flushes once `max_batch_age` elapses.
#[tokio::test]
async fn batch_flushes_on_max_batch_age() {
    let broker = make_broker("batch-age-grp").await;
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
                        .with_max_batch_age(Duration::from_millis(300))
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
}

/// The age trigger must not starve under a steady sub-cap trickle.
#[tokio::test]
async fn age_trigger_flushes_under_sustained_sub_cap_load() {
    let broker = make_broker("batch-age-load-grp").await;
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
                    BatchConsumerOptions::new()
                        .with_max_batch_size(1000)
                        .with_max_batch_age(Duration::from_millis(300))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    for seq in 0..15u32 {
        publisher
            .publish::<AgeUnderLoadTopic>(&BatchMessage::new(seq))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
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
}

/// `Ack` retires the batch — XPENDING drops to zero, no redelivery.
#[tokio::test]
async fn ack_retires_the_batch() {
    let url = redis_url().await;
    let group = "batch-ack-grp";
    let broker = connect_with_retry(url, group, Duration::from_secs(30)).await;
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
                        .with_max_batch_age(Duration::from_millis(300))
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

    let mut raw = raw_conn(url).await;
    let pending = xpending_count(&mut raw, "redis-batch-ack", group).await;
    assert_eq!(pending, 0, "the acked batch must leave nothing pending");
}

/// `Retry` redelivers the whole batch — the SAME stream entry ids come back
/// (a re-buffer, not a republish), with `x-retry-count` untouched — then
/// `Ack` stops it.
#[tokio::test]
async fn retry_redelivers_the_whole_batch_then_ack_stops_it() {
    let broker = make_broker("batch-retry-grp").await;
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

    let batches = handler.batches_full();
    assert_eq!(
        batches.len(),
        2,
        "ack on the redelivery must stop it, got {batches:?}"
    );

    let mut first: Vec<(u32, String, u32)> = batches[0].clone();
    let mut second: Vec<(u32, String, u32)> = batches[1].clone();
    first.sort_by_key(|(seq, ..)| *seq);
    second.sort_by_key(|(seq, ..)| *seq);
    assert_eq!(
        first, second,
        "redelivery must replay the identical entries (same ids, same retry count)"
    );
    assert_eq!(
        first.iter().map(|(seq, ..)| *seq).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(
        first.iter().all(|(_, _, retry_count)| *retry_count == 0),
        "a re-buffer must not increment x-retry-count, got {first:?}"
    );
}

/// `Defer` is indistinguishable from `Retry` on the batch path.
#[tokio::test]
async fn defer_redelivers_like_retry() {
    let broker = make_broker("batch-defer-grp").await;
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
}

/// `Reject` with a DLQ declared: every message in the batch is dead-lettered
/// and the loop keeps consuming.
#[tokio::test]
async fn rejected_batch_with_dlq_lands_every_message_and_continues() {
    let broker = make_broker("batch-reject-dlq-grp").await;
    broker.topology().declare::<RejectDlqTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<RejectDlqTopic>(&publisher, 0..3).await;
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
                        .with_max_batch_age(Duration::from_millis(300))
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

    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let dlq_group = make_broker("batch-reject-dlq-drain-grp").await;
    dlq_group
        .topology()
        .declare::<RejectDlqDrainTopic>()
        .await
        .unwrap();
    let mut dlq = dlq_group.consumer_supervisor();
    dlq.register::<RejectDlqDrainTopic, _>(
        RawRecorder {
            received: received.clone(),
        },
        ConsumerOptions::new(),
    )
    .expect("register dlq drain");
    let dlq_handle =
        tokio::spawn(dlq.run_until_timeout(std::future::pending::<()>(), Duration::from_secs(5)));
    assert!(
        poll_until(|| received.lock().unwrap().len() >= 3, TIMEOUT).await,
        "every rejected message must land in the DLQ"
    );
    dlq_handle.abort();

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
        ]
    );
}

/// `Reject` with no DLQ declared: the batch is discarded and the loop keeps
/// consuming.
#[tokio::test]
async fn rejected_batch_without_dlq_is_discarded_and_continues() {
    let broker = make_broker("batch-reject-no-dlq-grp").await;
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
                        .with_max_batch_age(Duration::from_millis(300))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    assert!(handler.wait_for_batches(2, TIMEOUT).await);
    shutdown.cancel();
    handle.await.unwrap().ok();

    assert_eq!(handler.batches()[1], vec![100]);
}

/// A panic inside the handler is caught and the batch is redelivered whole.
#[tokio::test]
async fn panic_in_handler_redelivers_the_batch() {
    let broker = make_broker("batch-panic-grp").await;
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
}

/// A panic while *building* the handler future is contained exactly like one
/// raised from inside the future.
#[tokio::test]
async fn panic_while_building_the_future_redelivers_the_batch() {
    let broker = make_broker("batch-panic-build-grp").await;
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
}

/// A flush that outlasts `handler_timeout` is abandoned and redelivered.
#[tokio::test]
async fn handler_timeout_defaults_to_retry_redelivery() {
    let broker = make_broker("batch-timeout-default-grp").await;
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
}

/// `with_handler_timeout_outcome(Ack)` makes a timed-out batch gone instead
/// of redelivered.
#[tokio::test]
async fn handler_timeout_outcome_ack_makes_the_batch_gone() {
    let broker = make_broker("batch-timeout-ack-grp").await;
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
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.cancel();
    handle.await.unwrap().ok();

    assert_eq!(
        handler.calls().len(),
        1,
        "Ack on timeout must not redeliver, got {:?}",
        handler.calls()
    );
}

/// An oversized and an undecodable message never reach the handler and are
/// dead-lettered exactly once each — adapted from the InMemory sibling:
/// there, a pre-handler drop is parked until the surviving batch's flush; on
/// Redis it is dead-lettered immediately (settled at drop time, per the
/// module doc's "why no lease during accumulation" note), so it lands in the
/// DLQ strictly *before* the surviving batch commits rather than alongside it.
#[tokio::test]
async fn oversize_and_undecodable_never_reach_the_handler_and_dlq_once_each() {
    let broker = make_broker("batch-drop-grp").await;
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

    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let dlq_group = make_broker("batch-drop-dlq-drain-grp").await;
    dlq_group
        .topology()
        .declare::<DropDlqRawTopic>()
        .await
        .unwrap();
    let mut dlq = dlq_group.consumer_supervisor();
    dlq.register::<DropDlqRawTopic, _>(
        RawRecorder {
            received: received.clone(),
        },
        ConsumerOptions::new(),
    )
    .expect("register dlq drain");
    let dlq_handle =
        tokio::spawn(dlq.run_until_timeout(std::future::pending::<()>(), Duration::from_secs(5)));
    assert!(poll_until(|| received.lock().unwrap().len() >= 2, TIMEOUT).await);
    dlq_handle.abort();

    let dead = received.lock().unwrap().clone();
    assert_eq!(
        dead.len(),
        2,
        "exactly one DLQ copy each for the oversized and undecodable message, got {dead:?}"
    );
    assert!(dead.contains(&serde_json::to_vec(&oversized).unwrap()));
    assert!(dead.contains(&b"{not valid json".to_vec()));
}

/// The `NotSequenced` bound is compile-time only; a hand-implemented topic
/// can claim it while its topology still declares sequencing. The runtime
/// guard must reject that.
#[tokio::test]
async fn runtime_guard_rejects_a_topic_that_declares_sequencing() {
    struct LiesAboutSequencing;
    impl Topic for LiesAboutSequencing {
        type Message = BatchMessage;
        type Codec = shove::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: OnceLock<QueueTopology> = OnceLock::new();
            TOPOLOGY.get_or_init(|| {
                TopologyBuilder::new("redis-batch-guard-test")
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

    let broker = make_broker("batch-guard-grp").await;
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
            assert!(msg.contains("redis-batch-guard-test"));
            assert!(msg.contains("run_fifo"));
        }
        other => panic!("expected ShoveError::Topology, got {other:?}"),
    }
}

/// Cancelling the shutdown token flushes a partially-filled batch instead of
/// discarding it.
#[tokio::test]
async fn shutdown_flushes_the_pending_partial_batch() {
    let broker = make_broker("batch-shutdown-grp").await;
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
                    BatchConsumerOptions::new()
                        .with_max_batch_size(1000)
                        .with_max_batch_age(Duration::from_secs(3600))
                        .with_shutdown(shutdown),
                )
                .await
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
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
}

/// Shutdown firing during the redelivery backoff sleep (after a `Retry`)
/// must return promptly, leaving the batch pending in the group's PEL for
/// the reaper — not blocking on the escalated backoff delay.
#[tokio::test]
async fn shutdown_mid_backoff_returns_promptly_and_leaves_batch_pending() {
    let url = redis_url().await;
    let group = "batch-shutdown-backoff-grp";
    let broker = connect_with_retry(url, group, Duration::from_secs(30)).await;
    broker
        .topology()
        .declare::<ShutdownBackoffTopic>()
        .await
        .unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<ShutdownBackoffTopic>(&publisher, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<ShutdownBackoffTopic, _>(
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

    // Wait for the first (Retry'd) flush, which puts the consumer into its
    // ~1s redelivery backoff sleep.
    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    let started = std::time::Instant::now();
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .expect("consumer must return promptly, not wait out the full backoff ceiling")
        .unwrap();
    assert!(
        result.is_ok(),
        "shutdown mid-backoff must not surface as an error: {result:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "shutdown must cut the backoff sleep short, took {:?}",
        started.elapsed()
    );

    // The batch must never have been acked — it stays pending for the
    // reaper's crash-recovery safety net.
    let mut raw = raw_conn(url).await;
    let pending = xpending_count(&mut raw, "redis-batch-shutdown-backoff", group).await;
    assert_eq!(
        pending, 3,
        "the un-acked batch must remain pending after a shutdown mid-backoff"
    );
}

/// `Retry` means "leave pending for reclaim": a batch left pending by a
/// consumer that dies mid-backoff (never re-reading its own PEL) is
/// recovered by the reaper sidecar, exactly like the single-message path.
#[tokio::test]
async fn reaper_reclaims_a_batch_left_pending_by_a_dead_consumer() {
    let url = redis_url().await;
    let stream = "redis-batch-reaper";
    let group = "batch-reaper-grp";
    let broker = connect_with_retry(url, group, Duration::from_secs(30)).await;
    broker.topology().declare::<ReaperTopic>().await.unwrap();
    let publisher = broker.publisher().await.unwrap();
    publish_seq::<ReaperTopic>(&publisher, 0..3).await;

    let handler = RecordingBatchHandler::new().scripting([Outcome::Retry]);
    let shutdown = CancellationToken::new();
    let consumer = broker.batch_consumer();
    let handle = tokio::spawn({
        let handler = handler.clone();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run::<ReaperTopic, _>(
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

    // Let the Retry flush happen, then kill the consumer task outright
    // (not a graceful shutdown) — it never gets to re-read its own PEL, so
    // the entries stay pending under a now-dead consumer name.
    assert!(handler.wait_for_batches(1, TIMEOUT).await);
    handle.abort();
    let _ = handle.await;

    let mut raw = raw_conn(url).await;
    let mut still_pending = 0usize;
    let deadline_pre = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline_pre {
        still_pending = xpending_count(&mut raw, stream, group).await;
        if still_pending == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        still_pending, 3,
        "the dead consumer's batch must still be pending before the reaper runs"
    );

    let client = connect_client_with_retry(url, group).await;
    let reaper_shutdown = CancellationToken::new();
    let reaper_handle = spawn_reaper(
        client,
        vec![stream.to_string()],
        group.to_string(),
        Duration::from_millis(100),
        0,
        reaper_shutdown.clone(),
    );

    // Prove redelivery: a fresh consumer name reading `>` must see the
    // reclaimed entries (the reaper re-XADDs then XACKs each claimed entry).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut redelivered = 0usize;
    while std::time::Instant::now() < deadline {
        let raw_reply: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg("reaper-verify-consumer")
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut raw)
            .await
            .unwrap_or(redis::Value::Nil);
        if let redis::Value::Array(ref outer) = raw_reply
            && let Some(redis::Value::Array(stream_pair)) = outer.first()
            && let Some(redis::Value::Array(entries)) = stream_pair.get(1)
        {
            redelivered = entries.len();
        }
        if redelivered > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    reaper_shutdown.cancel();
    let _ = reaper_handle.await;

    assert!(
        redelivered > 0,
        "the reaper did not redeliver the dead consumer's pending batch"
    );
}
