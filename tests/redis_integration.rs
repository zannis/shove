//! Integration tests for the Redis Streams backend.
//!
//! These tests spin up a Redis container automatically via testcontainers.
//! Docker (or compatible runtime) must be available.
//!
//! Run with:
//!   cargo test -q --test redis_integration --features redis-streams

#![cfg(feature = "redis-streams")]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};

use shove::consumer_group::ConsumerGroupConfig;
use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode, RedisQueueStatsProvider};
use shove::{
    Broker, ConsumerOptions, JsonCodec, MessageHandler, MessageMetadata, Outcome, Redis,
    SequenceFailure, SequencedTopic, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Shared Redis container (started once for the entire test binary)
// ---------------------------------------------------------------------------

static REDIS_URL: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
// Keep the container alive for the duration of the test binary. The
// `Mutex<Option<…>>` lets the atexit cleanup hook below `.take()` ownership
// and run the synchronous `rm()` logic, since Rust never drops `static`
// values at process exit and `ContainerAsync::rm()` takes `self` by value.
static REDIS_CONTAINER: OnceLock<Mutex<Option<testcontainers::ContainerAsync<RedisContainer>>>> =
    OnceLock::new();

// Register a `libc::atexit` hook on first access so the shared container is
// removed when the test binary exits normally. Using `libc::atexit` (already
// a transitive dep) avoids pulling in a new `ctor` dev-dep.
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
    // Mirror `ContainerOnDrop::drop`: run async `rm()` synchronously on a
    // dedicated single-threaded runtime in a dedicated OS thread so we don't
    // re-enter any runtime that might still be tearing down.
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

/// RAII wrapper for per-test Redis containers. `ContainerAsync::rm()` is
/// async, and the bare testcontainers Drop spawns a background tokio task
/// that may be aborted when the test runtime tears down — leaking the
/// container if the test panics or the binary is killed soon after. This
/// wrapper runs `rm()` synchronously on a dedicated runtime in a dedicated
/// thread, ensuring cleanup completes before scope exit (including unwind
/// from a failed assertion).
struct ContainerOnDrop(Option<testcontainers::ContainerAsync<RedisContainer>>);

impl ContainerOnDrop {
    fn new(container: testcontainers::ContainerAsync<RedisContainer>) -> Self {
        Self(Some(container))
    }
}

impl std::ops::Deref for ContainerOnDrop {
    type Target = testcontainers::ContainerAsync<RedisContainer>;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("container removed before drop")
    }
}

impl Drop for ContainerOnDrop {
    fn drop(&mut self) {
        if let Some(container) = self.0.take() {
            // Spawn a fresh single-threaded runtime on a dedicated OS thread
            // so we can `block_on` the async `rm()` without re-entering the
            // test's own runtime (which would deadlock).
            let handle = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("cleanup runtime");
                rt.block_on(async move {
                    let _ = container.rm().await;
                });
            });
            let _ = handle.join();
        }
    }
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
                // SAFETY: `atexit` requires the registered function be safe to
                // call at process exit; our cleanup is independent of static
                // destructors and uses its own runtime/thread. Note that
                // `std::thread::spawn` inside an `atexit` handler is not
                // strictly guaranteed by POSIX, but is reliable on the
                // platforms this crate targets (macOS, Linux with glibc/musl).
                // Worst-case failure mode is a leaked container, not UB.
                unsafe { libc::atexit(cleanup_shared_redis_container) };
            });
            url
        })
        .await
}

async fn make_broker(group: &str) -> Broker<Redis> {
    let url = redis_url().await;
    // Retry a few times — testcontainers occasionally returns before Redis
    // is fully accepting connections, especially when multiple containers
    // start in parallel under nextest.
    for attempt in 0u32..5 {
        match Broker::<Redis>::new(RedisConfig {
            mode: RedisMode::Standalone {
                url: url.to_owned(),
            },
            group: Some(group.into()),
            ..Default::default()
        })
        .await
        {
            Ok(b) => return b,
            Err(_) if attempt < 4 => {
                tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt + 1))).await;
            }
            Err(e) => panic!("connect to Redis after retries: {e}"),
        }
    }
    unreachable!()
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

// ---------------------------------------------------------------------------
// Test topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: u64,
}

struct OrdersTopic;
impl Topic for OrdersTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-orders")
                .hold_queue(Duration::from_millis(100))
                .dlq()
                .build()
        })
    }
}

struct RetryTopic;
impl Topic for RetryTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-retry")
                .hold_queue(Duration::from_millis(100))
                .dlq()
                .build()
        })
    }
}

struct RejectTopic;
impl Topic for RejectTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-reject")
                .hold_queue(Duration::from_millis(100))
                .dlq()
                .build()
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    account: String,
    seq: u64,
}

struct LedgerTopic;
impl Topic for LedgerTopic {
    type Message = Event;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-ledger")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(4)
                .allow_message_loss()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(LedgerTopic::sequence_key);
}
impl SequencedTopic for LedgerTopic {
    fn sequence_key(msg: &Event) -> String {
        msg.account.clone()
    }
}

// ---------------------------------------------------------------------------
// Test 1: basic_pubsub_ack
// ---------------------------------------------------------------------------

#[tokio::test]
async fn basic_pubsub_ack() {
    let broker = make_broker("redis-int-basic-ack").await;
    broker
        .topology()
        .declare::<OrdersTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<OrdersTopic>(&Order { id: 1 })
        .await
        .expect("publish");

    let counter = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<OrdersTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<OrdersTopic, _>(H(counter.clone()), ConsumerOptions::<Redis>::new())
        .expect("register");

    let probe = counter.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 1,
            Duration::from_secs(15),
        )
        .await;
    };

    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        1,
        "handler must be called exactly once"
    );
}

// ---------------------------------------------------------------------------
// Test 2: retry_then_ack_on_redeliver
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retry_then_ack_on_redeliver() {
    let broker = make_broker("redis-int-retry").await;
    broker
        .topology()
        .declare::<RetryTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<RetryTopic>(&Order { id: 42 })
        .await
        .expect("publish");

    let call_count = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<RetryTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, meta: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            if meta.retry_count == 0 {
                Outcome::Retry
            } else {
                Outcome::Ack
            }
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<RetryTopic, _>(
            H(call_count.clone()),
            ConsumerOptions::<Redis>::new().with_max_retries(5),
        )
        .expect("register");

    let probe = call_count.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 2,
            Duration::from_secs(10),
        )
        .await;
    };

    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(
        call_count.load(Ordering::Relaxed),
        2,
        "handler must be called exactly twice (retry + ack)"
    );
}

// ---------------------------------------------------------------------------
// Test 3: reject_no_panic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reject_no_panic() {
    let broker = make_broker("redis-int-reject").await;
    broker
        .topology()
        .declare::<RejectTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<RejectTopic>(&Order { id: 99 })
        .await
        .expect("publish");

    let call_count = Arc::new(AtomicU32::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicU32>);
    impl MessageHandler<RejectTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Reject
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<RejectTopic, _>(H(call_count.clone()), ConsumerOptions::<Redis>::new())
        .expect("register");

    let probe = call_count.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 1,
            Duration::from_secs(15),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(
        call_count.load(Ordering::Relaxed),
        1,
        "Reject must not cause redelivery"
    );
}

// ---------------------------------------------------------------------------
// DLQ delivery test: rejected messages must arrive in the DLQ stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reject_sends_to_dlq() {
    let broker = make_broker("redis-int-dlq-delivery-grp").await;
    broker
        .topology()
        .declare::<DlqDeliveryTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<DlqDeliveryTopic>(&Order { id: 77 })
        .await
        .expect("publish");

    let handled = Arc::new(AtomicU32::new(0));

    #[derive(Clone)]
    struct RejectH(Arc<AtomicU32>);
    impl MessageHandler<DlqDeliveryTopic> for RejectH {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Reject
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<DlqDeliveryTopic, _>(RejectH(handled.clone()), ConsumerOptions::<Redis>::new())
        .expect("register");

    let probe = handled.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 1,
            Duration::from_secs(15),
        )
        .await;
        // Give the DLQ XADD time to complete before we read.
        tokio::time::sleep(Duration::from_millis(300)).await;
    };

    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");

    // Read from the DLQ stream directly and verify the message arrived.
    let url = redis_url().await;
    let client = redis::Client::open(url).expect("redis client for DLQ check");
    let mut raw_conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("raw redis conn");

    // DLQ stream name — topology appends "-dlq" to the queue name.
    let base_stream = DlqDeliveryTopic::topology().queue();
    let dlq_stream = format!("{base_stream}-dlq");

    // Use raw XREAD to avoid AsyncCommands API uncertainty.
    let raw: redis::Value = redis::cmd("XREAD")
        .arg("COUNT")
        .arg(10i64)
        .arg("STREAMS")
        .arg(&dlq_stream)
        .arg("0-0")
        .query_async(&mut raw_conn)
        .await
        .unwrap_or(redis::Value::Nil);

    // Parse: [[stream_name, [[entry_id, [field, value, ...]], ...]]]
    let has_entries = match &raw {
        redis::Value::Array(outer) if !outer.is_empty() => match &outer[0] {
            redis::Value::Array(stream_pair) if stream_pair.len() >= 2 => match &stream_pair[1] {
                redis::Value::Array(entries) => !entries.is_empty(),
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };

    assert!(
        has_entries,
        "DLQ stream '{dlq_stream}' must contain at least one entry after Reject; got: {raw:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: fifo_same_key_in_order
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fifo_same_key_in_order() {
    let broker = make_broker("redis-int-fifo").await;
    broker
        .topology()
        .declare::<LedgerTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    for seq in 0..10u64 {
        publisher
            .publish::<LedgerTopic>(&Event {
                account: "acct-1".into(),
                seq,
            })
            .await
            .expect("publish");
    }

    let received = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));

    #[derive(Clone)]
    struct H(Arc<tokio::sync::Mutex<Vec<u64>>>);
    impl MessageHandler<LedgerTopic> for H {
        type Context = ();
        async fn handle(&self, msg: Event, _: MessageMetadata, _: &()) -> Outcome {
            self.0.lock().await.push(msg.seq);
            Outcome::Ack
        }
    }

    let received_c = received.clone();
    let mut group = broker.consumer_group();
    group
        .register_fifo::<LedgerTopic, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::default()),
            move || H(Arc::clone(&received_c)),
        )
        .await
        .expect("register_fifo");

    let probe = received.clone();
    let signal = async move {
        poll_until(
            move || probe.try_lock().map(|v| v.len() >= 10).unwrap_or(false),
            Duration::from_secs(15),
        )
        .await;
    };

    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");

    let seqs = received.lock().await;
    assert_eq!(seqs.len(), 10, "expected 10 messages, got {}", seqs.len());
    let expected: Vec<u64> = (0..10).collect();
    assert_eq!(*seqs, expected, "messages must arrive in sequence order");
}

// ---------------------------------------------------------------------------
// Additional test topics (unique names to avoid cross-test pollution)
// ---------------------------------------------------------------------------

struct DlqDeliveryTopic;
impl Topic for DlqDeliveryTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-dlq-delivery").dlq().build())
    }
}

struct HeadersTopic;
impl Topic for HeadersTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-headers").dlq().build())
    }
}

struct BatchTopic;
impl Topic for BatchTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-batch").build())
    }
}

struct DeferTopic;
impl Topic for DeferTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-defer")
                .hold_queue(Duration::from_millis(100))
                .build()
        })
    }
}

struct StatsTopic;
impl Topic for StatsTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-stats").build())
    }
}

// ---------------------------------------------------------------------------
// Test 5: publish_with_headers_visible_in_metadata
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_with_headers_visible_in_metadata() {
    let broker = make_broker("redis-int-headers-grp").await;
    broker
        .topology()
        .declare::<HeadersTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    let mut headers = std::collections::HashMap::new();
    headers.insert("x-trace-id".to_string(), "trace-abc".to_string());
    headers.insert("x-tenant".to_string(), "acme".to_string());
    publisher
        .publish_with_headers::<HeadersTopic>(&Order { id: 10 }, headers)
        .await
        .expect("publish_with_headers");

    let received_headers: Arc<
        tokio::sync::Mutex<Option<std::collections::HashMap<String, String>>>,
    > = Arc::new(tokio::sync::Mutex::new(None));

    let rh = received_headers.clone();
    let call_count = Arc::new(AtomicUsize::new(0));
    let cc = call_count.clone();

    #[derive(Clone)]
    struct H(
        Arc<tokio::sync::Mutex<Option<std::collections::HashMap<String, String>>>>,
        Arc<AtomicUsize>,
    );
    impl MessageHandler<HeadersTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, meta: MessageMetadata, _: &()) -> Outcome {
            *self.0.lock().await = Some(meta.headers.clone());
            self.1.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<HeadersTopic, _>(H(rh, cc), ConsumerOptions::<Redis>::new())
        .expect("register");

    let probe = call_count.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 1,
            Duration::from_secs(15),
        )
        .await;
    };

    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(call_count.load(Ordering::Relaxed), 1);

    let hdrs = received_headers.lock().await;
    let hdrs = hdrs.as_ref().expect("headers must be set");
    assert_eq!(
        hdrs.get("x-trace-id").map(String::as_str),
        Some("trace-abc"),
        "x-trace-id header must be preserved"
    );
    assert_eq!(
        hdrs.get("x-tenant").map(String::as_str),
        Some("acme"),
        "x-tenant header must be preserved"
    );
}

// ---------------------------------------------------------------------------
// Test 6: publish_batch_returns_correct_count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_batch_returns_correct_count() {
    let broker = make_broker("redis-int-batch-grp").await;
    broker
        .topology()
        .declare::<BatchTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");

    let msgs: Vec<Order> = (0..5).map(|i| Order { id: i }).collect();
    // Publisher<B>::publish_batch returns Result<()>; verify it succeeds for a full batch.
    let res = publisher.publish_batch::<BatchTopic>(&msgs).await;
    assert!(res.is_ok(), "publish_batch must succeed: {res:?}");

    // Verify all 5 messages arrived using the stats provider.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let stats = broker
        .queue_stats_provider()
        .get_queue_stats(BatchTopic::topology().queue())
        .await
        .expect("get_queue_stats");
    assert_eq!(
        stats.messages_ready + stats.messages_in_flight,
        5,
        "expected 5 total messages in stream after publish_batch"
    );
}

// ---------------------------------------------------------------------------
// Test 7: defer_does_not_increment_retry_count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn defer_does_not_increment_retry_count() {
    let broker = make_broker("redis-int-defer-grp").await;
    broker
        .topology()
        .declare::<DeferTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<DeferTopic>(&Order { id: 77 })
        .await
        .expect("publish");

    let call_count = Arc::new(AtomicUsize::new(0));
    // Store retry_count on second delivery to verify it stayed 0.
    let retry_on_second: Arc<AtomicU32> = Arc::new(AtomicU32::new(u32::MAX));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>, Arc<AtomicU32>);
    impl MessageHandler<DeferTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, meta: MessageMetadata, _: &()) -> Outcome {
            let n = self.0.fetch_add(1, Ordering::Relaxed);
            if n == 0 {
                Outcome::Defer
            } else {
                self.1.store(meta.retry_count, Ordering::Relaxed);
                Outcome::Ack
            }
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<DeferTopic, _>(
            H(call_count.clone(), retry_on_second.clone()),
            ConsumerOptions::<Redis>::new(),
        )
        .expect("register");

    let probe = call_count.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 2,
            Duration::from_secs(15),
        )
        .await;
    };

    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(
        call_count.load(Ordering::Relaxed),
        2,
        "handler must be called exactly twice"
    );
    assert_eq!(
        retry_on_second.load(Ordering::Relaxed),
        0,
        "Defer must not increment retry_count"
    );
}

// ---------------------------------------------------------------------------
// Test 8: autoscaler_stats_reflect_published_messages
// ---------------------------------------------------------------------------

#[tokio::test]
async fn autoscaler_stats_reflect_published_messages() {
    let broker = make_broker("redis-int-stats-grp").await;
    broker
        .topology()
        .declare::<StatsTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    for i in 0..5u64 {
        publisher
            .publish::<StatsTopic>(&Order { id: i })
            .await
            .expect("publish");
    }

    // Give Redis a moment to persist all messages before reading stats.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Use the new queue_stats_provider() method on Broker to access stats
    // without needing internal visibility into the Redis client.
    let stats_provider = broker.queue_stats_provider();
    let stats = stats_provider
        .get_queue_stats(StatsTopic::topology().queue())
        .await
        .expect("get_queue_stats");

    assert!(
        stats.messages_ready >= 5,
        "expected at least 5 ready messages, got {}",
        stats.messages_ready
    );
}

// ---------------------------------------------------------------------------
// Helper for finding a free port
// ---------------------------------------------------------------------------

fn find_free_port() -> u16 {
    // Bind to port 0 to let the OS allocate a free ephemeral port, then
    // immediately drop the listener so the port is available for Docker.
    // There is a small TOCTOU window, but it is negligible for test use.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind for free port");
    listener.local_addr().expect("local addr").port()
}

// ---------------------------------------------------------------------------
// Reconnect test topic
// ---------------------------------------------------------------------------

struct ReconnectTopic;
impl Topic for ReconnectTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-reconnect")
                .hold_queue(Duration::from_millis(100))
                .dlq()
                .build()
        })
    }
}

struct RequeuerRecoverTopic;
impl Topic for RequeuerRecoverTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-requeuer-recover")
                .hold_queue(Duration::from_millis(100))
                .dlq()
                .build()
        })
    }
}

// ---------------------------------------------------------------------------
// Reconnect test: consumer recovers after Redis container restart
// ---------------------------------------------------------------------------

/// Verify that the consumer survives a full Redis restart:
/// 1. Consume one message to prove it is working.
/// 2. Stop the Redis container (simulating a network partition or crash).
/// 3. Start the Redis container again.
/// 4. Redeclare topology and publish a second message.
/// 5. The running consumer reconnects and processes the new message.
#[tokio::test]
async fn consumer_recovers_after_redis_restart() {
    // Each reconnect test uses its own container so we can stop/start it.
    // The shared `redis_url()` container is not used here.
    // Use a dynamically allocated host port to avoid CI collisions.
    let host_port: u16 = find_free_port();
    let container = ContainerOnDrop::new(
        RedisContainer::default()
            .with_tag("7.0")
            .with_mapped_port(host_port, ContainerPort::Tcp(REDIS_PORT))
            .start()
            .await
            .expect("start Redis container"),
    );
    let host = container.get_host().await.expect("get host");
    let url = format!("redis://{host}:{host_port}/");

    let broker = Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone { url: url.clone() },
        group: Some("reconnect".into()),
        ..Default::default()
    })
    .await
    .expect("connect");

    broker
        .topology()
        .declare::<ReconnectTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<ReconnectTopic>(&Order { id: 1 })
        .await
        .expect("publish 1");

    let counter = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct CountingHandler(Arc<AtomicUsize>);
    impl MessageHandler<ReconnectTopic> for CountingHandler {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    let token = supervisor.cancellation_token();
    supervisor
        .register::<ReconnectTopic, _>(
            CountingHandler(counter.clone()),
            ConsumerOptions::<Redis>::new(),
        )
        .expect("register");

    // Wait for the first message to be consumed.
    let probe1 = counter.clone();
    assert!(
        poll_until(
            move || probe1.load(Ordering::Relaxed) >= 1,
            Duration::from_secs(10),
        )
        .await,
        "first message was not consumed"
    );

    // --- Simulate Redis outage ---
    // Kill and restart the container. The fixed port binding ensures
    // the consumer's cached URL remains valid across the restart.
    let container_id = container.id().to_string();
    let status = std::process::Command::new("docker")
        .args(["kill", &container_id])
        .status()
        .expect("docker kill");
    assert!(status.success(), "docker kill failed");

    // Let the consumer notice the disconnect and enter the backoff loop.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let status = std::process::Command::new("docker")
        .args(["start", &container_id])
        .status()
        .expect("docker start");
    assert!(status.success(), "docker start failed");

    // Give Redis time to initialise and accept connections.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify Redis is reachable before proceeding.
    let ping = std::process::Command::new("docker")
        .args(["exec", &container_id, "redis-cli", "ping"])
        .output()
        .expect("docker exec redis-cli");
    assert!(
        ping.status.success(),
        "redis-cli ping failed: {:?}",
        String::from_utf8_lossy(&ping.stderr)
    );

    // Reconnect and redeclare topology (data was lost in the restart).
    // Redis is confirmed running at this point.
    let broker2 = Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone { url: url.clone() },
        group: Some("reconnect".into()),
        ..Default::default()
    })
    .await
    .expect("reconnect after restart");

    broker2
        .topology()
        .declare::<ReconnectTopic>()
        .await
        .expect("redeclare");

    let publisher2 = broker2.publisher().await.expect("publisher2");
    publisher2
        .publish::<ReconnectTopic>(&Order { id: 2 })
        .await
        .expect("publish 2");

    // Wait for the consumer to reconnect and process the second message.
    let probe2 = counter.clone();
    assert!(
        poll_until(
            move || probe2.load(Ordering::Relaxed) >= 2,
            Duration::from_secs(30),
        )
        .await,
        "consumer did not recover after Redis restart (counter = {})",
        counter.load(Ordering::Relaxed)
    );

    // Clean shutdown.
    token.cancel();
    let outcome = supervisor
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(5))
        .await;
    assert!(
        outcome.is_clean(),
        "supervisor outcome not clean: {outcome:?}"
    );

    // Container cleanup runs via `ContainerOnDrop::drop` even on panic.
}

// ---------------------------------------------------------------------------
// Requeuer reconnect test
// ---------------------------------------------------------------------------

/// Verify that the hold-queue requeuer reconnects after a Redis restart and
/// continues delivering hold-set entries to the stream.
///
/// Fix #14: ZRANGE errors now propagate (instead of being swallowed as an
///   empty entry list), causing spawn_requeuer to enter its reconnect branch.
/// Fix #24: acquire_conn_with_retry retries with backoff on connection failure
///   instead of silently exiting the requeuer task.
///
/// The test demonstrates both: after kill+restart, a message that enters the
/// hold queue is redelivered by the requeuer — proving it reconnected and can
/// successfully poll the hold set.
#[tokio::test]
async fn requeuer_reconnects_after_redis_restart_and_delivers_hold_entries() {
    let host_port: u16 = find_free_port();
    let container = ContainerOnDrop::new(
        RedisContainer::default()
            .with_tag("7.0")
            .with_mapped_port(host_port, ContainerPort::Tcp(REDIS_PORT))
            .start()
            .await
            .expect("start Redis container"),
    );
    let host = container.get_host().await.expect("get host");
    let url = format!("redis://{host}:{host_port}/");

    let broker = Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone { url: url.clone() },
        group: Some("requeuer-recover".into()),
        ..Default::default()
    })
    .await
    .expect("connect");

    broker
        .topology()
        .declare::<RequeuerRecoverTopic>()
        .await
        .expect("declare");

    // Handler: first delivery → Retry (puts message in hold set);
    // second delivery (from hold set) → Ack.
    let call_count = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<RequeuerRecoverTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            let n = self.0.fetch_add(1, Ordering::Relaxed);
            if n == 0 { Outcome::Retry } else { Outcome::Ack }
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    let token = supervisor.cancellation_token();
    supervisor
        .register::<RequeuerRecoverTopic, _>(
            H(call_count.clone()),
            ConsumerOptions::<Redis>::new().with_max_retries(5),
        )
        .expect("register");

    // Kill and restart Redis. The consumer and requeuer both lose their
    // connections and must reconnect via their respective retry loops.
    let container_id = container.id().to_string();
    let status = std::process::Command::new("docker")
        .args(["kill", &container_id])
        .status()
        .expect("docker kill");
    assert!(status.success(), "docker kill failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let status = std::process::Command::new("docker")
        .args(["start", &container_id])
        .status()
        .expect("docker start");
    assert!(status.success(), "docker start failed");

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Redeclare topology (data was wiped) and publish a message via a fresh broker.
    let broker2 = Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone { url: url.clone() },
        group: Some("requeuer-recover".into()),
        ..Default::default()
    })
    .await
    .expect("reconnect after restart");

    broker2
        .topology()
        .declare::<RequeuerRecoverTopic>()
        .await
        .expect("redeclare");

    let publisher2 = broker2.publisher().await.expect("publisher2");
    publisher2
        .publish::<RequeuerRecoverTopic>(&Order { id: 1 })
        .await
        .expect("publish");

    // call_count >= 2 proves:
    //   1. Consumer reconnected and received the message (call 0 → Retry).
    //   2. Requeuer reconnected, polled the hold set (ZRANGE succeeded, fix #14),
    //      and redelivered the message (call 1 → Ack).
    let probe = call_count.clone();
    assert!(
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 2,
            Duration::from_secs(30),
        )
        .await,
        "requeuer did not reconnect and redeliver hold entry (call_count = {})",
        call_count.load(Ordering::Relaxed)
    );

    // Graceful supervisor drain so any error/panic tally from the reconnect
    // path is surfaced rather than swallowed by JoinSet drop on supervisor
    // drop. Matches the sibling reconnect test.
    token.cancel();
    let outcome = supervisor
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(5))
        .await;
    assert!(
        outcome.is_clean(),
        "supervisor outcome not clean: {outcome:?}"
    );

    // Container cleanup runs via `ContainerOnDrop::drop` even on panic.
}

// ---------------------------------------------------------------------------
// Test 9: redis_with_handler_timeout_aborts_slow_handler
//
// Redis Streams keeps a timed-out message in the consumer's PEL for
// XAUTOCLAIM to reclaim after `idle_ms` (= the configured handler_timeout).
// The consumer's periodic XAUTOCLAIM only runs every `idle_ms.max(30_000)`
// ms — at least 30 s — so redelivery within a short test window is not
// observable. Instead of asserting redelivery this test asserts the
// timeout actually fired: `with_handler_timeout(100ms)` aborts the handler
// before it can finish a 400 ms sleep, leaving `completed == 0` while
// `started >= 1`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn redis_with_handler_timeout_aborts_slow_handler() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use shove::ConsumerGroupConfig;
    use shove::redis::RedisConsumerGroupConfig;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SlowOrder {
        id: u64,
    }

    struct SlowOrdersTopic;
    impl Topic for SlowOrdersTopic {
        type Message = SlowOrder;
        type Codec = JsonCodec;
        fn topology() -> &'static shove::QueueTopology {
            static T: OnceLock<shove::QueueTopology> = OnceLock::new();
            T.get_or_init(|| {
                TopologyBuilder::new("redis-int-slow-orders")
                    .hold_queue(Duration::from_millis(100))
                    .dlq()
                    .build()
            })
        }
    }

    #[derive(Clone)]
    struct Ctx {
        started: Arc<AtomicU32>,
        completed: Arc<AtomicU32>,
    }

    struct SlowHandler;
    impl MessageHandler<SlowOrdersTopic> for SlowHandler {
        type Context = Ctx;
        async fn handle(&self, _msg: SlowOrder, _meta: MessageMetadata, ctx: &Ctx) -> Outcome {
            ctx.started.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(400)).await;
            ctx.completed.fetch_add(1, Ordering::SeqCst);
            Outcome::Ack
        }
    }

    let broker = make_broker("slow-orders-group").await;
    broker
        .topology()
        .declare::<SlowOrdersTopic>()
        .await
        .expect("declare");
    let ctx = Ctx {
        started: Arc::new(AtomicU32::new(0)),
        completed: Arc::new(AtomicU32::new(0)),
    };

    let mut group = broker.consumer_group().with_context(ctx.clone());
    group
        .register::<SlowOrdersTopic, _>(
            ConsumerGroupConfig::new(
                RedisConsumerGroupConfig::new(1..=1)
                    .with_handler_timeout(Duration::from_millis(100)),
            ),
            || SlowHandler,
        )
        .await
        .expect("register");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<SlowOrdersTopic>(&SlowOrder { id: 1 })
        .await
        .expect("publish");

    // Cancel once the handler has been entered AND given enough wall time
    // to confirm it didn't complete its 400ms sleep within the 100ms
    // timeout. The wait is gated on `started >= 1` so a regression that
    // never invokes the handler shows up as a poll_until failure rather
    // than a silently-passing `completed == 0`.
    let token = group.cancellation_token();
    let started_probe = ctx.started.clone();
    let canceller_token = token.clone();
    let canceller = tokio::spawn(async move {
        let entered = poll_until(
            move || started_probe.load(Ordering::SeqCst) >= 1,
            Duration::from_secs(2),
        )
        .await;
        // Give the 100ms timeout time to fire after the handler started.
        tokio::time::sleep(Duration::from_millis(300)).await;
        canceller_token.cancel();
        entered
    });
    let outcome = group
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    let entered = canceller.await.expect("canceller");
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(entered, "handler was never invoked");
    assert_eq!(
        ctx.completed.load(Ordering::SeqCst),
        0,
        "handler completed its 400ms sleep despite a 100ms handler_timeout",
    );
}

// ---------------------------------------------------------------------------
// Test 10: registry_default_handler_timeout_aborts_slow_handler
//
// Same shape as Test 9, but the per-group config does NOT call
// `with_handler_timeout` — the timeout is supplied by
// `ConsumerGroup::with_default_handler_timeout` and must reach
// the handler via the registry's pre-resolution.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn redis_registry_default_handler_timeout_aborts_slow_handler() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use shove::ConsumerGroupConfig;
    use shove::redis::RedisConsumerGroupConfig;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DefSlowOrder {
        id: u64,
    }

    struct DefSlowOrdersTopic;
    impl Topic for DefSlowOrdersTopic {
        type Message = DefSlowOrder;
        type Codec = JsonCodec;
        fn topology() -> &'static shove::QueueTopology {
            static T: OnceLock<shove::QueueTopology> = OnceLock::new();
            T.get_or_init(|| {
                TopologyBuilder::new("redis-int-default-slow-orders")
                    .hold_queue(Duration::from_millis(100))
                    .dlq()
                    .build()
            })
        }
    }

    #[derive(Clone)]
    struct Ctx {
        started: Arc<AtomicU32>,
        completed: Arc<AtomicU32>,
    }

    struct SlowHandler;
    impl MessageHandler<DefSlowOrdersTopic> for SlowHandler {
        type Context = Ctx;
        async fn handle(&self, _msg: DefSlowOrder, _meta: MessageMetadata, ctx: &Ctx) -> Outcome {
            ctx.started.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(400)).await;
            ctx.completed.fetch_add(1, Ordering::SeqCst);
            Outcome::Ack
        }
    }

    let broker = make_broker("default-slow-orders-group").await;
    broker
        .topology()
        .declare::<DefSlowOrdersTopic>()
        .await
        .expect("declare");
    let ctx = Ctx {
        started: Arc::new(AtomicU32::new(0)),
        completed: Arc::new(AtomicU32::new(0)),
    };

    let mut group = broker
        .consumer_group()
        .with_context(ctx.clone())
        .with_default_handler_timeout(Duration::from_millis(100));
    group
        .register::<DefSlowOrdersTopic, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::new(1..=1)),
            || SlowHandler,
        )
        .await
        .expect("register");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<DefSlowOrdersTopic>(&DefSlowOrder { id: 1 })
        .await
        .expect("publish");

    let token = group.cancellation_token();
    let started_probe = ctx.started.clone();
    let canceller_token = token.clone();
    let canceller = tokio::spawn(async move {
        let entered = poll_until(
            move || started_probe.load(Ordering::SeqCst) >= 1,
            Duration::from_secs(2),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        canceller_token.cancel();
        entered
    });
    let outcome = group
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    let entered = canceller.await.expect("canceller");
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert!(entered, "handler was never invoked");
    assert_eq!(
        ctx.completed.load(Ordering::SeqCst),
        0,
        "handler completed its 400ms sleep despite a 100ms registry default handler_timeout",
    );
}

// ---------------------------------------------------------------------------
// Concurrent-processing topics
// ---------------------------------------------------------------------------

struct ConcurrentTopic;
impl Topic for ConcurrentTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-concurrent").dlq().build())
    }
}

struct SequentialTopic;
impl Topic for SequentialTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-sequential").dlq().build())
    }
}

// ---------------------------------------------------------------------------
// Concurrent-processing test: handlers run in parallel up to prefetch_count
// ---------------------------------------------------------------------------
//
// With concurrent_processing=true and prefetch_count=10, a single consumer
// pulling 10 messages whose handlers each sleep 200ms should finish the wave
// in ~200ms, not 2000ms.

#[tokio::test]
async fn concurrent_processing_runs_handlers_in_parallel() {
    let broker = make_broker("redis-int-concurrent-grp").await;
    broker
        .topology()
        .declare::<ConcurrentTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    let total: usize = 10;
    for i in 0..total {
        publisher
            .publish::<ConcurrentTopic>(&Order { id: i as u64 })
            .await
            .expect("publish");
    }

    let count = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<ConcurrentTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            tokio::time::sleep(Duration::from_millis(200)).await;
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let count_for_handler = count.clone();
    let mut group = broker.consumer_group();
    group
        .register::<ConcurrentTopic, _>(
            ConsumerGroupConfig::new(
                RedisConsumerGroupConfig::new(1..=1)
                    .with_prefetch_count(total as u16)
                    .with_concurrent_processing(true),
            ),
            move || H(Arc::clone(&count_for_handler)),
        )
        .await
        .expect("register");

    let probe = count.clone();
    let started = std::time::Instant::now();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= total,
            Duration::from_secs(10),
        )
        .await;
    };

    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    let elapsed = started.elapsed();

    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(
        count.load(Ordering::Relaxed),
        total,
        "all {total} messages must be processed"
    );
    // Single-thread sequential would be ~2000ms (10 × 200ms). One concurrent
    // wave is ~200ms. Allow generous headroom for Redis RTT + scheduling.
    assert!(
        elapsed < Duration::from_millis(900),
        "concurrent processing took {elapsed:?}; expected < 900ms (sequential would be ~2s)"
    );
}

// ---------------------------------------------------------------------------
// Concurrent-processing test: concurrent_processing=false stays sequential
// even when prefetch_count is large
// ---------------------------------------------------------------------------
//
// With prefetch_count=10 but concurrent_processing=false, handlers must run
// one at a time. The registry clamps the effective prefetch to 1 for
// non-concurrent groups so XREADGROUP returns at most one message per fetch.

#[tokio::test]
async fn concurrent_processing_false_serializes_handlers() {
    let broker = make_broker("redis-int-sequential-grp").await;
    broker
        .topology()
        .declare::<SequentialTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    let total: usize = 5;
    for i in 0..total {
        publisher
            .publish::<SequentialTopic>(&Order { id: i as u64 })
            .await
            .expect("publish");
    }

    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let count = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H {
        in_flight: Arc<AtomicUsize>,
        max_seen: Arc<AtomicUsize>,
        count: Arc<AtomicUsize>,
    }
    impl MessageHandler<SequentialTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_seen.fetch_max(cur, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(80)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.count.fetch_add(1, Ordering::SeqCst);
            Outcome::Ack
        }
    }

    let handler = H {
        in_flight: in_flight.clone(),
        max_seen: max_seen.clone(),
        count: count.clone(),
    };
    let mut group = broker.consumer_group();
    group
        .register::<SequentialTopic, _>(
            ConsumerGroupConfig::new(
                RedisConsumerGroupConfig::new(1..=1)
                    .with_prefetch_count(total as u16)
                    .with_concurrent_processing(false),
            ),
            move || handler.clone(),
        )
        .await
        .expect("register");

    let probe = count.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::SeqCst) >= total,
            Duration::from_secs(10),
        )
        .await;
    };

    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(count.load(Ordering::SeqCst), total);
    assert_eq!(
        max_seen.load(Ordering::SeqCst),
        1,
        "concurrent_processing=false must keep at most one handler in-flight at a time"
    );
}

// ---------------------------------------------------------------------------
// Concurrent-processing test: register_fifo rejects concurrent_processing=true
// ---------------------------------------------------------------------------
//
// FIFO ordering is broken if a single shard's messages are dispatched
// concurrently, so `register_fifo` must reject configs with the flag set.

#[tokio::test]
async fn register_fifo_rejects_concurrent_processing() {
    let broker = make_broker("redis-int-fifo-reject-grp").await;
    broker
        .topology()
        .declare::<LedgerTopic>()
        .await
        .expect("declare");

    struct NoopHandler;
    impl MessageHandler<LedgerTopic> for NoopHandler {
        type Context = ();
        async fn handle(&self, _: Event, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    let mut group = broker.consumer_group();
    let result = group
        .register_fifo::<LedgerTopic, _>(
            ConsumerGroupConfig::new(
                RedisConsumerGroupConfig::new(1..=1).with_concurrent_processing(true),
            ),
            || NoopHandler,
        )
        .await;

    match result {
        Err(shove::ShoveError::Topology(msg)) => {
            assert!(
                msg.contains("concurrent_processing") && msg.contains("FIFO"),
                "unexpected error message: {msg}"
            );
        }
        other => panic!("expected Topology error, got {other:?}"),
    }

    // Group should drain cleanly even after the failed registration.
    let outcome = group
        .run_until_timeout(std::future::ready(()), Duration::from_millis(200))
        .await;
    assert_eq!(outcome.exit_code(), 0);
}

// ---------------------------------------------------------------------------
// Autoscaler — end-to-end scale-up under load
// ---------------------------------------------------------------------------

#[tokio::test]
async fn autoscaler_scales_up_under_load() {
    use shove::redis::{RedisAutoscalerBackend, RedisConsumerGroupRegistry};
    use shove::{AutoscalerConfig, Backend, QueueTopology};
    use std::sync::Arc as StdArc;
    use tokio::sync::Mutex as TokioMutex;
    use tokio_util::sync::CancellationToken;

    struct SlowTopic;
    impl Topic for SlowTopic {
        type Message = u64;
        type Codec = JsonCodec;
        fn topology() -> &'static QueueTopology {
            static T: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("redis-int-autoscaler").build())
        }
    }

    struct SlowHandler;
    impl MessageHandler<SlowTopic> for SlowHandler {
        type Context = ();
        async fn handle(&self, _m: u64, _meta: MessageMetadata, _ctx: &()) -> Outcome {
            // Slow enough that the autoscaler will see backlog.
            tokio::time::sleep(Duration::from_millis(500)).await;
            Outcome::Ack
        }
    }

    let url = redis_url().await;
    let cfg = RedisConfig {
        mode: RedisMode::Standalone {
            url: url.to_owned(),
        },
        group: Some("redis-int-autoscaler-grp".into()),
        ..Default::default()
    };

    // Use the public Backend::connect trait method to obtain a RedisClient
    // we can hand to both the registry and the autoscaler.
    let client = <Redis as Backend>::connect(cfg).await.expect("connect");

    // Declare topology via the standard topology declarer to avoid duplicating
    // declarer-construction logic here.
    let broker = Broker::<Redis>::from_client(client.clone());
    broker
        .topology()
        .declare::<SlowTopic>()
        .await
        .expect("declare");

    // Build a registry, register a single group, and start its consumers.
    let mut registry = RedisConsumerGroupRegistry::new(client.clone());
    registry
        .register::<SlowTopic, SlowHandler>(
            RedisConsumerGroupConfig::new(1..=5).with_prefetch_count(1),
            || SlowHandler,
            (),
        )
        .await
        .expect("register");
    registry.start_all();

    // Publish enough slow messages to build a real backlog.
    let publisher = broker.publisher().await.expect("publisher");
    for i in 0..30u64 {
        shove::Publisher::publish::<SlowTopic>(&publisher, &i)
            .await
            .expect("publish");
    }

    let registry = StdArc::new(TokioMutex::new(registry));

    // Tight autoscaler config so the test finishes quickly.
    let auto = AutoscalerConfig {
        poll_interval: Duration::from_millis(500),
        scale_up_multiplier: 1.5,
        scale_down_multiplier: 0.3,
        hysteresis_duration: Duration::from_millis(500),
        cooldown_duration: Duration::from_millis(500),
    };

    let mut autoscaler = RedisAutoscalerBackend::autoscaler(client.clone(), registry.clone(), auto);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let task = tokio::spawn(async move {
        autoscaler.run(shutdown_clone).await;
    });

    // Wait until the group has scaled past min_consumers.
    let registry_for_poll = registry.clone();
    let scaled = poll_until(
        || {
            let r = registry_for_poll.try_lock();
            match r {
                Ok(reg) => reg
                    .groups()
                    .get("redis-int-autoscaler")
                    .map(|g| g.active_consumers() > 1)
                    .unwrap_or(false),
                Err(_) => false,
            }
        },
        Duration::from_secs(20),
    )
    .await;

    shutdown.cancel();
    let _ = task.await;

    // Drain the registry's consumer groups before returning. We can't
    // try_unwrap the Arc — `registry_for_poll` still holds a strong ref —
    // so we go through the Mutex.
    drop(registry_for_poll);
    registry.lock().await.shutdown_all().await;

    assert!(scaled, "autoscaler failed to scale up under load");
}
