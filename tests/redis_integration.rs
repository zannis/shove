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

use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};

use shove::consumer_group::ConsumerGroupConfig;
use shove::redis::{
    RedisConfig, RedisConsumer, RedisConsumerGroupConfig, RedisMode, RedisQueueStatsProvider,
};
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
    connect_with_retry(url, group, Duration::from_secs(30)).await
}

/// Connect to Redis with a bounded retry loop.
///
/// Used by tests that manage their own per-test container (the two reconnect
/// tests) since testcontainers can return before Redis is bound, and used as
/// the underlying helper for [`make_broker`] against the shared container.
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

struct RetryTiersTopic;
impl Topic for RetryTiersTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-retry-tiers")
                .hold_queue(Duration::from_millis(100)) // tier 0 — used on first retry
                .hold_queue(Duration::from_secs(30)) // tier 1 — must NOT be used on first retry
                .dlq()
                .build()
        })
    }
}

struct RetryCapTopic;
impl Topic for RetryCapTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-retry-cap")
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
// Publisher connection reuse
// ---------------------------------------------------------------------------

/// Read `total_connections_received` from `INFO stats` — the cumulative count
/// of connections the server has accepted since startup.
async fn total_connections_received(conn: &mut MultiplexedConnection) -> u64 {
    let info: String = redis::cmd("INFO")
        .arg("stats")
        .query_async(conn)
        .await
        .expect("INFO stats");
    info.lines()
        .find_map(|line| line.strip_prefix("total_connections_received:"))
        .expect("total_connections_received present in INFO stats")
        .trim()
        .parse()
        .expect("total_connections_received parses as u64")
}

#[tokio::test]
async fn publisher_reuses_connection_across_publishes() {
    let broker = make_broker("redis-int-conn-reuse").await;
    broker
        .topology()
        .declare::<OrdersTopic>()
        .await
        .expect("declare");

    // Dedicated probe connection for the INFO snapshots, created once up
    // front so the probe itself contributes nothing to the measured delta.
    let url = redis_url().await;
    let probe_client = redis::Client::open(url).expect("raw client");
    let mut probe = probe_client
        .get_multiplexed_async_connection()
        .await
        .expect("probe conn");

    let publisher = broker.publisher().await.expect("publisher");
    // Warm-up publish so any lazily-established publisher connection exists
    // before the baseline snapshot.
    publisher
        .publish::<OrdersTopic>(&Order { id: 0 })
        .await
        .expect("warm-up publish");

    let before = total_connections_received(&mut probe).await;
    for i in 1..=50u64 {
        publisher
            .publish::<OrdersTopic>(&Order { id: i })
            .await
            .expect("publish");
    }
    let after = total_connections_received(&mut probe).await;

    let delta = after - before;
    assert!(
        delta <= 5,
        "50 publishes on one publisher must reuse a cached connection, \
         but the server accepted {delta} new connections"
    );
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

// Regression for the hold-tier off-by-one: the first retry must use hold
// tier 0 (the short 100ms delay), not tier 1. With the bug (indexing by
// retry_count + 1) the first retry went to the 30s tier and the redelivery
// would not arrive within this 8s window.
#[tokio::test]
async fn first_retry_uses_first_hold_tier() {
    let broker = make_broker("redis-int-retry-tiers").await;
    broker
        .topology()
        .declare::<RetryTiersTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<RetryTiersTopic>(&Order { id: 7 })
        .await
        .expect("publish");

    let call_count = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<RetryTiersTopic> for H {
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
        .register::<RetryTiersTopic, _>(
            H(call_count.clone()),
            ConsumerOptions::<Redis>::new().with_max_retries(5),
        )
        .expect("register");

    let probe = call_count.clone();
    // tier 0 = 100ms + ~500ms requeuer poll => redelivery well under 8s;
    // tier 1 = 30s would miss this window entirely (the off-by-one bug).
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 2,
            Duration::from_secs(8),
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
        "first retry must redeliver via the short tier-0 hold queue, not the 30s tier 1"
    );
}

// `max_retries = N` must allow 1 initial attempt + N retries before the
// message is dead-lettered (the documented contract). With max_retries=2 the
// handler runs exactly 3 times.
#[tokio::test]
async fn max_retries_allows_initial_plus_n_retries() {
    let broker = make_broker("redis-int-retry-cap-grp").await;
    broker
        .topology()
        .declare::<RetryCapTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<RetryCapTopic>(&Order { id: 5 })
        .await
        .expect("publish");

    let call_count = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<RetryCapTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Retry
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<RetryCapTopic, _>(
            H(call_count.clone()),
            ConsumerOptions::<Redis>::new().with_max_retries(2),
        )
        .expect("register");

    let probe = call_count.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 3,
            Duration::from_secs(15),
        )
        .await;
        // Allow any erroneous 4th redelivery (~100ms hold + ~500ms poll) to surface.
        tokio::time::sleep(Duration::from_millis(1500)).await;
    };

    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(
        call_count.load(Ordering::Relaxed),
        3,
        "max_retries=2 must allow 1 initial + 2 retries = 3 attempts before DLQ"
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
            *self.0.lock().await = Some((*meta.headers).clone());
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

#[tokio::test]
async fn publish_with_reserved_header_rejected() {
    let broker = make_broker("redis-int-headers-reserved-grp").await;
    broker
        .topology()
        .declare::<HeadersTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");

    // A publisher must not be able to forge an internal routing field; the
    // Redis consumer reads `x-retry-count` off the stream entry to drive
    // retry/DLQ routing, so accepting it would let a publisher poison delivery.
    let mut headers = std::collections::HashMap::new();
    headers.insert("x-retry-count".to_string(), "999".to_string());
    let err = publisher
        .publish_with_headers::<HeadersTopic>(&Order { id: 11 }, headers)
        .await
        .expect_err("reserved header must be rejected");
    assert!(
        matches!(err, shove::ShoveError::Validation(_)),
        "expected Validation error, got: {err:?}"
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
// Test 8b: autoscaler_stats_zero_after_full_consumption
// ---------------------------------------------------------------------------

struct ConsumedStatsTopic;
impl Topic for ConsumedStatsTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-stats-consumed").build())
    }
}

/// XACK leaves entries in the stream, so a backlog metric derived from XLEN
/// reads a fully-drained queue as permanently backlogged — the autoscaler
/// would pin every group at max consumers. After every message is consumed
/// and acked, the stats provider must report zero ready and zero in-flight.
#[tokio::test]
async fn autoscaler_stats_zero_after_full_consumption() {
    let broker = make_broker("redis-int-stats-consumed-grp").await;
    broker
        .topology()
        .declare::<ConsumedStatsTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    for i in 0..20u64 {
        publisher
            .publish::<ConsumedStatsTopic>(&Order { id: i })
            .await
            .expect("publish");
    }

    let counter = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<ConsumedStatsTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<ConsumedStatsTopic, _>(H(counter.clone()), ConsumerOptions::<Redis>::new())
        .expect("register");

    let probe = counter.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 20,
            Duration::from_secs(15),
        )
        .await;
    };
    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(counter.load(Ordering::Relaxed), 20, "all messages consumed");

    let stats = broker
        .queue_stats_provider()
        .get_queue_stats(ConsumedStatsTopic::topology().queue())
        .await
        .expect("get_queue_stats");

    assert_eq!(
        stats.messages_in_flight, 0,
        "all messages were acked — nothing in flight"
    );
    assert_eq!(
        stats.messages_ready, 0,
        "all messages were consumed and acked — a drained queue must not \
         report backlog (XLEN counts acked entries too)"
    );
}

/// Same contract as [`autoscaler_stats_zero_after_full_consumption`], but on
/// Redis 6.2 — the minimum supported server — where `XINFO GROUPS` carries no
/// `lag` field and the stats provider must derive the answer another way.
#[tokio::test]
async fn autoscaler_stats_zero_after_full_consumption_on_redis_62() {
    struct Stats62Topic;
    impl Topic for Stats62Topic {
        type Message = Order;
        type Codec = JsonCodec;
        fn topology() -> &'static shove::QueueTopology {
            static T: OnceLock<shove::QueueTopology> = OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("redis-int-stats-62").build())
        }
    }

    // Own 6.2 container — the shared one runs 7.0.
    let host_port: u16 = find_free_port();
    let container = ContainerOnDrop::new(
        RedisContainer::default()
            .with_tag("6.2")
            .with_mapped_port(host_port, ContainerPort::Tcp(REDIS_PORT))
            .start()
            .await
            .expect("start Redis 6.2 container"),
    );
    let host = container.get_host().await.expect("get host");
    let url = format!("redis://{host}:{host_port}/");

    let broker = connect_with_retry(&url, "redis-62-stats-grp", Duration::from_secs(30)).await;
    broker
        .topology()
        .declare::<Stats62Topic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    for i in 0..5u64 {
        publisher
            .publish::<Stats62Topic>(&Order { id: i })
            .await
            .expect("publish");
    }

    let counter = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<Stats62Topic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<Stats62Topic, _>(H(counter.clone()), ConsumerOptions::<Redis>::new())
        .expect("register");

    let probe = counter.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 5,
            Duration::from_secs(15),
        )
        .await;
    };
    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(counter.load(Ordering::Relaxed), 5, "all messages consumed");

    let stats = broker
        .queue_stats_provider()
        .get_queue_stats(Stats62Topic::topology().queue())
        .await
        .expect("get_queue_stats");

    assert_eq!(stats.messages_in_flight, 0, "all messages were acked");
    assert_eq!(
        stats.messages_ready, 0,
        "a drained queue must report zero backlog on Redis 6.2 even though \
         XINFO GROUPS has no lag field"
    );
}

// ---------------------------------------------------------------------------
// Stream maintenance on the direct consumer path
// ---------------------------------------------------------------------------

struct DirectTrimTopic;
impl Topic for DirectTrimTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-direct-trim").build())
    }
}

#[derive(Clone)]
struct DirectTrimHandler(Arc<AtomicUsize>);
impl MessageHandler<DirectTrimTopic> for DirectTrimHandler {
    type Context = ();
    async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
        self.0.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

/// Consumers driven through the direct `ConsumerSupervisor` path (no
/// `RedisConsumerGroupRegistry`) must also get stream maintenance: a stream
/// whose entries were all consumed and acked must be trimmed while such a
/// consumer is running.
#[tokio::test]
async fn direct_consumer_stream_is_trimmed() {
    let broker = make_broker("redis-int-direct-trim-grp").await;
    broker
        .topology()
        .declare::<DirectTrimTopic>()
        .await
        .expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    for i in 0..10u64 {
        publisher
            .publish::<DirectTrimTopic>(&Order { id: i })
            .await
            .expect("publish");
    }

    // First consumer run: consume and ack everything, then stop.
    let counter = Arc::new(AtomicUsize::new(0));
    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<DirectTrimTopic, _>(
            DirectTrimHandler(counter.clone()),
            ConsumerOptions::<Redis>::new(),
        )
        .expect("register");
    let probe = counter.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 10,
            Duration::from_secs(15),
        )
        .await;
    };
    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(counter.load(Ordering::Relaxed), 10, "all messages consumed");

    // Second consumer run on the now fully-acked stream: its maintenance
    // sweep must trim the acked entries.
    let mut supervisor = broker.consumer_supervisor();
    let token = supervisor.cancellation_token();
    supervisor
        .register::<DirectTrimTopic, _>(
            DirectTrimHandler(Arc::new(AtomicUsize::new(0))),
            ConsumerOptions::<Redis>::new(),
        )
        .expect("register");

    let url = redis_url().await;
    let probe_client = redis::Client::open(url).expect("raw client");
    let mut raw = probe_client
        .get_multiplexed_async_connection()
        .await
        .expect("raw conn");

    let trim_signal = async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let len: i64 = redis::cmd("XLEN")
                .arg("redis-int-direct-trim")
                .query_async(&mut raw)
                .await
                .expect("XLEN");
            if len <= 1 || std::time::Instant::now() > deadline {
                token.cancel();
                assert!(
                    len <= 1,
                    "a direct consumer's fully-acked stream must be trimmed, \
                     XLEN still {len}"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let outcome = supervisor
        .run_until_timeout(trim_signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
}

struct DlqKeepTopic;
impl Topic for DlqKeepTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-dlq-keep").dlq().build())
    }
}

#[derive(Clone)]
struct DlqKeepHandler;
impl MessageHandler<DlqKeepTopic> for DlqKeepHandler {
    type Context = ();
    async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
}

/// DLQ streams are an operator audit record: running a DLQ consumer must
/// never enrol the DLQ stream in maintenance trimming, even after every dead
/// message has been delivered and acknowledged.
#[tokio::test]
async fn dlq_stream_is_never_trimmed() {
    let group = "redis-int-dlq-keep-grp";
    let broker = make_broker(group).await;
    broker
        .topology()
        .declare::<DlqKeepTopic>()
        .await
        .expect("declare");
    let dlq = DlqKeepTopic::topology().dlq().expect("topic has a DLQ");

    // Seed five dead messages and mark them delivered + acked, mirroring a
    // DLQ that an operator's handler has already worked through.
    let url = redis_url().await;
    let probe_client = redis::Client::open(url).expect("raw client");
    let mut raw = probe_client
        .get_multiplexed_async_connection()
        .await
        .expect("raw conn");
    let mut ids = Vec::new();
    for i in 0..5 {
        let id: String = redis::cmd("XADD")
            .arg(dlq)
            .arg("*")
            .arg("payload")
            .arg(format!("{{\"id\":{i}}}"))
            .query_async(&mut raw)
            .await
            .expect("XADD");
        ids.push(id);
    }
    let _: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group)
        .arg("dlq-worker")
        .arg("COUNT")
        .arg(5)
        .arg("STREAMS")
        .arg(dlq)
        .arg(">")
        .query_async(&mut raw)
        .await
        .expect("XREADGROUP");
    let mut xack = redis::cmd("XACK");
    xack.arg(dlq).arg(group);
    for id in &ids {
        xack.arg(id);
    }
    let acked: i64 = xack.query_async(&mut raw).await.expect("XACK");
    assert_eq!(acked, 5);

    // Run a DLQ consumer over the fully-acked DLQ stream. If it (wrongly)
    // enrols the stream in maintenance, the startup sweep trims it.
    let cfg = RedisConfig::new(RedisMode::Standalone {
        url: url.to_owned(),
    })
    .with_group(group);
    let client = <Redis as shove::Backend>::connect(cfg)
        .await
        .expect("connect RedisClient");
    let dlq_task = tokio::spawn(async move {
        let consumer = RedisConsumer::new(client);
        let _ = consumer
            .run_dlq::<DlqKeepTopic, _>(DlqKeepHandler, ())
            .await;
    });

    tokio::time::sleep(Duration::from_millis(1500)).await;
    let len: i64 = redis::cmd("XLEN")
        .arg(dlq)
        .query_async(&mut raw)
        .await
        .expect("XLEN");

    dlq_task.abort();
    let _ = dlq_task.await;

    assert_eq!(
        len, 5,
        "DLQ streams are an audit record and must never be trimmed"
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

    let broker = connect_with_retry(&url, "reconnect", Duration::from_secs(30)).await;

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

    // Reconnect and redeclare topology (data was lost in the restart).
    // connect_with_retry covers the post-restart bind race.
    let broker2 = connect_with_retry(&url, "reconnect", Duration::from_secs(30)).await;

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

    let broker = connect_with_retry(&url, "requeuer-recover", Duration::from_secs(30)).await;

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

    // Redeclare topology (data was wiped) and publish a message via a fresh
    // broker. connect_with_retry covers the post-restart bind race.
    let broker2 = connect_with_retry(&url, "requeuer-recover", Duration::from_secs(30)).await;

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
// arch-8: register / register_fifo must auto-declare topology
// ---------------------------------------------------------------------------

struct RegAutoDeclareTopic;
impl Topic for RegAutoDeclareTopic {
    type Message = Order;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-reg-auto-decl").build())
    }
}

/// `consumer_group().register()` must create the Redis stream and consumer
/// group without requiring a prior `topology().declare()` call — identical to
/// RabbitMQ, NATS, Kafka, and InMemory which all auto-declare inside
/// `register`.
#[tokio::test]
async fn consumer_group_register_auto_declares_topology() {
    let broker = make_broker("redis-int-reg-auto-decl").await;

    // register must internally declare the stream and consumer group.
    // No explicit topology().declare() here.
    let count = Arc::new(AtomicUsize::new(0));
    #[derive(Clone)]
    struct H(Arc<AtomicUsize>);
    impl MessageHandler<RegAutoDeclareTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }

    let mut group = broker.consumer_group();
    let counter = count.clone();
    group
        .register::<RegAutoDeclareTopic, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::new(1..=1)),
            move || H(counter.clone()),
        )
        .await
        .expect("register must succeed and auto-declare stream + consumer group");

    // Publish after register — stream and group must exist by now, so these
    // messages will land after the group's $ start-ID and be visible.
    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<RegAutoDeclareTopic>(&Order { id: 1 })
        .await
        .expect("publish");

    let probe = count.clone();
    let signal = async move {
        poll_until(
            move || probe.load(Ordering::Relaxed) >= 1,
            Duration::from_secs(15),
        )
        .await;
    };

    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(count.load(Ordering::Relaxed), 1);
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
    let cfg = RedisConfig::new(RedisMode::Standalone {
        url: url.to_owned(),
    })
    .with_group("redis-int-autoscaler-grp");

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

// ---------------------------------------------------------------------------
// Reaper sidecar
// ---------------------------------------------------------------------------
//
// These tests exercise `spawn_reaper` directly so we can pass a sub-second
// interval and a zero min_idle_ms — the registry-bound interval floor (30 s)
// would otherwise make timing-based assertions painful.

mod reaper_tests {
    use super::*;
    use redis::aio::MultiplexedConnection;
    use shove::Backend;
    use shove::redis::{RedisClient, spawn_reaper};
    use tokio_util::sync::CancellationToken;

    /// Open a raw multiplexed connection to the shared Redis URL for issuing
    /// arbitrary commands (XADD / XGROUP / XREADGROUP / XPENDING). Each test
    /// uses unique stream and group names so they can run in parallel without
    /// touching one another's state.
    ///
    /// Mirrors `make_broker`'s retry: testcontainers occasionally returns
    /// before Redis is fully accepting connections, especially when many
    /// tests start in parallel under nextest.
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

    /// Connect a `RedisClient` (the shove-level handle) with the same
    /// container-init retry as `make_broker`. Used by the reaper tests so
    /// we don't have to go through `Broker` just to get a raw client.
    async fn connect_client_with_retry(url: &str, group: &str) -> RedisClient {
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

    /// Set up a stream + consumer group and claim one entry to a synthetic
    /// "dead" consumer name. Returns the entry's stream ID for any follow-up
    /// assertions.
    async fn seed_stale_entry(
        conn: &mut MultiplexedConnection,
        stream: &str,
        group: &str,
    ) -> String {
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(stream).query_async(conn).await;
        let _: redis::RedisResult<String> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(conn)
            .await;
        let id: String = redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("payload")
            .arg("v")
            .query_async(conn)
            .await
            .expect("XADD");
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg("dead-consumer")
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(conn)
            .await
            .expect("XREADGROUP claim to dead consumer");
        id
    }

    /// Return the number of XPENDING entries currently owned by
    /// `consumer_name` on `stream`/`group`.
    async fn pending_count_for(
        conn: &mut MultiplexedConnection,
        stream: &str,
        group: &str,
        consumer_name: &str,
    ) -> usize {
        let reply: redis::Value = redis::cmd("XPENDING")
            .arg(stream)
            .arg(group)
            .arg("-")
            .arg("+")
            .arg(100)
            .arg(consumer_name)
            .query_async(conn)
            .await
            .expect("XPENDING");
        match reply {
            redis::Value::Array(entries) => entries.len(),
            redis::Value::Nil => 0,
            other => panic!("unexpected XPENDING reply: {other:?}"),
        }
    }

    /// `spawn_reaper` must return promptly when the shutdown token is
    /// cancelled while the loop is in its inter-tick sleep. Uses a long
    /// interval so the sleep arm is the one that fires.
    #[tokio::test]
    async fn reaper_exits_promptly_on_shutdown_during_sleep() {
        let url = redis_url().await;
        let client = connect_client_with_retry(url, "reaper-exit-sleep").await;

        let shutdown = CancellationToken::new();
        // Long interval — the reaper will be parked in sleep almost immediately.
        let handle = spawn_reaper(
            client,
            vec!["reaper-exit-sleep-stream".to_string()],
            "reaper-exit-sleep".to_string(),
            Duration::from_secs(60),
            30_000,
            shutdown.clone(),
        );

        // Give the spawn a moment to enter its select-on-sleep.
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();

        tokio::time::timeout(Duration::from_millis(500), handle)
            .await
            .expect("reaper must exit within 500 ms of cancel")
            .expect("reaper task must not panic");
    }

    /// The reaper claims stale PEL entries and immediately re-delivers them:
    /// it XADDs the entry back to the stream and XACKs the original, leaving
    /// the reaper PEL empty. A fresh consumer must be able to receive the
    /// re-added entry via `XREADGROUP ... >`.
    #[tokio::test]
    async fn reaper_claims_stale_pel_entries() {
        let url = redis_url().await;
        let stream = "reaper-stale-stream";
        let group = "reaper-stale-grp";
        let reaper_name = format!("shove-reaper-{group}");

        let mut raw = raw_conn(url).await;
        let _ = seed_stale_entry(&mut raw, stream, group).await;

        let client = connect_client_with_retry(url, group).await;

        let shutdown = CancellationToken::new();
        // Short interval + zero min_idle so the reaper sweeps on its first tick.
        let handle = spawn_reaper(
            client,
            vec![stream.to_string()],
            group.to_string(),
            Duration::from_millis(100),
            0,
            shutdown.clone(),
        );

        // Poll up to 2 s for the re-delivered entry to appear via XREADGROUP >.
        // The reaper XADDs then XACKs each claimed entry, so the reaper PEL
        // stays empty; the entry appears as a new stream message instead.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut redelivered = 0usize;
        while std::time::Instant::now() < deadline {
            let raw_reply: redis::Value = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(group)
                .arg("stale-verify-consumer")
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

        shutdown.cancel();
        let _ = handle.await;

        // Reaper PEL must be empty — it XACKed after re-XADD.
        let reaper_pel = pending_count_for(&mut raw, stream, group, &reaper_name).await;

        assert!(
            redelivered > 0,
            "reaper did not redeliver the stale entry via XREADGROUP '>'"
        );
        assert_eq!(
            reaper_pel, 0,
            "reaper PEL must be empty after re-XADD + XACK (was {reaper_pel})"
        );
    }

    /// `spawn_reaper` accepts multiple streams and must sweep all of them
    /// in a single tick — not just the first. Evidence: after the reaper
    /// processes both streams, each one must have a re-delivered entry
    /// visible via `XREADGROUP ... >` (the reaper re-XADDs and XACKs each
    /// claimed entry, so the reaper PEL stays empty).
    #[tokio::test]
    async fn reaper_sweeps_all_provided_streams() {
        let url = redis_url().await;
        let group = "reaper-multi-grp";
        let streams = ["reaper-multi-s1", "reaper-multi-s2"];

        let mut raw = raw_conn(url).await;
        for s in &streams {
            let _ = seed_stale_entry(&mut raw, s, group).await;
        }

        let client = connect_client_with_retry(url, group).await;

        let shutdown = CancellationToken::new();
        let handle = spawn_reaper(
            client,
            streams.iter().map(|s| s.to_string()).collect(),
            group.to_string(),
            Duration::from_millis(100),
            0,
            shutdown.clone(),
        );

        // Poll both streams up to 3 s. The reaper re-XADDs and XACKs every
        // claimed entry, so entries appear as new stream messages deliverable
        // via `XREADGROUP ... >` rather than accumulating in the reaper PEL.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut redelivered = [0usize; 2];
        while std::time::Instant::now() < deadline {
            for (i, s) in streams.iter().enumerate() {
                if redelivered[i] > 0 {
                    continue;
                }
                let consumer = format!("verify-consumer-{i}");
                let raw_reply: redis::Value = redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(group)
                    .arg(&consumer)
                    .arg("COUNT")
                    .arg(10)
                    .arg("STREAMS")
                    .arg(s)
                    .arg(">")
                    .query_async(&mut raw)
                    .await
                    .unwrap_or(redis::Value::Nil);
                if let redis::Value::Array(ref outer) = raw_reply
                    && let Some(redis::Value::Array(stream_pair)) = outer.first()
                    && let Some(redis::Value::Array(entries)) = stream_pair.get(1)
                {
                    redelivered[i] = entries.len();
                }
            }
            if redelivered.iter().all(|c| *c > 0) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown.cancel();
        let _ = handle.await;

        for (i, s) in streams.iter().enumerate() {
            assert!(
                redelivered[i] > 0,
                "reaper did not redeliver stale entry on stream {s}"
            );
        }
    }

    /// Even when the reaper is iterating its `streams` Vec — between
    /// per-stream `autoclaim_all` calls — a shutdown signal should be
    /// observed. Configure a long interval but many streams so we land
    /// inside the for-loop, then cancel.
    #[tokio::test]
    async fn reaper_exits_promptly_on_shutdown_between_streams() {
        let url = redis_url().await;
        let group = "reaper-between-grp";

        let mut raw = raw_conn(url).await;
        let streams: Vec<String> = (0..8)
            .map(|i| format!("reaper-between-stream-{i}"))
            .collect();
        // Seed all 8 with stale entries so each per-stream sweep does
        // real work (even if tiny) — this gives the cancellation a
        // realistic mid-iteration window to fire.
        for s in &streams {
            let _ = seed_stale_entry(&mut raw, s, group).await;
        }

        let client = connect_client_with_retry(url, group).await;

        let shutdown = CancellationToken::new();
        // 200 ms interval so the first tick fires quickly; the for-loop
        // over 8 streams gives us the window.
        let handle = spawn_reaper(
            client,
            streams.clone(),
            group.to_string(),
            Duration::from_millis(200),
            0,
            shutdown.clone(),
        );

        // Wait for the first interval to fire, then cancel mid-loop.
        tokio::time::sleep(Duration::from_millis(220)).await;
        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("reaper must exit within 1 s of cancel during stream loop")
            .expect("reaper task must not panic");
    }

    /// After the reaper claims stale PEL entries via XAUTOCLAIM it must
    /// re-XADD them to the stream so that regular consumers can receive them
    /// through `XREADGROUP ... >`. Without this, claimed entries accumulate
    /// in the reaper's own PEL with nothing consuming them.
    ///
    /// Scenario:
    /// 1. A "dead" consumer holds an entry in its PEL (simulating a handler
    ///    that timed out without XACK'ing).
    /// 2. The reaper runs XAUTOCLAIM, transferring ownership to itself.
    /// 3. The reaper must immediately re-XADD the entry and XACK the original,
    ///    leaving the reaper PEL empty.
    /// 4. A fresh consumer then receives the re-added entry via
    ///    `XREADGROUP GROUP g fresh COUNT n STREAMS s >`.
    #[tokio::test]
    async fn reaper_redelivers_claimed_entries_to_stream() {
        let url = redis_url().await;
        let stream = "reaper-redeliver-stream";
        let group = "reaper-redeliver-grp";
        let reaper_name = format!("shove-reaper-{group}");

        let mut raw = raw_conn(url).await;
        // Seed one entry owned by a "dead" consumer — its PEL clock starts now.
        let _ = seed_stale_entry(&mut raw, stream, group).await;

        let client = connect_client_with_retry(url, group).await;

        let shutdown = CancellationToken::new();
        // min_idle = 0 → the entry is eligible for XAUTOCLAIM immediately.
        let handle = spawn_reaper(
            client,
            vec![stream.to_string()],
            group.to_string(),
            Duration::from_millis(100),
            0,
            shutdown.clone(),
        );

        // Poll until a fresh consumer can read a re-delivered entry OR we
        // time out. The delivery happens in two stages:
        //   a) XAUTOCLAIM moves the entry to the reaper's PEL.
        //   b) The reaper re-XADDs it and XACKs the original.
        // After (b) the entry appears as a new stream entry visible to `>`.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut redelivered = 0usize;
        while std::time::Instant::now() < deadline {
            let raw_reply: redis::Value = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(group)
                .arg("fresh-verification-consumer")
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

        shutdown.cancel();
        let _ = handle.await;

        // Also verify the reaper's own PEL is empty (it XACKed the original).
        let reaper_pel = pending_count_for(&mut raw, stream, group, &reaper_name).await;

        assert!(
            redelivered > 0,
            "reaper must re-XADD claimed entries so they are visible to new consumers via XREADGROUP '>'"
        );
        assert_eq!(
            reaper_pel, 0,
            "reaper PEL must be empty after re-XADD + XACK (was {reaper_pel})"
        );
    }

    /// XACK removes an entry from the PEL but not from the stream, so without
    /// trimming every stream grows forever. The reaper must XTRIM entries the
    /// group has fully acknowledged.
    #[tokio::test]
    async fn reaper_trims_fully_acked_entries() {
        let url = redis_url().await;
        let stream = "reaper-trim-acked-stream";
        let group = "reaper-trim-acked-grp";

        let mut raw = raw_conn(url).await;
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(stream).query_async(&mut raw).await;
        let _: redis::RedisResult<String> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut raw)
            .await;

        // Publish 10, deliver all to a consumer, ack all — PEL empty.
        let mut ids = Vec::new();
        for i in 0..10 {
            let id: String = redis::cmd("XADD")
                .arg(stream)
                .arg("*")
                .arg("payload")
                .arg(format!("v{i}"))
                .query_async(&mut raw)
                .await
                .expect("XADD");
            ids.push(id);
        }
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg("trim-consumer")
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut raw)
            .await
            .expect("XREADGROUP");
        let mut xack = redis::cmd("XACK");
        xack.arg(stream).arg(group);
        for id in &ids {
            xack.arg(id);
        }
        let acked: i64 = xack.query_async(&mut raw).await.expect("XACK");
        assert_eq!(acked, 10, "all 10 entries must be acked");

        let client = connect_client_with_retry(url, group).await;
        let shutdown = CancellationToken::new();
        // Large min_idle so XAUTOCLAIM reclaims nothing — this test isolates
        // the trim behaviour from redelivery.
        let handle = spawn_reaper(
            client,
            vec![stream.to_string()],
            group.to_string(),
            Duration::from_millis(100),
            60_000,
            shutdown.clone(),
        );

        // Acked entries must be trimmed away. The last delivered entry may
        // be kept (conservative MINID threshold), so expect XLEN <= 1.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut len: i64 = i64::MAX;
        while std::time::Instant::now() < deadline {
            len = redis::cmd("XLEN")
                .arg(stream)
                .query_async(&mut raw)
                .await
                .expect("XLEN");
            if len <= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown.cancel();
        let _ = handle.await;

        assert!(
            len <= 1,
            "fully-acked entries must be trimmed from the stream, XLEN still {len}"
        );
    }

    /// Trimming must never remove entries that are still pending (delivered
    /// but not acknowledged) — those are the at-least-once redelivery source.
    #[tokio::test]
    async fn reaper_trim_preserves_pending_entries() {
        let url = redis_url().await;
        let stream = "reaper-trim-pending-stream";
        let group = "reaper-trim-pending-grp";

        let mut raw = raw_conn(url).await;
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(stream).query_async(&mut raw).await;
        let _: redis::RedisResult<String> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut raw)
            .await;

        // Publish 5, deliver all, ack only the first 3 — entries 4 and 5
        // stay in the PEL.
        let mut ids = Vec::new();
        for i in 0..5 {
            let id: String = redis::cmd("XADD")
                .arg(stream)
                .arg("*")
                .arg("payload")
                .arg(format!("v{i}"))
                .query_async(&mut raw)
                .await
                .expect("XADD");
            ids.push(id);
        }
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg("trim-consumer")
            .arg("COUNT")
            .arg(5)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut raw)
            .await
            .expect("XREADGROUP");
        let acked: i64 = redis::cmd("XACK")
            .arg(stream)
            .arg(group)
            .arg(&ids[0])
            .arg(&ids[1])
            .arg(&ids[2])
            .query_async(&mut raw)
            .await
            .expect("XACK");
        assert_eq!(acked, 3);

        let client = connect_client_with_retry(url, group).await;
        let shutdown = CancellationToken::new();
        // Large min_idle so the pending entries are not autoclaim-redelivered
        // during the test window.
        let handle = spawn_reaper(
            client,
            vec![stream.to_string()],
            group.to_string(),
            Duration::from_millis(100),
            60_000,
            shutdown.clone(),
        );

        // Wait until the acked prefix is trimmed (XLEN drops to 2)…
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut len: i64 = i64::MAX;
        while std::time::Instant::now() < deadline {
            len = redis::cmd("XLEN")
                .arg(stream)
                .query_async(&mut raw)
                .await
                .expect("XLEN");
            if len <= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown.cancel();
        let _ = handle.await;

        assert_eq!(len, 2, "only the 3 acked entries may be trimmed");

        // …and the two pending entries must still be readable in the stream.
        let range: Vec<redis::Value> = redis::cmd("XRANGE")
            .arg(stream)
            .arg(&ids[3])
            .arg(&ids[4])
            .query_async(&mut raw)
            .await
            .expect("XRANGE");
        assert_eq!(
            range.len(),
            2,
            "pending (unacked) entries must survive the trim"
        );
    }

    /// A stream can carry more than one consumer group (fan-out via
    /// `RedisConfig::with_group`). Trimming driven by one group's progress
    /// must never delete entries another, slower group has not consumed.
    #[tokio::test]
    async fn reaper_trim_respects_slower_second_group() {
        let url = redis_url().await;
        let stream = "reaper-trim-multigroup-stream";
        let group_a = "reaper-trim-multigrp-a";
        let group_b = "reaper-trim-multigrp-b";

        let mut raw = raw_conn(url).await;
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(stream).query_async(&mut raw).await;
        for g in [group_a, group_b] {
            let _: redis::RedisResult<String> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream)
                .arg(g)
                .arg("0")
                .arg("MKSTREAM")
                .query_async(&mut raw)
                .await;
        }

        let mut ids = Vec::new();
        for i in 0..10 {
            let id: String = redis::cmd("XADD")
                .arg(stream)
                .arg("*")
                .arg("payload")
                .arg(format!("v{i}"))
                .query_async(&mut raw)
                .await
                .expect("XADD");
            ids.push(id);
        }

        // Group A consumes and acks everything.
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_a)
            .arg("ca")
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut raw)
            .await
            .expect("XREADGROUP a");
        let mut xack = redis::cmd("XACK");
        xack.arg(stream).arg(group_a);
        for id in &ids {
            xack.arg(id);
        }
        let _: i64 = xack.query_async(&mut raw).await.expect("XACK a");

        // Group B has only consumed (and acked) the first 4.
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_b)
            .arg("cb")
            .arg("COUNT")
            .arg(4)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut raw)
            .await
            .expect("XREADGROUP b");
        let _: i64 = redis::cmd("XACK")
            .arg(stream)
            .arg(group_b)
            .arg(&ids[0])
            .arg(&ids[1])
            .arg(&ids[2])
            .arg(&ids[3])
            .query_async(&mut raw)
            .await
            .expect("XACK b");

        // The reaper runs on behalf of group A.
        let client = connect_client_with_retry(url, group_a).await;
        let shutdown = CancellationToken::new();
        let handle = spawn_reaper(
            client,
            vec![stream.to_string()],
            group_a.to_string(),
            Duration::from_millis(100),
            60_000,
            shutdown.clone(),
        );

        // Wait for a trim to land, then check it stopped at B's checkpoint.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            let len: i64 = redis::cmd("XLEN")
                .arg(stream)
                .query_async(&mut raw)
                .await
                .expect("XLEN");
            if len < 10 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Give a subsequent tick the chance to (incorrectly) trim further.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let len: i64 = redis::cmd("XLEN")
            .arg(stream)
            .query_async(&mut raw)
            .await
            .expect("XLEN");

        shutdown.cancel();
        let _ = handle.await;

        assert_eq!(
            len, 7,
            "trim must stop at the slowest group's checkpoint (B acked 4 of \
             10, so the 3 entries below its last-delivered may go, keeping 7)"
        );
        // B's six undelivered entries must still be readable.
        let range: Vec<redis::Value> = redis::cmd("XRANGE")
            .arg(stream)
            .arg(&ids[4])
            .arg("+")
            .query_async(&mut raw)
            .await
            .expect("XRANGE");
        assert_eq!(
            range.len(),
            6,
            "entries group B has not consumed must survive the trim"
        );
    }

    /// A group that exists but has never consumed anything (created at `0`)
    /// expects the full stream — trimming must be skipped entirely.
    #[tokio::test]
    async fn reaper_trim_skipped_when_a_group_has_not_consumed() {
        let url = redis_url().await;
        let stream = "reaper-trim-freshgroup-stream";
        let group_a = "reaper-trim-freshgrp-a";
        let group_b = "reaper-trim-freshgrp-b";

        let mut raw = raw_conn(url).await;
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(stream).query_async(&mut raw).await;
        for g in [group_a, group_b] {
            let _: redis::RedisResult<String> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream)
                .arg(g)
                .arg("0")
                .arg("MKSTREAM")
                .query_async(&mut raw)
                .await;
        }

        let mut ids = Vec::new();
        for i in 0..10 {
            let id: String = redis::cmd("XADD")
                .arg(stream)
                .arg("*")
                .arg("payload")
                .arg(format!("v{i}"))
                .query_async(&mut raw)
                .await
                .expect("XADD");
            ids.push(id);
        }

        // Group A consumes and acks everything; group B never reads.
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_a)
            .arg("ca")
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut raw)
            .await
            .expect("XREADGROUP a");
        let mut xack = redis::cmd("XACK");
        xack.arg(stream).arg(group_a);
        for id in &ids {
            xack.arg(id);
        }
        let _: i64 = xack.query_async(&mut raw).await.expect("XACK a");

        let client = connect_client_with_retry(url, group_a).await;
        let shutdown = CancellationToken::new();
        let handle = spawn_reaper(
            client,
            vec![stream.to_string()],
            group_a.to_string(),
            Duration::from_millis(100),
            60_000,
            shutdown.clone(),
        );

        // Let several reaper ticks pass, then verify nothing was trimmed.
        tokio::time::sleep(Duration::from_millis(600)).await;
        let len: i64 = redis::cmd("XLEN")
            .arg(stream)
            .query_async(&mut raw)
            .await
            .expect("XLEN");

        shutdown.cancel();
        let _ = handle.await;

        assert_eq!(
            len, 10,
            "a group with no consumption yet (last-delivered 0-0) expects \
             the full stream — trim must be skipped"
        );
    }
}
