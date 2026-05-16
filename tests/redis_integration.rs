//! Integration tests for the Redis Streams backend.
//!
//! These tests spin up a Redis container automatically via testcontainers.
//! Docker (or compatible runtime) must be available.
//!
//! Run with:
//!   cargo test -q --test redis_integration --features redis-streams

#![cfg(feature = "redis-streams")]

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};

use shove::consumer_group::ConsumerGroupConfig;
use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode};
use shove::{
    Broker, ConsumerOptions, MessageHandler, MessageMetadata, Outcome, Redis, SequenceFailure,
    SequencedTopic, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Shared Redis container (started once for the entire test binary)
// ---------------------------------------------------------------------------

static REDIS_URL: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
// Keep the container alive for the duration of the test binary.
static REDIS_CONTAINER: OnceLock<testcontainers::ContainerAsync<RedisContainer>> = OnceLock::new();

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
            REDIS_CONTAINER.set(container).ok();
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
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-dlq-delivery").dlq().build())
    }
}

struct HeadersTopic;
impl Topic for HeadersTopic {
    type Message = Order;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-headers").dlq().build())
    }
}

struct BatchTopic;
impl Topic for BatchTopic {
    type Message = Order;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| TopologyBuilder::new("redis-int-batch").build())
    }
}

struct DeferTopic;
impl Topic for DeferTopic {
    type Message = Order;
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
    let container = RedisContainer::default()
        .with_tag("7.0")
        .with_mapped_port(host_port, ContainerPort::Tcp(REDIS_PORT))
        .start()
        .await
        .expect("start Redis container");
    let host = container.get_host().await.expect("get host");
    let url = format!("redis://{host}:{host_port}/");

    let broker = Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone { url: url.clone() },
        group: Some("reconnect".into()),
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

    container.rm().await.ok();
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
    let container = RedisContainer::default()
        .with_tag("7.0")
        .with_mapped_port(host_port, ContainerPort::Tcp(REDIS_PORT))
        .start()
        .await
        .expect("start Redis container");
    let host = container.get_host().await.expect("get host");
    let url = format!("redis://{host}:{host_port}/");

    let broker = Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone { url: url.clone() },
        group: Some("requeuer-recover".into()),
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

    container.rm().await.ok();
}
