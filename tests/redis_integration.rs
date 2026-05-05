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
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};

use shove::redis::{RedisConfig, RedisMode};
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
        .register_fifo::<LedgerTopic, _>(move || H(Arc::clone(&received_c)))
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
