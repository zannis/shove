//! Integration tests for the Redis Streams backend.
//!
//! These tests require a running Redis/Valkey instance. By default they
//! connect to `redis://127.0.0.1:6379/`. Set the `REDIS_URL` environment
//! variable to point at a different instance.
//!
//! Run with:
//!   docker run --rm -p 6379:6379 redis:7-alpine
//!   cargo test --test redis_integration --features redis-streams

#![cfg(feature = "redis-streams")]

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use shove::redis::{RedisConfig, RedisMode};
use shove::{
    Broker, ConsumerOptions, MessageHandler, MessageMetadata, Outcome, Redis,
    SequenceFailure, SequencedTopic, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

async fn make_broker(group: &str) -> Broker<Redis> {
    Broker::<Redis>::new(RedisConfig {
        mode: RedisMode::Standalone {
            url: redis_url(),
        },
        group: Some(group.into()),
    })
    .await
    .expect("connect to Redis")
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

/// Basic pub/sub topic — unique name per test to avoid cross-test stream pollution.
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

/// Topic used for the retry test — separate stream to avoid interference.
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

/// Topic used for the reject test — separate stream to avoid interference.
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

/// Sequenced topic for the FIFO ordering test.
struct LedgerTopic;
impl Topic for LedgerTopic {
    type Message = Event;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-int-ledger")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(4)
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

/// Publish one message, consume with Ack, assert handler called exactly once.
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
        .register::<OrdersTopic, _>(
            H(counter.clone()),
            ConsumerOptions::<Redis>::new(),
        )
        .expect("register");

    let probe = counter.clone();
    let signal = async move {
        poll_until(move || probe.load(Ordering::Relaxed) >= 1, Duration::from_secs(5)).await;
    };

    let outcome = supervisor
        .run_until_timeout(signal, Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "outcome: {outcome:?}");
    assert_eq!(counter.load(Ordering::Relaxed), 1, "handler must be called exactly once");
}

// ---------------------------------------------------------------------------
// Test 2: retry_then_ack_on_redeliver
// ---------------------------------------------------------------------------

/// Publish one message. Handler returns Retry on first delivery (retry_count==0)
/// and Ack on redeliver. Assert the handler was invoked exactly twice.
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
        // Wait until the handler has been called at least twice (retry + ack).
        poll_until(move || probe.load(Ordering::Relaxed) >= 2, Duration::from_secs(10)).await;
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

/// Publish one message. Handler returns Reject. Assert handler was called
/// exactly once and the supervisor shuts down cleanly (no panic).
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
        .register::<RejectTopic, _>(
            H(call_count.clone()),
            ConsumerOptions::<Redis>::new(),
        )
        .expect("register");

    // Wait for at least one invocation, then shut down.
    let probe = call_count.clone();
    let signal = async move {
        poll_until(move || probe.load(Ordering::Relaxed) >= 1, Duration::from_secs(5)).await;
        // Give the consumer a moment to route to DLQ before shutdown.
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

/// Publish 10 events for "acct-1" via LedgerTopic (sequenced). Consume with
/// register_fifo and assert all 10 are received in sequence order.
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
            move || {
                probe
                    .try_lock()
                    .map(|v| v.len() >= 10)
                    .unwrap_or(false)
            },
            Duration::from_secs(10),
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
