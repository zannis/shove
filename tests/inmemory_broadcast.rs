//! Broadcast: every instance of a service receives every message, as its own
//! ephemeral subscriber (`TopologyBuilder::broadcast`).
//!
//! The in-process backend is the substrate this feature is specified against,
//! because the two properties that matter — deliver-new, and nothing left
//! behind — are observable here directly rather than by inspecting a broker.
//! Two `broadcast_subscriber()` handles on one `InMemoryBroker` stand in for
//! two instances of a service.

#![cfg(feature = "inmemory")]

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use shove::inmemory::{InMemoryBroker, InMemoryConsumerGroupConfig};
use shove::{
    Broker, ConsumerGroupConfig, ConsumerOptions, InMemory, MessageHandler, MessageMetadata,
    Outcome, ShoveError, Topic, TopologyBuilder, define_topic,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invalidate {
    key: u64,
}

define_topic!(
    CacheInvalidations,
    Invalidate,
    TopologyBuilder::new("cache-invalidations-bcast")
        .broadcast()
        .build()
);

// A second, ordinary topic on the same broker — used to pin that broadcast
// changes nothing for topologies that do not ask for it.
define_topic!(
    PlainOrders,
    Invalidate,
    TopologyBuilder::new("plain-orders-bcast").build()
);

/// Records every key it is handed, then acks.
#[derive(Clone)]
struct Recorder {
    seen: Arc<Mutex<Vec<u64>>>,
}

impl Recorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn keys(&self) -> Vec<u64> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl MessageHandler<CacheInvalidations> for Recorder {
    type Context = ();
    async fn handle(&self, msg: Invalidate, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        self.seen.lock().expect("seen lock").push(msg.key);
        Outcome::Ack
    }
}

impl MessageHandler<PlainOrders> for Recorder {
    type Context = ();
    async fn handle(&self, msg: Invalidate, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        self.seen.lock().expect("seen lock").push(msg.key);
        Outcome::Ack
    }
}

/// Counts calls and always asks to retry.
#[derive(Clone)]
struct AlwaysRetry {
    calls: Arc<Mutex<u32>>,
}

impl MessageHandler<CacheInvalidations> for AlwaysRetry {
    type Context = ();
    async fn handle(&self, _msg: Invalidate, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        *self.calls.lock().expect("calls lock") += 1;
        Outcome::Retry
    }
}

/// Counts calls and always rejects.
#[derive(Clone)]
struct AlwaysReject {
    calls: Arc<Mutex<u32>>,
}

impl MessageHandler<CacheInvalidations> for AlwaysReject {
    type Context = ();
    async fn handle(&self, _msg: Invalidate, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        *self.calls.lock().expect("calls lock") += 1;
        Outcome::Reject
    }
}

/// Block until `topic` has exactly `n` live subscriptions.
///
/// The subscription is registered by the spawned delivery task, not by
/// `subscribe()` — that is what makes deliver-new structural. So a test that
/// publishes the instant `subscribe()` returns is racing the task's first poll,
/// and would be flaky for a reason that has nothing to do with the property
/// under test. Waiting on the registry removes the race without weakening it:
/// the deliver-new assertions below still publish *before* a subscriber exists.
async fn wait_for_subscribers(broker: &InMemoryBroker, topic: &str, n: usize) {
    for _ in 0..500 {
        if broker.broadcast_subscriber_count(topic) == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!(
        "timed out waiting for {n} subscriber(s) on '{topic}'; have {}",
        broker.broadcast_subscriber_count(topic)
    );
}

/// Give the delivery loops time to drain what was published.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// AC5 — the whole point of the feature: two subscribers in one process each
/// receive **every** message, rather than splitting them.
#[tokio::test]
async fn two_subscribers_each_receive_every_message() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    let publisher = broker.publisher().await.expect("publisher");

    let first = Recorder::new();
    let second = Recorder::new();

    let mut sub_a = broker.broadcast_subscriber();
    sub_a
        .subscribe::<CacheInvalidations, _>(first.clone(), ConsumerOptions::new())
        .expect("subscribe a");
    let mut sub_b = broker.broadcast_subscriber();
    sub_b
        .subscribe::<CacheInvalidations, _>(second.clone(), ConsumerOptions::new())
        .expect("subscribe b");

    wait_for_subscribers(&client, "cache-invalidations-bcast", 2).await;

    for key in 1..=5u64 {
        publisher
            .publish::<CacheInvalidations>(&Invalidate { key })
            .await
            .expect("publish");
    }
    settle().await;

    sub_a.cancellation_token().cancel();
    sub_b.cancellation_token().cancel();
    let outcome_a = sub_a
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    let outcome_b = sub_b
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    assert!(outcome_a.is_clean(), "subscriber a drained cleanly");
    assert!(outcome_b.is_clean(), "subscriber b drained cleanly");

    let mut a = first.keys();
    let mut b = second.keys();
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, vec![1, 2, 3, 4, 5], "subscriber a saw every message");
    assert_eq!(b, vec![1, 2, 3, 4, 5], "subscriber b saw every message");
}

/// AC4 — deliver-new: a subscriber that starts after a publish does not receive
/// it. There is no replay for an instance that was down.
#[tokio::test]
async fn a_subscriber_that_starts_late_misses_earlier_messages() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    let publisher = broker.publisher().await.expect("publisher");

    // Published with nobody subscribed at all: this one is nobody's to receive.
    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 100 })
        .await
        .expect("publish before anyone subscribes");

    let early = Recorder::new();
    let mut sub_early = broker.broadcast_subscriber();
    sub_early
        .subscribe::<CacheInvalidations, _>(early.clone(), ConsumerOptions::new())
        .expect("subscribe early");
    wait_for_subscribers(&client, "cache-invalidations-bcast", 1).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 200 })
        .await
        .expect("publish with one subscriber");
    settle().await;

    // Second instance starts now. 100 and 200 are both already past.
    let late = Recorder::new();
    let mut sub_late = broker.broadcast_subscriber();
    sub_late
        .subscribe::<CacheInvalidations, _>(late.clone(), ConsumerOptions::new())
        .expect("subscribe late");
    wait_for_subscribers(&client, "cache-invalidations-bcast", 2).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 300 })
        .await
        .expect("publish with two subscribers");
    settle().await;

    sub_early.cancellation_token().cancel();
    sub_late.cancellation_token().cancel();
    let _ = sub_early
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    let _ = sub_late
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;

    assert_eq!(
        early.keys(),
        vec![200, 300],
        "the early subscriber saw everything published after it subscribed, and not the one before"
    );
    assert_eq!(
        late.keys(),
        vec![300],
        "the late subscriber received only what was published after it arrived — no replay"
    );
}

/// AC6 — nothing survives a subscriber going away. Once the loops are drained
/// the registry is empty again, and a later publish reaches nobody rather than
/// piling up somewhere waiting to be reaped.
#[tokio::test]
async fn nothing_survives_a_subscriber_going_away() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    let publisher = broker.publisher().await.expect("publisher");

    let recorder = Recorder::new();
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(recorder.clone(), ConsumerOptions::new())
        .expect("subscribe");
    wait_for_subscribers(&client, "cache-invalidations-bcast", 1).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 1 })
        .await
        .expect("publish");
    settle().await;

    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "drained cleanly: {outcome:?}");

    assert_eq!(
        client.broadcast_subscriber_count("cache-invalidations-bcast"),
        0,
        "the subscription is deregistered when its loop ends — nothing to reap"
    );

    // A publish now must not resurrect or buffer anything.
    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 2 })
        .await
        .expect("publish after teardown succeeds — there is simply nobody listening");
    settle().await;
    assert_eq!(
        recorder.keys(),
        vec![1],
        "no message is delivered to a subscription that has gone away"
    );
    assert_eq!(
        client.broadcast_subscriber_count("cache-invalidations-bcast"),
        0,
        "publishing to a topic with no subscribers creates no state"
    );
}

/// AC2 — `Outcome::Retry` discards instead of redelivering. The retry budget is
/// pinned to zero for a broadcast subscription, so the first `Retry` lands on
/// the existing terminal (no-DLQ) arm rather than re-entering the loop.
///
/// The redelivery this pins out is the one that would otherwise be *invisible*:
/// with a non-zero budget the handler would simply be called again, and the
/// fan-out would quietly deliver one message twice to one subscriber.
#[tokio::test]
async fn retry_discards_rather_than_redelivering() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    let publisher = broker.publisher().await.expect("publisher");

    let calls = Arc::new(Mutex::new(0u32));
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(
            AlwaysRetry {
                calls: Arc::clone(&calls),
            },
            // Explicitly ask for retries: broadcast must override this, not
            // honour it. Without the pin, this is 5 extra deliveries.
            ConsumerOptions::new().with_max_retries(5),
        )
        .expect("subscribe");
    wait_for_subscribers(&client, "cache-invalidations-bcast", 1).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 1 })
        .await
        .expect("publish");
    settle().await;

    subscriber.cancellation_token().cancel();
    let _ = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;

    assert_eq!(
        *calls.lock().expect("calls lock"),
        1,
        "Retry is terminal on a broadcast subscription — the handler runs once"
    );
}

/// AC2 — `Outcome::Reject` likewise discards. There is no DLQ to route to,
/// because a broadcast topology cannot declare one.
#[tokio::test]
async fn reject_discards_with_no_dlq_to_route_to() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    let publisher = broker.publisher().await.expect("publisher");

    assert_eq!(
        <CacheInvalidations as Topic>::topology().dlq(),
        None,
        "a broadcast topology has no DLQ by construction"
    );

    let calls = Arc::new(Mutex::new(0u32));
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(
            AlwaysReject {
                calls: Arc::clone(&calls),
            },
            ConsumerOptions::new(),
        )
        .expect("subscribe");
    wait_for_subscribers(&client, "cache-invalidations-bcast", 1).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 1 })
        .await
        .expect("publish");
    settle().await;

    subscriber.cancellation_token().cancel();
    let _ = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;

    assert_eq!(
        *calls.lock().expect("calls lock"),
        1,
        "Reject retires the message here and now"
    );
}

/// AC3 — a broadcast topology cannot reach an entry point that *has* an
/// autoscaling knob. `BroadcastSubscriber` has no such method at all (a
/// compile-level guarantee); this pins the other half, that the scalable paths
/// refuse the topology rather than silently attaching a competing consumer to a
/// queue publishers never write to.
#[tokio::test]
async fn scalable_entry_points_refuse_a_broadcast_topology() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());

    let mut group = broker.consumer_group();
    let err = group
        .register::<CacheInvalidations, _>(
            ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=4)),
            Recorder::new,
        )
        .await
        .expect_err("consumer_group must refuse a broadcast topology");
    match err {
        ShoveError::Topology(msg) => assert!(
            msg.contains("broadcast") && msg.contains("broadcast_subscriber"),
            "the error names the conflict and the right entry point: {msg}"
        ),
        other => panic!("expected a Topology error, got {other:?}"),
    }

    let mut supervisor = broker.consumer_supervisor();
    let err = supervisor
        .register::<CacheInvalidations, _>(Recorder::new(), ConsumerOptions::new())
        .expect_err("consumer_supervisor must refuse a broadcast topology");
    assert!(
        matches!(err, ShoveError::Topology(ref m) if m.contains("broadcast_subscriber")),
        "expected a Topology error pointing at broadcast_subscriber, got {err:?}"
    );

    let _ = group
        .run_until_timeout(std::future::ready(()), Duration::from_millis(100))
        .await;
    let _ = supervisor
        .run_until_timeout(std::future::ready(()), Duration::from_millis(100))
        .await;
}

/// The mirror image: an ordinary topology is refused by the broadcast entry
/// point. Accepting it would attach a subscriber to a topic whose publishes all
/// go to the shared queue, so it would sit there receiving nothing.
#[tokio::test]
async fn broadcast_subscriber_refuses_an_ordinary_topology() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());

    let mut subscriber = broker.broadcast_subscriber();
    let err = subscriber
        .subscribe::<PlainOrders, _>(Recorder::new(), ConsumerOptions::new())
        .expect_err("a non-broadcast topology must be refused");
    assert!(
        matches!(err, ShoveError::Topology(ref m) if m.contains("not a broadcast topology")),
        "got {err:?}"
    );
    assert_eq!(
        client.broadcast_subscriber_count("plain-orders-bcast"),
        0,
        "a refused subscribe registers nothing"
    );
}

/// One subscription per topic per handle. A second would split this instance's
/// fan-out across two loops — competing consumption inside the one process that
/// was supposed to receive everything.
#[tokio::test]
async fn a_second_subscription_to_the_same_topic_is_refused() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());

    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(Recorder::new(), ConsumerOptions::new())
        .expect("first subscribe");
    let err = subscriber
        .subscribe::<CacheInvalidations, _>(Recorder::new(), ConsumerOptions::new())
        .expect_err("second subscribe to the same topic must be refused");
    assert!(
        matches!(err, ShoveError::Topology(ref m) if m.contains("already subscribed")),
        "got {err:?}"
    );

    subscriber.cancellation_token().cancel();
    let _ = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
}

/// AC8 at the wire level rather than the name level: an ordinary topology still
/// behaves exactly as it did, on the same broker, alongside broadcast traffic.
#[tokio::test]
async fn an_ordinary_topology_is_unaffected() {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<PlainOrders>()
        .await
        .expect("declare");
    let publisher = broker.publisher().await.expect("publisher");

    let recorder = Recorder::new();
    let mut supervisor = broker.consumer_supervisor();
    supervisor
        .register::<PlainOrders, _>(recorder.clone(), ConsumerOptions::new())
        .expect("register");

    publisher
        .publish::<PlainOrders>(&Invalidate { key: 42 })
        .await
        .expect("publish");
    settle().await;

    supervisor.cancellation_token().cancel();
    let outcome = supervisor
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(2))
        .await;
    assert!(outcome.is_clean(), "{outcome:?}");
    assert_eq!(recorder.keys(), vec![42]);
    assert_eq!(
        client.broadcast_subscriber_count("plain-orders-bcast"),
        0,
        "an ordinary topology creates no broadcast state"
    );
}
