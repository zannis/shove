//! Broadcast on Redis Streams: every instance reads the stream with a plain
//! `XREAD` from `$`, and leaves no consumer group behind.
//!
//! The property a broker can contradict here is the *absence* of state. The
//! obvious implementation — a consumer group per pod — passes a fan-out test
//! and then leaks an `XGROUP` entry on every restart, with nothing to reap it.
//! So these tests assert `XINFO GROUPS` is empty as directly as they assert the
//! fan-out itself.
//!
//! Requires Docker.

#![cfg(feature = "redis-streams")]

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};

use shove::redis::{RedisConfig, RedisMode};
use shove::topology::TopologyBuilder;
use shove::{
    Broker, ConsumerOptions, MessageHandler, MessageMetadata, Outcome, Redis, define_topic,
};

const STREAM: &str = "redis-bcast-invalidations";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invalidate {
    key: u64,
}

define_topic!(
    CacheInvalidations,
    Invalidate,
    TopologyBuilder::new("redis-bcast-invalidations")
        .broadcast()
        .build()
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

    /// Keys seen, minus the readiness probes — see [`warm_up`].
    fn keys(&self) -> Vec<u64> {
        self.seen
            .lock()
            .expect("seen lock")
            .iter()
            .copied()
            .filter(|k| *k != WARMUP_KEY)
            .collect()
    }

    fn saw_warmup(&self) -> bool {
        self.seen.lock().expect("seen lock").contains(&WARMUP_KEY)
    }
}

impl MessageHandler<CacheInvalidations> for Recorder {
    type Context = ();
    async fn handle(&self, msg: Invalidate, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        self.seen.lock().expect("seen lock").push(msg.key);
        Outcome::Ack
    }
}

/// Reserved key used only to detect that a subscriber's `XREAD` is live.
const WARMUP_KEY: u64 = 0;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct TestBroker {
    _container: testcontainers::ContainerAsync<RedisContainer>,
    url: String,
}

impl TestBroker {
    async fn start() -> Self {
        let container = RedisContainer::default()
            .with_tag("7.0")
            .start()
            .await
            .expect("failed to start Redis container");
        let host = container.get_host().await.expect("host");
        let port = container
            .get_host_port_ipv4(REDIS_PORT)
            .await
            .expect("port");
        Self {
            _container: container,
            url: format!("redis://{host}:{port}/"),
        }
    }

    async fn broker(&self) -> Broker<Redis> {
        for _ in 0..60 {
            let config = RedisConfig::new(RedisMode::Standalone {
                url: self.url.clone(),
            });
            if let Ok(b) = Broker::<Redis>::new(config).await {
                return b;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("Redis never became reachable at {}", self.url);
    }

    async fn raw(&self) -> MultiplexedConnection {
        redis::Client::open(self.url.as_str())
            .expect("raw client")
            .get_multiplexed_async_connection()
            .await
            .expect("raw connection")
    }

    /// `XINFO GROUPS` on the broadcast stream. Empty is the AC6 assertion.
    ///
    /// `XINFO GROUPS` errors when the key does not exist, which is itself a
    /// pass — no stream means no group — so that case maps to empty.
    async fn groups(&self) -> Vec<redis::Value> {
        let mut conn = self.raw().await;
        redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(STREAM)
            .query_async::<Vec<redis::Value>>(&mut conn)
            .await
            .unwrap_or_default()
    }
}

/// Publish readiness probes until every recorder has seen one.
///
/// Redis keeps no registry of `XREAD` readers — that absence is the feature —
/// so unlike NATS there is nothing server-side to poll for "is the subscriber
/// attached yet". Probing with a real publish is the only honest signal, and it
/// does not weaken the deliver-new assertions: [`Recorder::keys`] filters
/// [`WARMUP_KEY`] out, and the assertions below still publish their subjects
/// strictly after the probe has landed.
async fn warm_up(publisher: &shove::Publisher<Redis>, recorders: &[&Recorder]) {
    for _ in 0..200 {
        if recorders.iter().all(|r| r.saw_warmup()) {
            // Let any probe still in flight land before the caller publishes
            // the messages it means to assert on.
            tokio::time::sleep(Duration::from_millis(200)).await;
            return;
        }
        publisher
            .publish::<CacheInvalidations>(&Invalidate { key: WARMUP_KEY })
            .await
            .expect("publish warmup");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("subscribers never became live");
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(500)).await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The precondition, asserted directly: declaring a broadcast topology creates
/// no consumer group. The competing-consumer path runs `XGROUP CREATE … MKSTREAM`
/// for every topology, and a group created here would be a permanent orphan —
/// never read from, and never cleaned up, because a broadcast subscription
/// never holds a handle on it.
#[tokio::test]
async fn declaring_a_broadcast_topology_creates_no_consumer_group() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("declare");

    assert!(
        tb.groups().await.is_empty(),
        "declaring a broadcast topology must not create an XGROUP"
    );
}

/// AC5 — two subscribers each receive **every** message. A consumer group would
/// split them; a bare `XREAD` per instance does not.
#[tokio::test]
async fn two_subscribers_each_receive_every_message() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("declare");
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

    warm_up(&publisher, &[&first, &second]).await;

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
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;
    let outcome_b = sub_b
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;
    assert!(
        outcome_a.is_clean(),
        "subscriber a drained cleanly: {outcome_a:?}"
    );
    assert!(
        outcome_b.is_clean(),
        "subscriber b drained cleanly: {outcome_b:?}"
    );

    let mut a = first.keys();
    let mut b = second.keys();
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, vec![1, 2, 3, 4, 5], "subscriber a saw every message");
    assert_eq!(b, vec![1, 2, 3, 4, 5], "subscriber b saw every message");
}

/// AC4 — deliver-new. `$` means "entries added after this call", so an entry
/// already in the stream is never handed to a subscriber that arrives later.
#[tokio::test]
async fn a_subscriber_that_starts_late_misses_earlier_messages() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("declare");
    let publisher = broker.publisher().await.expect("publisher");

    // In the stream before anyone subscribes. Unlike NATS, Redis *retains*
    // this — which is exactly why deliver-new has to be enforced by the read
    // position rather than by retention.
    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 100 })
        .await
        .expect("publish before anyone subscribes");

    let early = Recorder::new();
    let mut sub_early = broker.broadcast_subscriber();
    sub_early
        .subscribe::<CacheInvalidations, _>(early.clone(), ConsumerOptions::new())
        .expect("subscribe early");
    warm_up(&publisher, &[&early]).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 200 })
        .await
        .expect("publish with one subscriber");
    settle().await;

    let late = Recorder::new();
    let mut sub_late = broker.broadcast_subscriber();
    sub_late
        .subscribe::<CacheInvalidations, _>(late.clone(), ConsumerOptions::new())
        .expect("subscribe late");
    warm_up(&publisher, &[&late]).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 300 })
        .await
        .expect("publish with two subscribers");
    settle().await;

    sub_early.cancellation_token().cancel();
    sub_late.cancellation_token().cancel();
    let _ = sub_early
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;
    let _ = sub_late
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;

    assert_eq!(
        early.keys(),
        vec![200, 300],
        "the early subscriber saw everything published after it subscribed, and not the \
         entry already sitting in the stream"
    );
    assert_eq!(
        late.keys(),
        vec![300],
        "the late subscriber received only what arrived after it did — no replay, even \
         though 100 and 200 are still in the stream"
    );
}

/// AC6 — nothing survives teardown. There is no `XGROUP`, so there is no PEL
/// and no consumer registry: not merely cleaned up, never created.
#[tokio::test]
async fn no_consumer_group_survives_the_subscription() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("declare");
    let publisher = broker.publisher().await.expect("publisher");

    let recorder = Recorder::new();
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(recorder.clone(), ConsumerOptions::new())
        .expect("subscribe");
    warm_up(&publisher, &[&recorder]).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 1 })
        .await
        .expect("publish");
    settle().await;
    assert_eq!(recorder.keys(), vec![1], "the subscriber was actually live");

    // While the subscription is *running* — the window in which a per-pod
    // consumer group would exist.
    assert!(
        tb.groups().await.is_empty(),
        "a live broadcast subscription must not have created an XGROUP"
    );

    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean(), "drained cleanly: {outcome:?}");

    assert!(
        tb.groups().await.is_empty(),
        "and none after teardown either"
    );

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 2 })
        .await
        .expect("publish after teardown");
    settle().await;
    assert_eq!(
        recorder.keys(),
        vec![1],
        "the torn-down subscriber received nothing after teardown"
    );
}

// ---------------------------------------------------------------------------
// Reconnect
// ---------------------------------------------------------------------------

fn find_free_port() -> u16 {
    // Bind to port 0 to let the OS allocate a free ephemeral port, then
    // immediately drop the listener so the port is available for Docker.
    // There is a small TOCTOU window, but it is negligible for test use.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind for free port");
    listener.local_addr().expect("local addr").port()
}

/// A broadcast subscription survives a Redis outage.
///
/// This is the property that made reconnecting the right default rather than a
/// nicety. `BroadcastSubscriber` only reports a task's error through
/// `SupervisorOutcome`, and only when `run_until_timeout` returns — i.e. at
/// shutdown. So a subscription that resolved on the first broker blip would go
/// on looking healthy while silently receiving nothing for the rest of the
/// process's life, and a cache-invalidation reader would serve stale data
/// indefinitely. A delivery gap is within the broadcast contract; that is not.
#[tokio::test]
async fn a_subscription_survives_a_redis_restart() {
    // Own container on a fixed host port, so the subscriber's URL stays valid
    // across the restart.
    let host_port: u16 = find_free_port();
    let container = RedisContainer::default()
        .with_tag("7.0")
        .with_mapped_port(host_port, ContainerPort::Tcp(REDIS_PORT))
        .start()
        .await
        .expect("start Redis container");
    let host = container.get_host().await.expect("get host");
    let tb = TestBroker {
        _container: container,
        url: format!("redis://{host}:{host_port}/"),
    };

    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("declare");
    let publisher = broker.publisher().await.expect("publisher");

    let recorder = Recorder::new();
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(recorder.clone(), ConsumerOptions::new())
        .expect("subscribe");
    warm_up(&publisher, &[&recorder]).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 1 })
        .await
        .expect("publish before the outage");
    settle().await;
    assert_eq!(recorder.keys(), vec![1], "live before the outage");

    // --- Redis outage ---
    let container_id = tb._container.id().to_string();
    let status = std::process::Command::new("docker")
        .args(["kill", &container_id])
        .status()
        .expect("docker kill");
    assert!(status.success(), "docker kill failed");

    // Let the subscription notice the disconnect and enter its backoff.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let status = std::process::Command::new("docker")
        .args(["start", &container_id])
        .status()
        .expect("docker start");
    assert!(status.success(), "docker start failed");

    // A restarted container has lost the stream (no persistence configured), so
    // publish through a fresh broker. Redis stream ids are time-based, so the
    // new entry sorts after the cursor the subscription is holding and is
    // therefore delivered rather than skipped.
    let broker2 = tb.broker().await;
    let publisher2 = broker2.publisher().await.expect("publisher2");
    publisher2
        .publish::<CacheInvalidations>(&Invalidate { key: 2 })
        .await
        .expect("publish after the outage");

    let probe = recorder.clone();
    let recovered = {
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        loop {
            if probe.keys().contains(&2) {
                break true;
            }
            if std::time::Instant::now() > deadline {
                break false;
            }
            // Keep publishing: the subscription may still be in backoff, and a
            // broadcast subscriber that reconnects after this message was sent
            // is *expected* to have missed it — the gap is in contract, the
            // permanent death is not.
            let _ = publisher2
                .publish::<CacheInvalidations>(&Invalidate { key: 2 })
                .await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    assert!(
        recovered,
        "the broadcast subscription never recovered from the Redis restart; saw {:?}",
        recorder.keys()
    );

    subscriber.cancellation_token().cancel();
    let _ = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;

    assert!(
        tb.groups().await.is_empty(),
        "reconnecting must not have created an XGROUP"
    );
}
