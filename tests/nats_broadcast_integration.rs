//! Broadcast on NATS JetStream: every instance gets its own **ephemeral pull
//! consumer**, and nothing survives it going away.
//!
//! The properties under test are the ones a broker can contradict and the
//! in-process substrate cannot: that JetStream accepts the ephemeral
//! `AckPolicy::None` / `DeliverPolicy::New` consumer at all (it does not, on
//! shove's default WorkQueue retention), that two of them each receive every
//! message rather than splitting it, and that no consumer is left on the stream
//! afterwards.
//!
//! Requires Docker.

#![cfg(feature = "nats")]

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_nats::jetstream::stream::RetentionPolicy;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};

use shove::nats::{NatsClient, NatsConfig};
use shove::topology::TopologyBuilder;
use shove::{
    Broker, ConsumerOptions, MessageHandler, MessageMetadata, Nats, NatsRetention,
    NatsStreamConfig, Outcome, define_topic,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invalidate {
    key: u64,
}

define_topic!(
    CacheInvalidations,
    Invalidate,
    TopologyBuilder::new("nats-bcast-invalidations")
        .broadcast()
        .build()
);

// A broadcast topology whose stream config pins the one retention that cannot
// serve broadcast. Declaring it must fail loudly rather than leave the failure
// to surface later as an opaque consumer-create error.
define_topic!(
    WorkQueuePinned,
    Invalidate,
    TopologyBuilder::new("nats-bcast-workqueue-pinned")
        .broadcast()
        .nats_stream_config(NatsStreamConfig {
            retention: NatsRetention::WorkQueue,
            max_age: None,
            max_bytes: None,
            max_messages: None,
            num_replicas: 1,
        })
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
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(4222).await.expect("port");
        let client =
            NatsClient::connect_with_retry(&NatsConfig::new(format!("nats://{host}:{port}")), 10)
                .await
                .expect("connect to NATS");
        Self {
            _container: container,
            client,
        }
    }

    fn broker(&self) -> Broker<Nats> {
        Broker::<Nats>::from_client(self.client.clone())
    }

    /// Consumers currently on `stream`. Empty is the AC6 assertion: an
    /// ephemeral consumer is *not* removed by dropping the client handle, so an
    /// empty list here means the subscription really did delete its own.
    async fn consumers_on(&self, stream: &str) -> Vec<String> {
        let stream = self
            .client
            .jetstream()
            .get_stream(stream)
            .await
            .expect("get stream");
        let mut names = stream.consumer_names();
        let mut live = Vec::new();
        while let Some(Ok(name)) = names.next().await {
            live.push(name);
        }
        live
    }

    /// Block until `stream` has exactly `n` consumers.
    ///
    /// The ephemeral consumer is created by the spawned delivery task, not by
    /// `subscribe()` — which is what makes deliver-new structural. Publishing
    /// the instant `subscribe()` returns would race that creation and be flaky
    /// for a reason unrelated to the property under test.
    async fn wait_for_consumers(&self, stream: &str, n: usize) {
        for _ in 0..600 {
            if self.consumers_on(stream).await.len() == n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "timed out waiting for {n} consumer(s) on '{stream}'; have {:?}",
            self.consumers_on(stream).await
        );
    }
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(500)).await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The precondition every other test here depends on, asserted directly rather
/// than inferred: shove declares a `.broadcast()` topology with `Interest`
/// retention, not the crate default `WorkQueue`. On WorkQueue, JetStream
/// rejects `AckPolicy::None` ("consumer in pull mode requires ack policy") and
/// `DeliverPolicy::New` ("consumer must be deliver all on workqueue stream"),
/// so broadcast would not work at all.
#[tokio::test]
async fn broadcast_stream_is_declared_with_interest_retention() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("declare");

    let info = tb
        .client
        .jetstream()
        .get_stream("nats-bcast-invalidations")
        .await
        .expect("get stream")
        .info()
        .await
        .expect("stream info")
        .config
        .retention;

    assert_eq!(
        info,
        RetentionPolicy::Interest,
        "a broadcast topology must be declared Interest — WorkQueue cannot serve an \
         ephemeral AckPolicy::None / DeliverPolicy::New consumer"
    );
}

/// Pinning a broadcast topology to WorkQueue is refused at declare time, with a
/// message that names the conflict — not left to fail later as an opaque
/// consumer-create error from the delivery loop.
#[tokio::test]
async fn broadcast_pinned_to_workqueue_is_refused_at_declare() {
    let tb = TestBroker::start().await;
    let err = tb
        .broker()
        .topology()
        .declare::<WorkQueuePinned>()
        .await
        .expect_err("declaring broadcast + WorkQueue must fail");

    let msg = err.to_string();
    assert!(
        msg.contains("WorkQueue") && msg.contains("broadcast"),
        "the error names the conflicting pair; got: {msg}"
    );
    assert!(
        msg.contains("Interest"),
        "the error points at the retention that does work; got: {msg}"
    );
}

/// AC5 — two subscribers each receive **every** message, rather than splitting
/// them. This is the whole point of the feature, and the property a consumer
/// group would violate.
#[tokio::test]
async fn two_subscribers_each_receive_every_message() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
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

    tb.wait_for_consumers("nats-bcast-invalidations", 2).await;

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

/// AC4 — deliver-new. A subscriber that starts late does not receive what it
/// missed; there is no replay for an instance that was down.
#[tokio::test]
async fn a_subscriber_that_starts_late_misses_earlier_messages() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("declare");
    let publisher = broker.publisher().await.expect("publisher");

    // Published with nobody subscribed. Under Interest retention this is not
    // even retained, let alone replayed.
    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 100 })
        .await
        .expect("publish before anyone subscribes");

    let early = Recorder::new();
    let mut sub_early = broker.broadcast_subscriber();
    sub_early
        .subscribe::<CacheInvalidations, _>(early.clone(), ConsumerOptions::new())
        .expect("subscribe early");
    tb.wait_for_consumers("nats-bcast-invalidations", 1).await;

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
    tb.wait_for_consumers("nats-bcast-invalidations", 2).await;

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
         one published before"
    );
    assert_eq!(
        late.keys(),
        vec![300],
        "the late subscriber received only what arrived after it did — no replay"
    );
}

/// AC6 — no durable consumer survives teardown.
///
/// This is the assertion that would silently pass for the wrong reason if
/// teardown were left to the server: an ephemeral consumer is **not** removed
/// when the client handle drops (verified: it stays listed on the stream), and
/// its `inactive_threshold` is 30s. So an empty consumer list within a second
/// of the drain means the subscription deleted its own consumer.
#[tokio::test]
async fn no_consumer_survives_a_clean_teardown() {
    let tb = TestBroker::start().await;
    let broker = tb.broker();
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
    tb.wait_for_consumers("nats-bcast-invalidations", 1).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 1 })
        .await
        .expect("publish");
    settle().await;
    assert_eq!(recorder.keys(), vec![1], "the subscriber was actually live");

    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean(), "drained cleanly: {outcome:?}");

    let live = tb.consumers_on("nats-bcast-invalidations").await;
    assert!(
        live.is_empty(),
        "the ephemeral consumer must be deleted by teardown, not left for the server's \
         30s inactive_threshold; still on the stream: {live:?}"
    );

    // And nothing accumulates: Interest retention with an `AckPolicy::None`
    // consumer drops delivered messages, and a publish with no subscribers at
    // all is retained by nobody.
    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 2 })
        .await
        .expect("publish after teardown");
    settle().await;
    let messages = tb
        .client
        .jetstream()
        .get_stream("nats-bcast-invalidations")
        .await
        .expect("get stream")
        .info()
        .await
        .expect("info")
        .state
        .messages;
    assert_eq!(
        messages, 0,
        "a broadcast stream retains nothing — neither delivered messages nor ones \
         published with no subscriber listening"
    );
    assert_eq!(
        recorder.keys(),
        vec![1],
        "and the torn-down subscriber received nothing after teardown"
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

/// A broadcast subscription survives a NATS outage, and leaves exactly one
/// ephemeral consumer behind while doing so.
///
/// Reconnecting is the right default here for a reason specific to
/// `BroadcastSubscriber`: it only reports a task's error through
/// `SupervisorOutcome`, and only when `run_until_timeout` returns — i.e. at
/// shutdown. A subscription that resolved on the first blip would look healthy
/// while receiving nothing for the rest of the process's life.
///
/// The consumer-count assertion is the other half. Each reconnect attempt
/// creates a *new* ephemeral consumer, and an ephemeral consumer is not removed
/// by the client disconnecting — so a reconnect loop that failed to delete its
/// previous consumer would accumulate one per attempt on the stream.
#[tokio::test]
async fn a_subscription_survives_a_nats_restart() {
    let host_port: u16 = find_free_port();
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = NatsContainer::default()
        .with_cmd(&cmd)
        .with_mapped_port(host_port, ContainerPort::Tcp(4222))
        .start()
        .await
        .expect("start NATS container");
    let host = container.get_host().await.expect("get host");
    let client =
        NatsClient::connect_with_retry(&NatsConfig::new(format!("nats://{host}:{host_port}")), 30)
            .await
            .expect("connect to NATS");
    let tb = TestBroker {
        _container: container,
        client,
    };

    let broker = tb.broker();
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
    tb.wait_for_consumers("nats-bcast-invalidations", 1).await;

    publisher
        .publish::<CacheInvalidations>(&Invalidate { key: 1 })
        .await
        .expect("publish before the outage");
    settle().await;
    assert_eq!(recorder.keys(), vec![1], "live before the outage");

    // --- NATS outage ---
    let container_id = tb._container.id().to_string();
    let status = std::process::Command::new("docker")
        .args(["kill", &container_id])
        .status()
        .expect("docker kill");
    assert!(status.success(), "docker kill failed");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let status = std::process::Command::new("docker")
        .args(["start", &container_id])
        .status()
        .expect("docker start");
    assert!(status.success(), "docker start failed");

    // The stream survives (JetStream file storage on the container's own
    // filesystem); the ephemeral consumer does not, so the subscription has to
    // create a fresh one. Publishing repeatedly because a reconnect that lands
    // after a given publish is *expected* to have missed it — the delivery gap
    // is in contract, the permanent death is not.
    let broker2 = Broker::<Nats>::from_client(
        NatsClient::connect_with_retry(&NatsConfig::new(format!("nats://{host}:{host_port}")), 60)
            .await
            .expect("reconnect publisher client"),
    );
    broker2
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("redeclare");
    let publisher2 = broker2.publisher().await.expect("publisher2");

    let recovered = {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if recorder.keys().contains(&2) {
                break true;
            }
            if std::time::Instant::now() > deadline {
                break false;
            }
            let _ = publisher2
                .publish::<CacheInvalidations>(&Invalidate { key: 2 })
                .await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    assert!(
        recovered,
        "the broadcast subscription never recovered from the NATS restart; saw {:?}",
        recorder.keys()
    );

    // One subscriber, so exactly one consumer — a reconnect loop that leaked
    // its previous ephemeral consumer would show several here.
    let live = tb.consumers_on("nats-bcast-invalidations").await;
    assert_eq!(
        live.len(),
        1,
        "reconnecting must delete the previous ephemeral consumer rather than \
         accumulate one per attempt; found {live:?}"
    );

    subscriber.cancellation_token().cancel();
    let _ = subscriber
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;

    let live = tb.consumers_on("nats-bcast-invalidations").await;
    assert!(
        live.is_empty(),
        "and teardown after a reconnect still deletes the live consumer; found {live:?}"
    );
}
