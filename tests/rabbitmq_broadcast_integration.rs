#![cfg(feature = "rabbitmq")]

//! Integration tests for RabbitMQ ephemeral per-instance broadcast
//! subscriptions.
//!
//! The two properties worth a real broker are the ones no unit test can reach:
//!
//! - **AC5 — nothing survives.** Every instance declares its own exclusive,
//!   auto-delete, server-named queue. When the subscriber goes away the queue
//!   must be gone from the broker, whether it drained cleanly or was aborted
//!   mid-run by a drain timeout. This is asserted against the management API's
//!   queue list, not inferred from the declaration flags.
//! - **The publisher route changes.** A broadcast publish goes to the
//!   `{queue}-fanout` exchange, not to the default exchange keyed by the queue
//!   name. Two subscribers each receiving every message is what proves it, and
//!   the absence of a `{queue}` queue is what proves the old route now leads
//!   nowhere — which is the deployment caveat in
//!   `docs/pages/concepts/broadcast.mdx` stated as a test.
//!
//! Run with:
//! `cargo nextest run --features rabbitmq --test rabbitmq_broadcast_integration`

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::RabbitMq as RabbitMqMarker;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::rabbitmq::{RabbitMqClient, RabbitMqConfig};
use shove::{TopologyBuilder, define_topic};

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio::sync::Mutex;
use tokio::time::Instant;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// Broker connection for this test.
///
/// Shared mode (the nextest `rabbitmq-setup` script exported
/// `RABBITMQ_AMQP_URL`) carves out a private vhost. That isolation matters more
/// here than in most files: the AC5 assertion is "the vhost contains no queue
/// bound to this exchange", which another test's leftovers would falsify.
struct TestContext {
    amqp_url: String,
    mgmt_url: String,
    vhost: Option<String>,
    _container: Option<testcontainers::ContainerAsync<RabbitMq>>,
}

impl TestContext {
    async fn new() -> Self {
        let http = reqwest::Client::new();

        if let Ok(base_amqp) = std::env::var("RABBITMQ_AMQP_URL") {
            let mgmt_url = std::env::var("RABBITMQ_MGMT_URL")
                .expect("RABBITMQ_MGMT_URL must be set when RABBITMQ_AMQP_URL is set");
            let vhost = format!("test-{}", uuid::Uuid::new_v4());

            let status = http
                .put(format!("{mgmt_url}/api/vhosts/{vhost}"))
                .basic_auth("guest", Some("guest"))
                .header("content-type", "application/json")
                .body("{}")
                .send()
                .await
                .expect("failed to create vhost")
                .status();
            assert!(status.is_success(), "create vhost returned {status}");

            let status = http
                .put(format!("{mgmt_url}/api/permissions/{vhost}/guest"))
                .basic_auth("guest", Some("guest"))
                .json(&std::collections::HashMap::from([
                    ("configure", ".*"),
                    ("write", ".*"),
                    ("read", ".*"),
                ]))
                .send()
                .await
                .expect("failed to set vhost permissions")
                .status();
            assert!(status.is_success(), "set permissions returned {status}");

            Self {
                amqp_url: format!("{base_amqp}/{vhost}"),
                mgmt_url,
                vhost: Some(vhost),
                _container: None,
            }
        } else {
            // Standalone mode. No consistent-hash plugin is enabled here:
            // `.broadcast()` cannot be combined with `.sequenced()`, so nothing
            // in this file needs it.
            let container = RabbitMq::default()
                .start()
                .await
                .expect("failed to start RabbitMQ container");

            let host = container.get_host().await.expect("failed to get host");
            let amqp_port = container
                .get_host_port_ipv4(5672)
                .await
                .expect("failed to get AMQP port");
            let mgmt_port = container
                .get_host_port_ipv4(15672)
                .await
                .expect("failed to get mgmt port");

            // The management API is not up the instant the port binds.
            tokio::time::sleep(Duration::from_secs(3)).await;

            Self {
                amqp_url: format!("amqp://guest:guest@{host}:{amqp_port}"),
                mgmt_url: format!("http://{host}:{mgmt_port}"),
                vhost: None,
                _container: Some(container),
            }
        }
    }

    fn rmq_config(&self) -> RabbitMqConfig {
        RabbitMqConfig::new(self.amqp_url.clone())
    }

    /// A broker on its own client.
    ///
    /// One client per subscriber, always: `run_until_timeout` cancels the
    /// *client's* shutdown token, so a client that has hosted one subscription
    /// is spent. Separate clients are also the faithful model of what this file
    /// is about — separate processes, each with its own connection.
    async fn broker(&self) -> Broker<RabbitMqMarker> {
        let client = RabbitMqClient::connect(&self.rmq_config())
            .await
            .expect("failed to connect to RabbitMQ");
        Broker::<RabbitMqMarker>::from_client(client)
    }

    fn vhost_path(&self) -> String {
        match self.vhost.as_deref() {
            Some(v) => v.to_string(),
            None => "%2F".to_string(),
        }
    }

    /// Every queue name currently declared in this test's vhost.
    async fn queue_names(&self) -> Vec<String> {
        let body: serde_json::Value = reqwest::Client::new()
            .get(format!(
                "{}/api/queues/{}",
                self.mgmt_url,
                self.vhost_path()
            ))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .expect("failed to list queues")
            .json()
            .await
            .expect("queue list was not JSON");
        body.as_array()
            .expect("queue list was not an array")
            .iter()
            .filter_map(|q| q.get("name").and_then(|n| n.as_str()).map(String::from))
            .collect()
    }

    /// Poll the queue list until it is empty, or `timeout` elapses.
    ///
    /// Queue deletion is asynchronous on the broker's side: the consumer cancel
    /// and channel close that trigger it are issued without a round trip, so a
    /// bare read immediately after shutdown can still see the queue. Polling
    /// distinguishes "deleted a moment later" from "never deleted", which is
    /// the distinction AC5 is actually about.
    async fn wait_for_no_queues(&self, timeout: Duration) -> Vec<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let names = self.queue_names().await;
            if names.is_empty() || Instant::now() >= deadline {
                return names;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// True if `exchange` exists in this test's vhost.
    async fn exchange_exists(&self, exchange: &str) -> bool {
        reqwest::Client::new()
            .get(format!(
                "{}/api/exchanges/{}/{exchange}",
                self.mgmt_url,
                self.vhost_path()
            ))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .expect("failed to query exchange")
            .status()
            .is_success()
    }

    async fn cleanup(&self) {
        let Some(vhost) = self.vhost.as_deref() else {
            return;
        };
        reqwest::Client::new()
            .delete(format!("{}/api/vhosts/{vhost}", self.mgmt_url))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .ok();
    }
}

// ---------------------------------------------------------------------------
// Topic and handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct Invalidate {
    key: String,
}

define_topic!(
    CacheInvalidations,
    Invalidate,
    TopologyBuilder::new("rmq-broadcast-invalidations")
        .broadcast()
        .build()
);

define_topic!(
    DeferTopic,
    Invalidate,
    TopologyBuilder::new("rmq-broadcast-defer")
        .broadcast()
        .build()
);

define_topic!(
    RetryTopic,
    Invalidate,
    TopologyBuilder::new("rmq-broadcast-retry")
        .broadcast()
        .build()
);

/// Records every key it sees and acks.
#[derive(Clone, Default)]
struct Recorder {
    seen: Arc<Mutex<Vec<String>>>,
}

impl Recorder {
    async fn keys(&self) -> Vec<String> {
        self.seen.lock().await.clone()
    }

    async fn wait_for(&self, target: usize, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while self.seen.lock().await.len() < target && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

macro_rules! recorder_for {
    ($($topic:ty),+ $(,)?) => {$(
        impl MessageHandler<$topic> for Recorder {
            type Context = ();
            async fn handle(&self, msg: Invalidate, _meta: MessageMetadata, _: &()) -> Outcome {
                self.seen.lock().await.push(msg.key);
                Outcome::Ack
            }
        }
    )+};
}

recorder_for!(CacheInvalidations, DeferTopic);

/// Always returns `Retry`. Counts calls, so a redelivery loop is visible as a
/// count above one rather than as a hang.
#[derive(Clone, Default)]
struct AlwaysRetry {
    calls: Arc<Mutex<Vec<String>>>,
}

impl MessageHandler<RetryTopic> for AlwaysRetry {
    type Context = ();
    async fn handle(&self, msg: Invalidate, _meta: MessageMetadata, _: &()) -> Outcome {
        self.calls.lock().await.push(msg.key);
        Outcome::Retry
    }
}

/// Defers the first delivery of each key and acks the redelivery, so the count
/// of handler calls tells `Defer` redelivered from a count of calls that says
/// it did not.
#[derive(Clone, Default)]
struct DeferOnce {
    calls: Arc<Mutex<Vec<String>>>,
}

impl DeferOnce {
    async fn calls(&self) -> Vec<String> {
        self.calls.lock().await.clone()
    }
}

impl MessageHandler<DeferTopic> for DeferOnce {
    type Context = ();
    async fn handle(&self, msg: Invalidate, _meta: MessageMetadata, _: &()) -> Outcome {
        let mut calls = self.calls.lock().await;
        let seen_before = calls.iter().filter(|k| **k == msg.key).count();
        calls.push(msg.key);
        if seen_before == 0 {
            Outcome::Defer
        } else {
            Outcome::Ack
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Every instance receives every message, the fanout exchange is what carries
/// them, and — AC5 — nothing is left on the broker once both instances stop.
#[tokio::test]
async fn broadcast_fans_out_and_leaves_no_queue_behind() {
    let ctx = TestContext::new().await;

    let publisher_broker = ctx.broker().await;
    publisher_broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("failed to declare broadcast topology");

    // The topology declares the exchange and no queue at all. A queue here
    // would be the addressable, permanently empty residue AC5 rules out — and
    // would also make the "old publisher route leads nowhere" caveat false.
    assert!(
        ctx.exchange_exists("rmq-broadcast-invalidations-fanout")
            .await,
        "declare_topology must create the fanout exchange"
    );
    assert_eq!(
        ctx.queue_names().await,
        Vec::<String>::new(),
        "a broadcast topology must declare no queue, not even the topic's own"
    );

    // Two instances, each on its own connection.
    let mut subscribers = Vec::new();
    let mut recorders = Vec::new();
    for _ in 0..2 {
        let broker = ctx.broker().await;
        let recorder = Recorder::default();
        let mut sub = broker.broadcast_subscriber();
        sub.subscribe::<CacheInvalidations, _>(recorder.clone(), ConsumerOptions::new())
            .expect("failed to subscribe");
        recorders.push(recorder);
        subscribers.push((broker, sub));
    }

    // Deliver-new: the subscriptions are created by the spawned delivery loops,
    // so publishing before they exist would be a race, not a test. Wait until
    // both queues are visible to the broker before publishing.
    let deadline = Instant::now() + Duration::from_secs(20);
    while ctx.queue_names().await.len() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let live = ctx.queue_names().await;
    assert_eq!(
        live.len(),
        2,
        "expected one ephemeral queue per subscriber, got {live:?}"
    );
    assert!(
        live.iter().all(|n| n != "rmq-broadcast-invalidations"),
        "the ephemeral queues must be server-named, not the topic name: {live:?}"
    );

    let keys: Vec<Invalidate> = (0..3)
        .map(|i| Invalidate {
            key: format!("user:{i}"),
        })
        .collect();
    let pubr = publisher_broker
        .publisher()
        .await
        .expect("failed to build publisher");
    for msg in &keys {
        pubr.publish::<CacheInvalidations>(msg)
            .await
            .expect("broadcast publish failed");
    }

    for recorder in &recorders {
        recorder.wait_for(3, Duration::from_secs(20)).await;
    }

    // Every instance saw every message — the whole point. A competing-consumer
    // topology would split these three across the two subscribers.
    let expected: HashSet<String> = keys.iter().map(|k| k.key.clone()).collect();
    for (i, recorder) in recorders.iter().enumerate() {
        let seen: HashSet<String> = recorder.keys().await.into_iter().collect();
        assert_eq!(
            seen, expected,
            "subscriber {i} did not receive the full fan-out"
        );
    }

    // Shut both down and close their connections.
    for (broker, sub) in subscribers {
        let token = sub.cancellation_token();
        token.cancel();
        let outcome = sub
            .run_until_timeout(std::future::pending(), Duration::from_secs(10))
            .await;
        assert!(
            !outcome.timed_out,
            "broadcast subscriber drain timed out; the abort path is not what this test is for"
        );
        broker.close().await;
    }

    // AC5: nothing survives. Not "the queue we know about is gone" — *no*
    // queue exists in the vhost, so a second ephemeral queue nobody accounted
    // for would fail this too.
    let leftover = ctx.wait_for_no_queues(Duration::from_secs(15)).await;
    assert_eq!(
        leftover,
        Vec::<String>::new(),
        "broadcast subscriptions left queues behind: {leftover:?}"
    );

    publisher_broker.close().await;
    ctx.cleanup().await;
}

/// AC5 on the abort path: a subscriber whose task is aborted rather than
/// drained still leaves nothing behind.
///
/// This is the case the design calls out specifically, because it is the one a
/// teardown-code-path implementation gets wrong: the queue is removed by the
/// broker because of *how it was declared*, not because cleanup code ran. The
/// process here is killed the hard way — the client is closed out from under
/// the subscriber and its handle dropped without a drain.
#[tokio::test]
async fn broadcast_queue_is_reclaimed_when_the_subscriber_is_dropped() {
    let ctx = TestContext::new().await;

    let broker = ctx.broker().await;
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("failed to declare broadcast topology");

    let recorder = Recorder::default();
    let mut sub = broker.broadcast_subscriber();
    sub.subscribe::<CacheInvalidations, _>(recorder, ConsumerOptions::new())
        .expect("failed to subscribe");

    let deadline = Instant::now() + Duration::from_secs(20);
    while ctx.queue_names().await.is_empty() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        ctx.queue_names().await.len(),
        1,
        "the subscription's ephemeral queue never appeared"
    );

    // No cancel, no drain: drop the subscriber handle, which drops the
    // `JoinSet` and aborts the delivery task mid-run.
    drop(sub);
    broker.close().await;

    let leftover = ctx.wait_for_no_queues(Duration::from_secs(15)).await;
    assert_eq!(
        leftover,
        Vec::<String>::new(),
        "an aborted broadcast subscription left a queue behind: {leftover:?}"
    );

    ctx.cleanup().await;
}

/// `Defer` redelivers within the deferring subscription and reaches no other.
///
/// The exclusive queue is what makes this true rather than approximately true:
/// a nack-requeue can only land back on the one queue it came from, and no
/// other instance is bound to it.
#[tokio::test]
async fn defer_redelivers_only_to_the_subscriber_that_deferred() {
    let ctx = TestContext::new().await;

    let publisher_broker = ctx.broker().await;
    publisher_broker
        .topology()
        .declare::<DeferTopic>()
        .await
        .expect("failed to declare broadcast topology");

    let deferring_broker = ctx.broker().await;
    let deferring = DeferOnce::default();
    let mut deferring_sub = deferring_broker.broadcast_subscriber();
    deferring_sub
        .subscribe::<DeferTopic, _>(deferring.clone(), ConsumerOptions::new())
        .expect("failed to subscribe");

    // A plain acking handler on purpose: a second `DeferOnce` would defer its
    // own copy and see two calls by its own doing, which is exactly the shape
    // this test is trying to rule out — the assertion would pass or fail for
    // the wrong reason.
    let bystander_broker = ctx.broker().await;
    let bystander = Recorder::default();
    let mut bystander_sub = bystander_broker.broadcast_subscriber();
    bystander_sub
        .subscribe::<DeferTopic, _>(bystander.clone(), ConsumerOptions::new())
        .expect("failed to subscribe");

    let deadline = Instant::now() + Duration::from_secs(20);
    while ctx.queue_names().await.len() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(ctx.queue_names().await.len(), 2);

    publisher_broker
        .publisher()
        .await
        .expect("failed to build publisher")
        .publish::<DeferTopic>(&Invalidate {
            key: "deferred".into(),
        })
        .await
        .expect("broadcast publish failed");

    // The deferring subscriber sees it twice: once deferred, once acked.
    let deadline = Instant::now() + Duration::from_secs(20);
    while deferring.calls().await.len() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Settle: give a redelivery that leaked to the bystander time to arrive
    // before asserting it did not.
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_eq!(
        deferring.calls().await,
        vec!["deferred".to_string(), "deferred".to_string()],
        "the deferring subscriber must see the message again"
    );
    assert_eq!(
        bystander.keys().await,
        vec!["deferred".to_string()],
        "the deferral must not be redelivered to the rest of the fan-out"
    );

    for (broker, sub) in [
        (deferring_broker, deferring_sub),
        (bystander_broker, bystander_sub),
    ] {
        sub.cancellation_token().cancel();
        let _ = sub
            .run_until_timeout(std::future::pending(), Duration::from_secs(10))
            .await;
        broker.close().await;
    }
    publisher_broker.close().await;
    ctx.cleanup().await;
}

/// `Retry` on a broadcast subscription discards, and does not loop.
///
/// RabbitMQ's shared `route_retry` falls back to a nack-**requeue** when a
/// topology declares no hold queues, and a broadcast topology can declare none.
/// Requeuing onto an exclusive queue puts the message straight back in front of
/// the same consumer, so reusing that path here would spin a broadcast message
/// forever at full speed. Hence the observable: exactly one handler call.
#[tokio::test]
async fn retry_discards_instead_of_requeuing_forever() {
    let ctx = TestContext::new().await;

    let publisher_broker = ctx.broker().await;
    publisher_broker
        .topology()
        .declare::<RetryTopic>()
        .await
        .expect("failed to declare broadcast topology");

    let broker = ctx.broker().await;
    let handler = AlwaysRetry::default();
    let mut sub = broker.broadcast_subscriber();
    sub.subscribe::<RetryTopic, _>(handler.clone(), ConsumerOptions::new())
        .expect("failed to subscribe");

    let deadline = Instant::now() + Duration::from_secs(20);
    while ctx.queue_names().await.is_empty() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(ctx.queue_names().await.len(), 1);

    publisher_broker
        .publisher()
        .await
        .expect("failed to build publisher")
        .publish::<RetryTopic>(&Invalidate {
            key: "doomed".into(),
        })
        .await
        .expect("broadcast publish failed");

    // A requeue loop on an exclusive queue redelivers with no delay at all, so
    // a few seconds is thousands of calls rather than a second one.
    tokio::time::sleep(Duration::from_secs(5)).await;

    assert_eq!(
        handler.calls.lock().await.clone(),
        vec!["doomed".to_string()],
        "a broadcast Retry must discard on the first attempt, not requeue"
    );

    sub.cancellation_token().cancel();
    let _ = sub
        .run_until_timeout(std::future::pending(), Duration::from_secs(10))
        .await;
    broker.close().await;
    publisher_broker.close().await;
    ctx.cleanup().await;
}
