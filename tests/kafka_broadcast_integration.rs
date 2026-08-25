#![cfg(feature = "kafka")]

//! Integration tests for Kafka ephemeral per-instance broadcast subscriptions.
//!
//! The property that needs a real broker is **AC6 — no consumer group**. A
//! broadcast subscriber assigns partitions manually and never commits, so the
//! broker should never learn that it exists: no JoinGroup, no OffsetCommit, and
//! nothing in `kafka-consumer-groups --list` no matter how many instances start
//! and stop.
//!
//! A bare "the group list does not contain X" assertion is worth very little on
//! its own — it also passes if the group list is empty because the query is
//! wrong, or because the subscriber never connected. So
//! [`broadcast_leaves_no_consumer_group`] carries a control: an ordinary
//! consumer group on a second topic, which *must* appear in the very same list
//! read at the very same moment.
//!
//! Run with:
//! `cargo nextest run --features kafka --test kafka_broadcast_integration`

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumerGroupConfig, KafkaOffsetReset};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use shove::{ShoveError, define_topic};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Mutex;
use tokio::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Invalidate {
    key: String,
}

define_topic!(
    CacheInvalidations,
    Invalidate,
    TopologyBuilder::new("kafka-broadcast-invalidations")
        .broadcast()
        .build()
);

define_topic!(
    DeferTopic,
    Invalidate,
    TopologyBuilder::new("kafka-broadcast-defer")
        .broadcast()
        .build()
);

define_topic!(
    RetryTopic,
    Invalidate,
    TopologyBuilder::new("kafka-broadcast-retry")
        .broadcast()
        .build()
);

// The control for AC6: an ordinary competing-consumer topic whose group the
// broker *does* register.
define_topic!(
    ControlTopic,
    Invalidate,
    TopologyBuilder::new("kafka-broadcast-control").build()
);

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

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

recorder_for!(CacheInvalidations, ControlTopic, DeferTopic);

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

/// Defers the first delivery of each key and acks the redelivery.
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
// Harness
// ---------------------------------------------------------------------------

struct TestBroker {
    _container: testcontainers::ContainerAsync<KafkaContainer>,
    brokers: String,
}

impl TestBroker {
    async fn start() -> Self {
        let container = KafkaContainer::default()
            .start()
            .await
            .expect("failed to start Kafka container");
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("failed to get Kafka port");
        Self {
            brokers: format!("127.0.0.1:{port}"),
            _container: container,
        }
    }

    fn brokers(&self) -> String {
        self.brokers.clone()
    }

    /// A broker on its **own** client, against the same container.
    ///
    /// One client per subscriber, always: `run_until_timeout` cancels the
    /// *client's* shutdown token, so a client that has hosted one subscription
    /// is spent. Separate clients also model what this file is about —
    /// separate processes.
    async fn broker(&self) -> Broker<Kafka> {
        let client = KafkaClient::connect_with_retry(&KafkaConfig::new(self.brokers()), 10)
            .await
            .expect("failed to connect to Kafka");
        Broker::<Kafka>::from_client(client)
    }

    /// Every consumer group the broker currently knows about.
    ///
    /// Read through an independent `BaseConsumer` rather than through anything
    /// under test, so a bug that stopped the broadcast subscriber connecting at
    /// all could not also silence this query.
    fn consumer_group_names(&self) -> HashSet<String> {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers())
            // The probe itself never subscribes and never commits, so it does
            // not create the group it is named after — which is the same reason
            // a broadcast subscriber does not create its own.
            .set("group.id", "broadcast-test-group-probe")
            .create()
            .expect("failed to create group-list probe");
        probe
            .client()
            .fetch_group_list(None, Duration::from_secs(10))
            .expect("failed to fetch group list")
            .groups()
            .iter()
            .map(|g| g.name().to_string())
            .collect()
    }

    async fn expand_topic(&self, topic: &str, partitions: usize) {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", self.brokers())
            .create()
            .expect("failed to create partition-expansion admin client");
        let results = admin
            .create_partitions(
                &[NewPartitions::new(topic, partitions)],
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
            )
            .await
            .expect("partition-expansion request failed");
        assert!(
            matches!(results.as_slice(), [Ok(name)] if name == topic),
            "partition expansion failed: {results:?}"
        );
    }

    fn partition_count(&self, topic: &str) -> usize {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers())
            .set("group.id", "broadcast-partition-count-probe")
            .create()
            .expect("failed to create metadata probe");
        let metadata = probe
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .expect("failed to fetch topic metadata");
        metadata
            .topics()
            .iter()
            .find(|candidate| candidate.name() == topic)
            .expect("declared topic missing from metadata")
            .partitions()
            .len()
    }

    async fn publish_to_partition(&self, topic: &str, partition: i32, msg: &Invalidate) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers())
            .create()
            .expect("failed to create partition-targeted producer");
        let payload = serde_json::to_string(msg).expect("serialize partition-targeted message");
        producer
            .send(
                FutureRecord::<str, str>::to(topic)
                    .partition(partition)
                    .payload(&payload),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .expect("partition-targeted publish failed");
    }
}

/// Wait until a broadcast subscription has certainly assigned its partitions.
///
/// There is nothing broker-side to poll for — that is the whole point of the
/// design — so this is a fixed settle window rather than a condition. Publishing
/// before the assignment lands would test deliver-new's failure mode instead of
/// the fan-out.
const ASSIGN_SETTLE: Duration = Duration::from_secs(5);

/// A caller-provided zero prefetch must still allow one handler to run.
///
/// The public option is a plain `u16`, so zero reaches the backend. A
/// zero-permit semaphore parks the loop after receiving its first record and
/// also prevents graceful shutdown from completing.
#[tokio::test]
async fn zero_prefetch_is_clamped_for_broadcast() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("failed to declare broadcast topic");

    let recorder = Recorder::default();
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(
            recorder.clone(),
            ConsumerOptions::new().with_prefetch_count(0),
        )
        .expect("failed to subscribe");
    tokio::time::sleep(ASSIGN_SETTLE).await;

    broker
        .publisher()
        .await
        .expect("failed to build publisher")
        .publish::<CacheInvalidations>(&Invalidate {
            key: "zero-prefetch".into(),
        })
        .await
        .expect("publish failed");

    recorder.wait_for(1, Duration::from_secs(10)).await;
    assert_eq!(recorder.keys().await, vec!["zero-prefetch"]);

    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending(), Duration::from_secs(2))
        .await;
    assert!(
        outcome.is_clean(),
        "zero-prefetch subscriber failed to drain: {outcome:?}"
    );
    broker.close().await;
}

/// A connected assign-only subscriber must discover partitions added later.
///
/// Messages sent before the new assignment is visible are legitimately before
/// that partition's deliver-new boundary. Repeated targeted publishes make the
/// boundary observable: once metadata refresh extends the assignment, a later
/// marker from the new partition must arrive without reconnecting the
/// subscriber. Markers on partition 0 pin the other half of the operation:
/// extending the assignment must preserve existing positions without loss or
/// replay.
#[tokio::test]
async fn connected_broadcast_discovers_new_partitions() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("failed to declare broadcast topic");

    let recorder = Recorder::default();
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<CacheInvalidations, _>(recorder.clone(), ConsumerOptions::new())
        .expect("failed to subscribe");
    tokio::time::sleep(ASSIGN_SETTLE).await;

    let original_partitions = tb.partition_count("kafka-broadcast-invalidations");
    tb.publish_to_partition(
        "kafka-broadcast-invalidations",
        0,
        &Invalidate {
            key: "before-expansion".into(),
        },
    )
    .await;
    recorder.wait_for(1, Duration::from_secs(10)).await;
    assert_eq!(recorder.keys().await, vec!["before-expansion"]);

    tb.expand_topic("kafka-broadcast-invalidations", original_partitions + 1)
        .await;

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut marker = 0u32;
    while !recorder
        .keys()
        .await
        .iter()
        .any(|key| key.starts_with("expanded-"))
        && Instant::now() < deadline
    {
        tb.publish_to_partition(
            "kafka-broadcast-invalidations",
            0,
            &Invalidate {
                key: format!("steady-{marker}"),
            },
        )
        .await;
        tb.publish_to_partition(
            "kafka-broadcast-invalidations",
            original_partitions as i32,
            &Invalidate {
                key: format!("expanded-{marker}"),
            },
        )
        .await;
        marker += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let delivery_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let seen = recorder.keys().await;
        let steady = seen.iter().filter(|key| key.starts_with("steady-")).count();
        if steady == marker as usize || Instant::now() >= delivery_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let seen = recorder.keys().await;
    assert!(
        seen.iter().any(|key| key.starts_with("expanded-")),
        "the connected subscriber never extended its assignment to partition {original_partitions}"
    );
    assert_eq!(
        seen.iter()
            .filter(|key| key.as_str() == "before-expansion")
            .count(),
        1,
        "refreshing the assignment must not replay an existing partition"
    );
    for expected in (0..marker).map(|i| format!("steady-{i}")) {
        assert_eq!(
            seen.iter().filter(|key| **key == expected).count(),
            1,
            "refreshing the assignment lost or replayed {expected}; saw {seen:?}"
        );
    }

    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending(), Duration::from_secs(5))
        .await;
    assert!(
        outcome.is_clean(),
        "subscriber failed to drain: {outcome:?}"
    );
    broker.close().await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// AC6: two broadcast subscribers come and go, and the broker ends up with no
/// consumer group for their topic — while an ordinary group on a control topic
/// shows up in the same list.
#[tokio::test]
async fn broadcast_leaves_no_consumer_group() {
    let tb = TestBroker::start().await;

    let setup = tb.broker().await;
    setup
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("failed to declare broadcast topic");
    setup
        .topology()
        .declare::<ControlTopic>()
        .await
        .expect("failed to declare control topic");
    setup.close().await;

    // Two broadcast instances, started and stopped twice over, so a group that
    // is only created on a *second* boot would still be caught.
    for _round in 0..2 {
        let mut running = Vec::new();
        for _ in 0..2 {
            let broker = tb.broker().await;
            let mut sub = broker.broadcast_subscriber();
            sub.subscribe::<CacheInvalidations, _>(Recorder::default(), ConsumerOptions::new())
                .expect("failed to subscribe");
            running.push((broker, sub));
        }
        tokio::time::sleep(ASSIGN_SETTLE).await;
        for (broker, sub) in running {
            sub.cancellation_token().cancel();
            let _ = sub
                .run_until_timeout(std::future::pending(), Duration::from_secs(10))
                .await;
            broker.close().await;
        }
    }

    // The control: an ordinary consumer group, which must register.
    let control_broker = tb.broker().await;
    let control_handler = Recorder::default();
    let h = control_handler.clone();
    let mut group = control_broker.consumer_group();
    group
        .register::<ControlTopic, _>(
            ConsumerGroupConfig::new(KafkaConsumerGroupConfig::new(1..=1)),
            move || h.clone(),
        )
        .await
        .expect("failed to register control group");
    let token = group.cancellation_token();
    let running_group = tokio::spawn(async move {
        group
            .run_until_timeout(token.cancelled_owned(), Duration::from_secs(10))
            .await
    });
    tokio::time::sleep(ASSIGN_SETTLE).await;

    let groups = tb.consumer_group_names();

    assert!(
        groups.contains("kafka-broadcast-control-consumer"),
        "the control group is missing, so this query proves nothing about the \
         broadcast group's absence; saw {groups:?}"
    );
    assert!(
        !groups.contains("kafka-broadcast-invalidations-broadcast"),
        "a broadcast subscription registered a consumer group: {groups:?}"
    );
    // Belt and braces: nothing named after the topic at all, so a group under
    // some other derived name (`-consumer`, a per-process UUID) fails too.
    let topic_groups: Vec<&String> = groups
        .iter()
        .filter(|g| g.contains("kafka-broadcast-invalidations"))
        .collect();
    assert!(
        topic_groups.is_empty(),
        "broadcast left consumer groups behind for its topic: {topic_groups:?}"
    );

    control_broker.close().await;
    let _ = running_group.await;
}

/// Every instance receives every message, and nothing published before a
/// subscription existed is replayed into it.
#[tokio::test]
async fn broadcast_fans_out_to_every_instance_from_the_tail() {
    let tb = TestBroker::start().await;

    let publisher_broker = tb.broker().await;
    publisher_broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("failed to declare broadcast topic");

    // Published before anyone subscribes. Deliver-new means no subscriber ever
    // sees this, which is what pins the assignment to the tail rather than to
    // `auto.offset.reset`.
    publisher_broker
        .publisher()
        .await
        .expect("failed to build publisher")
        .publish::<CacheInvalidations>(&Invalidate {
            key: "before-anyone-listened".into(),
        })
        .await
        .expect("publish failed");

    let mut running = Vec::new();
    let mut recorders = Vec::new();
    for _ in 0..2 {
        let broker = tb.broker().await;
        let recorder = Recorder::default();
        let mut sub = broker.broadcast_subscriber();
        sub.subscribe::<CacheInvalidations, _>(recorder.clone(), ConsumerOptions::new())
            .expect("failed to subscribe");
        recorders.push(recorder);
        running.push((broker, sub));
    }
    tokio::time::sleep(ASSIGN_SETTLE).await;

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
            .expect("publish failed");
    }

    for recorder in &recorders {
        recorder.wait_for(3, Duration::from_secs(30)).await;
    }
    // Settle, so a replay of the pre-subscription message has time to show up
    // before it is asserted absent.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let expected: HashSet<String> = keys.iter().map(|k| k.key.clone()).collect();
    for (i, recorder) in recorders.iter().enumerate() {
        let delivered = recorder.keys().await;
        // Length before set, and from the raw vector: collapsing into a
        // `HashSet` first would swallow a duplicate delivery, and broadcast is
        // documented as lossy *at-most-once*. A second copy of one key is as
        // much a contract break as a missing one.
        assert_eq!(
            delivered.len(),
            expected.len(),
            "subscriber {i} received {} deliveries for {} messages: {delivered:?}",
            delivered.len(),
            expected.len()
        );
        let seen: HashSet<String> = delivered.into_iter().collect();
        assert_eq!(
            seen, expected,
            "subscriber {i} saw the wrong set — a fan-out delivers every message to \
             every instance, and replays nothing from before it subscribed"
        );
    }

    for (broker, sub) in running {
        sub.cancellation_token().cancel();
        let _ = sub
            .run_until_timeout(std::future::pending(), Duration::from_secs(10))
            .await;
        broker.close().await;
    }
    publisher_broker.close().await;
}

/// `Defer` redelivers within the deferring subscription and reaches no other.
///
/// On Kafka this cannot be a requeue — there is no queue — so it is an
/// in-process redelivery. What matters is the observable: the deferring
/// instance handles the message twice, the other exactly once.
#[tokio::test]
async fn defer_redelivers_only_to_the_subscriber_that_deferred() {
    let tb = TestBroker::start().await;

    let publisher_broker = tb.broker().await;
    publisher_broker
        .topology()
        .declare::<DeferTopic>()
        .await
        .expect("failed to declare broadcast topic");

    let deferring_broker = tb.broker().await;
    let deferring = DeferOnce::default();
    let mut deferring_sub = deferring_broker.broadcast_subscriber();
    deferring_sub
        .subscribe::<DeferTopic, _>(deferring.clone(), ConsumerOptions::new())
        .expect("failed to subscribe");

    // A plain acking handler on purpose: a second `DeferOnce` would defer its
    // own copy and see two calls by its own doing, which is exactly the shape
    // this test is trying to rule out — the assertion would pass or fail for
    // the wrong reason.
    let bystander_broker = tb.broker().await;
    let bystander = Recorder::default();
    let mut bystander_sub = bystander_broker.broadcast_subscriber();
    bystander_sub
        .subscribe::<DeferTopic, _>(bystander.clone(), ConsumerOptions::new())
        .expect("failed to subscribe");

    tokio::time::sleep(ASSIGN_SETTLE).await;

    publisher_broker
        .publisher()
        .await
        .expect("failed to build publisher")
        .publish::<DeferTopic>(&Invalidate {
            key: "deferred".into(),
        })
        .await
        .expect("publish failed");

    let deadline = Instant::now() + Duration::from_secs(30);
    while deferring.calls().await.len() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Longer than the one-second defer delay, so a redelivery that leaked to
    // the bystander would have landed by now.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        deferring.calls().await,
        vec!["deferred".to_string(), "deferred".to_string()],
        "the deferring subscriber must see the message again"
    );
    assert_eq!(
        bystander.keys().await,
        vec!["deferred".to_string()],
        "a deferral must not be republished to the rest of the fan-out"
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
}

/// `reset_consumer_group_offsets` refuses a broadcast topology rather than
/// rewriting offsets for a group nothing on that path reads.
///
/// An integration test rather than a unit test because the refusal must happen
/// *before* the broker round trip: the point is that it is not a runtime
/// failure deep in the reset, and not a silent success either.
#[tokio::test]
async fn reset_consumer_group_offsets_refuses_a_broadcast_topology() {
    let tb = TestBroker::start().await;
    let broker = tb.broker().await;
    broker
        .topology()
        .declare::<CacheInvalidations>()
        .await
        .expect("failed to declare broadcast topic");

    let err = broker
        .reset_consumer_group_offsets::<CacheInvalidations>(
            &KafkaConsumerGroupConfig::new(1..=1),
            KafkaOffsetReset::Latest,
        )
        .await
        .expect_err("resetting offsets for a broadcast topology must be refused");

    assert!(
        matches!(err, ShoveError::Validation(_)),
        "expected a Validation error, got {err:?}"
    );
    assert!(
        err.to_string().contains("broadcast"),
        "the error must name broadcast as the reason; got: {err}"
    );

    broker.close().await;
}

/// `Retry` on a broadcast subscription discards, and does not loop.
///
/// This is the arm a naive implementation gets wrong in the most expensive way.
/// Kafka's shared `route_outcome` turns a `Retry` under budget into a delayed
/// **republish to the topic** — which on a broadcast topic is not a retry at
/// all: it fans the message back out to every instance, forever, at one
/// redelivery per second per subscriber. So the observable that matters is not
/// "the message was discarded" but "the handler ran exactly once, on each
/// instance", and a second subscriber is here to catch the fan-out half of it.
#[tokio::test]
async fn retry_discards_instead_of_looping_the_fan_out() {
    let tb = TestBroker::start().await;

    let publisher_broker = tb.broker().await;
    publisher_broker
        .topology()
        .declare::<RetryTopic>()
        .await
        .expect("failed to declare broadcast topic");

    let mut running = Vec::new();
    let mut handlers = Vec::new();
    for _ in 0..2 {
        let broker = tb.broker().await;
        let handler = AlwaysRetry::default();
        let mut sub = broker.broadcast_subscriber();
        sub.subscribe::<RetryTopic, _>(handler.clone(), ConsumerOptions::new())
            .expect("failed to subscribe");
        handlers.push(handler);
        running.push((broker, sub));
    }
    tokio::time::sleep(ASSIGN_SETTLE).await;

    publisher_broker
        .publisher()
        .await
        .expect("failed to build publisher")
        .publish::<RetryTopic>(&Invalidate {
            key: "doomed".into(),
        })
        .await
        .expect("publish failed");

    // Well past the one-second republish delay a looping implementation would
    // use, so "did not loop" is an observation rather than an absence of time.
    tokio::time::sleep(Duration::from_secs(8)).await;

    for (i, handler) in handlers.iter().enumerate() {
        let calls = handler.calls.lock().await.clone();
        assert_eq!(
            calls,
            vec!["doomed".to_string()],
            "subscriber {i} handled a Retry more than once — broadcast has no retry chain, \
             so the first Retry must discard"
        );
    }

    for (broker, sub) in running {
        sub.cancellation_token().cancel();
        let _ = sub
            .run_until_timeout(std::future::pending(), Duration::from_secs(10))
            .await;
        broker.close().await;
    }
    publisher_broker.close().await;
}
