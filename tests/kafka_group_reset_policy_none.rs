//! Integration test for a consumer group under `KafkaAutoOffsetReset::None`
//! on a fresh group id, through the public group API against a real broker.
//!
//! `None` is librdkafka's `auto.offset.reset=error`. A group member with no
//! usable committed offset receives `AutoOffsetReset` instead of a first
//! record and ends with a `ShoveError::Topology` that names it. That ends the
//! **member**, not the group: the group's spawner logs the error, adds one to
//! the group's error count and returns, and the group run keeps waiting for
//! its shutdown signal. Without autoscaling nothing respawns the member, so
//! the group runs below its minimum until it is shut down, and the count comes
//! back in `SupervisorOutcome::errors`.
//!
//! The direct-consumer path, where `KafkaConsumer::run` returns the error to
//! its caller, is covered in `kafka_offset_reset_integration.rs`; the respawn
//! timers are covered by the paused-time unit tests in `supervision.rs`. This
//! file pins the group lifecycle between the two, which no other test drives
//! through the public `None` configuration.
//!
//! The assertions read the `tracing` output through an in-memory writer,
//! because the spawner keeps only a count and discards the error's type. A
//! count alone cannot tell librdkafka's answer from a rejected configuration
//! token: the `Kafka consumer started` line lands before the native client
//! exists, and the spawner counts a client-creation failure and a receive
//! error the same way. The `consumer recv error` diagnostic naming
//! `AutoOffsetReset` can tell them apart, and the `Earliest` leg on the same
//! broker proves that the seeded records were there to fetch and that the
//! capture pipeline delivers the lines the `None` leg counts.

#![cfg(feature = "kafka")]

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaAutoOffsetReset, KafkaClient, KafkaConfig, KafkaConsumerGroupConfig};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::time::Instant;
use tracing::subscriber::set_global_default;
use tracing_subscriber::fmt::MakeWriter;

/// The topic, and so the `queue` field every consumer diagnostic carries.
const QUEUE: &str = "kafka-group-reset-policy-none";
/// Group ids that neither contain the other nor the topic name, so a
/// substring match on a log line cannot pick up the wrong leg.
const NONE_GROUP: &str = "fresh-group-under-none";
const EARLIEST_GROUP: &str = "control-group-under-earliest";

/// How long a leg may take to reach its first observation: a container start
/// is already behind us, but the member still has to join the group and be
/// assigned partitions on a small Docker host.
const LEG_TIMEOUT: Duration = Duration::from_secs(60);
/// After the member has ended, how long to watch for a second start or a
/// second exit before concluding that nothing respawned it. The autoscaler
/// tick that would drive a respawn defaults to 5 s; without autoscaling there
/// is no tick at all, and the window only has to outlast a reconnect attempt
/// that the permanent classification says must not happen.
const RESPAWN_WATCH: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Tick {
    id: String,
}

shove::define_topic!(SeedTopic, Tick, TopologyBuilder::new(QUEUE).build());

/// Records every message id it sees. The `None` leg must see none of them
/// and the `Earliest` leg all of them.
#[derive(Clone, Default)]
struct RecordingHandler {
    seen: Arc<Mutex<Vec<String>>>,
}

impl RecordingHandler {
    fn sorted_ids(&self) -> Vec<String> {
        let mut ids = self.seen.lock().expect("handler mutex poisoned").clone();
        ids.sort();
        ids
    }
}

impl MessageHandler<SeedTopic> for RecordingHandler {
    type Context = ();
    async fn handle(&self, msg: Tick, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .push(msg.id);
        Outcome::Ack
    }
}

/// An in-memory `tracing` writer, so the test asserts on what a subscriber
/// wrote rather than on shove's own formatting.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn contents(&self) -> String {
        let buffer = self.0.lock().expect("log buffer mutex poisoned");
        String::from_utf8_lossy(&buffer).into_owned()
    }

    /// Drop what has been captured so far, so each leg is asserted against
    /// its own output rather than everything accumulated before it.
    fn clear(&self) {
        self.0.lock().expect("log buffer mutex poisoned").clear();
    }

    /// The captured lines that contain every one of `needles`.
    fn lines_with(&self, needles: &[&str]) -> Vec<String> {
        self.contents()
            .lines()
            .filter(|line| needles.iter().all(|needle| line.contains(needle)))
            .map(str::to_owned)
            .collect()
    }
}

impl io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

struct TestBroker {
    _container: testcontainers::ContainerAsync<KafkaContainer>,
    port: u16,
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
            _container: container,
            port,
        }
    }

    /// A broker on its **own** client, against the same container. Every
    /// group lifecycle needs a fresh one: `run_until_timeout` cancels the
    /// client's shutdown token when its signal fires, so a client that has
    /// hosted one group is spent.
    async fn broker(&self) -> Broker<Kafka> {
        let client = KafkaClient::connect_with_retry(
            &KafkaConfig::new(format!("127.0.0.1:{}", self.port)),
            10,
        )
        .await
        .expect("failed to connect to Kafka");
        Broker::<Kafka>::from_client(client)
    }
}

fn seed_ids() -> Vec<String> {
    (0..3).map(|i| format!("seed-{i}")).collect()
}

/// A consumer group on `group_id` with one member and the given reset policy.
fn group_config(group_id: &str, policy: KafkaAutoOffsetReset) -> ConsumerGroupConfig<Kafka> {
    ConsumerGroupConfig::new(
        KafkaConsumerGroupConfig::new(1..=1)
            .with_group_id(group_id)
            .with_auto_offset_reset(policy),
    )
}

/// Under `None`, a fresh group's member ends with `ShoveError::Topology`
/// naming librdkafka's `AutoOffsetReset` answer. The group run does not end,
/// nothing respawns the member without autoscaling, and the one error comes
/// back in the `SupervisorOutcome` at shutdown. An `Earliest` group on the same
/// broker drains the seeded records with no error, which proves the records
/// were there and that the captured diagnostics are the ones this test counts.
#[tokio::test]
async fn reset_policy_none_on_a_fresh_group_ends_the_member_and_not_the_group() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    set_global_default(subscriber).expect("no other global subscriber in this test binary");

    let tb = TestBroker::start().await;
    let ids = seed_ids();
    {
        let seeder = tb.broker().await;
        seeder.topology().declare::<SeedTopic>().await.unwrap();
        let msgs: Vec<Tick> = ids.iter().map(|id| Tick { id: id.clone() }).collect();
        seeder
            .publisher()
            .await
            .unwrap()
            .publish_batch::<SeedTopic>(&msgs)
            .await
            .unwrap();
        seeder.close().await;
    }
    logs.clear();

    // --- `None` on a fresh group id -----------------------------------------

    let broker = tb.broker().await;
    let handler = RecordingHandler::default();
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<SeedTopic, _>(
            group_config(NONE_GROUP, KafkaAutoOffsetReset::None),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let run = group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(10));
    tokio::pin!(run);

    // The spawner's exit line marks the member's end. It carries no fields of
    // its own, only the error's text, so it is matched on its message alone:
    // this binary runs one group at a time. The run is polled in the same
    // select so a group run that returns on its own fails here, with the
    // outcome it returned, instead of hanging the wait.
    let member_exit = ["consumer task exited with error"];
    let deadline = Instant::now() + LEG_TIMEOUT;
    while logs.lines_with(&member_exit).is_empty() {
        assert!(
            Instant::now() < deadline,
            "no member ended within {LEG_TIMEOUT:?}; captured output:\n{}",
            logs.contents()
        );
        tokio::select! {
            outcome = &mut run => panic!("the group run ended on its own with {outcome:?}"),
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    // Watch for a respawn. The run must still be pending when the window
    // closes: the member's error ends the member, not the group.
    tokio::select! {
        outcome = &mut run => panic!("the group run ended after its member did, with {outcome:?}"),
        () = tokio::time::sleep(RESPAWN_WATCH) => {}
    }

    // The receive diagnostic is what tells librdkafka's answer to a fetch from
    // a client that refused its configuration before ever fetching.
    let recv_errors = logs.lines_with(&["consumer recv error", QUEUE]);
    assert!(
        recv_errors
            .iter()
            .any(|line| line.contains("AutoOffsetReset")),
        "the member's receive error must name librdkafka's AutoOffsetReset answer; \
         receive errors: {recv_errors:#?}\ncaptured output:\n{}",
        logs.contents()
    );
    let exits = logs.lines_with(&member_exit);
    assert_eq!(
        exits.len(),
        1,
        "exactly one member ended, so nothing respawned it: {exits:#?}"
    );
    assert!(
        exits[0].contains("topology error") && exits[0].contains("AutoOffsetReset"),
        "the member ends with ShoveError::Topology naming AutoOffsetReset: {}",
        exits[0]
    );
    let starts = logs.lines_with(&["Kafka consumer started", NONE_GROUP]);
    assert_eq!(
        starts.len(),
        1,
        "one member started and none replaced it without autoscaling: {starts:#?}"
    );
    assert!(
        handler.sorted_ids().is_empty(),
        "a fresh group under None must not consume: {:?}",
        handler.sorted_ids()
    );

    token.cancel();
    let outcome = run.await;
    assert_eq!(
        outcome.errors, 1,
        "the member's error is reported once at shutdown: {outcome:?}"
    );
    assert_eq!(outcome.panics, 0, "{outcome:?}");
    assert!(!outcome.timed_out, "{outcome:?}");
    broker.close().await;
    logs.clear();

    // --- `Earliest` control on the same broker ------------------------------

    let broker = tb.broker().await;
    let handler = RecordingHandler::default();
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<SeedTopic, _>(
            group_config(EARLIEST_GROUP, KafkaAutoOffsetReset::Earliest),
            move || h.clone(),
        )
        .await
        .unwrap();
    let token = group.cancellation_token();
    let run = group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(10));
    tokio::pin!(run);

    let deadline = Instant::now() + LEG_TIMEOUT;
    while handler.sorted_ids().len() < ids.len() {
        assert!(
            Instant::now() < deadline,
            "the Earliest control did not receive the seeded records within {LEG_TIMEOUT:?}; \
             got {:?}; captured output:\n{}",
            handler.sorted_ids(),
            logs.contents()
        );
        tokio::select! {
            outcome = &mut run => panic!("the control's group run ended on its own with {outcome:?}"),
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
    token.cancel();
    let outcome = run.await;
    assert!(outcome.is_clean(), "{outcome:?}");
    assert_eq!(handler.sorted_ids(), ids);
    let starts = logs.lines_with(&["Kafka consumer started", EARLIEST_GROUP]);
    assert_eq!(
        starts.len(),
        1,
        "the capture pipeline delivers the start line the None leg counted: {starts:#?}"
    );
    assert!(
        logs.lines_with(&["AutoOffsetReset"]).is_empty(),
        "a group with a reset policy never meets AutoOffsetReset:\n{}",
        logs.contents()
    );
    assert!(
        logs.lines_with(&member_exit).is_empty(),
        "the control's member must not end on its own:\n{}",
        logs.contents()
    );
    broker.close().await;
}
