//! The final offset commit of a stopping Kafka consumer, against librdkafka's
//! mock cluster (`rdkafka::mocking`), which serves the consumer group
//! protocol and offset commits from a localhost listener and needs no Docker.
//!
//! A member that stops issues one synchronous commit for every partition it
//! holds. When the coordinator rejects that commit, or gives no answer within
//! the shutdown deadline, the member ends with `ShoveError::Commit` instead
//! of a clean exit, and a group run counts that under `errors`; a rejection
//! while the group rebalances is tried again first. These tests inject the
//! rejection and the delay through the mock cluster's `OffsetCommit`
//! handling, which a real broker cannot be asked to do. The error itself is
//! read on the direct consumer, whose `run` returns it; the group run shows
//! the count and the exit code.
//!
//! The topic is bound with `external()`, because the mock broker has no
//! CreateTopics API: each test creates the topic through the mock API, as
//! infra would, and the record is produced with a raw rdkafka producer so it
//! carries a key beside its payload.
//!
//! `test-support` gates the deadline seam this file reads. The Kafka
//! coverage row enables it and runs this suite; the schema-registry row
//! compiles the binary too, but its test filter does not select it.

#![cfg(all(feature = "kafka", feature = "test-support"))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{DefaultProducerContext, FutureProducer, FutureRecord};
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::kafka::{
    KafkaClient, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig,
    shutdown_commit_deadline_for_test,
};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use shove::{CommitFailure, ShoveError, SupervisorOutcome};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const TOPIC: &str = "kafka-final-commit-mock";
/// The group the topic's default configuration joins: `{queue}-consumer`.
const GROUP_ID: &str = "kafka-final-commit-mock-consumer";
/// Markers that must never appear in the error a failed commit reports.
const KEY_MARKER: &str = "record-key-marker-7f3a";
const PAYLOAD_MARKER: &str = "record-payload-marker-2c9e";
/// A record reaches the handler on a mock cluster in well under this.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// The receive loop's own bound on its final commit, read through the
/// `test-support` seam so this file cannot drift from the constant; the
/// `Deadline` kind carries the value the loop used.
fn shutdown_commit_deadline() -> Duration {
    shutdown_commit_deadline_for_test()
}
/// How long a stop may take past the shutdown deadline before the test
/// gives up on it.
const DEADLINE_MARGIN: Duration = Duration::from_secs(20);
/// A drain timeout wide enough that the loop's own commit deadline, not the
/// drain, ends a commit that never answers.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(40);
/// A commit gate too wide to reopen within a test: the first completion is
/// committed asynchronously as soon as it lands, and the only commit after
/// that is the final one at shutdown, so an answer injected before the stop
/// meets the shutdown commit and no other.
const COMMIT_GATE: Duration = Duration::from_secs(3600);
/// The mock broker's round-trip time that holds every answer past the
/// shutdown deadline.
fn held_round_trip() -> Duration {
    shutdown_commit_deadline() + DEADLINE_MARGIN * 2
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
    note: String,
}

shove::define_topic!(
    OrdersTopic,
    Order,
    TopologyBuilder::new(TOPIC).external().build()
);

/// Acknowledges every record and counts it, so a test can wait for the
/// delivery with a bound.
#[derive(Clone, Default)]
struct AckingHandler {
    seen: Arc<Mutex<Vec<Order>>>,
}

impl AckingHandler {
    fn count(&self) -> usize {
        self.seen.lock().expect("handler mutex poisoned").len()
    }
}

impl MessageHandler<OrdersTopic> for AckingHandler {
    type Context = ();
    async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().expect("handler mutex poisoned").push(msg);
        Outcome::Ack
    }
}

/// Polls `done` until it holds, or panics once `timeout` has passed.
async fn wait_until(done: impl Fn() -> bool, timeout: Duration, what: &str) {
    let deadline = Instant::now() + timeout;
    while !done() {
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// One mock broker with the topic created, as infra would create it.
fn mock_cluster() -> MockCluster<'static, DefaultProducerContext> {
    let mock = MockCluster::new(1).expect("mock cluster");
    mock.create_topic(TOPIC, 1, 1).expect("mock topic");
    mock
}

async fn connect(mock: &MockCluster<'static, DefaultProducerContext>) -> KafkaClient {
    KafkaClient::connect_with_retry(&KafkaConfig::new(mock.bootstrap_servers()), 10)
        .await
        .expect("connect to the mock cluster")
}

/// Refuse every OffsetCommit from here on. Several answers are pushed so a
/// retry meets the same one.
fn reject_every_commit(mock: &MockCluster<'static, DefaultProducerContext>) {
    mock.request_errors(
        RDKafkaApiKey::OffsetCommit,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED; 8],
    );
}

/// Hold every answer past the shutdown deadline from here on.
fn hold_every_answer(mock: &MockCluster<'static, DefaultProducerContext>) {
    mock.broker_round_trip_time(-1, held_round_trip())
        .expect("raise the mock broker's round-trip time");
}

/// One record with a key and a payload the assertions can look for, through
/// a raw rdkafka producer so the key is set; the payload is the topic's JSON.
async fn produce_marked_record(bootstrap: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    let payload = serde_json::to_vec(&Order {
        id: "order-1".into(),
        note: PAYLOAD_MARKER.into(),
    })
    .expect("encode the record");
    producer
        .send(
            FutureRecord::to(TOPIC).key(KEY_MARKER).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
}

/// The group's committed position on the topic's one partition, read
/// through a raw consumer that never joins the group. `None` before the
/// first accepted commit.
async fn committed_position(bootstrap: &str) -> Option<i64> {
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", GROUP_ID)
            .create()
            .expect("probe consumer");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(TOPIC, 0);
        let committed = probe
            .committed_offsets(tpl, Duration::from_secs(5))
            .expect("read the committed offsets");
        committed
            .elements()
            .iter()
            .find(|e| e.partition() == 0)
            .and_then(|e| match e.offset() {
                Offset::Offset(offset) => Some(offset),
                _ => None,
            })
    })
    .await
    .expect("probe task")
}

/// Polls the broker until the group's committed position is `expected`.
async fn wait_for_committed_position(bootstrap: &str, expected: i64) {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while committed_position(bootstrap).await != Some(expected) {
        assert!(
            Instant::now() < deadline,
            "the broker did not accept the commit at {expected} within {DELIVERY_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Delivers the marked record to `handler` and waits until the broker has
/// accepted the asynchronous commit of its position. Only once it has does
/// an injection meet the final commit and no other, and the position a
/// failure leaves behind is known.
async fn deliver_one_record_and_wait_for_its_commit(
    mock: &MockCluster<'static, DefaultProducerContext>,
    handler: &AckingHandler,
) {
    let bootstrap = mock.bootstrap_servers();
    produce_marked_record(&bootstrap).await;
    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;
    wait_for_committed_position(&bootstrap, 1).await;
}

/// Runs one direct consumer on the topic, delivers the marked record, waits
/// for its asynchronous commit, applies `before_stop` to the mock cluster,
/// stops the consumer and returns what `run` returned with how long the
/// stop took.
async fn stop_a_consumer_after_one_record(
    mock: &MockCluster<'static, DefaultProducerContext>,
    before_stop: impl FnOnce(&MockCluster<'static, DefaultProducerContext>),
) -> (Result<(), ShoveError>, Duration) {
    let client = connect(mock).await;
    let handler = AckingHandler::default();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let consumer = KafkaConsumer::new(client);
    let run = tokio::spawn(async move {
        consumer
            .run::<OrdersTopic, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(token)
                    .with_commit_interval(COMMIT_GATE),
            )
            .await
    });

    deliver_one_record_and_wait_for_its_commit(mock, &handler).await;

    before_stop(mock);
    let stopped_at = Instant::now();
    shutdown.cancel();
    let result = run.await.expect("the run task completes");
    (result, stopped_at.elapsed())
}

/// Runs a one-member group on the topic through `run_until_timeout` with
/// `drain_timeout`, delivers the marked record, waits for its asynchronous
/// commit, applies `before_stop`, stops the run and returns the outcome.
async fn stop_a_group_after_one_record(
    mock: &MockCluster<'static, DefaultProducerContext>,
    drain_timeout: Duration,
    before_stop: impl FnOnce(&MockCluster<'static, DefaultProducerContext>),
) -> SupervisorOutcome {
    let broker = Broker::<Kafka>::from_client(connect(mock).await);
    let handler = AckingHandler::default();
    let h = handler.clone();
    let mut group = broker.consumer_group();
    group
        .register::<OrdersTopic, _>(
            ConsumerGroupConfig::new(
                KafkaConsumerGroupConfig::new(1..=1).with_commit_interval(COMMIT_GATE),
            ),
            move || h.clone(),
        )
        .await
        .expect("register on the mock cluster");
    let token = group.cancellation_token();
    let run = tokio::spawn(group.run_until_timeout(token.clone().cancelled_owned(), drain_timeout));

    deliver_one_record_and_wait_for_its_commit(mock, &handler).await;

    before_stop(mock);
    token.cancel();
    let outcome = run.await.expect("the run task completes");
    broker.close().await;
    outcome
}

/// The commit error `run` returned, checked to name the topic and the
/// acknowledged record's exclusive position on the only partition.
fn the_commit_error(result: &Result<(), ShoveError>) -> &CommitFailure {
    let Err(ShoveError::Commit(failed)) = result else {
        panic!("run ends with ShoveError::Commit: {result:?}");
    };
    assert_eq!(failed.topic, TOPIC);
    assert_eq!(
        failed.offsets,
        [(0, 1)],
        "the acknowledged record's position"
    );
    &failed.kind
}

/// A rejected OffsetCommit at shutdown ends the consumer with `Commit` of
/// the `Rejected` kind carrying the broker's answer, where 0.15.0 returned
/// `Ok(())`. The rejection leaves the group's position where the earlier
/// accepted commit put it: the error says this commit did not land, not
/// that nothing did.
#[tokio::test]
async fn a_rejected_final_commit_ends_the_consumer_with_the_rejected_kind() {
    let mock = mock_cluster();
    let (result, _took) = stop_a_consumer_after_one_record(&mock, reject_every_commit).await;

    let kind = the_commit_error(&result);
    let CommitFailure::Rejected(text) = kind else {
        panic!("a refused commit is reported as Rejected: {kind:?}");
    };
    assert!(
        text.contains("authorization failed") || text.contains("Authorization failed"),
        "the broker's answer is carried: {text}"
    );

    assert_eq!(
        committed_position(&mock.bootstrap_servers()).await,
        Some(1),
        "the earlier accepted commit stays the group's position"
    );
}

/// The error a rejected commit reports carries the broker's error code and
/// text, and the positions, and nothing of the record behind them: not its
/// payload, not its key. Checked on the `Display` and `Debug` renderings a
/// process would log.
#[tokio::test]
async fn a_rejected_commit_error_carries_no_record_payload_or_key() {
    let mock = mock_cluster();
    let (result, _took) = stop_a_consumer_after_one_record(&mock, reject_every_commit).await;

    let kind = the_commit_error(&result);
    assert!(matches!(kind, CommitFailure::Rejected(_)), "{kind:?}");
    let error = result.as_ref().expect_err("the commit error");
    let rendered = format!("{error} {error:?}");
    for marker in [PAYLOAD_MARKER, KEY_MARKER, "order-1"] {
        assert!(
            !rendered.contains(marker),
            "the error must not carry the record's {marker}: {rendered}"
        );
    }
}

/// A final commit the coordinator refuses with a rebalance in progress, as a
/// commit made while another member's leave rebalances the group is, is
/// tried again once the consumer has polled with its assignment paused,
/// inside the shutdown deadline. The second try lands and the member ends
/// clean, where it ended with `ShoveError::Commit` before.
#[tokio::test]
async fn a_final_commit_refused_while_the_group_rebalances_is_tried_again() {
    let mock = mock_cluster();
    let (result, took) = stop_a_consumer_after_one_record(&mock, |mock| {
        mock.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS],
        );
    })
    .await;

    assert!(
        result.is_ok(),
        "the second try lands and the member ends clean: {result:?}"
    );
    assert!(
        took < shutdown_commit_deadline(),
        "the retry stays inside the shutdown deadline, took {took:?}"
    );
    assert_eq!(
        committed_position(&mock.bootstrap_servers()).await,
        Some(1),
        "the acknowledged record's position is the group's"
    );
}

/// A final commit refused for a stale generation is tried again the same
/// way, and the member ends clean.
#[tokio::test]
async fn a_final_commit_refused_for_a_stale_generation_is_tried_again() {
    let mock = mock_cluster();
    let (result, took) = stop_a_consumer_after_one_record(&mock, |mock| {
        mock.request_errors(
            RDKafkaApiKey::OffsetCommit,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION],
        );
    })
    .await;

    assert!(result.is_ok(), "the member ends clean: {result:?}");
    assert!(
        took < shutdown_commit_deadline(),
        "the retry stays inside the shutdown deadline, took {took:?}"
    );
    assert_eq!(
        committed_position(&mock.bootstrap_servers()).await,
        Some(1),
        "the acknowledged record's position is the group's"
    );
}

/// An OffsetCommit held past the shutdown deadline ends the consumer within
/// that deadline plus a margin, with `Commit` of the `Deadline` kind carrying
/// the deadline: the result is unknown, the detached thread may still land
/// the commit. The mock broker's round-trip time is raised past the deadline
/// just before the stop, so the final `Sync` commit has no answer within it.
#[tokio::test]
async fn a_final_commit_past_the_deadline_ends_the_consumer_with_the_deadline_kind() {
    let mock = mock_cluster();
    let (result, took) = stop_a_consumer_after_one_record(&mock, hold_every_answer).await;

    let kind = the_commit_error(&result);
    let CommitFailure::Deadline(deadline) = kind else {
        panic!("a commit without an answer is reported as Deadline: {kind:?}");
    };
    assert_eq!(*deadline, shutdown_commit_deadline());
    assert!(
        took + Duration::from_secs(1) >= *deadline,
        "the consumer waits out the deadline before it gives up, took {took:?}"
    );
    assert!(
        took < *deadline + DEADLINE_MARGIN,
        "the consumer returns within the deadline plus a margin, took {took:?}"
    );
}

/// A group run sees the same failure through its outcome: the member's
/// `Commit` counts one under `errors`, so `is_clean()` is false and
/// `exit_code()` is 1, where 0.15.0 reported a clean run. The run still ends
/// on its stop signal and within its drain.
#[tokio::test]
async fn a_group_run_counts_a_rejected_final_commit_as_one_error() {
    let mock = mock_cluster();
    let outcome = stop_a_group_after_one_record(&mock, DRAIN_TIMEOUT, reject_every_commit).await;

    assert_eq!(outcome.errors, 1, "{outcome:?}");
    assert_eq!(outcome.panics, 0, "{outcome:?}");
    assert!(!outcome.timed_out, "{outcome:?}");
    assert!(!outcome.is_clean(), "{outcome:?}");
    assert_eq!(outcome.exit_code(), 1);
}

/// A drain that times out while the final commit is still waiting for its
/// answer aborts the member and reports `timed_out`, as in 0.15.0: the
/// aborted member records no error, and the outcome says the drain did not
/// finish. The mock broker's round-trip time is raised past the drain, so
/// neither the commit nor the drain can finish in time.
#[tokio::test]
async fn a_drain_that_times_out_during_the_final_commit_still_reports_timed_out() {
    const SHORT_DRAIN: Duration = Duration::from_secs(3);
    let mock = mock_cluster();
    let outcome = stop_a_group_after_one_record(&mock, SHORT_DRAIN, hold_every_answer).await;

    assert!(outcome.timed_out, "{outcome:?}");
    assert_eq!(
        outcome.errors, 0,
        "an aborted member records no error: {outcome:?}"
    );
    assert_eq!(outcome.exit_code(), 3);
}
