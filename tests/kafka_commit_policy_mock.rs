//! `CommitPolicy::PerRecord` on the concurrent Kafka consumer, against
//! librdkafka's mock cluster (`rdkafka::mocking`), which serves the consumer
//! group protocol, offset commits and request tracking from a localhost
//! listener and needs no Docker.
//!
//! Under `PerRecord` every completion is committed synchronously and the
//! next record is handed out only once the coordinator has confirmed that
//! commit; the assignment is paused meanwhile and the loop keeps polling.
//! These tests count the requests the mock broker receives, kill a member
//! the moment it holds the record behind a confirmed completion, refuse
//! every commit until the fenced consumer detector fires, hold one answer
//! while a handler runs past the member's poll interval, stop a member while
//! its commit is in flight, and move the group's position back behind a
//! killed member. The mock answers a connection in order, so an answer held
//! past the 10 s session timeout starves the heartbeats behind it and the
//! coordinator drops the member: a rebalance during a pause and a stop
//! against a frozen coordinator are therefore proved against a real broker,
//! in `kafka_rebalance.rs` and `kafka_integration.rs`. The mock's request tracking and its per-request answer delay
//! have no wrapper in `rdkafka::mocking`, so this file reads them through
//! `rdkafka::bindings`, in `Mock`.
//!
//! The topic is bound with `external()`, because the mock broker has no
//! CreateTopics API: each test creates the topic through the mock API, as
//! infra would, and produces through a raw rdkafka producer.
//!
//! The last section covers a republish and a dead letter that cannot be
//! published, under either commit policy, and a republish that waits out its
//! hold. A publish that may succeed later ends the member, which reconnects
//! and is handed the record again; one that can never succeed sends the
//! record to its DLQ, or discards it when the dead letter cannot be
//! published either. The record behind a waiting republish waits for its
//! permit while the member keeps polling and committing, and a revoke of the
//! record's partition ends the hold with nothing published. Those topics
//! are shove-owned, because a republish writes into its topic, and are
//! created through the mock API as well.
//!
//! A section runs a sequenced consumer against the same mock: a streak of
//! rejected commits fences it, and it reconnects once the close that error
//! exit makes has had its LeaveGroup answered; a stop that finds its commit
//! in flight unanswered reports the share of the shutdown deadline it
//! waited, as the standard consumer's does under `Interval`; and a direct
//! `run_fifo` whose future is dropped stops its shard, which leaves its
//! group.
//!
//! A section runs the DLQ drain against the same mock: a `handle_dead` that
//! panics or hangs costs the drain that one call and moves no handler
//! metric, one under `without_handler_timeout` is waited for past the
//! default timeout, the token of its options stops it as the client's token
//! does, and its commits are spaced by the fixed interval, re-offered when
//! refused and carried by the final commit at its stop. A drain whose task
//! is aborted, or that ends a connection on an error, closes its consumer
//! off the runtime thread.
//!
//! A section takes the broker down for a few seconds, which a settled member
//! rides out on the client it started with and a member fresh from a
//! rebalance reconnects through, and stops a member whose handler holds its
//! only permit past the poll interval, whose drain keeps polling through a
//! rebalance.
//!
//! A section stops a standard and a sequenced member while a backlog is
//! queued behind the record in hand, and times the commit the gate holds
//! back once a member has sat idle, on both.
//!
//! The final section runs a broadcast subscription against the same mock:
//! whether it stops or ends on an error, it closes its consumer on a thread
//! of its own, and with no thread to be had the handle is leaked rather than
//! closed on the runtime thread.
//!
//! `test-support` gates the seams this file reads. The Kafka coverage row
//! enables it and runs this suite; the schema-registry row compiles the
//! binary too, but its test filter does not select it.

#![cfg(all(feature = "kafka", feature = "test-support"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::os::raw::c_int;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(feature = "metrics")]
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use rdkafka::bindings::{
    rd_kafka_handle_mock_cluster, rd_kafka_mock_broker_push_request_error_rtts,
    rd_kafka_mock_get_requests, rd_kafka_mock_request_api_key, rd_kafka_mock_request_destroy_array,
    rd_kafka_mock_request_timestamp, rd_kafka_mock_start_request_tracking,
};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{
    BaseProducer, DefaultProducerContext, FutureProducer, FutureRecord, Producer,
};
use rdkafka::types::{RDKafkaApiKey, RDKafkaMockCluster, RDKafkaRespErr};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use shove::broadcast::BroadcastSubscriber;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::MessageHandler;
use shove::kafka::{
    CommitPolicy, KafkaClient, KafkaConfig, KafkaConsumer, KafkaConsumerGroup,
    KafkaConsumerGroupConfig, client_id_probe, commit_gate_probe, fence_probe,
    final_commit_spawn_probe, housekeeping_interval_for_test, pending_commit_budget_for_test,
    per_record_probe, permit_wait_probe, put_back_probe, shutdown_commit_deadline_for_test,
};
use shove::markers::Kafka;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::{SequenceFailure, TopologyBuilder};
use shove::{CommitFailure, DEFAULT_HANDLER_TIMEOUT, SequencedTopic, ShoveError};
use tokio::sync::{Notify, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const TOPIC: &str = "kafka-commit-policy-mock";
/// The group the topic's default configuration joins: `{queue}-consumer`.
const GROUP_ID: &str = "kafka-commit-policy-mock-consumer";
/// The mock broker's id with one broker configured.
const BROKER_ID: i32 = 1;
/// A record reaches the handler on a mock cluster in well under this.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// The shortest `max.poll.interval.ms` librdkafka admits, the pinned
/// `session.timeout.ms`; a member that stops polling for longer leaves its
/// group.
const SHORT_POLL_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
}

shove::define_topic!(
    OrdersTopic,
    Order,
    TopologyBuilder::new(TOPIC).external().build()
);

/// Acknowledges every record and records its id with the instant it
/// arrived, so a test can wait for a delivery with a bound and measure the
/// gap between two. `hold` keeps the record at that index in the handler
/// for that long before acknowledging it.
#[derive(Clone, Default)]
struct Acking {
    seen: Arc<Mutex<Vec<(String, Instant)>>>,
    hold: Option<(usize, Duration)>,
}

impl Acking {
    fn ids(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn count(&self) -> usize {
        self.seen.lock().expect("handler mutex poisoned").len()
    }

    fn arrived_at(&self, index: usize) -> Instant {
        self.seen.lock().expect("handler mutex poisoned")[index].1
    }
}

impl MessageHandler<OrdersTopic> for Acking {
    type Context = ();
    async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        let index = self.count();
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .push((msg.id, Instant::now()));
        if let Some((held, hold)) = self.hold
            && held == index
        {
            tokio::time::sleep(hold).await;
        }
        Outcome::Ack
    }
}

/// Acknowledges every record before the one at `index`, and on that one
/// signals `holding` and never returns: the member is killed while it holds
/// that record.
#[derive(Clone)]
struct HoldAt {
    index: usize,
    earlier: Acking,
    holding: Arc<Notify>,
}

impl MessageHandler<OrdersTopic> for HoldAt {
    type Context = ();
    async fn handle(&self, msg: Order, meta: MessageMetadata, ctx: &()) -> Outcome {
        if self.earlier.count() < self.index {
            return self.earlier.handle(msg, meta, ctx).await;
        }
        self.holding.notify_one();
        std::future::pending().await
    }
}

/// One mock broker with the topic created, as infra would create it, and
/// the two mock features `rdkafka::mocking` does not wrap: request tracking,
/// which counts the requests the broker received by API key, and a
/// per-request answer delay, which holds one OffsetCommit answer without
/// touching fetches or the group protocol on other connections.
///
/// The cluster is the one a producer configured with
/// `test.mock.num.brokers` owns, so its raw handle is reachable through
/// `rd_kafka_handle_mock_cluster` beside the safe wrapper the same client
/// hands out. The producer produces nothing; it lives for the cluster.
struct Mock {
    owner: BaseProducer,
    cluster: *mut RDKafkaMockCluster,
}

impl Mock {
    fn start() -> Self {
        let owner: BaseProducer = ClientConfig::new()
            .set("test.mock.num.brokers", "1")
            .create()
            .expect("a producer that owns a mock cluster");
        let cluster = unsafe { rd_kafka_handle_mock_cluster(owner.client().native_ptr()) };
        assert!(!cluster.is_null(), "the producer owns a mock cluster");
        let mock = Self { owner, cluster };
        mock.api()
            .create_topic(TOPIC, 1, 1)
            .expect("create the topic through the mock API");
        mock
    }

    /// The safe wrapper over the same cluster.
    fn api(&self) -> MockCluster<'_, DefaultProducerContext> {
        self.owner
            .client()
            .mock_cluster()
            .expect("the producer owns a mock cluster")
    }

    fn bootstrap(&self) -> String {
        self.api().bootstrap_servers()
    }

    /// Record every request from here on; see `requests_of`.
    fn track_requests(&self) {
        unsafe { rd_kafka_mock_start_request_tracking(self.cluster) }
    }

    /// How many requests of `api_key` the broker has received since
    /// `track_requests`, whichever client sent them.
    fn requests_of(&self, api_key: RDKafkaApiKey) -> usize {
        let mut count = 0usize;
        let requests = unsafe { rd_kafka_mock_get_requests(self.cluster, &mut count) };
        if requests.is_null() {
            return 0;
        }
        let wanted = i16::from(api_key);
        let matching = (0..count)
            .filter(|&i| unsafe { rd_kafka_mock_request_api_key(*requests.add(i)) } == wanted)
            .count();
        unsafe { rd_kafka_mock_request_destroy_array(requests, count) };
        matching
    }

    fn offset_commit_requests(&self) -> usize {
        self.requests_of(RDKafkaApiKey::OffsetCommit)
    }

    /// When each request of `api_key` reached the broker since
    /// `track_requests`, in arrival order, in microseconds of the mock's
    /// clock.
    fn request_timestamps(&self, api_key: RDKafkaApiKey) -> Vec<i64> {
        let mut count = 0usize;
        let requests = unsafe { rd_kafka_mock_get_requests(self.cluster, &mut count) };
        if requests.is_null() {
            return Vec::new();
        }
        let wanted = i16::from(api_key);
        let stamps = (0..count)
            .filter_map(|i| {
                let request = unsafe { *requests.add(i) };
                (unsafe { rd_kafka_mock_request_api_key(request) } == wanted)
                    .then(|| unsafe { rd_kafka_mock_request_timestamp(request) })
            })
            .collect();
        unsafe { rd_kafka_mock_request_destroy_array(requests, count) };
        stamps
    }

    /// Refuse the next `count` OffsetCommit requests with an error
    /// librdkafka neither retries nor rejoins the group over; the ones after
    /// are accepted.
    fn reject_commits(&self, count: usize) {
        let errors = vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED; count];
        self.api()
            .request_errors(RDKafkaApiKey::OffsetCommit, &errors);
    }

    /// Hold the answer to the next OffsetCommit for `delay`, then answer it
    /// with no error. Only that one request is affected: the broker answers
    /// a connection's requests in order, so a later OffsetCommit on the same
    /// connection waits behind it, but fetches and the group protocol on
    /// other connections do not.
    fn delay_next_offset_commit_answer(&self, delay: Duration) {
        self.answer_next_offset_commit_late(delay, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
    }

    /// Hold the answer to the next OffsetCommit for `delay`, then answer it
    /// with `err`: a rejection that arrives late, when `err` is one.
    fn answer_next_offset_commit_late(&self, delay: Duration, err: RDKafkaRespErr) {
        self.answer_next_late(RDKafkaApiKey::OffsetCommit, delay, err);
    }

    /// Hold the answer to the next request of `api_key` for `delay`, then
    /// answer it with `err`. The broker's own stack for `api_key` then
    /// shadows the cluster-wide one `request_errors` pushes onto, so a test
    /// uses one of the two per API key.
    fn answer_next_late(&self, api_key: RDKafkaApiKey, delay: Duration, err: RDKafkaRespErr) {
        let delay_ms = c_int::try_from(delay.as_millis()).expect("delay fits a C int");
        let err = unsafe {
            rd_kafka_mock_broker_push_request_error_rtts(
                self.cluster,
                BROKER_ID,
                i16::from(api_key),
                1,
                err as c_int,
                delay_ms,
            )
        };
        assert_eq!(
            err,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
            "push the delayed answer onto the mock broker"
        );
    }

    /// Accept every OffsetCommit from here on, dropping what is left of a
    /// `reject_commits` refusal.
    fn accept_commits(&self) {
        self.api().clear_request_errors(RDKafkaApiKey::OffsetCommit);
    }

    /// Answer the next `count` Produce requests with `err`, whichever client
    /// sends them; the ones after are accepted.
    fn refuse_produce(&self, err: RDKafkaRespErr, count: usize) {
        self.api()
            .request_errors(RDKafkaApiKey::Produce, &vec![err; count]);
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

async fn connect(bootstrap: &str) -> KafkaClient {
    KafkaClient::connect_with_retry(&KafkaConfig::new(bootstrap), 10)
        .await
        .expect("connect to the mock cluster")
}

/// Produces `ids` in order, through a raw rdkafka producer, as the topic's
/// JSON.
async fn produce(bootstrap: &str, ids: &[&str]) {
    produce_on(bootstrap, TOPIC, ids).await;
}

/// Produces one record onto the topic with `payload` as is, so a test can
/// put a record the topic's codec cannot decode in front of valid ones.
async fn produce_raw(bootstrap: &str, payload: Vec<u8>) {
    produce_raw_on(bootstrap, TOPIC, &payload).await;
}

/// `produce` onto another topic of the same shape.
async fn produce_on(bootstrap: &str, topic: &str, ids: &[&str]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    for id in ids {
        let payload = serde_json::to_vec(&Order { id: (*id).into() }).expect("encode the record");
        producer
            .send(
                FutureRecord::to(topic).key(*id).payload(&payload),
                Duration::from_secs(10),
            )
            .await
            .expect("produce to the mock cluster");
    }
}

/// The group's committed position on the topic's one partition, read
/// through a raw consumer that never joins the group. `None` before the
/// first accepted commit.
async fn committed_position(bootstrap: &str) -> Option<i64> {
    committed_position_on(bootstrap, TOPIC, GROUP_ID, 0).await
}

/// [`committed_position`] for any topic, group and partition.
async fn committed_position_on(
    bootstrap: &str,
    topic: &'static str,
    group: &'static str,
    partition: i32,
) -> Option<i64> {
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", group)
            .create()
            .expect("probe consumer");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(topic, partition);
        probe
            .committed_offsets(tpl, Duration::from_secs(5))
            .expect("read the committed offsets")
            .elements()
            .iter()
            .find(|e| e.partition() == partition)
            .and_then(|e| match e.offset() {
                Offset::Offset(offset) => Some(offset),
                _ => None,
            })
    })
    .await
    .expect("probe task")
}

/// Moves the group's committed position on the topic's one partition to
/// `offset`, through a raw consumer that joins the group for the commit
/// and leaves again: what a commit the broker applies late does to the
/// position. The mock refuses a commit from a consumer that is not a
/// member, so the committer joins; it is called once the member under test
/// is gone, so the join and the leave rebalance nobody.
async fn move_committed_position(bootstrap: &str, offset: i64) {
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        // The member's own assignor and session timeout, so the group sees
        // one protocol and one timing.
        let committer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", GROUP_ID)
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "false")
            .create()
            .expect("outside committer");
        committer
            .subscribe(&[TOPIC])
            .expect("subscribe the committer");
        let joined_by = std::time::Instant::now() + DELIVERY_TIMEOUT;
        while committer.assignment().expect("read the assignment").count() == 0 {
            assert!(
                std::time::Instant::now() < joined_by,
                "the committer joins the group within the bound"
            );
            let _ = committer.poll(Duration::from_millis(100));
        }
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(TOPIC, 0, Offset::Offset(offset))
            .expect("one partition");
        committer
            .commit(&tpl, CommitMode::Sync)
            .expect("move the committed position");
        drop(committer);
    })
    .await
    .expect("outside committer task");
}

/// Polls the broker until the group's committed position is `expected`.
async fn wait_for_committed_position(bootstrap: &str, expected: i64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while committed_position(bootstrap).await != Some(expected) {
        assert!(
            Instant::now() < deadline,
            "the broker did not accept the commit at {expected} within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One sequential direct consumer on the topic under `policy`: the one
/// permit `PerRecord` needs, and the same shape for the `Interval` control.
fn options(policy: CommitPolicy, shutdown: CancellationToken) -> ConsumerOptions<Kafka> {
    ConsumerOptions::<Kafka>::new()
        .with_prefetch_count(1)
        .with_concurrent_processing(true)
        .with_commit_policy(policy)
        .with_shutdown(shutdown)
}

/// Starts a direct consumer on the topic with `handler` and `options`, and
/// returns its run task and the token that stops it.
fn start<H>(
    client: KafkaClient,
    handler: H,
    options: impl FnOnce(CancellationToken) -> ConsumerOptions<Kafka>,
) -> (
    tokio::task::JoinHandle<Result<(), ShoveError>>,
    CancellationToken,
)
where
    H: MessageHandler<OrdersTopic, Context = ()> + 'static,
{
    let shutdown = CancellationToken::new();
    let opts = options(shutdown.clone());
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run::<OrdersTopic, _>(handler, (), opts)
            .await
    });
    (run, shutdown)
}

/// Starts a one-member group on the topic under `config`, through the
/// registry path, where the `test-support` seams on the group config apply.
/// Returns the broker to close, the run task and the token that stops it.
async fn start_group<H>(
    client: KafkaClient,
    handler: H,
    config: KafkaConsumerGroupConfig,
) -> (
    Broker<Kafka>,
    tokio::task::JoinHandle<shove::SupervisorOutcome>,
    CancellationToken,
)
where
    H: MessageHandler<OrdersTopic, Context = ()> + Clone + 'static,
{
    let broker = Broker::<Kafka>::from_client(client);
    let mut group = broker.consumer_group();
    group
        .register::<OrdersTopic, _>(ConsumerGroupConfig::new(config), move || handler.clone())
        .await
        .expect("register on the mock cluster");
    let token = group.cancellation_token();
    let run = tokio::spawn(
        group.run_until_timeout(token.clone().cancelled_owned(), Duration::from_secs(60)),
    );
    (broker, run, token)
}

/// Six records, six completions, six OffsetCommit requests: under
/// `PerRecord` every completion is committed, and confirmed, before the next
/// record is handed out, so the broker sees one commit per record and
/// nothing is left for a later drain.
#[tokio::test]
async fn per_record_issues_one_offset_commit_per_completion() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2", "o3", "o4", "o5", "o6"]).await;
    mock.track_requests();

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    wait_until(
        || handler.count() == 6,
        DELIVERY_TIMEOUT,
        "six records reaching the handler",
    )
    .await;
    wait_for_committed_position(&bootstrap, 6, DELIVERY_TIMEOUT).await;
    assert_eq!(
        mock.offset_commit_requests(),
        6,
        "one OffsetCommit per completion, none merged and none re-offered"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// The control: six records under `Interval` cost one commit inside the
/// window and one at the stop, against the six a `PerRecord` member makes.
/// The window is twice the test's own delivery bound, so every schedule the
/// test accepts falls inside it: the first completion finds the gate open
/// and is committed at once, the rest wait for the window, and the stop's
/// final commit carries them. An exact count inside a window shorter than
/// the bound depended on the runner.
#[tokio::test]
async fn interval_commits_once_inside_the_window_and_once_at_the_stop_over_six_records() {
    const INTERVAL: Duration = DELIVERY_TIMEOUT.saturating_mul(2);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2", "o3", "o4", "o5", "o6"]).await;
    mock.track_requests();

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::Interval(INTERVAL), token)
    });

    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the first completion's commit reaching the broker",
    )
    .await;
    wait_until(
        || handler.count() == 6,
        DELIVERY_TIMEOUT,
        "six records reaching the handler",
    )
    .await;
    assert_eq!(
        mock.offset_commit_requests(),
        1,
        "the completions after the first wait for the window"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the final commit carries the rest"
    );
    assert_eq!(committed_position(&bootstrap).await, Some(6));
}

/// A member killed the moment it holds the second record replays nothing of
/// the first on restart. Under `PerRecord` the second record reaches the
/// handler only once the first record's commit is confirmed, so holding it
/// is the proof that the commit landed; the kill is an abort of the run
/// task, with no final commit. A fresh member of the group is handed the
/// held record and nothing before it. A replay of the first record would
/// arrive before the second, in offset order, so the second record's commit
/// landing at the broker bounds the negative: once the position is 2,
/// nothing earlier is still to come.
#[tokio::test]
async fn a_member_killed_holding_the_record_behind_a_confirmed_completion_replays_nothing() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;

    let holding = Arc::new(Notify::new());
    let handler = HoldAt {
        index: 1,
        earlier: Acking::default(),
        holding: holding.clone(),
    };
    let (run, _shutdown) = start(connect(&bootstrap).await, handler.clone(), |token| {
        options(CommitPolicy::PerRecord, token)
    });

    tokio::time::timeout(DELIVERY_TIMEOUT, holding.notified())
        .await
        .expect("the handler holds the second record");
    assert_eq!(handler.earlier.ids(), vec!["o1".to_string()]);
    run.abort();
    let _ = run.await;
    assert_eq!(
        committed_position(&bootstrap).await,
        Some(1),
        "the first record's commit was confirmed before the second was handed out"
    );

    let restarted = Acking::default();
    let h = restarted.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_until(
        || restarted.count() >= 1,
        DELIVERY_TIMEOUT,
        "the restarted member receiving the held record",
    )
    .await;
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        restarted.ids(),
        vec!["o2".to_string()],
        "only the record the killed member held is redelivered"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A rejected commit is re-offered until the fenced consumer detector
/// fires. Every OffsetCommit is refused with an error librdkafka does not
/// retry, so the first completion's commit is rejected at once and
/// re-offered at the default interval, with the assignment paused, until
/// the fence ends the connection. The member then reconnects, joins the
/// group again and, with nothing committed, is handed the first record
/// again: that second delivery is the proof the fence fired, and nothing
/// else was handed out before it. The floor is lowered through the group
/// config's test seam, so the scenario takes seconds rather than the minute
/// the pinned floor needs.
#[tokio::test]
async fn a_rejected_commit_is_re_offered_until_the_fence_fires() {
    const FLOOR: Duration = Duration::from_secs(5);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    mock.reject_commits(512);

    let handler = Acking::default();
    let h = handler.clone();
    let (broker, run, token) = start_group(
        connect(&bootstrap).await,
        h,
        KafkaConsumerGroupConfig::new(1..=1)
            .with_prefetch_count(1)
            .with_concurrent_processing(true)
            .with_commit_policy(CommitPolicy::PerRecord)
            .with_commit_fence_floor_for_test(FLOOR),
    )
    .await;

    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;
    let first_delivery = handler.arrived_at(0);
    assert_eq!(
        fence_probe::last_threshold(),
        Some(FLOOR),
        "PerRecord judges rejected commits by the floor"
    );

    wait_until(
        || handler.count() >= 2,
        FLOOR + DELIVERY_TIMEOUT,
        "the reconnected member receiving the first record again",
    )
    .await;
    let until_fence = handler
        .arrived_at(1)
        .saturating_duration_since(first_delivery);
    assert!(
        until_fence >= FLOOR,
        "the fence waits out its floor, fired after {until_fence:?}"
    );
    assert_eq!(
        &handler.ids()[..2],
        &["o1".to_string(), "o1".to_string()],
        "nothing is handed out behind the rejected position; the reconnect replays it"
    );
    assert!(
        mock.offset_commit_requests() >= 3,
        "the rejected commit was re-offered, requests: {}",
        mock.offset_commit_requests()
    );
    // The re-offers are spaced by the default interval the page states,
    // 500 ms, so the loop polls between them: every gap between two
    // OffsetCommit requests at the broker is at least that, less a margin
    // for the clock. Read before the stop, whose final commit follows the
    // last re-offer at no such distance.
    let stamps = mock.request_timestamps(RDKafkaApiKey::OffsetCommit);
    for pair in stamps.windows(2) {
        let gap = Duration::from_micros(
            u64::try_from(pair[1] - pair[0]).expect("timestamps in arrival order"),
        );
        assert!(
            gap >= Duration::from_millis(450),
            "re-offers are spaced by the default interval, gap {gap:?} in {stamps:?}"
        );
    }
    assert_eq!(
        committed_position(&bootstrap).await,
        None,
        "no commit was accepted"
    );

    // The fence reconnected the member rather than ending it, which the
    // second delivery above proved. The stop then makes the final commit,
    // the mock refuses it like every other, and the member reports that
    // one refusal under `errors` instead of ending clean.
    token.cancel();
    let outcome = run.await.expect("the run task completes");
    assert_eq!(outcome.errors, 1, "{outcome:?}");
    assert_eq!(outcome.panics, 0, "{outcome:?}");
    assert!(!outcome.timed_out, "{outcome:?}");
    broker.close().await;
}

/// A rejected commit is re-offered until one is accepted, and only then is
/// the next record handed out. The mock refuses the first two OffsetCommit
/// requests and accepts the third, and the handler holds the second record
/// long enough to count the requests while it is held: three for the first
/// record, one per attempt, then one for the second.
#[tokio::test]
async fn a_rejected_commit_recovers_on_a_re_offer_before_the_next_record_is_handed_out() {
    const HOLD: Duration = Duration::from_secs(2);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    mock.reject_commits(2);

    let handler = Acking {
        hold: Some((1, HOLD)),
        ..Acking::default()
    };
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    wait_until(
        || handler.count() == 2,
        DELIVERY_TIMEOUT,
        "both records reaching the handler",
    )
    .await;
    assert_eq!(
        mock.offset_commit_requests(),
        3,
        "two refused attempts and the accepted re-offer, before the second record"
    );
    assert_eq!(committed_position(&bootstrap).await, Some(1));

    wait_for_committed_position(&bootstrap, 2, HOLD + DELIVERY_TIMEOUT).await;
    assert_eq!(mock.offset_commit_requests(), 4);
    assert_eq!(handler.ids(), vec!["o1".to_string(), "o2".to_string()]);

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A commit no thread could carry is never taken as confirmed. With every
/// thread refused, the first record's commit reaches no broker, the
/// position stays unset and the next record is held back; once threads are
/// allowed again the re-offer lands and the member goes on, with one commit
/// per record at the broker.
#[tokio::test]
async fn a_commit_without_a_thread_is_not_confirmed_and_is_re_offered() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    final_commit_spawn_probe::refuse_threads(true);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;
    assert_eq!(
        committed_position(&bootstrap).await,
        None,
        "a commit without a thread confirms nothing"
    );
    assert_eq!(mock.offset_commit_requests(), 0);
    assert_eq!(handler.ids(), vec!["o1".to_string()]);

    final_commit_spawn_probe::refuse_threads(false);
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(handler.ids(), vec!["o1".to_string(), "o2".to_string()]);
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the refused attempts sent nothing; one commit per record once threads were allowed"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A record dropped before the handler, here one the codec cannot decode,
/// is committed like a completion and holds the next record back the same
/// way. Every commit thread is refused, so the drop's commit confirms
/// nothing and is re-offered with the assignment paused: the valid record
/// behind it must not reach the handler meanwhile. Once threads are allowed
/// the re-offer lands, and the valid record arrives with the drop's
/// position already committed. Red before: the drop ran no pause, so the
/// valid record reached the handler with nothing committed.
#[tokio::test]
async fn a_record_dropped_before_the_handler_holds_the_next_record_until_its_commit_is_accepted() {
    /// Four re-offer windows: long enough for the drop's commit to be
    /// re-offered, short enough to keep the test to its scenario.
    const HOLD: Duration = Duration::from_secs(2);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    produce(&bootstrap, &["o2"]).await;
    mock.track_requests();
    final_commit_spawn_probe::refuse_threads(true);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    // The drop's commit is attempted, has no thread, and is re-offered;
    // nothing reaches the broker and the valid record waits behind it.
    wait_until(
        || per_record_probe::commit_attempts() >= 1,
        DELIVERY_TIMEOUT,
        "the drop's commit being attempted",
    )
    .await;
    tokio::time::sleep(HOLD).await;
    assert_eq!(
        handler.count(),
        0,
        "the valid record waits behind the unconfirmed drop: {:?}",
        handler.ids()
    );
    assert_eq!(
        mock.offset_commit_requests(),
        0,
        "no thread, so nothing reached the broker"
    );
    assert_eq!(committed_position(&bootstrap).await, None);
    assert!(
        per_record_probe::commit_attempts() >= 2,
        "the drop's commit was re-offered meanwhile, attempts {}",
        per_record_probe::commit_attempts()
    );

    // Threads again: the re-offer lands, the assignment resumes, and the
    // valid record arrives behind a committed position.
    final_commit_spawn_probe::refuse_threads(false);
    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the valid record reaching the handler",
    )
    .await;
    assert_eq!(handler.ids(), vec!["o2".to_string()]);
    assert!(
        committed_position(&bootstrap).await.unwrap_or(0) >= 1,
        "the drop's position was committed before the valid record was handed out"
    );
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the drop's re-offer that landed, and the valid record's commit"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// The same drop with its commit rejected twice by the broker: the valid
/// record waits while the position is re-offered, and arrives once the
/// third attempt is accepted, behind a committed position. The valid record
/// is produced only once the first rejection is at the broker, so it meets
/// an idle member between re-offers and not the commit wait, whose own
/// put-back would pause for it. Red before: the drop ran no pause and the
/// rejection paused nothing, so the valid record reached the handler while
/// the drop's position was still rejected.
#[tokio::test]
async fn a_record_dropped_before_the_handler_holds_the_next_record_while_its_commit_is_rejected() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    mock.track_requests();
    mock.reject_commits(2);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    // The first rejection is at the broker; the valid record arrives behind
    // a rejected position, with the member between re-offers.
    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the drop's commit being rejected once",
    )
    .await;
    produce(&bootstrap, &["o2"]).await;

    // The second rejection, and the valid record still waiting.
    wait_until(
        || mock.offset_commit_requests() >= 2,
        DELIVERY_TIMEOUT,
        "the drop's commit being rejected twice",
    )
    .await;
    assert_eq!(
        handler.count(),
        0,
        "the valid record waits while the drop's position is rejected: {:?}",
        handler.ids()
    );
    assert_eq!(committed_position(&bootstrap).await, None);

    // The third attempt is accepted, and the valid record follows it.
    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "the valid record reaching the handler",
    )
    .await;
    assert_eq!(handler.ids(), vec!["o2".to_string()]);
    assert!(
        committed_position(&bootstrap).await.unwrap_or(0) >= 1,
        "the drop's position was accepted before the valid record was handed out"
    );
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        mock.offset_commit_requests(),
        4,
        "two rejections and the accepted re-offer for the drop, then the valid record's commit"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A commit whose answer is late holds the next record back, and the member
/// keeps polling while a handler holds its one permit. The member runs with
/// the shortest poll interval librdkafka admits and the first record's
/// handler holds it past that interval: a loop that stopped polling while
/// the permit is held would leave the group, rejoin, and be handed the
/// first record again. The mock then holds the commit's answer, for less
/// than the session timeout, because the mock answers a connection in
/// order and a longer hold would starve the heartbeats behind it. The
/// handler sees each record once, the second only after the answer, and
/// both commits land.
#[tokio::test]
async fn a_late_answer_holds_the_next_record_while_the_member_keeps_polling() {
    const HANDLER_HOLD: Duration = Duration::from_secs(12);
    const ANSWER_HOLD: Duration = Duration::from_secs(6);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    mock.delay_next_offset_commit_answer(ANSWER_HOLD);

    let handler = Acking {
        hold: Some((0, HANDLER_HOLD)),
        ..Acking::default()
    };
    let h = handler.clone();
    let (broker, run, token) = start_group(
        connect(&bootstrap).await,
        h,
        KafkaConsumerGroupConfig::new(1..=1)
            .with_prefetch_count(1)
            .with_concurrent_processing(true)
            .with_commit_policy(CommitPolicy::PerRecord)
            .with_max_poll_interval_for_test(SHORT_POLL_INTERVAL),
    )
    .await;

    wait_until(
        || handler.count() == 2,
        HANDLER_HOLD + ANSWER_HOLD + DELIVERY_TIMEOUT,
        "both records reaching the handler",
    )
    .await;
    let gap = handler
        .arrived_at(1)
        .saturating_duration_since(handler.arrived_at(0));
    assert!(
        gap >= HANDLER_HOLD + ANSWER_HOLD - Duration::from_millis(500),
        "the second record waits for the handler and then for the answer, gap {gap:?}"
    );
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        handler.ids(),
        vec!["o1".to_string(), "o2".to_string()],
        "each record once: the member kept its membership through both waits"
    );
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "one commit per record, the late one not re-offered"
    );

    token.cancel();
    let outcome = run.await.expect("the run task completes");
    assert!(outcome.is_clean(), "{outcome:?}");
    broker.close().await;
}

/// A stop that lands while a commit is in flight waits for that commit's
/// answer, inside the shutdown deadline, and only then makes the final
/// commit, which carries the position again as every final commit does;
/// `run` returns only once the consumer has closed, so the member has left
/// its group when it does. The mock holds the first OffsetCommit answer,
/// the stop fires once that request has reached the broker, and the request
/// counts at return say what happened.
#[tokio::test]
async fn a_stop_during_a_commit_waits_for_its_answer_and_leaves_the_group_before_returning() {
    const HELD: Duration = Duration::from_secs(6);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1"]).await;
    mock.track_requests();
    mock.delay_next_offset_commit_answer(HELD);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });

    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the record's commit reaching the broker",
    )
    .await;
    let stopped_at = Instant::now();
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
    let took = stopped_at.elapsed();

    assert!(
        took >= HELD - Duration::from_millis(500),
        "the stop waits for the commit in flight, took {took:?}"
    );
    assert!(
        took < shutdown_commit_deadline_for_test(),
        "the stop ends inside the shutdown deadline, took {took:?}"
    );
    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the commit in flight is waited for, then the final commit carries the position again"
    );
    assert!(
        mock.requests_of(RDKafkaApiKey::LeaveGroup) >= 1,
        "the member has left the group when run returns"
    );
    assert_eq!(committed_position(&bootstrap).await, Some(1));
}

/// Kafka orders OffsetCommit requests within one connection only, so a
/// commit librdkafka abandoned on a socket timeout and retried can be
/// applied by the broker after a later one, and no client can prevent that;
/// the Kafka page cites the lines. What it costs can be shown: the member
/// is killed while it holds the third record, with two committed, and the
/// group's position is then moved back from 2 to 1, as a late commit would
/// move it. The restarted member replays the record behind the moved-back
/// position and loses nothing, and its own next commit repairs the
/// position; a position moved back further replays further, by the same
/// mechanism.
#[tokio::test]
async fn a_position_moved_back_behind_the_member_costs_a_replay_and_never_a_loss() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2", "o3"]).await;

    let holding = Arc::new(Notify::new());
    let handler = HoldAt {
        index: 2,
        earlier: Acking::default(),
        holding: holding.clone(),
    };
    let (run, _shutdown) = start(connect(&bootstrap).await, handler.clone(), |token| {
        options(CommitPolicy::PerRecord, token)
    });
    tokio::time::timeout(DELIVERY_TIMEOUT, holding.notified())
        .await
        .expect("the handler holds the third record");
    assert_eq!(
        handler.earlier.ids(),
        vec!["o1".to_string(), "o2".to_string()],
        "two records committed, the third held"
    );
    assert_eq!(committed_position(&bootstrap).await, Some(2));
    run.abort();
    let _ = run.await;

    move_committed_position(&bootstrap, 1).await;
    assert_eq!(
        committed_position(&bootstrap).await,
        Some(1),
        "the position moved back, as a late commit would move it"
    );

    let restarted = Acking::default();
    let h = restarted.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_for_committed_position(&bootstrap, 3, DELIVERY_TIMEOUT).await;
    assert_eq!(
        restarted.ids(),
        vec!["o2".to_string(), "o3".to_string()],
        "the record behind the moved-back position replays, nothing is lost, and the position is repaired"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A `Retry` whose republish fails under `PerRecord` ends the member rather
/// than leave the record pinned behind the next one: with its offset in
/// flight and never completed, every later commit would confirm nothing and
/// a crash would replay everything behind it. On Kafka a retry republishes
/// the record onto its own topic after the hold delay; the mock refuses
/// every Produce request from the moment the records are in, so that
/// republish fails. The member ends with a connection error that names the
/// republish, the second record was never handed out, and no commit reached
/// the broker.
#[tokio::test]
async fn a_republish_that_fails_under_per_record_ends_the_member_before_the_next_record() {
    const RETRY_TOPIC: &str = "kafka-commit-policy-retry";
    shove::define_topic!(
        RetryTopic,
        Order,
        TopologyBuilder::new(RETRY_TOPIC)
            .hold_queue(Duration::from_millis(200))
            .build()
    );

    /// Records every record it is handed and asks for a retry each time.
    #[derive(Clone, Default)]
    struct Retrying {
        seen: Arc<Mutex<Vec<String>>>,
    }
    impl MessageHandler<RetryTopic> for Retrying {
        type Context = ();
        async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
            self.seen
                .lock()
                .expect("handler mutex poisoned")
                .push(msg.id);
            Outcome::Retry
        }
    }

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(RETRY_TOPIC, 1, 1)
        .expect("create the retry topic through the mock API");
    produce_on(&bootstrap, RETRY_TOPIC, &["o1", "o2"]).await;
    mock.track_requests();
    // Every Produce from here on is refused with an error the producer does
    // not retry, so the republish fails on each of its attempts.
    mock.api().request_errors(
        RDKafkaApiKey::Produce,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED; 32],
    );

    let handler = Retrying::default();
    let h = handler.clone();
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let client = connect(&bootstrap).await;
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run::<RetryTopic, _>(
                h,
                (),
                options(CommitPolicy::PerRecord, token).with_max_reconnect_attempts(1),
            )
            .await
    });

    let result = tokio::time::timeout(DELIVERY_TIMEOUT, run)
        .await
        .expect("the member ends within the bound")
        .expect("the run task completes");
    let err = result.expect_err("a failed republish ends the member");
    assert!(
        matches!(&err, ShoveError::Connection(msg)
            if msg.contains("retry republish") && msg.contains("reconnects")),
        "the error names the republish and the reconnect that redelivers it: {err}"
    );
    assert_eq!(
        *handler.seen.lock().expect("handler mutex poisoned"),
        vec!["o1".to_string()],
        "the second record was never handed out"
    );
    assert_eq!(
        mock.offset_commit_requests(),
        0,
        "nothing was committed behind the record"
    );
    drop(shutdown);
}

// ---------------------------------------------------------------------------
// A partition handed back paused: the assign drained at the top of a pass, or
// by the receive arm
// ---------------------------------------------------------------------------

const RETURN_TOPIC: &str = "kafka-commit-policy-mock-return";
/// The group the topic's default configuration joins: `{queue}-consumer`.
const RETURN_GROUP_ID: &str = "kafka-commit-policy-mock-return-consumer";
/// Two partitions, so that a second member's join moves exactly one.
const RETURN_PARTITIONS: [i32; 2] = [0, 1];
/// The per-record member's payload limit; a record padded past it is
/// dropped before the handler.
const SIZE_LIMIT: usize = 1024;
/// The returned partition's record reaches the handler in well under this
/// once the partition is resumed, and never while it stays paused.
const RESUME_TIMEOUT: Duration = Duration::from_secs(15);
/// Cycles the receive-arm test runs before it gives up on meeting an assign
/// in the receive arm; the housekeeping tick wins one in a hundred or so.
const RECEIVE_ARM_ATTEMPTS: u32 = 3;

shove::define_topic!(
    ReturnTopic,
    Order,
    TopologyBuilder::new(RETURN_TOPIC).external().build()
);

/// Acknowledges every record, records its id with the partition it came
/// from, and holds every `hold-{round}` record until `release` has reached
/// that round.
#[derive(Clone)]
struct Holding {
    seen: Arc<Mutex<Vec<(String, i32)>>>,
    release: watch::Receiver<u32>,
}

impl Holding {
    fn new(release: watch::Receiver<u32>) -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            release,
        }
    }

    fn seen(&self) -> Vec<(String, i32)> {
        self.seen.lock().expect("handler mutex poisoned").clone()
    }

    fn has(&self, id: &str) -> bool {
        self.seen().iter().any(|(seen, _)| seen == id)
    }
}

impl MessageHandler<ReturnTopic> for Holding {
    type Context = ();
    async fn handle(&self, msg: Order, meta: MessageMetadata, _: &()) -> Outcome {
        let partition = meta.partition.expect("Kafka fills the partition");
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .push((msg.id.clone(), partition));
        if let Some(round) = msg.id.strip_prefix("hold-") {
            let round: u32 = round.parse().expect("a hold record names its round");
            let mut release = self.release.clone();
            release
                .wait_for(|released| *released >= round)
                .await
                .expect("the release sender outlives the handler");
        }
        Outcome::Ack
    }
}

/// Produces one record pinned to `partition` of the return topic and
/// returns the offset the broker gave it.
async fn produce_pinned(bootstrap: &str, id: &str, partition: i32) -> i64 {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    let payload = serde_json::to_vec(&Order { id: id.into() }).expect("encode the record");
    let delivery = producer
        .send(
            FutureRecord::<(), Vec<u8>>::to(RETURN_TOPIC)
                .partition(partition)
                .payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
    assert_eq!(
        delivery.partition, partition,
        "the record lands where it was pinned"
    );
    delivery.offset
}

/// Starts a direct consumer on the return topic with `handler` and
/// `options`, and returns its run task and the token that stops it.
fn start_on_return_topic(
    client: KafkaClient,
    handler: Holding,
    options: impl FnOnce(CancellationToken) -> ConsumerOptions<Kafka>,
) -> (
    tokio::task::JoinHandle<Result<(), ShoveError>>,
    CancellationToken,
) {
    let shutdown = CancellationToken::new();
    let opts = options(shutdown.clone());
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run::<ReturnTopic, _>(handler, (), opts)
            .await
    });
    (run, shutdown)
}

/// Polls the broker until the group's committed position on `partition` of
/// the return topic is `expected`.
async fn wait_for_return_position(bootstrap: &str, partition: i32, expected: i64, what: &str) {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while committed_position_on(bootstrap, RETURN_TOPIC, RETURN_GROUP_ID, partition).await
        != Some(expected)
    {
        assert!(
            Instant::now() < deadline,
            "{what}: partition {partition} did not reach position {expected} within {DELIVERY_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The two-partition return topic with a one-permit member A on it, and the
/// cycle that hands A a partition back paused: A holds `hold-{round}` on
/// partition 0 and is paused over both partitions, B joins and the
/// cooperative rebalance moves one partition to it, A's hold ends and A
/// works through what it kept, B leaves, and the moved partition returns to
/// an idle, unpaused A with the pause flag it left with. Under `PerRecord`
/// taking the held record pauses A; under `Interval` the record behind it
/// waits for the permit, and the one after that is put back, which pauses
/// A. `produced` counts the records on each partition, so a committed
/// position can be waited for whoever consumed the records.
struct ReturnCycle {
    /// Lives for the cluster; the broker is reached through `bootstrap`.
    _mock: Mock,
    bootstrap: String,
    policy: CommitPolicy,
    a: Holding,
    run_a: tokio::task::JoinHandle<Result<(), ShoveError>>,
    token_a: CancellationToken,
    release: watch::Sender<u32>,
    produced: [i64; 2],
}

impl ReturnCycle {
    async fn start(policy: CommitPolicy) -> Self {
        let mock = Mock::start();
        let bootstrap = mock.bootstrap();
        mock.api()
            .create_topic(RETURN_TOPIC, 2, 1)
            .expect("create the return topic through the mock API");
        mock.track_requests();
        let (release, released) = watch::channel(0);
        // Member A: one permit, and a payload limit a padded record exceeds.
        let a = Holding::new(released);
        let (run_a, token_a) =
            start_on_return_topic(connect(&bootstrap).await, a.clone(), |token| {
                options(policy, token).with_max_message_size(SIZE_LIMIT)
            });
        Self {
            _mock: mock,
            bootstrap,
            policy,
            a,
            run_a,
            token_a,
            release,
            produced: [0, 0],
        }
    }

    fn produced_on(&self, partition: i32) -> i64 {
        self.produced[usize::try_from(partition).expect("a partition index")]
    }

    async fn produce(&mut self, id: &str, partition: i32) {
        produce_pinned(&self.bootstrap, id, partition).await;
        self.produced[usize::try_from(partition).expect("a partition index")] += 1;
    }

    /// One cycle, see the type's doc. Returns once A's assign event for the
    /// returned partition has been queued, with the moved and the kept
    /// partition.
    async fn hand_a_partition_back_paused(&mut self, round: u32) -> (i32, i32) {
        let hold = format!("hold-{round}");
        self.produce(&hold, 0).await;
        wait_until(
            || self.a.has(&hold),
            DELIVERY_TIMEOUT,
            "A holding its record",
        )
        .await;
        // One probe per partition, unconsumed while A is paused: whichever
        // partition moves to B, B's first record comes from it.
        let put_back = put_back_probe::permit_wait();
        for partition in RETURN_PARTITIONS {
            self.produce(&format!("probe-{round}-{partition}"), partition)
                .await;
        }
        // Under `Interval` the pause is the permit wait's: the first probe
        // waits for the permit the hold keeps, and the second is put back.
        if matches!(self.policy, CommitPolicy::Interval(_)) {
            wait_until(
                || put_back_probe::permit_wait() > put_back,
                DELIVERY_TIMEOUT,
                "A pausing while a probe waits for the permit",
            )
            .await;
        }

        // Member B joins, and the cooperative rebalance moves one partition
        // to it while A is paused.
        let b = Holding::new(self.release.subscribe());
        let (run_b, token_b) =
            start_on_return_topic(connect(&self.bootstrap).await, b.clone(), |token| {
                ConsumerOptions::<Kafka>::new().with_shutdown(token)
            });
        wait_until(
            || !b.seen().is_empty(),
            DELIVERY_TIMEOUT,
            "the rebalance moving a partition to B",
        )
        .await;
        let moved = b.seen()[0].1;
        let kept = RETURN_PARTITIONS
            .into_iter()
            .find(|&partition| partition != moved)
            .expect("two partitions");

        // The holds end. A commits what it kept and resumes its assignment,
        // which no longer holds the moved partition: that one keeps A's
        // pause flag. A then works through the kept partition, a commit per
        // record, and idles unpaused once the last is committed.
        self.release
            .send(round)
            .expect("the handlers hold the receiver");
        wait_for_return_position(
            &self.bootstrap,
            kept,
            self.produced_on(kept),
            "A working through the kept partition",
        )
        .await;

        // B leaves, and its partition comes back to A, paused. A's rejoin
        // ends with the assign the rebalance callback queues for the loop,
        // which the probe counts.
        let queued = per_record_probe::assigns_queued();
        token_b.cancel();
        run_b
            .await
            .expect("B's task completes")
            .expect("B ends clean");
        wait_until(
            || per_record_probe::assigns_queued() > queued,
            DELIVERY_TIMEOUT,
            "A's assign being queued",
        )
        .await;
        (moved, kept)
    }

    /// Drops an oversize record on `kept` before the handler, and waits for
    /// its drop to be committed: the next thing through A's `recv()`.
    async fn drop_an_oversize_record_on(&mut self, kept: i32) {
        self.produce(&"x".repeat(SIZE_LIMIT), kept).await;
        wait_for_return_position(
            &self.bootstrap,
            kept,
            self.produced_on(kept),
            "the oversize record's drop being committed",
        )
        .await;
        assert!(
            !self.a.seen().iter().any(|(id, _)| id.len() >= SIZE_LIMIT),
            "the oversize record never reached the handler"
        );
    }

    /// Produces `id` on the returned partition and waits for A to receive
    /// it: the proof that the partition was resumed.
    async fn prove_resumed(&mut self, moved: i32, id: &str) {
        self.produce(id, moved).await;
        wait_until(
            || self.a.has(id),
            RESUME_TIMEOUT,
            "the record on the returned partition reaching A",
        )
        .await;
    }

    async fn stop(self) {
        self.token_a.cancel();
        self.run_a
            .await
            .expect("A's task completes")
            .expect("A ends clean");
    }
}

/// A partition handed back to a per-record member that idles unpaused
/// arrives paused: the member paused its whole assignment while it held a
/// record, and librdkafka keeps a partition's pause flag across a revoke.
/// The assign event is drained by whichever arm runs next, and when the
/// next thing through `recv()` is a record on the member's other partition,
/// that is the receive arm. A record dropped before the handler, here one
/// over the size limit, is taken like any other: the pause the moment it is
/// taken and the resume once its drop is committed cover the whole
/// assignment, the returned partition included. The record produced onto
/// the returned partition afterwards is the proof: it reaches the handler
/// only once the partition is resumed. Red without the pause at the take,
/// where the drop ran no cycle and the partition stayed paused.
///
/// The drain-site probe says which drain took the assign. The oversize
/// record is produced as soon as the assign is queued, so the receive arm
/// drains it unless the housekeeping tick fires first, in which case the
/// cycle runs again; the top-of-pass path has its own test below.
#[tokio::test]
async fn a_partition_handed_back_paused_is_resumed_when_the_receive_arm_drains_the_assign() {
    let mut cycle = ReturnCycle::start(CommitPolicy::PerRecord).await;
    let mut met_in_the_receive_arm = false;
    for round in 1..=RECEIVE_ARM_ATTEMPTS {
        let (moved, kept) = cycle.hand_a_partition_back_paused(round).await;
        let at_top_of_pass = per_record_probe::reconciled_at_top_of_pass();
        let at_receive_arm = per_record_probe::assigns_drained_at_receive_arm();
        cycle.drop_an_oversize_record_on(kept).await;
        if per_record_probe::assigns_drained_at_receive_arm() > at_receive_arm {
            // The receive arm drained the assign ahead of the oversize
            // record, whose pause and resume cycle resumed the returned
            // partition with the rest.
            cycle
                .prove_resumed(moved, &format!("after-return-{round}"))
                .await;
            met_in_the_receive_arm = true;
            break;
        }
        assert!(
            per_record_probe::reconciled_at_top_of_pass() > at_top_of_pass,
            "the assign was reconciled by one drain or the other"
        );
        // The housekeeping tick came first and the top-of-pass drain took
        // the assign; the partition is resumed all the same. Again.
        cycle
            .prove_resumed(moved, &format!("after-return-{round}"))
            .await;
    }
    assert!(
        met_in_the_receive_arm,
        "the receive arm drained no assign in {RECEIVE_ARM_ATTEMPTS} cycles"
    );
    cycle.stop().await;
}

/// The same return with nothing through `recv()` afterwards: the assign
/// waits in the channel until the housekeeping tick, the one arm due on an
/// idle member, runs a pass, and the drain at the top of that pass
/// reconciles it. The record produced onto the returned partition after the
/// tick is the proof it was resumed, and the drain-site probe says which
/// drain did it. Red without the top-of-pass call: the assign is drained
/// and discarded there, and the record never arrives.
#[tokio::test]
async fn a_partition_handed_back_paused_is_resumed_when_a_pass_drains_the_assign() {
    let mut cycle = ReturnCycle::start(CommitPolicy::PerRecord).await;
    let (moved, _kept) = cycle.hand_a_partition_back_paused(1).await;
    let at_top_of_pass = per_record_probe::reconciled_at_top_of_pass();
    let at_receive_arm = per_record_probe::assigns_drained_at_receive_arm();

    // Nothing is produced, so the next pass is the housekeeping tick's.
    tokio::time::sleep(housekeeping_interval_for_test() + Duration::from_secs(1)).await;
    assert!(
        per_record_probe::reconciled_at_top_of_pass() > at_top_of_pass,
        "the housekeeping pass drained the assign and reconciled it"
    );
    assert_eq!(
        per_record_probe::assigns_drained_at_receive_arm(),
        at_receive_arm,
        "nothing came through recv meanwhile"
    );

    cycle.prove_resumed(moved, "after-return").await;
    cycle.stop().await;
}

/// The same return under `Interval`, where the pause is the permit wait's:
/// the probe behind the held record waits for the permit, and the probe
/// after it is put back with the assignment paused. The moved partition
/// keeps that pause flag across the revoke and comes back paused once B
/// leaves, and nothing in the member's own cycle resumes it, because a
/// record taken under `Interval` pauses nothing. The housekeeping pass
/// drains the assign and re-applies the member's intent, unpaused, over the
/// whole assignment, so the record produced onto the returned partition
/// afterwards arrives. Red while only `PerRecord` re-applied the intent.
#[tokio::test]
async fn interval_partition_revoked_while_paused_resumes_on_reassign() {
    let mut cycle = ReturnCycle::start(CommitPolicy::Interval(Duration::from_millis(500))).await;
    let (moved, _kept) = cycle.hand_a_partition_back_paused(1).await;
    let at_top_of_pass = per_record_probe::reconciled_at_top_of_pass();
    let at_receive_arm = per_record_probe::assigns_drained_at_receive_arm();

    // Nothing is produced, so the next pass is the housekeeping tick's.
    tokio::time::sleep(housekeeping_interval_for_test() + Duration::from_secs(1)).await;
    cycle.prove_resumed(moved, "after-return").await;
    assert!(
        per_record_probe::reconciled_at_top_of_pass() > at_top_of_pass,
        "the housekeeping pass drained the assign and reconciled it"
    );
    assert_eq!(
        per_record_probe::assigns_drained_at_receive_arm(),
        at_receive_arm,
        "nothing came through recv before that pass"
    );
    cycle.stop().await;
}

/// The receive-arm return under `Interval`: the assign is drained by the
/// receive arm, ahead of the record over the size limit on the kept
/// partition that brought it, and that drain re-applies the member's intent
/// itself. Under `Interval` nothing else would, because the dropped record
/// pauses and resumes nothing. The record produced onto the returned
/// partition afterwards is the proof. The housekeeping tick can take the
/// assign first, in which case the cycle runs again, as in the `PerRecord`
/// test above.
#[tokio::test]
async fn interval_partition_handed_back_paused_is_resumed_when_the_receive_arm_drains_the_assign() {
    let mut cycle = ReturnCycle::start(CommitPolicy::Interval(Duration::from_millis(500))).await;
    let mut met_in_the_receive_arm = false;
    for round in 1..=RECEIVE_ARM_ATTEMPTS {
        let (moved, kept) = cycle.hand_a_partition_back_paused(round).await;
        let at_top_of_pass = per_record_probe::reconciled_at_top_of_pass();
        let at_receive_arm = per_record_probe::assigns_drained_at_receive_arm();
        cycle.drop_an_oversize_record_on(kept).await;
        cycle
            .prove_resumed(moved, &format!("after-return-{round}"))
            .await;
        if per_record_probe::assigns_drained_at_receive_arm() > at_receive_arm {
            met_in_the_receive_arm = true;
            break;
        }
        assert!(
            per_record_probe::reconciled_at_top_of_pass() > at_top_of_pass,
            "the assign was reconciled by one drain or the other"
        );
    }
    assert!(
        met_in_the_receive_arm,
        "the receive arm drained no assign in {RECEIVE_ARM_ATTEMPTS} cycles"
    );
    cycle.stop().await;
}

// ---------------------------------------------------------------------------
// The discard a dropped record carries, counted once, on the commit that
// retires it
// ---------------------------------------------------------------------------

/// `shove_messages_discarded_total` for `topic`, summed over its reasons,
/// from one draining snapshot of the debugging recorder.
#[cfg(feature = "metrics")]
fn discarded_total(snapshotter: &Snapshotter, topic: &str) -> u64 {
    snapshotter
        .snapshot()
        .into_hashmap()
        .iter()
        .filter(|(key, _)| key.key().name() == "shove_messages_discarded_total")
        .filter(|(key, _)| {
            key.key()
                .labels()
                .any(|label| label.key() == "topic" && label.value() == topic)
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("shove_messages_discarded_total is not a counter: {other:?}"),
        })
        .sum()
}

/// Installs the debugging recorder as the process's global recorder; one
/// per test process, which nextest gives every test.
#[cfg(feature = "metrics")]
fn install_recorder() -> Snapshotter {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install the debugging recorder");
    snapshotter
}

/// A record dropped before the handler on a topic with no DLQ is gone for
/// good once its position is committed, and `messages_discarded_total`
/// counts it then, once: the drop's commit under `PerRecord` is accepted at
/// once, and the counter moves on its confirmation.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn a_dropped_records_discard_is_counted_once_when_its_commit_is_accepted() {
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    produce(&bootstrap, &["o2"]).await;
    mock.track_requests();

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(
        discarded_total(&snapshotter, TOPIC),
        1,
        "the dropped record is counted once its commit is accepted"
    );
}

/// The same drop with its commit rejected twice: the discard rides each
/// re-offer and is counted once, on the third attempt the broker accepts,
/// never on a rejection and never twice.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn a_dropped_records_discard_is_counted_once_when_a_re_offer_is_accepted() {
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    mock.track_requests();
    mock.reject_commits(2);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_until(
        || mock.offset_commit_requests() >= 2,
        DELIVERY_TIMEOUT,
        "the drop's commit being rejected twice",
    )
    .await;
    produce(&bootstrap, &["o2"]).await;
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        mock.offset_commit_requests(),
        4,
        "two rejections, the accepted re-offer, and the valid record's commit"
    );
    // The stop's final commit carries the position again and confirms no
    // discard: the one discard was settled by the re-offer that landed.
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(
        discarded_total(&snapshotter, TOPIC),
        1,
        "the discard is counted once, on the re-offer the broker accepted"
    );
}

/// The drop's commit is in flight when the stop lands and comes back
/// rejected: the shutdown arm hands the discard back, the final commit
/// carries the position again, the broker accepts it, and the discard is
/// counted once on that commit, with the position where the final commit
/// put it.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn a_discard_behind_a_commit_rejected_at_the_stop_is_counted_once_by_the_final_commit() {
    const HELD: Duration = Duration::from_secs(3);
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce_raw(&bootstrap, b"not the topic's json".to_vec()).await;
    mock.track_requests();
    mock.answer_next_offset_commit_late(
        HELD,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
    );

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::PerRecord, token)
    });
    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the drop's commit reaching the broker",
    )
    .await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("the final commit is accepted, so the stop is clean");

    assert_eq!(
        mock.offset_commit_requests(),
        2,
        "the commit in flight, rejected late, and the final commit"
    );
    assert_eq!(
        committed_position(&bootstrap).await,
        Some(1),
        "the final commit carried the dropped record's position"
    );
    assert_eq!(
        discarded_total(&snapshotter, TOPIC),
        1,
        "the discard handed back at the stop is counted once, by the final commit"
    );
}

// ---------------------------------------------------------------------------
// A republish or a dead letter that cannot be published, under either commit
// policy, and a republish that waits out its hold
// ---------------------------------------------------------------------------

/// How many sends shove's producer makes of one record before it gives up
/// on it, its `MAX_PUBLISH_ATTEMPTS`: refusing this many Produce requests
/// fails exactly one publish.
const SENDS_PER_PUBLISH: usize = 3;
/// How long a test watches for something that must not happen.
const SETTLE: Duration = Duration::from_millis(500);
/// Bytes of padding that put a record past the `message.max.bytes` shove's
/// producer runs with, librdkafka's 1 MB default.
const OVERSIZE: usize = 1_200_000;

/// One delivery as a [`Recorder`] saw it.
#[derive(Debug, Clone)]
struct Delivery {
    id: String,
    retry_count: u32,
    partition: Option<i32>,
    offset: Option<i64>,
    at: Instant,
}

/// Records every delivery of an `Order` topic and answers it with `decide`,
/// given the record's id and the delivery's metadata.
#[derive(Clone)]
struct Recorder {
    seen: Arc<Mutex<Vec<Delivery>>>,
    decide: fn(&str, &MessageMetadata) -> Outcome,
}

impl Recorder {
    fn new(decide: fn(&str, &MessageMetadata) -> Outcome) -> Self {
        Self {
            seen: Arc::default(),
            decide,
        }
    }

    fn deliveries(&self) -> Vec<Delivery> {
        self.seen.lock().expect("handler mutex poisoned").clone()
    }

    fn count(&self) -> usize {
        self.seen.lock().expect("handler mutex poisoned").len()
    }

    fn ids(&self) -> Vec<String> {
        self.deliveries().into_iter().map(|d| d.id).collect()
    }

    fn times_seen(&self, id: &str) -> usize {
        self.deliveries().iter().filter(|d| d.id == id).count()
    }
}

impl<T: Topic<Message = Order>> MessageHandler<T> for Recorder {
    type Context = ();
    async fn handle(&self, msg: Order, metadata: MessageMetadata, _: &()) -> Outcome {
        let outcome = (self.decide)(&msg.id, &metadata);
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .push(Delivery {
                id: msg.id,
                retry_count: metadata.retry_count,
                partition: metadata.partition,
                offset: metadata.offset,
                at: Instant::now(),
            });
        outcome
    }
}

/// `Retry` on the first delivery of `A`, `Ack` otherwise.
fn retry_a_once(id: &str, metadata: &MessageMetadata) -> Outcome {
    if id == "A" && metadata.retry_count == 0 {
        Outcome::Retry
    } else {
        Outcome::Ack
    }
}

/// Starts a direct consumer of `T` with `handler` and `options`, and returns
/// its run task and the token that stops it.
fn start_on<T, H>(
    client: KafkaClient,
    handler: H,
    options: impl FnOnce(CancellationToken) -> ConsumerOptions<Kafka>,
) -> (
    tokio::task::JoinHandle<Result<(), ShoveError>>,
    CancellationToken,
)
where
    T: Topic,
    H: MessageHandler<T, Context = ()> + 'static,
{
    let shutdown = CancellationToken::new();
    let opts = options(shutdown.clone());
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run::<T, _>(handler, (), opts)
            .await
    });
    (run, shutdown)
}

/// One permit, the consumer's default commit interval, and `shutdown`.
fn one_permit(shutdown: CancellationToken) -> ConsumerOptions<Kafka> {
    ConsumerOptions::<Kafka>::new()
        .with_prefetch_count(1)
        .with_shutdown(shutdown)
}

/// Produces order `id` onto `partition` of `topic`, keyed by its id.
async fn produce_to(bootstrap: &str, topic: &str, partition: i32, id: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    let payload = serde_json::to_vec(&Order { id: id.into() }).expect("encode the record");
    producer
        .send(
            FutureRecord::to(topic)
                .partition(partition)
                .key(id)
                .payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
}

/// Produces `payload` onto `topic` as is.
async fn produce_raw_on(bootstrap: &str, topic: &str, payload: &[u8]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    producer
        .send(
            FutureRecord::<(), [u8]>::to(topic).payload(payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
}

/// Produces an order onto `topic` whose id is `prefix` padded past
/// [`OVERSIZE`], through a producer whose `message.max.bytes` admits it; the
/// mock broker sets no limit of its own. Returns the id.
async fn produce_oversize(bootstrap: &str, topic: &str, prefix: &str) -> String {
    let id = format!("{prefix}{}", "x".repeat(OVERSIZE));
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.max.bytes", "4000000")
        .create()
        .expect("mock producer for a large record");
    let payload = serde_json::to_vec(&Order { id: id.clone() }).expect("encode the record");
    producer
        .send(
            FutureRecord::to(topic).key(prefix).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce the large record to the mock cluster");
    id
}

/// A record as a raw consumer reads it back.
struct RawRecord {
    payload: Vec<u8>,
    headers: Vec<(String, String)>,
}

impl RawRecord {
    /// The value of the `key` header, as text, if the record carries one.
    fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// The first `count` records of partition 0 of `topic`, read from its start
/// by a raw consumer that commits nothing.
async fn read_records(bootstrap: &str, topic: &str, count: usize) -> Vec<RawRecord> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("{topic}-raw-reader"))
        .set("enable.auto.commit", "false")
        .create()
        .expect("raw consumer");
    let mut partitions = TopicPartitionList::new();
    partitions
        .add_partition_offset(topic, 0, Offset::Beginning)
        .expect("partition 0 from the start");
    consumer.assign(&partitions).expect("assign partition 0");
    let mut records = Vec::new();
    while records.len() < count {
        let msg = tokio::time::timeout(DELIVERY_TIMEOUT, consumer.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{topic} held {} of {count} records within {DELIVERY_TIMEOUT:?}",
                    records.len()
                )
            })
            .expect("raw consumer receive");
        let headers = msg
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|h| {
                        (
                            h.key.to_string(),
                            String::from_utf8_lossy(h.value.unwrap_or_default()).into_owned(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        records.push(RawRecord {
            payload: msg.payload().unwrap_or_default().to_vec(),
            headers,
        });
    }
    records
}

/// How many records `partition` of `topic` holds: its high watermark.
async fn high_watermark(bootstrap: &str, topic: &'static str, partition: i32) -> i64 {
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .expect("watermark probe");
        probe
            .fetch_watermarks(topic, partition, Duration::from_secs(10))
            .expect("fetch the watermarks")
            .1
    })
    .await
    .expect("watermark task")
}

/// Polls the broker until `group`'s committed position on partition 0 of
/// `topic` is `expected`, failing once `timeout` has passed.
async fn wait_for_position(
    bootstrap: &str,
    topic: &'static str,
    group: &'static str,
    expected: i64,
    timeout: Duration,
    what: &str,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let committed = committed_position_on(bootstrap, topic, group, 0).await;
        if committed == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: the committed position is {committed:?}, not {expected}, after {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One counter's series of a topic, as sorted `(reason, count)` pairs.
#[cfg(feature = "metrics")]
type ReasonCounts = Vec<(String, u64)>;

/// The `shove_messages_failed_total` and `shove_messages_discarded_total`
/// counters of `topic`, each with its counts above zero, from one snapshot:
/// a snapshot drains the counters it reads.
#[cfg(feature = "metrics")]
fn failed_and_discarded(snapshotter: &Snapshotter, topic: &str) -> (ReasonCounts, ReasonCounts) {
    let mut failed = Vec::new();
    let mut discarded = Vec::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let into = match key.key().name() {
            "shove_messages_failed_total" => &mut failed,
            "shove_messages_discarded_total" => &mut discarded,
            _ => continue,
        };
        if !key
            .key()
            .labels()
            .any(|l| l.key() == "topic" && l.value() == topic)
        {
            continue;
        }
        let reason = key
            .key()
            .labels()
            .find(|l| l.key() == "reason")
            .map_or_else(String::new, |l| l.value().to_string());
        match value {
            DebugValue::Counter(0) => {}
            DebugValue::Counter(n) => into.push((reason, n)),
            other => panic!("{} is not a counter: {other:?}", key.key().name()),
        }
    }
    failed.sort();
    discarded.sort();
    (failed, discarded)
}

/// A delayed republish holds its record's permit for the whole hold, so the
/// record fetched behind it waits for that permit, and the wait keeps
/// polling: a hold longer than `max.poll.interval.ms` keeps the member in its
/// group. A group member on the defaults, one permit and the republish
/// strategy, runs with the poll interval at its 10 s floor through the
/// `test-support` seam. `A` is retried with a 15 s hold and then acked, and
/// `B` waits for the permit through the hold. No rebalance or reconnect
/// happens, `B` and `A`'s copy follow the hold, and the member commits all
/// three.
#[tokio::test]
async fn republish_hold_longer_than_max_poll_interval_keeps_membership() {
    const HOLD_TOPIC: &str = "kafka-republish-long-hold";
    const HOLD_GROUP: &str = "kafka-republish-long-hold-consumer";
    /// Well past the poll interval: a member that stopped polling for it
    /// would leave its group.
    const LONG_HOLD: Duration = Duration::from_secs(15);
    shove::define_topic!(
        LongHoldTopic,
        Order,
        TopologyBuilder::new(HOLD_TOPIC)
            .hold_queue(LONG_HOLD)
            .build()
    );

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(HOLD_TOPIC, 1, 1)
        .expect("create the topic through the mock API");
    produce_on(&bootstrap, HOLD_TOPIC, &["A", "B"]).await;

    let handler = Recorder::new(retry_a_once);
    let token = CancellationToken::new();
    let h = handler.clone();
    let mut group = KafkaConsumerGroup::new::<LongHoldTopic, _>(
        HOLD_TOPIC,
        KafkaConsumerGroupConfig::new(1..=1).with_max_poll_interval_for_test(SHORT_POLL_INTERVAL),
        connect(&bootstrap).await,
        token.clone(),
        move || h.clone(),
        (),
    );
    group.start();

    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "A reaching the handler",
    )
    .await;
    let assigns_before = per_record_probe::assigns_queued();
    wait_until(
        || handler.count() == 3,
        LONG_HOLD + DELIVERY_TIMEOUT,
        "B and A's copy reaching the handler",
    )
    .await;
    tokio::time::sleep(SETTLE).await;

    let seen = handler.deliveries();
    assert_eq!(
        handler.ids(),
        ["A", "B", "A"],
        "B follows A's hold, and A's copy follows B: {seen:?}"
    );
    assert_eq!(
        seen.iter().map(|d| d.retry_count).collect::<Vec<_>>(),
        [0, 0, 1],
        "A comes back once, as its copy: {seen:?}"
    );
    let waited = seen[1].at.saturating_duration_since(seen[0].at);
    assert!(
        waited >= LONG_HOLD - Duration::from_millis(500),
        "B waits for the permit A's republish holds, waited {waited:?}"
    );
    assert_eq!(
        per_record_probe::assigns_queued(),
        assigns_before,
        "no rebalance and no reconnect while the republish held the permit past the poll interval"
    );

    token.cancel();
    group.shutdown().await;
    assert_eq!(
        committed_position_on(&bootstrap, HOLD_TOPIC, HOLD_GROUP, 0).await,
        Some(3),
        "the member still in its group commits both records and the copy"
    );
}

/// The positions acked before a record starts waiting for a permit are
/// committed during the wait. `X1` and `X2` are acked inside one commit
/// window: the commit of `X1` opens it, and the position past `X2` waits
/// for the gate. `A` is retried with a 20 s hold and its republish holds the
/// one permit through it, so `B`, fetched behind it, waits for that permit.
/// The position past `X2` is committed while `B` waits, and nothing past `A`.
#[tokio::test]
async fn acked_positions_commit_while_a_record_waits_for_a_permit() {
    const WAIT_TOPIC: &str = "kafka-permit-wait-commits";
    const WAIT_GROUP: &str = "kafka-permit-wait-commits-consumer";
    const WINDOW: Duration = Duration::from_secs(3);
    const HOLD: Duration = Duration::from_secs(20);
    shove::define_topic!(
        WaitTopic,
        Order,
        TopologyBuilder::new(WAIT_TOPIC).hold_queue(HOLD).build()
    );

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(WAIT_TOPIC, 1, 1)
        .expect("create the topic through the mock API");
    produce_on(&bootstrap, WAIT_TOPIC, &["X1", "X2", "A", "B"]).await;

    let handler = Recorder::new(retry_a_once);
    let (run, shutdown) =
        start_on::<WaitTopic, _>(connect(&bootstrap).await, handler.clone(), |token| {
            ConsumerOptions::<Kafka>::new()
                .with_prefetch_count(1)
                .with_commit_policy(CommitPolicy::Interval(WINDOW))
                .with_shutdown(token)
        });

    wait_until(
        || handler.count() == 3,
        DELIVERY_TIMEOUT,
        "X1, X2 and A reaching the handler",
    )
    .await;
    let retried_at = handler.deliveries()[2].at;
    wait_for_position(
        &bootstrap,
        WAIT_TOPIC,
        WAIT_GROUP,
        2,
        WINDOW.saturating_mul(3),
        "the acked records committed while B waits for its permit",
    )
    .await;
    assert!(
        retried_at.elapsed() < HOLD,
        "the commit landed while A's republish held the permit"
    );
    assert_eq!(
        handler.ids(),
        ["X1", "X2", "A"],
        "B still waits for the permit"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
    assert_eq!(
        committed_position_on(&bootstrap, WAIT_TOPIC, WAIT_GROUP, 0).await,
        Some(2),
        "nothing is committed past the record whose republish was waiting"
    );
}

/// A dead letter that cannot be published retires nothing. The broker
/// refuses the three sends of `A`'s dead letter with an unknown server
/// error, which nothing tells apart from an outage, so the member ends with
/// a connection error and reconnects; `A` is handed to the handler again,
/// rejected again, and dead-lettered once the DLQ takes writes, and `B` is
/// handled behind it. The reconnect resumes from the committed position, so
/// `A` coming back is the proof nothing was committed past it, and no
/// discard is counted.
#[tokio::test]
async fn dlq_publish_failure_redelivers_concurrent() {
    const REJECT_TOPIC: &str = "kafka-dlq-failure-redelivers";
    const REJECT_DLQ: &str = "kafka-dlq-failure-redelivers-dlq";
    const REJECT_GROUP: &str = "kafka-dlq-failure-redelivers-consumer";
    shove::define_topic!(
        RejectTopic,
        Order,
        TopologyBuilder::new(REJECT_TOPIC).dlq().build()
    );

    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    for topic in [REJECT_TOPIC, REJECT_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    produce_on(&bootstrap, REJECT_TOPIC, &["A", "B"]).await;
    mock.refuse_produce(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN, SENDS_PER_PUBLISH);

    let handler = Recorder::new(|id, _| {
        if id == "A" {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    });
    let (run, shutdown) =
        start_on::<RejectTopic, _>(connect(&bootstrap).await, handler.clone(), one_permit);
    wait_until(
        || handler.count() >= 1,
        DELIVERY_TIMEOUT,
        "A reaching the handler",
    )
    .await;
    let first_assign = per_record_probe::assigns_queued();

    wait_until(
        || handler.times_seen("A") >= 2,
        DELIVERY_TIMEOUT,
        "A handed to the handler again after its dead letter failed",
    )
    .await;
    assert!(
        per_record_probe::assigns_queued() > first_assign,
        "A came back through a reconnect"
    );
    wait_for_position(
        &bootstrap,
        REJECT_TOPIC,
        REJECT_GROUP,
        2,
        DELIVERY_TIMEOUT,
        "A dead-lettered and B acked",
    )
    .await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(
        handler.ids(),
        ["A", "A", "B"],
        "A is redelivered, and B is handled once, behind it"
    );
    assert_eq!(
        high_watermark(&bootstrap, REJECT_DLQ, 0).await,
        1,
        "A is dead-lettered once"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        discarded_total(&snapshotter, REJECT_TOPIC),
        0,
        "a record kept through a failed dead letter is never counted as discarded"
    );
}

/// A record dropped before the handler whose dead letter cannot be published
/// is redelivered too. The first record does not decode, and the broker
/// refuses the three sends of its dead letter; the member reconnects, drops
/// the record again and dead-letters it, and the valid record behind it is
/// handled once. No discard is counted.
#[tokio::test]
async fn a_pre_handler_drop_whose_dead_letter_fails_is_redelivered() {
    const DROP_TOPIC: &str = "kafka-pre-handler-dlq-failure";
    const DROP_DLQ: &str = "kafka-pre-handler-dlq-failure-dlq";
    const DROP_GROUP: &str = "kafka-pre-handler-dlq-failure-consumer";
    const POISON: &[u8] = b"not the topic's json";
    shove::define_topic!(
        DropTopic,
        Order,
        TopologyBuilder::new(DROP_TOPIC).dlq().build()
    );

    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    for topic in [DROP_TOPIC, DROP_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    produce_raw_on(&bootstrap, DROP_TOPIC, POISON).await;
    produce_on(&bootstrap, DROP_TOPIC, &["B"]).await;
    mock.refuse_produce(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN, SENDS_PER_PUBLISH);

    let handler = Recorder::new(|_, _| Outcome::Ack);
    let (run, shutdown) =
        start_on::<DropTopic, _>(connect(&bootstrap).await, handler.clone(), one_permit);
    wait_until(
        || per_record_probe::assigns_queued() >= 1,
        DELIVERY_TIMEOUT,
        "the member's first assignment",
    )
    .await;
    let first_assign = per_record_probe::assigns_queued();

    wait_for_position(
        &bootstrap,
        DROP_TOPIC,
        DROP_GROUP,
        2,
        DELIVERY_TIMEOUT,
        "the dropped record dead-lettered and B acked",
    )
    .await;
    assert!(
        per_record_probe::assigns_queued() > first_assign,
        "the member reconnected after the dead letter failed"
    );
    let dead = read_records(&bootstrap, DROP_DLQ, 1).await;
    assert_eq!(
        dead[0].payload, POISON,
        "the dropped record is dead-lettered byte for byte"
    );
    tokio::time::sleep(SETTLE).await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(handler.ids(), ["B"], "B is handled once");
    assert_eq!(
        high_watermark(&bootstrap, DROP_DLQ, 0).await,
        1,
        "the dropped record is dead-lettered once"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        discarded_total(&snapshotter, DROP_TOPIC),
        0,
        "a dropped record kept through a failed dead letter is never counted as discarded"
    );
}

/// A failed republish does not pin its partition. The broker refuses the
/// three sends of `A`'s retry copy with an unknown server error; the member
/// ends with a connection error and reconnects, `A` is handed to the handler
/// again as it arrived, its copy lands on the second try, and every record
/// behind it is handled and committed.
#[tokio::test]
async fn failed_republish_does_not_pin_the_partition() {
    const PIN_TOPIC: &str = "kafka-failed-republish-no-pin";
    const PIN_GROUP: &str = "kafka-failed-republish-no-pin-consumer";
    shove::define_topic!(
        PinTopic,
        Order,
        TopologyBuilder::new(PIN_TOPIC)
            .hold_queue(Duration::from_millis(200))
            .build()
    );

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(PIN_TOPIC, 1, 1)
        .expect("create the topic through the mock API");
    produce_on(&bootstrap, PIN_TOPIC, &["A", "B"]).await;
    mock.refuse_produce(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN, SENDS_PER_PUBLISH);

    let handler = Recorder::new(retry_a_once);
    let (run, shutdown) =
        start_on::<PinTopic, _>(connect(&bootstrap).await, handler.clone(), one_permit);
    wait_until(
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "A reaching the handler",
    )
    .await;
    let first_assign = per_record_probe::assigns_queued();

    wait_for_position(
        &bootstrap,
        PIN_TOPIC,
        PIN_GROUP,
        3,
        DELIVERY_TIMEOUT,
        "A's copy and B committed past the failed republish",
    )
    .await;
    assert!(
        per_record_probe::assigns_queued() > first_assign,
        "the member reconnected after the republish failed"
    );
    tokio::time::sleep(SETTLE).await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    let seen = handler.deliveries();
    assert_eq!(
        handler.ids(),
        ["A", "A", "B", "A"],
        "A is redelivered as it arrived, then B, then A's copy: {seen:?}"
    );
    assert_eq!(
        seen.iter().map(|d| d.retry_count).collect::<Vec<_>>(),
        [0, 0, 0, 1],
        "the redelivered A carries the count it arrived with: {seen:?}"
    );
}

/// A revoke during a hold ends the delayed republish with nothing
/// published. Member A holds both partitions with two permits, takes the
/// head of each and retries both, so two republishes wait out a 30 s hold.
/// Member B joins and the coordinator moves one partition to it, inside the
/// hold. B is handed the moved partition's head from its start, and A
/// publishes no copy of it; the head A kept is republished when the hold
/// runs out, which is the clock for the copy that must not come.
#[tokio::test]
async fn revoke_during_hold_skips_the_republish() {
    const REVOKE_TOPIC: &str = "kafka-revoke-during-hold";
    /// Far longer than the rebalance a joining member starts.
    const HOLD: Duration = Duration::from_secs(30);
    /// The bound for a wait that spans that rebalance: the mock coordinator
    /// waits up to `session.timeout.ms` for its members to rejoin.
    const REBALANCE_TIMEOUT: Duration = Duration::from_secs(60);
    shove::define_topic!(
        RevokeTopic,
        Order,
        TopologyBuilder::new(REVOKE_TOPIC).hold_queue(HOLD).build()
    );

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(REVOKE_TOPIC, 2, 1)
        .expect("create the topic through the mock API");
    produce_to(&bootstrap, REVOKE_TOPIC, 0, "h0").await;
    produce_to(&bootstrap, REVOKE_TOPIC, 1, "h1").await;

    let a = Recorder::new(|_, metadata| {
        if metadata.retry_count == 0 {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    });
    let (run_a, stop_a) =
        start_on::<RevokeTopic, _>(connect(&bootstrap).await, a.clone(), |token| {
            ConsumerOptions::<Kafka>::new()
                .with_prefetch_count(2)
                .with_concurrent_processing(true)
                .with_shutdown(token)
        });
    wait_until(
        || a.count() == 2,
        DELIVERY_TIMEOUT,
        "A retrying the head of each partition",
    )
    .await;
    let retried_at = a.deliveries()[1].at;

    let b = Recorder::new(|_, _| Outcome::Ack);
    let (run_b, stop_b) =
        start_on::<RevokeTopic, _>(connect(&bootstrap).await, b.clone(), |token| {
            ConsumerOptions::<Kafka>::new().with_shutdown(token)
        });
    wait_until(
        || b.count() >= 1,
        REBALANCE_TIMEOUT,
        "B handling the head of the partition it took",
    )
    .await;
    let first_of_b = b.deliveries()[0].clone();
    let moved = first_of_b.partition.expect("Kafka reports the partition");
    assert_eq!(
        first_of_b.offset,
        Some(0),
        "B starts at the head of the moved partition, which A never committed: {first_of_b:?}"
    );
    assert!(
        retried_at.elapsed() < HOLD,
        "the rebalance completed inside A's hold"
    );
    let moved_id = first_of_b.id.clone();
    let kept_id = if moved_id == "h0" { "h1" } else { "h0" };

    let copy_of = |id: &str| {
        a.deliveries()
            .into_iter()
            .chain(b.deliveries())
            .any(|d| d.id == id && d.retry_count == 1)
    };
    wait_until(
        || copy_of(kept_id),
        HOLD + DELIVERY_TIMEOUT,
        "the copy of the head A kept reaching a handler",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        !copy_of(&moved_id),
        "A published no copy of the head its revoked partition held"
    );
    let total = high_watermark(&bootstrap, REVOKE_TOPIC, 0).await
        + high_watermark(&bootstrap, REVOKE_TOPIC, 1).await;
    assert_eq!(
        total, 3,
        "the two heads and one copy, of the head A kept, moved {moved}"
    );

    // A stops inside the rebalance B's departure starts, so its final commit
    // may be refused; only what A published matters here.
    stop_b.cancel();
    run_b
        .await
        .expect("B's task completes")
        .expect("B stops clean");
    stop_a.cancel();
    let _ = run_a.await.expect("A's task completes");
}

/// A republish the broker refuses as too large can never succeed, so the
/// record goes to its DLQ under the death reason `republish_failed`. The
/// broker refuses the retry copy's one send with `MSG_SIZE_TOO_LARGE`, as it
/// refuses a copy past the topic's `max.message.bytes`; shove's producer
/// sends it no more, the dead letter lands, and the record is committed.
/// Nothing is republished into the topic and no discard is counted.
#[tokio::test]
async fn oversize_republish_goes_to_dlq() {
    const BIG_TOPIC: &str = "kafka-oversize-republish";
    const BIG_DLQ: &str = "kafka-oversize-republish-dlq";
    const BIG_GROUP: &str = "kafka-oversize-republish-consumer";
    shove::define_topic!(
        BigTopic,
        Order,
        TopologyBuilder::new(BIG_TOPIC)
            .hold_queue(Duration::from_millis(200))
            .dlq()
            .build()
    );

    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    for topic in [BIG_TOPIC, BIG_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    produce_on(&bootstrap, BIG_TOPIC, &["A"]).await;
    mock.refuse_produce(RDKafkaRespErr::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE, 1);

    let handler = Recorder::new(retry_a_once);
    let (run, shutdown) =
        start_on::<BigTopic, _>(connect(&bootstrap).await, handler.clone(), one_permit);

    let dead = read_records(&bootstrap, BIG_DLQ, 1).await;
    assert_eq!(
        dead[0].header("Shove-Death-Reason"),
        Some("republish_failed"),
        "the dead letter names the republish that could not be made"
    );
    assert_eq!(dead[0].header("Shove-Original-Queue"), Some(BIG_TOPIC));
    assert_eq!(
        dead[0].header("Shove-Retry-Count"),
        Some("0"),
        "the dead letter carries the count the record arrived with"
    );
    wait_for_position(
        &bootstrap,
        BIG_TOPIC,
        BIG_GROUP,
        1,
        DELIVERY_TIMEOUT,
        "the record committed once dead-lettered",
    )
    .await;
    tokio::time::sleep(SETTLE).await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(handler.ids(), ["A"], "the record is handled once");
    assert_eq!(
        high_watermark(&bootstrap, BIG_TOPIC, 0).await,
        1,
        "nothing is republished into the topic"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        discarded_total(&snapshotter, BIG_TOPIC),
        0,
        "the record is in its DLQ, so nothing is discarded"
    );
}

/// A record whose republish and dead letter can both never be published is
/// discarded and counted, the one dead letter that fails without a
/// redelivery. `A` is past the `message.max.bytes` shove's producer runs
/// with, so its retry copy and its dead letter are both refused before they
/// are sent. `A` is committed, counted once in `messages_discarded_total`
/// under `republish_failed`, and `B` is handled behind it.
#[tokio::test]
async fn an_unpublishable_record_whose_dead_letter_also_fails_is_discarded_as_lost() {
    const LOST_TOPIC: &str = "kafka-unpublishable-lost";
    const LOST_DLQ: &str = "kafka-unpublishable-lost-dlq";
    const LOST_GROUP: &str = "kafka-unpublishable-lost-consumer";
    shove::define_topic!(
        LostTopic,
        Order,
        TopologyBuilder::new(LOST_TOPIC)
            .hold_queue(Duration::from_millis(200))
            .dlq()
            .build()
    );

    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    for topic in [LOST_TOPIC, LOST_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    let big = produce_oversize(&bootstrap, LOST_TOPIC, "big-").await;
    produce_on(&bootstrap, LOST_TOPIC, &["B"]).await;

    let handler = Recorder::new(|id, _| {
        if id.starts_with("big-") {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    });
    let (run, shutdown) =
        start_on::<LostTopic, _>(connect(&bootstrap).await, handler.clone(), one_permit);
    wait_for_position(
        &bootstrap,
        LOST_TOPIC,
        LOST_GROUP,
        2,
        DELIVERY_TIMEOUT,
        "the unpublishable record discarded and B acked",
    )
    .await;
    tokio::time::sleep(SETTLE).await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(
        handler.ids(),
        [big, "B".to_string()],
        "the large record is handled once, and B behind it"
    );
    assert_eq!(
        high_watermark(&bootstrap, LOST_DLQ, 0).await,
        0,
        "no dead letter could be published"
    );
    assert_eq!(
        high_watermark(&bootstrap, LOST_TOPIC, 0).await,
        2,
        "no copy could be republished"
    );
    #[cfg(feature = "metrics")]
    {
        let (failed, discarded) = failed_and_discarded(&snapshotter, LOST_TOPIC);
        assert_eq!(
            discarded,
            [("republish_failed".to_string(), 1)],
            "the record is counted once as discarded"
        );
        assert_eq!(
            failed,
            [("republish_failed".to_string(), 1)],
            "the failed republish is counted once"
        );
    }
}

/// A record that waits a moment for the permit a running handler holds is
/// not put back. The wait polls only once the permit has not freed within a
/// grace, so a member on one permit draining a backlog pays no seek, pause
/// and refetch for each record behind a running one. Every record but the
/// first waits for the permit its predecessor holds, and none is handed back
/// to the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_waiting_briefly_for_a_permit_is_not_put_back() {
    use shove::kafka::permit_wait_probe;

    const DRAIN_TOPIC: &str = "kafka-permit-wait-drain";
    const RECORDS: usize = 200;
    shove::define_topic!(DrainTopic, Order, TopologyBuilder::new(DRAIN_TOPIC).build());

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(DRAIN_TOPIC, 1, 1)
        .expect("create the topic through the mock API");
    let ids: Vec<String> = (0..RECORDS).map(|i| format!("r{i}")).collect();
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    produce_on(&bootstrap, DRAIN_TOPIC, &id_refs).await;

    let handler = Recorder::new(|_, _| Outcome::Ack);
    let (run, shutdown) =
        start_on::<DrainTopic, _>(connect(&bootstrap).await, handler.clone(), one_permit);
    wait_until(
        || handler.count() == RECORDS,
        DELIVERY_TIMEOUT,
        "the backlog drained",
    )
    .await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert!(
        permit_wait_probe::entered() >= RECORDS / 2,
        "the records waited for the permit, {} waits",
        permit_wait_probe::entered()
    );
    assert_eq!(
        put_back_probe::permit_wait(),
        0,
        "no record was handed back to the broker while one waited for the permit"
    );
    assert_eq!(handler.ids(), ids, "each record once, in order");
}

/// A dead letter that fails with an error no retry gets past ends the member
/// with that error, where a reconnect would only fail the same way again. The
/// test seam refuses every dead letter with the `Topology` error a client
/// whose producer cannot be built answers with. `A` is rejected once, the
/// run returns that error with no reconnect left to try it again, and
/// nothing is committed or counted as discarded.
#[tokio::test]
async fn a_dlq_publish_failure_that_cannot_be_retried_ends_the_member() {
    use shove::kafka::dlq_publish_probe;

    const FATAL_TOPIC: &str = "kafka-dlq-failure-not-retryable";
    const FATAL_DLQ: &str = "kafka-dlq-failure-not-retryable-dlq";
    const FATAL_GROUP: &str = "kafka-dlq-failure-not-retryable-consumer";
    shove::define_topic!(
        FatalTopic,
        Order,
        TopologyBuilder::new(FATAL_TOPIC).dlq().build()
    );

    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    for topic in [FATAL_TOPIC, FATAL_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    produce_on(&bootstrap, FATAL_TOPIC, &["A", "B"]).await;
    dlq_publish_probe::refuse(true);

    let handler = Recorder::new(|id, _| {
        if id == "A" {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    });
    let (run, shutdown) =
        start_on::<FatalTopic, _>(connect(&bootstrap).await, handler.clone(), one_permit);
    let result = tokio::time::timeout(DELIVERY_TIMEOUT, run)
        .await
        .expect("the member ends within the bound")
        .expect("the run task completes");
    let err = result.expect_err("the dead letter's failure ends the member");
    assert!(
        matches!(&err, ShoveError::Topology(msg) if msg.contains("refused by the test seam")),
        "the member ends with the error that cannot be retried, as it came: {err}"
    );
    assert_eq!(
        handler.ids(),
        ["A"],
        "A is handled once: no reconnect handed it over again, and B waited behind it"
    );
    assert_eq!(
        committed_position_on(&bootstrap, FATAL_TOPIC, FATAL_GROUP, 0).await,
        None,
        "nothing is committed past A"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        discarded_total(&snapshotter, FATAL_TOPIC),
        0,
        "A is not retired, so nothing is discarded"
    );
    drop(shutdown);
}

/// Under `PerRecord` too, a dead letter that cannot be published retires
/// nothing. The broker refuses the three sends of `A`'s dead letter; the
/// member ends with a connection error before the next record is handed out
/// and reconnects, `A` is handed to the handler again and dead-lettered once
/// the DLQ takes writes, and `B` follows it. No discard is counted.
#[tokio::test]
async fn a_dlq_publish_failure_under_per_record_redelivers() {
    const PER_RECORD_TOPIC: &str = "kafka-dlq-failure-per-record";
    const PER_RECORD_DLQ: &str = "kafka-dlq-failure-per-record-dlq";
    const PER_RECORD_GROUP: &str = "kafka-dlq-failure-per-record-consumer";
    shove::define_topic!(
        PerRecordRejectTopic,
        Order,
        TopologyBuilder::new(PER_RECORD_TOPIC).dlq().build()
    );

    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    for topic in [PER_RECORD_TOPIC, PER_RECORD_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    produce_on(&bootstrap, PER_RECORD_TOPIC, &["A", "B"]).await;
    mock.refuse_produce(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN, SENDS_PER_PUBLISH);

    let handler = Recorder::new(|id, _| {
        if id == "A" {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    });
    let (run, shutdown) =
        start_on::<PerRecordRejectTopic, _>(connect(&bootstrap).await, handler.clone(), |token| {
            options(CommitPolicy::PerRecord, token)
        });
    wait_until(
        || handler.count() >= 1,
        DELIVERY_TIMEOUT,
        "A reaching the handler",
    )
    .await;
    let first_assign = per_record_probe::assigns_queued();

    wait_until(
        || handler.times_seen("A") >= 2,
        DELIVERY_TIMEOUT,
        "A handed to the handler again after its dead letter failed",
    )
    .await;
    assert!(
        per_record_probe::assigns_queued() > first_assign,
        "A came back through a reconnect"
    );
    wait_for_position(
        &bootstrap,
        PER_RECORD_TOPIC,
        PER_RECORD_GROUP,
        2,
        DELIVERY_TIMEOUT,
        "A dead-lettered and B acked",
    )
    .await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(
        handler.ids(),
        ["A", "A", "B"],
        "A is redelivered, and B is handed out only behind it"
    );
    assert_eq!(
        high_watermark(&bootstrap, PER_RECORD_DLQ, 0).await,
        1,
        "A is dead-lettered once"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        discarded_total(&snapshotter, PER_RECORD_TOPIC),
        0,
        "a record kept through a failed dead letter is never counted as discarded"
    );
}

/// The dead letter of a record that no longer decodes for its in-place
/// redelivery retires nothing when it cannot be published. On an
/// infra-owned topic a `Retry` waits in place and decodes the record again;
/// the codec here refuses every second decode, so that decode fails and the
/// record is dropped before the handler from inside its handler task. The
/// broker refuses the three sends of its dead letter, the task reports the
/// fault, and the member reconnects: the record is handed to the handler
/// again, and its second dead letter lands. No discard is counted.
#[tokio::test]
async fn an_in_place_dead_letter_that_fails_after_a_failed_decode_redelivers() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use shove::codec::Codec;

    const UNDECODABLE_TOPIC: &str = "kafka-in-place-undecodable-dlq-failure";
    const UNDECODABLE_DLQ: &str = "kafka-in-place-undecodable-dlq-failure-dlq";
    const UNDECODABLE_GROUP: &str = "kafka-in-place-undecodable-dlq-failure-consumer";

    static DECODES: AtomicUsize = AtomicUsize::new(0);

    /// JSON that refuses every second decode: the first, the third and so
    /// on succeed.
    struct EveryOtherDecode;
    impl Codec<Order> for EveryOtherDecode {
        const NAME: &'static str = "every-other-decode";
        fn encode(value: &Order) -> Result<Vec<u8>, ShoveError> {
            Ok(serde_json::to_vec(value)?)
        }
        fn decode(bytes: &[u8]) -> Result<Order, ShoveError> {
            if DECODES.fetch_add(1, Ordering::SeqCst) % 2 == 1 {
                return Err(ShoveError::Codec {
                    codec: Self::NAME,
                    source: "every second decode is refused".into(),
                });
            }
            Ok(serde_json::from_slice(bytes)?)
        }
    }

    shove::define_topic!(
        UndecodableTopic,
        Order,
        TopologyBuilder::new(UNDECODABLE_TOPIC)
            .external()
            .hold_queue(Duration::from_millis(200))
            .dlq()
            .build(),
        codec = EveryOtherDecode
    );

    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    for topic in [UNDECODABLE_TOPIC, UNDECODABLE_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    produce_on(&bootstrap, UNDECODABLE_TOPIC, &["A"]).await;
    mock.refuse_produce(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN, SENDS_PER_PUBLISH);

    let handler = Recorder::new(|_, _| Outcome::Retry);
    let (run, shutdown) =
        start_on::<UndecodableTopic, _>(connect(&bootstrap).await, handler.clone(), one_permit);
    wait_until(
        || handler.count() >= 1,
        DELIVERY_TIMEOUT,
        "A reaching the handler",
    )
    .await;
    let first_assign = per_record_probe::assigns_queued();

    wait_until(
        || handler.count() >= 2,
        DELIVERY_TIMEOUT,
        "A handed to the handler again after its dead letter failed",
    )
    .await;
    assert!(
        per_record_probe::assigns_queued() > first_assign,
        "A came back through a reconnect"
    );
    wait_for_position(
        &bootstrap,
        UNDECODABLE_TOPIC,
        UNDECODABLE_GROUP,
        1,
        DELIVERY_TIMEOUT,
        "A dead-lettered after its second failed decode",
    )
    .await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(handler.ids(), ["A", "A"], "A is redelivered once");
    let dead = read_records(&bootstrap, UNDECODABLE_DLQ, 1).await;
    assert_eq!(
        dead[0].header("Shove-Death-Reason"),
        Some("deserialization_error"),
        "the dead letter names the decode that failed"
    );
    assert_eq!(
        high_watermark(&bootstrap, UNDECODABLE_DLQ, 0).await,
        1,
        "A is dead-lettered once"
    );
    assert_eq!(
        high_watermark(&bootstrap, UNDECODABLE_TOPIC, 0).await,
        1,
        "nothing is written into the infra-owned topic"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        discarded_total(&snapshotter, UNDECODABLE_TOPIC),
        0,
        "a record kept through a failed dead letter is never counted as discarded"
    );
}

/// A rebalance completes while a record waits for a permit. Member A takes
/// the head of both partitions on one permit: the first is retried and its
/// republish holds the permit through a 90 s hold, so the second waits for
/// that permit. Member B joins, and the cooperative rebalance cannot finish
/// until A serves its callbacks, which only the permit wait's polling can
/// do. B handling the head of the partition it took, inside A's hold, shows
/// the wait served them.
#[tokio::test]
async fn a_rebalance_completes_while_a_record_waits_for_a_permit() {
    use shove::kafka::permit_wait_probe;

    const WAIT_REBALANCE_TOPIC: &str = "kafka-rebalance-during-permit-wait";
    /// Longer than `REBALANCE_TIMEOUT`, so a rebalance B's wait sees complete
    /// always completes inside the hold. The stops end the hold at once.
    const HOLD: Duration = Duration::from_secs(90);
    /// The bound for a wait that spans that rebalance: the mock coordinator
    /// waits up to `session.timeout.ms` for its members to rejoin.
    const REBALANCE_TIMEOUT: Duration = Duration::from_secs(60);
    shove::define_topic!(
        WaitRebalanceTopic,
        Order,
        TopologyBuilder::new(WAIT_REBALANCE_TOPIC)
            .hold_queue(HOLD)
            .build()
    );

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(WAIT_REBALANCE_TOPIC, 2, 1)
        .expect("create the topic through the mock API");
    produce_to(&bootstrap, WAIT_REBALANCE_TOPIC, 0, "h0").await;
    produce_to(&bootstrap, WAIT_REBALANCE_TOPIC, 1, "h1").await;

    let a = Recorder::new(|_, metadata| {
        if metadata.retry_count == 0 {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    });
    let (run_a, stop_a) =
        start_on::<WaitRebalanceTopic, _>(connect(&bootstrap).await, a.clone(), one_permit);
    wait_until(
        || a.count() >= 1,
        DELIVERY_TIMEOUT,
        "A retrying the head it took first",
    )
    .await;
    wait_until(
        || permit_wait_probe::entered() >= 2,
        DELIVERY_TIMEOUT,
        "the other head waiting for the permit the hold keeps",
    )
    .await;
    let retried_at = a.deliveries()[0].at;

    let b = Recorder::new(|_, _| Outcome::Ack);
    let (run_b, stop_b) =
        start_on::<WaitRebalanceTopic, _>(connect(&bootstrap).await, b.clone(), |token| {
            ConsumerOptions::<Kafka>::new().with_shutdown(token)
        });
    wait_until(
        || b.count() >= 1,
        REBALANCE_TIMEOUT,
        "B handling the head of the partition it took",
    )
    .await;
    assert!(
        retried_at.elapsed() < HOLD,
        "the rebalance completed inside A's hold, while A waited for its permit"
    );
    let first_of_b = b.deliveries()[0].clone();
    assert_eq!(
        first_of_b.offset,
        Some(0),
        "B starts at the head of the moved partition, which A never committed: {first_of_b:?}"
    );

    // A stops inside the rebalance B's departure starts, so its final commit
    // may be refused; only the rebalance inside the hold matters here.
    stop_b.cancel();
    run_b
        .await
        .expect("B's task completes")
        .expect("B stops clean");
    stop_a.cancel();
    let _ = run_a.await.expect("A's task completes");
}

// ---------------------------------------------------------------------------
// Commit answers under `Interval`, and a consumer closed off the runtime
// ---------------------------------------------------------------------------

/// How long a test holds the coordinator's answer to the LeaveGroup a
/// closing member sends: the close waits for it, so a close run on the
/// runtime thread would hold that thread this long.
const CLOSE_HOLD: Duration = Duration::from_secs(3);
/// The longest the ticker may stop for on a runtime nothing blocks. Far
/// above a scheduling hiccup, far below `CLOSE_HOLD`.
const TICK_STALL: Duration = Duration::from_secs(1);

/// Ticks every 10 ms on the runtime it is spawned on, so a test on a
/// current-thread runtime can tell whether anything held that thread: a
/// blocking call on it stops the ticks for as long as it blocks.
struct Ticker {
    ticks: Arc<Mutex<Vec<Instant>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Ticker {
    fn start() -> Self {
        let ticks = Arc::new(Mutex::new(Vec::new()));
        let recorded = ticks.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                recorded
                    .lock()
                    .expect("ticker mutex poisoned")
                    .push(Instant::now());
            }
        });
        Self { ticks, task }
    }

    /// The longest the ticks stopped for between `since` and now.
    fn longest_gap_since(&self, since: Instant) -> Duration {
        let ticks = self.ticks.lock().expect("ticker mutex poisoned");
        let mut previous = since;
        let mut longest = Duration::ZERO;
        for &tick in ticks.iter().filter(|&&tick| tick >= since) {
            longest = longest.max(tick.saturating_duration_since(previous));
            previous = tick;
        }
        longest.max(Instant::now().saturating_duration_since(previous))
    }
}

impl Drop for Ticker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Under `Interval` a commit the coordinator rejects is re-offered. The mock
/// refuses every OffsetCommit for 2 s and then accepts them. The three
/// records are handled inside that window, so every commit their
/// completions make is refused, and no record arrives after it. The
/// position still reaches the end of what was handled, through a re-offer,
/// with no reconnect and no record handled twice.
#[tokio::test]
async fn interval_rejected_commit_is_reoffered() {
    const REFUSING: Duration = Duration::from_secs(2);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2", "o3"]).await;
    mock.track_requests();
    mock.reject_commits(512);

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::Interval(Duration::from_millis(500)), token)
    });
    wait_until(
        || handler.count() == 3,
        DELIVERY_TIMEOUT,
        "three records reaching the handler",
    )
    .await;
    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "a commit reaching the broker",
    )
    .await;
    let assigns = per_record_probe::assigns_queued();
    tokio::time::sleep(REFUSING).await;
    assert_eq!(
        committed_position(&bootstrap).await,
        None,
        "every commit inside the window was refused"
    );

    mock.accept_commits();
    wait_for_committed_position(&bootstrap, 3, DELIVERY_TIMEOUT).await;
    assert_eq!(
        handler.ids(),
        ["o1", "o2", "o3"],
        "the position was re-offered, and no record was handed out again"
    );
    assert_eq!(
        per_record_probe::assigns_queued(),
        assigns,
        "the re-offer landed on the same connection, without a reconnect"
    );

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// Under `Interval` a commit the coordinator keeps rejecting, with no
/// rebalance resolving it, fences the member. Every OffsetCommit is refused,
/// so the streak the first rejection starts never clears; at the threshold
/// the member reconnects, and the new connection is handed both records
/// again from the position nothing committed past. The floor is lowered
/// through the group config's test seam, and the default interval keeps the
/// threshold at it.
#[tokio::test]
async fn interval_sustained_rejection_fences() {
    const FLOOR: Duration = Duration::from_secs(5);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    mock.reject_commits(512);

    let handler = Acking::default();
    let h = handler.clone();
    let (broker, run, token) = start_group(
        connect(&bootstrap).await,
        h,
        KafkaConsumerGroupConfig::new(1..=1)
            .with_prefetch_count(1)
            .with_commit_fence_floor_for_test(FLOOR),
    )
    .await;

    wait_until(
        || handler.count() == 2,
        DELIVERY_TIMEOUT,
        "both records reaching the handler",
    )
    .await;
    let first_delivery = handler.arrived_at(0);
    assert_eq!(
        fence_probe::last_threshold(),
        Some(FLOOR),
        "the default interval judges rejected commits by the floor"
    );

    wait_until(
        || fence_probe::fired() >= 1,
        FLOOR + DELIVERY_TIMEOUT,
        "the fence firing on the rejected streak",
    )
    .await;
    wait_until(
        || handler.count() >= 3,
        DELIVERY_TIMEOUT,
        "the reconnected member receiving the first record again",
    )
    .await;
    let until_fence = handler
        .arrived_at(2)
        .saturating_duration_since(first_delivery);
    assert!(
        until_fence >= FLOOR,
        "the fence waits out its floor, fired after {until_fence:?}"
    );
    assert_eq!(
        &handler.ids()[..3],
        &["o1".to_string(), "o2".to_string(), "o1".to_string()],
        "the reconnect replays from the position nothing committed past"
    );
    assert!(
        mock.offset_commit_requests() >= 3,
        "the rejected commit was re-offered before the fence, requests: {}",
        mock.offset_commit_requests()
    );
    assert_eq!(
        committed_position(&bootstrap).await,
        None,
        "no commit was accepted"
    );

    // The stop's final commit is refused like every other, so the member
    // reports that one refusal under `errors` instead of ending clean.
    token.cancel();
    let outcome = run.await.expect("the run task completes");
    assert_eq!(outcome.errors, 1, "{outcome:?}");
    assert_eq!(outcome.panics, 0, "{outcome:?}");
    assert!(!outcome.timed_out, "{outcome:?}");
    broker.close().await;
}

/// A rejected commit that carries a discard keeps the member, and is never
/// made on the runtime thread. The topic declares no DLQ, so `o1`'s
/// `Reject` discards it, and the commit that retires it must be confirmed
/// before the discard counts. The mock holds the answer to that commit for
/// 2 s and then refuses it, holds the re-offer's answer for 1 s and refuses
/// it too, and accepts the one after. The runtime is current-thread, and a
/// ticker on it keeps ticking through the held answers. No reconnect
/// happens, each record is handled once, and `messages_discarded_total`
/// counts nothing while a refusal is held or once it has landed, and `o1`
/// once, after the re-offer the broker accepted and before the stop.
#[cfg(feature = "metrics")]
#[tokio::test(flavor = "current_thread")]
async fn rejected_discard_commit_keeps_the_member() {
    const HELD: Duration = Duration::from_secs(2);
    /// How long the re-offer's refusal is held: the first refusal has
    /// landed by then, and nothing may count the discard meanwhile.
    const RE_OFFER_HELD: Duration = Duration::from_secs(1);
    let snapshotter = install_recorder();
    // A snapshot drains the counters it reads, so the reads add up here.
    let discarded = std::cell::Cell::new(0u64);
    let discarded_so_far = || {
        discarded.set(discarded.get() + discarded_total(&snapshotter, TOPIC));
        discarded.get()
    };
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1", "o2"]).await;
    mock.track_requests();
    let refused = RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED;
    mock.answer_next_offset_commit_late(HELD, refused);
    mock.answer_next_offset_commit_late(RE_OFFER_HELD, refused);

    let handler = Recorder::new(|id, _| {
        if id == "o1" {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    });
    let client = connect(&bootstrap).await;
    let ticker = Ticker::start();
    // Before the member starts: on this one thread, a commit that blocked it
    // would block this task too, so a mark taken after the record reached
    // the handler could come after the block.
    let started_at = Instant::now();
    let (run, shutdown) = start_on::<OrdersTopic, _>(client, handler.clone(), |token| {
        options(CommitPolicy::Interval(Duration::from_millis(500)), token)
    });
    wait_until(
        || handler.count() >= 1,
        DELIVERY_TIMEOUT,
        "o1 reaching the handler",
    )
    .await;
    let assigns = per_record_probe::assigns_queued();

    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the commit retiring o1 reaching the broker",
    )
    .await;
    assert_eq!(
        discarded_so_far(),
        0,
        "the discard does not count while the refusal of its commit is held"
    );
    wait_until(
        || mock.offset_commit_requests() >= 2,
        HELD + DELIVERY_TIMEOUT,
        "the refused commit being re-offered",
    )
    .await;
    assert_eq!(
        discarded_so_far(),
        0,
        "the refused commit counted no discard"
    );

    wait_for_committed_position(&bootstrap, 2, RE_OFFER_HELD + DELIVERY_TIMEOUT).await;
    wait_until(
        || discarded_so_far() >= 1,
        DELIVERY_TIMEOUT,
        "the accepted re-offer counting the discard",
    )
    .await;
    assert_eq!(
        discarded_so_far(),
        1,
        "the discard is counted once, on the re-offer the broker accepted"
    );
    let stall = ticker.longest_gap_since(started_at);
    assert!(
        stall < TICK_STALL,
        "the commit waited for its answer off the runtime thread, the ticker stopped for {stall:?}"
    );
    assert!(
        mock.offset_commit_requests() >= 3,
        "the two held rejections and the accepted re-offer, requests: {}",
        mock.offset_commit_requests()
    );
    assert_eq!(
        per_record_probe::assigns_queued(),
        assigns,
        "the rejected commits did not tear the member down"
    );
    assert_eq!(handler.ids(), ["o1", "o2"], "each record is handled once");

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
    assert_eq!(
        discarded_so_far(),
        1,
        "the stop's final commit counts the discard no second time"
    );
}

/// A member that ends on an error closes its consumer off the runtime
/// thread, and has left its group when the error is returned. Its record's
/// dead letter cannot be published, which ends the loop, and its one
/// reconnect attempt is spent at once, so the run returns the error. The
/// close waits for the coordinator to answer the member's LeaveGroup, which
/// the mock holds as a coordinator gone quiet would, and the run returns
/// only once that answer is in. A ticker on the same current-thread runtime
/// keeps ticking through the close.
#[tokio::test(flavor = "current_thread")]
async fn error_exit_closes_off_runtime() {
    const EXIT_TOPIC: &str = "kafka-error-exit-close";
    const EXIT_DLQ: &str = "kafka-error-exit-close-dlq";
    shove::define_topic!(
        ExitTopic,
        Order,
        TopologyBuilder::new(EXIT_TOPIC).dlq().build()
    );

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    for topic in [EXIT_TOPIC, EXIT_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    produce_on(&bootstrap, EXIT_TOPIC, &["A"]).await;
    mock.refuse_produce(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN, SENDS_PER_PUBLISH);
    mock.track_requests();
    mock.answer_next_late(
        RDKafkaApiKey::LeaveGroup,
        CLOSE_HOLD,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
    );

    let handler = Recorder::new(|_, _| Outcome::Reject);
    let ticker = Ticker::start();
    let (run, _shutdown) =
        start_on::<ExitTopic, _>(connect(&bootstrap).await, handler.clone(), |token| {
            one_permit(token).with_max_reconnect_attempts(1)
        });
    wait_until(
        || handler.count() >= 1,
        DELIVERY_TIMEOUT,
        "A reaching the handler",
    )
    .await;
    let failing_at = Instant::now();
    let result = tokio::time::timeout(DELIVERY_TIMEOUT, run)
        .await
        .expect("the member ends within the bound")
        .expect("the run task completes");
    let returned_after = failing_at.elapsed();
    assert!(
        matches!(&result, Err(ShoveError::Connection(_))),
        "the failed dead letter ends the member once its reconnect is spent: {result:?}"
    );

    assert!(
        returned_after >= CLOSE_HOLD,
        "the run returns once the held LeaveGroup is answered, returned after {returned_after:?}"
    );
    assert!(
        mock.requests_of(RDKafkaApiKey::LeaveGroup) >= 1,
        "the member left its group before the run returned"
    );
    let stall = ticker.longest_gap_since(failing_at);
    assert!(
        stall < TICK_STALL,
        "the close waited for the held LeaveGroup off the runtime thread, the ticker stopped for {stall:?}"
    );
}

/// A member whose task is aborted closes its consumer off the runtime thread
/// too. The record's commit has landed, so nothing is in flight when the
/// abort drops the receive loop where it waits; the close it leaves behind
/// waits for the held LeaveGroup answer while a ticker on the same
/// current-thread runtime keeps ticking.
#[tokio::test(flavor = "current_thread")]
async fn an_aborted_member_closes_off_runtime() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1"]).await;
    mock.track_requests();
    mock.answer_next_late(
        RDKafkaApiKey::LeaveGroup,
        CLOSE_HOLD,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
    );

    let handler = Acking::default();
    let h = handler.clone();
    let ticker = Ticker::start();
    let (run, _shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::Interval(Duration::from_millis(500)), token)
    });
    wait_for_committed_position(&bootstrap, 1, DELIVERY_TIMEOUT).await;

    let aborted_at = Instant::now();
    run.abort();
    let _ = run.await;
    tokio::time::sleep(CLOSE_HOLD + TICK_STALL).await;
    assert!(
        mock.requests_of(RDKafkaApiKey::LeaveGroup) >= 1,
        "the aborted member's close reached the coordinator"
    );
    let stall = ticker.longest_gap_since(aborted_at);
    assert!(
        stall < TICK_STALL,
        "the close waited for the held LeaveGroup off the runtime thread, the ticker stopped for {stall:?}"
    );
    assert_eq!(handler.ids(), ["o1"]);
}

/// Holds the answer to the next OffsetCommit and, once that request has
/// reached the broker, takes the broker down, as a coordinator that drops
/// off the network would: the connection carrying the commit closes with no
/// answer, and the commit's retry waits for a broker that does not come
/// back. The commit then has no answer for the whole share of the shutdown
/// deadline a stop gives it. A held answer alone would not do: the
/// heartbeat queued behind it times out after the 10 s session timeout and
/// fails the connection, and the retried commit is answered long before
/// the 15 s share has run out.
async fn take_the_broker_down_under_a_commit(mock: &Mock) {
    mock.delay_next_offset_commit_answer(pending_commit_budget_for_test());
    wait_until(
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "the record's commit reaching the broker",
    )
    .await;
    mock.api()
        .broker_down(BROKER_ID)
        .expect("take the mock broker down");
}

/// What a stop that gave up on its commit in flight reports: the `Deadline`
/// kind carrying the share of the shutdown deadline, and the position the
/// unanswered commit carried, after waiting out the share and inside the
/// deadline.
fn assert_the_share_was_waited_out(
    result: Result<(), ShoveError>,
    topic: &str,
    took: Duration,
    share: Duration,
) {
    let err = result.expect_err("the stop reports the commit it could not finish");
    let ShoveError::Commit(failed) = &err else {
        panic!("a Commit error naming the unanswered commit: {err:?}");
    };
    assert_eq!(failed.topic, topic);
    assert_eq!(
        failed.offsets,
        [(0, 1)],
        "the position the commit in flight carried"
    );
    assert_eq!(
        failed.kind,
        CommitFailure::Deadline(share),
        "the Deadline kind carrying the share of the shutdown deadline"
    );
    assert!(
        took >= share - Duration::from_millis(500) && took < shutdown_commit_deadline_for_test(),
        "the stop waits out its share and ends inside the deadline, took {took:?}"
    );
}

/// A stop that finds an `Interval` commit in flight, with no answer inside
/// the share of the shutdown deadline it is given, ends with
/// `ShoveError::Commit` of the `Deadline` kind carrying that share, and
/// makes no second commit. The broker goes down under the record's commit,
/// see [`take_the_broker_down_under_a_commit`], and the stop fires then.
#[tokio::test]
async fn an_interval_stop_with_its_commit_unanswered_reports_the_pending_share() {
    let share = pending_commit_budget_for_test();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1"]).await;
    mock.track_requests();

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::Interval(Duration::from_millis(500)), token)
    });
    take_the_broker_down_under_a_commit(&mock).await;
    let stopped_at = Instant::now();
    shutdown.cancel();
    let result = run.await.expect("the run task completes");
    let took = stopped_at.elapsed();

    assert_the_share_was_waited_out(result, TOPIC, took, share);
    assert_eq!(
        mock.offset_commit_requests(),
        1,
        "no second commit races the one in flight"
    );
    assert_eq!(handler.ids(), ["o1"]);
}

/// A completion that reaches the channel after its receive loop ended is not
/// a full channel, and is never counted as one. Two permits: `A`'s dead
/// letter cannot be published, which ends the loop while `B`'s handler is
/// still running, and `B` finishes after the loop is gone. The member
/// reconnects, `A` is dead-lettered, and `B` is handled again, because its
/// first completion reached no tracker.
#[tokio::test]
async fn a_completion_after_a_fault_is_not_counted_as_refused() {
    use shove::kafka::completion_probe;

    const LATE_TOPIC: &str = "kafka-completion-after-fault";
    const LATE_DLQ: &str = "kafka-completion-after-fault-dlq";
    const LATE_GROUP: &str = "kafka-completion-after-fault-consumer";
    /// Long enough that `A`'s failed dead letter ends the loop first: its
    /// three sends fail inside about half a second.
    const B_RUNS: Duration = Duration::from_secs(3);
    shove::define_topic!(
        LateTopic,
        Order,
        TopologyBuilder::new(LATE_TOPIC).dlq().build()
    );

    /// Rejects `A`, and acks `B` once it has run for `B_RUNS`.
    #[derive(Clone, Default)]
    struct SlowB {
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl SlowB {
        fn times_seen(&self, id: &str) -> usize {
            self.seen
                .lock()
                .expect("handler mutex poisoned")
                .iter()
                .filter(|seen| *seen == id)
                .count()
        }
    }

    impl MessageHandler<LateTopic> for SlowB {
        type Context = ();
        async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
            self.seen
                .lock()
                .expect("handler mutex poisoned")
                .push(msg.id.clone());
            if msg.id == "A" {
                return Outcome::Reject;
            }
            tokio::time::sleep(B_RUNS).await;
            Outcome::Ack
        }
    }

    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    for topic in [LATE_TOPIC, LATE_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    produce_on(&bootstrap, LATE_TOPIC, &["A", "B"]).await;
    mock.refuse_produce(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN, SENDS_PER_PUBLISH);

    let handler = SlowB::default();
    let (run, shutdown) =
        start_on::<LateTopic, _>(connect(&bootstrap).await, handler.clone(), |token| {
            ConsumerOptions::<Kafka>::new()
                .with_prefetch_count(2)
                .with_concurrent_processing(true)
                .with_shutdown(token)
        });
    wait_until(
        || handler.times_seen("B") >= 2,
        B_RUNS + DELIVERY_TIMEOUT,
        "B handed to the handler again after the reconnect",
    )
    .await;
    wait_for_position(
        &bootstrap,
        LATE_TOPIC,
        LATE_GROUP,
        2,
        B_RUNS + DELIVERY_TIMEOUT,
        "A dead-lettered and B acked after the reconnect",
    )
    .await;
    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");

    assert!(
        handler.times_seen("A") >= 2,
        "A came back through the reconnect"
    );
    assert_eq!(
        completion_probe::refused(),
        0,
        "B's completion after the loop ended is not counted as refused"
    );
}

// ---------------------------------------------------------------------------
// The sequenced consumer's commit answers
// ---------------------------------------------------------------------------

const SEQUENCED_TOPIC: &str = "kafka-commit-policy-mock-sequenced";
const SEQUENCED_DLQ: &str = "kafka-commit-policy-mock-sequenced-dlq";
/// The group a sequenced consumer of the topic joins: `{queue}-fifo`.
const SEQUENCED_GROUP: &str = "kafka-commit-policy-mock-sequenced-fifo";

shove::define_sequenced_topic!(
    SequencedOrders,
    Order,
    |order: &Order| order.id.clone(),
    TopologyBuilder::new(SEQUENCED_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(Duration::from_millis(50))
        .dlq()
        .build()
);

/// One mock broker with the sequenced topic and its DLQ created as well,
/// and `ids` produced onto the topic.
async fn sequenced_mock(ids: &[&str]) -> (Mock, String) {
    let mock = Mock::start();
    for topic in [SEQUENCED_TOPIC, SEQUENCED_DLQ] {
        mock.api()
            .create_topic(topic, 1, 1)
            .expect("create the topic through the mock API");
    }
    let bootstrap = mock.bootstrap();
    produce_on(&bootstrap, SEQUENCED_TOPIC, ids).await;
    (mock, bootstrap)
}

/// Starts one sequenced member on the topic through
/// `KafkaConsumerGroup::new_fifo`, whose config carries the fence floor
/// seam, and returns the group and the token that stops it.
async fn start_sequenced_group(
    bootstrap: &str,
    handler: &Recorder,
    fence_floor: Duration,
) -> (KafkaConsumerGroup, CancellationToken) {
    let token = CancellationToken::new();
    let h = handler.clone();
    let mut group = KafkaConsumerGroup::new_fifo::<SequencedOrders, _>(
        SEQUENCED_TOPIC,
        connect(bootstrap).await,
        KafkaConsumerGroupConfig::new(1..=1).with_commit_fence_floor_for_test(fence_floor),
        token.clone(),
        move || h.clone(),
        (),
    );
    group.start();
    (group, token)
}

/// A sequenced member whose commits keep being rejected, with no rebalance
/// resolving them, is fenced and reconnects, as the standard consumer is.
/// Every OffsetCommit is refused, so the streak the first rejection starts
/// never clears; at the threshold the member reconnects, and the new
/// connection is handed the first record again from the position nothing
/// committed past. The floor is lowered through the group config's test
/// seam, and the sequenced consumer's fixed 500 ms interval keeps the
/// threshold at it.
#[tokio::test]
async fn a_sequenced_member_whose_commits_keep_being_rejected_is_fenced() {
    const FLOOR: Duration = Duration::from_secs(5);
    let (mock, bootstrap) = sequenced_mock(&["o1", "o2"]).await;
    mock.track_requests();
    mock.reject_commits(512);

    let handler = Recorder::new(|_, _| Outcome::Ack);
    let (mut group, token) = start_sequenced_group(&bootstrap, &handler, FLOOR).await;
    wait_until(
        || handler.count() == 2,
        DELIVERY_TIMEOUT,
        "both records reaching the handler",
    )
    .await;
    let first_delivery = handler.deliveries()[0].at;

    wait_until(
        || fence_probe::fired() >= 1,
        FLOOR + DELIVERY_TIMEOUT,
        "the fence firing on the rejected streak",
    )
    .await;
    assert_eq!(
        fence_probe::last_threshold(),
        Some(FLOOR),
        "the sequenced consumer judges rejected commits by the floor"
    );
    wait_until(
        || handler.count() >= 3,
        DELIVERY_TIMEOUT,
        "the reconnected member receiving the first record again",
    )
    .await;
    let until_fence = handler.deliveries()[2]
        .at
        .saturating_duration_since(first_delivery);
    assert!(
        until_fence >= FLOOR,
        "the fence waits out its floor, fired after {until_fence:?}"
    );
    assert_eq!(
        handler.ids()[..3],
        ["o1", "o2", "o1"],
        "the reconnect replays from the position nothing committed past"
    );
    assert!(
        mock.offset_commit_requests() >= 3,
        "the rejected commit was re-offered before the fence, requests: {}",
        mock.offset_commit_requests()
    );
    assert_eq!(
        committed_position_on(&bootstrap, SEQUENCED_TOPIC, SEQUENCED_GROUP, 0).await,
        None,
        "no commit was accepted"
    );

    token.cancel();
    group.shutdown().await;
}

/// A sequenced member that ends a connection on an error waits for its
/// consumer's close, off the runtime thread, before it reconnects. The fence
/// ends the connection, and the mock holds the answer to the LeaveGroup the
/// close sends: the reconnect's JoinGroup reaches the broker only once that
/// answer is in. A ticker on the same current-thread runtime keeps ticking
/// through the close.
#[tokio::test(flavor = "current_thread")]
async fn a_sequenced_error_exit_closes_before_it_reconnects() {
    const FLOOR: Duration = Duration::from_secs(5);
    let (mock, bootstrap) = sequenced_mock(&["o1"]).await;
    mock.track_requests();
    mock.reject_commits(512);
    mock.answer_next_late(
        RDKafkaApiKey::LeaveGroup,
        CLOSE_HOLD,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
    );

    let handler = Recorder::new(|_, _| Outcome::Ack);
    let ticker = Ticker::start();
    // Before the member starts: on this one thread, a close that blocked it
    // would block this task too.
    let started_at = Instant::now();
    let (mut group, token) = start_sequenced_group(&bootstrap, &handler, FLOOR).await;
    wait_until(
        || fence_probe::fired() >= 1,
        FLOOR + DELIVERY_TIMEOUT,
        "the fence ending the connection",
    )
    .await;
    wait_until(
        || handler.count() >= 2,
        CLOSE_HOLD + DELIVERY_TIMEOUT,
        "the reconnected member receiving o1 again",
    )
    .await;

    let left = *mock
        .request_timestamps(RDKafkaApiKey::LeaveGroup)
        .first()
        .expect("the close sent a LeaveGroup");
    let rejoined = mock
        .request_timestamps(RDKafkaApiKey::JoinGroup)
        .into_iter()
        .find(|&joined| joined > left)
        .expect("the reconnect sent a JoinGroup after the LeaveGroup");
    let gap = Duration::from_micros(u64::try_from(rejoined - left).expect("arrival order"));
    assert!(
        gap >= CLOSE_HOLD,
        "the reconnect waits for the close, which waits for the held LeaveGroup answer: gap {gap:?}"
    );
    let stall = ticker.longest_gap_since(started_at);
    assert!(
        stall < TICK_STALL,
        "the close waited for the held LeaveGroup off the runtime thread, the ticker stopped for {stall:?}"
    );

    token.cancel();
    group.shutdown().await;
}

/// Dropping the future of a direct `run_fifo` stops its shard. The run's
/// task is aborted while its member holds the topic's partition, which
/// drops the future that waits for the shard; the shard task is aborted
/// with it, and the consumer it leaves behind closes off the runtime thread
/// and leaves its group. A record produced afterwards reaches no handler.
#[tokio::test]
async fn dropping_a_direct_run_fifo_stops_its_shard() {
    const LEAVE_TIMEOUT: Duration = Duration::from_secs(10);
    let (mock, bootstrap) = sequenced_mock(&["o1"]).await;
    mock.track_requests();

    let handler = Recorder::new(|_, _| Outcome::Ack);
    let client = connect(&bootstrap).await;
    let h = handler.clone();
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run_fifo::<SequencedOrders, _>(h, (), ConsumerOptions::<Kafka>::new())
            .await
    });
    wait_until(
        || handler.count() >= 1,
        DELIVERY_TIMEOUT,
        "o1 reaching the handler",
    )
    .await;

    run.abort();
    let _ = run.await;
    wait_until(
        || mock.requests_of(RDKafkaApiKey::LeaveGroup) >= 1,
        LEAVE_TIMEOUT,
        "the dropped run's shard leaving its group",
    )
    .await;
    produce_on(&bootstrap, SEQUENCED_TOPIC, &["o2"]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        handler.ids(),
        ["o1"],
        "a stopped shard hands no later record to the handler"
    );
}

/// The text of every event logged in this process, as `field=value` pairs,
/// for a test that reads what a run only logs. Installed as the global
/// subscriber: one per test process, which nextest gives every test.
#[derive(Clone, Default)]
struct Logged(Arc<Mutex<Vec<String>>>);

impl Logged {
    fn install() -> Self {
        use tracing::subscriber::set_global_default;
        use tracing_subscriber::layer::SubscriberExt;
        let logged = Self::default();
        set_global_default(tracing_subscriber::registry().with(logged.clone()))
            .expect("install the log capture");
        logged
    }

    fn lines(&self) -> Vec<String> {
        self.0.lock().expect("log mutex poisoned").clone()
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Logged {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        use tracing::field::Field;
        let mut line = String::new();
        event.record(&mut |field: &Field, value: &dyn std::fmt::Debug| {
            line.push_str(&format!("{}={value:?} ", field.name()));
        });
        self.0.lock().expect("log mutex poisoned").push(line);
    }
}

/// The same stop on a sequenced consumer: its commit in flight has no
/// answer inside the share, so the member ends with `ShoveError::Commit` of
/// the `Deadline` kind carrying it, and no second commit is made.
/// `run_fifo_until_timeout` counts that error under `errors` and logs it,
/// so the kind is read from the log.
#[tokio::test]
async fn a_sequenced_stop_with_its_commit_unanswered_reports_the_pending_share() {
    let logged = Logged::install();
    let share = pending_commit_budget_for_test();
    let (mock, bootstrap) = sequenced_mock(&["o1"]).await;
    mock.track_requests();

    let handler = Recorder::new(|_, _| Outcome::Ack);
    let stop = CancellationToken::new();
    let signal = stop.clone().cancelled_owned();
    let client = connect(&bootstrap).await;
    let h = handler.clone();
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run_fifo_until_timeout::<SequencedOrders, _, _>(
                h,
                (),
                ConsumerOptions::<Kafka>::new(),
                signal,
                shutdown_commit_deadline_for_test() + DELIVERY_TIMEOUT,
            )
            .await
    });
    take_the_broker_down_under_a_commit(&mock).await;
    let stopped_at = Instant::now();
    stop.cancel();
    let outcome = run.await.expect("the run task completes");
    let took = stopped_at.elapsed();

    assert_eq!(
        (outcome.errors, outcome.panics, outcome.timed_out),
        (1, 0, false),
        "the stop ends the member with its error: {outcome:?}"
    );
    let kind = CommitFailure::Deadline(share).to_string();
    let reported = logged
        .lines()
        .into_iter()
        .filter(|line| line.contains("offset commit at shutdown"))
        .collect::<Vec<_>>();
    assert!(
        !reported.is_empty() && reported.iter().all(|line| line.contains(&kind)),
        "the member's error is the Deadline kind carrying the share, `{kind}`: {reported:?}"
    );
    assert!(
        took >= share - Duration::from_millis(500) && took < shutdown_commit_deadline_for_test(),
        "the stop waits out its share and ends inside the deadline, took {took:?}"
    );
    assert_eq!(
        mock.offset_commit_requests(),
        1,
        "no second commit races the one in flight"
    );
    assert_eq!(handler.ids(), ["o1"]);
}

// ---------------------------------------------------------------------------
// The DLQ drain: its stop tokens, its handler call and its commits
// ---------------------------------------------------------------------------

const DRAIN_TOPIC: &str = "kafka-commit-policy-mock-drain";
const DRAIN_DLQ: &str = "kafka-commit-policy-mock-drain-dlq";
/// The group a drain of the topic's DLQ joins: `{dlq}-consumer`.
const DRAIN_GROUP: &str = "kafka-commit-policy-mock-drain-dlq-consumer";
/// How long a test waits for a drain that has to stop, or to get past a
/// handler call that hangs.
const DRAIN_BOUND: Duration = Duration::from_secs(15);

shove::define_topic!(
    DrainOrders,
    Order,
    TopologyBuilder::new(DRAIN_TOPIC).dlq().build()
);

/// What a [`Dead`] handler does with the dead letter it is told to fail.
#[derive(Clone, Copy)]
enum Fail {
    Panic,
    Hang,
    /// Returns once the test calls [`Dead::release`].
    Held,
}

/// Records the id of every dead letter a drain hands it, and fails each one
/// `fail` names.
#[derive(Clone, Default)]
struct Dead {
    seen: Arc<Mutex<Vec<String>>>,
    fail: Vec<(&'static str, Fail)>,
    released: Arc<Notify>,
}

impl Dead {
    /// This handler, failing the dead letter `id` as `how` says.
    fn failing(mut self, id: &'static str, how: Fail) -> Self {
        self.fail.push((id, how));
        self
    }

    /// Lets the call on a [`Fail::Held`] dead letter return.
    fn release(&self) {
        self.released.notify_one();
    }

    fn ids(&self) -> Vec<String> {
        self.seen.lock().expect("handler mutex poisoned").clone()
    }

    fn count(&self) -> usize {
        self.seen.lock().expect("handler mutex poisoned").len()
    }
}

impl MessageHandler<DrainOrders> for Dead {
    type Context = ();

    async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }

    async fn handle_dead(&self, msg: Order, _: DeadMessageMetadata, _: &()) {
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .push(msg.id.clone());
        match self.fail.iter().find(|(id, _)| *id == msg.id) {
            Some((id, Fail::Panic)) => panic!("handle_dead fails on {id}"),
            Some((_, Fail::Hang)) => std::future::pending::<()>().await,
            Some((_, Fail::Held)) => self.released.notified().await,
            None => {}
        }
    }
}

/// One mock broker with the drain's DLQ created, and a dead letter produced
/// onto it for each of `ids`.
async fn drain_mock(ids: &[&str]) -> (Mock, String) {
    let mock = Mock::start();
    mock.api()
        .create_topic(DRAIN_DLQ, 1, 1)
        .expect("create the topic through the mock API");
    let bootstrap = mock.bootstrap();
    produce_on(&bootstrap, DRAIN_DLQ, ids).await;
    (mock, bootstrap)
}

type Drain = tokio::task::JoinHandle<Result<(), ShoveError>>;

/// Runs a drain of the topic's DLQ on its own task.
fn start_drain(client: KafkaClient, handler: Dead, options: ConsumerOptions<Kafka>) -> Drain {
    tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run_dlq_with_options::<DrainOrders, _>(handler, (), options)
            .await
    })
}

/// [`wait_until`] that fails at once if the drain ends before `done` holds.
async fn wait_while_draining(run: &Drain, done: impl Fn() -> bool, timeout: Duration, what: &str) {
    wait_until(
        || {
            assert!(!run.is_finished(), "the drain ended before {what}");
            done()
        },
        timeout,
        what,
    )
    .await;
}

/// Stops a drain through `shutdown` and requires it to end clean, inside
/// [`DRAIN_BOUND`].
async fn stop_drain(run: Drain, shutdown: &CancellationToken) {
    shutdown.cancel();
    tokio::time::timeout(DRAIN_BOUND, run)
        .await
        .expect("the drain stops on the token of its options")
        .expect("the drain task completes")
        .expect("a clean stop");
}

/// A `handle_dead` that panics costs the drain that one call. The panic is
/// caught where the call is made and logged, the dead letter behind the
/// panicking one is handled, both are committed, and the drain goes on
/// running. A panic that unwound the drain's task ended the drain with the
/// dead letter behind it unread.
#[tokio::test]
async fn dlq_drain_survives_a_panicking_handle_dead() {
    let logged = Logged::install();
    let (_mock, bootstrap) = drain_mock(&["A", "B"]).await;

    let handler = Dead::default().failing("A", Fail::Panic);
    let shutdown = CancellationToken::new();
    let run = start_drain(
        connect(&bootstrap).await,
        handler.clone(),
        ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone()),
    );
    wait_while_draining(
        &run,
        || handler.count() == 2,
        DELIVERY_TIMEOUT,
        "B reaching handle_dead behind the panicking A",
    )
    .await;
    wait_for_position(
        &bootstrap,
        DRAIN_DLQ,
        DRAIN_GROUP,
        2,
        DELIVERY_TIMEOUT,
        "A, which panicked, committed with B",
    )
    .await;
    assert!(!run.is_finished(), "the panic did not end the drain");
    assert_eq!(
        handler.ids(),
        ["A", "B"],
        "each dead letter is handled once"
    );
    assert!(
        logged
            .lines()
            .iter()
            .any(|line| line.contains("handle_dead panicked")),
        "the panic is logged: {:?}",
        logged.lines()
    );

    stop_drain(run, &shutdown).await;
}

/// A `handle_dead` that outlasts the handler timeout is cut there, logged,
/// and its dead letter committed: the drain does not wait for a call that
/// never returns. The timeout is the options' own.
#[tokio::test]
async fn dlq_drain_cuts_a_hung_handle_dead_at_the_handler_timeout() {
    const CALL_LIMIT: Duration = Duration::from_millis(500);
    let logged = Logged::install();
    let (_mock, bootstrap) = drain_mock(&["A", "B"]).await;

    let handler = Dead::default().failing("A", Fail::Hang);
    let shutdown = CancellationToken::new();
    let run = start_drain(
        connect(&bootstrap).await,
        handler.clone(),
        ConsumerOptions::<Kafka>::new()
            .with_handler_timeout(CALL_LIMIT)
            .with_shutdown(shutdown.clone()),
    );
    wait_while_draining(
        &run,
        || handler.count() == 2,
        DRAIN_BOUND,
        "B reaching handle_dead behind the hung A",
    )
    .await;
    wait_for_position(
        &bootstrap,
        DRAIN_DLQ,
        DRAIN_GROUP,
        2,
        DRAIN_BOUND,
        "A, which timed out, committed with B",
    )
    .await;
    assert!(
        logged
            .lines()
            .iter()
            .any(|line| line.contains("handle_dead timed out")),
        "the timeout is logged: {:?}",
        logged.lines()
    );

    stop_drain(run, &shutdown).await;
}

/// `without_handler_timeout` leaves `handle_dead` unbounded on the drain. A
/// call that runs past the 30 s default is waited for: the dead letter behind
/// it is not handed out, and nothing is committed, until the call returns, and
/// both are committed then. A drain that bounded the call at the default cut
/// it there and went on to the next dead letter.
#[tokio::test]
async fn dlq_drain_without_a_handler_timeout_waits_for_handle_dead() {
    const PAST_THE_DEFAULT: Duration = Duration::from_secs(2);
    let (_mock, bootstrap) = drain_mock(&["A", "B"]).await;

    let handler = Dead::default().failing("A", Fail::Held);
    let shutdown = CancellationToken::new();
    let run = start_drain(
        connect(&bootstrap).await,
        handler.clone(),
        ConsumerOptions::<Kafka>::new()
            .without_handler_timeout()
            .with_shutdown(shutdown.clone()),
    );
    wait_while_draining(
        &run,
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "A reaching handle_dead",
    )
    .await;

    tokio::time::sleep(DEFAULT_HANDLER_TIMEOUT + PAST_THE_DEFAULT).await;
    assert!(!run.is_finished(), "the drain waits on the call");
    assert_eq!(
        handler.ids(),
        ["A"],
        "B is not handed out while the call on A runs past the default timeout"
    );
    assert_eq!(
        committed_position_on(&bootstrap, DRAIN_DLQ, DRAIN_GROUP, 0).await,
        None,
        "nothing is committed while the call on A runs"
    );

    handler.release();
    wait_while_draining(
        &run,
        || handler.count() == 2,
        DRAIN_BOUND,
        "B reaching handle_dead once the call on A returned",
    )
    .await;
    wait_for_position(
        &bootstrap,
        DRAIN_DLQ,
        DRAIN_GROUP,
        2,
        DRAIN_BOUND,
        "A and B committed",
    )
    .await;

    stop_drain(run, &shutdown).await;
}

/// The names of `topic`'s series in one draining snapshot of the debugging
/// recorder, sorted and without repeats.
#[cfg(feature = "metrics")]
fn series_of(snapshotter: &Snapshotter, topic: &str) -> Vec<String> {
    let mut names: Vec<String> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key()
                .labels()
                .any(|label| label.key() == "topic" && label.value() == topic)
        })
        .map(|(key, ..)| key.key().name().to_owned())
        .collect();
    names.sort();
    names.dedup();
    names
}

/// A `handle_dead` call moves no handler metric under the source topic,
/// whether it returns, panics or outlasts the handler timeout: no consumed,
/// in-flight, duration or failed series. The size sample of each dead letter
/// is the one series the drain records there.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn dlq_drain_handler_calls_move_no_handler_metric() {
    let snapshotter = install_recorder();
    let (_mock, bootstrap) = drain_mock(&["A", "B", "C"]).await;

    let handler = Dead::default()
        .failing("A", Fail::Panic)
        .failing("B", Fail::Hang);
    let shutdown = CancellationToken::new();
    let run = start_drain(
        connect(&bootstrap).await,
        handler.clone(),
        ConsumerOptions::<Kafka>::new()
            .with_handler_timeout(Duration::from_millis(500))
            .with_shutdown(shutdown.clone()),
    );
    wait_while_draining(
        &run,
        || handler.count() == 3,
        DRAIN_BOUND,
        "C reaching handle_dead behind the panicking A and the hung B",
    )
    .await;
    wait_for_position(
        &bootstrap,
        DRAIN_DLQ,
        DRAIN_GROUP,
        3,
        DRAIN_BOUND,
        "all three dead letters committed",
    )
    .await;
    stop_drain(run, &shutdown).await;

    assert_eq!(
        series_of(&snapshotter, DRAIN_TOPIC),
        ["shove_message_size_bytes"],
        "the drain records the size of each dead letter and nothing about the calls"
    );
}

/// The token of the options stops the drain, with the client's token never
/// fired. The stop commits the dead letter in hand.
#[tokio::test]
async fn dlq_drain_stops_on_options_shutdown() {
    let (_mock, bootstrap) = drain_mock(&["A"]).await;

    let client = connect(&bootstrap).await;
    let client_token = client.shutdown_token();
    let handler = Dead::default();
    let shutdown = CancellationToken::new();
    let run = start_drain(
        client,
        handler.clone(),
        ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone()),
    );
    wait_while_draining(
        &run,
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "A reaching handle_dead",
    )
    .await;

    shutdown.cancel();
    tokio::time::timeout(DRAIN_BOUND, run)
        .await
        .expect("the drain stops on the token of its options")
        .expect("the drain task completes")
        .expect("a clean stop");
    assert!(
        !client_token.is_cancelled(),
        "the stop did not go through the client's token"
    );
    assert_eq!(
        committed_position_on(&bootstrap, DRAIN_DLQ, DRAIN_GROUP, 0).await,
        Some(1),
        "the stop committed A"
    );
}

/// The client's token stops a drain whose options carry a token of their own
/// that nothing fires, as it stops one that carries none.
#[tokio::test]
async fn dlq_drain_stops_on_the_clients_token() {
    let (_mock, bootstrap) = drain_mock(&["A"]).await;

    let client = connect(&bootstrap).await;
    let client_token = client.shutdown_token();
    let handler = Dead::default();
    let options_token = CancellationToken::new();
    let run = start_drain(
        client,
        handler.clone(),
        ConsumerOptions::<Kafka>::new().with_shutdown(options_token.clone()),
    );
    wait_while_draining(
        &run,
        || handler.count() == 1,
        DELIVERY_TIMEOUT,
        "A reaching handle_dead",
    )
    .await;

    client_token.cancel();
    tokio::time::timeout(DRAIN_BOUND, run)
        .await
        .expect("the drain stops on the client's token")
        .expect("the drain task completes")
        .expect("a clean stop");
    assert!(
        !options_token.is_cancelled(),
        "the stop left the options' token alone"
    );
}

/// The drain does not commit once per dead letter. Forty dead letters are
/// handled and committed, and the broker receives far fewer OffsetCommit
/// requests than that: the drain commits at most once per interval, one
/// commit at a time. An asynchronous commit per record, whose answer nothing
/// read, sent forty.
#[tokio::test]
async fn dlq_drain_commits_are_gated() {
    const LETTERS: usize = 40;
    let ids: Vec<String> = (0..LETTERS).map(|n| format!("d{n}")).collect();
    let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let (mock, bootstrap) = drain_mock(&refs).await;
    mock.track_requests();

    let handler = Dead::default();
    let shutdown = CancellationToken::new();
    let run = start_drain(
        connect(&bootstrap).await,
        handler.clone(),
        ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone()),
    );
    wait_while_draining(
        &run,
        || handler.count() == LETTERS,
        DELIVERY_TIMEOUT,
        "every dead letter reaching handle_dead",
    )
    .await;
    wait_for_position(
        &bootstrap,
        DRAIN_DLQ,
        DRAIN_GROUP,
        i64::try_from(LETTERS).expect("a small count"),
        DELIVERY_TIMEOUT,
        "every dead letter committed",
    )
    .await;

    let commits = mock.offset_commit_requests();
    assert!(
        commits < LETTERS / 2,
        "{commits} OffsetCommit requests for {LETTERS} dead letters"
    );
    assert_eq!(
        handler.ids(),
        ids,
        "each dead letter is handled once, in order"
    );

    stop_drain(run, &shutdown).await;
}

/// A commit the coordinator refuses is offered again. Every OffsetCommit is
/// refused for two seconds, which the three dead letters are handled inside
/// of, and then accepted. The position still reaches the end of what was
/// handled, on the same connection and with no dead letter handled twice. A
/// refusal that nothing read left the position where it was.
#[tokio::test]
async fn dlq_drain_re_offers_a_rejected_commit() {
    const REFUSING: Duration = Duration::from_secs(2);
    let (mock, bootstrap) = drain_mock(&["A", "B", "C"]).await;
    mock.track_requests();
    mock.reject_commits(512);

    let handler = Dead::default();
    let shutdown = CancellationToken::new();
    let run = start_drain(
        connect(&bootstrap).await,
        handler.clone(),
        ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone()),
    );
    wait_while_draining(
        &run,
        || handler.count() == 3,
        DELIVERY_TIMEOUT,
        "three dead letters reaching handle_dead",
    )
    .await;
    wait_while_draining(
        &run,
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "a commit reaching the broker",
    )
    .await;
    let assigns = per_record_probe::assigns_queued();
    tokio::time::sleep(REFUSING).await;
    assert_eq!(
        committed_position_on(&bootstrap, DRAIN_DLQ, DRAIN_GROUP, 0).await,
        None,
        "every commit inside the window was refused"
    );

    mock.accept_commits();
    wait_for_position(
        &bootstrap,
        DRAIN_DLQ,
        DRAIN_GROUP,
        3,
        DRAIN_BOUND,
        "the refused position offered again",
    )
    .await;
    assert_eq!(
        handler.ids(),
        ["A", "B", "C"],
        "the position was offered again, and no dead letter was handled again"
    );
    assert_eq!(
        per_record_probe::assigns_queued(),
        assigns,
        "the re-offer landed on the same connection"
    );

    stop_drain(run, &shutdown).await;
}

/// The stop commits what the drain handled. Every commit the drain makes
/// while it runs is refused. Right after a refused re-offer, with the next
/// one an interval away, the coordinator accepts again and the drain is
/// stopped. The last OffsetCommit the broker receives comes after the stop,
/// and the position it leaves is that of all three dead letters, so the
/// stop's final commit carried it. A stop with no final commit of its own
/// left the position to whichever re-offer landed before it.
#[tokio::test]
async fn dlq_drain_stop_commits_what_it_handled() {
    let (mock, bootstrap) = drain_mock(&["A", "B", "C"]).await;
    mock.track_requests();
    mock.reject_commits(512);

    let handler = Dead::default();
    let shutdown = CancellationToken::new();
    let run = start_drain(
        connect(&bootstrap).await,
        handler.clone(),
        ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone()),
    );
    wait_while_draining(
        &run,
        || handler.count() == 3,
        DELIVERY_TIMEOUT,
        "three dead letters reaching handle_dead",
    )
    .await;
    wait_while_draining(
        &run,
        || mock.offset_commit_requests() >= 1,
        DELIVERY_TIMEOUT,
        "a commit reaching the broker",
    )
    .await;
    assert_eq!(
        committed_position_on(&bootstrap, DRAIN_DLQ, DRAIN_GROUP, 0).await,
        None,
        "the commits made while the drain ran were refused"
    );

    let refused = mock.offset_commit_requests();
    wait_while_draining(
        &run,
        || mock.offset_commit_requests() > refused,
        DELIVERY_TIMEOUT,
        "a refused position offered again",
    )
    .await;
    mock.accept_commits();
    let before_stop = mock.offset_commit_requests();
    stop_drain(run, &shutdown).await;
    let after_stop = mock.offset_commit_requests();
    assert!(
        after_stop > before_stop,
        "the last OffsetCommit followed the stop: {before_stop} before it, {after_stop} after"
    );
    assert_eq!(
        committed_position_on(&bootstrap, DRAIN_DLQ, DRAIN_GROUP, 0).await,
        Some(3),
        "the final commit carried the position of the three dead letters"
    );
}

/// A drain whose task is aborted closes its consumer off the runtime thread.
/// The dead letter's commit has landed, so nothing is in flight when the
/// abort drops the drain where it waits; the close it leaves behind waits for
/// the held LeaveGroup answer while a ticker on the same current-thread
/// runtime keeps ticking.
#[tokio::test(flavor = "current_thread")]
async fn an_aborted_dlq_drain_closes_off_runtime() {
    let (mock, bootstrap) = drain_mock(&["A"]).await;
    mock.track_requests();
    mock.answer_next_late(
        RDKafkaApiKey::LeaveGroup,
        CLOSE_HOLD,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
    );

    let handler = Dead::default();
    let ticker = Ticker::start();
    let run = start_drain(
        connect(&bootstrap).await,
        handler.clone(),
        ConsumerOptions::<Kafka>::new().with_shutdown(CancellationToken::new()),
    );
    wait_for_position(
        &bootstrap,
        DRAIN_DLQ,
        DRAIN_GROUP,
        1,
        DELIVERY_TIMEOUT,
        "A committed",
    )
    .await;

    let aborted_at = Instant::now();
    run.abort();
    let _ = run.await;
    tokio::time::sleep(CLOSE_HOLD + TICK_STALL).await;
    assert!(
        mock.requests_of(RDKafkaApiKey::LeaveGroup) >= 1,
        "the aborted drain's close reached the coordinator"
    );
    let stall = ticker.longest_gap_since(aborted_at);
    assert!(
        stall < TICK_STALL,
        "the close waited for the held LeaveGroup off the runtime thread, the ticker stopped for {stall:?}"
    );
    assert_eq!(handler.ids(), ["A"]);
}

/// A drain that ends a connection on an error waits for its consumer's close,
/// off the runtime thread, before it reconnects. The broker refuses one fetch
/// for authorization, which librdkafka reports to the consumer once and
/// leaves to the reconnect, and the mock holds the answer to the LeaveGroup
/// the close sends: the reconnect's JoinGroup reaches the broker only once
/// that answer is in. A ticker on the same current-thread runtime keeps
/// ticking through the close.
#[tokio::test(flavor = "current_thread")]
async fn a_dlq_drain_error_exit_closes_before_it_reconnects() {
    let (mock, bootstrap) = drain_mock(&["A"]).await;
    mock.track_requests();
    mock.answer_next_late(
        RDKafkaApiKey::LeaveGroup,
        CLOSE_HOLD,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
    );

    let handler = Dead::default();
    let ticker = Ticker::start();
    let shutdown = CancellationToken::new();
    let run = start_drain(
        connect(&bootstrap).await,
        handler.clone(),
        ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone()),
    );
    wait_for_position(
        &bootstrap,
        DRAIN_DLQ,
        DRAIN_GROUP,
        1,
        DELIVERY_TIMEOUT,
        "A committed",
    )
    .await;

    let failing_at = Instant::now();
    mock.api().request_errors(
        RDKafkaApiKey::Fetch,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
    );
    let joins_before = mock.requests_of(RDKafkaApiKey::JoinGroup);
    wait_while_draining(
        &run,
        || mock.requests_of(RDKafkaApiKey::JoinGroup) > joins_before,
        CLOSE_HOLD + DELIVERY_TIMEOUT,
        "the reconnected drain joining the group again",
    )
    .await;

    let left = *mock
        .request_timestamps(RDKafkaApiKey::LeaveGroup)
        .first()
        .expect("the close sent a LeaveGroup");
    let rejoined = mock
        .request_timestamps(RDKafkaApiKey::JoinGroup)
        .into_iter()
        .find(|&joined| joined > left)
        .expect("the reconnect sent a JoinGroup after the LeaveGroup");
    let gap = Duration::from_micros(u64::try_from(rejoined - left).expect("arrival order"));
    assert!(
        gap >= CLOSE_HOLD,
        "the reconnect waits for the close, which waits for the held LeaveGroup answer: gap {gap:?}"
    );
    let stall = ticker.longest_gap_since(failing_at);
    assert!(
        stall < TICK_STALL,
        "the close waited for the held LeaveGroup off the runtime thread, the ticker stopped for {stall:?}"
    );

    stop_drain(run, &shutdown).await;
}

// ---------------------------------------------------------------------------
// A broker bounce ridden out on the client the member started with, one soon
// after a rebalance that reconnects, and a stop that keeps polling while a
// handler holds the permit
// ---------------------------------------------------------------------------

/// A broker that goes away for a few seconds and comes back costs the member
/// nothing but the outage. librdkafka reports the refused reconnects, and
/// every broker being down, as consumer errors and reconnects on its own, so
/// the loop logs them and goes on polling on the same client: the
/// `client.id` the member started with is the only one it ever has, and the
/// record produced after the bounce reaches the handler. Red while every
/// receive error ended the loop and reconnected it on a fresh client.
#[tokio::test]
async fn broker_bounce_does_not_rejoin() {
    const BOUNCE: Duration = Duration::from_secs(3);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1"]).await;

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::Interval(Duration::from_millis(500)), token)
    });
    wait_for_committed_position(&bootstrap, 1, DELIVERY_TIMEOUT).await;
    let clients = client_id_probe::created();
    assert_eq!(
        clients.len(),
        1,
        "one consumer before the bounce: {clients:?}"
    );

    mock.api()
        .broker_down(BROKER_ID)
        .expect("take the mock broker down");
    tokio::time::sleep(BOUNCE).await;
    mock.api()
        .broker_up(BROKER_ID)
        .expect("bring the mock broker back");

    produce(&bootstrap, &["o2"]).await;
    wait_until(
        || handler.count() == 2,
        DELIVERY_TIMEOUT,
        "the record produced after the bounce reaching the handler",
    )
    .await;
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    assert_eq!(
        client_id_probe::created(),
        clients,
        "the member rode out the bounce on the client it started with"
    );
    assert_eq!(handler.ids(), ["o1", "o2"]);

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// A broker bounce soon after a rebalance ends a member's loop, and the
/// member reconnects on a fresh client. The rebalance may have changed the
/// assignment while a committed-offset fetch was pending, and librdkafka's
/// only trace of a fetch that change stranded is a `__TRANSPORT`, the code a
/// failed reconnect reports too (bundled librdkafka 2.12.1,
/// `rdkafka_assignment.c:283-298`). Member A takes both partitions of the
/// return topic, and B joins straight after, so the cooperative rebalance
/// revokes one of A's partitions within seconds of A's assign.
///
/// The broker then stays down until a member has reconnected. A member's
/// first reconnect to it fails at once where the host refuses the
/// connection, and once the connect times out where the host lets it hang,
/// after about 8 s on macOS; either way inside the default
/// `socket.timeout.ms`, 60 s, of the rebalance. A 3 s bounce on macOS
/// surfaces no `__TRANSPORT` at all, only every broker being down. The
/// close that ends the member's loop waits out the shutdown deadline against
/// the down broker, and the member reconnects after it. Red while every
/// `__TRANSPORT` was ridden out, whatever the rebalance callback had served.
#[tokio::test]
async fn broker_bounce_soon_after_a_rebalance_reconnects() {
    /// The bound for a wait that spans a rebalance: the mock coordinator
    /// waits up to `session.timeout.ms` for its members to rejoin.
    const REBALANCE_TIMEOUT: Duration = Duration::from_secs(60);
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(RETURN_TOPIC, 2, 1)
        .expect("create the return topic through the mock API");
    let (_release, released) = watch::channel(0);
    let a = Holding::new(released.clone());
    let (run_a, token_a) = start_on_return_topic(connect(&bootstrap).await, a.clone(), |token| {
        ConsumerOptions::<Kafka>::new().with_shutdown(token)
    });
    for partition in RETURN_PARTITIONS {
        produce_pinned(&bootstrap, &format!("a-{partition}"), partition).await;
    }
    wait_until(
        || a.seen().len() == RETURN_PARTITIONS.len(),
        DELIVERY_TIMEOUT,
        "A handling a record of each partition",
    )
    .await;

    let b = Holding::new(released);
    let (run_b, token_b) = start_on_return_topic(connect(&bootstrap).await, b.clone(), |token| {
        ConsumerOptions::<Kafka>::new().with_shutdown(token)
    });
    // Whichever partition moves to B, B has a record of it.
    for partition in RETURN_PARTITIONS {
        produce_pinned(&bootstrap, &format!("b-{partition}"), partition).await;
    }
    wait_until(
        || !b.seen().is_empty(),
        REBALANCE_TIMEOUT,
        "B handling a record of the partition it took",
    )
    .await;
    let before = client_id_probe::created();
    assert_eq!(
        before.len(),
        2,
        "one consumer per member before the bounce: {before:?}"
    );

    mock.api()
        .broker_down(BROKER_ID)
        .expect("take the mock broker down");
    wait_until(
        || client_id_probe::created().len() > before.len(),
        DELIVERY_TIMEOUT,
        "a member reconnecting on a fresh client while the broker is down",
    )
    .await;
    mock.api()
        .broker_up(BROKER_ID)
        .expect("bring the mock broker back");

    // The reconnect rebalances the group, so a final commit may be refused.
    token_a.cancel();
    token_b.cancel();
    let _ = run_a.await.expect("A's task completes");
    let _ = run_b.await.expect("B's task completes");
}

/// A stop that finds a handler holding the only permit keeps the member
/// polling while it waits for that permit: the drain polls with the
/// assignment paused and puts back what it is handed. Member A, a group
/// member with the poll interval at its 10 s floor, holds the head of
/// partition 0. A stops, and member B joins while A drains. The cooperative
/// rebalance cannot finish until A serves its revoke, which only a drain
/// that polls does. A's hold then runs past that interval and the session
/// timeout both. B handling a record of the partition it took, and no
/// LeaveGroup reaching the coordinator inside the hold, show the drain
/// served the rebalance and A stayed in its group: librdkafka leaves the
/// group for a member that outlasts its poll interval (bundled librdkafka
/// 2.12.1, `rdkafka_cgrp.c:5200-5201`). Its warning says so too, but it is
/// delivered through the consumer's own queue (rust-rdkafka 0.39,
/// `client.rs:257-276`), so a member that has stopped polling never logs
/// it; the LeaveGroup is the trace that does not depend on the member. Red
/// while the drain parked on the semaphore: A left its group at the poll
/// interval, and only then did B take its partitions.
#[tokio::test]
async fn shutdown_drain_keeps_polling() {
    /// How long past A's poll interval the coordinator's requests are read:
    /// librdkafka checks the interval on a timer of its own.
    const PAST_THE_INTERVAL: Duration = Duration::from_secs(3);
    /// The bound for a wait that spans a rebalance: the mock coordinator
    /// waits up to `session.timeout.ms` for its members to rejoin.
    const REBALANCE_TIMEOUT: Duration = Duration::from_secs(60);
    let logged = Logged::install();
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    mock.api()
        .create_topic(RETURN_TOPIC, 2, 1)
        .expect("create the return topic through the mock API");
    produce_pinned(&bootstrap, "hold-1", 0).await;
    mock.track_requests();

    let (release, released) = watch::channel(0);
    let a = Holding::new(released.clone());
    let token_a = CancellationToken::new();
    let h = a.clone();
    let mut group_a = KafkaConsumerGroup::new::<ReturnTopic, _>(
        RETURN_TOPIC,
        KafkaConsumerGroupConfig::new(1..=1)
            .with_prefetch_count(1)
            .with_max_poll_interval_for_test(SHORT_POLL_INTERVAL),
        connect(&bootstrap).await,
        token_a.clone(),
        move || h.clone(),
        (),
    );
    group_a.start();
    wait_until(
        || a.has("hold-1"),
        DELIVERY_TIMEOUT,
        "A holding the head of partition 0",
    )
    .await;

    token_a.cancel();
    let stopped_at = Instant::now();
    let b = Holding::new(released);
    let (run_b, token_b) = start_on_return_topic(connect(&bootstrap).await, b.clone(), |token| {
        ConsumerOptions::<Kafka>::new().with_shutdown(token)
    });
    // One record per partition behind the hold: whichever partition moves
    // to B, B has a record of it to handle.
    for partition in RETURN_PARTITIONS {
        produce_pinned(&bootstrap, &format!("behind-{partition}"), partition).await;
    }
    wait_until(
        || !b.seen().is_empty(),
        REBALANCE_TIMEOUT,
        "B handling a record of the partition it took",
    )
    .await;
    tokio::time::sleep_until(stopped_at + SHORT_POLL_INTERVAL + PAST_THE_INTERVAL).await;
    assert_eq!(
        mock.requests_of(RDKafkaApiKey::LeaveGroup),
        0,
        "no member left the group while A's drain waited out the hold"
    );
    let evicted: Vec<String> = logged
        .lines()
        .into_iter()
        .filter(|line| line.contains("maximum poll interval"))
        .collect();
    assert!(
        evicted.is_empty(),
        "A, polling through its drain, logged no eviction: {evicted:?}"
    );
    assert_eq!(
        a.seen()
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        ["hold-1"],
        "the drain handed A's handler nothing behind the held record"
    );

    // B stops inside the rebalance A's departure starts, so its final commit
    // may be refused; only the rebalance inside the hold matters here.
    release.send(1).expect("the handlers hold the receiver");
    group_a.shutdown().await;
    token_b.cancel();
    let _ = run_b.await.expect("B's task completes");
}

// ---------------------------------------------------------------------------
// A stop under a backlog, and the commit gate's wake after an idle spell
// ---------------------------------------------------------------------------

/// Records queued for the member, enough that one is always ready to receive
/// when the stop lands.
const BACKLOG: usize = 50;
/// The delivery whose handler stops the member.
const STOP_AT: usize = 10;
/// The commit gate's window in the gate test of the standard consumer: short
/// against the 5 s housekeeping tick, so a commit the gate's deadline woke is
/// told apart from one that waited for the tick.
const GATE_WINDOW: Duration = Duration::from_millis(1500);
/// The window a sequenced consumer commits on, its fixed default interval.
const SEQUENCED_WINDOW: Duration = Duration::from_millis(500);
/// How far a gated commit may land from the end of its window: the commit
/// thread's spawn and the request's way to the broker on one side, a busy
/// runner on the other.
const GATE_SLACK: Duration = Duration::from_millis(1000);

/// `BACKLOG` order ids, `o0` onwards.
fn backlog() -> Vec<String> {
    (0..BACKLOG).map(|i| format!("o{i}")).collect()
}

/// Acknowledges every record and counts it, and cancels `shutdown` from
/// inside the handler of delivery `STOP_AT`, so the stop lands while the
/// member holds that record with the backlog queued behind it.
#[derive(Clone)]
struct StopsAt {
    seen: Arc<AtomicUsize>,
    shutdown: CancellationToken,
}

impl StopsAt {
    fn new(shutdown: CancellationToken) -> Self {
        Self {
            seen: Arc::default(),
            shutdown,
        }
    }

    fn count(&self) -> usize {
        self.seen.load(Ordering::SeqCst)
    }
}

impl<T: Topic<Message = Order>> MessageHandler<T> for StopsAt {
    type Context = ();
    async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
        if self.seen.fetch_add(1, Ordering::SeqCst) + 1 == STOP_AT {
            self.shutdown.cancel();
        }
        Outcome::Ack
    }
}

/// A stop that lands while a backlog is queued ends the receive loop within
/// one pass: the record the member holds completes, nothing behind it
/// reaches the handler, and the final commit carries the position past the
/// last record handled. On one permit the next record waits for the one the
/// stopping handler holds, and the stop is read before that permit. The
/// handler acks at once, so no wait runs long enough to pause the assignment
/// and leave a spare permit, which would let a record skip the wait. The
/// records that entered the wait then count what the loop took: at most one
/// past the stop, where a loop that read the stop late would take the
/// backlog.
#[tokio::test]
async fn a_stop_under_a_backlog_hands_out_nothing_behind_the_record_in_hand() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    let ids = backlog();
    produce(
        &bootstrap,
        &ids.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;

    let shutdown = CancellationToken::new();
    let handler = StopsAt::new(shutdown.clone());
    let client = connect(&bootstrap).await;
    let h = handler.clone();
    let opts = one_permit(shutdown);
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run::<OrdersTopic, _>(h, (), opts)
            .await
    });
    tokio::time::timeout(DELIVERY_TIMEOUT, run)
        .await
        .expect("the stop ends the run")
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(
        handler.count(),
        STOP_AT,
        "nothing behind the record in hand reached the handler"
    );
    assert!(
        permit_wait_probe::entered() <= STOP_AT + 1,
        "the loop took at most one record past the stop: {} taken",
        permit_wait_probe::entered()
    );
    assert_eq!(
        committed_position(&bootstrap).await,
        Some(i64::try_from(STOP_AT).expect("a small count")),
        "the final commit carries every record handled"
    );
}

/// The same stop on a sequenced consumer: the record in hand is acked, and
/// the loop reads the stop before it takes the next one. The loop samples
/// the size of every record it takes, so with `metrics` on the samples
/// count what it took: none past the stop. A decode reads the stop too, so
/// the handler count alone would not tell a loop that took one record more.
#[tokio::test]
async fn a_sequenced_stop_under_a_backlog_hands_out_nothing_behind_the_record_in_hand() {
    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let ids = backlog();
    let (_mock, bootstrap) =
        sequenced_mock(&ids.iter().map(String::as_str).collect::<Vec<_>>()).await;

    let shutdown = CancellationToken::new();
    let handler = StopsAt::new(shutdown.clone());
    let client = connect(&bootstrap).await;
    let h = handler.clone();
    let opts = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown);
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run_fifo::<SequencedOrders, _>(h, (), opts)
            .await
    });
    tokio::time::timeout(DELIVERY_TIMEOUT, run)
        .await
        .expect("the stop ends the run")
        .expect("the run task completes")
        .expect("a clean stop");

    assert_eq!(
        handler.count(),
        STOP_AT,
        "nothing behind the record in hand reached the handler"
    );
    assert_eq!(
        committed_position_on(&bootstrap, SEQUENCED_TOPIC, SEQUENCED_GROUP, 0).await,
        Some(i64::try_from(STOP_AT).expect("a small count")),
        "the final commit carries every record handled"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        size_samples(&snapshotter, SEQUENCED_TOPIC),
        STOP_AT,
        "the loop took no record past the stop"
    );
}

/// How many `shove_message_size_bytes` samples `topic` has, from one
/// draining snapshot of the debugging recorder: one per record a loop took.
#[cfg(feature = "metrics")]
fn size_samples(snapshotter: &Snapshotter, topic: &str) -> usize {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| key.key().name() == "shove_message_size_bytes")
        .filter(|(key, ..)| {
            key.key()
                .labels()
                .any(|label| label.key() == "topic" && label.value() == topic)
        })
        .map(|(.., value)| match value {
            DebugValue::Histogram(samples) => samples.len(),
            other => panic!("shove_message_size_bytes is not a histogram: {other:?}"),
        })
        .sum()
}

/// The gap between the last two OffsetCommit requests the broker received.
fn last_commit_gap(mock: &Mock) -> Duration {
    let stamps = mock.request_timestamps(RDKafkaApiKey::OffsetCommit);
    let [.., opened, gated] = stamps[..] else {
        panic!("two commits reached the broker: {stamps:?}");
    };
    Duration::from_micros(u64::try_from(gated - opened).expect("arrival order"))
}

/// Asserts the last commit was gated: made `window` after the one before
/// it, give or take `GATE_SLACK`, on a single wake of the gate's deadline
/// since `woke_before` was read.
fn assert_gated(mock: &Mock, window: Duration, woke_before: usize) {
    let gap = last_commit_gap(mock);
    assert!(
        gap + GATE_SLACK >= window && gap <= window + GATE_SLACK,
        "the gated commit lands when its window ends, {window:?} after the one before it: \
         gap {gap:?}"
    );
    assert_eq!(
        commit_gate_probe::woke() - woke_before,
        1,
        "the gate's deadline woke the member once for the gated commit"
    );
}

/// A completion inside the commit gate's window, once the member has sat
/// idle past an earlier window, is committed when its window ends, on one
/// wake of the gate's deadline. The member commits its first record and
/// idles. A second record then finds the gate open and is committed at
/// once, which opens a fresh window, and a third, produced inside that
/// window, waits for its end on a single wake: the housekeeping tick is
/// seconds later, and a wake that fired on every pass would count more.
#[tokio::test]
async fn after_an_idle_spell_a_gated_commit_lands_when_its_window_ends() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, &["o1"]).await;
    mock.track_requests();

    let handler = Acking::default();
    let h = handler.clone();
    let (run, shutdown) = start(connect(&bootstrap).await, h, |token| {
        options(CommitPolicy::Interval(GATE_WINDOW), token)
    });
    wait_for_committed_position(&bootstrap, 1, DELIVERY_TIMEOUT).await;
    tokio::time::sleep(GATE_WINDOW * 2).await;

    produce(&bootstrap, &["o2"]).await;
    wait_for_committed_position(&bootstrap, 2, DELIVERY_TIMEOUT).await;
    let woke = commit_gate_probe::woke();
    produce(&bootstrap, &["o3"]).await;
    wait_for_committed_position(&bootstrap, 3, DELIVERY_TIMEOUT).await;
    assert_gated(&mock, GATE_WINDOW, woke);

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

/// The same on a sequenced consumer, which commits on its fixed default
/// interval. That window is shorter than `GATE_SLACK`, so the gap's lower
/// bound can never fail here, and the wake count is what pins the behaviour.
#[tokio::test]
async fn after_an_idle_spell_a_sequenced_gated_commit_lands_when_its_window_ends() {
    let (mock, bootstrap) = sequenced_mock(&["o1"]).await;
    mock.track_requests();

    let shutdown = CancellationToken::new();
    let handler = Recorder::new(|_, _| Outcome::Ack);
    let client = connect(&bootstrap).await;
    let h = handler.clone();
    let opts = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone());
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run_fifo::<SequencedOrders, _>(h, (), opts)
            .await
    });
    let committed = |position| {
        wait_for_position(
            &bootstrap,
            SEQUENCED_TOPIC,
            SEQUENCED_GROUP,
            position,
            DELIVERY_TIMEOUT,
            "the sequenced consumer's commit",
        )
    };
    committed(1).await;
    tokio::time::sleep(SEQUENCED_WINDOW * 4).await;

    produce_on(&bootstrap, SEQUENCED_TOPIC, &["o2"]).await;
    committed(2).await;
    let woke = commit_gate_probe::woke();
    produce_on(&bootstrap, SEQUENCED_TOPIC, &["o3"]).await;
    committed(3).await;
    assert_gated(&mock, SEQUENCED_WINDOW, woke);

    shutdown.cancel();
    run.await
        .expect("the run task completes")
        .expect("a clean stop");
}

// ---------------------------------------------------------------------------
// A broadcast subscription's consumer, closed off the runtime thread
// ---------------------------------------------------------------------------

const BROADCAST_TOPIC: &str = "kafka-commit-policy-mock-broadcast";
/// How long a broadcast subscription's teardown may take before the test
/// gives up on it.
const BROADCAST_DRAIN: Duration = Duration::from_secs(30);

shove::define_topic!(
    BroadcastOrders,
    Order,
    TopologyBuilder::new(BROADCAST_TOPIC).broadcast().build()
);

/// One mock broker with the broadcast topic created, and a subscriber on a
/// client of its own, subscribed with `options` and `handler`, which has
/// handled a record: the subscription starts at the tail, so records are
/// produced until one arrives. Requests are tracked from before the
/// subscription starts.
async fn live_broadcast_subscription(
    handler: &Recorder,
    options: ConsumerOptions<Kafka>,
) -> (Mock, BroadcastSubscriber<Kafka>) {
    let mock = Mock::start();
    mock.api()
        .create_topic(BROADCAST_TOPIC, 1, 1)
        .expect("create the topic through the mock API");
    mock.track_requests();
    let bootstrap = mock.bootstrap();
    let broker = Broker::<Kafka>::from_client(connect(&bootstrap).await);
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<BroadcastOrders, _>(handler.clone(), options)
        .expect("subscribe on the mock cluster");
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while handler.count() == 0 {
        assert!(
            Instant::now() < deadline,
            "no record reached the subscription within {DELIVERY_TIMEOUT:?}"
        );
        produce_on(&bootstrap, BROADCAST_TOPIC, &["b"]).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    (mock, subscriber)
}

/// The consumer of a subscription that has ended goes on fetching: it was
/// neither closed nor dropped, so its handle was leaked. A consumer closed
/// when its loop ended sends no request once the loop is gone.
async fn assert_the_consumer_was_leaked(mock: &Mock) {
    let fetches = mock.requests_of(RDKafkaApiKey::Fetch);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let later = mock.requests_of(RDKafkaApiKey::Fetch);
    assert!(
        later > fetches,
        "the consumer stopped fetching when its loop ended, so it was closed there, on the \
         runtime thread: {fetches} fetches then, {later} two seconds later"
    );
}

/// A broadcast subscription that stops closes its consumer on a thread of
/// its own, as every other consume loop does, and never on the runtime
/// thread. With no thread to be had for the close, the handle is leaked
/// rather than closed where the loop ran, so the consumer goes on fetching
/// after the subscription has ended cleanly.
#[tokio::test]
async fn a_stopped_broadcast_subscription_never_closes_its_consumer_on_the_runtime_thread() {
    let handler = Recorder::new(|_, _| Outcome::Ack);
    let (mock, subscriber) =
        live_broadcast_subscription(&handler, ConsumerOptions::<Kafka>::new()).await;

    final_commit_spawn_probe::refuse_threads(true);
    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending(), BROADCAST_DRAIN)
        .await;
    assert_eq!(
        (outcome.errors, outcome.panics, outcome.timed_out),
        (0, 0, false),
        "the stop ends the subscription cleanly: {outcome:?}"
    );
    assert_the_consumer_was_leaked(&mock).await;
}

/// The same on an error exit. The next fetch is answered with an unknown
/// broker error, which ends the subscription's connection, and its one
/// reconnect attempt is spent at once, so the subscription ends with the
/// error. With no thread to be had for the close, the handle is leaked
/// rather than closed where the loop ran. librdkafka backs the partition off
/// for `fetch.error.backoff.ms`, 500 ms, after that error, well inside the
/// window the leak is read in; a refused authorization would back it off for
/// 5 s.
#[tokio::test]
async fn a_broadcast_subscription_ending_on_an_error_never_closes_its_consumer_on_the_runtime_thread()
 {
    let handler = Recorder::new(|_, _| Outcome::Ack);
    let (mock, subscriber) = live_broadcast_subscription(
        &handler,
        ConsumerOptions::<Kafka>::new().with_max_reconnect_attempts(1),
    )
    .await;

    final_commit_spawn_probe::refuse_threads(true);
    let fetches = mock.requests_of(RDKafkaApiKey::Fetch);
    mock.api().request_errors(
        RDKafkaApiKey::Fetch,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN],
    );
    wait_until(
        || mock.requests_of(RDKafkaApiKey::Fetch) > fetches,
        DELIVERY_TIMEOUT,
        "the failed fetch reaching the broker",
    )
    .await;
    // The error reaches the loop with the fetch's answer; the exit follows.
    tokio::time::sleep(Duration::from_secs(2)).await;

    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending(), BROADCAST_DRAIN)
        .await;
    assert_eq!(
        (outcome.errors, outcome.panics, outcome.timed_out),
        (1, 0, false),
        "the failed fetch ended the subscription with its error: {outcome:?}"
    );
    assert_the_consumer_was_leaked(&mock).await;
}
