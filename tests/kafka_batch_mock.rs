//! The Kafka batch consumer (`KafkaConsumer::run_batch`) against librdkafka's
//! mock cluster (`rdkafka::mocking`), which serves the consumer group
//! protocol, offset commits and produce requests from a localhost listener
//! and needs no Docker. The mock lets a test refuse, delay or drop one kind
//! of request without touching the others.
//!
//! The first tests move a partition away from a member whose batch spans
//! it, by starting a second member in the same group: the cooperative
//! rebalance revokes one of the first member's two partitions. A record the
//! member holds when it learns of the revoke, a batch whose age deadline
//! fires with the revoke still undrained, and a rejected batch whose dead
//! letters are still landing when the revoke comes must not reach a handler
//! ahead of, or beside, the records the rebalance hands back or away, and
//! nothing of the revoked partition may be committed by the member that
//! lost it.
//!
//! The rest refuse or delay what a flush depends on. A commit refused with a
//! rebalance in progress is re-offered, a dead-lettered batch's included,
//! and the member stays in its group. A dead letter the broker refuses
//! redelivers the batch with nothing committed past it and no discard
//! counted. A dead-letter topic that answers slowly keeps the member
//! polling, so a rejected batch cannot outlast `max.poll.interval.ms`. A
//! stop's commits stay inside the shutdown deadline, a final commit the
//! broker refuses ends the member with `ShoveError::Commit`, and one refused
//! while the group rebalances is tried again.
//!
//! Every topic is created through the mock API, because the mock broker has
//! no CreateTopics API, and records are produced through a raw rdkafka
//! producer. The mock's per-request answer delay and its request log have no
//! wrapper in `rdkafka::mocking`, so `Mock` reaches them through
//! `rdkafka::bindings`.
//!
//! `test-support` gates the seams this file reads. The Kafka coverage row
//! enables it and runs this suite; the schema-registry row compiles the
//! binary too, but its test filter does not select it.

#![cfg(all(feature = "kafka", feature = "test-support"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::collections::HashSet;
use std::os::raw::c_int;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(feature = "metrics")]
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use rdkafka::bindings::{
    rd_kafka_handle_mock_cluster, rd_kafka_mock_broker_push_request_error_rtts,
    rd_kafka_mock_get_requests, rd_kafka_mock_request_api_key, rd_kafka_mock_request_destroy_array,
    rd_kafka_mock_start_request_tracking,
};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{
    BaseProducer, DefaultProducerContext, FutureProducer, FutureRecord, Producer,
};
use rdkafka::types::{RDKafkaApiKey, RDKafkaMockCluster, RDKafkaRespErr};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use shove::handler::BatchMessageHandler;
use shove::kafka::{
    BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer, client_id_probe,
    pending_commit_budget_for_test, per_record_probe, put_back_probe,
    shutdown_commit_deadline_for_test,
};
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::TopologyBuilder;
use shove::{CommitFailure, ShoveError};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// The mock broker's id with one broker configured.
const BROKER_ID: i32 = 1;
/// A record reaches the handler on a mock cluster in well under this.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// How many sends shove's producer makes of one record before it gives up
/// on it, its `MAX_PUBLISH_ATTEMPTS`: refusing this many Produce requests
/// fails exactly one dead letter.
const SENDS_PER_PUBLISH: usize = 3;
/// The shortest `max.poll.interval.ms` librdkafka admits, the pinned
/// `session.timeout.ms`; a member that stops polling for longer leaves its
/// group.
const SHORT_POLL_INTERVAL: Duration = Duration::from_secs(10);
/// How long past the shutdown deadline a test waits for a stop before it
/// gives up on it.
const STOP_MARGIN: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
}

/// One mock broker, and the per-request answer delay `rdkafka::mocking`
/// does not wrap. The cluster is the one a producer configured with
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
        Self { owner, cluster }
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

    fn create_topic(&self, topic: &str, partitions: i32) {
        self.api()
            .create_topic(topic, partitions, 1)
            .expect("create the topic through the mock API");
    }

    /// Answer the next `count` requests of `api_key` with `err`, whichever
    /// client sends them; the ones after are answered as usual.
    fn refuse(&self, api_key: RDKafkaApiKey, err: RDKafkaRespErr, count: usize) {
        self.api().request_errors(api_key, &vec![err; count]);
    }

    /// Hold the answer to each of the next `count` requests of `api_key` for
    /// `delay`, then answer it with no error. The broker answers one
    /// connection's requests in order, and each delay runs from its own
    /// request's arrival, so requests sent together are held together.
    fn answer_late(&self, api_key: RDKafkaApiKey, delay: Duration, count: usize) {
        let delay_ms = c_int::try_from(delay.as_millis()).expect("delay fits a C int");
        for _ in 0..count {
            let err = unsafe {
                rd_kafka_mock_broker_push_request_error_rtts(
                    self.cluster,
                    BROKER_ID,
                    i16::from(api_key),
                    1,
                    RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR as c_int,
                    delay_ms,
                )
            };
            assert_eq!(
                err,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
                "push the delayed answer onto the mock broker"
            );
        }
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

    /// Take the broker down, as a coordinator that drops off the network
    /// would: its connections close, a request it holds is never answered,
    /// and it accepts no connection for the rest of the test.
    fn broker_down(&self) {
        self.api()
            .broker_down(BROKER_ID)
            .expect("take the mock broker down");
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

fn producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer")
}

/// Produces order `id` onto `partition` of `topic` and returns its offset.
async fn produce_to(producer: &FutureProducer, topic: &str, partition: i32, id: &str) -> i64 {
    let payload = serde_json::to_vec(&Order { id: id.into() }).expect("encode the record");
    let delivery = producer
        .send(
            FutureRecord::<(), Vec<u8>>::to(topic)
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

/// Produces one record onto each of the two partitions of `topic` every
/// 10 ms until `stop` fires, and returns how many it produced on each.
async fn produce_until(
    bootstrap: String,
    topic: &'static str,
    stop: CancellationToken,
) -> [i64; 2] {
    let producer = producer(&bootstrap);
    let mut produced = [0i64; 2];
    let mut round = 0u64;
    while !stop.is_cancelled() {
        for partition in [0, 1] {
            produce_to(
                &producer,
                topic,
                partition,
                &format!("p{partition}-{round}"),
            )
            .await;
            produced[usize::try_from(partition).expect("a partition index")] += 1;
        }
        round += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    produced
}

/// The group's committed position on `partition` of `topic`, read through a
/// raw consumer that never joins the group. `None` before the first
/// accepted commit.
async fn committed_position(
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
    wait_for_position_on(bootstrap, topic, group, 0, expected, timeout, what).await;
}

/// [`wait_for_position`] on `partition`.
async fn wait_for_position_on(
    bootstrap: &str,
    topic: &'static str,
    group: &'static str,
    partition: i32,
    expected: i64,
    timeout: Duration,
    what: &str,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let committed = committed_position(bootstrap, topic, group, partition).await;
        if committed == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: the committed position of partition {partition} is {committed:?}, not \
             {expected}, after {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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

/// One record as a batch handler was handed it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Handled {
    id: String,
    partition: i32,
    offset: i64,
}

/// Records every flush it is handed and answers it with `decide`, given the
/// flush's index.
#[derive(Clone)]
struct Recorder {
    flushes: Arc<Mutex<Vec<Vec<Handled>>>>,
    decide: fn(usize) -> Outcome,
}

impl Recorder {
    fn new(decide: fn(usize) -> Outcome) -> Self {
        Self {
            flushes: Arc::default(),
            decide,
        }
    }

    fn acking() -> Self {
        Self::new(|_| Outcome::Ack)
    }

    fn flushes(&self) -> Vec<Vec<Handled>> {
        self.flushes.lock().expect("handler mutex poisoned").clone()
    }

    /// Every record handed over, flush after flush.
    fn handled(&self) -> Vec<Handled> {
        self.flushes().into_iter().flatten().collect()
    }

    fn handled_on(&self, partition: i32) -> Vec<Handled> {
        self.handled()
            .into_iter()
            .filter(|h| h.partition == partition)
            .collect()
    }

    fn has(&self, id: &str) -> bool {
        self.handled().iter().any(|h| h.id == id)
    }
}

impl<T: Topic<Message = Order>> BatchMessageHandler<T> for Recorder {
    type Context = ();
    async fn handle_batch(&self, messages: Vec<(Order, MessageMetadata)>, _: &()) -> Outcome {
        let flush: Vec<Handled> = messages
            .into_iter()
            .map(|(order, meta)| Handled {
                id: order.id,
                partition: meta.partition.expect("Kafka fills the partition"),
                offset: meta.offset.expect("Kafka fills the offset"),
            })
            .collect();
        let index = {
            let mut flushes = self.flushes.lock().expect("handler mutex poisoned");
            flushes.push(flush);
            flushes.len() - 1
        };
        (self.decide)(index)
    }
}

/// A [`Recorder`] that holds each flush, once recorded, until `release` is
/// set, so its member commits nothing of what it was handed for as long as
/// a test needs.
#[derive(Clone)]
struct Holding {
    recorder: Recorder,
    release: watch::Receiver<bool>,
}

impl<T: Topic<Message = Order>> BatchMessageHandler<T> for Holding {
    type Context = ();
    async fn handle_batch(&self, messages: Vec<(Order, MessageMetadata)>, ctx: &()) -> Outcome {
        let outcome = BatchMessageHandler::<T>::handle_batch(&self.recorder, messages, ctx).await;
        let mut release = self.release.clone();
        // A sender gone releases too: the test is over.
        let _ = release.wait_for(|released| *released).await;
        outcome
    }
}

/// Starts a batch consumer of `T` with `handler`, on the options `options`
/// builds over the stop token's, and returns its run task and that token.
fn start_batch<T>(
    client: KafkaClient,
    handler: impl BatchMessageHandler<T, Context = ()>,
    options: impl FnOnce(BatchConsumerOptions) -> BatchConsumerOptions,
) -> (
    tokio::task::JoinHandle<Result<(), ShoveError>>,
    CancellationToken,
)
where
    T: Topic<Message = Order> + shove::NotSequenced + 'static,
{
    let shutdown = CancellationToken::new();
    let opts = options(BatchConsumerOptions::new().with_shutdown(shutdown.clone()));
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run_batch::<T, _>(handler, (), opts)
            .await
    });
    (run, shutdown)
}

/// Stops a consumer and expects it to end clean.
async fn stop(run: tokio::task::JoinHandle<Result<(), ShoveError>>, token: CancellationToken) {
    token.cancel();
    run.await
        .expect("the run task completes")
        .expect("the consumer ends clean");
}

type Member = (
    tokio::task::JoinHandle<Result<(), ShoveError>>,
    CancellationToken,
);

/// Stops `first`, then `second` once the group has handed it what `first`
/// held. A member's leave rebalances the group, and a final commit made
/// while it rebalances is refused and tried again inside the shutdown
/// deadline; stopping in turn keeps the tests that end with it from waiting
/// on that retry.
async fn stop_in_turn(first: Member, second: Member) {
    let assigns = per_record_probe::assigns_queued();
    stop(first.0, first.1).await;
    wait_until(
        || per_record_probe::assigns_queued() > assigns,
        DELIVERY_TIMEOUT,
        "the remaining member taking the partitions of the one that left",
    )
    .await;
    stop(second.0, second.1).await;
}

/// Asserts that `handled`, one member's records in the order its flushes
/// were handed them, holds each partition's records in strictly rising
/// offset order: nothing of a partition arrives twice or ahead of a lower
/// offset.
fn assert_in_offset_order(member: &str, handled: &[Handled]) {
    for partition in [0, 1] {
        let offsets: Vec<i64> = handled
            .iter()
            .filter(|h| h.partition == partition)
            .map(|h| h.offset)
            .collect();
        if let Some(at) = offsets.windows(2).position(|pair| pair[0] >= pair[1]) {
            let shown = &offsets[at.saturating_sub(2)..(at + 3).min(offsets.len())];
            panic!(
                "{member} was handed partition {partition} out of offset order or twice, \
                 around {shown:?} of {} records",
                offsets.len()
            );
        }
    }
}

/// `shove_messages_discarded_total` for `topic`, summed over its reasons,
/// from one draining snapshot of the debugging recorder.
#[cfg(feature = "metrics")]
fn discarded_total(snapshotter: &Snapshotter, topic: &str) -> u64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "shove_messages_discarded_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "topic" && l.value() == topic)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(n) => n,
            other => panic!("the discard counter is not a counter: {other:?}"),
        })
        .sum()
}

#[cfg(feature = "metrics")]
fn install_recorder() -> Snapshotter {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install the debugging recorder");
    snapshotter
}

/// A member whose batch spans both partitions of a topic loses one of them
/// to a second member while records keep arriving on both. The record that
/// comes back with the revoke belongs to the partition the member keeps, and
/// the abandoned batch rewinds that partition, so the broker delivers that
/// record again behind the rewound ones: the copy in hand must not be
/// buffered. Every record is then handed to one member exactly once, and
/// each member gets each partition in offset order.
#[tokio::test]
async fn rebalance_mid_batch_drops_the_record_in_hand() {
    const TOPIC: &str = "kafka-batch-mock-in-hand";
    shove::define_topic!(InHandTopic, Order, TopologyBuilder::new(TOPIC).build());

    let mock = Mock::start();
    mock.create_topic(TOPIC, 2);
    let bootstrap = mock.bootstrap();

    let producing = CancellationToken::new();
    let production = tokio::spawn(produce_until(bootstrap.clone(), TOPIC, producing.clone()));

    // A's batch flushes only at its stop, so it spans both partitions when
    // the rebalance lands.
    let a = Recorder::acking();
    let (run_a, token_a) = start_batch::<InHandTopic>(connect(&bootstrap).await, a.clone(), |o| {
        o.with_max_batch_size(1_000_000)
            .with_max_batch_age(Duration::from_secs(600))
    });
    wait_until(
        || per_record_probe::assigns_queued() >= 1,
        DELIVERY_TIMEOUT,
        "A joining its group",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let b = Recorder::acking();
    let (run_b, token_b) = start_batch::<InHandTopic>(connect(&bootstrap).await, b.clone(), |o| {
        o.with_max_batch_age(Duration::from_millis(200))
    });
    wait_until(
        || !b.handled().is_empty(),
        DELIVERY_TIMEOUT,
        "the rebalance moving a partition to B",
    )
    .await;
    let moved = b.handled()[0].partition;
    tokio::time::sleep(Duration::from_secs(1)).await;
    producing.cancel();
    let produced = production.await.expect("the production task completes");
    let on_moved = produced[usize::try_from(moved).expect("a partition index")];
    wait_until(
        || i64::try_from(b.handled_on(moved).len()).expect("a count") >= on_moved,
        DELIVERY_TIMEOUT,
        "B handling every record of the moved partition",
    )
    .await;

    // A's stop flushes everything it kept.
    stop_in_turn((run_a, token_a), (run_b, token_b)).await;

    let (a_handled, b_handled) = (a.handled(), b.handled());
    assert_in_offset_order("A", &a_handled);
    assert_in_offset_order("B", &b_handled);
    let mut seen = HashSet::new();
    for record in a_handled.iter().chain(&b_handled) {
        assert!(
            seen.insert((record.partition, record.offset)),
            "{record:?} was handed to a handler twice"
        );
    }
    for partition in [0, 1] {
        let count = produced[usize::try_from(partition).expect("a partition index")];
        for offset in 0..count {
            assert!(
                seen.contains(&(partition, offset)),
                "partition {partition} offset {offset} never reached a handler"
            );
        }
    }
}

/// Produces `per_partition` records onto each of the two partitions of
/// `topic`, and returns each record's id with its partition.
async fn produce_on_both(
    producer: &FutureProducer,
    topic: &str,
    per_partition: usize,
) -> Vec<(String, i32)> {
    let mut produced = Vec::new();
    for round in 0..per_partition {
        for partition in [0, 1] {
            let id = format!("p{partition}-{round}");
            produce_to(producer, topic, partition, &id).await;
            produced.push((id, partition));
        }
    }
    produced
}

/// The ids of `records` on `partition`.
fn ids_on(records: &[(String, i32)], partition: i32) -> Vec<String> {
    records
        .iter()
        .filter(|(_, p)| *p == partition)
        .map(|(id, _)| id.clone())
        .collect()
}

/// How often the batch loop wakes for housekeeping in the tests that leave a
/// rebalance to the arm they are about: longer than any of them runs.
const NO_HOUSEKEEPING: Duration = Duration::from_secs(600);

/// A member's batch spans both partitions of an idle topic when a second
/// member's join revokes one of them. The revoke reaches the member in a
/// poll that hands it no record, and the member's housekeeping wake is
/// pushed past the test, so the batch's age deadline is the first arm to run
/// with the revoke still queued. The flush drains it first: the revoked
/// partition's records go to their new owner, which is handed them from the
/// committed position, the kept ones are handed to the member again, and
/// each record reaches one member once.
#[tokio::test]
async fn deadline_flush_after_revoke_skips_revoked_partition() {
    const TOPIC: &str = "kafka-batch-mock-deadline-revoke";
    const PER_PARTITION: usize = 5;
    /// Long enough for the second member's join to revoke a partition first.
    const AGE: Duration = Duration::from_secs(12);
    shove::define_topic!(DeadlineTopic, Order, TopologyBuilder::new(TOPIC).build());

    let mock = Mock::start();
    mock.create_topic(TOPIC, 2);
    let bootstrap = mock.bootstrap();
    let records = produce_on_both(&producer(&bootstrap), TOPIC, PER_PARTITION).await;

    let a = Recorder::acking();
    let (run_a, token_a) =
        start_batch::<DeadlineTopic>(connect(&bootstrap).await, a.clone(), |o| {
            o.with_max_batch_size(1000)
                .with_max_batch_age(AGE)
                .with_housekeeping_interval_for_test(NO_HOUSEKEEPING)
        });
    wait_until(
        || per_record_probe::assigns_queued() >= 1,
        DELIVERY_TIMEOUT,
        "A joining its group",
    )
    .await;
    // A fetches every record of both partitions into its batch.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let b = Recorder::acking();
    let (run_b, token_b) =
        start_batch::<DeadlineTopic>(connect(&bootstrap).await, b.clone(), |o| {
            o.with_max_batch_age(Duration::from_millis(200))
        });
    wait_until(
        || !b.handled().is_empty(),
        DELIVERY_TIMEOUT,
        "B being handed the moved partition",
    )
    .await;
    let moved = b.handled()[0].partition;
    let (moved_ids, kept_ids) = (ids_on(&records, moved), ids_on(&records, 1 - moved));
    wait_until(
        || moved_ids.iter().all(|id| b.has(id)),
        DELIVERY_TIMEOUT,
        "B handling the moved partition's records",
    )
    .await;
    wait_until(
        || kept_ids.iter().all(|id| a.has(id)),
        AGE.saturating_mul(2) + DELIVERY_TIMEOUT,
        "A handling the kept partition's records",
    )
    .await;
    stop_in_turn((run_a, token_a), (run_b, token_b)).await;

    let a_on_moved = a.handled_on(moved);
    assert!(
        a_on_moved.is_empty(),
        "A's deadline flush handed its handler records of the partition revoked before it: \
         {a_on_moved:?}"
    );
    let handled: Vec<Handled> = a.handled().into_iter().chain(b.handled()).collect();
    for (id, _) in &records {
        let times = handled.iter().filter(|h| &h.id == id).count();
        assert_eq!(times, 1, "{id} reached a handler {times} times");
    }
}

/// The same revoke, drained by the stop: the member's batch never ages out,
/// and the stop lands once the revoke is queued, which the second member
/// being handed a record proves, because the group hands it the moved
/// partition only once the first member has let it go. The stop's flush
/// drains the revoke first and abandons the batch, so the stopping member
/// hands its handler nothing; the second member takes over both partitions
/// and is handed every record once.
#[tokio::test]
async fn shutdown_flush_after_revoke_skips_revoked_partition() {
    const TOPIC: &str = "kafka-batch-mock-shutdown-revoke";
    const PER_PARTITION: usize = 5;
    shove::define_topic!(ShutdownTopic, Order, TopologyBuilder::new(TOPIC).build());

    let mock = Mock::start();
    mock.create_topic(TOPIC, 2);
    let bootstrap = mock.bootstrap();
    let records = produce_on_both(&producer(&bootstrap), TOPIC, PER_PARTITION).await;

    let a = Recorder::acking();
    let (run_a, token_a) =
        start_batch::<ShutdownTopic>(connect(&bootstrap).await, a.clone(), |o| {
            o.with_max_batch_size(1000)
                .with_max_batch_age(NO_HOUSEKEEPING)
                .with_housekeeping_interval_for_test(NO_HOUSEKEEPING)
        });
    wait_until(
        || per_record_probe::assigns_queued() >= 1,
        DELIVERY_TIMEOUT,
        "A joining its group",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let b = Recorder::acking();
    let (run_b, token_b) =
        start_batch::<ShutdownTopic>(connect(&bootstrap).await, b.clone(), |o| {
            o.with_max_batch_age(Duration::from_millis(200))
        });
    wait_until(
        || !b.handled().is_empty(),
        DELIVERY_TIMEOUT,
        "B being handed the moved partition",
    )
    .await;
    stop(run_a, token_a).await;
    wait_until(
        || records.iter().all(|(id, _)| b.has(id)),
        DELIVERY_TIMEOUT,
        "B handling every record once A has left",
    )
    .await;
    stop(run_b, token_b).await;

    assert!(
        a.handled().is_empty(),
        "A's stop handed its handler the batch the revoke abandoned: {:?}",
        a.handled()
    );
    let handled = b.handled();
    for (id, _) in &records {
        let times = handled.iter().filter(|h| &h.id == id).count();
        assert_eq!(times, 1, "{id} reached B {times} times");
    }
}

/// A batch's offset commit is refused with a rebalance in progress, three
/// times over. The batch was acked, so the commit is re-offered until the
/// coordinator takes it, and the member stays in its group: no reconnect,
/// nothing handled twice.
#[tokio::test]
async fn batch_commit_rejected_during_rebalance_keeps_member() {
    const TOPIC: &str = "kafka-batch-mock-commit-rejected";
    const GROUP: &str = "kafka-batch-mock-commit-rejected-consumer";
    shove::define_topic!(CommitTopic, Order, TopologyBuilder::new(TOPIC).build());

    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    let bootstrap = mock.bootstrap();
    let producer = producer(&bootstrap);
    for id in ["A", "B", "C"] {
        produce_to(&producer, TOPIC, 0, id).await;
    }
    mock.refuse(
        RDKafkaApiKey::OffsetCommit,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
        3,
    );

    let handler = Recorder::acking();
    let (run, token) =
        start_batch::<CommitTopic>(connect(&bootstrap).await, handler.clone(), |o| {
            o.with_max_batch_size(3)
                .with_max_batch_age(Duration::from_millis(500))
        });
    wait_until(
        || handler.flushes().len() == 1,
        DELIVERY_TIMEOUT,
        "the batch reaching the handler",
    )
    .await;
    let clients = client_id_probe::created().len();
    wait_for_position(
        &bootstrap,
        TOPIC,
        GROUP,
        3,
        DELIVERY_TIMEOUT,
        "the refused commit re-offered until it is accepted",
    )
    .await;
    stop(run, token).await;

    assert_eq!(
        client_id_probe::created().len(),
        clients,
        "a refused commit never reconnects the member"
    );
    assert_eq!(
        handler
            .handled()
            .iter()
            .map(|h| h.id.as_str())
            .collect::<Vec<_>>(),
        ["A", "B", "C"],
        "the acked batch is handled once"
    );
}

/// A rejected batch's dead letters land, and the synchronous commit they
/// ride is refused once with a rebalance in progress. The batch is settled
/// by then, so the refusal costs a re-offer: the position is offered again,
/// as an acked batch's is, and committed once the coordinator takes it. The
/// batch is handed to the handler once, each record is dead-lettered once,
/// and the member stays in its group.
#[tokio::test]
async fn a_dead_lettered_batch_whose_commit_is_refused_is_re_offered() {
    const TOPIC: &str = "kafka-batch-mock-reject-commit-refused";
    const DLQ: &str = "kafka-batch-mock-reject-commit-refused-dlq";
    const GROUP: &str = "kafka-batch-mock-reject-commit-refused-consumer";
    shove::define_topic!(
        RejectTopic,
        Order,
        TopologyBuilder::new(TOPIC).dlq().build()
    );

    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    mock.create_topic(DLQ, 1);
    let bootstrap = mock.bootstrap();
    let producer = producer(&bootstrap);
    for id in ["A", "B"] {
        produce_to(&producer, TOPIC, 0, id).await;
    }
    mock.refuse(
        RDKafkaApiKey::OffsetCommit,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
        1,
    );

    let handler = Recorder::new(|_| Outcome::Reject);
    let (run, token) =
        start_batch::<RejectTopic>(connect(&bootstrap).await, handler.clone(), |o| {
            o.with_max_batch_size(2)
                .with_max_batch_age(Duration::from_millis(500))
        });
    wait_for_position(
        &bootstrap,
        TOPIC,
        GROUP,
        2,
        DELIVERY_TIMEOUT,
        "the refused commit re-offered until it is accepted",
    )
    .await;
    stop(run, token).await;

    assert_eq!(
        client_id_probe::created().len(),
        1,
        "the refused commit did not reconnect the member"
    );
    assert_eq!(
        handler.flushes().len(),
        1,
        "the dead-lettered batch is not handed to the handler again"
    );
    assert_eq!(
        high_watermark(&bootstrap, DLQ, 0).await,
        2,
        "each record is dead-lettered once"
    );
}

/// The same refusal on a rejected batch with no DLQ to route it to. The
/// discards ride the commit that retires the batch: the refusal hands them
/// to the re-offer, which counts each once when it lands, and the batch is
/// handed to the handler once.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn a_discarded_batch_whose_commit_is_refused_counts_each_discard_once() {
    const TOPIC: &str = "kafka-batch-mock-discard-commit-refused";
    const GROUP: &str = "kafka-batch-mock-discard-commit-refused-consumer";
    shove::define_topic!(DiscardTopic, Order, TopologyBuilder::new(TOPIC).build());

    let snapshotter = install_recorder();
    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    let bootstrap = mock.bootstrap();
    let producer = producer(&bootstrap);
    for id in ["A", "B"] {
        produce_to(&producer, TOPIC, 0, id).await;
    }
    mock.refuse(
        RDKafkaApiKey::OffsetCommit,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
        1,
    );

    let handler = Recorder::new(|_| Outcome::Reject);
    let (run, token) =
        start_batch::<DiscardTopic>(connect(&bootstrap).await, handler.clone(), |o| {
            o.with_max_batch_size(2)
                .with_max_batch_age(Duration::from_millis(500))
        });
    wait_for_position(
        &bootstrap,
        TOPIC,
        GROUP,
        2,
        DELIVERY_TIMEOUT,
        "the refused commit re-offered until it is accepted",
    )
    .await;
    stop(run, token).await;

    assert_eq!(
        handler.flushes().len(),
        1,
        "the discarded batch is not handed to the handler again"
    );
    assert_eq!(
        discarded_total(&snapshotter, TOPIC),
        2,
        "each discarded record is counted once, by the re-offer that retired it"
    );
}

/// A rejected batch's dead letter is refused by the broker three sends
/// over, an error nothing tells apart from an outage. Nothing is committed
/// past the record: its partition is sought back, the batch is handed to the
/// handler again and dead-lettered once the DLQ takes writes, and no discard
/// is counted for it. The member never reconnects.
#[tokio::test]
async fn batch_dlq_publish_failure_redelivers() {
    const TOPIC: &str = "kafka-batch-mock-dlq-failure";
    const DLQ: &str = "kafka-batch-mock-dlq-failure-dlq";
    const GROUP: &str = "kafka-batch-mock-dlq-failure-consumer";
    shove::define_topic!(
        DlqFailureTopic,
        Order,
        TopologyBuilder::new(TOPIC).dlq().build()
    );

    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    mock.create_topic(DLQ, 1);
    let bootstrap = mock.bootstrap();
    produce_to(&producer(&bootstrap), TOPIC, 0, "A").await;
    mock.refuse(
        RDKafkaApiKey::Produce,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN,
        SENDS_PER_PUBLISH,
    );

    let handler = Recorder::new(|_| Outcome::Reject);
    let (run, token) =
        start_batch::<DlqFailureTopic>(connect(&bootstrap).await, handler.clone(), |o| {
            o.with_max_batch_size(1)
                .with_max_batch_age(Duration::from_millis(500))
        });
    wait_until(
        || handler.flushes().len() >= 2,
        DELIVERY_TIMEOUT,
        "A handed to the handler again after its dead letter failed",
    )
    .await;
    wait_for_position(
        &bootstrap,
        TOPIC,
        GROUP,
        1,
        DELIVERY_TIMEOUT,
        "A dead-lettered and committed",
    )
    .await;
    stop(run, token).await;

    assert_eq!(
        handler
            .handled()
            .iter()
            .map(|h| h.id.as_str())
            .collect::<Vec<_>>(),
        ["A", "A"],
        "A is redelivered once its dead letter failed"
    );
    assert_eq!(
        high_watermark(&bootstrap, DLQ, 0).await,
        1,
        "A is dead-lettered once"
    );
    assert_eq!(
        client_id_probe::created().len(),
        1,
        "the failed dead letter is redelivered without a reconnect"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        discarded_total(&snapshotter, TOPIC),
        0,
        "a record kept through a failed dead letter is never counted as discarded"
    );
}

/// Fifty records are rejected in one batch while the DLQ answers every
/// produce request four seconds late, with the member's poll interval at its
/// 10 s floor. The dead letters run concurrently, a bounded number at a
/// time, and the member keeps polling while they run, so a flush that takes
/// longer than the poll interval does not evict it: the batch is
/// dead-lettered once and committed with no rebalance and no reconnect. The
/// test checks its own premise, that the flush did outlast the poll
/// interval, so a change in the concurrency or in the mock's delay cannot
/// leave it passing without the polling it is about.
#[tokio::test]
async fn batch_reject_with_slow_dlq_stays_in_group() {
    const TOPIC: &str = "kafka-batch-mock-slow-dlq";
    const DLQ: &str = "kafka-batch-mock-slow-dlq-dlq";
    const GROUP: &str = "kafka-batch-mock-slow-dlq-consumer";
    const RECORDS: usize = 50;
    /// Under the producer's 5 s message timeout, so a held answer is late
    /// and never a failure.
    const DLQ_LATENCY: Duration = Duration::from_secs(4);
    shove::define_topic!(
        SlowDlqTopic,
        Order,
        TopologyBuilder::new(TOPIC).dlq().build()
    );

    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    mock.create_topic(DLQ, 1);
    let bootstrap = mock.bootstrap();
    let producer = producer(&bootstrap);
    for index in 0..RECORDS {
        produce_to(&producer, TOPIC, 0, &format!("r{index}")).await;
    }
    // More held answers than the dead letters can ever send requests.
    mock.answer_late(RDKafkaApiKey::Produce, DLQ_LATENCY, RECORDS);

    let handler = Recorder::new(|flush| {
        if flush == 0 {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    });
    let (run, token) =
        start_batch::<SlowDlqTopic>(connect(&bootstrap).await, handler.clone(), |o| {
            o.with_max_batch_size(RECORDS)
                .with_max_batch_age(Duration::from_secs(5))
                .with_max_poll_interval_for_test(SHORT_POLL_INTERVAL)
        });
    wait_until(
        || handler.flushes().len() == 1,
        DELIVERY_TIMEOUT,
        "the batch reaching the handler",
    )
    .await;
    let flushed_at = Instant::now();
    let clients = client_id_probe::created().len();
    let assigns = per_record_probe::assigns_queued();
    wait_for_position(
        &bootstrap,
        TOPIC,
        GROUP,
        i64::try_from(RECORDS).expect("a count"),
        DELIVERY_TIMEOUT,
        "the rejected batch dead-lettered and committed",
    )
    .await;
    let flush_took = flushed_at.elapsed();
    stop(run, token).await;

    assert!(
        flush_took > SHORT_POLL_INTERVAL,
        "the flush outlasts the poll interval, the premise of the test: it took {flush_took:?}"
    );
    assert_eq!(
        high_watermark(&bootstrap, DLQ, 0).await,
        i64::try_from(RECORDS).expect("a count"),
        "every rejected record is dead-lettered once"
    );
    assert_eq!(
        client_id_probe::created().len(),
        clients,
        "the member never reconnected"
    );
    assert_eq!(
        per_record_probe::assigns_queued(),
        assigns,
        "no rebalance: the member stayed in its group through the slow dead letters"
    );
    assert_eq!(handler.flushes().len(), 1, "the batch is handled once");
}

/// A member rejects a batch spanning both partitions of a topic while the
/// DLQ holds every produce answer for four seconds, so the dead letters take
/// eight rounds and the member polls between them. A second member joins
/// during that wait, and the rebalance revokes one of the first member's
/// partitions: the mock coordinator runs each of the two rounds a
/// cooperative rebalance takes a second short of the session timeout, so
/// the wait has to outlast about twenty seconds. The wait ends, the flush reads the revoke, and the batch is
/// abandoned: nothing of the revoked partition is committed, the second
/// member is handed it from the committed offset, the first member is
/// handed the partition it kept again from its start, and every record ends
/// up committed with no discard counted. The second member holds its
/// flushes until the first member's batch is over, so a commit of the
/// revoked partition by the first member would show.
#[tokio::test]
async fn a_rebalance_during_the_dead_letter_wait_commits_nothing_of_the_revoked_partition() {
    const TOPIC: &str = "kafka-batch-mock-dlq-wait-rebalance";
    const DLQ: &str = "kafka-batch-mock-dlq-wait-rebalance-dlq";
    const GROUP: &str = "kafka-batch-mock-dlq-wait-rebalance-consumer";
    const PER_PARTITION: usize = 64;
    /// Under the producer's 5 s message timeout, so a held answer is late
    /// and never a failure.
    const DLQ_LATENCY: Duration = Duration::from_secs(4);
    shove::define_topic!(WaitTopic, Order, TopologyBuilder::new(TOPIC).dlq().build());

    #[cfg(feature = "metrics")]
    let snapshotter = install_recorder();
    let mock = Mock::start();
    mock.create_topic(TOPIC, 2);
    mock.create_topic(DLQ, 1);
    let bootstrap = mock.bootstrap();
    let records = produce_on_both(&producer(&bootstrap), TOPIC, PER_PARTITION).await;
    let letters = i64::try_from(records.len()).expect("a count");
    let per_partition: Vec<i64> = (0..i64::try_from(PER_PARTITION).expect("a count")).collect();
    // More held answers than the dead letters can ever send requests.
    mock.answer_late(RDKafkaApiKey::Produce, DLQ_LATENCY, records.len());

    // A rejects the batch it holds when B joins, and acks what it is
    // handed after.
    let a = Recorder::new(|flush| {
        if flush == 0 {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    });
    let (run_a, token_a) = start_batch::<WaitTopic>(connect(&bootstrap).await, a.clone(), |o| {
        o.with_max_batch_size(records.len())
            .with_max_batch_age(Duration::from_secs(5))
    });
    wait_until(
        || a.flushes().len() == 1,
        DELIVERY_TIMEOUT,
        "A's batch reaching its handler",
    )
    .await;
    assert_eq!(
        a.flushes()[0].len(),
        records.len(),
        "A's batch spans every record of both partitions"
    );

    let (release, released) = watch::channel(false);
    let b = Recorder::acking();
    let holding = Holding {
        recorder: b.clone(),
        release: released,
    };
    let (run_b, token_b) = start_batch::<WaitTopic>(connect(&bootstrap).await, holding, |o| {
        o.with_max_batch_age(Duration::from_millis(200))
            .with_handler_timeout(DELIVERY_TIMEOUT)
    });
    wait_until(
        || !b.handled().is_empty(),
        DELIVERY_TIMEOUT,
        "the rebalance handing B a partition",
    )
    .await;
    let landed = high_watermark(&bootstrap, DLQ, 0).await;
    assert!(
        landed < letters,
        "the rebalance reached A while its dead letters were still landing, the premise of the \
         test: {landed} of {letters} had landed"
    );
    let moved = b.handled()[0].partition;
    let kept = 1 - moved;

    // The flush that waited for the dead letters ends once they have landed.
    // Abandoned to the revoke, it has A handed the partition it kept again;
    // committed past the revoke, it moves the revoked partition's position.
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let committed_moved = loop {
        let committed = committed_position(&bootstrap, TOPIC, GROUP, moved).await;
        if committed.is_some() || a.flushes().len() >= 2 {
            break committed;
        }
        assert!(
            Instant::now() < deadline,
            "A's flush ending did not happen within {DELIVERY_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        committed_moved, None,
        "A committed nothing of the partition the rebalance revoked during its wait"
    );

    let _ = release.send(true);
    for partition in [moved, kept] {
        wait_for_position_on(
            &bootstrap,
            TOPIC,
            GROUP,
            partition,
            i64::try_from(PER_PARTITION).expect("a count"),
            DELIVERY_TIMEOUT,
            "every record handled after the rebalance committed",
        )
        .await;
    }
    stop_in_turn((run_a, token_a), (run_b, token_b)).await;

    assert_eq!(
        b.handled_on(moved)
            .iter()
            .map(|h| h.offset)
            .collect::<Vec<_>>(),
        per_partition,
        "B is handed the revoked partition from the committed offset, each record once"
    );
    let a_after: Vec<Handled> = a.flushes()[1..].concat();
    assert!(
        a_after.iter().all(|h| h.partition == kept),
        "A is handed nothing of the revoked partition after the rebalance: {a_after:?}"
    );
    assert_eq!(
        a_after.iter().map(|h| h.offset).collect::<Vec<_>>(),
        per_partition,
        "A is handed the partition it kept again from its start, each record once"
    );
    assert_eq!(
        high_watermark(&bootstrap, DLQ, 0).await,
        letters,
        "the rejected batch is dead-lettered once"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        discarded_total(&snapshotter, TOPIC),
        0,
        "a batch abandoned to a rebalance counts no discard"
    );
}

/// A rejected batch's dead letters land, and the commit they ride reaches
/// the broker and has no answer: the broker goes down under it, as a
/// coordinator that drops off the network would. A stop that lands then ends
/// inside the shutdown deadline. The flush hands its commit to the stop as
/// the commit in flight, which the stop waits for for its share of the
/// deadline, and the run ends with `ShoveError::Commit` of the `Deadline`
/// kind carrying that share and the position the commit carried.
#[tokio::test]
async fn a_stop_during_a_dead_letter_commit_ends_inside_the_shutdown_deadline() {
    const TOPIC: &str = "kafka-batch-mock-stop-during-commit";
    const DLQ: &str = "kafka-batch-mock-stop-during-commit-dlq";
    shove::define_topic!(StopTopic, Order, TopologyBuilder::new(TOPIC).dlq().build());

    let share = pending_commit_budget_for_test();
    let deadline = shutdown_commit_deadline_for_test();
    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    mock.create_topic(DLQ, 1);
    let bootstrap = mock.bootstrap();
    let producer = producer(&bootstrap);
    for id in ["A", "B"] {
        produce_to(&producer, TOPIC, 0, id).await;
    }
    mock.track_requests();
    mock.answer_late(RDKafkaApiKey::OffsetCommit, share, 1);

    let handler = Recorder::new(|_| Outcome::Reject);
    let (run, token) = start_batch::<StopTopic>(connect(&bootstrap).await, handler.clone(), |o| {
        o.with_max_batch_size(2)
            .with_max_batch_age(Duration::from_millis(500))
    });
    wait_until(
        || mock.requests_of(RDKafkaApiKey::OffsetCommit) >= 1,
        DELIVERY_TIMEOUT,
        "the dead-lettered batch's commit reaching the broker",
    )
    .await;
    mock.broker_down();
    let stopped_at = Instant::now();
    token.cancel();
    let result = tokio::time::timeout(deadline + STOP_MARGIN, run)
        .await
        .expect("the stop ends inside the shutdown deadline")
        .expect("the run task completes");
    let took = stopped_at.elapsed();

    let Err(ShoveError::Commit(failed)) = &result else {
        panic!("the stop reports the commit it could not finish: {result:?}");
    };
    assert_eq!(failed.topic, TOPIC);
    assert_eq!(
        failed.offsets,
        [(0, 2)],
        "the position the dead-lettered batch's commit carried"
    );
    assert_eq!(
        failed.kind,
        CommitFailure::Deadline(share),
        "the Deadline kind carrying the share a commit in flight is given"
    );
    assert!(
        took < deadline,
        "the stop ends inside the shutdown deadline, took {took:?}"
    );
    assert_eq!(handler.flushes().len(), 1, "the batch is handed over once");
}

/// Produces `ids` onto partition 0 of `topic`, then starts a batch consumer
/// of `T` whose handler answers every flush with `outcome`, flushing two
/// records at a time with no age deadline a test reaches, so a third record
/// waits in the batch for the stop. Returns once the first two records'
/// position is committed: `outcome` must commit them.
async fn a_member_holding_a_record_for_its_stop<T>(
    mock: &Mock,
    topic: &'static str,
    group: &'static str,
    outcome: fn(usize) -> Outcome,
) -> (Recorder, Member)
where
    T: Topic<Message = Order> + shove::NotSequenced + 'static,
{
    let bootstrap = mock.bootstrap();
    let producer = producer(&bootstrap);
    for id in ["A", "B", "C"] {
        produce_to(&producer, topic, 0, id).await;
    }
    let handler = Recorder::new(outcome);
    let member = start_batch::<T>(connect(&bootstrap).await, handler.clone(), |o| {
        o.with_max_batch_size(2)
            .with_max_batch_age(Duration::from_secs(600))
    });
    wait_for_position(
        &bootstrap,
        topic,
        group,
        2,
        DELIVERY_TIMEOUT,
        "the first batch committed",
    )
    .await;
    (handler, member)
}

/// The stop's final commit is refused once with a rebalance in progress, as
/// a commit made while another member's leave rebalances the group is. The
/// stop polls with its assignment paused and commits again inside the
/// shutdown deadline, and the second try lands: the member ends clean, with
/// the position of the record its stop flushed committed.
#[tokio::test]
async fn a_final_commit_refused_while_the_group_rebalances_is_tried_again() {
    const TOPIC: &str = "kafka-batch-mock-final-commit-rebalancing";
    const GROUP: &str = "kafka-batch-mock-final-commit-rebalancing-consumer";
    shove::define_topic!(RetryTopic, Order, TopologyBuilder::new(TOPIC).build());

    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    let (handler, (run, token)) =
        a_member_holding_a_record_for_its_stop::<RetryTopic>(&mock, TOPIC, GROUP, |_| Outcome::Ack)
            .await;
    mock.refuse(
        RDKafkaApiKey::OffsetCommit,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
        1,
    );
    stop(run, token).await;

    assert_eq!(
        committed_position(&mock.bootstrap(), TOPIC, GROUP, 0).await,
        Some(3),
        "the second try carries the position of the record the stop flushed"
    );
    assert_eq!(
        handler.flushes().len(),
        2,
        "the stop flushes the record it held"
    );
}

/// A final commit the broker refuses outright ends the batch consumer with
/// `ShoveError::Commit` of the `Rejected` kind, naming the position it
/// carried, as it ends every other consumer.
#[tokio::test]
async fn a_refused_final_commit_ends_the_batch_consumer_with_a_commit_error() {
    const TOPIC: &str = "kafka-batch-mock-final-commit-refused";
    const GROUP: &str = "kafka-batch-mock-final-commit-refused-consumer";
    shove::define_topic!(RefusedTopic, Order, TopologyBuilder::new(TOPIC).build());

    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    let (_handler, (run, token)) =
        a_member_holding_a_record_for_its_stop::<RefusedTopic>(&mock, TOPIC, GROUP, |_| {
            Outcome::Ack
        })
        .await;
    mock.refuse(
        RDKafkaApiKey::OffsetCommit,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
        8,
    );
    token.cancel();
    let result = run.await.expect("the run task completes");

    let Err(ShoveError::Commit(failed)) = &result else {
        panic!("a refused final commit ends the run with ShoveError::Commit: {result:?}");
    };
    assert_eq!(failed.topic, TOPIC);
    assert_eq!(
        failed.offsets,
        [(0, 3)],
        "the position the final commit carried"
    );
    assert!(
        matches!(failed.kind, CommitFailure::Rejected(_)),
        "a refused commit is reported as Rejected: {:?}",
        failed.kind
    );
    assert_eq!(
        committed_position(&mock.bootstrap(), TOPIC, GROUP, 0).await,
        Some(2),
        "the earlier accepted commit stays the group's position"
    );
}

/// A stop flushes a batch the handler rejects, and the broker refuses every
/// commit. The flush's commit is the stop's commit in flight, the final
/// commit offers its position again, and the stop ends as a refused final
/// commit ends it: with `ShoveError::Commit` naming the position the
/// dead-lettered batch reached, its dead letters published once.
#[tokio::test]
async fn a_stop_whose_dead_letter_commit_is_refused_ends_with_a_commit_error() {
    const TOPIC: &str = "kafka-batch-mock-stop-flush-refused";
    const DLQ: &str = "kafka-batch-mock-stop-flush-refused-dlq";
    const GROUP: &str = "kafka-batch-mock-stop-flush-refused-consumer";
    shove::define_topic!(
        StopFlushTopic,
        Order,
        TopologyBuilder::new(TOPIC).dlq().build()
    );

    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    mock.create_topic(DLQ, 1);
    let (handler, (run, token)) =
        a_member_holding_a_record_for_its_stop::<StopFlushTopic>(&mock, TOPIC, GROUP, |flush| {
            if flush == 0 {
                Outcome::Ack
            } else {
                Outcome::Reject
            }
        })
        .await;
    mock.refuse(
        RDKafkaApiKey::OffsetCommit,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
        8,
    );
    token.cancel();
    let result = run.await.expect("the run task completes");

    let Err(ShoveError::Commit(failed)) = &result else {
        panic!("a stop whose commits are refused ends with ShoveError::Commit: {result:?}");
    };
    assert_eq!(
        failed.offsets,
        [(0, 3)],
        "the position the dead-lettered batch reached"
    );
    assert!(
        matches!(failed.kind, CommitFailure::Rejected(_)),
        "a refused commit is reported as Rejected: {:?}",
        failed.kind
    );
    assert_eq!(
        handler.flushes().len(),
        2,
        "the stop flushes the record it held once"
    );
    assert_eq!(
        high_watermark(&mock.bootstrap(), DLQ, 0).await,
        1,
        "the stop's rejected record is dead-lettered once"
    );
}

/// Rejected batches of two on a topic with eighteen more records already
/// fetched behind them, so a `recv()` made while a batch's commit awaits its
/// answer is handed one at once. The commit is answered in milliseconds,
/// inside the grace the wait gives it before it polls, so no record is put
/// back: a put-back pauses the assignment, which drops every partition's
/// prefetched records for a refetch on resume.
#[tokio::test]
async fn a_dead_letter_commit_answered_at_once_puts_no_record_back() {
    const TOPIC: &str = "kafka-batch-mock-commit-wait-grace";
    const DLQ: &str = "kafka-batch-mock-commit-wait-grace-dlq";
    const GROUP: &str = "kafka-batch-mock-commit-wait-grace-consumer";
    const RECORDS: usize = 20;
    shove::define_topic!(GraceTopic, Order, TopologyBuilder::new(TOPIC).dlq().build());

    let mock = Mock::start();
    mock.create_topic(TOPIC, 1);
    mock.create_topic(DLQ, 1);
    let bootstrap = mock.bootstrap();
    let producer = producer(&bootstrap);
    for index in 0..RECORDS {
        produce_to(&producer, TOPIC, 0, &format!("r{index}")).await;
    }

    let handler = Recorder::new(|_| Outcome::Reject);
    let (run, token) = start_batch::<GraceTopic>(connect(&bootstrap).await, handler.clone(), |o| {
        o.with_max_batch_size(2)
            .with_max_batch_age(Duration::from_secs(5))
    });
    wait_for_position(
        &bootstrap,
        TOPIC,
        GROUP,
        i64::try_from(RECORDS).expect("a count"),
        DELIVERY_TIMEOUT,
        "every rejected batch dead-lettered and committed",
    )
    .await;
    stop(run, token).await;

    assert_eq!(
        put_back_probe::commit_wait(),
        0,
        "a commit answered inside the grace puts no record back"
    );
    assert_eq!(
        high_watermark(&bootstrap, DLQ, 0).await,
        i64::try_from(RECORDS).expect("a count"),
        "every record is dead-lettered once"
    );
}
