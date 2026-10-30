//! A sequenced Kafka consumer retries a record where it sits, against
//! librdkafka's mock cluster (`rdkafka::mocking`), which serves the group
//! protocol, offset commits and produce requests from a localhost listener
//! and needs no Docker.
//!
//! A `Retry` keeps the record at its offset: the consumer waits the hold
//! delay while it keeps polling, then hands the same record back to the
//! handler, so nothing behind it for the key is handled first. A dead letter
//! that cannot be published keeps the record too. The mock broker has no
//! CreateTopics API, so each test creates its topics through the mock API and
//! produces through a raw rdkafka producer, which sets the record key the
//! sequenced consumer reads as the sequence key.

#![cfg(feature = "kafka")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(feature = "metrics")]
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{
    BaseProducer, DefaultProducerContext, FutureProducer, FutureRecord, Producer,
};
use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::{SequenceFailure, TopologyBuilder};
use shove::{SequencedTopic, SupervisorOutcome};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// A record reaches the handler on a mock cluster in well under this.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a test watches for a delivery that must not come.
const SETTLE: Duration = Duration::from_millis(500);
/// How long a stop may take before `run_fifo_until_timeout` aborts the
/// consumer and reports it timed out: room for the final commit and the
/// close.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);
/// The sequence key every record of a test carries.
const KEY: &str = "acct-1";

const ORDER_TOPIC: &str = "kafka-fifo-in-place-order";
const ORDER_DLQ: &str = "kafka-fifo-in-place-order-dlq";
const ORDER_GROUP: &str = "kafka-fifo-in-place-order-fifo";
/// The hold delay of the ordering test, short so two retries stay quick.
const ORDER_HOLD: Duration = Duration::from_millis(50);

const OUTAGE_TOPIC: &str = "kafka-fifo-in-place-dlq-outage";
const OUTAGE_DLQ: &str = "kafka-fifo-in-place-dlq-outage-dlq";
const OUTAGE_GROUP: &str = "kafka-fifo-in-place-dlq-outage-fifo";

const STOP_TOPIC: &str = "kafka-fifo-in-place-stop";
const STOP_DLQ: &str = "kafka-fifo-in-place-stop-dlq";
const STOP_GROUP: &str = "kafka-fifo-in-place-stop-fifo";
/// Long enough that the stop always lands inside the hold.
const STOP_HOLD: Duration = Duration::from_secs(30);

const WAIT_COMMIT_TOPIC: &str = "kafka-fifo-in-place-wait-commit";
const WAIT_COMMIT_DLQ: &str = "kafka-fifo-in-place-wait-commit-dlq";
const WAIT_COMMIT_GROUP: &str = "kafka-fifo-in-place-wait-commit-fifo";
/// Long enough that a commit the test sees before it runs out landed during
/// the hold.
const WAIT_COMMIT_HOLD: Duration = Duration::from_secs(30);

const REVOKE_TOPIC: &str = "kafka-fifo-in-place-revoke";
const REVOKE_DLQ: &str = "kafka-fifo-in-place-revoke-dlq";
const REVOKE_GROUP: &str = "kafka-fifo-in-place-revoke-fifo";
/// Far longer than the rebalance a joining member starts, so the revoke
/// always lands inside the hold.
const REVOKE_HOLD: Duration = Duration::from_secs(90);
/// The bound for a wait that spans that rebalance: the mock coordinator
/// waits up to `session.timeout.ms` for its members to rejoin.
const REBALANCE_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(feature = "test-support")]
const EVICT_TOPIC: &str = "kafka-fifo-in-place-evict";
#[cfg(feature = "test-support")]
const EVICT_DLQ: &str = "kafka-fifo-in-place-evict-dlq";
#[cfg(feature = "test-support")]
const EVICT_GROUP: &str = "kafka-fifo-in-place-evict-fifo";
/// The shortest `max.poll.interval.ms` librdkafka admits: it refuses a value
/// below `session.timeout.ms`, which shove pins at 10 s.
#[cfg(feature = "test-support")]
const EVICT_POLL_INTERVAL: Duration = Duration::from_secs(10);
/// A hold well past that interval: a member that stopped polling for it
/// would leave its group.
#[cfg(feature = "test-support")]
const EVICT_HOLD: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
}

shove::define_sequenced_topic!(
    OrderTopic,
    Order,
    |_order: &Order| KEY.to_string(),
    TopologyBuilder::new(ORDER_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(ORDER_HOLD)
        .dlq()
        .build()
);

shove::define_sequenced_topic!(
    OutageTopic,
    Order,
    |_order: &Order| KEY.to_string(),
    TopologyBuilder::new(OUTAGE_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(ORDER_HOLD)
        .dlq()
        .build()
);

shove::define_sequenced_topic!(
    StopTopic,
    Order,
    |_order: &Order| KEY.to_string(),
    TopologyBuilder::new(STOP_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(STOP_HOLD)
        .dlq()
        .build()
);

shove::define_sequenced_topic!(
    WaitCommitTopic,
    Order,
    |_order: &Order| KEY.to_string(),
    TopologyBuilder::new(WAIT_COMMIT_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(WAIT_COMMIT_HOLD)
        .dlq()
        .build()
);

shove::define_sequenced_topic!(
    RevokeTopic,
    Order,
    |order: &Order| order.id.clone(),
    TopologyBuilder::new(REVOKE_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(2)
        .hold_queue(REVOKE_HOLD)
        .dlq()
        .build()
);

#[cfg(feature = "test-support")]
shove::define_sequenced_topic!(
    EvictTopic,
    Order,
    |_order: &Order| KEY.to_string(),
    TopologyBuilder::new(EVICT_TOPIC)
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .hold_queue(EVICT_HOLD)
        .dlq()
        .build()
);

/// One delivery as the handler saw it.
#[derive(Debug, Clone)]
struct Delivery {
    id: String,
    retry_count: u32,
    redelivered: bool,
    partition: Option<i32>,
    offset: Option<i64>,
    at: Instant,
}

/// Records every delivery and answers it with `decide`, given the record's
/// id and the delivery's metadata.
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
                redelivered: metadata.redelivered,
                partition: metadata.partition,
                offset: metadata.offset,
                at: Instant::now(),
            });
        outcome
    }
}

/// One mock broker with the given topics created, one partition each. The
/// producer owns the cluster and produces nothing.
struct Mock {
    owner: BaseProducer,
}

impl Mock {
    fn start(topics: &[&str]) -> Self {
        let owner: BaseProducer = ClientConfig::new()
            .set("test.mock.num.brokers", "1")
            .create()
            .expect("a producer that owns a mock cluster");
        let mock = Self { owner };
        for topic in topics {
            mock.api()
                .create_topic(topic, 1, 1)
                .expect("create the topic through the mock API");
        }
        mock
    }

    /// Creates one more topic, with `partitions` partitions.
    fn create_topic(&self, topic: &str, partitions: i32) {
        self.api()
            .create_topic(topic, partitions, 1)
            .expect("create the topic through the mock API");
    }

    fn api(&self) -> MockCluster<'_, DefaultProducerContext> {
        self.owner
            .client()
            .mock_cluster()
            .expect("the producer owns a mock cluster")
    }

    fn bootstrap(&self) -> String {
        self.api().bootstrap_servers()
    }

    /// Answer every Produce request with `NOT_ENOUGH_REPLICAS` until
    /// `restore_produce`. librdkafka retries that error until the message
    /// times out, so each publish attempt fails after the producer's
    /// `message.timeout.ms` with nothing written.
    fn fail_produce(&self) {
        let errors = vec![RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS; 100_000];
        self.api().request_errors(RDKafkaApiKey::Produce, &errors);
    }

    fn restore_produce(&self) {
        self.api().clear_request_errors(RDKafkaApiKey::Produce);
    }
}

async fn connect(bootstrap: &str) -> KafkaClient {
    KafkaClient::connect_with_retry(&KafkaConfig::new(bootstrap), 10)
        .await
        .expect("connect to the mock cluster")
}

/// Produces one record per id onto `topic`, in order, all under `KEY`.
async fn produce(bootstrap: &str, topic: &str, ids: &[&str]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("mock producer");
    for id in ids {
        let payload = serde_json::to_vec(&Order { id: (*id).into() }).expect("encode the record");
        producer
            .send(
                FutureRecord::to(topic).key(KEY).payload(&payload),
                Duration::from_secs(10),
            )
            .await
            .expect("produce to the mock cluster");
    }
}

/// Produces the record `id` onto `partition` of `topic`, keyed by its id.
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

/// The high watermark of `topic`'s one partition: how many records it holds.
async fn high_watermark(bootstrap: &str, topic: &'static str) -> i64 {
    let bootstrap = bootstrap.to_owned();
    tokio::task::spawn_blocking(move || {
        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .expect("watermark probe");
        probe
            .fetch_watermarks(topic, 0, Duration::from_secs(10))
            .expect("fetch the watermarks")
            .1
    })
    .await
    .expect("watermark task")
}

/// The group's committed position on the topic's one partition, read
/// through a raw consumer that never joins the group. `None` before the
/// first accepted commit.
async fn committed_position(
    bootstrap: &str,
    topic: &'static str,
    group: &'static str,
) -> Option<i64> {
    committed_position_of(bootstrap, topic, group, 0).await
}

/// [`committed_position`] on `partition`.
async fn committed_position_of(
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

/// Polls the broker until the group's committed position on the topic's one
/// partition is `expected`, failing once `timeout` has passed.
async fn wait_for_committed_position(
    bootstrap: &str,
    topic: &'static str,
    group: &'static str,
    expected: i64,
    timeout: Duration,
    what: &str,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let committed = committed_position(bootstrap, topic, group).await;
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

/// The ids of the first `count` records of `topic`, read by a raw consumer
/// from the start of partition 0.
async fn read_ids(bootstrap: &str, topic: &str, count: usize) -> Vec<String> {
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
    let mut ids = Vec::new();
    while ids.len() < count {
        let msg = tokio::time::timeout(DELIVERY_TIMEOUT, consumer.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{topic} held {} of {count} records within {DELIVERY_TIMEOUT:?}",
                    ids.len()
                )
            })
            .expect("raw consumer receive");
        let order: Order =
            serde_json::from_slice(msg.payload().unwrap_or_default()).expect("an order payload");
        ids.push(order.id);
    }
    ids
}

/// A sequenced consumer running on its own task, on a fresh client, through
/// `run_fifo_until_timeout`: its outcome counts a consumer task that ended
/// with an error, a final commit the broker refused included, which
/// `run_fifo` only logs.
struct Running {
    stop: CancellationToken,
    task: JoinHandle<SupervisorOutcome>,
}

impl Running {
    async fn fifo<T>(bootstrap: &str, handler: &Recorder) -> Self
    where
        T: SequencedTopic<Message = Order>,
    {
        let client = connect(bootstrap).await;
        let stop = CancellationToken::new();
        let signal = stop.clone().cancelled_owned();
        let handler = handler.clone();
        let task = tokio::spawn(async move {
            KafkaConsumer::new(client)
                .run_fifo_until_timeout::<T, _, _>(
                    handler,
                    (),
                    ConsumerOptions::<Kafka>::new(),
                    signal,
                    STOP_TIMEOUT,
                )
                .await
        });
        Self { stop, task }
    }

    /// Waits for `done`, failing at once if the consumer ends first.
    async fn wait_until(&self, done: impl Fn() -> bool, timeout: Duration, what: &str) {
        wait_until(
            || {
                assert!(!self.task.is_finished(), "the consumer ended before {what}");
                done()
            },
            timeout,
            what,
        )
        .await;
    }

    /// Stops the consumer, fails unless it ended clean, final commit
    /// included, and returns how long the stop took.
    async fn stop(self) -> Duration {
        let started = Instant::now();
        let outcome = self.stop_outcome().await;
        assert!(
            outcome.is_clean(),
            "the consumer stops clean, its final commit accepted: {outcome:?}"
        );
        started.elapsed()
    }

    /// Stops the consumer and returns what its run reported.
    async fn stop_outcome(self) -> SupervisorOutcome {
        self.stop.cancel();
        self.task.await.expect("the consumer task joins")
    }
}

/// `Retry` on the first two deliveries of `A`, `Ack` otherwise.
fn retry_a_twice(id: &str, metadata: &MessageMetadata) -> Outcome {
    if id == "A" && metadata.retry_count < 2 {
        Outcome::Retry
    } else {
        Outcome::Ack
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

/// A `Retry` keeps the record at its offset. A record of the same key
/// behind it is handled only once the retried record is acked, so the
/// handler sees `A` three times and then `B`, with the count kept in memory.
/// Nothing is published to the topic: it still holds the two records.
#[tokio::test]
async fn fifo_retry_keeps_key_order() {
    let mock = Mock::start(&[ORDER_TOPIC, ORDER_DLQ]);
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, ORDER_TOPIC, &["A", "B"]).await;

    let handler = Recorder::new(retry_a_twice);
    let consumer = Running::fifo::<OrderTopic>(&bootstrap, &handler).await;
    consumer
        .wait_until(
            || handler.count() == 4,
            DELIVERY_TIMEOUT,
            "A's two retries and B reaching the handler",
        )
        .await;
    tokio::time::sleep(SETTLE).await;
    consumer.stop().await;

    let seen = handler.deliveries();
    assert_eq!(
        handler.ids(),
        ["A", "A", "A", "B"],
        "B waits behind every retry of A: {seen:?}"
    );
    assert_eq!(
        seen.iter().map(|d| d.retry_count).collect::<Vec<_>>(),
        [0, 1, 2, 0],
        "the retried record carries its attempt count: {seen:?}"
    );
    assert_eq!(
        seen.iter().map(|d| d.redelivered).collect::<Vec<_>>(),
        [false, true, true, false],
        "a retry hands the same record back as a redelivery: {seen:?}"
    );
    assert_eq!(
        high_watermark(&bootstrap, ORDER_TOPIC).await,
        2,
        "a retry publishes no copy into the topic"
    );
    assert_eq!(
        committed_position(&bootstrap, ORDER_TOPIC, ORDER_GROUP).await,
        Some(2),
        "both records are committed once they are acked"
    );
}

/// The records acked ahead of a held record are committed while it holds.
/// `A1`, `A2` and `A3` are acked back to back inside one commit window: the
/// commit of `A1` opens the window, and the positions of the two behind it
/// wait for the gate. `B` is then retried and held for 30 s. The position
/// past `A3` is committed during the hold, within a few windows, and nothing
/// is committed past `B`.
#[tokio::test]
async fn fifo_commits_acked_positions_during_a_hold() {
    let mock = Mock::start(&[WAIT_COMMIT_TOPIC, WAIT_COMMIT_DLQ]);
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, WAIT_COMMIT_TOPIC, &["A1", "A2", "A3", "B"]).await;

    let handler = Recorder::new(|id, metadata| {
        if id == "B" && metadata.retry_count == 0 {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    });
    let consumer = Running::fifo::<WaitCommitTopic>(&bootstrap, &handler).await;
    consumer
        .wait_until(
            || handler.count() == 4,
            DELIVERY_TIMEOUT,
            "B reaching the handler behind the three acked records",
        )
        .await;
    let held_at = handler.deliveries()[3].at;
    wait_for_committed_position(
        &bootstrap,
        WAIT_COMMIT_TOPIC,
        WAIT_COMMIT_GROUP,
        3,
        Duration::from_secs(10),
        "the acked records committed during B's hold",
    )
    .await;
    assert!(
        held_at.elapsed() < WAIT_COMMIT_HOLD,
        "the commit landed while B was held"
    );
    assert_eq!(
        handler.ids(),
        ["A1", "A2", "A3", "B"],
        "B is still held: nothing was handed back or taken behind it"
    );
    consumer.stop().await;
    assert_eq!(
        committed_position(&bootstrap, WAIT_COMMIT_TOPIC, WAIT_COMMIT_GROUP).await,
        Some(3),
        "nothing is committed past the held record"
    );
}

/// A hold longer than `max.poll.interval.ms` keeps the member in its group:
/// the consumer polls while it waits, so librdkafka never evicts it and the
/// group never rebalances. The member is started through
/// `KafkaConsumerGroup::new_fifo`, whose config carries the `test-support`
/// seam that shrinks the poll interval and which declares nothing on the
/// mock broker. `A` is retried once and then acked, and `B` follows it.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn fifo_hold_longer_than_max_poll_interval_does_not_evict() {
    use shove::kafka::{KafkaConsumerGroup, KafkaConsumerGroupConfig, per_record_probe};

    let mock = Mock::start(&[EVICT_TOPIC, EVICT_DLQ]);
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, EVICT_TOPIC, &["A", "B"]).await;

    let handler = Recorder::new(retry_a_once);
    let token = CancellationToken::new();
    let h = handler.clone();
    let mut group = KafkaConsumerGroup::new_fifo::<EvictTopic, _>(
        EVICT_TOPIC,
        connect(&bootstrap).await,
        KafkaConsumerGroupConfig::new(1..=1).with_max_poll_interval_for_test(EVICT_POLL_INTERVAL),
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
        EVICT_HOLD + DELIVERY_TIMEOUT,
        "A's retry and B reaching the handler",
    )
    .await;
    tokio::time::sleep(SETTLE).await;

    let seen = handler.deliveries();
    assert_eq!(
        handler.ids(),
        ["A", "A", "B"],
        "B follows A's retry: {seen:?}"
    );
    assert_eq!(
        seen.iter().map(|d| d.retry_count).collect::<Vec<_>>(),
        [0, 1, 0],
        "A comes back with its attempt counted, never as a fresh delivery: {seen:?}"
    );
    let held = seen[1].at.saturating_duration_since(seen[0].at);
    assert!(
        held >= EVICT_HOLD - Duration::from_millis(500),
        "A is handed back after its hold, held {held:?}"
    );
    assert_eq!(
        per_record_probe::assigns_queued(),
        assigns_before,
        "no rebalance and no reconnect while A was held past the poll interval"
    );

    token.cancel();
    group.shutdown().await;
    assert_eq!(
        committed_position(&bootstrap, EVICT_TOPIC, EVICT_GROUP).await,
        Some(2),
        "the member still in its group commits both records"
    );
}

/// A dead letter that cannot be published keeps its record. The DLQ topic
/// refuses every write; the rejected `A` stays uncommitted and `B` is not
/// handled while the publish is retried. Once the DLQ takes writes again,
/// `A` is dead-lettered, once, and `B` is handled. A record that is kept is
/// not discarded, so `messages_discarded_total` does not move.
#[tokio::test]
async fn fifo_dlq_publish_failure_redelivers() {
    /// Longer than one failed dead-letter publish, which tries three times
    /// and gives each attempt the producer's 5 s message timeout.
    const FAILED_PUBLISH: Duration = Duration::from_secs(20);

    #[cfg(feature = "metrics")]
    let snapshotter = {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install the debugging recorder");
        snapshotter
    };

    let mock = Mock::start(&[OUTAGE_TOPIC, OUTAGE_DLQ]);
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, OUTAGE_TOPIC, &["A", "B"]).await;
    mock.fail_produce();

    let handler = Recorder::new(|id, _| {
        if id == "A" {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    });
    let consumer = Running::fifo::<OutageTopic>(&bootstrap, &handler).await;
    consumer
        .wait_until(
            || handler.count() == 1,
            DELIVERY_TIMEOUT,
            "A reaching the handler",
        )
        .await;
    tokio::time::sleep(FAILED_PUBLISH).await;
    assert_eq!(
        handler.ids(),
        ["A"],
        "B is not handled while A's dead letter cannot be published"
    );
    assert_eq!(
        committed_position(&bootstrap, OUTAGE_TOPIC, OUTAGE_GROUP)
            .await
            .unwrap_or(0),
        0,
        "A is not committed while its dead letter cannot be published"
    );

    mock.restore_produce();
    consumer
        .wait_until(
            || handler.count() == 2,
            DELIVERY_TIMEOUT,
            "B reaching the handler once A is dead-lettered",
        )
        .await;
    assert_eq!(
        read_ids(&bootstrap, OUTAGE_DLQ, 1).await,
        ["A"],
        "A is dead-lettered once the DLQ takes writes again"
    );
    tokio::time::sleep(SETTLE).await;
    consumer.stop().await;

    assert_eq!(
        handler.ids(),
        ["A", "B"],
        "A is handled once and its dead letter retried without the handler"
    );
    assert_eq!(
        high_watermark(&bootstrap, OUTAGE_DLQ).await,
        1,
        "A is dead-lettered once"
    );
    assert_eq!(
        committed_position(&bootstrap, OUTAGE_TOPIC, OUTAGE_GROUP).await,
        Some(2),
        "A is committed once dead-lettered, and B once acked"
    );

    #[cfg(feature = "metrics")]
    {
        let discarded: Vec<(String, u64)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "shove_messages_discarded_total"
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == "topic" && l.value() == OUTAGE_TOPIC)
            })
            .map(|(key, _, _, value)| {
                let reason = key
                    .key()
                    .labels()
                    .find(|l| l.key() == "reason")
                    .map_or_else(String::new, |l| l.value().to_string());
                match value {
                    DebugValue::Counter(n) => (reason, n),
                    other => panic!("shove_messages_discarded_total is not a counter: {other:?}"),
                }
            })
            .filter(|(_, n)| *n > 0)
            .collect();
        assert!(
            discarded.is_empty(),
            "a record kept through a DLQ outage is never counted as discarded: {discarded:?}"
        );
    }
}

/// A stop during a hold returns at once, handles nothing behind the held
/// record and commits nothing past it. A consumer started afterwards on the
/// same group is handed the held record first, with its count read afresh
/// from its headers, which carry none.
#[tokio::test]
async fn fifo_shutdown_mid_hold_loses_nothing() {
    let mock = Mock::start(&[STOP_TOPIC, STOP_DLQ]);
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, STOP_TOPIC, &["A", "B"]).await;

    let first = Recorder::new(retry_a_once);
    let consumer = Running::fifo::<StopTopic>(&bootstrap, &first).await;
    consumer
        .wait_until(
            || first.count() == 1,
            DELIVERY_TIMEOUT,
            "A reaching the handler",
        )
        .await;
    tokio::time::sleep(SETTLE).await;
    let took = consumer.stop().await;
    assert!(
        took < STOP_HOLD / 3,
        "the stop cuts the hold short, took {took:?}"
    );
    assert_eq!(
        first.ids(),
        ["A"],
        "nothing behind the held record is handled once the stop lands"
    );
    assert_eq!(
        committed_position(&bootstrap, STOP_TOPIC, STOP_GROUP).await,
        Some(0),
        "the stop's final commit lands at the held record and not past it"
    );

    let second = Recorder::new(|_, _| Outcome::Ack);
    let restarted = Running::fifo::<StopTopic>(&bootstrap, &second).await;
    restarted
        .wait_until(
            || second.count() == 2,
            DELIVERY_TIMEOUT,
            "both records reaching the restarted consumer",
        )
        .await;
    tokio::time::sleep(SETTLE).await;
    restarted.stop().await;

    let seen = second.deliveries();
    assert_eq!(
        second.ids(),
        ["A", "B"],
        "the restarted consumer is handed the held record first: {seen:?}"
    );
    assert_eq!(
        seen[0].retry_count, 0,
        "the count starts again from the record's headers: {seen:?}"
    );
    assert_eq!(
        committed_position(&bootstrap, STOP_TOPIC, STOP_GROUP).await,
        Some(2),
        "both records are committed once the restarted consumer acked them"
    );
}

/// A revoke of the held record's partition ends the hold with nothing
/// committed for the record, and the partition's next owner is handed it
/// from its head. Member A alone holds both partitions, takes the head of
/// one and retries it, so it waits out a 90 s hold. Member B joins the group
/// and the coordinator moves one partition to it. The rebalance needs A to
/// serve its revoke, so B handling the moved head inside A's hold shows A
/// kept polling through it; A hands nothing of the moved partition to its
/// handler afterwards. Which partition moves is the assignor's choice. When
/// it is the held record's, the revoke ends A's hold and A goes on to the
/// head of the partition it kept; otherwise A keeps holding its record.
/// Neither member commits a partition past the head it holds: B commits the
/// moved head once it acked it, and A's kept partition stays at its head.
#[tokio::test]
async fn fifo_revoke_mid_hold_leaves_the_record_to_its_next_owner() {
    let mock = Mock::start(&[REVOKE_DLQ]);
    mock.create_topic(REVOKE_TOPIC, 2);
    let bootstrap = mock.bootstrap();
    produce_to(&bootstrap, REVOKE_TOPIC, 0, "h0").await;
    produce_to(&bootstrap, REVOKE_TOPIC, 1, "h1").await;

    let a = Recorder::new(|_, metadata| {
        if metadata.retry_count == 0 {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    });
    let member_a = Running::fifo::<RevokeTopic>(&bootstrap, &a).await;
    member_a
        .wait_until(
            || a.count() == 1,
            DELIVERY_TIMEOUT,
            "A taking the head of one partition",
        )
        .await;
    let held = a.deliveries()[0].clone();
    let held_partition = held.partition.expect("Kafka reports the partition");

    let b = Recorder::new(|_, _| Outcome::Ack);
    let member_b = Running::fifo::<RevokeTopic>(&bootstrap, &b).await;
    member_b
        .wait_until(
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
        held.at.elapsed() < REVOKE_HOLD,
        "the rebalance completed inside A's hold"
    );

    if moved == held_partition {
        let kept = 1 - moved;
        member_a
            .wait_until(
                || a.deliveries().iter().any(|d| d.partition == Some(kept)),
                REBALANCE_TIMEOUT,
                "A taking the head of the partition it kept",
            )
            .await;
        assert!(
            held.at.elapsed() < REVOKE_HOLD,
            "the revoke ended A's hold on the moved partition's head"
        );
    }
    tokio::time::sleep(SETTLE).await;

    let seen_by_a = a.deliveries();
    assert!(
        seen_by_a
            .iter()
            .filter(|d| d.at >= first_of_b.at)
            .all(|d| d.partition != Some(moved)),
        "A hands nothing of the moved partition to its handler once B took it: {seen_by_a:?}"
    );
    assert!(
        seen_by_a.iter().all(|d| !d.redelivered),
        "A never handed a held record back, its hold never ran out: {seen_by_a:?}"
    );
    assert_eq!(
        b.ids(),
        std::slice::from_ref(&first_of_b.id),
        "B handles the moved partition's head once"
    );

    // B stops with the group stable, so its final commit must land. A stops
    // inside the rebalance B's departure starts, and the coordinator may
    // refuse A's final commit with a rebalance in progress, which the Kafka
    // page documents for a stop that meets a rebalance; only the position A
    // commits matters here.
    member_b.stop().await;
    let a_outcome = member_a.stop_outcome().await;
    assert!(
        a_outcome.panics == 0 && !a_outcome.timed_out,
        "A stops without a panic and inside its bound: {a_outcome:?}"
    );
    let kept = 1 - moved;
    assert_eq!(
        committed_position_of(&bootstrap, REVOKE_TOPIC, REVOKE_GROUP, moved).await,
        Some(1),
        "the moved partition is committed past its head once, by B"
    );
    let kept_committed = committed_position_of(&bootstrap, REVOKE_TOPIC, REVOKE_GROUP, kept).await;
    assert!(
        matches!(kept_committed, None | Some(0)),
        "A never commits its kept partition past the head it still holds: {kept_committed:?}"
    );
}
