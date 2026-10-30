//! Record headers a producer may send, read and carried over by shove's Kafka
//! consumer, against librdkafka's mock cluster (`rdkafka::mocking`), which
//! needs no Docker.
//!
//! The Kafka protocol carries a header key as bytes and a value as bytes or
//! null, and a producer may repeat a key. rdkafka's `Headers` accessors
//! unwrap every key as UTF-8 and `OwnedHeaders::insert` takes only `&str`
//! keys, so these tests write and read headers through librdkafka's C API,
//! `rdkafka::bindings`. Each topic is created through the mock API, because
//! the mock broker has no CreateTopics, and every record the tests produce
//! goes through a raw rdkafka producer.

#![cfg(feature = "kafka")]

use std::collections::{HashMap, HashSet};
use std::ffi::CStr;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::bindings::{
    rd_kafka_header_add, rd_kafka_header_cnt, rd_kafka_header_get_all, rd_kafka_message_headers,
};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Message, OwnedHeaders};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{
    BaseProducer, DefaultProducerContext, FutureProducer, FutureRecord, Producer,
};
use rdkafka::types::{RDKafkaHeaders, RDKafkaRespErr};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::kafka::{BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::topology::{SequenceFailure, TopologyBuilder};
use shove::{BroadcastStart, SequencedTopic, ShoveError, Topic};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// A record reaches the handler, and a copy its topic, on a mock cluster in
/// well under this.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a test watches for a delivery that must not come.
const SETTLE: Duration = Duration::from_millis(500);
/// The hold delay of the topologies that retry or defer, short so a chain of
/// hops stays quick.
const HOLD: Duration = Duration::from_millis(50);

/// A header key that is not UTF-8, as a Go producer may send one.
const HOSTILE_KEY: &[u8] = &[0xff, 0xfe];
/// A header value that is not UTF-8.
const BINARY_VALUE: &[u8] = &[0x00, 0xff];
/// Every header a `String` map cannot hold, with a repeated key: a copy must
/// carry all of them byte for byte and in this order.
const HOSTILE_HEADERS: [(&[u8], Option<&[u8]>); 5] = [
    (HOSTILE_KEY, Some(b"k")),
    (b"bin", Some(BINARY_VALUE)),
    (b"null", None),
    (b"dup", Some(b"first")),
    (b"dup", Some(b"second")),
];

const HOSTILE_TOPIC: &str = "kafka-headers-hostile-key";
const RETRY_TOPIC: &str = "kafka-headers-retry-copy";
const DEFER_TOPIC: &str = "kafka-headers-defer-id";
const DEATH_TOPIC: &str = "kafka-headers-death-count";
const DEATH_DLQ: &str = "kafka-headers-death-count-dlq";
const DEAD_LETTER_TOPIC: &str = "kafka-headers-dead-letter";
const DEAD_LETTER_DLQ: &str = "kafka-headers-dead-letter-dlq";
const SEQUENCED_TOPIC: &str = "kafka-headers-sequenced";
const SEQUENCED_DLQ: &str = "kafka-headers-sequenced-dlq";
const BATCH_TOPIC: &str = "kafka-headers-batch";
const BATCH_DLQ: &str = "kafka-headers-batch-dlq";
const BROADCAST_TOPIC: &str = "kafka-headers-broadcast";
const RETRY_ID_TOPIC: &str = "kafka-headers-retry-ids";
const DEAD_ID_TOPIC: &str = "kafka-headers-dead-letter-ids";
const DEAD_ID_DLQ: &str = "kafka-headers-dead-letter-ids-dlq";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
}

shove::define_topic!(
    HostileTopic,
    Order,
    TopologyBuilder::new(HOSTILE_TOPIC).build()
);
shove::define_topic!(
    RetryTopic,
    Order,
    TopologyBuilder::new(RETRY_TOPIC).hold_queue(HOLD).build()
);
shove::define_topic!(
    DeferTopic,
    Order,
    TopologyBuilder::new(DEFER_TOPIC).hold_queue(HOLD).build()
);
shove::define_topic!(
    DeathTopic,
    Order,
    TopologyBuilder::new(DEATH_TOPIC).dlq().build()
);
shove::define_topic!(
    DeadLetterTopic,
    Order,
    TopologyBuilder::new(DEAD_LETTER_TOPIC).dlq().build()
);
shove::define_sequenced_topic!(
    SequencedOrders,
    Order,
    |order: &Order| order.id.clone(),
    TopologyBuilder::new(SEQUENCED_TOPIC)
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(1)
        .hold_queue(HOLD)
        .dlq()
        .build()
);
shove::define_topic!(
    BatchTopic,
    Order,
    TopologyBuilder::new(BATCH_TOPIC).dlq().build()
);
shove::define_topic!(
    BroadcastTopic,
    Order,
    TopologyBuilder::new(BROADCAST_TOPIC).broadcast().build()
);
shove::define_topic!(
    RetryIdTopic,
    Order,
    TopologyBuilder::new(RETRY_ID_TOPIC)
        .hold_queue(HOLD)
        .build()
);
shove::define_topic!(
    DeadIdTopic,
    Order,
    TopologyBuilder::new(DEAD_ID_TOPIC).dlq().build()
);

/// One delivery as the handler saw it.
#[derive(Clone)]
struct Delivery {
    id: String,
    metadata: MessageMetadata,
}

/// Records every delivery and answers it with `decide`, given the number of
/// deliveries before it and the delivery's metadata; a batch is answered by
/// its first message. Records every dead letter a drain hands it too.
#[derive(Clone)]
struct Recorder {
    seen: Arc<Mutex<Vec<Delivery>>>,
    dead: Arc<Mutex<Vec<(String, DeadMessageMetadata)>>>,
    decide: fn(usize, &MessageMetadata) -> Outcome,
}

impl Recorder {
    fn new(decide: fn(usize, &MessageMetadata) -> Outcome) -> Self {
        Self {
            seen: Arc::default(),
            dead: Arc::default(),
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

    fn dead_letters(&self) -> Vec<(String, DeadMessageMetadata)> {
        self.dead.lock().expect("handler mutex poisoned").clone()
    }
}

impl<T: Topic<Message = Order>> MessageHandler<T> for Recorder {
    type Context = ();
    async fn handle(&self, msg: Order, metadata: MessageMetadata, _: &()) -> Outcome {
        let mut seen = self.seen.lock().expect("handler mutex poisoned");
        let outcome = (self.decide)(seen.len(), &metadata);
        seen.push(Delivery {
            id: msg.id,
            metadata,
        });
        outcome
    }

    async fn handle_dead(&self, msg: Order, metadata: DeadMessageMetadata, _: &()) {
        self.dead
            .lock()
            .expect("handler mutex poisoned")
            .push((msg.id, metadata));
    }
}

impl<T: Topic<Message = Order>> BatchMessageHandler<T> for Recorder {
    type Context = ();
    async fn handle_batch(&self, messages: Vec<(Order, MessageMetadata)>, _: &()) -> Outcome {
        let mut seen = self.seen.lock().expect("handler mutex poisoned");
        let before = seen.len();
        let outcome = messages.first().map_or(Outcome::Ack, |(_, metadata)| {
            (self.decide)(before, metadata)
        });
        seen.extend(messages.into_iter().map(|(order, metadata)| Delivery {
            id: order.id,
            metadata,
        }));
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

    fn api(&self) -> MockCluster<'_, DefaultProducerContext> {
        self.owner
            .client()
            .mock_cluster()
            .expect("the producer owns a mock cluster")
    }

    fn bootstrap(&self) -> String {
        self.api().bootstrap_servers()
    }
}

async fn connect(bootstrap: &str) -> KafkaClient {
    KafkaClient::connect_with_retry(&KafkaConfig::new(bootstrap), 10)
        .await
        .expect("connect to the mock cluster")
}

/// Waits for `done`, failing once `DELIVERY_TIMEOUT` passes.
async fn wait_for(done: impl Fn() -> bool, what: &str) {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while !done() {
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {DELIVERY_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A shove consume loop running on its own task.
struct Running {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), ShoveError>>,
}

impl Running {
    /// Runs `consume` on its own task, with a consumer on a fresh client and
    /// that client's shutdown token, for `consume` to pass on as the loop's
    /// own as well: the DLQ drain stops on the client's token alone.
    async fn spawn<F, Fut>(bootstrap: &str, consume: F) -> Self
    where
        F: FnOnce(KafkaConsumer, CancellationToken) -> Fut,
        Fut: Future<Output = Result<(), ShoveError>> + Send + 'static,
    {
        let client = connect(bootstrap).await;
        let shutdown = client.shutdown_token();
        let task = tokio::spawn(consume(KafkaConsumer::new(client), shutdown.clone()));
        Self { shutdown, task }
    }

    /// The consumer of `T`'s topic.
    async fn start<T>(bootstrap: &str, handler: &Recorder) -> Self
    where
        T: Topic<Message = Order>,
    {
        let handler = handler.clone();
        Self::spawn(bootstrap, |consumer, shutdown| async move {
            consumer
                .run::<T, _>(
                    handler,
                    (),
                    ConsumerOptions::<Kafka>::new().with_shutdown(shutdown),
                )
                .await
        })
        .await
    }

    /// Waits for `done`, failing at once if the loop ends first.
    async fn wait_until(&self, done: impl Fn() -> bool, what: &str) {
        wait_for(
            || {
                assert!(!self.task.is_finished(), "the consumer ended before {what}");
                done()
            },
            what,
        )
        .await;
    }

    async fn stop(self) {
        self.shutdown.cancel();
        self.task
            .await
            .expect("the consumer task joins")
            .expect("the consumer stops clean");
    }
}

/// A header as it travels: its key, and its value or `None` for a null one.
type WireHeader = (Vec<u8>, Option<Vec<u8>>);

fn owned(headers: &[(&[u8], Option<&[u8]>)]) -> Vec<WireHeader> {
    headers
        .iter()
        .map(|(key, value)| (key.to_vec(), value.map(<[u8]>::to_vec)))
        .collect()
}

/// `headers` as an `OwnedHeaders`, keys as raw bytes. rdkafka hands its list
/// out as a reference to the zero-sized `BorrowedHeaders` at the list's own
/// address, so that reference cast back is the list librdkafka adds to.
fn wire(headers: &[(&[u8], Option<&[u8]>)]) -> OwnedHeaders {
    let list = OwnedHeaders::new_with_capacity(headers.len());
    let native = std::ptr::from_ref::<BorrowedHeaders>(list.as_borrowed())
        .cast_mut()
        .cast::<RDKafkaHeaders>();
    for (key, value) in headers {
        let key_len = isize::try_from(key.len()).expect("a short key");
        let (value_ptr, value_len) = match value {
            Some(value) => (
                value.as_ptr().cast(),
                isize::try_from(value.len()).expect("a short value"),
            ),
            None => (std::ptr::null(), 0),
        };
        let err = unsafe {
            rd_kafka_header_add(native, key.as_ptr().cast(), key_len, value_ptr, value_len)
        };
        assert_eq!(
            err,
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR,
            "librdkafka refused a header"
        );
    }
    list
}

fn order_payload(id: &str) -> Vec<u8> {
    serde_json::to_vec(&Order { id: id.into() }).expect("encode the record")
}

/// Produces `payload` to `topic` under `key`, carrying exactly `headers`, in
/// order.
async fn produce_record(
    bootstrap: &str,
    topic: &str,
    key: &str,
    payload: &[u8],
    headers: &[(&[u8], Option<&[u8]>)],
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("enable.idempotence", "false")
        .create()
        .expect("mock producer");
    producer
        .send(
            FutureRecord::to(topic)
                .key(key)
                .payload(payload)
                .headers(wire(headers)),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
}

/// Produces order `id`, keyed by its id, carrying exactly `headers`.
async fn produce(bootstrap: &str, topic: &str, id: &str, headers: &[(&[u8], Option<&[u8]>)]) {
    produce_record(bootstrap, topic, id, &order_payload(id), headers).await;
}

/// Every header of `msg`, in order and byte for byte.
fn headers_of(msg: &BorrowedMessage<'_>) -> Vec<WireHeader> {
    let mut list = std::ptr::null_mut();
    let err = unsafe { rd_kafka_message_headers(msg.ptr(), &mut list) };
    if err == RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOENT {
        return Vec::new();
    }
    assert_eq!(err, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
    let count = unsafe { rd_kafka_header_cnt(list) };
    (0..count)
        .map(|idx| {
            let mut name = std::ptr::null();
            let mut value = std::ptr::null();
            let mut size = 0;
            let err =
                unsafe { rd_kafka_header_get_all(list, idx, &mut name, &mut value, &mut size) };
            assert_eq!(err, RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
            let key = unsafe { CStr::from_ptr(name) }.to_bytes().to_vec();
            let value = (!value.is_null())
                .then(|| unsafe { std::slice::from_raw_parts(value.cast::<u8>(), size) }.to_vec());
            (key, value)
        })
        .collect()
}

/// A record as a raw consumer reads it back.
struct Record {
    key: String,
    payload: Vec<u8>,
    headers: Vec<WireHeader>,
}

impl Record {
    fn order_id(&self) -> String {
        serde_json::from_slice::<Order>(&self.payload)
            .expect("an order payload")
            .id
    }
}

/// The first `count` records of `topic`, read by a raw consumer from the
/// start of partition 0.
async fn read_topic(bootstrap: &str, topic: &str, count: usize) -> Vec<Record> {
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
        records.push(Record {
            key: String::from_utf8_lossy(msg.key().unwrap_or_default()).into_owned(),
            payload: msg.payload().unwrap_or_default().to_vec(),
            headers: headers_of(&msg),
        });
    }
    records
}

/// `headers` without shove's own, which a copy rewrites.
fn foreign(headers: &[WireHeader]) -> Vec<WireHeader> {
    headers
        .iter()
        .filter(|(key, _)| !key.starts_with(b"Shove-"))
        .cloned()
        .collect()
}

/// A handler's header map without shove's own.
fn foreign_map(headers: &HashMap<String, String>) -> HashMap<String, String> {
    headers
        .iter()
        .filter(|(key, _)| !key.starts_with("Shove-"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// What a handler's map holds of `HOSTILE_HEADERS`: the one header whose key
/// and value are both UTF-8, the repeated key at its last value.
fn hostile_headers_as_strings() -> HashMap<String, String> {
    HashMap::from([("dup".to_string(), "second".to_string())])
}

/// The value of every `key` header in `headers`, in order.
fn values_of(headers: &[WireHeader], key: &str) -> Vec<Option<Vec<u8>>> {
    headers
        .iter()
        .filter(|(k, _)| k == key.as_bytes())
        .map(|(_, value)| value.clone())
        .collect()
}

/// The value of every `key` header in `headers`, in order, as text.
fn texts_of(headers: &[WireHeader], key: &str) -> Vec<String> {
    values_of(headers, key)
        .into_iter()
        .map(|value| String::from_utf8_lossy(&value.unwrap_or_default()).into_owned())
        .collect()
}

/// A header key that is not UTF-8 reaches the receive loop as bytes: the
/// record is handed to the handler once, with only the headers whose key and
/// value are both UTF-8, and the consumer goes on to the record behind it.
#[tokio::test]
async fn non_utf8_header_key_does_not_panic_the_consumer() {
    let mock = Mock::start(&[HOSTILE_TOPIC]);
    let bootstrap = mock.bootstrap();
    produce(
        &bootstrap,
        HOSTILE_TOPIC,
        "hostile",
        &[
            (HOSTILE_KEY, Some(b"v")),
            (b"trace", Some(b"abc")),
            (b"bin", Some(BINARY_VALUE)),
        ],
    )
    .await;

    let handler = Recorder::new(|_, _| Outcome::Ack);
    let consumer = Running::start::<HostileTopic>(&bootstrap, &handler).await;
    consumer
        .wait_until(
            || handler.count() == 1,
            "the hostile record reaching the handler",
        )
        .await;
    produce(&bootstrap, HOSTILE_TOPIC, "after", &[]).await;
    consumer
        .wait_until(
            || handler.count() == 2,
            "the record behind it reaching the handler",
        )
        .await;
    tokio::time::sleep(SETTLE).await;
    consumer.stop().await;

    let seen = handler.deliveries();
    assert_eq!(
        seen.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
        ["hostile", "after"],
        "each record is handled once"
    );
    assert_eq!(
        *seen[0].metadata.headers,
        HashMap::from([("trace".to_string(), "abc".to_string())]),
        "the handler sees only the header whose key and value are UTF-8"
    );
}

/// A `Retry` republishes a copy that carries every header of the original,
/// byte for byte and in order, a repeated key twice.
#[tokio::test]
async fn retry_copy_preserves_binary_and_duplicate_headers() {
    let mock = Mock::start(&[RETRY_TOPIC]);
    let bootstrap = mock.bootstrap();
    let original: [(&[u8], Option<&[u8]>); 3] = [
        (b"bin", Some(BINARY_VALUE)),
        (b"dup", Some(b"first")),
        (b"dup", Some(b"second")),
    ];
    produce(&bootstrap, RETRY_TOPIC, "retried", &original).await;

    let handler = Recorder::new(|_, metadata| {
        if metadata.retry_count == 0 {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    });
    let consumer = Running::start::<RetryTopic>(&bootstrap, &handler).await;
    consumer
        .wait_until(
            || handler.count() == 2,
            "the retry copy reaching the handler",
        )
        .await;
    consumer.stop().await;

    let records = read_topic(&bootstrap, RETRY_TOPIC, 2).await;
    let copy = &records[1];
    assert_eq!(copy.key, "retried", "the second record is the retry copy");
    assert_eq!(
        foreign(&copy.headers),
        owned(&original),
        "the copy carries the original's headers byte for byte and in order: {:?}",
        copy.headers
    );
    assert_eq!(
        values_of(&copy.headers, "Shove-Retry-Count"),
        [Some(b"1".to_vec())],
        "the copy carries one retry count, the incremented one"
    );
    assert_eq!(
        values_of(&copy.headers, "Shove-Message-Id").len(),
        1,
        "the copy carries one message id: {:?}",
        copy.headers
    );
}

/// Each `Defer` republishes a copy whose `Shove-Message-Id` is the
/// publisher's id, carried as the copy's base, and one suffix: twenty defers
/// leave the id one suffix long, and every hop's id is new. Every copy after
/// the first is made from a copy that carries a base already, and read back
/// raw each carries one base id and one message id: the copy drops the ones
/// it replaces, which a handler's map, keeping the last of a repeated key,
/// would not show.
#[tokio::test]
async fn message_id_length_is_bounded_across_defers() {
    const DEFERS: usize = 20;
    let mock = Mock::start(&[DEFER_TOPIC]);
    let bootstrap = mock.bootstrap();
    let publisher = Broker::<Kafka>::from_client(connect(&bootstrap).await);
    publisher
        .publisher()
        .await
        .expect("a publisher on the client")
        .publish::<DeferTopic>(&Order {
            id: "deferred".into(),
        })
        .await
        .expect("publish through shove's producer");

    let handler = Recorder::new(|before, _| {
        if before < DEFERS {
            Outcome::Defer
        } else {
            Outcome::Ack
        }
    });
    let consumer = Running::start::<DeferTopic>(&bootstrap, &handler).await;
    consumer
        .wait_until(
            || handler.count() == DEFERS + 1,
            "the twentieth defer copy reaching the handler",
        )
        .await;
    tokio::time::sleep(SETTLE).await;
    consumer.stop().await;
    publisher.close().await;

    let deliveries = handler.deliveries();
    let ids: Vec<String> = deliveries
        .iter()
        .map(|d| d.metadata.delivery_id.clone())
        .collect();
    assert_eq!(ids.len(), DEFERS + 1, "the acked copy is delivered once");
    let base = &ids[0];
    assert!(
        deliveries[1..]
            .iter()
            .all(|d| { d.metadata.headers.get("Shove-Base-Message-Id") == Some(base) }),
        "every copy carries the publisher's id as its base"
    );
    let last = &ids[DEFERS];
    assert!(
        last.len() <= base.len() + 40,
        "after {DEFERS} defers the id is {} bytes over the publisher's: {last}",
        last.len().saturating_sub(base.len())
    );
    assert!(
        ids.iter().all(|id| id.starts_with(base.as_str())),
        "every hop keeps the publisher's id as its base: {ids:?}"
    );
    assert_eq!(
        ids.iter().collect::<HashSet<_>>().len(),
        ids.len(),
        "every hop's id is new: {ids:?}"
    );

    let records = read_topic(&bootstrap, DEFER_TOPIC, DEFERS + 1).await;
    for (hop, copy) in records.iter().enumerate().skip(1) {
        assert_eq!(
            texts_of(&copy.headers, "Shove-Base-Message-Id"),
            [base.clone()],
            "defer copy {hop} carries one base id: {:?}",
            copy.headers
        );
        assert_eq!(
            texts_of(&copy.headers, "Shove-Message-Id").len(),
            1,
            "defer copy {hop} carries one message id: {:?}",
            copy.headers
        );
    }
}

/// A dead letter's `Shove-Death-Count` is one past the record's own and stays
/// at `u32::MAX` once there.
#[tokio::test]
async fn death_count_saturates() {
    let mock = Mock::start(&[DEATH_TOPIC, DEATH_DLQ]);
    let bootstrap = mock.bootstrap();
    produce(
        &bootstrap,
        DEATH_TOPIC,
        "dead",
        &[(b"Shove-Death-Count", Some(b"4294967295"))],
    )
    .await;

    let handler = Recorder::new(|_, _| Outcome::Reject);
    let consumer = Running::start::<DeathTopic>(&bootstrap, &handler).await;
    let dead = read_topic(&bootstrap, DEATH_DLQ, 1).await;
    consumer.stop().await;

    assert_eq!(dead[0].key, "dead");
    assert_eq!(
        values_of(&dead[0].headers, "Shove-Death-Count"),
        [Some(b"4294967295".to_vec())],
        "the dead letter's death count holds at u32::MAX: {:?}",
        dead[0].headers
    );
}

/// A dead letter carries every header of the original byte for byte and in
/// order, a key that is not UTF-8, a binary value, a null value and a
/// repeated key included, and one each of shove's own, rewritten. The DLQ
/// drain then hands it to `handle_dead` once, with only the headers whose
/// key and value are UTF-8.
#[tokio::test]
async fn dead_letter_carries_every_header_byte_for_byte() {
    let mock = Mock::start(&[DEAD_LETTER_TOPIC, DEAD_LETTER_DLQ]);
    let bootstrap = mock.bootstrap();
    let mut original: Vec<(&[u8], Option<&[u8]>)> = vec![(b"Shove-Message-Id", Some(b"m-1"))];
    original.extend_from_slice(&HOSTILE_HEADERS[..3]);
    original.push((b"Shove-Retry-Count", Some(b"0")));
    original.extend_from_slice(&HOSTILE_HEADERS[3..]);
    produce(&bootstrap, DEAD_LETTER_TOPIC, "rejected", &original).await;

    let handler = Recorder::new(|_, _| Outcome::Reject);
    let consumer = Running::start::<DeadLetterTopic>(&bootstrap, &handler).await;
    let dead = read_topic(&bootstrap, DEAD_LETTER_DLQ, 1).await;
    consumer.stop().await;

    let headers = &dead[0].headers;
    assert_eq!(
        foreign(headers),
        owned(&HOSTILE_HEADERS),
        "the dead letter carries the original's headers byte for byte and in order: {headers:?}"
    );
    for (key, value) in [
        ("Shove-Message-Id", &b"m-1-dlq1"[..]),
        ("Shove-Base-Message-Id", b"m-1"),
        ("Shove-Retry-Count", b"0"),
        ("Shove-Death-Count", b"1"),
        ("Shove-Death-Reason", b"rejected"),
        ("Shove-Original-Queue", DEAD_LETTER_TOPIC.as_bytes()),
    ] {
        assert_eq!(
            values_of(headers, key),
            [Some(value.to_vec())],
            "the dead letter carries one {key}: {headers:?}"
        );
    }

    let drain = Running::spawn(&bootstrap, |consumer, shutdown| {
        let handler = handler.clone();
        async move {
            consumer
                .run_dlq_with_options::<DeadLetterTopic, _>(
                    handler,
                    (),
                    ConsumerOptions::<Kafka>::new().with_shutdown(shutdown),
                )
                .await
        }
    })
    .await;
    drain
        .wait_until(
            || !handler.dead_letters().is_empty(),
            "the dead letter reaching the drain",
        )
        .await;
    tokio::time::sleep(SETTLE).await;
    drain.stop().await;

    let drained = handler.dead_letters();
    assert_eq!(
        drained.len(),
        1,
        "the drain hands the dead letter over once"
    );
    let (id, metadata) = &drained[0];
    assert_eq!(id, "rejected");
    assert_eq!(metadata.reason.as_deref(), Some("rejected"));
    assert_eq!(metadata.original_queue.as_deref(), Some(DEAD_LETTER_TOPIC));
    assert_eq!(metadata.death_count, 1);
    let dead_letter_id = values_of(headers, "Shove-Message-Id")
        .into_iter()
        .flatten()
        .map(|id| String::from_utf8(id).expect("a UTF-8 message id"))
        .collect::<Vec<_>>();
    assert_eq!(
        std::slice::from_ref(&metadata.message.delivery_id),
        dead_letter_id.as_slice(),
        "the drain reads the dead letter's own id"
    );
    assert_eq!(
        foreign_map(&metadata.message.headers),
        hostile_headers_as_strings(),
        "the drain's map holds only the headers whose key and value are UTF-8"
    );
}

/// A sequenced consumer hands a record whose header key is not UTF-8 to its
/// handler and dead-letters it on `Reject`; the `FailAll` cascade then
/// dead-letters the next record of the key without the handler. Both dead
/// letters carry every header of their record byte for byte and in order.
#[tokio::test]
async fn sequenced_consumer_dead_letters_hostile_headers_byte_for_byte() {
    let mock = Mock::start(&[SEQUENCED_TOPIC, SEQUENCED_DLQ]);
    let bootstrap = mock.bootstrap();
    for id in ["rejected", "poisoned"] {
        produce_record(
            &bootstrap,
            SEQUENCED_TOPIC,
            "acct-1",
            &order_payload(id),
            &HOSTILE_HEADERS,
        )
        .await;
    }

    let handler = Recorder::new(|_, _| Outcome::Reject);
    let consumer = Running::spawn(&bootstrap, |consumer, shutdown| {
        let handler = handler.clone();
        async move {
            consumer
                .run_fifo::<SequencedOrders, _>(
                    handler,
                    (),
                    ConsumerOptions::<Kafka>::new().with_shutdown(shutdown),
                )
                .await
        }
    })
    .await;
    let dead = read_topic(&bootstrap, SEQUENCED_DLQ, 2).await;
    tokio::time::sleep(SETTLE).await;
    consumer.stop().await;

    assert_eq!(
        handler.ids(),
        ["rejected"],
        "the poisoned key's next record never reaches the handler"
    );
    assert_eq!(
        foreign_map(&handler.deliveries()[0].metadata.headers),
        hostile_headers_as_strings(),
        "the handler's map holds only the headers whose key and value are UTF-8"
    );
    assert_eq!(
        dead.iter().map(Record::order_id).collect::<Vec<_>>(),
        ["rejected", "poisoned"],
        "each record is dead-lettered once, in order"
    );
    for record in &dead {
        assert_eq!(
            foreign(&record.headers),
            owned(&HOSTILE_HEADERS),
            "the dead letter of {} carries its headers byte for byte and in order",
            record.order_id()
        );
    }
}

/// A batch consumer dead-letters a rejected batch, a record that does not
/// decode and one past the size limit included, each dead letter carrying
/// every header of its record byte for byte and in order.
#[tokio::test]
async fn batch_consumer_dead_letters_hostile_headers_byte_for_byte() {
    const SIZE_LIMIT: usize = 256;
    let mock = Mock::start(&[BATCH_TOPIC, BATCH_DLQ]);
    let bootstrap = mock.bootstrap();
    let records: [(&str, Vec<u8>); 3] = [
        ("decoded", order_payload("decoded")),
        ("undecodable", b"not an order".to_vec()),
        ("oversize", vec![b'x'; SIZE_LIMIT + 1]),
    ];
    for (key, payload) in &records {
        produce_record(&bootstrap, BATCH_TOPIC, key, payload, &HOSTILE_HEADERS).await;
    }

    let batch_size = records.len();
    let handler = Recorder::new(|_, _| Outcome::Reject);
    let consumer = Running::spawn(&bootstrap, |consumer, shutdown| {
        let handler = handler.clone();
        async move {
            consumer
                .run_batch::<BatchTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new()
                        .with_shutdown(shutdown)
                        .with_max_batch_size(batch_size)
                        .with_max_batch_age(Duration::from_secs(1))
                        .with_max_message_size(SIZE_LIMIT),
                )
                .await
        }
    })
    .await;
    let dead = read_topic(&bootstrap, BATCH_DLQ, batch_size).await;
    tokio::time::sleep(SETTLE).await;
    consumer.stop().await;

    assert_eq!(
        handler.ids(),
        ["decoded"],
        "only the record that decodes reaches the handler"
    );
    assert_eq!(
        foreign_map(&handler.deliveries()[0].metadata.headers),
        hostile_headers_as_strings(),
        "the handler's map holds only the headers whose key and value are UTF-8"
    );
    let mut keys: Vec<&str> = dead.iter().map(|record| record.key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        ["decoded", "oversize", "undecodable"],
        "each record is dead-lettered once"
    );
    for record in &dead {
        assert_eq!(
            foreign(&record.headers),
            owned(&HOSTILE_HEADERS),
            "the dead letter of {} carries its headers byte for byte and in order",
            record.key
        );
    }
}

/// A broadcast subscriber hands a record whose header key is not UTF-8 to its
/// handler once, with only the headers whose key and value are UTF-8, and
/// goes on to the record behind it.
#[tokio::test]
async fn broadcast_subscriber_survives_a_non_utf8_header_key() {
    let mock = Mock::start(&[BROADCAST_TOPIC]);
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, BROADCAST_TOPIC, "hostile", &HOSTILE_HEADERS).await;

    let broker = Broker::<Kafka>::from_client(connect(&bootstrap).await);
    let handler = Recorder::new(|_, _| Outcome::Ack);
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<BroadcastTopic, _>(
            handler.clone(),
            ConsumerOptions::new().with_broadcast_start(BroadcastStart::Head),
        )
        .expect("subscribe from the head");
    wait_for(
        || handler.count() == 1,
        "the hostile record reaching the handler",
    )
    .await;
    produce(&bootstrap, BROADCAST_TOPIC, "after", &[]).await;
    wait_for(
        || handler.count() == 2,
        "the record behind it reaching the handler",
    )
    .await;
    tokio::time::sleep(SETTLE).await;
    subscriber.cancellation_token().cancel();
    let outcome = subscriber
        .run_until_timeout(std::future::pending(), DELIVERY_TIMEOUT)
        .await;
    assert!(
        outcome.is_clean(),
        "the subscriber stops clean: {outcome:?}"
    );
    broker.close().await;

    assert_eq!(
        handler.ids(),
        ["hostile", "after"],
        "each record is handled once"
    );
    assert_eq!(
        *handler.deliveries()[0].metadata.headers,
        hostile_headers_as_strings(),
        "the handler sees only the header whose key and value are UTF-8"
    );
}

/// Records whose ids look like retry copies of one record, and two records
/// that carry no id, retry into copies whose ids are their own: each copy's
/// id is its record's base and the attempt, the base carried beside it.
#[tokio::test]
async fn retry_copies_of_distinct_records_never_share_an_id() {
    let mock = Mock::start(&[RETRY_ID_TOPIC]);
    let bootstrap = mock.bootstrap();
    for id in ["inv-r7", "inv-r8"] {
        produce(
            &bootstrap,
            RETRY_ID_TOPIC,
            id,
            &[(b"Shove-Message-Id", Some(id.as_bytes()))],
        )
        .await;
    }
    for id in ["foreign-2", "foreign-3"] {
        produce(&bootstrap, RETRY_ID_TOPIC, id, &[]).await;
    }

    let handler = Recorder::new(|_, metadata| {
        if metadata.retry_count == 0 {
            Outcome::Retry
        } else {
            Outcome::Ack
        }
    });
    let consumer = Running::start::<RetryIdTopic>(&bootstrap, &handler).await;
    consumer
        .wait_until(
            || handler.count() == 8,
            "every retry copy reaching the handler",
        )
        .await;
    tokio::time::sleep(SETTLE).await;
    consumer.stop().await;

    let mut copies: Vec<(String, String, Option<String>)> = handler
        .deliveries()
        .into_iter()
        .filter(|d| d.metadata.retry_count == 1)
        .map(|d| {
            let base = d.metadata.headers.get("Shove-Base-Message-Id").cloned();
            (d.id, d.metadata.delivery_id, base)
        })
        .collect();
    copies.sort();
    let foreign_base = |offset: u8| format!("{RETRY_ID_TOPIC}-0-{offset}");
    assert_eq!(
        copies,
        [
            (
                "foreign-2".to_string(),
                format!("{}-r1", foreign_base(2)),
                Some(foreign_base(2)),
            ),
            (
                "foreign-3".to_string(),
                format!("{}-r1", foreign_base(3)),
                Some(foreign_base(3)),
            ),
            (
                "inv-r7".to_string(),
                "inv-r7-r1".to_string(),
                Some("inv-r7".to_string()),
            ),
            (
                "inv-r8".to_string(),
                "inv-r8-r1".to_string(),
                Some("inv-r8".to_string()),
            ),
        ],
        "each record's retry copy is named after its own base"
    );
}

/// Records whose ids look like dead letters of one record, and two records
/// that carry no id, dead-letter under ids of their own, and a dead letter
/// replayed with its headers dies again under a new one: each dead letter's
/// id is its record's base and its death count, the base carried beside it.
#[tokio::test]
async fn dead_letters_of_distinct_records_and_deaths_never_share_an_id() {
    let mock = Mock::start(&[DEAD_ID_TOPIC, DEAD_ID_DLQ]);
    let bootstrap = mock.bootstrap();
    for id in ["a", "a-dlq"] {
        produce(
            &bootstrap,
            DEAD_ID_TOPIC,
            id,
            &[(b"Shove-Message-Id", Some(id.as_bytes()))],
        )
        .await;
    }
    for id in ["foreign-2", "foreign-3"] {
        produce(&bootstrap, DEAD_ID_TOPIC, id, &[]).await;
    }

    let handler = Recorder::new(|_, _| Outcome::Reject);
    let consumer = Running::start::<DeadIdTopic>(&bootstrap, &handler).await;
    let first_deaths = read_topic(&bootstrap, DEAD_ID_DLQ, 4).await;
    let dead_a = first_deaths
        .iter()
        .find(|record| record.key == "a")
        .expect("the dead letter of a");
    let replayed_headers: Vec<(&[u8], Option<&[u8]>)> = dead_a
        .headers
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_deref()))
        .collect();
    produce_record(
        &bootstrap,
        DEAD_ID_TOPIC,
        "a-replayed",
        &dead_a.payload,
        &replayed_headers,
    )
    .await;
    let dead = read_topic(&bootstrap, DEAD_ID_DLQ, 5).await;
    consumer.stop().await;

    let mut ids: Vec<(String, Vec<String>)> = dead
        .iter()
        .map(|record| {
            (
                record.key.clone(),
                texts_of(&record.headers, "Shove-Message-Id"),
            )
        })
        .collect();
    ids.sort();
    let foreign_base = |offset: u8| format!("{DEAD_ID_TOPIC}-0-{offset}");
    assert_eq!(
        ids,
        [
            ("a".to_string(), vec!["a-dlq1".to_string()]),
            ("a-dlq".to_string(), vec!["a-dlq-dlq1".to_string()]),
            ("a-replayed".to_string(), vec!["a-dlq2".to_string()]),
            (
                "foreign-2".to_string(),
                vec![format!("{}-dlq1", foreign_base(2))]
            ),
            (
                "foreign-3".to_string(),
                vec![format!("{}-dlq1", foreign_base(3))]
            ),
        ],
        "each dead letter is named after its own base and death"
    );
    let mut bases: Vec<(String, Vec<String>)> = dead
        .iter()
        .map(|record| {
            (
                record.key.clone(),
                texts_of(&record.headers, "Shove-Base-Message-Id"),
            )
        })
        .collect();
    bases.sort();
    assert_eq!(
        bases,
        [
            ("a".to_string(), vec!["a".to_string()]),
            ("a-dlq".to_string(), vec!["a-dlq".to_string()]),
            ("a-replayed".to_string(), vec!["a".to_string()]),
            ("foreign-2".to_string(), vec![foreign_base(2)]),
            ("foreign-3".to_string(), vec![foreign_base(3)]),
        ],
        "each dead letter carries its base once"
    );
}
