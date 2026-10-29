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
use shove::ShoveError;
use shove::Topic;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
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

const HOSTILE_TOPIC: &str = "kafka-headers-hostile-key";
const RETRY_TOPIC: &str = "kafka-headers-retry-copy";
const DEFER_TOPIC: &str = "kafka-headers-defer-id";
const DEATH_TOPIC: &str = "kafka-headers-death-count";
const DEATH_DLQ: &str = "kafka-headers-death-count-dlq";
const DEAD_LETTER_TOPIC: &str = "kafka-headers-dead-letter";
const DEAD_LETTER_DLQ: &str = "kafka-headers-dead-letter-dlq";

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

/// One delivery as the handler saw it.
#[derive(Clone)]
struct Delivery {
    id: String,
    metadata: MessageMetadata,
}

/// Records every delivery and answers it with `decide`, given the number of
/// deliveries before it and the delivery's metadata.
#[derive(Clone)]
struct Recorder {
    seen: Arc<Mutex<Vec<Delivery>>>,
    decide: fn(usize, &MessageMetadata) -> Outcome,
}

impl Recorder {
    fn new(decide: fn(usize, &MessageMetadata) -> Outcome) -> Self {
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

/// A shove consumer of one topic, running on its own task.
struct Running {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), ShoveError>>,
}

impl Running {
    async fn start<T>(bootstrap: &str, handler: &Recorder) -> Self
    where
        T: Topic<Message = Order>,
    {
        let client = connect(bootstrap).await;
        let shutdown = CancellationToken::new();
        let options = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone());
        let handler = handler.clone();
        let task = tokio::spawn(async move {
            KafkaConsumer::new(client)
                .run::<T, _>(handler, (), options)
                .await
        });
        Self { shutdown, task }
    }

    /// Waits for `done`, failing at once if the consumer ends first.
    async fn wait_until(&self, done: impl Fn() -> bool, what: &str) {
        let deadline = Instant::now() + DELIVERY_TIMEOUT;
        while !done() {
            assert!(!self.task.is_finished(), "the consumer ended before {what}");
            assert!(
                Instant::now() < deadline,
                "{what} did not happen within {DELIVERY_TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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

/// Produces order `id` to `topic` carrying exactly `headers`, in order.
async fn produce(bootstrap: &str, topic: &str, id: &str, headers: &[(&[u8], Option<&[u8]>)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("enable.idempotence", "false")
        .create()
        .expect("mock producer");
    let payload = serde_json::to_vec(&Order { id: id.into() }).expect("encode the record");
    producer
        .send(
            FutureRecord::to(topic)
                .key(id)
                .payload(&payload)
                .headers(wire(headers)),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
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

/// The first `count` records of `topic`, each as its order id and its
/// headers, read by a raw consumer from the start of partition 0.
async fn read_topic(bootstrap: &str, topic: &str, count: usize) -> Vec<(String, Vec<WireHeader>)> {
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
        let order: Order =
            serde_json::from_slice(msg.payload().unwrap_or_default()).expect("an order payload");
        records.push((order.id, headers_of(&msg)));
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

/// The value of every `key` header in `headers`, in order.
fn values_of(headers: &[WireHeader], key: &str) -> Vec<Option<Vec<u8>>> {
    headers
        .iter()
        .filter(|(k, _)| k == key.as_bytes())
        .map(|(_, value)| value.clone())
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
    let (id, copy) = &records[1];
    assert_eq!(id, "retried", "the second record is the retry copy");
    assert_eq!(
        foreign(copy),
        owned(&original),
        "the copy carries the original's headers byte for byte and in order: {copy:?}"
    );
    assert_eq!(
        values_of(copy, "Shove-Retry-Count"),
        [Some(b"1".to_vec())],
        "the copy carries one retry count, the incremented one"
    );
    assert_eq!(
        values_of(copy, "Shove-Message-Id").len(),
        1,
        "the copy carries one message id: {copy:?}"
    );
}

/// Each `Defer` republishes a copy whose `Shove-Message-Id` replaces the
/// previous hop's suffix instead of adding to it, so twenty defers leave the
/// publisher's id and one suffix, and every hop's id is new.
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

    let ids: Vec<String> = handler
        .deliveries()
        .into_iter()
        .map(|d| d.metadata.delivery_id)
        .collect();
    assert_eq!(ids.len(), DEFERS + 1, "the acked copy is delivered once");
    let base = &ids[0];
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

    assert_eq!(dead[0].0, "dead");
    assert_eq!(
        values_of(&dead[0].1, "Shove-Death-Count"),
        [Some(b"4294967295".to_vec())],
        "the dead letter's death count holds at u32::MAX: {:?}",
        dead[0].1
    );
}

/// A dead letter carries every header of the original byte for byte and in
/// order, a key that is not UTF-8, a binary value, a null value and a
/// repeated key included, and one each of shove's own, rewritten.
#[tokio::test]
async fn dead_letter_carries_every_header_byte_for_byte() {
    let mock = Mock::start(&[DEAD_LETTER_TOPIC, DEAD_LETTER_DLQ]);
    let bootstrap = mock.bootstrap();
    let foreign_headers: [(&[u8], Option<&[u8]>); 5] = [
        (HOSTILE_KEY, Some(b"k")),
        (b"bin", Some(BINARY_VALUE)),
        (b"null", None),
        (b"dup", Some(b"first")),
        (b"dup", Some(b"second")),
    ];
    let mut original: Vec<(&[u8], Option<&[u8]>)> = vec![(b"Shove-Message-Id", Some(b"m-1"))];
    original.extend_from_slice(&foreign_headers[..3]);
    original.push((b"Shove-Retry-Count", Some(b"0")));
    original.extend_from_slice(&foreign_headers[3..]);
    produce(&bootstrap, DEAD_LETTER_TOPIC, "rejected", &original).await;

    let handler = Recorder::new(|_, _| Outcome::Reject);
    let consumer = Running::start::<DeadLetterTopic>(&bootstrap, &handler).await;
    let dead = read_topic(&bootstrap, DEAD_LETTER_DLQ, 1).await;
    consumer.stop().await;

    let headers = &dead[0].1;
    assert_eq!(
        foreign(headers),
        owned(&foreign_headers),
        "the dead letter carries the original's headers byte for byte and in order: {headers:?}"
    );
    for (key, value) in [
        ("Shove-Message-Id", &b"m-1-dlq"[..]),
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
}
