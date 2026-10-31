//! Every Kafka consumer shove builds is created with the fetch queue
//! settings its options carry, proved against librdkafka's mock cluster
//! (`rdkafka::mocking`), which serves the consumer group protocol and fetch
//! requests from a localhost listener and needs no Docker.
//!
//! Each test runs one kind of consumer to the point where it has handled a
//! record, which proves librdkafka took its configuration, and then reads
//! what `fetch_queue_probe` recorded for the consumers the test created: the
//! `fetch.queue.backoff.ms`, `queued.min.messages` and
//! `queued.max.messages.kbytes` each one was handed. Without a setting the
//! backoff is the default of 25 ms and the two thresholds are left to
//! librdkafka. The standard consumer takes overrides through the options and
//! through the group config. The sequenced consumer, the DLQ drain and the
//! broadcast subscription take them through the options. The batch consumer
//! has no setting and runs on the default.
//!
//! The topics are created through the mock API, because the mock broker has
//! no CreateTopics API, and the ones a registry declares are bound with
//! `external()` for the same reason. Records are produced through a raw
//! rdkafka producer with idempotence off.
//!
//! `test-support` gates the probe this file reads. The Kafka coverage row
//! enables it and runs this suite; the schema-registry row compiles the
//! binary too, but its test filter does not select it.

#![cfg(all(feature = "kafka", feature = "test-support"))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{
    BaseProducer, DefaultProducerContext, FutureProducer, FutureRecord, Producer,
};
use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::consumer_group::ConsumerGroupConfig;
use shove::handler::{BatchMessageHandler, MessageHandler};
use shove::kafka::{
    BatchConsumerOptions, KafkaClient, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig,
    fetch_queue_probe,
};
use shove::markers::Kafka;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::topology::{SequenceFailure, TopologyBuilder};
use shove::{SequencedTopic, ShoveError};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// A record reaches the handler on a mock cluster in well under this.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a stop may take before the test gives up on it.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

const DIRECT_TOPIC: &str = "kafka-fetch-queue-mock-direct";
const SEQUENCED_TOPIC: &str = "kafka-fetch-queue-mock-sequenced";
const SEQUENCED_DLQ: &str = "kafka-fetch-queue-mock-sequenced-dlq";
const DRAIN_TOPIC: &str = "kafka-fetch-queue-mock-drain";
const DRAIN_DLQ: &str = "kafka-fetch-queue-mock-drain-dlq";
const BROADCAST_TOPIC: &str = "kafka-fetch-queue-mock-broadcast";
const BATCH_TOPIC: &str = "kafka-fetch-queue-mock-batch";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
}

shove::define_topic!(
    DirectOrders,
    Order,
    TopologyBuilder::new(DIRECT_TOPIC).external().build()
);

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

shove::define_topic!(
    DrainOrders,
    Order,
    TopologyBuilder::new(DRAIN_TOPIC).dlq().build()
);

shove::define_topic!(
    BroadcastOrders,
    Order,
    TopologyBuilder::new(BROADCAST_TOPIC).broadcast().build()
);

shove::define_topic!(
    BatchOrders,
    Order,
    TopologyBuilder::new(BATCH_TOPIC).build()
);

/// Acknowledges every record, dead letter and batch, and records the ids.
#[derive(Clone, Default)]
struct Acking {
    seen: Arc<Mutex<Vec<String>>>,
}

impl Acking {
    fn record(&self, id: String) {
        self.seen.lock().expect("handler mutex poisoned").push(id);
    }

    fn ids(&self) -> Vec<String> {
        self.seen.lock().expect("handler mutex poisoned").clone()
    }

    fn has_seen_any(&self) -> bool {
        !self.ids().is_empty()
    }
}

impl<T: Topic<Message = Order>> MessageHandler<T> for Acking {
    type Context = ();

    async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.record(msg.id);
        Outcome::Ack
    }

    async fn handle_dead(&self, msg: Order, _meta: DeadMessageMetadata, _: &()) {
        self.record(msg.id);
    }
}

impl<T: Topic<Message = Order>> BatchMessageHandler<T> for Acking {
    type Context = ();

    async fn handle_batch(&self, messages: Vec<(Order, MessageMetadata)>, _: &()) -> Outcome {
        for (order, _) in messages {
            self.record(order.id);
        }
        Outcome::Ack
    }
}

/// One mock broker. The cluster is the one a producer configured with
/// `test.mock.num.brokers` owns; the producer produces nothing and lives for
/// the cluster.
struct Mock {
    owner: BaseProducer,
}

impl Mock {
    fn start() -> Self {
        let owner: BaseProducer = ClientConfig::new()
            .set("test.mock.num.brokers", "1")
            .create()
            .expect("a producer that owns a mock cluster");
        Self { owner }
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

    fn with_topics(self, topics: &[&str]) -> Self {
        for topic in topics {
            self.api()
                .create_topic(topic, 1, 1)
                .expect("create the topic through the mock API");
        }
        self
    }
}

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

/// Produces record `id`, keyed by itself, through a raw rdkafka producer with
/// idempotence off.
async fn produce(bootstrap: &str, topic: &str, id: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("enable.idempotence", "false")
        .create()
        .expect("mock producer");
    let payload = serde_json::to_vec(&Order { id: id.into() }).expect("encode the record");
    producer
        .send(
            FutureRecord::to(topic).key(id).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
}

/// The three settings every consumer created so far was handed, as strings
/// as librdkafka was given them.
type Created = (Option<String>, Option<String>, Option<String>);

fn created_consumers() -> Vec<Created> {
    fetch_queue_probe::created()
        .into_iter()
        .map(|c| {
            (
                c.fetch_queue_backoff_ms,
                c.queued_min_messages,
                c.queued_max_messages_kbytes,
            )
        })
        .collect()
}

/// Asserts that at least one consumer was created and that each one was
/// handed exactly these settings.
fn assert_every_consumer_created_with(backoff: &str, min: Option<&str>, kbytes: Option<&str>) {
    let created = created_consumers();
    assert!(!created.is_empty(), "a consumer was created");
    let expected = (
        Some(backoff.to_owned()),
        min.map(str::to_owned),
        kbytes.map(str::to_owned),
    );
    for one in &created {
        assert_eq!(
            one, &expected,
            "(fetch.queue.backoff.ms, queued.min.messages, queued.max.messages.kbytes) of {created:?}"
        );
    }
}

/// Settings that differ from the default and from librdkafka's own.
fn tuned_options() -> ConsumerOptions<Kafka> {
    ConsumerOptions::<Kafka>::new()
        .with_queued_min_messages(5_000)
        .with_queued_max_messages_kbytes(2_048)
        .with_fetch_queue_backoff_ms(250)
}

fn tuned_group_config() -> KafkaConsumerGroupConfig {
    KafkaConsumerGroupConfig::new(1..=1)
        .with_queued_min_messages(5_000)
        .with_queued_max_messages_kbytes(2_048)
        .with_fetch_queue_backoff_ms(250)
}

fn assert_every_consumer_created_with_the_overrides() {
    assert_every_consumer_created_with("250", Some("5000"), Some("2048"));
}

type Run = JoinHandle<Result<(), ShoveError>>;

async fn stop(run: Run, shutdown: &CancellationToken) {
    shutdown.cancel();
    tokio::time::timeout(STOP_TIMEOUT, run)
        .await
        .expect("the consumer stops on its token")
        .expect("the run task completes")
        .expect("a clean stop");
}

/// Starts a standard consumer of the direct topic on `options`, produces a
/// record, waits for the handler to see it and stops the consumer.
async fn consume_one_directly(options: ConsumerOptions<Kafka>) {
    let mock = Mock::start().with_topics(&[DIRECT_TOPIC]);
    let bootstrap = mock.bootstrap();
    let handler = Acking::default();
    let shutdown = CancellationToken::new();
    let options = options.with_shutdown(shutdown.clone());
    let client = connect(&bootstrap).await;
    let run: Run = tokio::spawn({
        let handler = handler.clone();
        async move {
            KafkaConsumer::new(client)
                .run::<DirectOrders, _>(handler, (), options)
                .await
        }
    });

    produce(&bootstrap, DIRECT_TOPIC, "o1").await;
    wait_until(
        || handler.ids() == ["o1"],
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;

    stop(run, &shutdown).await;
}

#[tokio::test]
async fn a_consumer_with_no_fetch_queue_settings_consumes_on_the_default_backoff() {
    consume_one_directly(ConsumerOptions::<Kafka>::new()).await;

    assert_every_consumer_created_with("25", None, None);
}

#[tokio::test]
async fn a_consumer_with_every_fetch_queue_setting_consumes_on_them() {
    consume_one_directly(tuned_options()).await;

    assert_every_consumer_created_with_the_overrides();
}

#[tokio::test]
async fn a_group_member_consumes_on_the_group_configs_fetch_queue_settings() {
    let mock = Mock::start().with_topics(&[DIRECT_TOPIC]);
    let bootstrap = mock.bootstrap();
    let handler = Acking::default();
    let broker = Broker::<Kafka>::from_client(connect(&bootstrap).await);
    let mut group = broker.consumer_group();
    group
        .register::<DirectOrders, _>(ConsumerGroupConfig::new(tuned_group_config()), {
            let handler = handler.clone();
            move || handler.clone()
        })
        .await
        .expect("register on the mock cluster");
    let token = group.cancellation_token();
    let run = tokio::spawn(group.run_until_timeout(token.clone().cancelled_owned(), STOP_TIMEOUT));

    produce(&bootstrap, DIRECT_TOPIC, "o1").await;
    wait_until(
        || handler.ids() == ["o1"],
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;
    token.cancel();
    let outcome = run.await.expect("the group run completes");

    assert_eq!(
        (outcome.errors, outcome.panics, outcome.timed_out),
        (0, 0, false),
        "{outcome:?}"
    );
    assert_every_consumer_created_with_the_overrides();
}

#[tokio::test]
async fn a_sequenced_consumer_consumes_on_the_options_fetch_queue_settings() {
    let mock = Mock::start().with_topics(&[SEQUENCED_TOPIC, SEQUENCED_DLQ]);
    let bootstrap = mock.bootstrap();
    let handler = Acking::default();
    let shutdown = CancellationToken::new();
    let options = tuned_options().with_shutdown(shutdown.clone());
    let client = connect(&bootstrap).await;
    let run: Run = tokio::spawn({
        let handler = handler.clone();
        async move {
            KafkaConsumer::new(client)
                .run_fifo::<SequencedOrders, _>(handler, (), options)
                .await
        }
    });

    produce(&bootstrap, SEQUENCED_TOPIC, "o1").await;
    wait_until(
        || handler.ids() == ["o1"],
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;

    stop(run, &shutdown).await;
    assert_every_consumer_created_with_the_overrides();
}

#[tokio::test]
async fn the_dlq_drain_consumes_on_the_options_fetch_queue_settings() {
    let mock = Mock::start().with_topics(&[DRAIN_DLQ]);
    let bootstrap = mock.bootstrap();
    produce(&bootstrap, DRAIN_DLQ, "dead-1").await;
    let handler = Acking::default();
    let shutdown = CancellationToken::new();
    let options = tuned_options().with_shutdown(shutdown.clone());
    let client = connect(&bootstrap).await;
    let run: Run = tokio::spawn({
        let handler = handler.clone();
        async move {
            KafkaConsumer::new(client)
                .run_dlq_with_options::<DrainOrders, _>(handler, (), options)
                .await
        }
    });

    wait_until(
        || handler.ids() == ["dead-1"],
        DELIVERY_TIMEOUT,
        "the dead letter reaching the handler",
    )
    .await;

    stop(run, &shutdown).await;
    assert_every_consumer_created_with_the_overrides();
}

#[tokio::test]
async fn a_broadcast_subscription_consumes_on_the_options_fetch_queue_settings() {
    let mock = Mock::start().with_topics(&[BROADCAST_TOPIC]);
    let bootstrap = mock.bootstrap();
    let handler = Acking::default();
    let broker = Broker::<Kafka>::from_client(connect(&bootstrap).await);
    let mut subscriber = broker.broadcast_subscriber();
    subscriber
        .subscribe::<BroadcastOrders, _>(handler.clone(), tuned_options())
        .expect("subscribe on the mock cluster");
    let stop = CancellationToken::new();
    let run =
        tokio::spawn(subscriber.run_until_timeout(stop.clone().cancelled_owned(), STOP_TIMEOUT));

    // A subscription starts at the tail, so a record produced before its
    // partitions are assigned is skipped: keep producing until one arrives.
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while !handler.has_seen_any() {
        assert!(
            Instant::now() < deadline,
            "no record reached the subscription within {DELIVERY_TIMEOUT:?}"
        );
        produce(&bootstrap, BROADCAST_TOPIC, "b1").await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    stop.cancel();
    let outcome = run.await.expect("the subscriber run completes");

    assert_eq!(
        (outcome.errors, outcome.panics, outcome.timed_out),
        (0, 0, false),
        "{outcome:?}"
    );
    assert_every_consumer_created_with_the_overrides();
}

#[tokio::test]
async fn a_batch_consumer_consumes_on_the_default_backoff() {
    let mock = Mock::start().with_topics(&[BATCH_TOPIC]);
    let bootstrap = mock.bootstrap();
    let handler = Acking::default();
    let shutdown = CancellationToken::new();
    let options = BatchConsumerOptions::new()
        .with_max_batch_age(Duration::from_millis(200))
        .with_shutdown(shutdown.clone());
    let client = connect(&bootstrap).await;
    let run: Run = tokio::spawn({
        let handler = handler.clone();
        async move {
            KafkaConsumer::new(client)
                .run_batch::<BatchOrders, _>(handler, (), options)
                .await
        }
    });

    produce(&bootstrap, BATCH_TOPIC, "o1").await;
    wait_until(
        || handler.ids() == ["o1"],
        DELIVERY_TIMEOUT,
        "the record reaching the batch handler",
    )
    .await;

    stop(run, &shutdown).await;
    assert_every_consumer_created_with("25", None, None);
}

/// A setting outside librdkafka's range is refused with a typed error before
/// the consumer is created, and a group config with one is refused at
/// registration, before the registry declares or spawns anything. The topic
/// does not exist on the mock cluster, so a refusal that came after the broker
/// was asked for the topic would read as a missing topic.
#[tokio::test]
async fn an_out_of_range_setting_is_refused_before_a_consumer_is_created() {
    let mock = Mock::start();
    let bootstrap = mock.bootstrap();

    let err = tokio::time::timeout(
        DELIVERY_TIMEOUT,
        KafkaConsumer::new(connect(&bootstrap).await).run::<DirectOrders, _>(
            Acking::default(),
            (),
            ConsumerOptions::<Kafka>::new().with_fetch_queue_backoff_ms(300_001),
        ),
    )
    .await
    .expect("the refusal comes at startup, not after a retry loop")
    .expect_err("an out-of-range backoff is refused");
    assert!(
        matches!(&err, ShoveError::Validation(msg)
            if msg.contains("with_fetch_queue_backoff_ms") && msg.contains("0..=300000")),
        "{err:?}"
    );

    let mut group = Broker::<Kafka>::from_client(connect(&bootstrap).await).consumer_group();
    let err = group
        .register::<DirectOrders, _>(
            ConsumerGroupConfig::new(
                KafkaConsumerGroupConfig::new(1..=1).with_queued_min_messages(0),
            ),
            Acking::default,
        )
        .await
        .expect_err("an out-of-range threshold is refused at registration");
    assert!(
        matches!(&err, ShoveError::Validation(msg)
            if msg.contains("with_queued_min_messages") && msg.contains("1..=10000000")),
        "{err:?}"
    );

    assert!(
        created_consumers().is_empty(),
        "no consumer was created: {:?}",
        created_consumers()
    );
}
