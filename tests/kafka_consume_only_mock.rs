//! A client that only consumes builds no producer, proved against
//! librdkafka's mock cluster (`rdkafka::mocking`), which needs no Docker.
//!
//! librdkafka sends `InitProducerId` 500 ms after it creates an idempotent
//! producer, so every zero assertion waits past that timer. The tests count
//! the requests the mock broker receives by API key through
//! `rdkafka::bindings`, as `kafka_commit_policy_mock.rs` does, because
//! `rdkafka::mocking` wraps no request tracking. The topic is bound with
//! `external()` and created through the mock API, and the record is produced
//! by a raw rdkafka producer with idempotence off.

#![cfg(feature = "kafka")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::bindings::{
    rd_kafka_handle_mock_cluster, rd_kafka_mock_get_requests, rd_kafka_mock_request_api_key,
    rd_kafka_mock_request_destroy_array, rd_kafka_mock_start_request_tracking,
};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{
    BaseProducer, DefaultProducerContext, FutureProducer, FutureRecord, Producer,
};
use rdkafka::types::{RDKafkaApiKey, RDKafkaMockCluster};
use serde::{Deserialize, Serialize};
use shove::ShoveError;
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::kafka::{KafkaClient, KafkaConfig, KafkaConsumer};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const TOPIC: &str = "kafka-consume-only-mock";
/// A record reaches the handler on a mock cluster in well under this.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// Past librdkafka's 500 ms producer-id timer, with room for a slow machine.
const PRODUCER_ID_TIMER_MARGIN: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Order {
    id: String,
}

shove::define_topic!(
    OrdersTopic,
    Order,
    TopologyBuilder::new(TOPIC).external().build()
);

/// Acknowledges every record and records its id.
#[derive(Clone, Default)]
struct Acking {
    seen: Arc<Mutex<Vec<String>>>,
}

impl Acking {
    fn ids(&self) -> Vec<String> {
        self.seen.lock().expect("handler mutex poisoned").clone()
    }
}

impl MessageHandler<OrdersTopic> for Acking {
    type Context = ();
    async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .push(msg.id);
        Outcome::Ack
    }
}

/// One mock broker with the topic created. The raw cluster handle comes from
/// the producer that owns the cluster, beside the safe wrapper the same
/// client hands out; the producer produces nothing.
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

    fn producer_id_requests(&self) -> usize {
        self.requests_of(RDKafkaApiKey::InitProducerId)
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

/// Produces `id` through a raw rdkafka producer with idempotence off, so
/// the only `InitProducerId` the broker can see is shove's.
async fn produce(bootstrap: &str, id: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("enable.idempotence", "false")
        .create()
        .expect("mock producer");
    let payload = serde_json::to_vec(&Order { id: id.into() }).expect("encode the record");
    producer
        .send(
            FutureRecord::to(TOPIC).key(id).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .expect("produce to the mock cluster");
}

/// A client that connects, consumes a record and answers a health check
/// sends no `InitProducerId`, so it has no producer. The probe's metadata
/// request is the control that the tracking sees this client, and the first
/// publish, which builds the producer, is the control that it counts
/// `InitProducerId` at all.
#[tokio::test]
async fn a_client_that_only_consumes_sends_no_init_producer_id() {
    let mock = Mock::start();
    mock.track_requests();
    let bootstrap = mock.bootstrap();

    let client = connect(&bootstrap).await;
    let broker = Broker::<Kafka>::from_client(client.clone());
    let handler = Acking::default();
    let shutdown = CancellationToken::new();
    let options = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone());
    let run = tokio::spawn({
        let handler = handler.clone();
        async move {
            KafkaConsumer::new(client)
                .run::<OrdersTopic, _>(handler, (), options)
                .await
        }
    });

    produce(&bootstrap, "1").await;
    wait_until(
        || handler.ids() == ["1"],
        DELIVERY_TIMEOUT,
        "the record reaching the handler",
    )
    .await;
    let metadata_before = mock.requests_of(RDKafkaApiKey::Metadata);
    broker
        .ping()
        .await
        .expect("a health check on the consuming client");
    let probed_at = Instant::now();
    assert!(
        mock.requests_of(RDKafkaApiKey::Metadata) > metadata_before,
        "the tracking saw the probe's metadata request"
    );
    tokio::time::sleep_until(probed_at + PRODUCER_ID_TIMER_MARGIN).await;
    assert_eq!(
        mock.producer_id_requests(),
        0,
        "a client that only consumed and probed requested a producer id"
    );

    broker
        .publisher()
        .await
        .expect("a publisher on the client")
        .publish::<OrdersTopic>(&Order { id: "2".into() })
        .await
        .expect("publish through shove's producer");
    assert!(
        mock.producer_id_requests() >= 1,
        "the first publish built the idempotent producer"
    );
    wait_until(
        || handler.ids() == ["1", "2"],
        DELIVERY_TIMEOUT,
        "the published record reaching the handler",
    )
    .await;

    shutdown.cancel();
    run.await
        .expect("consumer task joins")
        .expect("consumer stops clean");
    broker.close().await;
}

/// A client closed before any publish has no producer to flush, and a
/// publish after the close is refused instead of building one.
#[tokio::test]
async fn a_client_closed_before_any_publish_builds_no_producer() {
    let mock = Mock::start();
    mock.track_requests();
    let bootstrap = mock.bootstrap();

    let broker = Broker::<Kafka>::from_client(connect(&bootstrap).await);
    broker
        .ping()
        .await
        .expect("a health check before the close");
    broker.close().await;

    let err = broker
        .publisher()
        .await
        .expect("a publisher handle needs no producer")
        .publish::<OrdersTopic>(&Order { id: "1".into() })
        .await
        .expect_err("a publish after the close is refused");
    let refused_at = Instant::now();
    assert!(
        matches!(err, ShoveError::Connection(_)),
        "expected ShoveError::Connection, got {err:?}"
    );
    tokio::time::sleep_until(refused_at + PRODUCER_ID_TIMER_MARGIN).await;
    assert_eq!(
        mock.producer_id_requests(),
        0,
        "a client closed before any publish requested a producer id"
    );
}

shove::define_topic!(
    MissingTopic,
    Order,
    TopologyBuilder::new("kafka-consume-only-missing")
        .external()
        .build()
);

impl MessageHandler<MissingTopic> for Acking {
    type Context = ();
    async fn handle(&self, msg: Order, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen
            .lock()
            .expect("handler mutex poisoned")
            .push(msg.id);
        Outcome::Ack
    }
}

/// The startup topic check is a metadata probe: a consumer that refuses a
/// missing external topic, and one that probes a present one before it runs,
/// request no producer id.
#[tokio::test]
async fn the_startup_topic_check_builds_no_producer() {
    let mock = Mock::start();
    mock.track_requests();
    let bootstrap = mock.bootstrap();

    let err = tokio::time::timeout(
        DELIVERY_TIMEOUT,
        KafkaConsumer::new(connect(&bootstrap).await).run::<MissingTopic, _>(
            Acking::default(),
            (),
            ConsumerOptions::<Kafka>::new(),
        ),
    )
    .await
    .expect("the refusal comes at startup, not after a retry loop")
    .expect_err("a topic nobody provisioned is refused at startup");
    assert!(matches!(err, ShoveError::Topology(_)), "{err:?}");

    let shutdown = CancellationToken::new();
    let options = ConsumerOptions::<Kafka>::new().with_shutdown(shutdown.clone());
    let client = connect(&bootstrap).await;
    let run = tokio::spawn(async move {
        KafkaConsumer::new(client)
            .run::<OrdersTopic, _>(Acking::default(), (), options)
            .await
    });
    let probed_at = Instant::now();
    tokio::time::sleep_until(probed_at + PRODUCER_ID_TIMER_MARGIN).await;
    shutdown.cancel();
    run.await
        .expect("consumer task joins")
        .expect("consumer stops clean");
    assert_eq!(
        mock.producer_id_requests(),
        0,
        "the startup topic check requested a producer id"
    );
}
