#![cfg(feature = "kafka")]

//! Integration tests for `Broker<Kafka>::ping`.

use std::time::Duration;

#[cfg(feature = "test-support")]
use rdkafka::ClientConfig;
#[cfg(feature = "test-support")]
use rdkafka::consumer::{BaseConsumer, Consumer as _};
#[cfg(feature = "test-support")]
use rdkafka::mocking::MockCluster;
#[cfg(feature = "test-support")]
use rdkafka::types::RDKafkaRespErr;
#[cfg(feature = "test-support")]
use shove::kafka::ping_probe_topic_for_test;
use shove::kafka::{KafkaClient, KafkaConfig};
use shove::{Broker, Kafka, ShoveError};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};

async fn start_broker() -> (
    testcontainers::ContainerAsync<KafkaContainer>,
    Broker<Kafka>,
) {
    let container = KafkaContainer::default()
        .start()
        .await
        .expect("start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("get Kafka port");
    let bootstrap = format!("127.0.0.1:{port}");
    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap), 10)
        .await
        .expect("connect to Kafka");
    (container, Broker::<Kafka>::from_client(client))
}

#[tokio::test]
async fn ping_succeeds_against_running_broker() {
    let (_container, broker) = start_broker().await;
    broker.ping().await.expect("ping should succeed");
}

/// A probe is repeated for the life of the process, so it must not ask the
/// cluster to describe every topic it hosts. Against a mock cluster with
/// three topics, the answer to a ping lists none of them, and at most the one
/// topic the probe asks about.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn ping_does_not_fetch_all_topics() {
    const TOPICS: [&str; 3] = ["ping-orders", "ping-invoices", "ping-shipments"];
    let mock = MockCluster::new(1).expect("mock cluster");
    for topic in TOPICS {
        mock.create_topic(topic, 1, 1).expect("mock topic");
    }
    let client = KafkaClient::connect(&KafkaConfig::new(mock.bootstrap_servers()))
        .await
        .expect("connect to the mock cluster");

    let listed = client
        .ping_topics_for_test(Duration::from_secs(5))
        .await
        .expect("the mock broker answers a ping");

    assert!(
        listed.len() <= 1
            && listed
                .iter()
                .all(|(topic, _)| !TOPICS.contains(&topic.as_str())),
        "the ping asked for the cluster's topics and was told about {listed:?}"
    );
}

/// A probe repeated for the life of the process must leave nothing behind. A
/// mock cluster creates any topic a metadata request allows it to create, so
/// a ping that allowed creation would find its probe topic created, and the
/// answer to it, and the cluster's topic list after it, would show that.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn ping_never_creates_its_probe_topic() {
    let mock = MockCluster::new(1).expect("mock cluster");
    mock.create_topic("ping-orders", 1, 1).expect("mock topic");
    let client = KafkaClient::connect(&KafkaConfig::new(mock.bootstrap_servers()))
        .await
        .expect("connect to the mock cluster");
    let probe = ping_probe_topic_for_test();

    let listed = client
        .ping_topics_for_test(Duration::from_secs(5))
        .await
        .expect("the mock broker answers a ping");

    let (_, error) = listed
        .iter()
        .find(|(topic, _)| topic == probe)
        .expect("the answer names the probe topic");
    assert_eq!(
        *error,
        Some(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART),
        "the broker did not report the probe topic as unknown, so the ping may have created it"
    );
    let observer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", mock.bootstrap_servers())
        .set("allow.auto.create.topics", "false")
        .create()
        .expect("an observer client");
    let cluster = observer
        .fetch_metadata(None, Duration::from_secs(5))
        .expect("the observer lists the cluster's topics");
    assert!(
        cluster.topics().iter().all(|topic| topic.name() != probe),
        "the cluster has the probe topic after a ping: {:?}",
        cluster.topics()
    );
}

/// Any answer from a broker shows it is alive, so a broker that refuses the
/// principal `Describe` on the probe topic still gets a successful ping.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn ping_succeeds_when_the_broker_refuses_the_probe_topic() {
    let mock = MockCluster::new(1).expect("mock cluster");
    let probe = ping_probe_topic_for_test();
    mock.topic_error(
        probe,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
    )
    .expect("refuse the probe topic");
    let client = KafkaClient::connect(&KafkaConfig::new(mock.bootstrap_servers()))
        .await
        .expect("connect to the mock cluster");

    Broker::<Kafka>::from_client(client.clone())
        .ping()
        .await
        .expect("a refusal for the probe topic is still an answer");

    let listed = client
        .ping_topics_for_test(Duration::from_secs(5))
        .await
        .expect("the mock broker answers a ping");
    let (_, error) = listed
        .iter()
        .find(|(topic, _)| topic == probe)
        .expect("the answer names the probe topic");
    assert_eq!(
        *error,
        Some(RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED),
        "the refusal never reached the ping"
    );
}

#[tokio::test]
async fn ping_returns_connection_error_after_close() {
    let (_container, broker) = start_broker().await;
    broker.close().await;
    let err = broker.ping().await.expect_err("ping after close must fail");
    assert!(
        matches!(err, ShoveError::Connection(_)),
        "expected ShoveError::Connection, got {err:?}"
    );
}

#[tokio::test]
async fn ping_with_timeout_honors_deadline() {
    // Point at an unreachable broker. Port 1 is reserved (tcpmux) and
    // refuses connections on every supported test platform.
    let config = KafkaConfig::new("127.0.0.1:1");
    let client = KafkaClient::connect(&config)
        .await
        .expect("connect builds the config; no real handshake yet");
    let broker = Broker::<Kafka>::from_client(client);

    let start = std::time::Instant::now();
    let err = broker
        .ping_with_timeout(Duration::from_millis(200))
        .await
        .expect_err("ping against unreachable broker must fail");
    let elapsed = start.elapsed();

    assert!(
        matches!(err, ShoveError::Connection(_)),
        "expected ShoveError::Connection, got {err:?}"
    );
    // Wide envelope: the metadata fetch is bounded by the passed timeout
    // (200 ms), plus librdkafka and spawn_blocking overhead.
    assert!(
        elapsed < Duration::from_secs(2),
        "ping_with_timeout(200ms) returned in {elapsed:?}, expected < 2s"
    );
}
