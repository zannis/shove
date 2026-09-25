//! Live checks for the Kafka producer topic-creation switch.

#![cfg(feature = "kafka")]

use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use shove::kafka::{KafkaClient, KafkaConfig};
use shove::topology::TopologyBuilder;
use shove::{Broker, Kafka, ShoveError};
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Payload {
    id: String,
}

shove::define_topic!(
    OptInTopic,
    Payload,
    TopologyBuilder::new("producer-opt-in").build()
);

shove::define_topic!(
    ResetTopic,
    Payload,
    TopologyBuilder::new("producer-reset-off").build()
);

shove::define_topic!(
    BrokerDisabledTopic,
    Payload,
    TopologyBuilder::new("broker-auto-create-disabled").build()
);

async fn start_kafka(
    auto_create: bool,
) -> (testcontainers::ContainerAsync<KafkaContainer>, String) {
    let container = KafkaContainer::default()
        .with_env_var(
            "KAFKA_AUTO_CREATE_TOPICS_ENABLE",
            if auto_create { "true" } else { "false" },
        )
        .start()
        .await
        .expect("start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("get Kafka port");
    (container, format!("127.0.0.1:{port}"))
}

async fn connect(config: KafkaConfig) -> Broker<Kafka> {
    let client = KafkaClient::connect_with_retry(&config, 10)
        .await
        .expect("connect to Kafka");
    Broker::<Kafka>::from_client(client)
}

// An all-topic metadata request cannot ask the broker to create a named topic.
fn live_partitions(bootstrap: &str, topic: &str) -> Vec<i32> {
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("allow.auto.create.topics", "false")
        .create()
        .expect("create metadata probe");
    let metadata = probe
        .fetch_metadata(None, Duration::from_secs(10))
        .expect("fetch all-topic metadata");
    let partitions = metadata
        .topics()
        .iter()
        .find(|entry| entry.name() == topic)
        .map(|entry| {
            assert_eq!(entry.error(), None, "topic metadata must be valid");
            entry.partitions().iter().map(|part| part.id()).collect()
        })
        .unwrap_or_default();
    println!("broker metadata: topic={topic}, partitions={partitions:?}");
    partitions
}

async fn read_payload(bootstrap: &str, topic: &str) -> Payload {
    let partitions = live_partitions(bootstrap, topic);
    assert!(!partitions.is_empty(), "published topic must exist");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("verify-{topic}"))
        .set("enable.auto.commit", "false")
        .set("allow.auto.create.topics", "false")
        .create()
        .expect("create independent consumer");
    let mut assignment = TopicPartitionList::new();
    for partition in partitions {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .expect("set partition offset");
    }
    consumer
        .assign(&assignment)
        .expect("assign topic partitions");
    let record = tokio::time::timeout(Duration::from_secs(15), consumer.recv())
        .await
        .expect("published record must arrive")
        .expect("read published record");
    let payload: Payload = serde_json::from_slice(record.payload().expect("record has a payload"))
        .expect("decode published payload");
    println!(
        "independent consumer: topic={topic}, partition={}, offset={}, payload={payload:?}",
        record.partition(),
        record.offset()
    );
    payload
}

#[tokio::test]
async fn producer_switch_is_per_client_and_can_be_reset_to_false() {
    let (_container, bootstrap) = start_kafka(true).await;
    let enabled =
        connect(KafkaConfig::new(&bootstrap).with_producer_auto_create_topics(true)).await;
    let disabled = connect(
        KafkaConfig::new(&bootstrap)
            .with_producer_auto_create_topics(true)
            .with_producer_auto_create_topics(false),
    )
    .await;
    let enabled_publisher = enabled.publisher().await.expect("create enabled publisher");
    let disabled_publisher = disabled
        .publisher()
        .await
        .expect("create disabled publisher");

    assert!(live_partitions(&bootstrap, "producer-opt-in").is_empty());
    let first = Payload {
        id: "enabled-client-created-topic".into(),
    };
    enabled_publisher
        .publish::<OptInTopic>(&first)
        .await
        .expect("enabled producer creates the first topic");
    assert_eq!(read_payload(&bootstrap, "producer-opt-in").await, first);

    assert!(live_partitions(&bootstrap, "producer-reset-off").is_empty());
    let blocked = Payload {
        id: "disabled-client-must-not-publish".into(),
    };
    let error = disabled_publisher
        .publish::<ResetTopic>(&blocked)
        .await
        .expect_err("the last false value must disable topic creation");
    println!("explicit false publish result: {error}");
    assert!(matches!(error, ShoveError::Connection(_)));
    assert!(live_partitions(&bootstrap, "producer-reset-off").is_empty());

    let second = Payload {
        id: "enabled-client-still-creates-topics".into(),
    };
    enabled_publisher
        .publish::<ResetTopic>(&second)
        .await
        .expect("another client's false value must not change the enabled producer");
    assert_eq!(read_payload(&bootstrap, "producer-reset-off").await, second);
    disabled.close().await;
    enabled.close().await;
}

#[tokio::test]
async fn producer_opt_in_respects_broker_disable_and_explicit_declaration() {
    let (_container, bootstrap) = start_kafka(false).await;
    let broker = connect(KafkaConfig::new(&bootstrap).with_producer_auto_create_topics(true)).await;
    let publisher = broker.publisher().await.expect("create enabled publisher");
    let payload = Payload {
        id: "declared-after-broker-refused-auto-create".into(),
    };

    assert!(live_partitions(&bootstrap, "broker-auto-create-disabled").is_empty());
    let error = publisher
        .publish::<BrokerDisabledTopic>(&payload)
        .await
        .expect_err("producer opt-in must not override the broker's disabled setting");
    println!("broker-disabled publish result: {error}");
    assert!(matches!(error, ShoveError::Connection(_)));
    assert!(live_partitions(&bootstrap, "broker-auto-create-disabled").is_empty());

    broker
        .topology()
        .declare::<BrokerDisabledTopic>()
        .await
        .expect("explicit declaration works when automatic creation is disabled");
    publisher
        .publish::<BrokerDisabledTopic>(&payload)
        .await
        .expect("publish succeeds after explicit declaration");
    assert_eq!(
        read_payload(&bootstrap, "broker-auto-create-disabled").await,
        payload
    );
    broker.close().await;
}
