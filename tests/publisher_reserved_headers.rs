//! `publish_with_headers` refuses header names under the reserved `shove-`
//! prefix, so a caller cannot forge the headers shove stamps itself.

#![cfg(any(feature = "inmemory", feature = "kafka", feature = "nats"))]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use shove::ShoveError;

const FORGED: &str = "Shove-Retry-Count";
const CALLER: &str = "x-trace-id";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Payload {
    id: String,
}

fn forged_headers() -> HashMap<String, String> {
    HashMap::from([(FORGED.to_string(), "99".to_string())])
}

fn caller_headers() -> HashMap<String, String> {
    HashMap::from([(CALLER.to_string(), "trace-1".to_string())])
}

fn assert_reserved(err: ShoveError) {
    match err {
        ShoveError::Validation(message) => assert!(
            message.contains(FORGED),
            "the error does not name the header: {message:?}"
        ),
        other => panic!("expected ShoveError::Validation, got {other:?}"),
    }
}

#[cfg(feature = "inmemory")]
mod inmemory {
    use shove::inmemory::{InMemoryConfig, InMemoryQueueStatsProvider as _};
    use shove::topology::TopologyBuilder;
    use shove::{Broker, InMemory};

    use super::{Payload, assert_reserved, caller_headers, forged_headers};

    const QUEUE: &str = "reserved-headers-inmemory";

    shove::define_topic!(ReservedTopic, Payload, TopologyBuilder::new(QUEUE).build());

    #[tokio::test]
    async fn publish_with_headers_rejects_shove_retry_count() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("create the in-process broker");
        broker
            .topology()
            .declare::<ReservedTopic>()
            .await
            .expect("declare the topic");
        let publisher = broker.publisher().await.expect("create the publisher");
        let stats = broker.queue_stats_provider();

        let err = publisher
            .publish_with_headers::<ReservedTopic>(
                &Payload {
                    id: "forged".into(),
                },
                forged_headers(),
            )
            .await
            .expect_err("a reserved header name is refused");
        assert_reserved(err);
        let queued = stats.get_queue_stats(QUEUE).await.expect("queue stats");
        assert_eq!(queued.messages_ready, 0, "the refused publish was enqueued");

        publisher
            .publish_with_headers::<ReservedTopic>(
                &Payload {
                    id: "honest".into(),
                },
                caller_headers(),
            )
            .await
            .expect("a name outside the reserved prefix is published");
        let queued = stats.get_queue_stats(QUEUE).await.expect("queue stats");
        assert_eq!(queued.messages_ready, 1);
        broker.close().await;
    }
}

#[cfg(feature = "kafka")]
mod kafka {
    use std::collections::HashMap;
    use std::time::Duration;

    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::{Headers as _, Message as _};
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};
    use shove::kafka::{KafkaClient, KafkaConfig};
    use shove::topology::TopologyBuilder;
    use shove::{Broker, Kafka};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};

    use super::{CALLER, Payload, assert_reserved, caller_headers, forged_headers};

    const TOPIC: &str = "reserved-headers-kafka";
    const TIMEOUT: Duration = Duration::from_secs(15);

    shove::define_topic!(ReservedTopic, Payload, TopologyBuilder::new(TOPIC).build());

    /// The headers of every record stored on the topic, read back by a client
    /// that shares nothing with the publisher.
    async fn stored_headers(bootstrap: &str) -> Vec<HashMap<String, String>> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", format!("verify-{TOPIC}"))
            .set("enable.auto.commit", "false")
            .set("allow.auto.create.topics", "false")
            .create()
            .expect("create the read-back consumer");
        let metadata = consumer
            .fetch_metadata(Some(TOPIC), TIMEOUT)
            .expect("fetch the topic metadata");
        let mut assignment = TopicPartitionList::new();
        let mut stored = 0;
        for topic in metadata.topics() {
            for partition in topic.partitions() {
                let (low, high) = consumer
                    .fetch_watermarks(TOPIC, partition.id(), TIMEOUT)
                    .expect("fetch the watermarks");
                stored += high - low;
                assignment
                    .add_partition_offset(TOPIC, partition.id(), Offset::Beginning)
                    .expect("name the partition");
            }
        }
        consumer.assign(&assignment).expect("assign the partitions");
        let mut records = Vec::new();
        for _ in 0..stored {
            let record = tokio::time::timeout(TIMEOUT, consumer.recv())
                .await
                .expect("a stored record arrives")
                .expect("read the stored record");
            records.push(
                record
                    .headers()
                    .map(|headers| {
                        headers
                            .iter()
                            .filter_map(|h| {
                                h.value.map(|v| {
                                    (h.key.to_string(), String::from_utf8_lossy(v).into_owned())
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            );
        }
        records
    }

    #[tokio::test]
    async fn publish_with_headers_rejects_shove_retry_count() {
        let container = KafkaContainer::default()
            .start()
            .await
            .expect("start the Kafka container");
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("get the Kafka port");
        let bootstrap = format!("127.0.0.1:{port}");
        let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap), 10)
            .await
            .expect("connect to Kafka");
        let broker = Broker::<Kafka>::from_client(client);
        broker
            .topology()
            .declare::<ReservedTopic>()
            .await
            .expect("declare the topic");
        let publisher = broker.publisher().await.expect("create the publisher");

        let err = publisher
            .publish_with_headers::<ReservedTopic>(
                &Payload {
                    id: "forged".into(),
                },
                forged_headers(),
            )
            .await
            .expect_err("a reserved header name is refused");
        assert_reserved(err);
        assert!(
            stored_headers(&bootstrap).await.is_empty(),
            "the refused publish reached the broker"
        );

        publisher
            .publish_with_headers::<ReservedTopic>(
                &Payload {
                    id: "honest".into(),
                },
                caller_headers(),
            )
            .await
            .expect("a name outside the reserved prefix is published");
        let stored = stored_headers(&bootstrap).await;
        assert_eq!(stored.len(), 1, "{stored:?}");
        assert_eq!(
            stored[0].get("Shove-Retry-Count").map(String::as_str),
            Some("0"),
            "{stored:?}"
        );
        assert_eq!(
            stored[0].get(CALLER).map(String::as_str),
            Some("trace-1"),
            "{stored:?}"
        );
        broker.close().await;
    }
}

#[cfg(feature = "nats")]
mod nats {
    use shove::nats::{NatsClient, NatsConfig};
    use shove::topology::TopologyBuilder;
    use shove::{Broker, Nats};
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::nats::{Nats as NatsContainer, NatsServerCmd};

    use super::{CALLER, Payload, assert_reserved, caller_headers, forged_headers};

    const STREAM: &str = "reserved-headers-nats";

    shove::define_topic!(ReservedTopic, Payload, TopologyBuilder::new(STREAM).build());

    #[tokio::test]
    async fn publish_with_headers_rejects_shove_retry_count() {
        let cmd = NatsServerCmd::default().with_jetstream();
        let container = NatsContainer::default()
            .with_cmd(&cmd)
            .start()
            .await
            .expect("start the NATS container");
        let host = container.get_host().await.expect("get the host");
        let port = container
            .get_host_port_ipv4(4222)
            .await
            .expect("get the NATS port");
        let client =
            NatsClient::connect_with_retry(&NatsConfig::new(format!("nats://{host}:{port}")), 10)
                .await
                .expect("connect to NATS");
        let broker = Broker::<Nats>::from_client(client.clone());
        broker
            .topology()
            .declare::<ReservedTopic>()
            .await
            .expect("declare the topic");
        let publisher = broker.publisher().await.expect("create the publisher");
        let mut stream = client
            .jetstream()
            .get_stream(STREAM)
            .await
            .expect("the stream exists");

        let err = publisher
            .publish_with_headers::<ReservedTopic>(
                &Payload {
                    id: "forged".into(),
                },
                forged_headers(),
            )
            .await
            .expect_err("a reserved header name is refused");
        assert_reserved(err);
        let stored = stream.info().await.expect("stream info").state.messages;
        assert_eq!(stored, 0, "the refused publish reached the stream");

        publisher
            .publish_with_headers::<ReservedTopic>(
                &Payload {
                    id: "honest".into(),
                },
                caller_headers(),
            )
            .await
            .expect("a name outside the reserved prefix is published");
        let stored = stream.info().await.expect("stream info").state.messages;
        assert_eq!(stored, 1);
        let message = stream
            .get_raw_message(1)
            .await
            .expect("read the stored message");
        assert_eq!(
            message.headers.get("Shove-Retry-Count").map(|v| v.as_str()),
            Some("0")
        );
        assert_eq!(
            message.headers.get(CALLER).map(|v| v.as_str()),
            Some("trace-1")
        );
        broker.close().await;
    }
}
