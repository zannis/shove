//! `publish_with_headers` refuses the header names a backend reserves for
//! itself, so a caller cannot forge what shove stamps or overwrite what it
//! writes.

#![cfg(any(
    feature = "inmemory",
    feature = "kafka",
    feature = "nats",
    feature = "redis-streams"
))]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use shove::ShoveError;

const CALLER: &str = "x-trace-id";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Payload {
    id: String,
}

fn forged_headers(name: &str) -> HashMap<String, String> {
    HashMap::from([(name.to_string(), "99".to_string())])
}

fn caller_headers() -> HashMap<String, String> {
    HashMap::from([(CALLER.to_string(), "trace-1".to_string())])
}

fn assert_reserved(err: ShoveError, name: &str) {
    match err {
        ShoveError::Validation(message) => assert!(
            message.contains(name),
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

    const FORGED: &str = "Shove-Retry-Count";
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
                forged_headers(FORGED),
            )
            .await
            .expect_err("a reserved header name is refused");
        assert_reserved(err, FORGED);
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

    const FORGED: &str = "Shove-Retry-Count";
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
                forged_headers(FORGED),
            )
            .await
            .expect_err("a reserved header name is refused");
        assert_reserved(err, FORGED);
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

    const FORGED: &str = "Shove-Retry-Count";
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
                forged_headers(FORGED),
            )
            .await
            .expect_err("a reserved header name is refused");
        assert_reserved(err, FORGED);
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

#[cfg(feature = "redis-streams")]
mod redis {
    use std::time::Duration;

    use redis::{Client, Value, cmd};
    use shove::redis::{RedisConfig, RedisMode};
    use shove::topology::TopologyBuilder;
    use shove::{Broker, Redis};
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};

    use super::{CALLER, Payload, assert_reserved, caller_headers, forged_headers};

    const FORGED: &str = "payload";
    const STREAM: &str = "reserved-headers-redis";

    shove::define_topic!(ReservedTopic, Payload, TopologyBuilder::new(STREAM).build());

    async fn connect(url: &str) -> Broker<Redis> {
        let mut last = None;
        for _ in 0..40 {
            let config = RedisConfig::new(RedisMode::Standalone {
                url: url.to_owned(),
            })
            .with_group("reserved-headers");
            match Broker::<Redis>::new(config).await {
                Ok(broker) => return broker,
                Err(e) => {
                    last = Some(e);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
        panic!("connect to Redis at {url}: {last:?}")
    }

    fn text(value: &Value) -> String {
        match value {
            Value::BulkString(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Value::SimpleString(text) => text.clone(),
            other => panic!("expected a string, got {other:?}"),
        }
    }

    /// Every field of every entry on the stream, in the order Redis stores
    /// them and with duplicate field names kept.
    async fn stored_entries(url: &str) -> Vec<Vec<(String, String)>> {
        let client = Client::open(url).expect("open a raw Redis client");
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("open a raw Redis connection");
        let reply: Value = cmd("XRANGE")
            .arg(STREAM)
            .arg("-")
            .arg("+")
            .query_async(&mut conn)
            .await
            .expect("XRANGE the stream");
        let Value::Array(entries) = reply else {
            panic!("XRANGE did not reply with an array: {reply:?}");
        };
        entries
            .into_iter()
            .map(|entry| {
                let Value::Array(parts) = entry else {
                    panic!("expected an entry array, got {entry:?}");
                };
                let Some(Value::Array(fields)) = parts.into_iter().nth(1) else {
                    panic!("an entry carries no field list");
                };
                fields
                    .chunks_exact(2)
                    .map(|pair| (text(&pair[0]), text(&pair[1])))
                    .collect()
            })
            .collect()
    }

    #[tokio::test]
    async fn publish_with_headers_rejects_payload() {
        let container = RedisContainer::default()
            .with_tag("7.0")
            .start()
            .await
            .expect("start the Redis container");
        let port = container
            .get_host_port_ipv4(REDIS_PORT)
            .await
            .expect("get the Redis port");
        let url = format!("redis://127.0.0.1:{port}/");
        let broker = connect(&url).await;
        broker
            .topology()
            .declare::<ReservedTopic>()
            .await
            .expect("declare the topic");
        let publisher = broker.publisher().await.expect("create the publisher");
        let honest = Payload {
            id: "honest".into(),
        };

        let err = publisher
            .publish_with_headers::<ReservedTopic>(&honest, forged_headers(FORGED))
            .await
            .expect_err("a header named payload is refused");
        assert_reserved(err, FORGED);
        assert!(
            stored_entries(&url).await.is_empty(),
            "the refused publish was written"
        );

        publisher
            .publish_with_headers::<ReservedTopic>(&honest, caller_headers())
            .await
            .expect("a name outside the reserved set is published");
        let stored = stored_entries(&url).await;
        assert_eq!(stored.len(), 1, "{stored:?}");
        let bodies: Vec<&str> = stored[0]
            .iter()
            .filter(|(field, _)| field == FORGED)
            .map(|(_, value)| value.as_str())
            .collect();
        assert_eq!(bodies.len(), 1, "one payload field per entry: {stored:?}");
        assert_eq!(
            serde_json::from_str::<Payload>(bodies[0]).expect("the stored payload decodes"),
            honest
        );
        assert!(
            stored[0]
                .iter()
                .any(|(field, value)| field == CALLER && value == "trace-1"),
            "{stored:?}"
        );
        broker.close().await;
    }
}
