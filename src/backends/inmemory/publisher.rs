use std::collections::HashMap;

use uuid::Uuid;

use crate::Topic;
use crate::backend::PublisherImpl;
use crate::error::{Result, ShoveError};
use crate::publisher_internal::validate_headers;

use super::client::{Envelope, InMemoryBroker};
use super::constants::{X_MESSAGE_ID, X_SEQUENCE_KEY};
use super::topology::InMemoryTopologyDeclarer;

/// Publishes messages into an [`InMemoryBroker`].
#[derive(Clone)]
pub struct InMemoryPublisher {
    broker: InMemoryBroker,
}

impl InMemoryPublisher {
    pub fn new(broker: InMemoryBroker) -> Self {
        Self { broker }
    }

    async fn publish_one<T: Topic>(
        &self,
        message: &T::Message,
        mut headers: HashMap<String, String>,
    ) -> Result<()> {
        let topology = T::topology();
        let payload = serde_json::to_vec(message)?;

        let queue_name = if let Some(seq) = topology.sequencing() {
            let key_fn = T::SEQUENCE_KEY_FN.ok_or_else(|| {
                ShoveError::Topology(format!(
                    "topic {} has sequencing config but SEQUENCE_KEY_FN is None — \
                     set const SEQUENCE_KEY_FN in your Topic impl",
                    topology.queue()
                ))
            })?;
            let key = key_fn(message);
            let shard = shard_index(&key, seq.routing_shards());
            headers.insert(X_SEQUENCE_KEY.to_string(), key);
            InMemoryTopologyDeclarer::shard_queue_name(topology.queue(), shard)
        } else {
            topology.queue().to_string()
        };

        let queue = self.broker.lookup(&queue_name)?;

        headers
            .entry(X_MESSAGE_ID.to_string())
            .or_insert_with(|| Uuid::new_v4().to_string());

        self.broker
            .enqueue(&queue, Envelope { payload, headers })
            .await
    }
}

impl InMemoryPublisher {
    pub async fn publish<T: Topic>(&self, message: &T::Message) -> Result<()> {
        self.publish_one::<T>(message, HashMap::new()).await
    }

    pub async fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        validate_headers(&headers)?;
        self.publish_one::<T>(message, headers).await
    }

    pub async fn publish_batch<T: Topic>(&self, messages: &[T::Message]) -> Result<()> {
        for message in messages {
            self.publish_one::<T>(message, HashMap::new()).await?;
        }
        Ok(())
    }
}

impl PublisherImpl for InMemoryPublisher {
    fn publish<T: Topic>(
        &self,
        msg: &T::Message,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        InMemoryPublisher::publish::<T>(self, msg)
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        InMemoryPublisher::publish_with_headers::<T>(self, msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        InMemoryPublisher::publish_batch::<T>(self, msgs)
    }
}

fn shard_index(key: &str, shards: u16) -> u16 {
    debug_assert!(shards > 0);
    (fnv1a_hash(key) % shards as u64) as u16
}

fn fnv1a_hash(key: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::topic::{SequencedTopic, Topic as TopicTrait};
    use crate::topology::{QueueTopology, SequenceFailure, TopologyBuilder};

    use crate::backends::inmemory::topology::InMemoryTopologyDeclarer as Declarer;
    use crate::topology::TopologyDeclarer;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Msg {
        id: u32,
    }

    struct SimpleTopic;
    impl TopicTrait for SimpleTopic {
        type Message = Msg;
        fn topology() -> &'static QueueTopology {
            static T: OnceLock<QueueTopology> = OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("simple-pub").dlq().build())
        }
    }

    struct SeqTopic;
    impl TopicTrait for SeqTopic {
        type Message = Msg;
        fn topology() -> &'static QueueTopology {
            static T: OnceLock<QueueTopology> = OnceLock::new();
            T.get_or_init(|| {
                TopologyBuilder::new("seq-pub")
                    .sequenced(SequenceFailure::Skip)
                    .routing_shards(4)
                    .hold_queue(std::time::Duration::from_secs(5))
                    .dlq()
                    .build()
            })
        }
        const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Self::sequence_key);
    }
    impl SequencedTopic for SeqTopic {
        fn sequence_key(message: &Self::Message) -> String {
            format!("key-{}", message.id % 8)
        }
    }

    struct SeqTopicNoKeyFn;
    impl TopicTrait for SeqTopicNoKeyFn {
        type Message = Msg;
        fn topology() -> &'static QueueTopology {
            static T: OnceLock<QueueTopology> = OnceLock::new();
            T.get_or_init(|| {
                TopologyBuilder::new("seq-nokey-pub")
                    .sequenced(SequenceFailure::Skip)
                    .routing_shards(2)
                    .hold_queue(std::time::Duration::from_secs(5))
                    .dlq()
                    .build()
            })
        }
    }

    async fn setup<T: TopicTrait>(broker: &InMemoryBroker) {
        let d = Declarer::new(broker.clone());
        d.declare(T::topology()).await.unwrap();
    }

    #[tokio::test]
    async fn publish_routes_to_main_queue() {
        let broker = InMemoryBroker::new();
        setup::<SimpleTopic>(&broker).await;

        let publisher = InMemoryPublisher::new(broker.clone());
        publisher
            .publish::<SimpleTopic>(&Msg { id: 7 })
            .await
            .unwrap();

        let queue = broker.lookup("simple-pub").unwrap();
        let env = queue.buffer.lock().await.pop_front().unwrap();
        let decoded: Msg = serde_json::from_slice(&env.payload).unwrap();
        assert_eq!(decoded.id, 7);
        assert!(env.headers.contains_key(X_MESSAGE_ID));
    }

    #[tokio::test]
    async fn publish_sequenced_routes_to_shard() {
        let broker = InMemoryBroker::new();
        setup::<SeqTopic>(&broker).await;

        let publisher = InMemoryPublisher::new(broker.clone());
        publisher.publish::<SeqTopic>(&Msg { id: 3 }).await.unwrap();

        // Same key → same shard for subsequent publishes.
        let key = SeqTopic::sequence_key(&Msg { id: 3 });
        let expected_shard = shard_index(&key, 4);
        let shard_queue = broker
            .lookup(&format!("seq-pub-seq-{expected_shard}"))
            .unwrap();
        let env = shard_queue.buffer.lock().await.pop_front().unwrap();
        assert_eq!(env.headers.get(X_SEQUENCE_KEY).unwrap(), &key);
    }

    #[tokio::test]
    async fn publish_sequenced_without_key_fn_errors() {
        let broker = InMemoryBroker::new();
        setup::<SeqTopicNoKeyFn>(&broker).await;
        let publisher = InMemoryPublisher::new(broker);
        let err = publisher
            .publish::<SeqTopicNoKeyFn>(&Msg { id: 1 })
            .await
            .unwrap_err();
        assert!(matches!(err, ShoveError::Topology(_)));
    }

    #[tokio::test]
    async fn publish_with_headers_rejects_reserved_prefix() {
        let broker = InMemoryBroker::new();
        setup::<SimpleTopic>(&broker).await;
        let publisher = InMemoryPublisher::new(broker);
        let mut headers = HashMap::new();
        headers.insert(X_SEQUENCE_KEY.to_string(), "nope".to_string());
        let err = publisher
            .publish_with_headers::<SimpleTopic>(&Msg { id: 1 }, headers)
            .await
            .unwrap_err();
        assert!(matches!(err, ShoveError::Validation(_)));
    }

    #[tokio::test]
    async fn publish_undeclared_queue_errors() {
        let broker = InMemoryBroker::new();
        // No declare.
        let publisher = InMemoryPublisher::new(broker);
        let err = publisher
            .publish::<SimpleTopic>(&Msg { id: 1 })
            .await
            .unwrap_err();
        assert!(matches!(err, ShoveError::Topology(_)));
    }

    #[tokio::test]
    async fn publish_batch_enqueues_all() {
        let broker = InMemoryBroker::new();
        setup::<SimpleTopic>(&broker).await;
        let publisher = InMemoryPublisher::new(broker.clone());

        let messages: Vec<Msg> = (0..5).map(|i| Msg { id: i }).collect();
        publisher
            .publish_batch::<SimpleTopic>(&messages)
            .await
            .unwrap();

        let queue = broker.lookup("simple-pub").unwrap();
        assert_eq!(queue.buffer.lock().await.len(), 5);
    }

    #[tokio::test]
    async fn shard_index_is_deterministic_per_key() {
        let a1 = shard_index("hello", 16);
        let a2 = shard_index("hello", 16);
        assert_eq!(a1, a2);
    }
}
