use std::collections::HashMap;

use uuid::Uuid;

use crate::Topic;
use crate::backend::PublisherImpl;
use crate::batch::BatchReport;
use crate::error::{Result, ShoveError};
use crate::publisher_internal::{shard_for_key, validate_headers};

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
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;

        if topology.broadcast() {
            // Broadcast has no shared queue to publish into — the message goes
            // to each live subscriber's own buffer, or nowhere if there are
            // none. `.broadcast()` forbids sequencing and a DLQ at build time,
            // so neither branch below can apply.
            headers
                .entry(X_MESSAGE_ID.to_string())
                .or_insert_with(|| Uuid::new_v4().to_string());
            self.broker
                .broadcast_publish(topology.queue(), Envelope::new(payload, headers))
                .await?;
            return Ok(());
        }

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
            .enqueue(&queue, Envelope::new(payload, headers))
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

    /// Publish a batch, as [`Publisher::publish_batch`] does but without the
    /// metrics wrapper.
    ///
    /// A partial failure returns [`ShoveError::PartialBatch`] carrying the
    /// indices still to re-publish. This backend has prefix semantics: the
    /// failing index is the only entry in `failed()`, and everything after it
    /// is `unattempted()`.
    ///
    /// [`Publisher::publish_batch`]: crate::publisher::Publisher::publish_batch
    pub async fn publish_batch<T: Topic>(&self, messages: &[T::Message]) -> Result<()> {
        self.publish_batch_report::<T>(messages)
            .await
            .resolve(messages.len())
            .result
    }

    pub(crate) async fn publish_batch_report<T: Topic>(
        &self,
        messages: &[T::Message],
    ) -> BatchReport {
        for (i, message) in messages.iter().enumerate() {
            if let Err(e) = self.publish_one::<T>(message, HashMap::new()).await {
                return BatchReport::prefix(i, messages.len(), e);
            }
        }
        BatchReport::all_succeeded()
    }
}

impl PublisherImpl for InMemoryPublisher {
    fn publish<T: Topic>(&self, msg: &T::Message) -> impl Future<Output = Result<()>> + Send {
        InMemoryPublisher::publish::<T>(self, msg)
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send {
        InMemoryPublisher::publish_with_headers::<T>(self, msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl Future<Output = BatchReport> + Send {
        InMemoryPublisher::publish_batch_report::<T>(self, msgs)
    }
}

fn shard_index(key: &str, shards: u16) -> u16 {
    shard_for_key(key, shards)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::OnceLock;

    use serde::{Deserialize, Serialize};

    use super::super::client::InMemoryConfig;
    use super::*;
    use crate::topic::{SequencedTopic, Topic as TopicTrait};
    use crate::topology::{QueueTopology, SequenceFailure, TopologyBuilder};

    use crate::backends::inmemory::topology::InMemoryTopologyDeclarer as Declarer;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Msg {
        id: u32,
    }

    struct SimpleTopic;
    impl TopicTrait for SimpleTopic {
        type Message = Msg;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static T: OnceLock<QueueTopology> = OnceLock::new();
            T.get_or_init(|| TopologyBuilder::new("simple-pub").dlq().build())
        }
    }

    struct SeqTopic;
    impl TopicTrait for SeqTopic {
        type Message = Msg;
        type Codec = crate::JsonCodec;
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
        type Codec = crate::JsonCodec;
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
        let outcome = publisher
            .publish_batch_report::<SimpleTopic>(&messages)
            .await
            .resolve(messages.len());
        outcome.result.unwrap();
        assert_eq!(outcome.succeeded, messages.len() as u64);
        assert_eq!(outcome.failed, 0);

        let queue = broker.lookup("simple-pub").unwrap();
        assert_eq!(queue.buffer.lock().await.len(), 5);
    }

    /// A batch that stops partway reports the failing index and the tail it
    /// never tried — the prefix shape.
    ///
    /// Made deterministic without any timing: the broker is shut down *first*,
    /// and the queue capacity is 2. Messages 0 and 1 take the fast path in
    /// `InMemoryBroker::enqueue` (space available, shutdown not consulted);
    /// message 2 finds the buffer full and the already-cancelled shutdown
    /// token resolves immediately.
    #[tokio::test]
    async fn publish_batch_reports_prefix_indices_on_partial_failure() {
        let capacity = NonZeroUsize::new(2).expect("2 is non-zero");
        let broker =
            InMemoryBroker::with_config(InMemoryConfig::default().with_default_capacity(capacity));
        setup::<SimpleTopic>(&broker).await;
        broker.shutdown();

        let publisher = InMemoryPublisher::new(broker.clone());
        let messages: Vec<Msg> = (0..5).map(|i| Msg { id: i }).collect();
        let outcome = publisher
            .publish_batch_report::<SimpleTopic>(&messages)
            .await
            .resolve(messages.len());

        assert_eq!(outcome.succeeded, 2);
        assert_eq!(outcome.failed, 3);
        let Err(ShoveError::PartialBatch(f)) = outcome.result else {
            panic!("expected a PartialBatch for a 2-of-5 batch");
        };
        assert_eq!(f.succeeded(), 2);
        assert_eq!(f.failed(), &[2]);
        assert_eq!(f.unattempted(), &[3, 4]);
        assert_eq!(f.to_republish(), &[2, 3, 4]);
        assert!(matches!(f.source(), ShoveError::Connection(_)));

        // The invariant, asserted for this backend.
        assert_eq!(f.succeeded() + f.to_republish().len(), messages.len());

        let queue = broker.lookup("simple-pub").unwrap();
        assert_eq!(queue.buffer.lock().await.len(), 2);
    }

    /// A batch where *nothing* landed is not partial, so it keeps returning
    /// the bare error it returned before `PartialBatch` existed.
    #[tokio::test]
    async fn publish_batch_wholly_failed_returns_the_bare_error() {
        let broker = InMemoryBroker::new();
        // No declare — every message fails the queue lookup.
        let publisher = InMemoryPublisher::new(broker);
        let messages: Vec<Msg> = (0..3).map(|i| Msg { id: i }).collect();
        let outcome = publisher
            .publish_batch_report::<SimpleTopic>(&messages)
            .await
            .resolve(messages.len());

        assert_eq!(outcome.succeeded, 0);
        assert_eq!(outcome.failed, 3);
        assert!(matches!(outcome.result, Err(ShoveError::Topology(_))));
    }

    #[tokio::test]
    async fn shard_index_is_deterministic_per_key() {
        let a1 = shard_index("hello", 16);
        let a2 = shard_index("hello", 16);
        assert_eq!(a1, a2);
    }
}
