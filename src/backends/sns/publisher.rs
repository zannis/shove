use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, warn};
use uuid::Uuid;

use crate::backends::sns::client::SnsClient;
use crate::backends::sns::topology::TopicRegistry;
use crate::error::{Result, ShoveError};
use crate::publisher::Publisher;
use crate::topic::Topic;

/// Maximum number of messages in a single SNS PublishBatch call.
const SNS_BATCH_LIMIT: usize = 10;

/// Convert a `HashMap<String, String>` into SNS message attributes.
fn hashmap_to_message_attributes(
    headers: HashMap<String, String>,
) -> HashMap<String, aws_sdk_sns::types::MessageAttributeValue> {
    headers
        .into_iter()
        .map(|(k, v)| {
            let attr = aws_sdk_sns::types::MessageAttributeValue::builder()
                .data_type("String")
                .string_value(v)
                .build()
                .expect("building MessageAttributeValue should not fail");
            (k, attr)
        })
        .collect()
}

/// Split a slice into chunks of at most `SNS_BATCH_LIMIT`.
fn chunk_batch<T>(items: &[T]) -> Vec<&[T]> {
    items.chunks(SNS_BATCH_LIMIT).collect()
}

/// SNS publisher that implements the `Publisher` trait.
#[derive(Clone)]
pub struct SnsPublisher {
    client: SnsClient,
    registry: Arc<TopicRegistry>,
}

impl SnsPublisher {
    pub fn new(client: SnsClient, registry: Arc<TopicRegistry>) -> Self {
        Self { client, registry }
    }

    async fn resolve_arn(&self, queue_name: &str) -> Result<String> {
        self.registry.get(queue_name).await.ok_or_else(|| {
            ShoveError::Topology(format!(
                "no SNS topic ARN registered for queue '{queue_name}'. \
                     Declare the topology first or provide an ARN override."
            ))
        })
    }

    async fn publish_single(
        &self,
        topic_arn: &str,
        payload: &str,
        group_id: Option<&str>,
        attributes: Option<HashMap<String, aws_sdk_sns::types::MessageAttributeValue>>,
    ) -> Result<()> {
        let mut req = self
            .client
            .inner()
            .publish()
            .topic_arn(topic_arn)
            .message(payload);

        if let Some(gid) = group_id {
            req = req
                .message_group_id(gid)
                .message_deduplication_id(Uuid::new_v4().to_string());
        }

        if let Some(attrs) = attributes {
            for (k, v) in attrs {
                req = req.message_attributes(k, v);
            }
        }

        req.send()
            .await
            .map_err(|e| ShoveError::Connection(format!("SNS publish failed: {e}")))?;

        Ok(())
    }

    async fn do_publish<T: Topic>(
        &self,
        message: &T::Message,
        headers: Option<HashMap<String, String>>,
    ) -> Result<()> {
        let payload = serde_json::to_string(message).map_err(ShoveError::Serialization)?;
        let topology = T::topology();
        let queue_name = topology.queue();
        let topic_arn = self.resolve_arn(queue_name).await?;

        let group_id = match (topology.sequencing(), T::SEQUENCE_KEY_FN) {
            (Some(_), Some(kf)) => Some(kf(message)),
            (Some(_), None) => {
                return Err(ShoveError::Topology(
                    "topic has sequencing config but no SEQUENCE_KEY_FN defined".to_string(),
                ));
            }
            (None, _) => None,
        };

        let attributes = headers.map(hashmap_to_message_attributes);

        debug!(queue_name, topic_arn, "publishing message to SNS");

        match self
            .publish_single(
                &topic_arn,
                &payload,
                group_id.as_deref(),
                attributes.clone(),
            )
            .await
        {
            Ok(()) => {
                debug!(queue_name, "message published to SNS");
                Ok(())
            }
            Err(e) => {
                warn!(queue_name, error = %e, "SNS publish failed, retrying once");
                self.publish_single(&topic_arn, &payload, group_id.as_deref(), None)
                    .await
            }
        }
    }
}

impl Publisher for SnsPublisher {
    async fn publish<T: Topic>(&self, message: &T::Message) -> Result<()> {
        self.do_publish::<T>(message, None).await
    }

    async fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        self.do_publish::<T>(message, Some(headers)).await
    }

    async fn publish_batch<T: Topic>(&self, messages: &[T::Message]) -> Result<()> {
        let topology = T::topology();
        let key_fn = T::SEQUENCE_KEY_FN;

        // Serialize all messages up front for fail-fast behaviour.
        let serialized: Result<Vec<String>> = messages
            .iter()
            .map(|m| serde_json::to_string(m).map_err(ShoveError::Serialization))
            .collect();

        // Pre-compute routing keys while we still have access to messages.
        let routing_keys: Option<Vec<String>> = key_fn.map(|kf| messages.iter().map(kf).collect());

        let payloads = serialized?;
        let queue_name = topology.queue();
        let topic_arn = self.resolve_arn(queue_name).await?;

        let has_sequencing = topology.sequencing().is_some();

        if has_sequencing && routing_keys.is_none() {
            return Err(ShoveError::Topology(
                "topic has sequencing config but no SEQUENCE_KEY_FN defined".to_string(),
            ));
        }

        debug!(
            queue_name,
            count = payloads.len(),
            "publishing batch to SNS"
        );

        // Build batch entries
        let entries: Vec<aws_sdk_sns::types::PublishBatchRequestEntry> = payloads
            .iter()
            .enumerate()
            .map(|(i, payload)| {
                let mut entry = aws_sdk_sns::types::PublishBatchRequestEntry::builder()
                    .id(i.to_string())
                    .message(payload);

                if let Some(ref keys) = routing_keys {
                    entry = entry
                        .message_group_id(&keys[i])
                        .message_deduplication_id(Uuid::new_v4().to_string());
                }

                entry
                    .build()
                    .expect("building PublishBatchRequestEntry should not fail")
            })
            .collect();

        // Chunk into groups of 10 and send
        for chunk in chunk_batch(&entries) {
            let result = self
                .client
                .inner()
                .publish_batch()
                .topic_arn(&topic_arn)
                .set_publish_batch_request_entries(Some(chunk.to_vec()))
                .send()
                .await
                .map_err(|e| ShoveError::Connection(format!("SNS batch publish failed: {e}")))?;

            let failed = result.failed();
            if !failed.is_empty() {
                return Err(ShoveError::Connection(format!(
                    "SNS batch publish: {} of {} messages failed. First error: {} (code: {})",
                    failed.len(),
                    chunk.len(),
                    failed[0].message().unwrap_or("unknown"),
                    failed[0].code(),
                )));
            }
        }

        debug!(queue_name, count = payloads.len(), "batch published to SNS");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_batch_empty() {
        let items: Vec<u8> = vec![];
        let chunks = chunk_batch(&items);
        assert!(chunks.is_empty());
    }

    #[test]
    fn chunk_batch_under_limit() {
        let items: Vec<u8> = vec![1, 2, 3];
        let chunks = chunk_batch(&items);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 3);
    }

    #[test]
    fn chunk_batch_exact_limit() {
        let items: Vec<u8> = (0..10).collect();
        let chunks = chunk_batch(&items);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 10);
    }

    #[test]
    fn chunk_batch_over_limit() {
        let items: Vec<u8> = (0..25).collect();
        let chunks = chunk_batch(&items);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 10);
        assert_eq!(chunks[1].len(), 10);
        assert_eq!(chunks[2].len(), 5);
    }

    #[test]
    fn chunk_batch_one_over_limit() {
        let items: Vec<u8> = (0..11).collect();
        let chunks = chunk_batch(&items);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 10);
        assert_eq!(chunks[1].len(), 1);
    }

    #[test]
    fn hashmap_to_message_attributes_empty() {
        let attrs = hashmap_to_message_attributes(HashMap::new());
        assert!(attrs.is_empty());
    }

    #[test]
    fn hashmap_to_message_attributes_single() {
        let mut map = HashMap::new();
        map.insert("x-trace-id".to_string(), "abc123".to_string());
        let attrs = hashmap_to_message_attributes(map);
        assert_eq!(attrs.len(), 1);
        let attr = attrs.get("x-trace-id").expect("key should be present");
        assert_eq!(attr.data_type(), "String");
        assert_eq!(attr.string_value(), Some("abc123"));
    }

    #[test]
    fn hashmap_to_message_attributes_multiple() {
        let mut map = HashMap::new();
        map.insert("key-a".to_string(), "val-a".to_string());
        map.insert("key-b".to_string(), "val-b".to_string());
        map.insert("key-c".to_string(), "val-c".to_string());
        let attrs = hashmap_to_message_attributes(map);
        assert_eq!(attrs.len(), 3);
        assert!(attrs.contains_key("key-a"));
        assert!(attrs.contains_key("key-b"));
        assert!(attrs.contains_key("key-c"));
    }
}
