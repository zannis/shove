use aws_sdk_sns::types::{MessageAttributeValue, PublishBatchRequestEntry};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use crate::backend::PublisherImpl;
use crate::backends::sns::client::SnsClient;
use crate::backends::sns::topology::TopicRegistry;
use crate::error::{Result, ShoveError};
use crate::metrics;
use crate::publisher_internal::validate_headers;
use crate::retry::Backoff;
use crate::topic::Topic;

/// Maximum number of messages in a single SNS PublishBatch call.
const SNS_BATCH_LIMIT: usize = 10;

/// FNV-1a 64-bit hash over arbitrary bytes (stable across versions).
fn fnv1a_64(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let mut hash = FNV_OFFSET;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Compute shard index using FNV-1a hash (stable across versions).
fn compute_shard(key: &str, shards: u16) -> u16 {
    (fnv1a_64(key.as_bytes()) % shards as u64) as u16
}

/// Derive a deterministic SNS `MessageDeduplicationId` from the serialised
/// payload.  Using the same ID for every attempt of the same payload means
/// SNS FIFO can deduplicate within its 5-minute window even when a publish
/// is retried after a network error (where the first attempt may have
/// already landed at the broker).
fn content_dedup_id(payload: &str) -> String {
    format!("{:016x}", fnv1a_64(payload.as_bytes()))
}

/// Convert a `HashMap<String, String>` into SNS message attributes.
fn hashmap_to_message_attributes(
    headers: HashMap<String, String>,
) -> HashMap<String, MessageAttributeValue> {
    headers
        .into_iter()
        .map(|(k, v)| {
            let attr = MessageAttributeValue::builder()
                .data_type("String")
                .string_value(v)
                .build()
                .expect("building MessageAttributeValue should not fail");
            (k, attr)
        })
        .collect()
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
        routing_shards: Option<u16>,
        attributes: Option<HashMap<String, MessageAttributeValue>>,
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
                .message_deduplication_id(content_dedup_id(payload));

            if let Some(shards) = routing_shards {
                let shard = compute_shard(gid, shards);
                let shard_attr = MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(shard.to_string())
                    .build()
                    .expect("building shard MessageAttributeValue should not fail");
                req = req.message_attributes("shard", shard_attr);
            }
        }

        if let Some(attrs) = attributes {
            for (k, v) in attrs {
                req = req.message_attributes(k, v);
            }
        }

        req.send().await.map_err(|e| {
            metrics::record_backend_error(
                metrics::BackendLabel::SnsSqs,
                metrics::BackendErrorKind::Publish,
            );
            ShoveError::Connection(format!("SNS publish failed: {e}"))
        })?;

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

        let routing_shards = match (topology.sequencing(), &group_id) {
            (Some(seq), Some(_)) => Some(seq.routing_shards()),
            _ => None,
        };

        let attributes = headers.map(hashmap_to_message_attributes);

        debug!(queue_name, topic_arn, "publishing message to SNS");

        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
        let mut last_err = None;

        for attempt in 0..3u32 {
            match self
                .publish_single(
                    &topic_arn,
                    &payload,
                    group_id.as_deref(),
                    routing_shards,
                    attributes.clone(),
                )
                .await
            {
                Ok(()) => {
                    debug!(queue_name, "message published to SNS");
                    return Ok(());
                }
                Err(e) => {
                    warn!(queue_name, attempt, error = %e, "SNS publish failed, retrying");
                    last_err = Some(e);
                    if attempt < 2 {
                        let delay = backoff.next().expect("backoff is infinite");
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_err.expect("loop ran at least once"))
    }
}

impl SnsPublisher {
    pub async fn publish<T: Topic>(&self, message: &T::Message) -> Result<()> {
        self.do_publish::<T>(message, None).await
    }

    pub async fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        validate_headers(&headers)?;
        self.do_publish::<T>(message, Some(headers)).await
    }

    pub async fn publish_batch<T: Topic>(&self, messages: &[T::Message]) -> (u64, Result<()>) {
        let topology = T::topology();
        let key_fn = T::SEQUENCE_KEY_FN;

        // Serialize all messages up front for fail-fast behaviour.
        let serialized: Result<Vec<String>> = messages
            .iter()
            .map(|m| serde_json::to_string(m).map_err(ShoveError::Serialization))
            .collect();

        // Pre-compute routing keys while we still have access to messages.
        let routing_keys: Option<Vec<String>> = key_fn.map(|kf| messages.iter().map(kf).collect());

        let payloads = match serialized {
            Ok(v) => v,
            Err(e) => return (0, Err(e)),
        };
        let queue_name = topology.queue();
        let topic_arn = match self.resolve_arn(queue_name).await {
            Ok(arn) => arn,
            Err(e) => return (0, Err(e)),
        };

        let has_sequencing = topology.sequencing().is_some();

        if has_sequencing && routing_keys.is_none() {
            return (
                0,
                Err(ShoveError::Topology(
                    "topic has sequencing config but no SEQUENCE_KEY_FN defined".to_string(),
                )),
            );
        }

        debug!(
            queue_name,
            count = payloads.len(),
            "publishing batch to SNS"
        );

        // Build batch entries
        let entries: Vec<PublishBatchRequestEntry> = payloads
            .iter()
            .enumerate()
            .map(|(i, payload)| {
                let mut entry = PublishBatchRequestEntry::builder()
                    .id(i.to_string())
                    .message(payload);

                if let Some(ref keys) = routing_keys {
                    entry = entry
                        .message_group_id(&keys[i])
                        .message_deduplication_id(content_dedup_id(payload));

                    if let Some(seq) = topology.sequencing() {
                        let shard = compute_shard(&keys[i], seq.routing_shards());
                        let shard_attr = MessageAttributeValue::builder()
                            .data_type("String")
                            .string_value(shard.to_string())
                            .build()
                            .expect("building shard MessageAttributeValue should not fail");
                        entry = entry.message_attributes("shard", shard_attr);
                    }
                }

                entry
                    .build()
                    .expect("building PublishBatchRequestEntry should not fail")
            })
            .collect();

        // Chunk into groups of 10 and send. Track the per-chunk outcome so
        // the wrapper can record accurate per-message counters even on partial
        // failure — the API-level `Result<()>` collapses the success/failure
        // split that SNS actually reports.
        let mut succeeded: u64 = 0;
        let mut first_err: Option<ShoveError> = None;
        for chunk in entries.chunks(SNS_BATCH_LIMIT) {
            let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
            let mut chunk_err: Option<ShoveError> = None;
            let mut chunk_succeeded: u64 = 0;

            for attempt in 0..3u32 {
                match self
                    .client
                    .inner()
                    .publish_batch()
                    .topic_arn(&topic_arn)
                    .set_publish_batch_request_entries(Some(chunk.to_vec()))
                    .send()
                    .await
                {
                    Ok(result) => {
                        let failed = result.failed();
                        chunk_succeeded = (chunk.len() - failed.len()) as u64;
                        if !failed.is_empty() {
                            metrics::record_backend_error(
                                metrics::BackendLabel::SnsSqs,
                                metrics::BackendErrorKind::Publish,
                            );
                            chunk_err = Some(ShoveError::Connection(format!(
                                "SNS batch publish: {} of {} messages failed. First error: {} (code: {})",
                                failed.len(),
                                chunk.len(),
                                failed[0].message().unwrap_or("unknown"),
                                failed[0].code(),
                            )));
                            // Partial failures are not transient — don't retry
                            break;
                        }
                        chunk_err = None;
                        break;
                    }
                    Err(e) => {
                        metrics::record_backend_error(
                            metrics::BackendLabel::SnsSqs,
                            metrics::BackendErrorKind::Publish,
                        );
                        let err = ShoveError::Connection(format!("SNS batch publish failed: {e}"));
                        warn!(queue_name, attempt, error = %err, "SNS batch chunk failed, retrying");
                        chunk_err = Some(err);
                        chunk_succeeded = 0;
                        if attempt < 2 {
                            let delay = backoff.next().expect("backoff is infinite");
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }

            succeeded += chunk_succeeded;
            if let Some(err) = chunk_err {
                first_err = Some(err);
                break;
            }
        }

        match first_err {
            Some(err) => (succeeded, Err(err)),
            None => {
                debug!(queue_name, count = payloads.len(), "batch published to SNS");
                (succeeded, Ok(()))
            }
        }
    }
}

impl PublisherImpl for SnsPublisher {
    fn publish<T: Topic>(&self, msg: &T::Message) -> impl Future<Output = Result<()>> + Send {
        SnsPublisher::publish::<T>(self, msg)
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send {
        SnsPublisher::publish_with_headers::<T>(self, msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl Future<Output = (u64, Result<()>)> + Send {
        SnsPublisher::publish_batch::<T>(self, msgs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn fnv1a_64_deterministic() {
        assert_eq!(fnv1a_64(b"hello"), fnv1a_64(b"hello"));
    }

    #[test]
    fn fnv1a_64_different_inputs_differ() {
        assert_ne!(fnv1a_64(b"hello"), fnv1a_64(b"world"));
    }

    #[test]
    fn content_dedup_id_deterministic() {
        let a = content_dedup_id(r#"{"id":1}"#);
        let b = content_dedup_id(r#"{"id":1}"#);
        assert_eq!(a, b);
    }

    #[test]
    fn content_dedup_id_different_payloads_differ() {
        let a = content_dedup_id(r#"{"id":1}"#);
        let b = content_dedup_id(r#"{"id":2}"#);
        assert_ne!(a, b);
    }

    #[test]
    fn content_dedup_id_is_16_hex_chars() {
        let id = content_dedup_id(r#"{"foo":"bar"}"#);
        assert_eq!(id.len(), 16);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn compute_shard_deterministic() {
        let a = compute_shard("order-123", 8);
        let b = compute_shard("order-123", 8);
        assert_eq!(a, b);
    }

    #[test]
    fn compute_shard_within_range() {
        for i in 0..100 {
            let key = format!("key-{i}");
            let shard = compute_shard(&key, 4);
            assert!(shard < 4, "shard {shard} out of range for 4 shards");
        }
    }

    #[test]
    fn compute_shard_distributes() {
        let mut counts = [0u32; 8];
        for i in 0..1000 {
            let shard = compute_shard(&format!("key-{i}"), 8) as usize;
            counts[shard] += 1;
        }
        for (i, count) in counts.iter().enumerate() {
            assert!(
                *count > 50,
                "shard {i} only got {count} messages out of 1000"
            );
        }
    }
}
