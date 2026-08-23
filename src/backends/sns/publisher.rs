use aws_sdk_sns::config::http::HttpResponse;
use aws_sdk_sns::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_sns::types::{MessageAttributeValue, PublishBatchRequestEntry};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use crate::backend::PublisherImpl;
use crate::backends::sns::client::SnsClient;
use crate::backends::sns::topology::TopicRegistry;
use crate::batch::BatchReport;
use crate::error::{Result, ShoveError};
use crate::metrics;
use crate::publisher_internal::{fnv1a_64, shard_for_key, validate_headers};
use crate::retry::Backoff;
use crate::topic::Topic;

/// Maximum number of messages in a single SNS PublishBatch call.
const SNS_BATCH_LIMIT: usize = 10;

/// Derive a deterministic SNS `MessageDeduplicationId` from the serialised
/// payload.  Using the same ID for every attempt of the same payload means
/// SNS FIFO can deduplicate within its 5-minute window even when a publish
/// is retried after a network error (where the first attempt may have
/// already landed at the broker).
fn content_dedup_id(payload: &str) -> String {
    format!("{:016x}", fnv1a_64(payload.as_bytes()))
}

/// Whether an SNS service-error code represents a transient failure worth
/// retrying.
///
/// These are AWS *wire* codes as returned by [`ProvideErrorMetadata::code`],
/// which for SNS's query protocol are the short forms — NOT the Rust variant
/// names (`InternalErrorException`) and NOT the SQS/JSON-protocol codes
/// (`RequestThrottled`, `OverLimit`, …). `InternalError` and `KMSThrottling`
/// are modeled in the `Publish` error set (see the SDK's
/// `protocol_serde::shape_publish` deserializer); `Throttling`/
/// `ThrottlingException` cover generic request throttling that surfaces
/// unmodeled. Everything else (authorization, invalid parameters, topic not
/// found) is permanent.
fn is_transient_sns_code(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("InternalError" | "KMSThrottling" | "Throttling" | "ThrottlingException")
    )
}

/// Maps an SNS `SdkError` to the appropriate `ShoveError` variant.
///
/// Transport-level failures (timeout, dispatch, response parse) are transient →
/// `Connection`; construction failures are code/config bugs → `Topology`;
/// service errors are classified by their AWS wire code via
/// [`is_transient_sns_code`] so the publish loop can stop retrying permanent
/// failures early.
fn map_sns_error<E>(context: &str, e: SdkError<E, HttpResponse>) -> ShoveError
where
    E: std::fmt::Debug + std::fmt::Display + ProvideErrorMetadata,
{
    match &e {
        // Transient transport-level errors
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            ShoveError::Connection(format!("{context}: {e}"))
        }
        // Construction failures are config/code bugs — permanent
        SdkError::ConstructionFailure(_) => ShoveError::Topology(format!("{context}: {e}")),
        // Service errors — classify by AWS wire code
        SdkError::ServiceError(se) => {
            if is_transient_sns_code(ProvideErrorMetadata::code(se.err())) {
                ShoveError::Connection(format!("{context}: {e}"))
            } else {
                ShoveError::Topology(format!("{context}: {e}"))
            }
        }
        // SdkError is #[non_exhaustive]; all current variants are handled above.
        _ => ShoveError::Unknown(format!("unrecognized AWS SDK error in {context}: {e}")),
    }
}

/// Convert a `HashMap<String, String>` into SNS message attributes.
fn hashmap_to_message_attributes(
    headers: HashMap<String, String>,
) -> Result<HashMap<String, MessageAttributeValue>> {
    headers
        .into_iter()
        .map(|(k, v)| {
            let attr = MessageAttributeValue::builder()
                .data_type("String")
                .string_value(v)
                .build()
                .map_err(|e| {
                    ShoveError::Validation(format!("invalid message attribute '{k}': {e}"))
                })?;
            Ok((k, attr))
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
                let shard = shard_for_key(gid, shards);
                let shard_attr = MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(shard.to_string())
                    .build()
                    .map_err(|e| ShoveError::Validation(format!("invalid shard attribute: {e}")))?;
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
            map_sns_error("SNS publish failed", e)
        })?;

        Ok(())
    }

    async fn do_publish<T: Topic>(
        &self,
        message: &T::Message,
        headers: Option<HashMap<String, String>>,
    ) -> Result<()> {
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_to_string(message)?;
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

        let attributes = headers.map(hashmap_to_message_attributes).transpose()?;

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
                    // Permanent failures (authorization, invalid parameters,
                    // topic not found) cannot succeed on retry — surface
                    // immediately instead of sleeping through the remaining
                    // attempts.
                    if !e.is_retryable() {
                        return Err(e);
                    }
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

    /// Sparse semantics: SNS names the entries it rejects, and every entry is
    /// stamped with its **global** index in `messages`, so a rejected entry
    /// maps straight back to the record the caller passed in.
    ///
    /// Two coarser failure modes exist and both resolve to *unattempted*
    /// rather than *failed*, because neither tells us per-record what
    /// happened: a chunk whose `PublishBatch` call errors outright, and every
    /// chunk after the one that broke the loop.
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
        let topology = T::topology();
        let key_fn = T::SEQUENCE_KEY_FN;

        // Serialize all messages up front for fail-fast behaviour.
        let serialized: Result<Vec<String>> = messages
            .iter()
            .map(<T::Codec as crate::Codec<T::Message>>::encode_to_string)
            .collect();

        // Pre-compute routing keys while we still have access to messages.
        let routing_keys: Option<Vec<String>> = key_fn.map(|kf| messages.iter().map(kf).collect());

        let total = messages.len();
        let payloads = match serialized {
            Ok(v) => v,
            Err(e) => return BatchReport::wholly_unattempted(total, e),
        };
        let queue_name = topology.queue();
        let topic_arn = match self.resolve_arn(queue_name).await {
            Ok(arn) => arn,
            Err(e) => return BatchReport::wholly_unattempted(total, e),
        };

        let has_sequencing = topology.sequencing().is_some();

        if has_sequencing && routing_keys.is_none() {
            return BatchReport::wholly_unattempted(
                total,
                ShoveError::Topology(
                    "topic has sequencing config but no SEQUENCE_KEY_FN defined".to_string(),
                ),
            );
        }

        debug!(
            queue_name,
            count = payloads.len(),
            "publishing batch to SNS"
        );

        // Build batch entries
        let entries = payloads
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
                        let shard = shard_for_key(&keys[i], seq.routing_shards());
                        let shard_attr = MessageAttributeValue::builder()
                            .data_type("String")
                            .string_value(shard.to_string())
                            .build()
                            .map_err(|e| {
                                ShoveError::Validation(format!("invalid shard attribute: {e}"))
                            })?;
                        entry = entry.message_attributes("shard", shard_attr);
                    }
                }

                entry
                    .build()
                    .map_err(|e| ShoveError::Validation(format!("invalid batch entry {i}: {e}")))
            })
            .collect::<Result<Vec<_>>>();
        let entries = match entries {
            Ok(v) => v,
            Err(e) => return BatchReport::wholly_unattempted(total, e),
        };

        // Chunk into groups of 10 and send. Track which *indices* each chunk
        // resolved so the wrapper can record accurate per-message counters and
        // hand the caller exact re-publish indices — the API-level
        // `Result<()>` collapses the split that SNS actually reports.
        let mut failed_idx: Vec<usize> = Vec::new();
        let mut unattempted_idx: Vec<usize> = Vec::new();
        let mut first_err: Option<ShoveError> = None;
        let mut chunk_start: usize = 0;
        for chunk in entries.chunks(SNS_BATCH_LIMIT) {
            // Entry ids are the global index (`.id(i.to_string())` above), so
            // this range is exactly the records in this chunk.
            let chunk_range = chunk_start..chunk_start.saturating_add(chunk.len());
            chunk_start = chunk_range.end;

            let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
            let mut chunk_err: Option<ShoveError> = None;
            // Indices this chunk resolved as rejected, and indices it left
            // unresolved. Exactly one of the two is populated per outcome.
            let mut chunk_failed: Vec<usize> = Vec::new();
            let mut chunk_unresolved: Vec<usize> = Vec::new();

            for attempt in 0..3u32 {
                chunk_failed.clear();
                chunk_unresolved.clear();
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
                            match parse_failed_indices(failed.iter().map(|f| f.id()), &chunk_range)
                            {
                                Some(idx) => chunk_failed = idx,
                                // SNS named an entry we can't map back to a
                                // record. Rather than guess, treat the whole
                                // chunk as outstanding.
                                None => {
                                    warn!(
                                        queue_name,
                                        "SNS named a failed entry id outside this chunk; \
                                         re-publishing the whole chunk"
                                    );
                                    chunk_unresolved = chunk_range.clone().collect();
                                }
                            }
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
                        let err = map_sns_error("SNS batch publish failed", e);
                        // The call itself failed, so SNS reported nothing
                        // per-record: every entry in the chunk is outstanding.
                        chunk_unresolved = chunk_range.clone().collect();
                        // Permanent failures (auth, invalid params, topic not
                        // found) can't succeed on retry — stop early.
                        if !err.is_retryable() {
                            chunk_err = Some(err);
                            break;
                        }
                        warn!(queue_name, attempt, error = %err, "SNS batch chunk failed, retrying");
                        chunk_err = Some(err);
                        if attempt < 2 {
                            let delay = backoff.next().expect("backoff is infinite");
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }

            failed_idx.append(&mut chunk_failed);
            unattempted_idx.append(&mut chunk_unresolved);
            if let Some(err) = chunk_err {
                first_err = Some(err);
                // The loop stops here, so every remaining chunk is untouched.
                unattempted_idx.extend(chunk_range.end..total);
                break;
            }
        }

        if first_err.is_none() {
            debug!(queue_name, count = payloads.len(), "batch published to SNS");
            return BatchReport::all_succeeded();
        }
        BatchReport::sparse(failed_idx, unattempted_idx, first_err)
    }
}

/// Map SNS's failed-entry ids back to indices in the original `messages`
/// slice.
///
/// Entries are stamped with their global index, so the id is the index. Returns
/// `None` if any id is unparseable or falls outside the chunk it came from —
/// the caller then treats the whole chunk as outstanding rather than trusting
/// a mapping it can't verify.
fn parse_failed_indices<'a>(
    ids: impl Iterator<Item = &'a str>,
    chunk_range: &std::ops::Range<usize>,
) -> Option<Vec<usize>> {
    let mut out = Vec::new();
    for id in ids {
        let idx: usize = id.parse().ok()?;
        if !chunk_range.contains(&idx) {
            return None;
        }
        out.push(idx);
    }
    out.sort_unstable();
    Some(out)
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
    ) -> impl Future<Output = BatchReport> + Send {
        SnsPublisher::publish_batch_report::<T>(self, msgs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_sns_codes_are_retryable() {
        for code in [
            "InternalError",
            "KMSThrottling",
            "Throttling",
            "ThrottlingException",
        ] {
            assert!(
                is_transient_sns_code(Some(code)),
                "{code} should be classified transient"
            );
        }
    }

    #[test]
    fn permanent_sns_codes_are_not_retryable() {
        for code in [
            "AuthorizationError",
            "InvalidParameter",
            "ParameterValueInvalid",
            "NotFound",
            "EndpointDisabled",
            "KMSAccessDenied",
        ] {
            assert!(
                !is_transient_sns_code(Some(code)),
                "{code} should be classified permanent"
            );
        }
        assert!(!is_transient_sns_code(None));
    }

    // `ProvideErrorMetadata::code()` returns the AWS wire code, not the Rust
    // variant name. Guard against regressing to the variant names (or to the
    // SQS/JSON-protocol codes), which never match a real SNS Publish error and
    // would silently make transient failures permanent.
    #[test]
    fn rust_variant_names_and_sqs_codes_do_not_match() {
        for code in [
            "InternalErrorException",  // Rust variant, not the wire code
            "KMSThrottlingException",  // Rust variant, not the wire code
            "ThrottledException",      // not in the Publish error set
            "RequestThrottled",        // SQS code
            "OverLimit",               // SQS code
            "KMS.ThrottlingException", // SQS code
        ] {
            assert!(
                !is_transient_sns_code(Some(code)),
                "{code} is not a real SNS Publish wire code and must not match"
            );
        }
    }

    #[test]
    fn hashmap_to_message_attributes_empty() {
        let attrs = hashmap_to_message_attributes(HashMap::new()).unwrap();
        assert!(attrs.is_empty());
    }

    #[test]
    fn hashmap_to_message_attributes_single() {
        let mut map = HashMap::new();
        map.insert("x-trace-id".to_string(), "abc123".to_string());
        let attrs = hashmap_to_message_attributes(map).unwrap();
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
        let attrs = hashmap_to_message_attributes(map).unwrap();
        assert_eq!(attrs.len(), 3);
        assert!(attrs.contains_key("key-a"));
        assert!(attrs.contains_key("key-b"));
        assert!(attrs.contains_key("key-c"));
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
}
