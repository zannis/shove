use std::collections::HashMap;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::header::NATS_MESSAGE_ID;
use async_nats::jetstream;
use bytes::Bytes;
use uuid::Uuid;

use crate::backend::PublisherImpl;
use crate::batch::BatchReport;
use crate::error::Result;
use crate::metrics;
use crate::publisher_internal::{shard_for_key, validate_headers};
use crate::retry::Backoff;
use crate::topic::Topic;
use crate::{QueueTopology, ShoveError};

use super::client::NatsClient;
use super::constants::{RETRY_COUNT_HEADER, SEQUENCE_KEY_HEADER};

const MAX_PUBLISH_ATTEMPTS: u32 = 3;

/// Publish a message to JetStream with retry on transient failures.
/// Shared by both the publisher and consumer (for DLQ publishes).
pub(super) async fn publish_with_retry(
    js: &jetstream::Context,
    subject: String,
    headers: HeaderMap,
    payload: Bytes,
    max_attempts: u32,
    label: &str,
) -> Result<()> {
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));

    for attempt in 1..=max_attempts {
        match js
            .publish_with_headers(subject.clone(), headers.clone(), payload.clone())
            .await
        {
            Ok(ack_future) => match ack_future.await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt == max_attempts {
                        metrics::record_backend_error(
                            metrics::BackendLabel::Nats,
                            metrics::BackendErrorKind::Publish,
                        );
                        return Err(ShoveError::Connection(format!(
                            "{label} ack failed after {max_attempts} attempts: {e}"
                        )));
                    }
                    let delay = backoff.next().unwrap_or(Duration::from_secs(2));
                    tracing::warn!(attempt, error = %e, "{label} ack failed, retrying");
                    tokio::time::sleep(delay).await;
                }
            },
            Err(e) => {
                if attempt == max_attempts {
                    metrics::record_backend_error(
                        metrics::BackendLabel::Nats,
                        metrics::BackendErrorKind::Publish,
                    );
                    return Err(ShoveError::Connection(format!(
                        "{label} failed after {max_attempts} attempts: {e}"
                    )));
                }
                let delay = backoff.next().unwrap_or(Duration::from_secs(2));
                tracing::warn!(attempt, error = %e, "{label} failed, retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }

    unreachable!()
}

#[derive(Clone)]
pub struct NatsPublisher {
    client: NatsClient,
}

impl NatsPublisher {
    pub async fn new(client: NatsClient) -> Result<Self> {
        Ok(Self { client })
    }

    /// Resolve the publish subject and, on a sequenced topic, the sequence key.
    ///
    /// The key is returned (not just hashed into the subject) because the
    /// consumer needs it to implement `SequenceFailure::FailAll` — the subject
    /// only identifies the shard, and a shard holds many keys.
    fn resolve_subject_and_key<T: Topic>(
        topology: &'static QueueTopology,
        message: &T::Message,
    ) -> (String, Option<String>) {
        if let Some(seq) = topology.sequencing()
            && let Some(key_fn) = T::SEQUENCE_KEY_FN
        {
            let key = key_fn(message);
            let shard = shard_for_key(&key, seq.routing_shards());
            return (format!("{}.shard.{shard}", topology.queue()), Some(key));
        }
        (topology.queue().to_string(), None)
    }

    fn build_headers(extra: Option<&HashMap<String, String>>, seq_key: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MESSAGE_ID, Uuid::new_v4().to_string().as_str());
        headers.insert(RETRY_COUNT_HEADER, "0");

        if let Some(extra) = extra {
            for (k, v) in extra {
                headers.insert(k.as_str(), v.as_str());
            }
        }

        // After the user headers: the sequence key drives FailAll poisoning, so
        // a caller must not be able to overwrite it. (`validate_headers` guards
        // the `x-` internal names shared with the other backends, but not the
        // `Shove-` names this backend uses.)
        if let Some(key) = seq_key {
            headers.insert(SEQUENCE_KEY_HEADER, key);
        }

        headers
    }

    async fn publish_raw(&self, subject: String, headers: HeaderMap, payload: Bytes) -> Result<()> {
        publish_with_retry(
            self.client.jetstream(),
            subject,
            headers,
            payload,
            MAX_PUBLISH_ATTEMPTS,
            "publish",
        )
        .await
    }
}

impl NatsPublisher {
    pub async fn publish<T: Topic>(&self, message: &T::Message) -> Result<()> {
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;
        let topology = T::topology();
        let (subject, seq_key) = Self::resolve_subject_and_key::<T>(topology, message);
        let headers = Self::build_headers(None, seq_key.as_deref());
        self.publish_raw(subject, headers, payload).await
    }

    pub async fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        extra_headers: HashMap<String, String>,
    ) -> Result<()> {
        validate_headers(&extra_headers)?;
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;
        let topology = T::topology();
        let (subject, seq_key) = Self::resolve_subject_and_key::<T>(topology, message);
        let headers = Self::build_headers(Some(&extra_headers), seq_key.as_deref());
        self.publish_raw(subject, headers, payload).await
    }

    /// Mixed semantics: ack failures are exact over the submitted prefix, and
    /// a submission break leaves an unattempted tail behind it.
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
        let prepared: Result<Vec<(String, HeaderMap, Bytes)>> = messages
            .iter()
            .map(|msg| {
                let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(msg)?;
                let (subject, seq_key) = Self::resolve_subject_and_key::<T>(topology, msg);
                let headers = Self::build_headers(None, seq_key.as_deref());
                Ok((subject, headers, payload))
            })
            .collect();
        let prepared = match prepared {
            Ok(v) => v,
            Err(e) => return BatchReport::wholly_unattempted(messages.len(), e),
        };
        // `prepared` is a 1:1 map over `messages`, so indices line up with the
        // caller's slice — which is what `resolve` is handed.
        let total = messages.len();
        debug_assert_eq!(prepared.len(), total);

        // Fire all publishes, then await all acks — O(1 RTT) instead of O(N RTT).
        // Submission and ack are tracked separately so the wrapper can
        // attribute partial-failure counters to what NATS actually accepted
        // before we surface the first error. Each ack carries its own index so
        // a sparse ack failure is reported at the record it belongs to rather
        // than collapsed into a count.
        let mut ack_futures = Vec::with_capacity(total);
        // No `failed` counterpart: nothing on the submission side can produce an
        // explicit rejection, because the record has not reached the server yet.
        let mut unattempted: Vec<usize> = Vec::new();
        let mut first_err: Option<ShoveError> = None;
        for (i, (subject, headers, payload)) in prepared.into_iter().enumerate() {
            match self
                .client
                .jetstream()
                .publish_with_headers(subject, headers, payload)
                .await
            {
                Ok(ack) => ack_futures.push((i, ack)),
                Err(e) => {
                    metrics::record_backend_error(
                        metrics::BackendLabel::Nats,
                        metrics::BackendErrorKind::Publish,
                    );
                    first_err = Some(ShoveError::Connection(format!("batch publish failed: {e}")));
                    // `publish_with_headers` returns before the record is on
                    // the wire: it fails on subject validation, the payload-size
                    // check, ack-permit acquisition, or the command channel. So
                    // index `i` never reached the broker either, and belongs
                    // with the tail behind it rather than in `failed`, which is
                    // reserved for records the server explicitly rejected.
                    unattempted.extend(i..total);
                    break;
                }
            }
        }

        // Drain every already-submitted ack even if submission broke early:
        // those messages were accepted by NATS and must be counted, not
        // abandoned. A submission error takes precedence in `first_err`; an
        // ack error only replaces it when nothing has failed yet.
        let mut ack_rejected: Vec<usize> = Vec::new();
        let mut ack_unconfirmed: Vec<usize> = Vec::new();
        for (i, ack) in ack_futures {
            if let Err(e) = ack.await {
                metrics::record_backend_error(
                    metrics::BackendLabel::Nats,
                    metrics::BackendErrorKind::Publish,
                );
                if ack_error_is_explicit_rejection(&e) {
                    ack_rejected.push(i);
                } else {
                    ack_unconfirmed.push(i);
                }
                if first_err.is_none() {
                    first_err = Some(ShoveError::Connection(format!(
                        "batch publish ack failed: {e}"
                    )));
                }
            }
        }
        if first_err.is_none() {
            return BatchReport::all_succeeded();
        }
        // Ack results all sit below the submission break, so prepending them
        // keeps the set ascending.
        ack_unconfirmed.extend(unattempted);
        BatchReport::sparse(ack_rejected, ack_unconfirmed, first_err)
    }
}

/// Whether a failed publish-ack means the server definitely rejected the record.
///
/// The batch contract splits by what the backend *confirmed*: `failed` is
/// "attempted and explicitly rejected", `unattempted` is "submitted without a
/// resolution the backend could confirm". An ack that timed out or died with
/// the connection is the second kind — the server may well have stored the
/// message — so reporting it as an explicit rejection overstates what is known.
/// Both sets are re-published either way; only the diagnosis differs.
///
/// Takes the whole error rather than its kind, because the kind alone cannot
/// answer the question for `Other`.
fn ack_error_is_explicit_rejection(err: &jetstream::context::PublishError) -> bool {
    use jetstream::context::PublishErrorKind as K;
    match err.kind() {
        // `StreamNotFound` is a `NO_RESPONDERS` status: nothing was listening
        // on the subject, so the record was certainly not stored.
        K::StreamNotFound | K::WrongLastMessageId | K::WrongLastSequence => true,
        K::TimedOut | K::BrokenPipe => false,
        // Raised before the record is submitted, so neither can reach an ack.
        // Classified anyway, and as refusals, because that is what they are.
        K::MaxPayloadExceeded | K::MaxAckPending => true,
        // `Other` is two different answers wearing one kind: a JetStream error
        // response whose code async-nats does not model — the server saying no,
        // in full — and a reply that could not be deserialized, which says
        // nothing at all. The server's own error object is what separates them.
        K::Other => {
            std::error::Error::source(err).is_some_and(|source| source.is::<jetstream::Error>())
        }
    }
}

impl PublisherImpl for NatsPublisher {
    fn publish<T: Topic>(&self, msg: &T::Message) -> impl Future<Output = Result<()>> + Send {
        NatsPublisher::publish::<T>(self, msg)
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send {
        NatsPublisher::publish_with_headers::<T>(self, msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl Future<Output = BatchReport> + Send {
        NatsPublisher::publish_batch_report::<T>(self, msgs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jetstream::context::{PublishError, PublishErrorKind as K};

    /// A JetStream error response, as async-nats deserializes one off the wire.
    fn server_error() -> jetstream::Error {
        serde_json::from_value(serde_json::json!({
            "code": 400,
            "err_code": 10071,
            "description": "maximum messages exceeded"
        }))
        .expect("a JetStream error response deserializes")
    }

    /// An ack that never arrived says nothing about whether the server stored
    /// the record, so it belongs in `unattempted` ("submitted without a
    /// resolution the backend could confirm"), not in `failed` ("attempted and
    /// explicitly reported as rejected").
    #[test]
    fn unconfirmed_acks_are_not_explicit_rejections() {
        for kind in [K::TimedOut, K::BrokenPipe] {
            assert!(
                !ack_error_is_explicit_rejection(&PublishError::new(kind)),
                "{kind:?} is ambiguous, not an explicit rejection"
            );
        }
    }

    /// `Other` is the kind async-nats gives both to a JetStream error response
    /// whose code it does not model and to a reply it could not deserialize.
    /// The first is the server rejecting the record in as many words; the
    /// second says nothing. Only the error's source separates them, so the kind
    /// alone cannot classify it.
    #[test]
    fn other_is_split_by_whether_the_server_answered() {
        assert!(
            ack_error_is_explicit_rejection(&PublishError::with_source(K::Other, server_error())),
            "a JetStream error response is the server saying no"
        );
        assert!(
            !ack_error_is_explicit_rejection(&PublishError::with_source(
                K::Other,
                std::io::Error::other("truncated reply"),
            )),
            "a reply that could not be read is not a rejection"
        );
        assert!(
            !ack_error_is_explicit_rejection(&PublishError::new(K::Other)),
            "`Other` with no source at all resolves nothing"
        );
    }

    /// The classifier only decides which set an index lands in; this pins what
    /// the caller actually receives once the publisher's two sets are resolved
    /// into a report — including the invariant that makes the split safe to act
    /// on. Mirrors `publish_batch_report`'s tail: ack results first, then the
    /// submission break's `failed` / `unattempted`.
    #[test]
    fn an_unconfirmed_ack_is_reported_as_unattempted_not_failed() {
        let total = 6;
        // Indices 0..4 were submitted: 1 was rejected outright, 2's ack never
        // resolved. Submission then broke at 4, leaving 5 unattempted.
        let mut ack_rejected = vec![1];
        let mut ack_unconfirmed = vec![2];
        ack_rejected.extend(vec![4]);
        ack_unconfirmed.extend(vec![5]);

        let out = BatchReport::sparse(
            ack_rejected,
            ack_unconfirmed,
            Some(ShoveError::Connection("ack timed out".into())),
        )
        .resolve(total);

        let Err(ShoveError::PartialBatch(f)) = out.result else {
            panic!("a batch with confirmed and unresolved records is partial");
        };
        assert_eq!(f.failed(), &[1, 4], "only explicit rejections are `failed`");
        assert_eq!(
            f.unattempted(),
            &[2, 5],
            "an unresolved ack sits with the never-submitted tail"
        );
        assert_eq!(f.to_republish(), &[1, 2, 4, 5]);
        assert_eq!(f.succeeded(), 2);
        assert_eq!(
            f.succeeded() + f.failed().len() + f.unattempted().len(),
            total
        );
        assert_eq!(f.succeeded() + f.to_republish().len(), total);
    }

    #[test]
    fn server_rejections_are_explicit() {
        for kind in [
            K::StreamNotFound,
            K::WrongLastMessageId,
            K::WrongLastSequence,
            K::MaxPayloadExceeded,
            K::MaxAckPending,
        ] {
            assert!(
                ack_error_is_explicit_rejection(&PublishError::new(kind)),
                "{kind:?} is the server saying no"
            );
        }
    }

    /// Nothing on the submission side can be an explicit rejection:
    /// `publish_with_headers` returns before the record is on the wire, failing
    /// on subject validation, the payload-size check, the ack permit or the
    /// command channel. So the record it broke on joins the tail behind it.
    #[test]
    fn a_submission_break_leaves_its_own_record_unattempted() {
        let total = 5;
        // Submission broke at index 2; indices 0 and 1 were submitted and their
        // acks came back clean.
        let out = BatchReport::sparse(
            Vec::new(),
            (2..total).collect(),
            Some(ShoveError::Connection("channel closed".into())),
        )
        .resolve(total);

        let Err(ShoveError::PartialBatch(f)) = out.result else {
            panic!("two confirmed and three unresolved records is a partial batch");
        };
        assert!(
            f.failed().is_empty(),
            "a record that never left the process was not rejected by anyone"
        );
        assert_eq!(f.unattempted(), &[2, 3, 4]);
        assert_eq!(f.to_republish(), &[2, 3, 4]);
        assert_eq!(f.succeeded(), 2);
        assert_eq!(f.succeeded() + f.to_republish().len(), total);
    }
}
