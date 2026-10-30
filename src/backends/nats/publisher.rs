use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::header::{HeaderName, HeaderValue, NATS_MESSAGE_ID};
use async_nats::jetstream;
use async_nats::jetstream::publish::PublishAck;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use uuid::{Builder, Uuid};

use crate::backend::PublisherImpl;
use crate::batch::BatchReport;
use crate::error::Result;
use crate::metrics;
use crate::publisher_internal::{shard_for_key, validate_headers};
use crate::retry::Backoff;
use crate::topic::Topic;
use crate::{QueueTopology, ShoveError};

use super::client::NatsClient;
use super::constants::{COPY_OF_HEADER, SEQUENCE_KEY_HEADER};

const MAX_PUBLISH_ATTEMPTS: u32 = 3;

/// The `Nats-Msg-Id` of a fresh publish: a version 4 UUID built from random
/// bytes the thread-local CSPRNG draws.
///
/// JetStream de-duplicates on the id, so a party that can publish to the stream
/// and predict an id could publish it ahead of the message and have the message
/// dropped as a duplicate. A CSPRNG draw gives away nothing about the next one,
/// and the generator reseeds from the OS now and then, so a publish makes no
/// call to the OS of its own. The id is a plain string, so a retry copy's
/// `-r{n}` and a dead letter's `-dlq` extend it as they extend any id.
///
/// `rand`'s `ThreadRng` is not reseeded on `fork`, so a child process forked
/// from a thread that has drawn ids repeats the ids that thread draws next,
/// and JetStream drops the later publish of a pair inside its duplicate
/// window.
fn new_message_id() -> Uuid {
    Builder::from_random_bytes(rand::random()).into_uuid()
}

/// How long the original of a copy waits before it is redelivered when the
/// copy's publish ended on a duplicate that could not be confirmed. The id
/// stays held until JetStream's duplicate window lets it go, so an immediate
/// redelivery would only run the handler again to find it held.
const UNCONFIRMED_COPY_REDELIVERY: Duration = Duration::from_secs(1);

/// Why [`publish_with_retry`] failed.
#[derive(Debug, thiserror::Error)]
pub(super) enum PublishFailure {
    /// The publish or its ack failed, or the message could not be built.
    #[error(transparent)]
    Failed(#[from] ShoveError),
    /// JetStream answered a copy's publish with a duplicate that could not be
    /// confirmed as that copy: another message holds its id, or the stored
    /// copy could not be read back.
    #[error(transparent)]
    Unconfirmed(ShoveError),
}

/// What [`publish_with_retry`] returns.
pub(super) type PublishResult = std::result::Result<(), PublishFailure>;

impl PublishFailure {
    /// The redelivery delay for the original whose copy failed: `None` for an
    /// immediate redelivery, which is what every other failure gets.
    pub(super) fn redelivery_delay(&self) -> Option<Duration> {
        match self {
            Self::Failed(_) => None,
            Self::Unconfirmed(_) => Some(UNCONFIRMED_COPY_REDELIVERY),
        }
    }

    fn into_error(self) -> ShoveError {
        match self {
            Self::Failed(e) | Self::Unconfirmed(e) => e,
        }
    }
}

/// Publish a message to JetStream with retry on transient failures.
/// Shared by both the publisher and consumer (for DLQ publishes).
///
/// `copy_origin` is `Some` for a retry copy or a dead letter, naming the
/// delivery it was made from. A copy's id can be predicted, so a message that
/// someone else stored under it first would make the copy's publish come back
/// as a duplicate PubAck. For a copy that PubAck counts only once the stored
/// message proves to be the same copy ([`is_same_copy`]), which costs one
/// stream read per duplicate, bounded by the client's request timeout;
/// otherwise it is a failed attempt, and the last one ends in
/// [`PublishFailure::Unconfirmed`]. For a user's publish (`None`) a duplicate
/// is a success: the stream already holds a message with the caller's id. A
/// `max_attempts` of zero is a `Validation` error, since there is no attempt
/// to make.
pub(super) async fn publish_with_retry(
    js: &jetstream::Context,
    subject: String,
    headers: HeaderMap,
    payload: Bytes,
    copy_origin: Option<&str>,
    max_attempts: u32,
    label: &str,
) -> PublishResult {
    if max_attempts == 0 {
        return Err(ShoveError::Validation(format!("{label} needs at least one attempt")).into());
    }
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));

    let mut attempt: u32 = 1;
    loop {
        match js
            .publish_with_headers(subject.clone(), headers.clone(), payload.clone())
            .await
        {
            Ok(ack_future) => match ack_future.await {
                Ok(ack) => {
                    let copy = copy_origin.map(|origin| SentCopy {
                        subject: &subject,
                        origin,
                        payload: &payload,
                    });
                    match confirm_ack(js, &ack, copy).await {
                        Ok(()) => return Ok(()),
                        Err(e) => {
                            if attempt >= max_attempts {
                                metrics::record_backend_error(
                                    metrics::BackendLabel::Nats,
                                    metrics::BackendErrorKind::Publish,
                                );
                                return Err(PublishFailure::Unconfirmed(e));
                            }
                            let delay = backoff.next_delay();
                            tracing::warn!(attempt, error = %e, "{label} was not confirmed, retrying");
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
                Err(e) => {
                    if attempt >= max_attempts {
                        metrics::record_backend_error(
                            metrics::BackendLabel::Nats,
                            metrics::BackendErrorKind::Publish,
                        );
                        return Err(ShoveError::Connection(format!(
                            "{label} ack failed after {max_attempts} attempts: {e}"
                        ))
                        .into());
                    }
                    let delay = backoff.next_delay();
                    tracing::warn!(attempt, error = %e, "{label} ack failed, retrying");
                    tokio::time::sleep(delay).await;
                }
            },
            Err(e) => {
                if attempt >= max_attempts {
                    metrics::record_backend_error(
                        metrics::BackendLabel::Nats,
                        metrics::BackendErrorKind::Publish,
                    );
                    return Err(ShoveError::Connection(format!(
                        "{label} failed after {max_attempts} attempts: {e}"
                    ))
                    .into());
                }
                let delay = backoff.next_delay();
                tracing::warn!(attempt, error = %e, "{label} failed, retrying");
                tokio::time::sleep(delay).await;
            }
        }
        attempt = attempt.saturating_add(1);
    }
}

/// A copy as its publish sent it.
#[derive(Clone, Copy)]
struct SentCopy<'a> {
    subject: &'a str,
    /// The delivery it was made from, as stamped in `Shove-Copy-Of`.
    origin: &'a str,
    payload: &'a [u8],
}

/// A message as JetStream returns it from the stream.
#[derive(Clone, Copy)]
struct StoredMessage<'a> {
    subject: &'a str,
    headers: &'a HeaderMap,
    payload: &'a [u8],
}

/// `Ok` when a PubAck can be trusted: always for a fresh store or a user's
/// publish (`copy` is `None`), and for a copy's duplicate only when the
/// message JetStream already stores under the copy's id is that same copy.
async fn confirm_ack(
    js: &jetstream::Context,
    ack: &PublishAck,
    copy: Option<SentCopy<'_>>,
) -> Result<()> {
    let Some(copy) = copy else {
        return Ok(());
    };
    if !ack.duplicate {
        return Ok(());
    }
    let stored = js
        .get_stream_no_info(&ack.stream)
        .await
        .map_err(|e| ShoveError::Connection(format!("reading back stream {}: {e}", ack.stream)))?
        .get_raw_message(ack.sequence)
        .await
        .map_err(|e| {
            ShoveError::Connection(format!(
                "reading back sequence {} of stream {} to confirm a duplicate copy: {e}",
                ack.sequence, ack.stream
            ))
        })?;
    let stored = StoredMessage {
        subject: stored.subject.as_str(),
        headers: &stored.headers,
        payload: &stored.payload,
    };
    if is_same_copy(&stored, &copy) {
        Ok(())
    } else {
        Err(ShoveError::Validation(format!(
            "stream {} holds another message at sequence {} under this copy's id",
            ack.stream, ack.sequence
        )))
    }
}

/// Whether a message stored under a copy's id is that copy: it went to the
/// same subject, names the same origin in `Shove-Copy-Of` and carries the same
/// payload. The subject matters because a stream can capture several: a copy
/// on another shard subject or `nats_subjects` subject would reach another
/// key's order or another subject-filtered reader.
fn is_same_copy(stored: &StoredMessage<'_>, sent: &SentCopy<'_>) -> bool {
    stored.subject == sent.subject
        && stored.headers.get(COPY_OF_HEADER).map(HeaderValue::as_str) == Some(sent.origin)
        && stored.payload == sent.payload
}

/// `value` as a NATS header value, or a `Validation` error naming `header`
/// when it holds a CR or LF, which the client's own conversion panics on.
pub(super) fn header_value(header: &str, value: &str) -> Result<HeaderValue> {
    HeaderValue::from_str(value).map_err(|e| {
        ShoveError::Validation(format!("header {header:?} cannot carry this value: {e}"))
    })
}

/// `name` as a NATS header name, or a `Validation` error when it holds a
/// colon, whitespace, a control or a non-ASCII character, which the client's
/// own conversion panics on.
fn header_name(name: &str) -> Result<HeaderName> {
    HeaderName::from_str(name)
        .map_err(|e| ShoveError::Validation(format!("{name:?} cannot be a header name: {e}")))
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

    /// The headers a publish carries. A user header or sequence key NATS
    /// cannot carry is a `Validation` error, raised before anything is sent.
    /// A fresh message has no `Shove-Retry-Count`: only a retry copy or a dead
    /// letter carries one, and a missing count reads as zero.
    fn build_headers(
        extra: Option<&HashMap<String, String>>,
        seq_key: Option<&str>,
    ) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        let id = new_message_id();
        headers.insert(
            NATS_MESSAGE_ID,
            header_value(
                "Nats-Msg-Id",
                id.hyphenated().encode_lower(&mut Uuid::encode_buffer()),
            )?,
        );

        if let Some(extra) = extra {
            for (k, v) in extra {
                headers.insert(header_name(k)?, header_value(k, v)?);
            }
        }

        // After the user headers: the sequence key drives FailAll poisoning, so
        // a caller must not be able to overwrite it. (`validate_headers` guards
        // the `x-` internal names shared with the other backends, but not the
        // `Shove-` names this backend uses.)
        if let Some(key) = seq_key {
            headers.insert(SEQUENCE_KEY_HEADER, header_value(SEQUENCE_KEY_HEADER, key)?);
        }

        Ok(headers)
    }

    async fn publish_raw(&self, subject: String, headers: HeaderMap, payload: Bytes) -> Result<()> {
        publish_with_retry(
            self.client.jetstream(),
            subject,
            headers,
            payload,
            None,
            MAX_PUBLISH_ATTEMPTS,
            "publish",
        )
        .await
        .map_err(PublishFailure::into_error)
    }
}

impl NatsPublisher {
    pub async fn publish<T: Topic>(&self, message: &T::Message) -> Result<()> {
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_bytes(message)?;
        let topology = T::topology();
        let (subject, seq_key) = Self::resolve_subject_and_key::<T>(topology, message);
        let headers = Self::build_headers(None, seq_key.as_deref())?;
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
        let headers = Self::build_headers(Some(&extra_headers), seq_key.as_deref())?;
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
                let headers = Self::build_headers(None, seq_key.as_deref())?;
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

        // Submit and settle concurrently. async-nats caps a JetStream context
        // at 5,000 in-flight acks and, by default, blocks a publish until a
        // permit frees; a permit is released only when its ack future is
        // polled to completion or dropped. Submitting every record before
        // awaiting any ack therefore deadlocks as soon as the callers sharing
        // this client hold more than the cap between them: each waits for a
        // permit that only its own un-polled acks can release. So every
        // publish is driven alongside the acks already pending, and the
        // pending set is drained once submission ends. Submission and ack are
        // still tracked separately so the wrapper can attribute
        // partial-failure counters to what NATS actually accepted, and each
        // ack carries its own index so a sparse failure is reported at the
        // record it belongs to rather than collapsed into a count.
        let js = self.client.jetstream();
        let mut pending = FuturesUnordered::new();
        let mut ack_rejected: Vec<usize> = Vec::new();
        let mut ack_unconfirmed: Vec<usize> = Vec::new();
        // No `failed` counterpart on the submission side: nothing there can
        // produce an explicit rejection, because the record has not reached
        // the server yet.
        let mut unattempted: Vec<usize> = Vec::new();
        let mut first_err: Option<ShoveError> = None;
        for (i, (subject, headers, payload)) in prepared.into_iter().enumerate() {
            // Pinned outside the select loop so an ack landing first does not
            // cancel a publish that may already be on the wire.
            let mut publish = std::pin::pin!(js.publish_with_headers(subject, headers, payload));
            let submitted = loop {
                tokio::select! {
                    submitted = &mut publish => break submitted,
                    Some((settled, outcome)) = pending.next(), if !pending.is_empty() => {
                        settle_ack(settled, outcome, &mut ack_rejected, &mut ack_unconfirmed, &mut first_err);
                    }
                }
            };
            match submitted {
                Ok(ack) => pending.push(async move { (i, ack.await.map(drop)) }),
                Err(e) => {
                    metrics::record_backend_error(
                        metrics::BackendLabel::Nats,
                        metrics::BackendErrorKind::Publish,
                    );
                    // A submission error takes precedence over any ack error
                    // already recorded.
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
        // Drain every submitted ack even if submission broke early: those
        // messages were accepted by NATS and must be counted, not abandoned.
        while let Some((settled, outcome)) = pending.next().await {
            settle_ack(
                settled,
                outcome,
                &mut ack_rejected,
                &mut ack_unconfirmed,
                &mut first_err,
            );
        }
        if first_err.is_none() {
            return BatchReport::all_succeeded();
        }
        ack_unconfirmed.extend(unattempted);
        BatchReport::sparse(ack_rejected, ack_unconfirmed, first_err)
    }
}

/// Record one settled publish ack for the batch report. A submission error
/// already in `first_err` keeps precedence; an ack error only fills an empty
/// slot.
fn settle_ack(
    index: usize,
    outcome: std::result::Result<(), jetstream::context::PublishError>,
    rejected: &mut Vec<usize>,
    unconfirmed: &mut Vec<usize>,
    first_err: &mut Option<ShoveError>,
) {
    let Err(e) = outcome else { return };
    metrics::record_backend_error(
        metrics::BackendLabel::Nats,
        metrics::BackendErrorKind::Publish,
    );
    if ack_error_is_explicit_rejection(&e) {
        rejected.push(index);
    } else {
        unconfirmed.push(index);
    }
    if first_err.is_none() {
        *first_err = Some(ShoveError::Connection(format!(
            "batch publish ack failed: {e}"
        )));
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
    use super::super::constants::RETRY_COUNT_HEADER;
    use super::*;
    use jetstream::context::{PublishError, PublishErrorKind as K};
    use uuid::{Variant, Version};

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

    /// A duplicate PubAck for a copy is trusted only when the message stored
    /// under the copy's id went to the same subject, names the same origin and
    /// carries the same payload.
    #[test]
    fn a_stored_message_is_the_copy_only_with_its_subject_origin_and_payload() {
        let subject = "orders.shard.0";
        let origin = "orders.billing-consumer.7";
        let mut headers = HeaderMap::new();
        headers.insert(COPY_OF_HEADER, origin);
        let no_origin = HeaderMap::new();
        let sent = SentCopy {
            subject,
            origin,
            payload: b"payload",
        };
        let ours = StoredMessage {
            subject,
            headers: &headers,
            payload: b"payload",
        };

        assert!(is_same_copy(&ours, &sent));
        assert!(
            !is_same_copy(
                &StoredMessage {
                    subject: "orders.shard.1",
                    ..ours
                },
                &sent
            ),
            "the same copy on another subject the stream captures"
        );
        assert!(
            !is_same_copy(
                &ours,
                &SentCopy {
                    origin: "orders.audit-consumer.7",
                    ..sent
                }
            ),
            "another consumer's dead letter of the same record in a shared DLQ"
        );
        assert!(
            !is_same_copy(
                &StoredMessage {
                    payload: b"impostor",
                    ..ours
                },
                &sent
            ),
            "a different payload under the same id"
        );
        assert!(
            !is_same_copy(
                &StoredMessage {
                    headers: &no_origin,
                    ..ours
                },
                &sent
            ),
            "a message that names no origin"
        );
    }

    fn id_of(headers: &HeaderMap) -> String {
        headers
            .get(NATS_MESSAGE_ID)
            .map(HeaderValue::as_str)
            .unwrap_or_default()
            .to_string()
    }

    /// A fresh draw per publish: no id repeats across many publishes, whichever
    /// threads make them.
    #[test]
    fn message_ids_are_unique_across_many_publishes() {
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        for _ in 0..50_000 {
            let headers = NatsPublisher::build_headers(None, None).unwrap();
            assert!(seen.insert(id_of(&headers)), "a publish repeated an id");
        }

        let threads: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..5_000)
                        .map(|_| id_of(&NatsPublisher::build_headers(None, None).unwrap()))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        for thread in threads {
            for id in thread.join().unwrap() {
                assert!(seen.insert(id), "two threads took the same id");
            }
        }
    }

    /// An id is a lowercase hyphenated version 4 UUID, so a copy's `-r{n}` and
    /// a dead letter's `-dlq` extend it as they extend any id.
    #[test]
    fn a_message_id_is_a_version_4_uuid() {
        let id = id_of(&NatsPublisher::build_headers(None, None).unwrap());
        let uuid = Uuid::parse_str(&id).unwrap();
        assert_eq!(uuid.get_version(), Some(Version::Random));
        assert_eq!(uuid.get_variant(), Variant::RFC4122);
        assert_eq!(id, uuid.hyphenated().to_string());
    }

    /// An id gives away nothing about the next. Consecutive ids are no counter
    /// step apart and open with no fixed prefix, so a party that can publish to
    /// the stream cannot take an id ahead of a message.
    #[test]
    fn consecutive_message_ids_are_not_sequential_and_share_no_prefix() {
        use std::collections::HashSet;

        let ids: Vec<u128> = (0..256)
            .map(|_| {
                let id = id_of(&NatsPublisher::build_headers(None, None).unwrap());
                Uuid::parse_str(&id).unwrap().as_u128()
            })
            .collect();
        for pair in ids.windows(2) {
            assert!(
                pair[0].abs_diff(pair[1]) > u128::from(u64::MAX),
                "{:032x} and {:032x} are close together",
                pair[0],
                pair[1]
            );
        }
        let prefixes: HashSet<u128> = ids.iter().map(|id| id >> 64).collect();
        assert_eq!(prefixes.len(), ids.len(), "two ids open alike");
    }

    /// A fresh message starts at retry count zero without saying so: every
    /// reader of the header takes a missing one for zero.
    #[test]
    fn a_fresh_publish_stamps_no_retry_count() {
        let headers = NatsPublisher::build_headers(None, None).unwrap();
        assert!(headers.get(RETRY_COUNT_HEADER).is_none());

        let mut extra = HashMap::new();
        extra.insert("x-trace-id".to_string(), "t".to_string());
        let headers = NatsPublisher::build_headers(Some(&extra), Some("key")).unwrap();
        assert!(headers.get(RETRY_COUNT_HEADER).is_none());
        assert_eq!(
            headers.get("x-trace-id").map(HeaderValue::as_str),
            Some("t")
        );
        assert_eq!(
            headers.get(SEQUENCE_KEY_HEADER).map(HeaderValue::as_str),
            Some("key")
        );
    }

    /// A zero attempt budget is a caller's mistake, answered before anything
    /// reaches the wire. The client never connects: nothing here needs it to.
    #[tokio::test]
    async fn zero_attempts_is_a_validation_error() {
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("nats://127.0.0.1:1")
            .await
            .expect("a lazily connecting client is built without a server");
        let js = jetstream::new(client);

        let result = publish_with_retry(
            &js,
            "subject".to_string(),
            HeaderMap::new(),
            Bytes::new(),
            None,
            0,
            "publish",
        )
        .await;

        assert!(
            matches!(
                result,
                Err(PublishFailure::Failed(ShoveError::Validation(_)))
            ),
            "got {result:?}"
        );
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
