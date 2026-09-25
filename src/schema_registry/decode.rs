//! Shared registry decode stage used by the Kafka consumer decode sites.

use std::sync::Arc;
use std::time::Duration;

use crate::codec::Codec;
use crate::error::{Result, ShoveError};

use super::client::SchemaRegistry;
use super::error::SchemaRegistryError;
use super::gate::{self, GateOutcome, SchemaEnforcement};
use super::wire::{FrameResult, SchemaId, WireFormat, parse_frame};

/// Outcome of the registry decode stage.
pub(crate) enum RegistryDecode<M> {
    /// Decoded successfully — hand `M` to the handler.
    Decoded(M),
    /// Reject to DLQ with this (payload-free) death reason.
    Dlq(&'static str),
    /// The registry could not answer for `id` right now. Nothing is wrong
    /// with the record: keep it and decode the same bytes again later.
    Unavailable {
        id: SchemaId,
        error: SchemaRegistryError,
    },
}

/// What a failed schema lookup means for the record that carried the id.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ResolveFailure {
    /// The registry answered, and the answer rules the record out for good.
    Dlq(&'static str),
    /// The registry did not answer; the same lookup may succeed later.
    Unavailable,
    /// The deployment is wrong: credentials, the base URL, an unexpected or
    /// undecodable response. Waiting would hide it, so the consumer stops.
    Fatal,
}

/// Classify a `resolve` error. A 404 is the registry's definite answer that
/// the id is unknown, so it stays a DLQ reason; a retriable transport failure
/// is an outage to wait out; everything else is a deployment fault.
pub(crate) fn classify_resolve_error(error: &SchemaRegistryError) -> ResolveFailure {
    match error {
        SchemaRegistryError::NotFound(_) => ResolveFailure::Dlq("schema_resolve_failed"),
        SchemaRegistryError::Incompatible { .. } => ResolveFailure::Dlq("schema_validation_failed"),
        SchemaRegistryError::Transport {
            retriable: true, ..
        } => ResolveFailure::Unavailable,
        SchemaRegistryError::Transport {
            retriable: false, ..
        }
        | SchemaRegistryError::Decode(_) => ResolveFailure::Fatal,
    }
}

/// Whether a frame's protobuf message index satisfies the consumer's
/// requirement. No requirement accepts everything; a frame without an index
/// (JSON) is not judged, because the setting has nothing to compare there.
pub(crate) fn message_index_accepted(required: Option<&[i32]>, actual: Option<&[i32]>) -> bool {
    match (required, actual) {
        (None, _) | (Some(_), None) => true,
        (Some(required), Some(actual)) => required == actual,
    }
}

/// Parse the Confluent frame, check the protobuf message index against
/// `required_index` when one is set, gate the subject, and decode the inner
/// payload via the topic's codec. `accepted` is the resolved accepted-subject
/// set.
///
/// The index check runs before the registry lookup: a rejected frame costs no
/// network round trip.
///
/// `lookup_bound` caps one lookup. A consume loop calls this from its receive
/// arm, where nothing polls the broker until the lookup answers, and a
/// registry that accepts the connection and never answers holds the request
/// for the client's whole timeout and retries; a bound below the member's
/// `max.poll.interval.ms` turns that into `Unavailable`, which the loop
/// resolves by waiting while it keeps polling, so the member stays in its
/// group. The lookup itself is dropped with the future, which takes this
/// waiter off the client's single-flight entry.
pub(crate) async fn registry_decode<M, C>(
    registry: &SchemaRegistry,
    wire_format: WireFormat,
    enforcement: SchemaEnforcement,
    accepted: &[Arc<str>],
    required_index: Option<&[i32]>,
    lookup_bound: Duration,
    bytes: &[u8],
) -> Result<RegistryDecode<M>>
where
    C: Codec<M>,
{
    let (id, payload, message_index) = match parse_frame(wire_format, bytes) {
        FrameResult::Framed {
            id,
            payload,
            message_index,
        } => (id, payload, message_index),
        FrameResult::Null | FrameResult::Unframed(_) => {
            return Ok(RegistryDecode::Dlq("schema_frame_invalid"));
        }
    };

    if !message_index_accepted(required_index, message_index.as_deref()) {
        tracing::warn!(
            schema_id = %id,
            required = ?required_index,
            actual = ?message_index,
            "protobuf message index is not the one this consumer requires, routing to DLQ"
        );
        return Ok(RegistryDecode::Dlq("schema_message_index_rejected"));
    }

    let resolved = match tokio::time::timeout(lookup_bound, registry.resolve(id)).await {
        Ok(resolved) => resolved,
        Err(_) => {
            return Ok(RegistryDecode::Unavailable {
                id,
                error: SchemaRegistryError::Transport {
                    retriable: true,
                    message: format!(
                        "registry lookup exceeded the {lookup_bound:?} bound that keeps the \
                         consumer polling inside its poll interval"
                    ),
                },
            });
        }
    };
    let schema = match resolved {
        Ok(s) => s,
        Err(e) => match classify_resolve_error(&e) {
            ResolveFailure::Dlq(reason) => {
                tracing::error!(schema_id = %id, error = %e, "schema resolve failed");
                return Ok(RegistryDecode::Dlq(reason));
            }
            ResolveFailure::Unavailable => {
                return Ok(RegistryDecode::Unavailable { id, error: e });
            }
            ResolveFailure::Fatal => {
                return Err(ShoveError::Topology(format!(
                    "schema registry lookup for schema id {id} failed in a way waiting cannot \
                     fix: {e}. This is a deployment fault (credentials, the base URL or an \
                     unexpected response), not an outage, so the consumer stops instead of \
                     stalling on it"
                )));
            }
        },
    };

    match gate::evaluate(&schema, accepted, enforcement) {
        GateOutcome::Accept => {
            if enforcement == SchemaEnforcement::Permissive && !schema.matches_any(accepted) {
                tracing::warn!(
                    schema_id = %id,
                    subject = schema.primary_subject().unwrap_or("?"),
                    "schema subject not accepted (permissive — decoding anyway)"
                );
            }
        }
        GateOutcome::RejectToDlq => {
            tracing::warn!(
                schema_id = %id,
                subject = schema.primary_subject().unwrap_or("?"),
                "schema subject not accepted, routing to DLQ"
            );
            return Ok(RegistryDecode::Dlq("schema_validation_failed"));
        }
    }

    let value = C::decode(payload)?;
    Ok(RegistryDecode::Decoded(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;
    use crate::schema_registry::wire::{SchemaId, build_frame};

    #[test]
    fn no_requirement_accepts_any_index() {
        assert!(message_index_accepted(None, Some(&[0])));
        assert!(message_index_accepted(None, Some(&[2, 1, 3])));
        assert!(message_index_accepted(None, None));
    }

    #[test]
    fn a_requirement_matches_exactly() {
        assert!(message_index_accepted(Some(&[0]), Some(&[0])));
        assert!(message_index_accepted(Some(&[2, 1]), Some(&[2, 1])));
        assert!(!message_index_accepted(Some(&[0]), Some(&[1])));
        assert!(!message_index_accepted(Some(&[0]), Some(&[0, 0])));
        assert!(!message_index_accepted(Some(&[1, 2]), Some(&[2, 1])));
    }

    /// JSON frames carry no index, so a requirement is inert on them rather
    /// than a blanket rejection.
    #[test]
    fn a_frame_without_an_index_is_not_judged() {
        assert!(message_index_accepted(Some(&[0]), None));
    }

    /// One case per `SchemaRegistryError` variant: a 404 and a subject
    /// mismatch are the registry's answer and go to the DLQ, a retriable
    /// transport failure is an outage to wait out, and a non-retriable one or
    /// an undecodable response is a deployment fault that stops the consumer.
    #[test]
    fn resolve_errors_map_one_case_per_variant() {
        assert_eq!(
            classify_resolve_error(&SchemaRegistryError::NotFound(7)),
            ResolveFailure::Dlq("schema_resolve_failed")
        );
        assert_eq!(
            classify_resolve_error(&SchemaRegistryError::Incompatible {
                got: "other-value".into(),
                accepted: vec!["t-value".into()],
            }),
            ResolveFailure::Dlq("schema_validation_failed")
        );
        assert_eq!(
            classify_resolve_error(&SchemaRegistryError::Transport {
                retriable: true,
                message: "server error 503".into(),
            }),
            ResolveFailure::Unavailable
        );
        assert_eq!(
            classify_resolve_error(&SchemaRegistryError::Transport {
                retriable: false,
                message: "unexpected status 401".into(),
            }),
            ResolveFailure::Fatal
        );
        assert_eq!(
            classify_resolve_error(&SchemaRegistryError::Decode("not json".into())),
            ResolveFailure::Fatal
        );
    }

    /// A registry nobody listens on is an outage: the stage reports
    /// `Unavailable` with the schema id, not a DLQ reason and not an error.
    #[tokio::test]
    async fn an_unreachable_registry_is_unavailable_not_poison() {
        let registry = SchemaRegistry::builder("http://127.0.0.1:9")
            .max_retries(0)
            .build();
        let frame = build_frame(WireFormat::Json, SchemaId(3), &[], b"{}");
        let accepted: [Arc<str>; 1] = [Arc::from("t-value")];
        let outcome = registry_decode::<serde_json::Value, JsonCodec>(
            &registry,
            WireFormat::Json,
            SchemaEnforcement::Enforce,
            &accepted,
            None,
            Duration::from_secs(60),
            &frame,
        )
        .await
        .expect("an outage is not a deployment fault");
        assert!(matches!(
            outcome,
            RegistryDecode::Unavailable {
                id: SchemaId(3),
                ..
            }
        ));
    }

    /// The mismatch is decided before the registry is asked anything: a
    /// registry nobody listens on still yields the index reason, not
    /// `schema_resolve_failed`.
    #[tokio::test]
    async fn a_rejected_index_never_reaches_the_registry() {
        let registry = SchemaRegistry::builder("http://127.0.0.1:9").build();
        let frame = build_frame(WireFormat::Protobuf, SchemaId(1), &[1], b"{}");
        let accepted: [Arc<str>; 1] = [Arc::from("t-value")];
        let outcome = registry_decode::<serde_json::Value, JsonCodec>(
            &registry,
            WireFormat::Protobuf,
            SchemaEnforcement::Enforce,
            &accepted,
            Some(&[0]),
            Duration::from_secs(60),
            &frame,
        )
        .await
        .expect("decode stage must not error");
        assert!(matches!(
            outcome,
            RegistryDecode::Dlq("schema_message_index_rejected")
        ));
    }
}
