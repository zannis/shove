//! Shared registry decode stage used by the Kafka consumer decode sites.

use std::sync::Arc;

use crate::codec::Codec;
use crate::error::Result;

use super::client::SchemaRegistry;
use super::gate::{self, GateOutcome, SchemaEnforcement};
use super::wire::{FrameResult, WireFormat, parse_frame};

/// Outcome of the registry decode stage.
pub(crate) enum RegistryDecode<M> {
    /// Decoded successfully — hand `M` to the handler.
    Decoded(M),
    /// Reject to DLQ with this (payload-free) death reason.
    Dlq(&'static str),
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
pub(crate) async fn registry_decode<M, C>(
    registry: &SchemaRegistry,
    wire_format: WireFormat,
    enforcement: SchemaEnforcement,
    accepted: &[Arc<str>],
    required_index: Option<&[i32]>,
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

    let schema = match registry.resolve(id).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(schema_id = %id, error = %e, "schema resolve failed");
            return Ok(RegistryDecode::Dlq("schema_resolve_failed"));
        }
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
