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

/// Parse the Confluent frame, gate the subject, and decode the inner payload
/// via the topic's codec. `accepted` is the resolved accepted-subject set.
pub(crate) async fn registry_decode<M, C>(
    registry: &SchemaRegistry,
    wire_format: WireFormat,
    enforcement: SchemaEnforcement,
    accepted: &[Arc<str>],
    bytes: &[u8],
) -> Result<RegistryDecode<M>>
where
    C: Codec<M>,
{
    let (id, payload) = match parse_frame(wire_format, bytes) {
        FrameResult::Framed { id, payload } => (id, payload),
        FrameResult::Null | FrameResult::Unframed(_) => {
            return Ok(RegistryDecode::Dlq("schema_frame_invalid"));
        }
    };

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
