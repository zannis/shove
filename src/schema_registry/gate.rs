//! Schema-identity gate: decide whether a resolved schema may be decoded.

use std::sync::Arc;

use super::schema::CachedSchema;

/// Whether a subject-mismatch is fatal (DLQ) or tolerated (warn + proceed).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SchemaEnforcement {
    /// Mismatch routes the message to the DLQ. Default — secure by default.
    #[default]
    Enforce,
    /// Mismatch is logged + counted; the message is still decoded.
    Permissive,
}

/// What the gate decided.
#[derive(Debug, PartialEq)]
pub enum GateOutcome {
    /// Subject accepted (or enforcement permissive) — proceed to decode.
    Accept,
    /// Subject rejected under `Enforce` — route to DLQ.
    RejectToDlq,
}

/// Evaluate the gate for a resolved schema. Pure — no I/O.
pub fn evaluate(
    schema: &CachedSchema,
    accepted: &[Arc<str>],
    enforcement: SchemaEnforcement,
) -> GateOutcome {
    if schema.matches_any(accepted) {
        return GateOutcome::Accept;
    }
    match enforcement {
        SchemaEnforcement::Enforce => GateOutcome::RejectToDlq,
        SchemaEnforcement::Permissive => GateOutcome::Accept,
    }
}

/// Derive the default accepted subject for a queue (TopicNameStrategy).
pub fn default_subject(queue: &str) -> Arc<str> {
    Arc::from(format!("{queue}-value"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_registry::schema::{CachedSchema, SchemaType};

    fn schema(subject: &str) -> CachedSchema {
        CachedSchema {
            id: 1,
            schema_type: SchemaType::Json,
            raw: Arc::from("{}"),
            subjects: vec![(Arc::from(subject), 1)].into(),
        }
    }

    #[test]
    fn accepts_matching_subject() {
        let s = schema("orders-value");
        let acc = [Arc::from("orders-value")];
        assert_eq!(
            evaluate(&s, &acc, SchemaEnforcement::Enforce),
            GateOutcome::Accept
        );
    }

    #[test]
    fn enforce_rejects_mismatch() {
        let s = schema("evil-value");
        let acc = [Arc::from("orders-value")];
        assert_eq!(
            evaluate(&s, &acc, SchemaEnforcement::Enforce),
            GateOutcome::RejectToDlq
        );
    }

    #[test]
    fn permissive_accepts_mismatch() {
        let s = schema("evil-value");
        let acc = [Arc::from("orders-value")];
        assert_eq!(
            evaluate(&s, &acc, SchemaEnforcement::Permissive),
            GateOutcome::Accept
        );
    }

    #[test]
    fn default_subject_appends_value() {
        assert_eq!(default_subject("orders").as_ref(), "orders-value");
    }

    #[test]
    fn enforcement_defaults_to_enforce() {
        assert_eq!(SchemaEnforcement::default(), SchemaEnforcement::Enforce);
    }
}
