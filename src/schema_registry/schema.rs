//! Cached schema metadata. Phase 1 stores identity + raw schema; parsed
//! descriptors are reserved for the dynamic-decode follow-on.

use std::sync::Arc;

/// Registry schema type as reported by `schemaType` (absent ⇒ Avro).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    Avro,
    Protobuf,
    Json,
}

impl SchemaType {
    /// Parse the registry's `schemaType` field. Confluent omits it for Avro.
    pub fn from_registry(s: Option<&str>) -> Self {
        match s {
            Some("PROTOBUF") => SchemaType::Protobuf,
            Some("JSON") => SchemaType::Json,
            _ => SchemaType::Avro,
        }
    }
}

/// A schema resolved by id and cached. One id may map to several subjects;
/// `subjects` holds every `(subject, version)` the registry reports.
#[derive(Debug, Clone)]
pub struct CachedSchema {
    pub id: u32,
    pub schema_type: SchemaType,
    pub raw: Arc<str>,
    /// `(subject, version)` pairs this id is registered under.
    pub subjects: Arc<[(Arc<str>, i32)]>,
}

impl CachedSchema {
    /// True if any subject this id is registered under is in `accepted`.
    pub fn matches_any(&self, accepted: &[Arc<str>]) -> bool {
        self.subjects
            .iter()
            .any(|(s, _)| accepted.iter().any(|a| a.as_ref() == s.as_ref()))
    }

    /// The first subject (for logging/metrics).
    pub fn primary_subject(&self) -> Option<&str> {
        self.subjects.first().map(|(s, _)| s.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(subjects: &[(&str, i32)]) -> CachedSchema {
        CachedSchema {
            id: 1,
            schema_type: SchemaType::Json,
            raw: Arc::from("{}"),
            subjects: subjects.iter().map(|(s, v)| (Arc::from(*s), *v)).collect(),
        }
    }

    #[test]
    fn matches_when_subject_in_accepted_set() {
        let s = schema(&[("orders-value", 3)]);
        assert!(s.matches_any(&[Arc::from("orders-value")]));
    }

    #[test]
    fn no_match_when_subject_absent() {
        let s = schema(&[("payments-value", 1)]);
        assert!(!s.matches_any(&[Arc::from("orders-value")]));
    }

    #[test]
    fn schema_type_defaults_to_avro_when_absent() {
        assert_eq!(SchemaType::from_registry(None), SchemaType::Avro);
        assert_eq!(
            SchemaType::from_registry(Some("PROTOBUF")),
            SchemaType::Protobuf
        );
    }

    #[test]
    fn schema_type_maps_json() {
        assert_eq!(SchemaType::from_registry(Some("JSON")), SchemaType::Json);
    }

    #[test]
    fn primary_subject_returns_first() {
        let s = schema(&[("x", 1), ("y", 2)]);
        assert_eq!(s.primary_subject(), Some("x"));

        let empty = CachedSchema {
            id: 2,
            schema_type: SchemaType::Avro,
            raw: Arc::from("{}"),
            subjects: Vec::new().into(),
        };
        assert_eq!(empty.primary_subject(), None);
    }
}
