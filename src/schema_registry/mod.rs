//! In-house Confluent Schema Registry decode for Kafka consumers.
//!
//! Decode-only (Phase 1): strips the Confluent wire frame (magic byte +
//! big-endian schema id, and for protobuf the message-index array), validates
//! the message's schema subject against an accepted set, and delegates the
//! inner payload to the topic's [`crate::codec::Codec`]. Schemas are fetched
//! from a Confluent-compatible registry over HTTP and cached so the hot path
//! is a single lock-free map read.
//!
//! Enable with the `kafka-schema-registry` feature.

mod error;
mod schema;
mod wire;

pub use error::SchemaRegistryError;
pub use schema::{CachedSchema, SchemaType};
pub use wire::{FrameResult, SchemaId, WireFormat};
