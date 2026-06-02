//! In-house Confluent Schema Registry decode for Kafka consumers.
//!
//! Decode-only: strips the Confluent wire frame (magic byte +
//! big-endian schema id, and for protobuf the message-index array), validates
//! the message's schema subject against an accepted set, and delegates the
//! inner payload to the topic's [`crate::codec::Codec`]. Schemas are fetched
//! from a Confluent-compatible registry over HTTP and cached so the hot path
//! is a single lock-free map read.
//!
//! Works with the [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
//! and with Redpanda's built-in Schema Registry, which exposes the same
//! Confluent-compatible REST API and wire format.
//!
//! Enable with the `kafka-schema-registry` feature.
//!
//! # Examples
//!
//! Build a registry client and attach it to a Kafka consumer:
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use shove::schema_registry::{SchemaEnforcement, SchemaRegistry, SchemaRegistryAuth};
//! use shove::consumer::ConsumerOptions;
//! use shove::markers::Kafka;
//!
//! // Build the registry client once and share it across consumers via Arc.
//! let registry = SchemaRegistry::builder("http://schema-registry:8081")
//!     .auth(SchemaRegistryAuth::Basic {
//!         user: "sr-user".into(),
//!         pass: "sr-pass".into(),
//!     })
//!     .timeout(Duration::from_secs(3))
//!     .max_retries(2)
//!     .negative_cache_ttl(Duration::from_secs(60))
//!     .build();
//!
//! // Attach the registry to per-consumer options.
//! let opts = ConsumerOptions::<Kafka>::new()
//!     .with_schema_registry(Arc::clone(&registry))
//!     .with_schema_enforcement(SchemaEnforcement::Enforce)
//!     .accept_schema_subjects(["orders-value"]);
//! ```

mod client;
pub(crate) mod decode;
mod error;
mod gate;
mod schema;
mod wire;

pub use client::{SchemaRegistry, SchemaRegistryAuth, SchemaRegistryBuilder};
pub use error::SchemaRegistryError;
pub use gate::SchemaEnforcement;
pub(crate) use gate::default_subject;
pub use schema::{CachedSchema, SchemaType};
pub use wire::{FrameResult, SchemaId, WireFormat};
