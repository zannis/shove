//! Type-safe async pub/sub for Rust on top of RabbitMQ, AWS SNS/SQS, NATS
//! JetStream, Apache Kafka, or an in-process broker.
//!
//! # The `Broker<B>` pattern
//!
//! Everything hangs off a single generic hub [`Broker<B>`], parameterised by a
//! backend marker `B` (one of [`RabbitMq`], [`Sqs`], [`Nats`], [`Kafka`],
//! [`InMemory`], each gated on its Cargo feature). The marker binds that
//! backend's client / publisher / consumer / topology / registry types
//! together; the generic wrappers below delegate through the sealed
//! [`Backend`] trait.
//!
//! ```text
//! Broker<B>
//!    ├─ .topology()             → TopologyDeclarer<B>
//!    ├─ .publisher().await      → Publisher<B>
//!    ├─ .consumer_supervisor()  → ConsumerSupervisor<B>   (all backends)
//!    ├─ .autoscaler()           → B::AutoscalerImpl       (all backends)
//!    └─ .consumer_group()       → ConsumerGroup<B>        (B: HasCoordinatedGroups)
//! ```
//!
//! # Capability gating
//!
//! - **Kafka, RabbitMQ, NATS, InMemory, Redis** (`redis-streams`) implement
//!   the [`HasCoordinatedGroups`] capability trait — they expose
//!   [`Broker::consumer_group`] for min/max-bounded coordinated groups with
//!   autoscaling.
//! - **SQS** does **not**. A "group" on SQS is N parallel independent
//!   pollers on one queue, which maps to [`ConsumerSupervisor`] (the
//!   backend-agnostic path available on every `Broker<B>`). Calling
//!   `consumer_group()` on `Broker<Sqs>` is a compile error.
//!
//! # Feature flags
//!
//! No features are enabled by default. Enable only what you need.
//!
//! | Feature                    | What it enables                                                                             |
//! |----------------------------|---------------------------------------------------------------------------------------------|
//! | `inmemory`                 | In-process broker, publisher, consumer, topology, groups, autoscaler (no external broker)   |
//! | `kafka`                    | Apache Kafka publisher, consumer, topology, consumer groups, autoscaling (plaintext only)   |
//! | `kafka-ssl`                | TLS + SASL mechanisms for Kafka — required for any authenticated cluster (implies `kafka`)  |
//! | `kafka-msk-iam`            | AWS MSK IAM OAUTHBEARER auth (implies `kafka-ssl`)                                         |
//! | `nats`                     | NATS JetStream publisher, consumer, topology, consumer groups, autoscaling                  |
//! | `rabbitmq`                 | RabbitMQ publisher, consumer, topology, consumer groups, autoscaling                        |
//! | `rabbitmq-transactional`   | RabbitMQ exactly-once routing via AMQP transactions (implies `rabbitmq`)                    |
//! | `pub-aws-sns`              | SNS publisher and topology declaration only                                                 |
//! | `aws-sns-sqs`              | Full SNS + SQS stack — publisher, SQS consumer, supervisor, autoscaling (implies `pub-aws-sns`) |
//! | `redis-streams`            | Redis/Valkey Streams publisher, consumer, topology, consumer groups, FIFO sharding          |
//! | `audit`                    | [`ShoveAuditHandler`] + [`AuditLog`] topic for persisting audit records through any backend |
//!
//! # Quickstart
//!
//! The example below uses the in-process backend so it needs no external
//! services. Swap `InMemory` for [`RabbitMq`], [`Sqs`], [`Nats`], or
//! [`Kafka`] — the topic definition, handler, and every call site stay
//! identical.
//!
//! ```no_run
//! # #[cfg(feature = "inmemory")]
//! # mod example {
//! use serde::{Deserialize, Serialize};
//! use shove::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
//! use shove::{
//!     Broker, ConsumerGroupConfig, InMemory, MessageHandler, MessageMetadata, Outcome,
//!     TopologyBuilder, define_topic,
//! };
//! use std::time::Duration;
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct OrderPaid { order_id: String }
//!
//! define_topic!(Orders, OrderPaid,
//!     TopologyBuilder::new("orders").dlq().build());
//!
//! struct Handler;
//! impl MessageHandler<Orders> for Handler {
//!     type Context = ();
//!     async fn handle(&self, msg: OrderPaid, _: MessageMetadata, _: &()) -> Outcome {
//!         println!("paid: {}", msg.order_id);
//!         Outcome::Ack
//!     }
//! }
//!
//! # pub async fn run() -> Result<(), shove::ShoveError> {
//! let broker = Broker::<InMemory>::new(InMemoryConfig::default()).await?;
//! broker.topology().declare::<Orders>().await?;
//!
//! let publisher = broker.publisher().await?;
//! publisher.publish::<Orders>(&OrderPaid { order_id: "ORD-1".into() }).await?;
//!
//! let mut group = broker.consumer_group();
//! group
//!     .register::<Orders, _>(
//!         ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=1)),
//!         || Handler,
//!     )
//!     .await?;
//!
//! let outcome = group
//!     .run_until_timeout(std::future::ready(()), Duration::from_secs(1))
//!     .await;
//! std::process::exit(outcome.exit_code());
//! # }
//! # }
//! ```
//!
//! # Ergonomics
//!
//! - [`MessageHandlerExt::audited`] — fluent audit wrapping:
//!   `handler.audited(sink)` instead of `Audited::new(handler, sink)`.
//! - [`TopologyDeclarer::declare_all`] — declare multiple topics in one
//!   call via tuple arities 1 through 16.
//! - [`ConsumerOptions::preset`] — shorthand for `new().with_prefetch_count(n)`.
//! - [`SupervisorOutcome::exit_code`] — canonical process exit code from a
//!   consumer group or supervisor: `0` clean, `1` any handler error,
//!   `2` any task panic, `3` drain timeout.
//!
//! # Observability
//!
//! Every interesting state change is emitted as a structured `tracing` event,
//! so wiring any `tracing-subscriber` gives a full operational trail without
//! handler-side instrumentation.
//!
//! Enable the `metrics` cargo feature to also emit operational counters,
//! histograms, and gauges through the [`metrics`](https://docs.rs/metrics)
//! facade — `messages_consumed_total`, `message_processing_duration_seconds`,
//! `messages_inflight`, `backend_errors_total`, and friends. `shove` is a
//! library, so it does not open a port: install your own recorder
//! ([`metrics-exporter-prometheus`](https://docs.rs/metrics-exporter-prometheus),
//! `metrics-exporter-statsd`, OpenTelemetry, etc.) and expose the endpoint
//! from your service. Override the `shove_` metric prefix with
//! [`metrics::set_prefix`] once at startup, before any broker activity. See
//! the module docs and the Observability guide for the full schema.
//!
//! # See also
//!
//! - [`TopologyBuilder`] for hold queues, DLQs, and sequenced routing.
//! - [`define_topic!`] and [`define_sequenced_topic!`] for the typed-topic
//!   macros.
//! - Per-backend modules: [`rabbitmq`], [`sns`], [`nats`], [`kafka`],
//!   [`inmemory`] — expose the config and client types bound to each
//!   marker.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod audit;
pub mod autoscale_metrics;
pub mod autoscaler;
pub mod backend;
pub mod broker;
pub mod codec;
pub mod codecs;
pub mod consumer;
pub mod consumer_group;
pub mod consumer_supervisor;
pub mod error;
pub mod handler;
#[doc(hidden)]
pub mod macros;
pub mod markers;
pub mod metadata;
pub mod metrics;
pub mod outcome;
pub mod publisher;
pub(crate) mod publisher_internal;
pub mod queue_depth;
#[cfg(feature = "kafka-schema-registry")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka-schema-registry")))]
pub mod schema_registry;
pub mod topic;
pub mod topology;
pub mod topology_declarer;

mod backends;
#[cfg(any(
    feature = "rabbitmq",
    feature = "nats",
    feature = "kafka",
    feature = "pub-aws-sns",
    feature = "aws-sns-sqs",
    feature = "redis-streams"
))]
pub(crate) mod retry;
#[cfg(any(
    feature = "inmemory",
    feature = "rabbitmq",
    feature = "nats",
    feature = "kafka",
    feature = "redis-streams",
    feature = "aws-sns-sqs"
))]
pub(crate) mod routing;
#[cfg(any(feature = "nats", feature = "kafka"))]
pub(crate) mod supervision;

pub use audit::{AuditHandler, AuditRecord, Audited};
pub use autoscale_metrics::AutoscaleMetrics;
pub use backend::{Backend, capability::HasCoordinatedGroups};
pub use codec::{Codec, JsonCodec, RawBytesCodec};
#[cfg(feature = "protobuf")]
#[cfg_attr(docsrs, doc(cfg(feature = "protobuf")))]
pub use codecs::protobuf::ProtobufCodec;
#[cfg(feature = "sbe")]
#[cfg_attr(docsrs, doc(cfg(feature = "sbe")))]
pub use codecs::sbe::{SbeByteOrder, SbeCodec, SbeCodecError, SbeFrame, SbeHeader, SbeMessage};
pub use consumer::{
    ConsumerOptions, DEFAULT_HANDLER_TIMEOUT, DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY,
};
pub use consumer_supervisor::{ConsumerSupervisor, SupervisorOutcome};
pub use error::ShoveError;
pub use handler::{BatchMessageHandler, MessageHandler, MessageHandlerExt};
pub use metadata::{
    DeadMessageMetadata, DeadMessageMetadataBuilder, MessageMetadata, MessageMetadataBuilder,
};
pub use outcome::Outcome;
#[cfg(any(feature = "rabbitmq", feature = "pub-aws-sns"))]
use std::time::Duration;
pub use topic::{NotSequenced, SequencedTopic, Topic};
#[cfg(feature = "kafka")]
pub use topology::KafkaCleanupPolicy;
pub use topology::{HoldQueue, QueueTopology, SequenceConfig, SequenceFailure, TopologyBuilder};
#[cfg(feature = "nats")]
pub use topology::{NatsRetention, NatsStreamConfig};

pub use autoscaler::{
    Autoscaler, AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics,
    ScalingStrategy, Stabilized, ThresholdStrategy,
};
pub use queue_depth::QueueDepthSampler;

// --- v2 generic wrappers (Phase 5) ---
pub use broker::Broker;
pub use consumer_group::{ConsumerGroup, ConsumerGroupConfig};
pub use publisher::Publisher;
pub use topology_declarer::{Topics, TopologyDeclarer};

#[cfg(feature = "inmemory")]
#[cfg_attr(docsrs, doc(cfg(feature = "inmemory")))]
pub use markers::InMemory;
#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
pub use markers::Kafka;
#[cfg(feature = "nats")]
#[cfg_attr(docsrs, doc(cfg(feature = "nats")))]
pub use markers::Nats;
#[cfg(feature = "rabbitmq")]
#[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq")))]
pub use markers::RabbitMq;
#[cfg(feature = "redis-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-streams")))]
pub use markers::Redis;
#[cfg(feature = "aws-sns-sqs")]
#[cfg_attr(docsrs, doc(cfg(feature = "aws-sns-sqs")))]
pub use markers::Sqs;

#[cfg(feature = "audit")]
#[cfg_attr(docsrs, doc(cfg(feature = "audit")))]
pub use audit::{AuditLog, ShoveAuditHandler};

/// Grace period for in-flight operations before closing connections.
#[cfg(any(feature = "rabbitmq", feature = "pub-aws-sns"))]
pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

// Backend re-exports.
//
// The recommended user-facing path is the generic `Broker<B>` /
// `Publisher<B>` / `ConsumerSupervisor<B>` / `ConsumerGroup<B>` /
// `TopologyDeclarer<B>` / `Autoscaler<B>` API. The per-backend modules
// below also expose their concrete client, publisher, consumer, topology,
// autoscaler, registry, and stats-provider types as a permanent escape
// hatch for code that needs to drive a backend directly — backend-specific
// configuration, custom stats providers, and integration tests that
// exercise the underlying machinery.
#[cfg(feature = "pub-aws-sns")]
#[cfg_attr(docsrs, doc(cfg(feature = "pub-aws-sns")))]
pub mod sns {
    pub use crate::backends::sns::client::SnsConfig;
    #[cfg(feature = "aws-sns-sqs")]
    #[cfg_attr(docsrs, doc(cfg(feature = "aws-sns-sqs")))]
    pub use crate::markers::Sqs;

    pub use crate::backends::sns::{
        client::SnsClient,
        publisher::SnsPublisher,
        topology::{SnsTopologyDeclarer, TopicRegistry},
    };

    #[cfg(feature = "aws-sns-sqs")]
    #[cfg_attr(docsrs, doc(cfg(feature = "aws-sns-sqs")))]
    pub use crate::backends::sns::{
        autoscaler::SqsAutoscalerBackend,
        consumer::SqsConsumer,
        consumer_group::{SqsConsumerGroup, SqsConsumerGroupConfig},
        registry::SqsConsumerGroupRegistry,
        stats::{SqsQueueStats, SqsQueueStatsProvider, SqsQueueStatsProviderTrait},
        topology::QueueRegistry,
    };
}

#[cfg(feature = "nats")]
#[cfg_attr(docsrs, doc(cfg(feature = "nats")))]
pub mod nats {
    pub use crate::backends::nats::NatsConfig;
    pub use crate::markers::Nats;

    pub use crate::backends::nats::{
        JetStreamStatsProvider, NatsAutoscalerBackend, NatsClient, NatsConsumer, NatsConsumerGroup,
        NatsConsumerGroupConfig, NatsConsumerGroupRegistry, NatsPublisher, NatsQueueStats,
        NatsQueueStatsProvider, NatsTopologyDeclarer,
    };
}

#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
pub mod kafka {
    pub use crate::backends::kafka::{KafkaCompression, KafkaConfig};
    pub use crate::markers::Kafka;

    pub use crate::backends::kafka::{
        BatchConsumerOptions, KafkaAutoOffsetReset, KafkaAutoscalerBackend, KafkaClient,
        KafkaConsumer, KafkaConsumerGroup, KafkaConsumerGroupConfig, KafkaConsumerGroupRegistry,
        KafkaLagStatsProvider, KafkaPublisher, KafkaPublisherConfig, KafkaQueueStats,
        KafkaQueueStatsProvider, KafkaTopologyDeclarer,
    };
    #[cfg(feature = "kafka-ssl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "kafka-ssl")))]
    pub use crate::backends::kafka::{KafkaSasl, KafkaTls};

    /// Test-only seam (see the `test-support` feature): runs the admin
    /// OAUTHBEARER token-priming path against a caller-supplied admin client so
    /// integration tests can exercise it against a local broker.
    #[cfg(all(feature = "kafka-msk-iam", feature = "test-support"))]
    #[cfg_attr(docsrs, doc(cfg(feature = "test-support")))]
    pub use crate::backends::kafka::prime_admin_oauth_token_for_test;
}

/// Redis Streams backend.
///
/// Uses Redis Streams (XADD / XREADGROUP) as the transport. Supports
/// consumer groups, hold queues (via sorted sets), DLQ routing, FIFO
/// sharding, and crash recovery via XAUTOCLAIM.
#[cfg(feature = "redis-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-streams")))]
pub mod redis {
    pub use crate::markers::Redis;

    pub use crate::backends::redis::{
        RedisAutoscalerBackend, RedisClient, RedisConfig, RedisConsumer, RedisConsumerGroup,
        RedisConsumerGroupConfig, RedisConsumerGroupRegistry, RedisMode, RedisPublisher,
        RedisQueueStats, RedisQueueStatsProvider, RedisTopologyDeclarer, XlenStatsProvider,
    };

    /// Test-only escape hatches. `#[doc(hidden)]` so they don't appear in
    /// rustdoc; integration tests in `tests/` (which are external crates)
    /// import them directly. Production code should not call these; the
    /// per-process maintenance registry owns reaper lifecycle.
    #[doc(hidden)]
    pub use crate::backends::redis::{spawn_maintenance, spawn_reaper};
}

/// In-process, non-durable broker backend.
///
/// Messages live only in this process, are not persisted, and are dropped on
/// shutdown. Suitable for tests and single-process apps; use another backend
/// (RabbitMQ, Kafka, NATS, SNS/SQS) for production workloads that require
/// durability or cross-process delivery.
#[cfg(feature = "inmemory")]
#[cfg_attr(docsrs, doc(cfg(feature = "inmemory")))]
pub mod inmemory {
    pub use crate::markers::InMemory;

    pub use crate::backends::inmemory::{
        BrokerStatsProvider, DEFAULT_QUEUE_CAPACITY, InMemoryAutoscalerBackend, InMemoryBroker,
        InMemoryConfig, InMemoryConsumer, InMemoryConsumerGroup, InMemoryConsumerGroupConfig,
        InMemoryConsumerGroupRegistry, InMemoryPublisher, InMemoryQueueStats,
        InMemoryQueueStatsProvider, InMemoryTopologyDeclarer,
    };
}

#[cfg(feature = "rabbitmq")]
#[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq")))]
pub mod rabbitmq {
    pub use crate::backends::rabbitmq::client::RabbitMqConfig;
    pub use crate::backends::rabbitmq::management::ManagementConfig;
    pub use crate::markers::RabbitMq;

    pub use crate::backends::rabbitmq::{
        autoscaler::RabbitMqAutoscalerBackend,
        client::RabbitMqClient,
        consumer::RabbitMqConsumer,
        consumer_group::{RabbitMqConsumerGroup, RabbitMqConsumerGroupConfig},
        headers::MESSAGE_ID_KEY,
        management::{QueueStats, QueueStatsProvider},
        publisher::RabbitMqPublisher,
        registry::ConsumerGroupRegistry,
        topology::RabbitMqTopologyDeclarer,
    };
}
