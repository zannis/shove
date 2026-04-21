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
//!    └─ .consumer_group()       → ConsumerGroup<B>        (B: HasCoordinatedGroups)
//! ```
//!
//! # Capability gating
//!
//! - **Kafka, RabbitMQ, NATS, InMemory** implement the
//!   [`HasCoordinatedGroups`] capability trait — they expose
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
//! | `kafka`                    | Apache Kafka publisher, consumer, topology, consumer groups, autoscaling                    |
//! | `nats`                     | NATS JetStream publisher, consumer, topology, consumer groups, autoscaling                  |
//! | `rabbitmq`                 | RabbitMQ publisher, consumer, topology, consumer groups, autoscaling                        |
//! | `rabbitmq-transactional`   | RabbitMQ exactly-once routing via AMQP transactions (implies `rabbitmq`)                    |
//! | `pub-aws-sns`              | SNS publisher and topology declaration only                                                 |
//! | `aws-sns-sqs`              | Full SNS + SQS stack — publisher, SQS consumer, supervisor, autoscaling (implies `pub-aws-sns`) |
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
pub mod consumer;
pub mod consumer_group;
pub mod consumer_supervisor;
pub mod error;
pub mod handler;
#[doc(hidden)]
pub mod macros;
pub mod markers;
pub mod metadata;
pub mod outcome;
pub mod publisher;
pub(crate) mod publisher_internal;
pub mod topic;
pub mod topology;
pub mod topology_declarer;

mod backends;
pub(crate) mod retry;

pub use audit::{AuditHandler, AuditRecord, Audited};
pub use autoscale_metrics::AutoscaleMetrics;
pub use backend::{Backend, capability::HasCoordinatedGroups};
pub use consumer::{
    ConsumerOptions, DEFAULT_HANDLER_TIMEOUT, DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY,
};
pub use consumer_supervisor::{ConsumerSupervisor, SupervisorOutcome};
pub use error::ShoveError;
pub use handler::{MessageHandler, MessageHandlerExt};
pub use metadata::{DeadMessageMetadata, MessageMetadata};
pub use outcome::Outcome;
#[cfg(any(feature = "rabbitmq", feature = "pub-aws-sns"))]
use std::time::Duration;
pub use topic::{SequencedTopic, Topic};
pub use topology::{HoldQueue, QueueTopology, SequenceConfig, SequenceFailure, TopologyBuilder};

pub use autoscaler::{
    Autoscaler, AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics,
    ScalingStrategy, Stabilized, ThresholdStrategy,
};

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
#[cfg(feature = "aws-sns-sqs")]
#[cfg_attr(docsrs, doc(cfg(feature = "aws-sns-sqs")))]
pub use markers::Sqs;

#[cfg(feature = "audit")]
#[cfg_attr(docsrs, doc(cfg(feature = "audit")))]
pub use audit::{AuditLog, ShoveAuditHandler};

/// Grace period for in-flight operations before closing connections.
#[cfg(any(feature = "rabbitmq", feature = "pub-aws-sns"))]
pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

// Backend re-exports — marker + config + management types only.
// Old per-backend publisher/consumer/consumer-group/autoscaler/topology
// types are now pub(crate) — users go through `Broker<B>` / `Publisher<B>` /
// `ConsumerSupervisor<B>` / `ConsumerGroup<B>` / `TopologyDeclarer<B>` /
// `Autoscaler<B>` (see DESIGN_V2 §11.4).
//
// Exception: types named by existing integration tests remain `pub` until
// tests migrate to the generic API. See Phase 11.4 caveat in the plan.
#[cfg(feature = "pub-aws-sns")]
#[cfg_attr(docsrs, doc(cfg(feature = "pub-aws-sns")))]
pub mod sns {
    pub use crate::backends::sns::client::SnsConfig;
    #[cfg(feature = "aws-sns-sqs")]
    #[cfg_attr(docsrs, doc(cfg(feature = "aws-sns-sqs")))]
    pub use crate::markers::Sqs;

    // Kept pub because `tests/sns_integration.rs` and `tests/sns_sqs_integration.rs`
    // use the inherent `SnsTopologyDeclarer` and associated registry types.
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

    // Kept pub because `tests/nats_integration.rs` uses these types directly.
    pub use crate::backends::nats::autoscaler::{
        JetStreamStatsProvider, NatsQueueStats, NatsQueueStatsProvider,
    };
    pub use crate::backends::nats::{
        NatsAutoscalerBackend, NatsClient, NatsConsumer, NatsConsumerGroup,
        NatsConsumerGroupConfig, NatsConsumerGroupRegistry, NatsPublisher, NatsTopologyDeclarer,
    };
}

#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
pub mod kafka {
    pub use crate::backends::kafka::KafkaConfig;
    pub use crate::markers::Kafka;

    // Kept pub because `tests/kafka_integration.rs` uses these types directly.
    pub use crate::backends::kafka::autoscaler::{
        KafkaLagStatsProvider, KafkaQueueStats, KafkaQueueStatsProvider,
    };
    pub use crate::backends::kafka::{
        KafkaAutoscalerBackend, KafkaClient, KafkaConsumer, KafkaConsumerGroup,
        KafkaConsumerGroupConfig, KafkaConsumerGroupRegistry, KafkaPublisher,
        KafkaTopologyDeclarer,
    };
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

    // Kept pub because `tests/inmemory_integration.rs` uses these types directly.
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

    // Kept pub because `tests/rabbitmq_integration.rs` uses these types directly.
    pub use crate::backends::rabbitmq::{
        autoscaler::RabbitMqAutoscalerBackend,
        client::RabbitMqClient,
        consumer::RabbitMqConsumer,
        consumer_group::{ConsumerGroup, ConsumerGroupConfig},
        headers::MESSAGE_ID_KEY,
        management::{QueueStats, QueueStatsProvider},
        publisher::RabbitMqPublisher,
        registry::ConsumerGroupRegistry,
        topology::RabbitMqTopologyDeclarer,
    };
}
