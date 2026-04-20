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
pub use topology::{
    HoldQueue, QueueTopology, SequenceConfig, SequenceFailure, TopologyBuilder,
};

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
pub use markers::InMemory;
#[cfg(feature = "kafka")]
pub use markers::Kafka;
#[cfg(feature = "nats")]
pub use markers::Nats;
#[cfg(feature = "rabbitmq")]
pub use markers::RabbitMq;
#[cfg(feature = "aws-sns-sqs")]
pub use markers::Sqs;

#[cfg(feature = "audit")]
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
pub mod sns {
    pub use crate::backends::sns::client::SnsConfig;
    pub use crate::markers::Sqs;

    // Kept pub because `tests/sns_integration.rs` and `tests/sns_sqs_integration.rs`
    // use the inherent `SnsTopologyDeclarer` and associated registry types.
    pub use crate::backends::sns::{
        client::SnsClient,
        publisher::SnsPublisher,
        topology::{SnsTopologyDeclarer, TopicRegistry},
    };

    #[cfg(feature = "aws-sns-sqs")]
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
