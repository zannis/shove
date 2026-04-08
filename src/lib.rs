pub mod audit;
pub mod autoscaler;
pub mod consumer;
pub mod error;
pub mod handler;
#[doc(hidden)]
pub mod macros;
pub mod metadata;
pub mod outcome;
pub mod publisher;
pub mod topic;
pub mod topology;

mod backends;
pub(crate) mod retry;

pub use audit::{AuditHandler, AuditRecord, Audited};
pub use consumer::{Consumer, ConsumerOptions};
pub use error::ShoveError;
pub use handler::MessageHandler;
pub use metadata::{DeadMessageMetadata, MessageMetadata};
pub use outcome::Outcome;
pub use publisher::Publisher;
#[cfg(any(feature = "rabbitmq", feature = "pub-aws-sns"))]
use std::time::Duration;
pub use topic::{SequencedTopic, Topic};
pub use topology::{
    HoldQueue, QueueTopology, SequenceConfig, SequenceFailure, TopologyBuilder, TopologyDeclarer,
    declare_topic,
};

pub use autoscaler::{
    Autoscaler, AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics,
    ScalingStrategy, Stabilized, ThresholdStrategy,
};

#[cfg(feature = "audit")]
pub use audit::{AuditLog, ShoveAuditHandler};

/// Grace period for in-flight operations before closing connections.
#[cfg(any(feature = "rabbitmq", feature = "pub-aws-sns"))]
pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

// Backend re-exports
#[cfg(feature = "pub-aws-sns")]
pub mod sns {
    pub use crate::backends::sns::{
        client::{SnsClient, SnsConfig},
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

#[cfg(feature = "rabbitmq")]
pub mod rabbitmq {
    pub use crate::backends::rabbitmq::{
        autoscaler::RabbitMqAutoscalerBackend,
        client::{RabbitMqClient, RabbitMqConfig},
        consumer::RabbitMqConsumer,
        consumer_group::{ConsumerGroup, ConsumerGroupConfig},
        headers::MESSAGE_ID_KEY,
        management::{ManagementConfig, QueueStats, QueueStatsProvider},
        publisher::RabbitMqPublisher,
        registry::ConsumerGroupRegistry,
        topology::RabbitMqTopologyDeclarer,
    };
}
