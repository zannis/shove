pub mod audit;
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

pub use audit::{AuditHandler, AuditRecord, Audited};
pub use consumer::{Consumer, ConsumerOptions};
pub use error::ShoveError;
pub use handler::MessageHandler;
pub use metadata::{DeadMessageMetadata, MessageMetadata};
pub use outcome::Outcome;
pub use publisher::Publisher;
pub use topic::{SequencedTopic, Topic};
pub use topology::{
    HoldQueue, QueueTopology, SequenceConfig, SequenceFailure, TopologyBuilder, TopologyDeclarer,
    declare_topic,
};

#[cfg(feature = "audit")]
pub use audit::{AuditLog, ShoveAuditHandler};

use std::time::Duration;

/// Grace period for in-flight operations before closing connections.
pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

/// Backoff delay before reconnecting after a consumer disconnect.
pub(crate) const RECONNECT_DELAY: Duration = Duration::from_secs(5);

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
        consumer::SqsConsumer,
        consumer_group::{SqsConsumerGroup, SqsConsumerGroupConfig},
        registry::SqsConsumerGroupRegistry,
        stats::{SqsQueueStats, SqsQueueStatsProvider},
        topology::QueueRegistry,
    };
}

#[cfg(feature = "rabbitmq")]
pub mod rabbitmq {
    pub use crate::backends::rabbitmq::{
        autoscaler::{Autoscaler, AutoscalerConfig},
        client::{RabbitMqClient, RabbitMqConfig},
        consumer::RabbitMqConsumer,
        consumer_group::{ConsumerGroup, ConsumerGroupConfig},
        management::{ManagementConfig, QueueStats, QueueStatsProvider},
        publisher::RabbitMqPublisher,
        registry::ConsumerGroupRegistry,
        topology::RabbitMqTopologyDeclarer,
    };
}
