pub mod autoscaler;
mod client;
mod consumer;
mod consumer_group;
mod publisher;
mod topology;

pub use autoscaler::{
    JetStreamStatsProvider, NatsAutoscalerBackend, NatsQueueStats, NatsQueueStatsProvider,
};
pub use client::{NatsClient, NatsConfig};
pub use consumer::NatsConsumer;
pub use consumer_group::{NatsConsumerGroup, NatsConsumerGroupConfig, NatsConsumerGroupRegistry};
pub use publisher::NatsPublisher;
pub use topology::NatsTopologyDeclarer;