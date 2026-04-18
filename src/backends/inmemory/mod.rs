//! Internal in-process broker implementation. Public surface and user-facing
//! docs live in [`crate::inmemory`].

mod autoscaler;
mod client;
mod constants;
mod consumer;
mod consumer_group;
mod publisher;
mod topology;

pub use autoscaler::{
    BrokerStatsProvider, InMemoryAutoscalerBackend, InMemoryQueueStats, InMemoryQueueStatsProvider,
};
pub use client::{InMemoryBroker, InMemoryConfig};
pub use constants::DEFAULT_QUEUE_CAPACITY;
pub use consumer::InMemoryConsumer;
pub use consumer_group::{
    InMemoryConsumerGroup, InMemoryConsumerGroupConfig, InMemoryConsumerGroupRegistry,
};
pub use publisher::InMemoryPublisher;
pub use topology::InMemoryTopologyDeclarer;
