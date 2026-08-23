pub mod autoscaler;
mod backend;
mod client;
mod constants;
mod consumer;
mod consumer_group;
#[cfg(feature = "kafka-msk-iam")]
mod msk_iam;
mod offset_reset;
mod publisher;
mod topology;

pub use autoscaler::{
    KafkaAutoscalerBackend, KafkaLagStatsProvider, KafkaQueueStats, KafkaQueueStatsProvider,
};
#[cfg(all(feature = "kafka-msk-iam", feature = "test-support"))]
pub use client::prime_admin_oauth_token_for_test;
pub use client::{KafkaClient, KafkaCompression, KafkaConfig};
#[cfg(feature = "kafka-ssl")]
pub use client::{KafkaSasl, KafkaTls};
pub use consumer::{BatchConsumerOptions, KafkaConsumer};
pub use consumer_group::{
    KafkaAutoOffsetReset, KafkaConsumerGroup, KafkaConsumerGroupConfig, KafkaConsumerGroupRegistry,
};
pub use offset_reset::{KafkaOffsetReset, KafkaOffsetResetReport, KafkaPartitionOffsetReset};
pub(crate) use offset_reset::{reset_group_offsets, resolved_reset_group_id};
pub use publisher::{KafkaPublisher, KafkaPublisherConfig};
pub use topology::KafkaTopologyDeclarer;
