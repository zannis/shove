pub mod autoscaler;
mod backend;
mod client;
mod constants;
mod consumer;
mod consumer_group;
#[cfg(feature = "kafka-msk-iam")]
mod msk_iam;
mod publisher;
mod topology;

pub use autoscaler::{
    KafkaAutoscalerBackend, KafkaLagStatsProvider, KafkaQueueStats, KafkaQueueStatsProvider,
};
#[cfg(all(feature = "kafka-msk-iam", feature = "test-support"))]
pub use client::prime_admin_oauth_token_for_test;
pub use client::{KafkaClient, KafkaConfig, KafkaProducerTuning};
#[cfg(feature = "kafka-ssl")]
pub use client::{KafkaSasl, KafkaTls};
pub use consumer::KafkaConsumer;
pub use consumer_group::{
    KafkaAutoOffsetReset, KafkaConsumerGroup, KafkaConsumerGroupConfig, KafkaConsumerGroupRegistry,
};
pub use publisher::{KafkaPublisher, KafkaPublisherConfig};
pub use topology::KafkaTopologyDeclarer;
