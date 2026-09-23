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
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use consumer::completion_probe;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use consumer::put_back_probe;
pub use consumer::{BatchConsumerOptions, KafkaConsumer};
pub(crate) use consumer_group::validate_commit_interval;

/// Test-only seam (see the `test-support` feature): the deadline the
/// concurrent receive loop gives its final synchronous commit at shutdown,
/// so an integration test that times a shutdown against a frozen broker
/// asserts against the constant itself rather than a hand-copied value.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn shutdown_commit_deadline_for_test() -> std::time::Duration {
    constants::SHUTDOWN_COMMIT_DEADLINE
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use consumer::fence_probe;

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use consumer::final_commit_spawn_probe;

/// Test-only seam (see the `test-support` feature): the `session.timeout.ms`
/// every shove consumer is created with, so a test that waits past it asserts
/// against the constant itself rather than a hand-copied value.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn session_timeout_for_test() -> std::time::Duration {
    std::time::Duration::from_millis(u64::from(constants::SESSION_TIMEOUT_MS))
}
pub use consumer_group::{
    KafkaAutoOffsetReset, KafkaConsumerGroup, KafkaConsumerGroupConfig, KafkaConsumerGroupRegistry,
};
pub use offset_reset::{KafkaOffsetReset, KafkaOffsetResetReport, KafkaPartitionOffsetReset};
pub(crate) use offset_reset::{reset_group_offsets, resolved_reset_group_id};
pub use publisher::{KafkaPublisher, KafkaPublisherConfig};
pub use topology::KafkaTopologyDeclarer;
