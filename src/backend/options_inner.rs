//! `ConsumerOptionsInner` — un-generic lowering of `ConsumerOptions<B>`
//! passed across the internal trait boundary so backend impls don't need
//! to name `B`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

#[cfg(feature = "kafka")]
use crate::backends::kafka::KafkaAutoOffsetReset;
use crate::consumer::{
    DEFAULT_HANDLER_TIMEOUT, DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY,
    validate_message_size,
};
use crate::error::Result;
use crate::outcome::Outcome;
#[cfg(feature = "kafka-schema-registry")]
use crate::schema_registry::{SchemaEnforcement, SchemaRegistry};

#[derive(Clone)]
#[allow(dead_code)] // Fields are read by backend consumers behind feature gates.
pub(crate) struct ConsumerOptionsInner {
    pub max_retries: u32,
    pub prefetch_count: u16,
    pub handler_timeout: Option<Duration>,
    /// What a handler timeout resolves to. `None` keeps each backend's
    /// historical default (`Outcome::Retry` everywhere except Redis, which
    /// leaves the entry in the PEL for `XAUTOCLAIM`). Set via
    /// `ConsumerOptions::with_handler_timeout_outcome`.
    pub handler_timeout_outcome: Option<Outcome>,
    pub max_pending_per_key: Option<usize>,
    pub max_message_size: Option<usize>,
    pub max_reconnect_attempts: Option<u32>,
    /// Maximum time a sequence key may remain in the AwaitingRetry state
    /// before its pending messages are dead-lettered (RabbitMQ only).
    #[cfg(feature = "rabbitmq")]
    pub hold_queue_timeout: Option<Duration>,
    pub shutdown: CancellationToken,
    pub processing: Arc<AtomicBool>,
    /// Consumer-group name for metrics labels. `None` is treated as `"default"`.
    pub consumer_group: Option<Arc<str>>,

    /// Kafka-only: explicit `group.id` to pass to rdkafka instead of the
    /// default `"{queue}-consumer"`. Used to implement fan-out (arch-K-3):
    /// two independent services consuming the same topic need distinct group
    /// IDs so they each receive all messages rather than sharing partitions.
    #[cfg(feature = "kafka")]
    pub kafka_group_id: Option<Arc<str>>,

    /// Kafka-only: rdkafka `auto.offset.reset` override. `None` falls back
    /// to the library default of `earliest`. Propagated from
    /// `KafkaConsumerGroupConfig::with_auto_offset_reset`.
    #[cfg(feature = "kafka")]
    pub kafka_auto_offset_reset: Option<KafkaAutoOffsetReset>,

    /// Kafka-only: Schema Registry client for decoding Confluent wire-framed
    /// messages. `None` disables registry-based decoding.
    #[cfg(feature = "kafka-schema-registry")]
    pub schema_registry: Option<Arc<SchemaRegistry>>,
    /// Kafka-only: how subject mismatches are handled. Default `Enforce`.
    #[cfg(feature = "kafka-schema-registry")]
    pub schema_enforcement: SchemaEnforcement,
    /// Kafka-only: accepted subject set. `None` derives `["{queue}-value"]` at
    /// decode time.
    // Read by the decode stage (Task 7).
    #[cfg(feature = "kafka-schema-registry")]
    pub schema_accepted_subjects: Option<Vec<Arc<str>>>,

    #[cfg(feature = "rabbitmq-transactional")]
    pub exactly_once: bool,
    #[cfg(feature = "aws-sns-sqs")]
    pub receive_batch_size: u16,
    #[cfg(feature = "nats")]
    pub max_ack_pending: Option<i64>,
}

impl ConsumerOptionsInner {
    /// Crate-internal constructor used by per-backend fallback paths
    /// (e.g. DLQ consumer loops) that need a plain `ConsumerOptionsInner`
    /// with library defaults bound to a supplied shutdown token.
    #[allow(dead_code)] // Used only by feature-gated backend consumers.
    pub(crate) fn defaults_with_shutdown(shutdown: CancellationToken) -> Self {
        Self {
            max_retries: 10,
            prefetch_count: 10,
            handler_timeout: Some(DEFAULT_HANDLER_TIMEOUT),
            handler_timeout_outcome: None,
            max_pending_per_key: Some(DEFAULT_MAX_PENDING_PER_KEY),
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            max_reconnect_attempts: None,
            #[cfg(feature = "rabbitmq")]
            hold_queue_timeout: None,
            shutdown,
            processing: Arc::new(AtomicBool::new(false)),
            consumer_group: None,
            #[cfg(feature = "kafka")]
            kafka_group_id: None,
            #[cfg(feature = "kafka")]
            kafka_auto_offset_reset: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_registry: None,
            #[cfg(feature = "kafka-schema-registry")]
            schema_enforcement: SchemaEnforcement::Enforce,
            #[cfg(feature = "kafka-schema-registry")]
            schema_accepted_subjects: None,
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: false,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: 0,
            #[cfg(feature = "nats")]
            max_ack_pending: None,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn test_shutdown() -> CancellationToken {
        CancellationToken::new()
    }
}

impl ConsumerOptionsInner {
    /// Returns `Ok(())` if the payload is within the configured
    /// `max_message_size`, or an error if it exceeds the limit. Always
    /// succeeds when no limit is set.
    ///
    /// Only some backend consumers route through this helper; a feature-limited
    /// build (e.g. just `aws-sns-sqs`) may not call it at all.
    #[allow(dead_code)]
    pub(crate) fn validate_payload_message_size(&self, len: usize) -> Result<()> {
        validate_message_size(len, self.max_message_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_with_shutdown_max_reconnect_attempts_is_none() {
        let opts = ConsumerOptionsInner::defaults_with_shutdown(CancellationToken::new());
        assert!(
            opts.max_reconnect_attempts.is_none(),
            "max_reconnect_attempts must default to None (unlimited)"
        );
    }

    #[test]
    fn defaults_with_shutdown_has_sensible_values() {
        let opts = ConsumerOptionsInner::defaults_with_shutdown(CancellationToken::new());
        assert_eq!(opts.max_retries, 10);
        assert_eq!(opts.prefetch_count, 10);
        assert!(opts.handler_timeout.is_some());
        assert!(opts.max_pending_per_key.is_some());
        assert!(opts.max_message_size.is_some());
        assert!(opts.consumer_group.is_none());
    }
}
