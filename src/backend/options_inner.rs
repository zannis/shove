//! `ConsumerOptionsInner` — un-generic lowering of `ConsumerOptions<B>`
//! passed across the internal trait boundary so backend impls don't need
//! to name `B`. See DESIGN_V2.md §8.3.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::consumer::{
    DEFAULT_HANDLER_TIMEOUT, DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_PENDING_PER_KEY,
    validate_message_size,
};
use crate::error::Result;

#[derive(Clone)]
pub(crate) struct ConsumerOptionsInner {
    pub max_retries: u32,
    pub prefetch_count: u16,
    pub handler_timeout: Option<Duration>,
    pub max_pending_per_key: Option<usize>,
    pub max_message_size: Option<usize>,
    pub shutdown: CancellationToken,
    pub processing: Arc<AtomicBool>,

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
    pub(crate) fn defaults_with_shutdown(shutdown: CancellationToken) -> Self {
        Self {
            max_retries: 10,
            prefetch_count: 10,
            handler_timeout: Some(DEFAULT_HANDLER_TIMEOUT),
            max_pending_per_key: Some(DEFAULT_MAX_PENDING_PER_KEY),
            max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
            shutdown,
            processing: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: false,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: 0,
            #[cfg(feature = "nats")]
            max_ack_pending: None,
        }
    }

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
