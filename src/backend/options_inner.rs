//! `ConsumerOptionsInner` — un-generic lowering of `ConsumerOptions<B>`
//! passed across the internal trait boundary so backend impls don't need
//! to name `B`. See DESIGN_V2.md §8.3.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct ConsumerOptionsInner {
    pub max_retries: u32,
    pub prefetch_count: u16,
    /// Phase 12 adds the matching field on `ConsumerOptions`. Until then this
    /// flag is set but never read; mark it allowed to suppress dead-code
    /// churn while the rest of the struct is wired up.
    #[allow(dead_code)]
    pub concurrent_processing: bool,
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
    /// Lossy but lossless-within-shape conversion into the current public
    /// `ConsumerOptions`. The `concurrent_processing` flag has no target
    /// field yet (Phase 12 adds it) and is dropped on the floor for now.
    ///
    /// Used during Phase 4 porting to bridge the new `ConsumerImpl` trait
    /// methods (which take `ConsumerOptionsInner`) down to the existing
    /// concrete inherent `run*` methods (which still take `ConsumerOptions`).
    #[allow(dead_code)] // consumed by per-backend `ConsumerImpl` impls as they port.
    pub(crate) fn into_consumer_options(self) -> crate::consumer::ConsumerOptions {
        crate::consumer::ConsumerOptions {
            max_retries: self.max_retries,
            prefetch_count: self.prefetch_count,
            shutdown: self.shutdown,
            processing: self.processing,
            handler_timeout: self.handler_timeout,
            max_pending_per_key: self.max_pending_per_key,
            max_message_size: self.max_message_size,
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: self.exactly_once,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: self.receive_batch_size,
            #[cfg(feature = "nats")]
            max_ack_pending: self.max_ack_pending,
        }
    }
}
