//! `ConsumerOptionsInner` — un-generic lowering of `ConsumerOptions<B>`
//! passed across the internal trait boundary so backend impls don't need
//! to name `B`. See DESIGN_V2.md §8.3.

// Skeleton: constructors and consumers land in subsequent phases.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct ConsumerOptionsInner {
    pub max_retries: u32,
    pub prefetch_count: u16,
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
