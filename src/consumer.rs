use tokio_util::sync::CancellationToken;

use crate::error::ShoveError;
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

/// Options for consumer behavior.
#[derive(Debug, Clone)]
pub struct ConsumerOptions {
    /// Maximum retries before automatically rejecting to DLQ.
    /// Only relevant if the topic has hold queues.
    pub max_retries: u32,
    /// Prefetch count (number of unacked messages the broker will deliver).
    pub prefetch_count: u16,
    /// Cancellation token for graceful shutdown. When triggered, the consumer
    /// finishes processing the current message, acks it, and returns `Ok(())`.
    pub shutdown: CancellationToken,
}

impl ConsumerOptions {
    /// Create consumer options with the given shutdown token.
    /// Uses defaults: `max_retries = 10`, `prefetch_count = 10`.
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            max_retries: 10,
            prefetch_count: 10,
            shutdown,
        }
    }

    /// Set the maximum number of retries before rejecting to DLQ.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the prefetch count (number of unacked messages the broker will deliver).
    pub fn with_prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }
}

/// Consume messages from a topic's queues.
///
/// This trait is intentionally **not object-safe** — methods are generic over
/// `T: Topic`. Backends are always concrete types (e.g., `RabbitMqConsumer`),
/// not `dyn Consumer`. For test doubles, implement the trait on a mock struct
/// or use an in-memory backend.
pub trait Consumer: Send + Sync + 'static {
    /// Run the main consumer loop. Blocks until shutdown signal.
    ///
    /// The consumer:
    /// 1. Reads `T::topology()` to resolve queue names
    /// 2. Consumes from the main queue
    /// 3. Deserializes to `T::Message`
    /// 4. Calls `handler.handle()`
    /// 5. Routes based on `Outcome` (ack, retry → hold, reject → DLQ)
    fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<(), ShoveError>> + Send;

    /// Run the consumer loop with sequenced (ordered) delivery.
    /// Blocks until shutdown signal.
    ///
    /// Messages sharing the same sequence key are delivered in strict order.
    /// Different sequence keys are independent and may be processed concurrently.
    ///
    /// `ConsumerOptions::prefetch_count` is ignored — sequenced consumers
    /// always use `prefetch_count = 1` per sub-queue to guarantee ordering.
    ///
    /// Returns `Err(ShoveError::Topology)` if `T::topology().sequencing` is `None`.
    fn run_sequenced<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<(), ShoveError>> + Send;

    /// Run a DLQ consumer loop for the topic. Blocks until shutdown signal.
    ///
    /// Calls `handler.handle_dead()` for each message, then always acks.
    ///
    /// Returns `Err(ShoveError::Topology)` if `T::topology().dlq` is `None`.
    fn run_dlq<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
    ) -> impl Future<Output = Result<(), ShoveError>> + Send;
}
