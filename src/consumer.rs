use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

/// Options for consumer behavior.
#[derive(Clone)]
pub struct ConsumerOptions {
    /// Maximum retries before automatically rejecting to DLQ.
    /// Only relevant if the topic has hold queues.
    pub max_retries: u32,
    /// Prefetch count (number of unacked messages the broker will deliver).
    pub prefetch_count: u16,
    /// Cancellation token for graceful shutdown. When triggered, the consumer
    /// finishes processing the current message, acks it, and returns `Ok(())`.
    pub shutdown: CancellationToken,
    /// Flag that indicates the consumer is currently processing a message.
    /// Used by the autoscaler to avoid scaling down busy consumers.
    pub processing: Arc<AtomicBool>,
    /// Maximum time a handler may spend processing a single message.
    /// If the handler exceeds this duration the message is retried.
    /// `None` means no timeout (the handler may run indefinitely).
    pub handler_timeout: Option<Duration>,
    /// Maximum number of locally buffered messages per sequence key in
    /// concurrent-sequenced consumers. When the limit is reached, new
    /// deliveries for that key are rejected to the DLQ.
    /// `None` means no limit (default).
    pub max_pending_per_key: Option<usize>,
}

impl ConsumerOptions {
    /// Create consumer options with the given shutdown token.
    /// Uses defaults: `max_retries = 10`, `prefetch_count = 10`.
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            max_retries: 10,
            prefetch_count: 10,
            shutdown,
            processing: Arc::new(AtomicBool::new(false)),
            handler_timeout: None,
            max_pending_per_key: None,
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

    /// Set the handler timeout. If a handler exceeds this duration the message
    /// is retried automatically.
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        self.handler_timeout = Some(timeout);
        self
    }

    /// Set the maximum number of locally buffered messages per sequence key.
    /// When exceeded, new deliveries for that key are rejected to the DLQ.
    pub fn with_max_pending_per_key(mut self, limit: usize) -> Self {
        self.max_pending_per_key = Some(limit);
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
    /// Run the main consumer loop sequentially. Blocks until shutdown signal.
    ///
    /// Equivalent to [`run_concurrent`](Consumer::run_concurrent) with
    /// `prefetch_count = 1`. Prefer `run_concurrent` for most workloads —
    /// it provides much higher throughput for I/O-bound handlers.
    fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let options = ConsumerOptions {
            prefetch_count: 1,
            ..options
        };
        self.run_concurrent(handler, options)
    }

    /// Run the consumer loop with concurrent message processing.
    /// Blocks until shutdown signal.
    ///
    /// Processes up to `prefetch_count` messages concurrently within the same
    /// consumer task, while **always acknowledging messages in delivery order**.
    /// This significantly improves throughput for handlers with I/O latency
    /// (HTTP calls, database queries, etc.) without requiring additional
    /// consumer instances.
    fn run_concurrent<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Run the consumer loop with sequenced (ordered) delivery.
    /// Blocks until shutdown signal.
    ///
    /// Messages sharing the same sequence key are delivered in strict order.
    /// Different sequence keys are independent and may be processed concurrently
    /// within the same shard.
    ///
    /// Each shard prefetches up to `ConsumerOptions::prefetch_count` messages.
    /// Messages for idle keys are processed immediately; messages for busy keys
    /// (in-flight handler or awaiting retry) are buffered locally and drained
    /// sequentially when the key becomes free. This avoids redelivery storms
    /// while consuming prefetch slots as natural back-pressure.
    ///
    /// Returns `Err(ShoveError::Topology)` if `T::topology().sequencing` is `None`.
    fn run_sequenced<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T>,
        options: ConsumerOptions,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Run a DLQ consumer loop for the topic. Blocks until shutdown signal.
    ///
    /// Calls `handler.handle_dead()` for each message, then always acks.
    ///
    /// Returns `Err(ShoveError::Topology)` if `T::topology().dlq` is `None`.
    fn run_dlq<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_correct() {
        let opts = ConsumerOptions::new(CancellationToken::new());
        assert_eq!(opts.max_retries, 10);
        assert_eq!(opts.prefetch_count, 10);
        assert!(opts.handler_timeout.is_none());
        assert!(opts.max_pending_per_key.is_none());
        assert!(!opts.processing.load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn with_max_retries_overrides() {
        let opts = ConsumerOptions::new(CancellationToken::new()).with_max_retries(5);
        assert_eq!(opts.max_retries, 5);
    }

    #[test]
    fn with_prefetch_count_overrides() {
        let opts = ConsumerOptions::new(CancellationToken::new()).with_prefetch_count(50);
        assert_eq!(opts.prefetch_count, 50);
    }

    #[test]
    fn with_handler_timeout_sets_timeout() {
        let opts = ConsumerOptions::new(CancellationToken::new())
            .with_handler_timeout(Duration::from_secs(30));
        assert_eq!(opts.handler_timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn builder_chains() {
        let opts = ConsumerOptions::new(CancellationToken::new())
            .with_max_retries(3)
            .with_prefetch_count(20)
            .with_handler_timeout(Duration::from_secs(5))
            .with_max_pending_per_key(100);
        assert_eq!(opts.max_retries, 3);
        assert_eq!(opts.prefetch_count, 20);
        assert_eq!(opts.handler_timeout, Some(Duration::from_secs(5)));
        assert_eq!(opts.max_pending_per_key, Some(100));
    }

    #[test]
    fn shutdown_token_propagated() {
        let token = CancellationToken::new();
        let opts = ConsumerOptions::new(token.clone());
        assert!(!opts.shutdown.is_cancelled());
        token.cancel();
        assert!(opts.shutdown.is_cancelled());
    }

    #[test]
    fn with_max_pending_per_key_sets_value() {
        let opts = ConsumerOptions::new(CancellationToken::new()).with_max_pending_per_key(50);
        assert_eq!(opts.max_pending_per_key, Some(50));
    }
}
