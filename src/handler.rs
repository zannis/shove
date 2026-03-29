use std::sync::Arc;

use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::outcome::Outcome;
use crate::topic::Topic;

/// Handler for processing messages from a topic's queues.
///
/// Parameterized on the `Topic`, not the message type directly.
/// This ensures the handler is bound to a specific topic and prevents
/// accidentally reusing a handler across topics that share a message type.
pub trait MessageHandler<T: Topic>: Send + Sync + 'static {
    /// Process a message from the main queue.
    fn handle(
        &self,
        message: T::Message,
        metadata: MessageMetadata,
    ) -> impl Future<Output = Outcome> + Send;

    /// Process a message from the dead-letter queue.
    ///
    /// The message is always acked (removed from DLQ) after this returns.
    /// Override for logging, alerting, or investigation.
    fn handle_dead(
        &self,
        _message: T::Message,
        metadata: DeadMessageMetadata,
    ) -> impl Future<Output = ()> + Send {
        async move {
            tracing::warn!(
                delivery_id = %metadata.message.delivery_id,
                reason = metadata.reason.as_deref().unwrap_or("unknown"),
                original_queue = metadata.original_queue.as_deref().unwrap_or("unknown"),
                death_count = metadata.death_count,
                "Dead-letter message received, no handler implemented"
            );
        }
    }
}

// Blanket impl: Arc<H> delegates to H. This allows sharing handlers across tasks.
impl<T: Topic, H: MessageHandler<T>> MessageHandler<T> for Arc<H> {
    fn handle(
        &self,
        message: T::Message,
        metadata: MessageMetadata,
    ) -> impl Future<Output = Outcome> + Send {
        (**self).handle(message, metadata)
    }

    fn handle_dead(
        &self,
        message: T::Message,
        metadata: DeadMessageMetadata,
    ) -> impl Future<Output = ()> + Send {
        (**self).handle_dead(message, metadata)
    }
}
