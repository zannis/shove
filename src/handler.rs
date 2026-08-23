use std::sync::Arc;

use crate::audit::{AuditHandler, Audited};
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::outcome::Outcome;
use crate::topic::Topic;

/// Handler for processing messages from a topic's queues.
///
/// Parameterized on the `Topic`, not the message type directly.
/// This ensures the handler is bound to a specific topic and prevents
/// accidentally reusing a handler across topics that share a message type.
pub trait MessageHandler<T: Topic>: Send + Sync + 'static {
    /// Shared dependencies injected on every invocation. `Clone` is required so
    /// the harness can clone it into each consumer task; in practice this is
    /// almost always an `Arc<AppState>`, where the clone is a refcount bump.
    type Context: Clone + Send + Sync + 'static;

    /// Process a message from the main queue.
    fn handle(
        &self,
        message: T::Message,
        metadata: MessageMetadata,
        ctx: &Self::Context,
    ) -> impl Future<Output = Outcome> + Send;

    /// Process a message from the dead-letter queue.
    ///
    /// The message is always acked (removed from DLQ) after this returns.
    /// Override for logging, alerting, or investigation.
    fn handle_dead(
        &self,
        _message: T::Message,
        metadata: DeadMessageMetadata,
        _ctx: &Self::Context,
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

/// Fluent extension trait for wrapping a handler in audit instrumentation.
///
/// Lets you write `MyHandler::new(state).audited(audit)` instead of
/// `Audited::new(MyHandler::new(state), audit)`. The wrapper's outcome and
/// retry behaviour is identical to calling `Audited::new` directly.
pub trait MessageHandlerExt<T: Topic>: MessageHandler<T> + Sized {
    /// Wrap `self` with the given audit handler, returning the combined
    /// [`Audited<Self, A>`](Audited).
    fn audited<A>(self, audit: A) -> Audited<Self, A>
    where
        A: AuditHandler<T>;
}

impl<T: Topic, H: MessageHandler<T>> MessageHandlerExt<T> for H {
    fn audited<A>(self, audit: A) -> Audited<Self, A>
    where
        A: AuditHandler<T>,
    {
        Audited::new(self, audit)
    }
}

/// Handler for processing a bounded batch of messages from a topic's main
/// queue in one call.
///
/// Parameterized on the `Topic`, not the message type directly, for the same
/// reason as [`MessageHandler`].
///
/// # Batch semantics
///
/// A batch handler returns a **single** [`Outcome`] for the whole batch,
/// not one per message — see the batch-consuming entry point on each
/// backend's consumer for exactly how each outcome is applied:
///
/// - `Ack` commits every message's offset in one batched commit — the
///   primitive this trait exists for (see the module-level rationale on
///   the batch entry point: the classic accumulate-then-flush pattern DB
///   sinks reimplement by hand around a single-message consumer).
/// - `Reject` is terminal for the **whole batch**, as it is on the
///   single-message path: every message in it is published to the DLQ (if
///   the topology declares one) and the offsets commit. Return it only for
///   a batch that will never become acceptable.
/// - `Retry` and `Defer` redeliver the entire batch as a unit, after an
///   escalating delay. There is no per-message retry-count tracking or
///   per-message DLQ routing at the batch level, so a handler that needs to
///   split a partially-failed batch should ack what it durably wrote and
///   return `Retry` only for the remainder on its *next* delivery, or handle
///   partial failure inside `handle_batch` itself (e.g. write successes,
///   then return `Retry` so the whole batch — successes included, which
///   your sink should treat idempotently — is redelivered for the rest).
pub trait BatchMessageHandler<T: Topic>: Send + Sync + 'static {
    /// Shared dependencies injected on every invocation — see
    /// [`MessageHandler::Context`].
    type Context: Clone + Send + Sync + 'static;

    /// Process a batch of messages, each paired with its own metadata
    /// (including its individual `retry_count`, in case a handler wants to
    /// make its own per-message decisions before returning a single
    /// batch-wide `Outcome`).
    fn handle_batch(
        &self,
        messages: Vec<(T::Message, MessageMetadata)>,
        ctx: &Self::Context,
    ) -> impl Future<Output = Outcome> + Send;
}

// Blanket impl: Arc<H> delegates to H. This allows sharing handlers across tasks.
impl<T: Topic, H: BatchMessageHandler<T>> BatchMessageHandler<T> for Arc<H> {
    type Context = H::Context;

    fn handle_batch(
        &self,
        messages: Vec<(T::Message, MessageMetadata)>,
        ctx: &H::Context,
    ) -> impl Future<Output = Outcome> + Send {
        (**self).handle_batch(messages, ctx)
    }
}

// Blanket impl: Arc<H> delegates to H. This allows sharing handlers across tasks.
impl<T: Topic, H: MessageHandler<T>> MessageHandler<T> for Arc<H> {
    type Context = H::Context;

    fn handle(
        &self,
        message: T::Message,
        metadata: MessageMetadata,
        ctx: &H::Context,
    ) -> impl Future<Output = Outcome> + Send {
        (**self).handle(message, metadata, ctx)
    }

    fn handle_dead(
        &self,
        message: T::Message,
        metadata: DeadMessageMetadata,
        ctx: &H::Context,
    ) -> impl Future<Output = ()> + Send {
        (**self).handle_dead(message, metadata, ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::metadata::{DeadMessageMetadata, MessageMetadata};
    use crate::outcome::Outcome;
    use crate::topology::{QueueTopology, TopologyBuilder};

    // -- test Topic --

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct TestMessage {
        value: u32,
    }

    struct TestTopic;
    impl Topic for TestTopic {
        type Message = TestMessage;
        type Codec = crate::JsonCodec;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<QueueTopology> = std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("handler-test").build())
        }
    }

    // -- test handlers --

    struct FixedOutcomeHandler(Outcome);
    impl MessageHandler<TestTopic> for FixedOutcomeHandler {
        type Context = ();
        async fn handle(&self, _msg: TestMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.0.clone()
        }
    }

    // -- helpers --

    fn test_metadata() -> MessageMetadata {
        MessageMetadata {
            retry_count: 0,
            delivery_id: "d-1".into(),
            redelivered: false,
            delivery_count: Some(1),
            headers: Arc::new(HashMap::new()),
        }
    }

    fn test_dead_metadata() -> DeadMessageMetadata {
        DeadMessageMetadata {
            message: test_metadata(),
            reason: Some("rejected".into()),
            original_queue: Some("handler-test".into()),
            death_count: 1,
        }
    }

    fn test_message() -> TestMessage {
        TestMessage { value: 42 }
    }

    // -- tests --

    /// Default `handle_dead` returns `()` without panicking.
    #[tokio::test]
    async fn default_handle_dead_returns_unit() {
        let handler = FixedOutcomeHandler(Outcome::Ack);
        // The default impl just logs; calling it must not panic and must return ().
        handler
            .handle_dead(test_message(), test_dead_metadata(), &())
            .await;
    }

    /// `Arc<H>` delegates `handle` to the inner handler and returns the correct outcome.
    #[tokio::test]
    async fn arc_blanket_handle_delegates_correctly() {
        let handler = Arc::new(FixedOutcomeHandler(Outcome::Ack));
        let outcome = handler.handle(test_message(), test_metadata(), &()).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    /// `Arc<H>` delegates `handle` for all outcome variants.
    #[tokio::test]
    async fn arc_blanket_handle_retry_outcome() {
        let handler = Arc::new(FixedOutcomeHandler(Outcome::Retry));
        let outcome = handler.handle(test_message(), test_metadata(), &()).await;
        assert!(matches!(outcome, Outcome::Retry));
    }

    /// `Arc<H>` delegates `handle_dead` to the inner handler's default impl without panicking.
    #[tokio::test]
    async fn arc_blanket_handle_dead_delegates_correctly() {
        let handler = Arc::new(FixedOutcomeHandler(Outcome::Ack));
        // Calling through Arc must reach the same default impl and return ().
        handler
            .handle_dead(test_message(), test_dead_metadata(), &())
            .await;
    }

    /// Handlers with a concrete `Context` receive it by reference on every call.
    #[tokio::test]
    async fn handle_receives_context_by_reference() {
        struct CtxHandler;
        impl MessageHandler<TestTopic> for CtxHandler {
            type Context = u32;
            async fn handle(
                &self,
                _msg: TestMessage,
                _meta: MessageMetadata,
                ctx: &u32,
            ) -> Outcome {
                assert_eq!(*ctx, 42);
                Outcome::Ack
            }
        }
        let outcome = CtxHandler
            .handle(test_message(), test_metadata(), &42)
            .await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    /// `MessageHandlerExt::audited` wraps a handler with an `Audited<Self, A>`
    /// adapter and preserves its outcome.
    #[tokio::test]
    async fn audited_extension_wraps_handler() {
        use crate::audit::{AuditHandler, AuditRecord};
        use crate::error::Result;

        struct NullAudit;
        impl AuditHandler<TestTopic> for NullAudit {
            async fn audit(&self, _rec: &AuditRecord<TestMessage>) -> Result<()> {
                Ok(())
            }
        }

        let wrapped = FixedOutcomeHandler(Outcome::Ack).audited(NullAudit);
        let outcome = wrapped.handle(test_message(), test_metadata(), &()).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    /// `Arc<H>` forwards the caller-supplied `ctx` reference unchanged.
    #[tokio::test]
    async fn arc_blanket_handle_forwards_context() {
        struct CtxHandler;
        impl MessageHandler<TestTopic> for CtxHandler {
            type Context = u32;
            async fn handle(
                &self,
                _msg: TestMessage,
                _meta: MessageMetadata,
                ctx: &u32,
            ) -> Outcome {
                assert_eq!(*ctx, 7);
                Outcome::Ack
            }
        }

        let handler = Arc::new(CtxHandler);
        let outcome = handler.handle(test_message(), test_metadata(), &7).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    // -- BatchMessageHandler --

    struct FixedOutcomeBatchHandler(Outcome);
    impl BatchMessageHandler<TestTopic> for FixedOutcomeBatchHandler {
        type Context = ();
        async fn handle_batch(
            &self,
            _messages: Vec<(TestMessage, MessageMetadata)>,
            _: &(),
        ) -> Outcome {
            self.0.clone()
        }
    }

    /// A batch handler receives every message passed to it, in order.
    #[tokio::test]
    async fn batch_handler_receives_all_messages_in_order() {
        struct CollectingBatchHandler;
        impl BatchMessageHandler<TestTopic> for CollectingBatchHandler {
            type Context = ();
            async fn handle_batch(
                &self,
                messages: Vec<(TestMessage, MessageMetadata)>,
                _: &(),
            ) -> Outcome {
                let values: Vec<u32> = messages.iter().map(|(m, _)| m.value).collect();
                assert_eq!(values, vec![1, 2, 3]);
                Outcome::Ack
            }
        }

        let batch = vec![
            (TestMessage { value: 1 }, test_metadata()),
            (TestMessage { value: 2 }, test_metadata()),
            (TestMessage { value: 3 }, test_metadata()),
        ];
        let outcome = CollectingBatchHandler.handle_batch(batch, &()).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    /// `Arc<H>` delegates `handle_batch` to the inner handler and returns
    /// the correct outcome.
    #[tokio::test]
    async fn arc_blanket_handle_batch_delegates_correctly() {
        let handler = Arc::new(FixedOutcomeBatchHandler(Outcome::Retry));
        let outcome = handler
            .handle_batch(vec![(test_message(), test_metadata())], &())
            .await;
        assert!(matches!(outcome, Outcome::Retry));
    }

    /// An empty batch is a valid (if degenerate) call — the handler still
    /// decides the outcome.
    #[tokio::test]
    async fn batch_handler_accepts_empty_batch() {
        let outcome = FixedOutcomeBatchHandler(Outcome::Ack)
            .handle_batch(Vec::new(), &())
            .await;
        assert!(matches!(outcome, Outcome::Ack));
    }
}
