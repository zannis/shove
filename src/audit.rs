use std::future::Future;
use std::time::{Duration, Instant};

use crate::error::Result;
#[cfg(test)]
use crate::error::ShoveError;
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::outcome::Outcome;
use crate::topic::Topic;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single audit record capturing one delivery attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord<M: Serialize> {
    /// Trace identifier propagated across retries via the `x-trace-id` header.
    pub trace_id: String,
    /// The topic name (queue name from the topology).
    pub topic: String,
    /// The deserialized message payload.
    pub payload: M,
    /// Delivery metadata.
    pub metadata: MessageMetadata,
    /// What the handler returned.
    pub outcome: Outcome,
    /// Wall-clock duration of the `handle()` call in milliseconds.
    pub duration_ms: u64,
    /// UTC timestamp when the handler completed.
    pub timestamp: DateTime<Utc>,
}

/// Trait for persisting audit records.
///
/// Integrators implement this for their chosen persistence backend.
/// Returning `Err` causes the message to be retried (strict auditing): the
/// message will be reprocessed even if the business handler already succeeded.
/// Handlers wrapped with [`Audited`] must therefore be **idempotent**.
pub trait AuditHandler<T: Topic>: Send + Sync + 'static {
    fn audit(&self, record: &AuditRecord<T::Message>) -> impl Future<Output = Result<()>> + Send;
}

/// Wraps a `MessageHandler` with audit logging.
///
/// `Audited<H, A>` implements `MessageHandler<T>`, so it is fully transparent
/// to consumers and consumer groups — no changes required at the call site.
///
/// **Idempotency requirement:** if the audit handler returns `Err`, the message
/// is retried regardless of what the business handler returned — including
/// `Ack`. The business handler may therefore run more than once for the same
/// message. Ensure your handler is idempotent (e.g. upsert by `delivery_id`).
///
/// Use [`Audited::with_audit_timeout`] to bound how long the audit handler may
/// block; on timeout the original outcome is preserved and no retry is forced.
pub struct Audited<H, A> {
    handler: H,
    audit_handler: A,
    /// Maximum time the audit handler may take before the message is retried.
    /// `None` means no timeout (default for backward compatibility).
    audit_timeout: Option<Duration>,
}

impl<H, A> Audited<H, A> {
    pub fn new(handler: H, audit_handler: A) -> Self {
        Self {
            handler,
            audit_handler,
            audit_timeout: None,
        }
    }

    /// Set the maximum time the audit handler may take. If exceeded, the
    /// original handler outcome is returned and the audit failure is logged.
    pub fn with_audit_timeout(mut self, timeout: Duration) -> Self {
        self.audit_timeout = Some(timeout);
        self
    }
}

impl<T, H, A> MessageHandler<T> for Audited<H, A>
where
    T: Topic,
    T::Message: Clone,
    H: MessageHandler<T>,
    A: AuditHandler<T>,
{
    type Context = H::Context;

    async fn handle(
        &self,
        message: T::Message,
        metadata: MessageMetadata,
        ctx: &H::Context,
    ) -> Outcome {
        // Guard against infinite recursion: skip auditing on the audit-log topic itself.
        #[cfg(feature = "audit")]
        {
            if T::topology().queue() == AuditLog::topology().queue() {
                return self.handler.handle(message, metadata, ctx).await;
            }
        }

        let trace_id = metadata
            .headers
            .get("x-trace-id")
            .cloned()
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let topic = T::topology().queue().to_owned();
        let payload_clone = message.clone();
        let metadata_clone = metadata.clone();

        let start = Instant::now();
        let outcome = self.handler.handle(message, metadata, ctx).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        let record = AuditRecord {
            trace_id,
            topic,
            payload: payload_clone,
            metadata: metadata_clone,
            outcome: outcome.clone(),
            duration_ms,
            timestamp: Utc::now(),
        };

        let audit_result = match self.audit_timeout {
            Some(timeout) => {
                match tokio::time::timeout(timeout, self.audit_handler.audit(&record)).await {
                    Ok(result) => result,
                    Err(_elapsed) => {
                        tracing::error!(
                            delivery_id = %record.metadata.delivery_id,
                            timeout_ms = timeout.as_millis() as u64,
                            "audit handler timed out, returning original outcome"
                        );
                        return outcome;
                    }
                }
            }
            None => self.audit_handler.audit(&record).await,
        };

        match audit_result {
            Ok(()) => outcome,
            Err(err) => {
                tracing::error!(
                    error = %err,
                    delivery_id = %record.metadata.delivery_id,
                    "audit handler failed, retrying message"
                );
                Outcome::Retry
            }
        }
    }

    fn handle_dead(
        &self,
        message: T::Message,
        metadata: DeadMessageMetadata,
        ctx: &H::Context,
    ) -> impl Future<Output = ()> + Send {
        self.handler.handle_dead(message, metadata, ctx)
    }
}

// --- ShoveAuditHandler: dogfood backend, gated behind `audit` feature ---

#[cfg(feature = "audit")]
mod shove_backend {
    use serde::Serialize;
    use serde_json::Value;

    use crate::audit::{AuditHandler, AuditRecord};
    use crate::backend::Backend;
    use crate::define_topic;
    use crate::error::{Result, ShoveError};
    use crate::publisher::Publisher;
    use crate::topic::Topic;
    use crate::topology::TopologyBuilder;

    define_topic!(
        pub AuditLog,
        AuditRecord<Value>,
        TopologyBuilder::new("shove-audit-log").dlq().build()
    );

    /// An `AuditHandler` that publishes audit records as messages to the
    /// `shove-audit-log` topic using any backend's `Publisher<B>`.
    pub struct ShoveAuditHandler<B: Backend> {
        publisher: Publisher<B>,
    }

    impl<B: Backend> ShoveAuditHandler<B> {
        pub fn new(publisher: Publisher<B>) -> Self {
            Self { publisher }
        }

        /// Convenience constructor that clones the borrowed publisher.
        pub fn for_publisher(publisher: &Publisher<B>) -> Self {
            Self {
                publisher: publisher.clone(),
            }
        }
    }

    impl<T, B> AuditHandler<T> for ShoveAuditHandler<B>
    where
        T: Topic,
        T::Message: Serialize,
        B: Backend,
    {
        async fn audit(&self, record: &AuditRecord<T::Message>) -> Result<()> {
            let value_record = AuditRecord {
                trace_id: record.trace_id.clone(),
                topic: record.topic.clone(),
                payload: serde_json::to_value(&record.payload)
                    .map_err(ShoveError::Serialization)?,
                metadata: record.metadata.clone(),
                outcome: record.outcome.clone(),
                duration_ms: record.duration_ms,
                timestamp: record.timestamp,
            };
            self.publisher.publish::<AuditLog>(&value_record).await
        }
    }
}

#[cfg(feature = "audit")]
#[cfg_attr(docsrs, doc(cfg(feature = "audit")))]
pub use shove_backend::{AuditLog, ShoveAuditHandler};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueueTopology;
    use crate::topology::TopologyBuilder;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::{Arc, OnceLock};

    // -- test Topic --

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct TestMessage {
        body: String,
    }

    struct TestTopic;
    impl Topic for TestTopic {
        type Message = TestMessage;
        fn topology() -> &'static QueueTopology {
            static TOPOLOGY: OnceLock<QueueTopology> = OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("audit-test").dlq().build())
        }
    }

    // -- mock handlers --

    struct FixedOutcomeHandler(Outcome);
    impl MessageHandler<TestTopic> for FixedOutcomeHandler {
        type Context = ();
        async fn handle(&self, _msg: TestMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            self.0.clone()
        }
    }

    struct OkAuditHandler;
    impl AuditHandler<TestTopic> for OkAuditHandler {
        async fn audit(&self, _record: &AuditRecord<TestMessage>) -> Result<()> {
            Ok(())
        }
    }

    /// Counts calls and captures the last trace_id.
    struct TrackingAuditHandler {
        call_count: AtomicU32,
        trace_id: tokio::sync::Mutex<Option<String>>,
    }
    impl TrackingAuditHandler {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                call_count: AtomicU32::new(0),
                trace_id: tokio::sync::Mutex::new(None),
            })
        }
    }
    impl AuditHandler<TestTopic> for Arc<TrackingAuditHandler> {
        async fn audit(&self, record: &AuditRecord<TestMessage>) -> Result<()> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            *self.trace_id.lock().await = Some(record.trace_id.clone());
            Ok(())
        }
    }

    struct FailingAuditHandler;
    impl AuditHandler<TestTopic> for FailingAuditHandler {
        async fn audit(&self, _record: &AuditRecord<TestMessage>) -> Result<()> {
            Err(ShoveError::Connection("audit publish failed".into()))
        }
    }

    fn test_metadata() -> MessageMetadata {
        MessageMetadata {
            retry_count: 0,
            delivery_id: "d-1".into(),
            redelivered: false,
            headers: HashMap::new(),
        }
    }

    fn test_message() -> TestMessage {
        TestMessage {
            body: "hello".into(),
        }
    }

    fn test_dead_metadata() -> DeadMessageMetadata {
        DeadMessageMetadata {
            message: test_metadata(),
            reason: Some("rejected".into()),
            original_queue: Some("audit-test".into()),
            death_count: 1,
        }
    }

    struct TrackingDeadHandler {
        called: AtomicBool,
    }
    impl TrackingDeadHandler {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                called: AtomicBool::new(false),
            })
        }
    }
    impl MessageHandler<TestTopic> for Arc<TrackingDeadHandler> {
        type Context = ();
        async fn handle(&self, _msg: TestMessage, _meta: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
        async fn handle_dead(&self, _msg: TestMessage, _meta: DeadMessageMetadata, _: &()) {
            self.called.store(true, Ordering::Relaxed);
        }
    }

    // -- tests --

    #[tokio::test]
    async fn audited_propagates_ack_outcome() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), OkAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata(), &()).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    #[tokio::test]
    async fn audited_propagates_reject_outcome() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Reject), OkAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata(), &()).await;
        assert!(matches!(outcome, Outcome::Reject));
    }

    #[tokio::test]
    async fn audited_propagates_retry_outcome() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Retry), OkAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata(), &()).await;
        assert!(matches!(outcome, Outcome::Retry));
    }

    #[tokio::test]
    async fn audited_propagates_defer_outcome() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Defer), OkAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata(), &()).await;
        assert!(matches!(outcome, Outcome::Defer));
    }

    #[tokio::test]
    async fn audited_returns_retry_when_audit_fails() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), FailingAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata(), &()).await;
        assert!(matches!(outcome, Outcome::Retry));
    }

    #[tokio::test]
    async fn audited_calls_audit_handler() {
        let tracker = TrackingAuditHandler::new();
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), tracker.clone());
        audited.handle(test_message(), test_metadata(), &()).await;
        assert_eq!(tracker.call_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn audited_uses_trace_id_from_header() {
        let tracker = TrackingAuditHandler::new();
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), tracker.clone());

        let mut meta = test_metadata();
        meta.headers
            .insert("x-trace-id".into(), "my-trace-123".into());

        audited.handle(test_message(), meta, &()).await;
        let captured: tokio::sync::MutexGuard<'_, Option<String>> = tracker.trace_id.lock().await;
        assert_eq!(captured.as_deref(), Some("my-trace-123"));
    }

    #[tokio::test]
    async fn audited_generates_trace_id_when_missing() {
        let tracker = TrackingAuditHandler::new();
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), tracker.clone());

        audited.handle(test_message(), test_metadata(), &()).await;

        let captured: tokio::sync::MutexGuard<'_, Option<String>> = tracker.trace_id.lock().await;
        let trace_id = captured.as_ref().expect("trace_id should be set");
        // Should be a valid UUID v4
        assert_eq!(trace_id.len(), 36);
        assert!(Uuid::parse_str(trace_id).is_ok());
    }

    #[cfg(feature = "audit")]
    #[tokio::test]
    async fn audited_skips_audit_for_audit_log_topic() {
        use super::shove_backend::AuditLog;

        struct AuditLogHandler;
        impl MessageHandler<AuditLog> for AuditLogHandler {
            type Context = ();
            async fn handle(
                &self,
                _msg: <AuditLog as Topic>::Message,
                _meta: MessageMetadata,
                _: &(),
            ) -> Outcome {
                Outcome::Ack
            }
        }

        struct PanicAuditHandler;
        impl AuditHandler<AuditLog> for PanicAuditHandler {
            async fn audit(
                &self,
                _record: &AuditRecord<<AuditLog as Topic>::Message>,
            ) -> Result<()> {
                panic!("audit handler should not be called for the audit topic");
            }
        }

        let audited = Audited::new(AuditLogHandler, PanicAuditHandler);
        let meta = MessageMetadata {
            retry_count: 0,
            delivery_id: "d-1".into(),
            redelivered: false,
            headers: HashMap::new(),
        };

        let msg = serde_json::from_value::<<AuditLog as Topic>::Message>(
            serde_json::json!({
                "trace_id": "t1",
                "topic": "test",
                "payload": null,
                "metadata": { "retry_count": 0, "delivery_id": "x", "redelivered": false, "headers": {} },
                "outcome": "Ack",
                "duration_ms": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }),
        )
        .unwrap();

        let outcome = audited.handle(msg, meta, &()).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    struct SlowAuditHandler;
    impl AuditHandler<TestTopic> for SlowAuditHandler {
        async fn audit(&self, _record: &AuditRecord<TestMessage>) -> Result<()> {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn audited_returns_original_outcome_on_audit_timeout() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), SlowAuditHandler)
            .with_audit_timeout(Duration::from_millis(10));
        let outcome = audited.handle(test_message(), test_metadata(), &()).await;
        // Should return the original Ack, not Retry, because audit timed out.
        assert!(matches!(outcome, Outcome::Ack));
    }

    #[tokio::test]
    async fn audited_no_timeout_blocks_on_slow_audit() {
        // Without a timeout, a failing audit handler returns Retry.
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), FailingAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata(), &()).await;
        assert!(matches!(outcome, Outcome::Retry));
    }

    #[tokio::test]
    async fn audited_handle_dead_does_not_panic() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), OkAuditHandler);
        audited
            .handle_dead(test_message(), test_dead_metadata(), &())
            .await;
    }

    #[tokio::test]
    async fn audited_handle_dead_delegates_to_inner_handler() {
        let tracker = TrackingDeadHandler::new();
        let audited = Audited::new(tracker.clone(), OkAuditHandler);
        audited
            .handle_dead(test_message(), test_dead_metadata(), &())
            .await;
        assert!(tracker.called.load(Ordering::Relaxed));
    }

    /// `Audited<H, A>::handle` forwards the caller-supplied `ctx` reference unchanged to the
    /// inner handler on the success path.
    #[tokio::test]
    async fn audited_handle_forwards_context() {
        struct CtxHandler;
        impl MessageHandler<TestTopic> for CtxHandler {
            type Context = u32;
            async fn handle(
                &self,
                _msg: TestMessage,
                _meta: MessageMetadata,
                ctx: &u32,
            ) -> Outcome {
                assert_eq!(*ctx, 11);
                Outcome::Ack
            }
        }

        let audited = Audited::new(CtxHandler, OkAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata(), &11).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    /// `Audited<H, A>::handle_dead` forwards the caller-supplied `ctx` reference unchanged to
    /// the inner handler.
    #[tokio::test]
    async fn audited_handle_dead_forwards_context() {
        struct CtxDeadHandler;
        impl MessageHandler<TestTopic> for CtxDeadHandler {
            type Context = u32;
            async fn handle(&self, _msg: TestMessage, _meta: MessageMetadata, _: &u32) -> Outcome {
                Outcome::Ack
            }
            async fn handle_dead(&self, _msg: TestMessage, _meta: DeadMessageMetadata, ctx: &u32) {
                assert_eq!(*ctx, 13);
            }
        }

        let audited = Audited::new(CtxDeadHandler, OkAuditHandler);
        audited
            .handle_dead(test_message(), test_dead_metadata(), &13)
            .await;
    }
}
