use std::future::Future;
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::error::ShoveError;
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::outcome::Outcome;
use crate::topic::Topic;

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
/// Returning `Err` causes the message to be retried (strict auditing).
pub trait AuditHandler<T: Topic>: Send + Sync + 'static {
    fn audit(
        &self,
        record: &AuditRecord<T::Message>,
    ) -> impl Future<Output = crate::error::Result<()>> + Send;
}

/// Wraps a `MessageHandler` with audit logging.
///
/// `Audited<H, A>` implements `MessageHandler<T>`, so it is fully transparent
/// to consumers and consumer groups — no changes required at the call site.
pub struct Audited<H, A> {
    handler: H,
    audit_handler: A,
}

impl<H, A> Audited<H, A> {
    pub fn new(handler: H, audit_handler: A) -> Self {
        Self {
            handler,
            audit_handler,
        }
    }
}

impl<T, H, A> MessageHandler<T> for Audited<H, A>
where
    T: Topic,
    T::Message: Clone,
    H: MessageHandler<T>,
    A: AuditHandler<T>,
{
    async fn handle(&self, message: T::Message, metadata: MessageMetadata) -> Outcome {
        // Guard against infinite recursion: skip auditing on the audit-log topic itself.
        #[cfg(feature = "audit")]
        {
            if T::topology().queue() == shove_backend::AuditLog::topology().queue() {
                return self.handler.handle(message, metadata).await;
            }
        }

        let trace_id = metadata
            .headers
            .get("x-trace-id")
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let topic = T::topology().queue().to_owned();
        let payload_clone = message.clone();
        let metadata_clone = metadata.clone();

        let start = Instant::now();
        let outcome = self.handler.handle(message, metadata).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        let record = AuditRecord {
            trace_id,
            topic,
            payload: payload_clone,
            metadata: metadata_clone,
            outcome: outcome.clone(),
            duration_ms,
            timestamp: chrono::Utc::now(),
        };

        match self.audit_handler.audit(&record).await {
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
    ) -> impl Future<Output = ()> + Send {
        self.handler.handle_dead(message, metadata)
    }
}

// --- ShoveAuditHandler: dogfood backend, gated behind `audit` feature ---

#[cfg(feature = "audit")]
mod shove_backend {
    use serde::Serialize;
    use serde_json::Value;

    use crate::audit::{AuditHandler, AuditRecord};
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
    /// `shove-audit-log` topic using any `Publisher` implementation.
    pub struct ShoveAuditHandler<P: Publisher> {
        publisher: P,
    }

    impl<P: Publisher> ShoveAuditHandler<P> {
        pub fn new(publisher: P) -> Self {
            Self { publisher }
        }
    }

    impl<T, P> AuditHandler<T> for ShoveAuditHandler<P>
    where
        T: Topic,
        T::Message: Serialize,
        P: Publisher,
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
pub use shove_backend::{AuditLog, ShoveAuditHandler};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::topology::TopologyBuilder;

    // -- test Topic --

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct TestMessage {
        body: String,
    }

    struct TestTopic;
    impl Topic for TestTopic {
        type Message = TestMessage;
        fn topology() -> &'static crate::topology::QueueTopology {
            static TOPOLOGY: std::sync::OnceLock<crate::topology::QueueTopology> =
                std::sync::OnceLock::new();
            TOPOLOGY.get_or_init(|| TopologyBuilder::new("audit-test").dlq().build())
        }
    }

    // -- mock handlers --

    struct FixedOutcomeHandler(Outcome);
    impl MessageHandler<TestTopic> for FixedOutcomeHandler {
        async fn handle(&self, _msg: TestMessage, _meta: MessageMetadata) -> Outcome {
            self.0.clone()
        }
    }

    struct OkAuditHandler;
    impl AuditHandler<TestTopic> for OkAuditHandler {
        async fn audit(&self, _record: &AuditRecord<TestMessage>) -> Result<(), ShoveError> {
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
        async fn audit(&self, record: &AuditRecord<TestMessage>) -> Result<(), ShoveError> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            *self.trace_id.lock().await = Some(record.trace_id.clone());
            Ok(())
        }
    }

    struct FailingAuditHandler;
    impl AuditHandler<TestTopic> for FailingAuditHandler {
        async fn audit(&self, _record: &AuditRecord<TestMessage>) -> Result<(), ShoveError> {
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

    // -- tests --

    #[tokio::test]
    async fn audited_propagates_ack_outcome() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), OkAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata()).await;
        assert!(matches!(outcome, Outcome::Ack));
    }

    #[tokio::test]
    async fn audited_propagates_reject_outcome() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Reject), OkAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata()).await;
        assert!(matches!(outcome, Outcome::Reject));
    }

    #[tokio::test]
    async fn audited_propagates_retry_outcome() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Retry), OkAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata()).await;
        assert!(matches!(outcome, Outcome::Retry));
    }

    #[tokio::test]
    async fn audited_propagates_defer_outcome() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Defer), OkAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata()).await;
        assert!(matches!(outcome, Outcome::Defer));
    }

    #[tokio::test]
    async fn audited_returns_retry_when_audit_fails() {
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), FailingAuditHandler);
        let outcome = audited.handle(test_message(), test_metadata()).await;
        assert!(matches!(outcome, Outcome::Retry));
    }

    #[tokio::test]
    async fn audited_calls_audit_handler() {
        let tracker = TrackingAuditHandler::new();
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), tracker.clone());
        audited.handle(test_message(), test_metadata()).await;
        assert_eq!(tracker.call_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn audited_uses_trace_id_from_header() {
        let tracker = TrackingAuditHandler::new();
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), tracker.clone());

        let mut meta = test_metadata();
        meta.headers
            .insert("x-trace-id".into(), "my-trace-123".into());

        audited.handle(test_message(), meta).await;
        let captured: tokio::sync::MutexGuard<'_, Option<String>> = tracker.trace_id.lock().await;
        assert_eq!(captured.as_deref(), Some("my-trace-123"));
    }

    #[tokio::test]
    async fn audited_generates_trace_id_when_missing() {
        let tracker = TrackingAuditHandler::new();
        let audited = Audited::new(FixedOutcomeHandler(Outcome::Ack), tracker.clone());

        audited.handle(test_message(), test_metadata()).await;

        let captured: tokio::sync::MutexGuard<'_, Option<String>> = tracker.trace_id.lock().await;
        let trace_id = captured.as_ref().expect("trace_id should be set");
        // Should be a valid UUID v4
        assert_eq!(trace_id.len(), 36);
        assert!(uuid::Uuid::parse_str(trace_id).is_ok());
    }

    #[cfg(feature = "audit")]
    #[tokio::test]
    async fn audited_skips_audit_for_audit_log_topic() {
        use super::shove_backend::AuditLog;

        struct AuditLogHandler;
        impl MessageHandler<AuditLog> for AuditLogHandler {
            async fn handle(
                &self,
                _msg: <AuditLog as Topic>::Message,
                _meta: MessageMetadata,
            ) -> Outcome {
                Outcome::Ack
            }
        }

        struct PanicAuditHandler;
        impl AuditHandler<AuditLog> for PanicAuditHandler {
            async fn audit(
                &self,
                _record: &AuditRecord<<AuditLog as Topic>::Message>,
            ) -> Result<(), ShoveError> {
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

        let outcome = audited.handle(msg, meta).await;
        assert!(matches!(outcome, Outcome::Ack));
    }
}
