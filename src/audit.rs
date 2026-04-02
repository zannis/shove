use std::future::Future;
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    ) -> impl Future<Output = Result<(), ShoveError>> + Send;
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
    async fn handle(
        &self,
        message: T::Message,
        metadata: MessageMetadata,
    ) -> Outcome {
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
    use crate::error::ShoveError;
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
        async fn audit(&self, record: &AuditRecord<T::Message>) -> Result<(), ShoveError> {
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
