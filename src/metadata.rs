/// Metadata about a consumed message, extracted from broker headers/properties.
#[derive(Debug, Clone)]
pub struct MessageMetadata {
    /// How many times this message has been retried (0 on first delivery).
    pub retry_count: u32,
    /// Opaque delivery identifier (AMQP tag, SQS receipt handle, etc).
    pub delivery_id: String,
    /// Whether the broker flagged this as a redelivery.
    pub redelivered: bool,
}

/// Metadata about a dead-lettered message.
#[derive(Debug, Clone)]
pub struct DeadMessageMetadata {
    /// Base message metadata.
    pub message: MessageMetadata,
    /// Why the message was dead-lettered (e.g., "rejected", "expired").
    pub reason: Option<String>,
    /// The original queue the message was in before being dead-lettered.
    pub original_queue: Option<String>,
    /// How many times this message has been dead-lettered.
    pub death_count: u32,
}
