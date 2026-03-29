use std::collections::HashMap;

use crate::error::ShoveError;
use crate::topic::Topic;

/// Publish messages to their topic's main queue.
///
/// The destination is derived from `T::topology().queue()` — callers never
/// specify queue names directly. For sequenced topics, the publisher
/// automatically routes via `T::SEQUENCE_KEY_FN` when `topology.sequencing()`
/// is configured.
///
/// This trait is intentionally **not object-safe** — methods are generic
/// over `T: Topic`. Backends are always concrete types.
pub trait Publisher: Send + Sync + Clone + 'static {
    /// Publish a single message to the topic's queue.
    fn publish<T: Topic>(
        &self,
        message: &T::Message,
    ) -> impl Future<Output = Result<(), ShoveError>> + Send;

    /// Publish a single message with additional string headers/attributes.
    fn publish_with_headers<T: Topic>(
        &self,
        message: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<(), ShoveError>> + Send;

    /// Publish a batch of messages to the topic's queue.
    ///
    /// Implementations should send all messages before awaiting confirmations.
    fn publish_batch<T: Topic>(
        &self,
        messages: &[T::Message],
    ) -> impl Future<Output = Result<(), ShoveError>> + Send;
}
