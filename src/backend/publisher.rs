//! Internal `PublisherImpl` trait. Backend-specific publisher structs
//! implement this; users call the public `Publisher<B>` wrapper that
//! delegates here.

use std::collections::HashMap;

use crate::batch::BatchReport;
use crate::error::Result;
use crate::topic::Topic;

// Methods are anchored by the InMemory port's `_anchor_*` helpers in
// `backend::mod` under the `inmemory` feature. Under
// `--no-default-features` no backend is compiled, so the trait methods
// genuinely have no call site; `dead_code` is expected there and the
// per-trait allow avoids polluting the default build with warnings
// until Phase 5+ adds the generic wrappers.
#[allow(dead_code)]
pub(crate) trait PublisherImpl: Send + Sync {
    fn publish<T: Topic>(&self, msg: &T::Message) -> impl Future<Output = Result<()>> + Send;

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Publish a batch, reporting per-index what happened.
    ///
    /// A backend describes only what it observed — which indices it attempted
    /// and had rejected ([`BatchReport::sparse`] / [`BatchReport::prefix`]),
    /// and which it never resolved. It does **not** decide whether that counts
    /// as a partial failure: [`BatchReport::resolve`] does that once, in
    /// `Publisher::publish_batch`, so the `messages_published_total` split and
    /// the [`ShoveError::PartialBatch`] contract stay identical across all six
    /// backends.
    ///
    /// The one rule a backend must honour: a record whose fate is **unknown**
    /// (submitted but unconfirmed) is `unattempted`, never confirmed.
    /// Duplicates over loss.
    ///
    /// [`ShoveError::PartialBatch`]: crate::error::ShoveError::PartialBatch
    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl Future<Output = BatchReport> + Send;
}
