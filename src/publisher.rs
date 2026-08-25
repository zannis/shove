//! Public `Publisher<B>` wrapper.

use std::collections::HashMap;
use std::time::Instant;

use crate::backend::{Backend, PublisherImpl};
#[cfg(doc)]
use crate::batch::BatchFailure;
use crate::error::Result;
use crate::metrics;
use crate::topic::Topic;

pub struct Publisher<B: Backend> {
    pub(crate) inner: B::PublisherImpl,
}

impl<B: Backend> Publisher<B> {
    pub(crate) fn new(inner: B::PublisherImpl) -> Self {
        Self { inner }
    }

    pub async fn publish<T: Topic>(&self, msg: &T::Message) -> Result<()> {
        let topic = T::topology().queue();
        let start = Instant::now();
        let res = self.inner.publish::<T>(msg).await;
        let elapsed = start.elapsed().as_secs_f64();
        metrics::record_published(topic, res.is_ok());
        metrics::record_publish_duration(topic, res.is_ok(), elapsed);
        res
    }

    pub async fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        let topic = T::topology().queue();
        let start = Instant::now();
        let res = self.inner.publish_with_headers::<T>(msg, headers).await;
        let elapsed = start.elapsed().as_secs_f64();
        metrics::record_published(topic, res.is_ok());
        metrics::record_publish_duration(topic, res.is_ok(), elapsed);
        res
    }

    /// Publish `msgs` as one batch.
    ///
    /// Returns `Ok(())` when every record was confirmed. On failure the error
    /// tells you how much of the batch survived:
    ///
    /// - [`ShoveError::PartialBatch`] — some records were confirmed and some
    ///   were not. It carries [`BatchFailure::to_republish`]: the indices into
    ///   `msgs` that still need publishing. Re-publish only those instead of
    ///   re-producing the whole batch.
    /// - any other error — the batch failed as a whole (encoding, topology,
    ///   an unreachable broker, an empty or wholly-rejected submission), and
    ///   the error is exactly the one this call has always returned. Existing
    ///   `match` arms keep working.
    ///
    /// # Index fidelity per backend
    ///
    /// Which indices you get back depends on how the backend fails. Both
    /// shapes are safe to feed straight into a re-publish; the difference is
    /// only how *tight* the set is.
    ///
    /// | Backend | Shape | What that means |
    /// |---|---|---|
    /// | Kafka | **sparse** | every record is attempted independently, so `failed()` names exactly the rejected ones and `unattempted()` is empty |
    /// | SNS | **sparse** | rejected entries are named per 10-entry chunk; a chunk that errors as a whole, and every later chunk, becomes `unattempted()` |
    /// | NATS | **sparse + tail** | ack failures are exact over the submitted prefix; if submission breaks partway, the rest of the batch is `unattempted()` |
    /// | RabbitMQ | **prefix** | confirms are awaited in order and the call stops at the first nack: `failed()` is that one index, `unattempted()` is everything after it |
    /// | Redis | **prefix or unresolved tail** | sequential; a server rejection names the current index, while a lost reply leaves the current index and tail `unattempted()` |
    /// | InMemory | **prefix** | sequential; stops at the first error |
    ///
    /// On the prefix backends a record after the failure point may in fact
    /// have been fine — it was simply never tried. On the sparse backends a
    /// record *after* a failure has usually already succeeded, which is
    /// exactly why "re-publish from the first failure" is the wrong move and
    /// [`to_republish`](BatchFailure::to_republish) is the right one.
    ///
    /// Records whose fate is ambiguous — submitted but never confirmed — are
    /// always reported as `unattempted`, never as succeeded. Duplicates over
    /// loss.
    ///
    /// # Example
    ///
    /// ```
    /// use shove::{Backend, Publisher, ShoveError, Topic};
    ///
    /// async fn publish_all<B: Backend, T: Topic>(
    ///     publisher: &Publisher<B>,
    ///     records: &[T::Message],
    /// ) -> Result<(), ShoveError>
    /// where
    ///     T::Message: Clone,
    /// {
    ///     match publisher.publish_batch::<T>(records).await {
    ///         Ok(()) => Ok(()),
    ///         Err(ShoveError::PartialBatch(f)) => {
    ///             // Only the outstanding records go back on the wire.
    ///             let retry: Vec<T::Message> = f
    ///                 .to_republish()
    ///                 .iter()
    ///                 .filter_map(|&i| records.get(i).cloned())
    ///                 .collect();
    ///             publisher.publish_batch::<T>(&retry).await
    ///         }
    ///         Err(e) => Err(e),
    ///     }
    /// }
    /// ```
    ///
    /// [`ShoveError::PartialBatch`]: crate::error::ShoveError::PartialBatch
    pub async fn publish_batch<T: Topic>(&self, msgs: &[T::Message]) -> Result<()> {
        let topic = T::topology().queue();
        let start = Instant::now();
        let report = self.inner.publish_batch::<T>(msgs).await;
        let elapsed = start.elapsed().as_secs_f64();
        // The backend reports per-index what happened — backends like SNS,
        // Kafka, and RabbitMQ can partially succeed before surfacing an
        // `Err`, so counting `msgs.len()` against the overall outcome would
        // either overcount failures or undercount successes. `resolve` turns
        // those indices into the success/failure split, and is the single
        // place the `succeeded + to_republish == total` invariant is enforced.
        let outcome = report.resolve(msgs.len());
        if outcome.succeeded > 0 {
            metrics::record_published_n(topic, true, outcome.succeeded);
        }
        if outcome.failed > 0 {
            metrics::record_published_n(topic, false, outcome.failed);
        }
        // Duration is one sample for the whole batch — that's the user-observable
        // call latency, regardless of how many messages were inside.
        if !msgs.is_empty() {
            metrics::record_publish_duration(topic, outcome.result.is_ok(), elapsed);
        }
        outcome.result
    }
}

impl<B: Backend> Clone for Publisher<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
