//! Per-record outcome of a [`Publisher::publish_batch`] call.
//!
//! Backends report a batch as an internal `BatchReport` — a record of which
//! indices were rejected and which were never resolved. The public wrapper
//! normalises that into either `Ok(())`, a bare backend error, or
//! [`ShoveError::PartialBatch`] carrying a [`BatchFailure`].
//!
//! [`Publisher::publish_batch`]: crate::publisher::Publisher::publish_batch

use std::fmt;

use crate::error::{Result, ShoveError};

/// Which records in a [`Publisher::publish_batch`] call did not make it.
///
/// Delivered inside [`ShoveError::PartialBatch`], and **only** when the batch
/// was genuinely partial: at least one record confirmed and at least one not.
/// A batch that fails as a whole (serialization error, topology error, an
/// unreachable broker) still returns the same bare error it always has, so
/// existing error matching keeps working.
///
/// The indices are positions in the `msgs` slice that was passed in. The one
/// number to act on is [`to_republish`](Self::to_republish): re-publishing
/// exactly those records is always correct, on every backend.
///
/// ```
/// # fn example(err: shove::ShoveError, records: &[String]) -> Vec<String> {
/// match err {
///     shove::ShoveError::PartialBatch(f) => f
///         .to_republish()
///         .iter()
///         .filter_map(|&i| records.get(i).cloned())
///         .collect(),
///     _ => records.to_vec(),
/// }
/// # }
/// ```
///
/// Not `Clone`: it owns a [`ShoveError`], which is not `Clone` because
/// `serde_json::Error` is not.
///
/// [`Publisher::publish_batch`]: crate::publisher::Publisher::publish_batch
#[derive(Debug)]
pub struct BatchFailure {
    failed: Vec<usize>,
    unattempted: Vec<usize>,
    to_republish: Vec<usize>,
    succeeded: usize,
    source: ShoveError,
}

impl BatchFailure {
    /// Indices the backend attempted and explicitly reported as rejected.
    ///
    /// Sparse on Kafka, SNS, and NATS; a single index on the prefix backends
    /// (RabbitMQ, InMemory, and Redis when Redis returned an explicit error).
    /// See the table on
    /// [`Publisher::publish_batch`](crate::publisher::Publisher::publish_batch).
    pub fn failed(&self) -> &[usize] {
        &self.failed
    }

    /// Indices that were never submitted, or were submitted without a
    /// resolution the backend could confirm.
    ///
    /// Ambiguous records land here rather than in
    /// [`succeeded`](Self::succeeded) — re-publishing risks a duplicate, not
    /// re-publishing risks a loss, and this crate always chooses the duplicate.
    pub fn unattempted(&self) -> &[usize] {
        &self.unattempted
    }

    /// `failed ∪ unattempted`, ascending and deduplicated — exactly the
    /// records to re-publish, and never empty.
    ///
    /// The two sets are disjoint, so this is also their concatenation:
    /// `to_republish().len() == failed().len() + unattempted().len()`.
    pub fn to_republish(&self) -> &[usize] {
        &self.to_republish
    }

    /// How many records the backend confirmed. Always `>= 1`.
    ///
    /// Both halves of the batch invariant hold:
    ///
    /// ```text
    /// succeeded() + failed().len() + unattempted().len() == msgs.len()
    /// succeeded() + to_republish().len()                 == msgs.len()
    /// ```
    pub fn succeeded(&self) -> usize {
        self.succeeded
    }

    /// The representative (first) backend error behind the failure.
    ///
    /// Named `source` for familiarity, but this is an inherent method, not
    /// [`std::error::Error::source`] — it returns the concrete [`ShoveError`].
    pub fn source(&self) -> &ShoveError {
        &self.source
    }

    /// Total records in the original batch.
    fn total(&self) -> usize {
        self.succeeded.saturating_add(self.to_republish.len())
    }
}

impl fmt::Display for BatchFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} of {} records published, {} need re-publishing ({} failed, {} unattempted); \
             first error: {}",
            self.succeeded,
            self.total(),
            self.to_republish.len(),
            self.failed.len(),
            self.unattempted.len(),
            self.source,
        )
    }
}

/// What a backend reports back from `PublisherImpl::publish_batch`.
///
/// Internal: the public wrapper turns it into a `Result<()>` and the metrics
/// split. Backends describe *what happened per index*; deciding whether that
/// is a partial failure, and keeping the invariant honest, happens once in
/// [`BatchReport::resolve`] rather than six times.
#[derive(Debug, Default)]
pub(crate) struct BatchReport {
    failed: Vec<usize>,
    unattempted: Vec<usize>,
    first_err: Option<ShoveError>,
}

/// The normalised outcome of a batch: the metrics split plus the value the
/// caller sees.
pub(crate) struct BatchOutcome {
    pub(crate) succeeded: u64,
    pub(crate) failed: u64,
    pub(crate) result: Result<()>,
}

// Every constructor is used by at least one backend, but never by all of
// them at once, so a single-backend feature set leaves some of them without
// a caller in the lib build.
#[allow(dead_code)] // Callers gated behind backend features.
impl BatchReport {
    /// Every record confirmed.
    pub(crate) fn all_succeeded() -> Self {
        Self::default()
    }

    /// Nothing was submitted — a pre-flight failure (encoding, topology, ARN
    /// resolution) that rejected the batch before any record left the process.
    ///
    /// Resolves to the bare `err`, never `PartialBatch`, because no record was
    /// confirmed.
    pub(crate) fn wholly_unattempted(total: usize, err: ShoveError) -> Self {
        Self {
            failed: Vec::new(),
            unattempted: (0..total).collect(),
            first_err: Some(err),
        }
    }

    /// Prefix semantics: the backend confirmed `confirmed` records in order,
    /// then failed on the next one and stopped.
    ///
    /// Index `confirmed` is the failure; everything after it was never
    /// submitted.
    pub(crate) fn prefix(confirmed: usize, total: usize, err: ShoveError) -> Self {
        if confirmed >= total {
            // The backend reported an error with nothing left unresolved.
            // Surface the error rather than silently dropping it; `resolve`
            // turns an empty index set into the bare error.
            return Self {
                failed: Vec::new(),
                unattempted: Vec::new(),
                first_err: Some(err),
            };
        }
        Self {
            failed: vec![confirmed],
            unattempted: (confirmed.saturating_add(1)..total).collect(),
            first_err: Some(err),
        }
    }

    /// Arbitrary index sets, for backends that attempt every record
    /// independently (Kafka), name their rejects (SNS), or mix a sparse
    /// prefix with an unattempted tail (NATS).
    pub(crate) fn sparse(
        failed: Vec<usize>,
        unattempted: Vec<usize>,
        first_err: Option<ShoveError>,
    ) -> Self {
        Self {
            failed,
            unattempted,
            first_err,
        }
    }

    /// The representative error, if the batch did not fully succeed.
    pub(crate) fn first_err(&self) -> Option<&ShoveError> {
        self.first_err.as_ref()
    }

    /// Replace the representative error, keeping the index sets.
    ///
    /// RabbitMQ needs this: when re-creating the confirm channel between retry
    /// attempts fails, the channel error is what the caller should see, but the
    /// index sets from the last attempt are still what needs re-publishing.
    pub(crate) fn with_error(mut self, err: ShoveError, total: usize) -> Self {
        if self.failed.is_empty() && self.unattempted.is_empty() {
            self.unattempted = (0..total).collect();
        }
        self.first_err = Some(err);
        self
    }

    /// Normalise into the metrics split and the caller-visible result.
    ///
    /// This is the single place the batch invariant is enforced:
    /// `succeeded + to_republish.len() == total`, with `to_republish` sorted
    /// ascending and free of duplicates. Out-of-range indices from a
    /// misbehaving backend are dropped rather than trusted.
    pub(crate) fn resolve(self, total: usize) -> BatchOutcome {
        let Self {
            failed,
            unattempted,
            first_err,
        } = self;

        debug_assert!(
            failed.iter().chain(unattempted.iter()).all(|&i| i < total),
            "batch report index out of range for a batch of {total}: \
             failed={failed:?} unattempted={unattempted:?}"
        );

        let failed = normalize(failed, total);
        // `failed` wins any overlap: an index the backend explicitly rejected
        // is not also "never submitted". Keeping the two sets disjoint is what
        // makes `succeeded() + failed().len() + unattempted().len()` equal the
        // batch size, rather than only `succeeded() + to_republish().len()`.
        let mut unattempted = normalize(unattempted, total);
        unattempted.retain(|i| failed.binary_search(i).is_err());

        let unresolved = failed.len().saturating_add(unattempted.len());
        let succeeded = total.saturating_sub(unresolved);

        let Some(err) = first_err else {
            debug_assert_eq!(
                unresolved, 0,
                "a batch report with no error must have no unresolved records"
            );
            return BatchOutcome {
                succeeded: to_u64(succeeded),
                failed: to_u64(unresolved),
                result: Ok(()),
            };
        };

        // Compatibility rule: `PartialBatch` only when the batch really is
        // partial. A wholly-failed batch keeps returning the bare error it
        // always returned, so existing `matches!(e, ShoveError::Topology(_))`
        // style matching does not regress.
        let result = if succeeded > 0 && unresolved > 0 {
            // Both sides are sorted, deduplicated and disjoint, so the union
            // is just the concatenation re-sorted.
            let mut to_republish = Vec::with_capacity(unresolved);
            to_republish.extend_from_slice(&failed);
            to_republish.extend_from_slice(&unattempted);
            to_republish.sort_unstable();
            Err(ShoveError::PartialBatch(Box::new(BatchFailure {
                failed,
                unattempted,
                to_republish,
                succeeded,
                source: err,
            })))
        } else {
            Err(err)
        };

        BatchOutcome {
            succeeded: to_u64(succeeded),
            failed: to_u64(unresolved),
            result,
        }
    }
}

/// Sort, deduplicate, and clamp an index set to the batch.
///
/// The clamp is a release-mode backstop for the `debug_assert!` in
/// [`BatchReport::resolve`]: a backend that reported an index outside the batch
/// has a bug, and dropping the index keeps `succeeded` honest rather than
/// letting a bogus index inflate the failure count.
fn normalize(mut idx: Vec<usize>, total: usize) -> Vec<usize> {
    idx.retain(|&i| i < total);
    idx.sort_unstable();
    idx.dedup();
    idx
}

/// Lossless on every target this crate supports; saturates rather than
/// truncating on a hypothetical 128-bit `usize`.
fn to_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(msg: &str) -> ShoveError {
        ShoveError::Connection(msg.to_string())
    }

    fn partial(out: &BatchOutcome) -> &BatchFailure {
        match &out.result {
            Err(ShoveError::PartialBatch(f)) => f,
            other => panic!("expected PartialBatch, got {other:?}"),
        }
    }

    #[test]
    fn all_succeeded_resolves_to_ok() {
        let out = BatchReport::all_succeeded().resolve(5);
        assert!(out.result.is_ok());
        assert_eq!(out.succeeded, 5);
        assert_eq!(out.failed, 0);
    }

    #[test]
    fn empty_batch_resolves_to_ok() {
        let out = BatchReport::all_succeeded().resolve(0);
        assert!(out.result.is_ok());
        assert_eq!(out.succeeded, 0);
        assert_eq!(out.failed, 0);
    }

    #[test]
    fn prefix_splits_into_failure_then_tail() {
        let out = BatchReport::prefix(2, 5, conn("boom")).resolve(5);
        let f = partial(&out);
        assert_eq!(f.succeeded(), 2);
        assert_eq!(f.failed(), &[2]);
        assert_eq!(f.unattempted(), &[3, 4]);
        assert_eq!(f.to_republish(), &[2, 3, 4]);
        assert_eq!(out.succeeded, 2);
        assert_eq!(out.failed, 3);
    }

    #[test]
    fn wholly_unattempted_returns_the_bare_error() {
        let out =
            BatchReport::wholly_unattempted(4, ShoveError::Topology("nope".into())).resolve(4);
        assert!(matches!(out.result, Err(ShoveError::Topology(_))));
        assert_eq!(out.succeeded, 0);
        assert_eq!(out.failed, 4);
    }

    #[test]
    fn prefix_with_zero_confirmed_returns_the_bare_error() {
        // Nothing confirmed is not a *partial* batch, so the caller keeps
        // seeing exactly the error it saw before this type existed.
        let out = BatchReport::prefix(0, 3, conn("boom")).resolve(3);
        assert!(matches!(out.result, Err(ShoveError::Connection(_))));
        assert_eq!(out.succeeded, 0);
        assert_eq!(out.failed, 3);
    }

    #[test]
    fn error_with_no_unresolved_indices_returns_the_bare_error() {
        let out = BatchReport::prefix(3, 3, conn("boom")).resolve(3);
        assert!(matches!(out.result, Err(ShoveError::Connection(_))));
        assert_eq!(out.succeeded, 3);
        assert_eq!(out.failed, 0);
    }

    #[test]
    fn sparse_keeps_exact_indices() {
        let out = BatchReport::sparse(vec![1, 4], Vec::new(), Some(conn("boom"))).resolve(6);
        let f = partial(&out);
        assert_eq!(f.failed(), &[1, 4]);
        assert!(f.unattempted().is_empty());
        assert_eq!(f.to_republish(), &[1, 4]);
        assert_eq!(f.succeeded(), 4);
    }

    #[test]
    fn to_republish_is_sorted_deduped_and_merged() {
        let out = BatchReport::sparse(vec![4, 1], vec![2, 5], Some(conn("boom"))).resolve(7);
        let f = partial(&out);
        assert_eq!(f.failed(), &[1, 4]);
        assert_eq!(f.unattempted(), &[2, 5]);
        assert_eq!(f.to_republish(), &[1, 2, 4, 5]);
        assert_eq!(f.succeeded(), 3);
        // The invariant, stated exactly as the contract does.
        assert_eq!(f.succeeded() + f.failed().len() + f.unattempted().len(), 7);
        assert_eq!(f.succeeded() + f.to_republish().len(), 7);
    }

    /// An index cannot be both "explicitly rejected" and "never submitted".
    /// If a backend ever reported both, the two sets must stay disjoint —
    /// otherwise `succeeded + failed.len() + unattempted.len()` over-counts
    /// and the documented invariant becomes a lie.
    #[test]
    fn an_index_in_both_sets_counts_once_as_failed() {
        let out = BatchReport::sparse(vec![4, 1], vec![4, 2, 5], Some(conn("boom"))).resolve(7);
        let f = partial(&out);
        assert_eq!(f.failed(), &[1, 4]);
        assert_eq!(f.unattempted(), &[2, 5], "4 is already accounted for");
        assert_eq!(f.to_republish(), &[1, 2, 4, 5]);
        assert_eq!(f.succeeded() + f.failed().len() + f.unattempted().len(), 7);
    }

    /// A backend reporting an index outside the batch is a bug, and
    /// `resolve`'s `debug_assert!` catches it in every test build. This pins
    /// the release-mode backstop underneath that assert: the index is dropped
    /// rather than trusted, so it cannot inflate the failure count or make
    /// `succeeded` saturate to zero.
    #[test]
    fn normalize_drops_out_of_range_indices() {
        assert_eq!(normalize(vec![1, 99], 3), vec![1]);
        assert_eq!(normalize(vec![3, 4], 3), Vec::<usize>::new());
    }

    #[test]
    fn normalize_sorts_and_dedups() {
        assert_eq!(normalize(vec![4, 1, 4, 2], 7), vec![1, 2, 4]);
    }

    #[test]
    fn with_error_keeps_the_index_sets() {
        let out = BatchReport::prefix(1, 4, conn("first"))
            .with_error(conn("second"), 4)
            .resolve(4);
        let f = partial(&out);
        assert_eq!(f.to_republish(), &[1, 2, 3]);
        assert_eq!(f.source().to_string(), "connection error: second");
    }

    #[test]
    fn with_error_on_an_empty_report_marks_everything_unattempted() {
        let out = BatchReport::all_succeeded()
            .with_error(conn("channel died"), 3)
            .resolve(3);
        assert!(matches!(out.result, Err(ShoveError::Connection(_))));
        assert_eq!(out.succeeded, 0);
        assert_eq!(out.failed, 3);
    }

    #[test]
    fn display_names_the_split_and_the_source() {
        let out = BatchReport::sparse(vec![1], vec![2], Some(conn("boom"))).resolve(3);
        let msg = out.result.unwrap_err().to_string();
        assert_eq!(
            msg,
            "batch publish: 1 of 3 records published, 2 need re-publishing \
             (1 failed, 1 unattempted); first error: connection error: boom"
        );
    }
}
