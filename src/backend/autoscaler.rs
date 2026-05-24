//! Internal `QueueStatsProviderImpl` trait.
//!
//! `Backend::AutoscalerImpl` now binds directly to the public
//! [`crate::autoscaler::AutoscalerBackend`] trait — that bound makes
//! `list_groups` / `fetch_metrics` / `scale` a hard requirement of every
//! backend, replacing the empty sealed marker that used to allow stub
//! impls to satisfy the trait. See `DESIGN_V2.md` §5, §9.1.

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::error::Result;

// Method anchored by the InMemory port's `_anchor_*` helpers in
// `backend::mod` under the `inmemory` feature. Under
// `--no-default-features` no backend is compiled, so the trait method
// genuinely has no call site; `dead_code` is expected there and the
// per-trait allow avoids polluting the default build with warnings
// until Phase 5+ adds the generic wrappers.
#[allow(dead_code)]
pub(crate) trait QueueStatsProviderImpl: Send + Sync {
    fn snapshot(&self, queue: &str) -> impl Future<Output = Result<AutoscaleMetrics>> + Send;
}
