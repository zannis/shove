//! Internal `AutoscalerBackendImpl` and `QueueStatsProviderImpl` traits.
//! See DESIGN_V2.md §5, §9.1.

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::autoscaler::AutoscalerBackend;
use crate::error::Result;

/// Internal sealing trait for [`crate::backend::Backend::AutoscalerImpl`].
///
/// Requiring [`AutoscalerBackend`] as a supertrait ensures every concrete
/// `AutoscalerImpl` exposes the full public autoscaling interface
/// (`list_groups` / `fetch_metrics` / `scale`) through generic
/// `Broker<B>`-based code, while the `pub(crate)` visibility of this trait
/// preserves the sealed-`Backend` invariant.
pub(crate) trait AutoscalerBackendImpl: AutoscalerBackend + Send + Sync {}

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
