//! Internal `AutoscalerBackendImpl` and `QueueStatsProviderImpl` traits.
//! See DESIGN_V2.md §5, §9.1.

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::error::Result;

pub(crate) trait AutoscalerBackendImpl: Send + Sync {
    // Kept as an empty trait for this phase; per-backend autoscaler bodies
    // bind here when backends port in Phase 4/7-10.
}

// Method anchored by the InMemory port's `_anchor_*` helpers in
// `backend::mod` under the `inmemory` feature. Under
// `--no-default-features` no backend is compiled, so the trait method
// genuinely has no call site; `dead_code` is expected there and the
// per-trait allow avoids polluting the default build with warnings
// until Phase 5+ adds the generic wrappers.
#[allow(dead_code)]
pub(crate) trait QueueStatsProviderImpl: Send + Sync {
    fn snapshot(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<AutoscaleMetrics>> + Send;
}
