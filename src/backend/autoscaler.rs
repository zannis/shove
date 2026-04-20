//! Internal `AutoscalerBackendImpl` and `QueueStatsProviderImpl` traits.
//! See DESIGN_V2.md §5, §9.1.

// Skeleton: implementors and call-sites land in subsequent phases.
#![allow(dead_code)]

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::error::Result;

pub(crate) trait AutoscalerBackendImpl: Send + Sync {
    // Kept as an empty trait for this phase; per-backend autoscaler bodies
    // bind here when backends port in Phase 4/7-10.
}

pub(crate) trait QueueStatsProviderImpl: Send + Sync {
    fn snapshot(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<AutoscaleMetrics>> + Send;
}
