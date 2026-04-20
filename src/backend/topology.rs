//! Internal `TopologyImpl` trait. See DESIGN_V2.md §5.

use crate::error::Result;
use crate::topic::Topic;

// Methods are anchored by the InMemory port's `_anchor_*` helpers in
// `backend::mod` under the `inmemory` feature. Under
// `--no-default-features` no backend is compiled, so the trait method
// genuinely has no call site; `dead_code` is expected there and the
// per-trait allow avoids polluting the default build with warnings
// until Phase 5+ adds the generic wrappers.
#[allow(dead_code)]
pub(crate) trait TopologyImpl: Send + Sync {
    fn declare<T: Topic>(&self) -> impl Future<Output = Result<()>> + Send;
}
