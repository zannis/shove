//! Internal `PublisherImpl` trait. Backend-specific publisher structs
//! implement this; users call the public `Publisher<B>` wrapper that
//! delegates here. See DESIGN_V2.md §5.

use std::collections::HashMap;

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
    fn publish<T: Topic>(&self, msg: &T::Message)
    -> impl Future<Output = Result<()>> + Send;

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send;

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl Future<Output = Result<()>> + Send;
}
