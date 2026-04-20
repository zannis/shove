//! Internal `PublisherImpl` trait. Backend-specific publisher structs
//! implement this; users call the public `Publisher<B>` wrapper that
//! delegates here. See DESIGN_V2.md §5.

// Skeleton: implementors and call-sites land in subsequent phases.
#![allow(dead_code)]

use std::collections::HashMap;

use crate::error::Result;
use crate::topic::Topic;

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
