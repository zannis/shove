//! Internal `TopologyImpl` trait. See DESIGN_V2.md §5.

// Skeleton: implementors and call-sites land in subsequent phases.
#![allow(dead_code)]

use crate::error::Result;
use crate::topic::Topic;

pub(crate) trait TopologyImpl: Send + Sync {
    fn declare<T: Topic>(&self) -> impl Future<Output = Result<()>> + Send;
}
