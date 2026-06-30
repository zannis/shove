//! Public `TopologyDeclarer<B>` + `Topics` tuple trait.

use crate::backend::{Backend, TopologyImpl};
use crate::error::Result;
#[cfg(feature = "kafka")]
use crate::markers::Kafka;
use crate::topic::Topic;

pub struct TopologyDeclarer<B: Backend> {
    pub(crate) inner: B::TopologyImpl,
}

impl<B: Backend> TopologyDeclarer<B> {
    pub(crate) fn new(inner: B::TopologyImpl) -> Self {
        Self { inner }
    }

    pub async fn declare<T: Topic>(&self) -> Result<()> {
        self.inner.declare::<T>().await
    }

    pub async fn declare_all<Ts: Topics>(&self) -> Result<()> {
        Ts::declare_all(self).await
    }
}

#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
impl TopologyDeclarer<Kafka> {
    /// Replication factor applied to every topic this declarer creates (main +
    /// DLQ). Defaults to `1` (single-broker dev) when unset; production
    /// clusters should set `>= 3` (or whatever the cluster's quorum demands).
    /// `declare` is idempotent and will not alter the replication of an
    /// already-existing topic.
    ///
    /// # Panics
    ///
    /// Panics if `n < 1`.
    pub fn with_replication_factor(mut self, n: i32) -> Self {
        self.inner = self.inner.with_replication_factor(n);
        self
    }

    /// Partition floor for the topics this declarer creates: each topic gets
    /// `max(topology_default, n)` partitions so Kafka can spread load across at
    /// least `n` consumers. `declare` only ever expands partition counts,
    /// never shrinks them.
    pub fn with_min_partitions(mut self, n: i32) -> Self {
        self.inner = self.inner.with_min_partitions(n);
        self
    }
}

/// Multi-topic declaration via tuples. Arities 1 through 16.
pub trait Topics: Sized {
    fn declare_all<B: Backend>(
        declarer: &TopologyDeclarer<B>,
    ) -> impl Future<Output = Result<()>> + Send;
}

macro_rules! impl_topics_for_tuple {
    ($( ($($T:ident),+) ),+ $(,)?) => {
        $(
            impl<$($T: Topic),+> Topics for ($($T,)+) {
                async fn declare_all<B: Backend>(d: &TopologyDeclarer<B>) -> Result<()> {
                    $( d.declare::<$T>().await?; )+
                    Ok(())
                }
            }
        )+
    };
}

impl_topics_for_tuple!(
    (T1),
    (T1, T2),
    (T1, T2, T3),
    (T1, T2, T3, T4),
    (T1, T2, T3, T4, T5),
    (T1, T2, T3, T4, T5, T6),
    (T1, T2, T3, T4, T5, T6, T7),
    (T1, T2, T3, T4, T5, T6, T7, T8),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14),
    (
        T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15
    ),
    (
        T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16
    ),
);
