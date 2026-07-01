//! Public `TopologyDeclarer<B>` + `Topics` tuple trait.

use crate::backend::{Backend, TopologyImpl};
use crate::error::Result;
#[cfg(feature = "kafka")]
use crate::markers::Kafka;
use crate::topic::Topic;
#[cfg(feature = "kafka")]
use crate::topology::KafkaCleanupPolicy;
#[cfg(feature = "kafka")]
use std::time::Duration;

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

    /// Kafka topic-level config entry (e.g. `retention.ms`) applied to every
    /// **main** topic this declarer creates or reconciles. Repeatable; later
    /// calls for the same key win. Per-topic entries set via
    /// [`TopologyBuilder::with_topic_config`](crate::topology::TopologyBuilder::with_topic_config)
    /// override these key-by-key. DLQ topics keep cluster defaults.
    ///
    /// On an already-existing topic, `declare` compares the declared keys
    /// against the live values and issues an alter when they drift, preserving
    /// the topic's other dynamic config entries.
    ///
    /// ```ignore
    /// broker.topology()
    ///     .with_topic_config("retention.ms", "604800000")
    ///     .declare::<IngestionTopic>()
    ///     .await?;
    /// ```
    pub fn with_topic_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.with_topic_config(key, value);
        self
    }

    /// Sets `retention.ms` from a [`Duration`]. Sugar for
    /// [`with_topic_config`](Self::with_topic_config); the same scope and
    /// reconcile semantics apply. Sub-millisecond precision is truncated.
    pub fn with_retention(self, retention: Duration) -> Self {
        self.with_topic_config("retention.ms", retention.as_millis().to_string())
    }

    /// Sets `retention.ms = -1`, retaining messages forever (typically paired
    /// with [`with_cleanup_policy`](Self::with_cleanup_policy) and
    /// [`KafkaCleanupPolicy::Compact`]). Sugar for
    /// [`with_topic_config`](Self::with_topic_config).
    pub fn with_retention_forever(self) -> Self {
        self.with_topic_config("retention.ms", "-1")
    }

    /// Sets `retention.bytes`, the maximum partition size before old segments
    /// are discarded. Sugar for
    /// [`with_topic_config`](Self::with_topic_config).
    pub fn with_retention_bytes(self, bytes: u64) -> Self {
        self.with_topic_config("retention.bytes", bytes.to_string())
    }

    /// Sets `cleanup.policy`. Sugar for
    /// [`with_topic_config`](Self::with_topic_config).
    pub fn with_cleanup_policy(self, policy: KafkaCleanupPolicy) -> Self {
        self.with_topic_config("cleanup.policy", policy.as_str())
    }

    /// Sets `max.message.bytes`, the largest record batch the topic accepts.
    /// Sugar for [`with_topic_config`](Self::with_topic_config).
    pub fn with_max_message_bytes(self, bytes: u32) -> Self {
        self.with_topic_config("max.message.bytes", bytes.to_string())
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
