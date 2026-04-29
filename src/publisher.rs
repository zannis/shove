//! Public `Publisher<B>` wrapper. See DESIGN_V2.md §6.2.

use std::collections::HashMap;
use std::time::Instant;

use crate::backend::{Backend, PublisherImpl};
use crate::error::Result;
use crate::metrics;
use crate::topic::Topic;

pub struct Publisher<B: Backend> {
    pub(crate) inner: B::PublisherImpl,
}

impl<B: Backend> Publisher<B> {
    pub(crate) fn new(inner: B::PublisherImpl) -> Self {
        Self { inner }
    }

    pub async fn publish<T: Topic>(&self, msg: &T::Message) -> Result<()> {
        let topic = T::topology().queue();
        let start = Instant::now();
        let res = self.inner.publish::<T>(msg).await;
        let elapsed = start.elapsed().as_secs_f64();
        metrics::record_published(topic, res.is_ok());
        metrics::record_publish_duration(topic, res.is_ok(), elapsed);
        res
    }

    pub async fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        let topic = T::topology().queue();
        let start = Instant::now();
        let res = self.inner.publish_with_headers::<T>(msg, headers).await;
        let elapsed = start.elapsed().as_secs_f64();
        metrics::record_published(topic, res.is_ok());
        metrics::record_publish_duration(topic, res.is_ok(), elapsed);
        res
    }

    pub async fn publish_batch<T: Topic>(&self, msgs: &[T::Message]) -> Result<()> {
        let topic = T::topology().queue();
        let start = Instant::now();
        let res = self.inner.publish_batch::<T>(msgs).await;
        let elapsed = start.elapsed().as_secs_f64();
        // One counter event per message so totals line up with what consumers
        // increment downstream; empty batches emit nothing (the call was a no-op).
        metrics::record_published_n(topic, res.is_ok(), msgs.len() as u64);
        // Duration is one sample for the whole batch — that's the user-observable
        // call latency, regardless of how many messages were inside.
        if !msgs.is_empty() {
            metrics::record_publish_duration(topic, res.is_ok(), elapsed);
        }
        res
    }
}

impl<B: Backend> Clone for Publisher<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
