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
        let (succeeded, res) = self.inner.publish_batch::<T>(msgs).await;
        let elapsed = start.elapsed().as_secs_f64();
        // The backend reports how many messages it actually accepted —
        // backends like SNS, Kafka, and RabbitMQ can partially succeed
        // before surfacing an `Err`, so counting `msgs.len()` against the
        // overall outcome would either overcount failures or undercount
        // successes. We split on the reported count.
        let total = msgs.len() as u64;
        let failed = total.saturating_sub(succeeded);
        if succeeded > 0 {
            metrics::record_published_n(topic, true, succeeded);
        }
        if failed > 0 {
            metrics::record_published_n(topic, false, failed);
        }
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
