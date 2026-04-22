//! Public `Publisher<B>` wrapper. See DESIGN_V2.md §6.2.

use std::collections::HashMap;

use crate::backend::{Backend, PublisherImpl};
use crate::error::Result;
use crate::topic::Topic;

pub struct Publisher<B: Backend> {
    pub(crate) inner: B::PublisherImpl,
}

impl<B: Backend> Publisher<B> {
    pub(crate) fn new(inner: B::PublisherImpl) -> Self {
        Self { inner }
    }

    pub async fn publish<T: Topic>(&self, msg: &T::Message) -> Result<()> {
        self.inner.publish::<T>(msg).await
    }

    pub async fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.publish_with_headers::<T>(msg, headers).await
    }

    pub async fn publish_batch<T: Topic>(&self, msgs: &[T::Message]) -> Result<()> {
        self.inner.publish_batch::<T>(msgs).await
    }
}

impl<B: Backend> Clone for Publisher<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
