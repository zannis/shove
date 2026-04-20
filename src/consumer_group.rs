//! Public `ConsumerGroup<B, Ctx>` -- specialist harness for coordinated
//! consumer groups. Gated on `B: HasCoordinatedGroups`. See DESIGN_V2.md §6.3.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::backend::RegistryImpl;
use crate::backend::capability::HasCoordinatedGroups;
use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::Topic;

pub struct ConsumerGroup<B: HasCoordinatedGroups, Ctx: Clone + Send + Sync + 'static = ()> {
    pub(crate) inner: B::RegistryImpl,
    // Phase 12 injects `ctx` into handler calls once `RegistryImpl::register`
    // relaxes its `Context = ()` bound. Until then the field is stored but
    // unused.
    #[allow(dead_code)]
    ctx: Ctx,
}

pub struct ConsumerGroupConfig<B: HasCoordinatedGroups> {
    pub(crate) inner: B::ConsumerGroupConfig,
}

impl<B: HasCoordinatedGroups> ConsumerGroupConfig<B> {
    pub fn new(inner: B::ConsumerGroupConfig) -> Self {
        Self { inner }
    }
}

impl<B: HasCoordinatedGroups> ConsumerGroup<B, ()> {
    pub(crate) fn new(inner: B::RegistryImpl) -> Self {
        Self { inner, ctx: () }
    }

    pub fn with_context<Ctx: Clone + Send + Sync + 'static>(
        self,
        ctx: Ctx,
    ) -> ConsumerGroup<B, Ctx> {
        ConsumerGroup {
            inner: self.inner,
            ctx,
        }
    }
}

impl<B: HasCoordinatedGroups, Ctx: Clone + Send + Sync + 'static> ConsumerGroup<B, Ctx> {
    pub async fn register<T, H>(
        &mut self,
        config: ConsumerGroupConfig<B>,
        factory: impl Fn() -> H + Send + Sync + 'static,
    ) -> Result<()>
    where
        T: Topic,
        // TODO(phase-12): relax to Context = Ctx once all backend RegistryImpl
        // impls accept non-() context.
        H: MessageHandler<T, Context = ()>,
    {
        self.inner.register::<T, H>(config.inner, factory, ()).await
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation_token()
    }

    pub async fn run_until_timeout<S>(self, signal: S, drain_timeout: Duration) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        self.inner.run_until_timeout(signal, drain_timeout).await
    }
}
