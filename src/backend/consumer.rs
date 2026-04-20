//! Internal `ConsumerImpl` trait. Backend-specific consumer structs
//! implement this; public harnesses (`ConsumerSupervisor<B>`,
//! `ConsumerGroup<B>`) delegate here. See DESIGN_V2.md §5.
//!
//! # Phase 4 bound
//!
//! The trait keeps the `H: MessageHandler<T, Context = ()>` narrowing that
//! lives on the existing `Consumer`/`MessageHandler` path across the crate.
//! The `ctx: H::Context` parameter is therefore `()` today — its presence
//! preserves the eventual signature without waiting for Phase 11/12 to
//! widen every concrete consumer impl. When those phases land and every
//! backend's inherent methods accept `H::Context`, the bound here is
//! relaxed to `H: MessageHandler<T>` without any further signature
//! churn at this trait boundary.

use crate::backend::ConsumerOptionsInner;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

// Methods are anchored by the InMemory port's `_anchor_*` helpers in
// `backend::mod` under the `inmemory` feature. Under
// `--no-default-features` no backend is compiled, so the trait methods
// genuinely have no call site; `dead_code` is expected there and the
// per-trait allow avoids polluting the default build with warnings
// until Phase 5+ adds the generic wrappers.
#[allow(dead_code)]
pub(crate) trait ConsumerImpl: Send + Sync {
    fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>;

    fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: SequencedTopic,
        H: MessageHandler<T, Context = ()>;

    fn run_dlq<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T, Context = ()>;
}
