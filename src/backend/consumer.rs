//! Internal `ConsumerImpl` trait. Backend-specific consumer structs
//! implement this; public harnesses (`ConsumerSupervisor<B>`,
//! `ConsumerGroup<B>`) delegate here. See DESIGN_V2.md §5.
//!
//! The trait bound is `H: MessageHandler<T>` — the `ctx: H::Context`
//! parameter is threaded to every spawned consumer task so handlers with
//! a non-unit [`MessageHandler::Context`](MessageHandler::Context)
//! work through the generic harness.

use std::future::Future;

use crate::backend::ConsumerOptionsInner;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

// Methods are anchored by the InMemory port's `_anchor_*` helpers in
// `backend::mod` under the `inmemory` feature. Under
// `--no-default-features` no backend is compiled, so the trait methods
// genuinely have no call site; `dead_code` is expected there and the
// per-trait allow avoids polluting the default build with warnings.
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
        H: MessageHandler<T>;

    fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: SequencedTopic,
        H: MessageHandler<T>;

    fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>;

    fn spawn_fifo_shards<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<Vec<tokio::task::JoinHandle<Result<()>>>>> + Send
    where
        T: SequencedTopic,
        H: MessageHandler<T>;
}
