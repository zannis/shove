//! Internal `ConsumerImpl` trait. Backend-specific consumer structs
//! implement this; public harnesses (`ConsumerSupervisor<B>`,
//! `ConsumerGroup<B>`) delegate here. See DESIGN_V2.md §5.

// Skeleton: implementors and call-sites land in subsequent phases.
#![allow(dead_code)]

use crate::backend::ConsumerOptionsInner;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

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

    fn run_dlq<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>;
}
