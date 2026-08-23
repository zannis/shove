//! Internal `BroadcastImpl` trait. Backends with an ephemeral per-instance
//! subscription primitive implement this; the public
//! [`BroadcastSubscriber<B>`](crate::broadcast::BroadcastSubscriber) delegates
//! here.
//!
//! Kept separate from [`ConsumerImpl`](crate::backend::ConsumerImpl) so that a
//! backend can be a full consumer without being able to broadcast — which is
//! exactly SQS's situation, and the reason
//! [`HasBroadcast`](crate::backend::capability::HasBroadcast) exists.

use std::future::Future;

use crate::backend::ConsumerOptionsInner;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::Topic;

// Anchored by the InMemory port's `_anchor_broadcast_impl` helper in
// `backend::mod`; genuinely uncalled under `--no-default-features`, where no
// backend is compiled at all.
#[allow(dead_code)]
pub(crate) trait BroadcastImpl: Send + Sync {
    /// Create this process's own ephemeral subscription to `T`'s topic and run
    /// `handler` against it until `options.shutdown` fires.
    ///
    /// # Contract
    ///
    /// - **Deliver-new.** Only messages published after this future has
    ///   subscribed are delivered. Nothing published earlier is replayed.
    /// - **Nothing survives.** Every piece of broker-side state the
    ///   subscription creates is torn down before the returned future
    ///   resolves — no consumer group, no durable consumer, no leftover queue.
    ///   The teardown also has to survive the task being dropped mid-run,
    ///   which is what a drain-timeout abort does.
    /// - **One consumer.** Exactly one delivery loop per call. Broadcast has no
    ///   autoscaling: a second consumer in the same process would split this
    ///   subscription rather than duplicate it.
    fn run_broadcast<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>;
}
