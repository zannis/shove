//! Internal `RegistryImpl` trait for coordinated consumer groups (Kafka,
//! RabbitMQ, NATS, InMemory). Not implemented by SQS. See DESIGN_V2.md §5.
//!
//! `register` accepts an `H::Context` that each backend clones into every
//! spawned task so handlers with a non-unit
//! [`MessageHandler::Context`](MessageHandler::Context)
//! work through the generic `ConsumerGroup<B, Ctx>` harness.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

// Methods are anchored by the InMemory port's `_anchor_*` helpers in
// `backend::mod` under the `inmemory` feature. Under
// `--no-default-features` no backend is compiled, so the trait methods
// genuinely have no call site; `dead_code` is expected there and the
// per-trait allow avoids polluting the default build with warnings.
#[allow(dead_code)]
pub(crate) trait RegistryImpl: Send {
    type GroupConfig;

    fn register<T, H>(
        &mut self,
        config: Self::GroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: Topic,
        H: MessageHandler<T>;

    fn register_fifo<T, H>(
        &mut self,
        _factory: impl Fn() -> H + Send + Sync + 'static,
        _ctx: H::Context,
    ) -> impl Future<Output = Result<()>> + Send
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        async {
            Err(ShoveError::Topology(
                "register_fifo is not yet implemented for this backend; \
                 use the backend-specific consumer's run_fifo_until_timeout instead"
                    .into(),
            ))
        }
    }

    fn cancellation_token(&self) -> CancellationToken;

    /// Run the registered consumer-group loops until `signal` resolves or
    /// the group's cancellation token fires, then drain in-flight work
    /// with a bounded grace window and return a `SupervisorOutcome`.
    ///
    /// # Implementor notes
    ///
    /// - The registry is consumed (`self`, not `&mut self`). Any task
    ///   handles or `JoinSet`s held by the implementation are owned by
    ///   the returned future and drop at the end of the async body.
    ///   Conventional shape: hold per-task handles in a `JoinSet`,
    ///   drive them with `join_next()` until the drain deadline, then
    ///   call `abort_all()` and let `Drop` finalize surviving tasks.
    /// - Implementations MUST NOT call `std::process::exit` or panic;
    ///   the caller decides exit policy based on the returned outcome.
    /// - `drain_timeout == Duration::ZERO` is allowed and means "abort
    ///   immediately after `signal` fires" — implementations should
    ///   not treat zero as "no timeout".
    /// - The `signal` future is `'static` because implementations may
    ///   `tokio::spawn` it onto a task for concurrent selection with the
    ///   cancellation token.
    fn run_until_timeout<S>(
        self,
        signal: S,
        drain_timeout: Duration,
    ) -> impl Future<Output = SupervisorOutcome> + Send
    where
        S: Future<Output = ()> + Send + 'static;
}
