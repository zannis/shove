//! Internal `RegistryImpl` trait for coordinated consumer groups (Kafka,
//! RabbitMQ, NATS, InMemory). Not implemented by SQS. See DESIGN_V2.md §5.

// Skeleton: implementors and call-sites land in subsequent phases.
#![allow(dead_code)]

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::Topic;

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

    fn cancellation_token(&self) -> CancellationToken;

    fn run_until_timeout<S>(
        self,
        signal: S,
        drain_timeout: Duration,
    ) -> impl Future<Output = SupervisorOutcome> + Send
    where
        S: Future<Output = ()> + Send + 'static;
}
