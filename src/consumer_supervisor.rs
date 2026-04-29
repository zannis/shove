//! Consumer supervisor harness and its outcome type. See DESIGN_V2.md §6.5.

use std::time::Duration;

use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::backend::{Backend, ConsumerImpl};
use crate::consumer::ConsumerOptions;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::Topic;

/// Result of draining a supervisor or consumer group.
#[must_use]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SupervisorOutcome {
    pub errors: usize,
    pub panics: usize,
    pub timed_out: bool,
}

impl SupervisorOutcome {
    /// Canonical process exit code: `0` clean, `1` any error, `2` any panic,
    /// `3` drain timeout. Highest non-zero condition wins.
    pub fn exit_code(&self) -> i32 {
        if self.timed_out {
            3
        } else if self.panics > 0 {
            2
        } else if self.errors > 0 {
            1
        } else {
            0
        }
    }

    /// True when no errors, panics, or drain timeouts were recorded.
    pub fn is_clean(&self) -> bool {
        self.exit_code() == 0
    }
}

/// Internal tally of errors and panics captured while draining a consumer
/// group. Each backend's `ConsumerGroup::shutdown_with_tally` and
/// `ConsumerGroupRegistry::shutdown_all_with_tally` fill this out so
/// `RegistryImpl::run_until_timeout` can return a truthful
/// [`SupervisorOutcome`].
///
/// Only coordinated-group backends (RabbitMQ / Kafka / NATS / InMemory) use
/// this; a supervisor-only build such as `aws-sns-sqs` doesn't.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ShutdownTally {
    pub errors: usize,
    pub panics: usize,
}

#[allow(dead_code)]
impl ShutdownTally {
    pub(crate) fn add(&mut self, other: ShutdownTally) {
        self.errors += other.errors;
        self.panics += other.panics;
    }
}

// ---------------------------------------------------------------------------
// Shared drain helper
// ---------------------------------------------------------------------------

/// Map a `JoinSet::join_next` result onto running `errors` / `panics`
/// counters. Shared by `ConsumerSupervisor::run_until_timeout` and each
/// backend's `run_fifo_until_timeout`.
///
/// `Ok(Err(ShoveError))` → `errors += 1`
/// Cancellation (`JoinError::is_cancelled`) → ignored
/// Other `JoinError` → `panics += 1`
pub(crate) fn tally_join_result(
    res: std::result::Result<Result<()>, JoinError>,
    errors: &mut usize,
    panics: &mut usize,
) {
    match res {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::error!(error = %e, "consumer task failed");
            *errors += 1;
        }
        Err(e) if e.is_cancelled() => {}
        Err(e) => {
            tracing::error!(error = %e, "consumer task panicked");
            *panics += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// ConsumerSupervisor<B, Ctx>
// ---------------------------------------------------------------------------

pub struct ConsumerSupervisor<B: Backend, Ctx: Clone + Send + Sync + 'static = ()> {
    consumer: B::ConsumerImpl,
    ctx: Ctx,
    shutdown: CancellationToken,
    tasks: JoinSet<Result<()>>,
}

impl<B: Backend> ConsumerSupervisor<B, ()> {
    pub(crate) fn new(client: &B::Client) -> Self {
        Self {
            consumer: B::make_consumer(client),
            ctx: (),
            shutdown: CancellationToken::new(),
            tasks: JoinSet::new(),
        }
    }

    pub fn with_context<Ctx: Clone + Send + Sync + 'static>(
        self,
        ctx: Ctx,
    ) -> ConsumerSupervisor<B, Ctx> {
        ConsumerSupervisor {
            consumer: self.consumer,
            ctx,
            shutdown: self.shutdown,
            tasks: self.tasks,
        }
    }
}

impl<B: Backend, Ctx: Clone + Send + Sync + 'static> ConsumerSupervisor<B, Ctx> {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub fn register<T, H>(&mut self, handler: H, options: ConsumerOptions<B>) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = Ctx>,
    {
        if T::topology().sequencing().is_some() {
            return Err(ShoveError::Topology(format!(
                "topic '{}' has a sequencing config; `ConsumerSupervisor::register` \
                 would silently drop FIFO ordering. Use the backend-specific \
                 consumer's `run_fifo` for sequenced topics until FIFO registration \
                 is surfaced on the harness.",
                T::topology().queue(),
            )));
        }
        let consumer = self.consumer.clone();
        let ctx = self.ctx.clone();
        let inner = options.with_shutdown(self.shutdown.clone()).into_inner();
        self.tasks
            .spawn(async move { consumer.run::<T, H>(handler, ctx, inner).await });
        Ok(())
    }

    pub async fn run_until_timeout<S>(
        mut self,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        tokio::select! {
            _ = signal => { self.shutdown.cancel(); }
            _ = self.shutdown.cancelled() => {}
        }

        let mut errors = 0usize;
        let mut panics = 0usize;

        let drain = {
            let tasks = &mut self.tasks;
            let errors = &mut errors;
            let panics = &mut panics;
            async move {
                while let Some(res) = tasks.join_next().await {
                    tally_join_result(res, errors, panics);
                }
            }
        };

        match tokio::time::timeout(drain_timeout, drain).await {
            Ok(()) => SupervisorOutcome { errors, panics, timed_out: false },
            Err(_) => {
                tracing::warn!(
                    timeout_ms = drain_timeout.as_millis() as u64,
                    "drain timeout elapsed; aborting surviving tasks"
                );
                self.tasks.abort_all();
                while let Some(res) = self.tasks.join_next().await {
                    tally_join_result(res, &mut errors, &mut panics);
                }
                SupervisorOutcome { errors, panics, timed_out: true }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_outcome_has_exit_code_zero() {
        assert_eq!(SupervisorOutcome::default().exit_code(), 0);
        assert!(SupervisorOutcome::default().is_clean());
    }

    #[test]
    fn errors_produce_exit_code_one() {
        let o = SupervisorOutcome {
            errors: 3,
            panics: 0,
            timed_out: false,
        };
        assert_eq!(o.exit_code(), 1);
    }

    #[test]
    fn panics_outrank_errors() {
        let o = SupervisorOutcome {
            errors: 3,
            panics: 1,
            timed_out: false,
        };
        assert_eq!(o.exit_code(), 2);
    }

    #[test]
    fn timeout_outranks_everything() {
        let o = SupervisorOutcome {
            errors: 3,
            panics: 1,
            timed_out: true,
        };
        assert_eq!(o.exit_code(), 3);
    }
}
