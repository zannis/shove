//! Consumer supervisor harness and its outcome type.

use std::time::Duration;

use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::backend::{Backend, ConsumerImpl};
use crate::broadcast::reject_broadcast;
use crate::consumer::ConsumerOptions;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

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

/// RAII guard that calls `AbortHandle::abort` on drop. Used to ensure that
/// when a wrapper task around a FIFO shard `JoinHandle` is itself aborted
/// (e.g. by `JoinSet::abort_all`), the inner shard task is aborted too —
/// not merely detached, which is what dropping a bare `JoinHandle` does.
pub(crate) struct AbortOnDrop(pub(crate) tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Map a `JoinSet::join_next` result onto running `errors` / `panics`
/// counters. Shared by `ConsumerSupervisor::run_until_timeout` and each
/// backend's `run_fifo_until_timeout`.
///
/// `Ok(Ok(()))` → success, ignored
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

/// Drive an already-spawned set of FIFO shard task handles to completion or
/// timeout, mirroring [`ConsumerSupervisor::run_until_timeout`] for
/// sequenced topics.
///
/// Each handle is wrapped in a [`JoinSet`] entry that maps
/// abort-cancellation to `Ok(())` and re-panics on real panics, so the
/// outer drain can use [`tally_join_result`] uniformly.
///
/// - Races `signal` against shards finishing on their own. `biased` puts
///   `signal` first so a shutdown trigger that's already ready wins ties
///   with a shard that has just completed — `shards_done` then runs as the
///   post-cancel drain instead.
/// - On signal, cancels `shutdown` and waits up to `drain_timeout` for
///   shards to finish.
/// - On timeout, calls `JoinSet::abort_all` and sets
///   [`SupervisorOutcome::timed_out`] = `true`. The wrapper holds an
///   [`AbortOnDrop`] guard around the inner shard `JoinHandle`, so aborting
///   the wrapper task aborts the inner shard task too — without it,
///   `JoinHandle::drop` would just *detach* the inner task, leaking it.
/// - `drain_timeout = Duration::ZERO` means "abort immediately after
///   `signal` fires" (no grace period); this is honored verbatim.
#[allow(dead_code)] // every caller is feature-gated; unused under --no-default-features
pub(crate) async fn drive_fifo_until_timeout<S>(
    handles: Vec<tokio::task::JoinHandle<Result<()>>>,
    shutdown: CancellationToken,
    signal: S,
    drain_timeout: Duration,
) -> SupervisorOutcome
where
    S: Future<Output = ()> + Send + 'static,
{
    let mut joinset: JoinSet<Result<()>> = JoinSet::new();
    for handle in handles {
        let abort_guard = AbortOnDrop(handle.abort_handle());
        joinset.spawn(async move {
            // Bind the guard to the wrapper task's stack so it drops with
            // the wrapper future — including when the wrapper is itself
            // aborted by `JoinSet::abort_all`. Calling `abort()` on an
            // already-finished inner task is a no-op.
            let _abort_guard = abort_guard;
            match handle.await {
                Ok(r) => r,
                Err(e) if e.is_cancelled() => Ok(()),
                Err(e) => std::panic::resume_unwind(e.into_panic()),
            }
        });
    }

    let mut errors = 0usize;
    let mut panics = 0usize;

    let shards_done = {
        let errors = &mut errors;
        let panics = &mut panics;
        let joinset = &mut joinset;
        async move {
            while let Some(res) = joinset.join_next().await {
                tally_join_result(res, errors, panics);
            }
        }
    };

    let signal_won = tokio::select! {
        biased;
        _ = signal => true,
        _ = shards_done => false,
    };

    if !signal_won {
        return SupervisorOutcome {
            errors,
            panics,
            timed_out: false,
        };
    }

    shutdown.cancel();
    let drain = async {
        while let Some(res) = joinset.join_next().await {
            tally_join_result(res, &mut errors, &mut panics);
        }
    };
    match tokio::time::timeout(drain_timeout, drain).await {
        Ok(()) => SupervisorOutcome {
            errors,
            panics,
            timed_out: false,
        },
        Err(_) => {
            tracing::warn!(
                timeout_ms = drain_timeout.as_millis() as u64,
                "run_fifo_until_timeout: drain timed out; aborting surviving shards"
            );
            joinset.abort_all();
            while let Some(res) = joinset.join_next().await {
                tally_join_result(res, &mut errors, &mut panics);
            }
            SupervisorOutcome {
                errors,
                panics,
                timed_out: true,
            }
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
    /// Queue names of every topic registered through `register` /
    /// `register_fifo`, used to reject duplicate registrations on the same
    /// supervisor (which would silently double-spawn consumer or shard
    /// tasks on the same broker queue).
    registered: std::collections::HashSet<&'static str>,
}

impl<B: Backend> ConsumerSupervisor<B, ()> {
    pub(crate) fn new(client: &B::Client) -> Self {
        Self {
            consumer: B::make_consumer(client),
            ctx: (),
            shutdown: CancellationToken::new(),
            tasks: JoinSet::new(),
            registered: std::collections::HashSet::new(),
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
            registered: self.registered,
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
        let queue = T::topology().queue();
        reject_broadcast::<T>("ConsumerSupervisor::register")?;
        if T::topology().sequencing().is_some() {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}' has a sequencing config; `ConsumerSupervisor::register` \
                 would silently drop FIFO ordering. Use `register_fifo` instead."
            )));
        }
        if !self.registered.insert(queue) {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}' is already registered on this supervisor"
            )));
        }
        let consumer = self.consumer.clone();
        let ctx = self.ctx.clone();
        let inner = options.with_shutdown(self.shutdown.clone()).into_inner();
        self.tasks
            .spawn(async move { consumer.run::<T, H>(handler, ctx, inner).await });
        Ok(())
    }

    pub async fn register_fifo<T, H>(
        &mut self,
        handler: H,
        options: ConsumerOptions<B>,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T, Context = Ctx>,
    {
        let queue = T::topology().queue();
        if T::topology().sequencing().is_none() {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}' implements `SequencedTopic` but its topology has no \
                 sequencing config; `ConsumerSupervisor::register_fifo` would attach to \
                 FIFO shard queues that were never declared. Use `register` for \
                 unsequenced topics, or add `.sequenced(...)` to the topology."
            )));
        }
        if !self.registered.insert(queue) {
            return Err(ShoveError::Topology(format!(
                "topic '{queue}' is already registered on this supervisor"
            )));
        }
        let ctx = self.ctx.clone();
        let inner = options.with_shutdown(self.shutdown.clone()).into_inner();
        let handles = self
            .consumer
            .spawn_fifo_shards::<T, H>(handler, ctx, inner)
            .await?;
        for handle in handles {
            // Hold an AbortOnDrop guard on the inner shard handle so that
            // when the supervisor's JoinSet aborts this wrapper (e.g. on
            // drain timeout escalation in `run_until_timeout`), the inner
            // shard task is aborted instead of detached.
            let abort_guard = AbortOnDrop(handle.abort_handle());
            self.tasks.spawn(async move {
                let _abort_guard = abort_guard;
                match handle.await {
                    Ok(r) => r,
                    Err(e) if e.is_cancelled() => Ok(()),
                    Err(e) => std::panic::resume_unwind(e.into_panic()),
                }
            });
        }
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
            Ok(()) => SupervisorOutcome {
                errors,
                panics,
                timed_out: false,
            },
            Err(_) => {
                tracing::warn!(
                    timeout_ms = drain_timeout.as_millis() as u64,
                    "drain timeout elapsed; aborting surviving tasks"
                );
                self.tasks.abort_all();
                while let Some(res) = self.tasks.join_next().await {
                    tally_join_result(res, &mut errors, &mut panics);
                }
                SupervisorOutcome {
                    errors,
                    panics,
                    timed_out: true,
                }
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

    use crate::error::ShoveError;

    #[tokio::test]
    async fn tally_increments_errors_on_task_failure() {
        let mut errors = 0usize;
        let mut panics = 0usize;
        tally_join_result(
            Ok(Err(ShoveError::Topology("test".into()))),
            &mut errors,
            &mut panics,
        );
        assert_eq!(errors, 1);
        assert_eq!(panics, 0);
    }

    #[tokio::test]
    async fn tally_increments_panics_on_join_panic() {
        let handle = tokio::spawn(async { panic!("boom") });
        let join_err = handle.await.unwrap_err();
        assert!(join_err.is_panic());
        let mut errors = 0usize;
        let mut panics = 0usize;
        tally_join_result(Err(join_err), &mut errors, &mut panics);
        assert_eq!(panics, 1);
        assert_eq!(errors, 0);
    }

    #[tokio::test]
    async fn tally_ignores_cancellation() {
        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        });
        handle.abort();
        let join_err = handle.await.unwrap_err();
        assert!(join_err.is_cancelled());
        let mut errors = 0usize;
        let mut panics = 0usize;
        tally_join_result(Err(join_err), &mut errors, &mut panics);
        assert_eq!(errors, 0);
        assert_eq!(panics, 0);
    }

    #[test]
    fn tally_does_not_count_success() {
        let mut errors = 0usize;
        let mut panics = 0usize;
        tally_join_result(Ok(Ok(())), &mut errors, &mut panics);
        assert_eq!(errors, 0);
        assert_eq!(panics, 0);
    }

    // -----------------------------------------------------------------------
    // drive_fifo_until_timeout — synthetic JoinHandle tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn drive_fifo_until_timeout_clean_completion_no_signal() {
        // All shards finish on their own before the signal fires. The
        // `shards_done` arm of the biased select wins, so the function
        // returns a clean outcome without ever cancelling the shutdown token.
        let handles: Vec<tokio::task::JoinHandle<Result<()>>> = vec![
            tokio::spawn(async { Ok(()) }),
            tokio::spawn(async { Ok(()) }),
        ];
        let shutdown = CancellationToken::new();
        let outcome = drive_fifo_until_timeout(
            handles,
            shutdown.clone(),
            std::future::pending::<()>(),
            Duration::from_secs(1),
        )
        .await;
        assert!(outcome.is_clean(), "unexpected outcome: {outcome:?}");
        assert!(
            !shutdown.is_cancelled(),
            "shutdown must not be cancelled when shards complete on their own"
        );
    }

    #[tokio::test]
    async fn drive_fifo_until_timeout_signal_then_clean_drain() {
        // Signal fires while shards are sleeping; they complete inside the
        // drain window so timed_out stays false.
        let handles: Vec<tokio::task::JoinHandle<Result<()>>> = vec![
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }),
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }),
        ];
        let shutdown = CancellationToken::new();
        let outcome = drive_fifo_until_timeout(
            handles,
            shutdown.clone(),
            async { /* signal ready immediately */ },
            Duration::from_secs(1),
        )
        .await;
        assert!(outcome.is_clean(), "unexpected outcome: {outcome:?}");
        assert!(
            shutdown.is_cancelled(),
            "shutdown must be cancelled once the signal fires"
        );
    }

    #[tokio::test]
    async fn drive_fifo_until_timeout_drain_timeout_aborts_surviving_shards() {
        // Shards never finish on their own; after signal fires the drain
        // window elapses and `abort_all` short-circuits them. Cancellation
        // surfaces as `is_cancelled()` join errors that the wrapper maps to
        // Ok(()), so the outcome is timed_out without false errors/panics.
        let handles: Vec<tokio::task::JoinHandle<Result<()>>> = (0..3)
            .map(|_| {
                tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok(())
                })
            })
            .collect();
        let shutdown = CancellationToken::new();
        let outcome = drive_fifo_until_timeout(
            handles,
            shutdown.clone(),
            async {},
            Duration::from_millis(20),
        )
        .await;
        assert!(outcome.timed_out, "expected timed_out: {outcome:?}");
        assert_eq!(outcome.errors, 0);
        assert_eq!(outcome.panics, 0);
        assert!(shutdown.is_cancelled());
    }

    #[tokio::test]
    async fn drive_fifo_until_timeout_propagates_shard_panic() {
        // A panicking shard surfaces through the wrapper's
        // `resume_unwind(into_panic())` path. The outer JoinSet catches the
        // resumed panic and tally_join_result counts it as a panic.
        let handles: Vec<tokio::task::JoinHandle<Result<()>>> =
            vec![tokio::spawn(async { panic!("synthetic shard panic") })];
        let shutdown = CancellationToken::new();
        let outcome = drive_fifo_until_timeout(
            handles,
            shutdown,
            std::future::pending::<()>(),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            outcome.panics, 1,
            "shard panic must be tallied: {outcome:?}"
        );
        assert_eq!(outcome.errors, 0);
        assert!(!outcome.timed_out);
    }

    #[tokio::test]
    async fn drive_fifo_until_timeout_tallies_shard_error() {
        // A shard that returns `Err(_)` increments the error counter and
        // does not panic.
        let handles: Vec<tokio::task::JoinHandle<Result<()>>> = vec![tokio::spawn(async {
            Err(ShoveError::Topology("synthetic shard error".into()))
        })];
        let shutdown = CancellationToken::new();
        let outcome = drive_fifo_until_timeout(
            handles,
            shutdown,
            std::future::pending::<()>(),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            outcome.errors, 1,
            "shard error must be tallied: {outcome:?}"
        );
        assert_eq!(outcome.panics, 0);
        assert!(!outcome.timed_out);
    }
}

#[cfg(all(test, feature = "inmemory"))]
mod inmemory_tests {
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    use crate::consumer::ConsumerOptions;
    use crate::error::ShoveError;
    use crate::inmemory::InMemoryConfig;
    use crate::markers::InMemory;
    use crate::topic::SequencedTopic;
    use crate::topology::{SequenceFailure, TopologyBuilder};
    use crate::{
        Broker, MessageHandler, MessageMetadata, Outcome, define_sequenced_topic, define_topic,
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct LedgerEntry {
        account_id: String,
    }

    define_sequenced_topic!(
        Ledger,
        LedgerEntry,
        |msg| msg.account_id.clone(),
        TopologyBuilder::new("supervisor-ledger-test")
            .sequenced(SequenceFailure::FailAll)
            .hold_queue(Duration::from_millis(50))
            .dlq()
            .build()
    );

    struct NoopHandler;
    impl MessageHandler<Ledger> for NoopHandler {
        type Context = ();
        async fn handle(&self, _: LedgerEntry, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    #[tokio::test]
    async fn register_fifo_runs_cleanly() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        broker
            .topology()
            .declare::<Ledger>()
            .await
            .expect("declare");

        let mut sup = broker.consumer_supervisor();
        sup.register_fifo::<Ledger, _>(NoopHandler, ConsumerOptions::<InMemory>::new())
            .await
            .expect("register_fifo");

        let token = sup.cancellation_token();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            token.cancel();
        });

        let outcome = sup
            .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(1))
            .await;
        assert!(outcome.is_clean(), "unexpected outcome: {outcome:?}");
    }

    #[tokio::test]
    async fn register_rejection_points_at_register_fifo() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        broker
            .topology()
            .declare::<Ledger>()
            .await
            .expect("declare");

        let mut sup = broker.consumer_supervisor();
        let result = sup.register::<Ledger, _>(NoopHandler, ConsumerOptions::<InMemory>::new());
        match result {
            Err(ShoveError::Topology(msg)) => {
                assert!(msg.contains("register_fifo"), "unexpected msg: {msg}");
            }
            other => panic!("expected Topology error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn register_fifo_rejects_duplicate_topic() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        broker
            .topology()
            .declare::<Ledger>()
            .await
            .expect("declare");

        let mut sup = broker.consumer_supervisor();
        sup.register_fifo::<Ledger, _>(NoopHandler, ConsumerOptions::<InMemory>::new())
            .await
            .expect("first register_fifo should succeed");

        let result = sup
            .register_fifo::<Ledger, _>(NoopHandler, ConsumerOptions::<InMemory>::new())
            .await;
        match result {
            Err(ShoveError::Topology(msg)) => {
                assert!(msg.contains("already registered"), "unexpected msg: {msg}");
            }
            other => panic!("expected Topology error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // register / register_fifo additional rejection paths and drain timeout
    // -----------------------------------------------------------------------

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Note {
        body: String,
    }

    define_topic!(
        Notes,
        Note,
        TopologyBuilder::new("supervisor-notes-test").build()
    );

    struct NoteHandler;
    impl MessageHandler<Notes> for NoteHandler {
        type Context = ();
        async fn handle(&self, _: Note, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    // A SequencedTopic intentionally paired with a non-sequenced topology so
    // we can exercise the runtime check in `register_fifo` that the topology
    // actually has `.sequenced(...)`.
    define_sequenced_topic!(
        UnsequencedBySchema,
        LedgerEntry,
        |msg| msg.account_id.clone(),
        TopologyBuilder::new("supervisor-no-seq-test").build()
    );

    struct UnsequencedHandler;
    impl MessageHandler<UnsequencedBySchema> for UnsequencedHandler {
        type Context = ();
        async fn handle(&self, _: LedgerEntry, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    #[tokio::test]
    async fn register_rejects_duplicate_non_sequenced_topic() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        broker.topology().declare::<Notes>().await.expect("declare");

        let mut sup = broker.consumer_supervisor();
        sup.register::<Notes, _>(NoteHandler, ConsumerOptions::<InMemory>::new())
            .expect("first register should succeed");

        let result = sup.register::<Notes, _>(NoteHandler, ConsumerOptions::<InMemory>::new());
        match result {
            Err(ShoveError::Topology(msg)) => {
                assert!(msg.contains("already registered"), "unexpected msg: {msg}");
            }
            other => panic!("expected Topology error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn register_fifo_rejects_topic_without_sequencing_config() {
        // The macro-generated `UnsequencedBySchema` implements `SequencedTopic`
        // but its topology has no `.sequenced(...)`. `register_fifo` must
        // reject it rather than silently attaching to shard queues that were
        // never declared.
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        // Skip declaring the topology — declaration would also fail (or be
        // no-op) and isn't what we're testing.

        let mut sup = broker.consumer_supervisor();
        let result = sup
            .register_fifo::<UnsequencedBySchema, _>(
                UnsequencedHandler,
                ConsumerOptions::<InMemory>::new(),
            )
            .await;
        match result {
            Err(ShoveError::Topology(msg)) => {
                assert!(
                    msg.contains("no \nsequencing config") || msg.contains("sequencing config"),
                    "unexpected msg: {msg}"
                );
            }
            other => panic!("expected Topology error, got {other:?}"),
        }
    }

    /// Handler that ignores cancellation and never returns within the test
    /// window, forcing the supervisor's drain timeout to elapse.
    struct StuckHandler;
    impl MessageHandler<Notes> for StuckHandler {
        type Context = ();
        async fn handle(&self, _: Note, _: MessageMetadata, _: &()) -> Outcome {
            std::future::pending::<()>().await;
            Outcome::Ack
        }
    }

    #[tokio::test]
    async fn run_until_timeout_drain_timeout_aborts_surviving_tasks() {
        // Publish one message, register a handler that blocks forever, fire
        // the signal immediately. The consumer task is still inside the
        // handler when shutdown is cancelled, so the drain window elapses
        // and `abort_all` triggers the timed_out branch.
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        broker.topology().declare::<Notes>().await.expect("declare");

        // Disable the handler timeout so the handler stays inside `.handle()`
        // long enough for the drain timeout to fire instead of the handler
        // timeout returning Retry.
        let opts = ConsumerOptions::<InMemory>::new().without_handler_timeout();

        let mut sup = broker.consumer_supervisor();
        sup.register::<Notes, _>(StuckHandler, opts)
            .expect("register");

        let publisher = broker.publisher().await.expect("publisher");
        publisher
            .publish::<Notes>(&Note {
                body: "stuck".into(),
            })
            .await
            .expect("publish");

        // Give the consumer a moment to pick up the message and enter the
        // handler before we signal shutdown.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let outcome = sup
            .run_until_timeout(
                async { /* signal immediately */ },
                Duration::from_millis(50),
            )
            .await;

        assert!(
            outcome.timed_out,
            "expected drain timeout, got: {outcome:?}"
        );
        assert_eq!(outcome.exit_code(), 3);
    }
}
