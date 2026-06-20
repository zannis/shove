//! Public `ConsumerGroup<B, Ctx>` -- specialist harness for coordinated
//! consumer groups. Gated on `B: HasCoordinatedGroups`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::autoscaler::AutoscalerConfig;
use crate::backend::RegistryImpl;
use crate::backend::capability::HasCoordinatedGroups;
use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

pub struct ConsumerGroup<B: HasCoordinatedGroups, Ctx: Clone + Send + Sync + 'static = ()> {
    pub(crate) inner: B::RegistryImpl,
    client: B::Client,
    autoscaler: Option<AutoscalerConfig>,
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
    pub(crate) fn new(inner: B::RegistryImpl, client: B::Client) -> Self {
        Self {
            inner,
            client,
            autoscaler: None,
            ctx: (),
        }
    }

    pub fn with_context<Ctx: Clone + Send + Sync + 'static>(
        self,
        ctx: Ctx,
    ) -> ConsumerGroup<B, Ctx> {
        ConsumerGroup {
            inner: self.inner,
            client: self.client,
            autoscaler: self.autoscaler,
            ctx,
        }
    }
}

impl<B: HasCoordinatedGroups, Ctx: Clone + Send + Sync + 'static> ConsumerGroup<B, Ctx> {
    /// Register a concurrent (unsequenced) topic handler.
    ///
    /// The backend **automatically declares** the required broker topology
    /// (queues, streams, topics, consumer groups — whatever the backend
    /// needs) before returning. Callers do **not** need to call
    /// `broker.topology().declare::<T>()` first; doing so is harmless but
    /// redundant. This guarantee holds across all backends (Redis, InMemory,
    /// RabbitMQ, NATS, Kafka, SQS).
    ///
    /// Returns an error if:
    /// - `T` has a sequencing config — use [`register_fifo`] instead.
    /// - The topic is already registered in this registry.
    /// - Topology declaration fails (e.g. broker unreachable).
    ///
    /// [`register_fifo`]: Self::register_fifo
    pub async fn register<T, H>(
        &mut self,
        config: ConsumerGroupConfig<B>,
        factory: impl Fn() -> H + Send + Sync + 'static,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T, Context = Ctx>,
    {
        if T::topology().sequencing().is_some() {
            return Err(ShoveError::Topology(format!(
                "topic '{}' has a sequencing config; `ConsumerGroup::register` \
                 would silently drop FIFO ordering. Use `register_fifo` instead.",
                T::topology().queue(),
            )));
        }
        self.inner
            .register::<T, H>(config.inner, factory, self.ctx.clone())
            .await
    }

    /// Register a FIFO (sequenced) topic handler.
    ///
    /// The backend **automatically declares** the required broker topology
    /// (queues, streams, shard queues, consumer groups — whatever the backend
    /// needs) before returning. Callers do **not** need to call
    /// `broker.topology().declare::<T>()` first; doing so is harmless but
    /// redundant. This guarantee holds across all backends (Redis, InMemory,
    /// RabbitMQ, NATS, Kafka, SQS).
    ///
    /// Returns an error if:
    /// - `T`'s topology has no sequencing config — use [`register`] instead.
    /// - The topic is already registered in this registry.
    /// - Topology declaration fails (e.g. broker unreachable).
    ///
    /// [`register`]: Self::register
    pub async fn register_fifo<T, H>(
        &mut self,
        config: ConsumerGroupConfig<B>,
        factory: impl Fn() -> H + Send + Sync + 'static,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T, Context = Ctx>,
    {
        if T::topology().sequencing().is_none() {
            return Err(ShoveError::Topology(format!(
                "topic '{}' implements `SequencedTopic` but its topology has no \
                 sequencing config; `ConsumerGroup::register_fifo` would attach to \
                 FIFO shard queues that were never declared. Use `register` for \
                 unsequenced topics, or add `.sequenced(...)` to the topology.",
                T::topology().queue(),
            )));
        }
        self.inner
            .register_fifo::<T, H>(config.inner, factory, self.ctx.clone())
            .await
    }

    /// Set a default handler timeout applied to every group registered
    /// through this `ConsumerGroup` whose per-group config did not call
    /// `with_handler_timeout`. Per-group explicit settings always win.
    ///
    /// Must be called **before** [`register`] / [`register_fifo`]. Each
    /// backend resolves the effective handler timeout at registration
    /// time, so a call after the first registration silently has no
    /// effect on the already-registered group.
    ///
    /// `broker.consumer_group()` also constructs a fresh registry on
    /// every call, so a default set on one returned handle does not
    /// propagate to subsequent `broker.consumer_group()` results.
    ///
    /// [`register`]: Self::register
    /// [`register_fifo`]: Self::register_fifo
    pub fn with_default_handler_timeout(mut self, timeout: std::time::Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "default_handler_timeout must be positive"
        );
        self.inner.set_default_handler_timeout(timeout);
        self
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation_token()
    }

    /// Enable autoscaling for this group. The autoscaler polls queue depth on
    /// `config.poll_interval` and scales consumers within the per-group
    /// `ConsumerGroupConfig` min/max range using `Stabilized<ThresholdStrategy>`.
    /// Its lifecycle is owned by [`run_until_timeout`](Self::run_until_timeout):
    /// it is stopped and joined before consumers drain. A panic in the
    /// autoscaler task (observed before the stop deadline) surfaces in the
    /// returned [`SupervisorOutcome`] as a panic.
    pub fn enable_autoscaling(mut self, config: AutoscalerConfig) -> Self {
        self.autoscaler = Some(config);
        self
    }

    pub async fn run_until_timeout<S>(self, signal: S, drain_timeout: Duration) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        let Some(config) = self.autoscaler else {
            return self.inner.run_until_timeout(signal, drain_timeout).await;
        };

        let mut inner = self.inner;
        // Token we race the external signal against; cancelling it cascades to
        // consumers exactly as the non-autoscaling path's broker token does.
        let consumer_token = inner.cancellation_token();
        inner.start_all();

        let registry = Arc::new(Mutex::new(inner));
        // Dedicated token so we can stop the autoscaler *before* draining
        // consumers (ordering required by the shutdown contract).
        let auto_token = CancellationToken::new();
        let handle =
            B::spawn_autoscaler(&self.client, registry.clone(), config, auto_token.clone());

        // Wait for the external signal or an externally-triggered cancel.
        let mut signal_task = tokio::spawn(signal);
        tokio::select! {
            _ = consumer_token.cancelled() => { signal_task.abort(); }
            res = &mut signal_task => { let _ = res; }
        }

        // Stop the autoscaler first; bounded-join so a stuck metrics poll can't
        // consume the drain budget. Count a panic observed before the deadline.
        auto_token.cancel();
        let mut autoscaler_panics = 0usize;
        let grace = Duration::from_secs(1);
        let mut handle = handle;
        tokio::select! {
            res = &mut handle => {
                if matches!(&res, Err(e) if e.is_panic()) {
                    autoscaler_panics += 1;
                }
            }
            _ = tokio::time::sleep(grace) => {
                handle.abort();
                if let Err(e) = handle.await
                    && e.is_panic()
                {
                    autoscaler_panics += 1;
                }
            }
        }

        // Autoscaler task (and its registry Arc clone) is now gone: sole owner.
        let inner = Arc::try_unwrap(registry)
            .unwrap_or_else(|_| unreachable!("autoscaler joined; registry Arc must be sole-owned"))
            .into_inner();

        // Broadcast shutdown to every registered group at once before draining,
        // matching the non-autoscaling lifecycle: the non-autoscaling path calls
        // broker_token.cancel() up-front so all group tokens are cancelled
        // concurrently before any sequential per-group drain begins.
        // Without this, drain_all_into cancels groups one at a time as it
        // iterates, so groups 2..N keep consuming until the sequential drain
        // reaches them.
        consumer_token.cancel();

        let mut outcome = inner.drain_until_timeout(drain_timeout).await;
        outcome.panics += autoscaler_panics;
        outcome
    }
}

#[cfg(all(test, feature = "inmemory"))]
mod tests {
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    use crate::autoscaler::AutoscalerConfig;
    use crate::define_sequenced_topic;
    use crate::define_topic;
    use crate::inmemory::{InMemoryConfig, InMemoryConsumerGroupConfig};
    use crate::topic::SequencedTopic;
    use crate::topology::{SequenceFailure, TopologyBuilder};
    use crate::{
        Broker, ConsumerGroupConfig, InMemory, MessageHandler, MessageMetadata, Outcome, ShoveError,
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct LedgerEntry {
        account_id: String,
    }

    define_sequenced_topic!(
        Ledger,
        LedgerEntry,
        |msg| msg.account_id.clone(),
        TopologyBuilder::new("ledger-test")
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
    async fn register_rejects_sequenced_topic() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        broker
            .topology()
            .declare::<Ledger>()
            .await
            .expect("declare");

        let mut group = broker.consumer_group();
        let result = group
            .register::<Ledger, _>(
                ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=1)),
                || NoopHandler,
            )
            .await;

        match result {
            Err(ShoveError::Topology(msg)) => {
                assert!(
                    msg.contains("sequencing config") && msg.contains("register_fifo"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("expected Topology error, got {other:?}"),
        }

        // Drain shouldn't hang on a group that never registered anything.
        let outcome = group
            .run_until_timeout(std::future::ready(()), Duration::from_millis(100))
            .await;
        assert_eq!(outcome.exit_code(), 0);
    }

    #[tokio::test]
    async fn register_fifo_drains_through_run_until_timeout() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        broker
            .topology()
            .declare::<Ledger>()
            .await
            .expect("declare");

        let mut group = broker.consumer_group();
        group
            .register_fifo::<Ledger, _>(
                ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::default()),
                || NoopHandler,
            )
            .await
            .expect("register_fifo");

        let outcome = group
            .run_until_timeout(std::future::ready(()), Duration::from_millis(500))
            .await;
        assert_eq!(outcome.exit_code(), 0);
    }

    #[tokio::test]
    async fn with_default_handler_timeout_chains_and_runs_clean() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        broker
            .topology()
            .declare::<Ledger>()
            .await
            .expect("declare");

        let mut group = broker
            .consumer_group()
            .with_default_handler_timeout(Duration::from_secs(5));
        group
            .register_fifo::<Ledger, _>(
                ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::default()),
                || NoopHandler,
            )
            .await
            .expect("register_fifo");

        let outcome = group
            .run_until_timeout(std::future::ready(()), Duration::from_millis(500))
            .await;
        assert_eq!(outcome.exit_code(), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "default_handler_timeout must be positive")]
    async fn with_default_handler_timeout_zero_panics() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        let _ = broker
            .consumer_group()
            .with_default_handler_timeout(Duration::ZERO);
    }

    // Two unsequenced topics used by the two-group autoscaling shutdown test.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct AlphaEvent {
        id: u32,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct BetaEvent {
        id: u32,
    }

    define_topic!(
        AlphaTopic,
        AlphaEvent,
        TopologyBuilder::new("alpha-autoscale-test").build()
    );
    define_topic!(
        BetaTopic,
        BetaEvent,
        TopologyBuilder::new("beta-autoscale-test").build()
    );

    struct AlphaHandler;
    impl MessageHandler<AlphaTopic> for AlphaHandler {
        type Context = ();
        async fn handle(&self, _: AlphaEvent, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    struct BetaHandler;
    impl MessageHandler<BetaTopic> for BetaHandler {
        type Context = ();
        async fn handle(&self, _: BetaEvent, _: MessageMetadata, _: &()) -> Outcome {
            Outcome::Ack
        }
    }

    /// Regression test for the autoscaling broadcast-cancel fix.
    ///
    /// Two groups are registered, autoscaling is enabled, and an immediate
    /// shutdown signal is sent. With the fix, `consumer_token.cancel()` is
    /// called before `drain_until_timeout`, broadcasting shutdown to BOTH
    /// group tokens at once. Without the fix, groups are cancelled sequentially
    /// inside `drain_all_into`, which means group 2's token is not cancelled
    /// until after group 1 fully drains.
    ///
    /// This test verifies a clean outcome (exit_code 0, not timed out) within
    /// a drain_timeout that is sufficient for concurrent cancellation but would
    /// be tight for sequential cancellation if consumers were doing slow work.
    /// With an empty queue the consumers are idle, so the timing difference is
    /// not observable here; what the test guarantees is that the two-group
    /// autoscaling shutdown path completes correctly and reports a clean outcome.
    #[tokio::test]
    async fn autoscaling_two_groups_broadcast_cancel_before_drain() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");

        let mut group = broker.consumer_group();
        group
            .register::<AlphaTopic, _>(
                ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=2)),
                || AlphaHandler,
            )
            .await
            .expect("register AlphaTopic");
        group
            .register::<BetaTopic, _>(
                ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::new(1..=2)),
                || BetaHandler,
            )
            .await
            .expect("register BetaTopic");

        // Immediate signal: shutdown fires before any polling cycle.
        let outcome = group
            .enable_autoscaling(AutoscalerConfig {
                poll_interval: Duration::from_millis(50),
                ..AutoscalerConfig::default()
            })
            .run_until_timeout(std::future::ready(()), Duration::from_millis(500))
            .await;

        assert_eq!(
            outcome.exit_code(),
            0,
            "two-group autoscaling shutdown must be clean: {outcome:?}"
        );
        assert!(
            !outcome.timed_out,
            "drain must not time out with broadcast cancel: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn enable_autoscaling_runs_and_drains_clean() {
        let broker = Broker::<InMemory>::new(InMemoryConfig::default())
            .await
            .expect("broker");
        broker
            .topology()
            .declare::<Ledger>()
            .await
            .expect("declare");

        let mut group = broker.consumer_group();
        group
            .register_fifo::<Ledger, _>(
                ConsumerGroupConfig::new(InMemoryConsumerGroupConfig::default()),
                || NoopHandler,
            )
            .await
            .expect("register_fifo");

        let outcome = group
            .enable_autoscaling(AutoscalerConfig {
                poll_interval: Duration::from_millis(50),
                ..AutoscalerConfig::default()
            })
            .run_until_timeout(
                async { tokio::time::sleep(Duration::from_millis(150)).await },
                Duration::from_millis(500),
            )
            .await;
        assert_eq!(outcome.exit_code(), 0);
    }
}
