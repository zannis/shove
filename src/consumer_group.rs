//! Public `ConsumerGroup<B, Ctx>` -- specialist harness for coordinated
//! consumer groups. Gated on `B: HasCoordinatedGroups`.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::backend::RegistryImpl;
use crate::backend::capability::HasCoordinatedGroups;
use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

pub struct ConsumerGroup<B: HasCoordinatedGroups, Ctx: Clone + Send + Sync + 'static = ()> {
    pub(crate) inner: B::RegistryImpl,
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

    pub async fn run_until_timeout<S>(self, signal: S, drain_timeout: Duration) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        self.inner.run_until_timeout(signal, drain_timeout).await
    }
}

#[cfg(all(test, feature = "inmemory"))]
mod tests {
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    use crate::define_sequenced_topic;
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
}
