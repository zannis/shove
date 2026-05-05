//! Redis Streams consumer-group registry.
//!
//! [`RedisConsumerGroupRegistry`] owns a collection of lazily-constructed task
//! factories (closures returning boxed futures). Calling [`start_all`] spawns
//! every registered consumer into a [`JoinSet`], and [`run_until_timeout`]
//! drives the set to completion or a configurable drain deadline.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner;
use crate::consumer_supervisor::{SupervisorOutcome, tally_join_result};
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

use crate::backend::consumer::ConsumerImpl;

use super::client::RedisClient;
use super::consumer::RedisConsumer;
use super::topology::RedisTopologyDeclarer;

// ---------------------------------------------------------------------------
// TaskFactory type alias
// ---------------------------------------------------------------------------

type BoxFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type TaskFactory = Box<dyn FnOnce() -> BoxFuture + Send>;

// ---------------------------------------------------------------------------
// RedisConsumerGroupConfig
// ---------------------------------------------------------------------------

/// Configuration for a [`RedisConsumerGroupRegistry`] registration.
///
/// `consumer_count` controls how many concurrent consumer tasks are spawned
/// for a single non-FIFO topic (minimum 1). FIFO topics always spawn one
/// task per shard regardless of this setting.
#[derive(Debug, Clone)]
pub struct RedisConsumerGroupConfig {
    consumer_count: u16,
}

impl RedisConsumerGroupConfig {
    /// Create a new config with the given concurrent consumer count.
    pub fn new(consumer_count: u16) -> Self {
        Self {
            consumer_count: consumer_count.max(1),
        }
    }

    /// The configured consumer count.
    pub fn consumer_count(&self) -> u16 {
        self.consumer_count
    }
}

impl Default for RedisConsumerGroupConfig {
    fn default() -> Self {
        Self::new(1)
    }
}

// ---------------------------------------------------------------------------
// RedisConsumerGroupRegistry
// ---------------------------------------------------------------------------

/// Registry that accumulates consumer-task factories and then starts them all
/// into a [`JoinSet`].
///
/// Typical lifecycle:
/// 1. `let mut reg = RedisConsumerGroupRegistry::new(client);`
/// 2. `reg.register::<T, H>(...).await?;`  — one call per topic
/// 3. `reg.run_until_timeout(signal, drain_timeout).await`
pub struct RedisConsumerGroupRegistry {
    client: RedisClient,
    tasks: Vec<TaskFactory>,
    shutdown: CancellationToken,
}

impl RedisConsumerGroupRegistry {
    /// Create a new registry backed by the given Redis client.
    pub fn new(client: RedisClient) -> Self {
        Self {
            client,
            tasks: Vec::new(),
            shutdown: CancellationToken::new(),
        }
    }

    /// Return the broker-wide shutdown token.
    ///
    /// Cancelling this token propagates shutdown to every consumer task
    /// spawned by this registry.
    pub fn broker_shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Register a non-FIFO topic handler.
    ///
    /// Spawns `*config.consumer_range().start()` (minimum 1) concurrent
    /// consumer tasks when [`start_all`] is called. Each task gets its own
    /// clone of `ctx` (via `H::Context: Clone`, already guaranteed by the
    /// [`MessageHandler`] trait bound).
    ///
    /// Topology structures (stream + consumer group) are declared before
    /// returning.
    pub async fn register<T, H>(
        &mut self,
        config: RedisConsumerGroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: Topic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let topology = T::topology();
        let declarer = RedisTopologyDeclarer::new(self.client.clone());
        declarer.declare(topology).await?;

        let n = config.consumer_count() as usize;

        for _ in 0..n {
            let client = self.client.clone();
            let shutdown = self.shutdown.clone();
            let handler = factory();
            let ctx = ctx.clone();

            let task: TaskFactory = Box::new(move || {
                Box::pin(async move {
                    let consumer = RedisConsumer::new(client);
                    let options = ConsumerOptionsInner::defaults_with_shutdown(shutdown);
                    consumer.run::<T, H>(handler, ctx, options).await
                })
            });
            self.tasks.push(task);
        }

        Ok(())
    }

    /// Register a FIFO (sequenced) topic handler.
    ///
    /// Spawns one task per shard. Each task fans out internally across that
    /// shard's stream via [`RedisConsumer::run_fifo`], which internally calls
    /// [`spawn_fifo_shards`] and awaits all shard handles.
    ///
    /// Topology structures are declared before returning.
    pub async fn register_fifo<T, H>(
        &mut self,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: SequencedTopic + 'static,
        H: MessageHandler<T> + 'static,
    {
        let topology = T::topology();
        let declarer = RedisTopologyDeclarer::new(self.client.clone());
        declarer.declare(topology).await?;

        let client = self.client.clone();
        let shutdown = self.shutdown.clone();
        let handler = factory();
        let ctx = ctx.clone();

        let task: TaskFactory = Box::new(move || {
            Box::pin(async move {
                let consumer = RedisConsumer::new(client);
                let options = ConsumerOptionsInner::defaults_with_shutdown(shutdown);
                consumer.run_fifo::<T, H>(handler, ctx, options).await
            })
        });
        self.tasks.push(task);

        Ok(())
    }

    /// Drain the accumulated task factories into `set`.
    ///
    /// Each factory is consumed (called once) and its returned future is
    /// spawned as a new entry in the [`JoinSet`].
    pub fn start_all(&mut self, set: &mut JoinSet<Result<()>>) {
        for factory in self.tasks.drain(..) {
            set.spawn(factory());
        }
    }

    /// Start all tasks, wait for `signal` or the internal shutdown token,
    /// then drain with `drain_timeout`.
    ///
    /// Returns a [`SupervisorOutcome`] summarising errors, panics, and
    /// whether the drain timed out.
    pub async fn run_until_timeout<S>(
        mut self,
        signal: S,
        drain_timeout: Duration,
    ) -> SupervisorOutcome
    where
        S: Future<Output = ()> + Send + 'static,
    {
        let mut set: JoinSet<Result<()>> = JoinSet::new();
        self.start_all(&mut set);

        let shutdown = self.shutdown.clone();

        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = signal => { shutdown.cancel(); }
        }

        let mut errors = 0usize;
        let mut panics = 0usize;

        let drain = async {
            while let Some(res) = set.join_next().await {
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
                    "RedisConsumerGroupRegistry: drain timed out; aborting surviving tasks"
                );
                set.abort_all();
                // Drain aborted tasks so the JoinSet is fully emptied.
                while let Some(res) = set.join_next().await {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_consumer_count() {
        let cfg = RedisConsumerGroupConfig::new(4);
        assert_eq!(cfg.consumer_count(), 4);
    }

    #[test]
    fn config_default_count_is_one() {
        let cfg = RedisConsumerGroupConfig::default();
        assert_eq!(cfg.consumer_count(), 1);
    }

    #[test]
    fn config_zero_clamped_to_one() {
        let cfg = RedisConsumerGroupConfig::new(0);
        assert_eq!(cfg.consumer_count(), 1);
    }

    #[test]
    fn config_large_consumer_count() {
        let cfg = RedisConsumerGroupConfig::new(u16::MAX);
        assert_eq!(cfg.consumer_count(), u16::MAX);
    }

    #[test]
    fn config_builder_chain_consumer_count_accessible() {
        let cfg = RedisConsumerGroupConfig::new(8);
        // Verify `consumer_count()` returns the configured value.
        assert_eq!(cfg.consumer_count(), 8);
        // Clone should preserve the value.
        let cloned = cfg.clone();
        assert_eq!(cloned.consumer_count(), 8);
    }
}
