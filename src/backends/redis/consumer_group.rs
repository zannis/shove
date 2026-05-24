//! Redis Streams consumer-group registry.
//!
//! [`RedisConsumerGroupRegistry`] owns a collection of lazily-constructed task
//! factories (closures returning boxed futures). Calling [`start_all`] spawns
//! every registered consumer into a [`JoinSet`], and [`run_until_timeout`]
//! drives the set to completion or a configurable drain deadline.

use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner;
use crate::consumer::{HandlerTimeoutConfig, resolve_handler_timeout};
use crate::consumer_supervisor::{SupervisorOutcome, tally_join_result};
use crate::error::{Result, ShoveError};
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
/// The consumer count range matches the other coordinated-group backends.
/// The registry spawns `min_consumers` consumer tasks at start; the
/// `max_consumers` ceiling will be honoured by a future autoscaler (Redis
/// has no scaling loop wired today, so `min_consumers` is the effective
/// in-flight count). Users who want a fixed `N` consumers pass
/// `N..=N`. FIFO topics always spawn one task per shard regardless of
/// this setting.
#[derive(Debug, Clone)]
pub struct RedisConsumerGroupConfig {
    prefetch_count: u16,
    min_consumers: u16,
    max_consumers: u16,
    concurrent_processing: bool,
    pub(crate) handler_timeout: HandlerTimeoutConfig,
}

impl RedisConsumerGroupConfig {
    /// Create a new config with the given consumer-count range.
    ///
    /// `range` sets `min_consumers..=max_consumers`. Both ends are clamped to
    /// at least 1.
    ///
    /// # Panics
    ///
    /// Panics if `*range.start() > *range.end()`.
    pub fn new(range: RangeInclusive<u16>) -> Self {
        let min = (*range.start()).max(1);
        let max = (*range.end()).max(1);
        assert!(
            min <= max,
            "min_consumers ({min}) must be <= max_consumers ({max})"
        );
        Self {
            prefetch_count: 10,
            min_consumers: min,
            max_consumers: max,
            concurrent_processing: false,
            handler_timeout: HandlerTimeoutConfig::Inherit,
        }
    }

    /// Set the prefetch count (the `COUNT` argument to XREADGROUP). With
    /// `concurrent_processing(true)` this is the upper bound on in-flight
    /// handlers per consumer task; with `concurrent_processing(false)` the
    /// effective prefetch is clamped to 1 so handlers serialize regardless.
    pub fn with_prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count = prefetch_count;
        self
    }

    /// Enable concurrent message processing within each consumer.
    ///
    /// When enabled, each consumer processes up to `prefetch_count` messages
    /// concurrently. Handlers are dispatched as independent tokio tasks and
    /// each routes its own outcome (XACK / hold-queue / DLQ).
    ///
    /// Not available for sequenced topics — [`register_fifo`] rejects configs
    /// with this flag set to preserve FIFO ordering within a shard.
    ///
    /// [`register_fifo`]: RedisConsumerGroupRegistry::register_fifo
    pub fn with_concurrent_processing(mut self, concurrent: bool) -> Self {
        self.concurrent_processing = concurrent;
        self
    }

    /// Set the maximum time a handler may spend processing a single
    /// message. If exceeded, the message is left in the PEL for XAUTOCLAIM.
    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "handler_timeout must be positive");
        self.handler_timeout = HandlerTimeoutConfig::Set(timeout);
        self
    }

    /// Returns the configured prefetch count.
    pub fn prefetch_count(&self) -> u16 {
        self.prefetch_count
    }

    /// Returns the minimum number of consumers.
    pub fn min_consumers(&self) -> u16 {
        self.min_consumers
    }

    /// Returns the maximum number of consumers. Currently used only as the
    /// upper bound for a future autoscaler — the registry spawns
    /// `min_consumers` tasks at start.
    pub fn max_consumers(&self) -> u16 {
        self.max_consumers
    }

    /// Returns whether concurrent processing is enabled.
    pub fn concurrent_processing(&self) -> bool {
        self.concurrent_processing
    }

    /// Returns the configured handler timeout. A freshly-constructed
    /// config reports `Some(DEFAULT_HANDLER_TIMEOUT)`; a registry-level
    /// default is not reflected here because the config does not know
    /// about its registry.
    pub fn handler_timeout(&self) -> Option<Duration> {
        Some(resolve_handler_timeout(self.handler_timeout, None))
    }
}

impl Default for RedisConsumerGroupConfig {
    fn default() -> Self {
        Self::new(1..=4)
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
    pub(super) default_handler_timeout: Option<Duration>,
}

impl RedisConsumerGroupRegistry {
    /// Create a new registry backed by the given Redis client.
    pub fn new(client: RedisClient) -> Self {
        Self {
            client,
            tasks: Vec::new(),
            shutdown: CancellationToken::new(),
            default_handler_timeout: None,
        }
    }

    /// Set the registry-level default handler timeout. Applies to every
    /// group whose `RedisConsumerGroupConfig` did not explicitly call
    /// `with_handler_timeout`. Per-group explicit settings always win.
    pub fn with_default_handler_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "default_handler_timeout must be positive"
        );
        self.default_handler_timeout = Some(timeout);
        self
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
    /// Spawns `config.min_consumers()` consumer tasks when [`start_all`] is
    /// called. Each task gets its own clone of `ctx` (via `H::Context: Clone`,
    /// already guaranteed by the [`MessageHandler`] trait bound). The
    /// `max_consumers` ceiling will be honoured by a future autoscaler.
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

        let resolved_handler_timeout =
            resolve_handler_timeout(config.handler_timeout, self.default_handler_timeout);

        // Non-concurrent groups clamp prefetch to 1 so each XREADGROUP fetches
        // at most one message, matching the other backends' "concurrent off"
        // semantics. Concurrent groups use the configured value as the
        // in-flight cap (semaphore size).
        let effective_prefetch = if config.concurrent_processing {
            config.prefetch_count.max(1)
        } else {
            1
        };
        let concurrent = config.concurrent_processing;

        let n = config.min_consumers() as usize;

        for _ in 0..n {
            let client = self.client.clone();
            let shutdown = self.shutdown.clone();
            let handler = factory();
            let ctx = ctx.clone();
            let handler_timeout = resolved_handler_timeout;

            let task: TaskFactory = Box::new(move || {
                Box::pin(async move {
                    let consumer = RedisConsumer::new(client);
                    let mut options = ConsumerOptionsInner::defaults_with_shutdown(shutdown);
                    options.handler_timeout = Some(handler_timeout);
                    options.prefetch_count = effective_prefetch;
                    if concurrent {
                        consumer.run_concurrent::<T, H>(handler, ctx, options).await
                    } else {
                        consumer.run::<T, H>(handler, ctx, options).await
                    }
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
    /// Rejects configs with `concurrent_processing(true)` — concurrent
    /// dispatch within a sequenced shard would break FIFO ordering.
    ///
    /// Topology structures are declared before returning.
    pub async fn register_fifo<T, H>(
        &mut self,
        config: RedisConsumerGroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: SequencedTopic + 'static,
        H: MessageHandler<T> + 'static,
    {
        if config.concurrent_processing {
            return Err(ShoveError::Topology(format!(
                "topic '{}' is sequenced; `concurrent_processing` on a FIFO consumer would \
                 break per-key ordering. Drop `with_concurrent_processing(true)` or use \
                 `register` for unsequenced topics.",
                T::topology().queue(),
            )));
        }

        let topology = T::topology();
        let declarer = RedisTopologyDeclarer::new(self.client.clone());
        declarer.declare(topology).await?;

        let resolved_handler_timeout =
            resolve_handler_timeout(config.handler_timeout, self.default_handler_timeout);

        let prefetch = config.prefetch_count.max(1);
        let n = config.min_consumers() as usize;
        for _ in 0..n {
            let client = self.client.clone();
            let shutdown = self.shutdown.clone();
            let handler = factory();
            let ctx = ctx.clone();
            let handler_timeout = resolved_handler_timeout;

            let task: TaskFactory = Box::new(move || {
                Box::pin(async move {
                    let consumer = RedisConsumer::new(client);
                    let mut options = ConsumerOptionsInner::defaults_with_shutdown(shutdown);
                    options.handler_timeout = Some(handler_timeout);
                    options.prefetch_count = prefetch;
                    consumer.run_fifo::<T, H>(handler, ctx, options).await
                })
            });
            self.tasks.push(task);
        }
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
    use crate::consumer::DEFAULT_HANDLER_TIMEOUT;

    #[test]
    fn config_default_handler_timeout_is_library_default() {
        let cfg = RedisConsumerGroupConfig::new(1..=1);
        assert_eq!(cfg.handler_timeout(), Some(DEFAULT_HANDLER_TIMEOUT));
    }

    #[test]
    fn with_handler_timeout_round_trips() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::from_secs(7));
        assert_eq!(cfg.handler_timeout(), Some(Duration::from_secs(7)));
    }

    #[test]
    fn config_inherit_resolves_to_registry_default_when_set() {
        let cfg = RedisConsumerGroupConfig::new(1..=1);
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(45),
        );
    }

    #[test]
    fn with_handler_timeout_beats_registry_default() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::from_secs(5));
        assert_eq!(
            resolve_handler_timeout(cfg.handler_timeout, Some(Duration::from_secs(45))),
            Duration::from_secs(5),
        );
    }

    #[test]
    #[should_panic(expected = "handler_timeout must be positive")]
    fn with_handler_timeout_zero_panics() {
        let _ = RedisConsumerGroupConfig::new(1..=1).with_handler_timeout(Duration::ZERO);
    }

    #[test]
    fn config_consumer_range() {
        let cfg = RedisConsumerGroupConfig::new(2..=4);
        assert_eq!(cfg.min_consumers(), 2);
        assert_eq!(cfg.max_consumers(), 4);
    }

    #[test]
    fn config_default_range_is_one_to_four() {
        let cfg = RedisConsumerGroupConfig::default();
        assert_eq!(cfg.min_consumers(), 1);
        assert_eq!(cfg.max_consumers(), 4);
    }

    #[test]
    fn config_zero_clamped_to_one() {
        let cfg = RedisConsumerGroupConfig::new(0..=0);
        assert_eq!(cfg.min_consumers(), 1);
        assert_eq!(cfg.max_consumers(), 1);
    }

    #[test]
    fn config_large_consumer_count() {
        let cfg = RedisConsumerGroupConfig::new(u16::MAX..=u16::MAX);
        assert_eq!(cfg.max_consumers(), u16::MAX);
    }

    #[test]
    fn config_default_prefetch_count_is_ten() {
        let cfg = RedisConsumerGroupConfig::default();
        assert_eq!(cfg.prefetch_count(), 10);
    }

    #[test]
    fn config_default_concurrent_processing_is_false() {
        let cfg = RedisConsumerGroupConfig::default();
        assert!(!cfg.concurrent_processing());
    }

    #[test]
    fn with_prefetch_count_round_trips() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_prefetch_count(64);
        assert_eq!(cfg.prefetch_count(), 64);
    }

    #[test]
    fn with_concurrent_processing_round_trips() {
        let cfg = RedisConsumerGroupConfig::new(1..=1).with_concurrent_processing(true);
        assert!(cfg.concurrent_processing());
    }

    #[test]
    fn builder_chain_preserves_all_fields() {
        let cfg = RedisConsumerGroupConfig::new(2..=8)
            .with_prefetch_count(32)
            .with_concurrent_processing(true)
            .with_handler_timeout(Duration::from_secs(3));
        assert_eq!(cfg.min_consumers(), 2);
        assert_eq!(cfg.max_consumers(), 8);
        assert_eq!(cfg.prefetch_count(), 32);
        assert!(cfg.concurrent_processing());
        assert_eq!(cfg.handler_timeout(), Some(Duration::from_secs(3)));
    }

    #[test]
    #[should_panic(expected = "min_consumers")]
    #[allow(clippy::reversed_empty_ranges)]
    fn config_min_greater_than_max_panics() {
        // Intentionally inverted: the constructor must assert that min <= max.
        let _ = RedisConsumerGroupConfig::new(5..=2);
    }
}
