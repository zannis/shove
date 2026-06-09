//! Redis Streams autoscaler — XINFO GROUPS (lag + pending) stats and the
//! `AutoscalerBackend` impl that drives `list_groups`/`fetch_metrics`/`scale`.

use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::autoscaler::{
    Autoscaler, AutoscalerBackend, AutoscalerConfig, ScalingDecision, ScalingMetrics, Stabilized,
    ThresholdStrategy,
};
use crate::backend::{AutoscalerBackendImpl, QueueStatsProviderImpl};
use crate::error::{Result, ShoveError};

use super::client::RedisClient;
use super::consumer_group::RedisConsumerGroupRegistry;

// ---------------------------------------------------------------------------
// RedisQueueStats
// ---------------------------------------------------------------------------

/// Point-in-time statistics for a single Redis Stream.
#[derive(Debug, Clone, Default)]
pub struct RedisQueueStats {
    /// Messages waiting to be delivered (stream length minus in-flight).
    pub messages_ready: u64,
    /// Messages currently held in the PEL (delivered but not yet acked).
    pub messages_in_flight: u64,
}

// ---------------------------------------------------------------------------
// RedisQueueStatsProvider trait
// ---------------------------------------------------------------------------

/// Abstraction over queue-depth lookup. Mirrors the
/// `NatsQueueStatsProvider` / `KafkaQueueStatsProvider` pattern so tests
/// (and downstream users) can swap a mock implementation into
/// [`RedisAutoscalerBackend`].
pub trait RedisQueueStatsProvider: Send + Sync {
    fn get_queue_stats(
        &self,
        queue: &str,
    ) -> impl std::future::Future<Output = Result<RedisQueueStats>> + Send;
}

// ---------------------------------------------------------------------------
// XlenStatsProvider — default impl backed by XINFO GROUPS
// ---------------------------------------------------------------------------

/// Reads queue depth from Redis via `XINFO GROUPS`: the group's `lag`
/// (undelivered entries) as ready backlog and its `pending` (PEL size) as
/// in-flight. Falls back to an XLEN-based estimate only when `lag` is
/// unavailable (Redis < 7.0, or indeterminate after entry deletion).
///
/// The name is historical — the provider originally computed
/// `XLEN - pending`, which counts acked-but-untrimmed history as backlog.
#[derive(Clone)]
pub struct XlenStatsProvider {
    client: RedisClient,
}

impl XlenStatsProvider {
    /// Create a new stats provider backed by the given [`RedisClient`].
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

impl RedisQueueStatsProvider for XlenStatsProvider {
    async fn get_queue_stats(&self, queue: &str) -> Result<RedisQueueStats> {
        use redis::streams::StreamInfoGroupsReply;

        let group = self.client.group();
        let mut conn = self.client.multiplexed_conn().await?;

        // XACK removes an entry from the PEL but not from the stream, so
        // `XLEN - pending` counts already-processed history as backlog and a
        // long-lived queue reads as permanently backlogged. `XINFO GROUPS`
        // exposes both the group's PEL size (`pending`) and its undelivered
        // count (`lag`) in one round trip.
        let info: StreamInfoGroupsReply = conn
            .query(redis::cmd("XINFO").arg("GROUPS").arg(queue))
            .await
            .map_err(|e| ShoveError::Connection(format!("XINFO GROUPS failed: {e}")))?;

        let Some(g) = info.groups.iter().find(|g| g.name == group) else {
            // Group not declared yet — nothing has been delivered, so the
            // whole stream is undelivered backlog.
            let len: u64 = conn
                .query(redis::cmd("XLEN").arg(queue))
                .await
                .map_err(|e| ShoveError::Connection(format!("XLEN failed: {e}")))?;
            return Ok(RedisQueueStats {
                messages_ready: len,
                messages_in_flight: 0,
            });
        };

        let in_flight = g.pending as u64;
        let messages_ready = match g.lag {
            Some(lag) => lag as u64,
            // `lag` is absent on Redis < 7.0 and nil when entry deletion
            // made it indeterminate. Degrade to the XLEN-based estimate —
            // an upper bound that over-counts acked-but-untrimmed history.
            None => {
                let len: u64 = conn
                    .query(redis::cmd("XLEN").arg(queue))
                    .await
                    .map_err(|e| ShoveError::Connection(format!("XLEN failed: {e}")))?;
                tracing::debug!(
                    queue,
                    group,
                    "XINFO GROUPS lag unavailable; falling back to XLEN-based backlog estimate"
                );
                len.saturating_sub(in_flight)
            }
        };

        Ok(RedisQueueStats {
            messages_ready,
            messages_in_flight: in_flight,
        })
    }
}

// ---------------------------------------------------------------------------
// RedisAutoscalerBackend
// ---------------------------------------------------------------------------

/// Autoscaler backend for Redis Streams.
///
/// Generic over the queue-stats provider so tests can inject a mock.
pub struct RedisAutoscalerBackend<S: RedisQueueStatsProvider = XlenStatsProvider> {
    stats_provider: S,
    registry: Arc<Mutex<RedisConsumerGroupRegistry>>,
}

impl RedisAutoscalerBackend<XlenStatsProvider> {
    /// Create a backend that reads queue depth from Redis via XINFO GROUPS.
    pub fn new(client: RedisClient, registry: Arc<Mutex<RedisConsumerGroupRegistry>>) -> Self {
        Self {
            stats_provider: XlenStatsProvider::new(client),
            registry,
        }
    }

    /// Convenience constructor that wires up a fully-configured autoscaler
    /// with [`Stabilized<ThresholdStrategy>`] from a single
    /// [`AutoscalerConfig`].
    pub fn autoscaler(
        client: RedisClient,
        registry: Arc<Mutex<RedisConsumerGroupRegistry>>,
        config: AutoscalerConfig,
    ) -> Autoscaler<Self, Stabilized<ThresholdStrategy>> {
        let strategy = Stabilized::new(
            ThresholdStrategy {
                scale_up_multiplier: config.scale_up_multiplier,
                scale_down_multiplier: config.scale_down_multiplier,
            },
            config.hysteresis_duration,
            config.cooldown_duration,
        );
        let backend = Self::new(client, registry);
        Autoscaler::new(backend, strategy, config.poll_interval)
    }
}

impl<S: RedisQueueStatsProvider> RedisAutoscalerBackend<S> {
    /// Create a backend with an explicit stats provider (useful for testing).
    pub fn with_stats_provider(
        stats_provider: S,
        registry: Arc<Mutex<RedisConsumerGroupRegistry>>,
    ) -> Self {
        Self {
            stats_provider,
            registry,
        }
    }
}

impl<S: RedisQueueStatsProvider> AutoscalerBackendImpl for RedisAutoscalerBackend<S> {}

impl<S: RedisQueueStatsProvider> AutoscalerBackend for RedisAutoscalerBackend<S> {
    type GroupId = String;

    async fn list_groups(&self) -> Result<Vec<Self::GroupId>> {
        let reg = self.registry.lock().await;
        Ok(reg.groups().keys().cloned().collect())
    }

    async fn fetch_metrics(&self, group: &Self::GroupId) -> Result<ScalingMetrics> {
        let (queue, prefetch, active) = {
            let reg = self.registry.lock().await;
            let g = reg
                .groups()
                .get(group)
                .ok_or_else(|| ShoveError::Topology(format!("group not found: {group}")))?;
            (
                g.queue().to_owned(),
                g.config().prefetch_count(),
                g.active_consumers(),
            )
        };

        let stats = self.stats_provider.get_queue_stats(&queue).await?;

        debug!(
            group = %group,
            queue = %queue,
            messages_ready = stats.messages_ready,
            messages_in_flight = stats.messages_in_flight,
            active_consumers = active,
            "fetched Redis metrics"
        );

        Ok(ScalingMetrics::new(
            stats.messages_ready,
            stats.messages_in_flight,
            active as u16,
            prefetch,
        ))
    }

    async fn scale(&self, group: &Self::GroupId, decision: ScalingDecision) -> Result<()> {
        let mut reg = self.registry.lock().await;
        let g = reg
            .groups_mut()
            .get_mut(group)
            .ok_or_else(|| ShoveError::Connection(format!("group not found: {group}")))?;

        match decision {
            ScalingDecision::ScaleUp(n) => {
                for _ in 0..n {
                    if !g.scale_up() {
                        warn!(group = %group, "scale-up requested but already at max consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "Redis scaled up");
            }
            ScalingDecision::ScaleDown(n) => {
                for _ in 0..n {
                    if !g.scale_down() {
                        debug!(group = %group, "scale-down requested but already at min consumers");
                        break;
                    }
                }
                info!(group = %group, consumers = g.active_consumers(), "Redis scaled down");
            }
            ScalingDecision::Hold => {}
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// QueueStatsProviderImpl
// ---------------------------------------------------------------------------

impl QueueStatsProviderImpl for XlenStatsProvider {
    async fn snapshot(&self, queue: &str) -> Result<AutoscaleMetrics> {
        let stats = <Self as RedisQueueStatsProvider>::get_queue_stats(self, queue).await?;
        Ok(AutoscaleMetrics {
            backlog: Some(stats.messages_ready),
            inflight: Some(stats.messages_in_flight),
            throughput_per_sec: None,
            processing_latency: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_stats_populates_messages_ready() {
        let stats = RedisQueueStats {
            messages_ready: 42,
            messages_in_flight: 3,
        };
        assert_eq!(stats.messages_ready, 42);
        assert_eq!(stats.messages_in_flight, 3);
    }

    #[test]
    fn queue_stats_default_is_zero() {
        let stats = RedisQueueStats::default();
        assert_eq!(stats.messages_ready, 0);
        assert_eq!(stats.messages_in_flight, 0);
    }

    #[test]
    fn saturating_sub_prevents_underflow() {
        // If in_flight > stream_len (shouldn't happen in practice), ready clamps to 0.
        let in_flight: u64 = 10;
        let stream_len: u64 = 5;
        let ready = stream_len.saturating_sub(in_flight);
        assert_eq!(ready, 0);
    }

    /// Helper that mirrors the XPENDING-reply parsing inside `get_queue_stats`.
    fn parse_xpending_in_flight(reply: &redis::Value) -> u64 {
        match reply {
            redis::Value::Array(parts) => {
                if let Some(redis::Value::Int(n)) = parts.first() {
                    *n as u64
                } else {
                    0
                }
            }
            _ => 0,
        }
    }

    #[test]
    fn xpending_reply_extracts_in_flight_count() {
        // XPENDING summary reply: [<count>, <min-id>, <max-id>, [...consumers]]
        let reply = redis::Value::Array(vec![
            redis::Value::Int(7),
            redis::Value::BulkString(b"1-0".to_vec()),
            redis::Value::BulkString(b"99-0".to_vec()),
        ]);
        assert_eq!(parse_xpending_in_flight(&reply), 7);
    }

    #[test]
    fn xpending_empty_array_returns_zero() {
        let reply = redis::Value::Array(vec![]);
        assert_eq!(parse_xpending_in_flight(&reply), 0);
    }

    #[test]
    fn xpending_non_int_first_element_returns_zero() {
        let reply = redis::Value::Array(vec![redis::Value::BulkString(b"unexpected".to_vec())]);
        assert_eq!(parse_xpending_in_flight(&reply), 0);
    }

    #[test]
    fn xpending_nil_reply_returns_zero() {
        assert_eq!(parse_xpending_in_flight(&redis::Value::Nil), 0);
    }

    #[test]
    fn messages_ready_is_stream_len_minus_in_flight() {
        let stream_len: u64 = 20;
        let in_flight: u64 = 5;
        let ready = stream_len.saturating_sub(in_flight);
        assert_eq!(ready, 15);
    }

    mod backend {
        use super::*;
        use crate::autoscaler::{AutoscalerBackend, ScalingDecision};
        use crate::backend::ConsumerOptionsInner;
        use crate::backends::redis::consumer_group::{
            ReaperFactory, RedisConsumerGroup, RedisConsumerGroupConfig,
            RedisConsumerGroupRegistry, Spawner,
        };
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use tokio::sync::Mutex;
        use tokio_util::sync::CancellationToken;

        struct MockStats {
            stats: HashMap<String, RedisQueueStats>,
        }

        impl RedisQueueStatsProvider for MockStats {
            async fn get_queue_stats(&self, queue: &str) -> Result<RedisQueueStats> {
                self.stats
                    .get(queue)
                    .cloned()
                    .ok_or_else(|| ShoveError::Topology(format!("not found: {queue}")))
            }
        }

        fn test_group_with_spawner(
            queue: &str,
            config: RedisConsumerGroupConfig,
            started: bool,
        ) -> RedisConsumerGroup {
            let group_token = CancellationToken::new();
            let spawner: Spawner = Arc::new(|options: ConsumerOptionsInner| {
                tokio::spawn(async move {
                    options.shutdown.cancelled().await;
                })
            });
            let reaper_factory: ReaperFactory = Arc::new(|| tokio::spawn(async {}));
            let mut g = RedisConsumerGroup {
                queue: queue.into(),
                consumers: Vec::with_capacity(config.max_consumers() as usize),
                config,
                spawner,
                group_token,
                error_count: Arc::new(AtomicUsize::new(0)),
                panic_count: Arc::new(AtomicUsize::new(0)),
                reaper_factory,
                reaper_handle: None,
            };
            if started {
                g.start();
            }
            g
        }

        fn make_registry(started: bool) -> Arc<Mutex<RedisConsumerGroupRegistry>> {
            let group = test_group_with_spawner(
                "test-queue",
                RedisConsumerGroupConfig::new(1..=4).with_prefetch_count(10),
                started,
            );
            let mut groups = HashMap::new();
            groups.insert("test-queue".to_string(), group);
            Arc::new(Mutex::new(RedisConsumerGroupRegistry::from_groups(groups)))
        }

        #[tokio::test]
        async fn list_groups_returns_registered() {
            let backend = RedisAutoscalerBackend::with_stats_provider(
                MockStats {
                    stats: HashMap::new(),
                },
                make_registry(false),
            );
            let groups = backend.list_groups().await.expect("list_groups");
            assert_eq!(groups, vec!["test-queue".to_string()]);
        }

        #[tokio::test]
        async fn fetch_metrics_uses_stats_provider() {
            let mut stats = HashMap::new();
            stats.insert(
                "test-queue".into(),
                RedisQueueStats {
                    messages_ready: 42,
                    messages_in_flight: 7,
                },
            );
            let backend = RedisAutoscalerBackend::with_stats_provider(
                MockStats { stats },
                make_registry(true),
            );
            let m = backend
                .fetch_metrics(&"test-queue".to_string())
                .await
                .expect("fetch_metrics");
            assert_eq!(m.messages_ready, 42);
            assert_eq!(m.messages_in_flight, 7);
            assert_eq!(m.active_consumers, 1);
            assert_eq!(m.prefetch_count, 10);
        }

        #[tokio::test]
        async fn scale_up_invokes_group_scale_up() {
            let registry = make_registry(true);
            let backend = RedisAutoscalerBackend::with_stats_provider(
                MockStats {
                    stats: HashMap::new(),
                },
                registry.clone(),
            );
            backend
                .scale(&"test-queue".to_string(), ScalingDecision::ScaleUp(1))
                .await
                .expect("scale up");
            let count = registry
                .lock()
                .await
                .groups()
                .get("test-queue")
                .unwrap()
                .active_consumers();
            assert_eq!(count, 2);
        }

        #[tokio::test]
        async fn scale_down_invokes_group_scale_down() {
            let registry = make_registry(true);
            let backend = RedisAutoscalerBackend::with_stats_provider(
                MockStats {
                    stats: HashMap::new(),
                },
                registry.clone(),
            );
            backend
                .scale(&"test-queue".to_string(), ScalingDecision::ScaleUp(2))
                .await
                .expect("scale up");
            backend
                .scale(&"test-queue".to_string(), ScalingDecision::ScaleDown(1))
                .await
                .expect("scale down");
            let count = registry
                .lock()
                .await
                .groups()
                .get("test-queue")
                .unwrap()
                .active_consumers();
            assert_eq!(count, 2);
        }
    }
}
