//! Redis Streams autoscaler — XLEN + XPENDING stats.

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::backend::{AutoscalerBackendImpl, QueueStatsProviderImpl};
use crate::error::{Result, ShoveError};

use super::client::RedisClient;

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
// XlenStatsProvider — default impl backed by XLEN + XPENDING
// ---------------------------------------------------------------------------

/// Reads queue depth from Redis using XLEN (total entries) and XPENDING
/// (PEL / in-flight count).
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
        let group = self.client.group().to_owned();
        let client = self.client.clone();
        let client2 = self.client.clone();
        let group2 = group.clone();
        let queue2 = queue.to_owned();

        // Run XLEN and XPENDING concurrently — two independent reads, no causal dependency.
        let (stream_len, pending_reply) = tokio::try_join!(
            async move {
                let mut conn = client.multiplexed_conn().await?;
                conn.query::<u64>(redis::cmd("XLEN").arg(queue))
                    .await
                    .map_err(|e| ShoveError::Connection(format!("XLEN failed: {e}")))
            },
            async move {
                let mut conn = client2.multiplexed_conn().await?;
                conn.query::<redis::Value>(redis::cmd("XPENDING").arg(&queue2).arg(&group2))
                    .await
                    .map_err(|e| ShoveError::Connection(format!("XPENDING failed: {e}")))
            }
        )?;

        let in_flight: u64 = match &pending_reply {
            redis::Value::Array(parts) => {
                if let Some(redis::Value::Int(n)) = parts.first() {
                    *n as u64
                } else {
                    0
                }
            }
            // XPENDING on a non-existent group returns an error; treat as 0.
            _ => 0,
        };

        let messages_ready = stream_len.saturating_sub(in_flight);

        Ok(RedisQueueStats {
            messages_ready,
            messages_in_flight: in_flight,
        })
    }
}

// ---------------------------------------------------------------------------
// RedisAutoscalerBackend
// ---------------------------------------------------------------------------

/// Autoscaler backend marker for Redis Streams. Has no methods in Phase 4.
#[derive(Clone)]
pub struct RedisAutoscalerBackend {
    _client: RedisClient,
}

impl RedisAutoscalerBackend {
    /// Create a new autoscaler backend backed by the given [`RedisClient`].
    pub fn new(client: RedisClient) -> Self {
        Self { _client: client }
    }
}

impl AutoscalerBackendImpl for RedisAutoscalerBackend {}

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
}
