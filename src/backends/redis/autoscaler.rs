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
// RedisQueueStatsProvider
// ---------------------------------------------------------------------------

/// Reads queue depth from Redis using XLEN and XPENDING.
#[derive(Clone)]
pub struct RedisQueueStatsProvider {
    client: RedisClient,
}

impl RedisQueueStatsProvider {
    /// Create a new stats provider backed by the given [`RedisClient`].
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }

    /// Fetch current stats for `queue` using XLEN (total entries) and
    /// XPENDING (PEL / in-flight count).
    pub async fn get_queue_stats(&self, queue: &str) -> Result<RedisQueueStats> {
        let mut conn = self.client.multiplexed_conn().await?;
        let group = self.client.group().to_owned();

        // XLEN {queue} — total entries in the stream.
        let stream_len: u64 = conn
            .query(redis::cmd("XLEN").arg(queue))
            .await
            .map_err(|e| ShoveError::Connection(format!("XLEN failed: {e}")))?;

        // XPENDING {queue} {group} - + 1 — returns summary: [count, min-id, max-id, consumers].
        // We use the summary form (no IDLE / consumer filter) to get the total PEL size.
        let pending_reply: redis::Value = conn
            .query(
                redis::cmd("XPENDING")
                    .arg(queue)
                    .arg(&group),
            )
            .await
            .unwrap_or(redis::Value::Nil);

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

impl QueueStatsProviderImpl for RedisQueueStatsProvider {
    async fn snapshot(&self, queue: &str) -> Result<AutoscaleMetrics> {
        let stats = self.get_queue_stats(queue).await?;
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
}
