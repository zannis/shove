//! Redis Streams topology declaration.
//!
//! Creates all necessary Redis structures (streams, consumer groups, sorted sets)
//! for a [`QueueTopology`]: main or shard streams with consumer groups,
//! optional DLQ stream, and hold queue keys.

use crate::error::{Result, ShoveError};
use crate::topology::QueueTopology;

use super::client::RedisClient;

// ---------------------------------------------------------------------------
// Stream naming helpers
// ---------------------------------------------------------------------------

/// Shard stream name for a sequenced topic at the given shard index.
/// Format: `{main_queue}-seq-{shard_index}`
pub fn shard_stream_name(main_queue: &str, shard: u16) -> String {
    format!("{main_queue}-seq-{shard}")
}

/// Hold queue sorted set key for a hold queue.
/// Format: `{hold_queue_name}:pending`
pub fn hold_set_name(hold_queue_name: &str) -> String {
    format!("{hold_queue_name}:pending")
}

// ---------------------------------------------------------------------------
// RedisTopologyDeclarer
// ---------------------------------------------------------------------------

/// Declares all Redis structures needed for a [`QueueTopology`].
///
/// Idempotent: existing streams/groups are left unchanged (BUSYGROUP errors
/// from duplicate XGROUP CREATE are treated as success).
pub struct RedisTopologyDeclarer {
    client: RedisClient,
}

impl RedisTopologyDeclarer {
    /// Create a new topology declarer with the given Redis client.
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }

    /// Shard stream name for a sequenced topic at the given shard index.
    /// Format: `{main_queue}-seq-{shard_index}`
    pub fn shard_stream_name(main_queue: &str, shard: u16) -> String {
        shard_stream_name(main_queue, shard)
    }

    /// Hold queue sorted set key for a hold queue.
    /// Format: `{hold_queue_name}:pending`
    pub fn hold_set_name(hold_queue_name: &str) -> String {
        hold_set_name(hold_queue_name)
    }

    /// Declare all Redis structures for the given topology.
    ///
    /// # Steps
    ///
    /// 1. If sequencing is enabled: create shard streams with consumer groups
    ///    for each shard (0 to `routing_shards - 1`)
    /// 2. Else: create the main stream with a consumer group
    /// 3. If DLQ is present: create the DLQ stream with a consumer group
    /// 4. Hold queues: no-op (Redis creates ZSET keys on first write)
    ///
    /// # Errors
    ///
    /// Returns `ShoveError::Topology` if stream or group creation fails
    /// (other than BUSYGROUP, which is idempotent).
    pub async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        // Create main or shard streams with consumer groups.
        if let Some(seq) = topology.sequencing() {
            // Sequenced: create shard streams.
            for shard_idx in 0..seq.routing_shards() {
                let stream_name = Self::shard_stream_name(topology.queue(), shard_idx);
                self.ensure_stream_and_group(&stream_name).await?;
            }
        } else {
            // Non-sequenced: create main stream.
            self.ensure_stream_and_group(topology.queue()).await?;
        }

        // Create DLQ stream if present.
        if let Some(dlq) = topology.dlq() {
            self.ensure_stream_and_group(dlq).await?;
        }

        // Hold queues: no-op (created on first ZADD by requeuer).
        // Just verify the naming helpers work if called.
        for _ in topology.hold_queues() {
            // Names are constructed by requeuer; no Redis operations here.
        }

        Ok(())
    }

    /// Idempotently create a stream and consumer group.
    ///
    /// Runs `XGROUP CREATE {stream} {group} $ MKSTREAM`.
    /// If the group already exists (BUSYGROUP), treats it as success.
    async fn ensure_stream_and_group(&self, stream: &str) -> Result<()> {
        let mut conn = self.client.multiplexed_conn().await?;
        let group = self.client.group();

        // Build: XGROUP CREATE {stream} {group} $ MKSTREAM
        let mut cmd = redis::cmd("XGROUP");
        cmd.arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM");

        // Execute and handle BUSYGROUP as OK.
        match conn.query::<()>(&mut cmd).await {
            Ok(_) => Ok(()),
            Err(ShoveError::Connection(e)) => {
                // Check if it's a BUSYGROUP error (idempotent case).
                if e.contains("BUSYGROUP") {
                    Ok(())
                } else {
                    Err(ShoveError::Topology(format!(
                        "failed to create stream/group {}/{}: {}",
                        stream, group, e
                    )))
                }
            }
            Err(other) => Err(other),
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
    fn hold_set_name_format() {
        assert_eq!(
            RedisTopologyDeclarer::hold_set_name("orders-hold-5s"),
            "orders-hold-5s:pending"
        );
    }

    #[test]
    fn shard_stream_name_format() {
        assert_eq!(
            RedisTopologyDeclarer::shard_stream_name("ledger", 3),
            "ledger-seq-3"
        );
    }

    #[test]
    fn shard_stream_name_zero() {
        assert_eq!(
            RedisTopologyDeclarer::shard_stream_name("ledger", 0),
            "ledger-seq-0"
        );
    }
}
