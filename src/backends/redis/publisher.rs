//! Redis Streams publisher — serializes messages to JSON and XADDs them to
//! the correct stream, with shove metadata as additional stream entry fields.

use std::collections::HashMap;

use crate::backend::publisher::PublisherImpl;
use crate::error::{Result, ShoveError};
use crate::topic::Topic;

use super::client::RedisClient;
use super::constants::{PAYLOAD_FIELD, X_SEQUENCE_KEY, DEFAULT_ROUTING_SHARDS};
use super::topology::RedisTopologyDeclarer;

// ---------------------------------------------------------------------------
// FNV-1a shard routing
// ---------------------------------------------------------------------------

/// Map a sequence key to a shard index using FNV-1a 32-bit hash mod `routing_shards`.
pub fn shard_for_key(key: &str, routing_shards: u16) -> u16 {
    assert!(routing_shards > 0, "routing_shards must be > 0");
    let mut hash: u32 = 2_166_136_261;
    for byte in key.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    (hash % routing_shards as u32) as u16
}

// ---------------------------------------------------------------------------
// RedisPublisher
// ---------------------------------------------------------------------------

/// Publishes messages to Redis Streams.
///
/// Implements [`PublisherImpl`]. Obtains a multiplexed connection per call
/// (cheap — the underlying connection is shared by the client).
#[derive(Clone)]
pub struct RedisPublisher {
    client: RedisClient,
}

impl RedisPublisher {
    /// Create a new publisher backed by the given [`RedisClient`].
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }

    /// Core XADD helper — resolves the stream name, serializes, and publishes.
    async fn publish_inner<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        let topology = T::topology();
        let payload =
            serde_json::to_string(msg).map_err(ShoveError::Serialization)?;

        // Determine stream name and optional sequence key.
        let (stream, sequence_key) = if let Some(key_fn) = T::SEQUENCE_KEY_FN {
            let seq_key = key_fn(msg);
            let routing_shards = topology
                .sequencing()
                .ok_or_else(|| ShoveError::Validation(
                    "topic has SEQUENCE_KEY_FN but topology.sequencing() is None; declare with sequenced()".into()
                ))?
                .routing_shards();
            let shard_idx = shard_for_key(&seq_key, routing_shards);
            let stream = RedisTopologyDeclarer::shard_stream_name(topology.queue(), shard_idx);
            (stream, Some(seq_key))
        } else {
            (topology.queue().to_owned(), None)
        };

        self.xadd_fields(&stream, &payload, &headers, sequence_key.as_deref())
            .await
    }

    /// Low-level XADD: build and execute the command.
    async fn xadd_fields(
        &self,
        stream: &str,
        payload: &str,
        headers: &HashMap<String, String>,
        sequence_key: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.client.multiplexed_conn().await?;

        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream).arg("*");
        cmd.arg(PAYLOAD_FIELD).arg(payload);
        for (k, v) in headers {
            cmd.arg(k).arg(v);
        }
        if let Some(seq_key) = sequence_key {
            cmd.arg(X_SEQUENCE_KEY).arg(seq_key);
        }

        conn.query::<redis::Value>(&mut cmd)
            .await
            .map(|_| ())
            .map_err(|e| ShoveError::Connection(format!("XADD to {stream} failed: {e}")))
    }

}

// ---------------------------------------------------------------------------
// PublisherImpl
// ---------------------------------------------------------------------------

impl PublisherImpl for RedisPublisher {
    fn publish<T: Topic>(
        &self,
        msg: &T::Message,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        self.publish_inner::<T>(msg, HashMap::new())
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        self.publish_inner::<T>(msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl std::future::Future<Output = (u64, Result<()>)> + Send {
        async move {
            let mut succeeded: u64 = 0;
            for msg in msgs {
                match self.publish_inner::<T>(msg, HashMap::new()).await {
                    Ok(()) => succeeded += 1,
                    Err(e) => return (succeeded, Err(e)),
                }
            }
            (succeeded, Ok(()))
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
    fn shard_for_key_is_stable_and_bounded() {
        assert!(shard_for_key("acct-1", 8) < 8);
        assert_eq!(shard_for_key("acct-1", 8), shard_for_key("acct-1", 8)); // stable
    }

    #[test]
    fn shard_distribution_reasonably_uniform() {
        let shards = 8u16;
        let mut buckets = vec![0u32; shards as usize];
        for i in 0..1000u32 {
            buckets[shard_for_key(&format!("account-{i}"), shards) as usize] += 1;
        }
        let occupied = buckets.iter().filter(|&&c| c > 0).count();
        assert!(occupied >= 6, "poor distribution: {buckets:?}");
    }
}
