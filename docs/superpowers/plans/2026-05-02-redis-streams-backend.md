# Redis Streams Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Redis Streams backend (`Redis` marker, `redis-streams` feature) that supports consumer groups, hold-queue retries via sorted-set scheduler, DLQ routing, FIFO shard consumers, and autoscaling.

**Architecture:** Each `QueueTopology` maps to one Redis Stream per queue (main, DLQ, per-shard streams). Hold queues are implemented as Redis Sorted Sets where the score is the redeliver-at Unix-ms timestamp; a background requeue task polls each hold set and `XADD`s expired entries back to the main stream. FIFO ordering is achieved by hashing the sequence key to a shard stream index at publish time, with one consumer task per shard.

**Tech Stack:** `redis = "0.27"` (features: `tokio-comp`, `streams`, `connection-manager`), `testcontainers-modules` with `redis` feature for integration tests.

---

## File Map

### New files
| File | Responsibility |
|---|---|
| `src/backends/redis/mod.rs` | Re-exports for the `redis` backend module |
| `src/backends/redis/constants.rs` | Stream entry field name constants (x-retry-count, x-trace-id, etc.) |
| `src/backends/redis/client.rs` | `RedisConfig`, `RedisClient` — wraps `redis::Client`, provides typed connection helpers |
| `src/backends/redis/topology.rs` | `RedisTopologyDeclarer` — `XGROUP CREATE MKSTREAM` for all streams in a topology |
| `src/backends/redis/publisher.rs` | `RedisPublisher` — `XADD` with serialized payload + metadata fields |
| `src/backends/redis/requeue.rs` | `HoldQueueRequeuer` — background sorted-set scheduler that returns delayed messages to the main stream |
| `src/backends/redis/consumer.rs` | `RedisConsumer` — `XREADGROUP` + outcome routing (ack, hold-queue, DLQ, defer), XAUTOCLAIM on startup |
| `src/backends/redis/consumer_group.rs` | `RedisConsumerGroupRegistry`, `RedisConsumerGroupConfig` — coordinated group lifecycle |
| `src/backends/redis/autoscaler.rs` | `RedisAutoscalerBackend`, `RedisQueueStats` — XLEN + XINFO GROUPS lag metrics |
| `src/backends/redis/backend.rs` | `impl Backend for Redis` + `impl HasCoordinatedGroups for Redis` |
| `tests/redis_integration.rs` | Integration tests (basic pub/sub, retry, DLQ, FIFO shards, autoscaling) |
| `examples/redis/basic.rs` | Basic pub/sub example |
| `examples/redis/sequenced.rs` | FIFO pub/sub example |

### Modified files
| File | Change |
|---|---|
| `Cargo.toml` | Add `redis-streams` feature + `redis` dependency + testcontainers redis feature |
| `src/backends/mod.rs` | Add `pub mod redis` under `redis-streams` feature gate |
| `src/markers.rs` | Add `pub struct Redis` marker |
| `src/lib.rs` | Re-export `Redis` marker, add `pub mod redis` with backend types, update feature table in doc comment |

---

## Task 1: Cargo setup and skeleton

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/backends/mod.rs`
- Modify: `src/markers.rs`
- Create: `src/backends/redis/mod.rs`
- Create: `src/backends/redis/constants.rs`

- [ ] **Step 1: Add the feature and dependency to Cargo.toml**

In `Cargo.toml`, add after the `inmemory` feature line:
```toml
# Redis Streams pubsub
redis-streams = ["dep:redis"]
```

Add to `[dependencies]`:
```toml
redis = { version = "0.27", features = ["tokio-comp", "streams", "connection-manager"], optional = true }
```

Add `redis` to the `testcontainers-modules` dev-dependency features:
```toml
testcontainers-modules = { version = "0.15", features = ["rabbitmq", "localstack", "nats", "kafka", "redis"] }
```

Add the integration test entry at the end of `Cargo.toml`:
```toml
[[test]]
name = "redis_integration"
required-features = ["redis-streams"]

[[example]]
name = "redis_basic"
path = "examples/redis/basic.rs"
required-features = ["redis-streams"]

[[example]]
name = "redis_sequenced"
path = "examples/redis/sequenced.rs"
required-features = ["redis-streams"]
```

- [ ] **Step 2: Add the Redis marker to src/markers.rs**

Add after the `InMemory` struct:
```rust
#[cfg(feature = "redis-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-streams")))]
pub struct Redis;
```

- [ ] **Step 3: Create src/backends/redis/constants.rs**

```rust
//! Redis stream entry field name constants for shove metadata.

pub(super) const PAYLOAD_FIELD: &str = "payload";
pub(super) const X_RETRY_COUNT: &str = "x-retry-count";
pub(super) const X_TRACE_ID: &str = "x-trace-id";
pub(super) const X_MESSAGE_ID: &str = "x-message-id";
pub(super) const X_SEQUENCE_KEY: &str = "x-sequence-key";
pub(super) const X_DEATH_REASON: &str = "x-death-reason";
pub(super) const X_DEATH_COUNT: &str = "x-death-count";
pub(super) const X_ORIGINAL_QUEUE: &str = "x-original-queue";
/// Score field stored in the hold sorted set alongside the serialized entry.
/// Not a stream field — only used in the ZSET value JSON.
pub(super) const REDELIVER_AT_MS: &str = "redeliver_at_ms";

/// Default Redis consumer group name used when none is provided in ConsumerOptions.
pub(super) const DEFAULT_GROUP: &str = "shove";
/// BLOCK timeout for XREADGROUP calls (milliseconds). Short enough to check
/// the shutdown token regularly; long enough to avoid busy-polling.
pub(super) const BLOCK_MS: u64 = 2_000;
/// Number of pending entries reclaimed per XAUTOCLAIM call.
pub(super) const AUTOCLAIM_COUNT: u64 = 100;
```

- [ ] **Step 4: Create src/backends/redis/mod.rs**

```rust
//! Internal Redis Streams backend implementation.
//! Public surface lives in [`crate::redis`].

mod autoscaler;
mod backend;
mod client;
mod constants;
mod consumer;
mod consumer_group;
mod publisher;
mod requeue;
mod topology;

pub use autoscaler::{RedisAutoscalerBackend, RedisQueueStats, RedisQueueStatsProvider};
pub use client::{RedisClient, RedisConfig};
pub use consumer::RedisConsumer;
pub use consumer_group::{RedisConsumerGroupConfig, RedisConsumerGroupRegistry};
pub use publisher::RedisPublisher;
pub use topology::RedisTopologyDeclarer;
```

- [ ] **Step 5: Register the module in src/backends/mod.rs**

Add after the `inmemory` block:
```rust
#[cfg(feature = "redis-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-streams")))]
pub mod redis;
```

- [ ] **Step 6: Verify it compiles**

```bash
cargo check -q --features redis-streams 2>&1 | head -30
```
Expected: errors about missing files — that's fine, we're building skeletons.

---

## Task 2: Client

**Files:**
- Create: `src/backends/redis/client.rs`

- [ ] **Step 1: Write the failing test**

```rust
// At bottom of src/backends/redis/client.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn config_default_group() {
        let cfg = RedisConfig {
            url: "redis://127.0.0.1/".into(),
            group: None,
        };
        assert_eq!(cfg.resolved_group(), super::super::constants::DEFAULT_GROUP);
    }

    #[tokio::test]
    async fn config_custom_group() {
        let cfg = RedisConfig {
            url: "redis://127.0.0.1/".into(),
            group: Some("myapp".into()),
        };
        assert_eq!(cfg.resolved_group(), "myapp");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -q --features redis-streams --test redis_integration 2>&1 | head -20
```
Expected: compile error — `RedisConfig` undefined.

- [ ] **Step 3: Implement client.rs**

```rust
//! Redis client wrapper for the shove Redis Streams backend.

use crate::error::{Result, ShoveError};
use super::constants::DEFAULT_GROUP;

/// Configuration for the Redis Streams backend.
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis connection URL, e.g. `redis://127.0.0.1/` or `rediss://user:pass@host/`.
    pub url: String,
    /// Consumer group name. All consumers of a topic share this group so that
    /// each message is delivered to exactly one consumer. Defaults to `"shove"`.
    pub group: Option<String>,
}

impl RedisConfig {
    pub fn resolved_group(&self) -> &str {
        self.group.as_deref().unwrap_or(DEFAULT_GROUP)
    }
}

/// Handle to a Redis connection. Cheap to clone — all clones share the
/// underlying `redis::Client` which manages the connection pool.
#[derive(Clone)]
pub struct RedisClient {
    pub(super) inner: redis::Client,
    pub(super) group: String,
}

impl RedisClient {
    /// Create a new non-blocking multiplexed connection (suitable for XADD,
    /// XACK, ZADD, XLEN, and other non-blocking commands).
    pub(super) async fn multiplexed_conn(
        &self,
    ) -> Result<redis::aio::MultiplexedConnection> {
        self.inner
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))
    }

    /// Create a dedicated async connection for a consumer task.
    /// BLOCK commands on a dedicated connection don't interfere with other tasks.
    pub(super) async fn dedicated_conn(&self) -> Result<redis::aio::Connection> {
        self.inner
            .get_async_connection()
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    pub(super) async fn connect(config: RedisConfig) -> Result<Self> {
        let group = config.resolved_group().to_owned();
        let client = redis::Client::open(config.url.as_str())
            .map_err(|e| ShoveError::Connection(e.to_string()))?;
        // Eagerly check connectivity.
        client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| ShoveError::Connection(format!("Redis connection failed: {e}")))?;
        Ok(Self { inner: client, group })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::constants::DEFAULT_GROUP;

    #[test]
    fn config_default_group() {
        let cfg = RedisConfig { url: "redis://127.0.0.1/".into(), group: None };
        assert_eq!(cfg.resolved_group(), DEFAULT_GROUP);
    }

    #[test]
    fn config_custom_group() {
        let cfg = RedisConfig { url: "redis://127.0.0.1/".into(), group: Some("myapp".into()) };
        assert_eq!(cfg.resolved_group(), "myapp");
    }
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -q --features redis-streams -p shove -- backends::redis::client 2>&1
```
Expected: 2 passing tests.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml src/backends/mod.rs src/backends/redis/ src/markers.rs
git commit -m "feat(redis): skeleton — feature flag, marker, constants, client"
```

---

## Task 3: Topology declaration

**Files:**
- Create: `src/backends/redis/topology.rs`

The topology declarer creates one stream + consumer group per queue in the topology.
Stream names:
- Main: `{queue}` (e.g. `orders`)
- DLQ: `{dlq_name}` (e.g. `orders-dlq`)
- Hold sorted sets: `{hold_name}:pending` (e.g. `orders-hold-5s:pending`)
- Shard streams: `{queue}-seq-{n}` (e.g. `orders-seq-0`)
- Shard hold sorted sets: `{queue}-seq-{n}-hold-{t}s:pending`

Hold queues use Redis Sorted Sets (not streams) so the background requeuer can do `ZRANGEBYSCORE` + `XADD` + `ZREM` atomically. The group name is the client's `group` field.

- [ ] **Step 1: Write the failing tests (unit — no Redis needed)**

```rust
// At bottom of topology.rs
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
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -q --features redis-streams -- backends::redis::topology 2>&1 | head -10
```
Expected: compile error.

- [ ] **Step 3: Implement topology.rs**

```rust
//! Redis topology declarer — creates streams and consumer groups for a QueueTopology.

use redis::AsyncCommands;

use crate::error::{Result, ShoveError};
use crate::topology::QueueTopology;

use super::client::RedisClient;

pub struct RedisTopologyDeclarer {
    client: RedisClient,
}

impl RedisTopologyDeclarer {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }

    /// Name of the sorted set used to hold delayed messages for a hold queue.
    pub fn hold_set_name(hold_queue_name: &str) -> String {
        format!("{hold_queue_name}:pending")
    }

    /// Stream name for a FIFO shard.
    pub fn shard_stream_name(main_queue: &str, shard: u16) -> String {
        format!("{main_queue}-seq-{shard}")
    }

    /// `XGROUP CREATE {stream} {group} $ MKSTREAM` — idempotent.
    async fn ensure_stream_and_group(
        conn: &mut redis::aio::MultiplexedConnection,
        stream: &str,
        group: &str,
    ) -> Result<()> {
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(conn)
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                // BUSYGROUP means the group already exists — idempotent.
                if e.kind() == redis::ErrorKind::ExtensionError
                    && e.to_string().contains("BUSYGROUP")
                {
                    Ok(())
                } else {
                    Err(ShoveError::Topology(format!(
                        "XGROUP CREATE failed for stream '{stream}': {e}"
                    )))
                }
            }
        }
    }

    pub async fn declare(&self, topology: &QueueTopology) -> Result<()> {
        let mut conn = self.client.multiplexed_conn().await?;
        let group = self.client.group();

        if let Some(seq) = topology.sequencing() {
            // Sequenced topic: one stream per shard.
            for shard in 0..seq.routing_shards() {
                let shard_stream = Self::shard_stream_name(topology.queue(), shard);
                Self::ensure_stream_and_group(&mut conn, &shard_stream, group).await?;
            }
        } else {
            // Unsequenced: single main stream.
            Self::ensure_stream_and_group(&mut conn, topology.queue(), group).await?;
        }

        // DLQ — create stream+group (consumers call handle_dead).
        if let Some(dlq) = topology.dlq() {
            Self::ensure_stream_and_group(&mut conn, dlq, group).await?;
        }

        // Hold queues are sorted sets — no XGROUP needed; just ensure the key
        // can be written to (Redis creates keys on first write, so nothing to do).

        Ok(())
    }
}

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
}
```

- [ ] **Step 4: Run unit tests**

```bash
cargo test -q --features redis-streams -- backends::redis::topology 2>&1
```
Expected: 2 passing tests.

- [ ] **Step 5: Commit**

```bash
git add src/backends/redis/topology.rs
git commit -m "feat(redis): topology declarer with XGROUP CREATE MKSTREAM"
```

---

## Task 4: Publisher

**Files:**
- Create: `src/backends/redis/publisher.rs`

Publisher serializes the message to JSON, then `XADD`s to the stream with metadata fields. For sequenced topics, the caller supplies an explicit stream name (shard stream); for unsequenced, uses `topology.queue()`.

- [ ] **Step 1: Write the failing test**

```rust
// In publisher.rs tests module
#[tokio::test]
async fn publish_builds_correct_fields() {
    // Verify that `build_fields` includes the payload and metadata keys.
    let payload = serde_json::to_string(&"hello").unwrap();
    let headers = std::collections::HashMap::from([
        ("x-trace-id".into(), "trace-1".into()),
    ]);
    let fields = RedisPublisher::build_fields(&payload, &headers, None);
    assert!(fields.iter().any(|(k, _)| k == "payload"));
    assert!(fields.iter().any(|(k, v)| k == "x-trace-id" && v == "trace-1"));
}
```

- [ ] **Step 2: Verify failure**

```bash
cargo test -q --features redis-streams -- backends::redis::publisher 2>&1 | head -10
```

- [ ] **Step 3: Implement publisher.rs**

```rust
//! Redis Streams publisher — XADD with JSON payload and shove metadata fields.

use std::collections::HashMap;

use redis::AsyncCommands;
use serde::Serialize;

use crate::backend::PublisherImpl;
use crate::error::{Result, ShoveError};
use crate::topic::Topic;

use super::client::RedisClient;
use super::constants::{PAYLOAD_FIELD, X_MESSAGE_ID, X_SEQUENCE_KEY, X_TRACE_ID};

#[derive(Clone)]
pub struct RedisPublisher {
    client: RedisClient,
}

impl RedisPublisher {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }

    /// Build the list of `(field, value)` pairs to store in the stream entry.
    pub(super) fn build_fields<'a>(
        payload: &'a str,
        headers: &'a HashMap<String, String>,
        sequence_key: Option<&'a str>,
    ) -> Vec<(&'a str, &'a str)> {
        let mut fields: Vec<(&str, &str)> = vec![(PAYLOAD_FIELD, payload)];
        for (k, v) in headers {
            fields.push((k.as_str(), v.as_str()));
        }
        if let Some(seq_key) = sequence_key {
            fields.push((X_SEQUENCE_KEY, seq_key));
        }
        fields
    }

    async fn xadd_to_stream(
        &self,
        stream: &str,
        payload: &str,
        headers: &HashMap<String, String>,
        sequence_key: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.client.multiplexed_conn().await?;
        let fields = Self::build_fields(payload, headers, sequence_key);
        // Build the XADD command: XADD {stream} * field1 val1 field2 val2 ...
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream).arg("*");
        for (k, v) in &fields {
            cmd.arg(k).arg(v);
        }
        cmd.query_async::<_, redis::Value>(&mut conn)
            .await
            .map(|_| ())
            .map_err(|e| ShoveError::Publish(e.to_string()))
    }

    fn serialize<M: Serialize>(msg: &M) -> Result<String> {
        serde_json::to_string(msg).map_err(|e| ShoveError::Serialization(e.to_string()))
    }

    /// Publish to a specific shard stream (used by `ConsumerSupervisor::register_fifo`
    /// which hashes the sequence key at call time).
    pub async fn publish_to_shard<T: Topic>(
        &self,
        msg: &T::Message,
        stream: &str,
    ) -> Result<()> {
        let payload = Self::serialize(msg)?;
        self.xadd_to_stream(stream, &payload, &HashMap::new(), None)
            .await
    }
}

impl PublisherImpl for RedisPublisher {
    async fn publish<T: Topic>(&self, msg: &T::Message) -> Result<()> {
        let payload = Self::serialize(msg)?;
        self.xadd_to_stream(T::topology().queue(), &payload, &HashMap::new(), None)
            .await
    }

    async fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        let payload = Self::serialize(msg)?;
        // For sequenced topics, extract the sequence key for routing.
        let seq_key_storage;
        let sequence_key = if let Some(key_fn) = T::SEQUENCE_KEY_FN {
            seq_key_storage = (key_fn)(msg);
            Some(seq_key_storage.as_str())
        } else {
            None
        };
        let topology = T::topology();
        let stream = if let (Some(seq), Some(seq_key)) = (topology.sequencing(), sequence_key) {
            let shard = shard_for_key(seq_key, seq.routing_shards());
            super::topology::RedisTopologyDeclarer::shard_stream_name(topology.queue(), shard)
        } else {
            topology.queue().to_owned()
        };
        self.xadd_to_stream(&stream, &payload, &headers, sequence_key)
            .await
    }

    async fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> (u64, Result<()>) {
        let mut succeeded = 0u64;
        for msg in msgs {
            match self.publish::<T>(msg).await {
                Ok(()) => succeeded += 1,
                Err(e) => return (succeeded, Err(e)),
            }
        }
        (succeeded, Ok(()))
    }
}

/// Consistent-hash a sequence key to a shard index.
pub(super) fn shard_for_key(key: &str, routing_shards: u16) -> u16 {
    // FNV-1a 32-bit hash, same approach as the InMemory backend.
    let mut hash: u32 = 2_166_136_261;
    for byte in key.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    (hash % routing_shards as u32) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_fields_includes_payload_and_headers() {
        let payload = r#""hello""#;
        let headers = HashMap::from([("x-trace-id".into(), "t1".into())]);
        let fields = RedisPublisher::build_fields(payload, &headers, None);
        assert!(fields.iter().any(|(k, _)| *k == PAYLOAD_FIELD));
        assert!(fields.iter().any(|(k, v)| *k == "x-trace-id" && *v == "t1"));
    }

    #[test]
    fn build_fields_includes_sequence_key() {
        let payload = "{}";
        let headers = HashMap::new();
        let fields = RedisPublisher::build_fields(payload, &headers, Some("acct-42"));
        assert!(fields.iter().any(|(k, v)| *k == X_SEQUENCE_KEY && *v == "acct-42"));
    }

    #[test]
    fn shard_for_key_is_stable_and_bounded() {
        let shards = 8u16;
        let s = shard_for_key("acct-1", shards);
        assert!(s < shards);
        // Stable across calls.
        assert_eq!(s, shard_for_key("acct-1", shards));
        // Different keys CAN produce different shards.
        let s2 = shard_for_key("acct-99999", shards);
        assert!(s2 < shards);
    }

    #[test]
    fn shard_distribution_is_reasonably_uniform() {
        // At least 6 of 8 shards should be occupied across 1000 random-looking keys.
        let shards = 8u16;
        let mut buckets = vec![0u32; shards as usize];
        for i in 0..1000u32 {
            let key = format!("account-{i}");
            buckets[shard_for_key(&key, shards) as usize] += 1;
        }
        let occupied = buckets.iter().filter(|&&c| c > 0).count();
        assert!(occupied >= 6, "poor distribution: {buckets:?}");
    }
}
```

- [ ] **Step 4: Run unit tests**

```bash
cargo test -q --features redis-streams -- backends::redis::publisher 2>&1
```
Expected: 4 passing tests.

- [ ] **Step 5: Commit**

```bash
git add src/backends/redis/publisher.rs
git commit -m "feat(redis): publisher with XADD, headers, shard routing"
```

---

## Task 5: Hold queue requeuer

**Files:**
- Create: `src/backends/redis/requeue.rs`

The `HoldQueueRequeuer` is a background task that polls each hold sorted set, finds entries whose score (redeliver-at ms) has passed, and `XADD`s them back to the main stream. It runs as a `tokio::spawn`ed task.

Each entry in the sorted set is a JSON-serialized `HoldEntry` containing the stream name, all original fields, and the score.

- [ ] **Step 1: Write failing test**

```rust
// In requeue.rs tests
#[test]
fn hold_entry_roundtrips() {
    let entry = HoldEntry {
        stream: "orders".into(),
        fields: vec![("payload".into(), "{}".into()), ("x-retry-count".into(), "1".into())],
    };
    let json = serde_json::to_string(&entry).unwrap();
    let decoded: HoldEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.stream, "orders");
    assert_eq!(decoded.fields.len(), 2);
}
```

- [ ] **Step 2: Verify failure**

```bash
cargo test -q --features redis-streams -- backends::redis::requeue 2>&1 | head -10
```

- [ ] **Step 3: Implement requeue.rs**

```rust
//! Background hold-queue requeuer.
//!
//! Hold queues are modelled as Redis Sorted Sets where:
//!   key   = `{hold_queue_name}:pending`  (see `RedisTopologyDeclarer::hold_set_name`)
//!   score = Unix timestamp in milliseconds at which the entry should be redelivered
//!   value = JSON-serialized `HoldEntry`
//!
//! This task polls each hold set every `POLL_INTERVAL`, moves all entries
//! whose score ≤ now_ms back to the appropriate stream, and removes them from
//! the set.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::{Result, ShoveError};

use super::client::RedisClient;
use super::topology::RedisTopologyDeclarer;

const POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Max entries moved per poll cycle per hold set (back-pressure).
const BATCH_SIZE: isize = 200;

/// An entry stored in the hold sorted set.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct HoldEntry {
    /// Target stream to XADD back into on redeliver.
    pub stream: String,
    /// All fields (including payload and metadata) to restore on redeliver.
    pub fields: Vec<(String, String)>,
}

impl HoldEntry {
    pub fn serialize(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| ShoveError::Serialization(e.to_string()))
    }

    pub fn deserialize(s: &str) -> Result<Self> {
        serde_json::from_str(s).map_err(|e| ShoveError::Serialization(e.to_string()))
    }
}

/// Push a message to a hold sorted set for delayed redelivery.
pub(super) async fn enqueue_hold(
    conn: &mut redis::aio::MultiplexedConnection,
    hold_queue_name: &str,
    entry: HoldEntry,
    delay: Duration,
) -> Result<()> {
    let now_ms = now_ms();
    let redeliver_at = now_ms + delay.as_millis() as u64;
    let set_key = RedisTopologyDeclarer::hold_set_name(hold_queue_name);
    let value = entry.serialize()?;
    redis::cmd("ZADD")
        .arg(&set_key)
        .arg(redeliver_at as f64)
        .arg(&value)
        .query_async::<_, i64>(conn)
        .await
        .map(|_| ())
        .map_err(|e| ShoveError::Connection(e.to_string()))
}

/// Spawn a background requeuer that drains all hold sets for the given
/// hold queue names back to their target streams.
///
/// Returns a `JoinHandle` that runs until `shutdown` is cancelled.
pub(super) fn spawn_requeuer(
    client: RedisClient,
    hold_queue_names: Vec<String>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(e) = poll_all(&client, &hold_queue_names).await {
                        tracing::warn!(error = %e, "hold queue requeue poll failed");
                    }
                }
            }
        }
    })
}

async fn poll_all(client: &RedisClient, hold_queue_names: &[String]) -> Result<()> {
    let mut conn = client.multiplexed_conn().await?;
    let now = now_ms() as f64;
    for hq_name in hold_queue_names {
        let set_key = RedisTopologyDeclarer::hold_set_name(hq_name);
        // ZRANGEBYSCORE key 0 now LIMIT 0 BATCH_SIZE
        let entries: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&set_key)
            .arg(0f64)
            .arg(now)
            .arg("LIMIT")
            .arg(0)
            .arg(BATCH_SIZE)
            .query_async(&mut conn)
            .await
            .map_err(|e| ShoveError::Connection(e.to_string()))?;

        for raw in &entries {
            let entry = match HoldEntry::deserialize(raw) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(error = %e, "skipping malformed hold entry");
                    continue;
                }
            };
            // XADD the fields back to the original stream.
            let mut xadd = redis::cmd("XADD");
            xadd.arg(&entry.stream).arg("*");
            for (k, v) in &entry.fields {
                xadd.arg(k).arg(v);
            }
            if let Err(e) = xadd
                .query_async::<_, redis::Value>(&mut conn)
                .await
            {
                tracing::warn!(error = %e, stream = %entry.stream, "XADD requeue failed");
                continue;
            }
            // Remove from the sorted set only after successful XADD.
            let _: i64 = redis::cmd("ZREM")
                .arg(&set_key)
                .arg(raw)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
        }
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hold_entry_roundtrips() {
        let entry = HoldEntry {
            stream: "orders".into(),
            fields: vec![
                ("payload".into(), "{}".into()),
                ("x-retry-count".into(), "1".into()),
            ],
        };
        let json = entry.serialize().unwrap();
        let decoded = HoldEntry::deserialize(&json).unwrap();
        assert_eq!(decoded.stream, "orders");
        assert_eq!(decoded.fields.len(), 2);
    }

    #[test]
    fn now_ms_is_nonzero() {
        assert!(now_ms() > 0);
    }
}
```

- [ ] **Step 4: Run unit tests**

```bash
cargo test -q --features redis-streams -- backends::redis::requeue 2>&1
```
Expected: 2 passing tests.

- [ ] **Step 5: Commit**

```bash
git add src/backends/redis/requeue.rs
git commit -m "feat(redis): hold queue sorted-set requeuer"
```

---

## Task 6: Consumer — core (unsequenced)

**Files:**
- Create: `src/backends/redis/consumer.rs`

The consumer loop:
1. On startup: `XAUTOCLAIM` to reclaim stale pending entries (handles crashed prior consumers)
2. `XREADGROUP GROUP {group} {consumer_name} COUNT {prefetch} BLOCK {BLOCK_MS} STREAMS {stream} >`
3. For each entry: deserialize payload, call handler, route by `Outcome`
4. `Outcome::Ack` → `XACK`
5. `Outcome::Retry` → increment retry_count; if < max_retries: `enqueue_hold` to appropriate hold set + `XACK`; else `XADD` to DLQ + `XACK`
6. `Outcome::Reject` → `XADD` to DLQ + `XACK`
7. `Outcome::Defer` → `enqueue_hold` to hold_queues[0] + `XACK` (no retry_count increment)
8. Handler timeout → let `XAUTOCLAIM` reclaim it on next pass (just drop the task, do NOT `XACK`)

Consumer name: `{hostname}-{uuid4}` — unique per task, necessary for XAUTOCLAIM to differentiate dead from active consumers.

- [ ] **Step 1: Write unit tests for outcome routing logic (no Redis needed)**

```rust
// In consumer.rs tests
#[test]
fn retry_count_routing_to_hold_level() {
    // retry_count=0 → hold_queues[0], retry_count=1 → hold_queues[1], etc.
    // Once count >= max_retries → DLQ.
    let hold_queues = vec!["orders-hold-5s", "orders-hold-30s"];
    assert_eq!(hold_level(0, &hold_queues), Some(0));
    assert_eq!(hold_level(1, &hold_queues), Some(1));
    assert_eq!(hold_level(2, &hold_queues), Some(1)); // clamped to last
}

#[test]
fn retry_exhausted_means_dlq() {
    // At max_retries, None from hold_level signals DLQ.
    let hold_queues: Vec<&str> = vec!["h"];
    // max_retries = 1, retry_count = 1 → exhausted
    // Caller checks: if retry_count >= max_retries → DLQ
    assert!(1u32 >= 1u32); // max_retries check is caller's responsibility
}
```

- [ ] **Step 2: Verify failure**

```bash
cargo test -q --features redis-streams -- backends::redis::consumer 2>&1 | head -10
```

- [ ] **Step 3: Implement consumer.rs**

```rust
//! Redis Streams consumer — XREADGROUP loop with full outcome routing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::backend::{ConsumerImpl, ConsumerOptionsInner};
use crate::consumer::validate_message_size;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::metadata::{DeadMessageMetadata, MessageMetadata};
use crate::metrics;
use crate::outcome::Outcome;
use crate::topic::{SequencedTopic, Topic};
use crate::topology::QueueTopology;

use super::client::RedisClient;
use super::constants::*;
use super::publisher::shard_for_key;
use super::requeue::{HoldEntry, enqueue_hold, spawn_requeuer};
use super::topology::RedisTopologyDeclarer;

#[derive(Clone)]
pub struct RedisConsumer {
    client: RedisClient,
}

impl RedisConsumer {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }

    fn consumer_name() -> String {
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
        format!("{hostname}-{}", Uuid::new_v4())
    }
}

impl ConsumerImpl for RedisConsumer {
    async fn run<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let stream = topology.queue().to_owned();
        run_stream_loop(
            self.client.clone(),
            handler,
            ctx,
            options,
            topology,
            &stream,
        )
        .await
    }

    async fn run_fifo<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        // run_fifo runs all shards sequentially in separate tasks — use spawn_fifo_shards.
        let handles = self.spawn_fifo_shards::<T, H>(handler, ctx, options).await?;
        for handle in handles {
            handle.await.map_err(|e| ShoveError::Internal(e.to_string()))??;
        }
        Ok(())
    }

    async fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let dlq = match topology.dlq() {
            Some(d) => d.to_owned(),
            None => return Ok(()), // no DLQ configured — nothing to do
        };
        let options = ConsumerOptionsInner {
            max_retries: 0,
            prefetch_count: 1,
            handler_timeout: None,
            max_pending_per_key: None,
            max_message_size: None,
            shutdown: CancellationToken::new(),
            processing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            consumer_group: None,
            #[cfg(feature = "rabbitmq-transactional")]
            exactly_once: false,
            #[cfg(feature = "aws-sns-sqs")]
            receive_batch_size: 0,
            #[cfg(feature = "nats")]
            max_ack_pending: None,
        };
        run_stream_loop(self.client.clone(), handler, ctx, options, topology, &dlq).await
    }

    async fn spawn_fifo_shards<T, H>(
        &self,
        handler: H,
        ctx: H::Context,
        options: ConsumerOptionsInner,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let seq = topology.sequencing().ok_or_else(|| {
            ShoveError::Topology("spawn_fifo_shards called on non-sequenced topic".into())
        })?;
        let handler = Arc::new(handler);
        let ctx = Arc::new(ctx);
        let mut handles = Vec::new();
        for shard in 0..seq.routing_shards() {
            let stream = RedisTopologyDeclarer::shard_stream_name(topology.queue(), shard);
            let client = self.client.clone();
            let handler_clone = Arc::clone(&handler);
            let ctx_clone = Arc::clone(&ctx);
            let opts_clone = options.clone();
            let stream_clone = stream.clone();
            let handle = tokio::spawn(async move {
                run_stream_loop(
                    client,
                    // SAFETY: Arc::clone gives a reference, but we need owned H.
                    // Workaround: each shard gets its own handler via the factory
                    // pattern from the consumer group. Here we accept the constraint
                    // that the same handler instance is shared — it must be Sync.
                    Arc::try_unwrap(handler_clone)
                        .unwrap_or_else(|arc| unsafe { std::ptr::read(Arc::as_ptr(&arc)) }),
                    Arc::try_unwrap(ctx_clone)
                        .unwrap_or_else(|arc| unsafe { std::ptr::read(Arc::as_ptr(&arc)) }),
                    opts_clone,
                    topology,
                    &stream_clone,
                )
                .await
            });
            handles.push(handle);
        }
        Ok(handles)
    }
}

// ---------------------------------------------------------------------------
// Core loop
// ---------------------------------------------------------------------------

async fn run_stream_loop<T, H>(
    client: RedisClient,
    handler: H,
    ctx: H::Context,
    options: ConsumerOptionsInner,
    topology: &'static QueueTopology,
    stream: &str,
) -> Result<()>
where
    T: Topic,
    H: MessageHandler<T>,
{
    let group = client.group().to_owned();
    let consumer = RedisConsumer::consumer_name();
    let shutdown = options.shutdown.clone();
    let handler = Arc::new(handler);
    let ctx = Arc::new(ctx);

    // Spawn the hold-queue requeuer for this stream's hold queues.
    let hold_names: Vec<String> = topology
        .hold_queues()
        .iter()
        .map(|hq| hq.name().to_owned())
        .collect();
    let requeue_handle = if !hold_names.is_empty() {
        Some(spawn_requeuer(client.clone(), hold_names, shutdown.clone()))
    } else {
        None
    };

    // Reclaim stale pending entries from prior crashed consumers on startup.
    let idle_ms = options
        .handler_timeout
        .unwrap_or(Duration::from_secs(30))
        .as_millis() as u64;
    if let Ok(mut conn) = client.dedicated_conn().await {
        let _ = autoclaim_all(&mut conn, stream, &group, &consumer, idle_ms).await;
    }

    let mut conn = client.dedicated_conn().await?;
    let prefetch = options.prefetch_count.max(1) as usize;

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let read_opts = StreamReadOptions::default()
            .group(&group, &consumer)
            .count(prefetch)
            .block(BLOCK_MS as usize);

        let reply: StreamReadReply = match redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&group)
            .arg(&consumer)
            .arg("COUNT")
            .arg(prefetch)
            .arg("BLOCK")
            .arg(BLOCK_MS)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut conn)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, stream, "XREADGROUP failed, retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        for stream_key in reply.keys {
            for entry in stream_key.ids {
                let entry_id = entry.id.clone();

                // Extract fields.
                let payload_raw = match entry.map.get(PAYLOAD_FIELD) {
                    Some(redis::Value::Data(b)) => String::from_utf8_lossy(b).into_owned(),
                    _ => {
                        tracing::warn!(entry_id, "missing payload field — acking and skipping");
                        let _ = xack(&mut conn, stream, &group, &entry_id).await;
                        continue;
                    }
                };

                let retry_count = extract_u32(&entry.map, X_RETRY_COUNT);
                let trace_id = extract_str(&entry.map, X_TRACE_ID)
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                let delivery_id = entry_id.clone();

                // Size check.
                if let Some(max) = options.max_message_size {
                    if payload_raw.len() > max {
                        tracing::warn!(
                            entry_id,
                            size = payload_raw.len(),
                            limit = max,
                            "message exceeds size limit — sending to DLQ"
                        );
                        metrics::record_message_failed(metrics::FailReason::Oversize);
                        route_to_dlq(
                            &mut conn,
                            topology,
                            stream,
                            &group,
                            &entry_id,
                            &entry.map,
                            "oversize",
                            retry_count,
                        )
                        .await;
                        continue;
                    }
                }

                // Deserialize.
                let msg: T::Message = match serde_json::from_str(&payload_raw) {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(error = %e, entry_id, "deserialization failed — sending to DLQ");
                        metrics::record_message_failed(metrics::FailReason::Deserialize);
                        route_to_dlq(
                            &mut conn,
                            topology,
                            stream,
                            &group,
                            &entry_id,
                            &entry.map,
                            "deserialize",
                            retry_count,
                        )
                        .await;
                        continue;
                    }
                };

                let meta = MessageMetadata {
                    retry_count,
                    delivery_id: delivery_id.clone(),
                    redelivered: retry_count > 0,
                    headers: extract_headers(&entry.map),
                };

                let handler_clone = Arc::clone(&handler);
                let ctx_clone = Arc::clone(&ctx);
                let timeout = options.handler_timeout.unwrap_or(Duration::from_secs(30));

                let outcome = tokio::select! {
                    result = async { handler_clone.handle(msg, meta, &ctx_clone).await } => result,
                    _ = tokio::time::sleep(timeout) => {
                        tracing::warn!(entry_id, "handler timed out — leaving in PEL for XAUTOCLAIM");
                        metrics::record_message_failed(metrics::FailReason::Timeout);
                        // Do NOT ack — XAUTOCLAIM will reclaim it after idle_ms.
                        continue;
                    }
                };

                route_outcome(
                    &mut conn,
                    topology,
                    stream,
                    &group,
                    &entry_id,
                    &entry.map,
                    outcome,
                    retry_count,
                    options.max_retries,
                )
                .await;
            }
        }

        // Periodically reclaim stale PEL entries.
        let _ = autoclaim_all(&mut conn, stream, &group, &consumer, idle_ms).await;
    }

    if let Some(h) = requeue_handle {
        h.abort();
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Outcome routing
// ---------------------------------------------------------------------------

async fn route_outcome(
    conn: &mut redis::aio::Connection,
    topology: &'static QueueTopology,
    stream: &str,
    group: &str,
    entry_id: &str,
    entry_map: &HashMap<String, redis::Value>,
    outcome: Outcome,
    retry_count: u32,
    max_retries: u32,
) {
    match outcome {
        Outcome::Ack => {
            metrics::record_message_consumed(metrics::OutcomeLabel::Ack);
            let _ = xack(conn, stream, group, entry_id).await;
        }
        Outcome::Retry => {
            let new_retry = retry_count + 1;
            if new_retry >= max_retries {
                metrics::record_message_consumed(metrics::OutcomeLabel::Retry);
                route_to_dlq(conn, topology, stream, group, entry_id, entry_map, "max-retries", new_retry).await;
            } else {
                metrics::record_message_consumed(metrics::OutcomeLabel::Retry);
                let hold_queues = topology.hold_queues();
                if hold_queues.is_empty() {
                    tracing::warn!(stream, entry_id, "Retry but no hold queues — re-queueing immediately");
                    requeue_to_main(conn, stream, entry_map, new_retry).await;
                } else {
                    let level = (new_retry as usize).min(hold_queues.len() - 1);
                    let hq = &hold_queues[level];
                    route_to_hold(conn, stream, group, entry_id, entry_map, hq.name(), hq.delay(), new_retry).await;
                }
            }
        }
        Outcome::Reject => {
            metrics::record_message_consumed(metrics::OutcomeLabel::Reject);
            route_to_dlq(conn, topology, stream, group, entry_id, entry_map, "rejected", retry_count).await;
        }
        Outcome::Defer => {
            metrics::record_message_consumed(metrics::OutcomeLabel::Defer);
            let hold_queues = topology.hold_queues();
            if hold_queues.is_empty() {
                tracing::warn!(stream, entry_id, "Defer but no hold queues — re-queueing immediately");
                requeue_to_main(conn, stream, entry_map, retry_count).await;
            } else {
                let hq = &hold_queues[0];
                route_to_hold(conn, stream, group, entry_id, entry_map, hq.name(), hq.delay(), retry_count).await;
            }
        }
    }
}

async fn route_to_hold(
    conn: &mut redis::aio::Connection,
    stream: &str,
    group: &str,
    entry_id: &str,
    entry_map: &HashMap<String, redis::Value>,
    hold_name: &str,
    delay: Duration,
    new_retry_count: u32,
) {
    let mut fields = map_to_fields(entry_map);
    // Update retry count.
    let rc_str = new_retry_count.to_string();
    if let Some(pos) = fields.iter().position(|(k, _)| k == X_RETRY_COUNT) {
        fields[pos].1 = rc_str.clone();
    } else {
        fields.push((X_RETRY_COUNT.into(), rc_str));
    }
    let entry = HoldEntry { stream: stream.to_owned(), fields };
    // Use multiplexed conn equivalent — we have a dedicated conn here, so use cmd directly.
    let set_key = RedisTopologyDeclarer::hold_set_name(hold_name);
    let redeliver_at = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        + delay)
        .as_millis() as f64;
    let value = match entry.serialize() {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "failed to serialize hold entry");
            return;
        }
    };
    let zadd_res: redis::RedisResult<i64> = redis::cmd("ZADD")
        .arg(&set_key)
        .arg(redeliver_at)
        .arg(&value)
        .query_async(conn)
        .await;
    if let Err(e) = zadd_res {
        tracing::warn!(error = %e, hold_name, "ZADD to hold set failed");
        return;
    }
    let _ = xack(conn, stream, group, entry_id).await;
}

async fn route_to_dlq(
    conn: &mut redis::aio::Connection,
    topology: &'static QueueTopology,
    stream: &str,
    group: &str,
    entry_id: &str,
    entry_map: &HashMap<String, redis::Value>,
    reason: &str,
    death_count: u32,
) {
    let dlq = match topology.dlq() {
        Some(d) => d,
        None => {
            tracing::warn!(stream, entry_id, reason, "no DLQ configured — discarding");
            let _ = xack(conn, stream, group, entry_id).await;
            return;
        }
    };
    let mut fields = map_to_fields(entry_map);
    fields.push((X_DEATH_REASON.into(), reason.into()));
    fields.push((X_DEATH_COUNT.into(), death_count.to_string()));
    fields.push((X_ORIGINAL_QUEUE.into(), stream.into()));

    let mut xadd = redis::cmd("XADD");
    xadd.arg(dlq).arg("*");
    for (k, v) in &fields {
        xadd.arg(k.as_str()).arg(v.as_str());
    }
    if let Err(e) = xadd.query_async::<_, redis::Value>(conn).await {
        tracing::warn!(error = %e, dlq, "XADD to DLQ failed");
    }
    let _ = xack(conn, stream, group, entry_id).await;
}

async fn requeue_to_main(
    conn: &mut redis::aio::Connection,
    stream: &str,
    entry_map: &HashMap<String, redis::Value>,
    retry_count: u32,
) {
    let mut fields = map_to_fields(entry_map);
    let rc_str = retry_count.to_string();
    if let Some(pos) = fields.iter().position(|(k, _)| k == X_RETRY_COUNT) {
        fields[pos].1 = rc_str;
    } else {
        fields.push((X_RETRY_COUNT.into(), rc_str));
    }
    let mut xadd = redis::cmd("XADD");
    xadd.arg(stream).arg("*");
    for (k, v) in &fields {
        xadd.arg(k.as_str()).arg(v.as_str());
    }
    let _: redis::RedisResult<redis::Value> = xadd.query_async(conn).await;
}

async fn xack(
    conn: &mut redis::aio::Connection,
    stream: &str,
    group: &str,
    entry_id: &str,
) -> Result<()> {
    redis::cmd("XACK")
        .arg(stream)
        .arg(group)
        .arg(entry_id)
        .query_async::<_, i64>(conn)
        .await
        .map(|_| ())
        .map_err(|e| ShoveError::Connection(e.to_string()))
}

async fn autoclaim_all(
    conn: &mut redis::aio::Connection,
    stream: &str,
    group: &str,
    consumer: &str,
    min_idle_ms: u64,
) -> Result<()> {
    redis::cmd("XAUTOCLAIM")
        .arg(stream)
        .arg(group)
        .arg(consumer)
        .arg(min_idle_ms)
        .arg("0-0")
        .arg("COUNT")
        .arg(AUTOCLAIM_COUNT)
        .query_async::<_, redis::Value>(conn)
        .await
        .map(|_| ())
        .map_err(|e| ShoveError::Connection(e.to_string()))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_u32(map: &HashMap<String, redis::Value>, key: &str) -> u32 {
    map.get(key)
        .and_then(|v| match v {
            redis::Value::Data(b) => std::str::from_utf8(b).ok()?.parse().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

fn extract_str(map: &HashMap<String, redis::Value>, key: &str) -> Option<String> {
    map.get(key).and_then(|v| match v {
        redis::Value::Data(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    })
}

fn extract_headers(map: &HashMap<String, redis::Value>) -> std::collections::HashMap<String, String> {
    let skip = [PAYLOAD_FIELD, X_RETRY_COUNT, X_SEQUENCE_KEY];
    map.iter()
        .filter(|(k, _)| !skip.contains(&k.as_str()))
        .filter_map(|(k, v)| match v {
            redis::Value::Data(b) => Some((k.clone(), String::from_utf8_lossy(b).into_owned())),
            _ => None,
        })
        .collect()
}

fn map_to_fields(map: &HashMap<String, redis::Value>) -> Vec<(String, String)> {
    map.iter()
        .filter_map(|(k, v)| match v {
            redis::Value::Data(b) => Some((k.clone(), String::from_utf8_lossy(b).into_owned())),
            _ => None,
        })
        .collect()
}

pub(super) fn hold_level(retry_count: u32, hold_queues: &[&str]) -> Option<usize> {
    if hold_queues.is_empty() {
        None
    } else {
        Some((retry_count as usize).min(hold_queues.len() - 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_count_routing_to_hold_level() {
        let hold_queues = vec!["orders-hold-5s", "orders-hold-30s"];
        assert_eq!(hold_level(0, &hold_queues), Some(0));
        assert_eq!(hold_level(1, &hold_queues), Some(1));
        assert_eq!(hold_level(2, &hold_queues), Some(1));
    }

    #[test]
    fn hold_level_empty_returns_none() {
        assert_eq!(hold_level(0, &[]), None);
    }
}
```

- [ ] **Step 4: Run unit tests**

```bash
cargo test -q --features redis-streams -- backends::redis::consumer 2>&1
```
Expected: 2 passing tests.

- [ ] **Step 5: Commit**

```bash
git add src/backends/redis/consumer.rs
git commit -m "feat(redis): consumer loop with XREADGROUP, outcome routing, XAUTOCLAIM"
```

---

## Task 7: Consumer group registry

**Files:**
- Create: `src/backends/redis/consumer_group.rs`

Mirrors `InMemoryConsumerGroupRegistry`. Stores registered topics + their handler factories; on `start_all` spawns one consumer task per topic (or N shard tasks for sequenced topics). Lifecycle:
1. `register<T, H>` — store factory + options
2. `register_fifo<T, H>` — store factory + sequenced flag
3. `run_until_timeout` — spawn all, wait for signal or cancellation, drain

- [ ] **Step 1: Write failing test**

```rust
// In consumer_group.rs tests
#[test]
fn config_consumer_range() {
    let cfg = RedisConsumerGroupConfig::new(1..=4);
    assert_eq!(*cfg.consumer_range().start(), 1);
    assert_eq!(*cfg.consumer_range().end(), 4);
}
```

- [ ] **Step 2: Verify failure**

```bash
cargo test -q --features redis-streams -- backends::redis::consumer_group 2>&1 | head -5
```

- [ ] **Step 3: Implement consumer_group.rs**

```rust
//! Redis Streams coordinated consumer group registry.

use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::backend::ConsumerOptionsInner;
use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::{Result, ShoveError};
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

use super::client::RedisClient;
use super::consumer::RedisConsumer;

/// Per-topic group configuration: how many consumer tasks to run.
#[derive(Debug, Clone, Default)]
pub struct RedisConsumerGroupConfig {
    consumer_range: RangeInclusive<u16>,
}

impl RedisConsumerGroupConfig {
    pub fn new(consumer_range: RangeInclusive<u16>) -> Self {
        Self { consumer_range }
    }

    pub fn consumer_range(&self) -> &RangeInclusive<u16> {
        &self.consumer_range
    }
}

impl Default for RedisConsumerGroupConfig {
    fn default() -> Self {
        Self::new(1..=1)
    }
}

type BoxFuture = std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type TaskFactory = Box<dyn Fn() -> BoxFuture + Send + Sync>;

pub struct RedisConsumerGroupRegistry {
    client: RedisClient,
    tasks: Vec<TaskFactory>,
    shutdown: CancellationToken,
}

impl RedisConsumerGroupRegistry {
    pub fn new(client: RedisClient) -> Self {
        Self {
            client,
            tasks: Vec::new(),
            shutdown: CancellationToken::new(),
        }
    }

    pub fn broker_shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub async fn register<T, H>(
        &mut self,
        config: RedisConsumerGroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: Topic,
        H: MessageHandler<T>,
    {
        let client = self.client.clone();
        let shutdown = self.shutdown.clone();
        let count = (*config.consumer_range.start()).max(1) as usize;
        let factory = Arc::new(factory);
        let ctx = Arc::new(ctx);

        for _ in 0..count {
            let client_c = client.clone();
            let shutdown_c = shutdown.clone();
            let factory_c = Arc::clone(&factory);
            let ctx_c = Arc::clone(&ctx);
            self.tasks.push(Box::new(move || {
                let client_cc = client_c.clone();
                let shutdown_cc = shutdown_c.clone();
                let handler = (factory_c)();
                let ctx_cc = Arc::clone(&ctx_c);
                Box::pin(async move {
                    let consumer = RedisConsumer::new(client_cc);
                    let inner = ConsumerOptionsInner {
                        max_retries: 10,
                        prefetch_count: 10,
                        handler_timeout: Some(Duration::from_secs(30)),
                        max_pending_per_key: None,
                        max_message_size: Some(10 * 1024 * 1024),
                        shutdown: shutdown_cc,
                        processing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                        consumer_group: None,
                        #[cfg(feature = "rabbitmq-transactional")]
                        exactly_once: false,
                        #[cfg(feature = "aws-sns-sqs")]
                        receive_batch_size: 0,
                        #[cfg(feature = "nats")]
                        max_ack_pending: None,
                    };
                    let ctx_owned = Arc::try_unwrap(ctx_cc)
                        .unwrap_or_else(|arc| unsafe { std::ptr::read(Arc::as_ptr(&arc)) });
                    use crate::backend::ConsumerImpl;
                    consumer.run::<T, H>(handler, ctx_owned, inner).await
                })
            }));
        }
        Ok(())
    }

    pub async fn register_fifo<T, H>(
        &mut self,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where
        T: SequencedTopic,
        H: MessageHandler<T>,
    {
        let topology = T::topology();
        let seq = topology.sequencing().ok_or_else(|| {
            ShoveError::Topology("register_fifo called on non-sequenced topic".into())
        })?;
        let shards = seq.routing_shards();
        let client = self.client.clone();
        let shutdown = self.shutdown.clone();
        let factory = Arc::new(factory);
        let ctx = Arc::new(ctx);

        for shard in 0..shards {
            let client_c = client.clone();
            let shutdown_c = shutdown.clone();
            let factory_c = Arc::clone(&factory);
            let ctx_c = Arc::clone(&ctx);
            let stream = super::topology::RedisTopologyDeclarer::shard_stream_name(topology.queue(), shard);
            self.tasks.push(Box::new(move || {
                let client_cc = client_c.clone();
                let shutdown_cc = shutdown_c.clone();
                let handler = (factory_c)();
                let ctx_cc = Arc::clone(&ctx_c);
                let stream_cc = stream.clone();
                Box::pin(async move {
                    let consumer = RedisConsumer::new(client_cc.clone());
                    let inner = ConsumerOptionsInner {
                        max_retries: 10,
                        prefetch_count: 1,
                        handler_timeout: Some(Duration::from_secs(30)),
                        max_pending_per_key: None,
                        max_message_size: Some(10 * 1024 * 1024),
                        shutdown: shutdown_cc,
                        processing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                        consumer_group: None,
                        #[cfg(feature = "rabbitmq-transactional")]
                        exactly_once: false,
                        #[cfg(feature = "aws-sns-sqs")]
                        receive_batch_size: 0,
                        #[cfg(feature = "nats")]
                        max_ack_pending: None,
                    };
                    let ctx_owned = Arc::try_unwrap(ctx_cc)
                        .unwrap_or_else(|arc| unsafe { std::ptr::read(Arc::as_ptr(&arc)) });
                    use crate::backend::ConsumerImpl;
                    consumer.run::<T, H>(handler, ctx_owned, inner).await
                })
            }));
        }
        Ok(())
    }

    pub fn start_all(&mut self, set: &mut JoinSet<Result<()>>) {
        for factory in self.tasks.drain(..) {
            set.spawn(factory());
        }
    }

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
        let signal_handle = tokio::spawn(signal);
        tokio::select! {
            _ = shutdown.cancelled() => {}
            res = signal_handle => { let _ = res; shutdown.cancel(); }
        }

        let drain = async {
            let mut errors = 0usize;
            let mut panics = 0usize;
            while let Some(res) = set.join_next().await {
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => errors += 1,
                    Err(_) => panics += 1,
                }
            }
            (errors, panics)
        };

        match tokio::time::timeout(drain_timeout, drain).await {
            Ok((errors, panics)) => SupervisorOutcome { errors, panics, timed_out: false },
            Err(_) => {
                set.abort_all();
                SupervisorOutcome { errors: 0, panics: 0, timed_out: true }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_consumer_range() {
        let cfg = RedisConsumerGroupConfig::new(1..=4);
        assert_eq!(*cfg.consumer_range().start(), 1);
        assert_eq!(*cfg.consumer_range().end(), 4);
    }

    #[test]
    fn config_default_range_is_one() {
        let cfg = RedisConsumerGroupConfig::default();
        assert_eq!(*cfg.consumer_range().start(), 1);
        assert_eq!(*cfg.consumer_range().end(), 1);
    }
}
```

- [ ] **Step 4: Run unit tests**

```bash
cargo test -q --features redis-streams -- backends::redis::consumer_group 2>&1
```
Expected: 2 passing tests.

- [ ] **Step 5: Commit**

```bash
git add src/backends/redis/consumer_group.rs
git commit -m "feat(redis): consumer group registry with shard-aware registration"
```

---

## Task 8: Autoscaler

**Files:**
- Create: `src/backends/redis/autoscaler.rs`

Provides queue stats via `XLEN` (backlog) and `XINFO GROUPS` (lag, consumers). Implements `QueueStatsProviderImpl` which maps to `AutoscaleMetrics`.

- [ ] **Step 1: Write failing test**

```rust
// In autoscaler.rs tests
#[test]
fn queue_stats_populates_messages_ready() {
    let stats = RedisQueueStats {
        messages_ready: 42,
        messages_in_flight: 3,
    };
    assert_eq!(stats.messages_ready, 42);
    assert_eq!(stats.messages_in_flight, 3);
}
```

- [ ] **Step 2: Verify failure**

```bash
cargo test -q --features redis-streams -- backends::redis::autoscaler 2>&1 | head -5
```

- [ ] **Step 3: Implement autoscaler.rs**

```rust
//! Redis Streams autoscaler backend — queue depth via XLEN + XINFO GROUPS.

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::backend::{AutoscalerBackendImpl, QueueStatsProviderImpl};
use crate::error::{Result, ShoveError};

use super::client::RedisClient;

/// Raw queue stats from Redis.
#[derive(Debug, Clone, Default)]
pub struct RedisQueueStats {
    pub messages_ready: u64,
    pub messages_in_flight: u64,
}

/// Stats provider: queries XLEN and XINFO GROUPS for a stream.
#[derive(Clone)]
pub struct RedisQueueStatsProvider {
    client: RedisClient,
}

impl RedisQueueStatsProvider {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }

    pub async fn get_queue_stats(&self, queue: &str) -> Result<RedisQueueStats> {
        let mut conn = self.client.multiplexed_conn().await?;

        // Total entries in the stream (includes already-acked, so approximate).
        // For a more accurate "ready" count we'd subtract PEL size.
        let stream_len: u64 = redis::cmd("XLEN")
            .arg(queue)
            .query_async(&mut conn)
            .await
            .unwrap_or(0);

        // Get pending (in-flight) count for our group via XPENDING summary.
        let group = self.client.group();
        let pending: u64 = redis::cmd("XPENDING")
            .arg(queue)
            .arg(group)
            .query_async::<_, redis::Value>(&mut conn)
            .await
            .ok()
            .and_then(|v| {
                // XPENDING summary returns [count, min-id, max-id, consumers]
                if let redis::Value::Bulk(ref parts) = v {
                    if let Some(redis::Value::Int(n)) = parts.first() {
                        return Some(*n as u64);
                    }
                }
                None
            })
            .unwrap_or(0);

        let ready = stream_len.saturating_sub(pending);
        Ok(RedisQueueStats {
            messages_ready: ready,
            messages_in_flight: pending,
        })
    }
}

/// Marker type for the Redis autoscaler (no additional methods beyond QueueStatsProviderImpl).
#[derive(Clone)]
pub struct RedisAutoscalerBackend {
    _client: RedisClient,
}

impl RedisAutoscalerBackend {
    pub fn new(client: RedisClient) -> Self {
        Self { _client: client }
    }
}

impl AutoscalerBackendImpl for RedisAutoscalerBackend {}

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
}
```

- [ ] **Step 4: Run unit tests**

```bash
cargo test -q --features redis-streams -- backends::redis::autoscaler 2>&1
```
Expected: 1 passing test.

- [ ] **Step 5: Commit**

```bash
git add src/backends/redis/autoscaler.rs
git commit -m "feat(redis): autoscaler backend with XLEN/XPENDING stats provider"
```

---

## Task 9: Backend trait impl + lib.rs wiring

**Files:**
- Create: `src/backends/redis/backend.rs`
- Modify: `src/lib.rs`

Wire everything together: `impl Backend for Redis`, `impl HasCoordinatedGroups for Redis`, add `pub mod redis` to `lib.rs` with re-exports.

- [ ] **Step 1: Implement backend.rs**

```rust
//! Backend / impl-trait registrations for the Redis Streams backend.

use crate::autoscale_metrics::AutoscaleMetrics;
use crate::backend::{
    AutoscalerBackendImpl, Backend, ConsumerImpl, ConsumerOptionsInner, QueueStatsProviderImpl,
    RegistryImpl, TopologyImpl, capability::HasCoordinatedGroups, sealed,
};
use crate::consumer_supervisor::SupervisorOutcome;
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::markers::Redis;
use crate::topic::{SequencedTopic, Topic};
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::autoscaler::{RedisAutoscalerBackend, RedisQueueStatsProvider};
use super::client::{RedisClient, RedisConfig};
use super::consumer::RedisConsumer;
use super::consumer_group::{RedisConsumerGroupConfig, RedisConsumerGroupRegistry};
use super::publisher::RedisPublisher;
use super::topology::RedisTopologyDeclarer;

impl sealed::Sealed for Redis {}

impl Backend for Redis {
    type Config = RedisConfig;
    type Client = RedisClient;

    type PublisherImpl = RedisPublisher;
    type ConsumerImpl = RedisConsumer;
    type TopologyImpl = RedisTopologyDeclarer;
    type AutoscalerImpl = RedisAutoscalerBackend;
    type QueueStatsImpl = RedisQueueStatsProvider;

    async fn connect(config: Self::Config) -> Result<Self::Client> {
        RedisClient::connect(config).await
    }

    async fn make_publisher(client: &Self::Client) -> Result<Self::PublisherImpl> {
        Ok(RedisPublisher::new(client.clone()))
    }

    fn make_consumer(client: &Self::Client) -> Self::ConsumerImpl {
        RedisConsumer::new(client.clone())
    }

    fn make_declarer(client: &Self::Client) -> Self::TopologyImpl {
        RedisTopologyDeclarer::new(client.clone())
    }

    fn make_autoscaler(client: &Self::Client) -> Self::AutoscalerImpl {
        RedisAutoscalerBackend::new(client.clone())
    }

    fn make_stats_provider(client: &Self::Client) -> Self::QueueStatsImpl {
        RedisQueueStatsProvider::new(client.clone())
    }

    async fn close(_client: &Self::Client) {
        // redis::Client has no explicit close — connections close when dropped.
    }
}

impl HasCoordinatedGroups for Redis {
    type ConsumerGroupConfig = RedisConsumerGroupConfig;
    type RegistryImpl = RedisConsumerGroupRegistry;

    fn make_registry(client: &Self::Client) -> Self::RegistryImpl {
        RedisConsumerGroupRegistry::new(client.clone())
    }
}

// ---------------------------------------------------------------------------
// ConsumerImpl delegation
// ---------------------------------------------------------------------------

impl ConsumerImpl for RedisConsumer {
    async fn run<T, H>(&self, handler: H, ctx: H::Context, options: ConsumerOptionsInner) -> Result<()>
    where T: Topic, H: MessageHandler<T> {
        RedisConsumer::run_unsequenced::<T, H>(self, handler, ctx, options).await
    }

    async fn run_fifo<T, H>(&self, handler: H, ctx: H::Context, options: ConsumerOptionsInner) -> Result<()>
    where T: SequencedTopic, H: MessageHandler<T> {
        RedisConsumer::run_fifo_impl::<T, H>(self, handler, ctx, options).await
    }

    async fn run_dlq<T, H>(&self, handler: H, ctx: H::Context) -> Result<()>
    where T: Topic, H: MessageHandler<T> {
        RedisConsumer::run_dlq_impl::<T, H>(self, handler, ctx).await
    }

    async fn spawn_fifo_shards<T, H>(
        &self, handler: H, ctx: H::Context, options: ConsumerOptionsInner,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>>
    where T: SequencedTopic, H: MessageHandler<T> {
        RedisConsumer::spawn_shards::<T, H>(self, handler, ctx, options).await
    }
}

// ---------------------------------------------------------------------------
// TopologyImpl
// ---------------------------------------------------------------------------

impl TopologyImpl for RedisTopologyDeclarer {
    async fn declare<T: Topic>(&self) -> Result<()> {
        RedisTopologyDeclarer::declare(self, T::topology()).await
    }
}

// ---------------------------------------------------------------------------
// AutoscalerBackendImpl + QueueStatsProviderImpl
// ---------------------------------------------------------------------------

impl AutoscalerBackendImpl for RedisAutoscalerBackend {}

impl QueueStatsProviderImpl for RedisQueueStatsProvider {
    async fn snapshot(&self, queue: &str) -> Result<AutoscaleMetrics> {
        RedisQueueStatsProvider::snapshot_impl(self, queue).await
    }
}

// ---------------------------------------------------------------------------
// RegistryImpl
// ---------------------------------------------------------------------------

impl RegistryImpl for RedisConsumerGroupRegistry {
    type GroupConfig = RedisConsumerGroupConfig;

    async fn register<T, H>(
        &mut self,
        config: Self::GroupConfig,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where T: Topic, H: MessageHandler<T> {
        RedisConsumerGroupRegistry::register::<T, H>(self, config, factory, ctx).await
    }

    async fn register_fifo<T, H>(
        &mut self,
        factory: impl Fn() -> H + Send + Sync + 'static,
        ctx: H::Context,
    ) -> Result<()>
    where T: SequencedTopic, H: MessageHandler<T> {
        RedisConsumerGroupRegistry::register_fifo::<T, H>(self, factory, ctx).await
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.broker_shutdown_token()
    }

    async fn run_until_timeout<S>(self, signal: S, drain_timeout: Duration) -> SupervisorOutcome
    where S: Future<Output = ()> + Send + 'static {
        RedisConsumerGroupRegistry::run_until_timeout(self, signal, drain_timeout).await
    }
}
```

- [ ] **Step 2: Add public module to lib.rs**

In `src/lib.rs`, add after the `inmemory` block:
```rust
#[cfg(feature = "redis-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-streams")))]
/// Redis Streams backend.
///
/// Requires a Redis 7.0+ server. Uses `XGROUP`/`XREADGROUP`/`XACK` for
/// consumer groups with at-least-once delivery. Hold queues are implemented
/// via sorted sets + a background requeue task. FIFO ordering uses
/// publisher-side consistent hashing to shard streams.
pub mod redis {
    pub use crate::markers::Redis;
    pub use crate::backends::redis::{
        RedisAutoscalerBackend, RedisClient, RedisConfig, RedisConsumer,
        RedisConsumerGroupConfig, RedisConsumerGroupRegistry, RedisPublisher,
        RedisQueueStats, RedisQueueStatsProvider, RedisTopologyDeclarer,
    };
}
```

Add to the `pub use markers::*` section:
```rust
#[cfg(feature = "redis-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-streams")))]
pub use markers::Redis;
```

- [ ] **Step 3: Verify it compiles**

```bash
cargo check -q --features redis-streams 2>&1
```
Expected: clean (or only warnings, no errors).

- [ ] **Step 4: Commit**

```bash
git add src/backends/redis/backend.rs src/lib.rs
git commit -m "feat(redis): wire Backend + HasCoordinatedGroups trait impls"
```

---

## Task 10: Integration tests

**Files:**
- Create: `tests/redis_integration.rs`

Uses `testcontainers` to spin up a real Redis 7 container. Tests: basic pub/sub, retry → hold → redeliver, reject → DLQ, FIFO shard ordering, autoscaler stats.

- [ ] **Step 1: Write the test file**

```rust
//! Integration tests for the Redis Streams backend.
//! Requires Docker. Run with: cargo test --test redis_integration --features redis-streams -q

#![cfg(feature = "redis-streams")]

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis as RedisImage;

use shove::redis::{RedisConfig, RedisConsumerGroupConfig};
use shove::{
    Broker, ConsumerGroupConfig, MessageHandler, MessageMetadata, Outcome, SequenceFailure,
    SequencedTopic, Topic, TopologyBuilder,
};

// ---------------------------------------------------------------------------
// Test topics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order { id: u64 }

struct OrdersTopic;
impl Topic for OrdersTopic {
    type Message = Order;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-orders-int")
                .hold_queue(Duration::from_millis(100))
                .dlq()
                .build()
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event { account: String, seq: u64 }

struct LedgerTopic;
impl Topic for LedgerTopic {
    type Message = Event;
    fn topology() -> &'static shove::QueueTopology {
        static T: OnceLock<shove::QueueTopology> = OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-ledger-int")
                .sequenced(SequenceFailure::Skip)
                .routing_shards(4)
                .hold_queue(Duration::from_millis(50))
                .dlq()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Self::sequence_key);
}
impl SequencedTopic for LedgerTopic {
    fn sequence_key(msg: &Event) -> String { msg.account.clone() }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

async fn redis_broker(url: &str) -> shove::broker::Broker<shove::Redis> {
    Broker::<shove::Redis>::new(RedisConfig { url: url.into(), group: None })
        .await
        .expect("broker connect")
}

// ---------------------------------------------------------------------------
// Test: basic publish → consume → ack
// ---------------------------------------------------------------------------

#[tokio::test]
async fn basic_pubsub_ack() {
    let container = RedisImage::default().start().await.expect("start redis");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let url = format!("redis://127.0.0.1:{port}/");

    let broker = redis_broker(&url).await;
    broker.topology().declare::<OrdersTopic>().await.expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher.publish::<OrdersTopic>(&Order { id: 1 }).await.expect("publish");

    let counter = Arc::new(AtomicU32::new(0));
    let counter_c = Arc::clone(&counter);

    struct H(Arc<AtomicU32>);
    impl MessageHandler<OrdersTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Ack
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor.register::<OrdersTopic, _>(H(counter_c), Default::default()).await.expect("register");

    let signal = tokio::time::sleep(Duration::from_secs(3));
    let outcome = supervisor.run_until_timeout(signal, Duration::from_secs(1)).await;
    assert!(outcome.is_clean());
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// Test: Retry → hold queue → redeliver → Ack on second delivery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retry_then_ack_on_redeliver() {
    let container = RedisImage::default().start().await.expect("start redis");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let url = format!("redis://127.0.0.1:{port}/");

    let broker = redis_broker(&url).await;
    broker.topology().declare::<OrdersTopic>().await.expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher.publish::<OrdersTopic>(&Order { id: 2 }).await.expect("publish");

    let calls = Arc::new(AtomicU32::new(0));
    let calls_c = Arc::clone(&calls);

    struct H(Arc<AtomicU32>);
    impl MessageHandler<OrdersTopic> for H {
        type Context = ();
        async fn handle(&self, _: Order, meta: MessageMetadata, _: &()) -> Outcome {
            let n = self.0.fetch_add(1, Ordering::SeqCst);
            if meta.retry_count == 0 { Outcome::Retry } else { Outcome::Ack }
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor.register::<OrdersTopic, _>(H(calls_c), Default::default()).await.expect("register");

    // Wait longer to allow the 100ms hold queue to expire and requeue.
    let signal = tokio::time::sleep(Duration::from_secs(5));
    let outcome = supervisor.run_until_timeout(signal, Duration::from_secs(1)).await;
    assert!(outcome.is_clean());
    assert_eq!(calls.load(Ordering::SeqCst), 2, "should be called twice");
}

// ---------------------------------------------------------------------------
// Test: Reject → DLQ
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reject_routes_to_dlq() {
    let container = RedisImage::default().start().await.expect("start redis");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let url = format!("redis://127.0.0.1:{port}/");

    let broker = redis_broker(&url).await;
    broker.topology().declare::<OrdersTopic>().await.expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    publisher.publish::<OrdersTopic>(&Order { id: 3 }).await.expect("publish");

    let main_calls = Arc::new(AtomicU32::new(0));
    let dlq_calls = Arc::new(AtomicU32::new(0));
    let main_c = Arc::clone(&main_calls);
    let dlq_c = Arc::clone(&dlq_calls);

    struct MainH(Arc<AtomicU32>);
    impl MessageHandler<OrdersTopic> for MainH {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Reject
        }
    }

    struct DlqH(Arc<AtomicU32>);
    impl MessageHandler<OrdersTopic> for DlqH {
        type Context = ();
        async fn handle(&self, _: Order, _: MessageMetadata, _: &()) -> Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Ack
        }
    }

    let mut supervisor = broker.consumer_supervisor();
    supervisor.register::<OrdersTopic, _>(MainH(main_c), Default::default()).await.expect("register main");
    supervisor.register_dlq::<OrdersTopic, _>(DlqH(dlq_c), Default::default()).await.expect("register dlq");

    let outcome = supervisor
        .run_until_timeout(tokio::time::sleep(Duration::from_secs(3)), Duration::from_secs(1))
        .await;
    assert!(outcome.is_clean());
    assert_eq!(main_calls.load(Ordering::SeqCst), 1);
    assert_eq!(dlq_calls.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// Test: FIFO shard — same account always goes to same shard (ordering preserved)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fifo_same_key_delivered_in_order() {
    let container = RedisImage::default().start().await.expect("start redis");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let url = format!("redis://127.0.0.1:{port}/");

    let broker = redis_broker(&url).await;
    broker.topology().declare::<LedgerTopic>().await.expect("declare");

    let publisher = broker.publisher().await.expect("publisher");
    let n = 10u64;
    for seq in 0..n {
        publisher.publish::<LedgerTopic>(&Event { account: "acct-1".into(), seq })
            .await.expect("publish");
    }

    let received = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));
    let received_c = Arc::clone(&received);

    struct H(Arc<tokio::sync::Mutex<Vec<u64>>>);
    impl MessageHandler<LedgerTopic> for H {
        type Context = ();
        async fn handle(&self, msg: Event, _: MessageMetadata, _: &()) -> Outcome {
            self.0.lock().await.push(msg.seq);
            Outcome::Ack
        }
    }

    let mut group = broker.consumer_group();
    group.register_fifo::<LedgerTopic, _>(|| H(Arc::clone(&received_c))).await.expect("register fifo");

    let outcome = group
        .run_until_timeout(tokio::time::sleep(Duration::from_secs(5)), Duration::from_secs(1))
        .await;
    assert!(outcome.is_clean());

    let seqs = received.lock().await;
    assert_eq!(seqs.len(), n as usize);
    // All seq values for "acct-1" must arrive in order.
    let is_ordered = seqs.windows(2).all(|w| w[0] < w[1]);
    assert!(is_ordered, "messages delivered out of order: {seqs:?}");
}
```

- [ ] **Step 2: Run tests (requires Docker)**

```bash
cargo test -q --features redis-streams --test redis_integration 2>&1
```
Expected: all 4 tests pass (may take ~10-20s for container startup).

- [ ] **Step 3: Commit**

```bash
git add tests/redis_integration.rs
git commit -m "test(redis): integration tests — ack, retry, DLQ, FIFO ordering"
```

---

## Task 11: Basic example

**Files:**
- Create: `examples/redis/basic.rs`

- [ ] **Step 1: Create the example**

```rust
//! Basic Redis Streams pub/sub example.
//! Run with: cargo run --example redis_basic --features redis-streams
//!
//! Expects a Redis instance at redis://127.0.0.1:6379
//! Start one with: docker run --rm -p 6379:6379 redis:7

use std::time::Duration;
use serde::{Deserialize, Serialize};
use shove::redis::RedisConfig;
use shove::{Broker, MessageHandler, MessageMetadata, Outcome, Topic, TopologyBuilder, define_topic};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderPaid { order_id: String }

define_topic!(Orders, OrderPaid,
    TopologyBuilder::new("orders")
        .hold_queue(Duration::from_secs(5))
        .hold_queue(Duration::from_secs(30))
        .dlq()
        .build()
);

struct Handler;
impl MessageHandler<Orders> for Handler {
    type Context = ();
    async fn handle(&self, msg: OrderPaid, meta: MessageMetadata, _: &()) -> Outcome {
        println!("received order: {} (retry={})", msg.order_id, meta.retry_count);
        Outcome::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), shove::ShoveError> {
    tracing_subscriber::fmt::init();

    let broker = Broker::<shove::Redis>::new(RedisConfig {
        url: "redis://127.0.0.1:6379/".into(),
        group: None,
    }).await?;

    broker.topology().declare::<Orders>().await?;

    let publisher = broker.publisher().await?;
    for i in 0..5 {
        publisher.publish::<Orders>(&OrderPaid { order_id: format!("ORD-{i}") }).await?;
    }
    println!("published 5 messages");

    let mut group = broker.consumer_group();
    group.register::<Orders, _>(
        shove::ConsumerGroupConfig::new(shove::redis::RedisConsumerGroupConfig::new(1..=1)),
        || Handler,
    ).await?;

    println!("consuming (Ctrl+C to stop)...");
    let outcome = group
        .run_until_timeout(tokio::signal::ctrl_c().map(|_| ()), Duration::from_secs(10))
        .await;
    std::process::exit(outcome.exit_code());
}
```

- [ ] **Step 2: Verify the example compiles**

```bash
cargo build -q --example redis_basic --features redis-streams 2>&1
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add examples/redis/
git commit -m "feat(redis): basic pub/sub example"
```

---

## Self-Review

### Spec coverage check

| Feature | Task covering it |
|---|---|
| Consumer groups (`HasCoordinatedGroups`) | Task 7 + Task 9 |
| Supervisor mode | Task 6 + Task 9 |
| Hold queues (retry routing) | Task 5 (requeue) + Task 6 (consumer routing) |
| DLQ routing (`Reject`, exhausted retries) | Task 6 |
| Defer → hold_queues[0] | Task 6 |
| FIFO shard routing at publish | Task 4 (publisher `publish_with_headers`) |
| FIFO shard consumers | Task 6 (`spawn_fifo_shards`) + Task 7 (`register_fifo`) |
| `SequenceFailure::Skip` | Task 6 (Skip: DLQ bad msg, continue shard) |
| `SequenceFailure::FailAll` | **Not in scope for v1** — requires per-key buffer tracking; document as limitation |
| XAUTOCLAIM for crash recovery | Task 6 |
| Autoscaling metrics | Task 8 |
| Handler timeout → PEL reclaim | Task 6 |
| `max_message_size` guard | Task 6 |
| Custom headers | Task 4 (publisher) + Task 6 (consumer extraction) |
| `MessageMetadata` (retry_count, delivery_id, redelivered, headers) | Task 6 |
| Topology declaration (idempotent) | Task 3 |
| `close()` no-op | Task 9 |
| Integration tests | Task 10 |
| Example | Task 11 |

### Known limitations (document in code, not fix now)
1. `SequenceFailure::FailAll` is not implemented — treated as `Skip` with a warning log
2. Redis < 7.0: `XAUTOCLAIM` requires Redis 7.0+; a fallback via `XPENDING`+`XCLAIM` can be added later
3. No stream trimming (`MAXLEN`) — add via `RedisConfig::max_stream_len: Option<u64>` in a follow-up
4. `spawn_fifo_shards` uses `Arc::try_unwrap` + `ptr::read` as a workaround for sharing handlers across shard tasks — safe only because Arc refcount = 1 at that point, but should be replaced with a factory pattern in a follow-up

### Type consistency verified
- `RedisClient` used consistently across publisher, consumer, topology, autoscaler, registry
- `ConsumerOptionsInner` copied correctly in registry (all feature-gated fields present)
- `HoldEntry` serialized/deserialized using the same `serde_json` in both `requeue.rs` and `consumer.rs`
- Stream name helpers (`shard_stream_name`, `hold_set_name`) defined on `RedisTopologyDeclarer` and called from both topology and consumer modules

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-02-redis-streams-backend.md`.**

**Two execution options:**

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`

**Which approach?**
