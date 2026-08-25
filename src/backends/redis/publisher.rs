//! Redis Streams publisher — serializes messages to JSON and XADDs them to
//! the correct stream, with shove metadata as additional stream entry fields.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::backend::publisher::PublisherImpl;
use crate::batch::BatchReport;
use crate::error::{Result, ShoveError};
use crate::publisher_internal::{shard_for_key as shared_shard_for_key, validate_headers};
use crate::topic::Topic;

use super::client::{RedisClient, RedisConnection};
use super::constants::{DEFAULT_ROUTING_SHARDS, PAYLOAD_FIELD, X_SEQUENCE_KEY};
use super::topology::RedisTopologyDeclarer;

// ---------------------------------------------------------------------------
// FNV-1a shard routing
// ---------------------------------------------------------------------------

/// Map a sequence key to a shard index using the shared FNV-1a hash.
///
/// Delegates to [`crate::publisher_internal::shard_for_key`] so a given key
/// routes to the same shard on every backend. Re-exported as
/// `redis::shard_for_key`.
pub fn shard_for_key(key: &str, routing_shards: u16) -> u16 {
    shared_shard_for_key(key, routing_shards)
}

// ---------------------------------------------------------------------------
// RedisPublisher
// ---------------------------------------------------------------------------

/// Publishes messages to Redis Streams.
///
/// Implements [`PublisherImpl`]. Holds one cached multiplexed connection
/// shared by every publish on this publisher and its clones — redis-rs dials
/// a fresh TCP socket on every `get_multiplexed_async_connection` call, so
/// acquiring per publish opened (and dropped) a connection per message.
/// On a publish error the cached connection is invalidated and the next
/// publish dials a fresh one; the error itself still propagates to the
/// caller unchanged.
#[derive(Clone)]
pub struct RedisPublisher {
    client: RedisClient,
    conn: Arc<Mutex<Option<RedisConnection>>>,
}

struct RedisPublishError {
    error: ShoveError,
    explicit_rejection: bool,
}

impl RedisPublishError {
    fn unresolved(error: ShoveError) -> Self {
        Self {
            error,
            explicit_rejection: false,
        }
    }

    fn xadd(stream: &str, source: redis::RedisError) -> Self {
        let explicit_rejection = matches!(
            source.kind(),
            redis::ErrorKind::Server(_) | redis::ErrorKind::Extension
        );
        Self {
            error: ShoveError::Connection(format!("XADD to {stream} failed: {source}")),
            explicit_rejection,
        }
    }
}

impl RedisPublisher {
    /// Create a new publisher backed by the given [`RedisClient`].
    pub fn new(client: RedisClient) -> Self {
        Self {
            client,
            conn: Arc::new(Mutex::new(None)),
        }
    }

    /// Clone the cached connection, dialing and caching one if absent.
    /// Clones share the underlying socket and multiplexer task, so the lock
    /// is held only for the clone, never across a query.
    async fn cached_conn(&self) -> Result<RedisConnection> {
        let mut guard = self.conn.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let c = self.client.multiplexed_conn().await?;
        *guard = Some(c.clone());
        Ok(c)
    }

    /// Drop the cached connection so the next publish dials a fresh one.
    async fn invalidate_conn(&self) {
        *self.conn.lock().await = None;
    }

    /// Core XADD helper — resolves the stream name, serializes, and publishes.
    /// Accepts an optional pre-acquired connection so `publish_batch` can reuse one.
    async fn publish_inner<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
        conn: Option<&mut RedisConnection>,
    ) -> std::result::Result<(), RedisPublishError> {
        let topology = T::topology();
        let payload = <T::Codec as crate::Codec<T::Message>>::encode_to_string(msg)
            .map_err(RedisPublishError::unresolved)?;

        let (stream, sequence_key) = if let Some(key_fn) = T::SEQUENCE_KEY_FN {
            let seq_key = key_fn(msg);
            let routing_shards = topology
                .sequencing()
                .ok_or_else(|| {
                    RedisPublishError::unresolved(ShoveError::Validation(
                        "topic has SEQUENCE_KEY_FN but topology.sequencing() is None; declare with sequenced()".into()
                    ))
                })?
                .routing_shards();
            let shard_idx = shard_for_key(&seq_key, routing_shards);
            let stream = RedisTopologyDeclarer::shard_stream_name(topology.queue(), shard_idx);
            (stream, Some(seq_key))
        } else {
            (topology.queue().to_owned(), None)
        };

        match conn {
            Some(c) => xadd_on_conn(c, &stream, &payload, &headers, sequence_key.as_deref())
                .await
                .map_err(|e| RedisPublishError::xadd(&stream, e)),
            None => {
                let mut owned = self
                    .cached_conn()
                    .await
                    .map_err(RedisPublishError::unresolved)?;
                let result = xadd_on_conn(
                    &mut owned,
                    &stream,
                    &payload,
                    &headers,
                    sequence_key.as_deref(),
                )
                .await;
                if result.is_err() {
                    self.invalidate_conn().await;
                }
                result.map_err(|e| RedisPublishError::xadd(&stream, e))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Inherent API
// ---------------------------------------------------------------------------

impl RedisPublisher {
    /// Publish `msg` to the topic's stream (or a sharded stream if `T` is
    /// sequenced).
    pub async fn publish<T: Topic>(&self, msg: &T::Message) -> Result<()> {
        self.publish_inner::<T>(msg, HashMap::new(), None)
            .await
            .map_err(|e| e.error)
    }

    /// Publish `msg` with caller-provided headers carried in the XADD entry.
    ///
    /// Rejects headers using a reserved prefix (e.g. `x-retry-count`,
    /// `x-message-id`, `x-sequence-key`) so a publisher cannot forge the
    /// internal routing/accounting fields the consumer reads off the stream
    /// entry — matching the other backends.
    pub async fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        validate_headers(&headers)?;
        self.publish_inner::<T>(msg, headers, None)
            .await
            .map_err(|e| e.error)
    }

    /// Publish a batch on a single multiplexed connection.
    ///
    /// XADDs are issued sequentially and the call returns at the first error.
    /// A server error explicitly rejects that index; a transport or protocol
    /// error leaves its fate unresolved, so that index and the untouched tail
    /// are reported as unattempted. Failing to acquire the connection at all
    /// leaves the whole batch unattempted.
    pub async fn publish_batch<T: Topic>(&self, msgs: &[T::Message]) -> Result<()> {
        self.publish_batch_report::<T>(msgs)
            .await
            .resolve(msgs.len())
            .result
    }

    pub(crate) async fn publish_batch_report<T: Topic>(&self, msgs: &[T::Message]) -> BatchReport {
        let mut conn = match self.cached_conn().await {
            Ok(c) => c,
            Err(e) => return BatchReport::wholly_unattempted(msgs.len(), e),
        };
        for (i, msg) in msgs.iter().enumerate() {
            if let Err(failure) = self
                .publish_inner::<T>(msg, HashMap::new(), Some(&mut conn))
                .await
            {
                self.invalidate_conn().await;
                return if failure.explicit_rejection {
                    BatchReport::prefix(i, msgs.len(), failure.error)
                } else {
                    BatchReport::sparse(Vec::new(), (i..msgs.len()).collect(), Some(failure.error))
                };
            }
        }
        BatchReport::all_succeeded()
    }
}

// ---------------------------------------------------------------------------
// PublisherImpl — thin forward over the inherent methods.
// ---------------------------------------------------------------------------

impl PublisherImpl for RedisPublisher {
    fn publish<T: Topic>(
        &self,
        msg: &T::Message,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        RedisPublisher::publish::<T>(self, msg)
    }

    fn publish_with_headers<T: Topic>(
        &self,
        msg: &T::Message,
        headers: HashMap<String, String>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        RedisPublisher::publish_with_headers::<T>(self, msg, headers)
    }

    fn publish_batch<T: Topic>(
        &self,
        msgs: &[T::Message],
    ) -> impl std::future::Future<Output = BatchReport> + Send {
        RedisPublisher::publish_batch_report::<T>(self, msgs)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Execute a single XADD on an already-open connection.
async fn xadd_on_conn(
    conn: &mut RedisConnection,
    stream: &str,
    payload: &str,
    headers: &HashMap<String, String>,
    sequence_key: Option<&str>,
) -> redis::RedisResult<()> {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(stream).arg("*");
    cmd.arg(PAYLOAD_FIELD).arg(payload);
    for (k, v) in headers {
        cmd.arg(k).arg(v);
    }
    if let Some(seq_key) = sequence_key {
        cmd.arg(X_SEQUENCE_KEY).arg(seq_key);
    }
    conn.query_redis::<redis::Value>(&mut cmd).await.map(|_| ())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::backends::redis::client::{RedisConfig, RedisMode};
    use crate::topology::TopologyBuilder;
    use crate::{ShoveError, define_topic};

    define_topic!(
        AmbiguousBatchTopic,
        u64,
        TopologyBuilder::new("redis-ambiguous-batch").build()
    );

    async fn read_command(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Option<Vec<Vec<u8>>> {
        loop {
            if let Some((command, consumed)) = parse_command(buffer) {
                buffer.drain(..consumed);
                return Some(command);
            }
            let mut chunk = [0u8; 4096];
            let read = stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
    }

    fn parse_command(buffer: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
        fn line(buffer: &[u8], start: usize) -> Option<(&[u8], usize)> {
            let rel = buffer.get(start..)?.windows(2).position(|w| w == b"\r\n")?;
            let end = start + rel;
            Some((&buffer[start..end], end + 2))
        }

        let (array, mut cursor) = line(buffer, 0)?;
        let count: usize = std::str::from_utf8(array.strip_prefix(b"*")?)
            .ok()?
            .parse()
            .ok()?;
        let mut parts = Vec::with_capacity(count);
        for _ in 0..count {
            let (length, after_length) = line(buffer, cursor)?;
            let length: usize = std::str::from_utf8(length.strip_prefix(b"$")?)
                .ok()?
                .parse()
                .ok()?;
            let end = after_length.checked_add(length)?;
            if buffer.get(end..end + 2)? != b"\r\n" {
                return None;
            }
            parts.push(buffer.get(after_length..end)?.to_vec());
            cursor = end + 2;
        }
        Some((parts, cursor))
    }

    async fn serve_mock_redis_connection(
        mut stream: TcpStream,
        xadds: Arc<AtomicUsize>,
    ) -> std::io::Result<()> {
        let mut buffer = Vec::new();
        while let Some(command) = read_command(&mut stream, &mut buffer).await {
            let name = command
                .first()
                .map(|part| part.to_ascii_uppercase())
                .unwrap_or_default();
            match name.as_slice() {
                b"INFO" => {
                    let info = b"# Server\r\nredis_version:7.2.0\r\n";
                    stream
                        .write_all(format!("${}\r\n", info.len()).as_bytes())
                        .await?;
                    stream.write_all(info).await?;
                    stream.write_all(b"\r\n").await?;
                }
                b"XADD" if xadds.fetch_add(1, Ordering::SeqCst) == 0 => {
                    stream.write_all(b"$3\r\n1-0\r\n").await?;
                }
                b"XADD" => {
                    // Model the transport disappearing after Redis applied the
                    // command but before its response reached the client.
                    return Ok(());
                }
                b"PING" => stream.write_all(b"+PONG\r\n").await?,
                _ => stream.write_all(b"+OK\r\n").await?,
            }
        }
        Ok(())
    }

    async fn start_ambiguous_xadd_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock Redis server");
        let address = listener.local_addr().expect("mock Redis address");
        let xadds = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let xadds = Arc::clone(&xadds);
                tokio::spawn(async move {
                    let _ = serve_mock_redis_connection(stream, xadds).await;
                });
            }
        });
        (format!("redis://{address}/"), server)
    }

    #[tokio::test]
    async fn ambiguous_xadd_response_is_unattempted_not_failed() {
        let (url, server) = start_ambiguous_xadd_server().await;
        let client = RedisClient::connect(
            RedisConfig::new(RedisMode::Standalone { url })
                .with_response_timeout(Duration::from_secs(3)),
        )
        .await
        .expect("connect to mock Redis");
        let publisher = RedisPublisher::new(client);

        let outcome = publisher
            .publish_batch_report::<AmbiguousBatchTopic>(&[1, 2, 3])
            .await
            .resolve(3);
        let ShoveError::PartialBatch(failure) = outcome.result.expect_err("batch must be partial")
        else {
            panic!("expected PartialBatch")
        };

        assert_eq!(failure.succeeded(), 1);
        assert!(
            failure.failed().is_empty(),
            "a lost reply is not an explicit broker rejection"
        );
        assert_eq!(failure.unattempted(), &[1, 2]);
        assert_eq!(failure.to_republish(), &[1, 2]);

        server.abort();
    }

    #[test]
    fn only_a_redis_error_response_is_an_explicit_rejection() {
        let response = redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "XADD rejected",
        ));
        assert!(RedisPublishError::xadd("stream", response).explicit_rejection);

        let transport = redis::RedisError::from(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "reply lost",
        ));
        assert!(!RedisPublishError::xadd("stream", transport).explicit_rejection);
    }

    #[test]
    fn shard_for_key_is_stable_and_bounded() {
        assert!(shard_for_key("acct-1", 8) < 8);
        assert_eq!(shard_for_key("acct-1", 8), shard_for_key("acct-1", 8)); // stable
    }

    #[test]
    fn shard_for_key_single_shard_always_zero() {
        for key in ["a", "b", "hello-world", "acct-9999"] {
            assert_eq!(shard_for_key(key, 1), 0, "single shard must always be 0");
        }
    }

    #[test]
    fn shard_for_key_different_keys_may_differ() {
        // With enough shards, distinct keys should not all land on the same shard.
        let shards = 16u16;
        let shard_a = shard_for_key("user-1", shards);
        let shard_b = shard_for_key("user-2", shards);
        let shard_c = shard_for_key("account-xyz", shards);
        // Can't assert they're all different (hash collisions are possible),
        // but with 16 shards and 3 distinct keys the probability they all collide is tiny.
        let all_same = shard_a == shard_b && shard_b == shard_c;
        assert!(!all_same, "expected at least two keys on different shards");
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

    #[test]
    fn shard_for_key_empty_string() {
        // Empty string is a valid key; must not panic and must return < shards.
        let result = shard_for_key("", 4);
        assert!(result < 4);
    }

    #[test]
    fn shard_for_key_with_max_shards() {
        // u16::MAX shards — result must be < u16::MAX.
        let result = shard_for_key("key", u16::MAX);
        assert!(result < u16::MAX);
    }

    #[test]
    fn shard_for_key_two_shards_splits_keys() {
        // With 100 keys and 2 shards both shards must be used at least once.
        let mut seen = [false; 2];
        for i in 0..100u32 {
            let shard = shard_for_key(&format!("key-{i}"), 2);
            seen[shard as usize] = true;
        }
        assert!(seen[0], "shard 0 was never hit");
        assert!(seen[1], "shard 1 was never hit");
    }
}
