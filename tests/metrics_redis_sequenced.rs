#![cfg(all(feature = "redis-streams", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: `shove_messages_failed_total` on the *sequenced* Redis
//! Streams consumer path.
//!
//! The Redis backend funnels its FIFO path through the shared
//! `route_outcome`, so it inherited the discard counters PR #63 added — but
//! nothing ever proved that at runtime: `metrics` was enabled on no coverage
//! entry but `inmemory`, so every `record_failed` call in
//! `backends/redis/consumer.rs` was type-checked and never executed. This
//! drives a real Redis container through both instrumented terminal
//! decisions — handler `Reject` and retry-budget exhaustion — and pins each
//! to exactly one increment.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot, and whose `snapshot()` drains every counter it reads. So:
//! own integration binary, a single `#[test]`, and exactly one snapshot taken
//! at the end — progress is waited on through handler counters and the DLQ
//! stream, never by peeking at the metrics.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};

use shove::consumer_group::ConsumerGroupConfig;
use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode};
// Imported item by item rather than through `shove::*`: the glob shadows the
// `metrics` crate this file names directly in `failed_total`'s signature.
use shove::broker::Broker;
use shove::codec::JsonCodec;
use shove::handler::MessageHandler;
use shove::markers::Redis;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topic::Topic;
use shove::{SequenceFailure, SequencedTopic, TopologyBuilder};

// ---------------------------------------------------------------------------
// Container harness
// ---------------------------------------------------------------------------

/// RAII wrapper around the test's Redis container.
///
/// The bare testcontainers `Drop` spawns a background tokio task to remove the
/// container, which can be aborted when the test runtime tears down — leaking
/// the container whenever the test panics. This runs `rm()` synchronously on a
/// dedicated runtime in a dedicated thread so cleanup completes before scope
/// exit, including on unwind from a failed assertion. Mirrors the equivalent
/// wrapper in `tests/redis_integration.rs`.
struct ContainerOnDrop(Option<testcontainers::ContainerAsync<RedisContainer>>);

impl Drop for ContainerOnDrop {
    fn drop(&mut self) {
        let Some(container) = self.0.take() else {
            return;
        };
        let handle = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async move {
                let _ = container.rm().await;
            });
        });
        let _ = handle.join();
    }
}

/// Start a Redis container and return it alongside its URL.
async fn start_redis() -> (ContainerOnDrop, String) {
    let container = RedisContainer::default()
        .with_tag("7.0")
        .start()
        .await
        .expect("start Redis container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("container Redis port");
    (
        ContainerOnDrop(Some(container)),
        format!("redis://{host}:{port}/"),
    )
}

/// Connect with a bounded retry loop — testcontainers can return before Redis
/// is actually accepting connections.
async fn connect_with_retry(url: &str, group: &str) -> Broker<Redis> {
    let start = std::time::Instant::now();
    let mut last_err: Option<shove::ShoveError> = None;
    while start.elapsed() < Duration::from_secs(30) {
        match Broker::<Redis>::new(
            RedisConfig::new(RedisMode::Standalone {
                url: url.to_owned(),
            })
            .with_group(group),
        )
        .await
        {
            Ok(broker) => return broker,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    panic!(
        "connect to Redis at {url}: {}",
        last_err.expect("at least one error before the timeout")
    );
}

// ---------------------------------------------------------------------------
// Topic
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerEntry {
    account: String,
}

struct Ledger;

impl Topic for Ledger {
    type Message = LedgerEntry;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-metrics-seq")
                // `Skip` rather than `FailAll`: this test is about the two
                // terminal decisions being counted once each, not about
                // cascade accounting (covered for the hand-rolled backends in
                // `metrics_inmemory_sequenced.rs`). `Skip` keeps the two keys
                // independent so each counter has one unambiguous source.
                .sequenced(SequenceFailure::Skip)
                // One shard: both keys land on the same consumer, so the run
                // is deterministic regardless of how the keys hash.
                .routing_shards(1)
                .hold_queue(Duration::from_millis(50))
                .dlq()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(Ledger::sequence_key);
}

impl SequencedTopic for Ledger {
    fn sequence_key(msg: &LedgerEntry) -> String {
        msg.account.clone()
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Counters {
    /// Handler invocations for the key whose message is rejected outright.
    reject_calls: Arc<AtomicU32>,
    /// Handler invocations for the key that exhausts its retry budget. With
    /// `max_retries = 1` the documented contract is 1 initial attempt + 1
    /// retry, so this settles at 2.
    retry_calls: Arc<AtomicU32>,
}

struct Handler;

impl MessageHandler<Ledger> for Handler {
    type Context = Counters;
    async fn handle(&self, msg: LedgerEntry, _meta: MessageMetadata, ctx: &Counters) -> Outcome {
        if msg.account == "acct-reject" {
            ctx.reject_calls.fetch_add(1, Ordering::SeqCst);
            Outcome::Reject
        } else {
            ctx.retry_calls.fetch_add(1, Ordering::SeqCst);
            Outcome::Retry
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot helper
// ---------------------------------------------------------------------------

type Snapshot = HashMap<
    metrics_util::CompositeKey,
    (
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    ),
>;

/// Sum `shove_messages_failed_total` across every series whose `reason` label
/// matches, so the assertion is on the number the operator actually alerts on.
fn failed_total(snapshot: &Snapshot, reason: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_failed_total")
        .filter(|(k, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "reason" && l.value() == reason)
        })
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("shove_messages_failed_total is not a counter: {other:?}"),
        })
        .sum()
}

/// `XLEN` on the DLQ stream. Used to wait for outcome routing to actually
/// land: the handler returning `Reject` happens *before* `record_failed`, so
/// gating the snapshot on handler counters alone would race.
async fn dlq_len(conn: &mut MultiplexedConnection, dlq: &str) -> u64 {
    redis::cmd("XLEN")
        .arg(dlq)
        .query_async(conn)
        .await
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn sequenced_reject_and_retry_exhaustion_are_each_counted_once() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let (_container, url) = start_redis().await;
    let broker = connect_with_retry(&url, "redis-metrics-seq-grp").await;
    broker
        .topology()
        .declare::<Ledger>()
        .await
        .expect("declare");

    let dlq = Ledger::topology().dlq().expect("topic declares a DLQ");
    let raw = redis::Client::open(url.as_str()).expect("raw redis client");
    let mut probe = raw
        .get_multiplexed_async_connection()
        .await
        .expect("probe connection");

    // Publish after `declare` — the consumer group is created at `$`, so
    // anything published earlier would never be delivered.
    let publisher = broker.publisher().await.expect("publisher");
    publisher
        .publish::<Ledger>(&LedgerEntry {
            account: "acct-reject".into(),
        })
        .await
        .expect("publish reject-key message");
    publisher
        .publish::<Ledger>(&LedgerEntry {
            account: "acct-retry".into(),
        })
        .await
        .expect("publish retry-key message");

    let ctx = Counters {
        reject_calls: Arc::new(AtomicU32::new(0)),
        retry_calls: Arc::new(AtomicU32::new(0)),
    };
    let mut group = broker.consumer_group().with_context(ctx.clone());
    group
        .register_fifo::<Ledger, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::default().with_max_retries(1)),
            || Handler,
        )
        .await
        .expect("register_fifo");

    // Both messages are dead-lettered, so a DLQ depth of 2 is the signal that
    // every `record_failed` for this run has already fired.
    let token = group.cancellation_token();
    let waiter = tokio::spawn(async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if dlq_len(&mut probe, dlq).await >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        token.cancel();
    });
    let outcome = group
        .run_until_timeout(std::future::pending::<()>(), Duration::from_secs(10))
        .await;
    waiter.await.expect("dlq waiter");
    assert!(outcome.is_clean(), "supervisor exited cleanly: {outcome:?}");

    let snapshot = snapshotter.snapshot().into_hashmap();

    assert_eq!(
        ctx.reject_calls.load(Ordering::SeqCst),
        1,
        "the rejected key must reach the handler exactly once"
    );
    assert_eq!(
        ctx.retry_calls.load(Ordering::SeqCst),
        2,
        "max_retries = 1 means 1 initial attempt + 1 retry"
    );

    assert_eq!(
        failed_total(&snapshot, "rejected"),
        1,
        "a handler-returned Reject on the sequenced path must increment \
         shove_messages_failed_total{{reason=\"rejected\"}} exactly once"
    );
    assert_eq!(
        failed_total(&snapshot, "max_retries_exceeded"),
        1,
        "retry-budget exhaustion on the sequenced path must increment \
         shove_messages_failed_total{{reason=\"max_retries_exceeded\"}} \
         exactly once — the intermediate hold-queue round trip is not a \
         failure and must not be counted"
    );
}
