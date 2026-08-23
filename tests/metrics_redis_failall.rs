#![cfg(all(feature = "redis-streams", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: a `SequenceFailure::FailAll` cascade on Redis Streams must
//! move `shove_messages_failed_total` **once**, not once per dead-lettered
//! message.
//!
//! `metrics::FailReason` states the rule: only the delivery that actually
//! failed is counted. The messages dead-lettered behind a poisoned sequence key
//! are collateral of that one already-counted failure, so counting them scales
//! the counter by the queue depth behind the key and makes it useless for the
//! alerting it exists to support.
//!
//! Nothing in CI could catch a violation of that rule here. #73 brought
//! `FailAll` to Redis with a `record_failed(.., Rejected)` on the poisoned-key
//! arm, and it survived review and merge because the rule was pinned by exactly
//! one in-memory test. #86 removed the call, and the poisoned-key arm in
//! `src/backends/redis/consumer.rs` now takes a `metrics::pending_discard`
//! record and nothing else — the discard half of the accounting, which stays
//! inert while a DLQ exists, with no failure counted either way. This test is
//! what makes putting a `record_failed` back go red.
//!
//! # Why the assertion is exact rather than incidental
//!
//! The fixture is the one `fifo_failall_poisons_same_key_after_reject` in
//! `redis_integration.rs` already uses, because its shape is precisely the
//! scenario the rule is about: five messages on `acct-A`, the third rejected,
//! so `acct-A/3` and `acct-A/4` are dead-lettered without ever reaching the
//! handler. Waiting for `XLEN` on the DLQ stream to reach **three** is what
//! gives the counter assertion teeth — it proves two cascade discards actually
//! happened, so a counter still reading 1 proves neither was counted. Count the
//! cascade site and this reads 3.
//!
//! `acct-B` rides along for the same reason it does in the original: it proves
//! the consumer kept running past the poisoning, so `rejected == 1` cannot be
//! satisfied by a consumer that simply stopped.
//!
//! The DLQ stream — rather than the handler counters — is what the snapshot is
//! gated on: the handler returns `Reject` *before* `record_failed` runs, so
//! waiting on handler counts alone would race the emission this asserts.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot and whose `snapshot()` *drains* every counter it reads. Hence
//! its own integration binary, a single `#[test]`, and exactly one snapshot
//! taken after the consumer group has stopped.

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
// `metrics` crate this file names directly in `Snapshot`.
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

async fn start_redis() -> (ContainerOnDrop, String) {
    let container = RedisContainer::default()
        .with_tag("7.0")
        .start()
        .await
        .expect("failed to start Redis container");
    let host = container.get_host().await.expect("failed to get host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("failed to get Redis port");
    let url = format!("redis://{host}:{port}/");
    (ContainerOnDrop(Some(container)), url)
}

/// Connect with a bounded retry loop: testcontainers can return before Redis is
/// actually bound.
async fn connect_with_retry(url: &str, group: &str) -> Broker<Redis> {
    let start = std::time::Instant::now();
    let budget = Duration::from_secs(30);
    let mut last_err: Option<shove::ShoveError> = None;
    while start.elapsed() < budget {
        match Broker::<Redis>::new(
            RedisConfig::new(RedisMode::Standalone {
                url: url.to_owned(),
            })
            .with_group(group),
        )
        .await
        {
            Ok(b) => return b,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    panic!(
        "connect to Redis at {url} after {budget:?}: {}",
        last_err.expect("must have at least one error before timeout")
    );
}

// ---------------------------------------------------------------------------
// Topic and handler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    account: String,
    seq: u64,
}

struct FailAllTopic;
impl Topic for FailAllTopic {
    type Message = Event;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        T.get_or_init(|| {
            TopologyBuilder::new("redis-metrics-failall")
                .sequenced(SequenceFailure::FailAll)
                .routing_shards(2)
                .hold_queue(Duration::from_millis(100))
                .dlq()
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> = Some(FailAllTopic::sequence_key);
}
impl SequencedTopic for FailAllTopic {
    fn sequence_key(msg: &Event) -> String {
        msg.account.clone()
    }
}

/// Counts handler invocations per key. `acct-A` must stay at 3 (seqs 0, 1 and
/// the rejected 2) — the cascaded deliveries never reach the handler.
#[derive(Clone)]
struct Counters {
    acct_a_calls: Arc<AtomicU32>,
    acct_b_calls: Arc<AtomicU32>,
}

struct Handler(Counters);
impl MessageHandler<FailAllTopic> for Handler {
    type Context = ();
    async fn handle(&self, msg: Event, _meta: MessageMetadata, _: &()) -> Outcome {
        if msg.account == "acct-B" {
            self.0.acct_b_calls.fetch_add(1, Ordering::SeqCst);
        } else {
            self.0.acct_a_calls.fetch_add(1, Ordering::SeqCst);
        }
        if msg.account == "acct-A" && msg.seq == 2 {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot helper
// ---------------------------------------------------------------------------

type Snapshot = std::collections::HashMap<
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

/// `XLEN` on the DLQ stream. Used to wait for outcome routing to actually land:
/// the handler returns `Reject` *before* `record_failed`, so gating on handler
/// counters alone would race.
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
async fn failall_cascade_is_counted_once_not_once_per_dead_letter() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let (_container, url) = start_redis().await;
    let broker = connect_with_retry(&url, "redis-metrics-failall-grp").await;
    broker
        .topology()
        .declare::<FailAllTopic>()
        .await
        .expect("declare");
    let dlq = FailAllTopic::topology().dlq().expect("topic has a DLQ");

    // Publish after `declare` — the consumer group is created at `$`, so
    // anything published earlier would never be delivered.
    let publisher = broker.publisher().await.expect("publisher");
    for seq in 0..5u64 {
        publisher
            .publish::<FailAllTopic>(&Event {
                account: "acct-A".into(),
                seq,
            })
            .await
            .expect("publish acct-A");
    }
    for seq in 0..3u64 {
        publisher
            .publish::<FailAllTopic>(&Event {
                account: "acct-B".into(),
                seq,
            })
            .await
            .expect("publish acct-B");
    }

    let ctx = Counters {
        acct_a_calls: Arc::new(AtomicU32::new(0)),
        acct_b_calls: Arc::new(AtomicU32::new(0)),
    };
    let handler_ctx = ctx.clone();
    let mut group = broker.consumer_group();
    group
        .register_fifo::<FailAllTopic, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::default().with_max_retries(0)),
            move || Handler(handler_ctx.clone()),
        )
        .await
        .expect("register_fifo");

    // acct-A/2 is rejected, then acct-A/3 and acct-A/4 are dead-lettered
    // without reaching the handler → exactly 3 entries in the DLQ. acct-B is on
    // its own shard task and must be handled normally, so wait for both rather
    // than assuming an interleaving.
    let mut probe = redis::Client::open(url.as_str())
        .expect("raw redis client")
        .get_multiplexed_async_connection()
        .await
        .expect("probe connection");
    let probe_ctx = ctx.clone();
    let signal = async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if dlq_len(&mut probe, dlq).await >= 3
                && probe_ctx.acct_b_calls.load(Ordering::SeqCst) >= 3
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };

    let outcome = group
        .run_until_timeout(signal, Duration::from_secs(10))
        .await;
    assert!(outcome.is_clean(), "supervisor exited cleanly: {outcome:?}");

    // Single, draining snapshot, taken only once the group has stopped so
    // nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    // Re-read the DLQ on a fresh connection: reaching 3 is the proof that two
    // cascade discards happened, which is what makes the counter assertion
    // below exact rather than incidental.
    let mut raw = redis::Client::open(url.as_str())
        .expect("raw redis client")
        .get_multiplexed_async_connection()
        .await
        .expect("raw connection");
    assert_eq!(
        dlq_len(&mut raw, dlq).await,
        3,
        "expected acct-A/2 plus the two poisoned messages in the DLQ"
    );

    assert_eq!(
        ctx.acct_a_calls.load(Ordering::SeqCst),
        3,
        "acct-A/0, acct-A/1 and the rejected acct-A/2 reach the handler; the \
         cascaded deliveries must not"
    );
    assert_eq!(
        ctx.acct_b_calls.load(Ordering::SeqCst),
        3,
        "acct-B must be unaffected by acct-A's poisoning"
    );

    // The rejected delivery is an independent failure — counted once. The two
    // dead-lettered behind the poisoned key are collateral of that same
    // failure. Counting them would read 3.
    assert_eq!(
        failed_total(&snapshot, "rejected"),
        1,
        "expected exactly one `rejected` failure (acct-A/2, the delivery that \
         actually reached the handler and failed); acct-A/3 and acct-A/4 were \
         dead-lettered as a FailAll cascade and must not be counted"
    );

    // Nothing in this fixture exhausts a retry budget (`max_retries(0)`, and
    // every non-poison delivery Acks), so a non-zero reading here would mean
    // the cascade was counted under a different reason rather than suppressed.
    assert_eq!(
        failed_total(&snapshot, "max_retries_exceeded"),
        0,
        "no delivery in this fixture exhausts a retry budget"
    );
}
