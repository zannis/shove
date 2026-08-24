#![cfg(all(feature = "redis-streams", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: a `SequenceFailure::FailAll` cascade on a Redis Streams
//! topic that declares **no DLQ** must move `shove_messages_discarded_total`
//! once per message that is gone — including the cascaded ones — while still
//! counting exactly **one** failure.
//!
//! This is the other half of `metrics_redis_failall.rs`. Both drive the same
//! poisoned-key arm in `src/backends/redis/consumer.rs`, whose accounting hangs
//! off one flag:
//!
//! ```ignore
//! let pending = metrics::pending_discard(.., topology.dlq().is_some());
//! ```
//!
//! `metrics_redis_failall.rs` declares a DLQ, so it only ever exercises the
//! `true` arm — where the pending discard settles against a DLQ retirement and
//! `shove_messages_discarded_total` correctly stays at zero. The `false` arm,
//! where `route_to_dlq` finds no DLQ and the bare `XACK` that retires a cascaded
//! entry is the thing that loses it, had no end-to-end coverage on this backend
//! at all.
//!
//! That arm is the quietest possible regression: messages vanish and no counter
//! moves. The `FailAll` cascade here has already drifted three times (#73
//! introduced it, #86 fixed the count, #90 pinned the DLQ-settled half), and
//! each time the drift went unnoticed because nothing in CI could catch it.
//!
//! # Why the assertion is exact rather than incidental
//!
//! With no DLQ there is nothing to wait *on*: `metrics_redis_failall.rs` gates
//! its snapshot on `XLEN` of the DLQ stream precisely because the handler
//! returns `Reject` *before* the accounting runs, so handler counters alone
//! would race the emission. A cascaded message here reaches neither the handler
//! nor any stream, and `Snapshotter::snapshot()` *drains* every counter it
//! reads, so the metric cannot be polled for progress either.
//!
//! So the fixture buys a barrier with `routing_shards(1)`: one shard stream, one
//! shard task, and a non-concurrent FIFO loop that settles each entry's outcome
//! before reading the next — so consume order == publish order and the barrier
//! is behind the accounting rather than racing it. `acct-A/0` is rejected and
//! poisons `acct-A`, `acct-A/1` and `acct-A/2` are cascade-discarded behind it,
//! and `acct-B/0` is published last. `acct-B/0` reaching the handler therefore
//! *proves* all three `acct-A` entries were already settled — and that the
//! consumer survived the poisoning, so the counters cannot be satisfied by a
//! consumer that stopped.
//!
//! The deviation from that file's `routing_shards(2)` is deliberate and load
//! bearing: with two shards `acct-B` is on its own shard task and says nothing
//! about `acct-A`'s progress. Raising it re-introduces the race.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot and whose `snapshot()` *drains* every counter it reads. Hence
//! its own integration binary, a single `#[test]`, and exactly one snapshot
//! taken after the consumer group has stopped.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
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

struct FailAllNoDlqTopic;
impl Topic for FailAllNoDlqTopic {
    type Message = Event;
    type Codec = JsonCodec;
    fn topology() -> &'static shove::QueueTopology {
        static T: std::sync::OnceLock<shove::QueueTopology> = std::sync::OnceLock::new();
        // Bare topology: no DLQ, no hold queues. `routing_shards(1)` is what
        // makes the consume order deterministic — see the module doc.
        T.get_or_init(|| {
            TopologyBuilder::new("redis-metrics-failall-no-dlq")
                .sequenced(SequenceFailure::FailAll)
                .routing_shards(1)
                .build()
        })
    }
    const SEQUENCE_KEY_FN: Option<fn(&Self::Message) -> String> =
        Some(FailAllNoDlqTopic::sequence_key);
}
impl SequencedTopic for FailAllNoDlqTopic {
    fn sequence_key(msg: &Event) -> String {
        msg.account.clone()
    }
}

/// Counts handler invocations per key. `acct-A` must stay at 1 (the rejected
/// seq 0) — the cascaded entries never reach the handler.
#[derive(Clone)]
struct Counters {
    acct_a_calls: Arc<AtomicU32>,
    acct_b_calls: Arc<AtomicU32>,
}

struct Handler(Counters);
impl MessageHandler<FailAllNoDlqTopic> for Handler {
    type Context = ();
    async fn handle(&self, msg: Event, _meta: MessageMetadata, _: &()) -> Outcome {
        if msg.account == "acct-B" {
            self.0.acct_b_calls.fetch_add(1, Ordering::SeqCst);
        } else {
            self.0.acct_a_calls.fetch_add(1, Ordering::SeqCst);
        }
        if msg.account == "acct-A" && msg.seq == 0 {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot helpers
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

/// Every `shove_messages_discarded_total` series in the snapshot, as
/// `(reason, count)` pairs.
///
/// Kept as pairs rather than a bare sum so a wrong total names the reason label
/// that produced it, which points straight at the call site.
fn discarded_series(snapshot: &Snapshot) -> Vec<(String, u64)> {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_discarded_total")
        .map(|(k, (_, _, value))| {
            let reason = k
                .key()
                .labels()
                .find(|l| l.key() == "reason")
                .map_or_else(|| "<unlabelled>".to_string(), |l| l.value().to_string());
            match value {
                DebugValue::Counter(n) => (reason, *n),
                other => panic!("shove_messages_discarded_total is not a counter: {other:?}"),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn failall_cascade_with_no_dlq_counts_every_message_as_discarded() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let (_container, url) = start_redis().await;
    let broker = connect_with_retry(&url, "redis-metrics-failall-no-dlq-grp").await;
    broker
        .topology()
        .declare::<FailAllNoDlqTopic>()
        .await
        .expect("declare");
    assert!(
        FailAllNoDlqTopic::topology().dlq().is_none(),
        "this fixture is the no-DLQ arm; a DLQ here would silently make the \
         discard assertion below read zero"
    );

    // Publish after `declare` — the consumer group is created at `$`, so
    // anything published earlier would never be delivered.
    //
    // Publish order is consume order (one shard stream). acct-A/0 poisons the
    // key, acct-A/1 and acct-A/2 cascade behind it, and acct-B/0 is the barrier.
    let publisher = broker.publisher().await.expect("publisher");
    for seq in 0..3u64 {
        publisher
            .publish::<FailAllNoDlqTopic>(&Event {
                account: "acct-A".into(),
                seq,
            })
            .await
            .expect("publish acct-A");
    }
    publisher
        .publish::<FailAllNoDlqTopic>(&Event {
            account: "acct-B".into(),
            seq: 0,
        })
        .await
        .expect("publish acct-B");

    let ctx = Counters {
        acct_a_calls: Arc::new(AtomicU32::new(0)),
        acct_b_calls: Arc::new(AtomicU32::new(0)),
    };
    let handler_ctx = ctx.clone();
    let mut group = broker.consumer_group();
    group
        .register_fifo::<FailAllNoDlqTopic, _>(
            ConsumerGroupConfig::new(RedisConsumerGroupConfig::default().with_max_retries(0)),
            move || Handler(handler_ctx.clone()),
        )
        .await
        .expect("register_fifo");

    // The barrier. `acct-B/0` is last on a single shard stream drained by a
    // non-concurrent FIFO loop, so its handler call proves acct-A/0's reject and
    // both cascade discards have already been settled.
    let probe_ctx = ctx.clone();
    let signal = async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if probe_ctx.acct_b_calls.load(Ordering::SeqCst) >= 1 {
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

    assert_eq!(
        ctx.acct_b_calls.load(Ordering::SeqCst),
        1,
        "acct-B must be handled after the poisoned acct-A backlog drains; the \
         consumer either stopped at the poisoning or never got there"
    );
    assert_eq!(
        ctx.acct_a_calls.load(Ordering::SeqCst),
        1,
        "only the rejected acct-A/0 reaches the handler; the cascaded entries \
         must not"
    );

    // All three acct-A entries are gone and the topology declares nowhere for
    // them to go, so every one of them is data loss: the rejected delivery
    // through `record_terminal` and the two cascaded ones through
    // `pending_discard`, each settled by the `XACK` that dropped it.
    let discarded = discarded_series(&snapshot);
    let discarded_total: u64 = discarded.iter().map(|(_, n)| *n).sum();
    assert_eq!(
        discarded_total, 3,
        "a no-DLQ FailAll cascade loses the rejected entry and everything behind \
         its key — all 3 must be counted as discarded; got {discarded:?}"
    );

    // The rejected delivery is an independent failure — counted once. The two
    // discarded behind the poisoned key are collateral of that same failure.
    // Counting them would read 3.
    assert_eq!(
        failed_total(&snapshot, "rejected"),
        1,
        "expected exactly one `rejected` failure (acct-A/0, the delivery that \
         actually reached the handler and failed); acct-A/1 and acct-A/2 were \
         discarded as a FailAll cascade and must not be counted"
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
