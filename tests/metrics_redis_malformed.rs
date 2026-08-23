#![cfg(all(feature = "redis-streams", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: `shove_messages_failed_total{reason="malformed"}` on the
//! Redis Streams consumer path.
//!
//! # What is being asserted
//!
//! A stream entry with no `payload` field is not a `shove` message at all —
//! it points at a foreign writer, or a publisher that is not using `shove`.
//! The consumer cannot hand it to a handler and cannot deserialize it, so it
//! XACKs it out of the group and counts it as `FailReason::Malformed`
//! (`backends/redis/consumer.rs`, the `fields.remove(PAYLOAD_FIELD)` `None`
//! arm). Note this is distinct from `deserialize`: there the payload was
//! present and the codec rejected it.
//!
//! Two properties:
//!
//! 1. **Exactly once.** One payload-less entry produces exactly one
//!    `malformed` increment.
//! 2. **Not counted again on the reaper's redelivery path.** This is the one
//!    worth the container. The counter is deliberately incremented *only on a
//!    successful XACK*:
//!
//!    ```ignore
//!    match xack(&mut conn, stream, &group, &entry_id).await {
//!        Ok(()) => metrics::record_failed(.., FailReason::Malformed),
//!        Err(e) => { /* backend_error, entry stays in the PEL */ }
//!    }
//!    ```
//!
//!    That ordering matters because of the maintenance sidecar in
//!    `backends/redis/reaper.rs`: it runs XAUTOCLAIM over the group's PEL and
//!    **XADDs every claimed entry back to the stream** as a fresh entry, then
//!    XACKs the original. A payload-less entry redelivered that way would land
//!    in the same `None` arm a second time. So had the counter been
//!    incremented before the XACK — or on the error branch too — a single
//!    corrupt entry could be counted once per reaper cycle, forever.
//!
//!    The test asserts the property the code relies on to make that
//!    impossible: after processing, the entry is gone from the PEL, so there
//!    is nothing for XAUTOCLAIM to claim and the re-XADD path cannot fire. It
//!    checks this by issuing XAUTOCLAIM itself, with `min-idle-time 0` — the
//!    same command `autoclaim_all` issues, with the idle threshold removed so
//!    it is strictly more eager than the real sidecar ever is — and requiring
//!    that it claims nothing. A count of 1 alongside an empty claim is the
//!    no-double-count property; the alternative implementation would leave the
//!    entry claimable and read 2 or more over time.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot, and whose `snapshot()` *drains* every counter it reads. So:
//! own integration binary, a single `#[test]`, and exactly one snapshot taken
//! at the end — progress is waited on through the handler counter, never by
//! peeking at the metrics.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisContainer};
use tokio_util::sync::CancellationToken;

use shove::redis::{RedisConfig, RedisConsumer, RedisMode};
// Imported item by item rather than through `shove::*`: the glob shadows the
// `metrics` crate this file names directly in `failed_total`'s signature.
use shove::{
    Backend, Broker, ConsumerOptions, MessageHandler, MessageMetadata, Outcome, Redis, Topic,
    TopologyBuilder, define_topic,
};

/// Consumer group for this test's stream.
const GROUP: &str = "metrics-malformed-grp";

// ---------------------------------------------------------------------------
// Topic and handler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Event {
    id: u32,
}

define_topic!(
    Events,
    Event,
    TopologyBuilder::new("metrics-malformed-redis")
        .dlq()
        .build()
);

/// Counts handler invocations. Used purely as the progress signal: the
/// sentinel message is published *after* the corrupt entry, and the consumer
/// processes a batch in order, so the handler seeing the sentinel proves the
/// corrupt entry has already been through the `None` arm.
#[derive(Clone)]
struct Counters {
    calls: Arc<AtomicU32>,
}

struct Handler;
impl MessageHandler<Events> for Handler {
    type Context = Counters;
    async fn handle(&self, _msg: Event, _meta: MessageMetadata, ctx: &Counters) -> Outcome {
        ctx.calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Ack
    }
}

/// Sum `shove_messages_failed_total` across every series whose `reason` label
/// matches, so the assertion is on the number the operator actually alerts on.
fn failed_total(
    snapshot: &std::collections::HashMap<
        metrics_util::CompositeKey,
        (
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        ),
    >,
    reason: &str,
) -> u64 {
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

/// Poll XPENDING until the group has nothing outstanding, or give up.
///
/// Reply is `[count, min-id, max-id, consumers]`; only the count is read.
async fn wait_for_empty_pel(
    conn: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let reply: redis::Value = redis::cmd("XPENDING")
            .arg(stream)
            .arg(GROUP)
            .query_async(conn)
            .await
            .expect("XPENDING");
        let pending = match &reply {
            redis::Value::Array(parts) => match parts.first() {
                Some(redis::Value::Int(n)) => *n,
                // An empty PEL answers with a nil-ish head on some versions.
                _ => 0,
            },
            redis::Value::Nil => 0,
            other => panic!("unexpected XPENDING reply: {other:?}"),
        };
        if pending == 0 {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_handler(ctx: &Counters, target: u32, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if ctx.calls.load(Ordering::SeqCst) >= target {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn payload_less_entry_is_counted_once_and_left_unclaimable() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    // `7.0`, matching `redis_integration.rs`: the testcontainers module still
    // defaults to Redis 5.0.14, which `shove` rejects at connect (it requires
    // 6.2+), and XAUTOCLAIM below is itself a 6.2 command.
    let container = RedisContainer::default()
        .with_tag("7.0")
        .start()
        .await
        .expect("failed to start Redis container");
    let host = container.get_host().await.expect("host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("port");
    let url = format!("redis://{host}:{port}");

    let cfg = RedisConfig::new(RedisMode::Standalone { url: url.clone() }).with_group(GROUP);
    let client = <Redis as Backend>::connect(cfg).await.expect("connect");
    let broker = Broker::<Redis>::from_client(client.clone());
    broker
        .topology()
        .declare::<Events>()
        .await
        .expect("declare topology");

    let stream = Events::topology().queue();

    // A second, plain connection for the raw stream surgery this test needs:
    // writing an entry `shove`'s publisher cannot produce, and later asking
    // Redis directly what is left in the PEL.
    let raw = redis::Client::open(url.as_str()).expect("open redis client");
    let mut raw_conn = raw
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection");

    // The corrupt entry: a well-formed stream entry with a field that is not
    // `payload` (the field `backends::redis::constants::PAYLOAD_FIELD` names,
    // which the consumer looks up before anything else). `shove`'s publisher
    // always writes it, so this models a foreign writer on the same stream —
    // exactly what `FailReason::Malformed` is for.
    let corrupt_id: String = redis::cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("not-the-payload-field")
        .arg("{}")
        .query_async(&mut raw_conn)
        .await
        .expect("XADD corrupt entry");

    // The sentinel, published through `shove` so it is a valid message, and
    // published *after* the corrupt entry so that its arrival at the handler
    // orders the corrupt entry strictly before it.
    broker
        .publisher()
        .await
        .expect("publisher")
        .publish::<Events>(&Event { id: 1 })
        .await
        .expect("publish sentinel");

    let counters = Counters {
        calls: Arc::new(AtomicU32::new(0)),
    };
    let shutdown = CancellationToken::new();
    let consumer = RedisConsumer::new(client.clone());
    let handler_ctx = counters.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        let opts = ConsumerOptions::<Redis>::new().with_shutdown(s);
        consumer.run::<Events, _>(Handler, handler_ctx, opts).await
    });

    assert!(
        wait_for_handler(&counters, 1, Duration::from_secs(30)).await,
        "timed out waiting for the sentinel to reach the handler; without it \
         there is no proof the corrupt entry was processed at all"
    );

    // The handler counter ticks *inside* the handler, before the consumer
    // XACKs the sentinel, so cancelling on that signal alone could stop the
    // consumer with the sentinel still pending and make the PEL check below
    // fail for a reason that has nothing to do with the corrupt entry. Wait
    // for the group's PEL to drain first. The corrupt entry is XACKed strictly
    // before the sentinel — it is the earlier stream id and this consumer is
    // the sequential loop — so an empty PEL means both are acked.
    assert!(
        wait_for_empty_pel(&mut raw_conn, stream, Duration::from_secs(30)).await,
        "timed out waiting for the pending-entries list to drain; entries left \
         in the PEL would be re-XADDed by the reaper"
    );

    shutdown.cancel();
    let _ = consume_handle.await;

    // (2) Nothing is left for the reaper to claim, so its re-XADD redelivery
    // path — the only way this entry could be counted a second time — cannot
    // fire. `min-idle-time 0` makes this strictly more eager than the real
    // sidecar, which uses the handler timeout as its threshold.
    let claimed: redis::Value = redis::cmd("XAUTOCLAIM")
        .arg(stream)
        .arg(GROUP)
        .arg("assert-nothing-reclaimable")
        .arg(0i64)
        .arg("0-0")
        .query_async(&mut raw_conn)
        .await
        .expect("XAUTOCLAIM");
    let reclaimable = claimed_entry_count(&claimed);
    assert_eq!(
        reclaimable, 0,
        "the corrupt entry {corrupt_id} (or the sentinel) is still in the PEL \
         and would be re-XADDed by the reaper, producing a second `malformed` \
         count for one entry; XAUTOCLAIM claimed {reclaimable} entries"
    );

    // Single, draining snapshot — taken only once the consumer has stopped, so
    // nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    // (1) Exactly once.
    assert_eq!(
        failed_total(&snapshot, "malformed"),
        1,
        "expected exactly one `malformed` failure for the single payload-less \
         entry, got {}",
        failed_total(&snapshot, "malformed")
    );

    // The sentinel was a perfectly good message: it must not have been counted
    // as anything, which also rules out the corrupt entry having poisoned the
    // batch around it.
    assert_eq!(
        failed_total(&snapshot, "deserialize"),
        0,
        "a missing `payload` field must be reported as `malformed`, not \
         `deserialize` — the two point at different faults"
    );
    assert_eq!(
        counters.calls.load(Ordering::SeqCst),
        1,
        "only the sentinel may reach the handler; the corrupt entry must be \
         retired before dispatch"
    );
}

/// Number of entries XAUTOCLAIM actually claimed.
///
/// The reply is `[next-cursor, [entries...], [deleted-ids...]]` on Redis 7+
/// and `[next-cursor, [entries...]]` on 6.2; only element 1 is read, so both
/// shapes work.
fn claimed_entry_count(reply: &redis::Value) -> usize {
    match reply {
        redis::Value::Array(parts) => match parts.get(1) {
            Some(redis::Value::Array(entries)) => entries.len(),
            _ => panic!("unexpected XAUTOCLAIM reply shape: {reply:?}"),
        },
        other => panic!("unexpected XAUTOCLAIM reply: {other:?}"),
    }
}
