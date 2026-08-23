#![cfg(all(feature = "rabbitmq", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: `shove_messages_failed_total{reason="sequence_timeout"}`
//! on the RabbitMQ hold-queue eviction path.
//!
//! # What is being asserted
//!
//! A sequence key enters `AwaitingRetry` when its handler returns `Retry`: the
//! message goes to a shard hold queue and the key is parked until it comes
//! back. Messages published for that key in the meantime are buffered
//! *in-process*, in `pending_deliveries`. If the key is still parked after
//! `hold_queue_timeout`, the eviction arm
//! (`backends/rabbitmq/consumer.rs`, the `eviction_ticker` branch) gives up on
//! it: it drops the key state and dead-letters every buffered delivery,
//! counting each one as `FailReason::SequenceTimeout`.
//!
//! Two properties, and the second is the one worth the container:
//!
//! 1. **Once per buffered message.** Two messages buffered behind the stuck
//!    key produce exactly two `sequence_timeout` increments.
//! 2. **The held message is not counted.** The message that actually caused
//!    the stall is sitting in the broker-side hold queue, untouched by the
//!    eviction arm — it will be redelivered when its TTL expires. Counting it
//!    would report a discard for a message that was never discarded. The hold
//!    queue TTL here is 300s against a test bounded well under that, so that
//!    message is still in the broker when the snapshot is taken: three
//!    messages published, two counted, one handler call.
//!
//! This is deliberately *not* the cascade case. See `metrics::FailReason`:
//! a `SequenceFailure::FailAll` cascade is uncounted because an
//! already-counted failure accounts for it, whereas here nothing else is
//! counted — the `Retry` that parked the key is not a failure — so the
//! buffered messages are each counted individually. The topic below uses
//! `SequenceFailure::Skip` precisely so poisoning cannot contribute a single
//! increment and any `sequence_timeout` observed is unambiguous.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot, and whose `snapshot()` *drains* every counter it reads. So:
//! own integration binary, a single `#[test]`, and exactly one snapshot taken
//! at the end — progress is waited on through the handler counter and the DLQ,
//! never by peeking at the metrics.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::RabbitMq as RabbitMqMarker;
use shove::metadata::{DeadMessageMetadata, MessageMetadata};
use shove::outcome::Outcome;
use shove::rabbitmq::{RabbitMqClient, RabbitMqConfig, RabbitMqConsumer};
// Imported item by item rather than through `shove::*`: the glob shadows the
// `metrics` crate this file names directly in `failed_total`'s signature.
use shove::{SequenceFailure, SequencedTopic, TopologyBuilder, define_sequenced_topic};

use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// How long the parked message stays in the broker-side hold queue. It must
/// not be redelivered before the test finishes: coming back would clear
/// `AwaitingRetry`, and the "held message is not counted" assertion below
/// depends on that message still being in the broker at snapshot time.
///
/// Sized against the test's own worst case rather than its typical runtime:
/// the two waits below are 30s each and the settle sleep is 2s, so a maximally
/// unlucky run reaches the snapshot ~62s in. 300s leaves that a wide margin —
/// the TTL costs nothing, since the message is never waited on.
const HOLD_QUEUE_TTL: Duration = Duration::from_secs(300);

/// How long a key may sit in `AwaitingRetry` before its buffered messages are
/// evicted. The consumer polls at half this, so eviction lands within ~1.5×.
const HOLD_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);

/// Broker connection for this test.
///
/// Shared mode (the nextest `rabbitmq-setup` script exported
/// `RABBITMQ_AMQP_URL`) carves out a private vhost, so the queues this test
/// declares are unreachable from the main suite's — and, because
/// `.config/nextest.toml` sets `retries = 1`, so that a retried run starts from
/// an empty broker rather than inheriting the failed attempt's messages. Exact
/// counter assertions depend on that isolation.
struct TestContext {
    amqp_url: String,
    mgmt_url: String,
    vhost: Option<String>,
    _container: Option<testcontainers::ContainerAsync<RabbitMq>>,
}

impl TestContext {
    async fn new() -> Self {
        let http = reqwest::Client::new();

        if let Ok(base_amqp) = std::env::var("RABBITMQ_AMQP_URL") {
            let mgmt_url = std::env::var("RABBITMQ_MGMT_URL")
                .expect("RABBITMQ_MGMT_URL must be set when RABBITMQ_AMQP_URL is set");
            let vhost = format!("test-{}", uuid::Uuid::new_v4());

            let status = http
                .put(format!("{mgmt_url}/api/vhosts/{vhost}"))
                .basic_auth("guest", Some("guest"))
                .header("content-type", "application/json")
                .body("{}")
                .send()
                .await
                .expect("failed to create vhost")
                .status();
            assert!(status.is_success(), "create vhost returned {status}");

            let status = http
                .put(format!("{mgmt_url}/api/permissions/{vhost}/guest"))
                .basic_auth("guest", Some("guest"))
                .json(&std::collections::HashMap::from([
                    ("configure", ".*"),
                    ("write", ".*"),
                    ("read", ".*"),
                ]))
                .send()
                .await
                .expect("failed to set vhost permissions")
                .status();
            assert!(status.is_success(), "set permissions returned {status}");

            Self {
                amqp_url: format!("{base_amqp}/{vhost}"),
                mgmt_url,
                vhost: Some(vhost),
                _container: None,
            }
        } else {
            // Standalone mode — no setup script ran, so start a broker here.
            // The default vhost "/" is fine: the container is this test's alone.
            let container = RabbitMq::default()
                .start()
                .await
                .expect("failed to start RabbitMQ container");

            let host = container.get_host().await.expect("failed to get host");
            let amqp_port = container
                .get_host_port_ipv4(5672)
                .await
                .expect("failed to get AMQP port");
            let mgmt_port = container
                .get_host_port_ipv4(15672)
                .await
                .expect("failed to get mgmt port");

            // Sequenced topics route through a consistent-hash exchange.
            let mut result = container
                .exec(ExecCommand::new([
                    "rabbitmq-plugins",
                    "enable",
                    "rabbitmq_consistent_hash_exchange",
                ]))
                .await
                .expect("failed to enable consistent hash plugin");
            let _ = result.stdout_to_vec().await;

            // Give RabbitMQ time to load the plugin and the management API.
            tokio::time::sleep(Duration::from_secs(3)).await;

            Self {
                amqp_url: format!("amqp://guest:guest@{host}:{amqp_port}"),
                mgmt_url: format!("http://{host}:{mgmt_port}"),
                vhost: None,
                _container: Some(container),
            }
        }
    }

    fn rmq_config(&self) -> RabbitMqConfig {
        RabbitMqConfig::new(self.amqp_url.clone())
    }

    /// Drop the private vhost (shared mode only); a standalone container is
    /// reclaimed when it is dropped.
    async fn cleanup(&self) {
        let Some(vhost) = self.vhost.as_deref() else {
            return;
        };
        reqwest::Client::new()
            .delete(format!("{}/api/vhosts/{vhost}", self.mgmt_url))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .ok();
    }
}

// ---------------------------------------------------------------------------
// Topic and handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LedgerEntry {
    account: String,
    seq: u32,
}

define_sequenced_topic!(
    Ledger,
    LedgerEntry,
    |msg: &LedgerEntry| msg.account.clone(),
    TopologyBuilder::new("metrics-seq-timeout-rmq")
        .dlq()
        .hold_queue(HOLD_QUEUE_TTL)
        // `Skip`, not `FailAll`: nothing here should ever poison a key, so any
        // uncounted-cascade behaviour is out of the picture and every
        // `sequence_timeout` increment can only have come from the eviction arm.
        .sequenced(SequenceFailure::Skip)
        .routing_shards(1)
        .build()
);

/// Handler invocations. Must stay at exactly 1: the first message parks the
/// key with a `Retry` and never returns within the test's lifetime, and the
/// two messages published behind it are buffered in-process and evicted
/// without ever reaching the handler.
#[derive(Clone)]
struct Counters {
    calls: Arc<AtomicU32>,
}

struct Handler;
impl MessageHandler<Ledger> for Handler {
    type Context = Counters;
    async fn handle(&self, _msg: LedgerEntry, _meta: MessageMetadata, ctx: &Counters) -> Outcome {
        ctx.calls.fetch_add(1, Ordering::SeqCst);
        // Parks the key in `AwaitingRetry` and sends this message to the shard
        // hold queue for `HOLD_QUEUE_TTL`.
        Outcome::Retry
    }
}

/// Counts arrivals on the topic's DLQ, so the test can prove the buffered
/// deliveries really were dead-lettered rather than merely never consumed.
#[derive(Clone)]
struct DlqCounter {
    count: Arc<AtomicU32>,
    signal: Arc<tokio::sync::Notify>,
}

impl DlqCounter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU32::new(0)),
            signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn count(&self) -> u32 {
        self.count.load(Ordering::SeqCst)
    }

    async fn wait_for_count(&self, target: u32, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.count() >= target {
                return true;
            }
            tokio::select! {
                _ = self.signal.notified() => {}
                _ = tokio::time::sleep_until(deadline) => return self.count() >= target,
            }
        }
    }
}

impl MessageHandler<Ledger> for DlqCounter {
    type Context = ();
    async fn handle(&self, _msg: LedgerEntry, _meta: MessageMetadata, _: &()) -> Outcome {
        Outcome::Ack
    }
    async fn handle_dead(&self, _msg: LedgerEntry, _meta: DeadMessageMetadata, _: &()) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.signal.notify_waiters();
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

/// Total `shove_messages_failed_total` across *every* reason, so the test can
/// assert that nothing beyond the two evictions was counted without having to
/// enumerate reasons it does not expect.
fn failed_total_all_reasons(
    snapshot: &std::collections::HashMap<
        metrics_util::CompositeKey,
        (
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        ),
    >,
) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_failed_total")
        .map(|(_, (_, _, value))| match value {
            DebugValue::Counter(n) => *n,
            other => panic!("shove_messages_failed_total is not a counter: {other:?}"),
        })
        .sum()
}

/// Wait until the handler has run once, i.e. the key is parked in
/// `AwaitingRetry` and the eviction clock has started.
async fn wait_for_handler(ctx: &Counters, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if ctx.calls.load(Ordering::SeqCst) >= 1 {
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
async fn hold_queue_eviction_counts_buffered_messages_but_not_the_held_one() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let ctx = TestContext::new().await;
    let client = RabbitMqClient::connect(&ctx.rmq_config())
        .await
        .expect("connect rabbitmq");
    let broker = Broker::<RabbitMqMarker>::from_client(client.clone());
    broker
        .topology()
        .declare::<Ledger>()
        .await
        .expect("declare topology");

    // All three share one sequence key, and are published before the consumer
    // starts so all three are on the shard queue when the first is dispatched.
    // The first parks the key; the other two are buffered behind it — whether
    // they land in `pending_deliveries` via the `InFlight` arm (handler still
    // running) or the `AwaitingRetry` arm (handler already returned `Retry`)
    // depends on scheduling, and both arms buffer, so the assertions hold
    // either way.
    let publisher = broker.publisher().await.expect("publisher");
    for seq in 0..3u32 {
        publisher
            .publish::<Ledger>(&LedgerEntry {
                account: "acct-stuck".into(),
                seq,
            })
            .await
            .expect("publish message");
    }

    let counters = Counters {
        calls: Arc::new(AtomicU32::new(0)),
    };

    // The DLQ consumer runs alongside the main one so the evictions are
    // observed as they happen, rather than after shutdown.
    let dlq_counter = DlqCounter::new();
    let dlq_consumer = RabbitMqConsumer::new(client.clone());
    let dh = dlq_counter.clone();
    let dlq_handle = tokio::spawn(async move { dlq_consumer.run_dlq::<Ledger, _>(dh, ()).await });

    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let handler_ctx = counters.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        // `prefetch_count(10)` so all three deliveries are pushed to this
        // consumer and the two behind the parked key are actually buffered
        // in-process — that in-process buffer is what the eviction arm drains.
        //
        // `max_retries(5)` keeps the retry budget well clear of the single
        // `Retry` issued here, so nothing can be dead-lettered as
        // `max_retries_exceeded` and be mistaken for an eviction.
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_prefetch_count(10)
            .with_max_retries(5)
            .with_hold_queue_timeout(HOLD_QUEUE_TIMEOUT);
        consumer
            .run_fifo::<Ledger, _>(Handler, handler_ctx, opts)
            .await
    });

    assert!(
        wait_for_handler(&counters, Duration::from_secs(30)).await,
        "timed out waiting for the first message to reach the handler and park the key"
    );

    // Exactly the two buffered messages must reach the DLQ. The eviction
    // ticker polls at `HOLD_QUEUE_TIMEOUT / 2`, so this lands within ~1.5×
    // the timeout; the budget is far wider to absorb CI scheduling.
    assert!(
        dlq_counter.wait_for_count(2, Duration::from_secs(30)).await,
        "expected 2 dead-lettered messages (the two buffered behind the stuck \
         key), got {}",
        dlq_counter.count()
    );

    // Give the consumer a further beat to do anything wrong — a third
    // dead-letter, or a second eviction pass over the same key — before
    // freezing the counters. `HOLD_QUEUE_TIMEOUT` is two more ticker periods.
    tokio::time::sleep(HOLD_QUEUE_TIMEOUT).await;

    shutdown.cancel();
    consume_handle
        .await
        .expect("consumer task panicked")
        .expect("consumer returned an error");
    dlq_handle.abort();
    client.shutdown().await;
    ctx.cleanup().await;

    // Single, draining snapshot — taken only once the consumer has stopped, so
    // nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    // (1) Once per buffered message.
    assert_eq!(
        failed_total(&snapshot, "sequence_timeout"),
        2,
        "expected exactly one `sequence_timeout` per message buffered behind \
         the stuck key (2), got {}",
        failed_total(&snapshot, "sequence_timeout")
    );

    // (2) The held message is not counted. It is still in the shard hold queue
    // — `HOLD_QUEUE_TTL` is 300s, well beyond this test's worst case — so it has been
    // neither dead-lettered nor discarded, and counting it would report a
    // discard that never happened. Three published, two counted, and the
    // handler saw exactly the one that is still parked.
    assert_eq!(
        counters.calls.load(Ordering::SeqCst),
        1,
        "only the message that parked the key may reach the handler; the two \
         buffered behind it are evicted without being dispatched"
    );
    assert_eq!(
        dlq_counter.count(),
        2,
        "the held message must not be dead-lettered — it is still in the hold \
         queue awaiting its TTL"
    );

    // ...and nothing else was counted under any other reason, which is the
    // same property stated without enumerating reasons: had the eviction arm
    // also counted the held message, or had the parked `Retry` been treated as
    // a failure, this would read 3.
    assert_eq!(
        failed_total_all_reasons(&snapshot),
        2,
        "no failure beyond the two evictions may be counted: the `Retry` that \
         parked the key is not a failure, and the held message was not discarded"
    );
}
