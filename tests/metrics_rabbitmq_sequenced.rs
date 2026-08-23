#![cfg(all(feature = "rabbitmq", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: `shove_messages_failed_total` on the *sequenced* RabbitMQ
//! consumer path (`RabbitMqConsumer::run_fifo`).
//!
//! The counters this asserts were added blind — `metrics` was enabled on no
//! coverage entry but `inmemory`, so every `record_failed` call in
//! `backends/rabbitmq/consumer.rs` type-checked and never ran. This drives a
//! real broker through both instrumented sequenced sites and — just as
//! importantly — asserts that a `SequenceFailure::FailAll` cascade is *not*
//! counted. See `metrics::FailReason` for why the cascade is excluded.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot, and whose `snapshot()` *drains* every counter it reads. So:
//! own integration binary, a single `#[test]`, and exactly one snapshot taken
//! at the end — progress is waited on through handler counters and the DLQ,
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
}

define_sequenced_topic!(
    Ledger,
    LedgerEntry,
    |msg: &LedgerEntry| msg.account.clone(),
    TopologyBuilder::new("metrics-seq-rmq")
        .dlq()
        .hold_queue(Duration::from_secs(1))
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(1)
        .build()
);

#[derive(Clone)]
struct Counters {
    /// Handler invocations for the key that gets poisoned. Must stay at 1: the
    /// two deliveries queued behind it are dead-lettered without ever reaching
    /// the handler — that is the cascade.
    reject_key_calls: Arc<AtomicU32>,
    /// Handler invocations for the key that exhausts its retry budget.
    retry_key_calls: Arc<AtomicU32>,
}

struct Handler;
impl MessageHandler<Ledger> for Handler {
    type Context = Counters;
    async fn handle(&self, msg: LedgerEntry, _meta: MessageMetadata, ctx: &Counters) -> Outcome {
        if msg.account.starts_with("acct-reject") {
            ctx.reject_key_calls.fetch_add(1, Ordering::SeqCst);
            Outcome::Reject
        } else {
            ctx.retry_key_calls.fetch_add(1, Ordering::SeqCst);
            Outcome::Retry
        }
    }
}

/// Counts arrivals on the topic's DLQ, so the test can prove the cascade
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

/// Wait until both handlers have run, so the reject has poisoned its key and
/// the retry has been parked in a shard hold queue.
async fn wait_for_handlers(ctx: &Counters, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if ctx.reject_key_calls.load(Ordering::SeqCst) >= 1
            && ctx.retry_key_calls.load(Ordering::SeqCst) >= 1
        {
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
async fn sequenced_discards_are_counted_but_failall_cascades_are_not() {
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

    // Publish before the consumer starts so the ordering is deterministic: all
    // three `acct-reject` deliveries are in the shard queue when the first one
    // is dispatched, which is what makes the other two buffer behind the key
    // and cascade once it is poisoned.
    let publisher = broker.publisher().await.expect("publisher");
    for _ in 0..3 {
        publisher
            .publish::<Ledger>(&LedgerEntry {
                account: "acct-reject".into(),
            })
            .await
            .expect("publish reject-key message");
    }
    publisher
        .publish::<Ledger>(&LedgerEntry {
            account: "acct-retry".into(),
        })
        .await
        .expect("publish retry-key message");

    let counters = Counters {
        reject_key_calls: Arc::new(AtomicU32::new(0)),
        retry_key_calls: Arc::new(AtomicU32::new(0)),
    };

    // The DLQ consumer runs alongside the main one so the retry's eventual
    // dead-lettering is observed as it happens, rather than after shutdown.
    let dlq_counter = DlqCounter::new();
    let dlq_consumer = RabbitMqConsumer::new(client.clone());
    let dh = dlq_counter.clone();
    let dlq_handle = tokio::spawn(async move { dlq_consumer.run_dlq::<Ledger, _>(dh, ()).await });

    let shutdown = CancellationToken::new();
    let consumer = RabbitMqConsumer::new(client.clone());
    let handler_ctx = counters.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        // `max_retries(1)` = one initial attempt plus one retry, so the
        // retry-key message is dead-lettered the moment it comes back from the
        // shard hold queue with `retry_count = 1`.
        let opts = ConsumerOptions::<RabbitMqMarker>::new()
            .with_shutdown(s)
            .with_prefetch_count(10)
            .with_max_retries(1);
        consumer
            .run_fifo::<Ledger, _>(Handler, handler_ctx, opts)
            .await
    });

    assert!(
        wait_for_handlers(&counters, Duration::from_secs(30)).await,
        "timed out waiting for both handlers: reject={} retry={}",
        counters.reject_key_calls.load(Ordering::SeqCst),
        counters.retry_key_calls.load(Ordering::SeqCst),
    );

    // Four messages must reach the DLQ: the rejected one, the two cascaded
    // behind its poisoned key, and the retry-exhausted one (after the 1s hold).
    assert!(
        dlq_counter.wait_for_count(4, Duration::from_secs(30)).await,
        "expected 4 dead-lettered messages, got {}",
        dlq_counter.count()
    );

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

    // The handler-returned Reject is an independent failure — counted once.
    assert_eq!(
        failed_total(&snapshot, "rejected"),
        1,
        "expected exactly one `rejected` failure (the message that actually \
         failed); the two cascaded deliveries behind the poisoned key must \
         not be counted"
    );

    // ...and the cascade really happened: three messages went to the DLQ for
    // that key while the handler saw exactly one of them.
    assert_eq!(
        counters.reject_key_calls.load(Ordering::SeqCst),
        1,
        "cascaded deliveries must skip the handler"
    );

    // Retry budget exhausted is likewise an independent failure.
    assert_eq!(
        failed_total(&snapshot, "max_retries_exceeded"),
        1,
        "expected exactly one `max_retries_exceeded` failure"
    );
    assert_eq!(
        counters.retry_key_calls.load(Ordering::SeqCst),
        1,
        "the retry-key message is dead-lettered on its way back in, so the \
         handler must see it exactly once"
    );
}
