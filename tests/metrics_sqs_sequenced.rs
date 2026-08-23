#![cfg(all(feature = "aws-sns-sqs", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

//! Integration test: `shove_messages_failed_total` on the *sequenced* SQS
//! consumer path (`SqsConsumer::run_fifo`).
//!
//! The counters this asserts were added blind — `metrics` was enabled on no
//! coverage entry but `inmemory`, so every `record_failed` call in
//! `backends/sns/consumer.rs` type-checked and never ran. This drives real
//! LocalStack queues through both instrumented sequenced sites and — just as
//! importantly — asserts that a `SequenceFailure::FailAll` cascade is *not*
//! counted. See `metrics::FailReason` for why the cascade is excluded.
//!
//! The cascade assertion has real teeth on SQS specifically. SQS retries by
//! resetting the message's visibility rather than republishing, so a message
//! that is rejected keeps coming back until native redrive retires it at
//! `maxReceiveCount`. Every one of those redeliveries re-enters the same
//! `retry_count >= max_retries` branch that counts `max_retries_exceeded` — it
//! is only the poisoned-key check *ahead* of that branch that keeps the counter
//! at 1. Drop the check and this test reads 2, 3, 4, ... instead.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot, and whose `snapshot()` *drains* every counter it reads. So:
//! own integration binary, a single `#[test]`, and exactly one snapshot taken
//! at the end — progress is waited on through handler counters and the DLQ,
//! never by peeking at the metrics.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::handler::MessageHandler;
use shove::markers::Sqs;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::publisher::Publisher;
use shove::sns::{SnsClient, SnsConfig, SqsConsumer};
// Imported item by item rather than through `shove::*`: the glob shadows the
// `metrics` crate this file names directly in `failed_total`'s signature.
use shove::{SequenceFailure, SequencedTopic, TopologyBuilder, define_sequenced_topic};

use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// Poll SNS and SQS against the LocalStack endpoint until both respond, or
/// panic after 30s, so the test does not race the container's boot.
async fn wait_for_localstack_ready(endpoint_url: &str) {
    let aws_config = aws_config::from_env()
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .load()
        .await;
    let sns = aws_sdk_sns::Client::new(&aws_config);
    let sqs = aws_sdk_sqs::Client::new(&aws_config);

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if sns.list_topics().send().await.is_ok() && sqs.list_queues().send().await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "LocalStack services not ready within 30s at {endpoint_url}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

struct TestBroker {
    #[allow(dead_code)]
    container: testcontainers::ContainerAsync<LocalStack>,
    endpoint_url: String,
}

impl TestBroker {
    async fn start() -> Self {
        // Dummy credentials for LocalStack.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
        }

        let auth_token = std::env::var("LOCALSTACK_AUTH_TOKEN")
            .expect("LOCALSTACK_AUTH_TOKEN must be set to run SNS/SQS integration tests");

        let container = LocalStack::default()
            .with_env_var("LOCALSTACK_AUTH_TOKEN", auth_token)
            .start()
            .await
            .expect("failed to start LocalStack container");

        let port = container
            .get_host_port_ipv4(4566)
            .await
            .expect("failed to get LocalStack port");
        let endpoint_url = format!("http://localhost:{port}");

        wait_for_localstack_ready(&endpoint_url).await;

        Self {
            container,
            endpoint_url,
        }
    }

    fn sns_config(&self) -> SnsConfig {
        SnsConfig {
            region: Some("us-east-1".into()),
            endpoint_url: Some(self.endpoint_url.clone()),
        }
    }

    async fn sqs_client(&self) -> aws_sdk_sqs::Client {
        let aws_config = aws_config::from_env()
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(&self.endpoint_url)
            .load()
            .await;
        aws_sdk_sqs::Client::new(&aws_config)
    }
}

// ---------------------------------------------------------------------------
// Topic and handler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LedgerEntry {
    account: String,
}

define_sequenced_topic!(
    Ledger,
    LedgerEntry,
    |msg: &LedgerEntry| msg.account.clone(),
    TopologyBuilder::new("metrics-sqs-seq")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(1)
        .hold_queue(Duration::from_secs(1))
        .dlq()
        .build()
);

#[derive(Clone)]
struct Counters {
    /// Handler invocations for the key that gets poisoned. Must stay at 1: the
    /// two deliveries buffered behind it are dead-lettered without ever
    /// reaching the handler — that is the cascade.
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
/// the retry has been parked behind its visibility timeout.
async fn wait_for_handlers(ctx: &Counters, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if ctx.reject_key_calls.load(Ordering::SeqCst) >= 1
            && ctx.retry_key_calls.load(Ordering::SeqCst) >= 1
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Count distinct messages that have landed on the topic's DLQ, stopping as
/// soon as `target` of them have been seen.
///
/// Each message is deleted once counted. The DLQ is FIFO, so leaving them
/// in-flight would hold the rest of their message group behind them for a full
/// visibility timeout — three of the four expected messages share the poisoned
/// key's group. Ids are still deduplicated in case a delete does not land.
async fn wait_for_dlq_count(
    sqs: &aws_sdk_sqs::Client,
    dlq_url: &str,
    target: usize,
    timeout: Duration,
) -> usize {
    let deadline = Instant::now() + timeout;
    let mut seen: HashSet<String> = HashSet::new();
    while seen.len() < target && Instant::now() < deadline {
        let result = sqs
            .receive_message()
            .queue_url(dlq_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await
            .expect("failed to receive from DLQ");
        for msg in result.messages() {
            if let Some(id) = msg.message_id() {
                seen.insert(id.to_string());
            }
            if let Some(receipt) = msg.receipt_handle() {
                sqs.delete_message()
                    .queue_url(dlq_url)
                    .receipt_handle(receipt)
                    .send()
                    .await
                    .expect("failed to delete DLQ message");
            }
        }
    }
    seen.len()
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sequenced_discards_are_counted_but_failall_cascades_are_not() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let test_broker = TestBroker::start().await;
    let sns_client = SnsClient::new(&test_broker.sns_config())
        .await
        .expect("create SNS client");
    let broker = Broker::<Sqs>::from_client(sns_client.clone());
    broker
        .topology()
        .declare::<Ledger>()
        .await
        .expect("declare topology");

    // Publish before the consumer starts so all three `acct-reject` deliveries
    // are already on the shard queue when the first one is dispatched: with
    // `prefetch_count(10)` they arrive in a single receive, the other two
    // buffer behind the in-flight key, and get dead-lettered as a cascade the
    // moment the handler poisons it.
    let publisher: Publisher<Sqs> = broker.publisher().await.expect("publisher");
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

    let shutdown = CancellationToken::new();
    let consumer = SqsConsumer::new(sns_client.clone(), sns_client.queue_registry().clone());
    let handler_ctx = counters.clone();
    let s = shutdown.clone();
    let consume_handle = tokio::spawn(async move {
        // `max_retries(1)` = one initial attempt plus one retry, so the
        // retry-key message is dead-lettered as soon as it comes back from its
        // visibility timeout with `retry_count = 1`.
        let opts = ConsumerOptions::<Sqs>::new()
            .with_shutdown(s)
            .with_prefetch_count(10)
            .with_max_retries(1);
        consumer
            .run_fifo::<Ledger, _>(Handler, handler_ctx, opts)
            .await
    });

    assert!(
        wait_for_handlers(&counters, Duration::from_secs(60)).await,
        "timed out waiting for both handlers: reject={} retry={}",
        counters.reject_key_calls.load(Ordering::SeqCst),
        counters.retry_key_calls.load(Ordering::SeqCst),
    );

    // All four messages must reach the DLQ: the rejected one, the two cascaded
    // behind its poisoned key, and the retry-exhausted one. SQS retires them
    // through native redrive at `maxReceiveCount`, so this also guarantees the
    // redelivery loop ran — which is exactly what would double-count if the
    // poisoned-key check stopped short-circuiting.
    let sqs = test_broker.sqs_client().await;
    let dlq_url = sns_client
        .queue_registry()
        .get("metrics-sqs-seq-dlq")
        .await
        .expect("DLQ should be registered");
    let dlq_count = wait_for_dlq_count(&sqs, &dlq_url, 4, Duration::from_secs(90)).await;
    assert_eq!(
        dlq_count, 4,
        "expected all 4 messages to be dead-lettered within 90s, saw {dlq_count}"
    );

    shutdown.cancel();
    consume_handle
        .await
        .expect("consumer task panicked")
        .expect("consumer returned an error");

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

    // ...and the cascade really happened: three messages for that key were
    // dead-lettered while the handler saw exactly one of them.
    assert_eq!(
        counters.reject_key_calls.load(Ordering::SeqCst),
        1,
        "cascaded deliveries must skip the handler"
    );

    // Retry budget exhausted is likewise an independent failure — counted once
    // even though the rejected message is redelivered until redrive retires it.
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
