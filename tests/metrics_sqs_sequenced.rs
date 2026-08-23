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
//! # Where the cascade comes from here, and why each scenario gets its own topic
//!
//! SQS does not republish on reject: `route_reject` sets visibility to 0 and
//! lets **native redrive** retire the message at `maxReceiveCount = 10`. So a
//! message whose key has been poisoned is redelivered nine more times before it
//! reaches the DLQ, and every one of those redeliveries hits the poisoned-key
//! skip — the site marked `// Cascade: intentionally not counted`. They would
//! satisfy `retry_count >= max_retries` too, since `retry_count` is derived from
//! `ApproximateReceiveCount`. That makes arrival on the DLQ a *proof* that nine
//! cascade discards happened, and a counter still reading 1 a proof that none of
//! them were counted. Count either site and this test reads ~9, not 1.
//!
//! That redelivery loop is also why the two scenarios get a topic each rather
//! than two keys on one. A message being retired by redrive is re-received
//! continuously on a zero visibility timeout, and on one shard queue that
//! starved the other scenario's key on CI — in both directions across runs
//! (`reject=1 retry=0`, then `reject=0 retry=1` once the publish order was
//! swapped). The second of those starved a key that had never been delivered
//! even once, so this is contention for the receive loop rather than anything
//! the poisoning does; no claim is made here about which SQS or LocalStack
//! FIFO rule produces it. Two topics means two shard queues and two consumers,
//! which removes the interaction rather than reasoning about it.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the global
//! recorder slot, and whose `snapshot()` *drains* every counter it reads. So:
//! own integration binary, a single `#[test]`, and exactly one snapshot taken
//! at the end — progress is waited on through handler counters and the DLQs,
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
// Topics and handlers — one topic per scenario, see the module docs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LedgerEntry {
    account: String,
}

define_sequenced_topic!(
    RejectLedger,
    LedgerEntry,
    |msg: &LedgerEntry| msg.account.clone(),
    TopologyBuilder::new("metrics-sqs-rej")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(1)
        .hold_queue(Duration::from_secs(1))
        .dlq()
        .build()
);

define_sequenced_topic!(
    RetryLedger,
    LedgerEntry,
    |msg: &LedgerEntry| msg.account.clone(),
    TopologyBuilder::new("metrics-sqs-ret")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(1)
        .hold_queue(Duration::from_secs(1))
        .dlq()
        .build()
);

/// Rejects on the first delivery, poisoning its key. Every redelivery after
/// that is skipped without reaching the handler, so the count must stay at 1.
struct RejectHandler;
impl MessageHandler<RejectLedger> for RejectHandler {
    type Context = Arc<AtomicU32>;
    async fn handle(
        &self,
        _msg: LedgerEntry,
        _meta: MessageMetadata,
        calls: &Arc<AtomicU32>,
    ) -> Outcome {
        calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Reject
    }
}

/// Always retries. With `max_retries(1)` the message comes back once, exhausts
/// the budget before reaching the handler again, and poisons its key — so this
/// count must stay at 1 as well.
struct RetryHandler;
impl MessageHandler<RetryLedger> for RetryHandler {
    type Context = Arc<AtomicU32>;
    async fn handle(
        &self,
        _msg: LedgerEntry,
        _meta: MessageMetadata,
        calls: &Arc<AtomicU32>,
    ) -> Outcome {
        calls.fetch_add(1, Ordering::SeqCst);
        Outcome::Retry
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

/// Wait until both handlers have run at least once.
async fn wait_for_handlers(
    reject_calls: &Arc<AtomicU32>,
    retry_calls: &Arc<AtomicU32>,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if reject_calls.load(Ordering::SeqCst) >= 1 && retry_calls.load(Ordering::SeqCst) >= 1 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Wait for `target` distinct messages to land on `dlq_url`.
///
/// Each message is deleted once counted, so a FIFO group is never held behind
/// an in-flight message; ids are still deduplicated in case a delete does not
/// land.
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
        .declare::<RejectLedger>()
        .await
        .expect("declare reject topology");
    broker
        .topology()
        .declare::<RetryLedger>()
        .await
        .expect("declare retry topology");

    let publisher: Publisher<Sqs> = broker.publisher().await.expect("publisher");
    publisher
        .publish::<RejectLedger>(&LedgerEntry {
            account: "acct-reject".into(),
        })
        .await
        .expect("publish reject-key message");
    publisher
        .publish::<RetryLedger>(&LedgerEntry {
            account: "acct-retry".into(),
        })
        .await
        .expect("publish retry-key message");

    let reject_calls = Arc::new(AtomicU32::new(0));
    let retry_calls = Arc::new(AtomicU32::new(0));
    let shutdown = CancellationToken::new();

    // `max_retries(1)` = one initial attempt plus one retry, so the retry-key
    // message is dead-lettered as soon as it comes back from its visibility
    // timeout with `retry_count = 1`.
    let opts = || {
        ConsumerOptions::<Sqs>::new()
            .with_shutdown(shutdown.clone())
            .with_prefetch_count(10)
            .with_max_retries(1)
    };

    let reject_consumer = SqsConsumer::new(sns_client.clone(), sns_client.queue_registry().clone());
    let reject_ctx = reject_calls.clone();
    let reject_opts = opts();
    let reject_handle = tokio::spawn(async move {
        reject_consumer
            .run_fifo::<RejectLedger, _>(RejectHandler, reject_ctx, reject_opts)
            .await
    });

    let retry_consumer = SqsConsumer::new(sns_client.clone(), sns_client.queue_registry().clone());
    let retry_ctx = retry_calls.clone();
    let retry_opts = opts();
    let retry_handle = tokio::spawn(async move {
        retry_consumer
            .run_fifo::<RetryLedger, _>(RetryHandler, retry_ctx, retry_opts)
            .await
    });

    assert!(
        wait_for_handlers(&reject_calls, &retry_calls, Duration::from_secs(90)).await,
        "timed out waiting for both handlers: reject={} retry={}",
        reject_calls.load(Ordering::SeqCst),
        retry_calls.load(Ordering::SeqCst),
    );

    // Each message must reach its DLQ, which on SQS only happens through native
    // redrive at `maxReceiveCount = 10`. That is what makes the counter
    // assertions below exact rather than incidental: arrival means the message
    // was received ten times, so nine of those receives took a cascade path
    // that must not have counted.
    let sqs = test_broker.sqs_client().await;
    let reject_dlq = sns_client
        .queue_registry()
        .get("metrics-sqs-rej-dlq")
        .await
        .expect("reject DLQ should be registered");
    let retry_dlq = sns_client
        .queue_registry()
        .get("metrics-sqs-ret-dlq")
        .await
        .expect("retry DLQ should be registered");
    let (rejected_dead, retried_dead) = tokio::join!(
        wait_for_dlq_count(&sqs, &reject_dlq, 1, Duration::from_secs(150)),
        wait_for_dlq_count(&sqs, &retry_dlq, 1, Duration::from_secs(150)),
    );
    assert_eq!(
        (rejected_dead, retried_dead),
        (1, 1),
        "expected both messages to be dead-lettered within 150s"
    );

    shutdown.cancel();
    reject_handle
        .await
        .expect("reject consumer task panicked")
        .expect("reject consumer returned an error");
    retry_handle
        .await
        .expect("retry consumer task panicked")
        .expect("retry consumer returned an error");

    // Single, draining snapshot — taken only once both consumers have stopped,
    // so nothing can emit into it while it is being read.
    let snapshot = snapshotter.snapshot().into_hashmap();

    // The handler-returned Reject is an independent failure — counted once, on
    // the receive that reached the handler. The nine redeliveries that followed
    // it were skipped as cascade of that same failure and must not be counted.
    assert_eq!(
        failed_total(&snapshot, "rejected"),
        1,
        "expected exactly one `rejected` failure (the delivery that actually \
         reached the handler); the redeliveries skipped behind the poisoned \
         key must not be counted"
    );
    assert_eq!(
        reject_calls.load(Ordering::SeqCst),
        1,
        "cascaded redeliveries must skip the handler"
    );

    // Retry budget exhausted is likewise an independent failure. Its
    // redeliveries satisfy `retry_count >= max_retries` too — only the
    // poisoned-key check ahead of that branch keeps this at 1.
    assert_eq!(
        failed_total(&snapshot, "max_retries_exceeded"),
        1,
        "expected exactly one `max_retries_exceeded` failure"
    );
    assert_eq!(
        retry_calls.load(Ordering::SeqCst),
        1,
        "the retry-key message is dead-lettered on its way back in, so the \
         handler must see it exactly once"
    );
}
