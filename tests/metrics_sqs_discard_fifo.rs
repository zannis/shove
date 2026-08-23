//! Integration test: the SQS *sequenced* (FIFO) consumer must count every
//! DLQ-terminal discard in `messages_failed_total`, exactly like its concurrent
//! sibling.
//!
//! `consume_loop_sequenced` and `drain_pending_for_key` reach the DLQ through
//! `router::route_reject` directly rather than through the shared
//! `route_outcome`, so before this test's fix they emitted no counter at all —
//! a `Reject` on a sequenced topic, and every message discarded behind it as
//! FailAll collateral, died silently.
//!
//! Uses `metrics-util::debugging::DebuggingRecorder`, which takes the *global*
//! recorder slot. That is why this lives in its own single-test binary instead
//! of in `sns_sqs_integration`: installed there it would observe (and be
//! polluted by) every other test in that binary.
//!
//! `Snapshotter::snapshot()` **drains** every counter it reads
//! (`AtomicU64::swap(0, SeqCst)`), so this test may snapshot exactly once, at
//! the very end. Progress is waited on through DLQ arrivals and handler
//! invocations — never by peeking at the metric under test.
//!
//! The exactly-once assertion has extra teeth on SQS: retries here reset
//! visibility rather than republishing, so a message that is not retired keeps
//! coming back and re-entering the same branch. A counter that read 1 by
//! accident would read 2, 3, 4, … instead of merely failing to move.

#![cfg(all(feature = "aws-sns-sqs", feature = "metrics"))]
#![allow(clippy::mutable_key_type)] // metrics-util's CompositeKey has interior mutability

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use shove::sns::{SnsClient, SnsConfig, SqsConsumer};
use shove::{
    Broker, ConsumerOptions, MessageHandler, MessageMetadata, Outcome, Publisher, SequenceFailure,
    SequencedTopic, Sqs, TopologyBuilder, define_sequenced_topic,
};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::localstack::LocalStack;
use tokio_util::sync::CancellationToken;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Entry {
    account: String,
    amount: u64,
}

// FailAll is the shape that exercises both label decisions at once: the
// handler's own `Reject` (reason=rejected) and the collateral discard of every
// later message on the poisoned key (also reason=rejected — see the PR).
define_sequenced_topic!(
    LedgerTopic,
    Entry,
    |msg: &Entry| msg.account.clone(),
    TopologyBuilder::new("sqs-seq-discard-metrics")
        .sequenced(SequenceFailure::FailAll)
        .routing_shards(1)
        .hold_queue(Duration::from_secs(1))
        .dlq()
        .build()
);

/// Rejects everything, and counts how many messages actually reached a handler.
/// The collateral discard must *not* show up here — that is what makes it
/// collateral rather than a second handler rejection.
#[derive(Clone)]
struct RejectHandler {
    invocations: Arc<AtomicUsize>,
}

impl MessageHandler<LedgerTopic> for RejectHandler {
    type Context = ();
    async fn handle(&self, _msg: Entry, _meta: MessageMetadata, _: &()) -> Outcome {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        Outcome::Reject
    }
}

/// Counts arrivals on the topic's DLQ, so the test can wait on real progress
/// instead of on the counter it is about to assert.
#[derive(Clone)]
struct DlqCounter {
    arrivals: Arc<AtomicUsize>,
}

impl MessageHandler<LedgerTopic> for DlqCounter {
    type Context = ();
    async fn handle(&self, _msg: Entry, _meta: MessageMetadata, _: &()) -> Outcome {
        self.arrivals.fetch_add(1, Ordering::SeqCst);
        Outcome::Ack
    }
}

/// Collect every `shove_messages_failed_total` sample as `(reason, count)`,
/// sorted so the assertion is order-independent.
///
/// Drains the recorder — call once.
fn failed_counters(snapshotter: &Snapshotter) -> Vec<(String, u64)> {
    let mut samples: Vec<(String, u64)> = snapshotter
        .snapshot()
        .into_hashmap()
        .into_iter()
        .filter(|(k, _)| k.key().name() == "shove_messages_failed_total")
        .map(|(k, (_, _, value))| {
            let reason = k
                .key()
                .labels()
                .find(|l| l.key() == "reason")
                .map(|l| l.value().to_string())
                .unwrap_or_default();
            let count = match value {
                DebugValue::Counter(c) => c,
                other => panic!("messages_failed_total is not a counter: {other:?}"),
            };
            (reason, count)
        })
        .filter(|(_, count)| *count > 0)
        .collect();
    samples.sort();
    samples
}

async fn wait_for(deadline: Duration, mut done: impl FnMut() -> bool) -> bool {
    tokio::time::timeout(deadline, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_ok()
}

async fn wait_for_localstack_ready(endpoint_url: &str) {
    let aws_config = aws_config::from_env()
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .load()
        .await;
    let sns = aws_sdk_sns::Client::new(&aws_config);
    let sqs = aws_sdk_sqs::Client::new(&aws_config);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if sns.list_topics().send().await.is_ok() && sqs.list_queues().send().await.is_ok() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "LocalStack services not ready within 30s at {endpoint_url}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn sequenced_dead_letter_counts_every_discarded_message_once() {
    let recorder = DebuggingRecorder::new();
    let snapshotter: Snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

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

    let sns_client = SnsClient::new(&SnsConfig {
        region: Some("us-east-1".into()),
        endpoint_url: Some(endpoint_url),
    })
    .await
    .expect("failed to create SNS client");

    let broker: Broker<Sqs> = Broker::<Sqs>::from_client(sns_client.clone());
    broker
        .topology()
        .declare::<LedgerTopic>()
        .await
        .expect("declare");
    let publisher: Publisher<Sqs> = broker.publisher().await.expect("publisher");

    // Two messages on the same sequence key. The first is rejected by the
    // handler and poisons the key; the second is discarded as FailAll
    // collateral without the handler ever seeing it. Both must be counted.
    for amount in [1_u64, 2] {
        publisher
            .publish::<LedgerTopic>(&Entry {
                account: "acct-1".to_string(),
                amount,
            })
            .await
            .expect("publish");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let dlq_arrivals = Arc::new(AtomicUsize::new(0));
    let dlq_consumer = SqsConsumer::new(sns_client.clone(), sns_client.queue_registry().clone());
    let dlq_handler = DlqCounter {
        arrivals: Arc::clone(&dlq_arrivals),
    };
    let dlq_task = tokio::spawn(async move {
        dlq_consumer
            .run_dlq::<LedgerTopic, _>(dlq_handler, ())
            .await
    });

    let handler_invocations = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let consumer = SqsConsumer::new(sns_client.clone(), sns_client.queue_registry().clone());
    let sc = shutdown.clone();
    let handler = RejectHandler {
        invocations: Arc::clone(&handler_invocations),
    };
    let handle = tokio::spawn(async move {
        consumer
            .run_fifo::<LedgerTopic, _>(
                handler,
                (),
                ConsumerOptions::<Sqs>::new()
                    .with_shutdown(sc)
                    .with_prefetch_count(10)
                    .with_max_retries(1),
            )
            .await
    });

    // Wait on DLQ arrivals, not on the metric: snapshotting drains it.
    let both_dead_lettered = wait_for(Duration::from_secs(90), || {
        dlq_arrivals.load(Ordering::SeqCst) >= 2
    })
    .await;

    // Let any *duplicate* emission land before snapshotting, so "exactly once"
    // is a real assertion rather than a race we happened to win. On SQS this
    // window matters: an unretired message would be redelivered and recounted.
    tokio::time::sleep(Duration::from_secs(5)).await;
    shutdown.cancel();
    handle.await.expect("consumer task should not panic").ok();
    dlq_task.abort();

    let dlq_seen = dlq_arrivals.load(Ordering::SeqCst);
    let handled = handler_invocations.load(Ordering::SeqCst);
    let failed = failed_counters(&snapshotter);

    assert!(
        both_dead_lettered,
        "expected 2 messages on the DLQ within 90s, saw {dlq_seen}; failed counters: {failed:?}"
    );
    assert_eq!(
        dlq_seen, 2,
        "exactly the two published messages must be dead-lettered"
    );
    assert_eq!(
        handled, 1,
        "only the first message may reach the handler — the second must be \
         discarded as FailAll collateral"
    );
    assert_eq!(
        failed,
        vec![("rejected".to_string(), 2)],
        "both the handler's Reject and the FailAll collateral discard must be \
         counted exactly once each under reason=rejected; got {failed:?}"
    );
}
