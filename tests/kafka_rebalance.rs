//! Rebalance-safety integration test for the Kafka backend.
//!
//! Exercises the partition revoke/reassign cycle against a real broker:
//! consumer A owns all partitions, consumer B joins (cooperative rebalance
//! moves partitions to B, which processes and commits on them), B leaves
//! (partitions return to A), and a final batch must both be processed AND
//! committed on every partition.
//!
//! Without the rebalance-aware offset tracking in
//! `src/backends/kafka/consumer.rs`, A's stale `PartitionTracker` for a
//! partition B committed on waits forever for a contiguous run that B
//! already consumed — the partition stops committing for the life of the
//! connection and this test times out waiting for lag to reach zero.

#![cfg(feature = "kafka")]

use serde::{Deserialize, Serialize};
use shove::broker::Broker;
use shove::consumer::ConsumerOptions;
use shove::error::Result as ShoveResult;
use shove::handler::MessageHandler;
use shove::kafka::{
    KafkaAutoOffsetReset, KafkaClient, KafkaConfig, KafkaConsumer, KafkaLagStatsProvider,
    KafkaQueueStatsProvider,
};
use shove::markers::Kafka;
use shove::metadata::MessageMetadata;
use shove::outcome::Outcome;
use shove::topology::TopologyBuilder;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaContainer};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Message type + topic
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SimpleMessage {
    id: String,
    content: String,
}

shove::define_topic!(
    RebalanceTopic,
    SimpleMessage,
    TopologyBuilder::new("kafka-rebalance").build()
);

// ---------------------------------------------------------------------------
// Handler: records ids into a shared set, counts per consumer instance
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SetHandler {
    seen: Arc<Mutex<HashSet<String>>>,
    own_count: Arc<AtomicU32>,
}

impl SetHandler {
    fn new(seen: Arc<Mutex<HashSet<String>>>) -> Self {
        Self {
            seen,
            own_count: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl MessageHandler<RebalanceTopic> for SetHandler {
    type Context = ();
    async fn handle(&self, msg: SimpleMessage, _meta: MessageMetadata, _: &()) -> Outcome {
        self.seen.lock().await.insert(msg.id);
        self.own_count.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const QUEUE: &str = "kafka-rebalance";
const GROUP_ID: &str = "kafka-rebalance-consumer";
const WAIT: Duration = Duration::from_secs(60);

async fn publish_batch(broker: &Broker<Kafka>, prefix: &str, n: u32) -> Vec<String> {
    let publisher = broker.publisher().await.unwrap();
    let messages: Vec<SimpleMessage> = (0..n)
        .map(|i| SimpleMessage {
            id: format!("{prefix}-{i}"),
            content: format!("payload {i}"),
        })
        .collect();
    publisher
        .publish_batch::<RebalanceTopic>(&messages)
        .await
        .expect("publish_batch should succeed");
    messages.into_iter().map(|m| m.id).collect()
}

async fn wait_for_ids(seen: &Mutex<HashSet<String>>, ids: &[String], what: &str) {
    let deadline = Instant::now() + WAIT;
    loop {
        {
            let set = seen.lock().await;
            if ids.iter().all(|id| set.contains(id)) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for all {what} messages to be processed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Polls consumer lag (high watermark minus committed offset, summed over all
/// partitions) until it reaches zero. This is the committed-offset
/// convergence check: lag can only reach zero when every partition's
/// committed offset equals its high watermark.
async fn wait_for_zero_lag(client: &KafkaClient, bootstrap: &str, what: &str) {
    let stats_provider = KafkaLagStatsProvider::new(client.clone());
    let deadline = Instant::now() + WAIT;
    loop {
        let stats = stats_provider
            .get_queue_stats(QUEUE, GROUP_ID, KafkaAutoOffsetReset::Earliest)
            .await
            .expect("get_queue_stats should succeed");
        if stats.messages_pending == 0 {
            return;
        }
        if Instant::now() >= deadline {
            dump_partition_state(bootstrap);
            panic!(
                "committed offsets did not converge to the high watermark {what}: \
                 {} message(s) still pending — partitions returned by the departed \
                 consumer have stalled commits",
                stats.messages_pending
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// Failure diagnostics: per-partition committed offset vs watermarks, printed
// when a convergence wait times out so a flake report shows exactly which
// partitions stalled and where.
fn dump_partition_state(bootstrap: &str) {
    use rdkafka::TopicPartitionList;
    use rdkafka::consumer::{BaseConsumer, Consumer as _};
    let consumer: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", GROUP_ID)
        .create()
        .expect("diag consumer");
    let md = consumer
        .fetch_metadata(Some(QUEUE), Duration::from_secs(5))
        .expect("metadata");
    let pids: Vec<i32> = md.topics()[0].partitions().iter().map(|p| p.id()).collect();
    let mut tpl = TopicPartitionList::new();
    for &pid in &pids {
        tpl.add_partition(QUEUE, pid);
    }
    let committed = consumer
        .committed_offsets(tpl, Duration::from_secs(5))
        .expect("committed");
    for pid in pids {
        let (low, high) = consumer
            .fetch_watermarks(QUEUE, pid, Duration::from_secs(5))
            .expect("watermarks");
        let c = committed
            .find_partition(QUEUE, pid)
            .map(|e| format!("{:?}", e.offset()))
            .unwrap_or_else(|| "absent".into());
        eprintln!("DIAG partition {pid}: low={low} high={high} committed={c}");
    }
}

fn spawn_consumer(
    client: KafkaClient,
    handler: SetHandler,
    shutdown: CancellationToken,
) -> JoinHandle<ShoveResult<()>> {
    let consumer = KafkaConsumer::new(client);
    tokio::spawn(async move {
        consumer
            .run::<RebalanceTopic, _>(
                handler,
                (),
                ConsumerOptions::<Kafka>::new()
                    .with_shutdown(shutdown)
                    .with_prefetch_count(10),
            )
            .await
    })
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commits_resume_on_partitions_returned_after_rebalance() {
    // Surface shove's rebalance/commit-retry logs in failure output (nextest
    // prints captured output for failed tests).
    let _ = tracing_subscriber::fmt()
        .with_env_filter("shove=debug")
        .try_init();
    let container = KafkaContainer::default()
        .start()
        .await
        .expect("failed to start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("failed to get Kafka port");
    let bootstrap_servers = format!("127.0.0.1:{port}");
    let client = KafkaClient::connect_with_retry(&KafkaConfig::new(&bootstrap_servers), 10)
        .await
        .expect("failed to connect to Kafka");
    let broker = Broker::<Kafka>::from_client(client.clone());

    // Declared with the default partition count (8 — ≥ 4 required so the
    // cooperative rebalance moves a meaningful set of partitions to B).
    broker.topology().declare::<RebalanceTopic>().await.unwrap();

    let seen: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    // Phase 1: consumer A alone owns all partitions; a full batch is
    // processed and committed.
    let handler_a = SetHandler::new(seen.clone());
    let shutdown_a = CancellationToken::new();
    let handle_a = spawn_consumer(client.clone(), handler_a.clone(), shutdown_a.clone());

    let batch1 = publish_batch(&broker, "b1", 100).await;
    wait_for_ids(&seen, &batch1, "batch-1").await;
    wait_for_zero_lag(
        &client,
        &bootstrap_servers,
        "after batch 1 (single consumer)",
    )
    .await;

    // Phase 2: consumer B joins the same group; the cooperative rebalance
    // revokes some partitions from A and assigns them to B. Probe batches
    // are published until B demonstrably processes messages, proving it owns
    // partitions (and will advance their committed offsets past A's stale
    // tracker state).
    let handler_b = SetHandler::new(seen.clone());
    let shutdown_b = CancellationToken::new();
    let handle_b = spawn_consumer(client.clone(), handler_b.clone(), shutdown_b.clone());

    let join_deadline = Instant::now() + WAIT;
    let mut probe = 0u32;
    while handler_b.own_count.load(Ordering::Relaxed) == 0 {
        assert!(
            Instant::now() < join_deadline,
            "consumer B never processed a message — rebalance did not move \
             partitions to it"
        );
        publish_batch(&broker, &format!("probe-{probe}"), 8).await;
        probe += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // A larger batch spread across all partitions so B commits well ahead of
    // A's stale per-partition trackers.
    let batch2 = publish_batch(&broker, "b2", 100).await;
    wait_for_ids(&seen, &batch2, "batch-2").await;
    wait_for_zero_lag(&client, &bootstrap_servers, "after batch 2 (two consumers)").await;

    // Phase 3: B leaves gracefully; its partitions return to A. A's offset
    // tracking for those partitions must reset — with stale trackers the
    // partitions B committed on never commit again on A.
    shutdown_b.cancel();
    handle_b.await.unwrap().ok();

    let batch3 = publish_batch(&broker, "b3", 100).await;
    wait_for_ids(&seen, &batch3, "batch-3").await;

    // The money assert: committed offsets on ALL partitions converge to the
    // high watermark. Pre-fix this times out on the partitions B committed
    // on while it owned them.
    wait_for_zero_lag(
        &client,
        &bootstrap_servers,
        "after batch 3 (B's partitions back on A)",
    )
    .await;

    shutdown_a.cancel();
    handle_a.await.unwrap().ok();
    broker.close().await;
}
