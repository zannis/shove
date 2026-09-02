//! Stress benchmarks for the Kafka backend.
//!
//! Spins up a Kafka testcontainer for the lifetime of the process. Requires a
//! running Docker daemon.
//!
//!     cargo run -q --example kafka_stress --features kafka
//!     cargo run -q --example kafka_stress --features kafka -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use std::sync::Arc;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::types::RDKafkaErrorCode;
use shove::kafka::{
    BatchConsumerOptions, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig,
    KafkaTopologyDeclarer,
};
use shove::{Backend, Kafka, Topic};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaImage};

use harness::{
    BatchConsumeFn, BatchTopologyFn, DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios,
};

/// Image tag started by `testcontainers_modules::kafka::apache` (its pinned
/// default), recorded in the results provenance so a reader knows which
/// broker produced the numbers.
const KAFKA_VERSION: &str = "3.8.0";

#[tokio::main]
async fn main() {
    let container = KafkaImage::default()
        .start()
        .await
        .expect("failed to start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("failed to read Kafka port");
    let bootstrap = format!("127.0.0.1:{port}");

    // One metadata probe and one admin client serve the readiness wait and
    // every purge. Building them once avoids paying a client construction
    // (threads, TCP connect, bootstrap metadata) at every scenario boundary.
    // The probe deliberately sets no `group.id`: it is never polled, and a
    // group-configured rdkafka consumer redirects broker events onto a
    // consumer queue nothing here would serve — a slow leak inside the very
    // process whose RSS the bench reports.
    let probe: Arc<BaseConsumer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .expect("build metadata probe consumer"),
    );
    let admin: Arc<AdminClient<DefaultClientContext>> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .expect("build admin client"),
    );

    wait_until_ready(&probe).await;

    let purge: harness::PurgeFn = Box::new(move |topology| {
        let probe = Arc::clone(&probe);
        let admin = Arc::clone(&admin);
        Box::pin(async move {
            // Delete the topology's topics AND the consumer groups derived
            // from them. The topic delete on its own resets storage and lets
            // the next scenario re-declare with a fresh partition count sized
            // to its own `max_consumers`, but it leaves the consumer group
            // state (offsets, rebalance epoch, dead members) sitting in
            // Kafka's group coordinator. With many short scenarios that
            // residue accumulates and the next scenario eats a long rebalance
            // before steady-state, flattening apparent throughput. Wiping
            // the groups too gives each scenario a clean baseline.
            //
            // Every name is derived from the topology handed in, so the seq
            // and broadcast topologies get purged too — not just the main
            // topic this wrapper happens to name in a constant.
            let mut topics: Vec<&str> = vec![topology.queue()];
            // `{queue}-consumer` / `{queue}-fifo` mirror shove's derived
            // group ids (src/backends/kafka/constants.rs).
            let mut groups = vec![
                format!("{}-consumer", topology.queue()),
                format!("{}-fifo", topology.queue()),
            ];
            if let Some(dlq) = topology.dlq() {
                topics.push(dlq);
                groups.push(format!("{dlq}-consumer"));
            }

            // Delete the groups first, waiting out membership through the
            // delete itself: `DeleteGroups` refuses a group that still has
            // members (`NonEmptyGroup`), which makes the broker the authority
            // on "have they left" — no hand-modelled group-state predicate to
            // drift out of sync with it. Members normally leave the moment
            // their scenario tears down (drivers are stopped cleanly now); one
            // that lost its LeaveGroup ages out at the 10 s consumer session
            // timeout (`SESSION_TIMEOUT_MS`, src/backends/kafka/constants.rs),
            // so a deadline is what the old fixed retry count was not: sized
            // to the failure mode, and paid only when something actually
            // lingers. `GroupIdNotFound` is the common clean case — most
            // scenarios never create the fifo or DLQ groups at all.
            //
            // This runs before the topic delete on purpose: a member that
            // still exists is subscribed by name to the topics below, and a
            // named metadata fetch auto-creates topics on this broker's
            // defaults — deleting topics first would let a lingering member
            // quietly resurrect one after the topics-are-gone check passed.
            // A group still refused at the deadline fails the purge loudly,
            // topics untouched, naming what the broker last reported.
            let group_refs: Vec<&str> = groups.iter().map(String::as_str).collect();
            let settle_started = std::time::Instant::now();
            loop {
                let refused: Vec<String> =
                    match admin.delete_groups(&group_refs, &AdminOptions::new()).await {
                        Ok(results) => results
                            .into_iter()
                            .filter_map(|result| match result {
                                Ok(_) => None,
                                Err((_, RDKafkaErrorCode::GroupIdNotFound)) => None,
                                Err((group, code)) => Some(format!("{group}: {code}")),
                            })
                            .collect(),
                        Err(e) => vec![format!("delete request failed: {e}")],
                    };
                if refused.is_empty() {
                    break;
                }
                if settle_started.elapsed() >= GROUP_SETTLE_DEADLINE {
                    return Err(format!(
                        "delete groups: still refused after {:?}: {}",
                        settle_started.elapsed(),
                        refused.join(", ")
                    ));
                }
                tokio::time::sleep(GROUP_SETTLE_POLL).await;
            }

            let results = admin
                .delete_topics(&topics, &AdminOptions::new())
                .await
                .map_err(|e| format!("delete_topics: {e}"))?;
            for result in results {
                if let Err((topic, err)) = result
                    && err != RDKafkaErrorCode::UnknownTopicOrPartition
                {
                    return Err(format!("delete topic {topic}: {err}"));
                }
            }
            // Topic deletion is asynchronous broker-side: the admin response
            // only accepts the request. Returning now lets the immediate
            // re-declare race the deletion — the fresh topic can fail to
            // create or be swept away by the outstanding delete. Poll the
            // full topic list (a *named* metadata fetch would auto-create
            // the topic on this broker's defaults) until every name is gone.
            let topic_probe = Arc::clone(&probe);
            let names: Vec<String> = topics.iter().map(|t| t.to_string()).collect();
            tokio::task::spawn_blocking(move || {
                harness::await_drain(
                    || match topic_probe.fetch_metadata(None, PROBE_RPC_TIMEOUT) {
                        Ok(md) => md
                            .topics()
                            .iter()
                            .map(|t| t.name().to_string())
                            .filter(|n| names.contains(n))
                            .map(|n| format!("topic {n} still present"))
                            .collect(),
                        Err(e) => vec![format!("metadata fetch failed: {e}")],
                    },
                    TOPIC_GONE_DEADLINE,
                    TOPIC_GONE_POLL,
                )
            })
            .await
            .map_err(|e| format!("purge probe task: {e}"))?
            .map_err(|e| format!("topic delete: {e}"))?;
            Ok(())
        })
    });

    let dlq_drain: DlqDrainFn<Kafka> = Box::new(|client, handler| {
        Box::pin(async move {
            let consumer = KafkaConsumer::new(client);
            consumer
                .run_dlq::<StressTestTopic, _>(handler, ())
                .await
                .map_err(|e| format!("run_dlq: {e}"))
        })
    });

    // Kafka is the only backend that has `run_batch` at all, which is why it
    // is the only wrapper supplying this closure — and why `consume_batch`
    // lands in every other backend's `unsupported[]` instead of being faked.
    // The harness invokes it once per scenario consumer; each invocation is
    // an independent group member.
    let batch_consume: BatchConsumeFn<Kafka> = Box::new(|client, handler, stop| {
        Box::pin(async move {
            let consumer = KafkaConsumer::new(client);
            consumer
                .run_batch::<StressTestTopic, _>(
                    handler,
                    (),
                    BatchConsumerOptions::new().with_shutdown(stop),
                )
                .await
                .map_err(|e| format!("run_batch: {e}"))
        })
    });

    // The generic declare creates the topic with Kafka's default partition
    // count; a batch scenario claiming N consumers needs at least N
    // partitions or the extra consumers sit idle while the row counts them.
    let batch_topology: BatchTopologyFn<Kafka> = Box::new(|client, consumers| {
        Box::pin(async move {
            KafkaTopologyDeclarer::new(client)
                .with_min_partitions(consumers as i32)
                .declare(StressTestTopic::topology())
                .await
                .map_err(|e| format!("declare: {e}"))
        })
    });

    let hcfg = HarnessConfig::<Kafka>::new("kafka")
        .with_purge(purge)
        .with_broker("Apache Kafka", KAFKA_VERSION, "docker single-node (KRaft)")
        .with_dlq_drain(dlq_drain)
        .with_batch_consume(batch_consume)
        .with_batch_topology(batch_topology)
        // Kafka runs a single FIFO task over every assigned partition —
        // there is no per-shard worker set here, so a fifo row must claim
        // one worker, not the shard count.
        .with_fifo_workers(1);
    run_all_scenarios(
        hcfg,
        || {
            let bootstrap = bootstrap.clone();
            async move {
                <Kafka as Backend>::connect(KafkaConfig::new(&bootstrap))
                    .await
                    .expect("connect Kafka")
            }
        },
        |consumers, prefetch, concurrent| {
            KafkaConsumerGroupConfig::new(consumers..=consumers)
                .with_prefetch_count(prefetch)
                .with_concurrent_processing(concurrent)
        },
    )
    .await;

    drop(container);
}

/// Poll the broker until a metadata fetch succeeds. Testcontainers' Kafka
/// image returns from `.start()` as soon as the process logs "started", but
/// the broker may still be coming up internally and reject the first
/// connection attempts. Without this wait, the first scenario eats the
/// startup latency inside its measurement window.
async fn wait_until_ready(probe: &Arc<BaseConsumer>) {
    let probe = Arc::clone(probe);
    tokio::task::spawn_blocking(move || {
        harness::await_drain(
            || match probe.fetch_metadata(None, PROBE_RPC_TIMEOUT) {
                Ok(md) if !md.brokers().is_empty() => Vec::new(),
                Ok(_) => vec!["no brokers in metadata yet".to_string()],
                Err(e) => vec![format!("metadata fetch failed: {e}")],
            },
            READY_DEADLINE,
            READY_POLL,
        )
    })
    .await
    .expect("readiness probe task")
    .unwrap_or_else(|e| panic!("Kafka broker did not become ready: {e}"));
}

/// Ceiling on the purge's group-delete wait. The wait ends the moment the
/// broker accepts every delete, so this bounds pathology only: a lost
/// LeaveGroup ages out at the 10 s consumer session timeout
/// (`SESSION_TIMEOUT_MS` in `src/backends/kafka/constants.rs`), covered here
/// three times over, while a genuinely leaked consumer keeps heartbeating and
/// can never settle — a tight ceiling is what stops one leak from stalling
/// every remaining scenario for minutes.
const GROUP_SETTLE_DEADLINE: Duration = Duration::from_secs(30);

/// Poll interval of the purge's group-delete wait.
const GROUP_SETTLE_POLL: Duration = Duration::from_millis(500);

/// Deadline for the broker to answer its first metadata fetch at startup.
const READY_DEADLINE: Duration = Duration::from_secs(60);

/// Poll interval of the startup readiness wait.
const READY_POLL: Duration = Duration::from_millis(200);

/// Deadline for deleted topics to actually disappear — topic deletion is
/// asynchronous broker-side, the admin response only accepts the request.
const TOPIC_GONE_DEADLINE: Duration = Duration::from_secs(15);

/// Poll interval of the topic-deletion wait.
const TOPIC_GONE_POLL: Duration = Duration::from_millis(200);

/// Per-call timeout for the probe's group-list and metadata RPCs.
const PROBE_RPC_TIMEOUT: Duration = Duration::from_secs(2);
