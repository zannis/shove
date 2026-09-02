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

    wait_until_ready(&bootstrap).await;

    // One probe serves every purge's group-list and metadata polls: neither
    // call subscribes, so the client never joins a group, and building it
    // once avoids paying a client construction (threads, TCP connect,
    // bootstrap metadata) at every scenario boundary.
    let purge_probe: Arc<BaseConsumer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", "shove-stress-purge-probe")
            .create()
            .expect("build purge probe consumer"),
    );

    let purge_bootstrap = bootstrap.clone();
    let purge: harness::PurgeFn = Box::new(move |topology| {
        let bootstrap = purge_bootstrap.clone();
        let probe = Arc::clone(&purge_probe);
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
            let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .create()
                .map_err(|e| format!("build admin client: {e}"))?;

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

            // Wait for the previous scenario's members to leave this
            // topology's groups before touching anything else. The order
            // matters twice over: `DeleteGroups` below refuses a group that
            // still has members (`NonEmptyGroup`), and a member that still
            // exists is subscribed by name to the topics about to be deleted
            // — a named metadata fetch auto-creates topics on this broker's
            // defaults, so deleting topics first would let a lingering member
            // quietly resurrect one after the "topics are gone" check passed.
            //
            // The batch teardown now stops its drivers cleanly, so the common
            // case sees every group already settled on the first poll. The
            // drain pays real time only when a LeaveGroup was lost — that
            // member ages out at the 10 s consumer session timeout
            // (`SESSION_TIMEOUT_MS`, src/backends/kafka/constants.rs) — or a
            // consumer genuinely leaked, which never drains and is why the
            // ceiling stays tight rather than generous.
            //
            // A drain failure is carried into the delete below instead of
            // aborting the purge: the delete tolerates an absent group, so a
            // drain that failed only because the broker would not answer the
            // list call must not fail a purge the delete would complete.
            let drain_probe = Arc::clone(&probe);
            let drain_names = groups.clone();
            let drain_result = tokio::task::spawn_blocking(move || {
                harness::await_drain(
                    || match drain_probe.fetch_group_list(None, PROBE_RPC_TIMEOUT) {
                        Ok(list) => drain_names
                            .iter()
                            .filter_map(|group| {
                                list.groups()
                                    .iter()
                                    .find(|g| g.name() == group.as_str())
                                    .and_then(|g| {
                                        harness::kafka_group_undrained(
                                            group,
                                            g.members().len(),
                                            g.state(),
                                        )
                                    })
                            })
                            .collect(),
                        Err(e) => vec![format!("membership list failed: {e}")],
                    },
                    GROUP_DRAIN_DEADLINE,
                    GROUP_DRAIN_POLL,
                )
            })
            .await
            .map_err(|e| format!("group drain task: {e}"))?;

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
            // Leftover group state (offsets, rebalance epoch, dead members)
            // skews the next scenario, so a failed delete is a dirty
            // boundary, not a shrug. The drain normally saw every group
            // settle before the topics were touched; this bounded retry is
            // the final arbiter, kept at full size because it also backstops
            // what the drain cannot see — rdkafka's group list carries no
            // per-group error field, so a coordinator error placeholder can
            // slip a not-actually-clean group through to here.
            let group_refs: Vec<&str> = groups.iter().map(String::as_str).collect();
            let mut last_err = None;
            for attempt in 0..10 {
                match admin.delete_groups(&group_refs, &AdminOptions::new()).await {
                    Ok(results) => {
                        let failed: Vec<String> = results
                            .into_iter()
                            .filter_map(|result| match result {
                                Ok(_) => None,
                                Err((_, RDKafkaErrorCode::GroupIdNotFound)) => None,
                                Err((group, code)) => Some(format!("{group}: {code}")),
                            })
                            .collect();
                        if failed.is_empty() {
                            last_err = None;
                            break;
                        }
                        last_err = Some(failed.join(", "));
                    }
                    Err(e) => last_err = Some(e.to_string()),
                }
                if attempt + 1 < 10 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
            match (last_err, drain_result) {
                (None, _) => Ok(()),
                (Some(delete), Ok(())) => Err(format!("delete groups: {delete}")),
                (Some(delete), Err(drain)) => Err(format!(
                    "delete groups: {delete} (membership drain: {drain})"
                )),
            }
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
async fn wait_until_ready(bootstrap: &str) {
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "shove-stress-probe")
        .create()
        .expect("build Kafka probe consumer");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match probe.fetch_metadata(None, Duration::from_secs(2)) {
            Ok(md) if !md.brokers().is_empty() => return,
            _ if std::time::Instant::now() >= deadline => {
                panic!("Kafka broker did not become ready within 60s")
            }
            _ => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

/// Ceiling on the purge's group-membership drain. The drain returns the
/// moment every group reports settled, so this bounds pathology only: a lost
/// LeaveGroup ages out at the 10 s consumer session timeout
/// (`SESSION_TIMEOUT_MS` in `src/backends/kafka/constants.rs`), covered here
/// three times over, while a genuinely leaked consumer keeps heartbeating and
/// can never drain — a tight ceiling is what stops one leak from stalling
/// every remaining scenario for minutes.
const GROUP_DRAIN_DEADLINE: Duration = Duration::from_secs(30);

/// Poll interval of the group-membership drain.
const GROUP_DRAIN_POLL: Duration = Duration::from_millis(500);

/// Deadline for deleted topics to actually disappear — topic deletion is
/// asynchronous broker-side, the admin response only accepts the request.
const TOPIC_GONE_DEADLINE: Duration = Duration::from_secs(15);

/// Poll interval of the topic-deletion wait.
const TOPIC_GONE_POLL: Duration = Duration::from_millis(200);

/// Per-call timeout for the probe's group-list and metadata RPCs.
const PROBE_RPC_TIMEOUT: Duration = Duration::from_secs(2);
