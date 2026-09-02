//! Stress benchmarks for the Kafka backend.
//!
//! Spins up a Kafka testcontainer for the lifetime of the process. Requires a
//! running Docker daemon.
//!
//!     cargo run -q --example kafka_stress --features kafka
//!     cargo run -q --example kafka_stress --features kafka -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

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

    let purge_bootstrap = bootstrap.clone();
    let purge: harness::PurgeFn = Box::new(move |topology| {
        let bootstrap = purge_bootstrap.clone();
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
            let probe: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .set("group.id", "shove-stress-purge-probe")
                .create()
                .map_err(|e| format!("build purge probe: {e}"))?;
            let names: Vec<String> = topics.iter().map(|t| t.to_string()).collect();
            tokio::task::spawn_blocking(move || {
                let deadline = std::time::Instant::now() + Duration::from_secs(15);
                loop {
                    let still: Vec<String> =
                        match probe.fetch_metadata(None, Duration::from_secs(2)) {
                            Ok(md) => md
                                .topics()
                                .iter()
                                .map(|t| t.name().to_string())
                                .filter(|n| names.contains(n))
                                .collect(),
                            Err(e) => {
                                if std::time::Instant::now() >= deadline {
                                    return Err(format!("purge probe metadata: {e}"));
                                }
                                std::thread::sleep(Duration::from_millis(200));
                                continue;
                            }
                        };
                    if still.is_empty() {
                        return Ok(());
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(format!(
                            "topics still present 15s after delete was accepted: {still:?}"
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }
            })
            .await
            .map_err(|e| format!("purge probe task: {e}"))??;
            // `DeleteGroups` refuses a group that still has members
            // (`NonEmptyGroup`), and the previous scenario's members are not
            // reliably gone by the time we get here: the batch flow aborts
            // its drivers rather than shutting them down, so whether their
            // LeaveGroup ever reaches the coordinator is a teardown race, and
            // a member that never sent one only ages out at the consumer
            // session timeout (10 s — `SESSION_TIMEOUT_MS` in
            // `src/backends/kafka/constants.rs`). A fixed retry budget on the
            // delete loses that race deterministically once enough members
            // linger — a 16-member batch cell held `NonEmptyGroup` past the
            // old 5 s budget on every repetition — so wait on the observable
            // instead: poll each group's membership until it reports empty,
            // then delete.
            let drain_probe: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .set("group.id", "shove-stress-purge-probe")
                .create()
                .map_err(|e| format!("build group drain probe: {e}"))?;
            let drain_groups = groups.clone();
            tokio::task::spawn_blocking(move || {
                await_group_drain(
                    |group| {
                        let list = drain_probe
                            .fetch_group_list(Some(group), Duration::from_secs(2))
                            .map_err(|e| format!("list group {group}: {e}"))?;
                        // An absent group lists nothing and sums to zero —
                        // the common clean case and a drained one are the
                        // same answer here.
                        Ok(list
                            .groups()
                            .iter()
                            .filter(|g| g.name() == group)
                            .map(|g| g.members().len())
                            .sum())
                    },
                    &drain_groups,
                    GROUP_DRAIN_DEADLINE,
                    GROUP_DRAIN_POLL,
                )
            })
            .await
            .map_err(|e| format!("group drain task: {e}"))??;
            // Leftover group state (offsets, rebalance epoch, dead members)
            // skews the next scenario, so a failed delete is a dirty
            // boundary, not a shrug. The drain above has already seen every
            // group empty; this bounded retry only covers the races it
            // cannot: a coordinator that has not yet applied the departures
            // it reported, or a member rejoining between the last poll and
            // the delete.
            let group_refs: Vec<&str> = groups.iter().map(String::as_str).collect();
            let mut last_err = None;
            for _ in 0..10 {
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
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            match last_err {
                Some(e) => Err(format!("delete groups: {e}")),
                None => Ok(()),
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
    let batch_consume: BatchConsumeFn<Kafka> = Box::new(|client, handler| {
        Box::pin(async move {
            let consumer = KafkaConsumer::new(client);
            consumer
                .run_batch::<StressTestTopic, _>(handler, (), BatchConsumerOptions::new())
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

/// Ceiling on the purge's group-membership drain. Sized from the worst case
/// rather than the group size: a member that died without a LeaveGroup is
/// evicted at the consumer session timeout (10 s), so 60 s covers that with
/// margin for a loaded host, while a group still populated past it is a real
/// dirty boundary worth failing loudly. The drain returns the moment the
/// groups report empty, so the ceiling is not a per-scenario cost.
const GROUP_DRAIN_DEADLINE: Duration = Duration::from_secs(60);

/// Poll interval of the group-membership drain.
const GROUP_DRAIN_POLL: Duration = Duration::from_millis(500);

/// Block until every group in `groups` reports zero members, polling
/// `fetch_members` (which maps a group id to its current live-member count,
/// `0` covering absent and empty alike) every `poll` up to `deadline`.
///
/// A transient fetch error is retried like a populated group; one still
/// failing at the deadline surfaces as the drain error. Runs on blocking
/// primitives (`thread::sleep`) — call it from `spawn_blocking`, like the
/// topic probe.
fn await_group_drain(
    mut fetch_members: impl FnMut(&str) -> Result<usize, String>,
    groups: &[String],
    deadline: Duration,
    poll: Duration,
) -> Result<(), String> {
    let end = std::time::Instant::now() + deadline;
    loop {
        let mut lingering: Vec<String> = Vec::new();
        let mut fetch_err: Option<String> = None;
        for group in groups {
            match fetch_members(group) {
                Ok(0) => {}
                Ok(members) => lingering.push(format!("{group}: {members} member(s)")),
                Err(e) => fetch_err = Some(e),
            }
        }
        if lingering.is_empty() && fetch_err.is_none() {
            return Ok(());
        }
        if std::time::Instant::now() >= end {
            return Err(match fetch_err {
                Some(e) => format!("group drain: {e}"),
                None => format!(
                    "groups still non-empty {}s after the previous scenario ended: {}",
                    deadline.as_secs(),
                    lingering.join(", ")
                ),
            });
        }
        std::thread::sleep(poll);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One group named like the harness's derived consumer group.
    fn groups() -> Vec<String> {
        vec!["shove-stress-bench-consumer".to_string()]
    }

    /// The defect this drain replaces: a fixed 10-attempt budget gave up on a
    /// group whose members simply had not finished leaving yet. The drain must
    /// keep polling well past ten rounds as long as the deadline allows.
    #[test]
    fn drain_outlasts_the_old_fixed_retry_budget() {
        let mut polls = 0u32;
        let result = await_group_drain(
            |_group| {
                polls += 1;
                // 16 members that only finish leaving on the 30th poll —
                // three times the old 10-attempt budget.
                Ok(if polls < 30 { 16 } else { 0 })
            },
            &groups(),
            Duration::from_secs(10),
            Duration::from_millis(1),
        );
        assert_eq!(result, Ok(()));
        assert!(polls >= 30, "drained after only {polls} polls");
    }

    /// An absent group reports zero members, so the drain returns immediately
    /// — the common clean case must not pay a single poll interval.
    #[test]
    fn drain_returns_at_once_when_groups_are_already_empty() {
        let result = await_group_drain(
            |_group| Ok(0),
            &groups(),
            Duration::from_secs(10),
            Duration::from_secs(10),
        );
        assert_eq!(result, Ok(()));
    }

    /// A group still populated at the deadline is a dirty scenario boundary:
    /// the error must name the group and its member count so the operator can
    /// see what was still attached.
    #[test]
    fn drain_deadline_error_names_the_lingering_group() {
        let result = await_group_drain(
            |_group| Ok(16),
            &groups(),
            Duration::from_millis(5),
            Duration::from_millis(1),
        );
        let err = result.expect_err("a never-draining group must error");
        assert!(
            err.contains("shove-stress-bench-consumer") && err.contains("16"),
            "error must name the group and member count, got: {err}"
        );
    }

    /// Transient list failures (broker mid-rebalance, metadata timeout) are
    /// retried like the topic probe's, not treated as a dirty boundary.
    #[test]
    fn drain_retries_past_transient_fetch_errors() {
        let mut polls = 0u32;
        let result = await_group_drain(
            |_group| {
                polls += 1;
                if polls < 3 {
                    Err("transient metadata timeout".to_string())
                } else {
                    Ok(0)
                }
            },
            &groups(),
            Duration::from_secs(10),
            Duration::from_millis(1),
        );
        assert_eq!(result, Ok(()));
    }

    /// A fetch error still failing at the deadline surfaces as the drain
    /// error rather than being silently swallowed by the retry.
    #[test]
    fn drain_deadline_error_surfaces_a_persistent_fetch_error() {
        let result = await_group_drain(
            |_group| Err("list groups: broker down".to_string()),
            &groups(),
            Duration::from_millis(5),
            Duration::from_millis(1),
        );
        let err = result.expect_err("a persistently failing fetch must error");
        assert!(err.contains("broker down"), "got: {err}");
    }
}
