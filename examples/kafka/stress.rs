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
            // Leftover group state (offsets, rebalance epoch, dead members)
            // skews the next scenario, so a failed delete is a dirty
            // boundary, not a shrug. An absent group is the common clean
            // case; a still-emptying group can need a beat after the
            // consumers close, hence the bounded retry.
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
    let batch_consume: BatchConsumeFn<Kafka> = Box::new(|client, handler, opts| {
        Box::pin(async move {
            let consumer = KafkaConsumer::new(client);
            consumer
                .run_batch::<StressTestTopic, _>(handler, (), batch_consumer_options(opts))
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

/// Map the scenario's batch knobs onto shove's [`BatchConsumerOptions`].
///
/// Named (rather than inlined in the closure) so a test can prove the CLI
/// values end up inside `BatchConsumerOptions` instead of being parsed and
/// dropped — the defect this knob's ticket exists to close. Everything except
/// the two mapped fields stays at shove's defaults, exactly as the previous
/// hardcoded `BatchConsumerOptions::new()` did.
fn batch_consumer_options(opts: harness::BatchOptions) -> BatchConsumerOptions {
    BatchConsumerOptions::new()
        .with_max_batch_size(opts.max_batch_size.get())
        .with_max_batch_age(Duration::from_millis(opts.max_batch_age_ms.get()))
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

// Run with: cargo nextest run --features kafka,inmemory --test bench_harness_kafka
// (or --example kafka_stress; `inmemory` because compiling this file as a test
// target also compiles the shared harness's test module, which drives
// everything over `shove::InMemory`.)
//
// tests/bench_harness_kafka.rs bridges this module into a real test target,
// like tests/bench_harness.rs does for the harness's. No CI test leg enables
// kafka+inmemory together, so in CI the bridge is only type-checked (the
// `clippy --all-features --all-targets` gate); execution is local. The
// remaining CI-unexecuted surface is this file's builder-call wiring — the
// values themselves agree by construction (both sides cite
// `shove::DEFAULT_KAFKA_MAX_BATCH_SIZE` / `_AGE`), and the getters are covered
// by src's own `batch_consumer_options_tests`.
#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use super::*;

    #[test]
    fn the_cli_batch_knobs_reach_batch_consumer_options() {
        // The end of the knob's journey: CLI → `Scenario.batch_options` →
        // `BatchConsumeFn` (both proven in the harness tests) → here, into
        // the `BatchConsumerOptions` handed to `run_batch`. Read back through
        // shove's getters, not inferred from the builder calls.
        let opts = harness::BatchOptions {
            max_batch_size: NonZeroUsize::new(50).expect("non-zero"),
            max_batch_age_ms: NonZeroU64::new(125).expect("non-zero"),
        };
        let mapped = batch_consumer_options(opts);
        assert_eq!(mapped.max_batch_size(), 50);
        assert_eq!(mapped.max_batch_age(), Duration::from_millis(125));
    }

    #[test]
    fn the_default_invocation_measures_exactly_what_it_always_did() {
        // The harness default and shove's own default must be the same values,
        // or an un-flagged run would silently measure something new and every
        // previously recorded 500/250 row would stop being comparable. This is
        // the test the harness's `BatchOptions::default()` doc comment defers
        // to.
        let mapped = batch_consumer_options(harness::BatchOptions::default());
        let shove_default = BatchConsumerOptions::default();
        assert_eq!(mapped.max_batch_size(), shove_default.max_batch_size());
        assert_eq!(mapped.max_batch_age(), shove_default.max_batch_age());
    }
}
