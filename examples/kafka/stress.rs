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
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use shove::kafka::{
    BatchConsumerOptions, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig,
    KafkaTopologyDeclarer,
};
use shove::{Backend, Kafka, Topic};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaImage};

use harness::{
    BatchConsumeFn, ConsumeTopologyFn, DlqDrainFn, HarnessConfig, ReadinessProbeFn,
    StressTestTopic, run_all_scenarios,
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
    // The probe deliberately sets no `group.id`: it is never subscribed, so a
    // group would only add coordinator state for the purges to clean up.
    // rdkafka still queues error/log events on the client's main queue and
    // metadata fetches never serve it, so each purge drains it (before the
    // topic wait below) — otherwise broker-flap noise would accumulate for
    // the process lifetime inside the very process whose RSS the bench
    // reports.
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
            //
            // Only refusals that can still improve ride the deadline:
            // `NonEmptyGroup` (the wait this loop exists for), the
            // coordinator-transient codes a broker answers with while it is
            // still settling, and librdkafka's client-internal codes (all
            // negative — timeout, transport), which are request-level
            // failures wearing per-group clothes: `DeleteGroups` fans out one
            // request per coordinator and merges a failed sub-request into
            // the per-group results. Anything else — authorization, invalid
            // group, unsupported API — would answer identically at the
            // deadline, so it fails the purge immediately. Waiting on
            // transport failures is deliberate: this process owns the broker
            // it started, so one that stops answering ends the run
            // regardless, and a transient blip must not cost a scenario its
            // matrix cell.
            //
            // The settle loop's RPCs are capped so the deadline bounds the
            // loop's actual wall clock — with no request timeout an admin
            // call blocks up to librdkafka's 60 s `socket.timeout.ms` default
            // against a hung broker, tripling the ceiling it promises.
            let settle_opts = AdminOptions::new().request_timeout(Some(ADMIN_RPC_TIMEOUT));
            let group_refs: Vec<&str> = groups.iter().map(String::as_str).collect();
            let settle_started = std::time::Instant::now();
            loop {
                let mut waiting: Vec<String> = Vec::new();
                match admin.delete_groups(&group_refs, &settle_opts).await {
                    Ok(results) => {
                        for result in results {
                            match result {
                                Ok(_) | Err((_, RDKafkaErrorCode::GroupIdNotFound)) => {}
                                Err((group, code))
                                    if (code as i32) < 0
                                        || matches!(
                                            code,
                                            RDKafkaErrorCode::NonEmptyGroup
                                                | RDKafkaErrorCode::CoordinatorLoadInProgress
                                                | RDKafkaErrorCode::CoordinatorNotAvailable
                                                | RDKafkaErrorCode::NotCoordinator
                                        ) =>
                                {
                                    waiting.push(format!("{group}: {code}"));
                                }
                                Err((group, code)) => {
                                    return Err(format!("delete group {group}: {code}"));
                                }
                            }
                        }
                    }
                    Err(e) => waiting.push(format!("delete request failed: {e}")),
                }
                if waiting.is_empty() {
                    break;
                }
                if settle_started.elapsed() >= GROUP_SETTLE_DEADLINE {
                    return Err(format!(
                        "delete groups: still refused after {:?}: {}",
                        settle_started.elapsed(),
                        waiting.join(", ")
                    ));
                }
                tokio::time::sleep(GROUP_SETTLE_POLL).await;
            }

            // Deliberately uncapped, unlike the settle loop's RPCs: librdkafka
            // defaults `DeleteTopics`' operation timeout to `socket.timeout.ms`
            // (60 s), meaning the broker may legitimately hold the response
            // while a many-partition deletion completes — and this call has no
            // retry loop around it, so a tight client cap would abandon (and
            // fail the purge on) a deletion that was in fact succeeding.
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
                // Serve the probe's event queue while we are here: nothing
                // else ever polls this consumer, so error events rdkafka
                // queued during a broker flap would otherwise sit on it for
                // the life of the process. Caveat: `poll` also returns `None`
                // after consuming a non-returnable event (stats, OAuth), so
                // this drains fully only while error events are the sole kind
                // enabled — do not turn on `statistics.interval.ms` here
                // without revisiting it.
                while topic_probe.poll(Duration::ZERO).is_some() {}
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

    let dlq_drain: DlqDrainFn<Kafka> = Box::new(|client, handler, _stop| {
        // This backend's `run_dlq` exits when the teardown closes the client;
        // the stop token is for backends without that path (see `DlqDrainFn`).
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
    let batch_consume: BatchConsumeFn<Kafka> = Box::new(|client, handler, opts, stop| {
        Box::pin(async move {
            let consumer = KafkaConsumer::new(client);
            consumer
                .run_batch::<StressTestTopic, _>(
                    handler,
                    (),
                    batch_consumer_options(opts).with_shutdown(stop),
                )
                .await
                .map_err(|e| format!("run_batch: {e}"))
        })
    });

    // The generic declare creates the topic with Kafka's default partition
    // count; a batch scenario claiming N consumers needs at least N
    // partitions or the extra consumers sit idle while the row counts them.
    let consume_topology: ConsumeTopologyFn<Kafka> = Box::new(|client, consumers| {
        Box::pin(async move {
            KafkaTopologyDeclarer::new(client)
                .with_min_partitions(consumers as i32)
                .declare(StressTestTopic::topology())
                .await
                .map_err(|e| format!("declare: {e}"))
        })
    });

    // The generic publisher cannot probe a Kafka group: librdkafka's sticky
    // partitioner puts every null-key record of a concurrently submitted round
    // on one partition, so a round reaches one member. Write one sentinel to
    // every partition instead, which reaches every assigned member in a round.
    let probe_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .expect("build readiness probe producer");
    let readiness_probe: ReadinessProbeFn = Box::new(move |topic, payload| {
        let producer = probe_producer.clone();
        Box::pin(async move {
            let meta_producer = producer.clone();
            let partitions = tokio::task::spawn_blocking(move || {
                meta_producer
                    .client()
                    .fetch_metadata(Some(topic), Duration::from_secs(5))
                    .map_err(|e| format!("fetch metadata for {topic}: {e}"))
                    .and_then(|m| {
                        m.topics()
                            .iter()
                            .find(|t| t.name() == topic)
                            .map(|t| t.partitions().len())
                            .ok_or_else(|| format!("topic {topic} missing from metadata"))
                    })
            })
            .await
            .map_err(|e| format!("metadata task: {e}"))??;
            for partition in 0..partitions {
                let partition = i32::try_from(partition)
                    .map_err(|_| format!("partition index {partition} exceeds i32"))?;
                producer
                    .send(
                        FutureRecord::<(), [u8]>::to(topic)
                            .partition(partition)
                            .payload(&payload),
                        Timeout::After(Duration::from_secs(5)),
                    )
                    .await
                    .map_err(|(e, _)| format!("probe partition {partition}: {e}"))?;
            }
            Ok(())
        })
    });

    let hcfg = HarnessConfig::<Kafka>::new("kafka")
        .with_purge(purge)
        .with_broker("Apache Kafka", KAFKA_VERSION, "docker single-node (KRaft)")
        .with_dlq_drain(dlq_drain)
        .with_batch_consume(batch_consume)
        .with_consume_topology(consume_topology)
        .with_readiness_probe(readiness_probe)
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
/// every remaining scenario for minutes. Checked between attempts, so the
/// wait can overshoot by one poll plus one capped RPC (bounded, unlike the
/// 60 s default RPC timeout it replaces); the error reports the time actually
/// paid.
const GROUP_SETTLE_DEADLINE: Duration = Duration::from_secs(30);

/// Poll interval of the purge's group-delete wait.
const GROUP_SETTLE_POLL: Duration = Duration::from_millis(500);

/// Per-request ceiling on the settle loop's `DeleteGroups` RPCs — the loop
/// retries, so a cap costs nothing but keeps `GROUP_SETTLE_DEADLINE` honest
/// against a hung broker (an uncapped admin call blocks up to librdkafka's
/// 60 s `socket.timeout.ms` default before the deadline is even checked).
/// Generous next to `PROBE_RPC_TIMEOUT` because a delete carries coordinator
/// work a metadata fetch does not. The one-shot `delete_topics` call stays
/// uncapped on purpose — see the comment at its call site.
const ADMIN_RPC_TIMEOUT: Duration = Duration::from_secs(5);

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
