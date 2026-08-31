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
use shove::kafka::{BatchConsumerOptions, KafkaConfig, KafkaConsumer, KafkaConsumerGroupConfig};
use shove::{Backend, Kafka};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaImage};

use harness::{BatchConsumeFn, DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

const TOPIC_NAME: &str = "shove-stress-bench";
const DLQ_NAME: &str = "shove-stress-bench-dlq";

/// Image tag started by `testcontainers_modules::kafka::apache`, recorded in
/// the results provenance so a reader knows which broker produced the numbers.
const KAFKA_VERSION: &str = "3.9";

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
    let purge: harness::PurgeFn = Box::new(move || {
        let bootstrap = purge_bootstrap.clone();
        Box::pin(async move {
            // Delete the topics AND the consumer groups derived from them.
            // The topic delete on its own resets storage and lets the next
            // scenario re-declare with a fresh partition count sized to its
            // own `max_consumers`, but it leaves the consumer group state
            // (offsets, rebalance epoch, dead members) sitting in Kafka's
            // group coordinator. With many short scenarios that residue
            // accumulates and the next scenario eats a long rebalance
            // before steady-state, flattening apparent throughput. Wiping
            // the group too gives each scenario a clean baseline.
            let Ok(admin): Result<AdminClient<DefaultClientContext>, _> = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .create()
            else {
                return;
            };
            let _ = admin
                .delete_topics(&[TOPIC_NAME, DLQ_NAME], &AdminOptions::new())
                .await;
            let main_group = format!("{TOPIC_NAME}-consumer");
            let dlq_group = format!("{DLQ_NAME}-consumer");
            let _ = admin
                .delete_groups(&[&main_group, &dlq_group], &AdminOptions::new())
                .await;
        })
    });

    let dlq_drain: DlqDrainFn<Kafka> = Box::new(|client, handler| {
        Box::pin(async move {
            let consumer = KafkaConsumer::new(client);
            let _ = consumer.run_dlq::<StressTestTopic, _>(handler, ()).await;
        })
    });

    // Kafka is the only backend that has `run_batch` at all, which is why it
    // is the only wrapper supplying this closure — and why `consume_batch`
    // lands in every other backend's `unsupported[]` instead of being faked.
    let batch_consume: BatchConsumeFn<Kafka> = Box::new(|client, handler| {
        Box::pin(async move {
            let consumer = KafkaConsumer::new(client);
            let _ = consumer
                .run_batch::<StressTestTopic, _>(handler, (), BatchConsumerOptions::new())
                .await;
        })
    });

    let hcfg = HarnessConfig::<Kafka>::new("kafka")
        .with_purge(purge)
        .with_broker("Apache Kafka", KAFKA_VERSION, "docker single-node (KRaft)")
        .with_dlq_drain(dlq_drain)
        .with_batch_consume(batch_consume);
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
