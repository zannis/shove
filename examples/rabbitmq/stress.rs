//! Stress benchmarks for the RabbitMQ backend.
//!
//! Spins up a RabbitMQ testcontainer (with the `rabbitmq_consistent_hash_exchange`
//! plugin enabled) for the lifetime of the process. Requires a running Docker
//! daemon.
//!
//!     cargo run -q --example rabbitmq_stress --features rabbitmq
//!     cargo run -q --example rabbitmq_stress --features rabbitmq -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use std::time::Duration;

use lapin::options::QueuePurgeOptions;
use lapin::{Connection, ConnectionProperties};
use shove::rabbitmq as rmq;
use shove::{Backend, RabbitMq};
use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;

use harness::{DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

const QUEUE_NAME: &str = "shove-stress-bench";

/// Image tag started by `testcontainers_modules::rabbitmq`, recorded in the
/// results provenance so a reader knows which server produced the numbers.
const RABBITMQ_VERSION: &str = "3";

#[tokio::main]
async fn main() {
    let container = RabbitMqImage::default()
        .start()
        .await
        .expect("failed to start RabbitMQ container");
    let port = container
        .get_host_port_ipv4(5672)
        .await
        .expect("failed to read AMQP port");
    let mut exec = container
        .exec(ExecCommand::new([
            "rabbitmq-plugins",
            "enable",
            "rabbitmq_consistent_hash_exchange",
        ]))
        .await
        .expect("failed to enable consistent-hash plugin");
    let _ = exec.stdout_to_vec().await;

    let uri = format!("amqp://guest:guest@localhost:{port}");

    wait_until_ready(&uri).await;

    let purge_uri = uri.clone();
    let purge: harness::PurgeFn = Box::new(move || {
        let uri = purge_uri.clone();
        Box::pin(async move {
            // Drain leftover messages so each scenario starts with an empty
            // queue. The topology (exchanges / bindings) is idempotent, so
            // purging rather than deleting keeps scenario boot cost low.
            let Ok(conn) = Connection::connect(&uri, ConnectionProperties::default()).await else {
                return;
            };
            if let Ok(ch) = conn.create_channel().await {
                let _ = ch
                    .queue_purge(QUEUE_NAME.into(), QueuePurgeOptions::default())
                    .await;
            }
            let _ = conn.close(0, "purge done".into()).await;
        })
    });

    // An AMQP delivery must be settled on the channel it arrived on, so the
    // drain runs on the fill phase's own client rather than a fresh one.
    let dlq_drain: DlqDrainFn<RabbitMq> = Box::new(|client, handler| {
        Box::pin(async move {
            let consumer = rmq::RabbitMqConsumer::new(client);
            let _ = consumer.run_dlq::<StressTestTopic, _>(handler, ()).await;
        })
    });

    let hcfg = HarnessConfig::<RabbitMq>::new("rabbitmq")
        .with_purge(purge)
        .with_broker("RabbitMQ", RABBITMQ_VERSION, "docker single-node")
        .with_dlq_drain(dlq_drain);
    run_all_scenarios(
        hcfg,
        || {
            let uri = uri.clone();
            async move {
                <RabbitMq as Backend>::connect(rmq::RabbitMqConfig::new(&uri))
                    .await
                    .expect("connect RabbitMQ")
            }
        },
        |consumers, prefetch, concurrent| {
            rmq::RabbitMqConsumerGroupConfig::new(consumers..=consumers)
                .with_prefetch_count(prefetch)
                .with_concurrent_processing(concurrent)
        },
    )
    .await;

    drop(container);
}

/// Open and close one AMQP channel — confirms the broker is past startup and
/// the just-enabled `consistent_hash_exchange` plugin is loaded. Replaces a
/// blind `sleep(2s)` that was previously racing slow CI hosts.
async fn wait_until_ready(uri: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(conn) = Connection::connect(uri, ConnectionProperties::default()).await
            && conn.create_channel().await.is_ok()
        {
            let _ = conn.close(0, "ready probe".into()).await;
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("RabbitMQ did not become ready within 30s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
