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
use shove::{Broker, RabbitMq};
use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;

use harness::{HarnessConfig, run_all_scenarios};

const QUEUE_NAME: &str = "shove-stress-bench";

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
    tokio::time::sleep(Duration::from_secs(2)).await;

    let uri = format!("amqp://guest:guest@localhost:{port}");

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

    let hcfg = HarnessConfig::<RabbitMq>::new("rabbitmq").with_purge(purge);
    run_all_scenarios(
        hcfg,
        || {
            let uri = uri.clone();
            async move {
                Broker::<RabbitMq>::new(rmq::RabbitMqConfig::new(&uri))
                    .await
                    .expect("connect RabbitMQ")
            }
        },
        |consumers, prefetch, concurrent| {
            rmq::ConsumerGroupConfig::new(consumers..=consumers)
                .with_prefetch_count(prefetch)
                .with_concurrent_processing(concurrent)
        },
    )
    .await;

    drop(container);
}
