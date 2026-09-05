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

use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties};
use shove::rabbitmq as rmq;
use shove::{Backend, RabbitMq, Topic};
use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;

use harness::{DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

/// Image tag started by `testcontainers_modules::rabbitmq` (its pinned
/// default), recorded in the
/// results provenance so a reader knows which server produced the numbers.
const RABBITMQ_VERSION: &str = "3.8.22";

#[tokio::main]
async fn main() {
    harness::spawn_ctrlc_watcher();
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
    let _container = harness::ContainerGuard::new(container);

    let uri = format!("amqp://guest:guest@localhost:{port}");

    wait_until_ready(&uri).await;

    let purge_uri = uri.clone();
    let purge: harness::PurgeFn = Box::new(move |topology| {
        let uri = purge_uri.clone();
        Box::pin(async move {
            // Delete every queue the topology owns so each scenario starts
            // empty: main queue, DLQ, hold queues, and for a sequenced
            // topology the per-shard queues plus their own hold queues
            // (`{queue}-seq-{i}`, `src/backends/rabbitmq/topology.rs`
            // naming). Delete rather than purge: `queue_delete` on an absent
            // queue succeeds (purge errors and closes the channel), and the
            // declare that follows every purge recreates the topology anyway.
            let mut queues: Vec<String> = vec![topology.queue().to_string()];
            if let Some(dlq) = topology.dlq() {
                queues.push(dlq.to_string());
            }
            for hq in topology.hold_queues() {
                queues.push(hq.name().to_string());
            }
            if let Some(seq) = topology.sequencing() {
                for shard in 0..seq.routing_shards() {
                    queues.push(format!("{}-seq-{shard}", topology.queue()));
                    for hq in topology.shard_hold_queue_names(shard) {
                        queues.push(hq.name().to_string());
                    }
                }
            }

            let conn = Connection::connect(&uri, ConnectionProperties::default())
                .await
                .map_err(|e| format!("connect: {e}"))?;
            let ch = conn
                .create_channel()
                .await
                .map_err(|e| format!("channel: {e}"))?;
            for queue in &queues {
                ch.queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
                    .await
                    .map_err(|e| format!("delete queue {queue}: {e}"))?;
            }
            let _ = conn.close(0, "purge done".into()).await;
            Ok(())
        })
    });

    // RabbitMQ's pre-handler retry gate (`retries_exhausted(0, 0)` is true)
    // dead-letters the fill's messages without ever invoking the handler, so
    // the fill's invocation counter never moves — the DLQ itself is the only
    // truthful completion signal. Passive declare reports the queue depth.
    let depth_uri = uri.clone();
    let dlq_depth: harness::DlqDepthFn = Box::new(move || {
        let uri = depth_uri.clone();
        Box::pin(async move {
            let dlq = StressTestTopic::topology()
                .dlq()
                .ok_or_else(|| "stress topology has no DLQ".to_string())?;
            let conn = Connection::connect(&uri, ConnectionProperties::default())
                .await
                .map_err(|e| format!("connect: {e}"))?;
            let ch = conn
                .create_channel()
                .await
                .map_err(|e| format!("channel: {e}"))?;
            let queue = ch
                .queue_declare(
                    dlq.into(),
                    QueueDeclareOptions {
                        passive: true,
                        ..QueueDeclareOptions::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| format!("passive declare {dlq}: {e}"))?;
            let depth = queue.message_count() as u64;
            let _ = conn.close(0, "depth probe".into()).await;
            Ok(depth)
        })
    });

    // An AMQP delivery must be settled on the channel it arrived on, so the
    // drain runs on the fill phase's own client rather than a fresh one.
    let dlq_drain: DlqDrainFn<RabbitMq> = Box::new(|client, handler, _stop| {
        // This backend's `run_dlq` exits when the teardown closes the client;
        // the stop token is for backends without that path (see `DlqDrainFn`).
        Box::pin(async move {
            let consumer = rmq::RabbitMqConsumer::new(client);
            consumer
                .run_dlq::<StressTestTopic, _>(handler, ())
                .await
                .map_err(|e| format!("run_dlq: {e}"))
        })
    });

    let hcfg = HarnessConfig::<RabbitMq>::new("rabbitmq")
        .with_purge(purge)
        .with_broker("RabbitMQ", RABBITMQ_VERSION, "docker single-node")
        .with_dlq_drain(dlq_drain)
        .with_dlq_depth(dlq_depth);
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
