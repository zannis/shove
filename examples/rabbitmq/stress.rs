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
use shove::batch_consumer::BatchConsumerOptions;
use shove::rabbitmq as rmq;
use shove::{Backend, Broker, RabbitMq, Topic};
use testcontainers::core::{CmdWaitFor, ExecCommand};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;

use harness::{BatchConsumeFn, DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

/// Image tag started by `testcontainers_modules::rabbitmq` (its pinned
/// default), recorded in the
/// results provenance so a reader knows which server produced the numbers.
const RABBITMQ_VERSION: &str = "3.8.22";

/// Memory the broker may hold before its memory alarm blocks publishers,
/// as an absolute amount (`MiB`: rabbitmqctl reads a bare `MB` as 10^6);
/// see the `set_vm_memory_high_watermark` exec in `main` for why the image
/// default is too low for the 64 KiB corpus.
///
/// Absolute rather than a fraction of the VM, because this number sets how
/// much of a six-million-message backlog a 3.8 classic queue keeps in RAM,
/// and that window sets the throughput of every small-payload consume cell.
/// The broker pages on RSS at half the limit, and RSS runs two to three
/// times Erlang's allocated bytes during a 64 B fill, so the window tracks
/// the limit closely. Measured 2026-09-10 on the 64 B consumer_group drain
/// at one consumer, everything else equal: a 9.6 GB limit (0.6 of a 16 GB
/// VM) held a ~1M-message window, filled in 262 s and drained at 12.2k
/// msg/s; 3.1 GB, 4.5 GB and 6 GiB held 240k to 580k, filled in 176 to
/// 181 s and drained at 18.8k, 19.5k and 17.8k. The fraction form gave the
/// 2026-09-09 re-run a limit three times the 2026-09-07 run's because the
/// VM had grown from 7.8 to 16 GB between them, and its multi-consumer 64 B
/// and 1 KiB cells read 20 to 45 % under the earlier pass for that reason
/// alone.
///
/// The floor is the 64 KiB corpus: 49 152 resident messages put the
/// broker's RSS at 5.1 GiB, so the 4.5 GB that 0.6 stood for on the 8 GB
/// VM it was chosen on trips the alarm at the end of every 64 KiB fill
/// (measured 2026-09-10, two of eight samples in alarm), while 6 GiB leaves
/// about 1 GiB of headroom and saw none. The VM needs room above this for
/// itself and the other containers; on 2026-09-09 an 8 GB VM thrashed once
/// the broker was allowed 6.2 GB, so run this harness on a 16 GB VM.
const RABBITMQ_MEMORY_HIGH_WATERMARK: &str = "6144MiB";

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
    // The 64 KiB drain corpus is 3.2 GB of queued messages, and the image's
    // default high watermark (0.4 of the Docker VM's memory, about 3.4 GB
    // on an 8 GB VM) sits right on top of it. Tripping the memory alarm
    // blocks the connection that is also registering the consumers, so on
    // an eight-consumer cell the last workers register seconds late and
    // the barrier rightly refuses a window most of the corpus has already
    // left. Pin the watermark to an absolute value the corpus clears; the
    // alarm is protecting a broker nothing else shares, and a fraction of
    // the VM would move the small-payload numbers with the VM's size (see
    // the constant). The exit-code condition is what makes the `expect`
    // mean "the watermark was set": without it a failing rabbitmqctl would
    // leave the default in place and the run would proceed into the very
    // alarm this guards against.
    let mut exec = container
        .exec(
            ExecCommand::new([
                "rabbitmqctl",
                "set_vm_memory_high_watermark",
                "absolute",
                RABBITMQ_MEMORY_HIGH_WATERMARK,
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .expect("failed to set the RabbitMQ memory high watermark");
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

    // The harness invokes it once per scenario consumer; every invocation
    // opens its own channel and basic.consumes the same queue, so N
    // invocations are N competing consumers over one corpus. That needs no
    // topology adjustment — where Kafka has to be declared with a partition
    // per consumer before a second member can be assigned any work, an AMQP
    // queue round-robins deliveries across whoever is subscribed.
    let batch_consume: BatchConsumeFn<RabbitMq> = Box::new(|client, handler, opts, stop| {
        Box::pin(async move {
            Broker::<RabbitMq>::from_client(client)
                .batch_consumer()
                .run::<StressTestTopic, _>(
                    handler,
                    (),
                    batch_consumer_options(opts).with_shutdown(stop),
                )
                .await
                .map_err(|e| format!("run_batch: {e}"))
        })
    });

    let hcfg = HarnessConfig::<RabbitMq>::new("rabbitmq")
        .with_purge(purge)
        .with_broker("RabbitMQ", RABBITMQ_VERSION, "docker single-node")
        .with_dlq_drain(dlq_drain)
        .with_dlq_depth(dlq_depth)
        .with_batch_consume(batch_consume);
    run_all_scenarios(
        hcfg,
        || {
            let uri = uri.clone();
            async move {
                harness::connect_with_retries("RabbitMQ", 10, Duration::from_secs(1), || {
                    let uri = uri.clone();
                    async move {
                        <RabbitMq as Backend>::connect(rmq::RabbitMqConfig::new(&uri))
                            .await
                            .map_err(|e| e.to_string())
                    }
                })
                .await
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

/// Map the scenario's batch knobs onto shove's [`BatchConsumerOptions`].
///
/// Named (rather than inlined in the closure) so a test can prove the CLI
/// values end up inside `BatchConsumerOptions` instead of being parsed and
/// dropped. Everything except the two mapped fields stays at shove's
/// defaults — the scenario's knobs are handed to the primitive, never
/// re-derived here. (This backend clamps `max_batch_size` to AMQP's u16
/// prefetch window inside `run_batch`, which is the primitive's business,
/// not this mapping's.)
fn batch_consumer_options(opts: harness::BatchOptions) -> BatchConsumerOptions<RabbitMq> {
    BatchConsumerOptions::new()
        .with_max_batch_size(opts.max_batch_size.get())
        .with_max_batch_age(Duration::from_millis(opts.max_batch_age_ms.get()))
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

// Example targets default to `test = false`, so this module only runs via
// tests/bench_harness_rabbitmq.rs, which pulls this file into a real test target.
#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use super::*;

    #[test]
    fn the_cli_batch_knobs_reach_batch_consumer_options() {
        // The end of the knob's journey: CLI → `Scenario.batch_options` →
        // `BatchConsumeFn` (both proven in the harness tests) → here, into the
        // `BatchConsumerOptions` handed to the generic batch consumer. Read
        // back through shove's getters, not inferred from the builder calls.
        let opts = harness::BatchOptions {
            max_batch_size: NonZeroUsize::new(50).expect("non-zero"),
            max_batch_age_ms: NonZeroU64::new(125).expect("non-zero"),
        };
        let mapped = batch_consumer_options(opts);
        assert_eq!(mapped.max_batch_size(), 50);
        assert_eq!(mapped.max_batch_age(), Duration::from_millis(125));
    }
}
