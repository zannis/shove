//! Stress benchmarks for the Redis Streams backend.
//!
//! Spins up a Redis testcontainer for the lifetime of the process. Requires a
//! running Docker daemon.
//!
//!     cargo run -q --example redis_stress --features redis-streams
//!     cargo run -q --example redis_stress --features redis-streams -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use std::time::Duration;

use redis::AsyncCommands;
use shove::batch_consumer::BatchConsumerOptions;
use shove::redis::{RedisConfig, RedisConsumer, RedisConsumerGroupConfig, RedisMode};
use shove::{Backend, Broker, Redis, Topic};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisImage};

use harness::{BatchConsumeFn, DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

/// Image tag pinned by the `.with_tag("7.0")` call below, recorded in the
/// results provenance so a reader knows which server produced the numbers.
const REDIS_VERSION: &str = "7.0";

#[tokio::main]
async fn main() {
    harness::spawn_ctrlc_watcher();
    let container = RedisImage::default()
        .with_tag("7.0")
        .start()
        .await
        .expect("failed to start Redis container");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("failed to read Redis port");
    let _container = harness::ContainerGuard::new(container);
    let url = format!("redis://127.0.0.1:{port}/");

    wait_until_ready(&url).await;

    let purge_url = url.clone();
    let purge: harness::PurgeFn = Box::new(move |topology| {
        let url = purge_url.clone();
        Box::pin(async move {
            // Drop every key the topology owns — the next scenario's declare
            // recreates them together with the consumer groups. XGROUP CREATE
            // uses MKSTREAM so this is safe. DEL is idempotent, so absent
            // keys cost nothing.
            //
            // The set is derived from the topology handed in: main stream,
            // DLQ stream, hold-queue streams and their `:pending` sorted
            // sets, and for a sequenced topology the per-shard streams plus
            // their own hold pairs (`src/backends/redis/topology.rs` naming).
            let mut keys: Vec<String> = vec![topology.queue().to_string()];
            if let Some(dlq) = topology.dlq() {
                keys.push(dlq.to_string());
            }
            for hq in topology.hold_queues() {
                keys.push(hq.name().to_string());
                keys.push(format!("{}:pending", hq.name()));
            }
            if let Some(seq) = topology.sequencing() {
                for shard in 0..seq.routing_shards() {
                    keys.push(format!("{}-seq-{shard}", topology.queue()));
                    for hq in topology.shard_hold_queue_names(shard) {
                        keys.push(hq.name().to_string());
                        keys.push(format!("{}:pending", hq.name()));
                    }
                }
            }

            let client = redis::Client::open(url).map_err(|e| format!("client: {e}"))?;
            let mut conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| format!("connect: {e}"))?;
            let _: i64 = conn.del(&keys).await.map_err(|e| format!("DEL: {e}"))?;
            Ok(())
        })
    });

    // The DLQ is a plain stream key, so XLEN is its exact depth. Supplying
    // the probe makes the fill's completion signal the DLQ population itself
    // rather than handler-invocation counts, which are hostage to each
    // backend's retry-gate ordering.
    let depth_url = url.clone();
    let dlq_depth: harness::DlqDepthFn = Box::new(move || {
        let url = depth_url.clone();
        Box::pin(async move {
            let dlq = StressTestTopic::topology()
                .dlq()
                .ok_or_else(|| "stress topology has no DLQ".to_string())?;
            let client = redis::Client::open(url).map_err(|e| format!("client: {e}"))?;
            let mut conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| format!("connect: {e}"))?;
            let depth: u64 = redis::cmd("XLEN")
                .arg(dlq)
                .query_async(&mut conn)
                .await
                .map_err(|e| format!("XLEN {dlq}: {e}"))?;
            Ok(depth)
        })
    });

    // Same client for fill and drain: a second connection would race the
    // first rather than read what it produced.
    let dlq_drain: DlqDrainFn<Redis> = Box::new(|client, handler, stop| {
        Box::pin(async move {
            // Redis's `run_dlq` runs until its task is dropped — `close` is a
            // no-op on this backend — so the scenario stop token is its only
            // stop signal. Cancelling at the `select!` boundary drops the
            // drain at an await point, exactly what the abort it replaces did.
            let consumer = RedisConsumer::new(client);
            tokio::select! {
                result = consumer.run_dlq::<StressTestTopic, _>(handler, ()) => {
                    result.map_err(|e| format!("run_dlq: {e}"))
                }
                () = stop.cancelled() => Ok(()),
            }
        })
    });

    // The harness invokes it once per scenario consumer; every invocation
    // XREADGROUPs the same stream under the client's group with its own
    // generated consumer name, so N invocations split one corpus like N group
    // members. That needs no topology adjustment — where Kafka has to be
    // declared with a partition per consumer before a second member can be
    // assigned any work, a Redis consumer group hands each entry to exactly
    // one of however many names read from it.
    let batch_consume: BatchConsumeFn<Redis> = Box::new(|client, handler, opts, stop| {
        Box::pin(async move {
            Broker::<Redis>::from_client(client)
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

    let hcfg = HarnessConfig::<Redis>::new("redis")
        .with_purge(purge)
        .with_broker("Redis Streams", REDIS_VERSION, "docker single-node")
        .with_dlq_drain(dlq_drain)
        .with_dlq_depth(dlq_depth)
        .with_batch_consume(batch_consume);
    run_all_scenarios(
        hcfg,
        || {
            let url = url.clone();
            async move {
                <Redis as Backend>::connect(RedisConfig::new(RedisMode::Standalone { url }))
                    .await
                    .expect("connect Redis")
            }
        },
        |consumers, prefetch, concurrent| {
            RedisConsumerGroupConfig::new(consumers..=consumers)
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
/// re-derived here.
fn batch_consumer_options(opts: harness::BatchOptions) -> BatchConsumerOptions<Redis> {
    BatchConsumerOptions::new()
        .with_max_batch_size(opts.max_batch_size.get())
        .with_max_batch_age(Duration::from_millis(opts.max_batch_age_ms.get()))
}

/// Block until `PING` returns `PONG`. Testcontainers exits `.start()` once
/// Redis logs "Ready to accept connections", but the multiplexed-connection
/// handshake can still race; this confirms the server actually serves a
/// command before the first scenario starts measuring.
async fn wait_until_ready(url: &str) {
    let client = redis::Client::open(url).expect("build Redis probe client");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
            let pong: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
            if matches!(pong, Ok(ref s) if s == "PONG") {
                return;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("Redis did not become ready within 30s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// Example targets default to `test = false`, so this module only runs via
// tests/bench_harness_redis.rs, which pulls this file into a real test target.
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
