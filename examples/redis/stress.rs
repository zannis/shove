//! Stress benchmarks for the Redis Streams backend.
//!
//! Spins up a Redis testcontainer for the lifetime of the process. Requires a
//! running Docker daemon.
//!
//!     cargo run -q --example redis_stress --features redis-streams
//!     cargo run -q --example redis_stress --features redis-streams -- --tier moderate

#[path = "../common/stress_test.rs"]
mod harness;

use redis::AsyncCommands;
use shove::redis::{RedisConfig, RedisConsumer, RedisConsumerGroupConfig, RedisMode};
use shove::{Backend, Redis, Topic};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisImage};

use harness::{DlqDrainFn, HarnessConfig, StressTestTopic, run_all_scenarios};

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

    let hcfg = HarnessConfig::<Redis>::new("redis")
        .with_purge(purge)
        .with_broker("Redis Streams", REDIS_VERSION, "docker single-node")
        .with_dlq_drain(dlq_drain)
        .with_dlq_depth(dlq_depth);
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
