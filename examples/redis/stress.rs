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
use shove::redis::{RedisConfig, RedisConsumerGroupConfig, RedisMode};
use shove::{Broker, Redis};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::{REDIS_PORT, Redis as RedisImage};

use harness::{HarnessConfig, run_all_scenarios};

const STREAM_KEY: &str = "shove-stress-bench";

#[tokio::main]
async fn main() {
    let container = RedisImage::default()
        .with_tag("7.0")
        .start()
        .await
        .expect("failed to start Redis container");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("failed to read Redis port");
    let url = format!("redis://127.0.0.1:{port}/");

    wait_until_ready(&url).await;

    let purge_url = url.clone();
    let purge: harness::PurgeFn = Box::new(move || {
        let url = purge_url.clone();
        Box::pin(async move {
            // Drop the stream itself — the next scenario's topology declare
            // recreates it together with the consumer group. XGROUP CREATE
            // uses MKSTREAM so this is safe.
            let Ok(client) = redis::Client::open(url) else {
                return;
            };
            let Ok(mut conn) = client.get_multiplexed_async_connection().await else {
                return;
            };
            let _: redis::RedisResult<i64> = conn.del(STREAM_KEY).await;
        })
    });

    let hcfg = HarnessConfig::<Redis>::new("redis").with_purge(purge);
    run_all_scenarios(
        hcfg,
        || {
            let url = url.clone();
            async move {
                Broker::<Redis>::new(RedisConfig {
                    mode: RedisMode::Standalone { url },
                    group: None,
                    ..Default::default()
                })
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

    // Explicit async cleanup. `ContainerAsync::Drop` spawns a background
    // task that may be aborted when the tokio runtime tears down, leaking
    // the Redis container. `rm()` runs synchronously here, so the container
    // is gone before `main` returns.
    container.rm().await.expect("remove Redis container");
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
