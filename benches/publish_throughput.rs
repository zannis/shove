//! Publish-throughput sanity benchmark.
//!
//! Measures publishes per second through `RabbitMqPublisher` against a real
//! testcontainers RabbitMQ broker. Used as a regression check for the
//! connection-recycling refactor — the ArcSwap-wrapped connection is on the
//! cold path (only touched by `create_*_channel`), so steady-state publish
//! throughput should be identical before and after.
//!
//! Run with:
//!
//!     cargo bench -q --features rabbitmq --bench publish_throughput
//!
//! Requires Docker.

use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use shove::rabbitmq::*;
use shove::*;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio::runtime::Runtime;

const PAYLOAD_BYTES: usize = 256;

#[derive(Serialize, Deserialize, Clone)]
struct BenchMsg {
    id: u64,
    payload: String,
}

define_topic!(
    PublishBenchTopic,
    BenchMsg,
    TopologyBuilder::new("publish-throughput-bench").build()
);

fn bench_publish(c: &mut Criterion) {
    if std::env::var("CI").is_ok() && std::env::var("BENCH_FORCE").is_err() {
        eprintln!("skipping publish_throughput bench in CI (set BENCH_FORCE=1 to run)");
        return;
    }

    let rt = Runtime::new().unwrap();
    let (_container, publisher) = rt.block_on(async {
        let container = RabbitMq::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5672).await.unwrap();
        let uri = format!("amqp://guest:guest@{host}:{port}/%2f");
        let client = RabbitMqClient::connect(&RabbitMqConfig::new(uri))
            .await
            .unwrap();

        let channel = client.create_channel().await.unwrap();
        RabbitMqTopologyDeclarer::new(channel)
            .declare(PublishBenchTopic::topology())
            .await
            .unwrap();

        let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();
        (container, publisher)
    });

    let payload = "x".repeat(PAYLOAD_BYTES);

    let mut group = c.benchmark_group("rabbitmq_publish");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("single_publish", |b| {
        b.to_async(&rt).iter(|| {
            let payload = payload.clone();
            let publisher = &publisher;
            async move {
                let msg = BenchMsg { id: 0, payload };
                publisher.publish::<PublishBenchTopic>(&msg).await.unwrap();
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_publish);
criterion_main!(benches);
