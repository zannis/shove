//! Benchmarks for the autoscaling consumer group system.
//!
//! Spawns a RabbitMQ container via testcontainers, runs benchmarks, and
//! cleans up automatically.
//!
//! Run with:
//!   cargo bench -q --features rabbitmq --bench autoscaler

#![cfg(feature = "rabbitmq")]

/// Panics at bench startup if Docker is not available.
fn require_docker() {
    match std::process::Command::new("docker").arg("info").output() {
        Ok(o) if o.status.success() => {}
        _ => panic!(
            "Docker is required to run these benchmarks. \
             Install Docker Desktop, colima, or podman and ensure the daemon is running."
        ),
    }
}

use criterion::{Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use shove::rabbitmq::*;
use shove::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

// ── Types ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Msg {
    id: u64,
}

define_topic!(BenchTopic, Msg, TopologyBuilder::new("bench").dlq().build());

#[derive(Clone)]
struct LatencyHandler {
    processed: Arc<AtomicU64>,
    delay: Duration,
}

impl MessageHandler<BenchTopic> for LatencyHandler {
    type Context = ();
    fn handle(
        &self,
        _: Msg,
        _: MessageMetadata,
        _: &(),
    ) -> impl Future<Output = Outcome> + Send {
        let processed = self.processed.clone();
        let delay = self.delay;
        async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            processed.fetch_add(1, Ordering::Relaxed);
            Outcome::Ack
        }
    }
}

// ── Infrastructure ───────────────────────────────────────────────────────────

struct Rabbit {
    client: RabbitMqClient,
    publisher: RabbitMqPublisher,
    mgmt_url: String,
    container: Option<testcontainers::ContainerAsync<RabbitMq>>,
}

impl Rabbit {
    async fn start() -> Self {
        require_docker();
        let container = RabbitMq::default().start().await.unwrap();
        let amqp_port = container.get_host_port_ipv4(5672).await.unwrap();
        let mgmt_port = container.get_host_port_ipv4(15672).await.unwrap();
        let host = container.get_host().await.unwrap().to_string();

        let uri = format!("amqp://guest:guest@{host}:{amqp_port}");
        let client = RabbitMqClient::connect(&RabbitMqConfig::new(&uri))
            .await
            .unwrap();

        let channel = client.create_channel().await.unwrap();
        RabbitMqTopologyDeclarer::new(channel)
            .declare(BenchTopic::topology())
            .await
            .unwrap();

        let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();

        Self {
            client,
            publisher,
            mgmt_url: format!("http://{host}:{mgmt_port}"),
            container: Some(container),
        }
    }

    async fn publish_n(&self, n: u64) {
        for id in 0..n {
            self.publisher
                .publish::<BenchTopic>(&Msg { id })
                .await
                .unwrap();
        }
    }

    async fn publish_batch(&self, n: u64) {
        let msgs: Vec<Msg> = (0..n).map(|id| Msg { id }).collect();
        self.publisher
            .publish_batch::<BenchTopic>(&msgs)
            .await
            .unwrap();
    }

    fn mgmt_config(&self) -> ManagementConfig {
        ManagementConfig::new(&self.mgmt_url, "guest", "guest")
    }

    fn new_registry(&self) -> ConsumerGroupRegistry {
        ConsumerGroupRegistry::new(self.client.clone())
    }

    /// Shut down the client and drop the container inside the runtime.
    async fn shutdown(mut self) {
        self.client.shutdown().await;
        drop(self.container.take());
    }
}

async fn drain(counter: &AtomicU64, target: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while counter.load(Ordering::Relaxed) < target {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timeout: processed {} / {target}",
            counter.load(Ordering::Relaxed)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

fn bench_autoscaler_decisions(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rabbit = rt.block_on(async {
        let r = Rabbit::start().await;
        r.publish_n(100).await;
        // Wait for management stats to catch up.
        tokio::time::sleep(Duration::from_secs(2)).await;
        r
    });

    let mut g = c.benchmark_group("autoscaler_decisions");
    g.sample_size(10);

    g.bench_function("poll_management_api", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut registry = rabbit.new_registry();
                registry
                    .register::<BenchTopic, LatencyHandler>(ConsumerGroupConfig::new(1..=1), || {
                        LatencyHandler {
                            processed: Arc::new(AtomicU64::new(0)),
                            delay: Duration::ZERO,
                        }
                    })
                    .await
                    .unwrap();
                registry.start_all();

                let registry = Arc::new(Mutex::new(registry));
                let mgmt_config = rabbit.mgmt_config();
                let mut autoscaler = RabbitMqAutoscalerBackend::autoscaler(
                    &mgmt_config,
                    registry.clone(),
                    AutoscalerConfig {
                        poll_interval: Duration::from_millis(1),
                        hysteresis_duration: Duration::ZERO,
                        cooldown_duration: Duration::ZERO,
                        ..Default::default()
                    },
                );

                let shutdown = CancellationToken::new();
                let handle = tokio::spawn({
                    let s = shutdown.clone();
                    async move { autoscaler.run(s).await }
                });

                tokio::time::sleep(Duration::from_millis(10)).await;
                shutdown.cancel();
                let _ = handle.await;
                registry.lock().await.shutdown_all().await;
            });
        });
    });

    rt.block_on(rabbit.shutdown());
    g.finish();
}

fn bench_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rabbit = rt.block_on(Rabbit::start());

    let mut g = c.benchmark_group("throughput");
    g.sample_size(10);

    for &(n, consumers, delay_ms) in &[
        (500u64, 1u16, 10u64),
        (500, 4, 10),
        (500, 8, 10),
        (500, 16, 10),
        (100, 1, 175), // avg of 50-300ms
        (100, 4, 175),
        (100, 8, 175),
    ] {
        let prefetch = (n as u16 / consumers).max(1);

        g.bench_function(format!("{n}msg_{consumers}c_{delay_ms}ms"), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        rabbit.publish_n(n).await;

                        let processed = Arc::new(AtomicU64::new(0));
                        let pc = processed.clone();
                        let delay = Duration::from_millis(delay_ms);

                        let mut registry = rabbit.new_registry();
                        registry
                            .register::<BenchTopic, LatencyHandler>(
                                ConsumerGroupConfig::new(consumers..=consumers)
                                    .with_prefetch_count(prefetch),
                                move || LatencyHandler {
                                    processed: pc.clone(),
                                    delay,
                                },
                            )
                            .await
                            .unwrap();

                        let start = std::time::Instant::now();
                        registry.start_all();
                        drain(&processed, n).await;
                        total += start.elapsed();

                        registry.shutdown_all().await;
                    }
                    total
                })
            });
        });
    }

    rt.block_on(rabbit.shutdown());
    g.finish();
}

fn bench_burst_autoscaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rabbit = rt.block_on(Rabbit::start());

    let mut g = c.benchmark_group("burst_autoscaling");
    g.sample_size(10);

    let burst: u64 = 200;
    let delay = Duration::from_millis(20);

    for &(label, min, max) in &[
        ("fixed_1", 1u16, 1u16),
        ("scale_2_4", 2, 4),
        ("scale_4_8", 4, 8),
    ] {
        let prefetch = (burst as u16 / max).max(1);

        g.bench_function(format!("{burst}burst_20ms_{label}"), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        rabbit.publish_batch(burst).await;

                        let processed = Arc::new(AtomicU64::new(0));
                        let pc = processed.clone();

                        let mut registry = rabbit.new_registry();
                        registry
                            .register::<BenchTopic, LatencyHandler>(
                                ConsumerGroupConfig::new(min..=max).with_prefetch_count(prefetch),
                                move || LatencyHandler {
                                    processed: pc.clone(),
                                    delay,
                                },
                            )
                            .await
                            .unwrap();

                        let start = std::time::Instant::now();
                        registry.start_all();

                        let registry = Arc::new(Mutex::new(registry));
                        let shutdown = CancellationToken::new();
                        let handle = if max > min {
                            let mgmt_config = rabbit.mgmt_config();
                            let mut autoscaler = RabbitMqAutoscalerBackend::autoscaler(
                                &mgmt_config,
                                registry.clone(),
                                AutoscalerConfig {
                                    poll_interval: Duration::from_secs(1),
                                    scale_up_multiplier: 1.5,
                                    hysteresis_duration: Duration::from_secs(2),
                                    cooldown_duration: Duration::from_secs(3),
                                    ..Default::default()
                                },
                            );
                            let s = shutdown.clone();
                            Some(tokio::spawn(async move { autoscaler.run(s).await }))
                        } else {
                            None
                        };

                        drain(&processed, burst).await;
                        total += start.elapsed();

                        shutdown.cancel();
                        if let Some(h) = handle {
                            let _ = h.await;
                        }
                        registry.lock().await.shutdown_all().await;
                    }
                    total
                })
            });
        });
    }

    rt.block_on(rabbit.shutdown());
    g.finish();
}

criterion_group!(
    benches,
    bench_autoscaler_decisions,
    bench_throughput,
    bench_burst_autoscaling,
);
criterion_main!(benches);
