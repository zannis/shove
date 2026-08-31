//! Consumer-count overhead benchmark for shove.
//!
//! Measures what shove itself costs as a consumer group scales out, with the
//! broker removed: the in-process backend is the substrate, so nothing here
//! reports a broker's queueing behaviour.
//!
//! Metrics:
//!   - **Startup** — registering `n` consumers and starting them.
//!   - **Shutdown** — `shutdown_all()` wall-clock duration.
//!   - **Dispatch** — publish-to-handler throughput with consumers
//!     pre-started and a fixed message count, so there is no backlog.
//!   - **RSS per consumer** — incremental memory cost of each consumer.
//!   - **Idle CPU** — CPU usage with consumers running but no messages flowing.
//!
//! The first three go through criterion. The last two do not: criterion
//! measures elapsed time, and memory and CPU share are not times. They are
//! sampled by a probe that runs once before the timing groups and prints to
//! stderr — dropping them would lose two of the five things this benchmark
//! exists to report.
//!
//! Run with:
//!
//!     cargo bench --no-default-features --features inmemory --bench consumer_overhead
//!
//! No Docker, no broker.
//!
//! # What moving off a real broker changed
//!
//! The in-process backend has no broker-side consumer registry to poll, so
//! **startup** measures registration plus task spawn, not the broker
//! acknowledging each consumer. That is the intended trade: this tier isolates
//! shove's own cost, and consumer-count scaling against real brokers belongs
//! to the stress harness, which sweeps consumer counts across all six
//! backends.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(target_os = "macos")]
use mach2::traps::mach_task_self;

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use serde::{Deserialize, Serialize};
use shove::inmemory::{InMemoryBroker, InMemoryConsumerGroupConfig, InMemoryConsumerGroupRegistry};
use shove::{
    Broker, InMemory, MessageHandler, MessageMetadata, Outcome, Publisher, TopologyBuilder,
    define_topic,
};
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::time::timeout;

use common::{PAYLOAD_SIZES, payload};

// ── Tuning ──────────────────────────────────────────────────────────────────

/// Consumer counts swept by every group.
///
/// The hand-rolled predecessor swept 128..4096 once per count. Criterion
/// repeats each point across a sample loop, so the top of that range would run
/// for hours while adding nothing: the scaling shape is already visible here.
const CONSUMER_COUNTS: [u16; 4] = [1, 16, 64, 256];

/// Messages per dispatch iteration.
const DISPATCH_MESSAGES: u64 = 512;

/// How long the resource probe samples idle CPU for.
const IDLE_SAMPLE: Duration = Duration::from_secs(2);

/// Ceiling on the wait for a dispatch round to complete. Nothing should come
/// close; it exists so a lost delivery fails the run rather than hanging it.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(60);

// ── Topic & message ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchMsg {
    id: u64,
    payload: String,
}

define_topic!(
    OverheadTopic,
    BenchMsg,
    TopologyBuilder::new("overhead-bench").dlq().build()
);

// ── Handler ─────────────────────────────────────────────────────────────────

/// Counts deliveries and signals once `target` have arrived. Does no work of
/// its own, so the groups below report framework cost only.
#[derive(Clone)]
struct BenchHandler {
    seen: Arc<AtomicU64>,
    target: u64,
    done: Arc<Notify>,
}

impl BenchHandler {
    fn new(target: u64) -> Self {
        Self {
            seen: Arc::new(AtomicU64::new(0)),
            target,
            done: Arc::new(Notify::new()),
        }
    }

    /// Resolves once `target` messages have been handled. `notify_one` stores
    /// a permit when no waiter is parked, so a completion cannot be missed.
    ///
    /// Bounded so a lost delivery fails the run instead of hanging it: an
    /// unbounded wait here is `cargo bench` producing no further output until
    /// CI kills the job, which reads as "slow" rather than "broken".
    async fn completed(&self) {
        if timeout(COMPLETION_TIMEOUT, self.done.notified())
            .await
            .is_err()
        {
            panic!(
                "handler saw {} of {} messages in {COMPLETION_TIMEOUT:?}: a delivery was lost",
                self.seen.load(Ordering::Relaxed),
                self.target,
            );
        }
    }
}

impl MessageHandler<OverheadTopic> for BenchHandler {
    type Context = ();

    async fn handle(&self, _msg: BenchMsg, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        if self.seen.fetch_add(1, Ordering::Relaxed).saturating_add(1) == self.target {
            self.done.notify_one();
        }
        Outcome::Ack
    }
}

// ── Fixtures ────────────────────────────────────────────────────────────────

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build bench runtime")
}

/// A broker nothing has touched yet, with the bench topic declared.
///
/// Every iteration gets its own: a registry's shutdown cascades to the client
/// it was built from, so a reused client would be retired after the first
/// round and record nothing on the second.
async fn fresh_broker() -> (InMemoryBroker, Broker<InMemory>) {
    let client = InMemoryBroker::new();
    let broker = Broker::<InMemory>::from_client(client.clone());
    broker
        .topology()
        .declare::<OverheadTopic>()
        .await
        .expect("declare bench topology");
    (client, broker)
}

/// A registry with `consumers` consumers registered but not started.
async fn registry(
    client: &InMemoryBroker,
    consumers: u16,
    handler: BenchHandler,
) -> InMemoryConsumerGroupRegistry {
    let mut registry = InMemoryConsumerGroupRegistry::new(client.clone());
    registry
        .register::<OverheadTopic, BenchHandler>(
            InMemoryConsumerGroupConfig::new(consumers..=consumers).with_prefetch_count(10),
            move || handler.clone(),
            (),
        )
        .await
        .expect("register consumer group");
    registry
}

async fn publish_n(publisher: &Publisher<InMemory>, payload: &str, count: u64) {
    for id in 0..count {
        let msg = BenchMsg {
            id,
            payload: payload.to_owned(),
        };
        publisher
            .publish::<OverheadTopic>(&msg)
            .await
            .expect("publish");
    }
}

// ── Resource probe (not a criterion measurement) ─────────────────────────────

fn current_cpu_secs() -> f64 {
    #[cfg(target_os = "macos")]
    {
        use mach2::task::task_info;
        use mach2::task_info::{MACH_TASK_BASIC_INFO, mach_task_basic_info, task_flavor_t};
        let mut info: mach_task_basic_info = unsafe { std::mem::zeroed() };
        let mut count = (size_of::<mach_task_basic_info>() / size_of::<u32>()) as u32;
        let kr = unsafe {
            task_info(
                mach_task_self(),
                MACH_TASK_BASIC_INFO as task_flavor_t,
                &mut info as *mut _ as *mut i32,
                &mut count,
            )
        };
        if kr == 0 {
            let user =
                info.user_time.seconds as f64 + info.user_time.microseconds as f64 / 1_000_000.0;
            let system = info.system_time.seconds as f64
                + info.system_time.microseconds as f64 / 1_000_000.0;
            user + system
        } else {
            0.0
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/self/stat") {
            let fields: Vec<&str> = content.split_whitespace().collect();
            if fields.len() > 14 {
                let ticks_per_sec = 100.0;
                let utime = fields[13].parse::<f64>().unwrap_or(0.0);
                let stime = fields[14].parse::<f64>().unwrap_or(0.0);
                return (utime + stime) / ticks_per_sec;
            }
        }
        0.0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0.0
    }
}

fn current_rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        use mach2::task::task_info;
        use mach2::task_info::{MACH_TASK_BASIC_INFO, mach_task_basic_info, task_flavor_t};
        let mut info: mach_task_basic_info = unsafe { std::mem::zeroed() };
        let mut count = (size_of::<mach_task_basic_info>() / size_of::<u32>()) as u32;
        let kr = unsafe {
            task_info(
                mach_task_self(),
                MACH_TASK_BASIC_INFO as task_flavor_t,
                &mut info as *mut _ as *mut i32,
                &mut count,
            )
        };
        if kr == 0 { info.resident_size } else { 0 }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/self/statm")
            && let Some(rss_pages) = content.split_whitespace().nth(1)
            && let Ok(pages) = rss_pages.parse::<u64>()
        {
            return pages.saturating_mul(4096);
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

/// Sample RSS-per-consumer and idle CPU, once per consumer count, and print a
/// table to stderr.
///
/// Process RSS does not shrink after consumers are torn down, so only the
/// first allocation at each count is real — averaging across repeated rounds
/// would be meaningless, which is why this is a single-shot probe and not a
/// criterion group.
fn resource_probe(rt: &Runtime) {
    eprintln!();
    eprintln!("consumer resource probe (not a criterion measurement)");
    eprintln!(
        "{:>10} {:>14} {:>12}",
        "CONSUMERS", "RSS KB/CONSUMER", "IDLE CPU"
    );
    eprintln!("{}", "-".repeat(40));

    for consumers in CONSUMER_COUNTS {
        let (rss_per_consumer_kb, idle_cpu_pct) = rt.block_on(async move {
            let (client, _broker) = fresh_broker().await;
            let handler = BenchHandler::new(u64::MAX);

            let rss_before = current_rss_bytes();
            let mut registry = registry(&client, consumers, handler).await;
            registry.start_all();

            let cpu_before = current_cpu_secs();
            let wall_start = Instant::now();
            tokio::time::sleep(IDLE_SAMPLE).await;
            let wall = wall_start.elapsed().as_secs_f64();
            let cpu_delta = current_cpu_secs() - cpu_before;

            let rss_delta_kb = current_rss_bytes().saturating_sub(rss_before) as f64 / 1024.0;
            registry.shutdown_all().await;

            let idle_cpu_pct = if wall > 0.0 {
                cpu_delta / wall * 100.0
            } else {
                0.0
            };
            (rss_delta_kb / f64::from(consumers), idle_cpu_pct)
        });

        eprintln!("{consumers:>10} {rss_per_consumer_kb:>14.1} {idle_cpu_pct:>11.1}%");
    }
    eprintln!();
}

// ── Startup ─────────────────────────────────────────────────────────────────

/// Registering and starting `n` consumers.
///
/// On the in-process backend this is registration plus task spawn — there is
/// no broker to acknowledge the consumers. Broker construction and topology
/// declaration happen outside the timer.
fn bench_startup(c: &mut Criterion) {
    let rt = runtime();
    resource_probe(&rt);

    let mut group = c.benchmark_group("consumer_overhead_startup");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    for consumers in CONSUMER_COUNTS {
        group.throughput(Throughput::Elements(u64::from(consumers)));
        group.bench_with_input(
            BenchmarkId::from_parameter(consumers),
            &consumers,
            |b, &consumers| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let (client, _broker) = fresh_broker().await;
                        let handler = BenchHandler::new(u64::MAX);

                        let start = Instant::now();
                        let mut reg = registry(&client, consumers, handler).await;
                        reg.start_all();
                        elapsed = elapsed.saturating_add(start.elapsed());

                        reg.shutdown_all().await;
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

// ── Shutdown ────────────────────────────────────────────────────────────────

/// `shutdown_all()` on a running group of `n` consumers. Building and starting
/// the registry happens outside the timer.
fn bench_shutdown(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("consumer_overhead_shutdown");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    for consumers in CONSUMER_COUNTS {
        group.throughput(Throughput::Elements(u64::from(consumers)));
        group.bench_with_input(
            BenchmarkId::from_parameter(consumers),
            &consumers,
            |b, &consumers| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let (client, _broker) = fresh_broker().await;
                        let handler = BenchHandler::new(u64::MAX);
                        let mut reg = registry(&client, consumers, handler).await;
                        reg.start_all();

                        let start = Instant::now();
                        reg.shutdown_all().await;
                        elapsed = elapsed.saturating_add(start.elapsed());
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

// ── Dispatch ────────────────────────────────────────────────────────────────

/// Publish-to-handler dispatch through a pre-started group of `n` consumers.
///
/// Consumers are up and the messages are already queued before the timer
/// starts, so this is dispatch cost with no startup and no backlog build-up.
/// Parameterized over consumer count *and* payload size, because the two
/// interact: more consumers only help until per-message serde dominates.
fn bench_dispatch(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("consumer_overhead_dispatch");
    group.throughput(Throughput::Elements(DISPATCH_MESSAGES));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for consumers in CONSUMER_COUNTS {
        for bytes in PAYLOAD_SIZES {
            let body = payload(bytes);
            group.bench_with_input(
                BenchmarkId::new(format!("consumers_{consumers}"), bytes),
                &body,
                |b, body| {
                    b.to_async(&rt).iter_custom(|iters| async move {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iters {
                            let (client, broker) = fresh_broker().await;
                            let publisher = broker.publisher().await.expect("publisher");
                            publish_n(&publisher, body, DISPATCH_MESSAGES).await;

                            let handler = BenchHandler::new(DISPATCH_MESSAGES);
                            let signal = handler.clone();
                            let mut reg = registry(&client, consumers, handler).await;

                            let start = Instant::now();
                            reg.start_all();
                            signal.completed().await;
                            elapsed = elapsed.saturating_add(start.elapsed());

                            reg.shutdown_all().await;
                        }
                        elapsed
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_startup, bench_shutdown, bench_dispatch);
criterion_main!(benches);
