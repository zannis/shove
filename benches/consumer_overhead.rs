//! Consumer-count overhead benchmark for shove.
//!
//! Measures what shove itself costs as a consumer group scales out, with the
//! broker removed: the in-process backend is the substrate, so nothing here
//! reports a broker's queueing behaviour.
//!
//! Metrics:
//!   - **Startup** — registering `n` consumers and starting them.
//!   - **Shutdown** — `shutdown_all()` wall-clock duration.
//!   - **Dispatch** — live publish-to-handler latency: the consumers are
//!     started *and warmed* before the timer, then a fixed message count is
//!     published inside it. No startup and no pre-built backlog in the number.
//!   - **RSS per consumer** — marginal memory cost of each added consumer,
//!     measured on a cumulative sweep.
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

/// Payload used by the dispatch warm-up burst. The warm-up exists to prove
/// the group is live, so it carries the cheapest payload in the set rather
/// than the round's own — its cost is pure overhead outside the timer.
const WARMUP_PAYLOAD_BYTES: usize = 64;

/// Warm-up deliveries before a dispatch round is timed: one per consumer, so
/// the burst scales with the group rather than being a fixed number that is
/// generous at 1 consumer and thin at 256.
fn warmup_messages(consumers: u16) -> u64 {
    u64::from(consumers).max(1)
}

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

/// Counts deliveries and signals at two milestones: `warm_target` (the group
/// is live and draining) and `target` (the round is finished). Does no work of
/// its own, so the groups below report framework cost only.
///
/// Two separate gates rather than one re-armable counter, because each
/// milestone is an exact `==` on a value produced by a unique atomic
/// increment: each fires once, so a stored `notify_one` permit can never be
/// left behind to satisfy the *other* wait early.
#[derive(Clone)]
struct BenchHandler {
    seen: Arc<AtomicU64>,
    warm_target: u64,
    target: u64,
    warm: Arc<Notify>,
    done: Arc<Notify>,
}

impl BenchHandler {
    /// A handler with no warm-up milestone. `0` is unreachable — the first
    /// delivery is already `1` — so [`BenchHandler::warmed`] must not be
    /// called on one of these.
    fn new(target: u64) -> Self {
        Self::with_warmup(0, target)
    }

    fn with_warmup(warm_target: u64, target: u64) -> Self {
        Self {
            seen: Arc::new(AtomicU64::new(0)),
            warm_target,
            target,
            warm: Arc::new(Notify::new()),
            done: Arc::new(Notify::new()),
        }
    }

    /// Resolves once the warm-up burst has been fully handled.
    async fn warmed(&self) {
        self.reached(&self.warm, self.warm_target, "warm-up").await;
    }

    /// Resolves once `target` messages have been handled.
    async fn completed(&self) {
        self.reached(&self.done, self.target, "dispatch").await;
    }

    /// Waits on one milestone gate. `notify_one` stores a permit when no
    /// waiter is parked, so a milestone reached before the wait is entered
    /// cannot be missed.
    ///
    /// Bounded so a lost delivery fails the run instead of hanging it: an
    /// unbounded wait here is `cargo bench` producing no further output until
    /// CI kills the job, which reads as "slow" rather than "broken".
    async fn reached(&self, gate: &Notify, target: u64, phase: &str) {
        if timeout(COMPLETION_TIMEOUT, gate.notified()).await.is_err() {
            panic!(
                "{phase}: handler saw {} of {target} messages in {COMPLETION_TIMEOUT:?}: \
                 a delivery was lost",
                self.seen.load(Ordering::Relaxed),
            );
        }
    }
}

impl MessageHandler<OverheadTopic> for BenchHandler {
    type Context = ();

    async fn handle(&self, _msg: BenchMsg, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        let seen = self.seen.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        if seen == self.warm_target {
            self.warm.notify_one();
        }
        if seen == self.target {
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

/// Sample marginal RSS-per-consumer and idle CPU across the consumer-count
/// sweep, and print a table to stderr.
///
/// **The sweep is cumulative.** Each row registers only the consumers the
/// previous row did not have, keeps every earlier consumer alive and running,
/// and divides the RSS growth by the number *added*. Tearing each round down
/// before the next — the obvious shape — makes every row after the first
/// understate: process RSS does not shrink on teardown, so the freed pages
/// stay resident and the allocator serves the next, larger round out of them.
/// The delta shrinks while the true cost does not. Nothing is freed between
/// rows here, so each delta is memory the added consumers actually caused.
///
/// Two limits survive and are why this stays a single-shot stderr table rather
/// than a criterion measurement:
///
/// - The first row also carries the process's one-time consumer machinery, so
///   it is an upper bound, not a per-consumer cost.
/// - Any row can still be served partly out of arena slack the allocator had
///   already mapped, which no in-process probe can subtract.
///
/// `CONSUMERS` is the live total after the row's additions; `RSS KB/ADDED`
/// divides that row's growth by its own additions only.
fn resource_probe(rt: &Runtime) {
    eprintln!();
    eprintln!("consumer resource probe (not a criterion measurement)");
    eprintln!("cumulative sweep: consumers accumulate across rows, so no row is");
    eprintln!("measured against pages an earlier row freed.");
    eprintln!(
        "{:>10} {:>8} {:>14} {:>12}",
        "CONSUMERS", "ADDED", "RSS KB/ADDED", "IDLE CPU"
    );
    eprintln!("{}", "-".repeat(48));

    rt.block_on(async {
        let (client, _broker) = fresh_broker().await;
        // One registry per row, all on the same client, rather than one
        // registry that grows: a registry rejects a second group for a queue
        // it already carries. Every one stays alive until the sweep ends —
        // dropping or shutting one down mid-sweep is exactly the reuse this
        // shape exists to avoid.
        let mut live: Vec<InMemoryConsumerGroupRegistry> = Vec::new();
        let mut previous: u16 = 0;

        for consumers in CONSUMER_COUNTS {
            let added = consumers.checked_sub(previous).filter(|added| *added > 0);
            let Some(added) = added else {
                panic!(
                    "CONSUMER_COUNTS must be strictly ascending for a cumulative sweep, \
                     got {consumers} after {previous}"
                );
            };
            previous = consumers;

            let rss_before = current_rss_bytes();
            let mut registry = registry(&client, added, BenchHandler::new(u64::MAX)).await;
            registry.start_all();

            let cpu_before = current_cpu_secs();
            let wall_start = Instant::now();
            tokio::time::sleep(IDLE_SAMPLE).await;
            let wall = wall_start.elapsed().as_secs_f64();
            let cpu_delta = current_cpu_secs() - cpu_before;
            let rss_delta_kb = current_rss_bytes().saturating_sub(rss_before) as f64 / 1024.0;

            live.push(registry);

            let idle_cpu_pct = if wall > 0.0 {
                cpu_delta / wall * 100.0
            } else {
                0.0
            };
            // `added` is non-zero by the guard above.
            let rss_per_added_kb = rss_delta_kb / f64::from(added);

            eprintln!("{consumers:>10} {added:>8} {rss_per_added_kb:>14.1} {idle_cpu_pct:>11.1}%");
        }

        for registry in live.iter_mut() {
            registry.shutdown_all().await;
        }
    });
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

/// Live publish-to-handler latency through a pre-started group of `n`
/// consumers.
///
/// The timed region is a *live* publish of `DISPATCH_MESSAGES` and the wait
/// for the last of them to reach a handler. Everything else is outside it:
/// the group is started and then warmed with [`warmup_messages`] deliveries
/// that must all be handled before the timer starts. Two reasons the warm-up
/// is not optional:
///
/// - It keeps consumer-task startup out of this measurement. Timing
///   `start_all()` here would make a pure startup regression show up in both
///   this group and `consumer_overhead_startup`, and leave live publish
///   latency — the thing this group exists to isolate — with no signal of
///   its own.
/// - Publishing the whole round *before* starting the consumers would measure
///   the drain of a pre-built backlog, which is a different quantity: the
///   publisher never competes with the handlers for the runtime.
///
/// What the warm-up guarantees is that the group is live and draining before
/// the timer starts, not that every individual consumer task has polled —
/// which message lands on which consumer is the backend's to decide. It uses
/// the smallest payload regardless of the round's own size, because its job
/// is liveness, not allocation shape.
///
/// Parameterized over consumer count *and* payload size, because the two
/// interact: more consumers only help until per-message serde dominates.
fn bench_dispatch(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("consumer_overhead_dispatch");
    group.throughput(Throughput::Elements(DISPATCH_MESSAGES));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let warmup_body = payload(WARMUP_PAYLOAD_BYTES);

    for consumers in CONSUMER_COUNTS {
        let warmup = warmup_messages(consumers);
        for bytes in PAYLOAD_SIZES {
            let body = payload(bytes);
            let warmup_body = warmup_body.clone();
            group.bench_with_input(
                BenchmarkId::new(format!("consumers_{consumers}"), bytes),
                &body,
                |b, body| {
                    let warmup_body = warmup_body.clone();
                    b.to_async(&rt).iter_custom(|iters| {
                        let warmup_body = warmup_body.clone();
                        async move {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iters {
                                let (client, broker) = fresh_broker().await;
                                let publisher = broker.publisher().await.expect("publisher");

                                let handler = BenchHandler::with_warmup(
                                    warmup,
                                    warmup.saturating_add(DISPATCH_MESSAGES),
                                );
                                let signal = handler.clone();
                                let mut reg = registry(&client, consumers, handler).await;
                                reg.start_all();

                                // Outside the timer: the wait returns only once
                                // the group has drained the whole burst, so the
                                // consumers are demonstrably live below.
                                publish_n(&publisher, &warmup_body, warmup).await;
                                signal.warmed().await;

                                let start = Instant::now();
                                publish_n(&publisher, body, DISPATCH_MESSAGES).await;
                                signal.completed().await;
                                elapsed = elapsed.saturating_add(start.elapsed());

                                reg.shutdown_all().await;
                            }
                            elapsed
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_startup, bench_shutdown, bench_dispatch);
criterion_main!(benches);
