//! Burst-autoscaling benchmark for the Kafka backend.
//!
//! Simulates bursts of randomized, memory-heavy messages and observes how
//! shove's autoscaling consumer group reacts. Each message occupies a
//! randomized amount of memory (200 KB–5 MB) and burns a linearly proportional
//! amount of real CPU time (~`rate_mb_s` MB/s), so processing cost tracks
//! message size. The autoscaler scales consumers on Kafka consumer lag while a
//! sampler records the consumer-count / lag / RSS timeline.
//!
//! Kafka specifics this exercises:
//!   - Parallelism is capped by partition count. Registering the group with
//!     `--min..=--max` auto-provisions `max(8, --max)` partitions, so scale-ups
//!     have somewhere to land.
//!   - The scaling signal is consumer lag (committed-offset based), not an
//!     instantaneous ready-count.
//!   - Every scale event triggers a group rebalance — a brief consumption pause
//!     visible as latency spikes / lag bumps on the timeline.
//!
//! Messages are published with a null key, so librdkafka's default
//! `consistent_random` partitioner spreads them across partitions (a keyed
//! topic would be a `SequencedTopic`, which pins `max_consumers = 1` and
//! defeats autoscaling).
//!
//! Requires a running Docker daemon. Run with:
//!
//!     cargo run -q --release --example kafka_burst_autoscale --features kafka
//!     cargo run -q --release --example kafka_burst_autoscale --features kafka -- \
//!         --duration 60 --min 1 --max 16 --output json

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde::{Deserialize, Serialize};
use shove::kafka::{
    KafkaAutoscalerBackend, KafkaClient, KafkaConfig, KafkaConsumerGroupConfig,
    KafkaConsumerGroupRegistry, KafkaLagStatsProvider, KafkaPublisher, KafkaQueueStatsProvider,
};
use shove::{
    AutoscalerConfig, MessageHandler, MessageMetadata, Outcome, TopologyBuilder, define_topic,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka as KafkaImage};
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const KB: u64 = 1024;
const MB: u64 = 1024 * 1024;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "kafka_burst_autoscale",
    about = "Burst-autoscaling benchmark for the Kafka backend"
)]
struct Cli {
    /// Total run duration for the load generator, in seconds.
    #[arg(long, default_value = "45")]
    duration: u64,

    /// Minimum consumers in the autoscaling group.
    #[arg(long, default_value = "1")]
    min: u16,

    /// Maximum consumers in the autoscaling group (also sizes partitions).
    #[arg(long, default_value = "8")]
    max: u16,

    /// Prefetch (in-flight messages) per consumer.
    #[arg(long, default_value = "10")]
    prefetch: u16,

    /// Concurrent CPU-burn slots (finite "core budget"). Defaults to the host's
    /// available parallelism.
    #[arg(long)]
    cpu_slots: Option<usize>,

    /// Processing rate: MB of payload "processed" per second of CPU burn.
    #[arg(long, default_value = "10.0")]
    rate_mb_s: f64,

    /// Minimum messages per burst.
    #[arg(long, default_value = "50")]
    burst_min: u32,

    /// Maximum messages per burst.
    #[arg(long, default_value = "500")]
    burst_max: u32,

    /// Minimum per-message payload size, in KB.
    #[arg(long, default_value = "200")]
    size_min_kb: u32,

    /// Maximum per-message payload size, in KB.
    #[arg(long, default_value = "5120")]
    size_max_kb: u32,

    /// Minimum idle gap between bursts, in milliseconds.
    #[arg(long, default_value = "500")]
    gap_min_ms: u64,

    /// Maximum idle gap between bursts, in milliseconds.
    #[arg(long, default_value = "4000")]
    gap_max_ms: u64,

    /// RNG seed for reproducible burst plans.
    #[arg(long, default_value = "42")]
    seed: u64,

    /// Telemetry sample interval, in milliseconds.
    #[arg(long, default_value = "250")]
    sample_ms: u64,

    /// Autoscaler poll interval, in milliseconds.
    #[arg(long, default_value = "1000")]
    poll_ms: u64,

    /// Autoscaler hysteresis duration, in milliseconds.
    #[arg(long, default_value = "2000")]
    hysteresis_ms: u64,

    /// Autoscaler cooldown duration, in milliseconds.
    #[arg(long, default_value = "3000")]
    cooldown_ms: u64,

    /// Output format for the final report.
    #[arg(long, default_value = "table")]
    output: OutputFormat,

    /// Hidden flag accepted (and ignored) so `cargo bench`-style invocations work.
    #[arg(long, hide = true)]
    bench: bool,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

// ── Message & topic ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LoadMsg {
    id: u64,
    size_bytes: u32,
    published_at_ns: u64,
}

define_topic!(
    BurstTopic,
    LoadMsg,
    TopologyBuilder::new("kafka-burst-autoscale").dlq().build()
);

// ── Size buckets ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SizeBucket {
    /// < 1 MB
    Small,
    /// 1 MB – 3 MB
    Medium,
    /// >= 3 MB
    Large,
}

impl SizeBucket {
    fn label(self) -> &'static str {
        match self {
            SizeBucket::Small => "<1MB",
            SizeBucket::Medium => "1-3MB",
            SizeBucket::Large => ">=3MB",
        }
    }

    const ALL: [SizeBucket; 3] = [SizeBucket::Small, SizeBucket::Medium, SizeBucket::Large];
}

fn bucket_for(size_bytes: u32) -> SizeBucket {
    let b = size_bytes as u64;
    if b < MB {
        SizeBucket::Small
    } else if b < 3 * MB {
        SizeBucket::Medium
    } else {
        SizeBucket::Large
    }
}

/// CPU-burn target duration for a message of `size_bytes` at `rate_mb_s` MB/s.
fn burn_target(size_bytes: u32, rate_mb_s: f64) -> Duration {
    debug_assert!(rate_mb_s > 0.0);
    let secs = (size_bytes as f64 / MB as f64) / rate_mb_s;
    Duration::from_secs_f64(secs)
}

// ── Burst planning (seeded, deterministic) ───────────────────────────────────────

#[derive(Debug, Clone)]
struct PlannedBurst {
    /// Per-message payload sizes (bytes) for this burst.
    sizes: Vec<u32>,
    /// Idle gap to wait after publishing this burst.
    gap: Duration,
}

struct PlanParams {
    seed: u64,
    duration: Duration,
    burst_min: u32,
    burst_max: u32,
    size_min_bytes: u32,
    size_max_bytes: u32,
    gap_min_ms: u64,
    gap_max_ms: u64,
}

/// Build a deterministic burst plan from a seed. Bursts accumulate until the
/// total inter-burst gap reaches `duration`, so the plan covers roughly the
/// requested wall-clock window.
fn plan_bursts(p: &PlanParams) -> Vec<PlannedBurst> {
    let mut rng = StdRng::seed_from_u64(p.seed);
    let mut plan = Vec::new();
    let mut elapsed = Duration::ZERO;

    while elapsed < p.duration {
        let count = rng.random_range(p.burst_min..=p.burst_max);
        let sizes: Vec<u32> = (0..count)
            .map(|_| rng.random_range(p.size_min_bytes..=p.size_max_bytes))
            .collect();
        let gap_ms = rng.random_range(p.gap_min_ms..=p.gap_max_ms);
        let gap = Duration::from_millis(gap_ms);
        elapsed += gap;
        plan.push(PlannedBurst { sizes, gap });
    }
    plan
}

// ── Latency recording ────────────────────────────────────────────────────────────

/// One end-to-end latency sample (publish → handler completion), in ns,
/// tagged with the message's size bucket.
#[derive(Debug, Clone, Copy)]
struct LatencySample {
    bucket: SizeBucket,
    e2e_ns: u64,
}

struct LatencyRecorder {
    tx: tokio::sync::mpsc::UnboundedSender<LatencySample>,
    rx: Mutex<tokio::sync::mpsc::UnboundedReceiver<LatencySample>>,
}

impl LatencyRecorder {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(rx),
        }
    }

    fn record(&self, sample: LatencySample) {
        let _ = self.tx.send(sample);
    }

    async fn drain(&self) -> Vec<LatencySample> {
        let mut rx = self.rx.lock().await;
        let mut out = Vec::new();
        while let Ok(s) = rx.try_recv() {
            out.push(s);
        }
        out
    }
}

/// p50/p95/p99/max in milliseconds for a set of nanosecond latencies.
#[derive(Debug, Clone, Copy, Default, Serialize)]
struct Percentiles {
    count: usize,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

/// Compute percentiles from nanosecond samples. `samples` is sorted in place.
fn percentiles(samples: &mut [u64]) -> Percentiles {
    if samples.is_empty() {
        return Percentiles::default();
    }
    samples.sort_unstable();
    let len = samples.len();
    let at = |pct: usize| samples[(len * pct / 100).min(len - 1)] as f64 / 1_000_000.0;
    Percentiles {
        count: len,
        p50_ms: at(50),
        p95_ms: at(95),
        p99_ms: at(99),
        max_ms: samples[len - 1] as f64 / 1_000_000.0,
    }
}

// ── Resource sampling ──────────────────────────────────────────────────────────

fn current_rss_bytes() -> u64 {
    #[cfg(target_os = "macos")]
    {
        use mach2::task::task_info;
        use mach2::task_info::{MACH_TASK_BASIC_INFO, mach_task_basic_info, task_flavor_t};
        use mach2::traps::mach_task_self;
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
            return pages * 4096;
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

/// One timeline sample.
#[derive(Debug, Clone, Copy, Serialize)]
struct TimelineSample {
    t_ms: u64,
    active_consumers: u16,
    lag: u64,
    rss_mb: f64,
}

// ── Handler ──────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct BurnHandler {
    epoch: Instant,
    processed: Arc<AtomicU64>,
    recorder: Arc<LatencyRecorder>,
    cpu: Arc<Semaphore>,
    rate_mb_s: f64,
}

impl MessageHandler<BurstTopic> for BurnHandler {
    type Context = ();

    async fn handle(&self, msg: LoadMsg, _meta: MessageMetadata, _: &()) -> Outcome {
        let size_bytes = msg.size_bytes;
        let rate = self.rate_mb_s;

        // Hold a CPU slot for the duration of the burn, modeling a finite core
        // budget regardless of how many consumers are running.
        let _permit = self.cpu.acquire().await.expect("semaphore closed");

        // Real CPU work on the blocking pool: allocate the payload, fault its
        // pages in (so RSS is genuine), then burn CPU proportional to size.
        tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; size_bytes as usize];
            // Touch one byte per 4 KB page to force resident memory.
            let mut i = 0;
            while i < buf.len() {
                buf[i] = (i & 0xff) as u8;
                i += 4096;
            }

            let target = burn_target(size_bytes, rate);
            let start = Instant::now();
            let mut acc: u64 = 0;
            // Burn until the proportional time has elapsed, doing real work over
            // the buffer so the optimizer can't elide it.
            loop {
                for chunk in buf.chunks(64) {
                    for &b in chunk {
                        acc = acc.wrapping_add(b as u64).rotate_left(1);
                    }
                }
                if start.elapsed() >= target {
                    break;
                }
            }
            std::hint::black_box(acc);
        })
        .await
        .expect("cpu burn task panicked");

        let acked_at = self.epoch.elapsed().as_nanos() as u64;
        self.recorder.record(LatencySample {
            bucket: bucket_for(size_bytes),
            e2e_ns: acked_at.saturating_sub(msg.published_at_ns),
        });
        self.processed.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

// ── Reporting ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct BucketReport {
    bucket: String,
    latency: Percentiles,
}

#[derive(Debug, Clone, Serialize)]
struct Report {
    // Configuration echo.
    duration_secs: u64,
    min_consumers: u16,
    max_consumers: u16,
    prefetch: u16,
    cpu_slots: usize,
    rate_mb_s: f64,
    seed: u64,
    // Workload totals.
    bursts: usize,
    total_messages: u64,
    total_mb: f64,
    processed: u64,
    wall_secs: f64,
    drain_secs: f64,
    // Throughput.
    throughput_msg_per_sec: f64,
    throughput_mb_per_sec: f64,
    // Latency.
    overall_latency: Percentiles,
    per_bucket: Vec<BucketReport>,
    // Autoscaling + resource peaks.
    peak_consumers: u16,
    peak_lag: u64,
    peak_rss_mb: f64,
    // Full timeline (for plotting).
    timeline: Vec<TimelineSample>,
}

fn print_table(r: &Report) {
    println!();
    println!("Kafka burst-autoscaling benchmark");
    println!(
        "  run: {}s | consumers {}..={} | prefetch {} | cpu_slots {} | rate {:.0} MB/s | seed {}",
        r.duration_secs,
        r.min_consumers,
        r.max_consumers,
        r.prefetch,
        r.cpu_slots,
        r.rate_mb_s,
        r.seed,
    );
    println!(
        "  workload: {} bursts | {} msgs | {:.1} MB | processed {} in {:.1}s (drain {:.1}s)",
        r.bursts, r.total_messages, r.total_mb, r.processed, r.wall_secs, r.drain_secs,
    );
    println!(
        "  throughput: {:.1} msg/s | {:.1} MB/s",
        r.throughput_msg_per_sec, r.throughput_mb_per_sec,
    );
    println!(
        "  peaks: {} consumers | {} lag | {:.1} MB RSS",
        r.peak_consumers, r.peak_lag, r.peak_rss_mb,
    );

    println!();
    println!(
        "{:<8} {:>8} {:>10} {:>10} {:>10} {:>10}",
        "BUCKET", "COUNT", "p50(ms)", "p95(ms)", "p99(ms)", "max(ms)"
    );
    println!("{}", "-".repeat(60));
    let p = &r.overall_latency;
    println!(
        "{:<8} {:>8} {:>10.1} {:>10.1} {:>10.1} {:>10.1}",
        "ALL", p.count, p.p50_ms, p.p95_ms, p.p99_ms, p.max_ms
    );
    for b in &r.per_bucket {
        let p = &b.latency;
        println!(
            "{:<8} {:>8} {:>10.1} {:>10.1} {:>10.1} {:>10.1}",
            b.bucket, p.count, p.p50_ms, p.p95_ms, p.p99_ms, p.max_ms
        );
    }

    print_timeline(r);

    println!();
    println!("e2e latency = publish → handler completion (queue wait + alloc + CPU burn).");
    println!("lag         = Kafka consumer lag (committed-offset based); reacts after commits.");
    println!("Watch for lag bumps / latency spikes aligned with consumer-count changes —");
    println!("those are group rebalances triggered by each scale event.");
}

/// Print a downsampled ASCII timeline of consumers / lag / RSS.
fn print_timeline(r: &Report) {
    if r.timeline.is_empty() {
        return;
    }
    println!();
    println!(
        "Timeline (consumers / lag / RSS MB), ~{} samples:",
        r.timeline.len()
    );
    println!(
        "{:>8} {:>6} {:>10} {:>10}",
        "t(s)", "cons", "lag", "rss(MB)"
    );
    println!("{}", "-".repeat(38));

    // Downsample to at most ~40 rows so the table stays readable.
    let max_rows = 40usize;
    let step = r.timeline.len().div_ceil(max_rows).max(1);
    for sample in r.timeline.iter().step_by(step) {
        println!(
            "{:>8.1} {:>6} {:>10} {:>10.1}",
            sample.t_ms as f64 / 1000.0,
            sample.active_consumers,
            sample.lag,
            sample.rss_mb,
        );
    }
}

// ── Telemetry sampler ──────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn spawn_sampler(
    epoch: Instant,
    sample_interval: Duration,
    registry: Arc<Mutex<KafkaConsumerGroupRegistry>>,
    lag_provider: Arc<KafkaLagStatsProvider>,
    queue: String,
    group_id: String,
    timeline: Arc<Mutex<Vec<TimelineSample>>>,
    peak_rss: Arc<AtomicU64>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(sample_interval) => {}
            }

            let active_consumers = {
                let reg = registry.lock().await;
                reg.groups()
                    .values()
                    .map(|g| g.active_consumers())
                    .sum::<usize>() as u16
            };

            let lag = lag_provider
                .get_queue_stats(&queue, &group_id)
                .await
                .map(|s| s.messages_pending)
                .unwrap_or(0);

            let rss = current_rss_bytes();
            peak_rss.fetch_max(rss, Ordering::Relaxed);

            timeline.lock().await.push(TimelineSample {
                t_ms: epoch.elapsed().as_millis() as u64,
                active_consumers,
                lag,
                rss_mb: rss as f64 / MB as f64,
            });
        }
    })
}

// ── Infrastructure ────────────────────────────────────────────────────────────────

fn require_docker() {
    match std::process::Command::new("docker").arg("info").output() {
        Ok(o) if o.status.success() => {}
        _ => panic!(
            "Docker is required to run this benchmark. \
             Install Docker Desktop, colima, or podman and ensure the daemon is running."
        ),
    }
}

// ── Main ────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".parse().unwrap()),
        )
        .try_init();

    require_docker();

    let cpu_slots = cli
        .cpu_slots
        .or_else(|| std::thread::available_parallelism().ok().map(|n| n.get()))
        .unwrap_or(4);

    // ── Start Kafka ──────────────────────────────────────────────────────────
    eprintln!("starting Kafka container...");
    let container = KafkaImage::default()
        .start()
        .await
        .expect("failed to start Kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("failed to read Kafka port");
    let bootstrap = format!("127.0.0.1:{port}");
    let client = KafkaClient::connect(&KafkaConfig::new(&bootstrap))
        .await
        .expect("failed to connect Kafka client");
    eprintln!("Kafka ready at {bootstrap}");

    // ── Register the autoscaling consumer group ──────────────────────────────
    let epoch = Instant::now();
    let processed = Arc::new(AtomicU64::new(0));
    let recorder = Arc::new(LatencyRecorder::new());
    let cpu = Arc::new(Semaphore::new(cpu_slots));

    let mut registry = KafkaConsumerGroupRegistry::new(client.clone());
    {
        let pc = processed.clone();
        let rec = recorder.clone();
        let cpu = cpu.clone();
        let rate = cli.rate_mb_s;
        registry
            .register::<BurstTopic, BurnHandler>(
                KafkaConsumerGroupConfig::new(cli.min..=cli.max)
                    .with_prefetch_count(cli.prefetch)
                    .with_concurrent_processing(true),
                move || BurnHandler {
                    epoch,
                    processed: pc.clone(),
                    recorder: rec.clone(),
                    cpu: cpu.clone(),
                    rate_mb_s: rate,
                },
                (),
            )
            .await
            .expect("failed to register consumer group");
    }

    // Capture queue + group id for the lag sampler before moving the registry.
    let (queue, group_id) = {
        let g = registry
            .groups()
            .values()
            .next()
            .expect("one group registered");
        (g.queue().to_string(), g.group_id().to_string())
    };

    registry.start_all();
    let registry = Arc::new(Mutex::new(registry));

    // ── Autoscaler ────────────────────────────────────────────────────────────
    let autoscaler_cfg = AutoscalerConfig {
        poll_interval: Duration::from_millis(cli.poll_ms),
        hysteresis_duration: Duration::from_millis(cli.hysteresis_ms),
        cooldown_duration: Duration::from_millis(cli.cooldown_ms),
        ..Default::default()
    };
    let mut autoscaler =
        KafkaAutoscalerBackend::autoscaler(client.clone(), registry.clone(), autoscaler_cfg);
    let shutdown = CancellationToken::new();
    let autoscaler_handle = {
        let s = shutdown.clone();
        tokio::spawn(async move { autoscaler.run(s).await })
    };

    // ── Telemetry sampler ──────────────────────────────────────────────────────
    let timeline = Arc::new(Mutex::new(Vec::<TimelineSample>::new()));
    let peak_rss = Arc::new(AtomicU64::new(current_rss_bytes()));
    let lag_provider = Arc::new(KafkaLagStatsProvider::new(client.clone()));
    let sampler_handle = spawn_sampler(
        epoch,
        Duration::from_millis(cli.sample_ms),
        registry.clone(),
        lag_provider,
        queue,
        group_id,
        timeline.clone(),
        peak_rss.clone(),
        shutdown.clone(),
    );

    // ── Load generator ──────────────────────────────────────────────────────────
    let plan = plan_bursts(&PlanParams {
        seed: cli.seed,
        duration: Duration::from_secs(cli.duration),
        burst_min: cli.burst_min,
        burst_max: cli.burst_max,
        size_min_bytes: cli.size_min_kb * KB as u32,
        size_max_bytes: cli.size_max_kb * KB as u32,
        gap_min_ms: cli.gap_min_ms,
        gap_max_ms: cli.gap_max_ms,
    });
    let total_messages: u64 = plan.iter().map(|b| b.sizes.len() as u64).sum();
    let total_bytes: u64 = plan
        .iter()
        .flat_map(|b| b.sizes.iter())
        .map(|&s| s as u64)
        .sum();
    eprintln!(
        "load plan: {} bursts | {} msgs | {:.1} MB | ~{}s",
        plan.len(),
        total_messages,
        total_bytes as f64 / MB as f64,
        cli.duration,
    );

    let publisher = KafkaPublisher::new(client.clone())
        .await
        .expect("failed to build publisher");

    let load_start = Instant::now();
    let mut next_id: u64 = 0;
    for (i, burst) in plan.iter().enumerate() {
        let msgs: Vec<LoadMsg> = burst
            .sizes
            .iter()
            .map(|&size_bytes| {
                let m = LoadMsg {
                    id: next_id,
                    size_bytes,
                    published_at_ns: epoch.elapsed().as_nanos() as u64,
                };
                next_id += 1;
                m
            })
            .collect();

        let (succeeded, res) = publisher.publish_batch::<BurstTopic>(&msgs).await;
        if let Err(e) = res {
            eprintln!("  burst {i}: publish_batch error after {succeeded}: {e}");
        }
        eprintln!(
            "  [{}/{}] published {} msgs (gap {:?})",
            i + 1,
            plan.len(),
            msgs.len(),
            burst.gap,
        );
        tokio::time::sleep(burst.gap).await;
    }
    let publish_done = Instant::now();

    // ── Drain ────────────────────────────────────────────────────────────────────
    eprintln!("load complete; draining {total_messages} messages...");
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        if processed.load(Ordering::Relaxed) >= total_messages {
            break;
        }
        if tokio::time::Instant::now() >= drain_deadline {
            eprintln!(
                "drain timeout: processed {} / {total_messages}",
                processed.load(Ordering::Relaxed),
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let drain_secs = publish_done.elapsed().as_secs_f64();
    let wall_secs = load_start.elapsed().as_secs_f64();

    // ── Teardown ────────────────────────────────────────────────────────────────
    shutdown.cancel();
    let _ = autoscaler_handle.await;
    let _ = sampler_handle.await;
    registry.lock().await.shutdown_all().await;
    client.shutdown().await;

    // ── Build report ──────────────────────────────────────────────────────────────
    let samples = recorder.drain().await;
    let mut overall: Vec<u64> = samples.iter().map(|s| s.e2e_ns).collect();
    let overall_latency = percentiles(&mut overall);

    let mut per_bucket = Vec::new();
    for bucket in SizeBucket::ALL {
        let mut ns: Vec<u64> = samples
            .iter()
            .filter(|s| s.bucket == bucket)
            .map(|s| s.e2e_ns)
            .collect();
        per_bucket.push(BucketReport {
            bucket: bucket.label().to_string(),
            latency: percentiles(&mut ns),
        });
    }

    let timeline = timeline.lock().await.clone();
    let peak_consumers = timeline
        .iter()
        .map(|s| s.active_consumers)
        .max()
        .unwrap_or(cli.min);
    let peak_lag = timeline.iter().map(|s| s.lag).max().unwrap_or(0);
    let peak_rss_mb = peak_rss.load(Ordering::Relaxed) as f64 / MB as f64;

    let processed_n = processed.load(Ordering::Relaxed);
    let report = Report {
        duration_secs: cli.duration,
        min_consumers: cli.min,
        max_consumers: cli.max,
        prefetch: cli.prefetch,
        cpu_slots,
        rate_mb_s: cli.rate_mb_s,
        seed: cli.seed,
        bursts: plan.len(),
        total_messages,
        total_mb: total_bytes as f64 / MB as f64,
        processed: processed_n,
        wall_secs,
        drain_secs,
        throughput_msg_per_sec: processed_n as f64 / wall_secs.max(f64::MIN_POSITIVE),
        throughput_mb_per_sec: (total_bytes as f64 / MB as f64) / wall_secs.max(f64::MIN_POSITIVE),
        overall_latency,
        per_bucket,
        peak_consumers,
        peak_lag,
        peak_rss_mb,
        timeline,
    };

    match cli.output {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
        }
        OutputFormat::Table => print_table(&report),
    }

    drop(container);
}

// ── Tests ────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_boundaries() {
        assert_eq!(bucket_for(200 * KB as u32), SizeBucket::Small);
        assert_eq!(bucket_for((MB - 1) as u32), SizeBucket::Small);
        assert_eq!(bucket_for(MB as u32), SizeBucket::Medium);
        assert_eq!(bucket_for((3 * MB - 1) as u32), SizeBucket::Medium);
        assert_eq!(bucket_for((3 * MB) as u32), SizeBucket::Large);
        assert_eq!(bucket_for((5 * MB) as u32), SizeBucket::Large);
    }

    #[test]
    fn burn_target_scales_linearly_with_size() {
        // At 10 MB/s: 1 MB → 100 ms, 5 MB → 500 ms.
        let one_mb = burn_target(MB as u32, 10.0);
        let five_mb = burn_target(5 * MB as u32, 10.0);
        assert!((one_mb.as_secs_f64() - 0.1).abs() < 1e-6);
        assert!((five_mb.as_secs_f64() - 0.5).abs() < 1e-6);
        // Linear: 5× the size → 5× the time.
        assert!((five_mb.as_secs_f64() / one_mb.as_secs_f64() - 5.0).abs() < 1e-6);
    }

    #[test]
    fn burn_target_respects_rate() {
        // Doubling the rate halves the time.
        let slow = burn_target(MB as u32, 10.0);
        let fast = burn_target(MB as u32, 20.0);
        assert!((slow.as_secs_f64() / fast.as_secs_f64() - 2.0).abs() < 1e-6);
    }

    fn test_params(seed: u64) -> PlanParams {
        PlanParams {
            seed,
            duration: Duration::from_secs(10),
            burst_min: 50,
            burst_max: 500,
            size_min_bytes: 200 * KB as u32,
            size_max_bytes: 5 * MB as u32,
            gap_min_ms: 500,
            gap_max_ms: 4000,
        }
    }

    #[test]
    fn plan_is_deterministic_for_a_seed() {
        let a = plan_bursts(&test_params(7));
        let b = plan_bursts(&test_params(7));
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.sizes, y.sizes);
            assert_eq!(x.gap, y.gap);
        }
    }

    #[test]
    fn plan_differs_across_seeds() {
        let a = plan_bursts(&test_params(1));
        let b = plan_bursts(&test_params(2));
        // Overwhelmingly likely to differ in either burst count or first sizes.
        let same = a.len() == b.len()
            && a.iter()
                .zip(b.iter())
                .all(|(x, y)| x.sizes == y.sizes && x.gap == y.gap);
        assert!(!same, "different seeds should produce different plans");
    }

    #[test]
    fn plan_respects_size_and_burst_bounds() {
        let p = test_params(99);
        let plan = plan_bursts(&p);
        assert!(!plan.is_empty());
        for burst in &plan {
            let n = burst.sizes.len() as u32;
            assert!(
                n >= p.burst_min && n <= p.burst_max,
                "burst size {n} out of bounds"
            );
            for &s in &burst.sizes {
                assert!(
                    s >= p.size_min_bytes && s <= p.size_max_bytes,
                    "payload size {s} out of bounds"
                );
            }
            let gap = burst.gap.as_millis() as u64;
            assert!(
                gap >= p.gap_min_ms && gap <= p.gap_max_ms,
                "gap {gap} out of bounds"
            );
        }
    }

    #[test]
    fn plan_covers_roughly_the_duration() {
        let p = test_params(3);
        let plan = plan_bursts(&p);
        let total_gap: Duration = plan.iter().map(|b| b.gap).sum();
        // Bursts accumulate until total gap reaches duration, so the sum is at
        // least the duration (and at most one extra max-gap beyond it).
        assert!(total_gap >= p.duration);
        assert!(total_gap < p.duration + Duration::from_millis(p.gap_max_ms));
    }

    #[test]
    fn percentiles_basic() {
        let mut data: Vec<u64> = (1..=100).map(|n| n * 1_000_000).collect(); // 1..100 ms
        let p = percentiles(&mut data);
        assert_eq!(p.count, 100);
        assert!((p.max_ms - 100.0).abs() < 1e-6);
        assert!((p.p50_ms - 51.0).abs() < 1.0);
        assert!((p.p99_ms - 100.0).abs() < 1.0);
    }

    #[test]
    fn percentiles_empty() {
        let p = percentiles(&mut []);
        assert_eq!(p.count, 0);
        assert_eq!(p.max_ms, 0.0);
    }
}
