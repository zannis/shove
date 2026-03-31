//! Consumer-count overhead benchmark for shove.
//!
//! Measures how throughput and per-consumer efficiency degrade as
//! the number of consumers increases from 128 to 4096.
//!
//! Requires Docker. Run with:
//!
//!     cargo bench -q --features rabbitmq --bench consumer_overhead
//!     cargo bench -q --features rabbitmq --bench consumer_overhead -- --handler slow
//!     cargo bench -q --features rabbitmq --bench consumer_overhead -- --concurrent
//!     cargo bench -q --features rabbitmq --bench consumer_overhead -- --output json

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use shove::rabbitmq::*;
use shove::*;
use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "consumer_overhead",
    about = "Benchmark overhead as consumer count increases (128..4096)"
)]
struct Cli {
    /// Which handler profile to run.
    #[arg(long, default_value = "fast")]
    handler: HandlerArg,

    /// Output format for the final report.
    #[arg(long, default_value = "table")]
    output: OutputFormat,

    /// Enable concurrent message processing within each consumer.
    #[arg(long)]
    concurrent: bool,

    /// Number of iterations per consumer count (results are averaged).
    #[arg(long, default_value = "3")]
    iterations: u32,

    /// Custom consumer counts (comma-separated). Overrides the default sweep.
    #[arg(long, value_delimiter = ',')]
    consumers: Option<Vec<u16>>,
}

#[derive(Clone, ValueEnum)]
enum HandlerArg {
    Fast,
    Slow,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

// ── Topic & message ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchMsg {
    id: u64,
    published_at_ns: u64,
}

define_topic!(
    OverheadTopic,
    BenchMsg,
    TopologyBuilder::new("overhead-bench")
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// ── Handler profiles ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum HandlerProfile {
    Fast,
    Slow,
}

impl std::fmt::Display for HandlerProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerProfile::Fast => write!(f, "fast (1-5ms)"),
            HandlerProfile::Slow => write!(f, "slow (50-300ms)"),
        }
    }
}

// ── Results ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct RunResult {
    consumers: u16,
    handler: String,
    concurrent: bool,
    iterations: u32,
    messages_per_run: u64,
    avg_throughput: f64,
    per_consumer_throughput: f64,
    avg_latency_p50_ms: f64,
    avg_latency_p95_ms: f64,
    avg_latency_p99_ms: f64,
    avg_peak_rss_mb: f64,
    avg_cpu_pct: f64,
    /// Throughput relative to the first (lowest) consumer count.
    scaling_factor: f64,
    /// Ideal throughput if scaling were linear from the first consumer count.
    ideal_throughput: f64,
    /// Overhead percentage: how much throughput is lost vs ideal linear scaling.
    overhead_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct Report {
    results: Vec<RunResult>,
}

// ── Infrastructure ──────────────────────────────────────────────────────────

struct Broker {
    client: RabbitMqClient,
    publisher: RabbitMqPublisher,
    mgmt_config: ManagementConfig,
    _container: testcontainers::ContainerAsync<RabbitMq>,
}

impl Broker {
    async fn start() -> Self {
        Self::require_docker();

        eprintln!("starting RabbitMQ container...");
        let container = RabbitMq::default().start().await.unwrap();
        let host = container.get_host().await.unwrap().to_string();
        let amqp_port = container.get_host_port_ipv4(5672).await.unwrap();
        let mgmt_port = container.get_host_port_ipv4(15672).await.unwrap();

        // Enable consistent-hash exchange plugin.
        let mut result = container
            .exec(ExecCommand::new([
                "rabbitmq-plugins",
                "enable",
                "rabbitmq_consistent_hash_exchange",
            ]))
            .await
            .unwrap();
        let _ = result.stdout_to_vec().await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        let uri = format!("amqp://guest:guest@{host}:{amqp_port}");
        let client = RabbitMqClient::connect(&RabbitMqConfig::new(&uri))
            .await
            .unwrap();

        let channel = client.create_channel().await.unwrap();
        RabbitMqTopologyDeclarer::new(channel)
            .declare(OverheadTopic::topology())
            .await
            .unwrap();

        let publisher = RabbitMqPublisher::new(client.clone()).await.unwrap();

        let mgmt_config =
            ManagementConfig::new(format!("http://{host}:{mgmt_port}"), "guest", "guest");

        eprintln!("RabbitMQ ready at {uri}");

        Self {
            client,
            publisher,
            mgmt_config,
            _container: container,
        }
    }

    fn require_docker() {
        match std::process::Command::new("docker").arg("info").output() {
            Ok(o) if o.status.success() => {}
            _ => panic!(
                "Docker is required. \
                 Install Docker Desktop, colima, or podman and ensure the daemon is running."
            ),
        }
    }

    async fn purge_queue(&self) {
        let http = reqwest::Client::new();
        let url = format!(
            "{}/api/queues/%2F/{}/contents",
            self.mgmt_config.base_url,
            OverheadTopic::topology().queue()
        );
        let _ = http
            .delete(&url)
            .basic_auth(&self.mgmt_config.username, Some(&self.mgmt_config.password))
            .send()
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    async fn publish_batch(&self, messages: &[BenchMsg]) {
        for chunk in messages.chunks(1000) {
            self.publisher
                .publish_batch::<OverheadTopic>(chunk)
                .await
                .unwrap();
        }
    }

    async fn shutdown(self) {
        self.client.shutdown().await;
    }
}

// ── Latency recording ───────────────────────────────────────────────────────

struct LatencyRecorder {
    tx: tokio::sync::mpsc::UnboundedSender<u64>,
    rx: Mutex<tokio::sync::mpsc::UnboundedReceiver<u64>>,
}

impl LatencyRecorder {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(rx),
        }
    }

    fn record(&self, enqueue_to_ack_ns: u64) {
        let _ = self.tx.send(enqueue_to_ack_ns);
    }

    async fn compute_percentiles(&self) -> (f64, f64, f64) {
        let mut rx = self.rx.lock().await;
        let mut records = Vec::new();
        while let Ok(r) = rx.try_recv() {
            records.push(r);
        }
        if records.is_empty() {
            return (0.0, 0.0, 0.0);
        }
        records.sort_unstable();
        let len = records.len();
        let p50 = records[len * 50 / 100] as f64 / 1_000_000.0;
        let p95 = records[len * 95 / 100] as f64 / 1_000_000.0;
        let p99 = records[len * 99 / 100] as f64 / 1_000_000.0;
        (p50, p95, p99)
    }
}

// ── Resource sampling ───────────────────────────────────────────────────────

fn current_cpu_secs() -> f64 {
    #[cfg(target_os = "macos")]
    {
        use mach2::task::task_info;
        use mach2::task_info::{MACH_TASK_BASIC_INFO, mach_task_basic_info, task_flavor_t};
        let mut info: mach_task_basic_info = unsafe { std::mem::zeroed() };
        let mut count = (size_of::<mach_task_basic_info>() / size_of::<u32>()) as u32;
        let kr = unsafe {
            task_info(
                mach2::traps::mach_task_self(),
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
                mach2::traps::mach_task_self(),
                MACH_TASK_BASIC_INFO as task_flavor_t,
                &mut info as *mut _ as *mut i32,
                &mut count,
            )
        };
        if kr == 0 {
            info.resident_size as u64
        } else {
            0
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(rss_pages) = content.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    return pages * 4096;
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

struct ResourceSnapshot {
    peak_rss_mb: f64,
    cpu_pct: f64,
}

struct ResourceSampler {
    peak_rss: Arc<AtomicU64>,
    baseline_rss: f64,
    baseline_cpu: f64,
    start: Instant,
    cancel: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl ResourceSampler {
    fn start() -> Self {
        let baseline_rss = current_rss_bytes() as f64 / (1024.0 * 1024.0);
        let baseline_cpu = current_cpu_secs();
        let peak_rss = Arc::new(AtomicU64::new(current_rss_bytes()));
        let cancel = CancellationToken::new();

        let peak = peak_rss.clone();
        let token = cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        let rss = current_rss_bytes();
                        peak.fetch_max(rss, Ordering::Relaxed);
                    }
                }
            }
        });

        Self {
            peak_rss,
            baseline_rss,
            baseline_cpu,
            start: Instant::now(),
            cancel,
            handle: Some(handle),
        }
    }

    async fn stop(mut self) -> ResourceSnapshot {
        self.cancel.cancel();
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }

        let peak_rss_mb = self.peak_rss.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
        let rss_delta = (peak_rss_mb - self.baseline_rss).max(0.0);

        let wall_secs = self.start.elapsed().as_secs_f64();
        let cpu_delta = current_cpu_secs() - self.baseline_cpu;
        let cpu_pct = if wall_secs > 0.0 {
            (cpu_delta / wall_secs) * 100.0
        } else {
            0.0
        };

        ResourceSnapshot {
            peak_rss_mb: rss_delta,
            cpu_pct,
        }
    }
}

// ── Handler ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct BenchHandler {
    epoch: Instant,
    processed: Arc<AtomicU64>,
    recorder: Arc<LatencyRecorder>,
    profile: HandlerProfile,
}

impl MessageHandler<OverheadTopic> for BenchHandler {
    async fn handle(&self, msg: BenchMsg, _meta: MessageMetadata) -> Outcome {
        match self.profile {
            HandlerProfile::Fast => {
                let delay_ms = rand::rng().random_range(1..=5);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            HandlerProfile::Slow => {
                let delay_ms = rand::rng().random_range(50..=300);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        let acked_at = self.epoch.elapsed().as_nanos() as u64;
        self.recorder
            .record(acked_at.saturating_sub(msg.published_at_ns));

        self.processed.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

// ── Single run ──────────────────────────────────────────────────────────────

struct IterationResult {
    throughput: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    peak_rss_mb: f64,
    cpu_pct: f64,
}

/// Message count scaled to consumer count so each run takes a reasonable time.
fn messages_for(consumers: u16, profile: HandlerProfile) -> u64 {
    match profile {
        // For fast handlers: enough messages so each consumer processes ~200,
        // giving a ~600ms minimum runtime per consumer.
        HandlerProfile::Fast => (consumers as u64 * 200).max(25_000),
        // For slow handlers: ~20 messages per consumer (mean 175ms each ≈ 3.5s per consumer).
        HandlerProfile::Slow => (consumers as u64 * 20).max(2_500),
    }
}

fn deadline_for(messages: u64, consumers: u16, profile: HandlerProfile) -> Duration {
    let expected_ms = match profile {
        HandlerProfile::Fast => (messages as f64 * 3.0) / consumers as f64,
        HandlerProfile::Slow => (messages as f64 * 175.0) / consumers as f64,
    };
    let deadline_ms = (expected_ms * 3.0).clamp(30_000.0, 600_000.0);
    Duration::from_millis(deadline_ms as u64)
}

async fn run_once(
    broker: &Broker,
    consumers: u16,
    profile: HandlerProfile,
    concurrent: bool,
    cancel: &CancellationToken,
) -> Result<IterationResult, String> {
    broker.purge_queue().await;

    let messages = messages_for(consumers, profile);
    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));

    // Publish all messages.
    let msgs: Vec<BenchMsg> = (0..messages)
        .map(|id| {
            let published_at_ns = epoch.elapsed().as_nanos() as u64;
            BenchMsg {
                id,
                published_at_ns,
            }
        })
        .collect();
    broker.publish_batch(&msgs).await;

    let sampler = ResourceSampler::start();

    // Start consumer group.
    let mut registry = ConsumerGroupRegistry::new(broker.client.clone());
    let pc = processed.clone();
    let rec = recorder.clone();
    registry
        .register::<OverheadTopic, BenchHandler>(
            ConsumerGroupConfig::new(consumers..=consumers)
                .with_prefetch_count((messages / consumers as u64).clamp(1, 100) as u16)
                .with_concurrent_processing(concurrent),
            move || BenchHandler {
                epoch,
                processed: pc.clone(),
                recorder: rec.clone(),
                profile,
            },
        )
        .await
        .map_err(|e| e.to_string())?;

    let start = Instant::now();
    registry.start_all();

    let deadline = tokio::time::Instant::now() + deadline_for(messages, consumers, profile);
    loop {
        if processed.load(Ordering::Relaxed) >= messages {
            break;
        }
        if cancel.is_cancelled() {
            registry.shutdown_all().await;
            let _ = sampler.stop().await;
            return Err("interrupted".to_string());
        }
        if tokio::time::Instant::now() >= deadline {
            let done = processed.load(Ordering::Relaxed);
            registry.shutdown_all().await;
            let _ = sampler.stop().await;
            return Err(format!(
                "timeout: processed {done} / {messages} ({:.0}%)",
                done as f64 / messages as f64 * 100.0
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let duration = start.elapsed();
    let throughput = messages as f64 / duration.as_secs_f64();
    let (p50, p95, p99) = recorder.compute_percentiles().await;
    let resources = sampler.stop().await;

    registry.shutdown_all().await;

    Ok(IterationResult {
        throughput,
        p50,
        p95,
        p99,
        peak_rss_mb: resources.peak_rss_mb,
        cpu_pct: resources.cpu_pct,
    })
}

// ── Main ────────────────────────────────────────────────────────────────────

const DEFAULT_CONSUMER_COUNTS: &[u16] = &[128, 256, 512, 1024, 2048, 4096];

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let profile = match cli.handler {
        HandlerArg::Fast => HandlerProfile::Fast,
        HandlerArg::Slow => HandlerProfile::Slow,
    };
    let consumer_counts = cli
        .consumers
        .unwrap_or_else(|| DEFAULT_CONSUMER_COUNTS.to_vec());
    let iterations = cli.iterations;

    let broker = Broker::start().await;

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        eprintln!("\ninterrupted, shutting down gracefully...");
        cancel_clone.cancel();
    });

    eprintln!(
        "consumer overhead benchmark | {} | {}x iterations | {}",
        profile,
        iterations,
        if cli.concurrent {
            "concurrent"
        } else {
            "sequential"
        }
    );
    eprintln!("consumer counts: {:?}\n", consumer_counts);

    let mut results: Vec<RunResult> = Vec::new();

    for &count in &consumer_counts {
        if cancel.is_cancelled() {
            eprintln!("skipping remaining counts");
            break;
        }

        let messages = messages_for(count, profile);
        eprintln!(
            "  {}c | {}msg x {} iterations ...",
            count, messages, iterations
        );

        let mut throughputs = Vec::new();
        let mut p50s = Vec::new();
        let mut p95s = Vec::new();
        let mut p99s = Vec::new();
        let mut rss_mbs = Vec::new();
        let mut cpu_pcts = Vec::new();
        let mut failed = false;

        for iter in 0..iterations {
            match run_once(&broker, count, profile, cli.concurrent, &cancel).await {
                Ok(r) => {
                    eprintln!(
                        "    [{}/{}] {:.0} msg/s | p50={:.1}ms p95={:.1}ms p99={:.1}ms",
                        iter + 1,
                        iterations,
                        r.throughput,
                        r.p50,
                        r.p95,
                        r.p99
                    );
                    throughputs.push(r.throughput);
                    p50s.push(r.p50);
                    p95s.push(r.p95);
                    p99s.push(r.p99);
                    rss_mbs.push(r.peak_rss_mb);
                    cpu_pcts.push(r.cpu_pct);
                }
                Err(e) => {
                    eprintln!("    [{}/{}] FAILED: {e}", iter + 1, iterations);
                    failed = true;
                    break;
                }
            }
        }

        if failed || throughputs.is_empty() {
            continue;
        }

        let avg = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
        let avg_throughput = avg(&throughputs);

        results.push(RunResult {
            consumers: count,
            handler: profile.to_string(),
            concurrent: cli.concurrent,
            iterations,
            messages_per_run: messages,
            avg_throughput,
            per_consumer_throughput: avg_throughput / count as f64,
            avg_latency_p50_ms: avg(&p50s),
            avg_latency_p95_ms: avg(&p95s),
            avg_latency_p99_ms: avg(&p99s),
            avg_peak_rss_mb: avg(&rss_mbs),
            avg_cpu_pct: avg(&cpu_pcts),
            // Filled in below.
            scaling_factor: 0.0,
            ideal_throughput: 0.0,
            overhead_pct: 0.0,
        });
    }

    // Compute scaling factors and overhead relative to the first consumer count.
    if let Some(baseline) = results.first() {
        let base_throughput = baseline.avg_throughput;
        let base_consumers = baseline.consumers as f64;
        let throughput_per_consumer = base_throughput / base_consumers;

        for r in &mut results {
            r.scaling_factor = r.avg_throughput / base_throughput;
            r.ideal_throughput = throughput_per_consumer * r.consumers as f64;
            r.overhead_pct = if r.ideal_throughput > 0.0 {
                (1.0 - r.avg_throughput / r.ideal_throughput) * 100.0
            } else {
                0.0
            };
        }
    }

    let report = Report { results };

    match cli.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&report).unwrap();
            println!("{json}");
        }
        OutputFormat::Table => {
            print_table(&report);
        }
    }

    broker.shutdown().await;
}

fn print_table(report: &Report) {
    println!();
    println!(
        "{:>10} {:>10} {:>10} {:>10} {:>10} {:>8} {:>8} {:>8} {:>8} {:>8} {:>6}",
        "CONSUMERS",
        "MSGS",
        "MSG/SEC",
        "MSG/SEC/C",
        "IDEAL",
        "OVRHEAD",
        "p50",
        "p95",
        "p99",
        "RSS(MB)",
        "CPU%"
    );
    println!("{}", "-".repeat(117));
    for r in &report.results {
        println!(
            "{:>10} {:>10} {:>10.0} {:>10.1} {:>10.0} {:>7.1}% {:>7.1}ms {:>7.1}ms {:>7.1}ms {:>8.1} {:>5.0}%",
            r.consumers,
            r.messages_per_run,
            r.avg_throughput,
            r.per_consumer_throughput,
            r.ideal_throughput,
            r.overhead_pct,
            r.avg_latency_p50_ms,
            r.avg_latency_p95_ms,
            r.avg_latency_p99_ms,
            r.avg_peak_rss_mb,
            r.avg_cpu_pct,
        );
    }
    println!();
    println!(
        "OVERHEAD = % throughput lost vs ideal linear scaling from the lowest consumer count."
    );
    println!(
        "MSG/SEC/C = throughput per consumer (efficiency). Drops indicate coordination overhead."
    );
}
