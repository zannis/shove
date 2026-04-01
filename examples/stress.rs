//! Stress benchmarks for shove under heavy message loads.
//!
//! Measures throughput, latency percentiles (p50/p95/p99), scaling efficiency,
//! and peak memory usage across three load tiers.
//!
//! Requires Docker. Run with:
//!
//!     cargo run -q --example stress --features rabbitmq
//!     cargo run -q --example stress --features rabbitmq -- --tier moderate
//!     cargo run -q --example stress --features rabbitmq -- --tier extreme

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
#[command(name = "stress", about = "Stress benchmarks for shove")]
struct Cli {
    /// Which tier(s) to run.
    #[arg(long, default_value = "all")]
    tier: TierArg,

    /// Which handler profile(s) to run.
    #[arg(long, default_value = "all")]
    handler: HandlerArg,

    /// Output format for the final report.
    #[arg(long, default_value = "table")]
    output: OutputFormat,

    /// Enable concurrent message processing within each consumer.
    /// Processes up to prefetch_count messages concurrently per consumer.
    #[arg(long)]
    concurrent: bool,

    /// Override prefetch count (default: computed from messages/consumers, clamped to [1, 100]).
    #[arg(long)]
    prefetch: Option<u16>,
}

#[derive(Clone, ValueEnum)]
enum HandlerArg {
    Zero,
    Fast,
    Slow,
    Heavy,
    All,
}

#[derive(Clone, ValueEnum)]
enum TierArg {
    Moderate,
    High,
    Extreme,
    All,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

// ── Topic & message ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StressMsg {
    id: u64,
    published_at_ns: u64,
}

define_topic!(
    StressTopic,
    StressMsg,
    TopologyBuilder::new("stress-bench")
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// ── Scenario definition ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum HandlerProfile {
    Zero,
    Fast,
    Slow,
    Heavy,
}

impl std::fmt::Display for HandlerProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerProfile::Zero => write!(f, "zero (no-op)"),
            HandlerProfile::Fast => write!(f, "fast (1-5ms)"),
            HandlerProfile::Slow => write!(f, "slow (50-300ms)"),
            HandlerProfile::Heavy => write!(f, "heavy (1-5s)"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Scenario {
    tier: &'static str,
    messages: u64,
    consumers: u16,
    handler: HandlerProfile,
    deadline: Duration,
    concurrent: bool,
    prefetch: Option<u16>,
}

/// Per-tier configuration with handler-specific message counts.
struct TierConfig {
    name: &'static str,
    consumers: &'static [u16],
    /// Message counts per handler: (zero, fast, slow, heavy).
    messages: (u64, u64, u64, u64),
}

const MODERATE: TierConfig = TierConfig {
    name: "moderate",
    consumers: &[1, 4, 8, 16, 32],
    messages: (50_000, 20_000, 5_000, 1_000),
};

const HIGH: TierConfig = TierConfig {
    name: "high",
    consumers: &[8, 16, 32, 64],
    messages: (500_000, 100_000, 5_000, 100),
};

const EXTREME: TierConfig = TierConfig {
    name: "extreme",
    consumers: &[32, 64, 128, 256],
    messages: (1_000_000, 500_000, 5_000, 500),
};

/// Compute a deadline from the expected runtime with 2x headroom, clamped to [30s, 5min].
fn scenario_deadline(messages: u64, consumers: u16, handler: HandlerProfile) -> Duration {
    let expected_ms = match handler {
        // Zero handler is broker-bound at ~40k msg/s regardless of consumer count.
        HandlerProfile::Zero => messages as f64 / 40.0, // 40k msg/s = 40 msg/ms
        // Handler-bound: scales with consumers.
        HandlerProfile::Fast => (messages as f64 * 3.0) / consumers as f64,
        HandlerProfile::Slow => (messages as f64 * 175.0) / consumers as f64,
        HandlerProfile::Heavy => (messages as f64 * 3000.0) / consumers as f64,
    };
    let deadline_ms = (expected_ms * 2.0).clamp(30_000.0, 300_000.0);
    Duration::from_millis(deadline_ms as u64)
}

fn build_scenarios(
    tier: &TierArg,
    handler: &HandlerArg,
    concurrent: bool,
    prefetch: Option<u16>,
) -> Vec<Scenario> {
    let mut scenarios = Vec::new();

    let handlers: Vec<HandlerProfile> = match handler {
        HandlerArg::Zero => vec![HandlerProfile::Zero],
        HandlerArg::Fast => vec![HandlerProfile::Fast],
        HandlerArg::Slow => vec![HandlerProfile::Slow],
        HandlerArg::Heavy => vec![HandlerProfile::Heavy],
        HandlerArg::All => vec![
            HandlerProfile::Zero,
            HandlerProfile::Fast,
            HandlerProfile::Slow,
            HandlerProfile::Heavy,
        ],
    };

    let tiers: Vec<&TierConfig> = match tier {
        TierArg::Moderate => vec![&MODERATE],
        TierArg::High => vec![&HIGH],
        TierArg::Extreme => vec![&EXTREME],
        TierArg::All => vec![&MODERATE, &HIGH, &EXTREME],
    };

    for tier_cfg in &tiers {
        for &h in &handlers {
            let messages = match h {
                HandlerProfile::Zero => tier_cfg.messages.0,
                HandlerProfile::Fast => tier_cfg.messages.1,
                HandlerProfile::Slow => tier_cfg.messages.2,
                HandlerProfile::Heavy => tier_cfg.messages.3,
            };
            for &c in tier_cfg.consumers {
                scenarios.push(Scenario {
                    tier: tier_cfg.name,
                    messages,
                    consumers: c,
                    handler: h,
                    deadline: scenario_deadline(messages, c, h),
                    concurrent,
                    prefetch,
                });
            }
        }
    }

    scenarios
}

// ── Result types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct ScenarioResult {
    tier: String,
    messages: u64,
    consumers: u16,
    handler: String,
    throughput_msg_per_sec: f64,
    /// Dispatch latency: publish → handler entry (queue wait + framework overhead).
    dispatch_p50_ms: f64,
    dispatch_p95_ms: f64,
    dispatch_p99_ms: f64,
    /// End-to-end latency: publish → handler completion (dispatch + handler work).
    e2e_p50_ms: f64,
    e2e_p95_ms: f64,
    e2e_p99_ms: f64,
    scaling_efficiency: f64,
    peak_rss_mb: f64,
    cpu_pct: f64,
    duration_secs: f64,
}

#[derive(Debug, Clone, Serialize)]
struct FailedResult {
    tier: String,
    messages: u64,
    consumers: u16,
    handler: String,
    error: String,
}

#[derive(Debug, Clone, Serialize)]
struct Report {
    results: Vec<ScenarioResult>,
    failures: Vec<FailedResult>,
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

        // Declare topology.
        let channel = client.create_channel().await.unwrap();
        RabbitMqTopologyDeclarer::new(channel)
            .declare(StressTopic::topology())
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
                "Docker is required to run stress benchmarks. \
                 Install Docker Desktop, colima, or podman and ensure the daemon is running."
            ),
        }
    }

    /// Purge the main queue via the management API so scenarios start clean.
    async fn purge_queue(&self) {
        let http = reqwest::Client::new();
        let url = format!(
            "{}/api/queues/%2F/{}/contents",
            self.mgmt_config.base_url,
            StressTopic::topology().queue()
        );
        let _ = http
            .delete(&url)
            .basic_auth(&self.mgmt_config.username, Some(&self.mgmt_config.password))
            .send()
            .await;
        // Brief pause for the purge to take effect.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    async fn publish_batch(&self, messages: &[StressMsg]) {
        // Publish in chunks of 1000 to avoid overwhelming the channel.
        for chunk in messages.chunks(1000) {
            self.publisher
                .publish_batch::<StressTopic>(chunk)
                .await
                .unwrap();
        }
    }

    async fn shutdown(self) {
        self.client.shutdown().await;
    }
}

// ── Latency recording ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct LatencyRecord {
    /// Time from publish to handler entry (includes broker transit + queue wait).
    enqueue_to_receive_ns: u64,
    /// Time from publish to handler completion (end-to-end).
    enqueue_to_ack_ns: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LatencyPercentiles {
    dispatch_p50: f64,
    dispatch_p95: f64,
    dispatch_p99: f64,
    e2e_p50: f64,
    e2e_p95: f64,
    e2e_p99: f64,
}

struct LatencyRecorder {
    tx: tokio::sync::mpsc::UnboundedSender<LatencyRecord>,
    rx: Mutex<tokio::sync::mpsc::UnboundedReceiver<LatencyRecord>>,
}

impl LatencyRecorder {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(rx),
        }
    }

    fn record(&self, record: LatencyRecord) {
        let _ = self.tx.send(record);
    }

    /// Returns `(dispatch_p50, dispatch_p95, dispatch_p99, e2e_p50, e2e_p95, e2e_p99)`.
    async fn compute_percentiles(&self) -> LatencyPercentiles {
        let mut rx = self.rx.lock().await;
        let mut records = Vec::new();
        while let Ok(r) = rx.try_recv() {
            records.push(r);
        }
        if records.is_empty() {
            return LatencyPercentiles::default();
        }
        let len = records.len();

        // Dispatch latency: publish → handler entry.
        records.sort_unstable_by_key(|r| r.enqueue_to_receive_ns);
        let dispatch_p50 = records[len * 50 / 100].enqueue_to_receive_ns as f64 / 1_000_000.0;
        let dispatch_p95 = records[len * 95 / 100].enqueue_to_receive_ns as f64 / 1_000_000.0;
        let dispatch_p99 = records[len * 99 / 100].enqueue_to_receive_ns as f64 / 1_000_000.0;

        // End-to-end latency: publish → handler completion.
        records.sort_unstable_by_key(|r| r.enqueue_to_ack_ns);
        let e2e_p50 = records[len * 50 / 100].enqueue_to_ack_ns as f64 / 1_000_000.0;
        let e2e_p95 = records[len * 95 / 100].enqueue_to_ack_ns as f64 / 1_000_000.0;
        let e2e_p99 = records[len * 99 / 100].enqueue_to_ack_ns as f64 / 1_000_000.0;

        LatencyPercentiles {
            dispatch_p50,
            dispatch_p95,
            dispatch_p99,
            e2e_p50,
            e2e_p95,
            e2e_p99,
        }
    }
}

// ── Resource sampling ────────────────────────────────────────────────────────

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
            // fields[13] = utime, fields[14] = stime (in clock ticks)
            if fields.len() > 14 {
                let ticks_per_sec = 100.0; // sysconf(_SC_CLK_TCK) is typically 100
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
                    return pages * 4096; // assume 4K page size
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
    cancel: tokio_util::sync::CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl ResourceSampler {
    fn start() -> Self {
        let baseline_rss = current_rss_bytes() as f64 / (1024.0 * 1024.0);
        let baseline_cpu = current_cpu_secs();
        let peak_rss = Arc::new(AtomicU64::new(current_rss_bytes()));
        let cancel = tokio_util::sync::CancellationToken::new();

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
struct StressHandler {
    epoch: Instant,
    processed: Arc<AtomicU64>,
    recorder: Arc<LatencyRecorder>,
    profile: HandlerProfile,
}

impl MessageHandler<StressTopic> for StressHandler {
    async fn handle(&self, msg: StressMsg, _meta: MessageMetadata) -> Outcome {
        let received_at = self.epoch.elapsed().as_nanos() as u64;

        // Simulate work based on handler profile.
        match self.profile {
            HandlerProfile::Zero => {}
            HandlerProfile::Fast => {
                let delay_ms = rand::rng().random_range(1..=5);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            HandlerProfile::Slow => {
                let delay_ms = rand::rng().random_range(50..=300);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            HandlerProfile::Heavy => {
                let delay_ms = rand::rng().random_range(1000..=5000);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        let acked_at = self.epoch.elapsed().as_nanos() as u64;
        self.recorder.record(LatencyRecord {
            enqueue_to_receive_ns: received_at.saturating_sub(msg.published_at_ns),
            enqueue_to_ack_ns: acked_at.saturating_sub(msg.published_at_ns),
        });

        self.processed.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

// ── Scenario execution ──────────────────────────────────────────────────────

struct ScenarioMetrics {
    throughput: f64,
    latencies: LatencyPercentiles,
    peak_rss_mb: f64,
    cpu_pct: f64,
    duration_secs: f64,
}

async fn run_scenario(
    broker: &Broker,
    scenario: &Scenario,
    cancel: &CancellationToken,
) -> Result<ScenarioMetrics, String> {
    broker.purge_queue().await;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));

    // Publish all messages.
    let messages: Vec<StressMsg> = (0..scenario.messages)
        .map(|id| {
            let published_at_ns = epoch.elapsed().as_nanos() as u64;
            StressMsg {
                id,
                published_at_ns,
            }
        })
        .collect();
    broker.publish_batch(&messages).await;

    // Start resource sampler.
    let sampler = ResourceSampler::start();

    // Start consumer group.
    let mut registry = ConsumerGroupRegistry::new(broker.client.clone());
    let pc = processed.clone();
    let rec = recorder.clone();
    let profile = scenario.handler;
    let consumers = scenario.consumers;
    registry
        .register::<StressTopic, StressHandler>(
            ConsumerGroupConfig::new(consumers..=consumers)
                .with_prefetch_count(
                    scenario
                        .prefetch
                        .unwrap_or((scenario.messages / consumers as u64).clamp(1, 100) as u16),
                )
                .with_concurrent_processing(scenario.concurrent),
            move || StressHandler {
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

    // Wait for all messages to be processed, timeout, or cancellation.
    let deadline = tokio::time::Instant::now() + scenario.deadline;
    loop {
        if processed.load(Ordering::Relaxed) >= scenario.messages {
            break;
        }
        if cancel.is_cancelled() {
            registry.shutdown_all().await;
            let _ = sampler.stop().await;
            return Err("interrupted".to_string());
        }
        if tokio::time::Instant::now() >= deadline {
            registry.shutdown_all().await;
            let _ = sampler.stop().await;
            return Err(format!(
                "timeout after {:?}: processed {} / {}",
                scenario.deadline,
                processed.load(Ordering::Relaxed),
                scenario.messages
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let duration = start.elapsed();
    let throughput = scenario.messages as f64 / duration.as_secs_f64();
    let latencies = recorder.compute_percentiles().await;
    let resources = sampler.stop().await;

    registry.shutdown_all().await;

    Ok(ScenarioMetrics {
        throughput,
        latencies,
        peak_rss_mb: resources.peak_rss_mb,
        cpu_pct: resources.cpu_pct,
        duration_secs: duration.as_secs_f64(),
    })
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let scenarios = build_scenarios(&cli.tier, &cli.handler, cli.concurrent, cli.prefetch);

    let broker = Broker::start().await;

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        eprintln!("\ninterrupted, shutting down gracefully...");
        cancel_clone.cancel();
    });

    eprintln!(
        "shove stress benchmarks{}",
        if cli.concurrent { " (concurrent)" } else { "" }
    );
    eprintln!("scenarios: {}\n", scenarios.len());

    let mut results: Vec<ScenarioResult> = Vec::new();
    let mut failures: Vec<FailedResult> = Vec::new();

    for (i, scenario) in scenarios.iter().enumerate() {
        if cancel.is_cancelled() {
            eprintln!("skipping remaining scenarios");
            break;
        }
        let prefetch_str = scenario
            .prefetch
            .map(|p| format!(" | pf={p}"))
            .unwrap_or_default();
        eprintln!(
            "[{}/{}] {} | {}msg | {}c{} | {} ...",
            i + 1,
            scenarios.len(),
            scenario.tier,
            scenario.messages,
            scenario.consumers,
            prefetch_str,
            scenario.handler,
        );

        match run_scenario(&broker, scenario, &cancel).await {
            Ok(m) => {
                eprintln!(
                    "  -> {:.1} msg/s | dispatch p50={:.1}ms p99={:.1}ms | e2e p50={:.1}ms p99={:.1}ms | cpu={:.0}% rss={:.1}MB | {:.1}s",
                    m.throughput,
                    m.latencies.dispatch_p50,
                    m.latencies.dispatch_p99,
                    m.latencies.e2e_p50,
                    m.latencies.e2e_p99,
                    m.cpu_pct,
                    m.peak_rss_mb,
                    m.duration_secs
                );
                results.push(ScenarioResult {
                    tier: scenario.tier.to_string(),
                    messages: scenario.messages,
                    consumers: scenario.consumers,
                    handler: scenario.handler.to_string(),
                    throughput_msg_per_sec: m.throughput,
                    dispatch_p50_ms: m.latencies.dispatch_p50,
                    dispatch_p95_ms: m.latencies.dispatch_p95,
                    dispatch_p99_ms: m.latencies.dispatch_p99,
                    e2e_p50_ms: m.latencies.e2e_p50,
                    e2e_p95_ms: m.latencies.e2e_p95,
                    e2e_p99_ms: m.latencies.e2e_p99,
                    scaling_efficiency: 0.0, // computed below
                    peak_rss_mb: m.peak_rss_mb,
                    cpu_pct: m.cpu_pct,
                    duration_secs: m.duration_secs,
                });
            }
            Err(e) => {
                eprintln!("  -> FAILED: {e}");
                failures.push(FailedResult {
                    tier: scenario.tier.to_string(),
                    messages: scenario.messages,
                    consumers: scenario.consumers,
                    handler: scenario.handler.to_string(),
                    error: e,
                });
            }
        }
    }

    // Compute scaling efficiency: throughput(N) / throughput(min_consumers) for same (tier, messages, handler).
    let mut baseline_throughputs: std::collections::HashMap<(String, u64, String), (u16, f64)> =
        std::collections::HashMap::new();
    for r in &results {
        let key = (r.tier.clone(), r.messages, r.handler.clone());
        let entry = baseline_throughputs
            .entry(key)
            .or_insert((r.consumers, r.throughput_msg_per_sec));
        if r.consumers < entry.0 {
            *entry = (r.consumers, r.throughput_msg_per_sec);
        }
    }

    for result in &mut results {
        let key = (result.tier.clone(), result.messages, result.handler.clone());
        if let Some(&(_, baseline)) = baseline_throughputs.get(&key)
            && baseline > 0.0
        {
            result.scaling_efficiency = result.throughput_msg_per_sec / baseline;
        }
    }

    let report = Report { results, failures };

    // Print report in requested format.
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
        "{:<10} {:>8} {:>5} {:>8} {:>8}  {:>9} {:>9} {:>9}  {:>9} {:>9} {:>9}  {:>6} {:>7} {:>5}",
        "TIER",
        "MSGS",
        "C",
        "HANDLER",
        "MSG/SEC",
        "disp p50",
        "disp p95",
        "disp p99",
        "e2e p50",
        "e2e p95",
        "e2e p99",
        "SCALE",
        "RSS(MB)",
        "CPU%"
    );
    println!("{}", "-".repeat(145));
    for r in &report.results {
        println!(
            "{:<10} {:>8} {:>5} {:>8} {:>8.0}  {:>8.1}ms {:>8.1}ms {:>8.1}ms  {:>8.1}ms {:>8.1}ms {:>8.1}ms  {:>5.1}x {:>7.1} {:>4.0}%",
            r.tier,
            r.messages,
            r.consumers,
            r.handler,
            r.throughput_msg_per_sec,
            r.dispatch_p50_ms,
            r.dispatch_p95_ms,
            r.dispatch_p99_ms,
            r.e2e_p50_ms,
            r.e2e_p95_ms,
            r.e2e_p99_ms,
            r.scaling_efficiency,
            r.peak_rss_mb,
            r.cpu_pct,
        );
    }
    println!();
    println!("dispatch = publish → handler entry (queue wait + framework overhead)");
    println!("e2e      = publish → handler completion (dispatch + handler work)");
    if !report.failures.is_empty() {
        println!("\nFailed scenarios:");
        for f in &report.failures {
            println!(
                "  {} | {}msg | {}c | {} — {}",
                f.tier, f.messages, f.consumers, f.handler, f.error
            );
        }
    }
}
