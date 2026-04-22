//! Shared stress-benchmark harness for all shove backends.
//!
//! Each backend's `stress.rs` is a thin wrapper that constructs a
//! `Broker<B>` and calls either [`run_all_scenarios`] (coordinated-group
//! backends: InMemory, Kafka, NATS, RabbitMQ) or
//! [`run_supervisor_scenarios`] (SQS).

#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use mach2::traps::mach_task_self;
use rand::RngExt;
use serde::{Deserialize, Serialize};
use shove::{
    Broker, ConsumerGroupConfig, ConsumerOptions,
    backend::{Backend, capability::HasCoordinatedGroups},
    handler::MessageHandler,
    metadata::MessageMetadata,
    outcome::Outcome,
    topology::TopologyBuilder,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "stress", about = "Stress benchmarks for shove")]
pub struct Cli {
    /// Which tier(s) to run.
    #[arg(long, default_value = "all")]
    pub tier: TierArg,

    /// Which handler profile(s) to run.
    #[arg(long, default_value = "all")]
    pub handler: HandlerArg,

    /// Output format for the final report.
    #[arg(long, default_value = "table")]
    pub output: OutputFormat,

    /// Enable concurrent message processing within each consumer.
    #[arg(long)]
    pub concurrent: bool,

    /// Override prefetch count (default: computed from messages/consumers, clamped per-backend).
    #[arg(long)]
    pub prefetch: Option<u16>,
}

#[derive(Clone, ValueEnum)]
pub enum HandlerArg {
    Zero,
    Fast,
    Slow,
    Heavy,
    All,
}

#[derive(Clone, ValueEnum)]
pub enum TierArg {
    Moderate,
    High,
    Extreme,
    All,
}

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
}

// ── Topic & message ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressTestMsg {
    pub id: u64,
    pub published_at_ns: u64,
}

shove::define_topic!(
    pub StressTestTopic,
    StressTestMsg,
    TopologyBuilder::new("shove-stress-bench")
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// ── Scenario definition ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerProfile {
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
pub struct Scenario {
    pub tier: &'static str,
    pub messages: u64,
    pub consumers: u16,
    pub handler: HandlerProfile,
    pub deadline: Duration,
    pub concurrent: bool,
    pub prefetch: Option<u16>,
}

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

fn scenario_deadline(messages: u64, consumers: u16, handler: HandlerProfile) -> Duration {
    let expected_ms = match handler {
        HandlerProfile::Zero => messages as f64 / 40.0,
        HandlerProfile::Fast => (messages as f64 * 3.0) / consumers as f64,
        HandlerProfile::Slow => (messages as f64 * 175.0) / consumers as f64,
        HandlerProfile::Heavy => (messages as f64 * 3000.0) / consumers as f64,
    };
    let deadline_ms = (expected_ms * 2.0).clamp(30_000.0, 300_000.0);
    Duration::from_millis(deadline_ms as u64)
}

fn build_scenarios(cli: &Cli) -> Vec<Scenario> {
    let handlers: Vec<HandlerProfile> = match cli.handler {
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

    let tiers: Vec<&TierConfig> = match cli.tier {
        TierArg::Moderate => vec![&MODERATE],
        TierArg::High => vec![&HIGH],
        TierArg::Extreme => vec![&EXTREME],
        TierArg::All => vec![&MODERATE, &HIGH, &EXTREME],
    };

    let mut scenarios = Vec::new();
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
                    concurrent: cli.concurrent,
                    prefetch: cli.prefetch,
                });
            }
        }
    }
    scenarios
}

/// Panic if Docker is unreachable. Shared by all container-backed backends.
pub fn require_docker() {
    match std::process::Command::new("docker").arg("info").output() {
        Ok(o) if o.status.success() => {}
        _ => panic!(
            "Docker is required to run stress benchmarks. \
             Install Docker Desktop, colima, or podman and ensure the daemon is running."
        ),
    }
}

// ── Latency recording ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct LatencyRecord {
    enqueue_to_receive_ns: u64,
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

        records.sort_unstable_by_key(|r| r.enqueue_to_receive_ns);
        let dispatch_p50 = records[len * 50 / 100].enqueue_to_receive_ns as f64 / 1_000_000.0;
        let dispatch_p95 = records[len * 95 / 100].enqueue_to_receive_ns as f64 / 1_000_000.0;
        let dispatch_p99 = records[len * 99 / 100].enqueue_to_receive_ns as f64 / 1_000_000.0;

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
        if kr == 0 {
            info.resident_size as u64
        } else {
            0
        }
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
pub struct StressTestHandler {
    epoch: Instant,
    processed: Arc<AtomicU64>,
    recorder: Arc<LatencyRecorder>,
    profile: HandlerProfile,
}

impl MessageHandler<StressTestTopic> for StressTestHandler {
    type Context = ();
    async fn handle(&self, msg: StressTestMsg, _meta: MessageMetadata, _: &()) -> Outcome {
        let received_at = self.epoch.elapsed().as_nanos() as u64;

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

fn default_prefetch(messages: u64, consumers: u16, cap: u16) -> u16 {
    (messages / consumers as u64).clamp(1, cap as u64) as u16
}

/// Purge closure — invoked between scenarios to clear the main queue.
/// Default is a no-op via [`noop_purge`].
pub type PurgeFn = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub fn noop_purge() -> PurgeFn {
    Box::new(|| Box::pin(async {}))
}

/// Knobs a backend binary supplies to the harness.
pub struct HarnessConfig<B: Backend> {
    pub backend_name: &'static str,
    /// Upper bound for computed default prefetch (e.g. SQS caps at 10).
    pub prefetch_cap: u16,
    /// Maximum batch size for `publish_batch` (some backends have SDK limits).
    pub publish_chunk_size: usize,
    /// Drain the main queue between scenarios.
    pub purge: PurgeFn,
    _backend: std::marker::PhantomData<fn() -> B>,
}

impl<B: Backend> HarnessConfig<B> {
    pub fn new(backend_name: &'static str) -> Self {
        Self {
            backend_name,
            prefetch_cap: 100,
            publish_chunk_size: 1000,
            purge: noop_purge(),
            _backend: std::marker::PhantomData,
        }
    }

    pub fn with_prefetch_cap(mut self, cap: u16) -> Self {
        self.prefetch_cap = cap;
        self
    }

    pub fn with_publish_chunk_size(mut self, chunk: usize) -> Self {
        self.publish_chunk_size = chunk;
        self
    }

    pub fn with_purge(mut self, purge: PurgeFn) -> Self {
        self.purge = purge;
        self
    }
}

/// Execute a single coordinated-group scenario. A fresh `Broker<B>` is built
/// per scenario because the generic `run_until_timeout` path trips the
/// broker-wide shutdown token once it completes.
async fn run_scenario_group<B, MkCfg, Connect, Fut>(
    hcfg: &HarnessConfig<B>,
    scenario: &Scenario,
    cancel: &CancellationToken,
    make_cfg: &MkCfg,
    connect: &Connect,
) -> Result<ScenarioMetrics, String>
where
    B: Backend + HasCoordinatedGroups,
    MkCfg: Fn(u16, u16, bool) -> B::ConsumerGroupConfig,
    Connect: Fn() -> Fut,
    Fut: Future<Output = Broker<B>>,
{
    let broker = connect().await;
    broker
        .topology()
        .declare::<StressTestTopic>()
        .await
        .map_err(|e| format!("declare: {e}"))?;
    let publisher = broker
        .publisher()
        .await
        .map_err(|e| format!("publisher: {e}"))?;
    (hcfg.purge)().await;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));

    let sampler = ResourceSampler::start();

    let prefetch = scenario.prefetch.unwrap_or_else(|| {
        default_prefetch(scenario.messages, scenario.consumers, hcfg.prefetch_cap)
    });

    let pc = processed.clone();
    let rec = recorder.clone();
    let profile = scenario.handler;
    let factory = move || StressTestHandler {
        epoch,
        processed: pc.clone(),
        recorder: rec.clone(),
        profile,
    };

    let mut group = broker.consumer_group();
    let inner_cfg = make_cfg(scenario.consumers, prefetch, scenario.concurrent);
    group
        .register::<StressTestTopic, _>(ConsumerGroupConfig::new(inner_cfg), factory)
        .await
        .map_err(|e| e.to_string())?;

    // Per-scenario stop signal (distinct from the broker's global shutdown
    // token, which we must not trip between scenarios).
    let scenario_stop = CancellationToken::new();
    let run_stop = scenario_stop.clone();
    let run_handle = tokio::spawn(async move {
        group
            .run_until_timeout(
                async move { run_stop.cancelled().await },
                Duration::from_secs(30),
            )
            .await
    });

    let start = Instant::now();

    let messages: Vec<StressTestMsg> = (0..scenario.messages)
        .map(|id| StressTestMsg {
            id,
            published_at_ns: epoch.elapsed().as_nanos() as u64,
        })
        .collect();
    for chunk in messages.chunks(hcfg.publish_chunk_size) {
        publisher
            .publish_batch::<StressTestTopic>(chunk)
            .await
            .map_err(|e| format!("publish_batch: {e}"))?;
    }

    let deadline = tokio::time::Instant::now() + scenario.deadline;
    let outcome = loop {
        if processed.load(Ordering::Relaxed) >= scenario.messages {
            break Ok(());
        }
        if cancel.is_cancelled() {
            break Err("interrupted".to_string());
        }
        if tokio::time::Instant::now() >= deadline {
            let done = processed.load(Ordering::Relaxed);
            break Err(format!(
                "timeout after {:?}: processed {done} / {}",
                scenario.deadline, scenario.messages
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let duration = start.elapsed();

    // Signal the consumer group to stop and wait for the drain to complete.
    scenario_stop.cancel();
    let _ = run_handle.await;

    let resources = sampler.stop().await;
    drop(publisher);
    broker.close().await;
    outcome?;

    let throughput = scenario.messages as f64 / duration.as_secs_f64();
    let latencies = recorder.compute_percentiles().await;

    Ok(ScenarioMetrics {
        throughput,
        latencies,
        peak_rss_mb: resources.peak_rss_mb,
        cpu_pct: resources.cpu_pct,
        duration_secs: duration.as_secs_f64(),
    })
}

/// Execute a single supervisor scenario (SQS). A fresh broker is built per
/// scenario for the same reason as [`run_scenario_group`].
async fn run_scenario_supervisor<B, MkOpts, Connect, Fut>(
    hcfg: &HarnessConfig<B>,
    scenario: &Scenario,
    cancel: &CancellationToken,
    make_opts: &MkOpts,
    connect: &Connect,
) -> Result<ScenarioMetrics, String>
where
    B: Backend,
    MkOpts: Fn(u16, bool) -> ConsumerOptions<B>,
    Connect: Fn() -> Fut,
    Fut: Future<Output = Broker<B>>,
{
    let broker = connect().await;
    broker
        .topology()
        .declare::<StressTestTopic>()
        .await
        .map_err(|e| format!("declare: {e}"))?;
    let publisher = broker
        .publisher()
        .await
        .map_err(|e| format!("publisher: {e}"))?;
    (hcfg.purge)().await;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));

    let sampler = ResourceSampler::start();

    let prefetch = scenario.prefetch.unwrap_or_else(|| {
        default_prefetch(scenario.messages, scenario.consumers, hcfg.prefetch_cap)
    });

    let mut supervisor = broker.consumer_supervisor();
    for _ in 0..scenario.consumers {
        let handler = StressTestHandler {
            epoch,
            processed: processed.clone(),
            recorder: recorder.clone(),
            profile: scenario.handler,
        };
        let opts = make_opts(prefetch, scenario.concurrent);
        supervisor
            .register::<StressTestTopic, _>(handler, opts)
            .map_err(|e| e.to_string())?;
    }

    // Per-scenario stop signal (distinct from the supervisor's own
    // cancellation token so we don't trip global state between scenarios).
    let scenario_stop = CancellationToken::new();
    let run_stop = scenario_stop.clone();
    let run_handle = tokio::spawn(async move {
        supervisor
            .run_until_timeout(
                async move { run_stop.cancelled().await },
                Duration::from_secs(30),
            )
            .await
    });

    let start = Instant::now();

    let messages: Vec<StressTestMsg> = (0..scenario.messages)
        .map(|id| StressTestMsg {
            id,
            published_at_ns: epoch.elapsed().as_nanos() as u64,
        })
        .collect();
    for chunk in messages.chunks(hcfg.publish_chunk_size) {
        publisher
            .publish_batch::<StressTestTopic>(chunk)
            .await
            .map_err(|e| format!("publish_batch: {e}"))?;
    }

    let deadline = tokio::time::Instant::now() + scenario.deadline;
    let outcome = loop {
        if processed.load(Ordering::Relaxed) >= scenario.messages {
            break Ok(());
        }
        if cancel.is_cancelled() {
            break Err("interrupted".to_string());
        }
        if tokio::time::Instant::now() >= deadline {
            let done = processed.load(Ordering::Relaxed);
            break Err(format!(
                "timeout after {:?}: processed {done} / {}",
                scenario.deadline, scenario.messages
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let duration = start.elapsed();

    // Signal the supervisor to stop and wait for the drain to complete.
    scenario_stop.cancel();
    let _ = run_handle.await;

    let resources = sampler.stop().await;
    drop(publisher);
    broker.close().await;
    outcome?;

    let throughput = scenario.messages as f64 / duration.as_secs_f64();
    let latencies = recorder.compute_percentiles().await;

    Ok(ScenarioMetrics {
        throughput,
        latencies,
        peak_rss_mb: resources.peak_rss_mb,
        cpu_pct: resources.cpu_pct,
        duration_secs: duration.as_secs_f64(),
    })
}

// ── Reporting ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct ScenarioResult {
    tier: String,
    messages: u64,
    consumers: u16,
    handler: String,
    throughput_msg_per_sec: f64,
    dispatch_p50_ms: f64,
    dispatch_p95_ms: f64,
    dispatch_p99_ms: f64,
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
    backend: String,
    results: Vec<ScenarioResult>,
    failures: Vec<FailedResult>,
}

fn compute_scaling(results: &mut [ScenarioResult]) {
    use std::collections::HashMap;
    let mut baselines: HashMap<(String, u64, String), (u16, f64)> = HashMap::new();
    for r in results.iter() {
        let key = (r.tier.clone(), r.messages, r.handler.clone());
        let entry = baselines
            .entry(key)
            .or_insert((r.consumers, r.throughput_msg_per_sec));
        if r.consumers < entry.0 {
            *entry = (r.consumers, r.throughput_msg_per_sec);
        }
    }
    for r in results.iter_mut() {
        let key = (r.tier.clone(), r.messages, r.handler.clone());
        if let Some(&(_, baseline)) = baselines.get(&key)
            && baseline > 0.0
        {
            r.scaling_efficiency = r.throughput_msg_per_sec / baseline;
        }
    }
}

fn print_table(report: &Report) {
    println!();
    println!("Backend: {}", report.backend);
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

// ── Entry points ────────────────────────────────────────────────────────────

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".parse().unwrap()),
        )
        .try_init();
}

fn spawn_ctrlc_watcher() -> CancellationToken {
    let cancel = CancellationToken::new();
    let clone = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        eprintln!("\ninterrupted, shutting down gracefully...");
        clone.cancel();
    });
    cancel
}

fn finalize_report(
    mut results: Vec<ScenarioResult>,
    failures: Vec<FailedResult>,
    backend_name: &str,
    output: OutputFormat,
) {
    compute_scaling(&mut results);
    let report = Report {
        backend: backend_name.to_string(),
        results,
        failures,
    };
    match output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&report).unwrap();
            println!("{json}");
        }
        OutputFormat::Table => {
            print_table(&report);
        }
    }
}

fn push_metrics(results: &mut Vec<ScenarioResult>, scenario: &Scenario, m: ScenarioMetrics) {
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
        scaling_efficiency: 0.0,
        peak_rss_mb: m.peak_rss_mb,
        cpu_pct: m.cpu_pct,
        duration_secs: m.duration_secs,
    });
}

/// Run every scenario against a coordinated-group backend (InMemory, Kafka,
/// NATS, RabbitMQ). Each binary supplies:
///
/// * `connect` — builds a fresh `Broker<B>`. Called once per scenario so the
///   broker-wide shutdown-token state tripped by `run_until_timeout` does not
///   bleed between scenarios.
/// * `make_cfg` — turns `(consumers, prefetch, concurrent)` into
///   `B::ConsumerGroupConfig`.
pub async fn run_all_scenarios<B, MkCfg, Connect, Fut>(
    hcfg: HarnessConfig<B>,
    connect: Connect,
    make_cfg: MkCfg,
) where
    B: Backend + HasCoordinatedGroups,
    MkCfg: Fn(u16, u16, bool) -> B::ConsumerGroupConfig,
    Connect: Fn() -> Fut,
    Fut: Future<Output = Broker<B>>,
{
    init_tracing();

    let cli = Cli::parse();
    let scenarios = build_scenarios(&cli);

    let cancel = spawn_ctrlc_watcher();

    eprintln!(
        "shove stress benchmarks — {}{}",
        hcfg.backend_name,
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

        match run_scenario_group(&hcfg, scenario, &cancel, &make_cfg, &connect).await {
            Ok(m) => push_metrics(&mut results, scenario, m),
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

    finalize_report(results, failures, hcfg.backend_name, cli.output);
}

/// Run every scenario against a supervisor-only backend (SQS). See
/// [`run_all_scenarios`] for the closure contract.
pub async fn run_supervisor_scenarios<B, MkOpts, Connect, Fut>(
    hcfg: HarnessConfig<B>,
    connect: Connect,
    make_opts: MkOpts,
) where
    B: Backend,
    MkOpts: Fn(u16, bool) -> ConsumerOptions<B>,
    Connect: Fn() -> Fut,
    Fut: Future<Output = Broker<B>>,
{
    init_tracing();

    let cli = Cli::parse();
    let scenarios = build_scenarios(&cli);

    let cancel = spawn_ctrlc_watcher();

    eprintln!(
        "shove stress benchmarks — {}{}",
        hcfg.backend_name,
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

        match run_scenario_supervisor(&hcfg, scenario, &cancel, &make_opts, &connect).await {
            Ok(m) => push_metrics(&mut results, scenario, m),
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

    finalize_report(results, failures, hcfg.backend_name, cli.output);
}
