//! Consumer-count overhead benchmark for shove.
//!
//! Measures the framework overhead as the number of consumers scales from
//! 128 to 4096.  Handlers are instant — the benchmark isolates framework
//! cost only.
//!
//! Metrics measured:
//!   - **Startup time**: `start_all()` → all consumers registered with broker.
//!   - **Shutdown time**: `shutdown_all()` wall-clock duration.
//!   - **Dispatch latency** (p50/p95/p99): publish → handler invocation, with
//!     consumers pre-started and a fixed message count so there is no
//!     queueing backlog.
//!   - **RSS per consumer**: incremental memory cost of each additional consumer.
//!   - **Idle CPU**: CPU usage with consumers running but no messages flowing.
//!
//! Uses the default prefetch mode (`run` with `prefetch_count=10`).
//!
//! Requires Docker. Run with:
//!
//!     cargo bench -q --features rabbitmq --bench consumer_overhead
//!     cargo bench -q --features rabbitmq --bench consumer_overhead -- --output json

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use shove::rabbitmq::*;
use shove::*;
use testcontainers::ImageExt;
use testcontainers::core::ExecCommand;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "consumer_overhead",
    about = "Benchmark framework overhead as consumer count increases (128..4096)"
)]
struct Cli {
    /// Output format for the final report.
    #[arg(long, default_value = "table")]
    output: OutputFormat,

    /// Number of iterations per consumer count (results are averaged).
    #[arg(long, default_value = "3")]
    iterations: u32,

    /// Fixed number of messages per dispatch-latency run.
    #[arg(long, default_value = "10000")]
    messages: u64,

    /// Seconds to sample idle CPU after consumers are up but before publishing.
    #[arg(long, default_value = "3")]
    idle_secs: u64,

    /// Custom consumer counts (comma-separated). Overrides the default sweep.
    #[arg(long, value_delimiter = ',')]
    consumers: Option<Vec<u16>>,

    /// Hidden flag accepted (and ignored) so `cargo bench` can pass `--bench`.
    #[arg(long, hide = true)]
    bench: bool,
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

// ── Results ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct RunResult {
    consumers: u16,
    iterations: u32,
    messages_per_run: u64,
    avg_startup_ms: f64,
    avg_shutdown_ms: f64,
    avg_dispatch_p50_ms: f64,
    avg_dispatch_p95_ms: f64,
    avg_dispatch_p99_ms: f64,
    rss_per_consumer_kb: f64,
    avg_idle_cpu_pct: f64,
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
        // Raise channel_max from the default 2047 to support up to 4096+ consumers,
        // each of which opens its own channel on the shared connection.
        let container = RabbitMq::default()
            .with_env_var(
                "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS",
                "-rabbit channel_max 8192",
            )
            .start()
            .await
            .unwrap();
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

    /// Poll the management API until the queue has at least `expected` consumers,
    /// or the timeout expires.
    async fn wait_for_consumers(&self, expected: u16, timeout: Duration) -> Result<(), String> {
        let http = reqwest::Client::new();
        let url = format!(
            "{}/api/queues/%2F/{}",
            self.mgmt_config.base_url,
            OverheadTopic::topology().queue()
        );
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            if let Ok(resp) = http
                .get(&url)
                .basic_auth(&self.mgmt_config.username, Some(&self.mgmt_config.password))
                .send()
                .await
                && let Ok(body) = resp.json::<serde_json::Value>().await
                && let Some(count) = body["consumers"].as_u64()
                && count >= expected as u64
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "timeout waiting for {expected} consumers to register"
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn publish_one(&self, msg: &BenchMsg) {
        self.publisher.publish::<OverheadTopic>(msg).await.unwrap();
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

    fn record(&self, latency_ns: u64) {
        let _ = self.tx.send(latency_ns);
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

// ── Resource helpers ────────────────────────────────────────────────────────

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

// ── Handler ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct BenchHandler {
    epoch: Instant,
    processed: Arc<AtomicU64>,
    recorder: Arc<LatencyRecorder>,
}

impl MessageHandler<OverheadTopic> for BenchHandler {
    async fn handle(&self, msg: BenchMsg, _meta: MessageMetadata) -> Outcome {
        // No simulated work — measures pure framework dispatch overhead.
        let received_at = self.epoch.elapsed().as_nanos() as u64;
        self.recorder
            .record(received_at.saturating_sub(msg.published_at_ns));

        self.processed.fetch_add(1, Ordering::Relaxed);
        Outcome::Ack
    }
}

// ── Single iteration ───────────────────────────────────────────────────────

struct IterationResult {
    startup_ms: f64,
    shutdown_ms: f64,
    dispatch_p50_ms: f64,
    dispatch_p95_ms: f64,
    dispatch_p99_ms: f64,
    idle_cpu_pct: f64,
}

/// Measure RSS per consumer in a dedicated run.
///
/// Process RSS doesn't shrink after consumers are torn down, so averaging
/// the delta across repeated iterations is meaningless — only the first
/// allocation is real. This function spins up consumers on a clean baseline,
/// snapshots the RSS delta, then tears everything down.
async fn measure_rss(broker: &Broker, consumers: u16) -> Result<f64, String> {
    broker.purge_queue().await;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));

    let rss_before = current_rss_bytes();

    let mut registry = ConsumerGroupRegistry::new(broker.client.clone());
    let pc = processed.clone();
    let rec = recorder.clone();
    registry
        .register::<OverheadTopic, BenchHandler>(
            ConsumerGroupConfig::new(consumers..=consumers),
            move || BenchHandler {
                epoch,
                processed: pc.clone(),
                recorder: rec.clone(),
            },
        )
        .await
        .map_err(|e| e.to_string())?;

    registry.start_all();
    broker
        .wait_for_consumers(consumers, Duration::from_secs(120))
        .await?;

    let rss_after = current_rss_bytes();
    let rss_delta_kb = rss_after.saturating_sub(rss_before) as f64 / 1024.0;

    registry.shutdown_all().await;

    Ok(rss_delta_kb / consumers as f64)
}

async fn run_once(
    broker: &Broker,
    consumers: u16,
    messages: u64,
    idle_secs: u64,
    cancel: &CancellationToken,
) -> Result<IterationResult, String> {
    broker.purge_queue().await;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));

    // ── 1. Startup: measure time to register all consumers with the broker ──
    let mut registry = ConsumerGroupRegistry::new(broker.client.clone());
    let pc = processed.clone();
    let rec = recorder.clone();
    registry
        .register::<OverheadTopic, BenchHandler>(
            ConsumerGroupConfig::new(consumers..=consumers),
            move || BenchHandler {
                epoch,
                processed: pc.clone(),
                recorder: rec.clone(),
            },
        )
        .await
        .map_err(|e| e.to_string())?;

    let startup_start = Instant::now();
    registry.start_all();
    broker
        .wait_for_consumers(consumers, Duration::from_secs(120))
        .await?;
    let startup_ms = startup_start.elapsed().as_secs_f64() * 1000.0;

    // ── 2. Idle CPU: consumers up, no messages flowing ──────────────────────
    let idle_cpu_before = current_cpu_secs();
    let idle_wall_start = Instant::now();
    tokio::time::sleep(Duration::from_secs(idle_secs)).await;
    let idle_wall_secs = idle_wall_start.elapsed().as_secs_f64();
    let idle_cpu_delta = current_cpu_secs() - idle_cpu_before;
    let idle_cpu_pct = if idle_wall_secs > 0.0 {
        (idle_cpu_delta / idle_wall_secs) * 100.0
    } else {
        0.0
    };

    // ── 3. Dispatch latency: publish into running consumers ─────────────────
    // Stamp each message at actual publish time so latency reflects real
    // broker-to-handler delay, not time sitting in a pre-built Vec.
    for id in 0..messages {
        let msg = BenchMsg {
            id,
            published_at_ns: epoch.elapsed().as_nanos() as u64,
        };
        broker.publish_one(&msg).await;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        if processed.load(Ordering::Relaxed) >= messages {
            break;
        }
        if cancel.is_cancelled() {
            registry.shutdown_all().await;
            return Err("interrupted".to_string());
        }
        if tokio::time::Instant::now() >= deadline {
            let done = processed.load(Ordering::Relaxed);
            registry.shutdown_all().await;
            return Err(format!(
                "timeout: processed {done} / {messages} ({:.0}%)",
                done as f64 / messages as f64 * 100.0
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let (p50, p95, p99) = recorder.compute_percentiles().await;

    // ── 4. Shutdown time ────────────────────────────────────────────────────
    let shutdown_start = Instant::now();
    registry.shutdown_all().await;
    let shutdown_ms = shutdown_start.elapsed().as_secs_f64() * 1000.0;

    Ok(IterationResult {
        startup_ms,
        shutdown_ms,
        dispatch_p50_ms: p50,
        dispatch_p95_ms: p95,
        dispatch_p99_ms: p99,
        idle_cpu_pct,
    })
}

// ── Main ────────────────────────────────────────────────────────────────────

const DEFAULT_CONSUMER_COUNTS: &[u16] = &[128, 256, 512, 1024, 2048, 4096];

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let consumer_counts = cli
        .consumers
        .unwrap_or_else(|| DEFAULT_CONSUMER_COUNTS.to_vec());
    let iterations = cli.iterations;
    let messages = cli.messages;
    let idle_secs = cli.idle_secs;

    let broker = Broker::start().await;

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        eprintln!("\ninterrupted, shutting down gracefully...");
        cancel_clone.cancel();
    });

    eprintln!(
        "consumer overhead benchmark | {messages} msgs | {}x iterations | idle={}s | prefetch=10",
        iterations, idle_secs,
    );
    eprintln!("consumer counts: {:?}\n", consumer_counts);

    let mut results: Vec<RunResult> = Vec::new();

    for &count in &consumer_counts {
        if cancel.is_cancelled() {
            eprintln!("skipping remaining counts");
            break;
        }

        eprintln!("  {}c | measuring RSS ...", count);
        let rss_per_consumer_kb = match measure_rss(&broker, count).await {
            Ok(v) => {
                eprintln!("    RSS: {:.1} KB/consumer", v);
                v
            }
            Err(e) => {
                eprintln!("    RSS measurement FAILED: {e}");
                continue;
            }
        };

        eprintln!("  {}c x {} iterations ...", count, iterations);

        let mut startups = Vec::new();
        let mut shutdowns = Vec::new();
        let mut p50s = Vec::new();
        let mut p95s = Vec::new();
        let mut p99s = Vec::new();
        let mut idle_cpus = Vec::new();
        let mut failed = false;

        for iter in 0..iterations {
            match run_once(&broker, count, messages, idle_secs, &cancel).await {
                Ok(r) => {
                    eprintln!(
                        "    [{}/{}] startup={:.0}ms shutdown={:.0}ms | p50={:.1}ms p95={:.1}ms p99={:.1}ms | idle_cpu={:.1}%",
                        iter + 1,
                        iterations,
                        r.startup_ms,
                        r.shutdown_ms,
                        r.dispatch_p50_ms,
                        r.dispatch_p95_ms,
                        r.dispatch_p99_ms,
                        r.idle_cpu_pct,
                    );
                    startups.push(r.startup_ms);
                    shutdowns.push(r.shutdown_ms);
                    p50s.push(r.dispatch_p50_ms);
                    p95s.push(r.dispatch_p95_ms);
                    p99s.push(r.dispatch_p99_ms);
                    idle_cpus.push(r.idle_cpu_pct);
                }
                Err(e) => {
                    eprintln!("    [{}/{}] FAILED: {e}", iter + 1, iterations);
                    failed = true;
                    break;
                }
            }
        }

        if failed || startups.is_empty() {
            continue;
        }

        let avg = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;

        results.push(RunResult {
            consumers: count,
            iterations,
            messages_per_run: messages,
            avg_startup_ms: avg(&startups),
            avg_shutdown_ms: avg(&shutdowns),
            avg_dispatch_p50_ms: avg(&p50s),
            avg_dispatch_p95_ms: avg(&p95s),
            avg_dispatch_p99_ms: avg(&p99s),
            rss_per_consumer_kb,
            avg_idle_cpu_pct: avg(&idle_cpus),
        });
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
        "{:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "CONSUMERS", "STARTUP", "SHUTDOWN", "p50", "p95", "p99", "KB/C", "IDLE CPU"
    );
    println!("{}", "-".repeat(90));
    for r in &report.results {
        println!(
            "{:>10} {:>9.0}ms {:>9.0}ms {:>9.1}ms {:>9.1}ms {:>9.1}ms {:>9.1} {:>8.1}%",
            r.consumers,
            r.avg_startup_ms,
            r.avg_shutdown_ms,
            r.avg_dispatch_p50_ms,
            r.avg_dispatch_p95_ms,
            r.avg_dispatch_p99_ms,
            r.rss_per_consumer_kb,
            r.avg_idle_cpu_pct,
        );
    }
    println!();
    println!("STARTUP   = time from start_all() until all consumers registered with broker.");
    println!("SHUTDOWN  = time for shutdown_all() to complete.");
    println!(
        "p50/95/99 = publish-to-handler dispatch latency (consumers pre-started, {} msgs).",
        report.results.first().map_or(0, |r| r.messages_per_run)
    );
    println!("KB/C      = incremental RSS per consumer (total RSS delta / consumer count).");
    println!("IDLE CPU  = CPU%% with consumers running, no messages flowing.");
}
