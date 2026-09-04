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
//!   - **RSS per consumer** — marginal memory cost of one consumer, measured
//!     by differencing two *isolated child processes*.
//!   - **Idle CPU** — CPU usage with consumers running but no messages flowing,
//!     measured in those same child processes.
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

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// Payload used by the dispatch warm-up. The warm-up exists to bring every
/// consumer instance up, so it carries the cheapest payload in the set rather
/// than the round's own — its cost is pure overhead outside the timer.
const WARMUP_PAYLOAD_BYTES: usize = 64;

/// How long the resource probe samples idle CPU for, in each child process.
const IDLE_SAMPLE: Duration = Duration::from_secs(2);

/// Isolated processes sampled per probe shape.
///
/// One process per shape cannot support the subtractions this table is made
/// of. RSS is reported in 4 KiB pages, and process start-up and allocator
/// jitter move it a page either way, so a single-sample difference the size of
/// a page says nothing — sampled once per shape, this probe printed
/// `GROUP FIXED: -20.0 KB`, a negative fixed cost. Sampling each shape in
/// several processes and differencing medians gives a figure that repeats, and
/// the spread across those processes gives the floor below which a difference
/// must not be read as a cost at all. Odd, so the median is an observed sample
/// rather than an interpolation between two. Each sample costs one process and
/// one [`IDLE_SAMPLE`], so this multiplies the probe's wall-clock by itself.
///
/// Five is also the smallest count that lets [`inner_range`] do its job: it
/// drops the highest and the lowest sample before measuring how far a shape
/// moved, so one aberrant process cannot set the bar for every comparison that
/// shape takes part in, and three samples have to survive that trim for the
/// range left over to mean anything.
const PROBE_REPEATS: usize = 5;

// Both properties are load-bearing above: even counts have no observed median,
// and at four or fewer, [`inner_range`] is left with one sample or none — a
// range of zero however far the processes actually moved, which would mark
// every difference resolved.
const _: () = assert!(PROBE_REPEATS >= 5 && PROBE_REPEATS % 2 == 1);

/// Ceiling on the wait for a dispatch round to complete. Nothing should come
/// close; it exists so a lost delivery fails the run rather than hanging it.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(60);

/// Ceiling on the warm-up's convergence to a fully-polled group. Which
/// consumer a delivery lands on is the backend's to decide, so the warm-up
/// republishes for the instances still missing; this bounds that loop so a
/// group that never converges fails the run with a count instead of publishing
/// forever.
const READINESS_TIMEOUT: Duration = Duration::from_secs(30);

/// Environment variable that turns this executable into an isolated RSS-probe
/// child for the [`Probe`] shape it names. Set only by [`probe_child`]; a
/// normal `cargo bench` invocation never has it.
const RSS_CHILD_ENV: &str = "SHOVE_BENCH_RSS_CONSUMERS";

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

/// State one consumer group's handler instances share: the delivery tally, the
/// per-instance readiness tally, and the two milestone gates the driver waits
/// on.
///
/// `polled_instances` is what makes the dispatch warm-up a real readiness
/// barrier. Each spawned consumer gets its *own* [`BenchHandler`] (see
/// [`registry`]) carrying a private `polled` flag; the first delivery an
/// instance handles flips that flag and increments this counter exactly once.
/// So `polled_instances == instances` means every consumer task in the group
/// has been polled and has run a handler to completion — not merely that some
/// subset drained the burst.
///
/// The two message gates are armed by the driver rather than fixed at
/// construction, because the warm-up publishes an amount it discovers as it
/// goes. Every wait re-checks the counter it is waiting on, so a `notify_one`
/// permit left behind by a milestone that was already met costs one extra loop
/// turn rather than releasing a wait early. Each gate is disarmed again the
/// moment its wait returns, so it is armed only while someone is parked on it —
/// otherwise a met milestone would keep firing `notify_one` on every later
/// delivery, which is exactly the traffic the timed round must not carry.
struct Gates {
    /// Deliveries handled by every instance in the group, together.
    seen: AtomicU64,
    /// Instances that have handled at least one delivery.
    polled_instances: AtomicU64,
    /// Consumer instances the group was configured with. `u64::MAX` on a
    /// group whose readiness is never waited on.
    instances: u64,
    /// `seen` value that completes the warm-up drain. `u64::MAX` when unarmed.
    drain_target: AtomicU64,
    drained: Notify,
    /// `seen` value that completes the timed round. `u64::MAX` when unarmed.
    round_target: AtomicU64,
    done: Notify,
}

impl Gates {
    /// Gates for a group of `instances` consumers, both milestones unarmed.
    fn for_group(instances: u64) -> Arc<Self> {
        Arc::new(Self {
            seen: AtomicU64::new(0),
            polled_instances: AtomicU64::new(0),
            instances,
            drain_target: AtomicU64::new(u64::MAX),
            drained: Notify::new(),
            round_target: AtomicU64::new(u64::MAX),
            done: Notify::new(),
        })
    }

    /// Gates for a group nothing publishes to. Every milestone is `u64::MAX`,
    /// which no counter reaches, so no gate ever fires and none may be waited
    /// on.
    fn inert() -> Arc<Self> {
        Self::for_group(u64::MAX)
    }

    /// How many consumer instances have not yet handled a delivery.
    fn unpolled_instances(&self) -> u64 {
        self.instances
            .saturating_sub(self.polled_instances.load(Ordering::SeqCst))
    }

    /// Waits until `published` deliveries have been handled, then disarms.
    ///
    /// Disarming is part of the measurement, not tidiness. `seen` is cumulative
    /// and never reset, so a target left behind stays satisfied for every later
    /// delivery: each message of the timed round would then fire `notify_one`
    /// on this shared `Notify`, whose no-waiter path is still a
    /// compare-exchange on one cache line. With up to 256 consumer tasks that
    /// is a contended atomic per message, added to the very region that exists
    /// to isolate shove's own dispatch cost.
    async fn drain_to(&self, published: u64) {
        self.drain_target.store(published, Ordering::SeqCst);
        self.wait_for(&self.drained, published, "warm-up").await;
        self.drain_target.store(u64::MAX, Ordering::SeqCst);
    }

    /// Arms the timed round at `count` deliveries beyond what has been handled
    /// so far.
    ///
    /// Callers must have drained the warm-up first: `seen` is read here, so a
    /// delivery still in flight would arm the gate past the end of the round.
    fn arm_round(&self, count: u64) {
        let target = self.seen.load(Ordering::SeqCst).saturating_add(count);
        self.round_target.store(target, Ordering::SeqCst);
    }

    /// Waits until the armed round has been fully handled, then disarms — same
    /// rule as [`Gates::drain_to`]: a gate is armed only while a wait is parked
    /// on it, so no delivery outside a wait can reach a `notify_one`.
    async fn completed(&self) {
        let target = self.round_target.load(Ordering::SeqCst);
        self.wait_for(&self.done, target, "dispatch").await;
        self.round_target.store(u64::MAX, Ordering::SeqCst);
    }

    /// Waits for `seen` to reach `target`, re-checking the counter on every
    /// wake so the wait cannot end early on a stale permit.
    ///
    /// Bounded so a lost delivery fails the run instead of hanging it: an
    /// unbounded wait here is `cargo bench` producing no further output until
    /// CI kills the job, which reads as "slow" rather than "broken".
    async fn wait_for(&self, gate: &Notify, target: u64, phase: &str) {
        while self.seen.load(Ordering::SeqCst) < target {
            if timeout(COMPLETION_TIMEOUT, gate.notified()).await.is_err() {
                panic!(
                    "{phase}: handlers saw {} of {target} messages in {COMPLETION_TIMEOUT:?}: \
                     a delivery was lost",
                    self.seen.load(Ordering::SeqCst),
                );
            }
        }
    }
}

/// One consumer instance's handler. Does no work of its own, so the groups
/// below report framework cost only.
///
/// `polled` is per instance and deliberately not shared: it is the difference
/// between "the group is draining" and "this consumer task has run".
struct BenchHandler {
    gates: Arc<Gates>,
    polled: AtomicBool,
}

impl BenchHandler {
    fn new(gates: &Arc<Gates>) -> Self {
        Self {
            gates: Arc::clone(gates),
            polled: AtomicBool::new(false),
        }
    }
}

impl MessageHandler<OverheadTopic> for BenchHandler {
    type Context = ();

    async fn handle(&self, _msg: BenchMsg, _meta: MessageMetadata, _ctx: &()) -> Outcome {
        if !self.polled.swap(true, Ordering::SeqCst) {
            // Wrapping is unreachable: one increment per instance, and the
            // group is bounded by `u16::MAX` consumers.
            self.gates.polled_instances.fetch_add(1, Ordering::SeqCst);
        }

        let seen = self
            .gates
            .seen
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        if seen >= self.gates.drain_target.load(Ordering::SeqCst) {
            self.gates.drained.notify_one();
        }
        if seen >= self.gates.round_target.load(Ordering::SeqCst) {
            self.gates.done.notify_one();
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
///
/// The factory mints a *fresh* handler per consumer — the group calls it once
/// per spawned instance — so each instance carries its own `polled` flag while
/// sharing one set of [`Gates`]. A factory that cloned one handler would make
/// readiness a single shared counter, which cannot distinguish "every consumer
/// ran" from "one consumer ran N times".
async fn registry(
    client: &InMemoryBroker,
    consumers: u16,
    gates: &Arc<Gates>,
) -> InMemoryConsumerGroupRegistry {
    let gates = Arc::clone(gates);
    let mut registry = InMemoryConsumerGroupRegistry::new(client.clone());
    registry
        .register::<OverheadTopic, BenchHandler>(
            InMemoryConsumerGroupConfig::new(consumers..=consumers).with_prefetch_count(10),
            move || BenchHandler::new(&gates),
            (),
        )
        .await
        .expect("register consumer group");
    registry
}

/// Publishes until **every** consumer instance in the group has handled at
/// least one delivery, then waits for that traffic to drain.
///
/// Which instance a delivery lands on is the backend's to decide, so one burst
/// of "a message per consumer" does not put every consumer to work: a
/// fast-scheduled subset can drain the whole burst while other spawned tasks
/// have not been polled once. This republishes for exactly the instances still
/// missing, and waits for each round to be fully handled before re-counting,
/// so it converges on the readiness tally itself rather than on a burst size
/// assumed to be sufficient.
///
/// It returns only when `polled_instances == instances` and nothing is in
/// flight, which is the precondition [`Gates::arm_round`] needs. It panics
/// with the tally if the group has not converged inside [`READINESS_TIMEOUT`].
async fn warm_up(publisher: &Publisher<InMemory>, body: &str, gates: &Gates) {
    let started = Instant::now();
    let mut published: u64 = 0;

    loop {
        let missing = gates.unpolled_instances();
        if missing == 0 {
            return;
        }
        if started.elapsed() >= READINESS_TIMEOUT {
            panic!(
                "warm-up: {} of {} consumer instances handled a delivery in \
                 {READINESS_TIMEOUT:?} ({published} warm-up messages published)",
                gates.polled_instances.load(Ordering::SeqCst),
                gates.instances,
            );
        }

        publish_n(publisher, body, missing).await;
        published = published.saturating_add(missing);
        gates.drain_to(published).await;
    }
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
            return pages.saturating_mul(page_bytes());
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

/// What one isolated probe process is asked to build.
#[derive(Clone, Copy)]
enum Probe {
    /// Runtime, broker and declared topology only — no registry, no group.
    /// Every other shape is measured against this one.
    Bare,
    /// One registry and one group holding `n` consumers.
    ///
    /// `Group(0)` is a real shape, not a degenerate one: the group is
    /// registered and started, and spawns no consumer task because
    /// `min_consumers` is zero. It prices the registry, the group record, the
    /// tokens and the spawner *directly*, instead of extrapolating them back
    /// out of a row that also contains consumers.
    Group(u16),
}

impl Probe {
    /// How the parent hands this shape to the child, via [`RSS_CHILD_ENV`].
    fn spec(self) -> String {
        match self {
            Probe::Bare => "bare".to_owned(),
            Probe::Group(n) => format!("group:{n}"),
        }
    }

    fn parse(spec: &str) -> Option<Self> {
        match spec {
            "bare" => Some(Probe::Bare),
            _ => spec
                .strip_prefix("group:")
                .and_then(|n| n.parse::<u16>().ok())
                .map(Probe::Group),
        }
    }

    /// Consumer tasks this shape spawns. The bare process has no group at all,
    /// which is a different thing from a group of none — the table shows both.
    fn consumers(self) -> u16 {
        match self {
            Probe::Bare => 0,
            Probe::Group(n) => n,
        }
    }
}

/// What one isolated probe process reported.
struct ProbeSample {
    rss_kb: f64,
    idle_cpu_pct: f64,
}

/// What one probe shape reported across [`PROBE_REPEATS`] processes.
struct ProbeStats {
    probe: Probe,
    /// Median process RSS — the median rather than the mean, so one process
    /// that happened to map an extra arena moves the row by nothing.
    rss_kb: f64,
    /// How far the identical measurement moved when nothing about it changed:
    /// the range of this shape's own processes **after the highest and the
    /// lowest are dropped** ([`inner_range`]). Trimmed rather than `max - min`
    /// because this number is a floor other figures are judged against, and a
    /// plain range is set by its single worst sample — one aberrant process in
    /// the bare shape would otherwise raise the bar under every `OVER BASE`
    /// cell and under `GROUP FIXED` at once.
    rss_spread_kb: f64,
    /// Median idle CPU.
    idle_cpu_pct: f64,
}

/// Sample one shape in [`PROBE_REPEATS`] separate processes.
fn probe_shape(probe: Probe) -> ProbeStats {
    let samples: Vec<ProbeSample> = (0..PROBE_REPEATS).map(|_| probe_child(probe)).collect();

    let mut rss: Vec<f64> = samples.iter().map(|s| s.rss_kb).collect();
    let mut idle_cpu: Vec<f64> = samples.iter().map(|s| s.idle_cpu_pct).collect();
    let rss_kb = median(&mut rss);

    ProbeStats {
        probe,
        rss_kb,
        // `median` left `rss` sorted, which is what `inner_range` reads.
        rss_spread_kb: inner_range(&rss),
        idle_cpu_pct: median(&mut idle_cpu),
    }
}

/// Median of `values`. Sorts in place, so a caller that wants the extremes can
/// read them off the ends afterwards.
fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    // The const assert on `PROBE_REPEATS` is what makes the index land.
    values.get(values.len() / 2).copied().unwrap_or(f64::NAN)
}

/// How far `sorted` moved, ignoring its single highest and single lowest
/// value: the range of what is left. With [`PROBE_REPEATS`] at 5 that is the
/// range of the middle three samples.
///
/// A plain `max - min` is decided by one sample at each end, and this number
/// is a **floor** — every difference it takes part in has to clear it. One
/// process that mapped an extra arena would set the bar for all of them; on
/// the bare shape, whose spread floors every `OVER BASE` cell and
/// `GROUP FIXED` alike, that one sample suppresses the whole baseline column
/// of the table, however tightly the other four processes agreed. Trimming
/// each end is what makes a shape's floor a property of its processes rather
/// than of its worst one. It buys tolerance for one aberrant process per
/// shape, not two: this is a range over samples, not a robust estimator, and
/// [`PROBE_REPEATS`] is what bounds how much of it can be thrown away.
fn inner_range(sorted: &[f64]) -> f64 {
    // Fewer than three samples leaves nothing between the two dropped ends;
    // NAN then marks every difference unresolved, which is the safe direction.
    let highest_inner = sorted.len().saturating_sub(2);
    match (sorted.get(1), sorted.get(highest_inner)) {
        (Some(low), Some(high)) if highest_inner >= 1 => high - low,
        _ => f64::NAN,
    }
}

/// The resolution floor for a difference between two shapes: the two shapes'
/// own spreads **added**.
///
/// Local to the pair on purpose. A single floor taken across the whole table —
/// the widest spread anywhere on it — lets one noisy shape set the bar for
/// every other comparison, including comparisons it takes no part in. One
/// anomalous `Group(256)` process would then raise the threshold that
/// `GROUP FIXED` (bare against the zero-consumer group) has to clear, and
/// suppress a real difference between two shapes that were both perfectly
/// stable. A difference can only be obscured by the noise on its own two
/// sides, so that is the only noise it is judged against.
///
/// Added rather than `max`, because the quantity being judged is a
/// *difference* between two shapes sampled in **independent** processes, and
/// each side carries its own jitter into it. Take two shapes whose samples
/// each span a page: `A` over `[1000, 1020]` with median 1000, `B` over
/// `[1010, 1030]` with median 1030. The medians differ by 30 and each spread
/// is 20, so `max` calls 30 resolved — while `B`'s lowest process actually
/// came in *below* `A`'s highest, a difference of -10 in the opposite
/// direction. Two sample sets that overlap have not shown this probe a
/// difference at all. Their sum, 40, is the width of the difference's own
/// admissible range (`B_max - A_min` down to `B_min - A_max`).
///
/// Clearing that width **implies** the two shapes did not overlap, but it is
/// the stricter of the two tests, not the same one. Since a median sits inside
/// its own trimmed samples, `delta > spread_a + spread_b` forces
/// `B_min > A_max` across the trimmed sets; the converse fails. For
/// `A = [970, 980, 1000, 1000, 1010]` against `B = [1020, 1030, 1030, 1050,
/// 1060]`, every sample of `A` is below every sample of `B`, yet each trimmed
/// spread is 20 and the medians differ by only 30, so the difference still
/// prints `~30.0`. Summing each side's whole spread, rather than only the part
/// of it facing the other shape, is what costs that resolution.
///
/// That error is the affordable one, and the direction is chosen. An extra `~`
/// leaves the observed number on the page to be judged; the opposite slip
/// prints an unresolved difference as a settled cost. A floor built from
/// `max()` did exactly that here before, so this bound stays conservative.
///
/// Never below one page either. Trimming can leave a shape's spread at zero —
/// three of its five processes reporting the identical RSS is ordinary, since
/// RSS moves in whole pages — and a floor of zero would call a single page of
/// movement a resolved cost. One page is the unit this instrument reports in,
/// so it is the smallest difference it can be asked to stand behind.
fn pair_floor(a: &ProbeStats, b: &ProbeStats) -> f64 {
    // The const assert on `PROBE_REPEATS` keeps both spreads real numbers here;
    // `inner_range` only yields NAN below three samples.
    (a.rss_spread_kb + b.rss_spread_kb).max(rss_quantum_kb())
}

/// One page in KB: the step process RSS moves in, and so the finest difference
/// this probe could observe at all.
fn rss_quantum_kb() -> f64 {
    page_bytes() as f64 / 1024.0
}

/// The kernel's page size, asked for rather than assumed.
///
/// `/proc/self/statm` counts pages, not bytes, and 4 KiB is not universal — an
/// arm64 kernel can be built with 16 KiB or 64 KiB pages. Reading the size back
/// keeps the RSS figures, and the resolution floor built out of them, in the
/// same units as the kernel that produced them.
#[cfg(unix)]
fn page_bytes() -> u64 {
    // SAFETY: `sysconf` reads a numeric limit by id and returns a `long`. No
    // pointers cross the boundary. A negative return (the id is unsupported)
    // fails the conversion and falls back to the near-universal 4 KiB.
    let reported = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(reported).unwrap_or(4096)
}

#[cfg(not(unix))]
fn page_bytes() -> u64 {
    4096
}

/// A difference between two shapes, printed with a `~` when it does not clear
/// that pair's floor.
///
/// The marked form still prints the observed difference, because that is the
/// measurement; the `~` says this probe cannot tell it apart from zero. It is
/// deliberately not `<floor`. The floor is built out of the observed spread of
/// [`PROBE_REPEATS`] samples, not a confidence interval, so it supports no
/// claim about where the true difference lies — and at `delta == floor`, which
/// is unremarkable when both are whole 4 KiB pages, `<floor` prints a strict
/// inequality that the probe's own numbers contradict.
fn resolved_delta(delta_kb: f64, floor_kb: f64) -> String {
    if delta_kb.abs() > floor_kb {
        format!("{delta_kb:.1}")
    } else {
        format!("~{delta_kb:.1}")
    }
}

/// The same difference spread over the consumers a row added, marked the same
/// way: an unresolved row-to-row difference gives an unresolved per-consumer
/// figure, and dividing it by the consumers added does not make it resolved.
fn resolved_per_consumer(delta_kb: f64, added: f64, floor_kb: f64) -> String {
    let per_consumer_kb = delta_kb / added;
    if delta_kb.abs() > floor_kb {
        format!("{per_consumer_kb:.1}")
    } else {
        format!("~{per_consumer_kb:.1}")
    }
}

/// Sample RSS-per-consumer and idle CPU **in isolated processes**,
/// [`PROBE_REPEATS`] per consumer count, and print a table to stderr.
///
/// # Why a child process per row
///
/// An in-process sweep cannot separate what a consumer costs from what its
/// surroundings cost. Registering a group allocates a registry, a group
/// record, tokens and a spawner whose cost does not scale with the consumer
/// count, so any row that builds a group folds that fixed cost into its
/// per-consumer figure — the more so the smaller the row. Tearing rows down
/// between measurements is worse still: process RSS does not shrink on
/// teardown, so a later row is served out of pages an earlier row freed and
/// understates by however much the allocator had already mapped.
///
/// Each row here is measured in a freshly `exec`'d copy of this executable
/// that builds exactly one broker, one registry and one group of `n`
/// consumers, samples, and exits. That gives two things no in-process probe
/// can:
///
/// - **`KB/CONSUMER` is a difference between two processes** whose fixed costs
///   are identical — same binary, same runtime, same one registry and one
///   group — so the registry/group overhead cancels instead of being divided
///   into the consumer count.
/// - **No row inherits another row's allocator state.** Each process starts
///   with a fresh arena, so nothing is served out of slack a previous
///   measurement mapped.
///
/// That cancellation covers **every** row including the first, because the
/// sweep starts from a [`Probe::Group(0)`] process: one registry and one group
/// that spawns no consumer. So the 1-consumer marginal is `row1 - group0`, one
/// consumer against an otherwise identical process, not `row1 - bare` with the
/// whole group cost folded into it.
///
/// `GROUP FIXED` is therefore measured rather than estimated — the
/// zero-consumer group's growth over the bare process, with no assumption that
/// per-consumer cost stays linear down to `n = 1`. `RSS KB` is total process
/// RSS; `OVER BASE` subtracts the bare process, which carries the runtime, the
/// broker and the declared topology but no registry and no group.
///
/// # Why several processes per shape
///
/// A process-to-process difference is only as good as the reproducibility of
/// each side, and one sample per side has none to show. RSS moves by whole
/// 4 KiB pages on start-up jitter alone, so two single samples can differ by a
/// page for no reason — sampled that way this probe printed
/// `GROUP FIXED: -20.0 KB`, a fixed cost with a negative sign, which is jitter
/// wearing a number's clothes.
///
/// Every shape is therefore sampled in [`PROBE_REPEATS`] processes and the
/// table reports each row's **median** and its own **spread** — how far that
/// shape's processes moved once the highest and the lowest are dropped
/// ([`inner_range`]), so a shape's floor is not decided by its one worst
/// process. That matters most on the bare shape, which is a side of every
/// `OVER BASE` cell and of `GROUP FIXED`: untrimmed, one aberrant bare process
/// raises the bar under the entire baseline column at once.
///
/// Each difference is then judged against **its own** floor — [`pair_floor`],
/// the two shapes' spreads added — rather than against one floor shared by the
/// whole table. Sharing one would let a single noisy shape raise the bar for
/// every unrelated comparison; adding the pair's two is what makes the floor
/// the uncertainty of the *difference*, which carries the jitter of both
/// independently sampled sides rather than of the noisier one. That sum is
/// held to a minimum of one page, the unit RSS moves in, so a trim that leaves
/// both sides at zero spread cannot turn a single page of movement into a
/// resolved cost.
///
/// A difference that does not clear its floor is printed with a leading `~`, in `OVER BASE`, in
/// `KB/CONSUMER` and in `GROUP FIXED` alike: the value observed, flagged as
/// indistinguishable from zero at this probe's resolution. It is not reported
/// as a bound on the true difference — [`PROBE_REPEATS`] samples support no
/// such claim. So a reader can tell a cost from noise without re-running
/// anything, and a regression has to exceed the jitter on the two shapes it
/// sits between before this probe will claim one.
///
/// The measured consumers are idle — started, with no traffic — which is what
/// `IDLE CPU` reports and what makes the RSS figure a resting cost rather than
/// a working-set peak.
fn resource_probe() {
    // Listing benchmark ids must not fork the process 5 times.
    if std::env::args().any(|arg| arg == "--list") {
        return;
    }

    let bare = probe_shape(Probe::Bare);
    let empty_group = probe_shape(Probe::Group(0));
    let rows: Vec<ProbeStats> = CONSUMER_COUNTS
        .iter()
        .map(|n| probe_shape(Probe::Group(*n)))
        .collect();

    eprintln!();
    eprintln!("consumer resource probe (not a criterion measurement)");
    eprintln!(
        "each row is the median of {PROBE_REPEATS} separate processes, each holding exactly one"
    );
    eprintln!("registry and one group, so KB/CONSUMER is a process-to-process difference with the");
    eprintln!("group cost cancelled — including row 1, which is differenced against the");
    eprintln!("zero-consumer group below it.");
    eprintln!("SPREAD is how far a row moved when re-measured, over its own processes with the");
    eprintln!("highest and lowest dropped, so one aberrant process cannot set a row's bar. Every");
    eprintln!("difference is judged against the SUM of the two SPREADs it is taken between — its");
    eprintln!(
        "own floor, carrying the jitter of both independently sampled sides, so a noisy shape"
    );
    eprintln!(
        "cannot raise the bar for a comparison it is not part of. That floor never drops below"
    );
    eprintln!(
        "one page ({:.1} KB), the unit RSS moves in. A difference that does not clear it prints",
        rss_quantum_kb()
    );
    eprintln!("with a leading `~`: the value observed, flagged");
    eprintln!(
        "as indistinguishable from zero here. `~` is not an upper bound — {PROBE_REPEATS} samples \
         give none."
    );
    eprintln!(
        "bare process (runtime + broker + topology, no registry, no group): {:.1} KB RSS \
         (spread {:.1} KB)",
        bare.rss_kb, bare.rss_spread_kb,
    );
    eprintln!(
        "{:>10} {:>10} {:>8} {:>11} {:>14} {:>10}",
        "CONSUMERS", "RSS KB", "SPREAD", "OVER BASE", "KB/CONSUMER", "IDLE CPU"
    );
    eprintln!("{}", "-".repeat(69));

    // The zero-consumer group leads the sweep and is what every later row is
    // differenced from. It spawns no consumer, so it has no KB/CONSUMER of its
    // own — the dash is the point, not a missing number.
    eprintln!(
        "{:>10} {:>10.1} {:>8.1} {:>11} {:>14} {:>9.1}%",
        empty_group.probe.consumers(),
        empty_group.rss_kb,
        empty_group.rss_spread_kb,
        resolved_delta(
            empty_group.rss_kb - bare.rss_kb,
            pair_floor(&empty_group, &bare),
        ),
        "-",
        empty_group.idle_cpu_pct,
    );

    let mut previous = &empty_group;
    for row in &rows {
        let added = f64::from(
            row.probe
                .consumers()
                .saturating_sub(previous.probe.consumers()),
        );
        let marginal = if added > 0.0 {
            resolved_per_consumer(
                row.rss_kb - previous.rss_kb,
                added,
                pair_floor(row, previous),
            )
        } else {
            "-".to_owned()
        };
        eprintln!(
            "{:>10} {:>10.1} {:>8.1} {:>11} {:>14} {:>9.1}%",
            row.probe.consumers(),
            row.rss_kb,
            row.rss_spread_kb,
            resolved_delta(row.rss_kb - bare.rss_kb, pair_floor(row, &bare)),
            marginal,
            row.idle_cpu_pct,
        );
        previous = row;
    }

    // The group's own fixed cost, measured rather than extrapolated: what a
    // registry and a started group cost with nothing spawned in them.
    eprintln!(
        "GROUP FIXED: {} KB — registry + group + tokens + spawner, measured as the \
         zero-consumer group's growth over the bare process, each the median of \
         {PROBE_REPEATS} processes",
        resolved_delta(
            empty_group.rss_kb - bare.rss_kb,
            pair_floor(&empty_group, &bare),
        ),
    );
    eprintln!();
}

/// Run one isolated probe process and read back its sample.
///
/// The child's stderr is captured rather than inherited. Criterion greets
/// every start-up with a gnuplot notice, so inheriting would print one notice
/// per consumer count into the very table these children are spawned to fill.
/// Capturing does not discard it: every failure path below reports the child's
/// stderr, so a probe that dies still says why.
fn probe_child(probe: Probe) -> ProbeSample {
    let spec = probe.spec();
    let exe = std::env::current_exe().expect("locate this bench executable for the RSS probe");
    let output = Command::new(exe)
        .env(RSS_CHILD_ENV, &spec)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap_or_else(|e| panic!("spawn RSS probe child `{spec}`: {e}"));

    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        panic!(
            "RSS probe child `{spec}` exited with {}; stderr: {}",
            output.status,
            stderr.trim(),
        );
    }

    let line = String::from_utf8_lossy(&output.stdout);
    let mut fields = line.split_whitespace();
    let parsed = fields
        .next()
        .and_then(|rss| rss.parse::<u64>().ok())
        .zip(fields.next().and_then(|cpu| cpu.parse::<f64>().ok()));
    let Some((rss_bytes, idle_cpu_pct)) = parsed else {
        panic!(
            "RSS probe child `{spec}` printed {line:?}, expected `<rss> <cpu>`; stderr: {}",
            stderr.trim(),
        );
    };

    ProbeSample {
        rss_kb: rss_bytes as f64 / 1024.0,
        idle_cpu_pct,
    }
}

/// The isolated probe process: build the requested [`Probe`] shape, hold it
/// idle for [`IDLE_SAMPLE`], print `<rss-bytes> <idle-cpu-percent>` and exit.
fn rss_probe_child(probe: Probe) {
    let rt = runtime();
    let (rss_bytes, idle_cpu_pct) = rt.block_on(async move {
        let (client, _broker) = fresh_broker().await;

        let live = match probe {
            // Broker and topology only — no registry, no group.
            Probe::Bare => None,
            // `Group(0)` registers and starts a group whose `min_consumers` is
            // zero, so `start_all()` spawns no consumer task.
            Probe::Group(consumers) => {
                let mut reg = registry(&client, consumers, &Gates::inert()).await;
                reg.start_all();
                Some(reg)
            }
        };

        let cpu_before = current_cpu_secs();
        let wall_start = Instant::now();
        tokio::time::sleep(IDLE_SAMPLE).await;
        let wall = wall_start.elapsed().as_secs_f64();
        let cpu_delta = current_cpu_secs() - cpu_before;
        let rss_bytes = current_rss_bytes();

        // Explicit, so the consumers are unambiguously alive for the whole
        // sample above rather than dropped at some earlier scope end.
        drop(live);

        let idle_cpu_pct = if wall > 0.0 {
            cpu_delta / wall * 100.0
        } else {
            0.0
        };
        (rss_bytes, idle_cpu_pct)
    });

    println!("{rss_bytes} {idle_cpu_pct}");
}

// ── Startup ─────────────────────────────────────────────────────────────────

/// Registering and starting `n` consumers.
///
/// On the in-process backend this is registration plus task spawn — there is
/// no broker to acknowledge the consumers. Broker construction and topology
/// declaration happen outside the timer.
fn bench_startup(c: &mut Criterion) {
    let rt = runtime();
    resource_probe();

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
                        let gates = Gates::inert();

                        let start = Instant::now();
                        let mut reg = registry(&client, consumers, &gates).await;
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
                        let gates = Gates::inert();
                        let mut reg = registry(&client, consumers, &gates).await;
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
/// the group is started and then warmed by [`warm_up`], which returns only
/// once every consumer instance has handled a delivery and nothing is in
/// flight. Two reasons the warm-up is not optional:
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
/// The warm-up is a per-instance barrier, not a fixed burst: each consumer
/// gets its own handler and flips a private flag on its first delivery, and
/// the warm-up republishes for whichever instances are still missing. A burst
/// of one message per consumer would not do — a fast-scheduled subset can
/// drain all of it, leaving other tasks to take their first poll inside the
/// timed region on a loaded runner. It uses the smallest payload regardless of
/// the round's own size, because its job is readiness, not allocation shape.
///
/// Parameterized over consumer count *and* payload size, because the two
/// interact: more consumers only help until per-message serde dominates.
fn bench_dispatch(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("consumer_overhead_dispatch");
    group.throughput(Throughput::Elements(DISPATCH_MESSAGES));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    // The 5 s criterion default measurement window is deliberate. This
    // group used to set 15 s, and back-to-back no-op runs still moved
    // 20-45% on the scheduler-bound ids: the variance is environmental,
    // not sampling, so the extra 10 s per id bought precision the
    // environment immediately discarded -- while tripling the wall-clock
    // of the bench-tier-a CI leg that runs this suite on every PR.

    let warmup_body = payload(WARMUP_PAYLOAD_BYTES);

    for consumers in CONSUMER_COUNTS {
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

                                let gates = Gates::for_group(u64::from(consumers));
                                let mut reg = registry(&client, consumers, &gates).await;
                                reg.start_all();

                                // Outside the timer: returns only once every
                                // consumer instance has handled a delivery and
                                // the warm-up traffic has drained, so no task's
                                // first poll can land inside the timed region.
                                warm_up(&publisher, &warmup_body, &gates).await;

                                gates.arm_round(DISPATCH_MESSAGES);
                                let start = Instant::now();
                                publish_n(&publisher, body, DISPATCH_MESSAGES).await;
                                gates.completed().await;
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

criterion_group!(
    name = benches;
    config = rss_child_gate();
    targets = bench_startup, bench_shutdown, bench_dispatch
);
criterion_main!(benches);

/// Criterion's default configuration — except in an isolated RSS probe child,
/// where this samples and exits instead of returning a configuration at all.
///
/// [`probe_child`] re-runs this executable with [`RSS_CHILD_ENV`] set, and that
/// child must never parse the parent's benchmark arguments. This is the earliest
/// point in the program at which we can intercept it: `criterion_main!` calls
/// the group function first, and `criterion_group!`'s named form evaluates this
/// expression before `.configure_from_args()`.
fn rss_child_gate() -> Criterion {
    if let Some(spec) = std::env::var_os(RSS_CHILD_ENV) {
        let probe = spec.to_str().and_then(Probe::parse).unwrap_or_else(|| {
            panic!("{RSS_CHILD_ENV} must be `bare` or `group:<count>`, got {spec:?}")
        });
        rss_probe_child(probe);
        std::process::exit(0);
    }

    Criterion::default()
}
