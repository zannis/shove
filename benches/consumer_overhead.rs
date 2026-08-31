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
use shove::inmemory::{
    BrokerStatsProvider, InMemoryBroker, InMemoryConsumerGroupConfig,
    InMemoryConsumerGroupRegistry, InMemoryQueueStatsProvider,
};
use shove::{
    Broker, InMemory, MessageHandler, MessageMetadata, Outcome, Publisher, Topic, TopologyBuilder,
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

/// Ceiling on the wait for a dispatch round to complete. Nothing should come
/// close; it exists so a lost delivery fails the run rather than hanging it.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(60);

/// Ceiling on the warm-up's convergence to a fully-polled group. Which
/// consumer a delivery lands on is the backend's to decide, so the warm-up
/// republishes for the instances still missing; this bounds that loop so a
/// group that never converges fails the run with a count instead of publishing
/// forever.
const READINESS_TIMEOUT: Duration = Duration::from_secs(30);

/// How often [`settle`] re-reads the counters it is waiting on.
///
/// Polled rather than notified on purpose: a notification would have to be
/// raised by [`BenchHandler::handle`], which is the one place this benchmark
/// cannot afford instrumentation that outlives the warm-up. Everything
/// [`settle`] waits on is already published by the counters, and it only ever
/// runs outside a timed region, so the poll costs wall-clock nobody measures
/// instead of atomics on the dispatch path.
const SETTLE_POLL: Duration = Duration::from_millis(1);

/// Environment variable that turns this executable into an isolated RSS-probe
/// child for the given consumer count. Set only by [`probe_child`]; a normal
/// `cargo bench` invocation never has it.
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
/// per-instance readiness tally, and the one milestone gate the driver waits
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
/// There is deliberately **one** gate, not one per phase. The timed round is
/// the only thing that needs waking the instant its last delivery lands, so it
/// is the only thing that gets a `Notify` fired from the handler; the warm-up
/// waits by polling in [`settle`] instead. A second gate would mean a second
/// atomic load and branch inside [`BenchHandler::handle`] on every timed
/// delivery, serving a phase that is already over.
///
/// `round_target` is armed by the driver rather than fixed at construction,
/// because the warm-up publishes an amount it discovers as it goes. The wait
/// re-checks the counter, so a `notify_one` permit left behind by a milestone
/// that was already met costs one extra loop turn rather than releasing a wait
/// early.
///
/// Its unarmed sentinel does double duty as the phase marker: `u64::MAX` means
/// no round is armed, which is the same thing as "the warm-up owns the group".
/// [`BenchHandler::handle`] already loads that word to find the round's end, so
/// testing it costs a register comparison and lets the per-instance readiness
/// accounting be skipped outright on every timed delivery instead of running
/// disarmed.
struct Gates {
    /// Deliveries handled by every instance in the group, together.
    seen: AtomicU64,
    /// Instances that have handled at least one delivery.
    polled_instances: AtomicU64,
    /// Consumer instances the group was configured with. `u64::MAX` on a
    /// group whose readiness is never waited on.
    instances: u64,
    /// `seen` value that completes the timed round. `u64::MAX` when unarmed.
    round_target: AtomicU64,
    done: Notify,
}

impl Gates {
    /// Gates for a group of `instances` consumers, the round unarmed.
    fn for_group(instances: u64) -> Arc<Self> {
        Arc::new(Self {
            seen: AtomicU64::new(0),
            polled_instances: AtomicU64::new(0),
            instances,
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

    /// Arms the timed round at `count` deliveries beyond what has been handled
    /// so far, and with it ends the warm-up phase for
    /// [`BenchHandler::handle`], which reads readiness accounting off this same
    /// word.
    ///
    /// Callers must have settled the warm-up first, on both counts: `seen` is
    /// read here, so a delivery still in flight would arm the gate past the end
    /// of the round *and* would be the first delivery of an instance whose
    /// readiness no longer gets recorded. [`settle`] is what makes both
    /// impossible, rather than unlikely.
    fn arm_round(&self, count: u64) {
        let target = self.seen.load(Ordering::SeqCst).saturating_add(count);
        self.round_target.store(target, Ordering::SeqCst);
    }

    /// Waits until the armed round has been fully handled, re-checking `seen`
    /// on every wake so the wait cannot end early on a stale permit.
    ///
    /// Notified rather than polled — unlike [`settle`], this wait *is* inside
    /// the timed region, so the wake latency it saves is latency the dispatch
    /// number would otherwise carry.
    ///
    /// Bounded so a lost delivery fails the run instead of hanging it: an
    /// unbounded wait here is `cargo bench` producing no further output until
    /// CI kills the job, which reads as "slow" rather than "broken".
    async fn completed(&self) {
        let target = self.round_target.load(Ordering::SeqCst);
        while self.seen.load(Ordering::SeqCst) < target {
            if timeout(COMPLETION_TIMEOUT, self.done.notified())
                .await
                .is_err()
            {
                panic!(
                    "dispatch: handlers saw {} of {target} messages in {COMPLETION_TIMEOUT:?}: \
                     a delivery was lost",
                    self.seen.load(Ordering::SeqCst),
                );
            }
        }
    }
}

/// Waits until the group has genuinely gone quiet: every one of `published`
/// warm-up deliveries handled, the queue empty, and the framework's own
/// in-flight tally back at zero.
///
/// **The handler count alone is not a drain barrier.** [`BenchHandler::handle`]
/// bumps `seen` and returns `Outcome::Ack`; everything the consumer loop does
/// with that outcome — joining the handler task, routing the outcome,
/// decrementing the queue's in-flight tally, going back around for more —
/// necessarily runs *after* the count has already moved. Waiting on the count
/// proves `handle` was called, not that the group settled, and on a 256-consumer
/// group at prefetch 10 that residue is exactly what would land inside
/// [`bench_dispatch`]'s timer or [`rss_probe_child`]'s idle sample.
///
/// `messages_in_flight` is the backend's own counter, incremented when an
/// envelope is picked up and decremented on the ack path, so it observes the
/// part `seen` cannot.
///
/// **The two conditions are ordered, not combined.** The consumer loop pops an
/// envelope off the buffer *before* it increments the in-flight tally, so a
/// stats read taken while messages are still being picked up can see both
/// counters at zero with deliveries live. Confirming `seen` first rules that
/// window out: once `seen` has reached `published`, every one of those messages
/// has already been popped and handled, so nothing can still be in the gap when
/// the stats read happens.
async fn settle(stats: &BrokerStatsProvider, gates: &Gates, published: u64) {
    let queue = OverheadTopic::topology().queue();
    let started = Instant::now();

    while gates.seen.load(Ordering::SeqCst) < published {
        if started.elapsed() >= COMPLETION_TIMEOUT {
            panic!(
                "warm-up: handlers saw {} of {published} messages in {COMPLETION_TIMEOUT:?}: \
                 a delivery was lost",
                gates.seen.load(Ordering::SeqCst),
            );
        }
        tokio::time::sleep(SETTLE_POLL).await;
    }

    loop {
        let stats = stats
            .get_queue_stats(queue)
            .await
            .expect("read bench queue stats");
        if stats.messages_ready == 0 && stats.messages_in_flight == 0 {
            return;
        }
        if started.elapsed() >= COMPLETION_TIMEOUT {
            panic!(
                "warm-up: {} ready and {} in flight on {queue} {COMPLETION_TIMEOUT:?} after \
                 {published} messages were handled: the group never went quiet",
                stats.messages_ready, stats.messages_in_flight,
            );
        }
        tokio::time::sleep(SETTLE_POLL).await;
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
        // What a timed delivery pays, in full: one read-modify-write the round
        // needs in order to know when it ends, and one load of the word that
        // says when that is. No warm-up gate is tested here — `settle` polls
        // its milestone from the driver.
        let seen = self
            .gates
            .seen
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let target = self.gates.round_target.load(Ordering::SeqCst);

        // That same load decides the phase, so readiness accounting adds no
        // memory traffic of its own: `round_target` holds `u64::MAX` exactly
        // while no round is armed, which is exactly while the warm-up owns the
        // group. Inside the timed round this is a comparison against a value
        // already in a register, and the block below does not execute at all —
        // skipped, not merely disarmed. Measured at 256 consumers: 0 entries
        // across the round's 512 deliveries, against 512 when it ran
        // unconditionally.
        if target == u64::MAX {
            // Warm-up only. The `swap` decides the increment, so two deliveries
            // racing here on the same instance (prefetch is greater than one)
            // still increment the tally exactly once; the relaxed load only
            // short-circuits after this instance's first delivery, and the flag
            // goes false -> true and no further, so a stale read costs one extra
            // swap and can neither double-count nor miss.
            if !self.polled.load(Ordering::Relaxed) && !self.polled.swap(true, Ordering::SeqCst) {
                // Wrapping is unreachable: one increment per instance, and the
                // group is bounded by `u16::MAX` consumers.
                self.gates.polled_instances.fetch_add(1, Ordering::SeqCst);
            }
        } else if seen >= target {
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
/// It returns only when `polled_instances == instances` and the group has
/// settled — which [`settle`] establishes against the backend's own in-flight
/// tally, not against the handler count, because a handler that has returned is
/// not a delivery the framework has finished with. That is the precondition
/// [`Gates::arm_round`] needs. It panics with the tally if the group has not
/// converged inside [`READINESS_TIMEOUT`].
async fn warm_up(
    publisher: &Publisher<InMemory>,
    stats: &BrokerStatsProvider,
    body: &str,
    gates: &Gates,
) {
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
        settle(stats, gates, published).await;
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
            return pages.saturating_mul(4096);
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

/// What one isolated probe process reported.
struct ProbeSample {
    /// Consumers live in that process. `0` is the baseline process.
    consumers: u16,
    rss_kb: f64,
    idle_cpu_pct: f64,
}

/// Sample RSS-per-consumer and idle CPU **in isolated processes**, one per
/// consumer count, and print a table to stderr.
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
///   into the consumer count. The row-1 marginal is against the baseline
///   process, so it alone still carries the one-time group cost; every later
///   row's marginal does not.
/// - **No row inherits another row's allocator state.** Each process starts
///   with a fresh arena, so nothing is served out of slack a previous
///   measurement mapped.
///
/// `GROUP FIXED` below is that separation stated directly: row 1's growth over
/// the baseline, minus one consumer priced at the largest row's marginal rate.
/// `RSS KB` is total process RSS; `OVER BASE` subtracts the baseline process,
/// which carries the runtime, the broker and the declared topology but no
/// consumer group.
///
/// The measured consumers are idle when sampled, but not untouched: the probe
/// waits until every consumer task has handled a warm-up delivery and that
/// traffic has fully drained before it opens the sample window. That is what
/// makes `IDLE CPU` a resting figure rather than a race against task start-up,
/// and it is wanted for `RSS KB` too — a task that has never been polled has
/// not yet allocated the machinery it receives on, so sampling one would
/// understate what a live consumer costs.
///
/// Its price is disclosed rather than removed, and measured rather than
/// asserted. Waking every instance is a coupon-collect against a prefetching
/// group, so the warm-up publishes far more than one message per instance —
/// 6.8x the instance count at 16 consumers, 17x at 256, 26x at 1024 — and the
/// allocator slack that traffic leaves behind stays in the row's RSS while the
/// baseline row, which publishes nothing, pays none of it.
///
/// That residue was sized by replaying the same volume in the same round sizes
/// once the group is already warm, so the replay adds traffic and no new task
/// state: it accounts for roughly 30% of what the warm-up adds at 256
/// consumers (268 KB of 904 KB) and 41% at 1024. The remaining majority is
/// consumer state that exists only once a task has been polled — the part an
/// unwarmed sample misses. Because the replay reuses arena the warm-up already
/// mapped, treat that share as a floor.
///
/// So `KB/CONSUMER` is an upper bound on a consumer's resting cost: honest
/// about the machinery, carrying some warm-up residue on top.
fn resource_probe() {
    // Listing benchmark ids must not fork the process 5 times.
    if std::env::args().any(|arg| arg == "--list") {
        return;
    }

    let baseline = probe_child(0);
    let rows: Vec<ProbeSample> = CONSUMER_COUNTS.iter().map(|n| probe_child(*n)).collect();

    eprintln!();
    eprintln!("consumer resource probe (not a criterion measurement)");
    eprintln!("each row is a separate process holding exactly one registry and one group,");
    eprintln!("so KB/CONSUMER is a process-to-process difference with the group cost cancelled.");
    eprintln!(
        "baseline process (runtime + broker + topology, no group): {:.1} KB RSS",
        baseline.rss_kb
    );
    eprintln!(
        "{:>10} {:>10} {:>11} {:>14} {:>10}",
        "CONSUMERS", "RSS KB", "OVER BASE", "KB/CONSUMER", "IDLE CPU"
    );
    eprintln!("{}", "-".repeat(60));

    let mut previous = &baseline;
    for row in &rows {
        let added = f64::from(row.consumers.saturating_sub(previous.consumers));
        let marginal = if added > 0.0 {
            (row.rss_kb - previous.rss_kb) / added
        } else {
            f64::NAN
        };
        eprintln!(
            "{:>10} {:>10.1} {:>11.1} {:>14.1} {:>9.1}%",
            row.consumers,
            row.rss_kb,
            row.rss_kb - baseline.rss_kb,
            marginal,
            row.idle_cpu_pct,
        );
        previous = row;
    }

    // The group's own fixed cost, separated rather than smeared: what row 1
    // cost over the baseline, minus a single consumer priced at the largest
    // row's marginal rate (the rate with the least fixed cost in it).
    let second_last = rows.len().checked_sub(2).and_then(|i| rows.get(i));
    if let (Some(first), Some(last), Some(second_last)) = (rows.first(), rows.last(), second_last) {
        let span = f64::from(last.consumers.saturating_sub(second_last.consumers));
        if span > 0.0 {
            let per_consumer = (last.rss_kb - second_last.rss_kb) / span;
            let fixed =
                (first.rss_kb - baseline.rss_kb) - per_consumer * f64::from(first.consumers);
            eprintln!(
                "GROUP FIXED: {fixed:.1} KB — registry + group + tokens + spawner, \
                 priced out of row 1 at the {}-consumer marginal rate ({per_consumer:.1} KB)",
                last.consumers,
            );
        }
    }
    eprintln!();
}

/// Run one isolated probe process and read back its sample.
///
/// The child's stderr is captured rather than inherited so that a probe which
/// dies still says why: every failure path below quotes it, where an inherited
/// stream would have scrolled past above the table this fills. On success it is
/// empty — the child exits from [`rss_child_gate`] before `Criterion` is ever
/// constructed, so none of criterion's own start-up output can reach it.
fn probe_child(consumers: u16) -> ProbeSample {
    let exe = std::env::current_exe().expect("locate this bench executable for the RSS probe");
    let output = Command::new(exe)
        .env(RSS_CHILD_ENV, consumers.to_string())
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap_or_else(|e| panic!("spawn RSS probe child for {consumers} consumers: {e}"));

    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        panic!(
            "RSS probe child for {consumers} consumers exited with {}; stderr: {}",
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
            "RSS probe child for {consumers} consumers printed {line:?}, expected `<rss> <cpu>`; \
             stderr: {}",
            stderr.trim(),
        );
    };

    ProbeSample {
        consumers,
        rss_kb: rss_bytes as f64 / 1024.0,
        idle_cpu_pct,
    }
}

/// The isolated probe process: build a group of `consumers`, wait until every
/// one of them has run, hold the group idle for [`IDLE_SAMPLE`], print
/// `<rss-bytes> <idle-cpu-percent>` and exit.
///
/// `consumers == 0` is the baseline: broker, topology and a publisher, but no
/// registry and no group. It is what every row is measured against, and it
/// builds the publisher for exactly that reason — so `OVER BASE` reports the
/// group rather than the group plus a publisher.
fn rss_probe_child(consumers: u16) {
    let rt = runtime();
    let (rss_bytes, idle_cpu_pct) = rt.block_on(async move {
        let (client, broker) = fresh_broker().await;

        // Built on every row, the baseline included, so its fixed cost cancels
        // in the subtraction instead of being charged to the consumers. The
        // baseline row holds one and never publishes through it.
        let publisher = broker.publisher().await.expect("publisher");

        let live = if consumers > 0 {
            let gates = Gates::for_group(u64::from(consumers));
            let mut reg = registry(&client, consumers, &gates).await;
            reg.start_all();

            // Readiness barrier, and the reason this probe uses real gates
            // rather than `Gates::inert()`. `start_all` only `tokio::spawn`s:
            // without waiting here, each consumer task's first poll — and
            // whatever it allocates on the way into its receive loop — would
            // land inside the sample window below, billed as idle CPU and
            // missing from the RSS figure. `warm_up` returns only once every
            // instance has handled a delivery *and* the backend reports nothing
            // ready and nothing in flight, so the ack-side work the last warm-up
            // deliveries leave behind has finished before `cpu_before` is read
            // rather than being sampled as idle.
            let stats = BrokerStatsProvider::new(client.clone());
            warm_up(&publisher, &stats, &payload(WARMUP_PAYLOAD_BYTES), &gates).await;

            Some(reg)
        } else {
            None
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
/// once every consumer instance has handled a delivery and the backend itself
/// reports nothing ready and nothing in flight — see [`settle`] for why the
/// handler count is not on its own enough to claim that. Two reasons the
/// warm-up is not optional:
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
    group.measurement_time(Duration::from_secs(15));

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
                                // the backend reports the warm-up traffic fully
                                // settled — nothing ready, nothing in flight —
                                // so neither a task's first poll nor the ack
                                // work trailing the last warm-up delivery can
                                // land inside the timed region.
                                let stats = BrokerStatsProvider::new(client.clone());
                                warm_up(&publisher, &stats, &warmup_body, &gates).await;

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
        let consumers = spec
            .to_str()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or_else(|| panic!("{RSS_CHILD_ENV} must be a consumer count, got {spec:?}"));
        rss_probe_child(consumers);
        std::process::exit(0);
    }

    Criterion::default()
}
