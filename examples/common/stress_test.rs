//! Shared stress-benchmark harness for all shove backends.
//!
//! Each backend's `stress.rs` is a thin wrapper that constructs a
//! `Broker<B>` and calls either [`run_all_scenarios`] (coordinated-group
//! backends: InMemory, Kafka, NATS, RabbitMQ, Redis) or
//! [`run_supervisor_scenarios`] (SQS).
//!
//! # Dimensions
//!
//! A scenario is `(flow, payload, tier, handler, consumers)`. The full
//! cross-product is thousands of scenarios and many hours, so the defaults are
//! deliberately narrow — `--flow consumer-group --payload 64` reproduces
//! exactly what this harness measured before flows and payloads existed — and
//! the sweeps are opt-in.
//!
//! A sampled core matrix, which is what `benches/results/bench-results.json`
//! is generated from:
//!
//! ```text
//! cargo run --release --example inmemory_stress --features inmemory -- \
//!     --flow all --payload all --tier moderate --handler fast \
//!     --consumers 1,8,32 --results-file benches/results/bench-results.json
//! ```
//!
//! `--results-file` merges into an existing file by backend key, so running
//! the six backend binaries in sequence against the same path accumulates one
//! cross-backend document. It never writes to stdout: `--output table|json`
//! keeps its own contract untouched.
//!
//! Broadcast fans every published message out to *every* subscriber, so its
//! processed total is `messages × consumers` rather than `messages`. Prefer
//! small `--consumers` values there.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use shove::{
    Broker,
    ConsumerGroupConfig,
    ConsumerOptions,
    backend::{
        Backend,
        capability::{HasBroadcast, HasCoordinatedGroups},
    },
    handler::{BatchMessageHandler, MessageHandler},
    metadata::{DeadMessageMetadata, MessageMetadata},
    outcome::Outcome,
    // `SequencedTopic` is unused by name but must be in scope: the
    // `define_sequenced_topic!` expansion resolves `Self::sequence_key`
    // through it.
    topic::{SequencedTopic as _, Topic},
    topology::{QueueTopology, SequenceFailure, TopologyBuilder},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Schema version of the emitted results document. Bumped only on a removal
/// or a semantic change of an existing field; a field a consumer can ignore
/// and still read the document correctly does not bump it.
///
/// v2 added `handler_cost` to every result row. It is additive in shape but
/// not ignorable: it says whether `throughput_msg_per_sec` measures shove or a
/// simulated sleep amortised over a batch, so a consumer that skips it
/// misreads a field it already reads. A v1 row cannot be given the marker
/// after the fact either — the run that produced it is over — so the version
/// is what makes the mismatch a loud refusal in [`merge_results_file`] rather
/// than a silent gap in a merged document.
///
/// v3 added `setup_secs` and, with it, changed what `duration_secs` means on
/// every consume row: it is now the drain alone, because the drivers hold a
/// readiness barrier before starting the clock. A v2 `duration_secs` on those
/// rows included the consumer group's join latency — a fixed cost that on the
/// `zero`/`fast` cells was the whole number — so the two versions' values are
/// not comparable and must not be charted on one axis. The same change
/// narrowed `handler_cost`: `framework` now additionally asserts that the
/// window was a drain, and rows that fail that gained the new `setup_bound`
/// value. Both are reasons a v2 reader cannot simply ignore the new field.
pub const RESULTS_SCHEMA_VERSION: u32 = 3;

/// The payload sizes that may appear in `payload_bytes`: 64 B, 1 KiB, 64 KiB.
pub const PAYLOAD_SIZES: [usize; 3] = [64, 1024, 65536];

/// Routing shards for the sequenced topic. Also the modulus for the sequence
/// key, so message ids spread evenly across shards.
const SEQ_SHARDS: u16 = 8;

/// Message id reserved for broadcast readiness sentinels. Corpus ids count up
/// from zero, so no measured message can carry it.
const SENTINEL_ID: u64 = u64::MAX;

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

    /// Which flow(s) to run: `all`, or a comma-separated list of
    /// `publish-single`, `publish-batch`, `consume-parallel`, `consume-fifo`,
    /// `consume-batch`, `consumer-group`, `supervisor`, `broadcast`,
    /// `dlq-drain`.
    ///
    /// When omitted, each entry point selects what this harness measured
    /// before flows were a dimension: `consumer-group` on coordinated-group
    /// backends, `supervisor` on SQS. A single hard default of
    /// `consumer-group` would be filtered out as unsupported on the
    /// supervisor path and the unchanged SQS invocation would run nothing.
    #[arg(long)]
    pub flow: Option<FlowArg>,

    /// Payload size(s) in bytes: `all`, or a comma-separated subset of
    /// `64`, `1024`, `65536`.
    #[arg(long, default_value = "64")]
    pub payload: PayloadArg,

    /// Override the tier's consumer counts, e.g. `--consumers 1,8,32`.
    /// This is the sampling lever for the published core matrix.
    #[arg(long)]
    pub consumers: Option<ConsumersArg>,

    /// Also write the versioned results document to this path, merging into
    /// any existing file by backend key. Never written to stdout.
    #[arg(long)]
    pub results_file: Option<String>,

    /// Human-readable hardware label for the results provenance block
    /// (default: derived from the host).
    #[arg(long)]
    pub hardware_label: Option<String>,
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

/// Parsed `--flow`. Kept as an explicit list rather than a `ValueEnum` so
/// `all` and a comma-separated subset can share one flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowArg(pub Vec<Flow>);

impl FromStr for FlowArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("all") {
            return Ok(FlowArg(Flow::ALL.to_vec()));
        }
        let mut flows = Vec::new();
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let flow = Flow::from_cli(part).ok_or_else(|| {
                let names: Vec<&str> = Flow::ALL.iter().map(|f| f.as_cli()).collect();
                format!(
                    "unknown flow '{part}'; expected `all` or one of: {}",
                    names.join(", ")
                )
            })?;
            if !flows.contains(&flow) {
                flows.push(flow);
            }
        }
        if flows.is_empty() {
            return Err("no flows selected".to_string());
        }
        Ok(FlowArg(flows))
    }
}

/// Parsed `--payload`. Every value is validated against [`PAYLOAD_SIZES`] at
/// parse time, so an out-of-set size can never reach a `ScenarioResult`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadArg(pub Vec<usize>);

impl FromStr for PayloadArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("all") {
            return Ok(PayloadArg(PAYLOAD_SIZES.to_vec()));
        }
        let mut sizes = Vec::new();
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let n: usize = part
                .parse()
                .map_err(|_| format!("payload '{part}' is not a number"))?;
            if !PAYLOAD_SIZES.contains(&n) {
                return Err(format!(
                    "payload {n} is not one of {PAYLOAD_SIZES:?}; the results schema pins \
                     `payload_bytes` to that set"
                ));
            }
            if !sizes.contains(&n) {
                sizes.push(n);
            }
        }
        if sizes.is_empty() {
            return Err("no payload sizes selected".to_string());
        }
        Ok(PayloadArg(sizes))
    }
}

/// Parsed `--consumers`, overriding the tier's built-in consumer counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumersArg(pub Vec<u16>);

impl FromStr for ConsumersArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut counts = Vec::new();
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let n: u16 = part
                .parse()
                .map_err(|_| format!("consumer count '{part}' is not a number"))?;
            if n == 0 {
                return Err("consumer count must be at least 1".to_string());
            }
            if !counts.contains(&n) {
                counts.push(n);
            }
        }
        if counts.is_empty() {
            return Err("no consumer counts selected".to_string());
        }
        Ok(ConsumersArg(counts))
    }
}

// ── Flow & mode ─────────────────────────────────────────────────────────────

/// The closed set of benchmarked flows. The string forms are the results
/// schema's `flow` values and are consumed by `chartgen`, so they are a
/// contract — not a display detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Flow {
    PublishSingle,
    PublishBatch,
    ConsumeParallel,
    ConsumeFifo,
    ConsumeBatch,
    ConsumerGroup,
    Supervisor,
    Broadcast,
    DlqDrain,
    Autoscaler,
}

impl Flow {
    /// Every variant, in schema-table order.
    pub const ALL: [Flow; 9] = [
        Flow::PublishSingle,
        Flow::PublishBatch,
        Flow::ConsumeParallel,
        Flow::ConsumeFifo,
        Flow::ConsumeBatch,
        Flow::ConsumerGroup,
        Flow::Supervisor,
        Flow::Broadcast,
        Flow::DlqDrain,
    ];

    /// The results-schema spelling. `chartgen` matches on these.
    pub fn as_str(&self) -> &'static str {
        match self {
            Flow::PublishSingle => "publish_single",
            Flow::PublishBatch => "publish_batch",
            Flow::ConsumeParallel => "consume_parallel",
            Flow::ConsumeFifo => "consume_fifo",
            Flow::ConsumeBatch => "consume_batch",
            Flow::ConsumerGroup => "consumer_group",
            Flow::Supervisor => "supervisor",
            Flow::Broadcast => "broadcast",
            Flow::DlqDrain => "dlq_drain",
            Flow::Autoscaler => "autoscaler",
        }
    }

    /// The `--flow` spelling: the schema name with `-` for `_`.
    pub fn as_cli(&self) -> &'static str {
        match self {
            Flow::PublishSingle => "publish-single",
            Flow::PublishBatch => "publish-batch",
            Flow::ConsumeParallel => "consume-parallel",
            Flow::ConsumeFifo => "consume-fifo",
            Flow::ConsumeBatch => "consume-batch",
            Flow::ConsumerGroup => "consumer-group",
            Flow::Supervisor => "supervisor",
            Flow::Broadcast => "broadcast",
            Flow::DlqDrain => "dlq-drain",
            Flow::Autoscaler => "autoscaler",
        }
    }

    fn from_cli(s: &str) -> Option<Flow> {
        Flow::ALL
            .iter()
            .copied()
            .find(|f| f.as_cli().eq_ignore_ascii_case(s))
    }

    /// How many workers actually share the message stream, which is what the
    /// deadline must be sized against.
    ///
    /// Only the competing-consumer flows divide the work `consumers` ways.
    /// `run_dlq` is a single loop no matter how many consumers the scenario
    /// names, a broadcast subscriber receives the whole stream rather than a
    /// share of it, and the publish flows are one sequential loop. Dividing
    /// their deadlines by the consumer count made them N× too tight, so a
    /// slow-but-healthy drain was recorded as a timeout failure.
    ///
    /// FIFO is capped at [`SEQ_SHARDS`]: the sequenced topology has that many
    /// shards, and consumers past the shard count cannot make independent
    /// progress, so sizing the deadline by the raw consumer count made a
    /// 256-consumer slow-handler scenario up to 32× too tight.
    pub fn effective_workers(&self, consumers: u16) -> u16 {
        match self {
            Flow::ConsumerGroup | Flow::ConsumeParallel | Flow::ConsumeBatch | Flow::Supervisor => {
                consumers
            }
            Flow::ConsumeFifo => consumers.min(SEQ_SHARDS),
            Flow::DlqDrain
            | Flow::Broadcast
            | Flow::PublishSingle
            | Flow::PublishBatch
            | Flow::Autoscaler => 1,
        }
    }

    /// Whether this flow's driver holds a readiness barrier, and so can record
    /// a `setup_secs` separated from its drain at all.
    ///
    /// The three answers, and why each one is where it is:
    ///
    /// - **Yes** for the four barrier-holding drivers — `run_scenario_batch`,
    ///   `run_scenario_supervisor` (non-FIFO), `run_scenario_group` (non-FIFO)
    ///   and `run_scenario_broadcast`. Each waits until every worker has
    ///   handled a sentinel before taking `start`.
    /// - **No** for `consume_fifo` and `dlq_drain`: neither has anywhere to
    ///   hang the probe — a sequenced sentinel routes to exactly one shard, and
    ///   the DLQ drain's measured phase has no publish path at all — so both
    ///   drivers emit `setup_secs: None` by construction.
    /// - **No** for the publish flows and the autoscaler, which construct no
    ///   consumer, so there is no worker for a barrier to wait on.
    ///
    /// [`HandlerCost::of`] consults this so that the `framework` marker's first
    /// guarantee is checked against the flow and not only against the recorded
    /// `Option`: a row whose flow can never separate its setup must not be able
    /// to claim it did by carrying a `setup_secs` value.
    pub fn holds_readiness_barrier(&self) -> bool {
        match self {
            Flow::ConsumeBatch
            | Flow::ConsumeParallel
            | Flow::ConsumerGroup
            | Flow::Supervisor
            | Flow::Broadcast => true,
            Flow::ConsumeFifo
            | Flow::DlqDrain
            | Flow::PublishSingle
            | Flow::PublishBatch
            | Flow::Autoscaler => false,
        }
    }

    /// The chart grouping key. Redundant with the flow for the consume flows
    /// by design — it is what chart family 3 (parallel vs sequenced) groups
    /// on, so `chartgen` never parses a flow name to place a bar.
    pub fn mode(&self) -> Mode {
        match self {
            Flow::ConsumeFifo => Mode::Fifo,
            Flow::ConsumeBatch | Flow::PublishBatch => Mode::Batch,
            _ => Mode::Parallel,
        }
    }
}

impl fmt::Display for Flow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Parallel,
    Fifo,
    Batch,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::Parallel => "parallel",
            Mode::Fifo => "fifo",
            Mode::Batch => "batch",
        }
    }
}

/// What a row's throughput number is actually a measurement *of*.
///
/// The handler profiles sleep, and the batch handler sleeps once per batch
/// while the per-message handler sleeps once per message — the correct model
/// of a batching sink, but it means the two are not measuring the same thing
/// under `--handler slow|heavy`. Recording which kind of cell a row is keeps a
/// consumer of the results document from having to re-derive that from a flow
/// name and a prose handler label, and keeps a simulated sleep from being
/// charted as a shove throughput claim.
///
/// The string forms are the schema's `handler_cost` values, so they are a
/// contract in the same way [`Flow::as_str`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerCost {
    /// The simulated work is negligible **and** the row's measured window is a
    /// drain, so the number is shove's own cost. The only cells that are
    /// comparable across flows.
    ///
    /// Precisely what this marker guarantees, since a bare "negligible
    /// handler" is not enough and used to be all it meant:
    ///
    /// 1. The driver **separated** fixed setup cost from the drain — it held a
    ///    readiness barrier until every worker was assigned and polling, took
    ///    `start` after it, and recorded everything from the first worker being
    ///    spawned up to that point as `setup_secs`. A row whose `setup_secs` is
    ///    absent did not separate them, so its `duration_secs` may be mostly
    ///    coordination latency — and a row whose flow holds no barrier
    ///    ([`Flow::holds_readiness_barrier`]) cannot have separated them
    ///    whatever it recorded.
    /// 2. The drain it then timed ran for at least
    ///    [`MIN_FRAMEWORK_WINDOW_SECS`], so the number is a rate rather than a
    ///    stopwatch reading over a window too short to hold one.
    ///
    /// A `zero`/`fast` row failing either half is [`HandlerCost::SetupBound`],
    /// not this. It does **not** guarantee anything about the broker's own
    /// steady state beyond that barrier — a backend whose throughput keeps
    /// climbing past the first assigned poll is still measured from the first
    /// one.
    Framework,
    /// The simulated work is negligible, but the row cannot support the
    /// [`HandlerCost::Framework`] claim: either the driver never separated
    /// fixed setup cost from the drain, or the drain window was too short to
    /// be a rate. The number is some mixture of shove's cost and a one-off
    /// coordination cost, in an unknown ratio, and must not be charted as a
    /// shove throughput claim.
    ///
    /// This is the marker CAF-667's `zero`/`fast` concurrent consume rows
    /// should have carried: their windows were ~3.2 s at one consumer and
    /// ~6.5 s at four and sixteen, invariant across a 32× range of corpus
    /// size, while the drain inside them was as little as 0.05 s.
    SetupBound,
    /// A batch-mode flow with a sleeping handler: the sleep is paid once per
    /// batch, so throughput scales with the batch size rather than with
    /// anything shove does.
    HandlerAmortised,
    /// A non-batch flow with a sleeping handler: the sleep is paid once per
    /// message, so the number is dominated by the handler but not amortised.
    HandlerBound,
    /// A publish-only flow: no consumer is constructed, so no handler runs and
    /// the profile only selects the message count.
    NoHandler,
}

/// How a driver split the wall-clock it spent on a scenario: what it excluded
/// as fixed setup cost, and what remained as the drain it actually timed.
///
/// Carried into [`HandlerCost::of`] because whether a row measures shove is
/// not decidable from its flow and handler alone — it also depends on whether
/// the run separated the two and how much drain was left. Both fields are
/// recorded on the row, so the derivation stays re-checkable by
/// [`validate_run`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WindowSplit {
    /// Seconds from the first worker being spawned to the readiness barrier
    /// being satisfied — the fixed coordination cost **excluded** from
    /// `duration_secs`. `None` means this driver does not separate the two, so
    /// `duration_secs` may still contain it.
    pub setup_secs: Option<f64>,
    /// The measured window, in seconds: the drain, when `setup_secs` is
    /// `Some`.
    pub duration_secs: f64,
}

/// How long a drain has to run before its rate is worth publishing as
/// [`HandlerCost::Framework`].
///
/// Not a correction — the readiness barrier already excludes setup, so setup's
/// magnitude no longer biases the rate. This is a floor on how much signal the
/// row has: 5,000 messages through one Kafka consumer drained in 0.048 s in
/// the CAF-667 sweep, which is a stopwatch reading dominated by prefetch and
/// first-poll effects, not a steady-state rate. One second is ~20× that, and
/// is tens of thousands of messages at every payload tier.
pub const MIN_FRAMEWORK_WINDOW_SECS: f64 = 1.0;

impl HandlerCost {
    /// The marker a row with this `flow`, `handler` and measured `window` must
    /// carry.
    ///
    /// `flow` and `handler` together settle what *kind* of cost the number is,
    /// because neither settles it alone: `publish_batch` is a batch-mode flow
    /// that runs no handler at all, and `consume_batch --handler fast` is a
    /// batch-mode flow whose number is still framework cost.
    ///
    /// `window` then settles whether a framework-cost cell actually *measured*
    /// that cost. It has to: a negligible handler says the number is not the
    /// handler's, which is not the same as saying it is shove's. On the
    /// `zero`/`fast` concurrent consume cells the window was a fixed group
    /// join latency, so the row was stamped "shove's own cost" over an
    /// interval in which shove had barely run — and because the derivation
    /// took only `(flow, handler)`, [`validate_run`] re-derived the same wrong
    /// answer and certified it.
    ///
    /// This is the single derivation. [`Scenario::handler_cost`] stamps a row
    /// with it and [`validate_run`] re-derives it to check one, so a row's
    /// marker is verifiable rather than merely present — see the refusal
    /// there for why a row that disagrees is a lie about what was measured.
    pub fn of(flow: Flow, handler: HandlerProfile, window: WindowSplit) -> HandlerCost {
        match flow {
            // No consumer is constructed, so there is no worker to wait for
            // and nothing for a barrier to exclude. The window is irrelevant
            // to a publish row and is not consulted.
            Flow::PublishSingle | Flow::PublishBatch => HandlerCost::NoHandler,
            _ => match handler {
                // Three conditions, and the flow is one of them: a recorded
                // `setup_secs` is only evidence of a separated window if this
                // flow's driver holds a barrier at all. `consume_fifo` and
                // `dlq_drain` always emit `None`, so a `Some` on either is a
                // value no run of that flow produced — and without the flow
                // check `validate_run` re-derived `framework` from it and
                // certified the row.
                HandlerProfile::Zero | HandlerProfile::Fast => match window.setup_secs {
                    // A finite, non-negative interval is the only value a
                    // barrier can have measured; anything else on a row is not
                    // evidence of a separated window.
                    Some(setup)
                        if setup.is_finite()
                            && setup >= 0.0
                            && flow.holds_readiness_barrier()
                            && window.duration_secs >= MIN_FRAMEWORK_WINDOW_SECS =>
                    {
                        HandlerCost::Framework
                    }
                    _ => HandlerCost::SetupBound,
                },
                // A sleeping handler's marker already says the number is the
                // sleep and not shove, so a setup floor underneath it changes
                // nothing a reader would conclude. Gating these on the window
                // too would relabel rows whose caveat is already stated.
                HandlerProfile::Slow | HandlerProfile::Heavy => match flow.mode() {
                    Mode::Batch => HandlerCost::HandlerAmortised,
                    Mode::Parallel | Mode::Fifo => HandlerCost::HandlerBound,
                },
            },
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            HandlerCost::Framework => "framework",
            HandlerCost::SetupBound => "setup_bound",
            HandlerCost::HandlerAmortised => "handler_amortised",
            HandlerCost::HandlerBound => "handler_bound",
            HandlerCost::NoHandler => "no_handler",
        }
    }
}

// ── Topic & message ─────────────────────────────────────────────────────────

/// The benchmark message.
///
/// `payload` is ASCII filler of exactly `payload_bytes` characters. The
/// default [`shove::JsonCodec`] encodes ASCII one byte per character, so the
/// declared size is the payload's exact wire cost — a `Vec<u8>` would
/// serialize as a JSON array of decimal integers and inflate 3–4×, making
/// `payload_bytes` a lie. `payload_bytes` measures the application payload,
/// not the full encoded body: the envelope below rides on top of it.
///
/// `id` and `published_at_ns` add a fixed envelope of roughly 50 bytes on top
/// of `payload`, identical across every payload tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressTestMsg {
    pub id: u64,
    pub published_at_ns: u64,
    pub payload: String,
}

/// ASCII filler of exactly `bytes` characters.
pub fn payload_of(bytes: usize) -> String {
    "x".repeat(bytes)
}

/// The sequence key for [`StressSeqTopic`]. Derived from the id so sequencing
/// costs zero wire bytes; spreads ids evenly over [`SEQ_SHARDS`] keys.
fn stress_sequence_key(msg: &StressTestMsg) -> String {
    (msg.id % SEQ_SHARDS as u64).to_string()
}

shove::define_topic!(
    pub StressTestTopic,
    StressTestMsg,
    TopologyBuilder::new("shove-stress-bench")
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// A separate topic, not a flag on the one above: `register_fifo` rejects a
// topology with no `sequenced(...)` config, and `sequenced()` cannot be added
// to a topology that is also used unsequenced without changing what every
// other flow measures.
shove::define_sequenced_topic!(
    pub StressSeqTopic,
    StressTestMsg,
    stress_sequence_key,
    TopologyBuilder::new("shove-stress-bench-seq")
        .sequenced(SequenceFailure::Skip)
        .routing_shards(SEQ_SHARDS)
        // `build()` panics on a sequenced topology carrying neither a hold
        // queue nor `allow_message_loss()`: ordering plus silent drops is a
        // pairing it refuses to declare.
        .hold_queue(Duration::from_secs(5))
        .dlq()
        .build()
);

// A third topic, again forced rather than chosen: `TopologyBuilder::build()`
// rejects `broadcast()` combined with `dlq()`, `hold_queue()` or
// `sequenced()`, so a broadcast topology cannot share either of the above.
shove::define_topic!(
    pub StressBroadcastTopic,
    StressTestMsg,
    TopologyBuilder::new("shove-stress-bench-bcast")
        .broadcast()
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

impl HandlerProfile {
    pub const ALL: [HandlerProfile; 4] = [
        HandlerProfile::Zero,
        HandlerProfile::Fast,
        HandlerProfile::Slow,
        HandlerProfile::Heavy,
    ];

    /// The inverse of [`Display`](fmt::Display), which is the form a result
    /// row's `handler` field carries. `None` for a label this harness could
    /// not have written.
    pub fn from_label(label: &str) -> Option<HandlerProfile> {
        HandlerProfile::ALL
            .into_iter()
            .find(|p| p.to_string() == label)
    }
}

impl fmt::Display for HandlerProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    pub flow: Flow,
    pub payload_bytes: usize,
}

impl Scenario {
    /// How many handler invocations this scenario waits for.
    ///
    /// Broadcast delivers every message to every subscriber, so its total is
    /// `messages × consumers`. Every other flow processes each message once.
    pub fn expected_processed(&self) -> u64 {
        match self.flow {
            Flow::Broadcast => self.messages.saturating_mul(self.consumers as u64),
            _ => self.messages,
        }
    }

    /// What this scenario's throughput number measures — see
    /// [`HandlerCost::of`], which is the derivation.
    ///
    /// Takes the measured window because the marker is not knowable before the
    /// scenario runs: whether a negligible-handler cell measured shove depends
    /// on whether its driver separated setup from drain and on how much drain
    /// was left to time.
    pub fn handler_cost(&self, window: WindowSplit) -> HandlerCost {
        HandlerCost::of(self.flow, self.handler, window)
    }
}

struct TierConfig {
    name: &'static str,
    consumers: &'static [u16],
    /// Per-consumer message count per handler profile, ordered
    /// (zero, fast, slow, heavy). Total messages for a scenario is
    /// `messages_per_consumer.X * scenario.consumers`, *unless*
    /// [`framework_corpus_floor`] raises it: a cell whose tier sizing is too
    /// small to drain for [`MIN_FRAMEWORK_WINDOW_SECS`] runs the floor's corpus
    /// instead, so the identity above is a lower bound rather than an
    /// invariant, and the corpus each cell actually ran is on the row as
    /// `messages`. The floored cells are the low consumer counts of a
    /// `zero`/`fast` column, which therefore hold a near-fixed corpus rather
    /// than one proportional to the consumer count.
    messages_per_consumer: (u64, u64, u64, u64),
}

const MODERATE: TierConfig = TierConfig {
    name: "moderate",
    consumers: &[1, 4, 8, 16, 32],
    messages_per_consumer: (5_000, 2_500, 500, 50),
};

const HIGH: TierConfig = TierConfig {
    name: "high",
    consumers: &[8, 16, 32, 64],
    messages_per_consumer: (20_000, 10_000, 1_000, 10),
};

const EXTREME: TierConfig = TierConfig {
    name: "extreme",
    consumers: &[32, 64, 128, 256],
    messages_per_consumer: (20_000, 10_000, 200, 5),
};

/// Smallest corpus a framework-cost consume cell is run with, before the byte
/// cap below applies.
///
/// The readiness barrier makes `duration_secs` a drain, but it cannot make a
/// drain long enough to be worth publishing: at CAF-667's sizes, five of the
/// six `zero`/`fast` concurrent cells drained in under a second — one of them
/// in 0.052 s — and [`HandlerCost::of`] now refuses them the `framework`
/// marker for it. Without a floor the guard is an alarm with no path to green.
///
/// Sized from the fastest post-barrier drain rate the CAF-667 rows imply
/// (~111,000 msg/s at 64 B, sixteen Kafka consumers), so the smallest cell
/// still clears [`MIN_FRAMEWORK_WINDOW_SECS`] with margin. Faster hardware may
/// drop back under it — that is the guard reporting a real loss of signal, not
/// a number to chase here.
const MIN_FRAMEWORK_CORPUS_MESSAGES: u64 = 150_000;

/// Ceiling on what the floor above may cost in bytes.
///
/// Without it the floor multiplies by the payload tier: 150,000 × 64 KiB is a
/// 9.6 GiB corpus staged through a publisher. The large tiers do not need the
/// floor anyway — bytes, not message count, bound the drain there, so their
/// windows are already long — so this caps the floor rather than trading it
/// off.
const MAX_FRAMEWORK_CORPUS_BYTES: u64 = 32 * 1024 * 1024;

/// The smallest corpus this cell may run with, or 0 where the floor does not
/// apply.
///
/// Only raises a corpus, never lowers one: a tier that already sizes a cell
/// above the floor keeps its own number.
///
/// The floor is restricted to cells whose drain is bounded by shove rather than
/// by a simulated sleep, because on a sleeping cell a larger corpus buys
/// nothing but wall-clock. `zero` never sleeps, so it always qualifies. `fast`
/// sleeps 1–5 ms per message, which concurrency hides and sequential
/// processing does not: the sequential `fast` cells drained at ~470 msg/s, so
/// this floor would turn a 5 s scenario into a 5-minute one — and those cells
/// are already drain-bound, so they never needed it. Batch mode is the
/// exception: it pays the sleep once per batch, so its `fast` cells are
/// drain-bound with or without `--concurrent`.
fn framework_corpus_floor(
    flow: Flow,
    handler: HandlerProfile,
    concurrent: bool,
    payload_bytes: usize,
) -> u64 {
    let negligible_and_not_sleep_bound = match handler {
        HandlerProfile::Zero => true,
        HandlerProfile::Fast => concurrent || flow.mode() == Mode::Batch,
        HandlerProfile::Slow | HandlerProfile::Heavy => false,
    };
    // Asked of the flow rather than re-listed here: FIFO, the DLQ drain and the
    // publish flows are excluded because no corpus can make them `framework`,
    // and that is the same fact [`Flow::holds_readiness_barrier`] already
    // carries for [`HandlerCost::of`]. A second hand-maintained list of it
    // would only drift from the first.
    //
    // Broadcast is the one flow subtracted from that set, and not because of
    // its barrier — it holds one. Its corpus is the *per-subscriber* workload,
    // so flooring `messages` multiplies by the fan-out width and a
    // 16-subscriber cell would run 2.4M handler invocations.
    let barrier_backed_drain = flow.holds_readiness_barrier() && flow != Flow::Broadcast;
    if !negligible_and_not_sleep_bound || !barrier_backed_drain {
        return 0;
    }
    let by_bytes = MAX_FRAMEWORK_CORPUS_BYTES / (payload_bytes as u64).max(1);
    MIN_FRAMEWORK_CORPUS_MESSAGES.min(by_bytes)
}

fn scenario_deadline(messages: u64, consumers: u16, handler: HandlerProfile) -> Duration {
    let expected_ms = match handler {
        HandlerProfile::Zero => messages as f64 / 40.0,
        HandlerProfile::Fast => (messages as f64 * 3.0) / consumers as f64,
        HandlerProfile::Slow => (messages as f64 * 175.0) / consumers as f64,
        HandlerProfile::Heavy => (messages as f64 * 3000.0) / consumers as f64,
    };
    // 3× expected to absorb steady-state variance and scheduling jitter; 60 s
    // floor keeps short scenarios from racing broker setup; 600 s ceiling
    // prevents any single scenario from blocking the whole sweep.
    let deadline_ms = (expected_ms * 3.0).clamp(60_000.0, 600_000.0);
    Duration::from_millis(deadline_ms as u64)
}

fn build_scenarios(cli: &Cli, default_flow: Flow, fifo_workers: u16) -> Vec<Scenario> {
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

    let flows: Vec<Flow> = match &cli.flow {
        Some(FlowArg(list)) => list.clone(),
        None => vec![default_flow],
    };

    let mut scenarios = Vec::new();
    for tier_cfg in &tiers {
        for &h in &handlers {
            let per_consumer = match h {
                HandlerProfile::Zero => tier_cfg.messages_per_consumer.0,
                HandlerProfile::Fast => tier_cfg.messages_per_consumer.1,
                HandlerProfile::Slow => tier_cfg.messages_per_consumer.2,
                HandlerProfile::Heavy => tier_cfg.messages_per_consumer.3,
            };
            let consumer_counts: &[u16] = match &cli.consumers {
                Some(ConsumersArg(list)) => list,
                None => tier_cfg.consumers,
            };
            for &c in consumer_counts {
                for &flow in &flows {
                    // FIFO worker topology is the shard set, not the consumer
                    // count: every coordinated-group backend pins FIFO
                    // replicas to 1 and spawns one worker per routing shard,
                    // so a consumer sweep re-measures the identical topology
                    // under different labels. Pin the scenario to the actual
                    // worker count and emit it once per (tier, handler,
                    // payload).
                    // Pin flows whose worker topology ignores the consumer
                    // sweep to the worker count they actually run, and emit
                    // them once per (tier, handler, payload): FIFO runs the
                    // backend's shard set (or one task on Kafka), and the DLQ
                    // drain is a single loop. Sweeping them re-measures one
                    // topology under many labels — and sizing their corpus by
                    // the swept count made slow/heavy single-loop scenarios
                    // arithmetically impossible inside the 600 s deadline
                    // ceiling (32 × 50 heavy messages through one loop needs
                    // ≥ 1,600 s).
                    // The publish flows join the pinned set: they run one
                    // sequential publisher loop with no consumers, so a
                    // swept `consumers` label would describe a topology that
                    // never ran. (Their `handler` column only selects the
                    // tier's corpus size — no handler executes.)
                    let consumers = match flow {
                        Flow::ConsumeFifo => fifo_workers,
                        Flow::DlqDrain | Flow::PublishSingle | Flow::PublishBatch => 1,
                        _ => c,
                    };
                    if matches!(
                        flow,
                        Flow::ConsumeFifo
                            | Flow::DlqDrain
                            | Flow::PublishSingle
                            | Flow::PublishBatch
                    ) && scenarios.iter().any(|s: &Scenario| {
                        s.flow == flow && s.tier == tier_cfg.name && s.handler == h
                    }) {
                        continue;
                    }
                    // Broadcast delivers the whole published stream to every
                    // subscriber, so the published corpus is already the
                    // per-worker workload — multiplying it by the subscriber
                    // count would scale each worker's load with the fan-out
                    // width instead of holding it constant.
                    let tier_messages = match flow {
                        Flow::Broadcast => per_consumer,
                        _ => per_consumer.saturating_mul(consumers as u64),
                    };
                    for &payload_bytes in &cli.payload.0 {
                        // Inside the payload loop, because the floor is capped
                        // by bytes and so differs per tier.
                        let messages = tier_messages.max(framework_corpus_floor(
                            flow,
                            h,
                            cli.concurrent,
                            payload_bytes,
                        ));
                        scenarios.push(Scenario {
                            tier: tier_cfg.name,
                            messages,
                            consumers,
                            handler: h,
                            deadline: scenario_deadline(
                                messages,
                                flow.effective_workers(consumers),
                                h,
                            ),
                            concurrent: cli.concurrent,
                            prefetch: cli.prefetch,
                            flow,
                            payload_bytes,
                        });
                    }
                }
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
    /// Return [`Outcome::Reject`] instead of `Ack`. Used only by the
    /// unmeasured fill phase of the DLQ-drain flow, which needs every message
    /// to land in the DLQ before the drain is timed.
    reject: bool,
    /// When set, a message carrying [`SENTINEL_ID`] is a broadcast readiness
    /// probe: it bumps this counter and is acked immediately — no simulated
    /// work, no latency record, no `processed` increment — so warmup
    /// deliveries can never leak into the measurement, even mid-flight.
    attach: Option<Arc<AtomicU64>>,
    /// Measure latency from this handler's own `epoch` instead of from the
    /// message's embedded `published_at_ns`. The DLQ drain needs this: its
    /// messages were stamped against the fill phase's epoch, so subtracting
    /// them from a fresh drain-phase clock compares two unrelated `Instant`
    /// bases and saturates to zero. With the flag, `dispatch` is "drain start →
    /// handler entry" and `e2e` is "drain start → handler completion".
    epoch_relative: bool,
}

impl StressTestHandler {
    fn new(
        epoch: Instant,
        processed: Arc<AtomicU64>,
        recorder: Arc<LatencyRecorder>,
        profile: HandlerProfile,
    ) -> Self {
        Self {
            epoch,
            processed,
            recorder,
            profile,
            reject: false,
            attach: None,
            epoch_relative: false,
        }
    }

    fn with_attach_counter(mut self, attach: Arc<AtomicU64>) -> Self {
        self.attach = Some(attach);
        self
    }

    fn rejecting(mut self) -> Self {
        self.reject = true;
        self
    }

    fn epoch_relative(mut self) -> Self {
        self.epoch_relative = true;
        self
    }

    async fn simulate_work(&self) {
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
    }

    /// Whether `id` is a readiness sentinel rather than corpus. Only true when
    /// this handler is carrying an attach flag: outside a readiness barrier
    /// there is nowhere to record the probe, and silently dropping a message
    /// would understate the drain instead of measuring it.
    fn is_sentinel(&self, id: u64) -> bool {
        self.attach.is_some() && id == SENTINEL_ID
    }

    fn note_attached(&self) {
        if let Some(attach) = &self.attach {
            attach.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn observe(&self, received_at: u64, published_at_ns: u64) {
        let base = if self.epoch_relative {
            0
        } else {
            published_at_ns
        };
        let acked_at = self.epoch.elapsed().as_nanos() as u64;
        self.recorder.record(LatencyRecord {
            enqueue_to_receive_ns: received_at.saturating_sub(base),
            enqueue_to_ack_ns: acked_at.saturating_sub(base),
        });
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

// One impl covers all three topics: they differ only in topology, and every
// one of them carries `StressTestMsg`.
impl<T> MessageHandler<T> for StressTestHandler
where
    T: Topic<Message = StressTestMsg>,
{
    type Context = ();

    async fn handle(&self, msg: StressTestMsg, _meta: MessageMetadata, _: &()) -> Outcome {
        if self.is_sentinel(msg.id) {
            self.note_attached();
            return Outcome::Ack;
        }
        let received_at = self.epoch.elapsed().as_nanos() as u64;
        self.simulate_work().await;
        self.observe(received_at, msg.published_at_ns);
        if self.reject {
            Outcome::Reject
        } else {
            Outcome::Ack
        }
    }

    // `run_dlq` dispatches here, not to `handle` — the DLQ drain flow is
    // measured entirely through this method.
    async fn handle_dead(&self, msg: StressTestMsg, _meta: DeadMessageMetadata, _: &()) {
        let received_at = self.epoch.elapsed().as_nanos() as u64;
        self.simulate_work().await;
        self.observe(received_at, msg.published_at_ns);
    }
}

/// Batch counterpart of [`StressTestHandler`], for Kafka's `run_batch`.
#[derive(Clone)]
pub struct StressBatchHandler {
    inner: StressTestHandler,
}

impl StressBatchHandler {
    pub fn new(inner: StressTestHandler) -> Self {
        Self { inner }
    }
}

impl<T> BatchMessageHandler<T> for StressBatchHandler
where
    T: Topic<Message = StressTestMsg>,
{
    type Context = ();

    async fn handle_batch(
        &self,
        messages: Vec<(StressTestMsg, MessageMetadata)>,
        _: &(),
    ) -> Outcome {
        let received_at = self.inner.epoch.elapsed().as_nanos() as u64;
        // Same split as `handle`: a readiness sentinel bumps the attach flag
        // and nothing else. A batch straddling the barrier is the normal case
        // — the last warmup round's sentinels and the first corpus chunk
        // arrive together — so without this the barrier's own traffic is
        // counted as drained corpus and recorded as latency. Settled before
        // the simulated work so a sentinel-only batch pays none of it.
        let corpus: Vec<&StressTestMsg> = messages
            .iter()
            .map(|(msg, _)| msg)
            .filter(|msg| {
                if self.inner.is_sentinel(msg.id) {
                    self.inner.note_attached();
                    return false;
                }
                true
            })
            .collect();
        if corpus.is_empty() {
            return Outcome::Ack;
        }
        // One simulated unit of work per batch, not per message: a batch
        // handler exists precisely so the per-message cost is amortised, and
        // sleeping per message would measure the sleep rather than batching.
        self.inner.simulate_work().await;
        for msg in corpus {
            self.inner.observe(received_at, msg.published_at_ns);
        }
        Outcome::Ack
    }
}

// ── Backend-supplied flow drivers ───────────────────────────────────────────

/// Purge closure — invoked between scenarios to clear **the topology the next
/// scenario will run against**, handed in as the argument. Three topologies
/// are in play (`StressTestTopic`, `StressSeqTopic`, `StressBroadcastTopic`)
/// and each owns distinct physical resources — main queue, DLQ, hold queues,
/// per-shard queues — so a purge hard-wired to one queue name leaves the other
/// two accumulating leftovers across scenarios. The wrapper derives every
/// physical name from the topology's accessors (`queue()`, `dlq()`,
/// `hold_queues()`, `sequencing()`), and returns `Err` when a clean starting
/// state cannot be established, which fails the scenario rather than measuring
/// a contaminated one. Default is a no-op via [`noop_purge`] — correct only
/// for InMemory, where a fresh client per scenario means fresh queues.
pub type PurgeFn = Box<
    dyn Fn(&'static QueueTopology) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

pub fn noop_purge() -> PurgeFn {
    Box::new(|_topology| Box::pin(async { Ok(()) }))
}

/// Drive one backend's DLQ drain.
///
/// This cannot be generic: `run_dlq` lives on the crate-private `ConsumerImpl`
/// trait and is only reachable through each backend's concrete consumer
/// struct. The client is handed in rather than captured so the drain runs
/// against the *same* connection the fill phase used — which is the only way
/// InMemory works at all, since its queues live inside the client.
///
/// The future resolves to `Err` when the drain loop itself fails (connection,
/// commit, routing), so the scenario reports the real cause instead of
/// waiting out its whole deadline and calling it a timeout.
pub type DlqDrainFn<B> = Box<
    dyn Fn(
            <B as Backend>::Client,
            StressTestHandler,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// Report the DLQ's current depth for [`StressTestTopic`].
///
/// Needed only where dead-lettering is asynchronous. On every shove-routed
/// backend a `Reject` publishes to the DLQ before acking, so the fill's
/// handler-invocation count equals the DLQ population. SQS is different: its
/// reject path only resets visibility, and the broker-side redrive policy
/// moves a message after `maxReceiveCount` receives — so the invocation
/// counter counts *attempts* (with duplicates) while the DLQ still fills.
/// When this closure is supplied, the fill phase additionally polls it until
/// the DLQ actually holds the scenario's messages before the drain is timed.
pub type DlqDepthFn =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<u64, String>> + Send>> + Send + Sync>;

/// Declare [`StressTestTopic`] sized for a scenario's consumer count.
///
/// Kafka needs this: the generic declare creates the topic with its default
/// partition count (8), and any consumer past that count is assigned no
/// partition at all — it polls forever and receives nothing, while the row
/// counts it as a worker.
///
/// Used by **both** the batch and the supervisor drivers. It was originally
/// batch-only, which left `consume_parallel` at 16 consumers running 8 real
/// workers behind a row that claimed 16. Nothing surfaced that until the
/// readiness barrier refused to start a window with half the group unassigned;
/// before it, the eight idle pollers were invisible and their absence was
/// simply folded into the throughput number.
///
/// The consumer-group driver does not need it: registering a group declares
/// with partitions sized to the config's `max_consumers`.
pub type ConsumeTopologyFn<B> = Box<
    dyn Fn(<B as Backend>::Client, u16) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// Drive one backend's batch consume. Supplied by Kafka alone — `run_batch`
/// exists on no other backend, so the absence of this closure is what makes
/// `consume_batch` unsupported elsewhere rather than something to fake.
/// Invoked once per scenario consumer, so the closure must be re-callable.
/// Errors surface exactly as for [`DlqDrainFn`].
pub type BatchConsumeFn<B> = Box<
    dyn Fn(
            <B as Backend>::Client,
            StressBatchHandler,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// Knobs a backend binary supplies to the harness.
pub struct HarnessConfig<B: Backend> {
    pub backend_name: &'static str,
    /// Upper bound for computed default prefetch (e.g. SQS caps at 10).
    pub prefetch_cap: u16,
    /// Maximum batch size for `publish_batch` (some backends have SDK limits).
    pub publish_chunk_size: usize,
    /// Drain the main queue between scenarios.
    pub purge: PurgeFn,
    /// Identity of the broker under test, for the results provenance.
    pub broker: BrokerInfo,
    /// Whether an absolute throughput number from this run is safe to
    /// publish. `false` for SQS on LocalStack, which measures LocalStack.
    pub representative: bool,
    pub dlq_drain: Option<DlqDrainFn<B>>,
    /// See [`DlqDepthFn`] — required only where dead-lettering is
    /// asynchronous (SQS).
    pub dlq_depth: Option<DlqDepthFn>,
    pub batch_consume: Option<BatchConsumeFn<B>>,
    /// See [`ConsumeTopologyFn`] — required only where the generic declare
    /// under-partitions for the consumer count (Kafka).
    pub consume_topology: Option<ConsumeTopologyFn<B>>,
    /// How many workers this backend actually runs for the FIFO flow. Most
    /// backends spawn one worker per routing shard ([`SEQ_SHARDS`]); Kafka
    /// runs a single FIFO task over every assigned partition, so its
    /// wrapper sets 1. The scenario's `consumers`, message volume, and
    /// deadline are all derived from this so the row describes the topology
    /// that ran.
    pub fifo_workers: u16,
    _backend: std::marker::PhantomData<fn() -> B>,
}

impl<B: Backend> HarnessConfig<B> {
    pub fn new(backend_name: &'static str) -> Self {
        Self {
            backend_name,
            prefetch_cap: 100,
            publish_chunk_size: 1000,
            purge: noop_purge(),
            broker: BrokerInfo {
                name: backend_name.to_string(),
                version: "unknown".to_string(),
                deployment: "unknown".to_string(),
            },
            representative: true,
            dlq_drain: None,
            dlq_depth: None,
            batch_consume: None,
            consume_topology: None,
            fifo_workers: SEQ_SHARDS,
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

    pub fn with_broker(mut self, name: &str, version: &str, deployment: &str) -> Self {
        self.broker = BrokerInfo {
            name: name.to_string(),
            version: version.to_string(),
            deployment: deployment.to_string(),
        };
        self
    }

    /// Mark this backend's absolute numbers as unsafe to publish.
    pub fn not_representative(mut self) -> Self {
        self.representative = false;
        self
    }

    pub fn with_dlq_drain(mut self, f: DlqDrainFn<B>) -> Self {
        self.dlq_drain = Some(f);
        self
    }

    pub fn with_dlq_depth(mut self, f: DlqDepthFn) -> Self {
        self.dlq_depth = Some(f);
        self
    }

    pub fn with_batch_consume(mut self, f: BatchConsumeFn<B>) -> Self {
        self.batch_consume = Some(f);
        self
    }

    pub fn with_consume_topology(mut self, f: ConsumeTopologyFn<B>) -> Self {
        self.consume_topology = Some(f);
        self
    }

    pub fn with_fifo_workers(mut self, workers: u16) -> Self {
        self.fifo_workers = workers.max(1);
        self
    }
}

// ── Scenario execution ──────────────────────────────────────────────────────

struct ScenarioMetrics {
    throughput: f64,
    latencies: LatencyPercentiles,
    peak_rss_mb: f64,
    cpu_pct: f64,
    duration_secs: f64,
    /// The fixed setup cost this scenario excluded from `duration_secs`, or
    /// `None` when its driver does not separate the two. See [`WindowSplit`].
    setup_secs: Option<f64>,
}

fn default_prefetch(messages: u64, consumers: u16, cap: u16) -> u16 {
    (messages / consumers as u64).clamp(1, cap as u64) as u16
}

/// Lazily build the scenario's messages one publish chunk at a time, so at
/// most `chunk_size × payload_bytes` is ever resident. Materialising the
/// whole corpus up front put the moderate 32-consumer 64 KiB scenario at
/// ~10 GiB before the first publish — an OOM staged inside the measurement
/// window, with the staging buffer's RSS and allocation time billed to the
/// backend. `published_at_ns` is stamped once per chunk, after the
/// chunk's payloads are cloned and immediately before its publish call, so
/// allocation cost is never measured as queue latency.
fn message_chunks(
    messages: u64,
    payload_bytes: usize,
    epoch: Instant,
    chunk_size: usize,
) -> impl Iterator<Item = Vec<StressTestMsg>> {
    let payload = payload_of(payload_bytes);
    let chunk_size = chunk_size.max(1) as u64;
    let mut next_id = 0u64;
    std::iter::from_fn(move || {
        if next_id >= messages {
            return None;
        }
        let end = next_id.saturating_add(chunk_size).min(messages);
        let mut chunk: Vec<StressTestMsg> = (next_id..end)
            .map(|id| StressTestMsg {
                id,
                published_at_ns: 0,
                payload: payload.clone(),
            })
            .collect();
        // One stamp for the whole chunk, taken after the payload clones:
        // stamping per message during construction billed up to a chunk's
        // worth of allocation (64 MiB at the largest tier) to the earliest
        // messages as broker dispatch latency.
        let stamp = epoch.elapsed().as_nanos() as u64;
        for msg in &mut chunk {
            msg.published_at_ns = stamp;
        }
        next_id = end;
        Some(chunk)
    })
}

/// Wait until `processed` reaches `target`, the deadline expires, or the run
/// is cancelled.
async fn await_completion(
    processed: &AtomicU64,
    target: u64,
    scenario: &Scenario,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + scenario.deadline;
    loop {
        if processed.load(Ordering::Relaxed) >= target {
            return Ok(());
        }
        if cancel.is_cancelled() {
            return Err("interrupted".to_string());
        }
        if tokio::time::Instant::now() >= deadline {
            let done = processed.load(Ordering::Relaxed);
            return Err(format!(
                "timeout after {:?}: processed {done} / {target}",
                scenario.deadline
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until the DLQ actually holds the scenario's messages.
///
/// The fill consumers must still be polling while this runs: on SQS a
/// message moves to the DLQ *during a receive attempt* once its receive
/// count exceeds the redrive policy's threshold, so stopping the pollers
/// first freezes the DLQ short of its target forever.
async fn await_dlq_depth(
    depth_of: &DlqDepthFn,
    scenario: &Scenario,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + scenario.deadline;
    loop {
        let depth = depth_of().await.map_err(|e| format!("depth check: {e}"))?;
        if depth >= scenario.messages {
            return Ok(());
        }
        if cancel.is_cancelled() {
            return Err("interrupted".to_string());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "DLQ holds {depth} / {} after {:?}; broker-side redrive has not \
                 dead-lettered every message",
                scenario.messages, scenario.deadline
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Fold a consumer task's join result into the scenario outcome. A worker
/// that erred, panicked, or timed out its drain ran a degraded topology for
/// some part of the measurement — recording that as a clean number would be
/// a lie, so it becomes a scenario failure instead.
fn check_worker_outcome(
    joined: Result<shove::SupervisorOutcome, tokio::task::JoinError>,
) -> Result<(), String> {
    match joined {
        Ok(outcome) if outcome.exit_code() == 0 => Ok(()),
        Ok(outcome) => Err(format!(
            "consumer ended unclean: {} errors, {} panics{}",
            outcome.errors,
            outcome.panics,
            if outcome.timed_out {
                ", drain timed out"
            } else {
                ""
            }
        )),
        Err(e) if e.is_cancelled() => Ok(()),
        Err(e) => Err(format!("consumer task panicked: {e}")),
    }
}

/// [`await_completion`], but for flows whose consumers run as spawned driver
/// tasks that can fail (`run_dlq`, `run_batch`). A driver that returns `Err`
/// fails the scenario immediately with the real cause — without this, a
/// consumer that dies on its first poll burns the entire deadline and is
/// reported as a timeout. A driver that returns `Ok` early is left alone: the
/// processed counter, not task exit, is what completion means.
async fn await_completion_or_driver_error(
    processed: &AtomicU64,
    target: u64,
    scenario: &Scenario,
    cancel: &CancellationToken,
    drivers: &mut tokio::task::JoinSet<Result<(), String>>,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + scenario.deadline;
    loop {
        if processed.load(Ordering::Relaxed) >= target {
            return Ok(());
        }
        if cancel.is_cancelled() {
            return Err("interrupted".to_string());
        }
        if tokio::time::Instant::now() >= deadline {
            let done = processed.load(Ordering::Relaxed);
            return Err(format!(
                "timeout after {:?}: processed {done} / {target}",
                scenario.deadline
            ));
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            Some(joined) = drivers.join_next() => match joined {
                Ok(Err(e)) => return Err(format!("consumer driver failed: {e}")),
                Ok(Ok(())) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => return Err(format!("consumer driver panicked: {e}")),
            },
        }
    }
}

fn finish(
    scenario: &Scenario,
    duration: Duration,
    setup: Option<Duration>,
    resources: ResourceSnapshot,
    latencies: LatencyPercentiles,
) -> ScenarioMetrics {
    ScenarioMetrics {
        throughput: scenario.expected_processed() as f64 / duration.as_secs_f64(),
        latencies,
        peak_rss_mb: resources.peak_rss_mb,
        cpu_pct: resources.cpu_pct,
        duration_secs: duration.as_secs_f64(),
        setup_secs: setup.map(|d| d.as_secs_f64()),
    }
}

/// Stop a sampler that is only started once the measured window opens.
///
/// `None` means setup failed before that happened, so there is no resource
/// window to report — zeroes, not a snapshot of the failed setup. The scenario
/// is being failed anyway; the numbers exist only so the row shape is uniform.
async fn stop_sampler(sampler: Option<ResourceSampler>) -> ResourceSnapshot {
    match sampler {
        Some(sampler) => sampler.stop().await,
        None => ResourceSnapshot {
            peak_rss_mb: 0.0,
            cpu_pct: 0.0,
        },
    }
}

/// How many sentinels a readiness round publishes per worker.
///
/// One per worker is not enough on a partitioned backend: a keyless Kafka
/// record goes to a partition librdkafka picks at random, so a single sentinel
/// reaches one member of an N-member group. With `OVERSUPPLY × N` per round the
/// chance a given partition is still uncovered after one round is
/// `((N-1)/N)^(OVERSUPPLY × N)` — about 1.6% at N = 16 — and the loop simply
/// publishes another round until every flag is set. Oversupply is cheap
/// (sentinels carry an empty payload and are never measured) and a missed round
/// is not, so this is deliberately generous.
const READINESS_OVERSUPPLY: usize = 4;

/// Block until every worker has handled at least one message, and return the
/// wall-clock that took.
///
/// This is what separates a drain measurement from a group-coordination
/// measurement. Consumer tasks are spawned and *then* the corpus is published,
/// so without a barrier the interval from `start` to completion contains the
/// group's join/rebalance latency — a fixed cost, invariant to corpus size. In
/// the CAF-667 Kafka sweep that cost was ~3.2 s at one consumer and ~6.5 s at
/// four and sixteen, and it swamped drains as short as 0.05 s: the published
/// `throughput` was `corpus / rebalance_delay`, understating the real drain
/// rate by up to 62×.
///
/// A fixed sleep is not a substitute and was rejected for the same reason
/// `run_scenario_broadcast` rejected it: it either races the slowest join or
/// costs every scenario the worst case. This waits for the actual event.
///
/// One flag per worker, never a shared total: a shared counter cannot tell one
/// fast worker's ten sentinels from ten workers' one each, which is exactly the
/// state — a single member holding every partition — that the barrier exists to
/// rule out. Sentinels bump only these flags, so a straggler's sentinel
/// arriving mid-measurement is invisible to `processed` and to the recorder.
async fn await_worker_readiness<B, T>(
    publisher: &shove::Publisher<B>,
    epoch: Instant,
    attach_flags: &[Arc<AtomicU64>],
) -> Result<(), String>
where
    B: Backend,
    T: Topic<Message = StressTestMsg>,
{
    let per_round = attach_flags
        .len()
        .saturating_mul(READINESS_OVERSUPPLY)
        .max(1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let round: Vec<StressTestMsg> = (0..per_round)
            .map(|_| StressTestMsg {
                id: SENTINEL_ID,
                published_at_ns: epoch.elapsed().as_nanos() as u64,
                payload: String::new(),
            })
            .collect();
        publisher
            .publish_batch::<T>(&round)
            .await
            .map_err(|e| format!("publish readiness sentinels: {e}"))?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let attached = attach_flags
            .iter()
            .filter(|c| c.load(Ordering::Relaxed) > 0)
            .count();
        if attached == attach_flags.len() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "only {attached} of {} workers were assigned and polling within 30s; \
                 the drain window cannot be separated from group setup",
                attach_flags.len()
            ));
        }
    }
}

/// Publish-only flows. There is no consumer, so `dispatch_*`/`e2e_*` record
/// the latency of the publish call itself — a real measurement rather than a
/// zero standing in for one.
/// Bring the broker to a clean, declared state for topic `T`.
///
/// The order is the point, and it is the same for every scenario driver, which
/// is why this is one function rather than a comment repeated six times.
///
/// Purge **before** declaring. Several backends' purge closures drop the whole
/// topology object rather than just its messages — NATS deletes the stream,
/// Kafka deletes the topics — so purging afterwards leaves every flow that does
/// not re-declare (the publish-only and supervisor paths) talking to something
/// that no longer exists. Skipping the purge entirely is worse and quieter: the
/// previous scenario's leftovers are counted as this one's, which is how a
/// `dlq_drain` number gets measured against a queue it did not fill.
async fn purge_then_declare<B, T>(hcfg: &HarnessConfig<B>, broker: &Broker<B>) -> Result<(), String>
where
    B: Backend,
    T: Topic,
{
    (hcfg.purge)(T::topology())
        .await
        .map_err(|e| format!("purge: {e}"))?;
    broker
        .topology()
        .declare::<T>()
        .await
        .map_err(|e| format!("declare: {e}"))
}

/// [`purge_then_declare`] for [`StressTestTopic`], but sized so every worker the
/// scenario claims can actually be assigned work.
///
/// The generic declare under-partitions Kafka's topic for the consumer count: a
/// plain declare takes the backend default (8 partitions), so a 16-consumer
/// scenario leaves 8 members assigned nothing. They poll, receive nothing, and
/// the row still counts them. Where the backend supplies a
/// [`ConsumeTopologyFn`] this declares with the scenario's consumer count
/// instead; where it does not (every backend but Kafka, none of which
/// partitions this way) it falls back to the generic path.
async fn purge_then_declare_sized<B>(
    hcfg: &HarnessConfig<B>,
    broker: &Broker<B>,
    client: &B::Client,
    consumers: u16,
) -> Result<(), String>
where
    B: Backend,
{
    match &hcfg.consume_topology {
        Some(prepare) => {
            (hcfg.purge)(StressTestTopic::topology())
                .await
                .map_err(|e| format!("purge: {e}"))?;
            prepare(client.clone(), consumers)
                .await
                .map_err(|e| format!("declare: {e}"))
        }
        None => purge_then_declare::<B, StressTestTopic>(hcfg, broker).await,
    }
}

async fn run_scenario_publish<B, Connect, Fut>(
    hcfg: &HarnessConfig<B>,
    scenario: &Scenario,
    connect: &Connect,
) -> Result<ScenarioMetrics, String>
where
    B: Backend,
    Connect: Fn() -> Fut,
    Fut: Future<Output = B::Client>,
{
    let client = connect().await;
    let broker = Broker::<B>::from_client(client.clone());
    purge_then_declare::<B, StressTestTopic>(hcfg, &broker).await?;
    let publisher = broker
        .publisher()
        .await
        .map_err(|e| format!("publisher: {e}"))?;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let sampler = ResourceSampler::start();

    let start = Instant::now();
    let publish = async {
        let chunks = message_chunks(
            scenario.messages,
            scenario.payload_bytes,
            epoch,
            hcfg.publish_chunk_size,
        );
        match scenario.flow {
            Flow::PublishBatch => {
                for chunk in chunks {
                    let sent_at = epoch.elapsed().as_nanos() as u64;
                    publisher
                        .publish_batch::<StressTestTopic>(&chunk)
                        .await
                        .map_err(|e| format!("publish_batch: {e}"))?;
                    let done_at = epoch.elapsed().as_nanos() as u64;
                    // One record per call, not per message: the call is the
                    // unit whose latency a batch publisher controls.
                    recorder.record(LatencyRecord {
                        enqueue_to_receive_ns: done_at.saturating_sub(sent_at),
                        enqueue_to_ack_ns: done_at.saturating_sub(sent_at),
                    });
                }
            }
            _ => {
                for chunk in chunks {
                    for msg in &chunk {
                        let sent_at = epoch.elapsed().as_nanos() as u64;
                        publisher
                            .publish::<StressTestTopic>(msg)
                            .await
                            .map_err(|e| format!("publish: {e}"))?;
                        let done_at = epoch.elapsed().as_nanos() as u64;
                        recorder.record(LatencyRecord {
                            enqueue_to_receive_ns: done_at.saturating_sub(sent_at),
                            enqueue_to_ack_ns: done_at.saturating_sub(sent_at),
                        });
                    }
                }
            }
        }
        Ok(())
    };

    // Bounded by the scenario deadline. A publish-only flow has no consumer
    // draining behind it, so on any backend with a bounded queue (InMemory
    // caps at DEFAULT_QUEUE_CAPACITY) a large enough scenario blocks forever
    // on backpressure. Without this it is an unkillable hang that burns the
    // rest of the sweep; with it, it is one recorded failure.
    let result = match tokio::time::timeout(scenario.deadline, publish).await {
        Ok(r) => r,
        Err(_) => Err(format!(
            "publish blocked past {:?}: the queue is full and nothing is consuming it \
             (raise the backend's queue capacity or lower --tier)",
            scenario.deadline
        )),
    };

    let duration = start.elapsed();
    let resources = sampler.stop().await;
    drop(publisher);
    broker.close().await;
    result?;

    let latencies = recorder.compute_percentiles().await;
    // `Some(ZERO)`, not `None`: a publish flow constructs no consumer, so there
    // is no worker startup to wait for and nothing for a barrier to exclude —
    // the window is the publish loop and nothing else. `None` would claim the
    // two were never separated, a different and false statement.
    Ok(finish(
        scenario,
        duration,
        Some(Duration::ZERO),
        resources,
        latencies,
    ))
}

/// Execute a single coordinated-group scenario. A fresh `Broker<B>` is built
/// per scenario because the generic `run_until_timeout` path trips the
/// broker-wide shutdown token once it completes.
///
/// Covers both `consumer_group` (unsequenced, `register`) and `consume_fifo`
/// (`register_fifo` against the sequenced topic).
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
    Fut: Future<Output = B::Client>,
{
    let fifo = scenario.flow == Flow::ConsumeFifo;

    let client = connect().await;
    let broker = Broker::<B>::from_client(client.clone());
    if fifo {
        purge_then_declare::<B, StressSeqTopic>(hcfg, &broker).await?;
    } else {
        purge_then_declare::<B, StressTestTopic>(hcfg, &broker).await?;
    }
    let publisher = broker
        .publisher()
        .await
        .map_err(|e| format!("publisher: {e}"))?;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));

    // Started after the readiness barrier — see `run_scenario_supervisor`.
    let mut sampler: Option<ResourceSampler> = None;

    let prefetch = scenario.prefetch.unwrap_or_else(|| {
        default_prefetch(scenario.messages, scenario.consumers, hcfg.prefetch_cap)
    });

    // The group calls the factory once per member, from inside the spawned
    // `run_until_timeout` task — so the flags cannot be *collected* as members
    // come up: the barrier would read a half-populated vec and conclude that
    // the two members it can see are the whole group. Create the full set up
    // front instead, sized to the member count the row is stamped with, and
    // let each factory call claim one. A group that brings up fewer members
    // than that leaves a flag at zero and the barrier fails the scenario with
    // its real count, which is the loud outcome.
    //
    // FIFO takes no flags, for the reason spelled out in
    // `run_scenario_supervisor`.
    let attach_flags: Vec<Arc<AtomicU64>> = if fifo {
        Vec::new()
    } else {
        (0..scenario.consumers)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect()
    };
    // A std lock, not the tokio one this module imports: the factory is a
    // plain `Fn`, so it cannot await, and this is claimed once per member at
    // spawn with no contention worth an async lock.
    let unclaimed = Arc::new(std::sync::Mutex::new(attach_flags.clone()));
    let pc = processed.clone();
    let rec = recorder.clone();
    let profile = scenario.handler;
    let factory = move || {
        let handler = StressTestHandler::new(epoch, pc.clone(), rec.clone(), profile);
        match unclaimed.lock().ok().and_then(|mut u| u.pop()) {
            Some(attach) => handler.with_attach_counter(attach),
            // FIFO (no flags were made), or a member beyond the stamped count
            // — a respawn topping the group back up, say. Either way it runs
            // without a probe rather than being denied a handler.
            None => handler,
        }
    };

    // Before the registration that brings the members up, for the reason
    // spelled out in `run_scenario_batch`: `setup_secs` runs from the first
    // worker being spawned, and on this flow that is `register`'s own await
    // plus the spawn of the group's run task.
    let setup_started = Instant::now();
    let mut group = broker.consumer_group();
    let inner_cfg = make_cfg(scenario.consumers, prefetch, scenario.concurrent);
    if fifo {
        group
            .register_fifo::<StressSeqTopic, _>(ConsumerGroupConfig::new(inner_cfg), factory)
            .await
            .map_err(|e| e.to_string())?;
    } else {
        group
            .register::<StressTestTopic, _>(ConsumerGroupConfig::new(inner_cfg), factory)
            .await
            .map_err(|e| e.to_string())?;
    }

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

    let setup_and_publish = async {
        // Hold until every member is assigned and polling, so the window below
        // is the drain and not the group's join latency.
        let setup = if fifo {
            None
        } else {
            await_worker_readiness::<B, StressTestTopic>(&publisher, epoch, &attach_flags).await?;
            Some(setup_started.elapsed())
        };

        sampler = Some(ResourceSampler::start());
        let start = Instant::now();
        let chunks = message_chunks(
            scenario.messages,
            scenario.payload_bytes,
            epoch,
            hcfg.publish_chunk_size,
        );
        for chunk in chunks {
            if fifo {
                publisher
                    .publish_batch::<StressSeqTopic>(&chunk)
                    .await
                    .map_err(|e| format!("publish_batch: {e}"))?;
            } else {
                publisher
                    .publish_batch::<StressTestTopic>(&chunk)
                    .await
                    .map_err(|e| format!("publish_batch: {e}"))?;
            }
        }
        Ok::<(Instant, Option<Duration>), String>((start, setup))
    };
    // A publish failure must still fall through to the teardown below: an
    // early `?` here would leave the spawned consumer group running, and a
    // leaked consumer eats the next scenario's messages. Same for a barrier
    // that times out.
    let ((start, setup), outcome) = match setup_and_publish.await {
        Ok(window) => (
            window,
            await_completion(&processed, scenario.expected_processed(), scenario, cancel).await,
        ),
        Err(e) => ((Instant::now(), None), Err(e)),
    };

    let duration = start.elapsed();
    // Stopped with the window — see `run_scenario_batch`.
    let resources = stop_sampler(sampler).await;

    // Signal the consumer group to stop and wait for the drain to complete.
    // A worker that erred or panicked mid-run fails the scenario: the target
    // may still have been reached by the survivors, but on a topology the
    // row does not describe.
    scenario_stop.cancel();
    let outcome = outcome.and(check_worker_outcome(run_handle.await));

    drop(publisher);
    broker.close().await;
    outcome?;

    let latencies = recorder.compute_percentiles().await;
    Ok(finish(scenario, duration, setup, resources, latencies))
}

/// Broadcast: every subscriber receives every message, so `consumers`
/// subscribers process `messages × consumers` in total.
async fn run_scenario_broadcast<B, Connect, Fut>(
    hcfg: &HarnessConfig<B>,
    scenario: &Scenario,
    cancel: &CancellationToken,
    connect: &Connect,
) -> Result<ScenarioMetrics, String>
where
    B: Backend + HasBroadcast,
    Connect: Fn() -> Fut,
    Fut: Future<Output = B::Client>,
{
    let client = connect().await;
    let broker = Broker::<B>::from_client(client.clone());
    purge_then_declare::<B, StressBroadcastTopic>(hcfg, &broker).await?;
    let publisher = broker
        .publisher()
        .await
        .map_err(|e| format!("publisher: {e}"))?;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));
    // Started inside the setup block, after the readiness barrier: sampling
    // from here would fold subscriber setup and barrier idle time into
    // cpu_pct and peak RSS — a resource window that doesn't match the
    // throughput window.
    let mut sampler: Option<ResourceSampler> = None;

    // Each `BroadcastSubscriber` stands in for one process. Subscriptions are
    // ephemeral, so they must all exist before anything is published —
    // anything published earlier is simply not delivered to them.
    //
    // Subscribe and publish inside one fallible block so an error anywhere in
    // it still reaches the teardown below — an early `?` would leave the
    // already-spawned subscribers running into the next scenario.
    let scenario_stop = CancellationToken::new();
    let mut handles = Vec::with_capacity(scenario.consumers as usize);
    // One attach flag per subscriber: the readiness barrier must know that
    // *each* subscription is live, and a shared total cannot tell one fast
    // subscriber's ten sentinels from ten subscribers' one. Sentinels touch
    // only these flags — never `processed`, the recorder, or the simulated
    // workload — so a straggler completing mid-measurement is invisible.
    let mut attach_flags: Vec<Arc<AtomicU64>> = Vec::with_capacity(scenario.consumers as usize);
    let setup_and_publish = async {
        // Before the subscribe loop, for the reason spelled out in
        // `run_scenario_batch`: subscribing and spawning N subscribers is part
        // of the interval `setup_secs` claims to cover.
        let setup_started = Instant::now();
        for _ in 0..scenario.consumers {
            let attach = Arc::new(AtomicU64::new(0));
            attach_flags.push(attach.clone());
            let handler = StressTestHandler::new(
                epoch,
                processed.clone(),
                recorder.clone(),
                scenario.handler,
            )
            .with_attach_counter(attach);
            let mut subscriber = broker.broadcast_subscriber();
            subscriber
                .subscribe::<StressBroadcastTopic, _>(handler, ConsumerOptions::new())
                .map_err(|e| format!("subscribe: {e}"))?;
            let run_stop = scenario_stop.clone();
            handles.push(tokio::spawn(async move {
                subscriber
                    .run_until_timeout(
                        async move { run_stop.cancelled().await },
                        Duration::from_secs(30),
                    )
                    .await
            }));
        }

        // Readiness barrier: publish unmeasured sentinels until every
        // subscriber has seen one. A fixed sleep raced broker attach —
        // ephemeral subscriptions receive nothing published before they
        // attach, so one slow attach made the target unreachable and the
        // scenario a timeout instead of a measurement.
        //
        // This flow had the barrier first; the shared helper is that loop
        // generalised, so the other consume flows now get the same guarantee
        // and the same recorded `setup_secs`.
        await_worker_readiness::<B, StressBroadcastTopic>(&publisher, epoch, &attach_flags).await?;
        let setup = setup_started.elapsed();

        sampler = Some(ResourceSampler::start());
        let start = Instant::now();
        let chunks = message_chunks(
            scenario.messages,
            scenario.payload_bytes,
            epoch,
            hcfg.publish_chunk_size,
        );
        for chunk in chunks {
            publisher
                .publish_batch::<StressBroadcastTopic>(&chunk)
                .await
                .map_err(|e| format!("publish_batch: {e}"))?;
        }
        Ok::<(Instant, Option<Duration>), String>((start, Some(setup)))
    };

    let ((start, setup), outcome) = match setup_and_publish.await {
        Ok(window) => (
            window,
            await_completion(&processed, scenario.expected_processed(), scenario, cancel).await,
        ),
        Err(e) => ((Instant::now(), None), Err(e)),
    };

    let duration = start.elapsed();
    // Stopped with the window — see `run_scenario_batch`.
    let resources = stop_sampler(sampler).await;

    scenario_stop.cancel();
    let mut outcome = outcome;
    for handle in handles {
        outcome = outcome.and(check_worker_outcome(handle.await));
    }

    drop(publisher);
    broker.close().await;
    outcome?;

    let latencies = recorder.compute_percentiles().await;
    Ok(finish(scenario, duration, setup, resources, latencies))
}

/// DLQ drain, in two phases. The fill (publish, then consume with a rejecting
/// handler so every message dead-letters) is **not** measured; only the drain
/// through `run_dlq` is.
///
/// Both phases run on one client, deliberately. InMemory keeps its queues
/// inside the client, so a reconnect between the phases would drain an empty
/// DLQ and report a meaningless number rather than failing.
///
/// The fill uses a [`ConsumerSupervisor`](shove::ConsumerSupervisor) rather
/// than a consumer group for the same reason: the supervisor owns a fresh
/// cancellation token, whereas a group's `run_until_timeout` trips the
/// *broker-wide* token — which is exactly why every other flow builds a new
/// broker per scenario. Tripping it here would kill the client before the
/// drain could use it.
async fn run_scenario_dlq<B, Connect, Fut>(
    hcfg: &HarnessConfig<B>,
    scenario: &Scenario,
    cancel: &CancellationToken,
    connect: &Connect,
) -> Result<ScenarioMetrics, String>
where
    B: Backend,
    Connect: Fn() -> Fut,
    Fut: Future<Output = B::Client>,
{
    let drain = hcfg
        .dlq_drain
        .as_ref()
        .ok_or_else(|| "backend supplied no dlq_drain closure".to_string())?;

    // ── Phase 1: fill the DLQ (unmeasured) ──
    let client = connect().await;
    let broker = Broker::<B>::from_client(client.clone());
    // This driver needs the purge more than any other: it measures the drain of
    // a queue it filled itself, so a leftover DLQ entry from the previous
    // scenario is counted as one of this scenario's N and the drain finishes
    // against messages it never published.
    purge_then_declare::<B, StressTestTopic>(hcfg, &broker).await?;
    let publisher = broker
        .publisher()
        .await
        .map_err(|e| format!("publisher: {e}"))?;

    let fill_epoch = Instant::now();
    let rejected = Arc::new(AtomicU64::new(0));
    let fill_recorder = Arc::new(LatencyRecorder::new());

    // Register and publish inside one fallible block so an error anywhere in
    // it still reaches the teardown — an early `?` would leave the spawned
    // fill consumers running into the next scenario.
    let fill_stop = CancellationToken::new();
    let mut fill_handles = Vec::with_capacity(scenario.consumers as usize);
    let fill = async {
        for _ in 0..scenario.consumers {
            // Zero profile: the fill is setup, so it should cost as little
            // wall-clock as possible whatever the scenario's handler profile is.
            let handler = StressTestHandler::new(
                fill_epoch,
                rejected.clone(),
                fill_recorder.clone(),
                HandlerProfile::Zero,
            )
            .rejecting();
            // `max_retries(0)` so a message dead-letters on its first
            // delivery instead of walking the hold-queue retry chain first.
            // Which side of the handler that happens on differs per backend
            // (RabbitMQ's gate fires before the first attempt, so its
            // handler never runs) — which is why fill completion prefers the
            // DLQ-depth probe over this handler's invocation count.
            let opts = ConsumerOptions::<B>::new()
                .with_max_retries(0)
                .with_prefetch_count(hcfg.prefetch_cap)
                .with_concurrent_processing(scenario.concurrent);
            let mut supervisor = broker.consumer_supervisor();
            supervisor
                .register::<StressTestTopic, _>(handler, opts)
                .map_err(|e| e.to_string())?;
            let run_stop = fill_stop.clone();
            fill_handles.push(tokio::spawn(async move {
                supervisor
                    .run_until_timeout(
                        async move { run_stop.cancelled().await },
                        Duration::from_secs(30),
                    )
                    .await
            }));
        }

        let chunks = message_chunks(
            scenario.messages,
            scenario.payload_bytes,
            fill_epoch,
            hcfg.publish_chunk_size,
        );
        for chunk in chunks {
            publisher
                .publish_batch::<StressTestTopic>(&chunk)
                .await
                .map_err(|e| format!("publish_batch: {e}"))?;
        }
        Ok::<(), String>(())
    };
    let fill_outcome = match fill.await {
        // Where dead-lettering is asynchronous (SQS redrive), the DLQ itself
        // is the completion signal, and it only advances while the fill
        // consumers keep polling — a message moves on a receive attempt, so
        // this must run *before* the consumers are stopped. The invocation
        // counter would fire far too early there: it counts attempts,
        // duplicates included, not dead-letters.
        Ok(()) => match &hcfg.dlq_depth {
            Some(depth_of) => await_dlq_depth(depth_of, scenario, cancel).await,
            None => await_completion(&rejected, scenario.messages, scenario, cancel).await,
        },
        Err(e) => Err(e),
    };
    fill_stop.cancel();
    let mut fill_outcome = fill_outcome;
    for handle in fill_handles {
        fill_outcome = fill_outcome.and(check_worker_outcome(handle.await));
    }
    drop(publisher);
    if let Err(e) = fill_outcome {
        broker.close().await;
        return Err(format!("dlq fill: {e}"));
    }

    // ── Phase 2: drain the DLQ (measured) ──
    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));
    let sampler = ResourceSampler::start();

    // `epoch_relative`: the DLQ'd messages carry `published_at_ns` stamped
    // against the *fill* phase's epoch, which shares no base with this
    // phase's clock — subtracting it would produce saturated-to-zero noise.
    // Drain latency is measured from drain start instead.
    let handler =
        StressTestHandler::new(epoch, processed.clone(), recorder.clone(), scenario.handler)
            .epoch_relative();

    let start = Instant::now();
    let mut drivers = tokio::task::JoinSet::new();
    drivers.spawn(drain(client.clone(), handler));

    let outcome = await_completion_or_driver_error(
        &processed,
        scenario.messages,
        scenario,
        cancel,
        &mut drivers,
    )
    .await;
    let duration = start.elapsed();

    // Abort rather than signal: `run_dlq` takes no shutdown token on several
    // backends (Kafka's stops only when its client closes), so awaiting it
    // would hang. The count is already in `processed`, so nothing is lost.
    drivers.abort_all();
    while drivers.join_next().await.is_some() {}

    let resources = sampler.stop().await;
    broker.close().await;
    outcome?;

    let latencies = recorder.compute_percentiles().await;
    // `None`: this window is not separated from the drain consumer's own
    // startup. The measured phase drains a queue that is *already* full, so
    // there is no publish path left to carry a readiness sentinel through —
    // any message this phase published would land in the main queue, not the
    // DLQ being drained. Saying so demotes this flow's negligible-handler rows
    // to `HandlerCost::SetupBound` instead of letting them keep a framework
    // claim the window cannot support. Filed as a follow-up; it needs a
    // different probe, not this one.
    Ok(finish(scenario, duration, None, resources, latencies))
}

/// Batch consume. Kafka-only: the closure exists nowhere else.
async fn run_scenario_batch<B, Connect, Fut>(
    hcfg: &HarnessConfig<B>,
    scenario: &Scenario,
    cancel: &CancellationToken,
    connect: &Connect,
) -> Result<ScenarioMetrics, String>
where
    B: Backend,
    Connect: Fn() -> Fut,
    Fut: Future<Output = B::Client>,
{
    let batch = hcfg
        .batch_consume
        .as_ref()
        .ok_or_else(|| "backend supplied no batch_consume closure".to_string())?;

    let client = connect().await;
    let broker = Broker::<B>::from_client(client.clone());
    purge_then_declare_sized::<B>(hcfg, &broker, &client, scenario.consumers).await?;
    let publisher = broker
        .publisher()
        .await
        .map_err(|e| format!("publisher: {e}"))?;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));
    // Started after the readiness barrier — see `run_scenario_supervisor`.
    let mut sampler: Option<ResourceSampler> = None;

    // One batch consumer per scenario consumer, sharing the group — the
    // scenario's `consumers` field and `effective_workers` both say N, so N
    // loops must actually run, not one loop wearing N's deadline.
    //
    // The count comes from `effective_workers` rather than from
    // `scenario.consumers` directly so that the loops that run and the
    // deadline they run against cannot drift apart: they are now the same
    // expression. That leaves exactly one thing for a test to pin — that
    // `effective_workers` still returns the consumer count this flow's row is
    // stamped with.
    let mut drivers = tokio::task::JoinSet::new();
    let workers = scenario.flow.effective_workers(scenario.consumers);
    let mut attach_flags: Vec<Arc<AtomicU64>> = Vec::with_capacity(workers as usize);
    // Before the loop, not after it: `setup_secs` is the interval from the
    // first worker being spawned to the barrier being satisfied, and the loop
    // below is inside it. Started after the loop, the wall-clock the driver
    // spends building and spawning N workers landed in neither recorded field —
    // it preceded `setup_secs` and it preceded the drain `start` — so the two
    // could not be added back up to the interval they describe.
    let setup_started = Instant::now();
    for _ in 0..workers {
        let attach = Arc::new(AtomicU64::new(0));
        attach_flags.push(attach.clone());
        let handler = StressBatchHandler::new(
            StressTestHandler::new(epoch, processed.clone(), recorder.clone(), scenario.handler)
                .with_attach_counter(attach),
        );
        drivers.spawn(batch(client.clone(), handler));
    }

    // A publish failure must still fall through to the teardown below rather
    // than early-return past it and leak the running batch consumers. The
    // readiness barrier joins that block for the same reason: a barrier that
    // times out must reach the teardown, not early-return past it.
    let setup_and_publish = async {
        await_worker_readiness::<B, StressTestTopic>(&publisher, epoch, &attach_flags).await?;
        let setup = setup_started.elapsed();

        sampler = Some(ResourceSampler::start());
        let start = Instant::now();
        let chunks = message_chunks(
            scenario.messages,
            scenario.payload_bytes,
            epoch,
            hcfg.publish_chunk_size,
        );
        for chunk in chunks {
            publisher
                .publish_batch::<StressTestTopic>(&chunk)
                .await
                .map_err(|e| format!("publish_batch: {e}"))?;
        }
        Ok::<(Instant, Option<Duration>), String>((start, Some(setup)))
    };
    let ((start, setup), outcome) = match setup_and_publish.await {
        Ok(window) => (
            window,
            await_completion_or_driver_error(
                &processed,
                scenario.messages,
                scenario,
                cancel,
                &mut drivers,
            )
            .await,
        ),
        Err(e) => ((Instant::now(), None), Err(e)),
    };
    let duration = start.elapsed();
    // Stopped with the window, not after teardown, so cpu_pct and peak RSS
    // describe the same interval as `duration_secs`.
    let resources = stop_sampler(sampler).await;

    // Same reasoning as the DLQ drain: `run_batch` has no shutdown token of
    // its own, so abort rather than await.
    drivers.abort_all();
    while drivers.join_next().await.is_some() {}

    drop(publisher);
    broker.close().await;
    outcome?;

    let latencies = recorder.compute_percentiles().await;
    Ok(finish(scenario, duration, setup, resources, latencies))
}

/// Execute a single supervisor scenario (SQS, and the `consume_parallel` /
/// `consume_fifo` flows on any backend). A fresh broker is built per scenario
/// for the same reason as [`run_scenario_group`].
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
    Fut: Future<Output = B::Client>,
{
    let fifo = scenario.flow == Flow::ConsumeFifo;

    let client = connect().await;
    let broker = Broker::<B>::from_client(client.clone());
    if fifo {
        purge_then_declare::<B, StressSeqTopic>(hcfg, &broker).await?;
    } else {
        // Sized, not generic: this flow runs `scenario.consumers` independent
        // supervisors against one shared group, so it needs at least that many
        // partitions for the same reason the batch flow does.
        purge_then_declare_sized::<B>(hcfg, &broker, &client, scenario.consumers).await?;
    }
    let publisher = broker
        .publisher()
        .await
        .map_err(|e| format!("publisher: {e}"))?;

    let epoch = Instant::now();
    let recorder = Arc::new(LatencyRecorder::new());
    let processed = Arc::new(AtomicU64::new(0));

    // Started after the readiness barrier, not here: sampling across worker
    // startup and barrier idle time folds them into cpu_pct and peak RSS, and
    // the resource window then describes a different interval than the
    // throughput window does. Same reasoning as `run_scenario_broadcast`.
    let mut sampler: Option<ResourceSampler> = None;

    let prefetch = scenario.prefetch.unwrap_or_else(|| {
        default_prefetch(scenario.messages, scenario.consumers, hcfg.prefetch_cap)
    });

    // One supervisor per consumer task. `ConsumerSupervisor::register`
    // enforces per-topic uniqueness, so we can't fan out N consumers from
    // a single supervisor for the same topic — N supervisors × 1 register
    // gives equivalent parallelism (each spawns its own `consumer.run` task
    // against the shared queue).
    // Register and publish inside one fallible block so an error anywhere in
    // it still reaches the teardown below — an early `?` would leave the
    // already-spawned supervisors running into the next scenario.
    let scenario_stop = CancellationToken::new();
    // FIFO: one registration — `register_fifo` spawns the whole shard set,
    // and the scenario's `consumers` already names that shard count. N
    // registrations would run N replicas of the set, a topology no other
    // entry point measures under this label.
    let replicas = if fifo { 1 } else { scenario.consumers };
    let mut supervisor_handles = Vec::with_capacity(replicas as usize);
    // One attach flag per replica, for the readiness barrier below. FIFO gets
    // none: its sentinel would route by `id % SEQ_SHARDS`, so one reserved id
    // reaches exactly one shard and can never prove the rest are polling — and
    // `register_fifo` takes a single handler for the whole shard set, so there
    // is no per-worker flag to hang the probe on. That flow therefore keeps an
    // unseparated window and reports `setup_secs: None`, which downgrades its
    // negligible-handler rows to `HandlerCost::SetupBound` rather than letting
    // them keep claiming to measure shove.
    let mut attach_flags: Vec<Arc<AtomicU64>> = Vec::with_capacity(replicas as usize);
    let setup_and_publish = async {
        // Before the registration loop: `setup_secs` runs from the first worker
        // being spawned, and registering N supervisors is part of bringing them
        // up. Started after the loop, that wall-clock fell outside both
        // recorded fields — see `run_scenario_batch`, which had the same gap.
        let setup_started = Instant::now();
        for _ in 0..replicas {
            let mut handler = StressTestHandler::new(
                epoch,
                processed.clone(),
                recorder.clone(),
                scenario.handler,
            );
            if !fifo {
                let attach = Arc::new(AtomicU64::new(0));
                attach_flags.push(attach.clone());
                handler = handler.with_attach_counter(attach);
            }
            let opts = make_opts(prefetch, scenario.concurrent);
            let mut supervisor = broker.consumer_supervisor();
            if fifo {
                supervisor
                    .register_fifo::<StressSeqTopic, _>(handler, opts)
                    .await
                    .map_err(|e| e.to_string())?;
            } else {
                supervisor
                    .register::<StressTestTopic, _>(handler, opts)
                    .map_err(|e| e.to_string())?;
            }
            let run_stop = scenario_stop.clone();
            let handle = tokio::spawn(async move {
                supervisor
                    .run_until_timeout(
                        async move { run_stop.cancelled().await },
                        Duration::from_secs(30),
                    )
                    .await
            });
            supervisor_handles.push(handle);
        }

        // Hold here until every worker is assigned and polling, so the window
        // below is the drain and not the group's join latency.
        let setup = if fifo {
            None
        } else {
            await_worker_readiness::<B, StressTestTopic>(&publisher, epoch, &attach_flags).await?;
            Some(setup_started.elapsed())
        };

        sampler = Some(ResourceSampler::start());
        let start = Instant::now();

        let chunks = message_chunks(
            scenario.messages,
            scenario.payload_bytes,
            epoch,
            hcfg.publish_chunk_size,
        );
        for chunk in chunks {
            if fifo {
                publisher
                    .publish_batch::<StressSeqTopic>(&chunk)
                    .await
                    .map_err(|e| format!("publish_batch: {e}"))?;
            } else {
                publisher
                    .publish_batch::<StressTestTopic>(&chunk)
                    .await
                    .map_err(|e| format!("publish_batch: {e}"))?;
            }
        }
        Ok::<(Instant, Option<Duration>), String>((start, setup))
    };

    let ((start, setup), outcome) = match setup_and_publish.await {
        Ok(window) => (
            window,
            await_completion(&processed, scenario.expected_processed(), scenario, cancel).await,
        ),
        Err(e) => ((Instant::now(), None), Err(e)),
    };

    let duration = start.elapsed();
    // Stopped with the window — see `run_scenario_batch`.
    let resources = stop_sampler(sampler).await;

    // Signal every supervisor to stop and wait for all drains to complete.
    scenario_stop.cancel();
    let mut outcome = outcome;
    for handle in supervisor_handles {
        outcome = outcome.and(check_worker_outcome(handle.await));
    }

    drop(publisher);
    broker.close().await;
    outcome?;

    let latencies = recorder.compute_percentiles().await;
    Ok(finish(scenario, duration, setup, resources, latencies))
}

// ── Reporting ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScenarioResult {
    // The three dimensions added for the results schema. Placed first so a
    // hand-read of the JSON leads with what the row is, then how it scored.
    flow: String,
    mode: String,
    payload_bytes: usize,

    tier: String,
    messages: u64,
    consumers: u16,
    handler: String,
    /// How to read `throughput_msg_per_sec` — see [`HandlerCost`]. Machine
    /// readable on purpose: the sibling `handler` field is the profile's prose
    /// label, and a consumer must not have to parse `"heavy (1-5s)"` and a
    /// flow name to find out that a bar is measuring a simulated sleep.
    ///
    /// `default` is not a fallback that hides a missing marker. It exists so
    /// that a v1 document, whose rows predate the field, still deserializes
    /// far enough for the schema-version check in [`merge_results_file`] to
    /// refuse it by version and say which version it is, rather than failing
    /// earlier with a serde error about a missing field. The refusal is the
    /// same either way; only its legibility differs. A v2 row that arrives
    /// without a marker — or with one that contradicts its own flow and
    /// handler — is refused by [`validate_run`], which re-derives it.
    #[serde(default)]
    handler_cost: String,
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
    /// The measured window. On every consume flow that holds a readiness
    /// barrier this is the **drain alone** — see `setup_secs` for what was
    /// taken out of it, and [`RESULTS_SCHEMA_VERSION`] for why that makes a v2
    /// value non-comparable.
    duration_secs: f64,
    /// The fixed setup cost excluded from `duration_secs`: seconds from the
    /// first worker being spawned to every worker being assigned and polling.
    ///
    /// `null` means this flow does not separate the two, so `duration_secs`
    /// may still contain it — which is why [`HandlerCost::of`] refuses a
    /// `framework` marker on such a row. Recorded rather than merely
    /// subtracted because the number is the interesting part of the defect
    /// this field exists for: it was ~3.2 s at one Kafka consumer and ~6.5 s
    /// at four and sixteen, against drains as short as 0.05 s.
    ///
    /// `default` for the same reason as `handler_cost`: so a v1/v2 document
    /// reaches the version check and is refused by version rather than by a
    /// serde error. Absent therefore reads as "not separated", which is the
    /// truthful reading of a pre-v3 row.
    #[serde(default)]
    setup_secs: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FailedResult {
    flow: String,
    mode: String,
    payload_bytes: usize,
    tier: String,
    messages: u64,
    consumers: u16,
    handler: String,
    error: String,
}

/// A capability hole: a flow this backend genuinely cannot perform.
///
/// Distinct from "not measured this run" — a flow that simply was not selected
/// appears in neither `results` nor here. Emitting a zero for one of these
/// would be a factual error, not a missing measurement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Unsupported {
    pub flow: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub name: String,
    pub version: String,
    pub deployment: String,
}

/// The stdout report. Unchanged in shape from before flows existed, so
/// `--output json` consumers keep working.
#[derive(Debug, Clone, Serialize)]
struct Report {
    backend: String,
    results: Vec<ScenarioResult>,
    failures: Vec<FailedResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackendRun {
    backend: String,
    broker: BrokerInfo,
    representative: bool,
    results: Vec<ScenarioResult>,
    failures: Vec<FailedResult>,
    unsupported: Vec<Unsupported>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Hardware {
    label: String,
    cpu: String,
    physical_cores: u32,
    ram_gb: u32,
    os: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchResults {
    schema_version: u32,
    generated_at: String,
    shove_version: String,
    rust_version: String,
    hardware: Hardware,
    runs: Vec<BackendRun>,
}

// ── Provenance ──────────────────────────────────────────────────────────────

/// UTC RFC 3339, second precision.
fn generated_at() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// The toolchain reporting the results.
///
/// Read at runtime from `rustc --version` (honouring `$RUSTC`) rather than
/// baked in by a `build.rs`: adding a build script to a published crate makes
/// every downstream consumer pay for a benchmark nicety. The caveat is that
/// this reports the toolchain on `PATH` at report time, which is the same one
/// that built the binary in every normal `cargo run` invocation.
fn rust_version() -> String {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    match std::process::Command::new(rustc).arg("--version").output() {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => "unknown".to_string(),
    }
}

fn first_field(content: &str, keys: &[&str]) -> Option<String> {
    first_field_where(content, keys, |_| true)
}

/// The first non-empty value whose key is in `keys` and whose value `accept`s.
///
/// Scans in **key priority order, not file order**: `keys` is a preference
/// list, so an earlier key wins even when a later key's line comes first in the
/// file. `/proc/cpuinfo` opens with `processor : 0` and only then names the
/// `model name`, so file order would hand every caller the fallback spelling it
/// listed last.
fn first_field_where(
    content: &str,
    keys: &[&str],
    accept: impl Fn(&str) -> bool,
) -> Option<String> {
    for want in keys {
        for line in content.lines() {
            let Some((k, v)) = line.split_once(':') else {
                continue;
            };
            if !k.trim().eq_ignore_ascii_case(want) {
                continue;
            }
            let v = v.trim();
            if !v.is_empty() && accept(v) {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// The CPU model out of `/proc/cpuinfo`, or `None` when the kernel does not
/// name one.
///
/// aarch64 kernels expose no `model name`, so several spellings are tried. The
/// last of them, `Processor`, collides case-insensitively with the per-core
/// `processor : 0` index line every architecture emits — which published
/// `"cpu": "0"` as hardware provenance. A model string is never a bare
/// integer, so rejecting one distinguishes the index line from the genuine
/// old-ARM `Processor : ARMv7 Processor rev 3` spelling without dropping it.
/// A pure parser, compiled on every platform so its tests run everywhere;
/// only the Linux `detect_hardware` actually feeds it a real `/proc/cpuinfo`.
fn cpu_model(cpuinfo: &str) -> Option<String> {
    first_field_where(
        cpuinfo,
        &["model name", "Model", "Hardware", "Processor"],
        |v| v.parse::<u64>().is_err(),
    )
}

/// Total physical cores from `/proc/cpuinfo`, counting unique
/// `(physical id, core id)` pairs. The per-block `cpu cores` field is
/// cores-per-socket, so it under-reports every multi-socket host by the
/// socket count. `None` when the kernel exposes neither id (common on ARM),
/// in which case the caller falls back to `available_parallelism`.
fn physical_core_count(cpuinfo: &str) -> Option<u32> {
    let mut pairs = std::collections::BTreeSet::new();
    let mut physical: Option<String> = None;
    let mut core: Option<String> = None;
    for line in cpuinfo.lines() {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        match k.trim() {
            // Each per-CPU block opens with its `processor` line.
            "processor" => {
                physical = None;
                core = None;
            }
            "physical id" => physical = Some(v.trim().to_string()),
            "core id" => core = Some(v.trim().to_string()),
            _ => continue,
        }
        if let (Some(p), Some(c)) = (&physical, &core) {
            pairs.insert((p.clone(), c.clone()));
        }
    }
    (!pairs.is_empty()).then_some(pairs.len() as u32)
}

#[cfg(target_os = "linux")]
fn detect_hardware() -> Hardware {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    // The architecture is the honest floor: it is always true, and it beats
    // "unknown" in a chart caption that exists to date and place a number.
    let cpu = cpu_model(&cpuinfo).unwrap_or_else(|| std::env::consts::ARCH.to_string());

    let physical_cores = physical_core_count(&cpuinfo)
        .or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .map(|n| n.get() as u32)
        })
        .unwrap_or(0);

    let ram_gb = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|m| first_field(&m, &["MemTotal"]))
        .and_then(|v| {
            v.split_whitespace()
                .next()
                .and_then(|n| n.parse::<u64>().ok())
        })
        .map(|kb| (kb / 1024 / 1024) as u32)
        .unwrap_or(0);

    let os = std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|c| {
            c.lines().find_map(|l| {
                l.strip_prefix("PRETTY_NAME=")
                    .map(|v| v.trim_matches('"').to_string())
            })
        })
        .unwrap_or_else(|| "Linux".to_string());

    Hardware {
        label: format!("{cpu} ({physical_cores}c / {ram_gb} GB)"),
        cpu,
        physical_cores,
        ram_gb,
        os,
    }
}

#[cfg(target_os = "macos")]
fn detect_hardware() -> Hardware {
    fn sysctl(key: &str) -> Option<String> {
        let out = std::process::Command::new("sysctl")
            .args(["-n", key])
            .output()
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    let cpu = sysctl("machdep.cpu.brand_string").unwrap_or_else(|| "unknown".to_string());
    let physical_cores = sysctl("hw.physicalcpu")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    let ram_gb = sysctl("hw.memsize")
        .and_then(|v| v.parse::<u64>().ok())
        .map(|b| (b / 1024 / 1024 / 1024) as u32)
        .unwrap_or(0);
    let os = std::process::Command::new("sw_vers")
        .arg("-productVersion")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| format!("macOS {}", String::from_utf8_lossy(&o.stdout).trim()))
        .unwrap_or_else(|| "macOS".to_string());

    Hardware {
        label: format!("{cpu} ({physical_cores}c / {ram_gb} GB)"),
        cpu,
        physical_cores,
        ram_gb,
        os,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn detect_hardware() -> Hardware {
    Hardware {
        label: "unknown".to_string(),
        cpu: "unknown".to_string(),
        physical_cores: std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(0),
        ram_gb: 0,
        os: std::env::consts::OS.to_string(),
    }
}

/// Merge `run` into the document at `path`, replacing any existing entry for
/// the same backend key and preserving the others.
///
/// This is what lets six single-backend binaries accumulate into the one
/// `bench-results.json` the schema names. Provenance is refreshed on every
/// write, so the file's timestamp always describes its newest measurement.
/// The document's own invariants, checked on every run a merge would write —
/// preserved and incoming alike. serde only proves the shape; a shape-valid
/// file with a foreign payload size, a flow in both lists, or a row whose
/// `handler_cost` is not what its flow, handler and recorded window measure
/// must not be re-signed as a valid results document.
fn validate_run(run: &BackendRun) -> Result<(), String> {
    fn check_row(
        backend: &str,
        kind: &str,
        flow: &str,
        mode: &str,
        payload_bytes: usize,
    ) -> Result<Flow, String> {
        let Some(known) = Flow::ALL.iter().find(|f| f.as_str() == flow) else {
            return Err(format!(
                "run '{backend}' has a {kind} with unknown flow '{flow}'"
            ));
        };
        if known.mode().as_str() != mode {
            return Err(format!(
                "run '{backend}' has a {kind} whose mode '{mode}' does not match flow '{flow}'"
            ));
        }
        if !PAYLOAD_SIZES.contains(&payload_bytes) {
            return Err(format!(
                "run '{backend}' has a {kind} with payload_bytes {payload_bytes} outside \
                 {PAYLOAD_SIZES:?}"
            ));
        }
        Ok(*known)
    }

    for r in &run.results {
        let flow = check_row(&run.backend, "result", &r.flow, &r.mode, r.payload_bytes)?;
        // `handler_cost` says what `throughput_msg_per_sec` measures, and it is
        // fully determined by the row's own flow and handler — so it is
        // checkable, not merely present. Re-derive it: a `consume_batch` +
        // `heavy` row stamped `framework` is shape-valid and passes the
        // schema-version check, and preserving it would re-sign into the
        // merged document exactly the mislabelling the field exists to
        // prevent. A missing marker fails here too — `#[serde(default)]` is
        // there so a v1 document reaches the version check, not so a v2 row
        // may decline to say what it measured.
        let Some(handler) = HandlerProfile::from_label(&r.handler) else {
            return Err(format!(
                "run '{}' has a result with unknown handler '{}'",
                run.backend, r.handler
            ));
        };
        let expected = HandlerCost::of(
            flow,
            handler,
            WindowSplit {
                setup_secs: r.setup_secs,
                duration_secs: r.duration_secs,
            },
        );
        if r.handler_cost != expected.as_str() {
            return Err(format!(
                "run '{}' has a result for flow '{}' with handler '{}', setup_secs {:?} and \
                 duration_secs {} whose handler_cost '{}' is not the '{}' that combination \
                 measures",
                run.backend,
                r.flow,
                r.handler,
                r.setup_secs,
                r.duration_secs,
                r.handler_cost,
                expected.as_str()
            ));
        }
    }
    for f in &run.failures {
        // A failure row records a scenario that produced no number, so it
        // carries no `handler_cost` to check.
        check_row(&run.backend, "failure", &f.flow, &f.mode, f.payload_bytes)?;
    }
    let legal_flows: Vec<&str> = Flow::ALL.iter().map(|f| f.as_str()).collect();
    for u in &run.unsupported {
        if !legal_flows.contains(&u.flow.as_str()) {
            return Err(format!(
                "run '{}' lists an unknown flow '{}' as unsupported",
                run.backend, u.flow
            ));
        }
        if u.reason.trim().is_empty() {
            return Err(format!(
                "run '{}' lists flow '{}' as unsupported without a reason",
                run.backend, u.flow
            ));
        }
        if run.results.iter().any(|r| r.flow == u.flow) {
            return Err(format!(
                "run '{}' lists flow '{}' as both measured and unsupported",
                run.backend, u.flow
            ));
        }
    }
    Ok(())
}

fn merge_results_file(
    path: &str,
    run: BackendRun,
    hardware_label: Option<&str>,
) -> Result<(), String> {
    let mut hardware = detect_hardware();
    let rust = rust_version();
    let shove = env!("CARGO_PKG_VERSION");

    let mut existing_label: Option<String> = None;
    let mut existing: Vec<BackendRun> = match std::fs::read_to_string(path) {
        Ok(content) => {
            let doc = serde_json::from_str::<BenchResults>(&content).map_err(|e| {
                format!(
                    "{path} exists but is not a v{RESULTS_SCHEMA_VERSION} results document: {e}"
                )
            })?;
            // A future version's document is shape-compatible enough to
            // deserialize, so without this it would be silently rewritten with
            // a v1 header and whatever fields this binary does not know about
            // dropped. Refuse instead: the six backend binaries accumulate into
            // one file, and a downgrade would corrupt the other five's rows.
            if doc.schema_version != RESULTS_SCHEMA_VERSION {
                return Err(format!(
                    "{path} is a v{} results document; this harness writes v{RESULTS_SCHEMA_VERSION}. \
                     Merging would silently downgrade it — move it aside first.",
                    doc.schema_version
                ));
            }
            // Provenance is document-level, so a merge from a different host,
            // toolchain, or crate build would silently re-attribute every
            // preserved run to this invocation's environment — the exact
            // mislabelling the provenance block exists to prevent. Refuse
            // instead, mirroring the schema-version contract above.
            let mut mismatches = Vec::new();
            if doc.shove_version != shove {
                mismatches.push(format!("shove_version {} != {shove}", doc.shove_version));
            }
            if doc.rust_version != rust {
                mismatches.push(format!("rust_version {} != {rust}", doc.rust_version));
            }
            if doc.hardware.cpu != hardware.cpu {
                mismatches.push(format!("cpu {} != {}", doc.hardware.cpu, hardware.cpu));
            }
            if doc.hardware.os != hardware.os {
                mismatches.push(format!("os {} != {}", doc.hardware.os, hardware.os));
            }
            if doc.hardware.physical_cores != hardware.physical_cores {
                mismatches.push(format!(
                    "physical_cores {} != {}",
                    doc.hardware.physical_cores, hardware.physical_cores
                ));
            }
            if doc.hardware.ram_gb != hardware.ram_gb {
                mismatches.push(format!(
                    "ram_gb {} != {}",
                    doc.hardware.ram_gb, hardware.ram_gb
                ));
            }
            if !mismatches.is_empty() {
                return Err(format!(
                    "{path} was generated in a different environment ({}); merging would \
                     re-attribute its runs to this one — move it aside first.",
                    mismatches.join(", ")
                ));
            }
            existing_label = Some(doc.hardware.label.clone());
            for preserved in &doc.runs {
                validate_run(preserved).map_err(|e| {
                    format!(
                        "{path} holds an invalid v{RESULTS_SCHEMA_VERSION} document ({e}) — \
                         refusing to rewrite it"
                    )
                })?;
            }
            doc.runs
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(format!("read {path}: {e}")),
    };
    validate_run(&run)?;

    existing.retain(|r| r.backend != run.backend);
    existing.push(run);
    existing.sort_by(|a, b| a.backend.cmp(&b.backend));

    // An explicit override names the document; without one, a label an
    // earlier invocation set must survive the merge — resetting it to the
    // detected default would silently relabel every preserved run.
    if let Some(label) = hardware_label {
        hardware.label = label.to_string();
    } else if let Some(label) = existing_label {
        hardware.label = label;
    }

    let doc = BenchResults {
        schema_version: RESULTS_SCHEMA_VERSION,
        generated_at: generated_at(),
        shove_version: shove.to_string(),
        rust_version: rust,
        hardware,
        runs: existing,
    };

    if let Some(parent) = std::path::Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(&doc).map_err(|e| format!("serialize results: {e}"))?;
    std::fs::write(path, json + "\n").map_err(|e| format!("write {path}: {e}"))
}

/// Everything that must match for two rows to be comparable on a scaling
/// curve — that is, every dimension *except* the consumer count being varied.
///
/// Before flows and payloads existed this was `(tier, messages, handler)`.
/// Flow and payload joined the key so a 64 KiB row cannot baseline a 64 B
/// one — and `messages` had to leave it: the total scales *with* the swept
/// consumer count, so keying on it put every row in its own family and the
/// published `scaling_efficiency` was 1.0 everywhere by construction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ScalingKey {
    flow: String,
    payload_bytes: usize,
    tier: String,
    handler: String,
}

impl ScalingKey {
    fn of(r: &ScenarioResult) -> Self {
        Self {
            flow: r.flow.clone(),
            payload_bytes: r.payload_bytes,
            tier: r.tier.clone(),
            handler: r.handler.clone(),
        }
    }
}

fn compute_scaling(results: &mut [ScenarioResult]) {
    let mut baselines: BTreeMap<ScalingKey, (u16, f64)> = BTreeMap::new();
    for r in results.iter() {
        let entry = baselines
            .entry(ScalingKey::of(r))
            .or_insert((r.consumers, r.throughput_msg_per_sec));
        if r.consumers < entry.0 {
            *entry = (r.consumers, r.throughput_msg_per_sec);
        }
    }
    for r in results.iter_mut() {
        if let Some(&(_, baseline)) = baselines.get(&ScalingKey::of(r))
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
        "{:<16} {:>7} {:<10} {:>8} {:>5} {:>8} {:>8}  {:>9} {:>9} {:>9}  {:>9} {:>9} {:>9}  {:>6} {:>7} {:>5}",
        "FLOW",
        "PAYLOAD",
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
    println!("{}", "-".repeat(170));
    for r in &report.results {
        println!(
            "{:<16} {:>7} {:<10} {:>8} {:>5} {:>8} {:>8.0}  {:>8.1}ms {:>8.1}ms {:>8.1}ms  {:>8.1}ms {:>8.1}ms {:>8.1}ms  {:>5.1}x {:>7.1} {:>4.0}%",
            r.flow,
            r.payload_bytes,
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
    println!("publish-only flows report the publish call's own latency in both columns");
    println!("dlq_drain reports latency relative to drain start, not original publish");
    if !report.failures.is_empty() {
        println!("\nFailed scenarios:");
        for f in &report.failures {
            println!(
                "  {} | {}B | {} | {}msg | {}c | {} — {}",
                f.flow, f.payload_bytes, f.tier, f.messages, f.consumers, f.handler, f.error
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

/// Capability holes for a backend reachable through [`run_all_scenarios`]:
/// coordinated groups and broadcast are both available (the bounds prove it),
/// so the only hole is batch consume on the five non-Kafka backends.
fn unsupported_for_group_backend<B: Backend>(hcfg: &HarnessConfig<B>) -> Vec<Unsupported> {
    let mut out = Vec::new();
    if hcfg.batch_consume.is_none() {
        out.push(Unsupported {
            flow: Flow::ConsumeBatch.as_str().to_string(),
            reason: "run_batch is implemented only for the Kafka backend; no other backend \
                     exposes a batch consume primitive"
                .to_string(),
        });
    }
    out
}

/// Capability holes for a supervisor-only backend (SQS).
///
/// `consume_fifo` is deliberately absent: SQS does implement `run_fifo`, and
/// listing it here would misreport a measured capability as a missing one.
fn unsupported_for_supervisor_backend<B: Backend>(hcfg: &HarnessConfig<B>) -> Vec<Unsupported> {
    let mut out = vec![
        Unsupported {
            flow: Flow::ConsumerGroup.as_str().to_string(),
            reason: "SQS does not implement HasCoordinatedGroups — it has no broker-side \
                     group coordinator, so shove uses ConsumerSupervisor (N independent \
                     pollers) instead"
                .to_string(),
        },
        Unsupported {
            flow: Flow::Broadcast.as_str().to_string(),
            reason: "SQS does not implement HasBroadcast — per-process fan-out needs a real \
                     queue plus an SNS subscription whose lifecycle shove does not manage, \
                     and a leaked queue costs money forever"
                .to_string(),
        },
    ];
    if hcfg.batch_consume.is_none() {
        out.push(Unsupported {
            flow: Flow::ConsumeBatch.as_str().to_string(),
            reason: "run_batch is implemented only for the Kafka backend; no other backend \
                     exposes a batch consume primitive"
                .to_string(),
        });
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn finalize_report<B: Backend>(
    mut results: Vec<ScenarioResult>,
    failures: Vec<FailedResult>,
    hcfg: &HarnessConfig<B>,
    cli: &Cli,
    unsupported: Vec<Unsupported>,
    interrupted: bool,
) {
    compute_scaling(&mut results);

    let mut results_file_failed = false;
    if interrupted && cli.results_file.is_some() {
        // The merge replaces this backend's whole entry, so a partial sweep
        // would silently overwrite a complete one and still look
        // publishable. Print what ran, keep the file untouched, exit red.
        eprintln!(
            "interrupted: results file not updated — a partial run must not replace a \
             complete one"
        );
        results_file_failed = true;
    } else if let Some(path) = cli.results_file.as_deref() {
        let run = BackendRun {
            backend: hcfg.backend_name.to_string(),
            broker: hcfg.broker.clone(),
            representative: hcfg.representative,
            results: results.clone(),
            failures: failures.clone(),
            unsupported,
        };
        match merge_results_file(path, run, cli.hardware_label.as_deref()) {
            Ok(()) => eprintln!("wrote results to {path}"),
            Err(e) => {
                eprintln!("failed to write results to {path}: {e}");
                results_file_failed = true;
            }
        }
    }

    let report = Report {
        backend: hcfg.backend_name.to_string(),
        results,
        failures,
    };
    match cli.output {
        OutputFormat::Json => match serde_json::to_string_pretty(&report) {
            Ok(json) => println!("{json}"),
            Err(e) => eprintln!("failed to serialize report: {e}"),
        },
        OutputFormat::Table => {
            print_table(&report);
        }
    }

    // The table is printed first so the run's numbers are never lost to the
    // exit, but a results file the caller asked for and did not get has to be
    // a red run. Task 5's chart-staleness leg regenerates SVGs from this file;
    // exiting 0 with it missing or stale is exactly how a benchmark silently
    // publishes yesterday's numbers. Only reachable when `--results-file` was
    // passed, so no existing invocation changes its exit code.
    if results_file_failed {
        std::process::exit(1);
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
        flow: scenario.flow.as_str().to_string(),
        mode: scenario.flow.mode().as_str().to_string(),
        payload_bytes: scenario.payload_bytes,
        tier: scenario.tier.to_string(),
        messages: scenario.messages,
        consumers: scenario.consumers,
        handler: scenario.handler.to_string(),
        handler_cost: scenario
            .handler_cost(WindowSplit {
                setup_secs: m.setup_secs,
                duration_secs: m.duration_secs,
            })
            .as_str()
            .to_string(),
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
        setup_secs: m.setup_secs,
    });
}

fn push_failure(failures: &mut Vec<FailedResult>, scenario: &Scenario, error: String) {
    eprintln!("  -> FAILED: {error}");
    failures.push(FailedResult {
        flow: scenario.flow.as_str().to_string(),
        mode: scenario.flow.mode().as_str().to_string(),
        payload_bytes: scenario.payload_bytes,
        tier: scenario.tier.to_string(),
        messages: scenario.messages,
        consumers: scenario.consumers,
        handler: scenario.handler.to_string(),
        error,
    });
}

fn announce(hcfg_name: &str, concurrent: bool, count: usize) {
    eprintln!(
        "shove stress benchmarks — {}{}",
        hcfg_name,
        if concurrent { " (concurrent)" } else { "" }
    );
    eprintln!("scenarios: {count}\n");
}

/// Why a selected flow will not be run on this backend, or `None` to run it.
///
/// A flow that cannot run must end up **absent** from `results[]` — never a
/// `0.0` row, and never a `failures[]` row either. A failure means "we tried
/// and it broke", which is a different and much more alarming claim than "this
/// backend has no such primitive". Splitting the two is the whole point of
/// `unsupported[]`, so the split is made here, before anything executes.
fn skip_reason<B: Backend>(
    hcfg: &HarnessConfig<B>,
    flow: Flow,
    supervisor_only: bool,
) -> Option<String> {
    let unsupported = if supervisor_only {
        unsupported_for_supervisor_backend(hcfg)
    } else {
        unsupported_for_group_backend(hcfg)
    };
    if let Some(u) = unsupported.iter().find(|u| u.flow == flow.as_str()) {
        return Some(format!("unsupported: {}", u.reason));
    }
    match (flow, supervisor_only) {
        (Flow::Autoscaler, _) => Some(
            "not part of this harness; the autoscaler is benched by benches/autoscaler.rs"
                .to_string(),
        ),
        // `supervisor` and `consume_parallel` drive the same `run` primitive
        // through the same harness. The results schema assigns the
        // `supervisor` name to SQS and `consume_parallel` to everyone else, so
        // each entry point runs one of them and says why it dropped the other
        // rather than publishing the same measurement twice under two names.
        (Flow::Supervisor, false) => Some(
            "`supervisor` is the SQS spelling of this measurement; `consume_parallel` \
             covers the same `run` primitive on this backend"
                .to_string(),
        ),
        (Flow::ConsumeParallel, true) => Some(
            "`consume_parallel` is spelled `supervisor` on this backend; it is the same \
             `run` primitive through the same harness"
                .to_string(),
        ),
        _ => None,
    }
}

/// Drop the flows this backend will not run, printing one line per dropped
/// flow. Never silent: a scenario that vanishes without explanation reads as
/// "covered" when it was not.
fn filter_scenarios<B: Backend>(
    hcfg: &HarnessConfig<B>,
    scenarios: Vec<Scenario>,
    supervisor_only: bool,
) -> Vec<Scenario> {
    let mut announced: Vec<Flow> = Vec::new();
    let mut kept = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        match skip_reason(hcfg, scenario.flow, supervisor_only) {
            Some(reason) => {
                if !announced.contains(&scenario.flow) {
                    announced.push(scenario.flow);
                    eprintln!("skipping {} — {reason}", scenario.flow);
                }
            }
            None => kept.push(scenario),
        }
    }
    if !announced.is_empty() {
        eprintln!();
    }
    kept
}

/// The default `ConsumerOptions` factory for the `consume_parallel` flow on a
/// backend whose wrapper only supplies a `ConsumerGroupConfig`. Generic, so it
/// works on every backend without the wrapper supplying anything.
fn default_consumer_options<B: Backend>(prefetch: u16, concurrent: bool) -> ConsumerOptions<B> {
    ConsumerOptions::<B>::new()
        .with_prefetch_count(prefetch)
        .with_concurrent_processing(concurrent)
}

fn announce_scenario(i: usize, total: usize, scenario: &Scenario) {
    let prefetch_str = scenario
        .prefetch
        .map(|p| format!(" | pf={p}"))
        .unwrap_or_default();
    eprintln!(
        "[{}/{}] {} | {}B | {} | {}msg | {}c{} | {} ...",
        i + 1,
        total,
        scenario.flow,
        scenario.payload_bytes,
        scenario.tier,
        scenario.messages,
        scenario.consumers,
        prefetch_str,
        scenario.handler,
    );
}

/// Run every selected scenario against a backend implementing
/// `HasCoordinatedGroups + HasBroadcast` (the trait docs are the
/// authoritative list of which backends those are). Each binary supplies:
///
/// * `connect` — builds a fresh `Broker<B>`. Called once per scenario so the
///   broker-wide shutdown-token state tripped by `run_until_timeout` does not
///   bleed between scenarios.
/// * `make_cfg` — turns `(consumers, prefetch, concurrent)` into
///   `B::ConsumerGroupConfig`.
///
/// The `HasBroadcast` bound is what makes the `broadcast` flow reachable here
/// and unreachable for SQS: the exclusion is a compile-time fact, not a
/// runtime skip.
pub async fn run_all_scenarios<B, MkCfg, Connect, Fut>(
    hcfg: HarnessConfig<B>,
    connect: Connect,
    make_cfg: MkCfg,
) where
    B: Backend + HasCoordinatedGroups + HasBroadcast,
    MkCfg: Fn(u16, u16, bool) -> B::ConsumerGroupConfig,
    Connect: Fn() -> Fut,
    Fut: Future<Output = B::Client>,
{
    init_tracing();

    let cli = Cli::parse();
    let cancel = spawn_ctrlc_watcher();
    let scenarios = filter_scenarios(
        &hcfg,
        build_scenarios(&cli, Flow::ConsumerGroup, hcfg.fifo_workers),
        false,
    );

    announce(hcfg.backend_name, cli.concurrent, scenarios.len());

    let mut results: Vec<ScenarioResult> = Vec::new();
    let mut failures: Vec<FailedResult> = Vec::new();

    for (i, scenario) in scenarios.iter().enumerate() {
        if cancel.is_cancelled() {
            eprintln!("skipping remaining scenarios");
            break;
        }
        announce_scenario(i, scenarios.len(), scenario);

        let outcome = match scenario.flow {
            Flow::PublishSingle | Flow::PublishBatch => {
                run_scenario_publish(&hcfg, scenario, &connect).await
            }
            Flow::ConsumerGroup | Flow::ConsumeFifo => {
                run_scenario_group(&hcfg, scenario, &cancel, &make_cfg, &connect).await
            }
            Flow::Broadcast => run_scenario_broadcast(&hcfg, scenario, &cancel, &connect).await,
            Flow::DlqDrain => run_scenario_dlq(&hcfg, scenario, &cancel, &connect).await,
            Flow::ConsumeBatch => run_scenario_batch(&hcfg, scenario, &cancel, &connect).await,
            // `run` through the generic supervisor: no wrapper closure needed,
            // so every backend gets this flow, not just SQS.
            Flow::ConsumeParallel => {
                run_scenario_supervisor(
                    &hcfg,
                    scenario,
                    &cancel,
                    &default_consumer_options::<B>,
                    &connect,
                )
                .await
            }
            // Filtered out before the loop; unreachable in practice.
            Flow::Supervisor | Flow::Autoscaler => {
                Err(format!("{} is not run on this backend", scenario.flow))
            }
        };

        match outcome {
            Ok(m) => push_metrics(&mut results, scenario, m),
            Err(e) => push_failure(&mut failures, scenario, e),
        }
    }

    let unsupported = unsupported_for_group_backend(&hcfg);
    finalize_report(
        results,
        failures,
        &hcfg,
        &cli,
        unsupported,
        cancel.is_cancelled(),
    );
}

/// Run every selected scenario against a supervisor-only backend (SQS). See
/// [`run_all_scenarios`] for the closure contract.
pub async fn run_supervisor_scenarios<B, MkOpts, Connect, Fut>(
    hcfg: HarnessConfig<B>,
    connect: Connect,
    make_opts: MkOpts,
) where
    B: Backend,
    MkOpts: Fn(u16, bool) -> ConsumerOptions<B>,
    Connect: Fn() -> Fut,
    Fut: Future<Output = B::Client>,
{
    init_tracing();

    let cli = Cli::parse();
    let cancel = spawn_ctrlc_watcher();
    // `Flow::Supervisor`, not `ConsumerGroup`: the group flow is unsupported
    // here and would be filtered out, leaving the default invocation running
    // zero scenarios.
    let scenarios = filter_scenarios(
        &hcfg,
        build_scenarios(&cli, Flow::Supervisor, hcfg.fifo_workers),
        true,
    );

    announce(hcfg.backend_name, cli.concurrent, scenarios.len());

    let mut results: Vec<ScenarioResult> = Vec::new();
    let mut failures: Vec<FailedResult> = Vec::new();

    for (i, scenario) in scenarios.iter().enumerate() {
        if cancel.is_cancelled() {
            eprintln!("skipping remaining scenarios");
            break;
        }
        announce_scenario(i, scenarios.len(), scenario);

        let outcome = match scenario.flow {
            Flow::PublishSingle | Flow::PublishBatch => {
                run_scenario_publish(&hcfg, scenario, &connect).await
            }
            Flow::Supervisor | Flow::ConsumeFifo => {
                run_scenario_supervisor(&hcfg, scenario, &cancel, &make_opts, &connect).await
            }
            Flow::ConsumeBatch => run_scenario_batch(&hcfg, scenario, &cancel, &connect).await,
            Flow::DlqDrain => run_scenario_dlq(&hcfg, scenario, &cancel, &connect).await,
            // All filtered out before the loop: `consumer_group` and
            // `broadcast` are capability holes recorded in `unsupported[]`,
            // and the other two are dropped with a printed reason.
            Flow::ConsumerGroup | Flow::Broadcast | Flow::ConsumeParallel | Flow::Autoscaler => {
                Err(format!("{} is not run on this backend", scenario.flow))
            }
        };

        match outcome {
            Ok(m) => push_metrics(&mut results, scenario, m),
            Err(e) => push_failure(&mut failures, scenario, e),
        }
    }

    let unsupported = unsupported_for_supervisor_backend(&hcfg);
    finalize_report(
        results,
        failures,
        &hcfg,
        &cli,
        unsupported,
        cancel.is_cancelled(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use shove::inmemory::{InMemoryConfig, InMemoryConsumer};

    fn cli(tier: &str, handler: &str) -> Cli {
        Cli::parse_from(["stress", "--tier", tier, "--handler", handler])
    }

    fn cli_args(args: &[&str]) -> Cli {
        let mut all = vec!["stress"];
        all.extend_from_slice(args);
        Cli::parse_from(all)
    }

    /// [`build_scenarios`] with the coordinated-group entry point's default
    /// flow, which is what every pre-existing test in this module assumed.
    fn build_scenarios_cg(cli: &Cli) -> Vec<Scenario> {
        build_scenarios(cli, Flow::ConsumerGroup, SEQ_SHARDS)
    }

    // ── Pre-existing scenario-sizing tests ──
    //
    // These four shipped with the harness but never actually ran: Cargo
    // defaults `[[example]]` targets to `test = false`, so no test binary was
    // ever built from this file. They execute now because
    // `tests/bench_harness.rs` includes this module in a real test target.

    #[test]
    fn high_fast_at_32c_yields_320_000_messages() {
        let scenarios = build_scenarios_cg(&cli("high", "fast"));
        let s = scenarios
            .iter()
            .find(|s| s.tier == "high" && s.consumers == 32)
            .expect("high/32c present");
        assert_eq!(s.messages, 320_000);
    }

    #[test]
    fn moderate_heavy_at_16c_yields_800_messages() {
        let scenarios = build_scenarios_cg(&cli("moderate", "heavy"));
        let s = scenarios
            .iter()
            .find(|s| s.tier == "moderate" && s.consumers == 16)
            .expect("moderate/16c present");
        assert_eq!(s.messages, 800);
    }

    #[test]
    fn extreme_fast_at_256c_yields_2_560_000_messages() {
        let scenarios = build_scenarios_cg(&cli("extreme", "fast"));
        let s = scenarios
            .iter()
            .find(|s| s.tier == "extreme" && s.consumers == 256)
            .expect("extreme/256c present");
        assert_eq!(s.messages, 2_560_000);
    }

    #[test]
    fn messages_scale_linearly_with_consumer_count() {
        let scenarios = build_scenarios_cg(&cli("high", "fast"));
        let at_8 = scenarios
            .iter()
            .find(|s| s.tier == "high" && s.consumers == 8)
            .expect("high/8c present");
        let at_64 = scenarios
            .iter()
            .find(|s| s.tier == "high" && s.consumers == 64)
            .expect("high/64c present");
        assert_eq!(at_64.messages, at_8.messages * 8);
    }

    // ── Framework-cost corpus floor ──

    #[test]
    fn a_framework_cell_too_small_to_drain_for_a_second_is_floored() {
        // `moderate` sizes `zero` at 5,000 messages per consumer, so the
        // one-consumer cell published 5,000 — which drained in 0.052 s on
        // Kafka. The barrier makes that window honest; it cannot make it
        // informative. The floor is what gives the cell a drain long enough to
        // earn the `framework` marker instead of being downgraded forever.
        let scenarios = build_scenarios_cg(&cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "zero",
            "--flow",
            "consumer-group",
            "--consumers",
            "1,4,16,32",
            "--payload",
            "64",
        ]));
        let at = |c: u16| {
            scenarios
                .iter()
                .find(|s| s.consumers == c)
                .unwrap_or_else(|| panic!("{c}c present"))
                .messages
        };
        // Raised: the tier's own sizing is below the floor.
        assert_eq!(at(1), MIN_FRAMEWORK_CORPUS_MESSAGES);
        assert_eq!(at(4), MIN_FRAMEWORK_CORPUS_MESSAGES);
        assert_eq!(at(16), MIN_FRAMEWORK_CORPUS_MESSAGES);
        // Untouched: 32 × 5,000 already clears it. The floor only ever raises,
        // so the tiers keep their own shape wherever they are big enough — the
        // per-consumer scaling above the floor is not flattened.
        assert_eq!(at(32), 160_000);
        assert!(at(32) > MIN_FRAMEWORK_CORPUS_MESSAGES);
    }

    #[test]
    fn the_corpus_floor_is_capped_by_bytes_not_just_messages() {
        // 150,000 × 64 KiB would stage a 9.6 GiB corpus through the publisher
        // to buy a window the payload size already provides. The cap is what
        // keeps the floor from multiplying by the payload tier.
        assert_eq!(
            framework_corpus_floor(Flow::ConsumeParallel, HandlerProfile::Zero, true, 64),
            MIN_FRAMEWORK_CORPUS_MESSAGES,
            "the smallest payload is bounded by message count"
        );
        for payload_bytes in [1024, 65536] {
            let floor = framework_corpus_floor(
                Flow::ConsumeParallel,
                HandlerProfile::Zero,
                true,
                payload_bytes,
            );
            assert!(
                floor < MIN_FRAMEWORK_CORPUS_MESSAGES,
                "{payload_bytes} B: floor {floor} was not capped"
            );
            assert!(
                floor * payload_bytes as u64 <= MAX_FRAMEWORK_CORPUS_BYTES,
                "{payload_bytes} B: floor {floor} exceeds the byte cap"
            );
        }
    }

    #[test]
    fn the_corpus_floor_spares_cells_a_bigger_corpus_would_only_slow_down() {
        // A sleeping handler's drain is the sleep, so a 30× corpus buys
        // wall-clock and no signal — and `fast` run sequentially drained at
        // ~470 msg/s, where the floor would turn a 5 s scenario into a 5-minute
        // one. Concurrency hides that sleep, which is why `fast` qualifies
        // there and only there.
        assert_eq!(
            framework_corpus_floor(Flow::ConsumeParallel, HandlerProfile::Fast, false, 64),
            0,
            "sequential fast must not be floored"
        );
        assert_eq!(
            framework_corpus_floor(Flow::ConsumeParallel, HandlerProfile::Fast, true, 64),
            MIN_FRAMEWORK_CORPUS_MESSAGES,
            "concurrent fast is drain-bound and must be floored"
        );
        // Batch mode pays the sleep once per batch, not per message, so the
        // sequential `fast` batch cell is drain-bound too and needs the floor
        // whether or not `--concurrent` is set.
        assert_eq!(
            framework_corpus_floor(Flow::ConsumeBatch, HandlerProfile::Fast, false, 64),
            MIN_FRAMEWORK_CORPUS_MESSAGES,
            "sequential fast batch amortises its sleep and must be floored"
        );
        for handler in [HandlerProfile::Slow, HandlerProfile::Heavy] {
            for concurrent in [false, true] {
                assert_eq!(
                    framework_corpus_floor(Flow::ConsumeParallel, handler, concurrent, 64),
                    0,
                    "{handler} / concurrent={concurrent}"
                );
            }
        }
        // And the flows for which no corpus produces a `framework` row: the
        // two that cannot separate their setup, the two with no drain at all,
        // and broadcast, whose corpus is per-subscriber so a floor multiplies
        // by the fan-out width.
        for flow in [
            Flow::ConsumeFifo,
            Flow::DlqDrain,
            Flow::PublishSingle,
            Flow::PublishBatch,
            Flow::Broadcast,
        ] {
            assert_eq!(
                framework_corpus_floor(flow, HandlerProfile::Zero, true, 64),
                0,
                "{flow}"
            );
        }
    }

    // ── Payload ──

    #[test]
    fn payload_of_produces_exactly_the_requested_byte_count() {
        for size in PAYLOAD_SIZES {
            let p = payload_of(size);
            assert_eq!(p.len(), size, "payload_of({size}) length");
            assert!(
                p.is_ascii(),
                "payload must be ASCII to keep 1 byte per char"
            );
        }
    }

    #[test]
    fn json_encoded_message_grows_by_the_payload_size() {
        // The whole point of a `String` payload over a `Vec<u8>`: the declared
        // `payload_bytes` must be the wire cost, not 3-4x less than it.
        let small = StressTestMsg {
            id: 1,
            published_at_ns: 2,
            payload: payload_of(64),
        };
        let large = StressTestMsg {
            id: 1,
            published_at_ns: 2,
            payload: payload_of(1024),
        };
        let small_len = serde_json::to_vec(&small).expect("encode").len();
        let large_len = serde_json::to_vec(&large).expect("encode").len();
        assert_eq!(large_len - small_len, 1024 - 64);
    }

    #[test]
    fn sequence_key_spreads_ids_over_the_shard_count() {
        let keys: std::collections::BTreeSet<String> = (0..64)
            .map(|id| {
                stress_sequence_key(&StressTestMsg {
                    id,
                    published_at_ns: 0,
                    payload: String::new(),
                })
            })
            .collect();
        assert_eq!(keys.len(), SEQ_SHARDS as usize);
    }

    // ── Flow / mode ──

    #[test]
    fn flow_strings_match_the_results_schema_closed_set() {
        let got: Vec<&str> = Flow::ALL.iter().map(|f| f.as_str()).collect();
        assert_eq!(
            got,
            vec![
                "publish_single",
                "publish_batch",
                "consume_parallel",
                "consume_fifo",
                "consume_batch",
                "consumer_group",
                "supervisor",
                "broadcast",
                "dlq_drain",
            ]
        );
    }

    #[test]
    fn every_flow_round_trips_through_its_cli_spelling() {
        for flow in Flow::ALL {
            assert_eq!(Flow::from_cli(flow.as_cli()), Some(flow), "{flow}");
        }
    }

    #[test]
    fn mode_is_the_chart_grouping_key_not_a_restatement_of_flow() {
        assert_eq!(Flow::ConsumeFifo.mode(), Mode::Fifo);
        assert_eq!(Flow::ConsumeBatch.mode(), Mode::Batch);
        assert_eq!(Flow::PublishBatch.mode(), Mode::Batch);
        assert_eq!(Flow::ConsumerGroup.mode(), Mode::Parallel);
        assert_eq!(Flow::Broadcast.mode(), Mode::Parallel);
    }

    // ── CLI parsing ──

    #[test]
    fn flow_defaults_per_entry_point_so_existing_invocations_are_unchanged() {
        let c = cli("moderate", "zero");
        assert!(c.flow.is_none());
        assert_eq!(c.payload.0, vec![64]);
        assert!(c.results_file.is_none());

        // The group entry point defaults to consumer_group, the supervisor
        // entry point to supervisor — each is what its harness measured
        // before flows were a dimension.
        let group = build_scenarios(&c, Flow::ConsumerGroup, SEQ_SHARDS);
        assert!(group.iter().all(|s| s.flow == Flow::ConsumerGroup));
        let sup = build_scenarios(&c, Flow::Supervisor, SEQ_SHARDS);
        assert!(sup.iter().all(|s| s.flow == Flow::Supervisor));
    }

    #[test]
    fn default_invocation_runs_scenarios_on_both_entry_points() {
        // Regression: a hard default of `consumer-group` was filtered out as
        // unsupported on the supervisor path, so the unchanged SQS invocation
        // ran zero scenarios.
        let c = cli("moderate", "zero");
        let hcfg = HarnessConfig::<shove::InMemory>::new("any");

        let group = filter_scenarios(
            &hcfg,
            build_scenarios(&c, Flow::ConsumerGroup, SEQ_SHARDS),
            false,
        );
        assert!(!group.is_empty(), "group default filtered to nothing");

        let sup = filter_scenarios(
            &hcfg,
            build_scenarios(&c, Flow::Supervisor, SEQ_SHARDS),
            true,
        );
        assert!(!sup.is_empty(), "supervisor default filtered to nothing");
    }

    #[test]
    fn flow_all_selects_every_variant() {
        let c = cli_args(&["--flow", "all"]);
        assert_eq!(c.flow.expect("--flow was given").0, Flow::ALL.to_vec());
    }

    #[test]
    fn payload_rejects_a_size_outside_the_schema_set() {
        let err = "128"
            .parse::<PayloadArg>()
            .expect_err("128 must be rejected");
        assert!(err.contains("not one of"), "{err}");
    }

    #[test]
    fn payload_all_is_exactly_the_schema_set() {
        assert_eq!("all".parse::<PayloadArg>().expect("all").0, PAYLOAD_SIZES);
    }

    #[test]
    fn consumers_override_replaces_the_tier_list() {
        let c = cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "zero",
            "--consumers",
            "1,8,32",
        ]);
        let scenarios = build_scenarios_cg(&c);
        let counts: std::collections::BTreeSet<u16> =
            scenarios.iter().map(|s| s.consumers).collect();
        assert_eq!(counts, [1, 8, 32].into_iter().collect());
    }

    #[test]
    fn consumers_rejects_zero() {
        assert!("0".parse::<ConsumersArg>().is_err());
    }

    // ── Scenario construction ──

    #[test]
    fn every_scenario_carries_a_schema_legal_payload_size() {
        let c = cli_args(&["--flow", "all", "--payload", "all", "--tier", "moderate"]);
        let scenarios = build_scenarios_cg(&c);
        assert!(!scenarios.is_empty());
        for s in &scenarios {
            assert!(
                PAYLOAD_SIZES.contains(&s.payload_bytes),
                "{} carried payload_bytes={}",
                s.flow,
                s.payload_bytes
            );
        }
    }

    #[test]
    fn flow_and_payload_multiply_the_scenario_count() {
        let one = build_scenarios_cg(&cli_args(&["--tier", "moderate", "--handler", "zero"]));
        let many = build_scenarios_cg(&cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "zero",
            "--flow",
            "all",
            "--payload",
            "all",
        ]));
        // Every flow multiplies by consumer counts × payloads, except the
        // pinned flows — FIFO always runs the backend's fixed worker
        // topology, and the DLQ drain and both publish flows are one loop —
        // which emit one row per (tier, handler, payload).
        let swept_flows = Flow::ALL.len() - 4;
        let pinned_flows = 4;
        let expected =
            one.len() * swept_flows * PAYLOAD_SIZES.len() + pinned_flows * PAYLOAD_SIZES.len();
        assert_eq!(many.len(), expected);
    }

    #[test]
    fn fifo_scenarios_are_pinned_to_the_shard_count() {
        // The consumer sweep is meaningless for FIFO: every coordinated-group
        // backend runs exactly one worker per shard regardless of the
        // requested count, so sweeping re-measures one topology under many
        // labels. One row per (tier, handler, payload), sized to the real
        // worker count.
        let scenarios = build_scenarios_cg(&cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "zero",
            "--flow",
            "consume-fifo",
            "--consumers",
            "1,8,32",
        ]));
        assert_eq!(scenarios.len(), 1);
        let s = &scenarios[0];
        assert_eq!(s.consumers, SEQ_SHARDS);
        assert_eq!(s.messages, 5_000 * SEQ_SHARDS as u64);
    }

    #[test]
    fn fifo_scenarios_honor_the_backends_actual_worker_count() {
        // Kafka runs one FIFO task over every assigned partition, so its
        // wrapper reports 1 — the row must claim one worker, size one
        // worker's corpus, and get one worker's deadline.
        let scenarios = build_scenarios(
            &cli_args(&[
                "--tier",
                "moderate",
                "--handler",
                "zero",
                "--flow",
                "consume-fifo",
                "--consumers",
                "1,8,32",
            ]),
            Flow::ConsumerGroup,
            1,
        );
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].consumers, 1);
        assert_eq!(scenarios[0].messages, 5_000);
    }

    #[test]
    fn broadcast_expects_one_delivery_per_subscriber() {
        let c = cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "zero",
            "--flow",
            "broadcast",
            "--consumers",
            "4",
        ]);
        let s = build_scenarios_cg(&c);
        let s = s.first().expect("one scenario");
        assert_eq!(s.expected_processed(), s.messages * 4);
    }

    #[test]
    fn non_broadcast_flows_expect_one_delivery_per_message() {
        let c = cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "zero",
            "--flow",
            "consumer-group",
            "--consumers",
            "4",
        ]);
        let s = build_scenarios_cg(&c);
        let s = s.first().expect("one scenario");
        assert_eq!(s.expected_processed(), s.messages);
    }

    // ── unsupported[] ──

    fn dummy_batch_fn() -> BatchConsumeFn<shove::InMemory> {
        Box::new(|_client, _h| Box::pin(async { Ok(()) }))
    }

    #[test]
    fn supervisor_backend_reports_group_and_broadcast_as_unsupported() {
        let hcfg = HarnessConfig::<shove::InMemory>::new("sqs");
        let u = unsupported_for_supervisor_backend(&hcfg);
        let flows: Vec<&str> = u.iter().map(|x| x.flow.as_str()).collect();
        assert!(flows.contains(&"consumer_group"), "{flows:?}");
        assert!(flows.contains(&"broadcast"), "{flows:?}");
        for entry in &u {
            assert!(
                entry.reason.len() > 20,
                "reason for {} is not a real reason: {:?}",
                entry.flow,
                entry.reason
            );
        }
    }

    #[test]
    fn supervisor_backend_does_not_claim_fifo_is_unsupported() {
        // SQS really does implement `run_fifo`; listing it would misreport a
        // measured capability as a missing one.
        let hcfg = HarnessConfig::<shove::InMemory>::new("sqs");
        let u = unsupported_for_supervisor_backend(&hcfg);
        assert!(!u.iter().any(|x| x.flow == "consume_fifo"), "{u:?}");
    }

    #[test]
    fn batch_consume_is_unsupported_without_a_closure_and_supported_with_one() {
        let without = HarnessConfig::<shove::InMemory>::new("redis");
        assert!(
            unsupported_for_group_backend(&without)
                .iter()
                .any(|x| x.flow == "consume_batch")
        );

        let with =
            HarnessConfig::<shove::InMemory>::new("kafka").with_batch_consume(dummy_batch_fn());
        assert!(
            !unsupported_for_group_backend(&with)
                .iter()
                .any(|x| x.flow == "consume_batch")
        );
    }

    #[test]
    fn a_flow_is_never_both_measured_and_unsupported() {
        let hcfg = HarnessConfig::<shove::InMemory>::new("redis");
        let unsupported = unsupported_for_group_backend(&hcfg);
        // The group entry point never dispatches ConsumeBatch without a
        // closure, so a measured row for it cannot coexist with the entry.
        let measured: Vec<String> = vec![
            Flow::ConsumerGroup.as_str().to_string(),
            Flow::Broadcast.as_str().to_string(),
            Flow::DlqDrain.as_str().to_string(),
        ];
        for u in &unsupported {
            assert!(!measured.contains(&u.flow), "{} appears in both", u.flow);
        }
    }

    #[test]
    fn a_single_loop_flow_is_not_given_an_n_times_tighter_deadline() {
        // `run_dlq` drains on one loop whatever the consumer count is, so
        // sizing its deadline as if 8 consumers shared the work turned a slow
        // but perfectly healthy drain into a recorded timeout.
        for flow in [
            Flow::DlqDrain,
            Flow::Broadcast,
            Flow::PublishSingle,
            Flow::PublishBatch,
        ] {
            assert_eq!(flow.effective_workers(32), 1, "{flow}");
        }
        // `ConsumeBatch` belongs in this list, not the one above: its driver
        // spawns one `run_batch` loop per scenario consumer, and its row is
        // stamped `consumers: N`. A `1` here would size the deadline for a
        // topology that is not the one running, and would contradict the row.
        for flow in [
            Flow::ConsumerGroup,
            Flow::ConsumeParallel,
            Flow::ConsumeBatch,
        ] {
            assert_eq!(flow.effective_workers(32), 32, "{flow}");
        }
    }

    #[test]
    fn fifo_effective_workers_are_capped_at_the_shard_count() {
        // The sequenced topology has SEQ_SHARDS shards; consumers past that
        // cannot make independent progress, so sizing the deadline by the raw
        // consumer count made large slow-handler scenarios up to 32× too
        // tight.
        assert_eq!(Flow::ConsumeFifo.effective_workers(4), 4);
        assert_eq!(Flow::ConsumeFifo.effective_workers(SEQ_SHARDS), SEQ_SHARDS);
        assert_eq!(Flow::ConsumeFifo.effective_workers(32), SEQ_SHARDS);
        assert_eq!(Flow::ConsumeFifo.effective_workers(256), SEQ_SHARDS);
    }

    fn scenario_for(flow: Flow, handler: HandlerProfile) -> Scenario {
        Scenario {
            tier: "moderate",
            messages: 100,
            consumers: 4,
            handler,
            deadline: Duration::from_secs(60),
            concurrent: false,
            prefetch: None,
            flow,
            payload_bytes: 64,
        }
    }

    /// A window that can carry a framework claim: setup was separated out, and
    /// the drain left behind ran long enough to be a rate.
    fn drain_bound() -> WindowSplit {
        WindowSplit {
            setup_secs: Some(3.2),
            duration_secs: 30.0,
        }
    }

    #[test]
    fn a_batch_row_with_a_simulated_sleep_is_marked_amortised_not_framework() {
        // `StressBatchHandler::handle_batch` sleeps once per *batch* while the
        // per-message handler sleeps once per message. That is the right model
        // of a batching sink, but it means a slow-handler batch row beats a
        // slow-handler parallel row by roughly the batch size for reasons that
        // are the simulated sleep, not shove. The row has to say so in a field,
        // because the alternative is a reader inferring it from a flow name.
        for handler in [HandlerProfile::Slow, HandlerProfile::Heavy] {
            assert_eq!(
                scenario_for(Flow::ConsumeBatch, handler).handler_cost(drain_bound()),
                HandlerCost::HandlerAmortised,
                "{handler}"
            );
        }
    }

    #[test]
    fn zero_and_fast_are_framework_cost_cells_in_every_barrier_backed_flow() {
        // The cells where the simulated work is negligible are the only ones
        // that measure shove itself, and they are comparable across flows —
        // which is what makes a batch-vs-parallel chart legitimate at all.
        // "Every consume flow" would overstate it: the two flows that hold no
        // barrier are covered by
        // `a_flow_whose_driver_holds_no_barrier_can_never_be_framework`.
        for flow in BARRIER_FLOWS {
            for handler in [HandlerProfile::Zero, HandlerProfile::Fast] {
                assert_eq!(
                    scenario_for(flow, handler).handler_cost(drain_bound()),
                    HandlerCost::Framework,
                    "{flow} / {handler}"
                );
            }
        }
    }

    /// Every flow that runs a handler, so a marker gate has to hold for all of
    /// them rather than for the two the defect was found on.
    const CONSUME_FLOWS: [Flow; 7] = [
        Flow::ConsumeBatch,
        Flow::ConsumeParallel,
        Flow::ConsumeFifo,
        Flow::ConsumerGroup,
        Flow::Supervisor,
        Flow::Broadcast,
        Flow::DlqDrain,
    ];

    /// The consume flows whose driver holds the readiness barrier, and so can
    /// record a separated `setup_secs` at all.
    ///
    /// Spelled out here rather than derived from
    /// [`Flow::holds_readiness_barrier`] on purpose: a test that asks the code
    /// under test which flows qualify cannot notice that code changing its
    /// mind. `a_flows_barrier_claim_matches_the_drivers_that_hold_one` pins the
    /// two against each other.
    const BARRIER_FLOWS: [Flow; 5] = [
        Flow::ConsumeBatch,
        Flow::ConsumeParallel,
        Flow::ConsumerGroup,
        Flow::Supervisor,
        Flow::Broadcast,
    ];

    /// The two consume flows that always report `setup_secs: null`, because
    /// neither has anywhere to hang a readiness probe — see
    /// `run_scenario_supervisor` (FIFO) and `run_scenario_dlq`.
    const UNSEPARATED_CONSUME_FLOWS: [Flow; 2] = [Flow::ConsumeFifo, Flow::DlqDrain];

    #[test]
    fn a_flows_barrier_claim_matches_the_drivers_that_hold_one() {
        // The predicate decides whether a row may claim `framework`, so it has
        // to keep naming the flows whose drivers actually hold a barrier. A
        // flow added later lands in neither list and fails here, rather than
        // silently inheriting a claim its driver cannot make.
        for flow in Flow::ALL {
            assert_eq!(
                flow.holds_readiness_barrier(),
                BARRIER_FLOWS.contains(&flow),
                "{flow}: the barrier-backed flows are {BARRIER_FLOWS:?}"
            );
        }
        for flow in CONSUME_FLOWS {
            assert!(
                BARRIER_FLOWS.contains(&flow) != UNSEPARATED_CONSUME_FLOWS.contains(&flow),
                "{flow} must be in exactly one of the two lists"
            );
        }
    }

    #[test]
    fn a_flow_whose_driver_holds_no_barrier_can_never_be_framework() {
        // `setup_secs: Some(_)` is a row's claim that its driver held a
        // readiness barrier and took `start` after it. Two flows can never make
        // that claim: `consume_fifo` has no per-worker flag to hang a sentinel
        // on, and `dlq_drain`'s measured phase drains an already-full queue,
        // with no publish path to carry one. Both always emit `None`.
        //
        // The derivation used to consult only the recorded `Option`, with no
        // per-flow knowledge of which flows can produce a `Some`. So a row
        // carrying `flow: consume_fifo`, `setup_secs: 0.0` and a 2 s
        // `duration_secs` re-derived to `framework`, and `validate_run` then
        // certified a window no run of that flow could have produced. The
        // marker's first guarantee is that the driver separated setup from
        // drain, and a flow with no barrier never did.
        for flow in UNSEPARATED_CONSUME_FLOWS {
            for handler in [HandlerProfile::Zero, HandlerProfile::Fast] {
                for setup_secs in [Some(0.0), Some(4.0)] {
                    assert_eq!(
                        HandlerCost::of(
                            flow,
                            handler,
                            WindowSplit {
                                setup_secs,
                                duration_secs: 2.0,
                            }
                        ),
                        HandlerCost::SetupBound,
                        "{flow} / {handler} / setup {setup_secs:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_negligible_handler_alone_does_not_earn_the_framework_marker() {
        // The defect this gate exists for. `handler_cost` was a pure function
        // of `(flow, handler)`, so `zero`/`fast` meant `framework`
        // unconditionally — and `validate_run` re-derived the same answer, so
        // the row was certified rather than caught. A negligible handler says
        // the number is not the *handler's* cost; it does not say the number is
        // shove's. Without a separated window it is neither.
        for flow in CONSUME_FLOWS {
            for handler in [HandlerProfile::Zero, HandlerProfile::Fast] {
                assert_eq!(
                    HandlerCost::of(
                        flow,
                        handler,
                        WindowSplit {
                            setup_secs: None,
                            // Generous on purpose: length cannot rescue a
                            // window whose composition is unknown.
                            duration_secs: 600.0,
                        }
                    ),
                    HandlerCost::SetupBound,
                    "{flow} / {handler}"
                );
            }
        }
    }

    #[test]
    fn the_caf_667_consume_windows_would_no_longer_be_stamped_framework() {
        // The measured (setup, drain) pairs from the Kafka sweep that produced
        // the mislabelled rows, taken as `duration - dispatch_p50` per cell:
        // 5,000 messages through one consumer drained in 0.052 s behind a
        // 3.17 s group join, and 80,000 through sixteen drained in 1.12 s
        // behind 5.52 s. Every one of them was published as `framework`, i.e.
        // as shove's own cost, and the worst understated the real drain rate by
        // 62×.
        // Five of the six fall below the floor even with the barrier in place,
        // because the corpus they drained was too small to occupy one. The
        // sixth — 80,000 messages through sixteen consumers — genuinely drained
        // for 1.122 s and *does* qualify once the setup is out of the window:
        // it was a real ~71,000 msg/s measurement all along, published as
        // 12,045 because the window was six times too long. The gate is not
        // "CAF-667 was worthless", it is "five of these six rows had no
        // measurement in them".
        for (handler, setup, drain, expected) in [
            (HandlerProfile::Zero, 3.17, 0.052, HandlerCost::SetupBound),
            (HandlerProfile::Zero, 6.04, 0.441, HandlerCost::SetupBound),
            (HandlerProfile::Zero, 5.52, 1.122, HandlerCost::Framework),
            (HandlerProfile::Fast, 3.18, 0.048, HandlerCost::SetupBound),
            (HandlerProfile::Fast, 6.13, 0.384, HandlerCost::SetupBound),
            (HandlerProfile::Fast, 5.80, 0.779, HandlerCost::SetupBound),
        ] {
            assert_eq!(
                HandlerCost::of(
                    Flow::ConsumeBatch,
                    handler,
                    WindowSplit {
                        setup_secs: Some(setup),
                        duration_secs: drain,
                    }
                ),
                expected,
                "{handler}: a {drain}s drain behind a {setup}s setup"
            );
        }
    }

    #[test]
    fn framework_needs_both_a_separated_setup_and_a_window_worth_measuring() {
        // The gate is two-sided, and the boundary is the interesting part: a
        // drain exactly at the floor qualifies, one just under it does not.
        // Pinning both directions stops the threshold from being quietly
        // widened into a no-op later.
        let separated = |duration_secs| WindowSplit {
            setup_secs: Some(4.0),
            duration_secs,
        };
        for flow in BARRIER_FLOWS {
            for handler in [HandlerProfile::Zero, HandlerProfile::Fast] {
                assert_eq!(
                    HandlerCost::of(flow, handler, separated(MIN_FRAMEWORK_WINDOW_SECS)),
                    HandlerCost::Framework,
                    "{flow} / {handler} at the floor"
                );
                assert_eq!(
                    HandlerCost::of(
                        flow,
                        handler,
                        separated(MIN_FRAMEWORK_WINDOW_SECS - f64::EPSILON.max(1e-9))
                    ),
                    HandlerCost::SetupBound,
                    "{flow} / {handler} just under the floor"
                );
            }
        }
    }

    #[test]
    fn a_sleeping_handlers_marker_does_not_depend_on_the_window() {
        // `handler_bound` and `handler_amortised` already tell a reader the
        // number is the simulated sleep and not shove, so a setup floor
        // underneath changes nothing they would conclude. Gating these too
        // would relabel rows whose caveat is already stated — and would lose
        // the batch-vs-parallel distinction the markers exist to carry.
        for window in [
            WindowSplit {
                setup_secs: None,
                duration_secs: 0.001,
            },
            WindowSplit {
                setup_secs: Some(600.0),
                duration_secs: 0.001,
            },
        ] {
            assert_eq!(
                HandlerCost::of(Flow::ConsumeBatch, HandlerProfile::Slow, window),
                HandlerCost::HandlerAmortised,
                "{window:?}"
            );
            assert_eq!(
                HandlerCost::of(Flow::ConsumeParallel, HandlerProfile::Heavy, window),
                HandlerCost::HandlerBound,
                "{window:?}"
            );
            // And a publish row consults the window not at all: it builds no
            // consumer, so there is no worker startup for a barrier to exclude.
            assert_eq!(
                HandlerCost::of(Flow::PublishBatch, HandlerProfile::Zero, window),
                HandlerCost::NoHandler,
                "{window:?}"
            );
        }
    }

    #[test]
    fn a_slow_non_batch_row_is_handler_bound_not_amortised() {
        // The distinction a boolean would have lost: a slow parallel row is
        // also dominated by the sleep, but it pays it once per message, so its
        // caveat is not the batch row's caveat.
        for flow in [Flow::ConsumeParallel, Flow::ConsumerGroup, Flow::DlqDrain] {
            assert_eq!(
                scenario_for(flow, HandlerProfile::Slow).handler_cost(drain_bound()),
                HandlerCost::HandlerBound,
                "{flow}"
            );
        }
    }

    #[test]
    fn only_a_batch_mode_flow_can_be_amortised() {
        for flow in Flow::ALL {
            for handler in [
                HandlerProfile::Zero,
                HandlerProfile::Fast,
                HandlerProfile::Slow,
                HandlerProfile::Heavy,
            ] {
                if scenario_for(flow, handler).handler_cost(drain_bound())
                    == HandlerCost::HandlerAmortised
                {
                    assert_eq!(flow.mode(), Mode::Batch, "{flow} is not a batch-mode flow");
                }
            }
        }
    }

    #[test]
    fn publish_flows_report_no_handler_whatever_the_profile() {
        // The publish flows never construct a consumer, so no handler runs and
        // the profile only picks the message count. Deriving the marker from
        // `mode()` alone would label `publish_batch --handler heavy` as
        // amortised work that never executed.
        for flow in [Flow::PublishSingle, Flow::PublishBatch] {
            for handler in [
                HandlerProfile::Zero,
                HandlerProfile::Fast,
                HandlerProfile::Slow,
                HandlerProfile::Heavy,
            ] {
                assert_eq!(
                    scenario_for(flow, handler).handler_cost(drain_bound()),
                    HandlerCost::NoHandler,
                    "{flow} / {handler}"
                );
            }
        }
    }

    #[test]
    fn the_handler_cost_marker_reaches_the_row_and_the_document() {
        let scenario = scenario_for(Flow::ConsumeBatch, HandlerProfile::Heavy);
        let mut rows = Vec::new();
        push_metrics(
            &mut rows,
            &scenario,
            ScenarioMetrics {
                throughput: 1.0,
                latencies: LatencyPercentiles::default(),
                peak_rss_mb: 0.0,
                cpu_pct: 0.0,
                duration_secs: 1.0,
                setup_secs: Some(0.5),
            },
        );
        assert_eq!(rows[0].handler_cost, HandlerCost::HandlerAmortised.as_str());

        let path = temp_path("handler-cost");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();
        let mut run = sample_run("kafka");
        run.results = rows;
        // Kafka is the one backend that measures this flow, so it cannot also
        // carry the `consume_batch` capability hole the fixture defaults to.
        run.unsupported.clear();
        merge_results_file(&p, run, None).expect("write");
        let doc: BenchResults =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        assert_eq!(doc.runs[0].results[0].handler_cost, "handler_amortised");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_v1_document_is_refused_by_version_rather_than_by_a_parse_error() {
        // v1 rows predate `handler_cost`, and the run that produced them is
        // over, so no marker can be recovered for them. They must not be
        // merged with an empty one, and the refusal has to name the version:
        // "missing field handler_cost" would send a reader looking for a bug
        // in this binary rather than at a stale file.
        let path = temp_path("v1-document");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(&p, sample_run("redis"), None).expect("first write");
        let v1 = std::fs::read_to_string(&path)
            .expect("read")
            .replace(
                &format!("\"schema_version\": {RESULTS_SCHEMA_VERSION}"),
                "\"schema_version\": 1",
            )
            .replace("\"handler_cost\": \"framework\",", "");
        assert!(!v1.contains("handler_cost"), "the v1 fixture still has one");
        std::fs::write(&path, &v1).expect("write v1");

        let err = merge_results_file(&p, sample_run("nats"), None).expect_err("must refuse");
        assert!(err.contains("v1"), "{err}");
        assert!(err.contains("move it aside"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).expect("re-read"), v1);

        let _ = std::fs::remove_file(&path);
    }

    /// Forwards each delivery to the batch handler as a one-message batch, so
    /// a backend without a batch primitive can still drive
    /// [`StressBatchHandler`] over a real queue.
    struct OneMessageBatches(StressBatchHandler);

    impl MessageHandler<StressTestTopic> for OneMessageBatches {
        type Context = ();

        async fn handle(&self, msg: StressTestMsg, meta: MessageMetadata, _: &()) -> Outcome {
            BatchMessageHandler::<StressTestTopic>::handle_batch(&self.0, vec![(msg, meta)], &())
                .await
        }
    }

    #[tokio::test]
    async fn batch_flow_runs_exactly_the_workers_its_row_claims() {
        // Three things have to agree about a `consume_batch` row's worker
        // count: the driver that executes it, `effective_workers` (which
        // sizes the deadline), and the `consumers` field stamped on the row.
        // They previously did not — one loop ran while both of the others
        // said N, so the loop was handed N times the messages against a
        // deadline divided by N, and the published row named a topology that
        // never existed. Asserting `effective_workers` alone cannot catch
        // that: only counting the loops that actually start can.
        let workers: u16 = 3;
        let scenario = Scenario {
            tier: "moderate",
            messages: 30,
            consumers: workers,
            handler: HandlerProfile::Zero,
            deadline: Duration::from_secs(30),
            concurrent: false,
            prefetch: None,
            flow: Flow::ConsumeBatch,
            payload_bytes: 64,
        };

        let started = Arc::new(AtomicU64::new(0));
        let counter = started.clone();
        let batch: BatchConsumeFn<shove::InMemory> = Box::new(move |client, handler| {
            let counter = counter.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::Relaxed);
                InMemoryConsumer::new(client)
                    .run::<StressTestTopic, _>(
                        OneMessageBatches(handler),
                        (),
                        ConsumerOptions::new(),
                    )
                    .await
                    .map_err(|e| format!("run: {e}"))
            })
        });

        let hcfg = HarnessConfig::<shove::InMemory>::new("inmemory").with_batch_consume(batch);
        let cancel = CancellationToken::new();
        let metrics = run_scenario_batch(&hcfg, &scenario, &cancel, &|| async {
            <shove::InMemory as Backend>::connect(InMemoryConfig::default())
                .await
                .expect("connect InMemory")
        })
        .await
        .expect("batch scenario");

        let ran = started.load(Ordering::Relaxed);
        assert_eq!(
            ran, workers as u64,
            "the driver started {ran} batch loops for a {workers}-consumer scenario"
        );
        assert_eq!(
            Flow::ConsumeBatch.effective_workers(workers),
            workers,
            "the deadline is sized for a different worker count than the driver runs"
        );

        let mut rows = Vec::new();
        push_metrics(&mut rows, &scenario, metrics);
        assert_eq!(
            rows[0].consumers as u64, ran,
            "the row claims {} workers but {ran} ran",
            rows[0].consumers
        );
    }

    #[tokio::test]
    async fn the_batch_flows_measured_window_excludes_worker_startup() {
        // The defect, end to end. The batch consumers are spawned and *then*
        // the corpus is published, so before the readiness barrier the measured
        // window contained however long the workers took to come up — a fixed
        // cost, invariant to corpus size, which on Kafka was ~3.2 s at one
        // consumer against drains as short as 0.05 s.
        //
        // The stand-in for that cost here is an explicit delay before each
        // consumer starts polling, which is deterministic where a real
        // rebalance is not. What must hold: the delay lands in `setup_secs`,
        // and `duration_secs` is the drain that happened after it.
        const STARTUP: Duration = Duration::from_millis(700);
        let scenario = Scenario {
            tier: "moderate",
            messages: 20,
            consumers: 2,
            handler: HandlerProfile::Zero,
            deadline: Duration::from_secs(30),
            concurrent: false,
            prefetch: None,
            flow: Flow::ConsumeBatch,
            payload_bytes: 64,
        };

        let batch: BatchConsumeFn<shove::InMemory> = Box::new(move |client, handler| {
            Box::pin(async move {
                tokio::time::sleep(STARTUP).await;
                InMemoryConsumer::new(client)
                    .run::<StressTestTopic, _>(
                        OneMessageBatches(handler),
                        (),
                        ConsumerOptions::new(),
                    )
                    .await
                    .map_err(|e| format!("run: {e}"))
            })
        });

        let hcfg = HarnessConfig::<shove::InMemory>::new("inmemory").with_batch_consume(batch);
        let cancel = CancellationToken::new();
        let metrics = run_scenario_batch(&hcfg, &scenario, &cancel, &|| async {
            <shove::InMemory as Backend>::connect(InMemoryConfig::default())
                .await
                .expect("connect InMemory")
        })
        .await
        .expect("batch scenario");

        let setup = metrics
            .setup_secs
            .expect("the barrier must record what it excluded");
        assert!(
            setup >= STARTUP.as_secs_f64(),
            "setup_secs {setup} did not cover the {STARTUP:?} the workers took to start"
        );
        assert!(
            metrics.duration_secs < STARTUP.as_secs_f64(),
            "duration_secs {} still contains worker startup; a 20-message drain \
             cannot take longer than {STARTUP:?}",
            metrics.duration_secs
        );

        // And the row it produces claims to measure shove only if that drain
        // was long enough to be a rate — which a 20-message one is not.
        let mut rows = Vec::new();
        push_metrics(&mut rows, &scenario, metrics);
        assert_eq!(rows[0].handler_cost, HandlerCost::SetupBound.as_str());
        assert!(
            rows[0].setup_secs.is_some(),
            "setup_secs must reach the row"
        );
    }

    /// Wall-clock burned synchronously per worker inside a driver's spawn loop,
    /// standing in for the per-worker registration a real backend pays there.
    ///
    /// Blocking rather than `tokio::time::sleep`: an awaited sleep would yield,
    /// and the loop's cost is precisely the time the driver spends *not*
    /// yielding between spawning one worker and spawning the next. Nothing else
    /// has to make progress during it — the barrier that does is held after the
    /// loop.
    ///
    /// Sized so the loop's total cannot be mistaken for barrier time.
    /// `await_worker_readiness` polls in 200 ms rounds, so two workers at this
    /// cost are five rounds' worth: a `setup_secs` that covers it is covering
    /// the spawn loop, not an unlucky number of sentinel rounds. Pre-fix these
    /// drivers recorded 0.2 s (supervisor) and 0.4 s (batch) here.
    const PER_WORKER_SPAWN: Duration = Duration::from_millis(500);

    #[tokio::test]
    async fn the_batch_flows_setup_window_covers_its_spawn_loop() {
        // `setup_secs` is documented as "seconds from the first worker being
        // spawned to the readiness barrier being satisfied", and `setup_started`
        // was taken *after* the spawn loop had already run — so the loop's own
        // wall-clock fell into neither field. It preceded `setup_secs` and it
        // preceded the drain `start`, which means the two recorded fields could
        // not be added back up to the interval they describe, and a backend
        // whose per-worker registration is slow reported that cost nowhere.
        let workers: u16 = 2;
        let scenario = Scenario {
            tier: "moderate",
            messages: 20,
            consumers: workers,
            handler: HandlerProfile::Zero,
            deadline: Duration::from_secs(30),
            concurrent: false,
            prefetch: None,
            flow: Flow::ConsumeBatch,
            payload_bytes: 64,
        };

        let batch: BatchConsumeFn<shove::InMemory> = Box::new(move |client, handler| {
            // Before the future is even constructed, so this is time spent
            // inside the driver's spawn loop and not inside the spawned task.
            std::thread::sleep(PER_WORKER_SPAWN);
            Box::pin(async move {
                InMemoryConsumer::new(client)
                    .run::<StressTestTopic, _>(
                        OneMessageBatches(handler),
                        (),
                        ConsumerOptions::new(),
                    )
                    .await
                    .map_err(|e| format!("run: {e}"))
            })
        });

        let hcfg = HarnessConfig::<shove::InMemory>::new("inmemory").with_batch_consume(batch);
        let cancel = CancellationToken::new();
        let metrics = run_scenario_batch(&hcfg, &scenario, &cancel, &|| async {
            <shove::InMemory as Backend>::connect(InMemoryConfig::default())
                .await
                .expect("connect InMemory")
        })
        .await
        .expect("batch scenario");

        let spawn_loop = PER_WORKER_SPAWN.as_secs_f64() * workers as f64;
        let setup = metrics
            .setup_secs
            .expect("the barrier must record what it excluded");
        assert!(
            setup >= spawn_loop,
            "setup_secs {setup} does not cover the {spawn_loop}s the driver spent \
             spawning {workers} workers"
        );
        // And the spawn loop stayed out of the drain, which is the half that
        // was already right.
        assert!(
            metrics.duration_secs < PER_WORKER_SPAWN.as_secs_f64(),
            "duration_secs {} contains worker startup",
            metrics.duration_secs
        );
    }

    #[tokio::test]
    async fn the_supervisor_flows_setup_window_covers_its_spawn_loop() {
        // The same defect in the other flow the issue names, and the reason
        // both are pinned: the two flows are compared against each other, so a
        // cost one of them drops and the other keeps would show up as a
        // difference between backends' throughput. `make_opts` is called once
        // per replica inside the registration loop, which makes it the hook a
        // test can hang a measurable per-worker cost on.
        let replicas: u16 = 2;
        let scenario = Scenario {
            tier: "moderate",
            messages: 20,
            consumers: replicas,
            handler: HandlerProfile::Zero,
            deadline: Duration::from_secs(30),
            concurrent: false,
            prefetch: None,
            flow: Flow::ConsumeParallel,
            payload_bytes: 64,
        };

        let hcfg = HarnessConfig::<shove::InMemory>::new("inmemory");
        let cancel = CancellationToken::new();
        let make_opts = |prefetch: u16, concurrent: bool| {
            std::thread::sleep(PER_WORKER_SPAWN);
            default_consumer_options::<shove::InMemory>(prefetch, concurrent)
        };
        let metrics = run_scenario_supervisor(&hcfg, &scenario, &cancel, &make_opts, &|| async {
            <shove::InMemory as Backend>::connect(InMemoryConfig::default())
                .await
                .expect("connect InMemory")
        })
        .await
        .expect("supervisor scenario");

        let spawn_loop = PER_WORKER_SPAWN.as_secs_f64() * replicas as f64;
        let setup = metrics
            .setup_secs
            .expect("the barrier must record what it excluded");
        assert!(
            setup >= spawn_loop,
            "setup_secs {setup} does not cover the {spawn_loop}s the driver spent \
             registering {replicas} supervisors"
        );
        assert!(
            metrics.duration_secs < PER_WORKER_SPAWN.as_secs_f64(),
            "duration_secs {} contains worker startup",
            metrics.duration_secs
        );
    }

    #[tokio::test]
    async fn the_supervisor_flow_declares_a_topology_sized_for_its_workers() {
        // `consume_parallel` runs N independent supervisors against one shared
        // group, so it needs at least N partitions — exactly like the batch
        // flow, which had this hook and this flow did not. On Kafka the generic
        // declare takes the default 8, so a 16-consumer scenario left 8 members
        // assigned no partition: they polled, received nothing, and the row
        // counted them anyway. The readiness barrier is what surfaced it —
        // it refused to open a window with half the group unassigned — and this
        // pins the fix so the hook cannot quietly go back to batch-only.
        let asked_for = Arc::new(AtomicU64::new(0));
        let seen = asked_for.clone();
        let consume_topology: ConsumeTopologyFn<shove::InMemory> =
            Box::new(move |client, consumers| {
                let seen = seen.clone();
                Box::pin(async move {
                    seen.store(consumers as u64, Ordering::Relaxed);
                    Broker::<shove::InMemory>::from_client(client)
                        .topology()
                        .declare::<StressTestTopic>()
                        .await
                        .map_err(|e| format!("declare: {e}"))
                })
            });

        let scenario = Scenario {
            tier: "moderate",
            messages: 10,
            consumers: 3,
            handler: HandlerProfile::Zero,
            deadline: Duration::from_secs(30),
            concurrent: false,
            prefetch: None,
            flow: Flow::ConsumeParallel,
            payload_bytes: 64,
        };
        let hcfg = HarnessConfig::<shove::InMemory>::new("inmemory")
            .with_consume_topology(consume_topology);
        let cancel = CancellationToken::new();
        run_scenario_supervisor(
            &hcfg,
            &scenario,
            &cancel,
            &default_consumer_options::<shove::InMemory>,
            &|| async {
                <shove::InMemory as Backend>::connect(InMemoryConfig::default())
                    .await
                    .expect("connect InMemory")
            },
        )
        .await
        .expect("supervisor scenario");

        assert_eq!(
            asked_for.load(Ordering::Relaxed),
            scenario.consumers as u64,
            "the sized declare was skipped, or sized for the wrong worker count"
        );
    }

    #[tokio::test]
    async fn dlq_drain_latency_is_measured_from_drain_start_not_a_foreign_epoch() {
        // DLQ'd messages carry `published_at_ns` stamped against the fill
        // phase's epoch. The drain handler's clock shares no base with it, so
        // subtracting produced saturated-to-zero (or garbage) latencies. With
        // `epoch_relative`, latency is elapsed-since-drain-start and a real
        // duration must be visible.
        let epoch = Instant::now();
        let recorder = Arc::new(LatencyRecorder::new());
        let processed = Arc::new(AtomicU64::new(0));
        let handler = StressTestHandler::new(
            epoch,
            processed.clone(),
            recorder.clone(),
            HandlerProfile::Zero,
        )
        .epoch_relative();

        std::thread::sleep(Duration::from_millis(5));
        let msg = StressTestMsg {
            id: 1,
            // A fill-epoch-relative stamp far in this clock's future — the
            // old subtraction saturated it to zero.
            published_at_ns: u64::MAX / 2,
            payload: payload_of(64),
        };
        let meta = DeadMessageMetadata::builder(MessageMetadata::builder().build()).build();
        <StressTestHandler as MessageHandler<StressTestTopic>>::handle_dead(
            &handler,
            msg,
            meta,
            &(),
        )
        .await;

        assert_eq!(processed.load(Ordering::Relaxed), 1);
        let p = recorder.compute_percentiles().await;
        assert!(
            p.e2e_p50 >= 4.0,
            "drain latency must be elapsed-since-drain-start, got {}ms",
            p.e2e_p50
        );
    }

    #[test]
    fn dlq_drain_scenarios_are_pinned_to_one_consumer() {
        // The drain is a single loop whatever the sweep says. Sizing its
        // corpus by the swept count made moderate/heavy/32c need ≥ 1,600 s
        // of handler time against the 600 s deadline ceiling — a scenario
        // that could only ever time out.
        let scenarios = build_scenarios_cg(&cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "heavy",
            "--flow",
            "dlq-drain",
            "--consumers",
            "1,8,32",
        ]));
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].consumers, 1);
        assert_eq!(scenarios[0].messages, 50);
    }

    #[test]
    fn broadcast_publishes_a_constant_per_subscriber_corpus() {
        // Every subscriber processes the whole published stream, so the
        // published corpus is already the per-worker workload; scaling it by
        // the fan-out width made heavy scenarios arithmetically impossible.
        let mk = |c: &str| {
            build_scenarios_cg(&cli_args(&[
                "--tier",
                "moderate",
                "--handler",
                "heavy",
                "--flow",
                "broadcast",
                "--consumers",
                c,
            ]))[0]
        };
        assert_eq!(mk("1").messages, mk("32").messages);
        let s = mk("32");
        assert_eq!(s.messages, 50);
        assert_eq!(s.expected_processed(), 50 * 32);
    }

    // ── Unsupported flows are filtered out, not failed ──

    fn all_flow_scenarios() -> Vec<Scenario> {
        build_scenarios_cg(&cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "zero",
            "--flow",
            "all",
            "--consumers",
            "1",
        ]))
    }

    #[test]
    fn an_unsupported_flow_is_dropped_before_it_can_become_a_failure_row() {
        // A `failures[]` row means "we tried and it broke". A capability hole
        // must never produce one, or a chart reader sees a broken benchmark
        // where there is simply no such primitive.
        let hcfg = HarnessConfig::<shove::InMemory>::new("redis");
        let kept = filter_scenarios(&hcfg, all_flow_scenarios(), false);
        assert!(
            !kept.iter().any(|s| s.flow == Flow::ConsumeBatch),
            "consume_batch survived the filter on a backend with no batch closure"
        );
    }

    #[test]
    fn kafka_keeps_consume_batch_once_a_closure_is_supplied() {
        let hcfg =
            HarnessConfig::<shove::InMemory>::new("kafka").with_batch_consume(dummy_batch_fn());
        let kept = filter_scenarios(&hcfg, all_flow_scenarios(), false);
        assert!(kept.iter().any(|s| s.flow == Flow::ConsumeBatch));
    }

    #[test]
    fn supervisor_path_drops_the_two_capability_holes() {
        let hcfg = HarnessConfig::<shove::InMemory>::new("sqs");
        let kept = filter_scenarios(&hcfg, all_flow_scenarios(), true);
        for hole in [Flow::ConsumerGroup, Flow::Broadcast] {
            assert!(!kept.iter().any(|s| s.flow == hole), "{hole} survived");
        }
        assert!(kept.iter().any(|s| s.flow == Flow::Supervisor));
        assert!(kept.iter().any(|s| s.flow == Flow::ConsumeFifo));
    }

    #[test]
    fn group_path_runs_consume_parallel_and_drops_the_sqs_spelling() {
        let hcfg = HarnessConfig::<shove::InMemory>::new("redis");
        let kept = filter_scenarios(&hcfg, all_flow_scenarios(), false);
        assert!(kept.iter().any(|s| s.flow == Flow::ConsumeParallel));
        assert!(!kept.iter().any(|s| s.flow == Flow::Supervisor));
    }

    #[test]
    fn every_dropped_flow_states_a_reason() {
        let hcfg = HarnessConfig::<shove::InMemory>::new("redis");
        for flow in Flow::ALL {
            if let Some(reason) = skip_reason(&hcfg, flow, false) {
                assert!(
                    reason.len() > 20,
                    "{flow} was dropped with a non-reason: {reason:?}"
                );
            }
        }
    }

    #[test]
    fn no_kept_flow_is_also_listed_as_unsupported() {
        for supervisor_only in [false, true] {
            let hcfg = HarnessConfig::<shove::InMemory>::new("b");
            let unsupported = if supervisor_only {
                unsupported_for_supervisor_backend(&hcfg)
            } else {
                unsupported_for_group_backend(&hcfg)
            };
            let kept = filter_scenarios(&hcfg, all_flow_scenarios(), supervisor_only);
            for s in &kept {
                assert!(
                    !unsupported.iter().any(|u| u.flow == s.flow.as_str()),
                    "{} is both run and declared unsupported",
                    s.flow
                );
            }
        }
    }

    // ── Provenance ──

    #[test]
    fn shove_version_comes_from_cargo_not_a_constant() {
        assert_eq!(env!("CARGO_PKG_VERSION"), shove_version_for_test());
    }

    fn shove_version_for_test() -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }

    #[test]
    fn generated_at_is_utc_rfc_3339() {
        let ts = generated_at();
        let parsed = chrono::DateTime::parse_from_rfc3339(&ts);
        assert!(parsed.is_ok(), "not RFC 3339: {ts}");
        assert!(ts.ends_with('Z'), "not UTC: {ts}");
    }

    #[test]
    fn detected_hardware_is_populated_from_the_host() {
        let hw = detect_hardware();
        assert!(!hw.label.is_empty());
        assert!(!hw.os.is_empty());
        // available_parallelism is a guaranteed fallback on both supported
        // platforms, so a zero here means detection silently failed.
        assert!(hw.physical_cores > 0, "physical_cores not detected");
        // A bare integer is a core index, never a CPU model. See
        // `cpu_model_never_reports_a_core_index` for how that happened.
        assert!(
            hw.cpu.parse::<u64>().is_err(),
            "cpu reported as a bare number: {}",
            hw.cpu
        );
    }

    #[test]
    fn cpu_model_never_reports_a_core_index() {
        // An aarch64 /proc/cpuinfo carries no `model name`, and its per-core
        // `processor : 0` index line matched the `Processor` key
        // case-insensitively — so a real run published `"cpu": "0"` as the
        // hardware provenance. A model string is never a bare integer.
        let aarch64 = "processor\t: 0\nBogoMIPS\t: 50.00\nCPU part\t: 0xd0c\n";
        assert_eq!(cpu_model(aarch64), None);

        // The legitimate old-ARM spelling still has to survive the guard.
        let armv7 = "Processor\t: ARMv7 Processor rev 3 (v7l)\nprocessor\t: 0\n";
        assert_eq!(
            cpu_model(armv7).as_deref(),
            Some("ARMv7 Processor rev 3 (v7l)")
        );

        let x86 = "processor\t: 0\nmodel name\t: AMD EPYC 7R13\n";
        assert_eq!(cpu_model(x86).as_deref(), Some("AMD EPYC 7R13"));
    }

    #[test]
    fn field_keys_are_a_preference_list_not_a_file_order_scan() {
        // `/proc/cpuinfo` names the fallback spelling on line 1 and the
        // preferred one further down, so a file-order scan returns whichever
        // the caller ranked *last*.
        let content = "Hardware\t: Generic ARM\nmodel name\t: AMD EPYC 7R13\n";
        assert_eq!(
            first_field(content, &["model name", "Hardware"]).as_deref(),
            Some("AMD EPYC 7R13")
        );
    }

    // ── Results document ──

    fn sample_run(backend: &str) -> BackendRun {
        BackendRun {
            backend: backend.to_string(),
            broker: BrokerInfo {
                name: backend.to_string(),
                version: "1.2.3".to_string(),
                deployment: "native single-node".to_string(),
            },
            representative: backend != "sqs",
            results: vec![ScenarioResult {
                flow: Flow::ConsumerGroup.as_str().to_string(),
                mode: Mode::Parallel.as_str().to_string(),
                payload_bytes: 1024,
                tier: "moderate".to_string(),
                messages: 100,
                consumers: 1,
                handler: HandlerProfile::Zero.to_string(),
                handler_cost: HandlerCost::Framework.as_str().to_string(),
                throughput_msg_per_sec: 1.0,
                dispatch_p50_ms: 0.0,
                dispatch_p95_ms: 0.0,
                dispatch_p99_ms: 0.0,
                e2e_p50_ms: 0.0,
                e2e_p95_ms: 0.0,
                e2e_p99_ms: 0.0,
                scaling_efficiency: 1.0,
                peak_rss_mb: 0.0,
                cpu_pct: 0.0,
                duration_secs: drain_bound().duration_secs,
                setup_secs: drain_bound().setup_secs,
            }],
            failures: vec![],
            unsupported: vec![Unsupported {
                flow: Flow::ConsumeBatch.as_str().to_string(),
                reason: "run_batch is Kafka-only".to_string(),
            }],
        }
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("shove-bench-{name}-{}.json", std::process::id()));
        p
    }

    #[test]
    fn results_file_merges_by_backend_and_preserves_the_others() {
        let path = temp_path("merge");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(&p, sample_run("redis"), None).expect("first write");
        merge_results_file(&p, sample_run("nats"), None).expect("second write");
        // Re-writing redis must replace, not duplicate.
        merge_results_file(&p, sample_run("redis"), None).expect("third write");

        let doc: BenchResults =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        let backends: Vec<&str> = doc.runs.iter().map(|r| r.backend.as_str()).collect();
        assert_eq!(backends, vec!["nats", "redis"]);
        assert_eq!(doc.schema_version, RESULTS_SCHEMA_VERSION);
        assert_eq!(doc.shove_version, env!("CARGO_PKG_VERSION"));
        assert!(!doc.rust_version.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn merging_into_a_future_schema_version_refuses_instead_of_downgrading() {
        let path = temp_path("schema-version");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(&p, sample_run("redis"), None).expect("first write");
        // Six backend binaries accumulate into one file. A newer one bumping
        // the version must not have its rows silently rewritten with this
        // binary's header by an older binary that ran afterwards.
        let next = RESULTS_SCHEMA_VERSION + 1;
        let bumped = std::fs::read_to_string(&path).expect("read").replace(
            &format!("\"schema_version\": {RESULTS_SCHEMA_VERSION}"),
            &format!("\"schema_version\": {next}"),
        );
        std::fs::write(&path, &bumped).expect("write bumped");

        let err = merge_results_file(&p, sample_run("nats"), None).expect_err("must refuse");
        assert!(err.contains(&format!("v{next}")), "{err}");

        // The refusal must leave the file untouched, not half-written.
        assert_eq!(std::fs::read_to_string(&path).expect("re-read"), bumped);

        let _ = std::fs::remove_file(&path);
    }

    /// A one-row Kafka run carrying `flow` × `handler` as `push_metrics` would
    /// write it. Kafka is the backend that measures `consume_batch`, so it
    /// cannot also carry the capability hole [`sample_run`] defaults to.
    fn batch_run(flow: Flow, handler: HandlerProfile) -> BackendRun {
        let mut run = sample_run("kafka");
        run.unsupported.clear();
        run.results.clear();
        push_metrics(
            &mut run.results,
            &scenario_for(flow, handler),
            ScenarioMetrics {
                throughput: 1.0,
                latencies: LatencyPercentiles::default(),
                peak_rss_mb: 0.0,
                cpu_pct: 0.0,
                // A window that supports a framework claim, so a row's marker
                // here is decided by its flow and handler rather than by a
                // too-short drain — that gate has its own tests.
                duration_secs: drain_bound().duration_secs,
                setup_secs: drain_bound().setup_secs,
            },
        );
        run
    }

    #[test]
    fn a_framework_claim_its_own_recorded_window_cannot_support_is_refused() {
        // The re-derivation is what makes the marker verifiable rather than
        // merely present, and it now has to consult the window — otherwise a
        // row can record a 0.05 s drain behind an unseparated setup and still
        // be certified as "shove's own cost", which is precisely the state
        // CAF-667's published rows were in.
        for (setup_secs, duration_secs, why) in [
            (None, 30.0, "setup never separated from the drain"),
            (Some(6.2), 0.05, "drain far too short to be a rate"),
            (
                Some(-1.0),
                30.0,
                "a negative setup interval was never measured",
            ),
            (
                Some(f64::NAN),
                30.0,
                "a NaN setup interval was never measured",
            ),
        ] {
            let mut run = batch_run(Flow::ConsumeBatch, HandlerProfile::Zero);
            run.results[0].setup_secs = setup_secs;
            run.results[0].duration_secs = duration_secs;
            // What the row would have been stamped, pre-fix.
            run.results[0].handler_cost = HandlerCost::Framework.as_str().to_string();

            let err = validate_run(&run).expect_err(&format!("must refuse: {why}"));
            assert!(err.contains("handler_cost"), "{why}: {err}");
            assert!(err.contains("setup_bound"), "{why}: {err}");

            // And the honest marker for the same row passes, so the refusal is
            // about the claim and not about the row's shape.
            run.results[0].handler_cost = HandlerCost::SetupBound.as_str().to_string();
            validate_run(&run).unwrap_or_else(|e| panic!("{why}: honest marker refused: {e}"));
        }
    }

    #[test]
    fn a_framework_claim_from_a_flow_that_holds_no_barrier_is_refused() {
        // The document-level half of
        // `a_flow_whose_driver_holds_no_barrier_can_never_be_framework`: a row
        // whose `setup_secs` could not have come from its own flow must be
        // refused, not re-derived into agreement. This is the shape a
        // hand-edited or mis-generated document takes — every real run of these
        // two flows writes `null` — and `validate_run` is the check that exists
        // to catch a marker its row cannot support.
        for flow in UNSEPARATED_CONSUME_FLOWS {
            let mut run = batch_run(flow, HandlerProfile::Zero);
            run.results[0].setup_secs = Some(0.0);
            run.results[0].duration_secs = 2.0;
            run.results[0].handler_cost = HandlerCost::Framework.as_str().to_string();

            let err = validate_run(&run)
                .expect_err(&format!("{flow}: a fabricated setup_secs must be refused"));
            assert!(err.contains("handler_cost"), "{flow}: {err}");
            assert!(err.contains("setup_bound"), "{flow}: {err}");

            // The honest marker for the same row passes, so the refusal is
            // about the claim rather than the row's shape.
            run.results[0].handler_cost = HandlerCost::SetupBound.as_str().to_string();
            validate_run(&run).unwrap_or_else(|e| panic!("{flow}: honest marker refused: {e}"));
        }
    }

    #[test]
    fn a_handler_cost_that_contradicts_its_row_is_refused_on_the_way_in() {
        // `handler_cost` is a claim about what the throughput number measures,
        // and it is fully determined by the row's own `flow` and `handler`. A
        // row that claims otherwise is not a shape error serde can catch — it
        // is exactly the mislabelling the field was added to prevent, so it
        // belongs with the other document invariants rather than being taken
        // on trust.
        let mut run = batch_run(Flow::ConsumeBatch, HandlerProfile::Heavy);
        assert_eq!(run.results[0].handler_cost, "handler_amortised");
        run.results[0].handler_cost = HandlerCost::Framework.as_str().to_string();

        let path = temp_path("cost-contradiction");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        let err = merge_results_file(&p, run, None).expect_err("must refuse");
        assert!(err.contains("handler_cost"), "{err}");
        assert!(err.contains("framework"), "{err}");
        assert!(err.contains("handler_amortised"), "{err}");
        assert!(
            !path.exists(),
            "a refused merge must not leave a document behind"
        );
    }

    #[test]
    fn a_missing_or_unknown_handler_cost_is_refused_rather_than_defaulted() {
        // `#[serde(default)]` exists so a v1 document reaches the version
        // check instead of dying on a missing field. It must not become a
        // silent accept of a v2 row with no marker: an empty marker and a
        // typo'd one are both "this row does not say what it measures".
        for bad in ["", "framework_cost", "FRAMEWORK"] {
            let mut run = batch_run(Flow::ConsumerGroup, HandlerProfile::Zero);
            run.results[0].handler_cost = bad.to_string();

            let path = temp_path("cost-unknown");
            let _ = std::fs::remove_file(&path);
            let p = path.to_string_lossy().to_string();

            let err = match merge_results_file(&p, run, None) {
                Ok(()) => panic!("must refuse handler_cost {bad:?}"),
                Err(e) => e,
            };
            assert!(err.contains("handler_cost"), "{bad:?}: {err}");
        }
    }

    #[test]
    fn a_preserved_row_with_a_false_handler_cost_is_not_re_signed() {
        // The merge path is where this bites: six backend binaries accumulate
        // into one file, so a row a stale or buggy producer wrote is read back
        // and re-signed into the merged document. A v2 header is not evidence
        // that the rows under it are honest.
        let path = temp_path("cost-preserved");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(
            &p,
            batch_run(Flow::ConsumeBatch, HandlerProfile::Heavy),
            None,
        )
        .expect("first write");
        let falsified = std::fs::read_to_string(&path).expect("read").replace(
            "\"handler_cost\": \"handler_amortised\"",
            "\"handler_cost\": \"framework\"",
        );
        assert!(falsified.contains("\"framework\""), "fixture did not take");
        std::fs::write(&path, &falsified).expect("write falsified");

        let err = merge_results_file(&p, sample_run("nats"), None).expect_err("must refuse");
        assert!(err.contains("handler_cost"), "{err}");
        assert!(err.contains("refusing to rewrite"), "{err}");
        // The refusal must leave the file untouched, not half-written.
        assert_eq!(std::fs::read_to_string(&path).expect("re-read"), falsified);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_preserved_row_that_carries_no_marker_at_all_is_refused_by_the_row() {
        // The other half of the preserved path, and the one the version check
        // cannot reach: the header still says v2, so nothing upstream objects,
        // and `#[serde(default)]` turns the absent key into an empty marker
        // rather than a parse error. Only the row check stands between that
        // and a re-signed document. Asserted through a real file rather than
        // an in-memory row, because the empty string is produced by
        // deserialization here, not by a caller.
        let path = temp_path("cost-preserved-absent");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(
            &p,
            batch_run(Flow::ConsumeBatch, HandlerProfile::Heavy),
            None,
        )
        .expect("first write");
        let stripped = std::fs::read_to_string(&path)
            .expect("read")
            .replace("\"handler_cost\": \"handler_amortised\",", "");
        assert!(!stripped.contains("handler_cost"), "fixture did not take");
        assert!(
            stripped.contains(&format!("\"schema_version\": {RESULTS_SCHEMA_VERSION}")),
            "the fixture must stay a current-version document"
        );
        std::fs::write(&path, &stripped).expect("write stripped");

        let err = merge_results_file(&p, sample_run("nats"), None).expect_err("must refuse");
        // Refused for the marker it lacks, naming the one its own flow and
        // handler derive — not for its version, which is correct.
        assert!(err.contains("handler_cost"), "{err}");
        assert!(err.contains("handler_amortised"), "{err}");
        assert!(err.contains("refusing to rewrite"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).expect("re-read"), stripped);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_unknown_handler_label_is_refused() {
        // The cost check reads the row's handler back, so an unparseable
        // handler label must be a refusal rather than a check that quietly
        // does not run.
        let mut run = batch_run(Flow::ConsumerGroup, HandlerProfile::Zero);
        run.results[0].handler = "brisk (7ms)".to_string();

        let path = temp_path("handler-unknown");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        let err = merge_results_file(&p, run, None).expect_err("must refuse");
        assert!(err.contains("brisk (7ms)"), "{err}");
    }

    #[test]
    fn every_pair_the_harness_writes_survives_its_own_validation() {
        // The guard above only helps if it agrees with the writer. Every row
        // `push_metrics` can emit must pass, or a real run would refuse to
        // record itself.
        for flow in Flow::ALL {
            for handler in HandlerProfile::ALL {
                let run = batch_run(flow, handler);
                validate_run(&run)
                    .unwrap_or_else(|e| panic!("{} x {handler} must validate: {e}", flow.as_str()));
            }
        }
    }

    #[test]
    fn results_file_carries_representative_false_for_sqs() {
        let path = temp_path("representative");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(&p, sample_run("sqs"), None).expect("write");
        let doc: BenchResults =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        let sqs = doc
            .runs
            .iter()
            .find(|r| r.backend == "sqs")
            .expect("sqs run");
        assert!(!sqs.representative);
        assert!(!sqs.unsupported.is_empty());
        assert!(!sqs.unsupported[0].reason.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn hardware_label_override_reaches_the_document() {
        let path = temp_path("label");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(&p, sample_run("kafka"), Some("MacBook Pro M4 Max")).expect("write");
        let doc: BenchResults =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        assert_eq!(doc.hardware.label, "MacBook Pro M4 Max");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn every_emitted_row_has_the_three_new_dimensions() {
        let path = temp_path("dimensions");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(&p, sample_run("inmemory"), None).expect("write");
        let raw: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        let row = &raw["runs"][0]["results"][0];
        for field in ["flow", "mode", "payload_bytes"] {
            assert!(!row[field].is_null(), "row is missing {field}: {row}");
        }
        let known: Vec<&str> = Flow::ALL.iter().map(|f| f.as_str()).collect();
        assert!(known.contains(&row["flow"].as_str().expect("flow is a string")));
        let bytes = row["payload_bytes"]
            .as_u64()
            .expect("payload_bytes is a number") as usize;
        assert!(PAYLOAD_SIZES.contains(&bytes));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn failed_rows_carry_flow_mode_and_payload_metadata() {
        // Failures are per-scenario records in the versioned document too, so
        // they need the same three dimensions as measured rows.
        let mut failures = Vec::new();
        let scenario = build_scenarios_cg(&cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "zero",
            "--flow",
            "consume-fifo",
        ]))[0];
        push_failure(&mut failures, &scenario, "boom".to_string());
        let v = serde_json::to_value(&failures[0]).expect("serialize");
        assert_eq!(v["flow"], "consume_fifo");
        assert_eq!(v["mode"], "fifo");
        assert_eq!(v["payload_bytes"], 64);
        assert_eq!(v["error"], "boom");
    }

    #[test]
    fn merging_from_a_different_environment_refuses_instead_of_relabelling() {
        let path = temp_path("provenance");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(&p, sample_run("redis"), None).expect("first write");
        // Simulate a document produced by a different toolchain: merging into
        // it would re-attribute the redis run to this host's provenance.
        let tampered = std::fs::read_to_string(&path)
            .expect("read")
            .replace(&rust_version(), "rustc 0.0.0 (someone-elses-box)");
        std::fs::write(&path, &tampered).expect("write tampered");

        let err = merge_results_file(&p, sample_run("nats"), None).expect_err("must refuse");
        assert!(err.contains("different environment"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).expect("re-read"), tampered);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_sentinel_never_counts_toward_the_measurement() {
        // Broadcast readiness sentinels run through the same handler as the
        // corpus; with a heavy profile a mid-flight sentinel used to survive
        // the warmup window and inflate the measured counter and
        // percentiles. The sentinel path must bump only the attach flag.
        let epoch = Instant::now();
        let recorder = Arc::new(LatencyRecorder::new());
        let processed = Arc::new(AtomicU64::new(0));
        let attach = Arc::new(AtomicU64::new(0));
        let handler = StressTestHandler::new(
            epoch,
            processed.clone(),
            recorder.clone(),
            HandlerProfile::Heavy,
        )
        .with_attach_counter(attach.clone());

        let sentinel = StressTestMsg {
            id: SENTINEL_ID,
            published_at_ns: 0,
            payload: String::new(),
        };
        let meta = MessageMetadata::builder().build();
        let outcome = <StressTestHandler as MessageHandler<StressBroadcastTopic>>::handle(
            &handler,
            sentinel,
            meta,
            &(),
        )
        .await;
        assert!(matches!(outcome, Outcome::Ack));
        assert_eq!(attach.load(Ordering::Relaxed), 1);
        assert_eq!(processed.load(Ordering::Relaxed), 0);
        let p = recorder.compute_percentiles().await;
        assert_eq!(p.e2e_p50, 0.0, "sentinel must record no latency");
    }

    #[tokio::test]
    async fn the_batch_handler_does_not_count_readiness_sentinels_either() {
        // The batch flow's readiness barrier publishes its sentinels through
        // the same topic as the corpus, so `handle_batch` sees them. Without
        // the same filter `handle` has, every sentinel is counted as a
        // processed message and recorded as a latency sample — which both
        // inflates the drain count and lets the barrier's own warmup traffic
        // satisfy the completion target.
        let epoch = Instant::now();
        let recorder = Arc::new(LatencyRecorder::new());
        let processed = Arc::new(AtomicU64::new(0));
        let attach = Arc::new(AtomicU64::new(0));
        let handler = StressBatchHandler::new(
            StressTestHandler::new(
                epoch,
                processed.clone(),
                recorder.clone(),
                HandlerProfile::Zero,
            )
            .with_attach_counter(attach.clone()),
        );

        let batch = vec![
            (
                StressTestMsg {
                    id: SENTINEL_ID,
                    published_at_ns: 0,
                    payload: String::new(),
                },
                MessageMetadata::builder().build(),
            ),
            (
                StressTestMsg {
                    id: 0,
                    published_at_ns: 0,
                    payload: String::new(),
                },
                MessageMetadata::builder().build(),
            ),
        ];
        let outcome = <StressBatchHandler as BatchMessageHandler<StressTestTopic>>::handle_batch(
            &handler,
            batch,
            &(),
        )
        .await;
        assert!(matches!(outcome, Outcome::Ack));
        assert_eq!(
            processed.load(Ordering::Relaxed),
            1,
            "the sentinel leaked into the measured count"
        );
        assert_eq!(attach.load(Ordering::Relaxed), 1, "attach flag not bumped");
    }

    #[tokio::test]
    async fn a_sentinel_only_batch_does_no_simulated_work() {
        // Oversupplied sentinels from the last readiness round arrive after
        // the barrier releases, inside the measured window. A batch holding
        // nothing but sentinels must not pay the handler's simulated sleep,
        // or the barrier's own traffic is charged to the drain. `heavy`
        // sleeps at least a second, so a fast return is unambiguous.
        let epoch = Instant::now();
        let attach = Arc::new(AtomicU64::new(0));
        let handler = StressBatchHandler::new(
            StressTestHandler::new(
                epoch,
                Arc::new(AtomicU64::new(0)),
                Arc::new(LatencyRecorder::new()),
                HandlerProfile::Heavy,
            )
            .with_attach_counter(attach.clone()),
        );
        let batch = vec![(
            StressTestMsg {
                id: SENTINEL_ID,
                published_at_ns: 0,
                payload: String::new(),
            },
            MessageMetadata::builder().build(),
        )];
        let started = Instant::now();
        let outcome = <StressBatchHandler as BatchMessageHandler<StressTestTopic>>::handle_batch(
            &handler,
            batch,
            &(),
        )
        .await;
        assert!(matches!(outcome, Outcome::Ack));
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "a sentinel-only batch paid the simulated work"
        );
        assert_eq!(attach.load(Ordering::Relaxed), 1, "attach flag not bumped");
    }

    #[test]
    fn publish_flows_are_pinned_to_one_worker() {
        // One sequential publisher loop runs regardless of the sweep; a row
        // labeled 32c would describe a topology that never existed.
        let scenarios = build_scenarios_cg(&cli_args(&[
            "--tier",
            "moderate",
            "--handler",
            "zero",
            "--flow",
            "publish-single,publish-batch",
            "--consumers",
            "1,8,32",
        ]));
        assert_eq!(scenarios.len(), 2);
        for s in &scenarios {
            assert_eq!(s.consumers, 1, "{}", s.flow);
            assert_eq!(s.messages, 5_000, "{}", s.flow);
        }
    }

    #[test]
    fn merging_refuses_to_resign_an_invalid_v1_document() {
        let path = temp_path("invalid-v1");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(&p, sample_run("redis"), None).expect("first write");
        // Shape-valid, semantically illegal: a payload size outside the
        // schema set must not be rewritten under a fresh v1 header.
        let tampered = std::fs::read_to_string(&path)
            .expect("read")
            .replace("\"payload_bytes\": 1024", "\"payload_bytes\": 128");
        std::fs::write(&path, &tampered).expect("write tampered");

        let err = merge_results_file(&p, sample_run("nats"), None).expect_err("must refuse");
        assert!(err.contains("payload_bytes 128"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).expect("re-read"), tampered);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_hardware_label_survives_merges_that_do_not_override_it() {
        let path = temp_path("label-survives");
        let _ = std::fs::remove_file(&path);
        let p = path.to_string_lossy().to_string();

        merge_results_file(&p, sample_run("kafka"), Some("bench host A")).expect("first write");
        merge_results_file(&p, sample_run("nats"), None).expect("second write");
        let doc: BenchResults =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        assert_eq!(doc.hardware.label, "bench host A");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn physical_cores_count_unique_socket_core_pairs() {
        // Dual-socket, 2 cores each: `cpu cores` says 2, the host has 4.
        let dual = "processor\t: 0\nphysical id\t: 0\ncore id\t: 0\ncpu cores\t: 2\n\
                    processor\t: 1\nphysical id\t: 0\ncore id\t: 1\ncpu cores\t: 2\n\
                    processor\t: 2\nphysical id\t: 1\ncore id\t: 0\ncpu cores\t: 2\n\
                    processor\t: 3\nphysical id\t: 1\ncore id\t: 1\ncpu cores\t: 2\n";
        assert_eq!(physical_core_count(dual), Some(4));

        // Hyperthreads share a (physical id, core id) pair and must not
        // double-count.
        let ht = "processor\t: 0\nphysical id\t: 0\ncore id\t: 0\n\
                  processor\t: 1\nphysical id\t: 0\ncore id\t: 0\n";
        assert_eq!(physical_core_count(ht), Some(1));

        // aarch64 exposes neither id — the caller falls back.
        let arm = "processor\t: 0\nBogoMIPS\t: 50.00\n";
        assert_eq!(physical_core_count(arm), None);
    }

    #[test]
    fn a_corrupt_results_file_is_an_error_not_a_silent_overwrite() {
        let path = temp_path("corrupt");
        std::fs::write(&path, "{not json").expect("seed");
        let p = path.to_string_lossy().to_string();
        let err = merge_results_file(&p, sample_run("redis"), None)
            .expect_err("corrupt file must not be silently replaced");
        assert!(err.contains("results document"), "{err}");
        let _ = std::fs::remove_file(&path);
    }

    // ── Regression guard for the stdout contract ──

    #[test]
    fn stdout_json_report_keeps_its_pre_existing_shape() {
        // The `--output json` consumers see `Report`, not `BenchResults`.
        // Its top-level keys must stay exactly these three.
        let report = Report {
            backend: "inmemory".to_string(),
            results: sample_run("inmemory").results,
            failures: vec![],
        };
        let v = serde_json::to_value(&report).expect("serialize");
        let obj = v.as_object().expect("object");
        let mut keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["backend", "failures", "results"]);
    }

    #[test]
    fn scenario_result_keeps_every_pre_existing_field() {
        let v = serde_json::to_value(&sample_run("inmemory").results[0]).expect("serialize");
        let obj = v.as_object().expect("object");
        for field in [
            "tier",
            "messages",
            "consumers",
            "handler",
            "throughput_msg_per_sec",
            "dispatch_p50_ms",
            "dispatch_p95_ms",
            "dispatch_p99_ms",
            "e2e_p50_ms",
            "e2e_p95_ms",
            "e2e_p99_ms",
            "scaling_efficiency",
            "peak_rss_mb",
            "cpu_pct",
            "duration_secs",
        ] {
            assert!(obj.contains_key(field), "lost pre-existing field {field}");
        }
        // 15 pre-existing + the 3 flow/mode/payload dimensions + the
        // handler-cost marker + the setup/drain split.
        assert_eq!(obj.len(), 20);
        assert!(obj.contains_key("handler_cost"));
        // Serialized even when `None`, as an explicit `null`: a reader has to
        // be able to tell "this driver did not separate setup from drain" from
        // "this document predates the field", and an omitted key conflates
        // them.
        assert!(obj.contains_key("setup_secs"));
    }

    #[test]
    fn scaling_is_computed_within_a_flow_and_payload_family() {
        let mut rows = vec![
            ScenarioResult {
                consumers: 1,
                throughput_msg_per_sec: 100.0,
                ..sample_run("x").results[0].clone()
            },
            ScenarioResult {
                consumers: 4,
                throughput_msg_per_sec: 400.0,
                ..sample_run("x").results[0].clone()
            },
            // Same tier/handler/messages but a different payload: must not
            // become the baseline for the rows above.
            ScenarioResult {
                consumers: 1,
                payload_bytes: 65536,
                throughput_msg_per_sec: 10.0,
                ..sample_run("x").results[0].clone()
            },
        ];
        compute_scaling(&mut rows);
        assert_eq!(rows[0].scaling_efficiency, 1.0);
        assert_eq!(rows[1].scaling_efficiency, 4.0);
        assert_eq!(rows[2].scaling_efficiency, 1.0);
    }
}
