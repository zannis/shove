# Benchmark runbook

How the numbers behind `benches/results/bench-results.json` and the charts in
`docs/public/bench/` are produced, one backend at a time.

Everything goes through `scripts/bench.sh`. It pins the matrix, picks the
right example binary and feature flag for the backend, checks the
prerequisites, and tees the harness output into `target/bench-logs/`.

## The matrix

Every published row is measured with the same sampled core matrix:

```text
--flow all --payload all --tier moderate --handler zero --consumers 1,2,4,8 --concurrent \
--drain-messages 6000000 --drain-max-bytes 3221225472 \
--load-rates 5000,25000,100000 --load-window-secs 10 --load-producers 8
```

| Knob | Value | Why |
|---|---|---|
| `--flow all` | publish single/batch, consume parallel/FIFO/batch, consumer group, broadcast, DLQ drain | one row per flow the backend supports; unsupported flows land in `unsupported[]` with a reason |
| `--payload all` | 64 B, 1 KiB, 64 KiB | the payload axis of the charts |
| `--tier moderate` | 5 000 messages per scenario | the only tier every backend finishes in minutes |
| `--handler zero` | no-op handler | measures shove and the broker, not simulated work |
| `--consumers 1,2,4,8` | four points on the scaling curve | the sampling lever for the published matrix |
| `--concurrent` | concurrent processing within each consumer | without it every consumer handles one message at a time, and the parallel flows measure ack round trips instead of throughput |
| `--drain-messages 6000000` | the drain corpus for the consume flows | see below; sized so the group assembles well inside the first half of the corpus on the fastest cell and the window still lasts several seconds |
| `--drain-max-bytes 3221225472` | cap on `corpus × payload_bytes` | 3 GiB: 6 M messages at 64 B, 3 M at 1 KiB, about 49 k at 64 KiB — the in-process backend holds the corpus resident and Kafka writes it to disk before every cell |
| `--load-rates …` | the offered-load ladder for the consume flows | see below; three rungs, because the ladder measures latency at a sustained rate and the ceiling comes from the drain |
| `--load-window-secs 10` | how long each rung holds its rate | long enough for a rate |
| `--load-producers 8` | paced producer tasks sharing each rung's rate, and fill tasks for the drain | one sequential publisher tops out near 70k msg/s on Kafka; on Kafka each task gets its own connection, because publishers cloned from one client share one producer instance and cap near 260k msg/s together |

## The drain

The consume flows (`consumer_group`, `consume_parallel`, `consume_batch`, and
`supervisor` on SQS) are measured two ways, and every row and every
`failures[]` entry of those flows records which under `method`. The drain is
the throughput ceiling; the ladder, below, is the latency measurement. The
charts never put the two on one axis: the throughput charts publish those
flows from drain rows only, and a cell that has rungs but no drain row is
withheld and captioned as such.

For each cell the harness publishes the whole corpus **before any consumer
exists**, through the same producer pool the ladder uses, then starts the
consumers and holds the readiness barrier until every one of them is assigned
and has processed a message. The clock starts there. Consumers complete
messages by unique id, so a redelivery is counted once, and the window closes
the instant the unique count crosses nine tenths of the corpus. The row
records:

| Field | Meaning |
|---|---|
| `throughput_msg_per_sec` | unique completions inside the window, over the window |
| `messages`, `drain.corpus`, `drain.published` | the corpus asked for and the fill's count; equal on every row, since a short fill fails the cell |
| `drain.fill_secs`, `drain.producers` | how long the unmeasured fill took, and over how many tasks |
| `drain.unique_at_start` | completions already done when the barrier passed: the workers that joined first were not idle |
| `drain.unique_at_end` | where the window closed, `corpus − corpus / 10` |
| `drain.unique_final`, `drain.deliveries`, `drain.duplicates` | the whole drain's completions, every handler invocation, and their difference |

A drain row's dispatch and end-to-end percentiles are zero. They would be
backlog residency, not latency, nothing publishes them, and recording one
per message is harness work inside the very window that measures the
ceiling; the latency numbers come from the ladder.

Why a slice and not the whole drain: before the barrier some workers ran
while others were still joining and every client's fetch pipeline was still
filling, so that stretch is not the steady state; at the end, partitions (or
the shared queue) run dry at different moments, so the last stretch runs with
fewer active workers than the row names. Inside the slice every worker is
assigned and the pipelines are at their steady depth, so the fetch work done
during the window is the fetch work of the messages completed in it. What the
slice does assume is that the corpus is spread evenly enough for no partition
to empty before nine tenths of the whole; null-key records under librdkafka's
sticky partitioner rotate partitions per batch, which holds that.

Two ways a drain fails rather than producing a row, both recorded in
`failures[]` with `method: drain`:

- **Consumed before assembly.** If half the corpus or more was already gone
  when the barrier passed, the remaining slice is not a steady state and the
  diagnostic names `--drain-messages` as the remedy. The tier-sized corpus a
  flag-less run drains is small enough that this is the expected outcome on
  a fast cell; the matrix's corpus is what makes the drain measurable.
- **Stalled.** A fill or a drain whose count does not advance for 60 s fails
  with the counts, so a consumer that cannot see the pre-published corpus
  ends the cell instead of hanging the sweep.

A drain is the consumers' ceiling under ideal supply: the broker serves it
from page cache with every fetch full, and no publisher competes for the
host. It is the number to compare backends and versions by, not a
prediction of what a live producer at that rate would see.

## The offered-load ladder

The ladder is the latency measurement. For each rate on it the harness
starts the consumers, waits for every one to be assigned and polling, and
then runs paced producer tasks that hold that rate for the window while the
consumers process alongside them. When the window closes the producers stop,
the consumers drain what is left, and the row records:

| Field | Meaning |
|---|---|
| `throughput_msg_per_sec` | what the consumers processed during the window, per second |
| `messages` | what the producer actually published |
| `load.offered_msg_per_sec` | the rate the producer was asked to hold |
| `load.achieved_publish_msg_per_sec` | the rate it held |
| `load.lag_at_window_end`, `load.peak_lag` | published minus processed when the window closed, and its peak |
| `load.producer_bound` | the producer landed under 95 percent of the offered rate while the consumers kept up with what it published, so their ceiling was never reached |
| `load.sustained` | the producer held the rate and the consumers were never more than 5 percent behind it, at the window's end or at its worst moment (`peak_lag`) |

A cell's ladder climbs in ascending order and stops at the first rung that
was not sustained, whether because the consumers fell behind, the producer
could not reach the rate, or the rung failed outright. Skipped rungs are
announced in the log and absent from the document.

Reading the verdicts:

- **Sustained** at a rate means the backend handles that offered load with
  the given consumer count. The dispatch percentiles on that row are real
  latency, since no backlog formed.
- **Not sustained** means the consumers fell behind what was published, so
  their ceiling lies between this rung and the last sustained one. The row's
  throughput is that ceiling. This holds even when the producer also fell
  short of the offered rate: on the in-process backend a full queue blocks
  the publisher, and that backlog is the consumers' doing.
- **Producer-bound** means the harness could not offer the rate on this
  host and the consumers kept up with everything it did offer. The row says
  nothing about the consumers' ceiling, and the charts withhold it. Expect
  it on every backend at 64 KiB above a few thousand messages per second,
  where the producer is pushing hundreds of megabytes a second.

The ladder's producer runs inside the harness process and contends with the
consumers for the host, so a rung it cannot hold says nothing about the
consumers: on this host every Kafka cell topped out producer-bound, which is
why the ceiling comes from the drain. A rung never supplies a throughput
point: the throughput charts publish the consume flows from drain rows only,
and a cell with rungs but no drain row is withheld and captioned as such.
The ladder feeds the dispatch-latency chart alone, and only through rungs
that were **sustained**: the consumers ended the window, and never fell
mid-window, more than 5 percent behind what was published.

The matrix lives in one place, the `MATRIX` array in `scripts/bench.sh`.
Do not pass the same knob again after `--`, the harness rejects a repeated
argument.

## Running one backend

```sh
scripts/bench.sh kafka
```

| Backend | Example | Feature | Needs |
|---|---|---|---|
| `inmemory` | `inmemory_stress` | `inmemory` | nothing |
| `kafka` | `kafka_stress` | `kafka` | Docker |
| `nats` | `nats_stress` | `nats` | Docker |
| `rabbitmq` | `rabbitmq_stress` | `rabbitmq` | Docker |
| `redis` | `redis_stress` | `redis-streams` | Docker |
| `sqs` | `sqs_stress` | `aws-sns-sqs` | Docker and `LOCALSTACK_AUTH_TOKEN` |

The Docker backends start their own testcontainer and tear it down at exit.
SQS runs against LocalStack, so its numbers measure LocalStack and the harness
marks them `representative: false`. Supply the token through dotenvx:

```sh
dotenvx run -- scripts/bench.sh sqs
```

Run backends one after another, never in parallel. Two harnesses on one host
contend for CPU and skew each other's windows, and two merges into the same
document race.

Kafka is the reference backend for the batched-consume flow, and the only one
besides in-memory the harness wires it for. Its batch and parallel scenarios
declare one partition per consumer so every group member gets work. Its
`publish_single` row is bounded by librdkafka's default 5 ms `linger.ms`,
because the publisher awaits each delivery report. That is shove's default
configuration and is what the row is meant to show.

Budget about an hour per Docker backend for the full matrix: per consume
cell a fill of up to six million messages, a drain of several seconds, three
rungs of ten seconds each, and group setup around each.

## Results document and provenance

The harness merges each backend's run into the results document by backend
key, so running the six backends in sequence against one path accumulates a
single cross-backend document.

The document carries one provenance block, host, OS, core count, RAM, Rust
and shove version, for every run in it. The harness refuses to merge a run
from a different environment into an existing document rather than mislabel
the rows already there. When that happens you have two honest options:

```sh
# Start a new document, moving the old one aside with a timestamp.
scripts/bench.sh kafka --fresh

# Or write somewhere else and compare by hand.
scripts/bench.sh kafka --results-file /tmp/kafka-only.json
```

Never edit the provenance block to make a merge go through.

Rerunning a single backend into the current document replaces only that
backend's entry. That is the intended way to refresh one backend after a
change to it, as long as the host and toolchain still match the document.

## Regenerating the charts

```sh
scripts/bench.sh charts
```

This renders every SVG under `docs/public/bench/` from the results document
and then runs the chart-generator test target, which byte-compares the
committed SVGs against the committed document. CI runs the same test, so a
results change that is not accompanied by regenerated charts fails there.

The unsuffixed files are the 64 B charts. The three families that slice on a
single payload (throughput vs consumers, parallel vs sequenced, dispatch
latency) also render at each larger harness payload as a `-1kib` / `-64kib`
sibling, each with its own `-dark` variant.

## Extra harness arguments

Anything after `--` goes to the harness verbatim, for knobs the matrix does
not pin:

```sh
scripts/bench.sh kafka -- --batch-max-size 1000 --batch-max-age-ms 100
scripts/bench.sh kafka -- --prefetch 200
scripts/bench.sh kafka -- --hardware-label "c7g.2xlarge"
```

Rows produced with a non-default knob are not comparable with the published
ones. Keep them in a separate results file.

## Reading a run

The harness prints a table per scenario and a summary at the end. Three
things to check before trusting a document:

- `Failed scenarios:` at the end of the log. A failed scenario is recorded in
  `failures[]` and produces no row, so a chart drawn from it is missing a
  point rather than showing a bad one.
- `handler_cost` on each row. `framework` means the window measured shove;
  `setup_bound` means the window was too short or unseparated from setup and
  the number is a stopwatch reading, not a rate.
- `setup_secs` on the consume rows. `None` means the driver could not
  separate setup from the drain, which is expected for FIFO.
- The `Offered-load rungs` table at the end of the log, one line per rung
  with its verdict. A cell whose first rung is already producer-bound is a
  host that cannot drive the ladder for that payload, not a backend result.
- The `drain:` line under each consume cell: `unique_at_start` well under
  half the corpus, `duplicates` at zero on the backends that deliver once,
  and a window of several seconds. A cell that failed as consumed before
  assembly wants a larger `--drain-messages`, not a rerun.

## Related benchmarks

The Criterion targets under `benches/` are separate from the stress matrix
and are not part of the published document:

```sh
cargo bench -q --no-default-features --bench pure_paths            # broker-free paths
cargo bench -q --features inmemory --bench consumer_overhead       # shove's own per-consumer cost
cargo bench -q --features inmemory --bench inmemory_flows          # flow coverage over in-memory
cargo bench -q --features rabbitmq --bench publish_throughput      # Docker
cargo bench -q --features rabbitmq --bench autoscaler              # Docker
```
