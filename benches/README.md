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
| `--drain-messages 6000000` | the drain corpus for the consume flows | see below; sized so the group assembles well inside the first half of the corpus on the fastest cell and the window still lasts several seconds. **SQS is the one exception, at 60 000** — see "The SQS corpus deviation" |
| `--drain-max-bytes 3221225472` | cap on `corpus × payload_bytes` | 3 GiB: 6 M messages at 64 B, 3 M at 1 KiB, about 49 k at 64 KiB — what the 8 GB Docker VM holds, and every containerised backend stages its corpus inside it. **In-process is the one exception, at 32 GiB** — see "The in-process byte cap deviation" |
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

One class of row on the current host is the opposite of that, and it is not
comparable with anything: a **single-consumer** drain whose loop spends most
of each cycle waiting on the Docker VM is measured at a reduced CPU clock, so
that row is a floor rather than a ceiling. In the committed document it is
Redis `consume_batch` at 64 B and 1 KiB with one consumer, published at
0.457x and 0.439x their `consume_parallel` cells — where an unrelated busy
process on the host takes the same cell *above* that comparator. Do not
quote, chart or diff a 1c drain rate from this host as a backend or version
result without reading **A lone consumer on the macOS host clocks down**
below; on those flows the multi-consumer cells of the same row are the
comparable ones.

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

The RabbitMQ binary pins the broker's memory high watermark to an absolute
6 GiB rather than the image's fraction of the VM, so the limit a pass runs
under does not move with the VM: the 2026-09-07 and 2026-09-09 passes ran the
same fraction on a 7.8 GB and then a 16 GB VM. 6 GiB is the floor the 64 KiB
corpus needs (5.1 GiB resident) without tripping the alarm that #172 removed,
so give the VM 16 GB.

Run with Low Power Mode off. macOS Low Power Mode caps the CPU and the
Docker VM with it, at about 0.6x on fills, drains and publish cells alike: on
2026-09-10 the same 64 B drain filled in 176 to 181 s and drained at 17.8k to
19.5k msg/s with it off, and filled in 244 to 283 s and drained at 11.4k to
13.6k with it on, at every watermark tried. This host enables it on battery
(`pmset -g custom` shows `powermode 1` under Battery Power), which is how the
2026-09-09 RabbitMQ pass and the later cells of the 2026-09-08 NATS pass were
measured; that is the whole of their multi-consumer dips. `scripts/bench.sh`
reads the mode in effect from `pmset -g` and refuses to start unless
`powermode` is 0; `BENCH_ALLOW_LOW_POWER=1` overrides the check for a run
whose numbers will not be published. Check any pass at its first 64 B fill
(cell 7) before letting it run for the full two and a half hours.

Every backend's stress binary wires the batch-consume driver, so
`consume_batch` is measured wherever the backend implements
`HasBatchConsumption` rather than recorded as a capability hole. SQS is the
one backend whose batch size does not come from the matrix: `ReceiveMessage`,
`DeleteMessageBatch` and `ChangeMessageVisibilityBatch` all cap at 10 entries,
shove's SQS batch consumer rejects a larger `max_batch_size` at startup rather
than clamping it, and the matrix's size exceeds that. The harness clamps to 10
when it builds the scenario and the row records 10, so an SQS batch bar is a
10-message batch and is not like-for-like against a backend that ran 500.

### A lone consumer on the macOS host clocks down

Low Power Mode is not the only power management on this host, and the second
effect survives `pmset -g` reading `powermode 0`. A cell that runs **one**
consumer whose loop spends most of each cycle waiting on the Docker VM is
measured at a reduced clock, and the rate it publishes is a floor for that
flow rather than its ceiling. Where the current document shows it is Redis
`consume_batch` at 64 B and 1 KiB: one consumer draws 132 388 msg/s where
`consume_parallel` at the same cell draws 289 728, while two consumers draw
726 872 — 363k each, 2.7x the per-consumer rate at one. Measured 2026-09-11
on the host this document was measured on, Low Power Mode off, one consumer,
`--concurrent --handler zero`, the matrix's six-million-message drain.

The clock is the cause rather than the loop, and each of the three
measurements that say so excludes something else:

- **An instrumented drain steps mid-run.** Per-cycle cost holds at ~0.9 ms
  for the first second and then ~3.7 ms for the rest of the drain (read
  0.53-0.67 -> 1.9-2.7 ms, ingest 0.17-0.22 -> 0.6-0.9 ms, flush 0.20-0.26 ->
  0.8-1.1 ms; 137 783 msg/s overall, against the document's 132 388). **All
  three phases scale by the same ~3.7x, ingest included** — and ingest is
  client-side decode with no I/O in it, so no server-side or on-the-wire cost
  can be what slowed it; only the thread running slower can. The fast first
  second is the fill's producers having just left the cores clocked up, which
  is also why a 600 000-message drain finishes inside that window and measures
  521 753 msg/s.
- **Any unrelated busy process removes it.** `yes > /dev/null` on the host —
  touching neither Redis, the VM nor the harness — takes the same cell to
  384 000 msg/s and holds it fast throughout; so does a 250 ms `INFO` sampler
  (416-434k); so does a second consumer, which is the document's own 2c row.
  A quiet control run immediately afterwards draws 148 000 again. Nothing
  charged per entry, per message or per round trip is removed by traffic that
  shares no work with the flow.
- **The transport is not the bound.** A parse-free `XREADGROUP` probe on one
  connection fits ~0.085 ms fixed + 0.74 us per entry — 0.469 ms at COUNT 500,
  an implied 1 065 662 msg/s — and gets no faster with a second flow keeping
  the socket busy (0.438 ms). A lone serial reader pays no large fixed
  per-read cost here.

Excluded by the same pass: `XREADGROUP`/`XACK` server time, tokio cross-worker
handoff (`TOKIO_WORKER_THREADS=1` steps identically), thread QoS and
efficiency-core placement (`QOS_CLASS_USER_INTERACTIVE` on every worker, and
`taskpolicy -t 0 -l 0`, both step identically, while `taskpolicy -c background`
is slow from the first cycle), the Redis reaper's 1 s `XTRIM MINID` (disabling
it changes nothing once any companion process is present), and per-entry and
per-byte costs.

So read a single-consumer cell on this host as a lower bound and compare
across the multi-consumer cells on the same row. Repeated quiet-host runs of
that Redis cell draw from a 130-270k band, which is why the deficit is not a
stable rate to reason from. The effect needs a loop that is both
single-threaded and mostly idle: it is absent at 64 KiB, where 32 MB of reply
per cycle keeps the core busy and that 1c row runs 1.17x its `consume_parallel`
instead; absent on Kafka, whose per-cycle client work is heavier; and absent
in-process, with no broker and no VM in the cycle.

Nothing in the harness compensates for it. A keep-awake companion would make
every cell measure a host state the published rows do not have, so the choice
here is to disclose the effect rather than paper over it; adding one is a
methodology decision in its own right.

### The backlog cap on the ladder

An offered-load rung the consumers cannot sustain builds a backlog on the
broker for as long as the producers run, and at 64 KiB the 25 000/s rung
offers 1.6 GB/s to a container in an 8 GB VM: Redis stopped answering under
it and took the rest of the pass down. A rung's producers therefore stop as
soon as `lag x payload_bytes` passes the backlog cap (`--load-backlog-max-bytes`,
default 1 GiB), the rung is recorded as not sustained with the window it
actually ran and `backlog_capped` set on its row, and the ladder skips the
rungs above it as it already does after any unsustained rung. The cap is
lower than the drain cap on purpose: a drain corpus sits on an idle broker
before the consumers start, while a rung's backlog piles onto a broker that
is also serving the consumers and still reclaiming the previous cell (Redis
survived a 3.2 GB corpus and died under a 3.2 GB backlog minutes later). A
sustained rung never approaches the cap, so nothing that passes is changed
by it.

Redis needs one more guard the cap cannot give: its streams keep consumed
entries until the reaper's next `XTRIM MINID` sweep, so a 64 KiB rung at
25 000/s pushes acknowledged but untrimmed entries into memory at 1.6 GB/s
with no lag to cap. The stress container therefore runs `redis-server` with
`maxmemory 6gb` and `noeviction`: a stream that outgrows it fails its XADD,
which the harness records as that cell's failure, instead of the process
being killed and every later cell dying with it. Because `UNLINK` frees in
the background and `maxmemory` counts what is not yet freed, the purge waits
for `lazyfree_pending_objects` to reach zero before the next cell starts.
The Redis stress binary also connects with
`RedisConfig::with_trim_interval(1 s)` in place of the library's default
sweep (every handler timeout, floored at 30 s), so at most about a second of
acknowledged traffic is resident at once instead of up to 48 GB of it at the
64 KiB rung. It is a retention choice for the container, recorded here rather
than in the matrix because no other backend has the knob.

### The SQS corpus deviation

SQS is also the one backend that does not drain the matrix's corpus. It runs
**60 000** messages, set as `SQS_DRAIN_MESSAGES` in `scripts/bench.sh` and
substituted into the matrix for that target only; every other knob is shared.
The script prints a `deviation:` line at the top of the SQS log.

The corpus is a message *count*, and six million was sized for the fastest
cell — in-process at ~4.7 M msg/s, where it buys a window of about a second.
Measured against LocalStack, SQS drains at ~900 msg/s and fills at ~660, so
the same count buys a 1.7-hour window to measure what a minute measures, and
the pass as a whole runs about 51 hours. The deviation exists because one
count means two different things on backends three orders of magnitude apart,
not to spare SQS a fair measurement.

Everything that makes a drain row a rate still holds at 60 000: the window
runs tens of seconds against a 1 s floor, the group assembles in the first
handful of messages, and `--drain-max-bytes` still binds the 64 KiB leg at
49 152, so that leg is not deviated at all.

It is recorded in two places rather than only here. Every drain row carries
`drain.corpus`, so the document says what each cell drained; and when a
slice's backends disagree the charts name them ("corpus differs by backend:
… sqs 60k") instead of listing the sizes unattributed. Nothing about reading
an SQS row changes — a rate is a rate — but a shorter window is a noisier
estimate, which is why the size is on the chart and not just in the script.

The FIFO cell deviates too, and separately, because the drain deviation never
reaches it: `consume_fifo` holds no barrier and takes no drain, so it publishes
the tier's 5 000 messages per shard and consumes them through the sequenced
path. LocalStack serves that path at a few messages per second, which makes
each of the three FIFO cells (one per payload) a ten-hour cell. SQS runs
**100 per FIFO worker** instead (one worker per shard on this backend), set
as `SQS_FIFO_MESSAGES` in `scripts/bench.sh` and passed as `--fifo-messages`,
which replaces the tier's per-consumer count in the same unit; every other
backend runs the tier's count. The row records the corpus it ran in
`messages`.

Kafka is the reference backend for the batched-consume flow. Its batch and
parallel scenarios declare one partition per consumer so every group member
gets work. Its `publish_single` row is bounded by librdkafka's default 5 ms
`linger.ms`, because the publisher awaits each delivery report. That is
shove's default configuration and is what the row is meant to show.

Budget per consume cell a fill of up to six million messages, a drain of
several seconds, three rungs of ten seconds each, and group setup around
each. A backend's wall clock is set by its drain rate, because the corpus is
a count: the slower the backend, the longer the same corpus takes to fill and
drain while measuring exactly the same thing.

An hour is the right order for the fast backends and roughly half what the
slow ones want. One 64 B, two-consumer `consume_batch` drain per backend, run
back to back on one idle 8-core Linux host, measured these rates — a guide
for scheduling, not published numbers:

| backend | msg/s | full matrix |
|---|---|---|
| `inmemory` | ~1 100 000 | minutes |
| `kafka` | ~240 000 | about an hour |
| `redis` | ~166 000 | about an hour |
| `nats` | ~48 000 | about an hour |
| `rabbitmq` | ~18 000 | closer to two |
| `sqs` | ~900 | about an hour, and only because of the corpus deviation above |

Ratios move with the host; the ordering does not. Time one cell before
committing an evening to a backend nobody has measured here.

### The in-process byte cap deviation

In-process is the one backend that does not cap its drain corpus at the
matrix's 3 GiB. It runs **32 GiB**, set as `INMEMORY_DRAIN_MAX_BYTES` in
`scripts/bench.sh` and substituted into the matrix for that target only; every
other knob, including `--drain-messages`, is shared. The script prints a
`deviation:` line at the top of the in-process log.

This is the mirror image of the SQS deviation. SQS drains a smaller corpus
because it is the only backend slow enough that the pinned *count* means
something else there; in-process is allowed a larger one because it is the only
backend fast enough that the pinned *byte cap* does. At 3 GiB the 64 KiB leg is
49 152 messages, and the last pre-cap in-process leg drained every one of its
64 KiB cells in 0.09-0.46 s — under the 1 s floor, so the harness marked all
eleven rows `setup_bound` and the charts withheld all eleven. The fastest
backend in the set published no 64 KiB consume rate at all. That is the hole
the cap was added to close.

Why only in-process gets it: 3 GiB is not a cautious number for the others, it
is the containerised limit. Every other backend stages its corpus inside the
8 GB Docker VM — Redis and NATS in container memory, Kafka through its page
cache — where 3 GiB is already over a third of the VM, and where a 3.2 GB
backlog has already taken Redis down mid-pass (see the backlog cap). In-process
stages its corpus in the harness process on the 64 GB host and never starts a
container, so it is the one backend whose cap can rise without touching the VM.

The published in-process leg carries the cap, so the paragraphs below are
measured rows rather than a forecast. At 32 GiB the 64 KiB leg drains 524 288
messages — 32 GiB of payload staged in the harness process, which is the cap
doing exactly what it says. All twelve 64 KiB drain rows land in 3.4-9.1 s,
every one of them clears the floor as `framework`, and the leg publishes whole
where it was withheld whole.

Rates at that corpus are 52 k-139 k msg/s, well below the 95 k-514 k the same
cells reported at 49 152. A rate measured over a 0.1 s window does not predict
the rate over a 5 s one, so size the corpus from the window you want, not from
the throughput a sub-floor cell happened to report. `MIN_FRAMEWORK_CORPUS_MESSAGES`'s
own doc declines to chase a window that hardware speed keeps moving — a cell
landing under the floor is withheld and captioned, which costs one bar rather
than the pass.

It moves the 1 KiB leg too, by design: at 3 GiB that leg was byte-bound at
3 145 728, and 32 GiB puts it back on the pinned count of 6 000 000 where the
64 B leg already sits. In-process now runs two of its three legs on the matrix's
own corpus rather than one, in 4.9-15.1 s per cell, and the 1 KiB cell that
previously drained in 0.95 s clears the floor with the rest: the leg has no
`setup_bound` drain row at either payload.

As with the SQS deviation it is recorded on the rows, not only here: every
drain row carries `drain.corpus`, and where a slice's backends disagree the
charts name them — today "corpus differs by backend: inmemory 524k / 6.0M;
kafka, nats, rabbitmq, redis 49k / 3.1M / 6.0M; sqs 49k / 60k" — rather than
listing sizes unattributed.

One thing it does **not** fix, so the rerun is not read as fixing it: two cells
failed "consumed before assembly" at 64 KiB with eight consumers, and the cap
clears only one. In-process cleared — its batch drain assembled at 30 500 of
524 288, comfortably inside the first half — and it has no failures left. The
other is RabbitMQ, whose eight-consumer drain ran through 46 467 of 49 152
messages before the last worker was assigned. That leg was not re-measured and
the cap cannot reach it anyway: RabbitMQ stages inside the 8 GB VM at the pinned
3 GiB, and putting the assembly well under half a corpus needs ~196 k messages,
or 12 GiB inside that VM. That one is a worker-assembly cost rather than a
corpus size, and it is still the document's one drain failure.

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

The harness prints a table per scenario and a summary at the end. What to
check before trusting a document:

- `Failed scenarios:` at the end of the log. A failed scenario is recorded in
  `failures[]` and produces no row, so a chart drawn from it is missing a
  point rather than showing a bad one.
- `handler_cost` on each row. `framework` means the window measured shove;
  `setup_bound` means the window was too short or unseparated from setup and
  the number is a stopwatch reading, not a rate.
- `setup_secs` on the consume rows. Every consume driver but one holds a
  readiness barrier and records the interval it excluded. `None` is expected on
  `consume_fifo`, which cannot hold one — a sequenced drain is not measurable,
  so that driver publishes its corpus inside the timed window and its
  negligible-handler rows stay `setup_bound`. `None` on a `dlq_drain` row is a
  row from a v6 document instead; the two are not comparable, which is why a v6
  file is refused at merge rather than written into.
- The `Offered-load rungs` table at the end of the log, one line per rung
  with its verdict. A cell whose first rung is already producer-bound is a
  host that cannot drive the ladder for that payload, not a backend result.
- The `drain:` line under each consume cell: `unique_at_start` well under
  half the corpus, `duplicates` at zero on the backends that deliver once,
  and a window of several seconds. A cell that failed as consumed before
  assembly wants a larger `--drain-messages`, not a rerun.
- Rows flat across consumer counts on RabbitMQ are the broker, not the
  harness: one classic queue is one Erlang process, and at 64 B it moves
  about 35k msg/s in this VM whether one or eight consumers drain it and
  whether they share a connection or not (2026-09-10). The publish fill hits
  the same ceiling. A cell 10 to 15 % under its neighbours is inside the
  cell-to-cell noise seen on the same host.
- A **1-consumer** cell far under its own 2-consumer per-consumer rate on the
  macOS host is that host's clock management, not the backend and not the
  flow: the published Redis `consume_batch` 64 B row draws 132k at one
  consumer against 363k each at two, and repeated quiet-host runs of it draw
  anywhere in 130-270k. Such a row is a floor, so it is not a rate to reason
  from and not comparable across backends or versions — see **A lone consumer
  on the macOS host clocks down**. The per-consumer rate at two consumers is
  the test, and it isolates the affected rows across the whole document: it is
  2.75x and 2.37x the 1c rate on those two Redis cells, and no higher than
  1.07x on all twelve others, which fall from one to two consumers the way
  contention makes them. A 1c row under parity with a *flat* 2c per-consumer
  rate — in-process at 1 KiB and 64 KiB, Kafka at 64 B — is a real result and
  not this effect.

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
