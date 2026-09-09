#!/usr/bin/env bash
# Run the published benchmark matrix for one backend, or regenerate the charts.
#
#   scripts/bench.sh <backend> [--results-file PATH] [--fresh] [-- <harness args>]
#   scripts/bench.sh charts    [--results-file PATH]
#
# See benches/README.md for the full runbook.
set -euo pipefail

cd "$(dirname "$0")/.."

RESULTS_FILE="benches/results/bench-results.json"
CHARTS_DIR="docs/public/bench"
LOG_DIR="target/bench-logs"

# The sampled core matrix every published row was measured with. Change it
# here and nowhere else — a document mixing matrices is not comparable.
#
# The consume flows are measured two ways, and the charts keep them apart:
#
# - The drain is the throughput ceiling. Each cell's corpus is published
#   before any consumer starts, and the row's rate is what the consumers
#   processed from the moment every one of them was assigned until nine
#   tenths of the corpus was gone — no producer in the window, so nothing in
#   the harness process can bound it. `--drain-messages` sizes the corpus so
#   that on the fastest cell the group assembles well inside the first half
#   of it and the window still lasts several seconds; `--drain-max-bytes`
#   caps the 64 KiB corpus at 3 GiB (about 49 k messages), which fits the
#   8 GB Docker VM every containerised backend stages its corpus in — see
#   "The in-process byte cap deviation" for the one backend that does not.
# - The offered-load ladder is the latency measurement: a paced producer
#   holds each rate for the window while the consumers run, and only a rung
#   the consumers kept up with has dispatch percentiles that are latency
#   rather than queue residency. Three rungs are enough for that; the ceiling
#   no longer comes from the ladder, so the rungs above the in-process
#   producer's own limit are gone.
MATRIX=(
  --flow all
  --payload all
  --tier moderate
  --handler zero
  --consumers "1,2,4,8"
  --concurrent
  --drain-messages 6000000
  --drain-max-bytes 3221225472
  --load-rates "5000,25000,100000"
  --load-window-secs 10
  --load-producers 8
)

# The first of the two documented deviations from the matrix above. Both are
# per-backend substitutions of a single knob, both exist because one number
# means two different things at opposite ends of the backend set, and both
# are recorded on every row they touch — see the two runbook sections.
#
# The corpus is a message *count*, and it was sized for the fastest cell —
# in-process at ~4.7 M msg/s, where six million messages buy a window of
# about a second. SQS measures LocalStack at ~900 msg/s and fills at ~660,
# so the same count buys a 1.7-hour window to measure what a minute would,
# and the pass as a whole runs about 51 hours. That is the count meaning two
# different things on two backends, not SQS being slow in a way worth
# publishing 51 hours to show.
#
# Everything that makes a drain row a rate still holds at this size: the
# window runs tens of seconds (the floor chartgen publishes at is 1 s), the
# group assembles in the first handful of messages, and the byte cap still
# binds the 64 KiB leg at 49 152, so that leg is unchanged. The row records
# `drain.corpus`, so the deviation is in the document rather than only here,
# and the charts name the backend whose corpus differs.
#
# This is a per-backend corpus, not a second matrix: every other knob is
# shared, so the flows and the axes stay comparable.
SQS_DRAIN_MESSAGES=60000
# The FIFO cell has no drain and no barrier, so the corpus deviation above
# never reaches it: it publishes the tier's 5 000 messages per shard and
# consumes them through the sequenced path, which LocalStack serves at a few
# messages per second. That is a ten-hour cell three times over (one per
# payload) for a number that measures LocalStack. 100 per FIFO worker (one
# per shard on this backend), the tier's own unit, keeps the cell a few
# minutes long and, as with the drain, the row records the corpus it ran
# (`messages`).
SQS_FIFO_MESSAGES=100

# The second deviation, and the mirror image of the first: SQS drains a
# smaller corpus because it is the only backend slow enough that the pinned
# count means something else there, and in-process is allowed a larger one
# because it is the only backend fast enough that the pinned *byte cap* does.
#
# At the matrix's 3 GiB the 64 KiB leg is 49 152 messages, and the 2026-09-08
# run drained every one of the in-process 64 KiB cells in 0.09-0.46 s. That is
# under `MIN_FRAMEWORK_WINDOW_SECS`, so the harness marked all eleven rows
# `setup_bound` and the charts withheld all eleven and captioned why. The
# fastest backend in the set published no 64 KiB consume rate at all.
#
# Why this is in-process's deviation alone: 3 GiB is not a conservative number
# for the others, it is the containerised limit. Every other backend stages its
# corpus inside the 8 GB Docker VM (Redis and NATS in container memory, Kafka
# through its page cache), where 3 GiB is already over a third of the VM and
# where a 3.2 GB backlog has already taken Redis down mid-pass — see the
# backlog cap. In-process stages its corpus in the harness process on the
# 64 GB host and never starts a container, so it is the one backend where the
# cap can rise without touching the VM.
#
# Sized from that run's fastest 64 KiB drain (514 k msg/s, `consumer_group` at
# two consumers): 32 GiB is 524 288 messages, which puts that cell at ~0.9 s
# and the other ten at 1.0-5.0 s, so the leg publishes instead of being
# withheld whole. Resident cost is ~34 GB of the 64 GB host. Clearing the last
# cell too would need ~40 GiB, and the harness's own floor doc declines to
# chase a window that hardware speed keeps moving; a cell that lands under the
# floor is withheld and captioned exactly as today, which loses one bar rather
# than the pass.
#
# It moves the 1 KiB leg as well, and that is intended rather than incidental:
# at 3 GiB that leg is byte-bound at 3 145 728, and 32 GiB puts it back on the
# pinned count of 6 000 000, where the 64 B leg already sits. So in-process
# runs two of its three legs on the matrix's own corpus instead of one, and the
# single 1 KiB cell that drained in 0.97 s clears the floor too. It costs about
# forty seconds of extra wall clock and 6 GiB rather than 3 GiB resident.
#
# `QUEUE_CAPACITY` in examples/inmemory/stress.rs is 8 000 000 and is a bound
# rather than a preallocation, so it already covers 524 288; the harness's
# `refused_drain_capacity` gate is what would catch it otherwise.
#
# What it does not fix, so the run is not read as fixing it: the four cells
# that failed "consumed before assembly" at 64 KiB / 8 consumers. In-process
# was one of them and this clears it, but the other three are RabbitMQ, whose
# eight-consumer group assembly ran through 37 000-46 300 messages. Putting
# that well under half a corpus needs ~196 k messages, which is 12 GiB in an
# 8 GB VM. RabbitMQ's three failures are a group-assembly cost, not a corpus
# size, and they are expected to fail again.
INMEMORY_DRAIN_MAX_BYTES=34359738368

usage() {
  sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-2}"
}

die() {
  echo "bench.sh: $*" >&2
  exit 1
}

# Replace a knob's value in MATRIX in place, rather than appending a second
# copy: the harness rejects a knob given twice, so a per-backend deviation has
# to substitute. Dies when the knob is absent, so a deviation can never
# silently stop deviating if the matrix above is edited.
substitute_knob() {
  local knob="$1" value="$2" i found=0
  for i in "${!MATRIX[@]}"; do
    if [ "${MATRIX[$i]}" = "$knob" ]; then
      MATRIX[$((i + 1))]="$value"
      found=1
    fi
  done
  [ "$found" = 1 ] || die "the matrix has no $knob for the $target deviation to replace"
}

[ $# -ge 1 ] || usage
target="$1"
shift

fresh=0
extra=()
while [ $# -gt 0 ]; do
  case "$1" in
    --results-file) [ $# -ge 2 ] || die "--results-file needs a path"; RESULTS_FILE="$2"; shift 2 ;;
    --fresh) fresh=1; shift ;;
    --) shift; extra=("$@"); break ;;
    -h|--help) usage 0 ;;
    *) die "unknown option '$1' (harness arguments go after '--')" ;;
  esac
done

case "$target" in
  inmemory) example=inmemory_stress; features=inmemory;      needs_docker=0 ;;
  kafka)    example=kafka_stress;    features=kafka;         needs_docker=1 ;;
  nats)     example=nats_stress;     features=nats;          needs_docker=1 ;;
  rabbitmq) example=rabbitmq_stress; features=rabbitmq;      needs_docker=1 ;;
  redis)    example=redis_stress;    features=redis-streams; needs_docker=1 ;;
  sqs)      example=sqs_stress;      features=aws-sns-sqs;   needs_docker=1 ;;
  charts)
    mkdir -p "$CHARTS_DIR"
    cargo run -q --no-default-features --example chartgen -- \
      --input "$RESULTS_FILE" --out-dir "$CHARTS_DIR"
    # The byte-compare test is what CI runs; a chart the test rejects is not
    # publishable.
    cargo nextest run --no-default-features -E 'binary(chartgen)'
    exit 0
    ;;
  *) die "unknown backend '$target' (inmemory|kafka|nats|rabbitmq|redis|sqs|charts)" ;;
esac

if [ "$needs_docker" = 1 ]; then
  docker info >/dev/null 2>&1 || die "Docker daemon is not reachable; the $target harness starts a testcontainer"
fi

if [ "$target" = sqs ]; then
  [ -n "${LOCALSTACK_AUTH_TOKEN:-}" ] \
    || die "LOCALSTACK_AUTH_TOKEN is not set; run through 'dotenvx run -- scripts/bench.sh sqs'"

  substitute_knob --drain-messages "$SQS_DRAIN_MESSAGES"
  # Appended rather than substituted: the matrix carries no --fifo-messages,
  # because every other backend runs the tier's FIFO corpus.
  MATRIX+=(--fifo-messages "$SQS_FIFO_MESSAGES")
fi

if [ "$target" = inmemory ]; then
  substitute_knob --drain-max-bytes "$INMEMORY_DRAIN_MAX_BYTES"
fi

if [ "$fresh" = 1 ] && [ -f "$RESULTS_FILE" ]; then
  backup="$RESULTS_FILE.$(date -u +%Y%m%dT%H%M%SZ).bak"
  mv "$RESULTS_FILE" "$backup"
  echo "moved existing results document aside: $backup"
fi

mkdir -p "$LOG_DIR" "$(dirname "$RESULTS_FILE")"
log="$LOG_DIR/$target-$(date -u +%Y%m%dT%H%M%SZ).log"

echo "backend:  $target ($example, --features $features)"
echo "matrix:   ${MATRIX[*]} ${extra[*]:-}"
if [ "$target" = sqs ]; then
  echo "deviation: drain corpus $SQS_DRAIN_MESSAGES, not the pinned 6000000, and FIFO corpus $SQS_FIFO_MESSAGES per FIFO worker — see the comments in this script"
fi
if [ "$target" = inmemory ]; then
  echo "deviation: drain byte cap $INMEMORY_DRAIN_MAX_BYTES, not the pinned 3221225472, so the 64 KiB leg drains 524288 messages rather than 49152 and the 1 KiB leg the pinned 6000000 rather than 3145728 — see the comments in this script"
fi
echo "results:  $RESULTS_FILE"
echo "log:      $log"

# The harness refuses to merge into a document produced on another host,
# toolchain or crate version. That refusal is correct: rerun with --fresh
# (or a new --results-file) rather than editing the provenance block.
cargo run -q --release --example "$example" --features "$features" -- \
  "${MATRIX[@]}" "${extra[@]+"${extra[@]}"}" --results-file "$RESULTS_FILE" \
  2>&1 | tee "$log"
