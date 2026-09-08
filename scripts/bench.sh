#!/usr/bin/env bash
# Run the published benchmark matrix for one backend, or regenerate the charts.
#
#   scripts/bench.sh <backend> [--results-file PATH] [--fresh] [--dry-run] [-- <harness args>]
#   scripts/bench.sh charts    [--results-file PATH]
#
# A knob given after '--' replaces its pinned copy in the matrix.
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
#   caps the 64 KiB corpus at 3 GiB (about 49 k messages), which the
#   in-process backend holds resident and Kafka writes to disk per cell.
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

# The one documented deviation from the matrix above, and the only backend
# that gets one.
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

usage() {
  sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-2}"
}

die() {
  echo "bench.sh: $*" >&2
  exit 1
}

[ $# -ge 1 ] || usage
target="$1"
shift

fresh=0
dry_run=0
extra=()
while [ $# -gt 0 ]; do
  case "$1" in
    --results-file) [ $# -ge 2 ] || die "--results-file needs a path"; RESULTS_FILE="$2"; shift 2 ;;
    --fresh) fresh=1; shift ;;
    --dry-run) dry_run=1; shift ;;
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

# SQS's documented corpus deviation is a property of the published matrix, not
# a choice made at the command line, so it is substituted into `MATRIX` itself
# — before the matrix is joined with the caller's arguments below. That
# ordering is what makes it the value a `--dry-run` reports, and what lets a
# caller's own `--drain-messages` replace it in turn for a one-off.
sqs_deviated=0
if [ "$target" = sqs ]; then
  for i in "${!MATRIX[@]}"; do
    if [ "${MATRIX[$i]}" = --drain-messages ]; then
      MATRIX[$((i + 1))]="$SQS_DRAIN_MESSAGES"
      sqs_deviated=1
    fi
  done
  [ "$sqs_deviated" = 1 ] \
    || die "the matrix has no --drain-messages for the sqs corpus deviation to replace"
fi

# Join the pinned matrix with the caller's own knobs. A knob named after `--`
# *replaces* its pinned copy rather than being appended after it: clap rejects
# a repeated argument outright ("cannot be used multiple times"), so appending
# made every pinned knob unchangeable — the run died before its first
# scenario, and a cell needing a corpus other than the pinned 6 M could not be
# started at all. Nothing else in the matrix moves.
#
# A replacement makes the resulting rows incomparable with the published ones,
# so each one is announced and belongs in its own --results-file. See "Extra
# harness arguments" in benches/README.md.
override_names=()
for arg in ${extra[@]+"${extra[@]}"}; do
  case "$arg" in --?*) override_names+=("${arg%%=*}") ;; esac
done

is_overridden() {
  local candidate
  for candidate in ${override_names[@]+"${override_names[@]}"}; do
    [ "$candidate" = "$1" ] && return 0
  done
  return 1
}

# MATRIX is a flat list of `--flag [value]` pairs, so a token following a flag
# is that flag's value unless it is itself a flag. Dropping an overridden knob
# has to drop its value with it — an orphaned value would reach the harness as
# a positional — and must drop *only* one token for a valueless flag like
# `--concurrent`, or the knob pinned after it disappears silently.
harness_args=()
replaced=()
i=0
while [ "$i" -lt "${#MATRIX[@]}" ]; do
  arg="${MATRIX[$i]}"
  next="${MATRIX[$((i + 1))]:-}"
  takes_value=0
  case "$next" in ""|--?*) ;; *) takes_value=1 ;; esac

  if is_overridden "$arg"; then
    if [ "$takes_value" = 1 ]; then
      replaced+=("$arg $next")
      i=$((i + 2))
    else
      replaced+=("$arg")
      i=$((i + 1))
    fi
    continue
  fi

  harness_args+=("$arg")
  i=$((i + 1))
  if [ "$takes_value" = 1 ]; then
    harness_args+=("$next")
    i=$((i + 1))
  fi
done

if [ "${#extra[@]}" -gt 0 ]; then
  harness_args+=("${extra[@]}")
fi
harness_args+=(--results-file "$RESULTS_FILE")

# A caller who names --drain-messages themselves has replaced the SQS corpus
# too, so the run is no longer the documented deviation and must not say it is.
if [ "$sqs_deviated" = 1 ] && is_overridden --drain-messages; then
  sqs_deviated=0
fi

if [ "${#replaced[@]}" -gt 0 ]; then
  for pinned in "${replaced[@]}"; do
    echo "bench.sh: caller overrides pinned matrix knob '$pinned'" >&2
  done
  echo "bench.sh: rows from this run are not comparable with the published matrix" >&2
fi

# Resolve and print the harness argv without building or starting anything —
# one argument per line, so an hour-long cell can be checked before it starts.
if [ "$dry_run" = 1 ]; then
  printf '%s\n' "${harness_args[@]}"
  exit 0
fi

if [ "$needs_docker" = 1 ]; then
  docker info >/dev/null 2>&1 || die "Docker daemon is not reachable; the $target harness starts a testcontainer"
fi

if [ "$target" = sqs ]; then
  [ -n "${LOCALSTACK_AUTH_TOKEN:-}" ] \
    || die "LOCALSTACK_AUTH_TOKEN is not set; run through 'dotenvx run -- scripts/bench.sh sqs'"
fi

if [ "$fresh" = 1 ] && [ -f "$RESULTS_FILE" ]; then
  backup="$RESULTS_FILE.$(date -u +%Y%m%dT%H%M%SZ).bak"
  mv "$RESULTS_FILE" "$backup"
  echo "moved existing results document aside: $backup"
fi

mkdir -p "$LOG_DIR" "$(dirname "$RESULTS_FILE")"
log="$LOG_DIR/$target-$(date -u +%Y%m%dT%H%M%SZ).log"

# The header goes into the log as well as to the terminal: the log is the
# artifact a results PR links, and a log that does not state the matrix it was
# measured with — deviation and overrides included — cannot be triaged against
# the runbook after the fact.
{
  echo "backend:  $target ($example, --features $features)"
  echo "harness:  ${harness_args[*]}"
  if [ "$sqs_deviated" = 1 ]; then
    echo "deviation: drain corpus $SQS_DRAIN_MESSAGES, not the pinned 6000000 — see the comment in this script"
  fi
  for pinned in ${replaced[@]+"${replaced[@]}"}; do
    echo "override: replaces pinned '$pinned' — not comparable with the published matrix"
  done
  echo "results:  $RESULTS_FILE"
  echo "log:      $log"
} | tee "$log"

# The harness refuses to merge into a document produced on another host,
# toolchain or crate version. That refusal is correct: rerun with --fresh
# (or a new --results-file) rather than editing the provenance block.
cargo run -q --release --example "$example" --features "$features" -- \
  "${harness_args[@]}" 2>&1 | tee -a "$log"
