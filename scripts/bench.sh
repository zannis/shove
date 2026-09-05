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

usage() {
  sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'
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
echo "results:  $RESULTS_FILE"
echo "log:      $log"

# The harness refuses to merge into a document produced on another host,
# toolchain or crate version. That refusal is correct: rerun with --fresh
# (or a new --results-file) rather than editing the provenance block.
cargo run -q --release --example "$example" --features "$features" -- \
  "${MATRIX[@]}" "${extra[@]+"${extra[@]}"}" --results-file "$RESULTS_FILE" \
  2>&1 | tee "$log"
