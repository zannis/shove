#!/usr/bin/env bash
#
# Refuse to release a commit that CI never tested.
#
# `ci.yml` has a `paths:` filter, so a commit touching only workflows or docs
# gets no CI run *at all*. That is how v0.13.0 came to be published from
# b36dcff98a57535d06049bc003db0cdbd2bca4d1, a commit no test job ever saw. The
# checks that *were* green on it -- CodeQL, the docs Workers build -- say
# nothing about the crate, so an "are all checks green?" gate would have waved
# it through. Hence the two rules here:
#
#   1. Name the ci.yml legs that are required, rather than accepting whatever
#      checks happen to be attached to the commit.
#   2. Treat an absent run as a failure. "No run" is not "nothing to check";
#      it is precisely the v0.13.0 case.
#
# Usage: require-green-ci.sh <commit-ish>
#
# The argument is resolved to a full SHA before anything is looked up, so an
# abbreviated SHA, a tag or a branch all work from a terminal.
#
# Env:
#   GITHUB_REPOSITORY  owner/repo (set for free inside Actions).
#   GH_TOKEN           token with `actions: read` and `contents: read` on that
#                      repo -- the latter only for resolving the argument.
#   GATE_LEVEL         `error` (default) or `warning` -- the Actions annotation
#                      level used to report a bad verdict. The exit status is
#                      the same either way; only the annotation changes, so a
#                      caller that intends to continue does not litter the run
#                      with red herrings.

set -euo pipefail

SHA_ARG="${1:-}"
if [ -z "$SHA_ARG" ]; then
  echo "usage: $0 <commit-ish>" >&2
  exit 2
fi

REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set to owner/repo}"
GATE_LEVEL="${GATE_LEVEL:-error}"

# Every job in ci.yml. `check` covers fmt/clippy/`cargo test
# --no-default-features`/`cargo publish --dry-run`, `feature-lint` covers
# clippy for each single-backend feature set, `coverage` is the per-backend
# integration matrix; a release wants all of them, so all of them are listed.
# Keep this in sync with ci.yml -- a job renamed there and not here fails the
# gate closed, which is the safe direction.
REQUIRED_JOBS="check feature-lint msrv audit"
REQUIRED_COVERAGE_LEGS="inmemory rabbitmq aws-sns-sqs nats kafka kafka-schema-registry redis-streams"

# GitHub renders a matrix job as "<job> (<value>, <value>, ...)" over the
# `matrix.include` keys, and truncates the result at 100 characters -- the
# kafka-schema-registry leg really does arrive as
# "coverage (kafka-schema-registry, kafka,kafka-schema-registry,protobuf, -E 'binary(/^kafka_schema_..."
# So the coverage legs are matched on the first parenthesised value
# (`matrix.name`) instead of on the whole string, which neither the truncation
# nor an edit to a `features` list can move.
COVERAGE_JOB="coverage"

annotate() {
  # Actions renders `::error::`/`::warning::` as annotations; outside Actions
  # that prefix is just noise, so fall back to a plain label.
  if [ -n "${GITHUB_ACTIONS:-}" ]; then
    printf '::%s::%s\n' "$GATE_LEVEL" "$1" >&2
  else
    printf '%s: %s\n' "$GATE_LEVEL" "$1" >&2
  fi
}

# `head_sha=` on the runs API matches the full 40-character SHA and nothing
# else. An abbreviated SHA matches no run and comes back empty -- which at this
# point in the script is indistinguishable from a commit CI genuinely never
# ran, so the gate would report a well-tested commit as "never tested". Resolve
# first and use the resolved value everywhere, and that whole class of wrong
# verdict goes away; accepting tags and branch names is a free side effect.
#
# This costs the release path nothing: publish.yml passes `github.sha`, already
# full-length, which resolves to itself. A resolution that fails exits 2 rather
# than 1, keeping "you gave me something that is not a commit" distinct from
# "this commit did not pass CI" -- publish.yml treats both as failure, so the
# gate still fails closed either way.
echo "Resolving ${SHA_ARG} to a commit in ${REPO}..."
if ! SHA=$(gh api "repos/${REPO}/commits/${SHA_ARG}" --jq '.sha') || [ -z "$SHA" ]; then
  annotate "Could not resolve '${SHA_ARG}' to a commit in ${REPO} (the API error is above). This is not a verdict about CI: the argument never named a commit, so there was nothing to look up a run for. Pass a commit SHA, tag or branch that exists in ${REPO}."
  exit 2
fi

if [ "$SHA" != "$SHA_ARG" ]; then
  echo "  ${SHA_ARG} -> ${SHA}"
fi

echo "Resolving ci.yml runs for ${SHA} in ${REPO}..."

runs=$(gh api \
  "repos/${REPO}/actions/workflows/ci.yml/runs?head_sha=${SHA}&per_page=100" \
  --paginate \
  --jq '.workflow_runs[] | [.id, .run_attempt, .status, (.conclusion // ""), .html_url] | @tsv')

if [ -z "$runs" ]; then
  annotate "No ci.yml run exists for ${SHA}. That commit has never been tested: ci.yml's \`paths:\` filter skips commits that touch only workflows or docs, so a green tick from CodeQL or the docs build means nothing here. Land the release from a commit that ran CI, or push a no-op change under one of ci.yml's \`paths:\` entries and let it run."
  exit 1
fi

# Re-runs reuse the run id and bump `run_attempt`, and the API reports the
# latest attempt -- so "most recent run" is simply the largest id. Sorting
# rather than taking the first element matters because the listing is not
# documented to be ordered.
latest=$(printf '%s\n' "$runs" | sort -k1,1nr | head -n1)
run_id=$(printf '%s' "$latest" | cut -f1)
run_attempt=$(printf '%s' "$latest" | cut -f2)
run_status=$(printf '%s' "$latest" | cut -f3)
run_conclusion=$(printf '%s' "$latest" | cut -f4)
run_url=$(printf '%s' "$latest" | cut -f5)

echo "Latest ci.yml run: ${run_url} (attempt ${run_attempt}, status=${run_status}, conclusion=${run_conclusion:-<none>})"

# Check `status` before `conclusion`, never the other way round: a run still
# going has no conclusion yet, and both `""` and `null` are falsy in the
# obvious `conclusion // "failure"` shorthand -- which would report a running
# leg as a failure, or worse, a defaulted-to-success as a pass. Neither
# in-progress nor queued is a pass, so both stop the release here.
if [ "$run_status" != "completed" ]; then
  annotate "ci.yml run ${run_id} for ${SHA} is '${run_status}', not completed. An unfinished run is not a green one; wait for ${run_url} to finish and dispatch again."
  exit 1
fi

if [ "$run_conclusion" != "success" ]; then
  annotate "ci.yml run ${run_id} for ${SHA} concluded '${run_conclusion:-<none>}', not 'success'. See ${run_url}."
  exit 1
fi

# A green *run* is not yet proof that the legs ran: a job that is skipped, or
# a matrix entry deleted from ci.yml, still leaves the run green. Enumerate the
# jobs and require each named leg by name. `?filter=latest` is the default, so
# these are the jobs of the newest attempt.
jobs_tsv=$(gh api \
  "repos/${REPO}/actions/runs/${run_id}/jobs?per_page=100" \
  --paginate \
  --jq '.jobs[] | [.name, .status, (.conclusion // "")] | @tsv')

if [ -z "$jobs_tsv" ]; then
  annotate "ci.yml run ${run_id} reports no jobs at all. See ${run_url}."
  exit 1
fi

# $1 = leg label, $2 = "exact" for a plain job, "coverage" for a matrix leg.
verify_leg() {
  local label="$1" mode="$2" line status conclusion
  line=$(printf '%s\n' "$jobs_tsv" | awk -F'\t' -v leg="$label" -v mode="$mode" -v job="$COVERAGE_JOB" '
    mode == "exact" && $1 == leg { print; exit }
    mode == "coverage" && (index($1, job " (" leg ",") == 1 || $1 == job " (" leg ")") { print; exit }
  ')

  if [ -z "$line" ]; then
    annotate "ci.yml run ${run_id} has no '${label}' job. The run is green but that leg never ran, so nothing tested it. See ${run_url}."
    return 1
  fi

  status=$(printf '%s' "$line" | cut -f2)
  conclusion=$(printf '%s' "$line" | cut -f3)
  if [ "$status" != "completed" ] || [ "$conclusion" != "success" ]; then
    annotate "ci.yml leg '${label}' is status=${status} conclusion=${conclusion:-<none>}, which is not a pass. See ${run_url}."
    return 1
  fi

  printf '  ok   %s\n' "$label"
}

echo "Verifying required legs of run ${run_id}:"
failed=0
for leg in $REQUIRED_JOBS; do
  verify_leg "$leg" exact || failed=1
done
for leg in $REQUIRED_COVERAGE_LEGS; do
  verify_leg "$leg" coverage || failed=1
done

if [ "$failed" -ne 0 ]; then
  annotate "${SHA} does not have a green ci.yml run covering every required leg; refusing to release it."
  exit 1
fi

echo "All required ci.yml legs passed for ${SHA}."
