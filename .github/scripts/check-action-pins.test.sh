#!/usr/bin/env bash
#
# Tests for check-action-pins.sh.
#
# The checker is a security gate, so the asymmetry between its two failure
# modes is the whole design: a spurious rejection is a confusing CI failure
# someone fixes in a minute, while a silent acceptance is an unpinned action
# running in a job that holds the signing key. Most of what is asserted below
# is therefore "this shape is rejected", including several that a naive
# line-matching checker accepts.
#
# Plain bash rather than a framework: the repo has no shell-test harness, and
# adding one to cover two scripts costs more than it returns.
#
# Usage: bash .github/scripts/check-action-pins.test.sh
# Run from the repository root.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK="${HERE}/check-action-pins.sh"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

passed=0
failed=0

# A 40-hex SHA, used wherever the value itself is not what is under test.
SHA=d23441a48e516b6c34aea4fa41551a30e30af803

fail() {
  failed=$((failed + 1))
  printf 'FAIL  %s\n' "$1"
  shift
  while [ "$#" -gt 0 ]; do printf '        %s\n' "$1"; shift; done
}

pass() {
  passed=$((passed + 1))
  printf 'ok    %s\n' "$1"
}

# fixture <name> <<'YAML' ... YAML   -> echoes the path it wrote
fixture() {
  local path="${WORK}/$1"
  mkdir -p "$(dirname "$path")"
  cat > "$path"
  printf '%s' "$path"
}

# expect_exit <expected-status> <description> <args...>
# Also asserts that a rejection actually says something: an exit-1 with no
# output is a gate nobody can act on.
expect_exit() {
  local want="$1" desc="$2"; shift 2
  local out status
  out="$( (cd "$REPO_ROOT" && "$CHECK" "$@") 2>&1 )"
  status=$?
  if [ "$status" -ne "$want" ]; then
    fail "$desc" "expected exit ${want}, got ${status}" "output:" "$out"
    return
  fi
  if [ "$want" -ne 0 ] && [ -z "$out" ]; then
    fail "$desc" "exited ${status} but said nothing"
    return
  fi
  pass "$desc"
}

# expect_message <expected-status> <substring> <description> <args...>
expect_message() {
  local want="$1" needle="$2" desc="$3"; shift 3
  local out status
  out="$( (cd "$REPO_ROOT" && "$CHECK" "$@") 2>&1 )"
  status=$?
  if [ "$status" -ne "$want" ]; then
    fail "$desc" "expected exit ${want}, got ${status}" "output:" "$out"
    return
  fi
  case "$out" in
    *"$needle"*) pass "$desc" ;;
    *) fail "$desc" "output did not mention '${needle}'" "output:" "$out" ;;
  esac
}

if [ ! -x "$CHECK" ]; then
  printf 'FAIL  %s is missing or not executable\n' "$CHECK"
  exit 1
fi

# --- accepted -----------------------------------------------------------

f="$(fixture pinned.yml <<YAML
jobs:
  build:
    steps:
      - uses: actions/checkout@${SHA} # v6.1.0
      - uses: some/action@${SHA} # 1.91 branch
YAML
)"
expect_exit 0 "accepts SHA-pinned entries with a release comment" "$f"

f="$(fixture subpath.yml <<YAML
jobs:
  build:
    steps:
      - uses: owner/repo/sub/dir@${SHA} # v3.2.1
YAML
)"
expect_exit 0 "accepts a subpath action reference" "$f"

# The token appearing in a step name, in a trailing comment and inside a shell
# body is not an action reference. A checker that flags these is one people
# route around, which is how a gate stops being a gate.
f="$(fixture prose.yml <<YAML
jobs:
  build:
    steps:
      - name: "Audit every uses: entry in this file"
        run: |
          echo "uses: actions/checkout@v6"
          grep -c 'uses:' .github/workflows/publish.yml
      - run: ./notes.sh # every uses: entry must carry a full SHA
      - uses: actions/checkout@${SHA} # v6.1.0
YAML
)"
expect_exit 0 "does not flag the token in a name, a comment or a run body" "$f"

# A step input that happens to be named 'uses' is a value, not a reference.
f="$(fixture with_input.yml <<YAML
jobs:
  build:
    steps:
      - uses: some/action@${SHA} # v1.0.0
        with:
          uses: legacy-mode
          other: actions/checkout@v6
YAML
)"
expect_exit 0 "does not treat a 'with:' input named uses as a reference" "$f"

f="$(fixture blockscalar_step.yml <<YAML
jobs:
  build:
    steps:
      - run: |
          # a comment inside the body
          uses: not-really/a-step@v1
      - uses: actions/checkout@${SHA} # v6.1.0
YAML
)"
expect_exit 0 "ignores a line inside a sequence-item block scalar" "$f"

# The fixture above reopens the scan at the *next step*, which the dash column
# happens to get right. The case it does not cover is a reference that is a
# sibling key of the same step: a block scalar's body is nested under its key,
# so the skipped region ends at that key's column, not at the dash. Getting it
# wrong does not raise a false alarm, it silently accepts -- the one outcome
# this checker exists to rule out.
f="$(fixture blockscalar_dash_sibling_uses.yml <<'YAML'
jobs:
  build:
    steps:
      - name: >-
          A step name long enough to fold
        uses: actions/checkout@v6
YAML
)"
expect_message 1 "actions/checkout@v6" "rejects a reference after a folded scalar opened on the dash line" "$f"

f="$(fixture blockscalar_dash_sibling_literal.yml <<'YAML'
jobs:
  build:
    steps:
      - name: |
          A step name
        uses: actions/checkout@main
YAML
)"
expect_message 1 "actions/checkout@main" "rejects a reference after a literal scalar opened on the dash line" "$f"

# A long `if:` folded over several lines is the idiomatic shape for this, which
# makes the case realistic rather than a curiosity.
f="$(fixture blockscalar_dash_if_sibling.yml <<'YAML'
jobs:
  build:
    steps:
      - if: >-
          github.event_name == 'push' &&
          github.ref == 'refs/heads/main'
        uses: actions/checkout@v6
YAML
)"
expect_message 1 "actions/checkout@v6" "rejects a reference after a folded if: opened on the dash line" "$f"

# The mirror of the three above: the same shape with a proper pin must still
# pass, so the fix reopens the scan rather than merely rejecting more.
f="$(fixture blockscalar_dash_sibling_pinned.yml <<YAML
jobs:
  build:
    steps:
      - name: >-
          A step name long enough to fold
        uses: actions/checkout@${SHA} # v6.1.0
YAML
)"
expect_exit 0 "accepts a pinned reference after a folded scalar on the dash line" "$f"

# The catch-all below must not turn ordinary workflow syntax into a violation.
# A flow sequence of scalars is readable -- it cannot hold a key -- and `${{ }}`
# is a GitHub expression, not a flow mapping. Rejecting these would make the
# gate unusable on the very workflows it guards.
f="$(fixture flow_scalars.yml <<YAML
on:
  push:
    branches: [main]
    tags: ["v*"]
jobs:
  build:
    if: \${{ github.event_name == 'push' && !contains(github.ref, '[x]') }}
    strategy:
      matrix: \${{ fromJSON('{"os":["ubuntu-latest"]}') }}
    steps:
      - "name": Check the tree
        run: echo ok
      - uses: actions/checkout@${SHA} # v6.1.0
YAML
)"
expect_exit 0 "accepts flow sequences of scalars, quoted keys and expressions" "$f"

# --- rejected: mutable refs ---------------------------------------------

f="$(fixture major_tag.yml <<'YAML'
jobs:
  build:
    steps:
      - uses: actions/checkout@v6
YAML
)"
expect_message 1 "actions/checkout@v6" "rejects a mutable major tag" "$f"

# The trap this whole change came out of: a version-shaped tag is still a
# repointable ref, so it is not a pin.
f="$(fixture version_tag.yml <<'YAML'
jobs:
  build:
    steps:
      - uses: cargo-bins/cargo-binstall@v1.20.1
YAML
)"
expect_message 1 "cargo-bins/cargo-binstall@v1.20.1" "rejects a full version tag" "$f"

f="$(fixture branch_ref.yml <<'YAML'
jobs:
  build:
    steps:
      - uses: dtolnay/rust-toolchain@1.91
YAML
)"
expect_message 1 "dtolnay/rust-toolchain@1.91" "rejects a branch ref" "$f"

f="$(fixture no_ref.yml <<'YAML'
jobs:
  build:
    steps:
      - uses: actions/checkout
YAML
)"
expect_exit 1 "rejects a reference with no ref at all" "$f"

f="$(fixture docker.yml <<'YAML'
jobs:
  build:
    steps:
      - uses: docker://alpine:3
YAML
)"
expect_exit 1 "rejects a docker:// reference" "$f"

f="$(fixture short_sha.yml <<'YAML'
jobs:
  build:
    steps:
      - uses: actions/checkout@d23441a
YAML
)"
expect_exit 1 "rejects an abbreviated SHA" "$f"

f="$(fixture upper_sha.yml <<'YAML'
jobs:
  build:
    steps:
      - uses: actions/checkout@D23441A48E516B6C34AEA4FA41551A30E30AF803 # v6.1.0
YAML
)"
expect_exit 1 "rejects a non-lowercase SHA" "$f"

# --- rejected: pinned but unreadable ------------------------------------

f="$(fixture no_comment.yml <<YAML
jobs:
  build:
    steps:
      - uses: actions/checkout@${SHA}
YAML
)"
expect_message 1 "comment" "rejects a SHA with no release comment" "$f"

f="$(fixture empty_comment.yml <<YAML
jobs:
  build:
    steps:
      - uses: actions/checkout@${SHA} #
YAML
)"
expect_exit 1 "rejects an empty release comment" "$f"

# --- rejected: forms the scanner cannot read ----------------------------

# Fail closed. GitHub parses this with a real YAML parser and would run the
# mutable ref; the scanner must not answer 'clean' for input it cannot read.
f="$(fixture flow_map.yml <<'YAML'
jobs:
  build:
    steps:
      - {uses: actions/checkout@v6}
YAML
)"
expect_message 1 "unrecognised" "rejects a flow-mapping step it cannot parse" "$f"

# Same key as far as GitHub is concerned. A scanner keyed on the literal
# characters 'uses:' never sees this one.
f="$(fixture quoted_key.yml <<'YAML'
jobs:
  build:
    steps:
      - "uses": actions/checkout@v6
YAML
)"
expect_exit 1 "rejects a quoted 'uses' key holding a mutable ref" "$f"

f="$(fixture quoted_value.yml <<'YAML'
jobs:
  build:
    steps:
      - uses: "actions/checkout@v6"
YAML
)"
expect_exit 1 "rejects a quoted mutable ref value" "$f"

# A double-quoted key is not its own characters: YAML decodes `\u0065` to `e`,
# so GitHub reads the key below as `uses` and runs the mutable ref. Anything
# that matches the literal characters `uses` is blind to it, which is why a key
# this scanner cannot read has to be a violation rather than a pass-through.
f="$(fixture escaped_key.yml <<'YAML'
jobs:
  build:
    steps:
      - "us\u0065s": actions/checkout@v6
YAML
)"
expect_message 1 "unrecognised" "rejects a quoted key holding a YAML escape sequence" "$f"

# Explicit-key syntax: valid YAML, the same mapping, and the key does not share
# a line with its value at all.
f="$(fixture explicit_key.yml <<'YAML'
jobs:
  build:
    steps:
      - ? uses
        : actions/checkout@v6
YAML
)"
expect_message 1 "unrecognised" "rejects an explicit '? key' entry" "$f"

# An anchor sits between the indent and the key, so the key is no longer where
# a line scanner looks for it.
f="$(fixture anchored_key.yml <<'YAML'
jobs:
  build:
    steps:
      - &step uses: actions/checkout@v6
YAML
)"
expect_message 1 "unrecognised" "rejects an anchor in key position" "$f"

# A flow sequence may hold a single-pair mapping with no braces at all: this is
# one step with a `uses` key, and it carries neither `{` nor a leading `- [`.
f="$(fixture flow_seq_pair.yml <<'YAML'
jobs:
  build:
    steps: [uses: actions/checkout@v6]
YAML
)"
expect_message 1 "unrecognised" "rejects a mapping hidden in a brace-less flow sequence" "$f"

# A flow collection left open across lines: no single line carries the whole
# thing, so no per-line rule can read it.
f="$(fixture flow_multiline.yml <<'YAML'
jobs:
  build:
    steps: [
      {uses: actions/checkout@v6}
    ]
YAML
)"
expect_message 1 "unrecognised" "rejects a flow collection spanning several lines" "$f"

# Inside a flow collection a quoted key needs no space before its `:` -- YAML
# allows the JSON spelling there, unlike in block style. So the same hidden
# step can be written with the colon adjacent, which no rule keyed on `: `
# sees at all.
f="$(fixture flow_seq_adjacent.yml <<'YAML'
jobs:
  build:
    steps: ["uses":actions/checkout@v6]
YAML
)"
expect_message 1 "unrecognised" "rejects an adjacent quoted key in a flow sequence" "$f"

f="$(fixture flow_map_adjacent.yml <<'YAML'
jobs:
  build:
    steps: [{"uses":actions/checkout@v6}]
YAML
)"
expect_message 1 "unrecognised" "rejects an adjacent quoted key in a flow mapping" "$f"

# A quote that never closes leaves the rest of the value on another line, so
# the scanner cannot know what the value ends up being.
f="$(fixture open_quote.yml <<'YAML'
jobs:
  build:
    env:
      PAYLOAD: "still going
        and going"
YAML
)"
expect_message 1 "unrecognised" "rejects a quoted value left open across lines" "$f"

# --- accepted: braces and colons inside ordinary scalars -----------------

# The flow rules must look at what the value *is*, not at which characters it
# contains. A quoted scalar is a scalar however much punctuation it holds, and
# rejecting these makes the gate unusable on workflows that pass JSON around.
f="$(fixture scalar_punctuation.yml <<YAML
on:
  push:
    branches: [main] # only: the default branch
env:
  PAYLOAD: '{"safe":true}'
  SHELL_SNIPPET: "for f in *; do echo \${f}; done # not a comment"
  COLONS: "a: b"
jobs:
  build:
    steps:
      - uses: actions/checkout@${SHA} # v6.1.0
YAML
)"
expect_exit 0 "accepts braces, colons and hashes inside quoted scalars" "$f"

# A plain scalar cannot open a flow collection -- that needs a \`[\` or \`{\` in
# the first position -- so punctuation later in one is just punctuation.
f="$(fixture plain_scalar_braces.yml <<YAML
env:
  TEMPLATE: prefix-{0}-suffix
  GLOB: src/**/[abc].rs
jobs:
  build:
    steps:
      - uses: actions/checkout@${SHA} # v6.1.0
YAML
)"
expect_exit 0 "accepts a plain scalar containing flow punctuation" "$f"

# \`permissions: {}\` is the recommended way to drop every token scope, so a gate
# that rejects it argues against the hardening it exists to enforce. An empty
# flow mapping is the one that provably holds no key.
f="$(fixture empty_flow_map.yml <<YAML
permissions: {}
jobs:
  build:
    permissions: { }
    steps:
      - uses: actions/checkout@${SHA} # v6.1.0
YAML
)"
expect_exit 0 "accepts an empty flow mapping" "$f"

f="$(fixture nonempty_flow_map.yml <<YAML
jobs:
  build:
    permissions: {contents: read}
    steps:
      - uses: actions/checkout@${SHA} # v6.1.0
YAML
)"
expect_message 1 "unrecognised" "rejects a non-empty flow mapping value" "$f"

# --- job-level reusable workflow calls ----------------------------------

f="$(fixture reusable_bad.yml <<'YAML'
jobs:
  call:
    uses: owner/repo/.github/workflows/wf.yml@main
YAML
)"
expect_exit 1 "rejects an unpinned reusable-workflow call at job level" "$f"

f="$(fixture reusable_good.yml <<YAML
jobs:
  call:
    uses: owner/repo/.github/workflows/wf.yml@${SHA} # v2.0.0
    with:
      thing: value
YAML
)"
expect_exit 0 "accepts a SHA-pinned reusable-workflow call at job level" "$f"

# --- local actions ------------------------------------------------------

mkdir -p "${WORK}/local-ok/.github/actions/setup"
cat > "${WORK}/local-ok/.github/actions/setup/action.yml" <<YAML
runs:
  using: composite
  steps:
    - uses: actions/checkout@${SHA} # v6.1.0
YAML
cat > "${WORK}/local-ok/wf.yml" <<'YAML'
jobs:
  build:
    steps:
      - uses: ./.github/actions/setup
YAML
out="$( (cd "${WORK}/local-ok" && "$CHECK" wf.yml) 2>&1 )"
if [ $? -eq 0 ]; then pass "accepts a local action whose own steps are pinned"
else fail "accepts a local action whose own steps are pinned" "output:" "$out"; fi

# A local composite action runs in the same job with the same permissions, so
# an unpinned third-party step inside it is the identical exposure.
mkdir -p "${WORK}/local-bad/.github/actions/setup"
cat > "${WORK}/local-bad/.github/actions/setup/action.yml" <<'YAML'
runs:
  using: composite
  steps:
    - uses: actions/checkout@v6
YAML
cat > "${WORK}/local-bad/wf.yml" <<'YAML'
jobs:
  build:
    steps:
      - uses: ./.github/actions/setup
YAML
out="$( (cd "${WORK}/local-bad" && "$CHECK" wf.yml) 2>&1 )"
status=$?
if [ "$status" -eq 1 ] && case "$out" in *action.yml*) true ;; *) false ;; esac; then
  pass "follows a local action reference into its own file"
else
  fail "follows a local action reference into its own file" "exit ${status}" "output:" "$out"
fi

# A local reference is a path, and a path is not a safe thing to hand an
# argument parser. `./a=b` reaches awk as a variable assignment and `./-setup`
# as a bundle of options, so awk reads neither file: it scans stdin, finds no
# reference, and succeeds. Scanning has to fail loudly there, because a scan
# that produces no records is indistinguishable from a file that is clean.
for odd in 'a=b' '-setup'; do
  root="${WORK}/local-odd-${odd}"
  mkdir -p -- "${root}/${odd}"
  cat > "${root}/${odd}/action.yml" <<'YAML'
runs:
  using: composite
  steps:
    - uses: actions/checkout@v6
YAML
  cat > "${root}/wf.yml" <<YAML
jobs:
  build:
    steps:
      - uses: ./${odd}
YAML
  out="$( (cd "$root" && "$CHECK" wf.yml) 2>&1 < /dev/null )"
  status=$?
  if [ "$status" -ne 0 ] && case "$out" in *actions/checkout@v6*) true ;; *) false ;; esac; then
    pass "reports the unpinned step inside a local action at ./${odd}"
  else
    fail "reports the unpinned step inside a local action at ./${odd}" \
      "exit ${status}" "output:" "$out"
  fi
done

# --- multiple files and usage errors ------------------------------------

good="$(fixture multi_good.yml <<YAML
jobs:
  build:
    steps:
      - uses: actions/checkout@${SHA} # v6.1.0
YAML
)"
bad="$(fixture multi_bad.yml <<'YAML'
jobs:
  build:
    steps:
      - uses: actions/setup-node@v4
YAML
)"
expect_message 1 "actions/setup-node@v4" "reports a violation in the second of two files" "$good" "$bad"

expect_exit 2 "exits 2 on a missing file" "${WORK}/does-not-exist.yml"
expect_exit 2 "exits 2 with no arguments"

# --- the real thing -----------------------------------------------------

expect_exit 0 "the repository's own publish.yml is fully pinned" \
  "${REPO_ROOT}/.github/workflows/publish.yml"

# ------------------------------------------------------------------------

printf '\n%d passed, %d failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
