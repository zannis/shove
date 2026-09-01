#!/usr/bin/env bash
#
# Refuse a workflow that executes an action from a mutable ref.
#
# The publish job holds `contents: write` and `id-token: write`, imports the
# long-lived release signing key and mints a crates.io publish token. Every
# action in that job runs with all of it in reach, and can equally poison the
# runner so a later step signs and publishes something else. A tag or a branch
# is a ref its owner can repoint at any commit, so "we reviewed that release"
# is only a claim; a commit SHA makes it checkable.
#
# Two rules, both enforced:
#
#   1. Every action reference is `owner/repo[/path]@<40 lowercase hex>`.
#      Including refs that look specific: `@v1.20.1` was a lightweight tag on
#      cargo-binstall, and a lightweight tag moves like any other.
#   2. A trailing `# comment` names the release the SHA came from. A bare hex
#      string is unreviewable, and an unenforced convention is not one.
#
# Usage: check-action-pins.sh <workflow.yml>...
# Exit:  0 clean, 1 violations found, 2 bad usage or unreadable file.
#
# Env:
#   GITHUB_ACTIONS  when set, violations are emitted as `::error::` annotations
#                   so they attach to the offending line in the UI.
#
# ## Reading YAML without a YAML parser
#
# Deliberate: this runs as a gate inside the privileged job, where "the parser
# was not installed so we skipped the check" is exactly the outcome being
# designed against. `yq` is not guaranteed on every runner image and PyYAML is
# not guaranteed in every python3. So the scanner is line-based -- with the one
# property that matters made explicit: it never answers "clean" for input it
# could not read. A form it does not understand is a violation, not a skip.
#
# It recognises the key only where YAML can start one -- after the leading
# indent, or after sequence dashes -- rather than anywhere the characters
# appear. That is what keeps a step `name:`, an inline comment or a shell line
# mentioning the token from being flagged, without the scanner having to
# understand any of them.

set -uo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <workflow.yml>..." >&2
  exit 2
fi

annotate() {
  # $1 = file, $2 = line, $3 = message
  if [ -n "${GITHUB_ACTIONS:-}" ]; then
    printf '::error file=%s,line=%s::%s\n' "$1" "$2" "$3"
  fi
  printf '%s:%s: %s\n' "$1" "$2" "$3" >&2
}

# Emits one record per finding, tab-separated:
#   VIOLATION <line> <message>
#   LOCAL     <line> <path>
scan() {
  awk '
    function indent_of(s,   n) { n = 0; while (substr(s, n + 1, 1) == " ") n++; return n }
    function trim(s) { sub(/^[ \t]+/, "", s); sub(/[ \t]+$/, "", s); return s }
    function unquote(s) {
      if (s ~ /^".*"$/ || s ~ /^'"'"'.*'"'"'$/) return substr(s, 2, length(s) - 2)
      return s
    }
    function violation(msg) { printf "VIOLATION\t%d\t%s\n", NR, msg }

    BEGIN { in_skip = 0; skip_indent = 0 }

    {
      sub(/\r$/, "")          # CRLF checkouts
      if (NR == 1) sub(/^\xef\xbb\xbf/, "")
    }

    /^[ \t]*$/ { next }

    # YAML forbids tabs in indentation, so a line indented with one is either
    # not the YAML we think it is or is about to be rejected by Actions anyway.
    # Either way this scanner cannot reason about its nesting.
    /^ *\t/ {
      violation("indented with a tab; this checker cannot determine nesting here. Use spaces.")
      next
    }

    {
      ind = indent_of($0)
      # Inside a block scalar or a `with:` block: everything nested deeper is
      # opaque content or step inputs, never an action reference. A line at or
      # left of the owning key closes the region and is reprocessed below.
      if (in_skip && ind > skip_indent) next
      in_skip = 0
    }

    # Whole-line comments.
    /^[ ]*#/ { next }

    # Flow style. `- {uses: actions/checkout@v6}` is valid, Actions runs it,
    # and a line scanner cannot follow it. Fail closed rather than skip.
    /[{,][ \t]*("uses"|'"'"'uses'"'"'|uses)[ \t]*:/ ||
    /^[ ]*-[ ]*[[{]/ {
      violation("unrecognised form: this line uses YAML flow style, which this checker cannot read. Rewrite it as a block mapping.")
      next
    }

    # An action reference: `uses` as a key, optionally quoted, at a position
    # where YAML can start one. Covers step entries, `- uses:`, and a job-level
    # reusable-workflow call, which needs pinning for the same reason.
    match($0, /^[ ]*(-[ ]+)*("uses"|'"'"'uses'"'"'|uses)[ \t]*:/) {
      rest = trim(substr($0, RSTART + RLENGTH))

      comment = ""
      if (match(rest, /[ \t]+#/)) {
        comment = trim(substr(rest, RSTART + RLENGTH))
        rest = trim(substr(rest, 1, RSTART - 1))
      }
      value = unquote(rest)

      if (value == "") {
        violation("could not read the value of this reference. Put it on the same line as the key.")
        next
      }
      if (value ~ /^\.\//) {
        printf "LOCAL\t%d\t%s\n", NR, value
        next
      }
      if (value !~ /^[A-Za-z0-9._-]+\/[A-Za-z0-9._\/-]+@[0-9a-f]{40}$/) {
        violation(sprintf("`%s` is not pinned. A tag or branch -- including one that looks like a version -- is a ref its owner can repoint; use the full 40-character lowercase commit SHA.", value))
        next
      }
      if (comment == "" || comment ~ /^#*$/) {
        violation(sprintf("`%s` is pinned but has no release comment. Add one naming the release, e.g. `# v1.2.3`, so the pin stays reviewable.", value))
        next
      }
      next
    }

    # Openers for the regions skipped above. Checked after the reference match
    # so a `uses:` key is never mistaken for one.
    match($0, /^[ ]*(-[ ]+)*[^ :]+[ \t]*:[ \t]*[|>][0-9]*[+-]?[ \t]*(#.*)?$/) {
      in_skip = 1
      # The column of the key, not of the sequence dash -- see the note on
      # `with:` below, which this has to agree with. For `- name: >-` the
      # folded body is nested under the key, so the region ends at the column
      # of `name`; measuring from the dash instead swallows every sibling key
      # of that step, and a `uses:` among them is then accepted in silence.
      match($0, /^[ ]*(-[ ]+)*/)
      skip_indent = RLENGTH
      next
    }
    match($0, /^[ ]*(-[ ]+)*("with"|'"'"'with'"'"'|with)[ \t]*:[ \t]*(#.*)?$/) {
      in_skip = 1
      # The indent of the key itself, not of the sequence dash: for `- run: |`
      # the block body is nested under the key, and the next sibling step sits
      # at the dash, which must reopen scanning.
      match($0, /^[ ]*(-[ ]+)*/)
      skip_indent = RLENGTH
      next
    }
  ' "$1"
}

status=0

# Worklist rather than recursion: a local composite action runs in the same job
# with the same permissions, so an unpinned third-party step inside one is the
# identical exposure and has to be followed. `seen` stops a cycle between two
# local actions from spinning.
queue=("$@")
seen=""

while [ "${#queue[@]}" -gt 0 ]; do
  file="${queue[0]}"
  queue=("${queue[@]:1}")

  case " $seen " in *" $file "*) continue ;; esac
  seen="$seen $file"

  if [ ! -f "$file" ]; then
    echo "$0: cannot read '$file'" >&2
    exit 2
  fi

  while IFS=$'\t' read -r kind line payload; do
    case "$kind" in
      VIOLATION)
        annotate "$file" "$line" "$payload"
        status=1
        ;;
      LOCAL)
        # `./path` is relative to the repository root, which is where this
        # script is run from.
        local_dir="${payload#./}"
        found=0
        for candidate in "$local_dir/action.yml" "$local_dir/action.yaml" "$local_dir"; do
          if [ -f "$candidate" ]; then
            queue+=("$candidate")
            found=1
            break
          fi
        done
        if [ "$found" -eq 0 ]; then
          annotate "$file" "$line" "local reference \`$payload\` does not resolve to a file, so its contents cannot be checked."
          status=1
        fi
        ;;
    esac
  done < <(scan "$file")
done

if [ "$status" -eq 0 ]; then
  printf 'Every action reference is pinned to a commit SHA: %s\n' "$*"
else
  echo "Refusing the workflow above: an action on a mutable ref runs in a job that can sign and publish releases." >&2
fi

exit "$status"
