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
#
# Recognising `uses` cannot be the whole of it, though, because a scanner that
# compares literal characters cannot enumerate the ways YAML spells a key:
# `"uses":` decodes to the same key, and so do an explicit `? uses` entry,
# an anchored key and a single-pair mapping inside a flow sequence. Each of
# those is a line no rule below matches, and matching nothing has to mean
# rejected rather than accepted. So the burden is inverted by a final catch-all:
# a line is accepted only once it is shown to hold no key, or to hold one this
# scanner can read literally. That is what makes the promise above true rather
# than aspirational; it also means an exotic-but-valid workflow gets a "rewrite
# this" error, which is the trade this gate exists to make.
#
# The same reasoning runs the other way for the value, and the symmetry matters
# because the two errors are not equally cheap. Whether a value can hold a key
# at all is decided by what the value *is* -- a flow collection, a quoted
# scalar, a plain scalar -- and never by which characters appear somewhere in
# it. Searching the raw line for a `{` both rejects `PAYLOAD: '{"safe":true}'`,
# where the brace is a character inside a scalar, and misses
# `steps: ["uses":actions/checkout@v6]`, where a quoted key needs no space
# before its `:` because YAML allows the JSON spelling inside flow. Reading the
# value's form instead gets both right: quoted scalars are stepped over whole,
# and a flow collection is walked with its quotes honoured.

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

    # Position of the `:` that ends a block mapping key: the first one followed
    # by a space, a tab or the end of the line. 0 when the line has no key at
    # all, which is what a sequence entry or a scalar continuation looks like.
    # Spelled out rather than matched so it stays the same under every awk.
    function key_colon(s,   i, c, n) {
      n = length(s)
      for (i = 1; i <= n; i++) {
        if (substr(s, i, 1) != ":") continue
        c = substr(s, i + 1, 1)
        if (c == "" || c == " " || c == "\t") return i
      }
      return 0
    }

    # Remove `${{ ... }}` expressions. They are GitHub syntax, not YAML flow
    # collections, and their bodies routinely carry braces and brackets of
    # their own -- `fromJSON('"'"'{"os":["a"]}'"'"')` is an ordinary matrix. Scanned to
    # the closing `}}` rather than matched, so an inner `}` does not end it.
    function strip_expressions(s,   out, i, j) {
      out = ""
      while ((i = index(s, "${{")) > 0) {
        out = out substr(s, 1, i - 1)
        s = substr(s, i + 3)
        j = index(s, "}}")
        # Unterminated on this line: hand back something the flow rule rejects
        # rather than quietly swallowing the rest of the value.
        if (j == 0) return out "{" s
        s = substr(s, j + 2)
      }
      return out s
    }

    # The position of the quote closing the scalar that starts at `i`, or 0
    # when it is never closed on this line. `'"'"''"'"'` inside a single-quoted scalar
    # and a backslash escape inside a double-quoted one do not close it.
    function quoted_end(s, i,   q, n, c) {
      q = substr(s, i, 1)
      n = length(s)
      i++
      while (i <= n) {
        c = substr(s, i, 1)
        if (q == "'"'"'") {
          if (c == "'"'"'") {
            if (substr(s, i + 1, 1) != "'"'"'") return i
            i += 2
            continue
          }
        } else {
          if (c == "\\") { i += 2; continue }
          if (c == "\"") return i
        }
        i++
      }
      return 0
    }

    # Walk a flow collection, stepping over quoted scalars whole. Flow is the
    # one value form on a line that can carry a key of its own, and `:` is how
    # it spells one -- with no space after it required when the key is quoted,
    # since YAML allows the JSON spelling inside flow. That is what makes
    # `["uses":actions/checkout@v6]` a step while looking like a list of one
    # string. Returns "" when the line is readable, else why it is not.
    function flow_probe(s,   i, n, c, prev, depth, e, j) {
      depth = 0
      n = length(s)
      for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        prev = " "
        if (i > 1) prev = substr(s, i - 1, 1)
        # A `#` only opens a comment after a space, so `[a#b]` keeps its hash.
        if (c == "#" && (prev == " " || prev == "\t")) break
        if (c == "\"" || c == "'"'"'") {
          e = quoted_end(s, i)
          if (e == 0) return "open"
          i = e
          continue
        }
        if (c == "{") {
          # `permissions: {}` is the recommended way to drop every token
          # scope, and an empty mapping demonstrably holds no key. Any other
          # mapping does, and this scanner cannot read one.
          j = i + 1
          while (substr(s, j, 1) == " " || substr(s, j, 1) == "\t") j++
          if (substr(s, j, 1) != "}") return "mapping"
          i = j
          continue
        }
        if (c == ":") return "mapping"
        if (c == "[") depth++
        else if (c == "]" || c == "}") depth--
      }
      if (depth != 0) return "open"
      return ""
    }

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

    # The catch-all, and the rule that makes the promise at the top of this file
    # true. Everything above recognises a shape; a line reaching here matched
    # none of them, and "matched nothing" must not mean "accepted". The rules
    # above cannot be an exhaustive list of the ways YAML spells a key -- the
    # `uses:` match compares literal characters, so `"us\u0065s":` is the same
    # key to GitHub and a different string to this scanner. So the burden is
    # inverted here: a line passes only by being provably unable to introduce a
    # key that this scanner misread.
    {
      head = $0
      sub(/^[ ]*/, "", head)
      # Sequence dashes are indentation as far as a key is concerned.
      while (substr(head, 1, 1) == "-" && \
             (length(head) == 1 || substr(head, 2, 1) == " " || substr(head, 2, 1) == "\t")) {
        head = substr(head, 2)
        sub(/^[ \t]*/, "", head)
      }

      # `? key` puts the key on a line of its own and its value on another, so
      # no line-based rule can pair them.
      if (substr(head, 1, 1) == "?" && \
          (length(head) == 1 || substr(head, 2, 1) == " " || substr(head, 2, 1) == "\t")) {
        violation("unrecognised form: an explicit `? key` entry, which this checker cannot read. Rewrite it as a `key: value` mapping.")
        next
      }
      # An anchor, alias or tag sits between the indent and the key, so the key
      # is no longer where a line scanner looks for it -- and an alias key is
      # not even present on the line that uses it.
      if (head ~ /^[&*!%@`]/) {
        violation("unrecognised form: an anchor, alias or tag in key position, which this checker cannot resolve. Write the key literally.")
        next
      }
      if (head ~ /^[[\]{},]/) {
        violation("unrecognised form: this line uses YAML flow style, which this checker cannot read. Rewrite it as a block mapping.")
        next
      }

      ci = key_colon(head)
      # No key on this line: a sequence entry, or the continuation of a
      # multi-line scalar. Neither can be a `uses` key.
      if (ci == 0) next

      key = substr(head, 1, ci - 1)
      value = substr(head, ci + 1)
      sub(/[ \t]+$/, "", key)

      # A plain key is its own characters; a quoted one is only its own
      # characters while it holds no escape. Either way, the `uses` rule ran
      # first, so a key this branch can read is a key that is not `uses`.
      if (key ~ /["'"'"'\\]/ && \
          key !~ /^"[^"\\]*"$/ && key !~ /^'"'"'[^'"'"']*'"'"'$/) {
        violation("unrecognised form: this checker could not read the key on this line. Write it as a plain or simply-quoted literal, so a `uses` key cannot hide in an escape.")
        next
      }

      value = strip_expressions(value)
      sub(/^[ \t]+/, "", value)

      # No value on this line: it is nested on the lines below, and those are
      # scanned on their own terms.
      if (value == "" || substr(value, 1, 1) == "#") next

      # An anchor, alias or tag in value position hides the node behind it from
      # a line scanner, exactly as one in key position does.
      if (value ~ /^[&*!]/) {
        violation("unrecognised form: an anchor, alias or tag in value position, which this checker cannot resolve. Write the value literally.")
        next
      }

      # What the value *is* decides this, not which characters it contains. A
      # quoted scalar is a scalar however much punctuation it holds, so its
      # braces and colons are characters rather than structure -- testing the
      # raw line for a `{` rejects `PAYLOAD: '"'"'{"safe":true}'"'"'`, an ordinary
      # value. The acceptance is only sound because the closing quote is found
      # on this line: a quote left open continues onto lines read out of
      # context, which is the one thing this scanner must not answer clean for.
      vc = substr(value, 1, 1)
      if (vc == "\"" || vc == "'"'"'") {
        e = quoted_end(value, 1)
        if (e == 0) {
          violation("unrecognised form: a quoted value left open across lines, which this checker cannot read. Close it on one line, or use a `|` or `>` block scalar.")
          next
        }
        rest = substr(value, e + 1)
        sub(/^[ \t]+/, "", rest)
        if (rest != "" && substr(rest, 1, 1) != "#") {
          violation("unrecognised form: this checker could not read the value on this line. Write it as a single plain or quoted scalar.")
        }
        next
      }

      # A flow collection is the only value form left that can hold a key --
      # a mapping, or a sequence, which YAML lets hold a single-pair mapping
      # with no braces at all, making `steps: [uses: actions/checkout@v6]` a
      # step. A sequence of scalars -- `branches: [main]` -- holds no key.
      if (vc == "[" || vc == "{") {
        why = flow_probe(value)
        if (why == "mapping") {
          violation("unrecognised form: this line uses YAML flow style, which this checker cannot read. Rewrite it as a block mapping.")
        } else if (why == "open") {
          violation("unrecognised form: a flow collection left open across lines, which this checker cannot read. Rewrite it as a block mapping.")
        }
        next
      }

      # A plain scalar. It cannot open a flow collection -- that needs a `[` or
      # a `{` in first position, handled above -- and block style forbids `: `
      # inside one, so there is no key for this scanner to have misread.
      next
    }
  ' < "$1"
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

  # A scan that fails produces no records, which is exactly what a clean file
  # produces. Reading the status is the only thing that tells the two apart, so
  # it is read before the records are looked at rather than after.
  if ! records="$(scan "$file")"; then
    echo "$0: failed to scan '$file'" >&2
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
  done <<< "$records"
done

if [ "$status" -eq 0 ]; then
  printf 'Every action reference is pinned to a commit SHA: %s\n' "$*"
else
  echo "Refusing the workflow above: an action on a mutable ref runs in a job that can sign and publish releases." >&2
fi

exit "$status"
