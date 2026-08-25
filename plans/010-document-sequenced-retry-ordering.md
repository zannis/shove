# Plan 010: Document that Retry breaks per-key ordering on sequenced topics

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before moving on. On any STOP condition, stop and
> report. Your reviewer maintains `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- docs/pages/guides/sequenced.mdx docs/pages/backends/kafka.mdx docs/pages/backends/nats.mdx src/macros.rs src/topology.rs`
> Compare any drifted file's cited text below before proceeding; mismatch = STOP.

## Status

- **Priority**: P1 (it's a false guarantee in shipped docs)
- **Effort**: S
- **Risk**: LOW (docs + rustdoc only — no code behavior change)
- **Depends on**: none
- **Category**: docs
- **Planned at**: commit `e902d7c` (main), 2026-07-02
- **Maintainer decision**: document now; a NATS in-place-retry fix is being
  designed separately (plan 019). Do not change retry behavior here.

## Why this matters

The docs promise sequenced topics deliver "messages with the same key … one at
a time, in strict publish order" (`docs/pages/guides/sequenced.mdx:3`). The
implementation does not honor that across retries: on `Outcome::Retry`, every
backend republishes an incremented copy of the message to the **tail** of the
shard/partition (NATS: `src/backends/nats/consumer.rs:263-284`
`hold_then_republish` publishes to `msg.subject` then acks the original; Kafka:
`route_outcome`'s FIFO path awaits the same delayed republish inline,
`src/backends/kafka/consumer.rs:1616-1631`). Messages of the same key already
queued behind the failed one are processed **before** its retry copy. Users
building ledgers/state machines on the documented guarantee can corrupt state
on any transient failure. Until the behavior is fixed, the docs must tell the
truth: per-key order is guaranteed only for messages that never retry.

## Current state

- `docs/pages/guides/sequenced.mdx` — the ordering guide. Line 3 states the
  strict-order promise. Lines ~100-121 ("when a message is permanently
  rejected…") document `SequenceFailure::Skip`/`FailAll` for **permanent**
  failure only; nothing covers transient `Retry`. Line ~233 documents the
  `Defer`-is-treated-as-`Retry` rule for sequenced consumers.
- `docs/pages/backends/kafka.mdx` (~line 148 "Ordering is partition-scoped…")
  and `docs/pages/backends/nats.mdx` (~line 131 pointer to the guide) each
  have a sequenced/ordering section that inherits the false promise.
- `src/macros.rs:61-67` — `define_sequenced_topic!` rustdoc example; the macro
  rustdoc (lines ~55-70) describes strict ordering.
- Mechanism to describe (verified in code): a `Retry` outcome holds the
  original un-acked for the backoff delay, then publishes an incremented copy
  to the tail and acks the original — so the key's later messages (already in
  the shard) run first. `Reject`/budget-exhaustion (the `SequenceFailure`
  paths) are NOT affected — they dead-letter, which the guide already covers.
- Docs prose conventions (maintainer preference): do not use "X — not Y"
  contrast constructions; avoid inline chains of 3+ method calls (use a code
  block instead).

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Rustdoc builds | `cargo doc --no-deps --all-features` | exit 0, no warnings |
| Docs grep | `grep -rn "strict publish order" docs/pages/` | only the amended text |

The vocs site build is not required (CI builds it); keep the mdx valid by
matching surrounding syntax.

## Scope

**In scope**:
- `docs/pages/guides/sequenced.mdx`
- `docs/pages/backends/kafka.mdx`, `docs/pages/backends/nats.mdx` (one
  gotcha/limitation bullet each)
- `src/macros.rs` (rustdoc caveat on `define_sequenced_topic!`)
- `src/topology.rs` (rustdoc caveat on `TopologyBuilder::sequenced` and/or
  `SequenceConfig` — find the `pub fn sequenced` rustdoc)

**Out of scope**:
- ANY code behavior change. No consumer/publisher/routing edits.
- `docs/pages/backends/redis.mdx` / `rabbitmq.mdx` / `sqs.mdx` — those
  backends' retry mechanics were not audited; do not assert anything about
  them (the guide-level caveat should be scoped to "how shove retries work"
  generically, which applies to all republish-based backends, but per-backend
  bullets are only added for kafka and nats).

## Steps

### Step 1: Guide — add a "Retries and ordering" section

In `docs/pages/guides/sequenced.mdx`, after the `SequenceFailure` section,
add a section explaining, in the guide's existing voice:

- What happens on `Outcome::Retry`: the failed message is retried by
  republishing an incremented copy to the tail of its shard, so messages for
  the same key that were already queued are processed before the retry copy.
- The precise guarantee as of today: per-key strict ordering holds for
  messages that succeed on first delivery; a transient failure reorders that
  message relative to its successors.
- Guidance for strict-order handlers: make handlers idempotent and
  order-defensive (e.g. version/sequence checks), or use
  `SequenceFailure::FailAll` with a DLQ redrive when reordering is worse than
  stopping the key.
- A short note that in-place retry (which would preserve order at the cost of
  blocking the shard for the backoff) is under design for NATS.

Also soften the absolute claims at line 3 and the "consumer-visible contract"
paragraph (~line 123) with a pointer to the new section.

### Step 2: Backend pages

Add one "Gotchas"/limitations bullet to `docs/pages/backends/kafka.mdx` and
`docs/pages/backends/nats.mdx` sequenced/ordering sections linking to the new
guide section.

### Step 3: Rustdoc

Add a `# Ordering across retries` caveat paragraph to the
`define_sequenced_topic!` macro docs (`src/macros.rs`) and to
`TopologyBuilder::sequenced`'s rustdoc (`src/topology.rs`), stating the same
guarantee in 2-3 sentences and pointing at the guide.

**Verify**: `cargo doc --no-deps --all-features` → exit 0;
`grep -rn "Ordering across retries" src/` → 2 hits.

## Done criteria

- [ ] `cargo doc --no-deps --all-features` exits 0
- [ ] The guide contains the new section; line-3 claim no longer promises
      unconditional strict order
- [ ] kafka.mdx and nats.mdx each gained exactly one bullet
- [ ] Rustdoc caveats present on the macro and `sequenced()`
- [ ] No `.rs` file changed except doc comments (`git diff --stat` shows only
      comment-line changes in src/)

## STOP conditions

- The guide's structure differs materially from the description (drift).
- You find existing text already documenting retry-reordering (would mean the
  finding was wrong — report it).

## Maintenance notes

- Plan 019 (NATS in-place retry design) will amend this section when/if the
  NATS fix lands; the Kafka caveat stays until a Kafka fix exists.
- Reviewer: check tone — this is an honest limitation note, not an apology;
  match the guide's existing directness.
