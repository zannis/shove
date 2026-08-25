# Plan 005: Migrate the remaining backends' route_outcome onto the shared retry decision

> **Executor instructions**: This plan has a **decision gate** before any code
> changes (Step 0). Do not skip it. Follow the rest step by step, run every
> verification command, and honor the STOP conditions. Update the status row in
> `plans/README.md` when done. This is the highest-risk plan in the set — when in
> doubt, STOP and report rather than improvise.
>
> **Drift check (run first)**:
> `git diff --stat <004's landing SHA>..HEAD -- src/routing.rs src/backends/kafka/consumer.rs src/backends/nats/consumer.rs src/backends/rabbitmq/router.rs src/backends/redis/consumer.rs`

## Status

- **Priority**: P3
- **Effort**: L
- **Risk**: HIGH
- **Depends on**: plans/004-shared-retry-decision-and-hold-index.md (MUST be
  landed first — this plan calls `crate::routing::decide_retry`, introduced there)
- **Category**: tech-debt
- **Planned at**: commit `106bb05`, 2026-06-20

## Why this matters

After plan 004, the retry-budget decision lives in `crate::routing::decide_retry`
and is unit-tested, but only the in-memory backend calls it. Kafka, NATS,
RabbitMQ, and Redis still inline their own copy of the same branch logic. Until
they call the shared function, the duplication — and the risk of a fix landing in
one backend but not the others — remains. This plan finishes the consolidation
for the decision branching while leaving each backend's execution (ack/commit/
publish/DLQ mechanics) backend-specific.

**This is genuinely risky and partly a design decision, not a mechanical
refactor.** The backends diverge on the empty-hold-queue case (see Step 0), and
their execution paths are very different (broker acks vs. offset commits vs.
Redis XACK). The advisor's honest assessment: do this only if the maintainer
wants the duplication gone badly enough to pay for careful per-backend
integration testing (Docker required for all four). If not, plan 004 already
captured most of the value (one tested decision function + index dedup) at a
fraction of the risk.

## Step 0 (DECISION GATE): Reconcile the empty-hold-queue behavior

The backends do **different** things when a `Retry`/`Defer` occurs and the topic
has **no hold queues** configured:

| Backend | Empty-hold-queue behavior on Retry/Defer | Evidence |
|---|---|---|
| Kafka | republish after a fixed `Duration::from_secs(1)` | `kafka/consumer.rs:413-414, 462-466` |
| NATS | republish after `Duration::from_secs(1)` | `nats/consumer.rs:323-324, 348-352` |
| In-memory | redeliver after `Duration::ZERO` (+ warn on Defer) | `inmemory/consumer.rs:734-746` |
| RabbitMQ | nack-with-requeue (broker redelivery, no delay) | `rabbitmq/router.rs:35` (skips republish) |
| Redis | (confirm by reading `redis/consumer.rs` `route_outcome` ~1135-1260) | — |

The `Outcome` doc comment (`src/outcome.rs:19-20, 60-61`) says the empty case is
"nacked with requeue (broker-level redelivery with no delay)" — which matches
RabbitMQ but NOT Kafka/NATS/in-memory. So either the docs are stale or several
backends are off-spec. **This is a maintainer decision and possibly a doc fix.**

`crate::routing::decide_retry` (from plan 004) intentionally returns only
`Hold { increment }` and does NOT encode a delay or the empty-case fallback — so
migrating to it does **not** force unifying these values. Each backend keeps its
own delay computation and empty-case handling after the `Hold` branch.

**Before writing code**: confirm with the maintainer (or, if running
non-interactively, record the assumption explicitly in your report and in
`plans/README.md`): *this migration preserves each backend's current empty-hold-
queue behavior; it does not unify them.* If the maintainer instead wants the
behaviors unified, STOP — that is a separate semantics change with its own test
plan, not this refactor.

## Current state

- `crate::routing::decide_retry(&Outcome, retry_count, max_retries) ->
  RetryDecision` and `RetryDecision { Ack, Dlq { reason }, Hold { increment } }`
  exist after plan 004 (`src/routing.rs`). `decide_retry` already emits the
  correct death reasons: `"rejected"` for `Reject`, `"max_retries_exceeded"` for
  exhausted `Retry`.
- The four `route_outcome` functions to migrate:
  - Kafka `src/backends/kafka/consumer.rs:344-505` — execution via
    `signal_completion`, `publish_to_dlq`, `run_delayed_republish`; death reasons
    currently the string literals `"max_retries_exceeded"` (line 395) and
    `"rejected"` (line 447).
  - NATS `src/backends/nats/consumer.rs:288-367` — execution via `msg.ack()`,
    `publish_to_dlq`, `hold_then_republish`; reasons `"max_retries_exceeded"`
    (line 313), `"rejected"` (line 338).
  - RabbitMQ `src/backends/rabbitmq/router.rs` (+ its `route_outcome` caller at
    `src/backends/rabbitmq/consumer.rs:1153`) — execution via channel
    publish/ack/nack and `route_retry`/`route_reject`.
  - Redis `src/backends/redis/consumer.rs:1135` (`route_outcome`) — execution via
    connection XACK / hold-enqueue / DLQ publish.
- Each currently inlines `if retry_count >= max_retries { DLQ } else { hold }`
  and a `Reject → DLQ` / `Defer → hold(no increment)` branch. These are exactly
  what `decide_retry` returns.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Per-backend integration tests (Docker REQUIRED) | `cargo nextest run --features <set> --test <backend>_integration` | all pass |
| Kafka feature set | `--features kafka,kafka-ssl,audit` | — |
| NATS feature set | `--features nats,audit` | — |
| RabbitMQ feature set | `--features rabbitmq,audit,rabbitmq-transactional` | — |
| Redis feature set | `--features redis-streams` | — |
| Lint all / none | `cargo clippy --all-features -- -D warnings` ; `cargo clippy --no-default-features -- -D warnings` | exit 0 |
| Format | `cargo fmt -- --check` | exit 0 |

Docker must be running for every backend's integration suite — these are the only
regression net for this change. If you cannot run a backend's integration tests,
STOP for that backend and report; do not migrate a backend you cannot verify.

## Scope

**In scope** — migrate the decision branching only, one backend at a time:
- `src/backends/kafka/consumer.rs`
- `src/backends/nats/consumer.rs`
- `src/backends/rabbitmq/router.rs` and `src/backends/rabbitmq/consumer.rs`
- `src/backends/redis/consumer.rs`

**Out of scope** (do NOT touch):
- The execution mechanics (ack/commit/publish/DLQ calls, `run_delayed_republish`,
  `hold_then_republish`, XACK, channel tx logic). Only the *branch selection*
  changes; the bodies of each branch stay byte-for-byte the same.
- The empty-hold-queue fallback values (see Step 0).
- `src/routing.rs` (owned by plan 004; if you need to change it, that is a sign
  the abstraction is wrong — STOP and report).
- The in-memory backend (already migrated in plan 004).

## Git workflow

- Branch: `advisor/005-migrate-route-outcome`
- **One commit per backend**, in this order: Kafka, NATS, Redis, RabbitMQ
  (RabbitMQ last — its tx/nack-requeue path is the most divergent). Each commit
  must leave the tree green (that backend's integration tests pass) before
  starting the next. Conventional Commits, e.g.
  `refactor(kafka): route outcome via shared decide_retry`.
- Do NOT push or open a PR unless instructed. No `Co-Authored-By` trailer.

## Steps

For EACH backend (Kafka → NATS → Redis → RabbitMQ), repeat this loop:

### Step N.a: Read the current `route_outcome` end to end
Confirm it matches the "Current state" excerpt. Identify the exact code for each
branch body: the Ack execution, the DLQ execution (and its reason string), the
hold/retry execution (increment + delay), and the defer execution.

### Step N.b: Replace only the branch selection
At the top of the function, compute
`let decision = crate::routing::decide_retry(&outcome, retry_count, max_retries);`
then `match decision { ... }`, moving each existing branch body under the
corresponding arm UNCHANGED:
- `RetryDecision::Ack` → the existing Ack body.
- `RetryDecision::Dlq { reason }` → the existing DLQ body, but pass `reason`
  through to `publish_to_dlq` instead of the hardcoded literal. Confirm the
  reason strings already match (`"rejected"` / `"max_retries_exceeded"`) so this
  is a no-op string-wise — it just removes the duplicated literal and the
  duplicated `retry_count >= max_retries` test.
- `RetryDecision::Hold { increment }` → for `increment == true`, the existing
  Retry body (compute delay, republish with `retry_count + 1`); for `increment ==
  false`, the existing Defer body (compute delay from `hold_queues[0]` / empty
  fallback, republish without incrementing). Keep each backend's own delay and
  empty-case logic exactly as-is.

The net change per backend: the `match outcome { ... }` becomes
`match decide_retry(...) { ... }`, the inline `if retry_count >= max_retries`
disappears (folded into `Dlq`), and the two DLQ reason literals come from the
decision. No execution call changes.

### Step N.c: Verify this backend before moving on
- `cargo build --features <set>` → exit 0.
- `cargo nextest run --features <set> --test <backend>_integration` → ALL pass.
  Pay special attention to: `retry_then_ack*`, `max_retries*_to_dlq` /
  `max_retries_allows_initial_plus_n_retries` (the budget boundary),
  `defer_*` (no increment, never DLQs), `rejected_message_lands_in_dlq`, and any
  `deserialization_failure_rejects_to_dlq`.
- Commit only if green. If any test fails, revert this backend's change and
  report (do NOT edit the test).

### Step Final: Whole-suite lint
- `cargo clippy --all-features -- -D warnings` → exit 0.
- `cargo clippy --no-default-features -- -D warnings` → exit 0.
- `cargo fmt -- --check` → exit 0.
- `grep -rn "retry_count >= max_retries" src/backends` → should return no matches
  in the four migrated backends (the boundary now lives only in `decide_retry`).

## Test plan

- No new production behavior, so the regression net is the existing per-backend
  integration suites (Docker). Each must pass unchanged after its backend's
  migration. These suites are extensive (e.g. `tests/rabbitmq_integration.rs` and
  `tests/kafka_integration.rs` cover retry/DLQ/defer/reject/sequenced/timeout).
- Optionally, add one focused test per backend asserting the budget boundary
  (`max_retries = N` ⇒ N+1 total attempts then DLQ) if not already present — most
  backends already have `max_retries_allows_initial_plus_n_retries`; do not
  duplicate it.
- The shared decision itself is unit-tested by plan 004; this plan does not add
  unit tests for `decide_retry`.

## Done criteria

ALL must hold:

- [ ] Kafka, NATS, Redis, and RabbitMQ `route_outcome` select their branch via
      `crate::routing::decide_retry`; the inline `retry_count >= max_retries`
      test is gone from all four (`grep` returns no matches in those backends).
- [ ] Each backend's integration suite passes (run with Docker, per the feature
      sets above) — recorded in your report which were actually run vs. deferred
      to CI.
- [ ] No execution mechanics or empty-hold-queue fallbacks changed (diff review:
      only branch selection and the DLQ reason source differ).
- [ ] `cargo clippy --all-features` and `--no-default-features` both clean;
      `cargo fmt -- --check` clean.
- [ ] Only in-scope files modified.
- [ ] `plans/README.md` status row for 005 updated; the Step 0 decision recorded.

## STOP conditions

Stop and report back (do not improvise) if:

- Step 0's decision is "unify the empty-hold-queue behavior" — that is out of
  scope here.
- Any backend's integration suite fails after migration, or you cannot run it
  (no Docker) — stop for that backend; do not migrate blind.
- A backend's DLQ reason strings do NOT already match `"rejected"` /
  `"max_retries_exceeded"` — report the mismatch; changing a death-reason header
  value is a behavior change that needs sign-off.
- RabbitMQ's tx/nack-requeue path doesn't map cleanly onto `Hold`/`Dlq` (it has
  rollback-on-ack-failure logic) — if folding it under `decide_retry` would
  obscure that logic, leave RabbitMQ inlined and report; partial migration
  (Kafka/NATS/Redis only) is an acceptable outcome.

## Maintenance notes

- After this, a change to the retry-budget boundary is a one-line edit in
  `decide_retry` that all backends pick up — but execution and empty-case
  behavior are still per-backend, so a reviewer must still think about all
  backends when changing those.
- The empty-hold-queue divergence remains an open item; consider a follow-up to
  align the `Outcome` docs with actual per-backend behavior (or align the
  backends with the docs).

---

## Revision 2026-06-20 — completing Redis and RabbitMQ (maintainer decisions made)

The first run migrated Kafka + NATS and STOPPED on Redis and RabbitMQ. The
maintainer resolved both blockers:

### Redis — migrate + unify the death-reason string (AUTHORIZED wire change)

`src/backends/redis/consumer.rs::route_outcome` (≈1136) is already a single
decision point. Migrate it to `decide_retry` exactly like Kafka/NATS, and change
the DLQ death-reason header value `"max-retries"` → `"max_retries_exceeded"`
(maintainer-approved; this is a visible change to the `X_DEATH_REASON` header for
the max-retries path — commit with a `!`). The reject reason `"rejected"` already
matches and stays.

- Map `RetryDecision::Dlq { reason }` to `route_to_dlq(..., reason, death_count)`,
  preserving the pre-refactor death count per path:
  `let death_count = if reason == "rejected" { retry_count } else { retry_count + 1 };`
  (max-retries previously passed `retry_count + 1`, reject passed `retry_count`).
- `Hold { increment: true }` → existing Retry sub-logic (empty→`requeue_to_stream`
  with `retry_count+1` + xack; else `route_to_hold`); `Hold { increment: false }`
  → existing Defer sub-logic. Empty-hold-queue behavior UNCHANGED.
- Update the unit test at `consumer.rs:1668` (asserts `"max-retries"`) and any
  assertion of that header value in `tests/redis_integration.rs`.
- Verify (Docker): `cargo nextest run --features redis-streams --test redis_integration`.

### RabbitMQ — centralize the *boundary predicate* only

RabbitMQ checks the budget **pre-handler** (a gate) at three identical sites —
`consumer.rs:626`, `:828`, `:1046` — two of them wired into sequenced-key
poisoning + pending-delivery rejection. That execution can't and shouldn't fold
into `decide_retry` (a post-outcome decision). What centralizes safely is the
drift-prone boundary itself:

- Add to `src/routing.rs`:
  `pub(crate) fn retries_exhausted(retry_count: u32, max_retries: u32) -> bool { retry_count >= max_retries }`
  and refactor `decide_retry` to call it (single source of truth). Add a unit test.
- Replace each `if retry_count >= options.max_retries` at `:626`, `:828`, `:1046`
  with `retries_exhausted(retry_count, options.max_retries)` (import it; the repo
  denies `clippy::absolute_paths`). Surrounding poisoning/nack logic UNCHANGED.
- Also swap the hold-index dup at `consumer.rs:1178`
  (`(retry_count as usize).min(shard_hold_queues.len() - 1)`) → `hold_index(...)`
  — 004's grep missed it (different variable name).
- Verify (Docker): `cargo nextest run --features rabbitmq,audit,rabbitmq-transactional --test rabbitmq_integration`.

After this, the retry boundary (`>=`) is defined once in `routing::retries_exhausted`
and used by every backend's decision (post-outcome via `decide_retry`, pre-handler
via the direct predicate). Execution stays per-backend by design.
