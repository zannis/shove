# Plan 019: DESIGN SPIKE — in-place retry for NATS sequenced topics

> **Executor instructions**: This is a design/spike plan, not a build plan.
> The deliverable is a written design (`plans/019-outcome.md`) plus at most a
> throwaway prototype — NO production code lands from this plan. On any STOP
> condition, stop and report. Your reviewer maintains `plans/README.md`; do
> not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- src/backends/nats/consumer.rs src/routing.rs docs/pages/guides/sequenced.mdx`
> Expected drift: plans 006/009/010 edits. The FIFO shard mechanics cited
> below must still hold (`max_ack_pending: 1`, tail republish on Retry).

## Status

- **Priority**: P3 (correctness gap is real but documented by plan 010)
- **Effort**: M (design), fix itself TBD by this design
- **Risk**: n/a (no production code)
- **Depends on**: 010 (docs state the current behavior this design will change)
- **Category**: direction/design
- **Planned at**: commit `e902d7c` (main), 2026-07-02
- **Maintainer decision**: NATS first; Kafka deferred until this design
  proves out.

## Why this matters

On a sequenced topic, `Outcome::Retry` republishes an incremented copy to the
shard tail (`src/backends/nats/consumer.rs:263-284`), so later messages of the
same key overtake the retried one — violating the ordering guarantee the
feature exists for. NATS is the cheapest backend to fix because JetStream has
native in-place redelivery: `AckKind::Nak(Some(delay))` redelivers the SAME
message after the delay without losing its stream position, and the shard
already runs `max_ack_pending: 1` (`consumer.rs:961`), so nothing overtakes an
un-acked head message. The hard part is retry accounting, not redelivery.

## Current state (the constraints the design must reconcile)

- Retry counting is **header-based** (`Shove-Retry-Count`), incremented on
  each republished copy; `Defer` uses `Nak(Some(delay))` precisely because it
  must NOT consume retry budget (`hold_then_republish` doc comment,
  nats/consumer.rs:190-206). A Nak-based Retry cannot rewrite headers — the
  redelivered message carries the original count.
- JetStream provides `msg.info().delivered` (delivery count) per message —
  the natural Nak-compatible counter, but it counts ALL deliveries including
  `Defer` naks and ack_wait redeliveries, so `delivered - 1` ≠ retry count
  when defers/timeouts occur. The design must either accept that conflation
  (document: on FIFO, defers/timeouts consume retry budget) or find a split
  (e.g. KV/object-store side counter, or accept `max_deliver` as the budget
  mechanism outright).
- The shared decision table `decide_retry(&outcome, retry_count, max_retries)`
  (`src/routing.rs`) is used by all backends — the FIFO path may need a
  variant input (delivery count) but must not fork the boundary semantics.
- Hold-queue delays (`hold_queues[idx].delay()`) are selected by retry count
  (`hold_index`, src/routing.rs) — Nak(delay) can carry the same computed
  delay, so multi-tier backoff survives.
- DLQ on exhaustion: currently the republish path dead-letters via
  `publish_to_dlq` with reason `max_retries_exceeded`; a Nak design must
  dead-letter when the budget is hit (publish-to-DLQ then Ack — same ordering
  as today's terminal path, nats/consumer.rs:307-316).
- FIFO shard loop: consumer.rs:980-1118 (one message at a time,
  `route_outcome` awaited inline).
- `max_deliver` on the shard consumer config is currently unset (unlimited).

## Deliverable — `plans/019-outcome.md` containing:

1. **Chosen mechanism** for FIFO Retry on NATS (expected: `Nak(Some(delay))`
   with delivery-count-based budget; but argue it against at least one
   alternative, e.g. per-key hold with in-memory requeue, or `max_deliver` +
   JetStream server-side DLQ advisories).
2. **Retry-budget semantics**: exactly how `max_retries` maps to delivery
   count on FIFO; what happens to `Defer` (today: Nak without budget — under
   the new design Defer and Retry both nak, so how do they differ, or does
   FIFO fold them?); whether the answer diverges from non-FIFO paths and how
   `decide_retry` accommodates it without forking the table.
3. **Crash-safety table**: for each step (handler fail → nak decision → DLQ
   publish → ack), what a crash between steps produces (dup vs loss) —
   must remain at-least-once, no loss.
4. **Ordering proof sketch**: why `max_ack_pending: 1` + Nak preserves
   per-key order across retries, including across consumer restart and
   consumer-deletion recovery (plan 017's scenario).
5. **Migration/compat**: messages in flight during an upgrade carry old
   headers; header-based count on the shard becomes vestigial — spell out
   the transition.
6. **Blast radius**: files/functions to change, test list (unit + the
   integration scenarios: retry preserves order, budget exhaustion
   dead-letters, defer still works, shutdown mid-nak), rough size, and
   whether Kafka could follow the same shape (pause/seek notes only).
7. **Open questions** requiring maintainer sign-off, each with a
   recommendation.

A throwaway prototype (ignored dir or `examples/`-style scratch, NOT
committed) may be used to validate Nak(delay) + delivered-count mechanics
against a container; cite observed behavior in the design.

## Verification

- The design document exists, covers all 7 sections, and every mechanism
  claim about async-nats/JetStream cites either the vendored crate source or
  observed prototype behavior (no folklore).

## STOP conditions

- JetStream's Nak(delay) does not actually hold stream position under
  `max_ack_pending: 1` (prototype disproves the premise — report
  immediately; the design pivots to the alternatives).

## Maintenance notes

- The implementation plan that follows this design should update plan 010's
  docs section for NATS and leave the Kafka caveat in place.
