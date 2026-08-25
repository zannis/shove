# Plan 018: Verify MSK IAM recurring OAUTHBEARER refresh for long-lived clients

> **Executor instructions**: This is an investigate-then-implement plan — the
> feasibility question is part of the work. Follow the steps; on any STOP
> condition, stop and report. Your reviewer maintains `plans/README.md`; do
> not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- src/backends/kafka/msk_iam.rs tests/kafka_oauthbearer_admin_integration.rs tests/kafka_msk_iam_unit.rs`
> Expected drift: plan 007 may have touched msk_iam.rs trait bounds. The
> `generate_oauth_token` impl below must still exist as described.

## Status

- **Priority**: P3
- **Effort**: M–L
- **Risk**: LOW (test-only)
- **Depends on**: none
- **Category**: tests
- **Planned at**: commit `e902d7c` (main), 2026-07-02
- **Confidence note**: MED — whether the *recurring* refresh is testable
  without real AWS is an open question this plan answers first.

## Why this matters

MSK IAM tokens live ~15 minutes; librdkafka re-invokes the refresh callback at
~80% of lifetime. The admin client's one-shot priming is fixed and covered
(`tests/kafka_oauthbearer_admin_integration.rs`, commit 032a257), and
single-shot generation has a smoke test — but nothing verifies a long-running
**consumer/producer** whose token expires actually gets a fresh one and keeps
consuming. That is the classic MSK production failure mode: everything works
for 15 minutes, then stalls with auth errors.

## Current state

- `src/backends/kafka/msk_iam.rs` (105 lines, read fully): `MskIamContext`
  implements `ClientContext` with `const ENABLE_REFRESH_OAUTH_TOKEN: bool =
  true` and `generate_oauth_token(&self, _principal_name)` which bridges to
  the AWS signer via a pre-captured Tokio `Handle` (`handle.block_on`).
  librdkafka owns the refresh *scheduling*; shove owns token *generation*.
- The existing OAUTHBEARER integration harness:
  `tests/kafka_oauthbearer_admin_integration.rs` runs a Kafka container
  configured for OAUTHBEARER with **unsecured JWTs** and a test-only context
  (see the file for how the broker is configured and how a substitute
  `ClientContext` supplies tokens) — this is the seam that lets OAUTHBEARER
  flows run without AWS. `prime_admin_oauth_token_for_test` in
  `src/backends/kafka/client.rs:1003-1009` (feature `test-support`) exists
  for the same reason.
- What "recurring refresh works" decomposes into:
  (1) librdkafka schedules re-invocation before expiry when the client's
  queue is being polled — for the **stream consumer** and **producer**,
  polling happens naturally; (2) shove's `generate_oauth_token` returns a
  fresh token each call (pure — easily unit-tested with a counting mock);
  (3) the fresh token actually reaches the broker connection (only
  observable end-to-end).
- Unit-test seam gap: `MskIamContext::generate_oauth_token` is hardwired to
  the AWS signer; the integration harness substitutes a whole different
  context type. A counting/expiring mock context can reuse the same pattern.
- Conventions: `cargo nextest run` (never `-q`); Conventional Commits; no
  `Co-Authored-By`; tests requiring the OAUTHBEARER broker follow whatever
  feature-gating `kafka_oauthbearer_admin_integration.rs` uses (read its
  `#![cfg(...)]` header).

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Existing OAUTHBEARER suite (Docker) | `cargo nextest run --features 'kafka-msk-iam test-support' --test kafka_oauthbearer_admin_integration` | all pass (read the file header for the exact feature set the CI uses; adjust) |
| Lint | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |

## Scope

**In scope**:
- A new test file `tests/kafka_oauthbearer_refresh_integration.rs` (or an
  addition to the existing OAUTHBEARER test file — choose whichever shares
  the harness more cleanly)
- `src/backends/kafka/client.rs` or `msk_iam.rs` ONLY for a `test-support`-
  gated seam if one is strictly required (mirror the existing
  `prime_admin_oauth_token_for_test` precedent; nothing outside
  `#[cfg(feature = "test-support")]`)

**Out of scope**:
- Any non-test-gated behavior change.
- LocalStack/real-AWS tests (`LOCALSTACK_AUTH_TOKEN` suites) — out of budget.

## Steps

### Step 1: Investigate (timeboxed)

Answer, with evidence from the existing OAUTHBEARER harness and the broker
config it uses:
1. Can the unsecured-JWT broker be configured with a short token lifetime, or
   does the token itself carry `exp` such that a test context can mint tokens
   with `lifetime_ms` ≈ 10-20s? (librdkafka schedules refresh from the
   `lifetime_ms` the callback returns — a short-lived token forces refresh
   within test time.)
2. Does a consumer created through shove's normal path work against that
   broker with a substitute context (the admin test substitutes contexts via
   `create_with_context` — find the equivalent consumer-side seam or justify
   a `test-support` one)?

If both answers are yes → step 2. If refresh-within-test-time cannot be
forced, STOP and report exactly which mechanism is missing.

### Step 2: The test

Consumer (or producer — consumer preferred, it is the long-lived polling
client) connects with a counting token context whose tokens expire in ~15s;
publish messages continuously for ~45s; assert (a) the token callback was
invoked ≥2 times (the count proves recurring refresh), and (b) messages
published after the first expiry are still consumed (proves the refreshed
token reached the connection). Budget generous timeouts; the test must pass
twice consecutively.

### Step 3: Unit test the generation contract

Cheap and unconditional: a mock context test asserting each
`generate_oauth_token` call produces a token with a fresh expiry (no caching
inside shove's layer). Place next to the existing unit tests in
`tests/kafka_msk_iam_unit.rs`.

## Done criteria

- [ ] Investigation answers recorded in NOTES with file/config evidence
- [ ] If feasible: refresh integration test passes twice; callback-count ≥2
      asserted
- [ ] Unit generation-contract test passes
- [ ] Existing OAUTHBEARER suite still passes
- [ ] Clippy exits 0; no non-test-gated src changes

## STOP conditions

- Step 1 answers "no" on either question (report; the plan gets rewritten or
  rejected with the evidence).
- The seam requires non-`test-support` changes to production code paths.

## Maintenance notes

- If this proves librdkafka does NOT service consumer-side refresh in some
  mode (the admin client had exactly that bug — 032a257), that is a P1
  finding; report immediately rather than working around it in the test.
