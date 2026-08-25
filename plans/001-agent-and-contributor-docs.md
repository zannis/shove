# Plan 001: Add agent + contributor onboarding docs (CLAUDE.md, AGENTS.md, CONTRIBUTING.md, .env.example)

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 106bb05..HEAD -- README.md Cargo.toml .github/workflows/ci.yml`
> If those files changed materially since this plan was written, re-read them
> before proceeding; the facts you inline below must match the live repo.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: dx
- **Planned at**: commit `106bb05`, 2026-06-20

## Why this matters

This repo is a ~40K-LOC Rust library with six feature-gated backends behind a
sealed `Backend` trait. Plans in this very directory are executed by agents that
have never seen the codebase. There is no `CLAUDE.md`/`AGENTS.md` to orient
them, and no `CONTRIBUTING.md`/`.env.example` to tell a human (or agent) how to
run the integration tests, which require Docker and a `LOCALSTACK_AUTH_TOKEN`
provided via dotenvx. A contributor who clones the repo and runs `cargo test`
hits panics (`tests/sns_integration.rs:75` panics if the token is unset) with
no guidance. These four files remove that friction and are pure additions with
zero risk to library code.

## Current state

Facts to inline (verified at `106bb05`):

- **No** `CLAUDE.md`, `AGENTS.md`, `CONTRIBUTING.md`, or `.env.example` exists at
  the repo root. (`.env` exists but is gitignored — do NOT read or copy it.)
- The crate name is `shove`, version `0.11.7`, `edition = "2024"`
  (`Cargo.toml:1-5`). README states "Rust 1.85+ (edition 2024)"
  (`README.md:129`).
- Backends and their Cargo feature flags (from `Cargo.toml:31-101` and the
  table in `README.md:84-91`):
  | Backend | Feature flag | Marker type |
  |---|---|---|
  | RabbitMQ | `rabbitmq` | `RabbitMq` |
  | AWS SNS+SQS | `aws-sns-sqs` (publisher-only: `pub-aws-sns`) | `Sqs` |
  | NATS JetStream | `nats` | `Nats` |
  | Apache Kafka | `kafka` (+ `kafka-ssl`, `kafka-msk-iam`, `kafka-schema-registry`) | `Kafka` |
  | Redis/Valkey Streams | `redis-streams` | `Redis` |
  | In-process | `inmemory` | `InMemory` |
  Other add-on features: `audit`, `metrics`, `protobuf`, `rabbitmq-transactional`.
- Architecture (from `src/lib.rs:1-60`): everything hangs off a single generic
  `Broker<B>` parameterised by a backend marker `B`. The marker binds that
  backend's client/publisher/consumer/topology/registry types via the sealed
  `Backend` trait (`src/backend/mod.rs`). Generic wrappers live in `src/`
  (`broker.rs`, `publisher.rs`, `consumer.rs`, `consumer_group.rs`,
  `consumer_supervisor.rs`, `autoscaler.rs`, `topology.rs`, `topology_declarer.rs`).
  Per-backend implementations live in `src/backends/<name>/` each with the same
  file layout: `backend.rs`, `client.rs`, `consumer.rs`, `consumer_group.rs`,
  `publisher.rs`, `topology.rs`, `autoscaler.rs`, `constants.rs`/`headers.rs`.
- Capability gating: Kafka/RabbitMQ/NATS/InMemory/Redis implement
  `HasCoordinatedGroups` and expose `Broker::consumer_group`. SQS does **not** —
  calling `consumer_group()` on `Broker<Sqs>` is a compile error; SQS uses
  `ConsumerSupervisor` instead (`src/lib.rs:22-31`).
- Typed-topic macros `define_topic!` / `define_sequenced_topic!` live in
  `src/macros.rs` (re-exported from `src/lib.rs`).
- **Test commands** (verified from `.github/workflows/ci.yml`):
  - Fast, Docker-free unit + inmemory run: `cargo test --no-default-features`
    (CI uses this at `ci.yml:50`) — but per repo convention (see below) prefer
    `cargo nextest run`.
  - Lint gates CI enforces: `cargo fmt -- --check`,
    `cargo clippy --all-features -- -D warnings`,
    `cargo clippy --no-default-features -- -D warnings` (`ci.yml:47-49`).
  - Coverage matrix runs `cargo llvm-cov nextest --features <feature-set>`
    per backend (`ci.yml:103-104`).
- **Integration test env requirements** (verified):
  - `tests/sns_integration.rs:74-78` reads `LOCALSTACK_AUTH_TOKEN` and panics if
    unset; it is injected into the localstack testcontainer.
  - `ci.yml:80` sets `LOCALSTACK_AUTH_TOKEN: ${{ secrets.LOCALSTACK_AUTH_TOKEN }}`.
  - `ci.yml:33-35` sets `AWS_ACCESS_KEY_ID: test`, `AWS_SECRET_ACCESS_KEY: test`,
    `AWS_DEFAULT_REGION: us-east-1` for the AWS backends.
  - Integration tests use `testcontainers` (real RabbitMQ/Kafka/NATS/Redis/
    LocalStack), so Docker must be running.
- **Repo conventions** the docs must state (these are real, observed conventions
  — confirm against the live repo before asserting them):
  - Tests are run with `cargo nextest run`, not `cargo test`. Integration tests
    that need secrets are run through dotenvx:
    `dotenvx run -- cargo nextest run`.
  - Commit messages follow Conventional Commits — see `git log --oneline -20`
    (e.g. `feat(nats): ...`, `fix(kafka): ...`, `chore(deps): ...`,
    `docs(readme): ...`). Releases are cut with `release: vX.Y.Z` commits;
    there is no CHANGELOG.md.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Fast unit/inmemory tests | `cargo test --no-default-features` | exit 0 |
| Format check | `cargo fmt -- --check` | exit 0 |
| Lint (all features) | `cargo clippy --all-features -- -D warnings` | exit 0 |
| Confirm no source touched | `git status --porcelain src/ Cargo.toml` | empty output |

(These four files are docs/config only — none of them affect the build. Running
the test/lint commands just confirms you did not accidentally touch code.)

## Scope

**In scope** (create these files only):
- `CLAUDE.md` (new)
- `AGENTS.md` (new — may be a one-line pointer to `CLAUDE.md`)
- `CONTRIBUTING.md` (new)
- `.env.example` (new)

**Out of scope** (do NOT touch):
- Any file under `src/`, `tests/`, `examples/`, `benches/`.
- `Cargo.toml`, `README.md`, CI workflows — toolchain pinning and MSRV are
  handled by plan 002; do not edit them here.
- The real `.env` file — do not read it, copy it, or reference any value from
  it. `.env.example` must contain only placeholder values.

## Git workflow

- Branch: `advisor/001-agent-and-contributor-docs`
- One commit; Conventional Commits style, e.g.
  `docs: add CLAUDE.md, CONTRIBUTING.md, and .env.example for onboarding`.
- Do NOT push or open a PR unless the operator instructed it.
- Do NOT add a `Co-Authored-By` trailer (repo convention).

## Steps

### Step 1: Create `.env.example`

Create `.env.example` at the repo root with placeholder values only (no real
secrets). It documents the env vars the integration tests consume:

```
# Copy to .env and fill in real values to run the AWS (SNS/SQS) and MSK-IAM
# integration tests locally. Loaded via dotenvx: `dotenvx run -- cargo nextest run`.

# Required for the SNS/SQS integration tests (LocalStack Pro container).
# Obtain from https://app.localstack.cloud (LocalStack auth token).
LOCALSTACK_AUTH_TOKEN=

# Dummy AWS credentials accepted by LocalStack — these exact values are fine.
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
```

**Verify**: `test -f .env.example && ! git check-ignore -q .env.example && echo OK`
→ prints `OK` (file exists and is NOT gitignored, so it will be committed).

### Step 2: Create `CONTRIBUTING.md`

Write `CONTRIBUTING.md` covering, in this order:

1. **Prerequisites**: Rust 1.85+ (edition 2024), Docker running (for integration
   tests via testcontainers), `cargo-nextest`, and `dotenvx` for secret-bearing
   tests.
2. **Fast feedback loop** (no Docker, no secrets): `cargo nextest run --no-default-features`
   runs the unit tests and inmemory-backed tests. (Note: CI uses
   `cargo test --no-default-features`; both work. State that this repo's
   convention is `cargo nextest run`, not `cargo test`.)
3. **Running integration tests**: explain that they require Docker, and that the
   AWS/MSK tests additionally require `LOCALSTACK_AUTH_TOKEN`. Show the dotenvx
   pattern: copy `.env.example` to `.env`, fill it in, then
   `dotenvx run -- cargo nextest run --features <backend-feature-set>`.
   Give the per-backend feature sets from `ci.yml:84-96` verbatim:
   - inmemory: `inmemory,metrics`
   - rabbitmq: `rabbitmq,audit,rabbitmq-transactional`
   - aws-sns-sqs: `pub-aws-sns,aws-sns-sqs,audit`
   - nats: `nats,audit`
   - kafka: `kafka,kafka-ssl,audit` (also needs `librdkafka-dev libsasl2-dev`
     on Linux — see `ci.yml:42,100`)
   - redis-streams: `redis-streams`
4. **Before opening a PR — the gates CI enforces** (`ci.yml:47-51`):
   `cargo fmt -- --check`, `cargo clippy --all-features -- -D warnings`,
   `cargo clippy --no-default-features -- -D warnings`,
   `cargo publish --dry-run --all-features`. Note `cargo audit --deny warnings`
   also runs (`ci.yml:70`).
5. **Conventions**: Conventional Commits; no CHANGELOG (releases are
   `release: vX.Y.Z` commits); MIT license.

Keep it concise (roughly 60–100 lines). Do not invent commands — every command
must come from `ci.yml` or this plan.

**Verify**: `test -f CONTRIBUTING.md && grep -q "dotenvx" CONTRIBUTING.md && grep -q "nextest" CONTRIBUTING.md && echo OK`
→ prints `OK`.

### Step 3: Create `CLAUDE.md`

Write `CLAUDE.md` as an agent-orientation map. Include, using the facts inlined
in "Current state" above (do not re-derive — they are verified):

1. **What this is**: one paragraph — type-safe async pub/sub over six backends,
   one generic `Broker<B>` API.
2. **Architecture map**: the generic layer in `src/` vs. per-backend
   implementations in `src/backends/<name>/`, the sealed `Backend` trait, and
   the identical per-backend file layout. Reproduce the backend↔feature↔marker
   table.
3. **Capability gating**: `HasCoordinatedGroups` (consumer groups) vs. SQS using
   `ConsumerSupervisor`; calling `consumer_group()` on `Broker<Sqs>` is a
   compile error.
4. **Where things live**: typed-topic macros in `src/macros.rs`; outcome/retry
   semantics in `src/outcome.rs` and `src/retry.rs`; each backend's retry/DLQ
   routing in `src/backends/<name>/consumer.rs::route_outcome`.
5. **Build/test/lint commands**: the exact commands from "Current state"
   (nextest, the `--no-default-features` fast path, the clippy/fmt gates,
   the dotenvx pattern for secret-bearing tests).
6. **Conventions for changes**: when fixing a delivery-semantics bug, the same
   fix usually must land in all six backends' `route_outcome` (the git log shows
   recurring "cross-backend consistency" commits) — note this explicitly so an
   agent doesn't fix one backend and miss the others. Conventional Commits; no
   `Co-Authored-By` trailers.

Target 120–250 lines. This file is the highest-leverage deliverable — make it
accurate and skimmable (headings, the table, short bullet lists).

**Verify**: `test -f CLAUDE.md && grep -q "HasCoordinatedGroups" CLAUDE.md && grep -q "route_outcome" CLAUDE.md && echo OK`
→ prints `OK`.

### Step 4: Create `AGENTS.md`

Many agent runtimes look for `AGENTS.md` rather than `CLAUDE.md`. Create
`AGENTS.md` as a short file that points to `CLAUDE.md` as the canonical guide,
e.g. a title plus: "This repository's agent and contributor guidance lives in
[`CLAUDE.md`](./CLAUDE.md) and [`CONTRIBUTING.md`](./CONTRIBUTING.md)." Do not
duplicate the content — a pointer avoids the two files drifting apart.

**Verify**: `test -f AGENTS.md && grep -q "CLAUDE.md" AGENTS.md && echo OK` → prints `OK`.

### Step 5: Confirm no code was touched and the build is intact

**Verify**:
- `git status --porcelain src/ Cargo.toml` → empty output.
- `cargo fmt -- --check` → exit 0.
- `cargo test --no-default-features` → exit 0 (sanity: the new files didn't
  break anything; they can't, but confirm).

## Test plan

No code changes, so no new Rust tests. Verification is the file-existence and
grep checks in each step plus the unchanged-source check in Step 5.

## Done criteria

ALL must hold:

- [ ] `CLAUDE.md`, `AGENTS.md`, `CONTRIBUTING.md`, `.env.example` all exist at
      the repo root.
- [ ] `.env.example` contains placeholder values only and no real credential
      values (manual read-through; the `AWS_*=test` dummies are intentional).
- [ ] `grep -rn "LOCALSTACK_AUTH_TOKEN" CONTRIBUTING.md .env.example` returns
      matches in both.
- [ ] `git status --porcelain src/ tests/ Cargo.toml` is empty (no code/manifest
      changes).
- [ ] `cargo fmt -- --check` exits 0.
- [ ] `plans/README.md` status row for 001 updated.

## STOP conditions

Stop and report back (do not improvise) if:

- A `CLAUDE.md`, `CONTRIBUTING.md`, or `.env.example` already exists with
  non-trivial content (the repo drifted since this plan was written) — do not
  overwrite; report what you found.
- You cannot determine a fact this plan asserts (e.g. the feature-set table no
  longer matches `Cargo.toml`/`ci.yml`) — report the mismatch instead of
  guessing.
- You feel the need to edit any file under `src/`, `tests/`, `Cargo.toml`, or
  `README.md` to complete this — that means scope is wrong; stop.

## Maintenance notes

- The per-backend feature sets in `CONTRIBUTING.md` and the feature table in
  `CLAUDE.md` are duplicated from `ci.yml` and `Cargo.toml`. If features change,
  these docs must be updated — a reviewer should check this on any PR that
  touches `Cargo.toml` features or the CI matrix.
- Plan 002 adds `rust-toolchain.toml` and an MSRV CI job; once it lands, the
  "Rust 1.85+" line in `CONTRIBUTING.md` should reference the pinned toolchain.
- Keep `AGENTS.md` a pointer, not a copy, so it can't drift from `CLAUDE.md`.
