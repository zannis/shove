# Plan 013: NATS stream-config parity — storage/discard/dedup knobs, builder sugar, bounded DLQ defaults

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before moving on. On any STOP condition, stop and
> report. Your reviewer maintains `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- src/topology.rs src/backends/nats/topology.rs docs/pages/backends/nats.mdx`
> Expected drift if earlier plans landed: 006 added `#[must_use]` in
> `src/topology.rs`; 009 added an `ack_wait` param to `declare_pull_consumer`
> in `src/backends/nats/topology.rs`. Other drift = STOP.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED (DLQ default bounds change retention behavior for
  unconfigured DLQs — deliberate, maintainer-approved)
- **Depends on**: 006, 009 (textual, same files)
- **Category**: dx + ops
- **Planned at**: commit `e902d7c` (main), 2026-07-02
- **Maintainer decisions**: (1) shove-created DLQ streams get bounded
  defaults, overridable; main streams stay unbounded by default but loudly
  documented. (2) NATS gets builder sugar mirroring the Kafka
  `with_retention`-style API.

## Why this matters

The crate's promise is one consistent API across backends, but the topic-config
ergonomics shipped Kafka-only: Kafka has `with_retention` /
`with_retention_bytes` / `with_cleanup_policy` / `with_max_message_bytes`
sugar on `TopologyBuilder` (src/topology.rs:436-483 on `e902d7c`) while NATS
requires a `NatsStreamConfig { .. }` struct literal, and three JetStream
settings are not expressible at all: `storage` (hardcoded `File`), `discard`
(hardcoded `New`), `duplicate_window` (hardcoded 120s) —
`src/backends/nats/topology.rs:51-64`. Worse, shove-created **DLQ** streams
always use the `None` config (topology.rs:106, 124) = unbounded on file
storage: an unconsumed DLQ grows until the disk fills, at which point every
publish on that server fails.

## Current state

- `src/topology.rs:33-57` — `NatsStreamConfig { retention, max_age: Option<Duration>,
  max_bytes: Option<i64>, max_messages: Option<i64>, num_replicas: usize }`
  with `Default` = WorkQueue/unbounded/1. `NatsRetention` enum directly above.
- `src/backends/nats/topology.rs:37-74` — `create_stream(name, subjects, config:
  Option<&NatsStreamConfig>)` maps the config onto `jetstream::stream::Config`,
  hardcoding `storage: StorageType::File`, `discard: DiscardPolicy::New`,
  `duplicate_window: Duration::from_secs(120)`; `None` config ⇒ defaults.
  Rustdoc on it explains create_or_update semantics: mutable fields reconcile;
  immutable (`retention`, `storage`) fail loud on change.
- DLQ creation sites passing `None`: `declare_standard` (topology.rs:106) and
  `declare_sequenced` (topology.rs:124); the sequenced main stream also passes
  `None` (topology.rs:121).
- `TopologyBuilder::nats_stream_config(NatsStreamConfig)` exists
  (src/topology.rs — search `pub fn nats_stream_config`); the Kafka sugar to
  mirror sits at src/topology.rs:416-483 (`with_topic_config`,
  `with_retention`, `with_retention_forever`, `with_retention_bytes`,
  `with_cleanup_policy`, …) — read those for the rustdoc/mutual-exclusivity
  conventions (`with_retention` vs `with_retention_forever` panic on
  conflicting use; follow the same pattern).
- Existing integration coverage to model on:
  `topology_managed_config_reconciles_existing_stream`
  (tests/nats_integration.rs:~622).
- Conventions: Conventional Commits, no `Co-Authored-By`; clippy `-D warnings`
  both feature sets; `absolute-paths = "deny"`; `cargo nextest run` (never
  `-q`). Docs prose: no "X — not Y" contrasts.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Lint | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Lint (min) | `cargo clippy -q --no-default-features -- -D warnings` | exit 0 |
| NATS unit | `cargo nextest run --features nats --lib` | all pass |
| NATS integration (Docker) | `cargo nextest run --features nats --test nats_integration` | all pass |
| Docs | `cargo doc --no-deps --all-features` | exit 0 |

## Scope

**In scope**:
- `src/topology.rs` (NatsStreamConfig fields, new enums, builder sugar)
- `src/backends/nats/topology.rs` (create_stream mapping, DLQ defaults)
- `tests/nats_integration.rs`
- `docs/pages/backends/nats.mdx` (config section: new knobs, DLQ defaults,
  unbounded-main-stream gotcha)

**Out of scope**:
- Kafka topology code (already has its sugar).
- `declare_pull_consumer` / consumer configs (plan 009).
- Changing the main-stream unbounded default.

## Steps

### Step 1: Extend NatsStreamConfig

Add fields (with `Default` preserving today's values):

```rust
/// Storage backend. Default File. Immutable on an existing stream.
pub storage: NatsStorage,          // enum { File, Memory } mirroring KafkaCleanupPolicy's style
/// What to do when limits are hit. Default DiscardOld? NO — default New (today's value).
pub discard: NatsDiscard,          // enum { New, Old }
/// JetStream dedup window. Default 120s (today's value).
pub duplicate_window: Duration,
```

Map them in `create_stream` (replacing the three hardcoded values). Update the
rustdoc immutability note: `storage` joins `retention` in the fail-loud set.

### Step 2: Bounded DLQ defaults

In `src/backends/nats/topology.rs`, add a module const + helper:

```rust
/// Default bounds for shove-created DLQ streams. A DLQ nobody consumes must
/// age out rather than fill the JetStream store: 14 days or 1 GiB, whichever
/// hits first. Override via TopologyBuilder::nats_dlq_stream_config.
const DLQ_DEFAULT_MAX_AGE: Duration = Duration::from_secs(14 * 24 * 60 * 60);
const DLQ_DEFAULT_MAX_BYTES: i64 = 1024 * 1024 * 1024;
```

DLQ creation sites use, in order of preference: an explicit per-topology DLQ
config (new `TopologyBuilder::nats_dlq_stream_config(NatsStreamConfig)` +
plumbing through `QueueTopology` mirroring how `nats_stream_config` is stored
and exposed — find its field/getter and copy the pattern), else
`NatsStreamConfig { max_age: Some(DLQ_DEFAULT_MAX_AGE), max_bytes:
Some(DLQ_DEFAULT_MAX_BYTES), retention: NatsRetention::Limits, ..Default::default() }`.
NOTE on retention: a DLQ is a holding pen that may be re-read for redrive;
verify what retention the DLQ *consumer* path expects — if `run_dlq` consumes
via a durable on the DLQ stream and WorkQueue semantics are assumed anywhere
(search `run_dlq` / dlq consumer creation in `src/backends/nats/consumer.rs`),
keep `retention` at today's default (WorkQueue) and bound only age/bytes.
Investigate first; state what you found in NOTES. When in doubt: change ONLY
`max_age`/`max_bytes` versus today's DLQ config.

### Step 3: Builder sugar

On `TopologyBuilder` (NATS-gated like `nats_stream_config`): `with_max_age`,
`with_max_bytes`, `with_max_messages`, `with_replicas`, `with_storage`,
`with_nats_retention` — each a thin mutation of the builder's
`NatsStreamConfig` (creating a default one if unset), mirroring the Kafka
sugar's rustdoc style, defaults stated, and pointing at
`nats_stream_config` for the full struct. Naming caution: `with_retention`
already exists as Kafka topic-config sugar on the same builder; the NATS
retention-policy setter must not collide — `with_nats_retention` (matches the
existing `nats_*` prefix family).

### Step 4: Tests

Integration (model on `topology_managed_config_reconciles_existing_stream`):
- declare with `with_storage(Memory)` + `with_max_bytes(..)` via sugar; fetch
  stream info; assert storage/limits;
- declare a topology with `.dlq()` and no explicit DLQ config; fetch the DLQ
  stream info; assert `max_age == 14d` and `max_bytes == 1GiB`;
- explicit `nats_dlq_stream_config` overrides the defaults.

Unit: builder sugar composes into the expected `NatsStreamConfig` (pure
assertions on the built topology).

**Verify**: all table commands pass.

## Done criteria

- [ ] `cargo fmt -- --check`, both clippy commands, `cargo doc` exit 0
- [ ] NATS unit + integration suites all pass, incl. the 3 new integration
      assertions
- [ ] `grep -n "StorageType::File" src/backends/nats/topology.rs` shows the
      value now flows from config (no hardcode in the literal)
- [ ] DLQ streams created without explicit config are bounded (test proves it)
- [ ] Docs updated (knobs + DLQ defaults + unbounded-main gotcha)
- [ ] No files outside scope modified

## STOP conditions

- The DLQ consumer path depends on WorkQueue retention or on unbounded
  storage in a way that makes bounded defaults lossy beyond the intended
  aging-out (report what you find in the `run_dlq` path).
- `QueueTopology` plumbing for the DLQ config requires touching backends
  other than NATS.
- Existing tests assert unbounded DLQ config (report; likely they just need
  the new expected values, but confirm intent with the reviewer via STOP).

## Maintenance notes

- Reviewer: the DLQ retention-policy decision from step 2's investigation is
  the load-bearing judgment call — check NOTES.
- Release notes: existing deployments' DLQ streams get bounds applied on next
  redeclare (create_or_update reconciles mutable limits). 14d/1GiB are
  defaults, not guarantees — operators with regulatory retention needs must
  set explicit configs.
- Plan 019's FIFO-retry design may add per-shard streams; it should reuse
  `NatsStreamConfig` plumbing from here.
