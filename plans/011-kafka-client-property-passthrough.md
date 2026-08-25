# Plan 011: KafkaConfig raw client-property passthrough with reserved keys

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before moving on. On any STOP condition, stop and
> report. Your reviewer maintains `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- src/backends/kafka/client.rs src/backends/kafka/consumer.rs src/backends/kafka/constants.rs docs/pages/backends/kafka.mdx`
> Expected drift if plans 006/007 landed first: 006 touched `consumer.rs`
> (shutdown commit, reconnect reset), `client.rs` (#[must_use] line),
> `kafka.mdx` (tuning bullet); 007 touched `consumer.rs`
> (rebalance context). None overlap this plan's excerpts except the kafka.mdx
> bullet, which this plan rewrites again. Other drift = STOP.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED (users can now set client properties; reserved-key guard is the mitigation)
- **Depends on**: 006 (textual, same files)
- **Category**: dx
- **Planned at**: commit `e902d7c` (main), 2026-07-02
- **Maintainer decision**: raw passthrough with a reserved-key deny list.
  Named helper methods are NOT in scope (may come later).

## Why this matters

`KafkaConfig` exposes brokers, TLS, and SASL — nothing else. Every librdkafka
client property (`compression.type`, `linger.ms`, `batch.size`,
`max.poll.interval.ms`, `session.timeout.ms`, `fetch.*`,
`queue.buffering.max.*`, `request.timeout.ms`, …) is either hardcoded
(`src/backends/kafka/constants.rs`) or unreachable, so any production tuning
requires forking the crate. Note the distinction that confused earlier
discussion: `TopologyBuilder::with_topic_config` (already shipped) sets
broker-side **topic** configs via the admin API; this plan adds the missing
**client** (producer/consumer) property surface. A stale comment at
`constants.rs:113-116` even references a `with_max_poll_interval` knob that
was never built.

## Current state

- `src/backends/kafka/client.rs:222-232` — `KafkaConfig { brokers, tls, sasl,
  allow_plaintext_credentials }` (tls/sasl/flag are `#[cfg(feature = "kafka-ssl")]`);
  builders `with_tls`/`with_sasl`/`allow_plaintext_credentials` at :252-274.
- `KafkaClient::connect` (client.rs:322-486) builds one `base_config:
  Arc<ClientConfig>` from brokers + TLS/SASL, then layers producer settings
  (`client.id`, `message.timeout.ms`, `acks=all`, `enable.idempotence=true`)
  when creating the `FutureProducer` (two cfg-gated arms, ~:436-476).
  `base_config()` (`pub(super)`, :551-553) is cloned by every consumer/admin/
  metadata call site.
- `src/backends/kafka/consumer.rs:714-755` — `create_stream_consumer` clones
  the base config and sets `group.id`, `client.id`,
  `partition.assignment.strategy=cooperative-sticky`,
  `enable.auto.commit=false`, `auto.offset.reset`, `session.timeout.ms`,
  `max.poll.interval.ms`, `fetch.min.bytes`, `fetch.wait.max.ms` (constants
  from `constants.rs:105-126`).
- `KafkaClient::config_entry` (client.rs:564-579) exposes single non-sensitive
  entries for tests, with a `SENSITIVE` deny list — reuse that style.
- Security context (must preserve): `base_config` is deliberately not `pub`
  because rdkafka's `ClientConfig` Debug dumps raw PEM (client.rs:542-550).
  The new `properties` vec on `KafkaConfig` must be covered by the existing
  hand-written `Debug` (client.rs:283-294) — property *values* may be
  sensitive (e.g. `sasl.oauthbearer.config`); print keys, redact values.
- Conventions: Conventional Commits, no `Co-Authored-By`; clippy `-D warnings`
  on `--all-features` and `--no-default-features`; `absolute-paths = "deny"`;
  `cargo nextest run` (never `-q` to nextest). Doc prose: no "X — not Y"
  contrasts; no inline chains of 3+ method calls.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Lint | `cargo clippy -q --all-features --all-targets -- -D warnings` | exit 0 |
| Lint (min) | `cargo clippy -q --no-default-features -- -D warnings` | exit 0 |
| Kafka unit | `cargo nextest run --features kafka --lib` | all pass |
| Kafka integration (Docker) | `cargo nextest run --features kafka --test kafka_integration` | all pass |
| Docs | `cargo doc --no-deps --all-features` | exit 0 |

## Scope

**In scope**:
- `src/backends/kafka/client.rs` (KafkaConfig fields/builders, connect wiring,
  Debug impl, unit tests)
- `src/backends/kafka/consumer.rs` (consumer-side property application ONLY —
  see step 3)
- `src/backends/kafka/constants.rs` (fix the stale comment at :113-116 —
  comment text only, no values)
- `docs/pages/backends/kafka.mdx` (tuning bullet + a short "Client properties"
  subsection)
- `tests/kafka_integration.rs` (one integration assertion via `config_entry`)

**Out of scope**:
- Named helper methods (`with_compression`, …).
- `with_topic_config` / topology / admin paths.
- NATS (plan 012 owns its escape hatch).
- Changing any default constant value.

## Steps

### Step 1: Config surface

Add to `KafkaConfig` (NOT cfg-gated — properties are useful without TLS):

```rust
/// Extra librdkafka client properties applied to every client this config
/// creates (producer, consumers, admin), in call order. Keys shove manages
/// are reserved and rejected at connect time; see `RESERVED_PROPERTIES`.
pub(crate) properties: Vec<(String, String)>,
```

Builder:

```rust
pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self
```

Add a module-level `RESERVED_PROPERTIES: &[&str]` covering the keys shove's
delivery semantics depend on. Minimum set (verify each is actually set
somewhere in the backend and add any others you find via
`grep -rn "\.set(" src/backends/kafka/`):
`bootstrap.servers`, `security.protocol`, `sasl.mechanism`, `sasl.username`,
`sasl.password`, `ssl.ca.location`, `ssl.ca.pem`, `ssl.certificate.location`,
`ssl.certificate.pem`, `ssl.key.location`, `ssl.key.pem`, `ssl.key.password`,
`ssl.endpoint.identification.algorithm`, `enable.auto.commit`,
`enable.idempotence`, `acks`, `group.id`, `client.id`,
`partition.assignment.strategy`, `auto.offset.reset`.

`KafkaClient::connect` returns `ShoveError::Validation` naming the offending
key if any property is reserved. Tunables that shove sets but a user may
legitimately want to override (`message.timeout.ms`, `session.timeout.ms`,
`max.poll.interval.ms`, `fetch.min.bytes`, `fetch.wait.max.ms`) are NOT
reserved — user values must win (see steps 2-3).

### Step 2: Producer/base wiring

In `connect`, after the TLS/SASL block builds `base_config`, apply
`config.properties` onto `base_config` (so consumers/admin inherit them),
EXCEPT that shove's producer block later sets `message.timeout.ms`, `acks`,
`enable.idempotence`, `client.id` unconditionally — change the producer arms
so a user-supplied `message.timeout.ms` wins over `MESSAGE_TIMEOUT_MS`
(`acks`/`enable.idempotence`/`client.id` are reserved, so no conflict).
Concretely: only set `message.timeout.ms` to the constant when the user did
not supply it.

Update the hand-written `Debug` for `KafkaConfig` to render property keys with
redacted values (e.g. `properties: {"linger.ms": "<redacted>", ...}` — keys
visible, values not).

### Step 3: Consumer-side precedence

`create_stream_consumer` clones the base config (which now already contains
user properties) and then overwrites `session.timeout.ms`,
`max.poll.interval.ms`, `fetch.min.bytes`, `fetch.wait.max.ms` with constants
— inverting user intent. Change it to set each of those four keys only when
the cloned config does not already contain the key
(`ClientConfig::config_map().contains_key(...)` — same accessor
`config_entry` uses). `group.id`/`client.id`/strategy/auto-commit/reset
remain unconditional (reserved).

### Step 4: Tests

Unit (client.rs `mod tests`):
- `with_property` round-trips into `config_entry` after connect-config
  assembly — if full `connect()` needs a broker, test the pure assembly by
  factoring the base-config construction into a testable helper, or assert
  via `KafkaClient::connect` in the integration test instead and unit-test
  only the reserved-key rejection (a `connect` against an unreachable broker
  still errors at Validation before any I/O for reserved keys — assert that).
- reserved key → `ShoveError::Validation` naming the key.
- Debug output shows the property key and not the value.

Integration (`tests/kafka_integration.rs`, model on any existing connect
test): connect with `.with_property("linger.ms", "25")` and assert
`client.config_entry("linger.ms") == Some("25")`; also assert a consumer-
tunable override (`.with_property("fetch.wait.max.ms", "100")`) survives —
verifiable via `config_entry` since consumers clone the same base config.

### Step 5: Docs

- `constants.rs:113-116`: replace the stale `with_max_poll_interval` pointer
  with `KafkaConfig::with_property("max.poll.interval.ms", ...)`.
- `docs/pages/backends/kafka.mdx`: add a short "Client properties" subsection
  (placement: near the TLS/SASL config docs) documenting `with_property`, the
  reserved-key list (link to the rustdoc), and precedence (user value wins for
  the five tunables; reserved keys error). Update the rebalance Gotcha bullet
  (rewritten by plan 006) to point at `with_property` for
  `session.timeout.ms` / `max.poll.interval.ms`.

**Verify**: all commands in the table pass.

## Done criteria

- [ ] `cargo fmt -- --check`, both clippy commands, `cargo doc` all exit 0
- [ ] `cargo nextest run --features kafka --lib` all pass (incl. new tests)
- [ ] `cargo nextest run --features kafka --test kafka_integration` all pass
      (incl. the new property assertions)
- [ ] Reserved key at connect returns `Validation` (test proves it)
- [ ] `grep -n "with_max_poll_interval" src/` → no matches
- [ ] Debug output test proves property values are redacted
- [ ] No files outside scope modified

## STOP conditions

- The five "user wins" tunables turn out to be load-bearing for shove's
  delivery semantics somewhere (e.g. a test depends on `fetch.wait.max.ms=50`)
  — report which, and whether it should be reserved instead.
- Making user properties reach consumers requires touching
  `consumer_group.rs` or other out-of-scope files.
- rdkafka rejects a property at create time in a way that breaks connect
  error mapping (report the shape).

## Maintenance notes

- Every new `.set(` added to the Kafka backend later must be checked against
  `RESERVED_PROPERTIES` (add it) or the "user wins" list (guard it) —
  reviewer should leave a comment near the list saying so.
- Named helpers can be layered on `with_property` later without churn.
- The `properties` field is a `Vec`, so repeated keys apply in call order —
  last one wins in rdkafka; document that in the rustdoc.
