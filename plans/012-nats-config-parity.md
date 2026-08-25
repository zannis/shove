# Plan 012: NatsConfig production parity — builders, JetStream domain, auth validation, escape hatch

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before moving on. On any STOP condition, stop and
> report. Your reviewer maintains `plans/README.md`; do not update it.
>
> **Drift check (run first)**:
> `git diff --stat e902d7c..HEAD -- src/backends/nats/client.rs docs/pages/backends/nats.mdx`
> Expected drift if plan 006 landed: a `#[must_use]` attribute line on
> `NatsConfig`. Other drift = STOP.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: LOW-MED (additive API; auth validation is a behavior change for
  misconfigured setups that silently worked)
- **Depends on**: 006 (textual)
- **Category**: dx
- **Planned at**: commit `e902d7c` (main), 2026-07-02
- **Maintainer decision**: full parity plan — builders mirroring Kafka's
  shape, exactly-one-auth validation at connect, first-class JetStream domain
  and timeouts, plus a ConnectOptions escape-hatch closure. Fields stay `pub`
  for one deprecation cycle (do not remove or rename fields).

## Why this matters

`NatsConfig` cannot express common production topologies: there is no
JetStream **domain** (leaf-node / hub-spoke deployments cannot reach their
streams at all), no request/connection timeout or reconnect knobs, and no
escape hatch to `async_nats::ConnectOptions` for anything else. Configuration
style also diverges from Kafka (raw `pub` fields vs consuming builders), and
auth methods resolve through a silent if/else precedence chain — an operator
who sets both `token` and `creds_file` gets token auth with no indication.

## Current state

All in `src/backends/nats/client.rs` (357 lines; read it fully before editing).

- `NatsConfig` (lines 14-32): `url` + TLS paths (`tls_ca_cert`,
  `tls_client_cert`, `tls_client_key`) + auth quartet (`username`, `password`,
  `token`, `nkey_seed`, `creds_file`), all `pub`. Only methods: `new(url)`,
  `url()`. Hand-written `Debug` (lines 62-90) redacts credentials.
- `connect` (lines 116-160): TLS-downgrade guard (TLS options + plaintext
  scheme = `ShoveError::Connection`), then builds
  `async_nats::ConnectOptions::new().name(client_name)` and wires TLS + the
  auth chain:

  ```rust
  if let (Some(user), Some(pass)) = (&config.username, &config.password) {
      opts = opts.user_and_password(user.clone(), pass.clone());
  } else if let Some(token) = &config.token {
      ...
  } else if let Some(seed) = &config.nkey_seed {
      ...
  } else if let Some(creds) = &config.creds_file {
      ...
  }
  ```

  then `jetstream::new(client.clone())` — no domain.
- Kafka's builder shape to mirror: `src/backends/kafka/client.rs:252-274`
  (`with_tls`, `with_sasl` — consuming `mut self -> Self`).
- async-nats 0.49 APIs you will use (verify names against the vendored crate
  docs before coding; note any renames in NOTES):
  `jetstream::with_domain(client, domain)`;
  `ConnectOptions::{connection_timeout, request_timeout, ping_interval,
  max_reconnects, retry_on_initial_connect}`.
- Conventions: Conventional Commits, no `Co-Authored-By`; clippy `-D warnings`
  both feature sets; `absolute-paths = "deny"`; `cargo nextest run` (never
  `-q` to nextest). Docs: `docs/pages/backends/nats.mdx:27` currently says
  config uses plain field assignment; that sentence changes.

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
- `src/backends/nats/client.rs`
- `docs/pages/backends/nats.mdx` (config section rewrite)
- `tests/nats_integration.rs` (domain/timeout smoke assertions where cheap)

**Out of scope**:
- Removing/renaming existing `pub` fields (deprecation cycle: keep them
  working; builders are additive).
- Stream/consumer configuration (plan 013) and ack_wait (plan 009).
- Other backends.

## Steps

### Step 1: New fields

Add to `NatsConfig` (private where possible — new fields need no
compatibility):

```rust
/// JetStream domain for leaf-node / multi-domain deployments. `None` uses
/// the default domain.
pub(crate) jetstream_domain: Option<String>,
pub(crate) connection_timeout: Option<Duration>,
pub(crate) request_timeout: Option<Duration>,
pub(crate) ping_interval: Option<Duration>,
pub(crate) max_reconnects: Option<usize>, // match async-nats' type exactly
/// Applied to the fully-built ConnectOptions last; escape hatch for options
/// without a first-class field.
pub(crate) connect_options_fn: Option<Box<dyn Fn(async_nats::ConnectOptions) -> async_nats::ConnectOptions + Send + Sync>>,
```

`connect_options_fn` breaks `Debug` derive — the hand-written Debug already
exists; render it as `Some(<fn>)`/`None`. If `NatsConfig: Clone` is
implemented or required anywhere (grep for `.clone()` on configs), use
`Arc<dyn Fn ...>` instead of `Box` — check first.

### Step 2: Builders

Consuming builders mirroring Kafka's shape, each with rustdoc stating the
default: `with_jetstream_domain`, `with_connection_timeout`,
`with_request_timeout`, `with_ping_interval`, `with_max_reconnects`,
`with_connect_options(f)`, plus auth/TLS builders over the existing fields:
`with_tls_ca_cert`, `with_client_certificate(cert, key)`,
`with_user_password(user, pass)`, `with_token`, `with_nkey_seed`,
`with_credentials_file`. Keep field assignment working (no deprecation
attributes yet — just document builders as the preferred style).

### Step 3: Connect wiring + auth validation

In `connect`:
1. Count configured auth methods (user+pass pair counts as one; a username
   without a password or vice versa is itself a `Validation` error). More
   than one → `ShoveError::Validation` listing the configured methods by
   name. Zero is fine (anonymous).
2. Wire the new options onto `ConnectOptions` when set; apply
   `connect_options_fn` **last** (document that it can override first-class
   fields; that is its purpose).
3. Build jetstream with the domain:
   `let jetstream = match &config.jetstream_domain { Some(d) => jetstream::with_domain(client.clone(), d), None => jetstream::new(client.clone()) };`

### Step 4: Tests

Unit (in-file `mod tests`, no broker needed):
- two auth methods → `Validation` naming both;
- username without password → `Validation`;
- Debug renders the new fields and never panics with `connect_options_fn` set;
- builders set the fields they claim (plain assertions).

Integration (`tests/nats_integration.rs`): one smoke test that
`with_request_timeout` + `with_connect_options(|o| o)` still connects and
publishes against the container (proves wiring compiles into a working
client). A true domain test requires a domain-configured server; if the
test container can't express it cheaply, assert only that
`with_jetstream_domain("acme")` produces a jetstream context whose API prefix
includes the domain (async-nats exposes the prefix on the Context — check;
if not accessible, skip with a NOTES entry rather than forcing it).

**Verify**: all table commands pass.

## Done criteria

- [ ] `cargo fmt -- --check`, both clippy commands, `cargo doc` exit 0
- [ ] `cargo nextest run --features nats --lib` all pass (incl. new tests)
- [ ] `cargo nextest run --features nats --test nats_integration` all pass
- [ ] Multi-auth config errors with `Validation` (test proves it)
- [ ] `docs/pages/backends/nats.mdx` no longer claims builder-less config;
      documents domain + escape hatch
- [ ] Existing `pub` field assignment still compiles (an existing test or a
      new one exercises it)
- [ ] No files outside scope modified

## STOP conditions

- async-nats 0.49 lacks any of the named ConnectOptions methods or
  `jetstream::with_domain` (report actual names/availability).
- Auth validation breaks an existing integration test that sets multiple
  methods deliberately (report it — that would be a real user pattern).
- `NatsConfig` turns out to require `Clone` somewhere that fights the
  closure field.

## Maintenance notes

- The escape-hatch closure runs last by contract; future first-class fields
  must be applied before it.
- Next cycle: deprecate direct field access (`#[deprecated]` on fields or
  privatize with accessors) once downstream users migrate to builders.
- Plan 009's `derive_ack_wait` invariant (`ack_wait > handler_timeout`) is
  where a future `with_ack_wait` knob should route.
