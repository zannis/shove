# Plan 003: Harden credential redaction in Debug impls (SchemaRegistryAuth, NatsConfig)

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan in
> `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 106bb05..HEAD -- src/schema_registry/client.rs src/backends/nats/client.rs src/backends/kafka/client.rs`
> If any of these changed, compare the "Current state" excerpts against the live
> code before proceeding; on a mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: security
- **Planned at**: commit `106bb05`, 2026-06-20

## Why this matters

This crate is careful about not leaking secrets through `Debug` — `KafkaTls`,
`KafkaSasl`, and `RabbitMqConfig` all have hand-written `Debug` impls that print
`<redacted>` for secret fields. Two credential-bearing types miss that bar:

1. `SchemaRegistryAuth` (`Bearer(String)` / `Basic { user, pass }`) derives
   nothing and has no `Debug` impl at all. It is not currently reachable via a
   `Debug`-printed parent, so this is a **latent** leak, not an active one — but
   the moment anyone adds `#[derive(Debug)]` to a struct that holds it (or logs
   it with `{:?}`), the bearer token / basic-auth password prints in clear.
2. `NatsConfig` has a hand-written `Debug` that redacts `username`, `token`, and
   `nkey_seed`, but silently **omits** the `password` and `tls_client_key`
   fields entirely. The password is not leaked today (it just isn't printed),
   but the omission is inconsistent with the surrounding redaction pattern and
   is exactly the kind of gap that becomes a leak when someone "completes" the
   struct later.

Both fixes are small, follow an existing in-repo pattern, and are guarded by
unit tests so the redaction can't silently regress.

## Current state

### SchemaRegistryAuth — `src/schema_registry/client.rs:17-23`

```rust
/// Authentication for registry HTTP calls.
#[derive(Clone)]
pub enum SchemaRegistryAuth {
    None,
    Bearer(String),
    Basic { user: String, pass: String },
}
```

No `Debug` derive or impl. The containing `SchemaRegistry` struct
(`client.rs:28-44`) does **not** currently derive `Debug`, which is why this is
latent rather than live. This type is only compiled under the
`kafka-schema-registry` feature (the module is gated in `src/lib.rs:166-168`).

### NatsConfig — `src/backends/nats/client.rs:14-32` and its Debug at `62-88`

Struct fields (secret-bearing ones called out):
```rust
pub struct NatsConfig {
    pub url: String,                       // may embed user:pass — already redacted in Debug
    pub tls_ca_cert: Option<PathBuf>,
    pub tls_client_cert: Option<PathBuf>,
    pub tls_client_key: Option<PathBuf>,   // OMITTED from Debug
    pub username: Option<String>,
    pub password: Option<String>,          // OMITTED from Debug
    pub token: Option<String>,
    pub nkey_seed: Option<String>,
    pub creds_file: Option<PathBuf>,
}
```

Current Debug impl (note: lists url, tls_ca_cert, tls_client_cert, username,
token, nkey_seed, creds_file — but NOT password, NOT tls_client_key):
```rust
impl fmt::Debug for NatsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted = /* nats://user:pass@host -> nats://***@host */;
        f.debug_struct("NatsConfig")
            .field("url", &redacted)
            .field("tls_ca_cert", &self.tls_ca_cert)
            .field("tls_client_cert", &self.tls_client_cert)
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("nkey_seed", &self.nkey_seed.as_ref().map(|_| "<redacted>"))
            .field("creds_file", &self.creds_file)
            .finish()
    }
}
```

There is an existing test in this file asserting the password does not appear in
Debug output (around `nats/client.rs:341`). Read it before editing and keep it
passing; extend it per the test plan.

### The pattern to match — `src/backends/kafka/client.rs`

`KafkaTls`'s Debug (`client.rs:58-81`) redacts each secret field as
`&self.field.as_ref().map(|_| "<redacted>")`. `KafkaSasl` (an **enum** with
secret-bearing variants) has a hand-written `Debug` at `client.rs:182` — read it
and mirror its style for the `SchemaRegistryAuth` enum (redact the token /
password, but it is fine to show the `Basic` username and the variant names).

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Build with SR feature | `cargo build --features kafka-schema-registry` | exit 0 |
| Unit test SR module | `cargo nextest run --features kafka-schema-registry schema_registry` | new + existing tests pass |
| Unit test NATS client | `cargo nextest run --features nats nats::client` | password redaction test passes |
| Lint (all features) | `cargo clippy --all-features -- -D warnings` | exit 0 |
| Format check | `cargo fmt -- --check` | exit 0 |

(If `cargo nextest` is unavailable, `cargo test` with the same filters works; the
repo convention is nextest.)

## Scope

**In scope** (modify only):
- `src/schema_registry/client.rs` — add a `Debug` impl for `SchemaRegistryAuth`
  (+ a unit test).
- `src/backends/nats/client.rs` — add the `password` and `tls_client_key` fields
  to the existing `NatsConfig` Debug impl (+ extend the existing redaction test).

**Out of scope** (do NOT touch):
- The `Clone` derive on `SchemaRegistryAuth` — keep it.
- The connection/HTTP logic, the redaction of `url`, or any other field's
  current behavior.
- Any other backend's config Debug impl (Kafka/RabbitMQ/Redis/SNS are already
  correct — do not "improve" them).
- Do not change any field's name, type, or visibility.

## Git workflow

- Branch: `advisor/003-credential-debug-hardening`
- One commit, Conventional Commits style, e.g.
  `fix(security): redact credentials in SchemaRegistryAuth and NatsConfig Debug`.
- Do NOT push or open a PR unless instructed. No `Co-Authored-By` trailer.

## Steps

### Step 1: Add a redacting `Debug` for `SchemaRegistryAuth`

In `src/schema_registry/client.rs`, add a hand-written `impl fmt::Debug for
SchemaRegistryAuth` (do NOT switch to `#[derive(Debug)]` — derive would print
the secrets). Target shape:

- `None` → `"None"`
- `Bearer(_)` → something like `Bearer("<redacted>")`
- `Basic { user, pass: _ }` → print the `user` value, redact `pass`
  (e.g. `Basic { user: "<user>", pass: "<redacted>" }`).

Match the formatting idiom used by `KafkaSasl`'s Debug impl
(`src/backends/kafka/client.rs:182`). Add `use std::fmt;` if not already
imported in this file.

**Verify**: `cargo build --features kafka-schema-registry` → exit 0.

### Step 2: Unit-test the SR redaction

Add a test (in the existing `#[cfg(test)] mod tests` in
`src/schema_registry/client.rs`, or create one) that builds each secret-bearing
variant with a recognizable sentinel value and asserts the sentinel does NOT
appear in `format!("{:?}", auth)` while `<redacted>` does. Cover `Bearer` and
`Basic` (the `pass` field). Use a literal placeholder token in the test such as
the string `"super-secret-token"` — this is test data, not a real credential.

**Verify**: `cargo nextest run --features kafka-schema-registry schema_registry`
→ the new test(s) pass.

### Step 3: Add the missing fields to `NatsConfig`'s Debug

In `src/backends/nats/client.rs`, extend the existing `impl fmt::Debug for
NatsConfig` so it lists **every** field. Add:

- `.field("password", &self.password.as_ref().map(|_| "<redacted>"))` —
  immediately after the `token` field, to match the existing redaction pattern.
- `.field("tls_client_key", &self.tls_client_key)` — next to the other
  `tls_client_*` fields (it is a path, so print it like `tls_client_cert`; the
  point is no field is silently dropped).

Do not change the existing `url` redaction or the other fields.

**Verify**: `cargo build --features nats` → exit 0.

### Step 4: Extend the NATS redaction test

Find the existing test near `src/backends/nats/client.rs:341` that asserts the
password is absent from Debug output. Update it so it now asserts the Debug
output contains `"<redacted>"` for the password field (construct a `NatsConfig`
with a sentinel password like `"sentinel-pw"`, assert that sentinel is NOT in the
`{:?}` output and that `password` appears as `<redacted>`). Keep any existing
assertions that still hold.

**Verify**: `cargo nextest run --features nats nats::client` → the redaction test
passes.

### Step 5: Full lint + format gate

**Verify**:
- `cargo clippy --all-features -- -D warnings` → exit 0.
- `cargo fmt -- --check` → exit 0.

## Test plan

- New: a `SchemaRegistryAuth` Debug-redaction test in
  `src/schema_registry/client.rs` (Step 2) — asserts secrets in `Bearer` and
  `Basic` are not printed and `<redacted>` is.
- Modified: the existing NATS password-redaction test in
  `src/backends/nats/client.rs` (Step 4) — now also asserts the `<redacted>`
  sentinel appears for `password`.
- Structural pattern to model the new SR test on: the existing redaction tests
  in `src/backends/kafka/client.rs` / `src/backends/nats/client.rs`.
- Verification: `cargo nextest run --features kafka-schema-registry,nats` → all
  pass, including the new and modified tests.

## Done criteria

ALL must hold:

- [ ] `SchemaRegistryAuth` has a hand-written `Debug` impl (no `#[derive(Debug)]`
      on it) that redacts the `Bearer` token and `Basic` password.
- [ ] `NatsConfig`'s Debug impl now lists `password` (redacted) and
      `tls_client_key`.
- [ ] `cargo nextest run --features kafka-schema-registry,nats` passes,
      including the new SR redaction test and the updated NATS test.
- [ ] `cargo clippy --all-features -- -D warnings` exits 0.
- [ ] `cargo fmt -- --check` exits 0.
- [ ] `grep -n "derive(Debug)" src/schema_registry/client.rs` does NOT show a
      derive on `SchemaRegistryAuth`.
- [ ] No files outside the two in-scope files are modified (`git status`).
- [ ] `plans/README.md` status row for 003 updated.

## STOP conditions

Stop and report back (do not improvise) if:

- The `SchemaRegistryAuth` enum or `NatsConfig` struct no longer matches the
  excerpts above (fields added/renamed since `106bb05`).
- You discover `SchemaRegistry` (or any struct holding `SchemaRegistryAuth`) now
  derives `Debug` AND is logged somewhere — that converts the latent leak into
  an active one; note it prominently in your report (it strengthens, not blocks,
  this fix).
- A clippy lint forces a structural change beyond adding the Debug impl/fields.

## Maintenance notes

- These types now require manual maintenance: any new secret-bearing field added
  to `NatsConfig` or new variant added to `SchemaRegistryAuth` must be redacted
  in the hand-written `Debug` and covered by the redaction test. A reviewer
  should reject `#[derive(Debug)]` on either type.
- No rotation is required — no secret value was ever committed; this plan only
  closes a print-path gap. (If, while working, you find an actual secret value
  in a committed file, STOP and report it as a separate finding with a rotation
  recommendation; do not paste the value anywhere.)
