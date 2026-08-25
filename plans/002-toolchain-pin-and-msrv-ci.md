# Plan 002: Adopt Rust 1.88 MSRV (let-chains) and gate it in CI

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving on. If a
> STOP condition occurs, stop and report. Update the status row in
> `plans/README.md` when done (unless a reviewer maintains the index).
>
> **Drift check (run first)**: `git diff --stat 106bb05..HEAD -- README.md Cargo.toml .github/workflows/ci.yml CONTRIBUTING.md`

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: dx
- **Planned at**: commit `106bb05`, 2026-06-20 — **revised 2026-06-20** after the
  first execution proved the real MSRV is **1.88**, not 1.85.

## Why this matters

The README advertised "Rust 1.85+", but the crate uses **let-chains**
(`if let … && let …`), stabilized in **Rust 1.88**, at `consumer_group.rs:215`,
`rabbitmq/publisher.rs:447`, `rabbitmq/consumer.rs:1246`, `nats/consumer.rs:238`,
`nats/publisher.rs:89`, and `kafka/consumer.rs:141`. Anyone pinned to 1.85–1.87
cannot build shove. The maintainer chose **option (a): adopt 1.88 as the true
MSRV** (rather than rewriting the let-chains to restore 1.85). This plan corrects
the documented MSRV everywhere and adds a CI job that builds on 1.88 so the floor
can't silently drift again.

## Current state (verified at the execution branch)

- No `rust-toolchain.toml`. `Cargo.toml` `[package]` has no `rust-version`.
- `README.md:129`: `- Rust 1.85+ (edition 2024)`.
- `CONTRIBUTING.md:8`: `- **Rust 1.85+** (edition 2024). …` (added by plan 001).
- CI `check` job (`.github/workflows/ci.yml:38-51`) installs `librdkafka-dev
  libsasl2-dev`, uses `dtolnay/rust-toolchain@stable`, runs fmt/clippy/test/
  publish-dry-run. It is the template for the new `msrv` job.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| MSRV build (full) | `cargo +1.88.0 build --all-features` | exit 0 |
| MSRV build (none) | `cargo +1.88.0 build --no-default-features` | exit 0 |
| Validate workflow YAML | `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` | exit 0 |

`rust-toolchain.toml` pinned to `1.88.0` makes cargo auto-select (and rustup
auto-install) that toolchain in the worktree, so `cargo build` alone exercises
1.88 once Step 2 lands.

## Scope

**In scope**: `Cargo.toml` (`[package]` only), `rust-toolchain.toml` (new),
`.github/workflows/ci.yml` (add one job), `README.md` (the requirements line),
`CONTRIBUTING.md` (the prerequisites line).

**Out of scope**: any `src/`/`tests/` file; dependency versions; the existing CI
jobs. Do NOT rewrite the let-chains (that was the rejected option b).

## Steps

### Step 1: Bump the documented MSRV to 1.88
- `Cargo.toml` `[package]`: add `rust-version = "1.88"` near `edition`.
- `README.md:129`: `Rust 1.85+` → `Rust 1.88+`.
- `CONTRIBUTING.md:8`: `Rust 1.85+` → `Rust 1.88+`.

**Verify**: `grep -rn "1\.85" README.md CONTRIBUTING.md Cargo.toml` → no matches;
`grep -n 'rust-version = "1.88"' Cargo.toml` → one match.

### Step 2: Create `rust-toolchain.toml`
```toml
[toolchain]
channel = "1.88.0"
components = ["rustfmt", "clippy"]
```
**Verify**: `cargo build --no-default-features` → exit 0 (cargo auto-selects
1.88.0; first run may install it).

### Step 3: Add the `msrv` CI job
Add alongside `check`/`audit`/`coverage` (do not modify those):
```yaml
  msrv:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - run: sudo apt-get update && sudo apt-get install -y librdkafka-dev libsasl2-dev
      - uses: dtolnay/rust-toolchain@1.88.0
      - uses: Swatinem/rust-cache@v2
      - run: cargo build --all-features
      - run: cargo build --no-default-features
```
Use `cargo build` (not `test`) — MSRV is about the crate's own tree, not
dev-deps. **Verify**: `python3 -c "import yaml; d=yaml.safe_load(open('.github/workflows/ci.yml')); assert 'msrv' in d['jobs']; print('OK')"`.

### Step 4: Verify the MSRV build
**Verify**: `cargo +1.88.0 build --all-features` and `--no-default-features`
both exit 0.

## Done criteria
- [ ] `rust-version = "1.88"` in `Cargo.toml`; `rust-toolchain.toml` pins `1.88.0`.
- [ ] README + CONTRIBUTING say "Rust 1.88+"; `grep "1\.85"` finds nothing in them.
- [ ] New `msrv` CI job builds `--all-features` + `--no-default-features` on
      `@1.88.0`; existing jobs unchanged; YAML parses.
- [ ] `cargo +1.88.0 build --all-features` exits 0.
- [ ] No `src/`/`tests/` files modified.

## STOP conditions
- `cargo +1.88.0 build` fails on something **newer** than let-chains (a feature
  stabilized after 1.88) — the real MSRV is higher still; report the exact
  feature/error and do not guess a version.
- A `rust-toolchain.toml` with a different channel already exists — report.

## Maintenance notes
- MSRV now lives in four places that must move together: `Cargo.toml`
  `rust-version`, `rust-toolchain.toml`, the `msrv` CI job, and the README/
  CONTRIBUTING lines.
