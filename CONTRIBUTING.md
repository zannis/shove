# Contributing to shove

Thanks for contributing! This guide covers how to build, test, and lint the
crate locally so your change passes CI on the first try.

## Prerequisites

- **Rust 1.91+** (edition 2024). The repo pins a toolchain via
  `rust-toolchain.toml`; matching the pinned floor guarantees your build matches
  the documented MSRV.
- **Docker**, running — the integration tests spin up real brokers
  (RabbitMQ, Kafka, NATS, Redis, LocalStack) via
  [`testcontainers`](https://docs.rs/testcontainers).
- **`cargo-nextest`** — this repo runs tests with `cargo nextest run`, not
  `cargo test`. Install with `cargo install cargo-nextest --locked`.
- **`dotenvx`** — used to inject secrets into the secret-bearing integration
  tests (AWS SNS/SQS, MSK-IAM). Install from https://dotenvx.com.

## Fast feedback loop (no Docker, no secrets)

The unit tests plus the in-memory-backed tests run without Docker or any
credentials:

```sh
cargo nextest run --no-default-features
```

(CI runs the equivalent `cargo test --no-default-features`; both work. This
repo's convention is `cargo nextest run`.)

## Running integration tests

Integration tests require **Docker**. The AWS (SNS/SQS) and MSK-IAM tests
additionally require a `LOCALSTACK_AUTH_TOKEN`, supplied through `dotenvx`:

```sh
cp .env.example .env
# edit .env and fill in LOCALSTACK_AUTH_TOKEN
dotenvx run -- cargo nextest run --features <backend-feature-set>
```

Per-backend feature sets (matching the CI matrix):

| Backend | Feature set |
|---|---|
| inmemory | `inmemory,metrics` |
| rabbitmq | `rabbitmq,audit,rabbitmq-transactional` |
| aws-sns-sqs | `pub-aws-sns,aws-sns-sqs,audit` |
| nats | `nats,audit` |
| kafka | `kafka,kafka-ssl,audit` |
| redis-streams | `redis-streams` |

The Kafka feature set also needs system libraries on Linux:
`librdkafka-dev` and `libsasl2-dev`.

Backends that need no secrets (inmemory, rabbitmq, nats, kafka, redis-streams)
can be run without `dotenvx`:

```sh
cargo nextest run --features <backend-feature-set>
```

## Before opening a PR — the gates CI enforces

```sh
cargo fmt -- --check
cargo clippy --all-features -- -D warnings
cargo clippy --no-default-features -- -D warnings
cargo publish --dry-run --all-features
```

CI also runs `cargo audit --deny warnings`.

## Rehearsing a release

Releases run through the **Publish** workflow
([`.github/workflows/publish.yml`](.github/workflows/publish.yml)), dispatched
manually with a `bump` of `patch`/`minor`/`major` (or `none` to re-run a
release whose version commit already landed).

### The commit being released must have a green CI run

Before anything else, the workflow resolves the `ci.yml` run for the commit it
is about to release and refuses to go on unless every leg — `check`, `msrv`,
`audit` and all seven `coverage` entries — completed successfully
([`.github/scripts/require-green-ci.sh`](.github/scripts/require-green-ci.sh)).

**A missing run fails the gate.** `ci.yml` has a `paths:` filter, so a commit
that touches only workflows or docs gets no run at all — which is how v0.13.0
came to be published from a commit no test job ever saw. The checks that were
green on it (CodeQL, the docs Workers build) say nothing about the crate, so
the gate names the `ci.yml` legs it wants rather than accepting "all checks
passed". An unfinished run is not a pass either.

The practical consequence: **cut a release from a commit that actually ran
CI**. If `main`'s head is a docs- or workflow-only commit, the gate will stop
the release; release from before it, or land a change under one of `ci.yml`'s
`paths:` entries first. On a `bump: <x>` dispatch the release commit is created
by the job and has no CI of its own by construction, so the gate checks its
parent and a follow-up step asserts the release commit differs from that parent
by nothing but the version bump.

### Rehearsing with `dry_run`

Because a crate version cannot be unpublished, the workflow takes a `dry_run`
input that rehearses the whole job without shipping anything:

```sh
gh workflow run publish.yml --ref <branch> -f bump=patch -f dry_run=true
```

A rehearsal runs every step of the real release — the version bump, the
`package.json`/`pnpm-lock.yaml` sync, the GPG-signed release commit and
annotated tag, the pure-version-bump assertion, the changelog generation and
the working-tree guard — and withholds only the three effects that cannot be
taken back:

| Step | Real release | `dry_run: true` |
|---|---|---|
| Push the release commit and tag | pushed with `--follow-tags` | verified locally with `git log --show-signature` and `git tag -v`, not pushed |
| Publish to crates.io | `cargo publish` | `cargo publish --dry-run` (no auth token minted) |
| Create GitHub release | `gh release create` | asserts the notes file exists, prints the command |

Two steps are downgraded rather than withheld, both for the same reason: a
rehearsal is normally dispatched from the branch that changes `publish.yml`,
and such a branch touches no path in `ci.yml`'s filter and carries only `ci:`
commits.

- **Empty release notes** fail a real release and only **warn** on a rehearsal
  — every commit in the branch's range is a `ci:` one, which `cliff.toml`
  skips. The rehearsal substitutes a stand-in file and carries on.
- **The CI gate** fails a real release and only **warns** on a rehearsal — such
  a branch has no `ci.yml` run to be green, so hard-failing would make the
  rehearsal useless for the one change it exists to validate. The verdict is
  still resolved and printed in full.

In both cases stopping would leave the later steps untested, and a rehearsal
publishes nothing, so there is no bad release to prevent.

Run one on a branch after changing `publish.yml`; the `bump: <x>` path is
otherwise only ever exercised by a real release.

## Conventions

- **Commit messages** follow [Conventional Commits](https://www.conventionalcommits.org)
  (e.g. `feat(nats): ...`, `fix(kafka): ...`, `docs(readme): ...`).
- There is **no CHANGELOG** — releases are cut with `release: vX.Y.Z` commits.
- The crate is licensed **MIT**.
