# Repository Guidelines

## Project Structure & Module Organization

This repository is a Rust 2021 workspace. Production code lives in `crates/`, split by responsibility: `zero-cache-server` provides the binary and WebSocket transport; `zero-cache-sqlite`, `zero-cache-change-source`, and `zero-cache-view-syncer` implement the replication path; protocol, auth, config, and shared types remain in their named crates. Unit tests generally live beside their implementation in `src/*.rs` under `mod tests`; cross-component server tests are in `crates/zero-cache-server/tests/`. Benchmark tooling and seed data live in `bench/`, while `scripts/` contains the supported developer entry points. Binary protocol fixtures are under `crates/zero-cache-change-source/testdata/`. See `PORTING.md` for the crate map and port status.

## Build, Test, and Development Commands

- `cargo build --workspace` builds every workspace crate.
- `scripts/run.sh` runs the release server locally; add `--docker` for the Postgres-backed Compose stack.
- `scripts/test.sh` runs workspace and load-test unit tests serially. Use `scripts/test.sh --with-pg` to include live replication, CVR, and mutation tests against disposable Postgres 16.
- `cargo test -p zero-cache-protocol` runs one crate while iterating.
- `cargo fmt --all -- --check` verifies formatting; `cargo clippy --workspace --all-targets -- -D warnings` catches common Rust issues.
- `scripts/bench.sh 5000 60` compares this server with upstream Zero using 5,000 clients for 60 seconds.

## Coding Style & Naming Conventions

Use standard `rustfmt` output (four-space indentation). Name modules, functions, and files in `snake_case`; structs, enums, and traits in `UpperCamelCase`; constants in `SCREAMING_SNAKE_CASE`. Keep crate boundaries aligned with the existing pipeline responsibilities. Prefer typed errors and `Result` propagation over panics in production paths. Do not remove `.cargo/config.toml`: it enables SQLite statement scan-status support.

## Testing Guidelines

Add focused unit tests next to changed code using descriptive `snake_case` names and `#[test]` or `#[tokio::test]`. Put end-to-end behavior in the server's `tests/` directory. Live Postgres tests share state, so keep them serial (`--test-threads=1`). No numeric coverage threshold is configured; new behavior and regressions should nevertheless be exercised before review.

## Zero Protocol Compatibility

Treat the official `@rocicorp/zero` implementation for the pinned protocol version as the wire-semantics reference. Preserve distinctions such as omitted fields versus explicit `null`; in particular, an unauthenticated `Sec-WebSocket-Protocol` payload encodes `{}`, and a new client group's first `initConnection` must include its `clientSchema`. Compatibility and benchmark clients must send the same URL parameters, headers, schema, and message sequence to both implementations, and a comparison is not valid when either target closes during initialization or fails to sustain the workload.

## Commit & Pull Request Guidelines

History currently uses a concise, descriptive subject without a type prefix (for example, `Rust port of Rocicorp zero-cache sync engine`). Keep commits narrowly scoped and write imperative or descriptive subjects. Pull requests should explain the behavior change, identify affected crates, list verification commands, and link relevant issues. Include logs or benchmark results for protocol, replication, or performance changes; screenshots are only useful for externally visible tooling or documentation changes.
