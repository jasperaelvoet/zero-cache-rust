# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A from-scratch, **test-first** Rust port of Rocicorp Zero's `zero-cache` sync engine: each unit's upstream tests are ported first (as the executable spec), then the implementation is written until they pass. It reads the same `ZERO_*` env vars and speaks the same client WebSocket protocol as the official `rocicorp/zero` server. The upstream TypeScript reference lives in `mono-src/` (git-ignored checkout) — consult it when porting behavior. Port status, config reference, and the honest known-gaps list live in `PORTING.md`.

## Commands

```sh
cargo build --workspace                                  # build everything
scripts/test.sh                                          # full suite (live-PG tests skip without a DB)
scripts/test.sh --with-pg                                # spins up disposable Postgres 16 so live tests run too
cargo test -p zero-cache-protocol                        # one crate while iterating
cargo test -p zero-cache-sqlite test_name                # single test
cargo fmt --all -- --check                               # formatting
cargo clippy --workspace --all-targets -- -D warnings    # lints
scripts/run.sh                                           # run the release server (--docker for the compose stack)
scripts/bench.sh 5000 60                                 # head-to-head bench vs official rocicorp/zero
```

Live-Postgres tests share one database and must run serially (`--test-threads=1`); point them at your own instance with `ZERO_TEST_PG_URL` / `ZERO_TEST_PG_TCP` instead of `--with-pg` if preferred.

Do not remove `.cargo/config.toml` — it builds SQLite with `SQLITE_ENABLE_STMT_SCANSTATUS`, which the query-planner cost model requires.

## Architecture

The data path, end to end:

```
Postgres (logical replication, pgoutput)
     │
     ▼
SQLite replica (WAL) ──► change fan-out ──► query invalidation ──► IVM re-hydration ──► poke ──► WebSocket client
     ▲                                                                                    ▲
     └── custom mutators (ZERO_MUTATE_URL) / custom queries (ZERO_QUERY_URL) ─────────────┘
```

- **Replicator** (`zero-cache-change-source` + `zero-cache-sqlite`): connects to upstream Postgres, initial-syncs published tables into a local SQLite replica, then streams and applies every commit. Reconnect-and-resume from confirmed LSN; schema drift triggers a full resync.
- **Change fan-out** (`zero-cache-sqlite`, `zero-cache-services`): commit → subscriber notification → lagged catch-up from the durable change-log.
- **View-syncer** (`zero-cache-view-syncer` + `zero-cache-zql`): per-client — hydrates desired queries from the replica via the ZQL planner/IVM operator graph, maintains a durable CVR (load → advance → flush with optimistic-concurrency version guards), and builds pokes on every relevant change.
- **Server** (`zero-cache-server`): WebSocket transport and wire protocol, `SyncService` orchestrator, config, auth (HS256 JWT), metrics/health endpoints, and the main binary. Cross-component e2e tests live in `crates/zero-cache-server/tests/`.
- **Mutations** (`zero-cache-mutagen`): CRUD pushes applied to upstream Postgres; custom mutators/queries are forwarded to the app's own HTTP servers (server-authoritative model), then results replicate back.
- **Scaling**: one change-streamer node owns the single Postgres replication slot and streams commits over WebSocket to many stateless view-syncer nodes (`ZERO_CHANGE_STREAMER_URI`); no second replication slot.

Foundation crates: `zero-cache-types` (lexi-version codec, LSN, pg types, shards), `zero-cache-protocol` (AST, up/down wire messages, poke, query hashing), `zero-cache-shared`, `zero-cache-config`, `zero-cache-auth`, `zero-cache-workers`.

Note: compiled `definePermissions` row-rules are ported but **not enforced** in the live path — the port targets server-authoritative apps (custom queries/mutators enforce auth in their own servers).

## Conventions

- Unit tests live beside their implementation in `mod tests`; end-to-end behavior goes in the server's `tests/` directory. Use descriptive `snake_case` test names with `#[test]` / `#[tokio::test]`.
- Standard `rustfmt` style. Prefer typed errors and `Result` propagation over panics in production paths.
- Keep crate boundaries aligned with the existing pipeline responsibilities.
- Commits: concise descriptive subjects, no type prefix. PRs should name affected crates and list verification commands; include logs or bench results for protocol/replication/performance changes.
- Binary protocol fixtures: `crates/zero-cache-change-source/testdata/`. Bench tooling and seed data: `bench/`.
