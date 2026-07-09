# zero-cache-rust

A from-scratch Rust port of [Rocicorp Zero's `zero-cache`](https://github.com/rocicorp/mono/tree/main/packages/zero-cache)
sync engine.

This is a **test-first** port: each unit's tests are ported first (as the
executable spec), then the implementation is written until they pass. It covers
the full sync data path end to end, with 1000+ tests across 13 crates.

See [`PORTING.md`](./PORTING.md) for the per-slice completion log and crate map.

## What's implemented

The full sync pipeline, ported and tested:

- **Replication ingest** — Postgres logical replication (`pgoutput`), initial
  snapshot sync, ongoing apply loop, replication-slot feedback.
- **Supervised replicator** — reconnect-and-resume from the confirmed LSN;
  schema-drift detection → resync (reset replica + re-run initial sync);
  live-verified across multi-cycle runs.
- **Change fan-out** — commit → subscriber notification → lagged catch-up from
  the durable change-log.
- **Query invalidation → re-hydration → poke** — a commit's changed tables →
  affected queries → IVM re-hydration → client poke.
- **Durable CVR** — load → advance → flush cycle with optimistic-concurrency
  version guards, backed by Postgres.
- **WebSocket sync** — the full wire protocol (`initConnection`, `ping`/`pong`,
  `changeDesiredQueries`, `push`, `pull`, `inspect`, `closeConnection`),
  init-ordering enforcement, pokes, and clean teardown.
- **Query-planner cost model** — the `scanstatus_v2` SQLite cost model
  (default-on, verified against real SQLite).
- **Observability** — an OTel-shaped metrics registry with live instrumentation,
  exported both as Prometheus scrape output and OTLP/HTTP push.
- **Top-level assembly** — a `SyncService` orchestrator and a runnable
  `zero-cache-server` binary.

## Build & test

```sh
cargo build --workspace
cargo test  --workspace -- --test-threads=1   # live-Postgres tests run serially
```

Some tests exercise a real Postgres (they skip cleanly if none is reachable).
Point them at your instance with `ZERO_TEST_PG` / `ZERO_TEST_PG_TCP`.

The bundled SQLite is built with `SQLITE_ENABLE_STMT_SCANSTATUS` (via
[`.cargo/config.toml`](./.cargo/config.toml)) so the scanstatus cost model is
active out of the box.

## Run

```sh
cargo run -p zero-cache-server --bin zero-cache-server
# zero-cache-server listening on 0.0.0.0:4848
```

Configuration (environment):

| Variable                | Default          | Meaning                              |
| ----------------------- | ---------------- | ------------------------------------ |
| `ZERO_LISTEN_ADDR`      | `0.0.0.0:4848`   | `host:port` for the WebSocket server |
| `ZERO_FANOUT_CAPACITY`  | `1024`           | per-connection commit buffer depth   |

## Docker

```sh
docker compose up --build
```

Brings up Postgres (pre-configured for logical replication) and the sync server
on `:4848`. See [`docker-compose.yml`](./docker-compose.yml).

## Conformance tests

[`conformance/`](./conformance) contains a differential test harness that
replays identical WebSocket protocol scenarios against this server and the
official `rocicorp/zero` container and asserts normalized-equivalent responses.
See [`conformance/README.md`](./conformance/README.md).

## Layout

```
crates/
  zero-cache-types/         # lexi-version codec, LSN, pg types, shards
  zero-cache-shared/        # bigint-json, shared utilities
  zero-cache-protocol/      # AST, up/down wire messages, poke, query hashing
  zero-cache-change-source/ # pgoutput decode, published-schema introspection
  zero-cache-sqlite/        # replica store, apply loop, change-log, fan-out
  zero-cache-zql/           # ZQL planner + IVM operator graph
  zero-cache-view-syncer/   # CVR store, query hydration, poke builder
  zero-cache-mutagen/       # CRUD/custom mutation application
  zero-cache-auth/          # read/write authorizers
  zero-cache-services/      # change-streamer, notifier, metrics
  zero-cache-server/        # WebSocket transport, SyncService, main binary
  zero-cache-workers/       # connection dispatch
  zero-cache-config/        # configuration
mono-src/                   # upstream reference checkout (git-ignored)
```
