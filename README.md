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

## Run, test, bench

The entry points in [`scripts/`](./scripts) use a compact terminal UI: long
Docker and Cargo output is captured under `target/script-logs/`, while the
terminal shows only status, elapsed time, and the final result. On failure, the
last useful lines and the full log path are printed. Set `VERBOSE=1` to stream
the underlying commands while debugging.

```sh
scripts/run.sh               # run the server (ZERO_* env passes through)
scripts/run.sh --docker      # or the Docker quick-start stack

scripts/test.sh              # full test suite (live-PG tests skip if no DB)
scripts/test.sh --with-pg    # spins up a disposable Postgres so they run too

scripts/bench.sh             # head-to-head vs official rocicorp/zero: same
                             # Postgres, same seeded data, identical load —
                             # side-by-side latency / throughput / CPU / memory
scripts/bench.sh 5000 60     # 5000 clients for 60s

scripts/simulate-production.sh          # sustained load + writes + autoscaling
scripts/simulate-production.sh --quick  # ~90 second smoke version
```

Live-Postgres tests run serially (they share one database); point them at your
own instance with `ZERO_TEST_PG_URL` / `ZERO_TEST_PG_TCP` instead of
`--with-pg` if you prefer.

The default test script also installs the lockfile-pinned official
`@rocicorp/zero` JavaScript client and runs it against a real Rust server with
custom query and mutation API endpoints. The black-box lifecycle covers query
hydration/completeness, optimistic writes, server mutation results, subsequent
query pokes, and fatal client diagnostics. Use plain `cargo test` for a faster
Rust-only iteration loop.

The bundled SQLite is built with `SQLITE_ENABLE_STMT_SCANSTATUS` (via
[`.cargo/config.toml`](./.cargo/config.toml)) so the scanstatus cost model is
active out of the box.

## Run

**Synced mode** (serves real data) — point it at a logical-replication Postgres:

```sh
ZERO_UPSTREAM_DB="host=localhost port=5432 user=postgres password=postgres dbname=zero" \
ZERO_REPLICA_FILE=./zero-replica.db \
cargo run -p zero-cache-server --bin zero-cache-server
# … starting replicator (initial sync)…
# … initial sync complete; serving from ./zero-replica.db
# … ops endpoint on 0.0.0.0:9600 (/metrics /healthz /readyz)
# … listening on 0.0.0.0:4848
```

The server initial-syncs the published tables into a durable WAL replica, streams
ongoing changes, and serves live query results + incremental pokes to WebSocket
clients. Without `ZERO_UPSTREAM_DB` it runs in **standalone mode** (in-memory,
protocol only). Readiness flips (`/readyz` → 200) once initial sync completes.

Configuration is read from `ZERO_*` env vars matching upstream zero-cache (see
[`config.rs`](./crates/zero-cache-server/src/config.rs)). Honored options:

| Variable                   | Default             | Meaning                                        |
| -------------------------- | ------------------- | ---------------------------------------------- |
| `ZERO_UPSTREAM_DB`         | *(unset)*           | libpq upstream conn string → **synced mode**   |
| `ZERO_PORT`                | `4848`              | WebSocket sync port (`ZERO_LISTEN_ADDR` overrides) |
| `ZERO_METRICS_ADDR`        | `0.0.0.0:9600`      | ops endpoint (`/metrics` `/healthz` `/readyz`) |
| `ZERO_REPLICA_FILE`        | `./zero-replica.db` | durable SQLite replica path                    |
| `ZERO_APP_ID`              | `zero`              | app id (schema/CVR isolation)                  |
| `ZERO_APP_PUBLICATIONS`    | *(shard default)*   | comma-separated upstream publications          |
| `ZERO_SHARD_NUM`           | `0`                 | shard number                                   |
| `ZERO_AUTH_SECRET`         | *(unset)*           | HS256 JWT secret → **auth enabled**            |
| `ZERO_AUTH_ISSUER` / `ZERO_AUTH_AUDIENCE` | *(unset)* | required `iss`/`aud` claims, if set          |
| `ZERO_MAX_CONNECTIONS`     | *(unbounded)*       | connection admission cap                       |
| `ZERO_ENABLE_CRUD_MUTATIONS` | `true`            | route client pushes to upstream Postgres       |
| `ZERO_AUTO_RESET`          | `true`              | resync replica on schema drift                 |
| `ZERO_LOG_LEVEL` / `ZERO_LOG_FORMAT` | `info` / `text` | logging                                    |
| `ZERO_TASK_ID` / `ZERO_SERVER_VERSION` / `ZERO_ADMIN_PASSWORD` | *(unset)* | instance id / version / admin |

Peripheral upstream vars (litestream, change-streamer discovery, cloud events,
mutate/query API servers, etc.) are **recognized** — if set they're logged as a
startup warning noting they aren't yet honored, so nothing is silently ignored.

## Docker

```sh
docker compose up --build
```

Brings up Postgres (pre-configured for logical replication) and the sync server
on `:4848`. See [`docker-compose.yml`](./docker-compose.yml).

## Horizontal scaling (change-streamer + view-syncers)

Like real zero, the port supports a multi-node topology so **one** node owns the
single Postgres replication slot and **many** stateless nodes serve clients:

- **Change-streamer / replicator node** — set `ZERO_UPSTREAM_DB`. It owns the
  slot, maintains the replica, and exposes a WebSocket replication stream on
  `ZERO_CHANGE_STREAMER_ADDR` (default `0.0.0.0:{port+1}`). Set
  `ZERO_NUM_SYNC_WORKERS=0` to make it a **dedicated** streamer (no client
  serving).
- **View-syncer nodes** (auto-scale these) — set
  `ZERO_CHANGE_STREAMER_URI=ws://<streamer-host>:{port+1}/replication`. Each
  bootstraps its replica from the streamer's snapshot, applies streamed commits,
  and serves clients — **no second replication slot**.

```sh
# node A: change-streamer (owns the slot)
ZERO_UPSTREAM_DB="…" ZERO_PORT=4848 zero-cache-server        # streams on :4849

# nodes B, C, … : view-syncers (scale horizontally)
ZERO_CHANGE_STREAMER_URI=ws://nodeA:4849/replication ZERO_PORT=4848 zero-cache-server
```

`ZERO_NUM_SYNC_WORKERS>0` sets the tokio worker-thread count (vertical
multi-core) on a node.

## Production deployment simulation

[`scripts/simulate-production.sh`](./scripts/simulate-production.sh) runs the
scalable topology for an extended traffic scenario instead of performing a
single benchmark burst. It starts Postgres, one dedicated change-streamer,
HAProxy, and an ephemeral pool of view-syncers; continuously mutates upstream
rows; and drives real hydration plus live-poke WebSocket traffic through a
warm-up, growth, peak, recovery, and quiet tail.

The local autoscaler uses HAProxy's active-session count and Docker CPU samples.
It has separate scale-out/scale-in thresholds, a scale-in stabilization window,
and a cooldown, and records every observation in
`simulation/results/<timestamp>/autoscaling.csv`. Each traffic phase writes its
latency, success, throughput, and fan-out report beside that file.

```sh
# About 15 minutes, up to six view-syncer replicas.
scripts/simulate-production.sh

# Custom traffic curve (concurrent clients:sustain seconds).
SIM_PHASES=50:60,500:180,1200:300,100:120 \
SIM_MAX_REPLICAS=8 SIM_TARGET_CONNECTIONS=150 \
scripts/simulate-production.sh

# Leave the deployment available for inspection after the run.
scripts/simulate-production.sh --quick --keep-up
```

This models orchestration behavior on a developer machine; it is not a claim
that Docker Compose itself is a production scheduler. The `view-syncer` service
is the unit a Kubernetes HPA or equivalent should scale in a real deployment.

## Benchmark internals

`./bench.sh` orchestrates everything under [`bench/`](./bench):

- `bench/docker-compose.bench.yml` — one Postgres, this port (`:4848`), and the
  official `rocicorp/zero` container (`:4849`), all on the same database
- `bench/seed.sql` — the identical dataset both servers sync
- `bench/loadtest/` — the load driver: thousands of concurrent WebSocket
  clients speaking the real `@rocicorp/zero` connect protocol
  (`/sync/v51/connect`), reporting connect success, ping latency
  p50/p90/p99, throughput, and per-container peak CPU / memory

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
