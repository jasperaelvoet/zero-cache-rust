# zero-cache (Rust port)

A from-scratch Rust reimplementation of Rocicorp's [`zero-cache`](https://github.com/rocicorp/mono/tree/main/packages/zero-cache) — the sync engine behind [Zero](https://zero.rocicorp.dev/). It reads the same `ZERO_*` environment variables as upstream and speaks the same client WebSocket protocol, so it can stand in for the official `rocicorp/zero` server.

> **Full change history** lives in git. This file is the human-readable overview; it replaced a 3,000-line append-only ledger (see `git log -- PORTING.md`).

---

## What it does

```
Postgres (logical replication)
     │  pgoutput stream
     ▼
SQLite replica (WAL)  ──►  IVM query hydration  ──►  poke  ──►  WebSocket client
     ▲                                                   ▲
     │ writes replicate back                             │ live updates on every commit
     └──────────────  custom mutators (ZERO_MUTATE_URL)  ┘
                       custom queries  (ZERO_QUERY_URL)
```

- **Replicator** connects to upstream Postgres, does an initial snapshot copy into a local SQLite replica, then streams and applies every commit.
- **View-syncer** serves each WebSocket client: hydrates its desired queries from the replica and pushes a `poke` on every relevant change.
- **Custom mutators/queries**: writes and reads are forwarded to the app's own HTTP servers (server-authoritative model), then results replicate back.
- **Scaling**: one replicator/change-streamer node streams commits to many view-syncer nodes (horizontal), and `ZERO_NUM_SYNC_WORKERS` sets worker threads (vertical).

---

## Status

The core sync pipeline is implemented and heavily tested (~1,300 tests across the workspace, many against a live Postgres). It is **not yet proven against a real app end-to-end** — see [Known gaps](#known-gaps).

| Area | State |
| --- | --- |
| PG logical replication → SQLite replica | ✅ Working, live-tested (incl. SCRAM/MD5 auth for RDS) |
| Initial sync + resync on schema drift | ✅ Working |
| Query hydration (filters, `orderBy`, `limit`, `start`, related, **`WHERE exists`/`or`**) | ✅ Working, live-tested |
| Live pokes on commit | ✅ Working |
| Custom mutators (`ZERO_MUTATE_URL`) | ✅ Forwarded + relayed; **mock-tested, not run against a real app server** |
| Custom queries (`ZERO_QUERY_URL`) | ✅ Transform fetch + hydration; same caveat |
| Session-cookie / header forwarding to app servers | ✅ Working |
| Horizontal + vertical scaling | ✅ Implemented |
| Litestream backup/restore | ✅ Wired (binary bundled); restore-format compat with rocicorp's fork not verified |
| Structured logging (`ZERO_LOG_LEVEL`/`FORMAT`, slow-query, etc.) | ✅ Working |
| Metrics/health (`/metrics`, `/healthz`, `/readyz`) | ✅ Working |
| Auth (HS256 JWT) | ✅ Working |
| Compiled `definePermissions` row-rules | ❌ **Not enforced** (see gaps) |

---

## Configuration

Reads upstream `ZERO_*` env vars. Notable ones:

| Var | Purpose |
| --- | --- |
| `ZERO_UPSTREAM_DB` | Postgres connection (libpq or URL). Enables synced mode. |
| `ZERO_REPLICA_FILE` | Path to the local SQLite replica. |
| `ZERO_APP_ID`, `ZERO_SHARD_NUM`, `ZERO_APP_PUBLICATIONS` | Shard identity + publications. |
| `ZERO_MUTATE_URL`, `ZERO_QUERY_URL` (+ `_API_KEY`) | App's custom mutator / synced-query servers. |
| `ZERO_QUERY_FORWARD_COOKIES`, `ZERO_MUTATE_FORWARD_COOKIES` | Forward the client's session cookie to those servers (cookie-auth apps). |
| `ZERO_QUERY_ALLOWED_CLIENT_HEADERS`, `ZERO_MUTATE_ALLOWED_CLIENT_HEADERS` | Which client headers to forward. |
| `ZERO_AUTH_SECRET` (+ `_ISSUER`/`_AUDIENCE`) | HS256 JWT validation. |
| `ZERO_CVR_DB`, `ZERO_CHANGE_DB` | Provisioned; port keeps CVR/change-log local (see gaps). |
| `ZERO_LITESTREAM_BACKUP_URL` | S3/object-store continuous backup + restore. |
| `ZERO_LOG_LEVEL`, `ZERO_LOG_FORMAT` (`text`/`json`) | Structured, levelled logging. |
| `ZERO_LOG_SLOW_HYDRATE_THRESHOLD`, `ZERO_LOG_SLOW_ROW_THRESHOLD`, `ZERO_LOG_IVM_SAMPLING`, `ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG`, `ZERO_LITESTREAM_LOG_LEVEL` | Observability tuning. |
| `ZERO_PORT`, `ZERO_METRICS_ADDR`, `ZERO_MAX_CONNECTIONS`, `ZERO_NUM_SYNC_WORKERS`, `ZERO_CHANGE_STREAMER_URI` | Server / scaling. |

Recognized-but-not-yet-honored vars are listed at startup as a warning.

---

## Running

```bash
# Docker (bundles the litestream binary):
docker build -t zero-cache-rust .
docker run -e ZERO_UPSTREAM_DB=postgres://… -e ZERO_REPLICA_FILE=/data/replica.db \
           -p 4848:4848 -p 9600:9600 zero-cache-rust

# Local:
ZERO_UPSTREAM_DB=… ZERO_REPLICA_FILE=./replica.db cargo run -p zero-cache-server
```

Health: `GET :9600/readyz` flips ready after initial sync; `:9600/metrics` is Prometheus.

---

## Running, testing & benchmarking

```bash
scripts/run.sh              # run the server (--docker for the compose stack)
scripts/test.sh             # full suite; live-PG tests skip without a DB
scripts/test.sh --with-pg   # disposable Docker Postgres so live tests run too
scripts/bench.sh            # head-to-head vs official rocicorp/zero (same
                            # data, identical load): latency, throughput,
                            # CPU, memory
```

Live-Postgres tests always run serially (they share one database); point at
your own instance with `ZERO_TEST_PG_URL` / `ZERO_TEST_PG_TCP` instead of
`--with-pg` if preferred.

---

## Known gaps

Honest list of what stands between this and "drop-in production ready":

1. **Compiled `definePermissions` are not enforced.** The `zero-cache-auth` read/write authorizers are ported and unit-tested but not wired into the live path (the write path passes `authorized: true`; permissions are never parsed from `ZERO_SCHEMA_JSON`). Closing this needs a compiled-permissions-JSON parser **and** `authData`/JWT-claims substitution into rule conditions.
   *Not a blocker for **server-authoritative** apps* (`defineQueries`/`defineMutators`), which enforce auth in their own query/mutate servers — that path works and is tested.
2. **Never run against a real app end-to-end.** All custom-mutator/query tests use faithful mock servers, not the actual `@rocicorp/zero` node framework. The wire contract (request shape, response parsing, cookie forwarding, complex-AST hydration) is verified; a full system test against a real app + its schema + a real Zero client is the remaining validation.
3. **CVR / change-log are stored locally, not in `ZERO_CVR_DB`/`ZERO_CHANGE_DB`.** The port scales via the change-streamer instead. Functionally equivalent for a single logical deployment; the config is accepted and the CVR schema is provisioned, but multi-node CVR sharing via Postgres is not used.
4. **Litestream restore-format** compatibility with rocicorp's litestream fork (used by some deployments) is unverified. Restore failure falls back to a full Postgres initial-sync (correct, slower), so it's never a correctness dependency.
5. **Not a byte-for-byte drop-in for the multi-process `rocicorp/zero` deployment** (admin password, worker dispatcher). This port is single-process; the client protocol matches, the internal process topology does not.

---

## Layout

```
crates/
  zero-cache-types        shared types (AST, specs, shards, LSN, …)
  zero-cache-shared       JSON/bigint, misc primitives
  zero-cache-protocol     wire protocol (connect, push, poke, queries, inspect)
  zero-cache-change-source  Postgres logical replication (pgoutput, slots, schema)
  zero-cache-sqlite       SQLite replica, apply loop, query builder, hydration
  zero-cache-zql          ZQL / IVM (table source, filter, joins, planner)
  zero-cache-view-syncer  CVR, poke building, query hydration
  zero-cache-mutagen      mutations, API-server forwarding
  zero-cache-auth         permission model + read/write authorizers
  zero-cache-services     metrics
  zero-cache-config       config normalization
  zero-cache-server       the binary: replicator, view-syncer, HTTP/WS, logging
  zero-cache-workers      connect-param parsing
scripts/                  run.sh · test.sh · bench.sh
bench/                    compose stack + seed + loadtest driver for bench.sh
```
