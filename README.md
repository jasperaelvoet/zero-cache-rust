# zero-cache-rust

A native Rust implementation of the Zero v1.7 cache server. The compatibility
reference is Rocicorp mono commit `6863de5f00a3c1e7dc09c83ea3263dec4a94ebee`
(`zero/v1.7.0`, sync protocol v51).

The public contract is the official Zero server contract: its `ZERO_*`
configuration, WebSocket and HTTP protocols, CVR/change schemas, replication
semantics, and deployment roles. Rust-specific protocol fallbacks and public
configuration extensions are not supported.

## Architecture

```text
Postgres logical replication
          │
          ▼
Zero SQLite replica (WAL2, sole committed writer)
          │ ordered change log
          ▼
client-group concurrent snapshots ──► persistent ZQL/IVM pipelines
          │                                      │
          └──────────────── CVR / pokes ◄────────┘
```

- `rusqlite` is the Rust API, linked to the exact SQLite amalgamation from
  `@rocicorp/zero-sqlite3@1.1.2`.
- Each view-syncer uses leapfrogging `BEGIN CONCURRENT` snapshots.
- Queries hydrate once and then advance from snapshot diffs.
- Durable CVRs and mutation state use the official Postgres schemas.

## Run

At minimum, configure an upstream database or an official change-streamer:

```sh
ZERO_UPSTREAM_DB='postgres://postgres:postgres@localhost/zero' \
ZERO_REPLICA_FILE=zero.db \
cargo run --release -p zero-cache-server --bin zero-cache-server
```

The server intentionally does not provide a protocol-only standalone mode.
An official `ZERO_*` option is never accepted as a no-op: options whose v1.7
subsystem is not yet connected cause a startup error that names the option.
See [PORTING.md](./PORTING.md) for the frozen compatibility contract.

## Verify

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
scripts/test.sh --with-pg
scripts/conformance.sh
scripts/bench.sh 2000 30
```

Benchmark results are valid only when both this binary and the pinned official
Zero server initialize and sustain 100% of the requested workload with no
timeouts, resets, or missing fan-out pokes.
