# Zero v1.7 compatibility contract

This repository targets exactly:

- Rocicorp mono commit `6863de5f00a3c1e7dc09c83ea3263dec4a94ebee`
- tag `zero/v1.7.0`
- sync protocol v51
- `@rocicorp/zero-sqlite3@1.1.2`

The checked-out reference is available under `mono-src/`; behavior is derived
from that pinned commit rather than the checkout's current `HEAD`.

## Authoritative surfaces

The official implementation defines:

- accepted configuration names, defaults, validation, and normalization;
- public HTTP and WebSocket routing and message ordering;
- replica, change-log, CVR, and permissions schemas;
- replication, resumption, reset, backup, and worker lifecycle behavior;
- ZQL planning, hydration, incremental advancement, and row synchronization;
- authentication, authorization, custom query, and mutation semantics;
- logging, metrics, inspection, health, readiness, and shutdown behavior.

Existing Rust APIs are internal and may change. An older Rust behavior is not a
compatibility requirement when it differs from the pinned server.

## Native SQLite

`rusqlite` remains the high-level API. The workspace patches
`libsqlite3-sys` with the official Zero amalgamation and compile definitions.
Startup verifies the exact version, required compile options, and
`BEGIN CONCURRENT`; replica creation verifies WAL2. The server fails fast if a
system or vanilla SQLite is linked accidentally.

Source provenance and checksums are recorded in
`vendor/libsqlite3-sys/ZERO-SQLITE3-SOURCE.md`.

## Acceptance gate

Completion requires differential conformance with the pinned official server
and 100% successful `ping`, `hydrate`, `fanout`, and `reconnect` workloads.
For each workload, p99 latency, throughput, CPU, and peak memory must be within
10% of official Zero. A comparison is invalid if either target fails setup,
times out, resets a connection, or misses required live pokes.

## Port status and known gaps

Differential conformance (`scripts/conformance.sh`: init / query / reconnect /
live-update-delete) is byte-for-byte green against the pinned official server.
Steady-state fanout is at parity (connected, ping, throughput); the outstanding
gap is initial **hydration** latency under high concurrency.

The query pipeline is mid-migration to the upstream client-group-owned,
replica-backed, incremental operator graph (see
`docs/query-pipeline-redesign.md`):

- **Client-group ownership** (upstream `ServiceRunner` + `ViewSyncerService`):
  implemented behind `ZERO_GROUP_OWNERSHIP` (default off) — a process-wide
  registry maps each `clientGroupID` to one shared `PipelineDriver`/snapshotter
  with cross-client query ref-counting. Single-connection conformance is green
  with the flag on and off. **Gap:** multi-connection advance fan-out (one group
  advance broadcast to every connection's poke) and a group-owned CVR are not
  yet done, so the flag stays off until they land and are covered by a
  multi-connection test.
- **Operator graph** (`crates/zero-cache-zql/src/ivm/`): Filter, Join, Skip,
  Take, and Exists are ported test-first from upstream; `build_pipeline` wires
  single-table, filter, skip, take, `related` joins, and `whereExists`.
  **Gap:** an OR of correlated subqueries (needs FanOut/FanIn) and `FlippedJoin`
  are not ported; those shapes fall back to the legacy path.
- **Incremental advancement:** direct queries advance incrementally from the
  snapshot diff. Complex queries are migrating off the legacy `materialize_query`
  full recompute onto the graph. `materialize_query` and the transient-graph
  rebuild remain until the persistent per-group graph lands.

Rust-only escape-hatch env vars remain temporarily and will be deleted once the
behavior they gate is validated by default: `ZERO_DEFER_CVR_ROWS` (upstream
defers by default — already the default here), `ZERO_IVM_GRAPH`,
`ZERO_GROUP_OWNERSHIP`, `ZERO_CVR_MAX_CONNS`, `ZERO_CVR_DEFER_FLUSH_CONCURRENCY`.
