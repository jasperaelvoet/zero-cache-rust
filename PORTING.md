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
  with cross-client query ref-counting, a per-group advance log that fans one
  group advance out to every connection (each reads each commit once from its
  own cursor, filtered to the queries it desires), and a GROUP-OWNED CVR: one
  in-memory CVR per group, checked out/in by transitions under the group's
  transition lock instead of re-loading the durable CVR from Postgres per
  transition. Covered by live multi-connection e2e tests
  (`tests/group_multiconn_e2e.rs`) and conformance-green with the flag on and
  off. **Gap (why it is still opt-in):** the 300-group fanout bench collapses
  with the flag on (hydrate-path CPU/memory, dominated by the per-transition
  group-CVR state clone); the per-group single-processing restructure that
  removes the per-connection re-clone is what flips the default.
- **Operator graph** (`crates/zero-cache-zql/src/ivm/`): Filter, Join, Skip,
  Take (global and per-parent PARTITIONED limits), Exists, FanOut, and FanIn
  are ported test-first from upstream; `build_pipeline` wires single-table,
  filter, keyset `start` bounds (including partial bound rows naming only the
  declared orderBy prefix), take, `related` joins, `whereExists`
  (EXISTS/NOT EXISTS), and an OR of correlated subqueries at any nesting depth
  (including inside an AND); every subquery's source carries its own ordering,
  so child bounded+ordered subqueries build too. Exercised end-to-end by the
  hunting-game-shaped suite (`tests/hunting_game_hard_e2e.rs`). **Gap:**
  `FlippedJoin` (unused — no planner emits `flip`).
- **Incremental advancement:** direct queries advance incrementally from the
  snapshot diff. Complex and bounded+ordered queries advance through the
  replica-backed operator graph (SQL-pushdown re-fetch), not the O(table)
  `materialize_query`; equivalence is oracle-tested. **Gap:** the graph is still
  rebuilt transiently per advance (a re-fetch, not a push of individual
  `SourceChange`s) — true push-incremental advance needs the persistent per-group
  graph. `materialize_query` remains only as the `ZERO_IVM_GRAPH=0` fallback and
  is slated for deletion.

Rust-only escape-hatch env vars remain temporarily and will be deleted once the
behavior they gate is validated by default: `ZERO_DEFER_CVR_ROWS` (upstream
defers by default — already the default here), `ZERO_IVM_GRAPH`,
`ZERO_GROUP_OWNERSHIP`, `ZERO_CVR_MAX_CONNS`, `ZERO_CVR_DEFER_FLUSH_CONCURRENCY`.
