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
  graph. `materialize_query` has been deleted from the production path and
  survives only as the test-side oracle.

Rust-only escape-hatch env vars remain temporarily and will be deleted once the
behavior they gate is validated by default: `ZERO_DEFER_CVR_ROWS` (upstream
defers by default — already the default here), `ZERO_GROUP_OWNERSHIP`,
`ZERO_CVR_MAX_CONNS`, `ZERO_CVR_DEFER_FLUSH_CONCURRENCY`.

Upstream Postgres TLS: connection strings honor `sslmode=disable|prefer|require`
with libpq `require` semantics (encrypt, no certificate verification — matching
upstream's `postgres.js` behavior, and required for managed Postgres like RDS
with `rds.force_ssl=1` whose certs are signed by a private provider CA). Both
the `tokio-postgres` connections and the raw replication-protocol connection
negotiate TLS (`crates/zero-cache-change-source/src/pg_tls.rs`). **Gap:**
`verify-ca`/`verify-full` are rejected at config parse (no CA-bundle plumbing)
rather than supported.

## Official `ZERO_*` option coverage

Every option in upstream's `zero-config.ts` is parsed and honored; there is no
longer a reject-list. The server fails startup only where upstream itself
asserts (removed `ZERO_SHARD_ID`, `sslmode=verify-*`, `upstream.type=custom`
(unreleased), `change.logBatchSize < 1`, more than one of jwk/jwksUrl/secret,
pool bounds below the sync-worker count, `backupUsingV5` without
`restoreUsingV5`, invalid `websocketCompressionOptions` JSON). Deprecated names
(`ZERO_PUSH_*`→`ZERO_MUTATE_*`, `ZERO_GET_QUERIES_*`→`ZERO_QUERY_*`,
`ZERO_CHANGE_STREAMER_ADDRESS`/`_PROTOCOL`, `ZERO_TARGET_CLIENT_ROW_COUNT`)
resolve to their replacement with a one-time startup warning.

Fully behavioral:
- **Connections/pools:** `UPSTREAM_MAX_CONNS` (+ hidden `_PER_WORKER`) bounds
  concurrently-open upstream mutation clients via a semaphore; `CVR_MAX_CONNS`
  sizes the CVR pool; the "≥1 conn per sync worker" checks fire at startup.
- **Replication:** `PG_REPLICATION_SLOT_FAILOVER` adds `(FAILOVER)` on PG 17+;
  `REPLICA_VACUUM_INTERVAL_HOURS` VACUUMs at startup off the `_zero.runtimeEvents`
  clock; `REPLICATION_LAG_REPORT_INTERVAL_MS` round-trips a
  `pg_logical_emit_message` and logs total lag; `CHANGE_STREAMER_STARTUP_DELAY_MS`
  delays takeover on dedicated streamer nodes.
- **Initial sync:** `TABLE_COPY_WORKERS` parallelizes the COPY read side across
  snapshot-bound connections; `TEXT_COPY` switches to text-format COPY;
  `PROFILE_COPY` logs per-table timings.
- **Shadow sync:** `SHADOW_SYNC_ENABLED`/`_INTERVAL_HOURS`/`_SAMPLE_RATE`/
  `_MAX_ROWS_PER_TABLE` run the jittered canary on the change-streamer node.
- **CVR GC:** the three `CVR_GARBAGE_COLLECTION_*` options drive the purger
  (`cvr_purger.rs`) with upstream's batch-growth/backoff schedule; batch 0 off.
- **Query engine:** `ENABLE_QUERY_PLANNER=false` builds pipelines without the
  cost model; `ENABLE_QUERY_COVERING` runs shadow coverage logging;
  `QUERY_HYDRATION_STATS` logs rows-considered; `YIELD_THRESHOLD_MS` yields the
  hydration loop.
- **Mutations:** `PER_USER_MUTATION_LIMIT_MAX`/`_WINDOW_MS` enforce a
  per-client-group sliding window (returns "Rate limit exceeded").
- **Auth:** `AUTH_REVALIDATE_INTERVAL_SECONDS` re-verifies the token and
  disconnects on expiry; `AUTH_RETRANSFORM_INTERVAL_SECONDS` re-hydrates.
- **Header forwarding:** `{QUERY,MUTATE}_ALLOWED_REQUEST_HEADERS` forward
  connection-request headers to the API servers (in addition to
  `_ALLOWED_CLIENT_HEADERS`).
- **Discovery:** `CHANGE_STREAMER_MODE=discover` resolves the URI from the
  cdc-schema `replicationState` the owner registers (`DISCOVERY_INTERFACE_
  PREFERENCES` picks the host IP); `dedicated` runs a local streamer.
- **WebSocket:** `WEBSOCKET_MAX_PAYLOAD_BYTES` caps incoming frames at the
  protocol layer.
- **Lifecycle/misc:** `LAZY_STARTUP` defers replication until the first sync
  request; `TASK_ID` falls back to the ECS TaskARN; `STORAGE_DB_TMP_DIR`,
  `SERVER_VERSION`, `ADMIN_PASSWORD`, `KEEPALIVE_TIMEOUT_MS`, all `LOG_*` honored.
- **Litestream:** every `LITESTREAM_*` knob is passed to the litestream
  process (config-file mode when the yaml exists), including the v5 restore/
  backup executables and the derived checkpoint page counts.
- **CloudEvents:** `CLOUD_EVENT_SINK_ENV`/`_EXTENSION_OVERRIDES_ENV` publish
  lifecycle ZeroEvents as gzip+base64 structured CloudEvents.

Accepted with a documented behavior note (parsed, honored where meaningful,
but not a byte-identical mechanism):
- **`WEBSOCKET_COMPRESSION`**: tungstenite does not implement RFC 7692
  permessage-deflate; the extension is negotiated, so a server that declines it
  interoperates identically — connections just run uncompressed. Options JSON
  is still validated at startup. Logged at WARN when enabled.
- **`ENABLE_TELEMETRY`**: this independent server never phones home to
  Rocicorp's endpoint (that would pollute the official fleet's anonymous
  dataset). "Enabled" keeps the `zero.*` usage counters local to the metrics
  endpoint; "disabled"/`DO_NOT_TRACK` matches upstream's opt-out exactly.
- **`CHANGE_STREAMER_BACK_PRESSURE_LIMIT_HEAP_PROPORTION` /
  `_FLOW_CONTROL_CONSENSUS_PADDING_SECONDS`**: parsed and carried, but the
  Rust fan-out uses a bounded broadcast channel rather than upstream's
  heap-proportion byte-accounting + per-subscriber ack consensus, so these
  tune a different backpressure mechanism. No config is rejected; the values
  are available for the future streamer-transport work (see interop verdict).
- **`ENABLE_QUERY_COVERING`**: shadow detection is exact-transformation-match
  (same root table + transformation hash), a subset of upstream's structural
  AST-subsumption check — diagnostic-only either way.
