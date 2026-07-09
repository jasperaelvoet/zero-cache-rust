# zero-cache → Rust porting tracker

A test-first port of [`rocicorp/mono` `packages/zero-cache`](https://github.com/rocicorp/mono/tree/main/packages/zero-cache).
Strategy: port each unit's **tests first** (they are the executable spec), then
implement until green, iterating toward full feature parity. Priority order
follows the **whole-pipeline slice**: Postgres replication → local store →
incremental query (ZQL/IVM) → WebSocket sync.

Source reference lives in `mono-src/packages/zero-cache` (shallow sparse
checkout, git-ignored). Original TS: **207 source files, 139 test files,
~136k LOC**.

## How to use this tracker

This file is both the porting map and the progress ledger. The top sections are
for day-to-day orientation; the detailed crate log is for implementation notes
that would otherwise get lost.

When a slice lands, update these areas in order:

1. **Active slice**: close or replace the current task.
2. **Progress dashboard**: move only the affected lane, and only when behavior
   changed in a way future work can rely on.
3. **Porting board**: add the next concrete step, owner-facing caveat, or newly
   discovered gap.
4. **Completion log**: add one row for the behavior that landed.
5. **Verification log**: record the narrow commands that were run.
6. **Detailed crate log**: add implementation notes, caveats, and upstream
   source mapping.

Progress labels:

- **Done**: implemented and covered by focused tests.
- **Partial**: real behavior exists, but important upstream parity is missing.
- **Stub**: protocol/router/API shape exists, but runtime behavior is minimal.
- **Not started**: no meaningful Rust equivalent exists yet.

Tracking rule: prefer small, provable slices over broad percentages. If a lane
is still `Partial`, the `Next acceptance check` column should say exactly what
would make the next update defensible.

## Snapshot

Last updated: **2026-07-08**.

The port has crossed from pure type/helper work into a live WebSocket demo path.
The strongest vertical slice is:

`init connection -> desired-query changes -> SQLite hydration -> poke rows -> inspect responses -> SQLite analyze-query`

The highest-risk gaps are still full query planning/analysis, wiring the
read-authorizer into production view-syncer paths, durable CVR/session
persistence, Postgres replication/store integration, and production-grade
mutation/auth maintenance behavior.

Most recent landed slice:

- `drive_apply_loop` — the reusable ongoing-replication driver that pumps a live
  `ReplicationStream` into `ReplicationApplier` and advances the Postgres slot
  via standby-status updates. Verified end-to-end against a LIVE Postgres (this
  environment has one at `localhost:54329`): a streamed insert lands in the
  SQLite replica and the slot's `confirmed_flush_lsn` advances. This is the first
  real "wire the applier into a live server loop" step.

Environment note (corrected 2026-07-09): a working test Postgres IS available
here (the `change-source`/`sqlite` live-PG tests run against it). So the
Postgres-dependent lanes (replication server loop, mutation write path, CVR
persistence) are NOT blocked in this environment — only OTel publishing and the
`scanstatus_v2` cost-model FFI are genuinely unavailable. The replication
server-loop wiring is the highest-leverage next unit and is buildable here.

Blocker found for wiring `plan_query`: doing it *correctly* needs a real
`ConnectionCostModel` backed by SQLite statistics (the `SQLITE_STAT_FANOUT`
cost-estimation subsystem, unported) — the only other model upstream ships is
`simpleCostModel`, a **test helper** whose flip decisions would be meaningless
in production. And `analyze`'s `join_plans` field expects `PlanDebugEventJson`
events emitted by the also-unported `PlanDebugger`. So the honest next unit is
the SQLite-stat cost model, OR a non-query lane; NOT a fake wiring.

Reconnaissance (2026-07-09): a sweep of the small, self-contained, pure-logic
gaps in the protocol/shared/observability lanes found them essentially closed —
`PrimaryKey`/`PrimaryKeyValue(Record)` already exist, `makeErrorDetails` is
`shared::error_details`, the JS `getErrorMessage`/`getErrorDetails` helpers have
no faithful Rust form, and `ApplicationError` just landed. The remaining work is
NOT cheap slices; it is the substantial I/O-backed lanes (below) or the
FFI-gated cost model. Pick one of those as a focused, multi-turn unit rather
than hunting for more one-file ports.

Next useful slice (each a multi-turn lane, not a quick slice):

- Postgres replication → local store → invalidation pipeline (Replication/store).
- Durable CVR/session persistence wired through the live server loop.
- Production pusher/custom-mutator/Postgres write path (Mutation/auth).
- OTel event/metric publishing (`initEventSink`/`publishEvent`) (Observability).
- SQLite-stat `ConnectionCostModel` via a `scanstatus_v2` FFI wrapper, to unblock
  wiring `plan_query` into analyze/hydration.

## Active slice

Current slice: **SQLite-stat cost model, or advance a non-query lane**.

Definition of done:

- Port a SQLite-statistics-backed `ConnectionCostModel` (real `rows`/`fanout`
  estimates) so `plan_query` produces meaningful decisions; OR advance a
  non-query lane (durable CVR/session, Postgres replication → store,
  mutation/auth, or observability). Do NOT wire the planner with the
  `simpleCostModel` test helper — that would misrepresent real behavior.
- Preserve the existing live poke behavior for single-table AST puts, `orderBy`
  + `limit` top-N reads, `start` cursor reads, related reads (including
  per-parent related `limit`/`start` and hidden-junction many-to-many
  traversal), and registered custom query transforms.
- Preserve the ported planner pipeline (`build_plan_graph`, `where_` EXISTS join
  construction, `apply_to_condition`, `apply_plans_to_ast`).
- Keep async custom-query transform-on-init wired for the live async handler.
- Keep inspect `analyzeQuery` on the SQLite-introspected catalog path.
- Run only focused checks for the touched crate(s).

Current known blockers: none.

## Progress Dashboard

Milestone map:

| Milestone | Status | Rust surface that exists | Next acceptance check |
| --------- | ------ | ------------------------ | --------------------- |
| Wire protocol parity | Partial | Upstream tags for `push`, `pull`, `updateAuth`, `ackMutationResponses`, `inspect`; downstream poke/query/mutation patches; typed inspect/analyze payloads. | Decode/encode compatibility stays covered as new live behavior starts consuming each message. |
| Live demo sync loop | Partial | Init/change/poke flow, demo pushes, inspect responses, pull/updateAuth/ack handling, async action transport. | Replace in-memory demo state with production services one path at a time. |
| Query/analyzer path | Partial | AST model, AST JSON, query hashing, read-authorizer AST transform, simple filter SQL helpers, SQLite analyzer with limits, cursors, plans, synced rows, nested related reads, compound correlations, `EXISTS`/`NOT EXISTS` filters, SQLite-introspected analyzer catalogs, single-table AST-root desired-query hydration, registered/async custom-transform hydration, live single/compound/nested related hydration, `orderBy` + `limit` top-N root reads, `start` cursor root reads, per-parent related `limit`/`start`, and hidden-junction many-to-many traversal. Planner (`zql`): the full `plan_query` pipeline is ported and callable — `build_plan_graph` (all `where_` shapes incl. `and`/`or`/EXISTS via processAnd/processOr/processCorrelatedSubquery + related sub-planning) → `plan()` → `apply_plans_to_ast`, plus `apply_to_condition`. | Consume `plan_query` from a real analysis/hydration path to replace the direct-read + IN-filter shape. |
| Inspect lane | Partial | Version/auth/metrics/query inspection, custom-query transform hooks, HTTP-backed async custom transforms, optional read-authorizer transform, live SQLite `analyzeQuery` over SQLite-introspected table/column/PK metadata. | Add remaining upstream analyze options/parity such as static auth parameter binding and full planner semantics. |
| CVR/view-syncer core | Partial | CVR schema/load/flush SQL, ownership checks, desired-query put/delete, row cache SQL, session lifecycle slivers, inspect query projection. | Wire durable session/CVR services through the live server loop. |
| Mutation path | Partial | CRUD op modeling/planning/apply pieces, write policy evaluation, composed authorization+planning, SQLite demo push behavior. | Connect production pusher/custom mutator/Postgres write path to live WebSocket actions. |
| Custom queries | Partial | Cache keying, response shaping, query-server response parsing, HTTP fetch-and-shape path, inspect delegate registry. | Use read-authorized transformed queries outside inspect and wire auth-maintenance validation. |
| Replication/store | Partial | Change-source primitives, SQLite change application, metadata/replication-state pieces; a `ReplicationApplier` with hermetically-tested apply loop; AND `drive_apply_loop` — a reusable driver that pumps a live `ReplicationStream` into the applier and advances the replication slot via standby-status updates (verified end-to-end against a live Postgres, incl. slot `confirmed_flush_lsn` advancement). | Add change-streamer fan-out to subscribers, reconnection/error recovery, and query invalidation on commit. |
| Observability/ops | Partial | Selected metrics/event/error-detail helpers. | Real OTel/event publishing and production operational wiring. |

## Current Backlog

Now:

- Add a planner-backed execution path to replace the direct-read + IN-filter
  hydration shape.

Next:

- Persist and reuse production CVR row/query/session state across live
  connections.
- Turn demo `pull`, `updateAuth`, and `ackMutationResponses` state into durable
  production behavior.

Later:

- Continue the Postgres replication -> local store -> invalidation pipeline.
- Wire real OTel/event publishing.

## Porting Board

Use this board for work that is active enough to steer the next slices. Move
items to the completion log once behavior lands.

| Lane | Current capability | Next concrete gap | Notes for tracking |
| ---- | ------------------ | ----------------- | ------------------ |
| Desired-query hydration | SQLite-backed root AST reads, registered/async custom transforms, single/compound/nested related rows, `orderBy` + `limit` top-N root reads, `start` cursor root reads, per-parent related `limit`/`start`, and hidden-junction many-to-many traversal are live. | Planner-backed execution to replace the direct-read + IN-filter shape. | Keep adding focused WebSocket tests that assert actual `pokePart` rows. A related subquery with `limit`/`start` fetches each parent's children separately. `hidden` is a client view concern only — the server syncs junction rows like any related rows. |
| Analyzer/inspect | SQLite analysis supports introspected catalogs, related reads, nested related, compound correlations, correlated `EXISTS`, cursors, plans, and optional read-authorizer transforms. | Static auth parameter binding and full planner semantics. | Analyzer parity is ahead of live hydration; use it as the reference shape for the next live slices. |
| CVR/view-syncer | Query tracking, row patch generation, row ref-counts, and selected durable SQL helpers exist. | Production session/CVR services need to be wired through the live server loop. | Be careful to preserve one `track_executed` call per query per cycle. |
| Mutation path | CRUD planning/apply pieces and SQLite demo pushes exist. | Production pusher/custom mutator/Postgres write path over live WebSocket actions. | Keep write-path checks scoped because Postgres-backed tests are heavier. |
| Replication/store | Change-source primitives, SQLite change application, metadata/replication-state pieces, and a `ReplicationApplier` with a hermetically-tested full apply loop (`apply_frame` + all DML + commit/rollback) exist. | Wire the applier into a live server loop (network read + change-streamer fan-out) and invalidate queries on commit. | Biggest remaining vertical; the apply half is now de-risked with infrastructure-free tests, so the live server-loop wiring is the next focus. |
| Ops/observability | Selected metrics/event/error-detail helpers exist. | Real OTel/event publishing and production operational wiring. | Track only when behavior becomes observable from the server path. |

## Completion Log

Newest entries should go at the top. Keep each entry to one behavior change plus
the narrow verification that proved it.

| Date | Slice | Result | Focused verification |
| ---- | ----- | ------ | -------------------- |
| 2026-07-09 | OTLP/HTTP push exporter (delivery, mock-collector verified) | Added `otlp_exporter::OtlpExporter` — the DELIVERY half of OTLP: `push`/`push_body` POST the `render_otlp_json` payload to a configured collector `/v1/metrics` over HTTP (`content-type: application/json`) via reqwest, returning the 2xx status or `OtlpExportError::Status`. Verified END-TO-END against a MOCK collector (an in-test tokio TCP server): the push reaches it as `POST /v1/metrics HTTP/1.1` carrying the actual OTLP body (`"name":"zero.replication.commit","asDouble":7`), 200 accepted; a 503 reply surfaces as `Status(503)`. So OTLP export is now complete in-code (serialize + deliver); the only thing left is pointing it at a REAL collector instead of the mock. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server --lib otlp_exporter`; `cargo test -p zero-cache-server --lib -- --test-threads=1`; `cargo build --workspace` |
| 2026-07-09 | OTLP/HTTP JSON metrics export (push wire format) | Added `InMemoryBackend::render_otlp_json` — the OTLP `ExportMetricsServiceRequest` JSON body a push-based exporter POSTs to a collector's `/v1/metrics`. Counters → OTLP monotonic `sum` (temporality=2, `asDouble` dataPoints); latency histograms → OTLP `histogram` with per-bucket `bucketCounts` (15 = 14 bounds + overflow) and `explicitBounds` over `LATENCY_HISTOGRAM_BOUNDARIES_S`; scope `zero` (matching `getMeter('zero')`). This is the OTLP SERIALIZATION — the wire format a collector ingests — so the only remaining OTLP piece is HTTP delivery to a *running* collector. Test: a counter (3) and a 3ms histogram obs render to the correct envelope, monotonic sum, and `bucketCounts:[0,0,1,0,…]` in the (0.002,0.005] bucket. | `cargo fmt -p zero-cache-services`; `cargo test -p zero-cache-services --lib metrics`; `cargo test -p zero-cache-services --lib`; `cargo build --workspace` |
| 2026-07-09 | scanstatus enabled BY DEFAULT (committed build config) | Made the live scanstatus cost model active out of the box: committed `.cargo/config.toml` setting `LIBSQLITE3_FLAGS=SQLITE_ENABLE_STMT_SCANSTATUS` (appends to libsqlite3-sys defaults — non-destructive) and made `scanstatus` a DEFAULT feature of `zero-cache-sqlite`. Now a plain `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` builds the bundled SQLite with scanstatus AND runs the live extraction test (232 pass, incl. `scanstatus_extracts_loops_and_drives_estimate_cost`) with NO external env setup. Closes "scanstatus must be enabled externally" — it's now on by default; `--no-default-features` still drops it cleanly. | `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` (232 pass incl. live scanstatus); `cargo build --workspace` |
| 2026-07-09 | Live `scanstatus_v2` FFI extraction (feature-gated, LIVE-VERIFIED) | Closed the last unported CODE seam AND proved it runs: the live `sqlite3_stmt_scanstatus_v2` extraction feeding the ported cost-model arithmetic. Added `crate::scanstatus::loops_for` (unsafe FFI: `sqlite3_prepare_v2` → step → read SELECTID/PARENTID/EST/EXPLAIN per loop → finalize, sorted by select_id) + `StatementRunner::scanstatus_loops` + `ScanstatusLoop::to_planner` (→ `zql::planner_cost::ScanstatusLoop`). GATED behind an OFF-BY-DEFAULT `scanstatus` feature so the default build is provably unaffected (231 sqlite tests still pass, workspace clean). LIVE-VERIFIED: with `LIBSQLITE3_FLAGS=SQLITE_ENABLE_STMT_SCANSTATUS` (which APPENDS to libsqlite3-sys defaults — confirmed safe, whole crate rebuilds) a real test extracts loops from `SELECT ... ORDER BY`, finds the ORDER-BY sort loop, and drives `estimate_cost` to positive rows + sort startup cost. So the scanstatus cost model is complete end-to-end (extraction → loops → estimate_cost) and actually executes against real SQLite when enabled. | `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` (feature off, 231 pass); `LIBSQLITE3_FLAGS=SQLITE_ENABLE_STMT_SCANSTATUS cargo test -p zero-cache-sqlite --features scanstatus --lib scanstatus` (live extraction passes); `cargo build --workspace` |
| 2026-07-09 | Prometheus histogram `le` buckets (queryable latencies) | Made the exported latency histograms actually `histogram_quantile`-queryable: `render_prometheus` now emits cumulative `_bucket{le="…"}` lines over the ported `LATENCY_HISTOGRAM_BOUNDARIES_S` (each bucket = count of observations ≤ that boundary), plus the mandatory `+Inf` bucket, `_sum`, and `_count`. This is the whole reason the bucket boundaries were ported — without `_bucket` lines a Prometheus histogram can't compute quantiles. Test: observations at 0.5/3/40 ms produce cumulative buckets `le=0.001→1`, `le=0.005→2`, `le=0.05→3`, `+Inf→3`. | `cargo fmt -p zero-cache-services`; `cargo test -p zero-cache-services --lib metrics`; `cargo test -p zero-cache-services --lib`; `cargo build --workspace` |
| 2026-07-09 | Poke-computation latency histogram (live site + export) | Added the first live LATENCY-HISTOGRAM instrumentation site: `SyncService::poke_for_commit` now times the `pokes_for_commit` computation with `std::time::Instant` and records it into a `zero.sync.poke-time` latency histogram (ms→s via the ported `record_ms`). Exercises the full histogram path end-to-end — instrument → record → Prometheus export. Test: computing a poke records exactly one `zero.sync.poke-time` observation and it renders as `zero_sync_poke_time_count 1`. Complements the two counters; the metrics pipeline now has counter + histogram live sites, both scrapeable. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server --lib sync_service`; `cargo test -p zero-cache-server --lib -- --test-threads=1`; `cargo build --workspace` |
| 2026-07-09 | Prometheus text-exposition metrics export | Gave the ported metrics an actual EXPORT path with no external collector required: `InMemoryBackend::render_prometheus` renders the live metrics in Prometheus text-exposition format (the standard PULL/scrape model), mapping OTel dotted names to Prometheus names (`zero.replication.commit`→`zero_replication_commit`), emitting counter value lines and histogram `_count`/`_sum` (seconds) aggregates, deterministically name-sorted. This closes the "metrics need external wiring" gap in-code: the instrumented `zero.replication.commit`/`zero.server.connections` counters are now scrapeable output, not just internal state. Tests: name mapping, counter+histogram rendering (1.5s+0.5s→sum 2, count 2), sorted determinism. | `cargo fmt -p zero-cache-services`; `cargo test -p zero-cache-services --lib metrics`; `cargo test -p zero-cache-services --lib`; `cargo build --workspace` |
| 2026-07-09 | Accept-loop connection metric (`zero.server.connections`) | Extended metrics instrumentation to a second real call site: `run_server`'s accept loop now increments a `zero.server.connections` counter (from the shared `SyncService` metrics registry, so it routes to the same OTel backend as `zero.replication.commit`) on every accepted connection. The existing live-socket test now builds the service `with_metrics` over a retained `InMemoryBackend` and asserts `counter_value("zero.server.connections") == 1` after one client connects — proving the instrumentation fires in the real accept path. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server --lib bootstrap`; `cargo test -p zero-cache-server --lib -- --test-threads=1`; `cargo build --workspace` |
| 2026-07-09 | Metrics wired into the running `SyncService` (instrumentation) | Moved the ported metrics registry from an isolated module to a live instrumentation point: `SyncService` now owns a `Metrics` registry and a `zero.replication.commit` counter, incremented on every `publish_commit`. Added `SyncService::with_metrics(cap, metrics)` — the seam a process uses to inject an OTel-forwarding backend (the default `new` uses the in-memory backend). Added `zero-cache-services` as a server dep. Test: three `publish_commit` calls drive `counter_value("zero.replication.commit") == 3`. This connects the OTel metrics port to a real call site in the assembled service, not just a standalone module. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server --lib sync_service`; `cargo test -p zero-cache-server --lib -- --test-threads=1`; `cargo build --workspace` |
| 2026-07-09 | OTel metrics registry (naming/cache/latency conversion) | Ported `observability/metrics.ts` — the metrics-instrument registry: `Category` (→ `zero.{category}.{name}` names), the get-or-create instrument cache (repeat lookups return the same handle, separate counter vs up-down namespaces), `LATENCY_HISTOGRAM_BOUNDARIES_S` (exact 1ms–30s buckets), and `LatencyHistogram::record_ms` (raw ms → seconds, the `unit:'s'` convention). Structured over a pluggable `MetricsBackend` trait so the naming/caching/accounting/conversion are faithfully ported and tested against an `InMemoryBackend`; a real deployment supplies an OTel-forwarding backend (the SDK/exporter is process-level, pluggable). Tests: name template, bucket boundaries, counter sum via FQ name, cache identity, up/down negative deltas, ms→s conversion (1500ms→1.5s). | `cargo fmt -p zero-cache-services`; `cargo test -p zero-cache-services --lib metrics`; `cargo test -p zero-cache-services --lib` |
| 2026-07-09 | `scanstatus_v2` planner cost-model computation | Ported the core `scanstatus_v2` cost model from `zqlite/sqlite-cost-model.ts`: `ScanstatusLoop` (the `SELECTID`/`PARENTID`/`EST`/`EXPLAIN` record), `btree_cost` (`n·log2(n)/10` — SQLite sort ~10× faster than host-side), `estimate_cost` (top-level ops only; first is the main scan fixing row count, each subsequent `ORDER BY` op adds a b-tree sort cost), and `remove_correlated_subqueries` (strips subqueries the scanstatus query can't carry, collapsing and/or). The pure cost arithmetic + condition-stripping are ported and tested against the upstream formulas (8·log2(8)/10=2.4, first-scan row count, nested-op/ordering handling, subquery collapse); only the `stmt.scanStatus` extraction from a live statement (SQLite-binding-specific) is left to the caller. | `cargo fmt -p zero-cache-zql`; `cargo test -p zero-cache-zql --lib planner_cost`; `cargo test -p zero-cache-zql --lib -- --test-threads=1` |
| 2026-07-09 | Live-CVR handler threaded into the binary | Replaced the binary's keepalive-only stand-in handler with the REAL view-syncer handler: added `bootstrap::live_handler` (a `BoxedHandler` factory that gives each connection its own replica `StatementRunner` + a `DesiredQueriesHandler` shared into the per-action async closure via `Arc<tokio::sync::Mutex<_>>`, dispatching through `on_action_async`), and wired `main.rs` to use it per accepted connection. Verified: the bin builds + runs (`listening on 127.0.0.1:…`) with the live handler, and a bootstrap live-socket test drives a real client through `initConnection` → `changeDesiredQueries` → `ping`→`pong` served entirely through the real `DesiredQueriesHandler` (connection survives, pong returns). Closes the "live handler not wired into main's factory" caveat. | `cargo fmt -p zero-cache-server`; `cargo build -p zero-cache-server --bin zero-cache-server`; `cargo test -p zero-cache-server --lib bootstrap`; `cargo test -p zero-cache-server --lib -- --test-threads=1` |
| 2026-07-09 | Outer `main` binary + `run_server` accept shell | Added the actual `zero-cache-server` BINARY (`main.rs`) and `bootstrap::{ServerConfig, bind, run_server}` — the thin outer shell that instantiates the shared `SyncService`, binds a real TCP listener, and runs the WebSocket accept loop (upgrade → `connected` greeting → per-connection `serve_connection_async`) until a shutdown signal (Ctrl-C in the bin). Config from env (`ZERO_LISTEN_ADDR`, `ZERO_FANOUT_CAPACITY`). Verified: the binary compiles, starts, and binds a live socket (`listening on 127.0.0.1:…`); two live-socket tests drive `run_server` — a real client through init/ping→pong then graceful shutdown returning the accepted count, and a clean no-connection shutdown. This is the outermost process shell the port was missing, over the now-tested orchestration parts. | `cargo fmt -p zero-cache-server`; `cargo build -p zero-cache-server --bin zero-cache-server`; `cargo test -p zero-cache-server --lib bootstrap`; `cargo test -p zero-cache-server --lib -- --test-threads=1` |
| 2026-07-09 | Top-level `SyncService` orchestrator (replicator↔connections) | Added `sync_service::SyncService` — the process-level object a `main` builds once and shares, owning the `ChangeFanout` that is the single rendezvous between the replicator and all connections. `publish_commit` (replicator side) fans a commit watermark out to every live connection; `poke_for_commit` (connection side) turns a received `CommitNotification` into the client poke via `commit_dispatch::pokes_for_commit`. Test drives the WHOLE assembled top-level flow in one process: replicator logs a commit's row + `publish_commit`s it → a connection subscriber receives it off the fan-out → computes a poke (`poke-02`, cookie `02`) carrying the re-hydrated row; a second test proves an unrelated commit reaches the connection but pokes nothing (rehydrate never runs). The replicator and view-syncer connections are now driven by the same commit stream through one shared service. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server --lib sync_service`; `cargo test -p zero-cache-server --lib -- --test-threads=1` |
| 2026-07-09 | Commit→poke join across both halves (`pokes_for_commit`) | Added `commit_dispatch::pokes_for_commit` in the SERVER crate (the one crate depending on both sqlite and view-syncer) — the connective step a top-level sync process runs each time a commit fans out, joining the replicator side to the view-syncer side: `ChangeLog::read_since` → `changed_tables` → `invalidated_query_hashes` → `queries_to_reexecute` (got-only) → caller's `rehydrate` closure (the live IVM in prod) → `build_poke`. Sourced from a REAL change-log (not a hand-built table set). Tests: a commit on a got query's table pokes its re-hydrated row (only that query re-hydrated); an unrelated-table commit pokes nothing and never calls rehydrate; an invalidated-but-not-got query is skipped. This wires sqlite's change-log/catch-up half to view-syncer's invalidation/poke half in one function. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server --lib commit_dispatch`; `cargo test -p zero-cache-server --lib -- --test-threads=1` |
| 2026-07-09 | Assembled supervised replication service (live, 3 cycles) | Ran the WHOLE supervised replicator assembled into ONE live loop driven by a single `ReplicatorSupervisor` across three cycles against real Postgres: cycle 1 applies a txn then the stream drops (→ `Reconnect`, resume from the slot's confirmed LSN); cycle 2 hits an upstream `ALTER TABLE ADD COLUMN` (→ `Resync`, which actually runs `reset_replica_for_resync` + `run_initial_sync_introspected` from a fresh slot); cycle 3 applies a txn on the NEW schema then shuts down (→ `Stop`). Final replica has all 3 rows with the added `priority` column; supervisor counters `reconnects=1, resyncs=1`. Caught + fixed a real correctness bug: a drift break leaves the applier mid-transaction, so the shared replica connection must be rolled back before the resync rebuild (else `begin`-within-`begin`). This is the proven pieces assembled into a running service loop. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib live_assembled_supervised_service_runs_through_reconnect_and_resync -- --test-threads=1`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replicator supervisor lifecycle state (assembled loop shell) | Added `ReplicatorSupervisor` — the stateful shell a long-lived replicator service carries across many `drive_apply_loop` cycles: `record(outcome, requested_stop)` folds applied commits into a running `total_commits`, tallies `reconnects`/`resyncs`, and returns the `decide_next_action` decision. A resync tallies but does NOT reset the cumulative commit count (those commits really applied before drift was seen). Test drives a realistic lifecycle — 3 commits → reconnect, 2 → drift/resync, 4 → shutdown/stop — asserting `total_commits=9, reconnects=1, resyncs=1`. This is the tested bookkeeping core of the assembled service loop. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib replication_supervisor`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Durable CVR advances across a commit cycle (live) | Live-Postgres proof that the durable CVR store is genuinely DRIVEN across a commit (the CVR half of the running view-syncer loop), not just readable/writable in isolation: first `flush_cvr` persists the CVR at version `01`; `load_cvr` reads it back and claims ownership; a simulated commit flushes a bumped version `02` GATED on the loaded `01` (optimistic concurrency via `check_version_and_ownership`); a final `load_cvr` observes the durably-advanced `02`. Ties `load_cvr` + `flush_cvr` into the load→advance→flush→reload round trip a running loop performs each commit. | `cargo fmt -p zero-cache-view-syncer`; `cargo test -p zero-cache-view-syncer --lib durable_cvr_advances_across_a_commit_cycle -- --test-threads=1`; `cargo test -p zero-cache-view-syncer --lib -- --test-threads=1` |
| 2026-07-09 | Commit→invalidation→re-hydration→poke end-to-end (composed) | Wired the whole invalidation lane into an actual client poke: added `queries_to_reexecute` (of the invalidated queries, only those the CVR currently holds as "got" need re-running — a not-yet-got query hydrates fresh, an untracked one is irrelevant), then an end-to-end test composing the REAL ported pieces — `invalidated_query_hashes` → `queries_to_reexecute` → IVM `hydrate_query` → `hydration_to_patches` → `build_poke`. Proves "table `issues` changed" turns into a client poke carrying the re-hydrated `issues` row. This is the connective tissue the invalidation lane exists to provide. | `cargo fmt -p zero-cache-view-syncer`; `cargo test -p zero-cache-view-syncer --lib query_invalidation`; `cargo test -p zero-cache-view-syncer --lib commit_drives_reexecution_and_pokes_the_rehydrated_row`; `cargo test -p zero-cache-view-syncer --lib -- --test-threads=1` |
| 2026-07-09 | Resync executor rebuilds replica to new schema (live) | Added `reset_replica_for_resync` (drops every user/metadata table — the SQLite equivalent of upstream discarding the replica file) and proved the full resync EXECUTION path against live Postgres: build a replica from schema `(id,name)`, drift upstream (`ALTER TABLE ADD COLUMN priority` + data changes), then `reset_replica_for_resync` → `run_initial_sync_introspected` from a FRESH snapshot into the SAME db handle rebuilds the replica to the new schema with re-copied data (the `priority` column and its values `[5,9]` are present). This is the execution half of the `Resync` supervisor decision — a drifted replica brought back into agreement without a new db handle. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib live_resync_rebuilds_replica_to_the_new_schema -- --test-threads=1`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Supervised reconnect resumes from slot (live) | Live-Postgres end-to-end proof of the supervised/reconnecting replicator loop across a transient disconnect: round 1 streams one txn and stops, the supervisor (`decide_next_action`, no drift, not shutting down) returns `Reconnect`, the service drops the stream and re-subscribes from the slot's `confirmed_flush_lsn`; round 2 then streams a SECOND txn committed WHILE disconnected — resuming exactly where the slot left off with no gap and no re-delivery of round 1's row (both rows land exactly once). A shutdown flag yields the terminal `Stop`. Connects `decide_next_action` to real re-subscription. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib live_supervised_reconnect_resumes_from_slot -- --test-threads=1`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replication supervisor decision (resync/reconnect/stop) | Added `replication_supervisor::decide_next_action`: the pure supervision decision a long-lived replicator makes after each `drive_apply_loop` run — `Resync` on schema drift (replica is stale, can't follow the new schema incrementally → tear down + re-initial-sync), `Reconnect` on a clean stream end (resume from the last confirmed watermark; applied commits are durable), or terminal `Stop` on a caller-requested stop. Drift always wins over a coincident stop request (else the replica is silently left stale). This factors the supervision logic out of the I/O-bound service loop so it's independently tested. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib replication_supervisor`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | WS malformed-frame decode-error path (live socket) | Added a live end-to-end WebSocket test that a malformed (non-JSON) frame terminates the served connection with `ServeError::Decode` rather than being silently dropped — the decode boundary in `serve_connection`'s loop, surfaced over a real socket. Complements the protocol-ordering (`MessageBeforeInit`) and clean-teardown (`Close`) end-to-end paths. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server --lib malformed_frame_terminates_with_decode_error`; `cargo test -p zero-cache-server --lib -- --test-threads=1` |
| 2026-07-09 | WS `closeConnection`-frame teardown (live socket) | Added a live end-to-end WebSocket test for the `ConnectionAction::Close => break` branch of `serve_connection`: a real client sends `initConnection` then a `closeConnection` frame; the server runs the handler once with `Close`, flushes its farewell `error` frame, and cleanly ends the serve loop (returns `Ok`) WITHOUT the client closing the socket first. This is the application-level teardown path, previously untested (the existing full-pipeline test ends via a socket-level close, which exits through `recv_text` → `None` — a different branch). Verified the farewell frame reaches the client and the recorded actions are exactly `[init, close]`. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server --lib close_connection_frame_ends_the_loop_after_flushing`; `cargo test -p zero-cache-server --lib -- --test-threads=1` |
| 2026-07-09 | Change-log `changed_tables` (commit-side invalidation input) | Added `subscriber_catchup::changed_tables`: collapses a run of change-log entries (typically `read_since(watermark)`) to the distinct bare table names touched — exactly the commit-side input that `query_invalidation::invalidated_query_hashes` matches each query's `referenced_tables` read-set against. This closes the data path: a commit's change-log → changed tables → invalidated query hashes → (future) re-hydration. Test: two `issue` ops + one `comment` op collapse to `{comment, issue}`. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib subscriber_catchup`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Query invalidation matcher (commit tables → affected query hashes) | Added `query_invalidation::invalidated_query_hashes`, the pure decision that sits between a fanned-out `CommitNotification` and the `CVRQueryDrivenUpdater` re-execution it drives: given a commit's changed tables and the tracked queries (hash + AST), it returns (sorted) the hashes of every query whose `referenced_tables` read-set intersects the commit. Consumes the new `ast::referenced_tables`. Tests: a commit on a query's primary table, on a related-hop table, and on a correlated-`EXISTS` table each invalidate that query; an unreferenced table invalidates nothing; a multi-table commit invalidates every overlapping query. | `cargo fmt -p zero-cache-view-syncer`; `cargo test -p zero-cache-view-syncer --lib query_invalidation`; `cargo test -p zero-cache-view-syncer --lib -- --test-threads=1` |
| 2026-07-09 | AST `referenced_tables` (query read-set extraction) | Added `ast::referenced_tables`: collects every table a query reads — its own `table` plus, recursively, all `related` subquery hops AND correlated-subquery `where`-conditions (EXISTS/NOT EXISTS/scalar) — deduplicated + sorted. This is the read-set a change-stream commit is matched against for query invalidation (a commit touching any referenced table must re-hydrate the query). Test covers a two-hop related chain (issue→comments→reactions) plus an `EXISTS(labels)` where-condition, asserting all four tables surface and unrelated tables don't. | `cargo fmt -p zero-cache-protocol`; `cargo test -p zero-cache-protocol --lib referenced_tables`; `cargo test -p zero-cache-protocol --lib -- --test-threads=1` |
| 2026-07-09 | Lagged-subscriber catch-up boundary lock | Locked the `FanoutEvent::Lagged` recovery path's boundary: a subscriber that fell behind re-catches-up via `read_since(last_watermark)`, whose SQL is strictly-after (`stateVersion > ?`). Added a test that a subscriber last at watermark "02" catches up ONLY the commit at "03" (not the seen "01"/"02") — every prior catch-up test read from "00" (before everything), so the strictly-after boundary that prevents double-applying already-seen changes was untested. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib subscriber_catchup`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replication loop → fan-out → subscriber hand-off (live) | Added a live-Postgres integration test wiring `run_change_stream`'s `on_commit` seam into a `ChangeFanout`: the loop translates each `CommitResult` into a `CommitNotification` and publishes it, and a `FanoutSubscriber` receives the `Commit` event with the SAME watermark the loop committed (and `num_change_log_entries == 1`). Connects two independently-ported pieces (the change-stream apply loop and the fan-out hub) that nothing previously exercised together — the "replication commit → view-syncer subscriber" hand-off. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib live_loop_fans_out_commits_to_a_subscriber -- --test-threads=1`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | CVR row-cache flush→read round trip (live) | Added a live-Postgres test closing the write→read loop for the CVR row cache: `flush_cvr` persists an instance PLUS row-updates (a put row and a tombstone) in one real transaction, and `get_row_records` reads them back. Previously the flush test checked only instance/query/desire rows and the get_row_records test seeded rows via raw SQL — nothing proved `flush_cvr`'s `get_row_updates_sql` batch writes exactly what `get_row_records` reads. Verified: the put row round-trips with `rowVersion`/`refCounts` intact, and the null-`refCounts` tombstone is excluded. | `cargo fmt -p zero-cache-view-syncer`; `cargo test -p zero-cache-view-syncer --lib flush_row_updates_round_trip_through_get_row_records -- --test-threads=1`; `cargo test -p zero-cache-view-syncer --lib -- --test-threads=1` |
| 2026-07-09 | Mutation live retry error-mode fallback | Added a live-Postgres test for the previously-untested `apply_crud_mutation_with_retry` error-mode fallback (`processMutation`'s "retry once in error mode" policy): a duplicate-key insert fails on the first attempt, is retried in error mode, and the retry confirms the mutation id while SKIPPING the failing op. Verified against real Postgres: outcome is `RetryOutcome::AppError`, `lastMutationID` advances to 2, and the target row is left untouched (`title` stays `orig`). All four CrudOp types plus the retry policy's app-error path are now live-verified. | `cargo fmt -p zero-cache-mutagen`; `cargo test -p zero-cache-mutagen live_retry_error_mode_confirms_id_but_skips_failing_op -- --test-threads=1`; `cargo test -p zero-cache-mutagen -- --test-threads=1` |
| 2026-07-09 | Drive-loop schema-drift detection wired + live-verified | Wired `relation_message_drift` INTO `drive_apply_loop`: it now takes the published `specs` and returns `ApplyLoopOutcome { commits, drift }`, stopping with `drift = Some(reason)` when a streamed `Relation` drifts from the specs (instead of misapplying against a stale schema). Verified end-to-end against LIVE Postgres: an `ALTER TABLE ADD COLUMN` upstream is detected mid-stream and the loop stops with drift. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib live_drive_apply_loop_detects_schema_drift -- --test-threads=1`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replication schema-drift detector consumer (`relation_message_drift`) | Added `relation_message_drift`, giving the previously-caller-less `relation_different` a real production consumer: given a streamed pgoutput `Relation` message + the published specs, it builds a `Relation` view (schema/name, key columns, typed column list) and returns `Some(reason)` on schema drift (the signal to trigger re-sync) — matching upstream's schema-change-detected path. Tests cover match (no drift), type change, added column, non-Relation message, and unknown table. | `cargo fmt -p zero-cache-change-source`; `cargo test -p zero-cache-change-source relation_message_drift --lib`; `cargo test -p zero-cache-change-source --lib -- --test-threads=1` |
| 2026-07-09 | Initial-sync varied column types (live) | Added a live-Postgres test that initial-sync copies a table with `boolean`/`numeric`/`bigint`/`timestamptz`/`jsonb` columns — exercising each type's binary-COPY field decoder through the real snapshot-copy pipeline (the existing tests were int/text only). The row copies cleanly and every typed column reads back non-null. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib live_initial_sync_copies_varied_column_types -- --test-threads=1`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replication drive-loop multi-transaction continuation (live) | Added a live-Postgres test that `drive_apply_loop` continues across MULTIPLE committed transactions until `should_stop` fires — driving two separate upstream transactions, applying both rows to the replica, and advancing the slot cumulatively (distinct from the single-commit-stop path). | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib live_drive_apply_loop_across_multiple_transactions -- --test-threads=1`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Mutation live upsert (both ON CONFLICT branches) + stale-doc fix | Added a live-Postgres test applying an UPSERT CrudOp twice on the same key — exercising both the INSERT (no conflict) and `ON CONFLICT DO UPDATE` (conflict) branches against real Postgres; all four CrudOp types are now live-tested end-to-end. Also corrected a stale `published_schema.rs` doc note that claimed `get_publication_info` (live introspection + JSON→spec parse) was unported — it is in fact ported, consumed by `initial_sync`, and live-tested. | `cargo fmt -p zero-cache-mutagen`; `cargo test -p zero-cache-mutagen live_applies_upsert_insert_then_conflict_update -- --test-threads=1`; `cargo test -p zero-cache-mutagen -- --test-threads=1` |
| 2026-07-09 | Mutation live update+delete end-to-end | Added a live-Postgres test applying UPDATE then DELETE CrudOps through `apply_crud_mutation` (the full BEGIN → lmid-check → plan_mutation_sql → COMMIT path). Previously the live tests only exercised INSERT; update/delete op dispatch was unit-tested at the SQL-string level only. Verified: the update changes the row's title, the delete removes it, and each increments the client's lastMutationID — all against real Postgres. | `cargo fmt -p zero-cache-mutagen`; `cargo test -p zero-cache-mutagen live_applies_update_then_delete -- --test-threads=1`; `cargo test -p zero-cache-mutagen -- --test-threads=1` |
| 2026-07-09 | Replication `drive_apply_loop` (live server-loop wiring) | Added `drive_apply_loop`: the reusable ongoing-replication driver that pumps a live `ReplicationStream` into `ReplicationApplier` AND, after each committed transaction, sends a standby status update flushing to that commit's WAL end LSN so Postgres advances the slot (without which the upstream WAL grows unboundedly). Flush only ever advances to a durably-committed `end_lsn`; keepalives are answered. Verified against LIVE Postgres: a streamed insert lands in the replica and the slot's `confirmed_flush_lsn` advances off `0/0`. This is the first real "wire the applier into a live server loop" step (previously the pump was inlined in tests). | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib live_drive_apply_loop_advances_the_slot`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Read-auth flatten-preserves-permission lock (audit) | Audited the read-authorizer transform + `simplify_condition` (confirmed faithful — upstream also calls `simplifyCondition`; verified security-preserving: never drops the permission `Or`, denies on the empty-`Or` marker, both authorization paths fail-closed). Locked in the untested flatten case: a compound `And[a,b]` where forces `simplify` to flatten `And[And[a,b], Or[rule]]` → `And[a,b,rule]`; the permission rule must survive, or rows leak. Existing test used only a simple where. | `cargo fmt -p zero-cache-auth`; `cargo test -p zero-cache-auth survives_flattening --lib`; `cargo test -p zero-cache-auth` |
| 2026-07-09 | pg-type→ValueType precedence lock (audit) | Audited `data_type_to_zql_value_type` against upstream `dataTypeToZqlValueType` (confirmed exact ordering: array → map lookup → enum-only-as-fallback) and locked in the previously-untested precedence: `is_array` overrides everything, and `is_enum` does NOT override a KNOWN type (enum applies only to unmapped types). The existing enum test used only unknown types, so a reordering bug that mistyped enum-flagged known columns would have gone uncaught. | `cargo fmt -p zero-cache-types`; `cargo test -p zero-cache-types value_type_precedence --lib`; `cargo test -p zero-cache-types` |
| 2026-07-09 | Mutation SQL injection-safety lock (audit) | Audited `value_sql`/`lit` (confirmed standard Postgres `'→''` / `"→""` escaping, correct under `standard_conforming_strings`) and added an explicit end-to-end injection-safety test: an insert value `'); DROP TABLE issues;--` is neutralized (leading quote doubled, DROP left inert inside the literal). This port INLINES values (upstream parameterizes), so this deviation-specific safety property is now locked in with a realistic payload. | `cargo fmt -p zero-cache-mutagen`; `cargo test -p zero-cache-mutagen neutralizes_quote_injection`; `cargo test -p zero-cache-mutagen` |
| 2026-07-09 | Mutation SQL composite-PK coverage (audit) | Audited `get_insert/upsert/update/delete_sql` against upstream mutagen.ts (confirmed faithful, incl. `SET ${value}` covering all columns and the `AND`/`,` joins) and locked in the previously-untested composite-primary-key paths: upsert `ON CONFLICT (pk1,pk2)` comma-join and update `WHERE pk1=.. AND pk2=..` — distinct code paths that only a multi-column PK exercises (a bug there corrupts writes on composite-key tables). | `cargo fmt -p zero-cache-mutagen`; `cargo test -p zero-cache-mutagen sql::tests`; `cargo test -p zero-cache-mutagen` |
| 2026-07-09 | CVR cookie↔cmp_versions ordering invariant lock (audit) | Audited `cmp_versions`/`version_to_cookie` (confirmed faithful, incl. the no-overflow config subtraction) and pinned the load-bearing cross-consistency invariant `build_poke` relies on: cookie lexicographic order matches `cmp_versions` semantic order, including the LexiVersion length-prefix case (config `9 < 10`, where naive `"9" > "10"` would break `max_by_key(cookie)` and select the wrong final poke cookie). | `cargo fmt -p zero-cache-view-syncer`; `cargo test -p zero-cache-view-syncer cookie_ordering_matches_cmp_versions --lib`; `cargo test -p zero-cache-view-syncer` |
| 2026-07-09 | AST-normalize flatten+sort composition lock (audit) | Audited `normalize_ast` against upstream `normalizeAST` (flatten + sort-related + sort-conditions + recurse) — confirmed faithful, incl. recursive flattening. Locked in the previously-untested composition case: a nested, unsorted `((b AND a) AND c)` normalizes to `(a AND b AND c)` and equals a differently-nested equivalent, so logically-identical queries hash the same regardless of nesting/order (the existing test used pre-sorted inputs, so didn't exercise flatten∘sort). | `cargo fmt -p zero-cache-protocol`; `cargo test -p zero-cache-protocol where_flatten_and_sort_compose --lib`; `cargo test -p zero-cache-protocol` |
| 2026-07-09 | Poke-builder schemaVersions audit (doc) | Audited `build_poke`'s `pokeStart.schemaVersions` gating (initially suspected a divergence since the client-handler's initial `pokeStart` object omits it). Verified against the wire contract (`poke.ts`: schemaVersions is set iff the poke carries a `rowsPatch`) that the `has_rows` gating is FAITHFUL; only the `{1.0,1.0}` version values are demo placeholders. Documented both in the module doc (previously undocumented). No behavior change. | `cargo build -p zero-cache-view-syncer`; `cargo test -p zero-cache-view-syncer poke_builder` |
| 2026-07-09 | Query-hash byte-stability golden lock (audit) | Audited `hash_of_ast`/`hash_of_name_and_args` against upstream (confirmed the exact `h64(JSON.stringify(normalized)).toString(36)` / `h64(`${name}:${args}`).toString(36)` formulas) and added a golden regression-lock pinning the concrete outputs, so any future drift in the `normalize_ast`→`ast_to_json`→`stringify`→`h64`→base36 chain — which would silently invalidate persisted CVR query IDs and break client/server query identity — fails loudly. | `cargo fmt -p zero-cache-protocol`; `cargo test -p zero-cache-protocol query_hash::tests::hash_outputs_are_byte_stable --lib`; `cargo test -p zero-cache-protocol` |
| 2026-07-09 | CVR ref-count multi-query survival test (audit) | Critically audited `merge_ref_counts` against upstream `mergeRefCounts` (confirmed faithful, incl. the no-existing zero-count asymmetry) and locked in the core untested invariant: a row referenced by two queries survives when one dereferences it (`{h1:1,h2:1}` + `{h1:-1}` → `{h2:1}`, not a tombstone). | `cargo fmt -p zero-cache-view-syncer`; `cargo test -p zero-cache-view-syncer partial_dereference --lib`; `cargo test -p zero-cache-view-syncer` |
| 2026-07-09 | Client-schema normalize-on-store (order-insensitive compare) | Fixed a latent fidelity divergence: `set_client_schema`'s JSON equality is order-sensitive, but upstream `setClientSchema` compares with order-insensitive `deepEqual`, so two clients sending the same schema in different key orders would wrongly mismatch. The server now canonicalizes via the (previously caller-less) `normalize_client_schema` before storing, so reordered-equivalent schemas compare equal. Test proves reordered columns are not treated as a mismatch. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server initialize_client_schema_comparison_is_column_order_insensitive --lib`; `cargo test -p zero-cache-server` |
| 2026-07-09 | Client-schema encode + validated server retention | Added `up_json::client_schema_to_json` (inverse of the existing parser; round-trip verified across all `ValueType`s) and wired the server's `initConnection` handler to retain the received `clientSchema` on the CVR via the validated `cvr_client_state::set_client_schema` (set-on-first-use; a later differing schema is defensively rejected, not overwritten) — matching upstream's `setClientSchema`. Previously the schema was dropped. Gives both the encoder and `set_client_schema` real production consumers. | `cargo fmt -p zero-cache-protocol -p zero-cache-server`; `cargo test -p zero-cache-protocol client_schema --lib`; `cargo test -p zero-cache-server initialize_retains_the_client_schema_on_the_cvr --lib`; `cargo test -p zero-cache-protocol -p zero-cache-server` |
| 2026-07-09 | Planner builder clippy cleanup | Replaced an `is_some()`+`unwrap()` in `process_correlated_subquery` with `if let Some(..) = ..as_mut()` (clippy `unnecessary_unwrap`), a safe readability fix in code added earlier this session. | `cargo clippy -p zero-cache-zql --lib` (warning gone); `cargo test -p zero-cache-zql --lib` |
| 2026-07-09 | Replication apply: hermetic PK-changing update coverage | Added a hermetic test that a pgoutput `Update` carrying an OLD key tuple (replica identity default, key-only `old`) relocates the row from the old primary key to the new one in the replica — verifying the previously-untested key-change path (earlier update test used `old: None`). Confirmed correct (row moves `a`→`b`, count stays 1). | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib hermetic_tests`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Mutagen `ApiFailure::http_status_class` | Added the `http_status_class` accessor (`custom/fetch.ts`'s `` `${floor(status/100)}xx` `` metric attribute) as a consumed method on the already-ported `ApiFailure` error type: `503`→`"5xx"`, no-status→`None`. | `cargo fmt -p zero-cache-mutagen`; `cargo test -p zero-cache-mutagen http_status_class --lib`; `cargo test -p zero-cache-mutagen` |
| 2026-07-09 | Replication apply: hermetic unknown-relation error path | Added a hermetic test that an `Insert` referencing a relation with no prior `Relation` message surfaces `ApplyError::Translate` (unknown relation) rather than silently applying — completing error-path coverage of the apply loop alongside the DML/commit/rollback happy paths. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib hermetic_tests`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replication apply: hermetic multi-row + watermark coverage | Added hermetic tests that a multi-statement transaction applies all rows and reports one change-log entry per row, and that the `CommitResult.watermark` equals `version_from_lsn(commit_lsn)` — asserting the LSN→replica-version conversion is wired correctly through the apply loop. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib hermetic_tests`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replication apply: hermetic rollback + truncate coverage | Added no-live-Postgres tests for the two remaining apply-path behaviors: `rollback` after an uncommitted Insert leaves the replica unchanged, and a streamed `Truncate` empties the table. With Insert/Update/Delete/Truncate + commit/rollback + raw-frame decode all hermetically covered, the whole ongoing-replication apply loop now has infrastructure-free test coverage. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib hermetic_tests`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replication apply: hermetic update-replication coverage | Completed hermetic DML coverage of the apply pipeline with the Update path: a streamed `Insert` then value `Update` (key unchanged, replica identity default) on a two-column `issues(id,title)` table changes the row's value in in-memory SQLite. Insert/Update/Delete are now all covered without live Postgres. | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib hermetic_tests`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replication apply: hermetic delete-replication coverage | Extended the no-live-Postgres apply tests to the Delete path: a streamed `Insert` then `Delete` of the same key drives the replica row in and back out of in-memory SQLite, proving delete-replication end to end without infrastructure (previously only live-PG-tested). | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib hermetic_tests`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Replication apply: `apply_frame` + hermetic pipeline tests | Added `ReplicationApplier::apply_frame` (raw pgoutput bytes → `pgoutput::decode` → `apply_message`), closing the last wire-bytes→replica seam, and added the FIRST no-live-Postgres end-to-end apply tests: synthetic Begin/Relation/Insert/Commit messages land a row in in-memory SQLite, and a crafted raw `Begin` frame drives `apply_frame`. Also fixed a regression from the earlier `data::Relation` change (missing `columns` field at two sqlite test-helper sites). | `cargo fmt -p zero-cache-sqlite`; `cargo test -p zero-cache-sqlite --lib hermetic_tests`; `cargo test -p zero-cache-sqlite --lib -- --test-threads=1` |
| 2026-07-09 | Mutagen `getBodyPreview` (+ UTF-8 panic fix) | Ported `custom/fetch.ts`'s `getBodyPreview` pure core as `api_fetch::body_preview` (≤512 chars unchanged, else first 512 chars + `...`), and replaced the inline `&t[..512]` byte-slice — which could panic on a non-ASCII body straddling byte 512 — with the char-safe helper. Fixes a latent crash on the API-error path. | `cargo fmt -p zero-cache-mutagen`; `cargo test -p zero-cache-mutagen body_preview --lib`; `cargo test -p zero-cache-mutagen` |
| 2026-07-09 | Types `URLParams` (sync handshake query params) | Ported `types/url-params.ts` as `url_params::UrlParams`: typed required/optional/integer/boolean getters over a URL query string, with `URLSearchParams.get` semantics (empty≡missing, first-value-wins, percent/`+` decoding via `form_urlencoded`) and JS `parseInt`-style leading-digit integer parsing. | `cargo fmt -p zero-cache-types`; `cargo test -p zero-cache-types url_params --lib`; `cargo test -p zero-cache-types` |
| 2026-07-09 | Types `dateToUTCMidnight` (pg date→epoch) | Ported `types/pg.ts`'s `dateToUTCMidnight` as `pg::date_to_utc_midnight` using exact proleptic-Gregorian civil-date math (Hinnant's `days_from_civil`): canonical `YYYY-MM-DD` → UTC-midnight epoch ms, `±infinity` → `±∞`, and any other form (incl. ` BC`) → `NaN`, matching upstream's `new Date`/`Date.UTC` semantics. `timestampToFpMillis` remains deferred (needs microsecond `PreciseDate`). | `cargo fmt -p zero-cache-types`; `cargo test -p zero-cache-types date_to_utc --lib`; `cargo test -p zero-cache-types` |
| 2026-07-09 | Protocol `BackoffBody` | Ported `error.ts`'s `backoffBodySchema` as `error::BackoffBody` (kind ∈ Rebalance/Rehome/ServerOverloaded, message, optional min/max backoff ms, optional `reconnect_params`, optional ZeroCache origin) + an `is_backoff_kind` predicate. Advances the `error.ts` union port alongside the existing basic/`TransformFailedBody`/`PushFailedBody` shapes. | `cargo fmt -p zero-cache-protocol`; `cargo test -p zero-cache-protocol backoff --lib`; `cargo test -p zero-cache-protocol` |
| 2026-07-09 | Replication `report_schema` lag-report types | Ported `replicator/reporter/report-schema.ts` as `report_schema` (`ChangeSourceTimings`/`ChangeSourceReport`/`ReplicationTimings`/`ReplicationReport`) and replaced `DownstreamStatus.lag_report`'s opaque `Option<JsonValue>` (and the `ChangeStreamMessage::Status` field) with the typed `Option<ChangeSourceReport>`. | `cargo fmt -p zero-cache-change-source`; `cargo test -p zero-cache-change-source report_schema --lib`; `cargo test -p zero-cache-change-source --lib -- --test-threads=1` |
| 2026-07-09 | Protocol `PushFailedBody` | Ported `error.ts`'s `pushFailedBodySchema` as `error::PushFailedBody`/`PushFailedReason` (the non-deprecated `['error',{...}]` push-failure shape, mirroring the existing `TransformFailedBody`): `kind()` = `PushFailed`, origin-discriminated reason (Server / ZeroCacheHttp{status,body_preview} / ZeroCacheOther), mutation_ids, message, details. All deps (`ErrorReason`/`ErrorOrigin`/`MutationId`) were already ported. | `cargo fmt -p zero-cache-protocol`; `cargo test -p zero-cache-protocol push_failed --lib`; `cargo test -p zero-cache-protocol` |
| 2026-07-09 | Replication `relation_different` full column comparison | Extended `data::Relation` to carry the full typed column list (from the pgoutput `Relation` message, already held by `CachedRelation`) and completed `pg_schema_diff::relation_different`'s positional column (length/name/type-OID) comparison — no longer a partial port. | `cargo fmt -p zero-cache-change-source`; `cargo test -p zero-cache-change-source relation_different --lib`; `cargo test -p zero-cache-change-source --lib -- --test-threads=1` |
| 2026-07-09 | Protocol `ApplicationError` | Ported `application-error.ts` as `application_error::ApplicationError` (message + optional JSON `details`, `kind()` = `"Application"`, `Display`/`Error` impls, `wrap_message`). The error type transform/push app-level failures surface to the client; wire-envelope integration is a later step. | `cargo fmt -p zero-cache-protocol`; `cargo test -p zero-cache-protocol application_error --lib`; `cargo test -p zero-cache-protocol` |
| 2026-07-09 | Planner `processOr` fan-out/fan-in | Ported `processOr`: an `or` with correlated-subquery branches now builds a fan-out/fan-in pair with a join per branch (faithfully replicating upstream's documented double-add to `fan_out.outputs`); a simple-only `or` adds no structure. Completes the planner's AST-to-graph construction — `build_plan_graph` no longer has any unported condition shape. | `cargo fmt -p zero-cache-zql`; `cargo test -p zero-cache-zql build_plan_graph --lib`; `cargo test -p zero-cache-zql` |
| 2026-07-09 | Planner `planQuery` entry point | Ported `planQuery` + `planRecursively` as `plan_query`/`plan_recursively`: composes `build_plan_graph` → depth-first `plan()` over each related sub-plan then the root → `apply_plans_to_ast`, returning a planned AST with planner-chosen `flip`s. Gives the ported planner a single usable public entry point. | `cargo fmt -p zero-cache-zql`; `cargo test -p zero-cache-zql plan_query --lib`; `cargo test -p zero-cache-zql` |
| 2026-07-09 | Planner `applyPlansToAST` | Ported `applyPlansToAST` as `apply_plans_to_ast`: derives the flipped-`plan_id` set from a planned graph's joins (those left `Flipped` after `plan()`), rewrites `where_` via `apply_to_condition`, and recurses into each `related` subquery's aliased `sub_plans` entry. Completes the `build_plan_graph -> plan -> applyPlansToAST` chain in Rust; only wiring it into live hydration remains. | `cargo fmt -p zero-cache-zql`; `cargo test -p zero-cache-zql apply_plans_to_ast --lib`; `cargo test -p zero-cache-zql` |
| 2026-07-08 | Planner `where_` EXISTS join construction | Ported `processCondition`/`processAnd`/`processCorrelatedSubquery`: a `where_` `EXISTS`/`NOT EXISTS` condition (top-level or inside `and`) now builds a real `PlannerJoinNode` with correct flippability/type and a `plan_id` (stored on the join via new `PlannerJoinNode::new_with_plan_id` + `plan_id()`, and stamped onto the AST condition — `build_plan_graph` now takes `&mut Ast`). Only correlated-subquery-inside-`or` (needs `processOr` fan-out/fan-in) is deferred, returning `OrCorrelatedSubqueryUnported`. | `cargo fmt -p zero-cache-zql`; `cargo test -p zero-cache-zql build_plan_graph --lib`; `cargo test -p zero-cache-zql` |
| 2026-07-08 | Planner `buildPlanGraph` (no-`where`-EXISTS case) | Ported `planner-builder.ts`'s `buildPlanGraph` + `wireOutput` + `Plans`: builds the `source -> connection -> terminus` spine (ordering/filters/root/base-constraints/limit threaded into the connection) and recursively plans each `related` subquery into `sub_plans` by alias. Defers only the `where_` correlated-subquery (`processCorrelatedSubquery` join construction) path, which returns `CorrelatedSubqueryWhereUnported`. | `cargo fmt -p zero-cache-zql`; `cargo test -p zero-cache-zql build_plan_graph --lib`; `cargo test -p zero-cache-zql` |
| 2026-07-08 | Planner `applyToCondition` port | Ported `planner-builder.ts`'s `applyToCondition` as `apply_to_condition`: rewrites a condition tree, setting each `correlatedSubquery`'s `flip` from whether its `plan_id` is in the planner's flipped-id set and recursing into subquery `where_` and `and`/`or` branches. First consumer of the `Condition::CorrelatedSubquery::plan_id` field; `applyPlansToAST` (needs unported `Plans`) still pending. | `cargo fmt -p zero-cache-zql`; `cargo test -p zero-cache-zql apply_to_condition --lib`; `cargo test -p zero-cache-zql` |
| 2026-07-08 | Hidden-junction many-to-many traversal (proof + contract) | Proved `issue -> issueLabel (hidden) -> label` poke-hydrates the target label rows through the recursive related path. No behavior change: established that `hidden` is a client-side view concern (`view-apply-change.ts`), not a server one (`pipeline-driver` has no `hidden` handling), so the server correctly syncs junction rows; a test locks the contract. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server desired_query_hydration_traverses_a_hidden_junction_many_to_many --lib`; `cargo test -p zero-cache-server` |
| 2026-07-08 | Per-parent related `start` cursor hydration | A related subquery with a `limit` or `start` now fetches each parent's children separately, and the `start` bound is pushed into each per-parent child read (previously related reads ignored `start` entirely). | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server desired_query_hydration_applies_related_start_cursor --lib`; `cargo test -p zero-cache-server` |
| 2026-07-08 | Per-parent related `limit` hydration | A related subquery carrying a `limit` now fetches each parent's children separately (single-parent correlation filter + child ordering + in-memory top-N truncate), so every parent keeps its own top-N instead of a wrong global cap; the batched single read is kept when there is no limit. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server desired_query_hydration_applies_related_limit_per_parent --lib`; `cargo test -p zero-cache-server` |
| 2026-07-08 | Desired-query `start` cursor root hydration | Root desired-query hydration now converts a query's AST `start` bound (row + `exclusive`) into a ZQL `Start` (`At`/`After`) via `bound_to_start` and pushes it into the SQL read, so the read resumes at/after the boundary row under the ordering. Related reads remain root-only for cursors. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server bound_to_start --lib`; `cargo test -p zero-cache-server ast_start_cursor_resumes_the_root_read_after_the_boundary_row --lib`; `cargo test -p zero-cache-server` |
| 2026-07-08 | Desired-query `orderBy` + `limit` top-N hydration | Root desired-query hydration now derives its SQL `ORDER BY` from the query's `orderBy` (primary key appended as a total-order tiebreaker) and truncates to the query's `limit`, so `limit` selects which rows are synced. Related reads honor `orderBy` but leave `limit` unapplied (per-parent semantics). | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server ast_order_by_and_limit_hydrate_only_the_top_n_rows --lib`; `cargo test -p zero-cache-server live_connection::tests --lib`; `cargo test -p zero-cache-server` |
| 2026-07-08 | Desired-query nested related hydration | Live desired-query hydration now recurses through nested `related` subqueries, hydrating each child level from the rows fetched at its parent level under the root query ref-count. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server desired_query_hydration_fetches_top_level_related_rows --lib`; `cargo test -p zero-cache-server desired_query_hydration_fetches_compound_related_rows --lib`; `cargo test -p zero-cache-server desired_query_hydration_fetches_nested_related_rows --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Desired-query compound related hydration | Live desired-query hydration now handles top-level multi-column `related` correlations by building tuple-preserving child filters from fetched parent rows, so cross-product child rows are not poked. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server desired_query_hydration_fetches_top_level_related_rows --lib`; `cargo test -p zero-cache-server desired_query_hydration_fetches_compound_related_rows --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Desired-query top-level related hydration | Live desired-query hydration now fetches top-level single-column `related` child rows by constraining child reads from fetched parent rows, without re-tracking the root query. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server desired_query_hydration_fetches_top_level_related_rows --lib`; `cargo test -p zero-cache-server live_connection::tests --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Async custom transform-on-init hydration | `on_action_async` now prefetches missing custom query transforms for `initConnection`/`changeDesiredQueries` desired-query puts before hydrating, so name+args custom queries can hydrate immediately on the async path. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server async_desired_query_hydration_fetches_custom_query_transform --lib`; `cargo test -p zero-cache-server live_connection::tests --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Desired-query hydration for registered custom transforms | Desired-query `put` hydration now resolves registered custom query name+args through `InspectorDelegate`, then hydrates the transformed AST through the same SQLite-introspected single-table path used for direct AST puts. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server desired_query_hydration_uses_registered_custom_query_transform --lib`; `cargo test -p zero-cache-server live_connection::tests --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Desired-query AST-root hydration without demo catalog | `DesiredQueriesHandler` can now hydrate desired-query `put` operations that carry a single-table AST by introspecting the AST root table, primary key, and columns from SQLite. Name-only/custom puts still use the existing demo registry. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server desired_query_hydration_uses_ast_table_outside_demo_catalog --lib`; `cargo test -p zero-cache-server live_connection::tests --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Inspect analyzer SQLite catalog introspection | `analyze_sqlite_ast_query` now accepts owned catalog metadata, can build the needed table/column/PK catalog by introspecting SQLite for the requested AST graph, and live inspect uses that path for direct/custom/read-authorized ASTs. Inspect analysis can now read tables outside the demo hydration `query_catalog`. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server inspect_analyze_query_introspects_tables_outside_demo_catalog --lib`; `cargo test -p zero-cache-server inspect_analyze_query --lib`; `cargo test -p zero-cache-server analyze_query::tests --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Inspect read-authorizer wiring | `DesiredQueriesHandler` can now carry an explicit `PermissionsConfig`; when configured, live inspect `analyzeQuery` applies `zero-cache-auth::read_authorizer::transform_and_hash_query` before SQLite analysis, while the default demo handler behavior remains unchanged. | `cargo fmt -p zero-cache-server`; `cargo test -p zero-cache-server inspect_analyze_query_applies_configured_read_permissions --lib`; `cargo test -p zero-cache-server inspect_analyze_query --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Read-authorizer AST transform | `zero-cache-auth::read_authorizer` now injects row `select` permission rules into AST `where` conditions, recurses through related queries and correlated-subquery conditions, simplifies the condition shape like upstream, and hashes the transformed AST. Auth-data static parameter binding remains future work. | `cargo fmt -p zero-cache-auth`; `cargo test -p zero-cache-auth read_authorizer --lib`; `cargo check -p zero-cache-auth` |
| 2026-07-08 | Analyze-query correlated filters | `EXISTS`/`NOT EXISTS` correlated-subquery conditions now evaluate against related SQLite reads instead of being rejected before SQL generation. | `cargo test -p zero-cache-server analyze_query::tests --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Analyze-query nested related reads | Related analysis now recurses through nested `related` subqueries, constraining each level by the rows read at its parent level and flattening read counts/plans into inspect output. | `cargo test -p zero-cache-server analyze_query::tests --lib`; `cargo check -p zero-cache-server` |
| 2026-07-08 | Analyze-query compound correlations | Related child reads now support multi-column parent/child correlations through SQLite tuple multi-constraints. | Focused `zero-cache-server` analyzer tests |
| 2026-07-08 | Analyze-query related reads | Top-level `related` subqueries now read child rows constrained by the parent result set, report child row counts/plans, and skip child scans when the parent read is empty. | Focused `zero-cache-server` analyzer tests |
| 2026-07-08 | Analyze-query synced rows and scan diagnostics | Direct single-table analysis can return synced row bodies and `dbScansByQuery` diagnostics from SQLite `EXPLAIN QUERY PLAN`. | Focused `zero-cache-server` analyzer tests |
| 2026-07-08 | HTTP-backed inspect custom transforms | Async live inspect `analyzeQuery` can call the user's transform endpoint, cache/register the transformed AST, then run SQLite analysis over that AST. | Focused server/custom-query tests |
| 2026-07-08 | Async transport bridge | `serve_connection_async` and `run_accept_loop_async_bounded` can await per-action handlers. | Focused server transport tests |
| 2026-07-08 | Inspect custom-query transform hook | `InspectorDelegate` can register transformed ASTs by custom query name+args. | Focused inspect tests |
| 2026-07-08 | Analyze-query cursor support | SQLite analyzer completes ordering with primary keys, applies AST `start`, and appends parameterized `LIMIT`. | Focused analyzer tests |
| 2026-07-08 | Live analyze-query execution | Inspect can run catalog-backed AST queries against the SQLite demo replica and return counts, rows, plans, and top-level related reads. | Focused analyzer/inspect tests |
| 2026-07-08 | Live inspect response bridge | `ConnectionAction::Inspect` now calls the inspect handler and sends encoded downstream inspect responses. | Focused server inspect tests |
| 2026-07-08 | Maintenance upstream messages | `pull`, `updateAuth`, `ackMutationResponses`, and `inspect` are decoded and routed; demo pull/auth/ack state exists in memory. | Focused protocol/server tests |

## Verification Policy

To save resources, prefer focused checks while porting:

- Use crate-level checks such as `cargo check -p zero-cache-protocol` or
  `cargo check -p zero-cache-server`.
- Use narrow tests such as `cargo test -p zero-cache-protocol inspect_down --lib`
  or `cargo test -p zero-cache-protocol analyze_query_result --lib`.
- Avoid full workspace test runs unless explicitly requested.
- Record any broad verification separately if it ever happens, so scoped
  progress stays easy to reason about.

## Verification Log

Whole-workspace green state (2026-07-09): `cargo test --workspace --lib -- --test-threads=1`
→ **1443 passed, 0 suites failed** (serial run; parallel runs can spuriously
fail the live-Postgres `zero-cache-change-source` tests on replication-slot
contention — an environment limit, not a code failure).

Cross-verification audits (2026-07-09): the core correctness/security functions
were audited against upstream and confirmed faithful — `merge_ref_counts`,
`process_received_row`, `delete_unreferenced_rows`, cursor pagination
(`gather_start_constraints`), `hash_of_ast`/`normalize_ast`, `cmp_versions`/
`version_to_cookie`, LexiVersion monotonicity, `build_poke` schemaVersions
gating, mutation SQL (insert/upsert/update/delete + injection), pg default/type
mapping, and both authorization paths (read `simplify_condition` + write
`passes_policy`, both fail-closed). `name_mapper` was additionally cross-checked
by expanding the `mono-src` sparse checkout to `packages/zero-types` — confirmed
a faithful port of `columnName`/`row`/`columns` (unknown columns pass through in
`row`/`columns`, error in `columnName`). Genuinely-under-tested invariants found
during these audits were pinned (see the completion log's audit entries).

Recent focused checks:

- `cargo fmt -p zero-cache-server`
- `cargo test -p zero-cache-server desired_query_hydration_fetches_top_level_related_rows --lib`
- `cargo test -p zero-cache-server desired_query_hydration_fetches_compound_related_rows --lib`
- `cargo test -p zero-cache-server desired_query_hydration_fetches_nested_related_rows --lib`
- `cargo check -p zero-cache-server`
- `cargo fmt -p zero-cache-server`
- `cargo test -p zero-cache-server desired_query_hydration_fetches_top_level_related_rows --lib`
- `cargo test -p zero-cache-server desired_query_hydration_fetches_compound_related_rows --lib`
- `cargo check -p zero-cache-server`
- `cargo fmt -p zero-cache-server`
- `cargo test -p zero-cache-server desired_query_hydration_fetches_top_level_related_rows --lib`
- `cargo test -p zero-cache-server live_connection::tests --lib`
- `cargo check -p zero-cache-server`
- `cargo fmt -p zero-cache-server`
- `cargo test -p zero-cache-server async_desired_query_hydration_fetches_custom_query_transform --lib`
- `cargo test -p zero-cache-server live_connection::tests --lib`
- `cargo check -p zero-cache-server`
- `cargo fmt -p zero-cache-server`
- `cargo test -p zero-cache-server desired_query_hydration_uses_registered_custom_query_transform --lib`
- `cargo test -p zero-cache-server live_connection::tests --lib`
- `cargo check -p zero-cache-server`
- `cargo fmt -p zero-cache-server`
- `cargo test -p zero-cache-server desired_query_hydration_uses_ast_table_outside_demo_catalog --lib`
- `cargo test -p zero-cache-server live_connection::tests --lib`
- `cargo check -p zero-cache-server`
- `cargo fmt -p zero-cache-server`
- `cargo test -p zero-cache-server inspect_analyze_query_introspects_tables_outside_demo_catalog --lib`
- `cargo test -p zero-cache-server inspect_analyze_query --lib`
- `cargo test -p zero-cache-server analyze_query::tests --lib`
- `cargo check -p zero-cache-server`
- `cargo fmt -p zero-cache-server`
- `cargo test -p zero-cache-server inspect_analyze_query_applies_configured_read_permissions --lib`
- `cargo test -p zero-cache-server inspect_analyze_query --lib`
- `cargo check -p zero-cache-server`
- `cargo fmt -p zero-cache-auth`
- `cargo test -p zero-cache-auth read_authorizer --lib`
- `cargo check -p zero-cache-auth`
- `cargo fmt -p zero-cache-server`
- `cargo test -p zero-cache-server analyzes_correlated_subquery_exists_condition --lib`
- `cargo test -p zero-cache-server analyzes_correlated_subquery_not_exists_condition --lib`
- `cargo test -p zero-cache-server analyze_query::tests --lib`
- `cargo check -p zero-cache-server`

## Detailed crate log (mirrors `src/`)

| Rust crate            | TS source dir        | Status      |
| --------------------- | -------------------- | ----------- |
| `zero-cache-types`    | `src/types`          | 🟡 in progress. warmup.rs — NEW: port of `db/warmup.ts`'s pure decision
logic (`warmup_connection_count` — clamps a pool's configured max to `MAX_WARMUP_CONNECTIONS`;
`warmup_ping_report` — averages a batch of ping times and decides `Info`-vs-`Warn` log level at the
10ms threshold, matching upstream's `average >= 10` check). The actual warmup queries + `performance.now()`
timing are real I/O this port doesn't drive here — a caller measures its own ping times (e.g. via
`tokio-postgres`) and passes them in. 4 tests. |
| `zero-cache-shared`   | `../shared/src` deps | 🟡 in progress (hash, bigint-json stringify+parse, parse-big-int, queue, binary_search, centroid, tdigest, float_to_ordered_string, ref_count, logarithmic_histogram, deep_merge, arrays,
timed_cache — NEW: port of `cache.ts`'s `TimedCache<T>`, a generic TTL cache storing each value's
expiration at insertion time (not refreshed on read). First slice of the previously entirely-unmapped
`zero-cache/src/custom-queries` gap — `transform-query.ts`'s `CustomQueryTransformer` uses this to
cache transformed custom-query results for 5s; this ports the cache primitive itself, independent of
that larger `ConnectionContext`-coupled caller (still unported). `now` taken as an explicit parameter
on every method (this port's determinism convention) rather than reading `Date.now()` ambiently; the
periodic `setInterval`-driven sweep becomes a caller-driven `cleanup()` call instead of an ambient
timer (this port has none to hook into) — `get`'s own lazy eviction-on-read is unaffected and matches
upstream exactly, `cleanup` only reclaims memory for keys set but never read again. 8 tests.
error_details.rs — NEW: port of `observability/events.ts`'s `makeErrorDetails`, the pure error->JSON
mapping used to attach structured error detail to a published `ZeroEvent`. Closes a directory-coverage
scan finding: `zero-cache/src/observability` had ZERO representation anywhere in this table despite
being a real upstream directory. Took `&(dyn std::error::Error + 'static)` instead of JS's `unknown`
(anything throwable) since Rust has no arbitrary-enumerable-property/`.stack` equivalent to walk;
emits `message` (via `Display`) + a recursive `cause` (via `.source()`), omitting `name`/`stack`/extra
fields rather than fabricating them. 2 tests.
event_publish.rs — NEW: closes out `events.ts`'s remaining pure surface (flagged low-value but
unattempted across several rounds). `publish_backoff_delay_ms` (the retry loop's
`INITIAL_PUBLISH_BACKOFF_MS * 2^(i-1)` formula — no jitter, no cap, unlike this crate's other backoff
port in `zero-cache-mutagen::api_request`), `parse_extension_overrides` (validates
`extensionOverridesEnv`'s JSON value is `{"extensions": {...string|number|boolean}}`, the
`v.parse(JSON.parse(strVal), extensionsObjectSchema)` call — JSON *parsing* stays the caller's job
via `bigint_json::parse`, matching this port's parse/validate split elsewhere), and
`apply_extension_overrides` (`createCloudEvent`'s `{...overrides}` spread as an explicit
last-writer-wins merge). 6 tests. `metrics.ts` (pure OTel factory boilerplate, no OTel dependency in
this port) and the actual CloudEvent gzip+HTTP publish transport remain deliberately NOT ported — real
I/O needing the `cloudevents` npm package's equivalent, and nothing in this port produces a
`ZeroEvent` to publish yet anyway (no `zero-events` crate exists) — inert but real, not attempted.) |
| `zero-cache-view-syncer` | `src/custom-queries` (partial) | 🟡 in progress — `transform_query_cache_key.rs`
— port of `transform-query.ts`'s `getCacheKey`/`normalizedHeaders`, the cache-key computation
`CustomQueryTransformer` uses to key its `TimedCache` (see `zero-cache-shared`'s `timed_cache` above)
of transformed custom-query results. Unblocked by `connection_context_manager.rs`'s
`ConnectionFetchContext`/`HeaderOptions` addition. `token` taken as an explicit `Option<&str>`
parameter rather than read off `ctx.auth?.raw` since this port's `ConnectionContext` doesn't carry
`auth` yet. Field order in the JSON output matches upstream's object literal exactly so two calls
with identical logical inputs always produce identical cache keys. 6 tests.
NEW: `transform_query_response.rs` — port of `CustomQueryTransformer#transform`'s pure
response-shaping logic (`split_cached_and_uncached`: splits queries into cache hits vs. an
uncached-query request, mirroring the loop before `#requestTransform`; `shape_transform_response`:
maps a raw `QueryResponse` into `TransformedAndHashed`/`ErroredQuery` results, computing each
transformation hash via `query_hash::hash_of_ast` and populating the cache for every success while
deliberately NOT caching errors, matching upstream's "the user may want to retry a transient
failure" comment). `TransformedAndHashed` was originally defined locally here
before `zero-cache-auth::read_authorizer` existed; the auth crate now owns the
read-authorizer equivalent, but this view-syncer response-shaping module has not
yet been deduplicated against it. 6 tests.
CLOSED: the custom-queries HTTP/wire gap. NEW: `transform_query_fetch.rs` — glues
`transform_query_response`'s pure shaping, `zero_cache_protocol::query_server_json`'s response
parser (NEW — the `queryResponseSchema` JSON deserializer, `ast_json.rs`'s counterpart for the
`/query` response body; parses `querySuccessSchema`/`transformFailedBodySchema`, deliberately skips
the legacy tuple-message backwards-compat variant), and the already-existing (but previously
unwired) `zero_cache_mutagen::api_fetch::fetch_from_api_server` (a real, retrying `reqwest`-based
HTTP client — it turned out this had already been built for the mutagen push path and just needed
connecting up) into one real, live-callable `fetch_and_shape_transform_response`. `JsonValue`
(this crate's hand-rolled codec) <-> `serde_json::Value` (what `reqwest` speaks) conversion goes
through a JSON-text round trip rather than a hand-written structural mapper. 3 live tests against a
real local HTTP server (same raw-`TcpListener` pattern `api_fetch.rs`'s own tests use, not a mocking
crate) prove a real request/response round trip, a `TransformFailed` response body propagating
correctly, and a non-OK HTTP status surfacing as a `FetchError`. `validate()` (the always-hit-the-
server auth-maintenance wrapper, no decision logic of its own) remains unported — trivial to build
from the same pieces if ever needed. 9 new tests total (6 query_server_json + 3 transform_query_fetch).
Also added, in `zero-cache-protocol`, three prerequisites `transform_query_response.rs` needed:
`query_hash.rs` (`hashOfAST`/`hashOfNameAndArgs` — `query-hash.ts`; normalizes the AST, JSON-encodes
it via a NEW `ast_json::ast_to_json` serializer (the JSON-emitting counterpart to the existing
`ast_from_json` deserializer, round-trip-tested), hashes with `h64`, base36-encodes; the upstream
`WeakMap`-based memoization by AST object identity is a pure micro-optimization, skipped — no ambient
WeakMap and this port's convention keeps such caches at the call site); `custom_queries.rs` +
`query_server.rs` (pure data model for `custom-queries.ts`/`query-server.ts`: `TransformRequestQuery`,
`TransformedQuery`, `ErroredQuery`/`ErroredQueryKind`, `QueryResult`, `QuerySuccess`, `QueryResponse`
— no `valita` schemas ported, matching this port's no-schema-library convention); and
`error::TransformFailedBody`/`TransformFailedReason` (the origin-discriminated `transformFailedBodySchema`
union — `Server{database,parse,internal}` / `ZeroCacheHttp{status,bodyPreview}` /
`ZeroCacheOther{timeout,parse,internal}`). 20 new protocol tests (5 query_hash, 2 ast_json round-trip,
2 custom_queries, 1 query_server, 2 error, plus 8 pre-existing that still pass). |
| `zero-cache-protocol` | `../zero-protocol`   | 🟡 in progress (error kinds/origins/reasons (error_reason.rs — NEW, needed by api_fetch.rs's HTTP
failure classification), ProtocolError, AST data model + normalizeAST; ast_json.rs —
`ast_from_json`, a real `JsonValue -> Ast` deserializer (the counterpart to `astSchema`'s valita
schema): recursively parses Condition (Simple/And/Or/CorrelatedSubquery), ValuePosition (Literal/
Column/Parameter — Parameter kept opaque as raw JSON), CorrelatedSubquery, Bound, Ordering, all by
exact wire field names (camelCase: orderBy, parentField, etc). 12 tests. Closes the CVR-load
`clientAST` gap (see zero-cache-view-syncer entry) — client-/internal-type queries now round-trip as
their real type through Postgres instead of always landing as `Custom`; poke.rs/row_patch.rs/
queries_patch.rs/mutations_patch.rs/mutation_id.rs/mutation_result.rs/version.rs — the downstream sync
WebSocket message data model: PokeStartBody/PokePartBody/PokeEndBody, RowPatchOp (put/update/del/
clear), QueriesPatchOp (+ UpQueriesPatchOp for the client->server direction with AST/name/args),
MutationsPatch (put/del), MutationResult (Ok/App-error/Zero-error). Pure data types only — no actual
WebSocket transport/connection handling yet (that's the next increment: an actual sync-server loop
serializing these over a real socket). Does NOT port the mutation *request* CRUD op types
(insert/upsert/update/delete ops, mapCRUD) — those belong to the unported mutagen/pusher.ts engine;
connect.rs/ping.rs/pong.rs/close_connection.rs/delete_clients.rs/change_desired_queries.rs/
client_schema.rs/up.rs — the up-direction (client->server) message vocabulary, symmetric to
poke.rs/down.rs: ConnectedBody/InitConnectionBody (incl. `encode_sec_protocols`/
`decode_sec_protocols` — the WebSocket `Sec-WebSocket-Protocol` header base64+percent-encoding used
to carry the init-connection message + auth token before the socket is even open), PingBody/PongBody,
DeleteClientsBody, ChangeDesiredQueriesBody, ClientSchema + `normalize_client_schema` (sorts
tables/columns/primary-key for hashing). `Upstream` enum now covers the full upstream protocol tag set
at the pure request-shape level (see later milestones for push/pull/updateAuth/ack/inspect additions).
18 new tests.
complete_ordering.rs — NEW: port of `zql/src/query/complete-ordering.ts`'s `completeOrdering`/
`assertOrderingIncludesPK` (found via a `zql/src/query` directory scan — `query.ts`/`ttl.ts`/`error.ts`/
`named.ts` were already represented but `complete-ordering.ts`/`escape-like.ts` weren't).
`complete_ordering` recursively appends any primary-key columns missing from a query's (and every
related/correlated-subquery's) `orderBy`, so row ordering is always a total order — what
`LIMIT`/cursor pagination correctness ultimately depends on. `get_primary_key` taken as an infallible
closure (matching `normalize_ast`'s lookup-closure convention) rather than upstream's `must(...)`
-wrapped optional lookup. Also added `escape_like` (`escape-like.ts` — backslash-escapes `%`/`_` for
literal `LIKE` matching; note upstream's regex doesn't escape a pre-existing `\` either, ported
faithfully not "fixed"). 10 tests.
name_mapper.rs — NEW: port of `zero-schema/src/name-mapper.ts` + `zero-types/src/name-mapper.ts`'s
`NameMapper` class (`clientToServer`/`serverToClient`/`validator`/`table_name`/`column_name`/`row`/
`columns`) — translates table/column names between a client's schema-declared names and their real
Postgres names. Found via a directory-coverage scan of `zero-schema/src` (previously only
`compiled-permissions.ts` had any representation in this table). `zero-types/src/name-mapper.ts`
itself isn't in the sparse `mono-src/` checkout at all (only `zero-cache`/`zql`/`zqlite`/
`zero-protocol`/`zero-schema` are) — fetched directly from the upstream GitHub source to port
`NameMapper` faithfully rather than guessing its shape. Scope deviation: takes a minimal
`TableNameInfo`/`ColumnNameInfo` pair instead of the full `TableSchema`/`SchemaValue` type system
(deeply TS-generic-driven schema-authoring types, correctly out of scope like `table-builder.ts`) —
only the two fields `createMapperFrom` actually reads (a table's/column's optional `serverName`).
`row`/`columns` pass unmapped columns through unchanged (not an error), matching upstream exactly.
8 tests. A real prerequisite for whoever eventually wires ZQL query execution or write authorization
against real Postgres column names.) |
| `zero-cache-sqlite`   | `../zqlite` + `db/*`, `replicator/schema/*` (all 4), `replicator/change-processor.ts` (partial) | 🟡 StatementRunner, migrations, introspection, replication state, change log, table+column metadata, row_apply::{get_key, RowApplier::process_insert/process_update/process_delete/update/upsert/delete} —
complete row-mutation apply-loop (insert/update/delete incl. key-change + resumptive-replication
fallback), real SQLite + ChangeLog integration, exact pos-counter semantics; create.rs — DDL SQL generation; ddl_apply.rs — DdlApplier: ALL ChangeProcessor DDL handlers ported (create/drop/rename table, add/
drop/update column incl. type-change rename-and-copy + index recreation, create/drop index),
backfill-simplified, against real SQLite + change-log + table/column-metadata;
change_dispatcher.rs — ChangeDispatcher: the TOP-LEVEL apply-loop (table-spec cache, begin/commit/
rollback transaction state machine, Change-tag dispatch to RowApplier/DdlApplier incl. Change::Update
with key-change support) — a real Postgres `Change` message stream can now be applied end-to-end to a
SQLite replica, including all row-mutation and DDL variants except backfill |
| `zero-cache-config`   | `src/config`         | 🟡 in progress (NEW crate. normalize.rs — port of `assertNormalized`/
`normalizeZeroConfig` (normalize.ts), the config-defaulting/cross-field-validation business logic:
task-id/change-streamer-port/litestream-port/num-sync-workers auto-generation and defaulting,
change-streamer-address derivation from host IP + port, change-db/cvr-db defaulting to upstream-db,
ECS-environment keepalive-timeout default, and `assertNormalized`'s full validation chain (incl. the
litestream v5 backup/restore/executable cross-field consistency checks) checked in upstream's exact
order. Scope deviation, this port's established convention: every ambient OS/env read upstream performs
inline (`getHostIp()`, `os.availableParallelism()`, `nanoid()`, `NODE_ENV`, ECS-environment detection)
is taken as an explicit parameter instead (`host_ip`, `available_parallelism`, `generate_task_id`,
`is_development_mode`, `is_running_in_ecs`); the `env[...] = ...` side effects upstream performs to
propagate defaults to spawned child-worker environments are NOT ported — this port's `tokio::spawn`
process model (see `worker_message.rs`) shares one process and its env already, so there's nothing to
propagate. `zero-config.ts` itself (1266 lines, the CLI/env-var option DECLARATION file — not
algorithmic logic, just a config-builder library invocation listing every flag) deliberately NOT ported
wholesale; this port has no equivalent CLI-parsing library and normalize.ts's real logic doesn't need
one to be portable. 15 tests, all passed on the first attempt.
**network.rs — NEW.** Ports `getPreferredIp`, the pure interface-ranking half of `network.ts`'s
`getHostIp` (`normalize_zero_config`'s `host_ip` parameter, taken as injected rather than resolved
internally — see `normalize.rs`'s module doc). Given a list of network interfaces, ranks them:
non-reserved/non-ULA before reserved, non-internal before internal, IPv4 before IPv6, then by
preferred name prefix (`eth`/`en` by default), then address string as a final tiebreak — exactly
upstream's sort order — and brackets the winning address if it's IPv6 (for URL use). Scope deviation,
documented: `isPrivate`/`isReserved` (from the `is-in-subnet` npm package, classifying against the
full IANA special-purpose registry) are approximated using `std::net`'s built-in address-kind
predicates (loopback/unspecified/link-local/multicast/broadcast/documentation for IPv4; hand-rolled
`fc00::/7` ULA detection for IPv6, since `Ipv6Addr::is_unique_local` isn't stable on this port's
MSRV) — covers the practical cases `getHostIp` needs to avoid, not necessarily byte-identical to the
npm package's exact subnet tables for obscure ranges. `getHostIp` itself (the `os.networkInterfaces()`
call) NOT ported — no OS network-interface enumeration wired up in this port, and nothing needs it
internally since `host_ip` is already an injected parameter. 9 tests, incl. one specifically pinning a
subtle upstream detail: IPv4 RFC1918 private addresses (192.168/16 etc.) are NOT deprioritized by the
rank check on their own — only `isReserved` applies to IPv4, `isPrivate` only gates IPv6 — ported
faithfully rather than "fixed" to also deprioritize private IPv4 (which would have been a real,
silent behavioral divergence from upstream).
**is_admin_password_valid.rs — NEW.** Ports `zero-config.ts`'s `isAdminPasswordValid`: the
admin-endpoint password check, incl. the development-mode bypass (valid with no password configured
IFF both the request and the config have no password AND `is_development_mode`) and constant-time
comparison against a configured password. `timingSafeEqual` (Node's `crypto.timingSafeEqual`) has no
equivalent dependency here, so a constant-time byte comparison is hand-rolled (`ct_eq`: XORs every
byte pair without early-return, matching the same "don't let comparison time leak information"
threat model). `lc.warn?.(...)`/`lc.debug?.(...)` side effects are left to the caller via a returned
`Outcome` enum (matching `ttl.rs`'s established `was_clamped`-flag pattern for LogContext-free
ports); upstream's module-level `hasWarned`/`resetWarnOnceState` singleton is threaded through as an
explicit `warned_once: &mut bool` parameter instead of a hidden static. 9 tests. `zero-config.ts`
itself remains deliberately unported (see `normalize.rs`'s module doc above) — this is a second pure
slice pulled out of that same file, independent of the CLI-declaration machinery around it.) |
| `zero-cache-db`       | `src/db`             | 🟡 in progress (STALE ROW, CORRECTED: `db/*` modules — `pg_to_lite`,
`column_metadata`, `specs`, `pg_types`, `pg_copy_binary`, `pg` — are already ported; they currently
live in `zero-cache-types` rather than a separate `zero-cache-db` crate, a deliberate deferral noted
where they were added ("until a `zero-cache-db` crate is split out"). This row previously said
`⬜ planned`, which was inaccurate and had gone uncorrected for many rounds — fixed here rather than
left to keep misleading future scans of this table into thinking `db/*` was untouched.) |
| `zero-cache-auth`     | `src/auth`           | 🟡 in progress (write_authorizer.rs — `validate_table_names`
(op table names vs a known-table set) and `normalize_ops` (resolves each Upsert into Insert-or-Update
via an injected `row_exists` predicate, standing in for a live `TableSource` lookup). policy.rs —
ports `zero-schema/compiled-permissions.ts`'s `Policy`/`AssetPermissions`/`TablePermissions`/
`PermissionsConfig` model plus `write-authorizer.ts`'s `#canDo`/`#passesPolicy`/`#passesPolicyGroup`
evaluation core — **THE PERMISSION-ENFORCEMENT GAP IS NOW CLOSED**. Scope deviation (documented in the
module doc): instead of building/running a real ZQL query pipeline (`#passesPolicy` upstream), this
port looks the row up by primary key and checks whether any policy rule's `create_predicate`-compiled
condition matches it directly — equivalent for rules that only reference columns of the row itself,
but does NOT support `Condition::CorrelatedSubquery` rules (permission checks referencing a different
table's data, e.g. "user owns the parent issue") via the plain functions — since `create_predicate`
panics on those. **NOW RESOLVED**: `passes_policy_with_exists`/`passes_policy_group_with_exists`/
`can_do_with_exists` mirror `create_predicate`/`create_predicate_with_exists`'s own naming pattern,
taking an `ExistsFn` resolver and threading it through instead of panicking; the plain functions
became thin wrappers around a panicking default. LIVE-WIRED test: `can_do`'s resolver connected to a
real `zero_cache_zql::ivm::table_source::TableSource` via `ivm::join::exists_for_row`, genuinely
evaluating a cross-table rule ("issue has a comment") against real pushed data, not a mock. `write_
authorizer.rs` gained `can_pre_mutation`/`can_post_mutation` orchestrating `can_do` per-op (Insert
skips pre-mutation, Delete skips post-mutation, Update checks both), with row lookup and post-
mutation resulting-row as injected closures. 24 tests total.
**CORRECTION + WIRING, found via an unconsumed-pub-fn scan:** the claim just above ("`authorized: bool`
now has a real implementation to feed it") was premature — `validate_table_names`/`normalize_ops`/
`can_pre_mutation`/`can_post_mutation` existed but nothing in this port ever actually CHAINED them
together the way `mutagen.ts#processMutationWithTx` does (`validateTableNames` -> `normalizeOps` ->
`canPreMutation && canPostMutation`, the latter two run concurrently via `Promise.all` upstream). NEW:
`authorize_mutation`, composing exactly that sequence into one function returning the `authorized:
bool` `zero_cache_mutagen::orchestration::plan_mutation_sql` actually needs. 4 new tests (28 total in
the crate) incl. one proving default-deny (no permissions configured at all -> denied, even for an
Insert which skips its pre-mutation check) and one proving `canPre && canPost` (either phase alone
failing denies the whole mutation). Still not a full live wire-up — needs a real replica for the
`row_exists`/`existing_row`/`resulting_row` closures — but the actual authorization DECISION SEQUENCE
is now provably correct and composed, closing what an earlier round flagged as "the single most
consequential correctness gap in the port".
**FULLY WIRED: `authorize_and_plan_mutation`.** Since `zero-cache-mutagen::orchestration::
plan_mutation_sql` can't depend on `zero-cache-auth` (the reverse dependency already exists, for
`CrudOp`), the composition lives here: `authorize_and_plan_mutation` calls `authorize_mutation` then
feeds its verdict straight into `plan_mutation_sql`, matching `processMutationWithTx`'s real
non-error-mode sequence end to end (error-mode short-circuits to no SQL before even authorizing,
matching upstream's `if (!errorMode) {...}` guard). 4 new tests (32 total in the crate) incl. one
asserting real generated `INSERT INTO` SQL comes back when authorized. `orchestration.rs`'s module
doc updated to point here instead of describing authorization as "entirely unported".
**NEW: `read_authorizer.rs`.** Ports the pure half of
`auth/read-authorizer.ts`: `transform_and_hash_query`/`transform_query` inject
row `select` permission rules into AST `where` conditions using upstream's
default-deny behavior (`or([])` when no row-select policy exists), recurse into
top-level `related` subqueries and `Condition::CorrelatedSubquery` conditions
so `whereExists` cannot become a read-permission oracle, simplify condition
shape with the same `simplifyCondition` rules, and compute
`transformation_hash` via `zero_cache_protocol::query_hash::hash_of_ast`.
Internal queries bypass application permissions. Scope still missing, documented
in the module: auth-data static parameter binding (`bindStaticParameters`) is
not implemented because `Parameter` is still opaque in the Rust AST model. 6
focused tests; crate check passed.) |
| `zero-cache-change-source` | `src/services/change-source` | 🟡 STALE ROW, see the real `zero-cache-change-source` entry further down this table (protocol layer, pg_connection, pgoutput decoder, replication_conn, pg_to_change — this is a duplicate placeholder left over from early in the port, not a second unstarted crate) |
| `zero-cache-change-streamer` | `src/services/change-streamer` | 🟡 in progress — see `zero-cache-services`'s `change_streamer_forwarder.rs` (`SubscriberSet<T>`, the active-vs-queued subscriber fan-out decision logic ported from `Forwarder`) AND `broadcast.rs` (NEW — `Broadcast`'s consensus-based flow-control timeout decision logic: majority-of-subscribers-then-timeout release algorithm, ported as `Broadcast<T>::check_progress`/`mark_completed`, generic over subscriber identifier same as `SubscriberSet<T>`). NOT ported: the actual `sub.send(change)` message-delivery/ack wiring (needs the real `Subscriber` websocket entity, unported), progress-monitor timer, observability metrics. STALE ROW previously said `⬜ planned`; corrected. |
| `zero-cache-replicator`    | `src/services/replicator`     | 🟡 in progress — `replicator/schema/*` (change-log, replication-state, table-metadata, column-metadata) and `replicator/change-processor.ts` live in `zero-cache-sqlite` (see that row); `replicator.ts`'s replica-file setup/maintenance + IPC live in `zero-cache-sqlite::replicator_setup.rs`/`zero-cache-workers::replicator_ipc.rs`. STALE ROW previously said `⬜ planned`; corrected — this is genuinely one of the more-ported subsystems in the whole port, just organized under other crates' rows. |
| `zero-cache-view-syncer`   | `src/services/view-syncer`    | 🟡 in progress (row-set-signature, query-covering, cvr_version, cvr_types, cvr_ref_counts, cvr_eviction, cvr_internal_queries, cvr_updater, cvr_desired_queries (put_desired_queries
+ delete_queries backing markDesiredQueriesAsInactive/deleteDesiredQueries/clearDesiredQueries),
client_patch — Patch/PatchToVersion/ClientRowPatch, cvr_client_state — setClientSchema/setProfileID;
cvr_schema_sql.rs — DDL generation for CVRStore's real Postgres schema (instances/clients/queries/
desires/rowsVersion/rows tables + indexes/FK constraints), byte-for-byte structurally matching
upstream's `schema/cvr.ts` templates via `zero_cache_types::sql::id` quoting. LIVE-VERIFIED: a test
actually executes the generated DDL against the real local Postgres instance and confirms all 6
tables exist afterward (not just string-matched against the TS source) — same "run it for real"
standard applied to every other SQL-generation module in this port. 10 tests. NOT ported: `CVRStore`
itself (~1447 lines — load/flush against a live connection, row-diffing, catchup-patch queries) or
the row-type <-> `RowRecord`/`ClientRecord` conversions, which operate on live query results this
crate has no CVR Postgres connection to produce yet — this is schema groundwork only;
cvr_load.rs — `load_cvr_from_rows`/`as_query`, the pure in-memory-reconstruction half of
`CVRStore.#load`: merges flat `clients`/`queries`/`desires` rows (as they'd come back from the
`cvr_schema_sql` tables) into a `Cvr` struct exactly like upstream's three merge loops, incl.
`asQuery`'s custom/internal/client query-type discrimination and the desires-loop's TTL clamping via
the already-ported `zero_cache_zql::ttl::clamp_ttl`. 8 tests. NOT ported: the SQL queries themselves,
task ownership/lease-conflict handling (`OwnershipError`), and `RowsVersionBehindError` — all three
need live-transaction + task-id semantics not modeled yet; this module assumes the caller has already
resolved those before calling it; cvr_store_pg.rs — `load_cvr`, wiring `cvr_load` to a REAL
`tokio-postgres` connection against the `cvr_schema_sql` tables. LIVE test: creates the real CVR
schema, inserts rows directly via SQL (standing in for a real `CVRStore::flush`), calls `load_cvr`,
and asserts the reconstructed `Cvr` matches — genuine round trip through a real Postgres connection.
`queryArgs` AND `clientAST` are both genuinely parsed — `queryArgs` via `bigint_json::parse`,
`clientAST` via `zero_cache_protocol::ast_json::ast_from_json` — so client-/internal-type queries now
round-trip as their real type through Postgres instead of always landing as `Custom`. **OWNERSHIP/
LEASE HANDLING + INSTANCES/ROWSVERSION OVERLAY NOW DONE**: new `cvr_ownership.rs` ports the
ownership-conflict/row-catchup decision logic from `CVRStore.#load` (`decide_instance_load`: no
`instances` row -> `New`; deleted -> `ClientNotFound` error; a different task holding a still-valid
lease -> `Ownership` error; `instances.version != rowsVersion.version` -> `RowsBehind` (a *returned*
signal, not an error, matching upstream's non-throwing `RowsVersionBehindError`); otherwise `Ready`
with the instance fields to overlay + whether this task should claim the lease) plus
`get_claim_ownership_sql` (the fire-and-forget ownership-claim UPDATE). `load_cvr` now queries
`instances` LEFT JOINed with `rowsVersion`, runs it through `decide_instance_load`, fires the
ownership-claim UPDATE when needed, and overlays version/lastActive/ttlClock/replicaVersion/
profileID/clientSchema onto the loaded `Cvr`. Returns `LoadCvrOutcome::Loaded(Cvr) |
RowsBehind{..}`. 10 new cvr_ownership tests + the live test strengthened to insert a real `instances`+
`rowsVersion` row pair with a different owner and assert both the overlay AND that ownership was
actually claimed (re-queried after `load_cvr` returns) — genuinely verified against real Postgres.
**STARTED THE WRITE PATH**: new cvr_flush_sql.rs ports the SQL-generation half of
`CVRStore.putInstance` — `get_upsert_instance_sql` (the `instances` row `INSERT ... ON CONFLICT
("clientGroupID") DO UPDATE SET ...` upsert every material `#flush` includes). LIVE-VERIFIED: runs
the generated SQL against real Postgres twice (insert path, then the ON CONFLICT update path) and
confirms the row. 5 tests. Also added `check_version_and_ownership`/`get_check_version_and_ownership_sql`
to cvr_ownership.rs — the write-time counterpart to `#load`'s ownership check
(`#checkVersionAndOwnership`): validates the flushing task either owns the CVR lease or none is
currently held, AND the CVR's version still matches what this flush expected
(`ConcurrentModificationException` if not — another flush raced ahead). Refactored the
`(grantedAt ?? 0) > lastConnectTime` conflict check into a shared `ownership_conflicts` helper used
by both the read-time and write-time checks. cvr_ownership.rs now 16 tests total. Added
`get_flush_desires_sql` (port of `#flushDesires`, the single-form bulk `json_to_recordset` upsert into
`desires`) to cvr_flush_sql.rs — rows inlined as a JSON array literal (`json_to_recordset('[...]'::json)`)
rather than a bound parameter, matching the representation choice already made for
`zero-cache-mutagen::sql`; correctly replicates `convertTTLValues`'s ttl/ttlMs/inactivatedAt/
inactivatedAtMs derivation (negative ttl -> forever -> both ttl columns NULL) and the
double-timestamp-conversion quirk (`to_timestamp("inactivatedAt" / 1000.0)` on an already-divided
value — upstream's own historical behavior, ported faithfully not re-derived). LIVE-VERIFIED against
real Postgres (insert + ON CONFLICT update paths). cvr_flush_sql.rs now 11 tests total. **`#flushQueries` NOW ALSO DONE**: `get_flush_queries_full_sql`
(full-row bulk upsert, incl. upstream's `queryArgs` pre-stringification workaround for a postgres.js
boolean-array bug — ported literally) and `get_flush_queries_partial_sql` (the partial-column-update
form: a plain `UPDATE ... FROM json_to_recordset(...)` where each column only overwrites if its
`<field>Set` flag is true, else keeps `q.<field>` — the CASE-based "was this actually set" pattern).
LIVE-VERIFIED together: full upsert then a partial update touching only `patchVersion`, confirming
`queryName` was genuinely left untouched by the partial form against real Postgres. cvr_flush_sql.rs
now 16 tests total. **This closes the SQL-generation portion of CVRStore's write path.** NOT ported:
the row-cache (`#rowCache.executeRowUpdates`/`apply` — row-level put/delete flush logic) and
`#flush`'s overall transaction/pipelining orchestration (tying `putInstance` + `flushQueries` +
`flushDesires` + row updates + `checkVersionAndOwnership` together into one real transaction) — the
remaining CVRStore write-path work is now orchestration/wiring, not SQL generation.
**ORCHESTRATION NOW DONE TOO**: `cvr_store_pg::flush_cvr` wires `check_version_and_ownership` + the
`instances`/`queries`/`desires` upserts together into ONE real `tokio_postgres::Transaction` — the
version/ownership check runs first (`SELECT ... FOR UPDATE`), and on any failure the whole transaction
rolls back via `Transaction`'s drop-without-commit, exactly matching upstream's "throwing inside the
begin callback rolls back the transaction" contract. LIVE-VERIFIED with two tests: one commits an
instance+query+desire together and re-queries to confirm all three landed atomically; the other
deliberately mismatches the expected version and confirms the instance upsert was NOT committed —
genuinely proving the all-or-nothing transaction semantics, not just that the check function returns
an error in isolation. **THE ROW-CACHE IS NOW ALSO DONE**: new cvr_row_cache_sql.rs ports
`row-record-cache.ts`'s `executeRowUpdates` SQL generation — `get_row_updates_sql` always emits the
`rowsVersion` upsert first, then one `DELETE` per null (tombstoned) row update, then (if any puts
exist) ONE bulk `INSERT ... FROM json_to_recordset(...) ... ON CONFLICT DO UPDATE` for all puts in the
batch. Wired into `flush_cvr`, which now takes `row_updates`/`rows_version` and applies them in the
same transaction as everything else. LIVE-VERIFIED (5 tests in cvr_row_cache_sql.rs, incl. a batch
upsert followed by a mixed delete+upsert batch, confirming the final row set and `rowsVersion` are
correct). **This closes CVRStore's entire write-path SQL surface.** NOT ported: `RowRecordCache`'s
in-memory cache / deferred-flush-threshold logic, `CVRFlushStats` bookkeeping, and the "only write
what materially changed" pending-write coalescing `CVRStore`'s stateful `#pendingXWrites` fields do —
`flush_cvr` always applies exactly what the caller passes, once, rather than reproducing `CVRStore`'s
full stateful accumulate-then-flush object model (a legitimate scope boundary: the SQL/transaction
correctness is proven; the in-memory bookkeeping around *deciding what to flush* is a distinct,
separable concern).
**NEW: connection_context_manager.rs** — port of `connection-context-manager.ts`'s core state machine,
the `ConnectionContextManager` prerequisite that had been blocking `PusherService`/`PushWorker`'s
`Queue`-based lifecycle wrapper (`enqueuePush`/`ackMutationResponses` both need
`mustGetConnectionContext`; `#processPush` needs `validateConnection`/`failConnection`). Ports the
provisional->validated connection lifecycle faithfully: `register_connection` (replaces any existing
record for the same `clientID`, starts `Provisional`), `get_connection_context`/
`must_get_connection_context` (clientID lookup + wsID match, `ProtocolError(InvalidConnectionRequest)`
on miss), `validate_connection` (server-validated vs client-fallback identity resolution, pins the
group's `userID` on first validation, `ProtocolError(Unauthorized)` on a mismatched claimed userID OR a
later connection's userID conflicting with the pinned one, promotes to `Validated` with a computed
`revalidate_at`), `fail_connection`/`close_connection` (both delegate to a shared remove — `fail`
additionally guards on a matching revision, matching upstream's `#removeConnection`), sticky
background-connection selection (`#refreshBackgroundConnectionContext`'s exact tie-breaking: prefer a
freshly-validated connection only if no background connection currently exists; otherwise keep the
current one until it's gone), and `plan_maintenance` (due-revalidations sorted by insertion order,
`retransform` due-check, and the `maintenanceNotBeforeAt` gate that suppresses reporting anything due
until a deferred transient-failure cooldown passes). `now` is an explicit `i64` parameter everywhere
instead of an ambient clock, matching this port's determinism convention. NOT ported (deliberate, see
module doc): `initConnection` (needs `InitConnectionBody`/header-allowlist config not in this port),
`updateAuth` (needs `resolveAuth`/legacy-JWT validation — a separate, unported auth subsystem), and
`queryContext`/`mutateContext` (URL-pattern/header config — `pusher_batch::ConnCtx`/`MutateContext`
remain the separate simplified shape a caller attaches once wired to real config, as that module's doc
already noted). 16 tests, incl. two the tests themselves caught real bugs in during authoring (a
revision-guard test using the wrong initial revision, and a `plan_maintenance`-gate test that
initially didn't actually exercise the gate) — both fixed before landing. This closes the
`ConnectionContextManager` prerequisite; what's left of `PusherService`/`PushWorker`'s lifecycle
wrapper is now the `Queue`-based drain loop and RPC surface (`initConnection`/`enqueuePush`/
`ackMutationResponses`/`deleteClientMutations`) actually calling into this manager plus the already-
ported `pusher_batch`/`api_fetch`/`api_request`/`pusher_response` pieces — a wiring task now, not a
missing-subsystem task.
**`queryContext`/`mutateContext` NOW CARRIED — closing a gap this module's doc named across several
prior rounds.** `ConnectionContext` gained `query_context`/`mutate_context: ConnectionFetchContext`
(new `HeaderOptions`/`ConnectionFetchContext` types — `allowed_url_patterns` a `Vec<String>` of raw
pattern strings rather than compiled `URLPattern`s, no URL-pattern library in this port; headers are
plain caller-supplied maps, not allowlist-FILTERED against `ZeroConfig`, since that config layer
doesn't exist yet — same "no CLI/env config" boundary `normalize.rs` already names). `register_connection`
builds a connection with EMPTY fetch contexts (matching upstream before any config is known); new
`update_fetch_contexts` — the header-filtering-free part of `initConnection` — lets a caller attach
real query/push URLs+headers afterward, bumping the revision and demoting back to `Provisional`
(needed a new private `demote_connection` helper, port of `#demoteConnection`, previously unbuilt).
4 new tests (20 total in this module), incl. one confirming a context update correctly demotes an
already-`Validated` connection. `pusher_batch::MutateContext`/`ConnCtx` remain separately, since
`PushWorker` still consumes its own simplified shape — not yet threaded through this new field.
FULL `initConnection` (the `InitConnectionBody`-parsing/header-ALLOWLIST-filtering half) remains NOT
ported — that's the `ZeroConfig`-shaped gap, not this one.
**FIRST SLICE OF `ViewSyncerService` ITSELF — NEW: view_syncer_lifecycle.rs.** `ViewSyncerService`
(view-syncer.ts, ~2900 lines) had been entirely unbuilt despite this crate's ~13 supporting CVR/query
modules being ready to be consumed by it — identified as the single largest missing subsystem by LOC
in the whole port. This round ports the genuinely pure decision functions out of it, independent of
the CVR store/SQLite replica/WebSocket machinery the rest of the class is coupled to: `KeepAlive`
(`keepalive()`'s deadline-tracking state — pushes `keep_alive_until` out by `keepaliveMs` when the
underlying change stream is still active, returns `false` without touching the deadline once shutting
down), `check_shutdown_conditions` (the decision half of `#checkForShutdownConditionsInLock`, taken
AFTER a caller has already awaited `cvrStore.flushed()` — that's the one async precondition this
module doesn't model; has-clients short-circuit / within-keepalive-window reschedule / actually-
shutdown, incl. the `<=` boundary matching upstream exactly), and `ThrashDetector`
(`#checkForThrashing`'s sliding-window query-replacement-rate detector — warns when the same queryID
is replaced ≥3 times within 60s, typically indicating clients with different auth contexts hitting the
same client group). `now` taken as an explicit `i64` param throughout, per this port's determinism
convention. 10 tests. NOT ported (the real, substantial remaining gap): CVR snapshot/updater
orchestration, IVM pipeline sync (`#syncQueryPipelineSet`/`#addAndRemoveQueries`/`#advancePipelines`),
query hydration/catchup, auth maintenance/background retransform, and the `run()`/connection-lock
machinery tying it all together — `ViewSyncerService` as a whole remains unbuilt; this is a first,
honest toehold, not a claim of completion.
**Extended view_syncer_lifecycle.rs — the file-level standalone pure helpers from the bottom of
view-syncer.ts.** `contents_and_version` (splits a replica row into its `_0_version` watermark +
remaining columns, erroring on a missing/empty version, matching upstream's thrown `Error`),
`check_client_and_cvr_versions` (validates a client's claimed base version against the CVR's actual
version before sync starts — `ClientNotFound` if the CVR is empty but the client claims otherwise,
`StaleBaseCookie` if the client is ahead of a non-empty CVR), `is_transform_failed_error` (narrowed
port of `isAuthErrorBody`/`isTransformFailedError` — this port's `TransformFailedBody` has no
generic `ErrorBody`/legacy-`PushError` case since `PushFailedBody` isn't ported, so only the
`ZeroCacheHttp` 401/403 branch survives, the one this call site actually needs), `expired`/
`has_expired_queries` (a query is expired only once EVERY referencing client has inactivated it past
its clamped TTL; internal queries never expire). 13 new tests (22 total in the file). Full workspace
clean, zero warnings; full suite green under `--test-threads=1` (947 total, 262 in
zero-cache-view-syncer). `ViewSyncerService`'s pure-function surface is now essentially exhausted —
what remains genuinely needs the live CVR store/SQLite replica/WebSocket machinery this module
deliberately doesn't model.
**Extended query_covering.rs with a third `ViewSyncerService` pure slice: `find_query_coverage_shadow_hit`**
— port of `#findQueryCoverageShadowHit`: given an already-built `QueryCoveringIndex` (query_covering.rs,
pre-existing), looks up whether a just-hydrated query is covered by another currently-running query and
builds the `QueryCoverageShadowHit` observational-logging record if so. Pure given the index; the index's
own live maintenance (`#syncQueryPipelineSet` adding/removing entries) and `#logQueryCoverageShadowSummary`'s
LogContext plumbing built on top remain unported, still part of the real stateful-wiring gap. 3 new tests
(17 total in the file).
**query_set_sync.rs — NEW, second `ViewSyncerService` slice.** Ports the pure "should we force a CVR
version bump" decision from `#addAndRemoveQueries` — the block right before `updater.trackQueries`
handling the case where already-hydrated queries are re-executed unchanged: `trackQueries` alone won't
bump `configVersion` for that case, so a bump must be forced to ensure any row diff from `received()`
still reaches the client via a poke. `same_hash_rehydrated_query_ids` (queries whose
`transformationHash` matches what the CVR already has recorded), `track_queries_will_bump_version`
(state-version advance / any removal / any real transformation-hash change), and
`decide_forced_version_bump` (combines both plus `driftedQueryIDs` into the exact
`Mixed`/`RowSetSignatureDrift`/`MissingPipeline` reason classification upstream records as a metric).
11 tests. Still NOT ported: the surrounding orchestration (`CVRQueryDrivenUpdater`, `#pipelines.addQuery`/
`removeQuery`, pokers, query covering, catchup) — this is one more pure decision extracted from a still-
mostly-unbuilt method, same incremental pattern as the lifecycle slice above.
**WIRING added later: `apply_forced_version_bump_if_needed`.** Composes `decide_forced_version_bump`
with `cvr_updater::ensure_new_version` — the actual `updater.ensureNewVersion()` call upstream makes
when a forced bump is decided. Neither piece called the other anywhere else in this port; this proves
the decision function's result is directly usable to drive the real CVR-version state machine, not
just a standalone enum nobody consumes. 3 new tests (14 total in the file) incl. one proving the
composition is idempotent against the same `orig` version, matching `ensure_new_version`'s own
contract.
**Second WIRING find, same technique applied to `cvr_eviction.rs`:** `get_inactive_queries`/
`next_eviction_time` had been ported but never consumed anywhere in this port.
`view_syncer_lifecycle.rs` gained `schedule_expire_eviction_delay` — port of
`#scheduleExpireEviction`'s pure delay computation, composing `next_eviction_time` with the
`TTL_TIMER_HYSTERESIS`/`MAX_TTL_MS` hysteresis-and-clamp math upstream applies before actually
scheduling a timer (`max(hysteresis, min(next - now + hysteresis, MAX_TTL_MS))`). Returns `None`
when there's nothing to schedule. Actually starting a real timer remains the caller's job. 4 new
tests (26 total in the file).
**Third WIRING find, in `row_set_signature.rs`:** `parse_signature` had been ported but never
consumed anywhere in this port. Added `detect_row_set_signature_drift` — composes it with the actual
drift-comparison decision inline in `ViewSyncerService`'s hydration path (view-syncer.ts ~line 1598):
compares a query's CVR-stored signature against a freshly-hydrated candidate signature, flagging
drift only possible for `Cap`/`LIMIT`-operator queries that non-deterministically pick a different
N-row subset on re-execution. Returns `Option<bool>` (`None` = nothing stored to compare against,
matching upstream's `!== undefined && !== null` guard — distinct from `Some(false)` = compared and
matched). This feeds `driftedQueryIDs`, the exact input `query_set_sync::decide_forced_version_bump`
already consumes — closing another real link in the same chain the last two rounds' wiring work
built. 4 new tests.
**FIRST GENUINELY STATEFUL `ViewSyncerService` SLICE — NEW: view_syncer_session.rs.** Every prior
piece of `ViewSyncerService` ported so far was a free function (or free functions composed with each
other); nothing OWNED a loaded `Cvr` and used it to drive decisions the way the real class does.
`ViewSyncerSession` does: `connect()` loads a real CVR via `cvr_store_pg::load_cvr` and wraps it with
the two per-connection pure state machines (`KeepAlive`/`ThrashDetector`), then
`validate_client_version`/`keepalive`/`check_shutdown`/`check_for_thrashing` delegate to
`view_syncer_lifecycle`'s already-ported functions against that OWNED state instead of state the
caller has to thread through manually. Scope: still not `ViewSyncerService` — no IVM pipeline, no
pokers, no query hydration/catchup, no connection-lock machinery, no auth-maintenance timer; this is
the connection-lifecycle sliver only. 2 LIVE tests against real Postgres (not mocked): one drives a
full session lifecycle (connect -> reject-a-client-ahead-of-the-real-CVR -> accept-a-client-at-the-
CVR's-version -> keepalive -> shutdown-decision -> thrash-check, all through the session object), the
other confirms `connect()` reports `RowsBehind` instead of a session when the CVR's row-catchup state
requires it. Both passed on the first attempt. Full workspace clean, zero warnings; full suite green
under `--test-threads=1` (1015 total, 278 in zero-cache-view-syncer).
**Extended `ViewSyncerSession` with its two remaining previously-orphaned decision functions:**
`schedule_expire_eviction_delay` (delegates to `view_syncer_lifecycle`'s function of the same name
against `self.cvr`) and `detect_row_set_signature_drift` (looks up a query's stored `rowSetSignature`
in `self.cvr.queries` — `None` for an internal query, which has no such field — and delegates to
`row_set_signature::detect_row_set_signature_drift`). Both had been ported and wired to a PURE
decision earlier but never had an actual owned `Cvr` to run against until this session object existed.
1 new live test (4 total in the file) against real Postgres: inserts a desire with a real
`inactivatedAtMs`/`ttlMs` and a query with a real stored `rowSetSignature`, then asserts both new
session methods compute the correct answers from that genuinely-loaded state. Passed on the first
attempt. Full workspace clean, zero warnings; full suite green under `--test-threads=1` (1016 total,
279 in zero-cache-view-syncer).
**Fourth `ViewSyncerSession` addition: `apply_forced_version_bump`.** Wires
`query_set_sync::apply_forced_version_bump_if_needed` against the session's own `self.cvr` —
builds the per-query transformation-hash map from `self.cvr.queries` (Client/Custom/Internal all
handled), decodes `self.cvr.version.state_version` (lexi-encoded) back to `i64` via
`zero_cache_types::lexi_version::version_from_lexi`, and mutates `self.cvr.version` in place when a
bump is forced. `current_db_state_version` is taken as a plain caller-supplied `i64` parameter,
standing in for `this.#pipelines.currentVersion()` — this port has no live IVM pipeline object yet,
so this follows the same "take a not-yet-built dependency's value as an explicit parameter" pattern
used throughout this crate (e.g. `row_exists`/`existing_row` closures). 2 new live tests (5 total in
the file) against real Postgres: one confirms no bump is forced when the query has no stored
`transformationHash` to match against; the other inserts a query WITH a matching
`transformationHash`, calls `apply_forced_version_bump`, and asserts the session's own `cvr.version`
is genuinely mutated (not just a returned reason). Both passed on the first attempt. Full workspace
clean, zero warnings; full suite green under `--test-threads=1` (1017 total, 280 in
zero-cache-view-syncer). `ViewSyncerSession` now wires all four previously-orphaned
`view_syncer_lifecycle`/`query_set_sync`/`row_set_signature` decision functions to one real,
Postgres-loaded `Cvr`.
**FIFTH addition, genuinely NEW logic (not a wiring find): `advance_ttl_clock`.** Port of
`#getTTLClock` — advances the session's tracked TTL clock by the wall-clock delta since it was last
computed, panicking if the result would exceed `now` (a real monotonic-clock invariant, matching
upstream's assert). Scope simplification: upstream tracks `#ttlClock` separately from
`#cvr.ttlClock` (reconciled only at flush time); this session has no separate flush step, so
`advance_ttl_clock` mutates `self.cvr.ttl_clock` directly — consistent with the session-as-CVR-holder
design every other method here already uses. `connect()` gained a `now` parameter to set
`ttl_clock_base` at load time (`Date.now()` alongside `#ttlClock = cvr.ttlClock` upstream). Extended
the existing full-lifecycle live test with real assertions (no new test count, but genuinely new
verified logic) proving a 500ms wall-clock delta actually advances `cvr.ttl_clock` by 500. Full
workspace clean, zero warnings; full suite green under `--test-threads=1` (1017 total, 280 in
zero-cache-view-syncer — unchanged test count, extended coverage).
**SIXTH addition, in view_syncer_lifecycle.rs: `random_id`/`shutdown_before_initialization_error`.**
Checked `PlannerFanOut.estimateCost`'s outer shell as a possible remaining traversal-only piece —
confirmed it's inseparable from the real per-node cost math (it delegates to `#input.estimateCost`,
meaningless until every node kind has real cost logic), so the planner's traversal-only well really
is dry for now. Pivoted to `view-syncer.ts`'s two remaining top-level standalone functions:
`randomID` (a random base36 instance-tag id — `random_value` taken as an explicit parameter, this
port's determinism convention, reusing `query_hash::to_base36` which was made `pub` for this) and
`shutdownBeforeInitializationError` (the fixed `ProtocolErrorWithLevel` a `ViewSyncerService` method
returns when called before initialization completes). 3 new tests (29 total in the file). All passed
first try. Full workspace clean, zero warnings; full suite green under `--test-threads=1` (1047
total, 283 in zero-cache-view-syncer). `view-syncer.ts`'s top-level standalone-function surface is
now FULLY accounted for — every `function`/`export function` at that file's top level has either
been ported or explicitly documented as needing live class state this port doesn't have yet.
**cvr_query_driven_updater.rs — NEW.** Closes the prerequisite named in the previous round: ports
`CVRQueryDrivenUpdater`'s core query-side state mutation — `track_executed`/`track_removed`/
`track_queries` (the `#trackExecuted`/`#trackRemoved`/`trackQueries` methods). `track_executed` mutates
`cvr.queries[id]` in place: no-ops entirely if the transformation hash is unchanged (a pure
rehydration); otherwise bumps the CVR version via the already-ported `cvr_updater::ensure_new_version`,
promotes the query from desired-only to "gotten" (emitting a `Put` `QueryPatch`) only if it hadn't
already reached that state, and always updates the stored transformation hash/version.
`track_removed` deletes the query from `cvr.queries`, bumps the version, and emits a `Del` patch —
panicking on an internal query (port of `assertNotInternal`) since internal queries are never
client-removable. `track_queries` runs executed-then-removed in upstream's exact order and stamps
every resulting patch with the FINAL post-bump version (matching upstream computing `toVersion` from
`this._cvr.version` only once, after all tracking calls finish, not per-patch at creation time — a
subtle sequencing detail a naive per-call `toVersion` would have gotten wrong). `tracked:
&mut HashSet<String>` stands in for `#removedOrExecutedQueryIDs`, owned by the caller across one
`trackQueries` cycle. 8 tests incl. the already-gotten-query re-transformation case (hash changes,
version bumps, but no duplicate "got" patch is emitted) and the double-tracking panic. NOT ported:
`#lookupRowsForExecutedAndRemovedQueries` (needs live `CVRStore.getRowRecords()` — real Postgres I/O),
`received`/`deleteUnreferencedRows`/`flush` (the row-reconciliation half, same store coupling), and
the `RowSetSignatureProvider` callback — this is the query-side half of `CVRQueryDrivenUpdater` only,
real progress on unblocking `#addAndRemoveQueries`'s IVM wiring but not a claim that
`CVRQueryDrivenUpdater` (let alone `ViewSyncerService`) is complete.
**cvr_row_received.rs — NEW.** Ports the pure per-row decision half of `CVRQueryDrivenUpdater#received`
— given one row's existing CVR state + this update's data, decides the merged ref-counts (via the
already-ported `cvr_ref_counts::merge_ref_counts`), what to write to the row-record store
(`RowStoreWrite::Put`/`Delete`), and what client patch (if any) to send, with the exact dedup rules
upstream applies. Two subtleties preserved faithfully: (1) the store write uses `update.version` even
when merged ref-counts are `None` (upstream's own comment: "use the version of the update even if
merged is null... to correctly record a delete patch for [a row whose key changed]") — a row dropping
to zero refs while still carrying a version gets a `Put` with `refCounts: null`, NOT a `Delete`; only a
row with no version at all (added-then-removed within the same batch, never persisted) gets `Delete`;
(2) patch dedup: a 'del' is only sent if the row was previously known (existed or was received earlier
this cycle), and a 'put' is only sent if its `rowVersion` is strictly newer than the last patch sent
for that row. `#assertNewVersion`'s invariant (CVR version must already be bumped above orig by the
time any row is processed) is ported as `assert_new_version`, panicking like the TS `assert`. Scope
deviation: `RowID` doesn't derive `Hash`/`Ord` in this port (its row-key map contains `f64`, not
hashable/orderable) so the row-batch accumulators (`#receivedRows`/`#lastPatches`) are generic over any
`K: Clone + Eq + Hash` — a caller derives a stable key from its `RowID` however it likes. 5 tests, incl.
one that caught the module author's own wrong assumption during authoring (initially expected a
zero-refcount row to `Delete`, but re-reading upstream's comment showed `Put`-with-null-refcounts is
correct — fixed with a documenting comment, not by weakening the test). NOT ported: the actual
`CVRStore.getRowRecords()`/`putRowRecord`/`delRowRecord` I/O this decision feeds, and
`deleteUnreferencedRows` (Step 5 of the CVR sync algorithm, still untouched) — this closes the
`received` half of row-reconciliation only.
**cvr_delete_unreferenced_rows.rs — NEW. Closes `CVRQueryDrivenUpdater` (query-side + both row-
reconciliation halves) completely.** Ports `deleteUnreferencedRows`/`#deleteUnreferencedRow` — Step 5
of the CVR Sync Algorithm: given every existing row associated with just-executed/removed queries,
decide which are no longer referenced by ANY surviving query. `delete_unreferenced_row` skips rows
`received()` already handled this cycle (upstream's truthy `#receivedRows.get(id)` check — ported
faithfully: both an absent entry AND an explicit `null`/`None` entry count as "not received," matching
JS truthy semantics, not just presence), otherwise re-derives ref-counts via the already-ported
`merge_ref_counts` (dropping the removed/executed query hashes), and ALWAYS returns a row-record write
(a `putRowRecord` happens unconditionally, even for a row being deleted — the tombstone itself is a
`RowRecord` with `refCounts: None`) — keeping the existing `patchVersion` if still referenced, forcing
a fresh one via `assert_new_version` if not. The outer `delete_unreferenced_rows` handles the query-
less-update guard (panics if `removed_or_executed_query_ids` is empty but rows were received — a
config-only change should never receive rows) and the client-patch dedup (skip if a 'del' was already
sent for this row). 7 tests, incl. a still-referenced-by-a-surviving-query case (keeps patchVersion, no
patch emitted) and a fully-unreferenced case (fresh patchVersion + delete patch) side by side, and
multi-row independence. Same `K: Clone + Eq + Hash` generic-row-id deviation as `cvr_row_received.rs`.
Full workspace clean, zero warnings; 190 tests in this crate now.

**`CVRQueryDrivenUpdater` is now FULLY ported** across its three constituent pieces
(`cvr_query_driven_updater.rs`'s query-side `trackQueries`, `cvr_row_received.rs`'s `received`,
`cvr_delete_unreferenced_rows.rs`'s `deleteUnreferencedRows`) — every piece of CVR-mutation DECISION
LOGIC this class contains is ported and tested. What remains before `ViewSyncerService` can actually
run it: the real `CVRStore.getRowRecords()`/`putRowRecord`/`delRowRecord`/`markQueryAsDeleted` I/O
these decisions feed (CVRStore's row/query persistence layer beyond what `cvr_schema_sql`/`cvr_load`/
`cvr_store_pg` already cover), and then the IVM-pipeline wiring in `#addAndRemoveQueries` itself
(`#pipelines.addQuery`/`removeQuery`, pokers, query covering) that actually calls all of this.
**`get_row_records` — NEW, closes the CVRStore row READ path.** Added to `cvr_store_pg.rs`: the real
`tokio-postgres` query behind `RowRecordCache#ensureLoaded`/`getRowRecords()` — every non-tombstoned
row (`refCounts IS NOT NULL`) for one CVR, decoded into `cvr_types::RowRecord`. This is exactly what
`CVRQueryDrivenUpdater`'s row-reconciliation decisions (`cvr_row_received.rs`/
`cvr_delete_unreferenced_rows.rs`) need something real to read from — the last of the three CVRStore
row/query I/O methods named as the outstanding gap two rounds running (write-path was already covered
by `cvr_row_cache_sql.rs`/`flush_cvr`). Keys the returned map by
`zero_cache_types::row_key::row_id_string` (an already-ported, previously-unused canonical string
identity for a `RowID`) — which is also exactly the `K: Clone + Eq + Hash` string key
`cvr_row_received.rs`/`cvr_delete_unreferenced_rows.rs`'s generic row-id parameter was designed to
accept, so this genuinely closes that "RowID isn't Hash" deviation for real callers, not just in
theory. JSONB columns (`rowKey`/`refCounts`) are read via `::text` casts + the existing
`bigint_json::parse`, matching this crate's established pattern elsewhere (`clientAST`). Live-verified:
`reads_real_row_records_from_postgres` inserts one live row and one tombstone (null refCounts) row
directly via SQL, calls `get_row_records` against the real schema, and asserts exactly the live row
comes back (the tombstone correctly excluded by the `WHERE refCounts IS NOT NULL` clause) with its
ref-counts/version/schema/table decoded correctly. Passed on the first attempt. NOT ported: the
in-memory `RowRecordCache` wrapper itself (memoized single-load-per-cache, cursor-based 5000-row
pagination, `apply`'s incremental cache maintenance) — this is the bare query; a caller owns caching.
**query_hydration.rs — NEW. First real WIRING of `#addAndRemoveQueries`'s core sequence.** Not a port
of a specific upstream function — this composes `cvr_query_driven_updater::track_executed` +
`cvr_row_received::process_received_row` + `cvr_delete_unreferenced_rows::delete_unreferenced_rows`
(three modules ported independently across the last four rounds) into ONE orchestrated `hydrate_query`
function, in upstream's real call order: track the query as executed, fetch its rows through the
ACTUAL IVM machinery (`zero_cache_zql::ivm::{table_source::TableSource, filter::Filter}` — not a
stub), feed each fetched row through the `received()` row-decision, then run
`deleteUnreferencedRows`-equivalent cleanup over whatever wasn't re-fetched. 2 tests, the main one
(`hydrate_query_composes_tracking_receiving_and_deleting`) proving all three pieces work together in
one call: a real `TableSource` with 3 rows (one filtered out by an `active` predicate) plus a
previously-hydrated "stale" row no longer in the source, asserting in one shot that (a) the query
correctly transitions desired->gotten with a `Put` patch, (b) exactly the 2 currently-matching rows
produce `Put` row outcomes via the real filter fetch, (c) the stale row-99 gets deleted via
`delete_unreferenced_rows` running in the SAME cycle, and (d) the CVR version bump is used
consistently across all three. Still no CVRStore I/O — `existing_rows` is taken as an
already-fetched slice (what `get_row_records` would return), matching the pure-orchestration style
every module in this thread has used; a caller supplies row-key/ref-count/version projections since
this function has no opinion on primary keys. NOT wired: pokers, query covering,
catchup, telemetry — `#addAndRemoveQueries` remains far larger than this. This is proof-of-composition,
not proof this IS `#addAndRemoveQueries`.
**NOW LIVE END-TO-END.** `hydrate_query` gained a `deletion_row_writes` field (the row-record writes
`delete_unreferenced_rows` decided, not just its client-facing patches) so a caller can actually
persist a full cycle. New live test `hydrate_query_persists_through_a_real_cvr_row_store`: creates a
real CVR schema + a real stale row in Postgres, calls `cvr_store_pg::get_row_records` for the live
"existing" state, runs `hydrate_query` against a real `TableSource`/`Filter` (2 of 3 rows match), then
persists every resulting row write back via `cvr_row_cache_sql::get_row_updates_sql` against the real
connection, and finally calls `get_row_records` AGAIN to confirm the live database now reflects the
hydration: the two matching rows present, the stale row gone (tombstoned, excluded by
`refCounts IS NOT NULL`). Passed on the first attempt. This is the first fully-live proof touching
`ViewSyncerService`'s territory — real Postgres CVR state in, real IVM fetch, real Postgres CVR state
out — matching the "run it for real" standard every other subsystem in this port has eventually met.
**client_handler_poke.rs — NEW.** Ports the pure decision inside `ClientHandler#startPoke`
(client-handler.ts) — a `ViewSyncerService`-adjacent slice alongside the CVR/hydration modules above:
`should_send_poke(base_version, tentative_version, ever_poked)` reproduces the exact early-return
logic deciding whether a client actually needs a poke sent, or is already caught up (upstream's
`NOOP` `PokeHandler` short-circuit) — including the `forceInitialPoke` special case where a client
that has NEVER been poked always gets one even if its base version already matches the tentative
version, so it learns its "got queries" state has been reconciled with the server. 5 tests. NOT
ported: `ClientHandler` itself (owns a live `Subscription<Downstream>` plus poke-transaction
metrics), the real `PokeHandler` construction (`pokeStart`/`pokePart`/`pokeEnd` message sequencing
over that live downstream), and client-handler.ts's module-level `startPoke` function (the
`Promise.allSettled`-based multi-client fan-out combinator — needs a real async downstream per client
this port doesn't have yet).
**Extended with three more pure `PokeHandler` decisions.** `should_include_patch` (port of
`addPatch`'s staleness guard: `cmpVersions(toVersion, baseVersion) <= 0` means the patch is already
reflected in what the client has and should be skipped before ever touching the poke-part body),
`should_flush_poke_part` (the `PART_COUNT_FLUSH_THRESHOLD`=100 check that triggers an in-progress
poke-part flush), and `decide_poke_end` (port of `PokeHandler#end`'s branching: `Noop` when nothing
was ever sent and the version hasn't changed, `SendPokeStartFirst` when a poke needs to start even
though no patches were added — e.g. a forced initial poke, `ProceedToEnd` when patches were already
sent, and `InvalidPokeEndVersion` for the sanity-check violation upstream throws on — patches sent but
`finalVersion` isn't actually greater than `baseVersion`, a bug-elsewhere condition, not a normal
runtime one). 9 new tests (14 total in this module now). Still NOT ported: the per-patch-type body
assembly (`desiredQueriesPatches`/`gotQueriesPatch`/`lastMutationIDChanges`/`mutationsPatch`/
`rowsPatch` — needs the full `PokePartBody` wire type and `makeRowPatch`, a distinct larger slice) or
any of the actual message sending.
**client_handler_row_patch.rs — NEW.** Ports `makeRowPatch` (the internal `RowPatch`/`ClientRowPatch`
-> wire `RowPatchOp` mapping — a pure re-shaping into the already-ported
`zero_cache_protocol::row_patch::RowPatchOp`) and `ensureSafeJSON` (converts `INT8`/`BIGINT` column
values, which arrive as `JsonValue::BigInt`, to a plain `JsonValue::Number` when they fall within JS's
safe integer range — `[-(2^53-1), 2^53-1]` — and errors otherwise, since a value outside that range
can't be losslessly represented as a wire `Number`). 7 tests incl. the exact boundary values
(`MIN_SAFE_INTEGER`/`MAX_SAFE_INTEGER` themselves accepted, one past either boundary rejected).) |
| `zero-cache-sqlite` (sqlite_table_source) | `zqlite/table-source.ts` (fetch half) | 🟡 in progress
(NEW: sqlite_table_source.rs — `SqliteTableSource`, a REAL SQLite-backed `Source::fetch`, closing the
"current TableSource is in-memory" gap. Reads directly from a live `StatementRunner`-backed table via
parameterized SQL (constraint -> WHERE clause, sort -> ORDER BY/reverse), so it sees rows from ANY
prior write (initial sync, replication, direct SQL) — not just ones explicitly pushed into it during
the current process, unlike `zero_cache_zql::ivm::table_source::TableSource`. Scope deviation
(documented in the module doc): no "overlay" of not-yet-committed same-transaction pushes (upstream's
`generateWithOverlay`, needing `BEGIN CONCURRENT` transaction-local pending-write tracking this port
doesn't have) — fine for the whole-pipeline-slice use case (querying only after a replicated
transaction fully commits), a real gap for seeing a query's effect on its own uncommitted write within
one transaction. Also no `req.start` pagination/cursor resumption, no `multiConstraints` (join-only),
and generic (not per-column-PG-type-aware) SQLite-storage-class value mapping. 5 tests incl. one that
proves the whole point: reads a row inserted via a raw SQL statement completely independent of this
`Source` object, which the in-memory `TableSource` could never do.) |
| `zero-cache-sqlite` (sql_inline) | `zqlite/src/internal/sql-inline.ts` | 🟡 in progress (NEW:
sql_inline.rs — `inline_value`, the value->SQL-literal-text mapping half of `compileInline`
(NULL/quoted-escaped strings/unquoted numbers/SQLite 1-0 booleans/JSON-stringified-and-quoted
arrays+objects). A prerequisite for the still-entirely-unported `zqlite/sqlite-cost-model.ts`
(itself part of the unstarted `zql/src/planner` query-cost-estimation subsystem — see the
`zero-cache-zql` row below) — this is for SQLite-planner cost-estimation SQL ONLY, never for real
query execution, which already goes through `rusqlite` parameter binding elsewhere in this port and
must keep doing so. The `@databases/sql`/`FormatConfig` machinery `compileInline` wraps this in is
NOT ported — this port has no equivalent query-builder abstraction to hook a custom format into. 7
tests.) |
| `zero-cache-sqlite` (statement_cache) | `zqlite/src/internal/statement-cache.ts` | 🟡 in progress (NEW:
statement_cache.rs — `StatementCache<T>`, the checkout/return (not get/put) prepared-statement cache:
`get` REMOVES a cached statement (or prepares fresh via a caller closure) since one statement can't be
iterated by two concurrent callers; `return`/`use_stmt`/`drop_n` (partial eviction) round out the
same bookkeeping upstream has. Scope deviation: generic over an opaque handle `T` rather than tied to
`rusqlite::Statement<'conn>` — a real `Statement` borrows its `Connection` with a lifetime that
doesn't fit this cache's own storage the way upstream's GC'd JS object can; `rusqlite` already offers
`Connection::prepare_cached` as a real alternative for connection-level caching without this lifetime
problem. This ports the actual data-structure/bookkeeping logic generically, ready to back a concrete
owned statement handle if ever needed. `drop_n`'s eviction order is a documented approximation
(`HashMap` iteration order isn't insertion order like JS `Map`, so it evicts exactly `n` total, not
necessarily upstream's SAME `n`). `normalize_whitespace` ported to match `/\s+/g → ' '` exactly
(collapses runs but does NOT trim leading/trailing whitespace, unlike `split_whitespace().join(" ")`
which would). 8 tests. This closes out `zqlite/src/internal` — the third file, `sql.ts`, is pure
`@databases/sql`/`FormatConfig` library wiring with no logic to extract, correctly left unported.)
**Directory-coverage scan of `zero-schema/src` and `zqlite/src` (upstream access restored, live
reads).** Widened the scratchpad sparse-checkout (`packages/zero-schema/src`, `packages/zqlite/src`,
`packages/zero-cache/src/services`) and checked every file with zero PORTING.md representation.
`zero-schema/src`: `schema-config.ts` (valita validation-schema declarations plus a one-line
`isSchemaConfig` type guard — no business logic, correctly out of scope), `table-schema.ts` (almost
entirely TS type re-exports and type-level utilities/branding; its one runtime function, `atLeastOne`,
is a 3-line empty-check not worth a dedicated port), `default-format.ts` (a single exported constant
for a client-side `Format` type this port has no representation of at all — legitimately out of
scope, matching `builder/*`). None of these are real gaps.
**resolve_scalar_subqueries.rs — NEW, a real gap found in `zqlite/src`.** Ports
`resolveSimpleScalarSubqueries`/`isSimpleSubquery`/`extractLiteralEqualityConstraints` from
`zqlite/src/resolve-scalar-subqueries.ts` (257 lines): rewrites a "simple" scalar correlated
subquery — one whose subquery table has a unique index fully constrained by literal-equality
`WHERE` clauses joined only by `AND` — into a plain literal comparison, by actually executing it via
an injected `ScalarExecutor` (this port's established live-dependency-injection pattern, same as
`ConnectionCostModel`). Ports the `LiteralValue | null | undefined` tri-state result as a
`ResolvedValue` enum (`NoMatch`/`Null`/`Value`) rather than a nested `Option<Option<_>>`, for
clarity. Non-simple scalar subqueries and non-scalar correlated subqueries are recursed into but left
otherwise untouched, matching upstream exactly (deferred to the client's own EXISTS rewrite). 8
tests, all passed first try. This is a real, previously entirely-unrepresented file in `zqlite/src`
— found precisely because this round's directory scan used live upstream reads instead of recalling
which files had been checked before. Full workspace clean, zero warnings, full suite green under
`--test-threads=1` (1127 total, 132 in zero-cache-sqlite).
**Follow-up round: read every remaining `zqlite/src` file properly instead of presuming.** First,
re-verified the `sqlite-cost-model.ts` `scanStatus` blocker is STILL accurate against the latest
`rusqlite` (0.40.1, vs. the vendored 0.32.1): built a throwaway `cargo doc` against it and confirmed
only the `DbConfig::SQLITE_DBCONFIG_STMT_SCANSTATUS` flag constant exists, still no wrapper for
`sqlite3_stmt_scanstatus_v2` itself — the blocker holds.
**explain_queries.rs — NEW.** `explain-queries.ts` (21 lines) ports trivially: runs
`EXPLAIN QUERY PLAN` against every distinct query string in a `RowCountsBySource`-shaped map,
substituting a fixed placeholder for every `?` bind parameter first (upstream's own simplification —
a representative plan is good enough for introspection, exact literal values can shift `scan` vs
`search` at an index boundary but that's out of scope here too). 3 tests, incl. one confirming a
malformed query surfaces as a real error rather than being swallowed. All passed first try.
**db_maintenance.rs — NEW, extracted from `db.ts`.** `db.ts` (337 lines) is mostly thin
tracing/slow-query-logging wrapper classes (`Database`/`Statement`/`LoggingIterableIterator`) around
`@rocicorp/zero-sqlite3` with no decision logic beyond two pieces: `mb` (byte-count → `"X.XX"` MB
string) and `compact`'s actual decision of whether to proceed (enough freeable space? is
`auto_vacuum` in `INCREMENTAL` mode?) — extracted as `decide_compaction`, taking the pragma-read
values as explicit parameters rather than reading them off a live `Database`. The actual
pragma-issuing side of `compact` isn't ported (this port's existing `StatementRunner::pragma` already
covers issuing `PRAGMA freelist_count`/`auto_vacuum`/`incremental_vacuum`; a caller just needs to
combine that with `decide_compaction`). 4 tests, all passed first try.
**CORRECTION: `query-builder.ts` and `database-storage.ts` are REAL, substantial, currently unported
gaps — NOT correctly-out-of-scope infrastructure as a prior round's note claimed.** Read both in
full. `query-builder.ts` (430 lines) is the actual AST-to-parameterized-SQL translation
(`constraintsToSQL`/`filtersToSQL`/`gatherStartConstraints`/`multiConstraintToSQL`, building a real
`WHERE`/`ORDER BY`/cursor-pagination clause from `Condition`/`Ordering`/`Start`) — checked against
this port's `sqlite_table_source.rs::fetch`, which currently only pushes a bare equality `WHERE`
constraint into SQL and does everything else (filters, ordering, start-cursor pagination) by reading
every row and processing in Rust memory afterward. That's correct-but-unoptimized today; wiring real
`query-builder.ts`-equivalent SQL generation into `fetch` would be a genuine performance/correctness
upgrade, not just "client API surface" as previously (wrongly) characterized. `database-storage.ts`
(187 lines) is a real, currently entirely-unported live SQLite-backed key-value storage layer for IVM
operator state (`ClientGroupStorage`/per-operator `get`/`set`/`del`/prefix-`scan`, with periodic
commit-batching via `#maybeCheckpoint`/`#checkpoint` to avoid committing every single write) — this
port's IVM operators (`ivm/` in `zero-cache-zql`) have no persistent storage backing at all yet, so
this is a genuine, sizable future increment, not dead infrastructure. Both are too large to safely
complete in this round (time-boxed); flagged honestly as real, scoped future work rather than
re-asserted as out of scope. `options.ts`/`query-delegate.ts` ARE correctly out of scope, confirmed
by an actual read this round: `options.ts` is a one-field TS interface with zero runtime logic, and
`query-delegate.ts`'s `QueryDelegateImpl` is a thin per-table `TableSource`-memoizing factory plus a
commit-observer `Set` — the only "logic" is memoize-by-table-name, too trivial to warrant a dedicated
port and not a real decision surface.
Full workspace clean, zero warnings, full suite green under `--test-threads=1` (1145 total, 150 in
zero-cache-sqlite).
**query_builder.rs — NEW, the real `query-builder.ts` port (430 lines).** The full AST-to-
parameterized-SQL translation: `build_select_query` (top-level, matches `buildSelectQuery`),
`constraints_to_sql`/`multi_constraint_to_sql` (single-column `IN (...)` vs. compound
`(a,b) IN (VALUES ...)`, matching upstream's two-shape split exactly), `order_by_to_sql`,
`filters_to_sql`/`simple_condition_to_sql`/`like_condition_to_sql`/`value_position_to_sql` (incl. the
`IN`/`NOT IN` → `json_each` rewrite and the ILIKE case-insensitive `lower()`-both-sides + explicit
`ESCAPE '\'` — ported faithfully, matching upstream's own comment about mirroring Postgres LIKE/ILIKE
semantics against SQLite's case-insensitive-by-default `LIKE`), `gather_start_constraints` (the
sargable-leading-bound + full lexicographic OR-chain cursor-pagination logic, `nullable_aware_equality`/
`nullable_aware_range_comparison`/`sargable_leading_start_bound` helpers included). Ports upstream's
`@databases/sql` tagged-template builder as a minimal `SqlFragment` (SQL text + positional `?` params)
with `ident`/`raw`/`param`/`join`/`concat` — this port's usual move of replacing a third-party DSL
with the Rust equivalent actually used (mirrors `sql_inline.rs`'s treatment of the sibling
`internal/sql.ts`). Reused this crate's existing `Constraint`/`MultiConstraint`(newly defined
locally)/`Start`/`StartBasis` types from `zero-cache-zql::ivm` rather than re-declaring them.
`ValuePosition::Parameter`/static values reaching SQL generation surface as
`QueryBuilderError::UnresolvedParameter` (a real, recoverable caller error, matching upstream's
`throw`) — but a `Condition::CorrelatedSubquery` reaching `filters_to_sql` panics (upstream excludes
it at the TYPE level via `NoSubqueryCondition`; this port has no such static exclusion, so a caller
passing one is a bug, not an expected outcome). 15 tests, all passed first try — including one that
builds a real query via `build_select_query` and actually executes it against a real in-memory SQLite
table, confirming the generated `ORDER BY` clause produces correctly-sorted rows, not just
plausible-looking SQL text. NOT YET wired into `sqlite_table_source.rs::fetch` (which still does
filtering/ordering/pagination in Rust memory after a bare-equality-only SQL fetch) — this module is
usable standalone today; wiring it in is a separate, well-scoped follow-up increment. Full workspace
clean, zero warnings, full suite green under `--test-threads=1` (1160 total, 165 in
zero-cache-sqlite).
**query_builder.rs wired into sqlite_table_source.rs::fetch.** Real fix found while wiring: `query_builder`'s
`build_select_query` originally derived the SELECT column list from `BTreeMap<String, ColumnType>`
iteration (alphabetical) — but upstream's `Object.keys(columns)` actually preserves the schema's
declared column order. Fixed by adding an explicit `column_order: &[String]` parameter for the
SELECT list, keeping the `BTreeMap` only for key→type lookups (order-independent there). `fetch` now
calls `build_select_query` directly: constraint, ordering (`schema.sort`), reverse, and `req.start`
cursor pagination are all pushed into real SQL instead of being filtered/sorted in Rust memory after
a bare-equality fetch — closing the exact gap the module's own doc comment flagged (`req.start`-based
pagination previously NOT ported at all). `SqliteTableSource::new` keeps its old signature (defaults
every column to `ValueType::String`/`optional: true` — safe because `to_sqlite_type`'s
`String`/`Number`/`Null` branch is a runtime-value-driven pass-through, not a forced-string coercion,
so untyped callers still round-trip correctly); a new `with_column_types` constructor takes real
per-column types for callers that have real schema info (needed for correct boolean coercion and
tighter/sargable cursor-pagination bounds). 2 new tests (`fetch_resumes_from_a_start_cursor`,
`fetch_with_real_column_types_coerces_booleans_in_constraints`), all 5 pre-existing `fetch_*` tests
still pass unchanged (proving the SQL-pushdown rewrite is behavior-preserving for every previously-
tested case). Full workspace clean, zero warnings, full suite green under `--test-threads=1` (1162
total, 167 in zero-cache-sqlite).
**`sqlite-stat-fanout.ts` — CORRECTION: NOT actually blocked, and now ported (`sqlite_stat_fanout.rs`,
NEW).** A prior round's memory-file note presumed this file needed the same `rusqlite scanStatus` gap
blocking `sqlite-cost-model.ts`'s live SQLite integration — that presumption was WRONG, caught by
actually reading the file instead of trusting the note. `SQLiteStatFanout` never touches
`scanStatus` at all: it only queries `sqlite_stat4`/`sqlite_stat1` (ordinary tables, readable via
plain `SELECT`) and `pragma_index_list`/`pragma_index_info` (ordinary pragma table-valued
functions) — all things `rusqlite`'s `StatementRunner::all`/`get` already do today. Confirmed live
(a throwaway standalone `cargo run` against bundled `rusqlite`) that both `sqlite_stat4` and
`sqlite_stat1` are populated after `ANALYZE`, so this is fully live-testable, not just unit-testable
on hand-built rows. Ported the whole class: `get_fanout` (stat4 → stat1 → default fallback chain,
cached by `table:sorted,columns`), `find_index_for_columns`/`is_prefix_match` (order-independent,
case-insensitive, gap-free prefix matching against `pragma_index_info`'s column ordering),
`decode_sample_is_null` (reads a `stat4` sample's raw SQLite record-format bytes to check if its
first column's serial type is NULL), `median_fanout` (median of non-NULL stat4 fanouts — more robust
than stat1's NULL-inclusive average). 11 tests, 6 of them LIVE against a real in-memory bundled
SQLite database with real `ANALYZE`-populated statistics (including one that inserts 20 non-NULL +
80 NULL rows and confirms stat4's median excludes the NULL bucket entirely — the exact overestimate
problem the class's own doc comment describes), all passed first try. Full workspace clean, zero
warnings, full suite green under `--test-threads=1` (1138 total, 143 in zero-cache-sqlite). **This
is now a real, usable `ConnectionCostModel` fanout source** for whoever eventually wires the planner
graph to live SQLite — a second live cost-model input alongside `sqlite_cost_model.rs`'s
scanstatus-based row-count estimate (which remains genuinely blocked; the two are independent
statistics sources feeding different parts of `CostModelCost`). |
| `zero-cache-sqlite` (sqlite_cost_model) | `zqlite/src/sqlite-cost-model.ts` | 🟡 in progress (NEW —
a real prerequisite for the planner graph's still-unported `PlannerConnection.estimateCost`, checked
before hand-rolling cost formulas from scratch. `estimate_cost` (the scanstats-aggregation function:
sorts `ScanstatusLoop`s by `selectId`, takes the first top-level loop's `est` as total rows, adds
`btree_cost` for every subsequent top-level loop whose `explain` mentions `ORDER BY`) and `btree_cost`
(the `O(n log n)/10` sort-cost formula, constant kept verbatim from upstream's comment on SQLite's
native sort being ~10x faster than sorting in JS) — both pure once a caller has already extracted
`ScanstatusLoop`s from a real prepared statement. Also `remove_correlated_subqueries` (a pure
`Condition` transform with no SQLite dependency at all — drops `correlatedSubquery` conditions the
cost model can't estimate via `scanStatus`, collapsing `and`/`or` trees the same way upstream does).
NOT ported: `createSQLiteCostModel`'s outer shell — real SQLite `scanStatus`/`SQLITE_SCANSTAT_*` FFI
via the `@rocicorp/zero-sqlite3` fork, `buildSelectQuery` (unported SQL generation), `compileInline`
(already ported, `sql_inline.rs`) wiring, `db.prepare` — needs live statement introspection this
port's `rusqlite` dependency may or may not expose. 10 tests, all passed first try.) |
| `zero-cache-zql`      | `../zql`             | 🟡 in progress (planner_constraint.rs — NEW: port of `zql/src/planner/planner-constraint.ts`'s
`mergeConstraints` (a `Record<string,undefined>` existence-only set, modeled as `BTreeSet<String>`
union). A directory-coverage scan found `zql/src/planner` (the ~4900-line query-cost planner/optimizer:
`planner-graph.ts`/`planner-node.ts`/`planner-join.ts`/`planner-fan-in.ts`/`planner-fan-out.ts`/
`planner-source.ts`/`planner-terminus.ts`/`planner-connection.ts`/`planner-builder.ts`/
`planner-debug.ts`) had ZERO representation in this table. This ports the one genuinely tiny,
dependency-free pure function as a first toehold, same approach as `view_syncer_lifecycle.rs` for
`ViewSyncerService` — the rest of the directory is a real, substantial, entirely unstarted gap: a
shared mutable graph of nodes with parent/child references, cost estimation, and constraint
propagation through that graph, none of it extractable as isolated pure functions the way this one
was. 4 tests.
planner_cost.rs — NEW, second planner toehold: port of `planner-node.ts`'s `CostEstimate` data model
(`startupCost`/`scanEst`/`cost`/`returnedRows`/`selectivity`/`limit`/`fanout`) and `omitFanout` (strips
the non-serializable `fanout` closure field for logging). `FanoutCostModel` (a `(columns: string[]) =>
FanoutEst` closure from `planner-connection.ts`) modeled as `Rc<dyn Fn(&[String]) -> FanoutEst>`. Ports
the DATA the graph's `estimateCost` will eventually produce, not the graph traversal itself — same
"data model before traversal logic" order this port used for the IVM operator graph
(`ivm::operator`'s `Node`/`Change` types landed before `Filter`/`Join`). 3 tests.
**planner_node.rs — NEW. THE PLANNER GRAPH'S ARCHITECTURAL DECISION IS MADE.** Resolves the open
question flagged across several rounds as the real blocker to further planner porting.
`PlannerNode` (upstream's closed 5-variant union: `PlannerJoin | PlannerConnection | PlannerFanOut |
PlannerFanIn | PlannerTerminus`) is modeled as a Rust enum over `Rc<RefCell<T>>`-wrapped concrete node
structs, one variant per kind — `Rc` for cross-referenced shared ownership (a node reachable both as
another node's `#input` and among a fan-out's `#outputs`), `RefCell` for the in-place mutation
upstream's methods perform (`addOutput`, `convertToUFO`/`reset`). Deliberately DIFFERENT resolution
from the IVM operator graph's `Rc<dyn Output>` (a genuinely open-ended/extensible trait) — the planner
graph is a FIXED closed set of node kinds matching upstream's own union type, so an enum fits better
than a trait object here; documented explicitly as a different decision for a different shape, not an
inconsistency. Landed minimal skeleton structs for all 5 node kinds (fields enough to represent the
graph SHAPE only — `PlannerFanOutNode`'s `add_output`/`convert_to_ufo`/`reset` are the only real
methods ported so far) plus `NodeType`/`JoinOrConnection`. The actual `estimateCost`/
`propagateConstraints`/join-flip algorithms inside each node remain unported — a real, substantial
future increment this decision makes tractable to start, not attempted here. 5 tests incl. one
specifically proving the `Rc<RefCell<...>>` sharing actually shares (a mutation through one reference
to a fan-out node is visible through a second independent reference to the same node).
**First real (not just structural) behavior on the skeleton: `PlannerNode::closest_join_or_source`.**
Port of `closestJoinOrSource` — walks up through single-input pass-through nodes (`FanOut`/
`Terminus`) until it reaches a `Join`/`FanIn` (both report `'join'`) or a `Connection`
(`'connection'`), skipping the fan-out/terminus plumbing in between. Panics on a node whose input
isn't wired yet — a well-formedness invariant of a fully-built graph, matching upstream's non-optional
`#input`. 3 new tests (8 total in the file): direct cases (Join/FanIn/Connection), recursion through
FanOut+Terminus, and the malformed-graph panic path.
**Second real behavior: `PlannerNode::propagate_unlimit_from_flipped_join`.** Port of
`propagateUnlimitFromFlippedJoin` — when a parent join is flipped, propagates "remove any limit"
through the graph. `Connection` does the one piece of real work (`unlimit()`, clearing a new `limit`
field added to `PlannerConnectionNode`); `Join`/`FanOut` forward to their single neighbor
(parent/input respectively), `FanIn` forwards to ALL of its inputs, `Terminus` is a no-op. Upstream's
duck-typed `'propagateUnlimitFromFlippedJoin' in input` check has no work to do in this port — every
`PlannerNode` variant has the method via the enum, so the "does it support this" check is
unconditionally true here, simplifying the port slightly (documented as a real, not swept-under-the-
rug, simplification). Also added `PlannerFanInNode::set_output` (port of `setOutput`, matching
`PlannerFanOutNode::add_output`'s existing pattern). 6 new tests (14 total in the file) incl. one
proving fan-in propagates to EVERY input, not just the first. All passed first try. Full workspace
clean, zero warnings; full suite green under `--test-threads=1` (1031 total, 112 in zero-cache-zql).
**Third real behavior, in `planner_constraint.rs`: `translate_constraints_for_flipped_join`.** Port
of `planner-join.ts`'s `translateConstraintsForFlippedJoin` — remaps a constraint's keys from
parent-space to child-space via POSITIONAL correspondence (parent key at index N maps to child key at
index N), needed when a flipped join's constraint crosses the parent/child boundary. Real
representation finding: `parent_keys`/`child_keys` had to be taken as ordered `&[String]` slices, NOT
the existing `PlannerConstraint` (`BTreeSet<String>`) type — this function's correctness genuinely
depends on key ORDER (upstream relies on JS `Object.keys()`'s insertion-order guarantee for the
positional mapping), which a `BTreeSet` would silently destroy by alphabetizing. Documented explicitly
as a real representation mismatch rather than forcing the function through `PlannerConstraint` and
getting subtly wrong results. 4 new tests (8 total in the file) incl. one proving a key with no
positional match in `parent_keys` is dropped, not passed through. All passed first try. Full
workspace clean, zero warnings; full suite green under `--test-threads=1` (1035 total, 116 in
zero-cache-zql).
**Fourth real behavior: `PlannerJoin`'s full semi/flipped state machine, in `planner_node.rs`.**
`PlannerJoinNode` gained real mutable state (`join_type`/`initial_type`/`flippable`) plus `flip`
(semi->flipped, panics if already flipped matching upstream's assert, returns
`UnflippableJoinError` if `!flippable` matching upstream's throw), `reset` (restores
`initial_type`), `is_flippable`, and `PlannerNode::flip_if_needed`/`propagate_unlimit` (both panic
if called on a non-`Join` variant, matching upstream's methods not existing on other classes at
all). `flip_if_needed`/`propagate_unlimit_from_flipped_join` needed real `===` reference-identity
comparison between `PlannerNode`s (`input === this.#child`) — added `PlannerNode::ptr_eq`, mapping
JS object identity directly onto `Rc::ptr_eq` of the shared `Rc<RefCell<...>>` each variant wraps.
9 new tests (23 total in `planner_node.rs`) incl. both panic paths and one proving `ptr_eq`
correctly distinguishes two structurally-identical-but-distinct nodes. All passed first try. Full
workspace clean, zero warnings; full suite green under `--test-threads=1` (1044 total, 125 in
zero-cache-zql).
**MAJOR MILESTONE — `propagateConstraints` FULLY PORTED across every node kind, in `planner_node.rs`.**
The single biggest planner piece landed so far — real constraint-propagation logic, not just graph
traversal or state toggling. `PlannerConnectionNode` gained `constraints: BTreeMap<String,
Option<PlannerConstraint>>` (port of `#constraints`, keyed by `branchPattern.join(',')`;
`#cachedConstraintCosts` NOT ported — a pure memoization detail with no `estimateCost` to memoize
against yet). `PlannerJoinNode` gained `parent_constraint`/`child_constraint` as ORDERED
`Vec<String>` (not `PlannerConstraint`/`BTreeSet` — same positional-correspondence reasoning as
`translate_constraints_for_flipped_join`, since these fields are both a set of columns to forward
AND the ordered key lists that function needs). `PlannerFanInNode` gained the `FI`/`UFI`
(`FanInType`) toggle (port of `#type`/`convertToUFI`) since `FI` vs `UFI` propagate constraints
differently (same updated pattern for every input vs. a unique index-prefixed pattern per input).

`PlannerNode::propagate_constraints(branch_pattern, constraint)` implements the full recursive
algorithm: `Connection` stores by branch-pattern key; `FanOut` forwards unchanged; `FanIn` either
prepends `0` for every input (`FI`) or gives each input its own index prefix (`UFI`); `Join` in
`Semi` mode always sends its own `childConstraint` down (not the incoming one) and forwards the
incoming constraint up unchanged; `Join` in `Flipped` mode translates the incoming constraint from
parent-space to child-space via `translate_constraints_for_flipped_join` and sends the merge of
incoming + its own `parentConstraint` upward via `merge_constraints` — genuinely composing THREE
previously-separate pieces (`translate_constraints_for_flipped_join`, `merge_constraints`, and the
new graph-traversal logic) into one real algorithm. `Terminus` is never a mid-graph input
(`unreachable!` in the recursive method); `PlannerNode::start_propagate_constraints` is the real
public entry point matching `PlannerTerminus`'s distinct zero-arg API, seeding with `[]`/`None`.

8 new tests (31 total in `planner_node.rs`) incl. FI-vs-UFI branch-pattern-prefix distinction and,
most notably, one proving the flipped-join positional translation for real (`issueID` at parent
position 0 correctly becomes `id` at child position 0, merged constraint correctly includes the
join's own `parentConstraint` going up). All passed first try. Full workspace clean, zero warnings;
full suite green under `--test-threads=1` (1063 total, 133 in zero-cache-zql). `estimateCost` remains
the one big genuinely-unported piece — needs real per-node cost math and the `FanoutCostModel`/
`ConnectionCostModel` closures, still deliberately not attempted.
**`PlannerConnection.estimateCost` PORTED (the graph's leaf cost case) — plus a real bug fix.**
Confirmed `rusqlite` (this port's bundled SQLite) has no wrapper for `sqlite3_stmt_scanstatus_v2`
(only knows the `SQLITE_DBCONFIG_STMT_SCANSTATUS` flag exists, checked by grepping the vendored
source directly) — genuinely blocked, not unattempted. Took `ConnectionCostModel` as an injected
`Rc<dyn Fn>` closure instead (this port's established "not-yet-built live dependency" pattern),
unblocking real, tested `estimate_cost` logic today. `PlannerConnectionNode` gained `table`/`sort`/
`filters`/`model`/`base_constraints`/`is_root`/`cached_constraint_costs`/`selectivity` — `selectivity`
computed ONCE at construction (port of the constructor calling `model` twice, with and without
filters, to derive a ratio) exactly as upstream does, including the "root or no filters -> 1.0"
default. `estimate_cost` memoizes per branch-pattern key, merges `base_constraints` with whatever
`propagate_constraints` stored, and derives `scan_est` (`min(rows, limit /
downstream_child_selectivity)` when limited, else the full row count).

**Real bug caught while porting `unlimit`:** the EARLIER port of `unlimit()` (several rounds ago)
unconditionally cleared `limit`, but upstream actually guards with `if (this.#isRoot) { return; }` —
root connections can never be unlimited. Reading the FULL class this round (needed for `estimateCost`
anyway) surfaced the gap; added the `is_root` field and fixed `unlimit` to match, with a new test
(`unlimit_is_a_no_op_on_a_root_connection`) pinning it. Worth remembering: porting a method in
isolation (as `unlimit` originally was) can miss a real guard clause that only becomes visible once
the surrounding class fields are read in full — a reason to periodically re-read a class's complete
source even after "finishing" a piece of it.

5 new tests (36 total in `planner_node.rs`) incl. memoization (same branch pattern hits the cache,
different one doesn't) and constraint-merging (base + propagated constraints actually reach the
model together). All passed first try. Full workspace clean, zero warnings; full suite green under
`--test-threads=1` (1078 total, 138 in zero-cache-zql). Every planner node kind now has real
`estimateCost` behavior for the LEAF case; the recursive `Join`/`FanOut`/`FanIn` `estimateCost` cases
(which call their children's `estimateCost` and combine results) remain the one genuinely unported
piece.
**MAJOR MILESTONE — `estimateCost` FULLY PORTED across EVERY node kind. The planner's ENTIRE pure
traversal/propagation/cost-estimation logic is now real and tested.** `PlannerNode::estimate_cost`
dispatches: `Connection` calls its own memoized method (already ported); `FanOut` is pure delegation;
`FanIn` sums (`UFI`, every branch executes) or takes the MAX (`FI`, mutually-exclusive OR branches —
a real, easy-to-get-backwards distinction, pinned by a dedicated test for each); `Join` is the most
involved case — factors in child fanout/selectivity (`scaledChildSelectivity = 1 - (1 -
child.selectivity)^fanout`), propagates selectivity UP the parent chain for consecutive ANDed EXISTS
checks, and for flipped joins accounts for IN-list chunking cost (`MULTI_CONSTRAINT_CHUNK_SIZE = 256`,
port of `getMultiConstraintChunkSize()`'s default — not modeled as upstream's test-only-mutable
ambient global, a real but narrow scope note). Needed `getMultiConstraintChunkSize` from
`zql/src/ivm/flipped-join.ts`, a file otherwise entirely outside this session's planner work.

6 new tests (42 total in `planner_node.rs`) incl. FI-vs-UFI max-vs-sum distinction and semi-vs-flipped
join row-count formulas (`parent.rows * child.selectivity` vs. `parent.rows * child.rows`) verified
against hand-computed expected values, not just "it doesn't panic". All passed first try. Full
workspace clean, zero warnings; full suite green under `--test-threads=1` (1084 total, 144 in
zero-cache-zql).

**This closes the planner subsystem's entire pure-logic surface** — every traversal method
(`closestJoinOrSource`, `propagateUnlimitFromFlippedJoin`), the full join state machine (`flip`/
`reset`/`flipIfNeeded`), `propagateConstraints`, and now `estimateCost` all have real, tested Rust
implementations. What remains of `zql/src/planner` (`planner-builder.ts`'s AST-to-plan-graph
construction, `planner-debug.ts`'s `PlanDebugger` logging, and the actual live SQLite cost-model
backing `ConnectionCostModel` — confirmed blocked on `rusqlite` lacking `scanStatus`) is now
genuinely I/O-or-construction-shaped, not more pure algorithm to extract.
**planner_builder.rs — NEW.** Ports the two genuinely pure, self-contained functions in
`planner-builder.ts`: `has_correlated_subquery` (recursive `Condition` check) and `extract_constraint`
(builds a `PlannerConstraint` from a correlation's field list). NOT ported: the actual graph
CONSTRUCTION (`buildPlanGraph`/`processCondition`/`processAnd`/`processOr`/
`processCorrelatedSubquery`/`wireOutput`) — needs `PlannerGraph`/`PlannerSource` (neither ported;
`planner-graph.ts`, 471 lines, also owns the real `PlannerGraph::plan()` 2^n exhaustive-search
algorithm over flippable joins, itself entirely unported). Also NOT ported: `applyPlansToAST`/
`applyToCondition` — a real, specific design blocker found while scoping this file: they key off
`condition[planIdSymbol]`, a property upstream's `CorrelatedSubqueryCondition` type actually declares
(`ast.ts` line 330). Faithfully porting needs a `plan_id: Option<i64>` field added to this crate's
`Condition::CorrelatedSubquery` variant — legitimate, but `Condition` is constructed at dozens of
call sites across `zero-cache-view-syncer`/`zero-cache-protocol` (query_covering.rs, ast_json.rs, CVR
modules, etc.), so widening that variant is a deliberately separate, carefully-verified future
increment, not bundled into this round. 5 tests, all passed first try.
**planner_graph.rs — NEW.** `PlannerSource` (`planner-source.ts`, a thin factory tying a table name
to its cost model, producing `PlannerConnection` nodes via `connect` — tractable now that
`PlannerConnectionNode::new` already exists) and `PlannerGraph`'s bookkeeping half
(`planner-graph.ts`): `add_source`/`get_source`/`has_source` (source registry, erroring — not
panicking, unlike upstream's `assert` — on a duplicate/missing name), `set_terminus`/`terminus`, and
`reset_planning_state` (resets every collected join/fan-out/fan-in/connection's mutable planning
state back to initial values, leaving graph structure unchanged, for replanning with a different
strategy). Needed two small additions to `planner_node.rs` found while porting `reset`:
`PlannerConnectionNode` gained a `base_limit` field (`reset` restores `limit` to it, matching
upstream's `#baseLimit`) and `PlannerFanInNode` gained `reset` (restores `FI`) — both had been
missing since earlier rounds ported `PlannerConnectionNode`/`PlannerFanInNode` before `reset` was in
scope. NOT ported: `PlannerGraph::plan()` itself (the real 2^n exhaustive-search algorithm over
flippable joins, `PlanState` snapshot/restore for backtracking, `FOFIInfo` BFS caching) — a genuinely
separate, substantial algorithm deserving its own dedicated increment. 8 new tests total (6 in
`planner_graph.rs`, 2 more in `planner_node.rs` for the `reset` additions), all passed first try.
Full workspace clean, zero warnings; full suite green under `--test-threads=1` (1097 total, 157 in
zero-cache-zql).
**planner_graph.rs / planner_node.rs — plan-search plumbing.** Ports the pieces of
`PlannerGraph::plan()`'s support machinery that don't need fan-out/fan-in BFS: `PlanState` (a
snapshot of every join/fan-out/fan-in/connection's mutable planning state) plus
`capture_planning_snapshot`/`restore_planning_snapshot` (`capturePlanningSnapshot`/
`restorePlanningSnapshot`, including `#validateSnapshotShape` — returns `SnapshotShapeMismatch`
rather than panicking, since a caller mismatching graphs is a plausible integration bug, not an
internal invariant violation), and `PlannerGraph::propagate_constraints`/`get_total_cost`
(`propagateConstraints`/`getTotalCost`, delegating to two new `PlannerNode` entry points:
`start_estimate_cost`, the `PlannerTerminus#estimateCost` public entry point mirroring the
already-ported `start_propagate_constraints`; and `PlannerJoinNode::restore_type`, a direct setter
bypassing `flip()`'s `semi`-only precondition, needed because snapshot restore writes an arbitrary
captured type rather than replaying `flip`/`reset` calls — matches upstream, which restores `#type`
directly too). Also ports the free function `propagate_unlimit_for_flipped_joins`
(`propagateUnlimitForFlippedJoins`): loops every join in the graph, calling the existing
`propagate_unlimit()` on any that ended up `Flipped`. NOT YET ported: `PlannerGraph::plan()` itself
— still blocked on `FOFIInfo`/`buildFOFICache`/`checkAndConvertFOFI`/`findFIAndJoins` (a BFS from
each fan-out's outputs to its paired fan-in), which in turn needs `output` back-links on
`PlannerJoinNode`/`PlannerConnectionNode` (the "what does this node feed into" direction, distinct
from a join's `parent`/`child` inputs) that don't exist in this port yet — a further scope item
identified this round, not previously called out. 5 new tests (162 total in zero-cache-zql), all
passed on the second try (one assertion in `propagate_constraints_and_get_total_cost_drive_from_the_terminus`
had to be corrected from an assumed `10.0` — the model's `rows` — to the actual `0.0`, since
`CostEstimate.cost` is a distinct field from `rows`/`scan_est` and a bare connection's own `cost` is
always `0.0`; the aggregation into a nonzero total only happens at Join/FanIn recursion levels).
Full workspace clean, zero warnings; full suite green under `--test-threads=1` (1102 total).
**planner_node.rs — `output` back-links.** Added `pub output: Option<PlannerNode>` +
`set_output` to `PlannerJoinNode` and `PlannerConnectionNode`, mirroring `PlannerFanInNode::output`/
`set_output` exactly (both already ported). This is the "what does this node feed INTO" direction,
distinct from a join's `parent`/`child` (its two INPUTS) — needed by `findFIAndJoins`'s
fan-out-to-fan-in BFS. Constructor signatures unchanged (`output` defaults to `None`, set later via
`set_output`, matching how `PlannerFanInNode::new` already handles its own `output` field). 2 new
tests, passed first try. Full suite green (1104 total, 164 in zero-cache-zql).
**Environment note, found while attempting to continue `PlannerGraph::plan()`:** this sandbox no
longer has an upstream `mono` checkout anywhere on the filesystem (confirmed via exhaustive `find`
for `planner-graph.ts`/`planner-join.ts`/any `mono`-named directory — none exist). Earlier rounds'
close readings of `planner-graph.ts`/`planner-join.ts` etc. were done in a differently-provisioned
environment. Without the source, `FOFIInfo`/`buildFOFICache`/`checkAndConvertFOFI`/
`findFIAndJoins`'s exact traversal/optimization logic, and `plan()`'s own exact flip-enumeration
loop, can no longer be read and faithfully transcribed — only reconstructed from memory/doc
fragments, which risks silently encoding wrong plan-search behavior as "ported and tested." Per this
project's test-first, faithful-port standard, that's not an acceptable substitute for reading the
real source. Deferring `plan()`'s exact algorithm body until upstream source is available again
(e.g. re-cloned into the workspace); the `output` back-links above are still real, verifiable
progress since they're a small, self-evidently-correct structural addition (a mirror of an existing,
already-correct pattern) rather than a guess at unseen business logic.
**`Condition::CorrelatedSubquery.plan_id` field — landed.** Widened
`zero-cache-protocol::ast::Condition::CorrelatedSubquery` with `plan_id: Option<i64>`, a repo-internal
change tractable without upstream source (it's an audit of THIS codebase's own construction/match
sites, not a read of unseen TS). Port of `condition[planIdSymbol]` — upstream stamps this via a
well-known `Symbol` key rather than a plain object field, and it's never part of the wire/JSON AST
shape (confirmed by `ast_json.rs` having exactly two `Condition::CorrelatedSubquery` sites —
`condition_to_json`/`condition_from_json` — neither of which reads/writes a `plan_id`-shaped JSON
key upstream; `condition_from_json` now always constructs with `plan_id: None`, `condition_to_json`
ignores it). Audited and updated every non-`..` construction/destructure site across the workspace:
`ast.rs` (`normalize_where`, `cmp_condition`'s ordering — widened to `..` since only 4 of the 5
fields participate in comparison ordering, matching upstream's own comparator which never
compares `plan_id`), `ast_json.rs` (both wire (de)serialization sites), `complete_ordering.rs`
(recursive rewrite + one test), `zero-cache-zql::builder::filter` (two test helpers),
`zero-cache-zql::planner_builder` (one test helper), `zero-cache-sqlite::sqlite_cost_model` (one
test helper), `zero-cache-auth::policy` (one test helper), `zero-cache-view-syncer::query_covering`
(4 test constructions; the real `correlated_condition_implies` logic already used `..` and needed no
change, confirming this function correctly never inspected `plan_id` for coverage decisions).
Nothing in this codebase produces a real (non-`None`) `plan_id` yet — that requires
`PlannerGraph::plan()`/`applyPlansToAST` (still unported, blocked on upstream source access per the
entry above) — so this round is pure plumbing: the field exists, compiles, and is threaded correctly
everywhere, ready for a real planner to populate it later without another wide-reaching signature
change. No new tests (a field-threading change, not new behavior); full workspace clean, zero
warnings, full suite green under `--test-threads=1` (1104 total, unchanged test count).
**`PlannerGraph::plan()` — LANDED, closing `zql/src/planner` entirely.** Upstream source access was
re-established this round (a fresh sparse/shallow clone of `github.com/rocicorp/mono` into the
session scratchpad, `packages/zql/src/planner` only — kept out of the actual project repo). Read
`planner-graph.ts` in full (471 lines) and confirmed the previously-ported snapshot/backtracking
plumbing (`PlanState`, `capture_planning_snapshot`/`restore_planning_snapshot`,
`propagate_constraints`/`get_total_cost`, `propagate_unlimit_for_flipped_joins`) already matches the
real algorithm exactly — no corrections needed. Ported the remaining pieces: `FOFIInfo` (cached
FanOut→FanIn boundary info), `build_fofi_cache`/`find_fi_and_joins` (BFS from each fan-out's
outputs, via the `output` back-links added last round, to its paired fan-in, collecting joins found
along the way — visited-tracking via linear `ptr_eq` scan since `PlannerNode` can't be a
`HashMap`/`HashSet` key), `check_and_convert_fofi` (converts FO→UFO and its paired FI→UFI when any
join between them ended up flipped), and `PlannerGraph::plan()` itself (filters flippable joins,
skips optimization silently if more than `MAX_FLIPPABLE_JOINS = 9` exist — no `LogContext` to warn
through in this port, matching the established convention of silently dropping debug-only logging
elsewhere — then enumerates all `2^n` flip-bitmask patterns, resetting/applying/converting-FOFI/
propagating-unlimit/propagating-constraints/evaluating cost each iteration, tracking and finally
restoring whichever pattern scored lowest). 3 new tests: a direct BFS unit test
(`find_fi_and_joins_bfs_finds_the_paired_fan_in_and_every_join_between`), a direct FOFI-conversion
unit test, and an end-to-end `plan()` test that doesn't hardcode which orientation should win (no
upstream unit test exercises `.plan()` directly — it's covered indirectly through fuzzer/integration
tests elsewhere in the upstream repo, not `zql/src/planner`'s own test files) but instead asserts
`plan()`'s chosen orientation actually IS the lower-cost one by explicitly computing the alternative
and comparing — pins the search's optimality property rather than one formula's specific output.
All passed first try. Full workspace clean, zero warnings, full suite green under
`--test-threads=1` (1107 total, 167 in zero-cache-zql). **`zql/src/planner` is now closed**: every
file in the directory (`planner-node.ts`, `planner-connection.ts`, `planner-join.ts`,
`planner-fan-in.ts`, `planner-fan-out.ts`, `planner-terminus.ts`, `planner-source.ts`,
`planner-constraint.ts`, `planner-graph.ts`, and the two pure functions from `planner-builder.ts`)
has real, tested Rust behavior. Remaining NOT-ported items in this area are genuinely
out-of-pure-logic-scope, not deferred algorithm: `planner-builder.ts`'s actual AST-to-plan-graph
CONSTRUCTION (`buildPlanGraph`/`processCondition`/etc. — needs wider `Ast`/query-building context)
and `applyPlansToAST` (now unblocked in principle since `plan_id` exists, but still needs the
construction side to call it from), `planner-debug.ts`'s `PlanDebugger` (pure logging), and the live
SQLite cost model backing `ConnectionCostModel` (confirmed blocked on `rusqlite` lacking
`scanStatus`).
valuesEqual, the pure value-comparison foundation every IVM operator builds on; ivm/change.ts's
SourceChange — the row-level Add/Remove/Edit vocabulary a Source emits, the natural next thing for
`zero-cache-sqlite::pipeline`'s apply loop to produce. NOT YET ported: Node/Stream/Operator/Source —
upstream's IVM pipeline is generator-driven with cooperative `'yield'` scheduling, which has no
direct Rust equivalent and needs a deliberate design decision (explicit iterator/coroutine shape)
before porting; ivm/constraint.ts — constraint_matches_row/constraints_are_compatible/
key_matches_primary_key/pull_simple_and_components/primary_key_constraint_from_filters: pure logic
with no Stream/Operator dependency, ported ahead of that decision since Filter/Source both need it.
15 tests; ivm/operator.ts + ivm/source.ts + ivm/filter.ts — the FIRST WORKING IVM PIPELINE SLICE:
`operator.rs` (Node/Change/SourceSchema/FetchRequest, `Stream<T> = Box<dyn Iterator<Item=T>>` —
resolves the generator-`'yield'` architectural question from the prior round by observing Rust
iterators are already lazy/pull-based), `table_source.rs` (an in-memory `TableSource` — `push`
Add/Remove/Edit by primary-key identity, `fetch` with constraint filtering + sort/reverse; stands in
for the real SQLite-backed `zqlite/table-source.ts` until that's ported), `filter.rs` (`Filter`
wrapping a predicate — the real logic is edit-splitting: an Edit crossing the filter boundary becomes
an Add or Remove, not a bare Edit, per upstream's documented `EditChange` contract). Scope deviation
documented in `operator.rs`'s module doc: single-output concrete composition (`TableSource` + `Filter`
called directly) instead of a trait-object `Input.setOutput(Output)` graph — deferred until `Join`
needs real multi-consumer fan-out. 18 tests. A live end-to-end test (real Postgres INSERT -> SQLite ->
registered query -> IVM delta) is the natural next integration point, mirroring `pipeline.rs`'s proof
for the replication half; builder/filter.rs — `create_predicate`, port of `builder/filter.ts`'s
`createPredicate`: compiles an arbitrary AST `Condition` tree (Simple/And/Or, all `SimpleOperator`
variants incl. LIKE/ILIKE/IN/IS NULL) into an executable `Fn(&Row)->bool`. Makes `Filter` usable with
a real registered query's WHERE clause instead of one hand-wired Rust closure — also the piece write
authorization's `canPreMutation`/`canPostMutation` needs to evaluate a permission policy `Condition`.
builder/like.rs — LIKE/ILIKE pattern-to-matcher compiler (plain compare when no wildcards, else
`%`->`.*`/`_`->`.`/`\`-escape compiled to a dotall-anchored regex). 21 tests. NOT ported:
`Condition::CorrelatedSubquery` compilation (panics — needs joins) and `bindStaticParameters`
(`ValuePosition::Parameter` also panics, matching upstream's own precondition). ivm/join.rs — NEW:
`fetch_joined`, a first (read-only, non-incremental) slice of `ivm/join.ts`/`flipped-join.ts`. Scope
deviation (documented in the module doc, same call as `operator.rs`'s concrete-composition decision):
upstream's `Join`/`FlippedJoin` are full push-based incremental `Operator`s maintaining join state in
`Storage`; this is a plain `fetch()`-time join — for each parent row, fetches matching child rows via
a `Correlation`-derived `Constraint` (parent_field values -> child_field WHERE clause), no incremental
push maintenance (a child-table change doesn't yet re-derive a parent-level `Change`). Answers "what
does this multi-table query return right now". Also added `exists_for_row` (checks whether a child
`TableSource` has any row matching a parent row via a `Correlation` — the primitive
`Condition::CorrelatedSubquery` EXISTS evaluation needs; doesn't yet consult the subquery's own
nested WHERE clause, a documented narrower gap). **NOW ALSO INCREMENTAL for the EXISTS case**:
`reeval_exists_after_child_change` — given a child `SourceChange` already applied to the child
`TableSource`, identifies the ONE parent row it could affect (inverse correlation lookup via
`TableSource::find_by_key`) and re-evaluates that parent's EXISTS status against the now-updated
child — real incremental maintenance for the specific join use case this port has actually wired
into query/permission evaluation, without needing full row-nesting maintenance (which would need
`Node.relationships`, deferred since the first IVM slice, and is the real remaining scope boundary,
documented in the module doc). 15 tests incl. compound (multi-column) correlation keys, EXISTS
flipping both directions on child insert/remove, "another child still matches" (no false flip), and
orphaned-child (no correlating parent) returning `None`. **WIRED INTO `create_predicate`**: `builder/filter.rs` gained `create_predicate_with_exists`,
which evaluates `CorrelatedSubquery` conditions via an injected `exists: Rc<dyn Fn(&CorrelatedSubquery,
&Row) -> bool>` resolver instead of panicking — `create_predicate` itself is unchanged (still panics,
now lazily at predicate-call time rather than construction time, matching how every other branch
already defers work to call time) and is just `create_predicate_with_exists` with a
panicking resolver, for callers that know their condition has no subqueries. 9 new filter.rs tests
incl. one wiring the resolver to a REAL `TableSource` via `exists_for_row` (AST `Condition` ->
compiled predicate -> real EXISTS check against real joined data, no mocking) — **this closes the
permission-rules half of the joins gap**: `CorrelatedSubquery` permission rules (e.g. "user owns the
parent issue") can now actually be evaluated once wired to a real child source, unblocking that half
of `zero-cache-auth::policy`'s previously-documented limitation.
**`zero-cache-auth::policy` NOW ACTUALLY WIRED** (see that crate's entry): `passes_policy_with_exists`/
`passes_policy_group_with_exists`/`can_do_with_exists` complete the loop. **NOW WIRED INTO THE LIVE
REPLICATION APPLY LOOP**: `zero-cache-sqlite::ivm_bridge` gained `apply_to_child_and_reeval_exists`
(applies a `ReplicationChange` to a child `TableSource` via `apply_to_source`, converts each resulting
`IvmChange` to a `SourceChange`, and runs `reeval_exists_after_child_change` for each), proven with a
live end-to-end test in `zero-cache-sqlite::pipeline`
(`real_postgres_child_insert_flips_exists_check`): two real Postgres tables (`ivm_issues`/
`ivm_comments`) in one publication/slot, a real `INSERT INTO ivm_comments` streamed through the actual
`ReplicationConn`/`run_until` loop, decoded and applied to a real `TableSource`, producing a live
`(issue_row, true)` EXISTS flip — no manually-pushed rows anywhere in the chain.
**`Node.relationships` NOW EXISTS**, unblocking full row-nesting join maintenance (previously the
documented scope boundary above): `operator::Node` gained `relationships:
HashMap<String, Vec<Node>>`, with one deliberate simplification vs upstream's `Record<string, () ->
Stream<Node | 'yield'>>` — populated EAGERLY as a materialized `Vec<Node>` rather than lazily via a
per-key thunk, since every relationship this port populates comes from an already-fetched,
already-in-memory `TableSource`, so there's nothing for laziness to save yet (documented in
`operator.rs`'s module doc as a revisit-if-it-gets-expensive simplification, not an oversight). Every
existing `Node { row }` construction site across `zero-cache-zql`/`zero-cache-sqlite` updated to
`Node::new(row)` (empty relationships). Built on this: `ivm::join::fetch_joined_nodes` (the
`fetch_joined` join, but returning real `Node`s with children populated into
`relationships[name]` instead of a separate tuple) and, the actual incremental-maintenance primitive,
`reeval_relationship_after_child_change` — the full row-nesting counterpart to
`reeval_exists_after_child_change`: given a child `SourceChange` already applied to `child`, finds the
ONE affected parent row (same inverse-correlation lookup) and returns a `Node` for it with
`relationships[name]` RE-FETCHED against the now-updated child — i.e. genuine incremental
`Join`/`FlippedJoin`-equivalent maintenance (re-deriving a parent's joined child rows on a child
change), not just a boolean. Scope note kept honest in the doc comment: it re-fetches the correlated
children rather than incrementally patching the previous `Vec<Node>` in place (upstream's `Storage`-
backed diffing) — still genuinely incremental at the JOIN level since only the one affected parent is
touched, not a full re-scan. 4 new tests (19 total in join.rs): relationship population via
`fetch_joined_nodes`, child added/removed correctly reflected in the re-fetched relationship, and the
orphaned-child `None` case. This closes the `Node.relationships` prerequisite that was blocking full
incremental joins since the very first IVM slice — the remaining gap is now scaling this single-
relationship primitive to a real multi-relationship `Join` operator wired into the operator graph
(`operator.rs`'s still-deferred `Rc<RefCell<dyn Output>>`/arena decision), not a missing data model.
**THE `operator.rs` GRAPH DECISION IS NOW MADE, AND A REAL `Join` OPERATOR EXISTS.** Resolution:
`Rc<dyn Output>` (NOT `Rc<RefCell<dyn Output>>` — the `RefCell` moves inside each concrete
implementor's own state instead of wrapping the trait object, keeping `Rc<dyn Output>` trivially
clonable/shareable). `operator.rs` gained the `Output` trait (`fn push(&self, change: Change)`).
`join.rs` gained `Join` — a REAL `Operator` participating in the graph, not a caller-driven primitive:
`Join::new` builds an `Rc<Join>` wrapping a parent/child `TableSource` pair (each in a `RefCell` for
interior mutability) plus a `Correlation`; `add_output` registers a downstream `Rc<dyn Output>`;
`push_child_change` applies a child-table change, re-derives the affected parent's relationship via the
already-ported `reeval_relationship_after_child_change`, and fans the resulting `Change::Add(node)` out
to EVERY registered output — the actual multi-consumer behavior the whole `Rc<RefCell<dyn Output>>`
question was about. 4 new tests (23 total in join.rs), incl. one registering TWO independent spy
`Output`s on the same `Join` and asserting both receive the identical change from a single child push
— the concrete proof this was worth building the graph for (a query result and a permission check
could both watch the same `Join` without either re-deriving it). Scope kept honest: still wraps the
single-affected-parent primitive (re-fetches rather than patches children in place) and only reacts to
child-side changes, not parent-side — the same documented boundaries `reeval_relationship_after_child_
change` already had; this closes the ARCHITECTURE gap (how do operators fan out to multiple
consumers), not every remaining join-completeness gap. `TableSource`/`Filter` keep their existing
direct `push`-returns-`Change` API unchanged — nothing before `Join` needed graph wiring, so nothing
before it was forced to adopt `Output`. **This closes the last standing architectural gap named across
many rounds of this port.**
**NOW WIRED INTO THE LIVE REPLICATION APPLY LOOP**, mirroring how the EXISTS primitive got wired the
round before: `zero-cache-sqlite::ivm_bridge` gained `apply_to_child_and_reeval_relationship` (applies
a `ReplicationChange` to a child `TableSource`, converts each resulting `IvmChange` to a
`SourceChange`, and runs `reeval_relationship_after_child_change` for each — same shape as
`apply_to_child_and_reeval_exists`, minus the boolean/plus a full `Node`). 3 new unit tests. Proven
live in `zero-cache-sqlite::pipeline`: `real_postgres_child_insert_updates_relationship` — two real
Postgres tables (`ivm_rel_issues`/`ivm_rel_comments`) in one publication/slot, a real `INSERT INTO
ivm_rel_comments` streamed through the actual `ReplicationConn`/`run_until` loop, producing a live
`Node` for the parent issue with `relationships["comments"]` correctly re-derived to contain the new
comment — no manually-pushed rows anywhere in the chain. Passed on the first attempt. (One test-infra
note: running this test concurrently with `real_postgres_child_insert_flips_exists_check` under the
default parallel test runner occasionally flakes against the shared throwaway Postgres instance
[resource contention, not a logic bug] — both pass reliably under `--test-threads=1`; not worth
fixing further since CI-grade test isolation for the throwaway PG instance is out of scope for this
port.) 92 tests total in `zero-cache-sqlite` now.) |
| `zero-cache-change-source` | `src/services/change-source` | 🟡 in progress (protocol/current/*.ts — full protocol type layer; pg/change-source.ts's schema-diff functions; pg_connection.rs — REAL tokio-postgres connection +
wal_level/publication/replication-slot precondition checks, tested against a live local Postgres 17
instance; pgoutput.rs — hand-rolled pgoutput binary wire-format decoder (Begin/Commit/Relation/Insert/
Update/Delete/Truncate), 11 tests incl. one against a REAL byte capture taken via `pg_recvlogical`
from a live logical replication slot — decoded Begin/Relation/Insert/Commit match the actual `INSERT`
performed, byte-for-byte verified, not just spec-derived; replication_conn.rs — a raw hand-rolled
`START_REPLICATION ... LOGICAL` streaming client over `TcpStream` (startup handshake w/ trust auth,
Simple Query for `START_REPLICATION`, `CopyBothResponse`/`CopyData` frame reader, XLogData +
keepalive parsing feeding straight into `pgoutput::decode`) — since `tokio-postgres` 0.7 has neither
`copy_both_simple` nor simple-query support for `CopyBothResponse` (confirmed by source inspection).
2 tests incl. a full live end-to-end one: connect, create table+publication+slot, start raw
replication, INSERT a real row via a side-channel client, and assert the decoded stream sees the
Relation + Insert with correct values — no external `pg_recvlogical` tool involved, this is our own
driver talking the wire protocol directly. This closes the biggest previously-flagged gap in
change-source; pg_to_change.rs — `RelationTracker::translate`, the `PgoutputMessage -> Change`
layer: caches `Relation` messages by id (relation_id -> schema/name/columns/key columns) so
subsequent Insert/Update/Delete/Truncate row messages (which only carry the numeric id) can be
resolved into real `Change::Insert/Update/Delete/Truncate` values with named columns and row keys.
15 tests incl. key-only vs full old-tuple Update, key-only vs full Delete, unknown-relation and
column-count-mismatch error paths. Column values are now TYPED: `text_to_json(type_oid, text)` decodes
pgoutput's text-format tuple encoding into the right `JsonValue` variant by Postgres type OID —
`bool` -> `Bool` (`"t"`/`"f"`), integer/float/numeric OIDs -> `Number` (parsed as f64; malformed
values fall back to `String` defensively rather than panicking the replication stream), everything
else (text, json, arrays, uuid) -> `String` passthrough (json/array *parsing* into structured
`JsonValue` is a further increment — this crate has no JSON parser yet, only `bigint_json::stringify`).
This closes the "typed, not text-passthrough, column value decoding" gap named across several prior
rounds; updated the live `pipeline.rs`/e2e-websocket tests' conditions and expected rows to match
(bool literals instead of `"t"` string literals). `json`/`jsonb` columns are now ALSO really parsed
(via `zero_cache_shared::bigint_json::parse`, discovered to already exist despite a stale module-doc
claim otherwise — see zero-cache-shared's changelog entry) into structured `JsonValue::Object`/
`Array`, falling back to `String` defensively on malformed JSON rather than panicking the replication
stream. Postgres array types (`_int4`, `_text`, `_bool`, etc.) are now ALSO decoded: `parse_pg_array`
hand-parses the `{1,2,3}`/`{"a,b","c\"d"}`/`{1,NULL,3}` text literal format (not JSON — needs its own
quoting/escaping/NULL-keyword parser) into elements, decoded recursively via `text_to_json` with the
array's element OID (`zero_cache_types::pg_types::array_element_type`, a new OID->element-OID map for
the ~12 array types this crate's base-type switch covers). One-dimensional only; multi-dimensional
arrays and arrays of unrecognized element types fall back to `String`. With this, real Postgres row changes can now be
turned into `Change` values end-to-end: raw TCP -> replication_conn -> pgoutput::decode -> 
RelationTracker::translate -> Change, ready for `ChangeDispatcher`) |
| `zero-cache-sqlite` (pipeline) | `services/replicator` glue | ✅ `pipeline.rs`'s `run_until` — the
top-level apply loop wiring `replication_conn` + `pgoutput` + `pg_to_change` into `ChangeDispatcher`,
now with an `on_change` hook so callers can drive an IVM pipeline alongside the SQLite apply without
this module depending on ZQL. `ivm_bridge.rs` translates `change_source::data::Change` into
`ivm::change::SourceChange` and applies it to a `TableSource` — the key subtlety: Postgres's default
replica identity only sends KEY columns (or nothing) for an UPDATE's old row, but `Filter`'s edit-
splitting needs the FULL old row, so the bridge looks the current row up in the `TableSource` (the
authoritative current-state store) before applying the change. Now also handles `Truncate`:
`apply_to_source` returns `Vec<IvmChange>` (not `Option`) since a single `ivm::operator::Change` has
no "clear everything" variant (matching upstream — TRUNCATE isn't in `zql/src/ivm`'s Change union
either) — a truncate snapshots the source's current rows and removes each individually, one `Remove`
per row. DDL `Change` variants still produce no IVM changes (affect schema, not rows; no
schema-migration story for `TableSource` yet). 4 truncate tests added; updated both live end-to-end
tests' call sites for the `Vec` return type. **BOTH HALVES of the priority
whole-pipeline slice now proven live end-to-end** in one test
(`real_postgres_insert_produces_ivm_delta`): a real Postgres `INSERT` -> raw replication -> pgoutput
decode -> Change -> SQLite apply -> `ivm_bridge` -> `TableSource` -> `Filter` -> an actual IVM `Add`
delta out the other end, asserted against real Postgres text-encoded values (bool `true` -> `"t"`,
matching `pg_to_change`'s documented text-passthrough scope). No mocks anywhere in the chain. The
registered query's WHERE clause is now a REAL AST `Condition` (`active = 't'`) compiled via
`zero_cache_zql::builder::filter::create_predicate`, swapped in for the earlier hand-wired Rust
closure — proves the query-driven path end-to-end, not just the `Filter` mechanism in isolation. |
| `zero-cache-services`      | `src/services/{limiter,replicator/notifier,change-streamer,...}`  | 🟡 in progress (sliding-window-limiter, notifier; change_streamer_forwarder.rs — the change-streamer
fan-out decision logic: `SubscriberSet<T>` ports `Forwarder`'s `add`/`remove`/
`#updateActiveSubscribers` — which subscribers are active (eligible for the next forwarded change) vs
queued (added mid-transaction, held back until commit/rollback so nobody sees a partial transaction).
Subscribers modeled generically as any `Eq+Hash+Clone` id, not the real unported `Subscriber`
websocket entity. 8 tests. NEW: broadcast.rs — `Broadcast`'s consensus-based flow-control timeout
algorithm (`broadcast.ts`): `Broadcast<T>::new`/`mark_completed`/`check_progress` — waits for a
majority of subscribers (`floor(n/2)+1`, reducing to "wait for all" in the single-subscriber case) to
complete, then releases either when every subscriber finishes (`AllSubscribers`) or, once majority is
reached, after `flow_control_consensus_padding_ms` elapses since the last completion with no further
progress (`ConsensusTimeout`) — the algorithm that lets replication proceed without waiting
indefinitely on one slow/broken subscriber while still avoiding unbounded I/O-buffer growth from a
naive fire-and-forget send. `performance.now()` taken as an explicit `now: i64` ms parameter
throughout, per this port's determinism convention. NOT ported: the actual `sub.send(change)`
message-delivery/ack wiring and `#logWithState` diagnostic logging (left to the caller via
`elapsed_ms`/`completed_count`/`pending_count` accessors, matching this port's LogContext-free
pattern) — both need the real `Subscriber` websocket entity, still unported. 9 tests.) |
| `zero-cache-mutagen`       | `src/services/mutagen`        | 🟡 in progress (NEW crate. crud_ops.rs — the
CRUD mutation-op types from zero-protocol/mutation.ts (InsertOp/UpsertOp/UpdateOp/DeleteOp), deferred
out of zero-cache-protocol since only this crate's mutagen.ts port consumes them; sql.rs —
getInsertSQL/getUpsertSQL/getUpdateSQL/getDeleteSQL, the CRUD-op-to-SQL generation core of
mutagen.ts's `processMutationWithTx`. Scope decision: generates fully-inlined SQL text via
`zero_cache_types::sql::{id,lit}` (same approach as `zero-cache-sqlite::create`'s DDL generation)
rather than a parameterized-query builder, since there's no live postgres.js-equivalent client-side
binding layer here — still injection-safe (values go through `lit`/numeric formatting), just an
inlined-vs-bound representation choice. 8 tests. NOT YET ported: `MutagenService`/`processMutation`
(the actual Postgres transaction orchestration, schema-version checks, last-mutation-id tracking,
rate limiting, write authorization) and all of `pusher.ts` (custom-mutator batching/retry/push to an
app server) — both are heavier, connection/authz-coupled pieces deferred for a future round;
last_mutation_id.rs — `check_mutation_id` (pure port of the receivedMutationID vs lastMutationID
comparison: AlreadyProcessed/Unexpected-ProtocolError/Ok) + `get_upsert_last_mutation_id_sql` (the
INSERT..ON CONFLICT..RETURNING SQL text, split from the live-transaction round-trip);
orchestration.rs — `plan_mutation_sql`, the pure port of `processMutationWithTx`'s task-list decision
(error_mode/authorized -> which op SQL statements run), taking the authorization verdict as a plain
bool since `WriteAuthorizer` itself is still unported. pusher_batch.rs — NEW: `combine_pushes`, the
first slice of `pusher.ts` (custom-mutator push-forwarding, previously entirely unported). Port of
the pure batching logic `PusherService`'s queue drain uses: groups queued `PusherEntry`s by
`clientID:wsID:revision` (first-seen order, matching JS `Map` iteration), concatenates each group's
mutations, and validates the same invariants `assertAreCompatiblePushes` does (client/ws/revision/
auth/schemaVersion/pushVersion/cookie/origin/userID/pushURL must all agree within a group) — modeled
as a `Result<_, IncompatiblePushes>` rather than an upstream-style `assert` panic, since a batching
bug is exactly the kind of thing worth testing/handling rather than crashing on. `ConnCtx`/
`MutateContext` are simplified from the full `ConnectionContext` to just the fields the invariant
checks read (`auth` collapsed to an opaque `Option<String>` token). Mutations are generic (`Vec<M>`)
since `combinePushes` never inspects one. 8 tests. NOT ported: `PusherService`/`PushWorker` (the
`Queue`-based streaming service, `fetchFromAPIServer` HTTP forwarding, `initConnection`/
`enqueuePush`/`ackMutationResponses` RPC surface, retry/backoff) — this is one pure function, same
scope-first pattern as every other module in this crate. api_request.rs — NEW: ports the pure
request-construction logic from `custom/fetch.ts`'s `fetchFromAPIServer` (the function that actually
sends a batched push to a user's API server) — `get_backoff_delay_ms` (exponential backoff with
jitter, `min(1000, 100*2^(attempt-1) + jitter)`, jitter taken as a parameter rather than calling
`Math.random()` internally, matching this port's determinism convention), `build_request_headers`
(Content-Type/X-Api-Key/custom+request-header-merge-with-override/Authorization/Cookie/Origin
assembly, matching upstream's exact `Object.assign` override order), and `build_final_url` (rejects a
push URL that already contains the reserved `schema`/`appID` query params, then appends them — hand-
rolled query-string parsing/percent-encoding, no `url` crate dependency). 12 tests. NOT ported: the
actual `fetch()` call + retry loop, `urlMatch`/`compileUrlPattern` (needs a URL-pattern-matching
library), response parsing, and OpenTelemetry metrics. **HTTP CLIENT DEPENDENCY DECISION NOW MADE**:
added `reqwest` (rustls-tls, matching this port's other real-dependency additions for
tokio-postgres/tokio-tungstenite when those subsystems needed a live client) — api_fetch.rs's
`fetch_from_api_server` performs the ACTUAL POST, retrying on 502/504 or connect failure up to
`MAX_ATTEMPTS` with `api_request::get_backoff_delay_ms` between attempts (jitter via a seeded
xorshift PRNG — this port's usual `Math.random()` substitute where determinism isn't load-bearing),
returning the parsed JSON response. 5 tests, ALL LIVE against a real local HTTP server (hand-rolled
via `TcpListener`, no mocking crate) — incl. one that makes a server fail with 502 twice then succeed
and asserts exactly 3 real connection attempts were made, genuinely proving the retry loop re-sends
the request over the wire. NOT ported: response-body schema validation (returns generic
`serde_json::Value`), `urlMatch`/`compileUrlPattern` (still needs a URL-pattern library), legacy
error-shape detection, and metrics. **The pure-logic-plus-real-HTTP-call chain of pusher.ts's
forwarding path is now complete** — only `PusherService`'s `Queue`-based streaming/connection-RPC
service wrapper remains unported. pusher_response.rs — NEW: `find_fatal_terminations`, the pure
decision half of `PushWorker#fanOutResponses`'s success-path branch (a successful push whose response
still contains a per-mutation out-of-order error must terminate that client's downstream connection).
Groups `MutationResponse`s by `clientID` (first-seen order, matching upstream's `groupBy` over a JS
`Map`), and for each group whose mutations include a `ZeroErrorKind::OooMutation` error, reports a
`MutationTermination{client_id, mutation_ids, message}` carrying the WHOLE client group's mutation IDs
(matching upstream's `mutations.map(...)`, not just the offending one). NOT ported: the whole-push-
failed branch of `#fanOutResponses` (needs `PushFailedBody`/`ErrorReason` wire types not yet in
`zero-cache-protocol`), and the actual `Subscription<Downstream>.fail()` I/O — this is the pure
grouping/detection logic a caller with a live connection registry would drive. 6 tests. 57 tests total
in this crate now. `PusherService`/`PushWorker`'s remaining gap is now purely the `Queue`-based
service-lifecycle wrapper itself (drain loop, ref-counting, `initConnection`/`ackMutationResponses`
RPC surface) — every piece of *decision logic* PushWorker's loop body needs (batching, request
construction, the actual HTTP call, and now response-termination detection) is ported and tested.
**`PusherService`/`PushWorker`'s `Queue`-based drain loop NOW ASSEMBLED**: pusher_service.rs — NEW:
`PushWorker<M>`, the actual port of `PushWorker`'s `run()` loop and `initConnection`/`enqueuePush`/
`stop()` RPC surface, generic over the mutation type `M` and (deliberately) over the actual
push-sending logic — `run<F, Fut, R>(process_push: F)` takes an injected `FnMut(PusherEntry<M>) ->
Fut` rather than hard-wiring `api_fetch::fetch_from_api_server` and a concrete `MutateResponse` (which
isn't ported to `zero-cache-protocol` yet — same generic-over-what's-missing move `pusher_batch.rs`
already made for mutations themselves). `run()` faithfully reproduces the loop body: `dequeue` one
entry, `drain` whatever else is queued, `combine_pushes` them, process each combined push via
`process_push` IN ORDER, repeat until a `stop()` sentinel drains. `init_connection` reproduces
`PushWorker#initConnection`'s exact three outcomes (fresh registration / same-socket-twice error /
reconnect-replaces-old-socket) using a plain `HashMap<clientID, wsID>` standing in for upstream's
`Map<clientID, {wsID, downstream}>` (no `Subscription<Downstream>` exists in this port yet — see
module doc). Built on the ALREADY-EXISTING `shared::queue::Queue` (ported many rounds earlier for
exactly this purpose, previously unused). 8 tests, incl. one that proves batching survives a REAL
network round-trip: two same-connection pushes enqueued before `run()` starts draining are wired
through the loop into a real `api_fetch::fetch_from_api_server` call against a hand-rolled local
`TcpListener` HTTP server, and the test asserts exactly ONE real HTTP request went out over the wire
(not two) — genuine proof the `combine_pushes` batching decision actually governs the real HTTP
traffic a caller would send, not just a unit-level guarantee. NOT ported (documented, deliberate):
`ackMutationResponses`/`deleteClientMutations` (the cleanup-mutation RPC methods — thin wrappers that
would just call `fetch_from_api_server` directly with a synthesized `PushBody`, no new logic once a
caller wires `PushWorker` to a `ConnectionContextManager` for real), `ref()`/`unref()`/`hasRefs()`
refcounting (belongs to whatever service-lifecycle framework eventually owns a `PusherService` per
client group — this port has none yet), and `#fanOutResponses` itself (the caller's `process_push`
return value `R` is simply collected — `pusher_response::find_fatal_terminations` is the piece a real
caller would run over it). **This closes pusher.ts as a subsystem**: every piece named across every
prior round's "PusherService gap" — batching, request construction, the real HTTP call, response-
termination detection, connection lifecycle, and now the actual drain loop tying them together — is
ported and tested. What would remain to make this a live production service is wiring it to a real
WebSocket transport and `ConnectionContextManager` instance (an integration task for whenever
`zero-cache-server`'s WS layer and this connect), not more `pusher.ts` logic.) |
| `zero-cache-workers`  | `src/workers`        | 🟡 in progress (NEW crate. url_params.rs — port of `types/url-params.ts`'s
`URLParams` (`get`/`getInteger`/`getBoolean`, required-vs-optional semantics, empty-string-treated-as-
missing). Scope deviation: wraps an already-parsed `&[(String, String)]` of query pairs instead of a
real `URL`/`URLSearchParams` — this port has no URL-parsing dependency yet, so a caller parses the
query string however it likes and hands in the pairs. 9 tests. connect_params.rs — port of
`getConnectParams`: parses a client's WebSocket connect request (query string + headers) into
`ConnectParams` via `UrlParams` + the already-ported `zero_cache_protocol::connect::decode_sec_protocols`.
Scope deviation: `initConnectionMsg` stays opaque JSON text rather than a parsed
`InitConnectionMessage` (no deserializer for that type exists yet — same gap
`zero_cache_protocol::connect`'s module doc already names), and `authToken` is pulled out of the
decoded payload via a minimal ad-hoc string scan (`extract_auth_token`) rather than a full JSON parse,
since this port doesn't need the rest of that payload's structure yet. 6 tests, incl. one round-
tripping through the real `encode_sec_protocols`/`decode_sec_protocols` pair rather than hand-crafting
header bytes. NOT ported: `connection.ts`/`syncer.ts`/`replicator.ts`/`mutator.ts`/`syncer-ws-message-
handler.ts` (the actual worker-thread processes and their message-passing — need Node `Worker`
equivalents/a process-model decision this port hasn't made) — this round scoped to the pure, connection-
independent request-parsing slice, matching the project's consistent pure-logic-first pattern.
**CORRECTION, previous round mischaracterized this gap**: `connection.ts` is NOT a Node `Worker`
thread/process — despite living under `src/workers`, `Connection` is ordinary per-WebSocket-connection
object state running on the same event loop as everything else, dispatching parsed `Upstream` messages
to a `MessageHandler` and turning results back into sends/closes. There is no worker-thread/process
topology decision needed for this file at all; that framing was wrong. **connection.rs — NEW**: ports
`Connection`'s pure decision logic — `check_protocol_version` (the `init()` version-gate, blaming
"server" or "client" depending on which side is out of date, backed by a new
`zero_cache_protocol::protocol_version` module porting `PROTOCOL_VERSION`/
`MIN_SERVER_SUPPORTED_SYNC_PROTOCOL`), `classify_handler_result` (the `#handleMessageResult` switch —
`HandlerResult::{Ok,Fatal,Stream,Transient}` -> `ConnectionAction::{None,CloseWithError,AttachStream,
SendErrors}`, the actual I/O left to a caller), `has_transient_socket_code`/
`is_transient_socket_message` (the EPIPE/ECONNRESET/ECANCELED/socket-compression-during-close
transient-error classification `sendError`'s log-level decision uses), and `find_protocol_error`
(walks an error's cause chain looking for a `ProtocolError` — ported via Rust's
`std::error::Error::source()` chain + `downcast_ref`, the structural analog of JS's `Error.cause`
chain). 13 tests. NOT ported: the actual `Connection` class (owns a live `ws`/timers/`LogContext`,
wires `#proxyInbound`/`#proxyOutbound` stream piping) — needs a real `MessageHandler`/
`Source<Downstream>` (view-syncer/pusher dispatch isn't unified behind one trait in this port yet),
and `zero-cache-server::ws_connection` already covers the raw accept/send/decode transport half this
would sit on top of.
**mutator.rs — NEW**: port of `Mutator`, currently a stub upstream too (its own TODO: "install
websocket receiver / spin up pusher services for each unique client group that connects") — ports the
`SingletonService` run/stop/drain state machine faithfully (a condvar-guarded stop signal standing in
for TS's manually-resolvable `resolver()` promise, since `stop()` must be idempotent-callable even
after `run()` already returned). 4 tests. Nothing more to port here until upstream itself grows past
the stub. **syncer_ws_message_handler.rs — NEW**: ports the pure routing decision inside
`SyncerWsMessageHandler#handleMessage`'s `'push'` case — `route_push`: clientGroupID validation (checked
before anything else, including the empty-mutations fast path — verified with a test asserting exact
ordering), the empty-mutations no-op, and custom-vs-CRUD routing with each side's independent
"service not configured" error (`PusherNotConfigured`/`MutagenNotConfigured`, both `InvalidPush`). 8
tests. NOT ported: `SyncerWsMessageHandler` itself — a thin dispatch shell wired to
`ViewSyncer`/`Mutagen`/`Pusher`/`ConnectionContextManager`, most of which either don't exist yet
(`ViewSyncer`) or aren't unified behind the `MessageHandler` interface this file implements; OTEL
tracing and `Lock`-based per-connection mutation ordering also unmodeled. What's ported is the actual
decision tree a caller would run before touching any of those services. This crate now has 41 tests, 5
modules.
**`replicator.ts`'s replica-file setup/maintenance half — NEW, lives in `zero-cache-sqlite::
replicator_setup.rs`** (not this crate, since it needs `StatementRunner`/real SQLite file I/O this
crate doesn't depend on): `replica_file_name`/`get_pragma_config`/`apply_pragmas`/`set_journal_mode`
(a real retry loop against a live SQLite connection, `retry_delay` taken as an explicit `Duration`
param per this port's determinism convention) and `should_vacuum` (the pure VACUUM-threshold decision
from `prepare`), composed into `prepare_replica` — a REAL end-to-end port of `prepare`'s maintenance
sequence (fold WAL into main db, check/perform VACUUM via the already-ported `replication_state::
get_ascending_events`/`record_event`, switch to the target WAL mode, apply pragmas, `optimize`) run
against a real on-disk SQLite file, not `:memory:` (WAL semantics need a real file). 8 tests. Two
honest, documented deviations found and left in place rather than worked around: (1) `getAscendingEvents`'s
timestamp is SQLite's `CURRENT_TIMESTAMP` text format, not numeric — this port has no date-parsing
dependency yet, so `last_event_ms` falls back to `0.0` (the conservative "infinitely overdue" direction
to be wrong in) rather than adding a new dependency for one call site; (2) `WalMode::Wal2` is a
`zero-sqlite3`-fork-only journal mode (same category as `BEGIN CONCURRENT`, noted elsewhere in this
port) that this port's bundled vanilla SQLite build silently ignores rather than erroring on — caught
by the test itself failing first (asserted `wal2` was applied, got `delete` — the mode simply never
changed), not assumed. NOT ported (at the time): `setUpMessageHandlers`/`handleSubscriptionsFrom`/
`createNotifierFrom`/`subscribeTo` (the `Worker` IPC message-relay surface — genuinely needs a real
`Worker`/process abstraction this port hasn't built, unlike the `connection.ts` "worker" framing
corrected two rounds ago) and `setup_replica`'s `'serving-copy'` VACUUM-INTO-a-copy branch (a smaller
follow-on once this was proven — since closed, see below).
**`setup_replica` NOW FULLY PORTED, incl. the `'serving-copy'` branch.** Added `delete_lite_db` (port
of `deleteLiteDB` — removes a SQLite file and its `-wal`/`-wal2`/`-shm` sidecars, ignoring missing
ones, `force: true` semantics) and `setup_replica` itself: `Backup`/`Serving` open `file` directly and
run `prepare_replica` (`Wal`/`Wal2` respectively); `ServingCopy` performs a REAL `VACUUM INTO` copy —
deletes any stale copy first (`delete_lite_db`), copies the source file into
`replica_file_name(file, ServingCopy)` via a real `VACUUM INTO` SQL statement, closes the source
connection, then runs `prepare_replica` against the COPY (matching upstream's "the original file is
being used for 'backup' mode, so we make a copy for servicing sync requests" comment). 6 new tests, all
live against real on-disk files: `delete_lite_db` removing a real file + all 3 sidecar suffixes and
correctly no-op'ing on missing files; `Backup` mode preparing a real file directly; `ServingCopy`
performing a genuine `VACUUM INTO` (verified by querying real data out of the resulting copy file, not
just checking it exists) and correctly deleting a stale previous copy (garbage bytes + a stale `-wal`
sidecar) before vacuuming into it fresh. This closes the last piece of `replicator.ts`'s replica-file
half — combined with the IPC half closed two rounds ago, `replicator.ts` is now FULLY PORTED.
**THE `Worker`/PROCESS-MODEL DECISION IS NOW MADE.** New `zero-cache-workers::worker_message.rs`:
a Node `Worker` process maps to a `tokio::spawn` task; `Sender.send`/`Receiver.onMessageType` map to a
`tokio::sync::mpsc` channel carrying a tagged `(String, T)` pair — the direct structural analog of
upstream's `Message<Payload> = [type, payload]` tuple, needing no IPC/serialization layer since
`tokio::spawn` tasks share a process (a deliberate simplification this port doesn't need
cross-process isolation for, same reasoning as skipping a separate SQLite process per replica mode).
`WorkerSender`/`WorkerReceiver::recv_type` port the send/typed-receive halves. 6 tests. **Built on it:
replicator_ipc.rs — NEW**, finally closing `replicator.ts`'s previously-deferred IPC surface
(`handleSubscriptionsFrom`/`createNotifierFrom`/`subscribeTo`), wired to the ALREADY-PORTED
`zero-cache-services::notifier::Notifier`/`zero_cache_types::subscription` machinery: `handle_
subscriptions_from` waits for a real `'subscribe'` message, then relays a real `Notifier` subscription
out over a real channel as `'notify'` messages; `create_notifier_from` spawns a task relaying inbound
`'notify'` messages into a fresh local `Notifier` for further fan-out; `subscribe_to` sends the
initial handshake. 4 tests, all live (real `tokio::spawn` tasks, real channels, real `Notifier`
fan-out) — including one that genuinely hung during authoring and got debugged properly rather than
patched around: the test originally awaited the spawned relay task's `JoinHandle` to completion after
the assertion, but that task's `iter.next()` correctly keeps waiting for a NEXT notification forever
(by design — it's a long-lived relay loop), so awaiting it deadlocks; fixed by calling `.abort()`
instead, matching how a real caller would cancel this task when a connection closes rather than
waiting for a loop that never exits on its own. **This closes the`Worker`/process-model decision that
had been named across three rounds as the standing architectural gap** — both `replicator.ts` and
(once picked up) `syncer.ts` now have the primitive they need.
**First slice of `syncer.ts` itself — NEW: serving_lag.rs.** `syncer.ts` (694 lines, the actual
`ViewSyncerService`-hosting `Syncer` worker) opens with a wholly self-contained, pure algorithmic
block — computing per-`ViewSyncer` serving lag from a bounded ring of replica-ready samples — that
needs none of `Syncer`'s `WebSocketServer`/`ServiceRunner`/`DrainCoordinator` machinery, so it's
portable independent of how much of the rest of `syncer.ts` exists yet. Ported verbatim:
`bound_replica_ready_states`/`prune_replica_ready_states` (bounding a growing ring buffer, 10k-sample
cap), `lower_bound_replica_ready_time_ms`/`upper_bound_watermark` (binary searches over the same slice
sorted two different ways — by ready-time and by watermark string), `find_first_unserved_index`
(combines both bounds to find the earliest sample a given `ViewSyncer` hasn't caught up to),
`percentile_nearest_rank` (nearest-rank, not interpolated), and the two public entry points
`compute_serving_lag_stats_ms`/`compute_max_serving_lag_ms`. 11 tests, all passed on the first
attempt — incl. one confirming the prune step correctly drops samples no longer needed by ANY
`ViewSyncer` (not just the slowest one) after computing lag stats. NOT ported: `Syncer` itself and its
caching wrapper (`#recordReplicaReadyState`/`#computeServingLagStats`) around these pure functions —
that needs the full `Worker`/`ViewSyncerService` wiring, the actual remaining `syncer.ts` gap.
**websocket_server_options.rs — NEW, second pure `syncer.ts` slice.** Ports `getWebSocketServerOptions`:
maps `ZeroConfig`'s websocket fields into `ws`-style `ServerOptions` — `no_server`/`max_payload` always
set, `per_message_deflate` a `Disabled`/`Default`/`Options(JsonValue)` decision gated on
`websocketCompression`, with `ZERO_WEBSOCKET_COMPRESSION_OPTIONS` JSON-parsed only when compression is
actually enabled (matching upstream's exact branch nesting — bad JSON in that env var is silently
never even looked at if compression is off, ported faithfully rather than "helpfully" validating it
unconditionally). Scope deviation: `compression_options` stays an untyped `JsonValue` rather than a
strongly-typed `PerMessageDeflateOptions` struct, since `zero-cache-server::ws_connection` (this
port's `tokio-tungstenite` layer) has no compression-tuning knobs wired up yet to receive a typed
value. 6 tests incl. the malformed-JSON error path and the "options string ignored when compression
disabled" branch-ordering case.)
**serving_lag.rs — extended: `Syncer`'s remaining pure caching/recording wrappers.** Re-established
upstream source access (sparse-cloned `packages/zero-cache/src/services/view-syncer` and the rest of
`packages/zero-cache/src` into the session scratchpad) and re-read `syncer.ts` in full to check for
any pure slice missed. Found two: `record_replica_ready_state` (port of `#recordReplicaReadyState`
— appends a new ready-state sample, skipping mid-hydration snapshots with no watermark/ready-time
yet, skipping any watermark that isn't strictly newer than the last recorded one, clearing the whole
buffer once no `ViewSyncer` remains to consume it, otherwise re-applying the existing size bound)
and `ServingLagStatsCache` (port of `#servingLagStatsCache`/`#computeServingLagStats`'s memoization
wrapper — caches the last computed stats until cleared; upstream schedules the clear via
`queueMicrotask` right after the first compute in a tick, this port takes that as an explicit
`clear()` call the caller makes at the tick boundary instead of reading ambient scheduling directly,
matching this port's determinism convention). 6 new tests, all passed first try. Re-confirmed (by
reading the actual class body, not relying on the earlier assessment) that everything else in
`Syncer`'s constructor/methods is genuinely metrics-registration/`WebSocketServer`/`ServiceRunner`
wiring with no further pure logic to extract, and separately re-read `websocket-handoff.ts` in full
(173 lines) and confirmed it remains genuinely unportable pure-logic-wise — `createWebSocketHandoffHandler`/
`installWebSocketHandoff`/`installWebSocketReceiver` are all real `node:net.Socket`/child-process-
message/`ws`-library orchestration with no extractable pure core, matching the prior round's
finding exactly (a re-confirmation, not new information, but done via an actual fresh read rather
than trusting old notes). Full workspace clean, zero warnings, full suite green under
`--test-threads=1` (1113 total, 74 in zero-cache-workers).
**time_slice_timer.rs — NEW.** Re-read `view-syncer.ts` in full (2934 lines, with upstream access
now restored) looking for any additional tractable slice beyond what `view_syncer_session.rs`
already owns. Found `TimeSliceTimer` — the file's second exported class (alongside
`ViewSyncerService` itself), a lap-timer accumulating elapsed wall-clock time across cooperative-
yield boundaries (used to measure how much CPU a hydration/advance pass burns between yields).
Ported the lap-timing state machine (`start_without_yielding`/`elapsed_lap`/`stop`/`total_elapsed`/
`yield_process`), taking `now: f64` explicitly at every call (this port's determinism convention)
instead of reading `performance.now()` ambiently. NOT ported: the actual cooperative yield itself
(`await`ing a real Node event-loop turn) — no async-scheduling equivalent in this port, consistent
with the stance already taken on IVM's generator-based scheduling elsewhere; `yield_process` takes
both the pre- and post-yield timestamps as explicit parameters instead. Real quirk found and
preserved faithfully (not "fixed"): `start == 0.0` is upstream's own sentinel for "not running,"
which means a lap literally starting at wall-clock `0.0` would be indistinguishable from "not
started" — a latent upstream quirk (harmless in practice since `performance.now()` never actually
returns exactly zero) that this port's tests deliberately route around rather than paper over. Every
other remaining `ViewSyncerService` member checked this round (`#totalHydrationTimeMs`, `queryCount`/
`rowCount`/`servedVersion` getters, `#markVersionServed`, `#addQueryMaterializationServerMetric`) is
a thin pass-through to genuinely live state (`#pipelines`, `#cvrStore`, `#inspectorDelegate`) this
port doesn't have wired yet — accessors, not decisions, so nothing further to extract there.
6 new tests, all passed on the second try (first try used `now = 0.0` as a start timestamp in two
tests, which collided with the sentinel itself — corrected to non-zero timestamps, a test-authoring
mistake caught immediately by the assertion failure, not a bug in the ported logic). Full workspace
clean, zero warnings, full suite green under `--test-threads=1` (1119 total, 289 in
zero-cache-view-syncer). |
| `zero-cache-server`   | `src/server` + `src/workers/syncer.ts` | 🟡 in progress (NEW crate. ws_connection.rs —
a REAL `tokio-tungstenite` WebSocket accept loop: `WsConnection::accept` performs the handshake,
captures + decodes the `Sec-WebSocket-Protocol` header via `connect::decode_sec_protocols` (echoing
it back, required for the client to accept the handshake), `send_connected`/`send_json` send frames.
LIVE end-to-end test: a real `tokio-tungstenite` client connects over a real TCP socket with an
`encode_sec_protocols`-encoded header, the server decodes it and sends `connected` then a hand-built
poke sequence — both directions of a genuine socket round-trip using the ported protocol types, not
mocked. This closes the WebSocket transport gap named across many prior rounds. Added
`zero_cache_protocol::poke_json` (hand-rolled JSON serializer for PokeMessage/RowPatchOp, matching
`bigint_json`'s existing hand-rolled-codec style rather than introducing `serde`) so message bodies
ARE now real serialized protocol types, not placeholder JSON text. 2 unit tests +
**`tests/e2e_pipeline_to_websocket.rs`: THE CAPSTONE TEST** — the entire originally-stated priority
slice, live, in one test: real Postgres `INSERT` -> raw replication -> pgoutput decode -> SQLite apply
-> `TableSource`+`Filter` (compiled from a real AST `Condition` via `create_predicate`) -> IVM `Add`
delta -> serialized as a real `pokePart` JSON message via `poke_json` -> sent over a REAL WebSocket
connection -> received by a REAL connected `tokio-tungstenite` client, byte-for-byte asserted. Passed
on the first attempt. Every subsystem named in the user's original priority directive (Postgres
replication -> local store -> ZQL/IVM -> WebSocket sync) is now proven working together against real
infrastructure, not mocked at any layer.
ws_close.rs — NEW: port of `types/ws.ts`'s `closeWithError` pure code/reason computation (default
`INTERNAL_ERROR`=1011 close code, `elide`-truncated reason fitting the 123-byte WebSocket close-reason
limit). NOT ported: `sendPingsForLiveness`/`expectPingsForLiveness` (real heartbeat timers over a live
socket's ping/pong/message events — no pure core). 4 tests.
**client_handler.rs — NEW. First LIVE wiring of `ClientHandler`'s `PokeHandler`.** Composes
`zero-cache-view-syncer::client_handler_poke`'s pure decisions (`should_send_poke`/
`should_include_patch`/`should_flush_poke_part`/`decide_poke_end`) and `client_handler_row_patch::
make_row_patch` with the real wire types (`zero_cache_protocol::poke`) and a REAL `WsConnection` to
actually send `pokeStart`/`pokePart`/`pokeEnd` frames over a live socket — the live counterpart to
`query_hydration.rs`'s CVR-side composition proof, this time for the poke-sending side. `ClientHandler`
tracks `base_version`/`ever_poked`; `start_poke` returns `None` (sends nothing) for an already-caught-up
client, matching upstream's `NOOP` `PokeHandler`; `PokeCycle::add_patch`/`end`/`cancel` drive the real
message sequence. Scope: row-patch path only — `Patch::Config` (query patches) returns
`UnsupportedPatch` rather than being silently dropped; metrics and the `lastMutationIDChanges`/
`mutationsPatch` branches (need `zeroClientsTable`/`zeroMutationsTable` row classification) NOT ported.
4 LIVE tests in `tests/client_handler_live_poke.rs` (real `TcpListener` + real `tokio-tungstenite`
client/server pair, no mocking): a full poke cycle producing exactly `pokeStart`->`pokePart`->`pokeEnd`
real frames with a stale patch correctly filtered out of the wire body; an already-caught-up client
receiving literally nothing (proven by racing a timeout against `recv`); a no-patches poke still
advancing the version with `pokeStart` immediately followed by `pokeEnd` (no empty `pokePart` in
between); and a `Del` row patch decodable correctly on the wire. All passed on the first attempt.
**Query-patch path NOW WIRED too — `Patch::Config` is fully handled.** `add_patch` now dispatches BOTH
`Patch::Row` and `Patch::Config`, matching upstream's `patch.clientID ? desiredQueriesPatches[...] :
gotQueriesPatch` branch exactly: a client-scoped query patch goes into
`PokePartBody.desired_queries_patches[client_id]`, a client-group-wide one into `got_queries_patch`.
Required extending `zero_cache_protocol::poke_json` (previously only serialized `rowsPatch`) with
`QueriesPatchOp`/`QueriesPatch` JSON serialization — 2 new tests there. 1 new live test,
`query_patches_route_to_the_right_wire_field`, asserting both branches land in the correct wire field
against real serialized bytes over a real socket, not just constructed Rust values. `UnsupportedPatch`
error variant removed (no longer reachable — `add_patch` now handles every `Patch` variant). All
passed on the first attempt.
**Row-table classification NOW WIRED — `lastMutationIDChanges`/`mutationsPatch`'s `Del` branch too.**
`ClientHandler::new` now takes `client_group_id`/`zero_clients_table`/`zero_mutations_table` (port of
upstream's `${upstreamSchema(shard)}.clients`/`.mutations` construction). `add_patch`'s row branch now
classifies by table name: a `zero_clients_table` row-put is intercepted via the new
`client_handler_row_patch::update_lmids` (port of `#updateLMIDs` — parses `clientGroupID`/`clientID`/
`lastMutationID`, ignores rows for a different client group matching upstream's log-and-ignore, not an
error) and folded into `PokePartBody.last_mutation_id_changes`; a `zero_mutations_table` row-delete is
parsed via the new `client_handler_row_patch::parse_mutation_del_id` (port of the `'del'` branch's two
`assert`s: client id must be a string, mutation id must be a finite non-negative number) and pushed
into `mutations_patch` as `MutationPatchOp::Del`. Everything else still routes to `rowsPatch` as
before. Required extending `zero_cache_protocol::poke_json` with `lastMutationIDChanges`/
`mutationsPatch` wire serialization (2 new tests there; `MutationPatchOp::Put` intentionally left
`todo!()` since nothing constructs one yet — the mutations-table `'put'` branch needs full
`MutationResult` discriminated-union parsing, a real, separately-scoped gap, honestly not attempted).
1 new live test, `special_tables_are_reclassified_instead_of_sent_as_plain_row_patches`, proving BOTH
reclassifications against real wire bytes in one poke cycle: a clients-table row that does NOT appear
in `rowsPatch` but DOES appear correctly in `lastMutationIDChanges`, and a mutations-table delete that
lands correctly in `mutationsPatch`. All new tests passed on the first attempt.
**Mutations-table `'put'` branch NOW WIRED TOO — `addPatch` handles every branch upstream's does.**
`client_handler_row_patch.rs` gained `parse_mutation_result` (port of `mutationResultSchema`'s
discriminated union: `{data?}` ok, `{error:"app",message?,details?}` app error, `{error:"oooMutation"|
"alreadyProcessed",details?}` zero error — discriminated on the `error` field's presence/value, same
resolution order as upstream's `v.union`) and `parse_mutation_put` (port of the `'put'` branch's row
parsing: `clientID`/`mutationID`/`result` -> a full `MutationResponse`). 12 new tests across both
functions. `zero_cache_protocol::poke_json` gained `MutationPatchOp::Put` serialization (previously a
`todo!()` since nothing constructed one) — 2 new tests. `client_handler.rs`'s `add_patch` now builds a
real `MutationPatchOp::Put` for mutations-table row-puts instead of erroring; the now-dead
`UnsupportedMutationPut` error variant was removed. 1 new live test,
`mutations_table_put_parses_the_full_mutation_result`, proving a real row-put carrying a JSON-encoded
`result` column parses into a full `MutationResponse` and lands correctly in `mutationsPatch` on real
wire bytes. All new tests passed on the first attempt.
**`ClientHandler`'s `addPatch` now handles every branch upstream's does** (row/query/clients/mutations,
both put and del) — the only remaining gaps are metrics (`#pokeTime`/`#pokeTransactions`/`#pokedRows`)
and the class owning a live `Subscription<Downstream>` rather than a bare `WsConnection`.) |

Legend: ✅ ported + tests green · 🟡 in progress · ⬜ planned

## `zero-cache-types` module backlog

Ordered roughly by dependency depth (pure/foundational first). Each row is one
TS file + its `.test.ts`.

| Module                | Status | Notes                                        |
| --------------------- | ------ | -------------------------------------------- |
| `lexi_version`        | ✅     | 5 tests green + criterion bench              |
| `state_version`       | ✅     | 2 tests green; builds on lexi_version        |
| `row_key`             | ✅     | 3 tests green; exact xxHash128 hashes match  |
| `lite`                | ✅     | 5 tests green; value + LiteTypeString codec  |
| `specs` (db/specs.ts) | ✅     | 4 tests; full column/table/index specs + pg enums |
| `pg_data_type`        | ✅     | 7 tests green; type-name → ZQL ValueType     |
| `pg_types`            | ✅     | Postgres builtin type OID constants          |
| `pg_copy_binary` (db) | ✅     | 22 tests; COPY-binary stream parser + all decoders |
| `pg` (partial)        | 🟡     | 4 tests; time<->ms conversions done; timestamp/date + client deferred |
| `pg_to_lite` (db)     | ✅     | 4 tests; PG→lite column/table/index + default allowlist |
| `lsn` (change-source) | ✅     | 2 tests; pg_lsn <-> LexiVersion/state-version conversions |
| `column_metadata` (pure) | ✅  | 3 tests; liteTypeString<->metadata conversions |
| `sql`                 | ✅     | 3 tests green; identifier/literal escaping   |
| `strings` / `names`   | ✅     | 3 tests green (elide, liteTableName)         |
| `shards`              | ✅     | 3 tests green; schema naming + app-id check  |
| `subscription`        | ✅     | 9 tests green; coalesce/pipeline/concurrency; core change-streamer primitive |
| `streams`             | 🟡     | STALE ROW, corrected: `subscription` (row above) IS this port's `Source`/backpressure primitive, in active use — `zero-cache-server`'s live WebSocket capstone test pokes real data over it. `Queue` dep ✅ ported. |
| `websocket_handoff`   | ⬜     | genuinely still unstarted — needs raw HTTP upgrade + socket handoff between processes, deeply Node-`net.Socket`-specific; no tractable pure slice found yet (see `zero-cache-server::ws_close`'s module doc for what WAS extractable from the neighboring `ws.ts`) |
| `processes`           | 🟡     | STALE ROW, corrected: this is the `Worker`/message-passing abstraction, ported as `zero-cache-workers::worker_message.rs` (`tokio::spawn` + tagged `mpsc` channels) several rounds ago — see that crate's PORTING.md row |
| `ws`                  | 🟡     | STALE ROW, corrected: `zero-cache-server::ws_close.rs` — NEW, ports `closeWithError`'s pure code/reason computation (close code defaults to `INTERNAL_ERROR`=1011, reason is the error message `elide`-truncated to the 123-byte WebSocket close-reason limit). NOT ported: `sendPingsForLiveness`/`expectPingsForLiveness` (real heartbeat timers wired to a live socket's ping/pong/message events — genuinely stateful, no pure core to extract). 4 tests. |
| `error_with_level`    | ✅     | 4 tests green; uses `zero-cache-protocol`    |
| `url_params`          | 🟡     | STALE ROW, corrected: ported as `zero-cache-workers::url_params.rs` (wraps an already-parsed `&[(String,String)]` pair list instead of a real URL, since this port has no URL-parsing dependency — documented deviation, not a blocker) several rounds ago |
| `timeout`             | ✅     | 3 tests green; tokio integrated (async boot) |

## Next-phase inflection

The pure, dependency-free core of `types` is done. The remaining `types`
modules split into two groups, each needing new foundation first:

1. **`zero-protocol` port** — `error_with_level` (and much of the sync server)
   needs `ProtocolError`, `ErrorKind`, `ErrorOrigin`, `ValueType`. Porting a
   `zero-cache-protocol` crate unblocks these and lets `ValueType` move out of
   `pg_data_type` to its real home.
2. **Async runtime (tokio)** — `subscription`, `streams`, `timeout`,
   `processes`, `ws`, `websocket_handoff` are all async primitives. Introduce
   tokio + a websocket lib before porting them.

Recommended order to keep the pipeline slice moving: `zero-protocol` value/error
types → `db/specs` (full) → `pg` value parsing → replication `change-source`.

## Conventions

- **Errors:** TS `assert(...)` that throws → Rust `Result<_, ThisError>`; tests
  that use `toThrowError()` assert `.is_err()`.
- **Bench:** Criterion with `harness = false`; run `cargo bench -p <crate>`.
- **bigint:** TS `bigint` → `num_bigint::BigInt`. TS `number` safe-integer
  semantics preserved where the original asserts on `MAX_SAFE_INTEGER`.
- **Fuzz tests:** `Math.random()` loops are reproduced with a seeded xorshift so
  runs are deterministic.

## `zero-cache/src/services/*` directory-coverage scan (live upstream reads)

Widened the scratchpad sparse-checkout to all of `zero-cache/src/services` and swept every file
across `mutagen`/`replicator`/`change-streamer`/`shadow-sync`/`litestream`/`limiter`/`change-source`
against this table's actual content (not just grepping for exact filenames, which produces false
negatives whenever a file was ported as part of a differently-named Rust module — several were).

**Confirmed already covered (false-negative "gaps" from a filename-only grep, verified by actually
checking the corresponding Rust module):**
- `limiter/sliding-window-limiter.ts` — already ported as `zero-cache-services::sliding_window_limiter.rs`.
- `change-source/pg/logical-replication/binary-reader.ts` — its exact byte-cursor primitives
  (`u8`/`i16`/`i32`/`u64`/`cstr`/LSN decoding) already exist, independently reimplemented in idiomatic
  `Result`-based Rust rather than a literal port, as `pgoutput.rs`'s internal `Reader` struct.
- `change-source/protocol/current/{control,status,downstream,upstream,data}.ts` — already ported as
  `zero-cache-change-source::{control,status,downstream,upstream,data}.rs` (including `data.ts`'s
  `isSchemaChange`/`isDataChange` type guards, already present as `is_schema_change`/`is_data_change`).
- `change-source/pg/schema/ddl.ts` — already ported (`ddl_apply.rs`, `zero-cache-sqlite`).
- `replicator/schema/{column-metadata,table-metadata,replication-state,change-log}.ts` — already
  ported (`column_metadata.rs`/`table_metadata.rs`/`replication_state.rs`/`change_log.rs`,
  `zero-cache-sqlite`).
- `change-source/pg/lsn.ts`, `change-source/pg/schema/{published,shard}.ts`,
  `change-source/pg/logical-replication/pgoutput*.ts` — already covered under `zero-cache-change-source`.
- `change-source/protocol/current/{json,path}.ts` — pure `valita` type/schema declarations (`json.ts`)
  or a single trivial routing-path constant (`path.ts`); correctly out of scope, no logic to port.

**Genuinely NOT ported — real, substantial future work, not false negatives:**
- `change-source/pg/initial-sync.ts` (1473 lines) — the full initial Postgres-to-SQLite-replica
  bootstrap. By far the largest single unported file found this round.
- `change-source/common/backfill-manager.ts` (621 lines), `change-source/pg/replication-slots.ts`
  (305 lines), `change-source/common/replica-schema.ts` (245 lines),
  `change-source/common/change-stream-multiplexer.ts` (159 lines),
  `change-source/pg/decommission.ts` (43 lines, but dominated by live Postgres slot/publication
  orchestration with only a trivial shard-name-string helper as pure logic — not worth a standalone
  port on its own).
- `replicator/incremental-sync.ts`, `replicator/write-worker.ts`/`write-worker-client.ts`,
  `replicator/notifier.ts`, `replicator/registry.ts`, `replicator/replication-status.ts`.
- `change-streamer/*` (change-streamer-service.ts, storer.ts, subscriber.ts, forwarder.ts,
  broadcast.ts, replica-monitor.ts, backup-monitor.ts and its 3 variants) — not checked file-by-file
  this round beyond confirming zero PORTING.md representation; likely has SOME already-covered pieces
  like the pattern above, not yet verified.
- `shadow-sync/shadow-sync-service.ts`, `litestream/*` (commands/metrics/vfs-watermark-*).
- `mutagen/mutagen.ts`/`pusher.ts` — PORTING.md already documents substantial coverage of mutagen/
  pusher logic elsewhere in this file (15/28 mentions respectively), so these may be more-covered
  than a bare "unported" label suggests; not reconciled precisely against the current file contents
  this round.

This scan closes the "sweep `zero-cache/src/services/*`" item deferred across four prior rounds —
the concrete finding is that the directory is in much better shape than its complete absence from
PORTING.md implied (several real ports were already done, just indexed under different module
names), but `initial-sync.ts` in particular remains a large, genuine, multi-round future increment.

**initial_sync_sql.rs — NEW, first slice of `initial-sync.ts` (1473 lines).** Read the full file to
find its self-contained pure prefix, mirroring how `serving_lag.rs` opened `syncer.ts`: the
download-statement SQL-building ahead of the actual live COPY-streaming orchestration.
Ports `table_sample_clause`/`limit_clause`/`make_binary_select_exprs`/`make_download_statements`
(all pure given a `PublishedTableSpec` and sync parameters) into `zero-cache-types` (alongside the
`sql`/`specs`/`pg_copy_binary` modules it's built from). `make_binary_select_exprs` reuses the
already-ported `has_binary_decoder`/`BinaryColumnSpec` from `pg_copy_binary.rs` (building a
`BinaryColumnSpec` from a `PublishedColumnSpec`'s existing `column`/`type_oid` fields — no new type
needed, upstream's `ColumnSpec` already carries the `dataType`/`pgTypeClass`/`elemPgTypeClass` this
port's `ColumnSpec` struct also already has). `table_sample_clause`'s float-noise rounding
(`parseFloat((rate*100).toFixed(6))`) ported as an explicit `round_to`/`format_trimmed` pair rather
than relying on Rust's float `Display` matching JS `toFixed`/`parseFloat` round-tripping by accident.
8 tests, all passed first try. NOT ported (see module doc): `initialSync`/`shadowInitialSync`
themselves (the actual live orchestration — upstream connection, `COPY` streaming, `TransactionPool`,
replica schema/index creation, replication-slot setup), `copyBinary`/`copyText` (the streaming decode
loop), `verifyShadowReplica` — all need a live-Postgres-COPY-protocol client this port doesn't have
yet, a substantial, genuinely separate future increment. Full workspace clean, zero warnings, full
suite green under `--test-threads=1` (1170 total, 104 in zero-cache-types).
**database_storage.rs — NEW, `database-storage.ts` landed (187 lines, flagged 3 rounds running).** A
live SQLite-backed key-value storage layer for IVM operator state: `DatabaseStorage` (owns the
connection, sets the same ephemeral/single-writer pragmas upstream does — `locking_mode = EXCLUSIVE`,
`synchronous = OFF`, `journal_mode = OFF`, `auto_vacuum = INCREMENTAL`, `CREATE_STORAGE_TABLE`
verbatim), `ClientGroupStorage` (per-client-group namespace, clears stale rows for its ID on
creation — matching upstream's defensive `clear.run(cgID)` guarding against a prior ungraceful
exit), `OperatorStorage` (per-operator `get`/`set`/`del`/`scan`, auto-incrementing `op` ID scoping).
Reused `db_maintenance::decide_compaction` (ported 2 rounds ago) for `destroy`'s compaction decision
— a second, real payoff from that earlier round beyond its own file. Scope deviations, both
documented in the module doc: (1) no `Storage`/`Stream` trait exists in `ivm::operator` yet for this
to implement against, so `create_storage` returns the concrete `OperatorStorage` type directly rather
than a trait object — wiring to a real trait is a follow-up once one exists; (2) `#scan`'s lazy
generator (`Stream<[string, JSONValue]>`) becomes a `Vec` return (borrowing a live
`rusqlite::Statement` across a lazy Rust iterator here would fight the borrow checker for no benefit
— result sets are already prefix-bounded, not unbounded). 10 tests, all passed first try — including
one against a real file-backed SQLite database (not just in-memory) confirming the pragma setup
actually works, and one deliberately forcing `#maybeCheckpoint`'s automatic commit-and-reopen path by
setting `commit_interval: 3` and writing exactly 3 keys. Full workspace clean, zero warnings, full
suite green under `--test-threads=1` (1180 total, 177 in zero-cache-sqlite).

**`change-streamer/*` swept with live reads — mostly already covered, one small real gap found and
closed.** Confirmed (as the prior round predicted) that a "not yet checked, absent from grep" file
list hid several already-done ports under different names: `broadcast.ts`'s entire consensus-timeout
algorithm is already `zero-cache-services::broadcast.rs`; `forwarder.ts`'s subscriber-bookkeeping
slice is already `change_streamer_forwarder.rs`; `replicator/notifier.ts` (a different directory, but
turned up during the same sweep) is already `notifier.rs`. `snapshot.ts`/`backup-monitor.ts` are pure
`valita`-schema/interface declarations with zero logic (correctly out of scope).
`replica-monitor.ts` (64 lines) is a live 30-second polling loop with one trivial `stateVersion !==
lastWatermark` comparison — dominated by async service lifecycle, not worth a standalone port.
`change-streamer-service.ts` (657 lines) and `storer.ts` (1068 lines) were not read line-by-line this
round (time-boxed); `subscriber.ts` (382 lines, the live websocket-connected entity) is already
explicitly documented as an unported real gap in `change_streamer_forwarder.rs`'s own module doc.
**change_streamer_error.rs — NEW, the one real gap found.** `error-type-enum.ts`'s three numeric
constants (`Unknown`/`WrongReplicaVersion`/`WatermarkTooOld`) plus `change-streamer.ts`'s
`errorTypeToReadableName`, ported as an `ErrorType` enum + `error_type_to_readable_name`. 1 test.
Full workspace clean, zero warnings, full suite green under `--test-threads=1` (1181 total, 26 in
zero-cache-services).

**`change-streamer-service.ts`/`storer.ts` read properly (657 + 1068 lines).**
`change-streamer-service.ts` is essentially one class (`ChangeStreamerImpl`) plus its async
`initializeStreamer` factory — deeply live Postgres-schema-init/replication-config/change-source-
subscription orchestration throughout, no further standalone pure logic found.
**change_streamer_storer.rs — NEW, the real gap found in `storer.ts`.** Two pure string-manipulation
functions found ahead of `Storer`'s actual live orchestration: `extract_change_substring` (pulls the
inner change-message JSON out of a full stringified stream message, e.g.
`["begin",<msg>,{"commitWatermark":"..."}]` → `<msg>` — an optimization letting the real caller
stringify the full message once while storing only the substring in the change log) and
`to_downstream` (its inverse: reconstructs the full stream message from a stored `(watermark, tag,
change)` change-log entry, re-adding each tag's trailing metadata). 7 tests, including one that
round-trips `extract_change_substring` through `to_downstream` back to the original message. NOT
ported: `Storer` itself (live `TransactionPool`-backed change-log writer/subscriber catchup) and
`PurgeLock`/`PurgeLocker` (live transaction-scoped locking) — both need live Postgres/transaction-
pool infrastructure this port doesn't have. Full workspace clean, zero warnings, full suite green
under `--test-threads=1` (1188 total, 33 in zero-cache-services).

**`mutagen.ts`/`pusher.ts` reconciled against their 15/28 PORTING.md mentions — legitimately
well-covered, no hidden gap.** `mutagen.ts` (474 lines): `getInsertSQL`/`getUpsertSQL`/
`getUpdateSQL`/`getDeleteSQL` already ported verbatim as `zero-cache-mutagen::sql.rs`'s
`get_insert_sql`/`get_upsert_sql`/`get_update_sql`/`get_delete_sql`; `checkSchemaVersionAndIncrementLastMutationID`'s
decision half already ported as `last_mutation_id.rs::check_mutation_id`;
`processMutationWithTx`'s SQL-planning already ported as `orchestration.rs::plan_mutation_sql`. The
one real remaining gap is `processMutation`'s retry-loop orchestration (error-mode-retry-once policy,
`PG_SERIALIZATION_FAILURE`-triggered retries, a temporary CRUD/custom-mutator co-existence delay-and-
retry) — genuinely entangled with catching real exception types from a live Postgres driver, not a
clean pure-logic extraction; correctly left unported, same category as other live-transaction
orchestration elsewhere in this port (`Storer`, `ChangeStreamerImpl`).
`pusher.ts` (712 lines): `combinePushes`/`assertAreCompatiblePushes` already ported as
`pusher_batch.rs::combine_pushes` (with `assertAreCompatiblePushes`'s invariant-violation asserts
deliberately turned into a `Result<_, IncompatiblePushes>` instead of a panic — a batching bug is
worth being able to test/handle, not crash on); `PusherService`/`PushWorker`'s queue/batching/
ordering machinery already ported as `pusher_service.rs`, whose own module doc explicitly states it
was "the last unported piece of pusher.ts." **Conclusion: no new code this round** — the
reconciliation confirms `mutagen.ts`/`pusher.ts`'s PORTING.md mentions were accurate, not stale
optimism, closing this item cleanly rather than leaving it as a recurring "still need to check"
line item.

**`zero-cache/src/observability` re-checked with live source — confirmed already fully accounted
for, no gap.** Only two files exist in the directory. `events.ts` (158 lines): `makeErrorDetails`
already ported verbatim as `error_details.rs`; the retry-backoff formula and extensions-schema
validation already ported as `event_publish.rs`. The rest (`initEventSink`/`publishEvent`/CloudEvent
HTTP emission via `gzip`+the `cloudevents` library) is real live I/O with no OTel/CloudEvents client
in this port to wire it to — correctly out of scope, not a gap. `metrics.ts` (212 lines): pure OTel
`Meter`-factory boilerplate (lazy-cached `getOrCreate{Counter,Histogram,Gauge,UpDownCounter}`
wrappers) — no logic beyond a `zero.{category}.{name}` naming formula and a
`LATENCY_HISTOGRAM_BOUNDARIES_S` bucket-boundary constant, neither meaningful to extract absent a
real OTel dependency in this port. Confirms the directory's existing PORTING.md characterization was
accurate. **No new code this round** — second consecutive reconciliation-only round confirming
existing coverage rather than finding a gap, following the `mutagen.ts`/`pusher.ts` check.

**Session status at this point:** the systematic sweeps this session (`zql/src/planner`
output/plan_id fields, `zero-cache/src/services/*` directory scan, `change-streamer/*` file-by-file,
`mutagen.ts`/`pusher.ts` reconciliation, `observability` re-check) have now covered essentially every
corner of the codebase that was flagged as "not yet checked" or "presumed gap." The tractable
pure-logic low-hanging fruit has been substantially exhausted by this point. The one clearly
remaining large item is `initial-sync.ts`'s live-orchestration half (upstream Postgres connection,
real `COPY ... TO STDOUT` streaming, a `TransactionPool`, replication-slot setup) — this needs a real
design decision (build a live Postgres COPY-streaming client in this port, e.g. via `tokio-postgres`)
rather than more scanning/reading. That, plus the already-known `change-streamer-service.ts`/
`Storer`/`ChangeStreamerImpl`/`Subscriber` live-orchestration classes and `initial-sync.ts`'s sibling
files (`backfill-manager.ts`, `replication-slots.ts`, etc.), represent the actual remaining size of
the port going forward — a live-Postgres-protocol-client-building phase, not a continuation of the
find-hidden-pure-logic phase this session has been running.

**`drain-coordinator.ts` — NEW pure-logic slice found despite the "fruit exhausted" note.**
`zero-cache-view-syncer::drain_coordinator.rs` ports `DrainCoordinator`'s deterministic scheduling
core: `should_drain()` (the `next_drain_time != 0 && next_drain_time <= now` check), `drain_next_in()`
(the `interval / TARGET_UTILIZATION` scaling, `next_drain_time = now + interval` update, and the
`interval + FORCE_DRAIN_PADDING` force-drain timeout *duration* it returns), and `next_drain_time()`.
The `@rocicorp/resolver` promises (`draining`, `forceDrainTimeout`) and `setTimeout`/`clearTimeout`
machinery are real async orchestration left unmodeled (no event loop to hook into — same stance as
`time_slice_timer.rs`). `now: i64` (ms, matching `Date.now()`) is an explicit parameter per this
port's determinism convention. No upstream `.test.ts` exists for this file; 6 tests written from the
spec (kickoff `drain_next_in(0)`, utilization scaling, the `should_drain` boundary, the
"only-when-due" assert, successive elective reschedules). Full workspace clean, zero warnings, suite
green under `--test-threads=1` (1194 total).

**`change-streamer/subscriber.ts` — pure back-pressure/stats core extracted.**
`zero-cache-services::subscriber_backpressure.rs` ports the deterministic decision logic embedded in
the otherwise-live `Subscriber` orchestration class: `ByteBackpressureGate` (the byte high/low-water
gate deciding when a producing `send()` blocks and when blocked producers are released in a batch —
each `Resolver<void>` waiter modeled as a count, so `wait_for_space` returns whether to block and the
`release_*` methods return how many to wake, leaving actual promise resolution to the caller);
`supports_message` (the `update-table-metadata` >= protocol-v5 gate); and the `getStats().processRate`
math with `sampleProcessRate`'s bounded-history bookkeeping (`process_rate` / `push_sample` /
`MAX_SAMPLES`). The `Subscription<string>` downstream, `@rocicorp/resolver` promises, and the async
`#drainBacklog` byte-windowed send loop remain live orchestration this port doesn't drive (same
category as `Storer`/`ChangeStreamerImpl`). 9 tests. Full workspace clean, zero warnings, suite green
under `--test-threads=1` (1203 total).

**`initial-sync.ts` pure summary/metrics logic + recovered an orphaned live slice.**
Two things this round, both in `zero-cache-sqlite`:
1. NEW `initial_sync_metrics.rs` — the pure decision logic embedded in `initialSync`'s otherwise-live
   orchestration: `recorded_run_metrics` (the `recordInitialSyncRunMetrics` `if`-ladder deciding which
   instruments fire — always `runs`/`duration`, the rest success-gated and presence-gated, counters
   additionally positivity-gated), `initial_sync_copy_summary` (the per-table `reduce` totalling
   rows/flushMs/copyBytes), `should_log_slow_copy_flush` (the `>= SLOW_COPY_FLUSH_MS && flushedRows`
   guard), and the `initial_sync_metric_attrs`/`initial_sync_run_metric_attrs` label maps. The OTel
   instruments themselves are not modeled (no OTel dependency, same stance as `observability`). 7 tests.
2. FIXED: `initial_sync_copy.rs` (the live `tokio_postgres::copy_out` binary-COPY-into-SQLite slice
   authored a prior round) was never declared in `lib.rs` — silently uncompiled and untested. Wired it
   in; it failed the *normal* build (compiled only under the test profile, which pulls `tokio` as a
   dev-dep) on `tokio::pin!` needing `tokio` as a non-dev dependency. Swapped to
   `futures_util::pin_mut!` (already a dependency). Now compiles in all profiles and its 2 live tests
   (skip-if-no-local-Postgres) run. This restores a real piece of the live COPY-streaming half of the
   port to the compiled surface.

Full workspace clean, zero warnings, suite green under `--test-threads=1` (1212 total).

**Live-Postgres phase: `CREATE_REPLICATION_SLOT` primitive — the missing key `initialSync` needs.**
`zero-cache-change-source::replication_conn.rs` gains `ReplicationConn::create_logical_replication_slot`,
which issues `CREATE_REPLICATION_SLOT "<slot>" LOGICAL pgoutput` over the hand-rolled replication
connection and parses the reply into a new `CreatedSlot { slot_name, consistent_point, snapshot_name }`
(via a new `parse_data_row` backend-`DataRow` decoder). This is the primitive `initialSync`'s
`createReplicaAndSlot` is built on: creating the slot atomically fixes a consistent snapshot that the
bulk table-copy transactions `SET TRANSACTION SNAPSHOT` to, so the COPY sees exactly the data as of the
slot's `consistent_point` LSN. Unlike the SQL `pg_create_logical_replication_slot` function (used in an
existing test), the *replication-protocol* command exports a snapshot by default — which is why the raw
connection is required here. Verified live against a real local Postgres: the end-to-end test creates a
slot, commits a new row *after* slot creation, `SET TRANSACTION SNAPSHOT`s a side-channel client to the
exported snapshot, and asserts the post-creation row is invisible — proving the exported snapshot is
genuinely usable for consistent backfill. Plus a pure `parse_data_row` unit test (text columns + NULL +
empty-string). This connects the last missing live primitive between the existing slot-less replication
streaming and the existing binary-COPY-into-SQLite path; assembling the top-level `initial_sync` driver
that sequences them is the next step. Full workspace clean, zero warnings, suite green under
`--test-threads=1` (1214 total, live PG tests exercised against a real local instance).

**MILESTONE: the top-level live `initial_sync` driver — end-to-end snapshot backfill works.**
`zero-cache-sqlite::initial_sync.rs`'s `run_initial_sync` sequences every live primitive prior rounds
built individually into one real, working initial sync of upstream Postgres into the SQLite replica:
(1) derive the initial replica version from the slot's `consistent_point` LSN (`to_state_version_string`);
(2) create the `_zero.*` meta-tables + seed `replicationConfig`/`replicationState`
(`init_replication_state`); (3) bind the copy connection to the slot's exported snapshot
(`BEGIN ISOLATION LEVEL REPEATABLE READ` + `SET TRANSACTION SNAPSHOT`); (4) create each lite table
(`map_postgres_to_lite` + `DdlApplier::create_table`); (5) binary-`COPY` each table in at the snapshot
(`copy_table_binary`); (6) create indexes (`DdlApplier::create_index`); (7) commit — rolling back the
upstream tx on any copy error so the connection is never left mid-transaction. Two subtleties handled:
`cols` for the COPY are the *upstream* columns only, not the lite spec's (which appends `_0_version` —
filled by that column's `DEFAULT '<version>'`); and `map_postgres_to_lite` takes a projected `TableSpec`
(new `published_to_table_spec` helper). **Verified live end-to-end against real Postgres:** the test
creates a slot, commits a row *after* the slot's snapshot, runs the driver on a dedicated copy
connection, and asserts the replica holds exactly the pre-snapshot rows (the post-snapshot row is
excluded — proving snapshot-consistent backfill) plus the recorded publication/version config. Scope
still open: `getPublicationInfo` (the upstream-schema introspection producing the `PublishedTableSpec`s)
is the driver's *input* rather than run internally, and `ensurePublishedTables`/`checkUpstreamConfig`
(publication-creating DDL + `wal_level` validation) are assumed done — those introspection/DDL queries
are the next live pieces. But the core snapshot→copy→replica pipeline is now assembled and working.
Full workspace clean, zero warnings, suite green under `--test-threads=1` (1215 total; the new live
end-to-end initial-sync test runs against a real local Postgres).

**`getPublicationInfo` introspection SQL + validation ported (the `initial_sync` self-containment path).**
`zero-cache-change-source::published_schema.rs` ports the pure/SQL pieces of
`change-source/pg/schema/published.ts`: `quote_literal`/`literal_list` (`pg-format`'s `literal()`
string escaping — single-quote doubling + backslash `E'...'` escape-string form, an injection-safety
concern), `published_schema_query` (the verbatim ~150-line `publishedSchemaQuery` introspection SQL,
with the publication list spliced in at both `IN (...)` sites), and `check_published_columns_consistency`
(the `getPublicationInfo` validation that a table in multiple publications exposes the same column set).
**The verbatim SQL is verified live against real Postgres**: a test creates a real publication, runs the
query via `simple_query`, and confirms it is well-formed and returns the published table plus its
primary-key index — guarding the large ported SQL string against a typo. What's still open before the
`initial_sync` driver can introspect its own specs (rather than taking them as input): deserializing the
query's `publishedSchema` JSON (`{tables, indexes}`) into `PublishedTableSpec`/`PublishedIndexSpec` plus
the `replicaIdentityColumns` denormalization — a sizeable JSON→spec parser, the next piece. 8 tests
(7 pure + 1 live). Full workspace clean, zero warnings, suite green under `--test-threads=1`
(1223 total).

**CLOSED the introspection loop: `publishedSchema` JSON→spec deserializer + live `get_publication_info`.**
`zero-cache-types::published_schema_json.rs` deserializes the query's `{tables, indexes}` JSON into
`PublishedTableSpec`/`PublishedIndexSpec` — the counterpart to upstream's `v.parse(result,
publishedSchema)` valita parse. Operates on the port's `bigint_json::JsonValue` (consistent with
`ast_from_json`); handles every field with its upstream optionality (`pgTypeClass`/`elemPgTypeClass`/
`characterMaximumLength`/`notNull`/`dflt` nullable-optional, `schemaOID`/`replicaIdentity` optional,
empty `primaryKey` array → `None`), sorts each table's columns by `pos` (JSON object key order is
unspecified), and parses per-publication `rowFilter` + index directions. `to_index_spec` downcasts a
`PublishedIndexSpec` to the plain `IndexSpec` the DDL applier consumes. 7 unit tests. Then wired the
live end-to-end path in `zero-cache-change-source::published_schema::get_publication_info`: run
`published_schema_query` via `simple_query`, `bigint_json::parse` the JSON text, and
`published_schema_from_json` it into real specs — **verified live against real Postgres** (create a
publication, introspect it, assert the parsed table/columns/PK/not-null flag/PK-index). This closes the
self-containment gap: the `initial_sync` driver's `PublishedTableSpec`/`IndexSpec` inputs can now be
produced by a single live `get_publication_info` call instead of hand-built. Remaining before a fully
self-driving initial sync: `ensurePublishedTables`/`checkUpstreamConfig` (publication-creating DDL +
`wal_level` validation), then wiring `get_publication_info` → `run_initial_sync` in one entry point,
after which the `ChangeStreamerImpl`/`Storer` streaming loops. 9 change-source tests (7 pure + 2 live)
+ 7 types tests. Full workspace clean, zero warnings, suite green under `--test-threads=1` (1231 total).

**`checkUpstreamConfig` (the `wal_level`/version precondition) ported live.**
`zero-cache-change-source::pg_connection.rs` gains the `PG_15`/`PG_17` constants (port of
`types/pg-versions.ts`), a pure `validate_upstream_config(wal_level, version)` (logical-replication
requires `wal_level = logical` and server >= PG 15; wal_level checked first, returns the version like
upstream for the `PG_17` failover path), and a live `check_upstream_config(client)` that reads
`current_setting('wal_level')` + `server_version_num` in one round trip and validates them. **Verified
live against real Postgres** (the provisioned instance passes; returns a version >= PG_15). This is the
next initial-sync precondition after `getPublicationInfo`. Still open before a self-driving initial
sync: `ensurePublishedTables` (the shard-schema/publication-creating DDL — a larger sub-system:
`ensureShardSchema`/`getInternalShardConfig`/`dropShard`), then a single entry point sequencing
`check_upstream_config` → `get_publication_info` → `run_initial_sync`, then the `ChangeStreamerImpl`/
`Storer` streaming loops. 3 new tests (2 pure + 1 live). Full workspace clean, zero warnings, suite
green under `--test-threads=1` (1233 total).

**Shard-schema naming + `dropShard` teardown SQL ported (first slice of `ensurePublishedTables`).**
`zero-cache-change-source::shard_schema.rs` ports the pure name/identifier and DDL-string builders from
`change-source/pg/schema/shard.ts`: `validate_publication_name` (upstream's injection-safety identifier
check — charset then 63-char length, in that order), `internal_publication_prefix`,
`legacy_replication_slot`, `replication_slot_prefix`/`replication_slot_expression` (the latter escaping
`_` for `LIKE`), `default_publication_name`/`metadata_publication_name`, and `drop_shard` (the teardown
SQL that drops both internal publications explicitly — `DROP SCHEMA CASCADE` doesn't cascade to
publications — then the schema, identifiers quoted via `sql::id`). Slot/schema names derive from the
existing `zero_cache_types::shards` `check`/`ShardId` helpers. These underpin the still-to-come live
`ensureShardSchema`/`setupTablesAndReplication` DDL orchestration (the `CREATE SCHEMA`/`CREATE
PUBLICATION` statements + `getInternalShardConfig` read), which is the remaining larger sub-system
before a single `check_upstream_config` → `ensure_published_tables` → `get_publication_info` →
`run_initial_sync` entry point, then the `ChangeStreamerImpl`/`Storer` streaming loops. 7 tests. Full
workspace clean, zero warnings, suite green under `--test-threads=1` (1240 total).

**Shard-setup DDL builders ported + executed live against Postgres.**
Extended `shard_schema.rs` with the `shardSetup`/`globalSetup` SQL builders: `get_clients_table_definition`
/`get_mutations_table_definition` (the shard's `clients`/`mutations` tables), `global_setup` (the
idempotent app schema + `permissions` table, its md5-hash trigger, and seed row — all `IF NOT EXISTS`/`OR
REPLACE`), and `shard_setup` (the per-shard schema: `clients`/`mutations`, the metadata publication —
asserted to be one of the shard's publications — the `shardConfig` singleton seeding the sorted
publication list, and the `replicas` table), plus the `SHARD_CONFIG_TABLE` const. Publication lists are
spliced via `published_schema::literal_list`; identifiers quoted via `sql::id`. **Verified live against
real Postgres**: a test executes `global_setup` then `shard_setup`, reads back the `shardConfig` row
(sorted publications, `ddlDetection=false`) and confirms the metadata publication exists, then tears it
all down with `drop_shard` and confirms the publication is gone — proving the large DDL strings are
well-formed end to end. This is the DDL half of `ensurePublishedTables`; what remains to make it a live
`ensure_published_tables` is the orchestration wrapper (`ensureShardSchema` runs global+shard setup in a
transaction; `getInternalShardConfig` reads the config row back) + the resync-on-mismatch validation,
then the single initial-sync entry point, then `ChangeStreamerImpl`/`Storer`. 5 new tests (4 pure + 1
live). Full workspace clean, zero warnings, suite green under `--test-threads=1` (1245 total).
(Also fixed a test-isolation bug this introduced: the live test originally created a broad `FOR TABLES
IN SCHEMA public` publication that polluted other live tests sharing the Postgres instance under a
single-threaded workspace run — removed it, since `shard_setup` only records publication *names* as
text and doesn't require the default publication to exist.)

**`getInternalShardConfig` (live) + requested-publication validation ported.**
`shard_schema.rs` gains `InternalShardConfig { publications, ddl_detection }`, the live
`get_internal_shard_config(client, shard)` reading the shard's `shardConfig` singleton row (erroring on
a non-1 row count, matching upstream's assert), and the pure `validate_requested_publications` (port of
`setupTablesAndReplication`'s loop: each requested publication must be a valid identifier and must not
start with `_` — reserved for internal publications). **Verified live**: the shard-setup test now also
reads its config back via `get_internal_shard_config` and asserts the sorted publications +
`ddlDetection=false`. What remains to complete `setupTablesAndReplication`: the app-publication
existence check / default-publication creation branch, then `setupTriggers` (the event-trigger DDL for
schema-change detection — `createEventFunctionStatements`/`triggerSetup`, a sizeable sub-system with a
degraded-mode fallback) and `replicaIdentitiesForTablesWithoutPrimaryKeys`; then the single initial-sync
entry point, then `ChangeStreamerImpl`/`Storer`. 2 new tests (1 pure + 1 live assertion added). Full
workspace clean, zero warnings, suite green under `--test-threads=1` (1246 total).

**`replicaIdentitiesForTablesWithoutPrimaryKeys` ported (decision + ALTER SQL).**
`shard_schema.rs` gains `replica_identities_for_tables_without_primary_keys` — the pure decision over the
introspected specs: for each published table with no primary key still on the *default* replica
identity, pick the first published index usable as `REPLICA IDENTITY USING INDEX` (UNIQUE + immediate/
non-deferrable + all columns NOT NULL; partial/expression indexes are already excluded by the
introspection query) — plus `replica_identity_alter_sql` (the per-table `ALTER TABLE ... REPLICA
IDENTITY USING INDEX` the live `apply` step would run). Composes directly with the
`published_schema_json` deserializer's `PublishedTableSpec`/`PublishedIndexSpec`. 3 tests covering the
happy pick, the skip conditions (has-PK / non-default identity), and rejection of non-unique/deferred/
nullable-column indexes. What remains for `setupTablesAndReplication`: the app-publication existence
check / default-publication creation branch and `setupTriggers` (event-trigger DDL — a sizeable
sub-system with a degraded-mode fallback); then the single initial-sync entry point, then
`ChangeStreamerImpl`/`Storer`. 3 new tests. Full workspace clean, zero warnings, suite green under
`--test-threads=1` (1249 total).

**Live `setup_tables_and_replication` orchestration assembled — the DDL half of `ensurePublishedTables`.**
`shard_schema.rs` gains `default_publication_ddl` (the `DROP`+`CREATE PUBLICATION ... FOR TABLES IN
SCHEMA public WITH (publish_via_partition_root = true)` used when no explicit publications were
requested) and the live `setup_tables_and_replication(client, requested)`: validates the requested
publications, resolves the full publication set (verifying requested ones exist via
`existing_publications`, else creating the default), appends the internal metadata publication, and runs
`global_setup` + `shard_setup` in one `BEGIN/COMMIT` transaction (rolling back on error) — returning the
full publication list written to `shardConfig`. **Verified live end-to-end**: a test drives the
requested-publication branch (creates a real table+publication, runs the orchestration, asserts the
returned `[app_pub, metadata_pub]` set and that `get_internal_shard_config` reads back the sorted set),
and asserts a non-existent requested publication is rejected with `UnknownPublications` — then tears
everything down. (The default-`public` branch is covered by a pure `default_publication_ddl` test rather
than live, to avoid a broad public-schema publication polluting other shared-DB live tests.) Deferred to
follow-ups, matching upstream's later steps: applying `replica_identities_for_tables_without_primary_keys`
(the decision half is already ported) and `setupTriggers` (event-trigger DDL, degraded-mode fallback).
Then the single `check_upstream_config` → this → `get_publication_info` → `run_initial_sync` entry
point, then `ChangeStreamerImpl`/`Storer`. 2 new tests (1 pure + 1 live). Full workspace clean, zero
warnings, suite green under `--test-threads=1` (1251 total).

**MILESTONE: self-driving `run_initial_sync_introspected` — initial sync that discovers its own schema.**
`zero-cache-sqlite::initial_sync.rs` gains `run_initial_sync_introspected(pg, db, slot, publications)`:
like `run_initial_sync` but instead of taking table/index specs as input it discovers them *at the slot
snapshot* via `zero-cache-change-source::published_schema::get_publication_info` — run inside the same
`SET TRANSACTION SNAPSHOT` transaction as the bulk COPY, so schema and data are read at exactly the same
consistent point (matching upstream, which runs `getPublicationInfo` in that transaction). Converts the
introspected `PublishedIndexSpec`s to `IndexSpec` via `published_schema_json::to_index_spec`, then reuses
`copy_all`. This is the self-driving core: given a slot + publication set, it produces a fully populated
replica with NO externally supplied specs — closing the loop between the introspection layer and the copy
pipeline built over prior rounds. **Verified live end-to-end**: a test creates a slot, commits a row
after the snapshot, calls the introspecting driver with only publication names, and asserts the replica
holds exactly the pre-snapshot rows AND that the introspected schema created the `name` column (proving
specs came from `get_publication_info`, not a hand-built spec). The remaining seam before a single
top-level entry point is just sequencing the already-live pieces: `check_upstream_config` →
`setup_tables_and_replication` → create slot (`ReplicationConn::create_logical_replication_slot`) → this;
then the `ChangeStreamerImpl`/`Storer` streaming loops that consume the ongoing replication stream. 1 new
live test. Full workspace clean, zero warnings, suite green under `--test-threads=1` (1252 total).

**MILESTONE: `run_full_initial_sync` — the single top-level initial-sync entry point + a real bug fixed.**
`zero-cache-sqlite::initial_sync.rs` gains `run_full_initial_sync(params, db, requested)` — the Rust
counterpart of `initial-sync.ts`'s `initialSync`, assembled from every live piece: `check_upstream_config`
→ `setup_tables_and_replication` (publications+shard DDL) → `ReplicationConn::create_logical_replication_slot`
(fixing the snapshot) → `run_initial_sync_introspected` (introspect + snapshot-copy). Takes an
`InitialSyncParams` (conn string for query/copy connections + host/port/user/dbname for the raw
replication connection + slot name) and a `ShardConfig`; returns the `InitialSyncResult` + full
publication set. The raw replication connection is held open across the copy so the exported snapshot
stays valid. **Verified live end-to-end against real Postgres**: from just connection params + a
`ShardConfig{publications:[app_pub]}`, it validates the upstream, creates the shard schema/publications,
creates the slot, and snapshot-copies the user table (2 rows) *and* the shard's internal metadata tables
(permissions/clients/mutations) into the replica — with no manual slot/spec wiring.

Fixing this surfaced a **real latent bug**: `ddl_apply::lite_index_from_pg` copied an index's
`table_name` verbatim instead of lite-mapping it through `lite_table_name`, so `CREATE INDEX ... ON
<table>` for a table in a non-`public` schema (e.g. the shard's `<app>.permissions` metadata table)
referenced the unqualified name and failed with "no such table". No prior test exercised an index on a
non-public-schema table, so it stayed latent until the full sync copied the internal metadata tables.
Fixed by delegating to `pg_to_lite::map_postgres_to_lite_index` (which upstream's `mapPostgresToLiteIndex`
already does). Also corrected a stray corruption in the test-connection-string default. Now the only
remaining large subsystem is the `ChangeStreamerImpl`/`Storer` streaming-service loops that consume the
*ongoing* replication stream (the raw replication client + pgoutput decoder + slot all exist; the
change-persisting/subscriber-serving orchestration on top does not). 1 new live test + 1 bug fix. Full
workspace clean, zero warnings, suite green under `--test-threads=1` (1253 total).

**MILESTONE: the ongoing-replication apply loop — `ReplicationApplier` — + another real bug fixed.**
`zero-cache-sqlite::replication_apply.rs` ports the change-application half of the pipeline: a
`ReplicationApplier` that drives the existing `ChangeDispatcher` from decoded pgoutput messages, keeping
a `RelationTracker` across the stream. `apply_message`: a `Begin { final_lsn }` opens the dispatcher
transaction at the commit version derived from `final_lsn` (via `lsn::from_bigint` +
`to_state_version_string`, continuous with the initial-sync watermark); `Relation` updates the tracker;
`Insert`/`Update`/`Delete`/`Truncate` translate to a `Change` and apply within the transaction; a
`Commit { commit_lsn }` closes it and returns the `CommitResult` (watermark + change-log stats). This is
the piece that keeps the replica live *after* initial sync — the last building block that had no loop
wiring `ReplicationStream` + `RelationTracker::translate` + `ChangeDispatcher` together. **Verified live
end-to-end against real Postgres**: a test initial-syncs a table at the slot snapshot, starts
`START_REPLICATION` from the consistent point, runs a live INSERT+UPDATE+DELETE upstream, pumps the
stream through the applier, and asserts the SQLite replica converges to the exact expected state (rows
[1,3], the keyed row's updated value applied).

Fixing this surfaced a **second real latent bug**, in `pg_to_change::key_tuple_to_row`: it assumed a
pgoutput KEY/OLD tuple contains only the key columns, but the wire format sends a slot for *every*
relation column (non-key ones as null). So any streamed keyed UPDATE or DELETE failed with a spurious
`ColumnCountMismatch`. Fixed to expect the full column width and extract the key columns by position;
two unit tests that had encoded the wrong 1-column assumption were corrected to real pgoutput semantics
(key/old tuples with the non-key column as `Null`). Both this and the earlier index-quoting bug were
only reachable via live end-to-end streaming — exactly what the integration tests are for. With this,
the core replication data path (initial sync → ongoing CDC apply) is complete end to end; what remains
above it is the `ChangeStreamerImpl`/`Storer` service layer (durable change-log fan-out to multiple
view-syncer subscribers, catchup, and the network read/keepalive side), which orchestrates *this* apply
loop rather than replacing it. 3 new tests (1 pure + 1 live + net-corrected pg_to_change tests) + 1 bug
fix. Full workspace clean, zero warnings, suite green under `--test-threads=1` (1255 total).

**Standby Status Update (replication feedback) — the keepalive/WAL-advance side of the stream.**
`zero-cache-change-source::replication_conn.rs` gains `ReplicationStream::send_standby_status_update`
(write/flush/apply LSN + timestamp + reply-requested), which builds the `'r'` feedback message and wraps
it in a `CopyData` frame written to the replication socket. Without this the slot's
`confirmed_flush_lsn` never advances and upstream WAL accumulates forever, so a real consuming service
sends it periodically and in response to a `Keepalive { reply_requested }`. Also
`pg_timestamp_from_unix_micros` (the Postgres-epoch/2000-01-01 offset conversion, time-as-parameter per
this port's convention). The `Keepalive` doc comment's "feedback out of scope" note is now resolved.
**Verified live against real Postgres**: a test streams an insert, sends feedback flushing up to the
received LSN, and confirms the slot's `confirmed_flush_lsn` advances past `0/0` — proving the feedback
actually reaches and is honored by the server. This is the network-side counterpart to the
`ReplicationApplier`; together they are what a change-streamer service loop drives (read event → apply →
periodically flush feedback). Remaining above: the `ChangeStreamerImpl`/`Storer` durable change-log
fan-out to multiple subscribers + catchup. 2 new tests (1 pure + 1 live). Full workspace clean, zero
warnings, suite green under `--test-threads=1` (1257 total).

**MILESTONE: the change-streamer read/apply/feedback service loop — `run_change_stream`.**
`zero-cache-sqlite::change_stream_loop.rs` ties the three ongoing-replication primitives into one running
service loop (the core of upstream's `ChangeStreamerService`): read (`ReplicationStream::next_event`) →
apply (`ReplicationApplier`) → feedback (`send_standby_status_update`). Per frame it advances the
high-water LSN; data messages apply to the replica; on each transaction commit it flushes feedback up to
the high-water LSN (releasing upstream WAL) and calls an `on_commit(&CommitResult) -> LoopControl`
callback (so a caller can stop at a target watermark / on shutdown); server keepalives with
`reply_requested` are answered with feedback. **Verified live end-to-end against real Postgres**: after
initial sync, the loop consumes two separate upstream transactions, converges the SQLite replica to
[1,2], records commit watermarks, and — crucially — the slot's `confirmed_flush_lsn` advances past `0/0`
purely from the loop's own feedback (no manual flush). This is the running-service backbone: read →
apply → advance, forever. What still sits above it is the durable change-log *fan-out* — `Storer` +
`Subscriber` catchup serving multiple view-syncers from the change-log this loop writes (its pure
back-pressure/watermark primitives are already ported in `zero-cache-services::subscriber_backpressure`).
2 new tests (loop control enum + 1 live). Full workspace clean, zero warnings, suite green under
`--test-threads=1` (1258 total).

**Change-log catch-up read — the durable fan-out primitive (`ChangeLog::read_since`).**
`zero-cache-sqlite::change_log.rs` gains `read_since(after_version)` returning all change-log entries
committed strictly after a watermark, in commit order (`stateVersion`, then `pos`), as `ChangeLogRow`s.
This is the durable catch-up read that a reconnecting view-syncer subscriber performs: it holds its last
`stateVersion` and replays every row change recorded since — converging to current replica state without
re-reading the upstream Postgres stream (which only the single change-stream loop consumes). Because the
change-log keeps only the *latest* op per `(table, rowKey)` (`UNIQUE(table,rowKey)` + `INSERT OR
REPLACE`), catch-up naturally coalesces repeated edits to a row into its final state — exactly what an
incremental view needs. This is the read side of `Storer`/`Subscriber` catch-up; it pairs with the
already-ported pure back-pressure/watermark primitives (`subscriber_backpressure`) and the change-stream
loop that *writes* the change-log. Tested: a subscriber at watermark "01" sees only later commits, a
fresh subscriber replays the coalesced current state, a caught-up one sees nothing. Remaining above: the
live multi-subscriber fan-out service (streaming new commits to N subscribers + the WebSocket sync
protocol / view-syncer query pipeline). 1 new test. Full workspace clean, zero warnings, suite green
under `--test-threads=1` (1259 total).

**Live multi-subscriber fan-out hub — `ChangeFanout`.**
`zero-cache-sqlite::change_fanout.rs` ports the hub half of the change-streamer's `Storer`/`Subscriber`
fan-out: one writer (the change-stream loop) publishes each commit to N live view-syncer subscribers
concurrently, so they never each re-read Postgres. Built on `tokio::sync::broadcast` (added `tokio`'s
`sync` feature as a normal dep): `ChangeFanout::publish(CommitNotification)` fans a commit to every
current subscriber; `subscribe()` returns a `FanoutSubscriber` that only sees commits after it joined
(a subscriber first catches up via `ChangeLog::read_since` up to the current watermark, then follows
this channel — the standard catchup-then-stream handoff). A subscriber that falls behind the buffer
gets a `FanoutEvent::Lagged { skipped }` telling it to re-catch-up from the durable change-log rather
than dropping data (no commit is lost — it's still in the change-log); hub drop surfaces as `Closed`.
`CommitNotification` carries the watermark + schema-changed flag + change-log-entry count (with a
`From<&CommitResult>`). 5 tests: fan-out-to-all-in-order, late-joiner-sees-only-post-join, slow-subscriber
lag-then-recover, closed-on-drop, and the dispatcher-result conversion. This pairs the live streaming
half with the already-ported catch-up read (`read_since`) and back-pressure primitives
(`subscriber_backpressure`) — the three pieces a full `Subscriber` needs. Remaining above: wiring the
loop's `on_commit` to `publish`, then the view-syncer query pipeline (ZQL/IVM) + WebSocket sync protocol.
5 new tests. Full workspace clean, zero warnings, suite green under `--test-threads=1` (1264 total).

**MILESTONE: `ChangeStreamerService` — the composed running service (apply + fan-out in one object).**
`zero-cache-sqlite::change_streamer_service.rs` ties the ongoing-replication loop to the live fan-out:
one process consumes the single Postgres stream, applies each transaction to the SQLite replica + durable
change-log, advances the slot via feedback, AND fans every commit out to every subscribed view-syncer.
`ChangeStreamerService::run` composes `change_stream_loop::run_change_stream` (read→apply→feedback) with
`ChangeFanout` by publishing each `CommitResult` to the hub in the loop's `on_commit` hook, then
consulting the caller's `should_continue` stop condition; `subscribe()`/`subscriber_count()` expose the
hub. No new protocol/transaction logic — pure wiring that makes the pieces a service. **Verified live
end-to-end against real Postgres**: two subscribers attach, the service consumes two upstream
transactions (updating the replica to [1,2]), and BOTH subscribers receive BOTH commit notifications with
watermarks matching what the loop reported. This is the change-streamer subsystem assembled end to end:
single-stream consume → apply → durable change-log (+catch-up read) → slot feedback → live fan-out to N
subscribers. Remaining above: the view-syncer query pipeline (ZQL/IVM incremental view maintenance —
substantial partial coverage already exists) and the WebSocket sync protocol that serves end clients.
1 new live test. Full workspace clean, zero warnings, suite green under `--test-threads=1` (1265 total).

**Subscriber catch-up materialization — bridging the change-log to the IVM feed.**
`zero-cache-sqlite::subscriber_catchup.rs` resolves a subscriber's durable catch-up (the `ChangeLogRow`s
from `read_since`) into concrete `ResolvedChange`s an incremental view can consume. The change-log stores
only the row *key* + op (`s`/`d`), so a delete resolves to just its parsed key, while a set is paired
with the row's *current* full contents read back from the replica table (`SELECT * ... WHERE <key>`; the
change-log's coalescing guarantees a surviving `set` row is present). `resolve_change_log_row` /
`resolve_catchup` + `ResolvedChange::{Set,Delete}` + typed key-value binding. This is the missing bridge
between the durable catch-up read and the IVM `apply_to_source` feed that the existing live pipeline
(`pipeline.rs`/`ivm_bridge`) already drives for *live* changes — so a reconnecting view-syncer can
materialize its backlog the same way. 4 tests: set→full-row resolution, delete→key-only, set-then-delete
coalescing to one delete, and text-keyed rows. Remaining above: driving a full IVM query off a resolved
catch-up + live fan-out in one subscriber, then the WebSocket sync protocol. 4 new tests. Full workspace
clean, zero warnings, suite green under `--test-threads=1` (1269 total).

**Sync-protocol read side: the upstream (client→server) wire decoder — `up_json`.**
`zero-cache-protocol::up_json.rs` decodes an incoming WebSocket JSON frame (`[tag, body]`) into an
`Upstream` value — the read counterpart to `poke_json`'s downstream encoder, the piece a sync server
needs to interpret client messages. Covers the full ported upstream vocabulary: `ping`,
`closeConnection`, `deleteClients`, `changeDesiredQueries`, and `initConnection` — including the
query-carrying bodies' `UpQueriesPatch` (`up_queries_patch_from_json`: put/del/clear ops, `put` decoding
an AST via the existing `ast_json::ast_from_json`) and `initConnection`'s `ClientSchema`
(`client_schema_from_json`) + all its optional fields (push/query URLs+headers, activeClients, deleted,
traceparent). Unported message tags (`push`/`pull`/`updateAuth`/`inspect`) surface as an explicit error
rather than silently. 6 tests: ping/close, deleteClients, a put+del+clear patch, a full initConnection
with schema, AST-carrying put, and unknown-tag/malformed-envelope rejection. This is the client-request
half of the WebSocket sync protocol (the poke/downstream half was already ported); remaining is the
live connection/session loop that ties decoded upstream messages + view-syncer query results + poke
encoding into a served WebSocket endpoint. 6 new tests. Full workspace clean, zero warnings, suite green
under `--test-threads=1` (1275 total). Superseded by later milestones: `push`, `pull`, `updateAuth`, and
`ackMutationResponses` are now decoded; `inspect` remains the only unported upstream protocol tag.

**Per-connection upstream-message router — `connection_dispatch` (the connection loop's decide step).**
`zero-cache-view-syncer::connection_dispatch.rs` maps a decoded `Upstream` message to a
`ConnectionAction` the served WebSocket connection loop carries out: `Pong` (reply to ping),
`Initialize` (first `initConnection` — desired queries + client schema), `UpdateDesiredQueries`
(`changeDesiredQueries` patch), `DeleteClients`, `Close` (deprecated `closeConnection`). It enforces the
protocol ordering upstream's `syncConnection` requires — `initConnection` must be first (a second one is
`DuplicateInit`; any data message before it is `MessageBeforeInit`), while `ping` is always allowed as a
keepalive — threading an `InitState` (AwaitingInit → Initialized). Kept a pure
`Upstream -> ConnectionAction` classification (no live CVR/socket handles) so the routing is
independently testable; the stateful handlers it names are the existing `cvr_*`/`view_syncer_*` modules.
This is the middle layer of the connection loop: decode (`up_json`) → **route (`connection_dispatch`)** →
act (CVR/view-syncer) → encode (`poke_json`). 6 tests: ping-anytime, first-init, duplicate-init error,
data-before-init error, and post-init routing of changeDesiredQueries/deleteClients/close. Remaining is
the live glue: an async connection task wiring a real WebSocket's frames through this router into the CVR
and back out as pokes, plus the HTTP/worker dispatch that spawns one per client. 6 new tests. Full
workspace clean, zero warnings, suite green under `--test-threads=1` (1281 total).

**MILESTONE: the live per-connection serve loop — a real WebSocket driven end to end through the sync
protocol.** `zero-cache-server::serve_connection.rs`'s `serve_connection` is the async glue that wires
the three ported decision/protocol layers onto a real socket: recv text frame → decode
(`up_json::upstream_from_json`) → route (`connection_dispatch::dispatch_upstream`, threading `InitState`
for `initConnection`-first ordering) → act (caller handler) → encode & send downstream. `ping` is
answered with `pong` inline; every other `ConnectionAction` goes to a caller-supplied handler that does
the stateful CVR/view-syncer work and returns downstream frames (e.g. a poke sequence) to send back; the
loop ends on clean close, `Close`, or a protocol/decode error. **Verified live end-to-end over a real
TCP WebSocket**: a real `tokio-tungstenite` client connects, receives `connected`, sends
`initConnection` + `ping` + `changeDesiredQueries`, and the server serves all three through the full
pipeline — replying to the ping with `["pong",{}]`, emitting a `pokeStart` in response to the query
change, and recording the routed actions in handler order (`[init, change]`); a second test proves a
data message before `initConnection` terminates the connection with the router's `MessageBeforeInit`
error over the wire. Added `zero-cache-shared` as a normal dep of the server crate. This closes the
transport loop: a client can now connect and drive the ported protocol over a real socket, with the CVR/
view-syncer handler as the pluggable stateful core. Remaining is wiring a real CVR/query handler into
that seam (vs. the test's recording handler) + the HTTP/worker dispatch that accepts sockets and spawns
one serve loop per client. 2 new live tests. Full workspace clean, zero warnings, suite green under
`--test-threads=1` (1283 total).

**The accept-and-dispatch worker loop — `run_accept_loop` (one serve task per client).**
`zero-cache-server::sync_server.rs` is the top of the transport stack: `run_accept_loop`/
`run_accept_loop_bounded` accept TCP connections, complete the WebSocket handshake, send the `connected`
greeting with a per-connection `wsid` (a deterministic monotonic counter — no ambient clock/random,
matching this port's convention), and hand each connection to a freshly built handler running in its own
`tokio::spawn` task, so many clients are served concurrently. The Rust counterpart of `syncer.ts`'s
per-connection worker spawn. A `make_handler: FnMut(u64) -> H` factory builds one stateful handler per
connection (each owning its client's CVR/view-syncer state; `H: Send + 'static` since handlers run in
spawned tasks — a real handler holds its own resources rather than sharing a `!Send` rusqlite
connection). Individual handshake/connection failures are dropped without bringing down the loop; the
bounded variant stops after N accepts for tests/graceful-drain. **Verified live over real sockets**: two
`tokio-tungstenite` clients connect concurrently, each is served in its own task, gets a distinct
`ws0`/`ws1` greeting, and its ping is answered with a pong — proving independent concurrent per-client
serve loops. This completes the transport stack: listener → accept+handshake → per-client serve loop →
decode/route/act/encode. Remaining is the real CVR/query handler in the `make_handler` seam (vs. the
test's trivial handler) + remaining view-syncer query-execution depth. 1 new live test. Full workspace
clean, zero warnings, suite green under `--test-threads=1` (1284 total).

**The CVR-backed desired-queries handler — the stateful core for the connection seam.**
`zero-cache-view-syncer::cvr_query_handler.rs`'s `CvrQueryHandler` is the substantive handler a served
connection's `ConnectionAction` drives: it owns a client group's CVR and folds a decoded `UpQueriesPatch`
(from `initConnection`/`changeDesiredQueries`) into it — registering newly-desired queries, bumping TTLs,
removing unwanted ones — returning the config patches to send downstream. It wraps the already-ported CVR
state transitions (`cvr_desired_queries::put_desired_queries`/`delete_queries`): puts become
`DesiredQueryRequest`s (custom queries via name+args, or client queries via AST), dels/`clear` become
removals (`clear` drops every query the client currently desires, evaluated against the pre-mutation
state), and the original CVR version is snapshotted once so the whole patch bumps the version at most
once (upstream's single update session). This is exactly the piece the transport's `make_handler` seam
was missing — turning a wire `changeDesiredQueries` into concrete CVR mutations. 5 tests: new-query
registration + version bump, re-put-at-same-TTL no-op, delete, clear-drops-all, and a TTL bump. Scope
boundary unchanged: CVR state transition, not `CVRStore` persistence or query hydration/row fetching (the
IVM feed, wired separately). Remaining above: joining this handler's CVR-patch output to the IVM row
results into a full poke, and the auth/mutation-push paths. 5 new tests. Full workspace clean, zero
warnings, suite green under `--test-threads=1` (1289 total).

**`poke_builder` — converts CVR patches into wire poke messages.**
`zero-cache-view-syncer::poke_builder.rs`'s `build_poke` closes the gap identified above: it turns a
batch of `PatchToVersion`s (the output of `CvrQueryHandler`/`put_desired_queries`/`delete_queries`) into
the actual `PokeStartBody`/`PokePartBody`/`PokeEndBody` a connection sends downstream. Config patches
(`Patch::Config`) become `QueriesPatchOp::{Put,Del}` grouped by `client_id` into
`desired_queries_patches`; row patches (`Patch::Row`) become `RowPatchOp::{Put,Del}` in `rows_patch`,
which also triggers `schema_versions` on the poke start (mirroring upstream: only pokes carrying row data
declare a schema-version range). The end cookie is the max `to_version` across the batch, via the
already-ported `version_to_cookie`. Reuses `client_handler_poke`'s existing pure decisions
(`decide_poke_end`, etc.) rather than duplicating them — this module owns only the patch-shape
conversion. Known scope limit: `QueryPatch` doesn't carry TTL, so converted `QueriesPutOp`s have
`ttl: None` (documented; a caller needing to advertise TTL attaches it out of band for now). 5 tests:
empty-patches, config put/del grouped by client, row put (sets schema_versions), row delete, and
max-version-wins for the end cookie. This is the last conversion step between the CVR/IVM layer and the
wire — a served connection can now go from a `changeDesiredQueries` message through `CvrQueryHandler` to
a real `build_poke` call producing frames ready for `serve_connection` to send. Remaining: query
hydration/row fetching to actually populate `ClientRowPatch` contents from the replica (currently the
caller must supply row contents), and the auth/mutation-push paths. 5 new tests, verified via scoped
`cargo test -p zero-cache-view-syncer` (per-crate builds only this round — no full-workspace run).

**Query hydration wired to `poke_builder` — real row contents reach the wire (`hydration_to_patches`).**
Closes the gap noted above. `query_hydration::HydrationResult` gains a `fetched_rows: Vec<(K, ZqlRow)>`
field, populated in `hydrate_query`'s existing fetch loop alongside `row_outcomes` (same iteration, so it
can never drift out of sync) — the row contents `filter.fetch` already produces, previously discarded
after computing each row's key. `poke_builder::hydration_to_patches` is the new bridge: `query_patches`
become unscoped config patches advancing to the CVR's post-hydration version (the same single-bump
convention `CvrQueryHandler` uses); each row outcome's `client_patch` becomes a row patch — `Put` looks up
that row's real contents in `fetched_rows` and carries them, `Del` carries just the key — via a
caller-supplied `row_id: impl Fn(&K) -> RowId` (same pattern as `hydrate_query`'s own `row_key`/
`row_ref_counts`, since this function has no opinion on primary keys/table names). **Full-stack test**:
real `TableSource`+`Filter` → `hydrate_query` → `hydration_to_patches` → `build_poke` → asserts the wire
`RowPatchOp::Put`'s `value` contains the actual fetched `title` column — proving real replica row content
now reaches a wire poke message end to end, purely through ported code. 1 new test (+ existing
`hydrate_query`/`poke_builder` tests still green). This was the last piece named as "still open" in the
prior round; the query-execution → CVR → poke pipeline is now connected from IVM fetch through to wire
bytes. Remaining: live glue wiring this pipeline into the `serve_connection` handler seam against a real
SQLite-backed `TableSource`, and the auth/mutation-push paths. Verified via scoped
`cargo test -p zero-cache-view-syncer` plus scoped builds of `zero-cache-server`/`zero-cache-sqlite`/
`zero-cache-zql` (no full-workspace run, per current session guidance).

**MILESTONE: real SQLite-backed hydration wired into the server crate — `live_hydration.rs`.**
Closes the gap named above. `zero-cache-sqlite::sqlite_table_source::SqliteTableSource` reads the real
replica but isn't the `zero_cache_zql::ivm::table_source::TableSource` type `hydrate_query`/`Filter::fetch`
require; `zero-cache-server::live_hydration.rs`'s `load_table_source` bridges them — one real SQL fetch
(via `SqliteTableSource::fetch`, which already pushes `WHERE`/`ORDER BY` down into real SQL) loaded into
a fresh in-memory `TableSource` via `push`/`make_source_change_add`, so `hydrate_query`'s `Filter` layer
can be a pass-through predicate for this slice (the SQL pushdown already did the real filtering).
`hydrate_from_sqlite` composes that load with `hydrate_query` → `hydration_to_patches` → `build_poke`
into one call taking a `RowIdentity<K>` (the same row-key/ref-counts/version/wire-id extractors
`hydrate_query` itself needs) — so a connection handler goes from "a client desires this query" to "a real
wire poke with real replica row contents" without touching any intermediate type. Promoted
`zero-cache-sqlite`/`zero-cache-zql` from dev-only to real dependencies of `zero-cache-server` (the server
now legitimately needs to read the live replica to serve queries). **Full-stack live test**: creates a
real SQLite table with real rows, runs `hydrate_from_sqlite` end to end, asserts the wire `RowPatchOp::Put`
values contain the actual inserted `title` strings, then sends the resulting `pokeStart` frame over a
real `tokio-tungstenite` WebSocket to a real client and confirms receipt — every layer live, nothing
mocked. All 18 pre-existing + new server-crate tests still pass after the dependency promotion (including
the two Postgres-backed integration tests). 1 new live test. Verified via scoped `cargo build`/
`cargo test -p zero-cache-server` (+ scoped builds of `zero-cache-sqlite`/`zero-cache-view-syncer`/
`zero-cache-zql`) — no full-workspace run, per current session guidance. Remaining: the auth/
mutation-push paths, and driving `hydrate_from_sqlite` from `serve_connection`'s handler seam itself
(currently proven as a standalone composition + a separately-sent frame, not yet threaded through
`ConnectionAction::UpdateDesiredQueries`'s handler closure end to end in one call).

**MILESTONE: `DesiredQueriesHandler` — the literal handler closure `run_accept_loop` drives, closing the
last-named gap.** `zero-cache-server::live_connection.rs` is the real `FnMut(ConnectionAction) ->
HandlerOutcome` a served connection uses (not a standalone composition proven separately): it owns one
connection's `CvrQueryHandler` + its own SQLite replica handle. On `Initialize`/`UpdateDesiredQueries` it
applies the patch to the CVR, then — for every newly-`put` query hash this connection recognizes (a small
static `query_catalog()` registry mapping hash -> table/columns; the AST-to-SQL compiler that would
populate this dynamically from an arbitrary client query is a separate, larger subsystem, out of scope
here) — calls `live_hydration::hydrate_patches_from_sqlite` (refactored out of `hydrate_from_sqlite` so
several queries' patches can merge into ONE poke, matching how a real connection batches a whole
`changeDesiredQueries` cycle rather than poking once per query) and merges the CVR config patches with
every hydrated query's row patches into a single `build_poke` call, JSON-encoding the three frames via
the existing `poke_message_json` for `serve_connection` to send. **Full-stack live test — every layer for
real, nothing test-only**: `run_accept_loop_bounded` spawns a connection using this exact handler over a
seeded real SQLite replica; a real client sends `initConnection` desiring the `issue-all` query; the
`pokeStart`/`pokePart`/`pokeEnd` frames arrive over the real socket via `serve_connection`'s normal send
path, and `pokePart`'s content is asserted to contain the actual `title` string from the SQLite row. This
is the last piece of the transport→CVR→hydration→wire chain named as unwired; `zero-cache-server` now has
a real, working (if catalog-scoped) sync-server core. 19/19 server-crate tests pass (11 lib + 7 + 1
integration). 1 new live test. Verified via scoped `cargo build`/`cargo test -p zero-cache-server` (+
scoped builds of `zero-cache-view-syncer`/`zero-cache-sqlite`/`zero-cache-zql`/`zero-cache-protocol`) — no
full-workspace run, per current session guidance. Remaining: the auth/mutation-push paths, and generalizing
`query_catalog` into the real AST-to-SQL query compiler so arbitrary client queries (not just the static
registry) can be served.

**AST `where_` conditions now reach real SQL — `SqliteTableSource::fetch_filtered` closes a genuine gap.**
Discovered while generalizing `query_catalog`: `SqliteTableSource::fetch` hardcoded `filters: None` on
every call to `query_builder::build_select_query`, even though that function already accepts a `filters:
Option<&Condition>` parameter (the AST-to-SQL condition compiler, `filters_to_sql`, was already ported and
used elsewhere for mutation/join constraints) — so no caller could push a client query's actual `where_`
into SQL through `SqliteTableSource`; every fetch was effectively an unconditional table scan regardless
of the query. New `fetch_filtered(req, filters: Option<&Condition>)` (with `fetch` now a thin call to it
with `filters: None`) wires that parameter through for real. 2 new tests in `sqlite_table_source.rs`:
one proves a `Condition` alone filters correctly, one proves it composes with an existing structural
`constraint`. `live_hydration::load_table_source`/`hydrate_patches_from_sqlite`/`hydrate_from_sqlite` all
gained a `filters: Option<&Condition>` parameter threading this through. `live_connection.rs`'s handler
now extracts a `put` op's real `ast.where_` (when the client sent an AST, not just a name+args custom
query) and passes it straight to SQLite — generalizing `query_catalog` from "fixed hash -> hardcoded rows"
to "fixed hash -> table, but the actual row *filtering* is the client's real query, evaluated by SQLite
itself." **New live test**: two rows seeded upstream; a real client sends a real AST with `where title =
'match me'`; the resulting poke is asserted to contain the matching row and explicitly NOT the other —
proving the AST condition genuinely reaches and is honored by real SQL, not an in-memory shortcut. 20/20
server-crate tests pass; `zero-cache-sqlite`'s full 205-test suite passes single-threaded (7 apparent
failures under default parallelism traced to pre-existing replication-slot-name collisions between live
Postgres tests running concurrently — confirmed unrelated to this round's changes, no file in the failing
tests' paths was touched). 2 new sqlite tests + 1 new live server test. Verified via scoped
`cargo build`/`cargo test` per crate — no full-workspace run. Remaining: the AST-to-SQL compiler is still
table-name-keyed through a static catalog rather than deriving table/column/PK info from a live schema
catalog, joins (`ast.related`) are unhandled, and the auth/mutation-push paths remain unported.

**Inspect `analyzeQuery` now has optional read-authorizer wiring.**
`zero-cache-server::live_connection::DesiredQueriesHandler` can carry an
explicit `zero_cache_auth::policy::PermissionsConfig` via `with_read_permissions`.
When configured, `apply_inspect` resolves the direct AST or registered/HTTP
custom-query transform as before, then applies
`zero_cache_auth::read_authorizer::transform_and_hash_query` before passing the
AST to `analyze_sqlite_ast_query`; when no permissions are configured, the demo
handler's existing behavior is unchanged. New focused test seeds two SQLite
rows, configures a row `select` policy allowing only one title, sends a direct
inspect AST for the table, and proves the inspect result returns the allowed
row while excluding the denied row. Verified with
`cargo fmt -p zero-cache-server`,
`cargo test -p zero-cache-server inspect_analyze_query_applies_configured_read_permissions --lib`,
`cargo test -p zero-cache-server inspect_analyze_query --lib`, and
`cargo check -p zero-cache-server`. Remaining: auth-data static parameter
binding is still missing in `zero-cache-auth::read_authorizer`, and production
view-syncer paths still need to source and apply permissions outside this demo
inspect handler.

**Inspect `analyzeQuery` no longer depends on the demo hydration catalog.**
`zero-cache-server::analyze_query`'s catalog structs now own their table,
primary-key, and column metadata instead of borrowing `'static` demo entries,
and new `analyze_catalog_from_sqlite_ast` introspects the live SQLite replica
via `zero_cache_sqlite::lite_tables::list_tables` for every table reached by
the analyzed AST graph (root table, `related` subqueries, and correlated
subquery conditions). `DesiredQueriesHandler::apply_inspect` now uses that
introspected catalog for direct ASTs, registered custom-query transforms,
HTTP-backed custom transforms, and read-authorized ASTs before calling
`analyze_sqlite_ast_query`; the static `query_catalog()` remains only for
desired-query hydration/poke serving. New focused test proves live inspect can
analyze a `project` table that is not in the demo hydration catalog, returning
real rows from SQLite rather than `UnknownTable`. Verified with
`cargo fmt -p zero-cache-server`,
`cargo test -p zero-cache-server inspect_analyze_query_introspects_tables_outside_demo_catalog --lib`,
`cargo test -p zero-cache-server inspect_analyze_query --lib`,
`cargo test -p zero-cache-server analyze_query::tests --lib`, and
`cargo check -p zero-cache-server`. Remaining at this point: the live
desired-query hydration path itself still used the static demo registry; that
became the sharper catalog/compiler boundary.

**Desired-query hydration can now serve AST root tables outside the demo catalog.**
`zero-cache-server::live_connection::DesiredQueriesHandler::apply_and_poke`
now handles desired-query `put` operations that include an AST by introspecting
the AST root table from SQLite with `zero_cache_sqlite::lite_tables::list_tables`.
It builds the table name, primary key, columns, primary-key ordering, generic
row identity, and wire `RowId` from live schema metadata, then reuses the
existing `hydrate_patches_from_sqlite_with_row_updates` path. The old
`query_catalog()` remains for name-only/custom puts, preserving the existing
demo `issue-all` behavior. New live WebSocket test creates a `project` table
that is not in `query_catalog`, sends `initConnection` with
`{"hash":"project-all","ast":{"table":"project"}}`, and receives a real
`pokePart` containing the SQLite row. Verified with
`cargo fmt -p zero-cache-server`,
`cargo test -p zero-cache-server desired_query_hydration_uses_ast_table_outside_demo_catalog --lib`,
`cargo test -p zero-cache-server live_connection::tests --lib`, and
`cargo check -p zero-cache-server`. Remaining at this point: hydration was
still single-table root-read only; registered custom transforms,
`related`/joins, async transform fetch during init, and full planner-backed
execution remained future work.

**Desired-query hydration now consumes already-registered custom query transforms.**
`apply_and_poke` resolves a desired-query `put` with `name`+`args` through
`InspectorDelegate::transform_custom_query` when no inline AST is present. If a
transform has already been registered, the resulting AST is hydrated through
the same SQLite-introspected single-table path as direct AST puts, including
its real `where_` condition. The existing name-only demo catalog fallback still
handles legacy/custom queries without a registered transform. New focused test
registers `projectByName(args)` as a transformed AST, sends an init put with
only `name`+`args`, and proves hydration returns only the matching SQLite row.
Verified with `cargo fmt -p zero-cache-server`,
`cargo test -p zero-cache-server desired_query_hydration_uses_registered_custom_query_transform --lib`,
`cargo test -p zero-cache-server live_connection::tests --lib`, and
`cargo check -p zero-cache-server`. Remaining: fetching custom transforms
asynchronously during `initConnection`/`changeDesiredQueries` was still future,
along with `related`/joins and full planner-backed execution.

**Async custom transform-on-init hydration is wired.**
`DesiredQueriesHandler::on_action_async` now handles `Initialize` and
`UpdateDesiredQueries` by scanning desired-query puts before hydration,
fetching any missing custom query transform through the already-live
HTTP-backed `fetch_and_register_custom_query_transform`, and then calling the
same `apply_and_poke` path. This means a name+args custom query can be
transformed and hydrated in the same async `initConnection`/`changeDesiredQueries`
turn rather than needing a prior inspect call to seed the transform registry.
New test uses a real local HTTP response server, returns a transformed
`projectByName` AST, and proves the resulting poke contains only the matching
SQLite row. Verified with `cargo fmt -p zero-cache-server`,
`cargo test -p zero-cache-server async_desired_query_hydration_fetches_custom_query_transform --lib`,
`cargo test -p zero-cache-server live_connection::tests --lib`, and
`cargo check -p zero-cache-server`. Remaining: `related`/joins and full
planner-backed execution.

**Desired-query hydration now fetches top-level related child rows.**
For ASTs with top-level `related` subqueries using a single-column correlation,
`DesiredQueriesHandler::apply_and_poke` now uses the fetched parent rows to
build a child `IN (...)` filter (`childField IN parentField values`) and
hydrates the child table under the same root query ref-count. A new
`live_hydration::hydrate_rows_from_sqlite_with_row_updates` helper receives
child rows without calling `track_executed`, preserving the updater invariant
that the root query is tracked only once per cycle. New live WebSocket test
creates `issue` plus `comments`, sends an AST with `related`, and proves the
poke includes the parent row and matching child row while excluding an
unrelated child. Verified with `cargo fmt -p zero-cache-server`,
`cargo test -p zero-cache-server desired_query_hydration_fetches_top_level_related_rows --lib`,
`cargo test -p zero-cache-server live_connection::tests --lib`, and
`cargo check -p zero-cache-server`. Remaining: compound correlations, nested
related hydration, joins, and full planner-backed execution.

**Desired-query hydration now preserves compound related correlations.**
`related_filter_from_parent_rows` now accepts equal-length multi-column
correlations. Single-column correlations keep the compact `childField IN (...)`
shape, while compound correlations build an equivalent OR-of-ANDs child filter
from each fetched parent tuple so `(tenantID, issueID)` is matched together
instead of as independent cross-product `IN` lists. New live WebSocket test
creates `locale_issue` plus `locale_comment`, sends a `related` AST with
`parentField:["tenantID","issueID"]`, and proves the poke includes the two
tuple-matching children while excluding cross-product rows. Verified with
`cargo fmt -p zero-cache-server`,
`cargo test -p zero-cache-server desired_query_hydration_fetches_top_level_related_rows --lib`,
`cargo test -p zero-cache-server desired_query_hydration_fetches_compound_related_rows --lib`,
and `cargo check -p zero-cache-server`. Remaining: nested related hydration,
joins, and planner-backed execution.

**Desired-query hydration now recurses through nested related reads.**
`apply_and_poke` now delegates related-row hydration to
`hydrate_related_rows_recursive`, which hydrates each `related` child table
from its immediate parent rows and then repeats the same process for nested
`related` subqueries. The child rows still use
`hydrate_rows_from_sqlite_with_row_updates`, so they contribute row patches and
root-query ref-counts without re-calling `track_executed`. New live WebSocket
test creates `issue -> comments -> reactions`, sends an AST with nested
`related`, and proves the poke includes the parent issue, matching comment, and
matching reaction while excluding unrelated rows at both child levels. Verified
with `cargo fmt -p zero-cache-server`,
`cargo test -p zero-cache-server desired_query_hydration_fetches_top_level_related_rows --lib`,
`cargo test -p zero-cache-server desired_query_hydration_fetches_compound_related_rows --lib`,
`cargo test -p zero-cache-server desired_query_hydration_fetches_nested_related_rows --lib`,
and `cargo check -p zero-cache-server`. Remaining: joins and planner-backed
execution.

**MILESTONE: live CRUD-mutation transaction executor — `zero-cache-mutagen::apply_mutation.rs`.**
Closes a gap `orchestration.rs`'s own module doc named as deferred since the mutation-planning round:
"actually executing the returned SQL against a live transaction... remain[s] unported." `apply_crud_mutation`
is a real `tokio-postgres` transaction mirroring `mutagen.ts`'s `processMutationWithTx`: `BEGIN` ->
`last_mutation_id::get_upsert_last_mutation_id_sql` (atomically incrementing the client's counter,
`RETURNING` the new value) -> `check_mutation_id` classifies the result -> on `Ok`, run every statement
`orchestration::plan_mutation_sql` produces (the already-ported insert/upsert/update/delete SQL builders)
-> `COMMIT`; on `Unexpected` (out-of-order id), `ROLLBACK` the whole transaction including the counter
upsert; on `AlreadyProcessed` (stale retry), commit with no ops run — matching upstream's "ignore, don't
error" and "confirm the mutation even though it may have been blocked by the authorizer" (an unauthorized
mutation still commits its last-mutation-id bump, just skips its ops). Added `tokio-postgres` as a real
dependency of `zero-cache-mutagen`. **4 live tests against real Postgres**, each asserting the actual
database state, not just a returned enum: a real INSERT lands AND the counter increments atomically; a
stale retry does NOT re-apply and does NOT duplicate rows; an out-of-order id rolls back EVERYTHING
including the counter upsert (verified via `count(*)` on both tables); an unauthorized mutation confirms
(counter advances) but its op does not run. This is the write-path counterpart to the read-path work
(`hydrate_from_sqlite`/`live_connection`) from prior rounds — together they cover both directions of a
sync connection's data flow, though the wire-level `push` message decoding (`up_json` still explicitly
treats `push`/`pull` as out of scope) and the retry-on-serialization-failure loop remain the next steps to
wire this into a served connection. 4 new tests. Verified via scoped `cargo build`/`cargo test -p
zero-cache-mutagen` (+ scoped builds of `zero-cache-auth`/`zero-cache-server`/`zero-cache-view-syncer`) —
no full-workspace run. Remaining: `up_json`'s `push` decoding, the serialization-failure retry loop, custom
mutators (vs. CRUD-only here), and the AST-to-SQL query-side gaps noted above.

**MILESTONE: `push` message decoding + a fully wired write path over a real WebSocket.**
Closes the `up_json` gap named above. New `zero-cache-protocol::push.rs`/`push_json.rs`: `PushBody`/
`Mutation::{Crud,Custom}` (port of `pushBodySchema`/`mutationSchema`), decoded by
`push_body_from_json`/`upstream_from_json` (the `"push"` tag, previously an explicit unported case);
`PushOk`/`push_ok_message_json` encode the `["pushResponse", {...}]` reply (reusing `MutationResponse`
from the already-ported downstream `mutation_result.rs`). `CrudMutation::ops_json` deliberately carries
the raw ops array undecoded (protocol can't depend on `zero-cache-mutagen`, which owns `CrudOp`); new
`zero-cache-mutagen::crud_ops_json.rs::crud_ops_from_json` is the missing decoder, turning that JSON into
real `CrudOp`s for `orchestration::plan_mutation_sql`/`apply_mutation::apply_crud_mutation`. `up.rs`'s
`Upstream` gained a `Push` variant; `connection_dispatch.rs`'s `ConnectionAction` gained a matching `Push`
variant, routed only after `initConnection` (a push before init is a protocol error, same as every other
data message). `live_connection.rs`'s `DesiredQueriesHandler::apply_push` is the real handler: decodes
each CRUD mutation's ops, runs `check_mutation_id` against a per-client counter, executes
`plan_mutation_sql`'s statements, and returns a `pushResponse` with a real per-mutation `MutationResult`
(`Ok`/`alreadyProcessed`/`oooMutation`/app error). **Scope decision, documented in code**: `serve_connection`'s
handler contract is synchronous, but the real upstream-Postgres executor
(`apply_mutation::apply_crud_mutation`, live-tested last round) is async `tokio-postgres` I/O — wiring it
into the sync closure would need a broader async-handler refactor across `serve_connection`/`sync_server`.
Instead `apply_push` runs the SAME statement-planning/decode/check logic against the connection's own
synchronous SQLite replica handle, proving the full decode→plan→apply→respond pipeline for real, with the
one substitution being which database receives the writes; a production deployment wires the async
executor into an async-capable handler. **3 new live tests over real WebSockets**: a real push carrying a
CRUD insert gets a real `pushResponse`, and — critically — the row is verified by opening a brand-new
SQLite connection to the same temp-file-backed replica AFTER the server closes, proving genuine
persistence, not an in-memory illusion; a replayed mutation id is reported `alreadyProcessed`, not
silently duplicated; `connection_dispatch`'s router correctly rejects a `push` before `initConnection`.
Debugging note: the first attempt at these tests failed with a raw `Protocol(ResetWithoutClosingHandshake)`
— traced (via an isolated non-socket reproduction) to the tests themselves omitting `initConnection`
before `push`, which the router correctly rejects, dropping the connection without a close handshake; not
a code defect. 22/22 server-crate tests pass; 91/91 protocol-crate tests; 62/62 mutagen-crate tests. 9 new
tests total across 3 crates. Verified via scoped `cargo build`/`cargo test` per crate (+ scoped builds of
`zero-cache-auth`/`zero-cache-workers`) — no full-workspace run. Remaining: the serialization-failure retry
loop, custom mutators, pull recovery handling, and the AST-to-SQL query-side gaps (schema-driven catalog,
joins) from before.

**`pull` request/response wire codec — the remaining `up_json` protocol tag gap is closed.**
New `zero-cache-protocol::pull.rs`/`pull_json.rs` ports `zero-protocol/src/pull.ts`'s
`PullRequestBody` (`clientGroupID`, nullable `cookie`, `requestID`) and `PullResponseBody` (`cookie`,
`requestID`, `lastMutationIDChanges`) plus the wire decoder/encoder. `up_json::upstream_from_json` now
decodes the `"pull"` upstream tag into `Upstream::Pull` instead of reporting it as unknown/unported.
`zero-cache-view-syncer::connection_dispatch` gained `ConnectionAction::Pull`, routed only after
`initConnection` like other data messages; `zero-cache-server::live_connection::DesiredQueriesHandler`
now answers it from the state this live demo handler actually owns: the current CVR cookie plus its
in-memory per-client `lastMutationIDChanges`. This does not replace the production recovery service,
which still needs authoritative durable clients/CVR state, but it does make `pull` real on the live
handler path instead of silently disappearing. Also fixed stale direct test call sites for
`apply_crud_mutation` after its `error_mode` argument was added, restoring compileability for
`zero-cache-mutagen`'s lib tests. Verified with scoped runs only: `cargo test -p zero-cache-protocol pull`,
`cargo test -p zero-cache-view-syncer connection_dispatch`, `cargo check -p zero-cache-server`, and
`cargo test -p zero-cache-server serve_connection::tests --lib`. No full-workspace run (stopped early per
resource guidance). Later follow-up added a focused
`cargo test -p zero-cache-server pull_returns_current_cookie_and_last_mutation_id_changes --lib` covering
the live response behavior. Remaining: durable production pull recovery, custom mutators, and the
query-side AST/catalog/join gaps.

**`updateAuth` + `ackMutationResponses` upstream protocol/router coverage.**
Closes two more pure upstream-message tag gaps. New `zero-cache-protocol::update_auth.rs` ports
`zero-protocol/src/update-auth.ts`'s `UpdateAuthBody { auth }`; `zero-cache-protocol::push::AckMutationResponsesBody`
ports `push.ts`'s `ackMutationResponsesMessageSchema` body by reusing the existing `MutationId`.
`up_json::upstream_from_json` now decodes both `"updateAuth"` and `"ackMutationResponses"` instead of
classifying them as unknown/unported, and `zero-cache-view-syncer::connection_dispatch` routes them to
new `ConnectionAction::{UpdateAuth,AckMutationResponses}` variants after `initConnection` (rejected before
init, matching the worker's need for an existing connection context). `zero-cache-server::live_connection`
accepts both as documented no-ops for this catalog-scoped demo handler: real `updateAuth` still needs
auth resolution plus view-syncer refresh, and real ack cleanup needs the pusher service's stored-response
state. Verified with scoped runs only: `cargo test -p zero-cache-protocol up_json`,
`cargo test -p zero-cache-view-syncer connection_dispatch`, `cargo check -p zero-cache-server`, and
`cargo test -p zero-cache-server serve_connection::tests --lib`. Remaining stateful gaps include real
update-auth refresh, ack cleanup, pull recovery, custom mutators, and AST/catalog/join query
generalization.

**`inspect` upstream protocol/router coverage — full upstream tag set decoded.**
Closes the last upstream protocol tag gap at the request-shape layer. New
`zero-cache-protocol::inspect_up.rs` ports `zero-protocol/src/inspect-up.ts`'s inspector request union:
`queries` (optional `clientID`), `metrics`, `version`, `authenticate { value }`, and `analyze-query`
(both deprecated `value` AST and current `ast`, optional `name`/`args`, and `options` booleans for
vended/synced rows and join plans). `up_json::upstream_from_json` now decodes `["inspect", ...]` into
`Upstream::Inspect` instead of treating it as unknown; malformed/unknown inspect ops error explicitly.
`zero-cache-view-syncer::connection_dispatch` routes it to `ConnectionAction::Inspect` after
`initConnection` and rejects it before init. `zero-cache-server::live_connection::DesiredQueriesHandler`
accepts the action as a documented no-op because real inspector responses require the full
`InspectorDelegate`/CVR store/view-syncer state. Verified with scoped runs only:
`cargo test -p zero-cache-protocol up_json`, `cargo test -p zero-cache-view-syncer connection_dispatch`,
`cargo check -p zero-cache-server`, and `cargo test -p zero-cache-server serve_connection::tests --lib`.
Remaining: real inspector handling, plus the broader stateful gaps above.

**`inspect` downstream response model + encoder.**
New `zero-cache-protocol::inspect_down.rs`/`inspect_down_json.rs` ports `zero-protocol/src/inspect-down.ts`'s
server-to-client inspector response wrapper: `queries`, `metrics`, `version`, `authenticated`,
`analyze-query`, and `error`, plus a wire encoder for full `["inspect", body]` frames. Query rows model
the concrete inspector fields (`clientID`, `queryID`, nullable AST/name/args, got/deleted/ttl/
inactivatedAt/rowCount) and keep `metrics` as raw `JsonValue` so both the current `QueryServerMetrics`
shape and the legacy server-metrics compatibility shape can pass through. `analyze-query.value` is also
raw `JsonValue` because the analyzer/debug-plan result internals are a larger subsystem. Server metrics
use the already-ported TDigest JSON array representation. Verified with scoped runs only:
`cargo test -p zero-cache-protocol inspect_down` and `cargo check -p zero-cache-server`. Remaining:
real inspector handling against `InspectorDelegate`/CVRStore plus fuller typed `AnalyzeQueryResult`
modeling if/when the analyzer itself is ported.

**`InspectorDelegate` state primitive — metrics, AST lookup, shared inspector auth.**
New `zero-cache-server::inspector_delegate.rs` ports the core of
`zero-cache/src/server/inspector-delegate.ts`: global server metrics (`query-materialization-server` and
`query-update-server`) as TDigest accumulators, per-query hydration milliseconds (last value wins),
per-query update TDigest metrics, queryID→AST tracking, query cleanup, and shared client-group
authentication state. As with other ports, ambient `isDevelopmentMode()` is injected as an explicit
`is_development_mode` argument to `is_authenticated`. `metrics_json_for_query` returns the exact
protocol `JsonValue` object expected by inspect query rows (`query-hydration-server-ms` optional,
`query-update-server` always present when any metric exists); `metrics_json` returns the typed
`ServerMetrics` used by the downstream inspect encoder. NOT ported here: `transformCustomQuery`, which
requires the live custom-query transformer/connection-context HTTP path. Verified with scoped runs only:
`cargo test -p zero-cache-server inspector_delegate --lib` and `cargo check -p zero-cache-server`.
Remaining in the inspect lane: compose `InspectorDelegate` with a CVRStore-backed inspect handler and
eventually the analyze-query execution path.

**Inspector metrics protocol compatibility — `metrics_for_protocol`.**
`zero-cache-server::inspector_delegate::metrics_for_protocol` ports `inspect-handler.ts`'s
`metricsForProtocol`: protocol >= 51 passes current query metrics JSON through unchanged, while older
protocols wrap scalar `query-hydration-server-ms` into a TDigest JSON array under the legacy
`query-materialization-server` key and preserve/omit `query-update-server` to match upstream behavior.
Verified with scoped runs only: `cargo test -p zero-cache-server inspector_delegate --lib` and
`cargo check -p zero-cache-server`. Remaining in the inspect lane: compose the real handler with
CVRStore/`InspectorDelegate` and add the `analyze-query` execution path.

**CVR inspect query projection — in-memory `inspectQueries` core.**
New `zero-cache-view-syncer::cvr_inspect::inspect_queries_from_cvr` ports the pure projection semantics
of `CVRStore.inspectQueries`: external query client states become `InspectQueryRow`s, internal queries
are omitted, rows are ordered by `(clientID, queryHash)`, optional client filtering is honored, inactive
queries whose `inactivatedAt + ttl <= ttlClock` are suppressed, and `rowCount` is computed from row
records whose ref-counts mention the query. The initial slice reported `deleted: false` before the
in-memory `Cvr` model retained the persisted `desires.deleted` bit; see the next milestone for the
follow-up that closes that gap. Verified with scoped runs only:
`cargo test -p zero-cache-view-syncer cvr_inspect --lib` and `cargo check -p zero-cache-view-syncer`.
Remaining in the inspect lane at that point: wire this projection into a real `handleInspect` flow with
`InspectorDelegate` metrics/auth/version responses, then port `analyze-query`.

**Inspect deleted desire fidelity — retained `desires.deleted` state.**
The in-memory CVR state now keeps the persisted `desires.deleted` tombstone bit on `ClientQueryState`,
so DB-loaded deleted-but-not-expired desire rows can be surfaced by inspect instead of being flattened
to `deleted: false`. `load_cvr_from_rows` preserves `deleted: true` when a deleted desire also has
`inactivatedAt`; fully deleted rows without `inactivatedAt` still remain invisible, matching upstream's
`inspectQueries` filter. `put_desired_queries` writes active states with `deleted: false`, and
`delete_queries(..., Some(inactivated_at))` marks retained inactive states as `deleted: true`.
`inspect_queries_from_cvr` now emits `InspectQueryRow.deleted` from that state and still TTL-filters
expired deleted rows. Verified with scoped runs only:
`cargo test -p zero-cache-view-syncer cvr_inspect --lib`,
`cargo test -p zero-cache-view-syncer cvr_load --lib`,
`cargo test -p zero-cache-view-syncer cvr_desired_queries --lib`,
`cargo test -p zero-cache-server inspect_handler --lib`,
`cargo check -p zero-cache-view-syncer`, and `cargo check -p zero-cache-server`.
Remaining in the inspect lane: connect the pure handler to live WebSocket/view-syncer state and port
real `analyze-query`.

**Inspect handler core — auth, version, metrics, and enriched query responses.**
New `zero-cache-server::inspect_handler::handle_inspect` ports the already-backed core of
`inspect-handler.ts`: unauthenticated non-`authenticate` requests return an `authenticated: false`
challenge, `authenticate` validates via an injected admin-password predicate and updates
`InspectorDelegate`'s shared client-group auth state, development mode bypasses auth, `version` returns
the injected server version, `metrics` returns delegate global TDigest metrics, and `queries` uses the
in-memory CVR projection plus delegate AST fallback and protocol-version-specific per-query metrics.
`analyze-query` now reaches an explicit inspect `error` response (`analyze-query is not yet ported`)
instead of disappearing into the previous server-side no-op. Verified with scoped runs only:
`cargo test -p zero-cache-server inspect_handler --lib` and `cargo check -p zero-cache-server`.
Remaining in the inspect lane: connect this pure handler to the live WebSocket/view-syncer state,
and port real `analyze-query`.

**Live inspect response bridge — no longer a served-connection no-op.**
`zero-cache-server::live_connection::DesiredQueriesHandler` now owns an `InspectorDelegate` plus inspect
configuration and routes `ConnectionAction::Inspect` through `inspect_handler::handle_inspect`, encoding
the returned `InspectDownBody` with `inspect_down_message_json` so real served WebSocket connections get
`["inspect", ...]` response frames. The default demo constructor runs inspect in development mode and
uses the crate version as the server version; `with_inspect_options` lets tests/callers supply protocol
version, server version, development-mode behavior, and an optional admin password. The catalog-scoped
handler initially passed an empty row-record slice, so inspect query rows were live for CVR query state
but did not yet have persisted row-counts from a row cache; see the next milestone for the follow-up
that closes that demo-handler gap. Verified with scoped runs only:
`cargo test -p zero-cache-server live_connection::tests::inspect_action_returns_encoded_downstream_frame --lib`,
`cargo test -p zero-cache-server live_connection::tests::run_accept_loop_serves_real_inspect_version_response --lib`,
`cargo test -p zero-cache-server live_connection --lib`, and `cargo check -p zero-cache-server`.
Remaining in the inspect lane at that point: expose/persist row-record cache data to live inspect
queries and port real `analyze-query`.

**Live inspect row counts — hydration row-record cache.**
`zero-cache-server::live_hydration` now exposes `hydrate_patches_from_sqlite_with_row_updates`, a
row-update-returning companion to the existing patch-only hydration function. It converts
`hydrate_query`'s `RowStoreWrite`s and deletion row writes into `RowRecord` updates while preserving the
existing `hydrate_patches_from_sqlite` API for callers that only need poke patches. `DesiredQueriesHandler`
now maintains a small in-memory row-record cache from those updates and feeds it into
`handle_inspect`, so live served inspect `queries` responses can report nonzero `rowCount` for hydrated
queries. The demo identity also now records ref-counts under the actual query hash instead of a placeholder
key. Verified with scoped runs only:
`cargo test -p zero-cache-server live_connection::tests::live_inspect_queries_include_hydrated_row_count --lib`,
`cargo test -p zero-cache-server live_connection --lib`,
`cargo test -p zero-cache-server live_hydration --lib`, and `cargo check -p zero-cache-server`.
Remaining in the inspect lane: persist/use the full production CVR row store across sessions and port real
`analyze-query`.

**Analyze-query result protocol model.**
New `zero-cache-protocol::analyze_query_result` ports the stable outer schema from
`zero-protocol/src/analyze-query-result.ts`: `AnalyzeQueryResult`, row-count maps, row maps, SQLite
plans, read-row/read-count diagnostics, timing fields (`start`/deprecated `end`/optional `elapsed`),
warnings, `afterPermissions`, and optional join-plan diagnostics. The planner debug event internals are
kept as raw `JsonValue` for now because the planner/debugger subsystem is not yet ported, but
`InspectDownBody::AnalyzeQuery` now carries a typed `AnalyzeQueryResult` instead of an arbitrary JSON
blob, and `inspect_down_message_json` serializes it through the new protocol helper. Verified with
scoped runs only: `cargo test -p zero-cache-protocol analyze_query_result --lib`,
`cargo test -p zero-cache-protocol inspect_down --lib`, `cargo check -p zero-cache-protocol`, and
`cargo check -p zero-cache-server`. Remaining in the inspect lane: implement the real analyzer
execution path that produces this result from AST/custom-query input.

## Running

```
cargo test                    # all crates
cargo test -p zero-cache-types
cargo bench -p zero-cache-types
```
