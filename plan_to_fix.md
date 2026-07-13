# Plan to fix: invalid differences vs official Zero (`zero/v1.7.0`, sync protocol v51)

This document enumerates every **invalid difference** found between this Rust port and the
pinned upstream reference in `mono-src/` (commit `6863de5`, tag `zero/v1.7.0`,
`@rocicorp/zero-sqlite3@1.1.2`). "Invalid" means a real behavioral divergence — a wire/CVR
incompatibility, a correctness bug, or a validation/semantic mismatch — **not** a purely
internal API-naming difference.

Findings were produced by a six-surface differential audit (protocol, replication,
view-syncer/IVM, config, auth/mutations, server/routing) plus the pre-existing
`BUGREPORT-whereexists-child-sync.md`. Each entry cites the Rust location, the upstream
reference, the divergence, the impact, and a fix direction.

Legend — **Severity**: Critical (silent wire/CVR/data corruption or client breakage) ·
High (correctness loss or client-facing protocol break in common cases) · Medium (edge-case
correctness or admission-behavior divergence) · Low (narrow/latent/defensive). Items marked
*(documented)* are already acknowledged in `PORTING.md` / the interop verdict; they are listed
for completeness because they are still real divergences, but they are not new discoveries.

---

## Fix status (as of this pass)

All findings below were implemented (test-first where practical) and the **full workspace test
suite is green** (`cargo test --workspace`, live-PG tests skipped without a DB). Each finding's
resolution:

### Runtime/stability bugs fixed beyond the original conformance audit

These were found from real production logs/screenshots (not upstream-diff findings) and fixed
with regression tests:

- **Shared replica handle leak (perf, all complex queries).** The transient IVM operator graph
  wired every operator to its input via a strong `input.set_output(self)` back-ref, forming an
  `Rc` cycle at each edge; the `SqliteSource`'s clone of the snapshotter's shared replica
  connection never dropped, so `with_current_shared` reopened a fresh snapshot at head on EVERY
  multi-operator hydration (flooded logs with "shared replica handle outlived hydration"). Fixed:
  each operator's `destroy()` drops its downstream output ref, `hydrate_via_graph` tears the graph
  down, `reopens_from_leak()` counter + regression test. (commit `78f60c0`)
- **`register_query` "query X is already active" → client "Zero mutation failed".** A second
  connection (or a reconnect) re-desiring an already-active group query failed the CVR transition
  and surfaced as a fatal client mutation-lifecycle error + re-init/mutation-push loop (observed as
  repeated `setLocation` pushes). Fixed: `register_query` is idempotent for an already-active
  query. (commit `61bc85a`)
- **L1 per-query transform error delivery** (see L1 row) is now upstream-faithful:
  `["transformError", …]` for per-query app/parse errors (connection stays open) vs a terminal
  close for whole-request failures. (commit `78f60c0`)
- **H5 DDL apply-side** (see H5 row) is now wired into the replicator so schema changes replicate
  inline. (commit `457ac67`)

| Finding | Status | Notes |
|---|---|---|
| **C1** whereExists child sync | ✅ Fixed (already committed in `group_transition.rs`) | root `where_` correlated subqueries hydrated via `hydrate_related_rows_recursive`; stale line refs |
| **C2** query-hash key order | ✅ Fixed | `correlated_subquery_to_json` reordered to `correlation,hidden,subquery,system`; hidden-hop fixture added |
| **H1** poke schemaVersions | ✅ Fixed | `build_poke` no longer stamps `{1.0,1.0}`; `pokeStart.schema_versions` always `None` |
| **H2** SERIALIZABLE mutations | ✅ Fixed | `build_transaction().isolation_level(Serializable)`; retry loop now reachable |
| **H3** validatePublications | ✅ Fixed | new `publication_validation.rs`; rejects RI NOTHING/INDEX-without-index/`_0_version`/bad idents |
| **H4** typed connect error | ✅ Fixed | upgrade completes, then typed `["error",…]` frame + close (no bare 400) |
| **H5** DDL-less schema change | ✅ Fixed | event-trigger install/detect (`ddl.rs`) + apply-side decode (`pg_to_change.rs` diffs `previousSchema`→`schema` into Change variants) wired into the replicator via `set_shard`; interim schema-hash poll remains as fallback. Remaining: table-metadata replication, attnum-resolved index compare, inline backfill scheduler (commit `457ac67`) |
| **H6** streamer transport | ⚠️ Partial | insert/update split, truncate/DDL/rollback on the wire, receive-side truncate/rollback **applied**, DDL forces resync; **durable CDC store + byte-accounting backpressure remain out of scope** |
| **M1** alias uniquify | ✅ Fixed | `uniquify_correlated_subquery_condition_aliases` ported; `graph_child_hops` reconstruction kept in sync |
| **M2/M3/M4** auth admission | ✅ Fixed | empty-userID rejected; multi-source skips legacy validation; JWKS refetch on unknown kid |
| **M5** CVR load-retry/reset | ✅ Fixed | `load_cvr_with_attempts` bounded loop → terminal `Reset`; wired into `ViewSyncerSession::connect` |
| **M6** poke cookie from CVR target | ❌ Deferred | reverted — the port emits multiple pokes per transition (config/hydration/fan-out), so `pokeID = versionToCookie(cvr.version)` collides across them, dropping `gotQueriesPatch`. Needs each split poke to carry its own distinct target version first; lowest value (conformance green without it). `build_poke_to_target` retained for the eventual correct migration |
| **M7** add_query replace-in-place | ✅ Fixed | duplicate id now removes+re-hydrates |
| **M8** push-incremental advance | ⚠️ Partial | advance abort/reset budget (`ResetPipelines`) added; **full per-`SourceChange` push redesign remains the tracked `query-pipeline-redesign.md` work** |
| **M9** multi-pub column check | ✅ Fixed | `check_published_columns_consistency` now on the live path |
| **M10** REPLICA IDENTITY FULL key | ✅ Fixed | `Full` relation → `RowKeyKind::Full`; dispatcher keys UPDATE/DELETE off PK. Residual: FULL-identity *backfill* |
| **M11** writer pragmas | ✅ Fixed | `replicator_setup::apply_pragmas` wired into `run_replicator` (busy_timeout 30000, analysis_limit, optimize) |
| **M12** subscriber validation | ✅ Fixed | `WrongReplicaVersion`/`WatermarkTooOld` typed errors |
| **CFG1–CFG5** config strictness | ✅ Fixed | bool/number parse errors are fatal; app-id/log-level/format/litestream-level validated |
| **L1** transform-failure hang | ✅ Fixed | concurrent fetch; per-query app/parse errors (and missing-from-response) → non-terminal `["transformError", …]` frame (upstream `sendQueryTransformApplicationErrors`); whole-request failures (transport/HTTP/`transformFailed`) → terminal close (`sendQueryTransformFailedError`) (commit `78f60c0`) |
| **L2** related dedup key | ✅ Fixed | keyed on `alias ?? ""` |
| **L3** kid mismatch fallback | ✅ Fixed | no fallback to sole key on kid mismatch |
| **L4** catchup column types | ✅ Fixed | `resolve_catchup_typed` + JsonValue on the wire (booleans restore) |
| **L5/L6** pgoutput binary/Type/Origin | ✅ Fixed | binary→raw bytes; Type/Origin decoded (no-op) |
| **L7/L8** header dup / retry class | ✅ Fixed | Authorization/Cookie/Origin overwrite; retry on timeout/request/body transport errors |
| **L9/L10** streamer `initial`/`mode` | ✅ Fixed | `initial` optional; `mode` plumbed |
| **L11/L12/L13** ttl/planner edges | ✅ Fixed | float compareTTL; empty-ttl no-panic; overflow key→`"undefined"` |
| **L14** unbounded catchup | ✅ Fixed | `read_since_bounded` ceiling added |

**Genuinely remaining (architecture-scale, not safely automatable in this pass):**
- **M8** true push-incremental IVM (the full `docs/query-pipeline-redesign.md` redesign).
- **H6** durable per-streamer CDC store + heap-proportion/ack-consensus backpressure, and
  receive-side **inline DDL apply** (currently forces a resync).
- **H5** apply-side decode of the emitted `{app}/{shard}/ddl` messages into incremental schema
  changes (install/detect half is done; interim poll is the fallback).
- **M6** a correct poke-cookie-from-CVR-target migration (needs the target version threaded per
  call site without dropping in-flight patches).
- **L1** per-query error delivery (client marks the individual failed query rather than the
  connection staying quietly query-less).

---

## Priority 0 — Critical: silent wire / query-identity corruption

### C1. `whereExists` correlated-subquery child rows filtered server-side but never synced
- **Where:** `crates/zero-cache-sqlite/src/query_builder.rs:344-408` (`exists_to_sql`);
  `crates/zero-cache-server/src/live_connection.rs:1325-1436` (`hydrate_put`, walks only `ast.related`).
- **Upstream:** `EXISTS` is an IVM join operator, so the correlated child rows flow through the
  pipeline and are part of the synced row-set.
- **Divergence:** the port pushes `whereExists`/`exists` down into SQL as a pure parent-row
  `[NOT] EXISTS(...)` filter and never emits the child rows. The Zero client re-runs the query
  pipeline locally over the rows it received, re-evaluates `whereExists` against **zero** child
  rows, and drops the parent → a query matching real data resolves to empty/`null`.
- **Trigger:** any top-level `whereExists` whose relation is not also pulled via a matching
  `.related(...)`. Nested `exists(...)` inside `related` subqueries mis-filter for the same reason.
- **Impact:** silent data-correctness failure; real app (Hunting Game) opens stuck. See
  `BUGREPORT-whereexists-child-sync.md` for the full write-up and affected queries.
- **Fix:** in `hydrate_put`, after hydrating root rows, walk `where_` recursively (through
  `And`/`Or`) collecting every `CorrelatedSubquery`, and hydrate their child rows via the
  existing `hydrate_related_rows_recursive` (correlation filter + subquery `where_`, recursing
  into nested `exists`/`related`); do the same inside each `related` subquery's own `where_`;
  keep those rows tracked so `rehydrate_tracked` re-syncs them. Add a `conformance/` regression
  test asserting a `whereExists` parent survives the client's local re-evaluation.

### C2. Query hash diverges for hidden (junction) hops — CorrelatedSubquery JSON key order
- **Where:** `crates/zero-cache-protocol/src/ast_json.rs:88-125` (`correlated_subquery_to_json`).
- **Upstream:** `mono-src/packages/zero-protocol/src/ast.ts` `transformAST` emits related-subquery
  object keys in order `correlation, hidden, subquery, system` (JSON.stringify literal order;
  absent fields dropped).
- **Divergence:** Rust emits `correlation, subquery, system, hidden` (pushes `hidden` last). Since
  `hashOfAST = to_base36(h64(JSON.stringify(normalizeAST(ast))))`, the stringified bytes differ
  **whenever a related subquery has `hidden` present** — i.e. `hidden: true` on junction edges
  (many-to-many, e.g. `issue.related('labels')`), which is extremely common.
- **Impact:** the query hash is the content-addressed query ID persisted in the CVR and shared
  across client/server (`CustomQueryRecord.id`, `InternalQueryRecord.id`, transform-cache key). A
  mismatched ID means the Rust server computes a different query identity than official
  clients/server for any query with a hidden hop → CVR entries do not line up. Silent.
- **Why undetected:** `hash_outputs_are_byte_stable` has no related-with-hidden fixture.
- **Fix:** reorder serialization to `correlation`, then `hidden` (if present), then `subquery`,
  then `system` (if present). Add a hidden-hop hash fixture cross-checked against upstream output.

---

## Priority 1 — High: correctness / client-facing protocol breaks

### H1. `pokeStart.schemaVersions` fabricated as `{min:1.0, max:1.0}` — upstream omits it
- **Where:** `crates/zero-cache-view-syncer/src/poke_builder.rs:210-219` (`build_poke`);
  reached via `live_connection.rs:2136`, `group_processor.rs:699`.
- **Upstream:** `client-handler.ts:206` builds `pokeStart = {pokeID, baseCookie}` and never
  populates `schemaVersions` in `zero-cache` (the field doc in `poke.ts:38-47` is stale).
- **Divergence:** the port stamps `schema_versions: Some({min:1.0,max:1.0})` on every poke with a
  `rowsPatch`. A client validating `schemaVersions` against its supported-schema range can reject
  the poke when its range excludes 1.0. Conformance is green only because the test app is schema v1.
- **Fix:** omit `schemaVersions` from `pokeStart` (leave `None`) unless upstream actually populates
  it; confirm against `client-handler.ts`.

### H2. CRUD mutations applied at READ COMMITTED, not SERIALIZABLE
- **Where:** `crates/zero-cache-mutagen/src/apply_mutation.rs:79` (`client.transaction()`).
- **Upstream:** `mutagen.ts:257` wraps every mutation in `runTx(db, …, {mode: Mode.SERIALIZABLE})`.
- **Divergence:** plain `BEGIN` at Postgres default isolation. (a) Concurrent client-groups can
  interleave in ways upstream's serializable snapshot forbids; (b) the entire
  `MAX_SERIALIZATION_ATTEMPTS` retry loop (`apply_mutation.rs:197-244`, only fires on SQLSTATE
  40001) is dead code because READ COMMITTED never raises serialization failures.
- **Fix:** issue `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` (or a serializable `BEGIN`) for
  mutation transactions so the existing retry loop becomes live.

### H3. `validatePublications` never invoked — unsupported tables silently synced
- **Where:** `crates/zero-cache-sqlite/src/initial_sync.rs:258-344` / `copy_all:511-595` (no
  validation step; no `UnsupportedTableSchemaError` in `crates/`).
- **Upstream:** `initial-sync.ts:202` + `schema/validation.ts:22-65` throw for `REPLICA IDENTITY
  NOTHING`, `REPLICA IDENTITY INDEX` without an index, a reserved `_0_version` column, and invalid
  identifiers.
- **Impact:** a `REPLICA IDENTITY NOTHING/INDEX` table syncs but its later UPDATE/DELETE can't key
  rows; a `_0_version` column produces a broken `CREATE TABLE` failing with a cryptic SQLite error
  instead of a clean rejection.
- **Fix:** port `validatePublications` and call it between `get_publication_info` and `copy_all`.

### H4. Connect-upgrade errors return HTTP 400 instead of a typed in-band WebSocket error
- **Where:** `crates/zero-cache-server/src/public_http.rs:81-90,119-125`.
- **Upstream:** `workers/connection.ts:136-145` always completes the WS upgrade, then sends a typed
  downstream `["error",{kind:"VersionNotSupported",…}]` via `closeWithError` before closing.
- **Divergence:** the port validates the sync request before the upgrade — unsupported protocol
  version, missing/invalid query params, or a malformed `Sec-WebSocket-Protocol` header all
  produce a `400 Bad Request` and no socket.
- **Impact:** an official `zero-client` never receives the typed `VersionNotSupported`/error
  payload it keys its "please update" UX and backoff on; it just sees a failed upgrade.
- **Fix:** accept the upgrade, then emit the typed downstream error message and close.

### H5. DDL never replicated inline — schema change without following DML is silently missed
- **Where:** `crates/zero-cache-change-source/src/shard_schema.rs:242` hardcodes
  `ddlDetection=false`; no `EVENT TRIGGER`/`triggerSetup` ported. Drift is detected only from a
  pgoutput `Relation` message (`replication_apply.rs:222`) → full drop+re-COPY
  (`replicator_service.rs:523-546`).
- **Upstream:** `change-source/pg/schema/ddl.ts` + `shard.ts` install event triggers that stream
  DDL as change messages applied incrementally, preserving data and slot position.
- **Divergence/impact:** *(partially documented — CLAUDE.md says "schema drift triggers a full
  resync")* but two correctness edges remain: (a) a DDL with **no subsequent DML** to that table
  emits no new `Relation` message, so the change is never detected and the replica stays silently
  stale; (b) precise commit-ordering of the DDL is lost.
- **Fix:** port the event-trigger DDL detection, or (interim) add a periodic schema-hash poll so a
  DML-less DDL still triggers resync.

### H6. Change-streamer transport drops truncate / DDL / rollback and mis-tags inserts *(documented interop blocker)*
- **Where:** `crates/zero-cache-server/src/change_streamer_wire.rs:29-64`,
  `change_streamer_server.rs:129-155`.
- **Upstream:** `downstream.ts`/`data.ts` carry `insert` (no `key`) vs `update` (nullable `key`),
  plus `truncate`, DDL (`create-table`/`add-column`/`drop-column`/`rename-table`/`create-index`/
  `backfill*`), and `rollback`.
- **Divergences:** (a) every `StreamedChange::Set` is emitted as `update` — an insert reaches an
  official downstream consumer tagged `update`; (b) `TRUNCATE` and all DDL are silently dropped to
  downstream view-syncer nodes; (c) non-durable in-memory broadcast fan-out
  (`change_fanout.rs`) with no durable CDC store / byte-accounting backpressure / ack consensus.
- **Impact:** real data-divergence for scaled (multi-node) deployments; the port is not drop-in
  interoperable with an official change-streamer. Single-node topology is unaffected by (a)/(b)
  round-trip since the port's own decoder is internally consistent.
- **Fix:** this is the streamer-transport rewrite tracked by the interop verdict; align the wire
  union with `data.ts`/`downstream.ts` and add durable storage + backpressure.

---

## Priority 2 — Medium: edge-case correctness & admission-behavior divergences

### M1. Same-alias correlated-subquery conditions overwrite each other (no alias uniquification)
- **Where:** `crates/zero-cache-zql/src/builder/pipeline.rs:188-193,277-312`.
- **Upstream:** `builder.ts:269` calls `uniquifyCorrelatedSubqueryConditionAliases`, appending
  `_0`,`_1`,… to every EXISTS/NOT-EXISTS subquery alias.
- **Divergence:** the port uses the raw `subquery.alias`. Two correlated-subquery conditions
  sharing an alias (e.g. `whereExists('comments') AND whereExists('comments')`) both write
  `node.relationships["comments"]` (second overwrites first) and both `Exists` operators read the
  same relationship → one EXISTS is evaluated against the wrong child set.
- **Fix:** port the uniquify pass before building the pipeline.

### M2. Valid token with no `userID` is admitted
- **Where:** `crates/zero-cache-server/src/bootstrap.rs:524-544` + `auth_token.rs:358-362`.
- **Upstream:** `server/syncer.ts:144-154` `validateLegacyJWT` throws `Unauthorized("UserID is
  required for JWT validation.")` when `userID` is falsy.
- **Divergence:** the port passes `expected_sub = None` when `userID` is absent, and `check_claims`
  imposes no subject constraint → a connection omitting the `userID` param but presenting a valid
  token is accepted (upstream rejects it).
- **Fix:** reject the connection when `userID` is empty and any token validator is configured.

### M3. Multi-source JWT config still validates instead of disabling legacy validation
- **Where:** `crates/zero-cache-server/src/auth_token.rs:96-117` (`from_config` picks
  highest-priority source and always validates).
- **Upstream:** `syncer.ts:142-143` installs `validateLegacyJWT` only when exactly one of
  `jwk`/`secret`/`jwksUrl` is configured; with ≥2 it leaves validation undefined.
- **Divergence:** the port rejects tokens upstream would pass through unvalidated for multi-source
  configs.
- **Fix:** when >1 token source is configured, skip legacy-JWT validation to match upstream.

### M4. JWKS revalidation never refetches on key rotation
- **Where:** `crates/zero-cache-server/src/auth_token.rs:169-215` (`verify_sync`, JwksUrl arm) uses
  only the key set cached at connect time.
- **Upstream:** `jwt.ts:63-66` `jwtVerify(token, remoteKeyset)` auto-fetches on an unknown `kid`.
- **Divergence:** after IdP key rotation, in-flight connections presenting the new `kid` fail
  revalidation and are dropped where upstream refetches and succeeds.
- **Fix:** refetch the JWKS on `kid` miss (with caching/rate-limit) before failing.

### M5. Bounded CVR load-retry and terminal `ClientNotFoundError` reset missing
- **Where:** `crates/zero-cache-view-syncer/src/cvr_store_pg.rs:87-135` +
  `view_syncer_session.rs:80-95` (`load_cvr` runs once, returns `RowsBehind`).
- **Upstream:** `cvr-store.ts:274-306` wraps `#load` in a `maxLoadAttempts` loop and, when the row
  cache never catches `instances.version`, throws `ClientNotFoundError` to force a client CVR reset.
- **Divergence:** a replica whose `rowsVersion` stays behind leaves the client retrying forever
  instead of being reset.
- **Fix:** add the bounded retry loop and emit the terminal reset.

### M6. `pokeEnd.cookie`/`pokeID` derived from patch versions, not the CVR target version
- **Where:** `crates/zero-cache-view-syncer/src/poke_builder.rs:186-235`.
- **Upstream:** `client-handler.ts:188-336` sets `pokeID = pokeEnd.cookie =
  versionToCookie(tentativeVersion)` (the CVR target, equal to each other).
- **Divergence:** the port computes `final_version = max(patch.to_version)` for the end cookie and
  an unrelated `"poke{seq}"` id; a config-only/forced poke below the CVR target advertises too low
  a cookie, and `build_poke` returns `None` for empty patches so it can't emit a version-advancing
  forced-initial empty poke. Aligns in the common hydration path (patches stamped at `cvr.version`).
- **Fix:** derive `pokeID`/`pokeEnd.cookie` from the CVR target version; allow empty forced pokes.

### M7. `add_query` errors on duplicate id instead of replace-in-place
- **Where:** `crates/zero-cache-view-syncer/src/pipeline_driver.rs:287-289`
  (`PipelineError::DuplicateQuery`).
- **Upstream:** `pipeline-driver.ts:606` `#addQueryImpl` removes+destroys the existing query first
  (`removeQuery(id,'replace-query')`) — how `unchanged-query-rehydrate` re-hydrates.
- **Divergence:** any upstream re-add/rehydrate flow fails here (guarded today only by group
  ref-counting at `group_pipeline.rs:225`).
- **Fix:** replace-in-place on duplicate id.

### M8. Complex/bounded `advance` re-fetches instead of pushing `SourceChange`s — no abort/reset *(documented)*
- **Where:** `crates/zero-cache-view-syncer/src/pipeline_driver.rs:388-433`.
- **Upstream:** `pipeline-driver.ts:948-1051,1201-1221` push individual changes and support a
  time-budget/`ResetPipelinesSignal` abort (`180-246,1094-1157`).
- **Divergence:** *(PORTING.md:82-89)* row-set result is oracle-equal, but (a) relationship-granular
  `CHILD` changes are flattened to top-level Add/Remove/Edit, and (b) the abort-into-reset path is
  entirely absent, so a pathologically slow advance is never aborted. **Fix:** the persistent
  per-group push-incremental graph (already the tracked redesign) + the reset/abort budget.

### M9. Multi-publication column-consistency check not run on the live path
- **Where:** `crates/zero-cache-change-source/src/published_schema.rs:227-254`
  (`get_publication_info` runs only `published_schema_query`; `check_published_columns_consistency`
  is test-only).
- **Upstream:** `schema/published.ts:262-301` throws "exported with different columns" when a table
  appears in multiple publications with differing column sets. Port picks an arbitrary set silently.
- **Fix:** run the consistency check on the live path.

### M10. `replicaIdentityColumns` denormalization dropped
- **Where:** `crates/zero-cache-change-source/src/published_schema.rs:222-254` (doc at `:24` admits
  the omission).
- **Upstream:** `schema/published.ts:185-234` computes per-table replica-identity columns
  (`d`/`i`/`f`) used for row-key selection under `REPLICA IDENTITY FULL`.
- **Impact (lower confidence):** initial sync doesn't need them, but the change-apply key path under
  `REPLICA IDENTITY FULL` may key rows incorrectly — verify whether the port reconstructs an
  equivalent key.
- **Fix:** port the denormalization or confirm an equivalent key source and document it.

### M11. Serving/writer replica runs with wrong runtime pragmas
- **Where:** `crates/zero-cache-sqlite/src/lib.rs:153-154` (used by `run_replicator`): `busy_timeout
  =5000`, `synchronous=NORMAL`, no `analysis_limit=1000`, no `PRAGMA optimize`. A correct
  implementation exists in `replicator_setup.rs` (`get_pragma_config`/`apply_pragmas`) but is not
  wired in.
- **Upstream:** `workers/replicator.ts:133-139` uses `busy_timeout=30000`, `analysis_limit=1000`,
  and runs `optimize`.
- **Impact:** writer aborts on lock contention ~6× sooner; the planner lacks the stat refresh
  upstream relies on (relevant to the hydration/perf gate).
- **Fix:** wire `replicator_setup::apply_pragmas` into `run_replicator`.

### M12. Change-streamer subscriber `replicaVersion`/`watermark` not validated
- **Where:** `crates/zero-cache-server/src/change_streamer_server.rs:189-231` (`serve_subscriber`
  reads only `watermark`, ignores `replicaVersion`).
- **Upstream:** returns `["error",{type:…}]` (`WrongReplicaVersion`/`WatermarkTooOld`) when a
  subscriber is on a stale replica version or requests a purged watermark.
- **Impact:** a view-syncer on an incompatible replica gets changes that can't apply cleanly instead
  of a directive to re-restore. **Fix:** validate and emit the typed errors.

---

## Priority 3 — Config validation strictness (upstream fails startup; port silently coerces)

### CFG1. Boolean parsing accepts wrong token set and silently swallows invalid values
- **Where:** `crates/zero-cache-server/src/config.rs:510-513,567-574` (`bool_` closure).
- **Upstream:** `options.ts:463-471` `parseBoolean` accepts only `true`/`1`→true, `false`/`0`→false,
  **throws** otherwise.
- **Divergence:** the port treats `1|true|yes|on` as true and **everything else as false silently**.
  So `yes`/`on` are wrongly accepted, and any typo (`AUTO_RESET=enabled`, `=2`, `=no`, garbage)
  silently becomes `false` — flipping default-`true` options (`auto_reset`, `enable_crud_mutations`,
  `enable_query_planner`, `enable_query_covering`, `enable_telemetry`) to disabled where upstream
  would refuse to start. **Severity: High within config.** **Fix:** match `parseBoolean` exactly;
  error on unrecognized tokens.

### CFG2. Numeric parsing silently defaults invalid input instead of erroring
- **Where:** `config.rs:525-530` (`u64_`/`f64_`) and every `.parse().ok().unwrap_or(default)` site
  (e.g. 542, 556-558, 622-624, 689-691).
- **Upstream:** `options.ts:387-392` does `Number(input)` and throws `TypeError` on `NaN`.
- **Divergence:** `UPSTREAM_MAX_CONNS=abc` yields 20 in the port vs a hard startup failure upstream.
  Also upstream options are `v.number()` (floats) — a fractional `YIELD_THRESHOLD_MS=10.5` is
  accepted upstream but silently reset to default by the port's integer parse. **Fix:** error on
  unparseable numerics; preserve float semantics where upstream uses `v.number()`.

### CFG3. `ZERO_APP_ID` not validated
- **Where:** `config.rs:555`.
- **Upstream:** `zero-config.ts:34-38` + `types/shards.ts:45-53` assert `app.id` matches
  `/^[a-z0-9_]+$/` (replication-slot naming) and throw `INVALID_APP_ID_MESSAGE` otherwise.
- **Divergence:** uppercase/hyphens/dots accepted → later failure or corrupted slot/schema names.
  **Fix:** validate at parse time.

### CFG4. `ZERO_LITESTREAM_LOG_LEVEL` default `warn` dropped and unvalidated
- **Where:** `config.rs:604` (unvalidated `Option<String>`, `None` when unset); `litestream.rs:31`
  only sets the env when `Some`.
- **Upstream:** `zero-config.ts:905-907` `literalUnion('debug','info','warn','error').default('warn')`.
- **Divergence:** litestream child falls back to its own default rather than `warn`; invalid values
  pass through. **Fix:** default to `warn` and validate the union.

### CFG5. `ZERO_LOG_LEVEL` / `ZERO_LOG_FORMAT` not validated
- **Where:** `config.rs:602-603` (plain string reads).
- **Upstream:** `literalUnion` enums (`zero-config.ts:565`, log-options).
- **Divergence:** an invalid `LOG_LEVEL=verbose` is silently accepted where upstream rejects it.
  *(Lower confidence — upstream `log-options.ts` not in the checkout, but the missing validation is
  clear.)* **Fix:** validate against the literal union.

---

## Priority 4 — Low: narrow, latent, or defensive divergences

- **L1. Custom-query transform failure hangs the query forever** — `live_connection.rs:813-841`
  (`fetch_missing_custom_query_transforms_for_patch` only `warn!`s on failure; `hydrate_put:1347-1349`
  returns no patches with no plan, and the query never transitions to `complete`). Transforms are
  fetched **sequentially**, so one slow app-query server stalls init; with `ZERO_QUERY_FORWARD_COOKIES`
  a single auth failure breaks every query as only a `warn!`. **Fix:** emit a terminal error / mark
  the query so the client stops waiting; fetch transforms concurrently; surface transform-auth
  failures as a connection-level error. (Secondary defect from `BUGREPORT-whereexists-child-sync.md`;
  Low severity as a divergence but High operational impact.)
- **L2. Related-subquery dedup key diverges for un-aliased hops** — `builder/pipeline.rs:166-181`
  keys dedup on `zsubq_0,zsubq_1,…` so multiple un-aliased related hops all survive; upstream
  `builder.ts:386-393` keys on `alias ?? ''` so they collapse to one (last-wins). Narrow — real
  related hops carry an alias.
- **L3. `select_jwk` falls back to the sole key on `kid` mismatch** — `auth_token.rs:421-442`
  substitutes the single key when the token's `kid` matches none; jose throws "no applicable key".
  Signature must still verify (no forgery), but accepts tokens jose rejects.
- **L4. Catch-up path loses declared column types** — `subscriber_catchup.rs:93-106` does `SELECT *`
  returning raw SQLite values; a boolean caught up here ships `1` instead of `true` (snapshotter
  restores types). Latent — part of the multi-node transport (H6); dormant single-node.
- **L5. Binary tuple column (`'b'`) decoded as UTF-8 text** — `pgoutput.rs:195-200`; non-UTF-8
  payloads error. Latent — neither side requests binary pgoutput.
- **L6. `Type`('Y')/`Origin`('O') pgoutput messages returned as `Unsupported`** — `pgoutput.rs:342`.
  No functional impact single-origin.
- **L7. Outgoing API-server headers can duplicate `Authorization`/`Cookie`/`Origin`** —
  `mutagen/src/api_request.rs:74-82` pushes into a `Vec` unconditionally; upstream `custom/fetch.ts
  :160-173` builds a map so custom headers of the same name overwrite. Duplicate header entries on
  the wire.
- **L8. Mutation retry classification narrower than upstream** — `mutagen/src/api_fetch.rs:176`
  retries only `e.is_connect()`; upstream `custom/fetch.ts:327-345` retries any `TypeError: fetch
  failed` (DNS/refused/TLS/mid-flight reset). Post-send resets fail immediately here.
- **L9. `initial` query param required by the streamer dispatcher but optional in spec** —
  `change_streamer_server.rs:93-95` 400s when absent; upstream defaults it to `false`.
- **L10. Streamer `mode=backup` vs `serving` ignored** — `change_streamer_server.rs:189-231` treats
  every subscriber identically; minor for single-replicator topology.
- **L11. `compareTTL` truncates sub-millisecond differences** — `zql/.../ttl.rs:88` casts
  `(ap-bp) as i64`; upstream `ttl.ts:59` returns a float. Only reachable with fractional-ms TTLs.
- **L12. `parse_ttl` panics on empty duration string** — `ttl.rs:69` `.last().unwrap()`; upstream
  yields `NaN`, no throw. Unreachable via type-safe constructors.
- **L13. `translate_constraints_for_flipped_join` drops an over-long parent key** —
  `planner_constraint.rs:72-76` vs `planner-join.ts:43` (sets a literal `"undefined"` key).
  Arguably more correct; unreachable with equal-length correlations.
- **L14. Fan-out lagged catch-up reads an unpinned replica with no upper watermark bound** —
  `change_log.rs:145-152` (`read_since`, no ceiling). Converges (idempotent by key) but not
  point-in-time; subset of H6.
- **L15. Writer `synchronous=NORMAL` vs upstream default** — `lib.rs:154`; safe on a disposable
  replica but a behavioral divergence (folded into M11).

---

## Coverage caveats (surfaces not fully diffed — schedule a follow-up audit)

- **IVM `join.rs`/`join_input.rs`, `filter.rs`, `fan_out.rs`, `constraint.rs`** were not
  exhaustively line-diffed (a sub-agent malfunctioned). Spot-checks of `data`, `skip`, `take`,
  `fan_in`, `exists` were faithful, and the builder-level correlated-subquery handling was audited
  (see M1/L2), but join parent/child change propagation and filter edit-split remain unverified.
- **Planner cost model / keyset-`start` bound building** was verified faithful in the planner
  crate, but the keyset-bound *builder* (`crates/zero-cache-zql/src/builder/`) partial-prefix and
  orderBy-completion paths were only partially cross-checked.

---

## Suggested fix order

1. **C1, C2** — silent data/identity corruption; both have bounded, well-understood fixes and land
   with new conformance fixtures.
2. **H1, H2, H3, H4** — client-facing correctness/protocol breaks with localized fixes.
3. **CFG1, CFG2** — cheap config-strictness fixes that prevent silent misconfiguration.
4. **M1–M7, M9, M11** — edge-case correctness and admission-behavior; M11 also helps the perf gate.
5. **H5, H6, M8, M10, M12** — the streamer-transport + push-incremental redesign already tracked by
   the interop verdict; larger structural work.
6. **L1** (operational impact) then the remaining **L** items opportunistically.
