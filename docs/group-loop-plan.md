# Piece 2: per-group processing loop + persistent push graphs

Implementation plan for the final architectural piece of the query-pipeline
redesign (see `query-pipeline-redesign.md` §6): one processing loop per client
group owning persistent per-query operator graphs (true push-incremental
advance) and the group CVR, fanning pokes to every connection. Ends with
`ZERO_GROUP_OWNERSHIP` default-on and the per-connection path deleted.

Verified against HEAD `c7fbfa0`. Load-bearing facts:

1. The push machinery is mostly present: `SqliteSource` has `push` + `set_db`
   (zero-cache-sqlite/src/sqlite_source.rs:102-124); `Skip`/`Take`
   self-register on their input (ivm/skip.rs:77, ivm/take.rs:80); `GraphFilter`
   (ivm/filter.rs:141), `Exists` (ivm/exists.rs:222), `FanOut`/`FanIn`
   (fan_out.rs:88, fan_in.rs:166) all implement `Output::push`. Gaps:
   `build_pipeline` never wires push edges for GraphFilter/Exists/FanOut→FanIn
   (outputs stay `ThrowOutput`), and `JoinInput` is fetch-only
   (ivm/join_input.rs:24-29,112). A push-capable join + the `SqliteSource`
   pending-change overlay (`generateWithOverlay`) are the only genuinely new
   operator work.
2. The loop's vehicle exists but nothing drives it: `GroupHandle`
   (group_pipeline.rs:100-198) pins a driver to an OS thread. The N×-per-commit
   cost is serve_connection.rs:262-291 (every connection's processor calls
   `rehydrate_tracked_async` per commit) × live_connection.rs:2620-2652 (each
   does checkout→clone→transition→flush of the group CVR).
3. Per-connection poke state separates cleanly: `build_poke_outcome`
   (live_connection.rs:2656-2820) needs only `(orig_version, patches, force)`
   plus `poke_seq`, `initial_base_version`, `last_poke_version`,
   `poked_last_mutation_ids`. `should_include_patch` (client_handler_poke.rs)
   is already the per-client base-cookie filter; a ported upstream
   `ClientHandler`/`PokeCycle` reference shape exists at
   zero-cache-server/src/client_handler.rs:71-150.

## Increments

### 1. Extract `GroupTransitionCore` from `DesiredQueriesHandler` (pure refactor, flag-off byte-identical)

New `zero-cache-server/src/group_transition.rs`: `GroupTransitionCore` owns the
group-scoped fields (`db`, `cvr_handler`, `row_records`, `row_bodies`,
`pending_row_updates`, `desired_puts`, `tracked`, `query_pipeline`) and methods
`hydrate_put` (live_connection.rs:2883), the patch-application half of
`apply_and_poke_staged` (:2387-2450), the advance half of `rehydrate_tracked`
(:2469-2508), `pipeline_changes_to_patches` (:2510-2616),
`refresh_durable_cvr`/`persist_transition` (:1146-1183, :1226-1281),
`refresh_last_mutation_ids` (:2828), `apply_row_updates`/`apply_row_bodies`
(:3214+). Methods return `(orig_version, Vec<PatchToVersion>, lmid_map)`, not
`HandlerOutcome`. `DesiredQueriesHandler` keeps per-connection delivery state
(`poke_seq`, `initial_base_version`, `last_poke_version`,
`poked_last_mutation_ids`, `pending_hydration`), `build_poke_outcome` reshaped
as `build_poke_frames(&mut ConnectionPokeState, ...)`, and all
auth/mutation/inspect state.

Tests: existing live_connection unit tests unchanged; add a scripted
frame-capture test pinning the exact frame sequence for
connect→put→commit→del (the byte-parity anchor for later increments);
conformance green. Risk: the retry-loop delivery checkpointing
(:1343-1347, :2630-2634) couples poke_seq rollback to CVR retry — keep
checkpoint/restore across the boundary. Size M-L, risk medium.

### 2. The group processor loop (flag-on only): one advance + one CVR transition + one flush per commit

New `zero-cache-server/src/group_processor.rs`: a tokio task per
`GroupService`, spawned on first attach, owning ONE `GroupTransitionCore` (the
group CVR lives inside the loop — no cell check-out/clone; the durable
Postgres CAS in `CvrPersistence::flush` remains the cross-node guard), ONE
`FanoutSubscriber`, a `HashMap<client_id, ConnectionHandle{writer_tx, poke:
ConnectionPokeState}>`, and a command mpsc (`Attach{client_id, base_cookie,
writer_tx, loaded_cvr_seed, reply}`, `Detach`, `ChangeDesiredQueries{client_id,
patch, resolved_asts, reply}`, `InspectSnapshot{reply}`).

Commit path (replaces serve_connection.rs:262-291): select! on subscriber +
commands; coalesce bursts (:277-281); `SharedGroupPipeline::advance()` (the
single-owner path — `AdvanceLog`/`poll_advance` cursors bypassed, deleted in
increment 8); `pipeline_changes_to_patches` ONCE; `persist_transition` ONCE;
then per attached connection `build_poke_frames` with ITS poke state into ITS
writer — upstream `startPoke(clients, newVersion)` (view-syncer.ts:1111). Row
and got patches go to every connection; desired patches are per-client-keyed;
`should_include_patch` dedups per base cookie.

Initialize/changeDesiredQueries: the connection task keeps the async pre-steps
(`fetch_missing_custom_query_transforms_for_patch`, live_connection.rs:1549 —
per-connection cookie/headers/bearer; `apply_read_permissions` :2905 —
per-connection auth_data), then sends resolved ASTs into the loop. The loop
applies + hydrates, sends the config poke then the staged hydration poke into
the requester's writer (replacing the `take_pending_hydration` handoff,
serve_connection.rs:249-259 — same FIFO order), and fans got/row patches to
other connections. NAMED RISK: the port reuses the query hash as
transformation hash (live_connection.rs:3022); with per-connection permission
transforms, the same hash can mean different ASTs per client. The loop must key
pipelines by a real transformation hash (hash of the transformed AST, upstream
`#syncQueryPipelineSet`) or ref-count `(hash, transformed-ast)` pairs.

Connection side: `serve_group_connection` variant (serve_connection.rs:186-368
shape) — read-loop/writer split kept, writer_tx handed to Attach, no fanout
subscriber. Push/Inspect/UpdateAuth/Pull/Ack stay in the connection processor
(live_connection.rs:1398-1433). Bootstrap flag-on branch attaches to the loop
(bootstrap.rs:453-475; :585-717 collapses on this path); flag-off untouched.

Tests: loop unit tests over a seeded replica (3 connections, 1 commit → one
poke batch each, advance counter == 1; late-join catchup; detach mid-commit);
group_multiconn_e2e + hunting_game_hard_e2e green; new e2e: B's new desired
query → A receives row/got patches too, poke chains valid. Size XL, risk high
— the structural increment.

### 3. Bench checkpoint (no code) — DONE, finding recorded

`LOAD_WORKLOAD=fanout scripts/bench.sh 300 30` with `ZERO_GROUP_OWNERSHIP=1`,
distinct groups AND `LOAD_CONNS_PER_GROUP=10` (30 groups × 10 conns).

RESULT (2026-07-11): the loop removes the steady-state N×-per-commit advance
(unit-proven: `one_commit_advances_once_and_pokes_every_connection` reads
advance_count == 1 for 3 connections in a group). BUT the fanout bench still
collapses flag-on (12% connected at 30×10; hydrate p50 3.5s vs ref 11ms)
because it dies at CONNECT-TIME HYDRATION, which is per-connection and
unchanged by advance-sharing: every one of the 300 connections does its own
connect-time durable-CVR Postgres load + its own hydration seed + CVR flush,
and 300 concurrent hydrations saturate CPU/Postgres regardless of grouping.
Even the 270 "cheap seed" connections (2nd+ desirer of a shared query) still
pay a full connect-time CVR load + full row poke + CVR flush.

CONCLUSION: the default flip (increment 8) is gated on PER-HYDRATION
efficiency (the orthogonal axis in the perf-gate memory: shared connect-time
CVR load per group, deferred/cheaper CVR flush, no per-connection
re-materialize), NOT on the advance-sharing the loop delivers. Increments 5-7
below deliver piece 2's STATED goal — true push-incremental advance (O(change)
not O(result)) — which is correctness/efficiency of the steady-state advance,
independent of the connect-time hydration gate. Do NOT block 5-7 on this bench;
the flip is a separate hydration-efficiency milestone.

### 4. Wire push edges in `build_pipeline` + port `Streamer` as `Collector` (zql-only)

Self-registration in `GraphFilter::new`/`Exists::new` (match skip.rs:77);
`apply_or` wires `fan_out.set_fan_in(&fan_in)` + branch outputs
(builder/pipeline.rs:304-332). Add `Collector`: an `Output` per query root
flattening Add/Remove/Edit/Child into `PipelineRowChange`s incl. relationship
child rows — port of `Streamer.#streamChanges` (pipeline-driver.ts:1252+),
reusing `insert_graph_nodes`'s flatten (pipeline_driver.rs:232-254) and the
min_row_version clamp. Ported push tests through BUILT pipelines. No
production behavior change. Size M, risk low.

### 5. Push-capable join + `SqliteSource` overlay (the hard operator work)

Make the join a real operator (redesign §4.6): Output of both parent and
child; parent change → re-emit with child fetch attached; child change →
`buildJoinConstraint` → `parent.fetch({constraint})` → `Change::Child`
(upstream join.ts:195-237; `make_child_change` exists, operator.rs:99-111).
Requires the pending-change overlay in `SqliteSource` (`generateWithOverlay`):
during a push, fetches see prev-snapshot state overlaid with the in-flight
change (upstream sets DBs only AFTER all pushes, pipeline-driver.ts:1039-1046).
Also `SqliteSource::remove_output` (Rc-identity) — `destroy` clears ALL
outputs (sqlite_source.rs:162-166), which would kill sibling queries' push
edges on shared sources. Tests: join.push/join.sibling/take.push/exists.push
subsets against `SqliteSource` with overlay. Size L-XL, risk high — the
correctness core; ported upstream tests are the spec.

### 6. `GraphPipelineDriver`: persistent graphs, shared snapshot, push advance (oracle-gated)

Same public surface as `PipelineDriver` so `GroupHandle`'s command enum
(group_pipeline.rs:52-82) is reused. Sources shared + snapshot-fresh:
driver-level `HashMap<source_key, Rc<SqliteSource>>` reading the Snapshotter's
CURRENT snapshot (make `Snapshot`'s db `Rc<StatementRunner>`-shareable,
snapshotter.rs:130-168 — fine, the driver is thread-confined); after each
`snapshotter.advance`, `source.set_db(curr)` on every source AFTER all pushes
(upstream ordering). Advance = change-log → SourceChanges → push:
`SnapshotChange{table, prev, next}` (snapshotter.rs:254-350) mapped as upstream
`#advance` (pipeline-driver.ts:983-1030): prev+next by PK → Edit; unpaired
prev → Remove (unique-conflict rows via get_unique_conflicts :388); bare next
→ Add. Push into every source for that table (all orderings); Collectors
accumulate per query. Per-query state `{root, collector, referenced_tables}`;
`Pipeline.rows` retained temporarily for remove_query/current_query_rows.
Runaway-push guard (cumulative push time > max(hydration time, floor) → drop +
rebuild that query) and `SnapshotError::Reset` → rebuild all. Storage:
MemoryStorage first; DatabaseStorage follow-up.

Oracle gate: every pipeline_driver test fixture runs commit streams through
BOTH push advance and a fresh `hydrate_via_graph` re-derivation, asserting
identical `PipelineRowChange` sets. Size L, risk medium.

### 7. Host the `!Send` driver on the `GroupHandle` thread; loop switches over

Swap `run_pipeline_thread`'s body (group_pipeline.rs:204-255) to build
`GraphPipelineDriver`. The group loop replaces `SharedGroupPipeline`
(`Mutex<PipelineDriver>`) with the `GroupHandle`; `GroupQuerySet` ref-counting
moves into the loop. Direct queries also advance through the graph —
`apply_direct_changes` dead on the flag-on path. Size M, risk medium.

### 8. Flip the default, then delete

1. Flip `ZERO_GROUP_OWNERSHIP` default on; full matrix: `scripts/test.sh
   --with-pg`, conformance, both e2e suites, `scripts/bench.sh 5000 60` +
   fanout. Proof: flag-on fanout sustains 100% at 300 groups, advances ==
   commits per group (not × connections), hydrate p50 within 10% of official.
2. Delete in order: per-connection bootstrap branch + handler plumbing;
   `QueryPipeline::Owned`; serve_synced_connection commit branch +
   take_pending_hydration handoff; `AdvanceLog`/`poll_advance`/
   `advance_to_head` then `SharedGroupPipeline`; `GroupCvrCell` check-out/in +
   adopt/checkin; `apply_direct_changes`/`is_direct_incremental_query`/
   `uses_prehydrated_rows`/`register_query` prehydration + hydrate_via_graph-
   as-advance; the env knob + compose passthrough + `HandlerDeps::
   group_ownership`. Optional: drop `Pipeline.rows` via CVR ref-counts; drop
   per-connection SQL hydration helpers once loop hydration is graph-only.
3. Each deletion lands conformance+e2e green; the increment-1 frame-capture
   pin is the last thing removed.

Sequencing: 1→2→3 fix the group-ownership economics with today's driver
semantics; 4→5→6→7 land the push engine behind the oracle without touching the
wire; 8 only after both prove out. 4-5 can proceed in parallel with 2-3
(different crates).

### 9. Flip-gate hydration efficiency (in progress)

9a. DONE (commit 11668ba): shared connect-time durable CVR load per group
(`GroupService.connect_cvr` OnceCell). Flag-on 30×10 fanout: connected 12%→50%,
hydrate p50 3.5s→2.46s. Dual-flag conformance + e2e green.

9b. NEXT — remove the per-connect/per-commit group-CVR row clone. The loop keeps
live state and `checkin_group_state` (group_transition.rs:581) CLONES cvr +
row_records + row_bodies into the cell EVERY transition, purely so connect-time
shells can read a snapshot; the loop never re-adopts (group_processor.rs has no
`cell.take`/`refresh_durable_cvr`). Under the connect burst, 300 connect
transitions each clone the group's full 1000-row state even when the connection
is the 2nd+ desirer of an already-hydrated query and changes no rows. Fix:
Arc-wrap `row_records`/`row_bodies` in `GroupTransitionCore` + `GroupCvrState`
and mutate via `Arc::make_mut`, so a transition that does not touch rows shares
the Arc (checkin = cheap Arc clone) and only cvr/client/desire records — the
small state — are copied. Blast radius: every read/mutation of row_records/
row_bodies in group_transition.rs + live_connection.rs (they share the type).
Gate: conformance dual-flag + group_multiconn/hunting e2e + the 30×10 flag-on
fanout must sustain ~flag-off. THEN inc 7 (push-on-thread) + inc 8 (flip+delete).

### 9c. Arc row-state DONE (ef076ca) — but disproves the clone hypothesis

Arc-wrapped GroupCvrState/GroupTransitionCore row_records+row_bodies
(copy-on-write via make_mut). Correct, dual-flag green, lower peak mem
(744→727 MiB). BUT the 30x10 flag-on fanout barely moved (connected ~50%→~41%
noise; hydrate p50 2.46s→2.34s; CPU still pinned 100%). CONCLUSION: the
per-transition group-CVR CLONE is NOT the flip gate (9b hypothesis / the old
PORTING.md note were wrong). The dominant connect-time cost is the
PER-CONNECTION FULL-ROW POKE SERIALIZATION — each of the 300 clients builds +
serializes its own ~1000-row hydration poke (rowsPatch) + processes row_records/
row_bodies on the 1-CPU server. This is the per-hydration efficiency axis
(perf-gate memory), and it bounds the FLAG-OFF distinct-group bench too (~4.2s
hydrate). So the flip gate == the general perf-gate hydration work, shared by
both flags: profile the per-connection hydrate poke path (fetch decode →
process_received_row → row_records/row_bodies build → poke JSON serialize → CVR
flush payload), cut redundant passes/copies, stream serialization. Not a §6
structural item — it is the orthogonal hydration-efficiency milestone.

### 7-finding (IMPORTANT): thread-hosted GraphPipelineDriver REGRESSES the connect bench

Increment 7 (host the !Send GraphPipelineDriver on the GroupHandle thread; new
group_graph_pipeline sync facade) WAS implemented and is CORRECT (conformance ON
green with it), but it collapses the flag-on 30x10 connect bench to ~1.7%
connected (vs ~40-50% with the in-process Mutex<PipelineDriver>). Cause: the
single per-group pipeline OS thread serializes EVERY connection's add_query/
register/advance as a blocking channel round-trip; under a 300-connection connect
burst the thread is a hard serialization bottleneck. That WIP is preserved in a
git stash ("mixed: my-pass-reduction + prior-turn inc7-wip") — NOT on main,
because it regresses perf and is not the flip gate. Reworking it needs either
(a) keep the Send Mutex<PipelineDriver> for the connect/hydrate path and use the
thread only for steady-state push-advance, or (b) batch connect commands, or
(c) accept push-advance is a steady-state-only win and don't route hydration
through the thread. The flip gate remains per-connection hydration poke
SERIALIZATION efficiency (9c), independent of inc 7.
