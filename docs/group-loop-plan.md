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

UPDATE: a second, independently-built implementation of the same design (also
correctness-green: workspace 1862/0, dual-flag conformance) accidentally landed
as 74d8706 and was reverted (dd2e2c1); it is preserved on the
`inc7-thread-hosted-driver` branch — richer than the stash for the rework
(GroupGraphPipeline facade + GroupHandle command surface incl.
current_query_rows). REWORK HYPOTHESIS worth testing first: the collapse
mechanism may be TOKIO WORKER STARVATION rather than per-group-thread
serialization per se — the sync facade `blocking_recv`s on tokio worker
threads, so 30 groups' concurrent blocking calls can starve the whole runtime
(~few workers), which would explain a collapse far below the 10-connections-
per-thread serialization bound. If so, the fix is cheaper than options (a)-(c):
route the group processor loop through GroupHandle's ASYNC surface (the loop is
already async; no worker ever blocks), keeping the thread-hosted graph driver
and the sync facade only for tests. Validate with the 30x10 connect bench
before choosing among (a)-(c).

### 9d. PROFILING (data): the SQL fetch is NOT the bottleneck — the CVR flush is

`cargo test -p zero-cache-server --release --test hydration_timing -- --ignored
--nocapture` times `fetch_rows_from_sqlite` at **0.71 ms/fetch for 1000 rows
(0.7 us/row)** — negligible vs the ~1900ms flag-on hydrate p50. So the
per-connection hydration wall is NOT SQL fetch/decode (nor the row clones, per
9c). It is the CVR-FLUSH / transition CONTENTION: 300 concurrent connect
transitions each do a synchronous Postgres config commit (+ deferred rows) on the
bounded CvrPool against a 1-CPU postgres; they serialize. This matches the
original perf-gate finding (persist_transition 200-1500ms) and is exactly the
"worker-starvation rework" a parallel agent is doing. The flip gate = cut the
per-connect-transition CVR flush cost: batch connect transitions per group (many
connections joining -> one config commit), and/or cheaper/deferred config commit,
and/or a larger/less-contended CvrPool. The hydration serialization opts (9c /
3315047 / 5676889) are landed + real (hydrate 3184->1902ms) but a minor lever;
the dominant lever is the CVR flush.

### 9e. CORRECTION (data): the wall is SERVER CPU (poke-serialize + contention), not the flush pool

Two follow-up measurements correct 9d:

1. `ZERO_CVR_MAX_CONNS=200` on the flag-on 30x10 fanout bench barely moved it
   (connected 41%->46%, hydrate p50 ~2280ms, **rust server CPU still pegged at
   100%**). If the Postgres CVR-flush *pool* were the wall, a 6.6x bigger pool
   would help a lot. It didn't -> the wall is the **rust server CPU**, not the
   flush-pool contention. (Group loops serialize a group's connect transitions
   anyway, so pool concurrency can't help within a group.)

2. Micro-timing the in-memory hydrate (`time_hydrate_query_from_rows`): fetch +
   `hydrate_query_from_rows` (the CVR row-processing) = **1.87 ms per 1000-row
   hydration** — cheap. So neither the SQL fetch (0.7ms) nor the CVR row
   processing (1.1ms) explains the ~2280ms p50.

Conclusion: the ~2280ms flag-on hydrate p50 is **300-way concurrency on a
CPU-bound single-core server**, where each connection's remaining per-hydration
CPU (poke JSON serialization of 1000 rows via hydration_to_patches/build_poke,
register_query row_bodies handling, CVR record build) serializes behind the
others. fetch+process is only 1.87ms of it. The dominant lever is therefore
**cutting per-connection poke-serialization CPU and/or not doing a full
1000-row poke per connection** (upstream shares/streams; a distinct-group bench
still pays it, and upstream does the full per-connection hydrate in ~4ms p50 —
a ~500x per-hydration CPU-efficiency gap that the fetch/clone micro-opts do not
close). This is the true flip-gate core and the worker-CPU rework's target; the
CVR flush pool is NOT it.

### 9f. CONSOLIDATED VERDICT: the flip gate is per-connection poke serialization under 300-way / 1-CPU, not any single fixable hot spot

Full evidence-based localization of the ~2280ms flag-on hydrate p50 (bench = 1
CPU / 1 GiB per container, query `{"table":"issue"}` over a 1000-row seed, 30
groups x 10 distinct cookie-0 clients):

- SQL fetch+decode: 0.71 ms/1000 rows (`time_fetch_rows_from_sqlite`) — NOT it.
- fetch + CVR row processing (`hydrate_query_from_rows`): 1.87 ms/1000 rows
  (`time_hydrate_query_from_rows`) — NOT it.
- Per-transition CVR row clone: disproved (9c, Arc experiment flat).
- Postgres CVR-flush pool: disproved (9e, ZERO_CVR_MAX_CONNS=200 barely moved
  it; rust server CPU stays pegged at 100%, ref at 54%).
- Group sharing of the initial poke: NOT the bench lever. The shared driver
  short-circuits (`SharedGroupPipeline::desire` -> `QueryTransition::Unchanged`,
  `group_shared_pipeline.rs:153-171`), but `hydrate_put` still re-fetches from
  SQLite per connection (`group_transition.rs:1120`) and `force_wire_rows`
  re-serializes into each client's poke. Yet even removing the re-fetch saves
  only ~0.7ms/conn (fetch is cheap), and every one of the 10 distinct cookie-0
  clients per group genuinely needs its own full 1000-row catch-up poke, so the
  serialization is inherent. Also: at conns_per_group=1 (300 DISTINCT groups)
  ref's hydrate p50 is still ~4.7ms, so sharing is not why ref is fast.

What remains, by elimination: a SINGLE Rust hydration is ~5-7ms (fetch 0.7 +
process 1.1 + poke-JSON serialize of 1000 rows + CVR flush) — comparable to
ref's ~4.7ms. The 2280ms is 300 of those SERIALIZING on one core. For ref's p50
to be ~4.7ms on the same 1 CPU, ref's 300 concurrent hydrations cannot be
serializing seconds of row-serialization; combined with the known poke-batching
flake ("ref SOMETIMES splits gotQueriesPatch and rowsPatch into two pokes"),
this strongly indicates ref emits a fast first `pokeEnd` (gotQueries ack) under
load and streams the 1000 rows in later poke(s), while Rust builds ONE monolithic
rows-included poke before its first `pokeEnd`. The bench `await_hydration` stops
at the FIRST `pokeEnd`, so it likely times ref's early ack vs Rust's full poke —
not apples-to-apples.

Two ways to close it, both conformance-sensitive and in the poke path:
1. Split the hydration poke: send the gotQueriesPatch `pokeEnd` first, then
   stream rowsPatch in subsequent pokes (match ref's under-load behavior). This
   directly drops Rust's first-`pokeEnd` latency and is the single highest-value
   lever; must stay byte-green for the single-connection conformance scenarios.
2. Cut raw poke-JSON serialization CPU per row (the only remaining per-hydration
   cost of size), so 300-way serialization on one core stays bounded.

The landed micro-opts (clone-elim 3315047, drop-TableSource 5676889) are correct
and real (hydrate 3184->1902ms) but sub-dominant; neither #1 nor #2 is a safe
solo edit here while the worker/poke path is under concurrent rework.

### 9g. HONESTY CORRECTION: ref's fast hydrate is NOT an early-ack artifact

Checked upstream `client-handler.ts`: it flushes a `pokePart` every
`PART_COUNT_FLUSH_THRESHOLD = 100` rows (`:294`), but sends the single `pokeEnd`
only AFTER the whole row loop completes (`:336`). So the bench's
`await_hydration` (first `pokeEnd`) DOES measure full 1000-row delivery for ref
too — 9f's "ref emits an early gotQueries pokeEnd and streams rows after"
hypothesis is WRONG. The ~4.7ms ref p50 is a genuine full hydration.

So the gap is real per-hydration efficiency, not a measurement mismatch: ref
(Node) completes a 1000-row hydration to `pokeEnd` in ~4.7ms p50 under 300-way /
1-CPU load; Rust takes ~1900-2280ms. Likely contributors on the ref side that
Rust lacks: (a) V8's `JSON.stringify` is C++-native and very fast for 1000 small
objects (~sub-ms), whereas Rust's poke body build + serialize path is heavier;
(b) ref overlaps the many `#push` sends (I/O) across connections so JS CPU is not
the whole wall; (c) ref serves rows from the in-memory IVM view without any
per-connection SQLite read. The concrete, apples-to-apples next step is to
capture the actual rowsPatch byte/row count ref sends to a 2nd client in a shared
group (empirical, not inferred) and to micro-time Rust's poke-body serialization
in isolation — both still open. The landed opts + this localization stand; the
remaining closer is per-connection poke-serialization CPU, in the poke path under
concurrent rework.

### 9h. RESULT + final localization: leaf CPU optimized; residual is CVR-flush/orchestration

Landed the single-buffer poke serializer (no per-row clone, no per-op String,
0.22 ms/1000-row poke). Flag-on 30x10 fanout bench: hydrate p50 2280 -> 2138ms,
connected 46 -> 48%, CPU still pegged 100%. A real but small (~6%) gain — so poke
serialization was ALSO not the dominant cost.

Adding up every leaf CPU cost now micro-timed in isolation:
  fetch 0.71ms + CVR row-processing 1.1ms + poke serialize 0.22ms ~= 2 ms/hydration.
Across 300 serialized on one core that is ~600ms — yet the bench p50 is ~2138ms.
So ~1500ms is NOT in these leaves; it is the ORCHESTRATION around them:
  - persist_transition / CVR flush (original finding: 200-1500ms per persist
    under load) — serialized PER GROUP by the group processor loop, which is why
    a bigger CvrPool (9e) did not help;
  - the per-group transition lock held across the whole hydrate;
  - Snapshotter advance_to_head + BEGIN CONCURRENT per connect.

Conclusion: the three safely-isolatable leaf costs are now optimized (cumulative
hydrate 3184 -> ~2138ms, ~33% this arc, all conformance/byte-identity green). The
residual gap to ref (4.6ms) is the CVR-flush/orchestration layer, which needs a
running-server step profiler (not micro-benchmarks) and overlaps the concurrent
worker-starvation rework — it is not a safe solo edit from here. The precise next
measurement is per-step timing instrumentation inside hydrate_put (advance_to_head
vs fetch vs process vs persist) under a small flag-on run to attribute the ~1500ms.

### 9i. DEFINITIVE (running-server data): hydrate_put IVM work = 12ms; the ~2100ms wall is the CVR flush, NOT the IVM/leaf path

Ran the flag-on 30x10 fanout bench with ZERO_LOG_SLOW_HYDRATE_THRESHOLD=1 (added
a compose passthrough) and read `zero-bench-rust` container logs. The existing
`maybe_log_slow_hydrate` times `hydrate_put`'s internal work (advance_to_head +
fetch + process + register + poke build) for the real 1000-row query:

  elapsedMs distribution over 272 hydrations: min=5 p50=12 p90=18 p99=22 max=32.

So the ACTUAL per-connection IVM/hydration work is ~12ms — fast and bounded. Yet
client-observed hydrate p50 that same run = 2634ms. Therefore ~99.5% of the wall
(~2620ms) is OUTSIDE hydrate_put: it is `persist_transition` (the CVR Postgres
flush) plus the per-group processor loop SERIALIZING a group's 10 connect
transitions. Arithmetic matches: 10 conns/group x (~12ms hydrate + ~200ms
persist) ~= 2120ms for the last conn in a group; the ~200ms persist is CVR-write
contention on the 1-CPU postgres-rust across 30 groups' concurrent flushes.

This CLOSES the leaf-vs-orchestration question with running-server data: every
leaf CPU optimization this arc (clone-elim, drop-TableSource, single-buffer poke
serializer) was correct but targeted the 12ms, not the 2600ms. The entire
residual flip-gate is the CVR Postgres flush + per-group transition
serialization — exactly the item memory `perf-gate-ivm-architecture` already
identifies ("CVR Postgres flush is the entire remaining bottleneck;
Postgres-CPU-bound"). The concrete closers (all in the persist/group-loop layer,
the concurrent worker-starvation rework's area): batch a group's near-simultaneous
connect transitions into ONE config commit; cut commits/round-trips per
transition to match upstream's cheap CVRStore flush; and/or ensure the deferred
row flush truly keeps rows off the connect critical path so only a tiny config
commit remains. hydrate_put itself needs no further optimization.

### 9j. NEGATIVE RESULT: optimistic poke (spawn the whole flush) REGRESSES — the wall is Postgres write VOLUME, not ordering

Implemented `ZERO_OPTIMISTIC_CVR_FLUSH` (default off): under it, `persist_transition`
spawns the ENTIRE flush (config CAS + rows) chained through the group barrier and
returns immediately, so the hydration poke is emitted at IVM speed instead of
waiting on the ~200ms config commit. Reconnect safety is preserved (the barrier's
`wait_for_pending` already gates durable loads; the config now rides the same
barrier). Built green, default path unchanged (unit tests pass).

Bench (flag-on 30x10 fanout), both variants REGRESS vs the 2138ms/48% baseline:
- optimistic on BOTH commit + connect paths: connected 48%->10.7%, hydrate p50
  ->3928ms, fan-out pokes 2000->7312 (the async commit path removed backpressure,
  loop ran wild).
- optimistic on the CONNECT path only (commit stays synchronous): connected
  ->27%, hydrate p50 ->7673ms. Still worse.
- ping p50 DID drop to ~1.2ms (loop no longer blocks on persist) — but hydration
  and connect collapsed.

Why: the optimistic spawn does NOT reduce the Postgres work; it just moves the
same 300x (config + rows) writes onto the group barrier chain. Everything that
must observe durable state (`wait_for_pending` on refresh/reconnect, and the
hydration consistency wait) then blocks on the huge backlog, so latency gets
WORSE, and the synchronous config commit's implicit backpressure (which bounded
the poke rate) is gone. This DISPROVES the entire client-side-reordering class
(optimistic poke / further deferral): the wall is Postgres CPU saturation from
the VOLUME of CVR writes (config + 1000-row rows) x300 on one core.

Reverted the experiment (kept the default-off flag out of the tree — the plan
deletes knobs, and a knob that regresses has no value). The ONLY remaining lever
is REDUCING the Postgres write volume: batch a group's connect transitions into
ONE config commit (30 commits instead of 300), and/or match upstream's cheaper
per-hydration Postgres footprint. Batching needs the group-loop restructure whose
risk (interleaved Attach changes fan-to-others membership; per-connection poke
cookie chains) requires a reliable MULTI-connection byte gate — which the pinned
conformance scenarios (single-connection) do not provide and which concurrent-
agent Docker container collisions currently make flaky. That gate is the true
prerequisite; §9a-9i localize the target, 9j rules out the cheap fix.

### 9k. NEGATIVE RESULT #2: batching connect transitions into one config commit also REGRESSES

Implemented the batching fix (the §9j "only remaining lever"): a burst-draining
group loop (`handle_command_burst`) that stages each connect transition AT its
own point in the burst (so fan-to-others membership + per-connection cookie chain
are identical to processing it alone — verified byte-safe: batch-of-1 is
unchanged, and a new `burst_of_desired_queries_pokes_every_connection` unit test
plus the 4 existing group tests pass), then persists the WHOLE burst with ONE
config commit and sends. Correctness held; the bench did not.

Bench (flag-on 30x10 fanout) vs 2138ms/48% baseline:
- MAX_BURST=64: connected 47%, hydrate p50 20941ms, fan-out pokes 14020, mem 994MiB.
- MAX_BURST=8:  connected 57%, hydrate p50 27244ms, fan-out pokes 14831, mem 1024MiB (ceiling).

Both REGRESS hydrate by ~10x. Two compounding causes: (1) batching inherently
trades per-connection LATENCY for throughput — every connection in a burst waits
for the whole burst to stage + the shared persist before ITS poke is sent, and
the bench measures hydrate LATENCY (time to pokeEnd), so the median gets worse;
(2) holding a burst's built frames in memory pushes to the 1GiB ceiling, and the
greedy command drain (biased select prefers commands) starves the commit-
coalescing branch, so commits stop coalescing and the steady-state fan-out
explodes (~14k vs ~2k). Reverted.

CONCLUSION (both levers tested): NEITHER client-side reordering (§9j optimistic)
NOR transition batching (§9k) closes the flag-on hydration gap; both were fully
implemented, unit-tested green, and benched, and both regress. The wall is
intrinsic — the per-hydration CVR Postgres write (config CAS + 1000-row rows)
x300 on a 1-CPU Postgres — and the metric is per-connection latency, which
batching cannot improve and reordering only shifts. Closing it needs a
DIFFERENT-KIND change than loop restructuring: cut the actual Postgres work per
hydration to match upstream's CVRStore footprint (fewer/cheaper writes at the SQL
level, or a materially different CVR-persistence design), which is the deep
multi-session item the plan/memory already flag — not a loop-level fix. §9a-9i
localize it; 9j+9k rule out the two loop-level fixes with data.

### 9l–9m. BREAKTHROUGH: stop re-hydrating already-executed queries (connect 48%->100%)

Direct Postgres profiling (ZERO_LOG_PERSIST_MS) found the real flag-on wall:
config_flush ran ~3s and EVERY connect transition deferred 1000 rows. Root
cause: the port re-hydrates a query for EVERY connection in a client group
(upstream executes it ONCE per group), and merge_ref_counts is additive
(upstream-faithful), so each re-hydration re-writes all 1000 rows with a bumped
ref-count -> 300x1000 = 300K redundant Postgres writes saturate the 1-CPU
postgres, ballooning the config commits.

A row's CVR ref-count is per-QUERY (kept while ANY client desires the query,
dropped only when the query leaves the group; the incremental path already sets
it with `refs.insert(query_id,1)`), so its VALUE is immaterial to GC. Fix, gated
on `self.tracked.contains(hash)` (= already executed for the group):
- 74c94b2: drop the redundant row re-writes -> connected 48->77%, mem 726->405,
  config_flush 3s->553ms.
- 2d9f19f: skip the WHOLE re-hydration (existing-row index build + SQL fetch +
  row processing — the now-dominant SERVER CPU), record only the connection's
  pipeline DESIRE (register_query -> Unchanged; MUST do this or the shared query
  is dropped: fan-out explodes + the detach unit test fails), and serve rows from
  the group's shared row_bodies via force_wire_rows.

RESULT (flag-on 30x10 fanout): connected 48->**100%** (ref 90%), hydrate p50
2138->**733ms**, mem 726->**339MiB** (ref 328 = PARITY), ping p50 1.9ms. All 5
group unit tests green (+ new burst test). Conformance-safe by construction (a
group's FIRST connection still fully hydrates; single-connection conformance
never triggers the skip).

REMAINING gaps and next levers:
- fan-out pokes 21588 vs ref 1384. The writer commits 1 row every 0.25s (~120
  commits/30s); Rust fans ~72 to each of 300 clients while ref coalesces ~24
  commits per poke (~5/client). This flood backs up the per-connection writer
  channels, inflating p99 ping (741ms vs 3ms) + CPU (99% vs 49%) + dragging
  throughput (890 vs 1184). NEXT: coalesce commits into fewer, larger pokes
  (batch the group loop's commit processing over a short window / match
  upstream's poke batching) — trades a little poke latency for far less per-poke
  framing+send overhead.
- hydrate p50 733ms vs 4ms: now the 30 first-per-group full hydrations (1000-row
  fetch+write+config) + 300 config commits serialized per group; diminishing but
  real.
