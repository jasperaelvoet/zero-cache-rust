# Query Pipeline Redesign — matching upstream Zero v1.7 IVM

Status: design spec (no code yet). Target: replace the per-connection,
re-materializing query pipeline with a client-group-owned, replica-backed,
incremental operator graph matching `zero/v1.7.0`, and pass the `<10%` perf
gate on the fanout workload.

## 0. Why (the measured problem)

The port is conformance-green but fails the perf gate by orders of magnitude
on fanout (21% clients sustained vs 99%; hydrate p50 11.5s vs 4.5ms; CPU
pegged). Three coupled root causes, all in the query pipeline:

1. **A `PipelineDriver` is built per WebSocket connection.**
   `crates/zero-cache-server/src/bootstrap.rs:376` constructs a fresh
   `PipelineDriver` inside the per-connection closure, and each one opens its
   own read-only replica handle (`bootstrap.rs:365`) and its own
   `Snapshotter`. N clients in a group ⇒ N replicas, N snapshot pairs, N full
   hydrations of the *same* queries.

2. **Every source is a full in-memory `SELECT *`.**
   `PipelineDriver::hydrate_sources` (`pipeline_driver.rs:113`) runs
   `SELECT <cols> FROM <table>` for *every* table and loads all rows into a
   `Vec<Row>`-backed `zero_cache_zql::ivm::table_source::TableSource`
   (`table_source.rs:36`). Memory and hydrate time scale with total table
   size × connections, not with query result size.

3. **Non-trivial queries recompute from scratch every commit.**
   `advance` (`pipeline_driver.rs:191`) only has a real incremental path for
   "direct" queries (`is_direct_incremental_query`, `pipeline_driver.rs:295`
   — no limit, no start, no related, no correlated subquery). Anything with a
   join, `EXISTS`, limit, or `related` falls through to
   `materialize_query` (`pipeline_driver.rs:370`), an O(table) full
   re-scan-and-diff on **every** commit for **every** affected query.

Upstream avoids all three: a single client-group `PipelineDriver` owns one
snapshotter and a set of **replica-backed** `TableSource`s (SQL pushdown, one
per table, *shared* across all of the group's query pipelines), and advances
each query by pushing individual `SourceChange`s through a real incremental
**operator graph** whose per-operator state lives in `Storage`.

References read for this spec (upstream `mono-src/`, port `crates/`):
`zql/src/ivm/operator.ts`, `source.ts`, `take.ts`, `skip.ts`, `fan-in.ts`,
`fan-out.ts`, `exists.ts`, `join.ts`, `flipped-join.ts`, `join-utils.ts`;
`zero-cache/src/services/view-syncer/pipeline-driver.ts`;
`zqlite/src/table-source.ts`, `database-storage.ts`;
port `ivm/{operator,table_source,filter,join,change,memory_storage}.rs`,
`view-syncer/pipeline_driver.rs`, `sqlite/{snapshotter,sqlite_table_source}.rs`.

---

## 1. Target architecture

### 1.1 The upstream model, mapped to Rust

```
                     one per client group (NOT per connection)
  ┌──────────────────────────────────────────────────────────────────┐
  │ PipelineDriver                                                     │
  │   Snapshotter (2 BEGIN CONCURRENT conns: prev, curr)              │
  │   storage: DatabaseStorage  (per-op KV, namespaced by opID)       │
  │   tables:  HashMap<String, Rc<RefCell<SqliteSource>>>  ← SHARED    │
  │   pipelines: HashMap<QueryId, Pipeline{ root: Rc<dyn Input>,       │
  │                                         collector: Rc<Collector> }>│
  └──────────────────────────────────────────────────────────────────┘
         │ advance(diff)                       ▲ fetch(req) → Stream<Node>
         ▼                                     │
   for each SourceChange:  source.push(change) │ pushes Change through graph
         │                                     │
         ▼                                     │
  SqliteSource ── Change ──► Filter ─► Join ─► Take ─► ... ─► Collector
   (SQL fetch)                (operators hold Storage-backed state)
```

Three shifts from today:

- **Ownership** moves from per-connection to per-client-group. The driver and
  its sources/pipelines are shared; connections attach/detach queries and read
  pokes (Section 6).
- **Sources** become replica-backed and *shared per table across all of a
  group's pipelines* (upstream `#getSource` memoizes in `#tables`,
  `pipeline-driver.ts:1054`), replacing the per-query in-memory `Vec<Row>`.
- **Advancement** becomes push-through-graph, not re-materialize
  (`pipeline-driver.ts:#advance`/`#push` at `:948`/`:1006`), replacing
  `materialize_query`.

### 1.2 The Rust ownership/mutability problem

Upstream is a **mutable observer graph**: each operator holds a mutable
reference to its downstream `Output` (`input.setOutput(this)`), and `push`
recurses downstream while `fetch` recurses upstream — often *during* a push
(e.g. `Join.#pushChildChange` calls `this.#parent.fetch({constraint})` in the
middle of handling a child change, `join.ts:231`). Operators also mutate their
own state on push (`Take`'s `TakeState`, `Exists`'s size cache). Rust cannot
express upstream's "everyone holds a `&mut` to everyone" graph directly.

Three candidate representations:

| Option | Shape | Pros | Cons |
|---|---|---|---|
| **A. `Rc<dyn Input>` nodes, interior `RefCell`/`Cell` per field** | Each operator is `Rc<Op>`; mutable fields (`output`, in-progress child change) are `RefCell`; child/parent are `Rc<dyn Input>`. `push(&self, ...)`. | Direct 1:1 with upstream `setOutput`/`push`; fan-out is just multiple `Rc` clones of a downstream; the current `Join` (`join.rs:257`) already works this way. | Must reason about borrow discipline: a node may not hold a `borrow_mut` of *its own* state across a call that re-enters the same node. Re-entrancy is real (`Exists` asserts `!inPush`, `exists.ts:110`). |
| **B. Arena + index edges** | `Vec<OpState>` arena, edges are `NodeId` (usize); a free-standing `push(graph, node, change)` walks by index. | No `Rc`/`RefCell`; one owner (`&mut Graph`); cache-friendly. | `fetch` mid-`push` needs simultaneous shared reads of upstream and `&mut` of the current node → fights the borrow checker exactly where upstream is most re-entrant; large rewrite of every operator to a non-method form. |
| **C. Enum-dispatch operator tree, owned inline** | `enum Operator { Source(..), Filter(Box<Operator>,..), Join{parent:Box<Operator>,child:Box<Operator>,..}, .. }` owned as a tree; `push`/`fetch` are `&mut self` match arms. | Static dispatch; no interior mutability for the *tree*; ownership is a plain tree. | A pure tree can't express fan-out/fan-in diamonds (one upstream feeding two `Output`s) or a `TableSource` shared by two pipelines — exactly what Section 0 needs. Would need `Rc` at the shared nodes anyway, degenerating toward A. |

**Recommendation: Option A** — `Rc<...>` operator nodes with **interior
mutability scoped to individual fields**, `dyn Input` for upstream edges and
`Rc<dyn Output>` for downstream edges. Rationale:

- It is the only option that natively expresses the two kinds of sharing
  Section 0 requires: a `SqliteSource` shared by every pipeline in the group
  (upstream `#tables`), and fan-out feeding multiple `Output`s.
- It mirrors upstream method-for-method, so the exhaustive upstream `*.test.ts`
  suites port with minimal reinterpretation.
- The port's existing `Output` trait (`operator.rs:120`) and `Join`
  (`join.rs:257`) already committed to this shape and pass tests.

**Borrow-safety rule (make it a reviewed invariant, not a hope):** an
operator's `push`/`fetch` may borrow *its own* `RefCell` fields only across
code that does **not** re-enter the same node. Concretely:

- Mutable per-op state (Take's `TakeState`, Exists' cache) is read/written to
  **`Storage`** (owned `Rc<dyn Storage>`, itself interior-mutable), not held in
  a `RefCell` borrow across a downstream `output.push`. This matches upstream,
  where all cross-push state is in `Storage` precisely so it survives
  re-entrancy — and it removes the aliasing hazard by construction.
- The only long-lived `RefCell` on an operator is `#output` (set once) and
  small "in-progress" markers (`Join.#inprogressChildChange`), which are
  `.take()`-style read into a local before any recursive call.
- Sources hold their SQLite handle in a `RefCell<StatementRunner>` swapped on
  advance (upstream `table.setDB`, `pipeline-driver.ts:1044`); `fetch` takes a
  short `borrow()`, never held across a `push`.

Because the whole graph is `!Send` (SQLite `StatementRunner` is `!Send`) and
already runs on a dedicated per-group thread/task, `Rc`/`RefCell` (not
`Arc`/`Mutex`) is correct and free of contention.

---

## 2. Trait foundation

Evolve `crates/zero-cache-zql/src/ivm/operator.rs`. The port already has
`Output`, `Storage`, `SourceSchema`, `FetchRequest`, `Start`, `MultiConstraint`
(`operator.rs:120-173`) and a `Stream<'a,T> = Box<dyn Iterator<Item=T>+'a>`
alias with no `'yield'` variant (correct — Rust iterators are pull-based;
`operator.rs:57`). What's missing is `InputBase`/`Input`/`Operator` and giving
`Output::push` the `pusher` argument.

### 2.1 Target signatures (matching `operator.ts:14-140`)

```rust
/// Port of InputBase (operator.ts:14).
pub trait InputBase {
    fn get_schema(&self) -> SourceSchema;
    /// Destroys the input, cascading to upstreams (operator.ts:19).
    fn destroy(&self);
}

/// Port of Input (operator.ts:26). `Rc<dyn Input>` is the upstream-edge type.
pub trait Input: InputBase {
    /// Tell the input where to send output (operator.ts:28).
    fn set_output(&self, output: Rc<dyn Output>);
    /// Returns nodes sorted per SourceSchema.compare_rows (operator.ts:43).
    /// No 'yield': pacing is the caller not calling .next() (see module doc).
    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node>;
}

/// Port of Output (operator.ts:93). NOTE the added `pusher`.
pub trait Output {
    /// Push an incremental change. `pusher` identifies the caller so a
    /// downstream that fans back (fan-in) can attribute the source branch.
    fn push(&self, change: Change, pusher: &dyn InputBase);
}

/// Operators are both Input and Output (operator.ts:126).
pub trait Operator: Input + Output {}

/// Storage — already present (operator.rs:131), unchanged shape.
pub trait Storage {
    fn set(&self, key: &str, value: JsonValue) -> Result<(), StorageError>;
    fn get(&self, key: &str, default: Option<JsonValue>)
        -> Result<Option<JsonValue>, StorageError>;
    fn scan(&self, prefix: Option<&str>) -> Result<Vec<(String, JsonValue)>, StorageError>;
    fn del(&self, key: &str) -> Result<(), StorageError>;
}
```

Helper types to add (thin, mechanical):

```rust
/// The "not yet wired" Output (operator.ts:114 throwOutput). Panics on push.
pub struct ThrowOutput;
impl Output for ThrowOutput { fn push(&self, _: Change, _: &dyn InputBase) { unreachable!("Output not set") } }

/// SourceSchema gains what operators need for ordering/keys.
pub struct SourceSchema {
    pub table_name: String,
    pub primary_key: PrimaryKey,
    pub sort: Ordering,
    pub relationships: BTreeMap<String, Box<SourceSchema>>, // NEW: for Join/Exists
    // system/is_hidden/columns still deferred until permissions need them.
}
impl SourceSchema {
    pub fn compare_rows(&self, a: &Row, b: &Row) -> Ordering; // make_comparator(sort)
}
```

### 2.2 What changes, what breaks

- **`Output::push` gains `pusher: &dyn InputBase`.** Breaks the current
  1-arg `Output` in `operator.rs:121` and its two impls: the `SpyOutput` test
  double in `join.rs:726` and `Join::push_child_change`'s call at
  `join.rs:328`. Both are mechanical (thread the arg through). The `Filter`
  in `filter.rs` returns `Option<Change>` today and is *not* an `Output` — it
  becomes a real `Operator` (Section 4).
- **`Node.relationships` stays eager `HashMap<String, Vec<Node>>`**
  (`operator.rs:67`) for correctness now, but upstream stores relationships as
  **lazy thunks** `() -> Stream<Node>` (`join.ts:256`, `#processParentNode`).
  For perf parity we must move to lazy relationships (a boxed closure or a
  small `Relationship` enum `{ Fetched(Vec<Node>) | Lazy(Rc<dyn Fn()->Stream>) }`)
  so a `Join` feeding a `Take` that consumes 5 rows does not fetch every
  child. This is a breaking change to `Node`; do it in the Join increment, not
  before (Section 7).
- **`MemoryStorage` (`memory_storage.rs`) is kept** as the test-time `Storage`
  and for unit tests ported from `memory-storage.test.ts`. Production uses the
  new `DatabaseStorage` (Section 3.4).
- **The existing in-memory `TableSource` (`table_source.rs`) is retained only
  as a test source** for operator unit tests (it already models `push` +
  `fetch` + pk-identity, matching `memory-source.ts` closely enough for
  operator tests). It is removed from the production path (Section 5).

---

## 3. Replica-backed `TableSource`

Goal: one `Source` per table, reading committed replica state via SQL
pushdown, applying `SourceChange`s by fanning to connected `Output`s, and
sharing a snapshot connection that leapfrogs on advance. Port of
`zqlite/src/table-source.ts`.

### 3.1 Building block already exists

`crates/zero-cache-sqlite/src/sqlite_table_source.rs` already does the **read**
half: `fetch`/`fetch_filtered` (`sqlite_table_source.rs:170`/`:185`) push
`constraint`, `multi_constraints`, `order`, `reverse`, `start` **and** an
arbitrary AST `Condition` down into SQL via
`query_builder::build_select_query`, and restore declared value types
(`with_column_types`, boolean/JSON coercion). It is currently a borrowing
`SqliteTableSource<'a>` holding `&'a StatementRunner`. Two things are missing
for the operator role: (a) it is not an `Input`/`Source` (no `push`, no
`set_output`, no schema-driven `compare_rows`), and (b) it borrows its DB
rather than owning a swappable handle.

### 3.2 New type: `SqliteSource` (owns handle, is a `Source`)

```rust
// crates/zero-cache-sqlite/src/sqlite_source.rs  (new)
pub struct SqliteSource {
    db: RefCell<StatementRunner>,     // swapped on advance (upstream setDB)
    schema: SourceSchema,
    columns: Vec<String>,
    column_types: BTreeMap<String, ColumnType>,
    outputs: RefCell<Vec<Rc<dyn Output>>>, // connect() registers here
    overlay: RefCell<Option<Overlay>>,     // Section 3.3 (phase 2)
}

impl SqliteSource {
    pub fn set_db(&self, db: StatementRunner);         // upstream table.setDB
    pub fn connect(self: &Rc<Self>, sort: Ordering, filters: Option<Condition>)
        -> Rc<dyn Input>;                              // upstream Source.connect
}

impl Input for SqliteSource { /* fetch delegates to build_select_query */ }
```

- **`fetch`** reuses the exact SQL-pushdown path from
  `sqlite_table_source.rs` (`build_select_query` + `query_uncached` +
  `sqlite_to_value`). Only the result-size rows a query actually needs are
  read — this is the fix for root cause #2.
- **`push(change: SourceChange)`** (upstream `Source.push`,
  `table-source.ts`): the replica is written by the **replicator**, not by the
  source (the source reads committed state). So `push` does **not** write
  SQLite; it (1) records the change in the transaction overlay (phase 2), and
  (2) elaborates the `SourceChange` into an operator `Change` (`Add`/`Remove`/
  `Edit` `Node`) and calls `output.push(change, self)` on every connected
  output. This is the driver's `#push` fan-out (`pipeline-driver.ts:1006`).
- **Multiple connections/outputs.** Upstream `connect()` returns a distinct
  `SourceInput` per consuming pipeline, each with its own sort/filter, all
  sharing the underlying table + overlay. Model each as a lightweight
  `Rc<ConnectedSource>` that holds `Rc<SqliteSource>` + its own `sort`/`filters`
  and its own `Rc<dyn Output>`. `SqliteSource::push` iterates all connections.

### 3.3 Snapshot isolation and the overlay

The `Snapshotter` (`crates/zero-cache-sqlite/src/snapshotter.rs`) already
implements the v1.7 leapfrog: two `BEGIN CONCURRENT` connections (`prev`,
`curr`); `advance_without_diff` (`snapshotter.rs:210`) rolls `prev` forward to
head and swaps roles; `advance` (`:224`) also returns a `SnapshotDiff` of
`SnapshotChange { table, prev_values, next_value, row_key }` computed from the
change-log (`materialize_diff`, `:254`).

Wiring to sources:

1. On advance, the driver reads the diff from the **curr** snapshot, pushes
   each `SourceChange` through the graph (sources still pointed at whatever
   handle they hold), then calls `source.set_db(curr.db())` on **every** source
   (upstream loop `pipeline-driver.ts:1043`). After that, `fetch` sees head
   state.
2. **Overlay (upstream `generateWithOverlay`, deferred to phase 2).** During a
   single advance, a source's committed SQLite state is still `prev` while we
   push changes; a `Join.fetch` issued mid-push must see the not-yet-committed
   row. Upstream interleaves an in-memory overlay of the pending push with the
   committed stream. The port's `SqliteTableSource` module doc
   (`sqlite_table_source.rs:12`) explicitly flags this as unbuilt. For the
   fanout workload (single-row commits, no self-referential mid-push fetch on
   the changed row) it is not on the critical path; build it only when a ported
   `join.push.test.ts`/`take.push.test.ts` case fails without it. Represent it
   as `Overlay { change: Change, position: Option<Row> }` consulted by `fetch`.

### 3.4 `DatabaseStorage` for operator state

Port `zqlite/src/database-storage.ts`. One SQLite DB per group-thread
(ephemeral, `journal_mode=OFF`, `synchronous=OFF`, `locking_mode=EXCLUSIVE`),
one table:

```sql
CREATE TABLE storage (clientGroupID TEXT, op NUMBER, key TEXT, val TEXT,
                      PRIMARY KEY(clientGroupID, op, key));
```

`create_storage()` hands each operator a monotonically increasing `opID`
(`database-storage.ts:172`); the returned `Storage` impl scopes all ops to
`(cgID, opID)`. Commit is batched every N writes (`#maybeCheckpoint`,
`:135`). In Rust: `struct DatabaseStorage { db: StatementRunner, ... }` with
`fn create_client_group_storage(&self, cg: &str) -> ClientGroupStorage`, and
`ClientGroupStorage::create_storage(&self) -> Rc<dyn Storage>` incrementing
`next_op_id`. Keep `MemoryStorage` as the trait's test impl.

---

## 4. Per-operator port plan

Each operator becomes a module under `crates/zero-cache-zql/src/ivm/`,
implementing `Operator` (`Input` + `Output`). Ordered by dependency so agents
can build them in parallel waves. For each: upstream source, its `Storage`
keys, push/fetch semantics, the Rust module, deps, and the upstream test to
port.

**Wave 0 (foundation, no operator deps):** the trait changes (Section 2),
`SqliteSource` (Section 3), `DatabaseStorage` (Section 3.4), and lazy
`Node.relationships`. Port `memory-source.test.ts` (subset) against the
retained in-memory `TableSource` and `database-storage.test.ts`.

### 4.1 Filter — `ivm/filter.rs` (evolve existing)

- **Upstream:** `filter.ts` + `filter-push.ts`; edit-splitting contract.
- **State:** none (stateless). No `Storage`.
- **push:** already correct in the port — `filter.rs:51` splits `Edit` into
  `Add`/`Remove`/`Edit`/drop based on old/new predicate verdicts. Change: it
  currently *returns* `Option<Change>`; make it an `Operator` that calls
  `self.output.push(change, self)` instead, and gains `fetch` that filters the
  upstream stream (`filter.rs:40` already does this against a `TableSource`;
  retarget to `Rc<dyn Input>`).
- **fetch:** pass-through filtered by predicate.
- **Deps:** none. **Test:** `filter.test.ts`, `filter-operators.test.ts`.

### 4.2 Skip — `ivm/skip.rs` (new)

- **Upstream:** `skip.ts:33`. Constructor `(input, Bound{row, exclusive})`.
- **State:** none — the bound is a constructor arg, not `Storage`.
- **push:** translates positions relative to the bound; uses
  `maybe-split-and-push-edit-change` for edits crossing the bound
  (`skip.ts`, imported `:12`).
- **fetch:** injects `start = {row: bound.row, basis: at/after}` into the
  upstream `FetchRequest` (`skip.ts:53`), honoring `reverse`.
- **Deps:** none (leaf-ish). **Test:** `skip.test.ts`.

### 4.3 Take — `ivm/take.rs` (new)

- **Upstream:** `take.ts:55`. Constructor `(input, storage, limit, partitionKey?)`.
- **`Storage` keys:**
  - `MAX_BOUND_KEY = "maxBound"` → `Row` (the largest bound ever taken, an
    overfetch hint; `take.ts:27`).
  - per-partition `takeStateKey` (from `getTakeStateKey(partitionKey, constraint)`)
    → `TakeState { size: number, bound: Option<Row> }` (`take.ts:29`).
- **push:** maintains `size ≤ limit` invariant at all times, tracks the
  boundary row, and on boundary changes fetches one replacement row from the
  input to backfill — the classic incremental limit. Emits `Add`/`Remove`
  (and `Edit` when a row moves within-window) accordingly. This is the single
  most intricate push (upstream `take.push.test.ts` is 208 KB); budget for it.
- **fetch:** `#initialFetch` on cold state (fills `TakeState`), else bounded
  by stored `bound` (`take.ts:93-120`).
- **Deps:** `Storage`. **Test:** `take.fetch.test.ts`, `take.push.test.ts`,
  `take.push-child.test.ts`.

### 4.4 FanOut / FanIn — `ivm/fan_out.rs`, `ivm/fan_in.rs` (new, paired)

- **Upstream:** `fan-out.ts:17`, `fan-in.ts:30`. These are `FilterOperator`s
  (a sub-protocol `beginFilter`/`endFilter`/`filter(node)` + `push`) used to
  evaluate **OR** conditions: `FanOut` forks the stream to one branch per OR
  arm; each arm is a `Filter`; `FanIn` merges and de-duplicates.
- **State:** `FanOut` holds `outputs: Vec<FilterOutput>` + a back-ref to its
  `FanIn` (`fan-out.ts:19-28`). `FanIn` accumulates pushes in
  `#accumulatedPushes: Vec<Change>` (`fan-in.ts:34`) and flushes them via
  `pushAccumulatedChanges` (dedup by row identity + change type) when
  `fanOutDonePushingToAllBranches` fires (`fan-in.ts:76`). No `Storage`.
- **push:** `FanOut.push` pushes to every branch then signals `FanIn`
  (`fan-out.ts:74`); `FanIn.push` just buffers (`fan-in.ts:71`); the flush
  merges + dedups so a row matching two OR arms is emitted once.
- **fetch:** `FanOut.fetch`/`filter` short-circuits `true` on first matching
  branch; `FanIn.fetch` is pass-through.
- **Deps:** `Filter`; introduces the `FilterOperator` sub-protocol
  (`beginFilter/endFilter/filter`) — port `push-accumulated.ts` alongside.
- **Test:** `fan-out-fan-in.test.ts`, `push-accumulated.test.ts`,
  `filter-operators.test.ts`.

> Note: `FanIn`/`FanOut` are only needed for OR queries. If the fanout
> benchmark and conformance corpus contain no `OR` at the pipeline level, this
> wave can be deferred behind Join/Take without blocking the perf gate — verify
> against the corpus before scheduling (Section 8).

### 4.5 Exists — `ivm/exists.rs` (new)

- **Upstream:** `exists.ts:21`. `FilterOperator`. Constructor
  `(input, relationshipName, parentJoinKey, "EXISTS"|"NOT EXISTS")`.
- **State:** an in-memory relationship-**size** cache `Map<String, bool>`
  keyed by the parent's `parentJoinKey` values (`#getCacheKey`, `exists.ts:224`),
  cleared on `endFilter` (`:76`). A `#inPush` flag disables cache reuse during
  a push because relationships are transiently inconsistent mid-push
  (`exists.ts:39`, asserted non-re-entrant `:110`). `#noSizeReuse` when the
  join key *is* the primary key (`:61`). Not `Storage`-backed (cache is
  per-fetch-pass).
- **push:** `Add`/`Remove`/`Edit` and non-matching child changes cannot change
  relationship emptiness → `#pushWithFilter` (`exists.ts:116`). A child
  `Add`/`Remove` on the watched relationship *can* flip existence: compute the
  new size; if it crossed `0↔1`, convert the child change into an `Add` or
  `Remove` of the parent node downstream (`exists.ts:139-200`), with the
  `NOT EXISTS` cases inverted. The port already has the *value* of this logic
  in non-operator form: `reeval_exists_after_child_change` (`join.rs:172`)
  computes exactly "did this parent's EXISTS flip". Reuse that primitive inside
  the operator.
- **fetch:** filters nodes by (cached-or-computed) relationship non-emptiness
  (`exists.ts:80`).
- **Deps:** requires `Node.relationships` populated by a `Join` upstream (it
  reads `node.relationships[relationshipName]`, `exists.ts:249`). Build after
  Join.
- **Test:** `exists.test.ts`, `exists.fetch.test.ts`, `exists.push.test.ts`,
  `exists.flip.push.test.ts`.

### 4.6 Join — `ivm/join.rs` (rewrite existing to full operator)

- **Upstream:** `join.ts:51`. Constructor
  `(parent, child, parentKey, childKey, relationshipName, hidden, system)`.
  Registers itself as `Output` of *both* parent and child
  (`join.ts:98-103`).
- **State:** no `Storage` of its own for the basic join; correlation is
  recomputed via `buildJoinConstraint`. In-progress markers
  `#inprogressChildChange` / `#inprogressChildChangePosition`
  (`join.ts:61`) drive the overlay so a parent fetched mid-child-push sees the
  pending child.
- **push:**
  - `#pushParent` (`join.ts:129`): `Add`/`Remove`/`Child`/`Edit` re-emitted
    downstream after `#processParentNode` attaches the (lazy) child stream.
    Asserts a parent `Edit` doesn't change the join key (`:167`).
  - `#pushChild` (`join.ts:195`): for a child change, `#pushChildChange`
    (`:221`) builds the parent-side constraint via
    `buildJoinConstraint(childRow, childKey, parentKey)`, fetches the affected
    parent(s) (`parent.fetch({constraint})`, `:231`), and for each emits a
    `Child` change naming `relationshipName` (`:237`). This is the incremental
    core the port currently fakes with `reeval_relationship_after_child_change`
    (`join.rs:212`) — replace with the real operator.
- **fetch:** for each parent node, `#processParentNode` (`join.ts:252`) sets
  `relationships[relationshipName]` to a **lazy** child fetch keyed by the
  correlation, with overlay if a child push is in progress.
- **Deps:** lazy `Node.relationships`, `join-utils` (port
  `buildJoinConstraint`, `isJoinMatch`, `rowEqualsForCompoundKey`,
  `generateWithOverlay`). The port already has correlation constraint helpers
  (`join.rs:53`, `:143`).
- **Test:** `join.fetch.test.ts`, `join.push.test.ts`, `join.sibling.test.ts`,
  `join-utils.test.ts`.

### 4.7 FlippedJoin — `ivm/flipped_join.rs` (new)

- **Upstream:** `flipped-join.ts:85`. Same args as `Join` but the **inner**
  join fetches child-first then parents. Used by the planner when driving from
  the child side is cheaper (the cost model chooses; see
  `sqlite_cost_model.rs`). Batches child→parent fetches into
  `multiConstraints` chunked at `MULTI_CONSTRAINT_CHUNK_SIZE = 256`
  (`flipped-join.ts:55`), merging sorted streams via `mergeSortedStreams`;
  filters incompatible children with `constraintsAreCompatible`
  (`flipped-join.ts:14`).
- **State:** analogous in-progress markers; relies on the source's batched
  `IN` fetch (already supported by `SqliteSource` via `multi_constraints`,
  `sqlite_table_source.rs:268`).
- **push/fetch:** child-driven; `#fetchBatched` groups children by canonical
  parent key before emitting (per the `MultiConstraint` invariant doc,
  `operator.ts:52`).
- **Deps:** `Join` (shares `join-utils`), `SqliteSource` batched fetch.
- **Test:** `flipped-join.fetch.test.ts`, `flipped-join.push.test.ts`,
  `flipped-join.chunked.test.ts`, `flipped-join.sibling.test.ts`,
  `flipped-join.more-fetch.test.ts`.

**Build order (dependency waves):**

```
Wave 0: traits + SqliteSource + DatabaseStorage + lazy relationships
Wave 1: Filter (evolve), Skip, Take           [parallelizable]
Wave 2: Join, join-utils                        (needs lazy relationships)
Wave 3: Exists, FlippedJoin                     (need Join)
Wave 4: FanOut/FanIn + push-accumulated         (OR support; may defer)
```

---

## 5. Pipeline build + advance

### 5.1 `build_pipeline` (replaces `materialize_query` at hydration)

Port `zql/src/builder/builder.ts`'s `buildPipeline` (used at
`pipeline-driver.ts:521`/`:642`). Signature:

```rust
// crates/zero-cache-zql/src/builder/pipeline.rs (new)
pub struct BuildDelegate<'d> {
    pub get_source: &'d dyn Fn(&str) -> Rc<SqliteSource>,      // memoized per table
    pub create_storage: &'d dyn Fn() -> Rc<dyn Storage>,       // new opID each call
}
pub fn build_pipeline(ast: &Ast, delegate: &BuildDelegate) -> Rc<dyn Input>;
```

It walks the AST and instantiates operators bottom-up: `get_source(table)` →
`connect(sort, where)` → wrap in `Filter`/`FanOut`+`FanIn` for the condition →
`Join`/`FlippedJoin` per `related`/correlated-subquery (planner picks which) →
`Exists` per `EXISTS` correlated subquery → `Skip` for `start` → `Take` for
`limit`. Each operator gets its own `create_storage()` (unique opID). The
returned root `Rc<dyn Input>` has its `set_output` pointed at a **Collector**.

**Collector** = the port's downstream sink translating operator `Change`s into
`PipelineRowChange` (the existing public type, `pipeline_driver.rs:44`), so the
CVR/poke layers above are untouched:

```rust
struct Collector { query_id: String, out: RefCell<Vec<PipelineRowChange>> }
impl Output for Collector { fn push(&self, c: Change, _: &dyn InputBase) { /* flatten Add/Remove/Edit/Child → PipelineRowChange(s) */ } }
```

Hydration = build the pipeline, then drain `root.fetch(&FetchRequest::default())`
into `PipelineRowChange::Add`s (upstream `addQuery` consumes the fetch stream).

### 5.2 `advance` (replaces per-commit re-materialization)

New `advance` body (compare current `pipeline_driver.rs:191`):

```
diff = snapshotter.advance(specs, all_tables)?          // unchanged
for change in diff.rows:
    source = tables[change.table]  (skip if no pipeline reads it)
    // reconstruct Add/Remove/Edit exactly as upstream #advance (pipeline-driver.ts:993):
    //   pair prev_values/next_value by primary key → Edit; else Remove(prev)+Add(next)
    for sc in to_source_changes(change):
        source.push(sc)            // fans through the graph → Collector(s)
for source in tables.values(): source.set_db(diff.curr.db())   // upstream :1043
return drain all collectors' accumulated PipelineRowChange
```

Each `source.push` drives *only* the operators reachable from that table across
*all* pipelines that read it — O(change × affected-pipeline-depth), never
O(table). Collectors accumulate per query; the driver returns them and updates
`row_set_signatures` via the existing `apply_signature_changes`
(`pipeline_driver.rs:233`, keep as-is).

### 5.3 Runaway-push guard (keep the safety net)

Upstream can *abort* an advance and fall back to re-hydration when the push
projects to cost more than a fresh hydrate (`#shouldAdvanceYieldMaybeAbortAdvance`,
`pipeline-driver.ts:1094`; `advancementResetTimeLimitMs`, `:191`). Port a
simplified version: track cumulative push time; if it exceeds
`max(hydration_time, floor)`, discard the graph for that query and re-run
`build_pipeline`+`fetch` (a full re-hydrate) for the affected queries. This
preserves correctness under pathological pushes and is the graceful-degradation
path that keeps the incremental engine from ever being *slower* than today.

### 5.4 Delete list

Once Section 5.1/5.2 land and pass, remove from `pipeline_driver.rs`:
`materialize_query` (`:370`), `materialize_related` (`:440`), `matching_rows`
(`:391`), `apply_direct_changes` (`:315`), `is_direct_incremental_query`
(`:295`), `direct_row_matches` (`:360`), `has_correlated_subquery` (`:305`),
`correlated_subqueries` (`:493`), and the `Pipeline{ rows, referenced_tables }`
struct's `rows` field (`:75`). The `sql_row_to_zql`/value-typing helpers
(`:632`) move into `SqliteSource` (they duplicate
`sqlite_table_source.rs`'s `sqlite_to_value`; consolidate).

---

## 6. Client-group ownership

Today (`bootstrap.rs:365-394`) each connection: opens its own readonly replica,
loads specs, builds its own `PipelineDriver`, and stashes it on
`DesiredQueriesHandler.pipeline_driver` (`live_connection.rs:653`). `advance`
is called per handler (`live_connection.rs:2186`, `:2604`) and `add_query` per
handler (`:2616`).

Target: **one `PipelineDriver` per `clientGroupID`**, shared by every
connection in that group (upstream owns it inside the client-group
`ViewSyncerService`).

### 6.1 Concrete changes

- **A group registry.** Introduce a process-wide
  `ClientGroupRegistry { map: Mutex<HashMap<String, Weak<GroupPipeline>>> }`
  where `GroupPipeline` owns the single `PipelineDriver` (plus the group's
  `DatabaseStorage`), pinned to a dedicated single-thread executor (the driver
  is `!Send`). `bootstrap.rs` already derives `client_group_id` from the
  connect URL (`:399`) — use it to `get_or_create` the group's pipeline instead
  of building one inline at `:376`.
- **Driver runs on its own task/thread.** Because `PipelineDriver` is `!Send`
  (SQLite handles, `Rc`), it can't live in the async connection future. Run it
  on a `LocalSet`/dedicated thread and communicate via a command channel:
  `AddQuery{ast, reply}`, `RemoveQuery{id}`, `Advance{reply}` returning
  `Vec<PipelineRowChange>`. Connections send commands; the driver serializes
  them. This also serializes advances across the group (correct: one snapshot
  leapfrog per group).
- **Reference counting.** Queries are ref-counted across the group's clients
  (the CVR desired-queries layer already tracks this —
  `cvr_desired_queries.rs`, `cvr_ref_counts.rs`). `add_query` is called once
  when the first client desires a query; `remove_query` when the last drops it.
  The pipeline stays alive while any connection holds an `Rc<GroupPipeline>`;
  dropped when the group empties (`Weak` in the registry).
- **`DesiredQueriesHandler`** (`live_connection.rs:648`) loses its owned
  `pipeline_driver: Option<PipelineDriver>` and instead holds
  `group: Rc<GroupPipeline>` (or a command sender). Its `advance` call sites
  (`:2186`, `:2604`) become "ask the group for changes since my last version";
  poke building (`poke_builder.rs`) and CVR flush are unchanged — they still
  consume `PipelineRowChange`.

### 6.2 Advance fan-out to connections

One group advance produces one `Vec<PipelineRowChange>` covering all active
queries; each connection filters to the queries it has desired and builds its
poke from its own CVR base version (the per-connection cookie chaining at
`live_connection.rs:664`/`:668` is untouched). This is exactly the
"one owner, many readers" upstream shape and is the structural fix for root
cause #1 — N clients now share one hydrate, one replica, one snapshot pair.

---

## 7. Incremental migration plan

Constraint: every commit builds and stays conformance-green; the new engine
lands behind the existing one until it fully replaces it. `PipelineRowChange`
(the boundary type) never changes, so CVR/poke/wire layers are insulated
throughout.

Ordered increments (each is one PR):

1. **Traits + retained-source refactor** (Wave 0a).
2. **`SqliteSource` + `DatabaseStorage`** (Wave 0b).
3. **`build_pipeline` + Collector, hydration only** — new engine hydrates,
   old `advance` still runs, behind a flag.
4. **`advance` via push-through-graph for single-table + Filter** — replace
   the `is_direct_incremental_query` path first (lowest risk, already
   incremental today).
5. **Skip + Take** operators and their AST wiring.
6. **Join + join-utils + lazy relationships** — flip `related`/correlated
   queries off `materialize_query` onto the graph.
7. **Exists + FlippedJoin.**
8. **FanOut/FanIn** (only if the corpus needs OR).
9. **Client-group ownership** (Section 6) — can land in parallel with 5-8 once
   3-4 prove the shared-driver command channel.
10. **Delete `materialize_query` et al.** (Section 5.4); remove the flag.

### First 3 concrete increments (files touched)

**Increment 1 — Operator trait foundation.**
- `crates/zero-cache-zql/src/ivm/operator.rs`: add `InputBase`, `Input`,
  `Operator`, `ThrowOutput`; add `pusher` to `Output::push`; extend
  `SourceSchema` with `relationships` + `compare_rows`.
- `crates/zero-cache-zql/src/ivm/filter.rs`: make `Filter` an `Operator`
  (keep the edit-split logic; retarget `fetch` to `Rc<dyn Input>`; push to
  `output`).
- `crates/zero-cache-zql/src/ivm/join.rs`: update the `Output` impl on
  `SpyOutput` and the `output.push` call to the new 2-arg signature (keep the
  existing `reeval_*` helpers for now — they'll back Exists).
- `crates/zero-cache-zql/src/ivm/table_source.rs`: mark as test-only source;
  no signature change required yet.
- Tests: port `operator`/`filter`/`filter-operators` unit tests.
- Verify: `cargo test -p zero-cache-zql`; full suite green (nothing outside zql
  changed behavior).

**Increment 2 — Replica-backed source + operator storage.**
- `crates/zero-cache-sqlite/src/sqlite_source.rs` (new): `SqliteSource`
  (`Input` + `push` + `connect` + `set_db`), reusing
  `sqlite_table_source.rs`'s `build_select_query` read path.
- `crates/zero-cache-sqlite/src/database_storage.rs` (extend existing file):
  add the `storage` KV table, `create_client_group_storage`,
  per-op `Storage` impl (port `database-storage.ts`).
- `crates/zero-cache-sqlite/src/lib.rs`: export `SqliteSource`,
  `DatabaseStorage`.
- Tests: port `table-source.test.ts` (fetch/push/overlay-off subset) and
  `database-storage.test.ts`.
- Verify: `cargo test -p zero-cache-sqlite`; source reads match the in-memory
  `TableSource` on a shared fixture.

**Increment 3 — `build_pipeline` + Collector, hydration path.**
- `crates/zero-cache-zql/src/builder/pipeline.rs` (new): `build_pipeline`,
  `BuildDelegate` (single-table + `Filter` only in this increment).
- `crates/zero-cache-view-syncer/src/pipeline_driver.rs`: add a
  `graph: Option<Rc<dyn Input>>` + `Collector` per `Pipeline`; make
  `add_query` build the graph and hydrate via `fetch` **behind a
  `ZERO_IVM_GRAPH` flag**, falling back to `materialize_query` when off; add a
  `#getSource`-style memoized `tables: HashMap<String, Rc<SqliteSource>>`.
- `crates/zero-cache-view-syncer/src/lib.rs`: re-export as needed.
- Tests: extend `pipeline_driver.rs` `mod tests` to assert graph-hydration
  equals `materialize_query` output on the existing fixtures
  (`:743`, `:891`).
- Verify: `cargo test -p zero-cache-view-syncer`; `scripts/test.sh` green with
  the flag both on and off; `scripts/conformance.sh` green.

---

## 8. Verification

### 8.1 Correctness

- **Ported upstream operator tests** are the executable spec (test-first, per
  CLAUDE.md). Each operator PR ports its named `*.test.ts` (Section 4) into the
  Rust module's `mod tests`. These are exhaustive push/fetch enumerations
  (`take.push.test.ts` alone is 208 KB) and are the primary correctness gate.
- **Equivalence harness:** during migration, run each query through *both*
  `materialize_query` and the new graph on the same snapshot + same commit
  stream and assert identical `PipelineRowChange` sets (a temporary
  `debug_assert` behind the `ZERO_IVM_GRAPH` flag). This catches divergence
  before the old path is deleted.
- **`scripts/conformance.sh`** must stay green at every increment — it is the
  end-to-end protocol/differential check against official `rocicorp/zero`.
- **Live-PG e2e** (`crates/zero-cache-server/tests/feature_e2e.rs`,
  `scripts/test.sh --with-pg`) for the replication→pipeline→poke path.
- **Reset paths:** verify `SnapshotError::Reset` (schema/permissions/truncate,
  `snapshotter.rs:275`) still discards and rebuilds pipelines; add the
  runaway-push fallback (Section 5.3) as a test that forces abort and asserts a
  correct re-hydrate.

### 8.2 Performance (the gate)

- **`scripts/bench.sh 5000 60`** — the head-to-head fanout bench vs official
  Zero. Gate: sustained-clients within 10% and hydrate p50 within 10%.
- Run bench at these checkpoints, expecting monotonic improvement:
  - After **Increment 2+3** (shared replica-backed source, per-group driver
    not yet done): expect the hydrate-memory/CPU cliff from `SELECT *` × N to
    disappear even before graph-advance — biggest single win.
  - After **Increment 4-7** (graph advance replaces `materialize_query`):
    expect advance CPU to drop from O(table) to O(change) on join/limit/exists
    queries — the fanout p50 fix.
  - After **Increment 9** (client-group ownership): expect N-client hydrate to
    collapse to ~1 hydrate per group — the sustained-clients fix.
- **Micro-checks:** `pipeline-driver`'s `ivm.advance-time` histogram
  (`pipeline-driver.ts:282`) has a Rust analog worth emitting; assert
  advance-time is independent of table size on a large-table fixture (regression
  guard against any accidental full-scan creeping back in).
- Keep `.cargo/config.toml`'s `SQLITE_ENABLE_STMT_SCANSTATUS` (the cost model
  the planner — and `FlippedJoin`'s child-vs-parent choice — depends on).

### 8.3 Definition of done

`scripts/conformance.sh` green, `scripts/test.sh --with-pg` green,
`scripts/bench.sh 5000 60` within the 10% gate on both sustained-clients and
hydrate p50, `materialize_query` and friends deleted, and the `ZERO_IVM_GRAPH`
flag removed (graph is the only path).
```
