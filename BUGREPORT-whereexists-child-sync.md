# Bug report: `whereExists` correlated-subquery child rows are filtered server-side but never synced to the client

**Component:** query serving / hydration (`zero-cache-server`, `zero-cache-sqlite`)
**Severity:** high — silently returns empty/`null` results for correct data
**Status confirmed against:** current working tree (uncommitted serving path)

---

## Summary

When the [Hunting Game](https://gitlab.jasperaelvoet.be) mobile app (stock `@rocicorp/zero` v1.7.0, WebSocket) is pointed at this Rust `zero-cache` port instead of the official one, it opens **stuck on the game lobby/invite screen** and throws `The action 'GO_BACK' was not handled by any navigator` when the user tries to leave. **Nothing errors at the sync layer** — which is exactly the tell: this is a *data-correctness* defect, not a crash.

Root cause: the port compiles a query's `whereExists(...)` / `exists(...)` conditions into a **server-side SQL `[NOT] EXISTS(...)` filter** on the parent, but it **never syncs the correlated-subquery child rows** to the client. Because a Zero client re-runs the query's pipeline *locally* over the rows it received, it re-evaluates `whereExists(...)` against **zero** child rows, the `exists` is false, and it **drops the parent row** — so a query that matches real data in Postgres resolves to empty/`null` on the device.

The precise failure condition is: **a top-level `whereExists` whose child rows are not also synced via a matching `.related(...)` on the same relation.** Queries that happen to `.related(...)` the same relation survive; queries that don't, collapse.

---

## Background: why the client needs the `exists` child rows

Zero is IVM on both ends. The server hydrates a query and streams rows to the client, and the **client re-executes the same query pipeline** (filters, `exists`, `related`, `orderBy`, `limit`) over its local replica to produce the reactive result. This is why the client only ever "sees" rows the server actually synced.

An `exists`/`whereExists` operator is part of that pipeline. In upstream (real) zero-cache, the `EXISTS` join is an IVM operator, so **every row that flows through it — including the correlated child rows — is part of the query's synced row set.** The client therefore holds those child rows and can re-evaluate the `exists` and reach the same answer as the server.

This port instead pushes `exists` down into SQL as a pure parent-row *filter*. The parent is matched correctly on the server, but the child rows that justified the match are never sent. The client's local re-evaluation then disagrees with the server and discards the parent.

---

## The defect in code

### 1. `whereExists` compiles to a filter-only SQL `EXISTS`

`crates/zero-cache-sqlite/src/query_builder.rs`

`filters_to_sql_with_outer` routes a correlated subquery straight to `exists_to_sql` (lines 344-346):

```rust
Condition::CorrelatedSubquery { related, op, .. } => {
    exists_to_sql(related, *op, outer_table)
}
```

`exists_to_sql` (lines 364-408) emits `[NOT] EXISTS (SELECT 1 FROM child WHERE <correlation> [AND <subquery where>])`. Its own doc comment (lines 356-363) states this is *"the SQL-pushdown equivalent of upstream's IVM `EXISTS` join."* It is equivalent **for filtering the parent**, but it produces **no rows** — the child is `SELECT 1`, discarded after the boolean test.

### 2. Hydration only syncs `ast.related`, never `where_`'s `exists` children

`crates/zero-cache-server/src/live_connection.rs` — `hydrate_put` (lines 1325-1436)

The query's `where_` (which contains the `CorrelatedSubquery` nodes) is passed to the SQL fetch **purely as a filter** — the comment at lines 1367-1370 is explicit:

```rust
// A client or already-transformed custom query's real `where_`
// condition — pushed all the way into SQL via `fetch_filtered`,
// not evaluated in memory.
let where_ = transformed_ast.and_then(|ast| ast.where_.as_ref());
```

Child-row hydration then walks **`ast.related` exclusively** (lines 1400-1416):

```rust
if let Some(ast) = transformed_ast {
    if let Some(related) = &ast.related {
        if let Ok(related_result) = hydrate_related_rows_recursive(
            &self.db, &mut self.cvr_handler.cvr, orig_version,
            &p.hash, &result.row_bodies, related,
        ) { /* extend result with related rows */ }
    }
}
```

`hydrate_related_rows_recursive` (lines 352-...) is exactly the machinery we need — for each `CorrelatedSubquery` it hydrates the matching child rows (correlation + subquery `where_`), recurses into nested `related`, and adds them to the synced set. **But it is only ever called with `ast.related`.** The correlated subqueries living in `ast.where_` are never handed to it, so their child rows are never emitted.

Result: parent filtered correctly on the server → parent row sent → **no `exists` child rows sent** → client re-evaluates `whereExists` with an empty child set → parent dropped → query empty.

---

## Affected app queries

All from `packages/zero/queries.ts` in the app repo. The distinguishing factor is whether the `whereExists` relation is *also* pulled via `.related(...)`.

| Query | Line | Shape | Client result |
|---|---|---|---|
| `getCurrentGameSettings` | 181 | `zql.game.whereExists("players", …)` — **no** `.related("players")` | **Empty** — client has the game row, zero player rows → `whereExists` false → game dropped → `null` |
| `getGameMessages` | 187 | `zql.message.whereExists("game", …)` + nested `exists`; `.related("fromPlayer")` only | **Empty** — no `game` row synced → top-level `whereExists("game")` false → all messages dropped |
| `getLocation` | 228 | `zql.playerLocation.whereExists("player", …)` + audience `exists`; no `.related("player")` | **Empty** — no `player` row synced → dropped |
| `getCurrentGameXPEvents` | 144 | `zql.progressionEvent.where(…).whereExists("game", g => g.whereExists("players", …))` | **Empty** — no `game`/`players` synced → dropped |
| `getGameState` | 253 | `zql.game.whereExists("players", …)` **with** `.related("players", …)` | **Mostly OK at top level** — the related `players` rows are synced, so the client can re-evaluate `whereExists("players")`. But every nested `exists(...)` inside the players/location filters (roles, H&S visibility) only re-evaluates correctly to the extent those rows also happen to be synced via `related`; any that aren't will mis-filter roster/location visibility. |
| `getPlayerState` | 170 | plain `.where(…)`, **no** `exists` | OK — unaffected |
| `getEndScreenSummary` | 153 | `.where(…)` + `.related("game", …)` (no top-level `exists`) | OK — unaffected |

So on launch the app routes into the lobby *correctly* (`getPlayerState` + `getGameState` both resolve), but the lobby's supporting state (`getCurrentGameSettings`, `getGameMessages`, `getLocation`) comes back empty — the game is unplayable and the UI reads as "stuck." (The navigation redbox is a separate app-side fragility reacting to that empty/partial state; see the last section.)

---

## Secondary defect: a query with a missing/failed transform hangs forever (never `complete`)

`crates/zero-cache-server/src/live_connection.rs`

Custom-query ASTs are fetched from `ZERO_QUERY_URL`. On failure the fetch **only logs and continues** — `fetch_missing_custom_query_transforms_for_patch` (lines 813-841):

```rust
Err(e) => crate::warn!("custom-query transform for '{name}' FAILED: {e}"),
```

Then in `hydrate_put`, if the transform never registered and the hash isn't in the demo `catalog`, there is **no plan**, and the function returns with no row patches (lines 1347-1349):

```rust
let Some(plan) = ast_plan.or(catalog_plan) else {
    return patches;
};
```

The query contributes no rows and — combined with the GOT-queries routing in `poke_builder.rs` — does not transition to `complete` on the client, so it stays `pending`/`unknown` **indefinitely** (this is what makes the invite screen's friends list spin forever: `isLoading = friendsStateResult.type === "unknown"`).

Aggravating factors:
- Transform fetches are awaited **sequentially** (loop at lines 817-840), so one slow/blocked app-query server stalls the whole init.
- `ZERO_QUERY_FORWARD_COOKIES` is enabled — a forwarded session cookie that fails to authenticate against the app query server (`api-dev.hunting-game.com`) breaks **every** query at once, and only as `warn!`.

**Suggested fix:** on transform failure, emit a terminal error / mark the query so the client stops waiting instead of hanging; fetch transforms concurrently; and surface transform-auth failures as a connection-level error rather than a silent `warn!`.

---

## Tertiary notes

- **Full re-hydration instead of IVM deltas.** `rehydrate_tracked` (lines 1246-...) re-runs full SQL hydration for every desired query on each upstream commit rather than emitting true IVM deltas. Correctness-wise it mostly works via CVR diffing, but combined with the missing `exists` child rows, any visibility filter that depends on those rows computes wrong membership/location results.
- **Panics recently converted to no-ops** on the serving path (`cvr_query_driven_updater.rs` `track_executed`, `poke_builder.rs` GOT-patch routing) are masking real stability edges — e.g. overlapping `initConnection` + `changeDesiredQueries` across the ~20 queries this app registers at launch. Worth revisiting once the sync-correctness issue above is fixed, since a dropped worker thread closes the connection (and the app treats a closed connection as "ready" and renders stale local data).

---

## Recommended fix (primary defect)

Sync the correlated-subquery child rows, not just `related` rows:

1. In `hydrate_put`, after hydrating the root rows, **walk the root `where_` (recursively through `And`/`Or`) to collect every `CorrelatedSubquery` node**, and hydrate their matching child rows using the existing `hydrate_related_rows_recursive` machinery (correlation filter + subquery `where_`, recursing into nested `exists` and nested `related`). Add those rows to the synced set / CVR just as `related` rows are added at lines 1411-1413.
2. Apply the same `exists`-extraction **inside each `related` subquery's own `where_`** (e.g. `getGameState`'s players/location filters embed `exists(...)`), so nested visibility rules re-evaluate correctly on the client.
3. Keep these child rows tracked so `rehydrate_tracked` re-syncs them when they change.

Net effect: the client holds every row its local pipeline needs to re-evaluate `exists`/`whereExists`, matching upstream IVM semantics. The clean long-term version is real IVM-backed serving rather than SQL-`EXISTS` pushdown, but syncing the `exists` child rows is the minimal correctness fix.

---

## How to reproduce / verify

1. Seed Postgres with a real active game: a `game` row + a `player` row for the current user with `hasLeft IS NULL` (exactly what `whereExists("players", …)` matches).
2. Subscribe a client to `getCurrentGameSettings` (top-level `whereExists("players")`, no `related`).
3. **Observe:** the server's SQL matches and would return the `game` row, but the client-side query resolves to `null`/empty — because no `player` child rows were synced for the client to re-evaluate the `exists`.
4. Contrast with `getGameState` (same `whereExists` but `.related("players")`) — that one resolves, proving the differentiator is whether the child rows are synced.
5. For the hang path: point `ZERO_QUERY_URL` at an endpoint that 401s the forwarded cookie and confirm the affected queries stay at `type: "unknown"` on the client indefinitely (only a `warn!` server-side).

Suggested regression test under `conformance/`: a `whereExists` parent must survive the client's local pipeline re-evaluation (i.e. the `exists` child rows must be present in the synced row set), covering both the no-`related` and nested-`exists` cases.

---

## Appendix: app-side manifestation (context only — not a bug in this repo)

For completeness, the reason the empty/partial data surfaces as a redbox rather than an empty screen: the app renders against its local op-sqlite replica, treats a `closed` sync connection as "ready", and drives navigation reactively from these query results — `router.replace` into a rootless lobby, then `Done → router.back()` with no screen behind it → `GO_BACK was not handled`. That fragility is being addressed (or not) on the app side separately; it is only listed here to close the loop on the observed symptom.
