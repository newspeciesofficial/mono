# Sync-Worker Port Status

Bottom-up port of `packages/zero-cache/src/services/view-syncer/` and
`packages/zql/src/ivm/` into `crates/sync-worker/`. Shadow-FFI surface
exposed via `crates/shadow-ffi/src/exports/`.

## Acceptance criteria for every ported file

1. **1:1 file layout** — Rust file name mirrors TS file name. Module
   docstring names the TS origin path and the public functions ported.
2. **Every TS public export is ported** (even types). Types become Rust
   structs/enums; functions become free functions.
3. **Branch-complete unit tests.** Every conditional (`if`, `match`,
   early-return, `for` loop that's exited mid-way) has at least one test
   that exercises it. Comments above each test name the branch.
4. **Panics only for TS `assert`s / `unreachable`**, and only when TS
   would throw. FFI boundary catches panics and surfaces as `napi::Error`.
5. **Shadow wrapper in `shadow-ffi/src/exports/<ts_file>.rs`** exposes
   the same public API with `#[napi]`. Value types go through
   `serde_json::Value`; stateful types use `External<Arc<Mutex<T>>>`.
6. **TS diff test in `packages/zero-cache/src/shadow/<ts_file>.test.ts`**
   loads the `.node`, runs TS impl and Rust impl on identical inputs,
   asserts the outputs match via `diff()`. Runs even with `ZERO_SHADOW=0`
   (the test itself forces the diff).

## Completed

| File | TS LOC | Rust LOC | Tests | Shadow exposed? |
|---|---|---|---|---|
| `ivm/stream.rs` | 43 | 10 | — (type alias) | No — types only |
| `ivm/change.rs` | 65 | 120 | 8 | No — pure types, consumed by ported operators |
| `ivm/data.rs` | 124 | 280 | 30 | **Yes** — `ivm_data_compare_values`, `_opt`, `values_equal`, `normalize_undefined`, `compare_rows` |
| `ivm/constraint.rs` | 202 | 470 | 24 | Not yet (next step) |
| `ivm/schema.rs` | 25 | 207 | 3 | No — trait vocabulary, no value-level shadows |
| `ivm/source.rs` | 96 | 314 | 10 | No — traits + tagged enums, no value-level shadows |
| `ivm/operator.rs` | 117 | 409 | 14 | No — traits + data types, no value-level shadows |
| `ivm/memory_storage.rs` | 50 | 296 | 18 | No — in-memory Storage, not exposed cross-crate |
| `ivm/maybe_split_and_push_edit_change.rs` | 38 | 294 | 5 | No — internal helper, not a value-level surface |
| `ivm/push_accumulated.rs` | 438 | 1307 | 35 | No — internal fan-in helper, not a value-level surface |
| `ivm/filter_operators.rs` | 160 | 1091 | 21 | No — internal trait/adapter surface, not called from TS |
| `ivm/filter.rs` | 58 | 552 | 11 | No — internal `FilterOperator`, not called from TS |
| `ivm/filter_push.rs` | 36 | 429 | 15 | No — internal fetch-path helper, not called from TS |
| `ivm/skip.rs` | 157 | 1042 | 31 | No — internal stateful operator, not called from TS |
| `ivm/take.rs` | 400 | 2419 | 55 | No — internal stateful operator, not called from TS |
| `ivm/join_utils.rs` | 167 | 777 | 27 | No — internal helpers, consumed by Join/FlippedJoin |
| `ivm/join.rs` | 296 | 912 | 20 | No — internal stateful operator, not called from TS |
| `ivm/flipped_join.rs` | 491 | 1232 | 24 | No — internal stateful operator, not called from TS |
| `ivm/exists.rs` | 270 | 1518 | 33 | No — internal stateful FilterOperator, not called from TS |
| `ivm/fan_out.rs` | 82 | 611 | 16 | No — internal FilterOperator, not called from TS |
| `ivm/fan_in.rs` | 93 | 531 | 13 | No — internal FilterOperator, not called from TS |
| `ivm/union_fan_out.rs` | 56 | 450 | 12 | No — internal Operator, not called from TS |
| `ivm/union_fan_in.rs` | 292 | 1308 | 31 | No — internal Operator, not called from TS |
| `ivm/memory_source.rs` | 820 | 2138 | 46 | No — internal test-only Source; production Source is TableSource (Layer 8) |
| `zqlite/table_source.rs` | 686 | 2417 | 66 | No (Layer 8; shadowing deferred to integration phase) |
| `zqlite/database_storage.rs` | 187 | 998 | 21 | No (Layer 8; shadowing deferred to integration phase) |
| `query/escape_like.rs` | 3 | 60 | 6 | No — utility, consumed by future builder |
| `query/complete_ordering.rs` | 93 | 367 | 9 | No — utility, consumed by future builder |
| `planner/planner_constraint.rs` | 21 | 123 | 7 | No — internal planner type |
| `planner/planner_node.rs` | 70 | 382 | 3 | No — internal planner type |
| `planner/planner_source.rs` | 36 | 114 | 3 | No — internal planner factory |
| `planner/planner_terminus.rs` | 40 | 121 | 4 | No — internal planner node |
| `planner/planner_fan_out.rs` | 108 | 247 | 8 | No — internal planner node |
| `planner/planner_fan_in.rs` | 241 | 499 | 12 | No — internal planner node |
| `planner/planner_join.rs` | 463 | 567 | 16 | No — internal planner node |
| `planner/planner_connection.rs` | 345 | 560 | 12 | No — internal planner node |
| `planner/planner_debug.rs` | 536 | 345 | 4 | No — diagnostics; observable shape only (see module docs) |
| `planner/planner_graph.rs` | 471 | 590 | 7 | No — internal cost-search engine |
| `planner/planner_builder.rs` | 382 | 619 | 9 | No — internal AST→graph translation |
| `zqlite/sqlite_stat_fanout.rs` | 468 | 828 | 23 | No — internal SQLite statistics reader |
| `zqlite/sqlite_cost_model.rs` | 216 | 981 | 22 | No — internal planner cost estimator (uses EXPLAIN QUERY PLAN + sqlite_stat1 instead of scanstatus; `compile_inline` now routes through `sql_inline`; `build_select_query` kept separate by design) |
| `zqlite/internal/sql_inline.rs` | 82 | 386 | 22 | No — inline SQL literal formatter, consumed by cost model |
| `zqlite/query_builder.rs` | 278 | 1061 | 33 | No — `buildSelectQuery` port. Called from `table_source::fetch_all` via `build_select_sql` (deferred filter TODO now closed) |
| `builder/like.rs` | 71 | 387 | 15 | No — LIKE/ILIKE predicate compilation (char-by-char matcher, no regex dep) |
| `builder/filter.rs` | 204 | 966 | 37 | No — `createPredicate` + `transformFilters` port; feeds row-level predicates into `memory_source::connect` and `table_source::connect` |
| `builder/builder.rs` | 796 | 1167 | 18 | No — `buildPipeline` AST→IVM operator-graph translator. Full integration tests land with `pipeline_driver` (Layer 11). |
| `view_syncer/client_schema.rs` | 122 | 580 | 18 | No — internal validation; called once per ClientGroup from `pipeline_driver` (Layer 11). Inlines `ZERO_VERSION_COLUMN_NAME`, `appSchema`, `upstreamSchema`, and trims `LiteAndZqlSpec` / `LiteTableSpec` / `ClientSchema` to just the fields this function reads. |
| `view_syncer/snapshotter.rs` | 578 | 1722 | 30 | No — internal leapfrog SQLite snapshot pair; consumed by `pipeline_driver` (Layer 11). Replaces TS `BEGIN CONCURRENT` + `journal_mode=wal2` with plain `BEGIN` (DEFERRED) on WAL — the bundled SQLite rusqlite uses ships without the wal2 + begin-concurrent extension. |
| `view_syncer/pipeline_driver.rs` | 1063 | 1943 | 30 | **GOAL — Layer 11 complete.** PipelineDriver + `hydrate` free function + `RowChange` family + `Timer` trait + `InspectorDelegate` marker. ✅ `TableSource::set_db` plumbing now closed: `advance()` and `advance_without_diff()` call `swap_table_source_dbs` which opens a fresh `Connection` per registered source on the snapshotter's db_file. ✅ Scalar-subquery companion plumbing now closed (see `zqlite/resolve_scalar_subqueries.rs`). `MeasurePushOperator` passthrough deferred; `ConnectionCostModel` wiring stubbed (accepts `enable_planner` flag but routes to `None` until a cost-model factory is exposed through the snapshotter). |
| `zqlite/resolve_scalar_subqueries.rs` | 257 | 1032 | 25 | No — internal AST→AST rewrite, consumed by `pipeline_driver::add_query`. Covers `resolve_simple_scalar_subqueries`, `is_simple_subquery`, `extract_literal_equality_constraints`, plus the `ScalarValue` tri-state (TS `LiteralValue \| null \| undefined`). Wired into `pipeline_driver::add_query_inner`: executes scalar subqueries synchronously via sub-pipelines, emits companion rows as `add` RowChanges after the main hydration stream, retains live companion Inputs on the Pipeline, and (in a follow-up refinement path) trips a panic / `ResetPipelinesSignal` when a push through the companion changes the resolved value. |

**Total: 978 Rust unit tests passing (701 baseline + 45 Layer 8c + 125 Layer 9 + 18 Layer 10b + 30 Layer 10a + 27 Layer 11 + 7 Deferred-1 `set_db` + advance swap + 25 Deferred-2 scalar-subquery companions), 37 TS shadow tests passing (28 baseline + 9 new `pipeline-driver.test.ts`).**

## Record-and-replay shadow harness (in progress)

The existing `pipeline-driver.test.ts` (9 tests) hand-derives expected rows
rather than diffing against real TS output. A real record-and-replay harness
is being built incrementally:

**Shipped:**
- `crates/replay-ctx/` — new crate housing the per-IO-kind FIFO queues.
  No napi dependency, so `sync-worker` can depend on it without leaking
  FFI types. Extracted from `shadow-ffi/src/replay_ctx.rs` (which now
  re-exports it under the old path for backward compat). 15 unit tests
  (5 pre-existing + 10 new covering `consume_sqlite_read` /
  `consume_sqlite_write` / `is_active` / `empty_payload`).
- `packages/zero-cache/src/shadow/sqlite-recorder.ts` — wraps a
  `zqlite/src/db.ts::Database` so every `prepare/run/get/all/iterate` and
  `pragma` call appends a `SqliteReadEntry` / `SqliteWriteEntry` to the
  active `RecordCtx`. 9 unit tests cover run/get/all/iterate/pragma/exec
  plus passthrough + bigint coercion.

**Shipped (cont.):**
- 5 Rust SQLite chokepoints now branch on `replay_ctx::is_active()`:
  `table_source::{query_rows, exec_write}`, `database_storage::run_sql`,
  `snapshotter::query_rows`, `sqlite_cost_model::query_plan`,
  `sqlite_stat_fanout::query_rows`. Non-active path unchanged; active
  path consumes from the queue and converts JSON rows back to the
  chokepoint's row shape via `zqlite::internal::replay_adapter`.
- `pipeline_driver_create_replay_mode` FFI constructor + 5 `_replay`
  entrypoints (`_init_replay`, `_add_query_replay`, `_advance_replay`,
  `_advance_without_diff_replay`, `_reset_replay`) in
  `shadow-ffi/src/exports/pipeline_driver.rs`. Each deserializes a
  `ReplayPayload`, wraps the op in `replay_ctx::enter(ctx, || {...})`,
  and logs (via eprintln, to avoid a `tracing` dep) any leftover
  queue entries after the call. `pipeline_driver_create_replay_mode`
  flips a new `replay_mode: Mutex<bool>` flag on the Rust
  `PipelineDriver`; `swap_table_source_dbs` early-returns when this
  flag is set so replay-mode advances don't try to `Connection::open`
  the replica file. +2 lib tests covering both branches
  (`swap_table_source_dbs_noop_in_replay_mode` /
  `..._swaps_when_replay_mode_off`). TS `ShadowNative` type updated
  with the new surface.

**Remaining (not yet implemented) — blockers surfaced by this work:**
1. **SQL-string parity between Rust and TS.** Rust
   `zqlite::query_builder` emits differently-quoted / differently-ordered
   SELECTs than TS `build-select-query`. `consume_sqlite_read` matches
   on exact `(sql, params)` equality, so the first `TableSource::fetch`
   under a real TS-recorded trace surfaces `SqliteMismatch`. A
   SQL-normalization layer (either parse-based or canonicalisation at
   record+replay time) is the unlock.
2. **Non-chokepoint rusqlite calls in `Snapshotter::init`.**
   `conn.execute_batch("BEGIN")` and `conn.pragma_update("synchronous",
   "OFF")` bypass the chokepoint entirely, so in replay mode the init
   path still tries to open a real rusqlite connection on the replica
   file. Must be routed through the chokepoint (or opaque `BEGIN`/PRAGMA
   replay entries added to the queue vocabulary).
3. **Recorder coverage gap.** The TS PipelineDriver's Snapshotter + lazy
   TableSource factory open DB handles we don't construct explicitly —
   the sqlite-recorder only wraps DBs we hand it. Full coverage needs
   either module-level patching of `zqlite/src/db.ts::Database` or a
   recording factory threaded through the TS constructors.
4. **RowChange emission ordering.** Rust and TS can emit identical row
   *sets* in different orders around companion subqueries. `diff`
   compares arrays positionally; scenarios that exercise companions
   will require relaxed set-equality until Rust's emission order is
   aligned.

These blockers are documented inline in the new
`packages/zero-cache/src/shadow/pipeline-driver.test.ts`. Each planned
TS→Rust scenario (`hydrate_simple_select`, `hydrate_with_where`,
`hydrate_with_order_by_and_limit`, `advance_insert`, `advance_delete`,
`advance_update`, `remove_query_cleans_up`, `two_queries_isolation`) is
`test.skip`-ped with the specific blocker cited. Three positive
`_replay` FFI round-trip tests validate the new surface is live:
(a) `create_replay_mode` produces a usable handle,
(b) `add_query_replay` on an uninitialised driver surfaces a clean
`napi::Error` (no panic abort), and
(c) `init_replay` installs the `ReplayCtx` on the thread-local and
surfaces a Rust-side error when the trace doesn't match what Rust
asks for.

**Crate topology (Part 2 option (a), chosen):** `replay-ctx` is a new
dependency-free crate; both `sync-worker` (consumer) and `shadow-ffi`
(producer) depend on it. sync-worker stays pure-Rust with no napi
leakage.

## Shadow-FFI exposure

Every `#[napi]` export lives under `crates/shadow-ffi/src/exports/`.
Pure functions cross the boundary as `serde_json::Value`; per-ClientGroup
state uses the `External<Arc<Mutex<T>>>` opaque-handle pattern in
`handle.rs`. Every entrypoint wraps its body in
`std::panic::catch_unwind(AssertUnwindSafe(...))` and converts panics
to `napi::Error::from_reason` so Rust `assert!` calls do not abort the
Node process (framework-wide rule; see `exports/ivm_data.rs` docstring).

| File | Exported functions | State? |
|---|---|---|
| `exports/lsn.rs` | `lsn_to_big_int`, `lsn_from_big_int` | stateless |
| `exports/ivm_data.rs` | `ivm_data_compare_values`, `_opt`, `values_equal`, `normalize_undefined`, `compare_rows` | stateless |
| `exports/ivm_constraint.rs` | `ivm_constraint_matches_row`, `ivm_constraints_are_compatible`, `ivm_constraint_matches_primary_key`, `ivm_key_matches_primary_key`, `ivm_pull_simple_and_components`, `ivm_primary_key_constraint_from_filters` | stateless |
| `exports/table_source.rs` | `table_source_create(replicaPath, tableName, primaryKey, columns) → TableSourceHandle`, `table_source_create_in_memory(tableName, primaryKey, columns, createTableSql, seedSql[]) → TableSourceHandle` | **stateful** (`Handle<Arc<TableSource>>`) |
| `exports/pipeline_driver.rs` | `pipeline_driver_create(clientGroupId, replicaPath, appId, shardNum, defaultYieldEveryMs, enablePlanner) → PipelineDriverHandle`, `_init(handle, clientSchema, tableSpecs, fullTables)`, `_initialized`, `_replica_version`, `_current_version`, `_reset`, `_destroy`, `_add_query(handle, transformationHash, queryId, ast, timer?) → Change[]`, `_remove_query`, `_advance(handle, timer?) → {version, numChanges, changes}`, `_advance_without_diff → version`, `_queries → [{queryId, table, transformationHash}]`, `_total_hydration_time_ms`, `_register_table_source(handle, tableName, TableSourceHandle)` | **stateful** (`Handle<PipelineDriver>`) — first end-to-end exercise of the opaque-handle pattern |

FFI-shape decisions surfaced during this layer:

- **AST / ClientSchema / LiteAndZqlSpec** cross as `serde_json::Value`.
  `AST` already derives serde; `ClientSchema` / `LiteAndZqlSpec` /
  `LiteTableSpec` do not, so `pipeline_driver.rs` has private
  `…Json` wire-shape structs plus `deserialize_*` helpers that fold
  back to the sync-worker types. Column typing uses camelCase fields
  (`dataType`, `allPotentialPrimaryKeys`, `zqlSpec`, `primaryKey`).
- **RowChange** collapses to `{type: 'add' | 'remove' | 'edit' |
  'yield', queryID, table, rowKey, row}` to match TS
  `RowAdd | RowRemove | RowEdit`. `advance` and `add_query` both
  **eagerly collect** their stream into an array — streaming across FFI
  would require a second handle type with no matching TS ergonomic
  benefit (the TS wrapper diffs the array anyway).
- **Timer** arrives as a JSON `{elapsedLapMs?: number, totalElapsedMs?:
  number}`; the Rust side wraps it in a `FixedTimer` with those two
  values. No JS callback across the FFI for now.
- **Yield threshold** is a constant `f64` passed at construct-time. The
  TS surface is a `() => number`; since the diff harness never mutates
  it, a constant is sufficient and avoids an FFI callback.
- **Inspector delegate** defaults to `NoopInspectorDelegate`. The TS
  diff harness does not exercise inspector surface yet.

TS side:

- `packages/zero-cache/src/shadow/native.ts` declares the new
  `ShadowNative` signatures, plus the opaque handle types
  `PipelineDriverHandle` / `TableSourceHandle` (branded `readonly
  __brand: '…'` records) and the `ShadowRowChange` discriminated-union
  wire type.
- `packages/zero-cache/src/shadow/pipeline-driver.test.ts` drives the
  new surface with 9 scenarios — hydrate-only (simple / WHERE /
  ORDER+LIMIT), two-queries-isolation, remove-query, reset,
  totalHydrationTimeMs, destroy-idempotence, plus a
  construct+init+version smoke test. Expected rows are hand-derived
  from the seed and diffed via `diff()`. A TS-driver side-by-side
  comparison against the Rust driver is **deferred**: the Rust
  `Snapshotter` requires plain `journal_mode=WAL` while the TS
  `Snapshotter` `assert`s `journal_mode=wal2` (rusqlite's bundled
  SQLite ships without the wal2 extension). The same replica file
  cannot satisfy both at once.

## Integration tests

End-to-end composition tests for the PipelineDriver live in
`crates/sync-worker/tests/integration_pipeline_driver.rs`. They build a
real SQLite replica fixture, wire `Snapshotter` + `DatabaseStorage` +
`TableSource` + `PipelineDriver`, and assert the RowChange stream
matches expectations. Run with
`cargo test -p zero-cache-sync-worker --test integration_pipeline_driver`.

| Scenario | Status | Notes / missing prerequisite |
|---|---|---|
| `hydrate_simple_select` | pass | `SELECT * FROM users` emits a `RowAdd` per replica row. |
| `hydrate_with_where` | pass | `age > 25` filter compiles + applies. |
| `hydrate_with_order_by_and_limit` | pass | `ORDER BY age DESC LIMIT 2` returns the expected top-2. |
| `advance_propagates_insert` | pass | Swap-ordering fix: `swap_table_source_dbs()` now runs AFTER the push fan-out in `advance_inner` (TS pipeline-driver.ts L731-735), so `push_change(Add)` observes the prev snapshot and its `"Row already exists"` assertion holds. |
| `advance_propagates_delete` | pass | Same swap-ordering fix — `push_change(Remove)` observes prev snapshot during fan-out. |
| `advance_propagates_update` | pass | `push_change(Edit)` asserts on `old_row` presence; the UPDATE becomes a no-op under the in-memory source shadow, and the fan-out emits a `RowEdit` to the subscribed query. |
| `remove_query_cleans_up` | pass | Unblocked by the swap-ordering fix. |
| `multi_query_isolation` | pass | Unblocked by the swap-ordering fix plus a per-connection `filter_predicate` gate in `TableSource::push_change` (TS `genPush` → `filterPush`). Without the gate, push events leaked across queries because `fullyAppliedFilters=true` tells the builder to skip a standalone `Filter` operator. |
| `yield_cap_triggers_reset` | pass | `set_max_yields_per_advance(0)` + a timer whose `elapsed_lap()` is huge produces `PipelineDriverError::Reset` on the first change. |

**Integration test totals: 9 scenarios. 9 passing, 0 ignored.** All
advance-path scenarios now exercise the full stack (Add / Remove /
Edit, single- and multi-query). Hydration and advance coverage both
end-to-end green.

## Revised layer plan (post Layer 8a discovery)

The initial "Layer 9 = builder" was too coarse. Builder depends on an entire
**planner subsystem** (`zql/src/planner/*.ts`, ~2700 TS LOC across ~10 files)
that wasn't in the original plan. `sqlite-cost-model` also depends on the
planner + query-builder + sqlite-stat-fanout, so it's been reclassified
from Layer 8c to "blocked-on-planner".

Actual remaining sequence:

- **Layer 8b** — `zqlite/database_storage.rs` ✅ done.
- **Layer 8.5 (NEW)** — planner subsystem (~10 files, ~2700 LOC).
  Bottom-up within itself:
  `planner-constraint → planner-node → planner-source → planner-terminus →
  planner-fan-out → planner-fan-in → planner-join → planner-graph →
  planner-builder → planner-connection`. Plus utility deps from
  `query/`: `complete-ordering`, `expression`, `escape-like`.
- **Layer 8c** — `zqlite/sqlite-cost-model.rs` + `zqlite/sqlite-stat-fanout.rs`.
  Unblocked by Layer 8.5.
- **Layer 9** — `zql/builder/{builder,filter,like}.rs` +
  `zqlite/query-builder.rs` + `zqlite/internal/sql-inline.rs`. Unblocked
  by Layer 8.5.
- **Layer 10a** — `view-syncer/snapshotter.rs`.
- **Layer 10b** — `view-syncer/client-schema.rs`.
- **Layer 11** — `view-syncer/pipeline-driver.rs`. The goal.

Deferred items from earlier layers that Layer 9 must close:
- ✅ `zqlite/table_source.rs::build_select_sql` renders no WHERE clause yet
  (filter condition is dropped). **Closed** — now delegates to
  `zqlite::query_builder::build_select_query` which renders the full
  `filtersToSQL` tree. `transform_filters` first strips correlated
  subqueries to match TS `NoSubqueryCondition`.
- ⚠️ `TableSource::fetch_all` skips overlay / filter-predicate wiring.
  **Partially closed** — per-connection predicate is now compiled
  (`TsConnection.filter_predicate`) and stored for future overlay
  plumbing, but `fetch_all` still relies on SQL-side filtering alone.
  Full overlay wiring lands with `pipeline_driver` (Layer 11).
- ✅ `memory_source.rs::connect` leaves `filter_predicate = None`.
  **Closed** — `connect` now calls
  `builder::filter::transform_filters` + `create_predicate` and stores
  the compiled predicate on the `Connection`.
- ✅ `sqlite_cost_model::compile_inline` was a passthrough stub.
  **Closed** — delegates to `zqlite::internal::sql_inline::compile_inline`.
- ℹ️ `sqlite_cost_model::build_select_query` stays a separate cost-model-
  only renderer (inlined literals for EXPLAIN QUERY PLAN). By design it
  does **not** share code with `query_builder::build_select_query` because
  the latter produces `(sql, params)` with `?` placeholders — see module
  doc on `sqlite_cost_model.rs`.
- ✅ Closed: `TableSource::set_db` + `PipelineDriver::advance` connection
  swap (was Mandatory 1). `TableSource::set_db(conn)` replaces the
  underlying `rusqlite::Connection` behind the state `Mutex`;
  `PipelineDriver::advance` and `PipelineDriver::advance_without_diff`
  now invoke `swap_table_source_dbs` after the snapshotter leapfrog,
  opening a fresh connection per registered source via
  `Snapshotter::open_connection` (added as a public helper).
  `rusqlite::prepare_cached` is per-connection so dropping the old
  conn suffices to invalidate the cache. 32 new unit tests cover the
  swap on both TableSource (3) and PipelineDriver (3 branches).
- ✅ Closed: `advance_inner` swap-ordering bug (integration-test-surfaced).
  `PipelineDriver::advance_inner` previously called `swap_table_source_dbs()`
  at the *start* of the advance, before the push fan-out. TS
  (pipeline-driver.ts L731-735) performs the `setDB` swap *after* the
  push loop so that each `TableSource` observes the prev snapshot
  during `push_change` (its `"Row already exists"` / `"Row not found"`
  presence assertions depend on this). The call site has been moved
  to the end of the fan-out loop, matching TS. `advance_without_diff`
  still swaps immediately after the snapshotter leapfrog (no push
  loop, no reordering needed). Additionally, a per-connection
  `filter_predicate` gate was added to `TableSource::push_change`
  (mirroring TS `filterPush` in memory-source.js) so that pushes do
  not leak to connections whose WHERE clause excludes the row —
  required because `fullyAppliedFilters=true` tells the builder to
  skip a standalone `Filter` operator. The integration fixture also
  now seeds an in-memory shadow DB for the `TableSource` (plain
  `BEGIN` on WAL cannot hold a writable snapshot the way TS BEGIN
  CONCURRENT + WAL2 can; see `integration_pipeline_driver::make_users_source`
  module doc). 4 previously `#[ignore]`d scenarios now pass
  (`advance_propagates_insert`, `advance_propagates_delete`,
  `remove_query_cleans_up`, `multi_query_isolation`).
- ✅ Closed: scalar-subquery companion pipelines (was Mandatory 2).
  `zqlite/resolve_scalar_subqueries.rs` ports the full TS algorithm
  (branch-complete, 25 tests). `PipelineDriver::add_query` now runs the
  AST through `resolve_simple_scalar_subqueries` before `build_pipeline`,
  synchronously executes each simple scalar subquery via a sub-pipeline
  on the shared `DriverDelegate`, emits the resolved companion rows as
  `add` RowChanges after the main hydration stream, and retains the
  live companion Inputs on the Pipeline (destroyed by `remove_query` /
  `reset`). A `CompanionListenerOutput` wraps each companion input so
  any future push whose resolved value diverges from the captured value
  panics with a `ResetPipelinesSignal` marker — the advance-path
  boundary translates that into `PipelineDriverError::Reset`. The
  three-state return from the TS executor (`LiteralValue | null |
  undefined`) is modelled explicitly by a `ScalarValue` enum with
  `Value`, `Null`, and `Absent` variants.

## Remaining dependency tree (bottom-up order)

Each row is a separate porting unit. "Depends on" lists the rows that
must be completed first.

### Layer 0 — pure types / aliases (done)
- `ivm/stream.rs`
- `ivm/data.rs`
- `ivm/change.rs`
- `ivm/constraint.rs` (done but shadow not yet exposed — low priority since
  it's consumed only by `filter-push` and `source`, which aren't shadowed
  directly)

### Layer 1 — schema + source interfaces (done)
- `ivm/schema.rs`
- `ivm/source.rs`
- `ivm/operator.rs`

### Layer 2 — stream utilities
- **`ivm/stopable-iterator.rs`** (~23 TS LOC) — `StoppableIterator<T>` class.
- **`ivm/maybe-split-and-push-edit-change.rs`** (~38 TS LOC) — helper
  used by filter and exists operators.
  Deps: `change::EditChange`, `operator::{InputBase, Output}`, `data::Row`.
- **`ivm/push-accumulated.rs`** (~438 TS LOC) — accumulates push()
  changes until stream completion. Non-trivial state.
  Deps: `change::Change`, `data::Node`, `operator::{InputBase, Output}`,
  `schema::SourceSchema`, `stream::Stream`.

### Layer 3 — stateless operator (first real IVM logic)
- **`ivm/filter-operators.rs`** (~200 TS LOC) — evaluates `SimpleCondition`
  / `Conjunction` / `Disjunction` against a row → bool.
  Deps: `ast::{Condition, SimpleCondition, Conjunction, Disjunction}`,
  `data::{Row, Value}`, `ivm/data::values_equal`.
- **`ivm/filter.rs`** (~150 TS LOC) — `Filter` operator wraps a predicate
  function; push/fetch/cleanup delegate to upstream with filter applied.
  Deps: operator.rs, maybe-split-and-push-edit-change.rs, filter-operators.rs.
- **`ivm/filter-push.rs`** (~50 TS LOC) — `filterPush` helper splits
  EditChanges into add/remove when predicate flips.
  Deps: change, operator, maybe-split-and-push-edit-change.

### Layer 4 — storage + stateful operators
- **`ivm/memory-storage.rs`** (~50 TS LOC) — `MemoryStorage` backend
  implementing `Storage`. Uses a `BTreeMap<JSONValue, JSONValue>`.
  Deps: operator::Storage.
- **`ivm/skip.rs`** (~200 TS LOC) — `Skip` operator, keeps a count of
  rows skipped per parent. Stateful.
- **`ivm/take.rs`** (~400 TS LOC) — `Take` operator, bounded output.
  Complex boundary maintenance.

### Layer 5 — join family (hardest)
- **`ivm/join-utils.rs`** (~100 TS LOC) — shared helpers.
- **`ivm/join.rs`** (~400 TS LOC) — `Join` operator, inner join with
  state per parent.
- **`ivm/flipped-join.rs`** (~200 TS LOC) — left-side flipped variant.

### Layer 6 — exists + fan
- **`ivm/exists.rs`** (~500 TS LOC) — EXISTS subquery. Depends on join.
- **`ivm/fan-in.rs`, `ivm/fan-out.rs`, `ivm/union-fan-in.rs`, `ivm/union-fan-out.rs`**
  (~150 LOC each) — parallel pipeline plumbing.

### Layer 7 — memory source
- **`ivm/memory-source.rs`** (~300 TS LOC) — in-memory `Source` impl,
  only used for tests but PipelineDriver references the interface.

### Layer 8 — SQLite-backed source (outside `zql/src/ivm/`)
- **`zqlite/src/table-source.rs`** (~600 TS LOC) — `TableSource`, the
  real production source. Reads rows from a SQLite replica.
  Deps: SQLite pool, operator, source.
- **`zqlite/src/database-storage.rs`** (~400 TS LOC) — `ClientGroupStorage`
  — per-group operator storage backed by SQLite.
- **`zqlite/src/sqlite-cost-model.rs`** (~300 TS LOC) — query planner
  cost estimator.

### Layer 9 — builder
- **`zql/src/builder/builder.rs`** (~800 TS LOC) — `buildPipeline(ast, …)`
  turns an AST into an operator graph. Central entrypoint PipelineDriver
  calls.

### Layer 10 — snapshotter
- **`view-syncer/snapshotter.rs`** (~578 TS LOC) — leapfrog SQLite
  snapshot pair producing `SnapshotDiff`.
- **`view-syncer/client-schema.rs`** (~122 TS LOC) — tiny schema check.

### Layer 11 — pipeline-driver (the goal)
- **`view-syncer/pipeline-driver.rs`** (~1063 TS LOC) — owns IVM graphs
  per client group. Public API: `addQuery`, `removeQuery`, `advance`,
  `hydrate`.

## How to resume

1. Pick the lowest incomplete layer. Prefer completing all files in a
   layer before descending. Port each file in isolation — do not peek
   ahead.
2. For each file:
   a. Read the TS source end-to-end.
   b. Port every export into `crates/sync-worker/src/ivm/<name>.rs` (or
      the appropriate subdir). Include a module docstring citing the TS
      origin.
   c. Add branch-complete unit tests.
   d. Run `cargo test -p zero-cache-sync-worker --lib` — must be green.
   e. If the function is called from outside sync-worker scope (see
      "Used by (out of scope)" in the module inventory), add a
      shadow-FFI export under `crates/shadow-ffi/src/exports/` and a TS
      diff test in `packages/zero-cache/src/shadow/`.
   f. Run `cargo test --workspace --lib` and `npx vitest run src/shadow`
      from `packages/zero-cache`.
3. Update this file: move the row from Remaining → Completed with the
   test count.

## Workspace commands

```bash
cd packages/zero-cache-rs
cargo test -p zero-cache-sync-worker --lib

# Rebuild shadow binary after adding new #[napi] exports:
cd crates/shadow-ffi && npx napi build --platform --release --js index.js --dts index.d.ts

# TS shadow diff tests:
cd packages/zero-cache && npx vitest run src/shadow
```

## Invariants to respect

- **No cross-crate imports inside `sync-worker`.** Each ported TS file
  lives here; we do NOT reach into `replicator`/`streamer`/… (those
  crates are deleted). If a TS file imports from `db/` or `types/`, that
  one is fine — keep those narrow.
- **`#![deny(unsafe_code)]` stands.** napi-rs lives in `shadow-ffi`
  where unsafe is allowed. sync-worker stays pure safe Rust.
- **Branch coverage before moving on.** Skipping branches because
  "they're obvious" is how TS/Rust parity bugs creep in.
- **No state that outlives a single public call** unless the function
  is modelled as an `External<Arc<Mutex<T>>>` handle owned by TS.

## Replay wiring status

Every SQLite chokepoint now checks `zero_cache_replay_ctx::is_active()`
before touching its real rusqlite connection; when a `ReplayCtx` is
installed, the chokepoint pops from the matching FIFO queue and converts
the recorded `serde_json::Value` payload into its native row shape.
Param slices are serialised to JSON via
`zqlite::internal::replay_adapter::params_to_json`, which mirrors the
`jsonify` helper in `packages/zero-cache/src/shadow/sqlite-recorder.ts`.

| # | Chokepoint | File:method | Replay honours `is_active()`? | New tests |
|---|---|---|---|---|
| 1 | `TableSource::query_rows` | `zqlite/table_source.rs` | yes — happy path + SQL-mismatch error | 2 |
| 2 | `TableSource::exec_write` | `zqlite/table_source.rs` | yes — happy path + SQL-mismatch error; `last_insert_rowid` intentionally discarded (callers only read `changes`) | 2 |
| 3 | `DatabaseStorage::run_sql` | `zqlite/database_storage.rs` | yes — `RunMode::Read` consumes from `sqlite_read` queue; `RunMode::Write` and `RunMode::Raw` both consume from `sqlite_write` queue (TS recorder writes BEGIN/COMMIT/PRAGMA via `.run()`). Positional rows come from the recorded object's insertion order (better-sqlite3 + serde_json `preserve_order` preserve SELECT column order). | 4 |
| 4 | `Snapshot::query_rows` | `view_syncer/snapshotter.rs` | yes — projects recorded rows into the caller-supplied `column_names` order; JSON `null` round-trips to `Value::None`; mismatches surface as `SnapshotterError::Assertion("replay: …")` | 2 |
| 5 | `SQLiteCostModelInner::query_plan` | `zqlite/sqlite_cost_model.rs` | yes — recorded rows deserialise by object-key (`id`, `parent`, `detail`); `est` always 0.0 (EXPLAIN QUERY PLAN never exposed sqlite_stat); mismatches surface as `rusqlite::Error::SqliteFailure(SQLITE_MISUSE, "replay: …")` | 2 |
| 6 | `SQLiteStatFanout::query_rows` | `zqlite/sqlite_stat_fanout.rs` | yes — **compromise shipped**: the generic `F: FnMut(&rusqlite::Row)` signature became `F: FnMut(&PositionalRow)` because synthesising a `&rusqlite::Row` from recorded JSON is not possible via rusqlite's public API. `PositionalRow` mirrors the subset of `rusqlite::Row` the three callsites use (`get::<I, T>(idx)`, `get_ref(idx)`) so closure bodies compile unchanged. The real chokepoint is now `query_rows_positional` returning `Vec<Vec<SqlValue>>`. | 3 |

### Non-trivial shape conversions

- **Chokepoint 3** stores `Vec<Vec<SqlValue>>` (positional) but the TS
  recorder emits `{column: value}` objects. Because serde_json's `Map`
  preserves insertion order when the `preserve_order` feature is
  enabled — which the workspace now enables in `Cargo.toml` —
  `Map::values()` yields cells in SELECT-column order. This is the
  only workspace-level change outside the chokepoints.
- **Chokepoint 6** could not cleanly accept the original
  `F: FnMut(&rusqlite::Row) -> Result<T>` in replay mode. The
  documented compromise was the second option in the brief: replace
  the generic path with a `PositionalRow` façade. All three existing
  callsites kept their closure bodies; only the chokepoint signature
  moved.

### Shadow-FFI exposure (follow-up)

Not touched here. The `_replay` FFI variants and
`pipeline_driver_create_replay_mode` constructor remain out of scope
for this task.

**After wiring: 1001 lib tests passing (978 baseline + 8 replay_adapter
unit tests + 15 chokepoint-specific replay tests). Integration tests
unchanged (9 passing). `cargo test --workspace` green.**

## Shadow scenarios (end-to-end record-and-replay)

Attempted to un-skip the 8 macro-diff scenarios in
`packages/zero-cache/src/shadow/pipeline-driver.test.ts`. Progress + real
divergences surfaced below.

**Harness shape shipped:**

- `createFixture()` in `pipeline-driver.test.ts` mirrors the real TS
  `services/view-syncer/pipeline-driver.test.ts` `beforeEach` (wal2
  replica file, `_zero.replicationState` init, storage DB, fakeReplicator,
  InspectorDelegate). Minimal `users` schema instead of the
  issues/comments relational fixture so the scalar-subquery companion
  path is bypassed.
- `runSplitScenario(name, ast)` records TS `init()` under one
  `RecordCtx` and TS `addQuery()` under another. Each FFI `_replay`
  call receives the specific slice it will consume
  (`replay_ctx::enter()` consumes-on-drop, so the full trace cannot
  be reused across calls — each call needs its own queue-slice).

**Divergences fixed while attempting Scenario 1 (`hydrate_simple_select`):**

| # | Divergence | TS recorded | Rust asked for | Fix site |
|---|---|---|---|---|
| 1 | `Snapshot::from_replay` consumed PRAGMAs from the wrong queue | `sqlite_read` (via TS `patchedPragma` in `sqlite-recorder-install.ts`) | `sqlite_write` via `consume_sqlite_exec` | `crates/sync-worker/src/view_syncer/snapshotter.rs:252-282` — now consumes via `consume_sqlite_read` |
| 2 | `Snapshot::from_replay` expected `BEGIN`, TS recorded `BEGIN CONCURRENT` | `sqlite_write` sql=`BEGIN CONCURRENT` | `sqlite_write` sql=`BEGIN` (Rust uses plain `BEGIN` because bundled rusqlite lacks the begin-concurrent extension) | same file — now consumes `"BEGIN CONCURRENT"` even though Rust's real-mode code still issues `BEGIN` locally (replay consumer must match the TS record, not the Rust issuer) |
| 3 | TS `new Database(...)` ctor eagerly issues `PRAGMA page_size` (first read recorded), not accounted for on Rust side | First `sqlite_read`: `PRAGMA page_size` | First Rust consume: `PRAGMA synchronous = OFF` | same file — now pre-consumes `PRAGMA page_size` before the synchronous-off pragma |

Each fix flowed through to the existing `replay_mode_init_add_query_advance_no_real_db`
lib test, whose hand-crafted `snapshotter_replay_payload` was updated to
match the new consume order (4 entries per Snapshot::create: PRAGMA
page_size read, PRAGMA synchronous read, PRAGMA journal_mode read, BEGIN
CONCURRENT write, state-version read).

**Remaining blocker — Scenario 1 still skipped:**

After the three fixes above, `init_replay` goes green end-to-end (all
26 sqlite_read entries + 1 sqlite_write entry from TS's recorded init
trace survive Rust's init without a chokepoint mismatch). The next call,
`add_query_replay`, fails with **"Source not found"** because the Rust
`PipelineDriver::get_or_create_source` in replay mode returns `None`
when no `TableSource` has been pre-registered. In TS the driver
lazily builds a `TableSource` via `#getSource` on first access; the Rust
port relied on an explicit `register_table_source` seam (used by the
integration tests), with no replay-mode equivalent.

### Follow-up to unblock all 8 scenarios

1. **Replay-mode `TableSource` constructor.** Add
   `TableSource::new_replay_mode(name, cols, pk) -> TableSource` that
   builds a `TableSource` with no underlying `rusqlite::Connection`; all
   `query_rows` / `exec_write` calls go through the existing chokepoint
   already gated on `replay_ctx::is_active()`.
2. **`PipelineDriver::get_or_create_source` replay branch.** When
   `replay_mode` is set and no source is registered for a table, synthesise
   one via `TableSource::new_replay_mode` using the registered
   `table_specs` + `primary_keys` for that table.
3. **Re-un-skip Scenario 1.** Expect more divergences: the first
   `TableSource::fetch` SELECT's SQL string must match TS
   `build-select-query.ts` byte-for-byte (the 25 golden tests assert
   parity on the Rust side but the TS fixture may hit a shape not
   covered — add goldens as discovered).
4. **Advance scenarios (4-7).** Add `_advance_replay` equivalent of the
   Snapshotter replay-mode leapfrog. The existing advance_replay FFI
   works for the snapshotter chokepoint, but the advance's
   `push_change` fan-out currently opens a real Connection in
   `swap_table_source_dbs` — already gated on `replay_mode`, so this
   should be fine once (1) + (2) close.
5. **`two_queries_isolation`.** Requires two separate TableSources in
   replay mode + correct per-connection filter_predicate wiring.

### Harness limits worth documenting

- `enter()` in `crates/replay-ctx/src/lib.rs:218` consumes the installed
  `ReplayCtx` on exit. If the caller wants a single trace to span
  multiple FFI calls (e.g. init+addQuery), either (a) split the TS
  recording into one `RecordCtx` per call (what the harness does now),
  or (b) add the `install_replay`/`uninstall_replay` pair described in
  the original task brief — a `Arc<Mutex<Option<ReplayCtx>>>` on the
  PipelineDriver that every method checks before consulting the
  thread-local. Option (a) is simpler and matches the TS-side recording
  boundaries exactly; option (b) is needed only if the trace has to
  survive across `enter()` boundaries.

**Test totals after this work: 1029 lib (unchanged — 1 existing replay
test's hand-crafted payload updated in lockstep with the fix), 9
integration (unchanged), 19 replay-ctx (unchanged), 43 shadow TS (3
positive `_replay` FFI smoke tests + 40 pre-existing), 8 shadow TS
skipped (scenario 1 with the `TableSource` factory blocker documented;
scenarios 2-8 downstream of it).** `cargo test --workspace --lib` green;
TS `npx vitest run src/shadow` green.

## Shadow scenarios — follow-up session (all 8 now green)

**Blocker closed**: `TableSource::new_replay_mode(name, cols, pk) -> Arc<TableSource>`
at `crates/sync-worker/src/zqlite/table_source.rs:538` (~99 LOC
including inline unique-index lookup from the active ReplayCtx).
`TableSourceState::conn` is now `Option<Connection>`; `None` means
replay-mode. Every chokepoint (`query_rows`, `exec_write`,
`discover_unique_indexes`, `row_exists`) already short-circuits on
`zero_cache_replay_ctx::is_active()` before touching the connection;
the `Option` unwraps are guarded by `.expect(..)` with a message
naming the caller bug that would trigger them.

`PipelineDriver::get_or_create_source` in
`crates/sync-worker/src/view_syncer/pipeline_driver.rs:1082` now has a
`replay_mode = true` branch that synthesises a `TableSource` via
`new_replay_mode` from the `table_specs` + `primary_keys` captured at
`init`. First-access cache: the synthesised source is inserted into
`self.tables` so a second `get_or_create_source` call returns the
identical `Arc<TableSource>`.

**Divergences surfaced + fixed while un-skipping each scenario:**

| # | Site | TS recorded | Rust asked for | Fix |
|---|---|---|---|---|
| 1 | `TableSource::discover_unique_indexes` (`crates/sync-worker/src/zqlite/table_source.rs:1223`) | Single joined SELECT with `json_group_array(col.name) AS columnsJSON` | Two-call pattern (`pragma_index_list(?)` + `pragma_index_info(?)`) | Rewrite the replay branch to consume the single TS-shape SELECT. Caller updated: `discover_unique_indexes(conn: Option<&Connection>, ...)` so `new_replay_mode` can consume the entry from the active ReplayCtx. |
| 2 | `TableSource::query_rows` (`crates/sync-worker/src/zqlite/table_source.rs:874`) | TS `#fetch` always issues an `EXPLAIN QUERY PLAN` read before the main SELECT | Rust has no diagnostic EXPLAIN | In replay mode, peek the next `sqlite_read` entry and consume it if its SQL starts with `EXPLAIN QUERY PLAN `. Added `zero_cache_replay_ctx::peek_sqlite_read` (`crates/replay-ctx/src/lib.rs:331`). |
| 3 | `rustInitArgs` in the TS harness (`packages/zero-cache/src/shadow/pipeline-driver.test.ts`) | `zqlSpec` includes `_0_version` (from `computeZqlSpecs`) | Rust test passed a `zqlSpec` without `_0_version`, so the replay-mode TableSource's column list was short and the recorded SELECT's `_0_version` column did not line up | Added `_0_version: {type: 'string'}` to the test fixture's `tableSpecs.users.zqlSpec`. |
| 4 | `PipelineDriver::pipeline_driver_create_replay_mode` callsite in the TS test | `DatabaseStorage.createClientGroupStorage('cg-shadow')` keys the recorded `OpStorage` SELECT on `cg-shadow` | Rust was called with `cg-${name}` | Harness change: always pass `'cg-shadow'` as the client-group id to the Rust driver. |
| 5 | `DatabaseStorage::OpStorage::{get, set, del, scan}` SQL (`crates/sync-worker/src/zqlite/database_storage.rs:462 / 443 / 553 / 510`) | TS template-literal verbatim — leading `\n        ` + trailing `\n      ` + specific whitespace around `WHERE` and the continuation `ON CONFLICT(... op, key) \n        DO \n          UPDATE` | Rust emitted a reformatted single-indent version | Rewrote each SQL constant as a string literal with the TS bytes verbatim. |
| 6 | `Snapshot::changes_since` SQL (`crates/sync-worker/src/view_syncer/snapshotter.rs:405`) | Single-line SQL: `... FROM "_zero.changeLog2" WHERE ...` | Rust had a newline-wrapped version | Collapsed to a single line; updated the one lib test (`snapshotter_replay_payload`) that hard-coded the multi-line form. |
| 7 | `Snapshot::get_row` call shape | TS `stmt.get(Object.values(key))` records `[["u1"]]` (single array arg that better-sqlite3 would expand) | Rust passed `["u1"]` (flattened) | Fixed on the TS recorder side: `jsonifyParams` now unwraps a single-array argument in `packages/zero-cache/src/shadow/sqlite-recorder-install.ts` before serialising. Documents the better-sqlite3 variadic vs. array-arg equivalence. |
| 8 | `row_exists` SQL (`crates/sync-worker/src/zqlite/table_source.rs:1325`) | TS `checkExists`: `SELECT 1 AS "exists" FROM ... WHERE ... LIMIT 1` | Rust: `SELECT 1 FROM ...` | Added the `AS "exists"` alias to match the TS byte-exact recording. Updated the one lib test that hard-coded the pre-fix form. |
| 9 | `build_insert_sql` (`crates/sync-worker/src/zqlite/table_source.rs:1376`) | TS uses `, ` (comma+space) between column identifiers | Rust used `,` (no space) | Column list separator flipped to `", "`. Lib test assertion updated. |
| 10 | Advance harness | TS `fakeReplicator.processTransaction` was inside the recording ctx and leaked its own SQL (e.g. `_zero.column_metadata` schema probes) into the replay queue | N/A | Moved `mutate(fixture)` OUTSIDE the advance `RecordCtx` so only pipelines.advance() SQL lands in the queue. |

**Final scenario status:**

| Scenario | Status |
|---|---|
| `hydrate_simple_select` | pass |
| `hydrate_with_where` | pass |
| `hydrate_with_order_by_and_limit` | pass |
| `advance_insert` | pass |
| `advance_delete` | pass |
| `advance_update` | pass |
| `remove_query_cleans_up` | pass |
| `two_queries_isolation` | pass |

**Test totals after this session: 1034 sync-worker lib (1029 + 5 new:
3 `new_replay_mode` cases + 2 `get_or_create_source` replay-mode
branches), 9 integration (unchanged), 19 replay-ctx (unchanged), 51
shadow TS (3 FFI smoke + 8 scenarios now green + 40 pre-existing,
zero skipped).** `cargo test --workspace --lib` green; `cargo test -p
zero-cache-sync-worker --test integration_pipeline_driver` green; TS
`npx vitest run src/shadow` green.

### Remaining tail: init_replay leftovers

`pipeline_driver_init_replay` still logs `replay ctx non-empty after
call: [("sqlite_read", 22)]` after each scenario. These are TS-recorded
reads the Rust init path intentionally does not consume (e.g. the TS
`populateFromExistingTables` column-metadata probes and the recorder's
`initReplicationState` + `CREATE TABLE users …` preamble). They do not
block scenario correctness — the diff asserts only on the RowChange
stream — but a cleaner harness would scope `initCtx` to just
`pipelines.init(clientSchema)` so the queue enters init with only the
entries Rust will consume. Left as a follow-up.

## Per-handle queue ordering (closes cross-handle SQL divergence)

In production, the TS `PipelineDriver` opens **multiple** `Database`
instances concurrently — at minimum the storage DB (DatabaseStorage)
and the replica DB (TableSource / Snapshotter / planner). Each instance
issues its own SQLite traffic, in its own internal order. The previous
recorder merged every handle's reads into a single per-kind FIFO queue,
so `consume_sqlite_read` in Rust would pop entries in TS's interleaved
order — which differs from Rust's interleaved order because Rust's
chokepoint sequencing is not byte-for-byte identical to TS's. Real
xyne sessions surfaced divergences like:

```
Rust asked for:  SELECT val FROM storage WHERE cg=? AND op=? AND key=?
                 (DatabaseStorage handle)
TS recorded:     SELECT il.name as index_name ...
                 (TableSource handle, discoverUniqueIndexes)
```

Both valid in isolation; merged in one queue → cross-handle pop.

**Fix:** per-handle FIFO. Each recorded entry is tagged with a
monotonic `db_handle_id`. The `ReplayPayload` carries a `HandleMap`
identifying which TS handle backs which Rust chokepoint kind
(currently just `storage`). On the Rust side, `DatabaseStorage`
chokepoints consume with `HandleFilter::Match(storage_id)` (only
storage-tagged entries); replica-side chokepoints (`TableSource`,
`Snapshotter`, `sqlite_cost_model`, `sqlite_stat_fanout`) consume with
`HandleFilter::Exclude(storage_id)` (any entry except the storage DB's).

Replica-side **exclude** rather than positive match: TS opens many
short-lived replica-ish handles (one per `Snapshot.create`, one per
`TableSource`), each with its own monotonic id. Rust doesn't need to
distinguish between them — it just must not grab the storage DB's
entries. This avoids per-snapshot bookkeeping while still solving the
cross-handle ordering bug.

**Files changed (Rust):**
- `crates/replay-ctx/src/lib.rs` — added `db_handle_id: Option<i64>` to
  `SqliteReadEntry` / `SqliteWriteEntry`; added `HandleMap` (storage,
  replica) on `ReplayPayload` / `ReplayCtx`; added `HandleFilter::{Any,
  Match, Exclude}` and `*_with_filter` variants of `consume_sqlite_*`
  (legacy `_for_handle` and bare consume kept as wrappers for backward
  compat). `entry_handle_matches` accepts untagged legacy entries
  unconditionally so the 1034 lib tests' hand-crafted payloads keep
  working. New helpers `replica_filter()` /  `storage_filter()` derive
  the right `HandleFilter` from the active context's handle map. +3 new
  unit tests covering per-handle FIFO interleave, untagged-legacy
  matching, and mismatched-handle exhaustion. Total replay-ctx tests:
  22 (19 existing + 3 new).
- `crates/sync-worker/src/zqlite/table_source.rs` — `query_rows`,
  `exec_write`, `discover_unique_indexes`, `row_exists` consume via
  `replica_filter()`.
- `crates/sync-worker/src/zqlite/database_storage.rs` — every
  chokepoint (`run_sql`, `exec_ddl_with`, `exec_pragma_with`,
  `exec_ddl_locked`, init's `PRAGMA page_size`) consumes via
  `storage_filter()`.
- `crates/sync-worker/src/view_syncer/snapshotter.rs` — `from_replay`,
  `query_rows`, the `reset_to_head` ROLLBACK consume via
  `replica_filter()`.
- `crates/sync-worker/src/zqlite/sqlite_cost_model.rs` — `query_plan`
  + `table_row_count` consume via `replica_filter()`.
- `crates/sync-worker/src/zqlite/sqlite_stat_fanout.rs` —
  `query_rows_positional` consumes via `replica_filter()`.

**Files changed (TS):**
- `packages/zero-cache/src/shadow/sqlite-recorder-install.ts` — new
  `getDbHandleId(db)` (export); per-Database monotonic id stamped via
  a global counter on `globalThis` (survives vitest HMR re-imports).
  Every `prepare()` copies the DB's id onto the returned `Statement`,
  and every recorded `sqlite_read` / `sqlite_write` entry carries
  `db_handle_id`.
- `packages/zero-cache/src/shadow/record-ctx.ts` — new `HandleMap`
  type; `RecordCtx.handleMap` slot the wrapper fills in; `toReplay`
  embeds it under the `handle_map` field of the `ReplayPayload` (which
  Rust deserialises into `ReplayCtx.handle_map`).
- `packages/zero-cache/src/shadow/shadow-pipeline-driver.ts` — new
  optional `resolveHandleMap` callback in `ShadowInitArgs`; resolved
  after TS `init()` runs and attached to every subsequent `RecordCtx`.
- `packages/zero-cache/src/server/syncer.ts` — supplies
  `resolveHandleMap: () => ({storage: getDbHandleId(operatorStorage.shadowDatabaseHandle)})`
  so production traces carry the storage-handle id. Replica handles
  are not enumerated; replica-side Rust chokepoints use the exclusion
  filter.
- `packages/zqlite/src/database-storage.ts` — added the
  `shadowDatabaseHandle` getter (exposes the underlying Database for
  the shadow harness only; documented as opaque-to-callers).

**Backward compat:** payloads without `handle_map` (every existing lib
test, every untagged trace, every test that hand-crafts a
`ReplayPayload`) still work — `replica_filter` / `storage_filter`
default to `HandleFilter::Any` when no handle map is installed, which
is byte-for-byte the previous behaviour. Untagged entries
(`db_handle_id: None`) match every filter regardless. The 1034
sync-worker lib + 9 integration + 22 replay-ctx + 60 shadow TS tests
all stay green.

**Limitations / follow-ups:**
- The native `.node` was rebuilt only for `darwin-arm64`. The
  `linux-x64-gnu.node` and `linux-x64-musl.node` artefacts in
  `crates/shadow-ffi/` are pre-change; rebuilding requires Docker
  cross-compile, deferred.
- Tarball + container restart + xyne login session not run in this
  session (would require touching the user's running containers; per
  user instructions those need explicit permission).

