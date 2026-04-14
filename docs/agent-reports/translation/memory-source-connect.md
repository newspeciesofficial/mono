# Translation report: memory-source-connect

## Files ported / extended

- `packages/zql/src/ivm/memory-source.ts` → `packages/zero-cache-rs/crates/zql-ivm/src/memory_source.rs`
  (extended from ~686 LOC → ~1,700 LOC — adds the full `MemorySource` struct,
  `Source::connect` returning a `SourceInput`, sorted-index primary storage,
  and `push`/`gen_push` with per-connection fan-out via `filter_push_decide`).
- `packages/zql/src/ivm/take.ts` → `packages/zero-cache-rs/crates/zql-ivm/src/take.rs`
  (extended from ~334 LOC → ~1,100 LOC — adds the full `Take` operator struct
  that implements `Input + Output + Operator`, plus `push`, `push_edit_change`,
  `push_with_hidden`, `initial_fetch`, and `get_state_and_constraint`).

## What was completed

### MemorySource (Wave 4 completion)

Wave 1 delivered the foundational types (`Bound`, `Overlay`, `Overlays`,
`compute_overlays`, `generate_with_overlay_inner`, `generate_with_start`,
`generate_with_constraint`, `MemorySourceCore`, `with_overlay`). This wave
composes them into a real `Source`:

- `MemorySource` struct with `Arc<Mutex<MemorySourceState>>` for shared
  mutability (state: table schema, sorted indexes keyed by serialised
  `AstOrdering`, connection list, overlay, push-epoch).
- `SortedIndex` — a `Vec<Row>` kept sorted via binary search under the
  `make_bound_comparator` closure. Avoids Rust's `BTreeSet<T: Ord>`
  constraint (sibling `zql-ivm-joins-btreeset-custom-cmp`).
- `impl BuilderSource for Arc<MemorySource>` so callers from `builder.rs`
  can `get_source(...).connect(sort, where, split_edit_keys, debug)` and
  receive an `Arc<dyn SourceInput>`.
- `MemorySourceInput` implements `Input + BuilderSourceInput` with
  `get_schema` / `set_output` / `fetch` / `destroy` and
  `fully_applied_filters`.
- `fetch` selects the right index (constraint-first keys + requested
  sort), builds a `RowBound` scan-start with min/max sentinels for
  un-constrained columns, clones rows from the index, applies the
  overlay via `compute_overlays` + `generate_with_overlay_inner`, then
  `generate_with_start` + `generate_with_constraint` + the optional
  filter predicate. Mirrors the TS `#fetch` generator end-to-end.
- `push(SourceChange)` / `gen_push(SourceChange)` fan changes out to
  every attached connection under an advanced `push_epoch`. Uses
  `filter_push_decide` (from the Wave 1 filter port) to split/filter
  edits before dispatch, then commits to every index.
- Split-edit: respects `split_edit_keys` per connection — if any key
  changes, the edit is decomposed into remove + add (two epoch bumps).

### Take (Wave 4 completion)

Wave 1 delivered the state shape (`TakeState`, `get_take_state_key`,
`set_take_state`, `constraint_matches_partition_key`,
`make_partition_key_comparator`). This wave adds the operator:

- `Take` struct with `Arc<dyn Input>` upstream, interior-mutable
  `Box<dyn Storage>`, `limit`, optional `partition_key` +
  `partition_key_comparator`, schema, output, and
  `row_hidden_from_fetch` (A31 equivalent — cleared inside
  `push_with_hidden`).
- `impl InputBase + Input + Output + Operator` on `Take`.
- `fetch`: two paths (single take state when constraint matches PK,
  MAX_BOUND clip otherwise), honouring `row_hidden_from_fetch`.
- `push`: add / remove / edit / child with full TS semantics:
  - `add` under limit → grow bound + push.
  - `add` below bound at limit → locate bound node, emit remove + add
    using `push_with_hidden`.
  - `add` above bound at limit → no-op.
  - `remove` within bound → shift bound; emit remove + add(newBound)
    when new bound is further out, else just shrink size.
  - `child` inside bound → forward.
- `push_edit_change` handles all six partition combinations
  (`old_cmp`/`new_cmp` ∈ {Less, Equal, Greater}²) exactly as
  `#pushEditChange` in TS: `replace_bound_and_forward` closure,
  `push_with_hidden` overlay for remove-then-add pairs, and a
  special-case for `limit == 1`.

## Tests ported

- `memory-source.test.ts` — ported the load-bearing behavioural tests:
  - `schema` → `connect_exposes_schema_with_provided_sort`
  - push-edit-then-fetch → `push_edit_then_fetch_returns_new_row`
  - push/remove cycle → `push_add_then_remove_then_fetch_is_empty`
  - scan sort + reverse → `fetch_returns_rows_sorted_by_primary_key`,
    `fetch_reverse_returns_rows_in_reverse_order`
  - constraint filtering → `fetch_with_constraint_filters_rows`
  - start semantics → `fetch_with_start_at_skips_before`
  - connection destroy → `destroy_removes_connection`
  - pre-connect push → `push_with_no_connections_still_updates_storage`
  - splitEditKeys → `gen_push_split_edit_when_split_edit_keys_change`
- `take.push.test.ts` — ported a representative subset covering each
  push path:
  - limit 0 no output
  - less-than-limit add at start
  - at-limit add below bound (remove + add)
  - at-limit add above bound (no-op)
  - remove bound (triggers bound shift)
  - remove above bound (ignored)
  - edit inside bounds (forward)
- Pre-existing Wave 1 tests still pass unchanged.

### Tests **not** ported (deferred; flagged for follow-up)

The TS file set here totals ~10,350 LOC of tests
(`memory-source.test.ts` 512, `take.fetch.test.ts` 1503,
`take.push-child.test.ts` 269, `take.push.test.ts` 8068). The deferred
set is the exhaustive snapshot-style table-driven cases in
`take.push.test.ts` that depend on the TS `runPushTest` harness +
`fetch-and-push-tests.ts` infra — those lean heavily on
`toMatchInlineSnapshot` and `Symbol(rc)` node-reference counting from
the `Catch` helper, which are out of scope for a mechanical Rust port.
The behavioural invariants those snapshots pin down (add-below-bound,
edit-crossing-bound, remove-at-bound) are covered in smaller form in
the integration tests listed above. Partition-key-bearing Take cases
are not exercised here; adding them is mechanical but expands the test
count by ~4×.

## Patterns applied

- **A1** class → struct + impl: `MemorySource`, `MemorySourceState`,
  `MemorySourceInput`, `SortedIndex`, `ConnectionState`, `Take`.
- **A5** TS `interface` → Rust `trait`: `MemorySourceInput` implements
  `Input`, `BuilderSourceInput`; `Take` implements `Input`, `Output`,
  `Operator`.
- **A6** tagged-union `switch` → exhaustive `match`: every
  `SourceChange` / `Change` dispatch (add / remove / edit / child).
- **A13/A19** struct update / `Object.fromEntries` → explicit
  `Constraint::new()` + `insert()` in `get_state_and_constraint`.
- **A22** generator → `Vec<Yielded<T>>` materialised streams per Wave 1
  deviation — no `genawaiter`.
- **A30/D1** `Yielded<T>` used throughout as the cooperative-yield
  sentinel.
- **A31 / scopeguard-style reset**: `push_with_hidden` sets and clears
  `row_hidden_from_fetch` on scope exit (the TS `try { yield* … } finally`
  translates to a manual before/after wrap — Rust `Drop` would also work
  here but is unnecessary since the closure does not panic).
- **A33** `StorageKey` enum (MaxBound / PerPartition) for the typed-key
  overload pattern.
- **A35** `Clone::clone` on `serde_json::Value` used when materialising
  rows into secondary indexes and overlays.
- **D9** `Bound { Min, Value(V), Max }` — reused from Wave 1 to build
  `RowBound` scan-start with per-column sentinels.
- **D10** overlay/epoch mechanism — reused from Wave 1 inside
  `MemorySource::gen_push_and_write` and `do_fetch`.
- **D11** `RelationshipThunk` — unchanged; not load-bearing in this slice
  since MemorySource produces leaf nodes with empty relationships.
- **F1** `thiserror` — the existing `BuildError` already covers the
  builder side; no new error variants added here.

## Deviations

1. **Materialised iterators.** Per Wave 1 deviation 1 (see
   `docs/agent-reports/translation/zql-ivm-ops.md`), `fetch`/`push`
   return `Vec<Yielded<T>>` rather than lazy iterators. The `Yielded::Yield`
   sentinel semantics are preserved but currently no operator in the
   port emits yields (there is no tokio runtime driving a yield point).
2. **`has_output` flag.** TS `genPush` does `if (output) …` to skip
   connections without a downstream output. In Rust, the default output
   is `ThrowOutput` (panics). We track a boolean `has_output` on each
   connection (and on `Take`) that flips to `true` in `set_output` so
   pre-wire pushes don't explode.
3. **Sorted-vec primary index, not BTreeSet.** The sibling pattern
   `zql-ivm-joins-btreeset-custom-cmp` rejected `std::collections::BTreeSet`
   because it needs a `T: Ord` type. We use a `Vec<Row>` + binary search
   under the comparator closure. Same O(log n) lookup / O(n) insert cost
   as the TS `BTreeSet` clone the TS code uses, same iteration order.
4. **No debug delegate propagation.** The `debug: Option<Arc<dyn
   DebugDelegate>>` argument to `connect` is stored on the connection
   but not consulted yet — it is not used inside `fetch`/`push` in the
   Rust port.
5. **`NullInputBase` for the `pusher` argument.** TS passes the
   SourceInput itself as `pusher` to downstream `Output::push`. In Rust
   we pass a zero-sized `NullInputBase` stub because no current Rust
   operator reads the pusher identity (UnionFanIn is the only one that
   would, and the current port accepts any `&dyn InputBase`). If UnionFanIn
   integration becomes necessary here, we'll need to route the
   connection's actual input pointer through — straightforward but
   mechanical follow-up.

## Verification

- `cargo build -p zero-cache-zql-ivm`: ok (0 errors, 0 warnings)
- `cargo test -p zero-cache-zql-ivm`: **178 passed / 0 failed / 0 ignored**
  (was 159; +19 new: 11 MemorySource integration tests, 8 Take push
  integration tests).
- `docs/ts-vs-rs-comparison.md` row 14 updated (status remains
  **Port partial** because `build_pipeline` still doesn't wire
  skip-as-input / take-as-input adapters into the builder's AST walker
  — Take and MemorySource are now both callable, just not auto-composed
  in `build_pipeline`).

## Next-module dependencies unblocked

- **`build_pipeline` skip/take/source adapters.** `MemorySource` now
  has a working `Arc<MemorySource>: Source` impl and `Take::new` wires
  an `Arc<dyn Input>` upstream. A follow-up pass through `builder.rs`
  can replace the `BuildError::Deferred(...)` branches in
  `make_take_operator` / `make_skip_operator` / source dispatch with
  actual constructions.
- **`pipeline_driver.rs`** can now construct a real operator graph for
  single-table queries, and can exercise push semantics end-to-end via
  `MemorySource::push` → connection outputs.
- **Phase 2 hydration smoke.** With Take + MemorySource operational the
  zqlite replica + WHERE + LIMIT + ORDER BY pipeline is unblocked for
  happy-path queries (no joins yet — those still go through the existing
  `Join` / `FlippedJoin` / `Exists` operators, all of which were already
  complete in Wave 2).
