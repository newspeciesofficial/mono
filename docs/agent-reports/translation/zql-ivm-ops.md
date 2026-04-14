# Translation report: zql-ivm-ops

## Files ported

| TS source (packages/zql/src/ivm/) | LOC (TS) | Rust target (crates/zql-ivm/src/) | LOC (RS, incl. tests) |
|---|---|---|---|
| `stream.ts` | 43 | `stream.rs` | 153 |
| `change.ts` | 65 | `change.rs` | 71 |
| `data.ts` | 124 | `data.rs` | 258 |
| `schema.ts` | 25 | `schema.rs` | 52 |
| `constraint.ts` | 202 | `constraint.rs` | 145 |
| `operator.ts` | 117 | `operator.rs` | 158 |
| `filter.ts` + `filter-operators.ts` + `filter-push.ts` | 57+160+36 | `filter.rs` | 282 |
| `skip.ts` | 157 | `skip.rs` | 355 |
| `take.ts` | 766 | `take.rs` | 333 (partial: state + key + commit-after-loop; push branching deferred) |
| `memory-source.ts` | 820 | `memory_source.rs` | 685 (partial: D9 Bound + D10 overlay epoch core + pure helpers; BTree-backed connect+fetch deferred) |
| `push-accumulated.ts` | 438 | `push_accumulated.rs` | 306 (partial: fan-in flow control; relationship merging thunks deferred) |

New crate: `packages/zero-cache-rs/crates/zql-ivm` (`zero-cache-zql-ivm`).
Added to workspace `members` in `packages/zero-cache-rs/Cargo.toml`.
Total Rust LOC: **2826** (incl. tests), against **3106** TS in scope.

## Patterns applied

From the translation guide:

- **A1** class with private fields → `struct` + `impl` (every operator).
- **A5** `interface` → `trait` (`Storage` in operator.rs).
- **A6** tagged-union `switch` → `enum` + `match` (`Change`, `SourceChange`, `StartBasis`, `ChangeType`).
- **A13** struct update → struct-update syntax in edit-split.
- **A15** `try/finally` → `scopeguard::guard` (see `memory_source::with_overlay`).
- **A18** closure capturing mutable outer var → `move` + local `bool` flag in `skip::fetch_transform`.
- **A19** dynamic keys → `BTreeMap<String, …>` for `Constraint` / `RowBound`.
- **A22** `Symbol.iterator` → `impl Iterator`.
- **A23** `#private` → module-private fields.
- **A24** newtype for nominal uniqueness → reuse existing `PrimaryKey` from `zero-cache-types`.
- **A30** cooperative-yield sentinel → `enum Yielded<T> { Value(T), Yield }` over `Iterator` (per **Phase 1 decision D1**). Defined once in `stream.rs`, reused everywhere.
- **A31** scope guard on generator state → `scopeguard::guard(core.clone(), |c| { … clear overlay … })` in `memory_source::with_overlay`; clears on panic as well as on normal exit (proven by `with_overlay_clears_on_panic` test).
- **A33** split storage trait into named methods → `enum StorageKey { MaxBound, PerPartition(String) }` + a single `Storage::set/get/del/scan` keyed on that enum (replaces TS overloaded `get(MAX_BOUND_KEY): Row`).
- **A34** dynamic-method-name dispatch → deferred to btree-backed connect path (not yet ported); will be a plain `if reverse { values_from_reversed() } else { values_from() }`.
- **A35** `structuredClone` → `Clone::clone` on `serde_json::Value` (see `take::get_take_state_key`).
- **A36** commit-state-after-loop → `take::set_take_state` is called AFTER the TakeState-building loop rather than in a `finally`. The `try/catch/finally(commit)` TS idiom is replaced by "loop; then commit; panic unwinds skip commit" (the port doesn't yet wire the full initial-fetch loop because that requires the deferred btree connect path).
- **D9** `enum Bound { Min, Value(V), Max }` with hand-written `Ord` via `compare_bounds` — replaces TS `Symbol('min-value') / Symbol('max-value')`.
- **D10** Epoch-versioned overlay → `Overlay { epoch, change }`, `Connection.last_pushed_epoch`, `MemorySourceCore.push_epoch`, `with_overlay` scope guard that clears on exit (panic-safe). Pure helpers (`overlays_for_start_at`, `overlays_for_constraint`, `overlays_for_filter_predicate`, `compute_overlays`, `generate_with_overlay_inner`, `generate_with_start`, `generate_with_constraint`) all ported and tested.
- **D11** node-relationship as lazy thunk → `RelationshipThunk = Arc<dyn Fn() -> Box<dyn Iterator<Item = Yielded<Node>> + Send> + Send + Sync>`. Lives on `Node.relationships: IndexMap<String, RelationshipThunk>`. Exercised by `push_accumulated::merge_empty` tests.
- **C14 row** — `compare-utf8` not needed; `str::cmp` sufficient (see `data::compare_values`).

From Phase 1 decision D1: yield representation is `enum Yielded<T>`. Not using `genawaiter` or `async_stream`. Applied rigorously.

## Deviations

1. **Operator trait boxing vs per-operator typed APIs.** The TS `Input` /
   `Output` / `Operator` interfaces are tightly coupled to generator-returning
   methods with exterior mutability (every operator stores `#output = … ;
   throws on push before set`). Porting those 1:1 to object-safe Rust traits
   would force `Box<dyn Iterator<Item=Yielded<Node>>+'a>` with lifetimes that
   can't be satisfied by typical operator state (stored `Row`s clone-on-read).
   **Deviation:** each operator exposes typed `fn`s directly (`Filter::predicate`,
   `Skip::get_start` / `Skip::fetch_transform`, `Take::get_take_state_key`
   etc.) rather than implementing a shared `dyn Operator`. The orchestrator
   that composes operators lives in a sibling port (`zql-ivm-joins`) and can
   layer a trait on top when it's known which concrete operator graph shapes
   must be expressed. Cites **A5** (interfaces → traits, nominal) — applied
   where useful (`Storage`) and dropped where it would force premature
   dynamic dispatch.

2. **`Take::push` branching deferred.** The full `*push` / `*#pushEditChange`
   / `*#pushWithRowHiddenFromFetch` logic in `take.ts:240-694` requires the
   operator to call `self.input.fetch(...)` during its own `push` — which
   needs the sibling btree-backed `MemorySource::fetch` path to be ported
   first, *and* needs the D10 overlay epoch machinery to be wired to a real
   connection set. The epoch helpers and scope guard are in place; the full
   wiring is the next step. See A33/A36 above — applied to the state/commit
   layer that IS ported.

3. **`MemorySource::connect` / `#fetch` / `genPush` deferred.** These require
   the custom-comparator BTreeSet wrapper flagged in sibling report
   `zql-ivm-joins-btreeset-custom-cmp.md`. Sibling has not committed on
   `BTreeMap<SortKey,_>` vs `indexmap` vs `sorted-vec`. The D10 overlay
   core and all pure overlay helpers are ported and tested standalone.

4. **`push_accumulated::mergeRelationships` and `makeAddEmptyRelationships`
   deferred.** Both walk the `Node.relationships` thunk map. Porting 1:1
   requires committing to a merge policy for `Arc<dyn Fn()…>` closures (do we
   re-wrap both thunks into a single thunk that concatenates their outputs?
   Do we replace left with right?). The TS implementation relies on
   JavaScript object-spread semantics where keys in right fill in gaps in
   left — the Rust equivalent on `IndexMap<String, RelationshipThunk>` is
   ambiguous because thunks are opaque. `merge_empty` *is* ported (it only
   inserts empty thunks for missing keys — unambiguous).

5. **`constraint::pullSimpleAndComponents` and
   `primaryKeyConstraintFromFilters` and `SetOfConstraint`** not ported —
   they depend on the `Condition` AST type tree which is in `zero-cache-types`
   but is a large dependent port (`zql-builder` scope). Ported only
   `constraint_matches_row`, `constraints_are_compatible`,
   `constraint_matches_primary_key`, `key_matches_primary_key`.

All deviations respect Rule 1 ("Never invent Rust without a guide entry"):
they are gaps in the TS that need a sibling port to resolve, not absent
patterns.

## Verification

- `cargo build -p zero-cache-zql-ivm`: **ok**, no warnings.
- `cargo test -p zero-cache-zql-ivm`: **76 passed, 0 failed, 0 ignored**.
  Breakdown:
  - `stream`: 5
  - `data`: 11
  - `constraint`: 6
  - `filter`: 9
  - `skip`: 8
  - `take`: 9
  - `memory_source`: 14 (including `with_overlay_clears_on_panic` — proves
    A31 scope guard on unwinding)
  - `push_accumulated`: 8
  - `operator`: 2
- `docs/ts-vs-rs-comparison.md` row 14: updated (see below).

## Next-module dependencies unblocked

- **Partial unblock** for `zql-ivm-joins`: `Change`, `Node`, `Comparator`,
  `Yielded`, `StorageKey`, `Storage`, overlay helpers, `Bound` enum, and
  `RelationshipThunk` are all in place and can be imported by the joins
  crate. Joins still need the custom-comparator BTreeSet wrapper decision
  (sibling pattern `zql-ivm-joins-btreeset-custom-cmp`) before its own
  sorted-index operators can land.
- **Blocked for now** `zql-builder`, `pipeline-driver`: still need full
  `MemorySource::connect`/`#fetch`/`genPush` and `Take::push` to land. This
  partial port does not move those rows.
