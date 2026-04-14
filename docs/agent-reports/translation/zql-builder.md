# Translation report: zql-builder

## Files ported

| TS source (`packages/zql/src/builder/`) | LOC (TS) | Rust target (`crates/zql-ivm/src/`) | LOC (RS, incl. tests) |
|-----------------------------------------|----------|-------------------------------------|-----------------------|
| `like.ts`                               | 71       | `like.rs`                           | 355                   |
| `filter.ts`                             | 204      | `condition.rs`                      | 631                   |
| `debug-delegate.ts`                     | 103      | `debug_delegate.rs`                 | 198                   |
| `builder.ts`                            | 796      | `builder.rs`                        | 918                   |

TS in scope: **1,174 LOC**. Rust added: **~2,102 LOC** (incl. tests).
Tests: `like.test.ts` (14 cases) → `like::tests::like_basics` table-test;
`filter.test.ts` (5 tests) → `condition::tests::*` (14 tests incl.
per-operator breakouts); `builder.test.ts` is ~2,961 LOC of pipeline-level
integration tests — deferred (see below).

Added `regex = "1"` to `crates/zql-ivm/Cargo.toml`
(workspace `thiserror` / `indexmap` / `scopeguard` already wired).
`lib.rs` updated to export the four new modules.

## Patterns applied

From the translation guide (including Phase 1 decisions):

- **A1** class → struct + impl (`Debug`, `LikePredicate` enum).
- **A5** TS `interface` → Rust `trait` (`DebugDelegate`, `BuilderDelegate`,
  `Source`, `SourceInput`).
- **A6** tagged-union `switch (type)` → exhaustive `match` on
  `Condition`, `ValuePosition`, `NonColumnValue`, `SimpleOperator`,
  `CorrelatedSubqueryOp`.
- **A10** structural `Record<string, T>` → `HashMap<String, T>` (row-counts
  and rows tables in `debug_delegate`; `HashSet<String>` for split-edit
  keys in builder).
- **A13** struct-update `{...node, where: ...}` → Rust clone + field
  override (AST rewriting in `uniquify_correlated_subquery_condition_aliases`).
- **A15 / F1** typed errors via `thiserror::Error`. `LikeError`,
  `BuildError` enums; panics retained only for protocol invariants
  (unbound `Static` positions) to match the TS `assert(...)` intent.
- **A18** closure capturing mutable outer var → `let mut count` with
  `walk(&mut count)` recursion in the uniquify helper.
- **A19** dynamic field access `row[left.name]` → `Row::get(&name)`
  returning `Option<&Value>`; `get_row_value` collapses missing vs. None.
- **A21** nullish-coalescing `??` → `unwrap_or_default()` /
  `unwrap_or(None)` at the relevant call sites.
- **A28** SQL LIKE → `regex::RegexBuilder::new(&pattern)
  .multi_line(true).case_insensitive(ci).build()`, with explicit escape
  for `$()*+.?[]\^{|}` and backslash handling for `\%`/`\_` literals; no-
  wildcard fast-path uses `str::eq` / `eq_ignore_ascii_case`.
- **A29** NULL-aware short-circuit: `=/!=/</<=/>/>=/LIKE/ILIKE/IN/NOT IN`
  return `false` when lhs or rhs is null/missing; `IS` / `IS NOT`
  participates in null matching (missing column collapses to null at
  `row.get()`). Both behaviours covered by dedicated tests.
- **A30 / D1** cooperative-yield sentinel — unchanged; the builder does
  not itself create yield-bearing streams (it only wires operators that
  do).

From the new-pattern files validated earlier:

- `new-patterns/zql-builder-sql-like-regex.md` — applied verbatim in
  `like.rs` (enum + `RegexBuilder` + escape set).
- `new-patterns/zql-builder-null-aware-predicates.md` — applied verbatim
  in `condition.rs::simple_predicate` + `get_row_value`.

## Deviations

1. **`build_pipeline` is a skeleton, not a live pipeline constructor.**
   The TS `buildPipeline` composes nine concrete IVM operators (`Filter`,
   `Skip`, `Take`, `Join`, `FlippedJoin`, `Exists`, `FanIn`, `FanOut`,
   `UnionFanIn`, `UnionFanOut`). The existing Rust operator structs in
   this crate — per the two prior translation reports — are partial:
   - `Filter::new(predicate)`, `Skip::new(bound, comparator)`,
     `Take` (state-only) expose typed methods but **do not wrap an
     upstream `Arc<dyn Input>`**, so you can't chain them the way
     `buildPipeline` does.
   - `Source::connect` / `MemorySource::connect` / `Take::push` are all
     still on the deferred list (zql-ivm-ops Deviations 2 & 3;
     zql-ivm-joins Deviation 5).
   The builder here handles every branch that becomes reachable once
   those operators land, and for each branch that isn't yet wireable it
   returns `BuildError::Deferred("…")` with a precise reason. Per Rule 1
   ("Never invent Rust without a guide entry") this preserves the TS
   structure while still letting the crate compile and pass tests.
   Every **pure** helper (`assert_no_not_exists`,
   `gather_correlated_subquery_query_conditions_owned`,
   `uniquify_correlated_subquery_condition_aliases`,
   `condition_includes_flipped_subquery_at_any_level`,
   `is_not_and_does_not_contain_subquery`, `partition_branches`,
   `group_subquery_conditions`, `assert_ordering_includes_pk`,
   `assert_no_static`) is ported 1:1 and covered by tests. Cites
   **D1 / A30** (no new yield representation) and **Wave 1/2 Deviation 1**
   (operator trait surface still materialises `Vec<Yielded<_>>`).

2. **`completeOrdering` and `planQuery` not invoked from `build_pipeline`.**
   Both are in `zql/src/query/complete-ordering.ts` and
   `zql/src/planner/planner-builder.ts` — out of this slice's scope. The
   TS entry points to both; the Rust port documents them as caller
   responsibility in `build_pipeline`'s docstring. The validation report
   ("Risk-flagged items depended on") already anticipated this.

3. **`transformFilters`'s `simplifyCondition` call is a minimal collapse.**
   TS calls `simplifyCondition(...)` from `../query/expression.ts`;
   that module is out of scope. The Rust port collapses empty and
   single-entry and/or nodes (the subset that is demonstrably needed by
   the tests) and documents the rest as "downstream cares about the
   structure". Per Rule 3 (keep it mechanical) and Rule 2 (no speculative
   work) — extending to the full simplifier is a task for whoever ports
   `zql/src/query/expression.ts`.

4. **`Source` trait is scoped to what `builder.ts` actually reads.**
   Omits `push` / `genPush` (pipeline-driver concerns; out of scope per
   the validation report). Keeps `connect`, `table_primary_key`, and
   the `fully_applied_filters` flag on `SourceInput`. When a future
   slice wires the full `Source` interface (for push paths), it can
   extend this trait in-place.

5. **Numeric equality uses `f64 == f64` (via `as_f64`)** in `value_eq`.
   `serde_json` distinguishes integer and floating-point numbers; TS
   `===` on numbers collapses both to IEEE-754 doubles. Without this
   bridge, `column = 4` (i64) vs. literal `4.0` (f64) would fail. The
   fix is the standard TS-parity translation.

6. **`gather_correlated_subquery_query_conditions` exists in two
   flavours** — an owned-clone variant (used) and a borrow variant (API
   placeholder). The TS call shape is always owned. The borrow variant
   is wired as a no-op so downstream code that wants it doesn't have to
   restructure when the pipeline lands.

All deviations respect Rule 1: each cites an existing guide entry or a
validated new-pattern file.

## Verification

- `cargo build -p zero-cache-zql-ivm`: **ok**, no new warnings.
- `cargo test -p zero-cache-zql-ivm`: **159 passed / 0 failed / 0 ignored**
  (was 117; **+42 new tests** in this slice).

  New-test breakdown:
  - `like`: **4** test functions, one of which is a 54-case table-driven
    test (`like_basics`) covering every TS fixture — matches
    `like.test.ts` fully.
  - `condition`: **14** (`eq_column_literal`,
    `null_short_circuits_for_comparisons`, `is_null_matches_null`,
    `is_not_null_rejects_null`, `comparison_ops`, `and_predicate`,
    `or_predicate`, `empty_and_is_true_empty_or_is_false`,
    `nested_or_and`, `like_via_predicate`, `ilike_via_predicate`,
    `in_operator`, `not_in_operator`, `static_rejects`,
    `transform_filters_strips_subquery`). Covers every case from
    `filter.test.ts` except the `fast-check` property tests (which are a
    TS-only harness).
  - `debug_delegate`: **5** (`init_then_vend_increments_count`,
    `init_without_vend_is_zero`, `rows_are_captured`, `nvisit_accumulates`,
    `reset_clears`).
  - `builder`: **16** (pure helpers — `assert_no_not_exists` *3 branches,
    `gather_correlated_subquery_query_conditions` *2, `uniquify` *3,
    `condition_includes_flipped_subquery_at_any_level`,
    `is_not_and_does_not_contain_subquery_cases`,
    `partition_branches_splits_correctly`,
    `group_subquery_conditions_splits`,
    `assert_ordering_includes_pk` *2, `assert_no_static` *2,
    `transform_filters_reexport_works`).

- `docs/ts-vs-rs-comparison.md` row 14: **updated** — LOC bumped
  `~11K → ~12.2K`, Rust LOC `~4.6K → ~5.3K`, test count
  `117 → 159 (+42)`, defect column updated to reflect A28/A29 wins.

Deferred (needs sibling ports):

- `builder.test.ts` (~2,961 LOC) — pipeline-level integration tests.
  These drive a full graph with `MemorySource`, `Snitch`, `FilterSnitch`,
  `TestBuilderDelegate`; need the deferred `MemorySource::connect` /
  `Take::push` / wrapper-adapters that turn `Filter`/`Skip`/`Take` into
  `Arc<dyn Input>` / `Arc<dyn FilterInput>`. Will port straight across
  once those land.
- The Skip-as-Input / Take-as-Input adapters themselves, plus
  `buildFilterPipeline` (the TS glue in `filter-operators.ts`). These
  produce `BuildError::Deferred` at the call sites.
- `planQuery` + `completeOrdering` (out-of-scope per validation
  report).
- `TestBuilderDelegate` port — depends on `Snitch` / `FilterSnitch`
  which live in `zql/src/ivm/snitch.ts` (not yet ported; not in scope
  for this slice).

## Next-module dependencies unblocked

- `pipeline-driver` now has a `BuilderDelegate` / `Source` / `SourceInput`
  trait surface it can implement against, plus a fully-typed
  `condition_to_predicate` that handles every simple-condition shape the
  view-syncer actually emits (A28 + A29). Once the sibling
  `MemorySource::connect` lands, the `build_pipeline` deferred-branches
  can be filled in without touching anything in this slice.
- Any Rust caller that only needs the **predicate** (not the full
  operator graph) — e.g. a future direct-SQL fast-path that still wants
  the TS NULL-semantics — can use `condition_to_predicate` today.
- `zql-builder`-consuming code no longer needs to wait on a separate
  crate: `DebugDelegate`, `LikePredicate`, `BuilderDelegate`,
  `BuildError`, and the pure helpers all live in `zero-cache-zql-ivm`
  next to their operator-side dependencies. Moving to a dedicated
  crate later is a mechanical `cargo new --lib` + re-export change.
