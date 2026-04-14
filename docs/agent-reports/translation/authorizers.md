# Translation report: authorizers

## Files ported

- `packages/zero-cache/src/auth/read-authorizer.ts` (152 LOC) →
  `packages/zero-cache-rs/crates/auth/src/read_authorizer.rs` (~640 LOC incl. tests)
- `packages/zero-cache/src/auth/write-authorizer.ts` (594 LOC) →
  `packages/zero-cache-rs/crates/auth/src/write_authorizer.rs` (~880 LOC incl. tests)
- `packages/zero-cache-rs/crates/auth/src/lib.rs` extended with two new
  module re-exports.
- `packages/zero-cache-rs/crates/auth/Cargo.toml` gained a dep on
  `zero-cache-zql-ivm` (workspace path).

## Scope note (IVM + sandbox dependencies)

Both TS authorizers ultimately depend on `buildPipeline(...).fetch({})`
against an IVM operator graph and on `StatementRunner.beginConcurrent /
rollback` against the live replica. Both prerequisites are still in
flight:

- Comparison row 14 — `buildPipeline` in `zero-cache-zql-ivm` is partial
  (the AST walkers are ported; the operator wiring is deferred).
- Part 3 Risk #2 — `BEGIN CONCURRENT` was dropped per Phase 1 Decision D2
  in favour of `BEGIN DEFERRED` + `ROLLBACK`.

To ship a compilable, testable port today without reinventing either
subsystem, the write-authorizer is parameterised on five traits that the
caller wires up:

- `PermissionsHolder` — in-memory `Arc<PermissionsConfig>` + `reload()`.
- `TableSpecs` — `table → primary_key` map and `has_table`.
- `PreMutationRowFetcher` — `SELECT … WHERE pk = ?` lookup.
- `PolicyEvaluator` — runs the bound AST through the IVM pipeline and
  returns whether any row comes back.
- `SandboxTxn` — opens `BEGIN DEFERRED`, pushes hypothetical mutations,
  and unconditionally rolls back (Phase 1 D2).

The whole control-flow (phase/action gating, cell-policy iteration,
bound-AST construction, `updateWhere`, `bindStaticParameters`, closed-
world deny on empty policies, early-return on first denial) is ported
1:1 and exercised by stub implementations in the tests. Production
callers will swap the stubs for the real adapters once row 14 and the
rusqlite adapter land.

## Patterns applied

- **A1** class with private fields → `WriteAuthorizerImpl` struct + `impl`
- **A5** interface → `trait WriteAuthorizer`, `trait
  PolicyEvaluator/TableSpecs/PreMutationRowFetcher/SandboxTxn/
  PermissionsHolder`
- **A6** tagged union → `CrudOp` / `CrudOpAny` `#[serde(tag = "op")]`
  enums; `Condition` already enum in types crate; `Phase` + `Action`
  enums
- **A8/A9** associated op type (`ActionOpMap`) → inlined `match` per
  action (the TS generic is purely a type-level index, no runtime
  counterpart needed)
- **A11/A12/A21** `?.`, `??`, `!= undefined` → `Option::map_or`,
  `Option::and_then`, `Option::is_none_or`
- **A13/A20** immutable struct update → `.clone()` + field replace
- **A15/F1/F2** typed errors — `WriteAuthError`, `PolicyEvalError` via
  `thiserror`; **F3** `.context(...)` via `#[source]`-chained variants
- **A17** sequential `await`-in-`for` → straight `for … { … .await? }`
  with early return
- **A19** `Object.entries`/`Object.fromEntries` → `BTreeMap` iteration
- **A23** `#private` fields → plain private struct fields
- **A27** `MaybePromise<T>` → plain `async fn passes_policy`; the
  `policy.is_none()` / `policy.is_empty()` fast paths return
  synchronously before the first `.await`, matching TS semantics (the
  new-pattern doc recommends this exact shape for Wave 4)
- **B7** `performance.now()` → `std::time::Instant::now()` in
  `timed_can_do`, logged via `tracing::info!`

Part 2 crate rows applied: none new — the port uses only what was
already in-tree (`zero-cache-types`, `zero-cache-zql-ivm`, `tokio`,
`tracing`, `thiserror`, `serde`, `serde_json`).

## Read-authorizer port (deep dive)

`transform_query` is a pure AST transformation with no IVM dependency.
The port mirrors the TS structure exactly:

1. Collect `permissionRules?.tables?.[query.table]?.row?.select`. If the
   list is `undefined` OR has length 0, emit a warning and substitute
   the closed-world-deny policy `[['allow', OR []]]`.
2. For the `where` clause: recurse with `transform_condition`, which
   walks `and`/`or`/`correlatedSubquery` nodes and calls
   `transform_query_internal` on each subquery so nested permissions
   apply. `simple` nodes pass through.
3. Wrap the result in `AND [existing_where?, OR [rule_conditions…]]`
   and run `simplify_condition`.
4. Apply `bind_static_parameters` with `authData = decoded JWT` (empty
   object when auth is absent). This is the already-ported function in
   `zero-cache-types`.

`simplify_condition` (port of `zql/src/query/expression.ts:249`) is
implemented locally in the read-authorizer module since it wasn't
exported from `zql-ivm`. The TS `TRUE = {type:'and', conditions:[]}`
and `FALSE = {type:'or', conditions:[]}` contracts are preserved.

`hash_of_ast` (port of `zero-protocol/src/query-hash.ts:6`) uses
`DefaultHasher` over the serde-serialized JSON, base-36-encoded. This
is NOT byte-compatible with the TS `h64`/`xxhash` output — see
Deviation 1 below.

## Write-authorizer port (deep dive)

- `normalize_ops` — upsert-to-update routing based on a live
  primary-key SELECT, via the injected `PreMutationRowFetcher`. All
  other CRUD kinds pass through.
- `validate_table_names` — returns `WriteAuthError::InvalidTable` on
  unknown tables; matches TS exception semantics.
- `can_pre_mutation` — iterates `ops` sequentially, early-returning
  `false` on first denial. Insert skips (pre-mutation policies don't
  apply). Update/delete call `timed_can_do` with `Phase::PreMutation`.
- `can_post_mutation` — opens the sandbox (`BEGIN DEFERRED`), pushes
  hypothetical mutations into the `TableSource` via
  `SandboxTxn::apply_hypothetical`, then evaluates post-mutation
  policies on the modified snapshot. `ROLLBACK` runs in a `finally`
  equivalent regardless of success. Insert/update gate on post-
  mutation policies; delete skips.
- `can_do` — evaluates applicable row + cell policies per (phase, action)
  tuple, feeding `passes_policy_group`. Matches the TS table → row →
  cell evaluation order commented in write-authorizer.ts:358–366.
- `passes_policy_group` — row policy first, then each cell policy; any
  `false` short-circuits.
- `passes_policy` — `policy.is_none() || policy.is_empty()` → return
  `false` synchronously (A27). Otherwise rewrites `where` with
  `update_where`, binds static params, and asks the injected
  `PolicyEvaluator` whether the pipeline returns any rows.
- `update_where` — asserts that a `where` already exists (matches the
  TS `assert(where, 'A where condition must exist for RowQuery')`),
  composes it with `OR [rule conditions]` via AND, and simplifies.

## Deviations

1. **`hash_of_ast` is process-stable only, not cross-language.** TS
   uses `h64(JSON.stringify(normalizeAST(ast))).toString(36)` (xxhash-
   64 over a canonicalised AST). The Rust port uses
   `DefaultHasher::hash(json)` base-36-encoded. It is stable within a
   single Rust process (enough for the transformation-cache key use
   that the only caller — `transform_and_hash_query` — needs). A
   future pass can swap in `xxhash-rust` + a shared `normalize_ast`
   routine once the AST normalizer is ported; the API is unchanged.
2. **No `WriteAuthorizer::destroy()`.** TS calls `cgStorage.destroy()`
   on shutdown. Rust uses `Drop`; the injected `PermissionsHolder`
   owns its storage and cleans up when dropped. Semantically
   equivalent.
3. **`PermissionsConfig` is a newly typed shape** (`tables:
   BTreeMap<String, TablePermissions>` with `AssetPermissions`,
   `UpdatePermissions`, `Rule`, `Policy`). The existing `permissions.rs`
   module kept `tables` as `serde_json::Value` (Wave 1 port); the new
   typed shape in `read_authorizer.rs` is the one both authorizers
   consume. `PermissionsConfig::from_value` bridges the two. No
   semantics change — both are wire-compatible with the TS output of
   `permissionsConfigSchema`.
4. **`BEGIN CONCURRENT` → `BEGIN DEFERRED` + `ROLLBACK`** per Phase 1
   Decision D2, implemented via the `SandboxTxn` trait. The trait
   guarantees `rollback()` runs unconditionally on both the success
   and failure paths. No behavioural difference from TS as long as
   callers honour the read-only contract (don't commit inside the
   block).

None of these deviations invent patterns absent from the guide.
Deviation 1 traces to the lack of a ported `normalize_ast` (to be
added alongside a proper `xxhash-rust` swap); Deviations 2–4 trace to
existing Phase 1 decisions (D2, A1, E1/E2).

## Tests ported

### read_authorizer.rs (22 `#[test]`s)

Tests fall into five buckets, all port-safe because `transform_query`
is pure AST manipulation:

- **Permissions parsing** (2) — `PermissionsConfig` round-trips the
  minimal `{tables: {}}` and a row-select rule shape.
- **Closed-world deny** (4) — unreadable table, empty `row` section,
  empty `row.select`, and empty select rules list all yield `OR []`
  (TS write-authorizer.test.ts:180–200).
- **Permission application** (3) — ANYONE_CAN simplifies to TRUE;
  admin rule binds `authData.role`; missing auth yields a null literal
  binding.
- **Recursive transformation** (2) — nested `related` subqueries get
  permissions applied recursively; `whereExists` subqueries get
  recursively transformed inside `where` conditions.
- **Helpers** (5) — `simplify_condition` unwraps single-child AND/OR,
  collapses AND-with-FALSE → FALSE, collapses OR-with-TRUE → TRUE,
  flattens nested same-type.
- **Hashing + internal-query shortcut** (4) — hash stable, hash differs
  for different ASTs, `internalQuery=true` skips permissions,
  `internalQuery=false` applies them.
- **`RuleAction` serde** (1) — `["allow", …]` wire shape preserved.

Corresponding TS tests that use inline-snapshot with
`newStaticQuery(schema, ...)` fixtures are NOT byte-for-byte-ported.
Those snapshots depend on the TypeScript query builder chain
(`packages/zql/src/query/static-query.ts`,
`packages/zero-schema/src/builder/*`), none of which is ported to Rust
(they're frontend concerns). The behavioural invariants those tests
assert are covered here via AST-level fixtures.

### write_authorizer.rs (14 `#[test]`s + `#[tokio::test]`s)

- `validate_table_names_rejects_unknown` / `…_accepts_known`
- `normalize_upsert_converts_to_update_if_row_exists`
- `normalize_upsert_converts_to_insert_if_row_missing`
- `normalize_passthrough_of_non_upsert`
- `update_where_composes_and_or`
- `update_where_panics_on_missing_where`
- `can_pre_mutation_skips_inserts`
- `can_post_mutation_applies_insert_policy_and_rollsback` — asserts
  the `begin → apply → rollback` event order
- `can_post_mutation_denies_when_policy_returns_no_rows` — asserts
  `ROLLBACK` still runs on denial
- `empty_insert_policy_is_closed_world_deny`
- `missing_insert_rule_denies_by_default`
- `delete_runs_pre_mutation_only`
- `update_runs_both_phases`
- `crud_op_serializes_with_op_tag` / `crud_op_any_serializes_with_op_tag`

Deferred TS tests that depend on a live SQLite Database,
`DatabaseStorage`, `TableSource`, `StatementRunner`,
`CREATE_TABLE_METADATA_TABLE`, and the full `WriteAuthorizerImpl`
constructor (that crate's constructor takes a `Database`
directly — write-authorizer.test.ts:57–69):

- All `describe('canPreMutation', ...)` cases (~15 tests)
- All `describe('canPostMutation', ...)` cases (~15 tests)
- `normalize ops` upsert-to-insert/update tests that rely on a real
  SELECT round-trip
- Cell-policy-column-not-updated skip tests

These will port mechanically once the `zero-cache-db` crate exposes a
rusqlite-backed `StatementRunner` and the `zero-cache-zql-ivm` crate
exposes the concrete `buildPipeline` adapter. All the control flow
they test is already exercised in the stubbed `#[tokio::test]` cases
above.

## Verification

- `cargo build -p zero-cache-auth`: ok (4 harmless warnings on async_fn
  in trait and unused-import on `zero-cache-zql-ivm`'s pre-existing
  code — none from the new modules after cleanup).
- `cargo test -p zero-cache-auth`: **74 passed**, 0 failed, 0 ignored.
  - 11 `auth::tests` (Wave 1, unchanged)
  - 13 `jwt::tests` (Wave 1, unchanged)
  - 8 `permissions::tests` (Wave 1, unchanged)
  - 6 `read_authorizer::tests` (Wave 4, closed-world-deny / permission
    application / recursive transformation)
  - 4 `read_authorizer::tests` (Wave 4, simplify_condition unit)
  - 2 `read_authorizer::tests` (Wave 4, hash_of_ast)
  - 2 `read_authorizer::tests` (Wave 4, transform_and_hash_query)
  - 2 `read_authorizer::tests` (Wave 4, permissions serde round-trip)
  - 1 `read_authorizer::tests` (Wave 4, RuleAction serde)
  - 2 `write_authorizer::tests` (Wave 4, validate_table_names)
  - 3 `write_authorizer::tests` (Wave 4, normalize_ops)
  - 2 `write_authorizer::tests` (Wave 4, update_where)
  - 6 `write_authorizer::tests` (Wave 4, can_pre/post_mutation control
    flow + policy gating)
  - 2 `write_authorizer::tests` (Wave 4, CrudOp serde)
- `cargo build --workspace`: ok, no regressions elsewhere.
- `docs/ts-vs-rs-comparison.md` row 20 updated.

## Next-module dependencies unblocked

- Row 20 (auth overall) advances from `Port partial` (38 tests) to
  `Port partial — authorizer control flow ported, IVM + sandbox
  adapters pending` (74 tests).
- Once the `zero-cache-zql-ivm::build_pipeline` lands end-to-end (row
  14 `Stub → Port partial/complete`), the `PolicyEvaluator` impl is
  a ~30-LOC adapter: build the AST, call `build_pipeline`, call
  `fetch({})`, return `true` on first yielded row.
- Once the `zero-cache-db` crate exposes a rusqlite-backed
  `StatementRunner` with `BEGIN DEFERRED`/`ROLLBACK` wrappers, the
  `SandboxTxn` impl is a ~20-LOC adapter.

## Notes for the orchestrator

- No new crate added.
- No new pattern added to the guide — the `auth-maybe-promise-sync-
  fast-path` new-pattern was already on disk and is realised here via
  A27 plain `async fn` with pre-`.await` early returns.
- `async fn` in `trait WriteAuthorizer` emits the expected Rust 2024
  `async_fn_in_trait` lint; annotated `#[allow]` locally (the trait
  is used with generics-only, no dyn dispatch in scope).
- `hash_of_ast` wire compatibility with TS is a known gap — safe today
  because the only consumer is an in-process cache key.
