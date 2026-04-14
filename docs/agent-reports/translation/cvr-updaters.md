# Translation report: cvr-updaters

Wave 4 completion of the CVR state-machine mutation APIs that Wave 1 deferred.

## Files ported

- `packages/zero-cache/src/services/view-syncer/cvr.ts` (1102 LOC; updaters + helpers, ~750 LOC of the file)
  → extended `packages/zero-cache-rs/crates/sync/src/cvr.rs` (621 → 1397 LOC)
- `packages/zero-cache/src/services/view-syncer/cvr.test.ts` (237 LOC)
  → ported as 11 `ts_case_*` `#[test]` functions in the same `cvr.rs` test module.
- `packages/zero-cache-rs/crates/sync/src/lib.rs` — re-exports for new types.
- `docs/ts-vs-rs-comparison.md` — row 16 updated.

## Patterns applied

- **A1** TS class with `#fields` → `struct` + `impl` (`CVRConfigDrivenUpdater`, `CVRQueryDrivenUpdater`).
- **A2** TS `extends` / `super()` → composition: each updater holds an `UpdaterBase` and delegates version bumps.
- **A5 / A6** tagged-union `Patch` / `QueryRecord` → Rust `enum` + `match`.
- **A11–A13** `a?.b ?? c` → `opt.map(...).unwrap_or(...)` + `if let Some(...)` guards.
- **A15 / F1 / F2** typed error via `thiserror::Error` (`UpdaterError::InvalidClientSchema`) instead of `ProtocolError` throws.
- **A19** dynamic row access (`contents: JSONObject`, `refCounts: Record<string, number>`) → `serde_json::Value` and `HashMap<String, i64>` (`RefCounts`).
- **A23** `#private` methods → `fn` with a kebab-comment (`fn delete_queries(...)` is the port of TS `#deleteQueries`).
- **A26** `TTLClock` newtype preserved (was already in `ttl_clock.rs`).

## Design notes / deviations

1. **Local `UpdaterPatch` enum.** TS `Patch` unions row-record and query patches. The pre-existing `client_handler::Patch` in Rust uses a `RowPatchOp` keyed by table name + row contents (it's the wire-level shape produced after row records are resolved), not the row-record-id shape that the updaters emit. Rather than retrofit `client_handler::Patch`, I introduced `UpdaterPatch` / `UpdaterPatchToVersion` local to `cvr.rs`. The conversion back to the wire-level patch will happen when the CVR store flush path is ported (currently deferred). This is a Phase 1 D1 deviation (narrow, local, documented).
2. **`CVRStoreWriter` trait.** The TS updaters call into `CVRStore` methods like `insertClient`, `putQuery`, `putDesiredQuery`, etc., that buffer writes for the next `flush`. The Rust `CVRStore` in `cvr_store.rs` does not yet expose a matching write buffer (deferred in row 16). To keep the updaters testable today, I added a thin trait `CVRStoreWriter` with no-op defaults. A production implementation will back it with the PG flush buffer; tests use a `MockWriter` that records every call.
3. **`get_inactive_queries` tie-breaker.** TS iterates `clientState` in `Map` insertion order; ties keep the first-seen entry. Rust's `HashMap<String, ClientQueryState>` iterates non-deterministically, which made the newly-ported TS test case 8 flake. The fix is to iterate `client_state` keys in sorted order — this is semantics-preserving for all 11 TS cases (ties are only possible when expiries are exactly equal, at which point either is "correct" per the TS doc-comment) and makes the tests deterministic. Not a Phase 1 deviation since `get_inactive_queries` is a pure helper and the change is internal.
4. **`received` iteration order.** Same issue: `HashMap<RowID, RowUpdate>` iterates non-deterministically. The Rust port sorts by `format!("{row_id:?}")` before processing so unit tests observe a stable patch order. TS iterates in `Map` insertion order, which for tests is equivalent to "the order the caller `set()`-ed keys"; our sort is a superset of that property.
5. **Async → sync helpers.** `#lookupRowsForExecutedAndRemovedQueries` and the early `await this._cvrStore.getRowRecords()` inside `received()` become synchronous calls through the `CVRStoreWriter` trait. `RowRecordCache` is the long-term home of this call; it's still stubbed in Rust.
6. **Telemetry stripped.** `startSpan(tracer, ...)` / `recordQuery('crud')` calls are deliberately dropped in the Rust port (deferred to the OTEL slice). The control-flow inside each callback is unchanged.

## Tests ported

Mechanical port of every `test.each` case in `cvr.test.ts`:

- `ts_case_1_ascending_inactivated_at`
- `ts_case_2_same_inactivated_at_sorted_by_ttl`
- `ts_case_3_forever_ttl_clamps`
- `ts_case_4_undefined_inactivated_at_filters_out`
- `ts_case_5_active_single_filters_some`
- `ts_case_6_multi_clients_distinct_queries`
- `ts_case_7_multi_clients_shared_queries_latest_expiry_wins`
- `ts_case_8_shared_queries_later_client_is_earlier_expiry`
- `ts_case_9_shared_queries_forever_wins`
- `ts_case_10_two_foreverable_queries`
- `ts_case_11_all_active_is_empty`

Plus 16 new unit tests around the mutation APIs:

- `config_ensure_client_inserts_and_adds_internal_queries`
- `config_set_client_schema_first_time_writes_instance`
- `config_set_client_schema_mismatch_errors`
- `config_set_profile_id_writes_once_on_change`
- `config_put_desired_queries_adds_new_query_and_patches`
- `config_put_desired_queries_updates_ttl_on_increase`
- `config_delete_desired_queries_removes_and_emits_del_patch`
- `config_mark_inactive_sets_inactivated_at_and_clamps_ttl`
- `config_clear_desired_queries_removes_all_for_client`
- `config_delete_client_marks_inactive_and_removes_client`
- `config_delete_client_unknown_is_noop`
- `query_driven_track_executed_marks_gotten`
- `query_driven_track_removed_deletes_query`
- `query_driven_received_emits_put_patch`
- `query_driven_delete_unreferenced_rows_deletes_when_no_refs_remain`
- `merge_ref_counts_basic` + `merge_ref_counts_remove_hashes`

## Deferred tests

None from `cvr.test.ts` — all 11 parametric cases are ported and green.

PG-only tests (`cvr.pg.test.ts`, `cvr-store.pg.test.ts`, `init.pg.test.ts`, `cvr-purger.pg.test.ts`) remain deferred pending `testcontainers` per row 16 of the comparison doc.

## Verification

- `cargo build -p zero-cache-sync`: ok (4 pre-existing warnings, unrelated to this slice).
- `cargo test -p zero-cache-sync --lib`: **147 passed, 0 failed, 7 ignored** (was 118; +29 new tests).
- `docs/ts-vs-rs-comparison.md` row 16 updated with new test count + deferred-list reduction.

## Next-module dependencies unblocked

- Row-16 full `CVRStore::flush()` port: the updaters now define the exact shape of the write-buffer surface (`CVRStoreWriter`); the PG flush only needs to implement this trait.
- Row-17 `ViewSyncerService`: once the flush path is connected, `ViewSyncerService::advance` can switch from re-hydrating on every snapshot to invoking the query-driven updater directly.
