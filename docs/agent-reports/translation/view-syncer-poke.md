# Translation report: view-syncer-poke

## Files ported / extended
- `packages/zero-cache/src/services/view-syncer/client-handler.ts` (LMID extraction, mutationsPatch, desiredQueriesPatches routing) →
  `packages/zero-cache-rs/crates/sync/src/client_handler.rs`
- `packages/zero-cache/src/services/view-syncer/view-syncer.ts` (upstreamSchema threading) →
  `packages/zero-cache-rs/crates/sync/src/view_syncer.rs`
- `packages/zero-protocol/src/mutations-patch.ts`, `mutation-id.ts` (wire types) →
  `packages/zero-cache-rs/crates/types/src/protocol/poke.rs` (added `MutationID`, `MutationResponse`,
  `MutationPatchOp`; added `mutations_patch` to `PokePartBody`).

## What landed

1. **LMID extraction** — `ClientHandler::new` now takes `upstream_schema: &str`
   (`"{app_id}_{shard_num}"`), stores `{schema}.clients` / `{schema}.mutations`
   table names, and `PokeSession::add_row_patch` routes by `tableName`:
   - `{schema}.clients`: `LmidRow` parsed via serde. Matching `clientGroupID`
     inserts `clientID → lastMutationID` into
     `PokePartBody.last_mutation_id_changes` (wire key: `lastMutationIDChanges`).
     Mismatched groups are warn-logged and dropped (parity with TS
     `#updateLMIDs:379-383`).
   - `{schema}.mutations`: put → parsed via `MutationRow`, pushed as
     `MutationPatchOp::Put { mutation: { id: {clientID, id}, result } }`;
     del → `{clientGroupID, clientID, mutationID}` row-key pulled out and
     pushed as `MutationPatchOp::Del { id: {clientID, id} }`.
   - Everything else stays in `rows_patch`.
   LMID and mutation rows are NEVER forwarded in `rows_patch`.

2. **`mutationsPatch` wire type** — new module-level `MutationPatchOp` added to
   `crates/types/src/protocol/poke.rs` + a `mutations_patch:
   Option<Vec<MutationPatchOp>>` field on `PokePartBody` (matches TS
   `mutationsPatchSchema` shape).

3. **`desiredQueriesPatches` routing** — already implemented and keyed by
   `client_id` (prior fix). Confirmed behaviour matches TS switch-case order:
   desired goes per-client, got goes global (`view_syncer.rs:385-395`).

4. **Shard plumbing** — `ViewSyncerService::new` now takes `shard_num: u32`
   so `connect_client` can propagate the fully-qualified upstream schema
   (`{app_id}_{shard_num}`) to `ClientHandler::new`. Single caller in
   `crates/server/src/syncer.rs::get_or_create_view_syncer` updated to pass
   `self.shard_num`.

## Patterns applied
- **A6** tagged-union enum + match for `RowPatchOp` → route-by-table dispatch.
- **A11 / A12** — typed row parse via `#[derive(Deserialize)]` + match; failed
  parse warn-logs and drops the row (TS `v.parse(..., 'passthrough')` parity
  minus the assertion failure — we log-and-drop because the Rust path has no
  request-scoped abort).
- **A21** — `let Some(...) else { warn!(...); return; }` for null-safe
  row-key traversal (`clientID`, `mutationID`).
- **A1 / A5** — new `LmidRow`, `MutationRow` private structs live alongside
  `ClientHandler`.
- **F2** — warn-logs on cross-group LMID rows / malformed mutation row-keys
  (non-fatal, mirrors TS `#lc.error` + continue).

## Deviations
- TS raises a hard assertion (`assert(typeof clientID === 'string', ...)`) on
  malformed mutation-row delete keys; Rust logs-and-drops instead. Rationale:
  A11/A12 — in Rust the single-process sync path has no per-patch
  `ProtocolError` wrapper, and dropping a single malformed patch preserves
  the rest of the poke. Matches existing row-parse failure policy in this
  file.

## Verification
- `cargo build -p zero-cache-types`: ok
- `cargo build -p zero-cache-sync`: ok (4 pre-existing warnings, unchanged)
- `cargo test -p zero-cache-sync`: **118 passed, 0 failed, 0 ignored** (was
  113; +5 new tests: `lmid_row_with_matching_group_populates_last_mutation_id_changes`,
  `lmid_row_with_wrong_group_is_ignored`,
  `mutation_row_put_populates_mutations_patch`,
  `mutation_row_del_populates_mutations_patch`,
  `regular_table_row_stays_in_rows_patch`).
- `docs/ts-vs-rs-comparison.md` rows 15 + 17 updated.

## Next-module dependencies unblocked
- The mutations pipeline (row 9/11) can now surface mutation-result rows to
  clients once a change-source populates `{schema}.mutations`.
- The Pusher (row 21) can rely on the client receiving `mutationsPatch`
  correctly once it forwards responses back via the CVR.
