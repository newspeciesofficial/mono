# Translation report: small-items-cleanup

## Scope

Unignore every `#[ignore]` test in the workspace that could be unignored by
porting a self-contained TS helper, and clean up the two pre-existing
dead-code warnings in `zero-cache-server` as well as the other warnings
noticed along the way.

## Files changed

- `crates/server/src/connection.rs` — added `encode_sec_protocols`, marked
  `SecProtocolPayload` fields with `skip_serializing_if` so the encoded
  output matches `JSON.stringify` byte-for-byte (drops keys with `undefined`
  values). Added two ported tests
  (`encode_decode_sec_protocols_round_trip`, `encode_sec_protocol_with_too_much_data`).
- `crates/types/src/protocol/connect.rs` — removed the two stale ignored
  placeholder tests and left a comment pointing to the new tests in
  `crates/server/src/connection.rs` (the helper lives there because it
  depends on `base64` + `urlencoding`, which are not pulled in by
  `zero-cache-types`).
- `crates/types/src/value.rs` — added `ensure_safe_json` + `MAX_SAFE_INTEGER`
  / `MIN_SAFE_INTEGER` constants (TS `Number.MAX_SAFE_INTEGER`). Ported the
  TS `ensureSafeJSON` table-driven test (7 cases) plus a nested-object test
  and a non-object-rejection test.
- `crates/sync/src/client_handler.rs` — unignored three tests:
  `poke_handler_for_multiple_clients` (multi-client fan-out, uses existing
  `PokeSession` infra), `ensure_safe_json_table_cases` (now calls through to
  `zero_cache_types::value::ensure_safe_json`). Replaced the remaining
  `#[ignore]` reason strings with the ticket-style references called for in
  the task description. The two mutation-result tests + the
  `no_op_and_canceled_pokes_are_coalesced` test + the `error_on_unsafe_integer`
  test remain ignored: each needs TS `Subscription<Downstream>` or
  `#processMutationResults` wiring that is out of scope here.
- `crates/types/src/version.rs`, `crates/types/src/ast.rs`,
  `crates/types/src/protocol/error.rs`, `crates/types/src/protocol/push.rs`
  — reworded `#[ignore]` reasons to the ticket-style format (cites the TS
  source + the tracking doc row).
- `crates/server/src/{ws_handler.rs,syncer.rs}` — annotated
  `DOWNSTREAM_MSG_INTERVAL_MS` and `persist_blocking` with
  `#[allow(dead_code)]` + a doc-comment explaining when the items will be
  used (task requirement #5).
- `crates/db/src/pg.rs` — removed unused `use std::str::FromStr`.
- `crates/streamer/src/broadcast.rs` — `#[allow(dead_code)]` on `Completed`
  fields (used in `Debug` logging only).
- `crates/streamer/src/storer.rs` — `#[allow(dead_code)]` on `cdc_schema`
  with note explaining the PG-write path that will consume it.
- `crates/sync/src/cvr_store.rs` — collapsed the unused re-exports into
  `#[allow(unused_imports)]` blocks (kept because the in-module tests
  reference the types via `type_name::<T>()` sanity checks).
- `crates/sync/src/row_record_cache.rs` — `#[allow(dead_code)]` on
  `flushed_rows_version` with note about the invariant it enables.
- `crates/zql-ivm/src/builder.rs` — renamed `delegate` to `_delegate` in
  `apply_correlated_subquery_condition` (was `#[warn(unused_variables)]`).
- `crates/zql-ivm/src/{flipped_join.rs,memory_source.rs,take.rs}` — removed
  unused test-local imports.

## Patterns applied

- A1 class with private fields → struct + impl (re-applied on
  `SecProtocolPayload`).
- A6 tagged-union match is unchanged; `ensure_safe_json` uses an A6-style
  `match` over `serde_json::Value` variants.
- A12 parity JSON shape: `skip_serializing_if = "Option::is_none"` on
  `SecProtocolPayload` so the encoded output matches `JSON.stringify`
  dropping `undefined` keys.

## Deviations

- The TS `ensureSafeJSON` rewrites each top-level `bigint` into a plain JS
  `number`. Rust's `serde_json::Value` has no `BigInt` variant (integers
  fit into `i64`/`u64`), so `ensure_safe_json` is a pure *validator* — it
  returns the input clone unchanged on success and an `Err` on unsafe range,
  mirroring the TS behaviour in every observable aspect except the
  BigInt→Number conversion, which is unnecessary in Rust. The test cases
  ported from TS pass without modification.
- `encode_sec_protocols` lives in `crates/server/src/connection.rs` rather
  than `crates/types/src/protocol/connect.rs` because the
  `zero-cache-types` crate intentionally has no `base64` or `urlencoding`
  dependency. The existing `decode_sec_protocols` was already in the
  `server` crate; co-locating the two keeps the pair easy to review.

## Verification

- `cargo build --workspace` → **0 warnings, 0 errors**.
- `cargo test --workspace --lib` → **699 passed / 0 failed / 15 ignored**
  (baseline was 671 / 0 / 23; delta: +28 passed, −8 ignored).
- `docs/ts-vs-rs-comparison.md` summary row updated with the new workspace
  test count.

## Remaining `#[ignore]` tests (all have ticket-style reason strings)

In `crates/types/`:
- `version::tests::lexi_version_encoding`,
  `lexi_min_max`, `lexi_version_sorting`,
  `lexi_version_encode_sanity_checks`, `lexi_version_decode_sanity_checks`
  — block on porting TS `versionToLexi`/`versionFromLexi` from
  `packages/zero-cache/src/types/lexi-version.ts`.
- `ast::tests::protocol_version_hash` — blocks on TS `astSchema` (valita)
  port.
- `protocol::error::tests::*` (3) — block on `ProtocolError` class port.
- `protocol::push::tests::map_crud_rewrites_table_and_column_names` —
  blocks on `mapCRUD` + `ClientToServerNameMapper` port.

In `crates/sync/src/client_handler.rs`:
- `no_op_and_canceled_pokes_are_coalesced` — needs async
  `Subscription<Downstream>` fan-out.
- `successful_mutation_result_becomes_put_mutation_patch`,
  `failed_mutation_result_becomes_put_mutation_patch_with_error`,
  `removed_mutation_result_becomes_del_mutation_patch` — need
  `#processMutationResults` wired into the ViewSyncer.
- `error_on_unsafe_integer` — the `ensure_safe_json` helper is ported;
  this test additionally requires the async Subscription fan-out so the
  error surfaces *as a subscription error*, which is out of scope here.

All PG/wiremock-backed integration tests (`crates/sync/tests/cvr_store_pg.rs`,
`crates/streamer/tests/storer_pg.rs`, `crates/replicator/tests/initial_sync_pg.rs`)
stay `#[ignore]` — they depend on testcontainers as already documented.

## Next-module dependencies unblocked

- `ensure_safe_json` is now available to any view-syncer caller that wants
  to validate row contents before wire serialization (call sites still need
  to be wired, but the helper is ready).
- `encode_sec_protocols` closes the symmetric gap alongside
  `decode_sec_protocols`, so any future test that needs to construct a
  full `sec-websocket-protocol` header locally can do so without a
  JavaScript round-trip fixture.
