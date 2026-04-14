# Translation report: snapshotter

## Files ported

- `packages/zero-cache/src/services/view-syncer/snapshotter.ts` (579 LOC)
  → `packages/zero-cache-rs/crates/sync/src/snapshotter.rs` (~780 LOC incl.
  tests; prior stub was ~456 LOC)
- `packages/zero-cache/src/services/view-syncer/snapshotter.test.ts` (707 LOC,
  vitest) → translated-in-scope as `#[cfg(test)]` tests in the matching
  `.rs` file (cases requiring `FakeReplicator` + `zqlite`'s logical-replication
  message emitter are deferred; see "Deferrals" below).

## Patterns applied

- **A1** TS class with private fields → Rust `struct Snapshotter` + `impl`,
  `Snapshot::create` / `begin_and_pin` helpers replace the TS static +
  constructor combo.
- **A15 / F1 / F2** — typed error enum `SnapshotError` (`thiserror`),
  `NotInitialized` / `AlreadyInitialized` / `Sqlite(rusqlite::Error)` /
  `Lock(String)` / `Join(tokio::task::JoinError)` / `Other(String)`.
  `ResetPipelinesSignal` and `InvalidDiffError` are also `thiserror`-derived.
- **A31** "must rollback on error" — `Snapshot::reset_to_head` always emits
  `ROLLBACK` before re-`BEGIN DEFERRED`; `rusqlite::Connection::drop` is the
  final safety net if a snapshot is dropped mid-txn.
- **C14** server-side portal/cursor streaming for rows within a snapshot:
  `Snapshot::for_each_change(prev_version, f)` streams via
  `rusqlite::Statement::query` (one-row-at-a-time cursor, the SQLite
  analogue of the PG portal pattern that `row_record_cache.rs::catchup_row_patches`
  uses via `query_raw`).
- **B4 / D3 / D1** already applied at crate level: the `AsyncSnapshotter`
  wrapper delegates all SQLite work to `tokio::task::spawn_blocking`
  (the single-process / single-thread-pool model from Phase 1 D3).
- **D2** (Phase 1 decision) — **no `BEGIN CONCURRENT`**. Read snapshots use
  `BEGIN DEFERRED` over the stock WAL journal mode. The `wal2` assertion
  from TS line 284 is dropped. Writers (a separate replicator thread) must
  serialise via `BEGIN IMMEDIATE`; that lives in the replicator crate and
  is out of this scope.

## Deviations (with rule citations)

1. **D2 — leapfrog removed.** TS holds two SQLite connections that take
   turns being "current" (`#curr`) and "previous" (`#prev`), swapping on
   each `advance()` so that the diff iterator can still read the prior
   snapshot's row values. We drop that: one connection per `Snapshotter`,
   `advance()` rolls back + re-`BEGIN`s in place. `#previousSnapshot()`
   collapses into a single `current()` method, as recommended in the
   translator brief.
2. **`SnapshotDiff` is eagerly materialised, not lazily iterated.**
   Because the previous read snapshot no longer exists in parallel, the
   diff can't be turned into a lazy iterator that reads from `prev` while
   the user applies changes to it. Instead, `Snapshotter::advance()`
   computes `num_changes` and `entries: Vec<ChangeLogEntry>` up front
   against the new snapshot. This matches what the Rust `PipelineDriver`
   currently consumes; the TS `SnapshotDiff[Symbol.iterator]` semantics
   that thread through `tableSpec`/`zqlSpec`/`uniqueKeys` are deferred
   (see Deferrals).
3. **`get_rows` NULL-filter is implemented at the Rust layer.** The
   critical correctness + perf workaround described in `AGENTS.md` ("NULL +
   OR = full table scan") is replicated bit-for-bit: only unique-key
   tuples whose every column is non-NULL contribute a branch to the `OR`
   clause; otherwise a full-table scan is produced.

## Deferrals (not translated in this scope)

- **TS `Diff` class** (`snapshotter.ts` lines 389–572): the per-change
  projection that combines `LiteAndZqlSpec`, `fromSQLiteTypes`, and the
  `my_app.permissions` reset detector. This depends on Rust analogues of
  `LiteTableSpecWithKeysAndVersion` / `LiteAndZqlSpec` that don't exist
  yet; the current `PipelineDriver` uses raw `ChangeLogEntry`s directly.
  The supporting primitives (`num_changes_since`, `changes_since`,
  `for_each_change`, `get_row`, `get_rows`) are all in place so this
  layer is a straight follow-up.
- **TS tests that rely on `fakeReplicator` + `ReplicationMessages`** (the
  `multiple prev values`, `non-syncable tables skipped`, `concurrent
  snapshot diffs`, `truncate`, `permissions change`, `schema change diff
  iteration throws SchemaChangeError`, `changelog iterator cleaned up on
  aborted iteration`, and `getRows filters out unique keys with NULL
  column values` scenarios). These hinge on the deferred `Diff` class
  above and on a zqlite-equivalent replicator test harness, neither of
  which exists in Rust yet. The closest analogues we *can* exercise
  today are covered by the new tests below (single-connection advance,
  concurrent snapshotters across the same file, empty diff, NULL-filter
  via `get_rows`).

## Consequence callouts (per translator brief)

1. **Leapfrog removed** — 1 SQLite connection per ViewSyncer instead of 2.
   Simpler locking and lower FD/handle pressure. The only downside is
   that while a reader is executing `ROLLBACK; BEGIN DEFERRED;` at the
   start of each `advance()`, a concurrent writer may momentarily block
   until the reader has re-acquired its read lock. This window is
   typically microseconds; in the TS leapfrog version the "spare"
   connection avoided even that momentary pause.
2. **`#currentSnapshot()` / `#previousSnapshot()` collapsed to a single
   `current()`.** Confirmed: we keep only `fn current(&self) -> &Snapshot`.
   There is no Rust equivalent of `#prev` any more.
3. **`SnapshotDiff` is recomputed each `advance()`**, not maintained
   incrementally. Every call to `advance()` materialises
   `entries: Vec<ChangeLogEntry>` via `changes_since(prev_version)` against
   the new snapshot. Callers must consume the diff before the next
   `advance()`; after the next advance, the `prev_version` boundary has
   moved and the previous diff reference is semantically stale
   (there is no `InvalidDiffError` check yet because the materialised
   `Vec` is self-contained).

## Cargo deps

No additions. Already-wired `rusqlite`, `thiserror`, `serde_json`, `tokio`,
`tempfile` (dev) cover everything. `scopeguard` was mentioned in the brief
but is not needed here: `Snapshot::reset_to_head` takes `self` by value
and emits `ROLLBACK` unconditionally before `BEGIN`; connection drop is
the backstop.

## Verification

- `cargo build -p zero-cache-sync`: **ok** (only pre-existing warnings in
  `cvr_store.rs` / `row_record_cache.rs`, none from `snapshotter.rs`).
- `cargo test -p zero-cache-sync`: **113 passed, 0 failed, 0 ignored**.
  Baseline was 105 (Wave 1 CVR suite); this port adds 8 new tests:
  - `snapshot_for_each_change_streams_cursor` (C14)
  - `snapshot_get_rows_filters_null_unique_keys` (NULL + OR workaround)
  - `snapshot_get_rows_empty_when_all_null`
  - `snapshotter_empty_diff`
  - `snapshotter_init_twice_is_error` (typed `AlreadyInitialized`)
  - `snapshotter_advance_before_init_is_error` (typed `NotInitialized`)
  - `snapshotter_advance_reuses_single_connection` (D2: no leapfrog)
  - `snapshotter_concurrent_snapshots_read_own_versions`
    (replaces the TS `concurrent snapshot diffs` test, scoped to what
    we can verify without the deferred `Diff` class)
  - plus: `async_snapshotter_with_current` (tokio integration via
    `with_current` closure)
- `docs/ts-vs-rs-comparison.md` row 13 updated (see line 37).

## Next-module dependencies unblocked

- **Row 14** (`pipeline-driver.rs` / `zql-ivm`) continues to compile
  unchanged — the `Snapshot`/`SnapshotDiff`/`AsyncSnapshotter`/
  `ChangeLogEntry`/`ResetPipelinesSignal`/`SET_OP`/`RESET_OP`/`TRUNCATE_OP`
  public surface that it consumes is preserved.
- A future translator of the full TS `Diff` iteration semantics can now
  build directly on `Snapshot::get_row`, `Snapshot::get_rows`, and
  `Snapshot::for_each_change`; the core lifecycle is done.
