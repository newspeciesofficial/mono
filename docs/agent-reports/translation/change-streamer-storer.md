# Translation report: change-streamer-storer

## Files ported

- `packages/zero-cache/src/services/change-streamer/storer.ts` (815) →
  `packages/zero-cache-rs/crates/streamer/src/storer.rs` (~720)
- `packages/zero-cache/src/services/change-streamer/broadcast.ts` (216) →
  `packages/zero-cache-rs/crates/streamer/src/broadcast.rs` (~320)
- `packages/zero-cache/src/services/change-streamer/subscriber.ts` (200) →
  `packages/zero-cache-rs/crates/streamer/src/subscriber.rs` (~430)
- `packages/zero-cache/src/services/change-streamer/schema/tables.ts` (364,
  `changeLog` DDL subset only) →
  `packages/zero-cache-rs/crates/streamer/src/schema.rs` (~200)
- `packages/zero-cache-rs/crates/streamer/src/lib.rs` updated to export the
  new modules; dev + prod dependencies extended with `postgres-protocol`,
  `bytes`, `async-stream`.
- Cascading call-site fixes in `streamer.rs` and `http.rs` because
  `Storer::new_noop()` now requires `(shard, replica_version)`.

## Patterns applied

- **A1** class with private fields → struct + impl (`Storer`, `Subscriber`,
  `Broadcast`).
- **A6** tagged union → enum + match (`QueueEntry`, `Downstream`,
  `SendResult`, `ErrorType`).
- **A15 / F1 / F2** typed errors via `thiserror` (`StorerError` with
  `QueueClosed`, `OwnershipLost`, `WatermarkTooOld`, `AutoReset`, `Io`).
- **B1 / C2** pub/sub with per-subscriber ACK — each push into a subscriber
  returns a `oneshot::Receiver<SendResult>`; `Broadcast` tracks pending via
  `Arc<Mutex<HashSet<String>>>` plus `Notify`.
- **B7** setTimeout/setInterval → `tokio::time::Instant` tracked inside
  `Broadcast::check_progress` (no spawned timer).
- **B11 Phase 1 decision**: `getDefaultHighWaterMark(false)` replaced by
  `pub const FLUSH_BYTES_THRESHOLD: usize = 16 * 1024;` and
  `StorerConfig::from_env()` reads `ZERO_CHANGE_STREAMER_BUDGET_MB`.
- **B12 Phase 1 decision**: heap-proportion backpressure dropped; instead a
  bounded `mpsc::channel(cap)` + an `Arc<AtomicU64>` byte counter + a
  `Notify`-backed `ReadyGate` gate `ready_for_more()`.
- **C1** bounded queue with explicit ACK — `QueueEntry::Ready(oneshot)`
  ports `allProcessed()`, and `ReadyHandle::wait()` ports `readyForMore()`.
- **C3** service lifecycle `start → drain → stop` — `Storer::run()` /
  `stop()` / in-loop `QueueEntry::Stop`.
- **A22** async-iterator consumption → tokio `while let Some(entry) =
  rx.recv().await`.
- **D8** batched UPSERT via `postgres_protocol::escape::escape_identifier`
  for dynamic schema names in the DDL module (`quoted_cdc_schema`).
- **E3** BigInt-safe JSON — `ChangeLogEntry.change` is `serde_json::Value`
  and we neither `#[serde(flatten)]` nor `#[serde(tag)]` over any
  BigInt-bearing payload; the storer accepts a pre-serialised `change_json:
  &str` so the upstream keeps control of the integer encoding (Risk #1).

## Deviations (cite Phase 1 rule)

- **D3 (single-process tokio):** The TS `Storer` uses `@rocicorp/resolver`
  and `Queue<QueueEntry>` (a JS promise-backed queue). The Rust port uses a
  bounded `tokio::sync::mpsc` + `oneshot`; no extra process boundary and no
  `@rocicorp/resolver`-style module-wide state.
- **Deferred / unit-testable gate:** The production PG write path
  (TransactionPool, concurrent `FOR UPDATE` on `replicationState`,
  `#trackBackfillMetadata` fan-out, `markResetRequired`,
  `terminateChangeDBLockHolders`, the `ensureReplicationConfig` migration
  engine, `assumeOwnership`, `getStartStreamInitializationParameters`) is
  not wired through `tokio_postgres` in this slice. `Storer::flush_tx`
  mirrors all rows into an `in_memory_log` so unit tests can exercise the
  commit / abort / rollback / catchup paths. A `PgPool` can be passed in
  via `Storer::new(...)` but the production path is a `TODO` per the
  "unit-testable only" phase constraint.
- **Ownership-change detection on commit:** TS acquires `FOR UPDATE` on
  `replicationState` inside the same pool as the change INSERTs. Not
  ported — `StorerError::OwnershipLost` is declared but not raised until
  the PG write path lands.
- **`ensureReplicationConfig` / `terminateChangeDBLockHolders` /
  `AutoResetSignal`:** Out of scope per the translator prompt (scoped only
  to the `changeLog` DDL + storer pipeline). DDL strings for
  `tableMetadata` and `backfilling` *are* emitted by `schema.rs` but the
  catchup + backfill-tracking logic is not ported.

## Verification

- `cargo build -p zero-cache-streamer`: ok (2 dead-code warnings for unused
  fields on `Completed` and `Storer::cdc_schema`; harmless, those are the
  PG write hooks).
- `cargo test -p zero-cache-streamer`: **52 passed; 0 failed; 0 ignored**
  (25 newly added: 9 storer, 6 subscriber, 4 broadcast, 6 schema; 27
  pre-existing in `client`, `http`, `streamer`).

### New tests (all passing)

- `storer::tests::store_buffers_and_commits_transaction`
- `storer::tests::abort_drops_in_flight_transaction`
- `storer::tests::rollback_drops_in_flight_and_resumes_catchup`
- `storer::tests::on_consumed_invoked_on_commit`
- `storer::tests::catchup_sends_history_to_subscriber`
- `storer::tests::purge_removes_watermarks_before_cutoff`
- `storer::tests::ready_for_more_engages_and_releases`
- `storer::tests::flush_bytes_threshold_is_16k`
- `storer::tests::config_default_budget_is_256mb`
- `subscriber::tests::backlog_flushes_on_set_caught_up`
- `subscriber::tests::duplicate_watermarks_are_skipped`
- `subscriber::tests::commit_updates_watermark_and_acked`
- `subscriber::tests::unsupported_update_table_metadata_is_skipped_pre_v5`
- `subscriber::tests::update_table_metadata_supported_on_v5`
- `subscriber::tests::num_pending_tracks_in_flight_messages`
- `broadcast::tests::empty_subscribers_marks_done_immediately`
- `broadcast::tests::done_resolves_when_all_subscribers_ack`
- `broadcast::tests::check_progress_waits_for_majority`
- `broadcast::tests::check_progress_completes_after_majority_and_padding`
- `schema::tests::cdc_schema_format`
- `schema::tests::quoted_cdc_schema_escapes_slash`
- `schema::tests::ddl_mentions_all_expected_tables`
- `schema::tests::change_log_uses_json_not_jsonb_for_change`
- `schema::tests::change_log_entry_round_trip`
- `schema::tests::app_name_constant`

Pre-existing tests in `streamer.rs` / `http.rs` / `client.rs` continue to
pass after the `Storer::new_noop(shard, replica_version)` signature change.

- `docs/ts-vs-rs-comparison.md` rows 9 and 10 updated to reflect the new
  wiring.

## Deferred items (not in scope)

- Production PG write path (tokio_postgres + deadpool-postgres) — needs
  testcontainers.
- `*.pg.test.ts` ports (storer.pg.test, broadcast.pg.test,
  subscriber.pg.test) — PG-dependent, deferred.
- `AutoResetSignal` escalation path and `markResetRequired` call on
  backup-replica divergence.
- `#trackBackfillMetadata` UPSERT fan-out for schema changes.
- `ensureReplicationConfig` 4-step initialisation + TRUNCATE-with-timeout.
- `terminateChangeDBLockHolders` race-against-timeout.
- `assumeOwnership` + `getStartStreamInitializationParameters` (reads
  `replicationState` / `backfilling` / `tableMetadata`).

## Next-module dependencies unblocked

- Row 11 (`services/replicator/incremental-sync.ts` → `change_log_consumer`)
  can now consume real `ChangeLogEntry` rows once a PG write path is wired
  in.
- Row 10 (change-streamer/service + HTTP) can swap its current noop /
  fake storer for the real one for integration testing.
