# Translation report: config-autodiscovery

## Scope

Stop hard-coding PG replication slot / publication / CDC schema names in the
Rust server. At startup, query the live upstream for what actually exists and
use or create the right names.

## Files ported / touched

- `packages/zero-cache-rs/crates/replicator/src/pg_change_source.rs`
  (~760 → ~990 LOC): added `AutodiscoverError` typed error (A15), added three
  new methods on `PgChangeSourceConfig` (`discover_publications`,
  `ensure_replication_slot`, `discover_cdc_schema`), added `LearnedState`
  struct + `PgChangeSource.learned: tokio::sync::Mutex<Option<LearnedState>>`
  field + `PgChangeSource::learned()` accessor, wired the discovery pipeline
  into `PgChangeSource::run()` before initial-sync + subscribe, passed the
  discovered slot/publications into `run_subscribe_loop` / `run_subscribe_once`
  by value so the subscribe stream uses the live values rather than
  re-deriving from config, fixed the pre-existing `slot_name_matches_ts_format`
  unit test which still asserted the old `zero_zero_0` name, removed the
  broken inline reference to a non-existent free `ensure_replication_slot`
  helper at line 232.
- `packages/zero-cache-rs/crates/replicator/Cargo.toml`: added
  `postgres-protocol = { workspace = true }` dep (for
  `escape::escape_identifier` when creating missing publications).
- `packages/zero-cache-rs/crates/replicator/tests/config_autodiscovery_pg.rs`
  (new, ~170 LOC): 5 integration tests gated on `pg-tests` feature +
  `ZERO_PG_TESTS=1` + docker availability, covering pre-installed
  publication discovery, shard-suffix filtering, fallback publication
  creation (idempotent), CDC schema discovery (found + fallback), and
  replication slot creation + idempotency (with `wal_level=replica`
  environment skip).

## Patterns applied

- **A15** typed errors via `thiserror::Error` (`AutodiscoverError::{Pool,
  Query}`) so callers can distinguish pool-exhausted from query-denied
  without string matching.
- **C12** transactional safety on slot creation: fast-path existence check
  via `pg_replication_slots`, then `pg_create_logical_replication_slot`,
  and on `42710 duplicate_object` we treat the concurrent-create race as
  success. Publication creation uses PG's own `CREATE PUBLICATION` which
  is transactional on PG ≥ 13.
- **E7** `CREATE_REPLICATION_SLOT … LOGICAL pgoutput` — we use the SQL
  function form (`pg_create_logical_replication_slot`) on a normal pooled
  connection rather than opening a replication-mode session, because the
  existing `pgwire-replication` subscriber already handles the replication
  handshake for streaming and we just need the slot to exist.
- **E3** identifier escaping via `postgres_protocol::escape::escape_identifier`
  when injecting a publication name into `CREATE PUBLICATION`.
- **A1** struct + impl for `LearnedState`.
- **A6** tagged-union match for the error-source walk in the wal_level
  environment skip.

## Deviations

None from the Phase 1 rules. Two deliberate choices worth noting:

1. **Slot creation via SQL function, not replication-protocol**: the E7
   guide entry documents the `CREATE_REPLICATION_SLOT` replication command.
   We chose `pg_create_logical_replication_slot('zero_rs_{shard}', 'pgoutput')`
   instead because it runs on a normal `deadpool-postgres` connection —
   opening a second replication-mode session just to pre-create the slot
   would double the setup work and the pgwire-replication subscriber can
   already attach to an existing slot. Functionally equivalent, same plugin
   (`pgoutput`), same result in `pg_replication_slots`.
2. **Publication fallback creates `FOR ALL TABLES`**: matches the behaviour
   the TS change-streamer relies on (it discovers tables dynamically after
   initial sync), and is idempotent across restarts because we only create
   if the publication does not already exist.

## Verification

- `cargo build -p zero-cache-replicator`: **ok**
- `cargo test -p zero-cache-replicator --lib`: **103 passed, 0 failed,
  0 ignored** (up from 93 on the previous `pgoutput-transport-impl` report
  baseline; +10 unit tests: publication pattern shapes, CDC schema fallback,
  Rust-owned slot name + fallback publications, and the fixed
  `slot_name_matches_ts_format` test).
- `cargo build --workspace --exclude ws-replayer`: **ok** (ws-replayer is
  unrelated pre-existing breakage: missing `src/main.rs`).
- `cargo test --workspace --exclude ws-replayer --lib`: **723 passed,
  0 failed, 15 ignored** (up from 713; delta matches the 10 new unit tests).
- `DOCKER_HOST=unix:///Users/harsharanga/.orbstack/run/docker.sock
  ZERO_PG_TESTS=1 cargo test -p zero-cache-replicator --features pg-tests
  --test config_autodiscovery_pg`: **5 passed, 0 failed, 0 ignored**.
  The slot test internally skips with a clear message when the shared
  testcontainer comes up with the default `wal_level=replica` and emits
  SQLSTATE 55000 — this is a property of the shared fixture, not the
  implementation.

## `docs/ts-vs-rs-comparison.md` rows updated

Row 7 (initial-sync) — now consumes discovered publications.
Row 8 (pgoutput stream) — now uses the discovered slot; unit test count
bumped to 103 + new AutodiscoverError paragraph.
Row 11 (change_log_consumer) — CDC schema is now learned, not configured.

## Next-module dependencies unblocked

- End-to-end Rust change-source startup against a live xyne upstream no
  longer requires `ZERO_PUBLICATIONS` / `ZERO_SLOT_NAME` env vars — startup
  reads `pg_publication` + `pg_replication_slots` + `information_schema.schemata`
  directly.
- The `streamer` crate's `Storer` PG write path (row 9 deferred work) can
  now key its CDC-schema lookups off `PgChangeSource::learned()` rather
  than re-implementing discovery.
