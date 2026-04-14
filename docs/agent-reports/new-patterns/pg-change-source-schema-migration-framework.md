# New pattern: versioned, incremental schema migrations on the upstream DB

## Category
A (Language) / Library-adjacent. The scope uses an internal framework
(`packages/zero-cache/src/db/migration.ts`, not in scope) to run numbered
migrations against the upstream Postgres shard schema.

## Where used
- `packages/zero-cache/src/services/change-source/pg/schema/init.ts:32-73`
  — `ensureShardSchema()` and `updateShardSchema()` call
  `runSchemaMigrations(lc, 'upstream-shard-{appID}', upstreamSchema(shard),
  db, initialSetup, getIncrementalMigrations(...))`.
- `packages/zero-cache/src/services/change-source/pg/schema/init.ts:80-257`
  — the migration map is a `Record<number, Migration>` with version jumps
  that either (a) throw `AutoResetSignal` to force a resync, or (b) execute
  plpgsql / SQL to upgrade triggers and tables.
- `packages/zero-cache/src/services/change-source/pg/schema/init.ts:260-266`
  — `CURRENT_SCHEMA_VERSION` derived by `Object.keys(...).reduce(max)`.

## TS form
```ts
await runSchemaMigrations(
  lc,
  'upstream-shard-' + appID,
  upstreamSchema(shard),
  db,
  {migrateSchema: (lc, tx) => setupTablesAndReplication(lc, tx, shard),
   minSafeVersion: 1},
  {
    6:  {migrateSchema: async (lc, sql) => { ... }},
    7:  {migrateSchema: async (lc, sql) => { ... }},
    // ...
    18: {migrateSchema: async (lc, sql) => { ... }},
  },
);
```

## Proposed Rust form
```rust
// Typical Rust idiom for this is `refinery` or `sqlx::migrate!`, but those
// expect static SQL files. Here the migration bodies are Rust functions
// (they call setupTriggers() etc.), so model it as:

type Migrator = for<'t> fn(&'t LogContext, &'t tokio_postgres::Transaction<'t>,
                          &'t Shard) -> BoxFuture<'t, Result<()>>;

struct MigrationMap { initial: Migrator, incremental: BTreeMap<u32, Migrator> }

async fn run_schema_migrations(..., map: MigrationMap) -> Result<()> {
    let tx = client.transaction().await?;
    // read current version from the shard's `schemaVersions` table,
    // lock-then-apply each pending migration, then commit.
    ...
}
```

## Classification
Redesign. `refinery`/`sqlx::migrate` do not fit because migrations are
written as Rust functions (some run plpgsql, some throw `AutoResetSignal`).
A small hand-rolled dispatcher (~100 LOC) backed by a `schemaVersions`
table on upstream is the right shape.

## Caveats
- Some migrations **deliberately throw `AutoResetSignal`** to force the
  supervisor to wipe the replica and run initial-sync again
  (`init.ts:67-69, 86-91`). The dispatcher must surface a typed
  "needs-reset" error distinct from a real failure.
- Several migrations run `setupTriggers(lc, sql, shard)` — i.e. they
  reinstall the plpgsql event triggers (see `pg-event-triggers` new-pattern).
- `minSafeVersion` is used to refuse to run an old binary against a
  newer schema. Preserve that semantic.

## Citation
- `refinery` (Rust migration runner) — shows the general shape but not
  adopted here because migrations are code, not SQL files:
  https://docs.rs/refinery/latest/refinery/
- `sqlx::migrate!`:
  https://docs.rs/sqlx/latest/sqlx/macro.migrate.html
