# Validation report: snapshotter

## Scope
- `packages/zero-cache/src/services/view-syncer/snapshotter.ts` — 578 LOC
- `packages/zero-cache/src/services/view-syncer/row-record-cache.ts` — 435 LOC
- Total LOC: 1013

Transitive modules inspected only to confirm that constructs used here map to
existing rows (`zqlite/src/db.ts`, `zero-cache/src/db/statements.ts`,
`zero-cache/src/db/transaction-pool.ts`, `zero-cache/src/db/run-transaction.ts`,
`zero-cache/src/services/replicator/schema/change-log.ts`,
`zero-cache/src/services/replicator/schema/replication-state.ts`). No TS files
outside the stated scope were read in full.

## Summary
| metric | count |
|---|---|
| TS constructs used | 22 |
| Constructs covered by guide | 20 |
| Constructs NOT covered | 2 |
| Libraries imported | 4 |
| Libraries covered by guide | 4 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 2 |

## Constructs used

All file:line samples are from `snapshotter.ts` unless prefixed
`row-record-cache.ts:`.

| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class Snapshotter` with `readonly` + `#private` fields (`#lc`, `#dbFile`, `#appID`, `#curr`, `#prev`) | snapshotter.ts:91, 92–97 | A1, A23 | covered |
| Inner `class Snapshot` with `static create(...)` factory and `readonly db` / `readonly version` fields | snapshotter.ts:266, 267, 293, 295 | A1 | covered |
| `class Diff implements SnapshotDiff` with `readonly` fields and a custom `[Symbol.iterator]()` returning `Iterator<Change>` with a `return()` hook for resource cleanup | snapshotter.ts:389, 412, 424, 537 | A1, A22 | covered |
| `interface SnapshotDiff extends Iterable<Change>` | snapshotter.ts:229 | A5, A22 | covered |
| Custom exception hierarchy: `class ResetPipelinesSignal extends Error`, `class InvalidDiffError extends Error` with `readonly name = …` | snapshotter.ts:258, 574 | A15, F1 | covered |
| `try / catch / throw new …` + `if (e instanceof …)` re-raise pattern inside iterator body | snapshotter.ts:530, row-record-cache.ts:184, 277 | A15, F1, F2 | covered |
| Tagged union over `RowChange.op` values (`RESET_OP` / `TRUNCATE_OP` / `SET_OP`) with `switch`-like `if` cascade and `ResetPipelinesSignal` throws on unrecoverable ops | snapshotter.ts:435, 441, 472 | A6 | covered |
| `assert(cond, msg\|() => msg)` + `must(x)` | snapshotter.ts:117, 134, 184, 284, 463, row-record-cache.ts:239, 250 | A6 (exhaustiveness / guards) | covered |
| Optional chaining (`this.#curr?.db.db.close()`, `changes.return?.(undefined)`, `this.#lc.debug?.(…)`) | snapshotter.ts:203, 204, 418, row-record-cache.ts:179, 272 | A11, A21 | covered |
| Nullish coalescing `a ?? b` and `a ??= b` | snapshotter.ts:464, 559, row-record-cache.ts:— (n/a) | A12 | covered |
| Object spread in AsyncGenerator intermediate rows (`{...cond, rowKey}`) — actually absent; construction uses plain `{}` literals | snapshotter.ts:517 | A13 / A20 | covered (no spread; plain literal) |
| `async function*` + `yield*` (`catchupRowPatches`) with `.cursor(N)` page yielding | row-record-cache.ts:307, 353 | A16, A22 | covered |
| `for await (const rows of <sql>.cursor(5000))` | row-record-cache.ts:169 | A16 | covered |
| Generic `TransactionPool(lc, Mode.READONLY).run(this.#db)` + `reader.processReadTask(tx => …)` | row-record-cache.ts:330, 333 | C1 (bounded mpsc/backpressure model), covered by internal `transaction-pool.ts` (row 4 of comparison) | covered |
| `runTx(db, tx => {…}, {mode: READ_COMMITTED})` (bigint-safe write-back flush) | row-record-cache.ts:244 | internal, row 4 of comparison | covered |
| `Map<string, LiteAndZqlSpec>` / `Set<string>` (syncableTables, allTableNames) / `CustomKeyMap<RowID, RowRecord>` | snapshotter.ts:176, 177, row-record-cache.ts:100, 166 | A10 (Record), A19 | covered |
| `resolver<T>()` (`@rocicorp/resolver`) with manual `.resolve` / `.reject` / `.promise` attach-and-ignore handler | row-record-cache.ts:160, 182, 228 | crate table: `@rocicorp/resolver` → `tokio::sync::oneshot` | covered |
| Injectable `setTimeout` (`readonly #setTimeout: typeof setTimeout`) called with 0-ms trampoline to start a background flush task | row-record-cache.ts:97, 124, 233 | B7 | covered |
| `performance.now()` + `Date.now()` for flush / scan timings | row-record-cache.ts:159, 242, 263, 318 | B7 (std::time::Instant) | covered |
| OpenTelemetry metrics: `getOrCreateHistogram`, `getOrCreateCounter` (`sync.cvr.flush-time`, `sync.cvr.rows-flushed`) with `record(elapsed, {attr})` / `.add(n)` | row-record-cache.ts:105, 111, 136, 148 | `@opentelemetry/*` metrics row of Part 2 | covered |
| `valita.parse(value, schema)` on each change-log row (runtime schema validation) | snapshotter.ts:434 | E1/E2 (serde custom Deserialize) | covered |
| `JSON.parse` / BigInt-safe `stringify` (`shared/bigint-json.ts`) inside error messages | snapshotter.ts:486 | D3 / E3 (BigInt JSON) | covered |
| SQLite sandbox: `BEGIN CONCURRENT` on a `wal2` journal db, `.pragma('synchronous = OFF')`, `.pragma('cache_size = -KiB')`, manual `rollback()`-and-rewrap connection on every tick ("leapfrog" of two `Database` handles) | snapshotter.ts:266–307, 383 | Part 3 Risk #2 (BEGIN CONCURRENT not in rusqlite) + `@rocicorp/zero-sqlite3` row of Part 2 | covered (risk-flagged) |
| `StatementRunner.statementCache.get(sql)` / `cached.statement.iterate(…)` / `cached.statement.safeIntegers(true)` + `try/finally` return-to-pool | snapshotter.ts:320, 325, 339, 344, 367, 372, 379 | internal (`db/statements.ts`, row 4 of comparison) wrapping `@rocicorp/zero-sqlite3` → `rusqlite` | covered |
| Manual `Iterator.next/return` construction (no generator), propagating iterator-protocol cleanup on throw by wrapping the body in `try/catch { cleanup(); throw }` and on `break` via the `return()` hook | snapshotter.ts:424–541 | A22 | covered |
| Postgres-side cursor streaming `tx<RowsRow[]>\`SELECT …\`.cursor(10000)` and `this.#db<RowsRow[]>\`…\`.cursor(5000)` | row-record-cache.ts:169, 353 | — | **NEW — see `new-patterns/snapshotter-postgres-cursor-streaming.md`** |
| `json_to_recordset` bulk upsert via parameterised JSON payload in `executeRowUpdates()` | row-record-cache.ts:409–425 | — | **NEW — see `new-patterns/snapshotter-json-to-recordset-bulk-upsert.md`** |

## Libraries used

| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `@rocicorp/logger` | `tracing` + `tracing-subscriber` | covered |
| `@rocicorp/resolver` | `tokio::sync::oneshot` | covered |
| `postgres` (porsager — tagged-template query, `cursor(N)`, `PendingQuery`, `Row`) | `tokio-postgres` + `deadpool-postgres` | covered (but streaming cursor idiom itself not in catalogue — see new-patterns) |
| `@rocicorp/zero-sqlite3` (via `zqlite/src/db.ts`, `table-source.ts`) | `rusqlite` (bundled) — **Part 3 Risk #2** for `BEGIN CONCURRENT` | covered (risk-flagged) |
| OpenTelemetry metrics (via `observability/metrics.ts`) | `opentelemetry` + `opentelemetry_sdk` + `opentelemetry-otlp` | covered |
| `../../../../shared/src/valita.ts` (runtime schema) | serde custom Deserialize (E1/E2) | covered (internal; not a 3rd-party crate) |
| `../../../../shared/src/bigint-json.ts` (BigInt-safe `stringify`) | `serde_json` + `arbitrary_precision` (see Part 3 Risk #3) | covered (risk-flagged) |

No third-party package imported in these two files is outside Part 2 of the
guide. All remaining imports (`shared/*`, `zero-protocol/*`, `zero-types/*`,
`zqlite/*`, in-package siblings) are internal modules covered by other rows of
`ts-vs-rs-comparison.md` (notably row 4 — transaction pool / pg utils, row 6 —
snapshotter reader pool, row 13 — snapshotter itself, row 16 — cvr-store /
schema).

## Risk-flagged items depended on

1. **`BEGIN CONCURRENT` on SQLite (Part 3 Risk #2).** The entire leapfrog
   design in `Snapshotter` requires the replica db to be in `wal2` journal
   mode and to support `BEGIN CONCURRENT`:
   - The `Snapshot` constructor runs `db.beginConcurrent()` followed by a
     read of `getReplicationState` to materialise the snapshot
     (snapshotter.ts:298–306).
   - `Snapshot.create` asserts `journal_mode === 'wal2'` (snapshotter.ts:284).
   - `resetToHead()` does `db.rollback()` and immediately rewraps the same
     connection with another `beginConcurrent()` to "leapfrog" to head
     (snapshotter.ts:383–386).
   - `advanceWithoutDiff()` alternates the two connections (`#curr`/`#prev`),
     reusing the previous rolled-back connection as the next head snapshot
     (snapshotter.ts:183–196).

   The Rust port (comparison row 13) currently runs without BEGIN CONCURRENT
   and without leapfrog — a single shared snapshot. The guide already flags
   that upstream `rusqlite`/SQLite do not ship this feature and that a custom
   `libsqlite3-sys` build (or a design drop-down to standard WAL + deferred
   transactions) is required. This module cannot be ported 1:1 until that
   decision is made.

2. **pipeline-driver / IVM (comparison row 14 — Stub).** `SnapshotDiff`
   consumers in view-syncer drive the IVM operator graph by replaying the
   returned `Change` stream against `prev` and `curr` table sources. The
   diff's correctness invariant — that each row appears at most once except
   around TRUNCATE, which is converted to a `ResetPipelinesSignal` — is only
   exercised once IVM is real. No new construct, but the Rust port of this
   module cannot be validated E2E until row 14 lands.

Additional notable semantics (already covered but worth flagging to the
translator):

- `numChangesSince` / `changesSince` read from `_zero.changeLog2` and require
  ordering by `(stateVersion ASC, pos ASC)` — identical SQL must be used on
  the Rust side (snapshotter.ts:310–323).
- `checkThatDiffIsValid` (snapshotter.ts:544–571) defends against consumers
  iterating after a subsequent `advance()` — it looks at
  `prevValue[_0_version]` / `curr.version` / `stateVersion` to detect a
  snapshot that has moved. In Rust the same guard maps to a generation
  counter on the `Diff` struct; drop-on-advance enforces via the borrow
  checker if `Diff` borrows from `Snapshotter`.
- `getRows` skips any `key` that contains a NULL column (snapshotter.ts:
  359–364). This is a hard correctness-and-perf fix already noted in the
  repo `CLAUDE.md` ("SQLite: NULL + OR = Full Table Scan"). The Rust port
  must preserve this filter literally.
- The `Diff` iterator's statement-cache lifecycle (checkout on
  `[Symbol.iterator]()`, `return` on explicit break / throw / end) is the
  only place in the module that is not trivially idiomatic in Rust; the
  natural translation is a RAII guard struct that calls
  `StatementCache::return(…)` in `Drop`, wrapped in an
  `impl Iterator for Diff`.

## Ready-to-port assessment

**Blocked on Part 3 Risk #2 (BEGIN CONCURRENT) and on comparison row 14 (IVM
pipeline driver).** Every language construct used in `snapshotter.ts` and
`row-record-cache.ts` maps onto an existing guide section (A1, A5, A6, A10,
A11, A12, A15, A16, A22, A23; B7; C1; D3; E1/E2; F1/F2). Every third-party
crate they transitively require has a Part 2 row.

Two genuinely new patterns came out of this scope and need guide entries
before the module can be translated mechanically:

1. **`snapshotter-postgres-cursor-streaming.md`** — the porsager `postgres`
   package's `tagged-template-string\`…\`.cursor(N)` idiom does not
   correspond to any single call in `tokio-postgres`. The closest equivalent
   is to bind a server-side Portal inside a transaction and drive it with
   repeated `execute_portal(n).await` calls, or to wrap that in a `Stream`
   adapter. Adding this to Part 2 (next to the `postgres` row) or as a new
   row in Part 1 Section B ("streaming query results with server-side
   cursor") would unblock both `row-record-cache.catchupRowPatches` and
   `RowRecordCache.#ensureLoaded`.

2. **`snapshotter-json-to-recordset-bulk-upsert.md`** — `executeRowUpdates`
   relies on a single `INSERT … SELECT … FROM json_to_recordset($1) AS x(…)
   ON CONFLICT … DO UPDATE …` to perform a variable-width batched upsert.
   In `tokio-postgres` the equivalent is either the same SQL with a
   `Json(Vec<RowJson>)` parameter, or `COPY FROM STDIN BINARY`. This is a
   Postgres SQL idiom rather than a TS construct, but since the crate row
   for `postgres`/`tokio-postgres` does not mention bulk-write idioms at all,
   naming the canonical Rust form avoids the translator reinventing it per
   table.

Outside those two, no additions to Part 1, Part 2, or Part 3 are required.
The `BEGIN CONCURRENT` requirement is already Part 3 Risk #2, and the
leapfrog architecture itself reduces in Rust to "two `rusqlite::Connection`
handles, rotate on each tick, call `BEGIN CONCURRENT` + read + `ROLLBACK`"
once the build-time dependency on a begin-concurrent-enabled libsqlite3 is
resolved. Behaviourally, everything else in the module is a mechanical port.
