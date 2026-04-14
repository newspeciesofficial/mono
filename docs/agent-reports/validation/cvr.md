# Validation report: cvr

## Scope
- `packages/zero-cache/src/services/view-syncer/cvr.ts` — 1102 LOC
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts` — 1351 LOC
- `packages/zero-cache/src/services/view-syncer/schema/cvr.ts` — 355 LOC
- `packages/zero-cache/src/services/view-syncer/schema/init.ts` — 258 LOC
- `packages/zero-cache/src/services/view-syncer/schema/types.ts` — 374 LOC
- `packages/zero-cache/src/services/view-syncer/ttl-clock.ts` — 15 LOC
- Total LOC: 3455

`cvr-context.ts` was listed as "if exists" in the prompt — it does not
exist in the repo (`view-syncer/` contains no file by that name). Not
read. `row-record-cache.ts` (435 LOC) is called via
`CVRStore.#rowCache` but is out of scope — I read only the call sites
(`getRowRecords`, `catchupRowPatches`, `executeRowUpdates`, `apply`,
`recordSyncFlushStats`, `flushed`, `hasPendingUpdates`, `clear`). A
separate validation pass should cover that file, which is where the
async-generator row-patch streaming (and the in-memory-vs-DB merge that
the view-syncer catchup relies on) actually lives.

## Summary
| metric | count |
|---|---|
| TS constructs used | 28 |
| Constructs covered by guide | 25 |
| Constructs NOT covered | 3 |
| Libraries imported | 6 |
| Libraries covered by guide | 6 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 1 |

## Constructs used

File paths are abbreviated: `cvr` = `cvr.ts`; `store` = `cvr-store.ts`;
`schema` = `schema/cvr.ts`; `types` = `schema/types.ts`;
`init` = `schema/init.ts`; `clock` = `ttl-clock.ts`.

| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class CVRUpdater { protected readonly _orig; protected _cvr; }` with subclass overrides | cvr:138-202; cvr:209-523; cvr:548-952 | A1 | covered |
| `class CVR*` uses `#private` method (`#deleteQueries`, `#trackExecuted`, `#trackRemoved`, `#lookupRowsForExecutedAndRemovedQueries`, `#flushQueries`, `#flushDesires`, `#checkVersionAndOwnership`, `#flush`, `#updateQueryFields`) | cvr:426, 675, 716, 626; store:230, 257, 717, 852, 926, 967 | A23 | covered |
| `abstract`-style inheritance: `CVRUpdater → CVRConfigDrivenUpdater/CVRQueryDrivenUpdater` | cvr:138, 209, 548 | A2 (redesign: composition + trait) | covered |
| `protected` + `super()` (`super(cvrStore, cvr, replicaVersion)`) | cvr:213, 566 | A2 | covered |
| Tagged union `QueryRecord = ClientQueryRecord \| CustomQueryRecord \| InternalQueryRecord` + `switch(query.type)` | types:229-235; types:326-371; store:110-150 | A6 | covered |
| String-literal union / `v.literal(...)` discriminant (`'client' \| 'custom' \| 'internal'`) | types:168, 213, 222 | A6 | covered |
| Generics with constraints (`function f<T extends RowKey>` via `CustomKeyMap<RowID, V>`) | cvr:550-553; store:184-187 | A8, A19 | covered |
| Struct update / spread `{...existing, patchVersion, refCounts: newRefCounts}` | cvr:939-943; cvr:155 | A13, A20 | covered |
| Optional chaining `?.` and nullish `??` | cvr:572, 779, 989, 1064; store:126, 401, 862, 947 | A11, A12, A21 | covered |
| Private `#field` / module-private | cvr:210, 549-555; store:163-194 | A23 | covered |
| Branded newtype via phantom `unique symbol` (`TTLClock`) | clock:1-16 | A24 (newtype) — *brand-via-phantom form NOT explicit in guide* | **NEW — see `new-patterns/cvr-ttl-clock-opaque-branded-type.md`** |
| `LexiVersion` (string, lexicographically ordered) | types:5; types:296-324 | A24 | covered |
| `structuredClone(cvr)` for mutable deep copy | cvr:155 | A13/A20 + prior `new-patterns/zql-ivm-joins-structured-clone.md` | covered (via prior new-pattern file) |
| `async` + `await` + `Promise.all` pipelining | store:276, 288, 1109, 1137 | A17 | covered |
| `async function*` / `AsyncGenerator<RowsRow[]>` for row-catchup streaming | store:637-651 (delegated to RowRecordCache) | A16, A22 | covered |
| Error hierarchy `ProtocolErrorWithLevel` + `ClientNotFoundError` / `OwnershipError` / `ConcurrentModificationException` / `InvalidClientSchemaError` / `RowsVersionBehindError` | store:1274-1351 | A15, F1, F2 | covered |
| `assert(cond, msg)` / `must(x, msg)` | cvr:160, 376, 474, 568; store:250 | A6 | covered |
| `Map<string, T>` / `Set<string>` / `CustomKeyMap<RowID, T>` / `CustomKeySet<RowID>` | cvr:549-553; store:184-195, 722 | A19, D1 (projection key) | covered |
| `sleep(ms)` polling loop for bounded retry (`load()`) | store:235-255 | B7 | covered |
| valita runtime schemas (`v.object`, `v.literal`, `v.union`, `v.Infer`, `v.parse`) | types:13-277; store:394-399 | prior `new-patterns/pg-change-source-valita-runtime-schema.md` | covered |
| Incremental schema-migration framework | init:12-258 | prior `new-patterns/pg-change-source-schema-migration-framework.md` | covered |
| OpenTelemetry spans (`startSpan`, `startAsyncSpan`, `tracer`) | cvr:218, 323, 431, 603, 676; store:236, 99 | `@opentelemetry/*` row | covered |
| `@rocicorp/logger` LogContext.withContext / `.debug?` / `.info?` / `.warn?` | cvr:280, 305; store:166, 244, 276, 322 | `@rocicorp/logger` row | covered |
| `performance.now()` timings | store:982, 1004, 1029, 1040, 1110, 1165, 1175 | B7 | covered |
| postgres.js tagged-template query with `${}` identifier interpolation (`tx(table)`) | store:301-316, 489-493, 546-548, 677-686 | `postgres` row | covered |
| **`json_to_recordset(${arrayOfObjects})` batched UPSERT** | store:741-792, 813-846, 878-923 | — | **NEW — see `new-patterns/cvr-json-to-recordset-batched-upsert.md`** |
| **`SELECT ... FOR UPDATE` + in-transaction version/ownership check, pipelined writes, throw-to-rollback** | store:926-957, 1026-1114, 367-377, 1257-1272 | — | **NEW — see `new-patterns/cvr-optimistic-row-lock-validate-then-write.md`** |
| **Version-bounded catchup query (`(afterVersion, upTo]`) over `queries`/`desires`/`rows` with lexi-sortable cookie string** | store:637-715; schema:141-144, 186-191, 316-318 | — (D1 doesn't fit) | **NEW — see `new-patterns/cvr-version-based-catchup-diff.md`** |
| `TransactionPool` (internal, `Mode.READONLY`) short-lived snapshot reads | store:668, 1216 | comparison row 4 (Port complete) | covered |
| `runTx(db, fn, {mode})` internal helper (postgres.js begin callback) | store:276, 1026 | `postgres` row | covered |
| `CREATE SCHEMA IF NOT EXISTS` + `CREATE TABLE` + `CREATE INDEX` + `ADD CONSTRAINT FOREIGN KEY ... ON DELETE CASCADE` | schema:27-351; init:26-214 | `postgres` row + migration-framework new-pattern | covered |
| `GIN` index on a JSONB column (`"refCounts"` → `rows USING GIN`) | schema:322-323 | `postgres` row | covered (standard SQL) |

## Libraries used

| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `postgres` (porsager/postgres.js) — types `MaybeRow`, `PendingQuery`, `Row` + tagged templates + `tx.cursor` (via row-record-cache) | `tokio-postgres` + `deadpool-postgres` + `postgres-types` | covered |
| `pg-format` (`ident(cvrSchema(shard))`) | `postgres-protocol::escape::escape_identifier` or `pg_escape` | covered |
| `@rocicorp/logger` | `tracing` + `tracing-subscriber` | covered |
| `@opentelemetry/api` + internal `otel/src/span.ts` | `opentelemetry` + `tracing-opentelemetry` | covered |
| `shared/src/valita.ts` (internal) | serde + `#[serde(...)]` — per prior new-pattern file | covered |
| `shared/src/bigint-json.ts` (internal stringify/parse preserving BigInt) | `serde_json` + `arbitrary_precision` + custom (de)serializers — guide Part 3 item 3 | covered (risk-flagged) |

No third-party package imported in this scope is outside Part 2 of the
guide.

## Risk-flagged items depended on

1. **BigInt-safe JSON (Part 3 item 3).** `schema/cvr.ts:4-8, 353-355`
   uses `stringify` from `shared/src/bigint-json.ts` when storing the
   JSONB `rowKey` column (so that bigint primary keys round-trip
   losslessly). `store.ts:4` re-imports `JSONObject` from the same
   module but the only place it crosses the DB boundary inside this
   scope is row-key bytes. The `arbitrary_precision` serde feature
   caveat (flatten/tag incompatibility) applies to whichever Rust
   struct wraps `rowKey`. `RowID::rowKey` in Rust should be
   `serde_json::Value` bound with `arbitrary_precision`, not a
   structured type.

No Part-3 item appears on the write path itself (no pgoutput, no
BEGIN CONCURRENT, no OTEL-churn-specific feature).

## Ready-to-port assessment

**Can be translated mechanically once the three new patterns are added
to the guide.** Nothing in this scope is blocked on a Risky / spec-heavy
item except for the shared BigInt-JSON policy, and that decision has
already been surfaced by previous validation rounds.

The three new-pattern files under `docs/agent-reports/new-patterns/`:

- `cvr-json-to-recordset-batched-upsert.md` — the TS uses postgres.js's
  ability to bind a JS array as a `json` parameter and unpack it with
  `json_to_recordset` for a one-statement bulk upsert. Rust has no
  drop-in; two equivalents are proposed (`Json<Value>` + the same SQL,
  or `UNNEST($1::text[], ...)`).
- `cvr-optimistic-row-lock-validate-then-write.md` — the CVR-flush
  pattern: `SELECT FOR UPDATE` on `instances` as the first statement of
  the transaction, throw to roll back on version/ownership mismatch,
  then pipeline all the writes. Different enough from C3/C4 to warrant
  its own entry.
- `cvr-version-based-catchup-diff.md` — the lexi-sortable cookie +
  `(afterVersion, upTo]` window pattern that backs catchup pokes.
  Comparison row 16 calls out that Rust currently re-hydrates instead of
  diffing; this is the exact TS shape that needs to land.

One addition to A24 (newtype) is also proposed in
`cvr-ttl-clock-opaque-branded-type.md`: TS "branded" types using a
phantom `unique symbol` translate to a `#[serde(transparent)]` +
`#[postgres(transparent)]` Rust newtype, distinct from the
plain-`struct UserId(String)` form the guide already shows.

**Cross-module dependencies.** Porting `cvr.rs` requires:
- `packages/zero-cache/src/types/lexi-version.ts` (LexiVersion) — row 1
  (complete).
- `packages/zero-cache/src/types/row-key.ts` (`rowIDString`,
  `normalizedKeyOrder`) — out of scope; small utility, should be part of
  the translation alongside this file.
- `packages/zero-cache/src/types/shards.ts` (`cvrSchema`,
  `upstreamSchema`, `ShardID`) — already used by change-source modules.
- `packages/zero-cache/src/db/migration.ts` — already documented in
  `new-patterns/pg-change-source-schema-migration-framework.md`.
- `packages/zero-cache/src/db/run-transaction.ts`
  (`runTx(db, fn, {mode})`) and
  `packages/zero-cache/src/db/transaction-pool.ts` (`TransactionPool`) —
  comparison row 4, already complete.
- `packages/zero-cache/src/services/view-syncer/row-record-cache.ts` —
  **must be ported before `cvr-store.rs` can be completed**, because
  the CVR flush fan-outs `executeRowUpdates`, `apply`, and the
  `catchupRowPatches` async generator through this cache. This is where
  comparison row 16's "re-hydrate instead of diffing" defect actually
  lives.

**What must be added to the guide before translation starts:**

1. New Part 1 entry (likely D7 or E7): "batched upsert via
   `json_to_recordset` or `UNNEST`", citing the new-pattern file.
2. New Part 1 entry under C: "optimistic concurrency with in-tx
   `FOR UPDATE` + version column, throw-to-rollback", citing the
   new-pattern file.
3. New Part 1 entry under D: "version-bounded catchup over persistent
   patch table (stateVersion:configVersion lexi cookie)", citing the
   new-pattern file.
4. Extend A24 with a one-line note: branded phantom-tagged TS types →
   `#[serde(transparent)]` + `#[postgres(transparent)]` newtype, citing
   the new-pattern file.

Once those four entries exist, this scope is a straight port. No Risky
item in Part 3 blocks it.
