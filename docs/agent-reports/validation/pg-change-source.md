# Validation report: pg-change-source

## Scope
- `packages/zero-cache/src/services/change-source/pg/change-source.ts` — 1405
- `packages/zero-cache/src/services/change-source/pg/initial-sync.ts` — 614
- `packages/zero-cache/src/services/change-source/pg/backfill-stream.ts` — 334
- `packages/zero-cache/src/services/change-source/pg/backfill-metadata.ts` — 18
- `packages/zero-cache/src/services/change-source/pg/decommission.ts` — 43
- `packages/zero-cache/src/services/change-source/pg/lsn.ts` — 45
- `packages/zero-cache/src/services/change-source/pg/schema/ddl.ts` — 390
- `packages/zero-cache/src/services/change-source/pg/schema/init.ts` — 283
- `packages/zero-cache/src/services/change-source/pg/schema/published.ts` — 312
- `packages/zero-cache/src/services/change-source/pg/schema/shard.ts` — 476
- `packages/zero-cache/src/services/change-source/pg/schema/validation.ts` — 73
- Total LOC: **3993**
- Note: `replication-messages.ts` does not exist in this tree. The
  `logical-replication/` subdirectory is out of scope (owned by another
  worker); only types/functions imported from it are noted below.

## Summary
| metric | count |
|---|---|
| TS constructs used | 28 |
| Constructs covered by guide | 24 |
| Constructs NOT covered | 4 |
| Libraries imported | 6 |
| Libraries covered by guide | 6 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 2 |

## Constructs used
| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class Foo { #field … }` with private fields | `change-source.ts:249-267, 555-560, 626-651` | A1 / A23 | covered |
| Tagged union + `switch` on `msg.tag` | `change-source.ts:704-788` | A6 | covered |
| `async function*` + `for await (…)` (backfill stream; main replication loop) | `backfill-stream.ts:57,126,172`, `change-source.ts:339` | A16 | covered |
| `Promise.all([…])` | `initial-sync.ts:185-191, 201-207, 459-464`, `backfill-stream.ts:135-140` | A17 | covered |
| Struct-spread `{…msg, relation: …}` | `change-source.ts:710, 724-731, 749-751` | A13 / A20 | covered |
| Optional chaining on calls / fields | `change-source.ts:284, 320-321, 341, 425-430, 465`, `validation.ts:32,47` | A11 / A21 | covered |
| `??` default | `change-source.ts:716, 728, 742`, `validation.ts:32` | A12 | covered |
| Custom Error subclasses (`AutoResetSignal`, `AbortError`, `UnsupportedSchemaChangeError`, `MissingEventTriggerSupport`, `ShutdownSignal`, `UnsupportedTableSchemaError`, `SchemaIncompatibilityError`) | `change-source.ts:180, 192-196, 1364-1404`, `validation.ts:67-73`, `backfill-stream.ts:115` | A15 / F1 | covered |
| `throw err` + `try/catch` + `?:`-style propagation | `change-source.ts:392-402, 655-680, 834-853`, `backfill-stream.ts:102-123` | A15 / F1 / F2 | covered |
| `setTimeout(…)` + `clearTimeout(…)` for delayed work | `change-source.ts:633, 798, 844-850` | B7 | covered |
| Node `Writable` + `pipeline()` for COPY streaming | `initial-sync.ts:7-8, 565-606` | B2 | covered |
| `Buffer` chunk handling / `TextDecoder` | `change-source.ts:1081-1084`, `initial-sync.ts:571-604`, `backfill-stream.ts:172-185` | B3 | covered |
| `bigint`/`BigInt` arithmetic on LSNs | `lsn.ts:22-44`, `change-source.ts:304, 617-619, 664-665` | D3 / E5 | covered |
| `Map<K,V>` + `Set<T>` + `symmetricDifferences/intersection/equals` | `change-source.ts:897-905, 926-944, 1192-1217`, `published.ts:283-298`, `initial-sync.ts:310-312` | D generic / A | covered |
| Exponential / fixed retry with jitterless `sleep()` | `change-source.ts:525-551` | C6 | covered |
| HashMap-backed schema-to-diff computation (`specsByID`, `columnsByID`) | `change-source.ts:1231-1238, 1320-1330` | A19 / D | covered |
| Atomic counter / progress (`status.rows++`, `totalRows/totalBytes`) | `backfill-stream.ts:140-200`, `initial-sync.ts:466-541` | D6 | covered |
| Parameterised Postgres queries via `sql\`…\`` and `sql.unsafe(...)` | pervasive — e.g. `change-source.ts:210-234, 457-501`, `shard.ts:261-293, 333-356`, `initial-sync.ts:121-137, 266-283` | Part 2 `postgres` | covered |
| `pg_logical_emit_message` + plpgsql event triggers (upstream side) | `ddl.ts:143-313, 327-369` | — | **NEW — see `new-patterns/pg-change-source-pg-event-triggers.md`** |
| `CREATE_REPLICATION_SLOT "..." LOGICAL pgoutput` on a replication-mode session | `initial-sync.ts:84-87, 371-383`, `backfill-stream.ts:226-254` | E4 (adjacent) | **NEW — see `new-patterns/pg-change-source-create-replication-slot.md`** |
| `COPY (…) TO STDOUT` + TSV parser + batched INSERT | `initial-sync.ts:563-606`, `backfill-stream.ts:151-194` | — | **NEW — see `new-patterns/pg-change-source-copy-to-stdout.md`** |
| `SET TRANSACTION SNAPSHOT '<name>'` + `importSnapshot` fan-out worker pool | `initial-sync.ts:146-153, 328-358`, `backfill-stream.ts:230-242` | — | **NEW — see `new-patterns/pg-change-source-snapshot-import.md`** |
| `valita` schema objects + `v.parse(…, schema, 'passthrough')` + `v.Infer` | `ddl.ts:20-118`, `published.ts:177-233`, `shard.ts:222-236`, `change-source.ts:1086`, `backfill-metadata.ts:6-18`, `backfill-stream.ts:277-313` | — | **NEW — see `new-patterns/pg-change-source-valita-runtime-schema.md`** |
| Numbered, incremental upstream schema migrations via `runSchemaMigrations` + `AutoResetSignal` | `init.ts:32-257` | — | **NEW — see `new-patterns/pg-change-source-schema-migration-framework.md`** |
| `class`-implements-interface (`Listener`, `ChangeSource`, `Sink<bigint>`) | `change-source.ts:249, 555` | A5 | covered |
| JSON `JSON.parse` / `JSON.stringify` on logs + event bodies | `change-source.ts:464-466, 489-494, 1080-1086` | E1 / Part 2 `json-custom-numbers` | covered |
| `Object.fromEntries` / `Object.entries` / `mapValues` | `change-source.ts:952-957, 1324-1330, 1333-1342`, `shard.ts:261-264` | A19 / A | covered |
| `process.versions.node` parse | `initial-sync.ts:347-355` | B6 | covered |

## Libraries used
| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `postgres` (porsager/postgres.js) — regular queries, replication-mode sessions, `sql.unsafe(...)`, `PostgresError`, `savepoint` | `tokio-postgres` + `deadpool-postgres` + `postgres-native-tls` (+ `postgres-protocol` for escape) | covered |
| `pg-format` (`literal`) | `postgres-protocol::escape::escape_literal` / `pg_escape` | covered |
| `@drdgvhbh/postgres-error-codes` (`PG_ADMIN_SHUTDOWN`, `PG_OBJECT_IN_USE`, `PG_INSUFFICIENT_PRIVILEGE`, `PG_CONFIGURATION_LIMIT_EXCEEDED`, `PG_UNDEFINED_TABLE`, `PG_UNDEFINED_COLUMN`) | `sqlstate` crate or hand-rolled constants | covered |
| `@rocicorp/logger` (`LogContext`, `.withContext(k,v)`, `.info?.`) | `tracing` + `tracing-subscriber` | covered |
| `node:stream` + `node:stream/promises` (`Writable`, `pipeline`) | `tokio::io::AsyncRead/Write` + `tokio::io::copy` or `futures::StreamExt` / `SinkExt::forward` | covered |
| `node:os` (`platform()`) | `std::env::consts::OS` | covered (B6 / stdlib) |

No third-party dependency in this scope is outside Part 2's crate table.

## Risk-flagged items depended on
- **E4 — pgoutput parser.** `change-source.ts:78-82` imports `Message`,
  `MessageMessage`, and `MessageRelation` from
  `./logical-replication/pgoutput.types.ts`, and uses `subscribe()` from
  `./logical-replication/stream.ts:305-311` to get the async message
  stream and ack channel. Every transaction body processed by
  `ChangeMaker.makeChanges()` (`change-source.ts:654-788`) assumes the
  pgoutput parser is fully correct (Begin / Relation / Insert / Update /
  Delete / Truncate / Message / Type / Origin / Commit). Until the
  pgoutput parser exists, nothing in this scope can be exercised
  end-to-end.
- **Risk: replication-mode `CREATE_REPLICATION_SLOT` + `SET TRANSACTION
  SNAPSHOT` coupling.** Initial sync opens a replication session, creates
  a slot to obtain `(consistent_point, snapshot_name)`, and then runs a
  pool of N read-only transactions in a *non-replication* connection that
  all attach to the same snapshot. The replication session must stay
  alive until all COPY workers have finished. Breaking that ordering
  breaks initial sync silently (COPY workers would see post-commit data).

## Ready-to-port assessment
All eleven files in scope were read in full. Every third-party dependency
maps to a crate already listed in guide Part 2. The only genuinely new
items are internal conventions (valita schemas, the in-tree migration
framework) and Postgres wire-protocol subcommands that sit *next to* the
pgoutput parser risk (E4) but are themselves simple to send via
`tokio_postgres::Client::simple_query` / `copy_out`. I have filed five
new-pattern files under `new-patterns/` for those.

The module **cannot be ported in isolation.** The main replication loop
(`change-source.ts:334-402`) pulls messages from `subscribe()` in the
`logical-replication/` subdir, which contains the pgoutput parser and the
Standby-status-update keepalive loop. Until that parser is either hand-rolled
or supplied by `pgwire-replication`, this module has nothing to consume and
no way to be integration-tested end-to-end.

Prerequisites before translation can start:
1. pgoutput parser + streaming replication loop + Standby keepalive
   (guide Part 3 risk #1; produced by the `logical-replication/` worker).
2. A Rust port of the internal `TsvParser` (`packages/zero-cache/src/db/
   pg-copy.ts`, out of scope) — required for both `initial-sync.rs` and
   `backfill-stream.rs`.
3. Ports of the internal abstractions imported by this module but not in
   scope: `TransactionPool` / `importSnapshot` (`db/transaction-pool.ts`),
   `runTx` / `pgClient` (`db/run-transaction.ts`, `types/pg.ts`),
   `Database` (`zqlite/src/db.ts`), `StatementRunner`
   (`db/statements.ts`), `ChangeStreamMultiplexer` / `BackfillManager`
   (`change-source/common/`), `ReplicationStatusPublisher`
   (`services/replicator/replication-status.ts`), and the
   `migration.ts` schema-migration runner.
4. Additions to `docs/rust-translation-guide.md` summarising the five new
   patterns above (valita → serde; CREATE_REPLICATION_SLOT via
   simple_query on a replication connection; COPY TO STDOUT streaming;
   SET TRANSACTION SNAPSHOT fan-out; numbered-incremental schema
   migrations with an `AutoResetSignal` escape hatch).

Once 1–4 are in place, the module itself is mechanically translatable: the
logic is dominated by pattern matching on pgoutput message tags and diffing
schema snapshots — both of which fit the guide's A6 / D patterns cleanly.
