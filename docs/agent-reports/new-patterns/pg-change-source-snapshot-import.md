# New pattern: SET TRANSACTION SNAPSHOT (snapshot import via transaction pool)

## Category
E (Wire / protocol) + C (Concurrency) — a transaction-pool of read-only
connections all attach to the snapshot name returned by
`CREATE_REPLICATION_SLOT`.

## Where used
- `packages/zero-cache/src/services/change-source/pg/initial-sync.ts:146-153`
  — within a read-only `runTx`, `await tx.unsafe('SET TRANSACTION SNAPSHOT
  \'${snapshot}\')` to get the published schema at the slot's
  `consistent_point`.
- `packages/zero-cache/src/services/change-source/pg/initial-sync.ts:328-358`
  — `startTableCopyWorkers()` imports the snapshot via `importSnapshot(...)`
  and spawns N `TransactionPool` workers all running at the same visibility.
- `packages/zero-cache/src/services/change-source/pg/backfill-stream.ts:220-254`
  — `createSnapshotTransaction()` opens a temporary replication slot, grabs
  the `snapshot_name`, runs a `TransactionPool` with `importSnapshot(...)`
  and then drops the slot. The tx remains open for the duration of the
  backfill stream.

## TS form
```ts
// initial-sync.ts
const {snapshot_name: snapshot, consistent_point: lsn} = slot;
const published = await runTx(sql, async tx => {
  await tx.unsafe(`SET TRANSACTION SNAPSHOT '${snapshot}'`);
  return getPublicationInfo(tx, publications);
}, {mode: Mode.READONLY});

// Fan out: N workers, each in their own READ-ONLY REPEATABLE READ tx,
// all attached to the same snapshot.
const {init} = importSnapshot(snapshot);
const tableCopiers = new TransactionPool(lc, READONLY, init, undefined, N);
tableCopiers.run(db);
```

## Proposed Rust form
```rust
// For a single read, a plain tokio-postgres transaction:
let tx = client.build_transaction()
    .isolation_level(IsolationLevel::RepeatableRead)
    .read_only(true)
    .start().await?;
tx.batch_execute(&format!("SET TRANSACTION SNAPSHOT '{}'", snapshot)).await?;
let published = get_publication_info(&tx, &publications).await?;
tx.commit().await?;

// For the N-way fan-out, use a `deadpool-postgres` pool of size N and have
// each worker, after acquiring a connection, BEGIN + SET TRANSACTION
// SNAPSHOT itself. Hold the connection for the lifetime of the COPY.
```

## Classification
Idiom-swap. The SQL is identical; what differs is the orchestration
(`TransactionPool` is an internal abstraction in `packages/zero-cache/src/db/
transaction-pool.ts`, not in this scope — but it's imported by both files).

## Caveats
- `SET TRANSACTION SNAPSHOT` must be the **first** statement of a
  `REPEATABLE READ` transaction (per the PG docs). That ordering must be
  preserved.
- The snapshot name is valid only while the exporting transaction (the
  replication-slot connection) is still open — `initial-sync.ts` keeps
  the replication session alive until all COPY workers finish.
  The backfill equivalent (`backfill-stream.ts`) drops the slot once all
  workers have imported.
- A "transaction pool" is effectively a worker pool with each worker
  holding a long-lived `REPEATABLE READ` transaction. In Rust, model it as
  a `Vec<JoinHandle>` each owning a connection and a `mpsc::Receiver<Task>`.

## Citation
- PostgreSQL docs — `SET TRANSACTION SNAPSHOT`:
  https://www.postgresql.org/docs/current/sql-set-transaction.html
- `tokio_postgres::Transaction` builder:
  https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.TransactionBuilder.html
- Snapshot export in the wire protocol via
  `pg_export_snapshot()` / `CREATE_REPLICATION_SLOT`:
  https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION
