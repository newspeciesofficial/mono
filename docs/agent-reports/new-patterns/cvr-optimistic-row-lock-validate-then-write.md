# New pattern: optimistic row-lock + in-transaction version/ownership check

## Category
C (Concurrency) / D (Data). Not currently in the guide's C3/C4 lifecycle or
C9 cancellation rows.

## Where used
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:926-957` —
  `#checkVersionAndOwnership(tx, expectedCurrentVersion, lastConnectTime)`
  runs `SELECT "version", "owner", "grantedAt" FROM ...instances
  WHERE "clientGroupID" = ${id} FOR UPDATE` as the first statement of the
  write transaction; throws `OwnershipError` or
  `ConcurrentModificationException` on mismatch.
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:1026-1114` —
  the `#flush` method wraps all CVR writes in a single `runTx` whose
  async callback (i) runs the FOR-UPDATE check, (ii) queues every
  write (instance, queries, desires, rows), (iii) pipelines them with
  `Promise.all`. Throwing inside the begin callback causes postgres.js to
  `ROLLBACK` — nothing is committed.
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:256-378` —
  the `load()` path detects a mismatch between `instances.version` and
  `rowsVersion.version` and fires a non-awaited
  `UPDATE ... SET owner = ..., grantedAt = ...` gated on
  `grantedAt <= to_timestamp(${lastConnectTime / 1000})`, signalling the
  current owner to stop writing.
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:1257-1272` —
  `checkVersion(tx, schema, id, expected)` is the read-only variant used
  by `catchupConfigPatches` (cvr-store.ts:671) to ensure the snapshot
  read sees the CVR at the expected version.

## TS form
```ts
async #flush(expected: CVRVersion, cvr: CVRSnapshot, lastConnectTime: number) {
  return await runTx(this.#db, async tx => {
    // 1. Acquire a row-level X-lock on instances and validate state.
    await this.#checkVersionAndOwnership(lc, tx, expected, lastConnectTime);
    // (Throws OwnershipError or ConcurrentModificationException → ROLLBACK.)

    // 2. Queue writes; the lock prevents another writer from advancing
    //    cvr.version under us.
    const writes: PendingQuery<Row[]>[] = [
      ...this.#writes,
      this.#pendingInstanceWrite!(tx, lastConnectTime),
      ...this.#flushQueries(tx, lc),
      this.#flushDesires(tx, lc),
      ...this.#rowCache.executeRowUpdates(tx, cvr.version, ..., 'allow-defer', lc),
    ];
    // 3. Pipeline all writes; postgres.js sends them without waiting for
    //    each round-trip because we are inside begin().
    return await Promise.all(writes);
  }, {mode: Mode.READ_COMMITTED});
}

async #checkVersionAndOwnership(lc, tx, expected, lastConnectTime) {
  const [row] = await tx`
    SELECT "version", "owner", "grantedAt" FROM ${this.#cvr('instances')}
      WHERE "clientGroupID" = ${this.#id}
      FOR UPDATE`;
  const {version, owner, grantedAt} = row ?? {version: EMPTY, owner: null, grantedAt: null};
  if (owner !== this.#taskID && (grantedAt ?? 0) > lastConnectTime) {
    throw new OwnershipError(owner, grantedAt, lastConnectTime);
  }
  if (version !== versionString(expected)) {
    throw new ConcurrentModificationException(versionString(expected), version);
  }
}
```

Key properties:

- The lock is held for the duration of the transaction; it serialises
  concurrent writers at the per-clientGroupID granularity.
- Pipelining (`Promise.all` on an array of `PendingQuery`) is possible
  because postgres.js keeps the transaction open until the begin
  callback returns, and buffers statements to the server asynchronously.
- The "fire-and-forget" owner-change signal in `load()` is not protected
  by a lock — the UPDATE is idempotent and gated on `grantedAt` so only
  the rightful new owner wins; the current owner discovers it on its next
  flush via `OwnershipError`.

## Proposed Rust form

`tokio-postgres` supports `SELECT ... FOR UPDATE` identically, and the
deadpool-postgres / `Client::transaction` API covers the transactional
pattern. Pipelining requires explicit `tokio::try_join!` over
`Client::query_raw` / prepared statements, because `tokio-postgres`'
pipelined extended-query protocol is exposed through concurrent `await`
on the same connection.

```rust
use tokio::try_join;

async fn check_version_and_ownership(
    tx: &tokio_postgres::Transaction<'_>,
    schema: &str,
    id: &str,
    task_id: &str,
    expected: &CvrVersion,
    last_connect_time: chrono::DateTime<Utc>,
) -> Result<(), FlushError> {
    let row = tx
        .query_opt(
            &format!(
                "SELECT \"version\", \"owner\", \"grantedAt\"
                   FROM {schema}.instances
                   WHERE \"clientGroupID\" = $1
                   FOR UPDATE"
            ),
            &[&id],
        )
        .await?;
    let (version, owner, granted_at) = match row {
        Some(r) => (
            r.get::<_, String>("version"),
            r.get::<_, Option<String>>("owner"),
            r.get::<_, Option<chrono::DateTime<Utc>>>("grantedAt"),
        ),
        None => (EMPTY_STATE_VERSION.to_string(), None, None),
    };
    if owner.as_deref() != Some(task_id) && granted_at.map_or(false, |g| g > last_connect_time) {
        return Err(FlushError::Ownership { owner, granted_at, last_connect_time });
    }
    if version != version_string(expected) {
        return Err(FlushError::ConcurrentModification { expected: version_string(expected), actual: version });
    }
    Ok(())
}

async fn flush(
    client: &mut tokio_postgres::Client,
    expected: &CvrVersion,
    /* ... */
) -> Result<FlushStats, FlushError> {
    // READ COMMITTED is the Postgres default; override with SET TRANSACTION if needed.
    let tx = client.transaction().await?;
    check_version_and_ownership(&tx, schema, id, task_id, expected, last_connect_time).await?;

    // Pipeline writes: the awaited set shares the single connection held by `tx`.
    let (q1, q2, q3) = try_join!(
        tx.execute(upsert_instance, &[...]),
        tx.execute(upsert_queries, &[...]),
        tx.execute(upsert_desires, &[...]),
    )?;
    // ... plus per-row updates
    tx.commit().await?;
    Ok(stats)
}
```

## Classification
Idiom-swap. The SQL shape is preserved verbatim; only the driver glue
differs. `FOR UPDATE` + per-row version columns is a classic
optimistic-concurrency-with-escalation-to-pessimistic pattern; both
postgres.js (begin callback) and `tokio-postgres` (`Client::transaction`)
express it naturally.

## Caveats
- Don't use `tokio-postgres` pooled connections without returning them to
  the pool after commit/rollback — deadpool-postgres already handles
  this. Just hold the `Transaction` for the duration of the flush.
- Postgres' default isolation is `READ COMMITTED`; that's what the TS
  passes (`Mode.READ_COMMITTED` at cvr-store.ts:1113). If you change
  this to `REPEATABLE READ` or `SERIALIZABLE` to avoid the `FOR UPDATE`,
  you must also decide how to detect and surface serialization failures
  — the current code relies on the explicit row-lock + version check, not
  on isolation level, for correctness.
- The "fire-and-forget" ownership-takeover UPDATE (cvr-store.ts:367) is
  **not** inside a transaction — it's a single idempotent statement
  whose WHERE clause ensures only one taker wins. Port the exact WHERE
  clause (`grantedAt <= to_timestamp(${lastConnectTime / 1000})`);
  `to_timestamp` expects seconds.
- The `rowsVersion` table (`schema/cvr.ts:283`) is deliberately
  un-FK'd from `instances` so that row-patch flushes don't conflict
  with the `FOR UPDATE` row lock on `instances`. Keep that design —
  adding a FK here would serialise all row writes against instance
  writes.
- Flush queues some writes in `#pendingRowRecordUpdates` that can be
  deferred ("allow-defer" at cvr-store.ts:1091). On defer, the CVR's
  `version` advances but `rowsVersion.version` lags; the next `load()`
  sees the mismatch and returns a `RowsVersionBehindError` that triggers
  a bounded retry loop. Preserve this behaviour or the catchup path will
  hang.

## Citation
- PostgreSQL `SELECT ... FOR UPDATE`:
  https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
- `tokio_postgres::Transaction`:
  https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Transaction.html
- deadpool-postgres transaction guide:
  https://docs.rs/deadpool-postgres/latest/deadpool_postgres/
- `tokio::try_join!` for pipelining:
  https://docs.rs/tokio/latest/tokio/macro.try_join.html
