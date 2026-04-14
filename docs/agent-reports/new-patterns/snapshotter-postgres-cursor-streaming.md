# New pattern: Postgres server-side cursor streaming for large result sets

## Category
Library (porsager `postgres` → `tokio-postgres`).

## Where used
- `packages/zero-cache/src/services/view-syncer/row-record-cache.ts:169` — bulk load of `cvr.rows` on first access.
- `packages/zero-cache/src/services/view-syncer/row-record-cache.ts:353` — `catchupRowPatches` streams changed rows to the poke pipeline.

## TS form
```ts
// Load-all at startup
for await (const rows of this.#db<RowsRow[]>`
  SELECT * FROM ${this.#cvr(`rows`)}
    WHERE "clientGroupID" = ${this.#cvrID} AND "refCounts" IS NOT NULL`
  .cursor(5000)) {
  for (const row of rows) {
    cache.set(rowRecord.id, rowRecord);
  }
}

// Generator that yields pages
async *catchupRowPatches(...): AsyncGenerator<RowsRow[], void, undefined> {
  // ...
  yield* query.cursor(10000);
}
```

The porsager `postgres` client executes this as a server-side Portal
(`BIND` + `EXECUTE n` loop) inside an implicit transaction, pulling `N` rows
at a time and emitting a `RowsRow[]` array per page.

## Proposed Rust form
```rust
// tokio-postgres: drive a server-side Portal explicitly inside a txn.
use futures::TryStreamExt;
use tokio_postgres::types::ToSql;

async fn load_row_records(
    client: &mut tokio_postgres::Client,
    schema: &str,
    cvr_id: &str,
) -> anyhow::Result<HashMap<RowID, RowRecord>> {
    let tx = client.transaction().await?;
    let stmt = tx
        .prepare(&format!(
            r#"SELECT * FROM "{schema}"."rows"
               WHERE "clientGroupID" = $1 AND "refCounts" IS NOT NULL"#
        ))
        .await?;
    let portal = tx.bind(&stmt, &[&cvr_id as &(dyn ToSql + Sync)]).await?;

    let mut out = HashMap::new();
    loop {
        let rows = tx.query_portal(&portal, 5000).await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let rec = RowsRow::from_row(&row)?;
            out.insert(rec.id.clone(), rec.into_record());
        }
    }
    tx.commit().await?;
    Ok(out)
}

// For generator-style page streaming, wrap the portal loop in an
// `async_stream::try_stream!` yielding `Vec<RowsRow>` per page.
pub fn catchup_row_patches_stream(
    tx: &tokio_postgres::Transaction<'_>,
    portal: tokio_postgres::Portal,
) -> impl Stream<Item = anyhow::Result<Vec<RowsRow>>> + '_ {
    async_stream::try_stream! {
        loop {
            let rows = tx.query_portal(&portal, 10_000).await?;
            if rows.is_empty() { break; }
            yield rows.into_iter().map(RowsRow::from_row).collect::<Result<_, _>>()?;
        }
    }
}
```

Notes:
- The outer transaction must live for the duration of the stream — Portals
  are bound to the transaction. In the TS code the porsager client opens a
  short-lived transaction per cursor invocation; in Rust we need an
  explicit `Transaction<'_>` handle.
- `catchupRowPatches` is called from inside a `TransactionPool` reader task
  (row-record-cache.ts:330–355), so the Rust side already has a
  `Transaction<'_>` in scope; pass it into the portal-building helper.

## Classification
Idiom-swap. The porsager tag-template `.cursor(N)` collapses
BEGIN/BIND/EXECUTE-loop/COMMIT into one call; `tokio-postgres` exposes each
step, so the translation is mechanical once a thin `paged_portal(sql, N, params)`
helper is factored out. Alternative: `COPY (SELECT …) TO STDOUT BINARY` via
`tokio-postgres` `copy_out`, which is faster but non-parameterisable for
dynamic `WHERE` clauses.

## Caveats
- Portal reads share the transaction's snapshot — long-running streams hold
  a transaction open. That matches the TS behaviour (the porsager cursor
  runs in an implicit txn), but on the Rust side we already wrap the call
  in `TransactionPool(lc, Mode.READONLY)`, so no change is needed beyond
  threading the `Transaction<'_>` through to the portal.
- `async-stream` is not currently in `Cargo.toml`; it is listed in the
  guide's Part 4 "Missing but needed" list.
- Page size is a tuning parameter; keep the TS constants (`5000` /
  `10000`) in the Rust port unless profiling suggests otherwise.

## Citation
- `tokio_postgres::Transaction::bind` and `query_portal`:
  https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Transaction.html#method.bind
  and `#method.query_portal`.
- `async-stream` crate for generator-style async iteration:
  https://docs.rs/async-stream/latest/async_stream/
- Postgres extended-query / Portal protocol:
  https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
