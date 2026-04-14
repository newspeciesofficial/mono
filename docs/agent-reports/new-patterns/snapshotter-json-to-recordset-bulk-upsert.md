# New pattern: Bulk upsert via `json_to_recordset` (or COPY) parameterised with a single JSON payload

## Category
Library (porsager `postgres` → `tokio-postgres`) / wire-protocol-adjacent.

## Where used
- `packages/zero-cache/src/services/view-syncer/row-record-cache.ts:384–425` — `executeRowUpdates` builds one `INSERT … SELECT … FROM json_to_recordset($1) AS x(…) ON CONFLICT … DO UPDATE …` statement from a `RowsRow[]` array.

## TS form
```ts
const rowRecordRows: RowsRow[] = [];
for (const [id, row] of rowUpdates.entries()) {
  if (row === null) {
    pending.push(tx`DELETE FROM ${this.#cvr('rows')}
                     WHERE "clientGroupID" = ${this.#cvrID}
                       AND "schema" = ${id.schema}
                       AND "table"  = ${id.table}
                       AND "rowKey" = ${id.rowKey}`);
  } else {
    rowRecordRows.push(rowRecordToRowsRow(this.#cvrID, row));
  }
}
if (rowRecordRows.length) {
  pending.push(tx`
    INSERT INTO ${this.#cvr('rows')}(
      "clientGroupID", "schema", "table", "rowKey",
      "rowVersion", "patchVersion", "refCounts")
    SELECT "clientGroupID", "schema", "table", "rowKey",
           "rowVersion", "patchVersion", "refCounts"
      FROM json_to_recordset(${rowRecordRows}) AS x(
        "clientGroupID" TEXT, "schema" TEXT, "table" TEXT,
        "rowKey" JSONB, "rowVersion" TEXT,
        "patchVersion" TEXT, "refCounts" JSONB)
    ON CONFLICT ("clientGroupID","schema","table","rowKey")
      DO UPDATE SET "rowVersion"   = excluded."rowVersion",
                    "patchVersion" = excluded."patchVersion",
                    "refCounts"    = excluded."refCounts"`);
}
```

The porsager client serialises the array as a single `jsonb` parameter and
the server expands it into rows; this amortises the round-trip cost for a
write batch that can be tens of thousands of rows.

## Proposed Rust form
```rust
use serde::Serialize;
use tokio_postgres::types::Json;

#[derive(Serialize)]
struct RowsRowJson<'a> {
    #[serde(rename = "clientGroupID")]  client_group_id: &'a str,
    schema:                              &'a str,
    table:                               &'a str,
    #[serde(rename = "rowKey")]          row_key: &'a serde_json::Value,
    #[serde(rename = "rowVersion")]      row_version: &'a str,
    #[serde(rename = "patchVersion")]    patch_version: &'a str,
    #[serde(rename = "refCounts")]       ref_counts: &'a serde_json::Value,
}

async fn bulk_upsert_rows(
    tx: &tokio_postgres::Transaction<'_>,
    schema: &str,
    rows: &[RowsRowJson<'_>],
) -> anyhow::Result<u64> {
    if rows.is_empty() { return Ok(0); }
    let sql = format!(
        r#"
        INSERT INTO "{schema}"."rows"(
            "clientGroupID", "schema", "table", "rowKey",
            "rowVersion", "patchVersion", "refCounts")
        SELECT "clientGroupID", "schema", "table", "rowKey",
               "rowVersion", "patchVersion", "refCounts"
          FROM json_to_recordset($1::jsonb) AS x(
            "clientGroupID" TEXT, "schema" TEXT, "table" TEXT,
            "rowKey" JSONB, "rowVersion" TEXT,
            "patchVersion" TEXT, "refCounts" JSONB)
        ON CONFLICT ("clientGroupID","schema","table","rowKey")
          DO UPDATE SET "rowVersion"   = EXCLUDED."rowVersion",
                        "patchVersion" = EXCLUDED."patchVersion",
                        "refCounts"    = EXCLUDED."refCounts"
        "#
    );
    let n = tx.execute(&sql, &[&Json(rows)]).await?;
    Ok(n)
}
```

For writes that routinely exceed ~50k rows per batch, swap `json_to_recordset`
for `COPY … FROM STDIN BINARY`:
```rust
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use futures::pin_mut;

let sink = tx.copy_in(
    r#"COPY "cvr_schema"."rows"(
         "clientGroupID","schema","table","rowKey","rowVersion",
         "patchVersion","refCounts") FROM STDIN BINARY"#).await?;
let writer = BinaryCopyInWriter::new(
    sink,
    &[Type::TEXT, Type::TEXT, Type::TEXT, Type::JSONB,
      Type::TEXT, Type::TEXT, Type::JSONB],
);
pin_mut!(writer);
for r in rows {
    writer.as_mut().write(&[
        &r.client_group_id, &r.schema, &r.table, &Json(r.row_key),
        &r.row_version, &r.patch_version, &Json(r.ref_counts),
    ]).await?;
}
writer.finish().await?;
```

## Classification
Idiom-swap. The TS code depends on porsager's automatic serialisation of a
JS array to `jsonb`; `tokio-postgres` needs the `Json<T>` wrapper from
`postgres-types` (already in `Cargo.toml`) to achieve the same shape.

## Caveats
- `json_to_recordset` is simple and parameterisable but does not preserve
  row order and imposes per-field casts. Use `COPY … BINARY` (above) for hot
  paths where the flush throughput matters — this is the documented
  faster-path in the Postgres wire protocol.
- `ON CONFLICT … DO UPDATE SET col = EXCLUDED.col` is identical in both
  paths; only the row-source (`json_to_recordset` vs `COPY`) changes.
- For the `DELETE` branch the TS code emits one statement per row. On the
  Rust side prefer a single `DELETE … WHERE ("clientGroupID","schema",
  "table","rowKey") IN (SELECT … FROM json_to_recordset($1))` to keep the
  flush a two-statement transaction regardless of batch size.
- The `Json<&[…]>` parameter must round-trip through `serde_json::Value`
  for the `rowKey` / `refCounts` fields, because those are already JSON
  objects in the source of truth.

## Citation
- `tokio_postgres::types::Json` (JSON/JSONB parameter wrapper):
  https://docs.rs/postgres-types/latest/postgres_types/struct.Json.html
- `tokio_postgres::binary_copy::BinaryCopyInWriter`:
  https://docs.rs/tokio-postgres/latest/tokio_postgres/binary_copy/struct.BinaryCopyInWriter.html
- Postgres `json_to_recordset` reference:
  https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-PROCESSING-TABLE
