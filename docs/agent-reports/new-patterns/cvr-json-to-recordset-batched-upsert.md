# New pattern: batched UPSERT via `json_to_recordset` (postgres.js array param → rowset)

## Category
Library (postgres driver idiom) / D (Data).

## Where used
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:741-792` — batched
  full-row upsert into `cvr.queries` driven by `json_to_recordset(${rows})`.
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:813-846` — batched
  partial update of `cvr.queries` using a `SET col = CASE WHEN u."<col>Set" THEN
  u."<col>" ELSE q."<col>" END` pattern joined against
  `json_to_recordset(${rows})`.
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:878-923` — batched
  upsert into `cvr.desires` with an inline `to_timestamp(... / 1000.0)` cast to
  bridge legacy `TIMESTAMPTZ` columns from the new `DOUBLE PRECISION`
  millisecond columns.

The `${rows}` interpolation is an array of plain JS objects; postgres.js
JSON-encodes it and the server parses it back with `json_to_recordset(...)`
into a typed rowset that participates in the `INSERT ... SELECT ... ON
CONFLICT` clause. One network round-trip, one SQL statement, N rows written.

## TS form
```ts
const rows = [...this.#pendingQueryUpdates.values()]; // objects with
                                                     // pre-stringified JSON args
await tx`
  INSERT INTO ${this.#cvr('queries')} (
    "clientGroupID", "queryHash", "clientAST", "queryName",
    "queryArgs", "patchVersion", "transformationHash",
    "transformationVersion", "internal", "deleted"
  )
  SELECT
    "clientGroupID", "queryHash", "clientAST", "queryName",
    CASE WHEN "queryArgs" IS NULL THEN NULL ELSE "queryArgs"::json END,
    "patchVersion", "transformationHash", "transformationVersion",
    "internal", "deleted"
  FROM json_to_recordset(${rows}) AS x(
    "clientGroupID" TEXT,
    "queryHash"     TEXT,
    "clientAST"     JSONB,
    "queryName"     TEXT,
    "queryArgs"     TEXT,
    "patchVersion"  TEXT,
    "transformationHash" TEXT,
    "transformationVersion" TEXT,
    "internal" BOOLEAN,
    "deleted" BOOLEAN
  )
  ON CONFLICT ("clientGroupID", "queryHash") DO UPDATE SET
    "clientAST"             = excluded."clientAST",
    "queryName"             = excluded."queryName",
    "queryArgs"             = CASE WHEN excluded."queryArgs" IS NULL THEN NULL
                                   ELSE excluded."queryArgs"::json END,
    "patchVersion"          = excluded."patchVersion",
    "transformationHash"    = excluded."transformationHash",
    "transformationVersion" = excluded."transformationVersion",
    "internal"              = excluded."internal",
    "deleted"               = excluded."deleted"
`;
```

## Proposed Rust form

Two mechanical choices; both one round-trip.

**Option A — keep `json_to_recordset`** (closest to TS):

```rust
use serde_json::{to_value, Value};
use tokio_postgres::types::Json;

#[derive(serde::Serialize)]
struct QueriesUpsertRow<'a> {
    #[serde(rename = "clientGroupID")] client_group_id: &'a str,
    #[serde(rename = "queryHash")]     query_hash:      &'a str,
    #[serde(rename = "clientAST")]     client_ast:      Option<Value>, // JSONB
    #[serde(rename = "queryName")]     query_name:      Option<&'a str>,
    #[serde(rename = "queryArgs")]     query_args:      Option<String>, // pre-stringified
    #[serde(rename = "patchVersion")]  patch_version:   Option<&'a str>,
    #[serde(rename = "transformationHash")]    transformation_hash:    Option<&'a str>,
    #[serde(rename = "transformationVersion")] transformation_version: Option<&'a str>,
    internal: Option<bool>,
    deleted:  bool,
}

let rows_json: Value = to_value(&rows)?;     // serde_json::to_value — Vec → JSON array
let stmt = r#"
    INSERT INTO cvr_SHARD.queries ( "clientGroupID", "queryHash", "clientAST", ... )
    SELECT "clientGroupID", "queryHash", "clientAST", ...
    FROM json_to_recordset($1::json) AS x(
        "clientGroupID" TEXT,
        "queryHash"     TEXT,
        "clientAST"     JSONB,
        ...
    )
    ON CONFLICT ("clientGroupID", "queryHash") DO UPDATE SET ...
"#;
tx.execute(stmt, &[&Json(rows_json)]).await?;
```

Notes on this option:
- `tokio_postgres::types::Json<T>` wraps any `Serialize` value as a bound
  JSON parameter; it round-trips via `$1::json`.
- The schema-name interpolation (`${this.#cvr('queries')}`) is the shard
  schema — produce the statement text with `format!` after escaping the
  schema identifier via `postgres-protocol::escape::escape_identifier`.

**Option B — UNNEST arrays** (more idiomatic Rust, skips JSON):

```rust
// Build column-parallel Vecs; `UNNEST` yields one row per index.
let client_group_ids: Vec<&str> = rows.iter().map(|r| r.client_group_id).collect();
let query_hashes:     Vec<&str> = rows.iter().map(|r| r.query_hash).collect();
// ... one Vec per column
tx.execute(
    r#"
    INSERT INTO cvr_SHARD.queries ("clientGroupID","queryHash",...)
    SELECT * FROM UNNEST($1::text[], $2::text[], ...)
        AS t("clientGroupID","queryHash",...)
    ON CONFLICT ("clientGroupID","queryHash") DO UPDATE SET ...
    "#,
    &[&client_group_ids, &query_hashes, ...],
).await?;
```

Notes:
- `UNNEST` requires one array per column (no JSONB columns as TEXT — cast
  client-side or server-side).
- For JSONB columns (`"clientAST"`) build a `Vec<Json<Value>>` and bind
  as `$3::jsonb[]`.

## Classification
Idiom-swap.

The exact TS shape (`json_to_recordset(${arrayOfObjects})`) is a
postgres.js-specific trick: the driver serialises a JS array as a JSON
parameter, and the SQL function re-parses it into a typed rowset. It has
no direct counterpart in `tokio-postgres` — but either `Json` + `$1::json`
(Option A) or `UNNEST($1::text[], ...)` (Option B) produces the same
N-rows-in-one-statement behaviour.

Option A is the smaller change from the TS (keeps the SQL identical up to
parameter binding); Option B is slightly faster and avoids the JSON
encode/parse, at the cost of needing one `Vec` per column.

## Caveats
- The TS code **pre-stringifies** `queryArgs` as TEXT and casts back to
  JSON inside the SELECT (`"queryArgs"::json`). This is a workaround for a
  postgres.js boolean-array bug (comment at cvr-store.ts:566). In Rust
  with `tokio-postgres`, this workaround is unnecessary — bind the value
  directly as `Json<Value>`.
- The partial-update variant at cvr-store.ts:813 encodes
  "update this field, leave the others unchanged" by including a boolean
  `<col>Set` flag per field and selecting between `excluded."<col>"` and
  `q."<col>"`. Preserve this structure — it matters because a single
  batch may carry a mix of full and partial updates for different rows.
- The `to_timestamp(.../1000.0)` cast at cvr-store.ts:898-903 bridges
  two generations of columns (old `TIMESTAMPTZ "inactivatedAt"` and new
  `DOUBLE PRECISION "inactivatedAtMs"`). Both columns are still
  populated for forward/backward compat; do not drop one without a
  migration row.
- `ON CONFLICT` target columns must be the real primary key
  (`clientGroupID, queryHash` for `queries`;
  `clientGroupID, clientID, queryHash` for `desires`) — these are defined
  in `schema/cvr.ts:133, 178` and are the basis of the upsert.

## Citation
- PostgreSQL `json_to_recordset` docs:
  https://www.postgresql.org/docs/current/functions-json.html
- `tokio_postgres::types::Json` (wraps a `Serialize` value as a JSON
  parameter):
  https://docs.rs/tokio-postgres/latest/tokio_postgres/types/struct.Json.html
- `UNNEST` alternative for batched insert:
  https://www.postgresql.org/docs/current/functions-array.html#ARRAY-FUNCTIONS-TABLE
