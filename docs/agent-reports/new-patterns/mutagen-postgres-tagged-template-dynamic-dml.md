# New pattern: postgres.js tagged-template dynamic DML (INSERT/UPDATE/DELETE by column object)

## Category
Library (wire/DML construction).

## Where used
- `packages/zero-cache/src/services/mutagen/mutagen.ts:391` — `tx\`INSERT INTO ${tx(create.tableName)} ${tx(create.value)}\``
- `packages/zero-cache/src/services/mutagen/mutagen.ts:399-403` — upsert with `ON CONFLICT (${tx(primaryKey)}) DO UPDATE SET ${tx(value)}`
- `packages/zero-cache/src/services/mutagen/mutagen.ts:416-420` — UPDATE with a composite-key `WHERE` built from `Object.entries(id).flatMap(...)`
- `packages/zero-cache/src/services/mutagen/mutagen.ts:429-437` — DELETE with a composite-key `WHERE` built by interleaving `tx\`AND\`` fragments

## TS form
```ts
// Identifier escape: tx(tableName), tx(primaryKey)
// Row spread: tx(value) where value is Record<string, PrimaryKeyValue>
// Fragment composition via an array of tagged fragments.
return tx`INSERT INTO ${tx(create.tableName)} ${tx(create.value)}`;

return tx`
  INSERT INTO ${tx(tableName)} ${tx(value)}
  ON CONFLICT (${tx(primaryKey)})
  DO UPDATE SET ${tx(value)}
`;

return tx`UPDATE ${tx(table)} SET ${tx(value)} WHERE ${Object.entries(id).flatMap(
  ([key, value], i) =>
    i ? [tx`AND`, tx`${tx(key)} = ${value}`] : tx`${tx(key)} = ${value}`,
)}`;
```

The postgres.js tagged template does three things in one expression:
1. Escapes identifiers passed to `tx(ident)` (wraps in `"…"` and doubles internal quotes).
2. Expands `tx(record)` into either a `(col1, col2, …) VALUES ($1, $2, …)` tuple
   (INSERT position) or `"col1" = $1, "col2" = $2, …` list (UPDATE `SET` position),
   by inspecting the call site.
3. Accepts arrays of tagged fragments and splices them in, renumbering `$N`
   parameters globally.

## Proposed Rust form
```rust
// Hand-build parameterised SQL. tokio-postgres has no equivalent of postgres.js
// tagged-template "expand an object into (cols) VALUES ($n,…) or SET …".
use postgres_protocol::escape::{escape_identifier, escape_literal};

fn build_insert(table: &str, row: &IndexMap<String, Value>) -> (String, Vec<&Value>) {
    let table = escape_identifier(table);
    let cols: Vec<String> = row.keys().map(|k| escape_identifier(k)).collect();
    let placeholders: Vec<String> =
        (1..=cols.len()).map(|i| format!("${i}")).collect();
    let sql = format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        cols.join(", "),
        placeholders.join(", "),
    );
    (sql, row.values().collect())
}

// UPDATE with composite PK:
fn build_update(table: &str, row: &IndexMap<String, Value>, pk: &[&str])
    -> (String, Vec<&Value>)
{
    // SET list
    let mut idx = 1usize;
    let set_clause: Vec<String> = row.keys().map(|k| {
        let s = format!("{} = ${idx}", escape_identifier(k));
        idx += 1;
        s
    }).collect();
    // WHERE on PK columns (re-using ordinals that point at the same Vec slot)
    let where_clause: Vec<String> = pk.iter().map(|k| {
        let s = format!("{} = ${idx}", escape_identifier(k));
        idx += 1;
        s
    }).collect();
    let sql = format!(
        "UPDATE {} SET {} WHERE {}",
        escape_identifier(table),
        set_clause.join(", "),
        where_clause.join(" AND "),
    );
    let mut params: Vec<&Value> = row.values().collect();
    for k in pk { params.push(&row[*k]); }
    (sql, params)
}

// Execute:
// client.execute(&sql, &params_as_trait_objects).await?;
// where params_as_trait_objects: &[&(dyn ToSql + Sync)].
```

## Classification
Redesign — no tokio-postgres helper builds dynamic `(cols) VALUES ($n,…)` or
`SET "c" = $n` clauses from a record. Must hand-build the SQL string and the
`&[&(dyn ToSql + Sync)]` parameter slice, and use
`postgres_protocol::escape::escape_identifier` for user-supplied identifiers.

## Caveats
- postgres.js inlines identifiers it has already escaped directly into the SQL,
  while keeping row values as numbered parameters. Do the same in Rust;
  **never** pass table/column names as `$N` parameters — PostgreSQL rejects
  that.
- Composite PKs: the TS uses `v.parse(value[key], primaryKeyValueSchema)` to
  validate each key column before splicing — keep that shape (validate before
  push into the `&dyn ToSql` vec).
- `tokio-postgres` does not expose the final rendered SQL + bound parameters the
  way postgres.js does via `q.string` / `q.parameters` (used for debug logging
  at mutagen.ts:332). If the mirror logging is wanted, log `&sql, &params`
  directly from the builder.

## Citation
- tokio-postgres `Client::execute` / `Client::query` (parameters are
  `&[&(dyn ToSql + Sync)]`, placeholders are `$N`):
  https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Client.html#method.execute
- `postgres-protocol::escape::escape_identifier`:
  https://docs.rs/postgres-protocol/latest/postgres_protocol/escape/fn.escape_identifier.html
- postgres.js dynamic helpers reference (for parity with TS behaviour):
  https://github.com/porsager/postgres#dynamic-inserts
