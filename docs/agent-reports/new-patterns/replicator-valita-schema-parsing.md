# New pattern: Valita runtime schema parsing with `.map(...)` transforms

## Category
Library.

## Where used
- `packages/zero-cache/src/services/replicator/schema/replication-state.ts:72-105` — `subscriptionStateSchema`, `subscriptionStateAndContextSchema`, `replicationStateSchema` use `v.object({…}).map(s => ({…, publications: v.parse(JSON.parse(s.publications), stringArray)}))` to validate rows and transform a stored JSON string column into a typed array in one pass.
- `packages/zero-cache/src/services/replicator/schema/change-log.ts:92-120` — `changeLogEntrySchema` and `rawChangeLogEntrySchema` use `v.literalUnion(SET_OP, DEL_OP, TRUNCATE_OP, RESET_OP)` and `.map(val => ({…, rowKey: val.op === 't' \|\| val.op === 'r' ? null : v.parse(parse(val.rowKey), jsonObjectSchema)}))` to switch the shape of the output based on the op discriminator.
- `packages/zero-cache/src/services/replicator/schema/change-log.ts:115-118` — nested `.map`: `backfillingColumnVersions: v.string().map(val => v.record(v.string()).parse(JSON.parse(val)))`.
- `packages/zero-cache/src/services/replicator/change-processor.ts` (consumer of the above).

## TS form
```ts
import * as v from '../../../../../shared/src/valita.ts';

const stringArray = v.array(v.string());

const subscriptionStateSchema = v
  .object({
    replicaVersion: v.string(),
    publications: v.string(),         // stored as JSON text in SQLite
    watermark: v.string(),
  })
  .map(s => ({
    ...s,
    publications: v.parse(JSON.parse(s.publications), stringArray),
  }));

export type SubscriptionState = v.Infer<typeof subscriptionStateSchema>;

// Usage:
const result = db.get(`SELECT … FROM "_zero.replicationConfig" …`);
return v.parse(result, subscriptionStateSchema);
```

## Proposed Rust form
Use `serde` with `#[derive(Deserialize)]` for the row shape, and do the post-parse transform either in a `TryFrom` impl or with `#[serde(deserialize_with = "...")]`:

```rust
use serde::Deserialize;
use serde_json;

#[derive(Deserialize)]
struct SubscriptionStateRow {
    #[serde(rename = "replicaVersion")]
    replica_version: String,
    publications: String,   // JSON text in SQLite
    watermark: String,
}

pub struct SubscriptionState {
    pub replica_version: String,
    pub publications: Vec<String>,
    pub watermark: String,
}

impl TryFrom<SubscriptionStateRow> for SubscriptionState {
    type Error = serde_json::Error;
    fn try_from(r: SubscriptionStateRow) -> Result<Self, Self::Error> {
        Ok(SubscriptionState {
            replica_version: r.replica_version,
            publications: serde_json::from_str(&r.publications)?,
            watermark: r.watermark,
        })
    }
}

// For the discriminated-union case (change-log.ts:92-106), prefer an enum:
#[derive(Deserialize)]
#[serde(try_from = "ChangeLogRow")]
pub enum ChangeLogEntry {
    Set { state_version: String, table: String, row_key: serde_json::Map<String, serde_json::Value> },
    Del { state_version: String, table: String, row_key: serde_json::Map<String, serde_json::Value> },
    Truncate { state_version: String, table: String },
    Reset    { state_version: String, table: String },
}
```

For the `rusqlite` row-to-struct mapping specifically, `serde_rusqlite` can replace the `db.get(...) → v.parse(...)` pair in one step.

## Classification
- **Idiom-swap.** Valita's `.map(fn)` during parse is replaced by a `TryFrom` conversion after `serde` decoding, or by `#[serde(deserialize_with = …)]`. The guarantees (strict typing, fails fast on missing/wrong fields) are preserved. The only semantic difference is that valita conflates validation and transformation in one pipeline; Rust separates them but the observable behaviour is identical.

## Caveats
- Valita's `v.literalUnion('s', 'd', 't', 'r')` matches on a string discriminator. In Rust, use `#[serde(tag = "op")]` on an enum **or** a custom `TryFrom<RawRow>` because the `op` column is one of several columns, not the tag of a single JSON object. Both work; the `TryFrom` route mirrors the TS flow more closely.
- Valita `.map` is allowed to throw (`v.parse` inside the callback); the Rust equivalent must surface these as `Err` from `TryFrom` and be bubbled up via `?`.
- `v.Infer<typeof schema>` has no direct Rust counterpart. Define the output struct explicitly — the TS `Infer` convenience is a type-system-only feature.
- Where the TS code uses `v.parse(x, schema, 'passthrough')` (change-log.ts:204) to permit extra fields, use `#[serde(default)]` or omit `#[serde(deny_unknown_fields)]` (serde's default already ignores unknown fields).

## Citation
- `serde` `deserialize_with` / custom converters: <https://serde.rs/field-attrs.html#deserialize_with>
- `serde` container `try_from` attribute for post-parse conversion: <https://serde.rs/container-attrs.html#try_from>
- `serde_rusqlite` (row-to-struct decoding for `rusqlite`): <https://crates.io/crates/serde_rusqlite>
- `rusqlite` `Connection::prepare_cached` (idiomatic replacement for valita's repeated `db.prepare`): <https://docs.rs/rusqlite/latest/rusqlite/struct.Connection.html#method.prepare_cached>
