# New pattern: valita runtime schema validation + inference

## Category
Library.

## Where used
- `packages/zero-cache/src/services/change-source/pg/schema/ddl.ts:20-118` — `v.object({...}).extend({...})`, `v.union(...)`, `v.literal(…)`, `v.Infer<typeof X>` used to define `DdlStartEvent` / `DdlUpdateEvent` / `SchemaSnapshotEvent` / `replicationEventSchema`.
- `packages/zero-cache/src/services/change-source/pg/schema/published.ts:177-233` — `publishedSchema = v.object({...}).map(...)` with a post-parse transformation adding `replicaIdentityColumns`.
- `packages/zero-cache/src/services/change-source/pg/schema/shard.ts:222-236` — `internalShardConfigSchema` + `replicaSchema = internalShardConfigSchema.extend(...)`; `v.parse(result[0], replicaSchema, 'passthrough')`.
- `packages/zero-cache/src/services/change-source/pg/schema/init.ts:135-149` — `v.parse(result[0], legacyShardConfigSchema, 'passthrough')`.
- `packages/zero-cache/src/services/change-source/pg/change-source.ts:1086` — `v.parse(json, replicationEventSchema, 'passthrough')` applied to the JSON payload of a `pg_logical_emit_message` DDL event.
- `packages/zero-cache/src/services/change-source/pg/backfill-stream.ts:277,313` — `v.parse(bf.table.metadata, tableMetadataSchema)`, `v.parse(val, columnMetadataSchema)`.
- `packages/zero-cache/src/services/change-source/pg/backfill-metadata.ts:6-18` — schema defined with `v.object({...})` and `v.record(...)`; types obtained via `v.Infer<typeof X>`.

## TS form
```ts
import * as v from '.../shared/src/valita.ts';

const ddlEventSchema = v.object({
  context: v.object({query: v.string()}).rest(v.string()),
}).extend({
  version: v.literal(PROTOCOL_VERSION),
  schema: publishedSchema,
});

const replicationEventSchema = v.union(
  ddlStartEventSchema,
  ddlUpdateEventSchema,
  schemaSnapshotEventSchema,
);
type ReplicationEvent = v.Infer<typeof replicationEventSchema>;

// Parse with unknown-key policy:
const event = v.parse(json, replicationEventSchema, 'passthrough');
```

## Proposed Rust form
```rust
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type")]
enum ReplicationEvent {
    #[serde(rename = "ddlStart")]
    DdlStart(DdlStartEvent),
    #[serde(rename = "ddlUpdate")]
    DdlUpdate(DdlUpdateEvent),
    #[serde(rename = "schemaSnapshot")]
    SchemaSnapshot(SchemaSnapshotEvent),
}

#[derive(Deserialize, Serialize, Debug)]
struct DdlEventCommon {
    version: u32,
    schema: PublishedSchema,
    context: Context,
    // Unknown fields are ignored by default (= 'passthrough' semantics) —
    // do NOT add `#[serde(deny_unknown_fields)]`.
}

// The `schema.map(...)` post-parse transform (adding replicaIdentityColumns)
// becomes either a `#[serde(from = "RawPublishedSchema")]` wrapper or a
// `TryFrom` conversion executed after deserialization.

let event: ReplicationEvent = serde_json::from_slice(&payload)?;
```

Notes:
- valita's `'passthrough'` parse option = serde's default behaviour (unknown
  keys silently ignored). valita's `.strict()` = `#[serde(deny_unknown_fields)]`.
- valita's `.rest(v.string())` (accept any additional string-valued keys) maps
  to `#[serde(flatten)] extra: HashMap<String, String>`.
- valita's `.map((raw) => derived)` post-parse transform has no direct serde
  analogue; use `#[serde(from = "Raw")] struct Derived { ... } impl From<Raw>
  for Derived { … }` or implement a custom `Deserialize`.
- `v.literal(N)` tag discrimination on a numeric version has no direct serde
  macro — check the field manually after deserialization or use
  `#[serde(tag="type")]` on string variants only.

## Classification
Idiom-swap. Every valita call has a serde analogue, but the derive attributes
must be chosen per-site (especially for `'passthrough'` vs `.strict()` and for
`.map()` transforms). Not a mechanical 1:1 rewrite.

## Caveats
- `v.Infer<typeof X>` produces an inferred TS type from a runtime schema.
  In Rust the struct is the source of truth and `serde` derives
  de/serialization from it — the direction is reversed.
- `v.union(...)` with non-tagged variants is expressible in serde but may need
  `#[serde(untagged)]`, which is slower and more error-prone. The DDL event
  schemas are tag-discriminated (`type: "ddlStart"` etc.), so `#[serde(tag =
  "type")]` is the right shape.
- `.map()` post-processing on `publishedSchema` (computes
  `replicaIdentityColumns` from `indexes` + `replicaIdentity`) must be
  re-expressed as a `From<Raw>` impl.

## Citation
- serde enum representations (adjacently, internally, externally tagged, and
  untagged): https://serde.rs/enum-representations.html
- `#[serde(from = "...")]` for post-parse transforms:
  https://serde.rs/container-attrs.html#from
