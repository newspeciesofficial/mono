//! Port of `packages/zero-cache/src/services/view-syncer/client-schema.ts`.
//!
//! Public exports ported:
//!
//! - [`check_client_schema`] — validates a client's declared schema
//!   against the replica's actual synced table specs. Called once per
//!   `ClientGroup` at `addQuery` time by `PipelineDriver`.
//! - [`ClientSchema`], [`ClientTableSchema`], [`ClientColumnSchema`] —
//!   minimal Rust views of the types from
//!   `packages/zero-protocol/src/client-schema.ts`. We only model the
//!   fields this function reads (`tables`, `columns.type`, `primaryKey`).
//!   Column `type` is carried as an opaque `String` ("string" | "number"
//!   | "boolean" | "null" | "json") to match the string-level equality
//!   comparison TS performs.
//! - [`LiteAndZqlSpec`], [`LiteTableSpec`], [`LiteColumnSpec`],
//!   [`LiteTableSpecWithKeys`] — minimal stand-ins for the types from
//!   `packages/zero-cache/src/db/specs.ts`. Only the fields
//!   `check_client_schema` touches are modelled. Full specs are out of
//!   scope for this file.
//! - [`ShardId`] — minimal stand-in for the type from
//!   `packages/zero-cache/src/types/shards.ts`. Only the fields required
//!   by the inlined `app_schema` / `upstream_schema` helpers are present.
//! - [`ClientSchemaError`] — Rust equivalent of the TS `ProtocolError`
//!   throws in this function. Carries the same [`ErrorKind`] /
//!   [`ErrorOrigin`] pair and is serialisable the same way.
//!
//! ## Stubs / inlined helpers
//!
//! - `ZERO_VERSION_COLUMN_NAME` is hardcoded as `"_0_version"`. Source:
//!   `packages/zero-cache/src/services/replicator/schema/constants.ts`.
//! - `appSchema` / `upstreamSchema` are inlined as [`app_schema`] /
//!   [`upstream_schema`] respectively. Source:
//!   `packages/zero-cache/src/types/shards.ts`.
//! - `LiteAndZqlSpec` / `LiteTableSpec` are trimmed to the fields this
//!   function reads. Source: `packages/zero-cache/src/db/specs.ts`.
//!
//! ## Branch coverage
//!
//! Every branch in the TS function has a corresponding unit test — see
//! the `tests` module below. Branch comments on each test name the
//! TS construct they cover.

use std::collections::BTreeSet;

use indexmap::IndexMap;
use thiserror::Error;
use zero_cache_types::error::{ErrorKind, ErrorOrigin};

// ─── ZERO_VERSION_COLUMN_NAME ────────────────────────────────────────
//
// Hardcoded from
// `packages/zero-cache/src/services/replicator/schema/constants.ts`:
//
//     export const ZERO_VERSION_COLUMN_NAME = '_0_version';
//
// Not a public re-export — that file lives outside the `view-syncer/`
// scope being ported here.

const ZERO_VERSION_COLUMN_NAME: &str = "_0_version";

// ─── ShardID helpers ─────────────────────────────────────────────────
//
// Inlined from `packages/zero-cache/src/types/shards.ts`:
//
//     export function appSchema({appID}: AppID) { return appID; }
//     export function upstreamSchema({appID, shardNum}: ShardID) {
//         return `${appID}_${shardNum}`;
//     }
//
// The TS function calls `check()` which asserts the appID/shardNum
// are well-formed. For this port we accept caller-supplied values
// directly — validation will happen at the ShardId construction site
// when we port `types/shards.ts`.

/// Minimal port of TS `ShardID` from `types/shards.ts`. Only the fields
/// `app_schema` / `upstream_schema` touch are modelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardId {
    pub app_id: String,
    pub shard_num: u32,
}

/// Port of TS `appSchema` from `types/shards.ts`.
pub fn app_schema(shard: &ShardId) -> &str {
    &shard.app_id
}

/// Port of TS `upstreamSchema` from `types/shards.ts`.
pub fn upstream_schema(shard: &ShardId) -> String {
    format!("{}_{}", shard.app_id, shard.shard_num)
}

// ─── ClientSchema types ──────────────────────────────────────────────

/// Minimal port of TS `ColumnSchema` from
/// `packages/zero-protocol/src/client-schema.ts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientColumnSchema {
    /// One of `"string" | "number" | "boolean" | "null" | "json"`.
    /// Carried as a `String` because TS compares via string equality.
    pub r#type: String,
}

/// Minimal port of TS `TableSchema` from
/// `packages/zero-protocol/src/client-schema.ts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTableSchema {
    /// Maps column name to the declared client type.
    pub columns: IndexMap<String, ClientColumnSchema>,
    /// The primary key, if the client declared one. `None` reproduces
    /// the TS "forced cast, missing primary key" test case.
    pub primary_key: Option<Vec<String>>,
}

/// Minimal port of TS `ClientSchema` from
/// `packages/zero-protocol/src/client-schema.ts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSchema {
    pub tables: IndexMap<String, ClientTableSchema>,
}

// ─── LiteAndZqlSpec ─────────────────────────────────────────────────

/// Minimal port of TS `ColumnSpec` from `db/specs.ts`. Only `dataType`
/// is read by `check_client_schema` (to emit the "unsupported data type"
/// error message for columns that exist in the replica but aren't
/// synced).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiteColumnSpec {
    pub data_type: String,
}

/// Minimal port of TS `LiteTableSpec` from `db/specs.ts`. Only `columns`
/// is read by `check_client_schema`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LiteTableSpec {
    pub columns: IndexMap<String, LiteColumnSpec>,
}

/// Minimal port of TS `LiteTableSpecWithKeysAndVersion` from
/// `db/specs.ts`. Only `allPotentialPrimaryKeys` is read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LiteTableSpecWithKeys {
    /// Every unique-over-non-null key the replica found.
    pub all_potential_primary_keys: Vec<Vec<String>>,
}

/// Minimal port of TS `SchemaValue` used inside `zqlSpec`. We only
/// need the `type` field (the string representation of a
/// [`crate::zqlite::table_source::ValueType`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZqlSchemaValue {
    pub r#type: String,
}

/// Minimal port of TS `LiteAndZqlSpec` from `db/specs.ts`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LiteAndZqlSpec {
    pub table_spec: LiteTableSpecWithKeys,
    /// Columns that survived ZQL filtering — i.e. can be synced to the
    /// client. Keyed by column name.
    pub zql_spec: IndexMap<String, ZqlSchemaValue>,
}

// ─── Error ───────────────────────────────────────────────────────────

/// Rust port of the TS `ProtocolError` throws in
/// `check_client_schema`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct ClientSchemaError {
    pub kind: ErrorKind,
    pub message: String,
    pub origin: ErrorOrigin,
}

impl ClientSchemaError {
    fn internal(message: String) -> Self {
        Self {
            kind: ErrorKind::Internal,
            message,
            origin: ErrorOrigin::ZeroCache,
        }
    }

    fn schema_version_not_supported(message: String) -> Self {
        Self {
            kind: ErrorKind::SchemaVersionNotSupported,
            message,
            origin: ErrorOrigin::ZeroCache,
        }
    }
}

// ─── check_client_schema ─────────────────────────────────────────────

/// Port of TS `checkClientSchema` from
/// `packages/zero-cache/src/services/view-syncer/client-schema.ts`.
///
/// Validates a client-declared schema against the replica's synced
/// specs. Returns `Ok(())` on success; otherwise returns a
/// [`ClientSchemaError`] whose `message` accumulates every validation
/// failure, joined by `\n` — matching TS behaviour.
pub fn check_client_schema(
    shard_id: &ShardId,
    client_schema: &ClientSchema,
    table_specs: &IndexMap<String, LiteAndZqlSpec>,
    full_tables: &IndexMap<String, LiteTableSpec>,
) -> Result<(), ClientSchemaError> {
    // Branch: nothing synced from upstream → Internal error.
    if full_tables.is_empty() {
        return Err(ClientSchemaError::internal(
            "No tables have been synced from upstream. \
             Please check that the ZERO_UPSTREAM_DB has been properly set."
                .to_string(),
        ));
    }

    let mut errors: Vec<String> = Vec::new();

    let client_tables: BTreeSet<String> = client_schema.tables.keys().cloned().collect();
    let replica_tables: BTreeSet<String> = table_specs.keys().cloned().collect();

    // Missing tables — client declares table X but replica doesn't sync it.
    // TS iterates `[...missingTables].sort()`.
    let missing_tables: BTreeSet<String> =
        client_tables.difference(&replica_tables).cloned().collect();
    for missing in &missing_tables {
        // Branch: table exists in upstream but couldn't be synced
        // (no PK / non-null unique index).
        if full_tables.contains_key(missing) {
            errors.push(format!(
                "The \"{missing}\" table is missing a primary key or non-null \
                 unique index and thus cannot be synced to the client",
            ));
        } else {
            // Branch: table is truly unknown.
            let app = format!("{}.", app_schema(shard_id));
            let shard = format!("{}.", upstream_schema(shard_id));
            let mut synced_tables: Vec<&str> = table_specs
                .keys()
                .filter(|t| !t.starts_with(&app) && !t.starts_with(&shard))
                .map(String::as_str)
                .collect();
            synced_tables.sort_unstable();
            let synced_list = synced_tables
                .iter()
                .map(|t| format!("\"{t}\""))
                .collect::<Vec<_>>()
                .join(",");
            // TS: `missing.includes('.') && !syncedTables.includes('.')`.
            // `syncedTables` is the pre-joined CSV string — so the
            // second check is against the string literally, not a set
            // membership. Mirror exactly.
            let schema_tip = if missing.contains('.') && !synced_list.contains('.') {
                " Note that zero does not sync tables from non-public schemas \
                 by default. Make sure you have defined a custom \
                 ZERO_APP_PUBLICATION to sync tables from non-public schemas."
            } else {
                ""
            };
            errors.push(format!(
                "The \"{missing}\" table does not exist or is not \
                 one of the replicated tables: {synced_list}.{schema_tip}",
            ));
        }
    }

    // Intersection of tables present on both sides. TS iterates sorted.
    let tables: BTreeSet<String> = client_tables
        .intersection(&replica_tables)
        .cloned()
        .collect();
    for table in &tables {
        let client_spec = client_schema
            .tables
            .get(table)
            .expect("guaranteed by intersection");
        let server_spec = table_specs.get(table).expect("guaranteed by intersection");
        // TS calls `must(fullTables.get(table))` — but fullTables only
        // contains tables that have actually been synced to Lite. If a
        // client table is in `tableSpecs` it must also be in
        // `fullTables` (the replicator seeds both together in
        // `computeZqlSpecs`). Matching the TS assertion: treat an
        // absent entry as a programmer error.
        let full_spec = full_tables
            .get(table)
            .expect("fullTables must contain every synced table");

        let client_columns: BTreeSet<String> = client_spec.columns.keys().cloned().collect();
        let synced_columns: BTreeSet<String> = server_spec.zql_spec.keys().cloned().collect();

        // Missing columns — declared by client but not in `zqlSpec`.
        let missing_columns: BTreeSet<String> = client_columns
            .difference(&synced_columns)
            .cloned()
            .collect();
        for missing in &missing_columns {
            // Branch: column exists in replica but unsupported type.
            if let Some(col) = full_spec.columns.get(missing) {
                errors.push(format!(
                    "The \"{table}\".\"{missing}\" column cannot be synced because it \
                     is of an unsupported data type \"{}\"",
                    col.data_type,
                ));
            } else {
                // Branch: column doesn't exist at all.
                let mut cols: Vec<&str> = synced_columns
                    .iter()
                    .filter(|c| c.as_str() != ZERO_VERSION_COLUMN_NAME)
                    .map(String::as_str)
                    .collect();
                cols.sort_unstable();
                let cols_list = cols
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                errors.push(format!(
                    "The \"{table}\".\"{missing}\" column does not exist \
                     or is not one of the replicated columns: {cols_list}.",
                ));
            }
        }

        // Column type mismatch — iterate intersection. TS uses
        // `intersection(clientColumns, syncedColumns)` which preserves
        // `clientColumns` insertion order (ES Set iteration order). To
        // keep output deterministic we sort.
        let shared: BTreeSet<String> = client_columns
            .intersection(&synced_columns)
            .cloned()
            .collect();
        for column in &shared {
            let client_type = &client_spec
                .columns
                .get(column)
                .expect("in intersection")
                .r#type;
            let server_type = &server_spec
                .zql_spec
                .get(column)
                .expect("in intersection")
                .r#type;
            if client_type != server_type {
                errors.push(format!(
                    "The \"{table}\".\"{column}\" column's upstream type \"{server_type}\" \
                     does not match the client type \"{client_type}\"",
                ));
            }
        }

        // Primary key validation.
        match &client_spec.primary_key {
            // Branch: client schema has no primary key at all.
            None => {
                errors.push(format!(
                    "The \"{table}\" table's client schema does not specify a primary key.",
                ));
            }
            Some(client_pk) => {
                let client_pk_set: BTreeSet<&str> = client_pk.iter().map(String::as_str).collect();
                // Branch: client PK doesn't match any replica unique index.
                let matches = server_spec
                    .table_spec
                    .all_potential_primary_keys
                    .iter()
                    .any(|key| {
                        let key_set: BTreeSet<&str> = key.iter().map(String::as_str).collect();
                        key_set == client_pk_set
                    });
                if !matches {
                    errors.push(format!(
                        "The \"{table}\" table's primaryKey <{}> \
                         is not associated with a non-null unique index.",
                        client_pk.join(","),
                    ));
                }
            }
        }
    }

    // Branch: accumulated errors → throw.
    if !errors.is_empty() {
        return Err(ClientSchemaError::schema_version_not_supported(
            errors.join("\n"),
        ));
    }
    Ok(())
}

// ─── Tests ───────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    fn shard() -> ShardId {
        ShardId {
            app_id: "zero".to_string(),
            shard_num: 0,
        }
    }

    fn col(t: &str) -> ClientColumnSchema {
        ClientColumnSchema {
            r#type: t.to_string(),
        }
    }

    fn zql_col(t: &str) -> ZqlSchemaValue {
        ZqlSchemaValue {
            r#type: t.to_string(),
        }
    }

    fn lcol(d: &str) -> LiteColumnSpec {
        LiteColumnSpec {
            data_type: d.to_string(),
        }
    }

    /// Build the same replica state the TS test constructs via SQL DDL.
    /// foo: id,a,b,c,d,e,f,notSyncedToClient,_0_version
    /// bar: id,d,e,f,_0_version
    /// nopk: (has no non-null unique index)
    fn replica() -> (
        IndexMap<String, LiteAndZqlSpec>,
        IndexMap<String, LiteTableSpec>,
    ) {
        let mut specs: IndexMap<String, LiteAndZqlSpec> = IndexMap::new();

        // foo — two unique indexes over non-null columns:
        //   foo_pkey (id)         → potential PK [id]
        //   foo_id_a_key (id, a)  → potential PK [id, a]
        let mut foo_zql: IndexMap<String, ZqlSchemaValue> = IndexMap::new();
        foo_zql.insert("id".to_string(), zql_col("string"));
        foo_zql.insert("a".to_string(), zql_col("number"));
        foo_zql.insert("b".to_string(), zql_col("boolean"));
        foo_zql.insert("c".to_string(), zql_col("json"));
        foo_zql.insert("d".to_string(), zql_col("number"));
        foo_zql.insert("e".to_string(), zql_col("number"));
        foo_zql.insert("f".to_string(), zql_col("number"));
        foo_zql.insert(ZERO_VERSION_COLUMN_NAME.to_string(), zql_col("string"));
        specs.insert(
            "foo".to_string(),
            LiteAndZqlSpec {
                table_spec: LiteTableSpecWithKeys {
                    all_potential_primary_keys: vec![
                        vec!["id".to_string()],
                        vec!["id".to_string(), "a".to_string()],
                    ],
                },
                zql_spec: foo_zql,
            },
        );

        // bar — pkey over id.
        let mut bar_zql: IndexMap<String, ZqlSchemaValue> = IndexMap::new();
        bar_zql.insert("id".to_string(), zql_col("string"));
        bar_zql.insert("d".to_string(), zql_col("number"));
        bar_zql.insert("e".to_string(), zql_col("boolean"));
        bar_zql.insert("f".to_string(), zql_col("json"));
        bar_zql.insert(ZERO_VERSION_COLUMN_NAME.to_string(), zql_col("string"));
        specs.insert(
            "bar".to_string(),
            LiteAndZqlSpec {
                table_spec: LiteTableSpecWithKeys {
                    all_potential_primary_keys: vec![vec!["id".to_string()]],
                },
                zql_spec: bar_zql,
            },
        );

        // Full tables: foo has the extra `notSyncedToClient` column;
        // bar has the same columns as zqlSpec; nopk is present but
        // isn't in `specs` (no unique non-null index).
        let mut full: IndexMap<String, LiteTableSpec> = IndexMap::new();
        let mut foo_cols: IndexMap<String, LiteColumnSpec> = IndexMap::new();
        foo_cols.insert("id".to_string(), lcol("text"));
        foo_cols.insert("a".to_string(), lcol("int"));
        foo_cols.insert("b".to_string(), lcol("bool"));
        foo_cols.insert("c".to_string(), lcol("json"));
        foo_cols.insert("d".to_string(), lcol("timestamp"));
        foo_cols.insert("e".to_string(), lcol("timestamptz"));
        foo_cols.insert("f".to_string(), lcol("date"));
        foo_cols.insert("notSyncedToClient".to_string(), lcol("custom_pg_type"));
        foo_cols.insert(ZERO_VERSION_COLUMN_NAME.to_string(), lcol("TEXT"));
        full.insert("foo".to_string(), LiteTableSpec { columns: foo_cols });

        let mut bar_cols: IndexMap<String, LiteColumnSpec> = IndexMap::new();
        bar_cols.insert("id".to_string(), lcol("text"));
        bar_cols.insert("d".to_string(), lcol("int"));
        bar_cols.insert("e".to_string(), lcol("bool"));
        bar_cols.insert("f".to_string(), lcol("json"));
        bar_cols.insert(ZERO_VERSION_COLUMN_NAME.to_string(), lcol("TEXT"));
        full.insert("bar".to_string(), LiteTableSpec { columns: bar_cols });

        let mut nopk_cols: IndexMap<String, LiteColumnSpec> = IndexMap::new();
        nopk_cols.insert("id".to_string(), lcol("text"));
        nopk_cols.insert("d".to_string(), lcol("int"));
        nopk_cols.insert("e".to_string(), lcol("bool"));
        nopk_cols.insert("f".to_string(), lcol("json"));
        nopk_cols.insert(ZERO_VERSION_COLUMN_NAME.to_string(), lcol("TEXT"));
        full.insert("nopk".to_string(), LiteTableSpec { columns: nopk_cols });

        (specs, full)
    }

    fn schema(tables: &[(&str, &[(&str, &str)], Option<&[&str]>)]) -> ClientSchema {
        let mut out: IndexMap<String, ClientTableSchema> = IndexMap::new();
        for (name, cols, pk) in tables {
            let mut columns: IndexMap<String, ClientColumnSchema> = IndexMap::new();
            for (c, t) in *cols {
                columns.insert((*c).to_string(), col(t));
            }
            out.insert(
                (*name).to_string(),
                ClientTableSchema {
                    columns,
                    primary_key: pk.map(|pk| pk.iter().map(|s| (*s).to_string()).collect()),
                },
            );
        }
        ClientSchema { tables: out }
    }

    // Branch: `fullTables.size === 0` → Internal error.
    #[test]
    fn nothing_synced() {
        let cs = schema(&[("foo", &[("id", "string")], Some(&["id"]))]);
        let err =
            check_client_schema(&shard(), &cs, &IndexMap::new(), &IndexMap::new()).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Internal);
        assert_eq!(err.origin, ErrorOrigin::ZeroCache);
        assert_eq!(
            err.message,
            "No tables have been synced from upstream. \
             Please check that the ZERO_UPSTREAM_DB has been properly set."
        );
    }

    // Branch: subset of valid tables → Ok.
    #[test]
    fn subset_ok_single_table() {
        let (specs, full) = replica();
        let cs = schema(&[("bar", &[("id", "string"), ("d", "number")], Some(&["id"]))]);
        check_client_schema(&shard(), &cs, &specs, &full).unwrap();
    }

    // Branch: empty client schema → Ok (loop bodies never executed).
    #[test]
    fn empty_client_schema_ok() {
        let (specs, full) = replica();
        let cs = ClientSchema {
            tables: IndexMap::new(),
        };
        check_client_schema(&shard(), &cs, &specs, &full).unwrap();
    }

    // Branch: all columns for both tables present, correct PK → Ok.
    #[test]
    fn both_tables_full_ok() {
        let (specs, full) = replica();
        let cs = schema(&[
            (
                "bar",
                &[
                    ("e", "boolean"),
                    ("id", "string"),
                    ("f", "json"),
                    ("d", "number"),
                ],
                Some(&["id"]),
            ),
            (
                "foo",
                &[
                    ("c", "json"),
                    ("id", "string"),
                    ("a", "number"),
                    ("b", "boolean"),
                ],
                Some(&["id"]),
            ),
        ]);
        check_client_schema(&shard(), &cs, &specs, &full).unwrap();
    }

    // Branch: all unique indexes can be PK — here ['id','a'].
    #[test]
    fn composite_primary_key_ok() {
        let (specs, full) = replica();
        let cs = schema(&[(
            "foo",
            &[
                ("c", "json"),
                ("id", "string"),
                ("a", "number"),
                ("b", "boolean"),
                ("d", "number"),
                ("e", "number"),
                ("f", "number"),
            ],
            Some(&["id", "a"]),
        )]);
        check_client_schema(&shard(), &cs, &specs, &full).unwrap();
    }

    // Same — PK order doesn't matter (set equality).
    #[test]
    fn composite_primary_key_reversed_ok() {
        let (specs, full) = replica();
        let cs = schema(&[(
            "foo",
            &[
                ("id", "string"),
                ("a", "number"),
                ("b", "boolean"),
                ("c", "json"),
                ("d", "number"),
                ("e", "number"),
                ("f", "number"),
            ],
            Some(&["a", "id"]),
        )]);
        check_client_schema(&shard(), &cs, &specs, &full).unwrap();
    }

    // Branch: missing table from non-public schema → schemaTip added.
    #[test]
    fn missing_table_non_public_schema() {
        let (specs, full) = replica();
        let cs = schema(&[("yyy.zzz", &[("id", "string")], Some(&["id"]))]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        assert_eq!(err.kind, ErrorKind::SchemaVersionNotSupported);
        assert_eq!(
            err.message,
            "The \"yyy.zzz\" table does not exist or is not one of the replicated tables: \
             \"bar\",\"foo\". Note that zero does not sync tables from non-public schemas \
             by default. Make sure you have defined a custom ZERO_APP_PUBLICATION to sync \
             tables from non-public schemas."
        );
    }

    // Branch: missing table (public) + missing column in another table.
    #[test]
    fn missing_table_and_missing_column() {
        let (specs, full) = replica();
        let cs = schema(&[
            (
                "bar",
                &[
                    ("e", "boolean"),
                    ("id", "string"),
                    ("f", "json"),
                    ("d", "number"),
                    ("zzz", "number"),
                ],
                Some(&["id"]),
            ),
            (
                "foo",
                &[
                    ("c", "json"),
                    ("id", "string"),
                    ("a", "number"),
                    ("b", "boolean"),
                ],
                Some(&["id"]),
            ),
            ("yyy", &[("id", "string")], Some(&["id"])),
        ]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        // TS snapshot:
        //   `The "yyy" table does not exist or is not one of the replicated tables: "bar","foo".
        //   The "bar"."zzz" column does not exist or is not one of the replicated columns: "d","e","f","id".`
        assert_eq!(
            err.message,
            "The \"yyy\" table does not exist or is not one of the replicated tables: \
             \"bar\",\"foo\".\n\
             The \"bar\".\"zzz\" column does not exist or is not one of the replicated \
             columns: \"d\",\"e\",\"f\",\"id\"."
        );
    }

    // Branch: column exists in fullSpec but unsupported type.
    #[test]
    fn unsupported_column_type() {
        let (specs, full) = replica();
        let cs = schema(&[(
            "foo",
            &[
                ("c", "json"),
                ("id", "string"),
                ("a", "number"),
                ("b", "boolean"),
                ("notSyncedToClient", "json"),
            ],
            Some(&["id"]),
        )]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        assert_eq!(
            err.message,
            "The \"foo\".\"notSyncedToClient\" column cannot be synced because it is of an \
             unsupported data type \"custom_pg_type\""
        );
    }

    // Branch: column type mismatch (multiple).
    #[test]
    fn column_type_mismatch() {
        let (specs, full) = replica();
        let cs = schema(&[(
            "foo",
            &[
                ("c", "json"),
                ("id", "string"),
                ("a", "string"),
                ("b", "number"),
                ("d", "number"),
                ("e", "number"),
                ("f", "number"),
            ],
            Some(&["id"]),
        )]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        assert_eq!(
            err.message,
            "The \"foo\".\"a\" column's upstream type \"number\" does not match the client \
             type \"string\"\n\
             The \"foo\".\"b\" column's upstream type \"boolean\" does not match the client \
             type \"number\""
        );
    }

    // Branch: table exists in fullTables but has no non-null unique index.
    #[test]
    fn table_missing_primary_key_on_replica() {
        let (specs, full) = replica();
        let cs = schema(&[
            ("nopk", &[("id", "string")], Some(&["id"])),
            (
                "foo",
                &[
                    ("c", "json"),
                    ("id", "string"),
                    ("a", "number"),
                    ("b", "boolean"),
                ],
                Some(&["id"]),
            ),
        ]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        assert_eq!(
            err.message,
            "The \"nopk\" table is missing a primary key or non-null unique index and thus \
             cannot be synced to the client"
        );
    }

    // Branch: client schema omits primary_key entirely.
    #[test]
    fn client_missing_primary_key() {
        let (specs, full) = replica();
        let cs = schema(&[(
            "foo",
            &[
                ("c", "json"),
                ("id", "string"),
                ("a", "number"),
                ("b", "boolean"),
            ],
            None,
        )]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        assert_eq!(
            err.message,
            "The \"foo\" table's client schema does not specify a primary key."
        );
    }

    // Branch: client declares a PK that doesn't match any unique index.
    #[test]
    fn client_wrong_primary_key_single() {
        let (specs, full) = replica();
        let cs = schema(&[(
            "foo",
            &[
                ("c", "json"),
                ("id", "string"),
                ("a", "number"),
                ("b", "boolean"),
            ],
            Some(&["a"]),
        )]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        assert_eq!(
            err.message,
            "The \"foo\" table's primaryKey <a> is not associated with a non-null unique \
             index."
        );
    }

    // Same branch, composite PK that isn't in allPotentialPrimaryKeys.
    #[test]
    fn client_wrong_primary_key_composite() {
        let (specs, full) = replica();
        let cs = schema(&[(
            "foo",
            &[
                ("c", "json"),
                ("id", "string"),
                ("a", "number"),
                ("b", "boolean"),
            ],
            Some(&["id", "a", "b"]),
        )]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        assert_eq!(
            err.message,
            "The \"foo\" table's primaryKey <id,a,b> is not associated with a non-null \
             unique index."
        );
    }

    // Branch: missing column (not in fullSpec at all) → lists available
    // replicated columns, and _0_version is filtered out.
    #[test]
    fn missing_column_lists_replicated_columns_without_version() {
        let (specs, full) = replica();
        let cs = schema(&[("bar", &[("id", "string"), ("zzz", "number")], Some(&["id"]))]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        assert_eq!(
            err.message,
            "The \"bar\".\"zzz\" column does not exist or is not one of the replicated \
             columns: \"d\",\"e\",\"f\",\"id\"."
        );
    }

    // Branch: non-public-looking name but syncedTables contains '.' →
    // schemaTip is suppressed. Exercises the TS
    // `!syncedTables.includes('.')` test on the joined CSV string.
    #[test]
    fn missing_table_suppresses_tip_when_synced_csv_contains_dot() {
        let (mut specs, mut full) = replica();
        // Insert a synced table whose name contains a '.' so the
        // pre-joined CSV also contains one. Avoid the appID/shardNum
        // prefixes so the table isn't filtered out.
        specs.insert(
            "other.thing".to_string(),
            LiteAndZqlSpec {
                table_spec: LiteTableSpecWithKeys {
                    all_potential_primary_keys: vec![vec!["id".to_string()]],
                },
                zql_spec: {
                    let mut m = IndexMap::new();
                    m.insert("id".to_string(), zql_col("string"));
                    m
                },
            },
        );
        let mut cols: IndexMap<String, LiteColumnSpec> = IndexMap::new();
        cols.insert("id".to_string(), lcol("text"));
        full.insert("other.thing".to_string(), LiteTableSpec { columns: cols });

        let cs = schema(&[("yyy.zzz", &[("id", "string")], Some(&["id"]))]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        // No schemaTip because the synced list already has a dot.
        assert_eq!(
            err.message,
            "The \"yyy.zzz\" table does not exist or is not one of the replicated tables: \
             \"bar\",\"foo\",\"other.thing\"."
        );
    }

    // Branch: synced tables include appID/shardNum-prefixed internal
    // tables — they must be filtered out of the error's CSV.
    #[test]
    fn internal_tables_are_filtered_from_error_list() {
        let (mut specs, mut full) = replica();
        // Add the "zero.permissions" and "zero_0.clients" internal
        // tables — TS: `CREATE TABLE "zero.permissions"` and
        // `"zero_0.clients"`.
        for name in ["zero.permissions", "zero_0.clients"] {
            specs.insert(
                name.to_string(),
                LiteAndZqlSpec {
                    table_spec: LiteTableSpecWithKeys {
                        all_potential_primary_keys: vec![vec!["lock".to_string()]],
                    },
                    zql_spec: {
                        let mut m = IndexMap::new();
                        m.insert("lock".to_string(), zql_col("boolean"));
                        m
                    },
                },
            );
            let mut cols: IndexMap<String, LiteColumnSpec> = IndexMap::new();
            cols.insert("lock".to_string(), lcol("bool"));
            full.insert(name.to_string(), LiteTableSpec { columns: cols });
        }

        let cs = schema(&[("yyy", &[("id", "string")], Some(&["id"]))]);
        let err = check_client_schema(&shard(), &cs, &specs, &full).unwrap_err();
        // Internal tables do not appear; synced CSV has no '.' so the
        // tip is suppressed (missing name has no dot either).
        assert_eq!(
            err.message,
            "The \"yyy\" table does not exist or is not one of the replicated tables: \
             \"bar\",\"foo\"."
        );
    }

    // Cover `app_schema` / `upstream_schema` directly.
    #[test]
    fn shard_schema_helpers() {
        let s = ShardId {
            app_id: "zero".to_string(),
            shard_num: 3,
        };
        assert_eq!(app_schema(&s), "zero");
        assert_eq!(upstream_schema(&s), "zero_3");
    }
}
