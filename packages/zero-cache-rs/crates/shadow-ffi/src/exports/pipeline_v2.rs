//! napi exports for PipelineV2 (view_syncer_v2). Production-facing —
//! no diff / verification against TS.
//!
//! TS wrapper must call these instead of the ivm_v1 `pipeline_driver_*`
//! exports to switch over.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use napi::bindgen_prelude::External;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use zero_cache_sync_worker::ivm::change::{AddChange, Change, EditChange, RemoveChange};
use zero_cache_sync_worker::ivm::data::{make_comparator, Node};
use zero_cache_sync_worker::ivm::schema::SourceSchema;
use zero_cache_sync_worker::ivm_v2::source::SnapshotReader;
use zero_cache_sync_worker::view_syncer_v2::ast_builder::ast_to_chain_spec;
use zero_cache_sync_worker::view_syncer_v2::driver::{
    AddQueryReq, NextChunk, PipelineV2, SourceFactory,
};
use zero_cache_sync_worker::view_syncer_v2::row_change::RowChange;
use zero_cache_sync_worker::view_syncer_v2::sqlite_source::SqliteSource;
use zero_cache_types::ast::{Direction, Ordering, System, AST};
use zero_cache_types::primary_key::PrimaryKey;

/// Registry of table schemas supplied by TS at create-time; the source
/// factory consults this to build `SqliteSource` for a given table.
#[derive(Clone)]
struct TableMeta {
    columns: Vec<String>,
    primary_key: Vec<String>,
    sort: Ordering,
    /// Column → logical Zero type (`"boolean"` | `"number"` | `"string"` |
    /// `"null"` | `"json"`). Used by `SqliteSource` to coerce row values
    /// out of raw SQLite storage (INTEGER 0/1 → Bool, JSON strings →
    /// parsed JSON, bigint overflow checks) so downstream predicate
    /// evaluation compares canonical JS types rather than relying on
    /// ad-hoc `Number↔Bool` patches in the comparators.
    column_types: HashMap<String, String>,
}

pub struct PipelineV2Handle {
    driver: Mutex<PipelineV2>,
    tables: Arc<Mutex<HashMap<String, TableMeta>>>,
    /// Driver-scoped SQLite reader. Opens one rusqlite connection on the
    /// replica file with an open `BEGIN` read transaction. Every
    /// `SqliteSource` built by the source factory holds a cheap clone,
    /// so all reads in the driver see the same pinned snapshot. TS
    /// triggers `refresh()` after each `snapshotter.advance*` call.
    reader: SnapshotReader,
}

fn run<R>(label: &'static str, f: impl FnOnce() -> Result<R, String>) -> napi::Result<R> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(msg)) => Err(napi::Error::from_reason(format!("{label}: {msg}"))),
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| format!("{label}: panic"));
            Err(napi::Error::from_reason(msg))
        }
    }
}

// ─── Handle construction ─────────────────────────────────────────────

#[napi(js_name = "pipeline_v2_create")]
pub fn pipeline_v2_create(db_path: String) -> napi::Result<External<PipelineV2Handle>> {
    run("pipeline_v2_create", || {
        let tables: Arc<Mutex<HashMap<String, TableMeta>>> = Arc::new(Mutex::new(HashMap::new()));
        let tables_for_factory = Arc::clone(&tables);
        let path = PathBuf::from(db_path);
        // One shared reader per driver — every SqliteSource we build
        // gets a cheap clone of this `SnapshotReader`, so reads across
        // all sources agree on the same pinned snapshot.
        let reader = SnapshotReader::new(path);
        let reader_for_factory = reader.clone();
        let factory: SourceFactory = Box::new(move |table: &str| {
            let meta = match tables_for_factory.lock().ok().and_then(|t| t.get(table).cloned()) {
                Some(m) => m,
                None => panic!("pipeline_v2: unknown table {}", table),
            };
            let schema = build_schema_from_meta(table, &meta);
            Box::new(SqliteSource::new_with_column_types(
                reader_for_factory.clone(),
                schema,
                meta.columns.clone(),
                meta.column_types.clone(),
            ))
        });
        let driver = PipelineV2::new(factory);
        Ok(External::new(PipelineV2Handle {
            driver: Mutex::new(driver),
            tables,
            reader,
        }))
    })
}

fn build_schema_from_meta(table: &str, meta: &TableMeta) -> SourceSchema {
    let mut columns = IndexMap::new();
    for c in &meta.columns {
        columns.insert(c.clone(), serde_json::Value::Null);
    }
    SourceSchema {
        table_name: table.into(),
        columns,
        primary_key: PrimaryKey::new(meta.primary_key.clone()),
        relationships: IndexMap::new(),
        is_hidden: false,
        system: System::Client,
        compare_rows: Arc::new(make_comparator(meta.sort.clone(), false)),
        sort: meta.sort.clone(),
    }
}

// ─── Schema registration ─────────────────────────────────────────────

#[derive(Deserialize)]
struct TableMetaJson {
    columns: Vec<String>,
    #[serde(rename = "primaryKey")]
    primary_key: Vec<String>,
    /// Optional sort; defaults to asc on primary-key columns.
    #[serde(default)]
    sort: Option<Vec<(String, String)>>,
    /// Column → Zero schema type (`boolean`|`number`|`string`|`null`|`json`).
    /// Populated by TS `rust-pipeline-driver-v2.ts#syncRustState` from
    /// `zqlSpec`. Columns not listed fall back to pass-through.
    #[serde(rename = "columnTypes", default)]
    column_types: HashMap<String, String>,
}

/// Register table schemas (columns, primary key, sort direction) so the
/// factory can build sources on demand. Must be called before `init`.
#[napi(js_name = "pipeline_v2_register_tables")]
pub fn pipeline_v2_register_tables(
    handle: &External<PipelineV2Handle>,
    tables_json: JsonValue,
) -> napi::Result<()> {
    run("pipeline_v2_register_tables", || {
        let input: IndexMap<String, TableMetaJson> =
            serde_json::from_value(tables_json).map_err(|e| e.to_string())?;
        let mut tables = handle
            .tables
            .lock()
            .map_err(|_| "tables mutex poisoned".to_string())?;
        for (name, json) in input {
            let sort: Ordering = if let Some(explicit) = json.sort {
                explicit
                    .into_iter()
                    .map(|(c, d)| {
                        let dir = if d.eq_ignore_ascii_case("desc") {
                            Direction::Desc
                        } else {
                            Direction::Asc
                        };
                        (c, dir)
                    })
                    .collect()
            } else {
                json.primary_key
                    .iter()
                    .cloned()
                    .map(|c| (c, Direction::Asc))
                    .collect()
            };
            tables.insert(
                name,
                TableMeta {
                    columns: json.columns,
                    primary_key: json.primary_key,
                    sort,
                    column_types: json.column_types,
                },
            );
        }
        Ok(())
    })
}

// ─── init / replica_version / current_version / destroy ──────────────

#[napi(js_name = "pipeline_v2_init")]
pub fn pipeline_v2_init(
    handle: &External<PipelineV2Handle>,
    replica_version: String,
) -> napi::Result<()> {
    run("pipeline_v2_init", || {
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        d.init(replica_version);
        Ok(())
    })
}

#[napi(js_name = "pipeline_v2_initialized")]
pub fn pipeline_v2_initialized(handle: &External<PipelineV2Handle>) -> napi::Result<bool> {
    run("pipeline_v2_initialized", || {
        let d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        Ok(d.initialized())
    })
}

#[napi(js_name = "pipeline_v2_replica_version")]
pub fn pipeline_v2_replica_version(
    handle: &External<PipelineV2Handle>,
) -> napi::Result<Option<String>> {
    run("pipeline_v2_replica_version", || {
        let d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        Ok(d.replica_version().map(String::from))
    })
}

#[napi(js_name = "pipeline_v2_current_version")]
pub fn pipeline_v2_current_version(
    handle: &External<PipelineV2Handle>,
) -> napi::Result<Option<String>> {
    run("pipeline_v2_current_version", || {
        let d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        Ok(d.current_version().map(String::from))
    })
}

/// Release the driver's current read-snapshot pin and re-pin at the
/// current DB head. Called by the TS wrapper after each
/// `Snapshotter.advance` / `advanceWithoutDiff` / `reset` so that every
/// subsequent Rust read (hydrate, `getRow`, refetch) sees exactly the
/// version the driver believes it is at — matching TS's
/// `table.setDB(curr.db.db)` loop which does the same thing on the
/// JS side.
#[napi(js_name = "pipeline_v2_refresh_snapshot")]
pub fn pipeline_v2_refresh_snapshot(
    handle: &External<PipelineV2Handle>,
) -> napi::Result<String> {
    run("pipeline_v2_refresh_snapshot", || {
        handle.reader.refresh()
    })
}

/// TS `PipelineDriver.reset(clientSchema)` Rust half — clear chains,
/// infos, in-flight hydrations, and lookup sources. Keeps `initialized`
/// true. Caller (TS wrapper) re-runs the initAndResetCommon sequence in
/// JS, then re-registers tables and calls `pipeline_v2_init`.
#[napi(js_name = "pipeline_v2_reset")]
pub fn pipeline_v2_reset(handle: &External<PipelineV2Handle>) -> napi::Result<()> {
    run("pipeline_v2_reset", || {
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        d.reset();
        Ok(())
    })
}

#[napi(js_name = "pipeline_v2_destroy")]
pub fn pipeline_v2_destroy(handle: &External<PipelineV2Handle>) -> napi::Result<()> {
    run("pipeline_v2_destroy", || {
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        d.destroy();
        Ok(())
    })
}

// ─── add_query / remove_query ────────────────────────────────────────

#[napi(js_name = "pipeline_v2_add_query")]
pub fn pipeline_v2_add_query(
    handle: &External<PipelineV2Handle>,
    transformation_hash: String,
    query_id: String,
    ast_json: JsonValue,
) -> napi::Result<JsonValue> {
    run("pipeline_v2_add_query", || {
        let ast: AST = serde_json::from_value(ast_json).map_err(|e| e.to_string())?;
        let tables = handle
            .tables
            .lock()
            .map_err(|_| "tables mutex poisoned".to_string())?;
        let meta = tables
            .get(&ast.table)
            .ok_or_else(|| format!("unknown table {}", ast.table))?
            .clone();
        drop(tables);
        let pk = PrimaryKey::new(meta.primary_key);
        let spec =
            ast_to_chain_spec(&ast, query_id.clone(), pk).map_err(|e| e.to_string())?;
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        let rows = d.add_query(transformation_hash, spec);
        Ok(json!(rows.iter().map(row_change_to_json).collect::<Vec<_>>()))
    })
}

#[napi(js_name = "pipeline_v2_remove_query")]
pub fn pipeline_v2_remove_query(
    handle: &External<PipelineV2Handle>,
    query_id: String,
) -> napi::Result<()> {
    run("pipeline_v2_remove_query", || {
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        d.remove_query(&query_id);
        Ok(())
    })
}

/// Start streaming hydration for a query. Returns immediately; rows are
/// drained via `pipeline_v2_next_chunk`. Pairs with `_next_chunk` to
/// match TS `PipelineDriver.addQuery` generator semantics with 100-row
/// batching across the napi boundary (see `ivm_v2::batching`).
#[napi(js_name = "pipeline_v2_add_query_start")]
pub fn pipeline_v2_add_query_start(
    handle: &External<PipelineV2Handle>,
    transformation_hash: String,
    query_id: String,
    ast_json: JsonValue,
) -> napi::Result<()> {
    run("pipeline_v2_add_query_start", || {
        let ast: AST = serde_json::from_value(ast_json).map_err(|e| e.to_string())?;
        let tables = handle
            .tables
            .lock()
            .map_err(|_| "tables mutex poisoned".to_string())?;
        let meta = tables
            .get(&ast.table)
            .ok_or_else(|| format!("unknown table {}", ast.table))?
            .clone();
        drop(tables);
        let pk = PrimaryKey::new(meta.primary_key);
        let spec = ast_to_chain_spec(&ast, query_id.clone(), pk).map_err(|e| e.to_string())?;
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        d.start_hydration(transformation_hash, spec);
        Ok(())
    })
}

/// Block on the next hydration chunk for `query_id`. Returns JSON:
///
/// ```json
/// { "rows": [...V2RowChange...], "isFinal": bool, "hydrationTimeMs": number|null }
/// ```
///
/// `hydrationTimeMs` is non-null only on the terminal chunk. TS loops
/// this until `isFinal === true`.
#[napi(js_name = "pipeline_v2_next_chunk")]
pub fn pipeline_v2_next_chunk(
    handle: &External<PipelineV2Handle>,
    query_id: String,
) -> napi::Result<JsonValue> {
    run("pipeline_v2_next_chunk", || {
        let chunk: NextChunk = {
            let mut d = handle
                .driver
                .lock()
                .map_err(|_| "driver mutex poisoned".to_string())?;
            d.next_chunk(&query_id)?
        };
        Ok(json!({
            "rows": chunk.rows.iter().map(row_change_to_json).collect::<Vec<_>>(),
            "isFinal": chunk.is_final,
            "hydrationTimeMs": chunk.hydration_time_ms,
        }))
    })
}

// ─── advance ──────────────────────────────────────────────────────────

/// Build an INSERT/UPDATE/DELETE SQL statement + params for a single
/// change on `table` using the columns / primary_key captured by
/// `pipeline_v2_register_tables`. Mirror of TS
/// `TableSource::#writeChange` at
/// `packages/zqlite/src/table-source.ts:426-471` which prepares the
/// same three statements from the table spec and binds row values by
/// position. Values pass through `json_to_sqlite` for JSON→SQLite
/// coercion (matches TS `toSQLiteTypes` at
/// `packages/zqlite/src/internal/tables.ts:120-144`).
fn sql_for_change(
    change: &Change,
    table: &str,
    meta: &TableMeta,
) -> Option<(String, Vec<rusqlite::types::Value>)> {
    let quoted_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let quoted_table = quoted_ident(table);
    match change {
        Change::Add(a) => {
            let mut cols: Vec<String> = Vec::new();
            let mut vals: Vec<rusqlite::types::Value> = Vec::new();
            for col in &meta.columns {
                let v = a.node.row.get(col).cloned().unwrap_or(None);
                cols.push(quoted_ident(col));
                vals.push(json_to_sqlite(v));
            }
            let placeholders: Vec<&str> = vals.iter().map(|_| "?").collect();
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                quoted_table,
                cols.join(","),
                placeholders.join(",")
            );
            Some((sql, vals))
        }
        Change::Remove(r) => {
            let mut where_clauses: Vec<String> = Vec::new();
            let mut vals: Vec<rusqlite::types::Value> = Vec::new();
            for k in &meta.primary_key {
                where_clauses.push(format!("{}=?", quoted_ident(k)));
                vals.push(json_to_sqlite(r.node.row.get(k).cloned().unwrap_or(None)));
            }
            if where_clauses.is_empty() {
                return None;
            }
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                quoted_table,
                where_clauses.join(" AND ")
            );
            Some((sql, vals))
        }
        Change::Edit(e) => {
            // All non-PK columns → SET. PK columns → WHERE (keyed by
            // the OLD row so PK-changing edits still locate the
            // target row — TS also updates against oldRow keys
            // at `table-source.ts:457-468`).
            let pk_set: std::collections::HashSet<&str> =
                meta.primary_key.iter().map(String::as_str).collect();
            let non_pk: Vec<&String> = meta
                .columns
                .iter()
                .filter(|c| !pk_set.contains(c.as_str()))
                .collect();
            if non_pk.is_empty() {
                return None;
            }
            let mut vals: Vec<rusqlite::types::Value> = Vec::new();
            let set_clauses: Vec<String> = non_pk
                .iter()
                .map(|c| {
                    vals.push(json_to_sqlite(
                        e.node.row.get(c.as_str()).cloned().unwrap_or(None),
                    ));
                    format!("{}=?", quoted_ident(c))
                })
                .collect();
            let where_clauses: Vec<String> = meta
                .primary_key
                .iter()
                .map(|k| {
                    vals.push(json_to_sqlite(
                        e.old_node.row.get(k.as_str()).cloned().unwrap_or(None),
                    ));
                    format!("{}=?", quoted_ident(k))
                })
                .collect();
            let sql = format!(
                "UPDATE {} SET {} WHERE {}",
                quoted_table,
                set_clauses.join(","),
                where_clauses.join(" AND ")
            );
            Some((sql, vals))
        }
        Change::Child(_) => None,
    }
}

/// Coerce a serialized JSON value (Option<serde_json::Value>) into the
/// matching rusqlite `Value` for bind-position params. Arrays / objects
/// are stored as JSON strings — matches TS `toSQLiteTypes`.
fn json_to_sqlite(v: Option<serde_json::Value>) -> rusqlite::types::Value {
    use rusqlite::types::Value as V;
    match v {
        None => V::Null,
        Some(serde_json::Value::Null) => V::Null,
        Some(serde_json::Value::Bool(b)) => V::Integer(if b { 1 } else { 0 }),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                V::Integer(i)
            } else if let Some(f) = n.as_f64() {
                V::Real(f)
            } else {
                V::Text(n.to_string())
            }
        }
        Some(serde_json::Value::String(s)) => V::Text(s),
        Some(other) => V::Text(other.to_string()),
    }
}

#[napi(js_name = "pipeline_v2_advance")]
pub fn pipeline_v2_advance(
    handle: &External<PipelineV2Handle>,
    table: String,
    change_json: JsonValue,
) -> napi::Result<JsonValue> {
    run("pipeline_v2_advance", || {
        let change = parse_change(&change_json)?;
        // Apply the mutation to the pinned read+write tx BEFORE
        // routing it through the IVM pipeline. Mirror of TS
        // `TableSource::genPush` which calls `writeChange(change)` at
        // `packages/zql/src/ivm/memory-source.ts:511` (wrapped by
        // `packages/zqlite/src/table-source.ts:414-423`). TS writes
        // to the prev-snapshot connection so subsequent pushes in the
        // same advance loop (e.g. step 2 querying the channels source
        // after step 1 has added a channel) observe the updated state.
        // Without this, RS's source stays frozen at the pinned
        // snapshot across all diff iterations — `Join::#pushChildChange`
        // at `packages/zql/src/ivm/join.ts:227` fetches
        // `this.#parent.fetch({constraint})` and misses rows that
        // earlier changes in the same diff added, producing silent
        // push-path divergences (fuzz_00089 channels.whereExists
        // ('participants') canary). Rolled back at the next
        // `pipeline_v2_refresh_snapshot`, matching TS Snapshotter
        // semantics (never commits — see
        // `packages/zero-cache/src/services/view-syncer/snapshotter.ts:54-56`).
        let trace = std::env::var("IVM_PARITY_TRACE").is_ok();
        let meta = {
            let tables = handle
                .tables
                .lock()
                .map_err(|_| "tables mutex poisoned".to_string())?;
            tables.get(&table).cloned()
        };
        if let Some(meta) = meta {
            if let Some((sql, params)) = sql_for_change(&change, &table, &meta) {
                if let Err(e) = handle.reader.apply_exec(sql.clone(), params) {
                    // Write conflicts (e.g. duplicate PK from a
                    // previous tx) are non-fatal for parity: we log
                    // but still run the IVM pipeline since the
                    // downstream assertion failures are more
                    // informative than a hidden silent skip.
                    if trace {
                        eprintln!(
                            "[ivm:rs:driver:apply_exec] table={} err={}",
                            table, e
                        );
                    }
                }
            }
        }
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        let rows = d.advance(&table, change);
        Ok(json!(rows.iter().map(row_change_to_json).collect::<Vec<_>>()))
    })
}

#[napi(js_name = "pipeline_v2_advance_without_diff")]
pub fn pipeline_v2_advance_without_diff(
    handle: &External<PipelineV2Handle>,
    new_version: String,
) -> napi::Result<()> {
    run("pipeline_v2_advance_without_diff", || {
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        d.advance_without_diff(new_version);
        Ok(())
    })
}

// ─── get_row / queries / hydration_budget_breakdown ──────────────────

#[napi(js_name = "pipeline_v2_get_row")]
pub fn pipeline_v2_get_row(
    handle: &External<PipelineV2Handle>,
    table: String,
    pk_json: JsonValue,
) -> napi::Result<Option<JsonValue>> {
    run("pipeline_v2_get_row", || {
        let pk_row: zero_cache_types::value::Row =
            serde_json::from_value(pk_json).map_err(|e| e.to_string())?;
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        Ok(d.get_row(&table, &pk_row).map(|r| serde_json::to_value(&r).unwrap()))
    })
}

#[napi(js_name = "pipeline_v2_queries")]
pub fn pipeline_v2_queries(handle: &External<PipelineV2Handle>) -> napi::Result<JsonValue> {
    run("pipeline_v2_queries", || {
        let d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        let items: Vec<JsonValue> = d
            .queries()
            .iter()
            .map(|(qid, info)| {
                json!({
                    "queryID": qid,
                    "transformationHash": info.transformation_hash,
                    "table": info.table,
                    "hydrationTimeMs": info.hydration_time_ms,
                })
            })
            .collect();
        Ok(JsonValue::Array(items))
    })
}

#[napi(js_name = "pipeline_v2_hydration_budget_breakdown")]
pub fn pipeline_v2_hydration_budget_breakdown(
    handle: &External<PipelineV2Handle>,
) -> napi::Result<JsonValue> {
    run("pipeline_v2_hydration_budget_breakdown", || {
        let d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        let items: Vec<JsonValue> = d
            .hydration_budget_breakdown()
            .into_iter()
            .map(|b| {
                json!({
                    "id": b.id,
                    "table": b.table,
                    "ms": b.ms,
                })
            })
            .collect();
        Ok(JsonValue::Array(items))
    })
}

#[napi(js_name = "pipeline_v2_total_hydration_time_ms")]
pub fn pipeline_v2_total_hydration_time_ms(handle: &External<PipelineV2Handle>) -> napi::Result<f64> {
    run("pipeline_v2_total_hydration_time_ms", || {
        let d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        Ok(d.total_hydration_time_ms())
    })
}

// ─── add_queries (batched) ───────────────────────────────────────────

#[derive(Deserialize)]
struct AddQueryJson {
    #[serde(rename = "transformationHash")]
    transformation_hash: String,
    #[serde(rename = "queryID")]
    query_id: String,
    ast: JsonValue,
}

#[napi(js_name = "pipeline_v2_add_queries")]
pub fn pipeline_v2_add_queries(
    handle: &External<PipelineV2Handle>,
    batch_json: JsonValue,
) -> napi::Result<JsonValue> {
    run("pipeline_v2_add_queries", || {
        let reqs_json: Vec<AddQueryJson> =
            serde_json::from_value(batch_json).map_err(|e| e.to_string())?;
        let mut reqs: Vec<AddQueryReq> = Vec::with_capacity(reqs_json.len());
        for r in reqs_json {
            let ast: AST = serde_json::from_value(r.ast).map_err(|e| e.to_string())?;
            let tables = handle
                .tables
                .lock()
                .map_err(|_| "tables mutex poisoned".to_string())?;
            let meta = tables
                .get(&ast.table)
                .ok_or_else(|| format!("unknown table {}", ast.table))?
                .clone();
            drop(tables);
            let pk = PrimaryKey::new(meta.primary_key);
            let spec =
                ast_to_chain_spec(&ast, r.query_id.clone(), pk).map_err(|e| e.to_string())?;
            reqs.push(AddQueryReq {
                transformation_hash: r.transformation_hash,
                spec,
            });
        }
        let mut d = handle
            .driver
            .lock()
            .map_err(|_| "driver mutex poisoned".to_string())?;
        let results = d.add_queries(reqs);
        let out: Vec<JsonValue> = results
            .into_iter()
            .map(|r| {
                let rows: Vec<JsonValue> = r.hydrated.iter().map(row_change_to_json).collect();
                json!({
                    "queryID": r.query_id,
                    "hydrationTimeMs": r.hydration_time_ms,
                    "rows": rows,
                })
            })
            .collect();
        Ok(JsonValue::Array(out))
    })
}

// ─── JSON helpers ────────────────────────────────────────────────────

fn row_change_to_json(rc: &RowChange) -> JsonValue {
    match rc {
        RowChange::Add(a) => json!({
            "type": "add",
            "queryID": a.query_id,
            "table": a.table,
            "rowKey": a.row_key,
            "row": a.row,
        }),
        RowChange::Remove(r) => json!({
            "type": "remove",
            "queryID": r.query_id,
            "table": r.table,
            "rowKey": r.row_key,
        }),
        RowChange::Edit(e) => json!({
            "type": "edit",
            "queryID": e.query_id,
            "table": e.table,
            "rowKey": e.row_key,
            "row": e.row,
        }),
    }
}

fn parse_change(v: &JsonValue) -> Result<Change, String> {
    let ty = v
        .get("type")
        .and_then(|x| x.as_str())
        .ok_or_else(|| "change missing `type`".to_string())?;
    match ty {
        "add" => {
            let row: zero_cache_types::value::Row = serde_json::from_value(
                v.get("row").cloned().unwrap_or(JsonValue::Null),
            )
            .map_err(|e| format!("parse add.row: {e}"))?;
            Ok(Change::Add(AddChange {
                node: Node {
                    row,
                    relationships: IndexMap::new(),
                },
            }))
        }
        "remove" => {
            let row: zero_cache_types::value::Row = serde_json::from_value(
                v.get("row").cloned().unwrap_or(JsonValue::Null),
            )
            .map_err(|e| format!("parse remove.row: {e}"))?;
            Ok(Change::Remove(RemoveChange {
                node: Node {
                    row,
                    relationships: IndexMap::new(),
                },
            }))
        }
        "edit" => {
            let new_row: zero_cache_types::value::Row = serde_json::from_value(
                v.get("row").cloned().unwrap_or(JsonValue::Null),
            )
            .map_err(|e| format!("parse edit.row: {e}"))?;
            let old_row: zero_cache_types::value::Row = serde_json::from_value(
                v.get("oldRow").cloned().unwrap_or(JsonValue::Null),
            )
            .map_err(|e| format!("parse edit.oldRow: {e}"))?;
            Ok(Change::Edit(EditChange {
                node: Node {
                    row: new_row,
                    relationships: IndexMap::new(),
                },
                old_node: Node {
                    row: old_row,
                    relationships: IndexMap::new(),
                },
            }))
        }
        other => Err(format!("unknown change type: {other}")),
    }
}
