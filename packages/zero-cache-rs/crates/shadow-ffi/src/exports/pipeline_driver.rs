//! Shadow of `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts`.
//!
//! First exercise of the stateful [`crate::handle`] pattern: one
//! [`PipelineDriver`] per TS-side `ViewSyncer`, constructed and driven
//! from TS while the Rust implementation runs in parallel. Diff-testing
//! is handled by the TS harness (`packages/zero-cache/src/shadow/
//! pipeline-driver.test.ts`).
//!
//! # FFI-shape decisions
//!
//! 1. **Stateful handle.** The driver lives behind
//!    `External<Arc<Mutex<PipelineDriver>>>`; TS never sees the inner type.
//! 2. **JSON-valued inputs / outputs.** Per the framework rule in the
//!    `exports/mod.rs` docstring, anything typed (AST, ClientSchema, Row,
//!    RowChange) crosses the FFI as `serde_json::Value`. The `AST` type
//!    already derives serde; `ClientSchema` / `LiteAndZqlSpec` /
//!    `LiteTableSpec` do not, so we deserialise them by hand here.
//! 3. **Eager collection of streamed outputs.** `add_query` and `advance`
//!    both return their `Vec<RowChange>` collected in one call. Streaming
//!    across FFI is awkward (TS would have to drain an iterator handle),
//!    and the TS wrapper diffs the collected arrays just fine.
//! 4. **Panic isolation.** Every entrypoint wraps its body in
//!    [`catch_unwind`] so a Rust `assert!` (e.g. `PipelineDriver::init`
//!    checking an unrecognised `ClientSchema` shape) surfaces as a
//!    `napi::Error` rather than aborting the Node process. This was a
//!    real bug previously; see module docstring on
//!    `crates/shadow-ffi/src/exports/ivm_data.rs`.
//! 5. **Yield threshold / inspector.** The constructor accepts the same
//!    numeric yield-threshold used by the integration tests. The
//!    inspector defaults to [`NoopInspectorDelegate`] — TS diff-tests do
//!    not exercise the inspector surface.
//! 6. **Timer.** Accepted as an opaque JSON `{elapsedLapMs, totalElapsedMs}`
//!    pair, read once per call. Rust wraps it in a fixed-value [`Timer`].
//!    Callers pass `{elapsedLapMs: 0, totalElapsedMs: 0}` (a.k.a.
//!    `NoopTimer`) for hydration-only diff tests.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use zero_cache_sync_worker::view_syncer::client_schema::{
    ClientColumnSchema, ClientSchema, ClientTableSchema, LiteAndZqlSpec, LiteColumnSpec,
    LiteTableSpec, LiteTableSpecWithKeys, ShardId, ZqlSchemaValue,
};
use zero_cache_sync_worker::view_syncer::pipeline_driver::{
    NoopInspectorDelegate, PipelineDriver, RowChange, RowChangeOrYield, Timer,
};
use zero_cache_sync_worker::view_syncer::snapshotter::Snapshotter;
use zero_cache_sync_worker::zqlite::database_storage::{DatabaseStorage, DatabaseStorageOptions};
use zero_cache_sync_worker::zqlite::table_source::TableSource;
use zero_cache_types::ast::AST;

use crate::exports::table_source::TableSourceHandle;
use crate::handle::{Handle, make_handle, with_handle};

// ─── Handle type ─────────────────────────────────────────────────────

pub type PipelineDriverHandle = Handle<PipelineDriver>;

// ─── Fixed-value Timer ───────────────────────────────────────────────

/// Timer implementation whose readings come from a frozen pair of numbers.
/// Used by [`parse_timer`] — TS hands in `{elapsedLapMs, totalElapsedMs}`;
/// we expose them as the two `Timer` methods. This mirrors the
/// integration-test `NoopTimer` / `FastTimer` pattern without needing a
/// callback across the FFI boundary.
struct FixedTimer {
    lap: f64,
    total: f64,
}

impl Timer for FixedTimer {
    fn elapsed_lap(&self) -> f64 {
        self.lap
    }
    fn total_elapsed(&self) -> f64 {
        self.total
    }
}

fn parse_timer(v: Option<JsonValue>) -> FixedTimer {
    let v = v.unwrap_or(JsonValue::Null);
    let lap = v
        .get("elapsedLapMs")
        .and_then(JsonValue::as_f64)
        .unwrap_or(0.0);
    let total = v
        .get("totalElapsedMs")
        .and_then(JsonValue::as_f64)
        .unwrap_or(0.0);
    FixedTimer { lap, total }
}

// ─── Panic guard ─────────────────────────────────────────────────────

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

// ─── Intermediate serde structs ──────────────────────────────────────

#[derive(Deserialize)]
struct ClientColumnSchemaJson {
    r#type: String,
}

#[derive(Deserialize)]
struct ClientTableSchemaJson {
    columns: IndexMap<String, ClientColumnSchemaJson>,
    #[serde(default, rename = "primaryKey")]
    primary_key: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct ClientSchemaJson {
    tables: IndexMap<String, ClientTableSchemaJson>,
}

#[derive(Deserialize)]
struct LiteColumnSpecJson {
    #[serde(rename = "dataType")]
    data_type: String,
}

#[derive(Deserialize)]
struct LiteTableSpecJson {
    columns: IndexMap<String, LiteColumnSpecJson>,
}

#[derive(Deserialize)]
struct ZqlSchemaValueJson {
    r#type: String,
}

#[derive(Deserialize)]
struct LiteAndZqlSpecJson {
    #[serde(rename = "allPotentialPrimaryKeys")]
    all_potential_primary_keys: Vec<Vec<String>>,
    #[serde(rename = "zqlSpec")]
    zql_spec: IndexMap<String, ZqlSchemaValueJson>,
}

fn deserialize_client_schema(v: JsonValue) -> Result<ClientSchema, String> {
    let j: ClientSchemaJson =
        serde_json::from_value(v).map_err(|e| format!("client schema: {e}"))?;
    let mut tables = IndexMap::new();
    for (name, tbl) in j.tables {
        let mut columns = IndexMap::new();
        for (c, cs) in tbl.columns {
            columns.insert(c, ClientColumnSchema { r#type: cs.r#type });
        }
        tables.insert(
            name,
            ClientTableSchema {
                columns,
                primary_key: tbl.primary_key,
            },
        );
    }
    Ok(ClientSchema { tables })
}

fn deserialize_table_specs(v: JsonValue) -> Result<IndexMap<String, LiteAndZqlSpec>, String> {
    let j: IndexMap<String, LiteAndZqlSpecJson> =
        serde_json::from_value(v).map_err(|e| format!("table specs: {e}"))?;
    let mut out = IndexMap::new();
    for (name, spec) in j {
        let mut zql = IndexMap::new();
        for (c, z) in spec.zql_spec {
            zql.insert(c, ZqlSchemaValue { r#type: z.r#type });
        }
        out.insert(
            name,
            LiteAndZqlSpec {
                table_spec: LiteTableSpecWithKeys {
                    all_potential_primary_keys: spec.all_potential_primary_keys,
                },
                zql_spec: zql,
            },
        );
    }
    Ok(out)
}

fn deserialize_full_tables(v: JsonValue) -> Result<IndexMap<String, LiteTableSpec>, String> {
    let j: IndexMap<String, LiteTableSpecJson> =
        serde_json::from_value(v).map_err(|e| format!("full tables: {e}"))?;
    let mut out = IndexMap::new();
    for (name, tbl) in j {
        let mut columns = IndexMap::new();
        for (c, col) in tbl.columns {
            columns.insert(
                c,
                LiteColumnSpec {
                    data_type: col.data_type,
                },
            );
        }
        out.insert(name, LiteTableSpec { columns });
    }
    Ok(out)
}

// ─── RowChange → JSON ────────────────────────────────────────────────

fn row_to_json(r: &zero_cache_types::value::Row) -> JsonValue {
    let mut out = serde_json::Map::new();
    for (k, v) in r.iter() {
        let j = match v {
            Some(x) => x.clone(),
            None => JsonValue::Null,
        };
        out.insert(k.clone(), j);
    }
    JsonValue::Object(out)
}

fn row_change_to_json(c: &RowChangeOrYield) -> JsonValue {
    match c {
        RowChangeOrYield::Yield => json!({"type": "yield"}),
        RowChangeOrYield::Change(RowChange::Add(a)) => json!({
            "type": "add",
            "queryID": a.query_id,
            "table": a.table,
            "rowKey": row_to_json(&a.row_key),
            "row": row_to_json(&a.row),
        }),
        RowChangeOrYield::Change(RowChange::Remove(r)) => json!({
            "type": "remove",
            "queryID": r.query_id,
            "table": r.table,
            "rowKey": row_to_json(&r.row_key),
            "row": JsonValue::Null,
        }),
        RowChangeOrYield::Change(RowChange::Edit(e)) => json!({
            "type": "edit",
            "queryID": e.query_id,
            "table": e.table,
            "rowKey": row_to_json(&e.row_key),
            "row": row_to_json(&e.row),
        }),
    }
}

// ─── Constructor ─────────────────────────────────────────────────────

/// Shadow of the TS constructor.
///
/// `replica_path` is the path to the SQLite file holding the replicated
/// data (identical to the one TS opens). `default_yield_every_ms`
/// corresponds to TS `yieldThresholdMs: () => number` — constant-valued
/// here because the JS callback crosses the FFI boundary awkwardly; a
/// future recorder layer can add a call-back surface.
#[napi(js_name = "pipeline_driver_create")]
pub fn pipeline_driver_create(
    client_group_id: String,
    replica_path: String,
    app_id: String,
    shard_num: u32,
    default_yield_every_ms: f64,
    enable_planner: bool,
) -> napi::Result<PipelineDriverHandle> {
    run("pipeline_driver_create", || {
        let snapshotter = Snapshotter::new(&replica_path, &app_id);
        let storage = DatabaseStorage::open_in_memory(DatabaseStorageOptions::default())
            .map_err(|e| format!("open in-memory storage: {e}"))?
            .create_client_group_storage(&client_group_id);
        let shard = ShardId { app_id, shard_num };
        let y = default_yield_every_ms;
        let driver = PipelineDriver::new(
            snapshotter,
            shard,
            storage,
            client_group_id,
            Arc::new(NoopInspectorDelegate),
            Arc::new(move || y),
            enable_planner,
        );
        Ok(make_handle(driver))
    })
}

// ─── Lifecycle ───────────────────────────────────────────────────────

#[napi(js_name = "pipeline_driver_init")]
pub fn pipeline_driver_init(
    driver: &PipelineDriverHandle,
    client_schema: JsonValue,
    table_specs: JsonValue,
    full_tables: JsonValue,
) -> napi::Result<()> {
    run("pipeline_driver_init", || {
        let cs = deserialize_client_schema(client_schema)?;
        let specs = deserialize_table_specs(table_specs)?;
        let full = deserialize_full_tables(full_tables)?;
        with_handle(driver, |d| {
            d.init(&cs, specs, full).map_err(|e| e.to_string())?;
            // Auto-create a TableSource per initialised table so callers
            // don't have to pre-register each source before `add_query`.
            // Mirrors the TS driver's lazy-on-demand creation.
            d.auto_register_table_sources()
                .map_err(|e| e.to_string())
        })
    })
}

#[napi(js_name = "pipeline_driver_initialized")]
pub fn pipeline_driver_initialized(driver: &PipelineDriverHandle) -> bool {
    with_handle(driver, |d| d.initialized())
}

#[napi(js_name = "pipeline_driver_replica_version")]
pub fn pipeline_driver_replica_version(driver: &PipelineDriverHandle) -> napi::Result<String> {
    run("pipeline_driver_replica_version", || {
        with_handle(driver, |d| d.replica_version().map_err(|e| e.to_string()))
    })
}

#[napi(js_name = "pipeline_driver_current_version")]
pub fn pipeline_driver_current_version(driver: &PipelineDriverHandle) -> napi::Result<String> {
    run("pipeline_driver_current_version", || {
        with_handle(driver, |d| d.current_version().map_err(|e| e.to_string()))
    })
}

#[napi(js_name = "pipeline_driver_reset")]
pub fn pipeline_driver_reset(
    driver: &PipelineDriverHandle,
    client_schema: JsonValue,
    table_specs: JsonValue,
    full_tables: JsonValue,
) -> napi::Result<()> {
    run("pipeline_driver_reset", || {
        let cs = deserialize_client_schema(client_schema)?;
        let specs = deserialize_table_specs(table_specs)?;
        let full = deserialize_full_tables(full_tables)?;
        with_handle(driver, |d| {
            d.reset(&cs, specs, full).map_err(|e| e.to_string())
        })
    })
}

#[napi(js_name = "pipeline_driver_destroy")]
pub fn pipeline_driver_destroy(driver: &PipelineDriverHandle) -> napi::Result<()> {
    run("pipeline_driver_destroy", || {
        with_handle(driver, |d| {
            d.destroy();
            Ok::<(), String>(())
        })
    })
}

// ─── Queries ─────────────────────────────────────────────────────────

/// Shadow of TS `addQuery(transformationHash, queryID, ast, timer)`. The
/// iterator TS returns is eagerly drained on the Rust side so the TS
/// wrapper diffs a plain `Array<RowChange>`.
#[napi(js_name = "pipeline_driver_add_query")]
pub fn pipeline_driver_add_query(
    driver: &PipelineDriverHandle,
    transformation_hash: String,
    query_id: String,
    ast: JsonValue,
    timer: Option<JsonValue>,
) -> napi::Result<JsonValue> {
    run("pipeline_driver_add_query", || {
        let ast: AST = serde_json::from_value(ast).map_err(|e| format!("bad AST: {e}"))?;
        let t = parse_timer(timer);
        with_handle(driver, |d| {
            let changes = d
                .add_query(&transformation_hash, &query_id, ast, &t)
                .map_err(|e| e.to_string())?;
            let json: Vec<JsonValue> = changes.iter().map(row_change_to_json).collect();
            Ok::<JsonValue, String>(JsonValue::Array(json))
        })
    })
}

#[napi(js_name = "pipeline_driver_remove_query")]
pub fn pipeline_driver_remove_query(
    driver: &PipelineDriverHandle,
    query_id: String,
) -> napi::Result<()> {
    run("pipeline_driver_remove_query", || {
        with_handle(driver, |d| {
            d.remove_query(&query_id);
            Ok::<(), String>(())
        })
    })
}

/// Shadow of TS `advance(timer)`. Returns `{version, numChanges, changes}`.
#[napi(js_name = "pipeline_driver_advance")]
pub fn pipeline_driver_advance(
    driver: &PipelineDriverHandle,
    timer: Option<JsonValue>,
) -> napi::Result<JsonValue> {
    run("pipeline_driver_advance", || {
        let t = parse_timer(timer);
        with_handle(driver, |d| {
            let res = d.advance(&t).map_err(|e| e.to_string())?;
            let changes: Vec<JsonValue> = res.changes.iter().map(row_change_to_json).collect();
            Ok::<JsonValue, String>(json!({
                "version": res.version,
                "numChanges": res.num_changes,
                "changes": changes,
            }))
        })
    })
}

#[napi(js_name = "pipeline_driver_advance_without_diff")]
pub fn pipeline_driver_advance_without_diff(driver: &PipelineDriverHandle) -> napi::Result<String> {
    run("pipeline_driver_advance_without_diff", || {
        with_handle(driver, |d| {
            d.advance_without_diff().map_err(|e| e.to_string())
        })
    })
}

/// Shadow of TS `queries()`. Returns an array (not a map) of
/// `{queryId, table, transformationHash}` for diagnostic diffing — a TS
/// `ReadonlyMap` doesn't serialise through JSON cleanly.
#[napi(js_name = "pipeline_driver_queries")]
pub fn pipeline_driver_queries(driver: &PipelineDriverHandle) -> napi::Result<JsonValue> {
    run("pipeline_driver_queries", || {
        with_handle(driver, |d| {
            let qs = d.queries();
            let mut out: Vec<JsonValue> = Vec::with_capacity(qs.len());
            for (id, info) in qs.iter() {
                out.push(json!({
                    "queryId": id,
                    "table": info.transformed_ast.table,
                    "transformationHash": info.transformation_hash,
                }));
            }
            Ok::<JsonValue, String>(JsonValue::Array(out))
        })
    })
}

#[napi(js_name = "pipeline_driver_total_hydration_time_ms")]
pub fn pipeline_driver_total_hydration_time_ms(driver: &PipelineDriverHandle) -> f64 {
    with_handle(driver, |d| d.total_hydration_time_ms())
}

/// Shadow of TS `hydrationBudgetBreakdown()`. Returns an array of
/// `{id, table, ms}` sorted DESC by `ms` for slow-hydrate diagnostics.
#[napi(js_name = "pipeline_driver_hydration_budget_breakdown")]
pub fn pipeline_driver_hydration_budget_breakdown(
    driver: &PipelineDriverHandle,
) -> napi::Result<JsonValue> {
    run("pipeline_driver_hydration_budget_breakdown", || {
        with_handle(driver, |d| {
            let bb = d.hydration_budget_breakdown();
            let out: Vec<JsonValue> = bb
                .into_iter()
                .map(|(id, table, ms)| json!({"id": id, "table": table, "ms": ms}))
                .collect();
            Ok::<JsonValue, String>(JsonValue::Array(out))
        })
    })
}

/// Shadow of TS `getRow(table, pk)`. Returns the row as a JSON object,
/// or `null` if no such row exists. Asserts the driver is initialized.
#[napi(js_name = "pipeline_driver_get_row")]
pub fn pipeline_driver_get_row(
    driver: &PipelineDriverHandle,
    table: String,
    pk: JsonValue,
) -> napi::Result<JsonValue> {
    run("pipeline_driver_get_row", || {
        let pk_row: zero_cache_types::value::Row =
            serde_json::from_value(pk).map_err(|e| format!("bad pk shape: {e}"))?;
        with_handle(driver, |d| {
            let row = d.get_row(&table, &pk_row);
            match row {
                Some(r) => serde_json::to_value(&r)
                    .map_err(|e| format!("serialise row: {e}")),
                None => Ok::<JsonValue, String>(JsonValue::Null),
            }
        })
    })
}

// ─── TableSource registration ────────────────────────────────────────

/// Shadow of the register-TableSource seam the Rust driver uses. TS
/// builds a [`TableSource`] via [`crate::exports::table_source::table_source_create`]
/// then hands the opaque handle back to register on the driver.
#[napi(js_name = "pipeline_driver_register_table_source")]
pub fn pipeline_driver_register_table_source(
    driver: &PipelineDriverHandle,
    table_name: String,
    source: &TableSourceHandle,
) -> napi::Result<()> {
    run("pipeline_driver_register_table_source", || {
        // Clone the `Arc<TableSource>` from the source handle, then install
        // it on the driver. Two handles now co-own the `TableSource` — the
        // caller's and the driver's — which is exactly the TS contract
        // (the TS `this.#tables.set(name, source)` retains a reference
        // without transferring ownership).
        let ts_arc: Arc<TableSource> = {
            let arc = source.as_ref().clone();
            let guard = arc.lock().expect("table source mutex poisoned");
            (*guard).clone()
        };
        with_handle(driver, |d| {
            d.register_table_source(&table_name, ts_arc);
            Ok::<(), String>(())
        })
    })
}

// Touch the `Mutex` import to silence an unused-import warning if the
// code above is ever refactored away.
#[allow(dead_code)]
fn _mutex_touch() -> Option<Mutex<()>> {
    None
}

