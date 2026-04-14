//! Shadow of `packages/zqlite/src/table-source.ts` — handle factory used
//! together with [`crate::exports::pipeline_driver`] so TS can construct
//! a Rust `TableSource` against a SQLite replica file and register it on
//! the Rust `PipelineDriver`.
//!
//! # FFI-shape decisions
//!
//! - The `TableSource` lives behind an opaque [`Handle`]; TS sees only a
//!   [`External`] pointer.
//! - The constructor opens a **fresh** `rusqlite::Connection` on the path
//!   TS hands in. Never share `better-sqlite3`'s connection across
//!   languages — both sides open the same file on their own and rely on
//!   WAL for concurrency.
//! - The column schema crosses the boundary as
//!   `{ [col]: { type: 'number' | 'string' | 'boolean' | 'null' | 'json',
//!   optional?: boolean } }` — matching the TS `SchemaValue` shape.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use indexmap::IndexMap;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::Value as JsonValue;

use zero_cache_sync_worker::zqlite::table_source::{SchemaValue, TableSource, ValueType};
use zero_cache_types::primary_key::PrimaryKey;

use crate::handle::{Handle, make_handle};

/// Public alias: opaque handle holding a refcounted [`TableSource`].
///
/// `Arc<TableSource>` is what [`register_table_source`] requires, and
/// what the driver itself stores internally.
pub type TableSourceHandle = Handle<Arc<TableSource>>;

#[derive(Debug, Deserialize)]
struct SchemaValueJson {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    optional: bool,
}

fn parse_columns(columns: JsonValue) -> Result<IndexMap<String, SchemaValue>, String> {
    let obj = match columns {
        JsonValue::Object(m) => m,
        other => return Err(format!("columns must be an object, got {other:?}")),
    };
    let mut out: IndexMap<String, SchemaValue> = IndexMap::new();
    for (k, v) in obj {
        let sv: SchemaValueJson = serde_json::from_value(v)
            .map_err(|e| format!("bad schema value for column {k}: {e}"))?;
        let value_type = match sv.kind.as_str() {
            "boolean" => ValueType::Boolean,
            "number" => ValueType::Number,
            "string" => ValueType::String,
            "null" => ValueType::Null,
            "json" => ValueType::Json,
            other => return Err(format!("unknown column type {other:?} for {k}")),
        };
        out.insert(
            k,
            SchemaValue {
                value_type,
                optional: sv.optional,
            },
        );
    }
    Ok(out)
}

fn run<R>(label: &'static str, f: impl FnOnce() -> Result<R, String>) -> napi::Result<R> {
    // `catch_unwind` is the framework-wide rule (see module docstring on
    // `ivm_data.rs`) — a Rust `assert!` inside TableSource must surface
    // as a recoverable `napi::Error` rather than aborting Node.
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

/// Shadow factory: open a `rusqlite::Connection` on `replica_path`, wrap it
/// in a [`TableSource`], and hand TS back an opaque handle. TS keeps a
/// separate `better-sqlite3` connection on the same file; WAL handles
/// concurrency.
#[napi(js_name = "table_source_create")]
pub fn table_source_create(
    replica_path: String,
    table_name: String,
    primary_key: Vec<String>,
    columns: JsonValue,
) -> napi::Result<TableSourceHandle> {
    run("table_source_create", || {
        let cols = parse_columns(columns)?;
        let conn =
            Connection::open(&replica_path).map_err(|e| format!("open {replica_path}: {e}"))?;
        let ts = TableSource::new(conn, table_name, cols, PrimaryKey::new(primary_key))
            .map_err(|e| format!("{e}"))?;
        Ok(make_handle(Arc::new(ts)))
    })
}

/// Shadow factory variant backed by an in-memory SQLite (no replica file).
///
/// Useful for TS diff tests that want to bypass the replica-file swap path
/// — both sides construct their source from the same identical seed data
/// handed in at construction time.
#[napi(js_name = "table_source_create_in_memory")]
pub fn table_source_create_in_memory(
    table_name: String,
    primary_key: Vec<String>,
    columns: JsonValue,
    create_table_sql: String,
    seed_sql: Vec<String>,
) -> napi::Result<TableSourceHandle> {
    run("table_source_create_in_memory", || {
        let cols = parse_columns(columns)?;
        let conn = Connection::open_in_memory().map_err(|e| format!("open memory: {e}"))?;
        conn.execute_batch(&create_table_sql)
            .map_err(|e| format!("create table: {e}"))?;
        for stmt in &seed_sql {
            conn.execute_batch(stmt)
                .map_err(|e| format!("seed ({stmt:?}): {e}"))?;
        }
        let ts = TableSource::new(conn, table_name, cols, PrimaryKey::new(primary_key))
            .map_err(|e| format!("{e}"))?;
        Ok(make_handle(Arc::new(ts)))
    })
}
