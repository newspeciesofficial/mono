//! napi exports for `view_syncer_v2::snapshotter::Snapshotter`.
//!
//! Lets TS delegate snapshot pinning to Rust — one `BEGIN CONCURRENT`
//! connection pair (leapfrog prev/curr) serves both the TS-side
//! snapshotter's internal reads (changelog iteration, per-row lookups
//! during diff) and Rust-side IVM reads (hydration, getRow, refetch).
//! This is the single connection both sides need to see the same
//! snapshot — no cross-connection race.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Mutex;

use napi::bindgen_prelude::External;
use rusqlite::types::Value as SqlValue;
use serde_json::{Value as JsonValue, json};

use zero_cache_sync_worker::ivm_v2::source::OwnedRow;
use zero_cache_sync_worker::view_syncer_v2::snapshotter::Snapshotter;

/// Opaque handle to a Rust-side `Snapshotter` plus per-handle state
/// needed for streaming changelog iteration.
pub struct SnapshotterHandle {
    inner: Mutex<Snapshotter>,
    /// Receiver for an in-flight `changelog_since` iteration. Populated
    /// by `_changelog_start`, drained by `_changelog_next_chunk`,
    /// cleared when the iteration completes or `_changelog_cleanup`.
    changelog_rx: Mutex<
        Option<crossbeam_channel::Receiver<OwnedRow>>,
    >,
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

#[napi(js_name = "snapshotter_create")]
pub fn snapshotter_create(db_path: String) -> napi::Result<External<SnapshotterHandle>> {
    run("snapshotter_create", || {
        let s = Snapshotter::new(PathBuf::from(db_path));
        Ok(External::new(SnapshotterHandle {
            inner: Mutex::new(s),
            changelog_rx: Mutex::new(None),
        }))
    })
}

#[napi(js_name = "snapshotter_init")]
pub fn snapshotter_init(
    handle: &External<SnapshotterHandle>,
) -> napi::Result<String> {
    run("snapshotter_init", || {
        let mut g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        g.init()
    })
}

#[napi(js_name = "snapshotter_initialized")]
pub fn snapshotter_initialized(
    handle: &External<SnapshotterHandle>,
) -> napi::Result<bool> {
    run("snapshotter_initialized", || {
        let g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        Ok(g.initialized())
    })
}

#[napi(js_name = "snapshotter_current_version")]
pub fn snapshotter_current_version(
    handle: &External<SnapshotterHandle>,
) -> napi::Result<Option<String>> {
    run("snapshotter_current_version", || {
        let g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        Ok(g.current_version().map(|s| s.to_string()))
    })
}

#[napi(js_name = "snapshotter_prev_version")]
pub fn snapshotter_prev_version(
    handle: &External<SnapshotterHandle>,
) -> napi::Result<Option<String>> {
    run("snapshotter_prev_version", || {
        let g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        Ok(g.prev_version().map(|s| s.to_string()))
    })
}

/// Leapfrog advance. Returns `{prevVersion, currVersion}` — the caller
/// typically uses these to drive a changelog iteration.
#[napi(js_name = "snapshotter_advance")]
pub fn snapshotter_advance(
    handle: &External<SnapshotterHandle>,
) -> napi::Result<JsonValue> {
    run("snapshotter_advance", || {
        let mut g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        let r = g.advance()?;
        Ok(json!({
            "prevVersion": r.prev_version,
            "currVersion": r.curr_version,
        }))
    })
}

#[napi(js_name = "snapshotter_num_changes_since")]
pub fn snapshotter_num_changes_since(
    handle: &External<SnapshotterHandle>,
    prev_version: String,
) -> napi::Result<i64> {
    run("snapshotter_num_changes_since", || {
        let g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        g.num_changes_since(&prev_version)
    })
}

/// Start streaming the changelog between `prev_version` (exclusive)
/// and the current snapshot. Stores the row receiver on the handle;
/// TS drains it via `snapshotter_changelog_next_chunk`.
///
/// Only one iteration may be active per handle at a time. Calling
/// `_start` while an iteration is in progress drops the previous
/// receiver (cancelling the worker's streaming).
#[napi(js_name = "snapshotter_changelog_start")]
pub fn snapshotter_changelog_start(
    handle: &External<SnapshotterHandle>,
    prev_version: String,
) -> napi::Result<()> {
    run("snapshotter_changelog_start", || {
        let g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        let rx = g.changelog_since(&prev_version)?;
        drop(g);
        let mut slot = handle
            .changelog_rx
            .lock()
            .map_err(|_| "changelog slot poisoned".to_string())?;
        *slot = Some(rx);
        Ok(())
    })
}

/// Drain up to 100 rows from the in-flight changelog iteration.
/// Returns `{ rows: [...], isFinal: bool }`. When the receiver
/// closes (no more rows), `isFinal` is true and the handle's slot
/// is cleared.
#[napi(js_name = "snapshotter_changelog_next_chunk")]
pub fn snapshotter_changelog_next_chunk(
    handle: &External<SnapshotterHandle>,
) -> napi::Result<JsonValue> {
    run("snapshotter_changelog_next_chunk", || {
        const CHUNK_SIZE: usize = 100;
        let mut slot = handle
            .changelog_rx
            .lock()
            .map_err(|_| "changelog slot poisoned".to_string())?;
        let rx = slot
            .as_ref()
            .ok_or_else(|| "no changelog iteration in progress".to_string())?
            .clone();
        let mut rows: Vec<OwnedRow> = Vec::with_capacity(CHUNK_SIZE);
        // Block for at least one row, then drain non-blocking up to
        // CHUNK_SIZE. This matches the consumer-paced pattern: each
        // call returns as soon as data is available, but batches
        // multiple rows per napi crossing.
        match rx.recv() {
            Ok(r) => rows.push(r),
            Err(_) => {
                // Receiver closed cleanly — iteration done.
                *slot = None;
                return Ok(json!({ "rows": [], "isFinal": true }));
            }
        }
        while rows.len() < CHUNK_SIZE {
            match rx.try_recv() {
                Ok(r) => rows.push(r),
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    // Drained and closed.
                    *slot = None;
                    let json_rows: Vec<JsonValue> =
                        rows.iter().map(owned_row_to_json).collect();
                    return Ok(json!({ "rows": json_rows, "isFinal": true }));
                }
            }
        }
        let json_rows: Vec<JsonValue> = rows.iter().map(owned_row_to_json).collect();
        Ok(json!({ "rows": json_rows, "isFinal": false }))
    })
}

/// Drop any in-flight changelog iteration. Safe to call even when
/// none is active (idempotent). Used by TS's diff iterator `return()`
/// hook when the caller abandons mid-iteration.
#[napi(js_name = "snapshotter_changelog_cleanup")]
pub fn snapshotter_changelog_cleanup(
    handle: &External<SnapshotterHandle>,
) -> napi::Result<()> {
    run("snapshotter_changelog_cleanup", || {
        let mut slot = handle
            .changelog_rx
            .lock()
            .map_err(|_| "changelog slot poisoned".to_string())?;
        *slot = None;
        Ok(())
    })
}

/// Run a read-only SQL query on the current (curr) pinned snapshot.
/// The caller (TS diff iterator) builds the SQL — typically a
/// `SELECT cols FROM t WHERE pk=?` for `getRow` or a multi-key OR
/// for `getRows`. Returns ALL matching rows as a JSON array (no
/// streaming needed — these queries are bounded in size).
#[napi(js_name = "snapshotter_read_in_curr")]
pub fn snapshotter_read_in_curr(
    handle: &External<SnapshotterHandle>,
    sql: String,
    params: Vec<JsonValue>,
    columns: Vec<String>,
) -> napi::Result<JsonValue> {
    run("snapshotter_read_in_curr", || {
        let g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        let reader = g
            .curr()
            .ok_or_else(|| "snapshotter not initialized".to_string())?;
        drop(g);
        let rx = reader.query_rows(sql, json_params_to_sql(&params), columns);
        let rows: Vec<OwnedRow> = rx.iter().collect();
        Ok(JsonValue::Array(
            rows.iter().map(owned_row_to_json).collect(),
        ))
    })
}

#[napi(js_name = "snapshotter_read_in_prev")]
pub fn snapshotter_read_in_prev(
    handle: &External<SnapshotterHandle>,
    sql: String,
    params: Vec<JsonValue>,
    columns: Vec<String>,
) -> napi::Result<JsonValue> {
    run("snapshotter_read_in_prev", || {
        let g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        let reader = g
            .prev()
            .ok_or_else(|| "snapshotter has no prev (not yet advanced)".to_string())?;
        drop(g);
        let rx = reader.query_rows(sql, json_params_to_sql(&params), columns);
        let rows: Vec<OwnedRow> = rx.iter().collect();
        Ok(JsonValue::Array(
            rows.iter().map(owned_row_to_json).collect(),
        ))
    })
}

#[napi(js_name = "snapshotter_destroy")]
pub fn snapshotter_destroy(
    handle: &External<SnapshotterHandle>,
) -> napi::Result<()> {
    run("snapshotter_destroy", || {
        if let Ok(mut slot) = handle.changelog_rx.lock() {
            *slot = None;
        }
        let mut g = handle
            .inner
            .lock()
            .map_err(|_| "snapshotter mutex poisoned".to_string())?;
        g.destroy();
        Ok(())
    })
}

// ─── Helpers ─────────────────────────────────────────────────────────

fn owned_row_to_json(row: &OwnedRow) -> JsonValue {
    let mut obj: HashMap<String, JsonValue> = HashMap::new();
    for (k, v) in row.iter() {
        obj.insert(k.clone(), sql_value_to_json(v));
    }
    json!(obj)
}

fn sql_value_to_json(v: &SqlValue) -> JsonValue {
    match v {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Integer(i) => json!(*i),
        SqlValue::Real(f) => json!(*f),
        SqlValue::Text(s) => json!(s),
        SqlValue::Blob(b) => json!(b),
    }
}

fn json_params_to_sql(params: &[JsonValue]) -> Vec<SqlValue> {
    params
        .iter()
        .map(|p| match p {
            JsonValue::Null => SqlValue::Null,
            JsonValue::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    SqlValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    SqlValue::Real(f)
                } else {
                    SqlValue::Null
                }
            }
            JsonValue::String(s) => SqlValue::Text(s.clone()),
            _ => SqlValue::Null,
        })
        .collect()
}
