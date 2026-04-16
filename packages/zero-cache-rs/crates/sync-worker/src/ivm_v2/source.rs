//! Channel-based SQLite access — ivm_v2 source layer.
//!
//! Two shapes live here:
//!
//! - [`ChannelSource`] — one-connection-per-instance. Each instance
//!   spawns its own SQLite worker thread with its own `rusqlite::Connection`.
//!   Used by tests and by code paths that don't need snapshot coherence
//!   across multiple sources.
//!
//! - [`SnapshotReader`] — one-connection-per-driver, shared by all
//!   [`crate::view_syncer_v2::sqlite_source::SqliteSource`]s belonging
//!   to a single `PipelineV2`. Holds an open `BEGIN` read transaction
//!   so every read in the driver sees the same snapshot. Supports a
//!   `refresh` command that ROLLBACKs and re-BEGINs, re-pinning the
//!   snapshot to the new DB head — called by the driver after the TS
//!   Snapshotter advances.
//!
//! Rows are shipped as owned `IndexMap<String, rusqlite::types::Value>`
//! so consumers don't hold any rusqlite borrows.

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use indexmap::IndexMap;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

/// Row as handed to the consumer.
pub type OwnedRow = IndexMap<String, SqlValue>;

/// One query request sent from the chain thread to the SQLite worker.
struct QueryCmd {
    sql: String,
    params: Vec<SqlValue>,
    columns: Vec<String>,
    rows_tx: Sender<OwnedRow>,
}

pub struct ChannelSource {
    cmd_tx: Option<Sender<QueryCmd>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl ChannelSource {
    pub fn new(db_path: PathBuf) -> Self {
        let (cmd_tx, cmd_rx) = unbounded::<QueryCmd>();
        let thread = thread::spawn(move || {
            let conn = match Connection::open(&db_path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[TRACE ivm_v2] ChannelSource: failed to open {db_path:?}: {e}");
                    return;
                }
            };
            for cmd in cmd_rx.iter() {
                run_query(&conn, cmd);
            }
        });
        Self {
            cmd_tx: Some(cmd_tx),
            thread: Some(thread),
        }
    }

    /// Send a query to the worker. Returns a `Receiver<OwnedRow>` that
    /// yields rows lazily. Drop the Receiver to cancel.
    pub fn query_rows(
        &self,
        sql: String,
        params: Vec<SqlValue>,
        columns: Vec<String>,
    ) -> Receiver<OwnedRow> {
        let (rows_tx, rows_rx) = bounded::<OwnedRow>(64);
        let cmd = QueryCmd {
            sql,
            params,
            columns,
            rows_tx,
        };
        if let Some(tx) = &self.cmd_tx {
            let _ = tx.send(cmd);
        }
        rows_rx
    }
}

impl Drop for ChannelSource {
    fn drop(&mut self) {
        // Close cmd channel → worker loop exits.
        drop(self.cmd_tx.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn run_query(conn: &Connection, cmd: QueryCmd) {
    let QueryCmd {
        sql,
        params,
        columns,
        rows_tx,
    } = cmd;
    let mut stmt = match conn.prepare_cached(&sql) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[TRACE ivm_v2] ChannelSource: prepare failed: {e}");
            return;
        }
    };
    let n_cols = stmt.column_count();
    let mut col_idx: Vec<(String, usize)> = Vec::with_capacity(columns.len());
    for col in &columns {
        for i in 0..n_cols {
            if stmt.column_name(i).map(|n| n == col).unwrap_or(false) {
                col_idx.push((col.clone(), i));
                break;
            }
        }
    }
    let mut rows = match stmt.query(params_from_iter(params.iter())) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[TRACE ivm_v2] ChannelSource: query failed: {e}");
            return;
        }
    };
    while let Ok(Some(row)) = rows.next() {
        let mut map: OwnedRow = IndexMap::new();
        for (name, idx) in &col_idx {
            if let Ok(v) = row.get::<usize, SqlValue>(*idx) {
                map.insert(name.clone(), v);
            }
        }
        if rows_tx.send(map).is_err() {
            // Consumer bailed — stop stepping and return to main loop.
            return;
        }
    }
}

// ─── SnapshotReader — driver-scoped shared reader ─────────────────────

enum ReaderCmd {
    Query(QueryCmd),
    /// Rollback the current read tx and begin a fresh one pinned at
    /// the current DB head. Caller blocks on `ack` until the worker
    /// has completed the refresh, returning the newly-pinned replica
    /// version on success. Subsequent queries run against the new
    /// snapshot.
    Refresh {
        ack: Sender<Result<String, String>>,
    },
    /// Return the replica version this reader is currently pinned at
    /// (read from `_zero.replicationState.stateVersion` within the
    /// pinned tx). This is the Rust-side equivalent of TS
    /// `Snapshot.version`.
    PinnedVersion {
        ack: Sender<Result<String, String>>,
    },
}

/// Driver-scoped SQLite reader. Holds ONE `rusqlite::Connection` in a
/// dedicated worker thread, with an open `BEGIN` read transaction
/// pinned to a specific snapshot. All reads issued by this reader see
/// the same committed state until `refresh()` is called.
///
/// Cloning is cheap — the connection and worker live inside an `Arc`
/// behind a sender. All clones target the same worker thread and the
/// same pinned snapshot.
pub struct SnapshotReader {
    inner: Arc<SnapshotReaderInner>,
}

struct SnapshotReaderInner {
    cmd_tx: Sender<ReaderCmd>,
    thread: std::sync::Mutex<Option<thread::JoinHandle<()>>>,
}

impl SnapshotReader {
    /// Open the replica file in a worker thread, start an initial
    /// `BEGIN` + `SELECT 1` to pin the first snapshot, then wait for
    /// commands.
    pub fn new(db_path: PathBuf) -> Self {
        let (cmd_tx, cmd_rx) = unbounded::<ReaderCmd>();
        let handle = thread::spawn(move || {
            let conn = match Connection::open(&db_path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!(
                        "[TRACE ivm_v2] SnapshotReader: failed to open {db_path:?}: {e}"
                    );
                    return;
                }
            };
            // Pin the initial snapshot eagerly. WAL mode read txns pin
            // on first SELECT, so we do BEGIN + SELECT 1 together.
            if let Err(e) = begin_read_snapshot(&conn) {
                eprintln!("[TRACE ivm_v2] SnapshotReader: initial BEGIN failed: {e}");
                return;
            }
            for cmd in cmd_rx.iter() {
                match cmd {
                    ReaderCmd::Query(q) => run_query(&conn, q),
                    ReaderCmd::Refresh { ack } => {
                        let result = refresh_snapshot(&conn)
                            .and_then(|_| read_pinned_version(&conn));
                        let _ = ack.send(result);
                    }
                    ReaderCmd::PinnedVersion { ack } => {
                        let _ = ack.send(read_pinned_version(&conn));
                    }
                }
            }
            // Channel closed → best-effort rollback and exit.
            let _ = conn.execute("ROLLBACK", []);
        });
        Self {
            inner: Arc::new(SnapshotReaderInner {
                cmd_tx,
                thread: std::sync::Mutex::new(Some(handle)),
            }),
        }
    }

    pub fn query_rows(
        &self,
        sql: String,
        params: Vec<SqlValue>,
        columns: Vec<String>,
    ) -> Receiver<OwnedRow> {
        let (rows_tx, rows_rx) = bounded::<OwnedRow>(64);
        let cmd = ReaderCmd::Query(QueryCmd {
            sql,
            params,
            columns,
            rows_tx,
        });
        let _ = self.inner.cmd_tx.send(cmd);
        rows_rx
    }

    /// Rollback the current read tx and start a new one at the current
    /// DB head. Blocks until the worker acknowledges. Returns the
    /// newly-pinned replica version. Subsequent `query_rows` calls
    /// will read from the new snapshot.
    pub fn refresh(&self) -> Result<String, String> {
        let (ack_tx, ack_rx) = bounded::<Result<String, String>>(1);
        self.inner
            .cmd_tx
            .send(ReaderCmd::Refresh { ack: ack_tx })
            .map_err(|_| "SnapshotReader worker thread is gone".to_string())?;
        ack_rx
            .recv()
            .map_err(|_| "SnapshotReader worker dropped before ack".to_string())?
    }

    /// Read `_zero.replicationState.stateVersion` within the pinned
    /// read tx. This is the version the reader sees; every row it
    /// returns is at this version.
    pub fn pinned_version(&self) -> Result<String, String> {
        let (ack_tx, ack_rx) = bounded::<Result<String, String>>(1);
        self.inner
            .cmd_tx
            .send(ReaderCmd::PinnedVersion { ack: ack_tx })
            .map_err(|_| "SnapshotReader worker thread is gone".to_string())?;
        ack_rx
            .recv()
            .map_err(|_| "SnapshotReader worker dropped before ack".to_string())?
    }
}

impl Clone for SnapshotReader {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for SnapshotReaderInner {
    fn drop(&mut self) {
        // cmd_tx is the only sender; closing it causes the worker loop
        // to exit. Join to ensure the ROLLBACK + Connection close run
        // before returning.
        drop(std::mem::replace(
            &mut self.cmd_tx,
            crossbeam_channel::unbounded().0,
        ));
        if let Ok(mut guard) = self.thread.lock() {
            if let Some(h) = guard.take() {
                let _ = h.join();
            }
        }
    }
}

fn begin_read_snapshot(conn: &Connection) -> Result<(), String> {
    // Matches the TS `Snapshotter`'s `beginConcurrent()` — uses the
    // WAL2-specific `BEGIN CONCURRENT` so this reader's pin has the
    // same semantics as the TS-side snapshot (same extension path, same
    // version-pinning behavior). Requires the replica to be in wal2
    // journal mode; we verify that explicitly so a misconfigured
    // replica fails loudly rather than silently using WAL-default
    // semantics.
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .map_err(|e| format!("PRAGMA journal_mode failed: {e}"))?;
    if mode.to_ascii_lowercase() != "wal2" {
        return Err(format!(
            "replica db must be in wal2 mode (current: {mode})"
        ));
    }
    conn.execute("BEGIN CONCURRENT", [])
        .map_err(|e| format!("BEGIN CONCURRENT failed: {e}"))?;
    // Force the snapshot to be pinned now — WAL read txns are lazy
    // until the first real SELECT. Without this, the pin happens at
    // the first query, widening the race window with TS's snapshotter.
    conn.query_row("SELECT 1", [], |_| Ok(()))
        .map_err(|e| format!("pinning SELECT failed: {e}"))?;
    Ok(())
}

fn refresh_snapshot(conn: &Connection) -> Result<(), String> {
    conn.execute("ROLLBACK", [])
        .map_err(|e| format!("ROLLBACK failed: {e}"))?;
    begin_read_snapshot(conn)
}

fn read_pinned_version(conn: &Connection) -> Result<String, String> {
    conn.query_row(
        "SELECT stateVersion FROM \"_zero.replicationState\"",
        [],
        |r| r.get::<_, String>(0),
    )
    .map_err(|e| format!("reading stateVersion failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup(path: &std::path::Path, n: usize) {
        let c = Connection::open(path).unwrap();
        c.execute_batch(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, txt TEXT);",
        )
        .unwrap();
        for i in 0..n {
            c.execute(
                "INSERT INTO t VALUES(?1, ?2)",
                rusqlite::params![i as i64, format!("row-{i}")],
            )
            .unwrap();
        }
    }

    #[test]
    fn streaming_query_all_rows() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        setup(&path, 100);

        let src = ChannelSource::new(path);
        let rx = src.query_rows(
            "SELECT id, txt FROM t ORDER BY id".into(),
            vec![],
            vec!["id".into(), "txt".into()],
        );
        let rows: Vec<OwnedRow> = rx.iter().collect();
        assert_eq!(rows.len(), 100);
        assert_eq!(
            rows[0].get("id"),
            Some(&SqlValue::Integer(0))
        );
    }

    #[test]
    fn early_break_stops_worker() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        setup(&path, 10_000);

        let src = ChannelSource::new(path);
        let rx = src.query_rows(
            "SELECT id, txt FROM t ORDER BY id".into(),
            vec![],
            vec!["id".into(), "txt".into()],
        );
        let mut iter = rx.into_iter();
        // Take 5 and drop.
        for _ in 0..5 {
            iter.next().expect("expected a row");
        }
        drop(iter);
        // Worker should be idle now; subsequent query should work.
        let rx2 = src.query_rows(
            "SELECT id, txt FROM t ORDER BY id".into(),
            vec![],
            vec!["id".into(), "txt".into()],
        );
        let n2 = rx2.iter().count();
        assert_eq!(n2, 10_000);
    }
}
