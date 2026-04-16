//! Rust-side port of TS `services/view-syncer/snapshotter.ts`.
//!
//! Owns the driver's SQLite read-snapshot pinning. Under WAL2 we hold
//! two long-lived `BEGIN CONCURRENT` connections — `prev` and `curr` —
//! that leapfrog each other as the ViewSyncer advances. This is the
//! mechanism that gives each ViewSyncer a stable, self-consistent view
//! of the database while the Replicator keeps writing new commits on a
//! third connection.
//!
//! Moving the Snapshotter into Rust eliminates the cross-connection
//! race that existed when the TS Snapshotter pinned on its own
//! better-sqlite3 connection and Rust pinned separately on its own
//! rusqlite connection — between the two pins, the replicator could
//! commit, producing rows visible to one side but not the other.
//!
//! Now: TS delegates to this Rust Snapshotter through napi. Both
//! snapshotter internal reads (changelog iteration, per-row lookups
//! during diff) AND IVM hot-path reads (hydration, getRow, refetch)
//! go through the same pinned connections here. Single source of
//! truth.
//!
//! Reads are issued via [`SnapshotReader::query_rows`], the channel-
//! based streaming path we use everywhere else. No collected Vecs on
//! the hot path — chunks flow through a bounded receiver.
//!
//! Scope of this phase:
//! - Leapfrog `prev`/`curr` management (init, advance).
//! - Exposing `SnapshotReader` clones so callers can issue SQL.
//! - `changelog_since(prev_version)` streaming iterator.
//!
//! Out of scope (already covered by [`SnapshotReader::query_rows`]):
//! - Individual `get_row` / `get_rows` / etc. — callers build the SQL.

use crossbeam_channel::Receiver;
use rusqlite::types::Value as SqlValue;
use std::path::PathBuf;

use crate::ivm_v2::source::{OwnedRow, SnapshotReader};

/// Result of a successful `advance`: the versions at which prev and
/// curr are now pinned. Matches the shape TS `Snapshotter.advance`
/// returns via its `SnapshotDiff.{prev,curr}.version`.
#[derive(Clone, Debug)]
pub struct AdvanceResult {
    pub prev_version: String,
    pub curr_version: String,
}

/// Driver-scoped Snapshotter.
///
/// Holds two `SnapshotReader`s in a leapfrog pattern. Each reader has
/// its own worker thread + rusqlite connection with an open
/// `BEGIN CONCURRENT` read tx pinned at a specific replica version.
pub struct Snapshotter {
    db_path: PathBuf,
    /// The current pin — all hot-path reads (hydration, getRow, refetch
    /// during advance) go through this reader's connection.
    curr: Option<SnapshotReader>,
    curr_version: Option<String>,
    /// The previous pin — populated after the first `advance`. Used by
    /// diff iteration's `get_rows(prev_table, ...)` calls which
    /// must see the state *before* the diff's changes were committed.
    prev: Option<SnapshotReader>,
    prev_version: Option<String>,
}

impl Snapshotter {
    pub fn new(db_path: PathBuf) -> Self {
        Self {
            db_path,
            curr: None,
            curr_version: None,
            prev: None,
            prev_version: None,
        }
    }

    /// TS `Snapshotter.init()` — open the first snapshot at the current
    /// DB head. Returns the pinned replica version. Errors if already
    /// initialized or if the replica isn't in wal2 mode.
    pub fn init(&mut self) -> Result<String, String> {
        if self.curr.is_some() {
            return Err("snapshotter already initialized".to_string());
        }
        let reader = SnapshotReader::new(self.db_path.clone());
        let version = reader.pinned_version()?;
        self.curr_version = Some(version.clone());
        self.curr = Some(reader);
        Ok(version)
    }

    pub fn initialized(&self) -> bool {
        self.curr.is_some()
    }

    pub fn current_version(&self) -> Option<&str> {
        self.curr_version.as_deref()
    }

    pub fn prev_version(&self) -> Option<&str> {
        self.prev_version.as_deref()
    }

    /// TS `Snapshotter.advanceWithoutDiff()` — leapfrog the snapshots.
    ///
    /// If `prev` is held, it is recycled: its pin is rolled back and a
    /// fresh `BEGIN CONCURRENT` is acquired on the same connection at
    /// the current DB head. The old `curr` becomes the new `prev`. The
    /// refreshed connection becomes the new `curr`.
    ///
    /// If `prev` is `None` (first advance), a new `SnapshotReader` is
    /// opened for the new `curr` and the old `curr` becomes `prev`.
    ///
    /// Errors if the snapshotter isn't initialized.
    pub fn advance(&mut self) -> Result<AdvanceResult, String> {
        let old_curr = self
            .curr
            .take()
            .ok_or_else(|| "snapshotter not initialized".to_string())?;
        let old_curr_version = self
            .curr_version
            .take()
            .ok_or_else(|| "snapshotter missing curr_version".to_string())?;

        // Build the new curr — either by recycling the old prev (leapfrog)
        // or by opening a fresh connection.
        let (next_curr, next_curr_version) = match self.prev.take() {
            Some(reader) => {
                let v = reader.refresh()?;
                (reader, v)
            }
            None => {
                let reader = SnapshotReader::new(self.db_path.clone());
                let v = reader.pinned_version()?;
                (reader, v)
            }
        };

        self.prev = Some(old_curr);
        self.prev_version = Some(old_curr_version.clone());
        self.curr = Some(next_curr);
        self.curr_version = Some(next_curr_version.clone());
        Ok(AdvanceResult {
            prev_version: old_curr_version,
            curr_version: next_curr_version,
        })
    }

    /// Current reader — a cheap clone. All hot-path reads use this.
    pub fn curr(&self) -> Option<SnapshotReader> {
        self.curr.clone()
    }

    /// Previous reader — `None` before the first advance. Diff iteration
    /// uses this for `get_rows` calls that need the prev-snapshot state.
    pub fn prev(&self) -> Option<SnapshotReader> {
        self.prev.clone()
    }

    /// Count the changelog entries between `prev_version` (exclusive)
    /// and the current snapshot. Matches TS `Snapshot.numChangesSince`.
    /// Runs on the `curr` connection.
    pub fn num_changes_since(&self, prev_version: &str) -> Result<i64, String> {
        let reader = self
            .curr
            .as_ref()
            .ok_or_else(|| "snapshotter not initialized".to_string())?;
        let rx = reader.query_rows(
            "SELECT COUNT(*) AS count FROM \"_zero.changeLog2\" \
             WHERE stateVersion > ?"
                .into(),
            vec![SqlValue::Text(prev_version.to_string())],
            vec!["count".into()],
        );
        let row = rx.recv().map_err(|_| "num_changes recv failed".to_string())?;
        match row.get("count") {
            Some(SqlValue::Integer(n)) => Ok(*n),
            _ => Err("unexpected shape from COUNT(*)".to_string()),
        }
    }

    /// Stream changelog entries between `prev_version` (exclusive) and
    /// the current snapshot, ordered by `(stateVersion ASC, pos ASC)`.
    /// Matches TS `Snapshot.changesSince`.
    ///
    /// Returns a `Receiver` of `OwnedRow`s with the exact columns TS
    /// uses: `stateVersion`, `table`, `rowKey`, `op`.
    pub fn changelog_since(
        &self,
        prev_version: &str,
    ) -> Result<Receiver<OwnedRow>, String> {
        let reader = self
            .curr
            .as_ref()
            .ok_or_else(|| "snapshotter not initialized".to_string())?;
        Ok(reader.query_rows(
            "SELECT \"stateVersion\", \"table\", \"rowKey\", \"op\" \
             FROM \"_zero.changeLog2\" \
             WHERE \"stateVersion\" > ? \
             ORDER BY \"stateVersion\" ASC, \"pos\" ASC"
                .into(),
            vec![SqlValue::Text(prev_version.to_string())],
            vec![
                "stateVersion".into(),
                "table".into(),
                "rowKey".into(),
                "op".into(),
            ],
        ))
    }

    /// Close both connections and drop their pins.
    pub fn destroy(&mut self) {
        self.curr = None;
        self.curr_version = None;
        self.prev = None;
        self.prev_version = None;
    }
}

impl Drop for Snapshotter {
    fn drop(&mut self) {
        self.destroy();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn prepare_replica(dir: &TempDir) -> PathBuf {
        let path = dir.path().join("replica.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = wal2;
            CREATE TABLE "_zero.replicationState" (stateVersion TEXT, lock INT);
            INSERT INTO "_zero.replicationState" VALUES ('v0', 1);
            CREATE TABLE "_zero.changeLog2" (
              stateVersion TEXT,
              pos INTEGER,
              "table" TEXT,
              rowKey TEXT,
              op TEXT
            );
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
            "#,
        )
        .unwrap();
        drop(conn);
        path
    }

    fn advance_writer(path: &PathBuf, to_version: &str, changes: &[(&str, i64)]) {
        let writer = Connection::open(path).unwrap();
        writer
            .execute(
                "UPDATE \"_zero.replicationState\" SET stateVersion = ?",
                [to_version],
            )
            .unwrap();
        for (i, (table, pos)) in changes.iter().enumerate() {
            writer
                .execute(
                    "INSERT INTO \"_zero.changeLog2\" VALUES (?, ?, ?, ?, ?)",
                    rusqlite::params![
                        to_version,
                        *pos,
                        *table,
                        format!("{{\"id\":{}}}", i + 1),
                        "s"
                    ],
                )
                .unwrap();
        }
    }

    #[test]
    fn init_reads_initial_version() {
        let dir = TempDir::new().unwrap();
        let path = prepare_replica(&dir);
        let mut s = Snapshotter::new(path);
        assert_eq!(s.init().unwrap(), "v0");
        assert!(s.initialized());
        assert_eq!(s.current_version(), Some("v0"));
        assert_eq!(s.prev_version(), None);
    }

    #[test]
    fn advance_leapfrogs_and_pins_new_version() {
        let dir = TempDir::new().unwrap();
        let path = prepare_replica(&dir);
        let mut s = Snapshotter::new(path.clone());
        s.init().unwrap();

        advance_writer(&path, "v1", &[("users", 0)]);

        let r = s.advance().unwrap();
        assert_eq!(r.prev_version, "v0");
        assert_eq!(r.curr_version, "v1");
        assert_eq!(s.current_version(), Some("v1"));
        assert_eq!(s.prev_version(), Some("v0"));

        // Second advance uses the leapfrog path (recycles the v0 conn).
        advance_writer(&path, "v2", &[("users", 0)]);
        let r = s.advance().unwrap();
        assert_eq!(r.prev_version, "v1");
        assert_eq!(r.curr_version, "v2");
        assert_eq!(s.current_version(), Some("v2"));
    }

    #[test]
    fn changelog_since_streams_expected_entries() {
        let dir = TempDir::new().unwrap();
        let path = prepare_replica(&dir);
        let mut s = Snapshotter::new(path.clone());
        s.init().unwrap();

        advance_writer(&path, "v1", &[("users", 0), ("users", 1)]);
        s.advance().unwrap();

        assert_eq!(s.num_changes_since("v0").unwrap(), 2);

        let rx = s.changelog_since("v0").unwrap();
        let rows: Vec<OwnedRow> = rx.iter().collect();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.get("table").cloned(), Some(SqlValue::Text("users".into())));
            assert_eq!(row.get("stateVersion").cloned(), Some(SqlValue::Text("v1".into())));
        }
    }

    #[test]
    fn prev_reader_sees_old_snapshot_after_advance() {
        let dir = TempDir::new().unwrap();
        let path = prepare_replica(&dir);
        let mut s = Snapshotter::new(path.clone());
        s.init().unwrap();

        // Pin at v0. Writer updates to v1.
        advance_writer(&path, "v1", &[("users", 0)]);
        s.advance().unwrap();

        // prev reader should still see v0, curr sees v1.
        let prev = s.prev().unwrap();
        let curr = s.curr().unwrap();
        assert_eq!(prev.pinned_version().unwrap(), "v0");
        assert_eq!(curr.pinned_version().unwrap(), "v1");
    }
}
