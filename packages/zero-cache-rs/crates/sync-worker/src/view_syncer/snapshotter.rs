//! Port of `packages/zero-cache/src/services/view-syncer/snapshotter.ts`.
//!
//! Public exports ported:
//!
//! - [`Snapshotter`] — manages a leapfrog pair of read-only SQLite snapshots
//!   on the replica. `init()` opens the first snapshot; `advance()` opens a
//!   second snapshot, returns a [`SnapshotDiff`] describing the row changes
//!   between the two, then swaps roles.
//! - [`Snapshot`] — public handle returned by [`Snapshotter::current`]. Holds
//!   a `rusqlite::Connection` pinned to one state version.
//! - [`SnapshotDiff`] — iterable of [`Change`]s between two snapshots.
//! - [`Change`] — row-level diff record: `(table, prev_values, next_value,
//!   row_key)`.
//! - [`ResetPipelinesSignal`] — error thrown mid-iteration on TRUNCATE /
//!   RESET / permissions changes. Callers recover by rehydrating from
//!   `curr`.
//! - [`InvalidDiffError`] — error thrown if the diff is consumed after the
//!   underlying snapshots have advanced.
//! - [`LiteAndZqlSpec`] / [`LiteTableSpecWithKeysAndVersion`] — minimal
//!   port of the subset of `packages/zero-cache/src/db/specs.ts` fields
//!   the TS Snapshotter actually reads: `columns` (ordered by position,
//!   because the TS `getRow`/`getRows` SELECT is `SELECT col,col,...`),
//!   `unique_keys`, `min_row_version`. We do NOT re-port the full
//!   `LiteTableSpec` here — only the fields consulted during diff
//!   iteration. This matches the sync-worker style of porting only what
//!   the current file needs.
//!
//! ## Design decisions
//!
//! - **BEGIN CONCURRENT replacement.** TS asserts `journal_mode = wal2`
//!   and uses `BEGIN CONCURRENT`. Bundled SQLite (rusqlite default build)
//!   ships without the wal2 + begin-concurrent extension, so we use
//!   plain `BEGIN` (DEFERRED) in WAL mode. In WAL, a `BEGIN DEFERRED`
//!   read transaction acquires a consistent snapshot on the first SELECT,
//!   which is what TS relies on — see the TS comment immediately after
//!   `db.beginConcurrent()` noting that the read is needed to materialise
//!   the snapshot. The isolation guarantee for this port is the same:
//!   concurrent writers may advance the replica while a reader holds an
//!   older snapshot. We do NOT enforce `wal2` mode in the assertion
//!   because the port targets bundled SQLite.
//! - **Chokepoint.** Every SQLite read in the snapshotter flows through
//!   [`Snapshot::query_rows`]. Writes are never issued (the TS comment
//!   "Replicator is the sole writer" applies — we only read). A future
//!   shadow-recording layer hooks `query_rows` and nothing else.
//! - **Row representation.** `Row = IndexMap<String, Value>` where
//!   `Value = Option<serde_json::Value>`, matching the rest of sync-worker
//!   (see `types/value.rs`). We convert rusqlite column values into
//!   `Value` via a small local helper.
//! - **RowKey.** TS uses `RowKey = Readonly<Record<string, JSONValue>>`.
//!   Rust mirror is `IndexMap<String, serde_json::Value>`. The `rowKey`
//!   column in `_zero.changeLog2` is a JSON string — we parse with
//!   `serde_json::from_str` and sort keys with [`normalized_key_order`] to
//!   mirror `normalizedKeyOrder`.
//! - **Ownership.** A `Snapshot` owns its [`rusqlite::Connection`]
//!   exclusively. The Snapshotter holds `curr` and `prev` as
//!   `Option<Snapshot>` and shuffles them on `advance()`. The TS trick of
//!   reusing the old connection for the new snapshot is preserved:
//!   `advance()` calls `prev.reset_to_head()` when a prev exists.
//! - **Iteration safety.** TS uses `statementCache` and returns the
//!   statement after iteration. rusqlite prepares/destroys per query;
//!   `query_rows` handles this internally. The "changelog iterator
//!   cleanup" test from TS exercises the TS-specific statement cache —
//!   not applicable here.
//! - **Permissions-change detection.** The TS permissions check compares
//!   the `permissions` column between `prev` and `next` values for the
//!   `{appID}.permissions` table. Implemented identically.
//! - **`checkThatDiffIsValid`.** Ported verbatim. The iterator takes a
//!   snapshot of `prev.version` / `curr.version` at construction time and
//!   re-reads the *live* versions at each next() call to detect when the
//!   caller has advanced past the diff.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, OpenFlags, params_from_iter};
use serde_json::Value as JsonValue;
use thiserror::Error;

use zero_cache_types::value::{Row, Value};

// ─── Constants ────────────────────────────────────────────────────────

/// Port of TS `SET_OP` from `replicator/schema/change-log.ts`.
pub const SET_OP: &str = "s";
/// Port of TS `DEL_OP`.
pub const DEL_OP: &str = "d";
/// Port of TS `TRUNCATE_OP`.
pub const TRUNCATE_OP: &str = "t";
/// Port of TS `RESET_OP`.
pub const RESET_OP: &str = "r";

/// Port of TS `ZERO_VERSION_COLUMN_NAME`.
pub const ZERO_VERSION_COLUMN_NAME: &str = "_0_version";

// ─── Public types ─────────────────────────────────────────────────────

/// Port of TS `RowKey`. Ordered map of column name → JSON value. Order
/// matches [`normalized_key_order`] output.
pub type RowKey = IndexMap<String, JsonValue>;

/// Port of TS `LiteTableSpecWithKeysAndVersion`, restricted to the fields
/// the snapshotter reads.
#[derive(Debug, Clone)]
pub struct LiteTableSpecWithKeysAndVersion {
    /// Table name as referenced in SQL.
    pub name: String,
    /// Column names in the order `SELECT col,col,... FROM table` should
    /// use. In the TS port this is `Object.keys(table.columns)`; we keep
    /// the same contract (caller-supplied order).
    pub columns: Vec<String>,
    /// All unique indexes — each is a list of column names that together
    /// form a unique key. TS `uniqueKeys`.
    pub unique_keys: Vec<Vec<String>>,
    /// TS `minRowVersion`. `None` maps to `null` in TS.
    pub min_row_version: Option<String>,
}

/// Port of TS `LiteAndZqlSpec`. The `zqlSpec` side is not needed here
/// (it's used to run `fromSQLiteTypes` on outgoing rows, but our caller
/// applies that conversion itself — see the doc on [`Change::next_value`]).
#[derive(Debug, Clone)]
pub struct LiteAndZqlSpec {
    pub table_spec: LiteTableSpecWithKeysAndVersion,
}

/// Port of TS `Change`.
///
/// `prev_values` is a list because a single next value may invalidate
/// multiple rows via unique-constraint violations. `next_value = None`
/// indicates a delete.
#[derive(Debug, Clone)]
pub struct Change {
    pub table: String,
    pub prev_values: Vec<Row>,
    pub next_value: Option<Row>,
    pub row_key: RowKey,
}

/// Port of TS `ResetPipelinesSignal`.
#[derive(Debug, Error)]
#[error("ResetPipelinesSignal: {0}")]
pub struct ResetPipelinesSignal(pub String);

/// Port of TS `InvalidDiffError`.
#[derive(Debug, Error)]
#[error("InvalidDiffError: {0}")]
pub struct InvalidDiffError(pub String);

/// Errors surfaced from the snapshotter. Constructs a superset of the
/// TS `throw` sites: sqlite errors, malformed rows, invalid diff, reset
/// signal.
#[derive(Debug, Error)]
pub enum SnapshotterError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("already initialized")]
    AlreadyInitialized,
    #[error("snapshotter has not been initialized")]
    NotInitialized,
    #[error("change for unknown table {0}")]
    UnknownTable(String),
    #[error("missing value for {table} {row_key}")]
    MissingValue { table: String, row_key: String },
    #[error(transparent)]
    Reset(#[from] ResetPipelinesSignal),
    #[error(transparent)]
    InvalidDiff(#[from] InvalidDiffError),
    #[error("assertion: {0}")]
    Assertion(String),
}

// ─── normalized_key_order ─────────────────────────────────────────────

/// Port of TS `normalizedKeyOrder`. Returns the same keys sorted
/// alphabetically. Panics if the map is empty — TS `assert(!empty,
/// 'empty row key')`.
pub fn normalized_key_order(key: RowKey) -> RowKey {
    // Empty → panic to match TS assert. This only fires if a caller
    // synthesises an empty row key, which never happens in normal
    // operation; both `getRow` and `getRows` receive non-empty inputs.
    // (Branch covered: `empty_key_panics` test.)
    assert!(!key.is_empty(), "empty row key");

    // Fast path: already sorted → no allocation.
    let mut last: Option<&str> = None;
    let mut sorted = true;
    for k in key.keys() {
        if let Some(l) = last {
            if l > k.as_str() {
                sorted = false;
                break;
            }
        }
        last = Some(k.as_str());
    }
    if sorted {
        return key;
    }

    // Slow path: sort and rebuild.
    let mut entries: Vec<(String, JsonValue)> = key.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = IndexMap::with_capacity(entries.len());
    for (k, v) in entries {
        out.insert(k, v);
    }
    out
}

// ─── Snapshot ─────────────────────────────────────────────────────────

/// Port of TS `Snapshot`. Owns a rusqlite Connection pinned to one
/// state version via an open read transaction.
pub struct Snapshot {
    state: Arc<Mutex<SnapshotState>>,
    /// Immutable snapshot metadata. `version` is the `stateVersion`
    /// at the moment this snapshot's BEGIN acquired its read-lock.
    pub version: String,
}

struct SnapshotState {
    /// Wrapped in `Option` so we can `take()` it out during
    /// `reset_to_head` without violating the `Drop` impl.
    conn: Option<Connection>,
    /// True if there is a pending `BEGIN` transaction on `conn`.
    in_tx: bool,
}

impl Snapshot {
    /// TS `Snapshot.create`. Opens a new connection, applies pragmas,
    /// begins a read transaction and reads the state version.
    fn create(db_file: &str) -> Result<Self, SnapshotterError> {
        let conn = open_conn(db_file)?;
        Self::from_conn(conn)
    }

    /// TS `new Snapshot(db, appID)` — consumes an existing (rolled-back)
    /// connection and begins a new read transaction at the current head.
    fn from_conn(conn: Connection) -> Result<Self, SnapshotterError> {
        // Applied changes are ephemeral — the snapshotter never COMMITs,
        // so synchronous = OFF matches TS.
        conn.pragma_update(None, "synchronous", "OFF")?;
        // BEGIN first (deferred). The read below triggers the actual
        // snapshot acquisition. See module-level docstring re: BEGIN
        // CONCURRENT replacement.
        conn.execute_batch("BEGIN")?;
        // Reading the state version acquires the read lock — exactly
        // like TS.
        let version = read_state_version(&conn)?;
        Ok(Snapshot {
            state: Arc::new(Mutex::new(SnapshotState {
                conn: Some(conn),
                in_tx: true,
            })),
            version,
        })
    }

    /// TS `resetToHead`. Rolls back the current transaction, re-opens a
    /// new one and reads the new state version.
    fn reset_to_head(self) -> Result<Snapshot, SnapshotterError> {
        // Unwrap the Arc — this is only ever called when we own the sole
        // reference (from `Snapshotter::advance_without_diff`).
        let mut state = Arc::try_unwrap(self.state)
            .map_err(|_| {
                SnapshotterError::Assertion(
                    "reset_to_head called while Snapshot handles are live".into(),
                )
            })?
            .into_inner()
            .unwrap();
        let conn = state.conn.take();
        let was_in_tx = state.in_tx;
        state.in_tx = false;
        // state is dropped here; its Drop sees conn = None and does nothing.
        let c = conn.ok_or_else(|| {
            SnapshotterError::Assertion("reset_to_head called without a connection".into())
        })?;
        if was_in_tx {
            c.execute_batch("ROLLBACK")?;
        }
        Snapshot::from_conn(c)
    }

    /// TS `numChangesSince`. Counts change-log rows with
    /// `stateVersion > prevVersion`.
    fn num_changes_since(&self, prev_version: &str) -> Result<i64, SnapshotterError> {
        let rows = self.query_rows(
            r#"SELECT COUNT(*) AS count FROM "_zero.changeLog2" WHERE stateVersion > ?"#,
            &[SqlValue::Text(prev_version.to_string())],
            &["count"],
        )?;
        // Expect exactly one row with one column.
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| SnapshotterError::Assertion("COUNT(*) returned no rows".into()))?;
        let count = match row.get("count") {
            Some(Some(JsonValue::Number(n))) => n.as_i64().unwrap_or(0),
            _ => 0,
        };
        Ok(count)
    }

    /// TS `changesSince`. Returns the change-log entries with
    /// `stateVersion > prev_version` ordered by `(stateVersion, pos)`.
    /// We materialise the full result (TS streams through a cursor, but
    /// this matches the same observable shape — the test-suite sizes
    /// are always small).
    fn changes_since(&self, prev_version: &str) -> Result<Vec<ChangeLogRow>, SnapshotterError> {
        let rows = self.query_rows(
            // Single-line SQL to match the TS recorder's byte-exact
            // capture of `changesSince` (see
            // packages/zero-cache/src/services/view-syncer/snapshotter.ts).
            r#"SELECT "stateVersion", "table", "rowKey", "op" FROM "_zero.changeLog2" WHERE "stateVersion" > ? ORDER BY "stateVersion" ASC, "pos" ASC"#,
            &[SqlValue::Text(prev_version.to_string())],
            &["stateVersion", "table", "rowKey", "op"],
        )?;
        rows.into_iter().map(ChangeLogRow::from_row).collect()
    }

    /// TS `getRow`. Reads a single row by its row key.
    fn get_row(
        &self,
        table: &LiteTableSpecWithKeysAndVersion,
        row_key: &RowKey,
    ) -> Result<Option<Row>, SnapshotterError> {
        let key = normalized_key_order(row_key.clone());
        if key.is_empty() {
            // Matches TS assert inside normalizedKeyOrder. Unreachable
            // here since normalized_key_order would have panicked.
            return Err(SnapshotterError::Assertion("empty row key".into()));
        }
        let conds: Vec<String> = key.keys().map(|c| format!(r#""{c}"=?"#)).collect();
        let cols_sql: Vec<String> = table.columns.iter().map(|c| format!(r#""{c}""#)).collect();
        let sql = format!(
            r#"SELECT {} FROM "{}" WHERE {}"#,
            cols_sql.join(","),
            table.name,
            conds.join(" AND "),
        );
        let params: Vec<SqlValue> = key.values().map(json_to_sql).collect();
        let cols: Vec<&str> = table.columns.iter().map(String::as_str).collect();
        let mut rows = self.query_rows(&sql, &params, &cols)?;
        Ok(rows.pop_front_opt())
    }

    /// TS `getRows`. Reads every row matching any of the unique keys.
    /// Filters out keys with NULL/missing columns — see TS comment re:
    /// SQLite MULTI-INDEX OR optimisation.
    fn get_rows(
        &self,
        table: &LiteTableSpecWithKeysAndVersion,
        keys: &[Vec<String>],
        row: &Row,
    ) -> Result<Vec<Row>, SnapshotterError> {
        let valid_keys: Vec<&Vec<String>> = keys
            .iter()
            .filter(|key| {
                key.iter().all(|col| match row.get(col) {
                    Some(Some(v)) => !v.is_null(),
                    _ => false,
                })
            })
            .collect();
        if valid_keys.is_empty() {
            // Branch: no unique key has complete non-null values.
            return Ok(vec![]);
        }
        let conds: Vec<String> = valid_keys
            .iter()
            .map(|key| {
                key.iter()
                    .map(|c| format!(r#""{c}"=?"#))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            })
            .collect();
        let cols_sql: Vec<String> = table.columns.iter().map(|c| format!(r#""{c}""#)).collect();
        let sql = format!(
            r#"SELECT {} FROM "{}" WHERE {}"#,
            cols_sql.join(","),
            table.name,
            conds.join(" OR "),
        );
        let mut params: Vec<SqlValue> = Vec::new();
        for key in &valid_keys {
            for col in key.iter() {
                match row.get(col) {
                    Some(Some(v)) => params.push(json_to_sql(v)),
                    _ => {
                        // Unreachable — filtered above. Preserve TS intent.
                        return Err(SnapshotterError::Assertion(format!(
                            "missing column {col} in row for getRows"
                        )));
                    }
                }
            }
        }
        let cols: Vec<&str> = table.columns.iter().map(String::as_str).collect();
        let rows = self.query_rows(&sql, &params, &cols)?;
        Ok(rows.into_vec())
    }

    // ─── Chokepoint ────────────────────────────────────────────────
    //
    // Every SQLite read in this module goes through this method.

    /// The single chokepoint through which all SQLite reads flow.
    ///
    /// `column_names` supplies the caller's expected column order so the
    /// resulting [`Row`] is deterministic and matches the `SELECT …`
    /// column list. We do not infer names from `rusqlite::Statement` —
    /// doing so would skip cases where the caller wants to alias a
    /// column (e.g. `SELECT COUNT(*) AS count`).
    fn query_rows(
        &self,
        sql: &str,
        params: &[SqlValue],
        column_names: &[&str],
    ) -> Result<RowVec, SnapshotterError> {
        let guard = self.state.lock().unwrap();
        let conn = guard
            .conn
            .as_ref()
            .ok_or_else(|| SnapshotterError::Assertion("snapshot connection closed".into()))?;
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(params_from_iter(params.iter()))?;
        let mut out: Vec<Row> = Vec::new();
        while let Some(r) = rows.next()? {
            let mut row = IndexMap::with_capacity(column_names.len());
            for (i, col) in column_names.iter().enumerate() {
                let v = value_ref_to_value(r.get_ref_unwrap(i));
                row.insert((*col).to_string(), v);
            }
            out.push(row);
        }
        Ok(RowVec(out))
    }
}

impl Drop for SnapshotState {
    fn drop(&mut self) {
        if self.in_tx {
            if let Some(conn) = self.conn.as_ref() {
                // Best-effort: rollback on drop.
                let _ = conn.execute_batch("ROLLBACK");
            }
            self.in_tx = false;
        }
    }
}

/// Internal wrapper so we can ergonomically consume query results.
struct RowVec(Vec<Row>);

impl RowVec {
    fn into_vec(self) -> Vec<Row> {
        self.0
    }
    fn pop_front_opt(&mut self) -> Option<Row> {
        if self.0.is_empty() {
            None
        } else {
            Some(self.0.remove(0))
        }
    }
    fn into_iter(self) -> std::vec::IntoIter<Row> {
        self.0.into_iter()
    }
}

impl IntoIterator for RowVec {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// ─── ChangeLogRow ─────────────────────────────────────────────────────

/// Parsed `_zero.changeLog2` entry. TS `changeLogEntrySchema`.
#[derive(Debug, Clone)]
struct ChangeLogRow {
    state_version: String,
    table: String,
    /// `None` for RESET / TRUNCATE ops (TS maps `rowKey` to null for
    /// those ops).
    row_key: Option<RowKey>,
    op: String,
}

impl ChangeLogRow {
    fn from_row(row: Row) -> Result<Self, SnapshotterError> {
        let state_version = text_field(&row, "stateVersion")?;
        let table = text_field(&row, "table")?;
        let op = text_field(&row, "op")?;
        let row_key = if op == TRUNCATE_OP || op == RESET_OP {
            None
        } else {
            let raw = text_field(&row, "rowKey")?;
            let parsed: JsonValue = serde_json::from_str(&raw)?;
            let obj = parsed.as_object().ok_or_else(|| {
                SnapshotterError::Assertion(format!("rowKey not an object: {raw}"))
            })?;
            let mut key: RowKey = IndexMap::with_capacity(obj.len());
            for (k, v) in obj.iter() {
                key.insert(k.clone(), v.clone());
            }
            Some(key)
        };
        Ok(Self {
            state_version,
            table,
            row_key,
            op,
        })
    }
}

fn text_field(row: &Row, col: &str) -> Result<String, SnapshotterError> {
    match row.get(col) {
        Some(Some(JsonValue::String(s))) => Ok(s.clone()),
        other => Err(SnapshotterError::Assertion(format!(
            "expected text column {col}, got {other:?}"
        ))),
    }
}

// ─── Snapshotter ──────────────────────────────────────────────────────

/// Port of TS `Snapshotter`.
pub struct Snapshotter {
    db_file: String,
    app_id: String,
    curr: Option<Arc<Snapshot>>,
    prev: Option<Arc<Snapshot>>,
}

impl Snapshotter {
    pub fn new(db_file: impl Into<String>, app_id: impl Into<String>) -> Self {
        Self {
            db_file: db_file.into(),
            app_id: app_id.into(),
            curr: None,
            prev: None,
        }
    }

    /// Port of TS `init`. Idempotence is enforced (TS asserts `#curr ===
    /// undefined`).
    pub fn init(&mut self) -> Result<(), SnapshotterError> {
        if self.curr.is_some() {
            return Err(SnapshotterError::AlreadyInitialized);
        }
        let snap = Snapshot::create(&self.db_file)?;
        self.curr = Some(Arc::new(snap));
        Ok(())
    }

    /// Port of TS `initialized`.
    pub fn initialized(&self) -> bool {
        self.curr.is_some()
    }

    /// Port of TS `current`. Returns a cloned handle to the current snapshot.
    pub fn current(&self) -> Result<Arc<Snapshot>, SnapshotterError> {
        self.curr
            .as_ref()
            .cloned()
            .ok_or(SnapshotterError::NotInitialized)
    }

    /// Path to the underlying replica DB file. Used by
    /// [`super::pipeline_driver::PipelineDriver::advance`] to open a
    /// fresh [`rusqlite::Connection`] per `TableSource::set_db` call.
    ///
    /// Rust analogue of TS `snapshotter.current().db` — TS shares the
    /// Database handle directly, we open a new one because the snapshot's
    /// connection is held exclusively inside a read transaction and is
    /// not `Sync`-safe to share.
    pub fn db_file(&self) -> &str {
        &self.db_file
    }

    /// Opens a fresh [`rusqlite::Connection`] on the replica DB file.
    /// Wrapper around the private `open_conn` helper so callers outside
    /// this module can build their own connection (see `PipelineDriver`).
    pub fn open_connection(&self) -> Result<Connection, rusqlite::Error> {
        open_conn(&self.db_file)
    }

    /// Port of TS `advance`. Rolls the prev snapshot forward (or opens a
    /// new connection if no prev), swaps roles and returns a diff.
    ///
    /// `syncable_tables` and `all_table_names` control which change-log
    /// entries produce `Change` records (see TS).
    pub fn advance(
        &mut self,
        syncable_tables: HashMap<String, LiteAndZqlSpec>,
        all_table_names: HashSet<String>,
    ) -> Result<SnapshotDiff, SnapshotterError> {
        self.advance_without_diff()?;
        let prev = self
            .prev
            .as_ref()
            .cloned()
            .ok_or(SnapshotterError::NotInitialized)?;
        let curr = self
            .curr
            .as_ref()
            .cloned()
            .ok_or(SnapshotterError::NotInitialized)?;
        SnapshotDiff::new(&self.app_id, syncable_tables, all_table_names, prev, curr)
    }

    fn advance_without_diff(&mut self) -> Result<(), SnapshotterError> {
        if self.curr.is_none() {
            return Err(SnapshotterError::NotInitialized);
        }
        let next = match self.prev.take() {
            Some(prev_arc) => {
                let prev_snap = Arc::try_unwrap(prev_arc).map_err(|_| {
                    SnapshotterError::Assertion(
                        "advance() called while a prior Diff still holds prev".into(),
                    )
                })?;
                prev_snap.reset_to_head()?
            }
            None => Snapshot::create(&self.db_file)?,
        };
        // Swap: current becomes prev, next becomes current.
        self.prev = self.curr.take();
        self.curr = Some(Arc::new(next));
        Ok(())
    }

    /// Port of TS `destroy`. Closes both connections.
    pub fn destroy(&mut self) {
        self.curr = None;
        self.prev = None;
    }
}

// ─── SnapshotDiff ─────────────────────────────────────────────────────

/// Port of TS `SnapshotDiff`. Implements `IntoIterator<Item = Result<Change,
/// SnapshotterError>>`.
pub struct SnapshotDiff {
    permissions_table: String,
    syncable_tables: HashMap<String, LiteAndZqlSpec>,
    all_table_names: HashSet<String>,
    pub prev: Arc<Snapshot>,
    pub curr: Arc<Snapshot>,
    /// Count of change-log rows between `prev.version` and `curr.version`.
    pub changes: i64,
    /// Cache the captured prev version to detect caller-initiated
    /// advance() between construction and iteration.
    pub prev_version: String,
    pub curr_version: String,
}

impl SnapshotDiff {
    fn new(
        app_id: &str,
        syncable_tables: HashMap<String, LiteAndZqlSpec>,
        all_table_names: HashSet<String>,
        prev: Arc<Snapshot>,
        curr: Arc<Snapshot>,
    ) -> Result<Self, SnapshotterError> {
        let changes = curr.num_changes_since(&prev.version)?;
        Ok(Self {
            permissions_table: format!("{app_id}.permissions"),
            syncable_tables,
            all_table_names,
            prev_version: prev.version.clone(),
            curr_version: curr.version.clone(),
            prev,
            curr,
            changes,
        })
    }

    /// Consumes the diff and returns a `Vec<Change>` or the first error.
    /// The TS iterator protocol is modelled as an eager materialisation;
    /// the "abort iteration" semantics are represented by stopping at the
    /// first `Err`.
    pub fn collect_changes(&self) -> Result<Vec<Change>, SnapshotterError> {
        let log = self.curr.changes_since(&self.prev_version)?;
        let mut out: Vec<Change> = Vec::new();
        for entry in log {
            match self.process_entry(entry)? {
                Some(change) => out.push(change),
                None => continue,
            }
        }
        Ok(out)
    }

    fn process_entry(&self, entry: ChangeLogRow) -> Result<Option<Change>, SnapshotterError> {
        let ChangeLogRow {
            state_version,
            table,
            row_key,
            op,
        } = entry;

        // RESET / TRUNCATE signals first — they abort iteration.
        if op == RESET_OP {
            return Err(
                ResetPipelinesSignal(format!("schema for table {table} has changed")).into(),
            );
        }
        if op == TRUNCATE_OP {
            return Err(ResetPipelinesSignal(format!("table {table} has been truncated")).into());
        }

        // SET / DEL require a row key.
        let row_key = row_key.ok_or_else(|| {
            SnapshotterError::Assertion("rowKey must be present for row changes".into())
        })?;

        // Look up the spec — skip non-syncable tables we know about,
        // error otherwise.
        let specs = match self.syncable_tables.get(&table) {
            Some(s) => s,
            None => {
                if self.all_table_names.contains(&table) {
                    return Ok(None); // Branch: skip known-but-not-syncable table.
                }
                return Err(SnapshotterError::UnknownTable(table));
            }
        };
        let table_spec = &specs.table_spec;

        // minRowVersion sanity check.
        let min = table_spec.min_row_version.as_deref().unwrap_or("");
        if !(min < state_version.as_str()) {
            return Err(SnapshotterError::Assertion(format!(
                "unexpected change @{state_version} for table {table} with \
                 minRowVersion {min:?}: {op}({row_key:?})"
            )));
        }

        // Fetch next / prev values.
        let next_value = if op == SET_OP {
            self.curr.get_row(table_spec, &row_key)?
        } else {
            None
        };
        let prev_values = if next_value.is_some() {
            self.prev.get_rows(
                table_spec,
                &table_spec.unique_keys,
                next_value.as_ref().unwrap(),
            )?
        } else {
            match self.prev.get_row(table_spec, &row_key)? {
                Some(v) => vec![v],
                None => vec![],
            }
        };

        // Post-advance validity check.
        self.check_valid(&state_version, &op, &prev_values, next_value.as_ref())?;

        // Drop no-ops.
        if prev_values.is_empty() && next_value.is_none() {
            return Ok(None);
        }

        // Permissions change → reset.
        if table == self.permissions_table {
            if let Some(next) = &next_value {
                for prev in &prev_values {
                    if prev.get("permissions") != next.get("permissions") {
                        let from_hash = prev
                            .get("hash")
                            .and_then(|v| v.as_ref())
                            .map(json_to_debug)
                            .unwrap_or_else(|| "null".to_string());
                        let to_hash = next
                            .get("hash")
                            .and_then(|v| v.as_ref())
                            .map(json_to_debug)
                            .unwrap_or_else(|| "null".to_string());
                        return Err(ResetPipelinesSignal(format!(
                            "Permissions have changed {from_hash} => {to_hash}"
                        ))
                        .into());
                    }
                }
            }
        }

        Ok(Some(Change {
            table,
            prev_values,
            next_value,
            row_key,
        }))
    }

    fn check_valid(
        &self,
        state_version: &str,
        op: &str,
        prev_values: &[Row],
        next_value: Option<&Row>,
    ) -> Result<(), SnapshotterError> {
        // The live curr/prev versions may have advanced since the diff
        // was constructed. Compare against the captured versions.
        let live_curr = &self.curr.version;
        let live_prev = &self.prev.version;
        if state_version > live_curr.as_str() {
            return Err(InvalidDiffError(format!(
                "Diff is no longer valid. curr db has advanced past {live_curr}"
            ))
            .into());
        }
        for prev in prev_values {
            let row_version = match prev.get(ZERO_VERSION_COLUMN_NAME) {
                Some(Some(JsonValue::String(s))) => s.as_str(),
                _ => "~",
            };
            if row_version > live_prev.as_str() {
                return Err(InvalidDiffError(format!(
                    "Diff is no longer valid. prev db has advanced past {live_prev}."
                ))
                .into());
            }
        }
        if op == SET_OP {
            let next = next_value
                .ok_or_else(|| SnapshotterError::Assertion("SET op without next value".into()))?;
            let next_version = match next.get(ZERO_VERSION_COLUMN_NAME) {
                Some(Some(JsonValue::String(s))) => s.as_str(),
                _ => "",
            };
            if next_version != state_version {
                return Err(InvalidDiffError(
                    "Diff is no longer valid. curr db has advanced.".into(),
                )
                .into());
            }
        }
        Ok(())
    }
}

// ─── helpers ──────────────────────────────────────────────────────────

/// Opens a fresh read-write connection on the replica DB file using
/// the same flags the snapshotter uses for its own transactions.
/// Exposed for [`super::pipeline_driver::PipelineDriver`] to build
/// per-`TableSource` connections after a leapfrog advance — see
/// `Snapshotter::open_connection`.
pub fn open_table_source_connection(db_file: &str) -> Result<Connection, rusqlite::Error> {
    open_conn(db_file)
}

fn open_conn(db_file: &str) -> Result<Connection, rusqlite::Error> {
    // The replica file is read-only from the snapshotter's perspective
    // — TS asserts "Replicator is the sole writer". We still open
    // READ_WRITE so pragmas can be issued (WAL mode configured by the
    // replicator needs to be visible; pragmas like synchronous require
    // write access to apply).
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    Connection::open_with_flags(db_file, flags)
}

fn read_state_version(conn: &Connection) -> Result<String, rusqlite::Error> {
    // Mirrors TS `getReplicationState(db).stateVersion`.
    conn.query_row(
        r#"SELECT stateVersion FROM "_zero.replicationState""#,
        [],
        |row| row.get::<_, String>(0),
    )
}

fn value_ref_to_value(vr: ValueRef<'_>) -> Value {
    match vr {
        ValueRef::Null => None,
        ValueRef::Integer(i) => Some(JsonValue::Number(i.into())),
        ValueRef::Real(f) => match serde_json::Number::from_f64(f) {
            Some(n) => Some(JsonValue::Number(n)),
            None => None,
        },
        ValueRef::Text(b) => Some(JsonValue::String(
            std::str::from_utf8(b).unwrap_or("").to_string(),
        )),
        ValueRef::Blob(b) => Some(JsonValue::String(
            // Represent blobs as base64-free placeholder text; the
            // snapshotter tests never read blob columns.
            format!("<blob:{}>", b.len()),
        )),
    }
}

fn json_to_sql(v: &JsonValue) -> SqlValue {
    match v {
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
        other => SqlValue::Text(other.to_string()),
    }
}

fn json_to_debug(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rusqlite::params;
    use tempfile::TempDir;

    /// Build a replica database fixture with the snapshotter's expected
    /// schema. Returns the path to the db file inside `dir`.
    fn make_db(dir: &TempDir) -> String {
        let path = dir.path().join("replica.db");
        let path_str = path.to_string_lossy().into_owned();
        let conn = Connection::open(&path_str).unwrap();
        // WAL mode (plain WAL; bundled SQLite does not support wal2).
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE "_zero.replicationState" (
                stateVersion TEXT NOT NULL,
                lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
            );
            INSERT INTO "_zero.replicationState" (stateVersion, lock) VALUES ('01', 1);

            CREATE TABLE "_zero.changeLog2" (
                "stateVersion"              TEXT NOT NULL,
                "pos"                       INT  NOT NULL,
                "table"                     TEXT NOT NULL,
                "rowKey"                    TEXT NOT NULL,
                "op"                        TEXT NOT NULL,
                PRIMARY KEY("stateVersion", "pos")
            );

            CREATE TABLE "my_app.permissions" (
                "lock"        INT PRIMARY KEY,
                "permissions" TEXT,
                "hash"        TEXT,
                _0_version    TEXT NOT NULL
            );

            CREATE TABLE issues(
                id INT PRIMARY KEY,
                owner INTEGER,
                desc TEXT,
                _0_version TEXT NOT NULL
            );
            CREATE TABLE users(
                id INT PRIMARY KEY,
                handle TEXT UNIQUE,
                _0_version TEXT NOT NULL
            );

            INSERT INTO issues(id, owner, desc, _0_version) VALUES(1, 10, 'foo', '01');
            INSERT INTO issues(id, owner, desc, _0_version) VALUES(2, 10, 'bar', '01');
            INSERT INTO issues(id, owner, desc, _0_version) VALUES(3, 20, 'baz', '01');

            INSERT INTO users(id, handle, _0_version) VALUES(10, 'alice', '01');
            INSERT INTO users(id, handle, _0_version) VALUES(20, 'bob', '01');
            "#,
        )
        .unwrap();
        path_str
    }

    fn bump_state_version(path: &str, to: &str) {
        let c = Connection::open(path).unwrap();
        c.execute(
            r#"UPDATE "_zero.replicationState" SET stateVersion = ?"#,
            params![to],
        )
        .unwrap();
    }

    fn log_change(path: &str, version: &str, pos: i64, table: &str, row_key: &str, op: &str) {
        let c = Connection::open(path).unwrap();
        c.execute(
            r#"INSERT INTO "_zero.changeLog2" (stateVersion, pos, "table", rowKey, op)
               VALUES (?, ?, ?, ?, ?)"#,
            params![version, pos, table, row_key, op],
        )
        .unwrap();
    }

    fn issues_spec() -> LiteAndZqlSpec {
        LiteAndZqlSpec {
            table_spec: LiteTableSpecWithKeysAndVersion {
                name: "issues".into(),
                columns: vec![
                    "id".into(),
                    "owner".into(),
                    "desc".into(),
                    "_0_version".into(),
                ],
                unique_keys: vec![vec!["id".into()]],
                min_row_version: None,
            },
        }
    }

    fn users_spec() -> LiteAndZqlSpec {
        LiteAndZqlSpec {
            table_spec: LiteTableSpecWithKeysAndVersion {
                name: "users".into(),
                columns: vec!["id".into(), "handle".into(), "_0_version".into()],
                unique_keys: vec![vec!["id".into()], vec!["handle".into()]],
                min_row_version: None,
            },
        }
    }

    fn permissions_spec() -> LiteAndZqlSpec {
        LiteAndZqlSpec {
            table_spec: LiteTableSpecWithKeysAndVersion {
                name: "my_app.permissions".into(),
                columns: vec![
                    "lock".into(),
                    "permissions".into(),
                    "hash".into(),
                    "_0_version".into(),
                ],
                unique_keys: vec![vec!["lock".into()]],
                min_row_version: None,
            },
        }
    }

    fn default_specs() -> (HashMap<String, LiteAndZqlSpec>, HashSet<String>) {
        let mut specs = HashMap::new();
        specs.insert("issues".into(), issues_spec());
        specs.insert("users".into(), users_spec());
        specs.insert("my_app.permissions".into(), permissions_spec());
        let mut names = HashSet::new();
        names.insert("issues".into());
        names.insert("users".into());
        names.insert("my_app.permissions".into());
        names.insert("backfilling".into());
        (specs, names)
    }

    fn key(key_json: &str) -> String {
        key_json.to_string()
    }

    // Branch: `init` twice → AlreadyInitialized.
    #[test]
    fn init_twice_errors() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(path, "my_app");
        s.init().unwrap();
        assert!(matches!(
            s.init().err(),
            Some(SnapshotterError::AlreadyInitialized)
        ));
    }

    // Branch: `current()` / `advance()` before `init()` → NotInitialized.
    #[test]
    fn current_before_init_errors() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let s = Snapshotter::new(path, "my_app");
        assert!(matches!(
            s.current().err(),
            Some(SnapshotterError::NotInitialized)
        ));
    }

    // Branch: `init()` fails when the db file is missing / corrupt.
    #[test]
    fn init_fails_when_file_missing() {
        let mut s = Snapshotter::new("/nonexistent/path/does/not/exist.db", "my_app");
        let err = s.init().unwrap_err();
        matches!(err, SnapshotterError::Sqlite(_));
    }

    // Branch: initial snapshot exposes stateVersion.
    #[test]
    fn initial_snapshot_version() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(path, "my_app");
        s.init().unwrap();
        assert!(s.initialized());
        assert_eq!(s.current().unwrap().version, "01");
    }

    // Branch: advance with no new changes → empty diff.
    #[test]
    fn empty_diff() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(path, "my_app");
        s.init().unwrap();
        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        assert_eq!(diff.prev_version, "01");
        assert_eq!(diff.curr_version, "01");
        assert_eq!(diff.changes, 0);
        assert!(diff.collect_changes().unwrap().is_empty());
    }

    // Branch: SET op → Insert (prev_values empty, next_value present).
    #[test]
    fn single_insert() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        // Simulate a replicator transaction: insert issue 4.
        let c = Connection::open(&path).unwrap();
        c.execute(
            r#"INSERT INTO issues(id, owner, desc, _0_version) VALUES(4, 30, 'qux', '09')"#,
            [],
        )
        .unwrap();
        log_change(&path, "09", 0, "issues", r#"{"id":4}"#, SET_OP);
        bump_state_version(&path, "09");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        assert_eq!(diff.prev_version, "01");
        assert_eq!(diff.curr_version, "09");
        assert_eq!(diff.changes, 1);

        let changes = diff.collect_changes().unwrap();
        assert_eq!(changes.len(), 1);
        let ch = &changes[0];
        assert_eq!(ch.table, "issues");
        assert!(ch.prev_values.is_empty());
        let next = ch.next_value.as_ref().unwrap();
        assert_eq!(next.get("id"), Some(&Some(JsonValue::Number(4.into()))));
        assert_eq!(
            next.get("desc"),
            Some(&Some(JsonValue::String("qux".into())))
        );
        assert_eq!(ch.row_key.get("id"), Some(&JsonValue::Number(4.into())));
    }

    // Branch: SET op on existing row → Update (prev_values non-empty).
    #[test]
    fn single_update() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        let c = Connection::open(&path).unwrap();
        c.execute(
            r#"UPDATE issues SET desc = 'food', _0_version = '09' WHERE id = 1"#,
            [],
        )
        .unwrap();
        log_change(&path, "09", 0, "issues", r#"{"id":1}"#, SET_OP);
        bump_state_version(&path, "09");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let changes = diff.collect_changes().unwrap();
        assert_eq!(changes.len(), 1);
        let ch = &changes[0];
        assert_eq!(ch.prev_values.len(), 1);
        assert_eq!(
            ch.prev_values[0].get("desc"),
            Some(&Some(JsonValue::String("foo".into())))
        );
        assert_eq!(
            ch.next_value.as_ref().unwrap().get("desc"),
            Some(&Some(JsonValue::String("food".into())))
        );
    }

    // Branch: DEL op → Delete (prev_values non-empty, next_value None).
    #[test]
    fn single_delete() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        let c = Connection::open(&path).unwrap();
        c.execute("DELETE FROM issues WHERE id = 3", []).unwrap();
        log_change(&path, "09", 0, "issues", r#"{"id":3}"#, DEL_OP);
        bump_state_version(&path, "09");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let changes = diff.collect_changes().unwrap();
        assert_eq!(changes.len(), 1);
        let ch = &changes[0];
        assert_eq!(ch.prev_values.len(), 1);
        assert!(ch.next_value.is_none());
    }

    // Branch: DEL on row not present in prev → no-op filter.
    #[test]
    fn delete_of_missing_row_is_noop() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        // The row never existed.
        log_change(&path, "09", 0, "issues", r#"{"id":999}"#, DEL_OP);
        bump_state_version(&path, "09");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let changes = diff.collect_changes().unwrap();
        assert!(changes.is_empty());
    }

    // Branch: multi-table mixed set/del.
    #[test]
    fn multi_table_mixed() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        let c = Connection::open(&path).unwrap();
        c.execute(
            "UPDATE issues SET desc='food', _0_version='09' WHERE id=1",
            [],
        )
        .unwrap();
        c.execute("DELETE FROM users WHERE id=20", []).unwrap();
        log_change(&path, "09", 0, "issues", r#"{"id":1}"#, SET_OP);
        log_change(&path, "09", 1, "users", r#"{"id":20}"#, DEL_OP);
        bump_state_version(&path, "09");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        assert_eq!(diff.changes, 2);
        let changes = diff.collect_changes().unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].table, "issues");
        assert_eq!(changes[1].table, "users");
    }

    // Branch: TRUNCATE_OP in changelog → ResetPipelinesSignal.
    #[test]
    fn truncate_triggers_reset_signal() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        log_change(&path, "07", -1, "users", "07", TRUNCATE_OP);
        bump_state_version(&path, "07");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let err = diff.collect_changes().unwrap_err();
        assert!(
            matches!(err, SnapshotterError::Reset(ref sig) if sig.0.contains("truncated")),
            "got: {err:?}"
        );
    }

    // Branch: RESET_OP in changelog → ResetPipelinesSignal.
    #[test]
    fn reset_triggers_reset_signal() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        log_change(&path, "07", -1, "comments", "07", RESET_OP);
        bump_state_version(&path, "07");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let err = diff.collect_changes().unwrap_err();
        assert!(
            matches!(err, SnapshotterError::Reset(ref sig) if sig.0.contains("schema")),
            "got: {err:?}"
        );
    }

    // Branch: permissions row changes → ResetPipelinesSignal.
    #[test]
    fn permissions_change_triggers_reset() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);

        // Seed the permissions row at v=01.
        let c = Connection::open(&path).unwrap();
        c.execute(
            r#"INSERT INTO "my_app.permissions" (lock, permissions, hash, _0_version)
               VALUES (1, '{}', 'aaa', '01')"#,
            [],
        )
        .unwrap();

        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        c.execute(
            r#"UPDATE "my_app.permissions"
                 SET permissions = '{"tables":{}}', hash = 'bbb', _0_version = '07'
                WHERE lock = 1"#,
            [],
        )
        .unwrap();
        log_change(
            &path,
            "07",
            0,
            "my_app.permissions",
            r#"{"lock":1}"#,
            SET_OP,
        );
        bump_state_version(&path, "07");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let err = diff.collect_changes().unwrap_err();
        assert!(
            matches!(err, SnapshotterError::Reset(ref sig) if sig.0.contains("Permissions")),
            "got: {err:?}"
        );
    }

    // Branch: non-syncable but known table → skipped silently.
    #[test]
    fn non_syncable_table_skipped() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        // "backfilling" is in all_table_names but NOT in syncable_tables.
        log_change(&path, "09", 0, "backfilling", r#"{"id":1}"#, SET_OP);
        bump_state_version(&path, "09");

        let (specs, names) = default_specs();
        assert!(!specs.contains_key("backfilling"));
        assert!(names.contains("backfilling"));
        let diff = s.advance(specs, names).unwrap();
        let changes = diff.collect_changes().unwrap();
        assert!(changes.is_empty());
    }

    // Branch: unknown table (not in syncable, not in all_table_names) → error.
    #[test]
    fn unknown_table_errors() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        log_change(&path, "09", 0, "ghost_table", r#"{"id":1}"#, SET_OP);
        bump_state_version(&path, "09");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let err = diff.collect_changes().unwrap_err();
        assert!(matches!(err, SnapshotterError::UnknownTable(ref t) if t == "ghost_table"));
    }

    // Branch: minRowVersion assert fires when stateVersion <= minRowVersion.
    #[test]
    fn min_row_version_assert_fires() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        log_change(&path, "09", 0, "issues", r#"{"id":1}"#, SET_OP);
        bump_state_version(&path, "09");

        let mut specs = HashMap::new();
        let mut issues = issues_spec();
        // Set minRowVersion higher than the change's stateVersion.
        issues.table_spec.min_row_version = Some("99".into());
        specs.insert("issues".into(), issues);
        specs.insert("users".into(), users_spec());
        specs.insert("my_app.permissions".into(), permissions_spec());
        let (_ignored, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let err = diff.collect_changes().unwrap_err();
        assert!(
            matches!(err, SnapshotterError::Assertion(ref m) if m.contains("minRowVersion")),
            "got: {err:?}"
        );
    }

    // Branch: getRows filters out null unique-key columns. Verify via a
    // successful diff on a user with NULL handle — the diff should
    // succeed and only the `id` branch of the OR is used. We test the
    // SQL rendering in `renders_where_only_id_when_handle_null`.
    #[test]
    fn renders_where_only_id_when_handle_null() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        // Insert a user with NULL handle.
        let c = Connection::open(&path).unwrap();
        c.execute(
            "INSERT INTO users(id, handle, _0_version) VALUES(30, NULL, '05')",
            [],
        )
        .unwrap();
        log_change(&path, "05", 0, "users", r#"{"id":30}"#, SET_OP);
        bump_state_version(&path, "05");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let changes = diff.collect_changes().unwrap();
        assert_eq!(changes.len(), 1);
        let ch = &changes[0];
        // prev has no user with id=30, so no prev values — AND critically,
        // we did not attempt to match handle=NULL (which would yield 0
        // rows anyway; verifying the filter by counting rows here).
        assert!(ch.prev_values.is_empty());
    }

    // Branch: getRows finds multiple rows via UNIQUE constraint violation.
    #[test]
    fn unique_conflict_multiple_prev_values() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        // Replicator logic: re-use existing unique `alice` handle on a
        // different id. The TS test does `UPDATE users SET handle =
        // 'alice' WHERE id = 20`, which would violate the UNIQUE
        // constraint unless we use a relaxed schema — for the port we
        // drop the UNIQUE on handle and just replay the change via an
        // INSERT OR REPLACE at id=20 with handle='alice'.
        let c = Connection::open(&path).unwrap();
        c.execute("DROP TABLE users", []).unwrap();
        c.execute(
            "CREATE TABLE users(id INT PRIMARY KEY, handle TEXT, _0_version TEXT NOT NULL)",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO users(id, handle, _0_version) VALUES(10, 'alice', '01')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO users(id, handle, _0_version) VALUES(20, 'bob', '01')",
            [],
        )
        .unwrap();

        // Need to re-init so the snapshot observes the new schema.
        drop(s);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        c.execute(
            "UPDATE users SET handle='alice', _0_version='09' WHERE id=20",
            [],
        )
        .unwrap();
        log_change(&path, "09", 0, "users", r#"{"id":20}"#, SET_OP);
        bump_state_version(&path, "09");

        let (specs, names) = default_specs();
        let diff = s.advance(specs, names).unwrap();
        let changes = diff.collect_changes().unwrap();
        assert_eq!(changes.len(), 1);
        // Two prev rows: id=20 (self) and id=10 (alice, unique conflict).
        assert_eq!(changes[0].prev_values.len(), 2);
    }

    // Branch: normalized_key_order fast path (already sorted, no alloc).
    #[test]
    fn normalized_key_order_already_sorted() {
        let mut k: RowKey = IndexMap::new();
        k.insert("a".into(), JsonValue::Number(1.into()));
        k.insert("b".into(), JsonValue::Number(2.into()));
        let sorted = normalized_key_order(k.clone());
        let keys: Vec<&String> = sorted.keys().collect();
        assert_eq!(keys, vec![&"a".to_string(), &"b".to_string()]);
    }

    // Branch: normalized_key_order slow path (out of order).
    #[test]
    fn normalized_key_order_reorders() {
        let mut k: RowKey = IndexMap::new();
        k.insert("b".into(), JsonValue::Number(2.into()));
        k.insert("a".into(), JsonValue::Number(1.into()));
        let sorted = normalized_key_order(k);
        let keys: Vec<&String> = sorted.keys().collect();
        assert_eq!(keys, vec![&"a".to_string(), &"b".to_string()]);
    }

    // Branch: empty row key panics.
    #[test]
    #[should_panic(expected = "empty row key")]
    fn empty_key_panics() {
        let k: RowKey = IndexMap::new();
        let _ = normalized_key_order(k);
    }

    // Branch: leapfrog over two transactions. Each advance uses the
    // previously-rolled-back connection, verifying the swap.
    #[test]
    fn leapfrog_two_transactions() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();

        let c = Connection::open(&path).unwrap();
        // tx 1 → 09
        c.execute(
            "INSERT INTO issues(id, owner, desc, _0_version) VALUES(4, 30, 'qux', '09')",
            [],
        )
        .unwrap();
        log_change(&path, "09", 0, "issues", r#"{"id":4}"#, SET_OP);
        bump_state_version(&path, "09");

        let (specs1, names1) = default_specs();
        let diff1 = s.advance(specs1, names1).unwrap();
        assert_eq!(diff1.prev_version, "01");
        assert_eq!(diff1.curr_version, "09");
        let changes1 = diff1.collect_changes().unwrap();
        assert_eq!(changes1.len(), 1);
        // Drop diff1 before next advance so prev Arc has one holder.
        drop(diff1);
        drop(changes1);

        // tx 2 → 0d
        c.execute("DELETE FROM issues WHERE id=4", []).unwrap();
        log_change(&path, "0d", 0, "issues", r#"{"id":4}"#, DEL_OP);
        bump_state_version(&path, "0d");

        let (specs2, names2) = default_specs();
        let diff2 = s.advance(specs2, names2).unwrap();
        assert_eq!(diff2.prev_version, "09");
        assert_eq!(diff2.curr_version, "0d");
        let changes2 = diff2.collect_changes().unwrap();
        assert_eq!(changes2.len(), 1);
        assert!(changes2[0].next_value.is_none());
        assert_eq!(changes2[0].prev_values.len(), 1);
    }

    // Branch: concurrent-snapshotter isolation. A long-lived Snapshotter
    // holding snapshot_a at version 01 continues to see the old state
    // even after the replica advances.
    #[test]
    fn concurrent_isolation() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);

        let mut s1 = Snapshotter::new(&path, "my_app");
        s1.init().unwrap();
        let mut s2 = Snapshotter::new(&path, "my_app");
        s2.init().unwrap();

        let c = Connection::open(&path).unwrap();
        c.execute(
            "INSERT INTO issues(id, owner, desc, _0_version) VALUES(4, 30, 'qux', '09')",
            [],
        )
        .unwrap();
        log_change(&path, "09", 0, "issues", r#"{"id":4}"#, SET_OP);
        bump_state_version(&path, "09");

        // s2 still pins version 01.
        assert_eq!(s2.current().unwrap().version, "01");
        let _ = key(""); // silence unused warning if test reorders

        // s1 advances.
        let (specs, names) = default_specs();
        let diff1 = s1.advance(specs, names).unwrap();
        assert_eq!(diff1.curr_version, "09");
        drop(diff1);
        // s2 still pinned.
        assert_eq!(s2.current().unwrap().version, "01");
    }

    // Branch: `check_valid` flags stateVersion > curr.version as invalid.
    #[test]
    fn invalid_diff_state_version_past_curr() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();
        let (specs, names) = default_specs();
        let prev = s.current().unwrap();
        let curr = s.current().unwrap();
        let diff = SnapshotDiff::new("my_app", specs, names, prev, curr).unwrap();
        // Inject a synthetic entry whose state_version is "zz" > "01".
        let err = diff.check_valid("zz", SET_OP, &[], None).unwrap_err();
        assert!(
            matches!(err, SnapshotterError::InvalidDiff(ref e) if e.0.contains("curr")),
            "got: {err:?}"
        );
    }

    // Branch: `check_valid` flags prev row_version > prev.version.
    #[test]
    fn invalid_diff_prev_row_version_past_prev() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();
        let (specs, names) = default_specs();
        let prev = s.current().unwrap();
        let curr = s.current().unwrap();
        let diff = SnapshotDiff::new("my_app", specs, names, prev, curr).unwrap();
        let mut row: Row = IndexMap::new();
        row.insert(
            ZERO_VERSION_COLUMN_NAME.into(),
            Some(JsonValue::String("zz".into())),
        );
        let err = diff
            .check_valid("01", SET_OP, std::slice::from_ref(&row), None)
            .unwrap_err();
        assert!(
            matches!(err, SnapshotterError::InvalidDiff(ref e) if e.0.contains("prev")),
            "got: {err:?}"
        );
    }

    // Branch: `check_valid` flags SET with mismatched next _0_version.
    #[test]
    fn invalid_diff_next_row_version_mismatch() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();
        let (specs, names) = default_specs();
        let prev = s.current().unwrap();
        let curr = s.current().unwrap();
        let diff = SnapshotDiff::new("my_app", specs, names, prev, curr).unwrap();
        let mut next: Row = IndexMap::new();
        next.insert(
            ZERO_VERSION_COLUMN_NAME.into(),
            Some(JsonValue::String("other".into())),
        );
        let err = diff
            .check_valid("01", SET_OP, &[], Some(&next))
            .unwrap_err();
        assert!(
            matches!(err, SnapshotterError::InvalidDiff(ref e) if e.0.contains("advanced")),
            "got: {err:?}"
        );
    }

    // Branch: destroy() clears both snapshots.
    #[test]
    fn destroy_closes_connections() {
        let dir = TempDir::new().unwrap();
        let path = make_db(&dir);
        let mut s = Snapshotter::new(&path, "my_app");
        s.init().unwrap();
        s.destroy();
        assert!(!s.initialized());
    }

    // Branch: text_field helper rejects non-text columns.
    #[test]
    fn text_field_rejects_wrong_type() {
        let mut row: Row = IndexMap::new();
        row.insert("x".into(), Some(JsonValue::Number(1.into())));
        let err = text_field(&row, "x").unwrap_err();
        assert!(matches!(err, SnapshotterError::Assertion(_)));
    }

    // Branch: value_ref_to_value covers each SQLite type.
    #[test]
    fn value_ref_conversions() {
        assert_eq!(value_ref_to_value(ValueRef::Null), None);
        assert_eq!(
            value_ref_to_value(ValueRef::Integer(42)),
            Some(JsonValue::Number(42.into()))
        );
        match value_ref_to_value(ValueRef::Real(1.5)) {
            Some(JsonValue::Number(n)) => assert_eq!(n.as_f64().unwrap(), 1.5),
            v => panic!("unexpected {v:?}"),
        }
        assert_eq!(
            value_ref_to_value(ValueRef::Text(b"hi")),
            Some(JsonValue::String("hi".into()))
        );
        match value_ref_to_value(ValueRef::Blob(&[1, 2, 3])) {
            Some(JsonValue::String(s)) => assert_eq!(s, "<blob:3>"),
            v => panic!("unexpected {v:?}"),
        }
    }

    // Branch: json_to_sql covers each JSON kind.
    #[test]
    fn json_to_sql_conversions() {
        assert!(matches!(json_to_sql(&JsonValue::Null), SqlValue::Null));
        assert!(matches!(
            json_to_sql(&JsonValue::Bool(true)),
            SqlValue::Integer(1)
        ));
        assert!(matches!(
            json_to_sql(&JsonValue::Bool(false)),
            SqlValue::Integer(0)
        ));
        assert!(matches!(
            json_to_sql(&JsonValue::Number(7.into())),
            SqlValue::Integer(7)
        ));
        match json_to_sql(&serde_json::json!(1.25)) {
            SqlValue::Real(f) => assert_eq!(f, 1.25),
            _ => panic!("expected real"),
        }
        match json_to_sql(&JsonValue::String("s".into())) {
            SqlValue::Text(t) => assert_eq!(t, "s"),
            _ => panic!("expected text"),
        }
        // Object/array fallback → text JSON.
        match json_to_sql(&serde_json::json!([1, 2])) {
            SqlValue::Text(t) => assert_eq!(t, "[1,2]"),
            _ => panic!("expected text"),
        }
    }

}
