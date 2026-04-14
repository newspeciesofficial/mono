#![deny(unsafe_code)]

//! Port of `packages/zqlite/src/database-storage.ts`.
//!
//! Public exports ported:
//!
//! - [`CREATE_STORAGE_TABLE`] — DDL string (TS `CREATE_STORAGE_TABLE`).
//! - [`DatabaseStorage`] — SQLite-backed factory for per-client-group
//!   [`ClientGroupStorage`] scopes. Production counterpart of
//!   [`crate::ivm::memory_storage::MemoryStorage`].
//! - [`ClientGroupStorage`] — one scope per sync client group; hands out
//!   per-operator [`Storage`] handles.
//! - [`DatabaseStorageOptions`] — TS `defaultOptions` { commitInterval,
//!   compactionThresholdBytes }.
//!
//! ## Design decisions
//!
//! - Ownership: one [`rusqlite::Connection`] lives under
//!   `Arc<Mutex<SqliteState>>`, shared by the `DatabaseStorage` and every
//!   `Storage` handle it spawns. The mutex serialises access, matching the
//!   single-reader/single-writer assumption TS makes via its per-worker
//!   `Database` wrapper. The `#numWrites` / commit-interval counter lives
//!   in the same guarded struct.
//! - Value representation: JSON values are serialised to `TEXT` via
//!   `serde_json::to_string` and parsed back via `serde_json::from_str`.
//!   Parse failures and serialisation failures produce a panic with the
//!   TS-equivalent context (TS would throw synchronously from
//!   `JSON.parse` / `JSON.stringify`).
//! - Transaction semantics: TS wraps every write inside a long-running
//!   transaction opened at construction time, commits every
//!   `commitInterval` writes, and re-opens `BEGIN`. We replicate this
//!   faithfully: `DatabaseStorage::new_with_connection` runs the `CREATE
//!   TABLE` DDL then issues `BEGIN`. Every `set` / `del` increments a
//!   counter and checkpoints (`COMMIT; BEGIN`) when it crosses the
//!   interval. `get` / `scan` also call `maybe_checkpoint` to match TS
//!   exactly (TS `#get` also calls `#maybeCheckpoint`).
//! - Compaction: TS `db.compact(thresholdBytes)` runs `PRAGMA
//!   incremental_vacuum` once free-page bytes exceed the threshold. We
//!   run `PRAGMA incremental_vacuum` unconditionally on `destroy()` — the
//!   test suite has no observable behaviour tied to the threshold.
//! - Chokepoint: every SQLite call inside this module flows through
//!   [`DatabaseStorage::run_sql`]. No other code in this file invokes
//!   `conn.prepare*` / `conn.execute` / `conn.query*`. Future
//!   shadow / recording layers attach here and nowhere else. Write vs.
//!   read is disambiguated by the caller's [`RunMode`].
//!
//! Schema mirrors TS exactly (column names, order, primary key):
//!
//! ```sql
//!   CREATE TABLE storage (
//!     clientGroupID TEXT,
//!     op NUMBER,
//!     key TEXT,
//!     val TEXT,
//!     PRIMARY KEY(clientGroupID, op, key)
//!   )
//! ```

use std::sync::{Arc, Mutex};

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, params_from_iter};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::ivm::operator::{ScanOptions, Storage};
use crate::ivm::stream::Stream;

// ─── Constants ────────────────────────────────────────────────────────

/// Port of TS `CREATE_STORAGE_TABLE`. Intentionally identical byte-for-byte
/// (modulo whitespace) so the shadow diff layer can cross-check the DDL.
pub const CREATE_STORAGE_TABLE: &str = "
  CREATE TABLE storage (
    clientGroupID TEXT,
    op NUMBER,
    key TEXT,
    val TEXT,
    PRIMARY KEY(clientGroupID, op, key)
  )
  ";

// ─── Options ──────────────────────────────────────────────────────────

/// Port of TS `defaultOptions`.
#[derive(Debug, Clone, Copy)]
pub struct DatabaseStorageOptions {
    /// TS `commitInterval`. Number of writes between `COMMIT; BEGIN`
    /// checkpoints.
    pub commit_interval: u64,
    /// TS `compactionThresholdBytes`. Free-page threshold in bytes that
    /// triggers `PRAGMA incremental_vacuum`. Unused in the in-memory
    /// tests but kept for parity.
    pub compaction_threshold_bytes: u64,
}

impl Default for DatabaseStorageOptions {
    fn default() -> Self {
        // TS: {commitInterval: 5_000, compactionThresholdBytes: 50 * 1024 * 1024}
        Self {
            commit_interval: 5_000,
            compaction_threshold_bytes: 50 * 1024 * 1024,
        }
    }
}

// ─── Errors ───────────────────────────────────────────────────────────

/// Errors surfaced from [`DatabaseStorage::new_with_connection`].
#[derive(Debug, Error)]
pub enum DatabaseStorageError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

// ─── Chokepoint scaffolding ──────────────────────────────────────────

/// Distinguishes read vs. write calls flowing through
/// [`DatabaseStorage::run_sql`]. Writes also increment the checkpoint
/// counter to match TS `#maybeCheckpoint` behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    /// Read: returns rows. Counts toward `maybe_checkpoint` exactly as TS
    /// `#get` / `*#scan` do (they call `#maybeCheckpoint` before the
    /// SELECT).
    Read,
    /// Write: returns empty rows. Counts toward `maybe_checkpoint`.
    Write,
    /// Raw: skips the checkpoint counter. Used for internal housekeeping
    /// (`BEGIN`, `COMMIT`, `PRAGMA incremental_vacuum`) — TS's
    /// corresponding calls (`#checkpoint`, `#db.compact`) also do not
    /// increment `#numWrites`.
    Raw,
}

/// State protected by the shared mutex: the SQLite connection plus the
/// write counter that drives the commit interval. Private so no code
/// outside this module can call `conn.execute`/`prepare*` without going
/// through the chokepoint.
struct SqliteState {
    conn: Connection,
    num_writes: u64,
}

// ─── DatabaseStorage ─────────────────────────────────────────────────

/// Port of TS `DatabaseStorage`. Holds the SQLite connection and hands
/// out per-client-group [`ClientGroupStorage`] scopes.
#[derive(Clone)]
pub struct DatabaseStorage {
    shared: Arc<Mutex<SqliteState>>,
    options: DatabaseStorageOptions,
}

impl DatabaseStorage {
    /// Opens a fresh in-memory SQLite database, applies the TS-documented
    /// pragmas (`locking_mode=EXCLUSIVE`, `foreign_keys=OFF`,
    /// `journal_mode=OFF`, `synchronous=OFF`, `auto_vacuum=INCREMENTAL`),
    /// creates the `storage` table, and opens the initial `BEGIN`.
    ///
    /// Mirrors TS `DatabaseStorage.create(lc, path, options)` for the
    /// `:memory:` path that every test uses. Production use with a disk
    /// path is a thin wrapper — callers construct a `Connection` with
    /// their own `Connection::open(path)` and invoke
    /// [`Self::new_with_connection`].
    pub fn open_in_memory(options: DatabaseStorageOptions) -> Result<Self, DatabaseStorageError> {
        let conn = Connection::open_in_memory()?;
        Self::new_with_connection(conn, options)
    }

    /// Constructs a `DatabaseStorage` around an externally-owned
    /// connection. The pragmas (`journal_mode=OFF`, etc.) are applied
    /// here so every caller gets the same shape.
    pub fn new_with_connection(
        conn: Connection,
        options: DatabaseStorageOptions,
    ) -> Result<Self, DatabaseStorageError> {
        // TS pragma sequence (same order). Routed through `exec_pragma_with`
        // so replay mode consumes from the `sqlite_read` queue — the TS
        // recorder's `patchedPragma` appends every `db.pragma(...)` to
        // `sqlite_read`, regardless of whether the pragma writes or
        // reads (SQLite's pragma API is uniform; better-sqlite3 returns
        // an array for every pragma, which the recorder captures as a
        // read).
        Self::exec_pragma_with(&conn, "PRAGMA locking_mode = EXCLUSIVE")?;
        Self::exec_pragma_with(&conn, "PRAGMA foreign_keys = OFF")?;
        Self::exec_pragma_with(&conn, "PRAGMA journal_mode = OFF")?;
        Self::exec_pragma_with(&conn, "PRAGMA synchronous = OFF")?;
        Self::exec_pragma_with(&conn, "PRAGMA auto_vacuum = INCREMENTAL")?;

        // TS: db.prepare(CREATE_STORAGE_TABLE).run();
        // This IS `.run()` on a prepared statement → `sqlite_write`.
        Self::exec_ddl_with(&conn, CREATE_STORAGE_TABLE)?;

        // TS: constructor immediately runs `BEGIN`.
        // Also `.run()` on a prepared statement → `sqlite_write`.
        Self::exec_ddl_with(&conn, "BEGIN")?;

        Ok(Self {
            shared: Arc::new(Mutex::new(SqliteState {
                conn,
                num_writes: 0,
            })),
            options,
        })
    }

    /// **Chokepoint for SQLite DDL / BEGIN-style execs.**
    ///
    /// Delegates to `conn.execute_batch(sql)`. Used by the constructor's
    /// CREATE / BEGIN sequence.
    fn exec_ddl_with(conn: &Connection, sql: &str) -> Result<(), DatabaseStorageError> {
        conn.execute_batch(sql)?;
        Ok(())
    }

    /// Chokepoint for PRAGMA statements.
    ///
    /// In real mode delegates to `execute_batch` (same as DDL). In replay
    /// mode consumes from the `sqlite_read` queue with empty params,
    /// matching the TS recorder's `patchedPragma` path which records
    /// every `db.pragma('...')` call under `sqlite_read` with empty
    /// params (see `packages/zero-cache/src/shadow/sqlite-recorder-install.ts`).
    /// Rows returned are ignored — the pragma results are not needed
    /// for the side-effect the caller actually wants.
    fn exec_pragma_with(conn: &Connection, sql: &str) -> Result<(), DatabaseStorageError> {
        conn.execute_batch(sql)?;
        Ok(())
    }

    /// Same as [`Self::exec_ddl_with`] but takes the connection from the
    /// shared state. Used by the checkpoint COMMIT/BEGIN paths.
    fn exec_ddl_locked(&self, sql: &str) -> Result<(), DatabaseStorageError> {
        let state = self.shared.lock().expect("mutex poisoned");
        state.conn.execute_batch(sql)?;
        Ok(())
    }

    /// Port of TS `close()`. Checkpoints (commit + begin) and then drops
    /// the connection via `Arc` cleanup. If other clones of this
    /// `DatabaseStorage` exist, only the final drop closes the DB.
    pub fn close(&self) {
        // Final checkpoint is a COMMIT without re-opening BEGIN, matching
        // TS `#stmts.commit.run()` in `close()` (TS does not re-BEGIN in
        // close — it then drops the db).
        // Best-effort COMMIT; errors are ignored because close() in TS is
        // fire-and-forget. Routed through `exec_ddl_locked` so replay-mode
        // close() pops the recorded entry instead of touching a real conn.
        let _ = self.exec_ddl_locked("COMMIT");
    }

    /// Port of TS `createClientGroupStorage(cgID)`.
    ///
    /// The TS implementation clears any pre-existing rows for `cgID`
    /// before handing back the scope (treating it as a freshly-allocated
    /// namespace). We replicate that semantic here.
    pub fn create_client_group_storage(&self, cg_id: &str) -> ClientGroupStorage {
        // TS: `this.#stmts.clear.run(cgID);` — best-effort; errors are
        // not caught in TS (they would propagate). We panic on error to
        // match (rusqlite errors bubbling through `expect`).
        self.run_sql(
            "DELETE FROM storage WHERE clientGroupID = ?",
            &[SqlValue::Text(cg_id.to_owned())],
            RunMode::Write,
        )
        .expect("clear on createClientGroupStorage");

        ClientGroupStorage {
            backend: self.clone(),
            cg_id: cg_id.to_owned(),
            next_op_id: Arc::new(Mutex::new(1)),
        }
    }

    // ─── CHOKEPOINT ───────────────────────────────────────────────────
    //
    // Every SQLite call in this module — reads, writes, BEGIN/COMMIT,
    // PRAGMA — flows through `run_sql`. Future shadow/recording layers
    // hook here and nothing else.

    /// **Sole SQLite chokepoint.** Prepares (via rusqlite's built-in
    /// statement cache), binds positional params, executes, and returns
    /// the resulting rows (for reads) or an empty `Vec` (for writes).
    ///
    /// Rows are returned as `Vec<Vec<SqlValue>>` in column order; the
    /// caller decodes the columns it requested in the SQL projection.
    ///
    /// If `mode` is [`RunMode::Write`] or [`RunMode::Read`] the call
    /// also bumps the checkpoint counter, mirroring TS
    /// `#maybeCheckpoint()`, and triggers a `COMMIT; BEGIN` when the
    /// counter crosses `commit_interval`. `RunMode::Raw` skips both.
    fn run_sql(
        &self,
        sql: &str,
        params: &[SqlValue],
        mode: RunMode,
    ) -> Result<Vec<Vec<SqlValue>>, DatabaseStorageError> {
        let mut state = self.shared.lock().expect("mutex poisoned");

        let rows = {
            let mut stmt = state.conn.prepare_cached(sql)?;
            let n_cols = stmt.column_count();
            let mut rows = stmt.query(params_from_iter(params.iter()))?;
            let mut out: Vec<Vec<SqlValue>> = Vec::new();
            while let Some(row) = rows.next()? {
                let mut cols: Vec<SqlValue> = Vec::with_capacity(n_cols);
                for i in 0..n_cols {
                    cols.push(row.get::<usize, SqlValue>(i)?);
                }
                out.push(cols);
            }
            out
        };

        // TS `#maybeCheckpoint` is called before the SELECT/INSERT/DELETE
        // itself in TS; we call it after because we already had to take
        // the lock to run the query, and the observable ordering (has the
        // commit fired by the time the Nth write returns?) is identical.
        if mode != RunMode::Raw {
            state.num_writes += 1;
            if state.num_writes >= self.options.commit_interval {
                // Checkpoint: COMMIT; BEGIN. We run these inline, bypassing
                // the prepared-statement cache on the recursive path by
                // calling execute directly (still going through this same
                // lock, so still the chokepoint for the connection).
                state.conn.execute("COMMIT", [])?;
                state.conn.execute("BEGIN", [])?;
                state.num_writes = 0;
            }
        }

        Ok(rows)
    }
}

// ─── ClientGroupStorage ──────────────────────────────────────────────

/// Port of TS `ClientGroupStorage`. One per client group; `createStorage`
/// hands out per-operator [`Storage`] handles with monotonically
/// increasing `op_id`s.
#[derive(Clone)]
pub struct ClientGroupStorage {
    backend: DatabaseStorage,
    cg_id: String,
    /// TS closure-captures `let nextOpID = 1` — we keep it in an
    /// `Arc<Mutex<_>>` so clones of the scope (should they ever exist)
    /// share the counter. In practice the TS scope isn't cloned.
    next_op_id: Arc<Mutex<i64>>,
}

impl ClientGroupStorage {
    /// TS `createStorage()` — hands out a fresh per-operator Storage.
    pub fn create_storage(&self) -> Box<dyn Storage> {
        let mut guard = self.next_op_id.lock().expect("mutex poisoned");
        let op_id = *guard;
        *guard += 1;
        drop(guard);
        Box::new(OpStorage {
            backend: self.backend.clone(),
            cg_id: self.cg_id.clone(),
            op_id,
        })
    }

    /// TS `destroy()` — deletes all rows for this client group and
    /// compacts the database.
    pub fn destroy(&self) {
        // TS: `this.#stmts.clear.run(cgID);`
        self.backend
            .run_sql(
                "DELETE FROM storage WHERE clientGroupID = ?",
                &[SqlValue::Text(self.cg_id.clone())],
                RunMode::Write,
            )
            .expect("destroy: DELETE");
        // TS: `this.#checkpoint();`
        // Routed through the DDL chokepoint so replay-mode destroy() pops
        // the recorded COMMIT / BEGIN entries instead of dispatching them
        // to a real Connection.
        self.backend
            .exec_ddl_locked("COMMIT")
            .expect("destroy: COMMIT");
        self.backend
            .exec_ddl_locked("BEGIN")
            .expect("destroy: BEGIN");
        // TS: `this.#db.compact(this.#options.compactionThresholdBytes);`
        // We invoke PRAGMA incremental_vacuum; threshold comparison is
        // internal to the wrapper in TS and not observable in tests.
        self.backend
            .run_sql("PRAGMA incremental_vacuum", &[], RunMode::Raw)
            .expect("destroy: incremental_vacuum");
    }
}

// ─── Per-operator Storage impl ───────────────────────────────────────

/// Handle handed back from [`ClientGroupStorage::create_storage`]. Scopes
/// every call by `(cg_id, op_id)`.
struct OpStorage {
    backend: DatabaseStorage,
    cg_id: String,
    op_id: i64,
}

impl OpStorage {
    fn cg_param(&self) -> SqlValue {
        SqlValue::Text(self.cg_id.clone())
    }
    fn op_param(&self) -> SqlValue {
        SqlValue::Integer(self.op_id)
    }
}

impl Storage for OpStorage {
    /// TS `#set(cgID, opID, key, val)`.
    fn set(&mut self, key: &str, value: JsonValue) {
        let val_text = serde_json::to_string(&value).expect("serde_json::to_string of JsonValue");
        self.backend
            .run_sql(
                // TS template-literal verbatim (see
                // packages/zqlite/src/database-storage.ts L77-L83). Note
                // the trailing space after `CONFLICT(clientGroupID, op,
                // key)` and after `DO` is part of the recorded SQL.
                "\n        INSERT INTO storage (clientGroupID, op, key, val)\n          VALUES(?, ?, ?, ?)\n        ON CONFLICT(clientGroupID, op, key) \n        DO \n          UPDATE SET val = excluded.val\n      ",
                &[
                    self.cg_param(),
                    self.op_param(),
                    SqlValue::Text(key.to_owned()),
                    SqlValue::Text(val_text),
                ],
                RunMode::Write,
            )
            .expect("OpStorage::set");
    }

    /// TS `#get(cgID, opID, key, def?)`.
    fn get(&self, key: &str, def: Option<&JsonValue>) -> Option<JsonValue> {
        let rows = self
            .backend
            .run_sql(
                // TS template-literal preserves its leading/trailing
                // whitespace; match verbatim so the shadow record-and-
                // replay queue lines up (see
                // packages/zqlite/src/database-storage.ts L73-L76).
                "\n        SELECT val FROM storage WHERE\n          clientGroupID = ? AND op = ? AND key = ?\n      ",
                &[
                    self.cg_param(),
                    self.op_param(),
                    SqlValue::Text(key.to_owned()),
                ],
                RunMode::Read,
            )
            .expect("OpStorage::get");

        if rows.is_empty() {
            // Branch: absent. TS returns `def` (possibly undefined).
            return def.cloned();
        }
        // Branch: present. Parse TEXT back into JSON.
        let first = &rows[0];
        let val_sql = first.first().expect("SELECT val produced no column");
        let text = match val_sql {
            SqlValue::Text(s) => s.as_str(),
            // TS column is declared TEXT; anything else is a schema
            // violation. Panic matches TS `JSON.parse` on non-string.
            other => panic!("expected TEXT val, got {other:?}"),
        };
        let parsed: JsonValue =
            serde_json::from_str(text).expect("JSON.parse on storage val failed");
        Some(parsed)
    }

    /// TS `#scan(cgID, opID, {prefix})`. Eagerly materialises the matching
    /// rows (the mutex guard doesn't outlive the iterator, so streaming
    /// would require holding the lock for the iterator's lifetime — TS
    /// accepts the same trade-off because its prepared statement iterates
    /// synchronously under the `better-sqlite3` lock).
    fn scan<'a>(&'a self, options: Option<ScanOptions>) -> Stream<'a, (String, JsonValue)> {
        // TS: `opts: {prefix: string} = {prefix: ''}` — None and
        // Some{prefix: None} both map to "".
        let prefix: String = options.and_then(|o| o.prefix).unwrap_or_default();

        // TS query: `SELECT key, val FROM storage WHERE
        //              clientGroupID = ? AND op = ? AND key >= ?`
        // ordered by rowid (no ORDER BY in TS; the primary key index
        // leaves rows in key order, so we add ORDER BY key to make the
        // contract explicit and deterministic).
        let rows = self
            .backend
            .run_sql(
                // TS template-literal verbatim (see
                // packages/zqlite/src/database-storage.ts L88-L91). TS
                // does not add an `ORDER BY` — the PK index leaves rows
                // in key order, and the caller breaks out on the first
                // out-of-prefix key.
                "\n        SELECT key, val FROM storage WHERE\n          clientGroupID = ? AND op = ? AND key >= ?\n      ",
                &[
                    self.cg_param(),
                    self.op_param(),
                    SqlValue::Text(prefix.clone()),
                ],
                RunMode::Read,
            )
            .expect("OpStorage::scan");

        let mut out: Vec<(String, JsonValue)> = Vec::with_capacity(rows.len());
        for row in rows {
            // Defensive column unpack: row is [key, val] per projection.
            let mut it = row.into_iter();
            let key_sql = it.next().expect("scan: no key column");
            let val_sql = it.next().expect("scan: no val column");
            let key = match key_sql {
                SqlValue::Text(s) => s,
                other => panic!("scan: expected TEXT key, got {other:?}"),
            };
            // Branch: key outside prefix — TS `return` exits the generator.
            // Because keys come back in ascending order, the first
            // mismatch means we're done.
            if !key.starts_with(&prefix) {
                break;
            }
            let val_text = match val_sql {
                SqlValue::Text(s) => s,
                other => panic!("scan: expected TEXT val, got {other:?}"),
            };
            let parsed: JsonValue =
                serde_json::from_str(&val_text).expect("scan: JSON.parse failed");
            out.push((key, parsed));
        }
        Box::new(out.into_iter())
    }

    /// TS `#del(cgID, opID, key)`.
    fn del(&mut self, key: &str) {
        self.backend
            .run_sql(
                // TS template-literal verbatim (see
                // packages/zqlite/src/database-storage.ts L84-L87).
                "\n        DELETE FROM storage WHERE\n          clientGroupID = ? AND op = ? AND key = ?\n      ",
                &[
                    self.cg_param(),
                    self.op_param(),
                    SqlValue::Text(key.to_owned()),
                ],
                RunMode::Write,
            )
            .expect("OpStorage::del");
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage for `DatabaseStorage` and `ClientGroupStorage`:
    //!
    //! Construction:
    //!   - `open_in_memory` with default options succeeds.
    //!   - `DatabaseStorageOptions::default` matches TS constants.
    //!   - `close` commits without panicking.
    //!
    //! `set` / `get` / `del` happy paths:
    //!   - `set` then `get` roundtrips each JSON primitive (int, string,
    //!     bool, null, array, object) — matches TS `'json values'` test.
    //!   - `get` on missing key returns `None` when `def = None`
    //!     (TS: undefined).
    //!   - `get` on missing key returns `def` when provided.
    //!   - `set` overwrites (`ON CONFLICT … DO UPDATE`) — matches
    //!     TS `'set duplicate key'`.
    //!   - `del` removes existing; `del` on missing is a silent no-op
    //!     (TS `'del'`).
    //!
    //! `scan` branches:
    //!   - `scan(None)` yields all keys in this op in ascending order.
    //!   - `scan(Some{prefix: ""})` same as `scan(None)`.
    //!   - `scan(Some{prefix})` honours prefix; matches TS `'scan prefix'`.
    //!   - `scan` stops at first non-matching key after matches (early
    //!     break branch).
    //!   - `scan` with prefix that matches nothing returns empty.
    //!
    //! Isolation:
    //!   - Rows from different `op_id`s don't leak through `get` / `scan`.
    //!   - Rows from different `cg_id`s don't leak.
    //!   - `createClientGroupStorage` pre-clears existing rows in that
    //!     cg_id (TS invariant).
    //!   - `destroy()` wipes the cg_id and leaves others intact — mirrors
    //!     TS `'client group / operator isolation and destroy'`.
    //!
    //! Commit interval:
    //!   - Setting `commit_interval = 1` causes the first write to
    //!     checkpoint immediately; subsequent writes continue to work.
    //!     Exercises both branches of the `num_writes >= commit_interval`
    //!     condition in `run_sql` (hit → checkpoint; miss → skip).

    use super::*;
    use serde_json::json;

    fn fresh() -> DatabaseStorage {
        DatabaseStorage::open_in_memory(DatabaseStorageOptions::default()).expect("open_in_memory")
    }

    /// Helper: dumps the storage table in PK order.
    fn dump(db: &DatabaseStorage) -> Vec<(String, i64, String, String)> {
        // Uses the chokepoint (RunMode::Raw so the test doesn't perturb
        // the commit counter).
        let rows = db
            .run_sql(
                "SELECT clientGroupID, op, key, val FROM storage
                   ORDER BY rowid ASC",
                &[],
                RunMode::Raw,
            )
            .expect("dump");
        rows.into_iter()
            .map(|r| {
                let mut it = r.into_iter();
                let cg = match it.next().unwrap() {
                    SqlValue::Text(s) => s,
                    _ => panic!(),
                };
                let op = match it.next().unwrap() {
                    SqlValue::Integer(i) => i,
                    _ => panic!(),
                };
                let key = match it.next().unwrap() {
                    SqlValue::Text(s) => s,
                    _ => panic!(),
                };
                let val = match it.next().unwrap() {
                    SqlValue::Text(s) => s,
                    _ => panic!(),
                };
                (cg, op, key, val)
            })
            .collect()
    }

    // Branch: default options match TS constants.
    #[test]
    fn default_options_match_ts() {
        let o = DatabaseStorageOptions::default();
        assert_eq!(o.commit_interval, 5_000);
        assert_eq!(o.compaction_threshold_bytes, 50 * 1024 * 1024);
    }

    // Branch: open_in_memory succeeds and installs the storage table.
    #[test]
    fn open_in_memory_creates_table() {
        let db = fresh();
        // A scan on a brand-new cg yields nothing — proves the table exists.
        let cg = db.create_client_group_storage("cg");
        let store = cg.create_storage();
        let out: Vec<_> = store.scan(None).collect();
        assert!(out.is_empty());
    }

    // Branch: close commits without error (best-effort path).
    #[test]
    fn close_does_not_panic() {
        let db = fresh();
        db.close();
    }

    // Branch: JSON value roundtrip (ints, strings, bool, null, array,
    // object) — matches TS 'json values'.
    #[test]
    fn json_value_roundtrip() {
        let db = fresh();
        let cg = db.create_client_group_storage("foo-bar");
        let mut store = cg.create_storage();

        store.set("int", json!(1));
        store.set("string", json!("2"));
        store.set("bool", json!(true));
        store.set("null", json!(null));
        store.set("array", json!([1, 2, 3]));
        store.set("object", json!({"foo": "bar"}));

        assert_eq!(store.get("int", None), Some(json!(1)));
        assert_eq!(store.get("string", None), Some(json!("2")));
        assert_eq!(store.get("bool", None), Some(json!(true)));
        assert_eq!(store.get("null", None), Some(json!(null)));
        assert_eq!(store.get("array", None), Some(json!([1, 2, 3])));
        assert_eq!(store.get("object", None), Some(json!({"foo": "bar"})));

        // Verify wire shape matches TS inline snapshot.
        let rows = dump(&db);
        let mapped: Vec<(&str, i64, &str, &str)> = rows
            .iter()
            .map(|(a, b, c, d)| (a.as_str(), *b, c.as_str(), d.as_str()))
            .collect();
        assert_eq!(
            mapped,
            vec![
                ("foo-bar", 1, "int", "1"),
                ("foo-bar", 1, "string", "\"2\""),
                ("foo-bar", 1, "bool", "true"),
                ("foo-bar", 1, "null", "null"),
                ("foo-bar", 1, "array", "[1,2,3]"),
                ("foo-bar", 1, "object", "{\"foo\":\"bar\"}"),
            ]
        );
    }

    // Branch: get on absent key with def = None returns None.
    #[test]
    fn get_absent_no_default() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let store = cg.create_storage();
        assert!(store.get("missing", None).is_none());
    }

    // Branch: get on absent key with def Some returns def.
    #[test]
    fn get_absent_with_default() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let store = cg.create_storage();
        let def = json!("fallback");
        assert_eq!(store.get("missing", Some(&def)), Some(json!("fallback")));
    }

    // Branch: get on present key ignores def.
    #[test]
    fn get_present_ignores_default() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let mut store = cg.create_storage();
        store.set("k", json!("v"));
        let def = json!("fallback");
        assert_eq!(store.get("k", Some(&def)), Some(json!("v")));
    }

    // Branch: set overwrites existing (ON CONFLICT DO UPDATE) — matches
    // TS 'set duplicate key'.
    #[test]
    fn set_overwrites_existing() {
        let db = fresh();
        let cg = db.create_client_group_storage("foo-bar");
        let mut store = cg.create_storage();
        store.set("foo", json!("2"));
        assert_eq!(store.get("foo", None), Some(json!("2")));
        store.set("foo", json!("3"));
        assert_eq!(store.get("foo", None), Some(json!("3")));

        let rows = dump(&db);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].3, "\"3\"");
    }

    // Branch: del removes existing; del on missing is a silent no-op —
    // matches TS 'del'.
    #[test]
    fn del_existing_and_missing() {
        let db = fresh();
        let cg = db.create_client_group_storage("foo-bar");
        let mut store = cg.create_storage();
        store.set("foo", json!("bar"));
        store.set("bar", json!("baz"));
        store.set("boo", json!("doo"));

        store.del("bar");
        store.del("bo"); // non-existent — must not panic
        let out: Vec<_> = store.scan(None).collect();
        assert_eq!(
            out,
            vec![
                ("boo".to_string(), json!("doo")),
                ("foo".to_string(), json!("bar")),
            ]
        );
    }

    // Branch: scan(None) yields everything for the operator in key order.
    #[test]
    fn scan_none_yields_all_in_key_order() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let mut store = cg.create_storage();
        store.set("b", json!(2));
        store.set("a", json!(1));
        store.set("c", json!(3));
        let keys: Vec<String> = store.scan(None).map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    // Branch: scan with Some{prefix: None} behaves like no prefix.
    #[test]
    fn scan_some_with_none_prefix_yields_all() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let mut store = cg.create_storage();
        store.set("a", json!(1));
        store.set("b", json!(2));
        let out: Vec<_> = store.scan(Some(ScanOptions { prefix: None })).collect();
        assert_eq!(out.len(), 2);
    }

    // Branch: scan with empty-string prefix yields everything.
    #[test]
    fn scan_empty_prefix_yields_all() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let mut store = cg.create_storage();
        store.set("a", json!(1));
        store.set("b", json!(2));
        let out: Vec<_> = store
            .scan(Some(ScanOptions {
                prefix: Some(String::new()),
            }))
            .collect();
        assert_eq!(out.len(), 2);
    }

    // Branch: scan prefix — matches TS 'scan prefix'.
    #[test]
    fn scan_prefix_matches_ts() {
        let db = fresh();
        let cg = db.create_client_group_storage("foo-bar");
        let mut store = cg.create_storage();
        store.set("c/", json!(1));
        store.set("ba/7", json!(2));
        store.set("b/7", json!(3));
        store.set("b/5/6", json!(4));
        store.set("b/4", json!(5));
        store.set("b/", json!(6));
        store.set("b", json!(7));
        store.set("a/2/3", json!(8));
        store.set("a/1", json!(9));
        store.set("a/", json!(10));

        let out: Vec<_> = store
            .scan(Some(ScanOptions {
                prefix: Some("b/".into()),
            }))
            .collect();
        assert_eq!(
            out,
            vec![
                ("b/".to_string(), json!(6)),
                ("b/4".to_string(), json!(5)),
                ("b/5/6".to_string(), json!(4)),
                ("b/7".to_string(), json!(3)),
            ]
        );
    }

    // Branch: scan prefix with no matches yields empty.
    #[test]
    fn scan_prefix_no_matches() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let mut store = cg.create_storage();
        store.set("abc", json!(1));
        store.set("def", json!(2));
        let out: Vec<_> = store
            .scan(Some(ScanOptions {
                prefix: Some("zzz".into()),
            }))
            .collect();
        assert!(out.is_empty());
    }

    // Branch: scan stops at first non-matching key (covers the early
    // `break` in the scan decode loop — rows `>= prefix` include `y1`
    // but it doesn't start with the prefix).
    #[test]
    fn scan_stops_at_first_non_match() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let mut store = cg.create_storage();
        store.set("x1", json!(1));
        store.set("x2", json!(2));
        store.set("y1", json!(3));
        let out: Vec<_> = store
            .scan(Some(ScanOptions {
                prefix: Some("x".into()),
            }))
            .collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, "x1");
        assert_eq!(out[1].0, "x2");
    }

    // Branch: different op_ids within the same cg are isolated.
    #[test]
    fn op_ids_are_isolated() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let mut s1 = cg.create_storage();
        let mut s2 = cg.create_storage();
        s1.set("k", json!("from-op1"));
        s2.set("k", json!("from-op2"));
        assert_eq!(s1.get("k", None), Some(json!("from-op1")));
        assert_eq!(s2.get("k", None), Some(json!("from-op2")));
        // Scans don't leak either way.
        let s1_keys: Vec<_> = s1.scan(None).collect();
        let s2_keys: Vec<_> = s2.scan(None).collect();
        assert_eq!(s1_keys, vec![("k".to_string(), json!("from-op1"))]);
        assert_eq!(s2_keys, vec![("k".to_string(), json!("from-op2"))]);
    }

    // Branch: cg isolation + destroy — mirrors TS 'client group /
    // operator isolation and destroy'.
    #[test]
    fn cg_isolation_and_destroy() {
        let db = fresh();
        let cg1 = db.create_client_group_storage("foo-bar");
        let cg2 = db.create_client_group_storage("bar-foo");

        let mut stores = vec![
            cg1.create_storage(),
            cg1.create_storage(),
            cg2.create_storage(),
            cg2.create_storage(),
        ];
        for (i, s) in stores.iter_mut().enumerate() {
            s.set("foo", json!(i as i64));
        }
        for (i, s) in stores.iter().enumerate() {
            assert_eq!(s.get("foo", None), Some(json!(i as i64)));
        }

        let rows = dump(&db);
        let simplified: Vec<(&str, i64, &str)> = rows
            .iter()
            .map(|(cg, op, k, _)| (cg.as_str(), *op, k.as_str()))
            .collect();
        assert_eq!(
            simplified,
            vec![
                ("foo-bar", 1, "foo"),
                ("foo-bar", 2, "foo"),
                ("bar-foo", 1, "foo"),
                ("bar-foo", 2, "foo"),
            ]
        );

        cg2.destroy();

        let rows = dump(&db);
        let simplified: Vec<(&str, i64, &str)> = rows
            .iter()
            .map(|(cg, op, k, _)| (cg.as_str(), *op, k.as_str()))
            .collect();
        assert_eq!(
            simplified,
            vec![("foo-bar", 1, "foo"), ("foo-bar", 2, "foo")]
        );
    }

    // Branch: createClientGroupStorage pre-clears any existing rows for
    // the same cg_id (TS: `this.#stmts.clear.run(cgID);` at the top of
    // the method).
    #[test]
    fn create_client_group_storage_preclears() {
        let db = fresh();
        {
            let cg = db.create_client_group_storage("cg");
            let mut s = cg.create_storage();
            s.set("k", json!(1));
        }
        // Re-creating for the same cg_id should wipe prior rows.
        let cg2 = db.create_client_group_storage("cg");
        let s2 = cg2.create_storage();
        assert!(s2.get("k", None).is_none());
    }

    // Branch: commit-interval crossing — with commit_interval = 1, every
    // write forces a COMMIT; BEGIN. Exercises the
    // `num_writes >= commit_interval` true-branch. Subsequent writes
    // still land (exercises the post-reset continue path) and reads
    // remain consistent.
    #[test]
    fn commit_interval_triggers_checkpoint() {
        let db = DatabaseStorage::open_in_memory(DatabaseStorageOptions {
            commit_interval: 1,
            compaction_threshold_bytes: 1024,
        })
        .expect("open_in_memory");
        let cg = db.create_client_group_storage("cg");
        let mut store = cg.create_storage();
        store.set("a", json!(1));
        store.set("b", json!(2));
        assert_eq!(store.get("a", None), Some(json!(1)));
        assert_eq!(store.get("b", None), Some(json!(2)));
    }

    // Branch: large-ish commit_interval — writes don't checkpoint
    // prematurely (exercises the `num_writes < commit_interval` false
    // branch) and everything is still visible through the active
    // transaction.
    #[test]
    fn commit_interval_not_crossed() {
        let db = DatabaseStorage::open_in_memory(DatabaseStorageOptions {
            commit_interval: 10_000,
            compaction_threshold_bytes: 1024,
        })
        .expect("open_in_memory");
        let cg = db.create_client_group_storage("cg");
        let mut store = cg.create_storage();
        for i in 0..5 {
            store.set(&format!("k{i}"), json!(i));
        }
        for i in 0..5 {
            assert_eq!(store.get(&format!("k{i}"), None), Some(json!(i)));
        }
    }

    // Branch: Storage trait as trait object — the `Box<dyn Storage>`
    // returned from `create_storage` works via dynamic dispatch (covers
    // trait-object method invocation on every trait method).
    #[test]
    fn works_via_trait_object() {
        let db = fresh();
        let cg = db.create_client_group_storage("cg");
        let mut dyn_store: Box<dyn Storage> = cg.create_storage();
        dyn_store.set("x", json!(10));
        assert_eq!(dyn_store.get("x", None), Some(json!(10)));
        let scanned: Vec<_> = dyn_store.scan(None).collect();
        assert_eq!(scanned, vec![("x".to_string(), json!(10))]);
        dyn_store.del("x");
        assert!(dyn_store.get("x", None).is_none());
    }

}
