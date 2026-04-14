//! Ephemeral operator storage backed by SQLite.
//!
//! This is a Rust port of `DatabaseStorage` from
//! `packages/zqlite/src/database-storage.ts`. It provides temporary storage
//! for IVM operator state, using a SQLite database that can spill to disk to
//! avoid consuming too much memory.
//!
//! Key design decisions (matching the TS implementation):
//! - `journal_mode = OFF`, `synchronous = OFF`, `locking_mode = EXCLUSIVE`
//!   because durability is irrelevant (this is scratch space).
//! - Commits every `commit_interval` writes to amortise transaction overhead.
//! - Partitioned by `clientGroupID` so each client group can be independently
//!   created and destroyed.

use rusqlite::Connection;
use serde_json::Value as JsonValue;
use tracing::debug;

/// SQL to create the storage table.
pub const CREATE_STORAGE_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS storage (
        clientGroupID TEXT,
        op NUMBER,
        key TEXT,
        val TEXT,
        PRIMARY KEY(clientGroupID, op, key)
    )
";

/// Default number of writes between checkpoints (commits).
const DEFAULT_COMMIT_INTERVAL: usize = 5_000;

/// Ephemeral SQLite-backed storage for IVM operator state.
pub struct OperatorStorage {
    conn: Connection,
    num_writes: usize,
    commit_interval: usize,
}

impl OperatorStorage {
    /// Create a new operator storage database at `path`.
    ///
    /// The database is configured for maximum throughput at the expense of
    /// durability (which is irrelevant for ephemeral operator state).
    pub fn create(path: &str) -> anyhow::Result<Self> {
        Self::create_with_interval(path, DEFAULT_COMMIT_INTERVAL)
    }

    /// Create with a custom commit interval (useful for testing).
    pub fn create_with_interval(path: &str, commit_interval: usize) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;

        // Match the TS `DatabaseStorage.create()` pragmas exactly.
        conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        conn.pragma_update(None, "foreign_keys", "OFF")?;
        conn.pragma_update(None, "journal_mode", "OFF")?;
        conn.pragma_update(None, "synchronous", "OFF")?;
        conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;

        conn.execute_batch(CREATE_STORAGE_TABLE)?;
        conn.execute_batch("BEGIN")?;

        debug!(path, "created operator storage");

        Ok(Self {
            conn,
            num_writes: 0,
            commit_interval,
        })
    }

    /// Get a value for a given (clientGroupID, opID, key) tuple.
    pub fn get(&mut self, cg_id: &str, op_id: i64, key: &str) -> anyhow::Result<Option<JsonValue>> {
        self.maybe_checkpoint()?;

        let mut stmt = self.conn.prepare_cached(
            "SELECT val FROM storage WHERE clientGroupID = ?1 AND op = ?2 AND key = ?3",
        )?;

        let result = stmt.query_row(rusqlite::params![cg_id, op_id, key], |row| {
            let val_str: String = row.get(0)?;
            Ok(val_str)
        });

        match result {
            Ok(val_str) => {
                let parsed: JsonValue = serde_json::from_str(&val_str)?;
                Ok(Some(parsed))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get a value or return a default.
    pub fn get_or(
        &mut self,
        cg_id: &str,
        op_id: i64,
        key: &str,
        default: JsonValue,
    ) -> anyhow::Result<JsonValue> {
        Ok(self.get(cg_id, op_id, key)?.unwrap_or(default))
    }

    /// Set a value (upsert).
    pub fn set(
        &mut self,
        cg_id: &str,
        op_id: i64,
        key: &str,
        val: &JsonValue,
    ) -> anyhow::Result<()> {
        self.maybe_checkpoint()?;

        let val_str = serde_json::to_string(val)?;
        self.conn.execute(
            "INSERT INTO storage (clientGroupID, op, key, val) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(clientGroupID, op, key) DO UPDATE SET val = excluded.val",
            rusqlite::params![cg_id, op_id, key, val_str],
        )?;
        Ok(())
    }

    /// Delete a key.
    pub fn del(&mut self, cg_id: &str, op_id: i64, key: &str) -> anyhow::Result<()> {
        self.maybe_checkpoint()?;

        self.conn.execute(
            "DELETE FROM storage WHERE clientGroupID = ?1 AND op = ?2 AND key = ?3",
            rusqlite::params![cg_id, op_id, key],
        )?;
        Ok(())
    }

    /// Scan all entries for a (clientGroupID, opID) with keys starting at `prefix`.
    pub fn scan(
        &mut self,
        cg_id: &str,
        op_id: i64,
        prefix: &str,
    ) -> anyhow::Result<Vec<(String, JsonValue)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT key, val FROM storage WHERE clientGroupID = ?1 AND op = ?2 AND key >= ?3",
        )?;

        let rows = stmt.query_map(rusqlite::params![cg_id, op_id, prefix], |row| {
            let key: String = row.get(0)?;
            let val: String = row.get(1)?;
            Ok((key, val))
        })?;

        let mut result = Vec::new();
        for row in rows {
            let (key, val_str) = row?;
            if !key.starts_with(prefix) {
                break;
            }
            let parsed: JsonValue = serde_json::from_str(&val_str)?;
            result.push((key, parsed));
        }
        Ok(result)
    }

    /// Delete all storage for a client group, then compact.
    pub fn clear_client_group(&mut self, cg_id: &str) -> anyhow::Result<()> {
        self.conn.execute(
            "DELETE FROM storage WHERE clientGroupID = ?1",
            rusqlite::params![cg_id],
        )?;
        self.checkpoint()?;
        Ok(())
    }

    /// Close the underlying database connection.
    pub fn close(mut self) -> anyhow::Result<()> {
        self.checkpoint()?;
        self.conn
            .close()
            .map_err(|(_conn, e)| anyhow::anyhow!("close: {e}"))?;
        Ok(())
    }

    /// Commit if we have reached the commit interval.
    fn maybe_checkpoint(&mut self) -> anyhow::Result<()> {
        self.num_writes += 1;
        if self.num_writes >= self.commit_interval {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Force a commit/begin cycle.
    fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.conn.execute_batch("COMMIT; BEGIN")?;
        self.num_writes = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::NamedTempFile;

    fn tmp_path() -> (NamedTempFile, String) {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        (tmp, path)
    }

    #[test]
    fn test_set_and_get() {
        let (_tmp, path) = tmp_path();
        let mut storage = OperatorStorage::create(&path).unwrap();

        storage.set("cg1", 1, "key1", &json!(42)).unwrap();
        let val = storage.get("cg1", 1, "key1").unwrap();
        assert_eq!(val, Some(json!(42)));
    }

    #[test]
    fn test_get_missing_key() {
        let (_tmp, path) = tmp_path();
        let mut storage = OperatorStorage::create(&path).unwrap();

        let val = storage.get("cg1", 1, "missing").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_get_or_default() {
        let (_tmp, path) = tmp_path();
        let mut storage = OperatorStorage::create(&path).unwrap();

        let val = storage.get_or("cg1", 1, "key", json!(0)).unwrap();
        assert_eq!(val, json!(0));

        storage.set("cg1", 1, "key", &json!(99)).unwrap();
        let val = storage.get_or("cg1", 1, "key", json!(0)).unwrap();
        assert_eq!(val, json!(99));
    }

    #[test]
    fn test_upsert() {
        let (_tmp, path) = tmp_path();
        let mut storage = OperatorStorage::create(&path).unwrap();

        storage.set("cg1", 1, "key1", &json!("old")).unwrap();
        storage.set("cg1", 1, "key1", &json!("new")).unwrap();
        let val = storage.get("cg1", 1, "key1").unwrap();
        assert_eq!(val, Some(json!("new")));
    }

    #[test]
    fn test_del() {
        let (_tmp, path) = tmp_path();
        let mut storage = OperatorStorage::create(&path).unwrap();

        storage.set("cg1", 1, "key1", &json!(true)).unwrap();
        storage.del("cg1", 1, "key1").unwrap();
        let val = storage.get("cg1", 1, "key1").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_scan() {
        let (_tmp, path) = tmp_path();
        let mut storage = OperatorStorage::create(&path).unwrap();

        storage.set("cg1", 1, "abc", &json!(1)).unwrap();
        storage.set("cg1", 1, "abd", &json!(2)).unwrap();
        storage.set("cg1", 1, "xyz", &json!(3)).unwrap();

        let result = storage.scan("cg1", 1, "ab").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("abc".to_string(), json!(1)));
        assert_eq!(result[1], ("abd".to_string(), json!(2)));
    }

    #[test]
    fn test_clear_client_group() {
        let (_tmp, path) = tmp_path();
        let mut storage = OperatorStorage::create(&path).unwrap();

        storage.set("cg1", 1, "key1", &json!(1)).unwrap();
        storage.set("cg1", 2, "key2", &json!(2)).unwrap();
        storage.set("cg2", 1, "key1", &json!(3)).unwrap();

        storage.clear_client_group("cg1").unwrap();

        assert_eq!(storage.get("cg1", 1, "key1").unwrap(), None);
        assert_eq!(storage.get("cg1", 2, "key2").unwrap(), None);
        // cg2 should be unaffected.
        assert_eq!(storage.get("cg2", 1, "key1").unwrap(), Some(json!(3)));
    }

    #[test]
    fn test_commit_interval() {
        let (_tmp, path) = tmp_path();
        // Use a very small commit interval to exercise the checkpoint path.
        let mut storage = OperatorStorage::create_with_interval(&path, 3).unwrap();

        for i in 0..10 {
            storage.set("cg1", 1, &format!("k{i}"), &json!(i)).unwrap();
        }

        // All values should be readable.
        for i in 0..10 {
            let val = storage.get("cg1", 1, &format!("k{i}")).unwrap();
            assert_eq!(val, Some(json!(i)));
        }
    }

    #[test]
    fn test_close() {
        let (_tmp, path) = tmp_path();
        let mut storage = OperatorStorage::create(&path).unwrap();

        storage.set("cg1", 1, "key", &json!("val")).unwrap();
        storage.close().unwrap();

        // Verify data persisted after close.
        let conn = Connection::open(&path).unwrap();
        let val: String = conn
            .query_row(
                "SELECT val FROM storage WHERE clientGroupID = 'cg1' AND op = 1 AND key = 'key'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(val, "\"val\"");
    }
}
