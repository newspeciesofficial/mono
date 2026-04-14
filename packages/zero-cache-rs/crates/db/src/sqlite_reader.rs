//! SQLite reader pool: multiple read-only connections for concurrent reads.
//!
//! All SQLite operations are dispatched through `tokio::task::spawn_blocking`
//! to avoid blocking the Tokio runtime.

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use tokio::sync::Semaphore;
use tracing::debug;

/// A pool of read-only SQLite connections.
///
/// Connections are opened with `PRAGMA query_only = ON` and any extra pragmas
/// supplied by the caller. A `Semaphore` limits concurrency to the pool size.
pub struct SqliteReaderPool {
    connections: Vec<Arc<Mutex<Connection>>>,
    semaphore: Arc<Semaphore>,
}

impl SqliteReaderPool {
    /// Open `num_readers` read-only connections to the database at `path`.
    ///
    /// Each connection is configured with:
    /// - `journal_mode = WAL` (required for concurrent reads alongside WAL writer)
    /// - `query_only = ON`
    /// - Any additional pragmas from `pragmas`
    pub fn new(path: &str, num_readers: usize, pragmas: &[(&str, &str)]) -> anyhow::Result<Self> {
        assert!(num_readers > 0, "num_readers must be positive");

        let mut connections = Vec::with_capacity(num_readers);
        for _ in 0..num_readers {
            let conn = Connection::open_with_flags(
                path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "query_only", "ON")?;

            for (key, value) in pragmas {
                conn.pragma_update(None, key, value)?;
            }

            connections.push(Arc::new(Mutex::new(conn)));
        }

        debug!(path, num_readers, "sqlite reader pool created");

        Ok(Self {
            connections,
            semaphore: Arc::new(Semaphore::new(num_readers)),
        })
    }

    /// Execute `f` on one of the pooled connections.
    ///
    /// The closure runs inside `tokio::task::spawn_blocking` so it is safe to
    /// perform synchronous SQLite operations without blocking the async runtime.
    pub async fn with_connection<F, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&Connection) -> anyhow::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        // Acquire a semaphore permit to find an available connection index.
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("semaphore closed: {e}"))?;

        // Find the first unlocked connection. Because the semaphore limits
        // concurrency to exactly `num_readers`, at least one connection must
        // be available.
        let conn = self
            .connections
            .iter()
            .find_map(|c| {
                if c.try_lock().is_ok() {
                    Some(Arc::clone(c))
                } else {
                    None
                }
            })
            // Fallback: just grab the first one (will block briefly).
            .unwrap_or_else(|| Arc::clone(&self.connections[0]));

        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
            f(&guard)
        })
        .await?
    }

    /// Number of reader connections in the pool.
    pub fn size(&self) -> usize {
        self.connections.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    /// Helper: create a temp database with a table and some rows.
    fn setup_db() -> (NamedTempFile, String) {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
             INSERT INTO t VALUES (1, 'a');
             INSERT INTO t VALUES (2, 'b');
             INSERT INTO t VALUES (3, 'c');",
        )
        .unwrap();
        drop(conn);

        (tmp, path)
    }

    #[tokio::test]
    async fn test_reader_pool_basic() {
        let (_tmp, path) = setup_db();
        let pool = SqliteReaderPool::new(&path, 2, &[]).unwrap();

        let rows = pool
            .with_connection(|conn| {
                let mut stmt = conn.prepare("SELECT id, val FROM t ORDER BY id")?;
                let result: Vec<(i64, String)> = stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                Ok(result)
            })
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn test_reader_pool_concurrent_reads() {
        let (_tmp, path) = setup_db();
        let pool = Arc::new(SqliteReaderPool::new(&path, 4, &[]).unwrap());

        let mut handles = Vec::new();
        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            handles.push(tokio::spawn(async move {
                pool.with_connection(|conn| {
                    let count: i64 =
                        conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))?;
                    Ok(count)
                })
                .await
                .unwrap()
            }));
        }

        for handle in handles {
            let count = handle.await.unwrap();
            assert_eq!(count, 3);
        }
    }

    #[tokio::test]
    async fn test_reader_pool_read_only() {
        let (_tmp, path) = setup_db();
        let pool = SqliteReaderPool::new(&path, 1, &[]).unwrap();

        // Attempting to write through a reader should fail.
        let result = pool
            .with_connection(|conn| {
                conn.execute("INSERT INTO t VALUES (99, 'x')", [])
                    .map_err(Into::into)
            })
            .await;

        assert!(result.is_err());
    }
}
