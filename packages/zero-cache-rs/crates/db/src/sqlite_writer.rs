//! Dedicated SQLite writer thread.
//!
//! The TypeScript zero-cache uses a separate process for SQLite writes to avoid
//! blocking the event loop. In Rust we achieve the same isolation with a
//! dedicated `std::thread` that owns the only writable `rusqlite::Connection`
//! and processes commands received over a `crossbeam_channel`.
//!
//! All public methods are async-safe: they send a command to the writer thread
//! and await the response via a `tokio::sync::oneshot` channel.

use crossbeam_channel::{Receiver, Sender};
use rusqlite::Connection;
use tracing::{debug, error, info};

/// Commands sent to the writer thread.
pub enum WriteCommand {
    /// Execute a single SQL statement with parameters.
    Execute {
        sql: String,
        params: Vec<rusqlite::types::Value>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<usize>>,
    },
    /// Execute a batch of SQL statements (no parameters, no return value).
    ExecuteBatch {
        sql: String,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
    },
    /// Shut the writer thread down gracefully.
    Shutdown,
}

/// Handle to a dedicated SQLite writer thread.
///
/// Dropping this handle does **not** shut down the thread automatically.
/// Call [`SqliteWriter::shutdown`] for a graceful stop (the thread will close
/// the connection and exit), or let it outlive the handle if that is acceptable.
pub struct SqliteWriter {
    tx: Sender<WriteCommand>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl SqliteWriter {
    /// Spawn a writer thread that opens a single `rusqlite::Connection` at `path`.
    ///
    /// The connection is configured with `journal_mode=WAL` plus any extra
    /// `pragmas` supplied by the caller.
    pub fn spawn(path: &str, pragmas: &[(&str, &str)]) -> anyhow::Result<Self> {
        let (tx, rx) = crossbeam_channel::unbounded::<WriteCommand>();
        let db_path = path.to_string();
        let extra_pragmas: Vec<(String, String)> = pragmas
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let handle = std::thread::Builder::new()
            .name(format!("sqlite-writer-{}", path))
            .spawn(move || {
                if let Err(e) = writer_loop(&db_path, &extra_pragmas, rx) {
                    error!(path = db_path, error = %e, "sqlite writer thread failed");
                }
            })?;

        Ok(Self {
            tx,
            handle: Some(handle),
        })
    }

    /// Execute a single SQL statement with parameters. Returns the number of
    /// rows changed.
    pub async fn execute(
        &self,
        sql: String,
        params: Vec<rusqlite::types::Value>,
    ) -> anyhow::Result<usize> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx.send(WriteCommand::Execute {
            sql,
            params,
            reply: reply_tx,
        })?;
        reply_rx.await?
    }

    /// Execute a batch of SQL statements (separated by `;`). No parameters,
    /// no return value beyond success/failure.
    pub async fn execute_batch(&self, sql: String) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx.send(WriteCommand::ExecuteBatch {
            sql,
            reply: reply_tx,
        })?;
        reply_rx.await?
    }

    /// Shut the writer thread down gracefully. Blocks until the thread exits.
    pub fn shutdown(mut self) {
        let _ = self.tx.send(WriteCommand::Shutdown);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for SqliteWriter {
    fn drop(&mut self) {
        // Send shutdown signal but don't block waiting for the thread.
        let _ = self.tx.send(WriteCommand::Shutdown);
    }
}

/// The event loop that runs on the dedicated writer thread.
fn writer_loop(
    path: &str,
    pragmas: &[(String, String)],
    rx: Receiver<WriteCommand>,
) -> anyhow::Result<()> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;

    for (key, value) in pragmas {
        conn.pragma_update(None, key, value)?;
    }

    info!(path, "sqlite writer thread started");

    for cmd in &rx {
        match cmd {
            WriteCommand::Execute { sql, params, reply } => {
                let result = execute_one(&conn, &sql, &params);
                let _ = reply.send(result);
            }
            WriteCommand::ExecuteBatch { sql, reply } => {
                let result = conn.execute_batch(&sql).map_err(Into::into);
                let _ = reply.send(result);
            }
            WriteCommand::Shutdown => {
                debug!(path, "sqlite writer thread shutting down");
                break;
            }
        }
    }

    drop(conn);
    info!(path, "sqlite writer thread stopped");
    Ok(())
}

/// Execute a single parameterised statement.
fn execute_one(
    conn: &Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> anyhow::Result<usize> {
    let params_ref: Vec<&dyn rusqlite::types::ToSql> = params
        .iter()
        .map(|v| v as &dyn rusqlite::types::ToSql)
        .collect();
    let rows = conn.execute(sql, params_ref.as_slice())?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::types::Value;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_sqlite_writer_round_trip() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        let writer = SqliteWriter::spawn(path, &[]).unwrap();

        // Create table.
        writer
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)".to_string())
            .await
            .unwrap();

        // Insert a row.
        let changed = writer
            .execute(
                "INSERT INTO t (id, name) VALUES (?1, ?2)".to_string(),
                vec![Value::Integer(1), Value::Text("hello".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(changed, 1);

        // Insert another row.
        let changed = writer
            .execute(
                "INSERT INTO t (id, name) VALUES (?1, ?2)".to_string(),
                vec![Value::Integer(2), Value::Text("world".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(changed, 1);

        // Verify via a direct read connection.
        let read_conn = Connection::open(path).unwrap();
        let mut stmt = read_conn
            .prepare("SELECT id, name FROM t ORDER BY id")
            .unwrap();
        let rows: Vec<(i64, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            rows,
            vec![(1, "hello".to_string()), (2, "world".to_string())]
        );

        writer.shutdown();
    }

    #[tokio::test]
    async fn test_sqlite_writer_execute_batch() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        let writer = SqliteWriter::spawn(path, &[]).unwrap();

        writer
            .execute_batch("CREATE TABLE a (x INTEGER); CREATE TABLE b (y TEXT);".to_string())
            .await
            .unwrap();

        // Verify both tables exist.
        let read_conn = Connection::open(path).unwrap();
        let tables: Vec<String> = read_conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"a".to_string()));
        assert!(tables.contains(&"b".to_string()));

        writer.shutdown();
    }

    #[tokio::test]
    async fn test_sqlite_writer_error_handling() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        let writer = SqliteWriter::spawn(path, &[]).unwrap();

        // Executing on a non-existent table should return an error.
        let result = writer
            .execute(
                "INSERT INTO nonexistent (id) VALUES (?1)".to_string(),
                vec![Value::Integer(1)],
            )
            .await;

        assert!(result.is_err());

        writer.shutdown();
    }

    #[tokio::test]
    async fn test_sqlite_writer_custom_pragmas() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        let writer =
            SqliteWriter::spawn(path, &[("synchronous", "OFF"), ("foreign_keys", "ON")]).unwrap();

        writer
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)".to_string())
            .await
            .unwrap();

        // If we got here without error, the pragmas were accepted.
        writer.shutdown();
    }
}
