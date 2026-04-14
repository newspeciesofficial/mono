//! Transaction helpers for PostgreSQL.
//!
//! Provides a simple `run_tx` function that executes a closure inside a
//! database transaction, committing on success and rolling back on error.
//! This is a simplified Rust equivalent of the TypeScript `TransactionPool`
//! from `packages/zero-cache/src/db/transaction-pool.ts`.

use std::future::Future;
use std::pin::Pin;

use crate::pg::PgPool;

/// A boxed future returned by the transaction closure.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Execute `f` inside a PostgreSQL transaction.
///
/// The transaction is committed if `f` returns `Ok`, rolled back otherwise.
pub async fn run_tx<F, T>(pool: &PgPool, f: F) -> anyhow::Result<T>
where
    F: for<'a> FnOnce(&'a deadpool_postgres::Transaction<'a>) -> BoxFuture<'a, anyhow::Result<T>>,
{
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;

    match f(&tx).await {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(e) => {
            // Transaction is rolled back on drop, but we do it explicitly for clarity.
            if let Err(rollback_err) = tx.rollback().await {
                tracing::warn!(
                    pool = pool.name(),
                    error = %rollback_err,
                    "rollback failed after transaction error"
                );
            }
            Err(e)
        }
    }
}

/// Execute a read-only transaction.
///
/// Same as `run_tx` but sets the transaction to READ ONLY mode first.
pub async fn run_read_tx<F, T>(pool: &PgPool, f: F) -> anyhow::Result<T>
where
    F: for<'a> FnOnce(&'a deadpool_postgres::Transaction<'a>) -> BoxFuture<'a, anyhow::Result<T>>,
{
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    tx.batch_execute("SET TRANSACTION READ ONLY").await?;

    match f(&tx).await {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(e) => {
            if let Err(rollback_err) = tx.rollback().await {
                tracing::warn!(
                    pool = pool.name(),
                    error = %rollback_err,
                    "rollback failed after read transaction error"
                );
            }
            Err(e)
        }
    }
}
