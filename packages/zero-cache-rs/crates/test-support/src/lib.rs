//! Shared test-support helpers for integration tests that need a real
//! Postgres instance.
//!
//! ### Usage
//! ```ignore
//! #[tokio::test]
//! #[ignore] // only runs with `--ignored` or `--features pg-tests`
//! async fn my_pg_test() {
//!     if !zero_cache_test_support::docker_available() {
//!         eprintln!("skipping - Docker not available");
//!         return;
//!     }
//!     let pg = zero_cache_test_support::spawn_postgres().await.unwrap();
//!     let pool = pg.pool("my-test").await.unwrap();
//!     // ... run DDL / queries ...
//! }
//! ```
//!
//! The container is amortised per-process via a `tokio::sync::OnceCell`
//! (pattern **B7** from the translation guide). Each test gets its own fresh
//! database inside the shared container via `spawn_postgres()`, which issues
//! `CREATE DATABASE <uuid>` on the admin connection and returns a
//! [`PgHandle`] bound to that database.

use std::sync::Arc;

use anyhow::{Context, Result};
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;
use tokio_postgres::NoTls;
use tracing::info;

/// Handle to a Postgres database running inside a shared testcontainer.
///
/// Dropping a `PgHandle` does **not** drop the underlying database — use
/// [`PgHandle::drop_db`] for explicit cleanup (the container itself is torn
/// down when the process exits, so orphaned DBs are harmless).
pub struct PgHandle {
    dbname: String,
    admin_conn_str: String,
    conn_str: String,
    // Keep the shared container alive for the lifetime of this handle.
    _container: Arc<ContainerAsync<Postgres>>,
}

struct Shared {
    container: Arc<ContainerAsync<Postgres>>,
    admin_conn_str: String,
    host: String,
    port: u16,
}

/// One container per test process. First caller wins; subsequent callers reuse it.
static SHARED: OnceCell<Shared> = OnceCell::const_new();

/// Check at runtime whether Docker is reachable. Integration tests use this to
/// skip cleanly (printing a message to stderr) when Docker is not available in
/// the environment. We intentionally shell out to `docker info` rather than
/// probe `/var/run/docker.sock` so that custom `DOCKER_HOST` (colima, podman)
/// also works.
pub fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn shared() -> Result<&'static Shared> {
    SHARED
        .get_or_try_init(|| async {
            info!("starting shared Postgres testcontainer");
            let container = Postgres::default()
                .start()
                .await
                .context("failed to start Postgres container")?;
            let host = container.get_host().await.context("get_host")?.to_string();
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .context("get_host_port_ipv4")?;
            // testcontainers-modules default: user=postgres, pass=postgres, db=postgres
            let admin_conn_str =
                format!("postgres://postgres:postgres@{host}:{port}/postgres?sslmode=disable");
            info!(%host, port, "Postgres testcontainer ready");
            Ok::<_, anyhow::Error>(Shared {
                container: Arc::new(container),
                admin_conn_str,
                host,
                port,
            })
        })
        .await
}

/// Spawn (or reuse) a Postgres container and create a fresh database inside
/// it for the caller. Returns a [`PgHandle`] whose `conn_str()` points at the
/// new database.
pub async fn spawn_postgres() -> Result<PgHandle> {
    let sh = shared().await?;

    // One fresh DB per test. UUIDs would be ideal; instead we use a timestamp
    // + counter to avoid pulling `uuid` into this crate's lightweight dep set.
    let dbname = unique_dbname();

    // Connect as admin and CREATE DATABASE.
    let (admin, admin_conn) = tokio_postgres::connect(&sh.admin_conn_str, NoTls)
        .await
        .with_context(|| format!("connect admin {}", sh.admin_conn_str))?;
    let admin_handle = tokio::spawn(async move {
        if let Err(e) = admin_conn.await {
            tracing::warn!(error = %e, "admin conn task ended");
        }
    });
    admin
        .execute(&format!("CREATE DATABASE {dbname}"), &[])
        .await
        .with_context(|| format!("CREATE DATABASE {dbname}"))?;
    drop(admin);
    admin_handle.abort();

    let conn_str = format!(
        "postgres://postgres:postgres@{host}:{port}/{dbname}?sslmode=disable",
        host = sh.host,
        port = sh.port
    );
    Ok(PgHandle {
        dbname,
        admin_conn_str: sh.admin_conn_str.clone(),
        conn_str,
        _container: Arc::clone(&sh.container),
    })
}

impl PgHandle {
    /// Full connection string for this test's fresh database.
    pub fn connect_str(&self) -> &str {
        &self.conn_str
    }

    /// Database name inside the shared container.
    pub fn dbname(&self) -> &str {
        &self.dbname
    }

    /// Create a `deadpool-postgres` pool scoped to this handle's database.
    pub async fn pool(&self, app_name: &str) -> Result<zero_cache_db::PgPool> {
        zero_cache_db::PgPool::new(self.connect_str(), 4, app_name).await
    }

    /// Build a raw `deadpool-postgres::Pool` (for callers that don't want the
    /// zero-cache wrapper, e.g. the streamer tests).
    pub async fn raw_pool(&self, app_name: &str) -> Result<Pool> {
        let url = url::Url::parse(self.connect_str()).context("parse connect_str")?;
        let mut cfg = Config::new();
        cfg.host = Some(url.host_str().unwrap_or("localhost").to_string());
        cfg.port = Some(url.port().unwrap_or(5432));
        cfg.dbname = Some(url.path().trim_start_matches('/').to_string());
        cfg.user = Some(url.username().to_string());
        cfg.password = url.password().map(|p| p.to_string());
        cfg.application_name = Some(app_name.to_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context("build raw pool")?;
        pool.resize(4);
        Ok(pool)
    }

    /// Explicitly drop this handle's database (best-effort; errors are logged
    /// but not propagated). Tests do **not** need to call this — the container
    /// is disposed of at process exit.
    pub async fn drop_db(&self) {
        if let Err(e) = drop_db_inner(&self.admin_conn_str, &self.dbname).await {
            tracing::warn!(dbname = %self.dbname, error = %e, "drop_db failed");
        }
    }
}

async fn drop_db_inner(admin_conn_str: &str, dbname: &str) -> Result<()> {
    let (admin, conn) = tokio_postgres::connect(admin_conn_str, NoTls).await?;
    let h = tokio::spawn(async move {
        let _ = conn.await;
    });
    // Terminate any open connections first.
    let _ = admin
        .execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = $1 AND pid <> pg_backend_pid()",
            &[&dbname],
        )
        .await;
    admin
        .execute(&format!("DROP DATABASE IF EXISTS {dbname}"), &[])
        .await?;
    drop(admin);
    h.abort();
    Ok(())
}

/// Seed a table with `(column_name, value)` pairs, one row per call. The
/// table must already exist. Columns and values must be aligned.
///
/// ```ignore
/// seed(&pool, "users", &[("id", "1".into()), ("name", "'alice'".into())]).await?;
/// ```
///
/// Values are inlined directly into the SQL — this is an integration-test
/// helper only, **not** a production utility. Prefer parametrised queries in
/// code outside tests.
pub async fn seed(pool: &zero_cache_db::PgPool, table: &str, row: &[(&str, String)]) -> Result<()> {
    let client = pool.get().await?;
    let cols: Vec<&str> = row.iter().map(|(c, _)| *c).collect();
    let vals: Vec<&str> = row.iter().map(|(_, v)| v.as_str()).collect();
    let sql = format!(
        "INSERT INTO {table} ({cols}) VALUES ({vals})",
        cols = cols.join(", "),
        vals = vals.join(", "),
    );
    client.execute(&sql, &[]).await?;
    Ok(())
}

fn unique_dbname() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("zctest_{nanos:x}_{n}")
}
