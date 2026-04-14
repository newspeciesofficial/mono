//! PostgreSQL connection pool backed by `deadpool-postgres`.
//!
//! Handles SSL/TLS negotiation based on connection URI parameters (`ssl`, `sslmode`),
//! matching the behaviour of the TypeScript `pgClient` function in
//! `packages/zero-cache/src/types/pg.ts`.

use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::NoTls;
use tracing::info;
use url::Url;

/// A named PostgreSQL connection pool.
///
/// The `name` field is used for logging so operators can distinguish between
/// the different pools (e.g. "cvr", "upstream", "change").
pub struct PgPool {
    pool: Pool,
    name: String,
}

/// SSL mode parsed from the connection URI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SslMode {
    /// No SSL at all (`sslmode=disable`).
    Disable,
    /// SSL with certificate verification disabled (`sslmode=no-verify`).
    NoVerify,
    /// SSL if the server supports it (`sslmode=prefer`, the default).
    Prefer,
}

impl PgPool {
    /// Create a new pool from a Postgres connection URI.
    ///
    /// `max_conns` controls the maximum number of connections in the pool.
    /// `app_name` is attached to each connection as `application_name` for
    /// identification in `pg_stat_activity`.
    pub async fn new(connection_uri: &str, max_conns: u32, app_name: &str) -> anyhow::Result<Self> {
        let ssl_mode = parse_ssl_mode(connection_uri)?;

        // Parse the URI to extract connection params for deadpool
        let parsed = Url::parse(connection_uri)
            .map_err(|e| anyhow::anyhow!("invalid connection URI: {e}"))?;

        let mut cfg = Config::new();
        cfg.host = Some(parsed.host_str().unwrap_or("localhost").to_string());
        cfg.port = Some(parsed.port().unwrap_or(5432));
        cfg.dbname = Some(parsed.path().trim_start_matches('/').to_string());
        if !parsed.username().is_empty() {
            cfg.user = Some(parsed.username().to_string());
        }
        if let Some(pw) = parsed.password() {
            cfg.password = Some(pw.to_string());
        }
        cfg.application_name = Some(app_name.to_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });

        let pool = match ssl_mode {
            SslMode::Disable => {
                info!(pool = app_name, ssl = "disabled", "creating pg pool");
                cfg.create_pool(Some(Runtime::Tokio1), NoTls)?
            }
            SslMode::NoVerify => {
                info!(pool = app_name, ssl = "no-verify", "creating pg pool");
                let tls_connector = TlsConnector::builder()
                    .danger_accept_invalid_certs(true)
                    .build()?;
                let connector = MakeTlsConnector::new(tls_connector);
                cfg.create_pool(Some(Runtime::Tokio1), connector)?
            }
            SslMode::Prefer => {
                info!(pool = app_name, ssl = "prefer", "creating pg pool");
                // "prefer" means try SSL first, but fall back to plaintext.
                // We use NoTls here because deadpool-postgres doesn't natively
                // support the "prefer" fallback. In practice most deployments either
                // force SSL or disable it explicitly. Using NoTls here matches the
                // TS behaviour where `ssl = 'prefer'` still connects without
                // rejecting plaintext.
                let tls_connector = TlsConnector::builder()
                    .danger_accept_invalid_certs(true)
                    .build()?;
                let connector = MakeTlsConnector::new(tls_connector);
                cfg.create_pool(Some(Runtime::Tokio1), connector)?
            }
        };

        pool.resize(max_conns as usize);

        Ok(Self {
            pool,
            name: app_name.to_string(),
        })
    }

    /// Obtain a connection from the pool.
    pub async fn get(&self) -> anyhow::Result<deadpool_postgres::Client> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("pg pool '{}' get: {}", self.name, e))?;
        Ok(client)
    }

    /// The logical name of this pool (for logging).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Current pool status (size, available, waiting).
    pub fn status(&self) -> deadpool_postgres::Status {
        self.pool.status()
    }
}

/// Parse the SSL mode from the connection URI query parameters.
///
/// Checks `sslmode` first, then `ssl`, defaulting to `Prefer`.
fn parse_ssl_mode(connection_uri: &str) -> anyhow::Result<SslMode> {
    let url = Url::parse(connection_uri)?;

    let flag = url
        .query_pairs()
        .find_map(|(k, v)| {
            if k == "sslmode" || k == "ssl" {
                Some(v.to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "prefer".to_string());

    match flag.as_str() {
        "disable" | "false" => Ok(SslMode::Disable),
        "no-verify" => Ok(SslMode::NoVerify),
        "prefer" | "true" | "require" => Ok(SslMode::Prefer),
        other => Err(anyhow::anyhow!("unsupported sslmode: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ssl_mode_disable() {
        let mode = parse_ssl_mode("postgres://localhost/db?sslmode=disable").unwrap();
        assert_eq!(mode, SslMode::Disable);
    }

    #[test]
    fn test_parse_ssl_mode_false() {
        let mode = parse_ssl_mode("postgres://localhost/db?ssl=false").unwrap();
        assert_eq!(mode, SslMode::Disable);
    }

    #[test]
    fn test_parse_ssl_mode_no_verify() {
        let mode = parse_ssl_mode("postgres://localhost/db?sslmode=no-verify").unwrap();
        assert_eq!(mode, SslMode::NoVerify);
    }

    #[test]
    fn test_parse_ssl_mode_prefer_default() {
        let mode = parse_ssl_mode("postgres://localhost/db").unwrap();
        assert_eq!(mode, SslMode::Prefer);
    }

    #[test]
    fn test_parse_ssl_mode_prefer_explicit() {
        let mode = parse_ssl_mode("postgres://localhost/db?sslmode=prefer").unwrap();
        assert_eq!(mode, SslMode::Prefer);
    }

    #[test]
    fn test_parse_ssl_mode_require() {
        let mode = parse_ssl_mode("postgres://localhost/db?sslmode=require").unwrap();
        assert_eq!(mode, SslMode::Prefer);
    }
}
