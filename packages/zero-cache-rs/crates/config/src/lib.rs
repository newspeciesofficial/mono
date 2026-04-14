#![doc = "Configuration parsing for zero-cache-rs, mirroring zero-cache TypeScript config."]
#![deny(unsafe_code)]

use std::num::NonZeroUsize;
use std::thread::available_parallelism;

use clap::Parser;

/// Raw config parsed from env vars / CLI flags via clap.
#[derive(Parser, Debug, Clone)]
#[command(name = "zero-cache", about = "Zero real-time sync cache server")]
pub struct ZeroConfig {
    /// The upstream authoritative Postgres database connection string.
    #[arg(long, env = "ZERO_UPSTREAM_DB")]
    pub upstream_db: String,

    /// Postgres database for CVRs (client view records). Defaults to upstream_db.
    #[arg(long, env = "ZERO_CVR_DB")]
    pub cvr_db: Option<String>,

    /// Max connections to the upstream database for committing mutations.
    #[arg(long, env = "ZERO_UPSTREAM_MAX_CONNS", default_value_t = 20)]
    pub upstream_max_conns: u32,

    /// Max connections to the CVR database.
    #[arg(long, env = "ZERO_CVR_MAX_CONNS", default_value_t = 30)]
    pub cvr_max_conns: u32,

    /// Postgres database for storing recent replication log entries.
    #[arg(long, env = "ZERO_CHANGE_DB")]
    pub change_db: Option<String>,

    /// Max connections to the change database.
    #[arg(long, env = "ZERO_CHANGE_MAX_CONNS", default_value_t = 5)]
    pub change_max_conns: u32,

    /// Port for sync connections.
    #[arg(long, env = "ZERO_PORT", default_value_t = 5858)]
    pub port: u16,

    /// Port on which the change-streamer runs. Defaults to port + 1.
    #[arg(long, env = "ZERO_CHANGE_STREAMER_PORT")]
    pub change_streamer_port: Option<u16>,

    /// URI to connect to an external change-streamer.
    #[arg(long, env = "ZERO_CHANGE_STREAMER_URI")]
    pub change_streamer_uri: Option<String>,

    /// File path to the SQLite replica.
    #[arg(long, env = "ZERO_REPLICA_FILE", default_value = "zero.db")]
    pub replica_file: String,

    /// Number of processes for view syncing. Defaults to available parallelism - 1.
    #[arg(long, env = "ZERO_NUM_SYNC_WORKERS")]
    pub num_sync_workers: Option<u32>,

    /// Number of IVM pool worker threads per syncer process.
    /// 0 means IVM runs on the syncer's main event loop.
    #[arg(long, env = "ZERO_NUM_POOL_THREADS", default_value_t = 0)]
    pub num_pool_threads: u32,

    /// Max time in ms a sync worker spends in IVM before yielding.
    #[arg(long, env = "ZERO_YIELD_THRESHOLD_MS", default_value_t = 10)]
    pub yield_threshold_ms: u32,

    /// Enable the query planner for optimizing ZQL queries.
    #[arg(
        long,
        env = "ZERO_ENABLE_QUERY_PLANNER",
        default_value_t = true,
        action = clap::ArgAction::Set,
    )]
    pub enable_query_planner: bool,

    /// Unique identifier for the app.
    #[arg(long, env = "ZERO_APP_ID", default_value = "zero")]
    pub app_id: String,

    /// Shard number of the app.
    #[arg(long, env = "ZERO_SHARD_NUM", default_value_t = 0)]
    pub shard_num: u32,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, env = "ZERO_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Password for administering the zero-cache server.
    #[arg(long, env = "ZERO_ADMIN_PASSWORD")]
    pub admin_password: Option<String>,

    /// Hours of inactivity after which a CVR is eligible for garbage collection.
    #[arg(
        long,
        env = "ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS",
        default_value_t = 48
    )]
    pub cvr_gc_inactivity_threshold_hours: u32,

    /// Initial interval in seconds for CVR garbage collection checks.
    #[arg(
        long,
        env = "ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS",
        default_value_t = 60
    )]
    pub cvr_gc_initial_interval_seconds: u32,

    /// Initial batch size for CVR garbage collection. 0 disables GC.
    #[arg(
        long,
        env = "ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE",
        default_value_t = 25
    )]
    pub cvr_gc_initial_batch_size: u32,

    /// Proportion of heap to use as back-pressure buffer (0.0 - 1.0).
    #[arg(
        long,
        env = "ZERO_BACK_PRESSURE_LIMIT_HEAP_PROPORTION",
        default_value_t = 0.04
    )]
    pub back_pressure_limit_heap_proportion: f64,

    /// Enable WebSocket per-message deflate compression.
    #[arg(
        long,
        env = "ZERO_WEBSOCKET_COMPRESSION",
        default_value_t = false,
        action = clap::ArgAction::Set,
    )]
    pub websocket_compression: bool,

    /// Maximum size of incoming WebSocket messages in bytes.
    #[arg(
        long,
        env = "ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES",
        default_value_t = 10_485_760
    )]
    pub websocket_max_payload_bytes: u32,

    /// Change streamer mode: "external" (default) connects to a WS change-streamer,
    /// "internal" uses the built-in Rust PG change source (initial sync + polling).
    #[arg(long, env = "ZERO_CHANGE_STREAMER_MODE", default_value = "external")]
    pub change_streamer_mode: String,

    /// Batch size for processing changes during hydration. Lower values stream
    /// patches to clients sooner at the cost of more frequent CVR updater calls.
    #[arg(long, env = "ZERO_CURSOR_PAGE_SIZE", default_value_t = 10000)]
    pub cursor_page_size: u32,

    /// Threshold in milliseconds after which an advance operation logs a warning
    /// with a full per-stage timing breakdown.
    #[arg(long, env = "ZERO_ADVANCE_TIMEOUT_MS", default_value_t = 30000)]
    pub advance_timeout_ms: u64,

    /// Enable least-loaded syncer routing. When set, new client groups are
    /// assigned to the syncer with the fewest active connections instead of
    /// using hash-based routing. Assignments are persisted to the file
    /// specified by `ZERO_SYNCER_ASSIGNMENTS_FILE`.
    #[arg(
        long,
        env = "ZERO_LEAST_LOADED_ROUTING",
        default_value_t = false,
        action = clap::ArgAction::Set,
    )]
    pub least_loaded_routing: bool,

    /// File path for persisting syncer assignments when least-loaded routing
    /// is enabled.
    #[arg(
        long,
        env = "ZERO_SYNCER_ASSIGNMENTS_FILE",
        default_value = "syncer-assignments.json"
    )]
    pub syncer_assignments_file: String,
}

/// Normalized config with all derived defaults resolved.
#[derive(Debug, Clone)]
pub struct NormalizedConfig {
    pub upstream_db: String,
    pub cvr_db: String,
    pub upstream_max_conns: u32,
    pub cvr_max_conns: u32,
    pub change_db: String,
    pub change_max_conns: u32,
    pub port: u16,
    pub change_streamer_port: u16,
    pub change_streamer_uri: Option<String>,
    pub replica_file: String,
    pub num_sync_workers: u32,
    pub num_pool_threads: u32,
    pub yield_threshold_ms: u32,
    pub enable_query_planner: bool,
    pub app_id: String,
    pub shard_num: u32,
    pub log_level: String,
    pub admin_password: Option<String>,
    pub cvr_gc_inactivity_threshold_hours: u32,
    pub cvr_gc_initial_interval_seconds: u32,
    pub cvr_gc_initial_batch_size: u32,
    pub back_pressure_limit_heap_proportion: f64,
    pub websocket_compression: bool,
    pub websocket_max_payload_bytes: u32,
    pub change_streamer_mode: ChangeStreamerMode,
    pub cursor_page_size: u32,
    pub advance_timeout_ms: u64,
    pub least_loaded_routing: bool,
    pub syncer_assignments_file: String,
}

/// Typed auth configuration surfaced from the three env vars the TS side
/// consults (`ZERO_AUTH_JWK`, `ZERO_AUTH_JWKS_URL`, `ZERO_AUTH_SECRET`).
///
/// Mirrors TS `auth.{jwk, jwksUrl, secret}` in `config/zero-config.ts`. This
/// struct is read once at startup and threaded through to the `Syncer` so
/// JWT verification can run without re-reading `std::env` per request.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Raw JWK JSON (single key). TS: `ZERO_AUTH_JWK`.
    pub jwk: Option<String>,
    /// Remote JWKS endpoint. TS: `ZERO_AUTH_JWKS_URL`.
    pub jwks_url: Option<String>,
    /// HMAC shared secret (HS256). TS: `ZERO_AUTH_SECRET`.
    pub secret: Option<String>,
}

impl AuthConfig {
    /// Load from process environment. Values set to empty strings are
    /// treated as unset.
    pub fn from_env() -> Self {
        fn non_empty(name: &str) -> Option<String> {
            std::env::var(name).ok().filter(|v| !v.is_empty())
        }
        Self {
            jwk: non_empty("ZERO_AUTH_JWK"),
            jwks_url: non_empty("ZERO_AUTH_JWKS_URL"),
            secret: non_empty("ZERO_AUTH_SECRET"),
        }
    }

    /// True when none of the three env vars are set — "dev mode", in which
    /// callers should fall back to decoding JWT payloads without verifying
    /// the signature (and log a warning).
    pub fn is_dev_mode(&self) -> bool {
        self.jwk.is_none() && self.jwks_url.is_none() && self.secret.is_none()
    }
}

/// Change streamer mode: how the Rust server gets replication data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeStreamerMode {
    /// Connect to an external WS-based change-streamer (TS server).
    External,
    /// Use the built-in Rust PG change source (initial sync + change log polling).
    Internal,
}

impl ChangeStreamerMode {
    /// Parse from a string value (case-insensitive).
    pub fn from_str_value(s: &str) -> Self {
        if s.eq_ignore_ascii_case("internal") {
            Self::Internal
        } else {
            Self::External
        }
    }

    /// Returns true if internal mode.
    pub fn is_internal(&self) -> bool {
        *self == Self::Internal
    }
}

impl NormalizedConfig {
    /// Compute derived defaults from the raw parsed config.
    pub fn from_raw(config: ZeroConfig) -> Self {
        let cvr_db = config
            .cvr_db
            .clone()
            .unwrap_or_else(|| config.upstream_db.clone());

        let change_db = config
            .change_db
            .clone()
            .unwrap_or_else(|| config.upstream_db.clone());

        let change_streamer_port = config.change_streamer_port.unwrap_or(config.port + 1);

        let num_sync_workers = config.num_sync_workers.unwrap_or_else(|| {
            let parallelism = available_parallelism()
                .unwrap_or(NonZeroUsize::new(2).expect("2 is non-zero"))
                .get() as u32;
            // Reserve 1 core for the replicator.
            parallelism.saturating_sub(1).max(1)
        });

        let change_streamer_mode = ChangeStreamerMode::from_str_value(&config.change_streamer_mode);

        NormalizedConfig {
            upstream_db: config.upstream_db,
            cvr_db,
            upstream_max_conns: config.upstream_max_conns,
            cvr_max_conns: config.cvr_max_conns,
            change_db,
            change_max_conns: config.change_max_conns,
            port: config.port,
            change_streamer_port,
            change_streamer_uri: config.change_streamer_uri,
            replica_file: config.replica_file,
            num_sync_workers,
            num_pool_threads: config.num_pool_threads,
            yield_threshold_ms: config.yield_threshold_ms,
            enable_query_planner: config.enable_query_planner,
            app_id: config.app_id,
            shard_num: config.shard_num,
            log_level: config.log_level,
            admin_password: config.admin_password,
            cvr_gc_inactivity_threshold_hours: config.cvr_gc_inactivity_threshold_hours,
            cvr_gc_initial_interval_seconds: config.cvr_gc_initial_interval_seconds,
            cvr_gc_initial_batch_size: config.cvr_gc_initial_batch_size,
            back_pressure_limit_heap_proportion: config.back_pressure_limit_heap_proportion,
            websocket_compression: config.websocket_compression,
            websocket_max_payload_bytes: config.websocket_max_payload_bytes,
            change_streamer_mode,
            cursor_page_size: config.cursor_page_size,
            advance_timeout_ms: config.advance_timeout_ms,
            least_loaded_routing: config.least_loaded_routing,
            syncer_assignments_file: config.syncer_assignments_file,
        }
    }

    /// Human-readable summary for startup logging.
    pub fn summary(&self) -> String {
        let mode = if self.change_streamer_mode.is_internal() {
            "internal"
        } else {
            "external"
        };
        format!(
            "app={} shard={} port={} change_streamer_port={} \
             sync_workers={} pool_threads={} replica={} streamer_mode={}",
            self.app_id,
            self.shard_num,
            self.port,
            self.change_streamer_port,
            self.num_sync_workers,
            self.num_pool_threads,
            self.replica_file,
            mode,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse config from CLI args (avoids unsafe env var manipulation in Rust 2024).
    fn parse_args(args: &[&str]) -> ZeroConfig {
        let mut full_args = vec!["zero-cache"];
        full_args.extend_from_slice(args);
        ZeroConfig::try_parse_from(full_args).expect("config should parse")
    }

    #[test]
    fn test_required_upstream_db() {
        // Without --upstream-db, parsing should fail.
        let result = ZeroConfig::try_parse_from(["zero-cache"]);
        assert!(result.is_err(), "should fail without upstream_db");
    }

    #[test]
    fn test_defaults() {
        let config = parse_args(&["--upstream-db", "postgres://localhost/zero"]);
        assert_eq!(config.upstream_db, "postgres://localhost/zero");
        assert_eq!(config.upstream_max_conns, 20);
        assert_eq!(config.cvr_max_conns, 30);
        assert_eq!(config.change_max_conns, 5);
        assert_eq!(config.port, 5858);
        assert!(config.cvr_db.is_none());
        assert!(config.change_db.is_none());
        assert!(config.change_streamer_port.is_none());
        assert!(config.change_streamer_uri.is_none());
        assert_eq!(config.replica_file, "zero.db");
        assert!(config.num_sync_workers.is_none());
        assert_eq!(config.num_pool_threads, 0);
        assert_eq!(config.yield_threshold_ms, 10);
        assert!(config.enable_query_planner);
        assert_eq!(config.app_id, "zero");
        assert_eq!(config.shard_num, 0);
        assert_eq!(config.log_level, "info");
        assert!(config.admin_password.is_none());
        assert_eq!(config.cvr_gc_inactivity_threshold_hours, 48);
        assert_eq!(config.cvr_gc_initial_interval_seconds, 60);
        assert_eq!(config.cvr_gc_initial_batch_size, 25);
        assert!((config.back_pressure_limit_heap_proportion - 0.04).abs() < f64::EPSILON);
        assert!(!config.websocket_compression);
        assert_eq!(config.websocket_max_payload_bytes, 10_485_760);
        assert_eq!(config.cursor_page_size, 10000);
        assert_eq!(config.advance_timeout_ms, 30000);
        assert!(!config.least_loaded_routing);
        assert_eq!(config.syncer_assignments_file, "syncer-assignments.json");
    }

    #[test]
    fn test_cli_overrides() {
        let config = parse_args(&[
            "--upstream-db",
            "postgres://prod/db",
            "--cvr-db",
            "postgres://prod/cvr",
            "--change-db",
            "postgres://prod/change",
            "--port",
            "5000",
            "--change-streamer-port",
            "5001",
            "--num-sync-workers",
            "8",
            "--num-pool-threads",
            "4",
            "--yield-threshold-ms",
            "20",
            "--enable-query-planner",
            "false",
            "--app-id",
            "myapp",
            "--shard-num",
            "2",
            "--log-level",
            "debug",
            "--admin-password",
            "secret",
            "--upstream-max-conns",
            "50",
            "--cvr-max-conns",
            "60",
            "--change-max-conns",
            "10",
            "--websocket-compression",
            "true",
            "--websocket-max-payload-bytes",
            "5242880",
            "--back-pressure-limit-heap-proportion",
            "0.08",
        ]);
        assert_eq!(config.upstream_db, "postgres://prod/db");
        assert_eq!(config.cvr_db.as_deref(), Some("postgres://prod/cvr"));
        assert_eq!(config.change_db.as_deref(), Some("postgres://prod/change"));
        assert_eq!(config.port, 5000);
        assert_eq!(config.change_streamer_port, Some(5001));
        assert_eq!(config.num_sync_workers, Some(8));
        assert_eq!(config.num_pool_threads, 4);
        assert_eq!(config.yield_threshold_ms, 20);
        assert!(!config.enable_query_planner);
        assert_eq!(config.app_id, "myapp");
        assert_eq!(config.shard_num, 2);
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.admin_password.as_deref(), Some("secret"));
        assert_eq!(config.upstream_max_conns, 50);
        assert_eq!(config.cvr_max_conns, 60);
        assert_eq!(config.change_max_conns, 10);
        assert!(config.websocket_compression);
        assert_eq!(config.websocket_max_payload_bytes, 5_242_880);
        assert!((config.back_pressure_limit_heap_proportion - 0.08).abs() < f64::EPSILON);
    }

    #[test]
    fn test_normalized_defaults() {
        let config = parse_args(&["--upstream-db", "postgres://localhost/zero"]);
        let normalized = NormalizedConfig::from_raw(config);

        // cvr_db defaults to upstream_db
        assert_eq!(normalized.cvr_db, "postgres://localhost/zero");
        // change_db defaults to upstream_db
        assert_eq!(normalized.change_db, "postgres://localhost/zero");
        // change_streamer_port defaults to port + 1
        assert_eq!(normalized.change_streamer_port, 5859);
        // num_sync_workers >= 1
        assert!(normalized.num_sync_workers >= 1);
    }

    #[test]
    fn test_normalized_explicit_values() {
        let config = parse_args(&[
            "--upstream-db",
            "postgres://prod/db",
            "--cvr-db",
            "postgres://prod/cvr",
            "--change-db",
            "postgres://prod/change",
            "--port",
            "5000",
            "--change-streamer-port",
            "6000",
            "--num-sync-workers",
            "4",
        ]);
        let normalized = NormalizedConfig::from_raw(config);

        assert_eq!(normalized.cvr_db, "postgres://prod/cvr");
        assert_eq!(normalized.change_db, "postgres://prod/change");
        assert_eq!(normalized.change_streamer_port, 6000);
        assert_eq!(normalized.num_sync_workers, 4);
    }

    #[test]
    fn test_auth_config_dev_mode() {
        let cfg = AuthConfig::default();
        assert!(cfg.is_dev_mode());
        let cfg = AuthConfig {
            secret: Some("s".into()),
            ..Default::default()
        };
        assert!(!cfg.is_dev_mode());
    }

    #[test]
    fn test_summary_format() {
        let config = parse_args(&["--upstream-db", "postgres://localhost/zero"]);
        let normalized = NormalizedConfig::from_raw(config);
        let summary = normalized.summary();
        assert!(summary.contains("app=zero"));
        assert!(summary.contains("port=5858"));
        assert!(summary.contains("shard=0"));
    }
}
