//! Schema initialisation SQL for the SQLite replica and PostgreSQL CVR.
//!
//! These are the bootstrap migrations that create the internal tables used
//! by the replicator and view-syncer. They are idempotent (`IF NOT EXISTS`).

// ---------------------------------------------------------------------------
// SQLite replica tables
// ---------------------------------------------------------------------------

/// Replication configuration table — tracks replica version, publications,
/// and the initial-sync context.
pub const SQLITE_REPLICATION_CONFIG: &str = r#"
CREATE TABLE IF NOT EXISTS "_zero.replicationConfig" (
    "replicaVersion" TEXT NOT NULL,
    "publications"   TEXT NOT NULL,
    "initialSyncContext" TEXT DEFAULT '{}',
    "lock" INTEGER PRIMARY KEY DEFAULT 1
)
"#;

/// Replication state table — tracks the current state version.
pub const SQLITE_REPLICATION_STATE: &str = r#"
CREATE TABLE IF NOT EXISTS "_zero.replicationState" (
    "stateVersion" TEXT NOT NULL,
    "lock" INTEGER PRIMARY KEY DEFAULT 1
)
"#;

/// Change log table — stores row-level changes for incremental view
/// maintenance. The view-syncer reads this to compute diffs between
/// snapshots.
pub const SQLITE_CHANGE_LOG: &str = r#"
CREATE TABLE IF NOT EXISTS "_zero.changeLog2" (
    "stateVersion" TEXT NOT NULL,
    "pos"          INTEGER NOT NULL,
    "table"        TEXT NOT NULL,
    "rowKey"       TEXT,
    "op"           TEXT NOT NULL,
    PRIMARY KEY ("stateVersion", "pos")
)
"#;

/// All SQLite replica bootstrap statements collected for convenience.
pub const SQLITE_REPLICA_INIT: &[&str] = &[
    SQLITE_REPLICATION_CONFIG,
    SQLITE_REPLICATION_STATE,
    SQLITE_CHANGE_LOG,
];

// ---------------------------------------------------------------------------
// PostgreSQL CVR tables (SQL strings, to be executed via `run_tx`)
// ---------------------------------------------------------------------------

/// Create the CVR schema if it doesn't exist.
pub const PG_CVR_SCHEMA: &str = r#"
CREATE SCHEMA IF NOT EXISTS "cvr"
"#;

/// Client groups table in the CVR schema.
pub const PG_CVR_CLIENT_GROUPS: &str = r#"
CREATE TABLE IF NOT EXISTS "cvr"."clientGroups" (
    "clientGroupID" TEXT PRIMARY KEY,
    "patchVersion"  TEXT NOT NULL DEFAULT '00',
    "cvrVersion"    TEXT NOT NULL DEFAULT '00',
    "lastActive"    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"#;

/// Clients table in the CVR schema.
pub const PG_CVR_CLIENTS: &str = r#"
CREATE TABLE IF NOT EXISTS "cvr"."clients" (
    "clientGroupID"  TEXT NOT NULL REFERENCES "cvr"."clientGroups"("clientGroupID") ON DELETE CASCADE,
    "clientID"       TEXT NOT NULL,
    "patchVersion"   TEXT NOT NULL DEFAULT '00',
    "lastMutationID" BIGINT NOT NULL DEFAULT 0,
    "lastActive"     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY ("clientGroupID", "clientID")
)
"#;

/// Desired query state in the CVR schema.
pub const PG_CVR_DESIRES: &str = r#"
CREATE TABLE IF NOT EXISTS "cvr"."desires" (
    "clientGroupID" TEXT NOT NULL REFERENCES "cvr"."clientGroups"("clientGroupID") ON DELETE CASCADE,
    "clientID"      TEXT NOT NULL,
    "queryHash"     TEXT NOT NULL,
    "patchVersion"  TEXT NOT NULL DEFAULT '00',
    "ast"           JSONB NOT NULL,
    "ttl"           TEXT,
    "gotAt"         TIMESTAMPTZ,
    PRIMARY KEY ("clientGroupID", "clientID", "queryHash")
)
"#;

/// All PG CVR bootstrap statements.
pub const PG_CVR_INIT: &[&str] = &[
    PG_CVR_SCHEMA,
    PG_CVR_CLIENT_GROUPS,
    PG_CVR_CLIENTS,
    PG_CVR_DESIRES,
];

/// Initialise the SQLite replica by running all bootstrap migrations.
///
/// This is safe to call multiple times (all statements use `IF NOT EXISTS`).
pub fn init_sqlite_replica(conn: &rusqlite::Connection) -> anyhow::Result<()> {
    for sql in SQLITE_REPLICA_INIT {
        conn.execute_batch(sql)?;
    }
    Ok(())
}

/// Initialise the PG CVR tables inside a transaction.
///
/// Caller is responsible for wrapping this in a transaction (e.g. via `run_tx`).
pub async fn init_pg_cvr(
    tx: &deadpool_postgres::Transaction<'_>,
) -> Result<(), tokio_postgres::Error> {
    for sql in PG_CVR_INIT {
        tx.batch_execute(sql).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_sqlite_replica_init_idempotent() {
        let conn = Connection::open_in_memory().unwrap();

        // Run twice to prove idempotency.
        init_sqlite_replica(&conn).unwrap();
        init_sqlite_replica(&conn).unwrap();

        // Verify tables exist by selecting from them.
        conn.execute_batch(
            r#"
            SELECT * FROM "_zero.replicationConfig";
            SELECT * FROM "_zero.replicationState";
            SELECT * FROM "_zero.changeLog2";
            "#,
        )
        .unwrap();
    }

    #[test]
    fn test_replication_config_schema() {
        let conn = Connection::open_in_memory().unwrap();
        init_sqlite_replica(&conn).unwrap();

        conn.execute(
            r#"INSERT INTO "_zero.replicationConfig" ("replicaVersion", "publications")
               VALUES ('1', '["pub1"]')"#,
            [],
        )
        .unwrap();

        let (version, pubs, ctx): (String, String, String) = conn
            .query_row(
                r#"SELECT "replicaVersion", "publications", "initialSyncContext"
                   FROM "_zero.replicationConfig""#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(version, "1");
        assert_eq!(pubs, r#"["pub1"]"#);
        assert_eq!(ctx, "{}");
    }

    #[test]
    fn test_replication_state_schema() {
        let conn = Connection::open_in_memory().unwrap();
        init_sqlite_replica(&conn).unwrap();

        conn.execute(
            r#"INSERT INTO "_zero.replicationState" ("stateVersion") VALUES ('abc123')"#,
            [],
        )
        .unwrap();

        let version: String = conn
            .query_row(
                r#"SELECT "stateVersion" FROM "_zero.replicationState""#,
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(version, "abc123");
    }
}
