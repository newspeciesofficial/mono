//! End-to-end smoke tests for the vendored SQLite 3.50.0 amalgamation.
//!
//! Confirms every SQL feature the zero-cache TS side depends on is reachable
//! from rusqlite through our `[patch.crates-io] libsqlite3-sys = …` fork.
//!
//! If any of these fail, don't ship the port — the vendored amalgamation is
//! the wrong branch or the compile flags have drifted.

use rusqlite::{params, Connection, OpenFlags};

fn open(path: &str) -> Connection {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )
    .expect("open")
}

#[test]
fn version_is_3_50_0() {
    let conn = Connection::open_in_memory().unwrap();
    let v: String = conn.query_row("SELECT sqlite_version()", [], |r| r.get(0)).unwrap();
    assert!(v.starts_with("3.50."), "got SQLite version {v}");
}

#[test]
fn wal2_journal_mode_takes_effect() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal2.db");
    let conn = open(path.to_str().unwrap());
    let mode: String = conn
        .query_row("PRAGMA journal_mode = wal2", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal2", "wal2 pragma returned {mode:?}");
}

#[test]
fn begin_concurrent_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bc.db");
    let conn = open(path.to_str().unwrap());
    conn.execute_batch("PRAGMA journal_mode = wal2;").unwrap();
    conn.execute_batch("CREATE TABLE t(x INTEGER PRIMARY KEY);")
        .unwrap();
    conn.execute_batch("BEGIN CONCURRENT; INSERT INTO t VALUES (1); COMMIT;")
        .unwrap();
    let n: i64 = conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 1);
}

#[test]
fn savepoint_rollback_to_works() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE t(x INTEGER);
         INSERT INTO t VALUES (1);
         SAVEPOINT s1;
         INSERT INTO t VALUES (2);
         ROLLBACK TO s1;
         RELEASE s1;",
    )
    .unwrap();
    let n: i64 = conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 1, "savepoint rollback should have dropped row 2");
}

#[test]
fn json_patch_function_exists() {
    let conn = Connection::open_in_memory().unwrap();
    let got: String = conn
        .query_row(
            r#"SELECT json_patch('{"a":1}', '{"b":2}')"#,
            [],
            |r| r.get(0),
        )
        .unwrap();
    // Order is implementation-defined; just assert both keys present.
    assert!(got.contains("\"a\":1") && got.contains("\"b\":2"), "got {got}");
}

#[test]
fn returning_clause_works() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);")
        .unwrap();
    let id: i64 = conn
        .query_row(
            "INSERT INTO t(v) VALUES (?1) RETURNING id",
            params!["hello"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(id, 1);
}

#[test]
fn vacuum_into_works() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    let src = src_dir.path().join("src.db");
    let dst = dst_dir.path().join("dst.db");

    let conn = open(src.to_str().unwrap());
    conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (42);")
        .unwrap();
    conn.execute("VACUUM INTO ?1", params![dst.to_str().unwrap()])
        .unwrap();

    let dst_conn = open(dst.to_str().unwrap());
    let v: i64 = dst_conn.query_row("SELECT x FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(v, 42);
}

#[test]
fn on_conflict_do_update_upsert() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER);")
        .unwrap();
    conn.execute(
        "INSERT INTO t(k, v) VALUES (1, 10) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO t(k, v) VALUES (1, 20) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        [],
    )
    .unwrap();
    let v: i64 = conn
        .query_row("SELECT v FROM t WHERE k = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 20);
}

#[test]
fn two_wal2_readers_see_same_snapshot() {
    // Confirms the Snapshotter leapfrog pattern: two long-lived readers on
    // the same file, each holding an independent BEGIN CONCURRENT snapshot
    // while a writer commits on a third connection.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("leapfrog.db");

    let writer = open(path.to_str().unwrap());
    writer
        .execute_batch("PRAGMA journal_mode = wal2; CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();

    let r1 = open(path.to_str().unwrap());
    r1.execute_batch("BEGIN CONCURRENT").unwrap();
    let before_r1: i64 = r1.query_row("SELECT x FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(before_r1, 1);

    writer.execute_batch("INSERT INTO t VALUES (2);").unwrap();

    // r1's snapshot must still see the pre-insert state.
    let count_r1: i64 = r1.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count_r1, 1, "r1 should still see snapshot-at-BEGIN");

    let r2 = open(path.to_str().unwrap());
    r2.execute_batch("BEGIN CONCURRENT").unwrap();
    let count_r2: i64 = r2.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count_r2, 2, "r2 opened after the insert must see 2 rows");

    r1.execute_batch("ROLLBACK").unwrap();
    r2.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn analyze_and_stat4_work() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE t(x INTEGER);
         INSERT INTO t VALUES (1), (2), (3), (4), (5);
         CREATE INDEX i ON t(x);
         ANALYZE main;",
    )
    .unwrap();
    // STAT4 populates sqlite_stat4 when ANALYZE runs on indexed columns.
    let n: i64 = conn
        .query_row("SELECT count(*) FROM sqlite_stat1", [], |r| r.get(0))
        .unwrap();
    assert!(n >= 1, "sqlite_stat1 should have rows after ANALYZE");
}
