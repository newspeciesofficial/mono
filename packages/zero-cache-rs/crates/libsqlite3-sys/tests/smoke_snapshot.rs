//! Small-scale proof that `sqlite3_snapshot_get` / `_open` actually work in
//! our build — BEFORE wiring them into the parallel-hydration path.
//!
//! Scenario under test:
//!   1. Writer inserts row A and commits
//!   2. Leader BEGIN CONCURRENT, reads to materialize the read lock,
//!      calls sqlite3_snapshot_get → captures LSN at A
//!   3. Writer inserts row B and commits (advancing LSN past A)
//!   4. Worker (separate Connection) calls sqlite3_snapshot_open(handle)
//!      and MUST see only row A — not A+B — because the snapshot was
//!      captured before B was committed
//!
//! If this passes: cross-connection snapshot sharing works in our build,
//! WAL2 is compatible, and we can build parallel hydration on top.
//!
//! If this fails: something is wrong with SQLITE_ENABLE_SNAPSHOT + WAL2
//! before we touch the IVM code, so we abort the plan early.

#![allow(unsafe_code)]

use libsqlite3_sys as ffi;
use rusqlite::Connection;
use std::ffi::CString;
use std::ptr;

// ─── Safe wrappers around the raw snapshot FFI ────────────────────────

struct SnapshotHandle {
    raw: *mut ffi::sqlite3_snapshot,
}

// SQLite snapshot handles carry no thread-affinity; forum guidance +
// source-code review confirm Send is safe. We Send them into rayon
// workers via std::sync::Arc in the real integration.
unsafe impl Send for SnapshotHandle {}
unsafe impl Sync for SnapshotHandle {}

impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { ffi::sqlite3_snapshot_free(self.raw) };
        }
    }
}

fn snapshot_get(conn: &Connection) -> Result<SnapshotHandle, String> {
    let db = unsafe { conn.handle() };
    let schema = CString::new("main").unwrap();
    let mut snap: *mut ffi::sqlite3_snapshot = ptr::null_mut();
    let rc = unsafe { ffi::sqlite3_snapshot_get(db, schema.as_ptr(), &mut snap) };
    if rc != ffi::SQLITE_OK as i32 {
        return Err(format!("sqlite3_snapshot_get rc={rc}"));
    }
    assert!(!snap.is_null(), "snapshot_get returned null on OK");
    Ok(SnapshotHandle { raw: snap })
}

fn snapshot_open(conn: &Connection, snap: &SnapshotHandle) -> Result<(), String> {
    let db = unsafe { conn.handle() };
    let schema = CString::new("main").unwrap();
    let rc = unsafe { ffi::sqlite3_snapshot_open(db, schema.as_ptr(), snap.raw) };
    if rc != ffi::SQLITE_OK as i32 {
        let msg = unsafe {
            let p = ffi::sqlite3_errmsg(db);
            std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
        };
        let ac = unsafe { ffi::sqlite3_get_autocommit(db) };
        return Err(format!(
            "sqlite3_snapshot_open rc={rc} errmsg=\"{msg}\" autocommit={ac}",
        ));
    }
    Ok(())
}

// ─── The actual smoke tests ───────────────────────────────────────────

fn open_wal(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal");
    // Per SQLite forum guidance — disable auto-checkpoint so the writer's
    // subsequent INSERT can't silently checkpoint the WAL past the snapshot's
    // LSN between the leader's `snapshot_get` and the worker's `snapshot_open`.
    conn.execute_batch("PRAGMA wal_autocheckpoint = 0").unwrap();
    conn
}

fn open_wal2(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode = wal2", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal2");
    conn.execute_batch("PRAGMA wal_autocheckpoint = 0").unwrap();
    conn
}

fn count(conn: &Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap()
}

#[test]
fn snapshot_get_succeeds_round_trip_same_connection_wal() {
    // Narrowest viable test: ONE Connection does snapshot_get + snapshot_open
    // on itself. If this fails, the SQLITE_ENABLE_SNAPSHOT build is broken
    // or the preconditions are wrong — and cross-connection is hopeless
    // until we fix that.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("roundtrip.db");

    let w = open_wal(&path);
    w.execute_batch("CREATE TABLE t(x INTEGER PRIMARY KEY);")
        .unwrap();
    w.execute_batch("INSERT INTO t VALUES (1);").unwrap();
    // Commit is implicit — execute_batch wraps in its own txn.
    drop(w);

    let conn = open_wal(&path);
    conn.execute_batch("BEGIN").unwrap();
    let _ = count(&conn); // materialize read lock
    let snap = snapshot_get(&conn).expect("roundtrip snapshot_get failed");
    conn.execute_batch("COMMIT").unwrap();

    // Now re-open to the same snapshot on the same connection.
    conn.execute_batch("BEGIN").unwrap();
    snapshot_open(&conn, &snap).expect("roundtrip snapshot_open failed");
    assert_eq!(count(&conn), 1);
    conn.execute_batch("COMMIT").unwrap();
}

#[test]
fn snapshot_pins_worker_to_leader_lsn_in_wal_mode() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.db");

    // Writer: initialize schema + first row.
    let writer = open_wal(&path);
    writer
        .execute_batch("CREATE TABLE t(x INTEGER PRIMARY KEY);")
        .unwrap();
    writer.execute_batch("INSERT INTO t VALUES (1);").unwrap();

    // Leader: open a plain deferred read transaction (NOT `BEGIN CONCURRENT`
    // — that registers as write-state and blocks snapshot_get). The
    // subsequent SELECT materialises the read-lock so the pager has a
    // concrete WAL position to hand back.
    let leader = Connection::open(&path).unwrap();
    leader.execute_batch("BEGIN").unwrap();
    let before = count(&leader);
    assert_eq!(before, 1, "leader should see row 1");
    let snap = snapshot_get(&leader).expect("snapshot_get failed");

    // Writer advances: now t = {1, 2}.
    writer.execute_batch("INSERT INTO t VALUES (2);").unwrap();

    // Sanity: a fresh Connection with no snapshot sees both rows.
    let fresh = Connection::open(&path).unwrap();
    assert_eq!(count(&fresh), 2, "fresh conn should see both rows");

    // Worker: pin to the leader's snapshot — MUST see only {1}. Per the
    // impl in sqlite3.c:190370, snapshot_open requires
    //   - db->autoCommit == 0 (we're in a txn) — so BEGIN first
    //   - btree txn state != WRITE
    //   - if txn state is already READ, nVdbeActive == 0
    // Starting a plain BEGIN sets autoCommit=0 but leaves btree state at
    // NONE until first read, which satisfies the else branch and lets
    // snapshot_open start a fresh read txn pinned to the handle's LSN.
    let worker = Connection::open(&path).unwrap();
    // Warm up the worker's WAL handle by taking a brief read+commit first
    // — an empty BEGIN leaves the pager with no WAL context, which
    // snapshot_open's internal sqlite3PagerSnapshotOpen rejects with
    // SQLITE_ERROR. A dummy read populates the pager's WAL state.
    let _: i64 = worker
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();

    worker.execute_batch("BEGIN").unwrap();
    snapshot_open(&worker, &snap).expect("snapshot_open failed");
    assert_eq!(
        count(&worker),
        1,
        "worker pinned to leader's snapshot must see only the pre-row-2 state",
    );

    // Release.
    worker.execute_batch("COMMIT").unwrap();
    leader.execute_batch("COMMIT").unwrap();
    drop(snap);
}

#[test]
fn snapshot_api_is_rejected_in_wal2_mode() {
    // DOCUMENTED LIMITATION — the SQLite begin-concurrent-wal2 branch
    // explicitly rejects the snapshot API in wal2 journal mode. See
    // vendor/sqlite3/sqlite3.c:72293 inside sqlite3WalSnapshotGet:
    //
    //     /* Snapshots may not be used with wal2 mode databases. */
    //     if( isWalMode2(pWal) ) return SQLITE_ERROR;
    //
    // This test pins that behaviour so we notice if an upstream bump ever
    // changes it. zero-cache's Snapshotter requires wal2 (asserted in
    // packages/zero-cache/src/services/view-syncer/snapshotter.ts), so
    // cross-connection snapshot sharing via sqlite3_snapshot_get/_open
    // is NOT usable for intra-CG parallel hydration. The coordination
    // has to come from a different mechanism (see add_queries_parallel
    // design in RustPipelineDriver).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal2.db");

    let writer = open_wal2(&path);
    writer
        .execute_batch("CREATE TABLE t(x INTEGER PRIMARY KEY);")
        .unwrap();
    writer.execute_batch("INSERT INTO t VALUES (1);").unwrap();

    let leader = Connection::open(&path).unwrap();
    leader.execute_batch("BEGIN").unwrap();
    assert_eq!(count(&leader), 1);
    let err = snapshot_get(&leader).err().expect("wal2 must reject snapshot_get");
    assert!(err.contains("rc=1"), "expected SQLITE_ERROR, got {err}");
}

#[test]
fn many_workers_all_see_same_snapshot_in_wal_mode() {
    // Same scenario as the wal2 case, but in plain WAL mode where the
    // snapshot API works. Documents the upper-bound of what IS reachable
    // via snapshot_get/open across connections if/when zero-cache ever
    // relaxes its WAL2 requirement.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("parallel_wal.db");

    let writer = open_wal(&path);
    writer
        .execute_batch("CREATE TABLE t(x INTEGER PRIMARY KEY);")
        .unwrap();
    for i in 0..10 {
        writer
            .execute_batch(&format!("INSERT INTO t VALUES ({i});"))
            .unwrap();
    }

    let leader = Connection::open(&path).unwrap();
    leader.execute_batch("BEGIN").unwrap();
    assert_eq!(count(&leader), 10);
    let snap = std::sync::Arc::new(snapshot_get(&leader).unwrap());

    // Between the snapshot and the workers opening it, the writer
    // commits more rows. Workers must not see these.
    for i in 10..20 {
        writer
            .execute_batch(&format!("INSERT INTO t VALUES ({i});"))
            .unwrap();
    }

    let path_str = path.to_string_lossy().into_owned();
    let mut handles = Vec::new();
    for worker_id in 0..8 {
        let path_clone = path_str.clone();
        let snap_clone = std::sync::Arc::clone(&snap);
        handles.push(std::thread::spawn(move || {
            let worker = Connection::open(&path_clone).unwrap();
            // Warm up WAL handle with a dummy read.
            let _: i64 = worker
                .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
                .unwrap();
            worker.execute_batch("BEGIN").unwrap();
            snapshot_open(&worker, &snap_clone)
                .unwrap_or_else(|e| panic!("worker {worker_id}: {e}"));
            let n = count(&worker);
            worker.execute_batch("COMMIT").unwrap();
            (worker_id, n)
        }));
    }

    for h in handles {
        let (worker_id, n) = h.join().unwrap();
        assert_eq!(
            n, 10,
            "worker {worker_id} must see exactly 10 rows (the leader's snapshot)",
        );
    }

    leader.execute_batch("COMMIT").unwrap();
}
