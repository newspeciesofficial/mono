//! Proof that a **single** `rusqlite::Connection` wrapped in
//! `Arc<Mutex<…>>` is a correct substrate for parallel hydration:
//!
//!  1. One leader Connection, `BEGIN CONCURRENT` in WAL2 mode,
//!     pinned to snapshot S.
//!  2. N rayon workers each take the mutex, do SELECTs, release — all
//!     workers see exactly snapshot S.
//!  3. A concurrent writer Connection commits new rows while the
//!     workers are running. Those new rows MUST NOT leak into worker
//!     queries because the leader's transaction holds S.
//!  4. CPU-bound post-SELECT work (simulated with a busy loop) overlaps
//!     across workers — mutex only guards the SQLite calls.
//!
//! If these pass we've verified the "shared connection pool" design
//! works for intra-CG parallel hydration on WAL2 without ever touching
//! `sqlite3_snapshot_get/_open`.

use rayon::prelude::*;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

fn open_wal2(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode = wal2", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal2");
    conn
}

#[test]
fn single_connection_shared_via_mutex_preserves_snapshot_across_workers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared.db");

    // Writer initialises the db with 100 rows.
    let writer = open_wal2(&path);
    writer
        .execute_batch("CREATE TABLE t(x INTEGER PRIMARY KEY);")
        .unwrap();
    for i in 0..100 {
        writer
            .execute_batch(&format!("INSERT INTO t VALUES ({i});"))
            .unwrap();
    }

    // Leader connection holds BEGIN CONCURRENT — pins to current LSN.
    // Workers share this Connection via a Mutex.
    let leader = open_wal2(&path);
    leader.execute_batch("BEGIN CONCURRENT").unwrap();
    let initial_count: i64 = leader
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(initial_count, 100);
    let shared = Arc::new(Mutex::new(leader));

    // Workers start issuing reads through the shared Connection.
    // In parallel, the writer thread commits additional rows that MUST
    // NOT be visible to the workers because the leader's txn still
    // holds the pre-commit snapshot.
    let path_str = path.to_string_lossy().into_owned();
    let writer_thread = thread::spawn(move || {
        let w = open_wal2(std::path::Path::new(&path_str));
        // Rapidly commit 10 more rows while workers run.
        for i in 100..110 {
            w.execute_batch(&format!("INSERT INTO t VALUES ({i});"))
                .unwrap();
            thread::sleep(std::time::Duration::from_millis(2));
        }
    });

    // 8 parallel workers — each does 10 rounds of SELECT + CPU work.
    let observations: Vec<Vec<i64>> = (0..8_u32)
        .into_par_iter()
        .map(|_worker_id| {
            let mut observed = Vec::with_capacity(10);
            for _ in 0..10 {
                let row_count = {
                    let conn = shared.lock().unwrap();
                    conn.query_row::<i64, _, _>("SELECT count(*) FROM t", [], |r| r.get(0))
                        .unwrap()
                };
                observed.push(row_count);
                // Simulate CPU-bound IVM work outside the lock — this is
                // the bit that actually parallelises.
                let mut n: u64 = 0;
                for i in 0..50_000 {
                    n = n.wrapping_add(i);
                }
                std::hint::black_box(n);
            }
            observed
        })
        .collect();

    writer_thread.join().unwrap();

    // Every observation from every worker must be 100 (the snapshot's
    // count), never 100 + something that the writer committed mid-test.
    for (wid, obs) in observations.iter().enumerate() {
        for (round, &n) in obs.iter().enumerate() {
            assert_eq!(
                n, 100,
                "worker {wid} round {round} saw {n} rows — snapshot leaked",
            );
        }
    }

    // Confirm the writer actually did commit: a fresh Connection with no
    // snapshot sees 110 now.
    let observer = open_wal2(&path);
    let committed: i64 = observer
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        committed, 110,
        "writer must have committed 10 additional rows",
    );

    // Release the leader's transaction.
    shared.lock().unwrap().execute_batch("COMMIT").unwrap();
}

#[test]
fn mutex_serialises_sqlite_but_cpu_work_parallelises() {
    // Measure that the mutex only serialises SQLite time, not
    // post-SELECT CPU time. A 4-worker run should be materially
    // faster than a serial run when the CPU chunk is >> SQLite chunk.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("perf.db");

    let w = open_wal2(&path);
    w.execute_batch("CREATE TABLE t(x INTEGER PRIMARY KEY);")
        .unwrap();
    for i in 0..100 {
        w.execute_batch(&format!("INSERT INTO t VALUES ({i});"))
            .unwrap();
    }

    let leader = open_wal2(&path);
    leader.execute_batch("BEGIN CONCURRENT").unwrap();
    let _: i64 = leader.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    let shared = Arc::new(Mutex::new(leader));

    // One unit of work: short SELECT (~0.03ms) + real CPU (~20ms).
    // Use `black_box` on BOTH the input and the accumulator to prevent
    // the release optimiser from removing the loop. SHA-style mixing
    // ensures the loop's work survives.
    let unit_of_work = |shared: &Arc<Mutex<Connection>>| -> u64 {
        let count: i64 = {
            let c = shared.lock().unwrap();
            c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap()
        };
        let mut n: u64 = std::hint::black_box(count as u64);
        for i in 0_u64..2_000_000 {
            n = n.wrapping_mul(6364136223846793005).wrapping_add(i);
            n ^= n >> 17;
            n = std::hint::black_box(n);
        }
        n
    };

    let n_units = 32;

    // Serial baseline — accumulate into a black-boxed sink so the
    // optimiser can't DCE the results.
    let t0 = Instant::now();
    let mut sink: u64 = 0;
    for _ in 0..n_units {
        sink ^= unit_of_work(&shared);
    }
    std::hint::black_box(sink);
    let serial_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Parallel via rayon (default = num_cores workers).
    let t0 = Instant::now();
    let par_sink: u64 = (0..n_units)
        .into_par_iter()
        .map(|_| unit_of_work(&shared))
        .reduce(|| 0, |a, b| a ^ b);
    std::hint::black_box(par_sink);
    let parallel_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Expect at least 2x speedup on any machine with ≥2 cores; the real
    // number is closer to N (num cores) for this ratio of CPU:SQLite
    // work. If this fails, the mutex is unexpectedly blocking CPU.
    let speedup = serial_ms / parallel_ms;
    eprintln!(
        "serial={serial_ms:.1}ms parallel={parallel_ms:.1}ms speedup={speedup:.2}x",
    );
    assert!(
        speedup > 1.8,
        "parallel should be materially faster than serial (got {speedup:.2}x)",
    );

    shared.lock().unwrap().execute_batch("COMMIT").unwrap();
}
