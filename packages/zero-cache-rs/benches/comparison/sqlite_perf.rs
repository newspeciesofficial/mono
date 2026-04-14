//! Head-to-head benchmark: Rust SQLite operations vs baseline.
//!
//! Tests the operations that matter most in zero-cache:
//! 1. Change log writes (replicator hot path)
//! 2. Change log reads (IVM advance hot path)
//! 3. Concurrent snapshot reads (ViewSyncer pattern)
//! 4. Operator storage CRUD (IVM state)
//! 5. Row serialization/deserialization
//!
//! Run: cargo bench -p zero-cache-types --bench sqlite_perf

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rusqlite::{Connection, params};
use std::sync::{Arc, Barrier, Mutex};

fn setup_replica_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA cache_size = -8000;
         PRAGMA mmap_size = 268435456;",
    )
    .unwrap();

    // Create replica tables matching zero-cache schema
    conn.execute_batch(
        r#"
        CREATE TABLE "_zero.replicationState" (
            stateVersion TEXT NOT NULL,
            lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );
        INSERT INTO "_zero.replicationState" (stateVersion) VALUES ('00');

        CREATE TABLE "_zero.changeLog2" (
            "stateVersion" TEXT NOT NULL,
            "pos" INT NOT NULL,
            "table" TEXT NOT NULL,
            "rowKey" TEXT NOT NULL,
            "op" TEXT NOT NULL,
            "backfillingColumnVersions" TEXT DEFAULT '{}',
            PRIMARY KEY("stateVersion", "pos")
        );
        CREATE INDEX "_zero.changeLog2_table_rowKey"
            ON "_zero.changeLog2" ("table", "rowKey");

        CREATE TABLE "user" (
            id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER,
            bio TEXT,
            created_at TEXT,
            updated_at TEXT,
            _0_version TEXT
        );
        "#,
    )
    .unwrap();

    // Seed 10,000 rows
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut stmt = tx
            .prepare("INSERT INTO \"user\" (id, name, email, age, bio, created_at, updated_at, _0_version) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")
            .unwrap();
        for i in 0..10_000 {
            stmt.execute(params![
                format!("user-{i:05}"),
                format!("User {i}"),
                format!("user{i}@example.com"),
                20 + (i % 50),
                format!("Bio for user {i} with some extra text to simulate realistic row sizes"),
                "2024-01-01T00:00:00Z",
                "2024-06-15T12:00:00Z",
                "00",
            ])
            .unwrap();
        }
    }
    tx.commit().unwrap();

    conn
}

fn setup_operator_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
         PRAGMA synchronous = OFF;
         PRAGMA locking_mode = EXCLUSIVE;
         CREATE TABLE storage (
            clientGroupID TEXT,
            op NUMBER,
            key TEXT,
            val TEXT,
            PRIMARY KEY(clientGroupID, op, key)
         );",
    )
    .unwrap();
    conn
}

// ---------------------------------------------------------------------------
// Benchmark 1: Change log writes (replicator hot path)
// ---------------------------------------------------------------------------
fn bench_changelog_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("changelog_writes");

    for batch_size in [1, 10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("batch", batch_size),
            &batch_size,
            |b, &size| {
                let conn = setup_replica_db();
                let mut version = 1u64;

                b.iter(|| {
                    let v = format!("{version:08x}");
                    let tx = conn.unchecked_transaction().unwrap();
                    {
                        let mut stmt = tx
                            .prepare_cached(
                                r#"INSERT OR REPLACE INTO "_zero.changeLog2"
                                   ("stateVersion","pos","table","rowKey","op","backfillingColumnVersions")
                                   VALUES (?1,?2,?3,?4,?5,'{}')"#,
                            )
                            .unwrap();
                        for i in 0..size {
                            stmt.execute(params![
                                &v,
                                i as i64,
                                "user",
                                format!(r#"{{"id":"user-{:05}"}}"#, i),
                                "s",
                            ])
                            .unwrap();
                        }
                    }
                    tx.execute(
                        r#"UPDATE "_zero.replicationState" SET stateVersion = ?1"#,
                        params![&v],
                    )
                    .unwrap();
                    tx.commit().unwrap();
                    version += 1;
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 2: Change log reads (IVM advance - read changes since version)
// ---------------------------------------------------------------------------
fn bench_changelog_reads(c: &mut Criterion) {
    let conn = setup_replica_db();

    // Insert 1000 changes across 100 versions
    let tx = conn.unchecked_transaction().unwrap();
    for v in 1..=100u64 {
        let version = format!("{v:08x}");
        for pos in 0..10 {
            tx.execute(
                r#"INSERT INTO "_zero.changeLog2"
                   ("stateVersion","pos","table","rowKey","op","backfillingColumnVersions")
                   VALUES (?1,?2,'user',?3,'s','{}')"#,
                params![
                    &version,
                    pos,
                    format!(r#"{{"id":"user-{:05}"}}"#, v * 10 + pos)
                ],
            )
            .unwrap();
        }
    }
    tx.commit().unwrap();

    let mut group = c.benchmark_group("changelog_reads");

    // Read all changes since version 0 (1000 entries)
    group.bench_function("read_1000_changes", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare_cached(
                    r#"SELECT "stateVersion","pos","table","rowKey","op"
                       FROM "_zero.changeLog2"
                       WHERE "stateVersion" > ?1
                       ORDER BY "stateVersion","pos""#,
                )
                .unwrap();
            let rows: Vec<(String, i64, String, String, String)> = stmt
                .query_map(params!["00000000"], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            black_box(rows.len())
        });
    });

    // Read changes for last 10 versions only (100 entries)
    group.bench_function("read_100_changes", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare_cached(
                    r#"SELECT "stateVersion","pos","table","rowKey","op"
                       FROM "_zero.changeLog2"
                       WHERE "stateVersion" > ?1
                       ORDER BY "stateVersion","pos""#,
                )
                .unwrap();
            let since = format!("{:08x}", 90u64);
            let rows: Vec<(String, i64, String, String, String)> = stmt
                .query_map(params![&since], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            black_box(rows.len())
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 3: Row reads (hydration — SELECT * from table)
// ---------------------------------------------------------------------------
fn bench_row_reads(c: &mut Criterion) {
    let conn = setup_replica_db();
    let mut group = c.benchmark_group("row_reads");

    // Full table scan (10k rows)
    group.bench_function("select_all_10k_rows", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, name, email, age, bio, created_at, updated_at FROM \"user\"",
                )
                .unwrap();
            let rows: Vec<(String, String, String, i64, String, String, String)> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            black_box(rows.len())
        });
    });

    // Primary key lookup (single row)
    group.bench_function("pk_lookup_single_row", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare_cached("SELECT id, name, email, age FROM \"user\" WHERE id = ?1")
                .unwrap();
            let row: (String, String, String, i64) = stmt
                .query_row(params!["user-05000"], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .unwrap();
            black_box(row)
        });
    });

    // Range query with LIMIT (pagination)
    group.bench_function("range_query_limit_50", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, name, email, age FROM \"user\" WHERE age > ?1 ORDER BY name LIMIT 50",
                )
                .unwrap();
            let rows: Vec<(String, String, String, i64)> = stmt
                .query_map(params![30], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            black_box(rows.len())
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 4: Operator storage CRUD (IVM state)
// ---------------------------------------------------------------------------
fn bench_operator_storage(c: &mut Criterion) {
    let mut group = c.benchmark_group("operator_storage");

    group.bench_function("set_1000_keys", |b| {
        let conn = setup_operator_db();
        let mut stmt = conn
            .prepare("INSERT OR REPLACE INTO storage (clientGroupID, op, key, val) VALUES (?1, ?2, ?3, ?4)")
            .unwrap();

        b.iter(|| {
            for i in 0..1000 {
                stmt.execute(params!["cg-1", 0, format!("key-{i}"), format!(r#"{{"v":{i}}}"#)])
                    .unwrap();
            }
        });
    });

    group.bench_function("get_1000_keys", |b| {
        let conn = setup_operator_db();
        // Pre-populate
        {
            let mut stmt = conn
                .prepare(
                    "INSERT INTO storage (clientGroupID, op, key, val) VALUES (?1, ?2, ?3, ?4)",
                )
                .unwrap();
            for i in 0..1000 {
                stmt.execute(params![
                    "cg-1",
                    0,
                    format!("key-{i}"),
                    format!(r#"{{"v":{i}}}"#)
                ])
                .unwrap();
            }
        }

        b.iter(|| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT val FROM storage WHERE clientGroupID = ?1 AND op = ?2 AND key = ?3",
                )
                .unwrap();
            for i in 0..1000 {
                let val: String = stmt
                    .query_row(params!["cg-1", 0, format!("key-{i}")], |r| r.get(0))
                    .unwrap();
                black_box(val);
            }
        });
    });

    group.bench_function("scan_1000_keys", |b| {
        let conn = setup_operator_db();
        {
            let mut stmt = conn
                .prepare("INSERT INTO storage (clientGroupID, op, key, val) VALUES (?1, ?2, ?3, ?4)")
                .unwrap();
            for i in 0..1000 {
                stmt.execute(params!["cg-1", 0, format!("key-{i:04}"), format!(r#"{{"v":{i}}}"#)])
                    .unwrap();
            }
        }

        b.iter(|| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT key, val FROM storage WHERE clientGroupID = ?1 AND op = ?2 ORDER BY key",
                )
                .unwrap();
            let rows: Vec<(String, String)> = stmt
                .query_map(params!["cg-1", 0], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            black_box(rows.len())
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 5: Concurrent reads (multiple threads reading same DB)
// ---------------------------------------------------------------------------
fn bench_concurrent_reads(c: &mut Criterion) {
    // Create a file-based DB for real WAL concurrency
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap().to_string();

    // Set up and seed
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE \"user\" (
                id TEXT PRIMARY KEY, name TEXT, email TEXT, age INTEGER, _0_version TEXT
             );",
        )
        .unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO \"user\" VALUES (?1, ?2, ?3, ?4, '00')")
                .unwrap();
            for i in 0..10_000 {
                stmt.execute(params![
                    format!("user-{i:05}"),
                    format!("User {i}"),
                    format!("u{i}@test.com"),
                    20 + (i % 50),
                ])
                .unwrap();
            }
        } // stmt dropped here
        tx.commit().unwrap();
    }

    let mut group = c.benchmark_group("concurrent_reads");

    for num_readers in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("readers", num_readers),
            &num_readers,
            |b, &n| {
                // Open N read connections
                let conns: Vec<Arc<Mutex<Connection>>> = (0..n)
                    .map(|_| {
                        let c = Connection::open(&path).unwrap();
                        c.execute_batch("PRAGMA journal_mode = WAL; PRAGMA query_only = ON;")
                            .unwrap();
                        Arc::new(Mutex::new(c))
                    })
                    .collect();

                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(n));
                    let handles: Vec<_> = (0..n)
                        .map(|i| {
                            let conn = conns[i].clone();
                            let barrier = barrier.clone();
                            std::thread::spawn(move || {
                                barrier.wait();
                                let conn = conn.lock().unwrap();
                                let mut stmt = conn
                                    .prepare_cached("SELECT COUNT(*) FROM \"user\" WHERE age > ?1")
                                    .unwrap();
                                let count: i64 = stmt.query_row(params![30], |r| r.get(0)).unwrap();
                                black_box(count)
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark 6: Memory — measure allocation for typical operations
// ---------------------------------------------------------------------------
fn bench_memory_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory");

    // Allocate + serialize + drop a batch of 20 RowChanges (mimics pool thread batch)
    group.bench_function("alloc_batch_20_row_changes", |b| {
        b.iter(|| {
            let mut batch = Vec::with_capacity(20);
            for i in 0..20 {
                let mut row_key = indexmap::IndexMap::new();
                row_key.insert(
                    "id".to_string(),
                    Some(serde_json::Value::String(format!("user-{i}"))),
                );
                let mut row = indexmap::IndexMap::new();
                row.insert(
                    "id".to_string(),
                    Some(serde_json::Value::String(format!("user-{i}"))),
                );
                row.insert(
                    "name".to_string(),
                    Some(serde_json::Value::String(format!("Name {i}"))),
                );
                row.insert(
                    "email".to_string(),
                    Some(serde_json::Value::String(format!("user{i}@test.com"))),
                );
                batch.push((row_key, row));
            }
            let json = serde_json::to_string(black_box(&batch)).unwrap();
            black_box(json.len())
        });
    });

    // Arc clone (simulates zero-copy channel send)
    group.bench_function("arc_clone_batch", |b| {
        let batch: Arc<Vec<String>> = Arc::new((0..20).map(|i| format!("row-{i}")).collect());
        b.iter(|| {
            let cloned = Arc::clone(black_box(&batch));
            black_box(cloned.len())
        });
    });

    group.finish();
}

// Arc already imported above; tempfile used in bench_concurrent_reads

criterion_group!(
    benches,
    bench_changelog_writes,
    bench_changelog_reads,
    bench_row_reads,
    bench_operator_storage,
    bench_concurrent_reads,
    bench_memory_patterns,
);
criterion_main!(benches);
