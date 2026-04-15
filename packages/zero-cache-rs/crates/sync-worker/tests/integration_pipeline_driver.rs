//! End-to-end integration tests for `PipelineDriver`.
//!
//! These tests compose the real layers — `Snapshotter`, `DatabaseStorage`,
//! `TableSource`, `build_pipeline`, `PipelineDriver` — against a live
//! SQLite replica fixture. They complement the per-module unit tests by
//! verifying that the whole stack agrees on row keys, schemas, change
//! translation, and the hydrate/advance protocol.
//!
//! Fixture convention:
//! - Every test gets its own `TempDir` + replica DB file with the
//!   snapshotter-required bookkeeping tables (`_zero.replicationState`
//!   and `_zero.changeLog2`) plus a `users(id, name, age, _0_version)`
//!   table.
//! - The `TableSource` for `users` is opened against the **same replica
//!   file** (not in-memory), because `PipelineDriver::advance()` calls
//!   `TableSource::set_db` to swap each registered source onto a fresh
//!   connection on the snapshotter's current DB file. Using an in-memory
//!   source here would lose rows on the first `advance()`.

use std::sync::Arc;

use indexmap::IndexMap;
use rusqlite::{Connection, params};
use serde_json::json;
use tempfile::TempDir;

use zero_cache_sync_worker::builder::builder::BuilderDelegate;
use zero_cache_sync_worker::ivm::operator::Storage;
use zero_cache_sync_worker::ivm::source::Source;
use zero_cache_sync_worker::view_syncer::client_schema::{
    ClientColumnSchema, ClientSchema, ClientTableSchema, LiteAndZqlSpec, LiteColumnSpec,
    LiteTableSpec, LiteTableSpecWithKeys, ShardId, ZqlSchemaValue,
};
use zero_cache_sync_worker::view_syncer::pipeline_driver::{
    NoopInspectorDelegate, NoopTimer, PipelineDriver, PipelineDriverError, RowChange,
    RowChangeOrYield, Timer,
};
use zero_cache_sync_worker::view_syncer::snapshotter::Snapshotter;
use zero_cache_sync_worker::zqlite::database_storage::{DatabaseStorage, DatabaseStorageOptions};
use zero_cache_sync_worker::zqlite::table_source::{SchemaValue, TableSource, ValueType};
use zero_cache_types::ast::{
    AST, Condition, Direction, LiteralValue, NonColumnValue, SimpleOperator, ValuePosition,
};
use zero_cache_types::primary_key::PrimaryKey;

// ─── Fixture helpers ──────────────────────────────────────────────────

/// Create a replica DB file at `dir/replica.db` with:
/// - `_zero.replicationState` seeded at stateVersion = `01`.
/// - `_zero.changeLog2` (empty).
/// - `users(id INTEGER PRIMARY KEY, name TEXT, age INTEGER, _0_version TEXT NOT NULL)`.
fn make_replica(dir: &TempDir) -> String {
    let path = dir.path().join("replica.db");
    let p = path.to_string_lossy().into_owned();
    let conn = Connection::open(&p).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "_zero.replicationState" (
            stateVersion TEXT NOT NULL,
            lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );
        INSERT INTO "_zero.replicationState" (stateVersion, lock) VALUES ('01', 1);

        CREATE TABLE "_zero.changeLog2" (
            "stateVersion" TEXT NOT NULL,
            "pos" INT NOT NULL,
            "table" TEXT NOT NULL,
            "rowKey" TEXT NOT NULL,
            "op" TEXT NOT NULL,
            PRIMARY KEY("stateVersion", "pos")
        );

        CREATE TABLE users(
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            _0_version TEXT NOT NULL
        );
        "#,
    )
    .unwrap();
    p
}

/// Seed a user row into the replica file.
fn seed_user(replica_path: &str, id: i64, name: &str, age: i64, version: &str) {
    let c = Connection::open(replica_path).unwrap();
    c.execute(
        "INSERT INTO users(id, name, age, _0_version) VALUES(?, ?, ?, ?)",
        params![id, name, age, version],
    )
    .unwrap();
}

/// Update replicationState + append a changeLog row. The PipelineDriver's
/// snapshotter will see these on the next `advance()`.
fn bump_version_and_log(
    replica_path: &str,
    new_version: &str,
    pos: i64,
    table: &str,
    row_key_json: &str,
    op: &str,
) {
    let c = Connection::open(replica_path).unwrap();
    c.execute(
        r#"UPDATE "_zero.replicationState" SET stateVersion = ?"#,
        params![new_version],
    )
    .unwrap();
    c.execute(
        r#"INSERT INTO "_zero.changeLog2" (stateVersion, pos, "table", rowKey, op)
           VALUES (?, ?, ?, ?, ?)"#,
        params![new_version, pos, table, row_key_json, op],
    )
    .unwrap();
}

/// Apply the actual row mutation on the replica file (the DML the
/// replicator would have performed). The PipelineDriver's snapshotter
/// reads the post-mutation state when resolving changeLog entries.
fn apply_insert(replica_path: &str, id: i64, name: &str, age: i64, version: &str) {
    let c = Connection::open(replica_path).unwrap();
    c.execute(
        "INSERT INTO users(id, name, age, _0_version) VALUES(?, ?, ?, ?)",
        params![id, name, age, version],
    )
    .unwrap();
}

fn apply_delete(replica_path: &str, id: i64) {
    let c = Connection::open(replica_path).unwrap();
    c.execute("DELETE FROM users WHERE id = ?", params![id])
        .unwrap();
}

fn apply_update_name(replica_path: &str, id: i64, name: &str, version: &str) {
    let c = Connection::open(replica_path).unwrap();
    c.execute(
        "UPDATE users SET name = ?, _0_version = ? WHERE id = ?",
        params![name, version, id],
    )
    .unwrap();
}

#[allow(dead_code)]
fn apply_update_age(replica_path: &str, id: i64, age: i64, version: &str) {
    let c = Connection::open(replica_path).unwrap();
    c.execute(
        "UPDATE users SET age = ?, _0_version = ? WHERE id = ?",
        params![age, version, id],
    )
    .unwrap();
}

/// TableSource for `users` backed by an **in-memory** SQLite seeded
/// from the current replica state.
///
/// Why in-memory rather than the replica file directly:
///
/// In TS production, `TableSource` is handed the snapshotter's
/// `beginConcurrent()` connection on a WAL2 replica. That conn sees a
/// snapshot-isolated view of the prev state, and its writes (from
/// `push_change`) live inside the BEGIN CONCURRENT txn — invisible to
/// external writers and rolled back on `resetToHead`. During `advance()`
/// the snapshotter swaps this conn forward AFTER the push fan-out, so
/// `push_change`'s presence assertions (`row_exists` / `!row_exists`)
/// always see the prev snapshot.
///
/// The Rust port's snapshotter uses plain `BEGIN` (DEFERRED) on WAL
/// because `rusqlite`'s bundled SQLite ships without the wal2 +
/// begin-concurrent extension. Plain BEGIN DEFERRED on WAL cannot
/// upgrade to a write txn if an external writer has committed in the
/// meantime (SQLITE_BUSY_SNAPSHOT), so the TS strategy of
/// "writes-go-into-the-snapshot-txn" isn't portable.
///
/// The integration fixture therefore uses an in-memory SQLite handed
/// to `TableSource::new` and seeded with the current replica state. The
/// snapshotter's `advance()` → `swap_table_source_dbs()` (now deferred
/// to after push fan-out) swaps this in-memory handle out for a fresh
/// replica-file connection. During the push fan-out the source reads
/// from the pre-seed in-memory state — which does NOT reflect
/// subsequent `apply_insert/apply_delete/apply_update_*` calls — so the
/// presence assertions behave identically to the TS BEGIN CONCURRENT
/// path. This matches the existing unit-test pattern
/// (`advance_swaps_table_source_db` in `pipeline_driver.rs`).
fn make_users_source(replica_path: &str) -> Arc<TableSource> {
    // In-memory shadow DB that mirrors the replica's `users` table.
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"CREATE TABLE users(
             id INTEGER PRIMARY KEY,
             name TEXT,
             age INTEGER,
             _0_version TEXT NOT NULL
           );"#,
    )
    .unwrap();
    // Seed from the replica's current state (post-seed_user, pre-
    // apply_*).
    {
        let replica_conn = Connection::open(replica_path).unwrap();
        let mut stmt = replica_conn
            .prepare("SELECT id, name, age, _0_version FROM users")
            .unwrap();
        let rows: Vec<(i64, Option<String>, Option<i64>, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for (id, name, age, v) in rows {
            conn.execute(
                "INSERT INTO users(id, name, age, _0_version) VALUES(?, ?, ?, ?)",
                params![id, name, age, v],
            )
            .unwrap();
        }
    }
    let mut cols = IndexMap::new();
    cols.insert("id".into(), SchemaValue::new(ValueType::Number));
    cols.insert("name".into(), SchemaValue::new(ValueType::String));
    cols.insert("age".into(), SchemaValue::new(ValueType::Number));
    cols.insert("_0_version".into(), SchemaValue::new(ValueType::String));
    let ts = TableSource::new(conn, "users", cols, PrimaryKey::new(vec!["id".into()]))
        .expect("TableSource::new");
    Arc::new(ts)
}

fn users_client_schema() -> ClientSchema {
    let mut tables = IndexMap::new();
    let mut cols = IndexMap::new();
    cols.insert(
        "id".into(),
        ClientColumnSchema {
            r#type: "number".into(),
        },
    );
    cols.insert(
        "name".into(),
        ClientColumnSchema {
            r#type: "string".into(),
        },
    );
    cols.insert(
        "age".into(),
        ClientColumnSchema {
            r#type: "number".into(),
        },
    );
    tables.insert(
        "users".into(),
        ClientTableSchema {
            columns: cols,
            primary_key: Some(vec!["id".into()]),
        },
    );
    ClientSchema { tables }
}

fn users_table_specs() -> IndexMap<String, LiteAndZqlSpec> {
    let mut zql = IndexMap::new();
    zql.insert(
        "id".into(),
        ZqlSchemaValue {
            r#type: "number".into(),
        },
    );
    zql.insert(
        "name".into(),
        ZqlSchemaValue {
            r#type: "string".into(),
        },
    );
    zql.insert(
        "age".into(),
        ZqlSchemaValue {
            r#type: "number".into(),
        },
    );
    let mut specs = IndexMap::new();
    specs.insert(
        "users".into(),
        LiteAndZqlSpec {
            table_spec: LiteTableSpecWithKeys {
                all_potential_primary_keys: vec![vec!["id".into()]],
            },
            zql_spec: zql,
        },
    );
    specs
}

fn users_full_tables() -> IndexMap<String, LiteTableSpec> {
    let mut cols = IndexMap::new();
    cols.insert(
        "id".into(),
        LiteColumnSpec {
            data_type: "integer".into(),
        },
    );
    cols.insert(
        "name".into(),
        LiteColumnSpec {
            data_type: "text".into(),
        },
    );
    cols.insert(
        "age".into(),
        LiteColumnSpec {
            data_type: "integer".into(),
        },
    );
    cols.insert(
        "_0_version".into(),
        LiteColumnSpec {
            data_type: "text".into(),
        },
    );
    let mut out = IndexMap::new();
    out.insert("users".into(), LiteTableSpec { columns: cols });
    out
}

/// Assemble a `PipelineDriver` + replica path + a TableSource for
/// `users` backed by that replica. The source is already registered on
/// the driver. The driver is NOT yet `init()`ed — tests do that.
struct Fixture {
    driver: PipelineDriver,
    replica_path: String,
    #[allow(dead_code)]
    dir: TempDir,
}

fn new_fixture() -> Fixture {
    let dir = TempDir::new().unwrap();
    let replica_path = make_replica(&dir);
    let snapshotter = Snapshotter::new(replica_path.clone(), "my_app");
    let storage = DatabaseStorage::open_in_memory(DatabaseStorageOptions::default())
        .unwrap()
        .create_client_group_storage("cg1");
    let shard = ShardId {
        app_id: "my_app".into(),
        shard_num: 0,
    };
    let driver = PipelineDriver::new(
        snapshotter,
        shard,
        storage,
        "cg1",
        Arc::new(NoopInspectorDelegate),
        Arc::new(|| 1_000.0),
        false, // enable_planner
    );
    // NOTE: TableSource registration is deferred until
    // `init_users()` so that the source's BEGIN read-txn snapshots
    // the *post-seed* replica state — otherwise tests that seed rows
    // between `new_fixture()` and `add_query()` would observe an
    // empty replica through the source's snapshot.
    Fixture {
        driver,
        replica_path,
        dir,
    }
}

/// Init the driver with the users-only schema/specs, and register a
/// `TableSource` whose BEGIN read-txn snapshots the replica at this
/// moment. All `seed_user(...)` calls must precede this; all
/// `apply_insert / apply_delete / apply_update_*` calls that simulate
/// replicator DML must follow it (so they are invisible to the
/// source's snapshot until `advance()` swaps the connection forward).
fn init_users(fixture: &Fixture) {
    fixture
        .driver
        .init(
            &users_client_schema(),
            users_table_specs(),
            users_full_tables(),
        )
        .unwrap();
    let source = make_users_source(&fixture.replica_path);
    fixture.driver.register_table_source("users", source);
}

// ─── AST builders ─────────────────────────────────────────────────────

fn ast_users_all() -> AST {
    AST {
        schema: None,
        table: "users".into(),
        alias: None,
        where_clause: None,
        related: None,
        start: None,
        limit: None,
        order_by: None,
    }
}

fn ast_users_age_gt(threshold: f64) -> AST {
    AST {
        schema: None,
        table: "users".into(),
        alias: None,
        where_clause: Some(Box::new(Condition::Simple {
            op: SimpleOperator::Gt,
            left: ValuePosition::Column { name: "age".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(threshold),
            },
        })),
        related: None,
        start: None,
        limit: None,
        order_by: None,
    }
}

fn ast_users_top_n_by_age_desc(limit: u64) -> AST {
    AST {
        schema: None,
        table: "users".into(),
        alias: None,
        where_clause: None,
        related: None,
        start: None,
        limit: Some(limit),
        order_by: Some(vec![
            ("age".into(), Direction::Desc),
            ("id".into(), Direction::Asc),
        ]),
    }
}

// ─── Assertion helpers ────────────────────────────────────────────────

fn adds_only(
    rows: &[RowChangeOrYield],
) -> Vec<&zero_cache_sync_worker::view_syncer::pipeline_driver::RowAdd> {
    rows.iter()
        .filter_map(|r| match r {
            RowChangeOrYield::Change(RowChange::Add(a)) => Some(a),
            _ => None,
        })
        .collect()
}

fn ids_of_adds(rows: &[RowChangeOrYield]) -> Vec<i64> {
    adds_only(rows)
        .iter()
        .filter_map(|a| {
            a.row
                .get("id")
                .and_then(|v| v.as_ref())
                .and_then(|v| v.as_i64())
        })
        .collect()
}

// ─── Tests ────────────────────────────────────────────────────────────

/// Scenario 1: `hydrate_simple_select`.
///
/// Branch under test: `PipelineDriver::add_query` → `build_pipeline` with
/// no filter / no order / no limit → `TableSource::connect` → the
/// `fetch(FetchRequest::default())` path yields every row as a `RowAdd`.
#[test]
fn hydrate_simple_select() {
    let f = new_fixture();
    seed_user(&f.replica_path, 1, "alice", 30, "01");
    seed_user(&f.replica_path, 2, "bob", 25, "01");
    seed_user(&f.replica_path, 3, "carol", 40, "01");
    init_users(&f);

    let rows = f
        .driver
        .add_query("h_all", "q_all", ast_users_all(), &NoopTimer)
        .unwrap();

    let adds = adds_only(&rows);
    assert_eq!(adds.len(), 3, "expected three hydrated adds");
    let mut ids = ids_of_adds(&rows);
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
    for a in &adds {
        assert_eq!(a.query_id, "q_all");
        assert_eq!(a.table, "users");
    }
}

/// Scenario 2: `hydrate_with_where`.
///
/// Branch under test: `build_pipeline` compiles a `Filter` operator from
/// the `where` clause; only rows satisfying `age > 25` should emit.
#[test]
fn hydrate_with_where() {
    let f = new_fixture();
    seed_user(&f.replica_path, 1, "alice", 30, "01");
    seed_user(&f.replica_path, 2, "bob", 25, "01");
    seed_user(&f.replica_path, 3, "carol", 40, "01");
    init_users(&f);

    let rows = f
        .driver
        .add_query("h_w", "q_w", ast_users_age_gt(25.0), &NoopTimer)
        .unwrap();

    let mut ids = ids_of_adds(&rows);
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 3],
        "only age>25 rows (alice, carol) should emit"
    );
}

/// Scenario 3: `hydrate_with_order_by_and_limit`.
///
/// Branch under test: ORDER BY age DESC + LIMIT 2 — the `Take` operator
/// plus ordered `TableSource::connect` path should surface the top-2 in
/// descending-age order.
#[test]
fn hydrate_with_order_by_and_limit() {
    let f = new_fixture();
    seed_user(&f.replica_path, 1, "alice", 30, "01");
    seed_user(&f.replica_path, 2, "bob", 25, "01");
    seed_user(&f.replica_path, 3, "carol", 40, "01");
    seed_user(&f.replica_path, 4, "dave", 35, "01");
    init_users(&f);

    let rows = f
        .driver
        .add_query("h_ol", "q_ol", ast_users_top_n_by_age_desc(2), &NoopTimer)
        .unwrap();

    let ids = ids_of_adds(&rows);
    // DESC by age: carol(40), dave(35), alice(30), bob(25). LIMIT 2 → [3, 4].
    assert_eq!(
        ids,
        vec![3, 4],
        "top-2 by age DESC should be carol then dave"
    );
}

/// Scenario 4: `advance_propagates_insert`.
///
/// Branch under test: after hydration, append a new row to the replica
/// + changeLog2, `advance()` the driver, and verify a `RowAdd` emerges
/// through the IVM pipeline for the subscribed query.
///
/// IGNORED — incompatible interaction between the newly-landed
/// `PipelineDriver::swap_table_source_dbs` (which calls
/// `TableSource::set_db` with a fresh connection on the snapshotter's
/// curr DB file at the start of every `advance()`) and
/// `TableSource::push_change`, which asserts "Row already exists" when
/// the source DB already has the row. In TS each `TableSource` holds a
/// prev-snapshot read transaction that doesn't observe the replicator's
/// DML until the driver swaps it forward *after* push_change fans out;
/// in the Rust port the swap happens before the push loop, so the row
/// the snapshotter just read as `next_value` is already present in the
/// source's post-swap connection. Needs a lib fix: either defer the
/// swap to after the push fan-out, or swap to a read-only
/// prev-snapshot handle for pushes (mirroring TS) before the fan-out
/// and move to curr afterwards.
#[test]
fn advance_propagates_insert() {
    let f = new_fixture();
    seed_user(&f.replica_path, 1, "alice", 30, "01");
    init_users(&f);

    let hydrate_rows = f
        .driver
        .add_query("h", "q1", ast_users_all(), &NoopTimer)
        .unwrap();
    assert_eq!(ids_of_adds(&hydrate_rows), vec![1]);

    // Append row 2 + changeLog entry.
    apply_insert(&f.replica_path, 2, "bob", 25, "02");
    bump_version_and_log(
        &f.replica_path,
        "02",
        0,
        "users",
        r#"{"id":2}"#,
        "s", // 's' = set (insert/update) in snapshotter parlance
    );

    let res = f.driver.advance(&NoopTimer).unwrap();
    assert_eq!(res.version, "02");
    let adds = adds_only(&res.changes);
    assert_eq!(adds.len(), 1, "one new RowAdd for the inserted row");
    assert_eq!(
        adds[0]
            .row
            .get("id")
            .and_then(|v| v.as_ref())
            .and_then(|v| v.as_i64()),
        Some(2)
    );
    assert_eq!(adds[0].query_id, "q1");
}

/// Scenario 5: `advance_propagates_delete`.
///
/// Branch under test: a `d` changeLog op on a previously-hydrated row
/// should reach the subscribed query as a `RowRemove`.
///
/// IGNORED — same root cause as `advance_propagates_insert`. The
/// snapshotter reads the row before deletion via its prev snapshot,
/// surfaces it as `prev_values` in the SnapshotChange, and the driver
/// calls `source.push_change(Remove(row))`. After `swap_table_source_dbs`
/// the source is pointed at the curr DB where the row has already been
/// deleted, so `push_change` asserts "Row not found".
#[test]
fn advance_propagates_delete() {
    let f = new_fixture();
    seed_user(&f.replica_path, 1, "alice", 30, "01");
    seed_user(&f.replica_path, 2, "bob", 25, "01");
    init_users(&f);

    f.driver
        .add_query("h", "q1", ast_users_all(), &NoopTimer)
        .unwrap();

    apply_delete(&f.replica_path, 1);
    bump_version_and_log(&f.replica_path, "02", 0, "users", r#"{"id":1}"#, "d");

    let res = f.driver.advance(&NoopTimer).unwrap();
    let removes: Vec<_> = res
        .changes
        .iter()
        .filter_map(|r| match r {
            RowChangeOrYield::Change(RowChange::Remove(rem)) => Some(rem),
            _ => None,
        })
        .collect();
    assert_eq!(removes.len(), 1, "one RowRemove for deleted id=1");
    assert_eq!(removes[0].query_id, "q1");
    assert_eq!(
        removes[0]
            .row_key
            .get("id")
            .and_then(|v| v.as_ref())
            .and_then(|v| v.as_i64()),
        Some(1)
    );
}

/// Scenario 6: `advance_propagates_update`.
///
/// Branch under test: an `s` changeLog op on an existing row (same PK)
/// should emerge as a `RowEdit` when the query's predicate is preserved.
/// Because the simple-SELECT query has no predicate, the row stays in
/// scope and we expect `Edit` (not a split Add+Remove).
#[test]
fn advance_propagates_update() {
    let f = new_fixture();
    seed_user(&f.replica_path, 1, "alice", 30, "01");
    init_users(&f);

    f.driver
        .add_query("h", "q1", ast_users_all(), &NoopTimer)
        .unwrap();

    apply_update_name(&f.replica_path, 1, "alice2", "02");
    bump_version_and_log(&f.replica_path, "02", 0, "users", r#"{"id":1}"#, "s");

    let res = f.driver.advance(&NoopTimer).unwrap();
    // Accept either a single Edit or a Remove+Add pair — some IVM paths
    // decompose edits when split_edit_keys triggers. The observable
    // invariant is: the updated row with name=alice2 is reachable.
    let mut saw_alice2 = false;
    for ch in &res.changes {
        match ch {
            RowChangeOrYield::Change(RowChange::Edit(e)) => {
                if e.row
                    .get("name")
                    .and_then(|v| v.as_ref())
                    .and_then(|v| v.as_str())
                    == Some("alice2")
                {
                    saw_alice2 = true;
                }
            }
            RowChangeOrYield::Change(RowChange::Add(a)) => {
                if a.row
                    .get("name")
                    .and_then(|v| v.as_ref())
                    .and_then(|v| v.as_str())
                    == Some("alice2")
                {
                    saw_alice2 = true;
                }
            }
            _ => {}
        }
    }
    assert!(
        saw_alice2,
        "expected to see the updated alice2 row in advance output; got: {:?}",
        res.changes
    );
}

/// Scenario 7: `remove_query_cleans_up`.
///
/// Branch under test: after `remove_query`, subsequent replica mutations
/// should produce no output for the removed query id.
///
/// IGNORED — needs a working advance path; blocked on the same
/// swap/push_change incompatibility described in
/// `advance_propagates_insert`.
#[test]
fn remove_query_cleans_up() {
    let f = new_fixture();
    seed_user(&f.replica_path, 1, "alice", 30, "01");
    init_users(&f);

    f.driver
        .add_query("h", "q1", ast_users_all(), &NoopTimer)
        .unwrap();
    assert!(f.driver.queries().contains_key("q1"));

    f.driver.remove_query("q1");
    assert!(!f.driver.queries().contains_key("q1"));

    // Push a change that *would* have mattered if q1 were still live.
    apply_insert(&f.replica_path, 2, "bob", 25, "02");
    bump_version_and_log(&f.replica_path, "02", 0, "users", r#"{"id":2}"#, "s");

    let res = f.driver.advance(&NoopTimer).unwrap();
    let for_q1: Vec<_> = res
        .changes
        .iter()
        .filter_map(|r| match r {
            RowChangeOrYield::Change(c) if c.query_id() == "q1" => Some(c),
            _ => None,
        })
        .collect();
    assert!(
        for_q1.is_empty(),
        "no events should reference the removed q1; got {:?}",
        for_q1
    );
    // Idempotent removal.
    f.driver.remove_query("q1");
}

/// Scenario 8: `multi_query_isolation`.
///
/// Branch under test: two queries over the same table with different
/// predicates. A push that satisfies only one predicate must reach only
/// that query's subscribed output, and the fan-out through the shared
/// `TableSource` must not cross-contaminate.
///
/// IGNORED — blocked on the same advance-time swap/push_change
/// incompatibility. The hydration half of this test works (verified by
/// the sibling `hydrate_with_where` test); the advance half cannot run
/// until the lib fix lands.
#[test]
fn multi_query_isolation() {
    let f = new_fixture();
    seed_user(&f.replica_path, 1, "alice", 10, "01"); // young
    seed_user(&f.replica_path, 2, "bob", 50, "01"); // old
    init_users(&f);

    // q_young: age < 30; q_old: age > 30.
    let ast_young = AST {
        schema: None,
        table: "users".into(),
        alias: None,
        where_clause: Some(Box::new(Condition::Simple {
            op: SimpleOperator::Lt,
            left: ValuePosition::Column { name: "age".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(30.0),
            },
        })),
        related: None,
        start: None,
        limit: None,
        order_by: None,
    };
    let ast_old = ast_users_age_gt(30.0);

    let r_young = f
        .driver
        .add_query("hy", "q_young", ast_young, &NoopTimer)
        .unwrap();
    let r_old = f
        .driver
        .add_query("ho", "q_old", ast_old, &NoopTimer)
        .unwrap();
    assert_eq!(ids_of_adds(&r_young), vec![1], "q_young sees only young");
    assert_eq!(ids_of_adds(&r_old), vec![2], "q_old sees only old");

    // Insert a young user; only q_young should see it.
    apply_insert(&f.replica_path, 3, "carol", 15, "02");
    bump_version_and_log(&f.replica_path, "02", 0, "users", r#"{"id":3}"#, "s");
    let res = f.driver.advance(&NoopTimer).unwrap();
    let young_adds: Vec<_> = res
        .changes
        .iter()
        .filter_map(|r| match r {
            RowChangeOrYield::Change(RowChange::Add(a)) if a.query_id == "q_young" => Some(a),
            _ => None,
        })
        .collect();
    let old_adds: Vec<_> = res
        .changes
        .iter()
        .filter_map(|r| match r {
            RowChangeOrYield::Change(RowChange::Add(a)) if a.query_id == "q_old" => Some(a),
            _ => None,
        })
        .collect();
    assert_eq!(young_adds.len(), 1, "q_young gets the new young row");
    assert_eq!(
        young_adds[0]
            .row
            .get("id")
            .and_then(|v| v.as_ref())
            .and_then(|v| v.as_i64()),
        Some(3)
    );
    assert!(
        old_adds.is_empty(),
        "q_old must not see the young row; got {:?}",
        old_adds
    );
}

/// Scenario 9: `yield_cap_triggers_reset`.
///
/// Branch under test: `set_max_yields_per_advance(0)` combined with a
/// `Timer` whose `elapsed_lap()` is huge forces
/// `should_advance_yield_maybe_abort` to emit a yield sentinel that
/// exceeds the cap on the first change → `PipelineDriverError::Reset`.
#[test]
fn yield_cap_triggers_reset() {
    struct FastTimer;
    impl Timer for FastTimer {
        fn elapsed_lap(&self) -> f64 {
            1e9
        }
        fn total_elapsed(&self) -> f64 {
            0.0
        }
    }

    let f = new_fixture();
    seed_user(&f.replica_path, 1, "alice", 30, "01");
    init_users(&f);
    f.driver
        .add_query("h", "q1", ast_users_all(), &NoopTimer)
        .unwrap();
    f.driver.set_max_yields_per_advance(Some(0));

    // Generate a change so the advance loop has something to iterate.
    apply_delete(&f.replica_path, 1);
    bump_version_and_log(&f.replica_path, "02", 0, "users", r#"{"id":1}"#, "d");

    let err = f.driver.advance(&FastTimer).err();
    assert!(
        matches!(err, Some(PipelineDriverError::Reset(_))),
        "expected Reset signal; got {err:?}"
    );
}

// Keep `BuilderDelegate` + `Storage` + `Source` in scope so that if they
// get inadvertently removed from the crate's public surface, the test
// build surfaces it immediately.
#[allow(dead_code)]
fn _surface_check() {
    fn _assert_trait_items<B: BuilderDelegate, S: Storage + ?Sized, R: Source + ?Sized>() {}
    // We don't actually call this — existence at compile time is enough.
    let _ = json!(null);
}

// ─── JOIN subquery push propagation ───────────────────────────────────
//
// Models the xyne-observed flicker: messages table push fails to
// propagate through a `channelConversationsPaginatedV3`-shaped query
// because the join subquery's child input (TableSourceInput on
// `messages`) never has its output slot wired. Driven in isolation —
// no Docker, no napi — so iteration is cheap.
//
// Scenario: one query that selects channels and includes a related
// subquery over messages joined by channelId. Insert a message for
// an existing channel, advance, assert the query emits a RowAdd
// carrying the new message in its channel parent's relationship.

use zero_cache_sync_worker::view_syncer::client_schema::{
    ClientColumnSchema as ClientColumnSchemaJ, ClientTableSchema as ClientTableSchemaJ,
};
use zero_cache_types::ast::{CompoundKey, Correlation, CorrelatedSubquery};

fn make_replica_join(dir: &TempDir) -> String {
    let path = dir.path().join("replica.db");
    let p = path.to_string_lossy().into_owned();
    let conn = Connection::open(&p).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "_zero.replicationState" (
            stateVersion TEXT NOT NULL,
            lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );
        INSERT INTO "_zero.replicationState" (stateVersion, lock) VALUES ('01', 1);

        CREATE TABLE "_zero.changeLog2" (
            "stateVersion" TEXT NOT NULL,
            "pos" INT NOT NULL,
            "table" TEXT NOT NULL,
            "rowKey" TEXT NOT NULL,
            "op" TEXT NOT NULL,
            PRIMARY KEY("stateVersion", "pos")
        );

        CREATE TABLE channels(
            id INTEGER PRIMARY KEY,
            name TEXT,
            _0_version TEXT NOT NULL
        );
        CREATE TABLE messages(
            id INTEGER PRIMARY KEY,
            channelId INTEGER NOT NULL,
            content TEXT,
            _0_version TEXT NOT NULL
        );
        "#,
    )
    .unwrap();
    p
}

fn seed_channel(path: &str, id: i64, name: &str, v: &str) {
    let c = Connection::open(path).unwrap();
    c.execute(
        "INSERT INTO channels(id, name, _0_version) VALUES(?, ?, ?)",
        params![id, name, v],
    )
    .unwrap();
}

fn apply_insert_message(path: &str, id: i64, channel_id: i64, content: &str, v: &str) {
    let c = Connection::open(path).unwrap();
    c.execute(
        "INSERT INTO messages(id, channelId, content, _0_version) VALUES(?, ?, ?, ?)",
        params![id, channel_id, content, v],
    )
    .unwrap();
}

fn make_channels_source(replica_path: &str) -> Arc<TableSource> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"CREATE TABLE channels(
             id INTEGER PRIMARY KEY,
             name TEXT,
             _0_version TEXT NOT NULL
           );"#,
    )
    .unwrap();
    let rc = Connection::open(replica_path).unwrap();
    let mut stmt = rc
        .prepare("SELECT id, name, _0_version FROM channels")
        .unwrap();
    let rows: Vec<(i64, Option<String>, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for (id, name, v) in rows {
        conn.execute(
            "INSERT INTO channels(id, name, _0_version) VALUES(?, ?, ?)",
            params![id, name, v],
        )
        .unwrap();
    }
    let mut cols = IndexMap::new();
    cols.insert("id".into(), SchemaValue::new(ValueType::Number));
    cols.insert("name".into(), SchemaValue::new(ValueType::String));
    cols.insert("_0_version".into(), SchemaValue::new(ValueType::String));
    let ts = TableSource::new(conn, "channels", cols, PrimaryKey::new(vec!["id".into()]))
        .expect("TableSource::new channels");
    Arc::new(ts)
}

fn make_messages_source(replica_path: &str) -> Arc<TableSource> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"CREATE TABLE messages(
             id INTEGER PRIMARY KEY,
             channelId INTEGER NOT NULL,
             content TEXT,
             _0_version TEXT NOT NULL
           );"#,
    )
    .unwrap();
    let rc = Connection::open(replica_path).unwrap();
    let mut stmt = rc
        .prepare("SELECT id, channelId, content, _0_version FROM messages")
        .unwrap();
    let rows: Vec<(i64, i64, Option<String>, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for (id, cid, content, v) in rows {
        conn.execute(
            "INSERT INTO messages(id, channelId, content, _0_version) VALUES(?, ?, ?, ?)",
            params![id, cid, content, v],
        )
        .unwrap();
    }
    let mut cols = IndexMap::new();
    cols.insert("id".into(), SchemaValue::new(ValueType::Number));
    cols.insert("channelId".into(), SchemaValue::new(ValueType::Number));
    cols.insert("content".into(), SchemaValue::new(ValueType::String));
    cols.insert("_0_version".into(), SchemaValue::new(ValueType::String));
    let ts = TableSource::new(conn, "messages", cols, PrimaryKey::new(vec!["id".into()]))
        .expect("TableSource::new messages");
    Arc::new(ts)
}

fn two_table_client_schema() -> ClientSchema {
    let mut tables = IndexMap::new();

    let mut ch_cols = IndexMap::new();
    ch_cols.insert("id".into(), ClientColumnSchemaJ { r#type: "number".into() });
    ch_cols.insert("name".into(), ClientColumnSchemaJ { r#type: "string".into() });
    tables.insert(
        "channels".into(),
        ClientTableSchemaJ { columns: ch_cols, primary_key: Some(vec!["id".into()]) },
    );

    let mut msg_cols = IndexMap::new();
    msg_cols.insert("id".into(), ClientColumnSchemaJ { r#type: "number".into() });
    msg_cols.insert("channelId".into(), ClientColumnSchemaJ { r#type: "number".into() });
    msg_cols.insert("content".into(), ClientColumnSchemaJ { r#type: "string".into() });
    tables.insert(
        "messages".into(),
        ClientTableSchemaJ { columns: msg_cols, primary_key: Some(vec!["id".into()]) },
    );

    ClientSchema { tables }
}

fn two_table_specs() -> IndexMap<String, LiteAndZqlSpec> {
    let mut specs = IndexMap::new();

    let mut ch_zql = IndexMap::new();
    ch_zql.insert("id".into(), ZqlSchemaValue { r#type: "number".into() });
    ch_zql.insert("name".into(), ZqlSchemaValue { r#type: "string".into() });
    specs.insert(
        "channels".into(),
        LiteAndZqlSpec {
            table_spec: LiteTableSpecWithKeys {
                all_potential_primary_keys: vec![vec!["id".into()]],
            },
            zql_spec: ch_zql,
        },
    );

    let mut msg_zql = IndexMap::new();
    msg_zql.insert("id".into(), ZqlSchemaValue { r#type: "number".into() });
    msg_zql.insert("channelId".into(), ZqlSchemaValue { r#type: "number".into() });
    msg_zql.insert("content".into(), ZqlSchemaValue { r#type: "string".into() });
    specs.insert(
        "messages".into(),
        LiteAndZqlSpec {
            table_spec: LiteTableSpecWithKeys {
                all_potential_primary_keys: vec![vec!["id".into()]],
            },
            zql_spec: msg_zql,
        },
    );
    specs
}

fn two_table_full_tables() -> IndexMap<String, LiteTableSpec> {
    let mut out = IndexMap::new();

    let mut ch_cols = IndexMap::new();
    ch_cols.insert("id".into(), LiteColumnSpec { data_type: "integer".into() });
    ch_cols.insert("name".into(), LiteColumnSpec { data_type: "text".into() });
    ch_cols.insert("_0_version".into(), LiteColumnSpec { data_type: "text".into() });
    out.insert("channels".into(), LiteTableSpec { columns: ch_cols });

    let mut msg_cols = IndexMap::new();
    msg_cols.insert("id".into(), LiteColumnSpec { data_type: "integer".into() });
    msg_cols.insert("channelId".into(), LiteColumnSpec { data_type: "integer".into() });
    msg_cols.insert("content".into(), LiteColumnSpec { data_type: "text".into() });
    msg_cols.insert("_0_version".into(), LiteColumnSpec { data_type: "text".into() });
    out.insert("messages".into(), LiteTableSpec { columns: msg_cols });

    out
}

/// Scenario JOIN-1: `advance_propagates_join_subquery_insert`.
///
/// Mirror of the xyne message-flicker scenario, in isolation.
///
/// Query: `SELECT * FROM channels` with related subquery
/// `messages WHERE channelId = channels.id` aliased as `msgs`.
///
/// Actions:
/// 1. Seed channel id=1.
/// 2. Initialise driver + register `channels` and `messages` sources.
/// 3. `add_query(q1)` — hydration yields the seeded channel with an
///    empty `msgs` relationship.
/// 4. Apply INSERT of a new message for channel 1 + append changeLog.
/// 5. `advance()` and assert: an `edit` RowChange for the channel row
///    (whose relationships now include the new message) OR an Add
///    surfacing the message downstream. Either way, the row must NOT
///    be silently dropped — the regression symptom is an empty
///    `res.changes` from the advance.
#[test]
fn advance_propagates_join_subquery_insert() {
    let dir = TempDir::new().unwrap();
    let replica_path = make_replica_join(&dir);
    seed_channel(&replica_path, 1, "general", "01");

    let snapshotter = Snapshotter::new(replica_path.clone(), "my_app");
    let storage = DatabaseStorage::open_in_memory(DatabaseStorageOptions::default())
        .unwrap()
        .create_client_group_storage("cg1");
    let shard = ShardId {
        app_id: "my_app".into(),
        shard_num: 0,
    };
    let driver = PipelineDriver::new(
        snapshotter,
        shard,
        storage,
        "cg1",
        Arc::new(NoopInspectorDelegate),
        Arc::new(|| 1_000.0),
        false,
    );

    driver
        .init(
            &two_table_client_schema(),
            two_table_specs(),
            two_table_full_tables(),
        )
        .unwrap();

    driver.register_table_source("channels", make_channels_source(&replica_path));
    driver.register_table_source("messages", make_messages_source(&replica_path));

    // AST: channels with related `msgs` subquery over messages.channelId = channels.id
    let sub_ast = AST {
        schema: None,
        table: "messages".into(),
        alias: Some("msgs".into()),
        where_clause: None,
        related: None,
        start: None,
        limit: None,
        order_by: None,
    };
    let ast = AST {
        schema: None,
        table: "channels".into(),
        alias: None,
        where_clause: None,
        related: Some(vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: Vec::from(["id".into()]),
                child_field: Vec::from(["channelId".into()]),
            },
            subquery: Box::new(sub_ast),
            system: None,
            hidden: None,
        }]),
        start: None,
        limit: None,
        order_by: None,
    };

    let hydrate = driver.add_query("h", "q1", ast, &NoopTimer).unwrap();
    // Seeded channel must appear in hydration.
    assert!(
        !adds_only(&hydrate).is_empty(),
        "hydration must emit at least one Add"
    );

    // Simulate the replicator's DML + changeLog append for a new
    // message on channel 1.
    apply_insert_message(&replica_path, 42, 1, "hello", "02");
    bump_version_and_log(
        &replica_path,
        "02",
        0,
        "messages",
        r#"{"id":42}"#,
        "s",
    );

    let res = driver.advance(&NoopTimer).unwrap();

    // The bug: res.changes is empty — the messages push was dropped
    // by unwired operator back-edges, so the query sees no change.
    //
    // The fix: the push propagates through the join pipeline. The
    // observable surface is either (a) an edit on the channel row
    // with the message in its `msgs` relationship, or (b) a direct
    // add/edit/remove referencing the new message row. Either one
    // demonstrates the push flow is wired end-to-end.
    assert!(
        !res.changes.is_empty(),
        "advance must emit at least one row change for the message \
         push propagating through the join subquery; got empty — \
         operator back-edges are still unwired for this path",
    );
}
