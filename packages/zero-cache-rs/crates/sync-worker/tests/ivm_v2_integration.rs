//! End-to-end integration: a real SQLite-backed chain wired from ivm_v2.
//!
//! Pipeline:
//!     TableSource (channel-based, real rusqlite)
//!       → Filter (predicate: flag=true)
//!       → Take (limit=3)
//!       → consumer iterates
//!
//! Plus a push-path test: caller pushes an Add change through Filter→Take
//! and verifies the emitted changes.

use rusqlite::{params, Connection};
use std::path::PathBuf;
use tempfile::TempDir;
use zero_cache_sync_worker::ivm::change::{AddChange, Change};
use zero_cache_sync_worker::ivm::data::{make_comparator, Node};
use zero_cache_sync_worker::ivm::schema::SourceSchema;
use zero_cache_sync_worker::ivm_v2::change::Node as V2Node;
use zero_cache_sync_worker::ivm_v2::filter::Filter;
use zero_cache_sync_worker::ivm_v2::operator::{FetchRequest, Input, InputBase, Operator};
use zero_cache_sync_worker::ivm_v2::source::ChannelSource;
use zero_cache_sync_worker::ivm_v2::take::Take;
use indexmap::IndexMap;
use serde_json::json;
use std::sync::Arc;
use zero_cache_types::ast::{Direction, Ordering, System};
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::Row;

fn setup_db(n: usize) -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, flag INTEGER NOT NULL, txt TEXT);",
    )
    .unwrap();
    for i in 0..n {
        // Alternate flag values so Filter has something to drop.
        let flag = if i % 2 == 0 { 1 } else { 0 };
        conn.execute(
            "INSERT INTO t VALUES(?1, ?2, ?3)",
            params![i as i64, flag, format!("row-{i}")],
        )
        .unwrap();
    }
    (dir, path)
}

fn schema() -> SourceSchema {
    let sort: Ordering = vec![("id".into(), Direction::Asc)];
    SourceSchema {
        table_name: "t".into(),
        columns: IndexMap::new(),
        primary_key: PrimaryKey::new(vec!["id".into()]),
        relationships: IndexMap::new(),
        is_hidden: false,
        system: System::Test,
        compare_rows: Arc::new(make_comparator(sort.clone(), false)),
        sort,
    }
}

/// Minimal adapter: wraps a `ChannelSource` query into an Input so
/// downstream operators can drive it.
///
/// One-shot: fetch() eagerly drains the channel on first call. Good
/// enough for integration proof; real wiring uses the channel lazily.
struct SourceInput {
    src: ChannelSource,
    sql: String,
    columns: Vec<String>,
    schema: SourceSchema,
}
impl SourceInput {
    fn new(path: PathBuf, sql: &str, columns: Vec<&str>, schema: SourceSchema) -> Self {
        Self {
            src: ChannelSource::new(path),
            sql: sql.to_string(),
            columns: columns.into_iter().map(String::from).collect(),
            schema,
        }
    }
}
impl InputBase for SourceInput {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {}
}
impl Input for SourceInput {
    fn fetch<'a>(
        &'a mut self,
        _req: FetchRequest,
    ) -> Box<dyn Iterator<Item = V2Node> + 'a> {
        let rx = self
            .src
            .query_rows(self.sql.clone(), vec![], self.columns.clone());
        // Convert SqlValue rows → ivm::data::Node (drop children).
        let nodes: Vec<V2Node> = rx
            .iter()
            .map(|owned_row| {
                let mut r = Row::new();
                for (k, v) in owned_row.into_iter() {
                    let jv = sql_value_to_json(v);
                    r.insert(k, jv);
                }
                V2Node {
                    row: r,
                    relationships: IndexMap::new(),
                }
            })
            .collect();
        Box::new(nodes.into_iter())
    }
}

fn sql_value_to_json(v: rusqlite::types::Value) -> Option<serde_json::Value> {
    match v {
        rusqlite::types::Value::Null => None,
        rusqlite::types::Value::Integer(i) => Some(json!(i)),
        rusqlite::types::Value::Real(f) => Some(json!(f)),
        rusqlite::types::Value::Text(s) => Some(json!(s)),
        rusqlite::types::Value::Blob(_) => None,
    }
}

#[test]
fn end_to_end_fetch_through_filter_and_take() {
    let (_dir, path) = setup_db(20);
    let source = SourceInput::new(
        path,
        "SELECT id, flag, txt FROM t ORDER BY id",
        vec!["id", "flag", "txt"],
        schema(),
    );
    let predicate: zero_cache_sync_worker::ivm_v2::filter::Predicate = Arc::new(|r: &Row| {
        matches!(r.get("flag"), Some(Some(v)) if v == &json!(1))
    });
    let filter = Filter::new(Box::new(source), predicate);
    let mut take = Take::new(Box::new(filter), 3);

    // Hydrate: fetch drives SQLite → Filter → Take.
    let rows: Vec<V2Node> = take.fetch(FetchRequest::default()).collect();
    assert_eq!(rows.len(), 3, "Take limit=3 with Filter should yield 3 rows");
    // First 3 flag-true rows are ids 0, 2, 4.
    assert_eq!(rows[0].row.get("id"), Some(&Some(json!(0))));
    assert_eq!(rows[1].row.get("id"), Some(&Some(json!(2))));
    assert_eq!(rows[2].row.get("id"), Some(&Some(json!(4))));
}

#[test]
fn end_to_end_push_through_filter_and_take() {
    // For push-side, we don't need a SQLite input — Take's push path
    // doesn't re-enter input fetch except on at-limit eviction. We use a
    // stub source instead to isolate the chain.
    struct Stub {
        s: SourceSchema,
    }
    impl InputBase for Stub {
        fn get_schema(&self) -> &SourceSchema {
            &self.s
        }
        fn destroy(&mut self) {}
    }
    impl Input for Stub {
        fn fetch<'a>(&'a mut self, _req: FetchRequest) -> Box<dyn Iterator<Item = V2Node> + 'a> {
            Box::new(std::iter::empty())
        }
    }
    let stub = Stub { s: schema() };
    let predicate: zero_cache_sync_worker::ivm_v2::filter::Predicate = Arc::new(|r: &Row| {
        matches!(r.get("flag"), Some(Some(v)) if v == &json!(1))
    });
    let filter = Filter::new(Box::new(stub), predicate);
    let mut take = Take::new(Box::new(filter), 2);

    // Push via Filter then Take manually (builder wiring comes later in #124).
    let change = |id: i64, flag: i64| {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("flag".into(), Some(json!(flag)));
        Change::Add(AddChange {
            node: V2Node {
                row: r,
                relationships: IndexMap::new(),
            },
        })
    };

    // The caller chains manually: for each change out of Filter, push to Take.
    let drive = |take: &mut Take, filter: &mut Filter, c: Change| -> Vec<Change> {
        let filter_emitted: Vec<Change> = filter.push(c).collect();
        let mut take_emitted = Vec::new();
        for c2 in filter_emitted {
            let mut emitted = take.push(c2).collect::<Vec<Change>>();
            take_emitted.append(&mut emitted);
        }
        take_emitted
    };

    // Rebuild for mutable chaining (Rust borrow rules prevent using take + filter
    // simultaneously via the wrapper above; instead, pull Filter inside Take's input).
    // We already did that via the Input trait — Filter is inside Take's input field.
    // So push needs to reach Filter via the top of the chain — the "caller"
    // feeds the TableSource side. But since Take owns Filter here, we need
    // a different wiring for push: a caller-visible entry point.
    //
    // For this test, push to Filter directly (we still own it conceptually) —
    // Instead, the simplest integration test here: push goes into Take's
    // embedded Filter via a dedicated top handle. But Take owns Filter.
    //
    // → The trait model assumes the CALLER orchestrates: takes changes from
    //   "some source of changes", feeds them through `filter.push()`, then
    //   through `take.push()`. The bottom input to Filter is a pull-side
    //   source (fetch only). This mirrors TS: push comes in at the top of
    //   the chain from the replicator, not from the TableSource.
    //
    // Rework: let take own filter, and filter own stub (pull source). Push
    // to Take directly by pushing Filter's emission first — but in ivm_v2
    // each operator's push method is independent; the caller composes them.
    // To fit that model we re-expose the chain flat:
    let _ = drive; // silence if unused

    // Simpler structure: keep filter and take as SEPARATE chain stages,
    // and feed changes through both manually.
    struct StubNoOp {
        s: SourceSchema,
    }
    impl InputBase for StubNoOp {
        fn get_schema(&self) -> &SourceSchema {
            &self.s
        }
        fn destroy(&mut self) {}
    }
    impl Input for StubNoOp {
        fn fetch<'a>(&'a mut self, _req: FetchRequest) -> Box<dyn Iterator<Item = V2Node> + 'a> {
            Box::new(std::iter::empty())
        }
    }
    let mut filter_top = Filter::new(
        Box::new(StubNoOp { s: schema() }),
        Arc::new(|r: &Row| matches!(r.get("flag"), Some(Some(v)) if v == &json!(1))),
    );
    let mut take_top = Take::new(Box::new(StubNoOp { s: schema() }), 2);

    // Push 5 changes, 3 with flag=1 and 2 with flag=0.
    let mut all_take_out: Vec<Change> = Vec::new();
    for (i, f) in [(1, 1), (2, 0), (3, 1), (4, 1), (5, 0)] {
        for fc in filter_top.push(change(i, f)) {
            for tc in take_top.push(fc) {
                all_take_out.push(tc);
            }
        }
    }

    // Filter passes ids 1, 3, 4 (all flag=1); Take limit=2 accepts 1 and 3,
    // drops 4 (since 4 > bound=3 under asc ordering).
    let kinds: Vec<_> = all_take_out
        .iter()
        .map(|c| match c {
            Change::Add(a) => ("Add", a.node.row.get("id").and_then(|v| v.clone())),
            Change::Remove(r) => ("Remove", r.node.row.get("id").and_then(|v| v.clone())),
            _ => ("Other", None),
        })
        .collect();

    // Expect two Add emissions: ids 1 and 3. Id 4 is dropped (beyond bound).
    assert_eq!(kinds.len(), 2, "expected 2 Add emissions from Take");
    assert_eq!(kinds[0], ("Add", Some(json!(1))));
    assert_eq!(kinds[1], ("Add", Some(json!(3))));

    // Silence the old closure.
    let _ = filter;
    let _ = take;
}
