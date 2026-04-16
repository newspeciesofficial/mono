//! End-to-end PipelineV2 driver test:
//! init → add_query → advance → remove_query → destroy.

use indexmap::IndexMap;
use serde_json::json;
use std::sync::Arc;
use zero_cache_sync_worker::ivm::change::{AddChange, Change, RemoveChange};
use zero_cache_sync_worker::ivm::data::{make_comparator, Node};
use zero_cache_sync_worker::ivm::schema::SourceSchema;
use zero_cache_sync_worker::ivm_v2::filter_t::Predicate;
use zero_cache_sync_worker::view_syncer_v2::driver::{AddQueryReq, PipelineV2, SourceFactory};
use zero_cache_sync_worker::view_syncer_v2::pipeline::{ChainSpec, InMemoryInput};
use zero_cache_sync_worker::view_syncer_v2::row_change::RowChange;
use zero_cache_types::ast::{Direction, Ordering, System};
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::Row;

fn schema(table: &str) -> SourceSchema {
    let sort: Ordering = vec![("id".into(), Direction::Asc)];
    SourceSchema {
        table_name: table.into(),
        columns: IndexMap::new(),
        primary_key: PrimaryKey::new(vec!["id".into()]),
        relationships: IndexMap::new(),
        is_hidden: false,
        system: System::Test,
        compare_rows: Arc::new(make_comparator(sort.clone(), false)),
        sort,
    }
}

fn row(id: i64) -> Row {
    let mut r = Row::new();
    r.insert("id".into(), Some(json!(id)));
    r.insert("flag".into(), Some(json!(true)));
    r
}

fn node_of(r: Row) -> Node {
    Node {
        row: r,
        relationships: IndexMap::new(),
    }
}

#[test]
fn driver_end_to_end_hydrate_advance_remove() {
    // Source factory returns different InMemoryInputs based on table name.
    let factory: SourceFactory = Box::new(|table: &str| -> Box<dyn zero_cache_sync_worker::ivm_v2::operator::Input> {
        let rows: Vec<Row> = match table {
            "messages" => (1..=5).map(row).collect(),
            _ => vec![],
        };
        Box::new(InMemoryInput::new(rows, schema(table)))
    });
    let mut driver = PipelineV2::new(factory);
    driver.init("0000".into());
    assert!(driver.initialized());
    assert_eq!(driver.replica_version(), Some("0000"));

    // Add query: SELECT FROM messages WHERE flag=true LIMIT 3.
    let predicate: Predicate = Arc::new(|r: &Row| {
        matches!(r.get("flag"), Some(Some(v)) if v == &json!(true))
    });
    let spec = ChainSpec {
        query_id: "q1".into(),
        table: "messages".into(),
        primary_key: PrimaryKey::new(vec!["id".into()]),
        predicate: Some(predicate),
        skip_bound: None,
        limit: Some(3),
        exists: None,
        exists_chain: vec![],
        join: None,
    };
    let hydrated = driver.add_query("hash_1".into(), spec);
    assert_eq!(hydrated.len(), 3);
    for rc in &hydrated {
        assert!(matches!(rc, RowChange::Add(_)));
    }
    assert_eq!(driver.queries().len(), 1);

    // Advance: push an Add for messages table. Should be dropped by Take
    // (size already at limit, new row id=100 > bound).
    let out = driver.advance(
        "messages",
        Change::Add(AddChange {
            node: node_of(row(100)),
        }),
    );
    assert!(
        out.is_empty(),
        "id=100 is beyond bound, Take should drop it"
    );

    // Advance: id=0 is better than bound (id=3), should evict.
    let out = driver.advance(
        "messages",
        Change::Add(AddChange {
            node: node_of(row(0)),
        }),
    );
    assert_eq!(out.len(), 2, "expect Remove(bound)+Add(new)");
    assert!(matches!(out[0], RowChange::Remove(_)));
    assert!(matches!(out[1], RowChange::Add(_)));

    // Advance for a table nothing cares about — no-op.
    let out = driver.advance(
        "conversations",
        Change::Remove(RemoveChange {
            node: node_of(row(9)),
        }),
    );
    assert!(out.is_empty());

    // Remove the query.
    driver.remove_query("q1");
    assert_eq!(driver.queries().len(), 0);

    // Destroy — no further API calls after this except via a new driver.
    driver.destroy();
    assert!(!driver.initialized());
}

#[test]
fn driver_add_queries_batch_and_budget_breakdown() {
    let factory: SourceFactory = Box::new(|table: &str| -> Box<dyn zero_cache_sync_worker::ivm_v2::operator::Input> {
        let rows: Vec<Row> = match table {
            "messages" => (1..=3).map(row).collect(),
            "users" => (1..=2).map(row).collect(),
            _ => vec![],
        };
        Box::new(InMemoryInput::new(rows, schema(table)))
    });
    let mut driver = PipelineV2::new(factory);
    driver.init("0000".into());

    let batch = vec![
        AddQueryReq {
            transformation_hash: "h_messages".into(),
            spec: ChainSpec {
                query_id: "q_m".into(),
                table: "messages".into(),
                primary_key: PrimaryKey::new(vec!["id".into()]),
                predicate: None,
                skip_bound: None,
                limit: None,
                exists: None,
                exists_chain: vec![],
                join: None,
            },
        },
        AddQueryReq {
            transformation_hash: "h_users".into(),
            spec: ChainSpec {
                query_id: "q_u".into(),
                table: "users".into(),
                primary_key: PrimaryKey::new(vec!["id".into()]),
                predicate: None,
                skip_bound: None,
                limit: None,
                exists: None,
                exists_chain: vec![],
                join: None,
            },
        },
    ];

    let results = driver.add_queries(batch);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].query_id, "q_m");
    assert_eq!(results[0].hydrated.len(), 3);
    assert_eq!(results[1].query_id, "q_u");
    assert_eq!(results[1].hydrated.len(), 2);

    let breakdown = driver.hydration_budget_breakdown();
    assert_eq!(breakdown.len(), 2);
    assert!(breakdown.iter().any(|b| b.id == "q_m" && b.table == "messages"));
    assert!(breakdown.iter().any(|b| b.id == "q_u" && b.table == "users"));

    let total = driver.total_hydration_time_ms();
    assert!(total >= 0.0);
}

#[test]
fn driver_get_row_lookup() {
    let factory: SourceFactory = Box::new(|table: &str| -> Box<dyn zero_cache_sync_worker::ivm_v2::operator::Input> {
        let rows: Vec<Row> = match table {
            "messages" => (1..=5).map(row).collect(),
            _ => vec![],
        };
        Box::new(InMemoryInput::new(rows, schema(table)))
    });
    let mut driver = PipelineV2::new(factory);
    driver.init("0000".into());

    // Build a PK-only row for lookup.
    let mut pk = Row::new();
    pk.insert("id".into(), Some(json!(3)));
    let found = driver.get_row("messages", &pk);
    assert!(found.is_some());
    let row_found = found.unwrap();
    assert_eq!(row_found.get("id"), Some(&Some(json!(3))));

    let mut missing_pk = Row::new();
    missing_pk.insert("id".into(), Some(json!(999)));
    assert!(driver.get_row("messages", &missing_pk).is_none());

    assert!(driver.get_row("no_such_table", &pk).is_some() == false);
}

#[test]
fn driver_advance_without_diff_bumps_version() {
    let factory: SourceFactory = Box::new(|_: &str| -> Box<dyn zero_cache_sync_worker::ivm_v2::operator::Input> {
        Box::new(InMemoryInput::new(vec![], schema("messages")))
    });
    let mut driver = PipelineV2::new(factory);
    driver.init("0000".into());
    assert_eq!(driver.current_version(), Some("0000"));
    driver.advance_without_diff("0001".into());
    assert_eq!(driver.current_version(), Some("0001"));
}
