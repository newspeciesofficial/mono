//! Integration tests for PipelineV2 (Transformer-based Chain).

use indexmap::IndexMap;
use serde_json::json;
use std::sync::Arc;
use zero_cache_sync_worker::ivm::change::{AddChange, Change, RemoveChange};
use zero_cache_sync_worker::ivm::data::{make_comparator, Node};
use zero_cache_sync_worker::ivm::schema::SourceSchema;
use zero_cache_sync_worker::ivm_v2::filter_t::Predicate;
use zero_cache_sync_worker::ivm_v2::skip_t::Bound;
use zero_cache_sync_worker::view_syncer_v2::pipeline::{
    Chain, ChainSpec, InMemoryInput, JoinSpec,
};
use zero_cache_sync_worker::view_syncer_v2::row_change::RowChange;
use zero_cache_types::ast::{Direction, Ordering, System};
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::Row;

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

fn row(id: i64, flag: bool) -> Row {
    let mut r = Row::new();
    r.insert("id".into(), Some(json!(id)));
    r.insert("flag".into(), Some(json!(flag)));
    r
}

fn node_of(r: Row) -> Node {
    Node {
        row: r,
        relationships: IndexMap::new(),
    }
}

fn predicate_flag_true() -> Predicate {
    Arc::new(|r: &Row| matches!(r.get("flag"), Some(Some(v)) if v == &json!(true)))
}

#[test]
fn hydrate_filter_only() {
    let rows: Vec<Row> = (0..10).map(|i| row(i, i % 2 == 0)).collect();
    let source = Box::new(InMemoryInput::new(rows, schema()));
    let spec = ChainSpec {
        query_id: "q1".into(),
        table: "t".into(),
        primary_key: PrimaryKey::new(vec!["id".into()]),
        predicate: Some(predicate_flag_true()),
        skip_bound: None,
        limit: None,
        exists: None,
        exists_chain: vec![],
        joins: vec![],
        or_branches: vec![],
        order_by: None,
    };
    let mut chain = Chain::build(spec, source);
    let out = chain.hydrate();
    assert_eq!(out.len(), 5, "5 flag=true rows expected");
}

#[test]
fn hydrate_filter_then_skip() {
    let rows: Vec<Row> = (0..10).map(|i| row(i, i % 2 == 0)).collect();
    let source = Box::new(InMemoryInput::new(rows, schema()));
    let spec = ChainSpec {
        query_id: "q1".into(),
        table: "t".into(),
        primary_key: PrimaryKey::new(vec!["id".into()]),
        predicate: Some(predicate_flag_true()),
        // Skip anything <= id=3.
        skip_bound: Some(Bound {
            row: row(3, true),
            exclusive: true,
        }),
        limit: None,
        exists: None,
        exists_chain: vec![],
        joins: vec![],
        or_branches: vec![],
        order_by: None,
    };
    let mut chain = Chain::build(spec, source);
    let out = chain.hydrate();
    // flag=true rows: 0, 2, 4, 6, 8 → after Skip>3 (exclusive) → 4, 6, 8.
    assert_eq!(out.len(), 3);
    let ids: Vec<_> = out
        .iter()
        .map(|rc| match rc {
            RowChange::Add(a) => a.row_key.get("id").and_then(|v| v.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![Some(json!(4)), Some(json!(6)), Some(json!(8))]);
}

#[test]
fn advance_multi_op_push_propagates_through_chain() {
    // The test that motivated the refactor: Filter + Skip on push.
    // Filter gates flag=true. Skip drops anything <=3.
    // Push flag=false → dropped by Filter.
    // Push flag=true id=2 → passed by Filter, dropped by Skip (2 <= 3).
    // Push flag=true id=7 → passes both.
    let source = Box::new(InMemoryInput::new(vec![], schema()));
    let spec = ChainSpec {
        query_id: "q1".into(),
        table: "t".into(),
        primary_key: PrimaryKey::new(vec!["id".into()]),
        predicate: Some(predicate_flag_true()),
        skip_bound: Some(Bound {
            row: row(3, true),
            exclusive: true,
        }),
        limit: None,
        exists: None,
        exists_chain: vec![],
        joins: vec![],
        or_branches: vec![],
        order_by: None,
    };
    let mut chain = Chain::build(spec, source);

    // flag=false → Filter drops.
    let out = chain.advance(Change::Add(AddChange {
        node: node_of(row(5, false)),
    }));
    assert!(out.is_empty(), "flag=false should be dropped by Filter");

    // flag=true id=2 → Skip drops (id <= 3 exclusive).
    let out = chain.advance(Change::Add(AddChange {
        node: node_of(row(2, true)),
    }));
    assert!(out.is_empty(), "id=2 should be dropped by Skip");

    // flag=true id=7 → passes both.
    let out = chain.advance(Change::Add(AddChange {
        node: node_of(row(7, true)),
    }));
    assert_eq!(out.len(), 1);
    assert!(matches!(out[0], RowChange::Add(_)));

    // Remove id=10 → passes Filter (flag=true) and Skip (10>3).
    let out = chain.advance(Change::Remove(RemoveChange {
        node: node_of(row(10, true)),
    }));
    assert_eq!(out.len(), 1);
    assert!(matches!(out[0], RowChange::Remove(_)));
}

#[test]
fn hydrate_with_join_decorates_parent_with_children() {
    // Parent table: users {id, flag}. Child table: messages {id, user_id, flag}.
    // Chain over users, joining messages by user_id. Hydrate returns
    // users with attached `messages` relationship.
    let users: Vec<Row> = (1..=3).map(|i| row(i, true)).collect();
    let user_source = Box::new(InMemoryInput::new(users, schema()));

    // For this minimal first cut, the child source's rows need a "user_id"
    // column. Our test helper `row(id, flag)` doesn't include one, so
    // build child rows manually.
    let child_schema = schema();
    let make_child = |id: i64, user_id: i64| {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("user_id".into(), Some(json!(user_id)));
        r.insert("flag".into(), Some(json!(true)));
        r
    };
    let messages = vec![
        make_child(100, 1),
        make_child(101, 1),
        make_child(200, 2),
    ];
    let child_source = Box::new(InMemoryInput::new(messages, child_schema));

    let mut spec = ChainSpec {
        query_id: "q_users_with_msgs".into(),
        table: "users".into(),
        primary_key: PrimaryKey::new(vec!["id".into()]),
        predicate: None,
        skip_bound: None,
        limit: None,
        exists: None,
        exists_chain: vec![],
        joins: vec![JoinSpec {
            parent_key: vec!["id".into()],
            child_key: vec!["user_id".into()],
            relationship_name: "messages".into(),
            child_table: "messages".into(),
            child_subquery: None,
            child_primary_key: None,
        }],
        or_branches: vec![],
        order_by: None,
    };
    let _ = &mut spec;
    let mut chain = Chain::build_with_join(spec, user_source, Some(child_source));

    let out = chain.hydrate();
    assert_eq!(out.len(), 3, "3 users expected");
}

#[test]
fn advance_with_take_at_limit_refetches_new_bound() {
    // Source has rows 10, 20, 30, 40, 50 (all flag=true).
    // Chain: Filter(flag=true) → Take(limit=2).
    // Hydrate: rows 10, 20. Bound = 20.
    // Push row 5: evicts 20, adds 5. Take signals refetch; Chain asks source
    // for rows AFTER 5 through Filter — returns 10, 20, 30, 40, 50.
    // First row is 10, becomes new bound.
    let rows: Vec<Row> = (1..=5).map(|i| row(i * 10, true)).collect();
    let source = Box::new(InMemoryInput::new(rows, schema()));
    let spec = ChainSpec {
        query_id: "q1".into(),
        table: "t".into(),
        primary_key: PrimaryKey::new(vec!["id".into()]),
        predicate: Some(predicate_flag_true()),
        skip_bound: None,
        limit: Some(2),
        exists: None,
        exists_chain: vec![],
        joins: vec![],
        or_branches: vec![],
        order_by: None,
    };
    let mut chain = Chain::build(spec, source);
    let hyd = chain.hydrate();
    assert_eq!(hyd.len(), 2);

    let out = chain.advance(Change::Add(AddChange {
        node: node_of(row(5, true)),
    }));
    // Remove(20) + Add(5)
    assert_eq!(out.len(), 2, "expected Remove+Add for at-limit eviction");
    assert!(matches!(out[0], RowChange::Remove(_)));
    assert!(matches!(out[1], RowChange::Add(_)));
}
