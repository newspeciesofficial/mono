//! Serialization benchmarks: measure JSON encode/decode performance
//! for the types that cross the wire (Row, AST, RowChange, Poke messages).
//!
//! Run: cargo bench -p zero-cache-types --bench serialization

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use zero_cache_types::ast::{AST, Condition, Direction, SimpleOperator, ValuePosition};
use zero_cache_types::pool_protocol::RowChange;
use zero_cache_types::protocol::downstream::Downstream;
use zero_cache_types::protocol::poke::{PokeEndBody, PokePartBody, PokeStartBody};
use zero_cache_types::protocol::row_patch::RowPatchOp;
use zero_cache_types::protocol::upstream::Upstream;
use zero_cache_types::value::{Row, json_i64, json_string};

fn make_row(n: usize) -> Row {
    let mut row = Row::new();
    for i in 0..n {
        row.insert(format!("col_{i}"), Some(json_string(format!("value_{i}"))));
    }
    row
}

fn make_row_change(n: usize) -> RowChange {
    let mut row_key = Row::new();
    row_key.insert("id".into(), Some(json_string("abc-123")));

    RowChange::Add {
        query_id: "q1".into(),
        table: "user".into(),
        row_key,
        row: make_row(n),
    }
}

fn make_ast() -> AST {
    AST {
        schema: None,
        table: "issue".into(),
        alias: None,
        where_clause: Some(Box::new(Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: "status".into(),
            },
            right: zero_cache_types::ast::NonColumnValue::Literal {
                value: zero_cache_types::ast::LiteralValue::String("open".into()),
            },
        })),
        related: None,
        start: None,
        limit: Some(50),
        order_by: Some(vec![
            ("created".into(), Direction::Desc),
            ("id".into(), Direction::Asc),
        ]),
    }
}

fn bench_row_serialize(c: &mut Criterion) {
    let row = make_row(10);
    c.bench_function("row_10_cols_serialize", |b| {
        b.iter(|| serde_json::to_string(black_box(&row)).unwrap())
    });

    let row = make_row(50);
    c.bench_function("row_50_cols_serialize", |b| {
        b.iter(|| serde_json::to_string(black_box(&row)).unwrap())
    });
}

fn bench_row_deserialize(c: &mut Criterion) {
    let json = serde_json::to_string(&make_row(10)).unwrap();
    c.bench_function("row_10_cols_deserialize", |b| {
        b.iter(|| serde_json::from_str::<Row>(black_box(&json)).unwrap())
    });

    let json = serde_json::to_string(&make_row(50)).unwrap();
    c.bench_function("row_50_cols_deserialize", |b| {
        b.iter(|| serde_json::from_str::<Row>(black_box(&json)).unwrap())
    });
}

fn bench_ast_serialize(c: &mut Criterion) {
    let ast = make_ast();
    c.bench_function("ast_with_condition_serialize", |b| {
        b.iter(|| serde_json::to_string(black_box(&ast)).unwrap())
    });
}

fn bench_ast_deserialize(c: &mut Criterion) {
    let json = serde_json::to_string(&make_ast()).unwrap();
    c.bench_function("ast_with_condition_deserialize", |b| {
        b.iter(|| serde_json::from_str::<AST>(black_box(&json)).unwrap())
    });
}

fn bench_row_change_serialize(c: &mut Criterion) {
    let change = make_row_change(10);
    c.bench_function("row_change_10_cols_serialize", |b| {
        b.iter(|| serde_json::to_string(black_box(&change)).unwrap())
    });
}

fn bench_upstream_parse(c: &mut Criterion) {
    let ping = r#"["ping",{}]"#;
    c.bench_function("upstream_parse_ping", |b| {
        b.iter(|| Upstream::from_json(black_box(ping)).unwrap())
    });

    let cdq = r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"abc","ast":{"table":"user"}}]}]"#;
    c.bench_function("upstream_parse_cdq", |b| {
        b.iter(|| Upstream::from_json(black_box(cdq)).unwrap())
    });
}

fn bench_downstream_serialize(c: &mut Criterion) {
    let poke_start = Downstream::PokeStart(PokeStartBody {
        poke_id: "poke-1".into(),
        base_cookie: Some("v42".into()),
        schema_versions: None,
        timestamp: Some(1234567890.0),
    });
    c.bench_function("downstream_poke_start_serialize", |b| {
        b.iter(|| black_box(&poke_start).to_json().unwrap())
    });
}

fn bench_batch_serialize(c: &mut Criterion) {
    // Simulate a batch of 20 row changes (matching BATCH_SIZE in pool-thread.ts)
    let batch: Vec<RowChange> = (0..20).map(|_| make_row_change(10)).collect();
    c.bench_function("batch_20_row_changes_serialize", |b| {
        b.iter(|| serde_json::to_string(black_box(&batch)).unwrap())
    });
}

criterion_group!(
    benches,
    bench_row_serialize,
    bench_row_deserialize,
    bench_ast_serialize,
    bench_ast_deserialize,
    bench_row_change_serialize,
    bench_upstream_parse,
    bench_downstream_serialize,
    bench_batch_serialize,
);
criterion_main!(benches);
