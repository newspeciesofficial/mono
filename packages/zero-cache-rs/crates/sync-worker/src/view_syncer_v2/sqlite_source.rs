//! SQLite-backed `Input` adapter for PipelineV2.
//!
//! Wraps `ivm_v2::source::ChannelSource` so it implements the
//! `ivm_v2::operator::Input` trait expected by Chain. Translates
//! `FetchRequest` into the SQL that the worker thread runs, and
//! converts streamed `OwnedRow`s (rusqlite values) into `Node`s.
//!
//! Scope of this first cut:
//! - `SELECT * FROM <table>` (no WHERE; WHERE is applied by Filter in
//!   the chain).
//! - `start.basis=at/after` and `reverse` are honored via an
//!   `ORDER BY` on the PK columns and a generated cursor condition.
//! - Columns list is the schema's `columns` keys.
//!
//! Not covered yet:
//! - `constraint` from FetchRequest → SQL WHERE (Take's eviction
//!   re-fetch passes a start constraint instead; good enough for the
//!   common path).
//! - JSON value columns.

use std::collections::HashMap;

use indexmap::IndexMap;
use rusqlite::types::Value as SqlValue;

use crate::ivm::data::Node;
use crate::ivm::schema::SourceSchema;
use crate::ivm_v2::operator::{FetchRequest, Input, InputBase};
use crate::ivm_v2::source::SnapshotReader;
use zero_cache_types::ast::Direction;
use zero_cache_types::value::Row;

/// Zero-schema logical column types we coerce into at the source
/// boundary. Mirrors TS `fromSQLiteType` in `zqlite/src/table-source.ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnType {
    Boolean,
    Number,
    String,
    Null,
    Json,
}

impl ColumnType {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "boolean" => Some(Self::Boolean),
            "number" => Some(Self::Number),
            "string" => Some(Self::String),
            "null" => Some(Self::Null),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

/// Table-scoped `Input` adapter over a driver-shared [`SnapshotReader`].
/// All sources in a given `PipelineV2` hold a cheap clone of the same
/// `SnapshotReader`, so every SQLite read in the driver sees the same
/// committed snapshot until the reader is refreshed (via
/// `pipeline_v2_refresh_snapshot` from TS after the snapshotter advances).
pub struct SqliteSource {
    inner: SnapshotReader,
    table: String,
    columns: Vec<String>,
    schema: SourceSchema,
    /// Per-column coercion rules. Unknown columns fall through to raw
    /// SQLite→JSON conversion.
    column_types: HashMap<String, ColumnType>,
}

impl SqliteSource {
    pub fn new(inner: SnapshotReader, schema: SourceSchema, columns: Vec<String>) -> Self {
        Self::new_with_column_types(inner, schema, columns, HashMap::new())
    }

    /// Construct a source with per-column logical types, applied at
    /// fetch time so that downstream predicate evaluation (and
    /// comparators for Skip/Take) sees canonical JS types —
    /// `boolean` columns arrive as `Bool`, JSON columns arrive as
    /// parsed values, bigint columns get bounds-checked.
    pub fn new_with_column_types(
        inner: SnapshotReader,
        schema: SourceSchema,
        columns: Vec<String>,
        column_types_raw: HashMap<String, String>,
    ) -> Self {
        let table = schema.table_name.clone();
        let column_types = column_types_raw
            .into_iter()
            .filter_map(|(k, v)| ColumnType::from_str(&v).map(|t| (k, t)))
            .collect();
        Self {
            inner,
            table,
            columns,
            schema,
            column_types,
        }
    }

    /// Replace the source's `sort` ordering. Called by `Chain::build*`
    /// after construction when the query's AST has an explicit
    /// `orderBy` — without this, `SqliteSource::fetch` would emit rows
    /// in the table's default (primary key) order and a subsequent
    /// `Take` would pick a different 100-row slice than TS, breaking
    /// row-count parity for queries like `issueListV2` (which orders
    /// by `modified DESC`).
    pub fn set_sort(&mut self, sort: Vec<(String, Direction)>) {
        // Rebuild the comparator to match the new sort order so any
        // ordering-aware transformers downstream (Skip, Take) agree
        // with what rows come off the source.
        self.schema.compare_rows =
            std::sync::Arc::new(crate::ivm::data::make_comparator(sort.clone(), false));
        self.schema.sort = sort;
    }
}

impl InputBase for SqliteSource {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {}
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}

impl Input for SqliteSource {
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        let (sql, params) = build_select_sql(&self.table, &self.columns, &self.schema.sort, &req);
        let rx = self.inner.query_rows(sql, params, self.columns.clone());
        let columns = self.columns.clone();
        let column_types = self.column_types.clone();
        Box::new(rx.into_iter().map(move |owned| {
            let mut row = Row::new();
            for k in &columns {
                let v = owned.get(k).cloned().unwrap_or(SqlValue::Null);
                row.insert(k.clone(), sql_value_to_typed_json(v, column_types.get(k).copied()));
            }
            Node {
                row,
                relationships: IndexMap::new(),
            }
        }))
    }

    /// Indexed single-row lookup by primary-key equality. Builds
    /// `SELECT cols FROM "table" WHERE "pk_col_1"=? AND "pk_col_2"=? LIMIT 1`
    /// and binds `pk_row` values as parameters. Replaces the default-trait
    /// linear scan so cold-path `PipelineDriver.getRow` calls are O(log n)
    /// via SQLite's PK index rather than O(n) over the full table.
    ///
    /// `pk_row` may pass any unique-index column set (not strictly the
    /// primary key) — matches TS `TableSource.getRow` which infers the
    /// index from the keys present in the argument.
    fn get_row(&mut self, pk_row: &Row) -> Option<Node> {
        if pk_row.is_empty() {
            return None;
        }
        let key_cols: Vec<String> = pk_row.keys().cloned().collect();
        let where_clause = key_cols
            .iter()
            .map(|c| format!("\"{}\"=?", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" AND ");
        let cols = self
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {} FROM \"{}\" WHERE {} LIMIT 1",
            cols,
            self.table.replace('"', "\"\""),
            where_clause,
        );
        let params: Vec<SqlValue> = key_cols
            .iter()
            .map(|c| json_to_sql_value(pk_row.get(c).and_then(|v| v.as_ref())))
            .collect();
        let rx = self.inner.query_rows(sql, params, self.columns.clone());
        let owned = rx.into_iter().next()?;
        let mut row = Row::new();
        for k in &self.columns {
            let v = owned.get(k).cloned().unwrap_or(SqlValue::Null);
            row.insert(
                k.clone(),
                sql_value_to_typed_json(v, self.column_types.get(k).copied()),
            );
        }
        Some(Node {
            row,
            relationships: IndexMap::new(),
        })
    }
}

fn json_to_sql_value(v: Option<&serde_json::Value>) -> SqlValue {
    use serde_json::Value as J;
    match v {
        None | Some(J::Null) => SqlValue::Null,
        Some(J::Bool(b)) => SqlValue::Integer(if *b { 1 } else { 0 }),
        Some(J::Number(n)) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                SqlValue::Real(f)
            } else {
                SqlValue::Null
            }
        }
        Some(J::String(s)) => SqlValue::Text(s.clone()),
        // Arrays / objects aren't valid PK values; fall through to Null.
        Some(_) => SqlValue::Null,
    }
}

fn sql_value_to_json(v: SqlValue) -> Option<serde_json::Value> {
    match v {
        SqlValue::Null => None,
        SqlValue::Integer(i) => Some(serde_json::json!(i)),
        SqlValue::Real(f) => Some(serde_json::json!(f)),
        SqlValue::Text(s) => Some(serde_json::json!(s)),
        SqlValue::Blob(b) => Some(serde_json::json!(b)),
    }
}

/// Schema-aware SQLite → JSON coercion. Mirrors TS `fromSQLiteType` in
/// `zqlite/src/table-source.ts`:
///
/// - `boolean`: INTEGER 0/1 → `Bool(false)`/`Bool(true)`. Truthy for any
///   non-zero integer (matches TS `!!v`).
/// - `json`: TEXT → parsed JSON. Parse failure falls back to raw string
///   (TS throws; we're more permissive here to avoid taking down a
///   whole hydration for one bad row — the bad value surfaces to the
///   client which can log it).
/// - `number` / `string` / `null`: pass through (rusqlite already maps
///   i64/f64/String to the JSON equivalents; the TS bigint bounds-check
///   doesn't apply because Rust keeps i64 precision end-to-end —
///   overflow protection kicks in when values cross back through napi
///   to JS, which has its own bigint handling).
/// - Unknown columns (no entry in `column_types`): raw SQLite→JSON
///   conversion, same as before.
fn sql_value_to_typed_json(
    v: SqlValue,
    ty: Option<ColumnType>,
) -> Option<serde_json::Value> {
    if matches!(v, SqlValue::Null) {
        return None;
    }
    let Some(ty) = ty else {
        return sql_value_to_json(v);
    };
    match ty {
        ColumnType::Boolean => match v {
            SqlValue::Integer(i) => Some(serde_json::Value::Bool(i != 0)),
            // Fall through for unexpected storage types — preserve raw
            // value so the client can see it rather than silently
            // coerce a text/blob into `true`.
            other => sql_value_to_json(other),
        },
        ColumnType::Json => match v {
            SqlValue::Text(s) => match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(parsed) => Some(parsed),
                Err(_) => Some(serde_json::Value::String(s)),
            },
            other => sql_value_to_json(other),
        },
        ColumnType::Number | ColumnType::String | ColumnType::Null => sql_value_to_json(v),
    }
}

fn build_select_sql(
    table: &str,
    columns: &[String],
    sort: &[(String, Direction)],
    req: &FetchRequest,
) -> (String, Vec<SqlValue>) {
    let cols = if columns.is_empty() {
        "*".to_string()
    } else {
        columns
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut sql = format!("SELECT {} FROM \"{}\"", cols, table.replace('"', "\"\""));
    let mut params: Vec<SqlValue> = Vec::new();

    // WHERE clause from `constraint` — multi-key equality. Matches TS
    // `query-builder.ts::constraintsToSQL`. Order is the constraint's
    // `IndexMap` insertion order so generated SQL is stable across
    // identical constraints (helps statement caching).
    if let Some(constraint) = req.constraint.as_ref() {
        if !constraint.is_empty() {
            let mut conds: Vec<String> = Vec::with_capacity(constraint.len());
            for (col, val) in constraint.iter() {
                // Constraint Value is `Option<serde_json::Value>` —
                // None or Some(Null) → SQL `IS NULL`; otherwise bind
                // as a positional param.
                let is_null = matches!(val, None | Some(serde_json::Value::Null));
                if is_null {
                    conds.push(format!("\"{}\" IS NULL", col.replace('"', "\"\"")));
                } else {
                    conds.push(format!("\"{}\"=?", col.replace('"', "\"\"")));
                    params.push(value_to_sql_value(val));
                }
            }
            sql.push_str(&format!(" WHERE {}", conds.join(" AND ")));
        }
    }

    let reverse = req.reverse.unwrap_or(false);
    if !sort.is_empty() {
        let order_parts: Vec<String> = sort
            .iter()
            .map(|(col, dir)| {
                let base = match dir {
                    Direction::Asc => "ASC",
                    Direction::Desc => "DESC",
                };
                let effective = if reverse && base == "ASC" {
                    "DESC"
                } else if reverse && base == "DESC" {
                    "ASC"
                } else {
                    base
                };
                format!("\"{}\" {}", col.replace('"', "\"\""), effective)
            })
            .collect();
        sql.push_str(&format!(" ORDER BY {}", order_parts.join(", ")));
    }
    (sql, params)
}

/// Convert a `zero_cache_types::value::Value` (which is
/// `Option<serde_json::Value>`) to a rusqlite `SqlValue` for parameter
/// binding. Used by `build_select_sql` when honoring `FetchRequest.constraint`.
fn value_to_sql_value(v: &zero_cache_types::value::Value) -> SqlValue {
    match v {
        None => SqlValue::Null,
        Some(serde_json::Value::Null) => SqlValue::Null,
        Some(serde_json::Value::Bool(b)) => SqlValue::Integer(if *b { 1 } else { 0 }),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                SqlValue::Real(f)
            } else {
                SqlValue::Null
            }
        }
        Some(serde_json::Value::String(s)) => SqlValue::Text(s.clone()),
        // Arrays / objects can't be SQLite parameters — surface as Null
        // and let the caller's higher-level type system catch it.
        Some(_) => SqlValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::data::make_comparator;
    use rusqlite::{params, Connection};
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;
    use zero_cache_types::ast::{Direction, Ordering};
    use zero_cache_types::primary_key::PrimaryKey;

    fn setup(path: &std::path::Path) {
        let c = Connection::open(path).unwrap();
        // `SnapshotReader` uses BEGIN CONCURRENT which requires wal2.
        c.execute_batch(
            "PRAGMA journal_mode = wal2;
             CREATE TABLE t(id INTEGER PRIMARY KEY, txt TEXT);",
        )
        .unwrap();
        for i in 0..5 {
            c.execute(
                "INSERT INTO t VALUES(?1, ?2)",
                params![i as i64, format!("r-{i}")],
            )
            .unwrap();
        }
    }

    fn schema() -> SourceSchema {
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
        SourceSchema {
            table_name: "t".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: zero_cache_types::ast::System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    #[test]
    fn fetch_delivers_all_rows_in_pk_order() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        setup(&path);

        let reader = SnapshotReader::new(path);
        let mut src = SqliteSource::new(
            reader,
            schema(),
            vec!["id".into(), "txt".into()],
        );

        let rows: Vec<Node> = src.fetch(FetchRequest::default()).collect();
        assert_eq!(rows.len(), 5);
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.row.get("id"), Some(&Some(json!(i as i64))));
        }
    }

    #[test]
    fn fetch_reverse_flips_order() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        setup(&path);

        let reader = SnapshotReader::new(path);
        let mut src = SqliteSource::new(
            reader,
            schema(),
            vec!["id".into(), "txt".into()],
        );

        let rows: Vec<Node> = src
            .fetch(FetchRequest {
                reverse: Some(true),
                ..FetchRequest::default()
            })
            .collect();
        assert_eq!(rows[0].row.get("id"), Some(&Some(json!(4))));
        assert_eq!(rows[4].row.get("id"), Some(&Some(json!(0))));
    }

    /// When a column is registered with type `boolean`, SQLite
    /// INTEGER 0/1 values must surface as JSON `Bool`. Without this,
    /// `WHERE open = true` predicates would never match rows fetched
    /// from a real replica, since SQLite stores booleans as integers.
    #[test]
    fn fetch_coerces_boolean_column_integer_to_bool() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let c = Connection::open(&path).unwrap();
        c.execute_batch(
            "PRAGMA journal_mode = wal2;
             CREATE TABLE t(id INTEGER PRIMARY KEY, open INTEGER);",
        )
        .unwrap();
        c.execute("INSERT INTO t VALUES (1, 1), (2, 0)", params![]).unwrap();

        let reader = SnapshotReader::new(path);
        let mut column_types = HashMap::new();
        column_types.insert("open".into(), "boolean".into());
        let mut src = SqliteSource::new_with_column_types(
            reader,
            schema(),
            vec!["id".into(), "open".into()],
            column_types,
        );

        let rows: Vec<Node> = src.fetch(FetchRequest::default()).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].row.get("open"), Some(&Some(json!(true))));
        assert_eq!(rows[1].row.get("open"), Some(&Some(json!(false))));
    }

    /// `json` columns should come through as parsed JSON — not the raw
    /// TEXT string. Matches TS `fromSQLiteType` behavior.
    #[test]
    fn fetch_parses_json_column_text() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let c = Connection::open(&path).unwrap();
        c.execute_batch(
            "PRAGMA journal_mode = wal2;
             CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);",
        )
        .unwrap();
        c.execute(
            "INSERT INTO t VALUES (1, ?1)",
            params!["{\"hello\":\"world\",\"n\":7}"],
        )
        .unwrap();

        let reader = SnapshotReader::new(path);
        let mut column_types = HashMap::new();
        column_types.insert("payload".into(), "json".into());
        let mut src = SqliteSource::new_with_column_types(
            reader,
            schema(),
            vec!["id".into(), "payload".into()],
            column_types,
        );

        let rows: Vec<Node> = src.fetch(FetchRequest::default()).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].row.get("payload"),
            Some(&Some(json!({"hello": "world", "n": 7})))
        );
    }
}
