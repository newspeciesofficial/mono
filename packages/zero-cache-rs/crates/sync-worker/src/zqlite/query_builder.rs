//! Port of `packages/zqlite/src/query-builder.ts`.
//!
//! Public exports ported:
//!
//! - [`build_select_query`] — produces a `SELECT … FROM … WHERE … ORDER BY`
//!   SQL string (with `?` placeholders) plus the positional parameter
//!   vector. Used by [`crate::zqlite::table_source::TableSource`] for
//!   `fetch`, and by the cost-model for plan estimation.
//! - [`constraints_to_sql`] — renders a [`Constraint`] as a list of
//!   `"col"=?` fragments + params.
//! - [`order_by_to_sql`] — renders `ORDER BY` with optional reverse.
//! - [`filters_to_sql`] — renders a filter tree (`SimpleCondition | And | Or`)
//!   into a single SQL expression + params.
//! - [`to_sqlite_type`] — re-exported here to parallel the TS module
//!   surface. Today we delegate to the identical helper in
//!   [`crate::zqlite::table_source`] to avoid duplication.
//!
//! ## Design decisions
//!
//! - **NoSubqueryCondition.** TS uses a type alias that excludes the
//!   `correlatedSubquery` arm at compile time. Rust has only one
//!   [`Condition`] enum; we panic with a clear message if a caller tries
//!   to render a correlated subquery — the TS equivalent would be a type
//!   error. Callers are expected to run `transformFilters`
//!   ([`crate::builder::filter::transform_filters`]) first to strip
//!   correlated-subquery branches.
//! - **`static` parameters.** TS throws. We panic with the same message.
//!   Static parameters should be resolved via
//!   [`zero_cache_types::ast::bind_static_parameters`] before this point.
//! - **Output shape.** TS returns an `SQLQuery` object that gets
//!   `format()`ed later. Our Rust output is the already-formatted
//!   `(String, Vec<SqlValue>)` pair — i.e. equivalent to TS
//!   `compile(...)` on the returned query. This matches the shape
//!   [`TableSource`] already expects.
//! - **Identifier escaping.** SQLite identifiers are wrapped in `"…"`
//!   with embedded `"` doubled. We inline the helper (private here) to
//!   match the TS `@databases/escape-identifier` output.

use indexmap::IndexMap;
use rusqlite::types::Value as SqlValue;

use zero_cache_types::ast::{Condition, Direction, Ordering, SimpleOperator, ValuePosition};
use zero_cache_types::value::{Row, Value};

use crate::ivm::constraint::Constraint;
use crate::ivm::operator::{Start, StartBasis};
use crate::zqlite::table_source::{SchemaValue, ValueType, to_sqlite_type as ts_to_sqlite_type};

/// Escape a SQLite identifier: wrap in `"…"`, double any embedded `"`.
fn escape_ident(ident: &str) -> String {
    if !ident.contains('"') {
        let mut out = String::with_capacity(ident.len() + 2);
        out.push('"');
        out.push_str(ident);
        out.push('"');
        return out;
    }
    let mut out = String::with_capacity(ident.len() + 4);
    out.push('"');
    for ch in ident.chars() {
        if ch == '"' {
            out.push('"');
            out.push('"');
        } else {
            out.push(ch);
        }
    }
    out.push('"');
    out
}

/// TS `toSQLiteType(v, type)` — mirror-exposed here for parity with the TS
/// module surface. Delegates to the identical helper in
/// [`crate::zqlite::table_source`].
#[inline]
pub fn to_sqlite_type(v: &Value, ty: ValueType) -> SqlValue {
    ts_to_sqlite_type(v, ty)
}

/// Convert a [`Value`] into a SQL literal for a column of type `ty`,
/// using the per-column schema the caller owns. Wrapper around
/// [`to_sqlite_type`] that takes a look-up instead of an explicit type.
fn column_to_sqlite_type(
    column: &str,
    v: &Value,
    columns: &IndexMap<String, SchemaValue>,
) -> SqlValue {
    let ty = columns
        .get(column)
        .map(|s| s.value_type)
        .unwrap_or(ValueType::String);
    to_sqlite_type(v, ty)
}

/// Derive the JS-ish type of an arbitrary literal. Port of
/// `getJsType(value)` — returns the [`ValueType`] key used by
/// `toSQLiteType` to branch.
fn get_js_type(v: &Value) -> ValueType {
    match v {
        None => ValueType::Null,
        Some(json) => match json {
            serde_json::Value::Null => ValueType::Null,
            serde_json::Value::String(_) => ValueType::String,
            serde_json::Value::Number(_) => ValueType::Number,
            serde_json::Value::Bool(_) => ValueType::Boolean,
            _ => ValueType::Json,
        },
    }
}

/// TS `constraintsToSQL(constraint, columns)`.
///
/// Returns one `"col"=?` fragment per constraint entry, plus the bound
/// params in matching order. Returns empty if `constraint` is `None`.
pub fn constraints_to_sql(
    constraint: Option<&Constraint>,
    columns: &IndexMap<String, SchemaValue>,
) -> (Vec<String>, Vec<SqlValue>) {
    let mut parts = Vec::new();
    let mut params = Vec::new();
    let Some(c) = constraint else {
        return (parts, params);
    };
    for (key, value) in c.iter() {
        // TS renders constraints via `sql\`${sql.ident(key)} = ${value}\``
        // which produces `"key" = ?` with spaces around the `=`.
        parts.push(format!("{} = ?", escape_ident(key)));
        params.push(column_to_sqlite_type(key, value, columns));
    }
    (parts, params)
}

/// TS `orderByToSQL(order, reverse)`.
///
/// When `reverse` is true, each direction flips (asc ↔ desc).
pub fn order_by_to_sql(order: &Ordering, reverse: bool) -> String {
    let parts: Vec<String> = order
        .iter()
        .map(|(f, d)| {
            let eff = if reverse { flip_direction(*d) } else { *d };
            let dir_str = match eff {
                Direction::Asc => "asc",
                Direction::Desc => "desc",
            };
            // TS mixes "asc"/"desc" in lowercase — `__dangerous__rawValue`
            // passes through unchanged, matching our literal.
            format!("{} {}", escape_ident(f), dir_str)
        })
        .collect();
    format!("ORDER BY {}", parts.join(", "))
}

fn flip_direction(d: Direction) -> Direction {
    match d {
        Direction::Asc => Direction::Desc,
        Direction::Desc => Direction::Asc,
    }
}

/// TS `filtersToSQL(filters)` — renders a NoSubqueryCondition.
///
/// ## Panics
/// - On [`Condition::CorrelatedSubquery`] — TS callers use a type alias
///   to exclude this branch. Callers must run `transform_filters` first.
pub fn filters_to_sql(filters: &Condition) -> (String, Vec<SqlValue>) {
    match filters {
        Condition::Simple { op, left, right } => simple_condition_to_sql(*op, left, right),
        Condition::And { conditions } => {
            // Branch: empty AND → TRUE. TS `sql\`TRUE\``.
            if conditions.is_empty() {
                return ("TRUE".to_string(), Vec::new());
            }
            let mut parts: Vec<String> = Vec::with_capacity(conditions.len());
            let mut params: Vec<SqlValue> = Vec::new();
            for c in conditions.iter() {
                let (s, p) = filters_to_sql(c);
                parts.push(s);
                params.extend(p);
            }
            (format!("({})", parts.join(" AND ")), params)
        }
        Condition::Or { conditions } => {
            // Branch: empty OR → FALSE. TS `sql\`FALSE\``.
            if conditions.is_empty() {
                return ("FALSE".to_string(), Vec::new());
            }
            let mut parts: Vec<String> = Vec::with_capacity(conditions.len());
            let mut params: Vec<SqlValue> = Vec::new();
            for c in conditions.iter() {
                let (s, p) = filters_to_sql(c);
                parts.push(s);
                params.extend(p);
            }
            (format!("({})", parts.join(" OR ")), params)
        }
        Condition::CorrelatedSubquery { .. } => {
            panic!(
                "filters_to_sql: CorrelatedSubquery is not representable in \
                 SQL. Run transform_filters first to strip correlated branches."
            );
        }
    }
}

fn simple_condition_to_sql(
    op: SimpleOperator,
    left: &ValuePosition,
    right: &zero_cache_types::ast::NonColumnValue,
) -> (String, Vec<SqlValue>) {
    use zero_cache_types::ast::NonColumnValue;
    // Branch: IN / NOT IN — TS inlines a literal array as JSON inside
    // `json_each(...)`. Static is an error.
    if matches!(op, SimpleOperator::IN | SimpleOperator::NotIn) {
        match right {
            NonColumnValue::Literal { value } => {
                let json = literal_value_to_json(value);
                let op_str = match op {
                    SimpleOperator::IN => "IN",
                    SimpleOperator::NotIn => "NOT IN",
                    _ => unreachable!(),
                };
                let left_sql = value_position_to_sql(left);
                let (left_str, left_params) = left_sql;
                let mut params = left_params;
                params.push(SqlValue::Text(serde_json::to_string(&json).unwrap()));
                return (
                    format!("{left_str} {op_str} (SELECT value FROM json_each(?))"),
                    params,
                );
            }
            NonColumnValue::Static { .. } => {
                panic!("Static parameters must be replaced before conversion to SQL");
            }
        }
    }

    let left_sql = value_position_to_sql(left);
    let right_sql = non_column_value_to_sql(right);

    // Branch: ILIKE → LIKE, NOT ILIKE → NOT LIKE (SQLite's LIKE is case
    // insensitive by default).
    let op_str = match op {
        SimpleOperator::Eq => "=",
        SimpleOperator::Ne => "!=",
        SimpleOperator::IS => "IS",
        SimpleOperator::IsNot => "IS NOT",
        SimpleOperator::Lt => "<",
        SimpleOperator::Gt => ">",
        SimpleOperator::Le => "<=",
        SimpleOperator::Ge => ">=",
        SimpleOperator::LIKE => "LIKE",
        SimpleOperator::NotLike => "NOT LIKE",
        SimpleOperator::ILIKE => "LIKE",
        SimpleOperator::NotIlike => "NOT LIKE",
        SimpleOperator::IN | SimpleOperator::NotIn => unreachable!("handled above"),
    };

    let (l_str, mut params) = left_sql;
    let (r_str, r_params) = right_sql;
    params.extend(r_params);
    (format!("{l_str} {op_str} {r_str}"), params)
}

fn value_position_to_sql(v: &ValuePosition) -> (String, Vec<SqlValue>) {
    match v {
        ValuePosition::Column { name } => (escape_ident(name), Vec::new()),
        ValuePosition::Literal { value } => {
            let as_value: Value = Some(literal_value_to_json(value));
            let ty = get_js_type(&as_value);
            ("?".to_string(), vec![to_sqlite_type(&as_value, ty)])
        }
        ValuePosition::Static { .. } => {
            panic!("Static parameters must be replaced before conversion to SQL");
        }
    }
}

fn non_column_value_to_sql(v: &zero_cache_types::ast::NonColumnValue) -> (String, Vec<SqlValue>) {
    use zero_cache_types::ast::NonColumnValue;
    match v {
        NonColumnValue::Literal { value } => {
            let as_value: Value = Some(literal_value_to_json(value));
            let ty = get_js_type(&as_value);
            ("?".to_string(), vec![to_sqlite_type(&as_value, ty)])
        }
        NonColumnValue::Static { .. } => {
            panic!("Static parameters must be replaced before conversion to SQL");
        }
    }
}

/// Exposed for [`crate::builder::filter`] — the builder/filter
/// module needs the same JS-shaped JSON value for its row predicates.
pub fn literal_value_to_json(lv: &zero_cache_types::ast::LiteralValue) -> serde_json::Value {
    use zero_cache_types::ast::{LiteralValue, ScalarLiteral};
    match lv {
        LiteralValue::Null => serde_json::Value::Null,
        LiteralValue::Bool(b) => serde_json::Value::Bool(*b),
        LiteralValue::Number(n) => number_f64_to_json(*n),
        LiteralValue::String(s) => serde_json::Value::String(s.clone()),
        LiteralValue::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|s| match s {
                    ScalarLiteral::String(s) => serde_json::Value::String(s.clone()),
                    ScalarLiteral::Number(n) => number_f64_to_json(*n),
                    ScalarLiteral::Bool(b) => serde_json::Value::Bool(*b),
                })
                .collect(),
        ),
    }
}

/// Convert an f64 to a `serde_json::Value::Number`, preferring the
/// integer representation when the float is whole and fits into `i64`.
/// Without this, `1.0` would serialize as `Number(1.0)` which is not
/// `==` to `Number(1)` — breaking predicate equality against rows that
/// deserialised ints as `Number(1)` via `json!(1)`.
fn number_f64_to_json(n: f64) -> serde_json::Value {
    if n.is_finite() && n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        serde_json::Value::Number((n as i64).into())
    } else {
        serde_json::Number::from_f64(n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }
}

/// Port of TS `gatherStartConstraints`. Produces one parenthesised OR
/// clause encoding the lexicographic "strictly after / before / at or
/// after / at or before" predicate over the ordering.
fn gather_start_constraints(
    start: &Start,
    reverse: bool,
    order: &Ordering,
    columns: &IndexMap<String, SchemaValue>,
) -> (String, Vec<SqlValue>) {
    let from: &Row = &start.row;
    let mut groups: Vec<String> = Vec::new();
    let mut params: Vec<SqlValue> = Vec::new();

    for i in 0..order.len() {
        let (i_field, i_dir) = &order[i];
        let mut group: Vec<String> = Vec::new();
        for j in 0..=i {
            if j == i {
                let v = from.get(i_field).cloned().unwrap_or(None);
                let bind = column_to_sqlite_type(i_field, &v, columns);
                match (*i_dir, reverse) {
                    (Direction::Asc, false) => {
                        // (? IS NULL OR "field" > ?)
                        group.push(format!("(? IS NULL OR {} > ?)", escape_ident(i_field)));
                        params.push(bind.clone());
                        params.push(bind);
                    }
                    (Direction::Asc, true) => {
                        // ("field" IS NULL OR "field" < ?)
                        group.push(format!("({0} IS NULL OR {0} < ?)", escape_ident(i_field)));
                        params.push(bind);
                    }
                    (Direction::Desc, false) => {
                        // ("field" IS NULL OR "field" < ?)
                        group.push(format!("({0} IS NULL OR {0} < ?)", escape_ident(i_field)));
                        params.push(bind);
                    }
                    (Direction::Desc, true) => {
                        // (? IS NULL OR "field" > ?)
                        group.push(format!("(? IS NULL OR {} > ?)", escape_ident(i_field)));
                        params.push(bind.clone());
                        params.push(bind);
                    }
                }
            } else {
                let (j_field, _) = &order[j];
                let v = from.get(j_field).cloned().unwrap_or(None);
                let bind = column_to_sqlite_type(j_field, &v, columns);
                group.push(format!("{} IS ?", escape_ident(j_field)));
                params.push(bind);
            }
        }
        groups.push(format!("({})", group.join(" AND ")));
    }

    // Branch: basis = At → add "row is exactly at start" clause.
    if matches!(start.basis, StartBasis::At) {
        let mut at_group: Vec<String> = Vec::new();
        for (f, _) in order.iter() {
            let v = from.get(f).cloned().unwrap_or(None);
            let bind = column_to_sqlite_type(f, &v, columns);
            at_group.push(format!("{} IS ?", escape_ident(f)));
            params.push(bind);
        }
        groups.push(format!("({})", at_group.join(" AND ")));
    }

    (format!("({})", groups.join(" OR ")), params)
}

/// TS `buildSelectQuery(tableName, columns, constraint, filters, order,
/// reverse, start)`.
///
/// Returns `(sql, params)` where `sql` uses `?` placeholders and
/// `params` is the positional bind vector.
pub fn build_select_query(
    table_name: &str,
    columns: &IndexMap<String, SchemaValue>,
    constraint: Option<&Constraint>,
    filters: Option<&Condition>,
    order: &Ordering,
    reverse: bool,
    start: Option<&Start>,
) -> (String, Vec<SqlValue>) {
    let select_cols: String = columns
        .keys()
        .map(|c| escape_ident(c))
        .collect::<Vec<_>>()
        .join(",");

    let mut where_parts: Vec<String> = Vec::new();
    let mut params: Vec<SqlValue> = Vec::new();

    let (c_parts, c_params) = constraints_to_sql(constraint, columns);
    where_parts.extend(c_parts);
    params.extend(c_params);

    // Branch: explicit `start` — render lexicographic ordering predicate.
    if let Some(s) = start {
        let (clause, sp) = gather_start_constraints(s, reverse, order, columns);
        where_parts.push(clause);
        params.extend(sp);
    }

    // Branch: filters tree — append a rendered SQL expression.
    if let Some(f) = filters {
        let (clause, fp) = filters_to_sql(f);
        where_parts.push(clause);
        params.extend(fp);
    }

    let where_clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };

    let order_clause = order_by_to_sql(order, reverse);

    let sql = format!(
        "SELECT {} FROM {}{} {}",
        select_cols,
        escape_ident(table_name),
        where_clause,
        order_clause,
    );
    // TS `@databases/sql` applies `text.trim()` in its formatter, stripping
    // any trailing whitespace (e.g. when `ORDER BY` has no parts, the
    // trailing space after `ORDER BY ` is trimmed).
    let sql = sql.trim_end().to_string();
    (sql, params)
}

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //! - `escape_ident` — no quote, with quote.
    //! - `constraints_to_sql` — None → empty; Some with 1 / multi.
    //! - `order_by_to_sql` — forward asc/desc; reverse flips.
    //! - `filters_to_sql` — Simple eq; And empty → TRUE; Or empty → FALSE;
    //!   And with 2; Or with 2; LIKE, ILIKE (→LIKE), NOT LIKE, NOT ILIKE
    //!   (→ NOT LIKE); IN literal array; NOT IN literal array; IS / IS NOT.
    //! - `filters_to_sql` — correlatedSubquery panics.
    //! - `filters_to_sql` — static parameter on left/right panics.
    //! - `build_select_query` — no where parts; with constraint only;
    //!   with start=After asc; with start=After desc; with start=At;
    //!   with filters; with start + filters + constraint.
    //! - `build_select_query` — reverse=true flips ORDER BY and start.

    use super::*;
    use rusqlite::types::Value as SqlValue;
    use serde_json::json;
    use zero_cache_types::ast::{
        AST, Condition, CorrelatedSubquery, CorrelatedSubqueryOp, Correlation, Direction,
        LiteralValue, NonColumnValue, ScalarLiteral, SimpleOperator, ValuePosition,
    };

    fn cols() -> IndexMap<String, SchemaValue> {
        let mut c = IndexMap::new();
        c.insert("id".to_string(), SchemaValue::new(ValueType::Number));
        c.insert("name".to_string(), SchemaValue::new(ValueType::String));
        c.insert("active".to_string(), SchemaValue::new(ValueType::Boolean));
        c
    }

    // Branch: escape_ident plain / with quote.
    #[test]
    fn escape_ident_variants() {
        assert_eq!(escape_ident("id"), "\"id\"");
        assert_eq!(escape_ident("we\"ird"), "\"we\"\"ird\"");
    }

    // Branch: constraints_to_sql None.
    #[test]
    fn constraints_none() {
        let (parts, params) = constraints_to_sql(None, &cols());
        assert!(parts.is_empty());
        assert!(params.is_empty());
    }

    // Branch: constraints_to_sql single entry.
    #[test]
    fn constraints_single() {
        let mut c = Constraint::new();
        c.insert("id".into(), Some(json!(7)));
        let (parts, params) = constraints_to_sql(Some(&c), &cols());
        assert_eq!(parts, vec!["\"id\" = ?".to_string()]);
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], SqlValue::Integer(7)));
    }

    // Branch: constraints_to_sql multiple entries preserved in order.
    #[test]
    fn constraints_multi() {
        let mut c = Constraint::new();
        c.insert("id".into(), Some(json!(1)));
        c.insert("name".into(), Some(json!("x")));
        let (parts, _params) = constraints_to_sql(Some(&c), &cols());
        assert_eq!(parts.len(), 2);
    }

    // Branch: order_by_to_sql forward asc/desc.
    #[test]
    fn order_by_forward() {
        let o: Ordering = vec![
            ("id".into(), Direction::Asc),
            ("name".into(), Direction::Desc),
        ];
        assert_eq!(
            order_by_to_sql(&o, false),
            "ORDER BY \"id\" asc, \"name\" desc"
        );
    }

    // Branch: reverse flips directions.
    #[test]
    fn order_by_reverse_flips() {
        let o: Ordering = vec![
            ("id".into(), Direction::Asc),
            ("name".into(), Direction::Desc),
        ];
        assert_eq!(
            order_by_to_sql(&o, true),
            "ORDER BY \"id\" desc, \"name\" asc"
        );
    }

    // Branch: Simple = condition.
    #[test]
    fn filters_simple_eq() {
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        };
        let (s, p) = filters_to_sql(&cond);
        assert_eq!(s, "\"id\" = ?");
        assert_eq!(p.len(), 1);
    }

    // Branch: IS condition renders IS.
    #[test]
    fn filters_simple_is() {
        let cond = Condition::Simple {
            op: SimpleOperator::IS,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Null,
            },
        };
        let (s, _p) = filters_to_sql(&cond);
        assert_eq!(s, "\"id\" IS ?");
    }

    // Branch: IS NOT condition.
    #[test]
    fn filters_simple_is_not() {
        let cond = Condition::Simple {
            op: SimpleOperator::IsNot,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Null,
            },
        };
        let (s, _p) = filters_to_sql(&cond);
        assert_eq!(s, "\"id\" IS NOT ?");
    }

    // Branch: LIKE.
    #[test]
    fn filters_like() {
        let cond = Condition::Simple {
            op: SimpleOperator::LIKE,
            left: ValuePosition::Column {
                name: "name".into(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::String("A%".into()),
            },
        };
        let (s, _) = filters_to_sql(&cond);
        assert_eq!(s, "\"name\" LIKE ?");
    }

    // Branch: NOT LIKE.
    #[test]
    fn filters_not_like() {
        let cond = Condition::Simple {
            op: SimpleOperator::NotLike,
            left: ValuePosition::Column {
                name: "name".into(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::String("A%".into()),
            },
        };
        let (s, _) = filters_to_sql(&cond);
        assert_eq!(s, "\"name\" NOT LIKE ?");
    }

    // Branch: ILIKE → LIKE (SQLite default is CI).
    #[test]
    fn filters_ilike_maps_to_like() {
        let cond = Condition::Simple {
            op: SimpleOperator::ILIKE,
            left: ValuePosition::Column {
                name: "name".into(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::String("a%".into()),
            },
        };
        let (s, _) = filters_to_sql(&cond);
        assert_eq!(s, "\"name\" LIKE ?");
    }

    // Branch: NOT ILIKE → NOT LIKE.
    #[test]
    fn filters_not_ilike_maps_to_not_like() {
        let cond = Condition::Simple {
            op: SimpleOperator::NotIlike,
            left: ValuePosition::Column {
                name: "name".into(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::String("a%".into()),
            },
        };
        let (s, _) = filters_to_sql(&cond);
        assert_eq!(s, "\"name\" NOT LIKE ?");
    }

    // Branch: IN literal array renders json_each.
    #[test]
    fn filters_in_literal_array() {
        let cond = Condition::Simple {
            op: SimpleOperator::IN,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Array(vec![
                    ScalarLiteral::Number(1.0),
                    ScalarLiteral::Number(2.0),
                ]),
            },
        };
        let (s, p) = filters_to_sql(&cond);
        assert_eq!(s, "\"id\" IN (SELECT value FROM json_each(?))");
        assert_eq!(p.len(), 1);
    }

    // Branch: NOT IN literal array.
    #[test]
    fn filters_not_in_literal_array() {
        let cond = Condition::Simple {
            op: SimpleOperator::NotIn,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Array(vec![ScalarLiteral::Number(1.0)]),
            },
        };
        let (s, _) = filters_to_sql(&cond);
        assert_eq!(s, "\"id\" NOT IN (SELECT value FROM json_each(?))");
    }

    // Branch: empty AND → TRUE.
    #[test]
    fn filters_empty_and_is_true() {
        let cond = Condition::And { conditions: vec![] };
        let (s, p) = filters_to_sql(&cond);
        assert_eq!(s, "TRUE");
        assert!(p.is_empty());
    }

    // Branch: empty OR → FALSE.
    #[test]
    fn filters_empty_or_is_false() {
        let cond = Condition::Or { conditions: vec![] };
        let (s, p) = filters_to_sql(&cond);
        assert_eq!(s, "FALSE");
        assert!(p.is_empty());
    }

    fn simple_eq(col: &str, v: f64) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: col.into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(v),
            },
        }
    }

    // Branch: AND with 2 conditions.
    #[test]
    fn filters_and_two() {
        let cond = Condition::And {
            conditions: vec![simple_eq("id", 1.0), simple_eq("active", 1.0)],
        };
        let (s, p) = filters_to_sql(&cond);
        assert_eq!(s, "(\"id\" = ? AND \"active\" = ?)");
        assert_eq!(p.len(), 2);
    }

    // Branch: OR with 2 conditions.
    #[test]
    fn filters_or_two() {
        let cond = Condition::Or {
            conditions: vec![simple_eq("id", 1.0), simple_eq("id", 2.0)],
        };
        let (s, p) = filters_to_sql(&cond);
        assert_eq!(s, "(\"id\" = ? OR \"id\" = ?)");
        assert_eq!(p.len(), 2);
    }

    // Branch: CorrelatedSubquery panics.
    #[test]
    #[should_panic(expected = "CorrelatedSubquery")]
    fn filters_correlated_subquery_panics() {
        let ast = AST {
            schema: None,
            table: "t".into(),
            alias: Some("a".into()),
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let cond = Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["uid".into()],
                },
                subquery: Box::new(ast),
                system: None,
                hidden: None,
            }),
            op: CorrelatedSubqueryOp::EXISTS,
            flip: None,
            scalar: None,
        };
        let _ = filters_to_sql(&cond);
    }

    // Branch: static left side panics.
    #[test]
    #[should_panic(expected = "Static parameters")]
    fn filters_static_left_panics() {
        use zero_cache_types::ast::{ParameterAnchor, ParameterField};
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Static {
                anchor: ParameterAnchor::AuthData,
                field: ParameterField::Single("uid".into()),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        };
        let _ = filters_to_sql(&cond);
    }

    // Branch: static right side panics.
    #[test]
    #[should_panic(expected = "Static parameters")]
    fn filters_static_right_panics() {
        use zero_cache_types::ast::{ParameterAnchor, ParameterField};
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Static {
                anchor: ParameterAnchor::AuthData,
                field: ParameterField::Single("uid".into()),
            },
        };
        let _ = filters_to_sql(&cond);
    }

    // Branch: static right side on IN panics.
    #[test]
    #[should_panic(expected = "Static parameters")]
    fn filters_static_in_panics() {
        use zero_cache_types::ast::{ParameterAnchor, ParameterField};
        let cond = Condition::Simple {
            op: SimpleOperator::IN,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Static {
                anchor: ParameterAnchor::AuthData,
                field: ParameterField::Single("ids".into()),
            },
        };
        let _ = filters_to_sql(&cond);
    }

    // Branch: build_select_query no where parts.
    #[test]
    fn build_no_where() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let (sql, params) = build_select_query("users", &cols(), None, None, &order, false, None);
        assert_eq!(
            sql,
            "SELECT \"id\",\"name\",\"active\" FROM \"users\" ORDER BY \"id\" asc"
        );
        assert!(params.is_empty());
    }

    // Branch: with constraint only.
    #[test]
    fn build_with_constraint() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut c = Constraint::new();
        c.insert("id".into(), Some(json!(7)));
        let (sql, params) =
            build_select_query("users", &cols(), Some(&c), None, &order, false, None);
        assert!(sql.contains("WHERE \"id\" = ?"));
        assert_eq!(params.len(), 1);
    }

    // Branch: with filters.
    #[test]
    fn build_with_filters() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = simple_eq("id", 1.0);
        let (sql, params) =
            build_select_query("users", &cols(), None, Some(&cond), &order, false, None);
        assert!(sql.contains("WHERE \"id\" = ?"));
        assert_eq!(params.len(), 1);
    }

    // Branch: with start After asc forward.
    #[test]
    fn build_with_start_after_asc() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(5)));
        let start = Start {
            row,
            basis: StartBasis::After,
        };
        let (sql, params) =
            build_select_query("users", &cols(), None, None, &order, false, Some(&start));
        // Forward + asc → (? IS NULL OR "id" > ?)
        assert!(sql.contains("(? IS NULL OR \"id\" > ?)"));
        assert!(!sql.contains("IS ?"));
        assert_eq!(params.len(), 2);
    }

    // Branch: with start After desc.
    #[test]
    fn build_with_start_after_desc() {
        let order: Ordering = vec![("id".into(), Direction::Desc)];
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(5)));
        let start = Start {
            row,
            basis: StartBasis::After,
        };
        let (sql, _params) =
            build_select_query("users", &cols(), None, None, &order, false, Some(&start));
        // Forward + desc → ("id" IS NULL OR "id" < ?)
        assert!(sql.contains("(\"id\" IS NULL OR \"id\" < ?)"));
    }

    // Branch: with start At asc — adds the at-group with IS.
    #[test]
    fn build_with_start_at_asc() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(5)));
        let start = Start {
            row,
            basis: StartBasis::At,
        };
        let (sql, _params) =
            build_select_query("users", &cols(), None, None, &order, false, Some(&start));
        // "at" clause contains `"id" IS ?`.
        assert!(sql.contains("\"id\" IS ?"));
    }

    // Branch: reverse flips direction and start.
    #[test]
    fn build_reverse_flips() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(5)));
        let start = Start {
            row,
            basis: StartBasis::After,
        };
        let (sql, _params) =
            build_select_query("users", &cols(), None, None, &order, true, Some(&start));
        // reverse + asc → ("id" IS NULL OR "id" < ?)
        assert!(sql.contains("(\"id\" IS NULL OR \"id\" < ?)"));
        assert!(sql.contains("ORDER BY \"id\" desc"));
    }

    // Branch: constraint + filters + start combine.
    #[test]
    fn build_combined() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut c = Constraint::new();
        c.insert("active".into(), Some(json!(true)));
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(5)));
        let start = Start {
            row,
            basis: StartBasis::After,
        };
        let cond = simple_eq("id", 10.0);
        let (sql, params) = build_select_query(
            "users",
            &cols(),
            Some(&c),
            Some(&cond),
            &order,
            false,
            Some(&start),
        );
        assert!(sql.contains("\"active\" = ?"));
        assert!(sql.contains("(? IS NULL OR \"id\" > ?)"));
        assert!(sql.contains("\"id\" = ?"));
        // constraint (1) + start (2) + filter (1) = 4
        assert_eq!(params.len(), 4);
    }

    // Branch: compound ordering with start emits nested groups.
    #[test]
    fn build_compound_start() {
        let order: Ordering = vec![
            ("id".into(), Direction::Asc),
            ("name".into(), Direction::Asc),
        ];
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(5)));
        row.insert("name".into(), Some(json!("x")));
        let start = Start {
            row,
            basis: StartBasis::After,
        };
        let (sql, _params) =
            build_select_query("users", &cols(), None, None, &order, false, Some(&start));
        // Group 0 (i=0): (? IS NULL OR "id" > ?)
        // Group 1 (i=1, j=0): ("id" IS ? AND (? IS NULL OR "name" > ?))
        assert!(sql.contains("(? IS NULL OR \"id\" > ?)"));
        assert!(sql.contains("\"id\" IS ? AND (? IS NULL OR \"name\" > ?)"));
    }

    // Default Ordering (empty).
    #[test]
    fn build_default_ordering_empty() {
        let order: Ordering = Vec::new();
        let (sql, _params) = build_select_query("users", &cols(), None, None, &order, false, None);
        // Empty ORDER BY clause is just "ORDER BY " — SQLite would reject
        // this; callers must pass a non-empty ordering. Documented.
        assert!(sql.starts_with("SELECT "));
    }

    // -----------------------------------------------------------------
    // Golden fixtures: 20 byte-for-byte comparisons against the TS
    // `buildSelectQuery` output, captured via
    // `scripts/dump-ts-sql.mjs`. The TS implementation is the oracle —
    // these tests guard the Rust port against drifting out of step with
    // it. If TS legitimately changes, update the goldens in lockstep.
    // -----------------------------------------------------------------

    fn golden_cols() -> IndexMap<String, SchemaValue> {
        let mut c = IndexMap::new();
        c.insert("id".into(), SchemaValue::new(ValueType::Number));
        c.insert("name".into(), SchemaValue::new(ValueType::String));
        c.insert("active".into(), SchemaValue::new(ValueType::Boolean));
        c.insert("age".into(), SchemaValue::new(ValueType::Number));
        c.insert("created_at".into(), SchemaValue::new(ValueType::Number));
        c
    }

    fn golden_special_cols() -> IndexMap<String, SchemaValue> {
        let mut c = IndexMap::new();
        c.insert("id".into(), SchemaValue::new(ValueType::Number));
        c.insert("we\"ird".into(), SchemaValue::new(ValueType::String));
        c
    }

    fn simple_num(col: &str, op: SimpleOperator, n: f64) -> Condition {
        Condition::Simple {
            op,
            left: ValuePosition::Column { name: col.into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(n),
            },
        }
    }
    fn simple_str(col: &str, op: SimpleOperator, s: &str) -> Condition {
        Condition::Simple {
            op,
            left: ValuePosition::Column { name: col.into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::String(s.into()),
            },
        }
    }
    fn simple_bool(col: &str, op: SimpleOperator, b: bool) -> Condition {
        Condition::Simple {
            op,
            left: ValuePosition::Column { name: col.into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Bool(b),
            },
        }
    }

    #[test]
    fn golden_01_no_filter_no_sort() {
        let order: Ordering = vec![];
        let (sql, _) = build_select_query("users", &golden_cols(), None, None, &order, false, None);
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" ORDER BY"#
        );
    }

    #[test]
    fn golden_02_order_id_asc() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let (sql, _) = build_select_query("users", &golden_cols(), None, None, &order, false, None);
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_03_order_id_desc() {
        let order: Ordering = vec![("id".into(), Direction::Desc)];
        let (sql, _) = build_select_query("users", &golden_cols(), None, None, &order, false, None);
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" ORDER BY "id" desc"#
        );
    }

    #[test]
    fn golden_04_order_multi_mixed() {
        let order: Ordering = vec![
            ("created_at".into(), Direction::Desc),
            ("id".into(), Direction::Asc),
        ];
        let (sql, _) = build_select_query("users", &golden_cols(), None, None, &order, false, None);
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" ORDER BY "created_at" desc, "id" asc"#
        );
    }

    #[test]
    fn golden_05_where_eq() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = simple_num("id", SimpleOperator::Eq, 7.0);
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE "id" = ? ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_06_where_gt() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = simple_num("age", SimpleOperator::Gt, 18.0);
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE "age" > ? ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_07_where_like() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = simple_str("name", SimpleOperator::LIKE, "A%");
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE "name" LIKE ? ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_08_where_and() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = Condition::And {
            conditions: vec![
                simple_num("age", SimpleOperator::Gt, 18.0),
                simple_str("name", SimpleOperator::Eq, "bob"),
            ],
        };
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE ("age" > ? AND "name" = ?) ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_09_where_or() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = Condition::Or {
            conditions: vec![
                simple_num("age", SimpleOperator::Gt, 18.0),
                simple_str("name", SimpleOperator::Eq, "bob"),
            ],
        };
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE ("age" > ? OR "name" = ?) ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_10_where_nested() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = Condition::Or {
            conditions: vec![
                Condition::And {
                    conditions: vec![
                        simple_num("age", SimpleOperator::Gt, 18.0),
                        simple_str("name", SimpleOperator::Eq, "bob"),
                    ],
                },
                simple_bool("active", SimpleOperator::Eq, true),
            ],
        };
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE (("age" > ? AND "name" = ?) OR "active" = ?) ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_11_limit_noop() {
        // Parallel to TS fixture 11 — `buildSelectQuery` has no LIMIT
        // parameter. Behaviour equals the simple ordered, unfiltered
        // fixture.
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let (sql, _) = build_select_query("users", &golden_cols(), None, None, &order, false, None);
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_12_combined() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut c = Constraint::new();
        c.insert("id".into(), Some(json!(7)));
        let cond = simple_str("name", SimpleOperator::Eq, "bob");
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            Some(&c),
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE "id" = ? AND "name" = ? ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_13_start_after_asc() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(5)));
        let start = Start {
            row,
            basis: StartBasis::After,
        };
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            None,
            &order,
            false,
            Some(&start),
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE (((? IS NULL OR "id" > ?))) ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_14_start_at_reverse() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(5)));
        let start = Start {
            row,
            basis: StartBasis::At,
        };
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            None,
            &order,
            true,
            Some(&start),
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE ((("id" IS NULL OR "id" < ?)) OR ("id" IS ?)) ORDER BY "id" desc"#
        );
    }

    #[test]
    fn golden_15_constraint_single() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut c = Constraint::new();
        c.insert("id".into(), Some(json!(7)));
        let (sql, _) =
            build_select_query("users", &golden_cols(), Some(&c), None, &order, false, None);
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE "id" = ? ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_16_constraint_compound() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let mut c = Constraint::new();
        c.insert("id".into(), Some(json!(7)));
        c.insert("active".into(), Some(json!(true)));
        let (sql, _) =
            build_select_query("users", &golden_cols(), Some(&c), None, &order, false, None);
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE "id" = ? AND "active" = ? ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_17_special_column() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = simple_str("we\"ird", SimpleOperator::Eq, "x");
        let (sql, _) = build_select_query(
            "users",
            &golden_special_cols(),
            None,
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            "SELECT \"id\",\"we\"\"ird\" FROM \"users\" WHERE \"we\"\"ird\" = ? ORDER BY \"id\" asc"
        );
    }

    #[test]
    fn golden_18_reverse_order() {
        let order: Ordering = vec![
            ("id".into(), Direction::Asc),
            ("name".into(), Direction::Desc),
        ];
        let (sql, _) = build_select_query("users", &golden_cols(), None, None, &order, true, None);
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" ORDER BY "id" desc, "name" asc"#
        );
    }

    #[test]
    fn golden_19_not_equal() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = simple_str("name", SimpleOperator::Ne, "bob");
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE "name" != ? ORDER BY "id" asc"#
        );
    }

    #[test]
    fn golden_20_in_literal() {
        let order: Ordering = vec![("id".into(), Direction::Asc)];
        let cond = Condition::Simple {
            op: SimpleOperator::IN,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Array(vec![
                    ScalarLiteral::Number(1.0),
                    ScalarLiteral::Number(2.0),
                    ScalarLiteral::Number(3.0),
                ]),
            },
        };
        let (sql, _) = build_select_query(
            "users",
            &golden_cols(),
            None,
            Some(&cond),
            &order,
            false,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT "id","name","active","age","created_at" FROM "users" WHERE "id" IN (SELECT value FROM json_each(?)) ORDER BY "id" asc"#
        );
    }
}
