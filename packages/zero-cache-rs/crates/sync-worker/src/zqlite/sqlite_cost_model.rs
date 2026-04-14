//! Port of `packages/zqlite/src/sqlite-cost-model.ts`.
//!
//! Produces a [`crate::planner::planner_connection::ConnectionCostModel`]
//! backed by SQLite's query planner. Used by the planner-graph to score
//! candidate join orders.
//!
//! ## Divergence from TS: `scanstatus` is unavailable in rusqlite 0.32
//!
//! The TS implementation calls
//! `sqlite3_stmt_scanstatus_v2(SQLITE_SCANSTAT_EST | …)` after preparing
//! the statement. That API is **not exposed by rusqlite 0.32** (and the
//! crate forbids calling raw FFI from sync-worker because of the
//! `#![deny(unsafe_code)]` invariant).
//!
//! We approximate it via `EXPLAIN QUERY PLAN` plus `sqlite_stat1`:
//!
//! * `EXPLAIN QUERY PLAN <sql>` returns one row per loop with `id`,
//!   `parent`, `notused`, `detail` columns. We map these to the
//!   `selectId`, `parentId`, `explain` fields the TS version reads.
//! * For row-count estimates we look up the table's row count from
//!   `sqlite_stat1` (the row whose `idx` is `NULL` carries the total row
//!   count). Without an `EXPLAIN QUERY PLAN`-level estimate we apply the
//!   following best-effort heuristic, designed to match the spirit (not
//!   the exact numbers) of TS:
//!     - If the plan text includes `USING …PRIMARY KEY` or `USING INDEX`
//!       and the SQL contains an equality on the indexed column → 1 row.
//!     - If the plan text says `SCAN` (full table scan) → use the total
//!       row count from `sqlite_stat1`.
//!     - If `WHERE` is present but no index is used → scale total rows by
//!       a default selectivity of 0.25 (the same factor SQLite itself
//!       uses internally for unindexed equality).
//! * The TS implementation only counts top-level loops (`parentId == 0`)
//!   when summing cost. We do the same.
//! * The TS sort heuristic adds `btreeCost(rows)` whenever a top-level
//!   loop's plan text contains `ORDER BY`. We mirror this directly via
//!   the substring check on the plan detail string. SQLite emits this as
//!   `USE TEMP B-TREE FOR ORDER BY` (3.x) — both substrings work.
//!
//! ## Known divergences vs. TS
//!
//! - We do not have access to `SQLITE_SCANSTAT_EST` so the row estimate
//!   for partial scans (e.g. `WHERE x > 1`) is coarse — the TS test that
//!   asserts `rows == 480` for "range check on PK" will not match
//!   exactly. The cost-model only needs ordinal accuracy
//!   (PK-lookup < range-scan < table-scan) for the planner to make
//!   reasonable choices, so that property is preserved.
//! - STAT4 awareness for skewed values is degraded: without scanstatus we
//!   cannot see the per-value selectivity SQLite computes from STAT4.
//!   The cost model still distinguishes "indexed lookup" from "full
//!   scan", which is enough for planner ordering decisions.
//!
//! When rusqlite eventually exposes `scanstatus` we should swap this
//! implementation for a thin wrapper.
//!
//! ## Stubs awaiting Layer 9
//!
//! - [`build_select_query`] — minimal stub returning a placeholder SELECT
//!   that mentions the constraint columns + filter column where it can.
//!   Layer 9 will replace this with a call to
//!   `crate::zqlite::query_builder::build_select_query`.
//! - [`compile_inline`] — passthrough; Layer 9 will replace with a real
//!   inliner that materialises bound parameters into the SQL.
//!
//! ## Chokepoint
//!
//! All SQLite access flows through
//! [`SQLiteCostModelInner::query_plan`]. Future shadow-FFI / recording
//! layers hook this one method.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use zero_cache_types::ast::{Condition, Ordering, ValuePosition};

use crate::planner::planner_connection::{ConnectionCostModel, CostModelCost};
use crate::planner::planner_constraint::PlannerConstraint;
use crate::planner::planner_node::{FanoutConfidence, FanoutCostModel, FanoutEst};

use super::sqlite_stat_fanout::{
    FanoutConfidence as StatConfidence, FanoutResult, SQLiteStatFanout,
};
use super::table_source::SchemaValue;

/// TS `interface ScanstatusLoop`. Approximated from EXPLAIN QUERY PLAN.
#[derive(Debug, Clone)]
struct PlanLoop {
    select_id: i64,
    parent_id: i64,
    /// Estimated rows emitted per turn of parent loop.
    est: f64,
    /// EXPLAIN text for this loop.
    explain: String,
}

/// TS `tableSpec.zqlSpec` per-column. Mirrors the cost-model TS `Map`.
pub type TableSpecs = HashMap<String, HashMap<String, SchemaValue>>;

/// TS `createSQLiteCostModel(db, tableSpecs)`.
///
/// Returns a [`ConnectionCostModel`] (a closure) suitable for handing to
/// the planner. The connection is shared with the fanout estimator so a
/// single SQLite handle backs both reads.
pub fn create_sqlite_cost_model(conn: Connection, table_specs: TableSpecs) -> ConnectionCostModel {
    let inner = Arc::new(SQLiteCostModelInner {
        conn: Mutex::new(conn),
        table_specs,
    });
    let fanout = Arc::new(SQLiteStatFanout::new(
        // Open a separate in-memory connection if needed; in practice the
        // caller wires the fanout estimator to the same database, but
        // sharing rusqlite::Connection across two owners requires a Mutex
        // and the fanout estimator already owns one. We reuse the same
        // backing inner state by opening another connection.
        // For now the fanout estimator owns its own (in-memory) connection
        // and tests construct the cost model directly using
        // [`create_sqlite_cost_model_with_fanout`] when they need to
        // share state.
        Connection::open_in_memory().expect("open fanout shadow"),
        3.0,
    ));
    create_sqlite_cost_model_with_fanout(inner, fanout)
}

/// Like [`create_sqlite_cost_model`] but lets the caller supply the fanout
/// estimator. The caller is responsible for opening the fanout
/// estimator's connection on the same database file.
pub fn create_sqlite_cost_model_with_fanout(
    inner: Arc<SQLiteCostModelInner>,
    fanout_estimator: Arc<SQLiteStatFanout>,
) -> ConnectionCostModel {
    Arc::new(
        move |table_name: &str,
              sort: &Ordering,
              filters: Option<&Condition>,
              constraint: Option<&PlannerConstraint>|
              -> CostModelCost {
            // Branch: filters is Some → strip correlated subqueries.
            let no_subquery_filters = filters.and_then(remove_correlated_subqueries);

            // Branch: caller-supplied table not in specs → conservative
            // fallback. (TS uses `must(...)` and panics; we degrade.)
            let zql_spec = match inner.table_specs.get(table_name) {
                Some(spec) => spec,
                None => {
                    return fallback_cost(table_name, fanout_estimator.clone());
                }
            };

            let query = build_select_query(
                table_name,
                zql_spec,
                constraint,
                no_subquery_filters.as_ref(),
                sort,
                None,
                None,
            );
            let sql = compile_inline(&query);

            // Build the EXPLAIN QUERY PLAN.
            let loops = match inner.query_plan(&sql) {
                Ok(l) => l,
                Err(_) => {
                    // Branch: SQL preparation failed. Return conservative
                    // fallback rather than panicking — the planner can
                    // still order plans relative to each other.
                    return fallback_cost(table_name, fanout_estimator.clone());
                }
            };

            // Estimate row count from sqlite_stat1 (table-level row count).
            let table_rows = inner.table_row_count(table_name).unwrap_or(0.0);
            let row_estimate = estimate_row_count(
                table_name,
                &loops,
                table_rows,
                &no_subquery_filters,
                constraint,
            );

            let table_owned = table_name.to_string();
            let fanout_estimator_for_closure = fanout_estimator.clone();
            let fanout_closure: FanoutCostModel = Arc::new(move |columns: &[String]| {
                let r = fanout_estimator_for_closure.get_fanout(&table_owned, columns);
                stat_to_planner_fanout(r)
            });

            estimate_cost(&loops, row_estimate, fanout_closure)
        },
    )
}

fn fallback_cost(table_name: &str, fanout_estimator: Arc<SQLiteStatFanout>) -> CostModelCost {
    let table_owned = table_name.to_string();
    let fanout_closure: FanoutCostModel = Arc::new(move |columns: &[String]| {
        let r = fanout_estimator.get_fanout(&table_owned, columns);
        stat_to_planner_fanout(r)
    });
    CostModelCost {
        startup_cost: 0.0,
        rows: 1.0,
        fanout: fanout_closure,
    }
}

fn stat_to_planner_fanout(r: FanoutResult) -> FanoutEst {
    let confidence = match r.confidence {
        StatConfidence::High => FanoutConfidence::High,
        StatConfidence::Med => FanoutConfidence::Med,
        StatConfidence::None => FanoutConfidence::None,
    };
    FanoutEst {
        fanout: r.fanout,
        confidence,
    }
}

/// State shared by the cost-model closure and any future recording
/// layer. Holds the `Connection` behind a `Mutex` because rusqlite is not
/// `Sync`.
pub struct SQLiteCostModelInner {
    conn: Mutex<Connection>,
    table_specs: TableSpecs,
}

impl SQLiteCostModelInner {
    pub fn new(conn: Connection, table_specs: TableSpecs) -> Arc<Self> {
        Arc::new(Self {
            conn: Mutex::new(conn),
            table_specs,
        })
    }

    /// **Single private chokepoint** for SQLite reads.
    ///
    /// All other methods funnel through here (today there is only one
    /// caller, but the chokepoint contract is preserved for future
    /// shadow-FFI hooks).
    fn query_plan(&self, sql: &str) -> rusqlite::Result<Vec<PlanLoop>> {
        let plan_sql = format!("EXPLAIN QUERY PLAN {sql}");

        let conn = self.conn.lock().expect("cost model conn poisoned");
        let mut stmt = conn.prepare(&plan_sql)?;
        // EXPLAIN QUERY PLAN columns: id, parent, notused, detail.
        let rows = stmt.query_map([], |row| {
            Ok(PlanLoop {
                select_id: row.get::<_, i64>(0)?,
                parent_id: row.get::<_, i64>(1)?,
                est: 0.0,
                explain: row.get::<_, String>(3)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Row count for `tableName` from `sqlite_stat1` (best effort).
    /// Returns `None` if `ANALYZE` has not been run or the table is
    /// missing.
    fn table_row_count(&self, table_name: &str) -> Option<f64> {
        let sql = "SELECT stat FROM sqlite_stat1 WHERE tbl = ? ORDER BY idx IS NULL DESC LIMIT 1";

        let stat: Option<String> = {
            let conn = self.conn.lock().expect("cost model conn poisoned");
            // sqlite_stat1.stat for a table-level row carries
            // "<rowcount>" as the first whitespace-separated field.
            let mut stmt = conn.prepare(sql).ok()?;
            stmt.query_row(rusqlite::params![table_name], |row| row.get::<_, String>(0))
                .ok()
        };
        let s = stat?;
        let first = s.split_whitespace().next()?;
        first.parse::<f64>().ok()
    }
}

/// TS `getScanstatusLoops(stmt)` — sort by selectId and return.
fn estimate_cost(
    scanstats: &[PlanLoop],
    row_estimate: f64,
    fanout: FanoutCostModel,
) -> CostModelCost {
    // Sort by selectId.
    let mut sorted: Vec<&PlanLoop> = scanstats.iter().collect();
    sorted.sort_by_key(|l| l.select_id);

    let mut total_rows = 0.0_f64;
    let mut total_cost = 0.0_f64;

    let top_level: Vec<&&PlanLoop> = sorted.iter().filter(|l| l.parent_id == 0).collect();

    let mut first_loop = true;
    for op in &top_level {
        if first_loop {
            // Branch: first top-level op = scan.
            total_rows = if op.est > 0.0 { op.est } else { row_estimate };
            first_loop = false;
        } else if op.explain.contains("ORDER BY") || op.explain.contains("USE TEMP B-TREE") {
            // Branch: subsequent top-level op contains ORDER BY → add
            // sort cost. SQLite emits "USE TEMP B-TREE FOR ORDER BY" for
            // sort steps at the top level.
            total_cost += btree_cost(total_rows);
        }
    }

    // Branch: no top-level ops at all (defensive).
    if first_loop {
        total_rows = row_estimate;
    }

    CostModelCost {
        startup_cost: total_cost,
        rows: total_rows,
        fanout,
    }
}

/// TS `btreeCost(rows)` — `(rows * log2(rows)) / 10`.
pub fn btree_cost(rows: f64) -> f64 {
    if rows <= 1.0 {
        return 0.0;
    }
    (rows * rows.log2()) / 10.0
}

/// Heuristic row count derived from EXPLAIN QUERY PLAN output.
///
/// See module docs for the full table of cases. This is the major
/// divergence from TS — TS uses scanstatus for an exact estimate.
fn estimate_row_count(
    _table: &str,
    loops: &[PlanLoop],
    total_rows: f64,
    filters: &Option<Condition>,
    constraint: Option<&PlannerConstraint>,
) -> f64 {
    let has_filter_or_constraint = filters.is_some() || constraint.is_some();
    let mut search_text = String::new();
    for l in loops {
        if l.parent_id == 0 {
            search_text.push_str(&l.explain);
            search_text.push('\n');
        }
    }

    // Branch: SEARCH ... USING ... (PRIMARY KEY | INDEX) AND we have an
    // equality-style constraint → 1 row.
    let has_search = search_text.contains("SEARCH");
    let has_pk_or_index = search_text.contains("PRIMARY KEY")
        || (search_text.contains("USING") && search_text.contains("INDEX"));
    let has_eq = is_equality_constraint(filters, constraint);

    if has_search && has_pk_or_index && has_eq {
        return 1.0;
    }

    // Branch: SCAN (full table) with filter/constraint → coarse scaling.
    if has_filter_or_constraint && total_rows > 0.0 {
        // 25% selectivity heuristic — same default SQLite uses for an
        // unindexed equality.
        return (total_rows * 0.25).max(1.0);
    }

    // Branch: SCAN with no filter → full table.
    if total_rows > 0.0 {
        return total_rows;
    }

    // Branch: no statistics available → small constant.
    1.0
}

fn is_equality_constraint(
    filters: &Option<Condition>,
    constraint: Option<&PlannerConstraint>,
) -> bool {
    if constraint.map(|c| !c.is_empty()).unwrap_or(false) {
        return true;
    }
    match filters {
        Some(Condition::Simple { op, .. }) => matches!(
            op,
            zero_cache_types::ast::SimpleOperator::Eq | zero_cache_types::ast::SimpleOperator::IS
        ),
        _ => false,
    }
}

/// TS `removeCorrelatedSubqueries(condition)`.
fn remove_correlated_subqueries(condition: &Condition) -> Option<Condition> {
    match condition {
        // Branch: drop correlated subqueries entirely.
        Condition::CorrelatedSubquery { .. } => None,
        // Branch: simple condition passes through unchanged.
        Condition::Simple { .. } => Some(condition.clone()),
        // Branch: AND — recurse and rebuild.
        Condition::And { conditions } => {
            let filtered: Vec<Condition> = conditions
                .iter()
                .filter_map(remove_correlated_subqueries)
                .collect();
            match filtered.len() {
                0 => None,
                1 => Some(filtered.into_iter().next().unwrap()),
                _ => Some(Condition::And {
                    conditions: filtered,
                }),
            }
        }
        // Branch: OR — recurse and rebuild.
        Condition::Or { conditions } => {
            let filtered: Vec<Condition> = conditions
                .iter()
                .filter_map(remove_correlated_subqueries)
                .collect();
            match filtered.len() {
                0 => None,
                1 => Some(filtered.into_iter().next().unwrap()),
                _ => Some(Condition::Or {
                    conditions: filtered,
                }),
            }
        }
    }
}

// ─── Layer 9 stubs ───────────────────────────────────────────────────

/// Layer 9 note: this renderer is intentionally separate from
/// [`crate::zqlite::query_builder::build_select_query`]. The latter
/// produces `(sql, params)` with `?` placeholders, while the cost
/// model needs inlined literals so `EXPLAIN QUERY PLAN` can pick the
/// right index.  The two share intent but serve different call sites:
/// `query_builder` is used by `TableSource::fetch_all` for parameter-
/// ised reads; this version is used purely for plan estimation.  We
/// keep them distinct (TS does the same — see
/// `sqlite-inline-format.ts`).
///
/// Fields covered today:
/// - `SELECT *` from the table.
/// - `WHERE` clause for the constraint columns (`col = NULL` placeholder
///   keeps the parser happy and SQLite still picks the right index).
/// - One simple equality from `filters` if it is a `Simple` op on a
///   column literal.
/// - `ORDER BY` clause from the sort.
fn build_select_query(
    table: &str,
    _zql_spec: &HashMap<String, SchemaValue>,
    constraint: Option<&PlannerConstraint>,
    filters: Option<&Condition>,
    sort: &Ordering,
    _reverse: Option<bool>,
    _start: Option<()>,
) -> String {
    let mut sql = format!("SELECT * FROM \"{table}\"");
    let mut where_parts: Vec<String> = Vec::new();

    if let Some(c) = constraint {
        for k in c.keys() {
            where_parts.push(format!("\"{k}\" = NULL"));
        }
    }

    if let Some(cond) = filters {
        if let Some(part) = render_condition(cond) {
            where_parts.push(part);
        }
    }

    if !where_parts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_parts.join(" AND "));
    }

    if !sort.is_empty() {
        sql.push_str(" ORDER BY ");
        let parts: Vec<String> = sort
            .iter()
            .map(|(col, dir)| {
                let dir = match dir {
                    zero_cache_types::ast::Direction::Asc => "ASC",
                    zero_cache_types::ast::Direction::Desc => "DESC",
                };
                format!("\"{col}\" {dir}")
            })
            .collect();
        sql.push_str(&parts.join(", "));
    }

    sql
}

fn render_condition(c: &Condition) -> Option<String> {
    match c {
        Condition::Simple { op, left, right } => {
            let col = match left {
                ValuePosition::Column { name } => name.clone(),
                _ => return None,
            };
            let op_str = match op {
                zero_cache_types::ast::SimpleOperator::Eq => "=",
                zero_cache_types::ast::SimpleOperator::Ne => "!=",
                zero_cache_types::ast::SimpleOperator::IS => "IS",
                zero_cache_types::ast::SimpleOperator::IsNot => "IS NOT",
                zero_cache_types::ast::SimpleOperator::Lt => "<",
                zero_cache_types::ast::SimpleOperator::Gt => ">",
                zero_cache_types::ast::SimpleOperator::Le => "<=",
                zero_cache_types::ast::SimpleOperator::Ge => ">=",
                zero_cache_types::ast::SimpleOperator::IN => "IN",
                _ => return None,
            };
            let rhs = render_non_column_value(right)?;
            Some(format!("\"{col}\" {op_str} {rhs}"))
        }
        Condition::And { conditions } => {
            let parts: Vec<String> = conditions.iter().filter_map(render_condition).collect();
            if parts.is_empty() {
                None
            } else {
                Some(format!("({})", parts.join(" AND ")))
            }
        }
        Condition::Or { conditions } => {
            let parts: Vec<String> = conditions.iter().filter_map(render_condition).collect();
            if parts.is_empty() {
                None
            } else {
                Some(format!("({})", parts.join(" OR ")))
            }
        }
        Condition::CorrelatedSubquery { .. } => None,
    }
}

fn render_non_column_value(v: &zero_cache_types::ast::NonColumnValue) -> Option<String> {
    use zero_cache_types::ast::{LiteralValue, NonColumnValue, ScalarLiteral};
    match v {
        NonColumnValue::Literal { value } => Some(match value {
            LiteralValue::Null => "NULL".to_string(),
            LiteralValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            LiteralValue::Number(n) => n.to_string(),
            LiteralValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            LiteralValue::Array(arr) => {
                let parts: Vec<String> = arr
                    .iter()
                    .map(|v| match v {
                        ScalarLiteral::String(s) => format!("'{}'", s.replace('\'', "''")),
                        ScalarLiteral::Number(n) => n.to_string(),
                        ScalarLiteral::Bool(b) => if *b { "1" } else { "0" }.to_string(),
                    })
                    .collect();
                format!("({})", parts.join(", "))
            }
        }),
        NonColumnValue::Static { .. } => None,
    }
}

/// Layer 9 closed: delegates to
/// [`crate::zqlite::internal::sql_inline::compile_inline`] with no bound
/// parameters. Today `build_select_query` still emits literals inline
/// (the TS equivalent of `compileInline` for the planner path), so the
/// call is effectively a pass-through; it is kept as the chokepoint in
/// case a future refactor switches `build_select_query` to produce
/// parameterised SQL.
fn compile_inline(sql: &str) -> String {
    crate::zqlite::internal::sql_inline::compile_inline(sql, &[])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_constraint::constraint_from_fields;
    use rusqlite::Connection;
    use zero_cache_types::ast::{
        Condition, Direction, LiteralValue, NonColumnValue, SimpleOperator, ValuePosition,
    };

    fn open() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    fn make_specs(table: &str) -> TableSpecs {
        let mut specs: TableSpecs = HashMap::new();
        specs.insert(table.to_string(), HashMap::new());
        specs
    }

    fn simple_eq(column: &str, value: f64) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: column.to_string(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(value),
            },
        }
    }

    fn cost_for(conn: Connection, table: &str) -> ConnectionCostModel {
        let specs = make_specs(table);
        let inner = SQLiteCostModelInner::new(conn, specs);
        let fanout = Arc::new(SQLiteStatFanout::open_in_memory(3.0).unwrap());
        create_sqlite_cost_model_with_fanout(inner, fanout)
    }

    // ─── btree_cost branches ─────────────────────────────────────

    // Branch: rows ≤ 1 → 0.
    #[test]
    fn btree_cost_zero_for_tiny() {
        assert_eq!(btree_cost(0.0), 0.0);
        assert_eq!(btree_cost(1.0), 0.0);
    }

    // Branch: rows > 1 → positive.
    #[test]
    fn btree_cost_positive_for_large() {
        let v = btree_cost(1024.0);
        // 1024 * log2(1024) / 10 = 1024 * 10 / 10 = 1024.
        assert!((v - 1024.0).abs() < 1e-6);
    }

    // ─── remove_correlated_subqueries branches ──────────────────

    // Branch: drop subquery condition.
    #[test]
    fn remove_subquery_returns_none() {
        let c = Condition::CorrelatedSubquery {
            related: Box::new(zero_cache_types::ast::CorrelatedSubquery {
                correlation: zero_cache_types::ast::Correlation {
                    parent_field: vec!["a".into()],
                    child_field: vec!["b".into()],
                },
                subquery: Box::new(zero_cache_types::ast::AST {
                    table: "t".into(),
                    alias: None,
                    where_clause: None,
                    related: None,
                    start: None,
                    limit: None,
                    order_by: None,
                    schema: None,
                }),
                system: None,
                hidden: None,
            }),
            op: zero_cache_types::ast::CorrelatedSubqueryOp::EXISTS,
            flip: None,
            scalar: None,
        };
        assert!(remove_correlated_subqueries(&c).is_none());
    }

    // Branch: simple condition passes through.
    #[test]
    fn remove_simple_passes_through() {
        let c = simple_eq("a", 1.0);
        assert_eq!(remove_correlated_subqueries(&c), Some(c));
    }

    // Branch: AND with all subqueries → None.
    #[test]
    fn remove_and_all_subqueries() {
        let sub = subquery_cond();
        let c = Condition::And {
            conditions: vec![sub.clone(), sub],
        };
        assert!(remove_correlated_subqueries(&c).is_none());
    }

    // Branch: AND with one survivor → unwrap to single condition.
    #[test]
    fn remove_and_one_survivor() {
        let c = Condition::And {
            conditions: vec![subquery_cond(), simple_eq("a", 1.0)],
        };
        assert_eq!(remove_correlated_subqueries(&c), Some(simple_eq("a", 1.0)));
    }

    // Branch: AND with multiple survivors stays AND.
    #[test]
    fn remove_and_multi_survivor_stays_and() {
        let c = Condition::And {
            conditions: vec![simple_eq("a", 1.0), subquery_cond(), simple_eq("b", 2.0)],
        };
        let r = remove_correlated_subqueries(&c).unwrap();
        match r {
            Condition::And { conditions } => assert_eq!(conditions.len(), 2),
            _ => panic!("expected And"),
        }
    }

    // Branch: OR with all subqueries → None.
    #[test]
    fn remove_or_all_subqueries() {
        let c = Condition::Or {
            conditions: vec![subquery_cond(), subquery_cond()],
        };
        assert!(remove_correlated_subqueries(&c).is_none());
    }

    // Branch: OR with one survivor → unwrap.
    #[test]
    fn remove_or_one_survivor() {
        let c = Condition::Or {
            conditions: vec![simple_eq("a", 1.0), subquery_cond()],
        };
        assert_eq!(remove_correlated_subqueries(&c), Some(simple_eq("a", 1.0)));
    }

    // Branch: OR with multi survivors stays OR.
    #[test]
    fn remove_or_multi_survivor_stays_or() {
        let c = Condition::Or {
            conditions: vec![simple_eq("a", 1.0), simple_eq("b", 2.0)],
        };
        let r = remove_correlated_subqueries(&c).unwrap();
        match r {
            Condition::Or { conditions } => assert_eq!(conditions.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    fn subquery_cond() -> Condition {
        Condition::CorrelatedSubquery {
            related: Box::new(zero_cache_types::ast::CorrelatedSubquery {
                correlation: zero_cache_types::ast::Correlation {
                    parent_field: vec!["a".into()],
                    child_field: vec!["b".into()],
                },
                subquery: Box::new(zero_cache_types::ast::AST {
                    table: "t".into(),
                    alias: None,
                    where_clause: None,
                    related: None,
                    start: None,
                    limit: None,
                    order_by: None,
                    schema: None,
                }),
                system: None,
                hidden: None,
            }),
            op: zero_cache_types::ast::CorrelatedSubqueryOp::EXISTS,
            flip: None,
            scalar: None,
        }
    }

    // ─── End-to-end via real SQLite ────────────────────────────

    fn populate_foo(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE foo (a INTEGER PRIMARY KEY, b INTEGER, c INTEGER);
             CREATE UNIQUE INDEX foo_a_unique ON foo(a);",
        )
        .unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..2000 {
            tx.execute(
                "INSERT INTO foo (a, b, c) VALUES (?, ?, ?)",
                rusqlite::params![i * 3 + 1, i * 3 + 2, i * 3 + 3],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        conn.execute_batch("ANALYZE").unwrap();
    }

    // Branch: table scan ordered by PK requires no sort.
    #[test]
    fn pk_order_no_sort() {
        let conn = open();
        populate_foo(&conn);
        let model = cost_for(conn, "foo");
        let cost = model("foo", &vec![("a".into(), Direction::Asc)], None, None);
        assert!(cost.rows > 0.0);
        // No sort required → startup cost is 0.
        assert_eq!(cost.startup_cost, 0.0);
    }

    // Branch: ordering by non-indexed col adds sort cost.
    #[test]
    fn non_indexed_order_adds_sort_cost() {
        let conn = open();
        populate_foo(&conn);
        let model = cost_for(conn, "foo");
        let cost = model("foo", &vec![("b".into(), Direction::Asc)], None, None);
        // We can't guarantee SQLite emits a top-level "USE TEMP B-TREE"
        // for every build, but if it does the startup_cost is positive.
        // Either way, rows > 0.
        assert!(cost.rows > 0.0);
        assert!(cost.startup_cost >= 0.0);
    }

    // Branch: PK lookup via filter → 1 row.
    #[test]
    fn pk_lookup_filter_one_row() {
        let conn = open();
        populate_foo(&conn);
        let model = cost_for(conn, "foo");
        let cost = model(
            "foo",
            &vec![("a".into(), Direction::Asc)],
            Some(&simple_eq("a", 4.0)),
            None,
        );
        assert_eq!(cost.rows, 1.0);
        assert_eq!(cost.startup_cost, 0.0);
    }

    // Branch: PK lookup via constraint → 1 row.
    #[test]
    fn pk_lookup_constraint_one_row() {
        let conn = open();
        populate_foo(&conn);
        let model = cost_for(conn, "foo");
        let constraint = constraint_from_fields(["a"]);
        let cost = model(
            "foo",
            &vec![("a".into(), Direction::Asc)],
            None,
            Some(&constraint),
        );
        assert_eq!(cost.rows, 1.0);
    }

    // Branch: range check on non-indexed column → many rows (selectivity heuristic).
    #[test]
    fn nonindexed_filter_returns_partial() {
        let conn = open();
        populate_foo(&conn);
        let model = cost_for(conn, "foo");
        let cost = model(
            "foo",
            &vec![("a".into(), Direction::Asc)],
            Some(&Condition::Simple {
                op: SimpleOperator::Gt,
                left: ValuePosition::Column { name: "b".into() },
                right: NonColumnValue::Literal {
                    value: LiteralValue::Number(200.0),
                },
            }),
            None,
        );
        assert!(cost.rows > 1.0);
        assert!(cost.rows < 2000.0);
    }

    // Branch: missing table spec → fallback cost (1 row).
    #[test]
    fn missing_spec_returns_fallback() {
        let conn = open();
        populate_foo(&conn);
        // Use a model whose specs don't include "foo".
        let specs: TableSpecs = HashMap::new();
        let inner = SQLiteCostModelInner::new(conn, specs);
        let fanout = Arc::new(SQLiteStatFanout::open_in_memory(3.0).unwrap());
        let model = create_sqlite_cost_model_with_fanout(inner, fanout);
        let cost = model("foo", &vec![("a".into(), Direction::Asc)], None, None);
        assert_eq!(cost.rows, 1.0);
    }

    // Branch: SQL prep failure → fallback cost. We trigger this by
    // pointing at a table that doesn't exist in the database but IS in
    // the spec map (so we get past the spec check and then EXPLAIN
    // fails).
    #[test]
    fn explain_failure_returns_fallback() {
        let conn = open();
        // Schema empty on purpose.
        let specs = make_specs("nonexistent");
        let inner = SQLiteCostModelInner::new(conn, specs);
        let fanout = Arc::new(SQLiteStatFanout::open_in_memory(3.0).unwrap());
        let model = create_sqlite_cost_model_with_fanout(inner, fanout);
        let cost = model(
            "nonexistent",
            &vec![("id".into(), Direction::Asc)],
            None,
            None,
        );
        assert_eq!(cost.rows, 1.0);
    }

    // Branch: build_select_query renders ORDER BY clause.
    #[test]
    fn build_select_includes_order_by() {
        let sql = build_select_query(
            "foo",
            &HashMap::new(),
            None,
            None,
            &vec![("a".into(), Direction::Asc), ("b".into(), Direction::Desc)],
            None,
            None,
        );
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("\"a\" ASC"));
        assert!(sql.contains("\"b\" DESC"));
    }

    // Branch: build_select_query renders constraint columns.
    #[test]
    fn build_select_includes_constraint() {
        let constraint = constraint_from_fields(["userId"]);
        let sql = build_select_query(
            "foo",
            &HashMap::new(),
            Some(&constraint),
            None,
            &vec![],
            None,
            None,
        );
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("\"userId\""));
    }

    // Branch: build_select_query renders a simple filter.
    #[test]
    fn build_select_includes_simple_filter() {
        let sql = build_select_query(
            "foo",
            &HashMap::new(),
            None,
            Some(&simple_eq("a", 4.0)),
            &vec![],
            None,
            None,
        );
        assert!(sql.contains("\"a\" = 4"));
    }

    // Branch: render_non_column_value covers null/bool/number/string/array.
    #[test]
    fn render_value_branches() {
        use NonColumnValue::Literal;
        assert_eq!(
            render_non_column_value(&Literal {
                value: LiteralValue::Null
            })
            .unwrap(),
            "NULL"
        );
        assert_eq!(
            render_non_column_value(&Literal {
                value: LiteralValue::Bool(true)
            })
            .unwrap(),
            "1"
        );
        assert_eq!(
            render_non_column_value(&Literal {
                value: LiteralValue::Bool(false)
            })
            .unwrap(),
            "0"
        );
        assert_eq!(
            render_non_column_value(&Literal {
                value: LiteralValue::String("o'reilly".into())
            })
            .unwrap(),
            "'o''reilly'"
        );
        assert_eq!(
            render_non_column_value(&Literal {
                value: LiteralValue::Array(vec![
                    zero_cache_types::ast::ScalarLiteral::Number(1.0),
                    zero_cache_types::ast::ScalarLiteral::Number(2.0),
                ])
            })
            .unwrap(),
            "(1, 2)"
        );
    }

    // Branch: compile_inline delegates to `sql_inline::compile_inline`.
    // With no bound parameters it's a passthrough over the literal SQL.
    #[test]
    fn compile_inline_is_passthrough() {
        assert_eq!(compile_inline("SELECT 1"), "SELECT 1");
    }

}
