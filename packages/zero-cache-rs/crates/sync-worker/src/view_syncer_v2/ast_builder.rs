//! AST → ChainSpec translation.
//!
//! Scope of this first cut:
//!
//! - `AST.table` → `ChainSpec.table`
//! - `AST.where_clause` → predicate closure (supports `Simple` conditions
//!   with literal RHS and `And` of simples)
//! - `AST.limit` → `ChainSpec.limit`
//! - `AST.start` → `ChainSpec.skip_bound`
//! - `AST.related` with `CorrelatedSubqueryOp::EXISTS` / `NotExists` →
//!   first `ExistsSpec` entry (multiple existss require a Vec on
//!   `ChainSpec`; TBD)
//!
//! Not supported yet:
//! - `Or`, nested conditions beyond top-level And
//! - `IN`, `LIKE`, `ILIKE`, `IS NOT`, `IS` (only `=`, `!=`, `<`, `>`, `<=`, `>=`)
//! - Multiple `related` → ExistsSpec stack
//! - OrderBy validation / direction flipping
//!
//! Expand in follow-ups; for now this handles the common xyne-spaces
//! sidebar/thread shape.

use std::sync::Arc;

use zero_cache_types::ast::{
    Bound as AstBound, Condition, CorrelatedSubqueryOp, LiteralValue, NonColumnValue,
    ScalarLiteral, SimpleOperator, ValuePosition, AST,
};
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::Row;

use crate::ivm_v2::exists_t::ExistsType;
use crate::ivm_v2::filter_t::Predicate;
use crate::ivm_v2::skip_t::Bound;

use super::pipeline::{ChainSpec, ExistsSpec};

/// Build a `ChainSpec` from an AST. Caller supplies the query_id (from
/// the transformation hash) and the primary key (looked up from the
/// table schema).
pub fn ast_to_chain_spec(
    ast: &AST,
    query_id: String,
    primary_key: PrimaryKey,
) -> Result<ChainSpec, AstBuildError> {
    let predicate = match ast.where_clause.as_deref() {
        None => None,
        Some(cond) => Some(build_predicate(cond)?),
    };

    let skip_bound = ast.start.as_ref().map(ast_bound_to_v2);
    let mut all_exists = collect_all_exists(ast);
    let exists = if all_exists.is_empty() {
        None
    } else {
        Some(all_exists.remove(0))
    };

    // Pass AST's `orderBy` through so the source picks up the query's
    // preferred sort. Without this, a table with PK-ASC default sort
    // would always emit in id-ASC order, and a `.limit(N)` against a
    // `.orderBy('modified', 'desc')` query would pick the wrong N rows.
    // Matches TS `connectToSourceAndCreateInput` which routes the AST's
    // sort into `Source.connect`.
    let order_by = ast.order_by.clone();

    Ok(ChainSpec {
        query_id,
        table: ast.table.clone(),
        primary_key,
        predicate,
        skip_bound,
        limit: ast.limit.map(|l| l as usize),
        exists,
        exists_chain: all_exists,
        join: None,
        order_by,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum AstBuildError {
    #[error("unsupported condition: {0}")]
    UnsupportedCondition(String),
    #[error("condition references a parameter (unbound) — bind statics first")]
    UnboundParameter,
}

fn build_predicate(cond: &Condition) -> Result<Predicate, AstBuildError> {
    let tree = build_predicate_tree(cond)?;
    Ok(Arc::new(move |row: &Row| tree.eval(row)))
}

fn build_predicate_tree(cond: &Condition) -> Result<PredicateNode, AstBuildError> {
    match cond {
        Condition::Simple { op, left, right } => {
            // Symmetric LHS handling — matches TS `createPredicate`
            // (zql/src/builder/filter.ts) which accepts any
            // `ValuePosition` on either side.
            //
            // - Column LHS / Literal RHS: build a runtime SimpleClause.
            // - Literal LHS / Literal RHS: constant-fold at build time.
            // - Static LHS or RHS: error — TS pre-binds via
            //   `bindStaticParameters` before predicate building, so a
            //   Static reaching us means the binding step was skipped.
            let rhs_literal = match right {
                NonColumnValue::Literal { value } => literal_to_json(value),
                NonColumnValue::Static { .. } => {
                    return Err(AstBuildError::UnboundParameter);
                }
            };
            match left {
                ValuePosition::Column { name } => {
                    Ok(PredicateNode::Simple(SimpleClause {
                        column: name.clone(),
                        op: *op,
                        rhs: rhs_literal,
                    }))
                }
                ValuePosition::Literal { value } => {
                    // Constant-fold: both sides are literal at build time,
                    // result is a constant boolean.
                    let lhs_literal = literal_to_json(value);
                    let result = eval_constant_simple(*op, &lhs_literal, &rhs_literal);
                    Ok(if result {
                        PredicateNode::AlwaysTrue
                    } else {
                        PredicateNode::AlwaysFalse
                    })
                }
                ValuePosition::Static { .. } => Err(AstBuildError::UnboundParameter),
            }
        }
        Condition::And { conditions } => {
            let children: Result<Vec<PredicateNode>, _> =
                conditions.iter().map(build_predicate_tree).collect();
            Ok(PredicateNode::And(children?))
        }
        Condition::Or { conditions } => {
            let children: Result<Vec<PredicateNode>, _> =
                conditions.iter().map(build_predicate_tree).collect();
            Ok(PredicateNode::Or(children?))
        }
        Condition::CorrelatedSubquery { .. } => {
            // EXISTS/NOT EXISTS are handled via ExistsSpec at the chain
            // level — they don't appear as runtime predicates here. Treat
            // as "always true" in the predicate context.
            Ok(PredicateNode::AlwaysTrue)
        }
    }
}

enum PredicateNode {
    Simple(SimpleClause),
    And(Vec<PredicateNode>),
    Or(Vec<PredicateNode>),
    AlwaysTrue,
    AlwaysFalse,
}

impl PredicateNode {
    fn eval(&self, row: &Row) -> bool {
        match self {
            PredicateNode::Simple(c) => c.eval(row),
            PredicateNode::And(cs) => cs.iter().all(|c| c.eval(row)),
            PredicateNode::Or(cs) => cs.iter().any(|c| c.eval(row)),
            PredicateNode::AlwaysTrue => true,
            PredicateNode::AlwaysFalse => false,
        }
    }
}

/// Apply a `SimpleOperator` to two known literal JSON values at build
/// time. Used for constant-folding `Literal op Literal` conditions
/// (e.g., `'a' = 'b'` → constant false). Mirrors the runtime evaluation
/// in `SimpleClause::eval`.
fn eval_constant_simple(
    op: SimpleOperator,
    lhs: &serde_json::Value,
    rhs: &serde_json::Value,
) -> bool {
    use SimpleOperator as Op;
    let lhs_is_null = lhs.is_null();
    if lhs_is_null {
        return match op {
            Op::IsNot => !rhs.is_null(),
            Op::IS => rhs.is_null(),
            _ => false,
        };
    }
    match op {
        Op::Eq => json_values_eq(lhs, rhs),
        Op::Ne => !json_values_eq(lhs, rhs),
        Op::IS => json_values_eq(lhs, rhs),
        Op::IsNot => !json_values_eq(lhs, rhs),
        Op::Lt => cmp_json_lt(lhs, rhs),
        Op::Gt => cmp_json_lt(rhs, lhs),
        Op::Le => !cmp_json_lt(rhs, lhs),
        Op::Ge => !cmp_json_lt(lhs, rhs),
        Op::IN => match rhs {
            serde_json::Value::Array(xs) => xs.iter().any(|x| json_values_eq(x, lhs)),
            _ => false,
        },
        Op::NotIn => match rhs {
            serde_json::Value::Array(xs) => !xs.iter().any(|x| json_values_eq(x, lhs)),
            _ => true,
        },
        Op::LIKE | Op::ILIKE | Op::NotLike | Op::NotIlike => {
            let Some(pattern) = rhs.as_str() else {
                return false;
            };
            let Some(lhs_s) = lhs.as_str() else {
                return false;
            };
            let matched = sql_like_matches(
                lhs_s,
                pattern,
                matches!(op, Op::ILIKE | Op::NotIlike),
            );
            matches!(op, Op::LIKE | Op::ILIKE) == matched
        }
    }
}

struct SimpleClause {
    column: String,
    op: SimpleOperator,
    rhs: serde_json::Value,
}

impl SimpleClause {
    fn eval(&self, row: &Row) -> bool {
        let lhs = match row.get(&self.column) {
            Some(Some(v)) => v,
            _ => {
                // NULL handling: IS/IsNot compare against NULL; else false.
                return match self.op {
                    SimpleOperator::IsNot => !self.rhs.is_null(),
                    SimpleOperator::IS => self.rhs.is_null(),
                    _ => false,
                };
            }
        };
        use SimpleOperator as Op;
        match self.op {
            Op::Eq => json_values_eq(lhs, &self.rhs),
            Op::Ne => !json_values_eq(lhs, &self.rhs),
            Op::IS => json_values_eq(lhs, &self.rhs),
            Op::IsNot => !json_values_eq(lhs, &self.rhs),
            Op::Lt => cmp_json_lt(lhs, &self.rhs),
            Op::Gt => cmp_json_lt(&self.rhs, lhs),
            Op::Le => !cmp_json_lt(&self.rhs, lhs),
            Op::Ge => !cmp_json_lt(lhs, &self.rhs),
            Op::IN => match &self.rhs {
                serde_json::Value::Array(xs) => xs.iter().any(|x| json_values_eq(x, lhs)),
                _ => false,
            },
            Op::NotIn => match &self.rhs {
                serde_json::Value::Array(xs) => !xs.iter().any(|x| json_values_eq(x, lhs)),
                _ => true,
            },
            Op::LIKE | Op::ILIKE | Op::NotLike | Op::NotIlike => {
                let Some(pattern) = self.rhs.as_str() else {
                    return false;
                };
                let Some(lhs_s) = lhs.as_str() else {
                    return false;
                };
                let matches =
                    sql_like_matches(lhs_s, pattern, matches!(self.op, Op::ILIKE | Op::NotIlike));
                matches!(self.op, Op::LIKE | Op::ILIKE) == matches
            }
        }
    }
}

/// Minimal SQL LIKE semantics: `%` = any-chars, `_` = single-char.
/// `case_insensitive` = true for ILIKE / NOT ILIKE.
fn sql_like_matches(input: &str, pattern: &str, case_insensitive: bool) -> bool {
    // Translate SQL LIKE to regex. Escape regex metachars except our wildcards.
    let mut re_src = String::from(if case_insensitive { "(?i)^" } else { "^" });
    for ch in pattern.chars() {
        match ch {
            '%' => re_src.push_str(".*"),
            '_' => re_src.push('.'),
            // regex metachars → escape
            '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                re_src.push('\\');
                re_src.push(ch);
            }
            other => re_src.push(other),
        }
    }
    re_src.push('$');
    match regex_lite::Regex::new(&re_src) {
        Ok(re) => re.is_match(input),
        Err(_) => false,
    }
}

/// JSON equality that normalises integer-vs-float numeric representations,
/// so `json!(1)` equals `json!(1.0)`. serde_json's default `PartialEq`
/// distinguishes integer and float storage.
fn json_values_eq(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            match (x.as_f64(), y.as_f64()) {
                (Some(xf), Some(yf)) => xf == yf,
                _ => x == y,
            }
        }
        _ => a == b,
    }
}

fn cmp_json_lt(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let xf = x.as_f64().unwrap_or(f64::NAN);
            let yf = y.as_f64().unwrap_or(f64::NAN);
            xf < yf
        }
        (Value::String(x), Value::String(y)) => x < y,
        _ => false,
    }
}

fn literal_to_json(lit: &LiteralValue) -> serde_json::Value {
    match lit {
        LiteralValue::String(s) => serde_json::json!(s),
        LiteralValue::Number(n) => serde_json::json!(n),
        LiteralValue::Bool(b) => serde_json::json!(b),
        LiteralValue::Null => serde_json::Value::Null,
        LiteralValue::Array(arr) => {
            let inner: Vec<_> = arr
                .iter()
                .map(|s| match s {
                    ScalarLiteral::String(x) => serde_json::json!(x),
                    ScalarLiteral::Number(x) => serde_json::json!(x),
                    ScalarLiteral::Bool(x) => serde_json::json!(x),
                })
                .collect();
            serde_json::Value::Array(inner)
        }
    }
}

fn ast_bound_to_v2(b: &AstBound) -> Bound {
    Bound {
        row: b.row.clone(),
        exclusive: b.exclusive,
    }
}

fn collect_all_exists(ast: &AST) -> Vec<ExistsSpec> {
    let mut out = Vec::new();
    if let Some(cond) = ast.where_clause.as_deref() {
        walk_exists(cond, &mut out);
    }
    out
}

/// True iff the condition tree (recursively) contains at least one
/// `CorrelatedSubquery`. Used by `walk_exists` to decide whether the
/// subquery's WHERE is simple enough to pre-compile into a child
/// predicate — nested EXISTS falls back to the legacy relationship
/// lookup path.
fn condition_has_correlated_subquery(cond: &Condition) -> bool {
    match cond {
        Condition::CorrelatedSubquery { .. } => true,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(condition_has_correlated_subquery)
        }
        _ => false,
    }
}

fn walk_exists(cond: &Condition, out: &mut Vec<ExistsSpec>) {
    match cond {
        Condition::CorrelatedSubquery { related, op, flip, .. } => {
            let et = match op {
                CorrelatedSubqueryOp::EXISTS => ExistsType::Exists,
                CorrelatedSubqueryOp::NotExists => ExistsType::NotExists,
            };
            let rel_name = related
                .subquery
                .alias
                .clone()
                .unwrap_or_else(|| related.subquery.table.clone());
            // `flip` is a planner optimization (zql builder.ts
            // `applyFilterWithFlips`) that restructures EXISTS filters
            // into a FlippedJoin when the child cardinality is much
            // lower than the parent's. For hydration output it produces
            // the same parent row set as the filter path — the
            // difference is push-time reactivity and efficiency. Our
            // v2 `Chain` currently lowers every EXISTS to `ExistsT`
            // (a pure filter transformer). When the TS planner sets
            // `flip: true` upstream we still produce the correct
            // parent set; push propagation semantics may differ.
            // Tracked for a proper FlippedJoin integration in Chain.
            if flip.unwrap_or(false) {
                eprintln!(
                    "[TRACE ivm_v2] ast_to_chain_spec: flip=true on relationship={} — falling back to ExistsT (hydration-equivalent; push may differ)",
                    rel_name
                );
            }
            // Always record the subquery's table + correlation +
            // full AST so the driver can recursively build a Chain
            // for the subquery pipeline. Mirrors TS
            // `buildPipelineInternal`: no special case for "nested vs
            // flat" — every subquery becomes its own Input, and the
            // parent `ExistsT` queries that Input with a correlation
            // constraint. Driver fills in `child_primary_key` and
            // `child_predicate` from the subquery source schema.
            out.push(ExistsSpec {
                relationship_name: rel_name,
                parent_join_key: related.correlation.parent_field.clone(),
                exists_type: et,
                child_table: Some(related.subquery.table.clone()),
                // Child primary key isn't known from the AST alone —
                // it lives in the replica's table metadata. The driver
                // looks it up via its registered table meta before
                // building the Chain.
                child_primary_key: None,
                child_key: related.correlation.child_field.clone(),
                // `child_predicate` is no longer used for EXISTS
                // resolution — the driver builds a full sub-Chain from
                // `child_subquery` below, which applies the subquery's
                // WHERE (including any nested EXISTS) exactly like TS
                // `buildPipelineInternal`. Kept on the struct for
                // back-compat with existing tests that pre-compile a
                // simple predicate.
                child_predicate: None,
                child_subquery: Some(Box::new(related.subquery.as_ref().clone())),
                // Driver fills this in after recursively building the
                // sub-Chain (it needs the source factory, which the
                // ast_builder doesn't have).
                child_exists_child_tables: None,
            });
        }
        Condition::And { conditions } | Condition::Or { conditions } => {
            for c in conditions {
                walk_exists(c, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use zero_cache_types::ast::{LiteralValue, NonColumnValue, SimpleOperator, ValuePosition};

    fn mk_simple(col: &str, op: SimpleOperator, v: LiteralValue) -> Condition {
        Condition::Simple {
            op,
            left: ValuePosition::Column { name: col.into() },
            right: NonColumnValue::Literal { value: v },
        }
    }

    fn mk_ast() -> AST {
        AST {
            schema: None,
            table: "messages".into(),
            alias: None,
            where_clause: Some(Box::new(Condition::And {
                conditions: vec![
                    mk_simple(
                        "conversation_id",
                        SimpleOperator::Eq,
                        LiteralValue::String("X".into()),
                    ),
                    mk_simple("deleted", SimpleOperator::Eq, LiteralValue::Bool(false)),
                ],
            })),
            related: None,
            start: None,
            limit: Some(50),
            order_by: None,
        }
    }

    #[test]
    fn builds_spec_for_simple_and_limit() {
        let ast = mk_ast();
        let spec = ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()]))
            .expect("ast_to_chain_spec");
        assert_eq!(spec.table, "messages");
        assert_eq!(spec.limit, Some(50));
        let predicate = spec.predicate.expect("predicate populated");

        // Predicate matches: conversation_id=X AND deleted=false
        let mut good = Row::new();
        good.insert("conversation_id".into(), Some(json!("X")));
        good.insert("deleted".into(), Some(json!(false)));
        assert!(predicate(&good));

        let mut bad_conv = Row::new();
        bad_conv.insert("conversation_id".into(), Some(json!("Y")));
        bad_conv.insert("deleted".into(), Some(json!(false)));
        assert!(!predicate(&bad_conv));

        let mut bad_del = Row::new();
        bad_del.insert("conversation_id".into(), Some(json!("X")));
        bad_del.insert("deleted".into(), Some(json!(true)));
        assert!(!predicate(&bad_del));
    }

    #[test]
    fn builds_spec_with_start() {
        let mut ast = mk_ast();
        let mut start_row = Row::new();
        start_row.insert("id".into(), Some(json!(100)));
        ast.start = Some(AstBound {
            row: start_row,
            exclusive: true,
        });
        let spec = ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()]))
            .unwrap();
        let bound = spec.skip_bound.expect("skip_bound populated");
        assert!(bound.exclusive);
        assert_eq!(bound.row.get("id"), Some(&Some(json!(100))));
    }

    #[test]
    fn or_predicate_evaluates_correctly() {
        let ast = AST {
            schema: None,
            table: "messages".into(),
            alias: None,
            where_clause: Some(Box::new(Condition::Or {
                conditions: vec![
                    mk_simple("id", SimpleOperator::Eq, LiteralValue::Number(1.0)),
                    mk_simple("id", SimpleOperator::Eq, LiteralValue::Number(2.0)),
                ],
            })),
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let spec =
            ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()])).unwrap();
        let p = spec.predicate.unwrap();
        let mut r1 = Row::new();
        r1.insert("id".into(), Some(json!(1)));
        let mut r3 = Row::new();
        r3.insert("id".into(), Some(json!(3)));
        assert!(p(&r1));
        assert!(!p(&r3));
    }

    #[test]
    fn in_operator_matches_array_membership() {
        let ast = AST {
            schema: None,
            table: "messages".into(),
            alias: None,
            where_clause: Some(Box::new(mk_simple(
                "id",
                SimpleOperator::IN,
                LiteralValue::Array(vec![
                    ScalarLiteral::Number(1.0),
                    ScalarLiteral::Number(3.0),
                    ScalarLiteral::Number(5.0),
                ]),
            ))),
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let spec =
            ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()])).unwrap();
        let p = spec.predicate.unwrap();
        let mut matching = Row::new();
        matching.insert("id".into(), Some(json!(3)));
        let mut not_matching = Row::new();
        not_matching.insert("id".into(), Some(json!(4)));
        assert!(p(&matching));
        assert!(!p(&not_matching));
    }

    #[test]
    fn like_operator_matches_pattern() {
        let ast = AST {
            schema: None,
            table: "messages".into(),
            alias: None,
            where_clause: Some(Box::new(mk_simple(
                "text",
                SimpleOperator::LIKE,
                LiteralValue::String("hello%".into()),
            ))),
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let spec =
            ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()])).unwrap();
        let p = spec.predicate.unwrap();
        let mut matching = Row::new();
        matching.insert("text".into(), Some(json!("hello world")));
        let mut not_matching = Row::new();
        not_matching.insert("text".into(), Some(json!("world hello")));
        assert!(p(&matching));
        assert!(!p(&not_matching));
    }

    /// Rows arriving at the predicate tree have already been coerced
    /// through `SqliteSource`'s schema-aware `sql_value_to_typed_json`,
    /// so boolean columns show up as `Bool`, not `Number`. This test
    /// ensures predicate evaluation works over canonical Bool values.
    /// Originally covered a temporary `Number↔Bool` patch in
    /// `json_values_eq`; that patch is gone now that coercion happens
    /// at the source boundary.
    #[test]
    fn bool_literal_matches_bool_column_value() {
        let ast = AST {
            schema: None,
            table: "issue".into(),
            alias: None,
            where_clause: Some(Box::new(Condition::And {
                conditions: vec![
                    mk_simple(
                        "projectID",
                        SimpleOperator::Eq,
                        LiteralValue::String("P".into()),
                    ),
                    mk_simple("open", SimpleOperator::Eq, LiteralValue::Bool(true)),
                    Condition::Or {
                        conditions: vec![
                            mk_simple(
                                "visibility",
                                SimpleOperator::Eq,
                                LiteralValue::String("public".into()),
                            ),
                            Condition::Simple {
                                op: SimpleOperator::Eq,
                                left: ValuePosition::Literal {
                                    value: LiteralValue::Null,
                                },
                                right: NonColumnValue::Literal {
                                    value: LiteralValue::String("crew".into()),
                                },
                            },
                        ],
                    },
                ],
            })),
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let spec =
            ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()])).unwrap();
        let p = spec.predicate.expect("predicate populated");
        // Row arrives from `SqliteSource` with schema-coerced types —
        // `open` is `Bool`, not `Integer` — so predicate compares Bool
        // against Bool literal via direct JSON equality.
        let mut r_open_public = Row::new();
        r_open_public.insert("id".into(), Some(json!("r1")));
        r_open_public.insert("projectID".into(), Some(json!("P")));
        r_open_public.insert("open".into(), Some(json!(true)));
        r_open_public.insert("visibility".into(), Some(json!("public")));
        assert!(p(&r_open_public));
        let mut r_closed = Row::new();
        r_closed.insert("id".into(), Some(json!("r2")));
        r_closed.insert("projectID".into(), Some(json!("P")));
        r_closed.insert("open".into(), Some(json!(false)));
        r_closed.insert("visibility".into(), Some(json!("public")));
        assert!(!p(&r_closed));
    }
}
