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

use super::pipeline::{ChainSpec, ExistsSpec, JoinSpec};

/// Build a `ChainSpec` from an AST. Caller supplies the query_id (from
/// the transformation hash) and the primary key (looked up from the
/// table schema).
pub fn ast_to_chain_spec(
    ast: &AST,
    query_id: String,
    primary_key: PrimaryKey,
) -> Result<ChainSpec, AstBuildError> {
    // Flip decisions come from the TS wrapper `rust-pipeline-driver-v2.ts`
    // which runs `planQuery` before sending the AST to Rust (commit
    // 23327573e). The wrapper invokes `applyPlansToAST`
    // (`packages/zql/src/planner/planner-builder.ts:322-355`) with the
    // full SQLite cost model, so the AST arrives with per-CSQ `flip`
    // already set by the planner. RS must not re-derive — the previous
    // `apply_planner_flips` heuristic pattern-matched on OR shape, which
    // could override the cost model's `flip: false` decisions and
    // diverge from TS. The function is kept in the module for now
    // (`#[cfg(test)]`-only) as a reference for the heuristic that
    // existed pre-TS-wrapper.
    let predicate = match ast.where_clause.as_deref() {
        None => None,
        Some(cond) => Some(build_predicate(cond)?),
    };
    // Push predicate — mirror of TS
    // `Connection.filters.predicate = createPredicate(filters)` at
    // `packages/zqlite/src/table-source.ts:253-256`. Same AST, but
    // evaluated with TS-JS NULL-LHS semantics (filter.ts:87-93) so
    // at push time rows with NULL-in-a-filtered-column are dropped
    // regardless of the op — matching TS push behaviour.
    let push_predicate = match ast.where_clause.as_deref() {
        None => None,
        Some(cond) => Some(build_push_predicate(cond)?),
    };

    let skip_bound = ast.start.as_ref().map(ast_bound_to_v2);

    // Detect top-level OR-of-EXISTS at the WHERE root. Mirror of TS
    // `applyFilterWithFlips` at `packages/zql/src/builder/builder.ts:376-442`
    // OR path: each branch is its own recursive filter pipeline and the
    // results union via `UnionFanOut + UnionFanIn`. We capture the
    // sub-case where every OR branch is a single `CorrelatedSubquery`
    // (`or(exists(A), exists(B), ...)`) and route it to an
    // `OrOfExistsT` transformer. Other OR shapes (branches that mix
    // EXISTS with plain cmp, or contain AND-of-EXISTS) still fall back
    // to the legacy sequential `exists_chain` path.
    // TS wraps WHERE bodies in a singleton `And` (see
    // `zql/src/query/query-impl.ts` where-builder) before sending,
    // so `or(...)` often arrives as `and([or(...)])`. Walk through
    // any chain of singleton `And` nodes to find the effective
    // top-level condition before checking for OR-of-EXISTS.
    fn peel_singleton_and(cond: &Condition) -> &Condition {
        let mut cur = cond;
        loop {
            match cur {
                Condition::And { conditions } if conditions.len() == 1 => {
                    cur = &conditions[0];
                }
                _ => return cur,
            }
        }
    }
    // mirrors TS builder.ts:367
    let has_flips = ast
        .where_clause
        .as_deref()
        .map(|c| condition_has_flips(c))
        .unwrap_or(false);
    if std::env::var("IVM_PARITY_TRACE").is_ok() {
        let cond_type = ast
            .where_clause
            .as_deref()
            .map(|c| condition_type_name(c))
            .unwrap_or("none");
        eprintln!(
            "[ivm:rs:builder.ts:361:apply-where type={} hasFlips={}]",
            cond_type,
            has_flips
        );
    }
    let or_branches: Vec<super::pipeline::OrBranchSpec> =
        match ast.where_clause.as_deref().map(peel_singleton_and) {
            Some(Condition::Or { conditions }) => {
                // mirrors TS builder.ts:375
                if std::env::var("IVM_PARITY_TRACE").is_ok() {
                    eprintln!("[ivm:rs:builder.ts:373:apply-where-with-flips]");
                }
                // mirrors TS builder.ts:385
                if std::env::var("IVM_PARITY_TRACE").is_ok() {
                    eprintln!(
                        "[ivm:rs:builder.ts:376:apply-filter-with-flips type=Or]"
                    );
                    eprintln!(
                        "[ivm:rs:builder.ts:417:apply-filter-with-flips-or-partition withFlipped={} withoutFlipped={}]",
                        conditions.iter().filter(|c| condition_has_flips(c)).count(),
                        conditions.iter().filter(|c| !condition_has_flips(c)).count()
                    );
                }
                // For each branch, recursively split into scalar part
                // + EXISTS list. Mirror of TS
                // `applyFilterWithFlips(end, cond, ...)` recursed per
                // branch (builder.ts:439).
                let mut branches = Vec::with_capacity(conditions.len());
                let mut bail = false;
                for cond in conditions {
                    // mirrors TS builder.ts:459
                    if std::env::var("IVM_PARITY_TRACE").is_ok() {
                        if let Condition::CorrelatedSubquery { related, .. } = cond {
                            let alias = related.subquery.alias.clone()
                                .unwrap_or_else(|| related.subquery.table.clone());
                            eprintln!(
                                "[ivm:rs:builder.ts:453:apply-filter-with-flips-correlated-subquery alias={}]",
                                alias
                            );
                        }
                    }
                    match split_branch(cond) {
                        Ok(b) => branches.push(b),
                        Err(()) => {
                            // Branch contains a nested shape (OR
                            // inside, spreads, etc.) we don't yet
                            // lower. Fall back to the legacy
                            // exists_chain path (AND semantics) —
                            // may produce divergent result but
                            // at least compiles and runs.
                            bail = true;
                            break;
                        }
                    }
                }
                if bail {
                    Vec::new()
                } else {
                    branches
                }
            }
            Some(Condition::And { conditions }) => {
                // mirrors TS builder.ts:391
                if std::env::var("IVM_PARITY_TRACE").is_ok() {
                    eprintln!("[ivm:rs:builder.ts:386:apply-filter-with-flips-and]");
                    eprintln!(
                        "[ivm:rs:builder.ts:390:apply-filter-with-flips-and-partition withFlipped={} withoutFlipped={}]",
                        conditions.iter().filter(|c| condition_has_flips(c)).count(),
                        conditions.iter().filter(|c| !condition_has_flips(c)).count()
                    );
                }
                Vec::new()
            }
            _ => {
                // mirrors TS builder.ts:369
                if std::env::var("IVM_PARITY_TRACE").is_ok() {
                    eprintln!("[ivm:rs:builder.ts:366:apply-where-no-flips]");
                }
                Vec::new()
            }
        };
    let (exists, all_exists) = if or_branches.is_empty() {
        let mut all_exists = collect_all_exists(ast);
        let exists = if all_exists.is_empty() {
            None
        } else {
            Some(all_exists.remove(0))
        };
        (exists, all_exists)
    } else {
        // OR branches own the EXISTS conditions; don't double-apply
        // them via exists/exists_chain.
        (None, Vec::new())
    };

    // Pass AST's `orderBy` through so the source picks up the query's
    // preferred sort. Without this, a table with PK-ASC default sort
    // would always emit in id-ASC order, and a `.limit(N)` against a
    // `.orderBy('modified', 'desc')` query would pick the wrong N rows.
    // Matches TS `connectToSourceAndCreateInput` which routes the AST's
    // sort into `Source.connect`.
    let order_by = ast.order_by.clone();

    // Map every `ast.related[]` entry to a JoinSpec. Mirrors TS
    // `buildPipelineInternal` at
    // `packages/zql/src/builder/builder.ts:347-355` which iterates
    // each `csq` in `ast.related` and wraps the current Input with
    // a Join via `applyCorrelatedSubQuery` (builder.ts:611-647).
    // Each subquery AST is preserved so the driver can recursively
    // build a sub-Chain — same recursion TS performs at
    // `buildPipelineInternal(sq.subquery, ...)` (builder.ts:626).
    let joins: Vec<JoinSpec> = ast
        .related
        .as_ref()
        .map(|rels| {
            rels.iter()
                .map(|r| JoinSpec {
                    parent_key: r.correlation.parent_field.clone(),
                    child_key: r.correlation.child_field.clone(),
                    relationship_name: r
                        .subquery
                        .alias
                        .clone()
                        .unwrap_or_else(|| r.subquery.table.clone()),
                    child_table: r.subquery.table.clone(),
                    child_subquery: Some(Box::new(r.subquery.as_ref().clone())),
                    child_primary_key: None, // driver fills in from schema
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(ChainSpec {
        query_id,
        table: ast.table.clone(),
        primary_key,
        predicate,
        push_predicate,
        skip_bound,
        limit: ast.limit.map(|l| l as usize),
        exists,
        exists_chain: all_exists,
        joins,
        or_branches,
        order_by,
        // Top-level ChainSpec never has a partition key — TS passes
        // `partitionKey = undefined` when buildPipelineInternal is
        // called for the root query (`builder.ts:193-201`). Only
        // sub-Chains built by the driver set this.
        partition_key: None,
    })
}

/// Port of the TS planner's `flip: true` decision, narrowed to the
/// specific shape RS currently supports.
///
/// TS background:
/// - `packages/zql/src/planner/planner-builder.ts:311-320` (`planQuery`)
///   runs `buildPlanGraph` + `planRecursively` over the AST and calls
///   `applyPlansToAST` (:357-382), which writes `flip: bool` onto
///   every `CorrelatedSubquery` condition based on the graph's
///   `PlannerJoin.type === 'flipped'` decisions.
/// - `packages/zql/src/builder/builder.ts:410-448`
///   (`applyFilterWithFlips` OR case) consumes that flag: an OR whose
///   branches contain a flipped CSQ is lowered to
///   `UnionFanOut + FlippedJoin-per-branch + UnionFanIn`, and
///   `UnionFanIn.fetch` → `mergeFetches`
///   (`packages/zql/src/ivm/union-fan-in.ts:218-300`) emits each PK
///   at most once (first-wins on duplicate PK, L260-265).
/// - The non-flipped path (same OR shape, `flip != true`) goes through
///   `applyOr` (`builder.ts:514-557`), a classic `FanOut + per-branch
///   Filter + FanIn` that emits rows as union-all.
///
/// Criterion (explicitly per the task spec, conservative narrow):
/// > Parent is in an OR whose EVERY branch is a single leaf EXISTS
/// > (no nested AND/OR, no scalar cmp).
///
/// When that holds for an `Or { conditions }`, we flip every CSQ in
/// that OR by setting `flip = Some(true)`. When any branch is scalar
/// or contains nested AND/OR / nested CSQs, we leave the OR alone —
/// the builder then takes the non-flip `applyOr` path
/// (`builder.ts:514-557`), matching RS's existing union-all behavior.
///
/// The pass recurses into:
///   - Every AND/OR's children
///   - Every CSQ's `related.subquery.where` (sub-ASTs that have their
///     own WHERE, since TS's `planRecursively` at
///     `planner-builder.ts:300-309` walks every sub-plan)
///   - Every `AST.related[].subquery` (same reason)
///
/// We deliberately do NOT port the cost model — the narrow criterion
/// is a syntactic pattern match the task explicitly bounded the port
/// to. Any OR shape outside this criterion keeps its current non-flip
/// behavior.
// `apply_planner_flips` / `flip_condition_if_leaf_exists_or` /
// `is_leaf_exists_csq` / `subquery_has_nested_csq` removed 2026-04-18.
// The flip decision is now supplied by the TS wrapper
// `rust-pipeline-driver-v2.ts` running `planQuery` before the AST
// reaches Rust — see `applyPlansToAST` at
// `packages/zql/src/planner/planner-builder.ts:322-355`. The
// previous RS-side heuristic pattern-matched OR-of-leaf-EXISTS and
// could override the cost-model `flip: false` decision; deleted to
// prevent that divergence.

#[derive(Debug, thiserror::Error)]
pub enum AstBuildError {
    #[error("unsupported condition: {0}")]
    UnsupportedCondition(String),
    #[error("condition references a parameter (unbound) — bind statics first")]
    UnboundParameter,
}

fn build_predicate(cond: &Condition) -> Result<Predicate, AstBuildError> {
    let tree = build_predicate_tree(cond)?;
    Ok(Arc::new(move |row: &Row| tree.eval(row, /*push_mode=*/false)))
}

/// Mirror of TS `createPredicate` at
/// `packages/zql/src/builder/filter.ts:26-94`. The TS JS predicate
/// — stored on `Connection.filters.predicate` at `table-source.ts:
/// 253-256` and invoked at push time via `filterPush` at
/// `filter-push.ts:21` — differs from SQL-fetch semantics on NULL
/// LHS: for any op that is NOT `IS` / `IS NOT`, TS's predicate returns
/// FALSE for a NULL-or-undefined LHS column (filter.ts:87-93):
///
/// ```ignore
///   return (row: Row) => {
///     const lhs = row[left.name];
///     if (lhs === null || lhs === undefined) {
///       return false;
///     }
///     return impl(lhs);
///   };
/// ```
///
/// This matters for operators where the SQL answer for NULL differs
/// — e.g. SQLite `NULL NOT IN (SELECT FROM json_each('[]'))` returns
/// TRUE, but the TS JS predicate returns FALSE. RS must match the JS
/// predicate at push time (fuzz_00673 canary).
///
/// The same `build_predicate_tree` is used; only the `eval` call-site
/// passes `push_mode=true`, which flips the NULL-LHS fallback in
/// `SimpleClause::eval` — see the `match self.op` inside that
/// function.
fn build_push_predicate(cond: &Condition) -> Result<Predicate, AstBuildError> {
    let tree = build_predicate_tree(cond)?;
    Ok(Arc::new(move |row: &Row| tree.eval(row, /*push_mode=*/true)))
}

/// Check if a condition tree contains any flipped CorrelatedSubquery.
/// Used only for IVM_PARITY_TRACE logs — zero overhead when env var unset
/// since the caller gates with `is_ok()` before calling this.
fn condition_has_flips(cond: &Condition) -> bool {
    match cond {
        Condition::CorrelatedSubquery { flip, .. } => flip.unwrap_or(false),
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(condition_has_flips)
        }
        Condition::Simple { .. } => false,
    }
}

/// Return a short type label for a condition, for trace logs.
fn condition_type_name(cond: &Condition) -> &'static str {
    match cond {
        Condition::Simple { .. } => "Simple",
        Condition::And { .. } => "And",
        Condition::Or { .. } => "Or",
        Condition::CorrelatedSubquery { .. } => "CorrelatedSubquery",
    }
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
            // mirrors TS builder.ts:517
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                eprintln!(
                    "[ivm:rs:builder.ts:509:apply-and conditions={}]",
                    conditions.len()
                );
            }
            let children: Result<Vec<PredicateNode>, _> =
                conditions.iter().map(build_predicate_tree).collect();
            Ok(PredicateNode::And(children?))
        }
        Condition::Or { conditions } => {
            // mirrors TS builder.ts:530
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                let subquery_count = conditions
                    .iter()
                    .filter(|c| matches!(c, Condition::CorrelatedSubquery { .. }))
                    .count();
                let other_count = conditions.len() - subquery_count;
                eprintln!(
                    "[ivm:rs:builder.ts:520:apply-or conditions={}]",
                    conditions.len()
                );
                eprintln!(
                    "[ivm:rs:builder.ts:522:apply-or-partition subqueryConditions={} otherConditions={}]",
                    subquery_count,
                    other_count
                );
                if subquery_count == 0 {
                    eprintln!("[ivm:rs:builder.ts:526:apply-or-no-subquery-simple-filter]");
                } else {
                    eprintln!("[ivm:rs:builder.ts:541:apply-or-with-subquery-fan-out-fan-in]");
                }
            }
            let children: Result<Vec<PredicateNode>, _> =
                conditions.iter().map(build_predicate_tree).collect();
            Ok(PredicateNode::Or(children?))
        }
        Condition::CorrelatedSubquery { related, .. } => {
            // mirrors TS builder.ts:633
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                let alias = related
                    .subquery
                    .alias
                    .clone()
                    .unwrap_or_else(|| related.subquery.table.clone());
                eprintln!(
                    "[ivm:rs:builder.ts:617:apply-correlated-subquery alias={} fromCondition=CorrelatedSubquery limit={:?}]",
                    alias,
                    related.subquery.limit
                );
                if related.subquery.limit == Some(0) {
                    eprintln!(
                        "[ivm:rs:builder.ts:621:apply-correlated-subquery-limit0-skip alias={}]",
                        alias
                    );
                }
            }
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
    /// `push_mode = true` mirrors TS `createPredicate` at
    /// `packages/zql/src/builder/filter.ts:87-93` which returns FALSE
    /// for any NULL-or-undefined LHS column (except `IS`/`IS NOT`,
    /// which are handled before the NULL-LHS guard at filter.ts:62-71).
    /// `push_mode = false` preserves the SQLite/SQL-fetch semantics
    /// (`NULL NOT IN (empty)` → TRUE, etc. — mirror of TS
    /// `zqlite/src/query-builder.ts:125-134`).
    fn eval(&self, row: &Row, push_mode: bool) -> bool {
        match self {
            PredicateNode::Simple(c) => c.eval(row, push_mode),
            PredicateNode::And(cs) => cs.iter().all(|c| c.eval(row, push_mode)),
            PredicateNode::Or(cs) => cs.iter().any(|c| c.eval(row, push_mode)),
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
        // mirrors TS: zqlite/src/query-builder.ts:125-134 (SQLite json_each semantics)
        return match op {
            Op::IsNot => !rhs.is_null(),
            Op::IS => rhs.is_null(),
            Op::NotIn => matches!(rhs, serde_json::Value::Array(xs) if xs.is_empty()),
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
    fn eval(&self, row: &Row, push_mode: bool) -> bool {
        let lhs = match row.get(&self.column) {
            Some(Some(v)) => v,
            _ => {
                // NULL lhs handling splits on mode:
                //
                //   `push_mode = true` mirrors TS `createPredicate` at
                //   `packages/zql/src/builder/filter.ts:62-93`. IS / IS NOT
                //   are special-cased at filter.ts:62-71 (createIsPredicate)
                //   so they still compare against NULL directly; for every
                //   OTHER operator the function returns FALSE for a
                //   NULL-or-undefined LHS (filter.ts:87-93:
                //   `if (lhs === null || lhs === undefined) return false`).
                //
                //   `push_mode = false` mirrors TS's SQL-fetch semantics
                //   (via SQLite `json_each` for IN/NOT IN — see TS
                //   `zqlite/src/query-builder.ts:125-134`):
                //     - `NULL NOT IN (SELECT FROM json_each('[]'))` → 1
                //       (TRUE, because the set is empty)
                //     - `NULL NOT IN (SELECT FROM json_each('[...]'))` → 0
                //       (NULL/FALSE in SQL three-valued logic)
                //     - `NULL IN (anything)` → 0
                //
                // Keeping both in one `eval` preserves RS's single-
                // predicate-tree + two-invocation pattern (one for fetch,
                // one for push) that mirrors TS's dual-storage on
                // `Connection.filters` at `table-source.ts:253-256`:
                // `{condition, predicate}` — SQL for fetch, JS for push.
                return match self.op {
                    SimpleOperator::IsNot => !self.rhs.is_null(),
                    SimpleOperator::IS => self.rhs.is_null(),
                    _ if push_mode => false,
                    // push_mode = false → SQL-fetch semantics.
                    SimpleOperator::NotIn => {
                        matches!(&self.rhs, serde_json::Value::Array(xs) if xs.is_empty())
                    }
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

/// Lower one OR branch's `Condition` to an `OrBranchSpec`.
/// Mirror of TS per-branch recursion at
/// `packages/zql/src/builder/builder.ts:439`
/// (`branches.push(applyFilterWithFlips(end, cond, ...))`).
///
/// Splits the branch into:
///   - A scalar predicate compiled from every non-EXISTS condition
///     in the branch, AND'd together (matches `Filter` application
///     after sub-pipelines in `buildPipelineInternal`).
///   - The list of EXISTS/NOT-EXISTS correlated subqueries in the
///     branch, AND'd together (matches sequential ExistsT application
///     after scalar Filter within a branch).
///
/// Supported branch shapes:
///   - `Simple`  → predicate only, no EXISTS
///   - `CorrelatedSubquery` → no predicate, single EXISTS
///   - `And { [..] }` → collect scalars into predicate, collect
///     CorrelatedSubquery into EXISTS list
///   - `Or`/nested `And(And)` → returns `Err(())` for the caller to
///     fall back to the legacy path (matches documented scope limit).
fn split_branch(cond: &Condition) -> Result<super::pipeline::OrBranchSpec, ()> {
    let mut scalar_nodes: Vec<Condition> = Vec::new();
    let mut exists_list: Vec<ExistsSpec> = Vec::new();
    collect_branch_parts(cond, &mut scalar_nodes, &mut exists_list)?;
    let predicate = if scalar_nodes.is_empty() {
        None
    } else {
        // AND the scalar conditions together — mirror of TS
        // `applyWhere` sequential Filter application.
        let combined = if scalar_nodes.len() == 1 {
            scalar_nodes.into_iter().next().unwrap()
        } else {
            Condition::And {
                conditions: scalar_nodes,
            }
        };
        match build_predicate(&combined) {
            Ok(p) => Some(p),
            Err(_) => return Err(()),
        }
    };
    // TS-planner flip decision for this branch. The `apply_planner_flips`
    // pass sets `flip: true` on every CSQ in every branch when the OR
    // matches the leaf-EXISTS criterion (mirrors TS
    // `applyFilterWithFlips` at
    // `packages/zql/src/builder/builder.ts:410-448`). A branch is in
    // flip-mode iff every CSQ within it is flipped; once set, the
    // containing `OrBranchesT` runs in `mergeFetches` first-wins-on-PK
    // mode (TS `packages/zql/src/ivm/union-fan-in.ts:218-300`).
    // Branches without any CSQ (pure scalar branches) can't be flipped,
    // so leave `flip_mode = false`.
    let flip_mode = branch_all_csqs_flipped(cond);
    Ok(super::pipeline::OrBranchSpec {
        predicate,
        exists: exists_list,
        flip_mode,
    })
}

/// True iff every `Condition::CorrelatedSubquery` reachable under
/// `cond` has `flip == Some(true)` and at least one CSQ exists.
/// Matches TS `applyPlansToAST` semantics at
/// `packages/zql/src/planner/planner-builder.ts:322-355`: the
/// planner tags every flipped CSQ with `flip = true`, so a branch
/// is "in the flipped path" iff all of its CSQs carry that flag.
fn branch_all_csqs_flipped(cond: &Condition) -> bool {
    fn walk(cond: &Condition, saw_csq: &mut bool) -> bool {
        match cond {
            Condition::CorrelatedSubquery { flip, .. } => {
                *saw_csq = true;
                flip.unwrap_or(false)
            }
            Condition::And { conditions } | Condition::Or { conditions } => {
                conditions.iter().all(|c| walk(c, saw_csq))
            }
            Condition::Simple { .. } => true,
        }
    }
    let mut saw_csq = false;
    let all_flipped = walk(cond, &mut saw_csq);
    all_flipped && saw_csq
}

fn collect_branch_parts(
    cond: &Condition,
    scalars: &mut Vec<Condition>,
    exists_list: &mut Vec<ExistsSpec>,
) -> Result<(), ()> {
    match cond {
        Condition::Simple { .. } => {
            scalars.push(cond.clone());
            Ok(())
        }
        Condition::CorrelatedSubquery { .. } => {
            walk_exists(cond, exists_list);
            Ok(())
        }
        Condition::And { conditions } => {
            // Flatten AND chains into the branch's scalar+EXISTS
            // buckets. Matches TS `applyWhere` which lowers
            // And-of-mixed into a linear Filter + ExistsT sequence.
            for c in conditions {
                collect_branch_parts(c, scalars, exists_list)?;
            }
            Ok(())
        }
        Condition::Or { .. } => {
            // Nested OR inside an OR branch is ONLY supported when it
            // contains no CSQ — then it's a pure scalar disjunction
            // which `build_predicate_tree` handles natively via its
            // `PredicateNode::Or` case. Push the whole sub-OR as a
            // single scalar and let the predicate compiler fold it.
            //
            // If the nested OR contains CSQs we'd need another layer
            // of UnionFanOut/FanIn — leave that as the fall-back so
            // the caller can retry via the legacy exists_chain path.
            //
            // Mirror of TS `applyFilterWithFlips` OR case which
            // recursively calls `applyOr` for the non-flipped branch
            // only when it has no flipped CSQs (builder.ts:422-435).
            if condition_has_correlated_subquery(cond) {
                Err(())
            } else {
                scalars.push(cond.clone());
                Ok(())
            }
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
                // Mirror of TS `applyFilterWithFlips` per-CSQ flip
                // decision — `true` means this CSQ is flipped, so TS
                // builds a FlippedJoin at `builder.ts:450-478` WITHOUT
                // the `EXISTS_LIMIT` cap from `builder.ts:314-320`.
                flip: flip.unwrap_or(false),
                // Mirror of `CorrelatedSubquery.system`. TS uses this
                // at `packages/zql/src/builder/builder.ts:316-319` to
                // pick `PERMISSIONS_EXISTS_LIMIT` (1) vs `EXISTS_LIMIT`
                // (3) when capping the upfront-Join child subquery.
                system: related.system.clone(),
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

    /// Regression test for fuzz_00633: `NOT IN []` on a nullable column.
    ///
    /// TS ground truth (zqlite/src/query-builder.ts:125-134): the query is
    /// compiled to SQLite as `visibleTo NOT IN (SELECT value FROM json_each('[]'))`.
    /// SQLite semantics: `NULL NOT IN (empty subquery)` → 1 (true), so rows
    /// with a NULL column pass the filter. RS must match.
    #[test]
    fn not_in_empty_array_passes_null_column() {
        let ast = AST {
            schema: None,
            table: "messages".into(),
            alias: None,
            where_clause: Some(Box::new(mk_simple(
                "visibleTo",
                SimpleOperator::NotIn,
                LiteralValue::Array(vec![]), // empty array
            ))),
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let spec =
            ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()])).unwrap();
        let p = spec.predicate.unwrap();
        let push_p = spec.push_predicate.unwrap();

        // Row with NULL visibleTo — fetch predicate (SQL semantics)
        // passes (mirrors SQLite: NULL NOT IN () → true).
        let mut null_row = Row::new();
        null_row.insert("visibleTo".into(), None); // NULL
        assert!(p(&null_row), "NULL NOT IN [] must be true at fetch (mirrors TS SQLite semantics)");

        // Push predicate mirrors TS `createPredicate`
        // (packages/zql/src/builder/filter.ts:87-93): for any op
        // that isn't IS/IS NOT, NULL LHS → false.
        assert!(
            !push_p(&null_row),
            "NULL NOT IN [] must be false at push (mirrors TS createPredicate: NULL lhs → false)"
        );

        // Row with a non-null visibleTo — also passes under both.
        let mut non_null_row = Row::new();
        non_null_row.insert("visibleTo".into(), Some(json!("u1")));
        assert!(p(&non_null_row), "non-null NOT IN [] must be true");
        assert!(push_p(&non_null_row), "non-null NOT IN [] must be true at push too");

        // Column missing entirely (also treated as NULL).
        let empty_row = Row::new();
        assert!(p(&empty_row), "missing column NOT IN [] must be true at fetch");
        assert!(!push_p(&empty_row), "missing column NOT IN [] must be false at push");
    }

    /// Complement: `NOT IN [non-empty]` with NULL column must be false.
    /// Mirrors SQLite: `NULL NOT IN ('u1')` → NULL (falsy).
    #[test]
    fn not_in_nonempty_array_rejects_null_column() {
        let ast = AST {
            schema: None,
            table: "messages".into(),
            alias: None,
            where_clause: Some(Box::new(mk_simple(
                "visibleTo",
                SimpleOperator::NotIn,
                LiteralValue::Array(vec![ScalarLiteral::String("u1".into())]),
            ))),
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let spec =
            ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()])).unwrap();
        let p = spec.predicate.unwrap();

        let mut null_row = Row::new();
        null_row.insert("visibleTo".into(), None); // NULL
        assert!(!p(&null_row), "NULL NOT IN [non-empty] must be false (SQL NULL semantics)");

        // Non-null value not in the list — passes.
        let mut non_null_row = Row::new();
        non_null_row.insert("visibleTo".into(), Some(json!("u2")));
        assert!(p(&non_null_row), "u2 NOT IN [u1] must be true");

        // Non-null value that IS in the list — must be false.
        let mut in_list_row = Row::new();
        in_list_row.insert("visibleTo".into(), Some(json!("u1")));
        assert!(!p(&in_list_row), "u1 NOT IN [u1] must be false");
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

    // ---- Planner-flip pass tests ---------------------------------
    //
    // These tests verify the narrow port of TS `applyPlansToAST` at
    // `packages/zql/src/planner/planner-builder.ts:322-355`. They
    // exercise the exact criterion given in the task: an OR whose
    // EVERY branch is a single leaf EXISTS gets `flip = Some(true)`
    // on every CSQ; any other OR shape (scalar branch, nested
    // AND/OR, NOT EXISTS, etc.) leaves `flip` unchanged.

    fn mk_csq_exists(rel_alias: &str, child_table: &str, parent_field: &str) -> Condition {
        use zero_cache_types::ast::{Correlation, CorrelatedSubquery, AST as CsqAst};
        Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec![parent_field.into()],
                    child_field: vec!["id".into()],
                },
                subquery: Box::new(CsqAst {
                    schema: None,
                    table: child_table.into(),
                    alias: Some(rel_alias.into()),
                    where_clause: None,
                    related: None,
                    start: None,
                    limit: None,
                    order_by: None,
                }),
                system: None,
                hidden: None,
            }),
            op: CorrelatedSubqueryOp::EXISTS,
            flip: None,
            scalar: None,
        }
    }

    fn mk_csq_not_exists(rel_alias: &str, child_table: &str, parent_field: &str) -> Condition {
        let mut c = mk_csq_exists(rel_alias, child_table, parent_field);
        if let Condition::CorrelatedSubquery { op, .. } = &mut c {
            *op = CorrelatedSubqueryOp::NotExists;
        }
        c
    }

    fn flip_of(cond: &Condition) -> Option<bool> {
        match cond {
            Condition::CorrelatedSubquery { flip, .. } => *flip,
            _ => panic!("expected CorrelatedSubquery"),
        }
    }

    // `planner_flip_*` tests removed with the `apply_planner_flips`
    // heuristic — flip decisions now come from TS-side `planQuery` via
    // `rust-pipeline-driver-v2.ts`. See commit log for history.

    #[test]
    fn or_branch_spec_carries_flip_mode_when_ast_sets_flip_true() {
        // End-to-end: when the TS planner (run upstream by
        // `rust-pipeline-driver-v2.ts`) has set `flip = Some(true)` on
        // each CSQ of an OR, the resulting ChainSpec's `or_branches[*]`
        // carry `flip_mode = true`. Mirrors TS `applyFilterWithFlips`
        // at `packages/zql/src/builder/builder.ts:410-448` taking the
        // flipped path because all branches have `flip`.
        let mut a = mk_csq_exists("A", "t_a", "id");
        let mut b = mk_csq_exists("B", "t_b", "id");
        if let Condition::CorrelatedSubquery { flip, .. } = &mut a {
            *flip = Some(true);
        }
        if let Condition::CorrelatedSubquery { flip, .. } = &mut b {
            *flip = Some(true);
        }
        let ast = AST {
            schema: None,
            table: "conv".into(),
            alias: None,
            where_clause: Some(Box::new(Condition::Or { conditions: vec![a, b] })),
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let spec =
            ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()])).unwrap();
        assert_eq!(spec.or_branches.len(), 2);
        assert!(spec.or_branches[0].flip_mode);
        assert!(spec.or_branches[1].flip_mode);
    }

    #[test]
    fn or_branch_spec_defaults_flip_mode_false_for_scalar_mixed_or() {
        // Non-leaf-EXISTS OR: the planner-port leaves `flip = None`,
        // so `flip_mode` stays false and the OrBranchesT takes its
        // union-all path (mirrors TS `applyOr` at
        // `packages/zql/src/builder/builder.ts:514-557`).
        let ast = AST {
            schema: None,
            table: "conv".into(),
            alias: None,
            where_clause: Some(Box::new(Condition::Or {
                conditions: vec![
                    mk_csq_exists("A", "t_a", "id"),
                    mk_simple("name", SimpleOperator::Eq, LiteralValue::String("x".into())),
                ],
            })),
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let spec =
            ast_to_chain_spec(&ast, "q1".into(), PrimaryKey::new(vec!["id".into()])).unwrap();
        assert_eq!(spec.or_branches.len(), 2);
        assert!(!spec.or_branches[0].flip_mode);
        assert!(!spec.or_branches[1].flip_mode);
    }
}
