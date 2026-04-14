//! Port of `packages/zql/src/builder/filter.ts`.
//!
//! Public exports ported:
//!
//! - [`NoSubqueryCondition`] — documented alias (see module-level note
//!   below): Rust's AST has a single [`Condition`] enum, and this
//!   module accepts any [`Condition`] but panics on the
//!   `CorrelatedSubquery` arm, matching TS's compile-time exclusion.
//! - [`create_predicate`] — compiles a [`Condition`] tree into a
//!   `Box<dyn Fn(&Row) -> bool + Send + Sync>`.
//! - [`transform_filters`] — strips correlated-subquery branches,
//!   returning a simplified condition plus a `conditions_removed` flag.
//! - [`simplify_condition`] — port of `zql/query/expression.ts`'s
//!   `simplifyCondition`. We inline it here because no other ported
//!   module needs it; keeping it local avoids reaching into unported
//!   `zql/query/expression.ts`.
//!
//! ## TS `NoSubqueryCondition` alias
//!
//! TS uses a distributive type to forbid `correlatedSubquery` at
//! compile time. Rust can't encode that without splitting the enum;
//! we document the invariant and panic on violation. All callers in
//! this crate (builder.rs, query_builder.rs) first run
//! [`transform_filters`] to strip correlated subqueries.

use zero_cache_types::ast::{Condition, SimpleOperator, ValuePosition};
use zero_cache_types::value::{Row, Value};

use crate::builder::like::{LikeFlags, LikePredicate, get_like_predicate};

/// TS `createPredicate(condition)`. Returns a shared predicate.
pub type RowPredicate = Box<dyn Fn(&Row) -> bool + Send + Sync>;

/// TS `createPredicate(condition: NoSubqueryCondition): (row: Row) => boolean`.
///
/// Panics on [`Condition::CorrelatedSubquery`] (TS forbids this at the
/// type level) and on `static` `left` / `right` arms (TS `assert`).
pub fn create_predicate(condition: &Condition) -> RowPredicate {
    match condition {
        Condition::CorrelatedSubquery { .. } => {
            panic!(
                "create_predicate: CorrelatedSubquery is not a valid input; \
                 run transform_filters first"
            );
        }
        Condition::And { conditions } => {
            let predicates: Vec<RowPredicate> = conditions.iter().map(create_predicate).collect();
            Box::new(move |row: &Row| {
                // Branch: AND — all must pass.
                for pred in predicates.iter() {
                    if !pred(row) {
                        return false;
                    }
                }
                true
            })
        }
        Condition::Or { conditions } => {
            let predicates: Vec<RowPredicate> = conditions.iter().map(create_predicate).collect();
            Box::new(move |row: &Row| {
                // Branch: OR — any pass.
                for pred in predicates.iter() {
                    if pred(row) {
                        return true;
                    }
                }
                false
            })
        }
        Condition::Simple { op, left, right } => build_simple_predicate(*op, left, right),
    }
}

fn non_column_value_to_value(v: &zero_cache_types::ast::NonColumnValue) -> Value {
    use zero_cache_types::ast::NonColumnValue;
    match v {
        NonColumnValue::Literal { value } => {
            Some(crate::zqlite::query_builder::literal_value_to_json(value))
        }
        NonColumnValue::Static { .. } => {
            panic!("static values should be resolved before creating predicates");
        }
    }
}

fn value_position_to_value(v: &ValuePosition) -> (Option<String>, Option<Value>) {
    // (Some(column), None) when a column reference; (None, Some(literal))
    // when a literal. Never both.
    match v {
        ValuePosition::Column { name } => (Some(name.clone()), None),
        ValuePosition::Literal { value } => (
            None,
            Some(Some(crate::zqlite::query_builder::literal_value_to_json(
                value,
            ))),
        ),
        ValuePosition::Static { .. } => {
            panic!("static values should be resolved before creating predicates");
        }
    }
}

fn build_simple_predicate(
    op: SimpleOperator,
    left: &ValuePosition,
    right: &zero_cache_types::ast::NonColumnValue,
) -> RowPredicate {
    let rhs: Value = non_column_value_to_value(right);
    let (lhs_col, lhs_literal) = value_position_to_value(left);

    // Branch: IS / IS NOT — special because they accept null rhs.
    if matches!(op, SimpleOperator::IS | SimpleOperator::IsNot) {
        let is_op = op;
        let rhs_for_is = rhs.clone();
        let impl_fn: Box<dyn Fn(&Value) -> bool + Send + Sync> = match is_op {
            SimpleOperator::IS => Box::new(move |lhs: &Value| lhs == &rhs_for_is),
            SimpleOperator::IsNot => Box::new(move |lhs: &Value| lhs != &rhs_for_is),
            _ => unreachable!(),
        };
        // Branch: left is literal — fold to constant.
        if let Some(lit) = lhs_literal {
            let result = impl_fn(&lit);
            return Box::new(move |_row: &Row| result);
        }
        let col = lhs_col.expect("either column or literal");
        return Box::new(move |row: &Row| {
            let v = row.get(&col).cloned().unwrap_or(None);
            impl_fn(&v)
        });
    }

    // Branch: rhs is null / undefined → always false (TS early-return).
    let rhs_is_null_or_undef = match &rhs {
        None => true,
        Some(serde_json::Value::Null) => true,
        _ => false,
    };
    if rhs_is_null_or_undef {
        return Box::new(|_row: &Row| false);
    }

    // rhs is non-null NonNullValue.
    let impl_fn = build_nonnull_predicate(op, rhs);

    // Branch: left is literal — fold.
    if let Some(lit) = lhs_literal {
        // Branch: lit is null/undef → false.
        if matches!(&lit, None | Some(serde_json::Value::Null)) {
            return Box::new(|_row: &Row| false);
        }
        let result = impl_fn(&lit);
        return Box::new(move |_row: &Row| result);
    }

    let col = lhs_col.expect("either column or literal");
    Box::new(move |row: &Row| {
        let v = row.get(&col).cloned().unwrap_or(None);
        // Branch: lhs null/undef → false.
        if matches!(&v, None | Some(serde_json::Value::Null)) {
            return false;
        }
        impl_fn(&v)
    })
}

/// TS `createPredicateImpl(rhs, operator)` — non-null rhs. Dispatch over
/// the full [`SimpleOperator`] set except IS/IS NOT (handled upstream).
fn build_nonnull_predicate(
    op: SimpleOperator,
    rhs: Value,
) -> Box<dyn Fn(&Value) -> bool + Send + Sync> {
    match op {
        SimpleOperator::Eq => {
            let r = rhs;
            Box::new(move |lhs: &Value| lhs == &r)
        }
        SimpleOperator::Ne => {
            let r = rhs;
            Box::new(move |lhs: &Value| lhs != &r)
        }
        SimpleOperator::Lt => {
            let r = rhs;
            Box::new(move |lhs: &Value| value_lt(lhs, &r))
        }
        SimpleOperator::Le => {
            let r = rhs;
            Box::new(move |lhs: &Value| value_lt(lhs, &r) || lhs == &r)
        }
        SimpleOperator::Gt => {
            let r = rhs;
            Box::new(move |lhs: &Value| value_lt(&r, lhs))
        }
        SimpleOperator::Ge => {
            let r = rhs;
            Box::new(move |lhs: &Value| value_lt(&r, lhs) || lhs == &r)
        }
        SimpleOperator::LIKE => wrap_like(rhs, LikeFlags::None, false),
        SimpleOperator::NotLike => wrap_like(rhs, LikeFlags::None, true),
        SimpleOperator::ILIKE => wrap_like(rhs, LikeFlags::IgnoreCase, false),
        SimpleOperator::NotIlike => wrap_like(rhs, LikeFlags::IgnoreCase, true),
        SimpleOperator::IN => build_in_predicate(rhs, false),
        SimpleOperator::NotIn => build_in_predicate(rhs, true),
        SimpleOperator::IS | SimpleOperator::IsNot => {
            unreachable!("handled by build_simple_predicate");
        }
    }
}

fn wrap_like(rhs: Value, flags: LikeFlags, not: bool) -> Box<dyn Fn(&Value) -> bool + Send + Sync> {
    let pred: LikePredicate = get_like_predicate(&rhs, flags);
    if not {
        Box::new(move |lhs: &Value| !pred(lhs))
    } else {
        Box::new(move |lhs: &Value| pred(lhs))
    }
}

fn build_in_predicate(rhs: Value, negate: bool) -> Box<dyn Fn(&Value) -> bool + Send + Sync> {
    // TS: `assert(Array.isArray(rhs), ...)`.
    let arr: Vec<serde_json::Value> = match &rhs {
        Some(serde_json::Value::Array(v)) => v.clone(),
        _ => panic!("Expected rhs to be an array for IN / NOT IN operator"),
    };
    // TS stores the values in a `Set`. We use a Vec since serde_json::Value
    // doesn't implement Hash. Callers use IN with small arrays.
    Box::new(move |lhs: &Value| {
        let contains = match lhs {
            Some(v) => arr.iter().any(|a| a == v),
            None => false,
        };
        if negate { !contains } else { contains }
    })
}

/// JS `<` comparison over two [`Value`]s.
///
/// For numbers we compare as f64. For strings we compare lexicographically.
/// Booleans coerce to numbers (true=1, false=0). Anything else returns
/// false (matches the TS fallthrough `undefined < x` behaviour).
fn value_lt(a: &Value, b: &Value) -> bool {
    use serde_json::Value as J;
    let (Some(a), Some(b)) = (a.as_ref(), b.as_ref()) else {
        return false;
    };
    match (a, b) {
        (J::Number(x), J::Number(y)) => {
            let xf = x.as_f64().unwrap_or(f64::NAN);
            let yf = y.as_f64().unwrap_or(f64::NAN);
            xf < yf
        }
        (J::String(x), J::String(y)) => x < y,
        (J::Bool(x), J::Bool(y)) => !(*x) && *y,
        (J::Bool(b), J::Number(n)) => {
            let bf = if *b { 1.0 } else { 0.0 };
            bf < n.as_f64().unwrap_or(f64::NAN)
        }
        (J::Number(n), J::Bool(b)) => {
            let bf = if *b { 1.0 } else { 0.0 };
            n.as_f64().unwrap_or(f64::NAN) < bf
        }
        _ => false,
    }
}

// ─── transform_filters ───────────────────────────────────────────────

/// TS `transformFilters(filters)` — strips correlated-subquery branches
/// from a filter tree.
///
/// Returns `(filters, conditions_removed)`. `filters = None` means the
/// caller must not apply any filter predicate. `conditions_removed =
/// true` means at least one branch was stripped (so downstream must
/// reapply the original filter at the IVM layer).
pub fn transform_filters(filters: Option<&Condition>) -> (Option<Condition>, bool) {
    // Branch: no filter at all.
    let Some(filters) = filters else {
        return (None, false);
    };
    match filters {
        Condition::Simple { .. } => (Some(filters.clone()), false),
        Condition::CorrelatedSubquery { .. } => (None, true),
        Condition::And { conditions } | Condition::Or { conditions } => {
            let is_or = matches!(filters, Condition::Or { .. });
            let mut transformed: Vec<Condition> = Vec::new();
            let mut removed = false;
            for cond in conditions.iter() {
                let (t, r) = transform_filters(Some(cond));
                // Branch: empty branch inside OR → whole OR is removed.
                if t.is_none() && is_or {
                    return (None, true);
                }
                removed = removed || r;
                if let Some(c) = t {
                    transformed.push(c);
                }
            }
            let simplified = simplify_condition(if is_or {
                Condition::Or {
                    conditions: transformed,
                }
            } else {
                Condition::And {
                    conditions: transformed,
                }
            });
            (Some(simplified), removed)
        }
    }
}

/// TS `simplifyCondition` from `zql/src/query/expression.ts`.
///
/// Rules:
/// - Simple / CorrelatedSubquery returned unchanged.
/// - Single-child AND/OR unwrapped to the child.
/// - Flattens nested ANDs inside AND / nested ORs inside OR.
/// - AND containing FALSE → FALSE.
/// - OR containing TRUE → TRUE.
pub fn simplify_condition(c: Condition) -> Condition {
    match c {
        Condition::Simple { .. } | Condition::CorrelatedSubquery { .. } => c,
        Condition::And { conditions } => simplify_junction(true, conditions),
        Condition::Or { conditions } => simplify_junction(false, conditions),
    }
}

fn simplify_junction(is_and: bool, conditions: Vec<Condition>) -> Condition {
    // Branch: single child → unwrap.
    if conditions.len() == 1 {
        return simplify_condition(conditions.into_iter().next().unwrap());
    }
    let simplified: Vec<Condition> = conditions.into_iter().map(simplify_condition).collect();
    let flattened = flatten(is_and, simplified);

    // Branch: AND with any FALSE → FALSE.
    if is_and && flattened.iter().any(is_always_false) {
        return make_false();
    }
    // Branch: OR with any TRUE → TRUE.
    if !is_and && flattened.iter().any(is_always_true) {
        return make_true();
    }
    if is_and {
        Condition::And {
            conditions: flattened,
        }
    } else {
        Condition::Or {
            conditions: flattened,
        }
    }
}

fn flatten(is_and: bool, conditions: Vec<Condition>) -> Vec<Condition> {
    let mut out: Vec<Condition> = Vec::with_capacity(conditions.len());
    for c in conditions {
        match (is_and, &c) {
            (true, Condition::And { conditions: inner }) => {
                for inner_c in inner {
                    out.push(inner_c.clone());
                }
            }
            (false, Condition::Or { conditions: inner }) => {
                for inner_c in inner {
                    out.push(inner_c.clone());
                }
            }
            _ => out.push(c),
        }
    }
    out
}

fn is_always_true(c: &Condition) -> bool {
    matches!(c, Condition::And { conditions } if conditions.is_empty())
}

fn is_always_false(c: &Condition) -> bool {
    matches!(c, Condition::Or { conditions } if conditions.is_empty())
}

fn make_true() -> Condition {
    // TS `TRUE` = `{type: 'and', conditions: []}`.
    Condition::And { conditions: vec![] }
}
fn make_false() -> Condition {
    // TS `FALSE` = `{type: 'or', conditions: []}`.
    Condition::Or { conditions: vec![] }
}

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //! - `create_predicate` for Simple Eq / Ne / Lt / Le / Gt / Ge
    //! - IS / IS NOT
    //! - LIKE / NOT LIKE / ILIKE / NOT ILIKE
    //! - IN / NOT IN
    //! - AND / OR (with passing, failing, empty cases)
    //! - Literal LHS short-circuits
    //! - Null/undefined LHS returns false
    //! - Null/undefined RHS returns false
    //! - IS NULL / IS NOT NULL semantics
    //! - static arm panics on left + right
    //! - CorrelatedSubquery arm panics
    //!
    //! `transform_filters`:
    //! - None input → (None, false)
    //! - Simple → pass-through
    //! - CorrelatedSubquery → (None, true)
    //! - And with CSQ child → removed child, removed=true
    //! - Or with any None child → whole tree removed
    //! - Nested combinations simplified
    //!
    //! `simplify_condition`:
    //! - Single-child AND / OR unwraps
    //! - Empty AND / OR preserved
    //! - AND containing FALSE → FALSE
    //! - OR containing TRUE → TRUE
    //! - Flattens nested AND inside AND / OR inside OR

    use super::*;
    use indexmap::IndexMap;
    use serde_json::json;
    use zero_cache_types::ast::{
        AST, Condition, CorrelatedSubquery, CorrelatedSubqueryOp, Correlation, LiteralValue,
        NonColumnValue, ParameterAnchor, ParameterField, ScalarLiteral, SimpleOperator,
        ValuePosition,
    };

    fn row_kv(pairs: &[(&str, Option<serde_json::Value>)]) -> Row {
        let mut r: Row = IndexMap::new();
        for (k, v) in pairs {
            r.insert((*k).to_string(), v.clone());
        }
        r
    }

    fn simple(op: SimpleOperator, col: &str, v: LiteralValue) -> Condition {
        Condition::Simple {
            op,
            left: ValuePosition::Column {
                name: col.to_string(),
            },
            right: NonColumnValue::Literal { value: v },
        }
    }

    // Eq
    #[test]
    fn simple_eq_true() {
        let pred = create_predicate(&simple(SimpleOperator::Eq, "id", LiteralValue::Number(1.0)));
        let row = row_kv(&[("id", Some(json!(1)))]);
        assert!(pred(&row));
    }
    #[test]
    fn simple_eq_false() {
        let pred = create_predicate(&simple(SimpleOperator::Eq, "id", LiteralValue::Number(1.0)));
        let row = row_kv(&[("id", Some(json!(2)))]);
        assert!(!pred(&row));
    }
    // Ne
    #[test]
    fn simple_ne() {
        let pred = create_predicate(&simple(SimpleOperator::Ne, "id", LiteralValue::Number(1.0)));
        assert!(pred(&row_kv(&[("id", Some(json!(2)))])));
        assert!(!pred(&row_kv(&[("id", Some(json!(1)))])));
    }
    // Lt / Le / Gt / Ge
    #[test]
    fn simple_lt() {
        let pred = create_predicate(&simple(
            SimpleOperator::Lt,
            "id",
            LiteralValue::Number(10.0),
        ));
        assert!(pred(&row_kv(&[("id", Some(json!(5)))])));
        assert!(!pred(&row_kv(&[("id", Some(json!(10)))])));
    }
    #[test]
    fn simple_le() {
        let pred = create_predicate(&simple(
            SimpleOperator::Le,
            "id",
            LiteralValue::Number(10.0),
        ));
        assert!(pred(&row_kv(&[("id", Some(json!(10)))])));
        assert!(!pred(&row_kv(&[("id", Some(json!(11)))])));
    }
    #[test]
    fn simple_gt() {
        let pred = create_predicate(&simple(
            SimpleOperator::Gt,
            "id",
            LiteralValue::Number(10.0),
        ));
        assert!(pred(&row_kv(&[("id", Some(json!(11)))])));
        assert!(!pred(&row_kv(&[("id", Some(json!(10)))])));
    }
    #[test]
    fn simple_ge() {
        let pred = create_predicate(&simple(
            SimpleOperator::Ge,
            "id",
            LiteralValue::Number(10.0),
        ));
        assert!(pred(&row_kv(&[("id", Some(json!(10)))])));
        assert!(!pred(&row_kv(&[("id", Some(json!(9)))])));
    }

    // IS / IS NOT — IS NULL semantics
    #[test]
    fn is_null() {
        let cond = Condition::Simple {
            op: SimpleOperator::IS,
            left: ValuePosition::Column { name: "a".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Null,
            },
        };
        let pred = create_predicate(&cond);
        assert!(pred(&row_kv(&[("a", Some(json!(null)))])));
        assert!(!pred(&row_kv(&[("a", Some(json!(1)))])));
    }
    #[test]
    fn is_not_null() {
        let cond = Condition::Simple {
            op: SimpleOperator::IsNot,
            left: ValuePosition::Column { name: "a".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Null,
            },
        };
        let pred = create_predicate(&cond);
        assert!(pred(&row_kv(&[("a", Some(json!(1)))])));
        assert!(!pred(&row_kv(&[("a", Some(json!(null)))])));
    }

    // LIKE / NOT LIKE / ILIKE / NOT ILIKE
    #[test]
    fn simple_like() {
        let pred = create_predicate(&simple(
            SimpleOperator::LIKE,
            "n",
            LiteralValue::String("a%".into()),
        ));
        assert!(pred(&row_kv(&[("n", Some(json!("abc")))])));
        assert!(!pred(&row_kv(&[("n", Some(json!("xyz")))])));
    }
    #[test]
    fn simple_not_like() {
        let pred = create_predicate(&simple(
            SimpleOperator::NotLike,
            "n",
            LiteralValue::String("a%".into()),
        ));
        assert!(!pred(&row_kv(&[("n", Some(json!("abc")))])));
        assert!(pred(&row_kv(&[("n", Some(json!("xyz")))])));
    }
    #[test]
    fn simple_ilike() {
        let pred = create_predicate(&simple(
            SimpleOperator::ILIKE,
            "n",
            LiteralValue::String("A%".into()),
        ));
        assert!(pred(&row_kv(&[("n", Some(json!("abc")))])));
    }
    #[test]
    fn simple_not_ilike() {
        let pred = create_predicate(&simple(
            SimpleOperator::NotIlike,
            "n",
            LiteralValue::String("A%".into()),
        ));
        assert!(!pred(&row_kv(&[("n", Some(json!("abc")))])));
    }

    // IN / NOT IN
    #[test]
    fn simple_in() {
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
        let pred = create_predicate(&cond);
        assert!(pred(&row_kv(&[("id", Some(json!(1)))])));
        assert!(pred(&row_kv(&[("id", Some(json!(2)))])));
        assert!(!pred(&row_kv(&[("id", Some(json!(3)))])));
    }
    #[test]
    fn simple_not_in() {
        let cond = Condition::Simple {
            op: SimpleOperator::NotIn,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Array(vec![ScalarLiteral::Number(1.0)]),
            },
        };
        let pred = create_predicate(&cond);
        assert!(!pred(&row_kv(&[("id", Some(json!(1)))])));
        assert!(pred(&row_kv(&[("id", Some(json!(2)))])));
    }

    // AND / OR
    #[test]
    fn and_all_pass() {
        let cond = Condition::And {
            conditions: vec![
                simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0)),
                simple(SimpleOperator::Eq, "b", LiteralValue::Number(2.0)),
            ],
        };
        let pred = create_predicate(&cond);
        assert!(pred(&row_kv(&[
            ("a", Some(json!(1))),
            ("b", Some(json!(2))),
        ])));
        assert!(!pred(&row_kv(&[
            ("a", Some(json!(1))),
            ("b", Some(json!(3))),
        ])));
    }
    #[test]
    fn empty_and_is_true() {
        let cond = Condition::And { conditions: vec![] };
        let pred = create_predicate(&cond);
        assert!(pred(&row_kv(&[])));
    }
    #[test]
    fn or_any_passes() {
        let cond = Condition::Or {
            conditions: vec![
                simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0)),
                simple(SimpleOperator::Eq, "a", LiteralValue::Number(2.0)),
            ],
        };
        let pred = create_predicate(&cond);
        assert!(pred(&row_kv(&[("a", Some(json!(1)))])));
        assert!(pred(&row_kv(&[("a", Some(json!(2)))])));
        assert!(!pred(&row_kv(&[("a", Some(json!(3)))])));
    }
    #[test]
    fn empty_or_is_false() {
        let cond = Condition::Or { conditions: vec![] };
        let pred = create_predicate(&cond);
        assert!(!pred(&row_kv(&[])));
    }

    // Null LHS branches.
    #[test]
    fn null_lhs_returns_false_for_numeric_ops() {
        let pred = create_predicate(&simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0)));
        assert!(!pred(&row_kv(&[("a", Some(json!(null)))])));
        assert!(!pred(&row_kv(&[("a", None)])));
    }

    // Null RHS branches — Eq with null → always false (not IS).
    #[test]
    fn null_rhs_returns_false() {
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: "a".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Null,
            },
        };
        let pred = create_predicate(&cond);
        assert!(!pred(&row_kv(&[("a", Some(json!(1)))])));
    }

    // Literal LHS short-circuits to constant.
    #[test]
    fn literal_lhs_short_circuits() {
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Literal {
                value: LiteralValue::Number(1.0),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        };
        let pred = create_predicate(&cond);
        assert!(pred(&row_kv(&[])));
    }

    // Literal null LHS → false.
    #[test]
    fn literal_null_lhs_false() {
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Literal {
                value: LiteralValue::Null,
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        };
        let pred = create_predicate(&cond);
        assert!(!pred(&row_kv(&[])));
    }

    // Static left panics.
    #[test]
    #[should_panic(expected = "static values")]
    fn static_left_panics() {
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
        let _ = create_predicate(&cond);
    }
    #[test]
    #[should_panic(expected = "static values")]
    fn static_right_panics() {
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Static {
                anchor: ParameterAnchor::AuthData,
                field: ParameterField::Single("uid".into()),
            },
        };
        let _ = create_predicate(&cond);
    }

    // CorrelatedSubquery arm panics.
    #[test]
    #[should_panic(expected = "CorrelatedSubquery")]
    fn csq_panics() {
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
        let _ = create_predicate(&cond);
    }

    // ─── transform_filters ────────────────────────────────────────────

    fn csq(child_alias: &str) -> Condition {
        let ast = AST {
            schema: None,
            table: "t".into(),
            alias: Some(child_alias.into()),
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        Condition::CorrelatedSubquery {
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
        }
    }

    // Branch: no filter.
    #[test]
    fn transform_none_passes_through() {
        let (t, r) = transform_filters(None);
        assert!(t.is_none());
        assert!(!r);
    }

    // Branch: simple pass through.
    #[test]
    fn transform_simple_pass_through() {
        let c = simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0));
        let (t, r) = transform_filters(Some(&c));
        assert_eq!(t, Some(c));
        assert!(!r);
    }

    // Branch: CSQ → removed.
    #[test]
    fn transform_csq_removed() {
        let c = csq("a");
        let (t, r) = transform_filters(Some(&c));
        assert!(t.is_none());
        assert!(r);
    }

    // Branch: AND drops CSQ child, keeps simple.
    #[test]
    fn transform_and_strips_csq_keeps_simple() {
        let simple_c = simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0));
        let cond = Condition::And {
            conditions: vec![simple_c.clone(), csq("x")],
        };
        let (t, r) = transform_filters(Some(&cond));
        assert!(r);
        // After stripping, only simple remains → simplified AND with one
        // child unwraps to the simple itself.
        assert_eq!(t, Some(simple_c));
    }

    // Branch: OR with any removed branch removes the whole OR.
    #[test]
    fn transform_or_with_csq_whole_removed() {
        let simple_c = simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0));
        let cond = Condition::Or {
            conditions: vec![simple_c, csq("x")],
        };
        let (t, r) = transform_filters(Some(&cond));
        assert!(t.is_none());
        assert!(r);
    }

    // Branch: AND of only CSQs → empty AND (TRUE), removed=true.
    #[test]
    fn transform_and_all_csq_becomes_true() {
        let cond = Condition::And {
            conditions: vec![csq("a"), csq("b")],
        };
        let (t, r) = transform_filters(Some(&cond));
        assert!(r);
        assert_eq!(t, Some(Condition::And { conditions: vec![] }));
    }

    // ─── simplify_condition ───────────────────────────────────────────

    #[test]
    fn simplify_simple_is_id() {
        let c = simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0));
        assert_eq!(simplify_condition(c.clone()), c);
    }

    #[test]
    fn simplify_single_child_and_unwraps() {
        let inner = simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0));
        let c = Condition::And {
            conditions: vec![inner.clone()],
        };
        assert_eq!(simplify_condition(c), inner);
    }

    #[test]
    fn simplify_flatten_nested_and() {
        let a = simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0));
        let b = simple(SimpleOperator::Eq, "b", LiteralValue::Number(2.0));
        let c = simple(SimpleOperator::Eq, "c", LiteralValue::Number(3.0));
        let nested = Condition::And {
            conditions: vec![
                Condition::And {
                    conditions: vec![a.clone(), b.clone()],
                },
                c.clone(),
            ],
        };
        let got = simplify_condition(nested);
        match got {
            Condition::And { conditions } => {
                assert_eq!(conditions.len(), 3);
                assert_eq!(conditions[0], a);
                assert_eq!(conditions[1], b);
                assert_eq!(conditions[2], c);
            }
            _ => panic!("expected AND"),
        }
    }

    // Branch: AND with FALSE → FALSE.
    #[test]
    fn simplify_and_with_false() {
        let a = simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0));
        let false_c = Condition::Or { conditions: vec![] };
        let cond = Condition::And {
            conditions: vec![a, false_c],
        };
        assert_eq!(
            simplify_condition(cond),
            Condition::Or { conditions: vec![] }
        );
    }

    // Branch: OR with TRUE → TRUE.
    #[test]
    fn simplify_or_with_true() {
        let a = simple(SimpleOperator::Eq, "a", LiteralValue::Number(1.0));
        let true_c = Condition::And { conditions: vec![] };
        let cond = Condition::Or {
            conditions: vec![a, true_c],
        };
        assert_eq!(
            simplify_condition(cond),
            Condition::And { conditions: vec![] }
        );
    }
}
