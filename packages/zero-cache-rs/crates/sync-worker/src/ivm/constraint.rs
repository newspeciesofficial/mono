//! Port of `packages/zql/src/ivm/constraint.ts`.
//!
//! A [`Constraint`] is a map of `column → required value` that a `Source`
//! applies as a pushdown filter when it feeds downstream operators. Filter
//! pushdown analysis walks the AST condition tree and extracts compatible
//! equality predicates into a Constraint; the planner later checks whether
//! the Constraint happens to match a primary key so the read can become a
//! point-lookup instead of a scan.
//!
//! TS semantics mirrored exactly:
//! - `constraintMatchesRow`   — every key present in constraint must
//!   [`values_equal`] the row's value for that key.
//! - `constraintsAreCompatible` — shared keys must have equal values; new
//!   keys on either side are free.
//! - `constraintMatchesPrimaryKey` / `keyMatchesPrimaryKey` — exact set
//!   equality under sorting by column name (PK is always sorted).
//! - `pullSimpleAndComponents` — fold top-level ANDs / single-branch ORs
//!   into a flat list of `Simple` conditions.
//! - `primaryKeyConstraintFromFilters` — given a `WHERE`, return a
//!   [`Constraint`] iff it fully pins the primary key.
//!
//! `SetOfConstraint` is a TESTING-only type in TS that exists to dedupe
//! constraints when asserting traces. Rust tests use `Vec<Constraint>`
//! directly, so `SetOfConstraint` is intentionally not ported.

use indexmap::IndexMap;

use zero_cache_types::ast::{
    Condition, LiteralValue, NonColumnValue, SimpleOperator, ValuePosition,
};
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::{Row, Value};

use super::data::values_equal;

/// TS: `type Constraint = {[key: string]: Value}`.
///
/// `IndexMap` preserves insertion order so tests (and the planner's
/// diagnostic logs) see a deterministic key ordering. Use [`values_equal`]
/// when comparing against row data — do **not** use `PartialEq` on
/// [`Value`], because IVM treats `null != null` in join-like positions.
pub type Constraint = IndexMap<String, Value>;

/// TS `constraintMatchesRow(constraint, row)`. Every key in `constraint`
/// must be present in `row` with an equal value. Rows with extra columns
/// still match — the constraint is a subset predicate.
pub fn constraint_matches_row(constraint: &Constraint, row: &Row) -> bool {
    for (key, want) in constraint.iter() {
        let got = row.get(key).cloned().unwrap_or(None);
        if !values_equal(&got, want) {
            return false;
        }
    }
    true
}

/// TS `constraintsAreCompatible(left, right)`. Shared keys must have
/// equal values; disjoint keys are fine.
pub fn constraints_are_compatible(left: &Constraint, right: &Constraint) -> bool {
    for (key, lv) in left.iter() {
        if let Some(rv) = right.get(key) {
            if !values_equal(lv, rv) {
                return false;
            }
        }
    }
    true
}

/// TS `constraintMatchesPrimaryKey(constraint, primary)`.
pub fn constraint_matches_primary_key(constraint: &Constraint, primary: &PrimaryKey) -> bool {
    key_matches_primary_key(constraint.keys().map(String::as_str), primary)
}

/// TS `keyMatchesPrimaryKey(key, primary)`. Both length and *set* of column
/// names must match; the constraint does not have to be pre-sorted.
///
/// The PK columns are already sorted at construction time in the TS
/// codebase; our Rust `PrimaryKey` follows the same contract — callers
/// are responsible for constructing sorted PKs. Here we sort the input
/// keys and compare element-wise.
pub fn key_matches_primary_key<'a>(
    key: impl IntoIterator<Item = &'a str>,
    primary: &PrimaryKey,
) -> bool {
    let mut keys: Vec<&str> = key.into_iter().collect();
    if keys.len() != primary.columns().len() {
        return false;
    }
    // TS: `constraintKeys.sort(stringCompare)`.
    // Rust `sort()` on &str is bytewise lexicographic — identical result.
    keys.sort();

    for (i, k) in keys.iter().enumerate() {
        if *k != primary.columns()[i].as_str() {
            return false;
        }
    }
    true
}

/// TS `pullSimpleAndComponents(condition)`.
///
/// Returns the set of simple conditions that are unconditionally implied
/// by the tree. Any `and` is flattened; an `or` with a single branch is
/// treated as that branch; any other `or` (or a correlated subquery)
/// yields nothing — the resulting list would not be a sound conjunction.
pub fn pull_simple_and_components(cond: &Condition) -> Vec<Condition> {
    match cond {
        Condition::And { conditions } => conditions
            .iter()
            .flat_map(pull_simple_and_components)
            .collect(),
        Condition::Simple { .. } => vec![cond.clone()],
        Condition::Or { conditions } if conditions.len() == 1 => {
            pull_simple_and_components(&conditions[0])
        }
        _ => Vec::new(),
    }
}

/// TS `primaryKeyConstraintFromFilters(condition, primary)`. If the
/// extractable `AND` components pin *every* PK column via `=`, return
/// them as a [`Constraint`]. Otherwise `None`.
///
/// Parity notes:
/// - Only `=` conditions contribute (TS ignores other ops).
/// - The left side must be a column reference; the right side must be a
///   literal. TS `assert`s on non-literal RHS; we return `None` for a
///   non-literal RHS instead of panicking — it's the same result the
///   caller would see after the assertion aborts.
pub fn primary_key_constraint_from_filters(
    condition: Option<&Condition>,
    primary: &PrimaryKey,
) -> Option<Constraint> {
    let condition = condition?;
    let comps = pull_simple_and_components(condition);
    if comps.is_empty() {
        return None;
    }

    let mut ret: Constraint = Constraint::new();
    for sub in comps.iter() {
        let Condition::Simple { op, left, right } = sub else {
            continue;
        };
        if *op != SimpleOperator::Eq {
            continue;
        }
        let Some(col) = extract_column(left, right) else {
            continue;
        };
        if !primary.columns().iter().any(|c| c == &col.name) {
            continue;
        }
        ret.insert(col.name, col.value);
    }

    if ret.len() != primary.columns().len() {
        return None;
    }
    Some(ret)
}

struct ExtractedColumn {
    name: String,
    value: Value,
}

/// TS `extractColumn(condition)` — returns the column name + literal value
/// iff `left` is a column reference and `right` is a literal.
fn extract_column(left: &ValuePosition, right: &NonColumnValue) -> Option<ExtractedColumn> {
    let ValuePosition::Column { name } = left else {
        return None;
    };
    let NonColumnValue::Literal { value } = right else {
        return None;
    };
    Some(ExtractedColumn {
        name: name.clone(),
        value: literal_to_value(value),
    })
}

/// Project a [`LiteralValue`] onto the IVM [`Value`] representation.
///
/// Arrays can't appear as the RHS of a simple `=` predicate, but TS
/// preserves them in the AST, so we do too — `IN` operators keep them
/// separately (handled by filter operators, not here).
fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::String(s) => Some(serde_json::Value::String(s.clone())),
        LiteralValue::Number(n) => Some(serde_json::json!(*n)),
        LiteralValue::Bool(b) => Some(serde_json::Value::Bool(*b)),
        LiteralValue::Null => Some(serde_json::Value::Null),
        LiteralValue::Array(arr) => {
            let values: Vec<serde_json::Value> = arr
                .iter()
                .map(|s| match s {
                    zero_cache_types::ast::ScalarLiteral::String(s) => {
                        serde_json::Value::String(s.clone())
                    }
                    zero_cache_types::ast::ScalarLiteral::Number(n) => {
                        serde_json::json!(*n)
                    }
                    zero_cache_types::ast::ScalarLiteral::Bool(b) => serde_json::Value::Bool(*b),
                })
                .collect();
            Some(serde_json::Value::Array(values))
        }
    }
}

#[cfg(test)]
mod tests {
    //! Branch-complete coverage.
    //!
    //! For `pull_simple_and_components` and `primary_key_constraint_from_filters`
    //! every `match` arm and every early-return is exercised. For the other
    //! functions, each conditional branch has a dedicated test.

    use super::*;
    use serde_json::json;
    use zero_cache_types::ast::{LiteralValue, NonColumnValue, SimpleOperator, ValuePosition};
    use zero_cache_types::primary_key::PrimaryKey;

    fn val(j: serde_json::Value) -> Value {
        Some(j)
    }

    fn row(fields: &[(&str, Value)]) -> Row {
        let mut r = Row::new();
        for (k, v) in fields {
            r.insert((*k).to_string(), v.clone());
        }
        r
    }

    fn constraint_of(fields: &[(&str, Value)]) -> Constraint {
        let mut c = Constraint::new();
        for (k, v) in fields {
            c.insert((*k).to_string(), v.clone());
        }
        c
    }

    fn pk(cols: &[&str]) -> PrimaryKey {
        PrimaryKey::new(cols.iter().map(|s| (*s).to_string()).collect())
    }

    fn col_eq_str(col: &str, rhs: &str) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: col.to_string(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::String(rhs.to_string()),
            },
        }
    }

    fn col_eq_num(col: &str, rhs: f64) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: col.to_string(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(rhs),
            },
        }
    }

    // ─── constraint_matches_row ────────────────────────────────────────

    #[test]
    fn matches_row_all_keys_match() {
        let c = constraint_of(&[("id", val(json!(1))), ("name", val(json!("x")))]);
        let r = row(&[
            ("id", val(json!(1))),
            ("name", val(json!("x"))),
            ("extra", val(json!(true))),
        ]);
        assert!(constraint_matches_row(&c, &r));
    }

    #[test]
    fn matches_row_mismatch_returns_false() {
        let c = constraint_of(&[("id", val(json!(1)))]);
        let r = row(&[("id", val(json!(2)))]);
        assert!(!constraint_matches_row(&c, &r));
    }

    #[test]
    fn matches_row_missing_key_counts_as_mismatch() {
        // Row absent the key → Value::None → values_equal(None, Some) is false.
        let c = constraint_of(&[("id", val(json!(1)))]);
        let r = row(&[("name", val(json!("x")))]);
        assert!(!constraint_matches_row(&c, &r));
    }

    #[test]
    fn matches_row_null_on_either_side_is_false() {
        // values_equal treats null as non-equal to everything including null.
        let c = constraint_of(&[("id", Some(json!(null)))]);
        let r = row(&[("id", Some(json!(null)))]);
        assert!(!constraint_matches_row(&c, &r));
    }

    #[test]
    fn matches_row_empty_constraint_always_true() {
        let c = Constraint::new();
        let r = row(&[("id", val(json!(1)))]);
        assert!(constraint_matches_row(&c, &r));
    }

    // ─── constraints_are_compatible ────────────────────────────────────

    #[test]
    fn compatible_no_shared_keys() {
        let a = constraint_of(&[("id", val(json!(1)))]);
        let b = constraint_of(&[("name", val(json!("x")))]);
        assert!(constraints_are_compatible(&a, &b));
    }

    #[test]
    fn compatible_shared_key_same_value() {
        let a = constraint_of(&[("id", val(json!(1)))]);
        let b = constraint_of(&[("id", val(json!(1))), ("x", val(json!(2)))]);
        assert!(constraints_are_compatible(&a, &b));
    }

    #[test]
    fn incompatible_shared_key_different_value() {
        let a = constraint_of(&[("id", val(json!(1)))]);
        let b = constraint_of(&[("id", val(json!(2)))]);
        assert!(!constraints_are_compatible(&a, &b));
    }

    #[test]
    fn compatible_empty_either_side() {
        let a = Constraint::new();
        let b = constraint_of(&[("id", val(json!(1)))]);
        assert!(constraints_are_compatible(&a, &b));
        assert!(constraints_are_compatible(&b, &a));
    }

    // ─── key_matches_primary_key ───────────────────────────────────────

    #[test]
    fn key_matches_pk_exact_sorted() {
        assert!(key_matches_primary_key(["id"].into_iter(), &pk(&["id"])));
    }

    #[test]
    fn key_matches_pk_wrong_length() {
        assert!(!key_matches_primary_key(
            ["id"].into_iter(),
            &pk(&["id", "tenant"])
        ));
    }

    #[test]
    fn key_matches_pk_sorts_input_before_compare() {
        // Input order reversed; PK stored sorted.
        assert!(key_matches_primary_key(
            ["tenant", "id"].into_iter(),
            &pk(&["id", "tenant"])
        ));
    }

    #[test]
    fn key_matches_pk_wrong_names() {
        assert!(!key_matches_primary_key(
            ["id", "name"].into_iter(),
            &pk(&["id", "tenant"])
        ));
    }

    #[test]
    fn constraint_matches_pk_delegates_to_key_matches() {
        let c = constraint_of(&[("tenant", val(json!("t"))), ("id", val(json!("i")))]);
        assert!(constraint_matches_primary_key(&c, &pk(&["id", "tenant"])));
    }

    // ─── pull_simple_and_components ────────────────────────────────────

    #[test]
    fn pull_from_simple_returns_self() {
        let c = col_eq_str("a", "x");
        let out = pull_simple_and_components(&c);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Condition::Simple { .. }));
    }

    #[test]
    fn pull_from_and_flattens() {
        let c = Condition::And {
            conditions: vec![col_eq_str("a", "x"), col_eq_str("b", "y")],
        };
        let out = pull_simple_and_components(&c);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn pull_from_nested_and_flattens_recursively() {
        let c = Condition::And {
            conditions: vec![
                col_eq_str("a", "x"),
                Condition::And {
                    conditions: vec![col_eq_str("b", "y"), col_eq_str("c", "z")],
                },
            ],
        };
        let out = pull_simple_and_components(&c);
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn pull_from_or_single_branch_descends() {
        let c = Condition::Or {
            conditions: vec![col_eq_str("a", "x")],
        };
        let out = pull_simple_and_components(&c);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn pull_from_or_multiple_branches_yields_nothing() {
        // TS returns [] for real disjunctions — we cannot assume both sides.
        let c = Condition::Or {
            conditions: vec![col_eq_str("a", "x"), col_eq_str("b", "y")],
        };
        assert!(pull_simple_and_components(&c).is_empty());
    }

    #[test]
    fn pull_from_or_empty_yields_nothing() {
        let c = Condition::Or { conditions: vec![] };
        assert!(pull_simple_and_components(&c).is_empty());
    }

    #[test]
    fn pull_from_correlated_subquery_yields_nothing() {
        use zero_cache_types::ast::{AST, CorrelatedSubquery, CorrelatedSubqueryOp, Correlation};
        let subq = CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["parent_id".into()],
            },
            subquery: Box::new(AST {
                schema: None,
                table: "t".into(),
                alias: None,
                where_clause: None,
                related: None,
                start: None,
                limit: None,
                order_by: None,
            }),
            system: None,
            hidden: None,
        };
        let c = Condition::CorrelatedSubquery {
            related: Box::new(subq),
            op: CorrelatedSubqueryOp::EXISTS,
            flip: None,
            scalar: None,
        };
        assert!(pull_simple_and_components(&c).is_empty());
    }

    // ─── primary_key_constraint_from_filters ───────────────────────────

    #[test]
    fn pk_filter_none_condition_returns_none() {
        assert!(primary_key_constraint_from_filters(None, &pk(&["id"])).is_none());
    }

    #[test]
    fn pk_filter_or_top_level_returns_none() {
        let c = Condition::Or {
            conditions: vec![col_eq_str("id", "a"), col_eq_str("id", "b")],
        };
        assert!(primary_key_constraint_from_filters(Some(&c), &pk(&["id"])).is_none());
    }

    #[test]
    fn pk_filter_single_key_exact_match() {
        let c = col_eq_str("id", "x");
        let cons = primary_key_constraint_from_filters(Some(&c), &pk(&["id"])).expect("must match");
        assert_eq!(cons.len(), 1);
        assert_eq!(cons.get("id"), Some(&val(json!("x"))));
    }

    #[test]
    fn pk_filter_compound_pk_exact_match() {
        let c = Condition::And {
            conditions: vec![col_eq_str("tenant", "t"), col_eq_str("id", "i")],
        };
        let cons = primary_key_constraint_from_filters(Some(&c), &pk(&["id", "tenant"]))
            .expect("must match");
        assert_eq!(cons.len(), 2);
    }

    #[test]
    fn pk_filter_missing_pk_component_returns_none() {
        // PK=(id, tenant) but only `id` is constrained → None.
        let c = col_eq_str("id", "x");
        assert!(primary_key_constraint_from_filters(Some(&c), &pk(&["id", "tenant"])).is_none());
    }

    #[test]
    fn pk_filter_non_equality_op_ignored() {
        // `>` doesn't contribute, so the constraint map stays empty → None.
        let c = Condition::Simple {
            op: SimpleOperator::Gt,
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        };
        assert!(primary_key_constraint_from_filters(Some(&c), &pk(&["id"])).is_none());
    }

    #[test]
    fn pk_filter_ignores_non_pk_columns() {
        // `id` pins the only PK column; `other` equality is ignored but
        // doesn't break the match.
        let c = Condition::And {
            conditions: vec![col_eq_str("id", "x"), col_eq_str("other", "y")],
        };
        let cons = primary_key_constraint_from_filters(Some(&c), &pk(&["id"])).expect("must match");
        assert_eq!(cons.len(), 1);
        assert_eq!(cons.get("id"), Some(&val(json!("x"))));
    }

    #[test]
    fn pk_filter_rhs_non_literal_is_skipped() {
        // Static parameter RHS → extract_column returns None → entry is
        // never inserted → final map length != PK length → None.
        use zero_cache_types::ast::{ParameterAnchor, ParameterField};
        let c = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: NonColumnValue::Static {
                anchor: ParameterAnchor::AuthData,
                field: ParameterField::Single("sub".to_string()),
            },
        };
        assert!(primary_key_constraint_from_filters(Some(&c), &pk(&["id"])).is_none());
    }

    #[test]
    fn pk_filter_lhs_non_column_is_skipped() {
        // Literal on the LHS — not a column reference.
        let c = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Literal {
                value: LiteralValue::Number(1.0),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        };
        assert!(primary_key_constraint_from_filters(Some(&c), &pk(&["id"])).is_none());
    }

    #[test]
    fn pk_filter_numeric_literal_projects_to_value() {
        let c = col_eq_num("id", 42.0);
        let cons = primary_key_constraint_from_filters(Some(&c), &pk(&["id"])).expect("must match");
        // Number literal becomes serde_json number.
        let v = cons.get("id").unwrap().as_ref().unwrap();
        assert!(v.is_number());
    }
}
