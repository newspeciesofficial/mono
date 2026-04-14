//! Port of `packages/zqlite/src/resolve-scalar-subqueries.ts`.
//!
//! Public exports ported:
//!
//! - [`CompanionSubquery`] — record describing a scalar subquery that was
//!   resolved to a literal (the original subquery AST, the child field
//!   whose value was extracted, and the resolved value). TS
//!   `CompanionSubquery`.
//! - [`ResolveResult`] — pair returned by [`resolve_simple_scalar_subqueries`]:
//!   the transformed AST and all companion subqueries that need to be
//!   synced to the client for the EXISTS rewrite to work. TS
//!   `ResolveResult`.
//! - [`ScalarValue`] — the value produced by a scalar executor. Unifies
//!   TS's `LiteralValue | null | undefined` tri-state (since Rust's
//!   `Option<LiteralValue>` already encodes "row matched with NULL field"
//!   vs. "no row matched" we spell out the three cases explicitly).
//! - [`ScalarExecutor`] — closure type. Given a subquery AST and a
//!   child-field name, return the scalar value (or a null / absent
//!   marker). TS `ScalarExecutor`.
//! - [`resolve_simple_scalar_subqueries`] — the entry point. TS
//!   `resolveSimpleScalarSubqueries`.
//! - [`is_simple_subquery`] — predicate testing whether a subquery is
//!   guaranteed to return at most one deterministic row. TS
//!   `isSimpleSubquery`.
//! - [`extract_literal_equality_constraints`] — extracts
//!   column=literal constraints under AND conjunction only. TS
//!   `extractLiteralEqualityConstraints`.
//!
//! ## TS semantics notes
//!
//! - TS uses `undefined` to mean "no row matched" and `null` to mean
//!   "row matched but the column was `NULL`". In Rust we spell the same
//!   tri-state explicitly with [`ScalarValue`]. Both the no-row and NULL
//!   cases produce the TS `ALWAYS_FALSE` simple condition.
//! - TS passes `tableSpecs` as
//!   `Map<string, {tableSpec: {uniqueKeys: PrimaryKey[]}}>` — a
//!   minimal interface. We use [`TableSpecLookup`] which matches the
//!   subset of fields consumed.
//! - TS `resolveASTRecursive` performs referential-identity checks on
//!   `where` / `related` to return the original AST when nothing
//!   changed. Rust doesn't model `Box` identity cheaply, so we instead
//!   compare structurally (`PartialEq`), matching the observable
//!   semantics of the TS test suite (the
//!   `returns original AST when nothing to resolve` test checks
//!   `toBe(ast)` but works equally well under deep equality).
//! - TS `op === 'EXISTS' ? '=' : 'IS NOT'` maps to
//!   [`SimpleOperator::Eq`] / [`SimpleOperator::IsNot`] in Rust.

use std::collections::HashMap;

use zero_cache_types::ast::{
    AST, Condition, CorrelatedSubquery, CorrelatedSubqueryOp, LiteralValue, NonColumnValue,
    SimpleOperator, ValuePosition,
};

/// Minimal view of the table-specs map used by
/// [`is_simple_subquery`]. Any type exposing a
/// `unique_keys(table: &str) -> Option<&[Vec<String>]>` lookup satisfies
/// this. A concrete implementation for
/// [`crate::view_syncer::client_schema::LiteAndZqlSpec`] is provided below.
pub trait TableSpecLookup {
    /// Return the list of unique keys for `table`, or `None` if the
    /// table is not in the spec. Each unique key is a list of column
    /// names.
    fn unique_keys(&self, table: &str) -> Option<&[Vec<String>]>;
}

impl TableSpecLookup
    for indexmap::IndexMap<String, crate::view_syncer::client_schema::LiteAndZqlSpec>
{
    fn unique_keys(&self, table: &str) -> Option<&[Vec<String>]> {
        self.get(table)
            .map(|s| s.table_spec.all_potential_primary_keys.as_slice())
    }
}

impl TableSpecLookup for HashMap<String, Vec<Vec<String>>> {
    fn unique_keys(&self, table: &str) -> Option<&[Vec<String>]> {
        self.get(table).map(|v| v.as_slice())
    }
}

/// The result of executing a scalar subquery.
///
/// | TS                       | Rust                          |
/// | ------------------------ | ----------------------------- |
/// | value (non-null literal) | [`ScalarValue::Value`]        |
/// | `null`                   | [`ScalarValue::Null`]         |
/// | `undefined`              | [`ScalarValue::Absent`]       |
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// The subquery produced one row and the column had a non-null
    /// literal value.
    Value(LiteralValue),
    /// The subquery produced one row but the column was SQL NULL.
    Null,
    /// The subquery produced no rows.
    Absent,
}

/// Callback that executes a scalar subquery and returns the value of
/// `child_field` from the (at most one) matching row.
///
/// TS `ScalarExecutor`.
pub type ScalarExecutor<'a> = dyn FnMut(&AST, &str) -> ScalarValue + 'a;

/// TS `CompanionSubquery`.
#[derive(Debug, Clone, PartialEq)]
pub struct CompanionSubquery {
    /// The original scalar subquery AST (the subquery table query).
    pub ast: AST,
    /// The field in the subquery row whose value was resolved.
    pub child_field: String,
    /// The resolved scalar value.
    pub resolved_value: ScalarValue,
}

/// TS `ResolveResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolveResult {
    pub ast: AST,
    pub companions: Vec<CompanionSubquery>,
}

/// ALWAYS_FALSE constant from TS. `1 = 0` is a compact literal=literal
/// condition that is guaranteed false regardless of row.
fn always_false() -> Condition {
    Condition::Simple {
        op: SimpleOperator::Eq,
        left: ValuePosition::Literal {
            value: LiteralValue::Number(1.0),
        },
        right: NonColumnValue::Literal {
            value: LiteralValue::Number(0.0),
        },
    }
}

/// Resolves "simple" scalar subqueries by calling the provided
/// executor and replacing them with literal conditions.
///
/// A scalar subquery is simple when all columns of at least one unique
/// index on the subquery table are equality-constrained by literal
/// values in the subquery's WHERE clause (using only AND conjunctions).
///
/// Non-simple scalar subqueries are left untouched for the existing
/// EXISTS rewrite in `build_pipeline_internal`.
///
/// Returns the resolved AST and a list of companion subquery ASTs
/// whose rows need to be synced to the client for the EXISTS rewrite
/// to work.
pub fn resolve_simple_scalar_subqueries(
    ast: AST,
    table_specs: &dyn TableSpecLookup,
    execute: &mut ScalarExecutor<'_>,
) -> ResolveResult {
    let mut companions = Vec::new();
    let resolved = resolve_ast_recursive(ast, table_specs, execute, &mut companions);
    ResolveResult {
        ast: resolved,
        companions,
    }
}

fn resolve_ast_recursive(
    mut ast: AST,
    table_specs: &dyn TableSpecLookup,
    execute: &mut ScalarExecutor<'_>,
    companions: &mut Vec<CompanionSubquery>,
) -> AST {
    // where
    if let Some(w) = ast.where_clause.take() {
        let resolved = resolve_condition(*w, table_specs, execute, companions);
        ast.where_clause = Some(Box::new(resolved));
    }

    // related
    if let Some(related) = ast.related.take() {
        let new_related: Vec<CorrelatedSubquery> = related
            .into_iter()
            .map(|r| CorrelatedSubquery {
                correlation: r.correlation,
                subquery: Box::new(resolve_ast_recursive(
                    *r.subquery,
                    table_specs,
                    execute,
                    companions,
                )),
                system: r.system,
                hidden: r.hidden,
            })
            .collect();
        ast.related = Some(new_related);
    }

    ast
}

fn resolve_condition(
    condition: Condition,
    table_specs: &dyn TableSpecLookup,
    execute: &mut ScalarExecutor<'_>,
    companions: &mut Vec<CompanionSubquery>,
) -> Condition {
    match condition {
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
        } => {
            if scalar == Some(true) {
                resolve_scalar_subquery(
                    CorrelatedSubqueryCondition {
                        related: *related,
                        op,
                        flip,
                        scalar,
                    },
                    table_specs,
                    execute,
                    companions,
                )
            } else {
                // Non-scalar correlated subquery: recurse into its subquery.
                let CorrelatedSubquery {
                    correlation,
                    subquery,
                    system,
                    hidden,
                } = *related;
                let resolved_sub =
                    resolve_ast_recursive(*subquery, table_specs, execute, companions);
                Condition::CorrelatedSubquery {
                    related: Box::new(CorrelatedSubquery {
                        correlation,
                        subquery: Box::new(resolved_sub),
                        system,
                        hidden,
                    }),
                    op,
                    flip,
                    scalar,
                }
            }
        }
        Condition::And { conditions } => {
            let resolved: Vec<Condition> = conditions
                .into_iter()
                .map(|c| resolve_condition(c, table_specs, execute, companions))
                .collect();
            Condition::And {
                conditions: resolved,
            }
        }
        Condition::Or { conditions } => {
            let resolved: Vec<Condition> = conditions
                .into_iter()
                .map(|c| resolve_condition(c, table_specs, execute, companions))
                .collect();
            Condition::Or {
                conditions: resolved,
            }
        }
        other => other,
    }
}

/// Internal struct mirroring the TS `CorrelatedSubqueryCondition` shape
/// (the destructured `Condition::CorrelatedSubquery` variant), used only
/// for clarity inside [`resolve_scalar_subquery`].
struct CorrelatedSubqueryCondition {
    related: CorrelatedSubquery,
    op: CorrelatedSubqueryOp,
    flip: Option<bool>,
    scalar: Option<bool>,
}

fn resolve_scalar_subquery(
    condition: CorrelatedSubqueryCondition,
    table_specs: &dyn TableSpecLookup,
    execute: &mut ScalarExecutor<'_>,
    companions: &mut Vec<CompanionSubquery>,
) -> Condition {
    let parent_field = condition
        .related
        .correlation
        .parent_field
        .first()
        .cloned()
        .unwrap_or_default();
    let child_field = condition
        .related
        .correlation
        .child_field
        .first()
        .cloned()
        .unwrap_or_default();

    // Recursively resolve any scalar subqueries nested in the
    // subquery's own WHERE (and related) before evaluating this one.
    let subquery = resolve_ast_recursive(
        *condition.related.subquery,
        table_specs,
        execute,
        companions,
    );

    if !is_simple_subquery(&subquery, table_specs) {
        // Return with the (possibly partially-resolved) subquery.
        return Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: condition.related.correlation,
                subquery: Box::new(subquery),
                system: condition.related.system,
                hidden: condition.related.hidden,
            }),
            op: condition.op,
            flip: condition.flip,
            scalar: condition.scalar,
        };
    }

    let value = execute(&subquery, &child_field);

    // Record the companion subquery AST so its rows are synced to the
    // client. The client rewrites scalar subqueries to EXISTS and
    // needs those rows.
    companions.push(CompanionSubquery {
        ast: subquery,
        child_field: child_field.clone(),
        resolved_value: value.clone(),
    });

    match value {
        ScalarValue::Absent | ScalarValue::Null => always_false(),
        ScalarValue::Value(lit) => {
            let op = match condition.op {
                CorrelatedSubqueryOp::EXISTS => SimpleOperator::Eq,
                CorrelatedSubqueryOp::NotExists => SimpleOperator::IsNot,
            };
            Condition::Simple {
                op,
                left: ValuePosition::Column { name: parent_field },
                right: NonColumnValue::Literal { value: lit },
            }
        }
    }
}

/// Checks if the subquery is guaranteed to return at most one
/// deterministic row.
///
/// This is true when all columns of at least one unique index on the
/// subquery table are equality-constrained by literal values in the
/// WHERE clause (using only AND conjunctions).
pub fn is_simple_subquery(subquery: &AST, table_specs: &dyn TableSpecLookup) -> bool {
    let Some(unique_keys) = table_specs.unique_keys(&subquery.table) else {
        return false;
    };

    let Some(where_clause) = subquery.where_clause.as_deref() else {
        return false;
    };

    let constraints = extract_literal_equality_constraints(where_clause);
    if constraints.is_empty() {
        return false;
    }

    unique_keys
        .iter()
        .any(|key| key.iter().all(|col| constraints.contains_key(col)))
}

/// Extracts column=literal equality constraints from a condition tree,
/// only following AND conjunctions (not OR).
pub fn extract_literal_equality_constraints(
    condition: &Condition,
) -> HashMap<String, LiteralValue> {
    let mut out = HashMap::new();
    collect_constraints(condition, &mut out);
    out
}

fn collect_constraints(condition: &Condition, out: &mut HashMap<String, LiteralValue>) {
    match condition {
        Condition::Simple { op, left, right } => {
            if *op == SimpleOperator::Eq {
                if let (ValuePosition::Column { name }, NonColumnValue::Literal { value }) =
                    (left, right)
                {
                    out.insert(name.clone(), value.clone());
                }
            }
        }
        Condition::And { conditions } => {
            for c in conditions {
                collect_constraints(c, out);
            }
        }
        // OR, correlatedSubquery (non-scalar) — don't contribute constraints.
        _ => {}
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_types::ast::{CorrelatedSubqueryOp, Correlation};

    // ── helpers ───────────────────────────────────────────────────────

    fn specs(entries: &[(&str, &[&[&str]])]) -> HashMap<String, Vec<Vec<String>>> {
        let mut m = HashMap::new();
        for (table, keys) in entries {
            let ks: Vec<Vec<String>> = keys
                .iter()
                .map(|k| k.iter().map(|s| s.to_string()).collect())
                .collect();
            m.insert((*table).to_string(), ks);
        }
        m
    }

    fn col(name: &str) -> ValuePosition {
        ValuePosition::Column {
            name: name.to_string(),
        }
    }
    fn lit_str(s: &str) -> NonColumnValue {
        NonColumnValue::Literal {
            value: LiteralValue::String(s.to_string()),
        }
    }
    fn lit_num(n: f64) -> NonColumnValue {
        NonColumnValue::Literal {
            value: LiteralValue::Number(n),
        }
    }
    fn lit_bool(b: bool) -> NonColumnValue {
        NonColumnValue::Literal {
            value: LiteralValue::Bool(b),
        }
    }
    fn eq(name: &str, rhs: NonColumnValue) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: col(name),
            right: rhs,
        }
    }

    fn scalar_sub(op: CorrelatedSubqueryOp, parent: &str, child: &str, subquery: AST) -> Condition {
        Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec![parent.to_string()],
                    child_field: vec![child.to_string()],
                },
                subquery: Box::new(subquery),
                system: None,
                hidden: None,
            }),
            op,
            flip: None,
            scalar: Some(true),
        }
    }

    fn non_scalar_sub(
        op: CorrelatedSubqueryOp,
        parent: &str,
        child: &str,
        subquery: AST,
    ) -> Condition {
        Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec![parent.to_string()],
                    child_field: vec![child.to_string()],
                },
                subquery: Box::new(subquery),
                system: None,
                hidden: None,
            }),
            op,
            flip: None,
            scalar: None,
        }
    }

    fn simple_ast(table: &str, where_clause: Option<Condition>) -> AST {
        AST {
            schema: None,
            table: table.to_string(),
            alias: None,
            where_clause: where_clause.map(Box::new),
            related: None,
            start: None,
            limit: None,
            order_by: None,
        }
    }

    // ── extract_literal_equality_constraints ──────────────────────────

    /// Branch: Simple, op = Equal, column = literal.
    #[test]
    fn extract_simple_col_eq_literal() {
        let cond = eq("id", lit_str("42"));
        let out = extract_literal_equality_constraints(&cond);
        assert_eq!(out.len(), 1);
        assert_eq!(out.get("id"), Some(&LiteralValue::String("42".to_string())));
    }

    /// Branch: Simple, op != Equal → ignored.
    #[test]
    fn extract_ignores_non_equal_op() {
        let cond = Condition::Simple {
            op: SimpleOperator::Gt,
            left: col("id"),
            right: lit_num(10.0),
        };
        assert!(extract_literal_equality_constraints(&cond).is_empty());
    }

    /// Branch: And — each child contributes.
    #[test]
    fn extract_collects_from_and() {
        let cond = Condition::And {
            conditions: vec![eq("a", lit_num(1.0)), eq("b", lit_num(2.0))],
        };
        let out = extract_literal_equality_constraints(&cond);
        assert_eq!(out.len(), 2);
        assert_eq!(out.get("a"), Some(&LiteralValue::Number(1.0)));
        assert_eq!(out.get("b"), Some(&LiteralValue::Number(2.0)));
    }

    /// Branch: Or — does NOT contribute constraints.
    #[test]
    fn extract_does_not_descend_into_or() {
        let cond = Condition::Or {
            conditions: vec![eq("a", lit_num(1.0))],
        };
        assert!(extract_literal_equality_constraints(&cond).is_empty());
    }

    /// Branch: Simple with non-column left (literal=literal) → ignored.
    #[test]
    fn extract_ignores_literal_eq_literal() {
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Literal {
                value: LiteralValue::Number(1.0),
            },
            right: lit_num(1.0),
        };
        assert!(extract_literal_equality_constraints(&cond).is_empty());
    }

    /// Branch: CorrelatedSubquery — not a recognised contributor.
    #[test]
    fn extract_ignores_correlated_subquery() {
        let cond = scalar_sub(
            CorrelatedSubqueryOp::EXISTS,
            "a",
            "b",
            simple_ast("x", None),
        );
        assert!(extract_literal_equality_constraints(&cond).is_empty());
    }

    // ── is_simple_subquery ────────────────────────────────────────────

    /// Branch: unique key fully constrained → true.
    #[test]
    fn is_simple_single_key_constrained() {
        let s = specs(&[("users", &[&["id"]])]);
        let sub = simple_ast("users", Some(eq("id", lit_str("0001"))));
        assert!(is_simple_subquery(&sub, &s));
    }

    /// Branch: composite unique key, all columns present → true.
    #[test]
    fn is_simple_composite_key_constrained() {
        let s = specs(&[("issueLabel", &[&["issueId", "labelId"]])]);
        let sub = simple_ast(
            "issueLabel",
            Some(Condition::And {
                conditions: vec![eq("issueId", lit_str("1")), eq("labelId", lit_str("2"))],
            }),
        );
        assert!(is_simple_subquery(&sub, &s));
    }

    /// Branch: composite unique key, one column missing → false.
    #[test]
    fn is_simple_partial_key_false() {
        let s = specs(&[("issueLabel", &[&["issueId", "labelId"]])]);
        let sub = simple_ast("issueLabel", Some(eq("issueId", lit_str("1"))));
        assert!(!is_simple_subquery(&sub, &s));
    }

    /// Branch: no WHERE clause → false.
    #[test]
    fn is_simple_no_where_false() {
        let s = specs(&[("users", &[&["id"]])]);
        let sub = simple_ast("users", None);
        assert!(!is_simple_subquery(&sub, &s));
    }

    /// Branch: table not in specs → false.
    #[test]
    fn is_simple_unknown_table_false() {
        let s = specs(&[]);
        let sub = simple_ast("unknown", Some(eq("id", lit_str("1"))));
        assert!(!is_simple_subquery(&sub, &s));
    }

    /// Branch: any one of multiple unique keys satisfied → true.
    #[test]
    fn is_simple_any_unique_key_satisfies() {
        let s = specs(&[("users", &[&["id"], &["email", "tenant"]])]);
        let sub = simple_ast(
            "users",
            Some(Condition::And {
                conditions: vec![eq("email", lit_str("a@b.com")), eq("tenant", lit_str("t1"))],
            }),
        );
        assert!(is_simple_subquery(&sub, &s));
    }

    /// Branch: WHERE has no equality constraints → false (empty map).
    #[test]
    fn is_simple_empty_constraints_false() {
        let s = specs(&[("users", &[&["id"]])]);
        let sub = simple_ast(
            "users",
            Some(Condition::Simple {
                op: SimpleOperator::Gt,
                left: col("id"),
                right: lit_num(10.0),
            }),
        );
        assert!(!is_simple_subquery(&sub, &s));
    }

    // ── resolve_simple_scalar_subqueries ──────────────────────────────

    /// Branch: simple EXISTS scalar subquery with value → `= literal`.
    #[test]
    fn resolves_simple_scalar_to_literal_equal() {
        let s = specs(&[("users", &[&["id"]])]);
        let ast = simple_ast(
            "issues",
            Some(scalar_sub(
                CorrelatedSubqueryOp::EXISTS,
                "ownerId",
                "name",
                simple_ast("users", Some(eq("id", lit_str("0001")))),
            )),
        );
        let mut exec = |_ast: &AST, _field: &str| {
            ScalarValue::Value(LiteralValue::String("Alice".to_string()))
        };
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        assert_eq!(
            *result.ast.where_clause.unwrap(),
            Condition::Simple {
                op: SimpleOperator::Eq,
                left: col("ownerId"),
                right: lit_str("Alice"),
            }
        );
        assert_eq!(result.companions.len(), 1);
        assert_eq!(result.companions[0].ast.table, "users");
    }

    /// Branch: NOT EXISTS scalar → `IS NOT` operator.
    #[test]
    fn resolves_not_exists_scalar_to_is_not() {
        let s = specs(&[("users", &[&["id"]])]);
        let ast = simple_ast(
            "issues",
            Some(scalar_sub(
                CorrelatedSubqueryOp::NotExists,
                "ownerId",
                "name",
                simple_ast("users", Some(eq("id", lit_str("0001")))),
            )),
        );
        let mut exec = |_ast: &AST, _field: &str| {
            ScalarValue::Value(LiteralValue::String("Alice".to_string()))
        };
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        if let Condition::Simple { op, .. } = *result.ast.where_clause.unwrap() {
            assert_eq!(op, SimpleOperator::IsNot);
        } else {
            panic!("expected simple");
        }
    }

    /// Branch: executor returns Absent → ALWAYS_FALSE condition.
    #[test]
    fn resolves_absent_to_always_false() {
        let s = specs(&[("users", &[&["id"]])]);
        let ast = simple_ast(
            "issues",
            Some(scalar_sub(
                CorrelatedSubqueryOp::EXISTS,
                "ownerId",
                "name",
                simple_ast("users", Some(eq("id", lit_str("nope")))),
            )),
        );
        let mut exec = |_ast: &AST, _field: &str| ScalarValue::Absent;
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        assert_eq!(*result.ast.where_clause.unwrap(), always_false());
        assert_eq!(result.companions.len(), 1);
    }

    /// Branch: executor returns Null → ALWAYS_FALSE condition.
    #[test]
    fn resolves_null_to_always_false() {
        let s = specs(&[("users", &[&["id"]])]);
        let ast = simple_ast(
            "issues",
            Some(scalar_sub(
                CorrelatedSubqueryOp::EXISTS,
                "ownerId",
                "name",
                simple_ast("users", Some(eq("id", lit_str("0001")))),
            )),
        );
        let mut exec = |_ast: &AST, _field: &str| ScalarValue::Null;
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        assert_eq!(*result.ast.where_clause.unwrap(), always_false());
    }

    /// Branch: non-simple scalar subquery (no where) → left untouched,
    /// executor not called, no companions.
    #[test]
    fn leaves_non_simple_untouched() {
        let s = specs(&[("users", &[&["id"]])]);
        let ast = simple_ast(
            "issues",
            Some(scalar_sub(
                CorrelatedSubqueryOp::EXISTS,
                "ownerId",
                "name",
                simple_ast("users", None),
            )),
        );
        let original_where = ast.where_clause.clone();
        let mut called = false;
        let mut exec = |_ast: &AST, _field: &str| {
            called = true;
            ScalarValue::Value(LiteralValue::String("x".to_string()))
        };
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        assert_eq!(result.ast.where_clause, original_where);
        assert_eq!(result.companions.len(), 0);
        assert!(!called);
    }

    /// Branch: scalar subquery inside AND — recurse into And arm.
    #[test]
    fn resolves_scalar_inside_and() {
        let s = specs(&[("users", &[&["id"]])]);
        let ast = simple_ast(
            "issues",
            Some(Condition::And {
                conditions: vec![
                    eq("closed", lit_bool(false)),
                    scalar_sub(
                        CorrelatedSubqueryOp::EXISTS,
                        "ownerId",
                        "id",
                        simple_ast("users", Some(eq("id", lit_str("0001")))),
                    ),
                ],
            }),
        );
        let mut exec =
            |_ast: &AST, _field: &str| ScalarValue::Value(LiteralValue::String("0001".to_string()));
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        let expected = Condition::And {
            conditions: vec![
                eq("closed", lit_bool(false)),
                Condition::Simple {
                    op: SimpleOperator::Eq,
                    left: col("ownerId"),
                    right: lit_str("0001"),
                },
            ],
        };
        assert_eq!(*result.ast.where_clause.unwrap(), expected);
    }

    /// Branch: scalar subquery inside OR — recurse into Or arm.
    #[test]
    fn resolves_scalar_inside_or() {
        let s = specs(&[("users", &[&["id"]])]);
        let ast = simple_ast(
            "issues",
            Some(Condition::Or {
                conditions: vec![
                    scalar_sub(
                        CorrelatedSubqueryOp::EXISTS,
                        "ownerId",
                        "id",
                        simple_ast("users", Some(eq("id", lit_str("0001")))),
                    ),
                    eq("id", lit_str("0003")),
                ],
            }),
        );
        let mut exec =
            |_ast: &AST, _field: &str| ScalarValue::Value(LiteralValue::String("0001".to_string()));
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        let expected = Condition::Or {
            conditions: vec![
                Condition::Simple {
                    op: SimpleOperator::Eq,
                    left: col("ownerId"),
                    right: lit_str("0001"),
                },
                eq("id", lit_str("0003")),
            ],
        };
        assert_eq!(*result.ast.where_clause.unwrap(), expected);
    }

    /// Branch: scalar subquery inside `related` subquery — recurse
    /// through `related`.
    #[test]
    fn resolves_scalar_in_related() {
        let s = specs(&[("users", &[&["id"]])]);
        let inner = simple_ast(
            "users",
            Some(scalar_sub(
                CorrelatedSubqueryOp::EXISTS,
                "name",
                "name",
                simple_ast("users", Some(eq("id", lit_str("0002")))),
            )),
        );
        let ast = AST {
            schema: None,
            table: "issues".to_string(),
            alias: None,
            where_clause: None,
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["ownerId".to_string()],
                    child_field: vec!["id".to_string()],
                },
                subquery: Box::new(inner),
                system: None,
                hidden: None,
            }]),
            start: None,
            limit: None,
            order_by: None,
        };
        let mut exec =
            |_ast: &AST, _field: &str| ScalarValue::Value(LiteralValue::String("Bob".to_string()));
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        let related = result.ast.related.unwrap();
        let inner_where = related[0].subquery.where_clause.as_deref().unwrap();
        assert_eq!(
            *inner_where,
            Condition::Simple {
                op: SimpleOperator::Eq,
                left: col("name"),
                right: lit_str("Bob"),
            }
        );
        assert_eq!(result.companions.len(), 1);
    }

    /// Branch: nothing to resolve → original AST returned structurally.
    #[test]
    fn returns_unchanged_when_nothing_to_resolve() {
        let s: HashMap<String, Vec<Vec<String>>> = HashMap::new();
        let ast = simple_ast("issues", Some(eq("id", lit_str("1"))));
        let original = ast.clone();
        let mut exec = |_ast: &AST, _field: &str| ScalarValue::Absent;
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        assert_eq!(result.ast, original);
        assert_eq!(result.companions.len(), 0);
    }

    /// Branch: non-scalar correlatedSubquery with inner scalar →
    /// outer stays correlatedSubquery, inner WHERE resolves.
    #[test]
    fn resolves_scalar_inside_non_scalar_correlated() {
        let s = specs(&[("label", &[&["id"]])]);
        let inner_scalar = scalar_sub(
            CorrelatedSubqueryOp::EXISTS,
            "labelID",
            "id",
            simple_ast("label", Some(eq("id", lit_str("label-001")))),
        );
        let ast = simple_ast(
            "issue",
            Some(non_scalar_sub(
                CorrelatedSubqueryOp::EXISTS,
                "id",
                "issueID",
                simple_ast("issueLabel", Some(inner_scalar)),
            )),
        );
        let mut exec = |_ast: &AST, _field: &str| {
            ScalarValue::Value(LiteralValue::String("resolved-label-id".to_string()))
        };
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        // Outer still correlatedSubquery, but inner subquery's WHERE
        // is now a Simple equality.
        if let Condition::CorrelatedSubquery {
            related, scalar, ..
        } = *result.ast.where_clause.unwrap()
        {
            assert_eq!(scalar, None);
            let inner_where = related.subquery.where_clause.as_deref().unwrap();
            assert_eq!(
                *inner_where,
                Condition::Simple {
                    op: SimpleOperator::Eq,
                    left: col("labelID"),
                    right: lit_str("resolved-label-id"),
                }
            );
        } else {
            panic!("expected outer correlatedSubquery");
        }
        assert_eq!(result.companions.len(), 1);
        assert_eq!(result.companions[0].ast.table, "label");
    }

    /// Branch: nested scalar subqueries (inner resolves first).
    #[test]
    fn resolves_nested_scalar_subqueries() {
        let s = specs(&[("config", &[&["key"]]), ("users", &[&["id"]])]);
        let inner_scalar = scalar_sub(
            CorrelatedSubqueryOp::EXISTS,
            "role",
            "value",
            simple_ast("config", Some(eq("key", lit_str("default_role")))),
        );
        let users_sub = simple_ast(
            "users",
            Some(Condition::And {
                conditions: vec![eq("id", lit_str("0001")), inner_scalar],
            }),
        );
        let ast = simple_ast(
            "issues",
            Some(scalar_sub(
                CorrelatedSubqueryOp::EXISTS,
                "ownerId",
                "id",
                users_sub,
            )),
        );
        let mut exec = |_ast: &AST, field: &str| match field {
            "value" => ScalarValue::Value(LiteralValue::String("admin".to_string())),
            "id" => ScalarValue::Value(LiteralValue::String("0001".to_string())),
            _ => ScalarValue::Absent,
        };
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        assert_eq!(
            *result.ast.where_clause.unwrap(),
            Condition::Simple {
                op: SimpleOperator::Eq,
                left: col("ownerId"),
                right: lit_str("0001"),
            }
        );
        // Both config and users companions recorded.
        assert_eq!(result.companions.len(), 2);
    }

    /// Branch: non-scalar correlatedSubquery with no nested scalar →
    /// returns unchanged, executor not called.
    #[test]
    fn non_scalar_without_nested_scalar_unchanged() {
        let s: HashMap<String, Vec<Vec<String>>> = HashMap::new();
        let ast = simple_ast(
            "issue",
            Some(non_scalar_sub(
                CorrelatedSubqueryOp::EXISTS,
                "id",
                "issueID",
                simple_ast("issueLabel", Some(eq("labelID", lit_str("some-id")))),
            )),
        );
        let original = ast.clone();
        let mut called = false;
        let mut exec = |_ast: &AST, _field: &str| {
            called = true;
            ScalarValue::Absent
        };
        let result = resolve_simple_scalar_subqueries(ast, &s, &mut exec);
        assert_eq!(result.ast, original);
        assert_eq!(result.companions.len(), 0);
        assert!(!called);
    }
}
