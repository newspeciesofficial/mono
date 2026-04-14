//! Port of `packages/zql/src/query/complete-ordering.ts`.
//!
//! Public exports:
//! - [`complete_ordering`] — walk an AST and append any missing
//!   primary-key columns to `orderBy`, including recursively into
//!   `related` subqueries and `EXISTS` / `NOT EXISTS` subqueries inside
//!   `where` clauses.
//! - [`assert_ordering_includes_pk`] — debug assertion helper used by
//!   the planner's test surface; returns `Err` instead of panicking
//!   (TS `assert` throws; we use `Result` for callers that want to
//!   recover — there is a panic-based `check_ordering_includes_pk`
//!   variant as well).

use std::collections::HashSet;

use zero_cache_types::ast::{AST, Condition, CorrelatedSubquery, Direction, Ordering};
use zero_cache_types::primary_key::PrimaryKey;

/// Caller-supplied function mapping a table name to its primary key.
/// TS uses `(tableName: string) => PrimaryKey` — we accept a closure.
pub type GetPrimaryKey<'a> = &'a dyn Fn(&str) -> PrimaryKey;

/// TS `completeOrdering(ast, getPrimaryKey)` — returns a new AST whose
/// `orderBy` (and every nested subquery's `orderBy`) is extended to
/// include all primary-key columns that are not already present.
pub fn complete_ordering(ast: &AST, get_primary_key: GetPrimaryKey<'_>) -> AST {
    let pk = get_primary_key(&ast.table);

    let related = ast.related.as_ref().map(|subs| {
        subs.iter()
            .map(|sq| CorrelatedSubquery {
                correlation: sq.correlation.clone(),
                subquery: Box::new(complete_ordering(&sq.subquery, get_primary_key)),
                system: sq.system,
                hidden: sq.hidden,
            })
            .collect::<Vec<_>>()
    });

    let where_clause = ast
        .where_clause
        .as_ref()
        .map(|c| Box::new(complete_ordering_in_condition(c, get_primary_key)));

    AST {
        schema: ast.schema.clone(),
        table: ast.table.clone(),
        alias: ast.alias.clone(),
        where_clause,
        related,
        start: ast.start.clone(),
        limit: ast.limit,
        order_by: Some(add_primary_keys(&pk, ast.order_by.as_deref())),
    }
}

/// TS `assertOrderingIncludesPK(ordering, pk)` — panics if any primary
/// key column is missing from `ordering` (matches TS `assert`).
pub fn assert_ordering_includes_pk(ordering: &Ordering, pk: &PrimaryKey) {
    let ordering_fields: HashSet<&str> = ordering.iter().map(|(field, _)| field.as_str()).collect();

    let missing: Vec<&String> = pk
        .columns()
        .iter()
        .filter(|pk_field| !ordering_fields.contains(pk_field.as_str()))
        .collect();

    if !missing.is_empty() {
        let names: Vec<&str> = missing.iter().map(|s| s.as_str()).collect();
        panic!(
            "Ordering must include all primary key fields. Missing: {}.",
            names.join(", ")
        );
    }
}

fn complete_ordering_in_condition(
    condition: &Condition,
    get_primary_key: GetPrimaryKey<'_>,
) -> Condition {
    match condition {
        // Branch: simple — no subquery to recurse into.
        Condition::Simple { .. } => condition.clone(),
        // Branch: correlatedSubquery — recurse into nested subquery.
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
        } => Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: related.correlation.clone(),
                subquery: Box::new(complete_ordering(&related.subquery, get_primary_key)),
                system: related.system,
                hidden: related.hidden,
            }),
            op: *op,
            flip: *flip,
            scalar: *scalar,
        },
        // Branch: `and`.
        Condition::And { conditions } => Condition::And {
            conditions: conditions
                .iter()
                .map(|c| complete_ordering_in_condition(c, get_primary_key))
                .collect(),
        },
        // Branch: `or`.
        Condition::Or { conditions } => Condition::Or {
            conditions: conditions
                .iter()
                .map(|c| complete_ordering_in_condition(c, get_primary_key))
                .collect(),
        },
    }
}

fn add_primary_keys(
    primary_key: &PrimaryKey,
    order_by: Option<&[(String, Direction)]>,
) -> Ordering {
    let existing: Vec<(String, Direction)> = order_by.map(|o| o.to_vec()).unwrap_or_default();

    // Primary-key columns that still need to be appended.
    let mut to_add: Vec<&String> = Vec::new();
    let seen: HashSet<&str> = existing.iter().map(|(f, _)| f.as_str()).collect();
    for pk_field in primary_key.columns() {
        if !seen.contains(pk_field.as_str()) && !to_add.iter().any(|s| *s == pk_field) {
            to_add.push(pk_field);
        }
    }

    // Branch: nothing to add → return input unchanged (or `[]` if none).
    if to_add.is_empty() {
        return existing;
    }

    let mut out = existing;
    for pk_field in to_add {
        out.push((pk_field.clone(), Direction::Asc));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_types::ast::{
        Condition, CorrelatedSubquery, CorrelatedSubqueryOp, Correlation, LiteralValue,
        NonColumnValue, SimpleOperator, ValuePosition,
    };

    fn pk(cols: &[&str]) -> PrimaryKey {
        PrimaryKey::new(cols.iter().map(|s| s.to_string()).collect())
    }

    fn make_ast(table: &str, order_by: Option<Ordering>) -> AST {
        AST {
            schema: None,
            table: table.to_string(),
            alias: None,
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by,
        }
    }

    fn lookup(name: &str) -> PrimaryKey {
        match name {
            "issue" => pk(&["id"]),
            "issueLabel" => pk(&["issueId", "labelId"]),
            "label" => pk(&["id"]),
            "comment" => pk(&["id"]),
            "user" => pk(&["id"]),
            _ => panic!("unknown table {name}"),
        }
    }

    // Branch: AST with no orderBy — pk appended from scratch.
    #[test]
    fn basic_no_order_by() {
        let ast = make_ast("issue", None);
        let out = complete_ordering(&ast, &lookup);
        assert_eq!(out.order_by, Some(vec![("id".to_string(), Direction::Asc)]));
    }

    // Branch: AST with non-pk orderBy — pk appended at the end.
    #[test]
    fn non_pk_order_by_appends_pk() {
        let ast = make_ast("issue", Some(vec![("title".to_string(), Direction::Asc)]));
        let out = complete_ordering(&ast, &lookup);
        assert_eq!(
            out.order_by,
            Some(vec![
                ("title".to_string(), Direction::Asc),
                ("id".to_string(), Direction::Asc),
            ])
        );
    }

    // Branch: orderBy includes part of composite pk — only missing part added.
    #[test]
    fn partial_order_adds_missing_pk_columns() {
        let ast = make_ast(
            "issueLabel",
            Some(vec![("labelId".to_string(), Direction::Asc)]),
        );
        let out = complete_ordering(&ast, &lookup);
        assert_eq!(
            out.order_by,
            Some(vec![
                ("labelId".to_string(), Direction::Asc),
                ("issueId".to_string(), Direction::Asc),
            ])
        );
    }

    // Branch: orderBy already contains all pk columns → unchanged.
    #[test]
    fn order_by_already_includes_pk_untouched() {
        let ast = make_ast("issue", Some(vec![("id".to_string(), Direction::Desc)]));
        let out = complete_ordering(&ast, &lookup);
        assert_eq!(
            out.order_by,
            Some(vec![("id".to_string(), Direction::Desc)])
        );
    }

    // Branch: nested related subqueries recursed into.
    #[test]
    fn related_subqueries_get_complete_ordering() {
        let inner = make_ast("label", None);
        let mut outer = make_ast("issue", None);
        outer.related = Some(vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".to_string()],
                child_field: vec!["issueId".to_string()],
            },
            subquery: Box::new(inner),
            system: None,
            hidden: None,
        }]);

        let out = complete_ordering(&outer, &lookup);
        let nested = &out.related.unwrap()[0];
        assert_eq!(
            nested.subquery.order_by,
            Some(vec![("id".to_string(), Direction::Asc)])
        );
    }

    // Branch: where clause with correlatedSubquery recursed into.
    #[test]
    fn exists_in_where_recurses() {
        let inner = make_ast("label", None);
        let mut outer = make_ast("issue", None);
        outer.where_clause = Some(Box::new(Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".to_string()],
                    child_field: vec!["issueId".to_string()],
                },
                subquery: Box::new(inner),
                system: None,
                hidden: None,
            }),
            op: CorrelatedSubqueryOp::EXISTS,
            flip: None,
            scalar: None,
        }));
        let out = complete_ordering(&outer, &lookup);
        match out.where_clause.as_deref().unwrap() {
            Condition::CorrelatedSubquery { related, .. } => {
                assert_eq!(
                    related.subquery.order_by,
                    Some(vec![("id".to_string(), Direction::Asc)])
                );
            }
            _ => panic!("expected correlatedSubquery"),
        }
    }

    // Branch: where with `and` / `or` recurses.
    #[test]
    fn and_or_nested_recurses() {
        let inner = make_ast("label", None);
        let simple = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        };
        let exists = Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".to_string()],
                    child_field: vec!["issueId".to_string()],
                },
                subquery: Box::new(inner),
                system: None,
                hidden: None,
            }),
            op: CorrelatedSubqueryOp::EXISTS,
            flip: None,
            scalar: None,
        };
        let or = Condition::Or {
            conditions: vec![simple.clone(), exists.clone()],
        };
        let and = Condition::And {
            conditions: vec![simple, or],
        };
        let mut outer = make_ast("issue", None);
        outer.where_clause = Some(Box::new(and));

        let out = complete_ordering(&outer, &lookup);
        let Condition::And { conditions } = out.where_clause.as_deref().unwrap() else {
            panic!("expected and");
        };
        let Condition::Or { conditions: inner } = &conditions[1] else {
            panic!("expected or");
        };
        let Condition::CorrelatedSubquery { related, .. } = &inner[1] else {
            panic!("expected exists");
        };
        assert_eq!(
            related.subquery.order_by,
            Some(vec![("id".to_string(), Direction::Asc)])
        );
    }

    // Branch: assert_ordering_includes_pk — all present → no panic.
    #[test]
    fn assert_ordering_ok_when_all_present() {
        assert_ordering_includes_pk(
            &vec![
                ("issueId".to_string(), Direction::Asc),
                ("labelId".to_string(), Direction::Asc),
            ],
            &pk(&["issueId", "labelId"]),
        );
    }

    // Branch: assert_ordering_includes_pk — missing → panics.
    #[test]
    #[should_panic(expected = "Missing: labelId")]
    fn assert_ordering_panics_on_missing() {
        assert_ordering_includes_pk(
            &vec![("issueId".to_string(), Direction::Asc)],
            &pk(&["issueId", "labelId"]),
        );
    }
}
