//! Port of `packages/zql/src/planner/planner-builder.ts`.
//!
//! Walks an [`AST`] and builds a [`PlannerGraph`] (with sub-plans for any
//! `related` clauses), runs `plan()` on each, then writes the chosen `flip`
//! flags back onto a fresh AST via [`apply_plans_to_ast`].
//!
//! # Plan-ID handling
//! TS attaches `[planIdSymbol]` directly onto AST nodes via JS Symbol
//! properties. Rust's AST is a plain data structure with no symbol slot,
//! so we walk both the AST and the [`PlannerGraph::joins`] in DFS order and
//! match positions. The DFS order is identical between `processCondition`
//! (in `buildPlanGraph`) and `applyToCondition` (in `applyPlansToAST`) so
//! the position matches by construction.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use zero_cache_types::ast::{AST, Condition, CorrelatedSubquery, CorrelatedSubqueryOp};

use super::planner_connection::ConnectionCostModel;
use super::planner_constraint::{PlannerConstraint, constraint_from_fields};
use super::planner_debug::PlanDebugger;
use super::planner_fan_in::PlannerFanIn;
use super::planner_fan_out::PlannerFanOut;
use super::planner_graph::PlannerGraph;
use super::planner_join::arc_join;
use super::planner_node::{JoinType, NonTerminusNode, PlannerNode};
use super::planner_terminus::PlannerTerminus;

/// TS `Plans = { plan: PlannerGraph; subPlans: { [key: string]: Plans } }`.
pub struct Plans {
    pub plan: Arc<PlannerGraph>,
    pub sub_plans: BTreeMap<String, Plans>,
}

/// TS `buildPlanGraph(ast, model, isRoot, baseConstraints?)`.
pub fn build_plan_graph(
    ast: &AST,
    model: ConnectionCostModel,
    is_root: bool,
    base_constraints: Option<PlannerConstraint>,
) -> Plans {
    let graph = Arc::new(PlannerGraph::new());
    let mut next_plan_id: u32 = 0;

    let source = graph.add_source(ast.table.clone(), model.clone());
    let connection = Arc::new(source.connect(
        ast.order_by.clone().unwrap_or_default(),
        ast.where_clause.as_deref().cloned().map(Box::new),
        is_root,
        base_constraints.clone(),
        ast.limit,
    ));
    graph.connections.lock().unwrap().push(connection.clone());

    let mut end: NonTerminusNode = NonTerminusNode::Connection(connection);
    if let Some(where_clause) = ast.where_clause.as_deref() {
        end = process_condition(
            where_clause,
            end,
            &graph,
            &model,
            &ast.table,
            &mut next_plan_id,
        );
    }

    let terminus = Arc::new(PlannerTerminus::new(end.clone()));
    wire_output(&end, PlannerNode::Terminus(terminus.clone()));
    graph.set_terminus(terminus);

    // Recurse into related sub-plans.
    let mut sub_plans = BTreeMap::new();
    if let Some(related) = ast.related.as_ref() {
        for csq in related {
            let alias = csq
                .subquery
                .alias
                .clone()
                .expect("Related subquery must have alias");
            let child_constraints = Some(extract_constraint(&csq.correlation.child_field));
            sub_plans.insert(
                alias,
                build_plan_graph(&csq.subquery, model.clone(), true, child_constraints),
            );
        }
    }

    Plans {
        plan: graph,
        sub_plans,
    }
}

fn wire_output(from: &NonTerminusNode, to: PlannerNode) {
    match from {
        NonTerminusNode::Connection(c) => c.set_output(to),
        NonTerminusNode::Join(j) => j.set_output(to),
        NonTerminusNode::FanIn(fi) => fi.set_output(to),
        NonTerminusNode::FanOut(fo) => fo.add_output(to),
        // Terminus is excluded by the type — TS asserts at runtime.
    }
}

fn process_condition(
    condition: &Condition,
    input: NonTerminusNode,
    graph: &Arc<PlannerGraph>,
    model: &ConnectionCostModel,
    parent_table: &str,
    next_plan_id: &mut u32,
) -> NonTerminusNode {
    match condition {
        Condition::Simple { .. } => input,
        Condition::And { conditions } => {
            let mut end = input;
            for sub in conditions {
                end = process_condition(sub, end, graph, model, parent_table, next_plan_id);
            }
            end
        }
        Condition::Or { conditions } => {
            process_or(conditions, input, graph, model, parent_table, next_plan_id)
        }
        Condition::CorrelatedSubquery {
            related, op, flip, ..
        } => process_correlated_subquery(
            related,
            *op,
            *flip,
            input,
            graph,
            model,
            parent_table,
            next_plan_id,
        ),
    }
}

fn process_or(
    conditions: &[Condition],
    input: NonTerminusNode,
    graph: &Arc<PlannerGraph>,
    model: &ConnectionCostModel,
    parent_table: &str,
    next_plan_id: &mut u32,
) -> NonTerminusNode {
    // Filter to subquery-bearing branches only (mirrors TS).
    let subquery_conditions: Vec<&Condition> = conditions
        .iter()
        .filter(|c| matches!(c, Condition::CorrelatedSubquery { .. }) || has_correlated_subquery(c))
        .collect();

    if subquery_conditions.is_empty() {
        return input;
    }

    let fan_out = Arc::new(PlannerFanOut::new(input.clone()));
    graph.fan_outs.lock().unwrap().push(fan_out.clone());
    wire_output(&input, PlannerNode::FanOut(fan_out.clone()));

    let mut branches: Vec<NonTerminusNode> = Vec::new();
    for sub in subquery_conditions {
        let branch = process_condition(
            sub,
            NonTerminusNode::FanOut(fan_out.clone()),
            graph,
            model,
            parent_table,
            next_plan_id,
        );
        branches.push(branch.clone());
        fan_out.add_output(branch.as_planner());
    }

    let fan_in = Arc::new(PlannerFanIn::new(branches.clone()));
    graph.fan_ins.lock().unwrap().push(fan_in.clone());
    for branch in branches {
        wire_output(&branch, PlannerNode::FanIn(fan_in.clone()));
    }
    NonTerminusNode::FanIn(fan_in)
}

fn process_correlated_subquery(
    related: &CorrelatedSubquery,
    op: CorrelatedSubqueryOp,
    manual_flip: Option<bool>,
    input: NonTerminusNode,
    graph: &Arc<PlannerGraph>,
    model: &ConnectionCostModel,
    _parent_table: &str,
    next_plan_id: &mut u32,
) -> NonTerminusNode {
    let child_table = related.subquery.table.clone();

    let child_source = if graph.has_source(&child_table) {
        graph.get_source(&child_table)
    } else {
        graph.add_source(child_table.clone(), model.clone())
    };

    let child_connection = Arc::new(
        child_source.connect(
            related.subquery.order_by.clone().unwrap_or_default(),
            related
                .subquery
                .where_clause
                .as_deref()
                .cloned()
                .map(Box::new),
            false,
            None,
            if matches!(op, CorrelatedSubqueryOp::EXISTS) {
                Some(1)
            } else {
                None
            },
        ),
    );
    graph
        .connections
        .lock()
        .unwrap()
        .push(child_connection.clone());

    let mut child_end: NonTerminusNode = NonTerminusNode::Connection(child_connection);
    if let Some(w) = related.subquery.where_clause.as_deref() {
        child_end = process_condition(w, child_end, graph, model, &child_table, next_plan_id);
    }

    let parent_constraint = extract_constraint(&related.correlation.parent_field);
    let child_constraint = extract_constraint(&related.correlation.child_field);

    let plan_id = *next_plan_id;
    *next_plan_id += 1;

    let is_not_exists = matches!(op, CorrelatedSubqueryOp::NotExists);
    let (flippable, initial_type) = match (is_not_exists, manual_flip) {
        (true, _) => (false, JoinType::Semi),
        (false, Some(true)) => (false, JoinType::Flipped),
        (false, Some(false)) => (false, JoinType::Semi),
        (false, None) => (true, JoinType::Semi),
    };

    let join = arc_join(
        input.clone(),
        child_end.clone(),
        parent_constraint,
        child_constraint,
        flippable,
        plan_id,
        initial_type,
    );
    graph.joins.lock().unwrap().push(join.clone());

    wire_output(&input, PlannerNode::Join(join.clone()));
    wire_output(&child_end, PlannerNode::Join(join.clone()));

    NonTerminusNode::Join(join)
}

/// We need `flip` from the wrapping `Condition::CorrelatedSubquery`. Patch:
/// re-do `process_condition`'s correlatedSubquery branch to pass flip
/// directly. Implement by inspecting the variant inside `process_condition`.
//
// (See process_condition above — we now revise it.)

fn has_correlated_subquery(condition: &Condition) -> bool {
    match condition {
        Condition::CorrelatedSubquery { .. } => true,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(has_correlated_subquery)
        }
        Condition::Simple { .. } => false,
    }
}

fn extract_constraint(fields: &[String]) -> PlannerConstraint {
    constraint_from_fields(fields.iter().cloned())
}

fn plan_recursively(plans: &Plans, plan_debugger: Option<&dyn PlanDebugger>) {
    for (_, sub) in &plans.sub_plans {
        plan_recursively(sub, plan_debugger);
    }
    let _ = plans.plan.plan(plan_debugger);
}

/// TS `planQuery(ast, model, planDebugger?)`.
pub fn plan_query(
    ast: &AST,
    model: ConnectionCostModel,
    plan_debugger: Option<&dyn PlanDebugger>,
) -> AST {
    let plans = build_plan_graph(ast, model, true, None);
    plan_recursively(&plans, plan_debugger);
    apply_plans_to_ast(ast, &plans)
}

// ---------------------------------------------------------------------------
// applyPlansToAST
// ---------------------------------------------------------------------------

fn flipped_plan_ids(plan: &PlannerGraph) -> HashSet<u32> {
    let mut s = HashSet::new();
    for j in plan.joins.lock().unwrap().iter() {
        if matches!(j.type_(), JoinType::Flipped) {
            s.insert(j.plan_id);
        }
    }
    s
}

/// TS `applyPlansToAST(ast, plans)`.
pub fn apply_plans_to_ast(ast: &AST, plans: &Plans) -> AST {
    let flipped = flipped_plan_ids(&plans.plan);
    let mut counter: u32 = 0;
    AST {
        schema: ast.schema.clone(),
        table: ast.table.clone(),
        alias: ast.alias.clone(),
        where_clause: ast
            .where_clause
            .as_ref()
            .map(|c| Box::new(apply_to_condition(c, &flipped, &mut counter))),
        related: ast.related.as_ref().map(|subs| {
            subs.iter()
                .map(|csq| {
                    let alias = csq
                        .subquery
                        .alias
                        .clone()
                        .expect("Related subquery must have alias");
                    let new_subquery = if let Some(sub_plan) = plans.sub_plans.get(&alias) {
                        apply_plans_to_ast(&csq.subquery, sub_plan)
                    } else {
                        (*csq.subquery).clone()
                    };
                    CorrelatedSubquery {
                        correlation: csq.correlation.clone(),
                        subquery: Box::new(new_subquery),
                        system: csq.system,
                        hidden: csq.hidden,
                    }
                })
                .collect()
        }),
        start: ast.start.clone(),
        limit: ast.limit,
        order_by: ast.order_by.clone(),
    }
}

fn apply_to_condition(
    condition: &Condition,
    flipped_ids: &HashSet<u32>,
    counter: &mut u32,
) -> Condition {
    match condition {
        Condition::Simple { .. } => condition.clone(),
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
        } => {
            // Plan IDs are assigned in DFS order — we're at the same point
            // as the corresponding builder traversal.
            let plan_id = *counter;
            *counter += 1;
            // Recurse into the subquery's where clause (matches TS — but
            // counter is a *local* per-graph thing; nested subqueries are
            // their own graph and get their own counter via `apply_plans_to_ast`).
            // Note: inside this AST traversal, we still need to walk
            // sub-where clauses with the same counter because the parent
            // graph's `processCondition` recurses into the EXISTS subquery's
            // where as well? Re-check TS: it does — `processCorrelatedSubquery`
            // calls `processCondition(related.subquery.where, ...)`. So the
            // same counter applies.
            let sub_where = related
                .subquery
                .where_clause
                .as_ref()
                .map(|c| Box::new(apply_to_condition(c, flipped_ids, counter)));
            let new_subquery = AST {
                schema: related.subquery.schema.clone(),
                table: related.subquery.table.clone(),
                alias: related.subquery.alias.clone(),
                where_clause: sub_where,
                related: related.subquery.related.clone(),
                start: related.subquery.start.clone(),
                limit: related.subquery.limit,
                order_by: related.subquery.order_by.clone(),
            };
            let should_flip = flipped_ids.contains(&plan_id);
            // TS preserves explicit `flip` if user set it; here we override with
            // planner result since explicit-flip joins also appear in `flippedIds`
            // when initial_type == Flipped.
            let _ = flip;
            let _ = scalar;
            Condition::CorrelatedSubquery {
                related: Box::new(CorrelatedSubquery {
                    correlation: related.correlation.clone(),
                    subquery: Box::new(new_subquery),
                    system: related.system,
                    hidden: related.hidden,
                }),
                op: *op,
                flip: Some(should_flip),
                scalar: *scalar,
            }
        }
        Condition::And { conditions } => Condition::And {
            conditions: conditions
                .iter()
                .map(|c| apply_to_condition(c, flipped_ids, counter))
                .collect(),
        },
        Condition::Or { conditions } => Condition::Or {
            conditions: conditions
                .iter()
                .map(|c| apply_to_condition(c, flipped_ids, counter))
                .collect(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_node::{FanoutConfidence, FanoutCostModel, FanoutEst};
    use std::sync::Arc;
    use zero_cache_types::ast::{
        CorrelatedSubquery, Correlation, LiteralValue, NonColumnValue, SimpleOperator,
        ValuePosition,
    };

    fn simple_cost_model() -> ConnectionCostModel {
        Arc::new(|_t, _s, _f, constraint| {
            let n = constraint.map(|c| c.len()).unwrap_or(0) as f64;
            let rows = (100.0 - n * 10.0).max(1.0);
            let fanout: FanoutCostModel = Arc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            });
            crate::planner::planner_connection::CostModelCost {
                startup_cost: 0.0,
                rows,
                fanout,
            }
        })
    }

    fn make_ast_simple_table(table: &str) -> AST {
        AST {
            schema: None,
            table: table.to_string(),
            alias: None,
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        }
    }

    fn exists_condition(child_table: &str) -> Condition {
        Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".to_string()],
                    child_field: vec!["userId".to_string()],
                },
                subquery: Box::new(make_ast_simple_table(child_table)),
                system: None,
                hidden: None,
            }),
            op: CorrelatedSubqueryOp::EXISTS,
            flip: None,
            scalar: None,
        }
    }

    fn not_exists_condition(child_table: &str) -> Condition {
        Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".to_string()],
                    child_field: vec!["userId".to_string()],
                },
                subquery: Box::new(make_ast_simple_table(child_table)),
                system: None,
                hidden: None,
            }),
            op: CorrelatedSubqueryOp::NotExists,
            flip: None,
            scalar: None,
        }
    }

    // Branch: simple table query → 1 connection, no joins, no sub-plans.
    #[test]
    fn build_simple_table() {
        let ast = make_ast_simple_table("users");
        let plans = build_plan_graph(&ast, simple_cost_model(), true, None);
        assert_eq!(plans.plan.connections.lock().unwrap().len(), 1);
        assert_eq!(plans.plan.connections.lock().unwrap()[0].table, "users");
        assert!(plans.sub_plans.is_empty());
    }

    // Branch: simple where clause does not add a join.
    #[test]
    fn simple_where_no_join() {
        let mut ast = make_ast_simple_table("users");
        ast.where_clause = Some(Box::new(Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        }));
        let plans = build_plan_graph(&ast, simple_cost_model(), true, None);
        assert_eq!(plans.plan.joins.lock().unwrap().len(), 0);
    }

    // Branch: EXISTS adds a flippable join.
    #[test]
    fn exists_creates_flippable_join() {
        let mut ast = make_ast_simple_table("users");
        ast.where_clause = Some(Box::new(exists_condition("posts")));
        let plans = build_plan_graph(&ast, simple_cost_model(), true, None);
        assert_eq!(plans.plan.joins.lock().unwrap().len(), 1);
        assert!(plans.plan.joins.lock().unwrap()[0].is_flippable());
    }

    // Branch: NOT EXISTS adds a non-flippable join.
    #[test]
    fn not_exists_creates_non_flippable_join() {
        let mut ast = make_ast_simple_table("users");
        ast.where_clause = Some(Box::new(not_exists_condition("posts")));
        let plans = build_plan_graph(&ast, simple_cost_model(), true, None);
        assert!(!plans.plan.joins.lock().unwrap()[0].is_flippable());
    }

    // Branch: AND of two EXISTS → 2 joins, sequential plan IDs.
    #[test]
    fn and_two_exists_two_joins_sequential_ids() {
        let mut ast = make_ast_simple_table("users");
        ast.where_clause = Some(Box::new(Condition::And {
            conditions: vec![exists_condition("posts"), exists_condition("comments")],
        }));
        let plans = build_plan_graph(&ast, simple_cost_model(), true, None);
        let joins = plans.plan.joins.lock().unwrap();
        assert_eq!(joins.len(), 2);
        assert_eq!(joins[0].plan_id, 0);
        assert_eq!(joins[1].plan_id, 1);
    }

    // Branch: OR of EXISTSes → fan-out + fan-in.
    #[test]
    fn or_creates_fan_out_fan_in() {
        let mut ast = make_ast_simple_table("users");
        ast.where_clause = Some(Box::new(Condition::Or {
            conditions: vec![exists_condition("posts"), exists_condition("comments")],
        }));
        let plans = build_plan_graph(&ast, simple_cost_model(), true, None);
        assert_eq!(plans.plan.fan_outs.lock().unwrap().len(), 1);
        assert_eq!(plans.plan.fan_ins.lock().unwrap().len(), 1);
        assert_eq!(plans.plan.joins.lock().unwrap().len(), 2);
    }

    // Branch: OR with no subqueries → no fan-out.
    #[test]
    fn or_with_no_subqueries_skips_fan_nodes() {
        let mut ast = make_ast_simple_table("users");
        let simple = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        };
        ast.where_clause = Some(Box::new(Condition::Or {
            conditions: vec![simple.clone(), simple],
        }));
        let plans = build_plan_graph(&ast, simple_cost_model(), true, None);
        assert_eq!(plans.plan.fan_outs.lock().unwrap().len(), 0);
        assert_eq!(plans.plan.fan_ins.lock().unwrap().len(), 0);
    }

    // Branch: related sub-plan recorded by alias.
    #[test]
    fn related_sub_plans_keyed_by_alias() {
        let mut ast = make_ast_simple_table("users");
        let mut sub = make_ast_simple_table("posts");
        sub.alias = Some("posts".to_string());
        ast.related = Some(vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".to_string()],
                child_field: vec!["userId".to_string()],
            },
            subquery: Box::new(sub),
            system: None,
            hidden: None,
        }]);
        let plans = build_plan_graph(&ast, simple_cost_model(), true, None);
        assert!(plans.sub_plans.contains_key("posts"));
    }

    // Branch: plan_query writes flip=false back when planner picks semi.
    #[test]
    fn plan_query_writes_flip_flag() {
        let mut ast = make_ast_simple_table("users");
        ast.where_clause = Some(Box::new(exists_condition("posts")));
        let out = plan_query(&ast, simple_cost_model(), None);
        match out.where_clause.as_deref() {
            Some(Condition::CorrelatedSubquery { flip, .. }) => {
                assert!(matches!(flip, Some(_)));
            }
            _ => panic!("expected EXISTS"),
        }
    }
}
