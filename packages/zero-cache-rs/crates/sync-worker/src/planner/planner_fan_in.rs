//! Port of `packages/zql/src/planner/planner-fan-in.ts`.
//!
//! [`PlannerFanIn`] joins the branches of a fan-out back together. Its type
//! is either `FI` (single shared scan; cost = max across branches; selectivity
//! = 1 - Π(1 - sᵢ)) or `UFI` (one scan per branch; cost = sum across
//! branches; same OR selectivity formula).

use std::sync::{Arc, Mutex};

use super::planner_constraint::PlannerConstraint;
use super::planner_debug::{PlanDebugEvent, PlanDebugger};
use super::planner_node::{
    CostEstimate, FanInType, FanoutCostModel, FanoutEst, FromInfo, JoinOrConnection, NodeKind,
    NonTerminusNode, PlannerNode, omit_fanout,
};

pub struct PlannerFanIn {
    type_: Mutex<FanInType>,
    output: Mutex<Option<PlannerNode>>,
    inputs: Vec<NonTerminusNode>,
}

impl PlannerFanIn {
    pub fn new(inputs: Vec<NonTerminusNode>) -> Self {
        Self {
            type_: Mutex::new(FanInType::FI),
            output: Mutex::new(None),
            inputs,
        }
    }

    pub fn type_(&self) -> FanInType {
        *self.type_.lock().expect("fan-in type mutex poisoned")
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        // TS hard-codes 'join' here.
        JoinOrConnection::Join
    }

    pub fn set_output(&self, node: PlannerNode) {
        *self.output.lock().expect("fan-in output mutex poisoned") = Some(node);
    }

    /// TS `get output(): PlannerNode { assert(...); }`.
    pub fn output(&self) -> PlannerNode {
        self.output
            .lock()
            .expect("fan-in output mutex poisoned")
            .clone()
            .expect("Output not set")
    }

    pub fn reset(&self) {
        *self.type_.lock().expect("fan-in type mutex poisoned") = FanInType::FI;
    }

    pub fn convert_to_ufi(&self) {
        *self.type_.lock().expect("fan-in type mutex poisoned") = FanInType::UFI;
    }

    /// TS `propagateUnlimitFromFlippedJoin()` — propagates to all inputs.
    pub fn propagate_unlimit_from_flipped_join(&self) {
        for input in &self.inputs {
            input.as_planner().propagate_unlimit_from_flipped_join();
        }
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
        plan_debugger: Option<&dyn PlanDebugger>,
    ) -> CostEstimate {
        // Initial total — placeholder fanout panics if no input ever runs.
        let throw_fanout: FanoutCostModel =
            Arc::new(|_cols: &[String]| -> FanoutEst { panic!("Failed to set fanout model") });
        let mut total = CostEstimate {
            startup_cost: 0.0,
            scan_est: 0.0,
            cost: 0.0,
            returned_rows: 0.0,
            selectivity: 0.0,
            limit: None,
            fanout: throw_fanout,
        };

        let kind = *self.type_.lock().expect("fan-in type mutex poisoned");

        if matches!(kind, FanInType::FI) {
            // Branch: FI — all inputs share the same branch_pattern with 0 prepended.
            let mut updated = Vec::with_capacity(branch_pattern.len() + 1);
            updated.push(0_usize);
            updated.extend_from_slice(branch_pattern);

            let mut maxrows: f64 = 0.0;
            let mut max_running: f64 = 0.0;
            let mut max_startup: f64 = 0.0;
            let mut max_scan: f64 = 0.0;
            let mut no_match: f64 = 1.0;
            let mut limit_set = false;

            for input in &self.inputs {
                let cost = input.as_planner().estimate_cost(
                    downstream_child_selectivity,
                    &updated,
                    plan_debugger,
                );
                total.fanout = cost.fanout.clone();
                if cost.returned_rows > maxrows {
                    maxrows = cost.returned_rows;
                }
                if cost.cost > max_running {
                    max_running = cost.cost;
                }
                if cost.startup_cost > max_startup {
                    max_startup = cost.startup_cost;
                }
                if cost.scan_est > max_scan {
                    max_scan = cost.scan_est;
                }
                no_match *= 1.0 - cost.selectivity;

                // assert(totalCost.limit === undefined || cost.limit === totalCost.limit, ...);
                if limit_set {
                    assert!(
                        total.limit == cost.limit,
                        "All FanIn inputs should have the same limit"
                    );
                }
                total.limit = cost.limit;
                limit_set = true;
            }

            total.returned_rows = maxrows;
            total.cost = max_running;
            total.selectivity = 1.0 - no_match;
            total.startup_cost = max_startup;
            total.scan_est = max_scan;
        } else {
            // Branch: UFI — each input gets a unique branch pattern; costs sum.
            let mut no_match: f64 = 1.0;
            let mut limit_set = false;
            for (i, input) in self.inputs.iter().enumerate() {
                let mut updated = Vec::with_capacity(branch_pattern.len() + 1);
                updated.push(i);
                updated.extend_from_slice(branch_pattern);

                let cost = input.as_planner().estimate_cost(
                    downstream_child_selectivity,
                    &updated,
                    plan_debugger,
                );
                total.fanout = cost.fanout.clone();
                total.returned_rows += cost.returned_rows;
                total.cost += cost.cost;
                total.scan_est += cost.scan_est;
                total.startup_cost += cost.startup_cost;
                no_match *= 1.0 - cost.selectivity;

                if limit_set {
                    assert!(
                        total.limit == cost.limit,
                        "All FanIn inputs should have the same limit"
                    );
                }
                total.limit = cost.limit;
                limit_set = true;
            }
            total.selectivity = 1.0 - no_match;
        }

        if let Some(d) = plan_debugger {
            d.log(PlanDebugEvent::NodeCost {
                attempt_number: None,
                node_type: "fan-in",
                node: match kind {
                    FanInType::FI => "FI".to_string(),
                    FanInType::UFI => "UFI".to_string(),
                },
                branch_pattern: branch_pattern.to_vec(),
                downstream_child_selectivity,
                cost_estimate: omit_fanout(&total),
                filters: None,
                ordering: None,
                join_type: None,
            });
        }

        total
    }

    pub fn propagate_constraints(
        &self,
        branch_pattern: &[usize],
        constraint: Option<&PlannerConstraint>,
        from: Option<FromInfo>,
        plan_debugger: Option<&dyn PlanDebugger>,
    ) {
        let kind = *self.type_.lock().expect("fan-in type mutex poisoned");

        if let Some(d) = plan_debugger {
            d.log(PlanDebugEvent::NodeConstraint {
                attempt_number: None,
                node_type: "fan-in",
                node: match kind {
                    FanInType::FI => "FI".to_string(),
                    FanInType::UFI => "UFI".to_string(),
                },
                branch_pattern: branch_pattern.to_vec(),
                constraint: constraint.cloned(),
                from: from
                    .as_ref()
                    .map(|f| f.kind.as_str().to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
            });
        }

        let me = FromInfo {
            kind: NodeKind::FanIn,
            name: match kind {
                FanInType::FI => "FI".to_string(),
                FanInType::UFI => "UFI".to_string(),
            },
        };

        if matches!(kind, FanInType::FI) {
            // Branch: FI — all inputs see the same `[0, ...]` branch pattern.
            let mut updated = Vec::with_capacity(branch_pattern.len() + 1);
            updated.push(0);
            updated.extend_from_slice(branch_pattern);
            for input in &self.inputs {
                input.as_planner().propagate_constraints(
                    &updated,
                    constraint,
                    Some(me.clone()),
                    plan_debugger,
                );
            }
            return;
        }

        // Branch: UFI — each input gets `[i, ...]`.
        for (i, input) in self.inputs.iter().enumerate() {
            let mut updated = Vec::with_capacity(branch_pattern.len() + 1);
            updated.push(i);
            updated.extend_from_slice(branch_pattern);
            input.as_planner().propagate_constraints(
                &updated,
                constraint,
                Some(me.clone()),
                plan_debugger,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_connection::{ConnectionCostModel, PlannerConnection};
    use crate::planner::planner_constraint::constraint_from_fields;
    use crate::planner::planner_node::{FanoutConfidence, FanoutEst};
    use crate::planner::planner_source::PlannerSource;
    use std::sync::Arc;
    use zero_cache_types::ast::{
        Condition, LiteralValue, NonColumnValue, SimpleOperator, ValuePosition,
    };

    fn simple_cost_model() -> ConnectionCostModel {
        Arc::new(|_table, _sort, _filters, constraint| {
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

    fn make_connection(table: &str) -> Arc<PlannerConnection> {
        let src = PlannerSource::new(table, simple_cost_model());
        Arc::new(src.connect(vec![], None, false, None, None))
    }

    fn make_fan_in(n: usize) -> (Vec<Arc<PlannerConnection>>, PlannerFanIn) {
        let conns: Vec<Arc<PlannerConnection>> = (0..n)
            .map(|i| make_connection(&format!("table{i}")))
            .collect();
        let inputs = conns
            .iter()
            .cloned()
            .map(NonTerminusNode::Connection)
            .collect();
        (conns, PlannerFanIn::new(inputs))
    }

    // Branch: initial state is FI.
    #[test]
    fn initial_state_fi() {
        let (_, fi) = make_fan_in(2);
        assert_eq!(fi.type_(), FanInType::FI);
    }

    // Branch: convert_to_ufi flips type.
    #[test]
    fn convert_to_ufi() {
        let (_, fi) = make_fan_in(2);
        fi.convert_to_ufi();
        assert_eq!(fi.type_(), FanInType::UFI);
    }

    // Branch: reset restores FI.
    #[test]
    fn reset_restores_fi() {
        let (_, fi) = make_fan_in(2);
        fi.convert_to_ufi();
        fi.reset();
        assert_eq!(fi.type_(), FanInType::FI);
    }

    // Branch: propagate_constraints with FI sends [0, ...] to every input.
    #[test]
    fn propagate_constraints_fi_sends_same_pattern() {
        let (conns, fi) = make_fan_in(2);
        let cstr = constraint_from_fields(["userId"]);
        fi.propagate_constraints(&[], Some(&cstr), None, None);
        for c in &conns {
            // Constraint stored under key "0".
            let est = c.estimate_cost(1.0, &[0], None);
            assert_eq!(est.returned_rows, 90.0);
        }
    }

    // Branch: propagate_constraints with UFI sends unique patterns.
    #[test]
    fn propagate_constraints_ufi_sends_unique_patterns() {
        let (conns, fi) = make_fan_in(3);
        fi.convert_to_ufi();
        let cstr = constraint_from_fields(["userId"]);
        fi.propagate_constraints(&[], Some(&cstr), None, None);
        for (i, c) in conns.iter().enumerate() {
            let est = c.estimate_cost(1.0, &[i], None);
            assert_eq!(est.returned_rows, 90.0);
        }
    }

    // Branch: set_output / output.
    #[test]
    fn output_set_and_get() {
        let (_, fi) = make_fan_in(2);
        let c = make_connection("comments");
        fi.set_output(PlannerNode::Connection(c.clone()));
        match fi.output() {
            PlannerNode::Connection(got) => assert!(Arc::ptr_eq(&got, &c)),
            _ => panic!("expected Connection"),
        }
    }

    // Branch: output() panics when not set.
    #[test]
    #[should_panic(expected = "Output not set")]
    fn output_panics_when_not_set() {
        let (_, fi) = make_fan_in(2);
        let _ = fi.output();
    }

    // Branch: closest_join_or_source returns Join.
    #[test]
    fn closest_returns_join() {
        let (_, fi) = make_fan_in(2);
        assert_eq!(fi.closest_join_or_source(), JoinOrConnection::Join);
    }

    // Branch: propagate_unlimit_from_flipped_join propagates to every input.
    #[test]
    fn unlimit_propagates_to_inputs() {
        let src1 = PlannerSource::new("a", simple_cost_model());
        let c1 = Arc::new(src1.connect(vec![], None, false, None, Some(5)));
        let src2 = PlannerSource::new("b", simple_cost_model());
        let c2 = Arc::new(src2.connect(vec![], None, false, None, Some(7)));
        let fi = PlannerFanIn::new(vec![
            NonTerminusNode::Connection(c1.clone()),
            NonTerminusNode::Connection(c2.clone()),
        ]);
        fi.propagate_unlimit_from_flipped_join();
        assert_eq!(c1.limit(), None);
        assert_eq!(c2.limit(), None);
    }

    // Branch: estimate_cost FI uses max across inputs and OR selectivity.
    #[test]
    fn fi_estimate_uses_max_and_or_selectivity() {
        // Build two connections with limit=1 and a filter to drive selectivity.
        fn selective(table: &str, pct: f64) -> Arc<PlannerConnection> {
            let model: ConnectionCostModel = Arc::new(move |_t, _s, filters, _c| {
                let rows = if filters.is_some() { pct } else { 100.0 };
                let fanout: FanoutCostModel = Arc::new(|_| FanoutEst {
                    fanout: 1.0,
                    confidence: FanoutConfidence::None,
                });
                crate::planner::planner_connection::CostModelCost {
                    startup_cost: 0.0,
                    rows,
                    fanout,
                }
            });
            let src = PlannerSource::new(table, model);
            Arc::new(src.connect(
                vec![("id".to_string(), zero_cache_types::ast::Direction::Asc)],
                Some(Box::new(Condition::Simple {
                    op: SimpleOperator::Eq,
                    left: ValuePosition::Column {
                        name: "x".to_string(),
                    },
                    right: NonColumnValue::Literal {
                        value: LiteralValue::Number(1.0),
                    },
                })),
                false,
                None,
                Some(1),
            ))
        }
        let a = selective("a", 50.0);
        let b = selective("b", 30.0);
        assert!((a.selectivity() - 0.5).abs() < 1e-9);
        assert!((b.selectivity() - 0.3).abs() < 1e-9);
        let fi = PlannerFanIn::new(vec![
            NonTerminusNode::Connection(a),
            NonTerminusNode::Connection(b),
        ]);
        let est = fi.estimate_cost(1.0, &[], None);
        // P(A OR B) = 1 - (1-0.5)(1-0.3) = 0.65.
        assert!((est.selectivity - 0.65).abs() < 1e-9);
    }

    // Branch: UFI selectivity uses same OR formula.
    #[test]
    fn ufi_estimate_or_selectivity() {
        fn selective(table: &str, pct: f64) -> Arc<PlannerConnection> {
            let model: ConnectionCostModel = Arc::new(move |_t, _s, filters, _c| {
                let rows = if filters.is_some() { pct } else { 100.0 };
                let fanout: FanoutCostModel = Arc::new(|_| FanoutEst {
                    fanout: 1.0,
                    confidence: FanoutConfidence::None,
                });
                crate::planner::planner_connection::CostModelCost {
                    startup_cost: 0.0,
                    rows,
                    fanout,
                }
            });
            let src = PlannerSource::new(table, model);
            Arc::new(src.connect(
                vec![("id".to_string(), zero_cache_types::ast::Direction::Asc)],
                Some(Box::new(Condition::Simple {
                    op: SimpleOperator::Eq,
                    left: ValuePosition::Column {
                        name: "x".to_string(),
                    },
                    right: NonColumnValue::Literal {
                        value: LiteralValue::Number(1.0),
                    },
                })),
                false,
                None,
                Some(1),
            ))
        }
        let a = selective("a", 50.0);
        let b = selective("b", 30.0);
        let fi = PlannerFanIn::new(vec![
            NonTerminusNode::Connection(a),
            NonTerminusNode::Connection(b),
        ]);
        fi.convert_to_ufi();
        let est = fi.estimate_cost(1.0, &[], None);
        assert!((est.selectivity - 0.65).abs() < 1e-9);
    }

    // Branch: empty inputs path triggers `throw_fanout` panic when caller invokes fanout.
    // (Not a panic on construction — only when `fanout(...)` is called. Skip
    // direct test of panic; verify total cost is zero/initial.)
    #[test]
    fn empty_inputs_returns_zero_totals() {
        let fi = PlannerFanIn::new(vec![]);
        let est = fi.estimate_cost(1.0, &[], None);
        assert_eq!(est.cost, 0.0);
        assert_eq!(est.returned_rows, 0.0);
        assert_eq!(est.selectivity, 0.0);
    }
}
