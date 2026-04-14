//! Port of `packages/zql/src/planner/planner-fan-out.ts`.
//!
//! [`PlannerFanOut`] forks an input into multiple downstream branches. Its
//! type field switches between `FO` (single fetch shared across branches)
//! and `UFO` (one fetch per branch — picked when any descendant join inside
//! a FanOut→FanIn pair is flipped).

use std::sync::Mutex;

use super::planner_constraint::PlannerConstraint;
use super::planner_debug::{PlanDebugEvent, PlanDebugger};
use super::planner_node::{
    CostEstimate, FanOutType, FromInfo, JoinOrConnection, NonTerminusNode, PlannerNode, omit_fanout,
};

pub struct PlannerFanOut {
    type_: Mutex<FanOutType>,
    outputs: Mutex<Vec<PlannerNode>>,
    input: NonTerminusNode,
}

impl PlannerFanOut {
    pub fn new(input: NonTerminusNode) -> Self {
        Self {
            type_: Mutex::new(FanOutType::FO),
            outputs: Mutex::new(Vec::new()),
            input,
        }
    }

    pub fn type_(&self) -> FanOutType {
        *self
            .type_
            .lock()
            .expect("planner fan-out type mutex poisoned")
    }

    pub fn add_output(&self, node: PlannerNode) {
        self.outputs
            .lock()
            .expect("planner fan-out outputs mutex poisoned")
            .push(node);
    }

    pub fn outputs(&self) -> Vec<PlannerNode> {
        self.outputs
            .lock()
            .expect("planner fan-out outputs mutex poisoned")
            .clone()
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        self.input.as_planner().closest_join_or_source()
    }

    pub fn propagate_constraints(
        &self,
        branch_pattern: &[usize],
        constraint: Option<&PlannerConstraint>,
        from: Option<FromInfo>,
        plan_debugger: Option<&dyn PlanDebugger>,
    ) {
        if let Some(d) = plan_debugger {
            d.log(PlanDebugEvent::NodeConstraint {
                attempt_number: None,
                node_type: "fan-out",
                node: "FO".to_string(),
                branch_pattern: branch_pattern.to_vec(),
                constraint: constraint.cloned(),
                from: from
                    .as_ref()
                    .map(|f| f.kind.as_str().to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
            });
        }

        // Construct FromInfo describing self for the input call.
        let me = FromInfo {
            kind: super::planner_node::NodeKind::FanOut,
            name: "FO".to_string(),
        };
        self.input.as_planner().propagate_constraints(
            branch_pattern,
            constraint,
            Some(me),
            plan_debugger,
        );
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
        plan_debugger: Option<&dyn PlanDebugger>,
    ) -> CostEstimate {
        let ret = self.input.as_planner().estimate_cost(
            downstream_child_selectivity,
            branch_pattern,
            plan_debugger,
        );

        if let Some(d) = plan_debugger {
            d.log(PlanDebugEvent::NodeCost {
                attempt_number: None,
                node_type: "fan-out",
                node: "FO".to_string(),
                branch_pattern: branch_pattern.to_vec(),
                downstream_child_selectivity,
                cost_estimate: omit_fanout(&ret),
                filters: None,
                ordering: None,
                join_type: None,
            });
        }

        ret
    }

    pub fn convert_to_ufo(&self) {
        *self.type_.lock().expect("fan-out type mutex poisoned") = FanOutType::UFO;
    }

    pub fn reset(&self) {
        *self.type_.lock().expect("fan-out type mutex poisoned") = FanOutType::FO;
    }

    /// TS `propagateUnlimitFromFlippedJoin()`. FanOut propagates to its input.
    pub fn propagate_unlimit_from_flipped_join(&self) {
        self.input
            .as_planner()
            .propagate_unlimit_from_flipped_join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_connection::{ConnectionCostModel, PlannerConnection};
    use crate::planner::planner_constraint::constraint_from_fields;
    use crate::planner::planner_node::{FanoutConfidence, FanoutCostModel, FanoutEst};
    use crate::planner::planner_source::PlannerSource;
    use std::sync::Arc;

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

    // Branch: initial state is FO.
    #[test]
    fn initial_state_fo() {
        let c = make_connection("users");
        let fo = PlannerFanOut::new(NonTerminusNode::Connection(c));
        assert_eq!(fo.type_(), FanOutType::FO);
    }

    // Branch: add_output appends in order.
    #[test]
    fn add_output_appends() {
        let c = make_connection("users");
        let fo = PlannerFanOut::new(NonTerminusNode::Connection(c));
        let o1 = make_connection("posts");
        let o2 = make_connection("comments");
        fo.add_output(PlannerNode::Connection(o1.clone()));
        fo.add_output(PlannerNode::Connection(o2.clone()));
        let out = fo.outputs();
        assert_eq!(out.len(), 2);
        match (&out[0], &out[1]) {
            (PlannerNode::Connection(a), PlannerNode::Connection(b)) => {
                assert!(Arc::ptr_eq(a, &o1));
                assert!(Arc::ptr_eq(b, &o2));
            }
            _ => panic!("expected Connection variants"),
        }
    }

    // Branch: convert_to_ufo flips the type field.
    #[test]
    fn convert_to_ufo() {
        let c = make_connection("users");
        let fo = PlannerFanOut::new(NonTerminusNode::Connection(c));
        fo.convert_to_ufo();
        assert_eq!(fo.type_(), FanOutType::UFO);
    }

    // Branch: reset restores FO.
    #[test]
    fn reset_restores_fo() {
        let c = make_connection("users");
        let fo = PlannerFanOut::new(NonTerminusNode::Connection(c));
        fo.convert_to_ufo();
        fo.reset();
        assert_eq!(fo.type_(), FanOutType::FO);
    }

    // Branch: propagate_constraints forwards to input (input acquires constraint).
    #[test]
    fn propagate_constraints_forwards_to_input() {
        let c = make_connection("users");
        let fo = PlannerFanOut::new(NonTerminusNode::Connection(c.clone()));
        let cstr = constraint_from_fields(["userId"]);
        fo.propagate_constraints(&[0], Some(&cstr), None, None);
        // After propagation, input's estimate at branch [0] should reflect 1 constraint.
        let est = c.estimate_cost(1.0, &[0], None);
        assert_eq!(est.returned_rows, 90.0);
    }

    // Branch: closest_join_or_source delegates to input.
    #[test]
    fn closest_delegates() {
        let c = make_connection("users");
        let fo = PlannerFanOut::new(NonTerminusNode::Connection(c));
        assert_eq!(fo.closest_join_or_source(), JoinOrConnection::Connection);
    }

    // Branch: estimate_cost forwards to input.
    #[test]
    fn estimate_cost_forwards() {
        let c = make_connection("users");
        let fo = PlannerFanOut::new(NonTerminusNode::Connection(c));
        let est = fo.estimate_cost(1.0, &[], None);
        assert_eq!(est.returned_rows, 100.0);
    }

    // Branch: propagate_unlimit_from_flipped_join walks down to input
    // (Connection.unlimit clears the limit).
    #[test]
    fn unlimit_walks_to_input() {
        let src = PlannerSource::new("users", simple_cost_model());
        let conn = Arc::new(src.connect(vec![], None, false, None, Some(5)));
        let fo = PlannerFanOut::new(NonTerminusNode::Connection(conn.clone()));
        assert_eq!(conn.limit(), Some(5));
        fo.propagate_unlimit_from_flipped_join();
        assert_eq!(conn.limit(), None);
    }
}
