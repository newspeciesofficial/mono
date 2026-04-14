//! Port of `packages/zql/src/planner/planner-terminus.ts`.
//!
//! [`PlannerTerminus`] is the sink of a plan graph. It owns one non-terminus
//! input and starts cost-estimation / constraint-propagation by calling into
//! that input with the initial values (`downstreamChildSelectivity = 1`,
//! empty branch pattern, `constraint = None`).

use super::planner_debug::PlanDebugger;
use super::planner_node::{CostEstimate, FromInfo, JoinOrConnection, NodeKind, NonTerminusNode};

pub struct PlannerTerminus {
    input: NonTerminusNode,
}

impl PlannerTerminus {
    pub fn new(input: NonTerminusNode) -> Self {
        Self { input }
    }

    /// TS `get pinned(): boolean { return true; }`.
    pub fn pinned(&self) -> bool {
        true
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        self.input.as_planner().closest_join_or_source()
    }

    /// Entry-point used by [`super::planner_graph::PlannerGraph::propagate_constraints`].
    pub fn propagate_constraints(&self, plan_debugger: Option<&dyn PlanDebugger>) {
        // TS: `this.#input.propagateConstraints([], undefined, this, planDebugger);`
        self.input.as_planner().propagate_constraints(
            &[],
            None,
            Some(FromInfo {
                kind: NodeKind::Terminus,
                name: "terminus".to_string(),
            }),
            plan_debugger,
        );
    }

    /// Entry-point used by [`super::planner_graph::PlannerGraph::get_total_cost`].
    pub fn estimate_cost(&self, plan_debugger: Option<&dyn PlanDebugger>) -> CostEstimate {
        // TS: `this.#input.estimateCost(1, [], planDebugger);`
        self.input
            .as_planner()
            .estimate_cost(1.0, &[], plan_debugger)
    }

    /// TS `propagateUnlimitFromFlippedJoin(): void { // No-op }`.
    pub fn propagate_unlimit_from_flipped_join(&self) {
        // Explicit no-op.
    }

    /// Accessor for wiring (planner-graph BFS traverses `output` fields).
    pub fn input(&self) -> &NonTerminusNode {
        &self.input
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_connection::{ConnectionCostModel, PlannerConnection};
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

    fn make_connection() -> Arc<PlannerConnection> {
        let src = PlannerSource::new("users", simple_cost_model());
        Arc::new(src.connect(vec![], None, false, None, None))
    }

    // Branch: pinned() is always true.
    #[test]
    fn pinned_returns_true() {
        let c = make_connection();
        let t = PlannerTerminus::new(NonTerminusNode::Connection(c));
        assert!(t.pinned());
    }

    // Branch: closest_join_or_source delegates to input.
    #[test]
    fn closest_delegates_to_connection_input() {
        let c = make_connection();
        let t = PlannerTerminus::new(NonTerminusNode::Connection(c));
        assert_eq!(t.closest_join_or_source(), JoinOrConnection::Connection);
    }

    // Branch: estimate_cost kicks chain with selectivity=1, empty pattern.
    #[test]
    fn estimate_cost_uses_empty_pattern() {
        let c = make_connection();
        let t = PlannerTerminus::new(NonTerminusNode::Connection(c));
        let ce = t.estimate_cost(None);
        assert_eq!(ce.returned_rows, 100.0);
    }

    // Branch: propagate_unlimit_from_flipped_join is a no-op (does not panic).
    #[test]
    fn propagate_unlimit_noop() {
        let c = make_connection();
        let t = PlannerTerminus::new(NonTerminusNode::Connection(c));
        t.propagate_unlimit_from_flipped_join();
    }
}
