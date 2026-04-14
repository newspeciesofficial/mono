//! Port of `packages/zql/src/planner/planner-join.ts`.
//!
//! [`PlannerJoin`] models a correlated subquery (EXISTS / NOT EXISTS) as a
//! join between a parent and child sub-graph. It can be in `semi` or
//! `flipped` state — flipping switches which side drives the loop.

use std::sync::{Arc, Mutex};

use thiserror::Error;

use super::planner_constraint::{PlannerConstraint, merge_constraints};
use super::planner_debug::{PlanDebugEvent, PlanDebugger};
use super::planner_node::{
    CostEstimate, FromInfo, JoinOrConnection, JoinType, NodeKind, NonTerminusNode, PlannerNode,
    omit_fanout,
};

#[derive(Debug, Error)]
#[error("{0}")]
pub struct UnflippableJoinError(pub String);

pub struct PlannerJoin {
    parent: NonTerminusNode,
    child: NonTerminusNode,
    parent_constraint: PlannerConstraint,
    child_constraint: PlannerConstraint,
    flippable: bool,
    pub plan_id: u32,
    output: Mutex<Option<PlannerNode>>,
    type_: Mutex<JoinType>,
    initial_type: JoinType,
}

impl PlannerJoin {
    pub fn new(
        parent: NonTerminusNode,
        child: NonTerminusNode,
        parent_constraint: PlannerConstraint,
        child_constraint: PlannerConstraint,
        flippable: bool,
        plan_id: u32,
        initial_type: JoinType,
    ) -> Self {
        Self {
            parent,
            child,
            parent_constraint,
            child_constraint,
            flippable,
            plan_id,
            output: Mutex::new(None),
            type_: Mutex::new(initial_type),
            initial_type,
        }
    }

    pub fn set_output(&self, node: PlannerNode) {
        *self.output.lock().expect("join output mutex poisoned") = Some(node);
    }

    pub fn output(&self) -> PlannerNode {
        self.output
            .lock()
            .expect("join output mutex poisoned")
            .clone()
            .expect("Output not set")
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        JoinOrConnection::Join
    }

    pub fn flip_if_needed(&self, input: &PlannerNode) {
        // TS: `if (input === this.#child) flip; else assert(input === parent)`.
        if input.ptr_eq(&self.child.as_planner()) {
            self.flip().expect("flip");
        } else {
            assert!(
                input.ptr_eq(&self.parent.as_planner()),
                "Can only flip a join from one of its inputs"
            );
        }
    }

    pub fn flip(&self) -> Result<(), UnflippableJoinError> {
        let mut t = self.type_.lock().expect("join type mutex poisoned");
        assert!(matches!(*t, JoinType::Semi), "Can only flip a semi-join");
        if !self.flippable {
            return Err(UnflippableJoinError(
                "Cannot flip a non-flippable join (e.g., NOT EXISTS)".to_string(),
            ));
        }
        *t = JoinType::Flipped;
        Ok(())
    }

    pub fn type_(&self) -> JoinType {
        *self.type_.lock().expect("join type mutex poisoned")
    }

    pub fn is_flippable(&self) -> bool {
        self.flippable
    }

    /// TS `propagateUnlimit()` — only valid on a flipped join.
    pub fn propagate_unlimit(&self) {
        assert!(
            matches!(self.type_(), JoinType::Flipped),
            "Can only unlimit a flipped join"
        );
        self.child
            .as_planner()
            .propagate_unlimit_from_flipped_join();
    }

    pub fn propagate_unlimit_from_flipped_join(&self) {
        // Continue up the parent chain.
        self.parent
            .as_planner()
            .propagate_unlimit_from_flipped_join();
    }

    pub fn propagate_constraints(
        &self,
        branch_pattern: &[usize],
        constraint: Option<&PlannerConstraint>,
        from: Option<FromInfo>,
        plan_debugger: Option<&dyn PlanDebugger>,
    ) {
        let kind = self.type_();
        if let Some(d) = plan_debugger {
            d.log(PlanDebugEvent::NodeConstraint {
                attempt_number: None,
                node_type: "join",
                node: self.get_name(),
                branch_pattern: branch_pattern.to_vec(),
                constraint: constraint.cloned(),
                from: from
                    .as_ref()
                    .map(|f| f.name.clone())
                    .unwrap_or_else(|| "unknown".to_string()),
            });
        }

        let me = FromInfo {
            kind: NodeKind::Join,
            name: self.get_name(),
        };

        match kind {
            JoinType::Semi => {
                // Branch: semi — child gets childConstraint, parent gets the incoming constraint.
                self.child.as_planner().propagate_constraints(
                    branch_pattern,
                    Some(&self.child_constraint),
                    Some(me.clone()),
                    plan_debugger,
                );
                self.parent.as_planner().propagate_constraints(
                    branch_pattern,
                    constraint,
                    Some(me),
                    plan_debugger,
                );
            }
            JoinType::Flipped => {
                // Branch: flipped — child gets translated incoming constraint;
                // parent gets merged(incoming, parent_constraint).
                let translated = translate_constraints_for_flipped_join(
                    constraint,
                    &self.parent_constraint,
                    &self.child_constraint,
                );
                self.child.as_planner().propagate_constraints(
                    branch_pattern,
                    translated.as_ref(),
                    Some(me.clone()),
                    plan_debugger,
                );
                let merged = merge_constraints(constraint, Some(&self.parent_constraint));
                self.parent.as_planner().propagate_constraints(
                    branch_pattern,
                    merged.as_ref(),
                    Some(me),
                    plan_debugger,
                );
            }
        }
    }

    pub fn reset(&self) {
        *self.type_.lock().expect("join type mutex poisoned") = self.initial_type;
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
        plan_debugger: Option<&dyn PlanDebugger>,
    ) -> CostEstimate {
        let child = self
            .child
            .as_planner()
            .estimate_cost(1.0, branch_pattern, plan_debugger);

        let child_keys: Vec<String> = self.child_constraint.keys().cloned().collect();
        let fanout_factor = (child.fanout)(&child_keys);
        let scaled_child_selectivity = 1.0 - (1.0 - child.selectivity).powf(fanout_factor.fanout);

        let kind = self.type_();
        let parent_input_selectivity = match kind {
            JoinType::Flipped => 1.0 * downstream_child_selectivity,
            JoinType::Semi => scaled_child_selectivity * downstream_child_selectivity,
        };

        let parent = self.parent.as_planner().estimate_cost(
            parent_input_selectivity,
            branch_pattern,
            plan_debugger,
        );

        let result = match kind {
            JoinType::Semi => {
                let scan = match parent.limit {
                    None => parent.returned_rows,
                    Some(limit) => {
                        parent
                            .returned_rows
                            .min(if downstream_child_selectivity == 0.0 {
                                0.0
                            } else {
                                limit as f64 / downstream_child_selectivity
                            })
                    }
                };
                CostEstimate {
                    startup_cost: parent.startup_cost,
                    scan_est: scan,
                    cost: parent.cost
                        + parent.scan_est * (child.startup_cost + child.cost + child.scan_est),
                    returned_rows: parent.returned_rows * child.selectivity,
                    selectivity: child.selectivity * parent.selectivity,
                    limit: parent.limit,
                    fanout: parent.fanout.clone(),
                }
            }
            JoinType::Flipped => {
                let scan = match parent.limit {
                    None => parent.returned_rows * child.returned_rows,
                    Some(limit) => (parent.returned_rows * child.returned_rows).min(
                        if downstream_child_selectivity == 0.0 {
                            0.0
                        } else {
                            limit as f64 / downstream_child_selectivity
                        },
                    ),
                };
                CostEstimate {
                    startup_cost: child.startup_cost,
                    scan_est: scan,
                    cost: child.cost
                        + child.scan_est * (parent.startup_cost + parent.cost + parent.scan_est),
                    returned_rows: parent.returned_rows * child.returned_rows,
                    selectivity: parent.selectivity * child.selectivity,
                    limit: parent.limit,
                    fanout: parent.fanout.clone(),
                }
            }
        };

        if let Some(d) = plan_debugger {
            d.log(PlanDebugEvent::NodeCost {
                attempt_number: None,
                node_type: "join",
                node: self.get_name(),
                branch_pattern: branch_pattern.to_vec(),
                downstream_child_selectivity,
                cost_estimate: omit_fanout(&result),
                filters: None,
                ordering: None,
                join_type: Some(kind),
            });
        }

        result
    }

    /// TS `getName()`.
    pub fn get_name(&self) -> String {
        let p = self.parent.as_planner().debug_name();
        let c = self.child.as_planner().debug_name();
        format!("{p} ⋈ {c}")
    }

    /// TS `getDebugInfo()`.
    pub fn get_debug_info(&self) -> JoinDebugInfo {
        JoinDebugInfo {
            name: self.get_name(),
            type_: self.type_(),
            plan_id: self.plan_id,
        }
    }

    /// Accessors for graph operations.
    pub fn parent(&self) -> &NonTerminusNode {
        &self.parent
    }
    pub fn child(&self) -> &NonTerminusNode {
        &self.child
    }
    pub fn parent_constraint(&self) -> &PlannerConstraint {
        &self.parent_constraint
    }
    pub fn child_constraint(&self) -> &PlannerConstraint {
        &self.child_constraint
    }
    pub fn initial_type(&self) -> JoinType {
        self.initial_type
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinDebugInfo {
    pub name: String,
    pub type_: JoinType,
    pub plan_id: u32,
}

/// Index-based key translation matching TS `translateConstraintsForFlippedJoin`.
fn translate_constraints_for_flipped_join(
    incoming: Option<&PlannerConstraint>,
    parent_constraint: &PlannerConstraint,
    child_constraint: &PlannerConstraint,
) -> Option<PlannerConstraint> {
    let incoming = incoming?;
    let parent_keys: Vec<&String> = parent_constraint.keys().collect();
    let child_keys: Vec<&String> = child_constraint.keys().collect();
    let mut translated = PlannerConstraint::new();
    for (key, _) in incoming.iter() {
        if let Some(idx) = parent_keys.iter().position(|k| *k == key) {
            // Map to child key at same position. TS would crash if child
            // is shorter — match that behaviour with `.get(idx)` panic.
            let target = child_keys
                .get(idx)
                .expect("child constraint shorter than parent");
            translated.insert((*target).clone(), ());
        }
    }
    if translated.is_empty() {
        None
    } else {
        Some(translated)
    }
}

// `Arc` constructor convenience to mirror `new PlannerJoin(...)` returning
// a heap-shared instance.
pub fn arc_join(
    parent: NonTerminusNode,
    child: NonTerminusNode,
    parent_constraint: PlannerConstraint,
    child_constraint: PlannerConstraint,
    flippable: bool,
    plan_id: u32,
    initial_type: JoinType,
) -> Arc<PlannerJoin> {
    Arc::new(PlannerJoin::new(
        parent,
        child,
        parent_constraint,
        child_constraint,
        flippable,
        plan_id,
        initial_type,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_connection::{ConnectionCostModel, PlannerConnection};
    use crate::planner::planner_constraint::constraint_from_fields;
    use crate::planner::planner_node::{FanoutConfidence, FanoutCostModel, FanoutEst};
    use crate::planner::planner_source::PlannerSource;

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

    fn make_join(
        flippable: bool,
    ) -> (
        Arc<PlannerConnection>,
        Arc<PlannerConnection>,
        Arc<PlannerJoin>,
    ) {
        let parent = make_connection("users");
        let child = make_connection("posts");
        let join = arc_join(
            NonTerminusNode::Connection(parent.clone()),
            NonTerminusNode::Connection(child.clone()),
            constraint_from_fields(["userId"]),
            constraint_from_fields(["id"]),
            flippable,
            0,
            JoinType::Semi,
        );
        (parent, child, join)
    }

    // Branch: initial state is semi.
    #[test]
    fn initial_state_is_semi() {
        let (_, _, j) = make_join(true);
        assert_eq!(j.type_(), JoinType::Semi);
    }

    // Branch: flip succeeds when flippable.
    #[test]
    fn flip_succeeds_when_flippable() {
        let (_, _, j) = make_join(true);
        j.flip().unwrap();
        assert_eq!(j.type_(), JoinType::Flipped);
    }

    // Branch: flip fails on non-flippable.
    #[test]
    fn flip_fails_when_not_flippable() {
        let (_, _, j) = make_join(false);
        assert!(j.flip().is_err());
    }

    // Branch: flip when already flipped → assertion panic.
    #[test]
    #[should_panic(expected = "Can only flip a semi-join")]
    fn double_flip_panics() {
        let (_, _, j) = make_join(true);
        j.flip().unwrap();
        let _ = j.flip();
    }

    // Branch: flip_if_needed flips when input is child.
    #[test]
    fn flip_if_needed_flips_for_child() {
        let (_, child, j) = make_join(true);
        j.flip_if_needed(&PlannerNode::Connection(child));
        assert_eq!(j.type_(), JoinType::Flipped);
    }

    // Branch: flip_if_needed leaves type alone for parent.
    #[test]
    fn flip_if_needed_noop_for_parent() {
        let (parent, _, j) = make_join(true);
        j.flip_if_needed(&PlannerNode::Connection(parent));
        assert_eq!(j.type_(), JoinType::Semi);
    }

    // Branch: reset restores initial type.
    #[test]
    fn reset_restores_initial() {
        let (_, _, j) = make_join(true);
        j.flip().unwrap();
        j.reset();
        assert_eq!(j.type_(), JoinType::Semi);
    }

    // Branch: propagate_constraints — semi sends childConstraint to child.
    #[test]
    fn semi_propagates_child_constraint_to_child() {
        let (_, child, j) = make_join(true);
        j.propagate_constraints(&[0], None, None, None);
        let est = child.estimate_cost(1.0, &[0], None);
        // child_constraint = {id} → 1 constraint → rows = 90.
        assert_eq!(est.returned_rows, 90.0);
    }

    // Branch: propagate_constraints — flipped sends translated to child + merged to parent.
    #[test]
    fn flipped_propagates_translated_to_child_and_merged_to_parent() {
        let (parent, _child, j) = make_join(true);
        j.flip().unwrap();
        // Incoming has the parent key — should translate to child key.
        let incoming = constraint_from_fields(["userId"]);
        j.propagate_constraints(&[0], Some(&incoming), None, None);
        // Parent's stored constraint at branch [0] = merged(incoming, parent_constraint)
        // = {userId} (same key in both → 1 entry).
        let p_est = parent.estimate_cost(1.0, &[0], None);
        assert_eq!(p_est.returned_rows, 90.0);
    }

    // Branch: estimate_cost semi vs flipped numeric equality on baseline.
    #[test]
    fn semi_and_flipped_have_equal_baseline_cost() {
        let (_, _, j) = make_join(true);
        let semi = j.estimate_cost(1.0, &[], None);
        j.reset();
        j.flip().unwrap();
        let flip = j.estimate_cost(1.0, &[], None);
        assert_eq!(semi.cost, flip.cost);
    }

    // Branch: propagate_unlimit panics if not flipped.
    #[test]
    #[should_panic(expected = "Can only unlimit a flipped join")]
    fn propagate_unlimit_panics_on_semi() {
        let (_, _, j) = make_join(true);
        j.propagate_unlimit();
    }

    // Branch: propagate_unlimit on flipped delegates down child chain.
    #[test]
    fn propagate_unlimit_walks_child() {
        // child must be a Connection with a limit so we can verify it gets cleared.
        let parent = make_connection("users");
        let src = PlannerSource::new("posts", simple_cost_model());
        let child = Arc::new(src.connect(vec![], None, false, None, Some(5)));
        let j = arc_join(
            NonTerminusNode::Connection(parent),
            NonTerminusNode::Connection(child.clone()),
            constraint_from_fields(["userId"]),
            constraint_from_fields(["id"]),
            true,
            0,
            JoinType::Semi,
        );
        j.flip().unwrap();
        j.propagate_unlimit();
        assert_eq!(child.limit(), None);
    }

    // Branch: get_name format.
    #[test]
    fn get_name_format() {
        let (_, _, j) = make_join(true);
        assert_eq!(j.get_name(), "users ⋈ posts");
    }

    // Branch: translate_constraints index alignment.
    #[test]
    fn translate_constraints_aligns_by_index() {
        let parent_c = constraint_from_fields(["issueID", "projectID"]);
        let child_c = constraint_from_fields(["id", "projectID"]);
        let incoming = constraint_from_fields(["issueID"]);
        let out =
            translate_constraints_for_flipped_join(Some(&incoming), &parent_c, &child_c).unwrap();
        let keys: Vec<&str> = out.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["id"]);
    }

    // Branch: translate_constraints returns None when incoming is None.
    #[test]
    fn translate_constraints_none_in_none_out() {
        let p = constraint_from_fields(["a"]);
        let c = constraint_from_fields(["b"]);
        assert!(translate_constraints_for_flipped_join(None, &p, &c).is_none());
    }

    // Branch: translate_constraints returns None when no overlap with parent.
    #[test]
    fn translate_constraints_no_overlap_returns_none() {
        let p = constraint_from_fields(["a"]);
        let c = constraint_from_fields(["b"]);
        let inc = constraint_from_fields(["zzz"]);
        assert!(translate_constraints_for_flipped_join(Some(&inc), &p, &c).is_none());
    }
}
