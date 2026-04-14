//! Port of `packages/zql/src/planner/planner-graph.ts`.
//!
//! Owns the planner nodes and drives the cost-search loop.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use super::planner_connection::{ConnectionCostModel, ConstraintMap, PlannerConnection};
use super::planner_debug::{PlanDebugEvent, PlanDebugger};
use super::planner_fan_in::PlannerFanIn;
use super::planner_fan_out::PlannerFanOut;
use super::planner_join::PlannerJoin;
use super::planner_node::{FanInType, FanOutType, JoinType, PlannerNode};
use super::planner_source::PlannerSource;
use super::planner_terminus::PlannerTerminus;

/// Maximum number of flippable joins to attempt exhaustive enumeration.
const MAX_FLIPPABLE_JOINS: usize = 9;

/// TS `PlanState`.
#[derive(Debug, Clone)]
pub struct PlanState {
    pub connections: Vec<ConnectionState>,
    pub joins: Vec<JoinState>,
    pub fan_outs: Vec<FanOutState>,
    pub fan_ins: Vec<FanInState>,
    pub connection_constraints: Vec<ConstraintMap>,
}

#[derive(Debug, Clone)]
pub struct ConnectionState {
    pub limit: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct JoinState {
    pub type_: JoinType,
}

#[derive(Debug, Clone)]
pub struct FanOutState {
    pub type_: FanOutType,
}

#[derive(Debug, Clone)]
pub struct FanInState {
    pub type_: FanInType,
}

/// Cached FO→FI relationship + joins between.
#[derive(Default)]
struct FoFiInfo {
    fi: Option<Arc<PlannerFanIn>>,
    joins_between: Vec<Arc<PlannerJoin>>,
}

pub struct PlannerGraph {
    sources: Mutex<HashMap<String, Arc<PlannerSource>>>,
    terminus: Mutex<Option<Arc<PlannerTerminus>>>,
    pub joins: Mutex<Vec<Arc<PlannerJoin>>>,
    pub fan_outs: Mutex<Vec<Arc<PlannerFanOut>>>,
    pub fan_ins: Mutex<Vec<Arc<PlannerFanIn>>>,
    pub connections: Mutex<Vec<Arc<PlannerConnection>>>,
}

impl Default for PlannerGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl PlannerGraph {
    pub fn new() -> Self {
        Self {
            sources: Mutex::new(HashMap::new()),
            terminus: Mutex::new(None),
            joins: Mutex::new(Vec::new()),
            fan_outs: Mutex::new(Vec::new()),
            fan_ins: Mutex::new(Vec::new()),
            connections: Mutex::new(Vec::new()),
        }
    }

    pub fn reset_planning_state(&self) {
        for j in self.joins.lock().expect("joins poisoned").iter() {
            j.reset();
        }
        for fo in self.fan_outs.lock().expect("fan_outs poisoned").iter() {
            fo.reset();
        }
        for fi in self.fan_ins.lock().expect("fan_ins poisoned").iter() {
            fi.reset();
        }
        for c in self
            .connections
            .lock()
            .expect("connections poisoned")
            .iter()
        {
            c.reset();
        }
    }

    pub fn add_source(
        &self,
        name: impl Into<String>,
        model: ConnectionCostModel,
    ) -> Arc<PlannerSource> {
        let name = name.into();
        let mut sources = self.sources.lock().expect("sources poisoned");
        assert!(
            !sources.contains_key(&name),
            "Source {name} already exists in the graph"
        );
        let s = Arc::new(PlannerSource::new(name.clone(), model));
        sources.insert(name, s.clone());
        s
    }

    pub fn get_source(&self, name: &str) -> Arc<PlannerSource> {
        let sources = self.sources.lock().expect("sources poisoned");
        sources
            .get(name)
            .cloned()
            .unwrap_or_else(|| panic!("Source {name} not found in the graph"))
    }

    pub fn has_source(&self, name: &str) -> bool {
        self.sources
            .lock()
            .expect("sources poisoned")
            .contains_key(name)
    }

    pub fn set_terminus(&self, t: Arc<PlannerTerminus>) {
        *self.terminus.lock().expect("terminus poisoned") = Some(t);
    }

    pub fn terminus(&self) -> Arc<PlannerTerminus> {
        self.terminus
            .lock()
            .expect("terminus poisoned")
            .clone()
            .expect("Cannot use graph without a terminus")
    }

    pub fn propagate_constraints(&self, plan_debugger: Option<&dyn PlanDebugger>) {
        let t = self
            .terminus
            .lock()
            .expect("terminus poisoned")
            .clone()
            .expect("Cannot propagate constraints without a terminus node");
        t.propagate_constraints(plan_debugger);
    }

    pub fn get_total_cost(&self, plan_debugger: Option<&dyn PlanDebugger>) -> f64 {
        let t = self.terminus();
        let est = t.estimate_cost(plan_debugger);
        est.cost + est.startup_cost
    }

    pub fn capture_planning_snapshot(&self) -> PlanState {
        let connections = self
            .connections
            .lock()
            .expect("connections poisoned")
            .iter()
            .map(|c| ConnectionState { limit: c.limit() })
            .collect::<Vec<_>>();
        let joins = self
            .joins
            .lock()
            .expect("joins poisoned")
            .iter()
            .map(|j| JoinState { type_: j.type_() })
            .collect::<Vec<_>>();
        let fan_outs = self
            .fan_outs
            .lock()
            .expect("fan_outs poisoned")
            .iter()
            .map(|fo| FanOutState { type_: fo.type_() })
            .collect::<Vec<_>>();
        let fan_ins = self
            .fan_ins
            .lock()
            .expect("fan_ins poisoned")
            .iter()
            .map(|fi| FanInState { type_: fi.type_() })
            .collect::<Vec<_>>();
        let connection_constraints = self
            .connections
            .lock()
            .expect("connections poisoned")
            .iter()
            .map(|c| c.capture_constraints())
            .collect::<Vec<_>>();
        PlanState {
            connections,
            joins,
            fan_outs,
            fan_ins,
            connection_constraints,
        }
    }

    pub fn restore_planning_snapshot(&self, state: &PlanState) {
        // Validate shape.
        let connections = self.connections.lock().expect("connections poisoned");
        let joins = self.joins.lock().expect("joins poisoned");
        let fan_outs = self.fan_outs.lock().expect("fan_outs poisoned");
        let fan_ins = self.fan_ins.lock().expect("fan_ins poisoned");
        assert_eq!(
            connections.len(),
            state.connections.len(),
            "Plan state mismatch: connections"
        );
        assert_eq!(joins.len(), state.joins.len(), "Plan state mismatch: joins");
        assert_eq!(
            fan_outs.len(),
            state.fan_outs.len(),
            "Plan state mismatch: fanOuts"
        );
        assert_eq!(
            fan_ins.len(),
            state.fan_ins.len(),
            "Plan state mismatch: fanIns"
        );
        assert_eq!(
            connections.len(),
            state.connection_constraints.len(),
            "Plan state mismatch: connectionConstraints"
        );

        // Restore connections.
        for (i, c) in connections.iter().enumerate() {
            // Limit setter — there is no `set_limit`; we use unlimit / re-init via reset+restore.
            // Easier: directly poke through (we have a private interface). Simulate via reset and re-snapshot.
            // To match TS behaviour: TS sets `c.limit = state.connections[i].limit` directly on the field.
            // Our connection has `limit: Mutex<Option<u64>>`. Add a setter method (below).
            c.set_limit(state.connections[i].limit);
            c.restore_constraints(&state.connection_constraints[i]);
        }

        // Restore joins.
        for (i, j) in joins.iter().enumerate() {
            let target = state.joins[i].type_;
            j.reset();
            if matches!(target, JoinType::Flipped) && !matches!(j.type_(), JoinType::Flipped) {
                j.flip().expect("flip during restore");
            }
            assert_eq!(
                target,
                j.type_(),
                "join is not in the correct state after reset"
            );
        }

        // Restore fan-out / fan-in.
        for (i, fo) in fan_outs.iter().enumerate() {
            let target = state.fan_outs[i].type_;
            if matches!(target, FanOutType::UFO) && matches!(fo.type_(), FanOutType::FO) {
                fo.convert_to_ufo();
            }
        }
        for (i, fi) in fan_ins.iter().enumerate() {
            let target = state.fan_ins[i].type_;
            if matches!(target, FanInType::UFI) && matches!(fi.type_(), FanInType::FI) {
                fi.convert_to_ufi();
            }
        }
    }

    /// Main planning algorithm — exhaustive 2^n flip enumeration.
    pub fn plan(&self, plan_debugger: Option<&dyn PlanDebugger>) -> PlanResult {
        let flippable_joins: Vec<Arc<PlannerJoin>> = self
            .joins
            .lock()
            .expect("joins poisoned")
            .iter()
            .filter(|j| j.is_flippable())
            .cloned()
            .collect();

        if flippable_joins.len() > MAX_FLIPPABLE_JOINS {
            // Branch: too many → bail.
            return PlanResult::TooComplex {
                flippable_joins: flippable_joins.len(),
            };
        }

        // Build FO→FI cache once.
        let fofi_cache = build_fofi_cache(self);

        let num_patterns = if flippable_joins.is_empty() {
            0
        } else {
            1u32 << flippable_joins.len()
        };

        let mut best_cost = f64::INFINITY;
        let mut best_plan: Option<PlanState> = None;
        let mut best_attempt: i64 = -1;

        for pattern in 0..num_patterns {
            self.reset_planning_state();

            if let Some(d) = plan_debugger {
                d.log(PlanDebugEvent::AttemptStart {
                    attempt_number: pattern as usize,
                    total_attempts: num_patterns as usize,
                });
            }

            // Apply flip pattern.
            for (i, j) in flippable_joins.iter().enumerate() {
                if pattern & (1 << i) != 0 {
                    j.flip().expect("flippable join must flip");
                }
            }

            // Derive FO/UFO and FI/UFI.
            check_and_convert_fofi(&fofi_cache);

            // Propagate unlimits.
            propagate_unlimit_for_flipped_joins(self);

            // Propagate constraints.
            self.propagate_constraints(plan_debugger);

            // Evaluate cost.
            let total = self.get_total_cost(plan_debugger);

            if let Some(d) = plan_debugger {
                d.log(PlanDebugEvent::PlanComplete {
                    attempt_number: pattern as usize,
                    total_cost: total,
                    flip_pattern: pattern,
                    join_states: self
                        .joins
                        .lock()
                        .expect("joins poisoned")
                        .iter()
                        .map(|j| (j.get_name(), j.type_()))
                        .collect(),
                });
            }

            if total < best_cost {
                best_cost = total;
                best_plan = Some(self.capture_planning_snapshot());
                best_attempt = pattern as i64;
            }
        }

        if let Some(plan) = &best_plan {
            self.restore_planning_snapshot(plan);
            self.propagate_constraints(plan_debugger);

            if let Some(d) = plan_debugger {
                d.log(PlanDebugEvent::BestPlanSelected {
                    best_attempt_number: best_attempt as usize,
                    total_cost: best_cost,
                    flip_pattern: best_attempt as u32,
                    join_states: self
                        .joins
                        .lock()
                        .expect("joins poisoned")
                        .iter()
                        .map(|j| (j.get_name(), j.type_()))
                        .collect(),
                });
            }
            PlanResult::Selected {
                attempt: best_attempt as u32,
                cost: best_cost,
            }
        } else {
            assert_eq!(
                num_patterns, 0,
                "no plan was found but flippable joins did exist!"
            );
            PlanResult::Empty
        }
    }
}

#[derive(Debug, Clone)]
pub enum PlanResult {
    Selected { attempt: u32, cost: f64 },
    Empty,
    TooComplex { flippable_joins: usize },
}

// ---------------------------------------------------------------------------
// FOFI cache + checks
// ---------------------------------------------------------------------------

fn build_fofi_cache(graph: &PlannerGraph) -> Vec<(Arc<PlannerFanOut>, FoFiInfo)> {
    let mut cache: Vec<(Arc<PlannerFanOut>, FoFiInfo)> = Vec::new();
    for fo in graph.fan_outs.lock().expect("fan_outs poisoned").iter() {
        let info = find_fi_and_joins(fo);
        cache.push((fo.clone(), info));
    }
    cache
}

fn check_and_convert_fofi(cache: &[(Arc<PlannerFanOut>, FoFiInfo)]) {
    for (fo, info) in cache {
        let has_flipped = info
            .joins_between
            .iter()
            .any(|j| matches!(j.type_(), JoinType::Flipped));
        if let (Some(fi), true) = (info.fi.clone(), has_flipped) {
            fo.convert_to_ufo();
            fi.convert_to_ufi();
        }
    }
}

fn find_fi_and_joins(fo: &Arc<PlannerFanOut>) -> FoFiInfo {
    let mut joins_between: Vec<Arc<PlannerJoin>> = Vec::new();
    let mut fi: Option<Arc<PlannerFanIn>> = None;

    let mut queue: Vec<PlannerNode> = fo.outputs();
    let mut visited_join: HashSet<usize> = HashSet::new();
    let mut visited_fan_out: HashSet<usize> = HashSet::new();
    let mut visited_fan_in: HashSet<usize> = HashSet::new();
    let mut visited_conn: HashSet<usize> = HashSet::new();

    while let Some(node) = queue.pop() {
        match node {
            PlannerNode::Join(j) => {
                let key = Arc::as_ptr(&j) as usize;
                if visited_join.insert(key) {
                    joins_between.push(j.clone());
                    queue.push(j.output());
                }
            }
            PlannerNode::FanOut(nested) => {
                let key = Arc::as_ptr(&nested) as usize;
                if visited_fan_out.insert(key) {
                    queue.extend(nested.outputs());
                }
            }
            PlannerNode::FanIn(found_fi) => {
                let key = Arc::as_ptr(&found_fi) as usize;
                if visited_fan_in.insert(key) {
                    fi = Some(found_fi);
                }
            }
            PlannerNode::Connection(c) => {
                let key = Arc::as_ptr(&c) as usize;
                visited_conn.insert(key);
                // Connections shouldn't appear in a well-formed FO→FI fan
                // body. TS leaves this as a no-op.
            }
            PlannerNode::Terminus(_) => {
                // Reached the end without finding FI.
            }
        }
    }
    FoFiInfo { fi, joins_between }
}

fn propagate_unlimit_for_flipped_joins(graph: &PlannerGraph) {
    for j in graph.joins.lock().expect("joins poisoned").iter() {
        if matches!(j.type_(), JoinType::Flipped) {
            j.propagate_unlimit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_constraint::constraint_from_fields;
    use crate::planner::planner_node::{
        FanoutConfidence, FanoutCostModel, FanoutEst, NonTerminusNode,
    };
    use std::sync::Arc;

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

    fn build_simple_graph() -> Arc<PlannerGraph> {
        let g = Arc::new(PlannerGraph::new());
        let src = g.add_source("users", simple_cost_model());
        let conn = Arc::new(src.connect(vec![], None, true, None, None));
        g.connections.lock().unwrap().push(conn.clone());
        let term = Arc::new(PlannerTerminus::new(NonTerminusNode::Connection(conn)));
        g.set_terminus(term);
        g
    }

    // Branch: add_source then has_source / get_source.
    #[test]
    fn add_and_lookup_source() {
        let g = PlannerGraph::new();
        g.add_source("users", simple_cost_model());
        assert!(g.has_source("users"));
        assert_eq!(g.get_source("users").name, "users");
    }

    // Branch: duplicate add_source panics.
    #[test]
    #[should_panic(expected = "already exists")]
    fn duplicate_source_panics() {
        let g = PlannerGraph::new();
        g.add_source("users", simple_cost_model());
        g.add_source("users", simple_cost_model());
    }

    // Branch: get_total_cost on a single-connection graph.
    #[test]
    fn total_cost_single_connection() {
        let g = build_simple_graph();
        let cost = g.get_total_cost(None);
        // Connection cost field is 0 in TS; startup_cost is 0 → total = 0.
        assert_eq!(cost, 0.0);
    }

    // Branch: capture / restore round-trips state.
    #[test]
    fn snapshot_round_trip() {
        let g = build_simple_graph();
        let snap = g.capture_planning_snapshot();
        g.restore_planning_snapshot(&snap);
        // No panic — round-trip OK.
    }

    // Branch: plan() on an empty (no joins) graph returns Empty.
    #[test]
    fn plan_empty_graph_returns_empty() {
        let g = build_simple_graph();
        let r = g.plan(None);
        assert!(matches!(r, PlanResult::Empty));
    }

    // Branch: plan() with too many flippable joins returns TooComplex.
    #[test]
    fn plan_too_many_returns_too_complex() {
        let g = Arc::new(PlannerGraph::new());
        let src = g.add_source("users", simple_cost_model());
        let root = Arc::new(src.connect(vec![], None, true, None, None));
        g.connections.lock().unwrap().push(root.clone());

        // 10 dummy flippable joins (parent=root, child=root) — exceeds MAX_FLIPPABLE_JOINS.
        for i in 0..(MAX_FLIPPABLE_JOINS + 1) {
            let j = crate::planner::planner_join::arc_join(
                NonTerminusNode::Connection(root.clone()),
                NonTerminusNode::Connection(root.clone()),
                constraint_from_fields(["a"]),
                constraint_from_fields(["b"]),
                true,
                i as u32,
                JoinType::Semi,
            );
            g.joins.lock().unwrap().push(j);
        }

        let term = Arc::new(PlannerTerminus::new(NonTerminusNode::Connection(root)));
        g.set_terminus(term);
        let r = g.plan(None);
        match r {
            PlanResult::TooComplex { flippable_joins } => {
                assert_eq!(flippable_joins, MAX_FLIPPABLE_JOINS + 1);
            }
            _ => panic!("expected TooComplex"),
        }
    }

    // Branch: reset_planning_state clears mutated state.
    #[test]
    fn reset_planning_state_resets() {
        let g = build_simple_graph();
        let conn = g.connections.lock().unwrap()[0].clone();
        conn.propagate_constraints(&[0], Some(&constraint_from_fields(["a"])), None, None);
        g.reset_planning_state();
        let est = conn.estimate_cost(1.0, &[0], None);
        assert_eq!(est.returned_rows, 100.0);
    }
}
