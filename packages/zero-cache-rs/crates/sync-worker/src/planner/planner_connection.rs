//! Port of `packages/zql/src/planner/planner-connection.ts`.
//!
//! [`PlannerConnection`] is the cost-bearing leaf of the plan graph. It
//! caches per-branch constraint costs and lets the graph mutate its limit
//! when a parent join is flipped (`unlimit()`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use zero_cache_types::ast::{Condition, Ordering};

use super::planner_constraint::{PlannerConstraint, merge_constraints};
use super::planner_debug::{PlanDebugEvent, PlanDebugger};
use super::planner_node::{
    CostEstimate, FanoutCostModel, FromInfo, JoinOrConnection, PlannerNode, omit_fanout,
};

/// TS `CostModelCost`.
#[derive(Clone)]
pub struct CostModelCost {
    pub startup_cost: f64,
    pub rows: f64,
    pub fanout: FanoutCostModel,
}

impl std::fmt::Debug for CostModelCost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CostModelCost")
            .field("startup_cost", &self.startup_cost)
            .field("rows", &self.rows)
            .finish_non_exhaustive()
    }
}

/// TS `ConnectionCostModel = (table, sort, filters, constraint) => CostModelCost`.
pub type ConnectionCostModel = Arc<
    dyn Fn(&str, &Ordering, Option<&Condition>, Option<&PlannerConstraint>) -> CostModelCost
        + Send
        + Sync,
>;

/// Captured constraint state for snapshotting.
pub type ConstraintMap = HashMap<String, Option<PlannerConstraint>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionSnapshot {
    pub limit: Option<u64>,
    pub constraints: Vec<(String, Option<PlannerConstraintJson>)>,
}

/// JSON-friendly view of a [`PlannerConstraint`] (set of column names).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlannerConstraintJson(pub Vec<String>);

impl From<&PlannerConstraint> for PlannerConstraintJson {
    fn from(c: &PlannerConstraint) -> Self {
        Self(c.keys().cloned().collect())
    }
}

impl From<&PlannerConstraintJson> for PlannerConstraint {
    fn from(j: &PlannerConstraintJson) -> Self {
        let mut c = PlannerConstraint::new();
        for k in &j.0 {
            c.insert(k.clone(), ());
        }
        c
    }
}

pub struct PlannerConnection {
    // Immutable structure
    pub table: String,
    pub name: String,
    sort: Ordering,
    filters: Option<Box<Condition>>,
    model: ConnectionCostModel,
    base_constraints: Option<PlannerConstraint>,
    base_limit: Option<u64>,
    is_root: bool,
    /// TS `selectivity` — set in constructor and never changed.
    selectivity: f64,

    output: Mutex<Option<PlannerNode>>,

    // Mutable planning state
    limit: Mutex<Option<u64>>,
    constraints: Mutex<ConstraintMap>,
    cached_costs: Mutex<HashMap<String, CostEstimate>>,
}

impl PlannerConnection {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: String,
        model: ConnectionCostModel,
        sort: Ordering,
        filters: Option<Box<Condition>>,
        is_root: bool,
        base_constraints: Option<PlannerConstraint>,
        limit: Option<u64>,
        name: Option<String>,
    ) -> Self {
        // TS: compute selectivity for EXISTS child connections (limit set + filters set).
        let selectivity = if let (Some(_lim), Some(filt)) = (limit, filters.as_ref()) {
            let with = (model)(&table, &sort, Some(filt), None);
            let without = (model)(&table, &sort, None, None);
            if without.rows > 0.0 {
                with.rows / without.rows
            } else {
                1.0
            }
        } else {
            1.0
        };

        let name_resolved = name.unwrap_or_else(|| table.clone());
        Self {
            table,
            name: name_resolved,
            sort,
            filters,
            model,
            base_constraints,
            base_limit: limit,
            is_root,
            selectivity,
            output: Mutex::new(None),
            limit: Mutex::new(limit),
            constraints: Mutex::new(HashMap::new()),
            cached_costs: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_output(&self, node: PlannerNode) {
        *self
            .output
            .lock()
            .expect("connection output mutex poisoned") = Some(node);
    }

    pub fn output(&self) -> PlannerNode {
        self.output
            .lock()
            .expect("connection output mutex poisoned")
            .clone()
            .expect("Output not set")
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        JoinOrConnection::Connection
    }

    pub fn limit(&self) -> Option<u64> {
        *self.limit.lock().expect("connection limit mutex poisoned")
    }

    /// Direct setter — used by the planner-graph snapshot restore path.
    /// Mirrors TS `c.limit = state.connections[i].limit`.
    pub fn set_limit(&self, limit: Option<u64>) {
        *self.limit.lock().expect("connection limit mutex poisoned") = limit;
    }

    pub fn selectivity(&self) -> f64 {
        self.selectivity
    }

    pub fn propagate_constraints(
        &self,
        path: &[usize],
        constraint: Option<&PlannerConstraint>,
        from: Option<FromInfo>,
        plan_debugger: Option<&dyn PlanDebugger>,
    ) {
        let key = path_key(path);
        {
            let mut cs = self.constraints.lock().expect("constraints mutex poisoned");
            cs.insert(key.clone(), constraint.cloned());
        }
        // Constraints changed → invalidate cost cache.
        self.cached_costs
            .lock()
            .expect("cached costs mutex poisoned")
            .clear();

        if let Some(d) = plan_debugger {
            d.log(PlanDebugEvent::NodeConstraint {
                attempt_number: None,
                node_type: "connection",
                node: self.name.clone(),
                branch_pattern: path.to_vec(),
                constraint: constraint.cloned(),
                from: from
                    .as_ref()
                    .map(|f| f.kind.as_str().to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
            });
        }
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
        plan_debugger: Option<&dyn PlanDebugger>,
    ) -> CostEstimate {
        let key = path_key(branch_pattern);

        // Cache hit
        if let Some(cached) = self
            .cached_costs
            .lock()
            .expect("cached costs mutex poisoned")
            .get(&key)
            .cloned()
        {
            return cached;
        }

        // Cache miss — compute.
        let constraint = self
            .constraints
            .lock()
            .expect("constraints mutex poisoned")
            .get(&key)
            .cloned()
            .unwrap_or(None);

        let merged = merge_constraints(self.base_constraints.as_ref(), constraint.as_ref());
        let cost = (self.model)(
            &self.table,
            &self.sort,
            self.filters.as_deref(),
            merged.as_ref(),
        );
        let lim = self.limit();
        let scan_est = match lim {
            None => cost.rows,
            Some(l) => cost.rows.min(l as f64 / downstream_child_selectivity),
        };
        let result = CostEstimate {
            startup_cost: cost.startup_cost,
            scan_est,
            cost: 0.0,
            returned_rows: cost.rows,
            selectivity: self.selectivity,
            limit: lim,
            fanout: cost.fanout.clone(),
        };
        self.cached_costs
            .lock()
            .expect("cached costs mutex poisoned")
            .insert(key, result.clone());

        if let Some(d) = plan_debugger {
            d.log(PlanDebugEvent::NodeCost {
                attempt_number: None,
                node_type: "connection",
                node: self.name.clone(),
                branch_pattern: branch_pattern.to_vec(),
                downstream_child_selectivity,
                cost_estimate: omit_fanout(&result),
                filters: self.filters.as_deref().cloned(),
                ordering: Some(self.sort.clone()),
                join_type: None,
            });
        }
        result
    }

    pub fn unlimit(&self) {
        // TS: root connections cannot be unlimited.
        if self.is_root {
            return;
        }
        let mut l = self.limit.lock().expect("connection limit mutex poisoned");
        if l.is_some() {
            *l = None;
            // TS comment: limit changes do not impact connection costs.
            // No cache invalidation needed.
        }
    }

    pub fn propagate_unlimit_from_flipped_join(&self) {
        self.unlimit();
    }

    pub fn reset(&self) {
        self.constraints
            .lock()
            .expect("constraints mutex poisoned")
            .clear();
        *self.limit.lock().expect("connection limit mutex poisoned") = self.base_limit;
        self.cached_costs
            .lock()
            .expect("cached costs mutex poisoned")
            .clear();
    }

    pub fn capture_constraints(&self) -> ConstraintMap {
        self.constraints
            .lock()
            .expect("constraints mutex poisoned")
            .clone()
    }

    pub fn restore_constraints(&self, c: &ConstraintMap) {
        let mut cs = self.constraints.lock().expect("constraints mutex poisoned");
        cs.clear();
        for (k, v) in c {
            cs.insert(k.clone(), v.clone());
        }
        self.cached_costs
            .lock()
            .expect("cached costs mutex poisoned")
            .clear();
    }

    /// TS `getConstraintsForDebug()` — returns a JSON-friendly map.
    pub fn get_constraints_for_debug(&self) -> Vec<(String, Option<PlannerConstraint>)> {
        self.constraints
            .lock()
            .expect("constraints mutex poisoned")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn get_filters_for_debug(&self) -> Option<Condition> {
        self.filters.as_deref().cloned()
    }

    pub fn get_sort_for_debug(&self) -> Ordering {
        self.sort.clone()
    }

    pub fn get_constraint_costs_for_debug(&self) -> Vec<(String, CostEstimate)> {
        self.cached_costs
            .lock()
            .expect("cached costs mutex poisoned")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

fn path_key(path: &[usize]) -> String {
    let mut s = String::new();
    for (i, n) in path.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&n.to_string());
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_constraint::constraint_from_fields;
    use crate::planner::planner_node::{FanoutConfidence, FanoutEst};

    fn simple_cost_model() -> ConnectionCostModel {
        Arc::new(|_t, _s, _f, constraint| {
            let n = constraint.map(|c| c.len()).unwrap_or(0) as f64;
            let rows = (100.0 - n * 10.0).max(1.0);
            let fanout: FanoutCostModel = Arc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            });
            CostModelCost {
                startup_cost: 0.0,
                rows,
                fanout,
            }
        })
    }

    fn make_conn() -> PlannerConnection {
        PlannerConnection::new(
            "users".to_string(),
            simple_cost_model(),
            vec![],
            None,
            false,
            None,
            None,
            None,
        )
    }

    // Branch: estimate_cost no constraints → BASE_COST.
    #[test]
    fn estimate_no_constraints() {
        let c = make_conn();
        let r = c.estimate_cost(1.0, &[], None);
        assert_eq!(r.returned_rows, 100.0);
        assert_eq!(r.selectivity, 1.0);
        assert!(r.limit.is_none());
    }

    // Branch: estimate_cost reduces with constraint.
    #[test]
    fn estimate_with_one_constraint() {
        let c = make_conn();
        c.propagate_constraints(&[0], Some(&constraint_from_fields(["userId"])), None, None);
        let r = c.estimate_cost(1.0, &[0], None);
        assert_eq!(r.returned_rows, 90.0);
    }

    // Branch: estimate_cost with multi-constraint reduces further.
    #[test]
    fn estimate_with_two_constraints() {
        let c = make_conn();
        c.propagate_constraints(
            &[0],
            Some(&constraint_from_fields(["userId", "postId"])),
            None,
            None,
        );
        let r = c.estimate_cost(1.0, &[0], None);
        assert_eq!(r.returned_rows, 80.0);
    }

    // Branch: per-branch caching keeps independent costs per pattern.
    #[test]
    fn multiple_branch_patterns_independent() {
        let c = make_conn();
        c.propagate_constraints(&[0], Some(&constraint_from_fields(["userId"])), None, None);
        c.propagate_constraints(&[1], Some(&constraint_from_fields(["postId"])), None, None);
        let r0 = c.estimate_cost(1.0, &[0], None);
        let r1 = c.estimate_cost(1.0, &[1], None);
        assert_eq!(r0.returned_rows, 90.0);
        assert_eq!(r1.returned_rows, 90.0);
    }

    // Branch: reset clears constraints and limit.
    #[test]
    fn reset_clears_state() {
        let c = make_conn();
        c.propagate_constraints(&[0], Some(&constraint_from_fields(["userId"])), None, None);
        let before = c.estimate_cost(1.0, &[0], None);
        assert_eq!(before.returned_rows, 90.0);
        c.reset();
        let after = c.estimate_cost(1.0, &[0], None);
        assert_eq!(after.returned_rows, 100.0);
    }

    // Branch: capture / restore constraints round-trips.
    #[test]
    fn capture_and_restore_constraints() {
        let c = make_conn();
        c.propagate_constraints(&[0], Some(&constraint_from_fields(["userId"])), None, None);
        let snap = c.capture_constraints();
        c.reset();
        c.restore_constraints(&snap);
        let r = c.estimate_cost(1.0, &[0], None);
        assert_eq!(r.returned_rows, 90.0);
    }

    // Branch: cached cost path returns same instance (pointer equal would
    // require Arc; we verify equal numbers).
    #[test]
    fn estimate_cache_stable() {
        let c = make_conn();
        c.propagate_constraints(&[0], Some(&constraint_from_fields(["userId"])), None, None);
        let a = c.estimate_cost(1.0, &[0], None);
        let b = c.estimate_cost(1.0, &[0], None);
        assert_eq!(a.returned_rows, b.returned_rows);
    }

    // Branch: unlimit on non-root clears limit.
    #[test]
    fn unlimit_clears_limit_on_non_root() {
        let conn = PlannerConnection::new(
            "users".to_string(),
            simple_cost_model(),
            vec![],
            None,
            false,
            None,
            Some(5),
            None,
        );
        assert_eq!(conn.limit(), Some(5));
        conn.unlimit();
        assert!(conn.limit().is_none());
    }

    // Branch: unlimit on root keeps limit.
    #[test]
    fn unlimit_noop_on_root() {
        let conn = PlannerConnection::new(
            "users".to_string(),
            simple_cost_model(),
            vec![],
            None,
            true,
            None,
            Some(5),
            None,
        );
        conn.unlimit();
        assert_eq!(conn.limit(), Some(5));
    }

    // Branch: selectivity computed when limit + filters present.
    #[test]
    fn selectivity_for_limited_filtered() {
        let model: ConnectionCostModel = Arc::new(|_t, _s, filters, _c| {
            let fanout: FanoutCostModel = Arc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            });
            CostModelCost {
                startup_cost: 0.0,
                rows: if filters.is_some() { 50.0 } else { 100.0 },
                fanout,
            }
        });
        let conn = PlannerConnection::new(
            "users".to_string(),
            model,
            vec![],
            Some(Box::new(Condition::Simple {
                op: zero_cache_types::ast::SimpleOperator::Eq,
                left: zero_cache_types::ast::ValuePosition::Column {
                    name: "x".to_string(),
                },
                right: zero_cache_types::ast::NonColumnValue::Literal {
                    value: zero_cache_types::ast::LiteralValue::Number(1.0),
                },
            })),
            false,
            None,
            Some(1),
            None,
        );
        assert!((conn.selectivity() - 0.5).abs() < 1e-9);
    }

    // Branch: selectivity defaults to 1.0 when no limit / no filters.
    #[test]
    fn selectivity_defaults_to_one() {
        let c = make_conn();
        assert_eq!(c.selectivity(), 1.0);
    }

    // Branch: PlannerConstraintJson roundtrip.
    #[test]
    fn constraint_json_roundtrip() {
        let c = constraint_from_fields(["a", "b"]);
        let j: PlannerConstraintJson = (&c).into();
        let s = serde_json::to_string(&j).unwrap();
        let back: PlannerConstraintJson = serde_json::from_str(&s).unwrap();
        let c2: PlannerConstraint = (&back).into();
        let keys: Vec<&str> = c2.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["a", "b"]);
    }
}
