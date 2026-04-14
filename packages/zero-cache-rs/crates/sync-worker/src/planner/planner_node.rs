//! Port of `packages/zql/src/planner/planner-node.ts`.
//!
//! Defines the [`PlannerNode`] sum type (Connection / Join / FanOut / FanIn
//! / Terminus) and the [`CostEstimate`] / [`FanoutCostModel`] types every
//! node produces during cost estimation.
//!
//! # Design choices vs. TS
//!
//! - **Sum type, not interface**: TS uses a structural union
//!   `PlannerJoin | PlannerConnection | ...`; Rust uses an enum whose
//!   variants hold `Arc<T>` of the concrete node type.
//! - **Method dispatch**: common methods (`closest_join_or_source`,
//!   `propagate_constraints`, `estimate_cost`,
//!   `propagate_unlimit_from_flipped_join`, `kind`) are implemented on
//!   [`PlannerNode`] with a `match` — one arm per variant.
//! - **Non-terminus upcast**: TS uses `Exclude<PlannerNode, PlannerTerminus>`
//!   as a compile-time guard. Rust expresses this at runtime via
//!   [`NonTerminusNode`] which can always be cheaply upcast to
//!   [`PlannerNode`].

use std::sync::Arc;

use super::planner_connection::PlannerConnection;
use super::planner_constraint::PlannerConstraint;
use super::planner_debug::PlanDebugger;
use super::planner_fan_in::PlannerFanIn;
use super::planner_fan_out::PlannerFanOut;
use super::planner_join::PlannerJoin;
use super::planner_terminus::PlannerTerminus;

/// TS `FanoutEst`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FanoutEst {
    pub fanout: f64,
    pub confidence: FanoutConfidence,
}

/// TS `'high' | 'med' | 'none'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutConfidence {
    High,
    Med,
    None,
}

/// TS `FanoutCostModel: (columns: string[]) => FanoutEst`.
pub type FanoutCostModel = Arc<dyn Fn(&[String]) -> FanoutEst + Send + Sync>;

/// TS `CostEstimate`. Identical field set; `fanout` is a closure.
#[derive(Clone)]
pub struct CostEstimate {
    pub startup_cost: f64,
    pub scan_est: f64,
    /// TS `cost`.
    pub cost: f64,
    /// TS `returnedRows`.
    pub returned_rows: f64,
    /// TS `selectivity`.
    pub selectivity: f64,
    /// TS `limit: number | undefined`.
    pub limit: Option<u64>,
    /// TS `fanout: FanoutCostModel`.
    pub fanout: FanoutCostModel,
}

impl std::fmt::Debug for CostEstimate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CostEstimate")
            .field("startup_cost", &self.startup_cost)
            .field("scan_est", &self.scan_est)
            .field("cost", &self.cost)
            .field("returned_rows", &self.returned_rows)
            .field("selectivity", &self.selectivity)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// Serializable view of [`CostEstimate`] — mirrors
/// `Omit<CostEstimate, 'fanout'>` used in debug logs.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CostEstimateJson {
    #[serde(rename = "startupCost")]
    pub startup_cost: f64,
    #[serde(rename = "scanEst")]
    pub scan_est: f64,
    pub cost: f64,
    #[serde(rename = "returnedRows")]
    pub returned_rows: f64,
    pub selectivity: f64,
    pub limit: Option<u64>,
}

/// TS `omitFanout(cost)`.
pub fn omit_fanout(cost: &CostEstimate) -> CostEstimateJson {
    CostEstimateJson {
        startup_cost: cost.startup_cost,
        scan_est: cost.scan_est,
        cost: cost.cost,
        returned_rows: cost.returned_rows,
        selectivity: cost.selectivity,
        limit: cost.limit,
    }
}

/// Kind tag ≡ TS `NodeType = PlannerNode['kind']`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Connection,
    Join,
    FanOut,
    FanIn,
    Terminus,
}

impl NodeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeKind::Connection => "connection",
            NodeKind::Join => "join",
            NodeKind::FanOut => "fan-out",
            NodeKind::FanIn => "fan-in",
            NodeKind::Terminus => "terminus",
        }
    }
}

/// TS `JoinOrConnection`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOrConnection {
    Join,
    Connection,
}

/// Source descriptor passed as the TS `from` argument to
/// `propagateConstraints`. We carry only what the debugger needs (kind +
/// optional human-readable name).
#[derive(Debug, Clone)]
pub struct FromInfo {
    pub kind: NodeKind,
    pub name: String,
}

impl FromInfo {
    pub fn from_node(node: &PlannerNode) -> Self {
        Self {
            kind: node.kind(),
            name: node.debug_name(),
        }
    }
    pub fn unknown() -> Self {
        Self {
            kind: NodeKind::Terminus, // placeholder; only `name` is used as "unknown"
            name: "unknown".to_string(),
        }
    }
}

/// TS `'semi' | 'flipped'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Semi,
    Flipped,
}

/// TS `'FO' | 'UFO'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanOutType {
    FO,
    UFO,
}

/// TS `'FI' | 'UFI'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanInType {
    FI,
    UFI,
}

/// TS `PlannerNode` (discriminated union).
#[derive(Clone)]
pub enum PlannerNode {
    Connection(Arc<PlannerConnection>),
    Join(Arc<PlannerJoin>),
    FanOut(Arc<PlannerFanOut>),
    FanIn(Arc<PlannerFanIn>),
    Terminus(Arc<PlannerTerminus>),
}

/// TS `Exclude<PlannerNode, PlannerTerminus>`.
#[derive(Clone)]
pub enum NonTerminusNode {
    Connection(Arc<PlannerConnection>),
    Join(Arc<PlannerJoin>),
    FanOut(Arc<PlannerFanOut>),
    FanIn(Arc<PlannerFanIn>),
}

impl NonTerminusNode {
    pub fn into_planner(self) -> PlannerNode {
        match self {
            Self::Connection(c) => PlannerNode::Connection(c),
            Self::Join(j) => PlannerNode::Join(j),
            Self::FanOut(fo) => PlannerNode::FanOut(fo),
            Self::FanIn(fi) => PlannerNode::FanIn(fi),
        }
    }

    pub fn as_planner(&self) -> PlannerNode {
        self.clone().into_planner()
    }
}

impl PlannerNode {
    pub fn kind(&self) -> NodeKind {
        match self {
            Self::Connection(_) => NodeKind::Connection,
            Self::Join(_) => NodeKind::Join,
            Self::FanOut(_) => NodeKind::FanOut,
            Self::FanIn(_) => NodeKind::FanIn,
            Self::Terminus(_) => NodeKind::Terminus,
        }
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        match self {
            Self::Connection(c) => c.closest_join_or_source(),
            Self::Join(j) => j.closest_join_or_source(),
            Self::FanOut(fo) => fo.closest_join_or_source(),
            Self::FanIn(fi) => fi.closest_join_or_source(),
            Self::Terminus(t) => t.closest_join_or_source(),
        }
    }

    /// Readable label for debug events. Mirrors TS `getNodeName`.
    pub fn debug_name(&self) -> String {
        match self {
            Self::Connection(c) => c.name.clone(),
            Self::Join(j) => j.get_name(),
            Self::FanOut(_) => "FO".to_string(),
            Self::FanIn(_) => "FI".to_string(),
            Self::Terminus(_) => "terminus".to_string(),
        }
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
        plan_debugger: Option<&dyn PlanDebugger>,
    ) -> CostEstimate {
        match self {
            Self::Connection(c) => {
                c.estimate_cost(downstream_child_selectivity, branch_pattern, plan_debugger)
            }
            Self::Join(j) => {
                j.estimate_cost(downstream_child_selectivity, branch_pattern, plan_debugger)
            }
            Self::FanOut(fo) => {
                fo.estimate_cost(downstream_child_selectivity, branch_pattern, plan_debugger)
            }
            Self::FanIn(fi) => {
                fi.estimate_cost(downstream_child_selectivity, branch_pattern, plan_debugger)
            }
            // Terminus does not expose the `(selectivity, pattern, debugger)`
            // variant — see `PlannerTerminus::estimate_cost`. It starts the
            // estimation chain from scratch.
            Self::Terminus(t) => t.estimate_cost(plan_debugger),
        }
    }

    pub fn propagate_constraints(
        &self,
        branch_pattern: &[usize],
        constraint: Option<&PlannerConstraint>,
        from: Option<FromInfo>,
        plan_debugger: Option<&dyn PlanDebugger>,
    ) {
        match self {
            Self::Connection(c) => {
                c.propagate_constraints(branch_pattern, constraint, from, plan_debugger)
            }
            Self::Join(j) => {
                j.propagate_constraints(branch_pattern, constraint, from, plan_debugger)
            }
            Self::FanOut(fo) => {
                fo.propagate_constraints(branch_pattern, constraint, from, plan_debugger)
            }
            Self::FanIn(fi) => {
                fi.propagate_constraints(branch_pattern, constraint, from, plan_debugger)
            }
            // Terminus has its own entry-point; callers do not send
            // constraints *to* terminus.
            Self::Terminus(_) => {}
        }
    }

    pub fn propagate_unlimit_from_flipped_join(&self) {
        match self {
            Self::Connection(c) => c.propagate_unlimit_from_flipped_join(),
            Self::Join(j) => j.propagate_unlimit_from_flipped_join(),
            Self::FanOut(fo) => fo.propagate_unlimit_from_flipped_join(),
            Self::FanIn(fi) => fi.propagate_unlimit_from_flipped_join(),
            Self::Terminus(t) => t.propagate_unlimit_from_flipped_join(),
        }
    }

    /// TS `node === other` using `Arc::ptr_eq` on the underlying box.
    pub fn ptr_eq(&self, other: &PlannerNode) -> bool {
        match (self, other) {
            (Self::Connection(a), Self::Connection(b)) => Arc::ptr_eq(a, b),
            (Self::Join(a), Self::Join(b)) => Arc::ptr_eq(a, b),
            (Self::FanOut(a), Self::FanOut(b)) => Arc::ptr_eq(a, b),
            (Self::FanIn(a), Self::FanIn(b)) => Arc::ptr_eq(a, b),
            (Self::Terminus(a), Self::Terminus(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn fanout_none() -> FanoutCostModel {
        Arc::new(|_cols: &[String]| FanoutEst {
            fanout: 1.0,
            confidence: FanoutConfidence::None,
        })
    }

    // Branch: omit_fanout keeps all numeric fields intact.
    #[test]
    fn omit_fanout_strips_function_field() {
        let c = CostEstimate {
            startup_cost: 1.0,
            scan_est: 2.0,
            cost: 3.0,
            returned_rows: 4.0,
            selectivity: 0.5,
            limit: Some(10),
            fanout: fanout_none(),
        };
        let j = omit_fanout(&c);
        assert_eq!(j.startup_cost, 1.0);
        assert_eq!(j.scan_est, 2.0);
        assert_eq!(j.cost, 3.0);
        assert_eq!(j.returned_rows, 4.0);
        assert_eq!(j.selectivity, 0.5);
        assert_eq!(j.limit, Some(10));
    }

    // Branch: CostEstimateJson roundtrips via serde with camelCase keys.
    #[test]
    fn cost_estimate_json_serde_roundtrip() {
        let j = CostEstimateJson {
            startup_cost: 1.5,
            scan_est: 2.5,
            cost: 3.5,
            returned_rows: 4.5,
            selectivity: 1.0,
            limit: None,
        };
        let s = serde_json::to_string(&j).unwrap();
        assert!(s.contains("\"startupCost\""));
        assert!(s.contains("\"scanEst\""));
        assert!(s.contains("\"returnedRows\""));
        let back: CostEstimateJson = serde_json::from_str(&s).unwrap();
        assert_eq!(back, j);
    }

    // Branch: NodeKind::as_str mapping covers every variant.
    #[test]
    fn node_kind_as_str() {
        assert_eq!(NodeKind::Connection.as_str(), "connection");
        assert_eq!(NodeKind::Join.as_str(), "join");
        assert_eq!(NodeKind::FanOut.as_str(), "fan-out");
        assert_eq!(NodeKind::FanIn.as_str(), "fan-in");
        assert_eq!(NodeKind::Terminus.as_str(), "terminus");
    }
}
