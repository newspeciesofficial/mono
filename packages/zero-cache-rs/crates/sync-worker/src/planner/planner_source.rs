//! Port of `packages/zql/src/planner/planner-source.ts`.
//!
//! [`PlannerSource`] is a tiny factory: given a sort, optional filter,
//! optional base constraints, and optional limit, it produces a
//! [`PlannerConnection`].

use zero_cache_types::ast::{Condition, Ordering};

use super::planner_connection::{ConnectionCostModel, PlannerConnection};
use super::planner_constraint::PlannerConstraint;

pub struct PlannerSource {
    pub name: String,
    model: ConnectionCostModel,
}

impl PlannerSource {
    pub fn new(name: impl Into<String>, model: ConnectionCostModel) -> Self {
        Self {
            name: name.into(),
            model,
        }
    }

    /// TS `connect(sort, filters, isRoot, baseConstraints?, limit?)`.
    pub fn connect(
        &self,
        sort: Ordering,
        filters: Option<Box<Condition>>,
        is_root: bool,
        base_constraints: Option<PlannerConstraint>,
        limit: Option<u64>,
    ) -> PlannerConnection {
        PlannerConnection::new(
            self.name.clone(),
            self.model.clone(),
            sort,
            filters,
            is_root,
            base_constraints,
            limit,
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_node::{FanoutConfidence, FanoutCostModel, FanoutEst};
    use std::sync::Arc;
    use zero_cache_types::ast::{LiteralValue, NonColumnValue, SimpleOperator, ValuePosition};

    fn simple_cost_model() -> ConnectionCostModel {
        Arc::new(|_t, _s, _f, _c| {
            let fanout: FanoutCostModel = Arc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            });
            crate::planner::planner_connection::CostModelCost {
                startup_cost: 0.0,
                rows: 100.0,
                fanout,
            }
        })
    }

    // Branch: connect with no filter / no constraints.
    #[test]
    fn connect_returns_connection() {
        let s = PlannerSource::new("users", simple_cost_model());
        let c = s.connect(vec![], None, false, None, None);
        assert_eq!(c.table, "users");
    }

    // Branch: connect with simple filter.
    #[test]
    fn connect_with_filter() {
        let s = PlannerSource::new("users", simple_cost_model());
        let cond = Some(Box::new(Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: NonColumnValue::Literal {
                value: LiteralValue::String("123".to_string()),
            },
        }));
        let c = s.connect(vec![], cond, false, None, None);
        assert_eq!(c.table, "users");
    }

    // Branch: multiple connect calls produce independent connections.
    #[test]
    fn multiple_connects_independent() {
        let s = PlannerSource::new("users", simple_cost_model());
        let a = s.connect(vec![], None, false, None, None);
        let b = s.connect(vec![], None, false, None, None);
        // They should be independent — mutate one's constraint, the other
        // is unaffected.
        a.propagate_constraints(
            &[0],
            Some(&crate::planner::planner_constraint::constraint_from_fields(
                ["x"],
            )),
            None,
            None,
        );
        let est_b = b.estimate_cost(1.0, &[0], None);
        assert_eq!(est_b.returned_rows, 100.0);
    }
}
