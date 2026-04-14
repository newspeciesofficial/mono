//! Port of `packages/zql/src/planner/planner-debug.ts`.
//!
//! TS file is 536 LOC of JSON-shape types plus a `formatPlannerEvents`
//! pretty-printer. The Rust port models the event types as `PlanDebugEvent`
//! (tagged enum) and an `AccumulatorDebugger` that stores them, matching
//! *observable shape* — not the exact TS string-formatting output. The
//! pretty-printer is ported in a simplified form (attempt-by-attempt lines
//! listing node names, costs, and the final best plan); byte-for-byte
//! parity with the TS formatter is explicitly out of scope for this layer.
//! See module docs on `planner/mod.rs`.

use std::sync::{Arc, Mutex};

use serde_json::Value as Json;
use zero_cache_types::ast::{Condition, Ordering};

use super::planner_constraint::PlannerConstraint;
use super::planner_node::{CostEstimateJson, JoinType};

/// TS `PlanDebugger` interface. `log` takes a [`PlanDebugEvent`] by value
/// because the debugger may choose to retain/own it.
pub trait PlanDebugger: Send + Sync {
    fn log(&self, event: PlanDebugEvent);
}

/// TS union `PlanDebugEvent`. Each variant's fields mirror the TS event
/// type; optional fields become `Option<T>`.
#[derive(Debug, Clone)]
pub enum PlanDebugEvent {
    AttemptStart {
        attempt_number: usize,
        total_attempts: usize,
    },
    NodeCost {
        attempt_number: Option<usize>,
        node_type: &'static str,
        node: String,
        branch_pattern: Vec<usize>,
        downstream_child_selectivity: f64,
        cost_estimate: CostEstimateJson,
        filters: Option<Condition>,
        ordering: Option<Ordering>,
        join_type: Option<JoinType>,
    },
    NodeConstraint {
        attempt_number: Option<usize>,
        node_type: &'static str,
        node: String,
        branch_pattern: Vec<usize>,
        constraint: Option<PlannerConstraint>,
        from: String,
    },
    ConstraintsPropagated {
        attempt_number: usize,
        /// Free-form JSON payload — the connection list.
        connection_constraints: Json,
    },
    PlanComplete {
        attempt_number: usize,
        total_cost: f64,
        flip_pattern: u32,
        join_states: Vec<(String, JoinType)>,
    },
    BestPlanSelected {
        best_attempt_number: usize,
        total_cost: f64,
        flip_pattern: u32,
        join_states: Vec<(String, JoinType)>,
    },
}

/// TS `AccumulatorDebugger`. Collects all events; for tests and
/// programmatic inspection.
#[derive(Default)]
pub struct AccumulatorDebugger {
    inner: Mutex<AccumulatorInner>,
}

#[derive(Default)]
struct AccumulatorInner {
    events: Vec<PlanDebugEvent>,
    current_attempt: usize,
}

impl AccumulatorDebugger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<PlanDebugEvent> {
        self.inner
            .lock()
            .expect("debugger mutex poisoned")
            .events
            .clone()
    }

    /// TS `getEvents<T>(type)` — filter by variant tag, returned as a
    /// cloned Vec.
    pub fn events_of(&self, matcher: impl Fn(&PlanDebugEvent) -> bool) -> Vec<PlanDebugEvent> {
        self.inner
            .lock()
            .expect("debugger mutex poisoned")
            .events
            .iter()
            .filter(|e| matcher(e))
            .cloned()
            .collect()
    }
}

impl PlanDebugger for AccumulatorDebugger {
    fn log(&self, event: PlanDebugEvent) {
        let mut inner = self.inner.lock().expect("debugger mutex poisoned");
        // Track current attempt number (mirrors TS `currentAttempt`).
        let ev = match event {
            PlanDebugEvent::AttemptStart {
                attempt_number,
                total_attempts,
            } => {
                inner.current_attempt = attempt_number;
                PlanDebugEvent::AttemptStart {
                    attempt_number,
                    total_attempts,
                }
            }
            // For node events, inject current attempt_number if missing —
            // matches TS
            // `if event.type === 'node-cost' || 'node-constraint' → event.attemptNumber = current`.
            PlanDebugEvent::NodeCost {
                attempt_number,
                node_type,
                node,
                branch_pattern,
                downstream_child_selectivity,
                cost_estimate,
                filters,
                ordering,
                join_type,
            } => PlanDebugEvent::NodeCost {
                attempt_number: attempt_number.or(Some(inner.current_attempt)),
                node_type,
                node,
                branch_pattern,
                downstream_child_selectivity,
                cost_estimate,
                filters,
                ordering,
                join_type,
            },
            PlanDebugEvent::NodeConstraint {
                attempt_number,
                node_type,
                node,
                branch_pattern,
                constraint,
                from,
            } => PlanDebugEvent::NodeConstraint {
                attempt_number: attempt_number.or(Some(inner.current_attempt)),
                node_type,
                node,
                branch_pattern,
                constraint,
                from,
            },
            other => other,
        };
        inner.events.push(ev);
    }
}

/// Convenience wrapper so callers can pass `Arc<AccumulatorDebugger>` as
/// `&dyn PlanDebugger`.
impl PlanDebugger for Arc<AccumulatorDebugger> {
    fn log(&self, event: PlanDebugEvent) {
        AccumulatorDebugger::log(self, event);
    }
}

/// TS `formatPlannerEvents` (simplified).
///
/// Divergence from TS: we do not attempt byte-for-byte parity; output is
/// human-readable and lists attempts with their node costs and final
/// selected plan. Callers that require the exact TS string should use
/// the TS formatter instead.
pub fn format_planner_events(events: &[PlanDebugEvent]) -> String {
    use std::collections::BTreeMap;
    let mut by_attempt: BTreeMap<usize, Vec<&PlanDebugEvent>> = BTreeMap::new();
    let mut best: Option<&PlanDebugEvent> = None;

    for e in events {
        match e {
            PlanDebugEvent::AttemptStart { attempt_number, .. }
            | PlanDebugEvent::PlanComplete { attempt_number, .. }
            | PlanDebugEvent::ConstraintsPropagated { attempt_number, .. } => {
                by_attempt.entry(*attempt_number).or_default().push(e);
            }
            PlanDebugEvent::NodeCost {
                attempt_number: Some(a),
                ..
            }
            | PlanDebugEvent::NodeConstraint {
                attempt_number: Some(a),
                ..
            } => {
                by_attempt.entry(*a).or_default().push(e);
            }
            PlanDebugEvent::BestPlanSelected { .. } => best = Some(e),
            _ => {}
        }
    }

    let mut out = String::new();
    for (attempt, evs) in &by_attempt {
        out.push_str(&format!("[Attempt {}]\n", attempt + 1));
        for ev in evs {
            match ev {
                PlanDebugEvent::NodeCost {
                    node,
                    cost_estimate,
                    ..
                } => {
                    out.push_str(&format!(
                        "  {}: cost={:.2}, rows={:.2}\n",
                        node, cost_estimate.cost, cost_estimate.returned_rows
                    ));
                }
                PlanDebugEvent::PlanComplete { total_cost, .. } => {
                    out.push_str(&format!("  Plan complete: total={total_cost:.2}\n"));
                }
                _ => {}
            }
        }
    }
    if let Some(PlanDebugEvent::BestPlanSelected {
        best_attempt_number,
        total_cost,
        join_states,
        ..
    }) = best
    {
        out.push_str(&format!(
            "Best plan: attempt {} cost={:.2}\n",
            best_attempt_number + 1,
            total_cost
        ));
        for (name, t) in join_states {
            out.push_str(&format!("  {name}: {t:?}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Branch: AccumulatorDebugger records events in insertion order.
    #[test]
    fn accumulator_records_events() {
        let d = AccumulatorDebugger::new();
        d.log(PlanDebugEvent::AttemptStart {
            attempt_number: 0,
            total_attempts: 2,
        });
        d.log(PlanDebugEvent::PlanComplete {
            attempt_number: 0,
            total_cost: 10.0,
            flip_pattern: 0,
            join_states: vec![],
        });
        assert_eq!(d.events().len(), 2);
    }

    // Branch: NodeCost without attempt_number inherits current_attempt.
    #[test]
    fn node_cost_inherits_attempt() {
        let d = AccumulatorDebugger::new();
        d.log(PlanDebugEvent::AttemptStart {
            attempt_number: 3,
            total_attempts: 4,
        });
        d.log(PlanDebugEvent::NodeCost {
            attempt_number: None,
            node_type: "connection",
            node: "users".to_string(),
            branch_pattern: vec![],
            downstream_child_selectivity: 1.0,
            cost_estimate: CostEstimateJson {
                startup_cost: 0.0,
                scan_est: 100.0,
                cost: 0.0,
                returned_rows: 100.0,
                selectivity: 1.0,
                limit: None,
            },
            filters: None,
            ordering: None,
            join_type: None,
        });
        match &d.events()[1] {
            PlanDebugEvent::NodeCost { attempt_number, .. } => {
                assert_eq!(*attempt_number, Some(3));
            }
            _ => panic!("expected NodeCost"),
        }
    }

    // Branch: formatter emits attempt headers.
    #[test]
    fn format_contains_attempt_headers() {
        let d = AccumulatorDebugger::new();
        d.log(PlanDebugEvent::AttemptStart {
            attempt_number: 0,
            total_attempts: 1,
        });
        d.log(PlanDebugEvent::PlanComplete {
            attempt_number: 0,
            total_cost: 42.0,
            flip_pattern: 0,
            join_states: vec![],
        });
        d.log(PlanDebugEvent::BestPlanSelected {
            best_attempt_number: 0,
            total_cost: 42.0,
            flip_pattern: 0,
            join_states: vec![("j1".to_string(), JoinType::Semi)],
        });
        let s = format_planner_events(&d.events());
        assert!(s.contains("[Attempt 1]"));
        assert!(s.contains("Plan complete"));
        assert!(s.contains("Best plan"));
        assert!(s.contains("j1"));
    }

    // Branch: events_of filters by predicate.
    #[test]
    fn events_of_filters() {
        let d = AccumulatorDebugger::new();
        d.log(PlanDebugEvent::AttemptStart {
            attempt_number: 0,
            total_attempts: 1,
        });
        d.log(PlanDebugEvent::PlanComplete {
            attempt_number: 0,
            total_cost: 1.0,
            flip_pattern: 0,
            join_states: vec![],
        });
        let plan_completes = d.events_of(|e| matches!(e, PlanDebugEvent::PlanComplete { .. }));
        assert_eq!(plan_completes.len(), 1);
    }
}
