//! Port of `packages/zql/src/planner/planner-constraint.ts`.
//!
//! `PlannerConstraint` is TS `Record<string, undefined>` — a set of column
//! names whose runtime values are not known at planning time but whose
//! presence at runtime is. We model it as an ordered map of column name → ()
//! using [`IndexMap`] so iteration order is preserved (this matches the
//! `Object.entries` ordering that `planner-join.ts` relies on when
//! translating constraints between parent and child spaces).

use indexmap::IndexMap;

/// TS `PlannerConstraint = Record<string, undefined>`.
///
/// Iteration order matches insertion order (important: parent↔child index
/// alignment in [`super::planner_join`] depends on it).
pub type PlannerConstraint = IndexMap<String, ()>;

/// TS `mergeConstraints(a, b)`.
///
/// - Both `None` → `None`.
/// - One `None` → return the other.
/// - Both `Some` → shallow merge; `b`'s keys overwrite `a`'s (TS `{...a, ...b}`).
pub fn merge_constraints(
    a: Option<&PlannerConstraint>,
    b: Option<&PlannerConstraint>,
) -> Option<PlannerConstraint> {
    match (a, b) {
        // Branch: both undefined.
        (None, None) => None,
        // Branch: first undefined → return second.
        (None, Some(b)) => Some(b.clone()),
        // Branch: second undefined → return first.
        (Some(a), None) => Some(a.clone()),
        // Branch: both present → shallow merge, b wins on conflict.
        (Some(a), Some(b)) => {
            let mut merged: PlannerConstraint = a.clone();
            for (k, v) in b.iter() {
                merged.insert(k.clone(), *v);
            }
            Some(merged)
        }
    }
}

/// Convenience helper: build a [`PlannerConstraint`] from a slice of
/// column names.
pub fn constraint_from_fields<I, S>(fields: I) -> PlannerConstraint
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut c = PlannerConstraint::new();
    for f in fields {
        c.insert(f.into(), ());
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    // Branch: both undefined returns undefined.
    #[test]
    fn both_undefined_returns_none() {
        assert!(merge_constraints(None, None).is_none());
    }

    // Branch: first undefined returns second.
    #[test]
    fn first_undefined_returns_second() {
        let b = constraint_from_fields(["a"]);
        let merged = merge_constraints(None, Some(&b)).unwrap();
        assert!(merged.contains_key("a"));
    }

    // Branch: second undefined returns first.
    #[test]
    fn second_undefined_returns_first() {
        let a = constraint_from_fields(["a"]);
        let merged = merge_constraints(Some(&a), None).unwrap();
        assert!(merged.contains_key("a"));
    }

    // Branch: merges non-overlapping constraints.
    #[test]
    fn merges_non_overlapping() {
        let a = constraint_from_fields(["a"]);
        let b = constraint_from_fields(["b"]);
        let merged = merge_constraints(Some(&a), Some(&b)).unwrap();
        assert_eq!(merged.len(), 2);
        assert!(merged.contains_key("a"));
        assert!(merged.contains_key("b"));
    }

    // Branch: second overwrites first for same key (same value — idempotent).
    #[test]
    fn overlap_keeps_single_key() {
        let a = constraint_from_fields(["a"]);
        let b = constraint_from_fields(["a"]);
        let merged = merge_constraints(Some(&a), Some(&b)).unwrap();
        assert_eq!(merged.len(), 1);
        assert!(merged.contains_key("a"));
    }

    // Branch: complex merge with partial overlap preserves insertion order.
    #[test]
    fn complex_merge_with_overlap() {
        let a = constraint_from_fields(["a", "b", "c"]);
        let b = constraint_from_fields(["b", "d"]);
        let merged = merge_constraints(Some(&a), Some(&b)).unwrap();
        let keys: Vec<&str> = merged.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["a", "b", "c", "d"]);
    }

    // Branch: iteration order stable.
    #[test]
    fn iteration_order_matches_insertion() {
        let c = constraint_from_fields(["z", "a", "m"]);
        let keys: Vec<&str> = c.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["z", "a", "m"]);
    }
}
