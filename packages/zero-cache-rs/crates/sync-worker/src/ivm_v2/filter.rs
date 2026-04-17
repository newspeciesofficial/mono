//! Filter operator — ivm_v2.
//!
//! Stateless predicate filter. Emits the input change iff the predicate
//! accepts it. `Edit` changes are split into `Remove` / `Add` / `Edit` /
//! nothing depending on whether the predicate matches the old and new rows.
//!
//! Simplifications vs `ivm/filter.rs`:
//! - No `FilterInput` / `FilterOutput` / `FilterOperator` trait hierarchy
//!   (those existed to thread the `'yield'` sentinel through sub-graphs).
//! - No `FilterStart` / `FilterEnd` adapter operators.
//! - No `Arc<Self>`, `Mutex`, or back-edge structs.
//! - `push(&mut self, Change) -> impl Iterator<Item=Change>` — zero or
//!   one output per input change.

use std::sync::Arc;

use zero_cache_types::value::Row;

use super::change::{AddChange, Change, Node, RemoveChange, SourceSchema};
use super::operator::{FetchRequest, Input, InputBase, Operator};

/// TS `predicate: (row: Row) => boolean`.
pub type Predicate = Arc<dyn Fn(&Row) -> bool + Send + Sync>;

pub struct Filter {
    input: Box<dyn Input>,
    predicate: Predicate,
    /// Schema cached at construction.
    schema: SourceSchema,
}

impl Filter {
    pub fn new(input: Box<dyn Input>, predicate: Predicate) -> Self {
        let schema = input.get_schema().clone();
        Self {
            input,
            predicate,
            schema,
        }
    }

    /// `push`-side predicate dispatch.
    ///
    /// Returns `Some(change)` when the change should be forwarded,
    /// `None` when dropped. `Edit` changes split across the predicate.
    fn filter_change(&self, change: Change) -> Option<Change> {
        match change {
            Change::Add(AddChange { ref node }) => {
                if (self.predicate)(&node.row) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Remove(RemoveChange { ref node }) => {
                if (self.predicate)(&node.row) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Child(ref c) => {
                if (self.predicate)(&c.node.row) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Edit(edit) => maybe_split_edit(edit, &*self.predicate),
        }
    }
}

impl InputBase for Filter {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.input.destroy();
    }
}

impl Input for Filter {
    /// Fetch: pass rows through unchanged, dropping ones that fail the predicate.
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        let predicate = Arc::clone(&self.predicate);
        let upstream = self.input.fetch(req);
        Box::new(upstream.filter(move |node| predicate(&node.row)))
    }
}

impl Operator for Filter {
    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!(
                "[ivm:rs:filter:push] change={}",
                change_name(&change)
            );
        }
        let filtered = self.filter_change(change);
        Box::new(filtered.into_iter())
    }
}

/// Split an `EditChange` based on the predicate's view of old vs new rows.
///
/// TS `maybeSplitAndPushEditChange`. Four branches:
/// - both rows pass    → forward as `Edit`
/// - only old passes   → `Remove(old_node)`
/// - only new passes   → `Add(node)`
/// - neither passes    → nothing
pub fn maybe_split_edit(
    edit: crate::ivm::change::EditChange,
    predicate: &(dyn Fn(&Row) -> bool + Send + Sync),
) -> Option<Change> {
    let old_was_present = predicate(&edit.old_node.row);
    let new_is_present = predicate(&edit.node.row);

    match (old_was_present, new_is_present) {
        (true, true) => Some(Change::Edit(edit)),
        (true, false) => Some(Change::Remove(RemoveChange { node: edit.old_node })),
        (false, true) => Some(Change::Add(AddChange { node: edit.node })),
        (false, false) => None,
    }
}

fn change_name(c: &Change) -> &'static str {
    match c {
        Change::Add(_) => "Add",
        Change::Remove(_) => "Remove",
        Change::Child(_) => "Child",
        Change::Edit(_) => "Edit",
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Coverage matches existing `ivm::filter_push` + `ivm::filter` tests:
    //! - No-predicate case is N/A (we always have one).
    //! - Add / Remove / Child: predicate true forwards, false drops.
    //! - Edit: each of the four predicate-combination branches.
    //! - Fetch: drops rows failing predicate.
    //! - Destroy delegates.
    //! - get_schema delegates.
    use super::*;
    use crate::ivm::change::{AddChange, ChangeType, ChildChange, ChildSpec, EditChange, RemoveChange};
    use crate::ivm::data::{make_comparator, Node};
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering as AtomOrdering};
    use std::sync::Arc as StdArc;
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;

    fn schema() -> SourceSchema {
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
        SourceSchema {
            table_name: "t".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }
    fn row(id: i64, flag: bool) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("flag".into(), Some(json!(flag)));
        r
    }
    fn node_of(r: Row) -> Node {
        Node {
            row: r,
            relationships: IndexMap::new(),
        }
    }

    struct StubInput {
        rows: Vec<Row>,
        schema: SourceSchema,
        destroyed: StdArc<AtomicBool>,
    }
    impl InputBase for StubInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {
            self.destroyed.store(true, AtomOrdering::SeqCst);
        }
    }
    impl Input for StubInput {
        fn fetch<'a>(&'a mut self, _req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
            let rows = self.rows.clone();
            Box::new(rows.into_iter().map(node_of))
        }
    }
    fn pred_flag_true() -> Predicate {
        Arc::new(|r: &Row| matches!(r.get("flag"), Some(Some(v)) if v == &json!(true)))
    }
    fn mk(predicate: Predicate) -> (Filter, StdArc<AtomicBool>) {
        let flag = StdArc::new(AtomicBool::new(false));
        let input = StubInput {
            rows: vec![],
            schema: schema(),
            destroyed: StdArc::clone(&flag),
        };
        (Filter::new(Box::new(input), predicate), flag)
    }
    fn mk_with_rows(predicate: Predicate, rows: Vec<Row>) -> Filter {
        let input = StubInput {
            rows,
            schema: schema(),
            destroyed: StdArc::new(AtomicBool::new(false)),
        };
        Filter::new(Box::new(input), predicate)
    }
    fn kind(c: &Change) -> ChangeType {
        match c {
            Change::Add(_) => ChangeType::Add,
            Change::Remove(_) => ChangeType::Remove,
            Change::Child(_) => ChangeType::Child,
            Change::Edit(_) => ChangeType::Edit,
        }
    }

    // ── push-side ──────────────────────────────────────────────────
    #[test]
    fn add_predicate_true_forwards() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Add(AddChange {
                node: node_of(row(1, true)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Add);
    }

    #[test]
    fn add_predicate_false_drops() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Add(AddChange {
                node: node_of(row(1, false)),
            }))
            .collect();
        assert!(out.is_empty());
    }

    #[test]
    fn remove_predicate_true_forwards() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Remove(RemoveChange {
                node: node_of(row(1, true)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Remove);
    }

    #[test]
    fn remove_predicate_false_drops() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Remove(RemoveChange {
                node: node_of(row(1, false)),
            }))
            .collect();
        assert!(out.is_empty());
    }

    #[test]
    fn child_predicate_true_forwards() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Child(ChildChange {
                node: node_of(row(1, true)),
                child: ChildSpec {
                    relationship_name: "rel".into(),
                    change: Box::new(Change::Add(AddChange {
                        node: node_of(row(2, true)),
                    })),
                },
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Child);
    }

    #[test]
    fn child_predicate_false_drops() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Child(ChildChange {
                node: node_of(row(1, false)),
                child: ChildSpec {
                    relationship_name: "rel".into(),
                    change: Box::new(Change::Add(AddChange {
                        node: node_of(row(2, false)),
                    })),
                },
            }))
            .collect();
        assert!(out.is_empty());
    }

    // ── Edit splitting — four branches ────────────────────────────
    #[test]
    fn edit_both_pass_forwards_edit() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Edit(EditChange {
                old_node: node_of(row(1, true)),
                node: node_of(row(1, true)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Edit);
    }

    #[test]
    fn edit_old_pass_new_fail_pushes_remove_old() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Edit(EditChange {
                old_node: node_of(row(7, true)),
                node: node_of(row(7, false)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Remove);
        if let Change::Remove(r) = &out[0] {
            assert_eq!(r.node.row.get("flag"), Some(&Some(json!(true))));
        }
    }

    #[test]
    fn edit_old_fail_new_pass_pushes_add_new() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Edit(EditChange {
                old_node: node_of(row(3, false)),
                node: node_of(row(3, true)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Add);
        if let Change::Add(a) = &out[0] {
            assert_eq!(a.node.row.get("flag"), Some(&Some(json!(true))));
        }
    }

    #[test]
    fn edit_both_fail_nothing() {
        let (mut f, _) = mk(pred_flag_true());
        let out: Vec<Change> = f
            .push(Change::Edit(EditChange {
                old_node: node_of(row(9, false)),
                node: node_of(row(9, false)),
            }))
            .collect();
        assert!(out.is_empty());
    }

    // ── fetch-side ────────────────────────────────────────────────
    #[test]
    fn fetch_drops_non_matching_rows() {
        let mut f = mk_with_rows(
            pred_flag_true(),
            vec![row(1, true), row(2, false), row(3, true), row(4, false)],
        );
        let out: Vec<Node> = f.fetch(FetchRequest::default()).collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].row.get("id"), Some(&Some(json!(1))));
        assert_eq!(out[1].row.get("id"), Some(&Some(json!(3))));
    }

    // ── lifecycle ─────────────────────────────────────────────────
    #[test]
    fn destroy_delegates() {
        let (mut f, flag) = mk(pred_flag_true());
        f.destroy();
        assert!(flag.load(AtomOrdering::SeqCst));
    }

    #[test]
    fn get_schema_returns_cached() {
        let (f, _) = mk(pred_flag_true());
        assert_eq!(f.get_schema().table_name, "t");
    }
}
