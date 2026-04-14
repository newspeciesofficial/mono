//! Port of `packages/zql/src/ivm/filter-push.ts`.
//!
//! Public export ported:
//!
//! - [`filter_push`] — the fetch-path equivalent of
//!   [`crate::ivm::maybe_split_and_push_edit_change::maybe_split_and_push_edit_change`]
//!   for the full [`Change`] enum.  Filters or splits a change based on
//!   a row predicate before forwarding to downstream `Output::push`.
//!
//! TS control flow preserved:
//!
//! ```text
//! if predicate is None:          forward unconditionally
//! else match change.type:
//!   Add | Remove: forward iff predicate(change.node.row)
//!   Child:       forward iff predicate(change.node.row)
//!   Edit:        delegate to maybe_split_and_push_edit_change
//! ```

use zero_cache_types::value::Row;

use crate::ivm::change::Change;
use crate::ivm::maybe_split_and_push_edit_change::maybe_split_and_push_edit_change;
use crate::ivm::operator::{InputBase, Output};
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

/// TS `filterPush(change, output, pusher, predicate?)`.
///
/// When `predicate` is `None`, every change is forwarded.  When set,
/// Add / Remove / Child changes are forwarded only if the predicate
/// accepts `change.node.row`.  Edit changes are delegated to
/// [`maybe_split_and_push_edit_change`] so a presence flip becomes an
/// Add or Remove downstream.
///
/// The returned [`Stream`] borrows `output` mutably for its lifetime,
/// matching the TS generator's delegation of `yield*` to
/// `output.push`.
///
/// ## Parity with TS
///
/// TS's `switch` includes a `default: unreachable(change)` — a call
/// that `throw`s.  Rust's exhaustiveness check for a `match` over
/// [`Change`] makes the equivalent unreachable; we omit the branch.
/// If a new variant is added to [`Change`], the compiler fails and
/// the author must update this function, which is the same outcome as
/// TS's `unreachable` assertion but caught at compile time.
pub fn filter_push<'a>(
    change: Change,
    output: &'a mut dyn Output,
    pusher: &'a dyn InputBase,
    predicate: Option<&(dyn Fn(&Row) -> bool + Send + Sync)>,
) -> Stream<'a, Yield> {
    // Branch: no predicate — pass through unconditionally.
    let Some(predicate) = predicate else {
        return output.push(change, pusher);
    };

    match change {
        // Branch: Add — forward iff predicate(new row).
        Change::Add(_) => {
            if predicate(change.node().row()) {
                output.push(change, pusher)
            } else {
                Box::new(std::iter::empty())
            }
        }
        // Branch: Remove — forward iff predicate(node row).
        Change::Remove(_) => {
            if predicate(change.node().row()) {
                output.push(change, pusher)
            } else {
                Box::new(std::iter::empty())
            }
        }
        // Branch: Child — forward iff predicate(parent row).
        Change::Child(_) => {
            if predicate(change.node().row()) {
                output.push(change, pusher)
            } else {
                Box::new(std::iter::empty())
            }
        }
        // Branch: Edit — split into Add/Remove/Edit/nothing via helper.
        Change::Edit(e) => maybe_split_and_push_edit_change(e, &predicate, output, pusher),
    }
}

// Tiny extension so `change.node().row()` reads naturally.
trait NodeRow {
    fn row(&self) -> &Row;
}
impl NodeRow for crate::ivm::data::Node {
    fn row(&self) -> &Row {
        &self.row
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch-complete coverage mirrors the TS `switch`:
    //!   - No predicate → unconditional forward (every variant).
    //!   - Add, predicate=true → forward.
    //!   - Add, predicate=false → dropped.
    //!   - Remove, predicate=true → forward.
    //!   - Remove, predicate=false → dropped.
    //!   - Child, predicate=true → forward.
    //!   - Child, predicate=false → dropped.
    //!   - Edit → delegates to maybe_split_and_push_edit_change
    //!     (covered via its four sub-branches: both present,
    //!     old-only, new-only, neither).

    use super::*;
    use crate::ivm::change::{
        AddChange, ChangeType, ChildChange, ChildSpec, EditChange, RemoveChange,
    };
    use crate::ivm::data::{Node, make_comparator};
    use crate::ivm::schema::SourceSchema;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::Arc;
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;

    fn make_schema() -> SourceSchema {
        let sort: Ordering = vec![("id".to_string(), Direction::Asc)];
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

    fn row_with(id: i64, flag: bool) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("flag".into(), Some(json!(flag)));
        r
    }

    fn node_of(row: Row) -> Node {
        Node {
            row,
            relationships: IndexMap::new(),
        }
    }

    struct DummyInput {
        schema: SourceSchema,
    }
    impl InputBase for DummyInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {}
    }

    struct Recorder {
        pushes: Vec<(ChangeType, Change)>,
    }
    impl Output for Recorder {
        fn push<'a>(&'a mut self, change: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes.push((change.change_type(), change));
            Box::new(std::iter::empty())
        }
    }

    fn always_true(_row: &Row) -> bool {
        true
    }
    fn flag_true(row: &Row) -> bool {
        matches!(row.get("flag"), Some(Some(v)) if v == &json!(true))
    }

    // Branch: predicate = None — unconditional forward (Add variant).
    #[test]
    fn no_predicate_forwards_add() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Add(AddChange {
            node: node_of(row_with(1, false)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, None).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Add);
    }

    // Branch: predicate = None — unconditional forward (Remove variant).
    #[test]
    fn no_predicate_forwards_remove() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Remove(RemoveChange {
            node: node_of(row_with(1, false)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, None).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Remove);
    }

    // Branch: predicate = None — unconditional forward (Child variant).
    #[test]
    fn no_predicate_forwards_child() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Child(ChildChange {
            node: node_of(row_with(1, false)),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange {
                    node: node_of(row_with(2, false)),
                })),
            },
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, None).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Child);
    }

    // Branch: predicate = None — unconditional forward (Edit variant).
    #[test]
    fn no_predicate_forwards_edit() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Edit(EditChange {
            old_node: node_of(row_with(1, false)),
            node: node_of(row_with(1, true)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, None).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Edit);
    }

    // Branch: Add with predicate=true → forward.
    #[test]
    fn add_predicate_true_forwards() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Add(AddChange {
            node: node_of(row_with(1, true)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Add);
    }

    // Branch: Add with predicate=false → dropped.
    #[test]
    fn add_predicate_false_drops() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Add(AddChange {
            node: node_of(row_with(1, false)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert!(sink.pushes.is_empty());
    }

    // Branch: Remove with predicate=true → forward.
    #[test]
    fn remove_predicate_true_forwards() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Remove(RemoveChange {
            node: node_of(row_with(1, true)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Remove);
    }

    // Branch: Remove with predicate=false → dropped.
    #[test]
    fn remove_predicate_false_drops() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Remove(RemoveChange {
            node: node_of(row_with(1, false)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert!(sink.pushes.is_empty());
    }

    // Branch: Child with predicate=true → forward.
    #[test]
    fn child_predicate_true_forwards() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Child(ChildChange {
            node: node_of(row_with(1, true)),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange {
                    node: node_of(row_with(2, true)),
                })),
            },
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Child);
    }

    // Branch: Child with predicate=false → dropped.
    #[test]
    fn child_predicate_false_drops() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Child(ChildChange {
            node: node_of(row_with(1, false)),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange {
                    node: node_of(row_with(2, false)),
                })),
            },
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert!(sink.pushes.is_empty());
    }

    // Branch: Edit, both old+new present → forwards Edit.
    #[test]
    fn edit_both_present_forwards_edit() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Edit(EditChange {
            old_node: node_of(row_with(1, true)),
            node: node_of(row_with(1, true)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Edit);
    }

    // Branch: Edit, old present, new absent → Remove(old).
    #[test]
    fn edit_old_present_new_absent_pushes_remove() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Edit(EditChange {
            old_node: node_of(row_with(1, true)),
            node: node_of(row_with(1, false)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Remove);
    }

    // Branch: Edit, old absent, new present → Add(new).
    #[test]
    fn edit_old_absent_new_present_pushes_add() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Edit(EditChange {
            old_node: node_of(row_with(1, false)),
            node: node_of(row_with(1, true)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Add);
    }

    // Branch: Edit, both absent → nothing pushed.
    #[test]
    fn edit_both_absent_pushes_nothing() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Edit(EditChange {
            old_node: node_of(row_with(1, false)),
            node: node_of(row_with(1, false)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&flag_true)).collect();
        assert!(sink.pushes.is_empty());
    }

    // Sanity: always_true predicate — ensure path uses predicate branches
    // (Add forwarded).
    #[test]
    fn always_true_predicate_forwards_add() {
        let mut sink = Recorder { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = Change::Add(AddChange {
            node: node_of(row_with(1, false)),
        });
        let _: Vec<_> = filter_push(change, &mut sink, &pusher, Some(&always_true)).collect();
        assert_eq!(sink.pushes.len(), 1);
    }
}
