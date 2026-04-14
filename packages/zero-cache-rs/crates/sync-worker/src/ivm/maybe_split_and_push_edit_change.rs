//! Port of `packages/zql/src/ivm/maybe-split-and-push-edit-change.ts`.
//!
//! Public export ported:
//!
//! - [`maybe_split_and_push_edit_change`] — helper used by the Filter and
//!   Exists operators to conditionally split an [`EditChange`] based on a
//!   predicate evaluated over the old and new rows.
//!
//! The TS function is a generator that `yield*`s one of four branches:
//!
//! 1. `oldWasPresent && newIsPresent`  → re-push the original `Edit`.
//! 2. `oldWasPresent && !newIsPresent` → push `Remove` with `oldNode`.
//! 3. `!oldWasPresent && newIsPresent` → push `Add` with `node`.
//! 4. `!oldWasPresent && !newIsPresent` → emit nothing.
//!
//! In Rust we return a [`Stream<'a, Yield>`] (i.e. the iterator returned by
//! [`Output::push`]). The fourth branch returns an empty iterator.

use zero_cache_types::value::Row;

use crate::ivm::change::{AddChange, Change, EditChange, RemoveChange};
use crate::ivm::operator::{InputBase, Output};
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

/// TS `maybeSplitAndPushEditChange(change, predicate, output, pusher)`.
///
/// Evaluates `predicate` on `change.old_node.row` and `change.node.row`;
/// depending on whether the row was / is present, pushes a variant of
/// [`Change`] through `output`.
///
/// The returned [`Stream`] borrows `output` mutably for its lifetime,
/// matching the TS generator's delegation of `yield*` to `output.push`.
pub fn maybe_split_and_push_edit_change<'a>(
    change: EditChange,
    predicate: &dyn Fn(&Row) -> bool,
    output: &'a mut dyn Output,
    pusher: &'a dyn InputBase,
) -> Stream<'a, Yield> {
    let old_was_present = predicate(&change.old_node.row);
    let new_is_present = predicate(&change.node.row);

    if old_was_present && new_is_present {
        // Branch 1: row presence unchanged → forward the Edit as-is.
        output.push(Change::Edit(change), pusher)
    } else if old_was_present && !new_is_present {
        // Branch 2: row was present but is no longer → Remove(old_node).
        output.push(
            Change::Remove(RemoveChange {
                node: change.old_node,
            }),
            pusher,
        )
    } else if !old_was_present && new_is_present {
        // Branch 3: row was not present but now is → Add(node).
        output.push(Change::Add(AddChange { node: change.node }), pusher)
    } else {
        // Branch 4: row was not present and still isn't → nothing to do.
        Box::new(std::iter::empty())
    }
}

#[cfg(test)]
mod tests {
    //! Branch-complete coverage:
    //!   - Branch 1: oldWasPresent && newIsPresent     → Edit is forwarded.
    //!   - Branch 2: oldWasPresent && !newIsPresent    → Remove(old_node).
    //!   - Branch 3: !oldWasPresent && newIsPresent    → Add(node).
    //!   - Branch 4: !oldWasPresent && !newIsPresent   → empty stream; no push.
    //!   - Predicate is invoked with the old row and the new row exactly once each.

    use super::*;
    use crate::ivm::change::ChangeType;
    use crate::ivm::data::{Node, make_comparator};
    use crate::ivm::schema::SourceSchema;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::Arc;
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;

    // ── Test harness ──────────────────────────────────────────────────

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

    struct DummyInput {
        schema: SourceSchema,
    }
    impl InputBase for DummyInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {}
    }

    /// Output that records every push in order. `push` returns an empty
    /// yield stream (matches TS generators that complete without yielding).
    struct RecordingOutput {
        pushes: Vec<(ChangeType, Change)>,
    }
    impl Output for RecordingOutput {
        fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes.push((change.change_type(), change));
            Box::new(std::iter::empty())
        }
    }

    fn row_with_id_present(id: i64, present: bool) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("present".into(), Some(json!(present)));
        r
    }

    fn node(row: Row) -> Node {
        Node {
            row,
            relationships: IndexMap::new(),
        }
    }

    fn predicate_present(row: &Row) -> bool {
        matches!(row.get("present"), Some(Some(v)) if v == &json!(true))
    }

    // ── Branch 1: oldWasPresent && newIsPresent ───────────────────────
    #[test]
    fn branch_both_present_forwards_edit() {
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = EditChange {
            old_node: node(row_with_id_present(1, true)),
            node: node(row_with_id_present(1, true)),
        };

        let stream =
            maybe_split_and_push_edit_change(change, &predicate_present, &mut sink, &pusher);
        // Drain the stream (empty, but exercising it drives the push).
        let collected: Vec<Yield> = stream.collect();
        assert!(collected.is_empty());

        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Edit);
        match &sink.pushes[0].1 {
            Change::Edit(e) => {
                assert_eq!(e.node.row.get("id"), Some(&Some(json!(1))));
                assert_eq!(e.old_node.row.get("id"), Some(&Some(json!(1))));
            }
            other => panic!("expected Edit, got {other:?}"),
        }
    }

    // ── Branch 2: oldWasPresent && !newIsPresent ──────────────────────
    #[test]
    fn branch_old_present_new_absent_pushes_remove_old_node() {
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = EditChange {
            old_node: node(row_with_id_present(7, true)),
            node: node(row_with_id_present(7, false)),
        };

        let _ = maybe_split_and_push_edit_change(change, &predicate_present, &mut sink, &pusher)
            .collect::<Vec<_>>();

        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Remove);
        match &sink.pushes[0].1 {
            Change::Remove(r) => {
                // The Remove carries the *old* node (the one that was present).
                assert_eq!(r.node.row.get("id"), Some(&Some(json!(7))));
                assert_eq!(r.node.row.get("present"), Some(&Some(json!(true))));
            }
            other => panic!("expected Remove, got {other:?}"),
        }
    }

    // ── Branch 3: !oldWasPresent && newIsPresent ──────────────────────
    #[test]
    fn branch_old_absent_new_present_pushes_add_new_node() {
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = EditChange {
            old_node: node(row_with_id_present(3, false)),
            node: node(row_with_id_present(3, true)),
        };

        let _ = maybe_split_and_push_edit_change(change, &predicate_present, &mut sink, &pusher)
            .collect::<Vec<_>>();

        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Add);
        match &sink.pushes[0].1 {
            Change::Add(a) => {
                // The Add carries the *new* node (the one that is now present).
                assert_eq!(a.node.row.get("id"), Some(&Some(json!(3))));
                assert_eq!(a.node.row.get("present"), Some(&Some(json!(true))));
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    // ── Branch 4: !oldWasPresent && !newIsPresent ─────────────────────
    #[test]
    fn branch_neither_present_pushes_nothing() {
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = EditChange {
            old_node: node(row_with_id_present(9, false)),
            node: node(row_with_id_present(9, false)),
        };

        let collected: Vec<Yield> =
            maybe_split_and_push_edit_change(change, &predicate_present, &mut sink, &pusher)
                .collect();
        assert!(collected.is_empty());
        assert!(sink.pushes.is_empty(), "no push expected in branch 4");
    }

    // Branch: predicate is invoked for both rows. Verified by counting
    // invocations on a closure that records each row.
    #[test]
    fn predicate_invoked_for_old_and_new_rows() {
        use std::cell::RefCell;
        let seen: RefCell<Vec<Row>> = RefCell::new(vec![]);
        let predicate = |row: &Row| -> bool {
            seen.borrow_mut().push(row.clone());
            predicate_present(row)
        };

        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        let change = EditChange {
            old_node: node(row_with_id_present(1, true)),
            node: node(row_with_id_present(2, false)),
        };

        let _ = maybe_split_and_push_edit_change(change, &predicate, &mut sink, &pusher)
            .collect::<Vec<_>>();

        let s = seen.borrow();
        assert_eq!(s.len(), 2, "predicate should run once per row");
        // Order: old first (per TS), then new.
        assert_eq!(s[0].get("id"), Some(&Some(json!(1))));
        assert_eq!(s[1].get("id"), Some(&Some(json!(2))));
    }
}
