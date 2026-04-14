//! Port of `packages/zql/src/ivm/push-accumulated.ts`.
//!
//! Public exports ported:
//!
//! - [`push_accumulated_changes`] — drains accumulated `Change`s from a
//!   fan-out/fan-in sub-graph (`OR` expansion) down to a single change
//!   per-type, then pushes exactly one representative change through
//!   `Output`. The TS module-level docstring is reproduced above the
//!   function.
//! - [`merge_relationships`] — combines two `Change`s by unioning their
//!   relationship maps (left wins on key collision). Mirrors TS
//!   `mergeRelationships`.
//! - [`make_add_empty_relationships`] — produces a closure that fills in
//!   empty relationship streams for any relationship declared in the
//!   [`SourceSchema`] but missing on a given `Change`. Mirrors TS
//!   `makeAddEmptyRelationships`.
//! - [`merge_empty`] — fills in empty relationship factories for every
//!   relationship name missing from a `relationships` map, in place.
//!   Mirrors TS `mergeEmpty`.
//!
//! ## Generator → iterator
//!
//! TS `yield* output.push(...)` becomes "return the iterator from
//! `output.push(...)`" because every branch yield-delegates exactly once
//! with no post-processing. The returned [`Stream<'a, Yield>`] borrows
//! `output` mutably for its lifetime.
//!
//! ## Relationship merging — key-collision semantics
//!
//! TS uses `{...right.relationships, ...left.relationships}`. In JS, later
//! spreads overwrite earlier ones — so **left wins** on collision. Using
//! [`IndexMap::extend`] with `right` first then `left` reproduces this.
//!
//! ## Panics
//!
//! Every `assert(...)` in TS becomes a `panic!` with the same message,
//! and every `unreachable()` becomes `unreachable!`. Callers that violate
//! the fan-out/fan-in contract should fail loudly and identically to TS.

use indexmap::IndexMap;

use crate::ivm::change::{AddChange, Change, ChangeType, ChildChange, EditChange, RemoveChange};
use crate::ivm::data::{Node, NodeOrYield, RelationshipFactory};
use crate::ivm::operator::{InputBase, Output};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

/// Closure type for the TS `mergeRelationships` parameter.
pub type MergeRelationshipsFn<'f> = dyn Fn(Change, Change) -> Change + 'f;

/// Closure type for the TS `addEmptyRelationships` parameter.
pub type AddEmptyRelationshipsFn<'f> = dyn Fn(Change) -> Change + 'f;

/// TS `pushAccumulatedChanges(accumulatedPushes, output, pusher,
/// fanOutChangeType, mergeRelationships, addEmptyRelationships)`.
///
/// See the TS docstring in `push-accumulated.ts` for the full contract.
/// Briefly:
///   - `fan_out_change_type == Remove` → exactly one `Remove` expected out.
///   - `fan_out_change_type == Add`    → exactly one `Add` expected out.
///   - `fan_out_change_type == Edit`   → adds/removes/edits may combine.
///   - `fan_out_change_type == Child`  → at most one `child` plus optional
///     add/remove conversions; at most 2 types total.
///
/// `accumulated_pushes` is drained (`accumulatedPushes.length = 0` in TS)
/// — we take it by `&mut` and `.clear()` it before we finish analysing.
pub fn push_accumulated_changes<'a>(
    accumulated_pushes: &mut Vec<Change>,
    output: &'a mut dyn Output,
    pusher: &'a dyn InputBase,
    fan_out_change_type: ChangeType,
    merge_relationships_cb: &dyn Fn(Change, Change) -> Change,
    add_empty_relationships: &dyn Fn(Change) -> Change,
) -> Stream<'a, Yield> {
    // Branch: no forks produced any change → nothing to push.
    if accumulated_pushes.is_empty() {
        return Box::new(std::iter::empty());
    }

    // Collapse down to a single change per type.
    let mut candidates_to_push: IndexMap<ChangeType, Change> = IndexMap::new();
    for change in accumulated_pushes.drain(..) {
        let ty = change.change_type();

        if fan_out_change_type == ChangeType::Child && ty != ChangeType::Child {
            // TS assert: at most one non-child per type when fan-out is child.
            assert!(
                !candidates_to_push.contains_key(&ty),
                "Fan-in:child expected at most one {ty:?} when fan-out is of type child"
            );
        }

        let merged = match candidates_to_push.shift_remove(&ty) {
            Some(existing) => merge_relationships_cb(existing, change),
            None => change,
        };
        candidates_to_push.insert(ty, merged);
    }

    let types: Vec<ChangeType> = candidates_to_push.keys().copied().collect();

    match fan_out_change_type {
        // ── remove → only removes out ────────────────────────────────
        ChangeType::Remove => {
            assert!(
                types.len() == 1 && types[0] == ChangeType::Remove,
                "Fan-in:remove expected all removes"
            );
            let c = candidates_to_push
                .shift_remove(&ChangeType::Remove)
                .expect("candidate map must contain Remove");
            output.push(add_empty_relationships(c), pusher)
        }
        // ── add → only adds out ─────────────────────────────────────
        ChangeType::Add => {
            assert!(
                types.len() == 1 && types[0] == ChangeType::Add,
                "Fan-in:add expected all adds"
            );
            let c = candidates_to_push
                .shift_remove(&ChangeType::Add)
                .expect("candidate map must contain Add");
            output.push(add_empty_relationships(c), pusher)
        }
        // ── edit → add/remove/edit mix ──────────────────────────────
        ChangeType::Edit => {
            assert!(
                types
                    .iter()
                    .all(|t| matches!(t, ChangeType::Add | ChangeType::Remove | ChangeType::Edit)),
                "Fan-in:edit expected all adds, removes, or edits"
            );
            let mut add_change = candidates_to_push.shift_remove(&ChangeType::Add);
            let mut remove_change = candidates_to_push.shift_remove(&ChangeType::Remove);
            let mut edit_change = candidates_to_push.shift_remove(&ChangeType::Edit);

            // Sub-branch A: `edit` supersedes add/remove — merge both in.
            if let Some(mut ec) = edit_change.take() {
                if let Some(ac) = add_change.take() {
                    ec = merge_relationships_cb(ec, ac);
                }
                if let Some(rc) = remove_change.take() {
                    ec = merge_relationships_cb(ec, rc);
                }
                return output.push(add_empty_relationships(ec), pusher);
            }

            // Sub-branch B: no edit, but both add and remove → reconstruct edit.
            if add_change.is_some() && remove_change.is_some() {
                let ac = add_change.expect("checked some");
                let rc = remove_change.expect("checked some");
                let synth = Change::Edit(EditChange {
                    node: ac.node().clone(),
                    old_node: rc.node().clone(),
                });
                return output.push(add_empty_relationships(synth), pusher);
            }

            // Sub-branch C: exactly one of add/remove — forward it.
            let only = add_change
                .or(remove_change)
                .expect("Fan-in:edit expected at least add or remove when edit absent");
            output.push(add_empty_relationships(only), pusher)
        }
        // ── child → child precedence, else at most one add OR remove ─
        ChangeType::Child => {
            assert!(
                types
                    .iter()
                    .all(|t| matches!(t, ChangeType::Add | ChangeType::Remove | ChangeType::Child)),
                "Fan-in:child expected all adds, removes, or children"
            );
            assert!(
                types.len() <= 2,
                "Fan-in:child expected at most 2 types on a child change from fan-out"
            );

            // Sub-branch A: a branch preserved the child → that wins.
            if let Some(cc) = candidates_to_push.shift_remove(&ChangeType::Child) {
                return output.push(cc, pusher);
            }

            let add_change = candidates_to_push.shift_remove(&ChangeType::Add);
            let remove_change = candidates_to_push.shift_remove(&ChangeType::Remove);

            // Sub-branch B: at most one of add/remove allowed.
            assert!(
                add_change.is_none() || remove_change.is_none(),
                "Fan-in:child expected either add or remove, not both"
            );

            let only = add_change
                .or(remove_change)
                .expect("Fan-in:child expected at least one add or remove");
            output.push(add_empty_relationships(only), pusher)
        }
    }
}

/// TS `mergeRelationships(left, right)`.
///
/// Puts relationships from `right` into `left` where missing. Left wins
/// on key collision (TS spread order: `{...right, ...left}`).
///
/// Contract:
///   - If `left.type == right.type`, merges straightforwardly for each
///     variant.
///   - Otherwise, `left` must be an `Edit` and `right` is `Add` or `Remove`
///     (the asymmetry comes from fan-in:edit's convert-edit-into-add/remove
///     case).
pub fn merge_relationships(left: Change, right: Change) -> Change {
    if left.change_type() == right.change_type() {
        return match left {
            // ── add ────────────────────────────────────────────────
            Change::Add(l) => {
                let r_node = right_node_expect(right, "add");
                Change::Add(AddChange {
                    node: Node {
                        row: l.node.row,
                        relationships: merge_rels(r_node.relationships, l.node.relationships),
                    },
                })
            }
            // ── remove ─────────────────────────────────────────────
            Change::Remove(l) => {
                let r_node = right_node_expect(right, "remove");
                Change::Remove(RemoveChange {
                    node: Node {
                        row: l.node.row,
                        relationships: merge_rels(r_node.relationships, l.node.relationships),
                    },
                })
            }
            // ── edit ───────────────────────────────────────────────
            Change::Edit(l) => match right {
                Change::Edit(r) => Change::Edit(EditChange {
                    node: Node {
                        row: l.node.row,
                        relationships: merge_rels(r.node.relationships, l.node.relationships),
                    },
                    old_node: Node {
                        row: l.old_node.row,
                        relationships: merge_rels(
                            r.old_node.relationships,
                            l.old_node.relationships,
                        ),
                    },
                }),
                other => panic!(
                    "mergeRelationships: when left.type is edit and types match, right.type must be edit, got {:?}",
                    other.change_type()
                ),
            },
            // ── child ──────────────────────────────────────────────
            Change::Child(l) => match right {
                Change::Child(r) => Change::Child(ChildChange {
                    node: Node {
                        row: l.node.row,
                        relationships: merge_rels(r.node.relationships, l.node.relationships),
                    },
                    child: l.child,
                }),
                other => panic!(
                    "mergeRelationships: when left.type is child and types match, right.type must be child, got {:?}",
                    other.change_type()
                ),
            },
        };
    }

    // Types differ → left must be Edit.
    let left_edit = match left {
        Change::Edit(e) => e,
        other => panic!(
            "mergeRelationships: when types differ, left.type must be edit, got left.type={:?}, right.type={:?}",
            other.change_type(),
            right.change_type()
        ),
    };

    match right {
        Change::Add(r) => Change::Edit(EditChange {
            node: Node {
                // `...left.node` — row kept, relationships merged.
                row: left_edit.node.row,
                relationships: merge_rels(r.node.relationships, left_edit.node.relationships),
            },
            old_node: left_edit.old_node,
        }),
        Change::Remove(r) => Change::Edit(EditChange {
            node: left_edit.node,
            old_node: Node {
                row: left_edit.old_node.row,
                relationships: merge_rels(r.node.relationships, left_edit.old_node.relationships),
            },
        }),
        // TS `unreachable()` — Edit↔Edit handled in the same-type branch
        // above, and Edit↔Child would violate the contract enforced by
        // `pushAccumulatedChanges`.
        _ => unreachable!(
            "mergeRelationships: unreachable right branch — expected add or remove when left is edit and types differ"
        ),
    }
}

/// Helper to pull the `node` out of an `Add` or `Remove` change. Panics
/// with a TS-shaped message otherwise (used only when `left.type ==
/// right.type` and both are non-Edit, non-Child variants).
fn right_node_expect(right: Change, expected: &str) -> Node {
    match right {
        Change::Add(r) => r.node,
        Change::Remove(r) => r.node,
        other => panic!(
            "mergeRelationships: when left.type is {expected} and types match, right.type must be {expected}, got {:?}",
            other.change_type()
        ),
    }
}

/// Merge `left` relationships on top of `right` (left wins on collision).
/// Reproduces the TS `{...right, ...left}` spread.
fn merge_rels(
    right: IndexMap<String, RelationshipFactory>,
    left: IndexMap<String, RelationshipFactory>,
) -> IndexMap<String, RelationshipFactory> {
    let mut out = right;
    for (k, v) in left {
        out.insert(k, v); // `insert` overwrites — left wins.
    }
    out
}

/// TS `makeAddEmptyRelationships(schema)`.
///
/// Returns a closure that takes a `Change` and, for every relationship
/// named in `schema.relationships` that is missing on the change's
/// node(s), adds an empty relationship factory via [`merge_empty`].
///
/// If `schema.relationships` is empty, the closure is a no-op returning
/// its input unchanged.
pub fn make_add_empty_relationships(
    schema: &SourceSchema,
) -> Box<dyn Fn(Change) -> Change + Send + Sync> {
    let relationship_names: Vec<String> = schema.relationships.keys().cloned().collect();

    Box::new(move |change: Change| -> Change {
        // Branch: no schema relationships → identity.
        if relationship_names.is_empty() {
            return change;
        }

        match change {
            Change::Add(AddChange { node }) => {
                let Node {
                    row,
                    mut relationships,
                } = node;
                merge_empty(&mut relationships, &relationship_names);
                Change::Add(AddChange {
                    node: Node { row, relationships },
                })
            }
            Change::Remove(RemoveChange { node }) => {
                let Node {
                    row,
                    mut relationships,
                } = node;
                merge_empty(&mut relationships, &relationship_names);
                Change::Remove(RemoveChange {
                    node: Node { row, relationships },
                })
            }
            Change::Edit(EditChange { node, old_node }) => {
                let Node {
                    row,
                    mut relationships,
                } = node;
                merge_empty(&mut relationships, &relationship_names);
                let old_row = old_node.row;
                let mut old_relationships = old_node.relationships;
                merge_empty(&mut old_relationships, &relationship_names);
                Change::Edit(EditChange {
                    node: Node { row, relationships },
                    old_node: Node {
                        row: old_row,
                        relationships: old_relationships,
                    },
                })
            }
            // TS: children only have relationships along the path → passthrough.
            Change::Child(c) => Change::Child(c),
        }
    })
}

/// TS `mergeEmpty(relationships, relationshipNames)`.
///
/// For every name in `relationship_names` not already present in
/// `relationships`, inserts a factory that yields an empty stream. Mutates
/// `relationships` in place.
pub fn merge_empty(
    relationships: &mut IndexMap<String, RelationshipFactory>,
    relationship_names: &[String],
) {
    for name in relationship_names {
        if !relationships.contains_key(name) {
            let factory: RelationshipFactory = Box::new(|| {
                Box::new(std::iter::empty::<NodeOrYield>()) as Box<dyn Iterator<Item = NodeOrYield>>
            });
            relationships.insert(name.clone(), factory);
        }
    }
}

#[cfg(test)]
mod tests {
    //! Branch-complete coverage (each test's comment names the branch):
    //!
    //! `push_accumulated_changes`:
    //!   - empty input returns empty stream.
    //!   - fan-out Remove → one Remove out.
    //!   - fan-out Remove → non-remove in input panics.
    //!   - fan-out Add → one Add out.
    //!   - fan-out Add → non-add in input panics.
    //!   - fan-out Edit sub-branch A: edit present, merges add/remove.
    //!   - fan-out Edit sub-branch A: edit present without add or remove.
    //!   - fan-out Edit sub-branch B: no edit, both add and remove → synth edit.
    //!   - fan-out Edit sub-branch C: only add survives.
    //!   - fan-out Edit sub-branch C: only remove survives.
    //!   - fan-out Edit → child in input panics.
    //!   - fan-out Child sub-branch A: child preserved wins.
    //!   - fan-out Child sub-branch B: add only.
    //!   - fan-out Child sub-branch B: remove only.
    //!   - fan-out Child → both add and remove panics.
    //!   - fan-out Child → more than 2 types panics.
    //!   - fan-out Child → duplicate non-child (two adds) panics.
    //!   - accumulated_pushes is drained after call.
    //!
    //! `merge_relationships`:
    //!   - same-type Add merges relationships (left wins on collision).
    //!   - same-type Remove merges relationships.
    //!   - same-type Edit merges both node and old_node relationships.
    //!   - same-type Child keeps left's `child`, merges relationships.
    //!   - same-type Add vs. non-Add (TS helper msg) → panic.
    //!   - same-type Remove vs. non-Remove → panic.
    //!   - same-type Edit with non-Edit right → panic.
    //!   - same-type Child with non-Child right → panic.
    //!   - types-differ Edit vs. Add → Edit with merged node relationships.
    //!   - types-differ Edit vs. Remove → Edit with merged old_node relationships.
    //!   - types-differ left-not-edit → panic.
    //!   - types-differ Edit vs. Child → unreachable panic.
    //!
    //! `make_add_empty_relationships`:
    //!   - schema with zero relationships → identity passthrough.
    //!   - Add: fills missing relationships.
    //!   - Remove: fills missing relationships.
    //!   - Edit: fills on both node and old_node.
    //!   - Child: returned unchanged.
    //!   - existing relationship is not overwritten with empty.
    //!
    //! `merge_empty`:
    //!   - inserts empty factory for missing name.
    //!   - no-op for present name.
    //!   - inserted factory yields empty stream.

    use super::*;
    use crate::ivm::change::{ChildSpec, EditChange};
    use crate::ivm::data::{Node, NodeOrYield, make_comparator};
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::Arc;
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Row;

    // ── Test harness ──────────────────────────────────────────────────

    fn make_schema_with_rels(rel_names: &[&str]) -> SourceSchema {
        let sort: Ordering = vec![("id".to_string(), Direction::Asc)];
        let mut rels = IndexMap::new();
        for n in rel_names {
            rels.insert(
                (*n).to_string(),
                SourceSchema {
                    table_name: (*n).to_string(),
                    columns: IndexMap::new(),
                    primary_key: PrimaryKey::new(vec!["id".into()]),
                    relationships: IndexMap::new(),
                    is_hidden: false,
                    system: System::Test,
                    compare_rows: Arc::new(make_comparator(sort.clone(), false)),
                    sort: sort.clone(),
                },
            );
        }
        SourceSchema {
            table_name: "t".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: rels,
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    fn make_schema() -> SourceSchema {
        make_schema_with_rels(&[])
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

    /// Records every push's type and change.
    struct RecordingOutput {
        pushes: Vec<(ChangeType, Change)>,
    }
    impl Output for RecordingOutput {
        fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes.push((change.change_type(), change));
            Box::new(std::iter::empty())
        }
    }

    fn row_with_id(id: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r
    }

    fn node(id: i64) -> Node {
        Node {
            row: row_with_id(id),
            relationships: IndexMap::new(),
        }
    }

    fn node_with_rels(id: i64, rel_names: &[&str]) -> Node {
        let mut rels: IndexMap<String, RelationshipFactory> = IndexMap::new();
        for n in rel_names {
            let factory: RelationshipFactory = Box::new(|| {
                Box::new(std::iter::empty::<NodeOrYield>()) as Box<dyn Iterator<Item = NodeOrYield>>
            });
            rels.insert((*n).to_string(), factory);
        }
        Node {
            row: row_with_id(id),
            relationships: rels,
        }
    }

    /// Test-only merge: unions relationships with left-wins behaviour.
    fn default_merge(left: Change, right: Change) -> Change {
        merge_relationships(left, right)
    }

    /// Test-only add-empty: no-op. Separate from make_add_empty_relationships
    /// so tests can independently observe push behaviour.
    fn id_add_empty(c: Change) -> Change {
        c
    }

    fn drain<'a>(s: Stream<'a, Yield>) {
        for _ in s {}
    }

    // ── push_accumulated_changes: empty input ─────────────────────────
    #[test]
    fn push_accumulated_empty_input_returns_empty_stream() {
        let mut pushes: Vec<Change> = vec![];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Remove,
            &default_merge,
            &id_add_empty,
        ));
        assert!(sink.pushes.is_empty());
    }

    // ── push_accumulated_changes: Remove → one Remove out ─────────────
    #[test]
    fn push_accumulated_remove_fanin_forwards_single_remove() {
        let mut pushes = vec![
            Change::Remove(RemoveChange { node: node(1) }),
            Change::Remove(RemoveChange {
                node: node_with_rels(1, &["rel_a"]),
            }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Remove,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Remove);
    }

    #[test]
    #[should_panic(expected = "Fan-in:remove expected all removes")]
    fn push_accumulated_remove_with_non_remove_panics() {
        let mut pushes = vec![
            Change::Remove(RemoveChange { node: node(1) }),
            Change::Add(AddChange { node: node(1) }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Remove,
            &default_merge,
            &id_add_empty,
        ));
    }

    // ── push_accumulated_changes: Add → one Add out ───────────────────
    #[test]
    fn push_accumulated_add_fanin_forwards_single_add() {
        let mut pushes = vec![
            Change::Add(AddChange { node: node(5) }),
            Change::Add(AddChange { node: node(5) }),
            Change::Add(AddChange { node: node(5) }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Add,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Add);
    }

    #[test]
    #[should_panic(expected = "Fan-in:add expected all adds")]
    fn push_accumulated_add_with_non_add_panics() {
        let mut pushes = vec![
            Change::Add(AddChange { node: node(1) }),
            Change::Remove(RemoveChange { node: node(1) }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Add,
            &default_merge,
            &id_add_empty,
        ));
    }

    // ── push_accumulated_changes: Edit sub-branches ───────────────────
    #[test]
    fn push_accumulated_edit_sub_a_with_edit_merges_add_and_remove() {
        let mut pushes = vec![
            Change::Edit(EditChange {
                node: node(2),
                old_node: node(1),
            }),
            Change::Add(AddChange { node: node(2) }),
            Change::Remove(RemoveChange { node: node(1) }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Edit,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Edit);
    }

    #[test]
    fn push_accumulated_edit_sub_a_edit_only() {
        let mut pushes = vec![Change::Edit(EditChange {
            node: node(2),
            old_node: node(1),
        })];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Edit,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Edit);
    }

    #[test]
    fn push_accumulated_edit_sub_b_add_plus_remove_synthesises_edit() {
        // The "edit split into add/remove by two branches" case from the TS docs.
        let mut pushes = vec![
            Change::Remove(RemoveChange { node: node(1) }),
            Change::Add(AddChange { node: node(2) }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Edit,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        match &sink.pushes[0].1 {
            Change::Edit(e) => {
                assert_eq!(e.node.row.get("id"), Some(&Some(json!(2))));
                assert_eq!(e.old_node.row.get("id"), Some(&Some(json!(1))));
            }
            other => panic!("expected Edit, got {other:?}"),
        }
    }

    #[test]
    fn push_accumulated_edit_sub_c_add_only() {
        let mut pushes = vec![Change::Add(AddChange { node: node(5) })];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Edit,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Add);
    }

    #[test]
    fn push_accumulated_edit_sub_c_remove_only() {
        let mut pushes = vec![Change::Remove(RemoveChange { node: node(7) })];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Edit,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Remove);
    }

    #[test]
    #[should_panic(expected = "Fan-in:edit expected all adds, removes, or edits")]
    fn push_accumulated_edit_with_child_panics() {
        let mut pushes = vec![Change::Child(ChildChange {
            node: node(1),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange { node: node(2) })),
            },
        })];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Edit,
            &default_merge,
            &id_add_empty,
        ));
    }

    // ── push_accumulated_changes: Child sub-branches ──────────────────
    #[test]
    fn push_accumulated_child_sub_a_child_wins() {
        let mut pushes = vec![
            Change::Child(ChildChange {
                node: node(1),
                child: ChildSpec {
                    relationship_name: "r".into(),
                    change: Box::new(Change::Add(AddChange { node: node(99) })),
                },
            }),
            Change::Add(AddChange { node: node(1) }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Child,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Child);
    }

    #[test]
    fn push_accumulated_child_sub_b_add_only() {
        let mut pushes = vec![Change::Add(AddChange { node: node(1) })];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Child,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Add);
    }

    #[test]
    fn push_accumulated_child_sub_b_remove_only() {
        let mut pushes = vec![Change::Remove(RemoveChange { node: node(1) })];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Child,
            &default_merge,
            &id_add_empty,
        ));
        assert_eq!(sink.pushes.len(), 1);
        assert_eq!(sink.pushes[0].0, ChangeType::Remove);
    }

    #[test]
    #[should_panic(expected = "Fan-in:child expected either add or remove, not both")]
    fn push_accumulated_child_both_add_and_remove_panics() {
        let mut pushes = vec![
            Change::Add(AddChange { node: node(1) }),
            Change::Remove(RemoveChange { node: node(1) }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Child,
            &default_merge,
            &id_add_empty,
        ));
    }

    #[test]
    #[should_panic(expected = "Fan-in:child expected at most 2 types")]
    fn push_accumulated_child_three_types_panics() {
        // Push a child, an add, AND a remove → types.len() == 3 before the
        // both-add-and-remove assert (which would otherwise fire first).
        // The length check is reached only when child isn't present (else
        // child wins earlier), but the assertion on `types.len() <= 2`
        // runs before the child-winning sub-branch. We need 3 distinct
        // types: add, remove, child all at once to land on the length
        // assert before the "either add or remove" one.
        let mut pushes = vec![
            Change::Add(AddChange { node: node(1) }),
            Change::Remove(RemoveChange { node: node(1) }),
            Change::Child(ChildChange {
                node: node(1),
                child: ChildSpec {
                    relationship_name: "r".into(),
                    change: Box::new(Change::Add(AddChange { node: node(2) })),
                },
            }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Child,
            &default_merge,
            &id_add_empty,
        ));
    }

    #[test]
    #[should_panic(expected = "expected at most one")]
    fn push_accumulated_child_duplicate_non_child_panics() {
        // Two adds with fan-out child: the dedicated assert inside the
        // merge loop fires before the final `types.length <= 2` check.
        let mut pushes = vec![
            Change::Add(AddChange { node: node(1) }),
            Change::Add(AddChange { node: node(1) }),
        ];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Child,
            &default_merge,
            &id_add_empty,
        ));
    }

    #[test]
    fn push_accumulated_drains_input_vec_after_call() {
        let mut pushes = vec![Change::Add(AddChange { node: node(1) })];
        let mut sink = RecordingOutput { pushes: vec![] };
        let pusher = DummyInput {
            schema: make_schema(),
        };
        drain(push_accumulated_changes(
            &mut pushes,
            &mut sink,
            &pusher,
            ChangeType::Add,
            &default_merge,
            &id_add_empty,
        ));
        assert!(pushes.is_empty(), "accumulated_pushes must be drained");
    }

    // ── merge_relationships: same-type branches ───────────────────────
    #[test]
    fn merge_relationships_add_add_unions_relationships_left_wins() {
        let left = Change::Add(AddChange {
            node: node_with_rels(1, &["a", "b"]),
        });
        let right = Change::Add(AddChange {
            node: node_with_rels(1, &["b", "c"]),
        });
        let merged = merge_relationships(left, right);
        match merged {
            Change::Add(c) => {
                let keys: Vec<&String> = c.node.relationships.keys().collect();
                // `{...right, ...left}` → right's order first, then left overrides.
                // Contents: b,c from right + a,b from left → a,b,c; b from left wins.
                assert!(keys.iter().any(|k| k.as_str() == "a"));
                assert!(keys.iter().any(|k| k.as_str() == "b"));
                assert!(keys.iter().any(|k| k.as_str() == "c"));
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn merge_relationships_remove_remove_unions() {
        let left = Change::Remove(RemoveChange {
            node: node_with_rels(1, &["x"]),
        });
        let right = Change::Remove(RemoveChange {
            node: node_with_rels(1, &["y"]),
        });
        let merged = merge_relationships(left, right);
        match merged {
            Change::Remove(c) => {
                assert_eq!(c.node.relationships.len(), 2);
            }
            other => panic!("expected Remove, got {other:?}"),
        }
    }

    #[test]
    fn merge_relationships_edit_edit_merges_both_nodes() {
        let left = Change::Edit(EditChange {
            node: node_with_rels(2, &["a"]),
            old_node: node_with_rels(1, &["x"]),
        });
        let right = Change::Edit(EditChange {
            node: node_with_rels(2, &["b"]),
            old_node: node_with_rels(1, &["y"]),
        });
        let merged = merge_relationships(left, right);
        match merged {
            Change::Edit(e) => {
                assert_eq!(e.node.relationships.len(), 2);
                assert_eq!(e.old_node.relationships.len(), 2);
            }
            other => panic!("expected Edit, got {other:?}"),
        }
    }

    #[test]
    fn merge_relationships_child_child_keeps_left_child() {
        let left_inner = Change::Add(AddChange { node: node(10) });
        let right_inner = Change::Add(AddChange { node: node(20) });
        let left = Change::Child(ChildChange {
            node: node_with_rels(1, &["a"]),
            child: ChildSpec {
                relationship_name: "rel-left".into(),
                change: Box::new(left_inner),
            },
        });
        let right = Change::Child(ChildChange {
            node: node_with_rels(1, &["b"]),
            child: ChildSpec {
                relationship_name: "rel-right".into(),
                change: Box::new(right_inner),
            },
        });
        let merged = merge_relationships(left, right);
        match merged {
            Change::Child(c) => {
                assert_eq!(c.child.relationship_name, "rel-left");
                assert_eq!(c.node.relationships.len(), 2);
            }
            other => panic!("expected Child, got {other:?}"),
        }
    }

    // Note: the TS `mergeRelationships` "when left.type is edit and types
    // match, right.type must be edit" assert is unreachable in Rust
    // because `left.change_type() == right.change_type()` plus exhaustive
    // enum matching on `left` proves `right`'s variant. The ported code
    // retains the panic for defensive parity, but no well-typed caller can
    // trigger it — so we don't assert on its message.

    // ── merge_relationships: types-differ ─────────────────────────────
    #[test]
    fn merge_relationships_edit_and_add_types_differ() {
        let left = Change::Edit(EditChange {
            node: node_with_rels(2, &["a"]),
            old_node: node(1),
        });
        let right = Change::Add(AddChange {
            node: node_with_rels(2, &["b"]),
        });
        let merged = merge_relationships(left, right);
        match merged {
            Change::Edit(e) => {
                // Node relationships merged; old_node untouched.
                assert_eq!(e.node.relationships.len(), 2);
                assert!(e.old_node.relationships.is_empty());
            }
            other => panic!("expected Edit, got {other:?}"),
        }
    }

    #[test]
    fn merge_relationships_edit_and_remove_types_differ() {
        let left = Change::Edit(EditChange {
            node: node(2),
            old_node: node_with_rels(1, &["x"]),
        });
        let right = Change::Remove(RemoveChange {
            node: node_with_rels(1, &["y"]),
        });
        let merged = merge_relationships(left, right);
        match merged {
            Change::Edit(e) => {
                assert!(e.node.relationships.is_empty());
                assert_eq!(e.old_node.relationships.len(), 2);
            }
            other => panic!("expected Edit, got {other:?}"),
        }
    }

    #[test]
    #[should_panic(expected = "when types differ, left.type must be edit")]
    fn merge_relationships_types_differ_non_edit_left_panics() {
        let left = Change::Add(AddChange { node: node(1) });
        let right = Change::Remove(RemoveChange { node: node(1) });
        let _ = merge_relationships(left, right);
    }

    #[test]
    #[should_panic(expected = "unreachable")]
    fn merge_relationships_edit_vs_child_unreachable() {
        let left = Change::Edit(EditChange {
            node: node(2),
            old_node: node(1),
        });
        let right = Change::Child(ChildChange {
            node: node(1),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange { node: node(2) })),
            },
        });
        let _ = merge_relationships(left, right);
    }

    // ── make_add_empty_relationships ──────────────────────────────────
    #[test]
    fn make_add_empty_relationships_no_schema_rels_passthrough() {
        let schema = make_schema();
        let f = make_add_empty_relationships(&schema);
        let input = Change::Add(AddChange { node: node(1) });
        let out = f(input);
        match out {
            Change::Add(a) => assert!(a.node.relationships.is_empty()),
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn make_add_empty_relationships_add_fills_missing() {
        let schema = make_schema_with_rels(&["comments", "tags"]);
        let f = make_add_empty_relationships(&schema);
        let out = f(Change::Add(AddChange { node: node(1) }));
        match out {
            Change::Add(a) => {
                assert!(a.node.relationships.contains_key("comments"));
                assert!(a.node.relationships.contains_key("tags"));
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn make_add_empty_relationships_remove_fills_missing() {
        let schema = make_schema_with_rels(&["x"]);
        let f = make_add_empty_relationships(&schema);
        let out = f(Change::Remove(RemoveChange { node: node(1) }));
        match out {
            Change::Remove(r) => assert!(r.node.relationships.contains_key("x")),
            other => panic!("expected Remove, got {other:?}"),
        }
    }

    #[test]
    fn make_add_empty_relationships_edit_fills_both_sides() {
        let schema = make_schema_with_rels(&["a"]);
        let f = make_add_empty_relationships(&schema);
        let out = f(Change::Edit(EditChange {
            node: node(2),
            old_node: node(1),
        }));
        match out {
            Change::Edit(e) => {
                assert!(e.node.relationships.contains_key("a"));
                assert!(e.old_node.relationships.contains_key("a"));
            }
            other => panic!("expected Edit, got {other:?}"),
        }
    }

    #[test]
    fn make_add_empty_relationships_child_passthrough() {
        let schema = make_schema_with_rels(&["a"]);
        let f = make_add_empty_relationships(&schema);
        let input = Change::Child(ChildChange {
            node: node(1),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange { node: node(2) })),
            },
        });
        let out = f(input);
        match out {
            Change::Child(c) => {
                // TS: `return change;` for child — relationships not mutated.
                assert!(c.node.relationships.is_empty());
            }
            other => panic!("expected Child, got {other:?}"),
        }
    }

    #[test]
    fn make_add_empty_relationships_preserves_existing_factory() {
        let schema = make_schema_with_rels(&["rel"]);
        let f = make_add_empty_relationships(&schema);
        // Provide a factory that yields one node so we can tell it wasn't
        // clobbered by an empty one.
        let mut rels: IndexMap<String, RelationshipFactory> = IndexMap::new();
        rels.insert(
            "rel".into(),
            Box::new(|| {
                Box::new(
                    vec![NodeOrYield::Node(Node {
                        row: row_with_id(42),
                        relationships: IndexMap::new(),
                    })]
                    .into_iter(),
                ) as Box<dyn Iterator<Item = NodeOrYield>>
            }),
        );
        let input = Change::Add(AddChange {
            node: Node {
                row: row_with_id(1),
                relationships: rels,
            },
        });
        let out = f(input);
        match out {
            Change::Add(a) => {
                let factory = a.node.relationships.get("rel").expect("rel present");
                let items: Vec<NodeOrYield> = factory().collect();
                assert_eq!(items.len(), 1, "existing factory preserved");
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    // ── merge_empty ───────────────────────────────────────────────────
    #[test]
    fn merge_empty_inserts_missing_names() {
        let mut rels: IndexMap<String, RelationshipFactory> = IndexMap::new();
        merge_empty(&mut rels, &["a".to_string(), "b".to_string()]);
        assert!(rels.contains_key("a"));
        assert!(rels.contains_key("b"));
    }

    #[test]
    fn merge_empty_noop_for_present_name() {
        let mut rels: IndexMap<String, RelationshipFactory> = IndexMap::new();
        // Seed a distinctive factory so we can tell if merge_empty replaces it.
        rels.insert(
            "present".into(),
            Box::new(|| {
                Box::new(vec![NodeOrYield::Yield].into_iter())
                    as Box<dyn Iterator<Item = NodeOrYield>>
            }),
        );
        merge_empty(&mut rels, &["present".to_string()]);
        let items: Vec<NodeOrYield> = rels["present"]().collect();
        assert_eq!(items.len(), 1, "present factory must not be replaced");
    }

    #[test]
    fn merge_empty_inserted_factory_yields_empty_stream() {
        let mut rels: IndexMap<String, RelationshipFactory> = IndexMap::new();
        merge_empty(&mut rels, &["z".to_string()]);
        let items: Vec<NodeOrYield> = rels["z"]().collect();
        assert!(items.is_empty());
    }
}
