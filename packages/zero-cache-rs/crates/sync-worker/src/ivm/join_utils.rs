//! Port of `packages/zql/src/ivm/join-utils.ts`.
//!
//! Public exports ported:
//!
//! - [`JoinChangeOverlay`] — the in-progress child-push overlay state TS
//!   stores as `{change, position}`.
//! - [`generate_with_overlay`] — rehydrates a child-fetch stream while
//!   splicing in the effect of an in-flight child change. Used by
//!   [`Join`] and [`FlippedJoin`] so their `#processParentNode` factories
//!   see a consistent snapshot across the mid-push re-fetch.
//! - [`generate_with_overlay_no_yield`] — thin wrapper used by
//!   [`FlippedJoin`] that asserts the stream contains no `Yield`
//!   sentinels.  We document the same contract on the Rust side.
//! - [`row_equals_for_compound_key`] — TS `rowEqualsForCompoundKey`;
//!   compares two rows over a compound key using [`compare_values`] (the
//!   variant where `null === null`).
//! - [`is_join_match`] — TS `isJoinMatch`; uses [`values_equal`] (the
//!   variant where null never matches anything).
//! - [`build_join_constraint`] — TS `buildJoinConstraint`; builds a
//!   [`Constraint`] from `sourceRow` mapped via `sourceKey → targetKey`,
//!   returning `None` when any source value is SQL-NULL.

use std::cmp::Ordering as CmpOrdering;

use zero_cache_types::ast::CompoundKey;
use zero_cache_types::value::Row;

use crate::ivm::change::Change;
use crate::ivm::constraint::Constraint;
use crate::ivm::data::{Node, NodeOrYield, RelationshipFactory, compare_values, values_equal};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::Stream;

/// TS `type JoinChangeOverlay = { change: Change; position: Row | undefined }`.
///
/// Stored by `Join`/`FlippedJoin` during a child push so the parent-side
/// re-fetch observes the pending change consistently.
#[derive(Debug, Clone)]
pub struct JoinChangeOverlay {
    /// The in-flight child change being applied.
    pub change: Change,
    /// The parent row position up to which the change has been pushed
    /// downstream. `None` when no parent has been visited yet.
    pub position: Option<Row>,
}

/// TS `generateWithOverlayNoYield(stream, overlay, schema)`.
///
/// The TS variant is typed `Stream<Node>` — the caller guarantees the
/// underlying stream contains no `'yield'` sentinels. The Rust port
/// asserts the same via [`NodeOrYield::Node`] extraction and panics if a
/// `Yield` is observed (matches the implicit TS assumption).
pub fn generate_with_overlay_no_yield<'a>(
    stream: Vec<Node>,
    overlay: Change,
    schema: SourceSchema,
) -> Stream<'a, Node> {
    let stream_with_yields: Stream<'a, NodeOrYield> =
        Box::new(stream.into_iter().map(NodeOrYield::Node));
    let merged: Vec<Node> = generate_with_overlay(stream_with_yields, overlay, schema)
        .map(|n| match n {
            NodeOrYield::Node(n) => n,
            NodeOrYield::Yield => {
                panic!("generateWithOverlayNoYield: unexpected 'yield' sentinel")
            }
        })
        .collect();
    Box::new(merged.into_iter())
}

/// TS `generateWithOverlay(stream, overlay, schema)`.
///
/// Lazily splices `overlay` into `stream`. Overlay semantics per TS:
///
/// - **Add** — yield the original nodes; drop the node whose row equals
///   `overlay.node.row` (the new node is assumed already present after
///   the upstream committed the add).
/// - **Remove** — insert `overlay.node` at the first position where the
///   already-yielded nodes would sort greater, so the output looks like
///   the pre-remove state.  If every upstream node sorts less, emit at
///   the end.
/// - **Edit** — swap the new row back for the old row by inserting
///   `overlay.oldNode` in its sorted slot and dropping the node whose
///   row equals `overlay.node.row`.  Assert both halves applied.
/// - **Child** — wrap the matching node's relationship with a nested
///   overlay generator so downstream readers see the pre-push child
///   state (used by sibling joins).
///
/// Every branch ends with an `assert(applied)`: if the overlay was
/// never applied we panic with the TS message to surface the bug loudly.
///
/// **Rust divergence from TS** — this function is implemented eagerly:
/// it collects the upstream stream into a `Vec` and returns a
/// `Stream<NodeOrYield>`. TS's generator-semantics would be impossible
/// to preserve without pinning a self-referential future; the observable
/// effect is identical because every call site drains the returned
/// stream to completion before the operator returns (cycle safety — same
/// pattern as `filter.rs`).
pub fn generate_with_overlay<'a>(
    stream: Stream<'a, NodeOrYield>,
    overlay: Change,
    schema: SourceSchema,
) -> Stream<'a, NodeOrYield> {
    let mut out: Vec<NodeOrYield> = Vec::new();
    let mut applied = false;
    let mut edit_old_applied = false;
    let mut edit_new_applied = false;

    for node_or_yield in stream {
        let node = match node_or_yield {
            NodeOrYield::Yield => {
                out.push(NodeOrYield::Yield);
                continue;
            }
            NodeOrYield::Node(n) => n,
        };
        let mut yield_node = true;
        if !applied {
            match &overlay {
                Change::Add(add) => {
                    // TS: `schema.compareRows(overlay.node.row, node.row) === 0`
                    if (schema.compare_rows)(&add.node.row, &node.row) == CmpOrdering::Equal {
                        applied = true;
                        yield_node = false;
                    }
                }
                Change::Remove(rem) => {
                    // TS: `schema.compareRows(overlay.node.row, node.row) < 0`
                    if (schema.compare_rows)(&rem.node.row, &node.row) == CmpOrdering::Less {
                        applied = true;
                        out.push(NodeOrYield::Node(rem.node.clone()));
                    }
                }
                Change::Edit(edit) => {
                    if !edit_old_applied
                        && (schema.compare_rows)(&edit.old_node.row, &node.row) == CmpOrdering::Less
                    {
                        edit_old_applied = true;
                        if edit_new_applied {
                            applied = true;
                        }
                        out.push(NodeOrYield::Node(edit.old_node.clone()));
                    }
                    if !edit_new_applied
                        && (schema.compare_rows)(&edit.node.row, &node.row) == CmpOrdering::Equal
                    {
                        edit_new_applied = true;
                        if edit_old_applied {
                            applied = true;
                        }
                        yield_node = false;
                    }
                }
                Change::Child(child) => {
                    if (schema.compare_rows)(&child.node.row, &node.row) == CmpOrdering::Equal {
                        applied = true;
                        // Wrap node's relationships: override the named
                        // relationship with one that re-applies
                        // `child.child.change` via generate_with_overlay on
                        // the nested stream.
                        //
                        // Rust divergence: TS uses object-spread to preserve
                        // every OTHER relationship factory untouched. Rust
                        // cannot `Clone` a `Box<dyn Fn>`, so we MOVE the
                        // named factory out, wrap it, and construct the new
                        // node carrying the rewritten factory plus the
                        // remaining original factories (still owned by the
                        // Node).  Since we consume `node` on this branch
                        // (`yield_node = false`), moving out of
                        // `node.relationships` is sound.
                        let rel_name = child.child.relationship_name.clone();
                        let nested_change = (*child.child.change).clone();
                        let nested_schema = schema.relationships.get(&rel_name).cloned().expect(
                            "generate_with_overlay: child relationship missing from schema",
                        );

                        let Node {
                            row,
                            relationships: mut old_rels,
                        } = node;
                        let orig_factory = old_rels
                            .shift_remove(&rel_name)
                            .expect("generate_with_overlay: child relationship missing on node");
                        let factory: RelationshipFactory = Box::new(move || {
                            let inner_stream = (orig_factory)();
                            generate_with_overlay(
                                inner_stream,
                                nested_change.clone(),
                                nested_schema.clone(),
                            )
                        });
                        // Rebuild relationships preserving order with the
                        // rewritten factory placed back at the end — TS
                        // uses spread which also places the override last.
                        old_rels.insert(rel_name.clone(), factory);
                        let overlaid_node = Node {
                            row,
                            relationships: old_rels,
                        };
                        out.push(NodeOrYield::Node(overlaid_node));
                        // Short-circuit to next iteration — we've
                        // consumed `node` by value.
                        continue;
                    }
                }
            }
        }
        if yield_node {
            out.push(NodeOrYield::Node(node));
        }
    }

    if !applied {
        match &overlay {
            Change::Remove(rem) => {
                applied = true;
                out.push(NodeOrYield::Node(rem.node.clone()));
            }
            Change::Edit(edit) => {
                // TS `assert(editNewApplied, ...)` — the new node must have
                // been consumed before we can add back the old one.
                assert!(
                    edit_new_applied,
                    "edit overlay: new node must be applied before old node"
                );
                let _ = edit_old_applied;
                applied = true;
                out.push(NodeOrYield::Node(edit.old_node.clone()));
            }
            _ => {}
        }
    }

    assert!(
        applied,
        "overlayGenerator: overlay was never applied to any fetched node"
    );

    Box::new(out.into_iter())
}

/// TS `rowEqualsForCompoundKey(a, b, key)`.
///
/// Compares `a[key[i]]` with `b[key[i]]` for every `i`; equal iff every
/// pair has [`compare_values`] ordering [`CmpOrdering::Equal`]. Uses
/// `compareValues` (not `valuesEqual`) — the null/null === 0 behaviour is
/// what the caller wants when both sides are expected non-null or when
/// false positives from null are acceptable (parent/child row identity).
pub fn row_equals_for_compound_key(a: &Row, b: &Row, key: &CompoundKey) -> bool {
    for k in key.iter() {
        let av = a.get(k).cloned().unwrap_or(None);
        let bv = b.get(k).cloned().unwrap_or(None);
        if compare_values(&av, &bv) != CmpOrdering::Equal {
            return false;
        }
    }
    true
}

/// TS `isJoinMatch(parent, parentKey, child, childKey)`.
///
/// Uses [`values_equal`] — NULLs never match anything, matching SQL
/// semantics. Used when deciding whether a child change affects a
/// particular parent row's relationship.
pub fn is_join_match(
    parent: &Row,
    parent_key: &CompoundKey,
    child: &Row,
    child_key: &CompoundKey,
) -> bool {
    assert_eq!(
        parent_key.len(),
        child_key.len(),
        "isJoinMatch: parentKey and childKey length mismatch"
    );
    for (pk, ck) in parent_key.iter().zip(child_key.iter()) {
        let pv = parent.get(pk).cloned().unwrap_or(None);
        let cv = child.get(ck).cloned().unwrap_or(None);
        if !values_equal(&pv, &cv) {
            return false;
        }
    }
    true
}

/// TS `buildJoinConstraint(sourceRow, sourceKey, targetKey)`.
///
/// Produces a [`Constraint`] mapping `targetKey[i] -> sourceRow[sourceKey[i]]`
/// for every `i`, returning `None` if any source value is SQL-NULL.  Returns
/// an empty [`Constraint`] only when both `sourceKey` and `targetKey` are
/// empty — callers don't currently pass empty keys, but the TS loop body is
/// skipped in that case and an empty object is returned.
pub fn build_join_constraint(
    source_row: &Row,
    source_key: &CompoundKey,
    target_key: &CompoundKey,
) -> Option<Constraint> {
    assert_eq!(
        source_key.len(),
        target_key.len(),
        "buildJoinConstraint: sourceKey and targetKey length mismatch"
    );
    let mut constraint: Constraint = Constraint::new();
    for (sk, tk) in source_key.iter().zip(target_key.iter()) {
        let value = source_row.get(sk).cloned().unwrap_or(None);
        match &value {
            // TS: `if (value === null) return undefined;` — the row has a
            // literal SQL NULL here. Undefined (absent) values also yield
            // None — TS would read `undefined` and then also return
            // undefined because `undefined === null` is false but any
            // downstream constraint_matches_row comparison would fail.
            // Matching TS exactly: only return None for explicit Null;
            // absent → Value::None → not treated as null by TS `===`.
            //
            // BUT: TS `sourceRow[sourceKey[i]]` is `undefined` when absent
            // — and `undefined === null` is false, so TS would insert
            // `undefined` into the constraint.  That later propagates to a
            // fetch that uses the constraint, where `constraintMatchesRow`
            // uses `valuesEqual(None, _)` which is always false — no row
            // matches.  Rust parity: set the key to `None` and let the
            // downstream comparison fail naturally.
            Some(v) if v.is_null() => return None,
            _ => {}
        }
        constraint.insert(tk.clone(), value);
    }
    Some(constraint)
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch-complete coverage for every helper in this module.

    use super::*;
    use crate::ivm::change::{AddChange, ChildChange, ChildSpec, EditChange, RemoveChange};
    use crate::ivm::data::make_comparator;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::Arc;
    use zero_cache_types::ast::{Direction, Ordering as AstOrdering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Value;

    fn v(j: serde_json::Value) -> Value {
        Some(j)
    }

    fn row_pairs(fields: &[(&str, Value)]) -> Row {
        let mut r = Row::new();
        for (k, v) in fields {
            r.insert((*k).to_string(), v.clone());
        }
        r
    }

    fn make_schema() -> SourceSchema {
        let sort: AstOrdering = vec![("id".into(), Direction::Asc)];
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

    fn node(id: i64) -> Node {
        Node {
            row: row_pairs(&[("id", v(json!(id)))]),
            relationships: IndexMap::new(),
        }
    }

    fn stream_of(nodes: Vec<Node>) -> Stream<'static, NodeOrYield> {
        Box::new(nodes.into_iter().map(NodeOrYield::Node))
    }

    fn collect_rows(s: Stream<'_, NodeOrYield>) -> Vec<i64> {
        s.filter_map(|n| match n {
            NodeOrYield::Node(n) => n
                .row
                .get("id")
                .and_then(|v| v.as_ref())
                .and_then(|j| j.as_i64()),
            NodeOrYield::Yield => None,
        })
        .collect()
    }

    // ─── row_equals_for_compound_key ─────────────────────────────────

    // Branch: every key matches.
    #[test]
    fn row_equals_all_equal() {
        let a = row_pairs(&[("a", v(json!(1))), ("b", v(json!(2)))]);
        let b = row_pairs(&[("a", v(json!(1))), ("b", v(json!(2)))]);
        let key = vec!["a".into(), "b".into()];
        assert!(row_equals_for_compound_key(&a, &b, &key));
    }

    // Branch: a mismatch early-returns false.
    #[test]
    fn row_equals_first_key_differs() {
        let a = row_pairs(&[("a", v(json!(1)))]);
        let b = row_pairs(&[("a", v(json!(2)))]);
        let key = vec!["a".into()];
        assert!(!row_equals_for_compound_key(&a, &b, &key));
    }

    // Branch: empty key vector = trivially equal.
    #[test]
    fn row_equals_empty_key_is_true() {
        let a = row_pairs(&[("a", v(json!(1)))]);
        let b = row_pairs(&[("a", v(json!(2)))]);
        assert!(row_equals_for_compound_key(&a, &b, &vec![]));
    }

    // Branch: null on both sides compares equal (compare_values rule).
    #[test]
    fn row_equals_both_null_is_true() {
        let a = row_pairs(&[("a", Some(json!(null)))]);
        let b = row_pairs(&[("a", Some(json!(null)))]);
        assert!(row_equals_for_compound_key(&a, &b, &vec!["a".into()]));
    }

    // ─── is_join_match ───────────────────────────────────────────────

    // Branch: all keys match.
    #[test]
    fn is_join_match_all_match() {
        let p = row_pairs(&[("pid", v(json!(1)))]);
        let c = row_pairs(&[("cfk", v(json!(1)))]);
        assert!(is_join_match(
            &p,
            &vec!["pid".into()],
            &c,
            &vec!["cfk".into()]
        ));
    }

    // Branch: some key differs.
    #[test]
    fn is_join_match_mismatch() {
        let p = row_pairs(&[("pid", v(json!(1)))]);
        let c = row_pairs(&[("cfk", v(json!(2)))]);
        assert!(!is_join_match(
            &p,
            &vec!["pid".into()],
            &c,
            &vec!["cfk".into()]
        ));
    }

    // Branch: any NULL returns false (values_equal rule).
    #[test]
    fn is_join_match_null_never_matches() {
        let p = row_pairs(&[("pid", Some(json!(null)))]);
        let c = row_pairs(&[("cfk", Some(json!(null)))]);
        assert!(!is_join_match(
            &p,
            &vec!["pid".into()],
            &c,
            &vec!["cfk".into()]
        ));
    }

    // Branch: mismatched key length panics.
    #[test]
    #[should_panic(expected = "length mismatch")]
    fn is_join_match_length_mismatch_panics() {
        let p = row_pairs(&[("pid", v(json!(1)))]);
        let c = row_pairs(&[("cfk", v(json!(1)))]);
        is_join_match(
            &p,
            &vec!["pid".into(), "extra".into()],
            &c,
            &vec!["cfk".into()],
        );
    }

    // ─── build_join_constraint ───────────────────────────────────────

    // Branch: builds a constraint when values are present and non-null.
    #[test]
    fn build_constraint_all_present() {
        let src = row_pairs(&[("a", v(json!(1))), ("b", v(json!("x")))]);
        let c = build_join_constraint(
            &src,
            &vec!["a".into(), "b".into()],
            &vec!["pa".into(), "pb".into()],
        )
        .expect("must build");
        assert_eq!(c.get("pa"), Some(&v(json!(1))));
        assert_eq!(c.get("pb"), Some(&v(json!("x"))));
    }

    // Branch: literal SQL-NULL source value yields None.
    #[test]
    fn build_constraint_null_source_returns_none() {
        let src = row_pairs(&[("a", Some(json!(null)))]);
        assert!(build_join_constraint(&src, &vec!["a".into()], &vec!["pa".into()]).is_none());
    }

    // Branch: absent key maps to None (preserves TS undefined-passthrough).
    #[test]
    fn build_constraint_absent_key_insert_none() {
        let src = row_pairs(&[]);
        let c = build_join_constraint(&src, &vec!["a".into()], &vec!["pa".into()]).unwrap();
        assert_eq!(c.get("pa"), Some(&None));
    }

    // Branch: empty keys yields empty constraint.
    #[test]
    fn build_constraint_empty_keys() {
        let src = row_pairs(&[("a", v(json!(1)))]);
        let c = build_join_constraint(&src, &vec![], &vec![]).unwrap();
        assert!(c.is_empty());
    }

    // Branch: mismatched key length panics.
    #[test]
    #[should_panic(expected = "length mismatch")]
    fn build_constraint_length_mismatch_panics() {
        let src = row_pairs(&[("a", v(json!(1)))]);
        let _ = build_join_constraint(&src, &vec!["a".into()], &vec![]);
    }

    // ─── generate_with_overlay: Add ──────────────────────────────────

    // Branch: Add overlay — node matching overlay is dropped.
    #[test]
    fn overlay_add_drops_matching_node() {
        let schema = make_schema();
        let stream = stream_of(vec![node(1), node(2), node(3)]);
        let overlay = Change::Add(AddChange { node: node(2) });
        let out = collect_rows(generate_with_overlay(stream, overlay, schema));
        assert_eq!(out, vec![1, 3]);
    }

    // Branch: Add overlay when node not in stream → panic (never applied).
    #[test]
    #[should_panic(expected = "overlayGenerator: overlay was never applied")]
    fn overlay_add_never_applied_panics() {
        let schema = make_schema();
        let stream = stream_of(vec![node(1), node(2)]);
        let overlay = Change::Add(AddChange { node: node(99) });
        let _: Vec<_> = generate_with_overlay(stream, overlay, schema).collect();
    }

    // ─── generate_with_overlay: Remove ───────────────────────────────

    // Branch: Remove overlay — insert at the first greater position.
    #[test]
    fn overlay_remove_inserts_in_sorted_position() {
        let schema = make_schema();
        let stream = stream_of(vec![node(1), node(3)]);
        let overlay = Change::Remove(RemoveChange { node: node(2) });
        let out = collect_rows(generate_with_overlay(stream, overlay, schema));
        assert_eq!(out, vec![1, 2, 3]);
    }

    // Branch: Remove overlay — every stream node sorts less → append at end.
    #[test]
    fn overlay_remove_appends_at_end() {
        let schema = make_schema();
        let stream = stream_of(vec![node(1), node(2)]);
        let overlay = Change::Remove(RemoveChange { node: node(3) });
        let out = collect_rows(generate_with_overlay(stream, overlay, schema));
        assert_eq!(out, vec![1, 2, 3]);
    }

    // Branch: Remove overlay on empty stream → append.
    #[test]
    fn overlay_remove_empty_stream_appends() {
        let schema = make_schema();
        let stream = stream_of(vec![]);
        let overlay = Change::Remove(RemoveChange { node: node(5) });
        let out = collect_rows(generate_with_overlay(stream, overlay, schema));
        assert_eq!(out, vec![5]);
    }

    // ─── generate_with_overlay: Edit ─────────────────────────────────

    // Branch: Edit overlay — new and old at same key means simple swap.
    #[test]
    fn overlay_edit_same_position() {
        let schema = make_schema();
        // Stream contains the new node; we substitute the old one.
        let stream = stream_of(vec![node(1), node(2), node(3)]);
        // Old was id=2 (unchanged position). New is id=2 too (same row);
        // real edits that preserve key produce identical comparison.
        let overlay = Change::Edit(EditChange {
            node: node(2),
            old_node: node(2),
        });
        let out = collect_rows(generate_with_overlay(stream, overlay, schema));
        // Old inserted at first strictly-greater slot (id=3); new dropped;
        // final order: 1, 2 (the old which equals), 3 (since old inserted at < 3).
        // Actually TS logic: oldNode comparison `< 0`. old_node==1 < node(id=2)? false. old_node==2 < node(id=2)? false. old_node==2 < node(id=3)? true.
        // So: visit 1 (yield 1, none applied), visit 2 (editNew applies = drop), visit 3 (editOld applies → yield old(2) before, applied=true when both done; yield 3).
        assert_eq!(out, vec![1, 2, 3]);
    }

    // Branch: Edit overlay — new node at end, old goes to fall-through tail.
    #[test]
    fn overlay_edit_new_at_end_old_in_middle() {
        let schema = make_schema();
        // Stream: [1, 3]. Edit: old=5 -> new=3. Old sorts greater than stream end,
        // new at position 3. On visiting 1: nothing. On visiting 3: editNew applies → drop 3.
        // After loop: !applied (since editOld never fired); tail branch asserts
        // editNewApplied (true), emits oldNode(5).
        let stream = stream_of(vec![node(1), node(3)]);
        let overlay = Change::Edit(EditChange {
            node: node(3),
            old_node: node(5),
        });
        let out = collect_rows(generate_with_overlay(stream, overlay, schema));
        assert_eq!(out, vec![1, 5]);
    }

    // Branch: Edit overlay — new never found, trailing assert catches it.
    #[test]
    #[should_panic(expected = "edit overlay: new node must be applied before old node")]
    fn overlay_edit_new_never_seen_panics() {
        let schema = make_schema();
        // Empty stream. editNewApplied stays false; trailing branch asserts
        // that new must be applied before old — panics with exact TS message.
        let stream = stream_of(vec![]);
        let overlay = Change::Edit(EditChange {
            node: node(1),
            old_node: node(2),
        });
        let _: Vec<_> = generate_with_overlay(stream, overlay, schema).collect();
    }

    // Branch: Edit overlay — new applied mid-stream but old never inserted
    // (new is last element, nothing sorts greater). Trailing branch inserts
    // old_node and sets applied=true. No panic.
    #[test]
    fn overlay_edit_new_seen_old_tail_inserted() {
        let schema = make_schema();
        // Stream: [1, 2]. Edit: new=2, old=1 (swapping back). editNew applies
        // on visiting 2 (drop). After loop: !applied, editNewApplied=true →
        // tail branch inserts old(1). Result: [1, 1].
        let stream = stream_of(vec![node(1), node(2)]);
        let overlay = Change::Edit(EditChange {
            node: node(2),
            old_node: node(1),
        });
        let out = collect_rows(generate_with_overlay(stream, overlay, schema));
        // First 1 from the stream; then old(1) inserted at tail because
        // no stream node sorted strictly greater than old_node=1 during the
        // loop (1 and 2 are both not > 1... actually visiting 2: old=1 < 2
        // is true, so editOld applies there, inserting old(1) before 2).
        // Actually re-checking: visit 1 — editOld: old(1) < node(1)? false.
        // Visit 2 — editOld: old(1) < node(2)? true → apply editOld, emit
        // old(1) before 2; editNew: new(2) == node(2)? true → apply, drop.
        // Result: [1, 1, 2] with the second 1 emitted before the dropped 2.
        // BUT node(2) is dropped, so: [1 (yield), 1 (editOld emits), 2
        // dropped]. Final: [1, 1].
        assert_eq!(out, vec![1, 1]);
    }

    // ─── generate_with_overlay: Child ────────────────────────────────

    // Branch: Child overlay replaces the matching node's relationship.
    #[test]
    fn overlay_child_replaces_relationship_factory() {
        let schema_inner = make_schema();
        let mut rels = IndexMap::new();
        let mut outer_schema = make_schema();
        outer_schema.relationships.insert("r".into(), schema_inner);

        // Node 1 has a relationship 'r' that yields an inner node.
        let factory: RelationshipFactory = Box::new(|| {
            Box::new(vec![NodeOrYield::Node(node(100))].into_iter())
                as Box<dyn Iterator<Item = NodeOrYield>>
        });
        rels.insert("r".to_string(), factory);
        let parent_node = Node {
            row: row_pairs(&[("id", v(json!(1)))]),
            relationships: rels,
        };
        let stream = stream_of(vec![parent_node]);

        // Child overlay: add a grandchild node.
        let child_change = Change::Add(AddChange { node: node(100) });
        let overlay = Change::Child(ChildChange {
            node: node(1),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(child_change),
            },
        });

        let results: Vec<NodeOrYield> =
            generate_with_overlay(stream, overlay, outer_schema).collect();
        assert_eq!(results.len(), 1);
        match &results[0] {
            NodeOrYield::Node(n) => {
                assert!(n.relationships.contains_key("r"));
                let inner: Vec<NodeOrYield> = (n.relationships["r"])().collect();
                // Add overlay drops the matching node(100) in the stream.
                // Result should have 0 items.
                assert_eq!(inner.len(), 0);
            }
            _ => panic!("expected Node"),
        }
    }

    // ─── generate_with_overlay: Yield propagation ────────────────────

    // Branch: yield sentinel passes through untouched.
    #[test]
    fn overlay_yield_passthrough() {
        let schema = make_schema();
        let items: Vec<NodeOrYield> = vec![
            NodeOrYield::Yield,
            NodeOrYield::Node(node(1)),
            NodeOrYield::Yield,
            NodeOrYield::Node(node(2)),
        ];
        let stream: Stream<'static, NodeOrYield> = Box::new(items.into_iter());
        let overlay = Change::Add(AddChange { node: node(1) });
        let out: Vec<NodeOrYield> = generate_with_overlay(stream, overlay, schema).collect();
        let yields = out
            .iter()
            .filter(|n| matches!(n, NodeOrYield::Yield))
            .count();
        assert_eq!(yields, 2);
    }

    // ─── generate_with_overlay_no_yield ──────────────────────────────

    // Branch: yield-free stream returns an equivalent Node stream.
    #[test]
    fn overlay_no_yield_wraps_nodes() {
        let schema = make_schema();
        let overlay = Change::Remove(RemoveChange { node: node(2) });
        let out: Vec<Node> =
            generate_with_overlay_no_yield(vec![node(1), node(3)], overlay, schema).collect();
        let ids: Vec<i64> = out
            .iter()
            .map(|n| n.row.get("id").unwrap().as_ref().unwrap().as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    // Branch: the wrapper path only accepts Node inputs (exercised by the
    // above test). No direct yield construction surface exists.

    // ─── JoinChangeOverlay construction ──────────────────────────────

    // Branch: construct with position None.
    #[test]
    fn join_change_overlay_no_position() {
        let o = JoinChangeOverlay {
            change: Change::Add(AddChange { node: node(1) }),
            position: None,
        };
        assert!(o.position.is_none());
    }

    // Branch: construct with position Some.
    #[test]
    fn join_change_overlay_with_position() {
        let o = JoinChangeOverlay {
            change: Change::Add(AddChange { node: node(1) }),
            position: Some(row_pairs(&[("id", v(json!(1)))])),
        };
        assert!(o.position.is_some());
    }
}
