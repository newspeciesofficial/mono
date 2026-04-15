//! Port of `packages/zql/src/ivm/join.ts`.
//!
//! Public exports ported:
//!
//! - [`Join`] — inner join that correlates parent rows with child rows
//!   over `parentKey`/`childKey`. Output is parent rows in parent order,
//!   each carrying a lazy `relationshipName` factory that yields matching
//!   child rows.
//!
//! ## Ownership divergence from TS
//!
//! The TS ctor wires `parent.setOutput({push: pushParent})` and
//! `child.setOutput({push: pushChild})` so the operator graph dispatches
//! pushes automatically. Rust cannot replicate that without creating a
//! cycle (the sink closure would need to capture `Arc<Join>`; Join would
//! own the upstream which owns the sink…). Instead:
//!
//! - The upstreams are held as `Arc<Mutex<Box<dyn Input>>>` so relationship
//!   factories can clone the handle and run `fetch` lazily.
//! - The pipeline driver (Layer 11) will be responsible for routing
//!   upstream pushes to [`Join::push_parent`] and [`Join::push_child`]
//!   explicitly. Until then these methods are the test entry points.
//! - [`Output::push`] on the `Join` itself is not wired — a downstream
//!   operator would not push *into* Join (push flows only downstream).
//!   We still implement `Output` because [`Operator`] requires it, but
//!   the implementation panics with a descriptive message.

use std::cmp::Ordering as CmpOrdering;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use zero_cache_types::ast::{CompoundKey, System};
use zero_cache_types::value::Row;

use crate::ivm::change::{AddChange, Change, ChildChange, ChildSpec, EditChange, RemoveChange};
use crate::ivm::data::{Node, NodeOrYield, RelationshipFactory};
use crate::ivm::join_utils::{
    JoinChangeOverlay, build_join_constraint, generate_with_overlay, is_join_match,
    row_equals_for_compound_key,
};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Operator, Output};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

/// TS `Join`.
pub struct Join {
    parent: Arc<Mutex<Box<dyn Input>>>,
    child: Arc<Mutex<Box<dyn Input>>>,
    parent_key: CompoundKey,
    child_key: CompoundKey,
    relationship_name: String,
    schema: SourceSchema,
    child_schema: SourceSchema,
    output: Arc<Mutex<Option<Box<dyn Output>>>>,
    inprogress_child_change: Arc<Mutex<Option<JoinChangeOverlay>>>,
}

/// TS `Args`. Grouped so the call site reads like the TS object literal.
pub struct JoinArgs {
    pub parent: Box<dyn Input>,
    pub child: Box<dyn Input>,
    pub parent_key: CompoundKey,
    pub child_key: CompoundKey,
    pub relationship_name: String,
    pub hidden: bool,
    pub system: System,
}

impl Join {
    /// TS `new Join({parent, child, parentKey, childKey, relationshipName, hidden, system})`.
    ///
    /// Panics (matching TS `assert`):
    /// - If `parent_key.len() != child_key.len()`.
    ///
    /// TS also asserts `parent !== child`. In Rust we can't compare
    /// `Box<dyn Input>` identity without extra plumbing; the pipeline
    /// driver enforces this invariant at graph-build time.
    pub fn new(args: JoinArgs) -> Self {
        assert_eq!(
            args.parent_key.len(),
            args.child_key.len(),
            "The parentKey and childKey keys must have same length",
        );

        let parent_schema = args.parent.get_schema().clone();
        let child_schema_inner = args.child.get_schema().clone();

        // TS: `{...parentSchema, relationships: {...parentSchema.relationships, [name]: {...childSchema, isHidden: hidden, system}}}`.
        let mut relationships = parent_schema.relationships.clone();
        let mut rel_schema = child_schema_inner.clone();
        rel_schema.is_hidden = args.hidden;
        rel_schema.system = args.system;
        relationships.insert(args.relationship_name.clone(), rel_schema);

        let schema = SourceSchema {
            table_name: parent_schema.table_name,
            columns: parent_schema.columns,
            primary_key: parent_schema.primary_key,
            relationships,
            is_hidden: parent_schema.is_hidden,
            system: parent_schema.system,
            compare_rows: parent_schema.compare_rows,
            sort: parent_schema.sort,
        };

        Self {
            parent: Arc::new(Mutex::new(args.parent)),
            child: Arc::new(Mutex::new(args.child)),
            parent_key: args.parent_key,
            child_key: args.child_key,
            relationship_name: args.relationship_name,
            schema,
            child_schema: child_schema_inner,
            output: Arc::new(Mutex::new(None)),
            inprogress_child_change: Arc::new(Mutex::new(None)),
        }
    }

    /// Wired variant: constructs a [`Join`] and immediately installs
    /// the back-edges TS writes via `parent.setOutput(this)` and
    /// `child.setOutput(this)` in the constructor. Returns
    /// `Arc<Mutex<Self>>` so the back-edge adapters (which impl
    /// `Output`) can hold a strong reference.
    ///
    /// Strong cycle (parent/child input → Join) is bounded by
    /// pipeline lifetime: the driver destroys the whole pipeline
    /// together on reset / remove_query.
    pub fn new_wired(args: JoinArgs) -> Arc<Mutex<Self>> {
        let arc = Arc::new(Mutex::new(Self::new(args)));
        let parent_back: Box<dyn Output> =
            Box::new(JoinParentBackEdge(Arc::clone(&arc)));
        let child_back: Box<dyn Output> =
            Box::new(JoinChildBackEdge(Arc::clone(&arc)));
        {
            let guard = arc.lock().unwrap();
            guard
                .parent
                .lock()
                .expect("join parent mutex poisoned")
                .set_output(parent_back);
            guard
                .child
                .lock()
                .expect("join child mutex poisoned")
                .set_output(child_back);
        }
        arc
    }

    /// Build the relationship factory for a parent node. Clones
    /// Arc-shared references so the factory can outlive a single fetch
    /// call.
    fn process_parent_node(&self, parent_row: Row) -> Node {
        let child = Arc::clone(&self.child);
        let inprogress = Arc::clone(&self.inprogress_child_change);
        let parent_key = self.parent_key.clone();
        let child_key = self.child_key.clone();
        let parent_row_clone = parent_row.clone();
        let child_schema = self.child_schema.clone();
        let schema_compare = Arc::clone(&self.schema.compare_rows);

        let factory: RelationshipFactory = Box::new(move || {
            // Build constraint from parent's compound key values.
            let constraint_opt = build_join_constraint(&parent_row_clone, &parent_key, &child_key);
            let nodes: Vec<NodeOrYield> = match constraint_opt {
                None => Vec::new(),
                Some(constraint) => {
                    // Lock the child input, run fetch, drain to Vec.
                    let child_guard = child.lock().expect("join child mutex poisoned");
                    let req = FetchRequest {
                        constraint: Some(constraint),
                        ..FetchRequest::default()
                    };
                    child_guard.fetch(req).collect()
                }
            };

            // Apply in-progress child-change overlay, matching TS semantics.
            let overlay_opt: Option<(Change, SourceSchema)> = {
                let guard = inprogress.lock().expect("join inprogress mutex poisoned");
                if let Some(overlay) = guard.as_ref() {
                    let matches = is_join_match(
                        &parent_row_clone,
                        &parent_key,
                        &overlay.change.node().row,
                        &child_key,
                    );
                    let past_position = overlay
                        .position
                        .as_ref()
                        .map(|pos| (schema_compare)(&parent_row_clone, pos) == CmpOrdering::Greater)
                        .unwrap_or(false);
                    if matches && past_position {
                        Some((overlay.change.clone(), child_schema.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            let stream: Stream<'static, NodeOrYield> = Box::new(nodes.into_iter());
            let out: Stream<'static, NodeOrYield> = match overlay_opt {
                Some((change, schema)) => generate_with_overlay(stream, change, schema),
                None => stream,
            };
            out as Box<dyn Iterator<Item = NodeOrYield>>
        });

        let mut relationships: IndexMap<String, RelationshipFactory> = IndexMap::new();
        relationships.insert(self.relationship_name.clone(), factory);

        Node {
            row: parent_row,
            relationships,
        }
    }

    /// TS `#pushParent(change)`.
    ///
    /// Public because we expose direct push entry points (see module docs).
    pub fn push_parent<'a>(&'a self, change: Change) -> Stream<'a, Yield> {
        // Build the downstream change eagerly, then invoke output.push and
        // drain — matches the TS yield* semantics with our eager collection
        // idiom from filter.rs.
        let downstream_change: Change = match change {
            Change::Add(AddChange { node }) => Change::Add(AddChange {
                node: self.process_parent_node(node.row),
            }),
            Change::Remove(RemoveChange { node }) => Change::Remove(RemoveChange {
                node: self.process_parent_node(node.row),
            }),
            Change::Child(ChildChange { node, child }) => Change::Child(ChildChange {
                node: self.process_parent_node(node.row),
                child,
            }),
            Change::Edit(EditChange { node, old_node }) => {
                // TS: assert the parent edit does not change relationship keys.
                assert!(
                    row_equals_for_compound_key(&old_node.row, &node.row, &self.parent_key),
                    "Parent edit must not change relationship.",
                );
                Change::Edit(EditChange {
                    node: self.process_parent_node(node.row),
                    old_node: self.process_parent_node(old_node.row),
                })
            }
        };
        self.push_to_output(downstream_change)
    }

    /// TS `#pushChild(change)`.
    pub fn push_child<'a>(&'a self, change: Change) -> Stream<'a, Yield> {
        match &change {
            Change::Add(_) | Change::Remove(_) | Change::Child(_) => {
                let row = change.node().row.clone();
                self.push_child_change(row, change)
            }
            Change::Edit(edit) => {
                let child_row = edit.node.row.clone();
                let old_child_row = edit.old_node.row.clone();
                assert!(
                    row_equals_for_compound_key(&old_child_row, &child_row, &self.child_key),
                    "Child edit must not change relationship.",
                );
                self.push_child_change(child_row, change)
            }
        }
    }

    /// TS `#pushChildChange(childRow, change)`.
    fn push_child_change<'a>(&'a self, child_row: Row, change: Change) -> Stream<'a, Yield> {
        // Set up in-progress overlay (TS try / finally).
        {
            let mut guard = self.inprogress_child_change.lock().unwrap();
            *guard = Some(JoinChangeOverlay {
                change: change.clone(),
                position: None,
            });
        }

        // Drop-guard equivalent for the TS `finally`: clear on unwind too.
        struct ClearGuard<'a> {
            slot: &'a Mutex<Option<JoinChangeOverlay>>,
        }
        impl<'a> Drop for ClearGuard<'a> {
            fn drop(&mut self) {
                if let Ok(mut g) = self.slot.lock() {
                    *g = None;
                }
            }
        }
        let _guard = ClearGuard {
            slot: &self.inprogress_child_change,
        };

        // Build constraint from the child row into parent key shape.
        let constraint_opt = build_join_constraint(&child_row, &self.child_key, &self.parent_key);
        let parent_nodes: Vec<NodeOrYield> = match constraint_opt {
            None => Vec::new(),
            Some(constraint) => {
                let parent_guard = self.parent.lock().expect("join parent mutex poisoned");
                let req = FetchRequest {
                    constraint: Some(constraint),
                    ..FetchRequest::default()
                };
                parent_guard.fetch(req).collect()
            }
        };

        // Iterate parent matches and emit a ChildChange per-match.
        let mut yields_buffer: Vec<Yield> = Vec::new();
        for parent_node in parent_nodes {
            match parent_node {
                NodeOrYield::Yield => {
                    yields_buffer.push(Yield);
                    continue;
                }
                NodeOrYield::Node(parent_node) => {
                    // Update position BEFORE building the processed node so
                    // process_parent_node sees the updated position when
                    // deciding whether to overlay.
                    {
                        let mut guard = self.inprogress_child_change.lock().unwrap();
                        if let Some(o) = guard.as_mut() {
                            o.position = Some(parent_node.row.clone());
                        }
                    }
                    let processed = self.process_parent_node(parent_node.row);
                    let child_change = Change::Child(ChildChange {
                        node: processed,
                        child: ChildSpec {
                            relationship_name: self.relationship_name.clone(),
                            change: Box::new(change.clone()),
                        },
                    });
                    for y in self.push_to_output(child_change) {
                        yields_buffer.push(y);
                    }
                }
            }
        }

        Box::new(yields_buffer.into_iter())
    }

    fn push_to_output<'a>(&'a self, change: Change) -> Stream<'a, Yield> {
        let mut out = self.output.lock().expect("join output mutex poisoned");
        match out.as_mut() {
            None => panic!("Output not set"),
            Some(sink) => {
                // Pusher identity: use the parent input (the upstream this
                // op represents). We can't easily reborrow; collect eagerly.
                //
                // For pusher, lock parent and reborrow its reference for
                // the call — then drop after collect.
                let parent_guard = self.parent.lock().expect("join parent mutex poisoned");
                let pusher: &dyn InputBase = &**parent_guard;
                let collected: Vec<Yield> = sink.push(change, pusher).collect();
                Box::new(collected.into_iter())
            }
        }
    }
}

impl InputBase for Join {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn destroy(&mut self) {
        // TS: `this.#parent.destroy(); this.#child.destroy();`
        {
            let mut p = self.parent.lock().expect("join parent mutex poisoned");
            p.destroy();
        }
        {
            let mut c = self.child.lock().expect("join child mutex poisoned");
            c.destroy();
        }
    }
}

impl Input for Join {
    fn set_output(&mut self, output: Box<dyn Output>) {
        *self.output.lock().expect("join output mutex poisoned") = Some(output);
    }

    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        // TS iterates parent.fetch and decorates each parent node with the
        // join relationship.  Yield sentinels pass through.
        let parent_guard = self.parent.lock().expect("join parent mutex poisoned");
        let parent_nodes: Vec<NodeOrYield> = parent_guard.fetch(req).collect();
        drop(parent_guard);

        let mut out: Vec<NodeOrYield> = Vec::with_capacity(parent_nodes.len());
        for n in parent_nodes {
            match n {
                NodeOrYield::Yield => out.push(NodeOrYield::Yield),
                NodeOrYield::Node(parent_node) => {
                    let processed = self.process_parent_node(parent_node.row);
                    out.push(NodeOrYield::Node(processed));
                }
            }
        }
        Box::new(out.into_iter())
    }
}

impl Output for Join {
    fn push<'a>(&'a mut self, _change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        panic!(
            "Join::push called directly — use push_parent / push_child instead \
             (pipeline driver wires upstream push routing)"
        );
    }
}

impl Operator for Join {}

/// Back-edge adapter installed on [`Join::parent`] so pushes from the
/// parent input route into [`Join::push_parent`]. Created by
/// [`Join::new_wired`]. Strong Arc; see that function for cycle notes.
pub struct JoinParentBackEdge(pub Arc<Mutex<Join>>);

impl Output for JoinParentBackEdge {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let guard = self.0.lock().expect("join back-edge mutex poisoned");
        let items: Vec<Yield> = guard.push_parent(change).collect();
        drop(guard);
        Box::new(items.into_iter())
    }
}

/// Back-edge adapter installed on [`Join::child`] so pushes from the
/// child input (e.g. the messages TableSource connection) route into
/// [`Join::push_child`]. This is the fix for the xyne message-flicker
/// symptom: without this wire the child's connection `output` slot
/// stays `None` and `TableSource::push_change` silently drops the
/// change.
pub struct JoinChildBackEdge(pub Arc<Mutex<Join>>);

impl Output for JoinChildBackEdge {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let guard = self.0.lock().expect("join back-edge mutex poisoned");
        let items: Vec<Yield> = guard.push_child(change).collect();
        drop(guard);
        Box::new(items.into_iter())
    }
}

/// Adapter that lets a wired [`Arc<Mutex<Join>>`] be plugged back into
/// the chain as `Box<dyn Input>`. Schema is cached at construction
/// to keep `get_schema(&self) -> &SourceSchema` lifetime-clean under
/// the `Arc<Mutex<>>` wrap.
pub struct ArcJoinAsInput {
    inner: Arc<Mutex<Join>>,
    schema: SourceSchema,
}

impl ArcJoinAsInput {
    pub fn new(inner: Arc<Mutex<Join>>) -> Self {
        let schema = inner.lock().unwrap().get_schema().clone();
        Self { inner, schema }
    }
}

impl InputBase for ArcJoinAsInput {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.inner.lock().unwrap().destroy();
    }
}

impl Input for ArcJoinAsInput {
    fn set_output(&mut self, output: Box<dyn Output>) {
        self.inner.lock().unwrap().set_output(output);
    }
    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        let guard = self.inner.lock().unwrap();
        let items: Vec<NodeOrYield> = guard.fetch(req).collect();
        drop(guard);
        Box::new(items.into_iter())
    }
}

impl Output for ArcJoinAsInput {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let mut guard = self.inner.lock().unwrap();
        let items: Vec<Yield> = guard.push(change, pusher).collect();
        drop(guard);
        Box::new(items.into_iter())
    }
}

impl Operator for ArcJoinAsInput {}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage.
    //!
    //! Mock `Input`s back parent/child with a fixed `Vec<Node>`; fetch
    //! applies the constraint via `values_equal`.  This is narrower than
    //! `MemorySource` but sufficient for branch-complete coverage.

    use super::*;
    use crate::ivm::constraint::{Constraint, constraint_matches_row};
    use crate::ivm::data::make_comparator;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering as AtOrdering};
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

    fn make_schema(name: &str, sort_col: &str) -> SourceSchema {
        let sort: AstOrdering = vec![(sort_col.into(), Direction::Asc)];
        SourceSchema {
            table_name: name.into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec![sort_col.into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    struct MockInput {
        schema: SourceSchema,
        rows: Vec<Row>,
        destroyed: Arc<AtomicBool>,
    }
    impl MockInput {
        fn new(schema: SourceSchema, rows: Vec<Row>) -> (Box<dyn Input>, Arc<AtomicBool>) {
            let destroyed = Arc::new(AtomicBool::new(false));
            (
                Box::new(Self {
                    schema,
                    rows,
                    destroyed: destroyed.clone(),
                }),
                destroyed,
            )
        }
    }
    impl InputBase for MockInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {
            self.destroyed.store(true, AtOrdering::SeqCst);
        }
    }
    impl Input for MockInput {
        fn set_output(&mut self, _output: Box<dyn Output>) {}
        fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
            // Filter rows by constraint, then sort by schema.compareRows.
            let mut matched: Vec<Row> = self
                .rows
                .iter()
                .filter(|r| match &req.constraint {
                    Some(c) => constraint_matches_row(c, r),
                    None => true,
                })
                .cloned()
                .collect();
            let cmp = Arc::clone(&self.schema.compare_rows);
            matched.sort_by(|a, b| (cmp)(a, b));
            let out: Vec<NodeOrYield> = matched
                .into_iter()
                .map(|r| {
                    NodeOrYield::Node(Node {
                        row: r,
                        relationships: IndexMap::new(),
                    })
                })
                .collect();
            Box::new(out.into_iter())
        }
    }

    struct RecordingOutput {
        log: Arc<Mutex<Vec<Change>>>,
    }
    impl Output for RecordingOutput {
        fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
            self.log.lock().unwrap().push(change);
            Box::new(std::iter::empty())
        }
    }

    fn mk_join(parent_rows: Vec<Row>, child_rows: Vec<Row>) -> (Join, Arc<Mutex<Vec<Change>>>) {
        let ps = make_schema("parent", "id");
        let cs = make_schema("child", "cid");
        let (p, _) = MockInput::new(ps, parent_rows);
        let (c, _) = MockInput::new(cs, child_rows);
        let mut j = Join::new(JoinArgs {
            parent: p,
            child: c,
            parent_key: vec!["id".into()],
            child_key: vec!["pid".into()],
            relationship_name: "kids".into(),
            hidden: false,
            system: System::Test,
        });
        let log = Arc::new(Mutex::new(Vec::new()));
        j.set_output(Box::new(RecordingOutput {
            log: Arc::clone(&log),
        }));
        (j, log)
    }

    fn count_children_in(change: &Change, rel: &str) -> usize {
        change.node().relationships[rel]().count()
    }

    // Branch: constructor asserts parent_key / child_key length parity.
    #[test]
    #[should_panic(expected = "same length")]
    fn new_panics_on_length_mismatch() {
        let ps = make_schema("p", "id");
        let cs = make_schema("c", "cid");
        let (p, _) = MockInput::new(ps, vec![]);
        let (c, _) = MockInput::new(cs, vec![]);
        let _ = Join::new(JoinArgs {
            parent: p,
            child: c,
            parent_key: vec!["id".into(), "x".into()],
            child_key: vec!["pid".into()],
            relationship_name: "r".into(),
            hidden: false,
            system: System::Test,
        });
    }

    // Branch: schema includes the new relationship with hidden + system.
    #[test]
    fn new_schema_carries_relationship_with_hidden_flag() {
        let ps = make_schema("p", "id");
        let cs = make_schema("c", "cid");
        let (p, _) = MockInput::new(ps, vec![]);
        let (c, _) = MockInput::new(cs, vec![]);
        let j = Join::new(JoinArgs {
            parent: p,
            child: c,
            parent_key: vec!["id".into()],
            child_key: vec!["pid".into()],
            relationship_name: "r".into(),
            hidden: true,
            system: System::Permissions,
        });
        let rel = &j.get_schema().relationships["r"];
        assert!(rel.is_hidden);
        assert!(matches!(rel.system, System::Permissions));
        assert_eq!(j.get_schema().table_name, "p");
    }

    // Branch: fetch yields parent rows with populated relationship factory.
    #[test]
    fn fetch_single_parent_matches_children() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let kids = vec![
            row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
            row_pairs(&[("cid", v(json!(11))), ("pid", v(json!(1)))]),
            row_pairs(&[("cid", v(json!(20))), ("pid", v(json!(2)))]),
        ];
        let (j, _) = mk_join(parents, kids);
        let nodes: Vec<NodeOrYield> = j.fetch(FetchRequest::default()).collect();
        assert_eq!(nodes.len(), 1);
        if let NodeOrYield::Node(n) = &nodes[0] {
            let kids_count = n.relationships["kids"]().count();
            assert_eq!(kids_count, 2);
        } else {
            panic!("expected node");
        }
    }

    // Branch: parent with no matching children → factory yields empty.
    #[test]
    fn fetch_parent_with_no_matches_empty_relationship() {
        let parents = vec![row_pairs(&[("id", v(json!(99)))])];
        let kids = vec![row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))])];
        let (j, _) = mk_join(parents, kids);
        let nodes: Vec<NodeOrYield> = j.fetch(FetchRequest::default()).collect();
        assert_eq!(nodes.len(), 1);
        if let NodeOrYield::Node(n) = &nodes[0] {
            assert_eq!(n.relationships["kids"]().count(), 0);
        }
    }

    // Branch: parent key value NULL → build_join_constraint returns None →
    // factory yields empty even if children exist.
    #[test]
    fn fetch_parent_with_null_key_yields_empty_relationship() {
        let parents = vec![row_pairs(&[("id", Some(json!(null)))])];
        let kids = vec![row_pairs(&[
            ("cid", v(json!(10))),
            ("pid", Some(json!(null))),
        ])];
        let (j, _) = mk_join(parents, kids);
        let nodes: Vec<NodeOrYield> = j.fetch(FetchRequest::default()).collect();
        assert_eq!(nodes.len(), 1);
        if let NodeOrYield::Node(n) = &nodes[0] {
            assert_eq!(n.relationships["kids"]().count(), 0);
        }
    }

    // Branch: empty parent input → empty output.
    #[test]
    fn fetch_empty_parent() {
        let (j, _) = mk_join(vec![], vec![row_pairs(&[("cid", v(json!(1)))])]);
        let n: Vec<_> = j.fetch(FetchRequest::default()).collect();
        assert!(n.is_empty());
    }

    // Branch: push_parent Add yields Add downstream with processed relationship.
    #[test]
    fn push_parent_add() {
        let parents = vec![];
        let kids = vec![row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(7)))])];
        let (j, log) = mk_join(parents, kids);
        let add = Change::Add(AddChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(7)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_parent(add).collect();
        let log = log.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert!(matches!(log[0], Change::Add(_)));
        assert_eq!(count_children_in(&log[0], "kids"), 1);
    }

    // Branch: push_parent Remove — forwards as Remove.
    #[test]
    fn push_parent_remove() {
        let (j, log) = mk_join(
            vec![],
            vec![row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(7)))])],
        );
        let rem = Change::Remove(RemoveChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(7)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_parent(rem).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Remove(_)));
    }

    // Branch: push_parent Child — forwards as Child preserving subchild.
    #[test]
    fn push_parent_child() {
        let (j, log) = mk_join(vec![], vec![]);
        let c = Change::Child(ChildChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
            child: ChildSpec {
                relationship_name: "other".into(),
                change: Box::new(Change::Add(AddChange {
                    node: Node {
                        row: row_pairs(&[("x", v(json!(1)))]),
                        relationships: IndexMap::new(),
                    },
                })),
            },
        });
        let _: Vec<_> = j.push_parent(c).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Child(_)));
    }

    // Branch: push_parent Edit asserts parent_key equality — passing case.
    #[test]
    fn push_parent_edit_preserves_key() {
        let (j, log) = mk_join(vec![], vec![]);
        let e = Change::Edit(EditChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(1))), ("name", v(json!("new")))]),
                relationships: IndexMap::new(),
            },
            old_node: Node {
                row: row_pairs(&[("id", v(json!(1))), ("name", v(json!("old")))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_parent(e).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Edit(_)));
    }

    // Branch: push_parent Edit panics if parent key changed.
    #[test]
    #[should_panic(expected = "Parent edit must not change relationship")]
    fn push_parent_edit_changing_key_panics() {
        let (j, _) = mk_join(vec![], vec![]);
        let e = Change::Edit(EditChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(2)))]),
                relationships: IndexMap::new(),
            },
            old_node: Node {
                row: row_pairs(&[("id", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_parent(e).collect();
    }

    // Branch: push_child Add — emits ChildChange for every matching parent.
    #[test]
    fn push_child_add_emits_childchange_per_parent() {
        let parents = vec![
            row_pairs(&[("id", v(json!(1)))]),
            row_pairs(&[("id", v(json!(2)))]),
        ];
        let (j, log) = mk_join(parents, vec![]);
        let add = Change::Add(AddChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_child(add).collect();
        let log = log.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert!(matches!(log[0], Change::Child(_)));
    }

    // Branch: push_child with NULL key → no parent matches → no push.
    #[test]
    fn push_child_null_key_no_push() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, log) = mk_join(parents, vec![]);
        let add = Change::Add(AddChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", Some(json!(null)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_child(add).collect();
        assert_eq!(log.lock().unwrap().len(), 0);
    }

    // Branch: push_child Remove routes through push_child_change.
    #[test]
    fn push_child_remove_emits_childchange() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, log) = mk_join(parents, vec![]);
        let rem = Change::Remove(RemoveChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_child(rem).collect();
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    // Branch: push_child Edit panics when child key changes.
    #[test]
    #[should_panic(expected = "Child edit must not change relationship")]
    fn push_child_edit_key_change_panics() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, _) = mk_join(parents, vec![]);
        let e = Change::Edit(EditChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(2)))]),
                relationships: IndexMap::new(),
            },
            old_node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_child(e).collect();
    }

    // Branch: push_child Child — pushes ChildChange to every parent.
    #[test]
    fn push_child_child_emits() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, log) = mk_join(parents, vec![]);
        let c = Change::Child(ChildChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
            child: ChildSpec {
                relationship_name: "gc".into(),
                change: Box::new(Change::Add(AddChange {
                    node: Node {
                        row: row_pairs(&[]),
                        relationships: IndexMap::new(),
                    },
                })),
            },
        });
        let _: Vec<_> = j.push_child(c).collect();
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    // Branch: destroy propagates to both upstream inputs.
    #[test]
    fn destroy_destroys_both_upstreams() {
        let ps = make_schema("p", "id");
        let cs = make_schema("c", "cid");
        let (p, pdestroy) = MockInput::new(ps, vec![]);
        let (c, cdestroy) = MockInput::new(cs, vec![]);
        let mut j = Join::new(JoinArgs {
            parent: p,
            child: c,
            parent_key: vec!["id".into()],
            child_key: vec!["pid".into()],
            relationship_name: "r".into(),
            hidden: false,
            system: System::Test,
        });
        j.destroy();
        assert!(pdestroy.load(AtOrdering::SeqCst));
        assert!(cdestroy.load(AtOrdering::SeqCst));
    }

    // Branch: Output::push on Join panics (internal wiring only).
    #[test]
    #[should_panic(expected = "Join::push called directly")]
    fn output_push_on_join_panics() {
        let (mut j, _) = mk_join(vec![], vec![]);
        let ps = make_schema("x", "id");
        let (dummy, _) = MockInput::new(ps, vec![]);
        let _: Vec<_> = (&mut j as &mut dyn Output)
            .push(
                Change::Add(AddChange {
                    node: Node {
                        row: row_pairs(&[("id", v(json!(1)))]),
                        relationships: IndexMap::new(),
                    },
                }),
                &*dummy as &dyn InputBase,
            )
            .collect();
    }

    // Branch: push_parent with no output set → panics with "Output not set".
    #[test]
    #[should_panic(expected = "Output not set")]
    fn push_parent_without_output_panics() {
        let ps = make_schema("p", "id");
        let cs = make_schema("c", "cid");
        let (p, _) = MockInput::new(ps, vec![]);
        let (c, _) = MockInput::new(cs, vec![]);
        let j = Join::new(JoinArgs {
            parent: p,
            child: c,
            parent_key: vec!["id".into()],
            child_key: vec!["pid".into()],
            relationship_name: "r".into(),
            hidden: false,
            system: System::Test,
        });
        let _: Vec<_> = j
            .push_parent(Change::Add(AddChange {
                node: Node {
                    row: row_pairs(&[("id", v(json!(1)))]),
                    relationships: IndexMap::new(),
                },
            }))
            .collect();
    }

    // Branch: fetch with constraint narrows parent set.
    #[test]
    fn fetch_with_constraint_filters_parent() {
        let parents = vec![
            row_pairs(&[("id", v(json!(1)))]),
            row_pairs(&[("id", v(json!(2)))]),
        ];
        let (j, _) = mk_join(parents, vec![]);
        let mut c: Constraint = Constraint::new();
        c.insert("id".into(), v(json!(2)));
        let out: Vec<NodeOrYield> = j
            .fetch(FetchRequest {
                constraint: Some(c),
                ..FetchRequest::default()
            })
            .collect();
        assert_eq!(out.len(), 1);
        if let NodeOrYield::Node(n) = &out[0] {
            assert_eq!(n.row.get("id"), Some(&v(json!(2))));
        }
    }
}
