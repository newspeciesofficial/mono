//! Port of `packages/zql/src/ivm/flipped-join.ts`.
//!
//! Public exports ported:
//!
//! - [`FlippedJoin`] — inner join that fetches children first and then
//!   iterates their related parents. Output nodes are parent rows in
//!   parent-order carrying a `relationshipName` factory with the matched
//!   children. Unlike [`crate::ivm::join::Join`], a parent without
//!   children is *not* emitted, despite the name "flipped" (see the TS
//!   doc comment reproduced on the type).
//!
//! ## Ownership divergence from TS
//!
//! Same divergence as [`crate::ivm::join::Join`] — the TS ctor wires
//! `parent.setOutput` / `child.setOutput` directly to the operator's
//! private push methods; in Rust we expose [`FlippedJoin::push_parent`]
//! and [`FlippedJoin::push_child`] as public entry points and leave the
//! routing to the pipeline driver (Layer 11).

use std::cmp::Ordering as CmpOrdering;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use indexmap::IndexMap;
use zero_cache_types::ast::{CompoundKey, System};
use zero_cache_types::value::{Row, Value};

use crate::ivm::change::{AddChange, Change, ChildChange, ChildSpec, EditChange, RemoveChange};
use crate::ivm::constraint::{Constraint, constraints_are_compatible};
use crate::ivm::data::{Node, NodeOrYield, RelationshipFactory};
use crate::ivm::join_utils::{
    JoinChangeOverlay, build_join_constraint, generate_with_overlay_no_yield, is_join_match,
    row_equals_for_compound_key,
};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Operator, Output};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

/// TS `FlippedJoin`.
pub struct FlippedJoin {
    parent: Arc<Mutex<Box<dyn Input>>>,
    child: Arc<Mutex<Box<dyn Input>>>,
    parent_key: CompoundKey,
    child_key: CompoundKey,
    relationship_name: String,
    schema: SourceSchema,
    parent_schema: SourceSchema,
    child_schema: SourceSchema,
    output: Arc<Mutex<Option<Box<dyn Output>>>>,
    inprogress_child_change: Arc<Mutex<Option<JoinChangeOverlay>>>,
}

/// TS `Args` for FlippedJoin ctor.
pub struct FlippedJoinArgs {
    pub parent: Box<dyn Input>,
    pub child: Box<dyn Input>,
    pub parent_key: CompoundKey,
    pub child_key: CompoundKey,
    pub relationship_name: String,
    pub hidden: bool,
    pub system: System,
}

impl FlippedJoin {
    /// TS `new FlippedJoin(args)`.
    pub fn new(args: FlippedJoinArgs) -> Self {
        assert_eq!(
            args.parent_key.len(),
            args.child_key.len(),
            "The parentKey and childKey keys must have same length",
        );

        let parent_schema_inner = args.parent.get_schema().clone();
        let child_schema_inner = args.child.get_schema().clone();

        let mut relationships = parent_schema_inner.relationships.clone();
        let mut rel_schema = child_schema_inner.clone();
        rel_schema.is_hidden = args.hidden;
        rel_schema.system = args.system;
        relationships.insert(args.relationship_name.clone(), rel_schema);

        let schema = SourceSchema {
            table_name: parent_schema_inner.table_name.clone(),
            columns: parent_schema_inner.columns.clone(),
            primary_key: parent_schema_inner.primary_key.clone(),
            relationships,
            is_hidden: parent_schema_inner.is_hidden,
            system: parent_schema_inner.system,
            compare_rows: Arc::clone(&parent_schema_inner.compare_rows),
            sort: parent_schema_inner.sort.clone(),
        };

        Self {
            parent: Arc::new(Mutex::new(args.parent)),
            child: Arc::new(Mutex::new(args.child)),
            parent_key: args.parent_key,
            child_key: args.child_key,
            relationship_name: args.relationship_name,
            schema,
            parent_schema: parent_schema_inner,
            child_schema: child_schema_inner,
            output: Arc::new(Mutex::new(None)),
            inprogress_child_change: Arc::new(Mutex::new(None)),
        }
    }

    /// Wired variant: returns `Arc<Self>` (no outer Mutex) so back-edges
    /// forward pushes via `&self` without reentrant-lock deadlock.
    pub fn new_wired(args: FlippedJoinArgs) -> Arc<Self> {
        let arc = Arc::new(Self::new(args));
        let parent_back: Box<dyn Output> =
            Box::new(FlippedJoinParentBackEdge(Arc::clone(&arc)));
        let child_back: Box<dyn Output> =
            Box::new(FlippedJoinChildBackEdge(Arc::clone(&arc)));
        arc.parent
            .lock()
            .expect("flipped_join parent mutex poisoned")
            .set_output(parent_back);
        arc.child
            .lock()
            .expect("flipped_join child mutex poisoned")
            .set_output(child_back);
        arc
    }

    /// TS `#pushParent(change)` — only forwards if the parent has at
    /// least one related child (inner-join semantics).
    pub fn push_parent<'a>(&'a self, change: Change) -> Stream<'a, Yield> {
        // Check if the change's parent node has any related child.
        let parent_row = change.node().row.clone();
        let has_related = self.has_related_child(&parent_row);
        if !has_related {
            return Box::new(std::iter::empty());
        }

        let downstream: Change = match change {
            Change::Add(AddChange { node }) => Change::Add(AddChange {
                node: self.flip_parent_node(node),
            }),
            Change::Remove(RemoveChange { node }) => Change::Remove(RemoveChange {
                node: self.flip_parent_node(node),
            }),
            Change::Child(ChildChange { node, child }) => Change::Child(ChildChange {
                node: self.flip_parent_node(node),
                child,
            }),
            Change::Edit(EditChange { node, old_node }) => {
                assert!(
                    row_equals_for_compound_key(&old_node.row, &node.row, &self.parent_key),
                    "Parent edit must not change relationship.",
                );
                Change::Edit(EditChange {
                    node: self.flip_parent_node(node),
                    old_node: self.flip_parent_node(old_node),
                })
            }
        };
        self.push_to_output(downstream)
    }

    /// TS `#pushChild(change)`.
    pub fn push_child<'a>(&'a self, change: Change) -> Stream<'a, Yield> {
        match &change {
            Change::Add(_) | Change::Remove(_) => self.push_child_change(change, false),
            Change::Edit(edit) => {
                assert!(
                    row_equals_for_compound_key(
                        &edit.old_node.row,
                        &edit.node.row,
                        &self.child_key
                    ),
                    "Child edit must not change relationship.",
                );
                self.push_child_change(change, true)
            }
            Change::Child(_) => self.push_child_change(change, true),
        }
    }

    /// TS `#pushChildChange(change, exists?)`.
    ///
    /// `exists` mirrors TS: when true, we skip the exists-probe and
    /// always emit a ChildChange; when false we need to check whether
    /// the parent already has some *other* matching child (and convert
    /// to Add/Remove otherwise).
    fn push_child_change<'a>(&'a self, change: Change, initial_exists: bool) -> Stream<'a, Yield> {
        {
            let mut guard = self.inprogress_child_change.lock().unwrap();
            *guard = Some(JoinChangeOverlay {
                change: change.clone(),
                position: None,
            });
        }
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
        let _g = ClearGuard {
            slot: &self.inprogress_child_change,
        };

        // Resolve matching parents for this child row.
        let constraint_opt =
            build_join_constraint(&change.node().row, &self.child_key, &self.parent_key);
        let parent_nodes: Vec<NodeOrYield> = match constraint_opt {
            None => Vec::new(),
            Some(constraint) => {
                let parent_guard = self.parent.lock().expect("flipped-join parent poisoned");
                let req = FetchRequest {
                    constraint: Some(constraint),
                    ..FetchRequest::default()
                };
                parent_guard.fetch(req).collect()
            }
        };

        let mut yields_buffer: Vec<Yield> = Vec::new();
        for node in parent_nodes {
            match node {
                NodeOrYield::Yield => {
                    yields_buffer.push(Yield);
                    continue;
                }
                NodeOrYield::Node(parent_node) => {
                    // Update position.
                    {
                        let mut guard = self.inprogress_child_change.lock().unwrap();
                        *guard = Some(JoinChangeOverlay {
                            change: change.clone(),
                            position: Some(parent_node.row.clone()),
                        });
                    }

                    // Build child-stream factory for this parent.
                    let child_stream_factory =
                        self.build_child_factory_for_parent(&parent_node.row);

                    // Decide `exists`: if we weren't pre-told, probe for
                    // any child that isn't the row being changed.
                    let mut exists = initial_exists;
                    if !exists {
                        let child_iter = child_stream_factory();
                        let change_row_key_cmp = Arc::clone(&self.child_schema.compare_rows);
                        for child in child_iter {
                            match child {
                                NodeOrYield::Yield => {
                                    yields_buffer.push(Yield);
                                    continue;
                                }
                                NodeOrYield::Node(child_node) => {
                                    if (change_row_key_cmp)(&child_node.row, &change.node().row)
                                        != CmpOrdering::Equal
                                    {
                                        exists = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if exists {
                        // Push as ChildChange.
                        let factory: RelationshipFactory = Box::new(move || {
                            Box::new(child_stream_factory().collect::<Vec<_>>().into_iter())
                        });
                        let mut new_rels = parent_node.relationships;
                        new_rels.insert(self.relationship_name.clone(), factory);
                        let flipped = Node {
                            row: parent_node.row,
                            relationships: new_rels,
                        };
                        let out_change = Change::Child(ChildChange {
                            node: flipped,
                            child: ChildSpec {
                                relationship_name: self.relationship_name.clone(),
                                change: Box::new(change.clone()),
                            },
                        });
                        for y in self.push_to_output(out_change) {
                            yields_buffer.push(y);
                        }
                    } else {
                        // Push the change as-is (Add or Remove) with the
                        // relationship materialised to `[change.node]`.
                        let change_node = change.node().clone();
                        let factory: RelationshipFactory = Box::new(move || {
                            Box::new(vec![NodeOrYield::Node(change_node.clone())].into_iter())
                        });
                        let mut new_rels = parent_node.relationships;
                        new_rels.insert(self.relationship_name.clone(), factory);
                        let flipped = Node {
                            row: parent_node.row,
                            relationships: new_rels,
                        };
                        let out_change = match &change {
                            Change::Add(_) => Change::Add(AddChange { node: flipped }),
                            Change::Remove(_) => Change::Remove(RemoveChange { node: flipped }),
                            // Edit / Child go through exists=true path above,
                            // so these arms are unreachable here. Replicate
                            // as identity to match TS `{...change, node}`.
                            Change::Edit(EditChange { old_node, .. }) => Change::Edit(EditChange {
                                node: flipped,
                                old_node: old_node.clone(),
                            }),
                            Change::Child(ChildChange { child, .. }) => {
                                Change::Child(ChildChange {
                                    node: flipped,
                                    child: child.clone(),
                                })
                            }
                        };
                        for y in self.push_to_output(out_change) {
                            yields_buffer.push(y);
                        }
                    }
                }
            }
        }

        Box::new(yields_buffer.into_iter())
    }

    fn flip_parent_node(&self, node: Node) -> Node {
        let parent_row = node.row.clone();
        let factory = self.build_child_factory_for_parent(&parent_row);
        let mut rels = node.relationships;
        let collected: RelationshipFactory =
            Box::new(move || Box::new(factory().collect::<Vec<_>>().into_iter()));
        rels.insert(self.relationship_name.clone(), collected);
        Node {
            row: node.row,
            relationships: rels,
        }
    }

    /// Returns a cloneable closure that, when called, yields the child
    /// stream for `parent_row`.
    fn build_child_factory_for_parent(
        &self,
        parent_row: &Row,
    ) -> Box<dyn Fn() -> Box<dyn Iterator<Item = NodeOrYield>> + Send + Sync> {
        let child = Arc::clone(&self.child);
        let parent_row = parent_row.clone();
        let parent_key = self.parent_key.clone();
        let child_key = self.child_key.clone();
        Box::new(move || {
            let constraint_opt = build_join_constraint(&parent_row, &parent_key, &child_key);
            match constraint_opt {
                None => Box::new(std::iter::empty::<NodeOrYield>())
                    as Box<dyn Iterator<Item = NodeOrYield>>,
                Some(constraint) => {
                    let guard = child.lock().expect("flipped-join child poisoned");
                    let req = FetchRequest {
                        constraint: Some(constraint),
                        ..FetchRequest::default()
                    };
                    let nodes: Vec<NodeOrYield> = guard.fetch(req).collect();
                    Box::new(nodes.into_iter())
                }
            }
        })
    }

    /// Returns whether the given parent row has at least one related
    /// child. Equivalent to the TS snippet that breaks out of the
    /// `childNodeStream` iterator on first concrete node.
    fn has_related_child(&self, parent_row: &Row) -> bool {
        let constraint_opt = build_join_constraint(parent_row, &self.parent_key, &self.child_key);
        let Some(constraint) = constraint_opt else {
            return false;
        };
        let guard = self.child.lock().expect("flipped-join child poisoned");
        let req = FetchRequest {
            constraint: Some(constraint),
            ..FetchRequest::default()
        };
        for n in guard.fetch(req) {
            if matches!(n, NodeOrYield::Node(_)) {
                return true;
            }
        }
        false
    }

    fn push_to_output<'a>(&'a self, change: Change) -> Stream<'a, Yield> {
        // Do NOT hold `self.parent` lock across downstream push — the
        // downstream may call back into `FlippedJoin::fetch` (which
        // locks `self.parent`), causing a reentrant Mutex deadlock.
        // Use `self` as the pusher identity (FlippedJoin implements
        // InputBase via cached schema).
        let mut out = self.output.lock().expect("flipped-join output poisoned");
        match out.as_mut() {
            None => panic!("Output not set"),
            Some(sink) => {
                let collected: Vec<Yield> = sink.push(change, self as &dyn InputBase).collect();
                Box::new(collected.into_iter())
            }
        }
    }
}

impl FlippedJoin {
    /// &self variant of destroy — call from `Arc<Self>`.
    pub fn destroy_arc(&self) {
        {
            let mut c = self.child.lock().expect("flipped-join child poisoned");
            c.destroy();
        }
        {
            let mut p = self.parent.lock().expect("flipped-join parent poisoned");
            p.destroy();
        }
    }

    /// &self variant of set_output.
    pub fn set_output_arc(&self, output: Box<dyn Output>) {
        *self.output.lock().expect("flipped-join output poisoned") = Some(output);
    }
}

impl InputBase for FlippedJoin {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn destroy(&mut self) {
        self.destroy_arc();
    }
}

impl Input for FlippedJoin {
    fn set_output(&mut self, output: Box<dyn Output>) {
        *self.output.lock().expect("flipped-join output poisoned") = Some(output);
    }

    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        // Translate parent-side constraints on join-key columns to
        // child-side constraints.
        let child_constraint: Option<Constraint> = match &req.constraint {
            None => None,
            Some(parent_c) => {
                let mut cc = Constraint::new();
                let mut had = false;
                for (k, v) in parent_c.iter() {
                    if let Some(idx) = self.parent_key.iter().position(|pk| pk == k) {
                        cc.insert(self.child_key[idx].clone(), v.clone());
                        had = true;
                    }
                }
                if had { Some(cc) } else { None }
            }
        };

        // Fetch children first.
        let mut child_nodes: Vec<Node> = Vec::new();
        let mut preyields: Vec<NodeOrYield> = Vec::new();
        {
            let child_guard = self.child.lock().expect("flipped-join child poisoned");
            let creq = FetchRequest {
                constraint: child_constraint,
                ..FetchRequest::default()
            };
            for n in child_guard.fetch(creq) {
                match n {
                    NodeOrYield::Yield => preyields.push(NodeOrYield::Yield),
                    NodeOrYield::Node(c) => child_nodes.push(c),
                }
            }
        }

        // If an inprogress child remove is live, re-insert the removed
        // node into child_nodes in its sorted slot (undoing the removal
        // so parents greater than `position` can still match it).
        {
            let guard = self.inprogress_child_change.lock().unwrap();
            if let Some(o) = guard.as_ref() {
                if let Change::Remove(rem) = &o.change {
                    let cmp = Arc::clone(&self.child_schema.compare_rows);
                    // Binary search for insert position.
                    let pos = child_nodes
                        .binary_search_by(|probe| (cmp)(&probe.row, &rem.node.row))
                        .unwrap_or_else(|e| e);
                    child_nodes.insert(pos, rem.node.clone());
                }
            }
        }

        // For each child, fetch matching parents.
        //
        // Rust divergence: TS uses an iterator-of-iterators with manual
        // `next()` pumping for the merge-sort. We collect each per-child
        // parent list into a Vec up-front because our mock / real sources
        // all drain lazily but allow re-iteration; the memory cost is
        // bounded by the join's fan-out.
        let parent_lists: Vec<Vec<Node>> = child_nodes
            .iter()
            .map(|child_node| {
                let constraint_opt =
                    build_join_constraint(&child_node.row, &self.child_key, &self.parent_key);
                match constraint_opt {
                    None => Vec::new(),
                    Some(c) => {
                        if let Some(rc) = &req.constraint {
                            if !constraints_are_compatible(&c, rc) {
                                return Vec::new();
                            }
                        }
                        let guard = self.parent.lock().expect("flipped-join parent poisoned");
                        let mut merged_constraint = c;
                        if let Some(rc) = &req.constraint {
                            for (k, v) in rc.iter() {
                                merged_constraint
                                    .entry(k.clone())
                                    .or_insert_with(|| v.clone());
                            }
                        }
                        let preq = FetchRequest {
                            constraint: Some(merged_constraint),
                            start: req.start.clone(),
                            reverse: req.reverse,
                        };
                        guard
                            .fetch(preq)
                            .filter_map(|n| match n {
                                NodeOrYield::Node(p) => Some(p),
                                NodeOrYield::Yield => None,
                            })
                            .collect()
                    }
                }
            })
            .collect();

        // Merge-sort the per-child parent streams.
        let reverse = req.reverse.unwrap_or(false);
        let schema_cmp = Arc::clone(&self.schema.compare_rows);
        let mut cursors: Vec<usize> = vec![0; parent_lists.len()];

        // Capture the in-progress overlay once, then evaluate per-parent.
        let overlay_snapshot: Option<JoinChangeOverlay> =
            self.inprogress_child_change.lock().unwrap().clone();

        let mut out: Vec<NodeOrYield> = preyields;
        loop {
            let mut min_idx: Option<usize> = None;
            let mut min_parent_group: Vec<usize> = Vec::new();
            for (i, cur) in cursors.iter().enumerate() {
                if *cur >= parent_lists[i].len() {
                    continue;
                }
                let candidate = &parent_lists[i][*cur];
                match min_idx {
                    None => {
                        min_idx = Some(i);
                        min_parent_group = vec![i];
                    }
                    Some(mi) => {
                        let min_node = &parent_lists[mi][cursors[mi]];
                        let c = (schema_cmp)(&candidate.row, &min_node.row);
                        let c = if reverse { c.reverse() } else { c };
                        if c == CmpOrdering::Equal {
                            min_parent_group.push(i);
                        } else if c == CmpOrdering::Less {
                            min_idx = Some(i);
                            min_parent_group = vec![i];
                        }
                    }
                }
            }
            let Some(_mi) = min_idx else { break };

            // Gather related child nodes for this parent group.
            let min_parent_node =
                parent_lists[min_parent_group[0]][cursors[min_parent_group[0]]].clone();
            let mut related_child_nodes: Vec<Node> = Vec::new();
            for i in &min_parent_group {
                related_child_nodes.push(child_nodes[*i].clone());
                cursors[*i] += 1;
            }

            // Apply overlay if applicable.
            let mut overlaid: Vec<Node> = related_child_nodes.clone();
            if let Some(ov) = &overlay_snapshot {
                if let Some(pos) = &ov.position {
                    if is_join_match(
                        &ov.change.node().row,
                        &self.child_key,
                        &min_parent_node.row,
                        &self.parent_key,
                    ) {
                        let pushed_for_this =
                            (self.parent_schema.compare_rows)(&min_parent_node.row, pos)
                                != CmpOrdering::Greater;
                        match &ov.change {
                            Change::Remove(rem) => {
                                if pushed_for_this {
                                    overlaid = related_child_nodes
                                        .into_iter()
                                        .filter(|n| {
                                            (self.child_schema.compare_rows)(&n.row, &rem.node.row)
                                                != CmpOrdering::Equal
                                        })
                                        .collect();
                                }
                            }
                            _ => {
                                if !pushed_for_this {
                                    // Apply overlay via generate_with_overlay_no_yield.
                                    overlaid = generate_with_overlay_no_yield(
                                        related_child_nodes.clone(),
                                        ov.change.clone(),
                                        self.child_schema.clone(),
                                    )
                                    .collect();
                                }
                            }
                        }
                    }
                }
            }

            if !overlaid.is_empty() {
                let rel_name = self.relationship_name.clone();
                let factory: RelationshipFactory = Box::new(move || {
                    Box::new(
                        overlaid
                            .clone()
                            .into_iter()
                            .map(NodeOrYield::Node)
                            .collect::<Vec<_>>()
                            .into_iter(),
                    )
                });
                let mut rels = min_parent_node.relationships;
                rels.insert(rel_name, factory);
                out.push(NodeOrYield::Node(Node {
                    row: min_parent_node.row,
                    relationships: rels,
                }));
            }
        }

        let _ = (schema_cmp, reverse);
        Box::new(out.into_iter())
    }
}

impl Output for FlippedJoin {
    fn push<'a>(&'a mut self, _change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        panic!("FlippedJoin::push called directly — use push_parent / push_child instead",);
    }
}

impl Operator for FlippedJoin {}

/// Back-edge installed on [`FlippedJoin::parent`]. Holds `Arc<FlippedJoin>`
/// directly (no outer Mutex); all methods on FlippedJoin already take
/// `&self` thanks to interior Mutex wrappers.
pub struct FlippedJoinParentBackEdge(pub Arc<FlippedJoin>);

impl Output for FlippedJoinParentBackEdge {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        eprintln!("[TRACE ivm] FlippedJoin::parent::push enter");
        let items: Vec<Yield> = self.0.push_parent(change).collect();
        eprintln!("[TRACE ivm] FlippedJoin::parent::push exit yields={}", items.len());
        Box::new(items.into_iter())
    }
}

/// Back-edge installed on [`FlippedJoin::child`]. Holds `Arc<FlippedJoin>`.
pub struct FlippedJoinChildBackEdge(pub Arc<FlippedJoin>);

impl Output for FlippedJoinChildBackEdge {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        eprintln!("[TRACE ivm] FlippedJoin::child::push enter");
        let items: Vec<Yield> = self.0.push_child(change).collect();
        eprintln!("[TRACE ivm] FlippedJoin::child::push exit yields={}", items.len());
        Box::new(items.into_iter())
    }
}

/// Adapter to plug a wired [`Arc<FlippedJoin>`] back into the chain
/// as `Box<dyn Input>`. Schema cached at construction.
pub struct ArcFlippedJoinAsInput {
    inner: Arc<FlippedJoin>,
    schema: SourceSchema,
}

impl ArcFlippedJoinAsInput {
    pub fn new(inner: Arc<FlippedJoin>) -> Self {
        let schema = inner.get_schema().clone();
        Self { inner, schema }
    }
}

impl InputBase for ArcFlippedJoinAsInput {
    fn get_schema(&self) -> &SourceSchema { &self.schema }
    fn destroy(&mut self) { self.inner.destroy_arc(); }
}

impl Input for ArcFlippedJoinAsInput {
    fn set_output(&mut self, output: Box<dyn Output>) {
        self.inner.set_output_arc(output);
    }
    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        let items: Vec<NodeOrYield> = self.inner.fetch(req).collect();
        Box::new(items.into_iter())
    }
}

impl Output for ArcFlippedJoinAsInput {
    fn push<'a>(&'a mut self, _change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        panic!("ArcFlippedJoinAsInput::push — use push_parent / push_child on FlippedJoin");
    }
}

impl Operator for ArcFlippedJoinAsInput {}

// Avoid unused-import warnings if paths aren't hit in tests.
#[allow(dead_code)]
fn _type_assertions(_v: Option<Value>) {}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage.

    use super::*;
    use crate::ivm::constraint::{Constraint, constraint_matches_row};
    use crate::ivm::data::make_comparator;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering as AtOrdering};
    use zero_cache_types::ast::{Direction, Ordering as AstOrdering};
    use zero_cache_types::primary_key::PrimaryKey;

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
        fn set_output(&mut self, _o: Box<dyn Output>) {}
        fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
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
            if req.reverse.unwrap_or(false) {
                matched.reverse();
            }
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
        fn push<'a>(&'a mut self, change: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.log.lock().unwrap().push(change);
            Box::new(std::iter::empty())
        }
    }

    fn mk(parent_rows: Vec<Row>, child_rows: Vec<Row>) -> (FlippedJoin, Arc<Mutex<Vec<Change>>>) {
        let ps = make_schema("parent", "id");
        let cs = make_schema("child", "cid");
        let (p, _) = MockInput::new(ps, parent_rows);
        let (c, _) = MockInput::new(cs, child_rows);
        let mut j = FlippedJoin::new(FlippedJoinArgs {
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

    // Branch: ctor panics on mismatched key lengths.
    #[test]
    #[should_panic(expected = "same length")]
    fn new_panics_on_mismatched_keys() {
        let ps = make_schema("p", "id");
        let cs = make_schema("c", "cid");
        let (p, _) = MockInput::new(ps, vec![]);
        let (c, _) = MockInput::new(cs, vec![]);
        let _ = FlippedJoin::new(FlippedJoinArgs {
            parent: p,
            child: c,
            parent_key: vec!["id".into(), "x".into()],
            child_key: vec!["pid".into()],
            relationship_name: "r".into(),
            hidden: false,
            system: System::Test,
        });
    }

    // Branch: schema relationship with hidden + system propagated.
    #[test]
    fn new_schema_merges_relationship() {
        let ps = make_schema("p", "id");
        let cs = make_schema("c", "cid");
        let (p, _) = MockInput::new(ps, vec![]);
        let (c, _) = MockInput::new(cs, vec![]);
        let j = FlippedJoin::new(FlippedJoinArgs {
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
    }

    // Branch: fetch empty child => no output (even if parents exist).
    #[test]
    fn fetch_empty_child_yields_empty() {
        let (j, _) = mk(vec![row_pairs(&[("id", v(json!(1)))])], vec![]);
        let out: Vec<_> = j.fetch(FetchRequest::default()).collect();
        assert!(out.is_empty());
    }

    // Branch: fetch single match yields parent with kids relationship.
    #[test]
    fn fetch_single_match() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let kids = vec![row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))])];
        let (j, _) = mk(parents, kids);
        let out: Vec<NodeOrYield> = j.fetch(FetchRequest::default()).collect();
        assert_eq!(out.len(), 1);
        if let NodeOrYield::Node(n) = &out[0] {
            let cs: Vec<NodeOrYield> = n.relationships["kids"]().collect();
            assert_eq!(cs.len(), 1);
        }
    }

    // Branch: fetch parent with zero matching child is NOT emitted.
    #[test]
    fn fetch_no_child_match_skips_parent() {
        let parents = vec![
            row_pairs(&[("id", v(json!(1)))]),
            row_pairs(&[("id", v(json!(2)))]),
        ];
        let kids = vec![row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(1)))])];
        let (j, _) = mk(parents, kids);
        let out: Vec<NodeOrYield> = j.fetch(FetchRequest::default()).collect();
        assert_eq!(out.len(), 1); // only id=1 emitted
    }

    // Branch: fetch merges multiple children to one parent.
    #[test]
    fn fetch_multi_children_grouped_under_parent() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let kids = vec![
            row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
            row_pairs(&[("cid", v(json!(11))), ("pid", v(json!(1)))]),
        ];
        let (j, _) = mk(parents, kids);
        let out: Vec<NodeOrYield> = j.fetch(FetchRequest::default()).collect();
        assert_eq!(out.len(), 1);
        if let NodeOrYield::Node(n) = &out[0] {
            assert_eq!(n.relationships["kids"]().count(), 2);
        }
    }

    // Branch: fetch with a parent-side constraint on join key -> child-side.
    #[test]
    fn fetch_translates_parent_constraint_to_child() {
        let parents = vec![
            row_pairs(&[("id", v(json!(1)))]),
            row_pairs(&[("id", v(json!(2)))]),
        ];
        let kids = vec![
            row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
            row_pairs(&[("cid", v(json!(20))), ("pid", v(json!(2)))]),
        ];
        let (j, _) = mk(parents, kids);
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

    // Branch: push_parent with no related child emits nothing.
    #[test]
    fn push_parent_no_children_no_push() {
        let (j, log) = mk(vec![], vec![]);
        let add = Change::Add(AddChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_parent(add).collect();
        assert_eq!(log.lock().unwrap().len(), 0);
    }

    // Branch: push_parent Add forwards when child exists.
    #[test]
    fn push_parent_add_with_child() {
        let kids = vec![row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(1)))])];
        let (j, log) = mk(vec![], kids);
        let add = Change::Add(AddChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_parent(add).collect();
        assert_eq!(log.lock().unwrap().len(), 1);
        assert!(matches!(log.lock().unwrap()[0], Change::Add(_)));
    }

    // Branch: push_parent Remove forwards.
    #[test]
    fn push_parent_remove_with_child() {
        let kids = vec![row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(1)))])];
        let (j, log) = mk(vec![], kids);
        let rem = Change::Remove(RemoveChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_parent(rem).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Remove(_)));
    }

    // Branch: push_parent Edit with key-change panics.
    #[test]
    #[should_panic(expected = "Parent edit must not change relationship")]
    fn push_parent_edit_key_change_panics() {
        // Include children for BOTH id=1 and id=2 so has_related_child
        // returns true for the new node; the key-equality assert fires.
        let kids = vec![
            row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(1)))]),
            row_pairs(&[("cid", v(json!(2))), ("pid", v(json!(2)))]),
        ];
        let (j, _) = mk(vec![], kids);
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

    // Branch: push_parent Child carries subchild.
    #[test]
    fn push_parent_child_forwards() {
        let kids = vec![row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(1)))])];
        let (j, log) = mk(vec![], kids);
        let c = Change::Child(ChildChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
            child: ChildSpec {
                relationship_name: "x".into(),
                change: Box::new(Change::Add(AddChange {
                    node: Node {
                        row: IndexMap::new(),
                        relationships: IndexMap::new(),
                    },
                })),
            },
        });
        let _: Vec<_> = j.push_parent(c).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Child(_)));
    }

    // Branch: push_parent Edit preserves key — forwards Edit.
    #[test]
    fn push_parent_edit_forwards() {
        let kids = vec![row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(1)))])];
        let (j, log) = mk(vec![], kids);
        let e = Change::Edit(EditChange {
            node: Node {
                row: row_pairs(&[("id", v(json!(1))), ("name", v(json!("n")))]),
                relationships: IndexMap::new(),
            },
            old_node: Node {
                row: row_pairs(&[("id", v(json!(1))), ("name", v(json!("o")))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_parent(e).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Edit(_)));
    }

    // Branch: push_child Add with no other child → emits Add.
    #[test]
    fn push_child_add_no_existing_emits_add() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, log) = mk(parents, vec![]);
        let add = Change::Add(AddChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_child(add).collect();
        assert_eq!(log.lock().unwrap().len(), 1);
        assert!(matches!(log.lock().unwrap()[0], Change::Add(_)));
    }

    // Branch: push_child Add with an existing sibling child → emits ChildChange.
    #[test]
    fn push_child_add_with_existing_sibling_emits_childchange() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let kids = vec![row_pairs(&[("cid", v(json!(5))), ("pid", v(json!(1)))])];
        let (j, log) = mk(parents, kids);
        let add = Change::Add(AddChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_child(add).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Child(_)));
    }

    // Branch: push_child Remove of the only child → emits Remove.
    #[test]
    fn push_child_remove_only_child_emits_remove() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, log) = mk(parents, vec![]);
        // child source returns empty so `exists` probe finds no other children;
        // change is Remove → falls into !exists branch.
        let rem = Change::Remove(RemoveChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_child(rem).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Remove(_)));
    }

    // Branch: push_child Edit with key-change panics.
    #[test]
    #[should_panic(expected = "Child edit must not change relationship")]
    fn push_child_edit_key_change_panics() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, _) = mk(parents, vec![]);
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

    // Branch: push_child Edit preserves key → emits ChildChange (exists=true).
    #[test]
    fn push_child_edit_preserves_key_emits_childchange() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, log) = mk(parents, vec![]);
        let e = Change::Edit(EditChange {
            node: Node {
                row: row_pairs(&[
                    ("cid", v(json!(10))),
                    ("pid", v(json!(1))),
                    ("nm", v(json!("n"))),
                ]),
                relationships: IndexMap::new(),
            },
            old_node: Node {
                row: row_pairs(&[
                    ("cid", v(json!(10))),
                    ("pid", v(json!(1))),
                    ("nm", v(json!("o"))),
                ]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_child(e).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Child(_)));
    }

    // Branch: push_child Child emits ChildChange (exists=true by default).
    #[test]
    fn push_child_child_emits_childchange() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, log) = mk(parents, vec![]);
        let c = Change::Child(ChildChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", v(json!(1)))]),
                relationships: IndexMap::new(),
            },
            child: ChildSpec {
                relationship_name: "x".into(),
                change: Box::new(Change::Add(AddChange {
                    node: Node {
                        row: IndexMap::new(),
                        relationships: IndexMap::new(),
                    },
                })),
            },
        });
        let _: Vec<_> = j.push_child(c).collect();
        assert!(matches!(log.lock().unwrap()[0], Change::Child(_)));
    }

    // Branch: push_child with NULL foreign key → no parent match, no push.
    #[test]
    fn push_child_null_fk_no_push() {
        let parents = vec![row_pairs(&[("id", v(json!(1)))])];
        let (j, log) = mk(parents, vec![]);
        let add = Change::Add(AddChange {
            node: Node {
                row: row_pairs(&[("cid", v(json!(10))), ("pid", Some(json!(null)))]),
                relationships: IndexMap::new(),
            },
        });
        let _: Vec<_> = j.push_child(add).collect();
        assert_eq!(log.lock().unwrap().len(), 0);
    }

    // Branch: destroy propagates to both upstreams in child-first order.
    #[test]
    fn destroy_propagates() {
        let ps = make_schema("p", "id");
        let cs = make_schema("c", "cid");
        let (p, pd) = MockInput::new(ps, vec![]);
        let (c, cd) = MockInput::new(cs, vec![]);
        let mut j = FlippedJoin::new(FlippedJoinArgs {
            parent: p,
            child: c,
            parent_key: vec!["id".into()],
            child_key: vec!["pid".into()],
            relationship_name: "r".into(),
            hidden: false,
            system: System::Test,
        });
        j.destroy();
        assert!(pd.load(AtOrdering::SeqCst));
        assert!(cd.load(AtOrdering::SeqCst));
    }

    // Branch: Output::push on FlippedJoin panics.
    #[test]
    #[should_panic(expected = "FlippedJoin::push called directly")]
    fn output_push_panics() {
        let (mut j, _) = mk(vec![], vec![]);
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

    // Branch: push_parent with no output panics.
    #[test]
    #[should_panic(expected = "Output not set")]
    fn push_parent_without_output_panics() {
        let kids = vec![row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(1)))])];
        let ps = make_schema("p", "id");
        let cs = make_schema("c", "cid");
        let (p, _) = MockInput::new(ps, vec![]);
        let (c, _) = MockInput::new(cs, kids);
        let j = FlippedJoin::new(FlippedJoinArgs {
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

    // Branch: fetch with reverse flag reverses parent order.
    #[test]
    fn fetch_reverse_order() {
        let parents = vec![
            row_pairs(&[("id", v(json!(1)))]),
            row_pairs(&[("id", v(json!(2)))]),
        ];
        let kids = vec![
            row_pairs(&[("cid", v(json!(1))), ("pid", v(json!(1)))]),
            row_pairs(&[("cid", v(json!(2))), ("pid", v(json!(2)))]),
        ];
        let (j, _) = mk(parents, kids);
        let out: Vec<NodeOrYield> = j
            .fetch(FetchRequest {
                reverse: Some(true),
                ..FetchRequest::default()
            })
            .collect();
        let ids: Vec<i64> = out
            .into_iter()
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => n
                    .row
                    .get("id")
                    .and_then(|v| v.as_ref())
                    .and_then(|j| j.as_i64()),
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec![2, 1]);
    }
}
