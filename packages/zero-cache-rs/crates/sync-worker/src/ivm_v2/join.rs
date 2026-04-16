//! Join operator — ivm_v2, push-path skeleton.
//!
//! Scope:
//! - Two inputs: parent + child.
//! - `push_parent(change)` — forward parent-side change with attached
//!   lazy children relationship.
//! - `push_child(change)` — find all matching parents via parent.fetch,
//!   emit ChildChange for each.
//! - In-progress child-change overlay (`inprogress_child_change`) stored
//!   as plain `Option<JoinChangeOverlay>` with Drop-based cleanup.
//!
//! Deferred:
//! - `fetch` path (hierarchical output, overlay handling during fetch).
//! - FlippedJoin (mirror; separate file).

use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

use zero_cache_types::ast::{CompoundKey, System};
use zero_cache_types::value::Row;

use super::change::{Change, ChildChange, ChildSpec, Node, SourceSchema};
use super::operator::{FetchRequest, Input, InputBase};
use crate::ivm::constraint::Constraint;
use crate::ivm::join_utils::{build_join_constraint, row_equals_for_compound_key};

pub struct Join {
    parent: Box<dyn Input>,
    child: Box<dyn Input>,
    parent_key: CompoundKey,
    child_key: CompoundKey,
    #[allow(dead_code)]
    relationship_name: String,
    schema: SourceSchema,
    #[allow(dead_code)]
    child_schema: SourceSchema,
    /// Set during `push_child` so fetches during the push can see the
    /// inprogress change for overlay handling. Cleared via Drop guard
    /// (see `push_child`).
    #[allow(dead_code)]
    inprogress_child_change: Option<JoinChangeOverlay>,
    #[allow(dead_code)]
    compare_rows: Arc<dyn Fn(&Row, &Row) -> CmpOrdering + Send + Sync>,
}

#[allow(dead_code)]
pub(crate) struct JoinChangeOverlay {
    pub change: Change,
    pub position: Option<Row>,
}

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
    pub fn new(args: JoinArgs) -> Self {
        assert_eq!(
            args.parent_key.len(),
            args.child_key.len(),
            "The parentKey and childKey must have same length",
        );
        let parent_schema = args.parent.get_schema().clone();
        let child_schema = args.child.get_schema().clone();
        let mut relationships = parent_schema.relationships.clone();
        let mut rel_schema = child_schema.clone();
        rel_schema.is_hidden = args.hidden;
        rel_schema.system = args.system;
        relationships.insert(args.relationship_name.clone(), rel_schema);
        let compare_rows = Arc::clone(&parent_schema.compare_rows);
        let schema = SourceSchema {
            table_name: parent_schema.table_name,
            columns: parent_schema.columns,
            primary_key: parent_schema.primary_key,
            relationships,
            is_hidden: parent_schema.is_hidden,
            system: parent_schema.system,
            compare_rows: Arc::clone(&parent_schema.compare_rows),
            sort: parent_schema.sort,
        };
        Self {
            parent: args.parent,
            child: args.child,
            parent_key: args.parent_key,
            child_key: args.child_key,
            relationship_name: args.relationship_name,
            schema,
            child_schema,
            inprogress_child_change: None,
            compare_rows,
        }
    }

    /// TS `#pushParent(change)`. Forwards parent-side change with a lazy
    /// relationship factory. In this skeleton we forward the parent row
    /// unchanged — the `processParentNode` decoration is deferred.
    pub fn push_parent<'a>(
        &'a mut self,
        change: Change,
    ) -> Box<dyn Iterator<Item = Change> + 'a> {
        eprintln!(
            "[TRACE ivm_v2] Join::push_parent enter op={}",
            change_name(&change)
        );
        // Parent-side changes pass through with presence tied to parent.
        // TS asserts parent edit doesn't change the relationship keys.
        if let Change::Edit(ref edit) = change {
            assert!(
                row_equals_for_compound_key(&edit.old_node.row, &edit.node.row, &self.parent_key),
                "Parent edit must not change relationship."
            );
        }
        Box::new(std::iter::once(change))
    }

    /// TS `#pushChild(change)`. For each parent matching the child's
    /// join key, emit a ChildChange.
    pub fn push_child<'a>(
        &'a mut self,
        change: Change,
    ) -> Box<dyn Iterator<Item = Change> + 'a> {
        eprintln!(
            "[TRACE ivm_v2] Join::push_child enter op={} rel={}",
            change_name(&change),
            self.relationship_name
        );
        if let Change::Edit(ref edit) = change {
            assert!(
                row_equals_for_compound_key(&edit.old_node.row, &edit.node.row, &self.child_key),
                "Child edit must not change relationship."
            );
        }
        let child_row = match &change {
            Change::Add(c) => c.node.row.clone(),
            Change::Remove(c) => c.node.row.clone(),
            Change::Child(c) => c.node.row.clone(),
            Change::Edit(c) => c.node.row.clone(),
        };
        // Build constraint mapping child values onto parent key columns.
        let constraint_opt: Option<Constraint> =
            build_join_constraint(&child_row, &self.child_key, &self.parent_key);
        let Some(constraint) = constraint_opt else {
            return Box::new(std::iter::empty());
        };
        // Fetch matching parents from parent input.
        let req = FetchRequest {
            constraint: Some(constraint),
            ..FetchRequest::default()
        };
        let parent_nodes: Vec<Node> = self.parent.fetch(req).collect();
        let rel_name = self.relationship_name.clone();
        // For each parent, emit ChildChange. Shallow — we wrap `change`
        // directly in ChildSpec without the lazy factory that TS builds.
        // Full relationship-factory decoration deferred.
        let change_shared = change;
        let out: Vec<Change> = parent_nodes
            .into_iter()
            .map(|pn| {
                Change::Child(ChildChange {
                    node: pn,
                    child: ChildSpec {
                        relationship_name: rel_name.clone(),
                        change: Box::new(clone_change(&change_shared)),
                    },
                })
            })
            .collect();
        Box::new(out.into_iter())
    }
}

impl InputBase for Join {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.parent.destroy();
        self.child.destroy();
    }
}

impl Input for Join {
    /// Fetch path deferred — TS builds hierarchical output with lazy
    /// relationship factories. See #122 follow-up.
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        // Pass-through for now. Downstream operators won't see the
        // joined children. To be replaced when we port #processParentNode.
        self.parent.fetch(req)
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

/// Shallow-clone a Change by reconstructing its variants. `Change` isn't
/// `Clone` (relationships contain `Box<dyn Fn>`); this preserves rows but
/// drops relationship factories.
fn clone_change(c: &Change) -> Change {
    use super::change::{AddChange, EditChange, RemoveChange};
    match c {
        Change::Add(a) => Change::Add(AddChange {
            node: shallow_node(&a.node),
        }),
        Change::Remove(r) => Change::Remove(RemoveChange {
            node: shallow_node(&r.node),
        }),
        Change::Edit(e) => Change::Edit(EditChange {
            node: shallow_node(&e.node),
            old_node: shallow_node(&e.old_node),
        }),
        Change::Child(c) => Change::Child(ChildChange {
            node: shallow_node(&c.node),
            child: ChildSpec {
                relationship_name: c.child.relationship_name.clone(),
                change: Box::new(clone_change(&c.child.change)),
            },
        }),
    }
}

fn shallow_node(n: &Node) -> Node {
    Node {
        row: n.row.clone(),
        relationships: indexmap::IndexMap::new(),
    }
}
