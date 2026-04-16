//! Join / FlippedJoin — ivm_v2 twin-input scaffold.
//!
//! Joins have two upstreams (parent + child) and don't fit the
//! single-upstream `Transformer` trait. Chain composition uses a
//! dedicated `BinaryTransformer` shape: both upstream iterators flow
//! in, decorated parent Nodes flow out.
//!
//! First-cut scope:
//!   - `fetch_through` takes parent + child iterators; returns parent
//!     rows with an attached lazy relationship factory.
//!   - `push_parent(change)` / `push_child(change)` — each emits
//!     transformed `Change`s. The refetch protocol (fetching from the
//!     other side during a push) is tracked as a follow-up: this
//!     scaffold emits the change bare without the hierarchical overlay.
//!
//! Not yet: overlay-aware child-change decoration matching TS
//! `#pushChildChange` fully.

use std::sync::Arc;

use zero_cache_types::ast::CompoundKey;
use zero_cache_types::value::Row;

use super::change::{Change, ChildChange, ChildSpec, Node};
use super::operator::{BinaryTransformer, FetchRequest};
use crate::ivm::data::RelationshipFactory;
use crate::ivm::join_utils::{build_join_constraint, row_equals_for_compound_key};

pub struct JoinT {
    parent_key: CompoundKey,
    child_key: CompoundKey,
    relationship_name: String,
}

impl JoinT {
    pub fn new(
        parent_key: CompoundKey,
        child_key: CompoundKey,
        relationship_name: String,
    ) -> Self {
        assert_eq!(
            parent_key.len(),
            child_key.len(),
            "parentKey and childKey must have same length"
        );
        Self {
            parent_key,
            child_key,
            relationship_name,
        }
    }

    /// Decorate parent rows with a lazy factory that, on demand, filters
    /// a shared child-rows snapshot by the join constraint.
    ///
    /// Scope: the child snapshot is provided up-front as a `Vec<Node>`
    /// (caller materialises from upstream). Fully-streaming two-sided
    /// fetch is a larger follow-up.
    pub fn process_parent(
        &self,
        parent_row: Row,
        child_snapshot: Arc<Vec<Node>>,
    ) -> Node {
        let parent_key = self.parent_key.clone();
        let child_key = self.child_key.clone();
        let relationship_name = self.relationship_name.clone();
        let parent_row_clone = parent_row.clone();
        let snapshot = Arc::clone(&child_snapshot);

        let factory: RelationshipFactory = Box::new(move || {
            let constraint =
                build_join_constraint(&parent_row_clone, &parent_key, &child_key);
            let Some(constraint) = constraint else {
                return Box::new(std::iter::empty());
            };
            let matches: Vec<crate::ivm::data::NodeOrYield> = snapshot
                .iter()
                .filter(|n| {
                    constraint
                        .iter()
                        .all(|(k, v)| n.row.get(k) == Some(v))
                })
                .map(|n| crate::ivm::data::NodeOrYield::Node(shallow_clone(n)))
                .collect();
            Box::new(matches.into_iter())
        });

        let mut relationships: indexmap::IndexMap<String, RelationshipFactory> =
            indexmap::IndexMap::new();
        relationships.insert(relationship_name, factory);
        Node {
            row: parent_row,
            relationships,
        }
    }

    /// Parent-side push: emit the change with parent keys unchanged.
    /// Asserts that Edit doesn't touch the join key.
    pub fn push_parent(&self, change: Change) -> Option<Change> {
        if let Change::Edit(ref edit) = change {
            assert!(
                row_equals_for_compound_key(&edit.old_node.row, &edit.node.row, &self.parent_key),
                "Parent edit must not change relationship."
            );
        }
        Some(change)
    }

    /// Child-side push: for each parent row matching the child's key,
    /// emit a ChildChange. Caller must supply the current parent
    /// snapshot (we can't reach into the parent input from here).
    pub fn push_child(
        &self,
        change: Change,
        parent_snapshot: &[Node],
    ) -> Vec<Change> {
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
        let Some(constraint) = build_join_constraint(&child_row, &self.child_key, &self.parent_key)
        else {
            return Vec::new();
        };
        parent_snapshot
            .iter()
            .filter(|n| constraint.iter().all(|(k, v)| n.row.get(k) == Some(v)))
            .map(|parent_node| {
                Change::Child(ChildChange {
                    node: shallow_clone(parent_node),
                    child: ChildSpec {
                        relationship_name: self.relationship_name.clone(),
                        change: Box::new(clone_change(&change)),
                    },
                })
            })
            .collect()
    }
}

impl BinaryTransformer for JoinT {
    fn fetch_through<'a>(
        &'a mut self,
        parent_upstream: Box<dyn Iterator<Item = Node> + 'a>,
        child_snapshot: Arc<Vec<Node>>,
        _req: FetchRequest,
    ) -> Box<dyn Iterator<Item = Node> + 'a> {
        let parent_key = self.parent_key.clone();
        let child_key = self.child_key.clone();
        let relationship_name = self.relationship_name.clone();
        Box::new(parent_upstream.map(move |n| {
            let snapshot = Arc::clone(&child_snapshot);
            let parent_row_clone = n.row.clone();
            let parent_key_clone = parent_key.clone();
            let child_key_clone = child_key.clone();
            let factory: RelationshipFactory = Box::new(move || {
                let Some(constraint) =
                    build_join_constraint(&parent_row_clone, &parent_key_clone, &child_key_clone)
                else {
                    return Box::new(std::iter::empty());
                };
                let matches: Vec<crate::ivm::data::NodeOrYield> = snapshot
                    .iter()
                    .filter(|cn| constraint.iter().all(|(k, v)| cn.row.get(k) == Some(v)))
                    .map(|cn| crate::ivm::data::NodeOrYield::Node(shallow_clone(cn)))
                    .collect();
                Box::new(matches.into_iter())
            });
            let mut relationships: indexmap::IndexMap<String, RelationshipFactory> =
                indexmap::IndexMap::new();
            relationships.insert(relationship_name.clone(), factory);
            Node {
                row: n.row,
                relationships,
            }
        }))
    }

    fn push_parent<'a>(
        &'a mut self,
        change: Change,
    ) -> Box<dyn Iterator<Item = Change> + 'a> {
        Box::new(JoinT::push_parent(self, change).into_iter())
    }

    fn push_child<'a>(
        &'a mut self,
        change: Change,
        parent_snapshot: &[Node],
    ) -> Box<dyn Iterator<Item = Change> + 'a> {
        let out = JoinT::push_child(self, change, parent_snapshot);
        Box::new(out.into_iter())
    }
}

fn shallow_clone(n: &Node) -> Node {
    Node {
        row: n.row.clone(),
        relationships: indexmap::IndexMap::new(),
    }
}

fn clone_change(c: &Change) -> Change {
    use super::change::{AddChange, EditChange, RemoveChange};
    match c {
        Change::Add(a) => Change::Add(AddChange {
            node: shallow_clone(&a.node),
        }),
        Change::Remove(r) => Change::Remove(RemoveChange {
            node: shallow_clone(&r.node),
        }),
        Change::Edit(e) => Change::Edit(EditChange {
            node: shallow_clone(&e.node),
            old_node: shallow_clone(&e.old_node),
        }),
        Change::Child(c) => Change::Child(ChildChange {
            node: shallow_clone(&c.node),
            child: ChildSpec {
                relationship_name: c.child.relationship_name.clone(),
                change: Box::new(clone_change(&c.child.change)),
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::AddChange;
    use crate::ivm::data::Node;
    use indexmap::IndexMap;
    use serde_json::json;

    fn row_with(id: i64, fk: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("fk".into(), Some(json!(fk)));
        r
    }
    fn node_of(r: Row) -> Node {
        Node {
            row: r,
            relationships: IndexMap::new(),
        }
    }

    #[test]
    fn process_parent_attaches_matching_children() {
        let join = JoinT::new(vec!["id".into()], vec!["fk".into()], "children".into());
        let children = Arc::new(vec![
            node_of(row_with(1, 100)),
            node_of(row_with(2, 100)),
            node_of(row_with(3, 200)),
        ]);
        let parent_node = join.process_parent(row_with(100, 0), Arc::clone(&children));
        let factory = parent_node.relationships.get("children").unwrap();
        let matches: Vec<_> = (*factory)().collect();
        // Should yield child nodes with fk=100 (ids 1 and 2).
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn push_child_emits_child_change_per_matching_parent() {
        let join = JoinT::new(vec!["id".into()], vec!["fk".into()], "children".into());
        let parents = vec![node_of(row_with(100, 0)), node_of(row_with(100, 0))];
        // Child with fk=100 arrives; every parent with id=100 should receive a ChildChange.
        let out = join.push_child(
            Change::Add(AddChange {
                node: node_of(row_with(42, 100)),
            }),
            &parents,
        );
        assert_eq!(out.len(), 2);
        for c in out {
            assert!(matches!(c, Change::Child(_)));
        }
    }

    #[test]
    fn push_child_ignores_non_matching_parent() {
        let join = JoinT::new(vec!["id".into()], vec!["fk".into()], "children".into());
        let parents = vec![node_of(row_with(1, 0))];
        let out = join.push_child(
            Change::Add(AddChange {
                node: node_of(row_with(42, 100)),
            }),
            &parents,
        );
        assert!(out.is_empty());
    }
}
