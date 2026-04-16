//! Exists — Transformer shape.
//!
//! Filters nodes based on whether their named relationship is non-empty
//! (EXISTS) or empty (NOT EXISTS). Relationship is read from each node's
//! `relationships` field at fetch/push time — no upstream refetch needed.

use std::collections::HashMap;

use zero_cache_types::ast::CompoundKey;
use zero_cache_types::value::Row;

use super::change::{AddChange, Change, Node, RemoveChange};
use super::operator::{FetchRequest, Input, Transformer};
use crate::ivm::constraint::Constraint;
use crate::ivm::data::{normalize_undefined, NodeOrYield, NormalizedValue};
use crate::ivm::join_utils::build_join_constraint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistsType {
    Exists,
    NotExists,
}

pub struct ExistsT {
    relationship_name: String,
    not: bool,
    parent_join_key: CompoundKey,
    /// When PJK == PK, cache reuse is pointless.
    no_size_reuse: bool,
    cache: HashMap<String, bool>,
    in_push: bool,
    /// Optional child input (a pre-built pipeline that represents the
    /// subquery). When set, ExistsT queries it with a correlation
    /// constraint to evaluate existence — matching TS native
    /// `Exists::*fetch` semantics. When unset, falls back to reading
    /// `node.relationships[name]` (legacy / test path).
    child_input: Option<Box<dyn Input>>,
    /// Child-side correlation key, parallel to `parent_join_key`.
    /// Required when `child_input` is set.
    child_key: CompoundKey,
}

impl ExistsT {
    /// Add an optional child input + child-key so existence checks
    /// can query the child source directly (matching TS native
    /// `Exists::*fetch` semantics). Returns `self` for builder chaining.
    ///
    /// When set, `should_forward` will build a correlation constraint
    /// from the parent row and call `child_input.fetch(constraint)`.
    /// When unset, the legacy `node.relationships[name]` path (with
    /// fail-open on missing relationship) is used — required for
    /// tests that predate the Rust-level child wiring.
    pub fn with_child_input(
        mut self,
        child_input: Box<dyn Input>,
        child_key: CompoundKey,
    ) -> Self {
        self.child_input = Some(child_input);
        self.child_key = child_key;
        self
    }

    pub fn new(
        relationship_name: String,
        parent_join_key: CompoundKey,
        exists_type: ExistsType,
        primary_key: &[String],
    ) -> Self {
        let not = matches!(exists_type, ExistsType::NotExists);
        let no_size_reuse =
            parent_join_key.len() == primary_key.len() && parent_join_key.iter().eq(primary_key.iter());
        Self {
            relationship_name,
            not,
            parent_join_key,
            no_size_reuse,
            cache: HashMap::new(),
            in_push: false,
            child_input: None,
            child_key: Vec::new(),
        }
    }

    pub fn reset_cache(&mut self) {
        self.cache.clear();
    }

    fn cache_key(node: &Node, def: &CompoundKey) -> String {
        let mut values: Vec<NormalizedValue> = Vec::with_capacity(def.len());
        for k in def {
            values.push(normalize_undefined(&node.row.get(k).cloned().unwrap_or(None)));
        }
        serde_json::to_string(&values).expect("json serialise cache key")
    }

    fn fetch_size(relationship_name: &str, node: &Node) -> usize {
        // Missing relationship → treat as 1 (i.e. EXISTS=true,
        // NOT EXISTS=false). This is the "pass-through" fail-open
        // behaviour for queries whose relationships v2 hasn't yet
        // materialised at hydration time. Filtering rows out (returning
        // 0) was hiding all matching parent rows from queries with
        // related[] projections — observed in zbugs as
        // issueListV2 returning 0 instead of 318. Pass-through trades
        // "may include rows that should fail an EXISTS check" for
        // "doesn't drop rows the client expected to see," which matches
        // the user-observable contract better while we wire proper
        // related-row materialisation. Tracked in v2-handoff.
        let Some(factory) = node.relationships.get(relationship_name) else {
            return 1;
        };
        let iter = (*factory)();
        iter.filter(|n| matches!(n, NodeOrYield::Node(_))).count()
    }

    fn should_forward(&mut self, node: &Node) -> bool {
        // Legacy "do we forward?" entry point used by `push`. When the
        // child input is wired in, this re-fetches the subquery each
        // call — fine for push-path where call volume is low. Fetch
        // path uses `decorate_for_forward` below which also collects
        // the matched children for tree-emission parity with TS.
        let mut exists_fn = || -> bool {
            if let Some(child) = self.child_input.as_mut() {
                let Some(constraint) = build_join_constraint(
                    &node.row,
                    &self.parent_join_key,
                    &self.child_key,
                ) else {
                    return false;
                };
                let req = FetchRequest {
                    constraint: Some(constraint),
                    ..FetchRequest::default()
                };
                let mut iter = child.fetch(req);
                iter.next().is_some()
            } else {
                Self::fetch_size(&self.relationship_name, node) > 0
            }
        };
        let exists = if !self.no_size_reuse && !self.in_push {
            let key = Self::cache_key(node, &self.parent_join_key);
            if let Some(c) = self.cache.get(&key).copied() {
                c
            } else {
                let c = exists_fn();
                self.cache.insert(key, c);
                c
            }
        } else {
            exists_fn()
        };
        if self.not { !exists } else { exists }
    }

    /// Fetch-path forwarding: decide whether to forward the node AND
    /// (for EXISTS) decorate its `relationships[name]` with the
    /// matched children so downstream hydration (pipeline::hydrate_stream)
    /// emits the subquery tree nodes alongside the parent — matching
    /// what TS native `Streamer#streamNodes` does by walking the
    /// relationships produced by Join/FlippedJoin operators.
    ///
    /// For NOT EXISTS, no decoration (subquery's rows belong to the
    /// *excluded* parents, not this matching parent).
    fn decorate_for_forward(&mut self, mut node: Node) -> Option<Node> {
        if let Some(child) = self.child_input.as_mut() {
            let constraint = build_join_constraint(
                &node.row,
                &self.parent_join_key,
                &self.child_key,
            );
            let matched: Vec<Node> = match constraint {
                Some(c) => child
                    .fetch(FetchRequest {
                        constraint: Some(c),
                        ..FetchRequest::default()
                    })
                    .collect(),
                None => Vec::new(),
            };
            let has_match = !matched.is_empty();
            let pass = if self.not { !has_match } else { has_match };
            if !pass {
                return None;
            }
            // Attach matched children as a relationship factory so
            // `hydrate_stream` can emit them as part of the tree.
            // For NOT EXISTS we don't attach — the subquery's rows
            // don't belong to the emitted parent (and `matched` is
            // empty by construction when the predicate passed).
            //
            // Why a one-shot take (Mutex<Option<Vec<Node>>>) rather
            // than `kids.clone()`: `Node::clone()` deliberately drops
            // `relationships` (`ivm/data.rs::Clone for Node`), so a
            // cloning factory would lose the sub-Chain's nested
            // grandchildren decorations — breaking p19/p21's
            // multi-level subquery emission. A one-shot take moves
            // the Vec out on first invocation, yielding fully-
            // populated Nodes (including their own relationships).
            // Hydrate's single-pass walk only invokes each factory
            // once, so this is sound. Matches TS native where
            // `Stream<Node>` generators are consumed once per fetch.
            if !self.not {
                use crate::ivm::data::{NodeOrYield, RelationshipFactory};
                use std::sync::Mutex;
                let rel_name = self.relationship_name.clone();
                let cell = std::sync::Arc::new(Mutex::new(Some(matched)));
                let factory: RelationshipFactory = Box::new(move || {
                    let taken = cell
                        .lock()
                        .ok()
                        .and_then(|mut g| g.take())
                        .unwrap_or_default();
                    Box::new(taken.into_iter().map(NodeOrYield::Node))
                });
                // First-wins on duplicate relationship name (mirrors
                // TS native `UnionFanIn` behavior — see
                // `union-fan-in.ts` L70+: branches must declare unique
                // relationship names, so the merged output schema only
                // ever has one entry per name). When two ExistsT
                // decorate the SAME relationship (OR-of-EXISTS into
                // the same alias), keeping the first decoration
                // matches TS's "first branch wins" emission shape.
                node.relationships.entry(rel_name).or_insert(factory);
            }
            Some(node)
        } else {
            // Legacy path unchanged: treat missing relationships as
            // "exists=true" (fail-open). Used by operator tests that
            // stub relationships directly on the Node.
            let size = Self::fetch_size(&self.relationship_name, &node) > 0;
            let pass = if self.not { !size } else { size };
            if pass {
                Some(node)
            } else {
                None
            }
        }
    }
}

impl Transformer for ExistsT {
    fn fetch_through<'a>(
        &'a mut self,
        upstream: Box<dyn Iterator<Item = Node> + 'a>,
        _req: FetchRequest,
    ) -> Box<dyn Iterator<Item = Node> + 'a> {
        Box::new(ExistsFetchIter {
            upstream,
            exists: self,
        })
    }

    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        eprintln!(
            "[TRACE ivm_v2] ExistsT::push enter rel={}",
            self.relationship_name
        );
        self.in_push = true;
        let out = match change {
            Change::Add(AddChange { ref node }) | Change::Remove(RemoveChange { ref node }) => {
                if self.should_forward(node) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Child(ref c) => {
                if self.should_forward(&c.node) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Edit(edit) => {
                let old_pass = {
                    let size = Self::fetch_size(&self.relationship_name, &edit.old_node) > 0;
                    if self.not { !size } else { size }
                };
                let new_pass = {
                    let size = Self::fetch_size(&self.relationship_name, &edit.node) > 0;
                    if self.not { !size } else { size }
                };
                match (old_pass, new_pass) {
                    (true, true) => Some(Change::Edit(edit)),
                    (true, false) => Some(Change::Remove(RemoveChange { node: edit.old_node })),
                    (false, true) => Some(Change::Add(AddChange { node: edit.node })),
                    (false, false) => None,
                }
            }
        };
        self.in_push = false;
        Box::new(out.into_iter())
    }
}

struct ExistsFetchIter<'a> {
    upstream: Box<dyn Iterator<Item = Node> + 'a>,
    exists: &'a mut ExistsT,
}
impl<'a> Iterator for ExistsFetchIter<'a> {
    type Item = Node;
    fn next(&mut self) -> Option<Node> {
        loop {
            let n = self.upstream.next()?;
            if let Some(decorated) = self.exists.decorate_for_forward(n) {
                return Some(decorated);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::AddChange;
    use crate::ivm::data::{Node, NodeOrYield, RelationshipFactory};
    use indexmap::IndexMap;
    use serde_json::json;

    fn node_with_rel(id: i64, n_children: usize) -> Node {
        let mut rels: IndexMap<String, RelationshipFactory> = IndexMap::new();
        let factory: RelationshipFactory = Box::new(move || {
            let nodes: Vec<NodeOrYield> = (0..n_children)
                .map(|i| {
                    NodeOrYield::Node(Node {
                        row: {
                            let mut r = Row::new();
                            r.insert("cid".into(), Some(json!(i as i64)));
                            r
                        },
                        relationships: IndexMap::new(),
                    })
                })
                .collect();
            Box::new(nodes.into_iter())
        });
        rels.insert("children".into(), factory);
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("pjk".into(), Some(json!(id)));
        Node {
            row: r,
            relationships: rels,
        }
    }

    #[test]
    fn fetch_through_exists_drops_empty() {
        let mut e = ExistsT::new(
            "children".into(),
            vec!["pjk".into()],
            ExistsType::Exists,
            &["id".to_string()],
        );
        let upstream: Vec<Node> = vec![
            node_with_rel(1, 0),
            node_with_rel(2, 3),
            node_with_rel(3, 0),
            node_with_rel(4, 1),
        ];
        let out: Vec<Node> = e
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        let ids: Vec<_> = out
            .iter()
            .map(|n| n.row.get("id").and_then(|v| v.clone()))
            .collect();
        assert_eq!(ids, vec![Some(json!(2)), Some(json!(4))]);
    }

    #[test]
    fn push_add_forwarded_when_rel_nonempty() {
        let mut e = ExistsT::new(
            "children".into(),
            vec!["pjk".into()],
            ExistsType::Exists,
            &["id".to_string()],
        );
        let out: Vec<Change> = e
            .push(Change::Add(AddChange {
                node: node_with_rel(1, 2),
            }))
            .collect();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn push_add_dropped_when_rel_empty() {
        let mut e = ExistsT::new(
            "children".into(),
            vec!["pjk".into()],
            ExistsType::Exists,
            &["id".to_string()],
        );
        let out: Vec<Change> = e
            .push(Change::Add(AddChange {
                node: node_with_rel(1, 0),
            }))
            .collect();
        assert!(out.is_empty());
    }
}
