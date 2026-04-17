//! FlippedJoin operator — ivm_v2, push-path skeleton.
//!
//! Mirror of `Join` with parent/child swapped. Used when the child's
//! cardinality is much smaller than the parent's.
//!
//! Same scope/trade-offs as `Join` (see join.rs).

use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

use zero_cache_types::ast::{CompoundKey, System};
use zero_cache_types::value::Row;

use super::change::{Change, ChildChange, ChildSpec, Node, SourceSchema};
use super::operator::{FetchRequest, Input, InputBase};
use crate::ivm::constraint::Constraint;
use crate::ivm::join_utils::{build_join_constraint, row_equals_for_compound_key};

pub struct FlippedJoin {
    parent: Box<dyn Input>,
    child: Box<dyn Input>,
    parent_key: CompoundKey,
    child_key: CompoundKey,
    relationship_name: String,
    schema: SourceSchema,
    #[allow(dead_code)]
    compare_rows: Arc<dyn Fn(&Row, &Row) -> CmpOrdering + Send + Sync>,
}

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
    pub fn new(args: FlippedJoinArgs) -> Self {
        assert_eq!(
            args.parent_key.len(),
            args.child_key.len(),
            "parentKey and childKey must have same length"
        );
        let parent_schema = args.parent.get_schema().clone();
        let child_schema = args.child.get_schema().clone();
        let mut relationships = child_schema.relationships.clone();
        let mut rel_schema = parent_schema.clone();
        rel_schema.is_hidden = args.hidden;
        rel_schema.system = args.system;
        relationships.insert(args.relationship_name.clone(), rel_schema);
        let compare_rows = Arc::clone(&child_schema.compare_rows);
        let schema = SourceSchema {
            table_name: child_schema.table_name,
            columns: child_schema.columns,
            primary_key: child_schema.primary_key,
            relationships,
            is_hidden: child_schema.is_hidden,
            system: child_schema.system,
            compare_rows: Arc::clone(&child_schema.compare_rows),
            sort: child_schema.sort,
        };
        Self {
            parent: args.parent,
            child: args.child,
            parent_key: args.parent_key,
            child_key: args.child_key,
            relationship_name: args.relationship_name,
            schema,
            compare_rows,
        }
    }

    /// TS `#pushChild` — child-side change flows straight through,
    /// decorated with a lazy parent relationship.
    pub fn push_child<'a>(
        &'a mut self,
        change: Change,
    ) -> Box<dyn Iterator<Item = Change> + 'a> {
        // mirrors TS flipped-join.ts:314
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!(
                "[ivm:rs:flipped-join.ts:312:push-child type={}]",
                change_name(&change)
            );
        }
        match &change {
            Change::Add(_) | Change::Remove(_) => {
                // mirrors TS flipped-join.ts:318
                if std::env::var("IVM_PARITY_TRACE").is_ok() {
                    eprintln!(
                        "[ivm:rs:flipped-join.ts:315:push-child-add-or-remove type={}]",
                        change_name(&change)
                    );
                }
            }
            Change::Edit(_) => {
                // mirrors TS flipped-join.ts:322
                if std::env::var("IVM_PARITY_TRACE").is_ok() {
                    eprintln!("[ivm:rs:flipped-join.ts:318:push-child-edit]");
                }
            }
            Change::Child(_) => {
                // mirrors TS flipped-join.ts:335
                if std::env::var("IVM_PARITY_TRACE").is_ok() {
                    eprintln!("[ivm:rs:flipped-join.ts:330:push-child-child]");
                }
            }
        }
        if let Change::Edit(ref edit) = change {
            assert!(
                row_equals_for_compound_key(&edit.old_node.row, &edit.node.row, &self.child_key),
                "Child edit must not change relationship."
            );
        }
        Box::new(std::iter::once(change))
    }

    /// TS `#pushParent` — for each child matching the parent's join key,
    /// emit a ChildChange.
    pub fn push_parent<'a>(
        &'a mut self,
        change: Change,
    ) -> Box<dyn Iterator<Item = Change> + 'a> {
        // mirrors TS flipped-join.ts:431
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!(
                "[ivm:rs:flipped-join.ts:422:push-parent type={}]",
                change_name(&change)
            );
        }
        if let Change::Edit(ref edit) = change {
            // mirrors TS flipped-join.ts:480
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                eprintln!("[ivm:rs:flipped-join.ts:468:push-parent-edit]");
            }
            assert!(
                row_equals_for_compound_key(&edit.old_node.row, &edit.node.row, &self.parent_key),
                "Parent edit must not change relationship."
            );
        }
        let parent_row = match &change {
            Change::Add(c) => c.node.row.clone(),
            Change::Remove(c) => c.node.row.clone(),
            Change::Child(c) => c.node.row.clone(),
            Change::Edit(c) => c.node.row.clone(),
        };
        let constraint_opt: Option<Constraint> =
            build_join_constraint(&parent_row, &self.parent_key, &self.child_key);
        let Some(constraint) = constraint_opt else {
            // mirrors TS flipped-join.ts:461
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                eprintln!(
                    "[ivm:rs:flipped-join.ts:451:push-parent-no-related-child-skip type={}]",
                    change_name(&change)
                );
            }
            return Box::new(std::iter::empty());
        };
        let req = FetchRequest {
            constraint: Some(constraint),
            ..FetchRequest::default()
        };
        let child_nodes: Vec<Node> = self.child.fetch(req).collect();
        if child_nodes.is_empty() {
            // mirrors TS flipped-join.ts:461
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                eprintln!(
                    "[ivm:rs:flipped-join.ts:451:push-parent-no-related-child-skip type={}]",
                    change_name(&change)
                );
            }
            return Box::new(std::iter::empty());
        }
        // mirrors TS flipped-join.ts:469
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!(
                "[ivm:rs:flipped-join.ts:455:push-parent-has-related-child type={}]",
                change_name(&change)
            );
        }
        let rel_name = self.relationship_name.clone();
        let change_shared = change;
        let out: Vec<Change> = child_nodes
            .into_iter()
            .map(|cn| {
                Change::Child(ChildChange {
                    node: cn,
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

impl InputBase for FlippedJoin {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.parent.destroy();
        self.child.destroy();
    }
}

impl Input for FlippedJoin {
    /// Mirrors TS `flipped-join.ts::*fetch`. Emits **parent** nodes (in
    /// the parent's sort order) with the join-relationship populated by
    /// matching child nodes. Only parents that have at least one
    /// matching child are emitted — this is the "flipped" EXISTS
    /// semantics: drive the scan from children and project back to
    /// parents.
    ///
    /// Non-streaming implementation: the full join is materialised up
    /// front (children collected, matching parents fetched per-child,
    /// deduplicated, resorted). This mirrors what the TS generator
    /// produces in total but without the lazy k-way merge — trade-off
    /// is memory for code-simplicity. Acceptable because v2's
    /// hydration path already materialises chunks before handing them
    /// across the napi boundary.
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        // mirrors TS flipped-join.ts:117
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!(
                "[ivm:rs:flipped-join.ts:116:fetch constraint={:?}]",
                req.constraint
            );
        }
        // Translate constraints: callers pass a constraint keyed by the
        // parent's field name (e.g., `parent_key=[...]`). Map those to
        // the equivalent child_key entries so the child fetch narrows
        // down.
        let mut child_constraint: Constraint = Constraint::new();
        let mut parent_passthrough: Constraint = Constraint::new();
        if let Some(constraint) = req.constraint.as_ref() {
            for (key, value) in constraint.iter() {
                if let Some(idx) = self.parent_key.iter().position(|k| k == key) {
                    child_constraint.insert(self.child_key[idx].clone(), value.clone());
                } else {
                    parent_passthrough.insert(key.clone(), value.clone());
                }
            }
        }
        let child_req = FetchRequest {
            constraint: if child_constraint.is_empty() {
                None
            } else {
                Some(child_constraint)
            },
            ..FetchRequest::default()
        };
        let child_nodes: Vec<Node> = self.child.fetch(child_req).collect();

        // Fan out: per child, fetch matching parents. De-dup by parent
        // row (via `row_equals_for_compound_key` over the primary key),
        // merging matched children into `relationships[name]`.
        use crate::ivm::data::{NodeOrYield, RelationshipFactory};
        use indexmap::IndexMap;

        let parent_pk = self.parent.get_schema().primary_key.columns().to_vec();
        let mut emitted: Vec<(Node, Vec<Node>)> = Vec::new();
        for child in &child_nodes {
            let Some(join_constraint) =
                build_join_constraint(&child.row, &self.child_key, &self.parent_key)
            else {
                continue;
            };
            // Combine any non-overlapping caller-supplied constraint
            // with the join constraint — matches TS's
            // `constraintsAreCompatible` + spread behavior for the
            // straightforward non-conflict case. Conflicts drop the
            // child (same as TS `empty`).
            let mut combined = join_constraint.clone();
            let mut conflict = false;
            for (k, v) in parent_passthrough.iter() {
                if let Some(existing) = combined.get(k) {
                    if existing != v {
                        conflict = true;
                        break;
                    }
                } else {
                    combined.insert(k.clone(), v.clone());
                }
            }
            if conflict {
                continue;
            }
            let parent_req = FetchRequest {
                constraint: Some(combined),
                reverse: req.reverse,
                ..FetchRequest::default()
            };
            for parent in self.parent.fetch(parent_req) {
                // Dedup: collapse repeated parent rows into one entry
                // and append the child. Primary-key equality mirrors
                // TS's row-identity check in its k-way merge.
                let idx = emitted.iter().position(|(p, _)| {
                    crate::ivm::join_utils::row_equals_for_compound_key(
                        &p.row,
                        &parent.row,
                        &parent_pk,
                    )
                });
                match idx {
                    Some(i) => emitted[i].1.push(child.clone()),
                    None => emitted.push((parent, vec![child.clone()])),
                }
            }
        }

        // Re-sort by the parent schema's compare_rows (with req.reverse
        // honoured) so downstream Take/Skip see parents in proper order.
        let compare_rows = std::sync::Arc::clone(&self.parent.get_schema().compare_rows);
        let reverse = req.reverse.unwrap_or(false);
        emitted.sort_by(|a, b| {
            let ord = compare_rows(&a.0.row, &b.0.row);
            if reverse {
                ord.reverse()
            } else {
                ord
            }
        });

        let rel_name = self.relationship_name.clone();
        let out_nodes: Vec<Node> = emitted
            .into_iter()
            .map(|(parent, children)| {
                let mut relationships: IndexMap<String, RelationshipFactory> =
                    parent.relationships;
                let rel_children = children;
                let factory: RelationshipFactory = Box::new(move || {
                    Box::new(rel_children.clone().into_iter().map(NodeOrYield::Node))
                        as Box<dyn Iterator<Item = NodeOrYield>>
                });
                relationships.insert(rel_name.clone(), factory);
                Node {
                    row: parent.row,
                    relationships,
                }
            })
            .collect();
        Box::new(out_nodes.into_iter())
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

#[cfg(test)]
mod tests {
    //! Focused hydration-path coverage for `FlippedJoin::fetch` — the
    //! output shape (parent rows with populated child relationships)
    //! must match TS `flipped-join.ts::*fetch` observable semantics.
    //! Push-path (`push_parent` / `push_child`) coverage lives with the
    //! Chain integration when the Chain wires the full FlippedJoin
    //! operator (currently only ExistsT is wired).
    use super::*;
    use crate::ivm::data::{make_comparator, Node, NodeOrYield};
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::Arc;
    use zero_cache_types::ast::{Direction, Ordering as AstOrdering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Row;

    use crate::ivm_v2::operator::InputBase;

    /// In-memory `Input` for tests — emits a fixed set of rows. Honors
    /// `req.constraint` via simple equality per key. No ordering logic;
    /// each instance carries a pre-sorted vec.
    struct MemSource {
        schema: SourceSchema,
        rows: Vec<Row>,
    }

    impl InputBase for MemSource {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {}
    }

    impl Input for MemSource {
        fn fetch<'a>(
            &'a mut self,
            req: FetchRequest,
        ) -> Box<dyn Iterator<Item = Node> + 'a> {
            let rows = self.rows.clone();
            let filtered: Vec<Node> = rows
                .into_iter()
                .filter(|r| match req.constraint.as_ref() {
                    None => true,
                    Some(c) => c.iter().all(|(k, v)| r.get(k) == Some(v)),
                })
                .map(|r| Node {
                    row: r,
                    relationships: IndexMap::new(),
                })
                .collect();
            Box::new(filtered.into_iter())
        }
    }

    fn mk_schema(table: &str, sort_col: &str) -> SourceSchema {
        let sort: AstOrdering = vec![(sort_col.into(), Direction::Asc)];
        SourceSchema {
            table_name: table.into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    fn row(pairs: &[(&str, serde_json::Value)]) -> Row {
        let mut r = Row::new();
        for (k, v) in pairs {
            r.insert((*k).into(), Some(v.clone()));
        }
        r
    }

    #[test]
    fn fetch_emits_parents_with_matching_children() {
        // Parents: [{id:1}, {id:2}, {id:3}]
        // Children: [{id:'c1', parentId:1}, {id:'c2', parentId:1}, {id:'c3', parentId:3}]
        // Expected output: parents 1 and 3 (parent 2 has no matching child),
        // with parent.relationships['kids'] populated:
        //   parent 1 → [c1, c2]
        //   parent 3 → [c3]
        let parent = Box::new(MemSource {
            schema: mk_schema("parents", "id"),
            rows: vec![
                row(&[("id", json!(1))]),
                row(&[("id", json!(2))]),
                row(&[("id", json!(3))]),
            ],
        });
        let child = Box::new(MemSource {
            schema: mk_schema("children", "id"),
            rows: vec![
                row(&[("id", json!("c1")), ("parentId", json!(1))]),
                row(&[("id", json!("c2")), ("parentId", json!(1))]),
                row(&[("id", json!("c3")), ("parentId", json!(3))]),
            ],
        });

        let mut fj = FlippedJoin::new(FlippedJoinArgs {
            parent,
            child,
            parent_key: vec!["id".into()],
            child_key: vec!["parentId".into()],
            relationship_name: "kids".into(),
            hidden: false,
            system: System::Test,
        });

        let out: Vec<Node> = fj.fetch(FetchRequest::default()).collect();
        assert_eq!(out.len(), 2, "only matched parents should emit");

        // Parent 1 with 2 kids.
        assert_eq!(out[0].row.get("id"), Some(&Some(json!(1))));
        let kids1: Vec<_> = out[0].relationships["kids"]()
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => Some(n),
                _ => None,
            })
            .collect();
        assert_eq!(kids1.len(), 2);

        // Parent 3 with 1 kid.
        assert_eq!(out[1].row.get("id"), Some(&Some(json!(3))));
        let kids3: Vec<_> = out[1].relationships["kids"]()
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => Some(n),
                _ => None,
            })
            .collect();
        assert_eq!(kids3.len(), 1);
    }

    #[test]
    fn fetch_honors_caller_constraint_on_parent_key() {
        // Caller narrows to parent id=1; only that parent should emit.
        let parent = Box::new(MemSource {
            schema: mk_schema("parents", "id"),
            rows: vec![row(&[("id", json!(1))]), row(&[("id", json!(2))])],
        });
        let child = Box::new(MemSource {
            schema: mk_schema("children", "id"),
            rows: vec![
                row(&[("id", json!("c1")), ("parentId", json!(1))]),
                row(&[("id", json!("c2")), ("parentId", json!(2))]),
            ],
        });
        let mut fj = FlippedJoin::new(FlippedJoinArgs {
            parent,
            child,
            parent_key: vec!["id".into()],
            child_key: vec!["parentId".into()],
            relationship_name: "kids".into(),
            hidden: false,
            system: System::Test,
        });
        let mut c = Constraint::new();
        c.insert("id".into(), Some(json!(1)));
        let out: Vec<Node> = fj
            .fetch(FetchRequest {
                constraint: Some(c),
                ..FetchRequest::default()
            })
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].row.get("id"), Some(&Some(json!(1))));
    }

    #[test]
    fn fetch_empty_when_no_children_match() {
        let parent = Box::new(MemSource {
            schema: mk_schema("parents", "id"),
            rows: vec![row(&[("id", json!(1))]), row(&[("id", json!(2))])],
        });
        let child = Box::new(MemSource {
            schema: mk_schema("children", "id"),
            rows: vec![],
        });
        let mut fj = FlippedJoin::new(FlippedJoinArgs {
            parent,
            child,
            parent_key: vec!["id".into()],
            child_key: vec!["parentId".into()],
            relationship_name: "kids".into(),
            hidden: false,
            system: System::Test,
        });
        let out: Vec<Node> = fj.fetch(FetchRequest::default()).collect();
        assert!(out.is_empty());
    }
}
