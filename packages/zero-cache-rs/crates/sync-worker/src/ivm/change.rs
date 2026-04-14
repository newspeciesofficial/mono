//! Port of `packages/zql/src/ivm/change.ts`.
//!
//! IVM change values flowing through the operator graph. One enum variant
//! per TS `type` discriminant:
//!
//! - `Add`    — a node and all children appear in the result
//! - `Remove` — a node and all children leave the result
//! - `Child`  — the node's row is unchanged but one of its descendant
//!              relationships changed
//! - `Edit`   — the row itself changed; sources may split this into a
//!              `Remove` + `Add` when presence or relationships change
//!
//! These values are **not** serialised to the wire; they're the internal
//! currency of the operator graph. `type Change = ...` in TS becomes a
//! plain Rust enum.
//!
//! TS:
//! ```ts
//! export type Change = AddChange | RemoveChange | ChildChange | EditChange;
//! ```

use super::data::Node;

/// IVM change value. Mirrors `change.ts:Change`.
#[derive(Debug, Clone)]
pub enum Change {
    /// TS `AddChange`.
    Add(AddChange),
    /// TS `RemoveChange`.
    Remove(RemoveChange),
    /// TS `ChildChange`.
    Child(ChildChange),
    /// TS `EditChange`.
    Edit(EditChange),
}

/// TS `AddChange.type = 'add'`.
#[derive(Debug, Clone)]
pub struct AddChange {
    pub node: Node,
}

/// TS `RemoveChange.type = 'remove'`.
#[derive(Debug, Clone)]
pub struct RemoveChange {
    pub node: Node,
}

/// TS `ChildChange.type = 'child'` — the node's row is unchanged, but one
/// of its relationship subtrees has a nested change.
#[derive(Debug, Clone)]
pub struct ChildChange {
    pub node: Node,
    pub child: ChildSpec,
}

/// The `child` field of a [`ChildChange`]: names the relationship that
/// changed and carries the nested change that applies to it.
#[derive(Debug, Clone)]
pub struct ChildSpec {
    pub relationship_name: String,
    pub change: Box<Change>,
}

/// TS `EditChange.type = 'edit'` — row changed. If the row's presence or
/// relationships change, operators split this into a [`Change::Remove`]
/// + [`Change::Add`] downstream.
#[derive(Debug, Clone)]
pub struct EditChange {
    pub node: Node,
    pub old_node: Node,
}

/// TS `Change['type']`. Mirrors the string discriminant used by downstream
/// operators that branch on `change.type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeType {
    Add,
    Remove,
    Child,
    Edit,
}

impl Change {
    /// TS `change.type` — the string discriminant.
    #[inline]
    pub fn change_type(&self) -> ChangeType {
        match self {
            Change::Add(_) => ChangeType::Add,
            Change::Remove(_) => ChangeType::Remove,
            Change::Child(_) => ChangeType::Child,
            Change::Edit(_) => ChangeType::Edit,
        }
    }

    /// The node the change carries (the new node for Edit — TS reads
    /// `change.node` unconditionally; Edit's `oldNode` is only read when
    /// callers specifically need the pre-edit row).
    #[inline]
    pub fn node(&self) -> &Node {
        match self {
            Change::Add(c) => &c.node,
            Change::Remove(c) => &c.node,
            Change::Child(c) => &c.node,
            Change::Edit(c) => &c.node,
        }
    }
}

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!   - `change_type()` — one arm per enum variant
    //!   - `node()` — one arm per enum variant (including Edit, which
    //!     returns the new node, not old_node)
    //!   - ChildChange round-trip through `ChildSpec.change` (nested variant)

    use super::super::data::Node;
    use super::*;
    use indexmap::IndexMap;

    fn empty_node() -> Node {
        Node {
            row: IndexMap::new(),
            relationships: Default::default(),
        }
    }

    fn row_with(key: &str, val: serde_json::Value) -> Node {
        let mut r = IndexMap::new();
        r.insert(key.to_string(), Some(val));
        Node {
            row: r,
            relationships: Default::default(),
        }
    }

    // ─── change_type(): all four variants ──────────────────────────────

    #[test]
    fn change_type_add() {
        assert_eq!(
            Change::Add(AddChange { node: empty_node() }).change_type(),
            ChangeType::Add
        );
    }

    #[test]
    fn change_type_remove() {
        assert_eq!(
            Change::Remove(RemoveChange { node: empty_node() }).change_type(),
            ChangeType::Remove
        );
    }

    #[test]
    fn change_type_child() {
        let c = Change::Child(ChildChange {
            node: empty_node(),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange { node: empty_node() })),
            },
        });
        assert_eq!(c.change_type(), ChangeType::Child);
    }

    #[test]
    fn change_type_edit() {
        assert_eq!(
            Change::Edit(EditChange {
                node: empty_node(),
                old_node: empty_node(),
            })
            .change_type(),
            ChangeType::Edit
        );
    }

    // ─── node(): all four arms, each confirming the current node ───────

    #[test]
    fn node_accessor_add() {
        let c = Change::Add(AddChange {
            node: row_with("v", serde_json::json!("add")),
        });
        assert_eq!(c.node().row.get("v"), Some(&Some(serde_json::json!("add"))));
    }

    #[test]
    fn node_accessor_remove() {
        let c = Change::Remove(RemoveChange {
            node: row_with("v", serde_json::json!("remove")),
        });
        assert_eq!(
            c.node().row.get("v"),
            Some(&Some(serde_json::json!("remove")))
        );
    }

    #[test]
    fn node_accessor_child_returns_parent_not_inner_child() {
        // Child carries a parent `node` plus an inner `change`. `node()` on
        // the outer Change must return the parent node, not the child's.
        let c = Change::Child(ChildChange {
            node: row_with("v", serde_json::json!("parent")),
            child: ChildSpec {
                relationship_name: "rel".into(),
                change: Box::new(Change::Add(AddChange {
                    node: row_with("v", serde_json::json!("child-inner")),
                })),
            },
        });
        assert_eq!(
            c.node().row.get("v"),
            Some(&Some(serde_json::json!("parent")))
        );
    }

    #[test]
    fn node_accessor_edit_returns_new_node_not_old() {
        let c = Change::Edit(EditChange {
            node: row_with("v", serde_json::json!("new")),
            old_node: row_with("v", serde_json::json!("old")),
        });
        assert_eq!(c.node().row.get("v"), Some(&Some(serde_json::json!("new"))));
    }
}
