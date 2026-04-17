//! Filter operator — Transformer shape (no upstream field).
//!
//! Same semantics as `filter::Filter` but with the ownership inverted:
//! upstream is passed into `fetch_through` at call time. Used by
//! `view_syncer_v2::pipeline::Chain` for flat composition.

use std::sync::Arc;

use zero_cache_types::value::Row;

use super::change::{AddChange, Change, Node, RemoveChange};
use super::filter::maybe_split_edit;
use super::operator::{FetchRequest, Transformer};

pub type Predicate = Arc<dyn Fn(&Row) -> bool + Send + Sync>;

pub struct FilterT {
    predicate: Predicate,
}

impl FilterT {
    pub fn new(predicate: Predicate) -> Self {
        Self { predicate }
    }
}

impl Transformer for FilterT {
    fn fetch_through<'a>(
        &'a mut self,
        upstream: Box<dyn Iterator<Item = Node> + 'a>,
        _req: FetchRequest,
    ) -> Box<dyn Iterator<Item = Node> + 'a> {
        let predicate = Arc::clone(&self.predicate);
        Box::new(upstream.filter(move |node| predicate(&node.row)))
    }

    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!(
                "[ivm:rs:filter_t:push] op={}",
                match &change {
                    Change::Add(_) => "Add",
                    Change::Remove(_) => "Remove",
                    Change::Child(_) => "Child",
                    Change::Edit(_) => "Edit",
                }
            );
        }
        let out: Option<Change> = match change {
            Change::Add(AddChange { ref node }) => {
                if (self.predicate)(&node.row) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Remove(RemoveChange { ref node }) => {
                if (self.predicate)(&node.row) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Child(ref c) => {
                if (self.predicate)(&c.node.row) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Edit(edit) => maybe_split_edit(edit, &*self.predicate),
        };
        Box::new(out.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::{AddChange, ChangeType, EditChange, RemoveChange};
    use crate::ivm::data::Node;
    use indexmap::IndexMap;
    use serde_json::json;

    fn row(id: i64, flag: bool) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("flag".into(), Some(json!(flag)));
        r
    }
    fn node_of(r: Row) -> Node {
        Node {
            row: r,
            relationships: IndexMap::new(),
        }
    }
    fn pred() -> Predicate {
        Arc::new(|r: &Row| matches!(r.get("flag"), Some(Some(v)) if v == &json!(true)))
    }
    fn kind(c: &Change) -> ChangeType {
        match c {
            Change::Add(_) => ChangeType::Add,
            Change::Remove(_) => ChangeType::Remove,
            Change::Child(_) => ChangeType::Child,
            Change::Edit(_) => ChangeType::Edit,
        }
    }

    #[test]
    fn fetch_through_filters_non_matching() {
        let mut f = FilterT::new(pred());
        let upstream: Vec<Node> = (0..4).map(|i| node_of(row(i, i % 2 == 0))).collect();
        let out: Vec<Node> = f
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn push_add_true_forwards() {
        let mut f = FilterT::new(pred());
        let out: Vec<Change> = f
            .push(Change::Add(AddChange {
                node: node_of(row(1, true)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
    }
    #[test]
    fn push_add_false_drops() {
        let mut f = FilterT::new(pred());
        let out: Vec<Change> = f
            .push(Change::Add(AddChange {
                node: node_of(row(1, false)),
            }))
            .collect();
        assert!(out.is_empty());
    }
    #[test]
    fn push_edit_split() {
        let mut f = FilterT::new(pred());
        let out: Vec<Change> = f
            .push(Change::Edit(EditChange {
                old_node: node_of(row(1, true)),
                node: node_of(row(1, false)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Remove);
    }
    #[test]
    fn push_remove_false_drops() {
        let mut f = FilterT::new(pred());
        let out: Vec<Change> = f
            .push(Change::Remove(RemoveChange {
                node: node_of(row(1, false)),
            }))
            .collect();
        assert!(out.is_empty());
    }
}
