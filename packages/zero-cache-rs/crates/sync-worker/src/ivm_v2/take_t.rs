//! Take — Transformer shape with pending-refetch signalling.
//!
//! Unpartitioned Take for the first integration. Maintains (size, bound);
//! on at-limit eviction signals a refetch to the Chain to find the new
//! bound.

use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

use indexmap::IndexMap;
use zero_cache_types::value::Row;

use super::change::{AddChange, Change, ChildChange, EditChange, Node, RemoveChange};
use super::operator::{FetchRequest, Transformer};
use crate::ivm::operator::{Start, StartBasis};

#[derive(Debug, Clone, Default)]
pub struct TakeState {
    pub size: usize,
    pub bound: Option<Row>,
}

pub struct TakeT {
    limit: usize,
    state: TakeState,
    compare_rows: Arc<crate::ivm::data::Comparator>,
    /// Pending refetch set during push, drained by Chain on
    /// `take_pending_refetch`.
    pending_refetch: Option<FetchRequest>,
}

impl TakeT {
    pub fn new(
        limit: usize,
        compare_rows: Arc<crate::ivm::data::Comparator>,
    ) -> Self {
        Self {
            limit,
            state: TakeState::default(),
            compare_rows,
            pending_refetch: None,
        }
    }
}

impl Transformer for TakeT {
    /// Hydrate: accept up to `limit` rows. Record the last as bound.
    fn fetch_through<'a>(
        &'a mut self,
        upstream: Box<dyn Iterator<Item = Node> + 'a>,
        _req: FetchRequest,
    ) -> Box<dyn Iterator<Item = Node> + 'a> {
        // If state is already populated, the chain is being re-fetched —
        // only yield rows <= bound.
        if self.state.size > 0 {
            let Some(bound) = self.state.bound.clone() else {
                return Box::new(std::iter::empty());
            };
            let comparator = Arc::clone(&self.compare_rows);
            return Box::new(upstream.take_while(move |n| {
                comparator(&bound, &n.row) != CmpOrdering::Less
            }));
        }
        // Initial fetch: accept up to `limit`, record bound.
        let limit = self.limit;
        let state = &mut self.state;
        let collected: Vec<Node> = upstream.take(limit).collect();
        state.size = collected.len();
        state.bound = collected.last().map(|n| n.row.clone());
        Box::new(collected.into_iter())
    }

    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        eprintln!(
            "[TRACE ivm_v2] TakeT::push enter op={} size={} has_bound={}",
            match &change {
                Change::Add(_) => "Add",
                Change::Remove(_) => "Remove",
                Change::Child(_) => "Child",
                Change::Edit(_) => "Edit",
            },
            self.state.size,
            self.state.bound.is_some(),
        );
        let out = self.push_internal(change);
        Box::new(out.into_iter())
    }

    fn take_pending_refetch(&mut self) -> Option<FetchRequest> {
        self.pending_refetch.take()
    }
}

impl TakeT {
    /// Inner push with full handling for Add / Remove / Edit / Child.
    /// Returns an owned `Vec<Change>` so the wrapper trait method (which
    /// returns a `Box<dyn Iterator + '_>`) can call this and box up the
    /// result without lifetime entanglement. Edit splits to Remove(old)
    /// + Add(new) and recurses through this same method, matching TS
    /// `take.ts`'s edit-as-split behavior.
    fn push_internal(&mut self, change: Change) -> Vec<Change> {
        let mut out: Vec<Change> = Vec::new();
        match change {
            Change::Add(AddChange { node }) => {
                if self.state.size < self.limit {
                    self.state.size += 1;
                    let row = node.row.clone();
                    if self
                        .state
                        .bound
                        .as_ref()
                        .map_or(true, |b| (self.compare_rows)(b, &row) == CmpOrdering::Less)
                    {
                        self.state.bound = Some(row);
                    }
                    out.push(Change::Add(AddChange { node }));
                } else {
                    // At limit: new row replaces bound iff it ranks
                    // *better* (lower in asc ordering).
                    let replaces = self
                        .state
                        .bound
                        .as_ref()
                        .map_or(false, |b| {
                            (self.compare_rows)(b, &node.row) == CmpOrdering::Greater
                        });
                    if replaces {
                        let old_bound = self.state.bound.clone().unwrap();
                        out.push(Change::Remove(RemoveChange {
                            node: Node {
                                row: old_bound.clone(),
                                relationships: IndexMap::new(),
                            },
                        }));
                        let new_row = node.row.clone();
                        self.state.bound = Some(new_row.clone());
                        out.push(Change::Add(AddChange { node }));
                        // Signal: give me the next row AFTER new_row so I
                        // can update the bound. Chain will drive source +
                        // preceding transformers with this request.
                        self.pending_refetch = Some(FetchRequest {
                            start: Some(Start {
                                row: new_row,
                                basis: StartBasis::After,
                            }),
                            ..FetchRequest::default()
                        });
                    }
                    // else: drop silently.
                }
            }
            Change::Remove(RemoveChange { node }) => {
                let was_bound = self
                    .state
                    .bound
                    .as_ref()
                    .map_or(false, |b| (self.compare_rows)(b, &node.row) == CmpOrdering::Equal);
                self.state.size = self.state.size.saturating_sub(1);
                if was_bound {
                    self.state.bound = None;
                    // Will be re-established by next fetch/refetch.
                }
                out.push(Change::Remove(RemoveChange { node }));
            }
            Change::Edit(EditChange { node, old_node }) => {
                // Edit = "row content changed." Split into the equivalent
                // Remove(old) + Add(new) and recurse — TS `take.ts`'s
                // edit-handling behavior. Doing it in two steps yields the
                // same downstream observable result regardless of whether
                // old/new are inside or outside the window:
                //   - both inside: emits Remove(old) then Add(new), client
                //     applies as edit on next reconciliation.
                //   - old inside, new outside (drop-out): Remove emitted,
                //     no Add (Add hits at-limit branch and is dropped if
                //     it doesn't beat bound).
                //   - old outside, new inside (drop-in): Remove a
                //     no-row-in-window, Add emits.
                //   - both outside: both no-ops.
                out.extend(self.push_internal(Change::Remove(RemoveChange {
                    node: old_node,
                })));
                out.extend(self.push_internal(Change::Add(AddChange { node })));
            }
            Change::Child(child) => {
                // ChildChange = "a relationship of `child.node` changed."
                // Forward only if the parent row is currently in our
                // window — i.e., the row's rank is at-or-before our bound,
                // OR we're below limit (so window covers everything we
                // know about). Otherwise the parent isn't synced and the
                // child change is irrelevant downstream.
                let in_window = self.state.size < self.limit
                    || self.state.bound.as_ref().map_or(true, |b| {
                        (self.compare_rows)(b, &child.node.row) != CmpOrdering::Less
                    });
                if in_window {
                    out.push(Change::Child(child));
                }
            }
        }
        out
    }

    fn ingest_refetch(&mut self, rows: Vec<Node>) {
        // The first returned row becomes the new bound.
        self.state.bound = rows.into_iter().next().map(|n| n.row);
        eprintln!(
            "[TRACE ivm_v2] TakeT::ingest_refetch new_bound_is_some={}",
            self.state.bound.is_some()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::{AddChange, RemoveChange};
    use crate::ivm::data::{make_comparator, Node};
    use indexmap::IndexMap;
    use serde_json::json;
    use zero_cache_types::ast::{Direction, Ordering};

    fn comparator() -> Arc<crate::ivm::data::Comparator> {
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
        Arc::new(Box::new(make_comparator(sort, false)))
    }
    fn row(id: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r
    }
    fn node_of(r: Row) -> Node {
        Node {
            row: r,
            relationships: IndexMap::new(),
        }
    }

    #[test]
    fn fetch_through_hydrates_up_to_limit() {
        let mut t = TakeT::new(3, comparator());
        let upstream = vec![node_of(row(1)), node_of(row(2)), node_of(row(3)), node_of(row(4))];
        let out: Vec<Node> = t
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        assert_eq!(out.len(), 3);
        assert_eq!(t.state.size, 3);
        assert_eq!(t.state.bound.as_ref().unwrap().get("id"), Some(&Some(json!(3))));
    }

    #[test]
    fn push_under_limit_accepts() {
        let mut t = TakeT::new(3, comparator());
        for id in 1..=3 {
            let out: Vec<Change> = t
                .push(Change::Add(AddChange {
                    node: node_of(row(id)),
                }))
                .collect();
            assert_eq!(out.len(), 1);
        }
        assert_eq!(t.state.size, 3);
        assert!(t.take_pending_refetch().is_none());
    }

    #[test]
    fn push_at_limit_evicts_and_signals_refetch() {
        let mut t = TakeT::new(2, comparator());
        // Hydrate with rows 10, 20 → bound=20.
        let upstream = vec![node_of(row(10)), node_of(row(20))];
        let _: Vec<Node> = t
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();

        // Push row 5 — beats bound, should evict 20, add 5.
        let out: Vec<Change> = t
            .push(Change::Add(AddChange {
                node: node_of(row(5)),
            }))
            .collect();
        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], Change::Remove(_)));
        assert!(matches!(out[1], Change::Add(_)));

        // Pending refetch: start = row(5), basis = After.
        let pending = t.take_pending_refetch().expect("refetch expected");
        let start = pending.start.expect("start populated");
        assert_eq!(start.row.get("id"), Some(&Some(json!(5))));
        assert_eq!(start.basis, StartBasis::After);

        // Chain simulates: provides row(10) as the next-after-5 row.
        t.ingest_refetch(vec![node_of(row(10))]);
        assert_eq!(t.state.bound.as_ref().unwrap().get("id"), Some(&Some(json!(10))));
    }

    #[test]
    fn push_remove_of_bound_clears_bound_no_refetch_signalled() {
        let mut t = TakeT::new(2, comparator());
        let upstream = vec![node_of(row(10)), node_of(row(20))];
        let _: Vec<Node> = t
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();

        let out: Vec<Change> = t
            .push(Change::Remove(RemoveChange {
                node: node_of(row(20)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Change::Remove(_)));
        assert_eq!(t.state.size, 1);
        // Bound cleared pending the next fetch.
        assert!(t.state.bound.is_none());
        // Remove-of-bound deferred-refetch not signalled in this first
        // cut; next hydrate re-populates.
        assert!(t.take_pending_refetch().is_none());
    }
}
