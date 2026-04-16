//! Take operator — single-core redesign.
//!
//! Scope (first pass):
//!
//! - Push path only: Add below limit, Add at limit with eviction, Remove.
//! - No partition keys, no maxBound tracking, no edit splitting.
//! - State is plain fields (`size`, `bound`). No `Mutex`.
//! - `push(&mut self, Change) -> Box<dyn Iterator<Item=Change> + '_>`
//!   backed by a `TakePushIter` holding disjoint borrows.
//!
//! Deferred to next iteration:
//!
//! - Fetch path (initial hydration, `fetch_partition_bound`, max-bound scan).
//! - Partition keys / `TakeState` per partition.
//! - Edit-splitting across bounds.
//! - Child-change handling for at-limit nodes.

use std::cmp::Ordering as CmpOrdering;
use std::collections::VecDeque;
use std::sync::Arc;

use zero_cache_types::value::Row;

use super::change::{AddChange, Change, Node, RemoveChange, SourceSchema};
use super::operator::{FetchRequest, Input, InputBase, Operator};

/// TS `type TakeState = { size: number; bound: Row | undefined }`.
#[derive(Debug, Clone, Default)]
pub struct TakeState {
    pub size: usize,
    pub bound: Option<Row>,
}

pub struct Take {
    input: Box<dyn Input>,
    limit: usize,
    state: TakeState,
    schema: SourceSchema,
    compare_rows: Arc<dyn Fn(&Row, &Row) -> CmpOrdering + Send + Sync>,
}

impl Take {
    pub fn new(input: Box<dyn Input>, limit: usize) -> Self {
        let schema = input.get_schema().clone();
        let compare_rows = Arc::clone(&schema.compare_rows);
        Self {
            input,
            limit,
            state: TakeState::default(),
            schema,
            compare_rows,
        }
    }

    /// TS `*push(change)`.
    pub fn push_iter<'a>(&'a mut self, change: Change) -> TakePushIter<'a> {
        eprintln!(
            "[TRACE ivm_v2] Take::push enter change={} size={} has_bound={}",
            change_name(&change),
            self.state.size,
            self.state.bound.is_some()
        );
        TakePushIter::new(
            change,
            &mut self.input,
            &mut self.state,
            self.limit,
            Arc::clone(&self.compare_rows),
        )
    }
}

impl InputBase for Take {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.input.destroy();
    }
}

impl Input for Take {
    /// Fetch path — unpartitioned only.
    ///
    /// First call (state empty): hydrate up to `limit` rows from input,
    /// track the last row as `bound`, persist in `state`.
    /// Subsequent calls: stream rows from input up to `bound`.
    ///
    /// Partitioned fetch + max-bound scan not yet ported (see #122).
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        if self.state.size == 0 && self.state.bound.is_none() {
            // Initial fetch — drive upstream, accept up to `limit`, track bound.
            let limit = self.limit;
            let compare_rows = Arc::clone(&self.compare_rows);
            // Consume upstream eagerly for the first `limit` rows so we can
            // persist the bound before returning. Short prefix — bounded by
            // `limit` reads from upstream, not the full table.
            let upstream = self.input.fetch(req);
            let mut collected: Vec<Node> = Vec::with_capacity(limit);
            for node in upstream {
                if collected.len() == limit {
                    break;
                }
                collected.push(node);
            }
            let bound = collected.last().map(|n| n.row.clone());
            self.state.size = collected.len();
            self.state.bound = bound;
            let _ = compare_rows;
            return Box::new(collected.into_iter());
        }
        // Subsequent fetch — stream upstream, stop at bound.
        let Some(bound) = self.state.bound.clone() else {
            return Box::new(std::iter::empty());
        };
        let compare_rows = Arc::clone(&self.compare_rows);
        let upstream = self.input.fetch(req);
        Box::new(upstream.take_while(move |node| {
            compare_rows(&bound, &node.row) != CmpOrdering::Less
        }))
    }
}

impl Operator for Take {
    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        Box::new(self.push_iter(change))
    }
}

// ───────── TakePushIter — hand-rolled iterator ─────────────────────

pub struct TakePushIter<'a> {
    pending: VecDeque<Change>,
    deferred: Option<DeferredWork<'a>>,
}

enum DeferredWork<'a> {
    /// After eviction, clear/refresh the bound. First implementation
    /// just clears — fetch-path port will refine to re-fetch the next
    /// in-range row via `input.fetch(FetchRequest { start: after anchor })`.
    RefreshBoundAfter {
        input: &'a mut Box<dyn Input>,
        state: &'a mut TakeState,
        #[allow(dead_code)]
        anchor: Row,
    },
}

impl<'a> TakePushIter<'a> {
    fn new(
        change: Change,
        input: &'a mut Box<dyn Input>,
        state: &'a mut TakeState,
        limit: usize,
        compare_rows: Arc<dyn Fn(&Row, &Row) -> CmpOrdering + Send + Sync>,
    ) -> Self {
        let mut pending = VecDeque::new();
        let mut deferred: Option<DeferredWork<'a>> = None;

        match change {
            Change::Add(AddChange { node }) => {
                if state.size < limit {
                    state.size += 1;
                    let row = node.row.clone();
                    if state
                        .bound
                        .as_ref()
                        .map_or(true, |b| compare_rows(b, &row) == CmpOrdering::Less)
                    {
                        state.bound = Some(row);
                    }
                    pending.push_back(Change::Add(AddChange { node }));
                } else {
                    // At limit — new row evicts bound iff it ranks higher
                    // (i.e. compare_rows(bound, new) < 0 means bound is
                    // worse than new in asc ordering; adjust per schema).
                    let replaces = state
                        .bound
                        .as_ref()
                        .map_or(false, |b| compare_rows(b, &node.row) == CmpOrdering::Greater);
                    if replaces {
                        let old_bound_row = state.bound.clone().unwrap();
                        pending.push_back(Change::Remove(RemoveChange {
                            node: Node {
                                row: old_bound_row,
                                relationships: indexmap::IndexMap::new(),
                            },
                        }));
                        let new_row = node.row.clone();
                        state.bound = Some(new_row.clone());
                        pending.push_back(Change::Add(AddChange { node }));
                        deferred = Some(DeferredWork::RefreshBoundAfter {
                            input,
                            state,
                            anchor: new_row,
                        });
                    }
                }
            }
            Change::Remove(RemoveChange { node }) => {
                let was_bound = state
                    .bound
                    .as_ref()
                    .map_or(false, |b| compare_rows(b, &node.row) == CmpOrdering::Equal);
                state.size = state.size.saturating_sub(1);
                if was_bound {
                    state.bound = None;
                }
                pending.push_back(Change::Remove(RemoveChange { node }));
            }
            Change::Child(_) | Change::Edit(_) => {
                eprintln!("[TRACE ivm_v2] Take::push: Child/Edit deferred — dropped");
            }
        }

        Self { pending, deferred }
    }
}

impl<'a> Iterator for TakePushIter<'a> {
    type Item = Change;

    fn next(&mut self) -> Option<Change> {
        if let Some(y) = self.pending.pop_front() {
            return Some(y);
        }
        if let Some(work) = self.deferred.take() {
            match work {
                DeferredWork::RefreshBoundAfter {
                    input,
                    state,
                    anchor: _,
                } => {
                    // Minimal: fetch first available row; next iteration
                    // will pass `start: after anchor` once fetch is ported.
                    let mut rows = input.fetch(FetchRequest::default());
                    state.bound = rows.next().map(|n| n.row);
                    eprintln!(
                        "[TRACE ivm_v2] Take::push: RefreshBoundAfter has_bound={}",
                        state.bound.is_some()
                    );
                }
            }
        }
        None
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

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Minimal push-path tests for ivm_v2::Take.
    //!
    //! - Add under limit → forwarded.
    //! - Adds fill to limit, bound updates to worst row.
    //! - Add at limit with better row → Remove(bound) + Add(new).
    //! - Remove forwards unconditionally.
    //! - Destroy delegates.

    use super::*;
    use crate::ivm::change::ChangeType;
    use crate::ivm::data::make_comparator;
    use indexmap::IndexMap;
    use serde_json::json;
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;

    fn schema() -> SourceSchema {
        // Ascending sort on id.
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
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

    fn row(id: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r
    }
    fn node_of(id: i64) -> Node {
        Node {
            row: row(id),
            relationships: IndexMap::new(),
        }
    }

    /// Stub input: holds a list of rows in order, fetch returns them.
    struct StubInput {
        rows: Vec<Row>,
        schema: SourceSchema,
        destroyed: std::sync::atomic::AtomicBool,
    }
    impl InputBase for StubInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {
            self.destroyed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl Input for StubInput {
        fn fetch<'a>(&'a mut self, _req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
            let rows = self.rows.clone();
            Box::new(rows.into_iter().map(|r| Node {
                row: r,
                relationships: IndexMap::new(),
            }))
        }
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
    fn add_under_limit_forwards_and_updates_bound() {
        let input = StubInput {
            rows: vec![],
            schema: schema(),
            destroyed: false.into(),
        };
        let mut take = Take::new(Box::new(input), 3);

        // Ascending ordering: bound is the LARGEST accepted row.
        for id in [1, 2, 3] {
            let out: Vec<Change> = take.push_iter(Change::Add(AddChange {
                node: node_of(id),
            }))
            .collect();
            assert_eq!(out.len(), 1);
            assert_eq!(kind(&out[0]), ChangeType::Add);
        }
        assert_eq!(take.state.size, 3);
        assert_eq!(
            take.state.bound.as_ref().unwrap().get("id"),
            Some(&Some(json!(3)))
        );
    }

    #[test]
    fn add_at_limit_with_better_row_evicts_and_replaces() {
        // Stub input provides "replacement" rows after eviction. After
        // row 5 is added (evicting bound=3), RefreshBoundAfter fetches
        // from the stub; first row returned becomes the new bound.
        let input = StubInput {
            // Simulated "remaining" rows after eviction — just a placeholder.
            rows: vec![row(42)],
            schema: schema(),
            destroyed: false.into(),
        };
        let mut take = Take::new(Box::new(input), 3);

        // Fill to limit with 10, 20, 30 (bound=30 under asc).
        for id in [10, 20, 30] {
            let _: Vec<Change> = take.push_iter(Change::Add(AddChange {
                node: node_of(id),
            }))
            .collect();
        }
        assert_eq!(take.state.size, 3);

        // At limit. Push 5 — sorts BEFORE 30 (better), should evict.
        let out: Vec<Change> = take.push_iter(Change::Add(AddChange {
            node: node_of(5),
        }))
        .collect();
        assert_eq!(out.len(), 2, "expected Remove(30) + Add(5)");
        assert_eq!(kind(&out[0]), ChangeType::Remove);
        assert_eq!(kind(&out[1]), ChangeType::Add);

        // After eviction, RefreshBoundAfter ran during iteration and
        // updated bound to the first row our stub returned (id=42).
        assert_eq!(
            take.state.bound.as_ref().unwrap().get("id"),
            Some(&Some(json!(42)))
        );
    }

    #[test]
    fn add_at_limit_with_worse_row_drops() {
        let input = StubInput {
            rows: vec![],
            schema: schema(),
            destroyed: false.into(),
        };
        let mut take = Take::new(Box::new(input), 2);

        // Fill to limit with 1, 2 (bound=2 asc).
        for id in [1, 2] {
            let _: Vec<Change> = take.push_iter(Change::Add(AddChange {
                node: node_of(id),
            }))
            .collect();
        }

        // Push 100 — sorts AFTER 2 (worse for asc-limit). Should drop.
        let out: Vec<Change> = take.push_iter(Change::Add(AddChange {
            node: node_of(100),
        }))
        .collect();
        assert!(out.is_empty(), "worse-than-bound Add must not emit");
        assert_eq!(take.state.size, 2);
    }

    #[test]
    fn remove_forwards_and_decrements_size() {
        let input = StubInput {
            rows: vec![],
            schema: schema(),
            destroyed: false.into(),
        };
        let mut take = Take::new(Box::new(input), 3);
        for id in [1, 2, 3] {
            let _: Vec<Change> = take.push_iter(Change::Add(AddChange {
                node: node_of(id),
            }))
            .collect();
        }

        let out: Vec<Change> = take.push_iter(Change::Remove(RemoveChange {
            node: node_of(2),
        }))
        .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Remove);
        assert_eq!(take.state.size, 2);
    }

    #[test]
    fn destroy_delegates_to_input() {
        let input = StubInput {
            rows: vec![],
            schema: schema(),
            destroyed: false.into(),
        };
        // Clone the flag out of input via a raw pointer — rusqlite idiom
        // not usable here. Use an Arc instead.
        use std::sync::{
            atomic::{AtomicBool, Ordering as AtomOrdering},
            Arc as StdArc,
        };
        let flag = StdArc::new(AtomicBool::new(false));
        let flag2 = StdArc::clone(&flag);
        struct ObservableInput {
            schema: SourceSchema,
            destroyed: StdArc<AtomicBool>,
        }
        impl InputBase for ObservableInput {
            fn get_schema(&self) -> &SourceSchema {
                &self.schema
            }
            fn destroy(&mut self) {
                self.destroyed.store(true, AtomOrdering::SeqCst);
            }
        }
        impl Input for ObservableInput {
            fn fetch<'a>(
                &'a mut self,
                _req: FetchRequest,
            ) -> Box<dyn Iterator<Item = Node> + 'a> {
                Box::new(std::iter::empty())
            }
        }
        let observable = ObservableInput {
            schema: schema(),
            destroyed: flag2,
        };
        let mut take = Take::new(Box::new(observable), 3);
        let _ = input; // silence unused

        take.destroy();
        assert!(flag.load(AtomOrdering::SeqCst));
    }
}
