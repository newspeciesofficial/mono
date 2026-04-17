//! Take — Transformer shape with pending-refetch signalling.
//!
//! Partition-aware TakeT — mirror of TS `Take` at
//! `packages/zql/src/ivm/take.ts`. The TS operator takes an optional
//! `partitionKey: PartitionKey` (ctor at take.ts:59-77) and keys its
//! `(size, bound)` state by the partition values of the row or
//! constraint (take.ts:721-736 `getTakeStateKey`). RS previously held a
//! single global `(size, bound)` — which is equivalent to TS with
//! `partitionKey = undefined` — and diverged on subquery-limit shapes
//! (`.related(..., q => q.limit(N))` / `.one()`) because every parent's
//! child window shared the same state, producing false at-limit
//! evictions across partitions.
//!
//! When `partition_key` is `None` the transformer behaves exactly like
//! the TS unpartitioned path: a single state keyed by `"take"` (mirror
//! of `getTakeStateKey` with empty `partition_values`). When
//! `partition_key` is `Some`, state is keyed per partition value tuple
//! and each partition maintains its own independent window — matching
//! TS behavior for sub-queries attached via `applyCorrelatedSubQuery`
//! (builder.ts:631 hands `sq.correlation.childField` as the
//! `partitionKey` to `buildPipelineInternal`, which flows through to
//! the sub-query's `Take` at builder.ts:341).

use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use zero_cache_types::ast::CompoundKey;
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
    /// Mirror of TS `Take#partitionKey` at
    /// `packages/zql/src/ivm/take.ts:52,63`. `None` ≡ TS `undefined`
    /// (no partitioning — single global state).
    partition_key: Option<CompoundKey>,
    /// Map from `take_state_key(partition_key, row)` → per-partition
    /// `(size, bound)`. Mirror of TS `this.#storage.get(takeStateKey)`
    /// at `take.ts:94, 142, 213`. When `partition_key` is `None`, all
    /// rows share the single key `"take"`.
    states: HashMap<String, TakeState>,
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
        Self::new_with_partition(limit, compare_rows, None)
    }

    /// Full ctor — mirror of TS `new Take(input, storage, limit,
    /// partitionKey?)` at `take.ts:59-77`.
    pub fn new_with_partition(
        limit: usize,
        compare_rows: Arc<crate::ivm::data::Comparator>,
        partition_key: Option<CompoundKey>,
    ) -> Self {
        Self {
            limit,
            partition_key,
            states: HashMap::new(),
            compare_rows,
            pending_refetch: None,
        }
    }
}

/// Mirror of TS `getTakeStateKey` at
/// `packages/zql/src/ivm/take.ts:721-736`. Builds a deterministic key
/// from the partition column values of `row`. The TS version returns
/// `JSON.stringify(['take', ...partitionValues])`; RS uses a simple
/// `\x1F`-separated serialisation that is equivalent for equality
/// purposes (the key is opaque — the only contract is that two rows
/// with the same partition values map to the same key).
fn take_state_key(
    partition_key: Option<&CompoundKey>,
    row: &Row,
) -> String {
    let mut out = String::from("take");
    if let Some(pk) = partition_key {
        for col in pk.iter() {
            out.push('\x1F');
            // `row.get(col)` yields `Option<&Option<Value>>` — pull the
            // inner `Value` or write a sentinel for NULL / missing.
            match row.get(col) {
                Some(Some(v)) => out.push_str(&serde_json::to_string(v).unwrap_or_default()),
                _ => out.push_str("null"),
            }
        }
    }
    out
}

impl Transformer for TakeT {
    /// Mirror of TS `Take#fetch` at `packages/zql/src/ivm/take.ts:87-150`.
    ///
    /// Walks the upstream per-row, routing each to its partition's
    /// `(size, bound)`. Per partition:
    ///   - If the partition has no state yet: INITIAL-FETCH path — accept
    ///     up to `limit` rows, record size + bound (take.ts:152-209).
    ///   - If the partition already has state: RE-FETCH path — emit rows
    ///     while `comparator(bound, row) >= 0` (take.ts:107-120).
    ///
    /// Note: the upstream iterator may carry rows from multiple
    /// partitions in one call (hydrate scans the whole source with no
    /// constraint). Per-row partition routing handles both cases —
    /// matches TS `getTakeStateKey`-per-row lookup at take.ts:141. When
    /// `partition_key` is `None` everything maps to the single
    /// `"take"` key (unpartitioned global state).
    fn fetch_through<'a>(
        &'a mut self,
        upstream: Box<dyn Iterator<Item = Node> + 'a>,
        _req: FetchRequest,
    ) -> Box<dyn Iterator<Item = Node> + 'a> {
        let limit = self.limit;
        let partition_key = self.partition_key.clone();
        // Snapshot which partitions already had state AT WALK START — any
        // partition NOT in this set is being initialised during THIS walk
        // and must stay in initial-fetch mode (accept up to `limit`)
        // regardless of intermediate `self.states` mutations. Mirror of
        // TS `#initialFetch` at `take.ts:152-209` which owns the whole
        // upstream iterator for one partition's initialisation.
        let preexisting: std::collections::HashSet<String> =
            self.states.keys().cloned().collect();
        let mut collected: Vec<Node> = Vec::new();
        for n in upstream {
            let key = take_state_key(partition_key.as_ref(), &n.row);
            if !preexisting.contains(&key) {
                // INITIAL-FETCH for this partition — accept up to `limit`.
                let s = self.states.entry(key).or_default();
                if s.size < limit {
                    s.size += 1;
                    s.bound = Some(n.row.clone());
                    collected.push(n);
                }
                // else: at limit for this partition → drop.
            } else {
                // RE-FETCH for this partition — emit rows ≤ bound.
                // Mirror of TS `Take#fetch` take.ts:99-121.
                let s = self.states.get(&key).unwrap();
                if let Some(ref bound) = s.bound {
                    if (self.compare_rows)(bound, &n.row) != CmpOrdering::Less {
                        collected.push(n);
                    }
                    // else: row beyond bound → drop.
                }
                // else bound is None → partition was emptied → drop.
            }
        }
        Box::new(collected.into_iter())
    }

    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            let row = match &change {
                Change::Add(c) => &c.node.row,
                Change::Remove(c) => &c.node.row,
                Change::Edit(c) => &c.node.row,
                Change::Child(c) => &c.node.row,
            };
            let key = take_state_key(self.partition_key.as_ref(), row);
            let (size, has_bound) = self
                .states
                .get(&key)
                .map(|s| (s.size, s.bound.is_some()))
                .unwrap_or((0, false));
            eprintln!(
                "[ivm:rs:take_t:push] op={} partition={:?} size={} has_bound={}",
                match &change {
                    Change::Add(_) => "Add",
                    Change::Remove(_) => "Remove",
                    Change::Child(_) => "Child",
                    Change::Edit(_) => "Edit",
                },
                key,
                size,
                has_bound,
            );
        }
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
    ///
    /// Mirror of TS `Take#push` at `packages/zql/src/ivm/take.ts:240-426`.
    /// TS looks up the state by `getTakeStateKey(partitionKey, row)`
    /// (take.ts:212); RS does the same via `take_state_key`, so each
    /// partition's `(size, bound)` is independent.
    fn push_internal(&mut self, change: Change) -> Vec<Change> {
        let mut out: Vec<Change> = Vec::new();
        match change {
            Change::Add(AddChange { node }) => {
                let key = take_state_key(self.partition_key.as_ref(), &node.row);
                let (size, bound_cloned) = {
                    let s = self.states.entry(key.clone()).or_default();
                    (s.size, s.bound.clone())
                };
                if std::env::var("IVM_PARITY_TRACE").is_ok() {
                    eprintln!(
                        "[ivm:rs:take_t:internal_add] size={}/limit={} partition={:?} bound_id={:?} row_id={:?}",
                        size,
                        self.limit,
                        key,
                        bound_cloned.as_ref().and_then(|r| r.get("id").cloned()),
                        node.row.get("id").cloned()
                    );
                }
                if size < self.limit {
                    let s = self.states.entry(key).or_default();
                    s.size += 1;
                    let row = node.row.clone();
                    if s
                        .bound
                        .as_ref()
                        .map_or(true, |b| (self.compare_rows)(b, &row) == CmpOrdering::Less)
                    {
                        s.bound = Some(row);
                    }
                    out.push(Change::Add(AddChange { node }));
                } else {
                    // At limit: new row replaces bound iff it ranks
                    // *better* (lower in asc ordering).
                    let replaces = bound_cloned
                        .as_ref()
                        .map_or(false, |b| {
                            (self.compare_rows)(b, &node.row) == CmpOrdering::Greater
                        });
                    if std::env::var("IVM_PARITY_TRACE").is_ok() {
                        eprintln!(
                            "[ivm:rs:take_t:internal_add] at_limit replaces={}",
                            replaces
                        );
                    }
                    if replaces {
                        let old_bound = bound_cloned.clone().unwrap();
                        out.push(Change::Remove(RemoveChange {
                            node: Node {
                                row: old_bound.clone(),
                                relationships: IndexMap::new(),
                            },
                        }));
                        let new_row = node.row.clone();
                        let s = self.states.entry(key).or_default();
                        s.bound = Some(new_row.clone());
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
                // Mirror of TS `packages/zql/src/ivm/take.ts:334-343`:
                // if bound is undefined (no window yet) OR
                // `compareRows(change.row, bound) > 0` (row ranks AFTER
                // bound — i.e. outside the at-most-`limit` window),
                // the remove is a no-op and must NOT be emitted.
                // Without this guard RS emits phantom Removes for
                // rows that were never in the window, which produces
                // spurious adds/removes in edit-split cases
                // (fuzz_00168 canary: channel update reaches TakeT
                // as Remove(old) + Add(new); if the channel was
                // outside the limit window, RS previously emitted
                // Remove(old) then the Add branch re-entered below-
                // limit state and emitted Add(new), producing a net
                // phantom insert).
                let key = take_state_key(self.partition_key.as_ref(), &node.row);
                let (bound_opt, _size) = match self.states.get(&key) {
                    Some(s) => (s.bound.clone(), s.size),
                    None => return out,
                };
                let Some(bound) = bound_opt else { return out };
                let cmp = (self.compare_rows)(&node.row, &bound);
                if cmp == CmpOrdering::Greater {
                    return out;
                }
                let was_bound = cmp == CmpOrdering::Equal;
                let s = self.states.entry(key).or_default();
                s.size = s.size.saturating_sub(1);
                if was_bound {
                    s.bound = None;
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
                // Forward only if the parent row is currently in the
                // partition's window — mirror of TS take.ts:416-425
                // which looks up `takeState` for the row's partition then
                // checks `compareRows(change.node.row, takeState.bound) <= 0`.
                let key = take_state_key(self.partition_key.as_ref(), &child.node.row);
                let in_window = match self.states.get(&key) {
                    Some(s) => {
                        s.size < self.limit
                            || s.bound.as_ref().map_or(true, |b| {
                                (self.compare_rows)(b, &child.node.row) != CmpOrdering::Less
                            })
                    }
                    None => false,
                };
                if in_window {
                    out.push(Change::Child(child));
                }
            }
        }
        out
    }

    fn ingest_refetch(&mut self, rows: Vec<Node>) {
        // Chain hands back the next row(s) after a pending_refetch. The
        // refetch only fires from an at-limit eviction in the active
        // partition, so the first returned row becomes that partition's
        // new bound. Route by the returned row's partition key to stay
        // partition-correct.
        let Some(first) = rows.into_iter().next() else {
            return;
        };
        let key = take_state_key(self.partition_key.as_ref(), &first.row);
        let s = self.states.entry(key).or_default();
        s.bound = Some(first.row.clone());
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!(
                "[ivm:rs:take_t:ingest_refetch] partition={:?} new_bound_is_some=true",
                take_state_key(self.partition_key.as_ref(), &first.row)
            );
        }
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

    /// Single-partition state helper — mirrors TS's unpartitioned
    /// behaviour where every row routes to the single `"take"` state.
    fn single_state(t: &TakeT) -> Option<&TakeState> {
        t.states.get(&take_state_key(t.partition_key.as_ref(), &Row::new()))
    }

    #[test]
    fn fetch_through_hydrates_up_to_limit() {
        let mut t = TakeT::new(3, comparator());
        let upstream = vec![node_of(row(1)), node_of(row(2)), node_of(row(3)), node_of(row(4))];
        let out: Vec<Node> = t
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        assert_eq!(out.len(), 3);
        let s = single_state(&t).expect("state populated");
        assert_eq!(s.size, 3);
        assert_eq!(s.bound.as_ref().unwrap().get("id"), Some(&Some(json!(3))));
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
        assert_eq!(single_state(&t).unwrap().size, 3);
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
        assert_eq!(single_state(&t).unwrap().bound.as_ref().unwrap().get("id"), Some(&Some(json!(10))));
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
        assert_eq!(single_state(&t).unwrap().size, 1);
        // Bound cleared pending the next fetch.
        assert!(single_state(&t).unwrap().bound.is_none());
        // Remove-of-bound deferred-refetch not signalled in this first
        // cut; next hydrate re-populates.
        assert!(t.take_pending_refetch().is_none());
    }

    /// Partition-aware: two partitions with separate `(size, bound)`.
    /// Mirror of TS `take.fetch.test.ts:447` `partitionKey: ['issueID']`.
    #[test]
    fn partitioned_take_state_per_partition() {
        let mut t = TakeT::new_with_partition(
            2,
            comparator(),
            Some(vec!["partitionId".into()]),
        );
        // Rows from two partitions — source-ordered by id.
        let mk = |id: i64, part: &str| -> Row {
            let mut r = row(id);
            r.insert("partitionId".into(), Some(json!(part)));
            r
        };
        let upstream = vec![
            node_of(mk(1, "A")),
            node_of(mk(2, "A")),
            node_of(mk(3, "A")), // dropped — partition A at limit
            node_of(mk(10, "B")),
            node_of(mk(11, "B")),
        ];
        let out: Vec<Node> = t
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        // partition A emits 2 rows, partition B emits 2 → total 4.
        assert_eq!(out.len(), 4);
    }

    /// Partitioned push: Add in partition B must not evict bound of A.
    #[test]
    fn partitioned_push_does_not_cross_partitions() {
        let mut t = TakeT::new_with_partition(
            1,
            comparator(),
            Some(vec!["partitionId".into()]),
        );
        let mk = |id: i64, part: &str| -> Row {
            let mut r = row(id);
            r.insert("partitionId".into(), Some(json!(part)));
            r
        };
        let upstream = vec![node_of(mk(100, "A")), node_of(mk(50, "B"))];
        let _: Vec<Node> = t
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        // Push a row into partition B — size is already 1 (limit=1) in B.
        // New row ranks lower (5 < 50) so B should evict.
        let out: Vec<Change> = t
            .push(Change::Add(AddChange { node: node_of(mk(5, "B")) }))
            .collect();
        // Expect Remove(B:50) + Add(B:5). Partition A untouched.
        assert_eq!(out.len(), 2);
        // Push a row into partition A that would evict A:100 only if A's bound
        // is still 100. If partition leakage occurred, B's push would have
        // stomped A's state.
        let out_a: Vec<Change> = t
            .push(Change::Add(AddChange { node: node_of(mk(99, "A")) }))
            .collect();
        // A was at size=1 with bound=100, and 99 < 100, so eviction.
        assert_eq!(out_a.len(), 2);
    }
}
