//! Port of `packages/zql/src/ivm/take.ts`.
//!
//! Public exports ported:
//!
//! - [`Take`] — limit operator; keeps at most `limit` rows (per partition,
//!   if `partition_key` is set) ordered by the input's comparator.
//! - [`PartitionKey`] — TS `type PartitionKey = PrimaryKey`. A list of
//!   column names used to partition rows into independent take buckets.
//! - [`TakeState`] — TS `type TakeState = {size, bound}`. The persistent
//!   per-partition state. Exposed to mirror the TS type surface.
//! - [`MAX_BOUND_KEY`] — TS constant `'maxBound'`. Exposed for parity.
//!
//! # Storage layout
//!
//! TS stores state as two key-shapes in the generic `Storage`:
//!
//! - `JSON.stringify(['take', ...partitionValues])` → `TakeState`
//! - `'maxBound'` → `Row` (the largest bound across any partition)
//!
//! We use the same keys. Values are serialized through `serde_json::Value`
//! so they flow through the `Storage` trait (which is typed on
//! `JsonValue`). `TakeState` serializes as `{"size": N, "bound": Row|null}`;
//! the `Row` for `maxBound` serializes as an object via its
//! `IndexMap<String, Option<JsonValue>>` representation.
//!
//! # Ownership model (matches `filter.rs`)
//!
//! - Upstream via `Box<dyn Input>` owned inline.
//! - Downstream via `Mutex<Box<dyn Output>>` (default `ThrowOutput`).
//! - Storage via `Mutex<Box<dyn Storage>>` (TS Storage is inherently
//!   mutable during `fetch` — `#initialFetch` writes `TakeState` before
//!   returning).
//! - `row_hidden_from_fetch` via `Mutex<Option<Row>>` because push and
//!   fetch both consult it; the TS code sets it inside one push frame
//!   and reads it from fetch frames triggered by downstream push
//!   handlers.
//!
//! Every `fetch` / `push` call collects results eagerly before returning
//! the iterator so that no `MutexGuard` lives across the returned stream.

use std::sync::{Arc, Mutex};

use serde_json::Value as JsonValue;

use zero_cache_types::value::{Row, Value};

use crate::ivm::change::{AddChange, Change, EditChange, RemoveChange};
use crate::ivm::constraint::Constraint;
use crate::ivm::data::{Comparator, Node, NodeOrYield, compare_values};
use crate::ivm::operator::{
    FetchRequest, Input, InputBase, Operator, Output, Start, StartBasis, Storage, ThrowOutput,
};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

/// TS `type PartitionKey = PrimaryKey`. A list of column names.
pub type PartitionKey = Vec<String>;

/// TS `const MAX_BOUND_KEY = 'maxBound'`.
pub const MAX_BOUND_KEY: &str = "maxBound";

/// TS `type TakeState = { size: number; bound: Row | undefined }`.
#[derive(Debug, Clone, PartialEq)]
pub struct TakeState {
    pub size: usize,
    /// TS `bound: Row | undefined`. `None` corresponds to TS `undefined`
    /// (take has accepted 0 rows despite state being created).
    pub bound: Option<Row>,
}

// ─── (de)serialization helpers for Storage blobs ──────────────────────

/// Encode a [`TakeState`] into `JsonValue` for persisting via [`Storage`].
///
/// Mirrors the TS shape exactly — TS stores `{size, bound}` where
/// `bound` is a Row (plain object) or `undefined`. JSON has no
/// `undefined`, so `None` is written as JSON `null`.
fn take_state_to_json(s: &TakeState) -> JsonValue {
    let bound = match &s.bound {
        Some(r) => serde_json::to_value(r).expect("row serialization must succeed"),
        None => JsonValue::Null,
    };
    serde_json::json!({ "size": s.size, "bound": bound })
}

/// Decode a [`TakeState`] from a [`Storage`] blob. Panics if the stored
/// value is ill-formed — TS does not validate either; corruption here
/// is a programmer error.
fn take_state_from_json(v: &JsonValue) -> TakeState {
    let obj = v.as_object().expect("TakeState must be a JSON object");
    let size = obj
        .get("size")
        .and_then(|s| s.as_u64())
        .expect("TakeState.size must be a non-negative integer") as usize;
    let bound = match obj.get("bound") {
        Some(JsonValue::Null) | None => None,
        Some(other) => Some(
            serde_json::from_value::<Row>(other.clone())
                .expect("TakeState.bound must deserialize to Row"),
        ),
    };
    TakeState { size, bound }
}

fn row_to_json(r: &Row) -> JsonValue {
    serde_json::to_value(r).expect("row serialization must succeed")
}

fn row_from_json(v: &JsonValue) -> Row {
    serde_json::from_value(v.clone()).expect("stored row must deserialize")
}

// ─── Key and constraint helpers (TS free functions) ───────────────────

/// Source of partition values — either a full [`Row`] or a [`Constraint`]
/// that happens to pin every partition column.
enum RowOrConstraint<'a> {
    Row(&'a Row),
    Constraint(&'a Constraint),
}

fn value_from_source(src: &RowOrConstraint<'_>, key: &str) -> Value {
    match src {
        RowOrConstraint::Row(r) => r.get(key).cloned().unwrap_or(None),
        RowOrConstraint::Constraint(c) => c.get(key).cloned().unwrap_or(None),
    }
}

/// TS `getTakeStateKey(partitionKey, rowOrConstraint)`.
///
/// Always `JSON.stringify(['take', ...partitionValues])`, where the order
/// is the order declared in `partitionKey`. When `partitionKey` is
/// `None`, the values array is empty — yielding the single key
/// `'["take"]'`.
fn get_take_state_key(
    partition_key: Option<&[String]>,
    row_or_constraint: Option<&RowOrConstraint<'_>>,
) -> String {
    let mut partition_values: Vec<JsonValue> = Vec::new();
    if let (Some(pk), Some(src)) = (partition_key, row_or_constraint) {
        for key in pk {
            let v = value_from_source(src, key);
            partition_values.push(match v {
                Some(j) => j,
                None => JsonValue::Null,
            });
        }
    }
    let mut arr: Vec<JsonValue> = Vec::with_capacity(1 + partition_values.len());
    arr.push(JsonValue::String("take".to_string()));
    arr.extend(partition_values);
    serde_json::Value::Array(arr).to_string()
}

/// TS `constraintMatchesPartitionKey(constraint, partitionKey)`.
fn constraint_matches_partition_key(
    constraint: Option<&Constraint>,
    partition_key: Option<&[String]>,
) -> bool {
    match (constraint, partition_key) {
        (None, None) => true,
        // TS `return constraint === partitionKey`: if exactly one is
        // undefined, they're not equal. Only `undefined === undefined`
        // passes the fast-path.
        (None, Some(_)) | (Some(_), None) => false,
        (Some(c), Some(pk)) => {
            if pk.len() != c.len() {
                return false;
            }
            for key in pk {
                if !c.contains_key(key) {
                    return false;
                }
            }
            true
        }
    }
}

/// TS `makePartitionKeyComparator(partitionKey)`.
fn make_partition_key_comparator(partition_key: &[String]) -> Comparator {
    let keys: Vec<String> = partition_key.to_vec();
    Box::new(move |a: &Row, b: &Row| {
        for key in keys.iter() {
            let av = a.get(key).cloned().unwrap_or(None);
            let bv = b.get(key).cloned().unwrap_or(None);
            let cmp = compare_values(&av, &bv);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    })
}

// ─── Take operator ────────────────────────────────────────────────────

/// TS `Take` — a bounded (`limit`) operator, optionally partitioned.
pub struct Take {
    input: Box<dyn Input>,
    storage: Mutex<Box<dyn Storage>>,
    limit: usize,
    partition_key: Option<PartitionKey>,
    /// TS `#partitionKeyComparator` — used to assert edits don't cross
    /// partition boundaries.
    partition_key_comparator: Option<Comparator>,
    /// TS `#rowHiddenFromFetch` — transient fetch overlay during split
    /// pushes.
    row_hidden_from_fetch: Mutex<Option<Row>>,
    output: Mutex<Box<dyn Output>>,
}

impl Take {
    /// TS `new Take(input, storage, limit, partitionKey?)`.
    ///
    /// Panics when `limit < 0` (TS asserts) or the input's ordering
    /// doesn't include every PK column (TS asserts via
    /// `assertOrderingIncludesPK`).
    pub fn new(
        input: Box<dyn Input>,
        storage: Box<dyn Storage>,
        limit: usize,
        partition_key: Option<PartitionKey>,
    ) -> Self {
        // TS: `assert(limit >= 0, 'Limit must be non-negative')`. In Rust
        // `usize` cannot be negative; we still document the contract.
        //
        // TS: `assertOrderingIncludesPK(input.getSchema().sort, ... .primaryKey)`.
        let schema = input.get_schema();
        assert_ordering_includes_pk(&schema.sort, schema.primary_key.columns());
        // TS: `input.setOutput(this)` — we defer back-edge wiring to the
        // pipeline driver, matching `filter.rs` (same Rust ownership
        // cycle reason).
        let partition_key_comparator = partition_key
            .as_ref()
            .map(|k| make_partition_key_comparator(k));
        Self {
            input,
            storage: Mutex::new(storage),
            limit,
            partition_key,
            partition_key_comparator,
            row_hidden_from_fetch: Mutex::new(None),
            output: Mutex::new(Box::new(ThrowOutput)),
        }
    }

    /// Wired variant: matches TS `new Take(...)` followed by
    /// `input.setOutput(this)`. Returns `Arc<Mutex<Self>>` so the
    /// back-edge adapter ([`TakePushBackEdge`]) can retain a strong
    /// reference. Use [`ArcTakeAsInput`] to plug the result back
    /// into the chain as `Box<dyn Input>`.
    pub fn new_wired(
        input: Box<dyn Input>,
        storage: Box<dyn Storage>,
        limit: usize,
        partition_key: Option<PartitionKey>,
    ) -> Arc<Mutex<Self>> {
        let arc = Arc::new(Mutex::new(Self::new(input, storage, limit, partition_key)));
        let back: Box<dyn Output> = Box::new(TakePushBackEdge(Arc::clone(&arc)));
        arc.lock().unwrap().input.set_output(back);
        arc
    }

    fn storage_get(&self, key: &str) -> Option<JsonValue> {
        let s = self.storage.lock().expect("take storage mutex poisoned");
        s.get(key, None)
    }

    fn storage_set(&self, key: &str, value: JsonValue) {
        let mut s = self.storage.lock().expect("take storage mutex poisoned");
        s.set(key, value);
    }

    fn get_take_state(&self, key: &str) -> Option<TakeState> {
        self.storage_get(key).map(|v| take_state_from_json(&v))
    }

    fn get_max_bound(&self) -> Option<Row> {
        self.storage_get(MAX_BOUND_KEY).map(|v| row_from_json(&v))
    }

    /// TS `#setTakeState`.
    fn set_take_state(
        &self,
        take_state_key: &str,
        size: usize,
        bound: Option<Row>,
        max_bound: Option<&Row>,
    ) {
        let state = TakeState {
            size,
            bound: bound.clone(),
        };
        self.storage_set(take_state_key, take_state_to_json(&state));
        if let Some(ref b) = bound {
            let update_max = match max_bound {
                None => true,
                Some(mb) => {
                    let cmp = (self.input.get_schema().compare_rows)(b, mb);
                    cmp == std::cmp::Ordering::Greater
                }
            };
            if update_max {
                self.storage_set(MAX_BOUND_KEY, row_to_json(b));
            }
        }
    }

    /// TS `#getStateAndConstraint(row)`.
    fn get_state_and_constraint(
        &self,
        row: &Row,
    ) -> (Option<TakeState>, String, Option<Row>, Option<Constraint>) {
        let take_state_key = get_take_state_key(
            self.partition_key.as_deref(),
            Some(&RowOrConstraint::Row(row)),
        );
        let take_state = self.get_take_state(&take_state_key);
        let (max_bound, constraint) = if take_state.is_some() {
            let max_bound = self.get_max_bound();
            let constraint = self.partition_key.as_ref().map(|pk| {
                let mut c = Constraint::new();
                for k in pk {
                    let v = row.get(k).cloned().unwrap_or(None);
                    c.insert(k.clone(), v);
                }
                c
            });
            (max_bound, constraint)
        } else {
            (None, None)
        };
        (take_state, take_state_key, max_bound, constraint)
    }

    /// Collect the upstream `fetch` stream eagerly, filtering per the TS
    /// `fetch` logic.
    fn fetch_partition_bound(&self, req: FetchRequest) -> Vec<NodeOrYield> {
        let take_state_key = get_take_state_key(
            self.partition_key.as_deref(),
            req.constraint
                .as_ref()
                .map(|c| RowOrConstraint::Constraint(c))
                .as_ref(),
        );
        let take_state = self.get_take_state(&take_state_key);
        let Some(take_state) = take_state else {
            // TS: first ever call → hydrate via initialFetch.
            return self.initial_fetch(req);
        };
        let Some(ref bound) = take_state.bound else {
            // TS: take accepted 0 rows; nothing to yield.
            return Vec::new();
        };

        let schema = self.input.get_schema();
        let compare_rows = Arc::clone(&schema.compare_rows);
        let hidden = self
            .row_hidden_from_fetch
            .lock()
            .expect("row_hidden_from_fetch mutex poisoned")
            .clone();

        let mut out: Vec<NodeOrYield> = Vec::new();
        for inode in self.input.fetch(req) {
            match inode {
                NodeOrYield::Yield => {
                    out.push(NodeOrYield::Yield);
                    continue;
                }
                NodeOrYield::Node(node) => {
                    // TS: `if (compareRows(bound, row) < 0) return;`
                    if compare_rows(bound, &node.row) == std::cmp::Ordering::Less {
                        return out;
                    }
                    if let Some(ref hidden_row) = hidden {
                        if compare_rows(hidden_row, &node.row) == std::cmp::Ordering::Equal {
                            continue;
                        }
                    }
                    out.push(NodeOrYield::Node(node));
                }
            }
        }
        out
    }

    /// TS main fetch path when partition_key is set but the fetch is
    /// unconstrained (or constrained on the wrong keys).
    fn fetch_maxbound_scan(&self, req: FetchRequest) -> Vec<NodeOrYield> {
        let Some(max_bound) = self.get_max_bound() else {
            return Vec::new();
        };
        let schema = self.input.get_schema();
        let compare_rows = Arc::clone(&schema.compare_rows);

        let mut out: Vec<NodeOrYield> = Vec::new();
        for inode in self.input.fetch(req) {
            match inode {
                NodeOrYield::Yield => {
                    out.push(NodeOrYield::Yield);
                    continue;
                }
                NodeOrYield::Node(node) => {
                    // TS: `if (compareRows(row, maxBound) > 0) return;`
                    if compare_rows(&node.row, &max_bound) == std::cmp::Ordering::Greater {
                        return out;
                    }
                    let take_state_key = get_take_state_key(
                        self.partition_key.as_deref(),
                        Some(&RowOrConstraint::Row(&node.row)),
                    );
                    let take_state = self.get_take_state(&take_state_key);
                    if let Some(ts) = take_state {
                        if let Some(ref b) = ts.bound {
                            if compare_rows(b, &node.row) != std::cmp::Ordering::Less {
                                out.push(NodeOrYield::Node(node));
                            }
                        }
                    }
                }
            }
        }
        out
    }

    /// TS `#initialFetch`.
    fn initial_fetch(&self, req: FetchRequest) -> Vec<NodeOrYield> {
        // TS asserts — panic on violation (matches TS throw).
        assert!(req.start.is_none(), "Start should be undefined");
        assert!(!req.reverse.unwrap_or(false), "Reverse should be false");
        assert!(
            constraint_matches_partition_key(
                req.constraint.as_ref(),
                self.partition_key.as_deref()
            ),
            "Constraint should match partition key"
        );

        if self.limit == 0 {
            return Vec::new();
        }

        let take_state_key = get_take_state_key(
            self.partition_key.as_deref(),
            req.constraint
                .as_ref()
                .map(|c| RowOrConstraint::Constraint(c))
                .as_ref(),
        );
        assert!(
            self.get_take_state(&take_state_key).is_none(),
            "Take state should be undefined"
        );

        let mut size: usize = 0;
        let mut bound: Option<Row> = None;
        let mut out: Vec<NodeOrYield> = Vec::new();
        for inode in self.input.fetch(req) {
            match inode {
                NodeOrYield::Yield => {
                    out.push(NodeOrYield::Yield);
                }
                NodeOrYield::Node(node) => {
                    bound = Some(node.row.clone());
                    size += 1;
                    out.push(NodeOrYield::Node(node));
                    if size == self.limit {
                        break;
                    }
                }
            }
        }
        // TS finally block: always set state if no exception.
        let max_bound = self.get_max_bound();
        self.set_take_state(&take_state_key, size, bound, max_bound.as_ref());
        out
    }

    /// TS `#pushWithRowHiddenFromFetch(row, change)`. Drop-guard for
    /// TS `try/finally` that clears the overlay even on panic.
    fn push_with_row_hidden_from_fetch(
        &self,
        row: Row,
        change: Change,
        pusher: &dyn InputBase,
    ) -> Vec<Yield> {
        struct Guard<'a>(&'a Mutex<Option<Row>>);
        impl<'a> Drop for Guard<'a> {
            fn drop(&mut self) {
                *self.0.lock().expect("row_hidden_from_fetch mutex poisoned") = None;
            }
        }
        *self
            .row_hidden_from_fetch
            .lock()
            .expect("row_hidden_from_fetch mutex poisoned") = Some(row);
        let _guard = Guard(&self.row_hidden_from_fetch);
        self.output_push(change, pusher)
    }

    /// Call the downstream output's `push` and collect yields.
    fn output_push(&self, change: Change, pusher: &dyn InputBase) -> Vec<Yield> {
        let mut o = self.output.lock().expect("take output mutex poisoned");
        let stream = o.push(change, pusher);
        stream.collect()
    }
}

/// TS `assertOrderingIncludesPK` (from `query/complete-ordering.ts`).
///
/// Inlined here because it's the only use inside IVM, and porting the
/// full `complete-ordering` file is out of scope for this layer.
fn assert_ordering_includes_pk(sort: &[(String, zero_cache_types::ast::Direction)], pk: &[String]) {
    let ordering_fields: Vec<&str> = sort.iter().map(|(f, _)| f.as_str()).collect();
    let missing: Vec<&str> = pk
        .iter()
        .filter(|c| !ordering_fields.contains(&c.as_str()))
        .map(|s| s.as_str())
        .collect();
    assert!(
        missing.is_empty(),
        "Ordering must include all primary key fields. Missing: {}.",
        missing.join(", ")
    );
}

impl InputBase for Take {
    fn get_schema(&self) -> &SourceSchema {
        self.input.get_schema()
    }

    fn destroy(&mut self) {
        self.input.destroy();
    }
}

impl Input for Take {
    fn set_output(&mut self, output: Box<dyn Output>) {
        *self.output.lock().expect("take output mutex poisoned") = output;
    }

    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        // TS dispatch:
        //   - No partition key (unpartitioned Take) → partition bound path.
        //   - partition_key set and constraint matches → partition bound path.
        //   - partition_key set, constraint missing or on different keys →
        //     maxBound scan path.
        let use_partition_bound = match (&self.partition_key, req.constraint.as_ref()) {
            (None, _) => true,
            (Some(pk), Some(c)) if constraint_matches_partition_key(Some(c), Some(pk)) => true,
            _ => false,
        };
        let collected = if use_partition_bound {
            self.fetch_partition_bound(req)
        } else {
            self.fetch_maxbound_scan(req)
        };
        Box::new(collected.into_iter())
    }
}

impl Output for Take {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        // Borrow the upstream as `&dyn InputBase` to forward downstream
        // as the pusher identity. This matches `filter.rs`: `&*self.input`
        // is a shared reborrow and `self.input` is never mutably
        // borrowed concurrently below (internal mutation happens through
        // the `Mutex`s on `storage` / `output` / `row_hidden_from_fetch`).
        let pusher: &dyn InputBase = &*self.input;
        let yields = self.push_impl(change, pusher);
        Box::new(yields.into_iter())
    }
}

impl Operator for Take {}

/// Back-edge adapter installed on [`Take::input`] so pushes from the
/// upstream (source connection) route into [`Take::push`]. Created by
/// [`Take::new_wired`].
pub struct TakePushBackEdge(pub Arc<Mutex<Take>>);

impl Output for TakePushBackEdge {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let mut guard = self.0.lock().expect("take back-edge mutex poisoned");
        let items: Vec<Yield> = guard.push(change, pusher).collect();
        drop(guard);
        Box::new(items.into_iter())
    }
}

/// Adapter that lets a wired [`Arc<Mutex<Take>>`] be plugged back into
/// the chain as `Box<dyn Input>`. Schema cached at construction.
pub struct ArcTakeAsInput {
    inner: Arc<Mutex<Take>>,
    schema: SourceSchema,
}

impl ArcTakeAsInput {
    pub fn new(inner: Arc<Mutex<Take>>) -> Self {
        let schema = inner.lock().unwrap().get_schema().clone();
        Self { inner, schema }
    }
}

impl InputBase for ArcTakeAsInput {
    fn get_schema(&self) -> &SourceSchema { &self.schema }
    fn destroy(&mut self) { self.inner.lock().unwrap().destroy(); }
}

impl Input for ArcTakeAsInput {
    fn set_output(&mut self, output: Box<dyn Output>) {
        self.inner.lock().unwrap().set_output(output);
    }
    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        let guard = self.inner.lock().unwrap();
        let items: Vec<NodeOrYield> = guard.fetch(req).collect();
        drop(guard);
        Box::new(items.into_iter())
    }
}

impl Output for ArcTakeAsInput {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let mut guard = self.inner.lock().unwrap();
        let items: Vec<Yield> = guard.push(change, pusher).collect();
        drop(guard);
        Box::new(items.into_iter())
    }
}

impl Operator for ArcTakeAsInput {}

impl Take {
    fn push_impl(&self, change: Change, pusher: &dyn InputBase) -> Vec<Yield> {
        // TS `push(change)`.
        if let Change::Edit(edit) = &change {
            return self.push_edit_change(edit.clone(), pusher);
        }

        let row_ref = change.node().row.clone();
        let (take_state, take_state_key, max_bound, constraint) =
            self.get_state_and_constraint(&row_ref);
        let Some(take_state) = take_state else {
            return Vec::new();
        };

        let compare_rows = Arc::clone(&self.input.get_schema().compare_rows);

        match &change {
            Change::Add(AddChange { node }) => {
                if take_state.size < self.limit {
                    // Below limit: accept unconditionally and maybe extend bound.
                    let new_bound = match take_state.bound.as_ref() {
                        None => Some(node.row.clone()),
                        Some(b) => {
                            if compare_rows(b, &node.row) == std::cmp::Ordering::Less {
                                Some(node.row.clone())
                            } else {
                                Some(b.clone())
                            }
                        }
                    };
                    self.set_take_state(
                        &take_state_key,
                        take_state.size + 1,
                        new_bound,
                        max_bound.as_ref(),
                    );
                    return self.output_push(change, pusher);
                }
                // At limit. Reject if >= bound.
                let bound = take_state
                    .bound
                    .as_ref()
                    .expect("size == limit implies bound set");
                if compare_rows(&node.row, bound) != std::cmp::Ordering::Less {
                    return Vec::new();
                }
                // Row goes strictly below bound: evict the current bound.
                let (bound_node, before_bound_node) =
                    self.find_evict_targets(bound, constraint.as_ref());
                let bound_node = bound_node.expect("Take: boundNode must be found during fetch");

                let remove_change = Change::Remove(RemoveChange {
                    node: bound_node.clone(),
                });
                // New bound selection: either `change.node.row` if it is
                // strictly greater than before_bound_node.row, else the
                // before_bound_node.row.
                let new_bound_row = match before_bound_node.as_ref() {
                    None => node.row.clone(),
                    Some(bb) => {
                        if compare_rows(&node.row, &bb.row) == std::cmp::Ordering::Greater {
                            node.row.clone()
                        } else {
                            bb.row.clone()
                        }
                    }
                };
                self.set_take_state(
                    &take_state_key,
                    take_state.size,
                    Some(new_bound_row),
                    max_bound.as_ref(),
                );
                let mut yields =
                    self.push_with_row_hidden_from_fetch(node.row.clone(), remove_change, pusher);
                yields.extend(self.output_push(change, pusher));
                yields
            }
            Change::Remove(RemoveChange { node }) => {
                let Some(bound) = take_state.bound.clone() else {
                    // TS: `change is after bound` (empty bucket).
                    return Vec::new();
                };
                let cmp_to_bound = compare_rows(&node.row, &bound);
                if cmp_to_bound == std::cmp::Ordering::Greater {
                    return Vec::new();
                }

                // Fetch the first item after the bound (reverse) to see
                // if there's a pull-in candidate.
                let before_bound_node: Option<Node> = {
                    let req = FetchRequest {
                        start: Some(Start {
                            row: bound.clone(),
                            basis: StartBasis::After,
                        }),
                        constraint: constraint.clone(),
                        reverse: Some(true),
                    };
                    let mut out: Option<Node> = None;
                    for n in self.input.fetch(req) {
                        match n {
                            NodeOrYield::Yield => continue,
                            NodeOrYield::Node(node) => {
                                out = Some(node);
                                break;
                            }
                        }
                    }
                    out
                };

                let mut new_bound: Option<NewBound> = before_bound_node.as_ref().map(|n| {
                    let push = compare_rows(&n.row, &bound) == std::cmp::Ordering::Greater;
                    NewBound {
                        node: n.clone(),
                        push,
                    }
                });
                let is_push = new_bound.as_ref().map(|nb| nb.push).unwrap_or(false);
                if !is_push {
                    let req = FetchRequest {
                        start: Some(Start {
                            row: bound.clone(),
                            basis: StartBasis::At,
                        }),
                        constraint: constraint.clone(),
                        reverse: Some(false),
                    };
                    for n in self.input.fetch(req) {
                        match n {
                            NodeOrYield::Yield => continue,
                            NodeOrYield::Node(node) => {
                                let push =
                                    compare_rows(&node.row, &bound) == std::cmp::Ordering::Greater;
                                new_bound = Some(NewBound {
                                    node: node.clone(),
                                    push,
                                });
                                if push {
                                    break;
                                }
                            }
                        }
                    }
                }

                if let Some(nb) = &new_bound {
                    if nb.push {
                        let mut yields = self.output_push(change.clone(), pusher);
                        self.set_take_state(
                            &take_state_key,
                            take_state.size,
                            Some(nb.node.row.clone()),
                            max_bound.as_ref(),
                        );
                        yields.extend(self.output_push(
                            Change::Add(AddChange {
                                node: nb.node.clone(),
                            }),
                            pusher,
                        ));
                        return yields;
                    }
                }
                // Underflow: size shrinks, bound becomes the same-or-earlier
                // row (or None if the partition emptied).
                let new_bound_row = new_bound.map(|nb| nb.node.row);
                self.set_take_state(
                    &take_state_key,
                    take_state.size - 1,
                    new_bound_row,
                    max_bound.as_ref(),
                );
                self.output_push(change, pusher)
            }
            Change::Child(_) => {
                // TS: forward only if row <= bound (child rows are already
                // in output iff their parent is).
                if let Some(ref bound) = take_state.bound {
                    if compare_rows(&row_ref, bound) != std::cmp::Ordering::Greater {
                        return self.output_push(change, pusher);
                    }
                }
                Vec::new()
            }
            Change::Edit(_) => unreachable!("handled above"),
        }
    }

    /// TS branch inside `push` Add at limit: identifies the current
    /// bound node (to evict) and possibly the one before it (to
    /// become the new bound if smaller than the insertion).
    fn find_evict_targets(
        &self,
        bound: &Row,
        constraint: Option<&Constraint>,
    ) -> (Option<Node>, Option<Node>) {
        if self.limit == 1 {
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: StartBasis::At,
                }),
                constraint: constraint.cloned(),
                reverse: Some(false),
            };
            let mut bound_node: Option<Node> = None;
            for n in self.input.fetch(req) {
                match n {
                    NodeOrYield::Yield => continue,
                    NodeOrYield::Node(node) => {
                        bound_node = Some(node);
                        break;
                    }
                }
            }
            (bound_node, None)
        } else {
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: StartBasis::At,
                }),
                constraint: constraint.cloned(),
                reverse: Some(true),
            };
            let mut bound_node: Option<Node> = None;
            let mut before_bound: Option<Node> = None;
            for n in self.input.fetch(req) {
                match n {
                    NodeOrYield::Yield => continue,
                    NodeOrYield::Node(node) => {
                        if bound_node.is_none() {
                            bound_node = Some(node);
                        } else {
                            before_bound = Some(node);
                            break;
                        }
                    }
                }
            }
            (bound_node, before_bound)
        }
    }

    /// TS `#pushEditChange`.
    fn push_edit_change(&self, change: EditChange, pusher: &dyn InputBase) -> Vec<Yield> {
        if let Some(ref cmp) = self.partition_key_comparator {
            assert!(
                cmp(&change.old_node.row, &change.node.row) == std::cmp::Ordering::Equal,
                "Unexpected change of partition key"
            );
        }

        let (take_state, take_state_key, max_bound, constraint) =
            self.get_state_and_constraint(&change.old_node.row);
        let Some(take_state) = take_state else {
            return Vec::new();
        };
        let bound = take_state.bound.clone().expect("Bound should be set");
        let compare_rows = Arc::clone(&self.input.get_schema().compare_rows);
        let old_cmp = compare_rows(&change.old_node.row, &bound);
        let new_cmp = compare_rows(&change.node.row, &bound);

        // TS `replaceBoundAndForwardChange`:
        let replace_bound_and_forward = |new_row: Row| -> Vec<Yield> {
            self.set_take_state(
                &take_state_key,
                take_state.size,
                Some(new_row),
                max_bound.as_ref(),
            );
            self.output_push(Change::Edit(change.clone()), pusher)
        };

        use std::cmp::Ordering as O;

        if old_cmp == O::Equal {
            if new_cmp == O::Equal {
                // Keeping bounds; no state update.
                return self.output_push(Change::Edit(change.clone()), pusher);
            }
            if new_cmp == O::Less {
                if self.limit == 1 {
                    return replace_bound_and_forward(change.node.row.clone());
                }
                // Find row before the old bound to become the new bound.
                let req = FetchRequest {
                    start: Some(Start {
                        row: bound.clone(),
                        basis: StartBasis::After,
                    }),
                    constraint: constraint.clone(),
                    reverse: Some(true),
                };
                let mut before_bound: Option<Node> = None;
                for n in self.input.fetch(req) {
                    match n {
                        NodeOrYield::Yield => continue,
                        NodeOrYield::Node(node) => {
                            before_bound = Some(node);
                            break;
                        }
                    }
                }
                let before_bound =
                    before_bound.expect("Take: beforeBoundNode must be found during fetch");
                self.set_take_state(
                    &take_state_key,
                    take_state.size,
                    Some(before_bound.row.clone()),
                    max_bound.as_ref(),
                );
                return self.output_push(Change::Edit(change.clone()), pusher);
            }
            // new_cmp > 0: old row was the bound, new row is beyond it.
            assert!(
                new_cmp == O::Greater,
                "New comparison must be greater than 0"
            );
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: StartBasis::At,
                }),
                constraint: constraint.clone(),
                reverse: Some(false),
            };
            let mut new_bound_node: Option<Node> = None;
            for n in self.input.fetch(req) {
                match n {
                    NodeOrYield::Yield => continue,
                    NodeOrYield::Node(node) => {
                        new_bound_node = Some(node);
                        break;
                    }
                }
            }
            let new_bound_node =
                new_bound_node.expect("Take: newBoundNode must be found during fetch");
            if compare_rows(&new_bound_node.row, &change.node.row) == O::Equal {
                return replace_bound_and_forward(change.node.row.clone());
            }
            // New row falls outside; remove old, add the new bound.
            self.set_take_state(
                &take_state_key,
                take_state.size,
                Some(new_bound_node.row.clone()),
                max_bound.as_ref(),
            );
            let mut yields = self.push_with_row_hidden_from_fetch(
                new_bound_node.row.clone(),
                Change::Remove(RemoveChange {
                    node: change.old_node.clone(),
                }),
                pusher,
            );
            yields.extend(self.output_push(
                Change::Add(AddChange {
                    node: new_bound_node,
                }),
                pusher,
            ));
            return yields;
        }

        if old_cmp == O::Greater {
            assert!(
                new_cmp != O::Equal,
                "Invalid state. Row has duplicate primary key"
            );
            if new_cmp == O::Greater {
                // Both outside: drop.
                return Vec::new();
            }
            assert!(new_cmp == O::Less, "New comparison must be less than 0");
            // Old outside, new inside. Need to evict the current bound
            // and add change.node as a fresh Add.
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: StartBasis::At,
                }),
                constraint: constraint.clone(),
                reverse: Some(true),
            };
            let mut old_bound_node: Option<Node> = None;
            let mut new_bound_node: Option<Node> = None;
            for n in self.input.fetch(req) {
                match n {
                    NodeOrYield::Yield => continue,
                    NodeOrYield::Node(node) => {
                        if old_bound_node.is_none() {
                            old_bound_node = Some(node);
                        } else {
                            new_bound_node = Some(node);
                            break;
                        }
                    }
                }
            }
            let old_bound_node =
                old_bound_node.expect("Take: oldBoundNode must be found during fetch");
            let new_bound_node =
                new_bound_node.expect("Take: newBoundNode must be found during fetch");
            self.set_take_state(
                &take_state_key,
                take_state.size,
                Some(new_bound_node.row.clone()),
                max_bound.as_ref(),
            );
            let mut yields = self.push_with_row_hidden_from_fetch(
                change.node.row.clone(),
                Change::Remove(RemoveChange {
                    node: old_bound_node,
                }),
                pusher,
            );
            yields.extend(self.output_push(
                Change::Add(AddChange {
                    node: change.node.clone(),
                }),
                pusher,
            ));
            return yields;
        }

        // old_cmp < 0.
        assert!(
            new_cmp != O::Equal,
            "Invalid state. Row has duplicate primary key"
        );
        if new_cmp == O::Less {
            return self.output_push(Change::Edit(change.clone()), pusher);
        }
        assert!(
            new_cmp == O::Greater,
            "New comparison must be greater than 0"
        );
        // Old inside, new beyond old bound.
        let req = FetchRequest {
            start: Some(Start {
                row: bound.clone(),
                basis: StartBasis::After,
            }),
            constraint: constraint.clone(),
            reverse: Some(false),
        };
        let mut after_bound: Option<Node> = None;
        for n in self.input.fetch(req) {
            match n {
                NodeOrYield::Yield => continue,
                NodeOrYield::Node(node) => {
                    after_bound = Some(node);
                    break;
                }
            }
        }
        let after_bound = after_bound.expect("Take: afterBoundNode must be found during fetch");
        if compare_rows(&after_bound.row, &change.node.row) == O::Equal {
            return replace_bound_and_forward(change.node.row.clone());
        }
        // Else: remove old, add the row after the bound.
        let mut yields = self.output_push(
            Change::Remove(RemoveChange {
                node: change.old_node.clone(),
            }),
            pusher,
        );
        self.set_take_state(
            &take_state_key,
            take_state.size,
            Some(after_bound.row.clone()),
            max_bound.as_ref(),
        );
        yields.extend(self.output_push(Change::Add(AddChange { node: after_bound }), pusher));
        yields
    }
}

struct NewBound {
    node: Node,
    push: bool,
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests {
    //! Branch coverage plan.
    //!
    //! Free functions:
    //!   - `constraint_matches_partition_key`: (None, None), (None, Some),
    //!     (Some, None), length mismatch, key missing, exact match.
    //!   - `get_take_state_key`: unpartitioned (no values), partitioned
    //!     from Row, partitioned from Constraint, partition value is null.
    //!   - `make_partition_key_comparator`: equal, less via first key,
    //!     tie then second key.
    //!   - `take_state_to_json` / `_from_json`: round-trip with `bound`
    //!     and with `bound: None`.
    //!   - `assert_ordering_includes_pk`: missing PK column panics;
    //!     complete ordering passes.
    //!
    //! `Take::new`:
    //!   - Panic when PK column absent from ordering.
    //!
    //! Fetch (partition_bound path):
    //!   - Initial fetch (take_state absent) hydrates state and emits
    //!     up to `limit` rows.
    //!   - `limit == 0` short-circuits.
    //!   - Subsequent fetch with `bound=None` returns nothing.
    //!   - Subsequent fetch streams upstream rows <= bound and stops
    //!     at the first row > bound.
    //!   - `row_hidden_from_fetch` suppresses exactly one row matching
    //!     the overlay.
    //!   - Yield sentinels propagate from upstream.
    //!   - Initial fetch panics on `start = Some`.
    //!   - Initial fetch panics on `reverse = true`.
    //!   - Initial fetch panics when constraint doesn't match PK.
    //!
    //! Fetch (maxbound scan path):
    //!   - `maxBound` absent → empty.
    //!   - Row > maxBound stops the scan.
    //!   - Row with no matching take state is skipped.
    //!   - Row with matching state and within that state's bound is
    //!     yielded.
    //!
    //! Push:
    //!   - No take state → drop.
    //!   - Add below limit → Add forwarded, size++, bound tracks max.
    //!   - Add at limit, >= bound → drop.
    //!   - Add at limit, < bound, limit==1 → evict bound, add.
    //!   - Add at limit, < bound, limit>1 → evict, pick new bound.
    //!   - Remove after bound → drop.
    //!   - Remove at bound, replacement available → Remove + new Add.
    //!   - Remove at bound, no replacement → Remove, size--.
    //!   - Remove when bound is None → drop.
    //!   - Child push with row <= bound → forwarded.
    //!   - Child push with row > bound → dropped.
    //!   - Edit, old == new == bound → forward with no state change.
    //!   - Edit, new < old == bound, limit==1 → replaceBound path.
    //!   - Edit, new < old == bound, limit>1 → new bound is beforeBound.
    //!   - Edit, new > old == bound → split remove+add.
    //!   - Edit old outside, new outside → drop.
    //!   - Edit old outside, new inside → split remove+add.
    //!   - Edit old inside (<bound), new inside → forward edit.
    //!   - Edit old inside, new outside → split remove+add with
    //!     replaceBound fast-path.
    //!   - Edit with partition-key divergence panics.
    //!
    //! Integration:
    //!   - `set_output` swaps the default ThrowOutput.
    //!   - `get_schema` delegates to upstream.
    //!   - `destroy` delegates to upstream.

    use super::*;
    use crate::ivm::change::{AddChange, ChildChange, ChildSpec, EditChange, RemoveChange};
    use crate::ivm::data::make_comparator;
    use crate::ivm::memory_storage::MemoryStorage;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicBool, Ordering as AtOrdering};
    use zero_cache_types::ast::{Direction, Ordering as AstOrdering, System};
    use zero_cache_types::primary_key::PrimaryKey;

    fn make_schema() -> SourceSchema {
        let sort: AstOrdering = vec![("id".to_string(), Direction::Asc)];
        SourceSchema {
            table_name: "t".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: StdArc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    fn make_schema_partitioned(pk_cols: &[&str], sort_cols: &[&str]) -> SourceSchema {
        let sort: AstOrdering = sort_cols
            .iter()
            .map(|c| ((*c).to_string(), Direction::Asc))
            .collect();
        SourceSchema {
            table_name: "t".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(pk_cols.iter().map(|s| (*s).to_string()).collect()),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: StdArc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    fn row(id: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r
    }

    fn row_part(part: &str, id: i64) -> Row {
        let mut r = Row::new();
        r.insert("issue_id".into(), Some(json!(part)));
        r.insert("id".into(), Some(json!(id)));
        r
    }

    fn node(id: i64) -> Node {
        Node {
            row: row(id),
            relationships: IndexMap::new(),
        }
    }

    fn node_of(row: Row) -> Node {
        Node {
            row,
            relationships: IndexMap::new(),
        }
    }

    /// Upstream that serves a fixed, sorted set of rows. Implements
    /// Input with support for `constraint` (pk filter) and `start`
    /// (basis at/after) and `reverse`.
    struct FakeSource {
        schema: SourceSchema,
        rows: Mutex<Vec<Row>>,
        destroyed: StdArc<AtomicBool>,
    }
    impl FakeSource {
        fn new(schema: SourceSchema, rows: Vec<Row>) -> Self {
            Self {
                schema,
                rows: Mutex::new(rows),
                destroyed: StdArc::new(AtomicBool::new(false)),
            }
        }
        #[allow(dead_code)]
        fn set_rows(&self, rows: Vec<Row>) {
            *self.rows.lock().unwrap() = rows;
        }
    }
    impl InputBase for FakeSource {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {
            self.destroyed.store(true, AtOrdering::SeqCst);
        }
    }
    impl Input for FakeSource {
        fn set_output(&mut self, _o: Box<dyn Output>) {}
        fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
            let compare = Arc::clone(&self.schema.compare_rows);
            let rows = self.rows.lock().unwrap().clone();
            let mut filtered: Vec<Row> = rows
                .into_iter()
                .filter(|r| match &req.constraint {
                    None => true,
                    Some(c) => c.iter().all(|(k, v)| {
                        let rv = r.get(k).cloned().unwrap_or(None);
                        rv == *v
                    }),
                })
                .collect();
            filtered.sort_by(|a, b| compare(a, b));
            if req.reverse.unwrap_or(false) {
                filtered.reverse();
            }
            if let Some(start) = req.start {
                let cmp_fwd: Arc<Comparator> = Arc::clone(&self.schema.compare_rows);
                // After reverse is applied, the row ordering viewed through
                // the stream is still ascending or descending relative to
                // the schema; `start.row` is given in natural order.
                // Position: skip until we reach (or pass) `start.row`.
                // TS: basis 'at' includes the matching row; 'after' skips it.
                if req.reverse.unwrap_or(false) {
                    // In reverse mode, we iterate largest→smallest.
                    // 'at': include rows <= start.row (start.row included).
                    // 'after': include rows < start.row.
                    filtered = filtered
                        .into_iter()
                        .filter(|r| {
                            let c = cmp_fwd(r, &start.row);
                            match start.basis {
                                StartBasis::At => c != std::cmp::Ordering::Greater,
                                StartBasis::After => c == std::cmp::Ordering::Less,
                            }
                        })
                        .collect();
                } else {
                    filtered = filtered
                        .into_iter()
                        .filter(|r| {
                            let c = cmp_fwd(r, &start.row);
                            match start.basis {
                                StartBasis::At => c != std::cmp::Ordering::Less,
                                StartBasis::After => c == std::cmp::Ordering::Greater,
                            }
                        })
                        .collect();
                }
            }
            let iter = filtered.into_iter().map(|r| NodeOrYield::Node(node_of(r)));
            Box::new(iter)
        }
    }

    /// Records every downstream push.
    struct Recorder {
        events: StdArc<Mutex<Vec<Change>>>,
    }
    impl Output for Recorder {
        fn push<'a>(&'a mut self, change: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.events.lock().unwrap().push(change);
            Box::new(std::iter::empty())
        }
    }

    fn mk_take(
        schema: SourceSchema,
        upstream_rows: Vec<Row>,
        limit: usize,
        partition_key: Option<PartitionKey>,
    ) -> (Take, StdArc<Mutex<Vec<Change>>>) {
        let src = FakeSource::new(schema, upstream_rows);
        let storage = Box::new(MemoryStorage::new()) as Box<dyn Storage>;
        let mut t = Take::new(Box::new(src), storage, limit, partition_key);
        let events: StdArc<Mutex<Vec<Change>>> = StdArc::new(Mutex::new(vec![]));
        t.set_output(Box::new(Recorder {
            events: StdArc::clone(&events),
        }));
        (t, events)
    }

    fn drain(s: Stream<'_, NodeOrYield>) -> Vec<NodeOrYield> {
        s.collect()
    }

    // ─── free function branches ────────────────────────────────────────

    // Branch: both None → equal (TS `undefined === undefined`).
    #[test]
    fn cmpk_both_none_is_true() {
        assert!(constraint_matches_partition_key(None, None));
    }

    // Branch: constraint None, partitionKey Some → false.
    #[test]
    fn cmpk_constraint_missing_returns_false() {
        let pk = vec!["a".to_string()];
        assert!(!constraint_matches_partition_key(None, Some(&pk)));
    }

    // Branch: constraint Some, partitionKey None → false.
    #[test]
    fn cmpk_partition_missing_returns_false() {
        let mut c = Constraint::new();
        c.insert("a".into(), Some(json!(1)));
        assert!(!constraint_matches_partition_key(Some(&c), None));
    }

    // Branch: length mismatch → false.
    #[test]
    fn cmpk_length_mismatch_returns_false() {
        let pk = vec!["a".to_string(), "b".to_string()];
        let mut c = Constraint::new();
        c.insert("a".into(), Some(json!(1)));
        assert!(!constraint_matches_partition_key(Some(&c), Some(&pk)));
    }

    // Branch: same length, missing key → false.
    #[test]
    fn cmpk_key_missing_returns_false() {
        let pk = vec!["a".to_string(), "b".to_string()];
        let mut c = Constraint::new();
        c.insert("a".into(), Some(json!(1)));
        c.insert("other".into(), Some(json!(2)));
        assert!(!constraint_matches_partition_key(Some(&c), Some(&pk)));
    }

    // Branch: exact match → true.
    #[test]
    fn cmpk_exact_match_returns_true() {
        let pk = vec!["a".to_string(), "b".to_string()];
        let mut c = Constraint::new();
        c.insert("a".into(), Some(json!(1)));
        c.insert("b".into(), Some(json!(2)));
        assert!(constraint_matches_partition_key(Some(&c), Some(&pk)));
    }

    // Branch: unpartitioned key.
    #[test]
    fn get_take_state_key_unpartitioned() {
        let k = get_take_state_key(None, None);
        assert_eq!(k, "[\"take\"]");
    }

    // Branch: partitioned from Row.
    #[test]
    fn get_take_state_key_partitioned_from_row() {
        let r = row_part("p1", 42);
        let pk = vec!["issue_id".to_string()];
        let k = get_take_state_key(Some(&pk), Some(&RowOrConstraint::Row(&r)));
        assert_eq!(k, "[\"take\",\"p1\"]");
    }

    // Branch: partitioned from Constraint.
    #[test]
    fn get_take_state_key_partitioned_from_constraint() {
        let mut c = Constraint::new();
        c.insert("issue_id".into(), Some(json!("p2")));
        let pk = vec!["issue_id".to_string()];
        let k = get_take_state_key(Some(&pk), Some(&RowOrConstraint::Constraint(&c)));
        assert_eq!(k, "[\"take\",\"p2\"]");
    }

    // Branch: partition value is null/undefined.
    #[test]
    fn get_take_state_key_partitioned_null_value() {
        let mut c = Constraint::new();
        c.insert("issue_id".into(), None);
        let pk = vec!["issue_id".to_string()];
        let k = get_take_state_key(Some(&pk), Some(&RowOrConstraint::Constraint(&c)));
        assert_eq!(k, "[\"take\",null]");
    }

    // Branch: partition comparator — equal.
    #[test]
    fn partition_cmp_equal() {
        let cmp = make_partition_key_comparator(&["p".to_string()]);
        let a = {
            let mut r = Row::new();
            r.insert("p".into(), Some(json!("x")));
            r
        };
        assert_eq!(cmp(&a, &a), std::cmp::Ordering::Equal);
    }

    // Branch: partition comparator — first key decides.
    #[test]
    fn partition_cmp_first_key_decides() {
        let cmp = make_partition_key_comparator(&["a".to_string(), "b".to_string()]);
        let mut x = Row::new();
        x.insert("a".into(), Some(json!(1)));
        x.insert("b".into(), Some(json!(99)));
        let mut y = Row::new();
        y.insert("a".into(), Some(json!(2)));
        y.insert("b".into(), Some(json!(0)));
        assert_eq!(cmp(&x, &y), std::cmp::Ordering::Less);
    }

    // Branch: partition comparator — tie then second key.
    #[test]
    fn partition_cmp_second_key_breaks_tie() {
        let cmp = make_partition_key_comparator(&["a".to_string(), "b".to_string()]);
        let mut x = Row::new();
        x.insert("a".into(), Some(json!(1)));
        x.insert("b".into(), Some(json!(1)));
        let mut y = Row::new();
        y.insert("a".into(), Some(json!(1)));
        y.insert("b".into(), Some(json!(2)));
        assert_eq!(cmp(&x, &y), std::cmp::Ordering::Less);
    }

    // Branch: take_state round-trip with bound set.
    #[test]
    fn take_state_json_roundtrip_with_bound() {
        let s = TakeState {
            size: 3,
            bound: Some(row(7)),
        };
        let j = take_state_to_json(&s);
        let back = take_state_from_json(&j);
        assert_eq!(s, back);
    }

    // Branch: take_state round-trip with bound None.
    #[test]
    fn take_state_json_roundtrip_without_bound() {
        let s = TakeState {
            size: 0,
            bound: None,
        };
        let j = take_state_to_json(&s);
        let back = take_state_from_json(&j);
        assert_eq!(s, back);
    }

    // Branch: assert_ordering_includes_pk — incomplete ordering panics.
    #[test]
    #[should_panic(expected = "Ordering must include all primary key fields")]
    fn assert_ordering_panics_when_missing_pk() {
        let sort = vec![("other".to_string(), Direction::Asc)];
        assert_ordering_includes_pk(&sort, &["id".to_string()]);
    }

    // Branch: assert_ordering_includes_pk — complete ordering passes.
    #[test]
    fn assert_ordering_ok_when_complete() {
        let sort = vec![("id".to_string(), Direction::Asc)];
        assert_ordering_includes_pk(&sort, &["id".to_string()]);
    }

    // Branch: Take::new panics when ordering omits PK.
    #[test]
    #[should_panic(expected = "Ordering must include all primary key fields")]
    fn take_new_panics_when_ordering_missing_pk() {
        let sort: AstOrdering = vec![("other".to_string(), Direction::Asc)];
        let schema = SourceSchema {
            table_name: "t".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: StdArc::new(make_comparator(sort.clone(), false)),
            sort,
        };
        let src = FakeSource::new(schema, vec![]);
        let _ = Take::new(Box::new(src), Box::new(MemoryStorage::new()), 3, None);
    }

    // ─── Fetch (partition_bound path) ───────────────────────────────────

    // Branch: initial fetch hydrates state and emits up to `limit` rows.
    #[test]
    fn fetch_initial_hydrates_and_limits() {
        let (t, _e) = mk_take(make_schema(), vec![row(1), row(2), row(3), row(4)], 2, None);
        let out = drain(t.fetch(FetchRequest::default()));
        let rows: Vec<_> = out
            .into_iter()
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => Some(n.row),
                NodeOrYield::Yield => None,
            })
            .collect();
        assert_eq!(rows, vec![row(1), row(2)]);
        // State persisted.
        let key = get_take_state_key(None, None);
        let state = t.get_take_state(&key).unwrap();
        assert_eq!(state.size, 2);
        assert_eq!(state.bound, Some(row(2)));
        // maxBound also set.
        assert_eq!(t.get_max_bound(), Some(row(2)));
    }

    // Branch: limit == 0 short-circuits initial fetch.
    #[test]
    fn fetch_limit_zero_returns_empty() {
        let (t, _e) = mk_take(make_schema(), vec![row(1), row(2)], 0, None);
        let out = drain(t.fetch(FetchRequest::default()));
        assert!(out.is_empty());
    }

    // Branch: subsequent fetch with bound=None returns nothing.
    #[test]
    fn fetch_subsequent_with_empty_bound_returns_empty() {
        let (t, _e) = mk_take(make_schema(), vec![], 2, None);
        // First call hydrates (empty set, size=0, bound=None).
        let _ = drain(t.fetch(FetchRequest::default()));
        // Now add a row upstream — second fetch should still see bound=None.
        // Rebuild: we can't mutate the FakeSource from outside `Take`.
        // So simulate by constructing fresh input; but to test the branch,
        // hand-install a TakeState with bound=None:
        let key = get_take_state_key(None, None);
        t.storage_set(
            &key,
            take_state_to_json(&TakeState {
                size: 0,
                bound: None,
            }),
        );
        let out = drain(t.fetch(FetchRequest::default()));
        assert!(out.is_empty());
    }

    // Branch: subsequent fetch emits rows <= bound, stops at first > bound.
    #[test]
    fn fetch_subsequent_stops_at_bound() {
        let (t, _e) = mk_take(make_schema(), vec![row(1), row(2), row(3), row(4)], 2, None);
        // Hydrate: bound becomes 2.
        let _ = drain(t.fetch(FetchRequest::default()));
        // Now second fetch must only return rows <= bound (1,2).
        let rows: Vec<_> = drain(t.fetch(FetchRequest::default()))
            .into_iter()
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => Some(n.row),
                NodeOrYield::Yield => None,
            })
            .collect();
        assert_eq!(rows, vec![row(1), row(2)]);
    }

    // Branch: row_hidden_from_fetch suppresses exactly the overlay row.
    #[test]
    fn fetch_respects_row_hidden_overlay() {
        let (t, _e) = mk_take(make_schema(), vec![row(1), row(2), row(3)], 3, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        *t.row_hidden_from_fetch.lock().unwrap() = Some(row(2));
        let rows: Vec<_> = drain(t.fetch(FetchRequest::default()))
            .into_iter()
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => Some(n.row),
                NodeOrYield::Yield => None,
            })
            .collect();
        assert_eq!(rows, vec![row(1), row(3)]);
    }

    // Branch: initial fetch panics on start.
    #[test]
    #[should_panic(expected = "Start should be undefined")]
    fn fetch_initial_panics_on_start() {
        let (t, _e) = mk_take(make_schema(), vec![row(1)], 1, None);
        let _ = drain(t.fetch(FetchRequest {
            start: Some(Start {
                row: row(1),
                basis: StartBasis::At,
            }),
            ..FetchRequest::default()
        }));
    }

    // Branch: initial fetch panics on reverse = true.
    #[test]
    #[should_panic(expected = "Reverse should be false")]
    fn fetch_initial_panics_on_reverse() {
        let (t, _e) = mk_take(make_schema(), vec![row(1)], 1, None);
        let _ = drain(t.fetch(FetchRequest {
            reverse: Some(true),
            ..FetchRequest::default()
        }));
    }

    // Branch: initial fetch panics when constraint doesn't match partition.
    #[test]
    #[should_panic(expected = "Constraint should match partition key")]
    fn fetch_initial_panics_when_constraint_mismatches_partition() {
        let schema = make_schema_partitioned(&["id"], &["issue_id", "id"]);
        let src = FakeSource::new(schema, vec![]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(
            Box::new(src),
            storage,
            2,
            Some(vec!["issue_id".to_string()]),
        );
        t.set_output(Box::new(Recorder {
            events: StdArc::new(Mutex::new(vec![])),
        }));
        // Build a constraint on a *different* field (id rather than issue_id)
        // so the partition path is taken (partition key present and no
        // constraint branch → maxBound path), but we want the partition
        // branch to trigger assertion failure in initial_fetch. The only way
        // to enter initial_fetch is via the partition_bound path with
        // matching constraint but no take state. So rig with a partial
        // constraint whose shape doesn't match the partition key. Using
        // "issue_id" but with an extra column bypasses match and goes to
        // maxBound; to hit the `Constraint should match partition key`
        // panic we invoke initial_fetch directly.
        let mut c = Constraint::new();
        c.insert("issue_id".into(), Some(json!("p")));
        c.insert("extra".into(), Some(json!(1)));
        let _ = t.initial_fetch(FetchRequest {
            constraint: Some(c),
            ..FetchRequest::default()
        });
    }

    // ─── Fetch (maxbound scan path) ─────────────────────────────────────

    // Branch: maxBound absent → empty.
    #[test]
    fn fetch_maxbound_path_empty_when_no_max() {
        let schema = make_schema_partitioned(&["id"], &["issue_id", "id"]);
        let src = FakeSource::new(schema, vec![]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(
            Box::new(src),
            storage,
            1,
            Some(vec!["issue_id".to_string()]),
        );
        t.set_output(Box::new(Recorder {
            events: StdArc::new(Mutex::new(vec![])),
        }));
        // Fetch with no constraint → maxBound path (partition_key set).
        let out = drain(t.fetch(FetchRequest::default()));
        assert!(out.is_empty());
    }

    // Branch: maxBound path yields rows whose partition state has bound
    // covering them; rows above maxBound terminate the scan.
    #[test]
    fn fetch_maxbound_path_filters_by_per_partition_state() {
        let schema = make_schema_partitioned(&["id"], &["issue_id", "id"]);
        let rows = vec![
            row_part("a", 1),
            row_part("a", 2),
            row_part("b", 3),
            row_part("b", 4),
        ];
        let src = FakeSource::new(schema, rows);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(
            Box::new(src),
            storage,
            1,
            Some(vec!["issue_id".to_string()]),
        );
        t.set_output(Box::new(Recorder {
            events: StdArc::new(Mutex::new(vec![])),
        }));
        // Hydrate each partition independently via constraint-matched fetch.
        let mut c_a = Constraint::new();
        c_a.insert("issue_id".into(), Some(json!("a")));
        let _ = drain(t.fetch(FetchRequest {
            constraint: Some(c_a),
            ..FetchRequest::default()
        }));
        let mut c_b = Constraint::new();
        c_b.insert("issue_id".into(), Some(json!("b")));
        let _ = drain(t.fetch(FetchRequest {
            constraint: Some(c_b),
            ..FetchRequest::default()
        }));
        // Now maxBound scan (no constraint, partition_key set).
        let rows: Vec<_> = drain(t.fetch(FetchRequest::default()))
            .into_iter()
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => Some(n.row),
                NodeOrYield::Yield => None,
            })
            .collect();
        // Each partition's limit=1 bound is the first row of that
        // partition; maxBound is the greater of the two bounds.
        // Rows yielded: only those <= their own partition's bound.
        // Expected: row_part("a", 1), row_part("b", 3) — first each.
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&row_part("a", 1)));
        assert!(rows.contains(&row_part("b", 3)));
    }

    // ─── Push branches (unpartitioned) ──────────────────────────────────

    // Branch: push when no take state exists — drop.
    #[test]
    fn push_add_drops_when_no_take_state() {
        let (mut t, ev) = mk_take(make_schema(), vec![], 2, None);
        let _: Vec<_> = (t.push(
            Change::Add(AddChange { node: node(1) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        assert!(ev.lock().unwrap().is_empty());
    }

    // Branch: Add below limit forwards and bumps bound.
    #[test]
    fn push_add_below_limit_forwards_and_updates_bound() {
        let (mut t, ev) = mk_take(make_schema(), vec![row(1)], 3, None);
        // Hydrate.
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        let _: Vec<_> = (t.push(
            Change::Add(AddChange { node: node(5) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        assert_eq!(ev.lock().unwrap().len(), 1);
        assert_eq!(ev.lock().unwrap()[0].node().row, row(5));
        let key = get_take_state_key(None, None);
        let s = t.get_take_state(&key).unwrap();
        assert_eq!(s.size, 2);
        assert_eq!(s.bound, Some(row(5)));
    }

    // Branch: Add below limit forwards, bound does NOT change if smaller.
    #[test]
    fn push_add_below_limit_does_not_lower_bound() {
        // Upstream already has row(3), row(5); hydrate with limit=3 but
        // only 2 rows → bound = row(5), size=2.
        let (mut t, ev) = mk_take(make_schema(), vec![row(3), row(5)], 3, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        // Add row(4) — below current bound but still under limit.
        let _: Vec<_> = (t.push(
            Change::Add(AddChange { node: node(4) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        let key = get_take_state_key(None, None);
        let s = t.get_take_state(&key).unwrap();
        assert_eq!(s.size, 3);
        // Bound still 5 — row(4) < row(5).
        assert_eq!(s.bound, Some(row(5)));
    }

    // Branch: Add at limit with row >= bound is dropped.
    #[test]
    fn push_add_at_limit_above_bound_drops() {
        let (mut t, ev) = mk_take(make_schema(), vec![row(1), row(2)], 2, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        let _: Vec<_> = (t.push(
            Change::Add(AddChange { node: node(9) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        assert!(ev.lock().unwrap().is_empty());
    }

    // Branch: Add at limit, below bound, limit=1 → evict + add.
    #[test]
    fn push_add_at_limit_below_bound_limit_one() {
        // Upstream: [2]. Hydrate: size=1, bound=2.
        let src_rows = vec![row(2)];
        let schema = make_schema();
        let src = FakeSource::new(schema, src_rows);
        let shared_input_rows: Vec<Row>; // unused but keeps binding clear
        let _ = shared_input_rows;
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 1, None);
        let ev: StdArc<Mutex<Vec<Change>>> = StdArc::new(Mutex::new(vec![]));
        t.set_output(Box::new(Recorder {
            events: StdArc::clone(&ev),
        }));
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        let _: Vec<_> = (t.push(
            Change::Add(AddChange { node: node(1) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        // Must have emitted a Remove(node(2)) and Add(node(1)).
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 2);
        assert!(matches!(e[0], Change::Remove(_)));
        assert_eq!(e[0].node().row, row(2));
        assert!(matches!(e[1], Change::Add(_)));
        assert_eq!(e[1].node().row, row(1));
    }

    // Branch: Add at limit, below bound, limit>1 → evict old bound, new
    // bound is max(before_bound, change.node).
    #[test]
    fn push_add_at_limit_below_bound_limit_many() {
        // Upstream: [1, 3, 5]. hydrate limit=2 → size=2, bound=3.
        let (mut t, ev) = mk_take(make_schema(), vec![row(1), row(3), row(5)], 2, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        // Push Add(row(2)). 2 < 3, so evict bound=3. beforeBound(during
        // reverse at bound) is row(1) under 'at' reverse semantics (oldest
        // item at-or-before bound that's second in reverse iteration).
        let _: Vec<_> = (t.push(
            Change::Add(AddChange { node: node(2) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 2);
        assert!(matches!(e[0], Change::Remove(_)));
        assert_eq!(e[0].node().row, row(3));
        assert!(matches!(e[1], Change::Add(_)));
        assert_eq!(e[1].node().row, row(2));
        let key = get_take_state_key(None, None);
        let s = t.get_take_state(&key).unwrap();
        // new bound: max(beforeBound=1, change.node=2) = 2.
        assert_eq!(s.bound, Some(row(2)));
        assert_eq!(s.size, 2);
    }

    // Branch: Remove after bound drops.
    #[test]
    fn push_remove_after_bound_drops() {
        let (mut t, ev) = mk_take(make_schema(), vec![row(1), row(2)], 2, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        let _: Vec<_> = (t.push(
            Change::Remove(RemoveChange { node: node(9) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        assert!(ev.lock().unwrap().is_empty());
    }

    // Branch: Remove with bound=None (empty bucket) drops.
    #[test]
    fn push_remove_bound_none_drops() {
        let (mut t, ev) = mk_take(make_schema(), vec![], 2, None);
        // Hydrate empty: state = {size:0, bound:None}.
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        let _: Vec<_> = (t.push(
            Change::Remove(RemoveChange { node: node(1) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        assert!(ev.lock().unwrap().is_empty());
    }

    // Branch: Remove at bound with a replacement waiting upstream → Remove + Add.
    #[test]
    fn push_remove_at_bound_with_replacement() {
        // Upstream: [1, 2, 3]. hydrate limit=2 → bound=2. Upstream still has
        // row(3) available as pull-in candidate.
        let (mut t, ev) = mk_take(make_schema(), vec![row(1), row(2), row(3)], 2, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        let _: Vec<_> = (t.push(
            Change::Remove(RemoveChange { node: node(2) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 2);
        assert!(matches!(e[0], Change::Remove(_)));
        assert_eq!(e[0].node().row, row(2));
        assert!(matches!(e[1], Change::Add(_)));
        assert_eq!(e[1].node().row, row(3));
    }

    // Branch: Remove at bound, no replacement → size--.
    #[test]
    fn push_remove_at_bound_no_replacement() {
        // Upstream holds [1, 2]; limit=3 so initial hydrate yields both,
        // bound=row(2), size=2. Before the Remove push, the upstream
        // Source has already deleted row(2) (Take sees the push AFTER the
        // Source has applied it). We simulate by rebuilding the operator
        // with the post-remove upstream and hand-installed state.
        let schema = make_schema();
        let src = FakeSource::new(schema, vec![row(1)]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 3, None);
        let ev: StdArc<Mutex<Vec<Change>>> = StdArc::new(Mutex::new(vec![]));
        t.set_output(Box::new(Recorder {
            events: StdArc::clone(&ev),
        }));
        // Install pre-remove state: bound=row(2), size=2.
        let key = get_take_state_key(None, None);
        t.storage_set(
            &key,
            take_state_to_json(&TakeState {
                size: 2,
                bound: Some(row(2)),
            }),
        );
        t.storage_set(MAX_BOUND_KEY, row_to_json(&row(2)));
        let _: Vec<_> = (t.push(
            Change::Remove(RemoveChange { node: node(2) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], Change::Remove(_)));
        let s = t.get_take_state(&key).unwrap();
        assert_eq!(s.size, 1);
        // bound falls back to row(1).
        assert_eq!(s.bound, Some(row(1)));
    }

    // Branch: Child with row <= bound forwards.
    #[test]
    fn push_child_within_bound_forwards() {
        let (mut t, ev) = mk_take(make_schema(), vec![row(1), row(2)], 2, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        let child = Change::Child(ChildChange {
            node: node(1),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange { node: node(10) })),
            },
        });
        let _: Vec<_> = (t.push(child, &StubPusher as &dyn InputBase)).collect();
        assert_eq!(ev.lock().unwrap().len(), 1);
        assert!(matches!(ev.lock().unwrap()[0], Change::Child(_)));
    }

    // Branch: Child with row > bound is dropped.
    #[test]
    fn push_child_outside_bound_dropped() {
        let (mut t, ev) = mk_take(make_schema(), vec![row(1), row(2)], 2, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        let child = Change::Child(ChildChange {
            node: node(9),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange { node: node(10) })),
            },
        });
        let _: Vec<_> = (t.push(child, &StubPusher as &dyn InputBase)).collect();
        // Without a take_state for node(9), the push is dropped before
        // we even examine the bound.
        assert!(ev.lock().unwrap().is_empty());
    }

    // Branch: Edit old == new == bound → forward with no state change.
    #[test]
    fn push_edit_old_eq_new_eq_bound_forwards() {
        let (mut t, ev) = mk_take(make_schema(), vec![row(1), row(2)], 2, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        // old_node.row and node.row BOTH equal bound (row(2)) — e.g. a row
        // whose non-id field flipped but the sort-relevant id stayed.
        let edit = Change::Edit(EditChange {
            old_node: node(2),
            node: node(2),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], Change::Edit(_)));
    }

    // Branch: Edit old==bound, new<bound, limit==1 → replaceBound fast path.
    #[test]
    fn push_edit_shrinks_bound_limit_one() {
        let (mut t, ev) = mk_take(make_schema(), vec![row(5)], 1, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        // Swap upstream to reflect the edit.
        // NOTE: our FakeSource holds its own rows. For the edit path tests
        // we don't need upstream to actually contain the edit — the
        // algorithm only consults upstream for bound lookups around old
        // bound position, which is still row(5) from the fetch viewpoint.
        ev.lock().unwrap().clear();
        let edit = Change::Edit(EditChange {
            old_node: node(5),
            node: node(3),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], Change::Edit(_)));
        let key = get_take_state_key(None, None);
        let s = t.get_take_state(&key).unwrap();
        assert_eq!(s.bound, Some(row(3)));
    }

    // Branch: Edit old==bound, new<bound, limit>1 → look up before-bound;
    // new bound is the before-bound row; the edit is forwarded.
    #[test]
    fn push_edit_shrinks_bound_limit_many_picks_before_bound() {
        // State BEFORE the edit: upstream=[1, 5], hydrate limit=2 →
        // bound=row(5), size=2. Upstream then applies edit row(5)→row(3),
        // leaving [1, 3] before the push reaches Take.
        let schema = make_schema();
        // Post-edit upstream.
        let src = FakeSource::new(schema, vec![row(1), row(3)]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 2, None);
        let ev: StdArc<Mutex<Vec<Change>>> = StdArc::new(Mutex::new(vec![]));
        t.set_output(Box::new(Recorder {
            events: StdArc::clone(&ev),
        }));
        let key = get_take_state_key(None, None);
        // Hand-install pre-edit state.
        t.storage_set(
            &key,
            take_state_to_json(&TakeState {
                size: 2,
                bound: Some(row(5)),
            }),
        );
        t.storage_set(MAX_BOUND_KEY, row_to_json(&row(5)));
        let edit = Change::Edit(EditChange {
            old_node: node(5),
            node: node(3),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], Change::Edit(_)));
        let s = t.get_take_state(&key).unwrap();
        // beforeBound = largest row < old bound (row(5)) in post-edit
        // upstream [1, 3] → row(3).
        assert_eq!(s.bound, Some(row(3)));
    }

    // Branch: Edit old==bound, new>bound → evict old, add new-bound-row-after.
    #[test]
    fn push_edit_grows_past_bound_splits_to_remove_add() {
        // Upstream AFTER the edit: [1, 7] (row(5)→row(7)).
        let schema = make_schema();
        let src = FakeSource::new(schema, vec![row(1), row(7)]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 2, None);
        let ev: StdArc<Mutex<Vec<Change>>> = StdArc::new(Mutex::new(vec![]));
        t.set_output(Box::new(Recorder {
            events: StdArc::clone(&ev),
        }));
        // Hydrate: bound = row(7), size = 2.
        let _ = drain(t.fetch(FetchRequest::default()));
        // Pre-edit bound was row(5); reset state to simulate pre-edit view.
        let key = get_take_state_key(None, None);
        t.storage_set(
            &key,
            take_state_to_json(&TakeState {
                size: 2,
                bound: Some(row(5)),
            }),
        );
        t.storage_set(MAX_BOUND_KEY, row_to_json(&row(5)));
        ev.lock().unwrap().clear();
        // Edit row(5) → row(7). old == bound, new > bound.
        let edit = Change::Edit(EditChange {
            old_node: node(5),
            node: node(7),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        let e = ev.lock().unwrap();
        // First fetch after upstream=[1,7] returns row at/after bound=5
        // → row(7). Since new.row == newBoundNode.row, TS takes the
        // `replaceBoundAndForwardChange` path: one Edit.
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], Change::Edit(_)));
        let s = t.get_take_state(&key).unwrap();
        assert_eq!(s.bound, Some(row(7)));
    }

    // Branch: Edit old==bound, new>bound, but newBoundNode != change.node →
    // split into Remove(old) + Add(newBoundNode).
    #[test]
    fn push_edit_grows_past_bound_splits_when_not_same_as_new_bound() {
        // Upstream AFTER the edit: [1, 6, 9]. Pre-edit bound was row(5),
        // row(5) becomes row(9). newBoundNode = row(6), which differs from
        // change.node row(9) → split.
        let schema = make_schema();
        let src = FakeSource::new(schema, vec![row(1), row(6), row(9)]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 2, None);
        let ev: StdArc<Mutex<Vec<Change>>> = StdArc::new(Mutex::new(vec![]));
        t.set_output(Box::new(Recorder {
            events: StdArc::clone(&ev),
        }));
        // Pre-edit view: hand-build state bound=5, size=2.
        let key = get_take_state_key(None, None);
        t.storage_set(
            &key,
            take_state_to_json(&TakeState {
                size: 2,
                bound: Some(row(5)),
            }),
        );
        t.storage_set(MAX_BOUND_KEY, row_to_json(&row(5)));
        ev.lock().unwrap().clear();
        let edit = Change::Edit(EditChange {
            old_node: node(5),
            node: node(9),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        let e = ev.lock().unwrap();
        // Expected: Remove(node(5)), Add(node(6)).
        assert_eq!(e.len(), 2);
        assert!(matches!(e[0], Change::Remove(_)));
        assert_eq!(e[0].node().row, row(5));
        assert!(matches!(e[1], Change::Add(_)));
        assert_eq!(e[1].node().row, row(6));
    }

    // Branch: Edit old>bound, new>bound → drop (both outside).
    #[test]
    fn push_edit_both_outside_drops() {
        let (mut t, ev) = mk_take(make_schema(), vec![row(1), row(2)], 2, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        let edit = Change::Edit(EditChange {
            old_node: node(8),
            node: node(9),
        });
        // No take_state for node(8) since no hydration touched that row;
        // push drops early.
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        assert!(ev.lock().unwrap().is_empty());
    }

    // Branch: Edit old>bound, new<bound → push out current bound, add new.
    #[test]
    fn push_edit_outside_to_inside_splits() {
        // Upstream AFTER the edit: [1, 2, 3] (row(8) becomes row(2)).
        // Pre-edit upstream (conceptually): [1, 3, 8]. We hydrate from
        // post-edit; then reset state to match pre-edit invariants.
        let schema = make_schema();
        let src = FakeSource::new(schema, vec![row(1), row(2), row(3)]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 2, None);
        let ev: StdArc<Mutex<Vec<Change>>> = StdArc::new(Mutex::new(vec![]));
        t.set_output(Box::new(Recorder {
            events: StdArc::clone(&ev),
        }));
        // State: bound=3, size=2 (pre-edit: rows [1, 3] kept, row(8) outside).
        let key = get_take_state_key(None, None);
        t.storage_set(
            &key,
            take_state_to_json(&TakeState {
                size: 2,
                bound: Some(row(3)),
            }),
        );
        t.storage_set(MAX_BOUND_KEY, row_to_json(&row(3)));
        ev.lock().unwrap().clear();
        // Edit row(8)→row(2): old outside, new inside. But `node(8)`
        // is looked up via get_state_and_constraint with partition key
        // None → unpartitioned key is shared → take_state exists.
        let edit = Change::Edit(EditChange {
            old_node: node(8),
            node: node(2),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        let e = ev.lock().unwrap();
        // Expected: Remove(oldBoundNode=row(3)), Add(change.node=row(2)).
        // oldBoundNode is the current bound row (first in reverse fetch
        // at bound=3); newBoundNode is the row just before it — row(2)
        // (the new row itself, post-edit).
        assert_eq!(e.len(), 2);
        assert!(matches!(e[0], Change::Remove(_)));
        assert_eq!(e[0].node().row, row(3));
        assert!(matches!(e[1], Change::Add(_)));
        assert_eq!(e[1].node().row, row(2));
        let s = t.get_take_state(&key).unwrap();
        // new bound = newBoundNode = row(2).
        assert_eq!(s.bound, Some(row(2)));
    }

    // Branch: Edit old<bound, new<bound → forward edit, no state change.
    #[test]
    fn push_edit_both_inside_forwards() {
        let (mut t, ev) = mk_take(make_schema(), vec![row(1), row(2), row(3)], 3, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        ev.lock().unwrap().clear();
        // old=row(1) and new=row(2) both < bound=row(3). Distinct ids so
        // the TS assertions `oldCmp !== 0`, `newCmp !== 0` hold.
        let edit = Change::Edit(EditChange {
            old_node: node(1),
            node: node(2),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], Change::Edit(_)));
    }

    // Branch: Edit old<bound, new>bound, afterBoundNode != change.node →
    // split remove(old) + add(afterBound).
    #[test]
    fn push_edit_inside_to_outside_splits() {
        // Upstream post-edit: [2, 3, 4, 9]. Pre-edit bound was row(3),
        // size=2. Edit row(1) → row(9). afterBound (row at basis=after
        // bound=3) = row(4). afterBound != change.node (row(9)) → split.
        let schema = make_schema();
        let src = FakeSource::new(schema, vec![row(2), row(3), row(4), row(9)]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 2, None);
        let ev: StdArc<Mutex<Vec<Change>>> = StdArc::new(Mutex::new(vec![]));
        t.set_output(Box::new(Recorder {
            events: StdArc::clone(&ev),
        }));
        let key = get_take_state_key(None, None);
        t.storage_set(
            &key,
            take_state_to_json(&TakeState {
                size: 2,
                bound: Some(row(3)),
            }),
        );
        t.storage_set(MAX_BOUND_KEY, row_to_json(&row(3)));
        ev.lock().unwrap().clear();
        let edit = Change::Edit(EditChange {
            old_node: node(1),
            node: node(9),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 2);
        assert!(matches!(e[0], Change::Remove(_)));
        assert_eq!(e[0].node().row, row(1));
        assert!(matches!(e[1], Change::Add(_)));
        assert_eq!(e[1].node().row, row(4));
        let s = t.get_take_state(&key).unwrap();
        assert_eq!(s.bound, Some(row(4)));
    }

    // Branch: Edit old<bound, new>bound, afterBoundNode == change.node →
    // replaceBoundAndForwardChange fast path.
    #[test]
    fn push_edit_inside_to_outside_replace_fast_path() {
        // Upstream post-edit: [2, 3, 4]. Pre-edit bound was row(3), size=2.
        // Edit row(1) → row(4). afterBound at bound=3 = row(4). new row
        // == afterBound → replaceBound.
        let schema = make_schema();
        let src = FakeSource::new(schema, vec![row(2), row(3), row(4)]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 2, None);
        let ev: StdArc<Mutex<Vec<Change>>> = StdArc::new(Mutex::new(vec![]));
        t.set_output(Box::new(Recorder {
            events: StdArc::clone(&ev),
        }));
        let key = get_take_state_key(None, None);
        t.storage_set(
            &key,
            take_state_to_json(&TakeState {
                size: 2,
                bound: Some(row(3)),
            }),
        );
        t.storage_set(MAX_BOUND_KEY, row_to_json(&row(3)));
        ev.lock().unwrap().clear();
        let edit = Change::Edit(EditChange {
            old_node: node(1),
            node: node(4),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
        let e = ev.lock().unwrap();
        assert_eq!(e.len(), 1);
        assert!(matches!(e[0], Change::Edit(_)));
        let s = t.get_take_state(&key).unwrap();
        assert_eq!(s.bound, Some(row(4)));
    }

    // Branch: Edit with partition-key divergence panics.
    #[test]
    #[should_panic(expected = "Unexpected change of partition key")]
    fn push_edit_cross_partition_panics() {
        let schema = make_schema_partitioned(&["id"], &["issue_id", "id"]);
        let src = FakeSource::new(schema, vec![row_part("a", 1)]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(
            Box::new(src),
            storage,
            2,
            Some(vec!["issue_id".to_string()]),
        );
        t.set_output(Box::new(Recorder {
            events: StdArc::new(Mutex::new(vec![])),
        }));
        // Hydrate partition "a".
        let mut c = Constraint::new();
        c.insert("issue_id".into(), Some(json!("a")));
        let _ = drain(t.fetch(FetchRequest {
            constraint: Some(c),
            ..FetchRequest::default()
        }));
        // Edit moves partition from "a" → "b".
        let edit = Change::Edit(EditChange {
            old_node: node_of(row_part("a", 1)),
            node: node_of(row_part("b", 1)),
        });
        let _: Vec<_> = (t.push(edit, &StubPusher as &dyn InputBase)).collect();
    }

    // ─── Integration: operator plumbing ─────────────────────────────────

    // Branch: set_output replaces ThrowOutput (observable because push no
    // longer panics when take state exists).
    #[test]
    fn set_output_replaces_default() {
        let (mut t, _ev) = mk_take(make_schema(), vec![row(1)], 3, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        // Already set by mk_take. Confirm no panic on push.
        let _: Vec<_> = (t.push(
            Change::Add(AddChange { node: node(2) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
    }

    // Branch: set_output never replaced → push panics through ThrowOutput.
    #[test]
    #[should_panic(expected = "Output not set")]
    fn default_output_panics_on_push() {
        let src = FakeSource::new(make_schema(), vec![row(1)]);
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 3, None);
        let _ = drain(t.fetch(FetchRequest::default()));
        let _: Vec<_> = (t.push(
            Change::Add(AddChange { node: node(2) }),
            &StubPusher as &dyn InputBase,
        ))
        .collect();
    }

    // Branch: get_schema delegates to upstream.
    #[test]
    fn get_schema_delegates() {
        let (t, _) = mk_take(make_schema(), vec![], 1, None);
        assert_eq!(t.get_schema().table_name, "t");
    }

    // Branch: destroy delegates to upstream.
    #[test]
    fn destroy_delegates_to_upstream() {
        let destroyed = StdArc::new(AtomicBool::new(false));
        let schema = make_schema();
        let src = FakeSource {
            schema,
            rows: Mutex::new(vec![]),
            destroyed: StdArc::clone(&destroyed),
        };
        let storage = Box::new(MemoryStorage::new());
        let mut t = Take::new(Box::new(src), storage, 1, None);
        t.destroy();
        assert!(destroyed.load(AtOrdering::SeqCst));
    }

    // ─── Pusher stub for push tests ─────────────────────────────────────

    struct StubPusher;
    impl InputBase for StubPusher {
        fn get_schema(&self) -> &SourceSchema {
            panic!("StubPusher::get_schema should not be called");
        }
        fn destroy(&mut self) {}
    }
}
