//! Port of `packages/zql/src/ivm/memory-source.ts`.
//!
//! Public exports ported:
//!
//! - [`Overlay`] — per-push, per-source overlay value (TS `Overlay`).
//! - [`Overlays`] — the two-slot `{add, remove}` projection used by the
//!   overlay generators (TS `Overlays`).
//! - [`MemorySource`] — in-memory implementation of
//!   [`crate::ivm::source::Source`]. This type is test-only in production
//!   (the real source is `TableSource`, ported separately in Layer 8), but
//!   it's the reference implementation every other source is measured
//!   against.
//! - [`generate_with_start`] — stream helper that honours `Start` bounds
//!   (TS `generateWithStart`).
//! - [`generate_with_overlay`] — splices overlay values into a row stream
//!   (TS `generateWithOverlay`).
//! - [`generate_with_overlay_inner`] — the core merge routine
//!   (TS `generateWithOverlayInner`).
//! - [`overlays_for_start_at`] — clips overlays that fall before a start
//!   row (TS `overlaysForStartAt`, exported under `overlaysForStartAtForTest`).
//! - [`overlays_for_constraint`] — drops overlays that don't match a
//!   constraint (TS `overlaysForConstraint`, exported under
//!   `overlaysForConstraintForTest`).
//! - [`stringify`] — bigint-aware JSON stringifier used for assert messages
//!   (TS `stringify`).
//!
//! ## Design notes (Rust-specific)
//!
//! - Data is stored as `BTreeMap<Vec<NormalizedValue>, Row>` keyed by the
//!   ordering's field values. Using a BTreeMap rather than TS's custom
//!   `BTreeSet<Row>` avoids re-implementing a B+Tree and gives us log-time
//!   lookup plus a native range scan.
//! - `MemorySource` holds its mutable fields inside `Mutex<...>` on a
//!   single `MemorySourceState`. Every trait method on `Source` takes
//!   `&self`, so interior mutability is required. Lock granularity is the
//!   whole state — matching TS's single-threaded model, where an entire
//!   `push` completes before another can start.
//! - `Connection` holds each downstream's `Output` in an
//!   `Arc<Mutex<Box<dyn Output>>>` so the connection handle can be cloned
//!   freely between the `MemorySource` (for push fan-out) and the
//!   downstream `SourceInput` (for `set_output`).
//! - We deliberately do NOT sprinkle `Yield` sentinels into fetch streams.
//!   TS emits them between rows for cooperative scheduling; Rust's
//!   consumers drain eagerly. This matches the existing ported operators
//!   (`filter.rs`, `skip.rs`, etc.) which already collect eagerly.
//! - `DebugDelegate` from TS is stashed as `Option<&dyn DebugDelegate>`
//!   at connection time but otherwise unused — there are no diagnostic
//!   hooks in the ported operator set yet.

use std::cmp::Ordering as CmpOrdering;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use zero_cache_types::ast::{Condition, Direction, Ordering};
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::{Row, Value};

use crate::ivm::change::{AddChange, Change, EditChange};
use crate::ivm::constraint::{
    Constraint, constraint_matches_primary_key, constraint_matches_row,
    primary_key_constraint_from_filters,
};
use crate::ivm::data::{Comparator, Node, compare_values, make_comparator, values_equal};
use crate::ivm::operator::{FetchRequest, InputBase, Output, Start, StartBasis};
use crate::ivm::schema::{SchemaValue, SourceSchema};
use crate::ivm::source::{
    DebugDelegate, GenPushStep, Source, SourceChange, SourceChangeAdd, SourceChangeRemove,
    SourceInput, TableSchema, Yield,
};
use crate::ivm::stream::Stream;

// ─── Overlay types ────────────────────────────────────────────────────

/// TS `Overlay = { epoch: number; change: SourceChange }`.
#[derive(Debug, Clone)]
pub struct Overlay {
    pub epoch: u64,
    pub change: SourceChange,
}

/// TS `Overlays = { add: Row | undefined; remove: Row | undefined }`.
#[derive(Debug, Clone, Default)]
pub struct Overlays {
    pub add: Option<Row>,
    pub remove: Option<Row>,
}

// ─── Bound / RowBound / BoundComparator ──────────────────────────────

/// TS `Bound = Value | MinValue | MaxValue`.
#[derive(Debug, Clone)]
pub enum Bound {
    Value(Value),
    Min,
    Max,
}

impl Bound {
    fn cmp_to(&self, other: &Bound) -> CmpOrdering {
        match (self, other) {
            (Bound::Min, Bound::Min) | (Bound::Max, Bound::Max) => CmpOrdering::Equal,
            (Bound::Min, _) => CmpOrdering::Less,
            (_, Bound::Min) => CmpOrdering::Greater,
            (Bound::Max, _) => CmpOrdering::Greater,
            (_, Bound::Max) => CmpOrdering::Less,
            (Bound::Value(a), Bound::Value(b)) => compare_values(a, b),
        }
    }
}

/// TS `RowBound = Record<string, Bound>`.
pub type RowBound = IndexMap<String, Bound>;

/// Convert a [`Row`] into a [`RowBound`] for scan-start computations.
fn row_to_row_bound(row: &Row) -> RowBound {
    row.iter()
        .map(|(k, v)| (k.clone(), Bound::Value(v.clone())))
        .collect()
}

/// Build a comparator over [`RowBound`]s according to an [`Ordering`].
/// Mirrors TS `makeBoundComparator` — per-field Asc/Desc handling over
/// [`Bound`] with Min/Max sentinels.
fn make_bound_comparator(sort: &Ordering) -> impl Fn(&RowBound, &RowBound) -> CmpOrdering + Clone {
    let sort = sort.clone();
    move |a: &RowBound, b: &RowBound| {
        for (key, dir) in sort.iter() {
            let av = a.get(key).cloned().unwrap_or(Bound::Value(None));
            let bv = b.get(key).cloned().unwrap_or(Bound::Value(None));
            let cmp = av.cmp_to(&bv);
            if cmp != CmpOrdering::Equal {
                return match dir {
                    Direction::Asc => cmp,
                    Direction::Desc => cmp.reverse(),
                };
            }
        }
        CmpOrdering::Equal
    }
}

// ─── Index ────────────────────────────────────────────────────────────

/// A single sort-order index over the stored rows. Matches TS `Index`,
/// minus `usedBy` — we don't GC indexes (TS stopped too; see the TS
/// comment in `#disconnect`).
struct Index {
    sort: Ordering,
    /// Rows kept sorted by a [`Comparator`]. Using a `Vec` keeps the port
    /// simple; insert/remove are O(n) but the test workloads are small.
    /// Production `TableSource` uses SQLite so this cost doesn't apply.
    rows: Vec<Row>,
    comparator: Arc<Comparator>,
}

impl Index {
    fn new(sort: Ordering) -> Self {
        let comparator = Arc::new(make_comparator(sort.clone(), false));
        Self {
            sort,
            rows: Vec::new(),
            comparator,
        }
    }

    fn insert(&mut self, row: Row) -> bool {
        let cmp = Arc::clone(&self.comparator);
        let pos = self.rows.binary_search_by(|existing| cmp(existing, &row));
        match pos {
            Ok(_) => false, // already present under this comparator's equivalence
            Err(idx) => {
                self.rows.insert(idx, row);
                true
            }
        }
    }

    fn remove(&mut self, row: &Row) -> bool {
        let cmp = Arc::clone(&self.comparator);
        match self.rows.binary_search_by(|existing| cmp(existing, row)) {
            Ok(idx) => {
                self.rows.remove(idx);
                true
            }
            Err(_) => false,
        }
    }

    fn contains(&self, row: &Row) -> bool {
        let cmp = Arc::clone(&self.comparator);
        self.rows
            .binary_search_by(|existing| cmp(existing, row))
            .is_ok()
    }

    fn key(&self) -> String {
        serde_json::to_string(&self.sort).expect("Ordering is JSON-serialisable")
    }
}

// ─── Connection ───────────────────────────────────────────────────────

/// A per-downstream connection record. Held both by the source (for push
/// fan-out) and by the returned [`SourceInput`] handle.
struct Connection {
    /// Connection id — stable identity used for disconnect.
    id: u64,
    sort: Ordering,
    /// Optional split-edit keys: if any of these differ between `row` and
    /// `oldRow` on an Edit, the source splits it into Remove + Add for
    /// this connection.
    split_edit_keys: Option<std::collections::HashSet<String>>,
    /// Optional filter predicate derived from the `filters` condition.
    filter_predicate: Option<Arc<dyn Fn(&Row) -> bool + Send + Sync>>,
    /// Raw filter condition, used by `primary_key_constraint_from_filters`.
    filter_condition: Option<Condition>,
    /// Output sink. `Arc<Mutex<_>>` so both the source (push) and the
    /// `SourceInput` handle (`set_output`) can mutate it.
    output: Arc<Mutex<Option<Box<dyn Output>>>>,
    /// Last push epoch this connection observed. See TS `lastPushedEpoch`.
    last_pushed_epoch: u64,
    /// Cached schema for this connection — computed once at connect time.
    #[allow(dead_code)]
    schema: SourceSchema,
}

// ─── MemorySourceState ────────────────────────────────────────────────

struct MemorySourceState {
    table_name: String,
    columns: IndexMap<String, SchemaValue>,
    primary_key: PrimaryKey,
    primary_index_sort: Ordering,
    indexes: IndexMap<String, Index>,
    connections: Vec<Connection>,
    overlay: Option<Overlay>,
    push_epoch: u64,
    next_connection_id: u64,
}

impl MemorySourceState {
    fn new(
        table_name: String,
        columns: IndexMap<String, SchemaValue>,
        primary_key: PrimaryKey,
    ) -> Self {
        let primary_index_sort: Ordering = primary_key
            .columns()
            .iter()
            .map(|c| (c.clone(), Direction::Asc))
            .collect();
        let mut indexes = IndexMap::new();
        let idx = Index::new(primary_index_sort.clone());
        indexes.insert(idx.key(), idx);
        Self {
            table_name,
            columns,
            primary_key,
            primary_index_sort,
            indexes,
            connections: Vec::new(),
            overlay: None,
            push_epoch: 0,
            next_connection_id: 0,
        }
    }

    fn primary_index(&self) -> &Index {
        let key =
            serde_json::to_string(&self.primary_index_sort).expect("Ordering is JSON-serialisable");
        self.indexes
            .get(&key)
            .expect("primary index must exist by invariant")
    }

    fn primary_index_mut(&mut self) -> &mut Index {
        let key =
            serde_json::to_string(&self.primary_index_sort).expect("Ordering is JSON-serialisable");
        self.indexes
            .get_mut(&key)
            .expect("primary index must exist by invariant")
    }

    /// TS `#getOrCreateIndex(sort)`. Creates a new index if one for this
    /// sort doesn't exist, populated from the primary index.
    fn get_or_create_index(&mut self, sort: &Ordering) -> String {
        let key = serde_json::to_string(sort).expect("Ordering is JSON-serialisable");
        if self.indexes.contains_key(&key) {
            return key;
        }
        let mut idx = Index::new(sort.clone());
        let primary_rows = self.primary_index().rows.clone();
        for row in primary_rows {
            idx.insert(row);
        }
        self.indexes.insert(key.clone(), idx);
        key
    }

    /// TS `#writeChange(change)`. Applies the change to every index.
    fn write_change(&mut self, change: &SourceChange) {
        for idx in self.indexes.values_mut() {
            match change {
                SourceChange::Add(c) => {
                    let added = idx.insert(c.row.clone());
                    assert!(
                        added,
                        "MemorySource: add must succeed since row existence was already checked"
                    );
                }
                SourceChange::Remove(c) => {
                    let removed = idx.remove(&c.row);
                    assert!(
                        removed,
                        "MemorySource: remove must succeed since row existence was already checked"
                    );
                }
                SourceChange::Edit(c) => {
                    let removed = idx.remove(&c.old_row);
                    assert!(
                        removed,
                        "MemorySource: edit remove must succeed since row existence was already checked"
                    );
                    idx.insert(c.row.clone());
                }
            }
        }
    }

    /// Does the primary index contain this row?
    fn exists(&self, row: &Row) -> bool {
        self.primary_index().contains(row)
    }
}

// ─── Public API ───────────────────────────────────────────────────────

/// TS `MemorySource` — in-memory [`Source`] backed by sorted row lists.
pub struct MemorySource {
    state: Arc<Mutex<MemorySourceState>>,
    table_schema: TableSchema,
}

impl MemorySource {
    /// TS `new MemorySource(tableName, columns, primaryKey, primaryIndexData?)`.
    ///
    /// The optional `primary_index_rows` parameter mirrors TS's
    /// `primaryIndexData` for [`MemorySource::fork`] — it pre-populates
    /// the primary index.
    pub fn new(
        table_name: impl Into<String>,
        columns: IndexMap<String, SchemaValue>,
        primary_key: PrimaryKey,
        primary_index_rows: Option<Vec<Row>>,
    ) -> Self {
        let name = table_name.into();
        let mut state = MemorySourceState::new(name.clone(), columns.clone(), primary_key.clone());
        if let Some(rows) = primary_index_rows {
            for row in rows {
                state.primary_index_mut().insert(row);
            }
        }
        let table_schema = TableSchema {
            name,
            server_name: None,
            columns: columns
                .into_iter()
                .map(|(k, v)| (k, v))
                .collect::<IndexMap<String, JsonValue>>(),
            primary_key,
        };
        Self {
            state: Arc::new(Mutex::new(state)),
            table_schema,
        }
    }

    /// TS `get data(): BTreeSet<Row>`. Returns a snapshot of the primary
    /// index rows in sort order.
    pub fn data(&self) -> Vec<Row> {
        self.state.lock().unwrap().primary_index().rows.clone()
    }

    /// TS `fork()`. Returns a new [`MemorySource`] seeded with a clone of
    /// the primary index data. Shares NO state with the original.
    pub fn fork(&self) -> Self {
        let state = self.state.lock().unwrap();
        Self::new(
            state.table_name.clone(),
            state.columns.clone(),
            state.primary_key.clone(),
            Some(state.primary_index().rows.clone()),
        )
    }

    /// TS `getIndexKeys()`. Exposed for tests that verify index lifecycle.
    pub fn get_index_keys(&self) -> Vec<String> {
        self.state.lock().unwrap().indexes.keys().cloned().collect()
    }

    /// Number of rows currently in the primary index. Handy for tests.
    pub fn len(&self) -> usize {
        self.state.lock().unwrap().primary_index().rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// TS `connect(sort, filters?, splitEditKeys?, debug?)` — Rust
    /// adaptation. Returns a [`Box<dyn SourceInput>`] wired back to the
    /// source via a stable connection id.
    ///
    /// TS asserts the ordering includes every PK column. We mirror that
    /// as a panic, matching every other TS `assert` we've ported.
    pub fn connect(
        &self,
        sort: Ordering,
        filters: Option<Condition>,
        split_edit_keys: Option<std::collections::HashSet<String>>,
        _debug: Option<&dyn DebugDelegate>,
    ) -> Box<dyn SourceInput> {
        assert_ordering_includes_pk(&sort, &self.state.lock().unwrap().primary_key);

        let mut state = self.state.lock().unwrap();
        let id = state.next_connection_id;
        state.next_connection_id += 1;

        // Compile a predicate via `builder/filter` — mirrors TS
        // `source.connect` which passes `filters` to `createPredicate`
        // inside the source. We first strip correlated-subquery
        // branches via `transform_filters`: the compiled predicate
        // only sees pure expressions.
        let predicate: Option<Arc<dyn Fn(&Row) -> bool + Send + Sync>> = match &filters {
            Some(f) => {
                let (stripped, _removed) = crate::builder::filter::transform_filters(Some(f));
                stripped.map(|c| {
                    let boxed = crate::builder::filter::create_predicate(&c);
                    let arc: Arc<dyn Fn(&Row) -> bool + Send + Sync> = Arc::from(boxed);
                    arc
                })
            }
            None => None,
        };

        let sort_for_schema = sort.clone();
        let compare_rows = Arc::new(make_comparator(sort_for_schema.clone(), false));
        let schema = SourceSchema {
            table_name: state.table_name.clone(),
            columns: state.columns.clone(),
            primary_key: state.primary_key.clone(),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: zero_cache_types::ast::System::Client,
            compare_rows,
            sort: sort_for_schema,
        };

        let output = Arc::new(Mutex::new(None));
        let conn = Connection {
            id,
            sort,
            split_edit_keys,
            filter_predicate: predicate,
            filter_condition: filters,
            output: Arc::clone(&output),
            last_pushed_epoch: 0,
            schema: schema.clone(),
        };
        state.connections.push(conn);
        drop(state);

        Box::new(MemorySourceInput {
            id,
            source: Arc::clone(&self.state),
            schema,
            output,
            fully_applied_filters: true,
        })
    }

    #[allow(dead_code)]
    fn disconnect(&self, id: u64) {
        let mut state = self.state.lock().unwrap();
        let idx = state
            .connections
            .iter()
            .position(|c| c.id == id)
            .expect("Connection not found");
        state.connections.remove(idx);
    }

    /// TS `push(change)`. Fan-out to every connection's `Output::push`.
    /// Returns an empty `Stream<'_, Yield>` since Rust consumers drain
    /// eagerly.
    pub fn push(&self, change: SourceChange) -> Stream<'_, Yield> {
        let _ = self.gen_push(change).count();
        Box::new(std::iter::empty())
    }

    /// TS `genPush(change)`. Yields one [`GenPushStep::Step`] per
    /// connection processed. No `Yield` sentinels (see module docs).
    pub fn gen_push(&self, change: SourceChange) -> Stream<'_, GenPushStep> {
        self.apply_push_with_split_edit(change);
        // We've already applied the change; report one Step per connection.
        let n_connections = self.state.lock().unwrap().connections.len();
        Box::new((0..n_connections).map(|_| GenPushStep::Step))
    }

    /// Apply push, splitting Edit → Remove+Add when any connection's
    /// splitEditKeys say so. Mirrors TS `genPushAndWriteWithSplitEdit`.
    fn apply_push_with_split_edit(&self, change: SourceChange) {
        let should_split = if let SourceChange::Edit(e) = &change {
            let state = self.state.lock().unwrap();
            let mut split = false;
            'outer: for conn in &state.connections {
                if let Some(keys) = &conn.split_edit_keys {
                    for key in keys.iter() {
                        let a = e.row.get(key).cloned().unwrap_or(None);
                        let b = e.old_row.get(key).cloned().unwrap_or(None);
                        if !values_equal(&a, &b) {
                            split = true;
                            break 'outer;
                        }
                    }
                }
            }
            split
        } else {
            false
        };

        match (change, should_split) {
            (SourceChange::Edit(e), true) => {
                let old = e.old_row;
                let new_row = e.row;
                self.apply_push(SourceChange::Remove(SourceChangeRemove {
                    row: old.clone(),
                }));
                self.apply_push(SourceChange::Add(SourceChangeAdd { row: new_row }));
            }
            (change, _) => self.apply_push(change),
        }
    }

    /// Mirrors TS `genPushAndWrite`: run the push fan-out, then commit
    /// the change to all indexes.
    fn apply_push(&self, change: SourceChange) {
        // 1. Assertions on presence / absence.
        {
            let state = self.state.lock().unwrap();
            match &change {
                SourceChange::Add(c) => {
                    assert!(
                        !state.exists(&c.row),
                        "Row already exists {}",
                        stringify(&change)
                    );
                }
                SourceChange::Remove(c) => {
                    assert!(state.exists(&c.row), "Row not found {}", stringify(&change));
                }
                SourceChange::Edit(c) => {
                    assert!(
                        state.exists(&c.old_row),
                        "Row not found {}",
                        stringify(&change)
                    );
                }
            }
        }

        // 2. Compute next epoch.
        let epoch = {
            let mut state = self.state.lock().unwrap();
            state.push_epoch += 1;
            state.push_epoch
        };

        // 3. Fan out to each connection. Snapshot (id, output, predicate)
        // under the state lock, then drop it so downstream `Output::push`
        // can re-enter `fetch` (which also locks state).
        let fanout: Vec<(
            u64,
            Arc<Mutex<Option<Box<dyn Output>>>>,
            Option<Arc<dyn Fn(&Row) -> bool + Send + Sync>>,
        )> = {
            let state = self.state.lock().unwrap();
            state
                .connections
                .iter()
                .map(|c| {
                    (
                        c.id,
                        Arc::clone(&c.output),
                        c.filter_predicate.as_ref().map(Arc::clone),
                    )
                })
                .collect()
        };

        for (id, output_slot, predicate) in fanout {
            // Update lastPushedEpoch and set overlay before forwarding.
            {
                let mut state = self.state.lock().unwrap();
                if let Some(conn) = state.connections.iter_mut().find(|c| c.id == id) {
                    conn.last_pushed_epoch = epoch;
                }
                state.overlay = Some(Overlay {
                    epoch,
                    change: change.clone(),
                });
            }

            // Build the downstream Change value.
            let downstream_change = match &change {
                SourceChange::Add(c) => Change::Add(AddChange {
                    node: Node {
                        row: c.row.clone(),
                        relationships: IndexMap::new(),
                    },
                }),
                SourceChange::Remove(c) => Change::Remove(crate::ivm::change::RemoveChange {
                    node: Node {
                        row: c.row.clone(),
                        relationships: IndexMap::new(),
                    },
                }),
                SourceChange::Edit(c) => Change::Edit(EditChange {
                    old_node: Node {
                        row: c.old_row.clone(),
                        relationships: IndexMap::new(),
                    },
                    node: Node {
                        row: c.row.clone(),
                        relationships: IndexMap::new(),
                    },
                }),
            };

            // Pusher identity — we use a transient input handle that
            // simply carries the connection's id for downstream's benefit.
            let pusher = PusherHandle { id };

            let mut output_guard = output_slot.lock().unwrap();
            if let Some(output) = output_guard.as_mut() {
                // Apply per-connection predicate via filter_push semantics.
                if let Some(pred) = &predicate {
                    let pred_ref: &(dyn Fn(&Row) -> bool + Send + Sync) = pred.as_ref();
                    let stream = crate::ivm::filter_push::filter_push(
                        downstream_change,
                        output.as_mut(),
                        &pusher,
                        Some(pred_ref),
                    );
                    // Drain eagerly to match TS `yield*` semantics.
                    let _ = stream.count();
                } else {
                    let _ = output.push(downstream_change, &pusher).count();
                }
            }
        }

        // 4. Clear overlay.
        {
            let mut state = self.state.lock().unwrap();
            state.overlay = None;
        }

        // 5. Commit change to every index.
        {
            let mut state = self.state.lock().unwrap();
            state.write_change(&change);
        }
    }

    /// TS `#fetch(req, conn)`.
    fn fetch_impl(&self, req: FetchRequest, connection_id: u64) -> Vec<Node> {
        let mut state = self.state.lock().unwrap();

        // Snapshot per-connection data. The connection may be destroyed
        // during nested fetches — handle gracefully.
        let Some(conn_idx) = state.connections.iter().position(|c| c.id == connection_id) else {
            return Vec::new();
        };
        let sort = state.connections[conn_idx].sort.clone();
        let filter_condition = state.connections[conn_idx].filter_condition.clone();
        let filter_predicate = state.connections[conn_idx].filter_predicate.clone();
        let last_pushed_epoch = state.connections[conn_idx].last_pushed_epoch;
        let primary_key = state.primary_key.clone();
        let overlay = state.overlay.clone();

        let reverse = req.reverse.unwrap_or(false);
        let connection_cmp: Arc<Comparator> = Arc::new(make_comparator(sort.clone(), false));
        let connection_cmp_for_scan = Arc::clone(&connection_cmp);
        let connection_comparator = move |a: &Row, b: &Row| {
            let c = connection_cmp_for_scan(a, b);
            if reverse { c.reverse() } else { c }
        };

        // Determine index sort.
        let pk_constraint =
            primary_key_constraint_from_filters(filter_condition.as_ref(), &primary_key);
        let fetch_or_pk_constraint = pk_constraint.clone().or_else(|| req.constraint.clone());

        let mut index_sort: Ordering = Vec::new();
        if let Some(fc) = &fetch_or_pk_constraint {
            for key in fc.keys() {
                index_sort.push((key.clone(), Direction::Asc));
            }
        }
        if primary_key.columns().len() > 1
            || fetch_or_pk_constraint.is_none()
            || !constraint_matches_primary_key(
                fetch_or_pk_constraint.as_ref().unwrap(),
                &primary_key,
            )
        {
            index_sort.extend(sort.iter().cloned());
        }

        let index_key = state.get_or_create_index(&index_sort);
        let index_rows: Vec<Row> = state
            .indexes
            .get(&index_key)
            .expect("just created")
            .rows
            .clone();
        let index_sort_clone = index_sort.clone();
        let index_cmp_bound = make_bound_comparator(&index_sort_clone);
        let index_cmp_for_rows: Arc<Comparator> =
            Arc::new(make_comparator(index_sort_clone.clone(), false));
        let reverse_for_index = reverse;
        let index_comparator = {
            let cmp = Arc::clone(&index_cmp_for_rows);
            move |a: &Row, b: &Row| {
                let c = cmp(a, b);
                if reverse_for_index { c.reverse() } else { c }
            }
        };

        let start_at_row = req.start.as_ref().map(|s| s.row.clone());

        // Build scanStart.
        let scan_start: Option<RowBound> = if let Some(fc) = &fetch_or_pk_constraint {
            let mut rb = RowBound::new();
            for (key, dir) in index_sort.iter() {
                if fc.contains_key(key) {
                    rb.insert(key.clone(), Bound::Value(fc.get(key).unwrap().clone()));
                } else {
                    let bound = if reverse {
                        if *dir == Direction::Asc {
                            Bound::Max
                        } else {
                            Bound::Min
                        }
                    } else if *dir == Direction::Asc {
                        Bound::Min
                    } else {
                        Bound::Max
                    };
                    rb.insert(key.clone(), bound);
                }
            }
            Some(rb)
        } else {
            start_at_row.as_ref().map(row_to_row_bound)
        };

        // Drop state lock so downstream output can re-enter.
        drop(state);

        // Generate rows filtered by scan_start, in requested direction.
        let rows_iter: Vec<Row> = if reverse {
            generate_rows_reverse(&index_rows, scan_start.as_ref(), &index_cmp_bound)
        } else {
            generate_rows_forward(&index_rows, scan_start.as_ref(), &index_cmp_bound)
        };

        // If we used a pk_constraint, wrap rows in `once` semantics so
        // overlay inner cannot revisit. In Rust iterators are already
        // single-pass; the Vec-based pipeline naturally mirrors this.

        let with_overlay: Vec<Node> = generate_with_overlay_vec(
            start_at_row.clone(),
            rows_iter,
            req.constraint.as_ref(),
            overlay.as_ref(),
            last_pushed_epoch,
            &index_comparator,
            filter_predicate.as_ref().map(|p| p.as_ref()),
        );

        // Apply start bounds using the connection comparator.
        let with_start: Vec<Node> =
            apply_start_bounds(with_overlay, req.start.as_ref(), &connection_comparator);

        // Apply constraint as a cut-off (TS `generateWithConstraint`
        // breaks out of the stream on first non-match).
        let with_constraint: Vec<Node> =
            apply_constraint_cutoff(with_start, req.constraint.as_ref());

        // Apply filter predicate as a filter.
        if let Some(pred) = filter_predicate {
            with_constraint
                .into_iter()
                .filter(|n| pred(&n.row))
                .collect()
        } else {
            with_constraint
        }
    }
}

impl Source for MemorySource {
    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn connect(
        &self,
        sort: Ordering,
        filters: Option<&Condition>,
        split_edit_keys: Option<&std::collections::HashSet<String>>,
        debug: Option<&dyn DebugDelegate>,
    ) -> Box<dyn SourceInput> {
        MemorySource::connect(
            self,
            sort,
            filters.cloned(),
            split_edit_keys.cloned(),
            debug,
        )
    }

    fn push<'a>(&'a self, change: SourceChange) -> Stream<'a, Yield> {
        MemorySource::push(self, change)
    }

    fn gen_push<'a>(&'a self, change: SourceChange) -> Stream<'a, GenPushStep> {
        MemorySource::gen_push(self, change)
    }
}

// ─── PusherHandle ─────────────────────────────────────────────────────

/// Minimal [`InputBase`] used as the `pusher` identity during fan-out.
/// Carries the connection id so downstream multi-input operators can
/// disambiguate by source.
struct PusherHandle {
    #[allow(dead_code)]
    id: u64,
}

impl InputBase for PusherHandle {
    fn get_schema(&self) -> &SourceSchema {
        panic!("PusherHandle::get_schema should not be called — it's only an identity marker")
    }
    fn destroy(&mut self) {}
}

// ─── MemorySourceInput (Input handle returned by connect) ─────────────

struct MemorySourceInput {
    id: u64,
    source: Arc<Mutex<MemorySourceState>>,
    schema: SourceSchema,
    output: Arc<Mutex<Option<Box<dyn Output>>>>,
    fully_applied_filters: bool,
}

impl InputBase for MemorySourceInput {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn destroy(&mut self) {
        // Mirror `MemorySource::disconnect`.
        let mut state = self.source.lock().unwrap();
        if let Some(pos) = state.connections.iter().position(|c| c.id == self.id) {
            state.connections.remove(pos);
        }
    }
}

impl crate::ivm::operator::Input for MemorySourceInput {
    fn set_output(&mut self, output: Box<dyn Output>) {
        *self.output.lock().unwrap() = Some(output);
    }

    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, crate::ivm::data::NodeOrYield> {
        // MemorySource requires re-entrant access to its state. We can't
        // call `fetch_impl` from here because it needs `&MemorySource`,
        // not just the state `Arc`. Reuse a compact local version.
        let nodes = fetch_via_state(&self.source, req, self.id);
        Box::new(nodes.into_iter().map(crate::ivm::data::NodeOrYield::Node))
    }
}

impl SourceInput for MemorySourceInput {
    fn fully_applied_filters(&self) -> bool {
        self.fully_applied_filters
    }
}

/// Helper usable from `MemorySourceInput::fetch` which only has an
/// `Arc<Mutex<MemorySourceState>>`, not a [`MemorySource`].
fn fetch_via_state(
    state_arc: &Arc<Mutex<MemorySourceState>>,
    req: FetchRequest,
    connection_id: u64,
) -> Vec<Node> {
    // Build a transient façade so we can call MemorySource::fetch_impl.
    let source = MemorySource {
        state: Arc::clone(state_arc),
        table_schema: TableSchema {
            name: String::new(),
            server_name: None,
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["__placeholder__".into()]),
        },
    };
    source.fetch_impl(req, connection_id)
}

// ─── Row scanning ─────────────────────────────────────────────────────

fn generate_rows_forward(
    rows: &[Row],
    scan_start: Option<&RowBound>,
    index_cmp_bound: &impl Fn(&RowBound, &RowBound) -> CmpOrdering,
) -> Vec<Row> {
    match scan_start {
        None => rows.to_vec(),
        Some(start) => {
            // Find first row with cmp >= 0.
            let pos = rows
                .iter()
                .position(|r| index_cmp_bound(&row_to_row_bound(r), start) != CmpOrdering::Less);
            match pos {
                Some(i) => rows[i..].to_vec(),
                None => Vec::new(),
            }
        }
    }
}

fn generate_rows_reverse(
    rows: &[Row],
    scan_start: Option<&RowBound>,
    index_cmp_bound: &impl Fn(&RowBound, &RowBound) -> CmpOrdering,
) -> Vec<Row> {
    match scan_start {
        None => rows.iter().rev().cloned().collect(),
        Some(start) => {
            // Reverse scan: find last row with cmp <= 0, iterate backwards.
            let pos = rows.iter().rposition(|r| {
                index_cmp_bound(&row_to_row_bound(r), start) != CmpOrdering::Greater
            });
            match pos {
                Some(i) => rows[..=i].iter().rev().cloned().collect(),
                None => Vec::new(),
            }
        }
    }
}

// ─── Overlay / start / constraint helpers ─────────────────────────────

/// TS `generateWithStart`.
///
/// Yields the input nodes, skipping items until the `start.basis` bound
/// is satisfied relative to `compare`.
pub fn generate_with_start<'a>(
    nodes: impl IntoIterator<Item = Node> + 'a,
    start: Option<Start>,
    compare: impl Fn(&Row, &Row) -> CmpOrdering + 'a,
) -> impl Iterator<Item = Node> + 'a {
    let nodes: Vec<Node> = nodes.into_iter().collect();
    let mut started = start.is_none();
    let start_row = start.as_ref().map(|s| s.row.clone());
    let basis = start.as_ref().map(|s| s.basis);
    nodes.into_iter().filter_map(move |node| {
        if started {
            return Some(node);
        }
        let start_row = start_row.as_ref().expect("started implies start present");
        let cmp = compare(&node.row, start_row);
        let ok = match basis.expect("basis set when start set") {
            StartBasis::At => cmp != CmpOrdering::Less,
            StartBasis::After => cmp == CmpOrdering::Greater,
        };
        if ok {
            started = true;
            Some(node)
        } else {
            None
        }
    })
}

fn apply_start_bounds(
    nodes: Vec<Node>,
    start: Option<&Start>,
    compare: &impl Fn(&Row, &Row) -> CmpOrdering,
) -> Vec<Node> {
    let Some(start) = start else {
        return nodes;
    };
    let mut started = false;
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        if !started {
            let cmp = compare(&node.row, &start.row);
            let ok = match start.basis {
                StartBasis::At => cmp != CmpOrdering::Less,
                StartBasis::After => cmp == CmpOrdering::Greater,
            };
            if !ok {
                continue;
            }
            started = true;
        }
        out.push(node);
    }
    out
}

fn apply_constraint_cutoff(nodes: Vec<Node>, constraint: Option<&Constraint>) -> Vec<Node> {
    let Some(c) = constraint else {
        return nodes;
    };
    let mut out = Vec::new();
    for node in nodes {
        if !constraint_matches_row(c, &node.row) {
            break;
        }
        out.push(node);
    }
    out
}

/// TS `overlaysForStartAt` (exported as `overlaysForStartAtForTest`).
pub fn overlays_for_start_at(
    overlays: Overlays,
    start_at: &Row,
    compare: &impl Fn(&Row, &Row) -> CmpOrdering,
) -> Overlays {
    let clip = |row: Option<Row>| -> Option<Row> {
        match row {
            None => None,
            Some(r) => {
                if compare(&r, start_at) == CmpOrdering::Less {
                    None
                } else {
                    Some(r)
                }
            }
        }
    };
    Overlays {
        add: clip(overlays.add),
        remove: clip(overlays.remove),
    }
}

/// TS `overlaysForConstraint` (exported as `overlaysForConstraintForTest`).
pub fn overlays_for_constraint(overlays: Overlays, constraint: &Constraint) -> Overlays {
    let clip = |row: Option<Row>| -> Option<Row> {
        match row {
            None => None,
            Some(r) => {
                if constraint_matches_row(constraint, &r) {
                    Some(r)
                } else {
                    None
                }
            }
        }
    };
    Overlays {
        add: clip(overlays.add),
        remove: clip(overlays.remove),
    }
}

/// TS `overlaysForFilterPredicate`.
fn overlays_for_filter_predicate(
    overlays: Overlays,
    predicate: &(dyn Fn(&Row) -> bool + Send + Sync),
) -> Overlays {
    let clip = |row: Option<Row>| -> Option<Row> {
        match row {
            None => None,
            Some(r) => {
                if predicate(&r) {
                    Some(r)
                } else {
                    None
                }
            }
        }
    };
    Overlays {
        add: clip(overlays.add),
        remove: clip(overlays.remove),
    }
}

/// TS `computeOverlays`.
fn compute_overlays(
    start_at: Option<&Row>,
    constraint: Option<&Constraint>,
    overlay: Option<&Overlay>,
    compare: &impl Fn(&Row, &Row) -> CmpOrdering,
    filter_predicate: Option<&(dyn Fn(&Row) -> bool + Send + Sync)>,
) -> Overlays {
    let mut overlays = Overlays::default();
    if let Some(ov) = overlay {
        match &ov.change {
            SourceChange::Add(c) => {
                overlays.add = Some(c.row.clone());
            }
            SourceChange::Remove(c) => {
                overlays.remove = Some(c.row.clone());
            }
            SourceChange::Edit(c) => {
                overlays.add = Some(c.row.clone());
                overlays.remove = Some(c.old_row.clone());
            }
        }
    }
    if let Some(sa) = start_at {
        overlays = overlays_for_start_at(overlays, sa, compare);
    }
    if let Some(c) = constraint {
        overlays = overlays_for_constraint(overlays, c);
    }
    if let Some(pred) = filter_predicate {
        overlays = overlays_for_filter_predicate(overlays, pred);
    }
    overlays
}

/// TS `generateWithOverlayInner`.
///
/// Merges `overlays.add` into the row stream at its sort position and
/// drops `overlays.remove`. Mirrors TS exactly, including the trailing
/// "emit add if never yielded" clause.
pub fn generate_with_overlay_inner(
    rows: impl IntoIterator<Item = Row>,
    overlays: Overlays,
    compare: &impl Fn(&Row, &Row) -> CmpOrdering,
) -> Vec<Node> {
    let mut add_yielded = false;
    let mut remove_skipped = false;
    let mut out = Vec::new();
    for row in rows {
        if !add_yielded {
            if let Some(add_row) = overlays.add.as_ref() {
                if compare(add_row, &row) == CmpOrdering::Less {
                    add_yielded = true;
                    out.push(Node {
                        row: add_row.clone(),
                        relationships: IndexMap::new(),
                    });
                }
            }
        }
        if !remove_skipped {
            if let Some(rem_row) = overlays.remove.as_ref() {
                if compare(rem_row, &row) == CmpOrdering::Equal {
                    remove_skipped = true;
                    continue;
                }
            }
        }
        out.push(Node {
            row,
            relationships: IndexMap::new(),
        });
    }
    if !add_yielded {
        if let Some(add_row) = overlays.add {
            out.push(Node {
                row: add_row,
                relationships: IndexMap::new(),
            });
        }
    }
    out
}

/// TS `generateWithOverlay`.
pub fn generate_with_overlay(
    start_at: Option<Row>,
    rows: impl IntoIterator<Item = Row>,
    constraint: Option<&Constraint>,
    overlay: Option<&Overlay>,
    last_pushed_epoch: u64,
    compare: &impl Fn(&Row, &Row) -> CmpOrdering,
    filter_predicate: Option<&(dyn Fn(&Row) -> bool + Send + Sync)>,
) -> Vec<Node> {
    let overlay_to_apply = match overlay {
        Some(ov) if last_pushed_epoch >= ov.epoch => Some(ov),
        _ => None,
    };
    let overlays = compute_overlays(
        start_at.as_ref(),
        constraint,
        overlay_to_apply,
        compare,
        filter_predicate,
    );
    generate_with_overlay_inner(rows, overlays, compare)
}

fn generate_with_overlay_vec(
    start_at: Option<Row>,
    rows: Vec<Row>,
    constraint: Option<&Constraint>,
    overlay: Option<&Overlay>,
    last_pushed_epoch: u64,
    compare: &impl Fn(&Row, &Row) -> CmpOrdering,
    filter_predicate: Option<&(dyn Fn(&Row) -> bool + Send + Sync)>,
) -> Vec<Node> {
    generate_with_overlay(
        start_at,
        rows,
        constraint,
        overlay,
        last_pushed_epoch,
        compare,
        filter_predicate,
    )
}

// ─── stringify ────────────────────────────────────────────────────────

/// TS `stringify(change)` — JSON stringifier. TS replaces bigints via a
/// reviver; our JSON values contain no bigint variants, so we just
/// serialize directly.
pub fn stringify(change: &SourceChange) -> String {
    match change {
        SourceChange::Add(c) => {
            format!(
                "{{\"type\":\"add\",\"row\":{}}}",
                serde_json::to_string(&c.row).unwrap_or_else(|_| "null".into())
            )
        }
        SourceChange::Remove(c) => {
            format!(
                "{{\"type\":\"remove\",\"row\":{}}}",
                serde_json::to_string(&c.row).unwrap_or_else(|_| "null".into())
            )
        }
        SourceChange::Edit(c) => {
            format!(
                "{{\"type\":\"edit\",\"row\":{},\"oldRow\":{}}}",
                serde_json::to_string(&c.row).unwrap_or_else(|_| "null".into()),
                serde_json::to_string(&c.old_row).unwrap_or_else(|_| "null".into())
            )
        }
    }
}

// ─── assertOrderingIncludesPK ─────────────────────────────────────────

fn assert_ordering_includes_pk(sort: &Ordering, pk: &PrimaryKey) {
    let ordering_fields: Vec<&str> = sort.iter().map(|(f, _)| f.as_str()).collect();
    let missing: Vec<&str> = pk
        .columns()
        .iter()
        .filter(|c| !ordering_fields.contains(&c.as_str()))
        .map(String::as_str)
        .collect();
    assert!(
        missing.is_empty(),
        "Ordering must include all primary key fields. Missing: {}.",
        missing.join(", ")
    );
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch-complete coverage for:
    //!   - [`MemorySource::new`] — default primary index created.
    //!   - [`MemorySource::new`] — with primary_index_rows pre-populated.
    //!   - [`MemorySource::connect`] — index NOT yet created for composite sort.
    //!   - [`MemorySource::connect`] — sort missing PK column panics.
    //!   - [`MemorySource::fork`] — clone preserves rows, independent state.
    //!   - [`MemorySource::push`] — add into empty table.
    //!   - [`MemorySource::push`] — add then add duplicate panics.
    //!   - [`MemorySource::push`] — remove non-existent panics.
    //!   - [`MemorySource::push`] — edit non-existent panics.
    //!   - [`MemorySource::push`] — edit present row updates indexes.
    //!   - [`MemorySource::push`] — edit with splitEditKeys → Remove+Add.
    //!   - [`MemorySource::push`] — fan-out invokes each connection's Output.
    //!   - [`MemorySource::push`] — filter predicate drops Add.
    //!   - `fetch` — empty table → empty.
    //!   - `fetch` — single row, no constraint, no start, no reverse.
    //!   - `fetch` — forward vs reverse.
    //!   - `fetch` — constraint filters.
    //!   - `fetch` — constraint on PK promotes to point-lookup index.
    //!   - `fetch` — start At bound inclusive.
    //!   - `fetch` — start After bound exclusive.
    //!   - `fetch` — overlay add during nested fetch (middle placement).
    //!   - `generate_with_overlay_inner` — the four TS tabular cases.
    //!   - `overlays_for_start_at` — add before / at / after bound.
    //!   - `overlays_for_constraint` — matching / non-matching.
    //!   - [`generate_with_start`] — basis At, basis After, no start.
    //!   - [`stringify`] — each SourceChange variant.
    //!   - [`Bound`] ordering — Min / Max / equal / Value comparisons.
    //!   - `get_index_keys` — reflects created indexes.
    //!   - `disconnect` via `Input::destroy` removes connection.

    use super::*;
    use crate::ivm::change::Change as IvmChange;
    use crate::ivm::data::NodeOrYield;
    use crate::ivm::operator::{Input, Output};
    use crate::ivm::source::{SourceChangeAdd, SourceChangeEdit, SourceChangeRemove};
    use indexmap::IndexMap;
    use serde_json::json;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering as AtOrdering};

    fn pk(cols: &[&str]) -> PrimaryKey {
        PrimaryKey::new(cols.iter().map(|s| (*s).to_string()).collect())
    }

    fn cols(list: &[&str]) -> IndexMap<String, SchemaValue> {
        let mut m = IndexMap::new();
        for k in list {
            m.insert((*k).to_string(), json!({"type":"string"}));
        }
        m
    }

    fn row_with(pairs: &[(&str, serde_json::Value)]) -> Row {
        let mut r = Row::new();
        for (k, v) in pairs {
            r.insert((*k).to_string(), Some(v.clone()));
        }
        r
    }

    fn sort_asc(fields: &[&str]) -> Ordering {
        fields
            .iter()
            .map(|f| ((*f).to_string(), Direction::Asc))
            .collect()
    }

    // ─── Bound ordering ────────────────────────────────────────────────

    // Branch: Min vs Min = Equal; Max vs Max = Equal.
    #[test]
    fn bound_min_min_and_max_max_equal() {
        assert_eq!(Bound::Min.cmp_to(&Bound::Min), CmpOrdering::Equal);
        assert_eq!(Bound::Max.cmp_to(&Bound::Max), CmpOrdering::Equal);
    }

    // Branch: Min < anything; anything > Min.
    #[test]
    fn bound_min_less_than_others() {
        assert_eq!(
            Bound::Min.cmp_to(&Bound::Value(Some(json!(0)))),
            CmpOrdering::Less
        );
        assert_eq!(
            Bound::Value(Some(json!(0))).cmp_to(&Bound::Min),
            CmpOrdering::Greater
        );
        assert_eq!(Bound::Min.cmp_to(&Bound::Max), CmpOrdering::Less);
    }

    // Branch: Max > anything; anything < Max.
    #[test]
    fn bound_max_greater_than_others() {
        assert_eq!(
            Bound::Max.cmp_to(&Bound::Value(Some(json!(0)))),
            CmpOrdering::Greater
        );
        assert_eq!(
            Bound::Value(Some(json!(0))).cmp_to(&Bound::Max),
            CmpOrdering::Less
        );
    }

    // Branch: Value vs Value delegates to compare_values.
    #[test]
    fn bound_value_vs_value_uses_compare_values() {
        assert_eq!(
            Bound::Value(Some(json!(1))).cmp_to(&Bound::Value(Some(json!(2)))),
            CmpOrdering::Less
        );
    }

    // ─── MemorySource::new ─────────────────────────────────────────────

    // Branch: new creates primary index only.
    #[test]
    fn new_creates_primary_index_only() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        assert_eq!(ms.get_index_keys().len(), 1);
        assert!(ms.is_empty());
    }

    // Branch: new with pre-populated rows.
    #[test]
    fn new_with_seed_rows() {
        let rows = vec![row_with(&[("a", json!("x"))])];
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), Some(rows));
        assert_eq!(ms.len(), 1);
    }

    // ─── connect ──────────────────────────────────────────────────────

    // Branch: connect with PK-only sort — no new index created.
    #[test]
    fn connect_pk_only_sort_no_new_index() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        let _input = ms.connect(sort_asc(&["a"]), None, None, None);
        // Primary index already exists; no extra index until fetch.
        assert_eq!(ms.get_index_keys().len(), 1);
    }

    // Branch: connect missing PK column → panic in assert_ordering_includes_pk.
    #[test]
    #[should_panic(expected = "must include all primary key fields")]
    fn connect_panics_when_sort_missing_pk() {
        let ms = MemorySource::new("t", cols(&["a", "b"]), pk(&["a"]), None);
        let _ = ms.connect(sort_asc(&["b"]), None, None, None);
    }

    // Branch: destroying a connection removes it.
    #[test]
    fn destroy_removes_connection() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        let mut input = ms.connect(sort_asc(&["a"]), None, None, None);
        assert_eq!(ms.state.lock().unwrap().connections.len(), 1);
        input.destroy();
        assert_eq!(ms.state.lock().unwrap().connections.len(), 0);
    }

    // ─── fork ──────────────────────────────────────────────────────────

    // Branch: fork copies rows, independent state.
    #[test]
    fn fork_is_independent() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("x"))]),
        }))
        .count();
        let forked = ms.fork();
        assert_eq!(forked.len(), 1);

        forked
            .push(SourceChange::Add(SourceChangeAdd {
                row: row_with(&[("a", json!("y"))]),
            }))
            .count();
        assert_eq!(forked.len(), 2);
        assert_eq!(ms.len(), 1); // original unchanged
    }

    // ─── push (add/remove/edit) with asserts ──────────────────────────

    // Branch: add into empty table.
    #[test]
    fn push_add_into_empty() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("x"))]),
        }))
        .count();
        assert_eq!(ms.len(), 1);
    }

    // Branch: duplicate add panics.
    #[test]
    #[should_panic(expected = "Row already exists")]
    fn push_add_duplicate_panics() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        let row = row_with(&[("a", json!("x"))]);
        ms.push(SourceChange::Add(SourceChangeAdd { row: row.clone() }))
            .count();
        ms.push(SourceChange::Add(SourceChangeAdd { row })).count();
    }

    // Branch: remove non-existent panics.
    #[test]
    #[should_panic(expected = "Row not found")]
    fn push_remove_nonexistent_panics() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        ms.push(SourceChange::Remove(SourceChangeRemove {
            row: row_with(&[("a", json!("x"))]),
        }))
        .count();
    }

    // Branch: edit non-existent panics.
    #[test]
    #[should_panic(expected = "Row not found")]
    fn push_edit_nonexistent_panics() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        ms.push(SourceChange::Edit(SourceChangeEdit {
            row: row_with(&[("a", json!("x"))]),
            old_row: row_with(&[("a", json!("x"))]),
        }))
        .count();
    }

    // Branch: edit updates indexes (old removed, new added).
    #[test]
    fn push_edit_updates_row() {
        let ms = MemorySource::new("t", cols(&["a", "b"]), pk(&["a"]), None);
        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("k")), ("b", json!("1"))]),
        }))
        .count();
        ms.push(SourceChange::Edit(SourceChangeEdit {
            old_row: row_with(&[("a", json!("k")), ("b", json!("1"))]),
            row: row_with(&[("a", json!("k")), ("b", json!("2"))]),
        }))
        .count();
        let data = ms.data();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].get("b"), Some(&Some(json!("2"))));
    }

    // ─── push fan-out ──────────────────────────────────────────────────

    struct CountingOutput {
        pushes: Arc<Mutex<Vec<IvmChange>>>,
    }
    impl Output for CountingOutput {
        fn push<'a>(&'a mut self, change: IvmChange, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes.lock().unwrap().push(change);
            Box::new(std::iter::empty())
        }
    }

    // Branch: push fan-out delivers the Add change to a connected Output.
    #[test]
    fn push_fanout_delivers_add() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        let mut input = ms.connect(sort_asc(&["a"]), None, None, None);
        let pushes = Arc::new(Mutex::new(Vec::new()));
        input.set_output(Box::new(CountingOutput {
            pushes: Arc::clone(&pushes),
        }));

        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("x"))]),
        }))
        .count();

        let p = pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert!(matches!(p[0], IvmChange::Add(_)));
    }

    // Branch: push with splitEditKeys splits Edit into Remove+Add for
    // that connection.
    #[test]
    fn push_edit_split_on_split_edit_keys() {
        let ms = MemorySource::new("t", cols(&["a", "b"]), pk(&["a"]), None);
        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("k")), ("b", json!("1"))]),
        }))
        .count();

        let mut split_keys = HashSet::new();
        split_keys.insert("b".to_string());
        let mut input = ms.connect(sort_asc(&["a"]), None, Some(split_keys), None);
        let pushes = Arc::new(Mutex::new(Vec::new()));
        input.set_output(Box::new(CountingOutput {
            pushes: Arc::clone(&pushes),
        }));

        // b changes → split.
        ms.push(SourceChange::Edit(SourceChangeEdit {
            old_row: row_with(&[("a", json!("k")), ("b", json!("1"))]),
            row: row_with(&[("a", json!("k")), ("b", json!("2"))]),
        }))
        .count();

        let p = pushes.lock().unwrap();
        assert_eq!(p.len(), 2, "Edit must split into Remove + Add");
        assert!(matches!(p[0], IvmChange::Remove(_)));
        assert!(matches!(p[1], IvmChange::Add(_)));
    }

    // Branch: edit with splitEditKeys but keys UNCHANGED → no split.
    #[test]
    fn push_edit_no_split_when_keys_unchanged() {
        let ms = MemorySource::new("t", cols(&["a", "b", "c"]), pk(&["a"]), None);
        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("k")), ("b", json!("1")), ("c", json!("x"))]),
        }))
        .count();

        let mut split_keys = HashSet::new();
        split_keys.insert("b".to_string());
        let mut input = ms.connect(sort_asc(&["a"]), None, Some(split_keys), None);
        let pushes = Arc::new(Mutex::new(Vec::new()));
        input.set_output(Box::new(CountingOutput {
            pushes: Arc::clone(&pushes),
        }));

        // b unchanged; c changes.
        ms.push(SourceChange::Edit(SourceChangeEdit {
            old_row: row_with(&[("a", json!("k")), ("b", json!("1")), ("c", json!("x"))]),
            row: row_with(&[("a", json!("k")), ("b", json!("1")), ("c", json!("y"))]),
        }))
        .count();

        let p = pushes.lock().unwrap();
        assert_eq!(
            p.len(),
            1,
            "Edit must NOT split when tracked keys unchanged"
        );
        assert!(matches!(p[0], IvmChange::Edit(_)));
    }

    // ─── fetch ─────────────────────────────────────────────────────────

    fn drain(input: &dyn Input, req: FetchRequest) -> Vec<Row> {
        input
            .fetch(req)
            .filter_map(|n| match n {
                NodeOrYield::Node(node) => Some(node.row),
                NodeOrYield::Yield => None,
            })
            .collect()
    }

    // Branch: fetch on empty table.
    #[test]
    fn fetch_empty_table() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        let input = ms.connect(sort_asc(&["a"]), None, None, None);
        let rows = drain(&*input, FetchRequest::default());
        assert!(rows.is_empty());
    }

    // Branch: fetch single row, forward.
    #[test]
    fn fetch_single_row_forward() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("x"))]),
        }))
        .count();
        let input = ms.connect(sort_asc(&["a"]), None, None, None);
        let rows = drain(&*input, FetchRequest::default());
        assert_eq!(rows.len(), 1);
    }

    // Branch: fetch reverse returns rows in reverse order.
    #[test]
    fn fetch_reverse_order() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        for v in ["a", "b", "c"] {
            ms.push(SourceChange::Add(SourceChangeAdd {
                row: row_with(&[("a", json!(v))]),
            }))
            .count();
        }
        let input = ms.connect(sort_asc(&["a"]), None, None, None);
        let rows = drain(
            &*input,
            FetchRequest {
                reverse: Some(true),
                ..FetchRequest::default()
            },
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("a"), Some(&Some(json!("c"))));
        assert_eq!(rows[2].get("a"), Some(&Some(json!("a"))));
    }

    // Branch: fetch with constraint filters to matching rows.
    #[test]
    fn fetch_with_constraint_cuts_off() {
        let ms = MemorySource::new("t", cols(&["a", "b"]), pk(&["a"]), None);
        for (a, b) in [("x", "1"), ("y", "1"), ("z", "2")] {
            ms.push(SourceChange::Add(SourceChangeAdd {
                row: row_with(&[("a", json!(a)), ("b", json!(b))]),
            }))
            .count();
        }
        let input = ms.connect(sort_asc(&["a"]), None, None, None);

        let mut constraint = Constraint::new();
        constraint.insert("b".into(), Some(json!("1")));
        let rows = drain(
            &*input,
            FetchRequest {
                constraint: Some(constraint),
                ..FetchRequest::default()
            },
        );
        assert_eq!(rows.len(), 2);
    }

    // Branch: fetch Start At is inclusive.
    #[test]
    fn fetch_start_at_is_inclusive() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        for v in ["a", "b", "c"] {
            ms.push(SourceChange::Add(SourceChangeAdd {
                row: row_with(&[("a", json!(v))]),
            }))
            .count();
        }
        let input = ms.connect(sort_asc(&["a"]), None, None, None);
        let rows = drain(
            &*input,
            FetchRequest {
                start: Some(Start {
                    row: row_with(&[("a", json!("b"))]),
                    basis: StartBasis::At,
                }),
                ..FetchRequest::default()
            },
        );
        assert_eq!(rows.len(), 2); // b, c
        assert_eq!(rows[0].get("a"), Some(&Some(json!("b"))));
    }

    // Branch: fetch Start After is exclusive.
    #[test]
    fn fetch_start_after_is_exclusive() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        for v in ["a", "b", "c"] {
            ms.push(SourceChange::Add(SourceChangeAdd {
                row: row_with(&[("a", json!(v))]),
            }))
            .count();
        }
        let input = ms.connect(sort_asc(&["a"]), None, None, None);
        let rows = drain(
            &*input,
            FetchRequest {
                start: Some(Start {
                    row: row_with(&[("a", json!("b"))]),
                    basis: StartBasis::After,
                }),
                ..FetchRequest::default()
            },
        );
        assert_eq!(rows.len(), 1); // c only
        assert_eq!(rows[0].get("a"), Some(&Some(json!("c"))));
    }

    // ─── get_index_keys tracks index creation ─────────────────────────

    // Branch: fetch with multi-column sort creates secondary index.
    #[test]
    fn fetch_creates_secondary_index_on_composite_sort() {
        let ms = MemorySource::new("t", cols(&["a", "b"]), pk(&["a"]), None);
        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("x")), ("b", json!("1"))]),
        }))
        .count();

        let before = ms.get_index_keys().len();
        let input = ms.connect(sort_asc(&["a", "b"]), None, None, None);
        let _ = drain(&*input, FetchRequest::default());
        let after = ms.get_index_keys().len();
        assert!(after > before, "fetch must create a new index");
    }

    // ─── overlays_for_start_at ─────────────────────────────────────────

    fn num_compare(a: &Row, b: &Row) -> CmpOrdering {
        let av = a.get("id").cloned().flatten();
        let bv = b.get("id").cloned().flatten();
        compare_values(&av, &bv)
    }

    // Branch: add row is before start → clipped.
    #[test]
    fn overlays_for_start_at_add_before_clipped() {
        let ov = Overlays {
            add: Some(row_with(&[("id", json!(0))])),
            remove: None,
        };
        let result = overlays_for_start_at(ov, &row_with(&[("id", json!(1))]), &num_compare);
        assert!(result.add.is_none());
    }

    // Branch: add row is at start → kept.
    #[test]
    fn overlays_for_start_at_add_at_start_kept() {
        let ov = Overlays {
            add: Some(row_with(&[("id", json!(1))])),
            remove: None,
        };
        let result = overlays_for_start_at(ov, &row_with(&[("id", json!(1))]), &num_compare);
        assert!(result.add.is_some());
    }

    // Branch: add row is after start → kept.
    #[test]
    fn overlays_for_start_at_add_after_kept() {
        let ov = Overlays {
            add: Some(row_with(&[("id", json!(2))])),
            remove: None,
        };
        let result = overlays_for_start_at(ov, &row_with(&[("id", json!(1))]), &num_compare);
        assert!(result.add.is_some());
    }

    // Branch: None overlays pass through untouched.
    #[test]
    fn overlays_for_start_at_none_passthrough() {
        let ov = Overlays::default();
        let result = overlays_for_start_at(ov, &row_with(&[("id", json!(1))]), &num_compare);
        assert!(result.add.is_none() && result.remove.is_none());
    }

    // ─── overlays_for_constraint ───────────────────────────────────────

    // Branch: add matches constraint → kept.
    #[test]
    fn overlays_for_constraint_match_kept() {
        let mut c = Constraint::new();
        c.insert("a".into(), Some(json!("b")));
        let ov = Overlays {
            add: Some(row_with(&[("a", json!("b"))])),
            remove: None,
        };
        let result = overlays_for_constraint(ov, &c);
        assert!(result.add.is_some());
    }

    // Branch: add mismatches → dropped.
    #[test]
    fn overlays_for_constraint_mismatch_dropped() {
        let mut c = Constraint::new();
        c.insert("a".into(), Some(json!("b")));
        let ov = Overlays {
            add: Some(row_with(&[("a", json!("c"))])),
            remove: None,
        };
        let result = overlays_for_constraint(ov, &c);
        assert!(result.add.is_none());
    }

    // ─── generate_with_overlay_inner ───────────────────────────────────

    // Branch: no overlay — rows pass through.
    #[test]
    fn overlay_inner_no_overlay_passthrough() {
        let rows = vec![row_with(&[("id", json!(1))]), row_with(&[("id", json!(2))])];
        let out = generate_with_overlay_inner(rows.clone(), Overlays::default(), &num_compare);
        assert_eq!(out.len(), 2);
    }

    // Branch: add overlay in the middle.
    #[test]
    fn overlay_inner_add_middle() {
        let rows = vec![row_with(&[("id", json!(1))]), row_with(&[("id", json!(3))])];
        let ov = Overlays {
            add: Some(row_with(&[("id", json!(2))])),
            remove: None,
        };
        let out = generate_with_overlay_inner(rows, ov, &num_compare);
        let ids: Vec<i64> = out
            .iter()
            .map(|n| n.row.get("id").unwrap().as_ref().unwrap().as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    // Branch: add overlay at the tail (never yielded inline).
    #[test]
    fn overlay_inner_add_tail() {
        let rows = vec![row_with(&[("id", json!(1))])];
        let ov = Overlays {
            add: Some(row_with(&[("id", json!(2))])),
            remove: None,
        };
        let out = generate_with_overlay_inner(rows, ov, &num_compare);
        assert_eq!(out.len(), 2);
    }

    // Branch: remove overlay drops matching row.
    #[test]
    fn overlay_inner_remove_drops_row() {
        let rows = vec![row_with(&[("id", json!(1))]), row_with(&[("id", json!(2))])];
        let ov = Overlays {
            add: None,
            remove: Some(row_with(&[("id", json!(1))])),
        };
        let out = generate_with_overlay_inner(rows, ov, &num_compare);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].row.get("id").unwrap().as_ref().unwrap().as_i64(),
            Some(2)
        );
    }

    // Branch: add + remove (TS "Basic edit" case) — same position.
    #[test]
    fn overlay_inner_add_and_remove_replace() {
        let rows = vec![
            row_with(&[("id", json!(1))]),
            row_with(&[("id", json!(2))]),
            row_with(&[("id", json!(3))]),
        ];
        let ov = Overlays {
            add: Some(row_with(&[("id", json!(2))])),
            remove: Some(row_with(&[("id", json!(2))])),
        };
        let out = generate_with_overlay_inner(rows, ov, &num_compare);
        assert_eq!(out.len(), 3);
    }

    // ─── generate_with_start ────────────────────────────────────────────

    // Branch: no start — yields all.
    #[test]
    fn generate_with_start_none_yields_all() {
        let nodes = vec![
            Node {
                row: row_with(&[("id", json!(1))]),
                relationships: IndexMap::new(),
            },
            Node {
                row: row_with(&[("id", json!(2))]),
                relationships: IndexMap::new(),
            },
        ];
        let out: Vec<Node> = generate_with_start(nodes, None, num_compare).collect();
        assert_eq!(out.len(), 2);
    }

    // Branch: start basis At — inclusive.
    #[test]
    fn generate_with_start_at_inclusive() {
        let nodes = vec![
            Node {
                row: row_with(&[("id", json!(1))]),
                relationships: IndexMap::new(),
            },
            Node {
                row: row_with(&[("id", json!(2))]),
                relationships: IndexMap::new(),
            },
        ];
        let out: Vec<Node> = generate_with_start(
            nodes,
            Some(Start {
                row: row_with(&[("id", json!(2))]),
                basis: StartBasis::At,
            }),
            num_compare,
        )
        .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].row.get("id").unwrap().as_ref().unwrap().as_i64(),
            Some(2)
        );
    }

    // Branch: start basis After — exclusive.
    #[test]
    fn generate_with_start_after_exclusive() {
        let nodes = vec![
            Node {
                row: row_with(&[("id", json!(1))]),
                relationships: IndexMap::new(),
            },
            Node {
                row: row_with(&[("id", json!(2))]),
                relationships: IndexMap::new(),
            },
            Node {
                row: row_with(&[("id", json!(3))]),
                relationships: IndexMap::new(),
            },
        ];
        let out: Vec<Node> = generate_with_start(
            nodes,
            Some(Start {
                row: row_with(&[("id", json!(2))]),
                basis: StartBasis::After,
            }),
            num_compare,
        )
        .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].row.get("id").unwrap().as_ref().unwrap().as_i64(),
            Some(3)
        );
    }

    // ─── stringify ─────────────────────────────────────────────────────

    // Branch: stringify Add.
    #[test]
    fn stringify_add() {
        let s = stringify(&SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("x"))]),
        }));
        assert!(s.contains("\"type\":\"add\""));
        assert!(s.contains("\"a\""));
    }

    // Branch: stringify Remove.
    #[test]
    fn stringify_remove() {
        let s = stringify(&SourceChange::Remove(SourceChangeRemove {
            row: row_with(&[("a", json!("x"))]),
        }));
        assert!(s.contains("\"type\":\"remove\""));
    }

    // Branch: stringify Edit.
    #[test]
    fn stringify_edit() {
        let s = stringify(&SourceChange::Edit(SourceChangeEdit {
            row: row_with(&[("a", json!("y"))]),
            old_row: row_with(&[("a", json!("x"))]),
        }));
        assert!(s.contains("\"type\":\"edit\""));
        assert!(s.contains("oldRow"));
    }

    // ─── push epoch + overlay smoke test ──────────────────────────────

    // Branch: push increments push_epoch once per application (non-split).
    #[test]
    fn push_increments_epoch() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        let _input = ms.connect(sort_asc(&["a"]), None, None, None);
        let e0 = ms.state.lock().unwrap().push_epoch;
        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("x"))]),
        }))
        .count();
        let e1 = ms.state.lock().unwrap().push_epoch;
        assert!(e1 > e0);
    }

    // Branch: fan-out with zero connections just writes.
    #[test]
    fn push_no_connections_still_writes() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("x"))]),
        }))
        .count();
        assert_eq!(ms.len(), 1);
    }

    // Branch: fetch during push observes the new row (nested fetch from
    // the output callback). Validates that the state lock is released
    // before Output::push runs.
    #[test]
    fn fetch_during_push_sees_new_row() {
        struct FetchingOutput {
            state: Arc<Mutex<MemorySourceState>>,
            observed_len: Arc<AtomicUsize>,
            conn_id: u64,
        }
        impl Output for FetchingOutput {
            fn push<'a>(&'a mut self, _change: IvmChange, _p: &dyn InputBase) -> Stream<'a, Yield> {
                let rows = fetch_via_state(&self.state, FetchRequest::default(), self.conn_id);
                self.observed_len.store(rows.len(), AtOrdering::SeqCst);
                Box::new(std::iter::empty())
            }
        }

        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        let mut input = ms.connect(sort_asc(&["a"]), None, None, None);
        // Grab the connection id out of state (only one connection here).
        let conn_id = ms.state.lock().unwrap().connections[0].id;
        let observed = Arc::new(AtomicUsize::new(0));
        input.set_output(Box::new(FetchingOutput {
            state: Arc::clone(&ms.state),
            observed_len: Arc::clone(&observed),
            conn_id,
        }));

        ms.push(SourceChange::Add(SourceChangeAdd {
            row: row_with(&[("a", json!("x"))]),
        }))
        .count();

        // During push, the overlay makes the new row visible via overlay.
        // After push, writeChange has already committed it.
        assert_eq!(observed.load(AtOrdering::SeqCst), 1);
    }

    // ─── gen_push step count ──────────────────────────────────────────

    // Branch: gen_push emits one Step per connection.
    #[test]
    fn gen_push_emits_one_step_per_connection() {
        let ms = MemorySource::new("t", cols(&["a"]), pk(&["a"]), None);
        let _i1 = ms.connect(sort_asc(&["a"]), None, None, None);
        let _i2 = ms.connect(sort_asc(&["a"]), None, None, None);
        let steps: Vec<GenPushStep> = ms
            .gen_push(SourceChange::Add(SourceChangeAdd {
                row: row_with(&[("a", json!("x"))]),
            }))
            .collect();
        assert_eq!(steps.len(), 2);
        assert!(steps.iter().all(|s| matches!(s, GenPushStep::Step)));
    }
}
