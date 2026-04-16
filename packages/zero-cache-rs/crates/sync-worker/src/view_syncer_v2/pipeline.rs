//! PipelineV2 — Chain driver built on ivm_v2 Transformers.
//!
//! **Option 2 architecture** (Transformer-based, no nested ownership):
//!
//! - `Chain` owns a `Source` and a flat `Vec<Box<dyn Transformer>>`.
//! - Transformers are pure: they don't hold their upstream. The Chain
//!   feeds upstream into `fetch_through` and drives push bottom-up via
//!   sequential calls to each transformer's `push`.
//! - No `Arc`, no `Mutex`, no `RefCell`, no `unsafe`, no sharing.
//! - Multi-operator push works correctly: a `Change` pushed into the
//!   Chain propagates through every transformer in order.
//!
//! Scope: `Source + any Vec<Transformer>` (Filter/Skip for now).
//! Take/Exists still use the `Operator` trait with interior input and
//! are integrated separately — that's a follow-up (task #125 next step).

use indexmap::IndexMap;
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::Row;

use crate::ivm::change::{AddChange, Change, ChildChange, EditChange, RemoveChange};
use crate::ivm::data::Node;
use crate::ivm_v2::exists_t::{ExistsT, ExistsType};
use crate::ivm_v2::filter_t::{FilterT, Predicate};
use crate::ivm_v2::join_t::JoinT;
use crate::ivm_v2::operator::{BinaryTransformer, FetchRequest, Input, InputBase, Transformer};
use crate::ivm_v2::skip_t::{Bound, SkipT};
use crate::ivm_v2::take_t::TakeT;
use std::sync::Arc as StdArc;
use zero_cache_types::ast::CompoundKey;

use super::row_change::{RowAdd, RowChange, RowEdit, RowRemove};

/// Chain specification — what transformers to apply, in order.
pub struct ChainSpec {
    pub query_id: String,
    pub table: String,
    pub primary_key: PrimaryKey,
    pub predicate: Option<Predicate>,
    pub skip_bound: Option<Bound>,
    pub limit: Option<usize>,
    pub exists: Option<ExistsSpec>,
    /// Additional EXISTS conditions beyond the first. Applied after the
    /// `exists` entry, in declaration order.
    pub exists_chain: Vec<ExistsSpec>,
    /// Optional Join (hierarchical related). First cut: single join
    /// applied AFTER all other transformers on the hydration path.
    pub join: Option<JoinSpec>,
    /// Query-level `ORDER BY` (from `AST.order_by`). When set, overrides
    /// the table-level default sort at Chain-build time so SqliteSource
    /// emits rows in this order — critical for `Take` slicing and
    /// `Skip` cursor semantics to match TS.
    pub order_by: Option<Vec<(String, zero_cache_types::ast::Direction)>>,
}

pub struct JoinSpec {
    pub parent_key: CompoundKey,
    pub child_key: CompoundKey,
    pub relationship_name: String,
    pub child_table: String,
}

pub struct ExistsSpec {
    pub relationship_name: String,
    pub parent_join_key: CompoundKey,
    pub exists_type: ExistsType,
    /// When set, the driver builds a child source for this table and
    /// hands it to `ExistsT` so the transformer can evaluate existence
    /// by querying the subquery pipeline directly — no reliance on
    /// pre-populated `node.relationships`. Matches TS native
    /// `Exists::*fetch`. Unset → legacy fail-open via relationships.
    pub child_table: Option<String>,
    /// Child primary key — used by `hydrate_stream` to build RowKey
    /// objects when emitting the subquery tree rows. Only populated
    /// by the driver (not the AST builder) because it lives in the
    /// replica's table registry, not the AST itself.
    pub child_primary_key: Option<PrimaryKey>,
    /// Parallel child-side key for the correlation. Only read when
    /// `child_table` is Some.
    pub child_key: CompoundKey,
    /// Legacy pre-compiled child predicate (used by existing tests
    /// that construct `ExistsSpec` directly without a full AST).
    /// The new driver path ignores this in favor of building a
    /// sub-Chain from `child_subquery`.
    pub child_predicate: Option<Predicate>,
    /// Full subquery AST — driver uses this to recursively build a
    /// sub-Chain via `ast_to_chain_spec` + `Chain::build_with_…`,
    /// mirroring TS native `buildPipelineInternal` which recursively
    /// builds a child `Input` for every subquery (including nested
    /// `EXISTS` inside the subquery).
    pub child_subquery: Option<Box<zero_cache_types::ast::AST>>,
    /// Snapshot of the sub-Chain's own `exists_child_tables` —
    /// driver fills this in after recursively building the
    /// child Chain. Lets the parent Chain expose grandchild table
    /// metadata so `hydrate_stream` can recurse arbitrarily deep,
    /// matching TS `Streamer#streamNodes` walking through nested
    /// `schema.relationships`.
    pub child_exists_child_tables: Option<ExistsChildTables>,
}

/// Multiple EXISTS conditions to stack in chain order. Used by the AST
/// builder when the WHERE clause has several CorrelatedSubquery entries.
/// `ChainSpec.exists` is kept for the single-EXISTS common path; use
/// `ChainSpec.exists_chain` for N.
pub type ExistsChain = Vec<ExistsSpec>;

pub struct Chain {
    query_id: String,
    table: String,
    primary_key: PrimaryKey,
    source: Box<dyn Input>,
    /// Applied in order during fetch and push.
    transformers: Vec<Box<dyn Transformer>>,
    /// Optional hierarchical join: if set, the child source + binary
    /// transformer are held here and engaged during hydrate.
    join: Option<JoinStage>,
    /// Recursive map from `ExistsT` relationship name → child meta.
    /// Each entry stores the child table, child primary key, **and**
    /// the child's own exists_child_tables map so `hydrate_stream`
    /// can recurse into grandchildren.
    ///
    /// Matches TS native `SourceSchema.relationships[name]` which
    /// contains the child's full schema (including its own
    /// relationships map) — `Streamer#streamNodes` uses that to
    /// recurse arbitrarily deep.
    exists_child_tables: ExistsChildTables,
}

/// Recursive map from relationship name → child meta. Wraps a
/// `HashMap` because Rust's type aliases cannot be self-referential.
#[derive(Debug, Clone, Default)]
pub struct ExistsChildTables(
    pub std::collections::HashMap<String, (String, PrimaryKey, Box<ExistsChildTables>)>,
);

impl ExistsChildTables {
    pub fn new() -> Self {
        Self(std::collections::HashMap::new())
    }
    pub fn get(&self, k: &str) -> Option<&(String, PrimaryKey, Box<ExistsChildTables>)> {
        self.0.get(k)
    }
    pub fn insert(
        &mut self,
        k: String,
        v: (String, PrimaryKey, Box<ExistsChildTables>),
    ) {
        self.0.insert(k, v);
    }
}

struct JoinStage {
    transformer: Box<dyn BinaryTransformer>,
    child_source: Box<dyn Input>,
    child_table: String,
}

impl Chain {
    /// Build without a Join — existing single-table chain.
    pub fn build(spec: ChainSpec, source: Box<dyn Input>) -> Self {
        Self::build_with_join(spec, source, None)
    }

    /// Build with an optional child source for the Join. The caller
    /// supplies the child source factory-output because Chain doesn't
    /// know how to materialize a second table's source.
    ///
    /// Back-compat shim — tests and older call sites go through this.
    /// Forwards to [`Self::build_with_join_and_exists`] with empty
    /// EXISTS child inputs (legacy fail-open path).
    pub fn build_with_join(
        spec: ChainSpec,
        source: Box<dyn Input>,
        child_source: Option<Box<dyn Input>>,
    ) -> Self {
        Self::build_with_join_and_exists(spec, source, child_source, Vec::new())
    }

    /// Full builder entry point. `exists_child_inputs` lines up with
    /// `spec.exists` (position 0) followed by `spec.exists_chain`
    /// (positions 1..). Each entry is `Some(child_input)` if the AST
    /// builder decided the subquery was simple enough to pre-compile
    /// into a child pipeline; otherwise `None` → ExistsT falls back
    /// to `node.relationships[name]`.
    pub fn build_with_join_and_exists(
        mut spec: ChainSpec,
        mut source: Box<dyn Input>,
        child_source: Option<Box<dyn Input>>,
        exists_child_inputs: Vec<Option<Box<dyn Input>>>,
    ) -> Self {
        let join_spec = spec.join.take();
        let order_by = spec.order_by.take();
        // Downcast to SqliteSource to override the sort when the AST
        // carries its own `orderBy`. Any other Input impl (test doubles,
        // etc.) just uses its constructor-time default sort.
        if let Some(order_by) = order_by {
            if let Some(sqlite) = source
                .as_any_mut()
                .and_then(|a| a.downcast_mut::<crate::view_syncer_v2::sqlite_source::SqliteSource>())
            {
                sqlite.set_sort(order_by);
            }
        }
        let join_stage = match (join_spec, child_source) {
            (Some(js), Some(child)) => Some(JoinStage {
                transformer: Box::new(JoinT::new(
                    js.parent_key,
                    js.child_key,
                    js.relationship_name,
                )),
                child_source: child,
                child_table: js.child_table,
            }),
            _ => None,
        };
        let mut chain = Self::build_inner(spec, source, exists_child_inputs);
        chain.join = join_stage;
        chain
    }

    fn build_inner(
        spec: ChainSpec,
        source: Box<dyn Input>,
        mut exists_child_inputs: Vec<Option<Box<dyn Input>>>,
    ) -> Self {
        let mut transformers: Vec<Box<dyn Transformer>> = Vec::new();
        let comparator = std::sync::Arc::clone(&source.get_schema().compare_rows);
        if let Some(predicate) = spec.predicate {
            transformers.push(Box::new(FilterT::new(predicate)));
        }
        if let Some(bound) = spec.skip_bound {
            transformers.push(Box::new(SkipT::new(bound, std::sync::Arc::clone(&comparator))));
        }
        let pk_cols: Vec<String> = spec.primary_key.columns().to_vec();
        // Iterator yields `(ExistsSpec, Option<child_input>)` — the
        // `spec.exists` single-entry and `spec.exists_chain` share the
        // same indexing into `exists_child_inputs`.
        let all_exists: Vec<ExistsSpec> = spec
            .exists
            .into_iter()
            .chain(spec.exists_chain.into_iter())
            .collect();
        // Pad / truncate to match — caller should pass exactly one
        // entry per ExistsSpec, but we defensively treat mismatch as
        // "no child input" (fall back to relationships).
        exists_child_inputs.resize_with(all_exists.len(), || None);
        let mut exists_child_tables = ExistsChildTables::new();
        for (es, maybe_child) in all_exists.into_iter().zip(exists_child_inputs.into_iter()) {
            // Record child-table / pk / nested-map metadata so
            // `hydrate_stream` can recurse into grandchildren when
            // emitting subquery rows. Matches TS
            // `schema.relationships[name]` carrying the child schema
            // (which itself has `relationships`) — what
            // `Streamer#streamNodes` follows for arbitrarily-deep
            // tree emission.
            if let (Some(tbl), Some(pk)) =
                (es.child_table.clone(), es.child_primary_key.clone())
            {
                let nested = es
                    .child_exists_child_tables
                    .clone()
                    .unwrap_or_default();
                // First-branch wins on duplicate relationship name —
                // matches TS native `UnionFanIn` (union-fan-in.ts L70+):
                // it merges branches' schemas but its
                // `relationshipsFromBranches` set tracks names so the
                // SAME relationship from multiple OR branches doesn't
                // double-register. Without this, `hydrate_stream`
                // walks the LATER branch's nested-grandchild map (e.g.
                // p21's branch B → channel.participants) and emits
                // extra rows TS never ships.
                exists_child_tables.0.entry(es.relationship_name.clone()).or_insert(
                    (tbl, pk, Box::new(nested))
                );
            }
            let mut t = ExistsT::new(
                es.relationship_name,
                es.parent_join_key,
                es.exists_type,
                &pk_cols,
            );
            if let Some(child_input) = maybe_child {
                t = t.with_child_input(child_input, es.child_key);
            }
            transformers.push(Box::new(t));
        }
        if let Some(limit) = spec.limit {
            transformers.push(Box::new(TakeT::new(limit, std::sync::Arc::clone(&comparator))));
        }
        Self {
            query_id: spec.query_id,
            table: spec.table,
            primary_key: spec.primary_key,
            source,
            transformers,
            join: None,
            exists_child_tables,
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn child_table(&self) -> Option<&str> {
        self.join.as_ref().map(|j| j.child_table.as_str())
    }

    /// Advance child-side. A change to the child table flows through
    /// `push_child`, emitting a `ChildChange` per matching parent in
    /// the current parent snapshot. The parent snapshot is materialized
    /// fresh at each call — no cached parent list for now.
    pub fn advance_child(&mut self, change: Change) -> Vec<RowChange> {
        let Some(join) = self.join.as_mut() else {
            return Vec::new();
        };
        // Materialize the parent snapshot from source → transformers.
        // Collect eagerly so the join.transformer.push_child call has a
        // stable borrow.
        let parent_snapshot: Vec<Node> = {
            let mut stream: Box<dyn Iterator<Item = Node>> =
                self.source.fetch(FetchRequest::default());
            for t in &mut self.transformers {
                stream = t.fetch_through(stream, FetchRequest::default());
            }
            stream.collect()
        };
        let emissions: Vec<Change> = join
            .transformer
            .push_child(change, &parent_snapshot)
            .collect();
        let query_id = &self.query_id;
        let table = &self.table;
        let pk = &self.primary_key;
        emissions
            .into_iter()
            .filter_map(|c| match c {
                Change::Child(cc) => {
                    // Emit the ChildChange as an Edit on the parent row
                    // (because the parent's children list has changed).
                    Some(RowChange::Edit(super::row_change::RowEdit {
                        query_id: query_id.clone(),
                        table: table.clone(),
                        row_key: build_row_key(pk, &cc.node.row),
                        row: cc.node.row,
                    }))
                }
                _ => None,
            })
            .collect()
    }

    /// Streaming hydration. Drives source → transformers → optional join
    /// without collecting the parent path, emitting `RowChange` chunks of
    /// `emit_chunk_size()` (default 100) via `on_chunk`. Returns total rows.
    ///
    /// `on_chunk(rows, is_final)` — consumer returns `false` to cancel;
    /// the driver then drops the iterator (stopping upstream) and returns.
    /// The final call always has `is_final = true`, even if the preceding
    /// chunk was full.
    ///
    /// Child-of-join is materialised to `Vec<Node>` once (cross-product
    /// lookup needs random access). Parent side streams all the way.
    pub fn hydrate_stream(
        &mut self,
        mut on_chunk: impl FnMut(Vec<RowChange>, bool) -> bool,
    ) -> usize {
        use crate::ivm_v2::batching::emit_chunk_size;
        let chunk_size = emit_chunk_size();
        let req = FetchRequest::default();

        // Pull the child once before we take long-lived borrows of the
        // other fields — the child borrow ends before the parent stream
        // starts, freeing `self.join` for the join.transformer borrow.
        let child_arc: Option<StdArc<Vec<Node>>> = if let Some(js) = self.join.as_mut() {
            Some(StdArc::new(js.child_source.fetch(req.clone()).collect()))
        } else {
            None
        };

        let query_id = self.query_id.clone();
        let table = self.table.clone();
        let pk = self.primary_key.clone();

        // Build the streaming parent chain.
        let parent_stream: Box<dyn Iterator<Item = Node> + '_> = {
            let mut s: Box<dyn Iterator<Item = Node> + '_> = self.source.fetch(req.clone());
            for t in &mut self.transformers {
                s = t.fetch_through(s, req.clone());
            }
            s
        };

        // Apply join decoration if configured. `&mut self.join` is
        // disjoint from `&mut self.source` and `&mut self.transformers`,
        // so the borrow checker is happy even though parent_stream is
        // still live.
        let final_stream: Box<dyn Iterator<Item = Node> + '_> =
            if let Some(js) = self.join.as_mut() {
                let child = child_arc.expect("child_arc built when join is Some");
                js.transformer.fetch_through(parent_stream, child, req)
            } else {
                parent_stream
            };

        // Deferred-flush: `pending` holds the most recently filled chunk.
        // When the next chunk fills, we flush `pending` with is_final=false
        // and move the just-filled chunk into `pending`. At end-of-stream
        // we flush `pending` (+ possibly a partial `cur`) with is_final=true
        // on the LAST call. Avoids emitting an empty terminal chunk when
        // the total is an exact multiple of chunk_size.
        let mut cur: Vec<RowChange> = Vec::with_capacity(chunk_size);
        let mut pending: Option<Vec<RowChange>> = None;
        let mut total: usize = 0;
        // Snapshot the rel→(child_table, child_pk) map. `for node in
        // final_stream` borrows `self` transitively through
        // `parent_stream` above, so we can't access
        // `self.exists_child_tables` inside the loop body.
        let exists_child_tables = self.exists_child_tables.clone();
        for node in final_stream {
            let row_key = build_row_key(&pk, &node.row);
            cur.push(RowChange::Add(RowAdd {
                query_id: query_id.clone(),
                table: table.clone(),
                row_key,
                row: node.row.clone(),
            }));
            total += 1;
            // Walk `node.relationships` recursively and emit child
            // rows. Matches TS native `Streamer#streamNodes` which
            // walks each node, emits its row, and recurses into
            // `schema.relationships[name]` to emit grandchildren.
            // The nested `exists_child_tables` carries the per-level
            // table+pk metadata, mirroring the child schema's
            // relationships map.
            emit_node_subtree(
                &node,
                &exists_child_tables,
                &query_id,
                &mut cur,
                &mut total,
            );
            if cur.len() >= chunk_size {
                if let Some(prev) = pending.take() {
                    if !on_chunk(prev, false) {
                        return total;
                    }
                }
                pending = Some(std::mem::take(&mut cur));
                cur.reserve(chunk_size);
            }
        }
        match (pending, cur) {
            (Some(prev), last) if !last.is_empty() => {
                if !on_chunk(prev, false) {
                    return total;
                }
                on_chunk(last, true);
            }
            (Some(prev), _empty) => {
                on_chunk(prev, true);
            }
            (None, last) => {
                on_chunk(last, true);
            }
        }
        total
    }

    /// Non-streaming wrapper over `hydrate_stream` for internal tests and
    /// other callers that need the full result up front.
    pub fn hydrate(&mut self) -> Vec<RowChange> {
        let mut all: Vec<RowChange> = Vec::new();
        self.hydrate_stream(|chunk, _is_final| {
            all.extend(chunk);
            true
        });
        all
    }

    /// Advance (parent-side). Pushes a change that originated on this
    /// chain's primary table through all transformers, then (if Join
    /// is configured) through push_parent on the BinaryTransformer with
    /// the change decorated as the parent side.
    pub fn advance(&mut self, change: Change) -> Vec<RowChange> {
        let mut buf: Vec<Change> = vec![change];
        let n = self.transformers.len();
        for idx in 0..n {
            let mut next: Vec<Change> = Vec::new();
            // split so we can borrow `preceding` and the current transformer
            // simultaneously for refetch.
            let (preceding, rest) = self.transformers.split_at_mut(idx);
            let t = &mut rest[0];
            for c in buf.drain(..) {
                let emissions: Vec<Change> = t.push(c).collect();
                next.extend(emissions);
                if let Some(req) = t.take_pending_refetch() {
                    let rows = fetch_prefix(&mut *self.source, preceding, req);
                    t.ingest_refetch(rows);
                }
            }
            buf = next;
        }
        // Parent-side join: forward through push_parent. Returns the
        // decorated change; it's already shaped as a parent Change.
        if let Some(join) = self.join.as_mut() {
            let mut next: Vec<Change> = Vec::new();
            for c in buf.drain(..) {
                for emit in join.transformer.push_parent(c) {
                    next.push(emit);
                }
            }
            buf = next;
        }
        let query_id = &self.query_id;
        let table = &self.table;
        let pk = &self.primary_key;
        buf.into_iter()
            .map(|c| match c {
                Change::Add(AddChange { node }) => RowChange::Add(RowAdd {
                    query_id: query_id.clone(),
                    table: table.clone(),
                    row_key: build_row_key(pk, &node.row),
                    row: node.row,
                }),
                Change::Remove(RemoveChange { node }) => RowChange::Remove(RowRemove {
                    query_id: query_id.clone(),
                    table: table.clone(),
                    row_key: build_row_key(pk, &node.row),
                }),
                Change::Edit(EditChange { node, .. }) => RowChange::Edit(RowEdit {
                    query_id: query_id.clone(),
                    table: table.clone(),
                    row_key: build_row_key(pk, &node.row),
                    row: node.row,
                }),
                // ChildChange = "a relationship's nested change happened, but the
                // parent row's columns are unchanged." From the client's POV the
                // client-side row's `_relationships` view changed, so we surface
                // it as a RowEdit on the parent row (matches TS Streamer's
                // Streamer#streamChanges, which produces an Edit on the parent).
                Change::Child(ChildChange { node, .. }) => RowChange::Edit(RowEdit {
                    query_id: query_id.clone(),
                    table: table.clone(),
                    row_key: build_row_key(pk, &node.row),
                    row: node.row,
                }),
            })
            .collect()
    }
}

/// Drive source + the transformers at indices `0..preceding.len()` with
/// `req`, returning the resulting rows. Used by `advance()` when a
/// transformer signals a mid-push refetch.
fn fetch_prefix(
    source: &mut dyn Input,
    preceding: &mut [Box<dyn Transformer>],
    req: FetchRequest,
) -> Vec<Node> {
    let mut stream: Box<dyn Iterator<Item = Node>> =
        Box::new(source.fetch(req.clone()).collect::<Vec<_>>().into_iter());
    for t in preceding.iter_mut() {
        let s = std::mem::replace(
            &mut stream,
            Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Node>>,
        );
        let collected: Vec<Node> = t.fetch_through(s, req.clone()).collect();
        stream = Box::new(collected.into_iter());
    }
    stream.collect()
}

impl Chain {
    /// Snapshot of this Chain's `exists_child_tables`. Used by the
    /// driver when building a parent `ExistsSpec` so the parent's
    /// map can include the child's nested map under its
    /// relationship's entry — enabling recursive tree walking in
    /// `hydrate_stream`.
    pub fn exists_child_tables_snapshot(&self) -> ExistsChildTables {
        self.exists_child_tables.clone()
    }
}

impl crate::ivm_v2::operator::InputBase for Chain {
    /// A Chain's output schema is its source's schema — `Filter`,
    /// `Skip`, `Take`, `ExistsT` don't change the row shape, they
    /// only filter or decorate.
    fn get_schema(&self) -> &crate::ivm::schema::SourceSchema {
        self.source.get_schema()
    }
    fn destroy(&mut self) {
        for t in &mut self.transformers {
            t.destroy();
        }
        self.source.destroy();
    }
}

impl crate::ivm_v2::operator::Input for Chain {
    /// Chain-as-Input: pipes the source through every transformer
    /// lazily, matching what `hydrate_stream` does internally.
    /// Enables Chain to be plugged in as the child input of a
    /// parent `ExistsT` so subqueries with their own nested EXISTS
    /// get the same recursive pipeline treatment TS native's
    /// `buildPipelineInternal` does.
    ///
    /// Streaming: `source.fetch(req)` returns a lazy iterator; each
    /// `transformer.fetch_through` wraps that iterator (Filter /
    /// Skip / Take / ExistsT all lazy). No materialisation here.
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        let mut s: Box<dyn Iterator<Item = Node> + 'a> = self.source.fetch(req.clone());
        for t in &mut self.transformers {
            s = t.fetch_through(s, req.clone());
        }
        s
    }
}

/// Recursively walk a node's relationships, emitting a RowChange
/// for each grandchild / great-grandchild / etc. — mirrors TS native
/// `Streamer#streamNodes` recursion. The `tables` map is the child's
/// own `exists_child_tables` (carrying its grandchildren's metadata),
/// so the recursion matches the schema.relationships chain TS uses.
fn emit_node_subtree(
    node: &Node,
    tables: &ExistsChildTables,
    query_id: &str,
    cur: &mut Vec<RowChange>,
    total: &mut usize,
) {
    for (rel_name, factory) in node.relationships.iter() {
        let Some((child_table, child_pk, nested)) = tables.get(rel_name.as_str()) else {
            continue;
        };
        let iter = (*factory)();
        for n_or_y in iter {
            let crate::ivm::data::NodeOrYield::Node(child_node) = n_or_y else {
                continue;
            };
            let child_row_key = build_row_key(child_pk, &child_node.row);
            cur.push(RowChange::Add(RowAdd {
                query_id: query_id.to_string(),
                table: child_table.clone(),
                row_key: child_row_key,
                row: child_node.row.clone(),
            }));
            *total += 1;
            // Recurse into grandchildren using the child's own
            // exists_child_tables — matches TS recursing through
            // `schema.relationships[name]` at each level.
            emit_node_subtree(&child_node, nested, query_id, cur, total);
        }
    }
}

fn build_row_key(pk: &PrimaryKey, row: &Row) -> Row {
    let mut key = Row::new();
    for col in pk.columns() {
        if let Some(v) = row.get(col).cloned() {
            key.insert(col.clone(), v);
        }
    }
    key
}

/// In-memory source for tests.
pub struct InMemoryInput {
    rows: Vec<Row>,
    schema: crate::ivm::schema::SourceSchema,
}
impl InMemoryInput {
    pub fn new(rows: Vec<Row>, schema: crate::ivm::schema::SourceSchema) -> Self {
        Self { rows, schema }
    }
}
impl InputBase for InMemoryInput {
    fn get_schema(&self) -> &crate::ivm::schema::SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {}
}
impl Input for InMemoryInput {
    fn fetch<'a>(&'a mut self, _req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        let rows = self.rows.clone();
        Box::new(rows.into_iter().map(|r| Node {
            row: r,
            relationships: IndexMap::new(),
        }))
    }
}
