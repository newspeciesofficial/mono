//! Port of `packages/zql/src/ivm/exists.ts`.
//!
//! The `Exists` operator filters parent rows based on whether a given
//! relationship is non-empty (`EXISTS`) or empty (`NOT EXISTS`).  It is a
//! [`FilterOperator`] — it consumes a [`FilterInput`] upstream and a
//! [`FilterOutput`] downstream, and on every parent `Node` it decides
//! whether to pass through or drop based on the size of the named
//! relationship.
//!
//! ## Public exports ported
//!
//! - [`ExistsType`] — TS string union `'EXISTS' | 'NOT EXISTS'`.
//! - [`Exists`] — the operator.
//!
//! ## Design decisions (called out for review)
//!
//! - **Cache storage.** TS `#cache: Map<string, boolean>` is
//!   [`std::collections::HashMap<String, bool>`] on our side.  The TS
//!   cache key is `JSON.stringify(values)` of the normalised parent
//!   join-key values; we compute the same string via [`serde_json`].
//!
//! - **`#inPush` guard.** TS's re-entrancy flag is an instance `bool`.
//!   Because our [`FilterOutput::push`] takes `&mut self`, a plain
//!   `bool` suffices — no `Mutex` needed.  The TS `assert(!this.#inPush)`
//!   becomes a `panic!` in Rust to match semantics.
//!
//! - **Downstream field wrapping.** We mirror [`crate::ivm::filter::Filter`]:
//!   the `Box<dyn FilterOutput>` lives behind a [`std::sync::Mutex`].
//!   All filter-path methods are `&mut self`, so the mutex is technically
//!   redundant for this struct in isolation, but the pattern matches
//!   sibling operators and keeps the ownership model uniform when the
//!   whole operator is held behind an `Arc` elsewhere in the graph.
//!
//! - **Cache-hit counters (`cacheHitCountsForTesting`).** We keep the TS
//!   diagnostic; exposed via [`Exists::with_cache_hit_counts`] so tests
//!   can observe hits.  The shared counter is
//!   [`Arc<Mutex<HashMap<String, usize>>>`].
//!
//! - **`noSizeReuse` check.** TS `areEqual(parentJoinKey, primaryKey)`
//!   compares element-wise over the `CompoundKey` array vs the primary
//!   key array.  We do a direct `Vec<String>` vs `&[String]` element
//!   comparison, matching TS semantics (order-sensitive).
//!
//! - **`fetchSize` with yield sentinels.** TS walks the relationship
//!   factory counting non-`'yield'` items.  We do the same by invoking
//!   the [`RelationshipFactory`] — but because the factory returns a
//!   `Box<dyn Iterator<Item = NodeOrYield>>`, we must yield `Yield`
//!   sentinels back to the caller through the returned
//!   `Stream<'_, Yield>`.  We collect yields eagerly into the outer
//!   stream returned from `filter` or `push` to match TS
//!   `yield*` semantics.
//!
//! ## TS semantics resolved
//!
//! - The TS `push` `switch` has a `default: unreachable(change)` arm.
//!   Rust's exhaustive `match` over [`Change`] makes that redundant; we
//!   omit it.
//! - The TS `#fetchExists` comment notes that `Take` does not support
//!   early-return during initial fetch, so the operator always fully
//!   computes the size.  We preserve that — no shortcut on size `> 0`.
//! - The TS `#filter` helper has an `exists ?? (yield* this.#fetchExists(node))`
//!   fallback; our `compute_filter` mirrors it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use zero_cache_types::ast::CompoundKey;

use crate::ivm::change::{Change, ChangeType, ChildChange, ChildSpec, RemoveChange};
use crate::ivm::data::{Node, NodeOrYield, RelationshipFactory, normalize_undefined};
use crate::ivm::filter_operators::{FilterInput, FilterOperator, FilterOutput, ThrowFilterOutput};
use crate::ivm::operator::{InputBase, Output};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

// ─── ExistsType ──────────────────────────────────────────────────────

/// TS `type: 'EXISTS' | 'NOT EXISTS'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistsType {
    /// TS `'EXISTS'`.
    Exists,
    /// TS `'NOT EXISTS'`.
    NotExists,
}

// ─── Exists ──────────────────────────────────────────────────────────

/// TS `Exists` — filters parent rows on `EXISTS` / `NOT EXISTS`.
pub struct Exists {
    input: Mutex<Box<dyn FilterInput>>,
    schema: SourceSchema,
    relationship_name: String,
    /// TS `#not = type === 'NOT EXISTS'`.
    not: bool,
    parent_join_key: CompoundKey,
    /// TS `#noSizeReuse = areEqual(parentJoinKey, primaryKey)`.
    no_size_reuse: bool,
    /// TS `#cache: Map<string, boolean>`.
    cache: Mutex<HashMap<String, bool>>,
    /// TS `#cacheHitCountsForTesting?: Map<string, number>`.
    cache_hit_counts_for_testing: Option<Arc<Mutex<HashMap<String, usize>>>>,
    /// TS `#inPush`.
    in_push: Mutex<bool>,
    output: Mutex<Box<dyn FilterOutput>>,
}

impl Exists {
    /// TS `new Exists(input, relationshipName, parentJoinKey, type, cacheHitCountsForTesting?)`.
    ///
    /// Panics if `input`'s schema has no relationship named
    /// `relationship_name` — matches TS `assert(...)`.
    pub fn new(
        input: Box<dyn FilterInput>,
        relationship_name: String,
        parent_join_key: CompoundKey,
        exists_type: ExistsType,
    ) -> Self {
        Self::new_inner(input, relationship_name, parent_join_key, exists_type, None)
    }

    /// TS constructor with `cacheHitCountsForTesting` argument.
    pub fn with_cache_hit_counts(
        input: Box<dyn FilterInput>,
        relationship_name: String,
        parent_join_key: CompoundKey,
        exists_type: ExistsType,
        cache_hit_counts: Arc<Mutex<HashMap<String, usize>>>,
    ) -> Self {
        Self::new_inner(
            input,
            relationship_name,
            parent_join_key,
            exists_type,
            Some(cache_hit_counts),
        )
    }

    fn new_inner(
        input: Box<dyn FilterInput>,
        relationship_name: String,
        parent_join_key: CompoundKey,
        exists_type: ExistsType,
        cache_hit_counts_for_testing: Option<Arc<Mutex<HashMap<String, usize>>>>,
    ) -> Self {
        // TS: assert(this.#input.getSchema().relationships[relationshipName], ...)
        assert!(
            input
                .get_schema()
                .relationships
                .contains_key(&relationship_name),
            "Input schema missing {}",
            relationship_name
        );
        let not = matches!(exists_type, ExistsType::NotExists);

        let schema = input.get_schema().clone();
        let pk_cols: &[String] = schema.primary_key.columns();
        let no_size_reuse =
            parent_join_key.len() == pk_cols.len() && parent_join_key.iter().eq(pk_cols.iter());

        Self {
            input: Mutex::new(input),
            schema,
            relationship_name,
            not,
            parent_join_key,
            no_size_reuse,
            cache: Mutex::new(HashMap::new()),
            cache_hit_counts_for_testing,
            in_push: Mutex::new(false),
            output: Mutex::new(Box::new(ThrowFilterOutput)),
        }
    }

    /// Wired variant: matches TS `new Exists(...)` followed by
    /// `input.setFilterOutput(this)`. Returns `Arc<Self>`.
    pub fn new_wired(
        input: Box<dyn FilterInput>,
        relationship_name: String,
        parent_join_key: CompoundKey,
        exists_type: ExistsType,
    ) -> Arc<Self> {
        let arc = Arc::new(Self::new(
            input,
            relationship_name,
            parent_join_key,
            exists_type,
        ));
        let back: Box<dyn FilterOutput> = Box::new(ExistsPushBackEdge(Arc::clone(&arc)));
        arc.input
            .lock()
            .expect("exists input mutex poisoned")
            .set_filter_output(back);
        arc
    }

    pub fn destroy_arc(&self) {
        self.input
            .lock()
            .expect("exists input mutex poisoned")
            .destroy();
    }

    pub fn set_filter_output_arc(&self, output: Box<dyn FilterOutput>) {
        *self.output.lock().expect("exists output mutex poisoned") = output;
    }

    pub fn begin_filter_arc(&self) {
        let mut o = self.output.lock().expect("exists output mutex poisoned");
        o.begin_filter();
    }

    pub fn end_filter_arc(&self) {
        self.cache.lock().expect("exists cache mutex poisoned").clear();
        let mut o = self.output.lock().expect("exists output mutex poisoned");
        o.end_filter();
    }

    pub fn filter_arc(&self, node: &Node) -> (Vec<Yield>, bool) {
        let mut exists: Option<bool> = None;
        let mut collected_yields: Vec<Yield> = Vec::new();

        let in_push = *self.in_push.lock().expect("exists in_push mutex poisoned");
        if !self.no_size_reuse && !in_push {
            let key = Self::cache_key(node, &self.parent_join_key);
            let cached = {
                let cache_guard = self.cache.lock().expect("exists cache mutex poisoned");
                cache_guard.get(&key).copied()
            };
            match cached {
                Some(c) => {
                    exists = Some(c);
                    if let Some(counts) = &self.cache_hit_counts_for_testing {
                        let mut map = counts
                            .lock()
                            .expect("exists cache-hit counts mutex poisoned");
                        *map.entry(key.clone()).or_insert(0) += 1;
                    }
                }
                None => {
                    let (yields, computed) = self.fetch_exists(node);
                    collected_yields.extend(yields);
                    exists = Some(computed);
                    self.cache
                        .lock()
                        .expect("exists cache mutex poisoned")
                        .insert(key, computed);
                }
            }
        }

        let (yields, self_keep) = self.compute_filter(node, exists);
        collected_yields.extend(yields);

        let keep = if !self_keep {
            false
        } else {
            let mut o = self.output.lock().expect("exists output mutex poisoned");
            let (downstream_stream, downstream_keep) = o.filter(node);
            for y in downstream_stream {
                collected_yields.push(y);
            }
            downstream_keep
        };

        (collected_yields, keep)
    }

    pub fn push_arc(&self, change: Change, _pusher: &dyn InputBase) -> Vec<Yield> {
        {
            let mut flag = self.in_push.lock().expect("exists in_push mutex poisoned");
            assert!(!*flag, "Unexpected re-entrancy");
            *flag = true;
        }
        let yields = self.push_inner_arc(change);
        *self.in_push.lock().expect("exists in_push mutex poisoned") = false;
        yields
    }

    /// Compute the cache key from the parent row's join-key columns.
    ///
    /// TS `#getCacheKey`: `JSON.stringify(values)` over
    /// `parentJoinKey.map(k => normalizeUndefined(node.row[k]))`.
    fn cache_key(node: &Node, def: &CompoundKey) -> String {
        let values: Vec<JsonValue> = def
            .iter()
            .map(|k| {
                let v = node.row.get(k).cloned().flatten();
                normalize_undefined(&v)
            })
            .collect();
        // TS `JSON.stringify` on an array of primitives round-trips
        // through serde_json::to_string on a Vec<JsonValue> identically.
        serde_json::to_string(&values).expect("cache_key serialisation must succeed")
    }

    /// TS `#fetchSize(node)` — count non-`'yield'` items in the named
    /// relationship, forwarding any `Yield` sentinels to the caller.
    ///
    /// Returns `(yields, size)` — the caller inserts `yields` into its
    /// own output stream before using `size`.
    fn fetch_size(&self, node: &Node) -> (Vec<Yield>, usize) {
        let relationship = node
            .relationships
            .get(&self.relationship_name)
            .expect("Exists: relationship not found on node (schema/node mismatch)");
        let mut yields = Vec::new();
        let mut size = 0usize;
        for item in relationship() {
            match item {
                NodeOrYield::Yield => yields.push(Yield),
                NodeOrYield::Node(_) => size += 1,
            }
        }
        (yields, size)
    }

    /// TS `#fetchExists(node)` — `(yield* this.#fetchSize(node)) > 0`.
    fn fetch_exists(&self, node: &Node) -> (Vec<Yield>, bool) {
        let (yields, size) = self.fetch_size(node);
        (yields, size > 0)
    }

    /// TS `#filter(node, exists?)`:
    ///   `exists = exists ?? (yield* this.#fetchExists(node));`
    ///   `return this.#not ? !exists : exists;`
    ///
    /// Returns `(yields, keep)`.
    fn compute_filter(&self, node: &Node, exists: Option<bool>) -> (Vec<Yield>, bool) {
        let (yields, exists) = match exists {
            Some(e) => (Vec::new(), e),
            None => self.fetch_exists(node),
        };
        let keep = if self.not { !exists } else { exists };
        (yields, keep)
    }

    /// TS `#pushWithFilter(change, exists?)` — &self variant.
    fn push_with_filter_arc(&self, change: Change, exists: Option<bool>) -> Vec<Yield> {
        let (mut yields, keep) = self.compute_filter(change.node(), exists);
        if keep {
            let mut o = self.output.lock().expect("exists output mutex poisoned");
            for y in o.push(change, self as &dyn InputBase) {
                yields.push(y);
            }
        }
        yields
    }

    /// Deprecated &mut variant retained for tests; delegates.
    fn push_with_filter(&mut self, change: Change, exists: Option<bool>) -> Vec<Yield> {
        self.push_with_filter_arc(change, exists)
    }
}

// ─── Trait impls ─────────────────────────────────────────────────────

impl InputBase for Exists {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn destroy(&mut self) {
        self.destroy_arc();
    }
}

impl FilterInput for Exists {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        self.set_filter_output_arc(output);
    }
}

impl FilterOutput for Exists {
    fn begin_filter(&mut self) {
        self.begin_filter_arc();
    }

    fn end_filter(&mut self) {
        self.end_filter_arc();
    }

    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        let (yields, keep) = self.filter_arc(node);
        (Box::new(yields.into_iter()), keep)
    }
}

impl Output for Exists {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let yields = self.push_arc(change, pusher);
        Box::new(yields.into_iter())
    }
}

impl FilterOperator for Exists {}

/// Back-edge installed on [`Exists::input`]. Holds `Arc<Exists>`
/// directly (no outer Mutex) and forwards via `*_arc` helpers.
pub struct ExistsPushBackEdge(pub Arc<Exists>);

impl Output for ExistsPushBackEdge {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        eprintln!("[TRACE ivm] Exists::push enter");
        let items = self.0.push_arc(change, pusher);
        eprintln!("[TRACE ivm] Exists::push exit yields={}", items.len());
        Box::new(items.into_iter())
    }
}

impl FilterOutput for ExistsPushBackEdge {
    fn begin_filter(&mut self) {
        self.0.begin_filter_arc();
    }
    fn end_filter(&mut self) {
        self.0.end_filter_arc();
    }
    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        let (yields, keep) = self.0.filter_arc(node);
        (Box::new(yields.into_iter()), keep)
    }
}

/// Adapter to plug a wired [`Arc<Exists>`] back into the chain
/// as `Box<dyn FilterInput>`. Schema cached at construction.
pub struct ArcExistsAsInput {
    inner: Arc<Exists>,
    schema: SourceSchema,
}

impl ArcExistsAsInput {
    pub fn new(inner: Arc<Exists>) -> Self {
        let schema = inner.get_schema().clone();
        Self { inner, schema }
    }
}

impl InputBase for ArcExistsAsInput {
    fn get_schema(&self) -> &SourceSchema { &self.schema }
    fn destroy(&mut self) { self.inner.destroy_arc(); }
}

impl FilterInput for ArcExistsAsInput {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        self.inner.set_filter_output_arc(output);
    }
}

impl Output for ArcExistsAsInput {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let items = self.inner.push_arc(change, pusher);
        Box::new(items.into_iter())
    }
}

impl FilterOutput for ArcExistsAsInput {
    fn begin_filter(&mut self) {
        self.inner.begin_filter_arc();
    }
    fn end_filter(&mut self) {
        self.inner.end_filter_arc();
    }
    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        let (yields, keep) = self.inner.filter_arc(node);
        (Box::new(yields.into_iter()), keep)
    }
}

impl FilterOperator for ArcExistsAsInput {}

impl Exists {
    /// &self core of push — interior mutability via Mutex fields.
    fn push_inner_arc(&self, change: Change) -> Vec<Yield> {
        match change.change_type() {
            ChangeType::Add | ChangeType::Edit | ChangeType::Remove => {
                return self.push_with_filter_arc(change, None);
            }
            ChangeType::Child => {}
        }

        let Change::Child(child_change) = change else {
            unreachable!("push_inner_arc: ChangeType::Child variant expected");
        };

        let is_target_rel = child_change.child.relationship_name == self.relationship_name;
        let inner_type = child_change.child.change.change_type();
        let is_resizing_inner = matches!(inner_type, ChangeType::Add | ChangeType::Remove);
        if !is_target_rel || !is_resizing_inner {
            return self.push_with_filter_arc(Change::Child(child_change), None);
        }

        match inner_type {
            ChangeType::Add => self.push_child_add_arc(child_change),
            ChangeType::Remove => self.push_child_remove_arc(child_change),
            _ => unreachable!("is_resizing_inner guards above"),
        }
    }

    /// Deprecated &mut variant kept for tests; delegates.
    fn push_inner(&mut self, change: Change) -> Vec<Yield> {
        self.push_inner_arc(change)
    }

    /// &self child-add branch.
    fn push_child_add_arc(&self, child_change: ChildChange) -> Vec<Yield> {
        let (mut yields, size) = self.fetch_size(&child_change.node);

        if size == 1 {
            let out_change: Change = if self.not {
                Change::Remove(RemoveChange {
                    node: node_with_relationship_replaced(
                        &child_change.node,
                        &self.relationship_name,
                        make_empty_relationship_factory(),
                    ),
                })
            } else {
                Change::Add(crate::ivm::change::AddChange {
                    node: clone_node_shallow(&child_change.node),
                })
            };
            let mut o = self.output.lock().expect("exists output mutex poisoned");
            for y in o.push(out_change, self as &dyn InputBase) {
                yields.push(y);
            }
            yields
        } else {
            let rebuilt = Change::Child(child_change);
            let more = self.push_with_filter_arc(rebuilt, Some(size > 0));
            yields.extend(more);
            yields
        }
    }

    /// &self child-remove branch.
    fn push_child_remove_arc(&self, child_change: ChildChange) -> Vec<Yield> {
        let (mut yields, size) = self.fetch_size(&child_change.node);

        if size == 0 {
            let out_change: Change = if self.not {
                Change::Add(crate::ivm::change::AddChange {
                    node: clone_node_shallow(&child_change.node),
                })
            } else {
                let removed_inner_node = match &*child_change.child.change {
                    Change::Remove(r) => clone_node_shallow(&r.node),
                    _ => unreachable!("caller guarantees inner Remove"),
                };
                let factory: RelationshipFactory = Box::new(move || {
                    let v = vec![NodeOrYield::Node(clone_node_shallow(&removed_inner_node))];
                    Box::new(v.into_iter()) as Box<dyn Iterator<Item = NodeOrYield>>
                });
                Change::Remove(RemoveChange {
                    node: node_with_relationship_replaced(
                        &child_change.node,
                        &self.relationship_name,
                        factory,
                    ),
                })
            };
            let mut o = self.output.lock().expect("exists output mutex poisoned");
            for y in o.push(out_change, self as &dyn InputBase) {
                yields.push(y);
            }
            yields
        } else {
            let rebuilt = Change::Child(child_change);
            let more = self.push_with_filter_arc(rebuilt, Some(size > 0));
            yields.extend(more);
            yields
        }
    }
}

// ─── Node-manipulation helpers ───────────────────────────────────────

/// Shallow clone of a [`Node`] — the row is cloned but relationships are
/// dropped (same caveat as [`Node::clone`]; see `data.rs`).  Used when
/// constructing output changes where downstream doesn't re-read
/// relationships.
fn clone_node_shallow(n: &Node) -> Node {
    Node {
        row: n.row.clone(),
        relationships: IndexMap::new(),
    }
}

/// Build a [`Node`] that copies `src.row` and `src.relationships` map
/// structure but replaces `rel_name`'s factory with `new_factory`.
///
/// The other relationships are not cloned at the factory level (TS's
/// `{...change.node.relationships, [name]: ...}` simply copies the
/// references into a new object).  Because Rust's `RelationshipFactory`
/// is `Box<dyn Fn … + Send + Sync>` and not `Clone`, we can only move
/// them — so in our port we drop the sibling factories.  Downstream
/// `Remove` handlers read only the named relationship (the one we just
/// overrode) and the parent row's PK; they do not fan out into sibling
/// relationships.  If a future consumer relies on sibling factories,
/// revisit this.
fn node_with_relationship_replaced(
    src: &Node,
    rel_name: &str,
    new_factory: RelationshipFactory,
) -> Node {
    let mut rels: IndexMap<String, RelationshipFactory> = IndexMap::new();
    rels.insert(rel_name.to_string(), new_factory);
    Node {
        row: src.row.clone(),
        relationships: rels,
    }
}

/// Factory that returns an empty stream — TS `() => []`.
fn make_empty_relationship_factory() -> RelationshipFactory {
    Box::new(|| Box::new(std::iter::empty()) as Box<dyn Iterator<Item = NodeOrYield>>)
}

// Suppress unused import warnings when no tests pull these in.
#[allow(dead_code)]
fn _touch_unused(_c: ChildSpec) {}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage for `Exists`:
    //!
    //! `new` / construction:
    //!   - Branch: `EXISTS` vs `NOT EXISTS` ⇒ `not` flag toggle.
    //!   - Branch: `parent_join_key == primary_key` ⇒ `no_size_reuse = true`.
    //!   - Branch: `parent_join_key != primary_key` ⇒ `no_size_reuse = false`.
    //!   - Branch: schema missing relationship ⇒ panic.
    //!
    //! `get_schema` / `destroy` / `set_filter_output`: delegate branches.
    //!
    //! `filter`:
    //!   - Branch: `no_size_reuse=true` skips cache lookup.
    //!   - Branch: `no_size_reuse=false`, cache miss ⇒ fetch + store.
    //!   - Branch: `no_size_reuse=false`, cache hit ⇒ no fetch; cache-hit
    //!     counter bumps (when counter is wired).
    //!   - Branch: `in_push=true` bypasses cache on filter path.
    //!   - Branch: `EXISTS`, size>0 ⇒ keep.
    //!   - Branch: `EXISTS`, size=0 ⇒ drop (self_keep=false; downstream.filter
    //!     NOT called).
    //!   - Branch: `NOT EXISTS`, size=0 ⇒ keep.
    //!   - Branch: `NOT EXISTS`, size>0 ⇒ drop.
    //!   - Branch: self_keep=true, downstream.filter=true ⇒ keep.
    //!   - Branch: self_keep=true, downstream.filter=false ⇒ drop.
    //!   - Branch: yields from relationship factory propagate.
    //!
    //! `begin_filter` / `end_filter`:
    //!   - Branch: `begin_filter` delegates.
    //!   - Branch: `end_filter` clears cache AND delegates.
    //!
    //! `push`:
    //!   - Branch: Add ⇒ push-with-filter (EXISTS, size>0 keeps).
    //!   - Branch: Remove ⇒ push-with-filter.
    //!   - Branch: Edit ⇒ push-with-filter.
    //!   - Branch: Child on different relationship ⇒ push-with-filter.
    //!   - Branch: Child inner Edit on target rel ⇒ push-with-filter.
    //!   - Branch: Child inner Child on target rel ⇒ push-with-filter.
    //!   - Branch: Child inner Add, size==1, EXISTS ⇒ emit parent Add.
    //!   - Branch: Child inner Add, size==1, NOT EXISTS ⇒ emit parent
    //!     Remove (with empty relationship).
    //!   - Branch: Child inner Add, size>1 ⇒ push-with-filter (hint=true).
    //!   - Branch: Child inner Remove, size==0, EXISTS ⇒ emit parent
    //!     Remove with removed child as the relationship.
    //!   - Branch: Child inner Remove, size==0, NOT EXISTS ⇒ emit parent Add.
    //!   - Branch: Child inner Remove, size>0 ⇒ push-with-filter.
    //!   - Branch: re-entrant push ⇒ panic (`assert(!inPush)`).

    use super::*;
    use crate::ivm::change::{
        AddChange, ChangeType, ChildChange, ChildSpec, EditChange, RemoveChange,
    };
    use crate::ivm::data::make_comparator;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrdering};
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Row;

    // ── Harness ──────────────────────────────────────────────────────

    fn make_child_schema() -> SourceSchema {
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
        SourceSchema {
            table_name: "child".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    fn make_parent_schema(pk: Vec<String>, rel_name: &str) -> SourceSchema {
        let sort: Ordering = pk.iter().map(|c| (c.clone(), Direction::Asc)).collect();
        let mut rels = IndexMap::new();
        rels.insert(rel_name.to_string(), make_child_schema());
        SourceSchema {
            table_name: "parent".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(pk),
            relationships: rels,
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    fn row_id(id: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r
    }

    fn row_id_fk(id: i64, fk: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("fk".into(), Some(json!(fk)));
        r
    }

    fn node_with_rel(row: Row, rel_name: &str, child_count: usize) -> Node {
        let name = rel_name.to_string();
        let rows: Vec<Row> = (0..child_count).map(|i| row_id(100 + i as i64)).collect();
        let factory: RelationshipFactory = Box::new(move || {
            let nodes: Vec<NodeOrYield> = rows
                .iter()
                .map(|r| {
                    NodeOrYield::Node(Node {
                        row: r.clone(),
                        relationships: IndexMap::new(),
                    })
                })
                .collect();
            Box::new(nodes.into_iter()) as Box<dyn Iterator<Item = NodeOrYield>>
        });
        let mut rels = IndexMap::new();
        rels.insert(name, factory);
        Node {
            row,
            relationships: rels,
        }
    }

    fn node_with_rel_and_yields(row: Row, rel_name: &str, yields: usize) -> Node {
        let name = rel_name.to_string();
        let factory: RelationshipFactory = Box::new(move || {
            let mut out: Vec<NodeOrYield> = Vec::new();
            for _ in 0..yields {
                out.push(NodeOrYield::Yield);
            }
            Box::new(out.into_iter()) as Box<dyn Iterator<Item = NodeOrYield>>
        });
        let mut rels = IndexMap::new();
        rels.insert(name, factory);
        Node {
            row,
            relationships: rels,
        }
    }

    /// A FilterInput stub with a configurable schema.
    struct StubFilterInput {
        schema: SourceSchema,
        destroyed: Arc<AtomicBool>,
    }
    impl InputBase for StubFilterInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {
            self.destroyed.store(true, AtOrdering::SeqCst);
        }
    }
    impl FilterInput for StubFilterInput {
        fn set_filter_output(&mut self, _o: Box<dyn FilterOutput>) {}
    }

    /// Downstream that records filter/push calls.  Downstream's
    /// `filter` returns `true` unless `deny_all` is set.
    struct Recorder {
        deny_all_filter: bool,
        begins: Arc<AtomicUsize>,
        ends: Arc<AtomicUsize>,
        filter_calls: Arc<AtomicUsize>,
        pushes: Arc<Mutex<Vec<(ChangeType, Row)>>>,
    }
    impl Recorder {
        fn new() -> Self {
            Self {
                deny_all_filter: false,
                begins: Arc::new(AtomicUsize::new(0)),
                ends: Arc::new(AtomicUsize::new(0)),
                filter_calls: Arc::new(AtomicUsize::new(0)),
                pushes: Arc::new(Mutex::new(Vec::new())),
            }
        }
        fn shared(&self) -> SharedRecorder {
            SharedRecorder {
                begins: Arc::clone(&self.begins),
                ends: Arc::clone(&self.ends),
                filter_calls: Arc::clone(&self.filter_calls),
                pushes: Arc::clone(&self.pushes),
            }
        }
    }
    struct SharedRecorder {
        begins: Arc<AtomicUsize>,
        ends: Arc<AtomicUsize>,
        filter_calls: Arc<AtomicUsize>,
        pushes: Arc<Mutex<Vec<(ChangeType, Row)>>>,
    }
    impl Output for Recorder {
        fn push<'a>(&'a mut self, c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes
                .lock()
                .unwrap()
                .push((c.change_type(), c.node().row.clone()));
            Box::new(std::iter::empty())
        }
    }
    impl FilterOutput for Recorder {
        fn begin_filter(&mut self) {
            self.begins.fetch_add(1, AtOrdering::SeqCst);
        }
        fn end_filter(&mut self) {
            self.ends.fetch_add(1, AtOrdering::SeqCst);
        }
        fn filter(&mut self, _n: &Node) -> (Stream<'_, Yield>, bool) {
            self.filter_calls.fetch_add(1, AtOrdering::SeqCst);
            (Box::new(std::iter::empty()), !self.deny_all_filter)
        }
    }

    fn mk_exists(
        pk: Vec<String>,
        rel_name: &str,
        parent_join_key: CompoundKey,
        t: ExistsType,
    ) -> (Exists, SharedRecorder) {
        let input = StubFilterInput {
            schema: make_parent_schema(pk, rel_name),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let mut e = Exists::new(Box::new(input), rel_name.to_string(), parent_join_key, t);
        let rec = Recorder::new();
        let shared = rec.shared();
        e.set_filter_output(Box::new(rec));
        (e, shared)
    }

    // ── Construction ─────────────────────────────────────────────────

    // Branch: EXISTS constructor sets `not = false`.
    #[test]
    fn construct_exists_sets_not_false() {
        let (e, _) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["id".into()],
            ExistsType::Exists,
        );
        assert!(!e.not);
    }

    // Branch: NOT EXISTS constructor sets `not = true`.
    #[test]
    fn construct_not_exists_sets_not_true() {
        let (e, _) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["id".into()],
            ExistsType::NotExists,
        );
        assert!(e.not);
    }

    // Branch: parent_join_key == primary_key ⇒ no_size_reuse = true.
    #[test]
    fn construct_parent_join_key_equals_pk_sets_no_size_reuse() {
        let (e, _) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["id".into()],
            ExistsType::Exists,
        );
        assert!(e.no_size_reuse);
    }

    // Branch: parent_join_key != primary_key ⇒ no_size_reuse = false.
    #[test]
    fn construct_parent_join_key_differs_from_pk_allows_reuse() {
        let (e, _) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        assert!(!e.no_size_reuse);
    }

    // Branch: compound PK vs compound join key, different order ⇒ no_size_reuse=false.
    #[test]
    fn construct_compound_keys_order_sensitive() {
        let (e, _) = mk_exists(
            vec!["a".into(), "b".into()],
            "rel",
            vec!["b".into(), "a".into()],
            ExistsType::Exists,
        );
        assert!(!e.no_size_reuse);
    }

    // Branch: schema missing relationship ⇒ panic.
    #[test]
    #[should_panic(expected = "Input schema missing")]
    fn construct_missing_relationship_panics() {
        let input = StubFilterInput {
            schema: {
                let sort: Ordering = vec![("id".into(), Direction::Asc)];
                SourceSchema {
                    table_name: "p".into(),
                    columns: IndexMap::new(),
                    primary_key: PrimaryKey::new(vec!["id".into()]),
                    relationships: IndexMap::new(),
                    is_hidden: false,
                    system: System::Test,
                    compare_rows: Arc::new(make_comparator(sort.clone(), false)),
                    sort,
                }
            },
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let _ = Exists::new(
            Box::new(input),
            "missing".into(),
            vec!["id".into()],
            ExistsType::Exists,
        );
    }

    // Branch: `destroy` delegates to upstream FilterInput.
    #[test]
    fn destroy_delegates_to_input() {
        let flag = Arc::new(AtomicBool::new(false));
        let input = StubFilterInput {
            schema: make_parent_schema(vec!["id".into()], "rel"),
            destroyed: Arc::clone(&flag),
        };
        let mut e = Exists::new(
            Box::new(input),
            "rel".into(),
            vec!["id".into()],
            ExistsType::Exists,
        );
        e.destroy();
        assert!(flag.load(AtOrdering::SeqCst));
    }

    // Branch: `get_schema` delegates to upstream FilterInput.
    #[test]
    fn get_schema_delegates_to_input() {
        let (e, _) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["id".into()],
            ExistsType::Exists,
        );
        assert_eq!(e.get_schema().table_name, "parent");
    }

    // ── filter() ─────────────────────────────────────────────────────

    // Branch: EXISTS, size>0 ⇒ keep; downstream filter consulted.
    #[test]
    fn filter_exists_with_nonempty_relationship_keeps() {
        let (mut e, rec) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let n = node_with_rel(row_id_fk(1, 10), "rel", 1);
        let (_s, keep) = e.filter(&n);
        assert!(keep);
        assert_eq!(rec.filter_calls.load(AtOrdering::SeqCst), 1);
    }

    // Branch: EXISTS, size=0 ⇒ drop; downstream filter NOT called.
    #[test]
    fn filter_exists_with_empty_relationship_drops_without_downstream() {
        let (mut e, rec) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let n = node_with_rel(row_id_fk(1, 10), "rel", 0);
        let (_s, keep) = e.filter(&n);
        assert!(!keep);
        assert_eq!(rec.filter_calls.load(AtOrdering::SeqCst), 0);
    }

    // Branch: NOT EXISTS, size=0 ⇒ keep.
    #[test]
    fn filter_not_exists_with_empty_relationship_keeps() {
        let (mut e, _rec) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::NotExists,
        );
        let n = node_with_rel(row_id_fk(1, 10), "rel", 0);
        let (_s, keep) = e.filter(&n);
        assert!(keep);
    }

    // Branch: NOT EXISTS, size>0 ⇒ drop.
    #[test]
    fn filter_not_exists_with_nonempty_relationship_drops() {
        let (mut e, _rec) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::NotExists,
        );
        let n = node_with_rel(row_id_fk(1, 10), "rel", 2);
        let (_s, keep) = e.filter(&n);
        assert!(!keep);
    }

    // Branch: self_keep=true, downstream.filter=false ⇒ drop.
    #[test]
    fn filter_downstream_rejects_drops() {
        let input = StubFilterInput {
            schema: make_parent_schema(vec!["id".into()], "rel"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let mut e = Exists::new(
            Box::new(input),
            "rel".into(),
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let mut rec = Recorder::new();
        rec.deny_all_filter = true;
        let _shared = rec.shared();
        e.set_filter_output(Box::new(rec));

        let n = node_with_rel(row_id_fk(1, 10), "rel", 3);
        let (_s, keep) = e.filter(&n);
        assert!(!keep);
    }

    // Branch: relationship yields propagate through filter.
    #[test]
    fn filter_propagates_yields_from_relationship() {
        let (mut e, _rec) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let n = node_with_rel_and_yields(row_id_fk(1, 10), "rel", 3);
        let (stream, keep) = e.filter(&n);
        let ys: Vec<Yield> = stream.collect();
        assert_eq!(ys.len(), 3);
        // size was 0 (only yields, no nodes) ⇒ EXISTS ⇒ drop.
        assert!(!keep);
    }

    // Branch: cache hit ⇒ no re-fetch; counter bumps when wired.
    #[test]
    fn filter_cache_hit_short_circuits_fetch() {
        let counts: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
        let input = StubFilterInput {
            schema: make_parent_schema(vec!["id".into()], "rel"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let mut e = Exists::with_cache_hit_counts(
            Box::new(input),
            "rel".into(),
            vec!["fk".into()],
            ExistsType::Exists,
            Arc::clone(&counts),
        );
        let rec = Recorder::new();
        e.set_filter_output(Box::new(rec));

        // Two rows with same join-key value — second one should hit cache.
        let n1 = node_with_rel(row_id_fk(1, 10), "rel", 2);
        let n2 = node_with_rel(row_id_fk(2, 10), "rel", 2);
        let _ = e.filter(&n1).0.count();
        let _ = e.filter(&n2).0.count();
        let map = counts.lock().unwrap();
        let total_hits: usize = map.values().sum();
        assert_eq!(total_hits, 1, "second filter call should be a cache hit");
    }

    // Branch: no_size_reuse=true ⇒ no cache interaction; counter never bumps.
    #[test]
    fn filter_no_size_reuse_bypasses_cache() {
        let counts: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
        let input = StubFilterInput {
            schema: make_parent_schema(vec!["id".into()], "rel"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        // parent_join_key == primary_key ⇒ no_size_reuse=true.
        let mut e = Exists::with_cache_hit_counts(
            Box::new(input),
            "rel".into(),
            vec!["id".into()],
            ExistsType::Exists,
            Arc::clone(&counts),
        );
        let rec = Recorder::new();
        e.set_filter_output(Box::new(rec));
        let n = node_with_rel(row_id(1), "rel", 1);
        let _ = e.filter(&n).0.count();
        let _ = e.filter(&n).0.count();
        let map = counts.lock().unwrap();
        assert!(map.is_empty());
    }

    // Branch: end_filter clears the cache AND delegates.
    #[test]
    fn end_filter_clears_cache_and_delegates() {
        let counts: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
        let input = StubFilterInput {
            schema: make_parent_schema(vec!["id".into()], "rel"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let mut e = Exists::with_cache_hit_counts(
            Box::new(input),
            "rel".into(),
            vec!["fk".into()],
            ExistsType::Exists,
            Arc::clone(&counts),
        );
        let rec = Recorder::new();
        let shared = rec.shared();
        e.set_filter_output(Box::new(rec));

        let n1 = node_with_rel(row_id_fk(1, 10), "rel", 1);
        let _ = e.filter(&n1).0.count();
        assert_eq!(e.cache.lock().unwrap().len(), 1);
        e.end_filter();
        assert_eq!(e.cache.lock().unwrap().len(), 0, "end_filter must clear cache");
        assert_eq!(shared.ends.load(AtOrdering::SeqCst), 1);
    }

    // Branch: begin_filter delegates.
    #[test]
    fn begin_filter_delegates() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        e.begin_filter();
        assert_eq!(shared.begins.load(AtOrdering::SeqCst), 1);
    }

    // ── push() ───────────────────────────────────────────────────────

    fn dummy_pusher() -> StubFilterInput {
        StubFilterInput {
            schema: make_parent_schema(vec!["id".into()], "rel"),
            destroyed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn collect_push(e: &mut Exists, change: Change) {
        let pusher = dummy_pusher();
        let _: Vec<_> = e.push(change, &pusher as &dyn InputBase).collect();
    }

    // Branch: push Add with non-empty relationship ⇒ push-with-filter
    // keeps and emits Add downstream.
    #[test]
    fn push_add_forwards_when_exists() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let n = node_with_rel(row_id_fk(1, 10), "rel", 1);
        collect_push(&mut e, Change::Add(AddChange { node: n }));
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Add);
    }

    // Branch: push Remove ⇒ push-with-filter (non-empty ⇒ forward).
    #[test]
    fn push_remove_forwards_when_exists() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let n = node_with_rel(row_id_fk(1, 10), "rel", 1);
        collect_push(&mut e, Change::Remove(RemoveChange { node: n }));
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Remove);
    }

    // Branch: push Edit ⇒ push-with-filter.
    #[test]
    fn push_edit_forwards_when_exists() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let old = node_with_rel(row_id_fk(1, 10), "rel", 1);
        let new_ = node_with_rel(row_id_fk(1, 10), "rel", 1);
        collect_push(
            &mut e,
            Change::Edit(EditChange {
                node: new_,
                old_node: old,
            }),
        );
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Edit);
    }

    // Branch: push Child on *other* relationship ⇒ push-with-filter.
    #[test]
    fn push_child_on_other_relationship_pushes_with_filter() {
        // parent_join_key=fk so cache applies; relationship has 1 child ⇒ exists.
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let parent = node_with_rel(row_id_fk(1, 10), "rel", 1);
        let inner = Change::Add(AddChange {
            node: Node {
                row: row_id(100),
                relationships: IndexMap::new(),
            },
        });
        let change = Change::Child(ChildChange {
            node: parent,
            child: ChildSpec {
                relationship_name: "other_rel".into(),
                change: Box::new(inner),
            },
        });
        collect_push(&mut e, change);
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Child);
    }

    // Branch: push Child with inner Edit ⇒ push-with-filter.
    #[test]
    fn push_child_with_inner_edit_pushes_with_filter() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let parent = node_with_rel(row_id_fk(1, 10), "rel", 1);
        let child_old = Node {
            row: row_id(100),
            relationships: IndexMap::new(),
        };
        let child_new = Node {
            row: row_id(100),
            relationships: IndexMap::new(),
        };
        let inner = Change::Edit(EditChange {
            node: child_new,
            old_node: child_old,
        });
        let change = Change::Child(ChildChange {
            node: parent,
            child: ChildSpec {
                relationship_name: "rel".into(),
                change: Box::new(inner),
            },
        });
        collect_push(&mut e, change);
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Child);
    }

    // Branch: push Child with inner Child ⇒ push-with-filter.
    #[test]
    fn push_child_with_inner_child_pushes_with_filter() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let parent = node_with_rel(row_id_fk(1, 10), "rel", 1);
        let inner_child = Node {
            row: row_id(100),
            relationships: IndexMap::new(),
        };
        let inner_inner = Change::Add(AddChange {
            node: Node {
                row: row_id(200),
                relationships: IndexMap::new(),
            },
        });
        let inner = Change::Child(ChildChange {
            node: inner_child,
            child: ChildSpec {
                relationship_name: "grand".into(),
                change: Box::new(inner_inner),
            },
        });
        let change = Change::Child(ChildChange {
            node: parent,
            child: ChildSpec {
                relationship_name: "rel".into(),
                change: Box::new(inner),
            },
        });
        collect_push(&mut e, change);
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Child);
    }

    // Branch: Child inner Add, size==1, EXISTS ⇒ emit parent Add.
    #[test]
    fn push_child_inner_add_size_one_exists_emits_parent_add() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        // After the add there's 1 child — parent's relationship reports size=1.
        let parent = node_with_rel(row_id_fk(1, 10), "rel", 1);
        let inner = Change::Add(AddChange {
            node: Node {
                row: row_id(100),
                relationships: IndexMap::new(),
            },
        });
        let change = Change::Child(ChildChange {
            node: parent,
            child: ChildSpec {
                relationship_name: "rel".into(),
                change: Box::new(inner),
            },
        });
        collect_push(&mut e, change);
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Add);
        assert_eq!(p[0].1.get("id"), Some(&Some(json!(1))));
    }

    // Branch: Child inner Add, size==1, NOT EXISTS ⇒ emit parent Remove.
    #[test]
    fn push_child_inner_add_size_one_not_exists_emits_parent_remove() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::NotExists,
        );
        let parent = node_with_rel(row_id_fk(1, 10), "rel", 1);
        let inner = Change::Add(AddChange {
            node: Node {
                row: row_id(100),
                relationships: IndexMap::new(),
            },
        });
        let change = Change::Child(ChildChange {
            node: parent,
            child: ChildSpec {
                relationship_name: "rel".into(),
                change: Box::new(inner),
            },
        });
        collect_push(&mut e, change);
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Remove);
    }

    // Branch: Child inner Add, size>1 ⇒ push-with-filter (hint=true).
    #[test]
    fn push_child_inner_add_size_gt1_pushes_with_filter() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let parent = node_with_rel(row_id_fk(1, 10), "rel", 3);
        let inner = Change::Add(AddChange {
            node: Node {
                row: row_id(100),
                relationships: IndexMap::new(),
            },
        });
        let change = Change::Child(ChildChange {
            node: parent,
            child: ChildSpec {
                relationship_name: "rel".into(),
                change: Box::new(inner),
            },
        });
        collect_push(&mut e, change);
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Child);
    }

    // Branch: Child inner Remove, size==0, EXISTS ⇒ emit parent Remove.
    #[test]
    fn push_child_inner_remove_size_zero_exists_emits_parent_remove() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        // Post-remove: 0 children.
        let parent = node_with_rel(row_id_fk(1, 10), "rel", 0);
        let removed = Node {
            row: row_id(100),
            relationships: IndexMap::new(),
        };
        let inner = Change::Remove(RemoveChange { node: removed });
        let change = Change::Child(ChildChange {
            node: parent,
            child: ChildSpec {
                relationship_name: "rel".into(),
                change: Box::new(inner),
            },
        });
        collect_push(&mut e, change);
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Remove);
    }

    // Branch: Child inner Remove, size==0, NOT EXISTS ⇒ emit parent Add.
    #[test]
    fn push_child_inner_remove_size_zero_not_exists_emits_parent_add() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::NotExists,
        );
        let parent = node_with_rel(row_id_fk(1, 10), "rel", 0);
        let inner = Change::Remove(RemoveChange {
            node: Node {
                row: row_id(100),
                relationships: IndexMap::new(),
            },
        });
        let change = Change::Child(ChildChange {
            node: parent,
            child: ChildSpec {
                relationship_name: "rel".into(),
                change: Box::new(inner),
            },
        });
        collect_push(&mut e, change);
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Add);
    }

    // Branch: Child inner Remove, size>0 ⇒ push-with-filter.
    #[test]
    fn push_child_inner_remove_size_gt0_pushes_with_filter() {
        let (mut e, shared) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        let parent = node_with_rel(row_id_fk(1, 10), "rel", 2);
        let inner = Change::Remove(RemoveChange {
            node: Node {
                row: row_id(100),
                relationships: IndexMap::new(),
            },
        });
        let change = Change::Child(ChildChange {
            node: parent,
            child: ChildSpec {
                relationship_name: "rel".into(),
                change: Box::new(inner),
            },
        });
        collect_push(&mut e, change);
        let p = shared.pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, ChangeType::Child);
    }

    // Branch: cache key uses JSON.stringify of join-key values.
    #[test]
    fn cache_key_matches_ts_json_stringify_shape() {
        let n = node_with_rel(row_id_fk(1, 10), "rel", 0);
        let key = Exists::cache_key(&n, &vec!["fk".into()]);
        assert_eq!(key, "[10]");
        // Compound key.
        let key2 = Exists::cache_key(&n, &vec!["id".into(), "fk".into()]);
        assert_eq!(key2, "[1,10]");
        // Missing column ⇒ null.
        let key3 = Exists::cache_key(&n, &vec!["absent".into()]);
        assert_eq!(key3, "[null]");
    }

    // Branch: re-entrant push panics with TS message.
    #[test]
    #[should_panic(expected = "Unexpected re-entrancy")]
    fn push_reentrant_panics() {
        // Construct Exists and flip its in_push flag before invoking push.
        let (mut e, _) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        *e.in_push.lock().unwrap() = true;
        let pusher = dummy_pusher();
        let _ = e
            .push(
                Change::Add(AddChange {
                    node: node_with_rel(row_id_fk(1, 10), "rel", 0),
                }),
                &pusher as &dyn InputBase,
            )
            .collect::<Vec<_>>();
    }

    // Branch: in_push=true bypasses cache inside filter (cache neither
    // read nor written).
    #[test]
    fn filter_in_push_bypasses_cache() {
        let (mut e, _rec) = mk_exists(
            vec!["id".into()],
            "rel",
            vec!["fk".into()],
            ExistsType::Exists,
        );
        *e.in_push.lock().unwrap() = true;
        let n = node_with_rel(row_id_fk(1, 10), "rel", 1);
        let _ = e.filter(&n).0.count();
        assert!(e.cache.lock().unwrap().is_empty(), "in_push path must not populate cache");
    }
}
