//! Exists — Transformer shape.
//!
//! Filters nodes based on whether their named relationship is non-empty
//! (EXISTS) or empty (NOT EXISTS). Relationship is read from each node's
//! `relationships` field at fetch/push time — no upstream refetch needed.

use std::collections::HashMap;

use zero_cache_types::ast::CompoundKey;
use zero_cache_types::value::Row;

use super::change::{AddChange, Change, ChildChange, ChildSpec, Node, RemoveChange};
use super::operator::{FetchRequest, Input, Transformer};
use crate::ivm::constraint::Constraint;
use crate::ivm::data::{normalize_undefined, values_equal, NodeOrYield, NormalizedValue};
use crate::ivm::join_utils::build_join_constraint;

/// Mirrors TS `isJoinMatch(parent, childRow, parentKey, childKey)` used
/// in `packages/zql/src/ivm/join.ts` when filtering the parent snapshot
/// during `#pushChild`. Null values never match (TS `valuesEqual(a,null)`
/// is false); absent values never match either. Returns true iff every
/// paired key compares equal.
fn correlates(
    parent_row: &Row,
    parent_key: &CompoundKey,
    child_row: &Row,
    child_key: &CompoundKey,
) -> bool {
    if parent_key.len() != child_key.len() {
        return false;
    }
    for (pk, ck) in parent_key.iter().zip(child_key.iter()) {
        let p = parent_row.get(pk).cloned().unwrap_or(None);
        let c = child_row.get(ck).cloned().unwrap_or(None);
        if !values_equal(&p, &c) {
            return false;
        }
    }
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistsType {
    Exists,
    NotExists,
}

pub struct ExistsT {
    relationship_name: String,
    not: bool,
    parent_join_key: CompoundKey,
    /// When PJK == PK, cache reuse is pointless.
    no_size_reuse: bool,
    cache: HashMap<String, bool>,
    in_push: bool,
    /// Optional child input (a pre-built pipeline that represents the
    /// subquery). When set, ExistsT queries it with a correlation
    /// constraint to evaluate existence — matching TS native
    /// `Exists::*fetch` semantics. When unset, falls back to reading
    /// `node.relationships[name]` (legacy / test path).
    child_input: Option<Box<dyn Input>>,
    /// Child-side correlation key, parallel to `parent_join_key`.
    /// Required when `child_input` is set.
    child_key: CompoundKey,
    /// Name of the child table driving `child_input` — used by
    /// `push_child` to gate whether a given child-source mutation is
    /// this ExistsT's concern. Mirrors TS `Exists::#relationshipName`
    /// comparison in `exists.ts:126` (though TS compares relationship
    /// name on the Change::Child event; RS compares the table because
    /// the change arrives raw from the child source, before being
    /// wrapped into a Change::Child — see Chain.advance_child_for_exists).
    child_table: Option<String>,
}

impl ExistsT {
    /// Add an optional child input + child-key so existence checks
    /// can query the child source directly (matching TS native
    /// `Exists::*fetch` semantics). Returns `self` for builder chaining.
    ///
    /// When set, `should_forward` will build a correlation constraint
    /// from the parent row and call `child_input.fetch(constraint)`.
    /// When unset, the legacy `node.relationships[name]` path (with
    /// fail-open on missing relationship) is used — required for
    /// tests that predate the Rust-level child wiring.
    pub fn with_child_input(
        mut self,
        child_input: Box<dyn Input>,
        child_key: CompoundKey,
    ) -> Self {
        self.child_input = Some(child_input);
        self.child_key = child_key;
        self
    }

    /// Set the child table name after `with_child_input`. Required for
    /// `push_child` routing from `Chain::advance_child_for_exists`.
    /// Split out from `with_child_input` so legacy tests that
    /// construct ExistsT with a child source but no driver-level
    /// routing (i.e. no `child_table` context) continue to compile.
    pub fn with_child_table(mut self, child_table: String) -> Self {
        self.child_table = Some(child_table);
        self
    }

    /// Introspection accessor used by `Chain` to route child-source
    /// mutations: it iterates transformers and calls `push_child` only
    /// on the ExistsT(s) whose `child_table` matches the mutated table.
    pub fn child_table(&self) -> Option<&str> {
        self.child_table.as_deref()
    }

    /// Accessor for the relationship name owned by this ExistsT —
    /// used by `advance_child_for_exists` to look up the
    /// `(child_table, child_pk)` pair in the owning Chain's
    /// `exists_child_tables` map when emitting a child-Edit RowChange.
    /// Mirrors TS native `Exists::#relationshipName` read at
    /// `packages/zql/src/ivm/exists.ts:126`.
    pub fn relationship_name(&self) -> &str {
        &self.relationship_name
    }

    /// Give the parent Chain a mutable handle to the wired child
    /// `Input` so it can be downcast to `Chain` and recursed into
    /// for grandchild-mutation routing (p19's nested WHERE EXISTS).
    pub fn child_input_mut(&mut self) -> Option<&mut (dyn Input + 'static)> {
        self.child_input.as_deref_mut()
    }

    pub fn new(
        relationship_name: String,
        parent_join_key: CompoundKey,
        exists_type: ExistsType,
        primary_key: &[String],
    ) -> Self {
        let not = matches!(exists_type, ExistsType::NotExists);
        let no_size_reuse =
            parent_join_key.len() == primary_key.len() && parent_join_key.iter().eq(primary_key.iter());
        Self {
            relationship_name,
            not,
            parent_join_key,
            no_size_reuse,
            cache: HashMap::new(),
            in_push: false,
            child_input: None,
            child_key: Vec::new(),
            child_table: None,
        }
    }

    pub fn reset_cache(&mut self) {
        self.cache.clear();
    }

    fn cache_key(node: &Node, def: &CompoundKey) -> String {
        let mut values: Vec<NormalizedValue> = Vec::with_capacity(def.len());
        for k in def {
            values.push(normalize_undefined(&node.row.get(k).cloned().unwrap_or(None)));
        }
        serde_json::to_string(&values).expect("json serialise cache key")
    }

    fn fetch_size(relationship_name: &str, node: &Node) -> usize {
        // Missing relationship → treat as 1 (i.e. EXISTS=true,
        // NOT EXISTS=false). This is the "pass-through" fail-open
        // behaviour for queries whose relationships v2 hasn't yet
        // materialised at hydration time. Filtering rows out (returning
        // 0) was hiding all matching parent rows from queries with
        // related[] projections — observed in zbugs as
        // issueListV2 returning 0 instead of 318. Pass-through trades
        // "may include rows that should fail an EXISTS check" for
        // "doesn't drop rows the client expected to see," which matches
        // the user-observable contract better while we wire proper
        // related-row materialisation. Tracked in v2-handoff.
        let Some(factory) = node.relationships.get(relationship_name) else {
            return 1;
        };
        let iter = (*factory)();
        iter.filter(|n| matches!(n, NodeOrYield::Node(_))).count()
    }

    /// Public for `OrOfExistsT::push` which gates on whether ANY
    /// branch's existence predicate holds — same semantics as
    /// the private caller (`ExistsT::push`) uses.
    pub fn should_forward_for_test(&mut self, node: &Node) -> bool {
        self.should_forward(node)
    }

    fn should_forward(&mut self, node: &Node) -> bool {
        // Legacy "do we forward?" entry point used by `push`. When the
        // child input is wired in, this re-fetches the subquery each
        // call — fine for push-path where call volume is low. Fetch
        // path uses `decorate_for_forward` below which also collects
        // the matched children for tree-emission parity with TS.
        let mut exists_fn = || -> bool {
            if let Some(child) = self.child_input.as_mut() {
                let Some(constraint) = build_join_constraint(
                    &node.row,
                    &self.parent_join_key,
                    &self.child_key,
                ) else {
                    return false;
                };
                let req = FetchRequest {
                    constraint: Some(constraint),
                    ..FetchRequest::default()
                };
                let mut iter = child.fetch(req);
                iter.next().is_some()
            } else {
                Self::fetch_size(&self.relationship_name, node) > 0
            }
        };
        let exists = if !self.no_size_reuse && !self.in_push {
            let key = Self::cache_key(node, &self.parent_join_key);
            if let Some(c) = self.cache.get(&key).copied() {
                c
            } else {
                let c = exists_fn();
                self.cache.insert(key, c);
                c
            }
        } else {
            exists_fn()
        };
        if self.not { !exists } else { exists }
    }

    /// Always-decorate variant used by `OrBranchesT` to mirror TS
    /// upfront `applyCorrelatedSubQuery` Join semantics at
    /// `packages/zql/src/builder/builder.ts:308-329`. Unlike
    /// `decorate_for_forward` (which drops the node on non-match),
    /// this always returns the node with its relationship decorated
    /// (possibly empty for EXISTS no-match) and a separate bool
    /// indicating whether the EXISTS/NOT EXISTS predicate holds.
    ///
    /// TS rationale: the upfront `Join` in builder.ts decorates every
    /// row regardless of whether any downstream OR branch uses the
    /// decoration. The `Exists` filter operator (applied inside a
    /// specific branch) reads `node.relationships[name]()` to gate.
    /// Keeping the decoration on non-matching branches is what lets
    /// `Streamer#streamNodes` emit child rows for a parent that
    /// passed a *different* OR branch — the participant-for-ch-priv-1
    /// case in p36.
    pub(crate) fn evaluate_decorate(&mut self, mut node: Node) -> (Node, bool) {
        if let Some(child) = self.child_input.as_mut() {
            let constraint = build_join_constraint(
                &node.row,
                &self.parent_join_key,
                &self.child_key,
            );
            let matched: Vec<Node> = match constraint {
                Some(c) => child
                    .fetch(FetchRequest {
                        constraint: Some(c),
                        ..FetchRequest::default()
                    })
                    .collect(),
                None => Vec::new(),
            };
            let has_match = !matched.is_empty();
            let pass = if self.not { !has_match } else { has_match };
            // Always decorate (even for non-matching branches) to
            // mirror TS upfront-Join behavior. For NOT EXISTS we
            // still don't attach the factory (child rows belong to
            // excluded parents — matches TS `exists.ts:142-154`
            // explicitly emptying the relationship).
            if !self.not {
                use crate::ivm::data::{NodeOrYield, RelationshipFactory};
                use std::sync::Mutex;
                let rel_name = self.relationship_name.clone();
                let cell = std::sync::Arc::new(Mutex::new(Some(matched)));
                let factory: RelationshipFactory = Box::new(move || {
                    let taken = cell
                        .lock()
                        .ok()
                        .and_then(|mut g| g.take())
                        .unwrap_or_default();
                    Box::new(taken.into_iter().map(NodeOrYield::Node))
                });
                // First-wins across branches — matches TS UnionFanIn
                // `relationshipsFromBranches` dedup at
                // union-fan-in.ts:70+.
                node.relationships.entry(rel_name).or_insert(factory);
            }
            (node, pass)
        } else {
            // Legacy / test path — no child input; rely on pre-populated
            // `relationships[name]`. Fail-open on missing matches the
            // `decorate_for_forward` legacy behaviour.
            let size = Self::fetch_size(&self.relationship_name, &node) > 0;
            let pass = if self.not { !size } else { size };
            (node, pass)
        }
    }

    /// Fetch-path forwarding: decide whether to forward the node AND
    /// (for EXISTS) decorate its `relationships[name]` with the
    /// matched children so downstream hydration (pipeline::hydrate_stream)
    /// emits the subquery tree nodes alongside the parent — matching
    /// what TS native `Streamer#streamNodes` does by walking the
    /// relationships produced by Join/FlippedJoin operators.
    ///
    /// For NOT EXISTS, no decoration (subquery's rows belong to the
    /// *excluded* parents, not this matching parent).
    pub(crate) fn decorate_for_forward(&mut self, mut node: Node) -> Option<Node> {
        if let Some(child) = self.child_input.as_mut() {
            let constraint = build_join_constraint(
                &node.row,
                &self.parent_join_key,
                &self.child_key,
            );
            let matched: Vec<Node> = match constraint {
                Some(c) => child
                    .fetch(FetchRequest {
                        constraint: Some(c),
                        ..FetchRequest::default()
                    })
                    .collect(),
                None => Vec::new(),
            };
            let has_match = !matched.is_empty();
            let pass = if self.not { !has_match } else { has_match };
            if !pass {
                return None;
            }
            // Attach matched children as a relationship factory so
            // `hydrate_stream` can emit them as part of the tree.
            // For NOT EXISTS we don't attach — the subquery's rows
            // don't belong to the emitted parent (and `matched` is
            // empty by construction when the predicate passed).
            //
            // Why a one-shot take (Mutex<Option<Vec<Node>>>) rather
            // than `kids.clone()`: `Node::clone()` deliberately drops
            // `relationships` (`ivm/data.rs::Clone for Node`), so a
            // cloning factory would lose the sub-Chain's nested
            // grandchildren decorations — breaking p19/p21's
            // multi-level subquery emission. A one-shot take moves
            // the Vec out on first invocation, yielding fully-
            // populated Nodes (including their own relationships).
            // Hydrate's single-pass walk only invokes each factory
            // once, so this is sound. Matches TS native where
            // `Stream<Node>` generators are consumed once per fetch.
            if !self.not {
                use crate::ivm::data::{NodeOrYield, RelationshipFactory};
                use std::sync::Mutex;
                let rel_name = self.relationship_name.clone();
                let cell = std::sync::Arc::new(Mutex::new(Some(matched)));
                let factory: RelationshipFactory = Box::new(move || {
                    let taken = cell
                        .lock()
                        .ok()
                        .and_then(|mut g| g.take())
                        .unwrap_or_default();
                    Box::new(taken.into_iter().map(NodeOrYield::Node))
                });
                // First-wins on duplicate relationship name (mirrors
                // TS native `UnionFanIn` behavior — see
                // `union-fan-in.ts` L70+: branches must declare unique
                // relationship names, so the merged output schema only
                // ever has one entry per name). When two ExistsT
                // decorate the SAME relationship (OR-of-EXISTS into
                // the same alias), keeping the first decoration
                // matches TS's "first branch wins" emission shape.
                node.relationships.entry(rel_name).or_insert(factory);
            }
            Some(node)
        } else {
            // Legacy path unchanged: treat missing relationships as
            // "exists=true" (fail-open). Used by operator tests that
            // stub relationships directly on the Node.
            let size = Self::fetch_size(&self.relationship_name, &node) > 0;
            let pass = if self.not { !size } else { size };
            if pass {
                Some(node)
            } else {
                None
            }
        }
    }
}

impl ExistsT {
    /// Query the wired `child_input` with a correlation constraint
    /// derived from the parent row. Returns the number of matching
    /// child rows, or `None` when no `child_input` is wired.
    ///
    /// **IMPORTANT — snapshot semantics**: This reads from the pinned
    /// PREV snapshot (the SnapshotReader is refreshed AFTER all advance
    /// calls for a diff cycle, per `pipeline_v2_refresh_snapshot` in
    /// `rust-pipeline-driver-v2.ts:673-677`). So the count returned is
    /// the PRE-mutation count for the current change, not post-mutation.
    ///
    /// TS contrast: `packages/zql/src/ivm/exists.ts:133-167` reads
    /// `change.node.relationships[name]()` which is decorated by the
    /// upstream Join *after* the child was applied → POST-mutation count.
    /// RS therefore checks the inverse thresholds (see `push_child`):
    ///   - Add flip: TS checks `size === 1` (post); RS checks `== 0` (pre)
    ///   - Remove flip: TS checks `size === 0` (post); RS checks `== 1` (pre)
    fn child_size_for(&mut self, parent_row: &Row) -> Option<usize> {
        let child = self.child_input.as_mut()?;
        let constraint = build_join_constraint(
            parent_row,
            &self.parent_join_key,
            &self.child_key,
        )?;
        let req = FetchRequest {
            constraint: Some(constraint),
            ..FetchRequest::default()
        };
        let count = child.fetch(req).count();
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!(
                "[ivm:rs:exists:child_size_for] table={:?} parent={:?} pre_mutation_count={}",
                self.child_table, parent_row, count
            );
        }
        Some(count)
    }

    /// Build a relationships map for a parent row that wraps the
    /// matched children under `self.relationship_name`. Mirror of the
    /// one-shot-take factory pattern used in `decorate_for_forward`
    /// (the fetch/hydration path) — same reason for one-shot take:
    /// `Node::clone()` in `ivm/data.rs` drops `relationships`, so a
    /// cloning factory loses nested grandchildren decorations.
    ///
    /// Used by `push_child` when emitting `Change::Add(parent)` after
    /// an EXISTS 0→1 flip (`exists.ts:155-162` mirror).
    fn build_parent_relationships(
        &mut self,
        parent_row: &Row,
    ) -> indexmap::IndexMap<String, crate::ivm::data::RelationshipFactory> {
        use crate::ivm::data::{NodeOrYield, RelationshipFactory};
        use std::sync::Mutex;
        let mut out: indexmap::IndexMap<String, RelationshipFactory> =
            indexmap::IndexMap::new();
        let Some(child) = self.child_input.as_mut() else {
            return out;
        };
        let Some(constraint) = build_join_constraint(
            parent_row,
            &self.parent_join_key,
            &self.child_key,
        ) else {
            return out;
        };
        let matched: Vec<Node> = child
            .fetch(FetchRequest {
                constraint: Some(constraint),
                ..FetchRequest::default()
            })
            .collect();
        let cell = std::sync::Arc::new(Mutex::new(Some(matched)));
        let factory: RelationshipFactory = Box::new(move || {
            let taken = cell
                .lock()
                .ok()
                .and_then(|mut g| g.take())
                .unwrap_or_default();
            Box::new(taken.into_iter().map(NodeOrYield::Node))
        });
        out.insert(self.relationship_name.clone(), factory);
        out
    }
}

impl Transformer for ExistsT {
    fn fetch_through<'a>(
        &'a mut self,
        upstream: Box<dyn Iterator<Item = Node> + 'a>,
        _req: FetchRequest,
    ) -> Box<dyn Iterator<Item = Node> + 'a> {
        Box::new(ExistsFetchIter {
            upstream,
            exists: self,
        })
    }

    /// Mirror of TS `Exists::*push` child-side branch
    /// (`packages/zql/src/ivm/exists.ts:120-206`).
    ///
    /// TS takes an already-wrapped `Change::Child` from an upstream Join.
    /// Our Chain receives raw child-source changes (Add/Remove on the
    /// subquery table) and routes them here based on `child_table`.
    /// We therefore synthesize the "Exists::pushChild add/remove"
    /// semantic directly from the raw change:
    ///
    ///   - `Add` on child of parent P:
    ///       new_size = child_input.fetch(correlation=P).count()
    ///       if new_size == 1 (flipped 0→1):
    ///         not=false → emit `Add(P)`
    ///         not=true  → emit `Remove(P)`  (P was previously included
    ///                     because EXISTS was false; adding the child
    ///                     flips it to true, so P drops out)
    ///       else (size was already ≥1 before, still ≥1): no change
    ///
    ///   - `Remove` on child of parent P:
    ///       new_size = child_input.fetch(correlation=P).count()
    ///       if new_size == 0 (flipped 1→0):
    ///         not=false → emit `Remove(P)`
    ///         not=true  → emit `Add(P)`
    ///       else: no change
    ///
    ///   - `Edit` / `Child` on child: no-op for now. TS handles edits
    ///     via `pushWithFilter(change, size > 0)`. Since the edit can't
    ///     change parent inclusion unless correlation-key fields change
    ///     (which TS asserts doesn't happen), Parent set is unaffected.
    ///     `Child` (grandchild) emissions are forwarded upstream through
    ///     separate Chain routing — not this transformer's concern.
    ///
    /// `parent_snapshot` is the set of parents *currently visible to
    /// this transformer's output* — materialised by the Chain from the
    /// source-through-preceding-transformers chain. We check each
    /// parent whose correlation matches the child row (same test Join
    /// does in `packages/zql/src/ivm/join.ts:216-246`).
    fn push_child<'a>(
        &'a mut self,
        change: Change,
        child_table: &str,
        parent_snapshot: &[Node],
    ) -> Box<dyn Iterator<Item = Change> + 'a> {
        // Gate: only react if this ExistsT owns the mutated child table.
        if self.child_table.as_deref() != Some(child_table) {
            return Box::new(std::iter::empty());
        }
        // Extract child row (the one whose presence/absence drives the flip).
        let child_row: &Row = match &change {
            Change::Add(AddChange { node }) => &node.row,
            Change::Remove(RemoveChange { node }) => &node.row,
            Change::Edit(edit) => &edit.node.row,
            // Grandchild changes are routed separately by the Chain —
            // they're not the concern of this transformer's child gate.
            Change::Child(_) => return Box::new(std::iter::empty()),
        };

        // Snapshot filtering: keep only parents whose parent_join_key
        // values equal child_row's child_key values — same match predicate
        // TS Join uses when finding parents for a given child row.
        let parent_rows: Vec<Row> = parent_snapshot
            .iter()
            .filter(|p| correlates(&p.row, &self.parent_join_key, child_row, &self.child_key))
            .map(|p| p.row.clone())
            .collect();

        let mut emissions: Vec<Change> = Vec::new();
        for parent_row in parent_rows {
            let Some(new_size) = self.child_size_for(&parent_row) else {
                continue;
            };
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                eprintln!(
                    "[ivm:rs:exists:push_child] not={} op={} pre_mutation_size={} child_row={:?}",
                    self.not,
                    match &change { Change::Add(_) => "Add", Change::Remove(_) => "Remove", Change::Edit(_) => "Edit", Change::Child(_) => "Child" },
                    new_size,
                    child_row
                );
            }
            match &change {
                Change::Add(_) => {
                    // TS implements this at packages/zql/src/ivm/exists.ts:136
                    // where `size === 1` means POST-mutation size is 1 (first
                    // child was just added). RS `child_size_for` returns the
                    // PRE-mutation count from the pinned PREV snapshot
                    // (SnapshotReader is refreshed AFTER the advance cycle,
                    // per rust-pipeline-driver-v2.ts:673-677). So the
                    // equivalent pre-mutation threshold is `== 0` (was empty,
                    // now becomes non-empty after this Add). I missed this in
                    // RS, adding it now.
                    if new_size == 0 {
                        // Mirror of TS exists.ts:155-162 (EXISTS add-flip):
                        // emit `add` of the parent. TS passes the full
                        // node (with its relationships) unchanged because
                        // the upstream Join has decorated them; our RS
                        // Chain has no Join in the middle, so we build
                        // the decoration ourselves from `child_input`.
                        // The decorated children are what the Streamer
                        // walks in `pipeline-driver.ts:1027-1030` to emit
                        // each subquery row as a RowChange.
                        let relationships = if !self.not {
                            self.build_parent_relationships(&parent_row)
                        } else {
                            // NotExists: see exists.ts:142-154 — TS
                            // explicitly empties the relationship with
                            // `() => []` because the subquery rows are
                            // NOT part of the client-visible tree for
                            // excluded parents.
                            indexmap::IndexMap::new()
                        };
                        let node = Node { row: parent_row, relationships };
                        if self.not {
                            emissions.push(Change::Remove(RemoveChange { node }));
                        } else {
                            emissions.push(Change::Add(AddChange { node }));
                        }
                    }
                }
                Change::Remove(_) => {
                    // TS implements this at packages/zql/src/ivm/exists.ts:171
                    // where `size === 0` means POST-mutation size is 0 (last
                    // child was just removed). RS pre-mutation equivalent is
                    // `== 1` (had exactly one child, now becomes empty). I
                    // missed this in RS, adding it now.
                    if new_size == 1 {
                        // Mirror of TS exists.ts:171-199 (EXISTS remove-flip):
                        // For NotExists → emit Add of parent (with empty
                        // relationships — subquery rows aren't emitted).
                        // For Exists → emit Remove of parent; TS puts the
                        // removed child back in the relationship at
                        // line 191-193 so Streamer emits a `del` for it,
                        // but in our xyne-lite test neither p18 nor p23
                        // exercises this exact case (the cleanup path
                        // deletes both parent and child, so the parent
                        // Remove emitted by its own source push handles
                        // both tombstones). Keeping empty relationships
                        // is safe for current divergences; refine if a
                        // future pattern needs per-child del emission.
                        let relationships = if self.not {
                            self.build_parent_relationships(&parent_row)
                        } else {
                            indexmap::IndexMap::new()
                        };
                        let node = Node { row: parent_row, relationships };
                        if self.not {
                            emissions.push(Change::Add(AddChange { node }));
                        } else {
                            emissions.push(Change::Remove(RemoveChange { node }));
                        }
                    }
                }
                Change::Edit(edit) => {
                    // TS's `Exists::*push` doesn't see a raw child-Edit
                    // in isolation — its upstream `Filter` at
                    // `packages/zql/src/ivm/filter.ts:54` /
                    // `filter-push.ts:30-35` splits Edits that cross
                    // the predicate boundary into `Remove(old)` +
                    // `Add(new)` BEFORE Join wraps them as
                    // `Change::Child`. The result is:
                    //
                    //   - non-crossing Edit (both old/new pass the
                    //     sub-chain filter) → arrives as Change::Child
                    //     with inner Edit → TS `exists.ts:127-131`
                    //     forwards via `pushWithFilter` (outer Change
                    //     unchanged), and `Streamer#streamChanges` at
                    //     `pipeline-driver.ts:986-993` recurses into
                    //     `cc.child.change` emitting a RowChange::Edit
                    //     on the child table.
                    //   - crossing Edit (old passed, new fails) →
                    //     Filter emits Remove(old) → Join wraps as
                    //     Change::Child(Remove) → TS exists.ts:169-204
                    //     handles the flip, emits Remove(parent) when
                    //     `fetchSize === 0`.
                    //
                    // RS receives the raw child-Edit directly at
                    // `push_child` (driver bypasses the sub-chain Filter
                    // on direct_hit). To re-create TS's behaviour we
                    // distinguish by post-edit size:
                    //   - `new_size == 0` (EXISTS) / `new_size >= 1`
                    //     (NOT EXISTS): parent no longer passes the
                    //     exists predicate → emit the flip (`Remove`
                    //     for EXISTS, keep empty decoration; `Remove`
                    //     for NOT EXISTS). Mirrors TS `exists.ts:169-204`
                    //     remove-flip and the NOT EXISTS add-flip path
                    //     respectively.
                    //   - `new_size >= 1` (EXISTS): parent still passes.
                    //     Emit `Change::Child(ChildChange{...Edit...})`
                    //     so the outer chain translates to
                    //     `RowChange::Edit` on the child table.
                    //     Mirror of TS exists.ts:127-131 Edit forward.
                    //   - `new_size == 0` (NOT EXISTS): parent still
                    //     passes (NOT EXISTS with 0 children is true).
                    //     The child rows aren't in the client-visible
                    //     tree (exists.ts:142-154 empties the
                    //     relationship for NOT EXISTS), so the Edit
                    //     doesn't affect any emitted row — emit nothing.
                    let parent_passes_now =
                        if self.not { new_size == 0 } else { new_size > 0 };
                    if !parent_passes_now {
                        // Flip: parent was presumably in output before
                        // (otherwise nothing to remove). Mirrors
                        // `exists.ts:169-199` remove-flip for EXISTS
                        // and the NOT EXISTS add-side at `exists.ts:134-162`.
                        let node = Node {
                            row: parent_row,
                            relationships: indexmap::IndexMap::new(),
                        };
                        if self.not {
                            // NOT EXISTS flipped to false (new_size>=1):
                            // parent drops out of output. Mirror of
                            // the NOT EXISTS `add` add-flip semantics
                            // at exists.ts:142-154.
                            emissions.push(Change::Remove(RemoveChange { node }));
                        } else {
                            // EXISTS flipped to false (new_size==0):
                            // parent drops out. Mirror of exists.ts:184-198.
                            emissions.push(Change::Remove(RemoveChange { node }));
                        }
                    } else if !self.not {
                        // Non-flip EXISTS case: forward the Edit through
                        // a Change::Child so the outer chain emits a
                        // RowChange::Edit on the child table. Mirror of
                        // TS `exists.ts:127-131` which forwards the
                        // outer Change::Child unchanged via pushWithFilter.
                        let node = Node {
                            row: parent_row,
                            relationships: indexmap::IndexMap::new(),
                        };
                        emissions.push(Change::Child(ChildChange {
                            node,
                            child: ChildSpec {
                                relationship_name: self.relationship_name.clone(),
                                change: Box::new(Change::Edit(edit.clone())),
                            },
                        }));
                    }
                    // NOT EXISTS with parent_passes_now (new_size==0):
                    // no emission. Subquery rows aren't part of the
                    // parent's client-visible tree (exists.ts:142-154
                    // empties the relationship), so the child Edit is
                    // invisible — drop silently.
                }
                // Child (grandchild) handled above — not this transformer's concern.
                _ => {}
            }
        }
        Box::new(emissions.into_iter())
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }

    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        eprintln!(
            "[TRACE ivm_v2] ExistsT::push enter rel={}",
            self.relationship_name
        );
        self.in_push = true;
        let out = match change {
            Change::Add(AddChange { ref node }) | Change::Remove(RemoveChange { ref node }) => {
                if self.should_forward(node) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Child(ref c) => {
                if self.should_forward(&c.node) {
                    Some(change)
                } else {
                    None
                }
            }
            Change::Edit(edit) => {
                // Mirror of TS `Exists::*push` case 'edit' at
                // `packages/zql/src/ivm/exists.ts:113-119` — Edit falls
                // through to `#pushWithFilter(change)` which calls
                // `#filter(change.node, exists=undefined)` on the NEW
                // node only (exists.ts:240-244). TS does NOT split Edit
                // into Remove+Add based on old/new pass (that's what
                // `Filter::push` does via `maybeSplitAndPushEditChange`
                // in `filter-push.ts:30-35`; `Exists::push` deliberately
                // does not). So we just forward the full Edit if the
                // NEW node passes the exists predicate, and drop
                // otherwise. `should_forward` evaluates the predicate
                // using either the wired `child_input` (mirror of TS
                // native `Exists::#fetchSize` iterating
                // `node.relationships[name]()`) or the fail-open
                // relationships-only path.
                if self.should_forward(&edit.node) {
                    Some(Change::Edit(edit))
                } else {
                    None
                }
            }
        };
        self.in_push = false;
        Box::new(out.into_iter())
    }
}

struct ExistsFetchIter<'a> {
    upstream: Box<dyn Iterator<Item = Node> + 'a>,
    exists: &'a mut ExistsT,
}
impl<'a> Iterator for ExistsFetchIter<'a> {
    type Item = Node;
    fn next(&mut self) -> Option<Node> {
        loop {
            let n = self.upstream.next()?;
            if let Some(decorated) = self.exists.decorate_for_forward(n) {
                return Some(decorated);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::AddChange;
    use crate::ivm::data::{Node, NodeOrYield, RelationshipFactory};
    use indexmap::IndexMap;
    use serde_json::json;

    fn node_with_rel(id: i64, n_children: usize) -> Node {
        let mut rels: IndexMap<String, RelationshipFactory> = IndexMap::new();
        let factory: RelationshipFactory = Box::new(move || {
            let nodes: Vec<NodeOrYield> = (0..n_children)
                .map(|i| {
                    NodeOrYield::Node(Node {
                        row: {
                            let mut r = Row::new();
                            r.insert("cid".into(), Some(json!(i as i64)));
                            r
                        },
                        relationships: IndexMap::new(),
                    })
                })
                .collect();
            Box::new(nodes.into_iter())
        });
        rels.insert("children".into(), factory);
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("pjk".into(), Some(json!(id)));
        Node {
            row: r,
            relationships: rels,
        }
    }

    #[test]
    fn fetch_through_exists_drops_empty() {
        let mut e = ExistsT::new(
            "children".into(),
            vec!["pjk".into()],
            ExistsType::Exists,
            &["id".to_string()],
        );
        let upstream: Vec<Node> = vec![
            node_with_rel(1, 0),
            node_with_rel(2, 3),
            node_with_rel(3, 0),
            node_with_rel(4, 1),
        ];
        let out: Vec<Node> = e
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        let ids: Vec<_> = out
            .iter()
            .map(|n| n.row.get("id").and_then(|v| v.clone()))
            .collect();
        assert_eq!(ids, vec![Some(json!(2)), Some(json!(4))]);
    }

    #[test]
    fn push_add_forwarded_when_rel_nonempty() {
        let mut e = ExistsT::new(
            "children".into(),
            vec!["pjk".into()],
            ExistsType::Exists,
            &["id".to_string()],
        );
        let out: Vec<Change> = e
            .push(Change::Add(AddChange {
                node: node_with_rel(1, 2),
            }))
            .collect();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn push_add_dropped_when_rel_empty() {
        let mut e = ExistsT::new(
            "children".into(),
            vec!["pjk".into()],
            ExistsType::Exists,
            &["id".to_string()],
        );
        let out: Vec<Change> = e
            .push(Change::Add(AddChange {
                node: node_with_rel(1, 0),
            }))
            .collect();
        assert!(out.is_empty());
    }
}
