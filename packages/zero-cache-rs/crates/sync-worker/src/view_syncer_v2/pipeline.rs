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
    /// All `ast.related[]` entries lowered to Joins, in declaration
    /// order. Mirrors TS `buildPipelineInternal` at
    /// `packages/zql/src/builder/builder.ts:347-355` which iterates
    /// every `csq` in `ast.related` and wraps the current Input with
    /// a `new Join({parent: end, child, ...})` via
    /// `applyCorrelatedSubQuery` (builder.ts:611-647). Each entry may
    /// carry its own sub-AST so the driver can recursively build a
    /// sub-Chain — matching the recursive `buildPipelineInternal`
    /// call on `sq.subquery` at builder.ts:626.
    pub joins: Vec<JoinSpec>,
    /// Top-level OR branches — TS mirror:
    /// `packages/zql/src/builder/builder.ts:376-442` `applyFilterWithFlips`
    /// OR path wraps each branch in its own recursive filter pipeline
    /// (`applyFilterWithFlips` recursed on the branch condition) and
    /// unions them via `UnionFanOut + UnionFanIn`. Each `OrBranchSpec`
    /// captures a branch's scalar predicate + list of EXISTS conditions
    /// AND'd together within the branch. When this is populated,
    /// `exists`/`exists_chain` are empty and the Chain builds a single
    /// `OrBranchesT` transformer that emits rows passing ANY branch,
    /// with the first passing branch's decoration (matches TS
    /// `UnionFanIn.fetch` → `mergeFetches` first-wins-on-duplicate-PK
    /// at union-fan-in.ts:208).
    pub or_branches: Vec<OrBranchSpec>,
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
    /// Full child subquery AST — mirrors what TS hands to
    /// `buildPipelineInternal(sq.subquery, ...)` at
    /// `packages/zql/src/builder/builder.ts:626`. The driver reads
    /// this to build a recursive sub-Chain per join, preserving
    /// nested `related[]` / `whereExists` / `where` semantics
    /// exactly like TS's recursive pipeline construction. `None`
    /// means "use a plain source for the child table" — used by
    /// legacy tests that construct a JoinSpec manually.
    pub child_subquery: Option<Box<zero_cache_types::ast::AST>>,
    /// Child-side primary key — filled by the driver after looking
    /// it up in the replica's table registry. Needed by
    /// `advance_child` when emitting child `RowChange`s so the
    /// row_key matches the child table's actual PK.
    pub child_primary_key: Option<PrimaryKey>,
}

/// Single branch of a top-level OR filter. TS mirror:
/// `packages/zql/src/builder/builder.ts:439` — each OR branch is
/// recursively passed to `applyFilterWithFlips(end, cond, ...)` which
/// produces a full sub-pipeline. In RS scope, a branch is represented
/// as a scalar predicate (all non-EXISTS conditions AND'd together)
/// PLUS a list of EXISTS conditions AND'd within the branch.
///
/// At fetch time, a row passes a branch iff
///   predicate(row) && every exists in `exists` matches.
/// If the row passes, the first-matching branch's EXISTS decorations
/// are attached to the emitted node (TS `mergeFetches`
/// first-wins-on-duplicate-PK at union-fan-in.ts:208).
pub struct OrBranchSpec {
    /// Scalar predicate compiled from the non-EXISTS sub-conditions of
    /// this branch (AND'd). `None` means "no scalar gate — branch is
    /// satisfied purely by its EXISTS list".
    pub predicate: Option<crate::ivm_v2::filter_t::Predicate>,
    /// EXISTS conditions to AND within this branch. May be empty if
    /// the branch is pure scalar.
    pub exists: Vec<ExistsSpec>,
    /// When true, this branch was produced by the TS planner's
    /// "flip" path — `applyFilterWithFlips` at
    /// `packages/zql/src/builder/builder.ts:410-448`. In that path,
    /// TS wraps the branch's inner CSQ in a `FlippedJoin` and unions
    /// branches with a `UnionFanIn` whose `fetch` calls `mergeFetches`
    /// at `packages/zql/src/ivm/union-fan-in.ts:218-300`. `mergeFetches`
    /// merge-sorts each branch's ordered row stream and skips rows
    /// whose PK duplicates the previously-yielded row (L260-265) —
    /// yielding each PK at most once (FIRST-WINS on duplicate PK).
    ///
    /// The non-flip TS path, by contrast, uses `applyOr` at
    /// `builder.ts:514-557` which stacks filters and emits a row if
    /// ANY branch's filter passes; that's the current union-all
    /// behavior here. `flip_mode` toggles between them: `true` =
    /// dedup-by-PK (mirrors mergeFetches), `false` = union-all (the
    /// existing non-flip path).
    ///
    /// The AST-builder sets this per-branch from the TS planner flag
    /// (`flip: true` on every CSQ in every branch of this OR). At
    /// runtime, all branches share the same `flip_mode` — it's a
    /// query-level decision; we store it per-branch for locality.
    pub flip_mode: bool,
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
    /// Hierarchical joins — one per `ast.related[]` entry, in
    /// declaration order. Mirrors TS `buildPipelineInternal` wrapping
    /// the current Input with a `new Join(...)` for each CSQ (see
    /// `packages/zql/src/builder/builder.ts:347-355`). Each
    /// `JoinStage.child_source` may itself be a Chain (built
    /// recursively by the driver) — that's what lets deeper nested
    /// `.related().related().related()` work, mirroring TS's
    /// `buildPipelineInternal(sq.subquery, ...)` recursion.
    joins: Vec<JoinStage>,
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
    /// Child-side primary key — populated from the driver so
    /// `advance_child` can emit `RowChange`s keyed by the correct
    /// column set. Previously the Chain relied on
    /// `child_source.get_schema().primary_key` which is correct for
    /// the immediate child but not for grandchildren when the
    /// child_source is itself a Chain that wraps a further Chain.
    child_primary_key: PrimaryKey,
}

impl Chain {
    /// Build without a Join — existing single-table chain.
    pub fn build(spec: ChainSpec, source: Box<dyn Input>) -> Self {
        Self::build_with_join(spec, source, None)
    }

    /// Back-compat shim for tests that hand-roll a single-level Join
    /// — forwards to the new multi-join variant with a single-entry
    /// (or empty) Vec.
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
    ///
    /// `child_source` is legacy single-join back-compat: if set AND
    /// `spec.joins.len() >= 1`, it's paired with `spec.joins[0]`.
    /// For the multi-join + recursive case (driver-built sub-Chains),
    /// use `build_with_joins_vec` which takes a parallel `Vec` of
    /// child inputs.
    pub fn build_with_join_and_exists(
        spec: ChainSpec,
        source: Box<dyn Input>,
        mut child_source: Option<Box<dyn Input>>,
        exists_child_inputs: Vec<Option<Box<dyn Input>>>,
    ) -> Self {
        // Pair the single legacy `child_source` with spec.joins[0]
        // (if both are present); subsequent joins get `None`.
        let mut join_child_inputs: Vec<Option<Box<dyn Input>>> =
            Vec::with_capacity(spec.joins.len());
        for i in 0..spec.joins.len() {
            join_child_inputs.push(if i == 0 { child_source.take() } else { None });
        }
        Self::build_with_joins_vec(
            spec,
            source,
            join_child_inputs,
            exists_child_inputs,
        )
    }

    /// Multi-join builder entry point. `join_child_inputs[i]` feeds
    /// `spec.joins[i]`. Each child input may itself be a Chain
    /// (recursively built by the driver) — that's what lets nested
    /// `.related().related()` work, mirroring TS's
    /// `buildPipelineInternal(sq.subquery, ...)` recursion.
    /// Shim for call sites that don't build OR-of-EXISTS branches.
    /// Forwards with an empty `or_branch_child_inputs` vec.
    pub fn build_with_joins_vec(
        spec: ChainSpec,
        source: Box<dyn Input>,
        join_child_inputs: Vec<Option<Box<dyn Input>>>,
        exists_child_inputs: Vec<Option<Box<dyn Input>>>,
    ) -> Self {
        Self::build_full(
            spec,
            source,
            join_child_inputs,
            exists_child_inputs,
            Vec::new(),
        )
    }

    /// Full builder — accepts one `Option<Box<dyn Input>>` per entry
    /// in `spec.joins` (position i → joins[i]), `spec.exists` + each
    /// `spec.exists_chain` entry (position 0 is exists, 1.. are
    /// chain), and a nested `Vec<Vec<...>>` for OR branches (outer
    /// index = branch, inner = per-branch `ExistsSpec` position).
    pub fn build_full(
        mut spec: ChainSpec,
        mut source: Box<dyn Input>,
        mut join_child_inputs: Vec<Option<Box<dyn Input>>>,
        exists_child_inputs: Vec<Option<Box<dyn Input>>>,
        or_branch_child_inputs: Vec<Vec<Option<Box<dyn Input>>>>,
    ) -> Self {
        let joins_spec = std::mem::take(&mut spec.joins);
        let order_by = spec.order_by.take();
        if let Some(order_by) = order_by {
            if let Some(sqlite) = source
                .as_any_mut()
                .and_then(|a| a.downcast_mut::<crate::view_syncer_v2::sqlite_source::SqliteSource>())
            {
                sqlite.set_sort(order_by);
            }
        }
        let mut join_stages: Vec<JoinStage> = Vec::with_capacity(joins_spec.len());
        // Parallel-iter joins spec + child inputs (padded with None).
        join_child_inputs.resize_with(joins_spec.len(), || None);
        for (js, maybe_child) in joins_spec.into_iter().zip(join_child_inputs.into_iter()) {
            let Some(child) = maybe_child else {
                // Join spec without a child input means the driver
                // couldn't build one (e.g. table missing). Skip — the
                // hydration/advance paths check `joins` lazily.
                continue;
            };
            // Resolve child_primary_key: spec-provided (driver looked it
            // up) wins; fall back to the child source's own schema.
            let child_pk = js
                .child_primary_key
                .clone()
                .unwrap_or_else(|| child.get_schema().primary_key.clone());
            join_stages.push(JoinStage {
                transformer: Box::new(JoinT::new(
                    js.parent_key,
                    js.child_key,
                    js.relationship_name,
                )),
                child_source: child,
                child_table: js.child_table,
                child_primary_key: child_pk,
            });
        }
        let mut chain = Self::build_inner_full(
            spec,
            source,
            exists_child_inputs,
            or_branch_child_inputs,
        );
        chain.joins = join_stages;
        chain
    }

    fn build_inner(
        spec: ChainSpec,
        source: Box<dyn Input>,
        exists_child_inputs: Vec<Option<Box<dyn Input>>>,
    ) -> Self {
        Self::build_inner_full(spec, source, exists_child_inputs, Vec::new())
    }

    fn build_inner_full(
        spec: ChainSpec,
        source: Box<dyn Input>,
        mut exists_child_inputs: Vec<Option<Box<dyn Input>>>,
        mut or_branch_child_inputs: Vec<Vec<Option<Box<dyn Input>>>>,
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
                // Tag the ExistsT with its child table so
                // Chain::advance_child_for_exists can route child-source
                // mutations to the correct transformer. Required by the
                // push_child wiring that mirrors TS `Exists::*push` child
                // branch (`packages/zql/src/ivm/exists.ts:120-206`).
                if let Some(ref tbl) = es.child_table {
                    t = t.with_child_table(tbl.clone());
                }
            }
            transformers.push(Box::new(t));
        }

        // Top-level OR branches — TS mirror: `applyFilterWithFlips`
        // OR path at builder.ts:376-442 recursively builds a full
        // sub-pipeline per branch and unions via `UnionFanOut +
        // UnionFanIn`. Each `OrBranchSpec` carries the branch's
        // scalar predicate + its Vec<ExistsSpec> (AND'd within the
        // branch). We build one `OrBranch` per spec and wrap into a
        // single `OrBranchesT` that emits rows passing ANY branch
        // with the first-passing branch's decoration (matches
        // `mergeFetches` first-wins at union-fan-in.ts:208).
        if !spec.or_branches.is_empty() {
            or_branch_child_inputs.resize_with(spec.or_branches.len(), Vec::new);
            // Flip-mode is a query-level toggle: if ANY branch was
            // flagged flip-mode by the AST builder (which in turn
            // mirrors the TS planner's `flip: true` decision at
            // `packages/zql/src/planner/planner-builder.ts:251-254`
            // applied via `applyPlansToAST` at :322-355), the whole
            // `UnionFanIn` runs in mergeFetches mode. TS's
            // `applyFilterWithFlips` OR path at
            // `packages/zql/src/builder/builder.ts:410-448`
            // constructs a single `UnionFanIn(ufo, branches)` per OR,
            // so the mode applies to the union as a whole. The AST
            // builder sets `flip_mode` identically on every branch
            // when the flip criterion holds (every branch is a leaf
            // EXISTS), so we just read branch[0].
            let flip_mode = spec.or_branches.iter().any(|b| b.flip_mode);
            let mut branches: Vec<crate::ivm_v2::or_exists_t::OrBranch> = Vec::new();
            for (branch_spec, mut branch_child_inputs) in spec
                .or_branches
                .into_iter()
                .zip(or_branch_child_inputs.into_iter())
            {
                branch_child_inputs
                    .resize_with(branch_spec.exists.len(), || None);
                let mut branch_exists: Vec<crate::ivm_v2::exists_t::ExistsT> = Vec::new();
                for (es, maybe_child) in branch_spec
                    .exists
                    .into_iter()
                    .zip(branch_child_inputs.into_iter())
                {
                    if let (Some(tbl), Some(pk)) =
                        (es.child_table.clone(), es.child_primary_key.clone())
                    {
                        let nested = es
                            .child_exists_child_tables
                            .clone()
                            .unwrap_or_default();
                        exists_child_tables
                            .0
                            .entry(es.relationship_name.clone())
                            .or_insert((tbl, pk, Box::new(nested)));
                    }
                    let mut t = crate::ivm_v2::exists_t::ExistsT::new(
                        es.relationship_name,
                        es.parent_join_key,
                        es.exists_type,
                        &pk_cols,
                    );
                    if let Some(child_input) = maybe_child {
                        t = t.with_child_input(child_input, es.child_key);
                        if let Some(ref tbl) = es.child_table {
                            t = t.with_child_table(tbl.clone());
                        }
                    }
                    branch_exists.push(t);
                }
                branches.push(crate::ivm_v2::or_exists_t::OrBranch {
                    predicate: branch_spec.predicate,
                    exists: branch_exists,
                });
            }
            // Build the OrBranchesT transformer. In flip-mode, wire
            // the PK cols so the fetch iterator can dedup rows by
            // primary key (mirrors TS `mergeFetches` skipping rows
            // whose PK equals the previously-yielded row's PK at
            // `packages/zql/src/ivm/union-fan-in.ts:260-265`). In
            // non-flip mode the transformer keeps its current
            // union-all behaviour (mirrors TS `applyOr` at
            // `packages/zql/src/builder/builder.ts:514-557`).
            let or_t = if flip_mode {
                crate::ivm_v2::or_exists_t::OrBranchesT::new(branches)
                    .with_flip_mode(pk_cols.clone())
            } else {
                crate::ivm_v2::or_exists_t::OrBranchesT::new(branches)
            };
            transformers.push(Box::new(or_t));
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
            joins: Vec::new(),
            exists_child_tables,
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    /// First-level child tables wired as `Join`s from `ast.related[]`.
    /// A mutation on any of these routes through `advance_child`.
    pub fn child_tables(&self) -> Vec<&str> {
        self.joins.iter().map(|j| j.child_table.as_str()).collect()
    }

    /// Back-compat single-child accessor — returns the first join's
    /// child table. Kept so existing callers compile; new code should
    /// use `child_tables()` for multi-join queries.
    pub fn child_table(&self) -> Option<&str> {
        self.joins.first().map(|j| j.child_table.as_str())
    }

    /// Recursive list of every child-table reachable through nested
    /// joins (any depth). Driver uses this to route grandchild-table
    /// mutations into the owning top-level chain. Mirrors how TS
    /// `buildPipelineInternal` ends up with a tree of Join operators
    /// each watching its own child source — any mutation on one of
    /// those child tables propagates up to the top-level Streamer.
    pub fn join_tables_recursive(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        for js in self.joins.iter_mut() {
            out.push(js.child_table.clone());
            if let Some(sub) = js
                .child_source
                .as_any_mut()
                .and_then(|a| a.downcast_mut::<Chain>())
            {
                out.extend(sub.join_tables_recursive());
            }
        }
        out
    }

    /// Recursive `advance_child` — mirrors TS `Join::#pushChild` at
    /// `packages/zql/src/ivm/join.ts:190-251` propagating up through
    /// every nested Join until the root. Algorithm:
    ///   1. If any of THIS chain's `joins[*].child_table == table`,
    ///      delegate to `advance_child(table, change)` (existing
    ///      single-level path) — that emits the new leaf row.
    ///   2. Otherwise, for each `joins[i]` whose child_source is a
    ///      Chain, recurse. Any RowChanges the sub-chain produces
    ///      are propagated up unchanged — the new leaf's table is
    ///      preserved, same as TS `Streamer#streamChanges` recursing
    ///      into `cc.child.change` with the child schema.
    pub fn advance_child_recursive(
        &mut self,
        table: &str,
        change: Change,
    ) -> Vec<RowChange> {
        if self.joins.iter().any(|j| j.child_table == table) {
            return self.advance_child(table, change);
        }
        let mut out = Vec::new();
        for js in self.joins.iter_mut() {
            let Some(sub) = js
                .child_source
                .as_any_mut()
                .and_then(|a| a.downcast_mut::<Chain>())
            else {
                continue;
            };
            out.extend(sub.advance_child_recursive(table, change.clone()));
        }
        out
    }

    /// Advance child-side. A change to a child table flows through
    /// the matching `JoinT::push_child`, emitting a `ChildChange`
    /// per matching parent in the current parent snapshot. Mirrors
    /// TS `Streamer#streamChanges` 'child' case in
    /// `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:964-972`:
    /// we recurse into `cc.child.change` with the child schema and
    /// emit the new child row as a RowChange.
    pub fn advance_child(&mut self, table: &str, change: Change) -> Vec<RowChange> {
        // Locate the JoinStage whose child_table matches.
        let Some(idx) = self.joins.iter().position(|j| j.child_table == table) else {
            return Vec::new();
        };
        let join = &mut self.joins[idx];
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
        let query_id = self.query_id.clone();
        let child_table = join.child_table.clone();
        let child_pk = join.child_primary_key.clone();
        let mut out: Vec<RowChange> = Vec::new();
        for c in emissions {
            // TS mirror: `Streamer#streamChanges` case 'child' at
            // `pipeline-driver.ts:964-972` — on `Change::Child`, recurse
            // into `cc.child.change` with the CHILD schema. For an Add
            // nested change, that emits the new child row as a
            // RowChange with the child's table name. We do the same
            // here — the parent's row_key isn't needed since the
            // client's `rowsPatch` is flat (server emits per-row ops
            // keyed by table+pk; client applies them into its row map).
            let Change::Child(cc) = c else { continue };
            match *cc.child.change {
                Change::Add(a) => {
                    out.push(RowChange::Add(RowAdd {
                        query_id: query_id.clone(),
                        table: child_table.clone(),
                        row_key: build_row_key(&child_pk, &a.node.row),
                        row: a.node.row,
                    }));
                }
                Change::Remove(r) => {
                    out.push(RowChange::Remove(RowRemove {
                        query_id: query_id.clone(),
                        table: child_table.clone(),
                        row_key: build_row_key(&child_pk, &r.node.row),
                    }));
                }
                Change::Edit(e) => {
                    out.push(RowChange::Edit(super::row_change::RowEdit {
                        query_id: query_id.clone(),
                        table: child_table.clone(),
                        row_key: build_row_key(&child_pk, &e.node.row),
                        row: e.node.row,
                    }));
                }
                // Nested Change::Child (grandchild mutation arriving
                // at a 2-level related). Requires recursive streamer
                // over nested schemas. Current single-JoinSpec Chain
                // only models one level of related(), so this branch
                // is unreachable for now.
                Change::Child(_) => {}
            }
        }
        out
    }

    /// Advance child-side of an EXISTS subquery.
    ///
    /// When an upstream child table (i.e. a subquery table referenced
    /// by an `whereExists(…)`) mutates, the parent chain's set of
    /// matching rows may flip. Mirrors TS `Exists::*push` child branch
    /// (`packages/zql/src/ivm/exists.ts:120-206`) — we materialise the
    /// current parent snapshot, then delegate to every transformer's
    /// `push_child` (default no-op; `ExistsT` overrides). The ExistsT
    /// whose `child_table` matches detects size-flip on each
    /// correlated parent and emits `Add(parent)` / `Remove(parent)`
    /// which we translate to `RowChange::Add` / `RowChange::Remove`.
    ///
    /// Parent snapshot is built from `source → transformers` with the
    /// target ExistsT excluded, so the snapshot represents parents
    /// that would pass every other filter. Including the target
    /// ExistsT itself would filter out exactly the parents whose
    /// inclusion we're trying to decide — we need the pre-ExistsT set.
    pub fn advance_child_for_exists(
        &mut self,
        child_table: &str,
        change: Change,
    ) -> Vec<RowChange> {
        // Find the index of the Transformer owning this child_table —
        // either a top-level ExistsT (standard case) or an OrBranchesT
        // whose internal ExistsT references the table (p32/p36 case).
        let mut target_idx: Option<usize> = None;
        for (i, t) in self.transformers.iter_mut().enumerate() {
            let Some(any) = t.as_any_mut() else { continue };
            if let Some(et) = any.downcast_mut::<crate::ivm_v2::exists_t::ExistsT>() {
                if et.child_table() == Some(child_table) {
                    target_idx = Some(i);
                    break;
                }
            } else if let Some(or_t) =
                any.downcast_mut::<crate::ivm_v2::or_exists_t::OrBranchesT>()
            {
                if or_t
                    .child_tables()
                    .into_iter()
                    .any(|c| c == child_table)
                {
                    target_idx = Some(i);
                    break;
                }
            }
        }
        let Some(target_idx) = target_idx else {
            return Vec::new();
        };

        // Parent snapshot = source → [transformers[0..target_idx]]
        // (i.e. the stream as it appears immediately before the target
        // ExistsT). Omitting the target ExistsT is essential: its own
        // filter would hide exactly the parents we want to re-evaluate.
        let parent_snapshot: Vec<Node> = {
            let mut stream: Box<dyn Iterator<Item = Node>> =
                self.source.fetch(FetchRequest::default());
            for (i, t) in self.transformers.iter_mut().enumerate() {
                if i >= target_idx {
                    break;
                }
                stream = t.fetch_through(stream, FetchRequest::default());
            }
            stream.collect()
        };

        // Call push_child on the target ExistsT. Capture the target's
        // relationship_name up-front so we can later look up its
        // `(child_table, child_pk)` entry in `exists_child_tables` for
        // emitting `RowChange::Edit` on the child table — mirrors TS
        // `Streamer#streamChanges` 'child' case recursion into
        // `cc.child.change` with the child schema at
        // `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:986-993`.
        let target = &mut self.transformers[target_idx];
        let rel_name_for_edit: Option<String> = target
            .as_any_mut()
            .and_then(|a| a.downcast_mut::<crate::ivm_v2::exists_t::ExistsT>())
            .map(|et| et.relationship_name().to_string());
        let emissions: Vec<Change> = target
            .push_child(change, child_table, &parent_snapshot)
            .collect();

        let query_id = self.query_id.clone();
        let table = self.table.clone();
        let pk = self.primary_key.clone();
        // Walk any populated relationships on the emitted parent node
        // to emit subquery rows — mirrors TS `Streamer#streamNodes`
        // recursion in `pipeline-driver.ts:1027-1030` which yields a
        // RowChange for every row in `node.relationships[*]`, then
        // recurses into each child's own relationships. Our
        // `emit_node_subtree` below implements the same recursion.
        let exists_tables = &self.exists_child_tables;
        let mut out: Vec<RowChange> = Vec::new();
        for c in emissions {
            match c {
                Change::Add(a) => {
                    out.push(RowChange::Add(RowAdd {
                        query_id: query_id.clone(),
                        table: table.clone(),
                        row_key: build_row_key(&pk, &a.node.row),
                        row: a.node.row.clone(),
                    }));
                    let mut total = 0usize;
                    emit_node_subtree(&a.node, exists_tables, &query_id, &mut out, &mut total);
                }
                Change::Remove(r) => {
                    out.push(RowChange::Remove(RowRemove {
                        query_id: query_id.clone(),
                        table: table.clone(),
                        row_key: build_row_key(&pk, &r.node.row),
                    }));
                    // TS `Streamer#streamNodes` also recurses on remove
                    // (same for-loop over relationships). We leave the
                    // Remove path with empty relationships here — see
                    // comments in `ExistsT::push_child`.
                }
                Change::Child(cc) => {
                    // Mirror of TS `Streamer#streamChanges` 'child'
                    // case at `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:986-993`:
                    // on `Change::Child`, recurse into `cc.child.change`
                    // with the CHILD schema and emit a RowChange for
                    // the child table. Populated by `ExistsT::push_child`
                    // when a child-Edit doesn't flip parent inclusion —
                    // TS mirror `exists.ts:127-131` which forwards
                    // child-Edit via `#pushWithFilter(change)`
                    // (wraps outer change unchanged so Streamer's
                    // recursion emits the inner Edit on the child
                    // table). Requires the child table+pk which
                    // `exists_child_tables` provides keyed by the
                    // target ExistsT's `relationship_name`.
                    let Some(ref rel_name) = rel_name_for_edit else {
                        continue;
                    };
                    let Some((child_tbl, child_pk, _nested)) =
                        exists_tables.get(rel_name.as_str())
                    else {
                        continue;
                    };
                    match *cc.child.change {
                        Change::Edit(e) => {
                            out.push(RowChange::Edit(RowEdit {
                                query_id: query_id.clone(),
                                table: child_tbl.clone(),
                                row_key: build_row_key(child_pk, &e.node.row),
                                row: e.node.row,
                            }));
                        }
                        // Add/Remove inside Change::Child would also be
                        // streamer-recursed in TS, but ExistsT::push_child
                        // currently emits those as top-level Add/Remove
                        // on the parent (the flip cases at exists.ts:134-204).
                        // Only the Edit case arrives here — skip others.
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// List of child tables this Chain subscribes to for EXISTS push
    /// propagation. Driver uses this to route child-source mutations
    /// to the right Chain(s). Includes top-level ExistsT child tables
    /// AND child tables inside any OrBranchesT — otherwise mutations
    /// on relationships referenced only by OR-branch EXISTS (p32, p36)
    /// never reach the ExistsT and the parent's OR evaluation doesn't
    /// flip on child add/remove.
    pub fn exists_child_tables_flat(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        for t in self.transformers.iter_mut() {
            let Some(any) = t.as_any_mut() else { continue };
            if let Some(et) = any.downcast_mut::<crate::ivm_v2::exists_t::ExistsT>() {
                if let Some(ct) = et.child_table() {
                    out.push(ct.to_string());
                }
            } else if let Some(or_t) =
                any.downcast_mut::<crate::ivm_v2::or_exists_t::OrBranchesT>()
            {
                for ct in or_t.child_tables() {
                    out.push(ct.to_string());
                }
            }
        }
        out
    }

    /// Recursive advance-child entry point mirroring the top-down
    /// push TS's `buildPipelineInternal` sets up: a grandchild-table
    /// mutation (e.g. `channels` update in p19) routes into THIS
    /// chain's sub-Chain, whose own filters/ExistsT re-evaluate and
    /// emit RowChanges representing the sub-query's now-changed set;
    /// each of those RowChanges is converted back into a `Change` on
    /// the sub-query's top-level table and fed into THIS chain's
    /// direct `advance_child_for_exists`, which then propagates the
    /// parent-set flip up. Repeats for 3+ level nesting.
    pub fn advance_child_for_exists_recursive(
        &mut self,
        table: &str,
        change: Change,
    ) -> Vec<RowChange> {
        // Case 1 — direct match: one of our ExistsT owns this child
        // table. Use the existing single-level path which drives
        // `ExistsT::push_child` directly and matches TS
        // `Exists::*push` child-branch flip / forward logic at
        // `packages/zql/src/ivm/exists.ts:134-204` (Add/Remove flip)
        // and `exists.ts:127-131` (Edit forwarded as Change::Child).
        let direct_hit = self
            .exists_child_tables_flat()
            .iter()
            .any(|t| t == table);
        if direct_hit {
            return self.advance_child_for_exists(table, change);
        }

        // Case 2 — recursive: hand the mutation to each child-input
        // Chain that is interested. Collect triples (outer_child_table,
        // sub_chain_top_table, sub_rowchanges) first to avoid nested
        // mutable borrows of `self`.
        struct PendingCascade {
            sub_chain_table: String,
            sub_rowchanges: Vec<RowChange>,
        }
        let mut pending: Vec<PendingCascade> = Vec::new();
        for t in self.transformers.iter_mut() {
            let Some(et) = t
                .as_any_mut()
                .and_then(|a| a.downcast_mut::<crate::ivm_v2::exists_t::ExistsT>())
            else {
                continue;
            };
            let Some(child) = et.child_input_mut() else { continue };
            let Some(sub_chain) = child
                .as_any_mut()
                .and_then(|a| a.downcast_mut::<Chain>())
            else {
                continue;
            };
            let sub_table = sub_chain.table().to_string();
            let sub_rowchanges: Vec<RowChange> = if sub_table == table {
                // Innermost: mutation is on the sub-chain's own source
                // table. Drive sub-chain's `advance` — its filter /
                // Take etc. decide whether the row's presence in the
                // sub-query's output changes.
                sub_chain.advance(change.clone())
            } else {
                // Grandchild further down — recurse.
                sub_chain.advance_child_for_exists_recursive(table, change.clone())
            };
            if sub_rowchanges.is_empty() {
                continue;
            }
            pending.push(PendingCascade {
                sub_chain_table: sub_table,
                sub_rowchanges,
            });
        }

        // Cascade: convert each sub-chain RowChange → Change on its
        // own top-level table, then run it through this chain's
        // direct push_child path. Matches TS `Exists::*push`
        // re-evaluating when its child input emits add/remove.
        //
        // Split by RowChange.table: RowChanges whose table matches the
        // sub-chain's top table (e.g. sub conv chain emits
        // RowChange::Remove(conversations)) need to feed outer push_child
        // so the outer Exists re-evaluates. RowChanges on a different
        // table (e.g. sub conv chain emits RowChange::Edit(channels)
        // via the push_child Edit forward at `exists.ts:127-131`)
        // describe a grandchild-table mutation that the outer Exists
        // doesn't need to re-evaluate for — the Streamer would emit it
        // directly. Mirror of TS `Streamer#streamChanges` at
        // `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:986-993`
        // recursing through each nested Change::Child with the
        // appropriate child schema — the innermost Change::Add/Remove/Edit
        // emits on its own table regardless of outer depth.
        let mut out: Vec<RowChange> = Vec::new();
        for p in pending {
            for rc in p.sub_rowchanges {
                if rc.table() != p.sub_chain_table {
                    // Grandchild RowChange — pass through as the outer
                    // Streamer would. No re-feed needed.
                    out.push(rc);
                    continue;
                }
                let ch = match rc {
                    RowChange::Add(a) => Change::Add(super::super::ivm::change::AddChange {
                        node: Node {
                            row: a.row,
                            relationships: indexmap::IndexMap::new(),
                        },
                    }),
                    RowChange::Remove(r) => Change::Remove(
                        super::super::ivm::change::RemoveChange {
                            node: Node {
                                // RowChange::Remove carries only the
                                // row_key — for ExistsT::push_child
                                // we only need fields matching the
                                // outer ExistsT's `child_key`. The
                                // child_key is always the sub-chain's
                                // top-level PK (same field set as
                                // row_key here), so this is sufficient.
                                row: r.row_key,
                                relationships: indexmap::IndexMap::new(),
                            },
                        },
                    ),
                    // Mirror of TS `Exists::*push` case 'child' at
                    // `packages/zql/src/ivm/exists.ts:127-131` where
                    // child-edits fall through `#pushWithFilter(change)`
                    // — the outer Change::Child (wrapping the Edit) is
                    // forwarded if the parent passes the exists
                    // predicate. `RowChange::Edit` from the sub-chain
                    // lacks a prior row, so we synthesize an EditChange
                    // with `old_node == node` (safe: correlation-key
                    // columns are preserved across Edits per Join's
                    // `rowEqualsForCompoundKey` assertion at
                    // `join.ts:162-167`; `push_child` reads only the
                    // correlation-key columns from `node.row`).
                    RowChange::Edit(e) => Change::Edit(
                        super::super::ivm::change::EditChange {
                            node: Node {
                                row: e.row.clone(),
                                relationships: indexmap::IndexMap::new(),
                            },
                            old_node: Node {
                                row: e.row,
                                relationships: indexmap::IndexMap::new(),
                            },
                        },
                    ),
                };
                out.extend(self.advance_child_for_exists(&p.sub_chain_table, ch));
            }
        }
        out
    }

    /// Like `exists_child_tables_flat` but walks nested sub-Chains
    /// recursively. Used to route grandchild-table mutations
    /// (e.g. p19 — top-level messages → exists(conversation) →
    /// exists(channel); a `channels` mutation must reach this chain
    /// so the cascade runs top-down).
    ///
    /// Also includes each nested chain's OWN top-level table, because
    /// a mutation on that table is what triggers the innermost
    /// re-evaluation (filters that previously passed may now fail).
    pub fn exists_child_tables_flat_recursive(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        for t in self.transformers.iter_mut() {
            let Some(any) = t.as_any_mut() else { continue };
            // Two kinds of transformers can own child tables:
            //   a) A top-level `ExistsT` (standard nested WHERE EXISTS).
            //   b) An `OrBranchesT` whose inner branches hold ExistsTs
            //      (top-level OR-of-EXISTS — p32/p36 shapes). Without
            //      descending into OR branches here, child-table
            //      mutations on relationships referenced only by an OR
            //      branch never reach the ExistsT.
            if let Some(or_t) =
                any.downcast_mut::<crate::ivm_v2::or_exists_t::OrBranchesT>()
            {
                for ct in or_t.child_tables() {
                    out.push(ct.to_string());
                }
                for ex in or_t.iter_all_exists_mut() {
                    let Some(child) = ex.child_input_mut() else { continue };
                    if let Some(sub_chain) = child
                        .as_any_mut()
                        .and_then(|a| a.downcast_mut::<Chain>())
                    {
                        out.push(sub_chain.table().to_string());
                        out.extend(sub_chain.exists_child_tables_flat_recursive());
                    }
                }
                continue;
            }
            let Some(et) = any.downcast_mut::<crate::ivm_v2::exists_t::ExistsT>() else {
                continue;
            };
            if let Some(ct) = et.child_table() {
                out.push(ct.to_string());
            }
            // Recurse into the sub-Chain's own tables.
            let Some(child) = et.child_input_mut() else { continue };
            if let Some(sub_chain) = child
                .as_any_mut()
                .and_then(|a| a.downcast_mut::<Chain>())
            {
                // Sub-chain's own top-level table — a mutation on it
                // is what triggers the innermost re-evaluation.
                out.push(sub_chain.table().to_string());
                // And anything the sub-chain itself subscribes to
                // recursively (for 3+ level nesting).
                out.extend(sub_chain.exists_child_tables_flat_recursive());
            }
        }
        out
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

        // Pull every join's child rows into `Arc<Vec<Node>>` BEFORE
        // taking the long-lived parent borrow. Mirror of TS
        // `buildPipelineInternal` evaluating each Join's child input
        // eagerly (Join.fetch takes a stream from its child). Each
        // child fetch needs `&mut js.child_source`, which is disjoint
        // from `&mut self.source` / `&mut self.transformers`, but
        // capturing all the child Arcs up front releases those borrows
        // before we start iterating the parent chain.
        let mut child_arcs: Vec<StdArc<Vec<Node>>> = Vec::with_capacity(self.joins.len());
        for js in self.joins.iter_mut() {
            child_arcs.push(StdArc::new(js.child_source.fetch(req.clone()).collect()));
        }

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

        // Apply EACH join's decoration in declaration order.
        // Mirrors TS `buildPipelineInternal` iterating `ast.related[]`
        // and folding `end = applyCorrelatedSubQuery(csq, ..., end, ...)`
        // at `packages/zql/src/builder/builder.ts:353-355`.
        let mut final_stream: Box<dyn Iterator<Item = Node> + '_> = parent_stream;
        for (js, child_arc) in self.joins.iter_mut().zip(child_arcs.into_iter()) {
            final_stream = js
                .transformer
                .fetch_through(final_stream, child_arc, req.clone());
        }

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
        // Parent-side join: forward through each join's push_parent.
        // Mirrors TS `buildPipelineInternal` chaining joins in order
        // (builder.ts:353-355); each downstream join wraps the prior
        // decorated parent node.
        for join in self.joins.iter_mut() {
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
    /// Downcast hook so a parent Chain can recurse into its own
    /// ExistsT's `child_input` (which is a `Box<dyn Input>`) and
    /// route grandchild-table mutations through the sub-Chain's
    /// `advance_child_for_exists_recursive`. Without this, nested
    /// WHERE EXISTS (e.g. p19) never sees grandchild changes.
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
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
