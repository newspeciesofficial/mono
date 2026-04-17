//! OrBranchesT — Transformer implementing OR at the WHERE level.
//!
//! TS mirror: `packages/zql/src/builder/builder.ts:308-329` upfront
//! `applyCorrelatedSubQuery` Joins + `:514-557` `applyOr`
//! (`FanOut` + per-branch `Filter`/`Exists` + `FanIn`).
//!
//! The upfront-Join loop decorates every row with every CSQ's
//! relationship factory BEFORE the OR gate runs. `applyOr` then
//! filters; the decorations live on the incoming node and survive the
//! OR gate unchanged. Every branch's decoration reaches the Streamer.
//!
//! RS structurally implements this path: for each row, every branch's
//! `ExistsT::evaluate_decorate` runs (always decorating, not filtering);
//! decorations accumulate first-wins on duplicate relationship name
//! (mirrors TS UnionFanIn `relationshipsFromBranches` dedup at
//! `union-fan-in.ts:70-85`); the row is emitted if ANY branch's
//! predicate + EXISTS combination passes.
//!
//! **Known divergence from TS when the TS planner flips branches**:
//! TS has an alternate `applyFilterWithFlips` path (`builder.ts:410-448`)
//! the cost-model planner triggers via `flip: true` on CSQs. That path
//! uses `FlippedJoin` + `mergeFetches` first-wins-on-duplicate-PK
//! (`union-fan-in.ts:260-267`), dropping the second branch's
//! decoration when both branches emit the same parent. RS doesn't
//! port the planner (task #148), so when TS flips a query (e.g. p32
//! `or(exists, exists)`), RS emits a superset of TS rows: same parent
//! set, plus the extra decorations TS's mergeFetches optimized away.
//! The parent-set correctness is preserved; only decoration row-count
//! diverges.

use indexmap::IndexMap;

use super::change::{Change, Node};
use super::exists_t::ExistsT;
use super::filter_t::Predicate;
use super::operator::{FetchRequest, Transformer};

/// One OR branch: scalar predicate (AND of simple conditions) AND'd
/// with a list of `ExistsT` filters. Pure-scalar branches have an
/// empty `exists` vec; pure-EXISTS branches have `predicate = None`.
pub struct OrBranch {
    pub predicate: Option<Predicate>,
    pub exists: Vec<ExistsT>,
}

impl OrBranch {
    /// Evaluate the branch on `node` and return the (possibly decorated)
    /// node + whether this branch's predicate holds.
    ///
    /// Decoration policy: EVERY `ExistsT` runs `evaluate_decorate`,
    /// which always attaches the relationship factory (even empty) and
    /// returns a separate "matched" bool. That lets the outer
    /// `OrBranchesT` decide whether to keep this branch's decorations
    /// based on its mode (first-wins vs union-all).
    ///
    /// `all_exists_pass` = true only if EVERY EXISTS in the branch
    /// matches (AND within a branch, matching
    /// `applyAnd`→`applyCorrelatedSubqueryCondition` ordering at
    /// `builder.ts:502-512`).
    fn evaluate(&mut self, node: Node) -> (Node, bool) {
        let scalar_pass = self
            .predicate
            .as_ref()
            .map_or(true, |p| p(&node.row));
        let mut cur = node;
        let mut all_exists_pass = true;
        for ex in self.exists.iter_mut() {
            let (decorated, matched) = ex.evaluate_decorate(cur);
            cur = decorated;
            if !matched {
                all_exists_pass = false;
            }
        }
        (cur, scalar_pass && all_exists_pass)
    }
}

pub struct OrBranchesT {
    branches: Vec<OrBranch>,
    /// TS-planner flip mode. TS mirror:
    /// `packages/zql/src/builder/builder.ts:410-448`
    /// `applyFilterWithFlips` OR path → `UnionFanIn.fetch` which
    /// calls `mergeFetches` at
    /// `packages/zql/src/ivm/union-fan-in.ts:218-300`. That merge
    /// skips rows whose PK matches the previously-yielded one
    /// (L260-265) — i.e. **first-wins on duplicate PK**. When
    /// `flip_mode = true` this transformer emits at most one row
    /// per PK (keyed by `pk_cols`) matching mergeFetches semantics.
    /// When `flip_mode = false` (the default / non-flipped path)
    /// the emission is union-all across branches, mirroring TS
    /// `applyOr` at `builder.ts:514-557`.
    flip_mode: bool,
    /// Primary-key columns used to dedup emitted rows when
    /// `flip_mode` is set. Empty when `flip_mode = false`.
    pk_cols: Vec<String>,
}

impl OrBranchesT {
    pub fn new(branches: Vec<OrBranch>) -> Self {
        Self {
            branches,
            flip_mode: false,
            pk_cols: Vec::new(),
        }
    }

    /// Enable flip-mode (mergeFetches-style first-wins on duplicate
    /// PK). Caller provides the table's PK columns so the fetch
    /// iterator can key dedup entries. TS mirror:
    /// `packages/zql/src/ivm/union-fan-in.ts:260-265` — merge loop
    /// skips rows whose `compareRows` equals `lastNodeYielded`,
    /// which for merge-sorted streams over the same table means
    /// equal PK.
    pub fn with_flip_mode(mut self, pk_cols: Vec<String>) -> Self {
        self.flip_mode = true;
        self.pk_cols = pk_cols;
        self
    }

    /// Enumerate all child tables referenced by any branch's ExistsT.
    /// Used by `Chain::exists_child_tables_flat` to route child-table
    /// mutations through the OR branches' ExistsTs on advance. Without
    /// this, a mutation on a relationship table (e.g. participants
    /// added for p32) never reaches the OR branch's ExistsT and the
    /// parent's OR evaluation doesn't flip.
    pub fn child_tables(&self) -> Vec<&str> {
        let mut out = Vec::new();
        for branch in &self.branches {
            for ex in &branch.exists {
                if let Some(ct) = ex.child_table() {
                    out.push(ct);
                }
            }
        }
        out
    }

    /// Find a mutable ExistsT (inside any branch) whose `child_table`
    /// matches the given name. Returns a mutable reference so the
    /// caller can invoke `push_child` on it. Mirror of the top-level
    /// search in `Chain::advance_child_for_exists`, but walks the OR
    /// branches rather than the top-level transformers.
    pub fn find_exists_for_child_table_mut(
        &mut self,
        child_table: &str,
    ) -> Option<&mut ExistsT> {
        for branch in self.branches.iter_mut() {
            for ex in branch.exists.iter_mut() {
                if ex.child_table() == Some(child_table) {
                    return Some(ex);
                }
            }
        }
        None
    }

    /// Mutable iterator over ALL ExistsTs across all branches. Used by
    /// `Chain::advance_child_for_exists_recursive` to descend into
    /// nested sub-Chains wired through each branch's ExistsT — same
    /// recursion as the top-level transformer-list walk does for
    /// non-OR chains.
    pub fn iter_all_exists_mut(&mut self) -> impl Iterator<Item = &mut ExistsT> {
        self.branches.iter_mut().flat_map(|b| b.exists.iter_mut())
    }
}

impl Transformer for OrBranchesT {
    fn fetch_through<'a>(
        &'a mut self,
        upstream: Box<dyn Iterator<Item = Node> + 'a>,
        _req: FetchRequest,
    ) -> Box<dyn Iterator<Item = Node> + 'a> {
        // Flip-mode vs union-all differ only in the post-filter
        // dedup step — both paths run every branch's
        // `evaluate_decorate` to accumulate decorations and drop
        // rows that no branch matches. In flip-mode we additionally
        // track the set of PKs we've already emitted and skip any
        // subsequent duplicate, mirroring TS `mergeFetches` at
        // `packages/zql/src/ivm/union-fan-in.ts:260-265`
        // ("if comparator(lastNodeYielded, minNode) === 0 continue;").
        Box::new(OrBranchesFetchIter {
            upstream,
            or_t: self,
            seen: std::collections::HashSet::new(),
        })
    }

    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        // Push path: mirror TS non-flipped `applyOr`
        // (`builder.ts:514-557`) — row passes iff any branch's
        // Exists filter passes. Decorations are not rewritten onto
        // the emitted change today; the Chain's separate child-push
        // routing handles relationship-bearing emissions (see
        // `Chain::advance_child_recursive`). Future work for #148:
        // re-emit a decorated Add to mirror TS UnionFanIn push.
        let probe_node = change_node(&change);
        let mut cur = Node {
            row: probe_node.row.clone(),
            relationships: IndexMap::new(),
        };
        let mut any_pass = false;
        for branch in self.branches.iter_mut() {
            let (decorated, passed) = branch.evaluate(cur);
            cur = decorated;
            if passed {
                any_pass = true;
            }
        }
        if any_pass {
            Box::new(std::iter::once(change))
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn push_child<'a>(
        &'a mut self,
        change: Change,
        child_table: &str,
        parent_snapshot: &[Node],
    ) -> Box<dyn Iterator<Item = Change> + 'a> {
        // Delegate to the ExistsT (inside one of the branches) whose
        // own child_table matches. Mirrors `ExistsT::push_child`
        // semantics — we re-use the per-ExistsT flip detection. The
        // caller (`Chain::advance_child_for_exists`) passes a
        // parent_snapshot computed BEFORE this transformer, so the
        // ExistsT sees the unfiltered parent universe.
        //
        // Why "first branch whose ExistsT matches" is sufficient: TS
        // uniquifies CSQ aliases (`builder.ts:723+`), so within one
        // query each relationship name appears at most once across all
        // branches — meaning at most one branch's ExistsT owns any
        // given child_table.
        let Some(ex) = self.find_exists_for_child_table_mut(child_table) else {
            return Box::new(std::iter::empty());
        };
        ex.push_child(change, child_table, parent_snapshot)
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}

/// Fetch iterator.
///
/// Non-flip (union-all) mode — TS mirror: `applyOr` at
/// `packages/zql/src/builder/builder.ts:514-557` with upfront
/// `applyCorrelatedSubQuery` Joins (`:308-329`). For each upstream
/// row, run every branch to collect decorations (first-wins on
/// duplicate relationship name via `IndexMap::entry().or_insert`),
/// emit the row iff any branch's predicate + EXISTS combination
/// passes. Duplicates are naturally absent here since each upstream
/// row is visited once.
///
/// Flip mode — TS mirror: `applyFilterWithFlips` OR path at
/// `builder.ts:410-448` → `UnionFanIn.fetch` → `mergeFetches`
/// at `packages/zql/src/ivm/union-fan-in.ts:218-300`. `mergeFetches`
/// merge-sorts per-branch streams and skips any row whose PK
/// duplicates the previously-yielded one (L260-265,
/// `if comparator(lastNodeYielded, minNode) === 0 continue;`).
/// Since in this transformer each upstream row is already visited
/// exactly once, we implement the same first-wins semantics by
/// tracking emitted PKs in `seen` and skipping subsequent matches.
/// This mirrors the observable output of mergeFetches: one row per
/// PK, from the first branch that would have matched in sort order.
struct OrBranchesFetchIter<'a> {
    upstream: Box<dyn Iterator<Item = Node> + 'a>,
    or_t: &'a mut OrBranchesT,
    /// PK-keyed dedup set. Only consulted when `or_t.flip_mode`.
    /// Each entry is the JSON-serialised tuple of PK column values
    /// (stable `serde_json::to_string` on an ordered Vec).
    seen: std::collections::HashSet<String>,
}

impl<'a> Iterator for OrBranchesFetchIter<'a> {
    type Item = Node;
    fn next(&mut self) -> Option<Node> {
        loop {
            let upstream_node = self.upstream.next()?;
            let mut cur = upstream_node;
            let mut any_pass = false;
            for branch in self.or_t.branches.iter_mut() {
                let (decorated, passed) = branch.evaluate(cur);
                cur = decorated;
                if passed {
                    any_pass = true;
                    // In flip mode, TS's `applyFilterWithFlips`
                    // constructs a SEPARATE `FlippedJoin` per branch
                    // (`packages/zql/src/builder/builder.ts:438-440`)
                    // whose output stream is merged by `mergeFetches`
                    // (`packages/zql/src/ivm/union-fan-in.ts:260-265`)
                    // with first-wins on duplicate PK. Concretely that
                    // means ONLY THE FIRST MATCHING BRANCH's relationship
                    // decoration survives for a given PK — later branches'
                    // decorations would have been dropped by mergeFetches.
                    // Short-circuit here so accumulated `cur` carries
                    // only the first-passing branch's decoration,
                    // mirroring TS's per-branch-stream emission.
                    if self.or_t.flip_mode {
                        break;
                    }
                }
            }
            if !any_pass {
                continue;
            }
            // Flip-mode dedup: mirrors TS `mergeFetches` skipping
            // rows whose PK equals the last-yielded row's PK at
            // `packages/zql/src/ivm/union-fan-in.ts:260-265`.
            // Non-flip path skips this check entirely (union-all).
            if self.or_t.flip_mode {
                let pk_key = pk_key_for_row(&cur.row, &self.or_t.pk_cols);
                if !self.seen.insert(pk_key) {
                    continue;
                }
            }
            return Some(cur);
        }
    }
}

/// Build a stable string key from a row's PK columns. Used by
/// flip-mode dedup (TS `mergeFetches` first-wins-on-duplicate-PK).
/// Serialises `pk_cols` in declaration order and JSON-encodes the
/// values; `null` / missing becomes JSON `null`.
fn pk_key_for_row(row: &zero_cache_types::value::Row, pk_cols: &[String]) -> String {
    let mut parts: Vec<serde_json::Value> = Vec::with_capacity(pk_cols.len());
    for col in pk_cols {
        let v = match row.get(col) {
            Some(Some(v)) => v.clone(),
            _ => serde_json::Value::Null,
        };
        parts.push(v);
    }
    serde_json::Value::Array(parts).to_string()
}

fn change_node(c: &Change) -> Node {
    match c {
        Change::Add(a) => a.node.clone(),
        Change::Remove(r) => r.node.clone(),
        Change::Child(c) => c.node.clone(),
        Change::Edit(e) => e.node.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use serde_json::json;
    use zero_cache_types::value::Row;

    fn mk_node(id: i64) -> Node {
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(id)));
        Node {
            row,
            relationships: IndexMap::new(),
        }
    }

    /// Branch that always passes, decorating nothing.
    fn always_pass_branch() -> OrBranch {
        OrBranch {
            predicate: Some(std::sync::Arc::new(|_row: &Row| true)),
            exists: Vec::new(),
        }
    }

    /// In non-flip mode the transformer MUST emit every row whose any
    /// branch passes — even if two rows arrive sharing a PK.
    /// Mirror of TS `applyOr` (`packages/zql/src/builder/builder.ts:514-557`):
    /// the FanOut→Filter→FanIn chain emits rows as union-all.
    #[test]
    fn non_flip_mode_emits_all_rows() {
        let mut t = OrBranchesT::new(vec![always_pass_branch(), always_pass_branch()]);
        // Upstream with 3 rows, including a duplicate PK.
        let upstream: Vec<Node> = vec![mk_node(1), mk_node(2), mk_node(1)];
        let out: Vec<Node> = t
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        assert_eq!(out.len(), 3);
    }

    /// In flip mode the transformer MUST dedup rows by PK,
    /// first-wins. Mirror of TS `mergeFetches` at
    /// `packages/zql/src/ivm/union-fan-in.ts:260-265` skipping rows
    /// whose PK matches the previously-yielded row.
    #[test]
    fn flip_mode_dedupes_duplicate_pk_first_wins() {
        let mut t = OrBranchesT::new(vec![always_pass_branch(), always_pass_branch()])
            .with_flip_mode(vec!["id".into()]);
        let upstream: Vec<Node> = vec![mk_node(1), mk_node(2), mk_node(1)];
        let out: Vec<Node> = t
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        // Three rows in, two distinct PKs out — dup dropped.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].row.get("id"), Some(&Some(json!(1))));
        assert_eq!(out[1].row.get("id"), Some(&Some(json!(2))));
    }

    /// Safety net: flip-mode doesn't drop legitimately distinct rows.
    #[test]
    fn flip_mode_keeps_distinct_pks() {
        let mut t = OrBranchesT::new(vec![always_pass_branch()])
            .with_flip_mode(vec!["id".into()]);
        let upstream: Vec<Node> = vec![mk_node(1), mk_node(2), mk_node(3)];
        let out: Vec<Node> = t
            .fetch_through(Box::new(upstream.into_iter()), FetchRequest::default())
            .collect();
        assert_eq!(out.len(), 3);
    }
}
