# RS↔TS Structural Parity — Honest Divergence Audit

Generated 2026-04-18 via four parallel source-code audits of the RS v2 IVM
port against `packages/zql/src/` TS spec. The user's rule: "TS is spec;
no novel ideas in RS; literal port operator-for-operator." This document
lists **every place where RS is NOT a literal port**, with TS and RS
file:line citations so the claim can be verified.

**The 1060/1060 behavioral-parity sweeps do not contradict this list.**
Every divergence below is either covered by tests that happen not to
trigger the gap, OR would cause a visible wire-format difference that
the fuzz corpus doesn't exercise. Behavioral equivalence on the tested
shapes does not mean structural equivalence.

---

## Index

1. [Fixed in this sweep (2026-04-18)](#fixed-in-this-sweep-2026-04-18)
2. [Open behavioral divergences](#open-behavioral-divergences)
3. [Open structural/performance divergences](#open-structural-only-divergences)
4. [Dead code / observability gaps](#dead-code--observability-gaps)
5. [Summary table](#summary)

---

## Fixed in this sweep (2026-04-18)

### FX-1: `apply_planner_flips` RS-side heuristic removed

- **Was**: `ast_builder.rs:310-452` — a novel RS pattern-match that set
  `flip = Some(true)` on CSQs whose OR shape matched a narrow criterion
  (OR-of-leaf-EXISTS, distinct child tables, no NOT EXISTS, no nested
  CSQ in the subquery). Layered on top of the TS cost-model planner.
- **Why wrong**: the TS wrapper `rust-pipeline-driver-v2.ts` already
  runs `planQuery` (commit 23327573e) → `applyPlansToAST` at
  `planner-builder.ts:322-355`. The AST arrives in Rust with
  per-CSQ `flip` already set. RS's heuristic could OVERRIDE a cost
  model `flip: false` decision to `true` — direct divergence risk.
- **Fix**: deleted `apply_planner_flips` + 5 associated tests. RS now
  consumes `flip` from the AST verbatim.
- **Commit**: this sweep.

### FX-2: `PERMISSIONS_EXISTS_LIMIT` wired through `system` field

- **Was**: `driver.rs:50-51` — `PERMISSIONS_EXISTS_LIMIT = 1` defined
  with `#[allow(dead_code)]`; `clone_sub_ast_with_exists_limit` always
  applied `EXISTS_LIMIT = 3`. TS `builder.ts:316-319` picks
  `PERMISSIONS_EXISTS_LIMIT` when `csq.related.system === 'permissions'`.
- **Why wrong**: permissions CSQs with ≥2 matching children emitted
  up to 3 decoration rows where TS caps at 1 — silent row-count
  over-emission on permission paths.
- **Fix**: added `system: Option<System>` to `ExistsSpec`, populated
  from `CorrelatedSubquery.system` in `ast_builder.rs:walk_exists`,
  read in `clone_sub_ast_with_exists_limit` to pick the right cap.
- **Commit**: this sweep.

---

## Open behavioral divergences

### BEH-1: Join `push_parent` doesn't decorate with child factory (RS simple DIV-1)

- **TS**: `packages/zql/src/ivm/join.ts:126-135` — `yield* this.#output.push({type:'add', node: this.#processParentNode(change.node.row, change.node.relationships)}, this)`
- **RS**: `packages/zero-cache-rs/crates/sync-worker/src/ivm_v2/join_t.rs:99-139` — returns raw `Change` with no relationship factory.
- **Impact**: downstream consumers never receive the joined child relationship stream on `push_parent`.
- **Effort**: med.

### BEH-2: Join `push_child` emits bare ChildChange (RS simple DIV-2)

- **TS**: `join.ts:246-256` — `node: this.#processParentNode(parentNode.row, parentNode.relationships)` inside `#pushChildChange`.
- **RS**: `join_t.rs:199-208` — `node: shallow_clone(parent_node)` (strips relationships).
- **Impact**: parent nodes in child-propagated changes carry no child data downstream.
- **Effort**: med.

### BEH-3: Join `#inprogressChildChange` overlay entirely missing (RS simple DIV-3)

- **TS**: `join.ts:53,227-260,275-295` — `#inprogressChildChange: JoinChangeOverlay` set during `#pushChildChange`, used in `#processParentNode` to apply `generateWithOverlay` correction for parents iterated past the in-progress child position.
- **RS**: `join_t.rs:59-95` — no overlay state; `process_parent` always reads the static snapshot.
- **Impact**: for multi-parent child pushes, parents ranked above the in-progress child position receive a stale child stream.
- **Effort**: high.

### BEH-4: Take Edit path always splits to Remove+Add (RS simple DIV-5)

- **TS**: `packages/zql/src/ivm/take.ts:444-712` — `#pushEditChange` has ~7 sub-cases keyed on `(oldCmp, newCmp)` vs `bound`. Forwards `Edit` downstream when both old/new are inside the window.
- **RS**: `packages/zero-cache-rs/crates/sync-worker/src/ivm_v2/take_t.rs:364-382` — unconditional `Remove(old) + Add(new)` split.
- **Impact**: wire-level change type differs — clients see `Remove+Add` instead of `Edit`. Final CVR state equivalent; patch operations not.
- **Effort**: high (port 250-line TS state machine).

### BEH-5: Take Remove doesn't refetch replacement (RS simple DIV-6)

- **TS**: `take.ts:354-418` — on Remove within/at bound, fetches next row after the bound and pushes `Remove + Add(newBound)`.
- **RS**: `take_t.rs:301-362` — clears `s.bound = None`; emits only `Remove`. No refetch, no replacement `Add`.
- **Impact**: after removing any row from a full window, window stays at `size-1` permanently instead of promoting the next eligible row. **LIMIT N semantics broken on Remove.**
- **Effort**: med.

### BEH-6: Take partitioned refetch uses `constraint: None` (RS simple DIV-8)

- **TS**: `take.ts:212-239` — `#getStateAndConstraint` builds a `constraint` from the partition key of the row; refetch is partition-scoped.
- **RS**: `take_t.rs:233-299` — all `pending_refetch` requests have `constraint: None`.
- **Impact**: partitioned take in RS refetches rows from all partitions; corrupts per-partition bounds.
- **Effort**: med.

### BEH-7: Take missing `MAX_BOUND_KEY` cross-partition fetch path (RS simple DIV-9)

- **TS**: `take.ts:20,130-150,724-740` — global `MAX_BOUND_KEY` tracks largest bound across partitions; used in unconstrained fetches for nested sub-queries.
- **RS**: `take_t.rs` — no `max_bound` field; the unconstrained fetch path is absent.
- **Impact**: nested sub-query shapes (e.g. `issues include issuelabels include label`) over-fetch without the global bound cutoff.
- **Effort**: med.

### BEH-8: Skip ignores `req.start` / `req.reverse` (RS simple DIV-10)

- **TS**: `skip.ts:43-65,104-163` — `fetch` delegates to `#getStart(req)` merging `req.start`, `req.reverse`, and `this.#bound` into a combined start.
- **RS**: `skip_t.rs:41-57` — plain `upstream.filter(should_be_present)`; `_req` ignored.
- **Impact**: paginated/reverse queries return rows RS would have excluded; misses `'empty'` short-circuit.
- **Effort**: med.

### BEH-9: FanOut missing `fanOutDonePushingToAllBranches` signal to FanIn (RS complex DIV-1)

- **TS**: `packages/zql/src/ivm/fan-out.ts:73-82` — iterates outputs, then calls `must(this.#fanIn).fanOutDonePushingToAllBranches(change.type)`.
- **RS**: `packages/zero-cache-rs/crates/sync-worker/src/ivm_v2/fan_out.rs:63-76` — emits `iter::once(change)`; no FanIn reference.
- **Impact**: FanIn's `accumulatedPushes` drain is never triggered; any push going through FanOut→branches→FanIn silently stalls.
- **Effort**: med.

### BEH-10: FanIn first-seen dedup instead of accumulate+flush (RS complex DIV-2)

- **TS**: `packages/zql/src/ivm/fan-in.ts:70-94` — `push()` only accumulates; actual forwarding in `*fanOutDonePushingToAllBranches` calls `pushAccumulatedChanges` with `identity` merge.
- **RS**: `fan_in.rs:65-90` — forwards first-arriving copy immediately based on a `dedup` count; discards subsequent.
- **Impact**: ordering of `pushAccumulatedChanges` lost; the merge-via-`identity` semantics are absent.
- **Effort**: high (needs DIV-1 wiring).

### BEH-11: UnionFanOut missing two-phase protocol calls (RS complex DIV-3)

- **TS**: `union-fan-out.ts:26-33` — `*push` calls `fanOutStartedPushing()` before, `fanOutDonePushing(change.type)` after.
- **RS**: `union_fan_out.rs:46-56` — emits `iter::once(change)`; no `#unionFanIn` handle, no protocol calls.
- **Impact**: UnionFanIn's accumulated-pushes drain via `mergeRelationships` never triggers.
- **Effort**: med.

### BEH-12: UnionFanIn missing `#fanOutPushStarted` + `#accumulatedPushes` (RS complex DIV-4)

- **TS**: `union-fan-in.ts:26-28,111-120,194-221` — two-phase: internal-change path vs accumulate-then-flush-via-`mergeRelationships`.
- **RS**: `union_fan_in.rs:12-16,47-86` — first-wins count-based dedup.
- **Impact**: fan-out-originated changes that need relationship merging across branches are incorrectly deduped instead of merged. `makeAddEmptyRelationships` fallback absent.
- **Effort**: high.

### BEH-13: UnionFanIn takes 1 input; TS takes N (RS complex DIV-5)

- **TS**: `union-fan-in.ts:30-92` — constructor takes branch `inputs`, merges their `relationships` into a unified schema with uniqueness assertions.
- **RS**: `union_fan_in.rs:19-25` — takes single `Box<dyn Input>`, copies its schema.
- **Impact**: multi-branch queries see incomplete schemas; relationships added by branch-specific operators invisible downstream.
- **Effort**: high.

### BEH-14: FlippedJoin not wired into pipeline (RS complex DIV + ast_builder DIV-1)

- **TS**: `packages/zql/src/builder/builder.ts:459-486` — `applyFilterWithFlips` 'correlatedSubquery' case creates `new FlippedJoin({parent, child, ...})`.
- **RS**: `ast_builder.rs:938-952`, `driver.rs:349-355` — flip:true lowers to `ExistsT` with cap disabled. An agent-attempted port (2026-04-18) regressed fuzz_00782/00985 and was reverted.
- **Impact**: push-path reactivity on flip:true CSQs differs from TS. Fetch-path output matches on tested shapes because ExistsT+no-cap produces the same parent set as FlippedJoin; but the operator graph is not a literal port.
- **Effort**: high. Schema bug (parent vs child base) identified — not sufficient alone.

### BEH-15: FlippedJoin schema uses child base (RS complex DIV-6)

- **TS**: `packages/zql/src/ivm/flipped-join.ts:77-89` — `this.#schema = { ...parentSchema, relationships: { ..., [name]: { ...childSchema, ... } } }` — parent base.
- **RS**: `flipped_join.rs:47-64` — `let schema = SourceSchema { table_name: child_schema.table_name, columns: child_schema.columns, ...}` — child base.
- **Impact**: downstream operators consulting schema metadata operate on child-table semantics. Dead in production today (DIV-14 unwired) but will break when wired.
- **Effort**: low (already identified; earlier swap attempt didn't close fuzz_00782/00985).

### BEH-16: FlippedJoin `push_child` is a no-op pass-through (RS complex DIV-7)

- **TS**: `flipped-join.ts:313-428` — `#pushChild` dispatches to `#pushChildChange` with parent-iteration, `#inprogressChildChange` overlay, conditional `child` vs `add`/`remove` emission.
- **RS**: `flipped_join.rs:78-119` — logs the change type; returns `iter::once(change)` unchanged.
- **Impact**: child mutations that should fan out to multiple parent ChildChange events, or should emit parent-level add/remove on first/last child, pass through raw.
- **Effort**: high.

### BEH-17: FlippedJoin `push_parent` always emits ChildChange (RS complex DIV-8)

- **TS**: `flipped-join.ts:430-502` — `#pushParent` emits `{...change, node: flip(change.node)}` preserving add/remove/child type.
- **RS**: `flipped_join.rs:123-198` — always wraps in `ChildChange`.
- **Impact**: downstream expects add/remove events, gets ChildChange wrappers; add/remove parent events are lost.
- **Effort**: med.

### BEH-18: ExistsT Remove+size=0 doesn't re-insert removed child (RS complex DIV-9)

- **TS**: `exists.ts:189-209` — emits `Remove` with `relationships: { ..., [rel]: () => [change.child.change.node] }` — removed child re-inserted into the emitted parent's relationship.
- **RS**: `exists_t.rs:691-723` — emits `Remove(parent)` with empty relationships.
- **Impact**: removed child row never emitted as `RowChange::Remove` to the client; subquery observers see the parent disappear but no tombstone for the child.
- **Effort**: med.

### BEH-19: AND-with-flips path is a stub (RS ast_builder DIV-2)

- **TS**: `builder.ts:390-413` — `applyFilterWithFlips` AND case calls `partitionBranches`, wraps `withoutFlipped` in `buildFilterPipeline`, recursively processes `withFlipped`.
- **RS**: `ast_builder.rs:177-188` — `Condition::And { .. } => Vec::new()` — returns empty `or_branches`, falls through to linear ExistsT chain.
- **Impact**: queries with `and(simpleFilter, flippedCSQ)` don't get non-flipped filter applied first; flipped CSQs inside AND don't get FlippedJoin.
- **Effort**: med (blocked on BEH-14).

### BEH-20: OR collapse to single OrBranchesT vs TS's 3 operators (RS ast_builder DIV-5)

- **TS**: `builder.ts:425-454` — `UnionFanOut + per-branch filter/join + UnionFanIn` as three wired operators.
- **RS**: `pipeline.rs:567-642` — single `OrBranchesT` transformer.
- **Impact**: cross-branch push reactivity not modelled; the per-branch push edges TS has are collapsed.
- **Effort**: med.

### BEH-21: OR non-flip uses OrBranchesT instead of FanOut/FanIn (RS ast_builder DIV-6)

- **TS**: `builder.ts:524-573` — non-flip OR uses `FanOut`/`FanIn` (union-all); flip OR uses `UnionFanOut`/`UnionFanIn` (dedup). Two distinct operator pairs.
- **RS**: `pipeline.rs:639-641` — `OrBranchesT::new(branches)` for both, toggled via `flip_mode`.
- **Impact**: push-event graph wiring for non-flip OR collapses TS's two distinct operator pairs into one.
- **Effort**: med.

### BEH-22: Live companion pipelines absent (RS driver DIV-9)

- **TS**: `pipeline-driver.ts:569-605` — each scalar-subquery companion pipeline has `setOutput.push` wired to throw `ResetPipelinesSignal` on value change.
- **RS**: `rust-pipeline-driver-v2.ts:826-862` — "Companion-pipeline reactivity (TS's live re-resolution on data change) is intentionally NOT implemented — v2 resolves once at `addQuery` time."
- **Impact**: a data change that invalidates the inlined scalar literal goes undetected. Queries with scalar subqueries can silently stale.
- **Effort**: med.

### BEH-23: Advance circuit-breaker absent (RS driver DIV-13)

- **TS**: `pipeline-driver.ts:867-887` — `#shouldAdvanceYieldMaybeAbortAdvance` checks elapsed vs `totalHydrationTimeMs / 2`; throws `ResetPipelinesSignal` on timeout.
- **RS**: `rust-pipeline-driver-v2.ts:667-676` — only yields when `timer.elapsedLap() > yieldThresholdMs()`; never resets.
- **Impact**: under a very large diff, CG can block longer than intended before re-hydration.
- **Effort**: low.

### BEH-24: Join child-fetch during parent-push doesn't apply overlay (RS driver DIV-11)

- **TS**: `join.ts:244`, `flipped-join.ts:55,343,361` — `#inprogressChildChange` overlays the in-progress Add/Remove during parent iteration.
- **RS**: `pipeline.rs decorate_parent_emission` — plain `collect()` from `child_source.fetch(constraint)`; no overlay.
- **Impact**: parent rows iterated past the in-progress child position get stale child snapshots.
- **Effort**: high.

### BEH-25: RowChange→Change round-trip in advance_child_for_exists_recursive drops Edit precision (RS driver DIV-12)

- **TS**: no equivalent; child-source push propagates through the already-wired pipeline.
- **RS**: `pipeline.rs:1138-1278` — recursive walker converts `RowChange → Change` mid-cascade (`old_node == node` for synthesized Edits — lossy).
- **Impact**: if an edit changes the correlation-key column, the lossy round-trip silently accepts it where TS would assert.
- **Effort**: high.

---

## Open structural-only divergences

### STR-1: Join fetch pre-materialises child snapshot (RS simple DIV-4)

- **TS**: `join.ts:110-118` — lazy per-parent `child.fetch(constraint)`.
- **RS**: `join_t.rs:229-272` — `child_snapshot: Arc<Vec<Node>>` pre-collected upstream.
- **Impact**: performance on small fuzz; behavioural risk when child ordering with per-parent constraint would filter more.
- **Effort**: high (trait redesign).

### STR-2: Take `rowHiddenFromFetch` overlay absent (RS simple DIV-7)

- **TS**: `take.ts:55,715-722` — transient exclusion during displacement push to prevent double-count on downstream re-fetch.
- **RS**: `take_t.rs:126-166` — `fetch_through` has no exclusion.
- **Impact**: during displacement re-fetch, row can appear twice (Remove event + re-fetch result).
- **Effort**: med.

### STR-3: ExistsT push boundary: raw child mutations via `push_child` (RS complex DIV-10)

- **TS**: `exists.ts:107-224` — `child` arrives already wrapped as `Change::Child` from upstream Join.
- **RS**: `exists_t.rs:561-826,833-953` — raw child-source `Change::Add`/`Remove` routed via separate `push_child` entry.
- **Impact**: any future operator inserted between Join and ExistsT in TS would not affect RS. Semantics preserved today.
- **Effort**: low (document as intentional).

### STR-4: `push_through_transformers` buffers between operators (RS driver DIV-1)

- **TS**: `join.ts:227`, `flipped-join.ts` — generator `yield*` chaining, no intermediate collection.
- **RS**: `pipeline.rs:1481-1498` — `buf: Vec<Change>` drained operator-by-operator.
- **Impact**: performance (memory spike proportional to change count).
- **Effort**: high (trait return-type redesign).

### STR-5: `advance_child` / `advance_child_for_exists` buffer parent snapshot (RS driver DIV-2/3)

- **TS**: generator `#parent.fetch(...)`.
- **RS**: `pipeline.rs:774-781, 925-935` — `parent_snapshot: Vec<Node> = stream.collect()`.
- **Impact**: memory spike under large parent tables.
- **Effort**: med.

### STR-6: `emit_node_subtree` uses precomputed `ExistsChildTables` (RS driver DIV-6)

- **TS**: `pipeline-driver.ts:1006-1054` — `#streamNodes` reads `schema.relationships` dynamically.
- **RS**: `pipeline.rs:1799-1829` — consults build-time static `ExistsChildTables` map.
- **Impact**: a relationship name present on a node but absent from the build-time map is silently dropped.
- **Effort**: low (document invariant) or med (switch to live schema).

### STR-7: Hydration two-phase emission (parents then related) (RS driver DIV-7)

- **TS**: `pipeline-driver.ts:506-513` — inline per-node parent+children interleaved.
- **RS**: `rust-pipeline-driver-v2.ts:513-549` — all parents buffered, then `#emitRelatedForRow` per parent.
- **Impact**: RowChange event ordering differs. CVR-equivalent, but consumers expecting parent-before-children per node see different sequence.
- **Effort**: high (needs RS to lower `ast.related[]` into JoinSpec inside Rust).

### STR-8: `#emitRelatedForRow` rebuilds pipeline per parent row (RS driver DIV-8)

- **TS**: `join.ts:272-294` — single Join operator reuses indexed cursor.
- **RS**: `rust-pipeline-driver-v2.ts:961-997` — new `buildPipeline(...)` call per-parent-per-related-level.
- **Impact**: O(parents × levels) overhead; behaviour equivalent.
- **Effort**: high (needs STR-7).

### STR-9: `partitionBranches` not ported (RS ast_builder DIV-3)

- **TS**: `builder.ts:798-812` — standalone exported helper used in AND and OR flip cases.
- **RS**: only grep-counted for log lines; real split never acted on.
- **Impact**: structural placeholder. Blocks BEH-19.
- **Effort**: low.

---

## Dead code / observability gaps

### OBS-1: MeasurePushOperator wrapping absent (RS driver DIV-10)

- **TS**: `pipeline-driver.ts:491-497` — every pipeline source wrapped in `MeasurePushOperator` for inspector telemetry.
- **RS**: `rust-pipeline-driver-v2.ts:839-846` — `decorateSourceInput: i => i` (identity); `_inspectorDelegate` unused.
- **Impact**: no inspector metrics for RS push events. No client-visible behaviour difference.
- **Effort**: low.

### DEAD-1: `FlippedJoin` operator in `flipped_join.rs` is dead in production

- **TS**: uses `FlippedJoin` for every `flip:true` CSQ.
- **RS**: module exists with full `push_parent`/`push_child`/`fetch` — but NOT wired into the driver (see BEH-14). Used only by its own `#[cfg(test)]` tests.
- **Impact**: code lives, never exercised by real queries. The 10 `IVM_PARITY_TRACE` logs in the file never fire.

---

## Summary

| Category | Count |
|---|---|
| **Open behavioral** | 25 (BEH-1..25) |
| **Open structural / performance-only** | 9 (STR-1..9) |
| **Dead code / observability** | 2 (OBS-1, DEAD-1) |
| Fixed in this sweep | 2 (FX-1, FX-2) |
| **Total structural-parity items** | **38** |

Behavioral-parity sweep stays at **1060/1060** on both hydrate and
advance after the 2 fixes landed in this sweep.

Each open item cites the exact TS and RS file:line so the gap can be
verified, sized, and either closed in a future sweep or explicitly
accepted as a documented structural choice.
