# RS Bug Investigation Notes

Per-divergence catalog organized for incremental porting. Each entry cites
the exact TS code path RS must mirror (user's hard rule: no novel ideas on
RS side; TS is authoritative).

## Session history

| Commit | Change | adv-diverge |
|---|---|---|
| (baseline) | seed corpus + fuzz 1055 ASTs | 204 |
| 597eb07ab | agent A threshold flip + agent B NotIn[] fix | 128 (thresholds wrong) |
| 18ea0f82b | threshold revert + child-push sub-chain routing | 113 |
| c4a010074 | or-branch decoration forward in push | 104 |

## Remaining categories (post-c4a010074, batch=30)

### related-only — 28

**Plain `.related(rel)` (7 cases, e.g. fuzz_00156)**
- TS: `applyCorrelatedSubQuery` at `builder.ts:611-647` wraps child in a Join operator; Join's `#pushChild` (`join.ts:190+`) emits `Change::Child` when a child row arrives that correlates to an existing parent.
- RS: `Chain::advance_child` calls `join.transformer.push_child` but the emission isn't reaching the RowChange::Add output. Suspected: JoinT::push_child doesn't emit Child for the non-correlated-existing-parent case, or the parent_snapshot (unconstrained) misses the correlation step.
- **Isolation reproduces: NO** — fuzz_00156 alone passes. Batch-level timing-or-state issue.

**`.related(rel, q => q.orderBy().limit(N))` (15 cases, e.g. fuzz_00161)**
- TS: child's Take operator evaluates push; at-limit refetch can emit Remove+Add.
- RS: `push_through_transformers` added in 18ea0f82b routes through child sub-chain, but Take operator's push semantics need verification for refetch/bound updates.
- Next: confirm if Take push emits the refetch correctly via single-AST trace.

**`.related(rel, q => q.related(subRel))` (4 cases)**
- TS: grandchild emission via `Streamer#streamChanges` recursion at `pipeline-driver.ts:964-972`.
- RS: `advance_child` doesn't recurse into grandchildren per nested related[].

### or(simple, csq) — 20

Running my `c4a010074` decoration-forward fix, some were fixed; 20 remain.
- TS: applyFilterWithFlips/applyOr at `builder.ts:410-557` — per-branch Filter + per-branch upfront-Join decoration, union via UnionFanIn.
- RS: `OrBranchesT::push` now decorates, but the emission may still miss cases where the upstream change is Edit (partially handled) or Child (unhandled). Single-AST trace for fuzz_00377 still fails in batch (works in isolation).

### EXISTS / EXISTS scalar — 13 + 12

- fuzz_00089 `channels.whereExists('participants')` passes in isolation, fails in batch.
- Hypothesis: batch-time race in how `advance_child_for_exists` fires vs when the replica snapshot is refreshed.

### EXISTS flip / flip+scalar — 7 + 8

- TS: `FlippedJoin` at `packages/zql/src/ivm/flipped-join.ts:336-419`.
- RS: falls back to ExistsT (see `ast_builder.rs:786-791` fallback comment). Task #148 — wire a real FlippedJoin operator into ChainSpec/driver.

### NOT EXISTS / NOT EXISTS scalar — 5 + 4

- TS: `exists.ts:171` uses `size === 0` for flip threshold.
- RS: thresholds now correct (per c4a010074). Remaining divergences likely from same batch-timing as the plain EXISTS cases.

### and/or compound CSQ (and(csq,csq), or(csq,csq), or(and,csq), or(or,csq)) — 2 each

- TS: applyOr/applyAnd compose per-branch; RS OrBranchesT has specific support. 
- These 8 divergences span both hydration and push paths; each shape likely needs an individual diagnosis.

### simple NOT IN — 1 (fuzz_00673)

Likely a different column-type or NULL variant not covered by agent B's fix. Re-check.

## Protocol to port each remaining class

For any shape category with N divergences:

1. Identify ONE representative divergence via `node -e "... find(r.outcome.status==='advance-diverge' && r.ast.where?.type===...)"` from `advance_coverage_run.json`.
2. Run it in isolation (`CORPUS_LIMIT=1` after filtering corpus to that ID): if passes in isolation, issue is batch-timing; if fails, issue is RS code.
3. For code bugs: read TS path (grep for the operator in `packages/zql/src/ivm/` + `builder/`). Read RS counterpart in `packages/zero-cache-rs/crates/sync-worker/src/ivm_v2/`.
4. Add env-gated TS log (revert after) + RS trace to confirm the divergence point.
5. Apply RS fix citing TS file:line. `cargo test`. Rebuild napi. Restart RS. Re-sweep.
6. Revert TS logs.

## Streaming architectural-parity notes

TS is generator-based throughout. RS:
- **Per-operator push**: returns `Box<dyn Iterator<Item=Change>>` — streaming ✓
- **Chain-level push chain (`Chain::advance`, `push_through_transformers`)**: Vec-buffered between operators. Bounded per-event (≤ few); not a streaming violation in practice but not literal parity with TS generator chaining.
- **Push-side parent fetch**: `parent_snapshot = stream.collect()` at `pipeline.rs:652, 790, 1164`. TS fetches lazily via `#parent.fetch({constraint})` at `join.ts:227`. **Known divergence.** Worse than Vec buffering because it scales with table row count. Not yet refactored because the API (`push_child(change, parent_snapshot: &[Node])`) pre-materializes; a faithful port needs API change to pass an iterator.

## Batch-vs-isolation flake

Several tests pass alone but fail in BATCH_SIZE=30 sweeps (observed: fuzz_00089, fuzz_00156, fuzz_00377). Root cause unknown. POKE_WAIT_MS=5000 doesn't help. Possibly related to the non-constrained `source.fetch` in push paths interacting with concurrent CVR diffs.
