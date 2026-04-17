# RS Bug Investigation Notes

Per-divergence diagnoses captured from sub-agent runs. Use these as the
starting point for Phase 6 fixes. Each entry cites the exact TS file:line
that RS must mirror, per the user's hard rule.

---

## fuzz_00082 — `EXISTS flip+scalar` push gap

**Query:**
```ts
zql.channels.where(({exists}) => exists('conversations', {flip: true, scalar: true}))
```

**Symptom:** TS adds `ch-test-1` + `co-test-1` after `INSERT conversations`;
RS adds nothing. RS misses the flipped-join child→parent advancement.

**TS push path (what should happen):**
- Entry: `packages/zql/src/builder/builder.ts:367` — `applyWhere` detects `conditionIncludesFlippedSubqueryAtAnyLevel(condition) === true`, routes to `applyFilterWithFlips` at line 373.
- Routing: `builder.ts:450` `case 'correlatedSubquery'` → builds child pipeline for `conversations` via `buildPipelineInternal`, creates **`FlippedJoin`** at `builder.ts:459`.
- On child INSERT: `FlippedJoin.#pushChild` (`packages/zql/src/ivm/flipped-join.ts:312`) → `#pushChildChange` (line 336) → builds constraint `{channelId: 'ch-test-1'}` → fetches matching parent → emits `Change.Add(ch-test-1)` with `relationships.conversations = [co-test-1]` at `flipped-join.ts:402-414`.

**RS counterpart status:**
- Currently: `ExistsT` fallback. RS `ast_builder.rs:786-791` explicitly logs `"flip=true on relationship=conversations — falling back to ExistsT (hydration-equivalent; push may differ)"` and emits `ExistsSpec` at line 800.
- Missing: a real `FlippedJoinT` operator. The `ExistsT` fallback would work for the 0→1 ADD case IF `child_primary_key` were filled — but it isn't.

**Root cause:**
- `ast_builder.rs:809` sets `child_primary_key: None` on the `ExistsSpec`.
- `pipeline.rs:424-443` requires both `child_table` AND `child_primary_key` to be `Some` to register the chain in `exists_child_tables`.
- Without that registration, `advance_child_for_exists("conversations", Insert(co-test-1))` is never called.
- So the chain never reacts to conversations mutations.

**Recommended fix (Phase 6):**
1. **Driver-level:** in the `RustPipelineDriverV2::build_chain` call site that invokes `ast_to_chain_spec`, fill `child_primary_key` from the schema/table-registry before passing to `Chain::build`. The comment at `ast_builder.rs:808-810` already reads "Driver fills in from schema." — that wiring is missing. This minimum fix makes `ExistsT::push_child` work for the 0→1 ADD case.
2. **Verify:** `exists_child_tables_flat()` (`pipeline.rs:881`) returns `"conversations"` after the fix.
3. **Long-term:** for proper parity, install a `FlippedJoinT` mirroring `flipped-join.ts:336-419` instead of using `ExistsT` for `flip:true`. This is task #148 (kept open).

---

## fuzz_00087 — `NOT EXISTS` gate-flip on child Add (HIGH-IMPACT)

**Query:**
```ts
zql.channels.where(({not, exists}) => not(exists('conversations')))
```

**Symptom:**
1. INSERT `ch-test-1` (no conversations) → ch-test-1 APPEARS (NOT EXISTS holds)
2. INSERT `co-test-1` in ch-test-1 → ch-test-1 should DISAPPEAR (NOT EXISTS no longer holds)
- TS net: 0 changes (appears then disappears).
- RS net: +1 (ch-test-1 added but never removed) — RS misses the disappearance.

**TS push path:**
- Pipeline: `source(channels) → Join(child=conversations) → Exists(NOT EXISTS)`
- On INSERT co-test-1, `Exists.push(Change::Child)` at `packages/zql/src/ivm/exists.ts:133-167` runs the `'add'` branch:
  1. `size = yield* this.#fetchSize(change.node)` (line 135) — reads `change.node.relationships['zsubq_conversations_8']()`, count = **1** (Join's in-memory store reflects the just-added co-test-1)
  2. `if (size === 1)` → true
  3. `if (this.#not)` → true (NOT EXISTS)
  4. Lines 142-154: emits `Remove(ch-test-1)` with empty relationships
- Result: -1 propagates upstream; combined with +1 from step 1 = net 0.

**RS counterpart:**
- `advance_child_for_exists_recursive("conversations", Add(co-test-1))` at `pipeline.rs:909` correctly routes to `advance_child_for_exists` at `pipeline.rs:730`.
- `parent_snapshot` built via `source.fetch(PREV snapshot)` returns ch-test-1 ✓.
- `correlates()` matches ch-test-1 ↔ co-test-1 ✓.
- Then `child_size_for(ch-test-1.row)` at `exists_t.rs:394` calls `child_input.fetch(constraint)` — **this queries SQLite via the SnapshotReader pinned to PREV snapshot (before co-test-1 was committed).**
- PREV snapshot returns **0** conversations for ch-test-1.
- `new_size = 0` ≠ 1 → the `if new_size == 1` branch at `exists_t.rs:540` is NOT taken.
- No `Remove(ch-test-1)` emitted.

**Root cause (CRITICAL — likely affects many divergences):**
`exists_t.rs:394 child_size_for` reads from the PREV (pre-mutation) SQLite snapshot. The TS equivalent reads from the Join operator's in-memory relationship storage which IS updated before `Exists::push` runs. So RS systematically undercounts after INSERT and overcounts after DELETE — every gate-flip via `child_size_for` is broken at advance-time.

**Recommended fix:**
- `exists_t.rs:394-406` (`child_size_for`) — derive post-mutation count from the Change itself instead of re-querying SQLite. Mirrors TS `exists.ts:135` semantics where `#fetchSize` already sees the mutation:
  - For `Change::Add(_)`:  `new_size = child_input.fetch(constraint).count() + 1`
  - For `Change::Remove(_)`: `new_size = child_input.fetch(constraint).count() - 1`
  - For `Change::Edit(_)`: PK-preserving edit, count unchanged → `new_size = child_input.fetch(constraint).count()`
- Cite: `// mirrors TS exists.ts:135 (#fetchSize reads Join's post-mutation in-memory store)`
- Alternative considered & rejected: refresh SnapshotReader to CURR mid-batch — would require restructuring snapshot timing, breaks streaming.

**Why this likely fixes multiple divergences:** any push-path test where `Exists::push_child` needs to detect a 0→1 or 1→0 size flip will fail today. That includes plain `EXISTS` (when count goes 0→1), `NOT EXISTS` (1→0 via Add, 0→1 via Remove), and the EXISTS-in-OR variants whose evaluation routes through `child_size_for`. Re-run the sweep after applying this fix and watch the diverge count drop sharply.

**⚠️ CONTRADICTION TO RESOLVE BEFORE FIXING:**
The existing `exists_t.rs:388-393` comment states the opposite of the agent's hypothesis:
> "Used by `push_child` to compute the *post-mutation* size … by the time `push_child` runs, the child source's SQLite connection has already applied the change. So pre-mutation size is inferred: `Add` → pre = post-1, `Remove` → pre = post+1, matching TS's `size === 1` / `size === 0` flip detection in `packages/zql/src/ivm/exists.ts:136/171`."

The empirical fact (RS misses `Remove(parent)` for `fuzz_00087`) proves a real bug. But which is right?

**Verify before fixing — add temporary instrumentation:**
1. In `exists_t.rs:394 child_size_for`, log the count returned per call along with the parent_row PK and the child_table.
2. In `exists.ts:135 #fetchSize`, log the count it sees (TS, only when `IVM_PARITY_TRACE=1`).
3. Run `harness-advance-coverage.ts` for `fuzz_00087` only (`CORPUS_LIMIT=N` after sorting corpus to put fuzz_00087 first, or run `harness-advance.ts` patterns).
4. Compare the two logs:
   - If RS's `child_size_for` returns 0 while TS's `#fetchSize` returns 1 → the agent's hypothesis is correct; the comment in `exists_t.rs` is aspirational. Fix as proposed.
   - If RS's `child_size_for` returns the same count as TS but the `if new_size == 1` branch still doesn't fire → the bug is elsewhere (e.g., `parent_snapshot` doesn't include `ch-test-1`, or `correlates()` is comparing wrong fields).
5. Revert the temporary logs after the bug is fixed (per the "TS-side logs only when env-gated" rule the TS log must be inside `if (process.env.IVM_PARITY_TRACE) …`).

**Do not apply the proposed fix without this verification step.** The contradiction with the existing comment means one of the two is wrong, and "no novel ideas / only port from TS" requires us to know which.

---

## Other divergence categories (from `parity_report.md`)

| Shape | Count | Sample IDs | Likely root cause |
|---|---|---|---|
| `or(or, csq)` | 6 | fuzz_00146, fuzz_00150, fuzz_00375 | Same family as p36 — OR-of-OR flattening interaction with CSQ decoration |
| `or(simple, csq)` | 5 | fuzz_00126, fuzz_00127 | OR-of-mixed-scalar+CSQ — needs flip path |
| `EXISTS scalar` | 2 | fuzz_00080, fuzz_00084 | Likely same `child_primary_key` bug as fuzz_00082 |
| `and(csq, csq)` | 2 | fuzz_00135, seed_18_… | AND-of-EXISTS push — inner ExistsT child push |
| `simple NOT IN []` | 1 | fuzz_00633 | NULL handling on empty IN list — RS more SQL-strict than TS |
| `or(simple, simple, csq)` + `or(simple, and, and)` | 2 | seed_17, seed_22 | Compound shape, fuzz didn't reach |

Generate fresh per-shape diagnoses by re-running `harness-coverage.ts` →
`parity_report.md`, then dispatching one diagnostic agent per shape with
the exact rendered ZQL + diff as input. The skill at
`.claude/skills/ivm-parity-sweep/SKILL.md` documents the parallel-fix
protocol.
