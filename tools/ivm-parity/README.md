# ivm-parity — IVM TS↔RS parity test suite

Standalone parity harness for the Zero IVM. Generates an exhaustive bounded
corpus of ZQL queries, runs each against both the TypeScript zero-cache and
the Rust zero-cache (`ZERO_USE_RUST_IVM_V2=1`) backed by the same PG, and
diffs the hydration payload + advancement row deltas. Surfaces every parity
gap as a real query that can be reproduced.

No browser. No React. No Playwright. Node.js client + WebSocket only.

## TL;DR

```bash
cd tools/ivm-parity

# Make sure both caches are running on 4858 (TS) and 4868 (RS), and PG is seeded.
# (See "Bring-up" below for first-run setup.)

npx tsx ast-fuzz.ts                                    # ~150ms — generate corpus
npx tsx seed-extractor.ts                              # ~50ms  — append shipped-query seeds
MAX_PARALLEL=8 npx tsx harness-coverage.ts             # ~5s    — hydration sweep
BATCH_SIZE=30 npx tsx harness-advance-coverage.ts      # ~2 min — advancement sweep
npx tsx parity-report.ts                               # combine into parity_report.md

cat parity_report.md
```

Or invoke the `ivm-parity-sweep` skill end-to-end. See
`.claude/skills/ivm-parity-sweep/SKILL.md` for the full SOP.

## What the suite does

| Phase | File | Output | What it does |
|---|---|---|---|
| 1 | `ast-fuzz.ts` | `ast_corpus.json`, `ast_corpus.zql.txt` | Bounded BFS over the AST/Condition ADT against `zero-schema.ts`. Deterministic. ~1000 ASTs covering simple/AND/OR/CSQ shapes, every (col, op) pair, EXISTS × NOT EXISTS × flip × scalar permutations, depth-3 nesting, related[] subtrees, limit/orderBy/start cursors. |
| 2 | `seed-extractor.ts` | (appends to `ast_corpus.json`) | Hand-picked archetypes inspired by `source-app/dashboard/src/zero/queries.ts` and `apps/zbugs/shared/queries.ts`. ~24 entries covering shapes the fuzzer doesn't naturally hit. |
| 5a | `harness-coverage.ts` | `coverage_run.json`, `parity_gaps.md` | Hydration sweep — for each AST, opens TS+RS WebSockets in parallel, hydrates, hashes the row mirror, diffs. ~5s for the full corpus at `MAX_PARALLEL=8`. |
| 5b | `harness-advance-coverage.ts` | `advance_coverage_run.json`, `advance_parity_gaps.md` | Push sweep — batches 30 ASTs, hydrates all, runs one shared mutation block, waits, diffs the per-AST row delta. ~2 min for the full corpus. |
| – | `parity-report.ts` | `parity_report.md` | Combines hydration + advance reports into one markdown deliverable grouped by shape. |
| – | `ast-render.ts` | (importable) | AST → human-readable ZQL builder string. Used by the report writers and the `.zql.txt` sidecar. |

Legacy / reference (still works, kept for targeted single-bug debugging):
| File | Purpose |
|---|---|
| `harness.ts`, `harness-advance.ts`, `harness-remove-query.ts` | Per-pattern harnesses against `patterns.ts` (43 hand-written patterns). Slower but easier to attach a debugger to a single shape. |
| `patterns.ts` | 43 hand-written ZQL patterns, each commented with its source-app counterpart. |

## Files

| File | Purpose |
|---|---|
| `schema.sql` | PG DDL — 6 tables matching `zero-schema.ts` |
| `seed.sql` | Base seed (~30 rows) |
| `seed-extras.sql` | Additive seed extension (+30 rows) for fuller branch coverage |
| `zero-schema.ts` | Zero builder schema |
| `package.json` | tsx + ws + @rocicorp/zero |
| `run.sh` | Bring-up helper |

## Bring-up (first run)

1. **Postgres** — Docker container on port 6434:
   ```bash
   cd apps/zbugs && npm run db-up
   ```
   Then create the `parity` DB (separate from zbugs's `postgres` DB):
   ```bash
   PGPASSWORD=password psql -h 127.0.0.1 -p 6434 -U user -d postgres -c 'CREATE DATABASE parity'
   PGPASSWORD=password psql -h 127.0.0.1 -p 6434 -U user -d parity -f tools/ivm-parity/schema.sql
   PGPASSWORD=password psql -h 127.0.0.1 -p 6434 -U user -d parity -f tools/ivm-parity/seed.sql
   PGPASSWORD=password psql -h 127.0.0.1 -p 6434 -U user -d parity -f tools/ivm-parity/seed-extras.sql
   ```

2. **Schema deploy** (compiles `zero-schema.ts` → `schema.json`):
   ```bash
   cd tools/ivm-parity && npm install && npm run deploy-schema
   ```

3. **Start both caches** in separate terminals (or background processes):
   ```bash
   # TS cache on :4858
   npm --workspace=tools/ivm-parity run start-ts

   # RS cache on :4868
   npm --workspace=tools/ivm-parity run start-rs
   ```

4. **Run a sweep** (per TL;DR above).

## Hard rules (user-mandated)

- **TS side gets logs only**, gated behind `IVM_PARITY_TRACE=1`. No semantic edits to `packages/zql/`, `packages/zero-protocol/`, `packages/zero-cache/`.
- **RS-side fixes must cite TS file:line** being mirrored. No novel ideas.
- **Don't modify the test apparatus** (`tools/ivm-parity/*`, `patterns.ts`, `seed.sql`) to make divergences disappear. They're real bugs.
- **Don't UPDATE or DELETE existing seed rows.** Use `seed-extras.sql` for additive expansion.

## Sample report shape

```markdown
# IVM TS/RS Parity Report

## Summary
| Metric | Hydration | Advancement |
|---|---|---|
| Total ASTs | 1011 | 1011 |
| OK | 986 (97.5%) | 911 (90.1%) |
| Diverge | 25 | hyd=25, adv=75 |

## Hydration divergences (by shape)
| Shape | Count | Example IDs |
|---|---|---|
| or(or, csq) | 6 | fuzz_00146, fuzz_00150, fuzz_00375, … |
| EXISTS | 5 | fuzz_00079, fuzz_00083, … |
…
```

Each divergence in the report includes the rendered ZQL, the row diff, and a link back to the raw AST in `coverage_run.json` / `advance_coverage_run.json`.

## Extending

- **More shapes**: edit caps in `ast-fuzz.ts` (`MAX_WHERE_DEPTH`, `MAX_BRANCHES_PER_NODE`, etc.) or add a `cat_*` category in `whereShapes`.
- **More tables**: edit `zero-schema.ts` (and `schema.sql` to match). The fuzzer reads the schema at runtime — new tables auto-appear in the corpus.
- **More mutations**: edit `MUTATIONS` in `harness-advance-coverage.ts`. Each entry is `{label, run, undo}` SQL.
- **More data**: append to `seed-extras.sql` (additive only) and `psql … -f seed-extras.sql`.

Re-running `ast-fuzz.ts` is deterministic — same caps + same schema = same corpus (verify with `shasum ast_corpus.json`).
