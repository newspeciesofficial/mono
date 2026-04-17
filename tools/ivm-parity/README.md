# ivm-parity — regression-hunting test suite for Zero's IVM

Find queries where two IVM implementations (e.g. the reference TypeScript
zero-cache vs. the Rust zero-cache with `ZERO_USE_RUST_IVM_V2=1`) disagree.

The suite generates an exhaustive bounded sample of ZQL shapes, runs each
against both zero-cache backends backed by the same Postgres, and diffs
both the hydration payload and the advancement (push) row deltas. Every
divergence is a real query you can paste into a debugger — automatically
catalogued in `parity_report.md`.

No browser. No Playwright. Node + WebSocket + Postgres only.

---

## TL;DR

```bash
cd tools/ivm-parity
npm install

# first-time DB setup (~10s)
npm run db-create            # creates 'parity' database on localhost:6434
npm run deploy-schema        # compiles zero-schema.ts -> schema.json
npm run db-migrate           # applies schema.sql + seed.sql + seed-extras.sql

# start both caches (one terminal each, or backgrounded)
npm run start-ts             # TS zero-cache on :4858
npm run start-rs             # RS zero-cache on :4868 (uses ZERO_USE_RUST_IVM_V2=1)

# run the full sweep (~2 min), emits parity_report.md
npm test

# run only the Rust unit tests (no caches, no PG needed, ~3s)
npm run test-rs
```

See [INDEX.md](INDEX.md) for which doc to read for each goal.

---

## Test surface — three independent things to run

### 1. Rust unit tests (fastest — ~3s)

Pure Rust, no external services. Validates the RS IVM operator implementations
in isolation (`Filter`, `Exists`, `Take`, `Skip`, `Join`, etc.).

```bash
# From the repo root:
cargo test --manifest-path packages/zero-cache-rs/crates/sync-worker/Cargo.toml --lib
cargo test --manifest-path packages/zero-cache-rs/crates/sync-worker/Cargo.toml --tests

# Or via the package.json helper (from tools/ivm-parity/):
npm run test-rs
```

Expected: **576 unit tests + 5 integration tests pass** as of commit `597eb07ab`.

Includes regression tests that pin known parity fixes:
- `not_in_empty_array_passes_null_column` (ast_builder.rs)
- `not_in_nonempty_array_rejects_null_column` (ast_builder.rs)

### 2. Full parity sweep (hydration + advancement — ~2 min)

Generates the corpus, runs each AST against both caches, diffs results.
**Requires** both caches running and Postgres seeded.

```bash
cd tools/ivm-parity
npm test                     # end-to-end: fuzz + seed + hydrate + advance + report
```

Or step by step:
```bash
npm run fuzz                 # ~150ms — emits ast_corpus.json (~1000 ASTs)
npm run seed-corpus          # appends ~24 shipped-query archetypes
npm run sweep:hydrate        # ~5s — hydration divergences -> coverage_run.json
npm run sweep:advance        # ~2 min — push divergences -> advance_coverage_run.json
npm run report               # combines -> parity_report.md
```

Exit codes:
- `sweep:hydrate` and `sweep:advance` exit 0 only when zero divergences + zero errors.
- `report` always exits 0 — it just combines results.

### 3. Legacy per-pattern harness (slower, one-bug-at-a-time debugging)

For triaging a single shape under a debugger:
```bash
npm run harness-legacy       # runs patterns.ts (43 hand-written patterns)
```

---

## Environment variables

All optional — the suite uses sensible defaults that match the `npm run start-{ts,rs}` scripts.

| Variable | Default | Used by |
|---|---|---|
| `PARITY_TS_URL` | `ws://localhost:4858/sync/v49/connect` | harness-coverage, harness-advance-coverage |
| `PARITY_RS_URL` | `ws://localhost:4868/sync/v49/connect` | harness-coverage, harness-advance-coverage |
| `PARITY_PG_URL` | `postgresql://user:password@127.0.0.1:6434/parity` | harness-advance-coverage |
| `MAX_PARALLEL` | 8 | harness-coverage (WebSocket concurrency) |
| `BATCH_SIZE` | 30 | harness-advance-coverage (ASTs per batch) |
| `POKE_WAIT_MS` | 3000 | harness-advance-coverage (wait after mutation) |
| `HYDRATION_TIMEOUT_MS` | 15000 | harness-coverage (per-AST timeout) |
| `HYDRATE_TIMEOUT_MS` | 20000 | harness-advance-coverage (per-batch timeout) |
| `CORPUS_LIMIT` | 0 (no limit) | Both — cap for smoke-running first N entries |
| `MAX_WHERE_DEPTH` | 3 | ast-fuzz.ts (enumeration depth cap) |
| `MAX_BRANCHES_PER_NODE` | 2 | ast-fuzz.ts |
| `MAX_RELATED_DEPTH` | 2 | ast-fuzz.ts |
| `MAX_RELATED_PER_NODE` | 2 | ast-fuzz.ts |
| `MAX_RELATIONSHIP_HOPS` | 3 | ast-fuzz.ts |
| `MAX_NODES` | 8 | ast-fuzz.ts (global AST size cap) |
| `IVM_PARITY_TRACE` | unset | TS + RS caches: when `=1` emit parity-diff debug logs (zero overhead when unset) |

To point the suite at a differently-named running setup:
```bash
PARITY_PG_URL=postgresql://user:password@127.0.0.1:6434/mydb \
PARITY_TS_URL=ws://localhost:4858/sync/v49/connect \
PARITY_RS_URL=ws://localhost:4868/sync/v49/connect \
  npm test
```

---

## File map

### Run + regenerate
| File | Purpose |
|---|---|
| `package.json` | npm scripts for every action |
| `schema.sql` + `seed.sql` + `seed-extras.sql` | PG DDL and data (apply once) |
| `zero-schema.ts` | Zero schema; **the fuzzer reads this at runtime** — new tables auto-extend the corpus |
| `ast-fuzz.ts` | Phase 1 — bounded BFS over the AST ADT → `ast_corpus.json` |
| `seed-extractor.ts` | Phase 2 — appends ~24 shipped-query archetypes |
| `harness-coverage.ts` | Hydration sweep — payload hash diff |
| `harness-advance-coverage.ts` | Advancement sweep — row delta diff after a shared mutation block |
| `parity-report.ts` | Combines both sweeps into `parity_report.md` |

### Reference / dev-only
| File | Purpose |
|---|---|
| `ast-render.ts` | AST → human-readable ZQL, used by reports + `.zql.txt` sidecar |
| `harness.ts`, `harness-advance.ts`, `harness-remove-query.ts` | Legacy per-pattern harnesses — kept for single-shape debugging |
| `patterns.ts` | 43 hand-written ZQL patterns, each commented with the archetype it models |
| `check-replica.ts`, `query-runner.ts`, `verify-patterns.ts`, `dump-*.ts`, `probe_*.ts` | Scratch scripts from earlier debugging sessions |

### Generated (gitignored)
| File | Source |
|---|---|
| `ast_corpus.json`, `ast_corpus.zql.txt` | `npm run fuzz` + `npm run seed-corpus` |
| `coverage_run.json`, `parity_gaps.md` | `npm run sweep:hydrate` |
| `advance_coverage_run.json`, `advance_parity_gaps.md` | `npm run sweep:advance` |
| `parity_report.md` | `npm run report` — the combined deliverable |

---

## First-time DB bring-up (detailed)

Run once per fresh PG instance:

```bash
# 1. Start Postgres 16+ on port 6434
cd apps/zbugs && npm run db-up

# 2. Create + seed the parity database
cd ../../tools/ivm-parity
npm run db-create
npm run db-migrate
```

The `parity` DB is deliberately separate from `apps/zbugs`'s `postgres` DB so
their tables can't collide. The `seed-extras.sql` is additive — re-running it
errors on duplicate PKs (harmless — intent is to apply once).

---

## Hard rules (read before "fixing" a divergence)

1. **Don't modify the test apparatus to hide a divergence.** The failing query is a real bug.
2. **Don't UPDATE or DELETE existing seed rows.** Add new rows only via `seed-extras.sql`.
3. **RS fixes must cite TS file:line.** No novel RS logic — the harness treats TS as reference.
4. **Don't edit TS source** except env-gated `console.log` instrumentation (revert after use).
5. **Re-run the full sweep after every fix** to confirm divergence count drops and nothing regresses.

---

## Troubleshooting

**Harness shows wildly inflated divergences (hundreds)**
Your RS cache likely died mid-sweep. Check:
```bash
lsof -i :4868 | grep LISTEN    # should show a PID
```
If no PID, restart with `npm run start-rs`.

**PG "database does not exist"**
The `parity` DB wasn't created. Run `npm run db-create && npm run db-migrate`.

**Divergences on `simple NOT IN`/`simple !=`/`simple NOT LIKE`/`simple >` shapes**
These are nullable-column-semantics paths. The known regression test for
empty NOT IN is in `ast_builder.rs`. Similar classes may surface for other
negated/ordering operators — catalog them by re-running the sweep.

**Want to change the schema**
Edit `zero-schema.ts` + `schema.sql`, then:
```bash
npm run deploy-schema        # regenerates schema.json
npm run db-migrate           # re-applies DDL + seed
# Restart both caches; replicas wipe on schema change
rm /tmp/ivm-parity-*.db*
npm run start-ts
npm run start-rs
npm run fuzz                 # fuzzer auto-picks up new schema
```

**RS cache not seeing changes after a code fix**
Rebuild the napi binding and restart RS:
```bash
cd packages/zero-cache-rs/crates/shadow-ffi && npm run build:debug
cp *.darwin-arm64.node ../../../zero/out/zero-cache-shadow-ffi/
cd ../../../../tools/ivm-parity
lsof -i :4868 | awk '/LISTEN/ {print $2}' | xargs kill
npm run start-rs             # & to background, or separate terminal
```

---

## Extending the corpus

- **More shape categories:** add a `cat_*` block in `ast-fuzz.ts` `whereShapes` / `relatedShapes`.
- **More data:** append to `seed-extras.sql` then `npm run db-migrate`.
- **More mutations:** edit `MUTATIONS` in `harness-advance-coverage.ts`.
- **Different schema:** edit `zero-schema.ts`. Fuzzer reads it at runtime — new tables auto-appear.

`npm run fuzz` is deterministic — same schema + same caps = same corpus. Verify with `shasum ast_corpus.json`.
