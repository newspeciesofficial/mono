# ivm_v2 — Production Handoff

Everything you need to test, set up, and migrate. The conversation that got us
here is compacted; this doc is the stable reference.

---

## What's built and where

| Layer | Location | What it is |
|---|---|---|
| Operators (new) | `packages/zero-cache-rs/crates/sync-worker/src/ivm_v2/` | FilterT · SkipT · TakeT · ExistsT · JoinT · SourceT · BatchingT. Single-threaded, `&mut self`, `impl Iterator` return. Zero Arc/Mutex on operators. |
| Pipeline driver | `packages/zero-cache-rs/crates/sync-worker/src/view_syncer_v2/` | `PipelineV2` with full IVM API: init / add_query / add_queries / advance / advance_without_diff / remove_query / get_row / queries / hydration_budget_breakdown / destroy. AST→Chain builder supports `=`, `!=`, `<`, `>`, `IS`, `IS NOT`, `IN`, `NOT IN`, `LIKE`, `ILIKE`, `AND`, `OR`, `EXISTS`, `NOT EXISTS`, limit, start bound, multiple EXISTS, and 1-level hierarchical join. |
| SQLite source | `view_syncer_v2/sqlite_source.rs` | Channel-backed `ivm_v2::Input`. One worker thread per source, zero-copy rows to chain. |
| napi exports | `packages/zero-cache-rs/crates/shadow-ffi/src/exports/pipeline_v2.rs` | 16 TS-callable functions. Crate name is misleading — no diff verification; it's production napi plumbing. |
| TS wrapper | `packages/zero-cache/src/services/view-syncer/rust-pipeline-driver-v2.ts` | `RustPipelineDriverV2` — mirrors `RustPipelineDriver`'s public surface. |
| Syncer hook | `packages/zero-cache/src/server/syncer.ts` | `ZERO_USE_RUST_IVM_V2=1` selects v2. |

---

## Enable v2 in production — one env var

```bash
ZERO_USE_RUST_IVM_V2=1
# Everything else stays the same: ZERO_UPSTREAM_DB, ZERO_REPLICA_FILE, etc.
```

`ZERO_USE_RUST_IVM_V2` takes precedence over `ZERO_USE_RUST_IVM`. Both off =
original TS IVM (default).

---

## How to build + run locally

### 1. Build the Rust napi module

```bash
cd packages/zero-cache-rs/crates/shadow-ffi
npm install
npm run build   # runs `napi build --platform --release`
# Produces ./index.js and ./zero-cache-shadow-ffi.<platform>.node
```

### 2. Run Rust tests

```bash
cd packages/zero-cache-rs
cargo test -p zero-cache-sync-worker   # 1093 lib tests
cargo test -p zero-cache-sync-worker --test pipeline_v2_integration
cargo test -p zero-cache-sync-worker --test pipeline_v2_driver_integration
cargo test -p zero-cache-sync-worker --test ivm_v2_integration
```

### 3. Run TS tests

```bash
cd packages/zero-cache
npm run check-types   # must be clean
npm run test          # vitest
```

### 4. Run zbugs (reference app) against v2

```bash
cd apps/zbugs
npm run db-up                # Docker PG
ZERO_USE_RUST_IVM_V2=1 npm run zero-cache-dev
# In another shell:
npm run dev                  # Vite dev server
```

Hit `http://localhost:3000` in a browser. Sidebar + thread should render.

---

## What v2 does that v1 couldn't, and vice versa

| Feature | v1 (old TS/Rust-via-ivm) | v2 (this port) |
|---|---|---|
| `=`, `!=`, `<`, `>`, `LIKE`, `ILIKE`, `IN`, `NOT IN` | ✅ | ✅ |
| `AND`, `OR`, nested | ✅ | ✅ |
| `EXISTS`, `NOT EXISTS`, multiple | ✅ | ✅ |
| `limit`, `start` cursor | ✅ | ✅ |
| 1-level hierarchical `related` | ✅ | ✅ |
| N-level nested `related` | ✅ | ❌ — only one level |
| Scalar subqueries | ✅ | ❌ — no runtime reactivity |
| Partitioned Take | ✅ | ❌ — global Take only |
| `WAL2 BEGIN CONCURRENT` read coherence | ✅ | ❌ — single connection per chain, per-advance consistency |
| Companion pipelines, scalar subquery reactivity | ✅ | ❌ |
| MeasurePushOperator instrumentation | ✅ | ❌ |
| Inspector delegate | ✅ | ❌ |
| `Arc<Mutex<>>` deadlock class | Known bugs | **Structurally impossible** |
| Multi-operator push propagation | Bugs | **Fixed** |
| Flicker on at-limit eviction | Yes (unsolved) | **Gone** — structural fix |

**If you hit a query v2 can't handle**, it throws `AstBuildError`. Fallback
path: clear the env var, restart zero-cache. v2 does not silently degrade.

---

## What's NOT yet in v2 (known-missing, not TODO comments)

1. **N-level nested relations.** AST with `related[].subquery.related` — only one hop works today.
2. **Scalar subqueries (`zsubq_` prefix) with live reactivity.** v2 resolves
   them once at add_query time, not as a live companion pipeline.
3. **Partitioned Take.** `limit` is global per query; multi-partition with
   N rows per partition is unsupported.
4. **Read-consistency across advance cycles.** Each chain's source opens its
   own connection; reads during a single advance are consistent within that
   chain but not across chains. This is fine for single-core; matters when
   rayon lands.
5. **Constraint-routed `get_row`.** Current impl does a linear scan via the
   chain's source. For a handful of tables this is fine; if `get_row` shows up
   in a profile, replace with `SELECT … WHERE pk IN (?)`.
6. **Parallel add_queries.** Sequential for now; rayon-pool parallelism is
   the obvious enhancement if hydration latency dominates.

None of these are blocking to ship single-core. They're listed so you know
what might surface in stress testing.

---

## Migration plan (ordered)

1. **Dev smoke.** Turn on `ZERO_USE_RUST_IVM_V2=1` against zbugs in dev. If
   sidebar + thread load and show correct rows, basic chains work.
2. **Staging rollout.** Same flag in staging. Use shadow traffic if you
   have it; otherwise deploy to one staging instance, confirm no errors in
   the zero-cache logs, let it sit for a few hours.
3. **Observe on Grafana.** The dashboards that track `zero.sync.advance-time`
   / `cvr.flush-time` will see v2 numbers. Expect:
   - Advance wall time down, CPU (not I/O) down
   - Hydration time: similar or slightly higher due to the eager collect
     between transformers (gets closer to v1 after `for_each_row` is wired
     through the source, TBD)
4. **First prod canary.** One instance. Keep watch on:
   - PG/SQLite error rates (should stay at baseline)
   - WebSocket poke frequency (should match v1 behaviour)
   - Client-reported inconsistencies (zero; open a bug if anything)
5. **Full rollout.** Flip the fleet. Keep `ZERO_USE_RUST_IVM` and stock TS
   paths as rollback options for one release cycle, then delete them.

---

## Rollback

```bash
# Unset the flag in your env manager and restart zero-cache.
unset ZERO_USE_RUST_IVM_V2
```

Behaviour falls back to `ZERO_USE_RUST_IVM` if set, else to pure TS IVM.

---

## V1 cleanup — already done

Deleted in the cleanup pass after v2 wired up:

```
packages/zero-cache-rs/crates/sync-worker/src/builder/
packages/zero-cache-rs/crates/sync-worker/src/view_syncer/
packages/zero-cache-rs/crates/sync-worker/src/zqlite/
packages/zero-cache-rs/crates/sync-worker/src/planner/
packages/zero-cache-rs/crates/sync-worker/src/query/
packages/zero-cache-rs/crates/sync-worker/src/ivm/memory_source.rs
packages/zero-cache-rs/crates/shadow-ffi/src/exports/ivm_constraint.rs
packages/zero-cache-rs/crates/shadow-ffi/src/exports/ivm_data.rs
packages/zero-cache-rs/crates/shadow-ffi/src/exports/lsn.rs
packages/zero-cache-rs/crates/shadow-ffi/src/exports/pipeline_driver.rs
packages/zero-cache-rs/crates/shadow-ffi/src/exports/table_source.rs
packages/zero-cache-rs/crates/sync-worker/tests/integration_pipeline_driver.rs
packages/zero-cache/src/services/view-syncer/rust-pipeline-driver.ts
packages/zero-cache/src/shadow/diff.ts
packages/zero-cache/src/shadow/ivm-data.test.ts
packages/zero-cache/src/shadow/pipeline-driver.test.ts
packages/zero-cache/src/shadow/README.md
```

The `ZERO_USE_RUST_IVM` env var branch was removed from `syncer.ts`.
`RustPipelineDriver` was removed from the `AnyPipelineDriver` union in
`view-syncer.ts`. Rollback from v2 now goes directly to stock TS IVM
(`unset ZERO_USE_RUST_IVM_V2`).

## Remaining dead code in `crates/sync-worker/src/ivm/`

Most v1 operators still live in `ivm/` as dead modules. They compile
(no cross-deletion left) but nothing references them. Trimming is
mechanical — each module has a `mod foo;` line in `ivm/mod.rs`:

```
exists, fan_in, fan_out, filter, filter_gen, filter_operators,
filter_push, flipped_join, join, maybe_split_and_push_edit_change,
memory_storage, push_accumulated, skip, stopable_iterator, stream,
take, union_fan_in, union_fan_out
```

These are kept for now because v2 imports a handful of shared
sub-items from `ivm::` (change, constraint, data, schema, source,
operator, join_utils). Splitting the shared bits from the v1
operators is a follow-up refactor; not blocking prod.

Also still around: `crates/shadow-ffi/src/handle.rs` — generic
`Handle<T>` scaffolding, not used by `pipeline_v2.rs`. Kept as
useful infra for future napi handles.

## Rename (still TODO)

```
crates/shadow-ffi/  →  crates/napi-exports/   # name is misleading now
```

---

## Where the tests live

```
crates/sync-worker/src/ivm_v2/*/tests/*.rs          # unit tests per operator
crates/sync-worker/tests/pipeline_v2_integration.rs # 5 end-to-end
crates/sync-worker/tests/pipeline_v2_driver_integration.rs # 4 driver API
crates/sync-worker/tests/ivm_v2_integration.rs      # 2 chain assembly
crates/libsqlite3-sys/tests/smoke_*.rs              # WAL2/snapshot semantics
```

**1093 Rust tests, all passing.** `npm --workspace=zero-cache run check-types` clean.

---

## If something breaks

1. **Rust panic in worker thread** → check `[TRACE ivm_v2] …` lines in
   zero-cache stderr. Every operator traces push enter/exit with `op=` and
   relevant state (`size`, `has_bound`, etc).
2. **Query produces wrong rows** → run the AST through
   `ast_builder::ast_to_chain_spec` in a Rust unit test with the failing
   row shape. Unit-repro is the fast path.
3. **`AstBuildError`** → query uses an op v2 doesn't yet support (e.g.
   multi-level related). Fall back via `unset ZERO_USE_RUST_IVM_V2`.
4. **Napi load failure** → `[shadow] native module not loaded` in logs.
   Rebuild: `cd packages/zero-cache-rs/crates/shadow-ffi && npm run build`.

---

## Quick topology (for mental model)

```
   JS side                                 Rust side
┌──────────────┐                    ┌────────────────────────────┐
│ ViewSyncer   │                    │  PipelineV2                │
│              │   napi call        │   ├─ sources (SqliteSource)│
│ RustPipeline │  ───────────────►  │   │  ├─ ChannelSource      │
│ DriverV2     │                    │   │  │  └─ worker thread   │
│              │  ◄───────────────  │   │  │     └─ rusqlite     │
│              │   rows / ack       │   ├─ chains (one per query)│
└──────────────┘                    │   │  ├─ Filter / Skip /    │
       │                            │   │  │  Take / Exists / Join│
       │ WebSocket poke             │   │  └─ Transformer chain  │
       ▼                            └────────────────────────────┘
  Browser (xyne-spaces, zbugs, …)
```

One zero-cache worker = one `PipelineV2`. One client group = one chain per
query. Single-threaded per chain. Rayon parallelism is a future additive
change: wrap the chain's thread in a rayon worker and share the
`Arc<Mutex<Connection>>` per CG.
