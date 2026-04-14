# Validation report: pipeline-driver

## Scope
- `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts` — 1063
- `packages/zero-cache/src/services/view-syncer/remote-pipeline-driver.ts` — 697
- `packages/zero-cache/src/services/view-syncer/pool-thread-manager.ts` — 301
- `packages/zero-cache/src/workers/pool-protocol.ts` — 333
- `packages/zero-cache/src/server/pool-thread.ts` — 925
- `packages/zero-cache/src/services/view-syncer/pool-thread-mapper.ts` — NOT PRESENT in tree (referenced only in comments of pool-thread-manager.ts:17, 270)
- Total LOC: 3319

## Summary
| metric | count |
|---|---|
| TS constructs used | 18 |
| Constructs covered by guide | 14 |
| Constructs NOT covered | 4 |
| Libraries imported | 8 (out-of-scope zql/zqlite/zero-protocol/shared counted as internal; @rocicorp/logger, node:worker_threads, node:crypto, node:os, node:path are external/stdlib) |
| Libraries covered by guide | 6 |
| Libraries NOT covered | 2 |
| Risk-flagged items the module depends on | 1 |

## Constructs used
| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class` with `#private` fields | pipeline-driver.ts:129 (`PipelineDriver`), remote-pipeline-driver.ts:213, pool-thread-manager.ts:57 | A1, A23 | covered |
| Tagged union discriminated by `type` + `switch` | pool-protocol.ts:89-98 (`PoolWorkerMsg`), :296-310 (`PoolWorkerResult`); pool-thread.ts:875 switch; remote-pipeline-driver.ts:546 switch | A6 | covered |
| Generator `function*` + `Iterable<…>` | pipeline-driver.ts:413 (`*addQuery`), :639 (`*#advance`), :818 (`*#push`), :880 (`*stream`), :1000 (`*hydrate`) | A22, A16 (sync generator variant) | covered (A22) |
| Async generator `async function*` + `for await` | remote-pipeline-driver.ts:458 (`async *#streamAdvanceChanges`) | A16 | covered |
| Iterator protocol `[Symbol.iterator]()` | pool-thread.ts:625 | A22 | covered |
| Promise + manual resolver (captured `resolve`/`reject`) | remote-pipeline-driver.ts:108 (`#beginWaiter`), :199 (`#waiter`), :521 `new Promise`; used for request/response and for streaming begin/pull/done | Library row `@rocicorp/resolver` → `tokio::sync::oneshot` | covered |
| `Promise.all(exits)` | pool-thread-manager.ts:172 | A17 | covered |
| Object spread `{...msg, requestId}` | remote-pipeline-driver.ts:520 | A20 | covered |
| `Map<K,V>` / `Set<K>` | pipeline-driver.ts:130, :132, :140, :141; remote-pipeline-driver.ts:219, :220; pool-thread-manager.ts:54; pool-thread.ts:109 | A19 (`Record`/`HashMap`) | covered |
| `WeakMap<Database, CostModel>` | pipeline-driver.ts:142, :302-310 | D2 | covered |
| Error subclass (`ResetPipelinesSignal` is thrown as control flow) | pipeline-driver.ts:541, :804; pool-thread.ts:438, :242 (`isResetSignal`) | A15, F1 | covered (Redesign — thrown control-flow → `Result`/`Err`) |
| `try/finally` to clean up per-request state | pipeline-driver.ts:439-570 (`#hydrateContext`), :660-740 (`#advanceContext`) | A15 | covered |
| `worker_threads.Worker` + `MessageChannel` + `MessagePort` (syncer side) | pool-thread-manager.ts:24, :186-223 (spawn, transferList, `on('message'|'messageerror'|'error'|'exit')`) | B4 | covered (Redesign) |
| `worker_threads` `workerData` + `parentPort`-replacement port | pool-thread.ts:32, :77-89, :871 (`port.on('message', …)`), :861 (`port.close()`) | B4 | covered |
| `setImmediate` batching a round-robin scheduler | pool-thread.ts:582 (`setImmediate(drainAdvanceQueue)`) | B7 | covered |
| `performance.now()` monotonic timing | pool-thread.ts:147, :370, :381, :485, :567, :670, :705, :745; remote-pipeline-driver.ts:363, :437, :549, :566, :582, :640 | — | **NEW** — see `new-patterns/pipeline-driver-performance-now.md` |
| `process.exit(0)` from a worker thread | pool-thread.ts:864 | B6 | covered |
| `randomUUID()` from `node:crypto` | pool-thread.ts:29, :98 | — | **NEW** — see `new-patterns/pipeline-driver-random-uuid.md` |
| `tmpdir()` + `path.join()` for per-thread scratch dir | pool-thread.ts:30, :31, :98 | — | **NEW** — see `new-patterns/pipeline-driver-tmpdir-path-join.md` |
| `setTimeout(..., 5000).unref()` shutdown grace + terminate fallback | pool-thread-manager.ts:159-169 | B7 + C3 | covered (terminate fallback is the C3 `timeout(grace, handle)` pattern) |
| `SharedArrayBuffer` + `TextEncoder`/`TextDecoder` zero-copy batch | pool-protocol.ts:316-333 (`encodeBatch`/`decodeBatch`), pool-thread.ts:136, remote-pipeline-driver.ts:562 | — | **NEW** — see `new-patterns/pipeline-driver-sharedarraybuffer-batch.md` |
| Readonly arrays / tuple types with `readonly` modifier | pipeline-driver.ts:59-83 (`RowAdd` etc.); pool-protocol.ts:110-117 (`DriverState`) | A1 (struct fields) | covered |
| Sticky routing per `clientGroupID` (driver.map inside a `ThreadEntry`) | pool-thread-manager.ts:112-137 (`registerDriver`/`unregisterDriver`); dispatch on `msg.clientGroupID` :226-241. The actual hash→index mapping ("PoolThreadMapper") is NOT in scope — only referenced in comments pool-thread-manager.ts:17, :270 | C5 | covered (the mapper owning the hash is out of scope; the per-thread registry is a plain `HashMap`) |
| Push-pull buffer (AdvanceStream) for streaming IPC | remote-pipeline-driver.ts:105-202 | C1 (bounded channel with implicit backpressure) | covered — BUT see caveat in Ready-to-port below: the TS buffer is UNbounded; Rust port should use a bounded `mpsc` |
| `AsyncIterable<T>` returned from `advance()` / `addQuery()` | remote-pipeline-driver.ts:425, :353, :458 | A16 | covered |
| `assert(...)` runtime invariant guards | pipeline-driver.ts throughout; remote-pipeline-driver.ts:316, :359 | F1 (`anyhow!`/`assert!`) | covered |
| `async`/`await` top-level methods (`init`, `reset`, `advance`, `getRow`, …) | remote-pipeline-driver.ts:310, :325, :339, :353, :410, :425 | A16, A17 | covered |
| `JSON.stringify` / `JSON.parse` over batch payload | pool-protocol.ts:324, :332 | E1/E2 (JSON via `serde_json`) | covered |
| `Object.fromEntries` / `Object.entries` | pipeline-driver.ts:504, :992 | A19 (iterate `HashMap`) | covered |
| `setOutput({push: cb})` Output interface from IVM operator | pipeline-driver.ts:469, :525 | A5 (trait) — interface lives in zql (out of scope per prompt); only the call site is in scope | covered as a call site |
| Fire-and-forget `removeQuery` with later "pending-response" cleanup | remote-pipeline-driver.ts:382-408 | C1 | covered |
| Worker lifecycle events (`worker.on('error'|'exit')`, `port.on('messageerror')`) | pool-thread-manager.ts:213-220 | B4 | covered |
| Thread restart on crash preserving `clientGroupID → driver` map | pool-thread-manager.ts:256-290 | B4 + C11 | covered (the re-registration of drivers under a new port maps to swapping a Vec entry and notifying each `Arc<Driver>`) |

## Libraries used
| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `@rocicorp/logger` (`LogContext.withContext`) | `tracing` + `tracing-subscriber` | covered |
| `node:worker_threads` (`Worker`, `MessageChannel`, `MessagePort`, `workerData`) | `std::thread::spawn` + `tokio::sync::mpsc` / `crossbeam-channel` (Part 1 B4) | covered |
| `node:crypto` (`randomUUID`) | `uuid` crate (already in Part 4 "present"); `uuid::Uuid::new_v4().to_string()` | **NOT in pattern table** — see `new-patterns/pipeline-driver-random-uuid.md` (crate present, pattern not) |
| `node:os` (`tmpdir`) | `std::env::temp_dir()` | **NOT in crate/pattern table** — see `new-patterns/pipeline-driver-tmpdir-path-join.md` |
| `node:path` (`path.join`) | `std::path::Path::join` / `PathBuf` | **NOT in pattern table** — see same file |
| `node:perf_hooks` (implicit global `performance.now()`) | `std::time::Instant` | **NOT in pattern table** — see `new-patterns/pipeline-driver-performance-now.md` |
| Internal: `zql/src/builder/builder.ts`, `zql/src/ivm/{operator,source,schema,change,data}.ts`, `zqlite/src/{table-source,database-storage,resolve-scalar-subqueries,sqlite-cost-model,db}.ts`, `zero-protocol/*`, `shared/*` | out of scope per WORKER-PROMPT (Symbols imported, don't re-read) | N/A |
| Internal: `../auth/load-permissions.ts`, `../config/zero-config.ts`, `../db/lite-tables.ts`, `../observability/metrics.ts`, `../server/inspector-delegate.ts`, `../server/logging.ts`, `../types/{row-key,shards}.ts`, `./client-schema.ts`, `./snapshotter.ts` | other-agent scopes | N/A |

## Risk-flagged items depended on
- **Full IVM operator graph + `zql` builder + `zqlite` table source** (comparison row 14, status "Stub"). `PipelineDriver` is fundamentally a thin orchestrator around `buildPipeline` + `Input.fetch`/`setOutput`/`destroy` + `TableSource`. Porting this file is blocked on porting `zql/src/ivm/**` and `zqlite/src/table-source.ts`, `zqlite/src/resolve-scalar-subqueries.ts`, `zqlite/src/sqlite-cost-model.ts`. These are the ~16.5K TS LOC noted as "~5% covered" in `ts-vs-rs-comparison.md`. Imports: pipeline-driver.ts:9-36.
- **`Snapshotter` / `ResetPipelinesSignal`** (comparison row 13, "Port partial" — no leapfrog, no BEGIN CONCURRENT). Used at pipeline-driver.ts:57, :195-226, :293-298, :621-635, :733-737; pool-thread.ts:46-48, :195-207, :242, :438-459. The `ResetPipelinesSignal` thrown from `#push`/`#shouldAdvanceYieldMaybeAbortAdvance` and caught in `handleAdvance` is control-flow-as-throw (A15 Redesign). Rust port must model this as an explicit `Result` variant, not a panic/error propagation — see pool-thread.ts:437-483 where the handler catches it, does `state.driver.reset()` + `advanceWithoutDiff()` + re-hydrate each query, and keeps streaming.
- **BEGIN CONCURRENT** (guide Part 3 item 2). Not used directly in these files, but `Snapshotter`'s semantics rely on it in TS (multiple concurrent readers). The pool-thread model (1 CG per thread, all IVM sequential on that thread) partially mitigates this, but advance on multiple CGs across pool threads still assumes independent read snapshots.

## Ready-to-port assessment

This module cannot be translated mechanically today. The blockers are, in order:

1. **IVM engine** — `PipelineDriver` exists to drive `buildPipeline(...)` from zql with a set of `TableSource`s from zqlite. Until `zql/src/ivm/**` and `zqlite/src/table-source.ts` have a Rust equivalent, pipeline-driver.ts cannot have one. The comparison table puts this at ~5% covered. This is the whitepaper-sized blocker, not a pipeline-driver concern.

2. **Snapshotter** — pipeline-driver calls `snapshotter.init/current/advance/advanceWithoutDiff/destroy`. Its Rust equivalent (`crates/sync/src/snapshotter.rs`) is "Port partial" (row 13) with no leapfrog and no BEGIN CONCURRENT.

3. **`ResetPipelinesSignal` control-flow** (A15 Redesign). The TS code throws this from deep inside generator iteration (pipeline-driver.ts:541, :804) and catches it at two specific places — the view-syncer for TTL limits, and pool-thread.ts:438 for in-advance reset. In Rust this has to become an explicit `enum` variant threaded through each `yield` of the generator (or an `Err(ResetSignal)` on the `Stream::Item`). Every caller becomes a `match` instead of a `try/catch`. This is not hard, just mandatory.

4. **`SharedArrayBuffer` zero-copy batches** (pool-protocol.ts:316-333) have no direct guide entry. The closest Rust equivalent for the same goal — avoid structured-clone of large Vecs between worker threads — is to put the batch on an `Arc<[u8]>` / `bytes::Bytes` in a `crossbeam-channel` or `tokio::sync::mpsc` (both `Send` a `*const T` rather than deep-copying). Because pool-thread.ts:125 uses `port.postMessage`, the JSON-encode → SAB → JSON-decode round trip is specifically designed to survive the structured-clone boundary of `MessageChannel`. In Rust, once workers are plain `std::thread`s and messages travel over `mpsc`, the SAB trick is unnecessary: pass `Vec<RowChange>` or `Arc<Vec<RowChange>>` directly. This simplifies the Rust port but means the pattern is a JS-only workaround, not a pattern to replicate.

5. **`performance.now()` timing diagnostics.** `AdvanceStream` exposes a whole `PoolTimings` payload (remote-pipeline-driver.ts:77-87) keyed off `performance.now()` read independently on syncer-thread and pool-thread. The Rust port uses `std::time::Instant` for monotonic durations; the pattern is otherwise identical. Entry in the guide required.

6. **Sticky routing** (C5 — covered): the **index-selection** lives in `pool-thread-mapper.ts`, which is **not present** in the tree today — only referenced in comments of `pool-thread-manager.ts:17, 270`. Either it was removed in favour of having ViewSyncer pick the index, or this is a pending file. The routing inside this scope (clientGroupID → driver in a `HashMap<String, RemotePipelineDriver>` per thread) is plain C5 mechanics once the mapper is written. The Rust port of the mapper is trivial (`ahash` + `Vec<mpsc::Sender<_>>` modulo N), but the design decision needs confirmation before porting.

7. **Unbounded push-pull buffer in `AdvanceStream`** (remote-pipeline-driver.ts:106 `#buffered: Array<…>`). This is unbounded in TS and relied on the syncer event loop consuming quickly. In Rust it must be a **bounded** `tokio::sync::mpsc::channel(cap)` (C1), otherwise a slow `for await (const change of changes)` consumer coupled with a fast pool thread will grow memory without limit. The guide already says this under C1 — just call it out in the deviations section.

What must be added to the guide before porting starts:
- **New pattern file**: `performance.now()` → `std::time::Instant`.
- **New pattern file**: `crypto.randomUUID()` → `uuid::Uuid::new_v4()` (crate present in Part 4 but no pattern row; add an entry, probably in B or D).
- **New pattern file**: `tmpdir()` + `path.join()` → `std::env::temp_dir()` + `PathBuf`. Short rule; add to B (Node stdlib).
- **New pattern file**: `SharedArrayBuffer` + `postMessage` pair as a zero-copy workaround for structured clone. Mark as "not needed in Rust" so the port does not replicate it.
- **Explicit confirmation** of `PoolThreadMapper`'s fate — either add the file or remove the comment references.

Files read in full: all five files in scope. `pool-thread-mapper.ts` was listed as optional and does not exist in the tree.

## New patterns files produced
- `docs/agent-reports/new-patterns/pipeline-driver-performance-now.md`
- `docs/agent-reports/new-patterns/pipeline-driver-random-uuid.md`
- `docs/agent-reports/new-patterns/pipeline-driver-tmpdir-path-join.md`
- `docs/agent-reports/new-patterns/pipeline-driver-sharedarraybuffer-batch.md`
