# Pool Worker Threads v3 — Architecture

Alignment doc. Everything below is verified from the code (with file:line references) or is an explicit decision we made together. If something here contradicts the actual code, fix the doc.

---

## 1. Goal

Move all SQLite work (hydration + advance + getRow) off the syncer's main thread onto worker threads. Free the syncer's event loop so it only handles WebSocket I/O, CVR PG writes, and poke delivery. Target: higher cross-CG throughput on multi-core machines without the v2 regressions.

---

## 2. Constraints (decided)

- **4 pool threads per syncer**
- **15-30 syncers per pod** (down from 150)
- **One client group (CG) is sticky to one pool thread for its lifetime**
- **Pool thread does ALL SQLite operations** — hydration, advance, getRow
- **Target density**: most pool threads hold 1-2 CGs. If a pool thread holds more, they're processed strictly sequentially (no yields).

---

## 3. Boundary: what moves and what stays

| Stays on syncer main thread | Moves to pool thread |
|---|---|
| WebSocket handlers, auth/JWT | Snapshotter (BEGIN CONCURRENT, read-marks) |
| Client message handlers (initConnection, changeDesiredQueries, updateAuth, deleteClients, inspect) | PipelineDriver (init, advance, addQuery, removeQuery, getRow, reset, destroy) |
| ViewSyncer `#lock` (per-CG serialization) | IVM operator graph (joins, filters, exists) |
| Replicator IPC subscription (version-ready) | |
| CVR load + flush (CVRStore, PG I/O) | |
| Poke delivery (ClientHandler, WebSocket sends) | |
| TTL clock, drain coordinator | |
| `PoolThreadMapper` (CG → pool thread assignment) | |

**Triggers for operations always come from the syncer side**: WebSocket messages, replicator IPC, TTL timers. The pool thread is a pure compute engine — it responds to commands, doesn't initiate anything.

---

## 4. Advance flow: Before vs After

### Before (stock 1.0.0) — everything on syncer main thread

Steady-state path (pipelines already synced):

```
SYNCER MAIN THREAD (one event loop per ViewSyncer)
  │
  ▼
#run loop — for await (state of #stateChanges)
  receives 'version-ready' from replicator IPC       view-syncer.ts:461
  │
  ▼
#runInLockWithCVR — acquires this.#lock              view-syncer.ts:392
  │
  ├─ First time only: #cvrStore.load()               view-syncer.ts:418-422
  └─ Subsequent: update ttlClock                     view-syncer.ts:425-431
  │
  ▼
#advancePipelines                                     view-syncer.ts:2059
  1. PipelineDriver.advance(timer) — SYNC            pipeline-driver.ts:606
     ├─ Snapshotter.advance()                         snapshotter.ts:175
     │    ├─ rollback prev connection
     │    ├─ BEGIN CONCURRENT on leapfrog connection  snapshotter.ts:298
     │    └─ SELECT stateVersion FROM "_zero.replicationState"  replication-state.ts:202
     └─ returns generator (not executed until iterated)
  
  2. #processChanges — consumer loop                  view-syncer.ts:1971
     for (change of generator):
       if change === 'yield':
         await timer.yieldProcess()                   → setImmediate via timeSliceQueue
         continue
       accumulate into rows map
       if rows.size % CURSOR_PAGE_SIZE(10000) === 0:  view-syncer.ts:2039
         await processBatch()                          → updater.received() + pokers.addPatch()
     if rows.size > 0: processBatch()                 view-syncer.ts:2043-2045
  
  3. #flushUpdater — CVR write to PG                  view-syncer.ts:879
     (CVRQueryDrivenUpdater.flush → CVRStore.flush)
  
  4. pokers.end(finalVersion) — pokeEnd WebSocket     view-syncer.ts:2117
     Promise.allSettled fan-out to all clients        client-handler.ts:95
  │
  ▼
this.#lock released (auto via withLock)
```

**Syncer main thread does everything**: SQLite, PG, WebSocket, all on one event loop. `'yield'` markers interleave IVM with I/O via the global `timeSliceQueue` lock (view-syncer.ts:2224) which serializes yields across all CGs on the syncer.

### After (pool threads v3) — SQLite/IVM on pool thread

```
SYNCER MAIN THREAD                               POOL THREAD (assigned to this CG)
  │                                                 │
  ▼                                                 │
#run receives version-ready                         │
  │                                                 │
  ▼                                                 │
#runInLockWithCVR — acquires this.#lock            │
  │                                                 │
  ▼                                                 │
#advancePipelines:                                  │
  await remotePipelineDriver.advance(timer)         │
     │                                              │
     │── {advance, requestId, clientGroupID} ────► │
     │                                              ▼
     │                                         drivers.get(cgID).advance(localTimer)
     │                                         (same PipelineDriver class as stock)
     │                                              │
     │ ◄── {advanceBegin, requestId, version,      │
     │      numChanges, snapshotMs} ────────────── │
     │                                              │
     │                                         for change of generator:
     │                                           if change === 'yield': continue   ← skip, no yields
     │                                           batch.push(change)
     │                                           if batch >= BATCH_SIZE: post batch
     │ ◄── {advanceChangeBatch, requestId, ...} ── │
     │                                              │
     │ ◄── {advanceComplete, requestId} ─────────── │
     │                                              │ (pool thread ready for next msg)
     ▼                                              
  for await (change of stream):
    accumulate rows
    if rows.size % 10000 === 0: processBatch()
  final processBatch()
  │
  ▼
  #flushUpdater — CVR write to PG (unchanged)
  │
  ▼
  pokers.end — pokeEnd WebSocket (unchanged)
  │
  ▼
this.#lock released
```

### What changed vs stock

| Aspect | Before | After |
|---|---|---|
| Location of IVM + SQLite reads | Syncer main thread | Pool thread (sticky per CG) |
| Syncer event loop during IVM | CPU-bound on IVM, yields every 10ms | Free (awaiting MessagePort) |
| `'yield'` markers | Consumer calls `timer.yieldProcess()` → `setImmediate` via `timeSliceQueue` | Pool thread consumer skips with `continue` |
| Global `timeSliceQueue` | Serializes yields across CGs on syncer | Not used in pool mode |
| SQLite connections per CG | 2 open on syncer process | 2 open on pool thread |
| CVR load/flush | On syncer (PG I/O) | Unchanged — on syncer |
| Poke delivery | On syncer (WebSocket) | Unchanged — on syncer |
| `#lock` scope | Entire `#runInLockWithCVR` callback | Same — IVM step is now an await |

### What did NOT change

- `#lock` still held for the whole advance (CVR load → IVM → CVR flush → pokeEnd). Same-CG client messages still wait.
- CVR flush still happens AFTER IVM, BEFORE pokeEnd (correctness: client must not commit before CVR is durable, or reconnect fails with `InvalidConnectionRequestBaseCookie`).
- Poke delivery still `Promise.allSettled` across clients.
- Replicator IPC unchanged.
- `PipelineDriver` class unchanged — same code runs, just inside a worker thread.

### Why this helps

Single-CG throughput is the same (IVM compute cost unchanged). **Cross-CG parallelism improves**: different pool threads run IVM on different cores simultaneously, and the syncer's event loop stays free during IVM so other CGs' I/O (WebSocket, PG) makes progress.

---

## 5. Multiple version-ready events — coalescing

Verified in `notifier.ts:37-40` and `subscription.ts:158-167,336`.

Each ViewSyncer subscribes to the Replicator's Notifier with a `coalesce: curr => curr` function. The Subscription keeps **at most one** pending message — if a new event arrives while one is pending, the new one replaces the old one (old one resolves to `'coalesced'`).

**Consequence**: when the replicator fires 10 version-ready events during a long advance, only ONE is waiting when the syncer finishes. The next advance jumps directly to the latest committed SQLite state. One Snapshotter advance processes changes across many commits (`_zero.changeLog2 WHERE stateVersion > prevVersion`).

The `ReplicaState` is just a wakeup signal — it doesn't carry a target version. Actual version comes from `SELECT stateVersion FROM _zero.replicationState` inside `Snapshotter` at advance time (assert: `view-syncer.ts:466`).

**In pool threads v3**: unchanged. Syncer still receives coalesced events, still sends `advance` to pool thread, which reads its own latest SQLite state.

---

## 6. Pool thread assignment — the mapper

### Why stickiness matters

Each CG's `PipelineDriver` holds in-memory IVM state: operator graphs, hydrated row caches, open SQLite connections, current snapshot version. This is expensive to rebuild (full re-hydration). If advance messages bounced across pool threads, every advance would rehydrate from scratch.

**Rule**: once CG is assigned to pool thread M, it MUST go to pool thread M for every subsequent op (advance, addQuery, removeQuery, getRow) until the ViewSyncer is destroyed.

### PoolThreadMapper (lives on the syncer)

```typescript
class PoolThreadMapper {
  #assignments = new Map<string, number>();     // clientGroupID → poolThreadIdx
  #loadPerThread: number[];

  constructor(private numPoolThreads: number) {
    this.#loadPerThread = new Array(numPoolThreads).fill(0);
  }

  // idempotent — existing CGs reuse their assignment
  assign(clientGroupID: string): number {
    const existing = this.#assignments.get(clientGroupID);
    if (existing !== undefined) return existing;
    
    // least-loaded pick
    let minIdx = 0;
    for (let i = 1; i < this.#loadPerThread.length; i++) {
      if (this.#loadPerThread[i] < this.#loadPerThread[minIdx]) minIdx = i;
    }
    this.#assignments.set(clientGroupID, minIdx);
    this.#loadPerThread[minIdx]++;
    return minIdx;
  }

  release(clientGroupID: string): number | undefined {
    const idx = this.#assignments.get(clientGroupID);
    if (idx === undefined) return undefined;
    this.#assignments.delete(clientGroupID);
    this.#loadPerThread[idx]--;
    return idx;
  }

  get(clientGroupID: string): number | undefined {
    return this.#assignments.get(clientGroupID);
  }
}
```

One mapper per syncer worker process, held by the `Syncer` class and injected into each ViewSyncer factory call.

### Lifecycle

- **ViewSyncer construction**: `mapper.assign(cgID)` picks a thread; `RemotePipelineDriver` is created pointing to it.
- **ViewSyncer destruction** (TTL or shutdown): `mapper.release(cgID)` and send `destroy` to the pool thread.
- **Reconnect within TTL**: reuse existing ViewSyncer → same pool thread → no rehydration.
- **Reconnect after TTL expiry**: ViewSyncer was destroyed, fresh assign → full rehydration.

### Pool thread crash recovery

1. Detect crash via worker thread `exit`/`error` event
2. **Restart the pool thread at the same index** — so `#loadPerThread[M]` stays valid
3. **Keep the mapper unchanged** — all CGs on M remain assigned to M (new, empty)
4. Mark those CGs as "needs re-init" — next operation triggers fresh `init` + `syncQueryPipelineSet`
5. Reject in-flight requests on the crashed thread with an error the syncer treats as `ResetPipelinesSignal`
6. Clients don't reconnect — WebSocket is on the syncer main thread; rehydration is transparent

**Why keep the mapping instead of reassigning**: reassigning N CGs to other threads would overload them. Re-hydration cost is the same regardless of thread index.

### No disk persistence

Pool thread state lives in memory on the worker thread. Writing the mapper to disk is useless — a syncer restart kills all its pool threads, so the saved assignments would point to empty threads anyway. On syncer restart, all CGs rehydrate when they next connect.

---

## 7. Communication protocol

```
SYNCER                               POOL THREAD
  RemotePipelineDriver            ↔  PipelineDriver per CG
  (proxy; all methods async)         (same stock class, running in worker_thread)

  Messages (syncer → pool):          Pool thread holds:
    init       (open Snapshotter,    Map<clientGroupID, PipelineDriver>
                create PipelineDriver)
    addQuery   (= hydration, reads SQLite)
    removeQuery
    advance    (streams results back)
    getRow
    destroy    (close Snapshotter)

  Messages (pool → syncer) for advance:
    advanceBegin {requestId, version, numChanges, snapshotMs}
    advanceChangeBatch {requestId, changes: RowChange[]} × N
    advanceComplete {requestId}
```

All methods on `RemotePipelineDriver` are `Promise`-returning. Syncer awaits each one.

### Streaming advance

Pool thread posts `advanceBegin` first (so syncer knows version/numChanges immediately), then zero or more `advanceChangeBatch` messages, then `advanceComplete`. Syncer consumes the stream via an async iterable inside `#processChanges` (`for await`).

### What the pool thread needs from `Timer`

`PipelineDriver` only calls `timer.elapsedLap()` and `timer.totalElapsed()` (pipeline-driver.ts:492, 665, 713, 717, 766). Never calls `yieldProcess()`/`start()`/`stop()`. Minimal Timer on pool thread:

```typescript
function createTimer(): Timer {
  const start = performance.now();
  const lapStart = start;
  return {
    elapsedLap: () => performance.now() - lapStart,
    totalElapsed: () => performance.now() - start,
  };
}
```

`elapsedLap()` grows forever on the pool thread (no reset). `#shouldYield()` always returns true after first `yieldThresholdMs`. That's fine — pool thread consumer skips `'yield'` markers with `continue`.

### Yield markers — skip on pool thread

**Decision**: pool thread does NOT yield on `'yield'` markers:

```typescript
for (const change of changes) {
  if (change === 'yield') continue;  // no yields on pool thread
  batch.push(change);
  if (batch.length >= BATCH_SIZE) {
    send({type: 'advanceChangeBatch', requestId, changes: batch});
    batch = [];
  }
}
```

**Rationale**: target state is 1 CG per pool thread, so no other CG to interleave with. Yields would add `setImmediate` overhead for no benefit. Sync handler is simpler (no re-entrancy, no global requestId landmine).

**SQLite is synchronous** (`@rocicorp/zero-sqlite3`, forked from `better-sqlite3`). When CG-A's SQLite query runs, the pool thread's entire V8 isolate is blocked — no other message handlers run until it returns. If 2+ CGs land on one pool thread, they're strictly sequential. Cross-pool-thread parallelism is real (separate V8 isolates, separate native bindings, different cores).

### Cross-CG ordering on the pool thread

**No explicit ordering.** Order is whatever FIFO arrives on the MessagePort, which is whatever V8 microtask scheduler happens to run each ViewSyncer's `#run` iteration in. Arbitrary from our code's perspective, but no starvation — every event eventually gets processed.

If you need priority ordering (small CGs first, hot CG first), add explicit logic. Not in v3 initially.

---

## 8. What the syncer does NOT wait for (and what it DOES)

### Pool thread moves to next CG immediately

After posting `advanceComplete` for CG-A, the pool thread's message handler returns and picks up the next queued message (could be CG-B). **Pool thread does not wait for syncer's CVR flush or pokeEnd.**

### Syncer runs CGs concurrently

Each ViewSyncer has its own `#lock`. CG-A and CG-B can be in different stages (CG-A in CVR flush, CG-B awaiting pool thread) at the same time, interleaved on the syncer's event loop via `await`s. CG-B does NOT wait for CG-A's `pokeEnd`.

### Within one CG, strict sequential

CG-A: `advance → CVR flush → pokeEnd → #lock release`. Any other operation on CG-A (e.g., `changeDesiredQueries`) waits on the `#lock` until all three complete.

### CVR flush and pokeEnd cannot be parallelized

Correctness requires pokeEnd after CVR flush succeeds. If the client commits to version X (via pokeEnd) but the CVR isn't persisted, a server crash + client reconnect results in `InvalidConnectionRequestBaseCookie` and a forced full resync. See `checkClientAndCVRVersions` at `view-syncer.ts:2237`.

---

## 9. Why v2 had "lot of delays" in production

Verified by reading v2 code (`feat/pool-worker-threads-v2` branch).

### 1. Pool thread handler was fully synchronous, no interleaving across CGs

V2 had `port.on('message', (msg) => {...sync handler...})`. The `advance` case ran the entire generator inline before returning. With ~25+ CGs per pool thread in production (4 pool threads × 100+ CGs), `advance` messages queued up FIFO. Each CG's full advance ran before the next one started. CG-B's tail latency = sum of all preceding CGs' advance times on that pool thread.

Stock 1.0.0 did not have this problem — `timeSliceQueue` interleaved CGs in 10ms slices. V2 dropped that on the pool thread.

**v3 fix**: target 1 CG per pool thread. Keep syncer count × pool thread count high enough that most pool threads hold 1-2 CGs. Don't rely on v1's fairness mechanism — avoid contention entirely.

### 2. Structured clone per batch on every postMessage

V2 kept V8 structured clone for MessagePort serialization. Measured at 0.03-0.06ms per typical batch. JSON string serialization measured 38% faster but was NOT adopted — CVR flush + poke dominated, structured clone wasn't the bottleneck.

**v3**: start with structured clone. Only switch if measurements show it matters.

### 3. `#lock` still held through CVR flush on the syncer

Same as stock. CVR flush (25-300ms) keeps the lock held. Client messages for the same CG wait. Not fixable without restructuring `#lock` semantics.

### 4. Global `currentRequestId` landmine

V2 had `let currentRequestId` at module scope. Fine for sync handler, but a landmine if anyone added `await` inside the handler — the global would get overwritten by the next message and the old handler's `send()` calls would use the wrong requestId.

**v3 fix**: every handler reads its requestId from the message and passes it through the call stack. No module-level state.

### 5. Missing `await` on `getRow()` in catchup poke

`must()` doesn't unwrap Promises. A Promise got passed to `contentsAndVersion` which tried to destructure `_0_version` and crashed. Fixed in v2 commit `0fe047078` with a one-word `await` addition.

**v3 fix**: `PipelineDriverInterface` declares every method as `Promise<...>`. Don't wrap pool thread returns in `must()` — use explicit `await` + `if (!x) throw`.

### 6. Destroy race

Pool thread teardown while a request was in flight, causing the syncer's await to hang or crash.

**v3 fix**: track in-flight requests with timeouts. On destroy, wait for in-flight requests or reject them cleanly.

### 7. No backpressure from pool thread

If pool thread streamed batches faster than syncer consumed them, the syncer-side buffer grew unbounded. Under load with large advances, this could consume significant memory.

**v3**: consider high-water mark on syncer-side buffer. Initially, rely on consumption speed being roughly matched to production speed.

### 8. Unbounded message queue during bursts

Sustained burst of version-ready events → many `advance` messages queue up on the pool thread port. Strict FIFO processing means later CGs wait. Monitor queue depth as a warning signal.

---

## 10. Mistakes from v2 to NOT repeat (summary)

1. **Missing `await getRow()`** → interface declares everything `Promise<...>`, explicit `await` + `if (!x) throw`
2. **Per-query PipelineDriver instead of per-CG** → one PipelineDriver per CG, keeps circuit breaker thresholds correct
3. **Reading targetVersion on syncer, sending to pool** → pool thread owns the version, reads its own `stateVersion`
4. **No requestId correlation** → every message has requestId, carried through closures not globals
5. **Missing metadata returns** (replicaVersion, currentVersion, etc.) → cache on syncer from `init`/`advance` responses or explicit messages
6. **Destroy race** → track in-flight requests
7. **Slow serialization** → stream in batches (already structured clone, revisit only if measured bottleneck)

---

## 11. Logging and diagnostics plan

**Goal**: every advance logs enough timing data to pinpoint regressions without a new deploy. v2's failure: wall time shot up with no sub-breakdown to identify the cause.

### Per-advance log (one line, at the end)

Extends the existing `finished processing advancement` log with pool-thread-specific fields:

| Field | Meaning | Why |
|---|---|---|
| `clientGroupID` | CG | Per-CG analysis |
| `poolThreadIdx` | Pool thread index | Detect hot/cold pool threads |
| `numChanges` | ChangeLog entries processed | Workload normalization |
| `numRows` | Total RowChange count streamed | Streaming cost proxy |
| `lockWaitMs` | Time waiting to acquire `#lock` | Same-CG contention |
| `ipcSendMs` | Time from `postMessage('advance')` to `advanceBegin` arrival | Pool thread queue backlog |
| `poolAdvanceMs` | Pool thread `driver.advance()` time (reported in `advanceBegin`) | Pure IVM compute cost |
| `snapshotMs` | `BEGIN CONCURRENT` + first read (from `advanceBegin`) | WAL lock acquisition |
| `poolStreamMs` | Pool thread iterating + posting batches (from `advanceComplete`) | Compute + iteration cost |
| `syncerStreamMs` | Syncer's `for await` loop time | Event loop contention |
| `cvrFlushMs` + breakdown (`rowCheckMs`, `txBeginMs`, `lockCheckMs`, `pipelineMs`, `postTxMs`) | PG CVR write | Already implemented in diagnostics branch |
| `pokeMs` | `pokers.end` time | Slow client / poke fanout |
| `wallMs` | Total wall time of `#advancePipelines` | Headline metric |

All collected during execution, logged once at the end. No logs in the hot loop.

### Pool thread logs

One log per advance (attached to `advanceComplete` or logged separately):

- `clientGroupID`, `requestId`, `poolThreadIdx`
- `snapshotMs`, `iterateMs`, `batchCount`, `totalRows`, `didReset`

Queue depth warning if pool thread's pending message count exceeds threshold.

### Syncer-level aggregates (once per minute)

- `advances`, `advanceWallP50/P95/P99`, `advancePoolP50/P95/P99`, `cvrFlushP50/P95/P99`
- `resetCount`
- `poolThreadAssignments`: CGs per pool thread (detect imbalance)
- `poolThreadQueueDepth` per thread
- `activeClientGroups` on this syncer

### Anomaly warnings (immediate, not batched)

- `wallMs > 1000` → "slow advance: ...breakdown..."
- `cvrFlushMs > 500` → "slow cvr flush: ..."
- `poolAdvanceMs > wallMs * 1.5` → "clock skew?"
- `ipcSendMs > 100` → "slow pool thread ipc: backlog=N"
- `lockWaitMs > 500` → "slow lock wait on CG X"
- Pool thread exit → "pool thread M crashed, restarting. affected CGs=N"

### Startup log

Log once at syncer start: pool thread count, yield threshold, sync worker count, pool thread PIDs.

### Grafana queries we must be able to answer from logs

1. **Which stage is slow?** — group wallMs by stage, bucket by time
2. **PG contention?** — cvrFlushMs time-series per pod, look for storms
3. **One pool thread overloaded?** — poolThreadIdx distribution per pod
4. **Hot CG?** — top CGs by advance count or wallMs
5. **Pool thread queue backing up?** — ipcSendMs + queue depth logs
6. **Syncer event loop saturated?** — syncerStreamMs (time in `for await` that isn't pool compute)

### Sanity check before deploy

Run local tests that synthetically reproduce failure modes and verify logs surface them:

1. Artificially slow one SQLite query → `poolAdvanceMs` spikes
2. Throttle CVR PG connection → `cvrFlushMs` spikes
3. Unbalance pool thread assignments → `poolThreadAssignments` shows imbalance
4. Block syncer event loop with busy loop → `lockWaitMs` and `syncerStreamMs` spike
5. Crash a pool thread → crash recovery log fires, affected CGs re-hydrate

If any failure mode produces confusing or missing logs, fix the logging before production.

---

## 12. Decisions (resolved)

| # | Question | Decision |
|---|---|---|
| D1 | Local `PipelineDriver` shape | **Keep stock `PipelineDriver` fully untouched.** Build `RemotePipelineDriver` as a parallel flow that is ONLY constructed and used when `ZERO_NUM_POOL_THREADS > 0`. Non-pool mode uses the stock `PipelineDriver` directly — no adapter, no interface wrapping, zero behavioral change when flag is off. Call sites branch once at ViewSyncer construction based on the flag. |
| D2 | Pool thread ↔ syncer transport | **Explicit `MessageChannel` per pool thread.** The syncer creates one `MessageChannel` per pool thread and transfers one end to the worker via the worker's `workerData`. The other end stays on the syncer. Isolates per-thread state, makes close semantics clean on crash. |
| D3 | Backpressure | **Not doing it in v3.** Consume as fast as pool thread produces. Revisit only if Section 11 logs show memory growth. |
| D4 | Pool thread crash retry cap | **Not doing it in v3.** Restart unconditionally at same index. Revisit if we see restart storms in production. |

### What "flag off = no change" means concretely

- `ViewSyncer` constructor receives either a stock `PipelineDriver` or a `RemotePipelineDriver`, typed as a union.
- All call sites that pass a change generator (`#processChanges`) must handle both sync and async iteration. Easiest: always use `for await` (works for sync generators too with a tiny helper, or branch at the call site).
- No edit to `pipeline-driver.ts` except possibly adding an `export` for types the remote needs. The stock class stays byte-for-byte the same on the sync path.
- No `PipelineDriverInterface` abstraction forced on the stock class. The "interface" exists only in the remote's type declaration.
- `Syncer` only spawns pool threads, creates `MessageChannel`s, and constructs `PoolThreadMapper` when flag is on. When off: zero pool-thread-related code runs.

---

## 13. Implementation plan (file-by-file)

Guiding rule from D1: **stock `PipelineDriver` and the non-pool code path must remain untouched.** All pool code is additive and gated behind `ZERO_NUM_POOL_THREADS > 0`.

### 13.1 New files

#### `packages/zero-cache/src/services/view-syncer/pool-thread/pool-thread-entry.ts`
Worker thread entry point. Spawned by `Syncer` on one end of a `MessageChannel`.

Responsibilities:
- Receive its `MessagePort` via `workerData` (`{port, poolThreadIdx, replicaFile, replicaVersion, lc}`).
- Maintain `Map<clientGroupID, PipelineDriver>` (re-uses the **stock, untouched** `PipelineDriver` class).
- Maintain `Map<clientGroupID, Snapshotter>` — or hold it inside the PipelineDriver; whichever matches stock construction today.
- Message loop (`port.on('message', handle)`):
  - `init {cgID, ...}` → construct `PipelineDriver` + `Snapshotter`, reply `initDone`.
  - `addQuery {cgID, queryID, ast, options}` → call `driver.addQuery(...)`, stream `addQueryBatch`, then `addQueryComplete`.
  - `removeQuery {cgID, queryID}` → `driver.removeQuery(...)`, reply `removeQueryDone`.
  - `advance {cgID, requestId}` → construct local `Timer`, call `driver.advance(timer)`, post `advanceBegin` with `{version, numChanges, snapshotMs}`, iterate generator with `if (change === 'yield') continue`, batch changes into `advanceChangeBatch`, finally post `advanceComplete {iterateMs, batchCount, totalRows}`.
  - `getRow {cgID, table, row, requestId}` → reply `getRowResult {requestId, row}`.
  - `reset {cgID, mode}` → call `driver.reset(...)`, reply `resetDone`.
  - `destroy {cgID}` → close Snapshotter, drop from map, reply `destroyDone`.
- Per-message requestId passed through closures. **No module-level state.**
- Errors → post `{type: 'error', requestId, message, stack}`; syncer rejects the waiting promise.

#### `packages/zero-cache/src/services/view-syncer/pool-thread/pool-thread-protocol.ts`
Shared type definitions for messages in both directions. One file so syncer and worker agree.

```typescript
export type SyncerToPool =
  | {type: 'init'; cgID; requestId; ...}
  | {type: 'addQuery'; cgID; requestId; queryID; ast; options}
  | {type: 'removeQuery'; cgID; requestId; queryID}
  | {type: 'advance'; cgID; requestId}
  | {type: 'getRow'; cgID; requestId; table; row}
  | {type: 'reset'; cgID; requestId; mode}
  | {type: 'destroy'; cgID; requestId};

export type PoolToSyncer =
  | {type: 'initDone'; requestId}
  | {type: 'addQueryBatch'; requestId; rows}
  | {type: 'addQueryComplete'; requestId}
  | {type: 'advanceBegin'; requestId; version; numChanges; snapshotMs}
  | {type: 'advanceChangeBatch'; requestId; changes}
  | {type: 'advanceComplete'; requestId; iterateMs; totalRows; didReset}
  | {type: 'getRowResult'; requestId; row}
  | {type: 'resetDone'; requestId}
  | {type: 'destroyDone'; requestId}
  | {type: 'error'; requestId; message; stack};
```

#### `packages/zero-cache/src/services/view-syncer/pool-thread/remote-pipeline-driver.ts`
Syncer-side proxy. ONE instance per ViewSyncer (per CG). All methods return Promises. Holds the `MessagePort` to its assigned pool thread (provided by `PoolThreadMapper`).

Shape:
```typescript
class RemotePipelineDriver {
  constructor(
    port: MessagePort,
    clientGroupID: string,
    lc: LogContext,
  ) {...}

  // mirrors stock PipelineDriver public surface, but all async
  async init(...): Promise<void>;
  async addQuery(...): Promise<AsyncIterable<Row>>;     // streams
  async removeQuery(...): Promise<void>;
  async advance(timer: Timer): Promise<{
    version: string;
    numChanges: number;
    snapshotMs: number;
    changes: AsyncIterable<RowChange>;                  // streams
  }>;
  async getRow(table, row): Promise<Row | undefined>;
  async reset(...): Promise<void>;
  async destroy(): Promise<void>;
}
```

Request correlation: each method allocates a requestId, registers a resolver in a `Map<number, Handler>`, posts the message, awaits the resolver. Streaming methods create an async queue that the caller iterates via `for await`; `complete` closes the queue.

**Important**: syncer's `Timer` is NOT sent across. The pool thread constructs its own per-advance `Timer`. `RemotePipelineDriver.advance()` ignores the passed `timer` argument for pool-thread-internal purposes — it only uses it for the syncer-side `lockWaitMs` / `wallMs` logging.

#### `packages/zero-cache/src/services/view-syncer/pool-thread/pool-thread-mapper.ts`
Exact code from Section 6. Held by `Syncer`, shared across all ViewSyncers on that syncer worker.

Additionally: `getPort(idx: number): MessagePort` — the mapper also owns the `MessagePort` → pool thread index mapping so that `assign()` can return both the index and the port.

#### `packages/zero-cache/src/services/view-syncer/pool-thread/pool-thread-manager.ts`
Owns the lifecycle of the N pool threads. One per `Syncer`.

Responsibilities:
- On construction: create N `MessageChannel`s, spawn N workers passing one port each via `workerData.port` (use `transferList: [port]`).
- Track each worker's `Worker` instance and syncer-side `MessagePort`.
- On worker `exit`/`error`: restart at same index, hand the new worker a fresh `MessageChannel`, update the port on the mapper. Log the event. Fire `onPoolThreadCrash(idx)` so the caller can mark affected CGs as "needs re-init".
- On `Syncer` shutdown: post `destroy` for all CGs, terminate workers.

### 13.2 Modified files

#### `packages/zero-cache/src/services/view-syncer/view-syncer.ts`
Minimal surgical changes:

1. Constructor takes an extra optional `remotePipelineDriverFactory?: (cgID) => RemotePipelineDriver`. When provided, construct `RemotePipelineDriver` instead of stock `PipelineDriver`.
2. Field `#pipelines` type becomes `PipelineDriver | RemotePipelineDriver` (union, not a common interface). Use `instanceof` checks only at the very few call sites that differ, or wrap in a thin local helper.
3. `#processChanges` — change the inner `for (const change of changes)` to `for await (const change of changes)`. `for await` works on sync iterables too, so the stock path still works.
4. Every call to `this.#pipelines.xxx(...)` gets `await` added. Stock `PipelineDriver` methods return non-Promises which `await` unwraps transparently → no behavior change on stock path.
5. `#advancePipelines` logging extended with the fields from Section 11 (`poolThreadIdx`, `ipcSendMs`, `poolAdvanceMs`, `poolStreamMs`, `syncerStreamMs`). Missing-await bug protection: no `must()` around anything that crosses the pool boundary.

**Non-goals** for this file:
- Do NOT refactor `#lock` semantics.
- Do NOT restructure CVR flush timing.
- Do NOT touch client-message handlers.

#### `packages/zero-cache/src/server/syncer.ts` (or wherever ViewSyncer is constructed)
1. If `ZERO_NUM_POOL_THREADS > 0`:
   - Construct one `PoolThreadManager` with `numPoolThreads = ZERO_NUM_POOL_THREADS`.
   - Construct one `PoolThreadMapper`.
   - Provide a `remotePipelineDriverFactory` to the ViewSyncer constructor that:
     - `idx = mapper.assign(cgID)`
     - `port = manager.getPort(idx)`
     - returns `new RemotePipelineDriver(port, cgID, lc)`
2. If `ZERO_NUM_POOL_THREADS === 0` (or unset): do NOT import or touch any pool-thread code. Construct stock `PipelineDriver` as today.

#### `packages/zero-cache/src/config/zero-config.ts`
Add `numPoolThreads?: number` config, env `ZERO_NUM_POOL_THREADS`, default `0`.

### 13.3 Logging (per Section 11)

- `view-syncer.ts #advancePipelines`: add `ipcSendMs`, `poolAdvanceMs`, `poolStreamMs`, `syncerStreamMs`, `poolThreadIdx` to the existing log line (only when `remote` path).
- `pool-thread-entry.ts`: per-advance log with `{cgID, requestId, poolThreadIdx, snapshotMs, iterateMs, batchCount, totalRows, didReset}`.
- `pool-thread-manager.ts`: log on spawn and on crash/restart.
- `syncer.ts`: startup log with `numPoolThreads`, `numSyncWorkers`, `ZERO_NUM_POOL_THREADS`.
- Aggregates (Section 11) can be deferred to a follow-up; per-advance lines are the priority.

### 13.4 Tests

Not adding new tests initially. Run existing `view-syncer` test suite in both modes:
- `ZERO_NUM_POOL_THREADS=0` (default): should be byte-for-byte equivalent to today.
- `ZERO_NUM_POOL_THREADS=4`: should pass the same tests.

Section 11 sanity checks (artificially slow query, throttle PG, unbalanced assignments, crash a pool thread) run manually in `xyne-spaces-login` before canary.

### 13.5 Order of work

1. Protocol file (types only)
2. `PoolThreadMapper` + unit test for assignment logic
3. `pool-thread-entry.ts` with `init` / `destroy` / `addQuery` / `removeQuery` / `getRow` / `reset` (the non-streaming ones)
4. `RemotePipelineDriver` — same subset
5. Wire through `Syncer` + `ViewSyncer` — run existing tests, expect pass
6. `advance` streaming end-to-end
7. Logging
8. Crash recovery in `PoolThreadManager`
9. Local integration test in zbugs with flag on
10. xyne-spaces-login deploy, Section 11 sanity checks
11. Production canary on one pod

---

## 14. Next steps

D1-D4 resolved. Implementation plan above is ready for review. Start with step 1 (protocol file) once you confirm the plan.
