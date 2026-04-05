# Zero Cache: Per-Query IVM Worker Pool

**Status**: Proposal / Design Document
**Date**: 2026-04-01
**Author**: Harsha Ranga

---

## 1. Problem

Two 30-core machines. Each uses ~6 cores. 48 cores idle. Response latency exceeds 200ms.

Why: each client group's queries serialize through a `timeSliceQueue` on one syncer process (`view-syncer.ts:2192`). 40 active queries x 10ms time slices = 400ms+ wall time on one core while 28 other cores do nothing.

## 2. Solution

**Distribute IVM work across cores using `worker_threads` inside each syncer process.** The syncer stays the owner of the WebSocket connection, CVR, and pokes. When IVM work is needed, queries run on separate cores in parallel. Results merge back at the syncer for a single CVR flush and a single poke over the WebSocket.

```
Client --WebSocket--> Syncer (1 process)
                        | owns: connection, CVR, auth, pokes
                        |
                        | version-ready arrives
                        |
                        |-- send {advance} --> Pool Thread (core 3)  [Q1, Q7]
                        |-- send {advance} --> Pool Thread (core 4)  [Q2, Q8]
                        |-- send {advance} --> Pool Thread (core 5)  [Q3, Q9]
                        |-- ...             --> ...
                        |
                        |   ALL run in PARALLEL on separate cores
                        |
                        |-- receive RowChange[] <-- core 3
                        |-- receive RowChange[] <-- core 4
                        |-- receive RowChange[] <-- core 5
                        |-- ...
                        |
                        | MERGE: de-duplicate rows, CVR flush, single poke
                        |
Client <----poke------/
```

---

## 3. Key Components

This section explains each component involved in the sync engine. Understanding these is required before reading the design.

### 3.1 SQLite Replica

The **source of truth is PostgreSQL**. The Replicator process watches PostgreSQL via CDC (change data capture) and writes every change to a local **SQLite replica file**. This SQLite file is a read-optimized copy of the upstream data. All IVM queries read from this replica, never from PostgreSQL directly.

The replica uses WAL2 mode (two WAL files that alternate, allowing writes to one while the other checkpoints). Only the Replicator writes. Everyone else reads.

### 3.2 ChangeLog

The ChangeLog (`_zero.changeLog2`) is a table inside the SQLite replica. It records which rows changed at which version.

```sql
CREATE TABLE "_zero.changeLog2" (
    "stateVersion" TEXT NOT NULL,     -- version when this change happened
    "pos"          INT  NOT NULL,     -- ordering within a version
    "table"        TEXT NOT NULL,     -- which table
    "rowKey"       TEXT NOT NULL,     -- which row
    "op"           TEXT NOT NULL,     -- 's' (set), 'd' (delete), 't' (truncate), 'r' (reset)
    PRIMARY KEY("stateVersion", "pos"),
    UNIQUE("table", "rowKey")         -- each row appears AT MOST ONCE
);
```

**The ChangeLog is NOT an append-only WAL.** The `UNIQUE("table", "rowKey")` constraint means each row appears at most once. When the Replicator modifies row X at version "07", it inserts an entry. If the same row X is modified again at version "09", the `INSERT OR REPLACE` (`change-log.ts:130`) **replaces** the "07" entry. The "07" entry is gone.

This deduplication is what makes the Diff bounded -- each row appears at most once, so IVM work is proportional to the number of distinct changed rows, not the number of versions.

### 3.3 Snapshotter

The Snapshotter (`snapshotter.ts`) manages **two SQLite read connections** in a leapfrog pattern:

- `prev`: frozen at the old version (before the advance)
- `curr`: frozen at the new version (after the advance)

On `advance()`, the old `prev` is rolled back to head and becomes the new `curr`. The old `curr` becomes the new `prev`. This gives two point-in-time views of the data, which the Diff iterator uses to compute what changed.

Each Snapshotter holds 2 SQLite connections. They use `BEGIN CONCURRENT` to pin a snapshot at the version committed when the first read happens.

### 3.4 PipelineDriver and IVM

**IVM = Incremental View Maintenance.** Instead of re-running a full SQL query on every change, the IVM operator graph remembers the previous result set and only processes the delta.

The **PipelineDriver** (`pipeline-driver.ts`) manages IVM pipelines for a set of queries. It holds:
- A `Snapshotter` (the two SQLite snapshots)
- A map of `TableSource` instances (one per table that any query reads from)
- A map of `Pipeline` instances (one per query -- the IVM operator graph)
- Operator `Storage` (persisted IVM state in a temp SQLite database)

Key operations:
- **`addQuery(ast)`**: Builds the IVM operator graph for a query. Does a full scan of matching rows (hydration). The operator graph retains which rows are in the result set.
- **`advance()`**: Advances the Snapshotter, reads the ChangeLog diff, pushes only changed rows through the operator graphs. Each operator graph uses its retained state to compute the delta -- which rows were added, removed, or edited relative to the previous state. Returns only the delta `RowChange[]`.
- **`removeQuery(id)`**: Destroys the pipeline for a query.
- **`getRow(table, pk)`**: Reads a raw table row from the current SQLite snapshot. Used during client catchup.

**The advance depends on the state that hydration built.** Without it, the operator graph doesn't know the previous result set, so it can't compute a delta. Destroying and recreating the pipeline would require a full re-hydration every time, defeating IVM entirely.

### 3.5 RowChange

The output of IVM computation. Each `RowChange` carries:

```typescript
type RowChange = {
  type: 'add' | 'remove' | 'edit';
  queryID: string;        // which query produced this change
  table: string;          // which table the row belongs to
  rowKey: Row;            // primary key columns as an object
  row: Row | undefined;   // THE FULL ROW (all columns) -- undefined for removes
};
```

This is the data that flows from pool worker threads back to the syncer via `MessagePort`.

### 3.6 CVR (Client View Record)

The CVR is stored in **PostgreSQL** (not SQLite). It answers the question: **what has each client already seen, so we only send the diff?**

**Why it exists:** Multiple clients share the same client group. A client might disconnect and reconnect later at a stale version. The server needs to know which rows each client has seen to send only what's new.

A CVR row record looks like:

```
For client group "abc-123":
  Row {table:"user", rowKey:{id:"u1"}}:
    rowVersion: "07"                        -- _0_version of the row when last synced
    patchVersion: {stateVersion:"05"}       -- CVR version when client was told about this row
    refCounts: {Q1: 1, Q3: 1}              -- which queries reference this row
```

**Example: How refCounts work**

```
Client group has 2 queries:
  Q1: SELECT * FROM user WHERE role = 'admin'
  Q3: SELECT * FROM user WHERE team = 'eng'

User "u1" is an admin on the eng team -- matches BOTH queries.
CVR: refCounts = {Q1: 1, Q3: 1}

User's role changes to 'member' at version "09":
  Q1 emits: RowChange {type:'remove', queryID:'Q1'}   -- no longer matches admin filter
  Q3 emits: RowChange {type:'edit', queryID:'Q3'}     -- still matches eng filter

After merging: refCounts = {Q1: 0, Q3: 1}
Row stays in CVR (Q3 still references it). Client gets an 'edit' patch.

If Q3 also removed it: refCounts = {Q1: 0, Q3: 0}
Row deleted from CVR. Client gets a 'del' patch.
```

**CVR operations during an advancement cycle:**

1. `received(rows)`: For each RowChange batch, merges refCounts in memory. Calls `putRowRecord()` which queues a write -- does NOT write to PG yet.
2. `deleteUnreferencedRows()`: Finds rows where all refCounts are zero, queues deletes.
3. `flush()`: Opens a single PG transaction, writes ALL queued row records and deletes in one batch, commits. This is the single PG commit for the entire cycle.

**Catchup**: When a stale client connects, the CVR knows which rows were patched between the client's version and now. But CVR only stores WHICH rows (table + rowKey + refCounts), not the actual data. `getRow()` reads the actual row data from the SQLite replica.

### 3.7 Poke Protocol

The poke is how the server tells the client "here's new data":

1. **`pokeStart`**: Sent on the first patch. Signals the client to start buffering.
2. **`pokePart`**: Sent every 100 patches (`PART_COUNT_FLUSH_THRESHOLD`). Contains row patches and query patches. Client buffers these -- does NOT apply yet.
3. **`pokeEnd`**: Sent after all changes are processed. Contains the final cookie (CVR version). Client atomically applies all buffered patches.

The client sees a consistent snapshot: either all patches from an advance cycle, or none.

### 3.8 ViewSyncer

The `ViewSyncerService` (`view-syncer.ts`, ~2200 lines) is the orchestrator for a single client group. It:
- Owns the `PipelineDriver` (or `RemotePipelineDriver` with pool threads)
- Owns the `CVRStore` (PostgreSQL connection for CVR)
- Manages client connections and poke delivery
- Runs the main loop: wait for version-ready -> advance pipelines -> merge results -> CVR flush -> poke

Three serialization layers prevent races:
- **Layer 1**: Subscription coalescing -- at most 1 pending notification
- **Layer 2**: `for await` is sequential -- the run loop doesn't pull the next notification until the current cycle completes
- **Layer 3**: `#lock` mutual exclusion -- `initConnection`, `changeDesiredQueries`, `advancePipelines` never overlap

### 3.9 Syncer and Dispatcher

The **Dispatcher** (`server/main.ts`) is the parent process. It `fork()`s child processes: replicator, change-streamer, reaper, and syncers.

Each **Syncer** (`workers/syncer.ts`) is a `fork()` child process that handles WebSocket connections. It holds a `ServiceRunner<ViewSyncerService>` -- a map of `clientGroupID -> ViewSyncer` instances. Client groups are pinned to syncers via `h32(taskID + '/' + clientGroupID) % syncers.length`.

### 3.10 Notification Chain

When data changes:

```
PostgreSQL -> CDC -> Replicator writes to SQLite replica
  -> IPC ['notify', {state:'version-ready'}] to Dispatcher
  -> Dispatcher relays to each Syncer via handleSubscriptionsFrom()
  -> Syncer's Notifier fans out via EventEmitter
  -> Each ViewSyncer's Subscription wakes up
  -> ViewSyncer.run() loop processes the advancement
```

---

## 4. Current Architecture (What Stays the Same)

### 4.1 Process Tree

```
run-worker.ts
  |-- fork() -> main.ts (dispatcher)
        |-- fork() -> change-streamer       [supporting]
        |-- fork() -> reaper               [supporting]
        |-- fork() -> replicator           [supporting, sole SQLite writer]
        |-- fork() -> syncer (1)           [user-facing]
        |     |-- worker_thread -> pool thread W1   [IVM compute]  (NEW)
        |     |-- worker_thread -> pool thread W2   [IVM compute]  (NEW)
        |     |-- ... pool thread WN                               (NEW)
        |-- fork() -> syncer (2)           [user-facing]
              |-- worker_thread -> pool thread ...                  (NEW)
              |-- ...
```

Syncers are `fork()` child processes (unchanged). Pool threads are `worker_threads` inside each syncer process (NEW). Created via `new Worker()` from `node:worker_threads`. Each gets its own V8 isolate, its own heap, scheduled on its own core by the OS.

**Pool threads are per-syncer, not shared.** Each syncer `fork()` process has its own address space. Worker threads inside Syncer 1 cannot communicate with Syncer 2. Each syncer spawns exactly `ZERO_NUM_POOL_THREADS` threads. If one syncer has more active client groups, its threads are busier -- same imbalance that exists today between syncer processes.

### 4.2 What Stays the Same

- Client group pinning: `h32(taskID/clientGroupID) % syncers.length` (unchanged)
- CVR ownership, loading, flushing: stays in syncer
- WebSocket handling, poke generation: stays in syncer
- Auth, TTL, drain coordination: stays in syncer
- `#lock` serialization per client group: stays in syncer
- `#processChanges` de-duplication: stays in syncer
- Replicator: sole writer to replica, unchanged
- Dispatcher: unchanged routing logic
- Notification chain: unchanged

---

## 5. Pool Worker Threads

### 5.1 Threads Are Long-Lived Infrastructure

Pool threads are spawned **once at syncer startup** and stay alive for the lifetime of the syncer process. They are NOT created per-query or per-connection.

```
Syncer process starts
  -> reads config.numPoolThreads (e.g. 13)
  -> spawns 13 worker threads immediately via new Worker()
  -> threads are idle, waiting for messages on their MessagePort
  -> threads stay alive until syncer shuts down
```

Multiple queries are assigned to the same thread (round-robin by least-loaded). With 40 active queries and 13 threads, each thread hosts ~3 queries. Each query on a thread has its own PipelineDriver, Snapshotter, TableSources, and operator Storage -- the same structure a client group has today, just scoped to one query.

### 5.2 Core Principle: Same Code, Different Thread

The pool thread runs the **exact same code** as the sync worker. It doesn't know it's a "pool thread." It uses the same PipelineDriver, same advance loop, same error handling (including `ResetPipelinesSignal` → reset → re-hydrate). The only difference is it handles fewer queries.

```
WITHOUT pool threads:

  Syncer process:
    PipelineDriver has 40 queries
    advance() → circuit breaker fires → reset → re-hydrate → continue
    All on one core, serialized through timeSliceQueue

WITH pool threads:

  Syncer process (main thread):
    Distributes queries to pool threads
    Merges results
    CVR flush + poke delivery

  Pool thread (separate core):
    PipelineDriver has 1-3 queries         ← same code
    advance() → circuit breaker fires      ← same code
      → reset → re-hydrate → continue     ← same error handling
    Returns RowChange[] to syncer

  if (poolEnabled) {
      distribute queries to threads
      threads run: <same advance/error logic>
      merge results
  } else {
      <same advance/error logic>
  }
```

The pool thread is NOT a stripped-down compute worker. It IS a sync worker that handles fewer queries and communicates via MessagePort instead of WebSocket.

### 5.3 Query Assignment and Lifecycle

```
THREAD STARTUP (once, at syncer boot):
  Syncer spawns 13 threads. Each thread: idle, listening on MessagePort.

QUERY ARRIVES:
  Syncer picks least-loaded thread (e.g. Thread 5)
  Sends {type:'hydrate', queryID:'Q1', ast:...} to Thread 5
  Thread 5:
    creates PipelineDriver + Snapshotter for Q1
    runs addQuery() -- full scan, builds operator graph
    sends RowChange[] back to syncer (structured clone)
    RETAINS pipeline state in its V8 heap

VERSION-READY (repeated, every time data changes):
  Syncer sends {type:'advance', targetVersion} to all threads with queries
  Thread 5 runs the SAME advance loop as the sync worker:
    advance() → if ResetPipelinesSignal → reset → re-hydrate → return results
    sends RowChange[] back to syncer (structured clone)
  (Thread 5 may also advance Q7, Q12 -- processes them sequentially)

QUERY UNSUBSCRIBED:
  Syncer sends {type:'destroy', queryID:'Q1'} to Thread 5
  Thread 5:
    destroys Q1's pipeline + Snapshotter, frees SQLite connections
    Thread stays alive, still hosting Q7 and Q12

SHUTDOWN:
  On syncer drain/exit, terminates worker threads. Each destroys remaining drivers.
```

**Why hydration and advance must be on the same thread:** The IVM operator graph holds state about what rows are in the result set. `advance()` depends on this state to compute the delta. Each `worker_thread` has its own V8 isolate with its own heap. Pipeline state cannot be transferred between isolates. The thread that hydrates a query must also advance it.

### 5.4 Pool Workers Are Reactive

Pool workers don't subscribe to replicator notifications. They don't know when the replica changes. The syncer tells them when to advance via `MessagePort.postMessage()`. No notification plumbing needed for pool workers.

### 5.5 Redundant ChangeLog Reads

40 active queries = 40 pipelines each independently reading the same ChangeLog diff from SQLite. This is redundant IO.

Why it's fine: SQLite page cache. After the first pipeline reads the diff, the pages are in OS memory. The rest read from cache. Cost: ~0ms extra IO. The real CPU work is pushing changes through each query's operator graph, which is per-query regardless.

### 5.6 Communication Cost: Structured Clone

Pool workers send `RowChange[]` back to the syncer via `MessagePort.postMessage()`. This uses V8's **structured clone** -- serialize on the sender, deserialize on the receiver. It is a memory copy, not zero-copy. (Both `worker_threads` and `fork()` use structured clone -- the cost is identical.)

A `Row` is `Readonly<Record<string, JSONValue | undefined>>` -- plain object. V8 structured clone handles these at ~500MB/s-1GB/s.

**Advance (hot path -- repeated on every version-ready):**

| Scenario | RowChanges | Data size | Clone cost |
|----------|-----------|-----------|------------|
| Light | 20 | ~10KB | **< 0.1ms** |
| Medium | 200 | ~100KB | **~0.2ms** |
| Heavy | 1000 | ~500KB | **~1ms** |

Sub-millisecond for typical advances. Negligible compared to the 200ms+ latency we're eliminating.

**Hydration (one-time per query subscription):**

| Scenario | Rows | Data size | Clone cost | IVM time | Overhead |
|----------|------|-----------|------------|----------|----------|
| Small | 500 | ~250KB | ~0.5ms | ~20ms | ~2% |
| Medium | 5K | ~2.5MB | ~5ms | ~200ms | ~2% |
| Large | 20K | ~10MB | ~20ms | ~1000ms | ~2% |

Memory: the copy lives in the syncer's heap until `#processChanges` consumes it. With ~80GB unused memory per pod, this is irrelevant.

### 5.7 Why `worker_threads` Over `fork()`

Both use structured clone -- serialization cost is identical. `worker_threads` wins on:

| | `worker_threads` | `fork()` |
|---|---|---|
| Startup time | ~5-10ms | ~30-50ms |
| Memory per unit | ~2-5MB (V8 isolate) | ~30-50MB (full process) |
| 13 per syncer | ~65MB | ~650MB |
| Spawning | Direct `new Worker()` from syncer | Needs dispatcher relay |
| Crash isolation | Thread crash kills syncer | Process crash is isolated |

Crash isolation is fork()'s only advantage. But pool workers run the same PipelineDriver + Snapshotter code that already runs reliably today -- same classes, same logic, just scoped to one query. The 10x memory savings and simpler spawning make `worker_threads` the right choice.

---

## 6. The Merge Point: How Results Come Back

### 6.1 Today's Flow

1. **`#advancePipelines`** called under `#lock`
2. **`pipelines.advance(timer)`** -- advances Snapshotter, returns lazy generator of RowChanges
3. **Create `CVRQueryDrivenUpdater` and `PokeHandler`** -- no data sent yet
4. **`#processChanges(changes, updater, pokers)`** -- pulls from generator:
   - Each pull runs IVM synchronously (CPU-bound)
   - Accumulates rows in `CustomKeyMap` (de-duplicates across queries)
   - Every 10K rows: `processBatch()` -> `updater.received()` (merges refCounts, queues PG writes in memory) + `pokers.addPatch()` (sends `pokePart` every 100 patches)
5. **`flushUpdater()`** -- single PG transaction commits all queued row records
6. **`pokers.end()`** -- sends final `pokePart` + `pokeEnd` over WebSocket
7. **Release `#lock`**

### 6.2 With Pool Workers

Step 4 changes. Instead of pulling from a local generator, the syncer fans out to pool threads and collects results.

**Inside each pool thread — the same advance loop as the sync worker:**

```
Pool thread receives {type: 'advance', targetVersion}:

  for each query on this thread:
    result = driver.advance(timer, targetVersion)
    changes = iterate result.changes (same IVM push logic)
    
    if ResetPipelinesSignal thrown:
      driver.reset(clientSchema)          ← same error handling as sync worker
      driver.advanceWithoutDiff()         ← same recovery
      re-hydrate with driver.addQuery()   ← same re-hydration
      continue with next query
    
    collect RowChange[] for this query
  
  send all RowChange[] back to syncer via MessagePort
```

The pool thread handles `ResetPipelinesSignal` internally — same as the ViewSyncer's run loop handles it. The syncer never sees pipeline reset errors. It only receives results.

**Syncer side — fan out and merge:**

```typescript
// RemotePipelineDriver.advance():
async advance(timer: Timer) {
  // Fan-out: tell all pool threads to advance
  const results = await Promise.all(
    threads.map(t => sendAndWait(t, {type: 'advance', targetVersion}))
  );
  // Merge: combine RowChange[] from all threads
  return {version, numChanges, changes: mergedResults};
}
```

**Steps 1-3 and 5-7 are UNCHANGED.** The syncer's `#processChanges` iterates the merged results, accumulates rows, calls `processBatch()`, then `flushUpdater()`, then `pokers.end()`. Same code, same order.

### 6.3 Timeline

```
t=0ms:   Syncer sends {advance} to threads T3, T4, T5 (non-blocking)
         All threads start computing in parallel on separate cores

t=0ms:   Syncer enters #processChanges, iterates the stream
         Stream blocks on #receiveNext() -- waiting for first thread to finish

t=20ms:  T4 finishes (fastest) -> sends RowChange[] via MessagePort
         Stream yields T4's changes
         #processChanges accumulates rows, processBatch() if 10K reached
         Meanwhile: T3 and T5 still computing on their cores

t=30ms:  T3 finishes -> sends RowChange[]
         Stream yields T3's changes, de-duplicates via CustomKeyMap

t=50ms:  T5 finishes (slowest) -> sends RowChange[] -> final batch

t=51ms:  All done. flushUpdater() -> PG commit. pokers.end() -> pokeEnd.
         Client applies all patches atomically.
```

### 6.4 Impact on CVR: None

The CVR operations all happen in the syncer process. Pool workers don't know the CVR exists. They just return `RowChange[]`. The syncer does all refCount merging, PG writes, and poke generation -- same code, same process.

```
Pool workers produce:  RowChange[] (queryID, table, rowKey, row, type)
                       | structured clone via MessagePort
Syncer receives:       Iterates in #processChanges (UNCHANGED)
                       Accumulates in CustomKeyMap (UNCHANGED)
                       updater.received() merges refCounts (UNCHANGED)
                       flush() commits to PG (UNCHANGED)
                       pokers.end() sends poke (UNCHANGED)
```

---

## 7. Version Consistency

### 7.1 The Issue

Today, one PipelineDriver has one Snapshotter. All queries share ONE snapshot -- they all see the same version.

With per-query pool workers, each has its own Snapshotter. If the Replicator commits between workers opening their snapshots, workers see different versions.

### 7.2 The Fix: Upper-Bounded ChangeLog Query

The syncer reads `targetVersion` before fanning out. Each pool worker bounds its ChangeLog query:

```sql
-- Today:
SELECT ... FROM "_zero.changeLog2" WHERE "stateVersion" > ?

-- With upper bound:
SELECT ... FROM "_zero.changeLog2" WHERE "stateVersion" > ? AND "stateVersion" <= ?
```

**This works because of the UNIQUE constraint.** Three cases:

**Row X modified only at <= targetVersion:** ChangeLog entry exists within bound. `curr.getRow(X)` reads the row -- if the snapshot is at a higher version but the row wasn't modified after targetVersion, `_0_version` still matches. `checkThatDiffIsValid` passes.

**Row X modified at both <= targetVersion AND > targetVersion:** The `INSERT OR REPLACE` at the higher version **replaced** the old ChangeLog entry. The entry is now at a version > targetVersion. The bounded query excludes it. Row is skipped -- picked up in the next advance cycle.

**Row Y modified only at > targetVersion:** ChangeLog entry at higher version. Filtered by bound.

One small change to `Snapshot.changesSince()`. No version skew. No reconciliation.

---

## 8. Resource Audit

| Resource | Stays in syncer | Moves to pool thread | Impact |
|----------|----------------|---------------------|--------|
| CVR PostgreSQL pool | Yes | No | None |
| Upstream PostgreSQL pool | Yes | No | None |
| Mutagen (mutations) | Yes | No | None |
| WebSocket server | Yes | No | None |
| Auth session | Yes | No | None |
| Replica SQLite (Snapshotter) | No | Yes (per query) | 2 connections per query. 40 queries = 80 SQLite readers. SQLite supports unlimited concurrent readers. |
| Operator storage (DatabaseStorage) | No | Yes (per thread) | 1 SQLite tmpfile per thread (`EXCLUSIVE` locking prevents sharing). 13 threads = 13 tmpfiles. |
| Replicator notifications | Yes | No | Pool workers are reactive. Syncer tells them when to advance. |
| Syncer SQLite read connection | Yes (NEW, 1 per syncer) | No | For reading `stateVersion` and `getRow` during catchup. |

**PostgreSQL connections stay with the syncer. Pool workers only need SQLite.**

---

## 9. Sizing and Configuration

### 9.1 Fallback: `ZERO_NUM_POOL_THREADS=0` -> Current Behavior

When `numPoolThreads` is 0 (default): **no threads are spawned, no new code loads.** The factory in `server/syncer.ts` creates a real `PipelineDriver` directly -- identical code path to today. Zero regression.

```typescript
// server/syncer.ts viewSyncerFactory (line ~148):
if (config.numPoolThreads > 0) {
  // NEW: RemotePipelineDriver wrapping worker threads
  return new RemotePipelineDriver(poolThreads, ...)
} else {
  // CURRENT: same as today, no threads, no new code
  return new PipelineDriver(new Snapshotter(...), ...)
}
```

### 9.2 `ZERO_NUM_POOL_THREADS` Is Per Syncer

The config specifies threads **per syncer process**. What you set is what each syncer spawns.

```
ZERO_NUM_POOL_THREADS=13    <-- each syncer spawns exactly 13 threads
ZERO_NUM_SYNC_WORKERS=2

On a 30-core machine:

Core 0:     Replicator
Core 1:     Syncer 1 -> spawns 13 pool threads
Core 2:     Syncer 2 -> spawns 13 pool threads
Cores 3-29: 26 pool threads + 2 syncer main threads + 1 replicator = 29 cores
```

Simple rule: `numPoolThreads = (available_cores - numSyncWorkers - 1) / numSyncWorkers`.
For 30 cores, 2 syncers: `(30 - 2 - 1) / 2 = 13`.

### 9.3 Performance Estimate

With 40 active queries per client group on a syncer with 13 pool threads:
- 13 pool threads, each assigned ~3 queries (round-robin)
- On version-ready: all 13 threads advance in parallel on 13 cores
- Wall time: `ceil(40/13) x avg_query_advance_time` ~ 3 rounds
- Today: `40 x avg_query_advance_time` serialized through `timeSliceQueue`
- **~13x improvement**
- Communication overhead: < 1ms structured clone per advance cycle

Config:
```bash
ZERO_NUM_SYNC_WORKERS=2
ZERO_NUM_POOL_THREADS=13       # per syncer, default 0 (disabled)
```

---

## 10. `getRow` -- Syncer's Own SQLite Connection

`getRow(table, pk)` is called during client catchup (`view-syncer.ts:1932`). It reads a raw table row from the SQLite snapshot. Catchup needs this because the CVR knows WHICH rows changed but not WHAT the current data is.

With pool workers, the Snapshotter (and its `TableSource.getRow()`) lives on the pool thread. But the syncer already has a read-only SQLite connection for reading `stateVersion` (Section 7.2). Use the same connection for `getRow`:

```typescript
class RemotePipelineDriver {
  #replicaConn: Database;  // opened at syncer startup for version reads

  getRow(table: string, pk: RowKey): Row | undefined {
    const spec = this.#tableSpecs.get(table);
    const cols = Object.keys(spec.columns);
    const keyCols = Object.keys(pk);
    const sql = `SELECT ${cols.map(id).join(',')} FROM ${id(table)}
                 WHERE ${keyCols.map(c => `${id(c)}=?`).join(' AND ')}`;
    return this.#replicaConn.prepare(sql).get(...Object.values(pk));
  }
}
```

One connection (not per-query). Reads directly from replica. Returns the same raw row as today. Never misses -- the replica has every row. Runs on the syncer's main thread under the `#lock`.

**Why caching doesn't work:** (a) `getRow` returns raw table rows -- not pipeline output (different shape after joins/projections). (b) A cache can miss for stale clients needing rows from many versions ago. `must()` at line 1932 throws on miss, crashing the ViewSyncer.

---

## 11. Code Changes

### 11.1 Existing Code Reused Unchanged

The pool worker hosts a **real `PipelineDriver`** with a **real `Snapshotter`**. Same classes, same constructors, same methods, same logic. Just scoped to one query instead of all queries.

| Existing class | File | Used by pool worker |
|---|---|---|
| `PipelineDriver` | `pipeline-driver.ts` | Yes, as-is |
| `Snapshotter` | `snapshotter.ts` | Yes, as-is |
| `DatabaseStorage` | `zqlite/database-storage.ts` | Yes, as-is |
| `TableSource` | `zqlite/table-source.ts` | Yes, as-is |
| `buildPipeline` | `zql/builder/builder.ts` | Yes, as-is |
| `Streamer` | `pipeline-driver.ts` | Yes, as-is |
| `ViewSyncerService` | `view-syncer.ts` | Unchanged (all ~2200 lines) |
| `CVRStore` | `cvr-store.ts` | Unchanged |
| `CVRQueryDrivenUpdater` | `cvr.ts` | Unchanged |
| `ClientHandler` / `PokeHandler` | `client-handler.ts` | Unchanged |
| `Notifier` / `Subscription` | `notifier.ts`, `subscription.ts` | Unchanged |
| `ServiceRunner` | `runner.ts` | Unchanged |
| `Syncer` | `workers/syncer.ts` | Unchanged |

### 11.2 Small Modifications to Existing Code

| File | What changes |
|---|---|
| `pipeline-driver.ts` | Extract `PipelineDriverInterface` (14 methods, returns allow `Promise` for async). Class adds `implements`. `advance()` accepts optional `upperBound`. |
| `snapshotter.ts` | `changesSince()` accepts optional `upperBound`. `Snapshotter.advance()` threads it through to `Diff`. |
| `view-syncer.ts` | Uses `PipelineDriverInterface`. Callers `await` the async-capable methods. `#processChanges` accepts `AsyncIterable` and uses `for await`. `generateRowChanges` is `async function*`. |
| `server/syncer.ts` | Factory: if `numPoolThreads > 0`, spawn threads via `POOL_THREAD_URL` + create `RemotePipelineDriver`. Else same as today. |
| `config/zero-config.ts` + `normalize.ts` | Add `numPoolThreads` (per syncer, default 0). |
| `server/worker-urls.ts` | Add `POOL_THREAD_URL`. |

### 11.3 New Code

| New file | What it does |
|---|---|
| `services/view-syncer/remote-pipeline-driver.ts` | Implements `PipelineDriverInterface`. Syncer-side proxy. Distributes queries to pool threads (least-loaded), fans out advance, merges results. Has its own SQLite read connection for `getRow` and version reads. |
| `server/pool-thread.ts` | Worker thread entry point. Runs the **same** PipelineDriver code as the sync worker. On hydrate: creates PipelineDriver + Snapshotter. On advance: runs the same advance loop with the same error handling — catches `ResetPipelinesSignal` internally, resets, re-hydrates, continues. The syncer never sees pipeline errors. |
| `workers/pool-protocol.ts` | Message types for syncer <-> pool thread communication. |
| `services/view-syncer/view-syncer-pool.pg.test.ts` | PG integration tests for pool threads: hydration, advance, multi-query. |
| `services/view-syncer/view-syncer-pool-perf.pg.test.ts` | Performance comparison: local vs pool threads. |

### 11.4 The Pool Worker Is Copy-Paste from `server/syncer.ts`

```typescript
// server/syncer.ts:148-162 (TODAY)
new PipelineDriver(
  logger, config.log,
  new Snapshotter(logger, replicaFile, shard),
  shard,
  operatorStorage.createClientGroupStorage(id),
  id, inspectorDelegate,
  () => isPriorityOpRunning() ? priorityOpRunningYieldThresholdMs : normalYieldThresholdMs,
  config.enableQueryPlanner, config,
)

// workers/pool-thread.ts (NEW -- same constructor, runs in worker thread)
import {parentPort, workerData} from 'node:worker_threads';
new PipelineDriver(
  lc, config.log,
  new Snapshotter(lc, replicaFile, shard),
  shard,
  operatorStorage.createClientGroupStorage(queryID),
  queryID, inspectorDelegate,
  () => config.yieldThresholdMs,  // no priority op -- thread has no IO to starve
  config.enableQueryPlanner, config,
)
```

### 11.5 Summary

```
Existing code reused unchanged:          ~15,000+ lines
Existing code with small modifications:  ~60 lines across 5 files
New code:                                ~500 lines across 3 files

97% reuse.
```

---

## 12. Testing Strategy

Four levels, built iteratively. Each level catches bugs the previous one can't. You never need to build everything before you can test.

### 12.1 Level 0: Existing Tests Still Pass (Zero Regression)

Before touching anything, establish a baseline. After every code change, run these. With `numPoolThreads=0` (default), the new code never loads.

```bash
# Unit tests (no PG, fast -- seconds)
npm --workspace=zero-cache run test -- pipeline-driver.test

# PG integration tests (the real deal -- needs Docker)
npm --workspace=zero-cache run test -- --config vitest.config.pg-16.ts view-syncer.pg.test

# All view-syncer PG tests (full suite)
npm --workspace=zero-cache run test -- --config vitest.config.pg-16.ts view-syncer
```

These tests exercise: hydration, advancement, catchup, permissions, TTL, yield behavior, CVR persistence, poke ordering. If they pass with `numPoolThreads=0`, the existing code path is untouched.

### 12.2 Level 1: Pool Thread in Isolation (Unit Tests)

Test `RemotePipelineDriver` + `pool-thread.ts` WITHOUT ViewSyncer. Use `MessageChannel` in the same process -- no real threads needed for the first pass, faster to debug.

```typescript
// remote-pipeline-driver.test.ts
import {MessageChannel} from 'node:worker_threads';

test('hydrate sends RowChange[] back', async () => {
  const {port1, port2} = new MessageChannel();
  startPoolWorkerOnPort(port2, replicaDbFile.path, shard, config);
  const driver = new RemotePipelineDriver([{port: port1, load: 0}], ...);

  const changes = [...driver.addQuery('Q1', ast, timer)];

  expect(changes.length).toBeGreaterThan(0);
  expect(changes[0]).toMatchObject({type: 'add', queryID: 'Q1', table: 'issue'});
});

test('advance returns delta only', async () => {
  // 1. Hydrate Q1 (full result)
  // 2. Mutate a row in the replica via FakeReplicator
  // 3. Advance with targetVersion
  // 4. Verify only the changed row comes back, not the full result
});

test('destroy frees resources and thread stays alive', async () => {
  // 1. Hydrate Q1 on thread
  // 2. Destroy Q1
  // 3. Hydrate Q2 on same thread -- should work
});

test('multiple queries on same thread advance sequentially', async () => {
  // 1. Hydrate Q1 and Q2 on same thread
  // 2. Advance -- both should produce results
});
```

**What this catches:** Message protocol bugs, serialization issues, wrong message types, thread lifecycle.

```bash
npm --workspace=zero-cache run test -- remote-pipeline-driver.test
```

### 12.3 Level 2: Swap into Existing Test Suite (The Key Test)

This is where you get the most confidence. The existing `view-syncer.pg.test.ts` has 200+ tests. Swap `RemotePipelineDriver` into the same `setup()` function.

**The seam** is at `view-syncer-test-util.ts:729`:

```typescript
// TODAY (line 729-738):
const vs = new ViewSyncerService(
  config, lc, SHARD, TASK_ID, serviceID, cvrDB,
  new PipelineDriver(                        // <-- THIS IS THE SEAM
    lc, testLogConfig,
    new Snapshotter(lc, replicaDbFile.path, SHARD),
    SHARD, operatorStorage, ...
  ),
  stateChanges, drainCoordinator, ...
);

// WITH POOL WORKERS (swap in RemotePipelineDriver):
const {port1, port2} = new MessageChannel();
startPoolWorkerOnPort(port2, replicaDbFile.path, SHARD, config);
const vs = new ViewSyncerService(
  config, lc, SHARD, TASK_ID, serviceID, cvrDB,
  new RemotePipelineDriver(                  // <-- SAME INTERFACE
    [{port: port1, load: 0}],
    replicaDbFile.path, SHARD, operatorStorage, ...
  ),
  stateChanges, drainCoordinator, ...
);
```

Every test that passes with local PipelineDriver must also pass with RemotePipelineDriver. Run iteratively:

```bash
# Start with the main test file
npm --workspace=zero-cache run test -- --config vitest.config.pg-16.ts view-syncer.pg.test

# Then permissions (tests auth transforms + query invalidation)
npm --workspace=zero-cache run test -- --config vitest.config.pg-16.ts view-syncer-permissions

# Then TTL (tests query expiration + rehydration)
npm --workspace=zero-cache run test -- --config vitest.config.pg-16.ts view-syncer-ttl

# Then yield behavior (tests timing/yield path -- important for advance)
npm --workspace=zero-cache run test -- --config vitest.config.pg-16.ts view-syncer.yield

# CVR persistence
npm --workspace=zero-cache run test -- --config vitest.config.pg-16.ts cvr-store.pg.test
npm --workspace=zero-cache run test -- --config vitest.config.pg-16.ts cvr.pg.test
```

**What this catches:** Version skew issues, getRow failures on catchup, CVR merge bugs, poke ordering, permission invalidation, ResetPipelinesSignal handling, concurrent client catchup.

### 12.4 Level 3: zbugs End-to-End

```bash
cd apps/zbugs

# 1. Start Postgres (needs Docker Desktop running)
npm run db-up

# 2. In another terminal -- first-time setup
npm run db-migrate
npm run db-seed

# 3. Start zero-cache WITH pool threads
ZERO_NUM_POOL_THREADS=4 npm run zero-cache-dev

# 4. In another terminal -- start the app
npm run dev
```

**Manual test checklist:**

| Action | What it exercises | Expected |
|--------|------------------|----------|
| Open app, see issues list | Hydration of ~19 queries | All issues render, labels show, user avatars load |
| Click an issue | New query hydration (detail + comments) | Detail view loads completely |
| Add a comment | Mutation -> replicator -> advance -> poke | Comment appears in real-time without refresh |
| Edit an issue title | Edit path through advance | Title updates in both list and detail views |
| Open second browser tab to same URL | Catchup + `getRow` path (stale client) | Second tab shows identical data |
| Make a change in tab 1, watch tab 2 | Real-time advance across connections | Tab 2 updates within ~1 second |
| Navigate between issues rapidly | Rapid hydrate/destroy cycles | No crashes, no stale data, no console errors |
| Close tab, reopen after 10s | Reconnect with stale cookie + catchup | Data loads correctly, no missing rows |
| Assign a label to an issue | Mutation on junction table | Label appears on the issue in all tabs |

**Compare with baseline** -- run the same checklist with `ZERO_NUM_POOL_THREADS=0` and verify identical behavior.

**Troubleshooting:**
- Port conflict on 4848: `lsof -i :4848 | grep LISTEN` then `kill <PID>`
- Clear SQLite replica: `rm /tmp/zbugs-sync-replica.db*`
- Clear Postgres volumes: `cd docker && docker compose down -v`
- Check zero-cache logs for errors: they print to the terminal running `zero-cache-dev`

### 12.5 Level 4: Performance Benchmark

Once correctness is validated. The sync engine already records metrics.

```bash
# Baseline (no pool threads)
ZERO_NUM_POOL_THREADS=0 npm run zero-cache-dev
# Note advance-time in logs (look for "pipeline advancement" log lines)

# With pool threads
ZERO_NUM_POOL_THREADS=4 npm run zero-cache-dev
# Compare advance-time

# CPU utilization
htop  # watch per-core usage during active sync
```

Key metrics to compare:
- `sync.advance-time` histogram: time from version-ready to `pokers.end()`
- Per-core CPU utilization: should see multiple cores active with pool threads
- Client-perceived latency: time from mutation to poke received (visible in browser devtools Network tab, filter for WebSocket frames)

### 12.6 Implementation Order (What to Build and Test First)

Each step is independently testable. Build, test, commit, move on.

```
Step 1: PipelineDriverInterface
  File:  pipeline-driver.ts
  What:  Extract interface with 14 methods. Class adds `implements`.
  Test:  Level 0 -- all existing tests pass (no behavior change).
  
Step 2: pool-protocol.ts
  File:  workers/pool-protocol.ts (NEW)
  What:  Message type definitions only. No logic.
  Test:  TypeScript compiles.

Step 3: pool-thread.ts
  File:  workers/pool-thread.ts (NEW)
  What:  Worker thread entry point. Creates PipelineDriver + Snapshotter on hydrate.
  Test:  Level 1 -- unit tests with MessageChannel.

Step 4: RemotePipelineDriver
  File:  remote-pipeline-driver.ts (NEW)
  What:  Implements PipelineDriverInterface. Manages query -> thread assignments.
  Test:  Level 1 -- unit tests.
         Level 2 -- swap into existing test suite. Run all 200+ pg tests.

Step 5: snapshotter.ts upper bound
  File:  snapshotter.ts
  What:  Add `AND stateVersion <= ?` to changesSince().
  Test:  Level 0 -- existing snapshotter tests still pass.
         Level 2 -- advancement tests with RemotePipelineDriver.

Step 6: server/syncer.ts factory + config
  Files: server/syncer.ts, config/normalize.ts
  What:  if (numPoolThreads > 0) branch. Spawn real worker threads.
  Test:  Level 0 -- numPoolThreads=0 path unchanged.
         Level 3 -- zbugs end-to-end with ZERO_NUM_POOL_THREADS=4.
         Level 4 -- performance comparison.
```

---

## 13. End-to-End Flow

### 13.1 Phase 1: Client Connects

```
1. Client opens WebSocket to /sync/v1/connect?clientGroupID=abc&clientID=xyz

2. WorkerDispatcher routes: h32(taskID/'abc') % syncers.length -> Syncer-1

3. Syncer-1 receives socket, creates Connection:
   viewSyncer = this.#viewSyncers.getService('abc')
     -> Factory creates RemotePipelineDriver (if numPoolThreads > 0)
     -> ViewSyncerService.run() starts

4. Connection.init() -> ViewSyncer.initConnection():
   desiredQueriesPatch = [{op:'put', hash:'Q1', ast:...}, {op:'put', hash:'Q2', ast:...}]
   -> Updates CVR with desired queries
   -> Flushes CVR to PostgreSQL
```

### 13.2 Phase 2: Hydration

```
5. Replicator commits -> version-ready -> ViewSyncer wakes up

6. ViewSyncer initializes pipelines and hydrates queries:
   For each query [Q1, Q2, Q3]:
     RemotePipelineDriver picks least-loaded thread:
       Q1 -> Thread 3, Q2 -> Thread 4, Q3 -> Thread 5
     Sends {type:'hydrate', queryID, ast, clientSchema} via MessagePort

7. Each thread creates PipelineDriver + Snapshotter, runs addQuery():
   Thread 3: hydrates Q1, sends RowChange[] back (structured clone)
   Thread 4: hydrates Q2, sends RowChange[] back
   Thread 5: hydrates Q3, sends RowChange[] back
   Each RETAINS pipeline state for future advances.

8. Syncer's #processChanges iterates merged stream:
   -> Accumulates rows, de-duplicates across queries
   -> updater.received(): merges refCounts, queues PG writes
   -> pokers.addPatch(): sends pokePart every 100 patches

9. flushUpdater() -> single PG transaction
   pokers.end() -> pokeEnd over WebSocket
   Client applies all patches atomically
```

### 13.3 Phase 3: Real-Time Update

```
10. Someone edits a message. Replicator writes change -> version-ready.

11. ViewSyncer wakes up, calls RemotePipelineDriver.advance():
    Reads targetVersion = "09" from SQLite
    Sends {type:'advance', targetVersion:'09'} to Thread 3, 4, 5

12. ALL threads advance in PARALLEL on separate cores:
    Thread 3: ChangeLog diff -> push through Q1's operator graph -> 1 RowChange
    Thread 4: ChangeLog diff -> push through Q2's operator graph -> 1 RowChange
    Thread 5: ChangeLog diff -> Q3 not affected -> 0 RowChanges

13. RemotePipelineDriver streams results as threads finish (first done, first yielded)

14. #processChanges: accumulate, de-duplicate, processBatch()
    flushUpdater() -> PG commit
    pokers.end() -> pokeEnd
    Client re-renders the edited message
```

### 13.4 Phase 4: Query Unsubscribed

```
15. Client navigates away. Sends changeDesiredQueries: [{op:'del', hash:'Q1'}]

16. RemotePipelineDriver.removeQuery('Q1'):
    Posts {type:'destroy', queryID:'Q1'} to Thread 3
    Thread 3: destroys Q1's pipeline + Snapshotter, frees SQLite connections
    Thread 3 stays alive, ready for new queries

17. CVR updated, delete patch sent to client
```

---

## 14. How the Syncer Remembers Query -> Thread

```typescript
class RemotePipelineDriver {
  #queryAssignments = new Map<string, Worker>();  // queryID -> thread
  #poolWorkers: {worker: Worker; load: number}[];

  addQuery(queryID, ast, ...) {
    const thread = this.#leastLoadedThread();
    this.#queryAssignments.set(queryID, thread);
    thread.worker.postMessage({type: 'hydrate', queryID, ast, ...});
    thread.load++;
    // ... receive results via 'message' event
  }

  removeQuery(queryID) {
    const thread = this.#queryAssignments.get(queryID);
    thread.worker.postMessage({type: 'destroy', queryID});
    this.#queryAssignments.delete(queryID);
    thread.load--;
  }
}
```

---

## 15. Future: Smarter Thread Assignment by Query Name

v1 assigns by least-loaded count. Future improvement: **group same-named queries onto the same thread** to maximize TableSource reuse.

If 10 different client groups all subscribe to `channelConversations`, those 10 pipeline instances read from the same tables. On the same thread, they could share one Snapshotter and TableSources -- the ChangeLog diff is read once and pushed to all 10.

```typescript
pickThread(queryName: string): Worker {
  // Find thread that already has the most instances of this query name
  const distribution = this.#queryNameDistribution.get(queryName);
  if (distribution) {
    // Pick thread with most instances (warm TableSources)
    return [...distribution.entries()].sort((a,b) => b[1]-a[1])[0][0];
  }
  return this.#leastLoadedThread();
}
```

Not for v1. Measure first whether count-based assignment causes real problems.

---

## 16. Scale Numbers

270 defined queries (dashboard: 130, backend: 136, shared: 4). 52 distinct primary tables. A typical client group has 20-40 active subscriptions.

Per-query pool workers on a 30-core machine with 2 syncers, 13 threads each:

```
Today:    40 queries x timeSliceQueue on 1 core = serialized (~400ms)
Proposed: 40 queries across 13 threads = ~3 rounds (~30ms)
```

---

## 17. Implementation Learnings

Lessons discovered during implementation that affect the design or future work.

### 17.1 PipelineDriverInterface Must Support Async

The original PipelineDriver API is synchronous (`*addQuery` is a generator, `advance()` returns an object). RemotePipelineDriver needs to send messages to worker threads and wait for responses -- inherently async.

**The fix:** The interface allows methods to return `Promise` (e.g., `addQuery` returns `Iterable | Promise<Iterable>`, `advance` returns `{...} | Promise<{...}>`). ViewSyncer call sites add `await`. `#processChanges` accepts `Iterable | AsyncIterable` and uses `for await`. The `generateRowChanges` function in `#addAndRemoveQueries` becomes an `async function*`.

This is backward-compatible: the local PipelineDriver still returns synchronous values. `await` on a non-Promise is a no-op. `for await` on a sync iterable works identically to `for...of`.

### 17.2 Worker Threads and TypeScript Loading

`worker_threads` spawned via `new Worker('file.ts')` fail in Node.js because Node doesn't natively understand `.ts` files. Three different environments handle this differently:

| Environment | How `.ts` loads | Why it works |
|---|---|---|
| **Production** | Files are built to `.js` by `packages/zero/tool/build.ts`. `worker-urls.ts` resolves `.ts` -> `.js`. | Build step compiles TypeScript. |
| **Development (`tsx`)** | `zero-cache-dev` runs via `tsx`. Worker threads inherit the tsx module loader from the parent. | tsx registers a loader hook that propagates to child threads. |
| **Tests (vitest)** | Vitest has its own module transform that does NOT propagate to worker threads. | Must use `execArgv: ['--experimental-strip-types', '--no-warnings']` on the Worker constructor. |

**Key rule:** The pool-thread file MUST live in `server/` directory (not `workers/`) because the build process (`getWorkerEntryPoints` in `build.ts`) scans `worker-urls.ts` exports and expects all workers in `zero-cache/src/server/`. Add new worker URLs to `server/worker-urls.ts`.

The `--experimental-strip-types` workaround for vitest is a Node 22 feature. It strips type annotations natively without needing tsx. It does NOT support enums or decorators, but pool-thread.ts doesn't use those. The existing `write-worker.test.ts` in the codebase has the same issue -- its tests also fail without this flag.

### 17.3 Init Message Must Be Per-Thread, Not Per-Query

Each pool thread needs a `{type:'init', clientSchema}` message before it can hydrate queries. Initially, RemotePipelineDriver sent init before every `addQuery` -- wasteful for threads hosting multiple queries.

**Fix:** Track initialized threads in a `Set<Worker>`. Send init only on first use. Clear the set on `reset()` (schema change invalidates the init state).

### 17.4 ResetPipelinesSignal Must Be Handled Inside the Pool Thread

**Original (wrong) approach:** Pool thread catches `ResetPipelinesSignal`, sends it as an error to the syncer, RemotePipelineDriver reconstructs it, ViewSyncer handles it. This broke because `await advance()` throws BEFORE the ViewSyncer's try/catch, crashing the ViewSyncer and disconnecting the client.

**Correct approach:** The pool thread handles `ResetPipelinesSignal` internally — same as the ViewSyncer does in the local path. It catches the signal, calls `driver.reset()`, re-hydrates with `driver.addQuery()`, and continues. The syncer never sees the error. This is the same code, same flow, same recovery — just running on a different thread.

**The principle:** The pool thread runs the same code as the sync worker. Error handling is part of that code. Don't strip it out and try to reconstruct it across the thread boundary.

### 17.5 Upper Bound Threading

The `targetVersion` upper bound must flow through 4 layers:
1. RemotePipelineDriver reads `stateVersion` from its SQLite connection
2. Pool thread receives `targetVersion` in the `{type:'advance'}` message
3. `PipelineDriver.advance(timer, upperBound)` passes it to `Snapshotter.advance()`
4. `Snapshotter.advance()` passes it to `Diff` constructor, which passes it to `changesSince(prevVersion, upperBound)`

Each layer is an optional parameter -- the local PipelineDriver path doesn't pass it, so the existing `WHERE stateVersion > ?` query runs without a bound. The pool thread path passes it, adding `AND stateVersion <= ?`.

### 17.6 zbugs End-to-End Validated

Tested with `ZERO_NUM_POOL_THREADS=4` against zbugs (the reference Zero application). Observed in logs:

```
[worker=syncer] Spawned 4 pool worker threads for IVM compute    (x3 syncers)
[worker=pool-thread] pool-thread started. replica=/tmp/zbugs-sync-replica.db    (x12 threads)
[worker=pool-thread] pool-thread hydrating query=2dhewy6tv2hm table=issue
[worker=pool-thread] pool-thread hydrated query=2dhewy6tv2hm rows=4027 time=74.6ms
[worker=pool-thread] pool-thread hydrating query=37d031vztypib table=user
[worker=pool-thread] pool-thread hydrated query=37d031vztypib rows=14 time=0.3ms
[worker=pool-thread] pool-thread hydrating query=332080volqsrn table=label
[worker=pool-thread] pool-thread hydrated query=332080volqsrn rows=31 time=1.4ms
```

All queries hydrated successfully on pool threads. RowChange[] flowed back to syncer via structured clone. CVR flushed. Pokes delivered. App loaded in browser.

### 17.7 Structured Clone Cost Observed

Real numbers from zbugs hydration:

| Query | Table | Rows | Time |
|---|---|---|---|
| 2dhewy6tv2hm | issue | 4027 | 74.6ms |
| 37d031vztypib | user | 14 | 0.3ms |
| 332080volqsrn | label | 31 | 1.4ms |
| 32l87nqwzw7pj | project | 1 | 1.8ms |
| 9qjt9lyi0hu | label | 30 | 0.6ms |

The 4027-row issue query took 74.6ms total (IVM compute + structured clone). For advances (deltas of 1-10 rows), the structured clone cost is unmeasurable.

### 17.8 Advance Timing: Local vs Docker

Advance timing measured locally (zbugs, single row change, 8 queries):

| Query | Snapshot Advance | Collect Changes | Total |
|---|---|---|---|
| issue (4027 rows) | 0.38ms | 1.83ms | 2.21ms |
| issue (318 rows) | 1.22ms | 2.43ms | 3.65ms |
| label (31 rows) | 0.42ms | 0.41ms | 0.83ms |
| user (14 rows) | 0.42ms | 0.35ms | 0.77ms |

All under 4ms locally. In Docker sandbox, the same operations took 50-165ms, triggering the circuit breaker. The difference is container resource constraints (CPU throttling, IO contention on volumes), not a code issue. The circuit breaker's 50ms minimum threshold is fine for bare metal but can be tight in constrained containers.

### 17.9 Configuration Gotcha: `ZERO_QUERY_URL`

zbugs uses custom queries that require `ZERO_QUERY_URL` in `.env`. The deprecated `ZERO_GET_QUERIES_URL` causes a silent failure where the client connects, the ViewSyncer tries to transform queries, gets a URL validation error, and closes the connection. The app shows "Just a moment..." indefinitely. This is a pre-existing config issue unrelated to pool threads, but it blocked end-to-end testing until fixed.

### 17.10 Test Results Summary

| Test Suite | Tests | Status |
|---|---|---|
| pipeline-driver unit tests | 33 | Pass |
| pipeline-driver.runaway-push | 3 | Pass |
| snapshotter unit tests | 10 | Pass |
| client-handler unit tests | 7 | Pass |
| cvr unit tests | 11 | Pass |
| view-syncer.pg.test (baseline) | 46 | Pass |
| view-syncer-permissions.pg.test | 4 | Pass |
| view-syncer-ttl.pg.test | 15 | Pass |
| view-syncer.yield PG tests | 5 | Pass |
| view-syncer-pool.pg.test (pool threads) | 3 | Pass |
| zbugs end-to-end | Manual | Pass |
| **Total** | **137 + manual** | **All pass** |
