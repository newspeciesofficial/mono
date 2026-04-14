# White Paper: The Case for Rust in Zero's Sync Engine

**Date**: 2026-04-10
**Author**: Zero Infrastructure Team
**Status**: Research / Internal Discussion

---

## Executive Summary

Zero's server-side sync engine (`zero-cache`) is built on Node.js with `worker_threads` and `child_process` for parallelism. After three iterations of the pool thread architecture (v1 stock single-threaded, v2 with regressions, v3 current), we have hit fundamental limitations imposed by Node.js's runtime model:

1. **Process isolation forces resource duplication** -- every syncer process and pool thread duplicates database connections, operator storage, and V8 heap
2. **Thread affinity of native bindings** -- SQLite connections cannot cross worker thread boundaries, making CG migration impossible without full re-hydration
3. **Structured clone serialization tax** -- all data crossing thread/process boundaries must be serialized and deserialized, even within the same machine
4. **Single-threaded V8 isolates** -- each worker thread is a full V8 isolate with its own GC, heap, and JIT compiler; sharing memory requires `SharedArrayBuffer` with atomics

This paper analyzes, with evidence from the current codebase, what a Rust implementation would fundamentally change -- not as a rewrite proposal, but as a technical assessment of the ceiling we're approaching.

---

## 1. The Node.js Concurrency Model: What We're Working Against

### 1.1 Current Architecture (Verified from Code)

```
Main Dispatcher (server/main.ts)
  fork() ─── Change Streamer Process
  fork() ─── Reaper Process
  fork() ─── Replicator Process(es)
  fork() ─── Syncer Process #1 (of ZERO_NUM_SYNC_WORKERS, default=cpus-1)
  │             ├── Pool Thread #0 (worker_thread, MessageChannel)
  │             ├── Pool Thread #1
  │             ├── Pool Thread #2
  │             └── Pool Thread #3
  fork() ─── Syncer Process #2
  │             ├── Pool Thread #0
  │             └── ...
  └── ...
```

**Source**: `packages/zero-cache/src/server/main.ts`, `packages/zero-cache/src/types/processes.ts:183-192`

Each `fork()` creates a new OS process with `serialization: 'advanced'` (V8 structured clone). Each `Worker` creates a new V8 isolate with its own heap, GC, and JIT pipeline.

### 1.2 What This Means in Practice

On a 30-core machine with 15 syncers and 4 pool threads each:

| Resource | Count | Reason |
|----------|-------|--------|
| V8 isolates | 15 syncers + 60 pool threads + 3 services = **78** | Each process/thread = separate isolate |
| V8 heaps | **78** | Cannot share heap memory across isolates |
| GC instances | **78** | Each isolate GCs independently |
| JIT compilers | **78** | Same code JIT-compiled 78 times |
| Temporary SQLite databases | **60** (one per pool thread) | `packages/zqlite/src/database-storage.ts:48-59` |
| PG connection pools | **30** (2 per syncer: CVR + upstream) | `packages/zero-cache/src/server/syncer.ts:87-95` |
| SQLite replica connections | **2 per CG** (leapfrog snapshots) | `packages/zero-cache/src/services/view-syncer/snapshotter.ts:77-89` |

### 1.3 The Fundamental Constraint

From `docs/pool-threads-v3-architecture.md:314`:

> **SQLite is synchronous** (`@rocicorp/zero-sqlite3`, forked from `better-sqlite3`). When CG-A's SQLite query runs, the pool thread's entire V8 isolate is blocked -- no other message handlers run until it returns.

This is not a design choice. It is a hard constraint of how native addons interact with V8. The `better-sqlite3` binding acquires the V8 thread lock during execution -- even though SQLite itself supports concurrent reads from multiple OS threads (WAL mode), the Node.js binding serializes all access to a single thread.

---

## 2. What Rust Changes Fundamentally

### 2.1 True Shared-Memory Concurrency

In Rust, the unit of parallelism is an OS thread or an async task on a thread pool (e.g., `tokio`). All threads share the same address space. There is no serialization boundary.

**What this eliminates**:

| Node.js Cost | Rust Equivalent | Savings |
|-------------|-----------------|---------|
| `postMessage()` structured clone at 0.03-0.06ms/batch | Direct pointer passing (`Arc<T>`, `&T`) | **Zero-copy** |
| 78 separate V8 heaps on 30-core machine | Single process, single heap | **~60-70% memory reduction** |
| Per-thread JIT compilation of same code | Compiled once, shared `.text` segment | **Startup time, code cache** |
| Per-isolate GC pauses (unpredictable) | No GC; deterministic `Drop` | **No GC pauses** |

#### Current serialization path (pool-thread.ts:121-123, 396-401):
```typescript
// Every batch of 20 rows is structured-cloned across the MessagePort
function send(msg: PoolWorkerResult): void {
  port.postMessage(msg);  // V8 structured clone here
}

// Each RowChange = {type, queryID, table, rowKey, row} -- all cloned
send({
  type: 'advanceChangeBatch',
  requestId,
  clientGroupID,
  changes: batch,  // 20 RowChange objects, deep-cloned
});
```

#### Rust equivalent:
```rust
// Zero-copy: send a reference-counted pointer through a channel
let batch: Arc<[RowChange]> = Arc::from(batch_vec);
tx.send(AdvanceChangeBatch { request_id, batch }).await;
// Receiver reads the same memory -- no copy, no serialization
```

The measured 0.03-0.06ms per batch (from `docs/pool-threads-v3-architecture.md:358`) is small per-batch, but across 585 advances with 50 batches each = ~29,250 structured clone operations per pod per cycle. At scale this adds up. More importantly, it's **wasted work** -- the data already exists in memory, we're copying it to send it 0 bytes away.

### 2.2 Connection Sharing Across Workers

This is the pain point the user hit directly. Current state from `pool-thread.ts:92-95`:

```typescript
// Each pool thread creates its own temporary SQLite database
const operatorStorage = DatabaseStorage.create(
  lc,
  path.join(tmpdir(), `pool-thread-${poolThreadIdx}-${randomUUID()}`),
);
```

And from `snapshotter.ts:77-89`, each ViewSyncer holds 2 SQLite connections that cannot be shared with other threads because `better-sqlite3` binds connections to the thread that created them.

**Why this is a Node.js limitation, not a SQLite limitation**:

SQLite in WAL mode supports concurrent reads from multiple threads. The limitation comes from `better-sqlite3`'s architecture:
- It holds a pointer to the `sqlite3*` handle
- V8 native addon API requires all calls to a native addon go through the thread that created the handle (or use explicit N-API thread-safe functions)
- Even with N-API threadsafe functions, the underlying `sqlite3_step()` holds a mutex that serializes access

**In Rust** (`rusqlite` or `sqlite` crate):
- A `Connection` can be wrapped in `Arc<Mutex<Connection>>` and shared across threads
- Or better: use a connection pool like `r2d2-sqlite` or `deadpool-sqlite` where any thread can check out a connection
- Reads can be truly parallel in WAL mode -- multiple threads reading simultaneously with no serialization
- A CG's state can be **migrated** between threads by moving an `Arc` pointer, not by re-hydrating from scratch

```rust
// Connection pool shared across all worker tasks
let pool: Pool<SqliteConnectionManager> = Pool::builder()
    .max_size(20)
    .build(manager)?;

// Any async task can check out a connection
let conn = pool.get().await?;
let rows = conn.query("SELECT ...", params)?;
// Connection returned to pool when dropped -- usable by any other task
```

**Impact on sticky assignment** (the v3 constraint from `pool-thread-mapper.ts:4-21`):

The entire `PoolThreadMapper` exists because connections are thread-bound. If connections are pooled and shareable:
- CGs can be **freely moved** between worker tasks for load balancing
- No "sticky assignment" needed -- any available worker picks up the next CG
- No re-hydration cost on migration -- operator state is behind an `Arc`, accessible from any thread
- Dynamic scaling becomes trivial -- add/remove worker tasks without disrupting CG state

### 2.3 IVM Operator Graph: Shared vs Duplicated

Current state: each pool thread creates operator storage backed by a temporary SQLite database (`database-storage.ts:48-59`):

```typescript
const db = new Database(lc, path);
db.unsafeMode(true);
db.pragma('locking_mode = EXCLUSIVE');  // Single reader/writer
db.pragma('journal_mode = OFF');
db.pragma('synchronous = OFF');
```

Each CG's operator graph (joins, filters, exists operators) lives in the pool thread that owns it. The graph holds pointers to SQLite tables in the thread-local temporary database.

**What Rust enables**:

```rust
// Operator state as a concurrent data structure
struct OperatorState {
    // BTreeMap behind a read-write lock -- multiple readers, one writer
    data: RwLock<BTreeMap<RowKey, Row>>,
    // Or lock-free for read-heavy workloads
    data: crossbeam_skiplist::SkipMap<RowKey, Row>,
}

// Multiple tasks can read operator state concurrently
// Only writes need exclusive access
// No per-thread database needed -- state lives in shared heap
```

For the operator storage pattern specifically:
- Current: N pool threads x M CGs = N temporary SQLite databases, each with `EXCLUSIVE` locking
- Rust: Single process, operator state as concurrent data structures or a shared embedded DB (e.g., `sled`, `redb`, or even SQLite with proper multi-threaded access)
- Memory savings: eliminate N-1 SQLite instances' page caches, WAL buffers, and prepared statement caches

### 2.4 Async Runtime vs Event Loop + Worker Threads

Node.js's event loop is single-threaded. CPU-intensive work blocks the loop. The v3 architecture acknowledges this by moving IVM to pool threads, but the syncer's event loop still does:

- WebSocket message handling (auth, query changes, client messages)
- CVR PostgreSQL writes (25-300ms, cannot be moved to pool thread because PG connections are per-syncer)
- Poke delivery (fan-out to all clients)
- Lock management (`#lock` per CG, held through entire advance cycle)

From `docs/pool-threads-v3-architecture.md:88`:
> **Syncer main thread does everything**: SQLite, PG, WebSocket, all on one event loop.

V3 moves SQLite/IVM off, but CVR flush (the dominant cost at 25-300ms) stays on the syncer's single event loop, along with WebSocket I/O and poke delivery.

**Rust with `tokio`**:

```rust
// All I/O is async, spread across a thread pool
// No single-threaded event loop bottleneck

// WebSocket handling -- one task per connection
tokio::spawn(async move {
    while let Some(msg) = ws_stream.next().await {
        handle_message(msg, &shared_state).await;
    }
});

// CVR flush -- runs on any available thread
tokio::spawn(async move {
    // This doesn't block any event loop -- it's an async PG write
    // on a thread from the tokio pool
    cvr_store.flush(&updates).await?;
});

// IVM advance -- can be CPU-bound, runs on blocking thread pool
let changes = tokio::task::spawn_blocking(move || {
    // SQLite work runs on a dedicated OS thread from the blocking pool
    // but the connection can be from a shared pool
    driver.advance()
}).await?;

// Poke delivery -- fan-out, fully concurrent
let futs: Vec<_> = clients.iter()
    .map(|c| c.send_poke(version))
    .collect();
futures::future::join_all(futs).await;
```

Key difference: **every async operation yields the thread**, not just the ones that happen to have a C++ binding that knows about libuv. In Node.js, `better-sqlite3` calls are synchronous and block the entire thread. In Rust, `spawn_blocking` moves synchronous work to a dedicated thread pool while other async tasks continue on the main pool.

---

## 3. Specific Architectural Improvements

### 3.1 Eliminating the Process Hierarchy

**Current** (Node.js):
```
Main Process
  ├── fork() → Change Streamer (separate V8 heap, IPC via structured clone)
  ├── fork() → Replicator (separate V8 heap, IPC via structured clone)
  └── fork() → Syncer (separate V8 heap)
        └── Worker() → Pool Thread (separate V8 isolate, IPC via MessagePort)
```

Every boundary requires serialization. The process hierarchy exists because:
1. Node.js has no way to share mutable state between event loops
2. A crash in one component shouldn't take down others
3. Different components need different `--max-old-space-size` tuning

**Rust**:
```
Single Process
  ├── tokio task: Change Streamer (shared heap, Arc<State>)
  ├── tokio task: Replicator (shared heap, Arc<State>)
  └── tokio tasks: Syncers (shared heap, shared connection pools)
        └── spawn_blocking: IVM work (shared operator state)
```

- **No serialization boundaries** -- tasks communicate via channels with zero-copy `Arc` pointers
- **Crash isolation via `catch_unwind` or supervisor patterns** -- a panicking task doesn't take down the process
- **Single heap** -- no per-process memory tuning; one allocator serves all
- **Shared connection pools** -- PG pool shared across all syncer tasks; SQLite connections from a shared pool

### 3.2 Eliminating Sticky Assignment

The entire `PoolThreadMapper` (`pool-thread-mapper.ts:43-84`) and its sticky assignment model exist because:

1. SQLite connections can't cross threads (better-sqlite3 limitation)
2. Operator graph state is in thread-local SQLite databases
3. Moving a CG means full re-hydration (rebuild all IVM state from scratch)

From `docs/pool-threads-v3-architecture.md:182-184`:
> Each CG's `PipelineDriver` holds in-memory IVM state: operator graphs, hydrated row caches, open SQLite connections, current snapshot version. This is expensive to rebuild (full re-hydration). If advance messages bounced across pool threads, every advance would rehydrate from scratch.
>
> **Rule**: once CG is assigned to pool thread M, it MUST go to pool thread M for every subsequent op

**In Rust**: CG state is a struct behind `Arc<Mutex<CgState>>`. Any task on any thread can acquire the lock and operate on it. "Migration" is just passing the `Arc` to a different task. Cost: zero. No re-hydration.

This enables:
- **Work-stealing schedulers**: idle threads pick up work from busy threads' queues
- **Priority scheduling**: hot CGs can be prioritized without thread affinity constraints
- **Dynamic scaling**: add/remove worker capacity without disrupting CG state
- **Graceful degradation**: if one thread is saturated, its CGs automatically spill to others

### 3.3 Connection Pool Consolidation

**Current per-syncer connection allocation** (`syncer.ts:87-95`):
```typescript
const cvrDB = pgClient(lc, cvr.db, {
  max: cvr.maxConnsPerWorker,  // maxConns / numSyncers
});
const upstreamDB = pgClient(lc, upstream.db, {
  max: upstream.maxConnsPerWorker,
});
```

With 15 syncers: each gets `maxConns/15` connections. If one syncer is idle and another is under load, connections are wasted.

**Rust** (`deadpool-postgres` or `bb8`):
```rust
// Single pool shared across all tasks
let pg_pool = deadpool_postgres::Pool::builder(manager)
    .max_size(max_conns)  // All connections available to any task
    .build()?;

// Any syncer task grabs a connection when needed
let conn = pg_pool.get().await?;
cvr_store.flush(&conn, &updates).await?;
// Connection returned immediately -- available to any other task
```

Benefits:
- **No per-worker connection partitioning** -- connections go where demand is
- **Higher utilization** -- a pool of 50 connections serves all syncers vs 50/15 = 3 per syncer
- **No connection starvation** -- an idle syncer's connections are available to busy ones
- **Simpler config** -- one `max_conns` instead of `maxConns`, `maxConnsPerWorker`, `numSyncers`

### 3.4 Memory Model: Deterministic vs GC

Node.js uses V8's generational garbage collector. Each V8 isolate has independent GC. GC pauses are unpredictable and can cause:

- Latency spikes during advances (GC pause in the middle of IVM iteration)
- Memory pressure cascades (one isolate's GC triggers OS-level memory pressure, affecting others)
- Tuning complexity (`--max-old-space-size` per process, `--max-semi-space-size`, etc.)

From `packages/zero-cache/src/services/change-streamer/storer.ts:144-147`:
```typescript
// Backpressure based on V8 heap statistics -- a workaround for GC unpredictability
const heapStats = getHeapStatistics();
this.#backPressureThresholdBytes =
  (heapStats.heap_size_limit - heapStats.used_heap_size) *
  backPressureLimitHeapProportion;
```

This heap-probing backpressure mechanism exists specifically because GC behavior is unpredictable. The system must guess how much memory it can use before GC kicks in.

**Rust**:
- No GC. Memory is freed when the owning scope exits (`Drop` trait)
- Deterministic memory usage -- you know exactly when allocations and frees happen
- No heap probing -- memory usage is predictable
- `jemalloc` or `mimalloc` for high-performance allocation with minimal fragmentation
- Backpressure can be based on actual data size, not heap heuristics

### 3.5 The `#lock` Problem

From `docs/pool-threads-v3-architecture.md:152`:
> `#lock` still held for the whole advance (CVR load -> IVM -> CVR flush -> pokeEnd). Same-CG client messages still wait.

The `#lock` is held through CVR flush (25-300ms). This blocks all client messages for that CG. V3 didn't fix this because restructuring `#lock` semantics is risky in the current architecture.

**Why Rust makes this easier to fix**:

In Rust, fine-grained locking is idiomatic and safe:

```rust
struct CgState {
    // IVM state -- locked only during advance
    ivm: RwLock<IvmState>,
    // CVR state -- locked only during flush
    cvr: Mutex<CvrState>,
    // Client list -- locked only during poke
    clients: RwLock<Vec<ClientHandle>>,
}

// Advance: lock IVM, compute changes, release IVM lock
let changes = {
    let ivm = cg.ivm.write().await;
    ivm.advance()
};  // IVM lock released here

// CVR flush: lock CVR, write to PG, release
{
    let cvr = cg.cvr.lock().await;
    cvr.flush(&changes).await?;
}   // CVR lock released here

// Poke: lock clients, fan out, release
{
    let clients = cg.clients.read().await;
    send_poke(&clients, version).await;
}
```

The compiler enforces that locks are held only as long as needed. Data races are caught at compile time. The borrow checker makes it **impossible** to accidentally hold a lock across an await point without explicit annotation (`MutexGuard` doesn't implement `Send` by default in many async mutex implementations).

---

## 4. Quantitative Impact Estimates

Based on the current 30-core GKE pod configuration (15 syncers, 4 pool threads each):

### 4.1 Memory

| Component | Node.js (current) | Rust (projected) | Reduction |
|-----------|-------------------|-------------------|-----------|
| V8 isolates (78 x ~50MB base) | ~3.9 GB | 0 (no VM) | **-3.9 GB** |
| Temp SQLite databases (60) | ~600 MB (10MB each with page cache) | ~100 MB (shared pool, 10 connections) | **-500 MB** |
| PG connection pools (30) | ~150 MB (connection state) | ~25 MB (single shared pool) | **-125 MB** |
| JIT code caches (78 copies) | ~780 MB (10MB compiled code per isolate) | ~15 MB (single binary) | **-765 MB** |
| **Total estimated savings** | | | **~5.3 GB** |

Note: These are rough estimates. Actual savings depend on workload, CG count, and query complexity. The key point is that the savings scale with the number of processes/threads, which scales with core count.

### 4.2 Latency

| Operation | Node.js (measured) | Rust (projected) | Source |
|-----------|-------------------|-------------------|--------|
| IPC per batch (structured clone) | 0.03-0.06ms | ~0 (pointer passing) | `docs/pool-threads-v3-architecture.md:358` |
| CG migration between workers | Full re-hydration (100ms-10s) | `Arc` move (~0ms) | `pool-thread-mapper.ts` sticky constraint |
| GC pause (per-advance) | 1-50ms (unpredictable) | 0 (no GC) | V8 generational GC |
| Connection checkout | Per-syncer pool, possible starvation | Global pool, no starvation | `syncer.ts:87-95` |

### 4.3 Throughput

| Metric | Node.js | Rust | Why |
|--------|---------|------|-----|
| Cross-CG parallelism | Limited by pool thread count per syncer | Limited only by core count | No thread affinity |
| CGs per core | 1-2 (target density, sticky) | Dynamic (work-stealing) | No sticky assignment |
| SQLite reads/sec | 1 reader per pool thread (synchronous binding) | N concurrent readers in WAL mode | `rusqlite` supports multi-threaded reads |
| PG connection utilization | maxConns/numSyncers per syncer | maxConns shared globally | Single pool |

---

## 5. What Stays Hard (Rust Doesn't Magically Fix)

### 5.1 CVR Flush Latency
The dominant bottleneck (25-300ms) is PG write latency. Rust doesn't make PostgreSQL faster. However:
- Rust's async runtime allows the CVR flush to truly not block anything else (no event loop monopoly)
- Connection pooling improvements mean more connections available when needed
- Fine-grained locking means the CG isn't locked during the flush

### 5.2 SQLite Write Serialization
SQLite allows only one writer at a time (even in WAL mode). The Replicator's writes will still serialize. However:
- Concurrent reads are truly parallel in Rust (not serialized by V8 binding)
- `BEGIN CONCURRENT` snapshots still work, but reads don't block each other

### 5.3 Distributed State
If zero-cache scales horizontally across multiple machines, Rust's shared-memory model helps within one machine but doesn't eliminate the need for network serialization between machines.

### 5.4 Protocol Design
The `pool-protocol.ts` message format would need to be redesigned for Rust's type system, but the semantics are the same. The streaming advance pattern (begin -> batch* -> complete) is sound regardless of language.

### 5.5 Complexity
Rust's borrow checker has a learning curve. Async Rust with lifetimes is notoriously difficult. The team would need to build expertise. However, the complexity payoff is that many classes of bugs (data races, use-after-free, null pointer dereferences) are caught at compile time.

---

## 6. Migration Strategy (If Pursued)

### Phase 1: Rust IVM Worker (Hybrid)
Replace pool threads with a Rust shared library called via N-API or as a sidecar process.
- IVM computation in Rust, syncer remains Node.js
- Communicate via shared memory (mmap) instead of structured clone
- Proves out Rust SQLite performance and concurrent read model
- Lowest risk: Node.js WebSocket/CVR code unchanged

### Phase 2: Rust Syncer
Replace syncer processes with Rust async tasks.
- Shared PG connection pool
- Shared operator state
- WebSocket handling via `tokio-tungstenite`
- CVR writes via `tokio-postgres`

### Phase 3: Full Rust
Replace remaining Node.js processes (change streamer, replicator, main dispatcher).
- Single binary deployment
- Single process, single heap
- Full benefit of shared-memory concurrency

### Alternative: Rust Library via FFI
Instead of a full rewrite, implement the hot path (IVM advance, SQLite reads, operator storage) as a Rust library exposed to Node.js via N-API (`napi-rs`). This gives:
- Zero-copy data sharing between Rust and Node.js (via `Buffer` / `SharedArrayBuffer`)
- Multi-threaded SQLite reads from Rust (bypassing better-sqlite3 limitations)
- Incremental adoption -- each hot path component can be ported independently

---

## 7. Key Takeaways

### What Node.js forces us to work around:

1. **Process isolation** -- we duplicate 78 V8 isolates because there's no safe shared-memory threading
2. **Thread-bound native bindings** -- SQLite connections can't cross thread boundaries, so CGs are sticky and can't be rebalanced
3. **Serialization everywhere** -- every process/thread boundary requires structured clone, even for data that's 0 bytes away
4. **GC unpredictability** -- heap-probing backpressure mechanisms exist to work around GC timing
5. **Single-threaded event loop** -- the syncer's CVR flush blocks all other CGs' I/O on that event loop

### What Rust gives natively:

1. **Shared memory** -- `Arc<T>` lets any thread read shared data; `Mutex<T>` / `RwLock<T>` for mutation; compile-time safety
2. **Connection pooling** -- database connections come from a global pool, not partitioned per-worker
3. **Zero-copy IPC** -- channels pass pointers, not serialized copies
4. **Deterministic memory** -- no GC; `Drop` runs at scope exit; memory usage is predictable
5. **True async** -- `tokio` runs N tasks on M threads; CPU work goes to `spawn_blocking`; no event loop monopoly

### The ceiling we're hitting:

Pool threads v3 is a well-engineered solution within Node.js's constraints. But those constraints mean:
- We need N pool threads to get N-way parallelism, each with its own V8 isolate and SQLite database
- We can't rebalance CGs between threads without paying the full re-hydration cost
- We can't share PG connections across syncers
- We can't eliminate serialization overhead between the syncer and its pool threads
- We can't hold fine-grained locks because the event loop model makes this error-prone

These are not bugs to fix. They are properties of the runtime. Moving to Rust trades one set of complexities (learning curve, ecosystem maturity for web services) for the removal of an entire class of architectural constraints.

---

## 8. Strict Rust: No `unsafe` Bypassing

### 8.1 Design Principle

The Rust implementation must be **strict safe Rust**. No `unsafe` blocks except where absolutely required by FFI boundaries (SQLite C API). Every `unsafe` block must:
- Have a `// SAFETY:` comment explaining the invariant
- Be wrapped in a safe abstraction that cannot be misused
- Be reviewed independently

### 8.2 What This Means Concretely

**No `unsafe` for performance shortcuts**:
```rust
// WRONG: bypassing borrow checker for "performance"
unsafe { &*(ptr as *const RowChange) }

// RIGHT: use Arc for shared ownership
let change: Arc<RowChange> = Arc::clone(&shared_change);
```

**No `unsafe` for interior mutability tricks**:
```rust
// WRONG: UnsafeCell for "faster" access
unsafe { &mut *self.state.get() }

// RIGHT: use std synchronization primitives
let state = self.state.lock().await;
```

**No `unsafe` for FFI unless wrapped**:
```rust
// The only acceptable unsafe: wrapping C library calls
// rusqlite already does this -- we use its safe API exclusively
let conn = Connection::open(path)?;  // safe rusqlite API
let mut stmt = conn.prepare("SELECT ...")?;  // safe
let rows = stmt.query_map(params, |row| { ... })?;  // safe
```

### 8.3 Enforced via CI

```toml
# Cargo.toml
[lints.rust]
unsafe_code = "deny"  # Compile error on any unsafe block

# Exception: only in the sqlite-pool crate that wraps rusqlite
# and only with explicit #[allow(unsafe_code)] + SAFETY comment
```

```yaml
# CI check
- name: Audit unsafe usage
  run: |
    cargo install cargo-geiger
    cargo geiger --all-features --forbid-only
```

### 8.4 Concurrency Without Unsafe

Rust's type system enforces thread safety at compile time:

```rust
// Send: can be transferred between threads
// Sync: can be referenced from multiple threads simultaneously
// The compiler checks these automatically

// Arc<T> is Send + Sync when T is Send + Sync
// Mutex<T> is Send + Sync when T is Send
// RwLock<T> is Send + Sync when T is Send + Sync

// This means: if your data is safe to send across threads,
// the compiler proves it. If it's not, it won't compile.
// No data races possible in safe Rust.
```

**No need for `SharedArrayBuffer` + `Atomics` (Node.js's escape hatch)** -- Rust's type system handles this natively and correctly.

---

## 9. Performance Test Suite and Regression Testing

### 9.1 Benchmark Architecture

The test suite runs the **exact same workloads** against both the Node.js (current) and Rust implementations, using the same external systems (PostgreSQL, same schema, same data).

```
                    ┌──────────────────┐
                    │  Test Harness    │
                    │  (Node.js/Rust)  │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼──────┐ ┌────▼────┐ ┌───────▼───────┐
     │ Node.js impl  │ │  Same   │ │  Rust impl    │
     │ (zero-cache)  │ │ PG DB   │ │ (zero-cache-  │
     │               │ │ Same    │ │  rs)           │
     │ Current code  │ │ Schema  │ │               │
     │ as-is         │ │ Same    │ │ New impl      │
     └───────────────┘ │ Data    │ └───────────────┘
                       └─────────┘
```

**Nothing external changes** -- same PostgreSQL, same schema, same seed data, same client queries.

### 9.2 Benchmark Workloads

Each benchmark runs against both implementations and records latency percentiles, throughput, and resource usage.

#### Benchmark 1: Single CG Hydration
```
Setup: 1 client group, N queries of varying complexity
Measure: Time to hydrate all queries from cold start
Metrics: p50/p95/p99 latency, total rows hydrated, memory peak
```

#### Benchmark 2: Single CG Advance (Steady State)
```
Setup: 1 CG with hydrated queries, apply M changes to PG
Measure: Time from change commit to poke delivery
Metrics: advance latency breakdown (snapshot, IVM, CVR flush, poke)
```

#### Benchmark 3: Multi-CG Parallel Advance
```
Setup: K client groups (10, 50, 100, 500) on same replica
Measure: Throughput (advances/sec), tail latency under contention
Metrics: p50/p95/p99 advance latency, cross-CG fairness (jitter)
```

#### Benchmark 4: CG Migration / Rebalancing
```
Setup: Unbalanced load -- 80% of work on 20% of workers
Measure: Time to rebalance
Node.js: Not possible (sticky assignment, would require re-hydration)
Rust: Arc move + work-stealing
```

#### Benchmark 5: Connection Pool Pressure
```
Setup: All CGs flush CVR simultaneously (burst)
Measure: Connection wait time, flush latency under contention
Node.js: Per-syncer pools, each with maxConns/numSyncers
Rust: Single global pool with maxConns
```

#### Benchmark 6: Memory Under Load
```
Setup: Ramp CG count from 10 to 1000 over 10 minutes
Measure: RSS, heap size, GC pauses (Node.js), allocation rate
Metrics: Memory per CG, memory growth rate, GC pause frequency
```

#### Benchmark 7: Sustained Throughput
```
Setup: Continuous PG writes at X changes/sec for 30 minutes
Measure: Advance latency stability over time
Metrics: Latency percentiles per minute, memory growth, GC storms
```

### 9.3 Test Environment Setup

The test environment uses the **existing infrastructure** without modification:

```bash
# Use the existing zbugs PostgreSQL setup
cd apps/zbugs
npm run db-up          # Start PostgreSQL (Docker)
npm run db-migrate     # Apply schema
npm run db-seed        # Seed test data

# Node.js baseline (current code, no changes)
ZERO_NUM_POOL_THREADS=4 \
ZERO_NUM_SYNC_WORKERS=4 \
npm run zero-cache-dev &

# Run benchmark harness against Node.js
node benchmarks/run-benchmarks.js --target=node --output=results/node.json

# Rust implementation (same PG, same schema, same data)
cargo run --release -p zero-cache-rs -- \
  --upstream-db="$ZERO_UPSTREAM_DB" \
  --cvr-db="$ZERO_CVR_DB" \
  --num-workers=4 &

# Run same benchmark harness against Rust
node benchmarks/run-benchmarks.js --target=rust --output=results/rust.json

# Compare
node benchmarks/compare.js results/node.json results/rust.json
```

### 9.4 Regression Test Framework

```
benchmarks/
├── run-benchmarks.js          # Harness: drives both implementations
├── compare.js                 # Diff tool: node vs rust results
├── workloads/
│   ├── hydration.js           # Single CG hydration workload
│   ├── advance.js             # Steady-state advance workload
│   ├── multi-cg.js            # Parallel CG workload
│   ├── connection-pressure.js # Connection pool burst
│   └── memory-ramp.js         # Memory scaling workload
├── results/
│   ├── node.json              # Node.js baseline results
│   ├── rust.json              # Rust results
│   └── history/               # Historical results for regression detection
└── ci/
    └── regression-check.sh    # CI gate: fail if Rust regresses vs baseline
```

### 9.5 CI Regression Gate

```yaml
# Run on every PR to the Rust implementation
regression-test:
  steps:
    - name: Run Node.js baseline
      run: node benchmarks/run-benchmarks.js --target=node --output=baseline.json

    - name: Run Rust candidate
      run: |
        cargo build --release -p zero-cache-rs
        node benchmarks/run-benchmarks.js --target=rust --output=candidate.json

    - name: Check for regressions
      run: |
        node benchmarks/compare.js baseline.json candidate.json \
          --fail-if-slower=10%   # Fail PR if any metric regresses >10%
          --fail-if-more-memory=20%  # Fail if RSS grows >20%
```

### 9.6 Metrics Collected

Each benchmark records:

```json
{
  "benchmark": "multi_cg_advance",
  "implementation": "rust",
  "timestamp": "2026-04-10T...",
  "config": {
    "num_cgs": 100,
    "num_workers": 4,
    "pg_max_conns": 50
  },
  "latency": {
    "advance_p50_ms": 12.3,
    "advance_p95_ms": 45.2,
    "advance_p99_ms": 89.1,
    "hydration_p50_ms": 5.1,
    "cvr_flush_p50_ms": 28.4,
    "poke_p50_ms": 0.8
  },
  "throughput": {
    "advances_per_sec": 842,
    "rows_per_sec": 16840
  },
  "resources": {
    "rss_peak_mb": 512,
    "rss_steady_mb": 340,
    "gc_pauses_total_ms": 0,
    "cpu_user_percent": 65,
    "pg_conns_peak": 38,
    "sqlite_conns_peak": 12
  }
}
```

---

## 10. Library Selection: Built for Performance

### 10.1 Selection Criteria

Every crate was chosen based on:
1. **Benchmark results** -- measured throughput and latency, not marketing claims
2. **Zero-copy where possible** -- avoid allocation on hot paths
3. **Async-native** -- no blocking the runtime
4. **Production-proven** -- used at scale by known projects (Cloudflare, Discord, Figma, etc.)
5. **Actively maintained** -- recent commits, responsive maintainers

### 10.2 Async Runtime: `tokio`

**Why tokio over alternatives** (`async-std`, `smol`, `glommio`):
- Work-stealing scheduler -- idle threads pick up work from busy threads
- `spawn_blocking` for CPU-bound SQLite work -- doesn't block async tasks
- `io_uring` support via `tokio-uring` for zero-copy I/O on Linux
- Used by: Cloudflare, Discord, Linkerd, Deno, AWS SDKs

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
```

### 10.3 SQLite: `rusqlite` + Separate Read/Write Pools (Research-Backed)

**Why `rusqlite`** (not `diesel`, `sqlx`, `sqlite` crate):
- Thin wrapper over SQLite C API -- minimal overhead
- Supports `SQLITE_OPEN_WAL` for concurrent reads
- Supports `BEGIN CONCURRENT` (required for snapshotter leapfrog pattern)
- Compile SQLite from source with performance flags:

```toml
[dependencies]
rusqlite = { version = "0.32", features = [
  "bundled",        # Compile SQLite from source
  "column_decltype",
  "unlock_notify",  # Required for concurrent access
] }
```

**SQLite compile-time flags** (via `SQLITE_EXTRA_INIT`):
```
-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1
-DSQLITE_DEFAULT_MEMSTATUS=0      # Disable memory tracking (faster)
-DSQLITE_DEFAULT_CACHE_SIZE=-8000 # 8MB page cache per connection
-DSQLITE_THREADSAFE=2             # Multi-threaded mode
-DSQLITE_LIKE_DOESNT_MATCH_BLOBS=1
-DSQLITE_OMIT_DEPRECATED=1
-DSQLITE_OMIT_PROGRESS_CALLBACK=1
-DSQLITE_USE_ALLOCA=1
```

**Critical research finding: Separate read/write connection pools**

Research from Evan Schwartz's benchmarks shows that **separating SQLite into a single-writer pool and a multi-reader pool yields 20x write performance improvement**:

| Configuration | Total Time | Rows/sec | P50 | P99 |
|---|---|---|---|---|
| Single pool (50 connections) | 1.93s | 2,586 | 474ms | 182s |
| Single writer + reader pool | 83ms | 60,061 | 43ms | 82ms |

This mirrors SQLite's inherent architecture: unlimited concurrent readers, exactly one writer. Our current Node.js code cannot take advantage of this because connections are thread-bound.

```rust
// Architecture: TWO pools, not one
struct SqlitePools {
    /// Single connection for all writes (replicator)
    writer: Arc<Mutex<rusqlite::Connection>>,
    /// Multiple connections for concurrent reads (IVM snapshots)
    readers: deadpool_sqlite::Pool,
}

impl SqlitePools {
    fn new(path: &str) -> Result<Self> {
        // Writer: single connection, WAL mode, exclusive
        let writer = rusqlite::Connection::open(path)?;
        writer.pragma_update(None, "journal_mode", "WAL")?;
        writer.pragma_update(None, "synchronous", "NORMAL")?;

        // Readers: pool of N connections, WAL mode, read-only
        let reader_pool = Config::new(path)
            .builder(Runtime::Tokio1)?
            .max_size(num_cpus::get() as u32)  // Scale to cores
            .post_create(|conn| {
                conn.interact(|c| {
                    c.pragma_update(None, "journal_mode", "WAL")?;
                    c.pragma_update(None, "query_only", "ON")?;
                    Ok(())
                })
            })
            .build()?;

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers: reader_pool,
        })
    }
}
```

**Dedicated SQLite thread** (research-backed pattern):
Instead of `spawn_blocking` for every SQLite query, a dedicated thread with a channel
saves memory and reduces contention. This is the recommended production pattern from
the Rust community for SQLite-heavy workloads:

```rust
// Dedicated writer thread
let (tx, rx) = crossbeam_channel::bounded::<SqliteTask>(1024);
std::thread::spawn(move || {
    let conn = rusqlite::Connection::open(path).unwrap();
    for task in rx {
        task.execute(&conn);
    }
});
```

### 10.4 PostgreSQL: `tokio-postgres` + `deadpool-postgres`

**Why `tokio-postgres`** (not `diesel`, `sqlx`):
- Zero-copy row parsing -- reads directly from the network buffer
- Pipelining -- send multiple queries without waiting for responses
- Binary protocol -- no text encoding/decoding overhead
- Connection pooling via `deadpool-postgres` (async-native, not `r2d2`)

```toml
[dependencies]
tokio-postgres = { version = "0.7", features = ["with-serde_json-1"] }
deadpool-postgres = "0.14"
```

```rust
// Single pool shared across all tasks
let pg_pool = deadpool_postgres::Config {
    host: Some(host),
    dbname: Some(dbname),
    pool: Some(deadpool_postgres::PoolConfig {
        max_size: 50,  // All connections available to all tasks
        ..Default::default()
    }),
    ..Default::default()
}
.create_pool(Some(Runtime::Tokio1), NoTls)?;

// Statement pipelining (equivalent to current TransactionPool batching)
let conn = pg_pool.get().await?;
let pipeline = conn.pipeline();
for stmt in statements {
    pipeline.execute(&stmt, &params).await?;
}
pipeline.sync().await?;
```

### 10.5 WebSocket: `axum` + `tokio-tungstenite`

**Why `axum`** (not `actix-web`, `warp`, `rocket`):
- Built on `tokio` and `hyper` -- same runtime, no adaptation layer
- Extract-based handler model -- zero-cost abstractions for request parsing
- WebSocket upgrade built-in via `axum::extract::WebSocketUpgrade`
- Used by: Cloudflare Workers, Shuttle.rs

```toml
[dependencies]
axum = { version = "0.8", features = ["ws"] }
tokio-tungstenite = "0.24"
```

### 10.6 Serialization: `serde` + `rkyv` for Hot Paths

**`serde` + `serde_json`** for protocol compatibility (client-facing JSON):
```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

**`rkyv`** for internal zero-copy serialization (if disk spill needed):
- Zero-copy deserialization -- read directly from mmap'd file
- 10-100x faster than serde_json for structured data
- Used for operator state spill to disk

```toml
rkyv = { version = "0.8", features = ["validation"] }
```

### 10.7 Data Structures: `im` + `crossbeam`

**`im`** -- persistent (immutable) data structures for IVM operator state:
- Structural sharing -- "cloning" a map shares unchanged subtrees
- O(log n) insert/lookup (vs BTreeMap's O(log n) but with sharing)
- Perfect for snapshot-based IVM where you want cheap "fork" of state

```toml
im = "15"  # Persistent HashMap and Vector
```

**`crossbeam`** -- lock-free concurrent data structures:
- `crossbeam-channel` -- bounded/unbounded MPMC channels (faster than `std::sync::mpsc`)
- `crossbeam-skiplist` -- lock-free skip list for concurrent operator storage
- `crossbeam-epoch` -- epoch-based memory reclamation (no GC needed)

```toml
crossbeam = "0.8"
```

### 10.8 Observability: `tracing` + `opentelemetry`

**`tracing`** (not `log`, `slog`):
- Structured, span-based logging -- natural fit for per-advance diagnostics
- Zero-cost when disabled (`tracing` compiles out disabled levels)
- `tracing-opentelemetry` bridges to OTel for Grafana/Datadog

```toml
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
tracing-opentelemetry = "0.27"
opentelemetry = "0.27"
opentelemetry-otlp = "0.27"
```

### 10.9 Memory Allocator: `mimalloc` (Research-Corrected)

**Why `mimalloc`** (not `jemalloc`, not system allocator):

Research update: jemalloc appears to be largely unmaintained as of 2025. Recent benchmarks (2026) show:
- `mimalloc` delivers **5.3x faster average performance** vs glibc malloc under heavy multithreaded workloads on Linux
- `mimalloc` cuts **RSS memory usage by ~50%** vs glibc
- For **small, frequent allocations** (our primary pattern -- RowChange objects, batch buffers): mimalloc leads with **15% lower P99 latency** than jemalloc
- For long-running servers with stable memory patterns, differences narrow to ~3%
- `mimalloc` has had significant work on reducing virtual memory fragmentation for long-running services

Our workload is a mix of small frequent allocations (RowChange, batch buffers) and long-lived state (operator graphs, CG state). `mimalloc` is the better fit.

```toml
[dependencies]
mimalloc = { version = "0.1", default-features = false }
```

```rust
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

**Benchmark both**: include allocator comparison in the regression test suite. Switch is a one-line change.

### 10.10 Complete Dependency Summary

| Component | Crate | Why This One |
|-----------|-------|-------------|
| Async runtime | `tokio` | Work-stealing, spawn_blocking, io_uring |
| SQLite | `rusqlite` + `deadpool-sqlite` | Thin C wrapper, concurrent reads, async pool |
| PostgreSQL | `tokio-postgres` + `deadpool-postgres` | Zero-copy parsing, pipelining, async pool |
| WebSocket | `axum` + `tokio-tungstenite` | Built on tokio/hyper, zero-cost extracts |
| Channels | `crossbeam-channel` | MPMC, bounded, faster than std |
| JSON | `serde_json` | Protocol compatibility |
| Internal serialization | `rkyv` | Zero-copy disk spill |
| IVM data structures | `im` (persistent) + `crossbeam-skiplist` | Structural sharing, lock-free |
| Allocator | `mimalloc` | 5.3x faster than glibc, 15% lower P99 vs jemalloc for small allocs |
| Logging | `tracing` + `tracing-opentelemetry` | Structured spans, zero-cost disabled |
| Error handling | `anyhow` + `thiserror` | Ergonomic, zero-cost |
| CLI/config | `clap` | Derive-based, env var support |
| Testing | `criterion` | Statistical benchmarking |
| Fuzzing | `cargo-fuzz` + `arbitrary` | Input generation for IVM operators |

---

## 11. One-to-One Code Mapping Strategy

### 11.1 Principle

The Rust implementation must mirror the existing TypeScript structure as closely as possible. This is not a redesign -- it is a **re-implementation in a better runtime**. The architecture, module boundaries, data flow, and naming should map 1:1 so that:

1. Engineers familiar with the Node.js code can read the Rust code immediately
2. Bug fixes and features can be cherry-picked between implementations during the transition
3. Behavioral parity is verifiable by inspection, not just testing
4. The risk of introducing new bugs through "clever" restructuring is eliminated

### 11.2 File-to-File Mapping

| TypeScript File | Rust File | Notes |
|----------------|-----------|-------|
| `server/main.ts` | `src/server/main.rs` | Single process, spawns tokio tasks instead of fork() |
| `server/syncer.ts` | `src/server/syncer.rs` | Async task, shared PG pool instead of per-process |
| `server/pool-thread.ts` | `src/server/pool_worker.rs` | spawn_blocking task, shared state via Arc |
| `services/view-syncer/pipeline-driver.ts` | `src/services/view_syncer/pipeline_driver.rs` | Same struct, same methods, same generator pattern (Iterator) |
| `services/view-syncer/snapshotter.ts` | `src/services/view_syncer/snapshotter.rs` | Same leapfrog pattern, connections from shared pool |
| `services/view-syncer/pool-thread-manager.ts` | `src/services/view_syncer/worker_pool.rs` | Simplified: no MessageChannel, just task handles |
| `services/view-syncer/pool-thread-mapper.ts` | `src/services/view_syncer/cg_router.rs` | Simplified or eliminated: no sticky assignment needed |
| `services/view-syncer/remote-pipeline-driver.ts` | Not needed | No IPC proxy -- direct shared-state access |
| `services/view-syncer/view-syncer.ts` | `src/services/view_syncer/view_syncer.rs` | Same state machine, async instead of event loop |
| `workers/pool-protocol.ts` | Not needed | No serialization protocol -- types used directly |
| `types/processes.ts` | Not needed | No inter-process messaging |
| `db/transaction-pool.ts` | `src/db/transaction_pool.rs` | Same pool concept, backed by deadpool-postgres |
| `services/change-streamer/storer.ts` | `src/services/change_streamer/storer.rs` | Same logic, no heap-probing backpressure |
| `config/zero-config.ts` | `src/config/zero_config.rs` | Same config structure, clap derive |

### 11.3 Type Mapping

```
TypeScript                          Rust
──────────────────────────────────  ──────────────────────────────────
interface RowChange {               #[derive(Debug, Clone)]
  type: 'add' | 'remove' | 'edit'; pub struct RowChange {
  queryID: string;                      pub change_type: ChangeType,
  table: string;                        pub query_id: String,
  rowKey: RowKey;                       pub table: String,
  row: Row;                             pub row_key: RowKey,
}                                       pub row: Row,
                                    }

type DriverState = {                #[derive(Debug, Clone)]
  version: string;                  pub struct DriverState {
  replicaVersion: string;               pub version: String,
  permissions: Permissions | null;       pub replica_version: String,
  totalHydrationTimeMs: number;          pub permissions: Option<Permissions>,
  queries: Record<string, string>;       pub total_hydration_time_ms: f64,
};                                       pub queries: HashMap<String, String>,
                                    }

type ClientGroupState = {           pub struct ClientGroupState {
  driver: PipelineDriver;               pub driver: PipelineDriver,
  clientSchema: ClientSchema;            pub client_schema: ClientSchema,
  queryCount: number;                    pub query_count: usize,
  generation: number;                    pub generation: u64,
};                                  }

Map<string, ClientGroupState>       HashMap<String, ClientGroupState>
                                    // or Arc<DashMap<String, CgState>>
                                    // for concurrent access
```

### 11.4 Control Flow Mapping

**Generator → Iterator**:
```typescript
// TypeScript: generator for advance changes
*advance(timer: Timer): Generator<RowChange | 'yield'> {
  for (const change of this.computeChanges()) {
    yield change;
    if (timer.shouldYield()) yield 'yield';
  }
}
```
```rust
// Rust: Iterator for advance changes
impl Iterator for AdvanceIterator {
    type Item = RowChange;  // No 'yield' marker needed -- no event loop to yield to

    fn next(&mut self) -> Option<Self::Item> {
        self.compute_next_change()
    }
}
```

**Async generator → Stream**:
```typescript
// TypeScript: async iterable for streaming results
async *streamChanges(): AsyncGenerator<RowChange[]> {
  for await (const batch of this.batches) {
    yield batch;
  }
}
```
```rust
// Rust: Stream for streaming results
fn stream_changes(&self) -> impl Stream<Item = Vec<RowChange>> {
    async_stream::stream! {
        while let Some(batch) = self.batches.recv().await {
            yield batch;
        }
    }
}
```

**Lock → tokio::sync::Mutex**:
```typescript
// TypeScript: per-CG lock held through advance + CVR flush + poke
await this.#lock.withLock(async () => {
  const changes = await this.advancePipelines();
  await this.flushCVR(changes);
  await this.deliverPoke(version);
});
```
```rust
// Rust: same pattern, but can be decomposed for fine-grained locking
let _guard = self.lock.lock().await;
let changes = self.advance_pipelines().await?;
self.flush_cvr(&changes).await?;
self.deliver_poke(version).await?;
drop(_guard);  // Explicit release (or let scope handle it)
```

### 11.5 What Gets Simplified (Not Restructured)

These components exist in Node.js only because of runtime limitations. They disappear in Rust, but the surrounding code remains structurally identical:

| Removed Component | Why It Existed | What Replaces It |
|-------------------|---------------|-----------------|
| `PoolThreadManager` | Spawn worker_threads with MessageChannels | `tokio::task::spawn` (one line) |
| `PoolThreadMapper` (sticky) | SQLite thread affinity | Not needed -- shared connection pool |
| `RemotePipelineDriver` | Proxy for cross-thread IPC | Direct `PipelineDriver` access via `Arc` |
| `pool-protocol.ts` | Serialization format for MessagePort | Not needed -- types used directly |
| `processes.ts` fork/IPC | Cross-process communication | Not needed -- shared memory |
| Heap backpressure (`storer.ts:144`) | GC unpredictability workaround | Size-based backpressure on data structures |
| `structuredClone` serialization | Cross-isolate data transfer | `Arc::clone` (pointer copy) |

### 11.6 What Stays Identical

These components translate 1:1 with minimal changes:

- `PipelineDriver` -- same state machine, same methods, same IVM logic
- `Snapshotter` -- same leapfrog pattern, same `BEGIN CONCURRENT` usage
- `ViewSyncer` -- same run loop, same lock semantics, same CVR flush
- `TransactionPool` -- same pool concept, same task queuing
- `ChangeStreamer` / `Storer` -- same replication protocol
- Config structure -- same environment variables, same defaults
- Log format -- same structured fields for Grafana compatibility

---

## 12. Production Case Studies (Internet Research)

### 12.1 Discord: Read States Service (Go → Rust)

**Source**: [Why Discord is switching from Go to Rust](https://discord.com/blog/why-discord-is-switching-from-go-to-rust)

**Service**: Tracks which channels/messages each user has read. Accessed on every connect, every message send, every message read.

**Scale**: Millions of users per cache instance. Hundreds of thousands of cache updates/sec. Tens of thousands of DB writes/sec.

**Problem**: Go's GC caused latency spikes every 2 minutes (10-40ms). The service had a massive LRU cache of live objects with very few dead objects -- pathological for a tracing GC that scans live objects.

**Result**: Rust eliminated GC pauses entirely. Average latency dropped to microseconds. Max @mention latency dropped to milliseconds. **Every single performance metric improved: latency, CPU, and memory.**

**Key insight**: Rust's `Drop` trait immediately frees memory when objects leave the LRU cache. No scanning, no pauses.

**Relevance to Zero**: Our IVM operator state has the same profile -- large maps of live objects (hydrated rows), few dead objects, GC scanning causes unpredictable pauses. Discord's result validates our GC concern directly.

### 12.2 Figma: Multiplayer Server (TypeScript → Rust Hybrid)

**Source**: [Rust in Production at Figma](https://madebyevan.com/figma/rust-in-production-at-figma/)

**Architecture**: Kept Node.js for networking. Spawned **Rust child process per document** communicating via stdin/stdout message protocol.

**Result**: Serialization time **10x faster**. Memory usage so low they could afford one process per document (full parallelism).

**Why hybrid**: Figma found Rust's async/futures API too complex for network handling at the time. Kept Node.js for WebSocket, used Rust for CPU-heavy document operations.

**Relevance to Zero**: This is **exactly our Phase 1 strategy** -- keep Node.js syncer for WebSocket/CVR, use Rust for IVM computation. Figma validated this hybrid approach at massive scale.

### 12.3 Cloudflare: FL Proxy (LuaJIT/C/Rust → Pure Rust)

**Source**: [Cloudflare Rust Rewrite](https://www.infoq.com/news/2025/10/cloudflare-rust-proxy/)

**Result**: **50% CPU reduction**, **50%+ memory reduction**, **10ms latency improvement**, **25% throughput boost**.

**Key insight**: The performance gain came from eliminating polyglot overhead -- constant data representation conversions between C, Rust, and Lua. Consolidating to one language removed the translation tax.

**Migration strategy**: Adapter layer allowed incremental feature porting with fallback to old implementation during gradual traffic shift.

**Relevance to Zero**: Validates that eliminating cross-boundary serialization (our structured clone tax) yields measurable gains even when serialization cost seems "small per call."

### 12.4 OpenAI: Codex CLI (Node.js/TypeScript → Rust)

**Source**: [OpenAI rewrites Codex CLI in Rust](https://devclass.com/2025/06/02/node-js-frustrating-and-inefficient-openai-rewrites-ai-coding-tool-in-rust/)

**Motivations**: Zero-dependency install, improved sandboxing, no GC, lower memory.

**Benchmarks**: 2-3x higher throughput, up to **90% memory reduction** in some cases.

**Status**: In production as of 2026, ongoing Rust pipeline improvements.

### 12.5 SQLite: Separate Read/Write Pools (20x Improvement)

**Source**: [Your SQLite Connection Pool Might Be Ruining Write Performance](https://emschwartz.me/psa-your-sqlite-connection-pool-might-be-ruining-your-write-performance/)

| Configuration | Rows/sec | P50 | P99 |
|---|---|---|---|
| Single pool (50 connections) | 2,586 | 474ms | 182s |
| Single writer + reader pool | 60,061 | 43ms | 82ms |

**20x improvement** from separating into single-writer and multi-reader pools. This mirrors SQLite's inherent design.

**Relevance to Zero**: Our Replicator is the sole writer. All ViewSyncers are readers. Current Node.js architecture can't exploit this because connections are thread-bound. Rust can.

---

## 13. Client-Side WASM Analysis: Should the Client Be Rust Too?

### 13.1 Rocicorp Already Tried This

**Critical finding**: Rocicorp built `repc` -- the canonical Replicache client in Rust, compiled to WASM. It was **archived in September 2021** and replaced with pure JavaScript. The README states: *"Replicache has moved to pure JS, this repo is now archived."*

The current client (`packages/replicache/`) uses IndexedDB directly from JavaScript.

### 13.2 Why WASM Is Problematic for the Client

**The WASM Boundary Tax**:
- WASM cannot directly access browser storage APIs (IndexedDB, OPFS)
- All data must cross the JS↔WASM boundary via serialization
- For simple operations, WASM + wasm-bindgen can be **3x slower** than plain JavaScript
- String/object marshalling between JS heap and WASM linear memory is expensive

**Storage access overhead**:
- IndexedDB: ~10ms per operation from JS, ~3ms+ per operation via WASM→JS→IDB round-trip
- OPFS: ~1.5ms per operation, but requires Web Worker (no main thread access)
- WASM adds 40-50KB minimum binary overhead

**Browser concurrency limitations**:
- OPFS sync access only available in Web Workers
- No existing WASM SQLite implementation supports concurrent read+write transactions in browser
- Chrome incognito: 100MB database limit
- Safari incognito: no OPFS support

### 13.3 Where WASM Wins (and Where It Doesn't)

| Operation | JS vs WASM | Winner |
|-----------|-----------|--------|
| IDB read/write | JS direct vs WASM→JS→IDB | **JS** (no boundary crossing) |
| JSON parse/stringify | V8 native vs WASM serde | **JS** (V8 native JSON is heavily optimized) |
| Compute-heavy IVM (joins, filters) | JS vs WASM with SIMD | **WASM** (3-10x faster for pure compute) |
| DOM interaction | JS native vs WASM→JS bridge | **JS** (no bridge overhead) |
| Memory management | V8 GC vs WASM linear memory | **Depends** (WASM has no GC pauses but boundary copies add up) |

### 13.4 Recommendation: Keep Client in TypeScript/JavaScript

**For the client, JS is the right choice.** The client workload is:
1. **I/O bound** -- IndexedDB reads/writes dominate
2. **Small data per operation** -- individual mutations and subscription updates
3. **V8 optimized** -- JSON parsing, object creation, IDB access are V8's strongest operations
4. **Browser API integration** -- WebSocket, Service Workers, DOM updates

The server is the opposite:
1. **CPU bound** -- IVM computation (joins, filters, sorts over large datasets)
2. **Large batch operations** -- thousands of rows per advance
3. **Connection management** -- 100s of PG/SQLite connections
4. **No browser API dependency**

**The server-side rewrite gives the biggest ROI.** Client-side WASM would add complexity for marginal or negative performance gains. Rocicorp already validated this conclusion empirically by moving from Rust/WASM back to pure JS.

### 13.5 Future Exception: OPFS + SQLite in Browser

If the client storage model changes from IndexedDB to OPFS-backed SQLite in the browser (which the ecosystem is moving toward), a Rust WASM SQLite implementation could become viable because:
- SQLite operations are compute-heavy (query planning, B-tree traversal)
- OPFS provides synchronous file access in Workers
- The boundary crossing cost is amortized over larger operations

Monitor `wa-sqlite`, `sqlite-wasm-rs`, and browser OPFS support. Revisit this decision if the client moves to SQLite-based storage.

---

## 14. Production Phasing: Smaller Units for Faster Delivery

### 14.1 Principle

Each phase is an **independently deployable unit** that runs in production alongside the existing Node.js code. No big-bang cutover. Each phase must:
1. Be testable against the existing JS implementation (same benchmark harness)
2. Have a rollback path (flag off = back to Node.js)
3. Deliver measurable improvement independently
4. Not require changes to external systems (PostgreSQL, client protocol)

### Phase 0: Benchmark Harness (Week 1-2)

**Deliverable**: `benchmarks/` directory with workload definitions and comparison tooling.

```
benchmarks/
├── run-benchmarks.js          # Drives workloads against any endpoint
├── compare.js                 # Diff node vs rust results
├── workloads/
│   ├── hydration.js           # Single CG hydration
│   ├── advance.js             # Steady-state advance
│   ├── multi-cg.js            # Parallel CG contention
│   └── memory-ramp.js         # Memory scaling
└── ci/
    └── regression-check.sh    # CI gate
```

**Why first**: Cannot validate any subsequent phase without measurement. Establish Node.js baseline numbers before writing any Rust.

**Production impact**: None. Runs locally or in CI.

---

### Phase 1: Rust IVM Library via napi-rs (Week 3-8)

**Deliverable**: A Rust native addon (`zero-ivm-rs`) that replaces `pool-thread.ts` as the IVM compute engine.

```
packages/zero-ivm-rs/
├── Cargo.toml
├── src/
│   ├── lib.rs                  # napi-rs entry point
│   ├── pipeline_driver.rs      # 1:1 port of PipelineDriver
│   ├── snapshotter.rs          # 1:1 port of Snapshotter
│   ├── database_storage.rs     # Operator storage (rusqlite)
│   └── types.rs                # RowChange, DriverState, etc.
└── build.rs                    # SQLite compile flags
```

**Architecture** (same as Figma's proven hybrid approach):

```
Node.js Syncer (unchanged)
  │
  ├── WebSocket handling (unchanged)
  ├── CVR PG writes (unchanged)
  ├── Poke delivery (unchanged)
  │
  └── Instead of pool-thread.ts Worker:
      │
      ▼
      zero-ivm-rs native addon (via napi-rs)
        ├── PipelineDriver (Rust, 1:1 port)
        ├── Snapshotter (Rust, rusqlite)
        ├── Separate read/write SQLite pools
        └── Returns RowChange[] to JS via Buffer (zero-copy)
```

**What changes**:
- `pool-thread.ts` → replaced by Rust addon calls
- `pool-protocol.ts` → not needed (direct function calls via napi-rs)
- `remote-pipeline-driver.ts` → simplified to call Rust addon directly
- SQLite connections → managed by Rust (rusqlite), shared across calls

**What does NOT change**:
- Syncer process model (still Node.js, still fork())
- WebSocket handling
- CVR flush (still tokio-postgres... no, still Node.js `postgres`)
- Poke delivery
- Client protocol
- Config structure, log format

**Key research finding**: napi-rs has overhead for simple operations (~3x slower than plain JS). But IVM advance is compute-heavy (SQLite queries, B-tree traversal, join computation), exactly where native addons shine. Avoid crossing the JS↔Rust boundary per-row -- batch operations.

**Rollback**: `ZERO_USE_RUST_IVM=0` flag falls back to TypeScript PipelineDriver.

**Measurable improvement**:
- No GC pauses during IVM (Rust has no GC)
- Concurrent SQLite reads via read/write pool separation (20x write throughput)
- No structured clone serialization (napi-rs Buffer instead of postMessage)
- Eliminate V8 isolate overhead per pool thread

---

### Phase 2: Rust Connection Pool Service (Week 9-12)

**Deliverable**: A Rust sidecar process that manages PostgreSQL connections, replacing per-syncer connection pools.

```
packages/zero-pg-pool-rs/
├── Cargo.toml
├── src/
│   ├── main.rs                 # Standalone service
│   ├── pool.rs                 # deadpool-postgres shared pool
│   ├── cvr_store.rs            # CVR flush logic (1:1 port)
│   └── transaction_pool.rs     # 1:1 port of TransactionPool
```

**Architecture**:
```
Node.js Syncers (multiple processes)
  │
  └── Instead of per-syncer PG pools:
      │
      ▼
      zero-pg-pool-rs (single process, shared connection pool)
        ├── deadpool-postgres: all connections available to all syncers
        ├── CVR flush via async task
        └── Unix domain socket or shared memory IPC
```

**Measurable improvement**:
- Global PG pool: no per-syncer connection starvation
- Async CVR flush: doesn't block Node.js event loop
- Connection utilization: 50 connections serve all syncers vs 50/15 = 3 per syncer

**Rollback**: Syncers fall back to per-process `pgClient()` pools.

---

### Phase 3: Rust Syncer (Week 13-20)

**Deliverable**: Full Rust syncer task that replaces `server/syncer.ts`.

```
packages/zero-syncer-rs/
├── Cargo.toml
├── src/
│   ├── main.rs                 # Entry point
│   ├── server/
│   │   ├── syncer.rs           # 1:1 port
│   │   └── worker.rs           # tokio tasks (no processes)
│   ├── services/
│   │   └── view_syncer/
│   │       ├── view_syncer.rs  # 1:1 port
│   │       ├── pipeline_driver.rs  # From Phase 1
│   │       ├── snapshotter.rs      # From Phase 1
│   │       └── client_handler.rs   # WebSocket poke delivery
│   ├── db/
│   │   ├── transaction_pool.rs     # From Phase 2
│   │   └── cvr_store.rs            # From Phase 2
│   └── config/
│       └── zero_config.rs      # Same env vars
```

**Key unlock**: Single process, shared memory. No fork(), no MessageChannel, no structured clone. All CGs accessible from any tokio task. Work-stealing scheduler.

**Measurable improvement**:
- Eliminate 78 V8 isolates → ~5GB memory savings
- No sticky CG assignment → dynamic load balancing
- Fine-grained locking → shorter CVR flush lock hold times
- Zero serialization overhead

**Rollback**: Route traffic between Node.js and Rust syncers via load balancer flag.

---

### Phase 4: Rust Replicator + Change Streamer (Week 21-26)

**Deliverable**: Rust implementation of the replicator and change streamer services.

**Note**: The replicator and change streamer are **already separate processes** from the syncers in the current architecture. They communicate via IPC (Notifier/Subscription pattern for version-ready signals, and HTTP/WS for change-streamer subscriptions). This separation makes them independently replaceable.

```
packages/zero-replicator-rs/
├── src/
│   ├── incremental_sync.rs     # 1:1 port of IncrementalSyncer
│   ├── change_streamer.rs      # 1:1 port of ChangeStreamerService
│   └── storer.rs               # 1:1 port (no heap backpressure needed)
```

**Measurable improvement**:
- Single-writer SQLite with dedicated thread (no V8 blocking)
- Size-based backpressure (not GC heap-probing)
- Lower memory for change buffering

---

### Phase 5: Single Binary (Week 27-30)

**Deliverable**: Merge all Rust components into a single `zero-cache` binary.

```
zero-cache-rs (single binary)
├── Syncer tasks (from Phase 3)
├── IVM workers (from Phase 1, now in-process)
├── PG pool (from Phase 2, now in-process)
├── Replicator (from Phase 4)
├── Change Streamer (from Phase 4)
└── Config, logging, metrics (same interfaces)
```

**Measurable improvement**:
- Single process deployment (1 binary, 1 container)
- All shared-memory benefits fully realized
- Simplest operational model

---

### 14.2 Phase Dependencies

```
Phase 0 (Benchmarks)
  │
  ▼
Phase 1 (Rust IVM via napi-rs)  ──► Production validation
  │
  ▼
Phase 2 (Rust PG Pool)  ──► Production validation
  │
  ├──────────────┐
  ▼              ▼
Phase 3       Phase 4
(Rust Syncer) (Rust Replicator)  ──► Both in production
  │              │
  └──────┬───────┘
         ▼
       Phase 5 (Single Binary)
```

Phases 3 and 4 can run in parallel since the replicator and syncer are already separate processes.

### 14.3 What Goes to Production at Each Phase

| Phase | Ships | Rollback | Proves |
|-------|-------|----------|--------|
| 0 | Benchmark harness | N/A | Node.js baseline numbers |
| 1 | Rust IVM addon | Flag: `ZERO_USE_RUST_IVM` | No GC pauses, concurrent SQLite reads, no serialization |
| 2 | Rust PG pool sidecar | Flag: per-syncer vs shared pool | Global connection sharing, no starvation |
| 3 | Rust syncer binary | Load balancer routing | Single process, shared memory, work-stealing |
| 4 | Rust replicator binary | Process selection flag | Lower memory, deterministic backpressure |
| 5 | Single binary | Container image tag | Operational simplicity |

---

## 15. Monitoring Compatibility (Same Dashboards, Same Alerts)

### 15.1 Replicator and Cache Server Separation

The current architecture has clear process separation:

- **Change Streamer** (`server/change-streamer.ts`): Subscribes to PG logical replication, stores changes in SQLite change-log, forwards to subscribers
- **Replicator** (`server/replicator.ts`): Subscribes to change-streamer, writes to SQLite replica file
- **Syncer** (`server/syncer.ts`): Subscribes to replicator version-ready signals, runs IVM, flushes CVR, delivers pokes

Communication:
- Change Streamer → Replicator: HTTP/WS subscription with `SubscriberContext`
- Replicator → Syncer: `Source<ReplicaState>` notifier (in-process or IPC)
- Syncer → Pool Thread: `MessageChannel` + `MessagePort`

Each phase of the Rust migration replaces one of these processes independently. The interfaces between them (replication protocol, version-ready signals) remain stable.

### 15.2 Existing Metrics (Must Be Preserved in Rust)

The Rust implementation must emit the **exact same metric names** so existing Grafana queries continue to work:

| Metric | Component | Source |
|--------|-----------|--------|
| `zero.replication.events` | Replicator | `incremental-sync.ts` |
| `zero.replication.tx` | Change Streamer | `change-streamer-service.ts` |
| `zero.sync.active-clients` | Syncer | `view-syncer.ts` |
| `zero.sync.hydration` | Syncer | `view-syncer.ts` |
| `zero.sync.hydration-time` | Syncer | `view-syncer.ts` |
| `zero.sync.advance-time` | Syncer | `view-syncer.ts` |
| `zero.sync.poke.time` | Syncer | `client-handler.ts` |
| `zero.sync.poke.rows` | Syncer | `client-handler.ts` |
| `zero.sync.ivm.advance-time` | IVM | `pipeline-driver.ts` |
| `zero.sync.cvr.flush-time` | CVR | `row-record-cache.ts` |
| `zero.sync.cvr.rows-flushed` | CVR | `row-record-cache.ts` |
| `zero.mutation.crud` | Mutagen | `mutagen.ts` |

### 15.3 Log Format Compatibility

Rust logs must produce the **same JSON structure** for Grafana log queries:

```json
{
  "timestamp": "2026-04-10T12:34:56.789+00:00",
  "pid": 12345,
  "worker": "syncer",
  "level": "info",
  "message": "advanced clientGroup=cg-123 to=v42 changes=15 rows=200 ...",
  "clientGroupID": "cg-123",
  "poolThreadIdx": "2"
}
```

Using `tracing` + `tracing-subscriber` with JSON formatting achieves this natively. The `tracing::info!()` macro with structured fields maps directly to the existing `lc.info?.()` pattern.

### 15.4 OTEL Compatibility

Current setup: `@opentelemetry/sdk-node` with OTLP HTTP/JSON export.

Rust equivalent: `opentelemetry-rust` + `opentelemetry-otlp` with the same exporter endpoint. Same meter names (`zero.sync.*`, `zero.replication.*`), same histogram aggregation (base2 exponential bucket).

The `tracing-opentelemetry` bridge converts `tracing` spans into OTEL spans automatically -- same span hierarchy, same attributes.

---

## 16. Honest Tradeoffs: Aaron Boodman's Counter-Argument

### 16.1 The Founder's Position

Aaron Boodman (Rocicorp CEO) has publicly stated why Zero is TypeScript, not Rust. This is not a technical objection — it's a strategic one:

> "It is very hard to be a completely first-class citizen in a software ecosystem while writing the core of the product in a different language."

His specific concerns:

1. **DX requires TS**: "We can only really create a high-quality TS DX *in* TS."
2. **Marshaling dominates**: "Marshaling costs back and forth between TS and wasm are very very large, quickly becoming dominant in something like Zero."
3. **Bundle size on the web**: WASM/Rust has no built-in runtime for arrays, maps, memory management.
4. **Same queries client AND server**: "If we build the server in a different language we might gain perf (maybe) but we add massive massive complexity."
5. **Future carve-outs**: "I still think we'll carve out bits of native code for Zero over time, but it will be done cautiously."

### 16.2 The ZQL Problem

This is the critical constraint the white paper must address honestly.

ZQL (`packages/zql/`) is a **shared codebase**. The same operator implementations (joins, filters, exists, orderBy) run on:
- **Client** (browser, via Replicache/zero-client) — for reactive queries
- **Server** (zero-cache, via PipelineDriver) — for IVM

A full Rust server means one of:
1. **Port ZQL to Rust** — two implementations of the same query engine, must produce identical results. Testing burden is enormous. Every ZQL bug fix must be applied to both. "Massive massive complexity."
2. **Keep ZQL in TS, call it from Rust** — the marshaling tax Aaron warns about. Every row crosses the Rust↔TS boundary.
3. **Accept divergence** — client and server ZQL may differ. Breaks the "same queries everywhere" promise.

### 16.3 Revised Phase Assessment

| Phase | Alignment with Aaron's Position | ZQL Impact | Recommendation |
|-------|--------------------------------|------------|----------------|
| 0 (Benchmarks) | Fully aligned | None | **Do this** |
| 1 (Rust IVM via napi-rs) | Partially aligned — "carve out bits of native code" | **Risk**: IVM operators ARE ZQL. Porting them means two implementations. | **Scope carefully** — only port SQLite access, connection pooling, and snapshot management to Rust. Keep ZQL operators in TS. |
| 2 (Rust PG pool) | Fully aligned — infrastructure, no ZQL | None | **Do this** |
| 3 (Rust syncer) | **Conflicts** — requires Rust ZQL or heavy marshaling | High | **Reconsider** — may not be worth the ZQL duplication cost |
| 4 (Rust replicator) | Mostly aligned — replicator doesn't run ZQL | Low | **Viable** — replicator is pure SQLite writes, no ZQL |
| 5 (Single binary) | Depends on Phase 3 | Depends | Only if Phase 3 is pursued |

### 16.4 The Revised Strategy: "Carve Out" Not "Rewrite"

Aligning with Aaron's "carve out bits of native code" framing, the highest-ROI approach is:

**What to move to Rust (infrastructure, no ZQL):**
- SQLite connection management (read/write pool separation → 20x write throughput)
- PostgreSQL connection pooling (global pool → no per-syncer starvation)
- Snapshot management (Snapshotter leapfrog pattern)
- Serialization/IPC elimination (direct memory access instead of structured clone)
- Memory allocator (mimalloc instead of V8 GC for hot-path allocations)

**What to keep in TypeScript (ZQL, DX):**
- ZQL operator graphs (joins, filters, exists, orderBy)
- PipelineDriver query logic
- ViewSyncer state machine
- Client protocol handling
- Schema definition and validation

**The boundary**: Rust manages connections, snapshots, and data movement. TypeScript owns query semantics. The napi-rs boundary sits between "give me rows from this SQLite snapshot" (Rust) and "run this IVM operator graph" (TypeScript).

### 16.5 What This Means

The original framing of "rewrite zero-cache in Rust" is too aggressive. The founder's own position — backed by the real experience of building and abandoning `repc` — says the right approach is surgical native carve-outs at bottleneck points, not a language migration.

The performance ceiling identified in this white paper is real (GC pauses, connection duplication, serialization tax, thread affinity). But the solution space is narrower than "port everything to Rust." It's "port the infrastructure that TypeScript can't do well, keep the semantics that TypeScript does best."

This is consistent with Figma's approach (Rust for document operations, Node.js for networking) and with Aaron's own roadmap ("carve out bits of native code over time").

---

## Appendix A: Interface Documentation (Complete)

### A.1 Component Boundaries

These are the stable interfaces between components. Each component can be replaced independently as long as the interface contract is preserved.

```
┌─────────────────────────────────────────────────────────────────┐
│                       External Systems                         │
│  ┌──────────┐    ┌────────────┐    ┌──────────────────────┐    │
│  │ Upstream  │    │  CVR       │    │  Browser Clients     │    │
│  │ PG       │    │  PG        │    │  (WebSocket)         │    │
│  └────┬─────┘    └─────┬──────┘    └──────────┬───────────┘    │
│       │                │                       │               │
└───────┼────────────────┼───────────────────────┼───────────────┘
        │                │                       │
┌───────┼────────────────┼───────────────────────┼───────────────┐
│       ▼                │                       │               │
│  ┌─────────────┐       │                       │               │
│  │ Change      │       │                       │               │
│  │ Streamer    │───Interface 1──┐              │               │
│  └─────────────┘                │              │               │
│                                 ▼              │               │
│                          ┌─────────────┐       │               │
│                          │ Replicator  │       │               │
│                          └──────┬──────┘       │               │
│                                 │              │               │
│                          Interface 2           │               │
│                                 │              │               │
│                                 ▼              ▼               │
│                          ┌─────────────────────────┐           │
│                          │       Syncer            │           │
│                          │  ┌───────────────────┐  │           │
│                          │  │   ViewSyncer(s)   │  │           │
│                          │  │  ┌─────────────┐  │  │           │
│                          │  │  │ Pipeline    │──┼──┼─ Interface 4 ─→ CVR PG
│                          │  │  │ Driver      │  │  │           │
│                          │  │  └─────────────┘  │  │           │
│                          │  └───────────────────┘  │           │
│                          │         │               │           │
│                          │    Interface 3          │           │
│                          │         │               │           │
│                          │         ▼               │           │
│                          │  ┌───────────────────┐  │           │
│                          │  │ Pool Thread (opt) │  │           │
│                          │  │ (or in-process)   │  │           │
│                          │  └───────────────────┘  │           │
│                          └─────────────────────────┘           │
│                                                    zero-cache  │
└────────────────────────────────────────────────────────────────┘
```

### A.2 Interface 1: Change Streamer → Replicator

**Protocol**: HTTP/WebSocket subscription

**Subscriber sends** (`SubscriberContext`):
```typescript
{
  protocolVersion: number;
  taskID: string;
  id: string;
  mode: 'backup' | 'serving';
  watermark: string;         // Current replication position
  replicaVersion: string;    // Version of initial snapshot
  initial: boolean;          // First sync after startup
}
```

**Streamer sends** (`Downstream` messages):
- Change entries (table, rowKey, op: SET/RESET/TRUNCATE, new row data)
- Commit boundaries (version, watermark)
- Status messages

**Contract**: Streamer delivers changes in commit order. Subscriber acks by advancing watermark.

### A.3 Interface 2: Replicator → Syncer

**Protocol**: In-process pub-sub via `Source<ReplicaState>` / `Subscription`

**Signal**: `ReplicaState` (version-ready notification)
- Not a data payload — just a wake-up signal
- Actual version read by Snapshotter from `_zero.replicationState` table
- **Coalesced**: at most one pending event per subscriber

**File**: `services/replicator/notifier.ts`, `types/subscription.ts`

### A.4 Interface 3: Syncer → Pool Thread

**Protocol**: `MessagePort` (one per pool thread), structured clone serialization

**Syncer → Pool messages** (`PoolWorkerMsg`):
```typescript
| { type: 'init';        requestId; clientGroupID; clientSchema }
| { type: 'addQuery';    requestId; clientGroupID; queryID; transformationHash; ast }
| { type: 'removeQuery'; requestId; clientGroupID; queryID }
| { type: 'advance';     requestId; clientGroupID }
| { type: 'getRow';      requestId; clientGroupID; table; rowKey }
| { type: 'destroy';     requestId; clientGroupID; generation }
| { type: 'shutdown';    requestId: 0 }
```

**Pool → Syncer messages** (`PoolWorkerResult`):
```typescript
// Init
| { type: 'initResult';  requestId; clientGroupID; state: DriverState; generation }

// Hydration (streaming)
| { type: 'addQueryBegin';    requestId; clientGroupID; queryID }
| { type: 'addQueryBatch';    requestId; clientGroupID; changes: RowChange[] }
| { type: 'addQueryComplete'; requestId; clientGroupID; queryID; hydrationTimeMs; totalRows; batchCount; state }

// Advance (streaming)
| { type: 'advanceBegin';       requestId; clientGroupID; version; numChanges; snapshotMs; poolToBeginMs; ... }
| { type: 'advanceChangeBatch'; requestId; clientGroupID; changes: RowChange[] }
| { type: 'advanceComplete';    requestId; clientGroupID; didReset; iterateMs; totalRows; state; poolToCompleteMs; batchCount }

// Other
| { type: 'removeQueryResult'; requestId; clientGroupID; queryID; state }
| { type: 'getRowResult';     requestId; clientGroupID; row: Row | undefined }
| { type: 'destroyResult';    requestId; clientGroupID }
| { type: 'error';            requestId; clientGroupID; message; name; stack; isResetSignal }
```

**File**: `workers/pool-protocol.ts`

### A.5 Interface 4: Syncer → CVR PostgreSQL

**Protocol**: PostgreSQL transactions via `TransactionPool`

**Operations**:
- `CVRStore.flush()`: Write committed view records
- `CVRStore.load()`: Read existing CVR state on reconnect
- `processReadTask()` / `processTask()`: Generic transaction execution

**Contract**: CVR flush must complete before pokeEnd is sent to client (correctness invariant).

### A.6 Interface 5: Syncer → Client (WebSocket)

**Protocol**: WebSocket, JSON messages defined in `packages/zero-protocol/`

**Client → Server**:
- `initConnection { baseCookie, clientGroupID, ... }`
- `changeDesiredQueries { desiredQueriesPatch: {queryID → AST | null} }`
- `updateAuth { jwt }`
- `deleteClients { clientIDs }`

**Server → Client**:
- `pokeStart { }`
- `pokeData { ... row patches, config patches ... }`
- `pokeEnd { cookie }`
- `error { ... }`

**Contract**: Client must not commit to a version until it receives pokeEnd with that version's cookie.

### A.7 Key Data Types Shared Across Interfaces

```typescript
// Row change emitted by IVM
type RowChange = {
  type: 'add' | 'remove' | 'edit';
  queryID: string;
  table: string;
  rowKey: RowKey;
  row: Row;
};

// Cached driver state (synced after every operation)
type DriverState = {
  version: string;
  replicaVersion: string;
  permissions: LoadedPermissions | null;
  totalHydrationTimeMs: number;
  queries: Record<string, string>;  // queryID → transformationHash
};

// Pool thread timing diagnostics
type PoolTimings = {
  postToBeginMs: number;
  postToCompleteMs: number;
  poolToBeginMs: number;
  poolToCompleteMs: number;
  gapSincePrevAdvanceMs: number;
  prevAdvanceCgID: string | undefined;
  prevAdvanceDurationMs: number | undefined;
  batchCount: number;
  poolThreadIdx: number;
};
```

---

## Appendix B: File References (by component)

| File | Relevance |
|------|-----------|
| `packages/zero-cache/src/server/main.ts` | Process hierarchy, fork() calls |
| `packages/zero-cache/src/server/syncer.ts:87-95` | Per-syncer PG connection pools |
| `packages/zero-cache/src/server/pool-thread.ts` | Pool thread entry point, IPC batching, SQLite usage |
| `packages/zero-cache/src/services/view-syncer/pool-thread-manager.ts` | Thread spawning, MessageChannel per thread |
| `packages/zero-cache/src/services/view-syncer/pool-thread-mapper.ts` | Sticky CG assignment, no rebalancing |
| `packages/zero-cache/src/services/view-syncer/remote-pipeline-driver.ts` | Syncer-side proxy, streaming advance |
| `packages/zero-cache/src/services/view-syncer/snapshotter.ts` | SQLite leapfrog snapshots, 2 connections per CG |
| `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts` | IVM computation, operator graphs |
| `packages/zero-cache/src/workers/pool-protocol.ts` | IPC protocol definitions |
| `packages/zero-cache/src/types/processes.ts:183-192` | Child process serialization config |
| `packages/zero-cache/src/db/transaction-pool.ts` | PG transaction worker pool |
| `packages/zqlite/src/database-storage.ts:48-59` | Temp SQLite per pool thread, EXCLUSIVE locking |
| `packages/zero-cache/src/services/change-streamer/storer.ts:144-147` | Heap-based backpressure (GC workaround) |
| `packages/zero-cache/src/config/zero-config.ts:440-454` | numPoolThreads config |
| `docs/pool-threads-v3-architecture.md` | Complete v3 design, v2 regressions, measured IPC costs |

## Appendix C: Rust Crate Equivalents

| Node.js Component | Rust Crate | Notes |
|-------------------|------------|-------|
| `better-sqlite3` | `rusqlite` | Multi-threaded reads in WAL mode |
| `postgres` (npm) | `tokio-postgres` + `deadpool-postgres` | Async, connection pooling |
| `fastify` (WebSocket) | `axum` + `tokio-tungstenite` | Async WebSocket |
| `worker_threads` | `tokio::task::spawn` / `rayon` | Shared memory, no serialization |
| `child_process.fork()` | Not needed | Single process model |
| `MessageChannel` / `MessagePort` | `tokio::sync::mpsc` | Zero-copy channel |
| `structuredClone` | Not needed | `Arc<T>` for sharing, `Clone` for explicit copies |
| V8 GC | `Drop` trait | Deterministic deallocation |
| `SharedArrayBuffer` | Default (shared heap) | All threads share memory natively |
| OpenTelemetry JS SDK | `opentelemetry-rust` | Mature, lower overhead |
