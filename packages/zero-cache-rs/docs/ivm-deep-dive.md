# IVM Deep Dive: How Zero's Incremental View Maintenance Works

> Audience: someone debugging the Rust port (`packages/zero-cache-rs/`) who needs
> to understand the TS implementation it mirrors. Every claim here points at a
> file and line; no hand-wavy descriptions.

---

## 1. What problem IVM solves

A ZQL query (e.g. `messages where conversationID = X order by sentAt desc limit 50`)
compiles into a tree of small **operators**. The tree must:

1. Produce the **initial result set** the first time it runs (called *hydration*).
2. **Incrementally update** that result set whenever any underlying row changes —
   without re-running the whole query.

IVM = "incremental view maintenance" = step 2.

A change to one row in PostgreSQL → SQLite replica must turn into a small,
exact diff at the top of the operator tree (`Add row X`, `Remove row Y`, `Edit
row Z`), which the view-syncer then ships to the client over WebSocket.

---

## 2. The two flows: `fetch` and `push`

Every operator implements two methods. They flow in opposite directions:

```
                              push (changes)
                              ────────────►
                  ┌────────────────────────────────────┐
                  │   FilterEnd (top of tree)          │  ← emits RowChange
                  ├────────────────────────────────────┤
                  │   Take (limit)                     │
                  ├────────────────────────────────────┤
                  │   Join (parent ⨝ child)            │
                  ├────────────────────────────────────┤
                  │   FilterStart                      │
                  ├────────────────────────────────────┤
                  │   TableSource (root)               │  ← receives source change
                  └────────────────────────────────────┘
                              ◄────────────
                              fetch (queries)
```

- **`fetch(req)`** flows **upward**. The top operator asks its input "give me
  your rows that match this constraint", which recursively asks down.
  Used during hydration and inside push when an operator needs context.
- **`push(change, pusher)`** flows **downward**. A source row changed at the
  bottom; the change propagates up to the top of the tree, transformed at each
  level (filtered, joined, limited).

TS interface: `packages/zql/src/ivm/operator.ts:24-77`.

---

## 3. The TS `Stream` type and why it matters

```ts
// packages/zql/src/ivm/stream.ts:8
export type Stream<T> = Iterable<T>;
```

A `Stream<T>` is just an **iterable** — but in practice it is always a
**generator function** (`function*`). That choice is load-bearing. Here's why:

### 3.1 The `*` and `yield*` operators (TS-specific)

```ts
// packages/zql/src/ivm/filter.ts:54
*push(change: Change) {
  yield* filterPush(change, this.#output, this, this.#predicate);
}
```

- `function* foo()` declares a **generator function**. It returns an iterator
  that yields values lazily — no work happens until the caller calls `.next()`.
- `yield value` produces one value and pauses execution.
- `yield* otherIterable` **delegates** to another iterable: every value the
  inner iterable yields is yielded out of the outer generator. Equivalent to
  `for (const v of otherIterable) yield v;`.

### 3.2 Why generators (not arrays) — three reasons

**(a) Lazy fetch from SQLite.** A `fetch(req)` may need to read thousands of
rows. If `Take` only wants the first 50, the source's iterator stops at row 50
and SQLite never reads the rest. TS uses `for-of` + `break` exactly for this:

```ts
// packages/zql/src/ivm/take.ts:175-186
for (const inputNode of this.#input.fetch(req)) {
  if (inputNode === 'yield') { yield 'yield'; continue; }
  yield inputNode;
  bound = inputNode.row;
  size++;
  if (size === this.#limit) {
    break;   // ← stops upstream iteration
  }
}
```

**(b) Cooperative scheduling — the `'yield'` sentinel.** Hydration of a large
query can take 100+ ms. Node.js is single-threaded; blocking that long freezes
the WebSocket loop. So `Stream` is actually `Stream<T | 'yield'>` and any deep
operation periodically yields the literal string `'yield'`. The pipeline driver
(`packages/zero-cache/src/services/view-syncer/pipeline-driver.ts`) checks a
`Timer`; when it sees `'yield'` and enough wall time has passed, it `await`s a
microtask before continuing. The contract is documented at
`packages/zql/src/ivm/operator.ts:34-40`:

> During fetch: If an input yields 'yield', 'yield' must be yielded to the
> caller of fetch immediately.

**(c) `try/finally` around resource cleanup.** A SQLite statement opened inside
a generator must be closed even if the consumer breaks early. Generators
guarantee the `finally` block runs when the iterator is closed. Example:

```ts
// packages/zql/src/ivm/filter-operators.ts:86-103
*fetch(req: FetchRequest): Stream<Node | 'yield'> {
  this.#output.beginFilter();
  try {
    for (const node of this.#input.fetch(req)) {
      if (yield* this.#output.filter(node)) yield node;
    }
  } finally {
    this.#output.endFilter();   // runs even if downstream broke early
  }
}
```

### 3.3 Mapping to Rust

TS pattern | Rust equivalent
---|---
`function*() { yield x }` | `Box<dyn Iterator<Item=X>>` (collected into a `Vec` then `into_iter()` in this codebase)
`yield* otherStream` | `for x in other { yields_buffer.push(x) }` — see `join.rs:338-340`
`'yield'` sentinel | `enum NodeOrYield { Node(Node), Yield }` — `ivm/data.rs`
`try / finally` | `struct ClearGuard { … } impl Drop` — see `join.rs:284-296`
Lazy iteration | **Lost.** Rust impl `.collect()`s into `Vec` eagerly inside `push_to_output` (more on this below).

The Rust port chose **eager collection** instead of true streaming because
keeping borrow lifetimes correct across re-entrant operator calls with `dyn
Iterator` is painful. Trade-off: hydration is no longer lazy — every operator
materialises its full intermediate result before pushing downstream. For
`limit 50` this is fine; for un-limited subqueries this is the perf ceiling
the user is asking us about.

---

## 4. The operator catalogue

Each operator has a single TS file in `packages/zql/src/ivm/` and a 1:1 Rust
file in `packages/zero-cache-rs/crates/sync-worker/src/ivm/`.

| Operator | TS file | Rust file | Purpose |
|---|---|---|---|
| **Source** (TableSource) | `memory-source.ts` (in-mem); zqlite has its own | `memory_source.rs`; `zqlite/table_source.rs` | Root. SQLite-backed; pushes `SourceChange` into all connected outputs. |
| **Filter** | `filter.ts` | `filter.rs` | Stateless predicate. Drops non-matching changes during push. |
| **FilterStart / FilterEnd** | `filter-operators.ts` | `filter_operators.rs` | Adapter pair that lets the filter sub-graph reuse a single `beginFilter / endFilter` cache during a fetch loop. |
| **Join** | `join.ts` | `join.rs` | Inner join, hierarchical. Output = parent rows; each parent has a lazy `relationships[name]` factory yielding child rows. |
| **FlippedJoin** | `flipped-join.ts` | `flipped_join.rs` | Same as Join but parent/child swapped. Used when child cardinality is small. |
| **Exists** | `exists.ts` | `exists.rs` | Semi-join: parent appears iff child has ≥1 match. Stateful (caches counts). |
| **Take** | `take.ts` | `take.rs` | LIMIT N. Maintains a "bound" row; rejects pushes past the bound; on at-limit Add, evicts the bound row first. |
| **Skip** | `skip.ts` | `skip.rs` | OFFSET N. |
| **FanOut / FanIn** | `fan-out.ts`, `fan-in.ts` | `fan_out.rs`, `fan_in.rs` | Diamond: one upstream → multiple downstreams → merge. Used for OR conditions. |
| **UnionFanOut / UnionFanIn** | `union-fan-out.ts`, `union-fan-in.ts` | `union_fan_out.rs`, `union_fan_in.rs` | Like FanOut/FanIn but for UNION sub-queries. |

Each operator's responsibility on `push`: receive a Change from its single
upstream, transform it (drop, copy, split into add+remove, decorate with
relationships), and forward to its downstream via `output.push(change, this)`.

---

## 5. Anatomy of a push: walking through one example

**Setup.** Pipeline for `messages where conversationID = X limit 50`:

```
TableSource(messages) → FilterStart → Filter(conversationID=X) → FilterEnd → Take(50)
```

**Event.** A new message row is inserted: `{id: 'm123', conversationID: 'X', sentAt: ...}`.

**TS trace:**

1. `TableSource.push({type:'add', row: {...}})` (`packages/zql/src/ivm/memory-source.ts`)
   → applies the row to its in-memory store, then iterates its connected
   outputs, calling `output.push(change, this)` for each. Yields control between
   each output (the genPush variant).

2. `FilterStart.push(change)` (`filter-operators.ts:82`):
   ```ts
   *push(change) { yield* this.#output.push(change, this); }
   ```
   Just forwards. Identity adapter.

3. `Filter.push(change)` (`filter.ts:54`):
   ```ts
   *push(change) { yield* filterPush(change, this.#output, this, this.#predicate); }
   ```
   `filterPush` (`filter-push.ts:18-35`) checks `predicate(change.node.row)`:
   - `conversationID === 'X'` → true → forwards to `FilterEnd.push`.
   - false → swallows the change. The pipeline emits nothing.

4. `FilterEnd.push(change)` (`filter-operators.ts:143`):
   ```ts
   *push(change) { yield* this.#output.push(change, this); }
   ```
   Identity adapter.

5. `Take.push(change)` (`take.ts`, ~`pushAdd` branch):
   - Reads its `TakeState{size, bound}` from storage.
   - If `size < limit`: accept; emit `{type:'add', node}`; update bound if row >
     current bound.
   - If `size === limit` and row ≤ bound: emit `{type:'remove', node: bound}`
     followed by `{type:'add', node: new}`. **This is where UI flicker comes
     from on at-limit inserts.**
   - If `size === limit` and row > bound: drop.

6. The `Take`'s output is the pipeline driver's collector. It captures the
   `add`/`remove` Changes and converts them into `RowChange` records that
   eventually hit the WebSocket as poke `delRow` / `putRow`.

**Per-step push count** for this example: 5 operators × 1 push each = 5 calls.
For an at-limit insert, Take emits **2** downstream pushes (remove + add) — so
the collector sees 2 RowChanges. That two-step is observed in the UI as
flicker if rendered without batching.

---

## 6. Push with a Join: where it gets weird

For `conversations.related('messages')`:

```
                           ┌── parent ── TableSource(conversations) → Filter → …
Join(rel='messages')  ──── │
                           └── child ─── TableSource(messages)
```

Join has **two upstreams**, parent and child. They both push *into Join*. TS
wires this in the constructor:

```ts
// packages/zql/src/ivm/join.ts:89-94
parent.setOutput({
  push: (change: Change) => this.#pushParent(change),
});
child.setOutput({
  push: (change: Change) => this.#pushChild(change),
});
```

So when a `messages` row is inserted, the source pushes to the Join's
`#pushChild`, **not** to Join's main output. `#pushChild`:

1. Builds a constraint: "give me parent rows whose key matches this child's
   key" (`join.ts:222-227`).
2. **Calls `this.#parent.fetch(constraint)`** to find which parent(s) the child
   belongs to. For each parent match:
3. Emits a `{type: 'child', node: parent, child: {relationshipName, change}}`
   downstream.

The downstream sees a `ChildChange` — meaning "this parent already in the result
set has a new child row" — and incorporates it.

**Note the dependency:** during a child push, Join calls **fetch on its
parent**. If a downstream operator (e.g. Take) reacts to that ChildChange by
calling **fetch on Join** (which calls fetch on its parent again), and locks
are non-reentrant, **this is the deadlock pattern we just fixed**.

---

## 7. The Rust port: where it diverges and why bugs surface

### 7.1 No back-edges in the constructor

TS does `parent.setOutput({push: c => this.#pushParent(c)})` inside Join's
constructor. Rust **cannot** without creating an `Arc` cycle: Join would own
the upstream which would own a closure capturing `Arc<Join>`.

The Rust workaround:

```rust
// crates/sync-worker/src/ivm/join.rs:134-149  (new_wired)
pub fn new_wired(args: JoinArgs) -> Arc<Self> {
    let arc = Arc::new(Self::new(args));
    let parent_back: Box<dyn Output> = Box::new(JoinParentBackEdge(Arc::clone(&arc)));
    let child_back:  Box<dyn Output> = Box::new(JoinChildBackEdge(Arc::clone(&arc)));
    arc.parent.lock().unwrap().set_output(parent_back);
    arc.child.lock().unwrap().set_output(child_back);
    arc
}
```

`JoinParentBackEdge` / `JoinChildBackEdge` are tiny Output structs that hold an
`Arc<Join>` and forward `push` to `Join::push_parent` / `Join::push_child`.
This re-creates the back-edge wiring TS gets for free from closures.

### 7.2 Interior mutability via `Mutex`

TS operators are single-threaded — no locks needed. Rust's `dyn Input + Send +
Sync` requires `&self` push (because `Output::push` takes `&mut self` but the
Output is shared). To mutate internal state from `&self`, every operator wraps
its mutable fields in `Mutex` (or `RefCell` inside a single-thread context).

Join has **four** Mutex'd fields:
- `parent: Arc<Mutex<Box<dyn Input>>>`
- `child: Arc<Mutex<Box<dyn Input>>>`
- `output: Arc<Mutex<Option<Box<dyn Output>>>>`
- `inprogress_child_change: Arc<Mutex<Option<JoinChangeOverlay>>>`

### 7.3 The deadlock class we hit (and fixed)

`std::sync::Mutex` in Rust is **non-reentrant**: locking it twice from the
same thread deadlocks (it doesn't even panic — it just hangs).

The bug pattern, found in Join, FlippedJoin, and Filter:

```rust
// BAD (paraphrase of the old code)
fn push_to_output(&self, change: Change) {
    let parent = self.parent.lock().unwrap();   // ← lock held
    let mut out = self.output.lock().unwrap();
    out.push(change, &*parent);                  // ← downstream fires…
    // …downstream Take calls Join.fetch, which calls self.parent.lock() → deadlock
}
```

The fix (`join.rs:348-364`):

```rust
fn push_to_output(&self, change: Change) {
    let mut out = self.output.lock().unwrap();
    // Pass `self` as InputBase identity — DO NOT hold parent lock here.
    out.push(change, self as &dyn InputBase).collect()
}
```

**Same fix applied to `flipped_join.rs` and `filter.rs`.** The remaining
operators (FanOut, FanIn, Take, Skip, Exists) were audited and do not hold
parent/input lock across `sink.push` — confirmed safe.

### 7.4 Why `parking_lot::ReentrantMutex` was a wrong turn

I floated swapping in `parking_lot::ReentrantMutex<RefCell<T>>`. It would have
turned the deadlock into a `RefCell` borrow_mut panic — **same logic bug,
different failure mode**. The actual fix is to not re-lock; the library swap
papers over but doesn't address the underlying lock-hold-across-callback
mistake.

---

## 8. Source-of-truth files: where the code lives

### TS (reference implementation)

| Concern | Path |
|---|---|
| Operator interface | `packages/zql/src/ivm/operator.ts` |
| `Stream<T>` definition | `packages/zql/src/ivm/stream.ts` |
| Change types (Add/Remove/Edit/Child) | `packages/zql/src/ivm/change.ts` |
| Source interface | `packages/zql/src/ivm/source.ts` |
| In-memory source | `packages/zql/src/ivm/memory-source.ts` |
| Filter | `packages/zql/src/ivm/filter.ts`, `filter-push.ts`, `filter-operators.ts` |
| Join | `packages/zql/src/ivm/join.ts`, `join-utils.ts` |
| Flipped join | `packages/zql/src/ivm/flipped-join.ts` |
| Exists | `packages/zql/src/ivm/exists.ts` |
| Take | `packages/zql/src/ivm/take.ts` |
| Skip | `packages/zql/src/ivm/skip.ts` |
| FanOut/FanIn | `packages/zql/src/ivm/fan-out.ts`, `fan-in.ts` |
| Union FanOut/FanIn | `packages/zql/src/ivm/union-fan-out.ts`, `union-fan-in.ts` |
| Pipeline driver | `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts` |
| View syncer (run loop) | `packages/zero-cache/src/services/view-syncer/view-syncer.ts` |
| Snapshotter (WAL2) | `packages/zero-cache/src/services/view-syncer/snapshotter.ts` |
| zqlite TableSource | `packages/zqlite/src/table-source.ts` |
| Pipeline builder | `packages/zql/src/builder/builder.ts` |

### Rust port (1:1 mirror in `packages/zero-cache-rs/crates/sync-worker/src/`)

| Concern | Path |
|---|---|
| Operator trait | `ivm/operator.rs` |
| `Stream<'a, T>` alias | `ivm/stream.rs` |
| Change enum | `ivm/change.rs` |
| Source trait | `ivm/source.rs` |
| In-memory source | `ivm/memory_source.rs` |
| Filter | `ivm/filter.rs`, `ivm/filter_push.rs`, `ivm/filter_operators.rs` |
| Join | `ivm/join.rs`, `ivm/join_utils.rs` |
| Flipped join | `ivm/flipped_join.rs` |
| Exists | `ivm/exists.rs` |
| Take | `ivm/take.rs` |
| Skip | `ivm/skip.rs` |
| FanOut/FanIn | `ivm/fan_out.rs`, `ivm/fan_in.rs` |
| Union FanOut/FanIn | `ivm/union_fan_out.rs`, `ivm/union_fan_in.rs` |
| Pipeline driver | `view_syncer/pipeline_driver.rs` |
| zqlite TableSource | `zqlite/table_source.rs` |
| Pipeline builder | `builder/builder.rs` |

### TS↔Rust glue

| Concern | Path |
|---|---|
| Rust napi entry points | `packages/zero-cache-rs/crates/shadow-ffi/src/lib.rs` |
| TS-side wrapper that calls into Rust | `packages/zero-cache/src/services/view-syncer/rust-pipeline-driver.ts` |
| Native loader | `packages/zero-cache/src/shadow/native.ts` |
| Feature flag | env var `ZERO_USE_RUST_IVM=1` |

---

## 9. Common failure modes & where to look

| Symptom | First place to check |
|---|---|
| Hang on hydration / advance | A `Mutex` held across a downstream `sink.push()` call. Grep for `.lock()` followed by `.push(` in operators. |
| `panic: Output not set` | A back-edge is missing — either `set_output` was never called, or `new_wired` wasn't used. See `builder.rs` for wiring sites. |
| Row appears then disappears (flicker) | Take eviction (Section 5, step 5) — Remove + Add pair. Not necessarily a bug; verify the Add eventually reaches client. |
| New row never appears | Source `push_change` filter-dropped on every connection. Add tracing to `zqlite/table_source.rs` `push_change`. |
| Different output from TS for same query | Hydration order or relationship factory eagerness. Compare `[TRACE row_change]` (Rust, in `pipeline_driver.rs:~1273`) against `[TRACE rust-rowchange]` (TS, in `rust-pipeline-driver.ts`). |

---

## 10. Mental model summary

- **Operators are nodes in a graph.** Each has 1+ upstream inputs and 1
  downstream output. Source has 0 upstreams.
- **Two flows:** `fetch` (top-down, asks for data) and `push` (bottom-up,
  delivers changes).
- **`Stream<T>` = lazy iterable.** TS uses `function*` + `yield*` for
  cooperative scheduling and resource cleanup. Rust port collects into `Vec`
  eagerly — loses laziness, gains lifetime sanity.
- **Joins have back-edges.** Parent and child both push into Join; Join also
  fetches from parent during a child push. This re-entrancy is the source of
  Rust's lock-ordering bugs.
- **Take splits inserts into Remove+Add at the limit.** That's the visible
  flicker on the user's chat UI when conversation list is full.
- **TS wires push closures in constructors; Rust uses `Arc<Self>` +
  `XBackEdge(Arc<X>)` adapter structs in `new_wired()`.** This is the largest
  structural divergence between the two implementations.
