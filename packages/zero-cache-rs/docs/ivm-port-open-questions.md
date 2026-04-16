# IVM Port: Open Questions for a Rust Specialist

> **Audience:** an experienced Rust engineer who knows traits, lifetimes, `Pin`,
> `async/await`, and ideally the `genawaiter` crate. **No prior knowledge of
> our codebase needed.**
>
> **Task:** look at each TS snippet, our current Rust translation, and the
> friction we've hit. For each problem, propose the cleanest Rust shape.

---

## Background (1 minute)

We're porting Zero's IVM (Incremental View Maintenance) operator graph from
TypeScript to Rust. The reference TS implementation is at
`packages/zql/src/ivm/`. Our Rust port is at
`packages/zero-cache-rs/crates/sync-worker/src/ivm/`.

TS uses **generators** (`function*`, `yield`, `yield*`) extensively. Rust
has no stable native generator support (`std::ops::Coroutine` is nightly-only
as of April 2026). We've adopted `genawaiter::sync` (stable Rust generators
on top of async/await machinery) — confirmed working via a smoke test
at `crates/sync-worker/src/ivm/filter_gen.rs`.

The big problems below are about **trait shape and ownership** — not about
genawaiter syntax.

---

## Problem 1: `Output::push` trait shape forces eager `.collect()` under a Mutex

### TS

```ts
// packages/zql/src/ivm/operator.ts
export interface Output {
  push(change: Change, pusher: InputBase): Stream<'yield'>;
}

// Caller (packages/zql/src/ivm/filter.ts:54)
*push(change: Change) {
  yield* filterPush(change, this.#output, this, this.#predicate);
}
```

`Stream<T> = Iterable<T>`. In TS, `output.push(change, pusher)` returns a
generator that lazily yields `'yield'` sentinels — the caller can iterate
it without anything special.

### Our Rust

```rust
// crates/sync-worker/src/ivm/operator.rs
pub trait Output: Send + Sync {
    fn push<'a>(
        &'a mut self,
        change: Change,
        pusher: &dyn InputBase,
    ) -> Stream<'a, Yield>;
}

// crates/sync-worker/src/ivm/stream.rs
pub type Stream<'a, T> = Box<dyn Iterator<Item = T> + 'a>;
```

The returned stream borrows `&'a mut self`. Callers that hold the output
behind a `Mutex<Box<dyn Output>>` (which we all do) must keep the lock
guard alive for the duration of iteration. So every operator does:

```rust
// crates/sync-worker/src/ivm/filter_gen.rs:84-99 (paraphrased)
let collected: Vec<Yield> = {
    let mut output_g = self.output.lock().unwrap();
    filter_push(change, &mut **output_g, …).collect()  // ← eager collect under lock
};
for y in collected { co.yield_(y).await; }
```

This **defeats the whole point** of switching to generators — we still
`.collect()` to a `Vec` between every operator.

### What we want

A trait shape where `Output::push` returns a stream that **doesn't borrow
`&mut self`** — so the caller can release the lock immediately and then
iterate the stream lazily.

### Constraints

- Must be `Send` (operators run inside `spawn_blocking`, may be touched from
  worker threads).
- Must work with `Box<dyn Output>` (operators are heterogeneous; we can't
  monomorphize the entire graph).
- Must not require `'static` *captures* (the closure may need to reference
  the operator's state via `Arc<Self>` clones).
- Must preserve the contract that the change has been "delivered" once the
  stream is fully consumed (sources commit on stream exhaustion).

### Specific question

What's the right shape for `Output::push`? Candidates we've considered:

```rust
// (a) Return a boxed generator that owns its captures
fn push(&self, change: Change, pusher: Arc<dyn InputBase>) -> GenBoxed<Yield>;

// (b) Take a callback (push-style API)
fn push(&self, change: Change, on_yield: &mut dyn FnMut() -> ControlFlow);

// (c) Pass in an &mut Vec (caller owns the buffer)
fn push(&self, change: Change, out: &mut Vec<Yield>);
```

Each has trade-offs we don't fully understand. Which is canonical Rust?

---

## Problem 2: Operator constructor wiring — TS `parent.setOutput({push: c => this.#pushParent(c)})`

### TS

```ts
// packages/zql/src/ivm/join.ts:89-94
constructor({parent, child, parentKey, childKey, …}) {
  // …
  parent.setOutput({
    push: (change: Change) => this.#pushParent(change),
  });
  child.setOutput({
    push: (change: Change) => this.#pushChild(change),
  });
}
```

TS closures naturally capture `this`. `parent` ends up holding a back-ref to
the Join. Garbage collector handles the cycle.

### Our Rust attempt

```rust
// crates/sync-worker/src/ivm/join.rs:134-149
pub fn new_wired(args: JoinArgs) -> Arc<Self> {
    let arc = Arc::new(Self::new(args));
    let parent_back: Box<dyn Output> = Box::new(JoinParentBackEdge(Arc::clone(&arc)));
    let child_back:  Box<dyn Output> = Box::new(JoinChildBackEdge(Arc::clone(&arc)));
    arc.parent.lock().unwrap().set_output(parent_back);
    arc.child.lock().unwrap().set_output(child_back);
    arc
}

struct JoinParentBackEdge(Arc<Join>);
impl Output for JoinParentBackEdge {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) -> Stream<'_, Yield> {
        Box::new(self.0.push_parent(change).into_iter())
    }
}
// (similar for JoinChildBackEdge)
```

Two adapter structs per operator, plus an `Arc<Self>` cycle (parent input
holds `Arc<Join>` → Join holds parent input). Cycle is bounded by pipeline
lifetime (driver tears down the whole graph), but it's still an explicit
strong-cycle that bothers reviewers.

### What we want

Ideally: `parent.set_output(self.as_parent_pusher())` returning some wrapper
that's not an entire boilerplate struct per operator. Or an entirely
different ownership model.

### Specific question

Is the `XBackEdge(Arc<Self>)` adapter struct + `Arc` cycle the right pattern,
or is there a way to capture `Arc<Self>` in a closure that satisfies
`Box<dyn Output>` without a named struct? Should we use `Weak<Self>` to
break the cycle? If so, how does cleanup work when the upstream tries to
push after the operator is dropped?

---

## Problem 3: TS `try / finally` for in-progress overlay state

### TS

```ts
// packages/zql/src/ivm/join.ts:216-251 (#pushChildChange)
*#pushChildChange(childRow: Row, change: Change): Stream<'yield'> {
  this.#inprogressChildChange = { change, position: undefined };
  try {
    const constraint = buildJoinConstraint(childRow, this.#childKey, this.#parentKey);
    const parentNodes = constraint ? this.#parent.fetch({constraint}) : [];

    for (const parentNode of parentNodes) {
      if (parentNode === 'yield') { yield parentNode; continue; }
      this.#inprogressChildChange.position = parentNode.row;
      const childChange: ChildChange = { … };
      yield* this.#output.push(childChange, this);
    }
  } finally {
    this.#inprogressChildChange = undefined;
  }
}
```

The `try/finally` guarantees `inprogressChildChange` is cleared even if the
caller breaks early (closes the iterator) or the downstream throws.

### Our Rust

```rust
// crates/sync-worker/src/ivm/join.rs:273-345 (push_child_change)
fn push_child_change<'a>(&'a self, child_row: Row, change: Change) -> Stream<'a, Yield> {
    {
        let mut guard = self.inprogress_child_change.lock().unwrap();
        *guard = Some(JoinChangeOverlay { change: change.clone(), position: None });
    }

    // Drop-guard equivalent for the TS `finally`: clear on unwind too.
    struct ClearGuard<'a> {
        slot: &'a Mutex<Option<JoinChangeOverlay>>,
    }
    impl<'a> Drop for ClearGuard<'a> {
        fn drop(&mut self) {
            if let Ok(mut g) = self.slot.lock() {
                *g = None;
            }
        }
    }
    let _guard = ClearGuard { slot: &self.inprogress_child_change };

    // … iteration that mutates `position` field …
    let mut yields_buffer: Vec<Yield> = Vec::new();
    // (eager collect again — see Problem 1)
    Box::new(yields_buffer.into_iter())
}
```

Drop guard works **only because we eagerly collect**. If we returned a lazy
generator (per Problem 1), the `_guard` would be dropped at function return
*before* the caller iterates, clearing `inprogressChildChange` too early.

### What we want

A way to tie the `ClearGuard` lifetime to the **iteration** of the returned
stream, not to the function call frame.

### Specific question

If we adopt a `GenBoxed`-returning `Output::push` (Problem 1), how do we
move a Drop guard *into the generator's closure* so it runs when the
generator is dropped (i.e., when iteration completes or is abandoned)? And
how do we structure a generator whose body has shared mutable state (the
`position` field) that callers downstream observe via `Arc<Mutex<…>>`
borrowed from the operator?

---

## Problem 4: TS `processParentNode` returns a Node with a lazy relationship factory

### TS

```ts
// packages/zql/src/ivm/join.ts:253-295
#processParentNode(parentNodeRow, parentNodeRelations) {
  const childStream = () => {
    const constraint = buildJoinConstraint(parentNodeRow, this.#parentKey, this.#childKey);
    const stream = constraint ? this.#child.fetch({constraint}) : [];
    // … overlay logic …
    return stream;
  };
  return {
    row: parentNodeRow,
    relationships: { ...parentNodeRelations, [this.#relationshipName]: childStream },
  };
}
```

`childStream` is a closure that captures `this.#child` and `this.#parentKey`
by reference. Called many times, lazily, by downstream operators that
choose when to materialize the relationship. Garbage collector keeps `this`
alive as long as any returned Node holds a closure.

### Our Rust

```rust
// crates/sync-worker/src/ivm/join.rs:154-218 (process_parent_node)
fn process_parent_node(&self, parent_row: Row) -> Node {
    let child = Arc::clone(&self.child);                       // Arc<Mutex<Box<dyn Input>>>
    let inprogress = Arc::clone(&self.inprogress_child_change);
    let parent_key = self.parent_key.clone();
    let child_key  = self.child_key.clone();
    let parent_row_clone = parent_row.clone();
    let child_schema = self.child_schema.clone();
    let schema_compare = Arc::clone(&self.schema.compare_rows);

    let factory: RelationshipFactory = Box::new(move || {
        let constraint_opt = build_join_constraint(&parent_row_clone, &parent_key, &child_key);
        let nodes: Vec<NodeOrYield> = match constraint_opt {
            None => Vec::new(),
            Some(constraint) => {
                let child_guard = child.lock().unwrap();    // ← lock held while iterating
                child_guard.fetch(FetchRequest { constraint: Some(constraint), ..Default::default() })
                    .collect()                              // ← eager again
            }
        };
        // … overlay …
        Box::new(nodes.into_iter()) as Box<dyn Iterator<Item = NodeOrYield>>
    });

    Node {
        row: parent_row,
        relationships: {
            let mut m = IndexMap::new();
            m.insert(self.relationship_name.clone(), factory);
            m
        },
    }
}
```

Seven `Arc::clone` / `.clone()` calls just to construct the closure. `child`
is locked across the entire `fetch().collect()`. Eager collection again
(Problem 1).

### Specific question

Is there a more idiomatic Rust pattern for "an operator method returns a
struct that contains a closure capturing N pieces of operator state"? Should
the operator hold its substate in a shared `Arc<JoinShared>` to halve the
clones? Is `dyn FnOnce`-vs-`dyn Fn` the right boundary (the factory may be
called more than once)?

---

## Problem 5: The `'yield'` sentinel — do we need it at all in Rust?

### TS

```ts
// packages/zql/src/ivm/operator.ts
fetch(req: FetchRequest): Stream<Node | 'yield'>;

// Contract documented at operator.ts:34-40:
// "If an input yields 'yield', 'yield' must be yielded to the caller of
//  fetch immediately."
```

The literal string `'yield'` is a cooperative-scheduling sentinel. Node.js
is single-threaded; long-running hydration would freeze the event loop, so
operators periodically yield `'yield'` and the pipeline driver `await`s a
microtask in between to let other connections breathe.

### Our Rust

```rust
// crates/sync-worker/src/ivm/data.rs
pub enum NodeOrYield {
    Node(Node),
    Yield,
}
// Threaded through every operator just like TS, but never actually
// emitted in our serial spawn_blocking model.
```

We currently propagate the `Yield` variant through every operator's fetch
output — but no Rust operator ever produces one. Tokio handles fairness via
multiple OS threads (`spawn_blocking` blocks one thread; other connections
run on others).

### Specific question

Should we **delete `NodeOrYield` and just return `Stream<Node>`** in the
Rust port? Pros: simpler code, fewer match arms, fewer allocations. Cons:
breaks line-for-line TS↔Rust parity in operator bodies. Are we missing a
case where the sentinel actually buys us something in a thread-per-task
runtime?

---

## Problem 6: FanOut — replay one stream to N consumers

### TS

```ts
// packages/zql/src/ivm/fan-out.ts:73-82
*push(change: Change) {
  for (const out of this.#outputs) {
    yield* out.push(change, this);
  }
  yield* must(this.#fanIn, '…').fanOutDonePushingToAllBranches(change.type);
}
```

`Change` is forwarded to **N downstream outputs** (one per branch of an
`OR` condition). Each `out.push` returns a fresh generator; TS just calls
each in sequence. Iteration order is meaningful — the paired `FanIn` uses
`fanOutDonePushingToAllBranches` to flush its accumulator after the last
branch.

### Why it's hard in Rust

`Change` is **not** `Clone` in our value-typed enum (it owns `Row`s and
nested boxes). To call `out.push(change, this)` N times we'd have to
`change.clone()` N times — but Change isn't cheap to clone, and its
sub-types weren't designed to be.

### Specific question

What's the right shape? Three candidates:
1. Implement `Clone` for `Change` (cheap-ish if rows are `Arc`'d).
2. Make `Output::push` take `&Change` instead of owning it.
3. Build a fan-out adapter that materializes each downstream call to a
   buffer first, then drains.

Today we have `(2)` partially: see `crates/sync-worker/src/ivm/fan_out.rs`.
It clones via a custom helper. Is that the right call?

---

## Problem 7: Take's at-limit eviction needs a re-entrant fetch on the same input

### TS

```ts
// packages/zql/src/ivm/take.ts:276-312 — at-limit Add path
let boundNode: Node | undefined;
for (const node of this.#input.fetch({       // ← fetch on same input we're processing a push from
  start: {row: takeState.bound, basis: 'at'},
  constraint,
  reverse: true,
})) {
  if (node === 'yield') { yield node; continue; }
  // … pick boundNode + the row before it as the new bound
}
```

When Take receives an Add at the limit and the new row is "better" than the
current bound, it must:
1. Find the current bound row via `input.fetch()` to construct a `Remove` change.
2. Find the "next-worst" row to become the new bound.
3. Emit `Remove(boundNode)` then `Add(change.node)`.

The `input.fetch()` is **on the same upstream that just pushed to us**.

### Why it's hard in Rust

This is the deadlock pattern we spent days fixing. Take's input is held
behind `Mutex<Box<dyn Input>>`. Inside push, we lock input to call fetch.
If the upstream is a Join, Join might hold its own lock while pushing into
us. Re-entrant locking on `std::sync::Mutex` deadlocks.

Our current workaround: never hold the parent lock across `sink.push`. But
this is **fragile** — every operator's push has to remember not to do it,
and there's no compiler check.

### Specific question

Is there a structural way to make re-entrant fetches safe? E.g., split the
input handle into a "push handle" and a "fetch handle" that don't share
locks? Use a re-entrant lock and accept the perf cost? Restructure so
fetches during push go through a separate, lock-free path?

---

## Problem 8: Exists's `#inPush` re-entrancy guard

### TS

```ts
// packages/zql/src/ivm/exists.ts:37, 78-92
#inPush = false;

*filter(node: Node): Generator<'yield', boolean> {
  let exists: boolean | undefined;
  if (!this.#noSizeReuse && !this.#inPush) {   // ← gate cache reuse
    const key = this.#getCacheKey(node, this.#parentJoinKey);
    exists = this.#cache.get(key);
    if (exists === undefined) {
      exists = yield* this.#fetchExists(node);
      this.#cache.set(key, exists);
    }
  }
  // …
}

*push(change: Change): Stream<'yield'> {
  this.#inPush = true;
  try {
    // … pushes that may cause recursive filter() calls …
  } finally {
    this.#inPush = false;
  }
}
```

A boolean flag toggled around push, so any recursive `filter()` invoked
from inside push **doesn't reuse the stale cache**.

### Why it's hard in Rust

`#inPush` needs interior mutability AND the toggle must survive across
yield points. Our current `Mutex<bool>` pattern works but the toggle/clear
discipline is implicit. If we adopt genawaiter, the `Drop` guard for
clearing the flag must move into the generator's closure (see Problem 3).

### Specific question

Same as Problem 3 but with a `bool` instead of an overlay struct — does
the same Drop-into-generator pattern work? Or is `AtomicBool` + try-then-
clear cleaner?

---

## Problem 9: TableSource per-connection push fanout with optional yield

### TS

```ts
// packages/zql/src/ivm/memory-source.ts (paraphrased — see source.ts:79 for interface)
*genPush(change: SourceChange): Stream<'yield' | undefined> {
  this.#applyToStorage(change);
  for (const conn of this.#connections) {
    yield* conn.push(rowChange);   // push to each connected output
    yield;                          // signal "one connection done" (undefined)
  }
}
```

A source has **N connected outputs** (one per query that reads from this
table). On push, the change goes to all of them, with an optional yield
between each so the caller can interleave other work.

### Why it's hard in Rust

Same as Problem 6 (multiple downstream pushes for one change), plus the
two-level yield contract (`'yield' | undefined`). Plus `Output::push` for
each connection borrows `&mut conn` mutably for the full stream — so we
can't lock-and-iterate without the caller-then-consumer ordering being
serial across connections.

### Specific question

Does the TableSource fanout fit the same answer as Problem 6, or is the
two-tier yield (`'yield'` for cooperative pause vs. `undefined` for
"connection N done") meaningful enough to need a different shape? In our
serial Rust runtime, can we just delete the `undefined` signal?

---

## Problem 10: FilterStart / FilterEnd begin/end caching for OR conditions

### TS

```ts
// packages/zql/src/ivm/filter-operators.ts:86-103
*fetch(req: FetchRequest): Stream<Node | 'yield'> {
  this.#output.beginFilter();          // ← signals "start of a filter loop"
  try {
    for (const node of this.#input.fetch(req)) {
      if (node === 'yield') { yield node; continue; }
      if (yield* this.#output.filter(node)) yield node;
    }
  } finally {
    this.#output.endFilter();          // ← signals "end of loop, drop caches"
  }
}
```

The pair `beginFilter()` / `endFilter()` brackets a fetch loop so any
operator inside the filter sub-graph (Exists, FanOut→FanIn) can cache
intermediate results for the duration. End of fetch → cache cleared.

### Why it's hard in Rust

The `try/finally` again — `endFilter` must run when the returned stream is
exhausted OR dropped early. With our current eager-collect pattern the
guard runs at function return. With a lazy generator it would run too
early (before iteration starts).

### Specific question

If you solve Problem 3 (Drop guard moves into generator closure), does
that automatically solve this? Or is the begin/end pair fundamentally
different because it brackets the *fetch loop*, not the push?

---

## How to use this document

For each problem:

1. Read the TS snippet (it's the source of truth).
2. Read our Rust attempt.
3. Read the specific question at the bottom.
4. **Propose a Rust shape** (trait signature, ownership pattern, Drop
   placement, etc.) that solves it cleanly.
5. If your proposal requires a wholesale trait redesign, say so — that's
   fine as long as you flag the cost.

We have a working pilot of `genawaiter` integration at
`crates/sync-worker/src/ivm/filter_gen.rs` — feel free to use that as the
testbed. The full TS source is at `packages/zql/src/ivm/`. Run tests with
`cargo test -p zero-cache-sync-worker --lib ivm::`.

Reach out with questions; the more concrete the better.
