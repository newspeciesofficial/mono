# New pattern: cooperative-yield token in a sync generator

## Category
A (Language) — specifically a hole in A16 (which covers only async
generators).

## Where used
- `packages/zql/src/ivm/operator.ts:41` — `fetch(req: FetchRequest): Stream<Node | 'yield'>`
- `packages/zql/src/ivm/stream.ts` — `type Stream<T> = Iterable<T>`
- `packages/zql/src/ivm/join.ts:110` — `*fetch(req): Stream<Node | 'yield'>`
- `packages/zql/src/ivm/join.ts:120` — `*#pushParent(change): Stream<'yield'>`
- `packages/zql/src/ivm/exists.ts:78` — `*filter(node): Generator<'yield', boolean>`
- `packages/zql/src/ivm/flipped-join.ts:116` — `*fetch` yielding `'yield'` eagerly
- `packages/zql/src/ivm/fan-out.ts:73` — `*push` delegating `yield*`
- `packages/zql/src/ivm/filter-operators.ts:37` — `filter(node: Node): Generator<'yield', boolean>`

## TS form
```ts
// Sentinel interleaved into the iterator. Callers must forward it immediately
// so the top-level driver can time-slice responsiveness across operators.
export type Stream<T> = Iterable<T>;

// Data stream: nodes OR 'yield' token
*fetch(req: FetchRequest): Stream<Node | 'yield'> {
  for (const parentNode of this.#parent.fetch(req)) {
    if (parentNode === 'yield') {
      yield parentNode;
      continue;
    }
    yield this.#processParentNode(parentNode.row, parentNode.relationships);
  }
}

// Boolean-returning filter that can yield intermittently, then returns a bool
*filter(node: Node): Generator<'yield', boolean> {
  const exists = yield* this.#fetchExists(node);
  return this.#not ? !exists : exists;
}
```

## Proposed Rust form
```rust
// Option A (recommended): an explicit item type in place of Node | 'yield'.
pub enum Yielded<T> { Value(T), Yield }

pub trait Input {
    fn fetch(&self, req: &FetchRequest) -> Box<dyn Iterator<Item = Yielded<Node>> + '_>;
}

// A generator that must also *return* a value (like `Generator<'yield', bool>`)
// is awkward with plain Iterator. Two idiomatic options:
//   1. Return `(Iterator<Item = Yield>, impl FnOnce() -> bool)` via a two-phase
//      API where the driver polls the iterator and then calls the finalizer.
//   2. Use `genawaiter::sync::Gen<Yield, (), Box<dyn Future<...>>>` which
//      preserves the "yield tokens then return a value" shape 1:1.
// Either works; pick one and apply uniformly to every filter-chain operator.

// Driver-side forwarding (equivalent to `yield* inner`):
for item in inner.fetch(req) {
    match item {
        Yielded::Yield => { /* give time-slice back */ }
        Yielded::Value(node) => { /* process */ }
    }
}
```

## Classification
- Redesign — picking the representation (sentinel enum vs `genawaiter`
  vs a custom pollable state machine) has to be done once and applied to
  every operator signature; it is not a per-file decision.

## Caveats
- Rust's `std::ops::Generator` is nightly-only; do not rely on it.
- `async_stream` (guide A16) is the wrong tool: these generators are sync
  (they do not await I/O), the `'yield'` is a cooperative-scheduling hint,
  not an await point. Using `Stream` + `tokio::pin!` would force the IVM
  graph onto the tokio runtime unnecessarily and pay an allocator tax for
  each `.poll_next`.
- `yield*` delegation (TS) becomes a plain `for` loop that forwards
  `Yielded::Yield` up and processes `Yielded::Value` locally. Mechanical, but
  every operator must do it.

## Citation
- Rust RFC 2033 "Experimental coroutines" — status tracker
  https://rust-lang.github.io/rfcs/2033-experimental-coroutines.html
- `genawaiter` crate — https://crates.io/crates/genawaiter (provides
  generators with yield-plus-return on stable Rust).
- `gen-iter` crate — https://crates.io/crates/gen-iter (lighter-weight
  alternative).
