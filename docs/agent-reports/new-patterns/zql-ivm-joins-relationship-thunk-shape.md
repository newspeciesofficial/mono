# New pattern: Node.relationships as `Record<string, () => Stream<Node | 'yield'>>`

## Category
D (Data) — the in-memory shape of a Node produced by the IVM graph.

## Where used
- `packages/zql/src/ivm/data.ts` — `type Node = { row: Row; relationships: Record<string, () => Stream<Node | 'yield'>> }`
- `packages/zql/src/ivm/join.ts:255-295` — builds a thunk that closes over
  `parentNodeRow` and returns a fresh stream each time it is called.
- `packages/zql/src/ivm/exists.ts:254,261` — calls
  `node.relationships[this.#relationshipName]()` to materialize the child
  stream for size counting.
- `packages/zql/src/ivm/flipped-join.ts:282, 365, 391, 410, 423-430, 442` —
  builds and invokes relationship thunks.

## TS form
```ts
// Value of relationships[name] is a zero-arg function returning a *fresh*
// stream every time — the thunk can be pulled multiple times without
// re-issuing the upstream fetch preserving order.
const childStream = () => {
  const constraint = buildJoinConstraint(parentNodeRow, this.#parentKey, this.#childKey);
  return constraint ? this.#child.fetch({constraint}) : [];
};

return {
  row: parentNodeRow,
  relationships: {
    ...parentNodeRelations,
    [this.#relationshipName]: childStream,
  },
};
```

## Proposed Rust form
```rust
use std::collections::HashMap;
type RelStream<'a> = Box<dyn Iterator<Item = Yielded<Node>> + 'a>;
type RelThunk = Arc<dyn Fn() -> RelStream<'static> + Send + Sync>;

pub struct Node {
    pub row: Row,
    pub relationships: HashMap<String, RelThunk>,
}

// Building one: an Arc'd closure capturing the parent row and a reference
// to the child operator (which must itself be Arc'd or have some
// interior-mutable state container).
let parent_row_owned = parent_node_row.clone();
let child = Arc::clone(&self.child);
let parent_key = self.parent_key.clone();
let child_key = self.child_key.clone();
let thunk: RelThunk = Arc::new(move || {
    let constraint = build_join_constraint(&parent_row_owned, &parent_key, &child_key);
    match constraint {
        Some(c) => Box::new(child.fetch(FetchRequest { constraint: Some(c), ..Default::default() })),
        None => Box::new(std::iter::empty()),
    }
});
```

## Classification
- Redesign — idiomatic in TS, awkward in Rust:
  - Lifetimes: the thunk captures references to the operator; the operator
    must outlive every `Node` it emits. Either `Arc<OperatorImpl>` everywhere
    (current memory cost) or parameterize lifetimes on every Node
    (cascades through the whole graph).
  - `Box<dyn Fn() -> Box<dyn Iterator>>` precludes `FnMut`; the closure
    must be `Fn` because it is called repeatedly. Any mutable state the
    closure needs must be behind `Cell`/`Mutex`.

## Caveats
- The thunk is called 0, 1, or many times. `Exists::#fetchSize` calls it to
  count size without consuming the outer stream. `generateWithOverlay`
  (in `join-utils.ts`) may call it again when wrapping a `child` overlay.
  An `FnOnce`-based model does *not* work.
- Rust `impl Iterator for Box<dyn ...>` is fine, but the returned iterator
  itself may hold references into the operator's storage. If the storage is
  `Arc<dyn Input>`, those references are `'static` via the `Arc`. If the
  storage is owned, the lifetime has to thread through — usually
  unacceptable at this depth.
- A simpler alternative is to *eagerly* materialize relationships as
  `Vec<Node>` at the point of construction. This loses the TS laziness (and
  costs a full table fetch per parent) but removes all lifetime machinery.
  Do not adopt without profiling — the laziness is load-bearing for
  correctness under `Exists` (which only counts) and in push paths that
  never re-read the relationship.

## Citation
- Rust reference, "Closure types" —
  https://doc.rust-lang.org/reference/types/closure.html
- `dyn-clone` crate (for cloning `Box<dyn Fn>`) —
  https://crates.io/crates/dyn-clone
