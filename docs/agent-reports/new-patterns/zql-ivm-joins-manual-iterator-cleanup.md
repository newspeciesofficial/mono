# New pattern: manual iterator cleanup in a merge loop

## Category
A (Language) — concerns iterator lifecycle semantics that don't map onto A22
(`impl Iterator`) directly.

## Where used
- `packages/zql/src/ivm/flipped-join.ts:158-197` — builds `parentIterators: Iterator<...>[]`
- `packages/zql/src/ivm/flipped-join.ts:287-308` — `try { … } catch { iter.throw?.(e) } finally { iter.return?.() }`
- `packages/zql/src/ivm/union-fan-in.ts:218-291` — `mergeFetches`, same pattern

## TS form
```ts
const parentIterators: Iterator<Node | 'yield'>[] = [];
let threw = false;
try {
  for (const childNode of childNodes) {
    const stream = this.#parent.fetch({...});
    parentIterators.push(stream[Symbol.iterator]());
  }
  // ... draw one value from each iterator, advance the min one ...
} catch (e) {
  threw = true;
  for (const iter of parentIterators) {
    try { iter.throw?.(e); } catch (_cleanupError) { /* swallow */ }
  }
  throw e;
} finally {
  if (!threw) {
    for (const iter of parentIterators) {
      try { iter.return?.(); } catch (_cleanupError) { /* swallow */ }
    }
  }
}
```

## Proposed Rust form
```rust
// In Rust, dropping an iterator runs any cleanup in its Drop impl. The TS
// cleanup protocol collapses to: pin the iterators in a Vec, run the merge
// loop inside a scope, and let Drop run automatically when the Vec goes out
// of scope — even on panic.

{
    let mut iters: Vec<Box<dyn Iterator<Item = Yielded<Node>>>> = Vec::new();
    for child in &child_nodes {
        iters.push(Box::new(self.parent.fetch(&req_for(child))));
    }
    // ... merge loop ...
    // On panic here, `iters` is dropped; each Box runs the iterator's Drop.
} // normal or panic exit: Drop fires here
```

## Classification
- Idiom-swap — the shape is standard Rust RAII. The caveat is that TS
  `iter.throw(e)` can observe the error *inside the generator* (so finally
  blocks inside the generator see `e`); Rust `Drop` does not let the
  iterator see why it is being dropped.

## Caveats
- If an operator's cleanup must know whether it was cancelled vs completed,
  TS can pass that distinction via `iter.throw` vs `iter.return`. In Rust
  there is no equivalent; add an explicit `close(&mut self, reason: CloseReason)`
  method on the operator trait and call it before drop where it matters.
  Reviewing the code in scope, **no cleanup currently branches on cancel vs
  complete**, so this is safe to ignore for now.
- Panics unwind the stack by default, which will Drop in-flight iterators
  cleanly. Do not compile with `panic = "abort"` on the IVM crate without
  reviewing this.

## Citation
- Rust reference, "Destructors" —
  https://doc.rust-lang.org/reference/destructors.html
- `scopeguard` crate (for explicit on-exit actions that need to run
  regardless of panic / ok) — https://crates.io/crates/scopeguard
