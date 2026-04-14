# New pattern: generator scope-guard on per-push state

## Category
A (Language) — a finally-reset-field pattern that spans a generator's
lifetime, not just a function call.

## Where used
- `packages/zql/src/ivm/exists.ts:107-212` — sets `this.#inPush = true`,
  resets in `finally`; asserts non-reentrancy up front.
- `packages/zql/src/ivm/join.ts:216-250` — sets
  `this.#inprogressChildChange = {change, position: undefined}`, resets in
  `finally`; read by `#processParentNode` (invoked deep inside the generator
  body).
- `packages/zql/src/ivm/flipped-join.ts:336-419` — same pattern for
  `this.#inprogressChildChange`.

## TS form
```ts
*push(change: Change): Stream<'yield'> {
  assert(!this.#inPush, 'Unexpected re-entrancy');
  this.#inPush = true;
  try {
    // ... yields arbitrarily, calls helpers that *read* #inPush ...
  } finally {
    this.#inPush = false;
  }
}

*#pushChildChange(childRow, change) {
  this.#inprogressChildChange = { change, position: undefined };
  try {
    for (const parentNode of this.#parent.fetch({constraint})) {
      this.#inprogressChildChange.position = parentNode.row; // mutated mid-yield
      yield* this.#output.push({...}, this);
    }
  } finally {
    this.#inprogressChildChange = undefined;
  }
}
```

The key property: `fetch` (called from elsewhere) reads
`this.#inprogressChildChange` to decide whether to apply an overlay. So the
field is *observably set* across `yield` points, not just within a synchronous
call frame.

## Proposed Rust form
```rust
// If the generator is an iterator (genawaiter or the Yielded<T> enum
// scheme), the `finally` block must run on:
//   (a) normal completion of the generator,
//   (b) early drop (consumer stopped pulling),
//   (c) panic.
// In Rust, implement Drop on a guard value owned by the generator's state:

struct InPushGuard<'a> { flag: &'a Cell<bool> }
impl Drop for InPushGuard<'_> {
    fn drop(&mut self) { self.flag.set(false); }
}

impl Exists {
    fn push(&self, change: Change) -> Gen<Yielded<()>, (), ...> {
        assert!(!self.in_push.get(), "Unexpected re-entrancy");
        self.in_push.set(true);
        let _guard = InPushGuard { flag: &self.in_push };
        // body; _guard dropped at scope end — even on early-drop / panic
    }
}

// For `#inprogressChildChange: Option<JoinChangeOverlay>` the guard becomes:
struct OverlayGuard<'a> { slot: &'a RefCell<Option<JoinChangeOverlay>> }
impl Drop for OverlayGuard<'_> {
    fn drop(&mut self) { *self.slot.borrow_mut() = None; }
}
```

## Classification
- Idiom-swap — the Rust form is idiomatic RAII; the non-trivial bit is that
  it has to live on the generator's captured state, not a plain stack frame.

## Caveats
- If the generator representation chosen in
  `zql-ivm-joins-cooperative-yield-token` is `Iterator<Item = Yielded<T>>`
  built as a hand-rolled state machine, the scope-guard has to be a field
  of the state machine with an explicit `Drop` — *not* a stack local in
  the iterator body, because there is no such body.
- The `#inprogressChildChange` field is read by *other* methods (`fetch`,
  `#processParentNode`) on the same operator while the push generator is
  suspended at a `yield`. In Rust this requires interior mutability
  (`Cell`/`RefCell`) on the operator struct. `Mutex`/`Arc` are overkill —
  the IVM graph is single-threaded within one pipeline.

## Citation
- `scopeguard` crate (`defer!` macro + `ScopeGuard<T, F>`) —
  https://crates.io/crates/scopeguard
- Rust Nomicon, "Destructors" —
  https://doc.rust-lang.org/nomicon/destructors.html
