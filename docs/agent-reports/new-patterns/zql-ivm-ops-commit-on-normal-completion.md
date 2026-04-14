# New pattern: Commit state only on normal generator completion

## Category
A (Language patterns) — builds on A15 (`try/catch/finally`) and on the
sibling pattern `zql-ivm-joins-generator-scope-guard` but with different
semantics.

## Where used
- `packages/zql/src/ivm/take.ts:174-208` — `*#initialFetch`
- Contrast: `packages/zql/src/ivm/take.ts:688-695` — `*#pushWithRowHiddenFromFetch`
  is a plain "reset on exit" scope guard (covered by the sibling pattern).

## TS form

```ts
*#initialFetch(req: FetchRequest): Stream<Node | 'yield'> {
  // ...
  let size = 0;
  let bound: Row | undefined;
  let downstreamEarlyReturn = true;
  let exceptionThrown = false;
  try {
    for (const inputNode of this.#input.fetch(req)) {
      if (inputNode === 'yield') { yield 'yield'; continue; }
      yield inputNode;
      bound = inputNode.row;
      size++;
      if (size === this.#limit) break;
    }
    downstreamEarlyReturn = false;
  } catch (e) {
    exceptionThrown = true;
    throw e;
  } finally {
    if (!exceptionThrown) {
      this.#setTakeState(takeStateKey, size, bound,
                         this.#storage.get(MAX_BOUND_KEY));
      assert(!downstreamEarlyReturn,
        'Unexpected early return prevented full hydration');
    }
  }
}
```

The `finally` block commits persistent state (`#setTakeState`) **only if no
exception escaped**. The `exceptionThrown` flag + `catch`-rethrow is the
TS way to distinguish "loop completed or broke naturally" from "loop threw".

## Proposed Rust form

Rust iterators don't have a direct analogue because `Drop` cannot
distinguish panic from normal scope exit without `std::thread::panicking()`
— which is discouraged. Three options:

**Option A — explicit completion flag checked after the loop, no `Drop` cleanup:**

```rust
fn initial_fetch<'a>(&'a self, req: FetchRequest)
  -> impl Iterator<Item = Node> + 'a
{
  let take_state_key = ...;
  let mut size = 0usize;
  let mut bound: Option<Row> = None;
  // The iterator yields Node items via a custom enum; the *completion*
  // handler runs as the LAST action of the outer orchestrating function.
  let inner = self.input.fetch(req);
  let outer = inner.inspect(|node| {
    size += 1;
    bound = Some(node.row.clone());
  }).take(self.limit);
  // State commit happens inline at the point the caller finishes consuming
  // the iterator, not inside a Drop impl. If the caller panics mid-iterate,
  // the state is intentionally not committed (same as the TS semantics).
  outer  // commit-on-completion is the caller's responsibility
}
```

This doesn't cleanly translate because the TS generator *itself* owns the
commit — the caller only consumes `Node`s.

**Option B — use `scopeguard::guard` with the "defer only on success" pattern:**

```rust
use scopeguard::guard;
let g = guard(/* state */, |_state| {
  // runs unconditionally on drop
});
// ... run loop ...
let _ = scopeguard::ScopeGuard::into_inner(g); // disarm if no panic
// then commit explicitly here
self.set_take_state(...);
```

`scopeguard::ScopeGuard::into_inner` disarms the guard; if a panic unwinds
before that, the guard runs and we can skip the commit. Inverting this
(commit only if no panic) is:

```rust
let commit_on_success = scopeguard::guard((), |_| { /* nothing */ });
// run loop
let _ = scopeguard::ScopeGuard::into_inner(commit_on_success);
self.set_take_state(...); // only reached on success
```

**Option C — structure the commit as the last statement after the loop:**

This is the idiomatic Rust approach for the equivalent semantics. Rust
`?`-propagation and panic-unwind already skip the commit on error, so no
flag is needed:

```rust
let mut size = 0usize;
let mut bound: Option<Row> = None;
for node in self.input.fetch(req) {
  /* yield node to caller, update size/bound */
  if size == self.limit { break; }
}
// Reached only if the loop completed normally (no panic, no `?`-return).
self.set_take_state(take_state_key, size, bound,
                    self.storage.get_max_bound());
```

Option C is the recommended translation. It changes the timing of the
commit only in the panic case: TS's `finally` runs in any-return case that
isn't a throw, including a downstream `return`/`break` of the iterator;
Rust's "statement after the loop" runs in the same cases *except* a panic,
where Rust unwinds past it (which is what the TS code already wanted).

## Classification
**Redesign** — the `finally`-with-`exceptionThrown`-flag idiom does not
have a clean Rust mirror because `Drop` cannot observe panic-vs-normal
without `thread::panicking()`. The right move is to inline the commit after
the loop and rely on panic unwinding to skip it.

## Caveats

- The TS code also `assert(!downstreamEarlyReturn)` inside the `finally`.
  That assertion guards against a consumer calling `.return()` on the
  generator (which would skip the `downstreamEarlyReturn = false` line).
  Rust iterators have no `.return()` — dropping the iterator is the only
  way to stop it early, and it takes the state with it. The assertion
  translates to "if the inner loop didn't run to completion or hit `break`,
  panic". That's a runtime check on `size == self.limit || inner_iter_exhausted`
  after the loop, not a separate flag.
- The sibling pattern `zql-ivm-joins-generator-scope-guard` covers
  "reset a field on scope exit"; the distinction here is that we want the
  commit to happen *only on success*, not always. Different semantics,
  different Rust translation.

## Citation

- `scopeguard::ScopeGuard::into_inner` for disarm-on-success RAII:
  https://docs.rs/scopeguard/latest/scopeguard/struct.ScopeGuard.html#method.into_inner .
- `std::thread::panicking` warning (why relying on it inside `Drop` is
  discouraged):
  https://doc.rust-lang.org/std/thread/fn.panicking.html .
- Rust error-handling RFC on `?` vs panic semantics:
  https://doc.rust-lang.org/book/ch09-02-recoverable-errors-with-result.html .
