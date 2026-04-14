# New pattern: Epoch-versioned per-source overlay for reentrant fetch during push

## Category
D (Data / state patterns) — this is an IVM correctness mechanism, not a
language or runtime feature.

## Where used
- `packages/zql/src/ivm/memory-source.ts:53-56` (`Overlay` / `Overlays` types)
- `packages/zql/src/ivm/memory-source.ts:100-101` (`#overlay`, `#pushEpoch` on `MemorySource`)
- `packages/zql/src/ivm/memory-source.ts:69-83` (`Connection.lastPushedEpoch`)
- `packages/zql/src/ivm/memory-source.ts:365-379` (`*genPush` sets overlay per-connection)
- `packages/zql/src/ivm/memory-source.ts:514-570` (`*genPush` splices overlay, clears via `setOverlay(undefined)` at end)
- `packages/zql/src/ivm/memory-source.ts:615-759` (`generateWithOverlay` / `computeOverlays` / `generateWithOverlayInner`)
- `packages/zql/src/ivm/take.ts:55-56,110-118,688-695` (sibling pattern: per-operator `#rowHiddenFromFetch` overlay)

## TS form

```ts
// MemorySource owns an overlay describing the in-flight write plus an epoch.
#overlay: Overlay | undefined;
#pushEpoch = 0;

*genPush(change: SourceChange) {
  // For each connected output, set overlay to the pending write, tagged
  // with this push's epoch, then invoke output.push. If the downstream
  // operator calls back into input.fetch during push, generateWithOverlay
  // splices the overlay into the fetch stream so the downstream sees the
  // change as if it were already committed.
  for (const conn of this.#connections) {
    conn.lastPushedEpoch = pushEpoch;
    setOverlay({epoch: pushEpoch, change});
    yield* filterPush(outputChange, output, input, filters?.predicate);
    yield undefined;
  }
  setOverlay(undefined);       // clear
  writeChange(change);         // then actually mutate the btree
}

// fetch consults #overlay, guarded by the connection's lastPushedEpoch so
// that older overlays (from a still-executing outer push on a different
// connection) are ignored.
function* generateWithOverlay(startAt, rows, constraint, overlay,
                              lastPushedEpoch, compare, filterPredicate) {
  const overlayToApply = overlay && lastPushedEpoch >= overlay.epoch
    ? overlay : undefined;
  // splice overlay.add before the first row > it, and skip overlay.remove
  yield* generateWithOverlayInner(rows, computeOverlays(...), compare);
}

// Take uses a simpler variant: a single "hidden row" the operator sets
// around a nested output.push so that any recursive input.fetch inside the
// pushed subtree skips that row.
#rowHiddenFromFetch: Row | undefined;
*#pushWithRowHiddenFromFetch(row: Row, change: Change) {
  this.#rowHiddenFromFetch = row;
  try { yield* this.#output.push(change, this); }
  finally { this.#rowHiddenFromFetch = undefined; }
}
```

## Proposed Rust form

```rust
struct Overlay { epoch: u64, change: SourceChange }

struct MemorySource {
  // ... indexes, connections ...
  overlay: std::cell::Cell<Option<Overlay>>, // or RefCell if inspecting
  push_epoch: std::cell::Cell<u64>,
}

impl MemorySource {
  fn gen_push<'a>(&'a self, change: SourceChange)
    -> impl Iterator<Item = YieldToken> + 'a
  {
    // yielded as a hand-rolled Iterator impl (not a generator — see
    // zql-ivm-joins-cooperative-yield-token for the sentinel design pick).
    // For each connection:
    //   conn.last_pushed_epoch.set(epoch);
    //   self.overlay.set(Some(Overlay { epoch, change: change.clone() }));
    //   // drive downstream output.push, bubbling YieldToken::Yield up
    // self.overlay.set(None);
    // self.write_change(&change);
    todo!()
  }
}

fn generate_with_overlay<'a>(
  start_at: Option<Row>,
  rows: impl Iterator<Item = Row> + 'a,
  constraint: Option<&'a Constraint>,
  overlay: Option<&'a Overlay>,
  last_pushed_epoch: u64,
  compare: impl Fn(&Row, &Row) -> std::cmp::Ordering + 'a,
  filter: Option<impl Fn(&Row) -> bool + 'a>,
) -> impl Iterator<Item = Node> + 'a { /* splice logic */ todo!() }
```

Because this must be called while another `&mut self` borrow is live (push
calls into output which calls back into fetch), the overlay lives behind
`Cell`/`RefCell` and is cleared unconditionally at the end of `gen_push`.
For the Take per-operator variant, a `RAII` scopeguard (`scopeguard::defer!`)
or a `Drop`-implementing helper struct replaces the `try/finally`.

## Classification
**Redesign** — the shape is idiosyncratic to the IVM algorithm and the "set,
iterate, clear" sequence must be preserved exactly. Any missed clear leaves
stale overlays visible to the next fetch; any clear-before-output-completes
breaks reentrant fetch during push. The epoch check
(`lastPushedEpoch >= overlay.epoch`) is specifically there to handle the
case where connection A's push triggers connection B's fetch while B has
not yet observed the push — in that case B's epoch is stale and it must
ignore the overlay. There is no direct Rust analogue because no Rust IVM
crate ships a comparable mechanism.

## Caveats

- The overlay is applied *in memory*, by splicing into the iterator. In
  Rust, the iterator lifetime must outlive the borrow of `self.overlay`,
  which is easiest with `Cell<Option<Overlay>>` + cloning per yield, not
  `RefCell` with a long-lived borrow (which would conflict with write).
- `Take.#rowHiddenFromFetch` is structurally similar but is *per-operator*
  and *single-row*. It does not need an epoch because only one push is ever
  in flight inside a given Take instance.
- Panic safety: the TS code clears overlay via `setOverlay(undefined)` as
  the last statement of `*genPush`, **not** in a `finally`. If the
  downstream push throws, the overlay will be left set. This appears to be
  intentional (the whole pipeline is considered poisoned after a throw) —
  the Rust port should decide explicitly whether to mirror that or use
  RAII (`scopeguard`) to always clear.
- The per-connection `lastPushedEpoch` is a cross-connection coordination
  mechanism; it has no analogue in any standard Rust collection.

## Citation

- Rocicorp IVM design overview: discussions of Zero's incremental view
  maintenance (Rocicorp Zero docs) — https://zero.rocicorp.dev/docs/query-ast
  (explains that pipelines are reentrant during push).
- `scopeguard` crate for RAII-style cleanup that the `try/finally` maps to:
  https://docs.rs/scopeguard/latest/scopeguard/ .
- `std::cell::Cell` reentrant-mutation pattern:
  https://doc.rust-lang.org/std/cell/struct.Cell.html .
