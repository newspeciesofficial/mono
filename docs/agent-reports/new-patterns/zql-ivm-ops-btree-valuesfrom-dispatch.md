# New pattern: Dynamic-method-name dispatch on a sorted set

## Category
A (Language patterns) — it's an idiom-swap that's trivial to translate but
worth documenting because it interacts with the custom-cmp BTreeSet pattern
the sibling already flagged.

## Where used
- `packages/zql/src/ivm/memory-source.ts:806-814` — `generateRows`:
  `data[reverse ? 'valuesFromReversed' : 'valuesFrom'](scanStart as Row | undefined);`

## TS form

```ts
function* generateRows(
  data: BTreeSet<Row>,
  scanStart: RowBound | undefined,
  reverse: boolean | undefined,
) {
  yield* data[reverse ? 'valuesFromReversed' : 'valuesFrom'](
    scanStart as Row | undefined,
  );
}
```

This picks between two methods on the custom `BTreeSet`
(`shared/src/btree-set.ts`) by a runtime boolean and delegates to the
returned iterator.

## Proposed Rust form

```rust
fn generate_rows<'a>(
  data: &'a BTreeSet<Row>,
  scan_start: Option<&'a Row>,
  reverse: bool,
) -> Box<dyn Iterator<Item = Row> + 'a> {
  if reverse {
    Box::new(data.values_from_reversed(scan_start))
  } else {
    Box::new(data.values_from(scan_start))
  }
}
```

Or, if the two iterators have different concrete types, return an
`itertools::Either` or an `enum { Asc(I1), Desc(I2) }` that implements
`Iterator`.

## Classification
**Direct** — plain if/else. The only reason it's a "new pattern" is that
the API surface of the target sorted-set crate must expose *both*
directions with a start point. `std::collections::BTreeSet::range` takes a
`Bound` and `rev()` on the returned iterator gives the reverse direction,
so this can be spelled with one method:

```rust
// with std BTreeSet<Row> (if custom cmp is wrapped):
let base = data.range(scan_start..);
if reverse { base.rev() } else { base }  // returns a concrete iterator type;
                                         // use impl Iterator or Either
```

## Caveats

- If the chosen Rust sorted-set does *not* expose a "seek-then-iterate-in-
  direction" primitive (for example `indexmap`'s sorted map doesn't), then
  the "seek" must be a linear scan + skip, which is O(n) not O(log n) and
  breaks the TS performance contract.
- `std::collections::BTreeSet::range()` takes `std::ops::Bound`, which maps
  cleanly onto the sentinel design in
  `zql-ivm-ops-bound-sentinel.md` (`Bound::Min` → `Bound::Unbounded` only
  in the first column; subsequent columns need the explicit enum).
- Return-type variance in Rust means the `if/else` branches must either
  both return `impl Iterator` with the same concrete type (impossible for
  `.rev()` vs. forward), or be boxed (`Box<dyn Iterator<Item=Row>>`), or
  use `itertools::Either`.

## Citation

- `std::collections::BTreeSet::range` (the closest stdlib analogue):
  https://doc.rust-lang.org/std/collections/struct.BTreeSet.html#method.range .
- `itertools::Either` for type-unifying branching iterators:
  https://docs.rs/itertools/latest/itertools/enum.Either.html .
