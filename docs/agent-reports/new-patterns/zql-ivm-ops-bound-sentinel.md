# New pattern: Min/Max sentinel values in sort-order keys

## Category
D (Data / state patterns).

## Where used
- `packages/zql/src/ivm/memory-source.ts:766-771` — sentinel declarations
- `packages/zql/src/ivm/memory-source.ts:787-804` — `compareBounds`
- `packages/zql/src/ivm/memory-source.ts:303-318` — constructing scan-start `RowBound` with sentinels for columns not present in the constraint
- `packages/zql/src/ivm/memory-source.ts:773-785` — `makeBoundComparator`

## TS form

```ts
type Bound = Value | MinValue | MaxValue;
type RowBound = Record<string, Bound>;
const minValue = Symbol('min-value');
type MinValue = typeof minValue;
const maxValue = Symbol('max-value');
type MaxValue = typeof maxValue;

function compareBounds(a: Bound, b: Bound): number {
  if (a === b) return 0;
  if (a === minValue) return -1;
  if (b === minValue) return 1;
  if (a === maxValue) return 1;
  if (b === maxValue) return -1;
  return compareValues(a, b);
}

// Usage: build a scan-start row where columns not in the constraint are
// pinned to the extreme in the direction of iteration, so the btree seek
// starts at the first row matching the constrained columns.
if (hasOwn(fetchOrPkConstraint, key)) {
  scanStart[key] = fetchOrPkConstraint[key];
} else {
  if (req.reverse) scanStart[key] = dir === 'asc' ? maxValue : minValue;
  else             scanStart[key] = dir === 'asc' ? minValue : maxValue;
}
```

## Proposed Rust form

```rust
#[derive(Clone, Debug)]
enum Bound {
  Min,
  Value(Value),
  Max,
}

impl Ord for Bound {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (self, other) {
      (Bound::Min, Bound::Min) | (Bound::Max, Bound::Max) => Equal,
      (Bound::Min, _) => Less,
      (_, Bound::Min) => Greater,
      (Bound::Max, _) => Greater,
      (_, Bound::Max) => Less,
      (Bound::Value(a), Bound::Value(b)) => compare_values(a, b),
    }
  }
}

type RowBound = std::collections::HashMap<String, Bound>;
```

Used identically to the TS version — to build a scan-start key for seeking
into the sorted index at the first row matching the constrained prefix.

## Classification
**Direct (Idiom-swap)** — Rust has no runtime sentinel value like
`Symbol()`; the idiomatic translation is a three-variant enum with a
hand-written `Ord`. Once translated, the call sites mechanically become
pattern matches on `Bound`.

## Caveats

- `compareValues` (from `packages/zql/src/ivm/data.ts`) treats `null === null`
  and uses `compareUTF8`. `Bound::Value(Value)` must forward to an equivalent
  total-order comparator including `Value::Null` < any other value. Do not
  derive `Ord` on `Value` without verifying this.
- The TS code uses `Symbol` *specifically* to guarantee uniqueness from any
  user value (no `Value` can ever be `=== minValue`). The Rust enum
  guarantees the same thing structurally — no confusion possible because
  `Bound::Min` is nominally distinct from `Bound::Value(_)`.
- Do *not* model these as `Bound::Value(Value::Null)` — `null` is a legal
  user value with well-defined ordering, whereas `Min`/`Max` are
  out-of-band.

## Citation

- Rust `std::ops::Bound` (for range queries) shows the standard-library
  precedent for a three-variant bound enum, though its use case differs
  (inclusive/exclusive/unbounded, not min/max values):
  https://doc.rust-lang.org/std/ops/enum.Bound.html .
- `std::cmp::Ord` hand implementation:
  https://doc.rust-lang.org/std/cmp/trait.Ord.html .
