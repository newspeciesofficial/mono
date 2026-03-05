# Why `generateWithOverlay` is safe with unordered Cap streams

## Context

Cap is used in EXISTS subqueries and doesn't enforce iteration order. But
`generateWithOverlay` (in `join-utils.ts`) uses `schema.compareRows` to
position overlay nodes — and Cap passes through the upstream schema's
`compareRows` unchanged (`cap.ts:81-82`), so the comparator may not match
the actual iteration order. This document explains why that's safe.

## What `generateWithOverlay` does

This function is called by Join/FlippedJoin during `push` to overlay an
in-flight change onto a fetched stream:

- **add**: The row was just added to the source, so it already appears in
  the fetch. Suppress it (the join processes it separately as a change).
- **remove**: The row was just removed from the source, so it's missing
  from the fetch. Re-insert it (the join needs the pre-change state).
- **child**: A child relationship changed. Replace the matching node with
  one carrying the modified relationship stream.
- **edit**: The new version is in the fetch (suppress it) and the old
  version is missing (re-insert it). Effectively a remove + add in one.

## The two categories of operation

### Equality-based: `compareRows === 0`

Used by: **add** (line 39), **child** (line 76), **edit-new** (line 65)

These scan every node in the stream (until `applied` is set) checking for
equality. It doesn't matter what order the nodes arrive in — if the target
row is in the stream, the `=== 0` check will find it. It's a linear scan
with an equality predicate. Order is irrelevant.

`compareRows` is derived from the schema's sort ordering, which always
includes the primary key (enforced by `assertOrderingIncludesPK`). So
`compareRows(a, b) === 0` means "same PK" — a unique identity check that
will match exactly one row in any ordering.

### Position-based: `compareRows < 0`

Used by: **remove** (line 46), **edit-old** (line 55)

In a correctly-sorted stream, `compareRows < 0` finds the insertion point —
the first node that sorts after the overlay node. This is the part that
breaks with wrong order. When the stream order doesn't match `compareRows`:

- `< 0` might fire on the **wrong** node (inserting at an arbitrary position)
- `< 0` might **never** fire (if no node happens to compare greater)

But it doesn't matter — the fallback at lines 100-112 catches both cases:

- **If the in-loop check fires**: `applied = true`, overlay node yielded
  once, fallback skipped.
- **If the in-loop check never fires**: `applied` stays false, fallback
  yields the overlay node once.

Either path yields the overlay node **exactly once**. The only difference is
where in the output it appears.

## Why wrong position is safe

Cap is only used in EXISTS subqueries. EXISTS checks `length > 0` — is the
set non-empty? The downstream consumer never looks at individual row
positions, only set membership.

The contract `generateWithOverlay` must satisfy for Cap is:

1. **Equality ops** (add, child, edit-new): The target row must be found
   and handled. Guaranteed by linear scan with `=== 0`.
2. **Position ops** (remove, edit-old): The missing row must appear in the
   output exactly once. Guaranteed by the in-loop-or-fallback pattern.
3. **No duplicates**: Equality ops set `applied = true` on match, preventing
   double processing. Position ops can only fire once (either in-loop or
   fallback, never both).

The output set is always correct — same rows, same count. Only the ordering
may be wrong. And EXISTS doesn't care about ordering.

## Pipeline topology: Cap is always terminal

EXISTS callbacks only allow `.where()` (filters) and `.exists()` (nested
EXISTS) — never `.related()`, `.start()`, `.limit()`, or `.orderBy()`. This
constrains the pipeline shape for any EXISTS subquery to:

```
Source → [nested EXISTS Joins + Exists filters]* → Cap
```

Cap is **always the last operator** in an EXISTS pipeline. Nothing receives
Cap's unordered output except the parent pipeline's Join (or FlippedJoin),
which consumes it as a **child** input stream.

### Nested EXISTS

For nested EXISTS like `exists(a, q => q.exists(b))`:

- `a`'s pipeline is: `Source(a) → Join(b) → Cap`
- `b`'s pipeline is: `Source(b) → Cap`

The Join for `b` sits **before** Cap in `a`'s pipeline (`builder.ts` lines
314-335). Cap'd `b` pipeline feeds into this Join as its **child** input.

Both push directions through this inner Join are safe:

- **Parent-side push** (Source(a) changes): Join fetches from the child
  (b's Cap'd pipeline) and calls `generateWithOverlay` on that unordered
  child stream — safe by the equality/fallback analysis above.
- **Child-side push** (Source(b) changes): the push goes through b's Cap,
  then into Join(b). Join fetches from the parent (Source(a)), which is
  properly ordered — `generateWithOverlay` works correctly.

Join(b) then pushes its results to Cap in `a`'s pipeline, which passes
them up to the outer parent Join. That outer Join uses `generateWithOverlay`
on `a`'s Cap'd stream — the same safe scenario again. Each nesting level
has the identical structure (ordered parent, unordered Cap'd child), so the
argument applies recursively at any depth.

### FlippedJoin

When an EXISTS condition is "flipped" (`builder.ts:473` via
`applyFilterWithFlips`), FlippedJoin replaces Join, with the Cap'd pipeline
as its child. FlippedJoin has three interactions with the Cap'd child stream:

1. **`generateWithOverlayNoYield`** (`flipped-join.ts:267`): Called on
   `relatedChildNodes` with child schema — identical to `generateWithOverlay`,
   safe by the equality/fallback analysis.
2. **`binarySearch` for remove** (`flipped-join.ts:150-156`): Re-inserts a
   removed child into `childNodes[]` using `compareRows`. If children are
   unordered (Cap), insertion position may be wrong. But the position in
   `childNodes` only determines which parent iterator slot the child maps
   to — the parent merge (`flipped-join.ts:199-221`) uses `compareRows` on
   **parent** rows to produce correctly-ordered output.
3. **Position tracking** (`flipped-join.ts:250-256`): The
   `hasInprogressChildChangeBeenPushedForMinParentNode` check uses
   `this.#parent.getSchema().compareRows` on **parent** rows, not child
   rows. The parent is always ordered.

Whether the inner join, outer join, or both are flipped in a nested EXISTS
doesn't change the analysis. Each FlippedJoin has the same structure: an
ordered parent and an unordered Cap'd child. The safety properties hold at
every level.

## The Cap passthrough

Cap returns the upstream schema unchanged (`cap.ts:81-82`), including
`compareRows`. When Join calls `generateWithOverlay` with Cap's schema, it
gets a comparator that reflects the logical sort order — but Cap's `fetch`
may return rows in a different order (PK-based point lookups iterate in
PK-set insertion order, not sort order).

The comparator is "wrong" for the actual iteration order, but the equality
checks don't care about order, and the position checks have a fallback that
guarantees correctness regardless.

## Tests

See `generate-with-overlay-unordered.test.ts` for tests that verify these
claims by feeding deliberately mis-ordered streams through `generateWithOverlay`.
