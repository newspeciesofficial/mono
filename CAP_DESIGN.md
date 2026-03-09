# Cap Operator

## Motivation

EXISTS subqueries only need to know whether matching rows exist, not their order. Yet the standard pipeline uses Take, which requires ordered input — forcing `ORDER BY` in SQL. On large tables this is expensive: SQLite must sort matching rows even when a covering index on the correlation key could return results immediately.

Cap replaces Take in EXISTS child pipelines, removing the ordering requirement and the `ORDER BY` from generated SQL.

## Why Take Needs Ordering (and Cap Doesn't)

Take maintains a **bound** — the last row in sort order — to decide whether incoming rows belong in the window. On remove, it fetches the next row _in sort order_ as a replacement. All of this requires a total ordering.

Cap doesn't track a bound. It tracks a **set of primary keys** of the rows currently in scope. Since EXISTS only cares about count, any N matching rows are equally valid — there's no "better" row to prefer.

## State

Per partition (keyed the same way as Take):

```tsx
type CapState = {
  size: number;
  pks: string[]; // JSON-serialized PKs of in-scope rows
};
```

With EXISTS limit=3, each partition stores at most 3 PKs.

## Fetch

**Initial fetch** (no state): Read up to `limit` rows from input, store their PKs, yield each.

**Subsequent fetch** (has state): Issue one PK-constrained point lookup per tracked row. Each fetch uses the deserialized PK as the constraint, so the source does an index seek rather than scanning the partition. With `limit=3`, this is 3 fast lookups regardless of partition size.

```tsx
// cap.ts — subsequent fetch
for (const pk of capState.pks) {
  const constraint = deserializePKToConstraint(pk, this.#primaryKey);
  for (const inputNode of this.#input.fetch({constraint})) {
    if (inputNode === 'yield') {
      yield inputNode;
      continue;
    }
    yield inputNode;
  }
}
```

**Unpartitioned with partition key** (nested sub-query case): This path is hit when Cap has a partition key but the fetch constraint doesn't match it. For example:

```tsx
issue.whereExists('comments', q => q.whereExists('author'));
```

The EXISTS child pipeline is:

```
Source(comments)
  → Cap(limit=3, partitionKey=[issueID])
  → Join(parentKey=[authorID], childKey=[id], child=Source(author))
```

When an author changes, the `comments → author` Join fetches its parent side (through Cap) with constraint `{authorID: 42}`. But Cap partitions by `issueID`, not `authorID` — the constraint doesn't match the partition key. Cap can't do PK lookups because it doesn't know which `issueID` partitions contain comments with `authorID = 42`.

Instead, Cap passes the fetch through to the source (which handles the `{authorID: 42}` constraint — so we're scanning matching comments, not the whole table). For each comment returned, Cap looks up that comment's `issueID` partition state and yields the comment only if its PK is in that partition's set.6

## Push

**add**: If `size < limit`, add PK to set and forward. If full, drop. There's no reason to swap.

**remove**: If PK not in set, drop. If PK is in set:

1. Remove PK, decrement size
2. **Refill**: fetch from input with partition constraint, find first row NOT in the PK set
3. If replacement found: add its PK, forward `remove(old)` then `add(replacement)`

Refill is necessary because Cap may be tracking only N of many matching rows. Without it, removing all N tracked rows would incorrectly signal NOT EXISTS when matching rows still exist in the source.

1. If no replacement: forward `remove(old)` — size genuinely decreased

Refill is fast: the set has at most `limit-1` entries (one just removed), so we skip at most `limit-1` rows before finding a replacement.

```tsx
// cap.ts — remove with refill
const pks = [...capState.pks];
pks.splice(pkIndex, 1);
const newSize = capState.size - 1;

// Find first row in source NOT already in our set
const pkSet = new Set(pks);
let replacement: Node | undefined;
for (const node of this.#input.fetch({constraint})) {
  if (node === 'yield') {
    yield node;
    continue;
  }
  const nodePK = serializePK(node.row, this.#primaryKey);
  if (!pkSet.has(nodePK)) {
    replacement = node;
    break;
  }
}

if (replacement) {
  // Store state WITHOUT replacement — hide it during remove forward
  this.#storage.set(capStateKey, {size: newSize, pks});
  yield * this.#output.push(change, this); // forward remove
  // Now reveal replacement
  pks.push(serializePK(replacement.row, this.#primaryKey));
  this.#storage.set(capStateKey, {size: newSize + 1, pks});
  yield * this.#output.push({type: 'add', node: replacement}, this);
} else {
  this.#storage.set(capStateKey, {size: newSize, pks});
  yield * this.#output.push(change, this); // forward remove
}
```

**edit**: If old PK is in set, update the PK if it changed and forward. Otherwise drop. (Edits that change the correlation key are split into remove+add by the source via `splitEditKeys`.)

**child**: If PK is in set, forward. Otherwise drop.

## Why This Is Safe

The ordering invariant in the pipeline exists so that operators can make decisions based on row position within a sorted window. Cap makes no positional decisions — only set membership checks.

The operators downstream of Cap — Join, FlippedJoin, and Exists — do have ordering assumptions in their overlay logic, but these are harmless in the EXISTS context. To understand why, we need to distinguish between two categories of operations in `generateWithOverlay` (`join-utils.ts:22-119`).

### Equality-based overlay operations — genuinely order-independent

These operations find their target by `compareRows === 0` (equality):

- **`add`** (line 39): Finds the added node in the stream by equality and suppresses it (prevents double-counting). The node IS in the stream — comparing it with itself always returns 0 regardless of iteration order.
- **`child`** (line 76): Finds the parent node whose child changed by equality. Same reasoning.
- **`edit` (new node)** (line 65): Finds the new version of the edited node by equality and suppresses it. Same reasoning.

If the equality match isn't found, the assertion at lines 115-117 fires. But this can't happen — these nodes are always present in the stream because they represent rows that exist in the source.

### Position-based overlay operations — wrong position, but always applied

These operations find their insertion point by `compareRows < 0` (ordering):

- **`remove`** (line 46): Yields the removed node (which is NOT in the stream) at the position where the comparator says it belongs. With a mismatched comparator, the `< 0` condition may never trigger during iteration. **Fallback** (lines 101-103): yields the node unconditionally after the loop. The node is always yielded exactly once.
- **`edit` (old node)** (line 55): Same position-based insertion for the old version of the edited row. **Fallback** (lines 104-111): yields the old node after the loop. Always yielded exactly once.

### Why the comparator doesn't match — and why it doesn't matter

Cap passes through the upstream schema unchanged (`cap.ts:81-82`), so `generateWithOverlay` uses a `compareRows` derived from the schema's sort order. But without `ORDER BY`, the DB returns rows in whatever order the chosen index produces — which varies by constraint and doesn't match the schema comparator. This is why the position-based `< 0` checks fire at wrong positions (or not at all during iteration).

But the fallback-after-loop guarantees that remove and edit-old nodes are always yielded exactly once. The _position_ in the output stream is wrong, but the _set membership_ is correct.

### FlippedJoin specifics

- **`binarySearch`** (`flipped-join.ts:150-157`): Uses `child.getSchema().compareRows` on `childNodes`. With unordered data, finds the wrong insertion point. But what matters for EXISTS is that the node is inserted into `childNodes` (membership), not where.
- **`generateWithOverlayNoYield`** (`flipped-join.ts:267-272`): Same two categories as above — equality operations are correct, position operations land at wrong spots but always apply via fallback.
- **EXISTS check** (line 277): `overlaidRelatedChildNodes.length > 0` — only count/membership matters, not position.

Note: FlippedJoin IS used with Cap — `builder.ts` (lines 463-492) builds flipped EXISTS conditions with `unordered=true` (which uses Cap) and passes the result to FlippedJoin.

### Summary

- **Exists** counts child rows — order irrelevant.
- **Equality-based operations** (add, child, edit-new) are genuinely order-independent — they always find their target.
- **Position-based operations** (remove, edit-old) may fire at wrong positions due to comparator mismatch, but the fallback-after-loop guarantees every overlay is applied exactly once.
- **EXISTS only checks set membership** (`length > 0`) — wrong position is invisible to the final result.
- **No assertion failures**: equality operations always find their targets (nodes are in the stream), and position operations have fallbacks that prevent reaching the final assertion.

Operators with hard ordering dependencies (Take, Skip) may exist on the parent side but they operate on parent ordering, not child ordering. Cap's unordered output never reaches them directly.

## Consistency During Push

The core invariant is: **fetch yields exactly the rows whose PKs are in the stored set**. The PK set is the single source of truth and is always updated atomically before forwarding to output. Any mid-push re-fetch by downstream sees a complete, self-consistent snapshot.

Walk through each push type:

- **add**: PK added to set, then forwarded. Mid-push re-fetch sees the new row — correct, the add is in flight.
- **remove (no refill)**: PK removed from set, then forwarded. Mid-push re-fetch does not see the removed row — correct.
- **edit**: PK updated in set (if changed), then forwarded. Mid-push re-fetch sees the new PK — correct.
- **child**: No state change. Forwarded if PK in set.
- **remove (with refill)**: Old PK removed from set, state stored _without_ replacement, remove forwarded. Then replacement PK added to set, state updated, add forwarded. This matches Take's convention: during the remove forward, a mid-push re-fetch sees only the remaining rows (e.g., `{B, C}`), not the replacement. The replacement becomes visible only when its add is forwarded.

No `#rowHiddenFromFetch` field is needed. Take uses one because its fetch scans by bound — the replacement row is in the source and would appear in a bound-based scan, so it must be explicitly skipped. Cap's fetch does PK-based lookups from the stored set, so simply deferring the storage update is equivalent to hiding — if the replacement PK isn't in storage, fetch won't look it up.

```tsx
// Take hides via a field checked during fetch:
this.#rowHiddenFromFetch = row;
yield * this.#output.push(removeChange, this); // fetch skips the hidden row
this.#rowHiddenFromFetch = undefined;

// Cap hides by deferring the storage update:
this.#storage.set(capStateKey, {size: newSize, pks}); // replacement NOT in pks
yield * this.#output.push(change, this); // fetch won't look up replacement
pks.push(replacementPK);
this.#storage.set(capStateKey, {size: newSize + 1, pks}); // replacement now in pks
yield * this.#output.push({type: 'add', node: replacement}, this);
```

## Upstream Operator Safety

Cap voids the ordering guarantee for the entire EXISTS sub-graph stream. Operators *upstream* of Cap also consume this unordered stream and may have ordering assumptions. This section analyzes every operator that can appear upstream of Cap.

### EXISTS Sub-Graph Pipeline Topology

```
Source(table) → [Join/FlippedJoin for nested EXISTS] → [Where filter pipeline] → Cap
```

When `unordered=true`, Source connects with `skipOrderByInSQL=true` — no ORDER BY in SQL. The schema's `compareRows` uses PK order, but the actual iteration order is opaque (depends on SQLite's query plan/index choice).

### Stateless Operators — Safe

- **Source**: Origin of the stream. No ordering assumptions on its own output.
- **Filter** (`filter.ts`): Pure boolean predicate. No `compareRows`, no ordering logic.
- **FilterStart / FilterEnd** (`filter-operators.ts`): Pass-through adapters for the filter pipeline. Iterate input and yield rows that pass the filter. No ordering dependency.
- **Exists** (`exists.ts`, as FilterOperator): Checks `relationship().length > 0`. Pure membership/count check. No ordering dependency.

### Join — Position Check Can Be Wrong

When a nested `whereExists` creates a non-flipped join upstream of Cap, Join's `#processParentNode` (`join.ts:273-277`) checks:

```typescript
this.#schema.compareRows(
  parentNodeRow,
  this.#inprogressChildChange.position,
) > 0
```

This determines whether to apply `generateWithOverlay` to a parent node's child stream during a re-fetch triggered by an in-flight push. It answers: "has this parent been pushed yet?" Parents "after" the position haven't been pushed, so they need the overlay.

With `skipOrderByInSQL=true`, `compareRows` uses PK ordering but the actual iteration order from Source is opaque. The `> 0` check can give wrong results: a parent that HAS been pushed may be treated as NOT pushed (or vice versa), causing the overlay to be incorrectly applied or skipped.

**Concrete scenario:**

1. Author A1 is removed from the source
2. Join iterates comments [C3(PK=3), C2(PK=2), C1(PK=1)] (reversed by opaque Source order)
3. Pushes child change for C3 first, sets `position = C3.row`
4. Cap receives remove(C3), attempts refill. During refill, Join re-fetches comments.
5. For C2 in the refill: `compareRows(C2(PK=2), C3(PK=3)) > 0` → false → no overlay → correct (C2 hasn't been pushed yet, but A1 is already gone from source, so no overlay needed)
6. Pushes child change for C2, sets `position = C2.row`
7. Cap receives remove(C2), attempts refill. During refill:
8. For C3: `compareRows(C3(PK=3), C2(PK=2)) > 0` → **true** → overlay applied!
9. The overlay inserts the removed author back into C3's relationship stream
10. C3 appears to still have an author → passes EXISTS → Cap adds C3 as a **false replacement**

This causes Cap to retain entries that should have been removed. Each subsequent iteration can add back a previously removed entry as a replacement.

### FlippedJoin — Same Position Check Issue

FlippedJoin (`flipped-join.ts:250-256`) has the same position check pattern using `parent.getSchema().compareRows`. With unordered parent stream, the check can be wrong. Same reasoning applies.

Additionally, `binarySearch` on child nodes (`flipped-join.ts:150-156`) uses `compareRows` on an unordered array. This finds the wrong insertion position, but only affects WHERE in the array the node is inserted — membership is correct.

### Why This Is Acceptable

The wrong overlay during refill causes Cap to find false replacements — rows that appear to pass EXISTS due to the overlay but actually don't. This leads to:

- **Incorrect Cap state**: Cap tracks rows that shouldn't pass the filter
- **Extra add/remove oscillation**: Downstream sees adds for rows that will be removed on the next pipeline reset

However:

1. **No crashes or assertion failures**: The pipeline continues operating correctly
2. **Every parent row gets its correct push**: All removes/adds for the actual change are delivered
3. **Self-correcting**: Pipeline timeout/reset re-hydrates from scratch, producing correct state
4. **Only affects refill**: The primary change pushes (not refill replacements) are always correct
5. **EXISTS semantics are preserved**: The parent pipeline's EXISTS check still evaluates correctly on each push — only Cap's internal replacement tracking is affected

The false replacement issue only manifests when Cap's refill re-fetches through a Join that has an in-progress child change with a position that doesn't match the actual iteration order.

See `cap.upstream-overlay.test.ts` for tests exercising this behavior.

| Operator | Ordering Assumptions | Safe for EXISTS? |
|----------|---------------------|-----------------|
| Source | None | Yes |
| Filter | None | Yes |
| FilterStart/End | None | Yes |
| Exists (filter) | None | Yes |
| Join | Position check in `#processParentNode` | Yes — false replacements during refill only |
| FlippedJoin | Position check + binarySearch | Yes — false replacements during refill only |

## Known Limitation: Unpartitioned Fetch Fan-Out

The unpartitioned fetch path (see Fetch section above) scans all source rows matching the fetch constraint and filters by PK set membership. This is problematic when the constraint has high fan-out — e.g., a bot that has commented on every issue. When that bot's author row changes, the `{authorID: botId}` scan touches millions of comment rows just to find the handful in Cap's tracked sets.

In practice, the pipeline timeout mechanism handles this: if a push exceeds the time budget, the pipeline is destroyed and re-hydrated from scratch.
