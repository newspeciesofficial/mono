# Validation report: zql-ivm-joins

## Scope
- `packages/zql/src/ivm/join.ts` — 296
- `packages/zql/src/ivm/exists.ts` — 270
- `packages/zql/src/ivm/fan-out.ts` — 82
- `packages/zql/src/ivm/fan-in.ts` — 93
- `packages/zql/src/ivm/join-utils.ts` — 167 (helper)
- `packages/zql/src/ivm/flipped-join.ts` — 491 (helper; the other "join" operator)
- `packages/zql/src/ivm/union-fan-out.ts` — 56 (helper)
- `packages/zql/src/ivm/union-fan-in.ts` — 292 (helper)
- `packages/zql/src/ivm/memory-storage.ts` — 50 (helper)
- Also consulted (read only enough to understand join semantics, not counted):
  `operator.ts`, `filter-operators.ts`, `change.ts`,
  `push-accumulated.ts` (first ~120 LOC).
- Total LOC in scope: **1797**
- `correlated-subquery.ts` does not exist under `packages/zql/src/ivm/`. The
  "correlated subquery" abstraction is an AST/builder concept
  (`zero-protocol/src/ast.ts`, `zql/src/builder/*`) that lowers to the IVM
  operators above (`Join`/`FlippedJoin` + optional `Exists` + optional
  `FanOut`/`FanIn`).

## Summary
| metric | count |
|---|---|
| TS constructs used | 22 |
| Constructs covered by guide | 19 |
| Constructs NOT covered | 3 |
| Libraries imported | 1 (`compare-utf8`, via `memory-storage.ts`) |
| Libraries covered by guide | 1 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 0 (but see "Out-of-guide scope") |

> Note on guide scope. `docs/rust-translation-guide.md` explicitly states that
> its scope is `packages/zero-cache/src/**` and that **zql is out of scope**.
> This report therefore uses the guide's A–G catalogue as the *closest*
> reference for each TS construct, but flags as NEW every construct whose
> semantics are materially IVM-specific and have no guide row (see
> "Ready-to-port assessment").

## Constructs used
| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class` with `#private` fields + public methods | `join.ts:43`, `exists.ts:19`, `fan-out.ts:16`, `fan-in.ts:29`, `flipped-join.ts:45`, `union-fan-in.ts:23`, `memory-storage.ts:17` | A1, A23 | covered |
| Interface implemented by class (`implements Input`, `implements FilterOperator`, `implements Storage`) | `join.ts:43`, `exists.ts:19`, `fan-out.ts:16`, `memory-storage.ts:17` | A5 | covered |
| Constructor argument destructuring with inline typed object (`type Args = {...}`) | `join.ts:22`, `flipped-join.ts:25` | A10 (`Record`/struct) | covered |
| Readonly fields (`readonly #parent: Input`) | `join.ts:44`, `flipped-join.ts:46`, `fan-in.ts:30` | A1 | covered |
| Tagged-union switch on `change.type` + `unreachable(change)` exhaustiveness | `join.ts:120-188`, `exists.ts:111-209`, `flipped-join.ts:312-333`, `flipped-join.ts:455-489` | A6 | covered |
| Synchronous generator functions (`function*`, `yield`, `yield*`) | `join.ts:110`, `join.ts:120`, `exists.ts:78`, `exists.ts:107`, `fan-out.ts:62`, `fan-out.ts:73`, `fan-in.ts:66`, `fan-in.ts:75`, `flipped-join.ts:116`, `flipped-join.ts:312`, `join-utils.ts:22`, `memory-storage.ts:36`, `union-fan-in.ts:100`, `union-fan-in.ts:111`, `union-fan-in.ts:218` (`mergeFetches`) | A22 (Symbol.iterator / impl Iterator) | covered (note: plain sync generators — not `async function*`, so A16's async-stream does *not* apply) |
| `Generator<'yield', boolean>` with discriminated yielded value | `exists.ts:78`, `exists.ts:224`, `exists.ts:246`, `exists.ts:253`, `filter-operators.ts:37` | — | NEW — see `new-patterns/zql-ivm-joins-cooperative-yield-token.md` |
| `yield* iterable` delegation (both for propagating `'yield'` tokens and for pushing further streams) | `join.ts:111-117`, `exists.ts:95`, `fan-out.ts:74-81`, `union-fan-in.ts:111-117` | A22 | covered (mechanical once iterator is built) |
| Manual iterator protocol (`stream[Symbol.iterator]()`, `iter.next()`, `iter.return?.()`, `iter.throw?.()`) in a merge/zip loop | `flipped-join.ts:183-197`, `flipped-join.ts:289-308`, `union-fan-in.ts:222-291` | A22 | covered shape-wise, but the *try/finally + iter.return?.()/iter.throw?.() cleanup protocol* is idiosyncratic | see `new-patterns/zql-ivm-joins-manual-iterator-cleanup.md` |
| Spread in object literal to build new Node: `{...node, relationships: {...node.relationships, [name]: stream}}` | `join.ts:288-294`, `flipped-join.ts:278-284`, `flipped-join.ts:386-414`, `exists.ts:146-150`, `exists.ts:188-195` | A13, A20 | covered |
| Computed-property-name in object literal (`[this.#relationshipName]: stream`) | `join.ts:292`, `flipped-join.ts:282`, `exists.ts:149`, `exists.ts:192` | A19 (dynamic keys → `HashMap`) | covered |
| `Record<string, () => Stream<Node \| 'yield'>>` (relationships map where values are thunks) | `join.ts:255`, `exists.ts:254` (`node.relationships[name]()`) | A10 (Record) + A22 | covered structurally, but the "relationship value is a zero-arg thunk that returns a fresh stream each time" is a Node shape — see `new-patterns/zql-ivm-joins-relationship-thunk-shape.md` |
| `Map<string, boolean>` used as a per-fetch cache that is cleared in `endFilter()` | `exists.ts:25`, `exists.ts:49`, `exists.ts:74` | D1 covers Map-with-TTL via `moka`; this Map has no TTL, just a bounded scope | covered — use `HashMap<String, bool>` |
| Assertions (`assert(cond, msg)` and `unreachable(change)`) | `join.ts:64`, `join.ts:162`, `exists.ts:51`, `exists.ts:108`, `flipped-join.ts:66`, `union-fan-in.ts:52` | A6 (`unreachable` → match exhaustiveness), plus F1 `thiserror`/`panic!` | covered |
| `try { … } finally { … }` used to always reset a state flag (`this.#inPush = false`, `this.#inprogressChildChange = undefined`) | `exists.ts:108-212`, `join.ts:221-250`, `flipped-join.ts:337-419`, `flipped-join.ts:287-308` | A15 (Rust uses `Drop`/RAII or scopeguard) | covered idiomatically, but "reset a field on scope exit from a generator that may be abandoned" is the key constraint — see `new-patterns/zql-ivm-joins-generator-scope-guard.md` |
| `JSON.stringify` of a small array of normalized values, used as a cache key | `exists.ts:234` | E1 / E2 (JSON) | covered (mechanical: `serde_json::to_string` on a `Vec<NormalizedValue>`, or a hand-rolled compound key) |
| `BTreeSet<Entry>` with a custom comparator | `memory-storage.ts:18` | D1 / D2 — no exact row | **not covered** — `BTreeSet` from `shared/src/btree-set.ts` is a project-internal sorted set with a custom comparator; Rust `std::collections::BTreeSet` does *not* take a closure comparator | see `new-patterns/zql-ivm-joins-btreeset-custom-cmp.md` |
| `structuredClone` | `memory-storage.ts:48` | — (no row; closest is A20 object spread) | **not covered for arbitrary JSON values** — Rust equivalent is "deep clone via `serde_json::Value::clone()` / `Clone::clone` on `JSONValue`", which is trivial because `JSONValue` is a plain enum tree, but the guide does not list `structuredClone` | see `new-patterns/zql-ivm-joins-structured-clone.md` |
| `compareUTF8` from `compare-utf8` package | `memory-storage.ts:1,9` | Part 2 row: `compare-utf8` → built-in `str::cmp` | covered |
| `emptyArray` / `identity` sentinels, `must(x, msg)` helper, `areEqual(a,b)` for arrays | `fan-in.ts:2`, `fan-in.ts:72`, `fan-out.ts:1,78`, `exists.ts:1,59` | A1/F1 (internal utility) | covered (these are local helpers; Rust forms are `&[]`, `std::convert::identity`, `.expect(msg)`, `slice::eq`) |
| `binarySearch(n, probe)` from `shared/src/binary-search.ts` | `flipped-join.ts:153-156` | — | covered — Rust `slice::binary_search_by` / `partition_point` |
| Reentrancy guard on a method (`assert(!this.#inPush)` in `exists.push`) | `exists.ts:108` | A18 (closures & mutation) | covered — translates to an `AtomicBool`/field toggle check |
| `emptyArray[Symbol.iterator]()` used as a "no results" iterator | `flipped-join.ts:174` | A22 | covered — `std::iter::empty()` |

## Libraries used
| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `compare-utf8` | built-in `str::cmp` (Part 2: "Valid UTF-8 byte order == codepoint order. No crate needed.") | covered |

Everything else is internal to the monorepo (`shared/*`, `zero-protocol/*`) and
not a third-party dep.

## Risk-flagged items depended on
- None from Part 3 (`pgoutput`, BEGIN CONCURRENT, `arbitrary_precision`, OTEL
  churn, async-trait) are imported by the IVM join/exists/fan-* code path.
- Indirect dependency surface that is flagged elsewhere: the IVM operators
  consume `Source` / `MemorySource` (not in scope), whose storage backend in
  the server path is SQLite — `docs/ts-vs-rs-comparison.md` row 6 notes
  "Not using BEGIN CONCURRENT" as a Rust-side deviation.

## Ready-to-port assessment

The join / exists / fan-out / fan-in operators are **data-structure-heavy but
semantically mechanical**. Almost everything maps to `impl Iterator for T`
(A22), `match` on a `Change` enum (A6), `Box<dyn FilterInput>`/`Box<dyn Input>`
trait objects (A3/A5), and struct-update syntax (A13). There is no async, no
I/O, no worker_threads, no EventEmitter and no protocol work in this slice.

However, the code is **not ready to port today** under the current guide, for
four reasons:

1. **The guide explicitly excludes zql** (`rust-translation-guide.md` line 4).
   Before this slice ships, the guide needs a zql section that covers at
   minimum the three constructs flagged NEW below — otherwise a downstream
   porter will re-invent them under time pressure and they will diverge.

2. **Synchronous cooperative-yield generators (`Generator<'yield', T>`) are
   not in the guide.** Guide row A16 covers `async function*` → `async_stream`
   + `impl Stream`, but these operators are *sync* generators whose only
   "yield control" signal is a sentinel value `'yield'` interleaved into the
   iterator. The Rust analogue is either a custom `enum Item<T> { Value(T),
   Yield }` over `Iterator`, or `genawaiter`/`gen-iter`. Picking one is a
   single design decision that must live in the guide before translation,
   because it propagates through *every* operator signature
   (`fetch`, `push`, `filter`). See
   `new-patterns/zql-ivm-joins-cooperative-yield-token.md`.

3. **The manual iterator cleanup protocol (`try` / `iter.return?.()` /
   `iter.throw?.()`) used by `FlippedJoin.fetch` and `mergeFetches`** has no
   direct Rust analogue. Rust iterators drop cleanly, so the cleanup happens
   for free via `Drop` — but the *order* of cleanup and the fact that an
   inner `throw` must not prevent cleanup of siblings needs an explicit note
   (loop over iterators calling a `close()`-style method, or rely on `drop`
   and let panics propagate; not the same semantics as TS).

4. **`BTreeSet<Entry>` with a custom comparator** cannot be expressed in
   `std::collections::BTreeSet` (which requires `Ord`, not a closure). Options
   are `BTreeMap<SortKey, Value>` with a wrapper-newtype `Ord` impl, or
   `indexmap`/`sorted-vec`. The guide should nominate one before porting
   `memory-storage.ts`.

The other two items (`structuredClone`, relationship-thunk shape) are
cosmetic: the translation is obvious (`Clone` on a `JSONValue` enum tree;
`Box<dyn Fn() -> Stream<...>>`) but they aren't in the guide's vocabulary.

**Minimum additions to the guide before porting starts:**

- A new subsection (e.g. "H. ZQL / IVM patterns") covering:
  - Cooperative-yield sync generator with a sentinel value (pattern
    `zql-ivm-joins-cooperative-yield-token`).
  - Generator "scope guard" for per-push state (finally-reset-field).
  - Custom-comparator sorted collection (pattern
    `zql-ivm-joins-btreeset-custom-cmp`).
  - Manual iterator cleanup in a merge (pattern
    `zql-ivm-joins-manual-iterator-cleanup`).
  - Node-relationship thunk shape (pattern
    `zql-ivm-joins-relationship-thunk-shape`).
- A note that `structuredClone(JSONValue)` = deep-clone of a `serde_json::Value`-
  like enum, no crate required.

Once those land, the join/exists/fan-out/fan-in slice is an estimated ~1.5 KLOC
of mechanical Rust (ballpark 1.1× LOC expansion over TS for pattern matches +
explicit lifetime annotations). The real size cost is test parity: the
existing `*.test.ts` files (`join.fetch.test.ts`, `join.push.test.ts`,
`flipped-join.*.test.ts`, `exists.*.test.ts`, `fan-out-fan-in.test.ts`,
`union-fan-in.test.ts`) are 6× the operator LOC and need to be either ported
or replaced with equivalent Rust integration tests, else "done" cannot be
claimed against the `ts-vs-rs-comparison.md` row 14 rubric.

**Not blocked on** any Risky item (E3, E4, pgoutput, BEGIN CONCURRENT, OTEL).
