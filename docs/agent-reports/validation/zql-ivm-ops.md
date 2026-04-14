# Validation report: zql-ivm-ops

## Scope

Files read in full (in `packages/zql/src/ivm/`, **not** `packages/zero-cache/`):

- `source.ts` — 96
- `memory-source.ts` — 820 (the only `Source` implementation in zql)
- `filter.ts` — 57
- `filter-operators.ts` — 160 (FilterInput / FilterOutput / FilterStart / FilterEnd; imported by `filter.ts`)
- `filter-push.ts` — 36 (imported by `filter.ts`, `memory-source.ts`)
- `take.ts` — 766
- `skip.ts` — 157
- `operator.ts` — 117 (supporting types the operators import)
- `change.ts` — 65 (supporting types)
- `data.ts` — 124 (supporting types + comparators)
- `schema.ts` — 25 (supporting types)
- `constraint.ts` — 202 (supporting types + helpers)
- `stream.ts` — 43 (supporting types)
- `push-accumulated.ts` — 438 (called by fan-in; consumed here only to understand push semantics)
- Total LOC in scope: **3106**

Explicit notes on the prompt:

- `ordered-snapshot.ts` and `snapshot.ts` **do not exist** under
  `packages/zql/src/ivm/` (verified). There is no "snapshot operator" in the
  IVM tree — `fork()` in `memory-source.ts:129` is the closest thing (clones
  the primary-index `BTreeSet`). `array-view.ts` / `view-apply-change.ts`
  produce the client-visible ordered snapshot but are outside my scope.
- The prompt asks about "source→SQLite statement compilation". **No SQL
  compilation happens inside `packages/zql/src/ivm/`.** `MemorySource` is a
  pure in-memory `BTreeSet<Row>` index with synthetic `minValue`/`maxValue`
  sentinels. The SQLite-backed `Source` is `packages/zqlite/src/table-source.ts`
  (out of scope, and outside zql). That file lives under `zqlite`, which is
  explicitly out of scope in `rust-translation-guide.md:4`. See the
  Ready-to-port assessment for the consequence.

## Summary
| metric | count |
|---|---|
| TS constructs used | 23 |
| Constructs covered by guide | 17 |
| Constructs NOT covered | 6 (3 already flagged by sibling `zql-ivm-joins`; 3 new here) |
| Libraries imported | 1 (`compare-utf8`, via `data.ts:1`) |
| Libraries covered by guide | 1 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 0 |

> Scope caveat. `rust-translation-guide.md:4` states zql is out of scope. Per
> the orchestrator's instruction to use the guide's A–G catalogue as the
> *closest* reference, I mark constructs `covered` when there is a mechanical
> Rust analogue in A–G, and `NEW` when the construct is IVM-specific and has
> no guide row. I do not re-derive patterns the sibling `zql-ivm-joins` report
> (`docs/agent-reports/validation/zql-ivm-joins.md`) already catalogued —
> those are cited inline.

## Constructs used

| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class` with `#private` fields + public methods | `filter.ts:18`, `skip.ts:25`, `take.ts:48`, `memory-source.ts:92`, `filter-operators.ts:61`, `filter-operators.ts:106` | A1, A23 | covered |
| `interface` implemented by `class` (`implements Operator`, `implements FilterOperator`, `implements Source`) | `filter.ts:18`, `skip.ts:25`, `take.ts:48`, `memory-source.ts:92`, `filter-operators.ts:61`, `filter-operators.ts:106` | A5 | covered |
| Readonly private fields (`readonly #input: Input`) | `skip.ts:26-28`, `take.ts:49-52`, `filter.ts:19-20` | A1 | covered |
| Tagged-union `switch` on `change.type` / `SourceChange.type` + `unreachable(change)` exhaustiveness | `filter-push.ts:18-35`, `take.ts:240-426`, `memory-source.ts:381-418` (`#writeChange`), `memory-source.ts:521-536` (`genPush`), `push-accumulated.ts:126-244`, `skip.ts:80-94` | A6 | covered |
| Synchronous generator functions (`function*`, `yield`, `yield*`) | `filter.ts:38` (`*filter(node)`), `filter.ts:54` (`*push`), `skip.ts:43` (`*fetch`), `skip.ts:78` (`*push`), `take.ts:87` (`*fetch`), `take.ts:152` (`*#initialFetch`), `take.ts:240` (`*push`), `take.ts:428` (`*#pushEditChange`), `take.ts:688` (`*#pushWithRowHiddenFromFetch`), `memory-source.ts:252` (`*#fetch`), `memory-source.ts:357` (`*push`), `memory-source.ts:365` (`*genPush`), `memory-source.ts:423,435,443,500,514,571,615,730,806` (module-level generators), `filter-operators.ts:49,53,82,86,127,143` (`throwFilterOutput`, `FilterStart.*fetch`, `*push`, `FilterEnd.*push`), `push-accumulated.ts:79`, `filter-push.ts:8` | A22 (`Symbol.iterator` / `impl Iterator`) | covered (sync generators; see below for the `'yield'` sentinel variant) |
| Generator with discriminated yield sentinel `Stream<Node \| 'yield'>` / `Stream<'yield'>` / `Stream<'yield' \| undefined>` | `operator.ts:41` (`fetch(req): Stream<Node \| 'yield'>`), `operator.ts:76` (`push(change, pusher): Stream<'yield'>`), `source.ts:79` (`push(change): Stream<'yield'>`), `source.ts:91` (`genPush(change): Stream<'yield' \| undefined>`), and every operator generator above | — | **NEW** — already flagged by sibling as `zql-ivm-joins-cooperative-yield-token`; same pattern applies to `filter`/`skip`/`take`/`MemorySource`. Cite sibling pattern, do not duplicate. |
| `yield*` delegation into an iterator | `filter.ts:55`, `skip.ts:81,93`, `take.ts:96,265,332,333,393,400,415,423,454,462,503,544,548,610,626,662,675,691`, `memory-source.ts:352,354,371`, `filter-operators.ts:83,94,119,144`, `filter-push.ts:15,22,27,31`, `push-accumulated.ts:132,142,167,189,200,224,236` | A22 | covered |
| `try { … } finally { … }` used to always reset transient per-call state | `take.ts:174-208` (`exceptionThrown`/`downstreamEarlyReturn` gate), `take.ts:690-694` (`#rowHiddenFromFetch = undefined`), `filter-operators.ts:88-102` (`endFilter()` in `FilterStart.fetch`) | A15 | covered structurally; same "reset a field on generator scope exit" pattern the sibling flagged as `zql-ivm-joins-generator-scope-guard`. Cite sibling. |
| `try { … } catch (e) { exceptionThrown = true; throw e; } finally { … }` — rethrow + finally-as-commit-gate | `take.ts:174-208` | A15 | **NEW (variant)** — see `new-patterns/zql-ivm-ops-commit-on-normal-completion.md`. Slightly different from the sibling's scope-guard: here the finally *conditionally commits state* only when no exception was thrown and no early return happened. |
| Struct update / object spread in change construction | `take.ts:318,401,549,613,677`, `memory-source.ts:545-562`, `push-accumulated.ts:251-324`, `push-accumulated.ts:378-415` | A13, A20 | covered |
| Computed-property-name in object literal (constraint building, overlay building) | `take.ts:219-222` (`Object.fromEntries(partitionKey.map(k => [k, row[k]]))`), `memory-source.ts:304-315` (scan-start construction), `push-accumulated.ts:259-265` | A19 | covered |
| Dynamic-key method dispatch `data[reverse ? 'valuesFromReversed' : 'valuesFrom'](…)` on `BTreeSet` | `memory-source.ts:811` | — | **NEW** — see `new-patterns/zql-ivm-ops-btree-valuesfrom-dispatch.md`. Combines with sibling pattern `zql-ivm-joins-btreeset-custom-cmp`; the dispatch itself must translate to two explicit method calls since Rust has no string-indexed method lookup. |
| Overloaded `interface` method signatures keyed by literal-string/typed-key (`TakeStorage.get(key: typeof MAX_BOUND_KEY): Row \| undefined; get(key: string): TakeState \| undefined`) | `take.ts:27-33` | — | **NEW** — see `new-patterns/zql-ivm-ops-literal-key-overload-storage.md`. Rust trait methods can't overload by literal; needs separate methods or an `enum Key`. |
| `JSON.stringify(['take', ...values])` as composite storage key | `take.ts:735` (`getTakeStateKey`) | E1 / E2 | covered (mechanical — `serde_json::to_string` on a `Vec<NormalizedValue>`). Identical to the sibling's `JSON.stringify`-as-cache-key note. |
| `Map<string, Index>` with string-serialized `Ordering` as key | `memory-source.ts:97,114,210,216,243` (`JSON.stringify(this.#primaryIndexSort)`) | A19 (Record) / D1 | covered |
| `Set<Connection>` and `Set<string>` (no TTL, plain container) | `memory-source.ts:66` (`usedBy: Set<Connection>`), `memory-source.ts:117-118`, `memory-source.ts:242` | D1 | covered — `HashSet<…>` |
| `BTreeSet<Row>` with a custom `Comparator` (closure-based cmp) + `valuesFrom` / `valuesFromReversed` / `clone()` / `has()` / `add()` / `delete()` | `memory-source.ts:2,65,107,116,135,140,234,238-240,368,385,394,407,413`, `memory-source.ts:806-814` | D1/D2 — no direct row | sibling flagged as `zql-ivm-joins-btreeset-custom-cmp`; adds method surface `valuesFrom`/`valuesFromReversed`/`clone`. Cite sibling. |
| Sentinel `Symbol` values for min/max `Bound` comparison (`minValue = Symbol('min-value')`, `maxValue = Symbol('max-value')`), used inside `RowBound = Record<string, Bound>` | `memory-source.ts:766-771`, `memory-source.ts:787-804` | — | **NEW** — see `new-patterns/zql-ivm-ops-bound-sentinel.md`. Rust has no runtime sentinels — `enum Bound { Min, Value(Value), Max }` is the translation. |
| Two-phase push protocol with per-push `overlay` (`#overlay: Overlay \| undefined`, `#pushEpoch`, `conn.lastPushedEpoch`) + `generateWithOverlay` splicing pending change into `fetch()` | `memory-source.ts:53-56` (types), `memory-source.ts:100-101,329-341,368-379,520-570`, `memory-source.ts:615-759` (`generateWithOverlay` / `generateWithOverlayInner`) | — | **NEW** — see `new-patterns/zql-ivm-ops-overlay-epoch-push.md`. This is the core "correctness trick" that lets a downstream operator call `fetch()` *during* a `push` and see the pending write as if already committed. No guide row. |
| "Hidden row" overlay on an operator (`Take.#rowHiddenFromFetch`) — toggled around a nested `output.push` so that any recursive `input.fetch` inside the pushed subtree skips that row | `take.ts:55-56,110-118,688-695` | — | Same family as overlay pattern above; covered by `new-patterns/zql-ivm-ops-overlay-epoch-push.md` (section "Sibling pattern: per-operator fetch-hide overlay"). |
| Closure that captures a mutable outer variable across `yield`s (`const that = this; const replaceBoundAndForwardChange = function*() { … that.#setTakeState(…); yield* that.#output.push(…); }`) | `take.ts:446-455` | A18 | covered (A18 covers closure capturing mutable outer var; the `that = this` is just the pre-arrow-function TS idiom to rebind `this` inside a `function*`). |
| `assert(cond, () => "msg")` / `assert(cond, 'msg')` / `unreachable(change)` / `unreachable()` | `take.ts:65,153-168,313,441,492,524,559,567,589,593,622,632,651,685`, `memory-source.ts:197,211,387,396,410,523,529,532`, `constraint.ts:1,150,168`, `push-accumulated.ts:97-101,128,138,148,207,216,230`, `skip.ts:--` (via `unreachable`) | A6 / F1 | covered — `assert!`, `unreachable!()`, typed errors via `thiserror`. |
| `emptyArray` / `once(iterable)` / `hasOwn` / `stringCompare` / `must(x)` / `compareUTF8` helpers from `shared/` | `memory-source.ts:3,4,320-323`, `take.ts:2,749`, `constraint.ts:2,63`, `data.ts:1,68`, `push-accumulated.ts:2,3,133,143,167,201,237` | A1 / Part-2 row | covered — internal utilities: `&[]`, hand-rolled `Once`, `HashMap::contains_key`, `str::cmp`, `.expect(msg)`, `str::cmp`. |
| `Object.keys(obj).length` / `Object.entries(a)` / `Object.fromEntries(…)` / `Object.values(…)` | `constraint.ts:50,139,188`, `memory-source.ts:268`, `take.ts:219-222`, `data.ts:119`, `push-accumulated.ts:371,388` | A19 (HashMap surface) | covered |

## Libraries used
| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `compare-utf8` | built-in `str::cmp` (Part 2: "Valid UTF-8 byte order == codepoint order. No crate needed.") | covered |

All other imports are internal to the monorepo (`shared/*`, `zero-protocol/*`,
`zero-types/*`) and are not third-party deps.

## Risk-flagged items depended on

- **None directly** from Part 3. These operators neither parse pgoutput nor
  touch `BEGIN CONCURRENT`, `arbitrary_precision`, OTEL, or `async-trait`.
- **Indirect:** The real `Source` used by zero-cache in production is the
  SQLite-backed `TableSource` in `packages/zqlite/src/table-source.ts`, which
  holds prepared statements against a WAL-mode SQLite replica. Row 6 of
  `docs/ts-vs-rs-comparison.md` notes "Not using BEGIN CONCURRENT" as a
  Rust-side deviation at that layer. Operators in this scope consume the
  `Source` interface defined in `source.ts` and are blind to which backend is
  attached, so they are not blocked on it — but a downstream port **cannot
  exercise these operators end-to-end against real data without `TableSource`
  being ported first**.

## Ready-to-port assessment

The `filter`, `skip`, `take`, `MemorySource`, and `push-accumulated`
operators are, like the join family, **data-structure-heavy but
semantically mechanical**. There is no async, no I/O, no worker threads, no
protocol parsing, and no third-party crate surface beyond `str::cmp`.
Mechanical translations:

- Class + `implements Operator` → `struct` + `impl Operator for …` (A1/A5).
- `match` on `Change` enum replaces every `switch (change.type)` (A6).
- Sync generator `function*` → `impl Iterator` (A22), plus a wrapper enum for
  the cooperative yield sentinel (see below).
- `BTreeSet<Row>` with closure cmp → `BTreeMap<SortKey, ()>` + wrapper
  newtype `Ord` impl, **or** the `sorted-vec`/`indexmap` route (sibling
  pattern `zql-ivm-joins-btreeset-custom-cmp`).
- `JSON.stringify` storage key → `serde_json::to_string` on a `Vec<Value>` or
  a compound tuple key (sibling pattern).

However, **this slice is not ready to port today** under the current guide,
for reasons below. Three are already flagged by the sibling
`zql-ivm-joins` report; three are new to this slice.

**Already flagged by sibling `zql-ivm-joins` (cited, not re-filed):**

1. `new-patterns/zql-ivm-joins-cooperative-yield-token.md` — the
   `Stream<Node | 'yield'>` / `Stream<'yield'>` / `Stream<'yield' | undefined>`
   sentinel. Every operator signature in this slice
   (`fetch`, `push`, `filter`, `genPush`) uses it. The design pick (custom
   `enum Item<T> { Value(T), Yield }` over `Iterator` vs.
   `genawaiter` / `gen-iter`) must be made once in the guide and propagated.
2. `new-patterns/zql-ivm-joins-generator-scope-guard.md` — the "reset a field
   on scope exit from a generator that may be abandoned" pattern applies here
   to `Take.#rowHiddenFromFetch` and to `FilterStart.fetch`'s `endFilter()`
   in a `finally`. The nuance unique to `Take.#initialFetch` (`take.ts:174-208`)
   is that the `finally` *also writes durable state* and uses
   `exceptionThrown` as a guard — filed separately below as
   `zql-ivm-ops-commit-on-normal-completion`.
3. `new-patterns/zql-ivm-joins-btreeset-custom-cmp.md` — `shared/src/btree-set.ts`
   is a custom-comparator sorted set; `std::collections::BTreeSet` does not
   take a closure. Translation options (`BTreeMap<SortKey, Value>` with a
   wrapper-newtype `Ord`, `indexmap`, or `sorted-vec`) must be nominated in
   the guide. Also adds a method surface the sibling didn't call out:
   `valuesFrom(row?)`, `valuesFromReversed(row?)`, and `clone()` used by
   `MemorySource.fork()` (`memory-source.ts:135`, `memory-source.ts:811`).

**New patterns unique to this slice (filed under `docs/agent-reports/new-patterns/`):**

1. `zql-ivm-ops-overlay-epoch-push.md` — the epoch-versioned per-source
   overlay that makes `fetch()` called *during* a `push()` see the pending
   write. This is not covered by any guide row and is the key correctness
   mechanism in `MemorySource.genPush` + `generateWithOverlay` +
   `generateWithOverlayInner`. It also covers the per-operator fetch-hide
   overlay used in `Take.#rowHiddenFromFetch`.
2. `zql-ivm-ops-bound-sentinel.md` — `Symbol('min-value')` /
   `Symbol('max-value')` sentinels used inside a
   `RowBound = Record<string, Bound>` during btree scan-start construction.
   Rust has no runtime sentinel; direct translation is
   `enum Bound { Min, Value(Value), Max }` + a custom `Ord` impl.
3. `zql-ivm-ops-literal-key-overload-storage.md` — `TakeStorage` declares
   overloaded `get(key: typeof MAX_BOUND_KEY): Row | undefined; get(key:
   string): TakeState | undefined` so that the `'maxBound'` key returns a
   different type from every other key. Rust trait methods cannot overload by
   literal string; needs either two methods (`get_max_bound`,
   `get_take_state`) or a typed `enum Key { MaxBound, PerPartition(String) }`
   plus `enum Value { Row(Row), TakeState(TakeState) }`.
4. `zql-ivm-ops-btree-valuesfrom-dispatch.md` — the
   `data[reverse ? 'valuesFromReversed' : 'valuesFrom'](scanStart)` idiom at
   `memory-source.ts:811`. Dynamic method dispatch by computed string in TS;
   in Rust this becomes a plain `if reverse { ... } else { ... }` match
   because there is no string-indexed method table. Mechanical but worth
   documenting since the naming of these two BTreeSet methods drives the API
   shape of whatever Rust sorted-set wrapper is chosen.
5. `zql-ivm-ops-commit-on-normal-completion.md` — the `try { for … } catch {
   exceptionThrown = true; throw e; } finally { if (!exceptionThrown)
   this.#setTakeState(…) }` idiom at `take.ts:174-208`. Sibling's
   `generator-scope-guard` covers "reset a field on scope exit" but not
   "commit state only on *normal* completion" — in Rust, the distinction is
   meaningful because `Drop` runs on both panic and normal return, so the
   equivalent is either an explicit completion flag + `std::panic::catch_unwind`
   (rarely appropriate), or restructuring so the commit is the last statement
   after the loop and panics simply abort the transaction.

**Minimum additions to the guide before porting this slice:**

- A zql/IVM subsection (already required by sibling report). In addition to
  the patterns sibling listed, it must cover:
  - Overlay-epoch push (pattern `zql-ivm-ops-overlay-epoch-push`).
  - Min/max sentinel → `enum Bound { Min, Value(V), Max }` (pattern
    `zql-ivm-ops-bound-sentinel`).
  - Typed-key overloaded `Storage` → `enum Key` + `enum Value` vs. multiple
    methods (pattern `zql-ivm-ops-literal-key-overload-storage`).
  - "Commit only on normal completion" idiom (pattern
    `zql-ivm-ops-commit-on-normal-completion`).
- An explicit statement that the **SQLite-backed Source** (`TableSource`
  in `zqlite`) is a separate port from the IVM operators, and that porting
  the IVM operators first against an in-memory `Source` impl in Rust is the
  recommended sequence. This slice does not see any SQL.

**Backpressure-per-operator note** (prompt asked for this explicitly). There
is no per-operator backpressure in the TS code. The only flow-control
mechanism is the `'yield'` sentinel, which is **cooperative responsiveness,
not backpressure**: it is inserted periodically by the data source (see
`packages/zql/src/ivm/test/random-yield-source.ts` and the sibling's
catalogue) and propagated by `yield <sentinel>` up the pipeline; consumers
may choose to park between yields but no operator pushes back on its
upstream. The Rust equivalent is straightforward for sync generators (drop
the iterator = cancel), but if the port moves to `async fn` for any reason
then bounded `tokio::sync::mpsc` (guide C1) would need to be introduced
*between* operators and is a redesign item, not a mechanical swap.

Once the above guide additions land, the estimated translation cost of this
slice is:

- `filter.ts` + `filter-operators.ts` + `filter-push.ts`: ~300 LOC Rust.
- `skip.ts`: ~180 LOC Rust.
- `take.ts`: ~900 LOC Rust (heavy branching in `*push` / `*#pushEditChange`
  — this is the highest-complexity operator in the IVM tree besides
  `flipped-join`).
- `memory-source.ts`: ~950 LOC Rust (btree wrapper + overlay machinery is
  non-trivial).
- `push-accumulated.ts`: ~450 LOC Rust.
- Operator/interface/types (`operator.ts`/`change.ts`/`data.ts`/`schema.ts`/
  `constraint.ts`/`stream.ts`): ~400 LOC Rust of traits + enums.

Total ~3.2 KLOC Rust (≈1.03× TS), plus test-parity cost of ~6× the operator
LOC for the co-located `*.test.ts` files
(`filter.test.ts`, `take.fetch.test.ts`, `take.push.test.ts`,
`take.push-child.test.ts`, `skip.test.ts`, `memory-source.test.ts`,
`push-accumulated.test.ts`, `filter-operators.test.ts`).

**Not blocked on** any Risky item (E3, E4, pgoutput, BEGIN CONCURRENT, OTEL,
async-trait).
