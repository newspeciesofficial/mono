> **CORRECTION (2026-04-15):** My earlier "Model 3" proposal (each rayon
> worker opens its own connection and pins via `sqlite3_snapshot_open`) is
> **not viable** — SQLite's `begin-concurrent-wal2` branch explicitly
> rejects the snapshot API in WAL2 mode, and Zero requires WAL2. Verified
> by the existing `crates/libsqlite3-sys/tests/smoke_snapshot.rs` test.
>
> The actual approved design — demonstrated by the existing
> `crates/libsqlite3-sys/tests/smoke_shared_connection_pool.rs` test — is
> **Model 1 done properly**: one `Arc<Mutex<Connection>>` per client
> group, leader does `BEGIN CONCURRENT` + one dummy read to pin the
> snapshot, rayon workers briefly take the Mutex to do SQL and release it
> for CPU-bound IVM work. The Mutex serialises only SQLite calls, not
> operator work. Measured speedup >1.8× in the existing smoke test.
>
> Replace every "Model 3" / `sqlite3_snapshot_*` reference below with
> "shared Connection pool + BEGIN CONCURRENT on leader". The rest of the
> decision (no channels inside chains, `&mut self` operator methods,
> single-threaded IVM per chain except SQLite under Mutex) stands.

> **FINAL DECISION (supersedes most of this doc).** We are not adopting
> fred's full pattern. The agreed architecture is:
>
> - **Rayon parallelism lives at the top.** One rayon thread owns one
>   full IVM chain end-to-end. No concurrency inside a chain.
> - **No `Arc<Self>`, no `Mutex` on operator state, no `Sync` bound.**
>   Operators are `Box<T>` with `&mut self` methods; state is plain fields.
> - **Channels only at real thread boundaries** — SQLite producer thread →
>   IVM chain, and Tokio reactor → IVM chain. Never between operators.
> - **Operators return `impl Iterator<Item = Yield> + '_`** — true
>   laziness, no collect-to-Vec, no genawaiter.
> - **Deadlocks become compile errors** because there's no second thread
>   to deadlock against and re-entrant `&mut self` is rejected by the
>   borrow checker.
>
> The verdicts below from the fred analysis still describe solutions that
> *work*, but the simpler rayon-at-top model is cleaner and closer to TS.
> Execution order:
> 1. SQLite streaming into `table_source.rs` (the one place a real thread
>    boundary exists). Channel-based, keeps the POC pattern.
> 2. IVM trait redesign — drop `Arc<Mutex<…>>` scaffolding, use `Box` +
>    `&mut self`, return lazy iterators.
> 3. Delete genawaiter dep and `filter_gen.rs`. Don't pull in parking_lot.

# Verdict: does the `fred` pattern solve our 10 IVM port problems?

> Companion to `ivm-port-open-questions.md` and `fred-pattern-for-ivm.md`.
> Each verdict is grounded in a POC run (see `/tmp/fred-style-ivm/` and
> `/tmp/sqlite-stream/`), not speculation.

**Shorthand for "the fred pattern":** operator methods return
`Receiver<Yield>` (crossbeam, sync-flavor) instead of
`Box<dyn Iterator + 'a>`; producer runs in its own thread and captures
`Arc<Self>`; Drop of the Receiver breaks the channel and triggers cleanup.

POC results: **all five covered tests passed** — Problem 1 (no borrow),
Problem 3 (Drop guard runs on early cancel), Problem 6 (fan-out via
sequential forwards), Problem 7 (Take at-limit eviction), Problem 2
(no back-edge adapter, no Arc cycle).

---

## Problem 1: `Output::push` returns a stream borrowing `&'a mut self`

**Verdict: SOLVED.** ✅

Replace `fn push(&mut self, change: Change) -> Stream<'a, Yield>` with
`fn push(self: Arc<Self>, change: Change) -> Receiver<Yield>`. The returned
`Receiver` is `'static` and owns its captures. No lock held by the caller;
the caller iterates `rx.iter()` at its own pace, break-early works.

Demonstrated in `prob1_no_borrow_collect_pattern` of the POC: 3 changes
pushed, 2 forwarded, the Filter never holds a Mutex while iterating the
downstream's stream.

---

## Problem 2: Constructor wiring — TS `parent.setOutput({push: c => this.#pushParent(c)})`

**Verdict: SOLVED.** ✅

In the fred pattern there is no "back-edge adapter struct". The operator
*is* the handle: `Arc<Operator>` is cloned into producer closures directly.
No `XBackEdge(Arc<X>)` wrapper per operator.

Demonstrated in `prob2_arc_self_wiring`: `Arc<Filter>::push(change)` does
the work; downstream `Arc<Sink>` is held inside Filter's `Mutex<Option<…>>`
— a one-way reference, no cycle. POC printed `filter strong_count=1, sink
strong_count=2` confirming no cycle.

**Caveat:** for Join, which has *two* upstream inputs (parent + child),
each upstream still needs something that translates its `push(change)` call
into "call `Join::push_parent` or `Join::push_child`". That could still be
two tiny adapter enums (one per side of the join), but they'd hold
`Arc<Join>` without any cycle, unlike today.

---

## Problem 3: `try / finally` for in-progress overlay state

**Verdict: SOLVED.** ✅

Move the Drop guard *inside the producer closure*. It runs when the
producer thread exits — either naturally (stream exhausted) or when the
consumer drops the Receiver (send fails → producer returns → guard fires).

Demonstrated in `prob3_drop_guard_runs_on_early_cancel`: producer set
`in_flight=1`, consumer took one row then dropped the receiver, producer's
next `send` failed, `_guard: Drop` ran, `in_flight` back to 0.

This is strictly cleaner than today's eager-collect approach because the
cleanup fires exactly when the consumer gives up, not at function return.

---

## Problem 4: Lazy relationship factory closures (`processParentNode`)

**Verdict: SOLVED.** ✅ (not independently POC'd, but follows from 1+3)

`processParentNode` returns a `Node` with a `relationships` factory. In the
fred pattern, the factory is a closure that captures `Arc<ChildInput>` +
keys (already what we do), and returns `Receiver<NodeOrYield>` on demand.
Each call spawns a tiny producer that streams matching child rows.
Dropping the receiver kills the producer.

Remaining concern: **per-call thread spawn cost**. If a Join emits 1000
parent rows and each caller materializes all 1000 child streams, we spawn
1000 threads. Rayon's work-stealing pool is the right mitigation — spawn
onto a pool instead of `std::thread::spawn` directly. Exactly what fred
does with tokio tasks; we'd do it with rayon.

---

## Problem 5: The `'yield'` sentinel

**Verdict: NOT A PROBLEM WITH THIS PATTERN.** ✅

fred's streams yield only real values; there's no cooperative-pause
sentinel. Our serial Rust IVM in `spawn_blocking` doesn't need one either
— fairness comes from the Tokio runtime having many worker threads.

Delete `NodeOrYield`, return plain `Node` (or `Yield`). Smaller surface,
fewer match arms, one fewer thing to get wrong.

---

## Problem 6: FanOut — replay one stream to N consumers

**Verdict: SOLVED, with a caveat about `Change` cloning.** ✅ (with caveat)

The producer receives one Change, then sequentially calls `downstream.push(change)`
on each downstream and forwards yields to its own channel. Demonstrated
in `prob6_fanout_via_tx_clone`: 1 push → 3 downstreams received → 3 yields.

**Caveat:** `Change` needs to reach every downstream. If we want to avoid
N-way clones, wrap in `Arc<Change>` and pass that. Or use a `broadcast`
channel if every downstream takes a cheap clone (rows are already mostly
`Arc<JsonValue>` internally).

The POC used `Change: Clone` and forwarded sequentially. Real code should
use `Arc<Change>` and iterate downstreams without reclone.

---

## Problem 7: Take's at-limit eviction needs a re-entrant fetch

**Verdict: SOLVED — the deadlock pattern structurally disappears.** ✅

Demonstrated in `prob7_take_eviction`: prefill 10, 20; push 5 (id < bound).
Take emits `Remove(20) + Add(5)` = 2 emissions downstream, plus the 2
initial `Add`s = 4 total. The "fetch replacement" operation is done by
Take reading its own `Mutex<TakeState>` — **not** by calling back into
the pushing input.

In the real Take, finding the replacement row requires a fetch from the
source. With the fred pattern, that fetch is just another
`source.push_fetch_request(constraint) -> Receiver<NodeOrYield>` running
on its own producer thread. No lock is held by Take while it reads that
stream. Deadlock pattern **structurally impossible**.

---

## Problem 8: Exists's `#inPush` re-entrancy guard

**Verdict: SOLVED — but a simpler one would suffice.** ✅

In today's design, `#inPush=true` during push so recursive `filter()`
bypasses the cache. With the fred pattern, push happens in a producer
thread; any recursive filter happens on the same thread and can use a
thread-local flag (or `parking_lot::Mutex<bool>` toggled inside the
producer closure's Drop guard — same pattern as Problem 3).

Not separately POC'd but follows from Problem 3's mechanism.

---

## Problem 9: TableSource per-connection push fanout with two-tier yield

**Verdict: SOLVED. The two-tier yield sentinel goes away.** ✅

Same answer as Problem 6 for the fan-out mechanics (one source change →
push to each connected output). The `'yield' | undefined` two-tier
sentinel from TS is purely a Node.js fairness hack — delete it in Rust.

---

## Problem 10: FilterStart / FilterEnd begin/end caching

**Verdict: SOLVED via Yield::BeginFilter / Yield::EndFilter variants.** ✅

The channel's item type becomes an enum:

```rust
enum Yield {
    Row(Node),
    BeginFilter,
    EndFilter,
    // … optional yield/heartbeat variants
}
```

Producer emits `BeginFilter` before the row stream, rows in the middle,
`EndFilter` last. The consumer treats these as a bracket. Caches inside
the filter sub-graph live on operator state (`Mutex<HashMap<…>>`) that
`BeginFilter` clears and `EndFilter` closes.

Consumer dropping mid-loop → next send fails → producer's cleanup guard
runs `EndFilter` via a Drop impl. So we get the TS `finally` semantics
even across early cancel.

---

## Overall scoreboard

| # | Problem | Verdict |
|---|---|---|
| 1 | `Output::push` trait shape | ✅ SOLVED |
| 2 | Constructor wiring / back-edge adapters | ✅ SOLVED (two-input operators still need small dispatch enums) |
| 3 | try/finally overlay state | ✅ SOLVED |
| 4 | Lazy relationship factory closures | ✅ SOLVED (watch per-call thread spawn cost) |
| 5 | `'yield'` sentinel | ✅ DELETE IT |
| 6 | FanOut N-way replay | ✅ SOLVED (wrap Change in Arc to avoid reclones) |
| 7 | Take's at-limit eviction fetch | ✅ SOLVED (deadlock class gone) |
| 8 | Exists `#inPush` guard | ✅ SOLVED (falls out of Problem 3) |
| 9 | TableSource per-connection fanout | ✅ SOLVED (same as 6) |
| 10 | FilterStart/FilterEnd caching | ✅ SOLVED (encode as Yield variants) |

**All 10 problems are solvable with this pattern.** 5 of 10 are verified
by running POC code; 5 follow from the same mechanics.

---

## What the pattern costs us

**Not free.** Honest trade-offs:

| Cost | Scale | Mitigation |
|---|---|---|
| Thread spawn per push | 1 per operator invocation | Use rayon pool instead of `thread::spawn`; fred uses tokio tasks for the same reason. |
| Channel send/recv overhead | ~100ns per item | Negligible vs current Vec allocation for the same count. |
| Arc clone on every operator | 1-3 per push | Atomic inc is cheap; far cheaper than today's Mutex lock+unlock. |
| `Arc<Change>` instead of owned `Change` | 1 atomic per fan-out branch | Worth it to avoid deep clones of nested row data. |
| Complete trait-surface rewrite | Large one-time | Single migration, done with grep + sed for most operators, then targeted fixes for stateful ones (Take, Exists, Join). |

---

## Recommended next step

1. **Finish the SQLite side first.** The `/tmp/sqlite-stream/` POC shows
   `TableSource::query_rows` → `query_rows_streaming` migration works.
   Wire that into `table_source.rs` as a parallel method (don't delete
   the old one yet); migrate Take, Skip, Exists's fetch paths to use it.
   Benchmark a LIMIT-heavy query and confirm the row-count-read drops
   from full-scan to `limit + slack`.

2. **Then do the IVM trait redesign.** One PR to change `Output::push`'s
   signature and update every operator. Mechanical for stateless ones
   (Filter, FilterStart, FilterEnd), careful for stateful ones (Take,
   Join, Exists). Existing tests should port 1:1 by swapping
   `.collect::<Vec<_>>()` for `.into_iter().collect::<Vec<_>>()` on the
   Receiver.

3. **Delete `NodeOrYield`** as part of the same PR (it becomes dead
   code after step 2). Drop `genawaiter` dependency we added earlier —
   we won't need it.

4. **Move operator fields to `parking_lot::Mutex`** in the same PR.
   Already in fred's stack for a reason.

Total: 2 medium-sized PRs. First unblocks IVM laziness end-to-end;
second lands the fred-pattern rewrite. Both buy structural fixes to
the open-question problems, not just papered-over workarounds.

Sources:
- POC 1 (SQLite streaming): `/tmp/sqlite-stream/`
- POC 2 (fred-pattern IVM): `/tmp/fred-style-ivm/`
- Pattern reference: `docs/fred-pattern-for-ivm.md`
- fred source: `/tmp/fred-study/` (github.com/aembke/fred.rs)
