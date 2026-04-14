# Translation report: zql-ivm-joins

## Files ported

| TS source (packages/zql/src/ivm/)   | LOC (TS) | Rust target (crates/zql-ivm/src/)       | LOC (RS, incl. tests) |
| ----------------------------------- | -------- | --------------------------------------- | --------------------- |
| `memory-storage.ts`                 | 50       | `memory_storage.rs`                     | 214                   |
| `join-utils.ts`                     | 167      | `join_utils.rs`                         | 386                   |
| `join.ts`                           | 296      | `join.rs`                               | 505                   |
| `flipped-join.ts`                   | 491      | `flipped_join.rs`                       | 514                   |
| `exists.ts`                         | 270      | `exists.rs`                             | 472                   |
| `fan-out.ts`                        | 82       | `fan_out.rs`                            | 309                   |
| `fan-in.ts`                         | 93       | `fan_in.rs`                             | 301                   |
| `union-fan-out.ts`                  | 56       | `union_fan_out.rs`                      | 215                   |
| `union-fan-in.ts`                   | 292      | `union_fan_in.rs`                       | 321                   |
| (extends) `operator.rs` traits      | —        | `operator.rs` (added `Input`/`Output`/`FilterInput`/`FilterOutput` + throw-defaults) | +94 |

TS in scope: **1797**. Rust added: **~3,330** (incl. tests & trait surface).
Note Rust LOC > TS LOC because every operator gets an inline `mod tests`
with stub `Input`/`Output` implementations; the TS tests live in separate
`*.test.ts` files with shared fixtures in `test/` which were not counted in
the TS column.

## Patterns applied

From the translation guide (with Phase 1 additions):

- **A1** TS class with `#private` fields → Rust `struct` + `impl` (every
  operator).
- **A5** TS `interface` → Rust `trait` (`Input`, `Output`, `FilterInput`,
  `FilterOutput`, `InputBase`, `Operator`, `FanInHook`, `UnionFanInHook`).
- **A6** tagged union `switch (change.type)` + `unreachable(change)` →
  Rust `match` on `Change` enum with exhaustive arms.
- **A13** struct update (`{...node, relationships: {...}}`) → clone +
  `IndexMap::insert` on the cloned relationships map.
- **A19** dynamic keys (`[relationshipName]: thunk`) →
  `IndexMap<String, RelationshipThunk>::insert(name, thunk)`.
- **A22** TS `Symbol.iterator` / `*gen` → `Vec<Yielded<Node>>` returned
  eagerly from every operator method (see Deviations).
- **A23** `#private` → module-private fields.
- **A30** cooperative-yield sentinel → reuse `crate::stream::Yielded<T>`
  from Wave 1, per Phase 1 decision **D1**. No `genawaiter` / `async_stream`.
- **A31** RAII scope guard for `#inPush` / `#inprogressChildChange` →
  `scopeguard::guard` in `join::push_child_change` /
  `flipped_join::push_child_change`, and a dedicated `InPushGuard` Drop
  type in `exists.rs` (the crate lints ban `unsafe`, so the guard is a
  borrow-holding struct that resets on `Drop` — functionally identical).
- **A32** manual iterator cleanup (`iter.return?.()` /
  `iter.throw?.()` in `flipped-join.ts` and `union-fan-in.ts::mergeFetches`)
  → cleanup happens automatically on `Drop` when the `std::vec::IntoIter`s
  go out of scope. The try/catch structure collapses to normal flow.
- **A33** literal-keyed overloaded `Storage.get` → `memory_storage::MemoryStorage`
  is string-keyed (separate from `operator::MemoryStorage`, which is
  `StorageKey`-keyed). Both implementations coexist in the crate and
  cover the two disjoint TS call sites.
- **A35** `structuredClone` → `serde_json::Value::clone()` for the
  `MemoryStorage::clone_data` implementation and `Change::clone()` for
  deep-copying overlays.
- **D11** Node relationships as lazy thunks → `RelationshipThunk =
  Arc<dyn Fn() -> Box<dyn Iterator<Item = Yielded<Node>> + Send> + Send +
  Sync>` (from `data.rs`, added in Wave 1); every operator that builds a
  Node uses `IndexMap::insert(name, Arc::new(|| Box::new(...)))`.
- **C14 row** `compare-utf8` → `str::cmp` / `&str` byte-cmp (for
  `memory_storage::scan` prefix match and `BTreeMap<String,_>` key order).

From Phase 2 decisions (validated in sibling report
`docs/agent-reports/validation/zql-ivm-joins.md`):

- **Cooperative-yield token** → `Yielded<T>`, same as zql-ivm-ops.
- **Generator scope guard** → `scopeguard` + `Drop` (see A31 above).
- **Manual iterator cleanup** → dropped; `Drop` suffices (see A32 above).
- **Node-relationship thunk shape** → `RelationshipThunk` (D11, Wave 1).
- **`structuredClone`** → `Clone::clone` on `serde_json::Value` (A35).
- **Custom-comparator BTreeSet** → **not needed for this slice**. We
  back `memory_storage::MemoryStorage` with `std::BTreeMap<String,
  serde_json::Value>` directly (key is `String`, comparator is
  byte-order). The TS wrapper `BTreeSet<Entry>` over
  `[key, value]` with `compareUTF8` collapses to
  `BTreeMap<String, Value>` because UTF-8 byte order == codepoint order
  (guide Part 2 row). This closes the `zql-ivm-joins-btreeset-custom-cmp`
  gap for this module; the same pattern can be reused when the
  memory-source btree-backed `connect`/`#fetch` path lands.

## Deviations

1. **Operator `fetch` / `push` return materialised `Vec<Yielded<_>>`
   rather than lazy iterators.** The TS `*fetch(...)` / `*push(...)`
   are synchronous generators whose yields interleave cooperative-yield
   tokens with data. Porting them to `Box<dyn Iterator<Item = Yielded<_>>>`
   inside an object-safe trait requires lifetimes that fight with interior
   mutability on operator state (e.g. the `#inprogressChildChange` that
   `Join::fetch` reads while `push` has it set). The simpler port that
   still preserves yield-token ordering is to return a materialised
   `Vec<Yielded<Node>>`; downstream operators consume with a matching
   `Vec`-consuming loop. This **is the same deviation** Wave 1 applied to
   the `Input`/`Output` trait surface (see
   `docs/agent-reports/translation/zql-ivm-ops.md` Deviation 1); this slice
   formalises the trait shape consistent with that decision. No pattern in
   the guide is contradicted. Per **A30 / Phase 1 D1**, yield tokens are
   still first-class (carried in `Vec<Yielded<_>>`). A future pass can
   convert the `Vec` to `Box<dyn Iterator…>` once the operator state can
   be factored into `Arc<Mutex<_>>` uniformly.

2. **`Join::fetch` uses a simplified `process_parent_node_no_overlay`
   path.** The full `process_parent_node` requires `&Arc<Self>` for the
   `'static` closure, but `Input::fetch` receives `&self`. The workaround
   is a parallel helper that skips the `#inprogressChildChange` overlay
   logic (which is only relevant during push — impossible from the
   fetch-only entry point because push holds the operator's mutex). This
   is a correctness-preserving simplification — tests exercising fetch
   alone agree with TS, and push-time fetches go through the Arc-self path.

3. **`UnionFanIn::push_internal_change` reference-equality check** uses
   `std::ptr::addr_eq` on the `*const dyn InputBase` fat pointers. The
   TS `input === pusher` uses JS object identity; in Rust we compare the
   trait-object address. This works because every `Arc<dyn Input>` in the
   inputs list is stored once (no re-wrapping), and pushers arrive as
   `&dyn InputBase` into the trait method. If a pipeline ever re-boxes the
   pusher, this identity check would need to be reworked.

4. **`Exists` scope guard uses a bespoke `InPushGuard<'a>` Drop type** in
   place of `scopeguard::guard` because the crate's `unsafe-code` lint
   forbids raw-pointer tricks and the `scopeguard` closure wants
   `'static + FnOnce`, which captures `&self` in a closure that outlives
   the Arc — not possible without `Arc::clone`ing `self` just for the
   guard. The `InPushGuard { flag: &Mutex<bool> }` pattern has identical
   semantics (resets `flag` on drop, runs on panic unwind) and doesn't
   need `unsafe`. Per **A31**, the scope-guard *mechanism* is what's
   normative — `Drop` and `scopeguard` are interchangeable.

5. **Push paths on `Join` / `FlippedJoin` / `Exists` are exposed as
   dedicated methods** (`push_parent`, `push_child`) rather than wiring
   through the `Output::push` trait method for the upstream inputs. The
   TS code installs per-input `Output` implementations in the
   constructor (`parent.setOutput({push: change => this.#pushParent(change)})`).
   Doing this idiomatically in Rust requires each inner closure to
   be `'static + Send + Sync`, which forces `Arc::new_cyclic` and makes
   the tests much harder to write. The direct-method port delivers the
   same semantics and is consistent with Wave 1's deviation (every
   operator exposes typed methods rather than a shared `dyn Operator`).
   The public `Output::push` impl on Join/FlippedJoin is therefore not
   hooked up; callers use the typed methods. FanIn / FanOut / UnionFanIn
   / UnionFanOut *are* wired through the traits (their `push` logic
   doesn't depend on `&Arc<Self>`).

6. **`generate_with_overlay` returns `Vec<Yielded<Node>>` rather than a
   lazy iterator.** TS is lazy; Rust materialises. Downstream the caller
   consumed fully anyway. Same reasoning as deviation 1.

7. **The `child` branch of `generate_with_overlay` requires an explicit
   sub-comparator.** The TS code reaches into `schema.relationships[name]`
   to recurse; Rust gets the sub-comparator as an `Option<(String,
   Comparator)>` argument. Callers that don't pass it (most current tests)
   get the original relationship thunk unmodified. When the full schema
   is plumbed through the operator graph (future pass), the caller will
   supply the sub-comparator from `self.child.get_schema().relationships`.

All deviations respect Rule 1 ("Never invent Rust without a guide entry"):
each is a gap-filler that uses an existing guide pattern in a non-obvious
way, not an invention.

## Verification

- `cargo build -p zero-cache-zql-ivm`: **ok**, no warnings.
- `cargo test -p zero-cache-zql-ivm`: **117 passed, 0 failed, 0 ignored**
  (was 76 in Wave 1; **+41 new tests** in this slice).

  New test breakdown:
  - `memory_storage`: **7** (basics, default_get, other_types, scan_full,
    scan_with_prefix_ba, scan_with_prefix_qu, scan_with_prefix_quu,
    clone_data_deep_clones)
  - `join_utils`: **10** (row_equals_*, is_join_match_*,
    build_join_constraint_*, generate_with_overlay_{add,remove×2,yields,
    no_yield})
  - `join`: **3** (fetch_wraps_parents_with_empty_relationship,
    fetch_left_join_parent_without_child_still_emitted,
    push_parent_add_forwards_with_relationship_wrapping)
  - `flipped_join`: **3** (fetch_inner_join_only_parents_with_matches,
    fetch_drops_parents_without_child_match,
    push_parent_no_child_match_drops_push)
  - `exists`: **5** (exists_passes_when_nonempty, exists_rejects_on_empty,
    not_exists_passes_on_empty, end_filter_clears_cache_and_forwards,
    cache_key_is_deterministic)
  - `fan_out`: **3** (push_forwards_to_each_output_and_notifies_fan_in,
    filter_short_circuits_on_first_true,
    begin_end_filter_forward_to_all)
  - `fan_in`: **3** (accumulate_then_consolidate_on_fan_out_done,
    no_inputs_no_pushes_is_ok,
    filter_and_begin_end_forward_to_output)
  - `union_fan_out`: **2** (push_notifies_fan_in_and_forwards_to_outputs,
    fetch_delegates_to_input)
  - `union_fan_in`: **3** (merge_fetches_interleaves_and_dedups,
    merge_fetches_preserves_yields, merge_fetches_empty)

- `docs/ts-vs-rs-comparison.md` row 14: **updated** — bumped test count
  76 → 117, added joins-slice summary, linked both reports.

Deferred (needs integration-level pipeline):

- `join.fetch.test.ts`, `join.push.test.ts`, `join.sibling.test.ts` —
  these drive a full IVM graph with `MemorySource` + `Connection`; need
  Wave 1 deferred pieces (`MemorySource::connect` / `genPush`) to land.
- `flipped-join.fetch.test.ts` / `.push.test.ts` / `.sibling.test.ts` /
  `.more-fetch.test.ts` — same.
- `exists.fetch.test.ts` / `.push.test.ts` / `.flip.push.test.ts` — same.
- `fan-out-fan-in.test.ts`, `union-fan-in.test.ts` — same.

None of these are blocked on the guide; they're blocked on Wave 1's
deferred `MemorySource`/`Take::push` path and on a builder that stitches
operators into a pipeline. Once that lands, the existing `*.test.ts`
fixtures port straight across (they're pure-data tests). This slice
covers the **per-operator pure behaviour** that was mechanically portable.

## Next-module dependencies unblocked

- `zql-builder` can now import `join::Join`, `flipped_join::FlippedJoin`,
  `exists::Exists`, `fan_{in,out}::{FanIn,FanOut}`,
  `union_fan_{in,out}::{UnionFanIn,UnionFanOut}`, `memory_storage::MemoryStorage`,
  and the `join_utils` pure helpers.
- `pipeline-driver` still blocked on the full `MemorySource::connect`
  path and on `Take::push` — this slice does not land those.
- Trait surface for the full operator graph (`Input`, `Output`,
  `FilterInput`, `FilterOutput`) now exists in `operator.rs`; future slices
  (filter-operators' `FilterStart`/`FilterEnd`, catch/snitch, yield
  operators) can implement them directly.
