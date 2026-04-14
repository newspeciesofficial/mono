# New pattern: Literal-key-overloaded Storage interface

## Category
A (Language patterns).

## Where used
- `packages/zql/src/ivm/take.ts:20` — `const MAX_BOUND_KEY = 'maxBound';`
- `packages/zql/src/ivm/take.ts:27-33` — `TakeStorage` interface with overloaded method signatures keyed by literal-string type
- `packages/zql/src/ivm/take.ts:72` — `this.#storage = storage as TakeStorage;` (the operator narrows the generic `Storage` from `operator.ts:109-117` into this local, specialised interface)

## TS form

```ts
const MAX_BOUND_KEY = 'maxBound';

// Two key-classes stored side by side in the same Storage:
//   - MAX_BOUND_KEY  (string literal 'maxBound') holds a Row
//   - any other key ('["take", partitionA, partitionB]' JSON string)
//     holds a TakeState
interface TakeStorage {
  get(key: typeof MAX_BOUND_KEY): Row | undefined;
  get(key: string): TakeState | undefined;
  set(key: typeof MAX_BOUND_KEY, value: Row): void;
  set(key: string, value: TakeState): void;
  del(key: string): void;
}
```

## Proposed Rust form

Option A — two typed methods:

```rust
trait TakeStorage {
  fn get_max_bound(&self) -> Option<Row>;
  fn set_max_bound(&self, row: Row);
  fn get_take_state(&self, key: &str) -> Option<TakeState>;
  fn set_take_state(&self, key: &str, state: TakeState);
  fn del(&self, key: &str);
}
```

Option B — typed-enum key + typed-enum value:

```rust
enum StorageKey {
  MaxBound,
  PerPartition(String),
}

enum StorageValue {
  Row(Row),
  TakeState(TakeState),
}

trait Storage {
  fn get(&self, key: &StorageKey) -> Option<StorageValue>;
  fn set(&self, key: StorageKey, value: StorageValue);
  fn del(&self, key: &StorageKey);
}
```

Option A is closer to the TS shape and avoids runtime type-check pattern
matches in the hot path. Option B matches the generic `Storage` interface
at `operator.ts:109-117` more literally.

## Classification
**Redesign** — Rust trait methods cannot overload by parameter value type.
The TS signature works only because TypeScript 4.9+ literal-string types
distinguish `'maxBound'` from `string`. There is no direct line-for-line
translation; the porter must make a design pick.

## Caveats

- The generic `Storage` in `operator.ts:109-117`
  (`set(key: string, value: JSONValue)`, `get(key: string, def?: JSONValue)`)
  is the interface shared with other operators (not only `Take`). Changing
  its Rust shape to Option B would cascade into every operator that uses
  `Storage` (`join.ts`, `flipped-join.ts`, `exists.ts`). Option A is
  Take-specific and can live alongside a generic `Storage` trait.
- The TS code uses `as TakeStorage` at `take.ts:72` to narrow; there is no
  runtime check. Any Rust design should not sneak in a runtime `panic!` —
  either the types force correctness at compile time (Option B) or the
  narrow methods don't exist generically (Option A).

## Citation

- TypeScript function overload semantics (the mechanism this pattern
  exploits): https://www.typescriptlang.org/docs/handbook/2/functions.html#function-overloads .
- Rust RFC 3668 and prior discussions explicitly rule out method overloading:
  https://rust-lang.github.io/rfcs/ (search "overload") — the canonical
  Rust answer is different method names or an `enum`.
