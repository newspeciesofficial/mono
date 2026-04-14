# New pattern: structuredClone over arbitrary JSONValue

## Category
A (Language)

## Where used
- `packages/zql/src/ivm/memory-storage.ts:48` —
  `structuredClone(Object.fromEntries(this.#data.values()))`

## TS form
```ts
cloneData(): Record<string, JSONValue> {
  return structuredClone(Object.fromEntries(this.#data.values()));
}
```

## Proposed Rust form
```rust
// JSONValue is a plain tree (no Arcs / no cycles). A plain `Clone` on the
// enum gives a structurally-deep copy.

pub fn clone_data(&self) -> BTreeMap<String, JsonValue> {
    self.data.clone()
}
```

## Classification
- Direct — `Clone::clone` on `serde_json::Value` (or any equivalent `JsonValue`
  enum) is already a deep copy because every variant owns its payload.

## Caveats
- `structuredClone` in JS also handles cycles, `Map`, `Set`, `Date`, etc.
  None of those appear in `JSONValue` (it is pure JSON), so the extra
  semantics are not needed.
- Do not introduce `Arc`/`Rc` into the `JsonValue` type without reviewing
  every `clone_data()` call site — a bump-clone would silently share state
  across storages.

## Citation
- `serde_json::Value` —
  https://docs.rs/serde_json/latest/serde_json/enum.Value.html
  (derived `Clone`).
