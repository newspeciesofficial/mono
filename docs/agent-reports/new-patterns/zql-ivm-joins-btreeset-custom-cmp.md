# New pattern: BTreeSet with custom comparator (sorted map keyed by UTF-8 string)

## Category
D (Data)

## Where used
- `packages/zql/src/ivm/memory-storage.ts:1-50` — the entire module.
- Backing implementation: `packages/shared/src/btree-set.ts` (internal).

## TS form
```ts
import {compareUTF8} from 'compare-utf8';
import {BTreeSet} from '../../../shared/src/btree-set.ts';

type Entry = [key: string, value: JSONValue];

function comparator(a: Entry, b: Entry): number {
  return compareUTF8(a[0], b[0]);
}

export class MemoryStorage implements Storage {
  #data: BTreeSet<Entry> = new BTreeSet(comparator);

  set(key: string, value: JSONValue) { this.#data.add([key, value]); }
  get(key: string, def?: JSONValue) {
    const r = this.#data.get([key, null]);            // lookup uses only key
    return r !== undefined ? r[1] : def;
  }
  del(key: string) { this.#data.delete([key, null]); }
  *scan(options?: {prefix: string}) {
    for (const entry of this.#data.valuesFrom(options && [options.prefix, null])) {
      if (options && !entry[0].startsWith(options.prefix)) return;
      yield entry;
    }
  }
  cloneData() { return structuredClone(Object.fromEntries(this.#data.values())); }
}
```

## Proposed Rust form
```rust
use std::collections::BTreeMap;

// No custom comparator: keys are `String`, and std BTreeMap already orders
// `String` by UTF-8 byte order (guide Part 2 notes "Valid UTF-8 byte order
// == codepoint order"). So the TS "custom comparator" is actually a no-op
// in Rust.

pub struct MemoryStorage {
    data: BTreeMap<String, JsonValue>,
}

impl MemoryStorage {
    pub fn set(&mut self, key: String, value: JsonValue) { self.data.insert(key, value); }
    pub fn get(&self, key: &str) -> Option<&JsonValue> { self.data.get(key) }
    pub fn del(&mut self, key: &str) { self.data.remove(key); }

    pub fn scan<'a>(&'a self, prefix: Option<&'a str>) -> impl Iterator<Item = (&'a str, &'a JsonValue)> + 'a {
        let start: &str = prefix.unwrap_or("");
        self.data
            .range::<str, _>(start..)
            .take_while(move |(k, _)| match prefix {
                Some(p) => k.starts_with(p),
                None => true,
            })
            .map(|(k, v)| (k.as_str(), v))
    }

    pub fn clone_data(&self) -> BTreeMap<String, JsonValue> { self.data.clone() }
}
```

## Classification
- Direct — `BTreeMap<String, JsonValue>` is a near-perfect match. The TS code
  uses `BTreeSet<[string, JSONValue]>` with a key-only comparator as a poor
  man's map; Rust uses a real map.

## Caveats
- The TS signature allows `scan()` with no arguments to mean "scan all". The
  Rust form uses `Option<&str>` for the prefix; keep the API shape consistent
  with how callers use it (`Storage::scan` in `operator.ts:115` takes an
  `options?: {prefix: string}` — a single optional param).
- `cloneData()` returns a *deep* clone (`structuredClone`). `BTreeMap::clone()`
  on `BTreeMap<String, serde_json::Value>` is deep because `serde_json::Value`'s
  `Clone` impl recurses. No separate `structuredClone` crate needed — see
  `zql-ivm-joins-structured-clone.md`.
- If the stored `JsonValue` type ever contains `Arc`/`Rc`, `.clone()` becomes
  a shallow bump; that is *not* the current shape (it's a tree of owned
  `Value`s) but worth noting for reviewers.
- Do **not** reach for `std::collections::BTreeSet<(String, JsonValue)>` —
  that would require `Ord` on `JsonValue`, which is not implemented for
  `serde_json::Value`. Use a map keyed by the string.

## Citation
- Rust stdlib, `BTreeMap::range` —
  https://doc.rust-lang.org/std/collections/struct.BTreeMap.html#method.range
- guide Part 2 row for `compare-utf8` → built-in `str::cmp`.
