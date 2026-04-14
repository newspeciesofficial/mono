# New pattern: js-xxhash `xxHash32` for stable routing hash

## Category
Library.

## Where used
- `packages/zero-cache/src/server/worker-dispatcher.ts:5` (`import {h32}`) and
  `worker-dispatcher.ts:71` (`h32(taskID + '/' + clientGroupID) % syncers.length`)
- Source of `h32`: `packages/shared/src/hash.ts:3` — `export const h32 = (s: string) => xxHash32(s, 0)` from `js-xxhash`.

## TS form
```ts
import {xxHash32} from 'js-xxhash';
export const h32 = (s: string) => xxHash32(s, 0);

// Routing fallback when there is no persisted assignment file:
return h32(taskID + '/' + clientGroupID) % syncers.length;
```

`js-xxhash` is xxHash32 (32-bit, non-cryptographic, seed 0). This output must be
preserved bit-for-bit because hash-based syncer routing must map the same
`(taskID, clientGroupID)` tuple to the same worker across TS and Rust servers,
otherwise sticky routing breaks.

## Proposed Rust form
```rust
// xxhash-rust — pure-Rust xxHash with a stable 32-bit variant.
use xxhash_rust::xxh32::xxh32;

fn h32(s: &str) -> u32 {
    xxh32(s.as_bytes(), 0)
}

// Usage:
let idx = (h32(&format!("{taskID}/{clientGroupID}")) as usize) % syncers.len();
```

## Classification
Idiom-swap — swap the crate, keep the algorithm (xxHash32 with seed 0).

## Caveats
- xxHash32 ≠ xxHash64 ≠ xxh3. Pick `xxh32` specifically.
- `js-xxhash` and `xxhash-rust` both return `u32` for xxHash32 and match the
  reference implementation by Yann Collet. Verify with the canonical test
  vectors (`"abc"` at seed 0 → `0x32D153FF`) before trusting cross-language
  equivalence.
- If the whole Rust syncer replaces the whole TS syncer atomically, the hash
  cross-compat is only needed during blue/green rollout; still worth keeping
  exact to avoid silent reshuffles.

## Citation
- `xxhash-rust` crate (100% safe Rust, supports xxh32/xxh64/xxh3):
  https://docs.rs/xxhash-rust/latest/xxhash_rust/xxh32/index.html
- Algorithm spec: https://github.com/Cyan4973/xxHash
