# New pattern: crypto.randomUUID() for per-thread scratch IDs

## Category
B (Node stdlib)

## Where used
- `packages/zero-cache/src/server/pool-thread.ts:29` (`import {randomUUID} from 'node:crypto'`)
- `packages/zero-cache/src/server/pool-thread.ts:98` (`path.join(tmpdir(), 'pool-thread-${idx}-${randomUUID()}')`)

## TS form
```ts
import {randomUUID} from 'node:crypto';
const dir = path.join(tmpdir(), `pool-thread-${poolThreadIdx}-${randomUUID()}`);
```

## Proposed Rust form
```rust
use uuid::Uuid;
use std::path::PathBuf;

let dir: PathBuf = std::env::temp_dir()
    .join(format!("pool-thread-{}-{}", pool_thread_idx, Uuid::new_v4()));
```

`uuid` is already listed for addition to `Cargo.toml` in Part 4 of the guide
(present in `Cargo.toml` today — see `docs/rust-translation-guide.md:215`
"`uuid`"). No new crate needed.

## Classification
Direct — `crypto.randomUUID()` emits an RFC 4122 v4 UUID as a hyphenated
lowercase string; `uuid::Uuid::new_v4().to_string()` is byte-identical.

## Caveats
- `Uuid::new_v4()` uses `getrandom` (CSPRNG on Linux). Matches
  `crypto.randomUUID()` security properties.
- For the scratch-directory use case we don't need cryptographic randomness —
  `nanoid` (also in Part 4) would be equally valid and shorter. Use `uuid`
  only because it's already present.

## Citation
- `uuid` crate — https://docs.rs/uuid/latest/uuid/struct.Uuid.html#method.new_v4
- `crypto.randomUUID` — https://nodejs.org/api/crypto.html#cryptorandomuuidoptions
