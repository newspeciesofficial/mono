# New pattern: tmpdir() + path.join() for per-worker scratch

## Category
B (Node stdlib)

## Where used
- `packages/zero-cache/src/server/pool-thread.ts:30` (`import {tmpdir} from 'node:os'`)
- `packages/zero-cache/src/server/pool-thread.ts:31` (`import path from 'node:path'`)
- `packages/zero-cache/src/server/pool-thread.ts:98` (`path.join(tmpdir(), 'pool-thread-…')`)

## TS form
```ts
import {tmpdir} from 'node:os';
import path from 'node:path';
const dir = path.join(tmpdir(), `pool-thread-${idx}-${randomUUID()}`);
```

## Proposed Rust form
```rust
use std::path::PathBuf;
let dir: PathBuf = std::env::temp_dir()
    .join(format!("pool-thread-{}-{}", idx, uuid::Uuid::new_v4()));
```

Notes:
- `std::env::temp_dir()` respects `TMPDIR` on Unix and `TMP`/`TEMP`/`USERPROFILE` on Windows, matching `os.tmpdir()` semantics.
- `Path::join` does not touch the filesystem — same as `path.join`.
- Use `PathBuf` when you own the path, `&Path` for borrows.
- For atomic temp-file creation instead, prefer the `tempfile` crate (already
  in `Cargo.toml` per Part 4). The `pool-thread.ts` case creates a *directory*
  the caller owns for its lifetime, so `tempfile::TempDir::new_in(...)` would
  be a safer choice (auto-cleanup on drop). The stock TS code does not
  auto-clean; the Rust port can opt in without behavioural regression.

## Classification
Direct — shape-for-shape path construction.

## Caveats
- `os.tmpdir()` does not create the directory. `std::env::temp_dir()` doesn't either.
- `randomUUID()` is handled in `pipeline-driver-random-uuid.md`.

## Citation
- `std::env::temp_dir` — https://doc.rust-lang.org/std/env/fn.temp_dir.html
- `std::path::Path::join` — https://doc.rust-lang.org/std/path/struct.Path.html#method.join
- `tempfile::TempDir` — https://docs.rs/tempfile/latest/tempfile/struct.TempDir.html
