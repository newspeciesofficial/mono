# New pattern: synchronous `node:fs` read/write for routing-assignment persistence

## Category
B (Node stdlib).

## Where used
- `packages/zero-cache/src/server/worker-dispatcher.ts:2` (`import {existsSync, readFileSync, writeFileSync} from 'node:fs'`)
- `worker-dispatcher.ts:38-54` — `existsSync` + `readFileSync('utf-8')` +
  `JSON.parse` on startup to rehydrate `(clientGroupID → syncer index)` map.
- `worker-dispatcher.ts:56-66` — `writeFileSync(assignmentsFile, JSON.stringify(data))`
  called inline from the request path (`assignSyncer`) on every new assignment.

## TS form
```ts
import {existsSync, readFileSync, writeFileSync} from 'node:fs';

if (assignmentsFile && existsSync(assignmentsFile)) {
  const data = JSON.parse(readFileSync(assignmentsFile, 'utf-8'));
  // …
}

function persistAssignments() {
  if (!assignmentsFile) return;
  writeFileSync(assignmentsFile, JSON.stringify(data));
}
```

Called synchronously inside `assignSyncer`, i.e. on the WebSocket handoff hot
path. Contrast with all the other `zero-cache` fs I/O, which is async.

## Proposed Rust form
```rust
// Blocking I/O on startup is fine (pre-server bind):
use std::{fs, path::Path};
use serde_json::from_str;

if let Some(path) = assignments_file.as_deref() {
    if Path::new(path).exists() {
        let data = fs::read_to_string(path)?;
        let map: HashMap<String, usize> = from_str(&data)?;
        // …
    }
}

// Writing from the async dispatch hot path: NEVER use std::fs there.
// Option A — offload to blocking pool (preserves TS "synchronous-looking" semantics
// but does not block the reactor):
tokio::task::spawn_blocking({
    let path = path.to_owned();
    let payload = serde_json::to_vec(&data)?;
    move || std::fs::write(path, payload)
})
.await??;

// Option B — tokio::fs (async-native):
tokio::fs::write(path, serde_json::to_vec(&data)?).await?;
```

## Classification
Idiom-swap — `std::fs` covers the startup read; the per-request `writeFileSync`
cannot be copied verbatim because `std::fs::write` inside an async request
handler would block the tokio reactor. Route through `tokio::fs` or
`spawn_blocking`.

## Caveats
- The TS implementation writes the full JSON blob on every assignment with no
  debouncing. That pattern will amplify tail latency under churn. Consider
  coalescing writes (e.g. a dirty-flag + periodic flush task) during porting.
- `writeFileSync` in Node does a write + fsync-on-close of the fd but does not
  do atomic rename. Rust port should either (a) match that (simple write) or
  (b) write to `path.tmp` and `fs::rename` to be crash-safe — be explicit about
  the choice.
- File permissions: Node defaults to `0o666 & ~umask`; Rust
  `fs::write`/`tokio::fs::write` uses `0o666 & ~umask` on Unix via
  `OpenOptions::create(true).write(true).truncate(true)` — compatible.

## Citation
- `std::fs` module docs (existsSync/readToString/write):
  https://doc.rust-lang.org/std/fs/index.html
- `tokio::fs` for async file I/O from within async tasks:
  https://docs.rs/tokio/latest/tokio/fs/index.html
- `tokio::task::spawn_blocking` for offloading sync I/O:
  https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html
