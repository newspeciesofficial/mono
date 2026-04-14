# Shadow-FFI Framework

Function-level differential testing of the Rust port of zero-cache against the stock TS implementation.

## What it does

For a shadowed function `funcA`:

1. TS calls `funcA()` normally and returns its result to production.
2. When `ZERO_SHADOW=1`, every nondeterministic IO `funcA` does (PG query, SQLite read, UUID, time read) is captured into a `RecordCtx` of per-IO-kind FIFO queues.
3. The TS harness invokes the matching Rust shadow `funcA_rust(input, replayCtx)`. Inside Rust, the equivalent IO primitives consume from the recording instead of touching real PG / SQLite / RNG / clock.
4. The two outputs are diffed. Divergences are logged (or thrown, depending on call site).
5. Production behaviour is unchanged — TS's result is still what's returned.

## Why per-kind queues, not a global trace

Sequential per-kind ordering catches the bugs we actually care about (wrong SQL, different params, missing call) while tolerating cross-kind reordering that's usually semantically equivalent. The function's output diff catches the rare cross-kind ordering bugs.

## Why opaque handles for stateful shadows

Per-ClientGroup state (e.g. an IVM operator graph) lives behind a napi-rs `External<Arc<Mutex<T>>>` handle owned by the TS object that owns the ClientGroup. There is **no** global Rust registry — handles are the only path to state, so cross-group leakage is structurally impossible.

## Files

| TS              | Rust                           | Role                                                                |
| --------------- | ------------------------------ | ------------------------------------------------------------------- |
| `record-ctx.ts` | `replay_ctx.rs`                | Per-IO-kind buffers; `withCtx` / `enter` install the active context |
| `wrappers.ts`   | (built into shadow primitives) | Recording wrappers around `Date.now`, UUID factories, etc.          |
| `diff.ts`       | —                              | Output comparator (BigInt ↔ string, Buffer ↔ array tolerant)        |
| `native.ts`     | `lib.rs` + `exports/`          | Loader and surface of the napi `.node` module                       |
| —               | `handle.rs`                    | `External<Arc<Mutex<T>>>` scaffolding for stateful shadows          |
| —               | `exports/lsn.rs`               | Demo: pure-function `lsn_to_big_int` / `lsn_from_big_int` shadows   |

## How to add a new shadow

### 1. Pure function (no state, no IO)

**Rust:** add a `#[napi]` export under `exports/`:

```rust
#[napi(js_name = "my_module_my_func")]
pub fn my_module_my_func(arg: String) -> napi::Result<MyResult> {
    let v = real_rust_impl(&arg)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    Ok(v.into())
}
```

Add the module in `exports/mod.rs`. Run:

```bash
cd packages/zero-cache-rs/crates/shadow-ffi && npx napi build --platform --release --js index.js --dts index.d.ts
```

**TS:** add the function signature to `ShadowNative` in `native.ts`. Wrap the call site:

```ts
import {shadow, loadShadowNative, diff} from '../shadow/index.ts';

export function myFunc(arg: string): MyResult {
  const ts = myFuncImpl(arg);
  if (shadow.enabled) {
    const native = loadShadowNative();
    diff('myFunc', ts, native.my_module_my_func(arg));
  }
  return ts;
}
```

### 2. Function that does IO (PG / SQLite / UUID / time)

Same as above plus:

- Wrap the call site in `withCtx(new RecordCtx(), () => …)` so the recording is captured.
- Pass `ctx.toReplay()` to the Rust shadow as its last argument (Rust signature takes `replay: serde_json::Value`).
- Inside the existing TS code, swap raw `Date.now()` / `nanoid()` / pg-pool calls for the recording wrappers (or instrument the chokepoint module that exports them).

### 3. Stateful function (per-ClientGroup state)

- Define a Rust state struct, e.g. `MyState`.
- Add a `#[napi]` `create_my_state(client_group_id: String) -> Handle<MyState>` and a `destroy_my_state(handle)` export. Use `crate::handle::make_handle` and `with_handle`.
- TS owns one handle per ClientGroup, stored on the per-ClientGroup object (e.g. `ViewSyncerService`).
- Every shadowed call passes the handle as its first FFI arg.

## Running

```bash
# Build the .node binary once
cd packages/zero-cache-rs/crates/shadow-ffi
npx napi build --platform --release --js index.js --dts index.d.ts

# Run shadow tests (pure-function tests skip cleanly if .node missing)
cd packages/zero-cache
npx vitest run src/shadow

# Enable in dev
ZERO_SHADOW=1 npm run start-zero-cache
```

## Status

Framework + LSN demo shadow landed. Next: replace each TS worker one at a time, gated by shadow comparisons.
