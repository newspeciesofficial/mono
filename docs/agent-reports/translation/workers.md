# Translation report: workers

## Scope
Apply Phase 1 Decision **D3 (single-process tokio)** to the Rust server:
remove speculative multi-process handoff, commit the routing-hash choice,
ensure WebSocket fan-out parity with TS. This is a cleanup + consolidation
pass, not a fresh port — the Rust tree already assumed single-process.

## Files ported / touched
- `packages/zero-cache-rs/Cargo.toml` — added `xxhash-rust = { version = "0.8", features = ["xxh32"] }` to workspace deps.
- `packages/zero-cache-rs/crates/server/Cargo.toml` — consumed `xxhash-rust.workspace = true`.
- `packages/zero-cache-rs/crates/server/src/dispatch.rs` — **new**, 210 LOC. Single-process hash dispatcher (`h32(taskID + "/" + clientGroupID) % N`) + mpsc-channel fan-out to N tokio sync-worker tasks.
- `packages/zero-cache-rs/crates/server/src/lib.rs` — wire `Dispatcher` into `AppState`; WS upgrade handler now routes through `Dispatcher::dispatch`, not directly into `Syncer::handle_connection`.
- `packages/zero-cache-rs/crates/server/src/syncer.rs` — rewrote `RoutingState::persist()` to use `tokio::fs` (write-tmp + atomic rename) with a `flush_pending` debounce flag. Field `routing_state: Option<RoutingState>` → `Option<Arc<RoutingState>>` so async flush tasks can outlive the caller. Retained `persist_blocking()` as the startup-read + unit-test primitive. Added `test_routing_state_persist_async_atomic_rename`.
- `packages/zero-cache-rs/crates/server/src/main.rs` — spawn `Dispatcher` with `config.num_sync_workers` workers and task ID `"{app_id}-{shard_num}"`.
- `packages/zero-cache-rs/crates/server/tests/ws_connect.rs` — updated three `AppState` construction sites to include the new `dispatcher` field.
- `packages/zero-cache-rs/crates/zql-ivm/src/lib.rs` — **incidental unblock**: added a placeholder `lib.rs` so the workspace parses (the crate had source files but no manifest target, causing `cargo build -p zero-cache-server` to fail at workspace-resolution time). Flagged for the `zql-ivm-*` translator to replace with real module declarations.

## Patterns applied
- **D3 (Phase 1 decision — single-process tokio)**: dropped multi-process child-worker topology. No `sendfd`, no `SCM_RIGHTS`, no `process.send` — both the TS `Syncer` workers and the `WorkerDispatcher` collapse into tokio tasks on one runtime. The Rust tree already had no speculative multi-process code (`grep` returned zero hits for `sendfd|SCM_RIGHTS|process\.send|worker_thread`), so this reduces to wiring the dispatch layer explicitly.
- **C5 / new-pattern `workers-js-xxhash-routing-hash`**: `xxhash-rust::xxh32::xxh32(bytes, 0)` replaces TS `js-xxhash`'s `xxHash32(s, 0)`. Canonical vectors verified: `""` → `0x02CC5D05`, `"abc"` → `0x32D153FF`, `"Nobody inspects the spammish repetition"` → `0xE2293B2F`.
- **B16 / new-pattern `workers-node-fs-sync-assignments-persistence`**: startup read stays blocking `std::fs::read_to_string` (pre-bind, safe); write from the async dispatch hot path uses `tokio::fs::write` on `<file>.tmp` followed by `tokio::fs::rename` — atomic replace, never blocks the reactor. `flush_pending: AtomicBool` debounces bursts so a storm of concurrent assignments coalesces into a single write.
- **A1, A6, B1 (keepalive), E6 (base64url), B9 (URL)** — unchanged; preserved the existing WebSocket upgrade, `sec-websocket-protocol` decode, CORS, auth extraction as-is.

## Deviations
- **Dispatch granularity**: TS dispatches to `syncers: Worker[]` (process array); Rust dispatches to `Vec<mpsc::Sender<Handoff>>`. Each worker task still shares the single `Arc<Syncer>`, so the worker boundary only shapes handoff fan-out — it does not partition state. This matches D3 literally ("collapses `Syncer`, `Mutator`, `WorkerDispatcher` and the handoff glue into one binary").
- **Task ID**: TS reads `taskID` from env / args; Rust synthesises it from `"{app_id}-{shard_num}"`. Deterministic and stable across restarts, which is all the hash cares about. If cross-language blue/green ever needs bit-level hash parity, the Rust task ID must match whatever the TS deployment uses — noted in `syncer.rs` but not env-wired yet (no config gap, just a future hook).
- **Debounce**: TS writes `writeFileSync` on *every* assignment. Rust coalesces via an atomic "flush pending" flag and a single `spawn`ed flush task. This is the explicit caveat in the B16 new-pattern doc ("That pattern will amplify tail latency under churn. Consider coalescing writes during porting."). Behavioural outcome (assignments persisted across restart) is preserved.
- **`zql-ivm` placeholder lib.rs** (incidental, not D3-related): an untracked sibling crate had source files but no `lib.rs`, breaking `cargo` workspace parsing. Added a comment-only stub so the server crate builds. Flagged for the owning translator.

## Verification
- `cargo build -p zero-cache-server`: **ok** (2 pre-existing warnings, unrelated — `DOWNSTREAM_MSG_INTERVAL_MS` constant, `std::str::FromStr` unused import in `db`).
- `cargo test -p zero-cache-server`: **36 unit + 3 integration passed, 0 failed, 0 ignored.**
  - New xxHash32 parity tests: `h32_abc_seed_0`, `h32_empty_seed_0`, `h32_stable_snapshot`, `worker_index_matches_h32_modulo_n`, `worker_index_is_deterministic`, `worker_index_depends_on_both_inputs`, `worker_index_with_single_worker`, `dispatch_routes_to_worker`.
  - New persistence test: `test_routing_state_persist_async_atomic_rename` (verifies async flush via `tokio::fs`, coalescing under three concurrent `.persist()` calls, and absence of leftover `.tmp`).
  - All pre-existing unit + integration tests (`ws_connect` × 3, connection × 6, ws_handler × 10, syncer × 5, health × 2, shutdown × 1) still green.
- `docs/ts-vs-rs-comparison.md` rows 18 and 19 updated with the D3 + B16 signal.

## Next-module dependencies unblocked
- **None new blocked** on this row. The dispatcher is a drop-in: any future multi-node / multi-pod work can swap `Dispatcher` for a real cross-process router without touching call sites.
- **Unblocks observability**: the `worker_id` now appears in tracing spans for every dispatch, which makes the Phase 2 observability work (`statz`, OTEL) straightforward.
- **Unblocks auth routing (row 20)**: the `ConnectParams` → `Syncer` path now has a single, well-defined entry point via `Dispatcher::dispatch` + `Syncer::handle_connection`. Adding JWT signature verification between `parse_connect_params` and `dispatch` is a localised change.

## Anti-patterns deliberately avoided
- Did **not** add `sendfd`, `nix::sys::socket::sendmsg`, or `unix::UnixStream` — per D3 those aren't needed.
- Did **not** introduce a new crate outside the guide (only `xxhash-rust`, which is the exact crate the new-pattern doc proposes).
- Did **not** touch TS source.
- Did **not** refactor the Syncer's internal shape; only the upstream routing into it.
