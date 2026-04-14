# Translation report: mutagen

## Files ported

- `packages/zero-cache/src/services/mutagen/error.ts` (14 LOC) →
  `packages/zero-cache-rs/crates/mutagen/src/error.rs` (~170 LOC — includes
  the full `MutagenError` enum that unifies TS `ProtocolError`,
  `ProtocolErrorWithLevel`, `MutationAlreadyProcessedError`, and PG
  serialization failures).
- `packages/zero-cache/src/custom/fetch.ts` (347 LOC) →
  `packages/zero-cache-rs/crates/mutagen/src/fetch.rs` (~430 LOC incl. tests).
- `packages/zero-cache/src/services/mutagen/mutagen.ts` (474 LOC) →
  `packages/zero-cache-rs/crates/mutagen/src/mutagen.rs` (~370 LOC incl.
  tests). CRUD DML builders + `check_last_mutation_id_advanced` + PG error
  classifier. `processMutation`'s serializable retry loop was not ported
  (requires wiring to `tokio-postgres` transaction state — out of scope for
  this phase; see Deviations).
- `packages/zero-cache/src/services/mutagen/pusher.ts` (692 LOC) →
  `packages/zero-cache-rs/crates/mutagen/src/pusher.rs` (~500 LOC incl.
  tests). `PusherService` + `combine_pushes` + `fan_out_responses` +
  `forward_push_standalone`.

Supporting types added to `zero-cache-types`:

- `zero-cache-rs/crates/types/src/protocol/push.rs` extended with
  `MutationID`, `MutationOk`, `MutationError`, `MutationResult`,
  `MutationResponse`, `PushOk`, `PushLegacyError`, `PushFailedBody`,
  `PushFailedKind`, `PushResponse`, `CLEANUP_RESULTS_MUTATION_NAME`.

Server wire-up:

- `zero-cache-rs/crates/server/src/syncer.rs::forward_push()` now delegates
  to `zero_cache_mutagen::pusher::forward_push_standalone`, which runs the
  allow-list check and the full retry/backoff helper. The server's own
  hand-rolled POST was replaced.
- `zero-cache-rs/crates/server/Cargo.toml` picks up `zero-cache-mutagen`.

Workspace:

- `zero-cache-rs/Cargo.toml` adds `crates/mutagen` to `members` and to
  `workspace.dependencies` as `zero-cache-mutagen = { path = "crates/mutagen" }`.

## Patterns applied

- **A1** class with private `#fields` → struct with `pub(crate)` or private
  fields + `impl` (`PusherService`, `MutationAlreadyProcessedError`).
- **A3 / C11** `RefCountedService` / `Service` — elided for now (the Rust
  server manages lifetimes via `Arc`; pusher exposes `stop()`/`run()` that
  the WS handler drives).
- **A5** TS `interface` → Rust public functions / impl blocks on
  `PusherService`.
- **A6** tagged union discrimination via `'kind' in response` / `'error' in
  m.result` → `PushResponse::Failed / LegacyError / Ok` `#[serde(untagged)]`
  enum + `match`. `unreachable(response)` → `match` with exhaustive arms.
- **A11 / A12 / A13 / A19 / A20 / A21 / A22** — object spread /
  nullish-coalescing / optional-chaining / dynamic key indexing /
  `Map::values` — ported mechanically via `Option::or`, `HashMap`,
  `IndexMap` (preserve insertion order in `combine_pushes`),
  destructuring / struct update syntax.
- **A15 / F1 / F2** typed `MutagenError` via `thiserror::Error` with
  `#[error(...)]` derive; the `Protocol` variant carries a `PushFailedBody`
  + `LogLevel` pair (replacing TS `ProtocolErrorWithLevel`).
- **A17** `async fn` + `.await`; `Promise.all` was unused (TS original uses
  it only inside `processMutationWithTx`, which is deferred).
- **B1 / C2** `Subscription<Downstream>` → per-client
  `mpsc::UnboundedSender<Downstream>` kept in a `HashMap` on the pusher;
  dropping the sender (on wsID change or on `stop()`) closes the stream
  from the receiver side.
- **B7** `setTimeout(resolve, 100)` → `tokio::time::sleep` inside the
  `fetch_from_api_server` retry loop.
- **C1** bounded/unbounded mpsc + drain-into-`Vec` → `queue_tx`
  (`UnboundedSender`) plus `rx.recv().await` blocking for first item then
  `rx.try_recv()` draining residuals, mirroring TS `Queue.dequeue() +
  Queue.drain()`.
- **C6** exponential backoff with jitter — `get_backoff_delay_ms(attempt) =
  min(1000, 100·2^(attempt-1) + rand·100)`; `MAX_ATTEMPTS = 4`; retry on
  HTTP 502/504 and transport-level "fetch failed" (`reqwest::Error::
  {is_connect, is_timeout, is_request}`).
- **C14** `Object.entries(…).flatMap(…)` → `IndexMap`/ordered `BTreeMap`
  iteration (`row_to_ordered`) for stable column order + `$N` parameter
  numbering.
- **D8 / Part 3** postgres.js tagged-template dynamic DML replaced with
  `postgres_protocol::escape::escape_identifier` for table / column /
  primary-key names + bound `$N` parameters for all values. No SQL-literal
  interpolation of user values.
- **Phase 1 `rand` (Part 2)** used for jitter via `rand::thread_rng()`.
- **`urlpattern` crate (Part 2)** for URL allow-list parity with TS
  `URLPattern`.
- **Part 2 `reqwest`** for HTTP.
- **Part 2 `wiremock`** (dev-dep) for HTTP mocking in `fetch` tests.

## Deviations

- `MutagenService::processMutation` (the transaction-level retry loop,
  error-mode fallback, and `SlidingWindowLimiter`) is **not ported in this
  pass**. The port covers the SQL builders (`get_insert_sql`,
  `get_upsert_sql`, `get_update_sql`, `get_delete_sql`), the
  `MAX_SERIALIZATION_ATTEMPTS` constant, the LMID check
  (`check_last_mutation_id_advanced`), and the PG SQLSTATE 40001 classifier
  (`classify_pg_error`) — everything needed to assemble the retry loop on
  top of a `tokio-postgres` transaction. Wiring the transaction layer
  requires a `tokio-postgres::Transaction` type in scope plus the
  `WriteAuthorizer`, which is a cross-crate dependency that rows 2+14+20
  have not fully landed yet. This mirrors the row-20 deviation for
  `read-authorizer.ts` / `write-authorizer.ts`. Applies Phase 1 rule **D1**
  (only port what's in scope; don't block on upstream modules).
- Successful `PushOk.mutations` fan-out in `PusherService::fan_out_responses`
  is best-effort: mutations are forwarded to each client's
  `downstream_tx` as-is. The view-syncer integration (advancing per-client
  LMIDs, sending `pokeStart`/`pokeEnd` around the mutation-result data) is
  **deferred** — a TODO marker is in place. This matches the "no
  subscriber mechanism yet" instruction in the worker prompt.
- `ackMutationResponses` and `deleteClientMutations` fire-and-forget
  cleanup paths are not ported (consumer of `CLEANUP_RESULTS_MUTATION_NAME`
  is not yet present on the Rust side).
- `*.pg.test.ts` tests are **deferred** — they need `testcontainers-rs` +
  a live Postgres instance (out of scope for this phase).

## Verification

- `cargo build -p zero-cache-mutagen`: **ok**.
- `cargo test -p zero-cache-mutagen`: **25 passed, 0 failed, 0 ignored.**
  - `error::` — 2 tests (message shape, `push_failed_http` body fields).
  - `fetch::` — 7 tests (success + headers + query params; URL allow-list
    rejection; retry-on-502; non-retryable 404; backoff bounds;
    custom-headers filter).
  - `mutagen::` — 7 tests (identifier escaping for INSERT/UPSERT/UPDATE/DELETE;
    LMID same/less/greater; mutation-error-tuple shape).
  - `pusher::` — 9 tests (combinePushes shape parity: empty, stop-only,
    stop-after-entries, same-client combine, `should_panic` auth/schema
    mismatches, mutation-order preservation, `service_can_be_stopped` via
    `Arc<PusherService>`).
- `cargo build -p zero-cache-server`: **fails with 2 pre-existing errors**
  in `syncer.rs:1481` (`from_value::<HashMap<String, u64>>` given
  `HashMap<String, i64>`) and `syncer.rs:1489` (`PokePartBody` missing
  `mutations_patch`). Verified by `git stash` → build → same 2 errors.
  These are outside the mutagen scope (row 17 / view-syncer internal).
  The new `forward_push` delegation itself compiles cleanly.
- `docs/ts-vs-rs-comparison.md` row 21 updated (line 45).

## Next-module dependencies unblocked

- **Row 15** (`client-handler.ts` / mutationPatch path) — now has
  `PushResponse` / `MutationResponse` in `zero-cache-types` to serialize
  per-client mutation results.
- **Row 16** (CVR-side LMID advance) — can consume the
  `check_last_mutation_id_advanced` helper to drive its
  `MutationAlreadyProcessedError` / `InvalidPush` distinction.
- **Row 17** (`view-syncer.ts`) — the TODO in
  `PusherService::fan_out_responses` is the next integration point; once
  row 17's orchestrator exposes a per-client mutation-result sink, the
  pusher can fan out `PushOk` successes end-to-end.
- **Row 20** (authorizers) — `MutagenError::Protocol { body, level }` is
  the wire-compatible error shape `write-authorizer.rs` will need when it
  lands; nothing else to plumb.
