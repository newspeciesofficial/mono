# Validation report: mutagen

## Scope
- `packages/zero-cache/src/services/mutagen/mutagen.ts` — 474
- `packages/zero-cache/src/services/mutagen/pusher.ts` — 692
- `packages/zero-cache/src/services/mutagen/error.ts` — 14
- Total LOC: 1180

Transitive read (not in scope, referenced for retry/backoff + fan-out semantics):
`packages/zero-cache/src/custom/fetch.ts:95-346` (`MAX_ATTEMPTS = 4`,
`getBackoffDelayMs` = `min(1000, 100 * 2^(attempt-1) + rand*100)`; 502/504 + `fetch failed`
retry path; URLPattern allow-list; headers: `X-Api-Key`, `Authorization: Bearer …`,
forwarded `Cookie`, `Origin`, filtered `customHeaders`; reserved params `schema`, `appID`).

## Summary
| metric | count |
|---|---|
| TS constructs used | 21 |
| Constructs covered by guide | 20 |
| Constructs NOT covered | 1 |
| Libraries imported | 9 |
| Libraries covered by guide | 9 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 2 |

## Constructs used
| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class` with private `#fields` | mutagen.ts:59-107, pusher.ts:69-101, 285-331 | A1, A23 | covered |
| `interface` → trait | mutagen.ts:51-57 (`Mutagen`), pusher.ts:33-52 (`Pusher`) | A5 | covered |
| `implements Service, RefCountedService` | mutagen.ts:59, pusher.ts:69; ref/unref/hasRefs at mutagen.ts:109-124, pusher.ts:240-255 | A3, C11 | covered |
| Tagged union discrimination via `'kind' in response` / `'error' in response` / `'error' in m.result` | pusher.ts:412, 429, 469, 492, 501 | A6 | covered |
| `unreachable(op)` / `unreachable(response)` / `unreachable(msgType)` | mutagen.ts:365, pusher.ts:476 | A6 | covered |
| Custom error class + `throw`/`try`/`catch` | mutagen.ts:263-307 (`MutationAlreadyProcessedError`, `ProtocolError`, `postgres.PostgresError`), error.ts:3-14 | A15, F1, F2 | covered |
| `async function` / `await` / `Promise.all` | mutagen.ts:237, 345-348, 384; pusher.ts:140-238, 387-401, 559-600 | A17 | covered |
| Retry loop with exponential backoff + jitter (invoked via `fetchFromAPIServer`) | pusher.ts:172-186, 223-232, 565-584 → custom/fetch.ts:175-187, 344-346 | C6 | covered |
| `postgres` error instanceof + SQLSTATE code (`40001` = PG_SERIALIZATION_FAILURE) | mutagen.ts:294-298 | F1 (+ `sqlstate` crate in Part 2) | covered |
| `setTimeout(resolve, 100)` delay for InvalidPush retry | mutagen.ts:286 | B7 | covered |
| Object spread `{...a, x: 1}` / `{...entries[0], push: {...entries[0].push, mutations: []}}` | mutagen.ts:412, pusher.ts:588-590, 632-637 | A13, A20 | covered |
| Optional chaining `?.` | mutagen.ts:131 (`this.#limiter?.canDo()`), 287 (`lc.debug?.`), pusher.ts:104, 210, 467 | A11, A21 | covered |
| Nullish coalescing `??` | mutagen.ts:148 (parameter default), pusher.ts:418, 546 | A12 | covered |
| Dynamic key indexing `id[key] = …`, `Object.entries(id).flatMap(…)` | mutagen.ts:412-420 | A19 | covered |
| `for…of` over `Map.entries()` / `Map.values()` | pusher.ts:421, 482, 631 | A22 | covered |
| `Map` as clientID→state | pusher.ts:293-300, 348, 380-383 | A19, D2 | covered |
| `resolver()` (manual promise) | mutagen.ts:64, 163 | Part 2 `@rocicorp/resolver` (→ `tokio::sync::oneshot`) | covered |
| `Queue<PusherEntryOrStop>` with `.dequeue()` + `.drain()` for batching | pusher.ts:90, 133, 267, 389-391 | C1 (bounded mpsc + drain into Vec) | covered |
| `Subscription<Downstream>` with `.cancel()` and `.fail(err)` (per-client outbound) | pusher.ts:297, 356, 378-384, 607 | B1, C2 (broadcast→mpsc fan-out) | covered |
| `groupBy(response.mutations, m => m.id.clientID)` (shared helper) | pusher.ts:417-420, 481 | A19 (HashMap<String, Vec<…>>) | covered |
| postgres.js tagged-template dynamic DML (INSERT/UPDATE/DELETE with `${tx(value)}`, `${tx(primaryKey)}`, array of tagged fragments) | mutagen.ts:391, 399-403, 416-420, 434-437, 447-454 | — | NEW — see `new-patterns/mutagen-postgres-tagged-template-dynamic-dml.md` |

## Libraries used
| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `postgres` (porsager) — `tx\`…\``, `TransactionSql`, `PostgresError`, `PendingQuery` | `tokio-postgres` + `deadpool-postgres` + `postgres-types` (+ `postgres-protocol::escape` for identifiers; `sqlstate` or hardcoded `"40001"` for serialization failure) | covered |
| `@drdgvhbh/postgres-error-codes` (`PG_SERIALIZATION_FAILURE`) | `sqlstate` crate or hand-rolled constants (`postgres-types`) | covered |
| `@rocicorp/logger` (`LogContext`, `.withContext`) | `tracing` + `tracing-subscriber` | covered |
| `@rocicorp/resolver` (`resolver()`) | `tokio::sync::oneshot` | covered |
| `jose` (`JWTPayload` type only — no verification in this module) | `jsonwebtoken` + `jwks-client-rs` / `jwtk` | covered |
| `urlpattern-polyfill` (`URLPattern`, transitively via `custom/fetch.ts`) — `compileUrlPattern`, `pattern.test(url)` | `urlpattern` crate | covered |
| Fetch API (`fetch`, `Response`, `Headers`, `JSON.parse` of body) — transitive via `custom/fetch.ts:189-237` | `reqwest::Client` | covered |
| `@opentelemetry/api` (indirect — `getOrCreateCounter`, `recordMutation`) | `opentelemetry` + `opentelemetry_sdk` + `opentelemetry-otlp` + `tracing-opentelemetry` | covered |
| `shared/*` internal (`assert`, `unreachable`, `must`, `getErrorMessage`, `Queue`, `groupBy`, `sleep`) | built into guide patterns (`tokio::time::sleep`, `anyhow`, `panic!`, `unreachable!`, `VecDeque`) | covered |

## Risk-flagged items depended on
- **HTTP push protocol versioning / schema parse of `PushResponse`** — `pushResponseSchema` is parsed out of the body via `valita`; `ProtocolErrorWithLevel` path in `fetchFromAPIServer` (custom/fetch.ts:249-267) classifies a parse failure as `PushFailed(Parse)`. Translating the valita schema to `serde` is Idiom-swap, but any drift in field names silently downgrades `push` to `PushFailed` and terminates connections — treat parity as a test-gate, not a code-review item.
- **`ProtocolError` / `ProtocolErrorWithLevel` error hierarchy** — pusher.ts:607 wraps a `PushFailedBody` and fails a `Subscription<Downstream>` with the explicit `logLevel: 'warn'`. Guide F1 (`thiserror::Error` enum) handles the enum shape, but the "attach a structured error body AND a log level to an `Err`" idiom (see `types/error-with-level.ts` used at pusher.ts:27) is a cross-cutting pattern reused outside this scope.

## Ready-to-port assessment

This module is close to mechanical except for one meaningful gap: the postgres.js tagged-template literal (`tx\`INSERT INTO ${tx(table)} ${tx(value)} …\``) is used to both escape identifiers and expand an object into `(cols) VALUES ($1,$2,…)` / `SET "c"=$1,…`. `tokio-postgres` does not offer that helper; the Rust port must hand-build the SQL string and the `&[&(dyn ToSql + Sync)]` parameter slice using `postgres_protocol::escape::escape_identifier` for user-supplied identifiers (see `new-patterns/mutagen-postgres-tagged-template-dynamic-dml.md`). Everything else — serializable retry loop (`MAX_SERIALIZATION_ATTEMPTS = 10`, bounded `setTimeout(100)` retry for `InvalidPush`), backoff/jitter (`min(1000, 100·2^(n-1) + rand·100)`, max 4 attempts, retry only on 502/504 or `fetch failed`), `clientID`-keyed push batching (`combinePushes` collapses queued `PusherEntry`s by clientID preserving mutation order), and fan-out-on-response (grouped by `mutationIDs[].clientID`, terminal `failDownstream` on `PushFailed` / `OutOfOrderMutation`) — matches guide idioms.

Blockers before the translator starts on `mutagen` and `pusher`:

1. Add an entry to the guide for the postgres.js dynamic-DML pattern (now captured in `new-patterns/mutagen-postgres-tagged-template-dynamic-dml.md`) so the translator has one approved shape: escape identifiers into the SQL literal, parameterise values, log the final `(sql, params)` tuple for parity with the TS `q.string; q.parameters` debug log at `mutagen.ts:332`.
2. The `custom/fetch.ts` retry/backoff helper is shared by `pusher.ts` and `view-syncer`'s query transform path. Port it as a standalone `reqwest` wrapper first; both callers depend on its exact `PushFailed` / `TransformFailed` tagging semantics.
3. `PushResponse` and `PushFailedBody` live in `zero-protocol` (row 1 in `ts-vs-rs-comparison.md`, "Port complete"). Re-verify that `PushResponse` is present on the Rust side before touching `pusher.ts`; if not, add it to `crates/types` first.

Given 1–3, porting should be straightforward. The fan-out-to-downstream piece
(`PusherService.#fanOutResponses`) hinges on `Subscription<Downstream>` semantics
that already exist on the Rust side (`crates/sync/src/client_handler.rs`); keep the
same `per-clientID onAuthFailure` invariants (pusher.ts:383, 467-474).
