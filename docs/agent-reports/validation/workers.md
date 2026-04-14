# Validation report: workers

## Scope
- `packages/zero-cache/src/workers/syncer.ts` — 294
- `packages/zero-cache/src/workers/connection.ts` — 457
- `packages/zero-cache/src/workers/syncer-ws-message-handler.ts` — 246
- `packages/zero-cache/src/workers/connect-params.ts` — 80
- `packages/zero-cache/src/workers/mutator.ts` — 29
- `packages/zero-cache/src/server/worker-dispatcher.ts` — 222
- Total LOC: 1328

Out of scope (explicitly excluded per assignment): `workers/pool-protocol.ts`
(already validated with pipeline-driver). Out of scope but referenced heavily:
`types/websocket-handoff.ts` (sender and receiver of
`process.send(msg, socket)` cross-worker handoff — see risk section).

## Summary
| metric | count |
|---|---|
| TS constructs used | 22 |
| Constructs covered by guide | 20 |
| Constructs NOT covered | 2 |
| Libraries imported | 9 |
| Libraries covered by guide | 8 |
| Libraries NOT covered | 1 |
| Risk-flagged items the module depends on | 2 |

## Constructs used
| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class` with private `#fields` | syncer.ts:71-130, connection.ts:74-117, syncer-ws-message-handler.ts:22-69, worker-dispatcher.ts:14-33 | A1, A23 | covered |
| `interface` → trait (`MessageHandler`, `SingletonService`, `Service`) | connection.ts:49-51, syncer.ts:71, mutator.ts:8, worker-dispatcher.ts:14 | A5, A3 | covered |
| Tagged union + `switch` on `msg[0]` / `path.worker` / `result.type` / `result.source` | syncer-ws-message-handler.ts:73-242, worker-dispatcher.ts:178-187, connection.ts:217-249 | A6 | covered |
| `unreachable(msgType)` / `unreachable(response)` | syncer-ws-message-handler.ts:241, connection.ts:—; connection.ts (via `findProtocolError` tail recursion) | A6 | covered |
| Generics (`startAsyncSpan<HandlerResult[]>(…)`, `ServiceRunner<ViewSyncer & ActivityBasedService>`, `Subscription<Downstream>`) | syncer-ws-message-handler.ts:83, syncer.ts:74-76, connection.ts:46 | A8 | covered |
| Custom error + `throw`/`try`/`catch` / `ProtocolErrorWithLevel` | connection.ts:177-215, 296-309, 355-364; worker-dispatcher.ts:100-107 | A15, F1, F2 | covered |
| Optional chaining `?.` / nullish coalescing `??` | syncer.ts:146-150, 255, connection.ts:298-299, connect-params.ts:48-49, syncer-ws-message-handler.ts:140 | A11, A12, A21 | covered |
| `async function` / `await` / `Promise.all` | syncer.ts:132-233, connection.ts:177-215, syncer-ws-message-handler.ts:71-245 | A17 | covered |
| `setInterval` / `clearTimeout` for liveness pongs | connection.ts:113-116, 167 | B7 | covered |
| Event-listener add/remove on `ws` (`addEventListener('close'\|'error')`) | connection.ts:109-110, 157-158 | B1 | covered |
| `node:stream` `pipeline(Readable.from(stream), new Writable({objectMode:true,…}))` + `createWebSocketStream` | connection.ts:2, 4, 261-294 | B2 (guide calls out missing `createWebSocketStream` equivalent — split with `futures::StreamExt::split`) | covered |
| `@rocicorp/lock` (`new Lock(); withLock(…)`) for per-connection mutation ordering | syncer-ws-message-handler.ts:2, 51, 147-167 | Part 2 `@rocicorp/lock` → `tokio::sync::Mutex` | covered |
| `@rocicorp/resolver` (manual promise) | syncer.ts:2, 81; mutator.ts:1, 13 | Part 2 `@rocicorp/resolver` → `tokio::sync::oneshot` | covered |
| `@opentelemetry/api` tracer + `startSpan` / `startAsyncSpan` | syncer-ws-message-handler.ts:1, 4, 20, 83-237 | Part 2 `@opentelemetry/*` → `opentelemetry` + `tracing-opentelemetry` | covered |
| `valita.parse(value, upstreamSchema)` for wire-message validation | connection.ts:5, 185-187 | E1/E2 (`serde` custom Deserialize) | covered |
| `URL` / `URLSearchParams` (`new URL(u ?? '', 'http://unused/')`, `URLParams`) | worker-dispatcher.ts:93-107, 156-161, 172-174; connect-params.ts:28, 39 | B9 | covered |
| `node:worker_threads` `MessagePort` (`SyncerWorkerData.replicatorPort`) | syncer.ts:4, 34-36 | B4 | covered |
| `node:process` `pid` | syncer.ts:3, 72; mutator.ts:3, 9 | B6 | covered |
| `Map`-keyed connections (`clientID → Connection`) + replace-on-reconnect | syncer.ts:77, 176-182, 204-219 | A19, D2 | covered |
| Service-registry pattern (`ServiceRunner<V>`) with "keepalive"/`hasRefs` | syncer.ts:74-118 | C11 | covered |
| Graceful drain + timeout wait (`DrainCoordinator.forceDrainTimeout`) | syncer.ts:24, 78, 245-264 | C3, C9 | covered |
| `sec-websocket-protocol` base64url-wrapped URI-encoded JSON payload decode (`decodeSecProtocols`) | connect-params.ts:5, 51-53 → `zero-protocol/src/connect.ts:79-86` (`atob` + `decodeURIComponent` + `TextDecoder`) | E6 (base64url) + B3 (`Uint8Array` → `Vec<u8>`) | covered |
| `node:fs` synchronous `existsSync` / `readFileSync` / `writeFileSync` for assignment persistence (called from the hot path) | worker-dispatcher.ts:2, 38-54, 56-66 | — | NEW — see `new-patterns/workers-node-fs-sync-assignments-persistence.md` |
| `xxHash32`-based routing hash via `h32(taskID + '/' + clientGroupID) % syncers.length` | worker-dispatcher.ts:5, 71; `shared/src/hash.ts:3` | — (Part 2 has no `js-xxhash` entry) | NEW — see `new-patterns/workers-js-xxhash-routing-hash.md` |

## Libraries used
| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `ws` (`WebSocketServer`, `WebSocket`, `createWebSocketStream`, `perMessageDeflate`, `maxPayload`) | `tokio-tungstenite` (raw) + `axum::extract::ws` (HTTP upgrade) | covered |
| `@rocicorp/lock` | `tokio::sync::Mutex` | covered |
| `@rocicorp/logger` | `tracing` + `tracing-subscriber` | covered |
| `@rocicorp/resolver` | `tokio::sync::oneshot` | covered |
| `@opentelemetry/api` (tracer, spans) | `opentelemetry` + `opentelemetry_sdk` + `tracing-opentelemetry` | covered |
| `url-pattern` (`new UrlPattern('(/:base)/:worker/v:version/:action').match(pathname)`) | `urlpattern` crate (or axum router `matchit`) | covered (Part 2 explicitly lists both `url-pattern` and `urlpattern-polyfill`) |
| `node:http2` `IncomingHttpHeaders` (type only, for `connect-params.ts`) | `http::HeaderMap` (via `axum`) | covered (Part 2 axum) |
| `jose` (`JWTPayload` etc., via `auth/jwt.ts` → `tokenConfigOptions`, `verifyToken`) | `jsonwebtoken` + `jwks-client-rs` / `jwtk` | covered |
| `js-xxhash` (`xxHash32`, via `shared/src/hash.ts`) | — (no entry) | NEW — see `new-patterns/workers-js-xxhash-routing-hash.md` |

## Risk-flagged items depended on
- **Cross-process WebSocket handoff** (`process.send(message, socket)` +
  `WebSocketServer({noServer:true}).handleUpgrade(…)`). `types/websocket-handoff.ts`
  is out of scope, but `syncer.ts:122-127` installs the receiver side and
  `server/worker-dispatcher.ts:169-190` the sender side. No tokio/axum drop-in.
  See `new-patterns/workers-child-process-socket-handoff.md` for both
  translation options (single-process tokio tasks, or `SCM_RIGHTS` fd passing
  via `sendfd`). This is the structural pivot of the module.
- **Least-loaded-with-persistence routing** (worker-dispatcher.ts:38-90). The TS
  implementation maintains `assignments: Map<clientGroupID, syncerIdx>`,
  persists on every assignment via `writeFileSync`, and falls back to
  hash-based routing when `assignmentsFile` is unset. Guide pattern C5 covers
  hash-based sticky routing via `ahash`/`DefaultHasher`; persistence and
  least-loaded aren't in C5, but are mechanical once we pick `tokio::fs` vs
  `spawn_blocking` (see `new-patterns/workers-node-fs-sync-assignments-persistence.md`).

## Ready-to-port assessment

These six files are largely mechanical to translate once the two new patterns are added to the guide (`xxhash-rust` crate for `h32` parity, and the node:fs + socket-handoff shapes). The construct set is small: tagged unions, async iteration over a `Source<Downstream>`, a per-connection `@rocicorp/lock`, OTEL spans around four message types, and a handful of `ws`-library details (perMessageDeflate config JSON, `maxPayload`, `createWebSocketStream`).

The structural blocker is **not in the files themselves** but in the runtime shape they assume: one process per "syncer" with TCP sockets handed over via Node IPC. The guide's preferred Rust shape is single-process tokio, which collapses `Syncer`, `Mutator`, `WorkerDispatcher` and the handoff glue into one binary — tracked in `new-patterns/workers-child-process-socket-handoff.md`. Row 18 in `ts-vs-rs-comparison.md` already marks `connection.ts`, `syncer-ws-message-handler.ts`, `connect-params.ts` as **Port partial** in `crates/server/src/{connection.rs, ws_handler.rs, syncer.rs}`; row 19 covers `syncer.ts` at the same partial state; `worker-dispatcher.ts` does not have a Rust counterpart because routing is currently a single-tokio-process no-op. This means "port the remainder" = (a) complete the message-switch coverage inside `ws_handler.rs` for `changeDesiredQueries`, `updateAuth`, `deleteClients`, `initConnection`, `inspect`, `ackMutationResponses`, `push`; (b) add CRUD-vs-Custom dispatch (mutagen path vs pusher path); (c) port the `DrainCoordinator` + `ServiceRunner` keepalive/ref-count semantics; and (d) formalise routing — single-process means no dispatcher, but if we keep the multi-process topology we need the socket-handoff pattern.

Blockers before the translator starts on these workers:

1. Decide: single-process tokio (drop `WorkerDispatcher`, drop `Mutator` worker, fold `Syncer` behaviour into the axum server) **or** multi-process with `sendfd` (preserve the TS topology 1:1). The two options produce very different `crates/server/src/*.rs` trees. The architecture in `ts-vs-rs-comparison.md` currently implies option A.
2. Add `xxhash-rust` to `Cargo.toml` (and Part 4 of the guide) — needed for `h32` parity if **any** cross-language routing bridge ever runs (blue/green rollout).
3. Add the `types/websocket-handoff.ts` decision + `node:fs` persistence policy to the guide so the translator has one shape to pick.
4. Port the `sec-websocket-protocol` decode (`decodeSecProtocols` — base64url → UTF-8 → JSON) to the Rust side; `ts-vs-rs-comparison.md` row 18 says this is "both handled" already, so spot-check parity.

Given 1–4, porting is straightforward and testable against the existing `ws_connect` integration test + Playwright flows mentioned in the comparison doc.
