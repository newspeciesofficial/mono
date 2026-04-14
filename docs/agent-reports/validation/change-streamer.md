# Validation report: change-streamer

## Scope

Non-test .ts files under `packages/zero-cache/src/services/change-streamer/`
(including `schema/`). Test files (`*.test.ts`, `*.pg.test.ts`) were skipped
per scope instructions; the `.d.ts` ambient type module was read but is a
third-party type stub.

- `change-streamer.ts` — 189
- `change-streamer-service.ts` — 602
- `change-streamer-http.ts` — 311
- `storer.ts` — 815
- `forwarder.ts` — 143
- `broadcast.ts` — 216
- `subscriber.ts` — 200
- `backup-monitor.ts` — 257
- `replica-monitor.ts` — 64
- `snapshot.ts` — 48
- `error-type-enum.ts` — 7
- `test-utils.ts` — 22
- `parse-prometheus-text-format.d.ts` — 54 (ambient type stub)
- `schema/init.ts` — 135
- `schema/tables.ts` — 364
- **Total (non-test LOC):** ~3,427

## Summary

| metric | count |
|---|---|
| TS constructs used | 31 |
| Constructs covered by guide | 29 |
| Constructs NOT covered | 2 |
| Libraries imported | 11 |
| Libraries covered by guide | 11 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 2 |

## Constructs used

| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class Foo { #x: T; bar() {} }` with private `#fields` | `change-streamer-service.ts:249-321`, `storer.ts:105-155`, `subscriber.ts:20-40`, `broadcast.ts:17-50`, `forwarder.ts:12-28`, `backup-monitor.ts:49-83` | A1 / A23 | covered |
| `abstract`/`interface` style (interface `ChangeStreamer`, `ChangeStreamerService extends ChangeStreamer, Service`) | `change-streamer.ts:42-49`, `change-streamer.ts:177-189` | A3 / A5 | covered |
| Tagged union + switch on `type`/`tag` (`case 'begin' \| 'commit' \| ...`) | `change-streamer-service.ts:358-408`, `storer.ts:395-523`, `storer.ts:650-751`, `forwarder.ts:109-133` | A6 | covered |
| `enum`-like namespaced const ints (`ErrorType.Unknown`, `WrongReplicaVersion`, `WatermarkTooOld`) | `error-type-enum.ts:1-8`, used in `storer.ts:612-615`, `subscriber.ts:188` | A7 | covered |
| Generic function with constraint via `min(...(initial as AtLeastOne<LexiVersion>))` | `change-streamer-service.ts:554-555` | A8 | covered |
| `a ?? b` (nullish coalescing) | `change-streamer-service.ts:531`, `change-streamer-http.ts:132-133` | A12 | covered |
| Struct/object spread (`{...stringParams}`) | `change-streamer-http.ts:301-311` | A13 / A20 | covered |
| `try { throw new CustomError } catch` + custom error subclasses (`AutoResetSignal extends AbortError`, `UnrecoverableError`) | `schema/tables.ts:296-298`, `change-streamer-service.ts:374-386`, `storer.ts:240-242,485-489` | A15 / F1 / F2 | covered |
| `async function*` + `for await (x of gen())` | `backup-monitor.ts:159-203` (`async *#fetchWatermarks`), `change-streamer-service.ts:358` (`for await (const change of stream.changes)`), `storer.ts:576-580` (`for await (const entries of tx<...>.cursor(2000))`) | A16 | covered |
| `Promise.all([…])` | `change-streamer-service.ts:434-443`, `storer.ts:556-559`, `schema/tables.ts:264-269` | A17 | covered |
| `Symbol.iterator` / async-iterator consumption via `.cursor(2000)` (postgres cursor) | `storer.ts:576-580` | A22 | covered |
| Newtype-ish branded types (`LexiVersion`, `AtLeastOne<LexiVersion>`) | `change-streamer-service.ts:9-12`, `change-streamer-service.ts:554` | A24 | covered |
| `EventEmitter`-equivalent pub/sub via `Subscription` (broadcast to N subscribers with per-subscriber ACK) | `subscriber.ts:20-122`, `forwarder.ts:12-143`, `broadcast.ts:17-207` | B1 / C2 | covered |
| Streams/async iterable transports (`Source<Downstream>`, `streamIn`/`streamOut` over WebSocket) | `change-streamer-http.ts:12,142,161,248,257`, `change-streamer.ts:48,175` | B2 | covered |
| `Buffer`/`Uint8Array`-style byte accounting (tracking `json.length` serialized payload sizes) | `storer.ts:251-271,313-325,427` | B3 | covered |
| `process.on('SIGTERM')`-style lifecycle via `RunningState.stop(...)` (not directly signal handling, but RunningState is used here) | `change-streamer-service.ts:320,436-448,580-585`, `backup-monitor.ts:55,245-256`, `replica-monitor.ts:22-61` | B5 / C3 | covered |
| `getHeapStatistics()` from `node:v8` for heap-proportioned back-pressure thresholds | `storer.ts:3,144-154` | B6 (runtime primitive) | covered |
| `setTimeout`/`setInterval`/`clearInterval`/`clearTimeout` | `backup-monitor.ts:90-93,246`, `forwarder.ts:30-54`, `schema/tables.ts:257-267`, `change-streamer-service.ts:68,295,320,515,575` | B7 | covered |
| `AbortSignal`/`AbortError` (fetch cancellation via `RunningState.signal`) | `backup-monitor.ts:165-174`, `storer.ts:4,240-242,370` | B8 | covered |
| `URL`/`URLSearchParams` | `change-streamer-http.ts:131-133,245,253-255,264,299-311`, `schema/init.ts` (none) | B9 | covered |
| Global `fetch` | `backup-monitor.ts:168` | B10 | covered |
| Bounded-queue / explicit "consumed" ACK back-pressure | `storer.ts:116-325` (`#queue`, `#approximateQueuedBytes`, `readyForMore`, `#maybeReleaseBackPressure`), `change-streamer-service.ts:410-414` | C1 | covered |
| Pub/sub broadcast with per-subscriber ACK (`Broadcast`, `checkProgress`, consensus-based flow control) | `broadcast.ts:17-207`, `forwarder.ts:30-107`, `subscriber.ts:113-122` | C2 | covered |
| Service lifecycle `start → drain → stop` (Service interface, `RunningState`) | `change-streamer-service.ts:320,336-447,580-585`, `storer.ts:333-361,794-801`, `backup-monitor.ts:245-256`, `replica-monitor.ts:36-63` | C3 | covered |
| Connection pool with fair queueing (`TransactionPool` wrapping postgres) | `storer.ts:13,432-506,532-559` | C4 | covered |
| Exponential backoff with jitter (`RunningState.backoff`) | `change-streamer-service.ts:436-448` | C6 | covered |
| Cancellation propagation (`stream.changes.cancel()` on error, `subscription.cancel()`) | `change-streamer-service.ts:347,419,582`, `backup-monitor.ts:100,133,251`, `subscriber.ts:194-199` | C9 | covered |
| `Map<K, V>` / `Set<K>` collections (with string-ordered scans for watermarks) | `change-streamer-service.ts:262,511-517,541-577`, `backup-monitor.ts:57-58,206-241`, `forwarder.ts:15-16,60-132` | D1 (simple map variant) | covered |
| Atomic counter / stats (`getOrCreateCounter('replication','transactions')`, `numPending/numProcessed` with periodic sampling) | `change-streamer-service.ts:274-278,378`, `subscriber.ts:132-176` | D6 | covered |
| LSN-derived lexi watermark comparisons / lexicographic `<`/`>` string compare | `change-streamer-service.ts:554-569`, `storer.ts:218-223,576-580`, `subscriber.ts:50-58,96-110`, `backup-monitor.ts:207,229-242` | E5 | covered |
| JSON-preserving BigInt precision via `BigIntJSON.stringify` | `storer.ts:7,260` | E3 | covered (flagged Risky by guide) |
| Binary pgoutput parser — **not in this module**, but `change-streamer-service.ts` consumes the decoded `ChangeStreamData` stream it produces | `change-streamer-service.ts:358-415` (downstream of the risk-flagged parser) | E4 | covered (dependency only) |
| `Subscription`/resolver-based manual promise plumbing (`@rocicorp/resolver`) for per-change pending/done | `broadcast.ts:33,52-77`, `storer.ts:431-451,285-311,336-358`, `change-streamer-service.ts:272` | Library → `@rocicorp/resolver` (oneshot) | covered |
| `node:stream` `getDefaultHighWaterMark` used as I/O flush threshold | `change-streamer-service.ts:3,334` | — | **NEW — see `new-patterns/change-streamer-node-stream-high-water-mark.md`** |
| `node:v8` `getHeapStatistics().heap_size_limit / used_heap_size` to size a back-pressure budget | `storer.ts:3,144-154` | — | **NEW — see `new-patterns/change-streamer-node-v8-heap-statistics.md`** |

## Libraries used

| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `postgres` (porsager) — `PendingQuery`, `Row`, `TransactionSql`, `.cursor(2000)`, template-tagged SQL, `db.unsafe(...)` | `tokio-postgres` + `deadpool-postgres` + `postgres-types` | covered |
| `@rocicorp/logger` (`LogContext`, `.withContext(...)`, `info?./warn?./debug?./error?.`) | `tracing` + `tracing-subscriber` | covered |
| `@rocicorp/resolver` (`resolver<T>()`, `Resolver<T>`) | `tokio::sync::oneshot` | covered |
| `@fastify/websocket`, `fastify` (HTTP + WS upgrade; `HttpService`) | `axum` + `tower-http` | covered |
| `ws` (`WebSocket` client + server) | `tokio-tungstenite` + `axum::extract::ws` | covered |
| `parse-prometheus-text-format` | `prometheus-parse` / `prom-parse` | covered |
| `pg-format` (`ident(...)`) | `postgres-protocol::escape` / `pg_escape` | covered |
| `shared/src/valita` (schema/runtime validation) | `serde` + `#[serde(tag=..., content=...)]`-style deserialization; bespoke valita doesn't map 1:1 but the catalogue covers it via serde | covered (Idiom-swap under A6 / E1) |
| `shared/src/bigint-json` (`BigIntJSON.stringify`) | `serde_json` with `arbitrary_precision` + custom BigInt-as-string (de)serializer | covered (Risky per E3) |
| `shared/src/queue.ts` (`Queue<QueueEntry>`) | `tokio::sync::mpsc::channel(cap)` (bounded) or `crossbeam::queue` | covered (C1 — explicit "consumed"-ack bounded queue) |
| `shared/src/abort-error.ts` (`AbortError`), `shared/src/asserts.ts`, `shared/src/iterables.ts`, `shared/src/must.ts`, `shared/src/set-utils.ts`, `shared/src/resolved-promises.ts`, `shared/src/enum.ts` | `thiserror` / `anyhow`; stdlib iterator adapters; `itertools` | covered |

No third-party package imported here falls outside the guide's Part 2
crate table.

## Risk-flagged items depended on

- **E4 — pgoutput parser / logical replication loop (Risky, guide Part 3 item 1).**
  `change-streamer-service.ts:343-346` starts the upstream stream via
  `this.#source.startStream(lastWatermark, backfillRequests)` and then iterates
  its `stream.changes` async iterable (`change-streamer-service.ts:358`). The
  change-streamer itself does not decode pgoutput, but its "forward-store-ack"
  pipeline is driven entirely by the output of that risk item. Any regression
  or missing message type in the parser (e.g. `update-table-metadata`,
  `backfill` — see `subscriber.ts:178-185` for the v5 / v6 protocol gates) will
  surface here.

- **E3 — `serde_json` `arbitrary_precision` + `#[serde(flatten)]`/`tag`
  incompatibility (Risky, guide Part 3 item 3).** `storer.ts:260` eagerly
  `BigIntJSON.stringify(change)`s every change into a JSON string column
  (`changeLog.change JSON NOT NULL`, `schema/tables.ts:37-50`). The chosen Rust
  shape for the `Change` enum must avoid serde `tag`/`flatten` attributes on
  any BigInt-bearing fields, otherwise the Rust write path and TS read path
  disagree on JSON representation.

- **C2 — Pub/sub broadcast with per-subscriber ACK (Redesign, guide Part 1
  §C).** The whole `Forwarder`/`Broadcast`/`Subscriber` trio (`forwarder.ts`,
  `broadcast.ts`, `subscriber.ts`) implements "dispatcher task feeding N mpsc
  channels" — i.e. the exact Redesign case. It is NOT a simple `broadcast::channel`
  fan-out because slow subscribers must not lose messages, catchup must
  interleave with live stream, and the consensus-based flow-control timeout
  (`broadcast.ts:95-189`) is custom. Not a gap in the guide, but the largest
  Redesign surface in scope.

- **Not-in-guide dependency: `node:stream` `getDefaultHighWaterMark`.**
  Used as the byte threshold between fire-and-forget forwarding and
  ACK-tracked `forwardWithFlowControl` (`change-streamer-service.ts:333-404`).
  See `new-patterns/change-streamer-node-stream-high-water-mark.md`. Not
  blocking, but must be pinned to a constant in the Rust port.

- **Not-in-guide dependency: `node:v8` `getHeapStatistics()`.**
  Used to size the storer's byte-queue back-pressure budget as a proportion of
  the process heap (`storer.ts:144-154`). See
  `new-patterns/change-streamer-node-v8-heap-statistics.md`. This is Node/V8
  specific — the Rust port cannot replicate it; a bounded channel or a fixed
  MB budget is the idiomatic replacement.

## Ready-to-port assessment

This module can be translated **mechanically today with two caveats**.

The pattern inventory is dominated by constructs already in the guide:
tagged-union `switch` on `ChangeStreamMessage` tags (A6), async-iterable
streams (A16), resolver/oneshot-style promise plumbing, bounded queues with
explicit ACK (C1), and a custom dispatcher with per-subscriber ACK + catchup
interleaving (C2). All third-party imports — `postgres`, `@fastify/websocket`,
`ws`, `parse-prometheus-text-format`, `pg-format`, `@rocicorp/logger`,
`@rocicorp/resolver` — have entries in Part 2.

The two items missing from the guide are both Node-runtime introspection
primitives:

1. `node:stream` `getDefaultHighWaterMark(false)` at
   `change-streamer-service.ts:3,334` — used to decide when to switch from
   fire-and-forget to flow-controlled forwarding. This is a constant (16 KiB
   on modern Node). Port should pick a constant and stop using the Node
   primitive.
2. `node:v8` `getHeapStatistics()` at `storer.ts:3,144-154` — used as a
   dynamic byte budget proxied off `heap_size_limit - used_heap_size` times
   `--back-pressure-limit-heap-proportion`. Rust has no direct analogue (heap
   is not bounded the same way). Port must either (a) take an absolute MB
   budget from config, or (b) read a cgroup / system memory limit; the
   semantics necessarily drift.

Neither is blocking. Both deserve their own `new-patterns/*.md` entries so
the orchestrator can pick the replacement policy before translation begins.

The module **does** inherit two pre-existing guide-flagged risks (E4
pgoutput parser and E3 BigInt JSON) and one large Redesign surface (C2
per-subscriber ACK). None of these are local to the change-streamer
implementation — they are already the biggest flagged items in the guide
and already called out in `ts-vs-rs-comparison.md` row 10 (current Rust state:
"Port partial — interfaces present, not exercised end-to-end"). Porting
should not begin until:

- pgoutput parser design is picked (E4 / Part 3 item 1).
- BigInt JSON representation is nailed down (E3 / Part 3 item 3) — especially
  because the storer writes `change` as a JSON column (`schema/tables.ts:45`).
- The two new-pattern entries below are merged into the guide.
