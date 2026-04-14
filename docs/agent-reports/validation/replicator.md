# Validation report: replicator

## Scope
- `packages/zero-cache/src/services/replicator/replicator.ts` — 110
- `packages/zero-cache/src/services/replicator/incremental-sync.ts` — 208
- `packages/zero-cache/src/services/replicator/change-processor.ts` — 932
- `packages/zero-cache/src/services/replicator/notifier.ts` — 64
- `packages/zero-cache/src/services/replicator/registry.ts` — 18
- `packages/zero-cache/src/services/replicator/replication-status.ts` — 202
- `packages/zero-cache/src/services/replicator/write-worker-client.ts` — 164
- `packages/zero-cache/src/services/replicator/write-worker.ts` — 99
- `packages/zero-cache/src/services/replicator/test-utils.ts` — 233
- `packages/zero-cache/src/services/replicator/schema/change-log.ts` — 242
- `packages/zero-cache/src/services/replicator/schema/column-metadata.ts` — 362
- `packages/zero-cache/src/services/replicator/schema/constants.ts` — 7
- `packages/zero-cache/src/services/replicator/schema/replication-state.ts` — 205
- `packages/zero-cache/src/services/replicator/schema/table-metadata.ts` — 111
- Total LOC: 2957

Note: there is no `initial-sync.ts` under `services/replicator/`. Initial sync lives in `services/change-source/pg/initial-sync.ts` (row 7 in `ts-vs-rs-comparison.md`) and is out of scope here. The `ChangeProcessor` in this module is nonetheless used by initial-sync through the `'initial-sync'` mode (change-processor.ts:67, 361-364).

## Summary
| metric | count |
|---|---|
| TS constructs used | 24 |
| Constructs covered by guide | 23 |
| Constructs NOT covered | 1 |
| Libraries imported | 7 |
| Libraries covered by guide | 7 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 3 |

## Constructs used
| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class` with private `#fields` | replicator.ts:55-87, incremental-sync.ts:26-61, change-processor.ts:87-113, notifier.ts:31-42, change-log.ts:122-164, column-metadata.ts:68-138 | A1, A23 | covered |
| `interface` → trait | replicator.ts:19-42 (`ReplicaStateNotifier`, `Replicator`), write-worker-client.ts:22-28 (`WriteWorkerClient`), registry.ts:3-18 (`ReplicatorRegistry`) | A5 | covered |
| Abstract service pattern (`Service` interface with `run/stop`) | replicator.ts:55 (`implements Service`) | A3 | covered |
| Tagged union + `switch` on `message[0]` / `msg.tag` | incremental-sync.ts:107-160, change-processor.ts:238-286 | A6 | covered |
| `unreachable(msg)` exhaustiveness check | change-processor.ts:285, 366 | A6 | covered |
| Generics with constraints | write-worker-client.ts:41, 51, 123; test-utils.ts:67 (`ReplicationMessages<TablesAndKeys extends Record<string, string \| string[]>>`) | A8 | covered |
| Optional chaining `?.` | replicator.ts:73 (`lc.info?.`), change-processor.ts:117, 140, 577, 596, 620 | A11 | covered |
| Nullish coalescing `??` | change-processor.ts:211 (`msg.json ?? JSON_PARSED`), 394, 498 (`oldKey ?? newKey`), 820 | A12 | covered |
| Object spread `{...a, x: 1}` | change-processor.ts:130-140 (`...backfillStatus, table: …`), 391 (`…spec, primaryKey: [...] `), 437 (`...newRow.row, [ZERO_VERSION_COLUMN_NAME]`), 482, replication-status.ts:55-58 | A13, A20 | covered |
| Throw + try/catch | change-processor.ts:151-154, 183-195, 205, 218, 415, 878 | A15 | covered |
| `async function` / `await` | replicator.ts:102-108, incremental-sync.ts:63-172, write-worker-client.ts:154-159, replication-status.ts:71-79 | A17 | covered |
| `for await (const message of downstream)` (async iteration over `Source`) | incremental-sync.ts:105 | A16, A22 | covered |
| Closure capturing outer var via `move` | incremental-sync.ts:64 (`err => this.#state.stop(lc, err)`), notifier.ts:36-39 | A18 | covered |
| `Object.keys(row).map(…)`, `Object.values(row)`, `Object.entries(…)` | change-processor.ts:456, 499-508, 557, 854 | A19 | covered |
| Newtype-style watermark / LexiVersion string | change-processor.ts:318, change-log.ts:177-197 | A24 | covered |
| EventEmitter fan-out → watch/broadcast | notifier.ts:1, 32, 59 (`new EventEmitter() … .on('version', notify) … .listeners('version')`) | B1, C2 | covered |
| `node:worker_threads` (`Worker`, `parentPort`, `postMessage`, `terminate`, `on('message'\|'error'\|'exit')`) | write-worker-client.ts:2, 76-113, 127, 150-157; write-worker.ts:2, 22-26, 84-99 | B4 | covered |
| Cancellation via `Source.cancel()` / `RunningState.cancelOnStop` | incremental-sync.ts:96, 161-168 | B8, C9 | covered |
| `setInterval` / `clearInterval` | replication-status.ts:26, 63-67, 83 | B7 | covered |
| Backpressure / coalescing subscription (latest-only) | notifier.ts:35-42 (`Subscription.create({coalesce: curr => curr})`) | B1 (`watch`) / C1 | covered |
| Pub-sub broadcast with per-subscriber latest state | notifier.ts:31-63 | B1, C2 | covered |
| Service lifecycle `start → stop` | replicator.ts:93-109, incremental-sync.ts:63-173, 205-207 | C3 | covered |
| Exponential backoff with jitter via `RunningState` | incremental-sync.ts:36, 95, 170 | C6 | covered |
| Custom error hierarchy + `AbortError` | change-processor.ts:3, 121-126, 130 | A15, F1, F2 | covered |
| Singleton per-DB registry via `WeakMap<Database, …>` | column-metadata.ts:69, 144-162 | D2 | covered |
| Lazy prepared-statement caching | table-metadata.ts:55-112 (`this.#setUpstreamMetadata ??= db.prepare(…)`) | A12 | covered |
| JSON serialisation with BigInt support (`stringify`, `parse` from `bigint-json`) | change-log.ts:2-5, 200-229; replication-state.ts:11-13 | E3 | covered (risk-flagged) |
| Valita schema parsing (`v.object`, `v.array`, `v.parse`, `.map`) | replication-state.ts:10, 74-111; change-log.ts:92-120 | — | **NEW — see `new-patterns/replicator-valita-schema-parsing.md`** |
| `WeakMap` | column-metadata.ts:69 | D2 | covered |
| `Map<K,V>` + `Map.entries/keys/values` | change-processor.ts:97, 320, 378-400; table-metadata.ts:87-97 | D1 (no-TTL variant → `HashMap`) | covered |
| Structured-clone IPC across worker boundary | write-worker-client.ts:30-55, 127, 150; write-worker.ts:84-99 | B4 | covered |
| `as const` object literal | test-utils.ts:79-86 (relation literal) | — (TS-only) | covered (no runtime effect) |

## Libraries used
| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `@rocicorp/logger` (`LogContext`, `.withContext`, `.info?.`, `.warn?.`, `.debug?.`, `.error?.`) | `tracing` + `tracing-subscriber` | covered |
| `@rocicorp/zero-sqlite3` (`SqliteError` + `SQLITE_BUSY` check; `Database`, `Statement`, `prepare`, `run`, `exec`, `pragma`, `close`) via local `zqlite` wrapper | `rusqlite` (bundled) + manual SQLITE_BUSY retry; `StatementRunner` already exists in the Rust tree | covered |
| `@rocicorp/resolver` (`resolver()`, `Resolver<T,E>`) | `tokio::sync::oneshot` (or `futures::channel::oneshot`) | covered |
| `eventemitter3` (`EventEmitter`, `on/off/listeners`) | `tokio::sync::watch` (this specific latest-only coalescing usage); fallback `broadcast` | covered |
| `node:worker_threads` (`Worker`, `parentPort`, `MessageChannel`-style `postMessage` protocol) | `std::thread::spawn` + dedicated tokio runtime + `mpsc`/`oneshot` channels (pattern B4) | covered |
| Internal `../change-streamer/change-streamer.ts` (`ChangeStreamer`, `Downstream`, `PROTOCOL_VERSION`) | hand-rolled Rust `ChangeStreamer` interface (see `crates/streamer/src/streamer.rs`, marked "Port partial" in row 10 of `ts-vs-rs-comparison.md`) | covered — depends on **risk-flagged** pgoutput (row 8) upstream of it |
| Internal `../../../shared/src/valita.ts` (valita validator) | no direct crate equivalent; guide does not name valita. Idiomatic Rust equivalent is `serde` + `#[derive(Deserialize)]` with custom `serde_json` round-tripping for the `.map(...)` transforms (e.g. publications JSON-string → `Vec<String>`). | **NEW — see `new-patterns/replicator-valita-schema-parsing.md`** |
| Internal `../../../shared/src/bigint-json.ts` (`stringify`, `parse`, `jsonObjectSchema`) | `serde_json` with `arbitrary_precision` feature, or BigInt-as-string (guide E3, Part 3 item 3) | covered (risk-flagged) |
| Internal `../../../zero-events/src/status.ts` (types only) | type-only — Rust equivalent types in `crates/types` | covered |
| Internal `../../types/subscription.ts` (`Subscription.create({coalesce, cleanup})`, `Source`/`Sink`) | `tokio::sync::watch` (exact fit for "latest value, coalesced") or custom `Stream` wrapper | covered under B1 / C2 |
| `../../observability/metrics.ts` (`getOrCreateCounter`) | `opentelemetry` + `opentelemetry_sdk` metrics API | covered |

## Risk-flagged items depended on
- **E4 / Part 3 item 1 — pgoutput parser & logical replication loop.** `incremental-sync.ts:86-94` calls `this.#changeStreamer.subscribe({ protocolVersion, taskID, id, mode, watermark, replicaVersion, initial })` and consumes a `Source<Downstream>` where each `Downstream` is the parsed pgoutput change. The replicator itself does not parse pgoutput, but it is the **sole consumer** of the parser's output and cannot operate without it. Per `ts-vs-rs-comparison.md` row 8 this is currently **Missing** in Rust (no pgoutput parser exists), which means the Rust replicator has nothing to read from on the Rust-only path.
- **E3 / Part 3 item 3 — `serde_json` `arbitrary_precision` vs `flatten`/`tag`.** `change-log.ts:200, 229` and `replication-state.ts:11, 131` call `stringify(…)` / `parse(…)` from `shared/src/bigint-json.ts` to preserve BigInt precision when writing rowKeys and `initialSyncContext` into SQLite TEXT columns. Any Rust port must round-trip BigInts through strings or use `arbitrary_precision` without `#[serde(flatten)]`/`tag` on those types (which this module does not use, so the constraint is easy to honour).
- **Part 2 note on `@rocicorp/zero-sqlite3` — BEGIN CONCURRENT is not in upstream SQLite.** `change-processor.ts:352` explicitly calls `db.beginConcurrent()` in `serving` mode and comments that this is required so view syncers can simulate IVM on historic snapshots. The guide and `ts-vs-rs-comparison.md` row 6 flag this as the second big risk. The Rust port must either custom-build libsqlite3 with the `begin-concurrent` branch (via `libsqlite3-sys` + `buildtime_bindgen`) or drop to standard WAL + deferred transactions (and accept the IVM-snapshot behaviour change). The `backup` mode uses `beginImmediate()` which is a direct `BEGIN IMMEDIATE` on rusqlite, and is safe to port as-is.

## Ready-to-port assessment

This module is **architecturally ready to translate** once the upstream pgoutput change-source is in place. Every concurrency pattern (worker-thread write delegation, latest-value notifier, lifecycle with exponential backoff, tagged-union message dispatch) has a direct counterpart in the Rust translation guide, and the per-file LOC is small (≈3K). The existing partial Rust port (`crates/replicator/src/{replicator.rs,notifier.rs,change_log_consumer.rs}` — rows 11 and 12 of `ts-vs-rs-comparison.md`) already implements the easy half (notifier via `tokio::sync::watch`; replicator service + subscription). What is **not** ready:

1. **pgoutput upstream dependency (row 8 of the comparison doc).** Until a Rust change-source writes to the `changeLog` / CDC tables, `IncrementalSyncer.run` has nothing to consume, so a 1:1 port of `incremental-sync.ts` cannot be E2E-verified. The "Port partial" status in row 11 is caused by this missing dependency, not by anything in the replicator module itself.
2. **BEGIN CONCURRENT decision.** Before porting the `serving` branch of `TransactionProcessor` (`change-processor.ts:344-353`), the team must decide: custom libsqlite3 build vs. fallback WAL+deferred. This is a policy choice, not a pattern gap — but the guide only documents the fallback, not the custom-build procedure.
3. **Valita → serde.** The valita schemas at `replication-state.ts:74-111` and `change-log.ts:92-120` use `.map(fn)` transforms (e.g. parse the `publications` text column into `string[]`, parse the `rowKey` string into a JSON object for `SET`/`DEL` but `null` for `TRUNCATE`/`RESET`). The translation is straightforward (`#[serde(deserialize_with=…)]` or a post-parse step) but valita itself is not in the guide — see `new-patterns/replicator-valita-schema-parsing.md` for the recommended shape and citations.

Besides those three, the port is **mechanical**. Pattern map:

- `Notifier` (notifier.ts) → `tokio::sync::watch::channel(ReplicaState)` (already partly done in Rust); `EventEmitter.listeners` → iterate over the receivers' senders. Because every subscriber wants *latest-only*, `watch` is a cleaner fit than `broadcast`. The "send current state on subscribe" semantic is native to `watch::Receiver` (it returns the current value on the first `.changed()/.borrow()`).
- `ThreadWriteWorkerClient` / `write-worker.ts` → dedicated OS thread hosting a tokio runtime, with a `mpsc<Request>` in and a `mpsc<Response>` out (or `oneshot` per call). Protocol is already typed via `ArgsMap`/`ResultMap` — translate to a Rust enum with `#[serde(tag, content)]`-free encoding (since it's in-process, use a plain Rust enum). The `writeError` fire-and-forget channel maps to a separate `mpsc<anyhow::Error>` consumed by an error handler.
- `ChangeProcessor` + `TransactionProcessor` → struct + inner struct; the big switch on `msg.tag` (change-processor.ts:238-286) maps to a `match` on a `Change` enum (E4 parser must have produced that enum already). The `SQLITE_BUSY` retry loop (change-processor.ts:171-196) translates verbatim with `rusqlite::Error::SqliteFailure(ffi::Error { code: ErrorCode::DatabaseBusy, .. }, _)`.
- All three `schema/*.ts` files are raw prepared-statement CRUD against SQLite TEXT columns — direct translation to `rusqlite::Statement` + `Connection::prepare_cached`. `WeakMap<Database, ColumnMetadataStore>` becomes a per-`Arc<Connection>` OnceCell or simply inlining — Rust doesn't need the "prepare statements once" guard because `prepare_cached` already deduplicates.
- `ReplicationStatusPublisher` → straight port using `tokio::time::interval` for the 3-second refresh; be explicit about `MissedTickBehavior::Delay` (B7 caveat).

No new crates required beyond what the guide already lists. No new concurrency primitive is introduced. The only pattern entry not already in the guide is **valita's `.map(…)` transform during parsing**, which is documented in the companion `new-patterns/replicator-valita-schema-parsing.md`. Once that is added to the guide and the pgoutput parser + BEGIN CONCURRENT policy are decided, this whole directory can be translated in a single focused pass.
