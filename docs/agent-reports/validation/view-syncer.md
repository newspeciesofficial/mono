# Validation report: view-syncer

## Scope
- `packages/zero-cache/src/services/view-syncer/view-syncer.ts` — 2476 LOC
- `packages/zero-cache/src/services/view-syncer/client-handler.ts` — 459 LOC
- `packages/zero-cache/src/services/view-syncer/error.ts` — does not exist (skipped)
- Total LOC: 2935

Notes on scope: per the worker prompt, I did not re-read `cvr-store.ts`, `cvr.ts`,
or `schema/types.ts`. I noted only the symbols imported from those modules and
trusted their translation status from `ts-vs-rs-comparison.md` rows 15–17.

## Summary
| metric | count |
|---|---|
| TS constructs used | 27 |
| Constructs covered by guide | 27 |
| Constructs NOT covered | 0 |
| Libraries imported | 6 |
| Libraries covered by guide | 6 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 3 |

## Constructs used

All file:line samples are from `view-syncer.ts` unless prefixed `client-handler.ts:`.

| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class ViewSyncerService implements ViewSyncer, ActivityBasedService` with many `readonly` / `#private` fields | view-syncer.ts:181, 230, 235 | A1, A23 | covered |
| `interface ViewSyncer { … }` | view-syncer.ts:129 | A5 | covered |
| `class ClientHandler` with `#private` fields (`#baseVersion`, `#downstream`, `#lc`, metrics) | client-handler.ts:117, 125 | A1, A23 | covered |
| `interface PokeHandler { addPatch/cancel/end }` + `const NOOP: PokeHandler` | client-handler.ts:72, 78 | A5 | covered |
| Tagged union `RowChange` / `Patch = ConfigPatch \| RowPatch` + `switch(type)` + `unreachable(patch)` | client-handler.ts:63, 232–291, view-syncer.ts:2090–2104 | A6 | covered |
| String-literal union `PokeType = 'hydration' \| 'advance' \| 'catchup' \| 'config'` | client-handler.ts:84 | A6 (string-enum form) | covered |
| `type CustomQueryTransformMode = 'all' \| 'missing'` + `satisfies` narrowing | view-syncer.ts:179, 1571 | A6 | covered |
| Generics with constraints `fn<B, R, M extends [cmd:string,B]>` | view-syncer.ts:996 | A8 | covered |
| `async function` + `await` + `Promise` usage throughout | view-syncer.ts:460, 514, 1022 | A17 | covered |
| `async function*` / `yield*` / `for await … of` (generateRowChanges, processChanges, catchupRowPatches) | view-syncer.ts:1809, 1816, 2067, 1969 | A16, A22 | covered |
| `Promise.allSettled` for broadcast to N clients | client-handler.ts:99, 102, 105 | A17 | covered |
| `Promise.race` (readyState) | view-syncer.ts:454 | A17 | covered |
| Optional chaining `a?.b?.c` and `?.method()` | view-syncer.ts:747, 1076, 1456 | A11, A21 | covered |
| Nullish coalescing `a ?? b` and assignment `??=` | view-syncer.ts:1079, 215, client-handler.ts:215, 237 | A12 | covered |
| Spread to build partial CVR snapshot `{...this.#cvr, ttlClock}` | view-syncer.ts:437 | A13, A20 | covered |
| `Map<string, ClientHandler>` / `Map<string, CustomQueryRecord>` / `CustomKeyMap<RowID, RowUpdate>` | view-syncer.ts:230, 1277, 2038 | D1/A19 (Record<K,V> form) | covered |
| `Set<string>` (clientIDsToDelete, removeQueriesQueryIds) | view-syncer.ts:1143, 1668 | A10 | covered |
| Exception hierarchy: `throw new ProtocolErrorWithLevel({…}, 'warn')`, `ClientNotFoundError`, `ResetPipelinesSignal`, `ProtocolError`; `try/catch/instanceof` | view-syncer.ts:412, 496, 2175, 2358 | A15, F1, F2, F3 | covered |
| `assert(cond, msg)` / `unreachable(x)` / `must(x, …)` | view-syncer.ts:475, 2103, 478, client-handler.ts:290 | A6 | covered |
| `setTimeout` / `clearTimeout` + injectable `SetTimeout` + `setImmediate` (via `yieldProcess`) | view-syncer.ts:159, 667, 2331 | B7 | covered |
| `Lock` (@rocicorp/lock) wrapping async critical sections | view-syncer.ts:235, 401, 2295, 2328 | crate `@rocicorp/lock` → `tokio::sync::Mutex` | covered |
| `resolver<T>()` (@rocicorp/resolver) for manually-resolved promises (`#stopped`, `#initialized`) | view-syncer.ts:237, 238 | crate `@rocicorp/resolver` → `tokio::sync::oneshot` | covered |
| `Subscription<Downstream>` / `Subscription.create({cleanup})` + `.push` / `.fail` / `.cancel` | view-syncer.ts:61, 714, client-handler.ts:170 | C1 (bounded mpsc + push/ACK) | covered |
| OpenTelemetry spans: `startSpan`, `startAsyncSpan`, `manualSpan`, `span.setAttribute` | view-syncer.ts:676, 1015, 1838 | `@opentelemetry/*` → `opentelemetry` + `tracing-opentelemetry` | covered |
| OpenTelemetry metrics: `getOrCreateCounter` / `getOrCreateHistogram` / `getOrCreateUpDownCounter` | view-syncer.ts:258, 268, client-handler.ts:127 | `@opentelemetry/*` metrics | covered |
| Environment access `process.env.ZERO_CURSOR_PAGE_SIZE` + `parseInt` | view-syncer.ts:2310 | B6 | covered |
| `performance.now()` timings throughout | view-syncer.ts:399, 1761, client-handler.ts:203 | B7 (std::time::Instant) | covered |
| `valita` runtime schema parsing (`v.parse`, `v.object`, `mutationRowSchema`, `lmidRowSchema`, `rowSchema`) | client-handler.ts:251, 374, 395, 419 | E1/E2 (serde) | covered |
| BigInt → safe Number coercion in `ensureSafeJSON` | client-handler.ts:441–459 | D3 | covered |
| `LogContext.withContext(k,v)` | view-syncer.ts:404, 1023 | `@rocicorp/logger` → `tracing` + span fields | covered |

## Libraries used

| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `@rocicorp/lock` | `tokio::sync::Mutex` | covered |
| `@rocicorp/logger` | `tracing` + `tracing-subscriber` | covered |
| `@rocicorp/resolver` | `tokio::sync::oneshot` | covered |
| `postgres` (only `Row` type imported from it) | `tokio-postgres` (Row) | covered |
| `../../shared/src/valita.ts` (Rocicorp-internal) | serde / custom Deserialize (E1/E2 pattern) | covered (internal, not a 3rd-party crate; schemas translate to `#[derive(Deserialize)]`) |
| OpenTelemetry (via internal `observability/metrics.ts` + `otel/src/span.ts`) | `opentelemetry` + `opentelemetry_sdk` + `tracing-opentelemetry` | covered |

No third-party package imported in these two files is outside Part 2 of the
guide. All remaining imports (`shared/*`, `zero-protocol/*`, `zero-cache` siblings)
are internal modules covered by other rows of `ts-vs-rs-comparison.md`
(types 1, cvr-store 16, pipeline-driver 14, snapshotter 13, auth/read-authorizer 2,
config 3, remote-pipeline-driver — pool-threads architecture doc).

## Risk-flagged items depended on

1. **ZQL / IVM pipeline driver** (comparison row 14, marked **Stub**). View-syncer
   awaits `pipelines.addQuery`, `pipelines.advance`, `pipelines.advanceWithoutDiff`,
   `pipelines.getRow`, `pipelines.currentVersion()`, `pipelines.currentPermissions()`,
   `pipelines.queries()`, `pipelines.hydrationBudgetBreakdown()`,
   `pipelines.totalHydrationTimeMs()`, `pipelines.reset()` — used at
   view-syncer.ts:484, 520, 1380, 1816, 1981, 2146, 503, 1548, 1573, 1790, 515.
   Without a real operator graph the poke stream produced by `#processChanges`
   is incomplete for joins / correlated subqueries / IN-subqueries.
2. **CVR row-diff path** (comparison row 16, **Port partial**). View-syncer
   relies on `CVRStore.catchupRowPatches` and `.catchupConfigPatches`
   (view-syncer.ts:1945, 1954) and on `CVRQueryDrivenUpdater.received` /
   `.deleteUnreferencedRows` / `.trackQueries` (view-syncer.ts:1776, 1858, 2053).
   Current Rust re-hydrates instead of diffing, so catchup pokes here will not
   match TS semantics once wired.
3. **pgoutput / change-source** (comparison row 8, **Missing**). View-syncer
   subscribes to `ReplicaState` via `Subscription<ReplicaState>` and only drives
   `#advancePipelines` when `state === 'version-ready'` (view-syncer.ts:470, 475).
   Without a working change-source, this loop never ticks in Rust-only mode and
   `mutationsPatch` / `lastMutationIDChanges` extraction cannot be exercised
   E2E.

Per-client-group state machine items that intersect these risks:

- `lastMutationIDChanges` is not derived inside view-syncer; it is assembled
  by `ClientHandler.#updateLMIDs` from row-patches whose `id.table` equals
  `${app}_{shard}.clients` (client-handler.ts:158, 246, 371). The row contents
  are validated against `lmidRowSchema` (`clientGroupID, clientID,
  lastMutationID`) at client-handler.ts:395. Cross-client-group rows are
  logged and dropped. A mismatch between our Rust shard-name formatter
  (`upstreamSchema`, imported from `types/shards.ts`) and the clients-table
  name would silently drop all LMIDs — this is the exact defect recorded in
  comparison row 15.
- `mutationsPatch` is similarly keyed on `${app}_{shard}.mutations` rows and
  validated against `mutationRowSchema` (client-handler.ts:159, 248, 401).
  Deletes carry `{clientID, mutationID}` in the row-key; the put path parses
  `result` via `mutationResultSchema` from `zero-protocol/src/push.ts`.

## Ready-to-port assessment

**Can be translated mechanically today, but blocked end-to-end on prior rows.**
Every language / stdlib / concurrency / error / wire pattern used in
`view-syncer.ts` and `client-handler.ts` is already in the guide: async+await,
async iterators (`for await`, `yield*` of `async function*`), a `Lock` wrapper
over critical sections, `Subscription` as a bounded mpsc with
push-ACK backpressure, `startPoke` building a fan-out `PokeHandler` over
N per-client pokers with `Promise.allSettled`, OTEL spans/metrics, valita
runtime schemas, injected `setTimeout`, `resolver()` for one-shots,
`performance.now` timings, `thiserror`-style exception hierarchy (`ProtocolError`
/ `ProtocolErrorWithLevel` / `ClientNotFoundError` / `ResetPipelinesSignal`),
`setImmediate` yielding via a global `Lock` (C10 cooperative yield). No new
pattern files needed.

The **per-client-group state machine** reads as:

1. `initConnection` (view-syncer.ts:671) creates a `ClientHandler`, inserts
   into `#clients`, schedules `#runInLockForClient` → `#handleConfigUpdate`.
2. `run()` (view-syncer.ts:460) awaits `#stateChanges` Subscription. On every
   `version-ready` it acquires the `#lock`, loads/refreshes the CVR via
   `CVRStore.load` (priority-op queued), and either calls
   `#advancePipelines` (if already synced) or first-time
   `#hydrateUnchangedQueries` + `#syncQueryPipelineSet`.
3. Config updates (`changeDesiredQueries`, `deleteClients`, `updateAuth`) go
   through `#runInLockForClient` → `#handleConfigUpdate` → `#updateCVRConfig`
   → `CVRConfigDrivenUpdater.flush`. Resulting `PatchToVersion[]` are sent
   to every client currently at the prior `cvr.version` via `startPoke(...,
   'config')`.
4. Replica ticks (`state === 'version-ready'`) call `#advancePipelines`,
   which gets a `CVRQueryDrivenUpdater`, creates a pokers set only over
   clients at `cvr.version`, streams `RowChange` events from
   `pipelines.advance(timer)` into `#processChanges`, batches by
   `CURSOR_PAGE_SIZE=10000`, writes each batch through `updater.received`,
   forwards returned `PatchToVersion[]` into `pokers.addPatch`, flushes the
   CVR, then `pokers.end(finalVersion)`.
5. Hydration (`#syncQueryPipelineSet` → `#addAndRemoveQueries`) does the same
   but drives row changes from a concatenated `async function*` over
   `pipelines.addQuery(...)` for each added query, then
   `updater.deleteUnreferencedRows`, then `#catchupClients` against the old
   CVR (excluding just-hydrated query hashes), then `pokers.end`.
6. TTL: `#ttlClock` is advanced by wall-clock delta at each lock entry,
   written to the CVR on every flush, and, if no flush in the last minute,
   written out-of-lock via `#updateTTLClockInCVRWithoutLock` + a
   `TTL_CLOCK_INTERVAL=60000` timer. `#scheduleExpireEviction` (cvr.ts
   `nextEvictionTime`) fires `#removeExpiredQueries` → `#syncQueryPipelineSet`.
7. Shutdown: `#checkForShutdownConditionsInLock` + `#scheduleShutdown` +
   `#keepaliveMs` (5000 default) keep the service alive for at least
   keepalive-ms after the last disconnect, waiting on `cvrStore.flushed`
   before closing.

**Pokes from operator outputs.** The assembly chain is:

- `PipelineDriver` yields `RowChange { type: 'add'|'edit'|'remove',
  queryID, table, rowKey, row }`.
- `#processChanges` buckets them into a `CustomKeyMap<RowID, RowUpdate>`
  keyed by `rowIDString({schema:'', table, rowKey})`. `refCounts[queryID]`
  is incremented/decremented; `row`'s `_0_version` is split out via
  `contentsAndVersion` (view-syncer.ts:2334). Every `CURSOR_PAGE_SIZE` rows
  or at end, the batch is flushed via `CVRQueryDrivenUpdater.received`,
  which returns `PatchToVersion[]` fed to `pokers.addPatch`.
- `ClientHandler.startPoke` builds the wire frames: first `PokeStartBody`
  `{pokeID, baseCookie}`, then repeated `PokePartBody` accumulating
  `desiredQueriesPatches[clientID]`, `gotQueriesPatch`, `rowsPatch`,
  `lastMutationIDChanges`, `mutationsPatch`; flushed every
  `PART_COUNT_FLUSH_THRESHOLD=100` parts; terminated by `PokeEndBody
  {pokeID, cookie[, cancel]}`. If `toVersion <= baseVersion` for a patch,
  the patch is skipped (client-handler.ts:227) — this is the TS analogue of
  CVR-diff suppression; the Rust port will need the same guard.

**Backpressure to the client.** `ClientHandler` writes via
`Subscription<Downstream>.push(msg)` and `await`s the returned `result`
(client-handler.ts:169). Slow clients thus throttle poke assembly, but since
`startPoke` fans out with `Promise.allSettled`, a failed/slow client never
blocks other clients (client-handler.ts:99). The group rate is limited by
the slowest connection, which matches C1's "bounded `mpsc` for implicit
backpressure" rule.

**Blockers (none new in this module):**

- Row 14 — pipeline-driver / IVM must be real, not a stub, before the pokes
  produced here are correct for joined queries.
- Row 15 — `lastMutationIDChanges` extraction was already flagged as "not
  implemented" in Rust; the TS source shows exactly how: match
  `patch.id.table === ${app}_{shard}.clients`, validate row via
  `lmidRowSchema`, populate `body.lastMutationIDChanges[clientID]`.
- Row 16 — CVR row-diff (`CVRStore.catchupRowPatches` /
  `CVRQueryDrivenUpdater.received` + `.deleteUnreferencedRows`) must produce
  real `PatchToVersion[]` for view-syncer's pokers to fill `rowsPatch`.

No additions to the translation guide are required for this module. All
patterns and crates used here already have entries in Parts 1 and 2.
