# GUIDE-DELTA — Phase 1 aggregate

Consolidates 38 new-pattern files and 14 validation reports into a single diff
to apply to `docs/rust-translation-guide.md`.

---

## 1. Deduplicated new-pattern table

| # | Pattern name | Category | Classification | Files referencing | Source reports |
|---|---|---|---|---|---|
| 1 | Lenient UTF-8 decode for wire-protocol C-strings | D/E (data/wire) | Idiom-swap | pgoutput/binary-reader.ts:3-4,38-54 | pgoutput |
| 2 | Valita runtime schema → serde (incl. `.map()` transforms) | C (library/data) | Idiom-swap | pg/schema/{ddl,published,shard,init}.ts; pg/{change-source,backfill-*}.ts; replicator/schema/{replication-state,change-log}.ts | pg-change-source, replicator |
| 3 | `node:stream` `getDefaultHighWaterMark` flush threshold | B (node stdlib) | Idiom-swap (constant) | change-streamer-service.ts:3,334,391-404 | change-streamer |
| 4 | `node:v8` `getHeapStatistics()` back-pressure budget | B (node stdlib) | Redesign | storer.ts:3,144-154,251-325 | change-streamer |
| 5 | `json_to_recordset` / `UNNEST` batched upsert | C/E (lib/wire) | Idiom-swap | cvr-store.ts:741-923; row-record-cache.ts:384-425 | cvr, snapshotter |
| 6 | Optimistic row-lock `FOR UPDATE` + version/ownership check | C/D (concurrency/data) | Idiom-swap | cvr-store.ts:926-957,1026-1114,1257-1272 | cvr |
| 7 | Branded phantom-tagged primitive (`TTLClock`) → `#[serde(transparent)]` + `#[postgres(transparent)]` newtype | A (language; extends A24) | Direct | ttl-clock.ts:1-16; cvr.ts:992-1058; cvr-store.ts:56-97,486-514 | cvr |
| 8 | Version-bounded catchup-patch query (lexi cookie, open-lower/closed-upper) | D (data) | Idiom-swap | cvr-store.ts:637-715,1257-1272 | cvr |
| 9 | postgres.js tagged-template dynamic DML (INSERT/UPDATE/DELETE by record) | C (library/wire) | Redesign | mutagen.ts:391,399-403,416-420,429-437 | mutagen |
| 10 | COPY (query) TO STDOUT streaming + TSV parsing | E (wire) | Idiom-swap | initial-sync.ts:565-606; backfill-stream.ts:151-194 | pg-change-source |
| 11 | `CREATE_REPLICATION_SLOT … LOGICAL pgoutput` via simple-query on replication connection | E (wire) | Risky | initial-sync.ts:84-88,371-383; backfill-stream.ts:220-254 | pg-change-source |
| 12 | Upstream plpgsql event triggers + `pg_logical_emit_message` | E (wire) | Direct (install) / Risky (consume) | pg/schema/ddl.ts:143-369; pg/schema/shard.ts:242-399; change-source.ts:753-1139 | pg-change-source |
| 13 | Versioned incremental upstream schema migrations with `AutoResetSignal` | A/lib | Redesign | pg/schema/init.ts:32-266 | pg-change-source |
| 14 | `SET TRANSACTION SNAPSHOT` + N-way transaction-pool fan-out | C/E | Idiom-swap | initial-sync.ts:146-358; backfill-stream.ts:220-254 | pg-change-source |
| 15 | `MaybePromise<T>` sync fast-path in async-shaped API | A (language) | Idiom-swap | write-authorizer.ts:528-569 | auth |
| 16 | `performance.now()` monotonic timing (`Timer` with lap) | B (node stdlib) | Direct | pool-thread.ts:147,370,485,567-745; remote-pipeline-driver.ts:363-640; pipeline-driver.ts:620-625 | pipeline-driver |
| 17 | `crypto.randomUUID()` for scratch IDs | B (node stdlib) | Direct | pool-thread.ts:29,98 | pipeline-driver |
| 18 | `tmpdir()` + `path.join()` for per-worker scratch dir | B (node stdlib) | Direct | pool-thread.ts:30-98 | pipeline-driver |
| 19 | `SharedArrayBuffer` + postMessage zero-copy workaround (not needed in Rust) | C (concurrency) | Redesign (drop) | workers/pool-protocol.ts:316-333; pool-thread.ts:129-144; remote-pipeline-driver.ts:562-636 | pipeline-driver |
| 20 | Cross-worker WebSocket/socket handoff via `process.send(msg, handle)` | B/C | Redesign | workers/syncer.ts:28,122-127; worker-dispatcher.ts:11,169-190 | workers |
| 21 | `js-xxhash` xxHash32 for stable routing hash | C (library) | Idiom-swap | worker-dispatcher.ts:5,71 | workers |
| 22 | Synchronous `node:fs` read/write for routing-assignment persistence | B (node stdlib) | Idiom-swap | worker-dispatcher.ts:2,38-66 | workers |
| 23 | NULL-aware predicate short-circuiting (IS/IS NOT vs other ops) | A/D (zql) | Idiom-swap | zql/builder/filter.ts:62-150 | zql-builder |
| 24 | SQL LIKE/ILIKE → regex, with no-wildcard fast path | A (zql) | Idiom-swap | zql/builder/like.ts:4-71 | zql-builder |
| 25 | Custom-comparator sorted set (`BTreeSet<Entry>` with closure cmp) | D (zql) | Direct (`BTreeMap<String,_>`) | zql/ivm/memory-storage.ts; shared/src/btree-set.ts | zql-ivm-joins, zql-ivm-ops |
| 26 | Cooperative-yield token in sync generator (`Stream<T \| 'yield'>`) | A (zql) | Redesign | zql/ivm/{operator,join,exists,flipped-join,fan-out,filter-operators,stream}.ts | zql-ivm-joins, zql-ivm-ops |
| 27 | Generator scope-guard on per-push state (`#inPush` / `#inprogressChildChange`) | A (zql) | Idiom-swap | zql/ivm/{exists,join,flipped-join}.ts | zql-ivm-joins, zql-ivm-ops |
| 28 | Manual iterator cleanup (`iter.return?/throw?`) in merge loop | A (zql) | Idiom-swap (Drop) | zql/ivm/{flipped-join,union-fan-in}.ts | zql-ivm-joins |
| 29 | Node-relationship lazy thunk (`Record<string, () => Stream<Node>>`) | D (zql) | Redesign | zql/ivm/data.ts; join/exists/flipped-join | zql-ivm-joins |
| 30 | `structuredClone` over JSONValue → derived `Clone` | A (zql) | Direct | zql/ivm/memory-storage.ts:48 | zql-ivm-joins |
| 31 | Min/Max sentinel values in sort-order bounds (`Bound::{Min,Value,Max}`) | D (zql) | Direct (idiom-swap) | zql/ivm/memory-source.ts:303-318,766-804 | zql-ivm-ops |
| 32 | Dynamic-method-name dispatch on BTreeSet (`valuesFrom` vs `valuesFromReversed`) | A (zql) | Direct | zql/ivm/memory-source.ts:806-814 | zql-ivm-ops |
| 33 | Commit state only on normal generator completion (try/catch/finally with `exceptionThrown` flag) | A (zql) | Redesign | zql/ivm/take.ts:174-208 | zql-ivm-ops |
| 34 | Literal-key-overloaded Storage interface (`MAX_BOUND_KEY` vs string) | A (zql) | Redesign | zql/ivm/take.ts:20,27-33,72 | zql-ivm-ops |
| 35 | Epoch-versioned per-source overlay for reentrant fetch during push | D (zql) | Redesign | zql/ivm/memory-source.ts:53-101,365-570,615-759; take.ts:55-56,110-118,688-695 | zql-ivm-ops |
| 36 | Postgres server-side cursor streaming (portal + `query_portal` loop) | C/E (library/wire) | Idiom-swap | row-record-cache.ts:169,353 | snapshotter |

Note: **BigInt-safe JSON** is already covered by Part 3 Risk #3 + Part 2
`json-custom-numbers` row; see section 5 below for how it resurfaces.

---

## 2. Proposed additions to Part 1 of the guide

### Proposed A (language) additions

| # | TS | Rust | Verdict | Rule |
|---|---|---|---|---|
| A25 | `new TextDecoder()` without `{fatal:true}` on binary blobs | `String::from_utf8_lossy(&buf).into_owned()` (never `str::from_utf8`) | Idiom-swap | Use lossy decode for all wire-protocol C-strings; `simdutf8` optional hot-path. |
| A26 | TS branded phantom-tagged primitive (`type T = {[tag]: true}`) | `#[serde(transparent)] #[postgres(transparent)] struct T(f64)` newtype | Direct | Extension of A24: forward the SQL/JSON encoding via `transparent`; `Default` must yield the zero value. |
| A27 | `MaybePromise<T>` / sometimes-sync return in an awaited API | `async fn` (one trivial `Poll::Ready`) or `impl Future` + `futures::future::Either::Left(future::ready(v))` | Idiom-swap | Prefer plain `async fn`; reach for `Either` only if a benchmark shows the extra poll matters. |
| A28 | SQL LIKE/ILIKE/NOT [I]LIKE pattern predicate | `regex::RegexBuilder::new(...).multi_line(true).case_insensitive(ci).build()?`; no-wildcard fast-path uses `str::eq` / `to_lowercase` | Idiom-swap | Precompile per `(pattern,flags)`; escape `$()*+.?[]\^{\|}`; `\\_` / `\\%` literal; appears in zql only. |
| A29 | NULL-aware predicate semantics (NULL in `=`/`<` etc = false; `IS`/`IS NOT` participates) | `match (op, lhs, rhs)` with explicit null branches; split AST into `BoundValuePosition` to make `static` unrepresentable | Idiom-swap | Collapse "column missing" with "column is null" at `row.get(...)`. Zql only. |
| A30 | Cooperative-yield sentinel in sync generator (`Stream<T \| 'yield'>`) | `enum Yielded<T> { Value(T), Yield }` over `Iterator`, or `genawaiter::sync::Gen` | Redesign | Pick once; propagate to every operator signature (`fetch`/`push`/`filter`). Do NOT use `async_stream` (A16) — these are sync. |
| A31 | `try { … } finally { this.#field = undefined }` scope guard on a generator | RAII guard struct with `Drop` field of the operator's state machine; `Cell`/`RefCell` for interior mutability | Idiom-swap | Guard must live on generator state, not a stack local. |
| A32 | Manual iterator cleanup (`iter.return?/throw?`) in merge | Rely on `Drop` of the boxed iterators stored in a `Vec`; no per-reason signalling without an explicit `close(reason)` method | Idiom-swap | Unwinding panics drop iterators cleanly; avoid `panic=abort`. |
| A33 | TS function overload by literal string key (`get(key: typeof MAX_BOUND_KEY): Row; get(key: string): TakeState`) | Split trait methods (`get_max_bound` / `get_take_state`) OR typed `enum Key` + `enum Value` | Redesign | No Rust method overloading by arg value type. |
| A34 | Dynamic method dispatch by computed method name | `if cond { data.values_from(…) } else { data.values_from_reversed(…) }`; unify branches with `itertools::Either` or `Box<dyn Iterator>` | Direct | Plain if/else. |
| A35 | `structuredClone` over JSONValue tree | `Clone::clone` on `serde_json::Value` (or equivalent enum); no crate needed | Direct | Reject introducing `Arc`/`Rc` into the value enum without auditing every clone site. |
| A36 | Commit state only on normal generator completion (`exceptionThrown` + `finally`) | Inline commit after the loop; rely on `?`-return / panic unwind to skip it | Redesign | `Drop` cannot distinguish panic vs normal return; do not use `thread::panicking()`. |
| A37 | Versioned incremental upstream schema migrations with `AutoResetSignal` escape hatch | Hand-rolled dispatcher (~100 LOC) over `BTreeMap<u32, Migrator>`; `refinery`/`sqlx::migrate!` don't fit because migrations are code | Redesign | Typed "needs-reset" error distinct from failure; preserve `minSafeVersion` guard. |

### Proposed B (Node runtime / stdlib) additions

| # | TS / Node | Rust | Verdict | Notes |
|---|---|---|---|---|
| B11 | `getDefaultHighWaterMark(false)` as flush threshold | `const FLUSH_BYTES_THRESHOLD: usize = 16 * 1024;` (+ optional CLI env) | Idiom-swap | Constant; matches Node default. |
| B12 | `v8.getHeapStatistics()` heap-proportional budget | Config-driven absolute byte budget **or** `sysinfo::System::total_memory()` × proportion; enforce via bounded `mpsc` or `Notify` | Redesign | No V8 heap-limit concept; semantics necessarily drift. Keep CLI flag name for operator parity. |
| B13 | `performance.now()` + lap timer | `std::time::Instant` + helper `Timer { start, lap_start }` with `.total_elapsed_ms() -> f64` | Direct | Cross-thread/process Instants are not comparable; ship relative durations only. |
| B14 | `crypto.randomUUID()` | `uuid::Uuid::new_v4()` | Direct | Already in Cargo. |
| B15 | `os.tmpdir()` + `path.join()` | `std::env::temp_dir().join(format!(...))`; `tempfile::TempDir::new_in(...)` for auto-cleanup | Direct | `std::env::temp_dir` respects `TMPDIR` / `TMP` / `TEMP`. |
| B16 | Sync `node:fs` read/write on async request path | Startup reads: `std::fs`; hot path: `tokio::fs` or `tokio::task::spawn_blocking`; never `std::fs` in async handler | Idiom-swap | Consider debouncing; pick atomic-rename vs plain write explicitly. |
| B17 | `process.send(msg, handle)` + `WebSocketServer({noServer:true}).handleUpgrade` | **Preferred:** collapse to single tokio process + `mpsc<WebSocket>` per worker task (axum `WebSocketUpgrade::on_upgrade`). **Fallback:** `sendfd` (SCM_RIGHTS) on Unix + `tokio::net::TcpStream::from_std`. | Redesign | No portable equivalent. Commit to Option A in the guide to close this out. |

### Proposed C (concurrency / architectural) additions

| # | Pattern | Rust | Verdict | Sketch |
|---|---|---|---|---|
| C12 | Optimistic row-lock + in-tx version/ownership check | `SELECT … FOR UPDATE` as first statement of `client.transaction()`; return typed `Ownership`/`ConcurrentModification` errors to trigger rollback; pipeline remaining writes with `tokio::try_join!` | Idiom-swap | Preserve READ COMMITTED; don't use REPEATABLE READ to replace the row lock. |
| C13 | SET TRANSACTION SNAPSHOT fan-out (N readers share one PG snapshot) | `build_transaction().isolation_level(RepeatableRead).read_only(true).start()`, then `SET TRANSACTION SNAPSHOT '<name>'` as first statement; exporter session must stay alive until all importers finish | Idiom-swap | Model `TransactionPool` as `Vec<JoinHandle>` each owning a conn + mpsc receiver. |
| C14 | Postgres server-side Portal streaming | `Transaction::bind(&stmt, &params).await?` → `query_portal(&portal, N).await?` loop, optionally wrapped in `async_stream::try_stream!` | Idiom-swap | Portal's lifetime is bound to the transaction; thread `Transaction<'_>` through. |

### Proposed D (data / state) additions

| # | Pattern | Rust | Verdict | Notes |
|---|---|---|---|---|
| D7 | Version-bounded catchup-patch query on lexi cookie (`(after, upTo]`) | `SELECT … WHERE "patchVersion" > $1 AND "patchVersion" <= $2` with `stateVersion:configVersion` lexi strings; run inside a `RepeatableRead read_only` snapshot | Idiom-swap | `NULL` patchVersion rows are never catchup-eligible (SQL 3VL). |
| D8 | Batched UPSERT via `json_to_recordset($1::jsonb)` or `UNNEST($1::text[], …)` | `tokio_postgres::types::Json(&rows)` bound as `$1::jsonb`; or column-parallel `Vec<…>` with `UNNEST` | Idiom-swap | Prefer `json_to_recordset` for fidelity to TS; `COPY … FROM STDIN BINARY` for >50k-row batches. |
| D9 | Min/Max sentinel bounds over user values | `enum Bound { Min, Value(V), Max }` with hand-written `Ord` (Min < any < Max) | Direct | Do **not** model as `Value::Null`; null is a legal user value with its own order. |
| D10 | Epoch-versioned per-source overlay for reentrant fetch during push | `Cell<Option<Overlay>>` + per-connection `last_pushed_epoch: Cell<u64>`; splice overlay into iterator in `generate_with_overlay`; unconditionally clear after inner `output.push` loop | Redesign | No stdlib analogue. Decide explicitly whether to clear in `Drop`/scopeguard or only on success — TS clears only on success. |
| D11 | Node-relationship as lazy thunk (`Record<name, () => Stream<Node>>`) | `HashMap<String, Arc<dyn Fn() -> Box<dyn Iterator<Item=Yielded<Node>>> + Send + Sync>>`; requires `Arc<Operator>` storage so the closure is `'static` | Redesign | Thunk is called 0..N times → must be `Fn`, not `FnOnce`. Eager materialisation as `Vec<Node>` loses TS laziness. |

### Proposed E (wire / protocol) additions

| # | Pattern | Rust | Verdict | Notes |
|---|---|---|---|---|
| E7 | `CREATE_REPLICATION_SLOT … LOGICAL pgoutput` + `DROP_REPLICATION_SLOT` on replication connection | `Config::replication_mode(ReplicationMode::Logical)` + `Client::simple_query(...)`; parse `SimpleQueryMessage::Row` by column name | Risky | Extended query protocol is unavailable on replication connections. Retry once on `42501` after `ALTER ROLE … WITH REPLICATION` on a normal connection. |
| E8 | `COPY (<query>) TO STDOUT` streaming + TSV decode | `Client::copy_out(...)` returning `Stream<Item=Result<Bytes>>`; hand-roll a TSV parser matching PG text-COPY escapes (`\t`, `\n`, `\\`, `\N`=NULL) | Idiom-swap | COPY inside a replication-mode connection doesn't work; use a normal connection + `SET TRANSACTION SNAPSHOT`. |
| E9 | Upstream plpgsql event triggers + `pg_logical_emit_message` | Send plpgsql source verbatim via `batch_execute`; install inside a SAVEPOINT so `42501` downgrades to `ddlDetection=false`; identifiers via `postgres-protocol::escape::escape_identifier`, literals via `escape_literal` | Direct (install) / Risky (consume via pgoutput `Message`) | Requires PG 15+. Per-shard schema isolation must be preserved. |

### Proposed G (testing) — no additions from this wave.

---

## 3. Proposed additions to Part 2 (crate selection)

### Proposed additions

| TS package | Rust crate(s) | Used for | Caveats |
|---|---|---|---|
| `js-xxhash` (`xxHash32` seed 0) | `xxhash-rust` (feature `xxh32`) | Stable routing hash across TS/Rust (blue-green rollout) | Verify `"abc"` seed 0 → `0x32D153FF`. `xxh32` ≠ `xxh64` ≠ `xxh3`. |
| `valita` (shared/src/valita.ts) | `serde` + `serde_json` (already listed) | Runtime schema parsing of JSON / SQLite TEXT columns | `'passthrough'` = default; `.strict()` = `#[serde(deny_unknown_fields)]`; `.map(fn)` = `#[serde(from="Raw")]` / `TryFrom`; `.rest(...)` = `#[serde(flatten)] extra: HashMap`. `v.Infer` has no analogue — struct is the source of truth. |
| `postgres-protocol::escape` | `postgres-protocol` (already transitive) — explicit import | Dynamic DML identifier/literal escaping when hand-building SQL (mutagen, DDL install) | Never pass identifiers as `$N` parameters. Always parameterise values. |
| `async-stream` | `async-stream` (already in Part 4 "missing") | Portal-streaming cursor adapters (`try_stream!`); async generators for catchupRowPatches | Promote from "missing" to explicit use for D7/C14/E8. |
| `sendfd` (if multi-process path kept) | `sendfd` + `tokio::net::UnixStream` | SCM_RIGHTS fd passing (Option B of B17) | Unix-only. Prefer Option A (single-process tokio) and skip this crate. |
| `scopeguard` | `scopeguard` | Generator scope-guards and commit-on-success RAII | `ScopeGuard::into_inner` disarms guard on success. |
| `genawaiter` **or** `gen-iter` (option) | `genawaiter` (preferred) | Sync generators that must also return a value (exists `filter`) | Only if A30 Option B is chosen. Otherwise stay on `enum Yielded<T>` over `Iterator`. |
| `simdutf8` (optional hot-path) | `simdutf8` | Fast UTF-8 pre-validation before `from_utf8_lossy` in pgoutput reader | Optional. Only if profiling shows decode in top frames. |
| `sysinfo` (optional) | `sysinfo` | Deriving a memory-budget proxy for B12 | Only if absolute config knob isn't sufficient. |
| `sqlstate` (alternative) | `sqlstate` | Symbolic SQLSTATE constants (`42501` etc.) | Alternative is a 5-line internal const module. Either is fine; pick one. |

---

## 4. Proposed additions to Part 3 (known risk items)

### Proposed additions

- **postgres.js tagged-template dynamic DML** — has no direct `tokio-postgres`
  equivalent. All call sites (`mutagen/mutagen.ts:391-437`,
  `cvr-store.ts:741-923`, `row-record-cache.ts:384-425`,
  `snapshotter.ts` via internal transaction-pool, DDL install in
  `pg/schema/ddl.ts`) must hand-build SQL with
  `postgres-protocol::escape::escape_identifier` + numbered placeholders +
  `&[&(dyn ToSql + Sync)]`. Factor one internal builder crate — do not
  reinvent per call site.

- **Child-process socket handoff (`process.send(msg, socket)`)** — no
  portable Rust equivalent. Decision: commit to **single-process tokio**
  (collapse `WorkerDispatcher` + `Syncer` + `Mutator` into the axum server;
  route WebSockets via `mpsc<WebSocket>` fan-out keyed by
  `h32(taskID + "/" + clientGroupID) % N`). Record this as a companion bullet
  under B4 so the fallback `sendfd` path is explicitly out of scope unless
  someone re-opens it.

- **IVM cooperative-yield representation (A30)** — the design pick
  (`enum Yielded<T>` vs `genawaiter`) must be committed in the guide before
  any zql/ivm operator is ported. Propagating a mistaken choice through the
  operator graph is a multi-week retrofit.

- **Reentrant fetch-during-push overlay (D10)** — no Rust IVM crate ships a
  comparable mechanism. Spike a small `memory-source` port first; the epoch
  check must exist before any join/exists operator is translated.

- **BEGIN CONCURRENT — upgrade the existing Risk #2 with cross-module
  impact.** Concretely blocks: `snapshotter` (leapfrog), `auth` (`canPostMutation`
  sandbox transaction), `replicator` (`change-processor.ts:352` in `serving`
  mode), `pipeline-driver` (Snapshotter dependency). The guide currently
  describes the two options (custom libsqlite3 build vs. WAL + deferred);
  this wave adds the blocked-module list so the decision's scope is explicit.

---

## 5. Cross-cutting risk inventory

### Risks blocking multiple modules

| Risk | Blocking these modules |
|---|---|
| E4 pgoutput parser + Standby keepalive (no mainline crate) | pgoutput, pg-change-source, change-streamer, replicator, view-syncer (via `version-ready` tick) |
| BEGIN CONCURRENT (not in upstream SQLite) | snapshotter, replicator (change-processor `serving`), auth (canPostMutation sandbox), pipeline-driver (indirect via Snapshotter) |
| E3 `arbitrary_precision` + `#[serde(flatten)]`/`tag` incompat (BigInt JSON) | change-streamer (storer `change` JSON column), replicator (`rowKey`/`initialSyncContext` TEXT columns), cvr (rowKey JSONB), mutagen (HTTP push schema) |
| IVM operator graph (comparison row 14; stub in Rust) | zql-builder, pipeline-driver, view-syncer (poke correctness for joins/subqueries), auth (read + write policy evaluation) |
| CVR row-diff catchup path (comparison row 16; port partial) | view-syncer (`startPoke` `rowsPatch`), cvr |
| `bindStaticParameters` with `preMutationRow` (comparison row 2) | auth (read + write authorizer), mutagen (downstream) |
| JWT verification (comparison row 20; missing) | auth |
| postgres.js tagged-template dynamic DML | mutagen, cvr (upsert batch), snapshotter (row-record-cache batch) |
| Cross-worker socket handoff (B17 decision wait) | workers, syncer topology |
| TsvParser Rust port (out of scope in new-patterns) | pg-change-source, initial-sync, backfill-stream |

---

## 6. Per-module port readiness matrix

| Module | Port readiness | Blocking items |
|---|---|---|
| pgoutput | Ready once A25 lands | Budget ~880 LOC Rust for parser + binary-reader + stream + lsn; transport via `copy_both_simple` still to confirm |
| pg-change-source | Blocked | pgoutput (E4), TsvParser, TransactionPool/runTx/Database, 5 new patterns (valita, replication-slot, COPY TO STDOUT, event triggers, migration framework) |
| change-streamer | Ready once A/B additions land | pgoutput (E4), E3 BigInt JSON, C2 per-subscriber ACK Redesign (in-module, not a gap); B11 + B12 new-pattern decisions |
| replicator | Blocked | pgoutput (E4), BEGIN CONCURRENT decision, valita → serde addition |
| pgoutput transport | Ready once A25 lands | Same as pgoutput row |
| zql-ivm-ops | Blocked | 5 new-pattern additions (A30/31/33/36, D9/D10/D11-via-sibling); guide's own exclusion of zql must be relaxed or a companion section added |
| zql-ivm-joins | Blocked | 5 new-pattern additions (A30/A31/A32/A35, D11); BTreeMap design pick |
| zql-builder | Blocked | IVM operator graph + `completeOrdering` + `planQuery` + `simplifyCondition` must land first; A28 + A29 additions |
| pipeline-driver | Blocked | Full IVM engine, Snapshotter (row 13), ResetPipelinesSignal as `enum` (A15 Redesign applied), B13/B14/B15 new patterns, SAB batch = "drop in Rust" note |
| snapshotter | Blocked | BEGIN CONCURRENT (Risk #2), IVM (row 14); D8 + C14 new patterns must be added |
| view-syncer | Ready once IVM lands | Rows 14, 15 (LMIDs), 16 (row-diff); no guide additions required locally |
| cvr | Ready once CVR-specific additions land | D7, D8, C12, A26 new patterns; row-record-cache.ts must be ported before cvr-store.rs; E3 for `rowKey` |
| auth | Blocked | JWT verify missing (row 20), IVM (row 14), `bindStaticParameters`+`preMutationRow` (row 2), BEGIN CONCURRENT (Risk #2); A27 addition |
| mutagen | Blocked (decision-wait) | Postgres tagged-template DML decision (Risk add), `custom/fetch.ts` retry helper port, `PushResponse` in zero-protocol (row 1 confirmed) |
| workers | Blocked (decision-wait) | Single-process vs multi-process decision; `xxhash-rust` addition; B17 + B16 new patterns; complete message-switch coverage in ws_handler.rs |

---

## 7. Recommended next-wave ordering (Phase 2 translation)

1. **Commit guide decisions** — land all Part 1 additions (A25–A37, B11–B17,
   C12–C14, D7–D11, E7–E9), Part 2 additions (xxhash-rust, valita→serde,
   postgres-protocol::escape, async-stream, scopeguard, + `genawaiter` *iff*
   A30 Option B), and Part 3 additions (tagged-template DML, single-process
   tokio commitment, IVM yield representation pick, D10 overlay mechanism,
   BEGIN CONCURRENT blocked-module list). No code moves until this is done.

2. **pgoutput + logical-replication transport** (unblocks pg-change-source,
   change-streamer, replicator, view-syncer's `version-ready` tick). Budget
   ~880 LOC Rust; risky/spec-heavy per existing Part 3 Risk #1.

3. **BEGIN CONCURRENT policy decision** — custom libsqlite3 build via
   `libsqlite3-sys` `buildtime_bindgen` vs. fallback WAL+deferred. Required
   before snapshotter, replicator `serving` branch, and `canPostMutation` can
   be merged. No code yet; policy pick only.

4. **zql-ivm-ops** (unblocks zql-ivm-joins, zql-builder, pipeline-driver).
   Translate `filter`/`skip`/`take`/`memory-source`/`push-accumulated` +
   `operator`/`change`/`data`/`schema`/`constraint`/`stream` traits against
   an in-memory `Source`. ~3.2 KLOC Rust.

5. **zql-ivm-joins** (unblocks zql-builder, pipeline-driver end-to-end).
   `join`/`flipped-join`/`exists`/`fan-out`/`fan-in`/`union-fan-in`. ~1.5 KLOC
   Rust.

6. **zql-builder** (unblocks view-syncer poke correctness). Requires `zql` +
   `completeOrdering` + `planQuery` + `simplifyCondition` ported first; ~1 KLOC
   Rust.

7. **snapshotter** (unblocks pipeline-driver + view-syncer advance loop).
   Depends on BEGIN CONCURRENT decision (step 3). Port `snapshotter.ts` +
   `row-record-cache.ts`.

8. **cvr** (unblocks view-syncer catchup + mutation patch). Row-record-cache
   first, then cvr.rs, then cvr-store.rs. Depends on A26/C12/D7/D8 additions.

9. **pipeline-driver** (unblocks view-syncer end-to-end). Depends on IVM (4+5),
   Snapshotter (7), `ResetPipelinesSignal` as `enum` variant. `performance.now`
   / `randomUUID` / `tmpdir` already direct; `SharedArrayBuffer` = drop.

10. **replicator** (unblocks view-syncer ingest). Depends on pgoutput (2) +
    BEGIN CONCURRENT decision (3) + valita → serde.

11. **change-streamer** (unblocks external subscribers). Depends on pgoutput
    (2) + E3 BigInt JSON shape; C2 per-subscriber ACK is in-module.

12. **view-syncer** (end-to-end). Depends on 4/5/7/8/9/10/11. No guide
    additions required locally.

13. **auth** (depends on JWT row 20, IVM 4+5, `bindStaticParameters` row 2,
    BEGIN CONCURRENT 3). Mostly mechanical once all four blockers clear.

14. **mutagen + pusher** (depends on `custom/fetch.ts` retry helper +
    tagged-template DML decision + `PushResponse` in zero-protocol).

15. **workers / dispatcher** (depends on the single-process vs multi-process
    commitment). If single-process: fold into axum server; delete
    `worker-dispatcher`/`Mutator`. If multi-process: port via `sendfd`.

File: `/Users/harsharanga/code/mono-v2/docs/agent-reports/GUIDE-DELTA.md`
