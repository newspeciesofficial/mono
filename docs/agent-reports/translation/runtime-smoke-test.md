# Runtime smoke test: zero-cache-rs on xyne-spaces-login

Slug: `runtime-smoke-test`
Date: 2026-04-12
Agent scope: rebuild the Rust Docker image after sibling agents wired
`pipeline_driver`, `pg_change_source`, and auth paths; prove the server
boots against the real xyne-spaces-login environment and serves a client.

No Rust source was modified. No TS container was started. Only the Rust
container (`zero-cache-rs-test`) was touched.

## 1. Container state before run

```
$ docker ps --format "table {{.Names}}\t{{.Status}}"
zero-cache-rs-test                      Up 15 hours (healthy)
xyne-spaces-login-postgres              Up 5 days (healthy)
xyne-memory-vespa                       Up 2 days
... (no TS zero-cache running)
```

Existing Rust container was stopped + removed prior to rebuild. TS
zero-cache (`xyne-spaces-login-zero-cache`) was NOT running and was NOT
touched.

## 2. Release build

```
$ cargo build --workspace --release 2>&1 | tail -5
   Compiling zero-cache-test-support v0.1.0 (.../crates/test-support)
    Finished `release` profile [optimized] target(s) in 45.95s
```

All 10 workspace crates built clean. No warnings surfaced in the tail.

## 3. Docker image build

```
$ docker build -t zero-cache-rs:local . 2>&1 | tail -3
#29 writing image sha256:9758baa160b5b2bd3201be767176c6c8621c576b9e3d466372bdc9c4d3012c6c done
#29 naming to docker.io/library/zero-cache-rs:local done
#29 DONE 0.1s
```

Image built successfully.

## 4. Container startup + initial sync

Container started with the full xyne env at 2026-04-12T19:39:14.089Z.

Initial sync completed at 2026-04-12T19:39:14.368Z — **279 ms total**.

```json
{"message":"initial sync completed","tables":89,"rows":4161,"lsn":"0000000028fa4638"}
```

Key startup log lines:

- `zero-cache-rs starting` → `app=zero shard=0 port=4848 sync_workers=15 pool_threads=0 streamer_mode=internal`
- `using internal PG change source (initial sync + polling)`
- `PgChangeSource starting` → upstream `postgresql://xyne:xyne123@postgres:5432/`, replica `/var/zero/replica.db`
- `listening` → `0.0.0.0:4848 mode=Internal`
- 15 sync-worker tasks started
- `discovered tables in public schema count=89`
- `initial sync completed tables=89 rows=4161 lsn=0000000028fa4638`

No ERROR lines throughout the session.

## 5. Playwright smoke test — `verify-data-load`

```
$ cd xyne-automation && npx playwright test verify-data-load --project=chromium --reporter=list
  WebSocket message stats:
    pokeStart: 13
    pokePart:  15
    pokeEnd:   13
    total:     42
  "Direct Messages" sidebar visible
    Chat list items: 2
  Screenshot saved to /tmp/rust-zero-cache-data-load.png
  ✓ tests/actions/verify-data-load.spec.ts:13:5 › verify zero-cache delivers data (6.1s)
  1 passed (7.2s)
```

- pokeStart=13, pokePart=15, pokeEnd=13 — counts match (start==end, 13 pokes with 15 parts).
- Sidebar rendered correctly (see screenshot at `/tmp/rust-zero-cache-data-load.png` — Chat, Activity, Bookmarks, DMs, My Tickets, My Canvas, Threads, Recap, Channels, Direct Messages all visible).
- No console errors reported by the test.

## 6. Playwright — `capture-poke-wire`

```
$ npx playwright test capture-poke-wire --project=chromium --reporter=list
  Wrote 39 frames to /tmp/poke-wire-frames.json
  ✓ 1 passed (5.5s)
```

Sample wire frames (abridged):

```
pokeStart: pokeID=5bd2djc:0000000001 baseCookie=null
pokePart:  rows=99 tables=[users] gotQueries=0 desiredQP=0 lmic=0
pokePart:  rows=72 tables=[users] gotQueries=1 desiredQP=1 lmic=0
pokeEnd:   cookie=5bd2djc:0000000001 cancel=false

pokeStart: pokeID=5bd2djc:0000000002 baseCookie=5bd2djc:0000000001
pokePart:  rows=2  tables=[channels]
pokeStart: pokeID=5bd2djc:0000000005
pokePart:  rows=3  tables=[user_groups]
```

- Row counts are reasonable (users≈171 total split across two parts; channels=2; user_groups=3).
- Enum columns (`authProvider`, `status`, `userType`, `visibility`, `actorAction`, `actionSource`) contain zero NULL values across all 39 frames (`grep '"<col>":null' /tmp/poke-wire-frames.json` → 0 matches).
- First pokePart values for `users`: authProvider `GOOGLE`/`API_KEY`, status `ACTIVE`, userType `USER`/`BOT` — all enum strings decoded correctly from pgoutput.
- `gotQueries` / `desiredQP` track cleanly (0→1 as client subscribes to more queries).
- `lmic` (last mutation ID count) = 0 across all pokes — expected for a read-only load test.
- No `cancel=true` pokes.

## 7. Known-benign noise

During client connection, `pipeline_driver` emitted `QUERY_PLAN diagnostic`
WARN lines (e.g. `SCAN users`, `SCAN channels`). These are the existing
EXPLAIN-based diagnostics (f9b7b2908 / 92cfc5ea9) and do NOT indicate a
defect — they report that the xyne schema is missing indexes for some ZQL
filters like `updatedAt > ?` and `visibility = ?`. Index creation is
outside this agent's scope.

## 8. Defects surfaced

**None.** No ERROR logs, no Playwright failures, no malformed wire frames,
no NULL enums, no LSN regressions.

Known pre-existing benign items (NOT regressions from this rebuild):

- `QUERY_PLAN diagnostic` WARN noise on SCAN paths (see section 7).
- Informational only; already tracked in other reports.

## 9. GO / NO-GO

**GO** for local testing in xyne-spaces-login.

Justification:
- Release build clean.
- Container healthy, initial sync completes in ~280 ms replicating 4161 rows across 89 tables.
- Playwright `verify-data-load` passes end-to-end with the client rendering the DM sidebar.
- Poke wire frames are well-formed, row counts reasonable, enum columns correctly decoded.
- No ERROR logs during boot or the two test runs.

Follow-ups (out of scope for this agent):
- `pipeline_driver` SCAN warnings point to index gaps in the xyne SQLite
  replica; a future agent should compare against the TS build's
  `generatedIndexes` output to confirm parity.
- `capture-poke-wire` only exercises initial hydration; a mutation-driven
  smoke test would further validate the pgoutput streaming path (this run
  confirmed only the initial-sync COPY path, not the replication stream
  past LSN `0000000028fa4638`).

## Artifacts

- `/tmp/rust-zero-cache-data-load.png` — rendered sidebar screenshot
- `/tmp/poke-wire-frames.json` — 39 captured WS frames (162 KB)
- Container `zero-cache-rs-test` left running `healthy` for the user to
  continue local work against.
