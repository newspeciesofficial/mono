# Translation report: pgoutput-transport

## Scope

Port the async transport layer on top of the Wave-1 pgoutput parser — the
part of
`packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts`
that opens a logical-replication connection, drains the `CopyBothDuplex`
stream into typed `StreamMessage`s, and keeps the WAL sender alive via
Standby Status Updates.

## Files ported

- `packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts`
  (242 LOC)
  → `packages/zero-cache-rs/crates/replicator/src/pgoutput/stream.rs`
  (454 LOC — framing helpers were already in place from Wave 1; this slice
  added the `StreamError` enum, the `SubscribeConfig`, the generic
  `run_subscribe` dispatch loop, the placeholder `subscribe` entrypoint, and
  four `tokio::test` cases for the new surface).

No sibling `subscription.ts` exists in the TS tree (confirmed via grep).

## Patterns applied

From `docs/rust-translation-guide.md`:

- **F1 — thiserror enum.** `StreamError` with `#[from] ParseError` and
  SQLSTATE-aligned variants (`42501`, `55006`, `55000`).
- **A6 — tagged union + match.** `StreamMessageBody::{PgOutput, Keepalive}`
  (already in place from Wave 1) drives the dispatch branch.
- **A16 — async generator → `Stream`.** Forwarded via `tokio::sync::mpsc`
  receiver; the loop is a plain `tokio::spawn`ed task that owns the parser
  and both transport halves (keeps lifetimes simple vs. `async_stream!`).
- **B7 — `setInterval` → `tokio::time::interval` with
  `MissedTickBehavior::Delay`.** Matches the TS `livenessTimer`.
- **B8 — cancellation = drop.** Dropping the returned receiver terminates
  the loop on the next send.
- **A25 — lenient UTF-8** already applied in the Wave-1 binary reader; no
  new string decoding here.
- **B3 — `bytes::Bytes`** for frame data and outbound acks.
- **C14 — portal streaming** is the shape template, though the actual
  transport crate is blocked (see Deviations).
- **Phase 1 Risk #1 — pgoutput parser + Standby keepalive.** This slice
  implements the keepalive loop; the transport open is still blocked on a
  crate decision (see Deviations).

## Behavioural parity with TS

| TS (`stream.ts`) | Rust |
|---|---|
| `subscribe()` top-level (lines 34-147) | `subscribe()` — intentionally returns `StreamError::TransportNotImplemented` until the transport crate lands |
| `walSenderTimeoutMs` query (72-77) | Passed in via `SubscribeConfig::wal_sender_timeout_ms`; the query itself is the caller's job once transport is wired |
| `manualKeepaliveTimeout = walSenderTimeoutMs * 0.75` | `manual_keepalive = ms * 3 / 4` (integer math equivalent) |
| `livenessTimer = setInterval(..., manualKeepaliveTimeout / 5)` | `tokio::time::interval(tick)` with `MissedTickBehavior::Delay` |
| `sendAck(lsn)` + `lastAckTime = Date.now()` | `writable.send(...)` + `last_ack = Instant::now()` |
| `parseStreamMessage(buf, parser)` on every frame (209-229) | `parse_stream_message` — already Wave-1; now called from `run_subscribe` |
| `shouldRespond` branch (the TS sink consumer acks when true) | Inlined reply inside `run_subscribe` (ack lsn=0 on `Keepalive{should_respond:true}`) |
| `Subscription.create({cleanup: ...})` with `bufferMessages: 5` | `mpsc::channel(cfg.channel_capacity)`, default 5 |
| `formatPublicationNames` (154-158) | `format_publication_names` (already Wave-1; unchanged) |
| `startReplicationStream` retry on 55006 with 10ms sleep, 55000 → `AutoResetSignal` | `StreamError::SlotInUse` and `StreamError::AutoReset` variants defined; the retry loop itself is colocated with the transport and will land when `subscribe()` is wired |

## Deviations

- **Transport not wired** — Per Part 3 Risk #1 and the translator prompt's
  "last resort" guidance, I did **not** hand-roll the wire protocol. Stock
  `tokio-postgres` 0.7.17 does not expose `copy_both_simple` /
  `ReplicationMode::Logical` / `CopyBothDuplex` (verified by inspecting
  `~/.cargo/registry/src/index.crates.io-.../tokio-postgres-0.7.17/src/`;
  none of the three identifiers appear). The prompt's three options were:
  (a) use `simple_query` + `AsyncMessage::CopyBoth` (not available on
  0.7.17 either), (b) bump `tokio-postgres` to a version that exposes
  the replication submodule, (c) hand-roll the wire protocol (explicitly
  marked last resort). Pulling in either a newer `tokio-postgres` fork or
  the `pgwire-replication` crate (listed in guide Part 2 as a Risk option)
  is a non-trivial dependency change that affects all SQL call sites in the
  workspace (TLS story, SCRAM, connection pool). That decision is outside
  this slice's scope per the translator prompt ("mark Port partial with a
  clear 'needs X' and stop").
  
  **Mitigation.** The dispatch loop is a generic `run_subscribe<R, W>` over
  `Stream<Item=Result<Bytes>>` + `Sink<Bytes>`. Once the transport crate
  decision is made, wiring is one `.map()` from `CopyBothDuplex`'s native
  item type into `Bytes` plus a `PollSender` for acks. None of the loop
  logic needs to change.

- **Inline keepalive reply vs. TS external sink.** TS lets the sink
  consumer decide whether to ack a `shouldRespond=true` keepalive by
  calling `sendAck(0n)` from the subscription consumer. In Rust I reply
  directly inside `run_subscribe` because (a) the keepalive byte and the
  `last_ack` bookkeeping live together anyway, and (b) there is no
  `Subscription` abstraction above this to route the signal. Observable
  behaviour on the wire is identical.

- **Pre-existing broken `change_log_consumer.rs` tests fixed.** In-scope
  per the prompt ("the translation guide rule requires `cargo test
  -p <crate>` green"). Two tests constructed `ChangeLogRow` with the
  old `{state_version, table, row_key, op, row_data}` fields; I rewrote
  them to the current `{watermark, pos, change: json}` shape where
  `change.tag` is `insert`/`delete` and `change.relation.name` / `change.new`
  / `change.key` match what `apply_changes_to_sqlite` reads. All
  assertions preserved (row counts, watermark advancement).

## Verification

- `cargo build -p zero-cache-replicator`: **ok** (no new warnings from
  this slice; the pre-existing `unused import` in `crates/db/src/pg.rs` is
  untouched, out of scope).
- `cargo test -p zero-cache-replicator`: **91 passed, 0 failed, 0 ignored**
  (pre-slice: 15 compile errors in `change_log_consumer.rs` tests; now
  compiles and passes). 10 tests live in `pgoutput::stream::tests`:
  - `make_ack_layout`
  - `parse_keepalive_should_respond_false`
  - `parse_keepalive_should_respond_true`
  - `parse_unknown_tag_is_none`
  - `parse_xlog_begin`
  - `format_publication_names_escapes_quotes`
  - `subscribe_returns_transport_not_implemented` — asserts the
    placeholder error
  - `run_subscribe_forwards_xlog_frames` — ingests a BEGIN frame and
    checks that `Message::Begin` surfaces on the receiver with the right
    LSN + xid; then asserts clean shutdown on source close
  - `run_subscribe_forwards_keepalive_and_replies_when_requested` —
    feeds a `shouldRespond=1` keepalive and checks that both the message
    is forwarded AND a 34-byte `'r'` ack reaches the sink
  - `run_subscribe_sends_manual_keepalive_on_idle` — uses
    `tokio::time::pause` + `advance` to fast-forward past the 75%
    window with no incoming traffic, verifies a manual keepalive is
    written
- `docs/ts-vs-rs-comparison.md` row 8 updated with the partial-port
  status, verification counts, and a clear "needs transport crate
  decision" note.

## Next-module dependencies unblocked

- **Partial unblock** for `pg-change-source`, `change-streamer`,
  `replicator`, and `view-syncer` — the dispatch loop and `StreamMessage`
  surface are now stable; downstream callers can write against
  `run_subscribe` immediately, substituting a mock stream in tests.
- **Blocked** on a transport-crate decision for end-to-end operation:
  either bump `tokio-postgres` to a release that exposes
  `copy_both_simple` (verify feature stability first), or adopt
  `pgwire-replication` (Part 2 Risk-option row — its own TLS/SCRAM stack
  needs to be reconciled with the existing `native-tls`-backed pool).
  This decision must happen before the pg-change-source slice can be
  merged.
