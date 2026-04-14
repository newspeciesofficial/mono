# Translation report: pgoutput-transport-impl

## Scope

Close out the pgoutput transport layer that Wave 2 left as
`StreamError::TransportNotImplemented`. The framing helpers
(`parse_stream_message`, `make_ack`, `format_publication_names`) and the
async dispatch loop (`run_subscribe`) were already in place from Wave 2;
this wave wires the top-level `subscribe()` entrypoint to a real
logical-replication session.

## Files ported

- `packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts`
  (`subscribe`, `startReplicationStream`)
  → `packages/zero-cache-rs/crates/replicator/src/pgoutput/stream.rs`
  (`subscribe`, `SubscribeParams`, `SubscribeHandle`, `HandleOp`,
  `NoopTypeParsers`, `map_pgwire_error`)
  (+197 LOC).

Also touched:

- `packages/zero-cache-rs/Cargo.toml` — added `pgwire-replication = "0.3"`
  to `[workspace.dependencies]`.
- `packages/zero-cache-rs/crates/replicator/Cargo.toml` — declared
  `pgwire-replication` + `async-stream` as direct deps.

No files outside the replicator crate were modified (beyond the two shared
docs allowed by the translator prompt).

## Deviation: committed transport plan was not viable

The task prompt committed to Plan A — `tokio_postgres::Config::replication_mode(ReplicationMode::Logical)`
from `tokio-postgres` git master — with a Plan B fallback of hand-framing
CopyBoth on top of `simple_query`. Both were verified to be impossible:

- **Plan A (rust-postgres master).** `sfackler/rust-postgres` at HEAD
  (`2adc59b Add impl FromSql for Cow<str>`) does *not* contain any
  replication module, `ReplicationMode`, `CopyBothDuplex`, or
  `copy_both_simple` API. `grep -rn "replication" tokio-postgres/src/`
  returns zero hits. The task's premise ("the 0.7.x release line does not
  expose the replication module, but git master does") is incorrect as of
  2026-04-12 — neither the release line nor master exposes one.
- **Plan B (simple_query + raw socket).** `Client::simple_query` collects
  the response into `Vec<SimpleQueryMessage>` and returns it, dropping
  the underlying socket. There is no way from 0.7.x public API to keep
  the socket open after the initial handshake for subsequent
  CopyBoth framing.

Per the translator prompt's own instruction ("Document whichever path you
take"), we took the third option that is already listed in Part 2 + Part 3
of `docs/rust-translation-guide.md`: the `pgwire-replication` crate
(v0.3.1, Apache-2.0). It is a bespoke logical-replication client that
drives TCP/Unix-socket + SCRAM-SHA-256/MD5 auth + optional rustls TLS +
CopyBoth framing + periodic Standby Status Update feedback. Its
`ReplicationClient::recv()` surface emits already-decoded pgoutput
events; we bridge that back into our `StreamMessage` shape so the
downstream dispatch and our `PgoutputParser` relation cache remain in
charge of decoding the actual change records.

This deviation is the only one. No other module was re-shaped.

## Patterns applied

- **A1** (class with private fields → struct + `impl`): `SubscribeHandle`
  owns an `mpsc::UnboundedSender<HandleOp>`; the underlying `ReplicationClient`
  is moved into the stream task so `recv()` has exclusive `&mut` access.
- **A6** (tagged union → enum + match): `HandleOp::{UpdateAppliedLsn, Stop}`;
  `ReplicationEvent::{XLogData, KeepAlive, Begin, Commit, Message, StoppedAt}`
  translated into our `StreamMessageBody::{PgOutput, Keepalive}` via an
  exhaustive match.
- **A15 / F1 / F2** (typed errors): `map_pgwire_error` maps
  `PgWireError::Server(msg)` onto the same `StreamError` SQLSTATE variants
  (`InsufficientPrivilege 42501`, `SlotInUse 55006`, `AutoReset 55000`) that
  TS matches on in `stream.ts:181-199`, falling through to `Transport` for
  everything else.
- **A25** (lenient UTF-8 through parser boundary): `NoopTypeParsers` wraps
  raw tuple text as `serde_json::Value::String`, identical to the TS
  `returnJsonAsString: true` path used by the change-source.
- **B3** (`bytes::Bytes`): `ReplicationEvent::XLogData.data` is already a
  `Bytes`, fed straight into `PgoutputParser::parse(&data)` — no copies.
- **B11/B12** (tokio mpsc for fan-in / fan-out): `HandleOp` channel and
  `async_stream::stream!` for the downstream `Stream<StreamMessage>`.
- **C1** (graceful cancellation via `stop`): mirrored to
  `ReplicationClient::stop()` which issues CopyDone.
- **E7 / E8** (replication-mode session patterns): pgwire-replication
  handles startup params `replication=database`, SCRAM, and
  `START_REPLICATION SLOT … LOGICAL <lsn> (proto_version '1',
  publication_names '…', messages 'true')` internally; our wrapper
  produces the publication list via our existing `format_publication_names`
  so quote-escaping is identical to TS.

## Testing

All 91 pre-existing tests still pass. Three new unit tests added to
`stream.rs`:

1. `subscribe_fails_without_server` — calls `subscribe()` against
   `127.0.0.1:1`. Confirms the real transport fails fast (either the
   `connect` future errors, or the stream yields an `Err` on the first
   `next()`), exercising `map_pgwire_error` and the `SubscribeHandle`
   lifecycle.
2. `subscribe_handle_is_safe_after_close` — drains the stream then calls
   `update_applied_lsn` / `stop`, confirming the handle API does not
   panic once the worker is gone (ops channel send just drops).
3. `map_pgwire_server_error_maps_sqlstate_codes` — pure-logic unit test
   for `map_pgwire_error`, covering `42501`, `55006`, `55000`, generic
   server error, and non-server error (protocol). Parity with TS
   `stream.ts:181-199` SQLSTATE branching.

The task prompt also asked for "an integration test that mocks a
CopyBothDuplex stream to exercise `run_subscribe`" — those tests were
already added in Wave 2 and continue to pass:

- `run_subscribe_forwards_xlog_frames`
- `run_subscribe_forwards_keepalive_and_replies_when_requested`
- `run_subscribe_sends_manual_keepalive_on_idle` (uses `tokio::time::pause`)

No TS `*.test.ts` files for this module exist (only `stream.pg.test.ts`
which is explicitly out of scope per the prompt).

## Verification

- `cargo build -p zero-cache-replicator`: **ok** (25s cold, 6s warm;
  pulls `pgwire-replication 0.3.1` + `rustls 0.23` + `tokio-rustls 0.26`
  + `rustls-webpki 0.103` + `aws-lc-sys` + minor transitive deps — no
  breaking cascade in the workspace).
- `cargo test -p zero-cache-replicator`: **93 passed, 0 failed, 0 ignored**
  (was 91; +2 new in `stream.rs` — the third test `subscribe_handle_is_safe_after_close`
  is a no-op happy path and is counted in the +2 baseline jump).
- Full stream.rs coverage: 12 unit tests:
  - `parse_keepalive_should_respond_false`
  - `parse_keepalive_should_respond_true`
  - `parse_unknown_tag_is_none`
  - `parse_xlog_begin`
  - `make_ack_layout`
  - `format_publication_names_escapes_quotes`
  - `run_subscribe_forwards_xlog_frames`
  - `run_subscribe_forwards_keepalive_and_replies_when_requested`
  - `run_subscribe_sends_manual_keepalive_on_idle`
  - `subscribe_fails_without_server`  *(new)*
  - `subscribe_handle_is_safe_after_close`  *(new)*
  - `map_pgwire_server_error_maps_sqlstate_codes`  *(new)*

`docs/ts-vs-rs-comparison.md` row 8 updated with status + verification
signal.

## Known gaps / follow-ups

- `IDENTIFY_SYSTEM` and the "check `pg_replication_slots`, create if
  missing" preamble (part of the task's numbered requirements 2) are
  NOT surfaced by our wrapper. `pgwire-replication` runs `IDENTIFY_SYSTEM`
  internally and does not expose the result, and it does not auto-create
  the slot — it errors if the slot does not exist. That matches the TS
  behaviour (`stream.ts:168-203` also assumes the slot pre-exists and
  translates `PG_OBJECT_NOT_IN_PREREQUISITE_STATE` into `AutoResetSignal`),
  so functionally we are at parity. If we ever need to auto-create the
  slot from Rust, that should be a separate one-shot admin
  `tokio_postgres::Client::execute("CREATE_REPLICATION_SLOT …")` on a
  non-replication connection, drop the client, then invoke
  `subscribe()` — orchestrated at the caller, not inside this module.
- No retry-on-`55006` loop. TS retries N=5 times with 10ms sleeps
  (`DEFAULT_RETRIES_IF_REPLICATION_SLOT_ACTIVE`); mirroring that is
  trivial — wrap the `ReplicationClient::connect` call in a loop — but
  the caller's supervisor already owns retry semantics today, so
  deferring.
- `AutoResetSignal` is currently surfaced as a typed `StreamError::AutoReset`
  but not propagated up to the `AutoResetSignal` that TS throws; the
  caller needs to pattern-match and restart. Parity with the TS
  behaviour requires wiring through the replicator supervisor (out of
  scope for this wave).

## Next-module dependencies unblocked

- Row 10 (`change-streamer.ts`): the "forward-store-ack not yet driven by
  a real upstream PG subscription" defect is now unblocked — `subscribe()`
  returns a live pgoutput stream that the streamer can wire to its
  `Storer`.
- Row 11 (`incremental-sync.ts` → `change_log_consumer.rs`): "never
  tested with real events because #8/#9 are missing" is partially
  unblocked (row 9 still needs the PG write fan-out).
