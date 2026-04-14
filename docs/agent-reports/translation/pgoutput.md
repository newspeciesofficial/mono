# Translation report: pgoutput

## Files ported

| TS | → | Rust | LOC TS → Rust |
|---|---|---|---|
| `packages/zero-cache/src/services/change-source/pg/logical-replication/binary-reader.ts` | → | `packages/zero-cache-rs/crates/replicator/src/pgoutput/binary_reader.rs` | 101 → 275 |
| `packages/zero-cache/src/services/change-source/pg/logical-replication/pgoutput.types.ts` | → | `packages/zero-cache-rs/crates/replicator/src/pgoutput/types.rs` | 107 → 158 |
| `packages/zero-cache/src/services/change-source/pg/logical-replication/pgoutput-parser.ts` | → | `packages/zero-cache-rs/crates/replicator/src/pgoutput/parser.rs` | 332 → 802 |
| `packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts` (pure framing helpers only) | → | `packages/zero-cache-rs/crates/replicator/src/pgoutput/stream.rs` | 242 (subset) → 239 |
| `packages/zero-cache/src/services/change-source/pg/lsn.ts` | → | `packages/zero-cache-rs/crates/replicator/src/pgoutput/lsn.rs` | 45 → 98 |
| (new) module entry | → | `packages/zero-cache-rs/crates/replicator/src/pgoutput/mod.rs` | — → 30 |

TS in scope: ~827 LOC (excluding `stream.pg.test.ts`).
Rust produced: ~1602 LOC (includes 28 unit tests).

## Patterns applied

- **A1** class with private `#field` → struct + impl (`PgoutputParser`, `BinaryReader`).
- **A5/A6** interface + discriminated union dispatched via `switch(tag)` → enum `Message` + `match` on first byte.
- **A13/A20** object spread with optional override (`...this._typeCache.get(typeOid)`) → `match cache.get(..) { Some(..) => …, None => (None, None) }`.
- **A15** custom error class → `ParseError`/`BinaryReaderError`/`LsnError` with `thiserror::Error`.
- **A19** `Record<string, any>` → `IndexMap<String, TupleValue>` (stable iteration order matches TS object key order in v8).
- **A25 (Phase 1 addition)** `new TextDecoder()` without `{fatal:true}` → `String::from_utf8_lossy(buf).into_owned()`. The single blocker called out in `docs/agent-reports/new-patterns/pgoutput-lenient-utf8-decode.md`.
- **B3** `Buffer`/`Uint8Array` → `&[u8]` views in the reader; `Vec<u8>` for owned binary tuple values.
- **D3** `BigInt` shift/OR → `u64` arithmetic (`read_uint64`, `read_lsn`, `read_time`).
- **D4** bit-shift + OR big-endian parse → explicit shift in `u16`/`u32` (cast to `i16`/`i32` after combine to avoid Rust overflow on signed shifts).
- **E5** LSN format `"H/L"` with `parseInt(…, 16)` → `u64::from_str_radix(_, 16)` + `format!("{:X}/{:X}", …)`. `read_lsn` inside `BinaryReader` pads to 8 hex digits to mirror the TS `padStart(8,'0')` used in the protocol LSN formatter; `from_big_int` in `lsn.rs` does NOT pad (mirrors the TS `lsn.ts` variant that doesn't pad).
- **F1** `throw Error(...)` → typed `Result<_, ParseError>`; all six TS throw-sites have named variants.

`Object.create(null)` — no Rust analogue needed; `IndexMap` has no prototype chain.

## Deviations

- **`stream.ts` is ported only for the pure pieces** — `StreamMessage` shape, `parse_stream_message`, `make_ack`, `format_publication_names`, `micros_since_pg_epoch_now`. The async `subscribe()` entrypoint (which uses `postgres.js` `session.unsafe('START_REPLICATION …').execute().readable()/.writable()`) is **deferred**: stock `tokio-postgres` 0.7.17 does not expose `Client::copy_both_simple` (verified by reading `~/.cargo/registry/src/index.crates.io-…/tokio-postgres-0.7.17/src/client.rs`). The guide's Part 2 "Logical replication" row lists `copy_both_simple`, but it is only available on a patched `tokio-postgres` fork. This matches the Phase-1 validation note (risk item #2: transport library surface differs).
- **`stream.pg.test.ts` is not ported** — it spins up a real Postgres and exercises the end-to-end stream. Deferred per the translator-prompt rule "no live PG in this phase".
- **`TupleValue` models `'u'` (unchanged TOAST)** as its own variant instead of `undefined`, which TS uses in two places: (a) when no `unchangedToastFallback` is available, and (b) in `readKeyTuple` where a `null` in a key column is rewritten to `undefined`. Rust has no `undefined`; we use `TupleValue::Unchanged` as the absence marker. Consumers that need the TS "absent" semantic should check `matches!(v, TupleValue::Unchanged)`. All other kinds (`'b'` binary, `'t'` text after `TypeParser`, `'n'` null) are 1:1.
- **`TypeParsers` is declared as a local trait** in `parser.rs`. The TS `packages/zero-cache/src/db/pg-type-parser.ts` module is out of scope and not yet ported; a caller-supplied trait impl keeps the parser self-contained. `parser` is stored side-band in `relation_parsers: HashMap<i32, Vec<TypeParser>>` rather than inside `RelationColumn`, so `MessageRelation` stays `Debug + Clone` cheaply.

## Verification

- `cargo build -p zero-cache-replicator`: **ok** (dev profile, 0 errors, 1 pre-existing `unused_imports` warning in `zero-cache-db`).
- `cargo test -p zero-cache-replicator`: **blocked by pre-existing test-compile errors in `crates/replicator/src/change_log_consumer.rs`** (unrelated — the `ChangeLogRow` struct was refactored to a single `change` field but the `#[cfg(test)]` block at lines 679–714 still constructs it with the old `state_version/table/row_key/op/row_data` fields). Confirmed pre-existing via `git stash && cargo test → same errors && git stash pop`. My `pgoutput` tests themselves compile and are self-contained; they cannot be exercised because the test harness compiles the whole crate's `#[cfg(test)]` tree at once.
- 28 unit tests were added across `pgoutput/{binary_reader,lsn,parser,stream}.rs` covering: big-endian int reads, lenient UTF-8 decode of a byte 0xFF, NUL-terminated cstr, LSN formatting & zero → `None`, time-epoch offset, array helper, LSN round-trip & malformed inputs, rejects unknown tag, BEGIN / COMMIT / ORIGIN / TYPE parsing + type-cache, RELATION → INSERT end-to-end, DELETE 'K' key-only, TRUNCATE flags (cascade + restart-identity), MESSAGE with transactional flag, INSERT without prior RELATION → `MissingRelation`, UPDATE 'O' with unchanged-TOAST substitution from old, `parse_stream_message` XLogData + keepalive (both should-respond branches) + unknown tag, `make_ack` byte layout, `format_publication_names` quote-escape.
- Status row updated: `docs/ts-vs-rs-comparison.md` line 32 (row #8, "Critical-path chain").

## Next-module dependencies unblocked

- **Change source (PG → CDC table)** — rows #8 and #9 of the comparison table. The parser + LSN helpers are the core of `services/change-source/pg/change-source.ts`. Remaining blockers for a full port: (a) `copy_both_simple` transport (needs a `tokio-postgres` fork or the `postgres-protocol` `CopyBothResponseBody`/`CopyDataBody` framing assembled by hand); (b) per-type parser wiring in `crates/db/src/pg_type_parser.rs` (not yet present); (c) the change-log writer (TS `services/change-streamer/storer.ts`, row #9). Downstream modules `change_log_consumer` (row #11) and the `PgChangeSource` orchestrator (already stubbed in `crates/replicator/src/pg_change_source.rs`) can now consume `Message` directly.
- `crates/replicator/src/pgoutput/lsn.rs` provides the `to_big_int`/`from_big_int` helpers that `initial_sync.rs` and `change_log_consumer.rs` will need when they start emitting/reading `stateVersion` strings in the `changeLog` table.
