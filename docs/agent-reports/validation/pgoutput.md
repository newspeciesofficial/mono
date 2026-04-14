# Validation report: pgoutput

## Scope
- `packages/zero-cache/src/services/change-source/pg/logical-replication/pgoutput-parser.ts` — 332 LOC
- `packages/zero-cache/src/services/change-source/pg/logical-replication/binary-reader.ts` — 101 LOC
- `packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts` — 242 LOC (the file referred to in the prompt as `subscription.ts`; there is no `subscription.ts` in this subtree — the logical-replication stream lives in `stream.ts`)
- `packages/zero-cache/src/services/change-source/pg/logical-replication/pgoutput.types.ts` — 107 LOC
- `packages/zero-cache/src/services/change-source/pg/logical-replication/stream.pg.test.ts` — 477 LOC (read for behavioural contract; not part of the translation surface yet)
- `packages/zero-cache/src/services/change-source/pg/lsn.ts` — 45 LOC
- Total LOC in scope (non-test): 827
- Total LOC including test: 1,304

## Summary
| metric | count |
|---|---|
| TS constructs used | 27 |
| Constructs covered by guide | 26 |
| Constructs NOT covered | 1 |
| Libraries imported | 5 |
| Libraries covered by guide | 5 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 2 |

## Constructs used

| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class Foo { #x } …` with private `#field` and `public` methods | `pgoutput-parser.ts:28-36`; `binary-reader.ts:7-12` | A1, A23 | covered |
| Discriminated union (`Message = MessageBegin \| MessageCommit \| …`) dispatched via `switch` on byte tag | `pgoutput.types.ts:12-22`; `pgoutput-parser.ts:44-69` | A6 | covered |
| `interface` describing a tagged record shape | `pgoutput.types.ts:24-107` | A5 | covered |
| `Record<string, any>` dynamic column tuple | `pgoutput.types.ts:42-43,49,104-106`; `pgoutput-parser.ts:192-212` | A19 (with A10 for `Record<K,V>`) | covered |
| Tagged union exhaustive `switch` with `throw Error(...)` default | `pgoutput-parser.ts:67-69,137-147,207-209` | A6 + F1 | covered |
| Object spread with optional override (`...this._typeCache.get(typeOid)`) | `pgoutput-parser.ts:156-167` | A13 / A20 | covered |
| `Object.create(null)` to avoid prototype chain | `pgoutput-parser.ts:241,261` | A19 (HashMap/BTreeMap have no prototype; see note below) | covered |
| Bit-shift + OR big-endian parse (`<<24 \| <<16 \| …`) | `binary-reader.ts:21-36` | D4 | covered |
| `Uint8Array.subarray` zero-copy slice | `binary-reader.ts:46,56-60` | B3 (`&[u8]` view; use `bytes::Bytes` for ref-counted) | covered |
| `TextDecoder` (non-fatal) for pg C-strings | `binary-reader.ts:3-4,52-54` | — | NEW — see `new-patterns/pgoutput-lenient-utf8-decode.md` |
| NUL-terminated C-string scan (`b.indexOf(0x00, p)`) | `binary-reader.ts:38-50` | D4 | covered |
| `BigInt` arithmetic (shift, OR, add) for 64-bit LSN & PG epoch | `binary-reader.ts:88-96`; `lsn.ts:22-45` | D3 | covered |
| `BigInt("0x...")` hex parse | `lsn.ts:26-27` | D3 + E5 (`u64::from_str_radix(_, 16)`) | covered |
| Hex format with padding (`toString(16).padStart(8,'0').toUpperCase()`) | `binary-reader.ts:82-85`; `lsn.ts:44` | E5 (`format!("{:X}/{:X}", …)` / `format!("{:08X}", …)`) | covered |
| `Buffer.alloc(n)` + `writeBigInt64BE` / `readBigUInt64BE` / `subarray` | `stream.ts:219,223,227,232-241` | B3 + D4 (`bytes::BytesMut`, `byteorder::BE::write_u64`) | covered |
| Async function returning a duplex `(Source<T>, Sink<U>)` pair | `stream.ts:34-147` | B2 + C1 (streams + bounded channel) | covered |
| Construction via `postgres.Sql` library + `session.unsafe(\`START_REPLICATION …\`).execute()` returning duplex `.readable()/.writable()` | `stream.ts:170-179` | Part 2 ("Logical replication (inside `postgres`)") | covered (redesign — risk-flagged) |
| `setInterval` keepalive timer | `stream.ts:98-104` | B7 (`tokio::time::interval` with `MissedTickBehavior::Delay`) | covered |
| `postgres.PostgresError` instance check + SQLSTATE branch | `stream.ts:181-199` | F1 + Part 2 (`sqlstate` or hand-rolled) | covered |
| `await sleep(10)` retry in a for-loop | `stream.ts:168-203` | C6 (exponential backoff; here a fixed sleep is used) | covered |
| Custom error class extending `AbortError` (`AutoResetSignal`) thrown with `{cause: e}` | `stream.ts:196-198` | A15 + F1 | covered |
| `defu(...)` deep-merge of Postgres options | `stream.ts:46-64` | Part 2 (`serde_json_merge` or hand-roll) | covered |
| `pipe({source, sink, parse, bufferMessages})` — custom stream wiring from Node `Readable` into a `Subscription` sink | `stream.ts:133-141` | B2 + C1 | covered |
| `Subscription.create<T>({cleanup})` — custom async queue with FIFO and `queued` counter | `stream.ts:109-116` | C1 (bounded `tokio::sync::mpsc` with `Drop`-based cleanup) | covered |
| `readable.once('close', …)` / `.once('error', …)` event handlers | `stream.ts:118-131` | B2 + C3 (select on `readable.next().await` returning `None` / `Err`) | covered |
| Hex byte-literal switch cases with char-code comments (`0x42 /*B*/`) | `pgoutput-parser.ts:47-66`; elsewhere | A6 | covered |
| Bit-flag extraction via `& 0b1` / `& 0b10` | `pgoutput-parser.ts:114,301-302,316` | A6 / bit ops are direct in Rust | covered |
| `Array.from({length}, fn, this)` with explicit `thisArg` | `binary-reader.ts:69-71` | D4 / basic collection (`(0..n).map(\|_\| f()).collect::<Vec<_>>()`) | covered |

Note on `Object.create(null)`: used to avoid prototype-chain leakage when keys
come from an untrusted source (relation column names). In Rust, `HashMap` /
`BTreeMap` have no analogous hazard, so this pattern is a non-issue.

## Libraries used

| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `postgres` (porsager/postgres, v3.4.7) — used for the replication session, `session.unsafe(...).execute()`, `stream.readable()/writable()`, `.simple()`, `PostgresError`, SSL options plumbing | `tokio-postgres` + `deadpool-postgres` + `postgres-types` (+ `postgres-native-tls`) for the general client; `tokio_postgres::replication::CopyBothDuplex` (returned from `simple_query_copy_both`) for the streaming logical-replication channel; hand-rolled pgoutput parser or `pgwire-replication` for decoding | covered — risk-flagged (see below) |
| `defu` (deep merge) | `serde_json_merge` or a ~30-LOC hand-roll (Part 2) — for a static options struct, a plain literal merge is usually enough | covered |
| `@drdgvhbh/postgres-error-codes` (`PG_ADMIN_SHUTDOWN`, `PG_OBJECT_IN_USE`, `PG_OBJECT_NOT_IN_PREREQUISITE_STATE`) | `sqlstate` crate or a hand-rolled `const` table mapping `&str` SQLSTATE → constant name (Part 2) | covered |
| `@rocicorp/logger` (`LogContext.info?./warn?./debug?./error?.`) | `tracing` + `tracing-subscriber` (Part 2); `lc.info?.(...)` maps to `tracing::info!(...)` inside a span | covered |
| Internal: `postgres.js`'s own `simple()` (which is a simple-query wrapper) | `tokio_postgres::Client::simple_query` | covered (transitively by the `postgres` row) |
| Internal module `../../../../types/streams.ts` (`pipe`, `Source`, `Sink`) | No 1:1 crate — translate locally using `tokio::sync::mpsc` + `futures::Stream`/`Sink`; the guide covers `Stream` + `Sink` + `.forward()` in B2 | covered |
| Internal module `../../../../types/subscription.ts` (`Subscription<T,M>`) | Local translation; guide covers it under C1 + C11 (bounded mpsc with cleanup on drop) | covered |

## Risk-flagged items depended on

- **E4 — Binary pgoutput parser.** This whole module IS the risk item. Every
  message shape (`Begin`, `Relation`, `Insert`, `Update` (K/O/N sub-tags),
  `Delete` (K/O sub-tags), `Commit`, `Truncate` (cascade/restartIdentity
  bits), `Type`, `Origin`, `Message`) and tuple-submessage (`b`=binary /
  `t`=text / `n`=null / `u`=unchanged-TOAST) is enumerated here and must be
  preserved bit-for-bit. The protocol constants used are taken directly from
  the PG `pgoutput.c` source tree and the
  [`protocol-logicalrep-message-formats`](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
  docs.

- **Logical replication transport (Part 2 row).** `postgres.js` returns
  node-stream `readable()/writable()` handles from
  `session.unsafe('START_REPLICATION SLOT … LOGICAL …').execute()`. The Rust
  equivalent is `tokio_postgres::Client::copy_both_simple` (or the newer
  `replication` submodule) returning a `CopyBothDuplex<Bytes>` that implements
  `Stream<Item = Result<Bytes, _>>` + `Sink<Bytes>` — shape-compatible but
  the library surface is different.

## Ready-to-port assessment

This scope can be translated now, but only if the team accepts it as a
**risky / spec-heavy** port and budgets the 500+ LOC parser the guide already
calls out. All TS constructs in scope map cleanly to documented Rust patterns
except for **lenient UTF-8 decoding** of PG C-strings via `TextDecoder`
(non-fatal); that single gap is filed in
`docs/agent-reports/new-patterns/pgoutput-lenient-utf8-decode.md`. Nothing in
the scope requires a Redesign that is not already named in the guide.

Concretely, to start a mechanical port I would want the following in the
guide first:

1. A single-sentence entry on **lenient UTF-8 decoding** (`String::from_utf8_lossy` as the default for wire-protocol strings unless the protocol guarantees UTF-8) — see the new-patterns file.

That is the only blocker strictly arising from this scope. The remaining
risks are design decisions the guide already flags:

2. **Confirm the transport.** Use `tokio_postgres::Client::copy_both_simple`
   wrapped around a `START_REPLICATION SLOT <n> LOGICAL <lsn>
   (proto_version '1', publication_names '<list>', messages 'true')` simple
   query. Verify that `tokio-postgres` surfaces the server's CopyData frames
   (the `w` XLogData and `k` PrimaryKeepaliveMessage framing in
   `stream.ts:215-228`) verbatim — this is how the TS code reads LSNs out of
   byte 1 and keepalive `shouldRespond` out of byte 17. If the crate instead
   strips the CopyData header, the handler needs to be adjusted; if it
   preserves it, this is a line-for-line translation.
3. **Pick a SQLSTATE source.** The `sqlstate` crate has the same symbolic
   constants as `@drdgvhbh/postgres-error-codes`; the alternative is a
   5-line internal module. Either is fine — name the choice in the guide.
4. **Pick a keepalive strategy.** The TS code runs a `setInterval` at
   `wal_sender_timeout * 0.75 / 5` and sends an empty ack when idle. The
   equivalent is `tokio::time::interval` with
   `MissedTickBehavior::Delay` inside a `tokio::select!` loop that also
   watches the inbound CopyBothDuplex stream. Guide B7 covers this.
5. **Custom `Subscription` / `pipe` wiring** is local internal code covered
   by C1/B2; translate with a bounded `tokio::sync::mpsc::channel(5)` and a
   forwarding task that `.await`s on both the incoming stream and a
   `CancellationToken`.

Estimated LOC in Rust for a 1:1 port of this scope:

- `pgoutput-parser.rs` — ~400 LOC (332 LOC in TS; Rust gets match ergonomics
  but verbose enum construction)
- `binary-reader.rs` — ~120 LOC
- `stream.rs` — ~300 LOC (the keepalive + ack framing is ~40 LOC on its own)
- `lsn.rs` — ~60 LOC

Total ~880 LOC Rust for ~827 LOC TS — consistent with the guide's "budget
~500 LOC for a parser module" (parser only) and ~350 LOC for the transport
plumbing.

Once the guide adds the lenient UTF-8 note, this scope is mechanical. The
execution risk is entirely concentrated in two things which are both
already in the guide's Risk list:

- Getting the pgoutput message framing right (every byte, every version-1
  assumption this code makes — e.g. no `proto_version '2'` streaming,
  no `proto_version '3'` two-phase, no v16 column-list filtering).
- Getting the `CopyBothDuplex` transport + Standby Status Update framing
  (`r` + LSN×3 + microseconds-since-2000-01-01) correct.
