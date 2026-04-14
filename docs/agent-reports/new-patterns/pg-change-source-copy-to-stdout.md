# New pattern: COPY (query) TO STDOUT streaming with TSV parsing

## Category
E (Wire / protocol) — part of the Postgres wire protocol but distinct from
pgoutput (E4). This is used for initial sync and schema-change backfills.

## Where used
- `packages/zero-cache/src/services/change-source/pg/initial-sync.ts:565-606`
  — `pipeline(await from.unsafe('COPY (…) TO STDOUT').readable(), new
  Writable({ write, final }))` with a per-chunk `TsvParser`.
- `packages/zero-cache/src/services/change-source/pg/backfill-stream.ts:151-194`
  — `for await (const data of copyStream)` with the same TSV parsing and a
  `flushThresholdBytes` batching loop.
- Buffer / flushing constants defined at
  `initial-sync.ts:412-416` (`INSERT_BATCH_SIZE`, `MAX_BUFFERED_ROWS`,
  `BUFFERED_SIZE_THRESHOLD`) and `backfill-stream.ts:49` (chunk size
  matched to Node's `getDefaultHighWaterMark()`).

## TS form
```ts
const readable = await from.unsafe(`COPY (${select}) TO STDOUT`).readable();
await pipeline(
  readable,
  new Writable({
    highWaterMark: BUFFERED_SIZE_THRESHOLD,
    write(chunk: Buffer, _enc, cb) {
      for (const text of tsvParser.parse(chunk)) {
        /* per-value parse, batch into prepared INSERTs */
      }
      cb();
    },
    final(cb) { flush(); cb(); },
  }),
);
```

## Proposed Rust form
```rust
use futures::StreamExt;
use tokio_postgres::{binary_copy::BinaryCopyOutStream, types::Type};

// Option A: text COPY (matches the TS code byte-for-byte)
let stream = client.copy_out(&format!("COPY ({}) TO STDOUT", select)).await?;
tokio::pin!(stream);
let mut parser = TsvParser::new();
while let Some(chunk) = stream.next().await {
    let chunk = chunk?;                 // bytes::Bytes
    for value in parser.feed(&chunk) {
        // flush into prepared INSERT / rusqlite::Statement::execute
    }
}

// Option B: BinaryCopyOutStream with an explicit schema — faster, no
// escape decoding, but requires OID-to-Rust mapping. Prefer once stable.
```

## Classification
Idiom-swap. `tokio-postgres::Client::copy_out()` returns `impl Stream<Item =
Result<Bytes, Error>>`, which maps cleanly onto the TS `.readable()`
AsyncIterator. The TSV parser (`packages/zero-cache/src/db/pg-copy.ts`, not
in scope) needs a Rust port — there is no crate that exposes Postgres text
COPY tuple/field parsing.

## Caveats
- Text COPY escaping (`\t`, `\n`, `\\`, `\N` = NULL) must be ported
  byte-for-byte. See `TsvParser` in `db/pg-copy.ts`.
- `COPY` inside a replication-mode connection does not work — initial-sync
  uses a normal (non-replication) `pgClient` with `SET TRANSACTION
  SNAPSHOT '<name>'` to attach to the replication slot's snapshot.
- `postgres.js` has a known hang at end-of-COPY (see comment at
  `backfill-stream.ts:120-122`). `tokio-postgres`'s `copy_out` signals
  end-of-stream with `None`; drop the stream and the connection is reusable.
- The backfill uses `importSnapshot()` via `TransactionPool` (internal), which
  maps to spawning a dedicated `tokio::task` that holds a `REPEATABLE READ`
  transaction and answers read requests over an `mpsc` channel.

## Citation
- `tokio_postgres::Client::copy_out`:
  https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Client.html#method.copy_out
- PostgreSQL COPY wire format:
  https://www.postgresql.org/docs/current/sql-copy.html
