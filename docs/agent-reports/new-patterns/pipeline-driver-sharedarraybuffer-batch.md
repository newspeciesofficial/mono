# New pattern: SharedArrayBuffer+TextEncoder as a postMessage zero-copy workaround

## Category
C (Concurrency — cross-thread data transfer)

## Where used
- `packages/zero-cache/src/workers/pool-protocol.ts:316-333` (`encodeBatch`, `decodeBatch`)
- `packages/zero-cache/src/server/pool-thread.ts:129-144` (`sendBatch`)
- `packages/zero-cache/src/services/view-syncer/remote-pipeline-driver.ts:562,603,636` (`decodeBatch(msg.changesBuf, msg.changesLen)`)

## TS form
```ts
// Encode RowChange[] into a SharedArrayBuffer (readable from both threads
// without structured clone).
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeBatch(changes: RowChange[]) {
  const encoded = encoder.encode(JSON.stringify(changes));
  const buf = new SharedArrayBuffer(encoded.byteLength);
  new Uint8Array(buf).set(encoded);
  return {buf, len: encoded.byteLength};
}

export function decodeBatch(buf: SharedArrayBuffer, len: number): RowChange[] {
  return JSON.parse(decoder.decode(new Uint8Array(buf, 0, len))) as RowChange[];
}
```

The buffer is attached as a regular property on the message (NOT in the
`transferList`), so Node's `postMessage` treats it as a shared-memory handle
and skips structured-cloning the bytes. Each batch message is ~object header +
one `SharedArrayBuffer` pointer.

## Proposed Rust form
When workers are `std::thread`s communicating via `mpsc`/`crossbeam-channel`,
there is no structured-clone cost — every `Send` value is moved by pointer
already. The SAB trick is unnecessary. Translate as a plain batch message:

```rust
use tokio::sync::mpsc;
use bytes::Bytes;

pub enum PoolResult {
    AdvanceChangeBatch {
        request_id: u64,
        client_group_id: String,
        changes: Vec<RowChange>,        // moved by pointer — no copy
    },
    // ...
}

// Or, if we want the batch to be cheaply cloneable for fanout:
pub struct AdvanceChangeBatch {
    request_id: u64,
    changes: Bytes,                     // pre-encoded JSON, Arc<[u8]> under the hood
}
```

If the two threads must share the *same* allocation and only one writes, the
equivalent is an `Arc<Vec<RowChange>>` or `Arc<[u8]>`. `bytes::Bytes` is the
idiomatic choice for the "pre-encoded bytes, multiple readers" case; see B3
in the guide.

## Classification
Redesign — don't port the SAB mechanism; replace with a move across `mpsc`
(or `Arc<…>` / `bytes::Bytes` if shared-read is actually needed).

## Caveats
- The TS code does JSON-round-trip even with shared memory. This is a quirk
  of the `MessageChannel` boundary (structured clone of arbitrary object
  graphs is expensive, so the author pre-serialises). In Rust the structured-
  clone boundary does not exist; we can ship the `Vec<RowChange>` directly
  and skip the JSON round-trip entirely. This is a *latency* and *allocation*
  win on the Rust side.
- If the Rust port ever needs to push batches across a real process boundary
  (e.g. to a subprocess), reintroduce the encode/decode with `serde_json` or
  `bincode`. The crate `bytes::Bytes` composes well with either.
- `SharedArrayBuffer` byteLength is independent of the written length; that's
  why TS passes a separate `len`. A Rust `Vec<u8>` or `Bytes` carries its own
  length — no separate field.

## Citation
- `bytes::Bytes` — https://docs.rs/bytes/latest/bytes/struct.Bytes.html
- Tokio mpsc — https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html
- Node `MessagePort.postMessage` (structured clone behaviour with SAB) —
  https://nodejs.org/api/worker_threads.html#portpostmessagevalue-transferlist
