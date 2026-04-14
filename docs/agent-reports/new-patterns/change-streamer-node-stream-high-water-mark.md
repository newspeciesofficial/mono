# New pattern: `node:stream` `getDefaultHighWaterMark` as an I/O flush threshold

## Category

B (Node stdlib)

## Where used

- `packages/zero-cache/src/services/change-streamer/change-streamer-service.ts:3`
  (import)
- `packages/zero-cache/src/services/change-streamer/change-streamer-service.ts:334`
  (`const flushBytesThreshold = getDefaultHighWaterMark(false);`)
- `packages/zero-cache/src/services/change-streamer/change-streamer-service.ts:391-404`
  (threshold switch between `forward` fire-and-forget and
  `forwardWithFlowControl` with per-subscriber ACK).

## TS form

```ts
import {getDefaultHighWaterMark} from 'node:stream';

// 16 KiB on modern Node.js for non-objectMode streams.
const flushBytesThreshold = getDefaultHighWaterMark(false);

for await (const change of stream.changes) {
  unflushedBytes += this.#storer.store(entry);
  if (unflushedBytes < flushBytesThreshold) {
    this.#forwarder.forward(entry); // fire-and-forget
  } else {
    await this.#forwarder.forwardWithFlowControl(entry); // wait for ACKs
    unflushedBytes = 0;
  }
}
```

## Proposed Rust form

```rust
// Pin the threshold to the documented Node default (16 KiB for bytes streams).
// Expose it as a config knob — a dynamic runtime query is not available.
const DEFAULT_HIGH_WATER_MARK: usize = 16 * 1024;

let mut unflushed_bytes = 0usize;
while let Some(change) = stream.changes.next().await {
    unflushed_bytes += storer.store(&entry);
    if unflushed_bytes < DEFAULT_HIGH_WATER_MARK {
        forwarder.forward(entry); // fire-and-forget
    } else {
        forwarder.forward_with_flow_control(entry).await; // wait for ACKs
        unflushed_bytes = 0;
    }
}
```

## Classification

Idiom-swap. Node exposes the default high-water mark as an API; in Rust the
value is a constant baked into the application (or a `tokio::io::BufWriter`
capacity if a concrete writer is in use). The semantics — "switch from
pipelined send to ACK-awaited send once this many bytes are in flight" — are
preserved by a constant.

## Caveats

- Node's `getDefaultHighWaterMark(false)` returns 16 KiB at the time of
  writing, but is technically mutable at runtime via
  `stream.setDefaultHighWaterMark`. The TS code does not mutate it, so a
  constant is behavior-preserving today. If we want knob-parity, expose a CLI
  / env var (`ZERO_CS_FLUSH_BYTES`, default 16384) instead of hard-coding.
- The threshold only controls when the forward side yields; the store side
  has an independent heap-proportion back-pressure (see the
  `change-streamer-node-v8-heap-statistics.md` pattern).

## Citation

- Node.js docs:
  <https://nodejs.org/api/stream.html#streamgetdefaulthighwatermarkobjectmode>
  — documents the 16384-byte default and `setDefaultHighWaterMark`.
