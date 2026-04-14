# New pattern: performance.now() monotonic timing

## Category
B (Node stdlib / runtime)

## Where used
- `packages/zero-cache/src/server/pool-thread.ts:147` (`createTimer()`)
- `packages/zero-cache/src/server/pool-thread.ts:370` (`handleAdvance` entry)
- `packages/zero-cache/src/server/pool-thread.ts:485` (advance end)
- `packages/zero-cache/src/server/pool-thread.ts:567,670,705,745` (round-robin scheduler)
- `packages/zero-cache/src/services/view-syncer/remote-pipeline-driver.ts:363,437,549,566,582,640` (postTime/beginTime/completeTime stamps)
- `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts:620,625` (`tAdvanceStart`, `snapshotMs`)

## TS form
```ts
const start = performance.now();
// ... work ...
const elapsedMs = performance.now() - start;

// Driver-level Timer interface exposes laps and total elapsed:
type Timer = {
  elapsedLap: () => number;   // ms since last lap reset
  totalElapsed: () => number; // ms since Timer construction
};
```

## Proposed Rust form
```rust
use std::time::Instant;

pub struct Timer {
    start: Instant,
    lap_start: Instant,
}

impl Timer {
    pub fn new() -> Self {
        let now = Instant::now();
        Self { start: now, lap_start: now }
    }
    pub fn total_elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }
    pub fn elapsed_lap_ms(&self) -> f64 {
        self.lap_start.elapsed().as_secs_f64() * 1000.0
    }
    pub fn reset_lap(&mut self) { self.lap_start = Instant::now(); }
}
```

For raw duration measurements, prefer `Instant` over `SystemTime` (the latter is
subject to wall-clock adjustments; `Instant` is monotonic, matching
`performance.now()`'s contract).

For serialising durations to structured logs or metrics, keep them as `f64`
milliseconds to match the TS log format.

## Classification
Direct — 1:1 mapping, `performance.now()` → `Instant::now()` + `.elapsed()`.

## Caveats
- `performance.now()` is relative to an arbitrary origin on both sides of the
  `MessageChannel` (syncer vs pool thread), and the two origins differ. The TS
  code already handles this by sending *relative durations* (`poolToBeginMs`,
  `poolToCompleteMs`, etc.) rather than absolute timestamps. The Rust port must
  follow the same rule: `Instant` values are not comparable across threads
  either (they are monotonic *per process* but not meaningful if serialised).
- The precision of `Instant::now()` on Linux is typically 1 ns from
  `CLOCK_MONOTONIC`; `performance.now()` in Node is µs-precision. This does
  not affect correctness.

## Citation
- `std::time::Instant` — https://doc.rust-lang.org/std/time/struct.Instant.html
- Node `performance.now()` — https://nodejs.org/api/perf_hooks.html#performancenow
