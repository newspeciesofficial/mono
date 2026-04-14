# New pattern: `node:v8` `getHeapStatistics()` as a back-pressure budget

## Category

B (Node stdlib)

## Where used

- `packages/zero-cache/src/services/change-streamer/storer.ts:3` (import)
- `packages/zero-cache/src/services/change-streamer/storer.ts:144-154`
  (budget computation)
- `packages/zero-cache/src/services/change-streamer/storer.ts:119,251-325`
  (budget enforcement via `#approximateQueuedBytes` and `readyForMore`).

## TS form

```ts
import {getHeapStatistics} from 'node:v8';

const heapStats = getHeapStatistics();
this.#backPressureThresholdBytes =
  (heapStats.heap_size_limit - heapStats.used_heap_size) *
  backPressureLimitHeapProportion;

// Later, per change written:
this.#approximateQueuedBytes += json.length;
if (this.#approximateQueuedBytes > this.#backPressureThresholdBytes) {
  this.#readyForMore = resolver(); // upstream will await this
}
```

## Proposed Rust form

Rust processes do not expose a bounded V8-style heap. Two equivalent shapes:

```rust
// Option A: a fixed MB budget from config.
pub struct StorerConfig {
    pub back_pressure_budget_bytes: usize, // e.g. 256 * 1024 * 1024
}

let threshold = cfg.back_pressure_budget_bytes;

// Option B: derive from the cgroup / system memory limit via `sysinfo` or
// `cgroups-rs`, then multiply by a proportion.
let total = sysinfo::System::new_all().total_memory() as usize; // bytes
let threshold = (total as f64 * cfg.back_pressure_limit_proportion) as usize;

// Enforcement uses a bounded mpsc channel (preferred) OR an AtomicUsize
// counter + Notify:
let queued = AtomicUsize::new(0);
let ready_for_more = tokio::sync::Notify::new();

fn store(&self, json_len: usize) -> Option<impl Future<Output=()> + '_> {
    let cur = self.queued.fetch_add(json_len, Ordering::Relaxed) + json_len;
    if cur > self.threshold {
        Some(self.ready_for_more.notified())
    } else {
        None
    }
}
```

## Classification

Redesign. The TS code reads V8-specific runtime state at construction time;
Rust has no equivalent because heap limits are not bounded by the runtime.
The replacement is a static config knob (Option A) or a cgroup-derived
budget (Option B). Enforcement should additionally switch to a bounded
`tokio::sync::mpsc::channel(cap)` if message count (rather than bytes) is an
acceptable proxy, which is the idiomatic Rust form of C1.

## Caveats

- The TS path samples `heap_size_limit - used_heap_size` exactly once at
  construction. It does not dynamically grow or shrink the budget. A static
  Rust config preserves this semantics faithfully.
- Rust `mimalloc` (already a dep per Part 2) does not expose a heap-limit
  concept; a hard cap must come from cgroups/k8s memory limits or an explicit
  CLI flag.
- The `proportion` knob (`--back-pressure-limit-heap-proportion`) is a
  user-visible config; Rust port should keep the same CLI name for operator
  parity, even if the underlying derivation differs.

## Citation

- Node.js `v8.getHeapStatistics()` docs:
  <https://nodejs.org/api/v8.html#v8getheapstatistics> — documents
  `heap_size_limit` and `used_heap_size` semantics.
- `tokio::sync::mpsc` bounded channel for back-pressure:
  <https://docs.rs/tokio/latest/tokio/sync/mpsc/fn.channel.html>.
- `sysinfo` crate (system memory):
  <https://docs.rs/sysinfo/latest/sysinfo/struct.System.html#method.total_memory>.
