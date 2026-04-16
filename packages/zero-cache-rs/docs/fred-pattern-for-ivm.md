# The `fred` Pattern: Channel-Returning Methods for IVM

> Notes from reading [fred](https://docs.rs/fred/latest/fred/) (Redis client,
> ~15k LOC of Tokio-based Rust) — extracting the patterns that apply to our
> IVM port. See `ivm-port-open-questions.md` for the problems this targets.

---

## The core pattern in one sentence

**Methods that produce a sequence of values return `Receiver<T>` (or
`impl Stream<Item=T>`), not `Box<dyn Iterator>`. The producer runs in its
own task and owns the sender; the consumer owns the receiver; either side
dropping breaks the channel and cleans up automatically.**

---

## The channel abstraction (fred's `runtime/_tokio.rs`)

fred wraps tokio's mpsc with a small runtime-agnostic shim:

```rust
// src/runtime/_tokio.rs:127-149
pub fn channel<T: Send + 'static>(size: usize) -> (Sender<T>, Receiver<T>) {
    if size == 0 {
        let (tx, rx) = unbounded_channel();
        (Sender { tx: SenderKind::Unbounded(tx) },
         Receiver { rx: ReceiverKind::Unbounded(rx) })
    } else {
        let (tx, rx) = bounded_channel(size);
        (Sender { tx: SenderKind::Bounded(tx) },
         Receiver { rx: ReceiverKind::Bounded(rx) })
    }
}
```

`size == 0` means unbounded; any positive size is bounded. Identical API;
the enum switches flavors internally. This lets callers pick backpressure
vs. latency trade-off per call site.

The `Receiver<T>` has:

```rust
pub fn into_stream(self) -> impl Stream<Item = T> + 'static {
    match self.rx {
        ReceiverKind::Bounded(rx)   => ReceiverStream::new(rx).boxed(),
        ReceiverKind::Unbounded(rx) => UnboundedReceiverStream::new(rx).boxed(),
    }
}
```

`tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream}` adapt
mpsc receivers into the `futures::Stream` trait. This is the "receiver
becomes a stream" move.

---

## Pattern 1: Scan with Drop-driven continuation

fred's `SCAN` (which Redis paginates with a cursor) returns a
`Stream<Item = Result<ScanResult, Error>>`. Each `ScanResult` is **one
page**. Dropping it fetches the next page unless the caller cancels.

```rust
// src/commands/impls/scan.rs (paraphrased)
pub fn scan(inner, pattern, count, type) -> impl Stream<Item = Result<ScanResult, Error>> {
    let (tx, rx) = channel(0);
    let response = ResponseKind::KeyScan(KeyScanInner { args, tx: tx.clone(), … });
    let command = (CommandKind::Scan, Vec::new(), response).into();
    let _ = interfaces::default_send_command(inner, command);
    rx.into_stream()
}
```

Producer side: the router task receives the command, executes one page,
builds a `ScanResult { scan_state, can_continue, … }`, and sends it on
`tx`.

Consumer side: receives one `ScanResult`, reads the page, then **drops**
the `ScanResult`. The Drop impl fires the next page:

```rust
// src/types/scan.rs:102-108
impl Drop for ScanResult {
    fn drop(&mut self) {
        if self.can_continue {
            next_key_page(&self.inner, &mut self.scan_state);
        }
    }
}
```

If the consumer calls `ScanResult::cancel(self)` instead of letting it
drop, cancel consumes `self.scan_state` via `take()` without setting up
the next page — the stream naturally ends.

This is a very clean idiom: **"drop = demand next page, cancel = stop"**.
No explicit `.next()` call needed; the consumer's `for-await` loop drives
the continuation.

---

## Pattern 2: Cancellation by dropping the Receiver

Every fred function that returns a `Stream` sends results through a tokio
mpsc channel. If the consumer drops the stream (or the iterator over it):

1. The `Receiver` drops.
2. The next `tx.send(...)` (or `tx.try_send(...)`) on the producer side
   returns `Err(SendError)`.
3. The producer's code path always propagates this into "stop and clean
   up".

No cancellation token needed. The channel's own disconnect semantics is
the cancellation signal.

This is **exactly** the pattern we just proved in `/tmp/sqlite-stream/`:
consumer drops → producer's send errors → producer returns → thread joins.

---

## Pattern 3: Shared mutable state behind `Arc<Inner>` + `parking_lot::Mutex`

fred's `RedisClient` is a thin wrapper around `Arc<ClientInner>`. Every
operator/method on the client does `self.inner()` to access shared state.
Inside `ClientInner`:

```rust
// runtime/_tokio.rs:163-164
pub type Mutex<T>  = parking_lot::Mutex<T>;
pub type RwLock<T> = parking_lot::RwLock<T>;
```

Two things to note:

- **`parking_lot` — not `std::sync`**. Faster, no poisoning, `.lock()`
  returns the guard directly (no `Result`).
- **The Mutex is on state**, not on the whole struct. Shared config is
  behind `Arc<Config>`; mutable counters are behind `Arc<RwLock<…>>`; the
  router task has its own channel-backed state. No single "lock the whole
  client" object.

For our IVM: each operator keeps its mutable bits (`bound`, `cache`,
`in_push`) behind `parking_lot::Mutex`, and uses `Arc<Self>` capture for
closures. No global operator-level lock.

---

## Pattern 4: Broadcast for fan-out to many consumers

For pubsub, where one incoming message fans out to N subscribed consumers,
fred uses `tokio::sync::broadcast`:

```rust
// clients/pubsub.rs (paraphrased)
let mut message_rx = subscriber.message_rx();   // broadcast::Receiver
while let Ok(message) = message_rx.recv().await {
    println!("Received: {:?}", message);
}
```

`broadcast::Sender<T>` requires `T: Clone`. Each subscriber gets its own
`Receiver`; producer calls `tx.send(msg)` once and it's delivered to all.

For IVM FanOut: **if `Change` were cheap to clone**, broadcast is the
natural shape. Since `Change` owns nested data, we'd need to `Arc<Change>`
first, then broadcast.

---

## Pattern 5: Oneshot for request/response

For non-streaming commands (GET, SET), fred uses `tokio::sync::oneshot`:
the client `.send()`s the command with a oneshot `Sender<Response>`
attached; the router fills it when the response arrives; the client
`.await`s the oneshot `Receiver<Response>`.

For IVM: not directly useful for operators (we don't do request/response),
but useful if we ever add "ask this operator a synchronous question" RPCs.

---

## Mapping fred's patterns to our IVM trait shape

**Current:**

```rust
pub trait Output: Send + Sync {
    fn push<'a>(
        &'a mut self,
        change: Change,
        pusher: &dyn InputBase,
    ) -> Stream<'a, Yield>;   // Box<dyn Iterator + 'a> borrowing self
}
```

**fred-style redesign:**

```rust
pub trait Output: Send + Sync {
    fn push(
        self: Arc<Self>,
        change: Change,
        pusher: Arc<dyn InputBase>,
    ) -> Receiver<Yield>;     // owned, 'static, no borrow of self
}
```

Everything else follows:

- **No `&'a mut self`** → no lock held across consumption.
- **`Receiver<Yield>`** → consumer decides pacing, can break early, drop
  ends production.
- **`Arc<Self>`** → closures inside producer thread capture cleanly.
- **Bounded channel** → backpressure built in.
- **Drop guards move into the producer closure** → fires when producer
  exits (via consumer disconnect or natural end), solving our "try/finally
  must outlive the stream" problem.

---

## What to reuse and what to adapt for IVM

| fred | IVM | Reason |
|---|---|---|
| `tokio::sync::mpsc` | `crossbeam::channel` | We're inside `spawn_blocking`, not async. crossbeam is sync-native and already in our workspace. |
| `impl Stream<Item=T>` (futures) | `impl Iterator<Item=T>` (crossbeam `rx.iter()`) | Sync IVM model. If we ever want to yield into the async layer, wrap in `tokio_stream::wrappers::ReceiverStream`. |
| `parking_lot::Mutex` | Same | Drop-in faster replacement for `std::sync::Mutex`. Add to workspace. |
| `tokio::spawn` (tasks) | `std::thread::spawn` (or rayon worker) | Producer lives in a real OS thread; consumer pulls from its channel. |
| `Drop for ScanResult` (continuation) | `Drop for GenHandle` (cleanup) | Same pattern: Drop runs when consumer abandons. |
| `broadcast` for pubsub | `Arc<Change>` + `tx.clone()` per downstream | FanOut: clone tx per branch, or use broadcast if Change is shareable. |

---

## How to use this document

1. Read `ivm-port-open-questions.md` for the 10 problems.
2. Use the patterns above as a vocabulary — when a problem says "we can't
   hold a stream across `&'a mut self`", the answer is "return
   `Receiver<T>`, producer owns `Sender<T>`".
3. Run the POC at `/tmp/sqlite-stream/` to see Pattern 2 (cancellation by
   drop) working on real SQLite.
4. Run the POC at
   `packages/zero-cache-rs/crates/sync-worker/src/ivm/filter_gen.rs` for
   the genawaiter comparison.

See also:
- fred source: `/tmp/fred-study/` (cloned from github.com/aembke/fred.rs)
- Key fred files: `src/runtime/_tokio.rs`, `src/types/scan.rs`,
  `src/commands/impls/scan.rs`, `src/clients/pubsub.rs`

Sources:
- [fred on docs.rs](https://docs.rs/fred/latest/fred/)
- [fred on GitHub](https://github.com/aembke/fred.rs)
