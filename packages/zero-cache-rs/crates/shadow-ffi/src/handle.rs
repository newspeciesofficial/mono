//! Opaque per-ClientGroup state handles.
//!
//! TS owns one handle per ClientGroup; Rust state behind the handle is
//! reachable only through that pointer. There is deliberately no global
//! `HashMap<ClientGroupID, State>` — that would re-introduce the very
//! cross-group leakage the handle pattern exists to prevent.
//!
//! ## Lifetime contract
//!
//! 1. TS calls `create_*_handle(client_group_id)` when the per-group
//!    structure on the TS side (e.g. `ViewSyncerService`) is constructed.
//! 2. Every shadow call routes through the handle; Rust never re-derives
//!    "which ClientGroup does this belong to" from the request payload.
//! 3. When the TS object is destroyed, it explicitly drops the handle (or
//!    JS GC drops it). The Rust state is freed deterministically.
//!
//! ## Why `External<Arc<Mutex<T>>>`?
//!
//! - `External<T>` is napi-rs's opaque pointer; JS sees it as a black box.
//! - `Arc` lets nested helpers temporarily hold a reference without owning.
//! - `Mutex` provides the interior mutability the FFI boundary requires
//!    (napi-rs hands us `&External`, not `&mut External`). Contention is
//!    per-ClientGroup, never cross-group.
//!
//! Concrete `ClientGroupState` types live alongside the worker that owns
//! them (e.g. a future `pipeline_handle.rs`). This module provides only
//! the shared scaffolding.

use napi::bindgen_prelude::External;
use std::sync::{Arc, Mutex};

/// Convenience alias: opaque handle to per-ClientGroup state.
///
/// Shadow exports take `&Handle<MyState>` and call `.lock()` to mutate.
pub type Handle<T> = External<Arc<Mutex<T>>>;

/// Wrap fresh state in a handle ready to be returned to TS.
///
/// The handle's `Drop` runs when JS GC's it (or TS calls a `destroy_*`
/// export), at which point the `Arc` count falls to zero and the underlying
/// `T` is dropped on whichever thread happens to release the last reference.
pub fn make_handle<T>(state: T) -> Handle<T> {
    External::new(Arc::new(Mutex::new(state)))
}

/// Borrow a handle's state for the duration of `f`.
///
/// `Mutex` poisoning is **recovered** here rather than treated as fatal.
/// Rationale: a panic inside a shadow FFI call (e.g. a replay mismatch
/// triggered by a trace the Rust port didn't fully anticipate) will
/// poison the mutex. If we propagate that forever, every subsequent
/// shadow call on the same handle also fails — the shadow quality
/// signal is lost for the remainder of the process lifetime, hiding
/// downstream regressions behind a single noisy poison.
///
/// Shadow state is **advisory / diagnostic**: production already
/// committed to the TS-authoritative result before the shadow path
/// runs. Recovering from poison lets the shadow keep reporting
/// divergences for the next call; each entrypoint is wrapped in
/// `catch_unwind` so any panic inside `f` is caught and surfaced as a
/// typed `napi::Error` by the caller.
///
/// If the inner `T` was in the middle of mutating when the poisoning
/// panic fired, it may be in an inconsistent state. That is acceptable:
/// the next shadow call either succeeds (inconsistency was transient)
/// or reports a fresh divergence. Either way, it no longer masks the
/// signal.
pub fn with_handle<T, R>(h: &Handle<T>, f: impl FnOnce(&mut T) -> R) -> R {
    let arc = h.as_ref().clone();
    let mut guard = match arc.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            // Log once per recovery so the first cause panic doesn't
            // go entirely silent, but don't fail the call.
            eprintln!(
                "[shadow-ffi] recovered from poisoned shadow handle mutex \
                 (downstream shadow divergences will keep reporting)"
            );
            poisoned.into_inner()
        }
    };
    f(&mut guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two handles → two independent state instances, no sharing.
    #[test]
    fn handles_are_isolated() {
        #[derive(Default)]
        struct Counter(usize);

        let a: Handle<Counter> = make_handle(Counter::default());
        let b: Handle<Counter> = make_handle(Counter::default());

        with_handle(&a, |s| s.0 += 1);
        with_handle(&a, |s| s.0 += 1);
        with_handle(&b, |s| s.0 += 100);

        assert_eq!(with_handle(&a, |s| s.0), 2);
        assert_eq!(with_handle(&b, |s| s.0), 100);
    }
}
