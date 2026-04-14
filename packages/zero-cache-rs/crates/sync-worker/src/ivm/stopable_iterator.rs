//! Port of `packages/zql/src/ivm/stopable-iterator.ts`.
//!
//! TS:
//! ```ts
//! export class StoppableIterator<T> {
//!   #iterator: Iterator<T>;
//!   #stopped = false;
//!   constructor(iterator: Iterator<T>) { this.#iterator = iterator; }
//!   next() { if (this.#stopped) throw new Error('Iterator has been stopped'); return this.#iterator.next(); }
//!   stop() { this.#stopped = true; }
//! }
//! ```
//!
//! Used by operators (Take, Exists) that hand an iterator to downstream
//! consumers but need to invalidate it when their own state changes
//! (e.g. a push() arrives between fetch() and the consumer's next() call).
//!
//! Rust parity:
//!
//! - TS `next()` throws `new Error('Iterator has been stopped')`. The
//!   ported [`StoppableIterator::next`] returns a `Result<Option<T>,
//!   StopError>` so callers can distinguish stop-after-consumed from
//!   iterator exhaustion.
//! - `Iterator for &mut StoppableIterator<T>` is **not** implemented:
//!   `Iterator::next` cannot fail, so it would have to drop the stopped
//!   signal. Callers use the inherent `next()` method.

use thiserror::Error;

/// Error returned by [`StoppableIterator::next`] when the iterator has
/// been stopped. Matches TS `throw new Error('Iterator has been stopped')`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("Iterator has been stopped")]
pub struct StopError;

/// An iterator that can be explicitly stopped.
pub struct StoppableIterator<I: Iterator> {
    inner: I,
    stopped: bool,
}

impl<I: Iterator> StoppableIterator<I> {
    /// TS constructor.
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            stopped: false,
        }
    }

    /// Advance the iterator. Returns:
    /// - `Err(StopError)` if [`stop`](Self::stop) has been called.
    /// - `Ok(Some(item))` if the inner iterator yielded.
    /// - `Ok(None)` if the inner iterator is exhausted.
    pub fn next(&mut self) -> Result<Option<I::Item>, StopError> {
        if self.stopped {
            return Err(StopError);
        }
        Ok(self.inner.next())
    }

    /// TS `stop()`. Subsequent [`next`](Self::next) calls return
    /// `Err(StopError)`.
    pub fn stop(&mut self) {
        self.stopped = true;
    }

    /// Whether [`stop`](Self::stop) has been called. Not in TS; exposed
    /// for ergonomic callers that want to skip work they know will fail.
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }
}

#[cfg(test)]
mod tests {
    //! Branch-complete:
    //! - next() when not stopped and inner yields       → Ok(Some)
    //! - next() when not stopped and inner is exhausted → Ok(None)
    //! - next() when stopped                            → Err(StopError)
    //! - stop() transitions the flag
    //! - stop() is idempotent (TS `#stopped = true` is a write, not a toggle)
    //! - is_stopped() observes the flag in both states

    use super::*;

    #[test]
    fn next_when_not_stopped_yields_inner_items() {
        // branch: `stopped == false`, `inner.next()` → Some
        let mut it = StoppableIterator::new(vec![1, 2, 3].into_iter());
        assert_eq!(it.next(), Ok(Some(1)));
        assert_eq!(it.next(), Ok(Some(2)));
        assert_eq!(it.next(), Ok(Some(3)));
    }

    #[test]
    fn next_past_inner_exhaustion_returns_none_not_err() {
        // branch: `stopped == false`, `inner.next()` → None
        let mut it = StoppableIterator::new(std::iter::empty::<i32>());
        assert_eq!(it.next(), Ok(None));
        // Polling again still returns None, never StopError.
        assert_eq!(it.next(), Ok(None));
    }

    #[test]
    fn next_after_stop_returns_stop_error() {
        // branch: `stopped == true`
        let mut it = StoppableIterator::new(vec![1, 2].into_iter());
        it.stop();
        assert_eq!(it.next(), Err(StopError));
    }

    #[test]
    fn stop_mid_iteration_prevents_further_reads() {
        let mut it = StoppableIterator::new(vec![1, 2, 3].into_iter());
        assert_eq!(it.next(), Ok(Some(1)));
        it.stop();
        assert_eq!(it.next(), Err(StopError));
        // Even though the underlying iterator still has items, we can't see them.
    }

    #[test]
    fn stop_is_idempotent() {
        let mut it = StoppableIterator::new(vec![1].into_iter());
        it.stop();
        it.stop();
        assert!(it.is_stopped());
        assert_eq!(it.next(), Err(StopError));
    }

    #[test]
    fn is_stopped_reflects_flag() {
        let mut it = StoppableIterator::new(std::iter::empty::<()>());
        assert!(!it.is_stopped());
        it.stop();
        assert!(it.is_stopped());
    }

    #[test]
    fn stop_error_display_matches_ts_message() {
        // TS throws `new Error('Iterator has been stopped')`.
        assert_eq!(StopError.to_string(), "Iterator has been stopped");
    }
}
