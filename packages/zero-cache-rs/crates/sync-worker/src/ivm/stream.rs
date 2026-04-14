//! Port of `packages/zql/src/ivm/stream.ts`.
//!
//! TS:
//! ```ts
//! export type Stream<T> = Iterable<T>;
//! ```
//!
//! In Rust, `Stream<T>` is `Box<dyn Iterator<Item = T> + 'a>`. Callers
//! commonly take a `Stream<impl Iterator<Item = T>>` on hot paths to avoid
//! the dyn dispatch where the concrete iterator is known. The type alias
//! below documents the shape for parity with the TS codebase.

/// TS: `Stream<T> = Iterable<T>` — a forward-only lazy iterator.
pub type Stream<'a, T> = Box<dyn Iterator<Item = T> + 'a>;
