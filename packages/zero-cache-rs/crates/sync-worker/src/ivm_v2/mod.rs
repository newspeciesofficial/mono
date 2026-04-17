//! Parallel IVM module using the single-core trait redesign.
//!
//! Rebuilds the operator graph on top of a simpler, TS-closer trait
//! shape: plain `&mut self`, returns `impl Iterator<Item = Yield> + '_`
//! (or a named iterator struct for stateful operators), no `Arc`, no
//! `Mutex`, no back-edge adapter structs. The `Send` bound keeps every
//! operator compatible with future rayon-at-top parallelism without
//! changing call sites.
//!
//! ## Status — iterative port
//!
//! - [x] `operator.rs` — new traits.
//! - [x] `change.rs` — re-exports.
//! - [x] `take.rs` — push-path only (no fetch, no partition keys yet).
//! - [ ] Take fetch / partition / max-bound.
//! - [ ] Filter, FilterStart, FilterEnd.
//! - [ ] Skip, Exists.
//! - [ ] Join, FlippedJoin.
//! - [ ] FanOut, FanIn, UnionFanOut, UnionFanIn.
//! - [ ] `table_source.rs` channel-based `query_rows`.
//!
//! When everything in this module has parity with `ivm/`, the old
//! module is removed and this one is renamed.

pub mod batching;
pub mod change;
pub mod exists;
pub mod exists_t;
pub mod or_exists_t;
pub mod fan_in;
pub mod fan_out;
pub mod filter;
pub mod filter_t;
pub mod flipped_join;
pub mod join;
pub mod join_t;
pub mod operator;
pub mod skip;
pub mod skip_t;
pub mod source;
pub mod take;
pub mod take_t;
pub mod union_fan_in;
pub mod union_fan_out;
