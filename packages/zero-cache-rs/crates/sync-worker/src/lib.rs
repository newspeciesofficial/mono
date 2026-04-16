//! Port of the sync worker and its IVM operator graph.
//!
//! Mirrors `packages/zero-cache/src/services/view-syncer/` and
//! `packages/zql/src/ivm/` from the stock TS codebase. Each Rust module
//! is a function-for-function port of the TS file with the same name.
//!
//! Structural rules:
//!
//! - File names and module structure mirror TS 1:1 so diffing is easy.
//! - No "shared helper" crate — each ported TS file lives inside this
//!   crate. The framework's rule is that each shadowed TS file gets its
//!   Rust source in the crate that shadows it; we follow that here too.
//! - Every public item has a docstring that names the TS origin.
//!
//! Shadow exposure happens in `crates/shadow-ffi/src/exports/sync_worker/`.
//! This crate stays pure Rust; it doesn't know about napi.

pub mod ivm;
pub mod ivm_v2;
pub mod view_syncer_v2;
