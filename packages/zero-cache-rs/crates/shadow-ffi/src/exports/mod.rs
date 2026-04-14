//! `#[napi]`-annotated exports — the surface TS calls.
//!
//! Each shadowed function lives in its own submodule. Conventions:
//!
//! - File name mirrors the TS module being shadowed (e.g. `lsn.rs` ↔
//!   `services/change-source/pg/lsn.ts`).
//! - The `#[napi]` function name is `ts_module_function`, lower_snake_case.
//! - Stateful functions take a [`crate::handle::Handle`] as the first
//!   param and never reach for a global registry.
//!
//! The TS wrapper at the call site (`packages/zero-cache/src/shadow/`) is
//! responsible for invoking the TS implementation, invoking the matching
//! Rust export, and diffing the two outputs.

pub mod ivm_constraint;
pub mod ivm_data;
pub mod lsn;
pub mod pipeline_driver;
pub mod table_source;
