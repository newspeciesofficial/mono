//! Shadow-FFI: Rust functions exposed to TS zero-cache for differential testing.
//!
//! Per-ClientGroup state lives behind opaque [`napi::bindgen_prelude::External`]
//! handles. TS holds one handle per ClientGroup; Rust never sees a registry.
//! See [`handle`] for the contract.

#![allow(unsafe_code)] // napi-rs internals require unsafe; scoped to this crate.

#[macro_use]
extern crate napi_derive;

pub mod exports;
pub mod handle;
