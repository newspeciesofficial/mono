//! `#[napi]`-annotated exports — the surface TS calls.
//!
//! Production napi plumbing for `PipelineV2`. Stateful functions take a
//! [`crate::handle::Handle`] as the first param and never reach for a
//! global registry.

pub mod pipeline_v2;
pub mod snapshotter;
