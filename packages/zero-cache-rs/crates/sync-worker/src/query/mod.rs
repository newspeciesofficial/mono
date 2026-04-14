//! Port of `packages/zql/src/query/` utility modules that planner/builder depend on.
//!
//! Intentionally narrow — only the files the planner subsystem (or Layer 9
//! builder) consume. `query-impl.ts`, `named.ts`, etc. live far above this
//! layer and are not ported here.

pub mod complete_ordering;
pub mod escape_like;
