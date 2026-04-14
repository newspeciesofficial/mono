//! Port of `packages/zql/src/builder/`.
//!
//! The builder sub-tree wires an AST into an IVM operator graph. Layer 9
//! contains:
//!
//! - [`like`]    — LIKE / ILIKE predicate compilation.
//! - [`filter`]  — simple-condition predicate + correlated-subquery
//!                 stripping (`transformFilters` port).
//! - [`builder`] — the main entrypoint (`buildPipeline`).

pub mod builder;
pub mod filter;
pub mod like;
