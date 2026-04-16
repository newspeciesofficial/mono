//! Pipeline driver built on ivm_v2.
//!
//! Scope of this first cut (task #124 first milestone):
//!
//! - Build simple chains: TableSource → Filter → Take from a narrow input
//!   spec. No AST translation — callers pass operator-level parameters.
//! - Hydrate: pull `limit` rows through the chain, emit them as
//!   `RowChange::Add` values.
//! - Advance: accept a `SourceChange`, push through the chain, emit the
//!   resulting `RowChange`s.
//!
//! Intentionally out of scope for this cut:
//! - AST → operator building (handled by `builder/builder.rs` for v1; v2
//!   builder is a separate follow-up).
//! - Join / hierarchical output (need full `processParentNode` port).
//! - CVR persistence (separate subsystem).
//! - Cost model, scalar-subquery resolution, companion queries.
//! - Partitioned Take.

pub mod ast_builder;
pub mod driver;
pub mod pipeline;
pub mod row_change;
pub mod snapshotter;
pub mod sqlite_source;
