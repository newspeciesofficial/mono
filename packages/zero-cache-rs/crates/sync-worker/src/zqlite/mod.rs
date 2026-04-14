//! Port of `packages/zqlite/src/` — SQLite-backed pieces of the IVM graph.
//!
//! Currently contains [`table_source`], the production
//! [`crate::ivm::source::Source`] implementation, and
//! [`database_storage`], the production
//! [`crate::ivm::operator::Storage`] implementation. Future layers will
//! add `sqlite_cost_model` here.

pub mod database_storage;
pub mod internal;
pub mod query_builder;
pub mod resolve_scalar_subqueries;
pub mod sqlite_cost_model;
pub mod sqlite_stat_fanout;
pub mod table_source;
