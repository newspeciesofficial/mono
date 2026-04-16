//! IVM (Incremental View Maintenance) operator graph.
//!
//! Mirrors `packages/zql/src/ivm/`. One Rust module per TS file. The
//! order of the submodules here roughly reflects the dependency order —
//! later modules depend on earlier ones.

pub mod change;
pub mod constraint;
pub mod data;
pub mod exists;
pub mod fan_in;
pub mod fan_out;
pub mod filter;
pub mod filter_gen;
pub mod filter_operators;
pub mod filter_push;
pub mod flipped_join;
pub mod join;
pub mod join_utils;
pub mod maybe_split_and_push_edit_change;
pub mod memory_storage;
pub mod operator;
pub mod push_accumulated;
pub mod schema;
pub mod skip;
pub mod source;
pub mod stopable_iterator;
pub mod stream;
pub mod take;
pub mod union_fan_in;
pub mod union_fan_out;
