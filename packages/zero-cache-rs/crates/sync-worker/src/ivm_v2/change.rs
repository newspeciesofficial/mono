//! Re-exports of the `Change` types from the legacy `ivm` module.
//!
//! The row-change value shape is identical; only operator traits
//! change. Re-exporting keeps `ivm_v2::Change` a single import site.

pub use crate::ivm::change::{
    AddChange, Change, ChangeType, ChildChange, ChildSpec, EditChange, RemoveChange,
};
pub use crate::ivm::data::{Node, NodeOrYield};
pub use crate::ivm::schema::SourceSchema;
pub use crate::ivm::source::Yield;
