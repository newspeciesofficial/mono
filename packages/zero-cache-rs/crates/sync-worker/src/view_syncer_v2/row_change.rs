//! Row-level changes emitted to callers (TS poke producers).
//!
//! These mirror `pipeline_driver::RowChange` / `RowAdd` / `RowRemove` /
//! `RowEdit` but re-exported from the v2 driver so callers that choose
//! v2 don't import from v1's module.

use zero_cache_types::value::Row;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowAdd {
    pub query_id: String,
    pub table: String,
    pub row_key: Row,
    pub row: Row,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRemove {
    pub query_id: String,
    pub table: String,
    pub row_key: Row,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowEdit {
    pub query_id: String,
    pub table: String,
    pub row_key: Row,
    pub row: Row,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowChange {
    Add(RowAdd),
    Remove(RowRemove),
    Edit(RowEdit),
}

impl RowChange {
    pub fn query_id(&self) -> &str {
        match self {
            Self::Add(r) => &r.query_id,
            Self::Remove(r) => &r.query_id,
            Self::Edit(r) => &r.query_id,
        }
    }
    pub fn table(&self) -> &str {
        match self {
            Self::Add(r) => &r.table,
            Self::Remove(r) => &r.table,
            Self::Edit(r) => &r.table,
        }
    }
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::Add(_) => "add",
            Self::Remove(_) => "remove",
            Self::Edit(_) => "edit",
        }
    }
}
