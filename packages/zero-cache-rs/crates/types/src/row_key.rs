//! Row key types.
//!
//! Maps to `types/row-key.ts`.

use crate::value::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A row key: the subset of a [`Row`] that forms the primary key values.
/// Uses `BTreeMap` for deterministic ordering (matching TS `normalizedKeyOrder`).
pub type RowKey = BTreeMap<String, serde_json::Value>;

/// Row ID including schema and table context.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowID {
    pub schema: String,
    pub table: String,
    pub row_key: RowKey,
}

/// Extracts the row key from a full row given primary key column names.
pub fn extract_row_key(row: &Row, pk_columns: &[String]) -> RowKey {
    pk_columns
        .iter()
        .filter_map(|col| {
            row.get(col)
                .and_then(|v| v.as_ref())
                .map(|v| (col.clone(), v.clone()))
        })
        .collect()
}
