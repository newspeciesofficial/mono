//! Pool thread protocol types.
//!
//! Maps to `workers/pool-protocol.ts`.
//! In the Rust implementation, these types are used directly (no serialization
//! boundary) since all tasks share the same address space. The types are kept
//! for structural compatibility and for the trait interfaces.

use crate::value::Row;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Snapshot of a PipelineDriver's cacheable state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DriverState {
    pub version: String,
    pub replica_version: String,
    /// Opaque permissions blob (JSON). `None` = no permissions loaded.
    pub permissions: Option<serde_json::Value>,
    pub total_hydration_time_ms: f64,
    /// queryID -> transformationHash
    pub queries: HashMap<String, String>,
}

/// A single row change emitted by the IVM pipeline during advance or hydration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RowChange {
    Add {
        #[serde(rename = "queryID")]
        query_id: String,
        table: String,
        #[serde(rename = "rowKey")]
        row_key: Row,
        row: Row,
    },
    Remove {
        #[serde(rename = "queryID")]
        query_id: String,
        table: String,
        #[serde(rename = "rowKey")]
        row_key: Row,
    },
    Edit {
        #[serde(rename = "queryID")]
        query_id: String,
        table: String,
        #[serde(rename = "rowKey")]
        row_key: Row,
        row: Row,
    },
}

/// A batch of row changes, reference-counted for zero-copy sharing between tasks.
pub type RowChangeBatch = Arc<Vec<RowChange>>;

/// Timing diagnostics for a single advance operation.
#[derive(Debug, Clone, Default)]
pub struct AdvanceTimings {
    pub snapshot_ms: f64,
    pub iterate_ms: f64,
    pub total_ms: f64,
    pub batch_count: u32,
    pub total_rows: u32,
    pub did_reset: bool,
}

/// Result of an advance operation.
pub struct AdvanceResult {
    pub version: String,
    pub num_changes: u32,
    pub timings: AdvanceTimings,
    pub changes: Vec<RowChange>,
    pub state: DriverState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::json_string;

    #[test]
    fn row_change_serde() {
        let mut row_key = Row::new();
        row_key.insert("id".into(), Some(json_string("abc")));

        let mut row = Row::new();
        row.insert("id".into(), Some(json_string("abc")));
        row.insert("name".into(), Some(json_string("test")));

        let change = RowChange::Add {
            query_id: "q1".into(),
            table: "user".into(),
            row_key,
            row,
        };

        let json = serde_json::to_string(&change).unwrap();
        assert!(json.contains(r#""type":"add""#));
        assert!(json.contains(r#""queryID":"q1""#));

        let parsed: RowChange = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, RowChange::Add { .. }));
    }
}
