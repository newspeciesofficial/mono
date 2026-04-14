//! Client View Record (CVR) types.
//!
//! Maps to `services/view-syncer/schema/types.ts`.
//! CVR tracks what each client has seen, enabling correct reconnection.

use crate::row_key::RowID;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A CVR version: combines state version with an optional config version.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CVRVersion {
    pub state_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_version: Option<u64>,
}

pub type NullableCVRVersion = Option<CVRVersion>;

/// A row record in the CVR: tracks which queries reference this row.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowRecord {
    pub patch_version: CVRVersion,
    pub id: RowID,
    pub row_version: String,
    /// query hash -> ref count. `None` = tombstone (row deleted).
    pub ref_counts: Option<HashMap<String, i64>>,
}

/// Per-client state for a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientQueryState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inactivated_at: Option<f64>,
    pub ttl: i64,
    pub version: CVRVersion,
}

/// A query record in the CVR.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRecord {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transformation_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transformation_version: Option<CVRVersion>,
    #[serde(default)]
    pub client_state: HashMap<String, ClientQueryState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_version: Option<CVRVersion>,
}

/// A client record in the CVR.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientRecord {
    pub id: String,
    pub desired_query_ids: Vec<String>,
}

/// CVR row patch operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum RowPatch {
    Put {
        id: RowID,
        #[serde(rename = "rowVersion")]
        row_version: String,
    },
    Del {
        id: RowID,
    },
}

/// CVR query patch operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryPatch {
    pub op: PatchOp,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PatchOp {
    Put,
    Del,
}
