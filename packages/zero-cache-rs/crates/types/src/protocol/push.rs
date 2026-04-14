//! Push/mutation types.

use crate::primary_key::{PrimaryKey, PrimaryKeyValueRecord};
use crate::value::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum CRUDOp {
    Insert {
        #[serde(rename = "tableName")]
        table_name: String,
        #[serde(rename = "primaryKey")]
        primary_key: PrimaryKey,
        value: Row,
    },
    Upsert {
        #[serde(rename = "tableName")]
        table_name: String,
        #[serde(rename = "primaryKey")]
        primary_key: PrimaryKey,
        value: Row,
    },
    Update {
        #[serde(rename = "tableName")]
        table_name: String,
        #[serde(rename = "primaryKey")]
        primary_key: PrimaryKey,
        value: Row,
    },
    Delete {
        #[serde(rename = "tableName")]
        table_name: String,
        #[serde(rename = "primaryKey")]
        primary_key: PrimaryKey,
        value: PrimaryKeyValueRecord,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Mutation {
    Crud {
        id: u64,
        #[serde(rename = "clientID")]
        client_id: String,
        name: String,
        args: Vec<CRUDMutationArg>,
        timestamp: f64,
    },
    Custom {
        id: u64,
        #[serde(rename = "clientID")]
        client_id: String,
        name: String,
        args: Vec<serde_json::Value>,
        timestamp: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CRUDMutationArg {
    pub ops: Vec<CRUDOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushBody {
    #[serde(alias = "clientGroupID")]
    pub client_group_id: String,
    pub mutations: Vec<Mutation>,
    pub push_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,
    pub timestamp: f64,
    #[serde(alias = "requestID")]
    pub request_id: String,
}

/// Maps to `zero-protocol/src/mutation-id.ts` (`MutationID`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationID {
    pub id: u64,
    #[serde(rename = "clientID")]
    pub client_id: String,
}

/// `MutationOk` — no `error` field.
/// Note: this uses an untagged union with `MutationError` in `MutationResult`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MutationOk {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// `MutationError` — always has `error` field. App-level (`error: 'app'`) or
/// zero-level (`error: 'oooMutation'` | `'alreadyProcessed'`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationError {
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// `MutationResult = MutationOk | MutationError` (untagged).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MutationResult {
    Err(MutationError),
    Ok(MutationOk),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationResponse {
    pub id: MutationID,
    pub result: MutationResult,
}

/// `PushOk` — has `mutations` field. Matches `pushOkSchema`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushOk {
    pub mutations: Vec<MutationResponse>,
}

/// Deprecated legacy push error body: `{error: 'http'|'zeroPusher'|...}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushLegacyError {
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    #[serde(
        default,
        rename = "mutationIDs",
        skip_serializing_if = "Option::is_none"
    )]
    pub mutation_ids: Option<Vec<MutationID>>,
}

/// `PushResponse = PushOk | PushLegacyError | PushFailedBody` (untagged union).
///
/// Order matters: `PushFailedBody` is discriminated by `"kind"`, `PushLegacyError`
/// by `"error"`, and `PushOk` by `"mutations"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PushResponse {
    /// The new-style `PushFailedBody` with `kind: "pushFailed"`.
    Failed(PushFailedBody),
    /// Legacy push error body discriminated by `error` field.
    LegacyError(PushLegacyError),
    /// Successful push response with mutation results.
    Ok(PushOk),
}

/// `PushFailedBody` from `zero-protocol/src/error.ts`.
/// Minimum shape needed by the translator — keeps the fields used at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushFailedBody {
    pub kind: PushFailedKind,
    pub origin: crate::error::ErrorOrigin,
    pub reason: crate::error::ErrorReason,
    pub message: String,
    #[serde(rename = "mutationIDs")]
    pub mutation_ids: Vec<MutationID>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "bodyPreview"
    )]
    pub body_preview: Option<String>,
}

/// Literal `"pushFailed"` kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PushFailedKind {
    PushFailed,
}

/// TS `CLEANUP_RESULTS_MUTATION_NAME` — keep in sync with zero-protocol.
pub const CLEANUP_RESULTS_MUTATION_NAME: &str = "_zero_cleanupResults";

// ---------------------------------------------------------------------------
// Tests ported from zero-protocol/src/push.test.ts.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #[test]
    #[ignore = "needs TS `mapCRUD` + `ClientToServerNameMapper` ported from \
                packages/zero-protocol/src/push.ts — tracked in \
                docs/ts-vs-rs-comparison.md row 1 (`Protocol & types`)"]
    fn map_crud_rewrites_table_and_column_names() {
        // TS test: `map names`. Takes a CRUD payload and a clientToServer
        // mapper and asserts:
        // - tableName is rewritten (`issue`→`issues`, `comment`→`comments`)
        // - column names in value are rewritten (`ownerId`→`owner_id`)
        // - primaryKey array is rewritten (`id`→`comment_id`)
        // - tables with no `.from(...)` mapping stay unchanged
    }
}
