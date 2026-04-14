//! Poke message types (server → client data sync).

use super::queries_patch::QueriesPatch;
use super::row_patch::RowPatchOp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A mutation ID, identifying a mutation within a client. Maps to TS
/// `zero-protocol/src/mutation-id.ts#mutationIDSchema`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MutationID {
    pub id: u64,
    #[serde(rename = "clientID")]
    pub client_id: String,
}

/// The result of a mutation. Maps to TS
/// `zero-protocol/src/push.ts#mutationResultSchema`. Intentionally modelled as
/// `serde_json::Value` — this passes through unchanged on the wire; type
/// discrimination (`MutationOk` vs `MutationError`) is done on the client.
pub type MutationResult = serde_json::Value;

/// A mutation response: `{id: MutationID, result: MutationResult}`. Maps to TS
/// `zero-protocol/src/push.ts#mutationResponseSchema`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MutationResponse {
    pub id: MutationID,
    pub result: MutationResult,
}

/// A single mutations-patch operation. Maps to TS
/// `zero-protocol/src/mutations-patch.ts`. Only `put` carries a full mutation
/// response; `del` carries just the id (resolve/release on client).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum MutationPatchOp {
    Put { mutation: MutationResponse },
    Del { id: MutationID },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaVersions {
    pub min_supported_version: u32,
    pub max_supported_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PokeStartBody {
    /// The TS protocol uses `pokeID` (capital I-D), not `pokeId`.
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    pub base_cookie: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_versions: Option<SchemaVersions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PokePartBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "lastMutationIDChanges"
    )]
    pub last_mutation_id_changes: Option<HashMap<String, u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired_queries_patches: Option<HashMap<String, QueriesPatch>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub got_queries_patch: Option<QueriesPatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_patch: Option<Vec<RowPatchOp>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutations_patch: Option<Vec<MutationPatchOp>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PokeEndBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    pub cookie: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel: Option<bool>,
}
