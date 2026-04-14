//! Query patch operations.

use crate::ast::AST;
use serde::{Deserialize, Serialize};

/// Downstream query patch (server → client).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum QueriesPatchOp {
    Put {
        hash: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        ttl: Option<u64>,
    },
    Del {
        hash: String,
    },
    Clear {},
}

/// Upstream query patch (client → server), includes AST or name+args.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum UpQueriesPatchOp {
    Put {
        hash: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        ttl: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ast: Option<AST>,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<Vec<serde_json::Value>>,
    },
    Del {
        hash: String,
    },
    Clear {},
}

pub type QueriesPatch = Vec<QueriesPatchOp>;
pub type UpQueriesPatch = Vec<UpQueriesPatchOp>;
