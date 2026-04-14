//! Row patch operations (server → client).

use crate::primary_key::PrimaryKeyValueRecord;
use crate::value::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum RowPatchOp {
    Put {
        #[serde(rename = "tableName")]
        table_name: String,
        value: Row,
    },
    Update {
        #[serde(rename = "tableName")]
        table_name: String,
        id: PrimaryKeyValueRecord,
        #[serde(skip_serializing_if = "Option::is_none")]
        merge: Option<serde_json::Map<String, serde_json::Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        constrain: Option<Vec<String>>,
    },
    Del {
        #[serde(rename = "tableName")]
        table_name: String,
        id: PrimaryKeyValueRecord,
    },
    Clear {},
}
