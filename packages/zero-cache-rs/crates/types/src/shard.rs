//! Shard and app identification types.
//!
//! Maps to `types/shards.ts`.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Application identifier. Must match pattern `^[a-z0-9_]+$`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AppID(pub String);

impl Default for AppID {
    fn default() -> Self {
        Self("zero".into())
    }
}

impl fmt::Display for AppID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Shard number within an application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct ShardNum(pub u32);

/// Combined shard identifier: app + shard number.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardID {
    pub app_id: AppID,
    pub shard_num: ShardNum,
}

impl ShardID {
    /// The schema name for this shard's data: `{app_id}_{shard_num}`.
    pub fn schema_name(&self) -> String {
        format!("{}_{}", self.app_id, self.shard_num.0)
    }
}

impl fmt::Display for ShardID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.app_id, self.shard_num.0)
    }
}

/// Trait for types that carry an app ID.
pub trait HasAppID {
    fn app_id(&self) -> &str;
}

impl HasAppID for ShardID {
    fn app_id(&self) -> &str {
        &self.app_id.0
    }
}
