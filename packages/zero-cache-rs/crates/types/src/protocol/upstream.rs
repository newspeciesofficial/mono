//! Upstream (client → server) WebSocket messages.
//!
//! Wire format: JSON array `["messageType", body]`.
//! Custom serde implementation handles the tuple encoding.

use super::push::PushBody;
use super::queries_patch::UpQueriesPatch;
use serde::{Deserialize, Serialize};

/// Body of `changeDesiredQueries` message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeDesiredQueriesBody {
    pub desired_queries_patch: UpQueriesPatch,
}

/// Body of `updateAuth` message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateAuthBody {
    pub auth: String,
}

/// Body of `deleteClients` message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteClientsBody {
    #[serde(skip_serializing_if = "Option::is_none", alias = "clientIDs")]
    pub client_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", alias = "clientGroupIDs")]
    pub client_group_ids: Option<Vec<String>>,
}

/// Body of `pull` message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullRequestBody {
    #[serde(alias = "clientGroupID")]
    pub client_group_id: String,
    pub cookie: Option<String>,
    #[serde(alias = "requestID")]
    pub request_id: String,
}

/// All upstream message types, decoded from `["tag", body]` wire format.
#[derive(Debug, Clone)]
pub enum Upstream {
    Ping,
    Push(PushBody),
    Pull(PullRequestBody),
    ChangeDesiredQueries(ChangeDesiredQueriesBody),
    UpdateAuth(UpdateAuthBody),
    DeleteClients(DeleteClientsBody),
    CloseConnection,
}

/// Deserialize from the `["tag", body]` wire format.
impl Upstream {
    pub fn from_json(raw: &str) -> Result<Self, serde_json::Error> {
        let arr: (String, serde_json::Value) = serde_json::from_str(raw)?;
        match arr.0.as_str() {
            "ping" => Ok(Self::Ping),
            "push" => Ok(Self::Push(serde_json::from_value(arr.1)?)),
            "pull" => Ok(Self::Pull(serde_json::from_value(arr.1)?)),
            "changeDesiredQueries" => {
                Ok(Self::ChangeDesiredQueries(serde_json::from_value(arr.1)?))
            }
            "updateAuth" => Ok(Self::UpdateAuth(serde_json::from_value(arr.1)?)),
            "deleteClients" => Ok(Self::DeleteClients(serde_json::from_value(arr.1)?)),
            "closeConnection" => Ok(Self::CloseConnection),
            _ => Err(serde::de::Error::custom(format!(
                "unknown message type: {}",
                arr.0
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ping() {
        let msg = Upstream::from_json(r#"["ping", {}]"#).unwrap();
        assert!(matches!(msg, Upstream::Ping));
    }

    #[test]
    fn parse_change_desired_queries() {
        let json = r#"["changeDesiredQueries", {"desiredQueriesPatch": [{"op": "put", "hash": "abc123", "ast": {"table": "user"}}]}]"#;
        let msg = Upstream::from_json(json).unwrap();
        match msg {
            Upstream::ChangeDesiredQueries(body) => {
                assert_eq!(body.desired_queries_patch.len(), 1);
            }
            other => panic!("Expected ChangeDesiredQueries, got {other:?}"),
        }
    }

    #[test]
    fn unknown_type_errors() {
        let result = Upstream::from_json(r#"["bogus", {}]"#);
        assert!(result.is_err());
    }
}
