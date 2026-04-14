//! Unified error types for zero-cache-rs.

use serde::{Deserialize, Serialize};

/// All error kinds from the Zero protocol.
/// Maps to `zero-protocol/src/error.ts` ErrorKind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorKind {
    AuthInvalidated,
    ClientNotFound,
    InvalidConnectionRequest,
    InvalidConnectionRequestBaseCookie,
    InvalidConnectionRequestLastMutationID,
    InvalidConnectionRequestClientDeleted,
    InvalidMessage,
    InvalidPush,
    PushFailed,
    MutationFailed,
    MutationRateLimited,
    Rebalance,
    Rehome,
    TransformFailed,
    Unauthorized,
    VersionNotSupported,
    SchemaVersionNotSupported,
    ServerOverloaded,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorOrigin {
    Client,
    Server,
    ZeroCache,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorReason {
    Database,
    Parse,
    #[serde(rename = "oooMutation")]
    OutOfOrderMutation,
    #[serde(rename = "unsupportedPushVersion")]
    UnsupportedPushVersion,
    Internal,
    #[serde(rename = "http")]
    Http,
    Timeout,
}

/// Application-level error covering all subsystems.
#[derive(Debug, thiserror::Error)]
pub enum ZeroCacheError {
    #[error("protocol: {kind:?} — {message}")]
    Protocol { kind: ErrorKind, message: String },

    #[error("database: {0}")]
    Database(String),

    #[error("sqlite: {0}")]
    Sqlite(String),

    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("config: {0}")]
    Config(String),

    #[error("websocket: {0}")]
    WebSocket(String),

    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, ZeroCacheError>;
