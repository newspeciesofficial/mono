//! Protocol error body types (server → client).

use crate::error::{ErrorKind, ErrorOrigin};
use serde::{Deserialize, Serialize};

/// Error body sent in `["error", body]` downstream messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorBody {
    pub kind: ErrorKind,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<ErrorOrigin>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_backoff_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_backoff_ms: Option<u64>,
}

// ---------------------------------------------------------------------------
// Tests ported from zero-protocol/src/error.test.ts.
// ---------------------------------------------------------------------------
//
// The TS side has a `ProtocolError` class and `isProtocolError` guard that
// wrap `ErrorBody`. The Rust port only mirrors the wire body — there is no
// `ProtocolError` yet — so the behavioural tests are parked as `#[ignore]`.

#[cfg(test)]
mod tests {
    #[test]
    #[ignore = "needs TS `ProtocolError` class + `isProtocolError` guard \
                ported from packages/zero-protocol/src/error.ts — tracked in \
                docs/ts-vs-rs-comparison.md row 1 (`Protocol & types`)"]
    fn protocol_error_exposes_error_body_and_metadata() {
        // TS: `exposes error body and metadata`.
    }

    #[test]
    #[ignore = "needs TS `ProtocolError` class + `isProtocolError` guard \
                ported from packages/zero-protocol/src/error.ts — tracked in \
                docs/ts-vs-rs-comparison.md row 1 (`Protocol & types`)"]
    fn protocol_error_preserves_cause() {
        // TS: `preserves error cause when provided`.
    }

    #[test]
    #[ignore = "needs TS `ProtocolError` class + `isProtocolError` guard \
                ported from packages/zero-protocol/src/error.ts — tracked in \
                docs/ts-vs-rs-comparison.md row 1 (`Protocol & types`)"]
    fn protocol_error_has_useful_stack_trace() {
        // TS: `has useful stack trace`.
    }
}
