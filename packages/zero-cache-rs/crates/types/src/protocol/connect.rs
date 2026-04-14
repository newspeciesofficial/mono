//! Connection handshake types.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectedBody {
    pub wsid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<f64>,
}

// ---------------------------------------------------------------------------
// Tests ported from zero-protocol/src/connect.test.ts.
// ---------------------------------------------------------------------------

// The behavioural tests for `encodeSecProtocols` / `decodeSecProtocols` live
// alongside the implementation in
// `crates/server/src/connection.rs` — see `encode_decode_sec_protocols_round_trip`
// and `encode_sec_protocol_with_too_much_data`. The helpers can't live in
// `zero-cache-types` because they depend on `base64` + `urlencoding`, which
// are pulled in by the `server` crate.
