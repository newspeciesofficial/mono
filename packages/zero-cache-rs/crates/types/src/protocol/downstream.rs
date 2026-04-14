//! Downstream (server → client) WebSocket messages.
//!
//! Wire format: JSON array `["messageType", body]`.

use super::connect::ConnectedBody;
use super::error::ErrorBody;
use super::poke::{PokeEndBody, PokePartBody, PokeStartBody};
use serde::Serialize;

/// All downstream message types.
#[derive(Debug, Clone)]
pub enum Downstream {
    Connected(ConnectedBody),
    Error(ErrorBody),
    Pong,
    PokeStart(PokeStartBody),
    PokePart(PokePartBody),
    PokeEnd(PokeEndBody),
}

impl Downstream {
    /// Serialize to the `["tag", body]` wire format.
    pub fn to_json(&self) -> serde_json::Result<String> {
        match self {
            Self::Connected(body) => tagged_json("connected", body),
            Self::Error(body) => tagged_json("error", body),
            Self::Pong => Ok(r#"["pong",{}]"#.to_owned()),
            Self::PokeStart(body) => tagged_json("pokeStart", body),
            Self::PokePart(body) => tagged_json("pokePart", body),
            Self::PokeEnd(body) => tagged_json("pokeEnd", body),
        }
    }
}

/// Serialize a message as `["tag", body]`.
fn tagged_json(tag: &str, body: &impl Serialize) -> serde_json::Result<String> {
    let body_val = serde_json::to_value(body)?;
    serde_json::to_string(&serde_json::json!([tag, body_val]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connected_wire_format() {
        let msg = Downstream::Connected(ConnectedBody {
            wsid: "ws-123".into(),
            timestamp: Some(1234567890.0),
        });
        let json = msg.to_json().unwrap();
        assert!(json.starts_with(r#"["connected","#));
        assert!(json.contains(r#""wsid":"ws-123""#));
    }

    #[test]
    fn pong_wire_format() {
        let json = Downstream::Pong.to_json().unwrap();
        assert_eq!(json, r#"["pong",{}]"#);
    }
}
