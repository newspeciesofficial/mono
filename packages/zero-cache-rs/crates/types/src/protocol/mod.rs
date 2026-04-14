//! WebSocket protocol types (upstream + downstream messages).
//!
//! Maps to `zero-protocol/src/up.ts` and `zero-protocol/src/down.ts`.
//! Wire format: JSON arrays `["messageType", body]`.
//!
//! Full implementation of message types will be added incrementally.
//! This module provides the foundation structures.

pub mod connect;
pub mod downstream;
pub mod error;
pub mod poke;
pub mod push;
pub mod queries_patch;
pub mod row_patch;
pub mod upstream;

/// Current protocol version. Must match `zero-protocol/src/protocol-version.ts`.
pub const PROTOCOL_VERSION: u32 = 49;

/// Minimum server-supported protocol version.
pub const MIN_SERVER_SUPPORTED_VERSION: u32 = 30;
