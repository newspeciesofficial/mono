//! Version types: LexiVersion and NullableVersion.
//!
//! LexiVersion is a lexicographically sortable string used as watermarks,
//! cookies, and state versions throughout Zero. The ordering property means
//! `"01" < "02" < "0a"` etc., enabling simple string comparison for
//! version ordering without numeric parsing.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A lexicographically sortable version string.
///
/// Wraps a `String` with `Ord` derived from the string's natural byte order,
/// which is the correct ordering for lexi-encoded versions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct LexiVersion(pub String);

impl LexiVersion {
    pub const ZERO: LexiVersion = LexiVersion(String::new());

    #[inline]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for LexiVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for LexiVersion {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for LexiVersion {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Nullable version — `None` before first sync (maps to JS `null`).
pub type NullableVersion = Option<LexiVersion>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering() {
        let v1 = LexiVersion::new("01");
        let v2 = LexiVersion::new("02");
        let v3 = LexiVersion::new("0a");
        assert!(v1 < v2);
        assert!(v2 < v3);
    }

    #[test]
    fn serde_round_trip() {
        let v = LexiVersion::new("abc123");
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, r#""abc123""#);

        let parsed: LexiVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(v, parsed);
    }

    #[test]
    fn nullable_version() {
        let none: NullableVersion = None;
        let some: NullableVersion = Some(LexiVersion::new("v1"));

        assert_eq!(serde_json::to_string(&none).unwrap(), "null");
        assert_eq!(serde_json::to_string(&some).unwrap(), r#""v1""#);
    }

    // -----------------------------------------------------------------------
    // Tests ported from zero-cache/src/types/lexi-version.test.ts.
    //
    // These exercise the `versionToLexi` / `versionFromLexi` / `min` / `max`
    // helpers from the TS `lexi-version` module. In Rust the equivalents are
    // currently implemented only as private helpers inside
    // `sync::schema::types` — the public `LexiVersion` in this file is just a
    // wrapper around a pre-encoded string. Once the encoding helpers are
    // promoted to this module (or exposed via a public function here) these
    // tests should un-ignore.
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "needs TS `versionToLexi`/`versionFromLexi` ported from \
                packages/zero-cache/src/types/lexi-version.ts — tracked in \
                docs/ts-vs-rs-comparison.md row 1 (`Protocol & types`)"]
    fn lexi_version_encoding() {
        // TS test: `LexiVersion encoding`. Table of (num, lexi) pairs:
        //   0 → "00", 10 → "0a", 35 → "0z", 36 → "110", 46655 → "2zzz",
        //   2^32 → "61z141z4", MAX_SAFE_INT → "a2gosa7pa2gv",
        //   2^64 → "c3w5e11264sgsg", 2^75 → "e65gym2kbgwjf668",
        //   2^128 → "of5lxx1zz5pnorynqglhzmsp34",
        //   2^160 → "utwj4yidkw7a8pn4g709kzmfoaol3x8g",
        //   2^186 → "zx6sp2h09v22524mnljo7dsm6cz9iehtq4xds",
        //   36^36 - 1 → "z".repeat(37).
    }

    #[test]
    #[ignore = "needs TS `LexiVersion.min`/`.max` ported from \
                packages/zero-cache/src/types/lexi-version.ts — tracked in \
                docs/ts-vs-rs-comparison.md row 1 (`Protocol & types`)"]
    fn lexi_min_max() {
        // TS test: `min max`.
    }

    #[test]
    #[ignore = "needs TS `versionToLexi` + JS `localeCompare` ported from \
                packages/zero-cache/src/types/lexi-version.ts — tracked in \
                docs/ts-vs-rs-comparison.md row 1 (`Protocol & types`)"]
    fn lexi_version_sorting() {
        // TS test: `LexiVersion sorting`. Fuzz tests that versionToLexi(n)
        // preserves numeric ordering under string comparison.
    }

    #[test]
    #[ignore = "needs TS `versionToLexi` (with its range/validity checks) \
                ported from packages/zero-cache/src/types/lexi-version.ts — \
                tracked in docs/ts-vs-rs-comparison.md row 1"]
    fn lexi_version_encode_sanity_checks() {
        // TS test: `LexiVersion encode sanity checks`. Negative, non-integer,
        // out-of-safe-range, and >=2^187 values must throw.
    }

    #[test]
    #[ignore = "needs TS `versionFromLexi` (with its validity checks) ported \
                from packages/zero-cache/src/types/lexi-version.ts — tracked \
                in docs/ts-vs-rs-comparison.md row 1"]
    fn lexi_version_decode_sanity_checks() {
        // TS test: `LexiVersion decode sanity checks`. Bad strings must throw.
    }
}
