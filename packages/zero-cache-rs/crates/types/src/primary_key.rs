//! Primary key types.
//!
//! Maps to `zero-protocol/src/primary-key.ts`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// Non-empty list of column names forming a primary key.
/// Invariant: always has at least one element (enforced at construction).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrimaryKey(Vec<String>);

impl PrimaryKey {
    /// Creates a new `PrimaryKey`. Panics if `columns` is empty.
    pub fn new(columns: Vec<String>) -> Self {
        assert!(
            !columns.is_empty(),
            "PrimaryKey requires at least one column"
        );
        Self(columns)
    }

    #[inline]
    pub fn columns(&self) -> &[String] {
        &self.0
    }

    #[inline]
    pub fn first(&self) -> &str {
        &self.0[0]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Always returns `false` — PrimaryKey is guaranteed non-empty at construction.
    #[inline]
    pub fn is_empty(&self) -> bool {
        false // Invariant: always at least one column
    }
}

impl fmt::Display for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.0.join(", "))
    }
}

/// Scalar value allowed in a primary key column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PrimaryKeyValue {
    String(String),
    Number(f64),
    Bool(bool),
}

impl fmt::Display for PrimaryKeyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => write!(f, "{s}"),
            Self::Number(n) => write!(f, "{n}"),
            Self::Bool(b) => write!(f, "{b}"),
        }
    }
}

/// Record mapping primary key column names to their values.
pub type PrimaryKeyValueRecord = BTreeMap<String, PrimaryKeyValue>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_key_serde() {
        let pk = PrimaryKey::new(vec!["id".into()]);
        let json = serde_json::to_string(&pk).unwrap();
        assert_eq!(json, r#"["id"]"#);

        let pk2: PrimaryKey = serde_json::from_str(&json).unwrap();
        assert_eq!(pk, pk2);
    }

    #[test]
    fn composite_key_serde() {
        let pk = PrimaryKey::new(vec!["org_id".into(), "user_id".into()]);
        let json = serde_json::to_string(&pk).unwrap();
        assert_eq!(json, r#"["org_id","user_id"]"#);
    }

    #[test]
    fn primary_key_value_serde() {
        let v: PrimaryKeyValue = serde_json::from_str(r#""abc""#).unwrap();
        assert_eq!(v, PrimaryKeyValue::String("abc".into()));

        let v: PrimaryKeyValue = serde_json::from_str("42").unwrap();
        assert_eq!(v, PrimaryKeyValue::Number(42.0));

        let v: PrimaryKeyValue = serde_json::from_str("true").unwrap();
        assert_eq!(v, PrimaryKeyValue::Bool(true));
    }

    #[test]
    #[should_panic(expected = "at least one column")]
    fn empty_primary_key_panics() {
        PrimaryKey::new(vec![]);
    }
}
