//! Client schema types.
//!
//! Maps to `zero-protocol/src/client-schema.ts`.
//! Describes the shape of tables as seen by the client: column types and
//! primary keys. Used for schema negotiation and query validation.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// The set of value types Zero can represent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    String,
    Number,
    Boolean,
    Null,
    Json,
}

/// Schema for a single column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    #[serde(rename = "type")]
    pub value_type: ValueType,
}

/// Schema for a single table: its columns and primary key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    pub columns: IndexMap<String, ColumnSchema>,
    pub primary_key: Vec<String>,
}

/// The full client schema: a map of table name to [`TableSchema`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientSchema {
    pub tables: IndexMap<String, TableSchema>,
}

impl ClientSchema {
    /// Returns a normalized copy with tables and columns sorted by name.
    /// Used for deterministic hashing.
    pub fn normalize(&self) -> Self {
        let mut tables: Vec<_> = self.tables.iter().collect();
        tables.sort_by_key(|(name, _)| (*name).clone());

        ClientSchema {
            tables: tables
                .into_iter()
                .map(|(name, table)| {
                    let mut cols: Vec<_> = table.columns.iter().collect();
                    cols.sort_by_key(|(name, _)| (*name).clone());

                    let mut pk = table.primary_key.clone();
                    pk.sort();

                    (
                        name.clone(),
                        TableSchema {
                            columns: cols
                                .into_iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                            primary_key: pk,
                        },
                    )
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_schema_round_trip() {
        let json = r#"{
            "tables": {
                "user": {
                    "columns": {
                        "id": {"type": "string"},
                        "name": {"type": "string"},
                        "age": {"type": "number"}
                    },
                    "primaryKey": ["id"]
                }
            }
        }"#;

        let schema: ClientSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.tables.len(), 1);

        let user = &schema.tables["user"];
        assert_eq!(user.columns.len(), 3);
        assert_eq!(user.columns["id"].value_type, ValueType::String);
        assert_eq!(user.columns["age"].value_type, ValueType::Number);
        assert_eq!(user.primary_key, vec!["id"]);
    }

    #[test]
    fn normalize_matches_ts_snapshot() {
        // Ported from zero-protocol/src/client-schema.test.ts `normalize`.
        // The TS test builds an unsorted schema and asserts the normalized
        // output matches a specific sorted shape.
        let json = r#"{
            "tables": {
                "b": {
                    "columns": {
                        "z": {"type": "number"},
                        "y": {"type": "string"}
                    },
                    "primaryKey": ["z", "y"]
                },
                "d": {
                    "columns": {
                        "v": {"type": "null"},
                        "a": {"type": "null"},
                        "b": {"type": "json"}
                    },
                    "primaryKey": ["b"]
                },
                "g": {
                    "columns": {
                        "i": {"type": "boolean"},
                        "k": {"type": "string"},
                        "j": {"type": "json"}
                    },
                    "primaryKey": ["k", "i"]
                }
            }
        }"#;

        let schema: ClientSchema = serde_json::from_str(json).unwrap();
        let n = schema.normalize();

        let table_names: Vec<_> = n.tables.keys().collect();
        assert_eq!(table_names, vec!["b", "d", "g"]);

        let b_cols: Vec<_> = n.tables["b"].columns.keys().collect();
        assert_eq!(b_cols, vec!["y", "z"]);
        assert_eq!(n.tables["b"].primary_key, vec!["y", "z"]);

        let d_cols: Vec<_> = n.tables["d"].columns.keys().collect();
        assert_eq!(d_cols, vec!["a", "b", "v"]);
        assert_eq!(n.tables["d"].primary_key, vec!["b"]);

        let g_cols: Vec<_> = n.tables["g"].columns.keys().collect();
        assert_eq!(g_cols, vec!["i", "j", "k"]);
        assert_eq!(n.tables["g"].primary_key, vec!["i", "k"]);
    }

    #[test]
    fn normalize_sorts_keys() {
        let json = r#"{
            "tables": {
                "zebra": {
                    "columns": {"z": {"type": "string"}, "a": {"type": "number"}},
                    "primaryKey": ["z", "a"]
                },
                "alpha": {
                    "columns": {"b": {"type": "boolean"}},
                    "primaryKey": ["b"]
                }
            }
        }"#;

        let schema: ClientSchema = serde_json::from_str(json).unwrap();
        let normalized = schema.normalize();

        let table_names: Vec<_> = normalized.tables.keys().collect();
        assert_eq!(table_names, vec!["alpha", "zebra"]);

        let zebra_cols: Vec<_> = normalized.tables["zebra"].columns.keys().collect();
        assert_eq!(zebra_cols, vec!["a", "z"]);

        assert_eq!(normalized.tables["zebra"].primary_key, vec!["a", "z"]);
    }
}
