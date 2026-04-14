//! Port of `packages/zql/src/ivm/schema.ts`.
//!
//! Public exports ported:
//!
//! - [`SourceSchema`] — information about the nodes output by an operator.
//!
//! TS uses `TableSchema` from `zero-types/src/schema.ts` only for column
//! definitions (`Record<string, SchemaValue>`) embedded in `SourceSchema`.
//! We inline the equivalent here as `columns: IndexMap<String, SchemaValue>`
//! and defer a full `SchemaValue` port to when an operator actually reads
//! from it (none in Layers 0–2). For now `SchemaValue` is represented by
//! [`serde_json::Value`] — zero-cache-types does not yet export a typed
//! version, and every existing consumer of `SourceSchema` in the ported
//! Rust code only needs the *presence* of columns, not their shape.
//!
//! The TS `compareRows` field is a `Comparator` closure. In Rust we store
//! it as `Arc<Comparator>` because `SourceSchema` is cloned and shared
//! across the operator graph (every downstream operator holds a reference
//! to its upstream's schema).

use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use zero_cache_types::ast::{Ordering, System};
use zero_cache_types::primary_key::PrimaryKey;

use crate::ivm::data::Comparator;

/// Placeholder for TS `SchemaValue` (`zero-types/src/schema-value.ts`).
///
/// None of the Layer 0–2 ports read individual column metadata — they only
/// iterate the key set. Keeping the value side as raw JSON lets us defer
/// the `SchemaValue` port until a consumer needs it (likely around the
/// source-row conversion layer).
pub type SchemaValue = JsonValue;

/// TS `SourceSchema` (`ivm/schema.ts`).
///
/// Information about the nodes output by an operator. Every operator
/// exposes one via `Input::getSchema()`. The struct is `Clone` because the
/// graph wires each operator up with a (cheap) clone of its upstream's
/// schema.
#[derive(Clone)]
pub struct SourceSchema {
    /// Table name. TS `tableName`.
    pub table_name: String,
    /// Column definitions (name → SchemaValue). TS `columns`.
    pub columns: IndexMap<String, SchemaValue>,
    /// Primary key columns. TS `primaryKey`.
    pub primary_key: PrimaryKey,
    /// Nested relationships, keyed by relationship name. TS `relationships`.
    pub relationships: IndexMap<String, SourceSchema>,
    /// Whether the source is hidden from sync (e.g. a permissions-only
    /// filter). TS `isHidden`.
    pub is_hidden: bool,
    /// Which system is responsible for this schema being present. TS
    /// `system`. Rows from `System::Permissions` must not be synced.
    pub system: System,
    /// Row comparator matching [`Self::sort`]. TS `compareRows`.
    ///
    /// Stored as `Arc<Comparator>` so clones share one closure. The
    /// `Comparator` itself is a `Box<dyn Fn … + Send + Sync>`; wrapping
    /// it in `Arc` avoids re-boxing per clone.
    pub compare_rows: Arc<Comparator>,
    /// Ordering of the rows. TS `sort`.
    pub sort: Ordering,
}

impl std::fmt::Debug for SourceSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSchema")
            .field("table_name", &self.table_name)
            .field("columns", &self.columns.keys().collect::<Vec<_>>())
            .field("primary_key", &self.primary_key)
            .field(
                "relationships",
                &self.relationships.keys().collect::<Vec<_>>(),
            )
            .field("is_hidden", &self.is_hidden)
            .field("system", &self.system)
            .field("sort", &self.sort)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    //! Branch coverage: `SourceSchema` is data-only; the only meaningful
    //! behaviour is that `compare_rows` is callable through the stored
    //! `Arc` and the Debug impl renders every field. Covered below.

    use super::*;
    use crate::ivm::data::make_comparator;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::cmp::Ordering as CmpOrdering;
    use zero_cache_types::ast::Direction;
    use zero_cache_types::value::{Row, Value};

    fn row(fields: &[(&str, Value)]) -> Row {
        let mut r = Row::new();
        for (k, v) in fields {
            r.insert((*k).to_string(), v.clone());
        }
        r
    }

    // Branch: construct a SourceSchema, invoke compare_rows via Arc, and
    // confirm every public field round-trips.
    #[test]
    fn source_schema_holds_all_fields_and_compare_rows_is_callable() {
        let sort: Ordering = vec![("id".to_string(), Direction::Asc)];
        let cmp = make_comparator(sort.clone(), false);
        let schema = SourceSchema {
            table_name: "users".into(),
            columns: {
                let mut c = IndexMap::new();
                c.insert("id".into(), json!({"type": "number"}));
                c.insert("name".into(), json!({"type": "string"}));
                c
            },
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Client,
            compare_rows: Arc::new(cmp),
            sort,
        };

        assert_eq!(schema.table_name, "users");
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.primary_key.columns(), &["id".to_string()]);
        assert!(schema.relationships.is_empty());
        assert!(!schema.is_hidden);
        assert!(matches!(schema.system, System::Client));
        assert_eq!(schema.sort.len(), 1);

        let a = row(&[("id", Some(json!(1)))]);
        let b = row(&[("id", Some(json!(2)))]);
        assert_eq!((schema.compare_rows)(&a, &b), CmpOrdering::Less);
        assert_eq!((schema.compare_rows)(&b, &a), CmpOrdering::Greater);
        assert_eq!((schema.compare_rows)(&a, &a), CmpOrdering::Equal);
    }

    // Branch: clone shares the Arc<Comparator>; both clones must observe
    // identical compare behaviour.
    #[test]
    fn source_schema_clone_shares_comparator() {
        let sort: Ordering = vec![("id".to_string(), Direction::Asc)];
        let cmp = make_comparator(sort.clone(), false);
        let original = SourceSchema {
            table_name: "t".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: true,
            system: System::Permissions,
            compare_rows: Arc::new(cmp),
            sort,
        };

        let cloned = original.clone();
        assert!(Arc::ptr_eq(&original.compare_rows, &cloned.compare_rows));
        assert!(cloned.is_hidden);
        assert!(matches!(cloned.system, System::Permissions));
    }

    // Branch: nested relationships map populates and is visible through
    // the Debug impl's key list.
    #[test]
    fn source_schema_nested_relationships_reachable() {
        let sort: Ordering = vec![];
        let inner = SourceSchema {
            table_name: "inner".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort: sort.clone(),
        };
        let mut rels = IndexMap::new();
        rels.insert("child".to_string(), inner);

        let outer = SourceSchema {
            table_name: "outer".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: rels,
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        };

        assert!(outer.relationships.contains_key("child"));
        assert_eq!(outer.relationships["child"].table_name, "inner");

        // Debug impl renders relationship key list.
        let s = format!("{outer:?}");
        assert!(s.contains("child"));
        assert!(s.contains("outer"));
    }
}
