//! Port of `packages/zql/src/ivm/source.ts`.
//!
//! Public exports ported:
//!
//! - [`SourceChangeAdd`], [`SourceChangeRemove`], [`SourceChangeEdit`] —
//!   value-typed structs, one per TS discriminant.
//! - [`SourceChange`] — tagged enum over the three variants above.
//! - [`Source`] — trait corresponding to the TS `Source` interface.
//! - [`SourceInput`] — trait extending [`InputBase`] with the
//!   `fullyAppliedFilters` flag (TS `SourceInput extends Input`).
//! - [`DebugDelegate`] — marker trait; the diagnostic method surface is
//!   deferred to a later port (TS `builder/debug-delegate.ts`).
//! - [`TableSchema`] — thin placeholder for the TS type from
//!   `zero-types/src/schema.ts`. Kept local because a full port of
//!   `zero-types` is out of scope for Layer 1.
//!
//! The TS `push`/`genPush` return `Stream<'yield'>` / `Stream<'yield' |
//! undefined>`. We model `'yield'` and `'yield' | undefined` via two
//! zero-sized tag types.

use indexmap::IndexMap;

use zero_cache_types::ast::{Condition, Ordering};
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::Row;

use crate::ivm::operator::{Input, InputBase};
use crate::ivm::stream::Stream;

use std::collections::HashSet;

/// TS `SourceChangeAdd { type: 'add'; row: Row }`.
#[derive(Debug, Clone)]
pub struct SourceChangeAdd {
    pub row: Row,
}

/// TS `SourceChangeRemove { type: 'remove'; row: Row }`.
#[derive(Debug, Clone)]
pub struct SourceChangeRemove {
    pub row: Row,
}

/// TS `SourceChangeEdit { type: 'edit'; row: Row; oldRow: Row }`.
#[derive(Debug, Clone)]
pub struct SourceChangeEdit {
    pub row: Row,
    pub old_row: Row,
}

/// TS `SourceChange = SourceChangeAdd | SourceChangeRemove | SourceChangeEdit`.
/// Tagged on the variant; the TS `type` discriminant is recoverable via
/// [`SourceChange::kind`].
#[derive(Debug, Clone)]
pub enum SourceChange {
    Add(SourceChangeAdd),
    Remove(SourceChangeRemove),
    Edit(SourceChangeEdit),
}

/// The string discriminant TS uses on `change.type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceChangeKind {
    Add,
    Remove,
    Edit,
}

impl SourceChange {
    /// TS `change.type`.
    #[inline]
    pub fn kind(&self) -> SourceChangeKind {
        match self {
            Self::Add(_) => SourceChangeKind::Add,
            Self::Remove(_) => SourceChangeKind::Remove,
            Self::Edit(_) => SourceChangeKind::Edit,
        }
    }

    /// The post-change row (the new row for Edit). Matches TS `change.row`.
    #[inline]
    pub fn row(&self) -> &Row {
        match self {
            Self::Add(c) => &c.row,
            Self::Remove(c) => &c.row,
            Self::Edit(c) => &c.row,
        }
    }
}

/// Yield sentinel — matches TS `'yield'` in `Stream<'yield'>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Yield;

/// One step of [`Source::gen_push`] — matches TS `Stream<'yield' | undefined>`.
/// `Step` corresponds to `undefined` (one input processed), `Yield` to
/// `'yield'` (relinquish control to caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenPushStep {
    /// TS `undefined` — a push into one connected input completed.
    Step,
    /// TS `'yield'` — the source is yielding control to the caller.
    Yield,
}

/// Placeholder for TS `TableSchema` (`zero-types/src/schema.ts`). The
/// upstream crate does not yet expose a typed `TableSchema`, so we inline
/// the minimum fields consumed by `Source::tableSchema` callers. Full
/// port is deferred until a caller needs the `columns` shape (none in
/// Layer 1).
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub server_name: Option<String>,
    pub columns: IndexMap<String, serde_json::Value>,
    pub primary_key: PrimaryKey,
}

/// Marker for TS `DebugDelegate`. Concrete diagnostic methods
/// (`initQuery`, `rowVended`, etc.) are deferred to a dedicated port —
/// they're Layer-2+ concerns and no Layer 1 consumer actually invokes
/// them. Keeping the trait present as a named type lets [`Source::connect`]
/// accept `Option<&dyn DebugDelegate>` exactly as the TS signature does.
pub trait DebugDelegate: Send + Sync {}

/// TS `Source` interface — the root of an operator pipeline.
///
/// Mirrors the TS methods:
///   - `get tableSchema()` → [`Source::table_schema`]
///   - `connect(sort, filters?, splitEditKeys?, debug?)` → [`Source::connect`]
///   - `push(change)` → [`Source::push`]
///   - `genPush(change)` → [`Source::gen_push`]
pub trait Source: Send + Sync {
    /// TS `get tableSchema(): TableSchema`.
    fn table_schema(&self) -> &TableSchema;

    /// TS `connect(sort, filters?, splitEditKeys?, debug?)`.
    ///
    /// Returns a fresh [`SourceInput`] an operator can plug into. To free
    /// resources, downstream operators call `destroy()` on the returned
    /// input (see [`InputBase::destroy`]).
    fn connect(
        &self,
        sort: Ordering,
        filters: Option<&Condition>,
        split_edit_keys: Option<&HashSet<String>>,
        debug: Option<&dyn DebugDelegate>,
    ) -> Box<dyn SourceInput>;

    /// TS `push(change): Stream<'yield'>`. Iterating the returned stream
    /// to exhaustion commits the change to every connected input.
    fn push<'a>(&'a self, change: SourceChange) -> Stream<'a, Yield>;

    /// TS `genPush(change): Stream<'yield' | undefined>`. Each
    /// [`GenPushStep::Step`] corresponds to one connected input being
    /// pushed; [`GenPushStep::Yield`] relinquishes control.
    fn gen_push<'a>(&'a self, change: SourceChange) -> Stream<'a, GenPushStep>;
}

/// TS `interface SourceInput extends Input { readonly fullyAppliedFilters: boolean }`.
///
/// Implementors are `Input`s — they can be connected to an operator via
/// `setOutput` and fetched from. The extra `fullyAppliedFilters` flag
/// tells downstream operators whether the source applied all predicates
/// itself (no further filter operator required).
pub trait SourceInput: Input {
    /// TS `readonly fullyAppliedFilters`.
    fn fully_applied_filters(&self) -> bool;
}

// Required because `dyn SourceInput` must be usable where a `dyn Input`
// is expected. The impl routes through the trait method directly.
impl InputBase for Box<dyn SourceInput> {
    fn get_schema(&self) -> &crate::ivm::schema::SourceSchema {
        (**self).get_schema()
    }
    fn destroy(&mut self) {
        (**self).destroy();
    }
}

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!   - [`SourceChange::kind`] — every discriminant variant
    //!   - [`SourceChange::row`] — every variant returns the post-change row;
    //!     the Edit case specifically returns `row`, not `old_row`
    //!   - Construction of each `SourceChange*` struct with `row` / `old_row`
    //!   - [`GenPushStep`] variants are distinct and clone-comparable
    //!   - [`Yield`] is clone-comparable (matches the `'yield'` sentinel usage)

    use super::*;
    use serde_json::json;

    fn row_with(key: &str, val: serde_json::Value) -> Row {
        let mut r = Row::new();
        r.insert(key.to_string(), Some(val));
        r
    }

    // ─── kind(): every variant ─────────────────────────────────────────

    #[test]
    fn source_change_kind_add() {
        let c = SourceChange::Add(SourceChangeAdd {
            row: row_with("id", json!(1)),
        });
        assert_eq!(c.kind(), SourceChangeKind::Add);
    }

    #[test]
    fn source_change_kind_remove() {
        let c = SourceChange::Remove(SourceChangeRemove {
            row: row_with("id", json!(1)),
        });
        assert_eq!(c.kind(), SourceChangeKind::Remove);
    }

    #[test]
    fn source_change_kind_edit() {
        let c = SourceChange::Edit(SourceChangeEdit {
            row: row_with("id", json!(2)),
            old_row: row_with("id", json!(1)),
        });
        assert_eq!(c.kind(), SourceChangeKind::Edit);
    }

    // ─── row(): Add/Remove arms expose the single row ──────────────────

    #[test]
    fn source_change_row_add_returns_row() {
        let c = SourceChange::Add(SourceChangeAdd {
            row: row_with("v", json!("add")),
        });
        assert_eq!(c.row().get("v"), Some(&Some(json!("add"))));
    }

    #[test]
    fn source_change_row_remove_returns_row() {
        let c = SourceChange::Remove(SourceChangeRemove {
            row: row_with("v", json!("remove")),
        });
        assert_eq!(c.row().get("v"), Some(&Some(json!("remove"))));
    }

    // ─── row(): Edit arm specifically returns `row`, not `old_row` ─────

    #[test]
    fn source_change_row_edit_returns_new_row_not_old_row() {
        let c = SourceChange::Edit(SourceChangeEdit {
            row: row_with("v", json!("new")),
            old_row: row_with("v", json!("old")),
        });
        assert_eq!(c.row().get("v"), Some(&Some(json!("new"))));
        // old_row is still accessible via the struct when caller needs it.
        if let SourceChange::Edit(e) = &c {
            assert_eq!(e.old_row.get("v"), Some(&Some(json!("old"))));
        } else {
            panic!("expected Edit variant");
        }
    }

    // ─── GenPushStep: both variants distinct ───────────────────────────

    #[test]
    fn gen_push_step_variants_compare_distinct() {
        assert_eq!(GenPushStep::Step, GenPushStep::Step);
        assert_eq!(GenPushStep::Yield, GenPushStep::Yield);
        assert_ne!(GenPushStep::Step, GenPushStep::Yield);
    }

    // ─── Yield sentinel ─────────────────────────────────────────────────

    #[test]
    fn yield_sentinel_is_unit_and_eq() {
        let a = Yield;
        let b = Yield;
        assert_eq!(a, b);
        // Clone/Copy parity with the TS `'yield'` string literal.
        let _c = a;
    }

    // ─── TableSchema construction parity ───────────────────────────────

    #[test]
    fn table_schema_holds_ts_fields() {
        let ts = TableSchema {
            name: "users".into(),
            server_name: Some("pg_users".into()),
            columns: {
                let mut c = IndexMap::new();
                c.insert("id".into(), json!({"type": "number"}));
                c
            },
            primary_key: PrimaryKey::new(vec!["id".into()]),
        };
        assert_eq!(ts.name, "users");
        assert_eq!(ts.server_name.as_deref(), Some("pg_users"));
        assert_eq!(ts.columns.len(), 1);
        assert_eq!(ts.primary_key.columns(), &["id".to_string()]);
    }

    // Branch: server_name absent (TS `serverName?` is optional).
    #[test]
    fn table_schema_without_server_name() {
        let ts = TableSchema {
            name: "t".into(),
            server_name: None,
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
        };
        assert!(ts.server_name.is_none());
    }
}
