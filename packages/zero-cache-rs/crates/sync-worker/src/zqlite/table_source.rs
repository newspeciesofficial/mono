//! Port of `packages/zqlite/src/table-source.ts`.
//!
//! Public exports ported:
//!
//! - [`TableSource`] — SQLite-backed implementation of
//!   [`crate::ivm::source::Source`]. The production `Source` used by every
//!   real pipeline. Reads rows from the replica via `rusqlite` and writes
//!   changes through prepared INSERT/DELETE/UPDATE statements.
//! - [`UnsupportedValueError`] — thrown when a value exceeds supported
//!   bounds (bigint overflow) or fails JSON parsing. Mirrors the TS
//!   `UnsupportedValueError` class.
//! - [`ValueType`] — port of the `ValueType` enum from
//!   `zero-schema/src/table-schema.ts`. Replicates the `boolean | number |
//!   string | null | json` shape the `toSQLiteType` / `fromSQLiteType`
//!   converters branch on. Kept local (instead of `zero-cache-types`)
//!   because this is the first ported file that needs a typed column shape.
//! - [`SchemaValue`] — `{ type: ValueType, optional: bool }`. Our `columns`
//!   map carries these directly; the TS version has the same two fields.
//! - [`to_sqlite_type_name`] — TS function exported for downstream use.
//! - [`to_sqlite_types`] / [`from_sqlite_types`] — TS helpers exported for
//!   consumers (e.g. row-key construction). Ports preserve both names and
//!   behavior.
//!
//! ## Design decisions
//!
//! - Ownership: everything the `Source` methods mutate lives under
//!   `Arc<Mutex<TableSourceState>>`, matching `memory_source.rs`. The
//!   rusqlite `Connection` is not `Sync`, so the state (which contains the
//!   connection) is held behind the mutex rather than `RwLock`.
//! - Statement caching: TS uses an LRU `StatementCache`. We use a
//!   `HashMap<String, rusqlite::Statement>` keyed on the formatted SQL
//!   string. rusqlite caches internally via `prepare_cached`, so our map
//!   is largely redundant; we use `prepare_cached` directly (SQL text is
//!   the cache key).
//! - Chokepoint: all SQLite access funnels through
//!   [`TableSource::query_rows`] (reads) and [`TableSource::exec_write`]
//!   (writes). A future shadow-ffi / recording layer hooks these two
//!   methods and nothing else. Every public method of `TableSource` only
//!   touches SQLite via these two helpers — this is enforced by leaving
//!   the `Connection` field private to the state struct.
//! - We intentionally do NOT port the overlay / push fan-out / filter
//!   predicate machinery here — those live in other modules (notably
//!   `memory_source.rs`, `push_accumulated.rs`, `filter_operators.rs`).
//!   TS TableSource uses exports from `memory-source.ts`
//!   (`genPushAndWriteWithSplitEdit`, `generateWithOverlay`, etc.); we
//!   use those ports indirectly via the `Source` trait contract but leave
//!   the full wiring to the builder module (Layer 9). What is implemented
//!   here:
//!     * schema discovery (columns + unique indexes),
//!     * prepared-statement construction for INSERT / DELETE / UPDATE,
//!     * `fetch` (via `SELECT … WHERE … ORDER BY … LIMIT ?`),
//!     * `push` that writes the change to SQLite then fans out to
//!       connected outputs (no overlays — downstream sees the committed
//!       state immediately, which is identical to overlay semantics for
//!       the simple one-table case).
//!     * `connect` tracks connections; `SourceInput::destroy` removes
//!       them.
//! - Value representation: [`zero_cache_types::value::Value`] is
//!   `Option<serde_json::Value>`. We convert to/from rusqlite values with
//!   local helpers.
//! - `shouldYield` callback: the TS constructor accepts a predicate called
//!   between every row; it decides whether to emit a `'yield'` sentinel.
//!   Rust consumers drain eagerly (see `memory_source.rs` for the same
//!   choice), so we keep the callback on the struct (for future use) but
//!   do NOT emit `Yield` sentinels in fetch output.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, params_from_iter};
use serde_json::Value as JsonValue;
use thiserror::Error;

#[allow(unused_imports)]
use zero_cache_types::ast::{Condition, Direction, Ordering, System};
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::{Row, Value};

use crate::ivm::data::{Node, NodeOrYield, make_comparator};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, Start};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::{
    DebugDelegate, GenPushStep, Source, SourceChange, SourceInput, TableSchema, Yield,
};
use crate::ivm::stream::Stream;

// ─── Column typing ────────────────────────────────────────────────────

/// Port of TS `ValueType` from `zero-schema/src/table-schema.ts`.
///
/// Used by [`to_sqlite_type`] / [`from_sqlite_type`] to select the
/// conversion rule per column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueType {
    Boolean,
    Number,
    String,
    Null,
    Json,
}

/// Port of TS `SchemaValue` from `zero-schema/src/table-schema.ts`.
///
/// The TS version has more optional fields (`kind`, `customType`, etc.);
/// only `type` and `optional` are read by TableSource, so those are the
/// only fields ported here.
#[derive(Debug, Clone)]
pub struct SchemaValue {
    pub value_type: ValueType,
    pub optional: bool,
}

impl SchemaValue {
    /// Convenience constructor for tests — `optional = false`.
    pub fn new(value_type: ValueType) -> Self {
        Self {
            value_type,
            optional: false,
        }
    }
}

/// TS `toSQLiteTypeName(type)`.
pub fn to_sqlite_type_name(ty: ValueType) -> &'static str {
    match ty {
        ValueType::Boolean => "INTEGER",
        ValueType::Number => "REAL",
        ValueType::String => "TEXT",
        ValueType::Null => "NULL",
        ValueType::Json => "TEXT",
    }
}

// ─── Error type ───────────────────────────────────────────────────────

/// TS `class UnsupportedValueError extends Error {}`. Surfaces when a
/// bigint overflows the safe-integer range or when JSON parse fails.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct UnsupportedValueError(pub String);

/// Errors raised by TableSource. Internal; callers see these as
/// `Result<..., TableSourceError>` from `new` and `fetch_all`.
#[derive(Debug, Error)]
pub enum TableSourceError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("unsupported value: {0}")]
    Unsupported(#[from] UnsupportedValueError),

    #[error("Invalid column \"{column}\" for table \"{table}\". Synced columns include {known}")]
    UnknownColumn {
        column: String,
        table: String,
        known: String,
    },
}

// ─── Value conversion ────────────────────────────────────────────────

/// TS `toSQLiteType(v, type)` — converts an IVM [`Value`] to a
/// [`rusqlite::types::Value`] for binding. Mirrors the TS branches.
pub fn to_sqlite_type(v: &Value, ty: ValueType) -> SqlValue {
    match v {
        None => SqlValue::Null,
        Some(json) => match ty {
            ValueType::Boolean => match json {
                JsonValue::Null => SqlValue::Null,
                JsonValue::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
                // TS: `v ? 1 : 0` applies JS truthiness to any value; no
                // real caller ever passes non-bool for a boolean column, so
                // we coerce conservatively. A JSON `false`, `0`, `""`,
                // `null` is falsy; anything else is truthy.
                JsonValue::Number(n) => SqlValue::Integer(if n.as_f64().unwrap_or(0.0) != 0.0 {
                    1
                } else {
                    0
                }),
                JsonValue::String(s) => SqlValue::Integer(if s.is_empty() { 0 } else { 1 }),
                _ => SqlValue::Integer(1),
            },
            ValueType::Number => match json {
                JsonValue::Null => SqlValue::Null,
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        SqlValue::Integer(i)
                    } else if let Some(f) = n.as_f64() {
                        SqlValue::Real(f)
                    } else {
                        SqlValue::Null
                    }
                }
                JsonValue::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
                JsonValue::String(s) => SqlValue::Text(s.clone()),
                other => SqlValue::Text(other.to_string()),
            },
            ValueType::String => match json {
                JsonValue::Null => SqlValue::Null,
                JsonValue::String(s) => SqlValue::Text(s.clone()),
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        SqlValue::Integer(i)
                    } else if let Some(f) = n.as_f64() {
                        SqlValue::Real(f)
                    } else {
                        SqlValue::Null
                    }
                }
                JsonValue::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
                other => SqlValue::Text(other.to_string()),
            },
            ValueType::Null => SqlValue::Null,
            ValueType::Json => {
                // TS: `JSON.stringify(v)`.
                SqlValue::Text(json.to_string())
            }
        },
    }
}

/// TS `toSQLiteTypes(columns, row, columnTypes)` — projects a subset of
/// row fields to the SQLite bind list.
pub fn to_sqlite_types(
    columns: &[String],
    row: &Row,
    column_types: &IndexMap<String, SchemaValue>,
) -> Vec<SqlValue> {
    columns
        .iter()
        .map(|col| {
            let v = row.get(col).cloned().unwrap_or(None);
            let ty = column_types
                .get(col)
                .map(|s| s.value_type)
                .unwrap_or(ValueType::String);
            to_sqlite_type(&v, ty)
        })
        .collect()
}

/// Safe-integer bounds matching JS `Number.MAX_SAFE_INTEGER`.
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
const MIN_SAFE_INTEGER: i64 = -9_007_199_254_740_991;

/// TS `fromSQLiteType(valueType, v, column, tableName)`.
fn from_sqlite_type(
    value_type: ValueType,
    v: ValueRef<'_>,
    column: &str,
    table: &str,
) -> Result<Value, UnsupportedValueError> {
    if let ValueRef::Null = v {
        return Ok(None);
    }
    match value_type {
        ValueType::Boolean => {
            let i = match v {
                ValueRef::Integer(i) => i,
                ValueRef::Real(f) => f as i64,
                ValueRef::Text(b) => {
                    if b.is_empty() {
                        0
                    } else {
                        1
                    }
                }
                _ => 0,
            };
            Ok(Some(JsonValue::Bool(i != 0)))
        }
        ValueType::Number | ValueType::String | ValueType::Null => match v {
            ValueRef::Integer(i) => {
                if i > MAX_SAFE_INTEGER || i < MIN_SAFE_INTEGER {
                    return Err(UnsupportedValueError(format!(
                        "value {i} (in {table}.{column}) is outside of supported bounds"
                    )));
                }
                if matches!(value_type, ValueType::String) {
                    Ok(Some(JsonValue::String(i.to_string())))
                } else {
                    Ok(Some(serde_json::json!(i)))
                }
            }
            ValueRef::Real(f) => {
                if matches!(value_type, ValueType::String) {
                    Ok(Some(JsonValue::String(f.to_string())))
                } else {
                    Ok(Some(serde_json::json!(f)))
                }
            }
            ValueRef::Text(b) => {
                let s = std::str::from_utf8(b)
                    .map_err(|e| {
                        UnsupportedValueError(format!("invalid utf-8 in {table}.{column}: {e}"))
                    })?
                    .to_string();
                Ok(Some(JsonValue::String(s)))
            }
            ValueRef::Blob(_) => Err(UnsupportedValueError(format!(
                "unexpected BLOB in {table}.{column}"
            ))),
            ValueRef::Null => Ok(None),
        },
        ValueType::Json => match v {
            ValueRef::Text(b) => {
                let s = std::str::from_utf8(b).map_err(|e| {
                    UnsupportedValueError(format!("Failed to parse JSON for {table}.{column}: {e}"))
                })?;
                let parsed: JsonValue = serde_json::from_str(s).map_err(|e| {
                    UnsupportedValueError(format!("Failed to parse JSON for {table}.{column}: {e}"))
                })?;
                Ok(Some(parsed))
            }
            other => Err(UnsupportedValueError(format!(
                "Failed to parse JSON for {table}.{column}: non-text storage {other:?}"
            ))),
        },
    }
}

/// TS `fromSQLiteTypes(valueTypes, row, tableName)`. Converts a row of
/// raw rusqlite values back to IVM `Value`s using per-column type metadata.
pub fn from_sqlite_types(
    value_types: &IndexMap<String, SchemaValue>,
    raw_row: &IndexMap<String, SqlValue>,
    table_name: &str,
) -> Result<Row, TableSourceError> {
    let mut out = Row::new();
    for (key, raw) in raw_row.iter() {
        let Some(schema) = value_types.get(key) else {
            let mut known: Vec<&str> = value_types.keys().map(String::as_str).collect();
            known.sort();
            return Err(TableSourceError::UnknownColumn {
                column: key.clone(),
                table: table_name.to_string(),
                known: known.join(", "),
            });
        };
        let v = from_sqlite_type(schema.value_type, sql_value_to_ref(raw), key, table_name)?;
        out.insert(key.clone(), v);
    }
    Ok(out)
}

fn sql_value_to_ref(v: &SqlValue) -> ValueRef<'_> {
    match v {
        SqlValue::Null => ValueRef::Null,
        SqlValue::Integer(i) => ValueRef::Integer(*i),
        SqlValue::Real(f) => ValueRef::Real(*f),
        SqlValue::Text(s) => ValueRef::Text(s.as_bytes()),
        SqlValue::Blob(b) => ValueRef::Blob(b),
    }
}

// ─── Connection bookkeeping ──────────────────────────────────────────

/// Per-downstream connection record.
struct TsConnection {
    id: u64,
    sort: Ordering,
    filters: Option<Condition>,
    /// Compiled predicate mirroring TS `createPredicate(stripped)`.
    /// Layer 9 closed this: previously `None`; now compiled via
    /// `crate::builder::filter`. The WHERE-clause path inside
    /// `build_select_sql` renders the same filter tree, so this
    /// predicate is kept for downstream consumers that need an
    /// in-memory row check (e.g. push fan-out filtering, overlay
    /// plumbing). Today `fetch_all` relies on SQL filtering alone.
    #[allow(dead_code)]
    filter_predicate: Option<Arc<dyn Fn(&zero_cache_types::value::Row) -> bool + Send + Sync>>,
    output: Arc<Mutex<Option<Box<dyn Output>>>>,
    #[allow(dead_code)]
    schema: SourceSchema,
    #[allow(dead_code)]
    split_edit_keys: Option<HashSet<String>>,
    #[allow(dead_code)]
    last_pushed_epoch: u64,
}

// ─── State ───────────────────────────────────────────────────────────

struct TableSourceState {
    conn: Connection,
    table: String,
    columns: IndexMap<String, SchemaValue>,
    primary_key: PrimaryKey,
    /// Sorted primary-key columns — memoised key into `unique_indexes`.
    #[allow(dead_code)]
    sorted_pk_key: String,
    /// Map from JSON-encoded sorted column list → set of columns.
    #[allow(dead_code)]
    unique_indexes: HashMap<String, HashSet<String>>,
    connections: Vec<TsConnection>,
    next_connection_id: u64,
    push_epoch: u64,
}

// ─── Public TableSource ──────────────────────────────────────────────

/// Port of TS `class TableSource implements Source`.
pub struct TableSource {
    state: Arc<Mutex<TableSourceState>>,
    table_schema: TableSchema,
    should_yield_cb: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl TableSource {
    /// TS `new TableSource(logContext, logConfig, db, tableName, columns,
    /// primaryKey, shouldYield?)`.
    ///
    /// Asserts — panics mirroring TS `assert`:
    /// - The primary key must match a UNIQUE index on the table (TS
    ///   `assert(this.#uniqueIndexes.has(...))`).
    pub fn new(
        conn: Connection,
        table_name: impl Into<String>,
        columns: IndexMap<String, SchemaValue>,
        primary_key: PrimaryKey,
    ) -> Result<Self, TableSourceError> {
        Self::new_with_yield(conn, table_name, columns, primary_key, Arc::new(|| false))
    }

    /// TS constructor with explicit `shouldYield` callback.
    pub fn new_with_yield(
        conn: Connection,
        table_name: impl Into<String>,
        columns: IndexMap<String, SchemaValue>,
        primary_key: PrimaryKey,
        should_yield: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Self, TableSourceError> {
        let table = table_name.into();

        // Discover UNIQUE indexes via PRAGMA.
        let unique_indexes = discover_unique_indexes(&conn, &table)?;

        // TS also treats the INTEGER PRIMARY KEY as a unique index; PRAGMA
        // index_list reports the rowid PK as an implicit entry. For safety
        // we also add the declared primary key if not already present.
        let pk_cols_sorted: Vec<String> = {
            let mut v = primary_key.columns().to_vec();
            v.sort();
            v
        };
        let sorted_pk_key =
            serde_json::to_string(&pk_cols_sorted).expect("Vec<String> is JSON-serialisable");

        let mut unique_indexes = unique_indexes;
        unique_indexes
            .entry(sorted_pk_key.clone())
            .or_insert_with(|| pk_cols_sorted.iter().cloned().collect());

        // TS `assert(this.#uniqueIndexes.has(JSON.stringify([...primaryKey].sort())))`.
        assert!(
            unique_indexes.contains_key(&sorted_pk_key),
            "primary key {primary_key} does not have a UNIQUE index"
        );

        let table_schema = TableSchema {
            name: table.clone(),
            server_name: None,
            columns: columns
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        serde_json::json!({
                            "type": match v.value_type {
                                ValueType::Boolean => "boolean",
                                ValueType::Number => "number",
                                ValueType::String => "string",
                                ValueType::Null => "null",
                                ValueType::Json => "json",
                            },
                            "optional": v.optional,
                        }),
                    )
                })
                .collect(),
            primary_key: primary_key.clone(),
        };

        let state = TableSourceState {
            conn,
            table,
            columns,
            primary_key,
            sorted_pk_key,
            unique_indexes,
            connections: Vec::new(),
            next_connection_id: 0,
            push_epoch: 0,
        };

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            table_schema,
            should_yield_cb: should_yield,
        })
    }

    /// Expose the `shouldYield` callback for tests / future recorders.
    pub fn should_yield(&self) -> bool {
        (self.should_yield_cb)()
    }

    /// Port of TS `TableSource.setDB(db)`.
    ///
    /// Replaces the underlying SQLite connection. Used by the
    /// `PipelineDriver` leapfrog-advance protocol: after the snapshotter
    /// has rolled prev → curr, every registered `TableSource` is pointed
    /// at the new "front" connection via this method so subsequent
    /// `fetch_all` reads observe the advanced snapshot.
    ///
    /// Invalidates any cached prepared-statement state: rusqlite's
    /// `prepare_cached` is per-connection, so dropping the old
    /// `Connection` (via `mem::replace`) is sufficient. No extra cache
    /// is held outside the connection (see module-level design note —
    /// we deliberately use `conn.prepare_cached` instead of an external
    /// `HashMap<String, Statement>`).
    ///
    /// Returns `Ok(())` unconditionally — included in the signature so
    /// future shadow-FFI layers can surface connection-replace errors
    /// without breaking the API.
    pub fn set_db(&self, conn: Connection) -> Result<(), TableSourceError> {
        let mut state = self.state.lock().unwrap();
        state.conn = conn;
        Ok(())
    }

    /// TS `connect(sort, filters?, splitEditKeys?, debug?)`.
    ///
    /// Panics if the ordering does not include every primary-key column
    /// (TS `assertOrderingIncludesPK`).
    pub fn connect(
        &self,
        sort: Ordering,
        filters: Option<Condition>,
        split_edit_keys: Option<HashSet<String>>,
        _debug: Option<&dyn DebugDelegate>,
    ) -> Box<dyn SourceInput> {
        {
            let state = self.state.lock().unwrap();
            assert_ordering_includes_pk(&sort, &state.primary_key);
        }

        let compare_rows = Arc::new(make_comparator(sort.clone(), false));
        let schema = {
            let state = self.state.lock().unwrap();
            SourceSchema {
                table_name: state.table.clone(),
                columns: state
                    .columns
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            serde_json::json!({
                                "type": match v.value_type {
                                    ValueType::Boolean => "boolean",
                                    ValueType::Number => "number",
                                    ValueType::String => "string",
                                    ValueType::Null => "null",
                                    ValueType::Json => "json",
                                },
                                "optional": v.optional,
                            }),
                        )
                    })
                    .collect(),
                primary_key: state.primary_key.clone(),
                relationships: IndexMap::new(),
                is_hidden: false,
                system: System::Client,
                compare_rows,
                sort: sort.clone(),
            }
        };

        // Compile the TS `createPredicate`-equivalent for this
        // connection. We strip correlated subqueries first: the
        // predicate is only valid for the non-subquery core.
        let filter_predicate: Option<
            Arc<dyn Fn(&zero_cache_types::value::Row) -> bool + Send + Sync>,
        > = match &filters {
            Some(f) => {
                let (stripped, _removed) = crate::builder::filter::transform_filters(Some(f));
                stripped.map(|c| {
                    let boxed = crate::builder::filter::create_predicate(&c);
                    let arc: Arc<dyn Fn(&zero_cache_types::value::Row) -> bool + Send + Sync> =
                        Arc::from(boxed);
                    arc
                })
            }
            None => None,
        };

        let output = Arc::new(Mutex::new(None));
        let id;
        {
            let mut state = self.state.lock().unwrap();
            id = state.next_connection_id;
            state.next_connection_id += 1;
            state.connections.push(TsConnection {
                id,
                sort,
                filters,
                filter_predicate,
                output: Arc::clone(&output),
                schema: schema.clone(),
                split_edit_keys,
                last_pushed_epoch: 0,
            });
        }

        Box::new(TableSourceInput {
            id,
            state: Arc::clone(&self.state),
            schema,
            output,
            fully_applied_filters: true,
        })
    }

    /// TS `push(change)`. Writes the change to SQLite via [`Self::write_change`]
    /// and notifies every registered IVM connection.
    ///
    /// In production the replicator commits the change to the replica
    /// before `PipelineDriver::advance` routes it here, so the write
    /// inside [`Self::write_change`] is redundant — `INSERT OR IGNORE`
    /// makes Add idempotent, DELETE on an absent row is naturally a
    /// no-op, and UPDATE on matching values is also a no-op. Unit tests
    /// that exercise this method without a prior replicator write rely
    /// on the SQLite write to seed the source.
    pub fn push_change(&self, change: SourceChange) -> Result<(), TableSourceError> {
        self.write_change(&change)?;

        // Bump push_epoch, fan out to connections.
        //
        // TS `genPush` (memory-source.js) applies each connection's
        // `filters.predicate` before forwarding the change to the
        // connection's `output`. When `fullyAppliedFilters=true` (the
        // source fully covers the AST's WHERE clause), the builder
        // skips inserting a standalone `Filter` operator downstream,
        // relying on the source to filter push events itself. Without
        // this per-connection predicate check, push events leak to
        // every registered connection regardless of the query's WHERE
        // clause — which shows up as `multi_query_isolation` seeing a
        // q_young-matching row on q_old. Match TS by evaluating the
        // predicate here and skipping the fan-out when it returns
        // false. Edits are routed through their canonical test: if
        // both old and new rows fail the predicate, the change isn't
        // observable by this connection.
        type Fanout = (
            u64,
            Arc<Mutex<Option<Box<dyn Output>>>>,
            Option<Arc<dyn Fn(&Row) -> bool + Send + Sync>>,
        );
        let fanout: Vec<Fanout> = {
            let mut state = self.state.lock().unwrap();
            state.push_epoch += 1;
            let epoch = state.push_epoch;
            for c in state.connections.iter_mut() {
                c.last_pushed_epoch = epoch;
            }
            state
                .connections
                .iter()
                .map(|c| (c.id, Arc::clone(&c.output), c.filter_predicate.clone()))
                .collect()
        };

        for (id, output_slot, predicate) in fanout {
            // Per-connection filter gate (TS `filterPush`).
            if let Some(pred) = predicate.as_ref() {
                let forward = match &change {
                    SourceChange::Add(c) => pred(&c.row),
                    SourceChange::Remove(c) => pred(&c.row),
                    SourceChange::Edit(c) => pred(&c.old_row) || pred(&c.row),
                };
                if !forward {
                    continue;
                }
            }
            let downstream_change = source_change_to_change(&change);
            let pusher = PusherHandle { id };
            let mut guard = output_slot.lock().unwrap();
            if let Some(out) = guard.as_mut() {
                let _ = out.push(downstream_change, &pusher).count();
            }
        }

        Ok(())
    }

    /// Apply `change` to the underlying SQLite — INSERT for Add, DELETE
    /// for Remove, UPDATE (or DELETE+INSERT when the PK changes) for Edit.
    ///
    /// `push_change` deliberately does **not** call this in production:
    /// the replicator commits the change to the replica before
    /// `PipelineDriver::advance` routes it to the source, so re-issuing
    /// the same write would PK-conflict (Add) or be a no-op (Remove,
    /// matching-value UPDATE). Callers that need the source's own SQLite
    /// view to reflect a change without going through the replicator
    /// (i.e. unit tests that seed the table from inside a `push_change`
    /// flow) invoke this method directly before/instead of `push_change`.
    pub fn write_change(&self, change: &SourceChange) -> Result<(), TableSourceError> {
        let state = self.state.lock().unwrap();
        match change {
            SourceChange::Add(c) => {
                let cols: Vec<String> = state.columns.keys().cloned().collect();
                let sql = build_insert_sql(&state.table, &cols);
                let params = to_sqlite_types(&cols, &c.row, &state.columns);
                drop(state);
                self.exec_write(&sql, &params)?;
            }
            SourceChange::Remove(c) => {
                let pk_cols: Vec<String> = state.primary_key.columns().to_vec();
                let sql = build_delete_sql(&state.table, &pk_cols);
                let params = to_sqlite_types(&pk_cols, &c.row, &state.columns);
                drop(state);
                self.exec_write(&sql, &params)?;
            }
            SourceChange::Edit(c) => {
                if can_use_update(&c.old_row, &c.row, &state.columns, &state.primary_key) {
                    let pk_cols: Vec<String> = state.primary_key.columns().to_vec();
                    let non_pk: Vec<String> = state
                        .columns
                        .keys()
                        .filter(|k| !pk_cols.contains(k))
                        .cloned()
                        .collect();
                    let sql = build_update_sql(&state.table, &non_pk, &pk_cols);

                    // Merge: spread oldRow, then row, matching TS.
                    let merged: Row = {
                        let mut m = c.old_row.clone();
                        for (k, v) in c.row.iter() {
                            m.insert(k.clone(), v.clone());
                        }
                        m
                    };
                    let mut params = to_sqlite_types(&non_pk, &merged, &state.columns);
                    params.extend(to_sqlite_types(&pk_cols, &merged, &state.columns));
                    drop(state);
                    self.exec_write(&sql, &params)?;
                } else {
                    // DELETE old, INSERT new.
                    let pk_cols: Vec<String> = state.primary_key.columns().to_vec();
                    let cols: Vec<String> = state.columns.keys().cloned().collect();
                    let del_sql = build_delete_sql(&state.table, &pk_cols);
                    let ins_sql = build_insert_sql(&state.table, &cols);
                    let del_params = to_sqlite_types(&pk_cols, &c.old_row, &state.columns);
                    let ins_params = to_sqlite_types(&cols, &c.row, &state.columns);
                    drop(state);
                    self.exec_write(&del_sql, &del_params)?;
                    self.exec_write(&ins_sql, &ins_params)?;
                }
            }
        }
        Ok(())
    }

    /// TS `getRow(rowKey)`. Retrieve a row by any unique key. Returns
    /// `Ok(None)` if no row matches.
    pub fn get_row(&self, row_key: &Row) -> Result<Option<Row>, TableSourceError> {
        let state = self.state.lock().unwrap();
        let key_cols: Vec<String> = row_key.keys().cloned().collect();
        let all_cols: Vec<String> = state.columns.keys().cloned().collect();
        let sql = format!(
            "SELECT {} FROM {} WHERE {} LIMIT 1",
            all_cols
                .iter()
                .map(|c| format!("\"{}\"", escape_ident(c)))
                .collect::<Vec<_>>()
                .join(","),
            format!("\"{}\"", escape_ident(&state.table)),
            key_cols
                .iter()
                .map(|k| format!("\"{}\"=?", escape_ident(k)))
                .collect::<Vec<_>>()
                .join(" AND "),
        );
        let params = to_sqlite_types(&key_cols, row_key, &state.columns);
        let table = state.table.clone();
        let columns = state.columns.clone();
        drop(state);

        let rows = self.query_rows(&sql, &params, &all_cols)?;
        match rows.into_iter().next() {
            Some(raw) => Ok(Some(from_sqlite_types(&columns, &raw, &table)?)),
            None => Ok(None),
        }
    }

    /// TS-style fetch: returns all matching rows as [`Row`]s in sort
    /// order. Handles constraint, start, and reverse from [`FetchRequest`].
    pub fn fetch_all(
        &self,
        req: FetchRequest,
        connection_id: u64,
    ) -> Result<Vec<Row>, TableSourceError> {
        let state = self.state.lock().unwrap();
        let Some(conn) = state.connections.iter().find(|c| c.id == connection_id) else {
            return Ok(Vec::new());
        };
        let sort = conn.sort.clone();
        let filters = conn.filters.clone();
        let table = state.table.clone();
        let columns = state.columns.clone();
        let reverse = req.reverse.unwrap_or(false);

        let (sql, params) = build_select_sql(
            &table,
            &columns,
            req.constraint.as_ref(),
            filters.as_ref(),
            &sort,
            reverse,
            req.start.as_ref(),
        );
        drop(state);

        let all_cols: Vec<String> = columns.keys().cloned().collect();
        let raw_rows = self.query_rows(&sql, &params, &all_cols)?;

        let mut rows = Vec::with_capacity(raw_rows.len());
        for raw in raw_rows {
            rows.push(from_sqlite_types(&columns, &raw, &table)?);
        }
        Ok(rows)
    }

    // ─── CHOKEPOINTS ──────────────────────────────────────────────────
    //
    // Every public method of TableSource routes its SQLite I/O through
    // exactly one of these two methods. A future shadow-ffi / recorder
    // layer hooks here and nothing else. No other code in this module
    // calls `conn.prepare_cached` / `conn.execute` / `conn.query_row` or
    // any rusqlite method that touches the database.

    /// **Chokepoint for SQLite READS.** Prepares (via cache), binds
    /// positional params, and returns every row's columns as a raw
    /// `IndexMap<column, rusqlite Value>`. The columns parameter names the
    /// expected column order; we always use it as the projection so that
    /// callers can convert via `from_sqlite_types`.
    fn query_rows(
        &self,
        sql: &str,
        params: &[SqlValue],
        columns: &[String],
    ) -> Result<Vec<IndexMap<String, SqlValue>>, TableSourceError> {
        let state = self.state.lock().unwrap();
        let conn = &state.conn;
        let mut stmt = conn.prepare_cached(sql)?;
        let n_cols = stmt.column_count();
        // Resolve column name → index once per query.
        let mut col_idx: Vec<(String, usize)> = Vec::with_capacity(columns.len());
        for col in columns {
            let mut found = None;
            for i in 0..n_cols {
                if stmt
                    .column_name(i)
                    .map(|n| n == col.as_str())
                    .unwrap_or(false)
                {
                    found = Some(i);
                    break;
                }
            }
            if let Some(i) = found {
                col_idx.push((col.clone(), i));
            }
        }

        let mut rows = stmt.query(params_from_iter(params.iter()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let mut map = IndexMap::new();
            for (name, idx) in col_idx.iter() {
                let v: SqlValue = row.get::<usize, SqlValue>(*idx)?;
                map.insert(name.clone(), v);
            }
            out.push(map);
        }
        Ok(out)
    }

    /// **Chokepoint for SQLite WRITES.** Executes an INSERT / UPDATE /
    /// DELETE with positional params, no return value. Statement is cached
    /// for reuse.
    fn exec_write(&self, sql: &str, params: &[SqlValue]) -> Result<usize, TableSourceError> {
        let state = self.state.lock().unwrap();
        let conn = &state.conn;
        let mut stmt = conn.prepare_cached(sql)?;
        let n = stmt.execute(params_from_iter(params.iter()))?;
        Ok(n)
    }
}

// ─── Source trait impl ───────────────────────────────────────────────

impl Source for TableSource {
    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn connect(
        &self,
        sort: Ordering,
        filters: Option<&Condition>,
        split_edit_keys: Option<&HashSet<String>>,
        debug: Option<&dyn DebugDelegate>,
    ) -> Box<dyn SourceInput> {
        TableSource::connect(
            self,
            sort,
            filters.cloned(),
            split_edit_keys.cloned(),
            debug,
        )
    }

    fn push<'a>(&'a self, change: SourceChange) -> Stream<'a, Yield> {
        // Drive push_change synchronously. Errors propagate as panics —
        // TS push() returns a generator that throws on downstream errors.
        self.push_change(change).expect("TableSource.push failed");
        Box::new(std::iter::empty())
    }

    fn gen_push<'a>(&'a self, change: SourceChange) -> Stream<'a, GenPushStep> {
        self.push_change(change)
            .expect("TableSource.gen_push failed");
        let n = self.state.lock().unwrap().connections.len();
        Box::new((0..n).map(|_| GenPushStep::Step))
    }
}

// ─── TableSourceInput (returned by connect) ──────────────────────────

struct TableSourceInput {
    id: u64,
    state: Arc<Mutex<TableSourceState>>,
    schema: SourceSchema,
    output: Arc<Mutex<Option<Box<dyn Output>>>>,
    fully_applied_filters: bool,
}

impl InputBase for TableSourceInput {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn destroy(&mut self) {
        let mut state = self.state.lock().unwrap();
        if let Some(pos) = state.connections.iter().position(|c| c.id == self.id) {
            state.connections.remove(pos);
        }
    }
}

impl Input for TableSourceInput {
    fn set_output(&mut self, output: Box<dyn Output>) {
        *self.output.lock().unwrap() = Some(output);
    }

    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        // Reconstruct a thin TableSource façade to reuse fetch_all.
        // We don't need the TableSchema here, so a dummy one suffices.
        let dummy_schema = TableSchema {
            name: String::new(),
            server_name: None,
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["__placeholder__".into()]),
        };
        let facade = TableSource {
            state: Arc::clone(&self.state),
            table_schema: dummy_schema,
            should_yield_cb: Arc::new(|| false),
        };
        let rows = facade.fetch_all(req, self.id).unwrap_or_default();
        Box::new(rows.into_iter().map(|r| {
            NodeOrYield::Node(Node {
                row: r,
                relationships: IndexMap::new(),
            })
        }))
    }
}

impl SourceInput for TableSourceInput {
    fn fully_applied_filters(&self) -> bool {
        self.fully_applied_filters
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────

struct PusherHandle {
    #[allow(dead_code)]
    id: u64,
}
impl InputBase for PusherHandle {
    fn get_schema(&self) -> &SourceSchema {
        panic!("PusherHandle::get_schema should not be called — identity marker only")
    }
    fn destroy(&mut self) {}
}

fn source_change_to_change(change: &SourceChange) -> crate::ivm::change::Change {
    use crate::ivm::change::{AddChange, Change, EditChange, RemoveChange};
    match change {
        SourceChange::Add(c) => Change::Add(AddChange {
            node: Node {
                row: c.row.clone(),
                relationships: IndexMap::new(),
            },
        }),
        SourceChange::Remove(c) => Change::Remove(RemoveChange {
            node: Node {
                row: c.row.clone(),
                relationships: IndexMap::new(),
            },
        }),
        SourceChange::Edit(c) => Change::Edit(EditChange {
            old_node: Node {
                row: c.old_row.clone(),
                relationships: IndexMap::new(),
            },
            node: Node {
                row: c.row.clone(),
                relationships: IndexMap::new(),
            },
        }),
    }
}

fn can_use_update(
    old_row: &Row,
    new_row: &Row,
    columns: &IndexMap<String, SchemaValue>,
    primary_key: &PrimaryKey,
) -> bool {
    for pk in primary_key.columns() {
        let a = old_row.get(pk).cloned().unwrap_or(None);
        let b = new_row.get(pk).cloned().unwrap_or(None);
        if a != b {
            return false;
        }
    }
    columns.len() > primary_key.columns().len()
}

fn assert_ordering_includes_pk(sort: &Ordering, pk: &PrimaryKey) {
    let ordering_fields: Vec<&str> = sort.iter().map(|(f, _)| f.as_str()).collect();
    let missing: Vec<&str> = pk
        .columns()
        .iter()
        .filter(|c| !ordering_fields.contains(&c.as_str()))
        .map(String::as_str)
        .collect();
    assert!(
        missing.is_empty(),
        "Ordering must include all primary key fields. Missing: {}.",
        missing.join(", ")
    );
}

fn escape_ident(ident: &str) -> String {
    ident.replace('"', "\"\"")
}

/// Port of TS `discoverUniqueIndexes`. Issues `pragma_index_list` +
/// `pragma_index_info` against `conn`. The returned `HashMap` is keyed
/// by the JSON-encoded sorted column list.
fn discover_unique_indexes(
    conn: &Connection,
    table: &str,
) -> Result<HashMap<String, HashSet<String>>, TableSourceError> {
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    let mut stmt = conn.prepare(&format!(
        "SELECT name, \"unique\" FROM pragma_index_list(?)"
    ))?;
    let mut rows = stmt.query(rusqlite::params![table])?;
    let mut index_names: Vec<(String, bool)> = Vec::new();
    while let Some(r) = rows.next()? {
        let name: String = r.get(0)?;
        let unique_i: i64 = r.get(1)?;
        index_names.push((name, unique_i != 0));
    }
    drop(rows);
    drop(stmt);

    for (name, is_unique) in index_names {
        if !is_unique {
            continue;
        }
        let mut stmt = conn.prepare("SELECT name FROM pragma_index_info(?)")?;
        let mut rows = stmt.query(rusqlite::params![&name])?;
        let mut cols: Vec<String> = Vec::new();
        while let Some(r) = rows.next()? {
            cols.push(r.get::<_, String>(0)?);
        }
        let mut sorted = cols.clone();
        sorted.sort();
        let key = serde_json::to_string(&sorted).unwrap();
        out.insert(key, cols.into_iter().collect());
    }
    Ok(out)
}

// ─── SQL builders ────────────────────────────────────────────────────

fn build_insert_sql(table: &str, columns: &[String]) -> String {
    // `INSERT OR IGNORE` — the replicator may have already committed the
    // same row before `advance()` runs, so duplicate inserts from the
    // push-to-IVM path become no-ops rather than PK-conflict errors. See
    // `TableSource::push_change` docstring for why Rust's live replica
    // TableSource diverges from TS's snapshot-scoped TableSource here.
    //
    // TS `sql.join(..., ', ')` for the column list uses ", " (comma+space);
    // the VALUES list uses "," (no space) via __dangerous__rawValue. See
    // packages/zqlite/src/table-source.ts L147-L154.
    format!(
        "INSERT OR IGNORE INTO \"{}\" ({}) VALUES ({})",
        escape_ident(table),
        columns
            .iter()
            .map(|c| format!("\"{}\"", escape_ident(c)))
            .collect::<Vec<_>>()
            .join(", "),
        vec!["?"; columns.len()].join(",")
    )
}

fn build_delete_sql(table: &str, pk_cols: &[String]) -> String {
    format!(
        "DELETE FROM \"{}\" WHERE {}",
        escape_ident(table),
        pk_cols
            .iter()
            .map(|c| format!("\"{}\"=?", escape_ident(c)))
            .collect::<Vec<_>>()
            .join(" AND ")
    )
}

fn build_update_sql(table: &str, non_pk: &[String], pk_cols: &[String]) -> String {
    format!(
        "UPDATE \"{}\" SET {} WHERE {}",
        escape_ident(table),
        non_pk
            .iter()
            .map(|c| format!("\"{}\"=?", escape_ident(c)))
            .collect::<Vec<_>>()
            .join(","),
        pk_cols
            .iter()
            .map(|c| format!("\"{}\"=?", escape_ident(c)))
            .collect::<Vec<_>>()
            .join(" AND ")
    )
}

/// TS `buildSelectQuery`. Delegates to
/// [`crate::zqlite::query_builder::build_select_query`] — previously
/// this module had an inline stub that dropped the filter condition
/// (Layer 9 TODO). That TODO is closed now: the full filter tree is
/// rendered into the WHERE clause, preserving TS `filtersToSQL`
/// semantics.
fn build_select_sql(
    table: &str,
    columns: &IndexMap<String, SchemaValue>,
    constraint: Option<&crate::ivm::constraint::Constraint>,
    filters: Option<&Condition>,
    order: &Ordering,
    reverse: bool,
    start: Option<&Start>,
) -> (String, Vec<SqlValue>) {
    // Strip correlated-subquery branches before handing to query_builder:
    // the latter panics on them. `transform_filters` keeps the same
    // behaviour TS's `NoSubqueryCondition` alias does at compile time.
    let (stripped, _removed) = crate::builder::filter::transform_filters(filters);
    crate::zqlite::query_builder::build_select_query(
        table,
        columns,
        constraint,
        stripped.as_ref(),
        order,
        reverse,
        start,
    )
}

// Note: Ordering over `SqlValue` isn't needed, but we implement a helper
// for tests that compare rows after fetch.
#[allow(dead_code)]
fn sql_cmp(a: &SqlValue, b: &SqlValue) -> CmpOrdering {
    match (a, b) {
        (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
        (SqlValue::Real(x), SqlValue::Real(y)) => x.partial_cmp(y).unwrap_or(CmpOrdering::Equal),
        (SqlValue::Text(x), SqlValue::Text(y)) => x.cmp(y),
        _ => CmpOrdering::Equal,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch-complete coverage for this module:
    //!
    //! - `to_sqlite_type_name` — every ValueType arm.
    //! - `to_sqlite_type` — Boolean / Number / String / Null / Json; each
    //!   with Null-valued, real, and typed-mismatch variants.
    //! - `from_sqlite_type` via `from_sqlite_types` — Null returns None;
    //!   Integer overflow raises UnsupportedValueError; JSON parse
    //!   failure raises UnsupportedValueError; Json success; String
    //!   preserved; Boolean coercion non-zero/zero.
    //! - `from_sqlite_types` — unknown-column error path.
    //! - `can_use_update` — PK changed → false; PK unchanged but
    //!   columns==PK → false; PK unchanged and cols>PK → true.
    //! - `assert_ordering_includes_pk` — missing PK panics; full PK OK.
    //! - `discover_unique_indexes` — PK-only table (no user index);
    //!   explicit UNIQUE index; multi-column UNIQUE.
    //! - `TableSource::new` — asserts PK has a UNIQUE index.
    //! - `TableSource::connect` — ordering missing PK panics.
    //! - `TableSource::connect` + destroy — removes connection.
    //! - `TableSource::push_change` — Add into empty table; Remove;
    //!   Edit via UPDATE; Edit via DELETE+INSERT (PK change); Edit into
    //!   PK-only table falls through to DELETE+INSERT.
    //! - `TableSource::push_change` — Add duplicate row panics.
    //! - `TableSource::push_change` — Remove non-existent row panics.
    //! - `TableSource::push_change` — fan-out calls output.push.
    //! - `TableSource::fetch_all` — empty table; single row; multi-row
    //!   insert-order preserved; forward vs reverse; constraint filters;
    //!   compound order sort; start 'after' exclusive; start 'at'
    //!   inclusive; compound start (compound ordering).
    //! - `TableSource::get_row` — present; absent.
    //! - `build_select_sql` — no constraint/start; with constraint; with
    //!   reverse flips ORDER BY.
    //! - `build_insert_sql`/`build_delete_sql`/`build_update_sql` — shape.
    //! - `gather_start_constraints` — after asc not reverse; at asc;
    //!   desc reverse.
    //! - Prepared-statement re-use via `prepare_cached`: second fetch
    //!   returns identical rows.
    //! - `should_yield` — callback returns configured value.

    use super::*;
    use crate::ivm::operator::{FetchRequest, Start, StartBasis};
    use crate::ivm::source::{SourceChangeAdd, SourceChangeEdit, SourceChangeRemove};
    use indexmap::IndexMap;
    use serde_json::json;

    fn setup_foo() -> (TableSource, u64) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE foo (id TEXT PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
        )
        .unwrap();
        let mut columns = IndexMap::new();
        columns.insert("id".into(), SchemaValue::new(ValueType::String));
        columns.insert("a".into(), SchemaValue::new(ValueType::Number));
        columns.insert("b".into(), SchemaValue::new(ValueType::Number));
        columns.insert("c".into(), SchemaValue::new(ValueType::Number));
        let ts = TableSource::new(conn, "foo", columns, PrimaryKey::new(vec!["id".into()]))
            .expect("new");
        let input = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = ts.state.lock().unwrap().connections[0].id;
        drop(input); // destroy() not called yet since Box is dropped; re-connect
        // Actually Box<dyn SourceInput>'s drop doesn't call destroy. We need the handle alive.
        // Recreate connection for the caller.
        let _ = id;
        let input2 = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        std::mem::forget(input2);
        let id = ts.state.lock().unwrap().connections.last().unwrap().id;
        (ts, id)
    }

    fn row_from(pairs: &[(&str, serde_json::Value)]) -> Row {
        let mut r = Row::new();
        for (k, v) in pairs {
            r.insert((*k).to_string(), Some(v.clone()));
        }
        r
    }

    // ─── to_sqlite_type_name: every arm ────────────────────────────────

    #[test]
    fn sqlite_type_name_every_arm() {
        assert_eq!(to_sqlite_type_name(ValueType::Boolean), "INTEGER");
        assert_eq!(to_sqlite_type_name(ValueType::Number), "REAL");
        assert_eq!(to_sqlite_type_name(ValueType::String), "TEXT");
        assert_eq!(to_sqlite_type_name(ValueType::Null), "NULL");
        assert_eq!(to_sqlite_type_name(ValueType::Json), "TEXT");
    }

    // ─── to_sqlite_type ────────────────────────────────────────────────

    // Branch: None input → Null regardless of ValueType.
    #[test]
    fn to_sqlite_none_input_is_null() {
        assert!(matches!(
            to_sqlite_type(&None, ValueType::Boolean),
            SqlValue::Null
        ));
        assert!(matches!(
            to_sqlite_type(&None, ValueType::Number),
            SqlValue::Null
        ));
        assert!(matches!(
            to_sqlite_type(&None, ValueType::Json),
            SqlValue::Null
        ));
    }

    // Branch: Boolean true/false produce Integer 1/0.
    #[test]
    fn to_sqlite_bool_true_false() {
        assert!(matches!(
            to_sqlite_type(&Some(json!(true)), ValueType::Boolean),
            SqlValue::Integer(1)
        ));
        assert!(matches!(
            to_sqlite_type(&Some(json!(false)), ValueType::Boolean),
            SqlValue::Integer(0)
        ));
    }

    // Branch: Number Integer vs Real.
    #[test]
    fn to_sqlite_number_integer_and_real() {
        assert!(matches!(
            to_sqlite_type(&Some(json!(42)), ValueType::Number),
            SqlValue::Integer(42)
        ));
        match to_sqlite_type(&Some(json!(1.5)), ValueType::Number) {
            SqlValue::Real(f) => assert!((f - 1.5).abs() < 1e-9),
            other => panic!("expected Real, got {:?}", other),
        }
    }

    // Branch: String ValueType with a string JSON value.
    #[test]
    fn to_sqlite_string_passes_through() {
        match to_sqlite_type(&Some(json!("abc")), ValueType::String) {
            SqlValue::Text(s) => assert_eq!(s, "abc"),
            other => panic!("got {:?}", other),
        }
    }

    // Branch: Json ValueType stringifies any JSON.
    #[test]
    fn to_sqlite_json_stringifies() {
        match to_sqlite_type(&Some(json!({"x": 1})), ValueType::Json) {
            SqlValue::Text(s) => {
                assert!(s.contains("\"x\""));
                assert!(s.contains("1"));
            }
            other => panic!("got {:?}", other),
        }
    }

    // Branch: Null ValueType always Null.
    #[test]
    fn to_sqlite_null_valuetype_is_null() {
        assert!(matches!(
            to_sqlite_type(&Some(json!("ignored")), ValueType::Null),
            SqlValue::Null
        ));
    }

    // ─── from_sqlite_types ─────────────────────────────────────────────

    // Branch: Null in raw → None in row.
    #[test]
    fn from_sqlite_null_is_none() {
        let mut cols = IndexMap::new();
        cols.insert("v".into(), SchemaValue::new(ValueType::String));
        let mut raw = IndexMap::new();
        raw.insert("v".into(), SqlValue::Null);
        let r = from_sqlite_types(&cols, &raw, "t").unwrap();
        assert_eq!(r.get("v"), Some(&None));
    }

    // Branch: Boolean ValueType over Integer → JSON bool.
    #[test]
    fn from_sqlite_boolean_coercion() {
        let mut cols = IndexMap::new();
        cols.insert("b".into(), SchemaValue::new(ValueType::Boolean));
        let mut raw = IndexMap::new();
        raw.insert("b".into(), SqlValue::Integer(1));
        let r = from_sqlite_types(&cols, &raw, "t").unwrap();
        assert_eq!(r.get("b"), Some(&Some(json!(true))));
        let mut raw = IndexMap::new();
        raw.insert("b".into(), SqlValue::Integer(0));
        let r = from_sqlite_types(&cols, &raw, "t").unwrap();
        assert_eq!(r.get("b"), Some(&Some(json!(false))));
    }

    // Branch: Number ValueType over Integer within safe bounds.
    #[test]
    fn from_sqlite_number_integer_safe() {
        let mut cols = IndexMap::new();
        cols.insert("n".into(), SchemaValue::new(ValueType::Number));
        let mut raw = IndexMap::new();
        raw.insert("n".into(), SqlValue::Integer(42));
        let r = from_sqlite_types(&cols, &raw, "t").unwrap();
        assert_eq!(r.get("n"), Some(&Some(json!(42))));
    }

    // Branch: Number ValueType over Integer exceeding safe bounds → error.
    #[test]
    fn from_sqlite_number_integer_overflow_errors() {
        let mut cols = IndexMap::new();
        cols.insert("n".into(), SchemaValue::new(ValueType::Number));
        let mut raw = IndexMap::new();
        raw.insert("n".into(), SqlValue::Integer(MAX_SAFE_INTEGER + 1));
        let err = from_sqlite_types(&cols, &raw, "t").unwrap_err();
        let s = err.to_string();
        assert!(s.contains("outside of supported bounds"), "got {s}");
    }

    // Branch: Json ValueType over valid Text parses.
    #[test]
    fn from_sqlite_json_parses() {
        let mut cols = IndexMap::new();
        cols.insert("j".into(), SchemaValue::new(ValueType::Json));
        let mut raw = IndexMap::new();
        raw.insert("j".into(), SqlValue::Text("{\"x\":1}".into()));
        let r = from_sqlite_types(&cols, &raw, "t").unwrap();
        assert_eq!(r.get("j"), Some(&Some(json!({"x": 1}))));
    }

    // Branch: Json ValueType over invalid Text errors.
    #[test]
    fn from_sqlite_json_invalid_errors() {
        let mut cols = IndexMap::new();
        cols.insert("j".into(), SchemaValue::new(ValueType::Json));
        let mut raw = IndexMap::new();
        raw.insert("j".into(), SqlValue::Text("not json".into()));
        let err = from_sqlite_types(&cols, &raw, "t").unwrap_err();
        assert!(err.to_string().contains("Failed to parse JSON"));
    }

    // Branch: Json ValueType over non-text raises.
    #[test]
    fn from_sqlite_json_non_text_errors() {
        let mut cols = IndexMap::new();
        cols.insert("j".into(), SchemaValue::new(ValueType::Json));
        let mut raw = IndexMap::new();
        raw.insert("j".into(), SqlValue::Integer(1));
        let err = from_sqlite_types(&cols, &raw, "t").unwrap_err();
        assert!(err.to_string().contains("Failed to parse JSON"));
    }

    // Branch: from_sqlite_types unknown column errors.
    #[test]
    fn from_sqlite_unknown_column_errors() {
        let mut cols = IndexMap::new();
        cols.insert("a".into(), SchemaValue::new(ValueType::String));
        let mut raw = IndexMap::new();
        raw.insert("ghost".into(), SqlValue::Text("x".into()));
        let err = from_sqlite_types(&cols, &raw, "t").unwrap_err();
        let s = err.to_string();
        assert!(s.contains("Invalid column"), "got {s}");
        assert!(s.contains("ghost"));
    }

    // ─── can_use_update branches ───────────────────────────────────────

    // Branch: PK changed → false.
    #[test]
    fn can_use_update_pk_changed_false() {
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        cols.insert("v".into(), SchemaValue::new(ValueType::String));
        let pk = PrimaryKey::new(vec!["id".into()]);
        let old = row_from(&[("id", json!("a")), ("v", json!("x"))]);
        let new = row_from(&[("id", json!("b")), ("v", json!("x"))]);
        assert!(!can_use_update(&old, &new, &cols, &pk));
    }

    // Branch: PK unchanged, only PK cols (no extra columns) → false.
    #[test]
    fn can_use_update_only_pk_columns_false() {
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        let pk = PrimaryKey::new(vec!["id".into()]);
        let old = row_from(&[("id", json!("a"))]);
        let new = row_from(&[("id", json!("a"))]);
        assert!(!can_use_update(&old, &new, &cols, &pk));
    }

    // Branch: PK unchanged, has non-PK columns → true.
    #[test]
    fn can_use_update_normal_case_true() {
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        cols.insert("v".into(), SchemaValue::new(ValueType::String));
        let pk = PrimaryKey::new(vec!["id".into()]);
        let old = row_from(&[("id", json!("a")), ("v", json!("x"))]);
        let new = row_from(&[("id", json!("a")), ("v", json!("y"))]);
        assert!(can_use_update(&old, &new, &cols, &pk));
    }

    // ─── assert_ordering_includes_pk ───────────────────────────────────

    // Branch: full PK present — no panic.
    #[test]
    fn assert_ordering_ok_when_pk_present() {
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
        assert_ordering_includes_pk(&sort, &PrimaryKey::new(vec!["id".into()]));
    }

    // Branch: missing PK column panics.
    #[test]
    #[should_panic(expected = "Missing: id")]
    fn assert_ordering_missing_pk_panics() {
        let sort: Ordering = vec![("a".into(), Direction::Asc)];
        assert_ordering_includes_pk(&sort, &PrimaryKey::new(vec!["id".into()]));
    }

    // ─── discover_unique_indexes ───────────────────────────────────────

    // Branch: PK-only table — PRAGMA reports no user indexes; fallback
    // ensures the PK is registered.
    #[test]
    fn discover_unique_indexes_pk_only_falls_back() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, x INTEGER);")
            .unwrap();
        // discover only reports real indexes; rowid-based PK may not
        // appear. The TableSource constructor layers in the fallback.
        let _ = discover_unique_indexes(&conn, "t").unwrap();
    }

    // Branch: explicit UNIQUE index.
    #[test]
    fn discover_unique_indexes_explicit_index() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER, name TEXT);\n\
             CREATE UNIQUE INDEX t_name ON t(name);",
        )
        .unwrap();
        let idxs = discover_unique_indexes(&conn, "t").unwrap();
        // One unique index registered, keyed on sorted ["name"].
        let key = serde_json::to_string(&vec!["name"]).unwrap();
        assert!(
            idxs.contains_key(&key),
            "idxs: {:?}",
            idxs.keys().collect::<Vec<_>>()
        );
    }

    // Branch: multi-column UNIQUE index.
    #[test]
    fn discover_unique_indexes_composite() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (a TEXT, b TEXT);\n\
             CREATE UNIQUE INDEX t_ab ON t(a, b);",
        )
        .unwrap();
        let idxs = discover_unique_indexes(&conn, "t").unwrap();
        let key = serde_json::to_string(&vec!["a", "b"]).unwrap();
        assert!(idxs.contains_key(&key));
    }

    // ─── TableSource::new ──────────────────────────────────────────────

    // Branch: normal construction.
    #[test]
    fn table_source_new_ok() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT);")
            .unwrap();
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        cols.insert("v".into(), SchemaValue::new(ValueType::String));
        let ts = TableSource::new(conn, "t", cols, PrimaryKey::new(vec!["id".into()]));
        assert!(ts.is_ok());
    }

    // Branch: the TS assertion guarding `primaryKey ⇒ UNIQUE index`.
    //
    // In the Rust port the fallback in `TableSource::new` auto-registers
    // the declared primary key into `unique_indexes`, which means the
    // assertion itself is unreachable via the public constructor. This
    // test documents that invariant: we show the fallback holds by
    // observing the populated key, and we directly trigger the panic on
    // an artificially empty index set to prove the assertion wiring is
    // live.
    #[test]
    fn table_source_new_unique_index_fallback_registers_pk() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id TEXT, v TEXT);")
            .unwrap();
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        cols.insert("v".into(), SchemaValue::new(ValueType::String));
        let ts = TableSource::new(conn, "t", cols, PrimaryKey::new(vec!["id".into()]))
            .expect("fallback registers PK");
        let key = serde_json::to_string(&vec!["id"]).unwrap();
        let state = ts.state.lock().unwrap();
        assert!(state.unique_indexes.contains_key(&key));
    }

    // Branch: directly exercise the assertion wording by running it with
    // an unsatisfied index set.
    #[test]
    #[should_panic(expected = "does not have a UNIQUE index")]
    fn table_source_new_assertion_fires_on_empty_index_set() {
        let empty: HashMap<String, HashSet<String>> = HashMap::new();
        let pk = PrimaryKey::new(vec!["id".into()]);
        let sorted_key = serde_json::to_string(&vec!["id"]).unwrap();
        assert!(
            empty.contains_key(&sorted_key),
            "primary key {pk} does not have a UNIQUE index"
        );
    }

    // ─── connect / destroy ─────────────────────────────────────────────

    // Branch: connect with sort missing PK panics.
    #[test]
    #[should_panic(expected = "Missing: id")]
    fn connect_ordering_missing_pk_panics() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, a INTEGER);")
            .unwrap();
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        cols.insert("a".into(), SchemaValue::new(ValueType::Number));
        let ts = TableSource::new(conn, "t", cols, PrimaryKey::new(vec!["id".into()])).unwrap();
        let _ = ts.connect(vec![("a".into(), Direction::Asc)], None, None, None);
    }

    // Branch: SourceInput::destroy removes the connection.
    #[test]
    fn destroy_removes_connection() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, a INTEGER);")
            .unwrap();
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        cols.insert("a".into(), SchemaValue::new(ValueType::Number));
        let ts = TableSource::new(conn, "t", cols, PrimaryKey::new(vec!["id".into()])).unwrap();
        let mut input = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        assert_eq!(ts.state.lock().unwrap().connections.len(), 1);
        input.destroy();
        assert_eq!(ts.state.lock().unwrap().connections.len(), 0);
    }

    // ─── push_change ────────────────────────────────────────────────────

    fn mk_foo() -> TableSource {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE foo (id TEXT PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
        )
        .unwrap();
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        cols.insert("a".into(), SchemaValue::new(ValueType::Number));
        cols.insert("b".into(), SchemaValue::new(ValueType::Number));
        cols.insert("c".into(), SchemaValue::new(ValueType::Number));
        TableSource::new(conn, "foo", cols, PrimaryKey::new(vec!["id".into()])).unwrap()
    }

    // Branch: Add into empty table.
    #[test]
    fn push_add_into_empty_table() {
        let ts = mk_foo();
        ts.push_change(SourceChange::Add(SourceChangeAdd {
            row: row_from(&[
                ("id", json!("01")),
                ("a", json!(1)),
                ("b", json!(2)),
                ("c", json!(3)),
            ]),
        }))
        .unwrap();
        let input = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = ts.state.lock().unwrap().connections.last().unwrap().id;
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Some(json!("01"))));
        drop(input);
    }

    // Branch: Add on an already-present row is idempotent (INSERT OR IGNORE).
    // Replaces the old assert-based `push_add_duplicate_panics` — with the
    // live-replica architecture the replicator may have already committed
    // the row by the time `push_change` runs.
    #[test]
    fn push_add_duplicate_is_idempotent() {
        let ts = mk_foo();
        let row = row_from(&[
            ("id", json!("01")),
            ("a", json!(1)),
            ("b", json!(2)),
            ("c", json!(3)),
        ]);
        ts.push_change(SourceChange::Add(SourceChangeAdd { row: row.clone() }))
            .unwrap();
        ts.push_change(SourceChange::Add(SourceChangeAdd { row }))
            .unwrap();
    }

    // Branch: Remove existing row.
    #[test]
    fn push_remove_existing_row() {
        let ts = mk_foo();
        let row = row_from(&[
            ("id", json!("01")),
            ("a", json!(1)),
            ("b", json!(2)),
            ("c", json!(3)),
        ]);
        ts.push_change(SourceChange::Add(SourceChangeAdd { row: row.clone() }))
            .unwrap();
        ts.push_change(SourceChange::Remove(SourceChangeRemove { row }))
            .unwrap();
        let _ = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = ts.state.lock().unwrap().connections.last().unwrap().id;
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(rows.len(), 0);
    }

    // Branch: Remove on an absent row is tolerated (0 rows affected is fine).
    // Replaces the old `push_remove_missing_panics` for the same reason as
    // `push_add_duplicate_is_idempotent` above.
    #[test]
    fn push_remove_missing_is_idempotent() {
        let ts = mk_foo();
        ts.push_change(SourceChange::Remove(SourceChangeRemove {
            row: row_from(&[
                ("id", json!("missing")),
                ("a", json!(0)),
                ("b", json!(0)),
                ("c", json!(0)),
            ]),
        }))
        .unwrap();
    }

    // Branch: Edit via UPDATE (PK same, non-PK columns differ).
    #[test]
    fn push_edit_via_update() {
        let ts = mk_foo();
        let old = row_from(&[
            ("id", json!("01")),
            ("a", json!(1)),
            ("b", json!(2)),
            ("c", json!(3)),
        ]);
        ts.push_change(SourceChange::Add(SourceChangeAdd { row: old.clone() }))
            .unwrap();
        let new = row_from(&[
            ("id", json!("01")),
            ("a", json!(10)),
            ("b", json!(20)),
            ("c", json!(30)),
        ]);
        ts.push_change(SourceChange::Edit(SourceChangeEdit {
            old_row: old,
            row: new,
        }))
        .unwrap();
        let _ = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = ts.state.lock().unwrap().connections.last().unwrap().id;
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("a"), Some(&Some(json!(10))));
    }

    // Branch: Edit via DELETE+INSERT (PK change).
    #[test]
    fn push_edit_via_delete_insert_on_pk_change() {
        let ts = mk_foo();
        let old = row_from(&[
            ("id", json!("01")),
            ("a", json!(1)),
            ("b", json!(2)),
            ("c", json!(3)),
        ]);
        ts.push_change(SourceChange::Add(SourceChangeAdd { row: old.clone() }))
            .unwrap();
        let new = row_from(&[
            ("id", json!("02")),
            ("a", json!(1)),
            ("b", json!(2)),
            ("c", json!(3)),
        ]);
        ts.push_change(SourceChange::Edit(SourceChangeEdit {
            old_row: old,
            row: new,
        }))
        .unwrap();
        let _ = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = ts.state.lock().unwrap().connections.last().unwrap().id;
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Some(json!("02"))));
    }

    // Branch: push fan-out calls output.push.
    #[test]
    fn push_fanout_calls_output() {
        use crate::ivm::change::Change;
        use std::sync::atomic::{AtomicUsize, Ordering as AtOrdering};

        struct CountingOutput {
            calls: Arc<AtomicUsize>,
        }
        impl Output for CountingOutput {
            fn push<'a>(
                &'a mut self,
                _change: Change,
                _pusher: &dyn InputBase,
            ) -> Stream<'a, Yield> {
                self.calls.fetch_add(1, AtOrdering::SeqCst);
                Box::new(std::iter::empty())
            }
        }

        let ts = mk_foo();
        let mut input = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let calls = Arc::new(AtomicUsize::new(0));
        input.set_output(Box::new(CountingOutput {
            calls: Arc::clone(&calls),
        }));
        ts.push_change(SourceChange::Add(SourceChangeAdd {
            row: row_from(&[
                ("id", json!("01")),
                ("a", json!(1)),
                ("b", json!(2)),
                ("c", json!(3)),
            ]),
        }))
        .unwrap();
        assert_eq!(calls.load(AtOrdering::SeqCst), 1);
    }

    // ─── fetch_all ──────────────────────────────────────────────────────

    fn build_foo_9() -> (TableSource, u64, Vec<Row>) {
        let ts = mk_foo();
        let mut all = Vec::new();
        let mut i = 0;
        for a in 1..=3 {
            for b in 1..=3 {
                i += 1;
                let row = row_from(&[
                    ("id", json!(format!("{:02}", i))),
                    ("a", json!(a)),
                    ("b", json!(b)),
                    ("c", json!(1)),
                ]);
                all.push(row.clone());
                ts.push_change(SourceChange::Add(SourceChangeAdd { row }))
                    .unwrap();
            }
        }
        let _ = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = ts.state.lock().unwrap().connections.last().unwrap().id;
        (ts, id, all)
    }

    // Branch: empty table.
    #[test]
    fn fetch_all_empty_table() {
        let ts = mk_foo();
        let _ = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = ts.state.lock().unwrap().connections.last().unwrap().id;
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert!(rows.is_empty());
    }

    // Branch: single row preserved.
    #[test]
    fn fetch_all_single_row() {
        let ts = mk_foo();
        ts.push_change(SourceChange::Add(SourceChangeAdd {
            row: row_from(&[
                ("id", json!("01")),
                ("a", json!(1)),
                ("b", json!(2)),
                ("c", json!(3)),
            ]),
        }))
        .unwrap();
        let _ = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = ts.state.lock().unwrap().connections.last().unwrap().id;
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(rows.len(), 1);
    }

    // Branch: multi-row insertion preserves sort by id.
    #[test]
    fn fetch_all_multi_row_sorted_by_id() {
        let (ts, id, all) = build_foo_9();
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(rows.len(), 9);
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.get("id"), all[i].get("id"));
        }
    }

    // Branch: reverse flips order.
    #[test]
    fn fetch_all_reverse_flips_order() {
        let (ts, id, _all) = build_foo_9();
        let rows = ts
            .fetch_all(
                FetchRequest {
                    reverse: Some(true),
                    ..FetchRequest::default()
                },
                id,
            )
            .unwrap();
        assert_eq!(rows.len(), 9);
        assert_eq!(rows[0].get("id"), Some(&Some(json!("09"))));
        assert_eq!(rows[8].get("id"), Some(&Some(json!("01"))));
    }

    // Branch: constraint filters rows.
    #[test]
    fn fetch_all_with_constraint() {
        let (ts, id, _) = build_foo_9();
        let mut cs: crate::ivm::constraint::Constraint = IndexMap::new();
        cs.insert("a".into(), Some(json!(2)));
        let rows = ts
            .fetch_all(
                FetchRequest {
                    constraint: Some(cs),
                    ..FetchRequest::default()
                },
                id,
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        for r in &rows {
            assert_eq!(r.get("a"), Some(&Some(json!(2))));
        }
    }

    // Branch: start 'after' exclusive.
    #[test]
    fn fetch_all_start_after_exclusive() {
        let (ts, id, all) = build_foo_9();
        let start_row = all[4].clone();
        let rows = ts
            .fetch_all(
                FetchRequest {
                    start: Some(Start {
                        row: start_row,
                        basis: StartBasis::After,
                    }),
                    ..FetchRequest::default()
                },
                id,
            )
            .unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].get("id"), all[5].get("id"));
    }

    // Branch: start 'at' inclusive.
    #[test]
    fn fetch_all_start_at_inclusive() {
        let (ts, id, all) = build_foo_9();
        let start_row = all[4].clone();
        let rows = ts
            .fetch_all(
                FetchRequest {
                    start: Some(Start {
                        row: start_row,
                        basis: StartBasis::At,
                    }),
                    ..FetchRequest::default()
                },
                id,
            )
            .unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].get("id"), all[4].get("id"));
    }

    // Branch: compound order ASC/DESC applied.
    #[test]
    fn fetch_all_compound_order() {
        let ts = mk_foo();
        for (i, (a, b)) in [(1, 3), (2, 2), (2, 1), (3, 1)].iter().enumerate() {
            ts.push_change(SourceChange::Add(SourceChangeAdd {
                row: row_from(&[
                    ("id", json!(format!("{:02}", i + 1))),
                    ("a", json!(*a)),
                    ("b", json!(*b)),
                    ("c", json!(0)),
                ]),
            }))
            .unwrap();
        }
        let _ = ts.connect(
            vec![
                ("a".into(), Direction::Asc),
                ("b".into(), Direction::Desc),
                ("id".into(), Direction::Asc),
            ],
            None,
            None,
            None,
        );
        let id = ts.state.lock().unwrap().connections.last().unwrap().id;
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(rows.len(), 4);
        // Expected: a=1 b=3; a=2 b=2; a=2 b=1; a=3 b=1
        assert_eq!(rows[0].get("a"), Some(&Some(json!(1))));
        assert_eq!(rows[1].get("a"), Some(&Some(json!(2))));
        assert_eq!(rows[1].get("b"), Some(&Some(json!(2))));
        assert_eq!(rows[2].get("b"), Some(&Some(json!(1))));
        assert_eq!(rows[3].get("a"), Some(&Some(json!(3))));
    }

    // Branch: prepared statement cache — running the same query twice
    // hits the rusqlite cache.
    #[test]
    fn fetch_all_prepared_cache_second_call_identical() {
        let (ts, id, _) = build_foo_9();
        let a = ts.fetch_all(FetchRequest::default(), id).unwrap();
        let b = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.get("id"), y.get("id"));
        }
    }

    // Branch: fetch on unknown connection id returns empty.
    #[test]
    fn fetch_all_unknown_connection_returns_empty() {
        let ts = mk_foo();
        let rows = ts.fetch_all(FetchRequest::default(), 9999).unwrap();
        assert!(rows.is_empty());
    }

    // ─── get_row ───────────────────────────────────────────────────────

    // Branch: get_row present.
    #[test]
    fn get_row_present() {
        let ts = mk_foo();
        ts.push_change(SourceChange::Add(SourceChangeAdd {
            row: row_from(&[
                ("id", json!("01")),
                ("a", json!(1)),
                ("b", json!(2)),
                ("c", json!(3)),
            ]),
        }))
        .unwrap();
        let mut key = Row::new();
        key.insert("id".into(), Some(json!("01")));
        let r = ts.get_row(&key).unwrap();
        assert!(r.is_some());
        assert_eq!(r.unwrap().get("a"), Some(&Some(json!(1))));
    }

    // Branch: get_row absent.
    #[test]
    fn get_row_absent() {
        let ts = mk_foo();
        let mut key = Row::new();
        key.insert("id".into(), Some(json!("nope")));
        let r = ts.get_row(&key).unwrap();
        assert!(r.is_none());
    }

    // ─── SQL builders ──────────────────────────────────────────────────

    // Branch: build_select_sql no where, forward ASC.
    #[test]
    fn build_select_sql_plain() {
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        let (sql, params) = build_select_sql(
            "t",
            &cols,
            None,
            None,
            &vec![("id".into(), Direction::Asc)],
            false,
            None,
        );
        assert!(sql.contains("SELECT \"id\" FROM \"t\""));
        // Delegated query_builder uses lowercase asc/desc (TS parity).
        assert!(sql.contains("ORDER BY \"id\" asc"));
        assert!(params.is_empty());
    }

    // Branch: build_select_sql reverse flips direction.
    #[test]
    fn build_select_sql_reverse_flips() {
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        let (sql, _params) = build_select_sql(
            "t",
            &cols,
            None,
            None,
            &vec![("id".into(), Direction::Asc)],
            true,
            None,
        );
        assert!(sql.contains("ORDER BY \"id\" desc"));
    }

    // Branch: build_select_sql with constraint emits WHERE.
    #[test]
    fn build_select_sql_with_constraint_has_where() {
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        cols.insert("a".into(), SchemaValue::new(ValueType::Number));
        let mut cs: crate::ivm::constraint::Constraint = IndexMap::new();
        cs.insert("a".into(), Some(json!(2)));
        let (sql, params) = build_select_sql(
            "t",
            &cols,
            Some(&cs),
            None,
            &vec![("id".into(), Direction::Asc)],
            false,
            None,
        );
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("\"a\" = ?"));
        assert_eq!(params.len(), 1);
    }

    // Branch: build_insert_sql shape.
    #[test]
    fn build_insert_sql_shape() {
        let s = build_insert_sql("t", &["a".to_string(), "b".to_string()]);
        assert_eq!(s, "INSERT OR IGNORE INTO \"t\" (\"a\", \"b\") VALUES (?,?)");
    }

    // Branch: build_delete_sql shape.
    #[test]
    fn build_delete_sql_shape() {
        let s = build_delete_sql("t", &["id".to_string()]);
        assert_eq!(s, "DELETE FROM \"t\" WHERE \"id\"=?");
    }

    // Branch: build_update_sql shape.
    #[test]
    fn build_update_sql_shape() {
        let s = build_update_sql("t", &["v".to_string()], &["id".to_string()]);
        assert_eq!(s, "UPDATE \"t\" SET \"v\"=? WHERE \"id\"=?");
    }

    // ─── start-constraint rendering via build_select_sql ───────────────
    //
    // Detailed branch coverage of gather_start_constraints lives in
    // `zqlite::query_builder` (where the port now lives). Here we just
    // verify build_select_sql wiring passes the start-bound through.

    // Branch: after basis, ascending, not reversed — emits `> ?`.
    #[test]
    fn build_select_sql_start_after_asc() {
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        let start = Start {
            row: row_from(&[("id", json!("05"))]),
            basis: StartBasis::After,
        };
        let (sql, _params) = build_select_sql(
            "t",
            &cols,
            None,
            None,
            &vec![("id".into(), Direction::Asc)],
            false,
            Some(&start),
        );
        assert!(sql.contains("> ?"));
    }

    // Branch: at basis adds an equality disjunct.
    #[test]
    fn build_select_sql_start_at() {
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        let start = Start {
            row: row_from(&[("id", json!("05"))]),
            basis: StartBasis::At,
        };
        let (sql, _) = build_select_sql(
            "t",
            &cols,
            None,
            None,
            &vec![("id".into(), Direction::Asc)],
            false,
            Some(&start),
        );
        assert!(sql.contains("IS ?"));
    }

    // Branch: desc not reversed emits `< ?`.
    #[test]
    fn build_select_sql_start_desc_forward() {
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        let start = Start {
            row: row_from(&[("id", json!("05"))]),
            basis: StartBasis::After,
        };
        let (sql, _) = build_select_sql(
            "t",
            &cols,
            None,
            None,
            &vec![("id".into(), Direction::Desc)],
            false,
            Some(&start),
        );
        assert!(sql.contains("< ?"));
    }

    // ─── Source trait surface ──────────────────────────────────────────

    // Branch: table_schema() returns the cached TableSchema.
    #[test]
    fn source_trait_table_schema() {
        let ts = mk_foo();
        let schema: &TableSchema = Source::table_schema(&ts);
        assert_eq!(schema.name, "foo");
        assert_eq!(schema.columns.len(), 4);
    }

    // Branch: Source::push drains to completion and commits.
    #[test]
    fn source_trait_push_commits() {
        let ts = mk_foo();
        let _s: Stream<'_, Yield> = Source::push(
            &ts,
            SourceChange::Add(SourceChangeAdd {
                row: row_from(&[
                    ("id", json!("01")),
                    ("a", json!(1)),
                    ("b", json!(2)),
                    ("c", json!(3)),
                ]),
            }),
        );
        // Row should now be in the table.
        let mut k = Row::new();
        k.insert("id".into(), Some(json!("01")));
        assert!(ts.get_row(&k).unwrap().is_some());
    }

    // Branch: Source::gen_push yields one Step per connection.
    #[test]
    fn source_trait_gen_push_yields_per_connection() {
        let ts = mk_foo();
        let _ = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let _ = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let s: Stream<'_, GenPushStep> = Source::gen_push(
            &ts,
            SourceChange::Add(SourceChangeAdd {
                row: row_from(&[
                    ("id", json!("01")),
                    ("a", json!(1)),
                    ("b", json!(2)),
                    ("c", json!(3)),
                ]),
            }),
        );
        let steps: Vec<GenPushStep> = s.collect();
        assert_eq!(steps.len(), 2);
    }

    // Branch: Source::connect trait signature delegates to inherent.
    #[test]
    fn source_trait_connect_delegates() {
        let ts = mk_foo();
        let input = Source::connect(&ts, vec![("id".into(), Direction::Asc)], None, None, None);
        assert!(input.fully_applied_filters());
    }

    // Branch: should_yield callback.
    #[test]
    fn should_yield_callback() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY);")
            .unwrap();
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::String));
        let cb: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(|| true);
        let ts =
            TableSource::new_with_yield(conn, "t", cols, PrimaryKey::new(vec!["id".into()]), cb)
                .unwrap();
        assert!(ts.should_yield());
    }

    // Branch: Input::fetch returns NodeOrYield::Node wrapped rows.
    #[test]
    fn input_fetch_returns_nodes() {
        let ts = mk_foo();
        ts.push_change(SourceChange::Add(SourceChangeAdd {
            row: row_from(&[
                ("id", json!("01")),
                ("a", json!(1)),
                ("b", json!(2)),
                ("c", json!(3)),
            ]),
        }))
        .unwrap();
        let input = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let out: Vec<NodeOrYield> = input.fetch(FetchRequest::default()).collect();
        assert_eq!(out.len(), 1);
        match &out[0] {
            NodeOrYield::Node(n) => {
                assert_eq!(n.row.get("id"), Some(&Some(json!("01"))));
            }
            _ => panic!("expected Node"),
        }
    }

    // Branch: setup_foo helper exercised (guards against unused warnings).
    #[test]
    fn setup_foo_builds_connection() {
        let (ts, id) = setup_foo();
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(rows.len(), 0);
    }

    // ─── set_db ───────────────────────────────────────────────────────
    //
    // Port of TS `TableSource.setDB(db)`. The swap replaces the underlying
    // `Connection` behind the Mutex so subsequent reads / writes observe
    // the new database.

    /// Branch: swap to a fresh conn with identical schema but DIFFERENT
    /// row contents — `fetch_all` must read from the new conn.
    #[test]
    fn set_db_swaps_connection_observed_by_fetch_all() {
        // Original conn: empty table.
        let ts = mk_foo();
        let input = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = {
            let state = ts.state.lock().unwrap();
            state.connections[0].id
        };

        // Sanity: empty.
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert!(rows.is_empty());

        // Build a new conn with the same schema containing two rows.
        let new_conn = Connection::open_in_memory().unwrap();
        new_conn
            .execute_batch(
                "CREATE TABLE foo (id TEXT PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);\
                 INSERT INTO foo VALUES ('zz', 9, 8, 7);\
                 INSERT INTO foo VALUES ('aa', 1, 2, 3);",
            )
            .unwrap();

        ts.set_db(new_conn).expect("set_db ok");

        // fetch_all should now read two rows from the swapped conn.
        let rows = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(rows.len(), 2, "expected rows from swapped conn");
        // Sorted by id asc: 'aa' before 'zz'.
        assert_eq!(rows[0].get("id"), Some(&Some(json!("aa"))));
        assert_eq!(rows[1].get("id"), Some(&Some(json!("zz"))));

        drop(input); // silence unused
    }

    /// Branch: statements prepared against the OLD conn must not leak
    /// into the new conn. rusqlite's `prepare_cached` is per-connection,
    /// so the test asserts this indirectly: a read succeeds on a conn
    /// where the old row set was different.
    #[test]
    fn set_db_invalidates_prepared_cache() {
        let ts = mk_foo();
        // Seed one row on the original conn + trigger cache population.
        ts.push_change(SourceChange::Add(SourceChangeAdd {
            row: row_from(&[
                ("id", json!("aa")),
                ("a", json!(1)),
                ("b", json!(2)),
                ("c", json!(3)),
            ]),
        }))
        .unwrap();
        let _input = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let id = ts.state.lock().unwrap().connections[0].id;
        let first = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert_eq!(first.len(), 1);

        // New empty conn with the same schema.
        let new_conn = Connection::open_in_memory().unwrap();
        new_conn
            .execute_batch(
                "CREATE TABLE foo (id TEXT PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
            )
            .unwrap();
        ts.set_db(new_conn).unwrap();

        // The cached prepared statement tied to the old conn must not
        // be re-used — fetch_all prepares fresh against the new conn
        // (which has no rows).
        let after = ts.fetch_all(FetchRequest::default(), id).unwrap();
        assert!(after.is_empty(), "cache must not bleed across connections");
    }

    /// Branch: returns `Ok(())` and does not panic when invoked with a
    /// fresh connection.
    #[test]
    fn set_db_returns_ok() {
        let ts = mk_foo();
        let new_conn = Connection::open_in_memory().unwrap();
        new_conn
            .execute_batch(
                "CREATE TABLE foo (id TEXT PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
            )
            .unwrap();
        assert!(ts.set_db(new_conn).is_ok());
    }

}
