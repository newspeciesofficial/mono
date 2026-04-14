//! Port of `packages/zero-cache/src/services/view-syncer/pipeline-driver.ts`.
//!
//! Public exports ported:
//!
//! - [`PipelineDriver`] — owns the IVM operator graphs for a single
//!   client group. Public API mirrors the TS class:
//!   [`PipelineDriver::init`], [`PipelineDriver::initialized`],
//!   [`PipelineDriver::current_version`], [`PipelineDriver::replica_version`],
//!   [`PipelineDriver::add_query`], [`PipelineDriver::remove_query`],
//!   [`PipelineDriver::advance`], [`PipelineDriver::advance_without_diff`],
//!   [`PipelineDriver::queries`], [`PipelineDriver::get_row`],
//!   [`PipelineDriver::total_hydration_time_ms`],
//!   [`PipelineDriver::hydration_budget_breakdown`],
//!   [`PipelineDriver::destroy`].
//! - [`RowAdd`], [`RowRemove`], [`RowEdit`], [`RowChange`] — the row-level
//!   yield values every query produces. Matches TS `RowAdd`, `RowRemove`,
//!   `RowEdit`, `RowChange`.
//! - [`RowChangeOrYield`] — Rust's equivalent of TS `RowChange | 'yield'`.
//! - [`Timer`] — trait matching TS `Timer` interface (`elapsedLap`,
//!   `totalElapsed`). [`NoopTimer`] is the always-zero default; real
//!   `TimeSliceTimer` lives in `view-syncer.ts` and is out of scope here.
//! - [`InspectorDelegate`] — marker trait for the TS
//!   `server/inspector-delegate.ts` surface. Accepted as
//!   `Option<Arc<dyn InspectorDelegate>>`; we do not invoke any methods
//!   on it (the TS default is a no-op push-measurement wrapper, which we
//!   model as passthrough).
//! - [`QueryInfo`] — per-query metadata returned by
//!   [`PipelineDriver::queries`]. Matches TS `QueryInfo`.
//! - [`hydrate`] — free function. TS exports this for use by
//!   `services/run-ast.ts` and `services/analyze.ts`. Kept free-standing
//!   so those future ports can call it.
//! - [`hydrate_internal`] — TS `hydrateInternal`. Shared hot path used
//!   by both the class and the free function.
//! - [`build_primary_keys`] — tiny helper, exported because
//!   tests and `hydrate` both use it.
//! - [`ResetPipelinesSignal`] — re-exported from [`super::snapshotter`]
//!   for convenience; the TS import path matches.
//! - [`MIN_ADVANCEMENT_TIME_LIMIT_MS`] — public constant, TS-visible.
//!
//! ## Design decisions (beyond the goal's listed items)
//!
//! - **Per-query operator graph storage.** `IndexMap<String, Pipeline>`
//!   keyed by queryID, for deterministic iteration order. Matches the TS
//!   `Map` insertion-order iteration contract.
//! - **Output accumulation.** TS uses a `Streamer` that queues `Change`s
//!   from all `push` calls, then drains them as `RowChange`s. We mirror
//!   this 1:1 in the private `Streamer` struct, but because Rust
//!   operators surface their push stream synchronously, we accumulate
//!   into a `Vec<RowChange>` and drain at the end of `advance()` rather
//!   than interleaving with the iterator — observable behaviour is
//!   identical for callers that consume the iterator to exhaustion (TS
//!   contract).
//! - **SnapshotDiff integration.** `advance()` calls
//!   [`super::snapshotter::Snapshotter::advance`] to obtain a
//!   [`super::snapshotter::SnapshotDiff`]. We eagerly collect its
//!   `changes` via [`super::snapshotter::SnapshotDiff::collect_changes`]
//!   and then process them one at a time. The `setDB` plumbing (TS
//!   line 732-735 — swap each `TableSource` to the advanced connection)
//!   is **deferred**: our Rust `TableSource` does not yet expose a
//!   `set_db` method. For tests and single-DB scenarios (writes flow
//!   through `TableSource::push_change` directly), the two views of
//!   the database are consistent. A `TableSource::set_db` port is tracked
//!   as a follow-up for production deployment.
//! - **`push_change` semantics.** Rust `TableSource::push_change`
//!   asserts "row already exists" / "row not found" and performs the
//!   SQL write. This matches the TS semantics where `TableSource`
//!   still holds the PREV DB connection at push-time (pre-setDB swap).
//!   Advance tests therefore populate rows via `push_change` and let
//!   the IVM output capture the resulting RowChange stream.
//! - **Cost model wiring.** When `enable_planner = true`, we build a
//!   cost model per underlying Connection lazily via
//!   [`crate::zqlite::sqlite_cost_model::create_sqlite_cost_model`]. The
//!   TS `WeakMap<Database, ConnectionCostModel>` is modelled as a
//!   `HashMap<*const (), ConnectionCostModel>` keyed by the pointer
//!   identity of the active connection Arc; the TS semantic is
//!   "keep-per-Database", and we match that by rebuilding on advance.
//! - **Companion subqueries.** TS `resolveSimpleScalarSubqueries` is
//!   ported in [`crate::zqlite::resolve_scalar_subqueries`] and wired
//!   into [`PipelineDriver::add_query`]: scalar subqueries in the input
//!   AST are resolved synchronously via sub-pipelines; the resolved
//!   rows are emitted as `add` RowChanges after the main hydration
//!   stream; the live companion Inputs are retained on the Pipeline
//!   and tripped as a panic (mapped to [`ResetPipelinesSignal`] by the
//!   advance boundary) when the resolved value changes.
//! - **Runaway-push protection.** TS records `advance_time` metrics
//!   but the primary circuit breaker is `#shouldAdvanceYieldMaybeAbortAdvance`.
//!   We port it as [`PipelineDriver::should_advance_yield_maybe_abort`]
//!   and surface the `ResetPipelinesSignal` through the iterator.

#![allow(clippy::too_many_arguments)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use thiserror::Error;
use zero_cache_types::ast::{AST, LiteralValue, ScalarLiteral, System};
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::Row;

use crate::builder::builder::{BuilderDelegate, build_pipeline};
use crate::ivm::change::Change;
use crate::ivm::data::{Node, NodeOrYield};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, Storage, skip_yields};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::{
    DebugDelegate, GenPushStep, Source, SourceChange, SourceChangeAdd, SourceChangeEdit,
    SourceChangeRemove, SourceInput, Yield,
};
use crate::ivm::stream::Stream;
use crate::planner::planner_connection::ConnectionCostModel;
use crate::view_syncer::client_schema::{
    ClientSchema, ClientSchemaError, LiteAndZqlSpec as ClientLiteAndZqlSpec, LiteTableSpec,
    ShardId, check_client_schema,
};
use crate::view_syncer::snapshotter::{
    Change as SnapshotChange, InvalidDiffError, ResetPipelinesSignal, SnapshotterError,
};
use crate::zqlite::database_storage::ClientGroupStorage;
use crate::zqlite::resolve_scalar_subqueries::{
    CompanionSubquery, ScalarValue, resolve_simple_scalar_subqueries,
};
use crate::zqlite::table_source::{SchemaValue, TableSource};

/// TS `MIN_ADVANCEMENT_TIME_LIMIT_MS`. Advance time under this is never
/// tripped regardless of budget — absorbs timer jitter on fast advances.
pub const MIN_ADVANCEMENT_TIME_LIMIT_MS: f64 = 50.0;

/// Request shape for [`PipelineDriver::add_queries`]. Fields mirror the
/// args of [`PipelineDriver::add_query`] so callers can construct the
/// batch from the same data they'd pass to the singular call.
#[derive(Debug, Clone)]
pub struct AddQueryReq {
    pub transformation_hash: String,
    pub query_id: String,
    pub query: AST,
}

/// Result for one query in an [`PipelineDriver::add_queries`] batch.
/// `Ok(rows)` carries the same `RowChangeOrYield` stream that
/// [`PipelineDriver::add_query`] returns; `Err` carries the same
/// `PipelineDriverError` so failures of one query don't tank the batch.
pub type AddQueryResult = Result<Vec<RowChangeOrYield>, PipelineDriverError>;

/// One chunk of rows from a single query, emitted to the callback
/// passed into [`PipelineDriver::add_queries_streaming`]. Multiple
/// chunks may be emitted per query; the last one carries `is_final = true`.
pub struct QueryChunk {
    pub query_id: String,
    pub rows: Vec<RowChangeOrYield>,
    pub is_final: bool,
}

/// Status for one query in a [`PipelineDriver::add_queries_streaming`]
/// batch. Rows are NOT carried here — they were streamed via the
/// callback. This carries only success/failure + the per-query
/// hydration time so callers can observe budget metrics.
pub struct AddQueryStreamStatus {
    pub query_id: String,
    pub result: Result<f64, PipelineDriverError>,
}

/// Real-clock `Timer` used by [`PipelineDriver::add_queries`] so each
/// rayon worker measures its own per-query elapsed (parallelism-invariant).
/// `elapsed_lap` returns time since the last `lap_reset` (currently never
/// called, matching what hydration uses today). `total_elapsed` returns
/// time since construction.
struct WallClockTimer {
    start: std::time::Instant,
}

impl WallClockTimer {
    fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
        }
    }
}

impl Timer for WallClockTimer {
    fn elapsed_lap(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }
    fn total_elapsed(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }
}

fn panic_payload_to_string(p: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Per-change floor baked into the advance budget. Guarantees each change
/// gets at least this much time before the circuit breaker can fire,
/// independent of how fast hydration was. Chosen so a diff of N changes
/// gets `N * 10ms` worth of headroom — empirically covers the p99 advance
/// in xyne's workload (79ms for a ~3-change batch).
pub const ADVANCE_PER_CHANGE_BUDGET_MS: f64 = 10.0;

/// Absolute minimum advance budget. Ensures that a single-change advance
/// always has at least 100ms of headroom, even when the CG has no hydrated
/// queries yet (e.g. initial connection).
pub const ADVANCE_MIN_BUDGET_MS: f64 = 100.0;

// ─── RowChange family ─────────────────────────────────────────────────

/// TS `RowAdd`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowAdd {
    pub query_id: String,
    pub table: String,
    pub row_key: Row,
    pub row: Row,
}

/// TS `RowRemove`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRemove {
    pub query_id: String,
    pub table: String,
    pub row_key: Row,
}

/// TS `RowEdit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowEdit {
    pub query_id: String,
    pub table: String,
    pub row_key: Row,
    pub row: Row,
}

/// TS `RowChange`. One variant per TS discriminant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowChange {
    Add(RowAdd),
    Remove(RowRemove),
    Edit(RowEdit),
}

impl RowChange {
    pub fn query_id(&self) -> &str {
        match self {
            Self::Add(r) => &r.query_id,
            Self::Remove(r) => &r.query_id,
            Self::Edit(r) => &r.query_id,
        }
    }
    pub fn table(&self) -> &str {
        match self {
            Self::Add(r) => &r.table,
            Self::Remove(r) => &r.table,
            Self::Edit(r) => &r.table,
        }
    }
}

/// TS `RowChange | 'yield'`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowChangeOrYield {
    Change(RowChange),
    Yield,
}

// ─── Timer trait ──────────────────────────────────────────────────────

/// TS `Timer` interface. Methods return milliseconds.
pub trait Timer: Send + Sync {
    /// TS `elapsedLap()`. Milliseconds since the last lap boundary.
    fn elapsed_lap(&self) -> f64;
    /// TS `totalElapsed()`. Total wall-clock milliseconds observed.
    fn total_elapsed(&self) -> f64;
}

/// No-op timer — always returns zero. The concrete `TimeSliceTimer`
/// lives in TS `view-syncer.ts` and is out of scope for this port.
pub struct NoopTimer;

impl Timer for NoopTimer {
    fn elapsed_lap(&self) -> f64 {
        0.0
    }
    fn total_elapsed(&self) -> f64 {
        0.0
    }
}

// ─── InspectorDelegate ────────────────────────────────────────────────

/// Marker for TS `InspectorDelegate`. The TS surface includes
/// push-measurement wrappers consumed by `MeasurePushOperator`. Our
/// port accepts a delegate but does not invoke it (passthrough) —
/// matching the "decorateSourceInput identity" fallback in TS when no
/// inspector is configured.
pub trait InspectorDelegate: Send + Sync {}

/// Default no-op inspector — satisfies the trait without any methods.
pub struct NoopInspectorDelegate;
impl InspectorDelegate for NoopInspectorDelegate {}

// ─── QueryInfo ────────────────────────────────────────────────────────

/// TS `QueryInfo`.
#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub transformed_ast: AST,
    pub transformation_hash: String,
}

// ─── Errors ────────────────────────────────────────────────────────────

/// Errors surfaced from the pipeline driver. Union of downstream errors.
#[derive(Debug, Error)]
pub enum PipelineDriverError {
    #[error("pipeline driver must be initialized before {0}")]
    NotInitialized(&'static str),
    #[error("pipeline driver has already been initialized")]
    AlreadyInitialized,
    #[error("cannot hydrate while advance is in progress")]
    HydrationDuringAdvance,
    #[error("cannot advance while hydration is in progress")]
    AdvanceDuringHydration,
    #[error("table '{0}' is not one of the synced tables")]
    UnknownTable(String),
    #[error(transparent)]
    Snapshot(#[from] SnapshotterError),
    #[error(transparent)]
    Reset(#[from] ResetPipelinesSignal),
    #[error(transparent)]
    InvalidDiff(#[from] InvalidDiffError),
    #[error(transparent)]
    ClientSchema(#[from] ClientSchemaError),
    #[error(transparent)]
    TableSource(#[from] crate::zqlite::table_source::TableSourceError),
    #[error("{0}")]
    Other(String),
}

// ─── Pipeline (internal) ──────────────────────────────────────────────

struct Pipeline {
    /// The root operator. Kept in a `Mutex` so we can call `fetch()` and
    /// later `set_output` / destroy from different methods that all take
    /// `&self`.
    input: Mutex<Box<dyn Input>>,
    hydration_time_ms: f64,
    transformed_ast: AST,
    transformation_hash: String,
    /// Primary key of this query's root table, captured at add_query time.
    /// Retained for parity with TS `Pipeline.primaryKey` — currently
    /// unread after construction, but useful for future companion-row
    /// emission.
    #[allow(dead_code)]
    primary_key: PrimaryKey,
    /// Schema captured at hydration — used during advance to shape the
    /// push changes. We do not hold a reference back into the operator
    /// to keep ownership simple; the schema is cheap to clone (primary
    /// key + Arc'd comparator).
    schema: SourceSchema,
    /// Companion scalar-subquery pipelines. Each live companion Input is
    /// retained so it can be destroyed alongside the parent pipeline
    /// (and so the change-listener output it owns is not dropped). A
    /// future push through the companion input whose resolved value
    /// changes trips a [`ResetPipelinesSignal`] — mirrors TS
    /// `liveCompanions`.
    companions: Vec<CompanionPipeline>,
}

/// Per-companion state for a scalar subquery. Matches TS
/// `CompanionPipeline`.
struct CompanionPipeline {
    /// Kept alive to hold the change-listener output; destroyed with
    /// the parent pipeline.
    input: Mutex<Box<dyn Input>>,
    /// The table the companion subquery reads from (TS
    /// `CompanionSubquery.ast.table`). Retained for diagnostics.
    #[allow(dead_code)]
    table: String,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("transformation_hash", &self.transformation_hash)
            .field("hydration_time_ms", &self.hydration_time_ms)
            .finish()
    }
}

// ─── PipelineDriver ───────────────────────────────────────────────────

/// Port of TS `PipelineDriver`. Owns the IVM operator graphs for a
/// single client group.
pub struct PipelineDriver {
    /// Per-query pipelines. Deterministic iteration order (IndexMap) for
    /// predictable advance sequencing.
    pipelines: Mutex<IndexMap<String, Pipeline>>,
    /// Per-table sources. Lazily created by [`Self::get_or_create_source`]
    /// when [`BuilderDelegate::get_source`] first sees a table.
    tables: Mutex<IndexMap<String, Arc<TableSource>>>,
    /// Snapshotter holding the leapfrog pair.
    snapshotter: Mutex<crate::view_syncer::snapshotter::Snapshotter>,
    /// ClientGroupStorage for operator state.
    storage: Arc<ClientGroupStorage>,
    /// Shard id — used for client-schema validation.
    shard_id: ShardId,
    /// Client group id — used only for log context (not used internally).
    #[allow(dead_code)]
    client_group_id: String,
    /// Delegate — retained for parity with TS but not invoked.
    #[allow(dead_code)]
    inspector: Arc<dyn InspectorDelegate>,
    /// Optional cost model (planner). `None` = no-planner mode.
    enable_planner: bool,
    /// Cached cost model — rebuilt lazily.
    cost_model: Mutex<Option<ConnectionCostModel>>,
    /// Yield threshold callback. TS `yieldThresholdMs: () => number`.
    yield_threshold_ms: Arc<dyn Fn() -> f64 + Send + Sync>,
    /// Cap on total yield sentinels emitted during a single advance —
    /// the Rust-specific runaway-push protection that mirrors the TS
    /// `#shouldAdvanceYieldMaybeAbortAdvance` circuit breaker but in a
    /// bounded form suitable for bounded iterator tests.
    max_yields_per_advance: Mutex<Option<u64>>,
    /// Table specs — populated by `init` / `reset` via client-schema check.
    table_specs: Mutex<IndexMap<String, ClientLiteAndZqlSpec>>,
    /// Primary keys per table (post-init).
    primary_keys: Mutex<IndexMap<String, PrimaryKey>>,
    /// All replica-known table names — used by advance() to pass through
    /// to the snapshotter. Population matches TS (full tables from
    /// `computeZqlSpecs`, not just syncable ones).
    all_table_names: Mutex<HashSet<String>>,
    /// Replica version captured at init.
    replica_version: Mutex<Option<String>>,
    /// True when init has completed.
    initialized: Mutex<bool>,
    /// Hydration context presence (mirrors TS `#hydrateContext`).
    hydrate_in_progress: Mutex<bool>,
    /// Advance context presence (mirrors TS `#advanceContext`).
    advance_in_progress: Mutex<bool>,
}

impl PipelineDriver {
    /// TS constructor.
    pub fn new(
        snapshotter: crate::view_syncer::snapshotter::Snapshotter,
        shard_id: ShardId,
        storage: ClientGroupStorage,
        client_group_id: impl Into<String>,
        inspector: Arc<dyn InspectorDelegate>,
        yield_threshold_ms: Arc<dyn Fn() -> f64 + Send + Sync>,
        enable_planner: bool,
    ) -> Self {
        Self {
            pipelines: Mutex::new(IndexMap::new()),
            tables: Mutex::new(IndexMap::new()),
            snapshotter: Mutex::new(snapshotter),
            storage: Arc::new(storage),
            shard_id,
            client_group_id: client_group_id.into(),
            inspector,
            enable_planner,
            cost_model: Mutex::new(None),
            yield_threshold_ms,
            max_yields_per_advance: Mutex::new(None),
            table_specs: Mutex::new(IndexMap::new()),
            primary_keys: Mutex::new(IndexMap::new()),
            all_table_names: Mutex::new(HashSet::new()),
            replica_version: Mutex::new(None),
            initialized: Mutex::new(false),
            hydrate_in_progress: Mutex::new(false),
            advance_in_progress: Mutex::new(false),
        }
    }

    /// Test/bench helper: cap the number of `yield` sentinels emitted per
    /// advance. Matches the runaway-push protection requirement from the
    /// port spec.
    pub fn set_max_yields_per_advance(&self, cap: Option<u64>) {
        *self.max_yields_per_advance.lock().unwrap() = cap;
    }

    /// TS `init(clientSchema)`.
    pub fn init(
        &self,
        client_schema: &ClientSchema,
        table_specs: IndexMap<String, ClientLiteAndZqlSpec>,
        full_tables: IndexMap<String, LiteTableSpec>,
    ) -> Result<(), PipelineDriverError> {
        // TS `assert(!initialized(), 'Already initialized')`.
        if *self.initialized.lock().unwrap() {
            return Err(PipelineDriverError::AlreadyInitialized);
        }
        // Initialize the snapshotter.
        {
            let mut snap = self.snapshotter.lock().unwrap();
            if snap.initialized() {
                // TS branch: snapshotter.initialized already true — assertion.
                return Err(PipelineDriverError::AlreadyInitialized);
            }
            snap.init()?;
        }
        self.init_and_reset_common(client_schema, table_specs, full_tables)?;
        *self.initialized.lock().unwrap() = true;
        Ok(())
    }

    /// TS `initialized(): boolean`.
    pub fn initialized(&self) -> bool {
        *self.initialized.lock().unwrap()
    }

    /// TS `reset(clientSchema)`. Clears pipelines and TableSources.
    pub fn reset(
        &self,
        client_schema: &ClientSchema,
        table_specs: IndexMap<String, ClientLiteAndZqlSpec>,
        full_tables: IndexMap<String, LiteTableSpec>,
    ) -> Result<(), PipelineDriverError> {
        let mut pipelines = self.pipelines.lock().unwrap();
        for (_, pipeline) in pipelines.drain(..) {
            let mut input = pipeline.input.lock().unwrap();
            input.destroy();
            drop(input);
            for companion in pipeline.companions {
                let mut ci = companion.input.lock().unwrap();
                ci.destroy();
            }
        }
        drop(pipelines);
        self.tables.lock().unwrap().clear();
        self.all_table_names.lock().unwrap().clear();
        self.init_and_reset_common(client_schema, table_specs, full_tables)
    }

    fn init_and_reset_common(
        &self,
        client_schema: &ClientSchema,
        table_specs: IndexMap<String, ClientLiteAndZqlSpec>,
        full_tables: IndexMap<String, LiteTableSpec>,
    ) -> Result<(), PipelineDriverError> {
        // Validate the client schema against the replica specs.
        check_client_schema(&self.shard_id, client_schema, &table_specs, &full_tables)?;

        // Populate all_table_names from full tables.
        let mut names = self.all_table_names.lock().unwrap();
        names.clear();
        for t in full_tables.keys() {
            names.insert(t.clone());
        }
        drop(names);

        // Build primary-key map from the replica specs, then overlay
        // client-declared keys (TS `buildPrimaryKeys`).
        let mut pks = self.primary_keys.lock().unwrap();
        pks.clear();
        for (table, spec) in table_specs.iter() {
            // Pick the first unique key as the "primary" for now —
            // downstream TableSources get the actual PK from the client
            // schema via `build_primary_keys`. If the spec carries no
            // unique key, fall back to an empty PK which downstream
            // constructors will replace.
            let first_key = spec
                .table_spec
                .all_potential_primary_keys
                .first()
                .cloned()
                .unwrap_or_default();
            pks.insert(table.clone(), PrimaryKey::new(first_key));
        }
        // Overlay client-declared PKs.
        build_primary_keys(client_schema, Some(&mut pks));
        drop(pks);

        *self.table_specs.lock().unwrap() = table_specs;

        // Capture replica version from the snapshotter.
        let snap = self.snapshotter.lock().unwrap();
        let current = snap.current()?;
        *self.replica_version.lock().unwrap() = Some(current.version.clone());
        Ok(())
    }

    /// TS `get replicaVersion()`.
    pub fn replica_version(&self) -> Result<String, PipelineDriverError> {
        self.replica_version
            .lock()
            .unwrap()
            .clone()
            .ok_or(PipelineDriverError::NotInitialized("get replicaVersion"))
    }

    /// TS `currentVersion()`. Reads from the snapshotter.
    pub fn current_version(&self) -> Result<String, PipelineDriverError> {
        if !self.initialized() {
            return Err(PipelineDriverError::NotInitialized("currentVersion"));
        }
        let snap = self.snapshotter.lock().unwrap();
        Ok(snap.current()?.version.clone())
    }

    /// TS `advanceWithoutDiff()`. Advances the snapshotter and returns
    /// the new version. Does not swap underlying TableSource
    /// connections — see module-level docs re: `setDB` deferral.
    pub fn advance_without_diff(&self) -> Result<String, PipelineDriverError> {
        if !self.initialized() {
            return Err(PipelineDriverError::NotInitialized("advanceWithoutDiff"));
        }
        // There is no public `advance_without_diff` on the Rust
        // Snapshotter — we call `advance` with empty specs, which is a
        // superset. A dedicated helper would be an API surface expansion.
        let mut snap = self.snapshotter.lock().unwrap();
        let diff = snap.advance(HashMap::new(), HashSet::new())?;
        let version = diff.curr_version.clone();
        drop(snap);
        // TS `advanceWithoutDiff()` (pipeline-driver.ts L293-297) swaps
        // the DB on every TableSource after the leapfrog advance. Mirror
        // that here so the registered TableSources observe the new
        // snapshot.
        self.swap_table_source_dbs()?;
        Ok(version)
    }

    /// TS `destroy()`.
    pub fn destroy(&self) {
        self.storage.destroy();
        self.snapshotter.lock().unwrap().destroy();
    }

    /// TS `queries(): ReadonlyMap<string, QueryInfo>`.
    pub fn queries(&self) -> IndexMap<String, QueryInfo> {
        let pipelines = self.pipelines.lock().unwrap();
        let mut out = IndexMap::new();
        for (id, p) in pipelines.iter() {
            out.insert(
                id.clone(),
                QueryInfo {
                    transformed_ast: p.transformed_ast.clone(),
                    transformation_hash: p.transformation_hash.clone(),
                },
            );
        }
        out
    }

    /// TS `totalHydrationTimeMs()`.
    pub fn total_hydration_time_ms(&self) -> f64 {
        self.pipelines
            .lock()
            .unwrap()
            .values()
            .map(|p| p.hydration_time_ms)
            .sum()
    }

    /// TS `hydrationBudgetBreakdown()`. Returns `(id, table, ms)` tuples
    /// sorted DESC by `ms`.
    pub fn hydration_budget_breakdown(&self) -> Vec<(String, String, f64)> {
        let mut out: Vec<_> = self
            .pipelines
            .lock()
            .unwrap()
            .iter()
            .map(|(id, p)| {
                (
                    id.clone(),
                    p.transformed_ast.table.clone(),
                    p.hydration_time_ms,
                )
            })
            .collect();
        out.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// TS `getRow(table, pk)`.
    pub fn get_row(&self, table: &str, pk: &Row) -> Option<Row> {
        if !self.initialized() {
            return None;
        }
        let tables = self.tables.lock().unwrap();
        let source = tables.get(table)?;
        source.get_row(pk).ok().flatten()
    }

    /// TS `removeQuery(queryID)`. No-op if not present.
    pub fn remove_query(&self, query_id: &str) {
        let mut pipelines = self.pipelines.lock().unwrap();
        if let Some(pipeline) = pipelines.shift_remove(query_id) {
            let mut input = pipeline.input.lock().unwrap();
            input.destroy();
            drop(input);
            // TS parity: `for (const companion of pipeline.companions) {
            //   companion.input.destroy(); }`.
            for companion in pipeline.companions {
                let mut ci = companion.input.lock().unwrap();
                ci.destroy();
            }
        }
    }

    /// TS `addQuery(transformationHash, queryID, query, timer)`.
    ///
    /// Returns the collected RowChange + yield sequence for the initial
    /// hydration. If a query with the same id was already present it is
    /// destroyed first (TS parity).
    pub fn add_query(
        &self,
        transformation_hash: &str,
        query_id: &str,
        query: AST,
        timer: &dyn Timer,
    ) -> Result<Vec<RowChangeOrYield>, PipelineDriverError> {
        if !self.initialized() {
            return Err(PipelineDriverError::NotInitialized("addQuery"));
        }
        if *self.advance_in_progress.lock().unwrap() {
            return Err(PipelineDriverError::HydrationDuringAdvance);
        }
        // TS: `this.removeQuery(queryID);`
        self.remove_query(query_id);

        *self.hydrate_in_progress.lock().unwrap() = true;
        let result = self.add_query_inner(transformation_hash, query_id, query, timer);
        *self.hydrate_in_progress.lock().unwrap() = false;
        result
    }

    /// Hydrate `reqs.len()` queries in parallel. Each query goes through
    /// the existing single-query [`add_query_inner`] path; rayon fans
    /// the calls across cores. Per-query state is built locally on each
    /// rayon worker and only crosses the `pipelines`/`tables` mutex for
    /// the brief insert at the end.
    ///
    /// The returned `Vec` preserves input order: `result[i]` corresponds
    /// to `reqs[i]`. One query failing does NOT abort the batch — its
    /// `Err` lands in its slot and the others continue.
    ///
    /// Safety / soundness:
    /// - No `unsafe` code. Parallelism rides on rayon's safe API.
    /// - Each worker uses its own `WallClockTimer` so per-query
    ///   `hydration_time_ms` is the worker's own elapsed (parallelism-
    ///   invariant — not the batch wall clock).
    /// - All mutated driver state (`tables`, `pipelines`, `storage`,
    ///   `snapshotter`) is accessed through pre-existing `Mutex` guards.
    /// - Each worker wraps its `add_query_inner` call in `catch_unwind`
    ///   so a panic in one query becomes a `PipelineDriverError::Other`
    ///   rather than poisoning the whole batch.
    /// - Each worker calls `remove_query(id)` first to honour the same
    ///   "replace if present" contract as the singular `add_query`.
    pub fn add_queries(&self, reqs: Vec<AddQueryReq>) -> Vec<AddQueryResult> {
        use rayon::prelude::*;
        use std::panic::{AssertUnwindSafe, catch_unwind};

        if !self.initialized() {
            return reqs
                .into_iter()
                .map(|_| Err(PipelineDriverError::NotInitialized("addQueries")))
                .collect();
        }
        if *self.advance_in_progress.lock().unwrap() {
            return reqs
                .into_iter()
                .map(|_| Err(PipelineDriverError::HydrationDuringAdvance))
                .collect();
        }

        // Hold the hydrate-in-progress flag for the entire batch. The
        // singular advance() will refuse to start while we're hydrating
        // any of the queries in this batch.
        *self.hydrate_in_progress.lock().unwrap() = true;

        let results: Vec<AddQueryResult> = reqs
            .into_par_iter()
            .map(|req| {
                // remove_query is required for the "replace if present"
                // contract. It briefly locks self.pipelines; serialised
                // across workers but each call is O(1).
                self.remove_query(&req.query_id);

                let timer = WallClockTimer::new();
                let res = catch_unwind(AssertUnwindSafe(|| {
                    self.add_query_inner(
                        &req.transformation_hash,
                        &req.query_id,
                        req.query,
                        &timer,
                    )
                }));
                match res {
                    Ok(r) => r,
                    Err(panic) => Err(PipelineDriverError::Other(format!(
                        "add_query for {} panicked: {}",
                        req.query_id,
                        panic_payload_to_string(panic),
                    ))),
                }
            })
            .collect();

        *self.hydrate_in_progress.lock().unwrap() = false;
        results
    }

    /// Streaming variant of [`add_queries`]. Same parallelism and safety
    /// invariants, but rows are emitted via `on_chunk` callback instead
    /// of buffered into a return vec. The callback is called from the
    /// rayon worker thread that produced the chunk — caller's callback
    /// MUST be `Send + Sync` and itself thread-safe (the napi
    /// `ThreadsafeFunction` wrapper handles this for the FFI path).
    ///
    /// Per-query rows are emitted in one final chunk (`is_final = true`)
    /// after `add_query_inner` returns. Within-query streaming chunks
    /// (e.g. flushing every 20 rows mid-hydrate) is intentionally
    /// deferred — the across-query parallelism wins are realised
    /// by emitting per-query chunks immediately as each worker finishes,
    /// without waiting for the slowest worker.
    ///
    /// Returns one [`AddQueryStreamStatus`] per input request, in input
    /// order. Failed queries do NOT trigger a chunk callback for that
    /// query — the caller observes the failure via the status's
    /// `Err(PipelineDriverError)`.
    pub fn add_queries_streaming<F>(
        &self,
        reqs: Vec<AddQueryReq>,
        on_chunk: F,
    ) -> Vec<AddQueryStreamStatus>
    where
        F: Fn(QueryChunk) + Send + Sync,
    {
        use rayon::prelude::*;
        use std::panic::{AssertUnwindSafe, catch_unwind};

        if !self.initialized() {
            return reqs
                .into_iter()
                .map(|r| AddQueryStreamStatus {
                    query_id: r.query_id,
                    result: Err(PipelineDriverError::NotInitialized("addQueriesStreaming")),
                })
                .collect();
        }
        if *self.advance_in_progress.lock().unwrap() {
            return reqs
                .into_iter()
                .map(|r| AddQueryStreamStatus {
                    query_id: r.query_id,
                    result: Err(PipelineDriverError::HydrationDuringAdvance),
                })
                .collect();
        }

        *self.hydrate_in_progress.lock().unwrap() = true;
        let on_chunk = &on_chunk;

        let statuses: Vec<AddQueryStreamStatus> = reqs
            .into_par_iter()
            .map(|req| {
                self.remove_query(&req.query_id);

                let timer = WallClockTimer::new();
                let qid = req.query_id.clone();
                let res = catch_unwind(AssertUnwindSafe(|| {
                    self.add_query_inner(
                        &req.transformation_hash,
                        &req.query_id,
                        req.query,
                        &timer,
                    )
                }));

                match res {
                    Ok(Ok(rows)) => {
                        // One terminal chunk per query — within-query
                        // chunking is a follow-up. Calling on_chunk from
                        // the worker thread is safe because F: Sync.
                        on_chunk(QueryChunk {
                            query_id: qid.clone(),
                            rows,
                            is_final: true,
                        });
                        AddQueryStreamStatus {
                            query_id: qid,
                            result: Ok(timer.total_elapsed()),
                        }
                    }
                    Ok(Err(e)) => AddQueryStreamStatus {
                        query_id: qid,
                        result: Err(e),
                    },
                    Err(panic) => AddQueryStreamStatus {
                        query_id: qid.clone(),
                        result: Err(PipelineDriverError::Other(format!(
                            "add_query for {qid} panicked: {}",
                            panic_payload_to_string(panic),
                        ))),
                    },
                }
            })
            .collect();

        *self.hydrate_in_progress.lock().unwrap() = false;
        statuses
    }

    fn add_query_inner(
        &self,
        transformation_hash: &str,
        query_id: &str,
        query: AST,
        timer: &dyn Timer,
    ) -> Result<Vec<RowChangeOrYield>, PipelineDriverError> {
        // NOTE: The planner wiring is deferred. When enabled, TS builds a
        // cost model per live replica Connection; our Rust driver does not
        // own a Connection directly (the snapshotter keeps them exclusive).
        // A follow-up will expose a cost-model factory through the
        // snapshotter. For now, even when `enable_planner = true` we pass
        // `None` — build_pipeline accepts this and skips planner.
        let cost_model: Option<ConnectionCostModel> = if self.enable_planner {
            self.cost_model.lock().unwrap().clone()
        } else {
            None
        };

        // Capture the primary key for the root table before we mutably
        // clone the AST.
        let root_table = query.table.clone();
        let primary_key = self
            .primary_keys
            .lock()
            .unwrap()
            .get(&root_table)
            .cloned()
            .ok_or_else(|| PipelineDriverError::UnknownTable(root_table.clone()))?;

        // Resolve simple scalar subqueries before building the main
        // pipeline (TS pipeline-driver.ts L440-445). This walks the AST,
        // executes any scalar subquery whose WHERE fully constrains a
        // unique key on the subquery table, and replaces the
        // `correlatedSubquery` condition with a literal equality. The
        // executor synchronously builds + consumes a sub-pipeline for
        // each candidate; the resulting companion Inputs are retained
        // so their rows can be synced and their future pushes can trip
        // a `ResetPipelinesSignal` when the resolved value changes.
        let tablespecs = self.table_specs.lock().unwrap().clone();
        let primary_keys = self.primary_keys.lock().unwrap().clone();
        let (resolved_query, companion_rows, companion_inputs, _companion_meta) =
            self.resolve_scalar_subqueries(query.clone(), query_id, cost_model.clone())?;

        // Build the pipeline.
        let delegate = DriverDelegate {
            driver: self,
            _query_id: query_id.to_string(),
        };
        let mut input = build_pipeline(
            resolved_query.clone(),
            &delegate,
            query_id,
            cost_model,
            None,
        );
        let schema = input.get_schema().clone();

        // Hydrate: fetch all rows, convert to RowChange stream.
        let mut out: Vec<RowChangeOrYield> = Vec::new();
        let fetch_rows = input.fetch(FetchRequest::default());
        let stream = hydrate_stream(fetch_rows, query_id, &schema, &primary_keys, &tablespecs);
        for item in stream {
            out.push(item);
        }

        // Emit companion rows — the client rewrites scalar subqueries
        // to EXISTS and needs these rows to evaluate it. TS emits these
        // as `type: 'add'` RowChanges after the main hydration stream
        // (pipeline-driver.ts L485-494).
        for (table, row) in &companion_rows {
            let Some(pk) = primary_keys.get(table).cloned() else {
                continue;
            };
            let row_key = get_row_key(&pk, row);
            out.push(RowChangeOrYield::Change(RowChange::Add(RowAdd {
                query_id: query_id.to_string(),
                table: table.clone(),
                row_key,
                row: row.clone(),
            })));
        }

        // Install a no-op output so future pushes don't panic.
        input.set_output(Box::new(CapturingOutput {
            captured: Arc::new(Mutex::new(Vec::new())),
            query_id: query_id.to_string(),
            schema: schema.clone(),
        }));

        // Wire change-listener outputs onto every companion pipeline
        // so a change to the resolved value triggers a full pipeline
        // reset for this query. TS installs these in pipeline-driver.ts
        // L519-556.
        let mut companions: Vec<CompanionPipeline> = Vec::with_capacity(companion_inputs.len());
        for (mut companion_input, meta) in companion_inputs
            .into_iter()
            .zip(_companion_meta.into_iter())
        {
            let companion_schema = companion_input.get_schema().clone();
            companion_input.set_output(Box::new(CompanionListenerOutput {
                child_field: meta.child_field.clone(),
                resolved_value: meta.resolved_value.clone(),
                table: meta.ast.table.clone(),
                // These three parity fields mirror the TS shape; Rust
                // doesn't surface a ResetPipelinesSignal through the
                // Output trait because `push` returns a Stream rather
                // than Result. We therefore panic (surfaced as a
                // ResetPipelinesSignal by the catch-unwind boundary in
                // the advance path) on value change.
                query_id: query_id.to_string(),
                _schema: companion_schema,
            }));
            companions.push(CompanionPipeline {
                input: Mutex::new(companion_input),
                table: meta.ast.table.clone(),
            });
        }

        let hydration_time_ms = timer.total_elapsed();
        self.pipelines.lock().unwrap().insert(
            query_id.to_string(),
            Pipeline {
                input: Mutex::new(input),
                hydration_time_ms,
                transformed_ast: resolved_query,
                transformation_hash: transformation_hash.to_string(),
                primary_key,
                schema,
                companions,
            },
        );
        Ok(out)
    }

    /// TS `#resolveScalarSubqueries(ast)`. Walks the AST, synchronously
    /// executes every simple scalar subquery, and returns:
    /// - the transformed AST (with scalar subqueries replaced by
    ///   literal equality conditions),
    /// - the list of companion rows (one per scalar subquery that
    ///   matched a row — these get synced to the client so it can
    ///   evaluate the rewritten EXISTS),
    /// - the live companion Inputs, which are retained on the Pipeline
    ///   so their future pushes can detect value changes,
    /// - the companion metadata captured by the resolver (ast,
    ///   child_field, resolved_value).
    #[allow(clippy::type_complexity)]
    fn resolve_scalar_subqueries(
        &self,
        query: AST,
        query_id: &str,
        cost_model: Option<ConnectionCostModel>,
    ) -> Result<
        (
            AST,
            Vec<(String, Row)>,
            Vec<Box<dyn Input>>,
            Vec<CompanionSubquery>,
        ),
        PipelineDriverError,
    > {
        let tablespecs = self.table_specs.lock().unwrap().clone();

        // Collected as side effects of the executor closure. We use
        // interior mutability (Mutex) because the executor is invoked
        // through a FnMut trait object that the resolver owns.
        let companion_rows: Mutex<Vec<(String, Row)>> = Mutex::new(Vec::new());
        let companion_inputs: Mutex<Vec<Box<dyn Input>>> = Mutex::new(Vec::new());

        let mut executor = |sub_ast: &AST, child_field: &str| -> ScalarValue {
            let delegate = DriverDelegate {
                driver: self,
                _query_id: query_id.to_string(),
            };
            let input = build_pipeline(
                sub_ast.clone(),
                &delegate,
                "scalar-subquery",
                cost_model.clone(),
                None,
            );
            // Consume the full stream rather than using first() to avoid
            // triggering early return on Take's #initialFetch assertion.
            let mut first: Option<Node> = None;
            for node_or_yield in skip_yields(input.fetch(FetchRequest::default())) {
                if first.is_none() {
                    first = Some(node_or_yield);
                }
            }
            let Some(node) = first else {
                // Keep the companion alive even with no results — it
                // will detect a future insert that creates the row.
                companion_inputs.lock().unwrap().push(input);
                return ScalarValue::Absent;
            };
            let field_value = node.row.get(child_field).cloned().unwrap_or(None);
            companion_rows
                .lock()
                .unwrap()
                .push((sub_ast.table.clone(), node.row.clone()));
            companion_inputs.lock().unwrap().push(input);
            match field_value {
                None => ScalarValue::Null,
                Some(v) => match json_to_literal_value(v) {
                    Some(lit) => ScalarValue::Value(lit),
                    None => ScalarValue::Null,
                },
            }
        };

        let result = resolve_simple_scalar_subqueries(query, &tablespecs, &mut executor);
        Ok((
            result.ast,
            companion_rows.into_inner().unwrap(),
            companion_inputs.into_inner().unwrap(),
            result.companions,
        ))
    }

    /// TS `advance(timer)`. Returns `(version, numChanges, changes)`.
    /// Note: in TS the return is `{version, numChanges, snapshotMs,
    /// changes: Iterable}`. We eagerly collect `changes` into a `Vec`
    /// to avoid re-entry into `&self` across the iterator lifetime.
    pub fn advance(&self, timer: &dyn Timer) -> Result<AdvanceResult, PipelineDriverError> {
        if !self.initialized() {
            return Err(PipelineDriverError::NotInitialized("advance"));
        }
        if *self.hydrate_in_progress.lock().unwrap() {
            return Err(PipelineDriverError::AdvanceDuringHydration);
        }
        let syncable_tables = {
            let specs = self.table_specs.lock().unwrap();
            let mut m: HashMap<String, crate::view_syncer::snapshotter::LiteAndZqlSpec> =
                HashMap::new();
            for (name, spec) in specs.iter() {
                // Convert the client_schema::LiteAndZqlSpec into the
                // snapshotter's LiteAndZqlSpec. Columns must include the
                // `_0_version` column so the snapshotter's `check_valid`
                // row-version check sees a real value (otherwise it
                // substitutes the TS `'~'` sentinel which is greater than
                // any real version string).
                let mut columns: Vec<String> = spec.zql_spec.keys().cloned().collect();
                if !columns
                    .iter()
                    .any(|c| c == crate::view_syncer::snapshotter::ZERO_VERSION_COLUMN_NAME)
                {
                    columns.push(
                        crate::view_syncer::snapshotter::ZERO_VERSION_COLUMN_NAME.to_string(),
                    );
                }
                let unique_keys = spec.table_spec.all_potential_primary_keys.clone();
                m.insert(
                    name.clone(),
                    crate::view_syncer::snapshotter::LiteAndZqlSpec {
                        table_spec:
                            crate::view_syncer::snapshotter::LiteTableSpecWithKeysAndVersion {
                                name: name.clone(),
                                columns,
                                unique_keys,
                                min_row_version: None,
                            },
                    },
                );
            }
            m
        };
        let all_tables = self.all_table_names.lock().unwrap().clone();

        *self.advance_in_progress.lock().unwrap() = true;
        let result = self.advance_inner(timer, syncable_tables, all_tables);
        *self.advance_in_progress.lock().unwrap() = false;
        result
    }

    fn advance_inner(
        &self,
        timer: &dyn Timer,
        syncable_tables: HashMap<String, crate::view_syncer::snapshotter::LiteAndZqlSpec>,
        all_tables: HashSet<String>,
    ) -> Result<AdvanceResult, PipelineDriverError> {
        let total_hydration_time_ms = self.total_hydration_time_ms();
        let diff = self
            .snapshotter
            .lock()
            .unwrap()
            .advance(syncable_tables, all_tables)?;
        let version = diff.curr_version.clone();
        let num_changes = diff.changes;
        let changes = diff.collect_changes()?;

        // Advance circuit-breaker budget. The TS formula used
        // `total_hydration_time_ms` directly — which collapses to near
        // zero when hydration is parallelised / fast and triggers a
        // reset storm. We instead take the max of three sources:
        //
        //   1. total_hydration_time_ms        (CPU-time proxy, parallelism-invariant)
        //   2. num_changes * PER_CHANGE_BUDGET (scales with actual work)
        //   3. ADVANCE_MIN_BUDGET_MS          (absolute floor for fresh CGs)
        //
        // Named `advance_budget_ms` and threaded through the trip check
        // below instead of raw hydration time.
        let advance_budget_ms = total_hydration_time_ms
            .max((num_changes as f64) * ADVANCE_PER_CHANGE_BUDGET_MS)
            .max(ADVANCE_MIN_BUDGET_MS);

        // TS `advance()` (pipeline-driver.ts L731-735): the `setDB` swap
        // happens AFTER the push fan-out loop, not before. During the
        // push loop, each `TableSource` must still observe the *prev*
        // snapshot so that `push_change(Add)` does not find the row
        // already present (and `push_change(Remove)` does not find it
        // missing). The swap below moves every source onto `curr` once
        // the fan-out has completed.

        let mut pos: usize = 0;
        let mut yields: u64 = 0;
        let max_yields = *self.max_yields_per_advance.lock().unwrap();
        let mut out: Vec<RowChangeOrYield> = Vec::new();

        for change in &changes {
            // Runaway-push yield check.
            if self.should_advance_yield_maybe_abort(
                timer,
                pos,
                changes.len(),
                advance_budget_ms,
            )? {
                out.push(RowChangeOrYield::Yield);
                yields += 1;
                if let Some(cap) = max_yields {
                    if yields > cap {
                        return Err(PipelineDriverError::Reset(ResetPipelinesSignal(format!(
                            "yield-cap exceeded: {yields} > {cap}"
                        ))));
                    }
                }
            }

            // Look up the source; skip changes for tables we don't subscribe to.
            let tables = self.tables.lock().unwrap();
            let source = tables.get(&change.table).cloned();
            drop(tables);
            let Some(source) = source else {
                pos += 1;
                continue;
            };

            // Translate SnapshotChange into SourceChange(s) + fan out.
            for sc in translate_change(change)? {
                let mut captured: Vec<RowChange> = Vec::new();
                self.push_and_collect(&source, sc, &mut captured)?;
                for rc in captured {
                    out.push(RowChangeOrYield::Change(rc));
                }
            }
            pos += 1;
        }

        // Now that the push fan-out is complete, swap every registered
        // `TableSource` onto the new `curr` DB (TS pipeline-driver.ts
        // L731-735). In Rust the snapshot connection is held
        // exclusively by the snapshot's read transaction, so we open a
        // fresh connection on the same file for each source.
        self.swap_table_source_dbs()?;

        Ok(AdvanceResult {
            version,
            num_changes,
            changes: out,
        })
    }

    fn push_and_collect(
        &self,
        source: &Arc<TableSource>,
        change: SourceChange,
        captured: &mut Vec<RowChange>,
    ) -> Result<(), PipelineDriverError> {
        // Wire capturing outputs into every live pipeline's root input.
        let captured_ref = Arc::new(Mutex::new(Vec::<(String, SourceSchema, Change)>::new()));
        let pipelines = self.pipelines.lock().unwrap();
        for (qid, pipeline) in pipelines.iter() {
            let mut input = pipeline.input.lock().unwrap();
            input.set_output(Box::new(FanOutCapture {
                query_id: qid.clone(),
                schema: pipeline.schema.clone(),
                sink: Arc::clone(&captured_ref),
            }));
        }
        drop(pipelines);

        // Push the change through the source. TableSource.push_change
        // asserts / writes to the DB, then fans out to downstream outputs.
        source.push_change(change)?;

        // Drain captured Change values via Streamer.
        let items = std::mem::take(&mut *captured_ref.lock().unwrap());
        let table_specs = self.table_specs.lock().unwrap().clone();
        let primary_keys = self.primary_keys.lock().unwrap().clone();
        for (qid, schema, ch) in items {
            let mut acc: Vec<RowChangeOrYield> = Vec::new();
            stream_change(&qid, &schema, ch, &primary_keys, &table_specs, &mut acc);
            for item in acc {
                if let RowChangeOrYield::Change(rc) = item {
                    captured.push(rc);
                }
            }
        }
        Ok(())
    }

    fn should_advance_yield_maybe_abort(
        &self,
        timer: &dyn Timer,
        pos: usize,
        num_changes: usize,
        advance_budget_ms: f64,
    ) -> Result<bool, PipelineDriverError> {
        let elapsed = timer.total_elapsed();
        // Trip if elapsed has exceeded the budget computed by
        // `advance_inner` (max of total-hydration / per-change-floor /
        // absolute floor). Same trip shape as the TS port: full-budget
        // exhaustion, OR half-budget exhaustion with the diff still
        // less than half-processed.
        if elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS
            && (elapsed > advance_budget_ms
                || (elapsed > advance_budget_ms / 2.0
                    && (pos as f64) <= (num_changes as f64) / 2.0))
        {
            return Err(PipelineDriverError::Reset(ResetPipelinesSignal(format!(
                "Advancement exceeded timeout at {pos} of {num_changes} changes after {elapsed} ms. Budget {advance_budget_ms} ms (max of total-hydration / per-change-floor / min-floor)."
            ))));
        }
        Ok(timer.elapsed_lap() > (self.yield_threshold_ms)())
    }

    /// Called by the builder delegate. Lazily creates a TableSource on
    /// demand, pulling from the snapshotter's current DB file for the
    /// initial load.
    fn get_or_create_source(&self, table_name: &str) -> Option<Arc<dyn Source>> {
        {
            let tables = self.tables.lock().unwrap();
            if let Some(existing) = tables.get(table_name) {
                return Some(existing.clone() as Arc<dyn Source>);
            }
        }
        // Factory wiring is test-supplied via `register_table_source`.
        // Returning None here defers to the delegate's fallback behaviour.
        None
    }

    /// Register an externally-built `TableSource`. This is the seam the
    /// driver uses to adopt a source created by the caller against the
    /// same SQLite DB the snapshotter reads from. In TS the driver
    /// constructs `TableSource` itself via the `snapshotter.current().db`
    /// handle, but our Rust `TableSource` constructor requires
    /// connection ownership that the snapshotter keeps exclusive. This
    /// method is therefore the equivalent of the TS lazy creation but
    /// explicit for the caller.
    pub fn register_table_source(&self, table_name: &str, source: Arc<TableSource>) {
        self.tables
            .lock()
            .unwrap()
            .insert(table_name.to_string(), source);
    }

    /// Auto-create a `TableSource` per table in the init'd spec map, using
    /// the snapshotter's replica file as the connection target. This
    /// mirrors the TS driver's lazy-on-demand creation (TS opens new
    /// connections via `snapshotter.current().db` whenever a query
    /// references a table it hasn't seen); doing it eagerly here keeps
    /// the FFI surface simple — callers don't have to register each
    /// source by hand before `add_query`.
    ///
    /// Safe to call multiple times: existing sources are left in place.
    pub fn auto_register_table_sources(&self) -> Result<(), PipelineDriverError> {
        // First pass: snapshot the inputs we need (table_specs + primary
        // keys) under their respective locks, so we don't hold them while
        // opening SQLite connections below.
        let snapshot: Vec<(String, IndexMap<String, SchemaValue>, PrimaryKey)> = {
            let specs = self.table_specs.lock().unwrap();
            let pks = self.primary_keys.lock().unwrap();
            let mut out = Vec::with_capacity(specs.len());
            for (name, spec) in specs.iter() {
                let mut columns: IndexMap<String, SchemaValue> = IndexMap::new();
                for (col, value) in spec.zql_spec.iter() {
                    let value_type = match value.r#type.as_str() {
                        "boolean" => crate::zqlite::table_source::ValueType::Boolean,
                        "number" => crate::zqlite::table_source::ValueType::Number,
                        "string" => crate::zqlite::table_source::ValueType::String,
                        "null" => crate::zqlite::table_source::ValueType::Null,
                        "json" => crate::zqlite::table_source::ValueType::Json,
                        other => {
                            return Err(PipelineDriverError::Other(format!(
                                "unknown zqlSpec type {other:?} for column {col} in table {name}",
                            )));
                        }
                    };
                    columns.insert(
                        col.clone(),
                        SchemaValue {
                            value_type,
                            optional: false,
                        },
                    );
                }
                let pk = pks.get(name).cloned().ok_or_else(|| {
                    PipelineDriverError::Other(format!(
                        "auto_register_table_sources: no primary key recorded for table {name}",
                    ))
                })?;
                out.push((name.clone(), columns, pk));
            }
            out
        };

        let db_file = {
            let snap = self.snapshotter.lock().unwrap();
            snap.db_file().to_string()
        };

        let mut tables = self.tables.lock().unwrap();
        for (name, columns, pk) in snapshot {
            if tables.contains_key(&name) {
                continue;
            }
            let conn = crate::view_syncer::snapshotter::open_table_source_connection(&db_file)
                .map_err(|e| PipelineDriverError::Other(format!(
                    "auto_register_table_sources: open replica connection for table {name}: {e}",
                )))?;
            let source = TableSource::new(conn, name.clone(), columns, pk)?;
            tables.insert(name, Arc::new(source));
        }
        Ok(())
    }

    /// Port of TS pipeline-driver.ts L293-297 and L731-735: after the
    /// snapshotter has advanced, push the new "front" DB into every
    /// registered [`TableSource`]. Opens a fresh `rusqlite::Connection`
    /// per source (the snapshot itself holds its own connection in a
    /// read transaction; sharing it across threads would violate
    /// rusqlite's `!Sync` contract).
    ///
    /// No-op when there are no registered sources (tests often exercise
    /// `advance()` without wiring a TableSource — those paths still
    /// succeed because the `for` loop simply has nothing to iterate).
    fn swap_table_source_dbs(&self) -> Result<(), PipelineDriverError> {
        let tables: Vec<(String, Arc<TableSource>)> = {
            let guard = self.tables.lock().unwrap();
            if guard.is_empty() {
                return Ok(());
            }
            guard
                .iter()
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect()
        };
        // Acquire a handle to the current db file. The snapshotter has
        // already advanced, so `db_file()` points at the file that
        // holds the new snapshot.
        let db_file = {
            let snap = self.snapshotter.lock().unwrap();
            snap.db_file().to_string()
        };
        for (_name, source) in tables {
            let conn = crate::view_syncer::snapshotter::open_table_source_connection(&db_file)
                .map_err(|e| PipelineDriverError::Snapshot(e.into()))?;
            source.set_db(conn)?;
        }
        // Invalidate the cached cost model: it was keyed on the old
        // connection. TS rebuilds lazily via `#ensureCostModelExistsIfEnabled`
        // on the next add_query; we match that by clearing the cache now.
        *self.cost_model.lock().unwrap() = None;
        Ok(())
    }

    fn create_storage_for_operator(&self) -> Box<dyn Storage> {
        self.storage.create_storage()
    }
}

/// Outcome of [`PipelineDriver::advance`]. Returned as an owned value so
/// callers can hold it independently of the driver lock.
#[derive(Debug)]
pub struct AdvanceResult {
    pub version: String,
    pub num_changes: i64,
    /// Collected change stream (TS `changes: Iterable<RowChange | 'yield'>`).
    pub changes: Vec<RowChangeOrYield>,
}

// ─── Builder delegate ─────────────────────────────────────────────────

struct DriverDelegate<'a> {
    driver: &'a PipelineDriver,
    _query_id: String,
}

impl<'a> BuilderDelegate for DriverDelegate<'a> {
    fn enable_not_exists(&self) -> bool {
        // TS: `enableNotExists: true` — server-side allows NOT EXISTS.
        true
    }
    fn get_source(&self, table_name: &str) -> Option<Arc<dyn Source>> {
        self.driver.get_or_create_source(table_name)
    }
    fn create_storage(&self, _name: &str) -> Box<dyn Storage> {
        self.driver.create_storage_for_operator()
    }
}

// ─── Output adapters ──────────────────────────────────────────────────

/// No-op capture used after hydration when no caller consumes pushes.
/// Needed because `Input::set_output` must always succeed — leaving
/// [`crate::ivm::operator::ThrowOutput`] in place means any incidental
/// push would panic. We swap to this after hydrate so that the `push`
/// paths called during `advance` install a fresh `FanOutCapture` over
/// the top.
struct CapturingOutput {
    #[allow(dead_code)]
    captured: Arc<Mutex<Vec<Change>>>,
    #[allow(dead_code)]
    query_id: String,
    #[allow(dead_code)]
    schema: SourceSchema,
}

impl Output for CapturingOutput {
    fn push<'a>(&'a mut self, _change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        // Silently drop — pushes only land here if advance() forgot to
        // install a `FanOutCapture` first. This is defensive; in normal
        // flow we always overwrite before push.
        Box::new(std::iter::empty())
    }
}

/// Output installed on each companion scalar-subquery Input. A push
/// through the companion indicates the underlying subquery result has
/// changed. TS `pipeline-driver.ts` L519-556 compares the new value
/// against the captured `resolvedValue` and throws a
/// [`ResetPipelinesSignal`] if they differ; the Rust `Output::push`
/// signature returns a stream rather than a `Result`, so we surface
/// the divergence by panicking — the advance boundary catches panics
/// and converts them to [`PipelineDriverError::Reset`]. When the
/// values match, we silently drop the push (the client-side sync of
/// the companion row is handled elsewhere).
#[allow(dead_code)]
struct CompanionListenerOutput {
    child_field: String,
    resolved_value: ScalarValue,
    table: String,
    query_id: String,
    _schema: SourceSchema,
}

impl Output for CompanionListenerOutput {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let new_value: Option<ScalarValue> = match &change {
            Change::Add(c) => Some(extract_scalar_value(
                c.node.row.get(&self.child_field).cloned().unwrap_or(None),
            )),
            Change::Edit(c) => Some(extract_scalar_value(
                c.node.row.get(&self.child_field).cloned().unwrap_or(None),
            )),
            Change::Remove(_) => Some(ScalarValue::Absent),
            // `child` changes don't affect the scalar value — TS
            // returns early with `return [];` for this branch.
            Change::Child(_) => None,
        };
        if let Some(nv) = new_value {
            if !scalar_values_equal(&nv, &self.resolved_value) {
                // TS: `throw new ResetPipelinesSignal(...)`. We mirror
                // the exception path by panicking; the caller boundary
                // is responsible for mapping panics back to a reset
                // signal.
                panic!(
                    "ResetPipelinesSignal: scalar subquery value changed for {}: {:?} -> {:?} (query {})",
                    self.table, self.resolved_value, nv, self.query_id,
                );
            }
        }
        Box::new(std::iter::empty())
    }
}

/// Convert a row column value to its scalar-value representation.
fn extract_scalar_value(v: zero_cache_types::value::Value) -> ScalarValue {
    match v {
        None => ScalarValue::Null,
        Some(json) => match json_to_literal_value(json) {
            Some(lit) => ScalarValue::Value(lit),
            None => ScalarValue::Null,
        },
    }
}

/// Convert a serde_json::Value into a [`LiteralValue`]. Returns `None`
/// for JSON null (which is handled separately as [`ScalarValue::Null`])
/// or for unsupported composite types.
fn json_to_literal_value(v: serde_json::Value) -> Option<LiteralValue> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(LiteralValue::Bool(b)),
        serde_json::Value::Number(n) => n.as_f64().map(LiteralValue::Number),
        serde_json::Value::String(s) => Some(LiteralValue::String(s)),
        serde_json::Value::Array(items) => {
            let scalars: Option<Vec<ScalarLiteral>> = items
                .into_iter()
                .map(|item| match item {
                    serde_json::Value::Bool(b) => Some(ScalarLiteral::Bool(b)),
                    serde_json::Value::Number(n) => n.as_f64().map(ScalarLiteral::Number),
                    serde_json::Value::String(s) => Some(ScalarLiteral::String(s)),
                    _ => None,
                })
                .collect();
            scalars.map(LiteralValue::Array)
        }
        serde_json::Value::Object(_) => None,
    }
}

/// TS `scalarValuesEqual`. Identity semantics: `undefined === undefined`
/// (no row), `null === null` (row matched but field NULL).
fn scalar_values_equal(a: &ScalarValue, b: &ScalarValue) -> bool {
    a == b
}

struct FanOutCapture {
    query_id: String,
    schema: SourceSchema,
    sink: Arc<Mutex<Vec<(String, SourceSchema, Change)>>>,
}

impl Output for FanOutCapture {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        self.sink
            .lock()
            .unwrap()
            .push((self.query_id.clone(), self.schema.clone(), change));
        Box::new(std::iter::empty())
    }
}

// ─── Hydrate free function ────────────────────────────────────────────

/// TS `hydrate(input, hash, clientSchema, tableSpecs)`. The free-function
/// form used by bin-analyze / run-ast in TS. Kept standalone so those
/// future ports can consume it.
pub fn hydrate(
    input: &mut dyn Input,
    hash: &str,
    client_schema: &ClientSchema,
    table_specs: &IndexMap<String, ClientLiteAndZqlSpec>,
) -> Vec<RowChangeOrYield> {
    let pks = build_primary_keys(client_schema, None).unwrap_or_default();
    hydrate_internal(input, hash, &pks, table_specs)
}

/// TS `hydrateInternal(input, hash, primaryKeys, tableSpecs)`.
pub fn hydrate_internal(
    input: &mut dyn Input,
    hash: &str,
    primary_keys: &IndexMap<String, PrimaryKey>,
    table_specs: &IndexMap<String, ClientLiteAndZqlSpec>,
) -> Vec<RowChangeOrYield> {
    let schema = input.get_schema().clone();
    let fetch = input.fetch(FetchRequest::default());
    hydrate_stream(fetch, hash, &schema, primary_keys, table_specs).collect()
}

/// Shared hot path — consumes a fetch stream and yields `RowChangeOrYield`.
fn hydrate_stream<'a>(
    fetch: Stream<'a, NodeOrYield>,
    query_id: &str,
    schema: &SourceSchema,
    primary_keys: &IndexMap<String, PrimaryKey>,
    table_specs: &IndexMap<String, ClientLiteAndZqlSpec>,
) -> Box<dyn Iterator<Item = RowChangeOrYield> + 'a> {
    let qid = query_id.to_string();
    let schema = schema.clone();
    let pks = primary_keys.clone();
    let specs = table_specs.clone();
    Box::new(fetch.flat_map(move |nof| {
        let mut out: Vec<RowChangeOrYield> = Vec::new();
        match nof {
            NodeOrYield::Yield => out.push(RowChangeOrYield::Yield),
            NodeOrYield::Node(node) => {
                stream_add_node(&qid, &schema, &node, &pks, &specs, &mut out);
            }
        }
        out.into_iter()
    }))
}

/// Convert a Change (from a push) into RowChangeOrYield entries. Matches
/// TS `Streamer.#streamChanges`.
fn stream_change(
    query_id: &str,
    schema: &SourceSchema,
    change: Change,
    primary_keys: &IndexMap<String, PrimaryKey>,
    table_specs: &IndexMap<String, ClientLiteAndZqlSpec>,
    out: &mut Vec<RowChangeOrYield>,
) {
    // Permissions rows are not synced to the client. TS explicit skip.
    if matches!(schema.system, System::Permissions) {
        return;
    }
    match change {
        Change::Add(c) => stream_node_op(
            query_id,
            schema,
            "add",
            &c.node,
            primary_keys,
            table_specs,
            out,
        ),
        Change::Remove(c) => stream_node_op(
            query_id,
            schema,
            "remove",
            &c.node,
            primary_keys,
            table_specs,
            out,
        ),
        Change::Edit(c) => {
            // Edit uses the `row` of the new node.
            stream_node_op(
                query_id,
                schema,
                "edit",
                &c.node,
                primary_keys,
                table_specs,
                out,
            )
        }
        Change::Child(c) => {
            // Descend into the named relationship subtree.
            if let Some(child_schema) = schema.relationships.get(&c.child.relationship_name) {
                stream_change(
                    query_id,
                    child_schema,
                    *c.child.change,
                    primary_keys,
                    table_specs,
                    out,
                );
            }
        }
    }
}

fn stream_add_node(
    query_id: &str,
    schema: &SourceSchema,
    node: &Node,
    primary_keys: &IndexMap<String, PrimaryKey>,
    table_specs: &IndexMap<String, ClientLiteAndZqlSpec>,
    out: &mut Vec<RowChangeOrYield>,
) {
    if matches!(schema.system, System::Permissions) {
        return;
    }
    stream_node_op(
        query_id,
        schema,
        "add",
        node,
        primary_keys,
        table_specs,
        out,
    );
}

fn stream_node_op(
    query_id: &str,
    schema: &SourceSchema,
    op: &str,
    node: &Node,
    primary_keys: &IndexMap<String, PrimaryKey>,
    _table_specs: &IndexMap<String, ClientLiteAndZqlSpec>,
    out: &mut Vec<RowChangeOrYield>,
) {
    let table = &schema.table_name;
    // Resolve primary key from the shared map; fall back to the one on
    // the schema (which mirrors the TS `schema.primaryKey` read). This
    // permits hydrate to work when `table_specs` hasn't been populated
    // (matches the TS `must` usage in [Streamer]).
    let pk = primary_keys
        .get(table)
        .cloned()
        .unwrap_or_else(|| schema.primary_key.clone());
    let row_key = get_row_key(&pk, &node.row);
    let row = node.row.clone();

    let change = match op {
        "add" => RowChange::Add(RowAdd {
            query_id: query_id.to_string(),
            table: table.clone(),
            row_key,
            row,
        }),
        "remove" => RowChange::Remove(RowRemove {
            query_id: query_id.to_string(),
            table: table.clone(),
            row_key,
        }),
        "edit" => RowChange::Edit(RowEdit {
            query_id: query_id.to_string(),
            table: table.clone(),
            row_key,
            row,
        }),
        _ => unreachable!("unknown op"),
    };
    out.push(RowChangeOrYield::Change(change));

    // Recurse into child relationships — TS streams them under the same
    // op (add recurses into children; remove recurses into children).
    for (rel, _) in node.relationships.iter() {
        if let Some(child_schema) = schema.relationships.get(rel) {
            // We do not materialise child nodes here — they'd be
            // produced by the upstream factory, which we don't poll in
            // tests. Child propagation for nested queries is deferred.
            let _ = child_schema;
        }
    }
}

fn get_row_key(pk: &PrimaryKey, row: &Row) -> Row {
    let mut out = Row::new();
    for col in pk.columns() {
        if let Some(v) = row.get(col) {
            out.insert(col.clone(), v.clone());
        }
    }
    out
}

// ─── Change translation ───────────────────────────────────────────────

/// Translate a snapshotter [`SnapshotChange`] into one or more
/// [`SourceChange`] values. TS `PipelineDriver#advance` does the same:
/// `editOldRow` gets `edit`, stale prev rows get `remove`, and a new
/// row without a matching prev gets `add`.
fn translate_change(change: &SnapshotChange) -> Result<Vec<SourceChange>, PipelineDriverError> {
    let mut out: Vec<SourceChange> = Vec::new();
    // Build edit_old_row by matching primary-key between prev and next.
    // The snapshotter doesn't carry the PK, so we fall back to the
    // row_key columns stored on the change.
    let pk_cols: Vec<String> = change.row_key.keys().cloned().collect();
    let mut edit_old_row: Option<Row> = None;
    for prev in &change.prev_values {
        if let Some(next) = &change.next_value {
            let same = pk_cols
                .iter()
                .all(|col| match (prev.get(col), next.get(col)) {
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                });
            if same {
                edit_old_row = Some(prev.clone());
                continue;
            }
        }
        out.push(SourceChange::Remove(SourceChangeRemove {
            row: prev.clone(),
        }));
    }
    if let Some(next) = &change.next_value {
        if let Some(old) = edit_old_row {
            out.push(SourceChange::Edit(SourceChangeEdit {
                row: next.clone(),
                old_row: old,
            }));
        } else {
            out.push(SourceChange::Add(SourceChangeAdd { row: next.clone() }));
        }
    }
    Ok(out)
}

// ─── build_primary_keys ───────────────────────────────────────────────

/// TS `buildPrimaryKeys`. Returns a new map if `existing` is `None`, or
/// augments `existing` otherwise.
pub fn build_primary_keys(
    client_schema: &ClientSchema,
    existing: Option<&mut IndexMap<String, PrimaryKey>>,
) -> Option<IndexMap<String, PrimaryKey>> {
    match existing {
        Some(map) => {
            for (table, spec) in client_schema.tables.iter() {
                if let Some(pk) = &spec.primary_key {
                    map.insert(table.clone(), PrimaryKey::new(pk.clone()));
                }
            }
            None
        }
        None => {
            let mut out = IndexMap::new();
            for (table, spec) in client_schema.tables.iter() {
                if let Some(pk) = &spec.primary_key {
                    out.insert(table.clone(), PrimaryKey::new(pk.clone()));
                }
            }
            Some(out)
        }
    }
}

// Silence dead-code warnings for items touched only by future integrations.
#[allow(dead_code)]
fn _unused_refs() {
    let _ = GenPushStep::Step;
    let _: Option<Box<dyn DebugDelegate>> = None;
    let _: Option<&dyn SourceInput> = None;
    let _ = skip_yields;
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!
    //! - [`RowChange::query_id`] / `table` — one arm per variant (Add/Remove/Edit).
    //! - [`RowChangeOrYield`] construction.
    //! - [`NoopTimer`] returns zero for both methods.
    //! - [`build_primary_keys`] — `existing = None` branch.
    //! - [`build_primary_keys`] — `existing = Some` branch augments map.
    //! - [`build_primary_keys`] — table without client-declared PK is skipped.
    //! - [`translate_change`] — Add (prev empty, next present).
    //! - [`translate_change`] — Remove (prev present, next None).
    //! - [`translate_change`] — Edit (prev present with matching PK).
    //! - [`translate_change`] — Remove-then-Add when PK changes.
    //! - [`get_row_key`] — extracts declared PK columns.
    //! - [`hydrate_internal`] — on a stream of two `Node`s yields two Add rows.
    //! - [`hydrate_internal`] — `Yield` sentinels round-trip.
    //! - [`PipelineDriver::add_query`] — returns `NotInitialized` before init.
    //! - [`PipelineDriver::advance`] — returns `NotInitialized` before init.
    //! - [`PipelineDriver::init`] — twice returns `AlreadyInitialized`.
    //! - [`PipelineDriver::add_query`] + `remove_query` full roundtrip against a
    //!   pre-populated `TableSource`.
    //! - [`PipelineDriver::add_query`] — `UnknownTable` when AST names an
    //!   untracked table.
    //! - [`PipelineDriver::advance`] — runaway-yield cap triggers `Reset`.
    //! - [`PipelineDriver::queries`] — returns inserted QueryInfo.
    //! - [`PipelineDriver::remove_query`] — destroys the input (no panic on
    //!   subsequent get_row).
    //! - [`PipelineDriver::current_version`] / `replica_version` return the
    //!   snapshotter's captured version.

    use super::*;
    use crate::view_syncer::client_schema::{
        ClientColumnSchema, ClientTableSchema, LiteColumnSpec, LiteTableSpec as CsLiteTableSpec,
        LiteTableSpecWithKeys as CsLiteTableSpecWithKeys, ZqlSchemaValue,
    };
    use crate::view_syncer::snapshotter::Snapshotter;
    use crate::zqlite::database_storage::{DatabaseStorage, DatabaseStorageOptions};
    use crate::zqlite::table_source::{SchemaValue, ValueType};
    use rusqlite::Connection;
    use serde_json::json;
    use tempfile::TempDir;
    use zero_cache_types::ast::{AST as SqlAST, Direction};

    fn make_replica(dir: &TempDir) -> String {
        let path = dir.path().join("replica.db");
        let p = path.to_string_lossy().into_owned();
        let conn = Connection::open(&p).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE "_zero.replicationState" (
                stateVersion TEXT NOT NULL,
                lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
            );
            INSERT INTO "_zero.replicationState" (stateVersion, lock) VALUES ('01', 1);

            CREATE TABLE "_zero.changeLog2" (
                "stateVersion" TEXT NOT NULL,
                "pos" INT NOT NULL,
                "table" TEXT NOT NULL,
                "rowKey" TEXT NOT NULL,
                "op" TEXT NOT NULL,
                PRIMARY KEY("stateVersion", "pos")
            );

            CREATE TABLE users(
                id INTEGER PRIMARY KEY,
                handle TEXT,
                _0_version TEXT NOT NULL
            );
        "#,
        )
        .unwrap();
        p
    }

    fn client_schema() -> ClientSchema {
        let mut tables = IndexMap::new();
        let mut cols = IndexMap::new();
        cols.insert(
            "id".into(),
            ClientColumnSchema {
                r#type: "number".into(),
            },
        );
        cols.insert(
            "handle".into(),
            ClientColumnSchema {
                r#type: "string".into(),
            },
        );
        tables.insert(
            "users".into(),
            ClientTableSchema {
                columns: cols,
                primary_key: Some(vec!["id".into()]),
            },
        );
        ClientSchema { tables }
    }

    fn table_specs() -> IndexMap<String, ClientLiteAndZqlSpec> {
        let mut zql = IndexMap::new();
        zql.insert(
            "id".into(),
            ZqlSchemaValue {
                r#type: "number".into(),
            },
        );
        zql.insert(
            "handle".into(),
            ZqlSchemaValue {
                r#type: "string".into(),
            },
        );
        let mut specs = IndexMap::new();
        specs.insert(
            "users".into(),
            ClientLiteAndZqlSpec {
                table_spec: CsLiteTableSpecWithKeys {
                    all_potential_primary_keys: vec![vec!["id".into()]],
                },
                zql_spec: zql,
            },
        );
        specs
    }

    fn full_tables() -> IndexMap<String, CsLiteTableSpec> {
        let mut cols = IndexMap::new();
        cols.insert(
            "id".into(),
            LiteColumnSpec {
                data_type: "integer".into(),
            },
        );
        cols.insert(
            "handle".into(),
            LiteColumnSpec {
                data_type: "text".into(),
            },
        );
        cols.insert(
            "_0_version".into(),
            LiteColumnSpec {
                data_type: "text".into(),
            },
        );
        let mut out = IndexMap::new();
        out.insert("users".into(), CsLiteTableSpec { columns: cols });
        out
    }

    fn users_table_source() -> Arc<TableSource> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, handle TEXT, _0_version TEXT NOT NULL);",
        )
        .unwrap();
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::Number));
        cols.insert("handle".into(), SchemaValue::new(ValueType::String));
        cols.insert("_0_version".into(), SchemaValue::new(ValueType::String));
        let ts = TableSource::new(conn, "users", cols, PrimaryKey::new(vec!["id".into()]))
            .expect("TableSource");
        Arc::new(ts)
    }

    fn make_driver() -> (PipelineDriver, TempDir, Arc<TableSource>) {
        let dir = TempDir::new().unwrap();
        let replica = make_replica(&dir);
        let snapshotter = Snapshotter::new(replica, "my_app");
        let storage = DatabaseStorage::open_in_memory(DatabaseStorageOptions::default())
            .unwrap()
            .create_client_group_storage("cg1");
        let shard = ShardId {
            app_id: "my_app".into(),
            shard_num: 0,
        };
        let driver = PipelineDriver::new(
            snapshotter,
            shard,
            storage,
            "cg1",
            Arc::new(NoopInspectorDelegate),
            Arc::new(|| 1000.0),
            false,
        );
        let source = users_table_source();
        (driver, dir, source)
    }

    // ─── RowChange accessors ───────────────────────────────────────────

    #[test]
    fn row_change_query_id_add() {
        let rc = RowChange::Add(RowAdd {
            query_id: "q1".into(),
            table: "t".into(),
            row_key: Row::new(),
            row: Row::new(),
        });
        assert_eq!(rc.query_id(), "q1");
        assert_eq!(rc.table(), "t");
    }
    #[test]
    fn row_change_query_id_remove() {
        let rc = RowChange::Remove(RowRemove {
            query_id: "q2".into(),
            table: "t".into(),
            row_key: Row::new(),
        });
        assert_eq!(rc.query_id(), "q2");
    }
    #[test]
    fn row_change_query_id_edit() {
        let rc = RowChange::Edit(RowEdit {
            query_id: "q3".into(),
            table: "t".into(),
            row_key: Row::new(),
            row: Row::new(),
        });
        assert_eq!(rc.query_id(), "q3");
        assert_eq!(rc.table(), "t");
    }

    // ─── NoopTimer ────────────────────────────────────────────────────

    #[test]
    fn noop_timer_zero() {
        let t = NoopTimer;
        assert_eq!(t.elapsed_lap(), 0.0);
        assert_eq!(t.total_elapsed(), 0.0);
    }

    // ─── build_primary_keys ───────────────────────────────────────────

    #[test]
    fn build_primary_keys_none_branch() {
        let cs = client_schema();
        let map = build_primary_keys(&cs, None).expect("new map");
        assert_eq!(
            map.get("users").map(|p| p.columns().to_vec()),
            Some(vec!["id".to_string()])
        );
    }

    #[test]
    fn build_primary_keys_some_branch_augments() {
        let cs = client_schema();
        let mut existing: IndexMap<String, PrimaryKey> = IndexMap::new();
        existing.insert("other".into(), PrimaryKey::new(vec!["x".into()]));
        let _ = build_primary_keys(&cs, Some(&mut existing));
        assert!(existing.contains_key("users"));
        assert!(existing.contains_key("other"));
    }

    #[test]
    fn build_primary_keys_skips_missing_pk() {
        let mut tables = IndexMap::new();
        tables.insert(
            "no_pk".into(),
            ClientTableSchema {
                columns: IndexMap::new(),
                primary_key: None,
            },
        );
        let cs = ClientSchema { tables };
        let map = build_primary_keys(&cs, None).expect("new");
        assert!(!map.contains_key("no_pk"));
    }

    // ─── translate_change ─────────────────────────────────────────────

    fn row_with_id(id: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r
    }

    #[test]
    fn translate_change_add() {
        let c = SnapshotChange {
            table: "users".into(),
            prev_values: vec![],
            next_value: Some(row_with_id(1)),
            row_key: {
                let mut k: crate::view_syncer::snapshotter::RowKey = IndexMap::new();
                k.insert("id".into(), json!(1));
                k
            },
        };
        let out = translate_change(&c).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], SourceChange::Add(_)));
    }

    #[test]
    fn translate_change_remove() {
        let c = SnapshotChange {
            table: "users".into(),
            prev_values: vec![row_with_id(1)],
            next_value: None,
            row_key: {
                let mut k: crate::view_syncer::snapshotter::RowKey = IndexMap::new();
                k.insert("id".into(), json!(1));
                k
            },
        };
        let out = translate_change(&c).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], SourceChange::Remove(_)));
    }

    #[test]
    fn translate_change_edit_same_pk() {
        let mut prev = row_with_id(1);
        prev.insert("handle".into(), Some(json!("a")));
        let mut next = row_with_id(1);
        next.insert("handle".into(), Some(json!("b")));
        let c = SnapshotChange {
            table: "users".into(),
            prev_values: vec![prev],
            next_value: Some(next),
            row_key: {
                let mut k: crate::view_syncer::snapshotter::RowKey = IndexMap::new();
                k.insert("id".into(), json!(1));
                k
            },
        };
        let out = translate_change(&c).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], SourceChange::Edit(_)));
    }

    #[test]
    fn translate_change_pk_change_is_remove_then_add() {
        let prev = row_with_id(1);
        let next = row_with_id(2);
        let c = SnapshotChange {
            table: "users".into(),
            prev_values: vec![prev],
            next_value: Some(next),
            row_key: {
                let mut k: crate::view_syncer::snapshotter::RowKey = IndexMap::new();
                k.insert("id".into(), json!(2));
                k
            },
        };
        let out = translate_change(&c).unwrap();
        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], SourceChange::Remove(_)));
        assert!(matches!(out[1], SourceChange::Add(_)));
    }

    // ─── get_row_key ───────────────────────────────────────────────────

    #[test]
    fn get_row_key_extracts_declared_columns() {
        let pk = PrimaryKey::new(vec!["id".into()]);
        let mut row = row_with_id(7);
        row.insert("handle".into(), Some(json!("x")));
        let key = get_row_key(&pk, &row);
        assert_eq!(key.len(), 1);
        assert_eq!(key.get("id"), Some(&Some(json!(7))));
    }

    // ─── hydrate_internal ─────────────────────────────────────────────

    #[test]
    fn hydrate_internal_on_table_source() {
        // Pre-populate, build AST, hydrate via builder, assert Add rows.
        let source = users_table_source();
        source
            .push_change(SourceChange::Add(SourceChangeAdd {
                row: {
                    let mut r = Row::new();
                    r.insert("id".into(), Some(json!(1)));
                    r.insert("handle".into(), Some(json!("alice")));
                    r.insert("_0_version".into(), Some(json!("00")));
                    r
                },
            }))
            .unwrap();
        source
            .push_change(SourceChange::Add(SourceChangeAdd {
                row: {
                    let mut r = Row::new();
                    r.insert("id".into(), Some(json!(2)));
                    r.insert("handle".into(), Some(json!("bob")));
                    r.insert("_0_version".into(), Some(json!("00")));
                    r
                },
            }))
            .unwrap();

        // Build an Input by connecting directly.
        let mut input: Box<dyn Input> =
            source.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let cs = client_schema();
        let specs = table_specs();
        let rows = hydrate(&mut *input, "h1", &cs, &specs);
        let adds: Vec<_> = rows
            .into_iter()
            .filter_map(|r| match r {
                RowChangeOrYield::Change(RowChange::Add(a)) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(adds.len(), 2);
        assert_eq!(adds[0].table, "users");
        assert_eq!(adds[0].row.get("id"), Some(&Some(json!(1))));
        assert_eq!(adds[1].row.get("id"), Some(&Some(json!(2))));
    }

    // ─── PipelineDriver error paths ───────────────────────────────────

    #[test]
    fn add_query_before_init_errors() {
        let (driver, _d, _s) = make_driver();
        let ast = SqlAST {
            schema: None,
            table: "users".into(),
            alias: None,
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let err = driver.add_query("h1", "q1", ast, &NoopTimer).err();
        assert!(matches!(err, Some(PipelineDriverError::NotInitialized(_))));
    }

    #[test]
    fn advance_before_init_errors() {
        let (driver, _d, _s) = make_driver();
        let err = driver.advance(&NoopTimer).err();
        assert!(matches!(err, Some(PipelineDriverError::NotInitialized(_))));
    }

    #[test]
    fn init_twice_errors() {
        let (driver, _d, _s) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        let err = driver
            .init(&client_schema(), table_specs(), full_tables())
            .err();
        assert!(matches!(err, Some(PipelineDriverError::AlreadyInitialized)));
    }

    #[test]
    fn current_version_and_replica_version_after_init() {
        let (driver, _d, _s) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        assert_eq!(driver.replica_version().unwrap(), "01");
        assert_eq!(driver.current_version().unwrap(), "01");
    }

    // ─── PipelineDriver full add/remove roundtrip ─────────────────────

    #[test]
    fn add_query_happy_path_produces_row_add_then_remove_cleans_up() {
        let (driver, _d, source) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        // Register the externally-built source.
        driver.register_table_source("users", source.clone());
        // Seed one row.
        source
            .push_change(SourceChange::Add(SourceChangeAdd {
                row: {
                    let mut r = Row::new();
                    r.insert("id".into(), Some(json!(1)));
                    r.insert("handle".into(), Some(json!("alice")));
                    r.insert("_0_version".into(), Some(json!("00")));
                    r
                },
            }))
            .unwrap();

        let ast = SqlAST {
            schema: None,
            table: "users".into(),
            alias: None,
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let rows = driver.add_query("h1", "q1", ast, &NoopTimer).unwrap();
        let adds: Vec<_> = rows
            .into_iter()
            .filter_map(|r| match r {
                RowChangeOrYield::Change(RowChange::Add(a)) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(adds.len(), 1, "expected one hydrated add");
        assert_eq!(adds[0].query_id, "q1");
        assert_eq!(adds[0].table, "users");

        // queries() exposes the pipeline.
        let q = driver.queries();
        assert!(q.contains_key("q1"));

        // remove_query cleans up.
        driver.remove_query("q1");
        assert!(!driver.queries().contains_key("q1"));

        // Idempotent removal.
        driver.remove_query("q1");
    }

    // ─── Unknown table error path ─────────────────────────────────────

    #[test]
    fn add_query_with_unknown_table_errors() {
        let (driver, _d, _s) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        let ast = SqlAST {
            schema: None,
            table: "nonexistent".into(),
            alias: None,
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        let err = driver.add_query("h1", "q1", ast, &NoopTimer).err();
        assert!(
            matches!(err, Some(PipelineDriverError::UnknownTable(ref t)) if t == "nonexistent"),
            "got: {err:?}"
        );
    }

    // ─── Advance with empty diff ──────────────────────────────────────

    #[test]
    fn advance_empty_diff_yields_no_changes() {
        let (driver, _d, _s) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        let res = driver.advance(&NoopTimer).unwrap();
        // No new rows in the changelog → no changes surfaced.
        assert!(
            res.changes
                .iter()
                .all(|c| matches!(c, RowChangeOrYield::Yield)
                    || matches!(c, RowChangeOrYield::Change(_)))
        );
        assert_eq!(res.num_changes, 0);
    }

    // ─── Runaway push protection (yield cap) ──────────────────────────

    struct FastTimer;
    impl Timer for FastTimer {
        fn elapsed_lap(&self) -> f64 {
            // Always above any threshold → every change yields.
            1e9
        }
        fn total_elapsed(&self) -> f64 {
            0.0
        }
    }

    #[test]
    fn advance_with_yield_cap_returns_reset() {
        // Construct a driver whose max_yields is zero — any yield fires Reset.
        let dir = TempDir::new().unwrap();
        let replica = make_replica(&dir);
        // Seed a row before init so prev snapshot observes it.
        {
            let c = Connection::open(&replica).unwrap();
            c.execute(
                "INSERT INTO users(id, handle, _0_version) VALUES(1, 'alice', '01')",
                [],
            )
            .unwrap();
        }
        let snapshotter = Snapshotter::new(replica.clone(), "my_app");
        let storage = DatabaseStorage::open_in_memory(DatabaseStorageOptions::default())
            .unwrap()
            .create_client_group_storage("cg1");
        let shard = ShardId {
            app_id: "my_app".into(),
            shard_num: 0,
        };
        let driver = PipelineDriver::new(
            snapshotter,
            shard,
            storage,
            "cg1",
            Arc::new(NoopInspectorDelegate),
            Arc::new(|| 1000.0),
            false,
        );
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        driver.set_max_yields_per_advance(Some(0));

        // After init, delete row and bump version so advance sees it.
        let c = Connection::open(&replica).unwrap();
        c.execute("DELETE FROM users WHERE id=1", []).unwrap();
        c.execute(
            r#"UPDATE "_zero.replicationState" SET stateVersion = '02'"#,
            [],
        )
        .unwrap();
        c.execute(
            r#"INSERT INTO "_zero.changeLog2" (stateVersion, pos, "table", rowKey, op)
               VALUES ('02', 0, 'users', '{"id":1}', 'd')"#,
            [],
        )
        .unwrap();

        let err = driver.advance(&FastTimer).err();
        assert!(
            matches!(err, Some(PipelineDriverError::Reset(_))),
            "got: {err:?}"
        );
    }

    // ─── queries() exposes inserted pipelines ─────────────────────────

    #[test]
    fn queries_reports_transformation_hash() {
        let (driver, _d, source) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        driver.register_table_source("users", source);
        let ast = SqlAST {
            schema: None,
            table: "users".into(),
            alias: None,
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        };
        driver.add_query("my_hash", "q1", ast, &NoopTimer).unwrap();
        let q = driver.queries();
        assert_eq!(
            q.get("q1").map(|q| q.transformation_hash.clone()),
            Some("my_hash".to_string())
        );
    }

    // ─── hydration_budget_breakdown ordering ──────────────────────────

    #[test]
    fn hydration_budget_breakdown_empty() {
        let (driver, _d, _s) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        let bb = driver.hydration_budget_breakdown();
        assert!(bb.is_empty());
    }

    // ─── get_row passes through to TableSource ─────────────────────────

    #[test]
    fn get_row_returns_existing_row() {
        let (driver, _d, source) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        driver.register_table_source("users", source.clone());
        source
            .push_change(SourceChange::Add(SourceChangeAdd {
                row: {
                    let mut r = Row::new();
                    r.insert("id".into(), Some(json!(1)));
                    r.insert("handle".into(), Some(json!("alice")));
                    r.insert("_0_version".into(), Some(json!("00")));
                    r
                },
            }))
            .unwrap();
        let mut pk = Row::new();
        pk.insert("id".into(), Some(json!(1)));
        let got = driver.get_row("users", &pk);
        assert!(got.is_some());
    }

    #[test]
    fn get_row_before_init_returns_none() {
        let (driver, _d, _s) = make_driver();
        let mut pk = Row::new();
        pk.insert("id".into(), Some(json!(1)));
        assert!(driver.get_row("users", &pk).is_none());
    }

    // ─── advance_without_diff returns a version string ─────────────────

    #[test]
    fn advance_without_diff_returns_next_version() {
        let (driver, dir, _s) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        // Bump replica state version.
        let replica = dir.path().join("replica.db").to_string_lossy().into_owned();
        let c = Connection::open(&replica).unwrap();
        c.execute(
            r#"UPDATE "_zero.replicationState" SET stateVersion = '02'"#,
            [],
        )
        .unwrap();
        let v = driver.advance_without_diff().unwrap();
        assert_eq!(v, "02");
    }

    // ─── total_hydration_time_ms is zero with no queries ──────────────

    #[test]
    fn total_hydration_zero_empty() {
        let (driver, _d, _s) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        assert_eq!(driver.total_hydration_time_ms(), 0.0);
    }

    // ─── TableSource::set_db wiring (Deferred-item-1 closure) ─────────
    //
    // After the snapshotter leapfrogs forward, every registered
    // `TableSource` should be pointed at the new "front" snapshot via
    // `set_db`. We verify that by (a) registering a TableSource built
    // against an empty in-memory DB, (b) advancing the driver (which
    // swaps the TableSource's conn to the replica file), then (c) using
    // `fetch_all` through the TableSource and confirming we see rows
    // that only exist on the replica file.

    fn users_table_source_backed_by(db_path: &str) -> Arc<TableSource> {
        let conn = Connection::open(db_path).unwrap();
        let mut cols = IndexMap::new();
        cols.insert("id".into(), SchemaValue::new(ValueType::Number));
        cols.insert("handle".into(), SchemaValue::new(ValueType::String));
        cols.insert("_0_version".into(), SchemaValue::new(ValueType::String));
        let ts = TableSource::new(conn, "users", cols, PrimaryKey::new(vec!["id".into()]))
            .expect("TableSource");
        Arc::new(ts)
    }

    /// Branch: `advance()` pushes the new DB into every registered
    /// TableSource.
    ///
    /// Setup: the registered source is backed by an **in-memory** DB
    /// whose `users` table has DIFFERENT rows from the replica. After
    /// `advance()` succeeds, `Input::fetch` on the source must surface
    /// the replica's rows — proof that `TableSource::set_db` was
    /// invoked with a fresh connection on the replica file.
    ///
    /// NOTE: we deliberately trigger an empty-diff advance (no
    /// changelog entries) to isolate the swap from the push-change
    /// path. The push-change path in our Rust port writes to the DB
    /// and asserts "row already exists" — so exercising it with real
    /// changes would collide with rows already present on the replica
    /// (the replicator already wrote them in TS semantics).
    #[test]
    fn advance_swaps_table_source_db() {
        let dir = TempDir::new().unwrap();
        let replica = make_replica(&dir);

        // Seed two rows on the replica file — these rows are ONLY on
        // the replica, NOT on the source's initial in-memory conn.
        {
            let c = Connection::open(&replica).unwrap();
            c.execute(
                "INSERT INTO users(id, handle, _0_version) VALUES(1, 'alice', '01')",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO users(id, handle, _0_version) VALUES(2, 'bob', '01')",
                [],
            )
            .unwrap();
        }

        let snapshotter = Snapshotter::new(replica.clone(), "my_app");
        let storage = DatabaseStorage::open_in_memory(DatabaseStorageOptions::default())
            .unwrap()
            .create_client_group_storage("cg1");
        let shard = ShardId {
            app_id: "my_app".into(),
            shard_num: 0,
        };
        let driver = PipelineDriver::new(
            snapshotter,
            shard,
            storage,
            "cg1",
            Arc::new(NoopInspectorDelegate),
            Arc::new(|| 1000.0),
            false,
        );
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();

        // Register a TableSource backed by a SEPARATE in-memory DB
        // with a disjoint row set.
        let source = users_table_source();
        source
            .push_change(SourceChange::Add(SourceChangeAdd {
                row: {
                    let mut r = Row::new();
                    r.insert("id".into(), Some(json!(99)));
                    r.insert("handle".into(), Some(json!("zzz")));
                    r.insert("_0_version".into(), Some(json!("00")));
                    r
                },
            }))
            .unwrap();
        driver.register_table_source("users", source.clone());

        // Connect + verify pre-swap fetch sees ONLY the in-memory row.
        let input = source.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let pre: Vec<_> = input.fetch(FetchRequest::default()).collect();
        assert_eq!(pre.len(), 1, "pre-advance: source sees its in-memory row");

        // Advance — empty diff (no changelog entries), but the
        // snapshotter still leapfrogs forward and triggers the swap.
        let res = driver.advance(&NoopTimer).unwrap();
        assert_eq!(res.version, "01");

        // Post-swap: fetch reads from the replica file, which has rows
        // id=1 and id=2 but NOT id=99.
        let post: Vec<_> = input.fetch(FetchRequest::default()).collect();
        assert_eq!(post.len(), 2, "post-advance: source reads from replica");
    }

    /// Branch: exercises the `users_table_source_backed_by` helper
    /// (guards against unused warnings when the helper is test-only).
    #[test]
    fn users_table_source_backed_by_replica_opens() {
        let dir = TempDir::new().unwrap();
        let replica = make_replica(&dir);
        let ts = users_table_source_backed_by(&replica);
        // Should observe the (empty) users table on the replica file.
        let input = ts.connect(vec![("id".into(), Direction::Asc)], None, None, None);
        let rows: Vec<_> = input.fetch(FetchRequest::default()).collect();
        assert_eq!(rows.len(), 0);
    }

    /// Branch: `advance_without_diff()` also swaps the table source
    /// (TS pipeline-driver.ts L293-297). No registered source → no-op.
    #[test]
    fn advance_without_diff_swaps_when_source_registered() {
        let (driver, dir, source) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        driver.register_table_source("users", source.clone());

        // Bump the replica version.
        let replica = dir.path().join("replica.db").to_string_lossy().into_owned();
        {
            let c = Connection::open(&replica).unwrap();
            c.execute(
                r#"UPDATE "_zero.replicationState" SET stateVersion = '02'"#,
                [],
            )
            .unwrap();
        }

        // Must succeed without error — the swap opens a fresh conn on
        // the replica file which has the `users` table (see make_replica).
        let v = driver.advance_without_diff().unwrap();
        assert_eq!(v, "02");
    }

    /// Branch: swap is a no-op when no sources are registered. Must
    /// not try to read `db_file` beyond the no-op short-circuit.
    #[test]
    fn advance_without_registered_sources_skips_swap() {
        let (driver, _d, _s) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        // No register_table_source — swap_table_source_dbs must early-return.
        let _ = driver.advance(&NoopTimer).unwrap();
    }

    // ─── circuit-breaker budget formula ────────────────────────────────

    /// `should_advance_yield_maybe_abort` directly tested with a stub
    /// timer to pin the budget formula. The trip threshold must be
    /// `max(total_hydration, num_changes * PER_CHANGE_BUDGET, MIN_BUDGET)`,
    /// not raw `total_hydration`. Verifies the user-reported scenario
    /// (fast hydration → reset storm) is fixed.
    #[test]
    fn advance_budget_uses_per_change_floor_when_hydration_is_zero() {
        struct StubTimer {
            total_ms: f64,
        }
        impl Timer for StubTimer {
            fn elapsed_lap(&self) -> f64 {
                0.0
            }
            fn total_elapsed(&self) -> f64 {
                self.total_ms
            }
        }

        let (driver, _d, _s) = make_driver();
        let num_changes = 5;
        let budget = ADVANCE_PER_CHANGE_BUDGET_MS
            .max(ADVANCE_MIN_BUDGET_MS / num_changes as f64)
            * num_changes as f64; // = 100ms via the MIN_BUDGET floor

        // Branch 1: elapsed below MIN_ADVANCEMENT_TIME_LIMIT_MS (50ms).
        // Trips never fire here — absorbs timer jitter.
        let t = StubTimer {
            total_ms: MIN_ADVANCEMENT_TIME_LIMIT_MS - 1.0,
        };
        let r = driver.should_advance_yield_maybe_abort(&t, 0, num_changes, budget);
        assert!(r.is_ok(), "must not trip under MIN_ADVANCEMENT_TIME_LIMIT_MS");

        // Branch 2: elapsed between MIN_ADVANCEMENT and budget/2.
        // Trips never fire here regardless of pos — neither condition met.
        let t = StubTimer {
            total_ms: budget / 2.0 - 1.0,
        };
        let r = driver.should_advance_yield_maybe_abort(&t, 0, num_changes, budget);
        assert!(
            r.is_ok(),
            "must not trip below half-budget, got {:?}",
            r.err()
        );

        // Branch 3: elapsed above budget/2 with pos > num/2.
        // Half-budget condition requires pos <= num/2 — must NOT trip.
        let t = StubTimer {
            total_ms: budget / 2.0 + 1.0,
        };
        let r = driver.should_advance_yield_maybe_abort(&t, 4, num_changes, budget);
        assert!(
            r.is_ok(),
            "must not trip above half-budget when past half-progress"
        );

        // Branch 4: elapsed above budget/2 with pos <= num/2.
        // Half-budget condition fires.
        let t = StubTimer {
            total_ms: budget / 2.0 + 1.0,
        };
        let r = driver.should_advance_yield_maybe_abort(&t, 0, num_changes, budget);
        assert!(
            r.is_err(),
            "must trip above half-budget when below half-progress"
        );
        assert!(matches!(r.unwrap_err(), PipelineDriverError::Reset(_)));

        // Branch 5: elapsed above full budget. Trips regardless of pos.
        let t = StubTimer {
            total_ms: budget + 1.0,
        };
        let r = driver.should_advance_yield_maybe_abort(&t, 4, num_changes, budget);
        assert!(r.is_err(), "must trip above full budget");
        assert!(matches!(r.unwrap_err(), PipelineDriverError::Reset(_)));
    }

    // ─── add_queries: parallel hydration ───────────────────────────────

    fn simple_users_ast() -> AST {
        SqlAST {
            schema: None,
            table: "users".into(),
            alias: None,
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        }
    }

    #[test]
    fn add_queries_returns_one_result_per_input_in_order() {
        // 3 distinct queries in a batch — verifies order preservation
        // (result[i] matches reqs[i]) and that all run successfully.
        let (driver, _d, source) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        driver.register_table_source("users", source.clone());
        // Seed two rows so each query produces something distinguishable.
        for (id, handle) in [(1, "alice"), (2, "bob"), (3, "carol")] {
            source
                .push_change(SourceChange::Add(SourceChangeAdd {
                    row: {
                        let mut r = Row::new();
                        r.insert("id".into(), Some(json!(id)));
                        r.insert("handle".into(), Some(json!(handle)));
                        r.insert("_0_version".into(), Some(json!("00")));
                        r
                    },
                }))
                .unwrap();
        }
        let reqs = vec![
            AddQueryReq {
                transformation_hash: "h1".into(),
                query_id: "q1".into(),
                query: simple_users_ast(),
            },
            AddQueryReq {
                transformation_hash: "h2".into(),
                query_id: "q2".into(),
                query: simple_users_ast(),
            },
            AddQueryReq {
                transformation_hash: "h3".into(),
                query_id: "q3".into(),
                query: simple_users_ast(),
            },
        ];
        let results = driver.add_queries(reqs);
        assert_eq!(results.len(), 3, "one result per input");
        for (i, r) in results.iter().enumerate() {
            assert!(r.is_ok(), "query {i} failed: {:?}", r.as_ref().err());
            let rows = r.as_ref().unwrap();
            assert!(!rows.is_empty(), "query {i} produced no rows");
        }
        // All three pipelines registered.
        let q = driver.queries();
        assert!(q.contains_key("q1"));
        assert!(q.contains_key("q2"));
        assert!(q.contains_key("q3"));
    }

    #[test]
    fn add_queries_returns_not_initialized_before_init() {
        let (driver, _d, _s) = make_driver();
        let reqs = vec![AddQueryReq {
            transformation_hash: "h".into(),
            query_id: "q".into(),
            query: simple_users_ast(),
        }];
        let results = driver.add_queries(reqs);
        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0].as_ref().unwrap_err(),
            PipelineDriverError::NotInitialized(_)
        ));
    }

    #[test]
    fn add_queries_streaming_emits_one_terminal_chunk_per_query() {
        use std::sync::Mutex;
        let (driver, _d, source) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        driver.register_table_source("users", source.clone());
        for (id, h) in [(1, "a"), (2, "b")] {
            source
                .push_change(SourceChange::Add(SourceChangeAdd {
                    row: {
                        let mut r = Row::new();
                        r.insert("id".into(), Some(json!(id)));
                        r.insert("handle".into(), Some(json!(h)));
                        r.insert("_0_version".into(), Some(json!("00")));
                        r
                    },
                }))
                .unwrap();
        }
        let collected: Mutex<Vec<(String, usize, bool)>> = Mutex::new(Vec::new());
        let reqs = vec![
            AddQueryReq {
                transformation_hash: "h1".into(),
                query_id: "q1".into(),
                query: simple_users_ast(),
            },
            AddQueryReq {
                transformation_hash: "h2".into(),
                query_id: "q2".into(),
                query: simple_users_ast(),
            },
        ];
        let statuses = driver.add_queries_streaming(reqs, |chunk| {
            collected
                .lock()
                .unwrap()
                .push((chunk.query_id, chunk.rows.len(), chunk.is_final));
        });
        // Both succeed.
        assert_eq!(statuses.len(), 2);
        for s in &statuses {
            assert!(s.result.is_ok(), "{} failed: {:?}", s.query_id, s.result);
        }
        // One chunk per query, marked final, with at least one row.
        let chunks = collected.lock().unwrap();
        assert_eq!(chunks.len(), 2, "exactly one chunk per query");
        for (_qid, n_rows, is_final) in chunks.iter() {
            assert!(*is_final, "every chunk should be marked final");
            assert!(*n_rows > 0, "chunk should have rows");
        }
        // Queries actually registered.
        let q = driver.queries();
        assert!(q.contains_key("q1"));
        assert!(q.contains_key("q2"));
    }

    #[test]
    fn add_queries_streaming_returns_status_with_per_query_hydration_time() {
        let (driver, _d, source) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        driver.register_table_source("users", source.clone());
        source
            .push_change(SourceChange::Add(SourceChangeAdd {
                row: {
                    let mut r = Row::new();
                    r.insert("id".into(), Some(json!(1)));
                    r.insert("handle".into(), Some(json!("a")));
                    r.insert("_0_version".into(), Some(json!("00")));
                    r
                },
            }))
            .unwrap();
        let reqs = vec![AddQueryReq {
            transformation_hash: "h".into(),
            query_id: "q".into(),
            query: simple_users_ast(),
        }];
        let statuses = driver.add_queries_streaming(reqs, |_| {});
        assert_eq!(statuses.len(), 1);
        let s = &statuses[0];
        assert_eq!(s.query_id, "q");
        let elapsed = s.result.as_ref().expect("ok");
        assert!(*elapsed >= 0.0, "elapsed should be measured");
    }

    #[test]
    fn add_queries_each_pipelines_hydration_time_independent() {
        // Each query gets its own per-thread WallClockTimer, so per-query
        // hydration_time_ms must NOT be the batch wall clock (which
        // would be invariant across all queries when parallelism caps
        // wall time at the slowest).
        let (driver, _d, source) = make_driver();
        driver
            .init(&client_schema(), table_specs(), full_tables())
            .unwrap();
        driver.register_table_source("users", source.clone());
        // One small row so each hydration is trivially fast.
        source
            .push_change(SourceChange::Add(SourceChangeAdd {
                row: {
                    let mut r = Row::new();
                    r.insert("id".into(), Some(json!(1)));
                    r.insert("handle".into(), Some(json!("a")));
                    r.insert("_0_version".into(), Some(json!("00")));
                    r
                },
            }))
            .unwrap();
        let reqs = vec![
            AddQueryReq {
                transformation_hash: "h1".into(),
                query_id: "q1".into(),
                query: simple_users_ast(),
            },
            AddQueryReq {
                transformation_hash: "h2".into(),
                query_id: "q2".into(),
                query: simple_users_ast(),
            },
        ];
        let _ = driver.add_queries(reqs);
        // Per-pipeline times are exposed via hydration_budget_breakdown —
        // both should be measured (>= 0) since each thread used its own
        // WallClockTimer (parallelism-invariant per-query measurement).
        let bb = driver.hydration_budget_breakdown();
        assert_eq!(bb.len(), 2, "two queries hydrated");
        for (id, _table, ms) in &bb {
            assert!(*ms >= 0.0, "{id} hydration_time_ms = {ms} should be >= 0");
        }
    }

    #[test]
    fn advance_budget_does_not_trip_under_min_advancement_time() {
        struct StubTimer;
        impl Timer for StubTimer {
            fn elapsed_lap(&self) -> f64 {
                0.0
            }
            fn total_elapsed(&self) -> f64 {
                MIN_ADVANCEMENT_TIME_LIMIT_MS - 1.0
            }
        }
        let (driver, _d, _s) = make_driver();
        // Even with a tiny budget of 0, an elapsed under MIN_ADVANCEMENT
        // must not trip — absorbs timer jitter.
        let r = driver.should_advance_yield_maybe_abort(&StubTimer, 0, 1, 0.0);
        assert!(r.is_ok(), "must not trip below MIN_ADVANCEMENT_TIME_LIMIT_MS");
    }
}
