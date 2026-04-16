//! PipelineV2 driver — multiple queries, IVM API surface.
//!
//! Mirrors the IVM-only slice of the TS `PipelineDriver`:
//! `init / initialized / current_version / add_query / remove_query /
//! advance / get_row / queries / destroy`.
//!
//! Scope of this first cut:
//! - Source factory hooks — caller provides a closure that builds a
//!   source for a given table name.
//! - Each query's `ChainSpec` is provided directly — no AST translation
//!   yet.
//! - `advance()` takes an iterator of `(table, Change)` tuples and routes
//!   each change to the chains that read from that table.
//! - `get_row` looks up a row by table+pk using the source directly.
//!
//! Not implemented:
//! - AST → ChainSpec translation (follow-up).
//! - `advance_without_diff`.
//! - `hydration_budget_breakdown`, `total_hydration_time_ms`.
//! - Parallel `add_queries` via rayon.

use indexmap::IndexMap;
use std::collections::HashMap;

use zero_cache_types::value::Row;

use crate::ivm::change::Change;
use crate::ivm_v2::operator::{FetchRequest, Input};

use super::pipeline::{Chain, ChainSpec};
use super::row_change::RowChange;

/// Event sent from a hydration worker thread back to the driver.
enum HydrationEvent {
    Chunk(Vec<RowChange>),
    Final {
        chain: Chain,
        hydration_time_ms: f64,
    },
    /// Worker panicked during hydration. `next_chunk` surfaces this as
    /// an error to TS so the caller gets an exception — matching TS
    /// where a hydration failure throws, rather than silently returning
    /// a partial result set.
    Error(String),
}

/// Per-query state while hydration is running on a worker thread.
struct InFlightState {
    rx: crossbeam_channel::Receiver<HydrationEvent>,
    worker: Option<std::thread::JoinHandle<()>>,
    table: String,
    transformation_hash: String,
}

/// Result of a single `next_chunk` call.
pub struct NextChunk {
    pub rows: Vec<RowChange>,
    pub is_final: bool,
    pub hydration_time_ms: Option<f64>,
}

/// Factory that creates a fresh source (SQLite-backed or in-memory) for
/// a given table name. Used by `add_query` to plug upstream into a
/// new chain.
pub type SourceFactory = Box<dyn Fn(&str) -> Box<dyn Input> + Send + Sync>;

/// Per-query metadata tracked across the CG.
#[derive(Clone)]
pub struct QueryInfo {
    pub transformation_hash: String,
    pub table: String,
    pub hydration_time_ms: f64,
}

pub struct PipelineV2 {
    /// Queries keyed by queryID; `IndexMap` preserves insertion order.
    chains: IndexMap<String, Chain>,
    infos: IndexMap<String, QueryInfo>,
    source_factory: SourceFactory,
    initialized: bool,
    replica_version: Option<String>,
    current_version: Option<String>,
    /// Lookup sources — separate from chains so `get_row` can query a
    /// table without going through any per-query chain.
    lookup_sources: IndexMap<String, Box<dyn Input>>,
    /// Hydrations currently running on worker threads, keyed by
    /// queryID. Entry is moved into `chains`/`infos` when the worker
    /// emits the `Final` event.
    in_flight: HashMap<String, InFlightState>,
}

/// One entry in an `add_queries` batch.
pub struct AddQueryReq {
    pub transformation_hash: String,
    pub spec: ChainSpec,
}

/// Result of `add_queries` for a single request — preserves input order.
pub struct AddQueryResult {
    pub query_id: String,
    pub hydrated: Vec<RowChange>,
    pub hydration_time_ms: f64,
}

/// Per-query hydration cost, matching TS `hydrationBudgetBreakdown`.
pub struct HydrationBreakdownItem {
    pub id: String,
    pub table: String,
    pub ms: f64,
}

impl PipelineV2 {
    pub fn new(source_factory: SourceFactory) -> Self {
        Self {
            chains: IndexMap::new(),
            infos: IndexMap::new(),
            source_factory,
            initialized: false,
            replica_version: None,
            current_version: None,
            lookup_sources: IndexMap::new(),
            in_flight: HashMap::new(),
        }
    }

    /// TS `init(client_schema)` — for now, just a state flip. Schema
    /// validation + tableSpec/fullTables wiring will land with the
    /// AST builder port.
    pub fn init(&mut self, replica_version: String) {
        self.replica_version = Some(replica_version.clone());
        self.current_version = Some(replica_version);
        self.initialized = true;
    }

    pub fn initialized(&self) -> bool {
        self.initialized
    }

    pub fn replica_version(&self) -> Option<&str> {
        self.replica_version.as_deref()
    }
    pub fn current_version(&self) -> Option<&str> {
        self.current_version.as_deref()
    }

    /// TS `add_query(transformation_hash, query_id, ast)`. Hydrates and
    /// returns the initial Add stream.
    pub fn add_query(
        &mut self,
        transformation_hash: String,
        spec: ChainSpec,
    ) -> Vec<RowChange> {
        let query_id = spec.query_id.clone();
        let table = spec.table.clone();
        let source = (self.source_factory)(&table);
        let t0 = std::time::Instant::now();
        let mut chain = Chain::build(spec, source);
        let rows = chain.hydrate();
        let hydration_time_ms = t0.elapsed().as_secs_f64() * 1000.0;
        eprintln!(
            "[TRACE ivm_v2] PipelineV2::add_query q={} table={} hyd_ms={:.2} rows={}",
            query_id,
            table,
            hydration_time_ms,
            rows.len()
        );
        self.chains.insert(query_id.clone(), chain);
        self.infos.insert(
            query_id,
            QueryInfo {
                transformation_hash,
                table,
                hydration_time_ms,
            },
        );
        rows
    }

    /// TS `remove_query(query_id)` — drop chain + metadata. Aborts any
    /// in-flight hydration by dropping its receiver (the worker will see
    /// its send fail and exit cleanly).
    pub fn remove_query(&mut self, query_id: &str) {
        self.chains.shift_remove(query_id);
        self.infos.shift_remove(query_id);
        if let Some(state) = self.in_flight.remove(query_id) {
            // rx is dropped here → worker's next send fails → worker cancels
            // → hydrate_stream returns → worker sends Final (fails) → exits.
            drop(state.rx);
            if let Some(w) = state.worker {
                let _ = w.join();
            }
        }
    }

    /// Start streaming hydration for a new query on a worker thread.
    /// Returns immediately; rows are drained via `next_chunk(query_id)`.
    ///
    /// The worker owns the chain while hydrating and sends it back via
    /// the `Final` event when done, at which point the driver parks it
    /// in `chains` for subsequent `advance()` calls.
    pub fn start_hydration(&mut self, transformation_hash: String, spec: ChainSpec) {
        let query_id = spec.query_id.clone();
        let table = spec.table.clone();

        // Idempotent cleanup for matching-id pre-existing state.
        self.remove_query(&query_id);

        // Build chain on the main thread — Chain construction touches
        // source_factory which isn't Send.
        let source = (self.source_factory)(&table);
        let child_source = spec
            .join
            .as_ref()
            .map(|js| (self.source_factory)(&js.child_table));
        // TS native `buildPipelineInternal` recursively builds a
        // full `Input` for each EXISTS subquery (source + filter +
        // nested Exists/Join/Take etc). Our RS equivalent: for each
        // `ExistsSpec.child_subquery`, run `ast_to_chain_spec` on the
        // subquery AST and recursively build a Chain — then hand the
        // Chain-as-Input to the parent `ExistsT`. This supports
        // nested EXISTS to arbitrary depth, matching TS semantics.
        //
        // `child_primary_key` is set from the *source* schema of the
        // subquery's root table so `hydrate_stream` can emit
        // subquery-tree rows with a proper rowKey (TS
        // `Streamer#streamNodes` uses the child schema's primaryKey
        // for the same purpose).
        let mut spec = spec;
        let mut exists_child_inputs: Vec<Option<Box<dyn crate::ivm_v2::operator::Input>>> =
            Vec::with_capacity(spec.exists.iter().count() + spec.exists_chain.len());
        fn build_child_input(
            es: &mut crate::view_syncer_v2::pipeline::ExistsSpec,
            source_factory: &SourceFactory,
        ) -> Option<Box<dyn crate::ivm_v2::operator::Input>> {
            let tbl = es.child_table.as_deref()?;
            // Recursive sub-Chain for the subquery's full AST.
            if let Some(sub_ast) = es.child_subquery.as_deref() {
                use crate::view_syncer_v2::ast_builder::ast_to_chain_spec;
                let sub_source = (source_factory)(tbl);
                let sub_pk = sub_source.get_schema().primary_key.clone();
                es.child_primary_key = Some(sub_pk.clone());
                let sub_spec =
                    ast_to_chain_spec(sub_ast, format!("exists-sub:{}", tbl), sub_pk)
                        .ok()?;
                let mut sub_spec_mut = sub_spec;
                let mut nested_inputs: Vec<Option<Box<dyn crate::ivm_v2::operator::Input>>> =
                    Vec::new();
                if let Some(sub_es) = sub_spec_mut.exists.as_mut() {
                    nested_inputs.push(build_child_input(sub_es, source_factory));
                }
                for sub_es in sub_spec_mut.exists_chain.iter_mut() {
                    nested_inputs.push(build_child_input(sub_es, source_factory));
                }
                let sub_chain =
                    crate::view_syncer_v2::pipeline::Chain::build_with_join_and_exists(
                        sub_spec_mut,
                        sub_source,
                        None,
                        nested_inputs,
                    );
                // Snapshot the sub-Chain's grandchild metadata so the
                // PARENT Chain's `exists_child_tables` can include it
                // under this relationship's entry. Matches TS
                // `Streamer#streamNodes` recursing through nested
                // `schema.relationships`.
                es.child_exists_child_tables =
                    Some(sub_chain.exists_child_tables_snapshot());
                Some(Box::new(sub_chain) as Box<dyn crate::ivm_v2::operator::Input>)
            } else {
                // Legacy test path — simple source (+ optional filter
                // from a pre-compiled predicate).
                let src = (source_factory)(tbl);
                es.child_primary_key = Some(src.get_schema().primary_key.clone());
                Some(match es.child_predicate.clone() {
                    Some(pred) => Box::new(crate::ivm_v2::filter::Filter::new(src, pred)),
                    None => src,
                })
            }
        }
        if let Some(es) = spec.exists.as_mut() {
            exists_child_inputs.push(build_child_input(es, &self.source_factory));
        }
        for es in spec.exists_chain.iter_mut() {
            exists_child_inputs.push(build_child_input(es, &self.source_factory));
        }
        let chain = Chain::build_with_join_and_exists(
            spec,
            source,
            child_source,
            exists_child_inputs,
        );

        // 4-deep chunk pipeline is enough to keep the worker busy while
        // TS consumes; bounded so memory stays capped.
        let (tx, rx) = crossbeam_channel::bounded::<HydrationEvent>(4);

        let worker = std::thread::spawn(move || {
            let send_tx = tx.clone();
            // Catch panics inside hydrate_stream so a bug in an operator
            // surfaces as an Error event (and therefore a TS exception)
            // instead of a silently-truncated result.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let mut chain = chain;
                let t0 = std::time::Instant::now();
                chain.hydrate_stream(|chunk, _is_final| {
                    // Cancel cleanly if the driver dropped rx (remove_query
                    // / destroy / TS generator abandoned).
                    send_tx.send(HydrationEvent::Chunk(chunk)).is_ok()
                });
                let hydration_time_ms = t0.elapsed().as_secs_f64() * 1000.0;
                (chain, hydration_time_ms)
            }));
            match outcome {
                Ok((chain, hydration_time_ms)) => {
                    let _ = tx.send(HydrationEvent::Final {
                        chain,
                        hydration_time_ms,
                    });
                }
                Err(panic) => {
                    let msg = panic
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "hydration worker panicked".to_string());
                    let _ = tx.send(HydrationEvent::Error(msg));
                }
            }
        });

        self.in_flight.insert(
            query_id,
            InFlightState {
                rx,
                worker: Some(worker),
                table,
                transformation_hash,
            },
        );
    }

    /// Block on the next chunk from an in-flight hydration. Returns
    /// `Ok(NextChunk { is_final: false, .. })` for chunk events and
    /// `Ok(NextChunk { is_final: true, .. })` on the terminal event.
    /// Returns `Err(msg)` if the worker failed (TS surfaces this as an
    /// exception, matching TS where hydration errors throw).
    ///
    /// On the final event, the chain is moved into `chains` and the
    /// `QueryInfo` into `infos` so that subsequent `advance()` and
    /// `queries()` calls see the query.
    pub fn next_chunk(&mut self, query_id: &str) -> Result<NextChunk, String> {
        let event = match self.in_flight.get_mut(query_id) {
            Some(state) => state.rx.recv(),
            None => {
                // Query isn't in flight — treat as final (nothing more
                // to emit). Mirrors TS's idempotent generator shape.
                return Ok(NextChunk {
                    rows: Vec::new(),
                    is_final: true,
                    hydration_time_ms: None,
                });
            }
        };
        match event {
            Ok(HydrationEvent::Chunk(rows)) => Ok(NextChunk {
                rows,
                is_final: false,
                hydration_time_ms: None,
            }),
            Ok(HydrationEvent::Final {
                chain,
                hydration_time_ms,
            }) => {
                let state = self
                    .in_flight
                    .remove(query_id)
                    .expect("state was present above");
                if let Some(w) = state.worker {
                    let _ = w.join();
                }
                self.chains.insert(query_id.to_string(), chain);
                self.infos.insert(
                    query_id.to_string(),
                    QueryInfo {
                        transformation_hash: state.transformation_hash,
                        table: state.table,
                        hydration_time_ms,
                    },
                );
                Ok(NextChunk {
                    rows: Vec::new(),
                    is_final: true,
                    hydration_time_ms: Some(hydration_time_ms),
                })
            }
            Ok(HydrationEvent::Error(msg)) => {
                // Worker caught a panic inside hydrate_stream and sent
                // Error. Drop the in-flight entry and propagate.
                if let Some(state) = self.in_flight.remove(query_id) {
                    if let Some(w) = state.worker {
                        let _ = w.join();
                    }
                }
                Err(msg)
            }
            Err(_) => {
                // Channel closed without Final or Error (e.g. worker
                // thread exited abnormally before sending anything). Treat
                // as error rather than silent completion.
                if let Some(state) = self.in_flight.remove(query_id) {
                    if let Some(w) = state.worker {
                        let _ = w.join();
                    }
                }
                Err(format!(
                    "hydration worker for {query_id} exited without Final"
                ))
            }
        }
    }

    /// Route `change` to every chain whose table matches `table`. If
    /// the chain is a join and `table` matches its child table,
    /// route through `advance_child` instead.
    pub fn advance(
        &mut self,
        table: &str,
        change: Change,
    ) -> Vec<RowChange> {
        let mut out = Vec::new();
        for (qid, chain) in self.chains.iter_mut() {
            let Some(_info) = self.infos.get(qid) else { continue };
            let parent_matches = chain.table() == table;
            let child_matches = chain.child_table() == Some(table);
            if parent_matches {
                let c = shallow_clone_change(&change);
                out.extend(chain.advance(c));
            } else if child_matches {
                let c = shallow_clone_change(&change);
                out.extend(chain.advance_child(c));
            }
        }
        out
    }

    pub fn queries(&self) -> &IndexMap<String, QueryInfo> {
        &self.infos
    }

    /// TS `add_queries(batch)` — batched hydration. Sequential for now;
    /// rayon parallelism is an orthogonal follow-up.
    pub fn add_queries(&mut self, batch: Vec<AddQueryReq>) -> Vec<AddQueryResult> {
        let mut out = Vec::with_capacity(batch.len());
        for req in batch {
            let query_id = req.spec.query_id.clone();
            let t0 = std::time::Instant::now();
            let hydrated = self.add_query(req.transformation_hash, req.spec);
            let hydration_time_ms = t0.elapsed().as_secs_f64() * 1000.0;
            out.push(AddQueryResult {
                query_id,
                hydrated,
                hydration_time_ms,
            });
        }
        out
    }

    /// TS `advance_without_diff()` — bump version only, no change diffing.
    pub fn advance_without_diff(&mut self, new_version: String) {
        self.current_version = Some(new_version);
    }

    /// TS `get_row(table, pk_row)` — fetch one row by PK.
    /// Returns `None` if not found. Delegates to the `Input::get_row`
    /// method, which `SqliteSource` overrides with an indexed SELECT
    /// by PK. The lookup source is lazy-built per table and cached.
    pub fn get_row(&mut self, table: &str, pk_row: &Row) -> Option<Row> {
        if !self.lookup_sources.contains_key(table) {
            let src = (self.source_factory)(table);
            self.lookup_sources.insert(table.to_string(), src);
        }
        let src = self.lookup_sources.get_mut(table)?;
        src.get_row(pk_row).map(|n| n.row)
    }

    /// TS `hydration_budget_breakdown()`.
    pub fn hydration_budget_breakdown(&self) -> Vec<HydrationBreakdownItem> {
        self.infos
            .iter()
            .map(|(id, info)| HydrationBreakdownItem {
                id: id.clone(),
                table: info.table.clone(),
                ms: info.hydration_time_ms,
            })
            .collect()
    }

    /// TS `total_hydration_time_ms()`.
    pub fn total_hydration_time_ms(&self) -> f64 {
        self.infos.values().map(|i| i.hydration_time_ms).sum()
    }

    /// TS `destroy()` — drop all chains. Source and upstream cleanups
    /// run via Drop.
    pub fn destroy(&mut self) {
        // Abort any in-flight hydrations before clearing state. Dropping
        // each receiver causes the worker's next send to fail; joining
        // waits for the worker to exit so the chain is properly dropped.
        for (_qid, state) in self.in_flight.drain() {
            drop(state.rx);
            if let Some(w) = state.worker {
                let _ = w.join();
            }
        }
        self.chains.clear();
        self.infos.clear();
        self.lookup_sources.clear();
        self.initialized = false;
    }

    /// TS `reset(clientSchema)` — clears all per-query state but keeps
    /// `initialized = true`. The caller re-registers table metadata and
    /// may call `init` again to refresh `replica_version`.
    ///
    /// Parity with TS `PipelineDriver.reset`: TS tears down every pipeline
    /// input + companions and clears `#pipelines` / `#tables` /
    /// `#allTableNames`, then re-runs `#initAndResetCommon`. This method
    /// is the Rust-side half of that operation; the TS wrapper runs the
    /// JS-side half.
    pub fn reset(&mut self) {
        for (_qid, state) in self.in_flight.drain() {
            drop(state.rx);
            if let Some(w) = state.worker {
                let _ = w.join();
            }
        }
        self.chains.clear();
        self.infos.clear();
        self.lookup_sources.clear();
        // initialized stays true; replica_version is refreshed by the
        // TS wrapper's follow-up `init` call.
    }
}

fn shallow_clone_change(c: &Change) -> Change {
    use crate::ivm::change::{AddChange, ChildChange, ChildSpec, EditChange, RemoveChange};
    use crate::ivm::data::Node;
    fn shallow_node(n: &Node) -> Node {
        Node {
            row: n.row.clone(),
            relationships: indexmap::IndexMap::new(),
        }
    }
    match c {
        Change::Add(a) => Change::Add(AddChange {
            node: shallow_node(&a.node),
        }),
        Change::Remove(r) => Change::Remove(RemoveChange {
            node: shallow_node(&r.node),
        }),
        Change::Edit(e) => Change::Edit(EditChange {
            node: shallow_node(&e.node),
            old_node: shallow_node(&e.old_node),
        }),
        Change::Child(c) => Change::Child(ChildChange {
            node: shallow_node(&c.node),
            child: ChildSpec {
                relationship_name: c.child.relationship_name.clone(),
                change: Box::new(shallow_clone_change(&c.child.change)),
            },
        }),
    }
}
