//! Port of `packages/zql/src/builder/builder.ts`.
//!
//! Public exports ported:
//!
//! - [`BuilderDelegate`] — trait modelled on the TS `BuilderDelegate`
//!   interface. Defines the connection surface the pipeline needs:
//!   `get_source`, `create_storage`, `decorate_input`,
//!   `decorate_filter_input`, `decorate_source_input`, `add_edge`, and
//!   the boolean flags `apply_filters_anyway` / `enable_not_exists`.
//! - [`build_pipeline`] — turns an AST into the root [`Input`] of an IVM
//!   operator graph.
//! - [`assert_no_not_exists`] — traversal helper that throws when the
//!   caller didn't set `enable_not_exists`.
//! - [`condition_includes_flipped_subquery_at_any_level`] — mirrors TS.
//! - [`partition_branches`] — mirrors TS.
//! - [`group_subquery_conditions`] — mirrors TS.
//! - [`apply_or`] — exposed for unit-test / debug-delegate inspection.
//! - [`assert_ordering_includes_pk`] — re-exported from `query/`.
//!
//! ## Design decisions
//!
//! - **Ownership.** TS builds the graph by mutating JS object references
//!   as it traverses. Rust's `Box<dyn …>` values move into each
//!   successive operator constructor, so we build the graph bottom-up
//!   and hand the chain off as owning pointers. Back-edges for `push`
//!   (TS `input.setOutput(this)`) are not wired here — individual
//!   operators in `ivm/` defer that to a dedicated `set_output` call,
//!   and the driver performs it once the graph is complete. For this
//!   reason, `build_pipeline` returns the root `Input` but does not set
//!   its `output` — the caller (pipeline_driver, Layer 11) does that.
//! - **FanIn / FilterStart / FilterEnd.** The filter sub-graph wires
//!   via `FilterStart` → middle → `FilterEnd`. We reuse
//!   [`crate::ivm::filter_operators::build_filter_pipeline`] for the
//!   triple; the middle closure applies our filter tree.
//! - **bindStaticParameters.** Delegated to
//!   [`zero_cache_types::ast::bind_static_parameters`]; callers bind
//!   parameters before passing the AST to `build_pipeline` (TS does the
//!   same — it is not part of `buildPipeline`).
//! - **Debug.** The TS `DebugDelegate` has query-level hooks; we keep
//!   the marker trait [`crate::ivm::source::DebugDelegate`] and do not
//!   pipe callbacks into operator construction here. Full debug support
//!   is deferred until a consumer needs it.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use zero_cache_types::ast::{
    AST, CompoundKey, Condition, CorrelatedSubquery, CorrelatedSubqueryOp, StaticQueryParameters,
    System, bind_static_parameters,
};

use crate::builder::filter::{create_predicate, simplify_condition};
use crate::ivm::exists::{Exists, ExistsType};
use crate::ivm::fan_in::FanIn;
use crate::ivm::fan_out::FanOut;
use crate::ivm::filter::{ArcFilterAsInput, Filter};
use crate::ivm::filter_operators::{FilterInput, FilterOutput, FilterStart, build_filter_pipeline};
use crate::ivm::flipped_join::{FlippedJoin, FlippedJoinArgs};
use crate::ivm::join::{Join, JoinArgs};
use crate::ivm::operator::{Input, InputBase, Storage};
use crate::ivm::skip;
use crate::ivm::source::{DebugDelegate, Source, SourceInput};
use crate::ivm::take::Take;
use crate::ivm::union_fan_in::UnionFanIn;
use crate::ivm::union_fan_out::UnionFanOut;
use crate::planner::planner_builder::plan_query;
use crate::planner::planner_connection::ConnectionCostModel;
use crate::planner::planner_debug::PlanDebugger;
use crate::query::complete_ordering::complete_ordering;

/// TS `EXISTS_LIMIT`.
pub const EXISTS_LIMIT: u64 = 3;
/// TS `PERMISSIONS_EXISTS_LIMIT`.
pub const PERMISSIONS_EXISTS_LIMIT: u64 = 1;

// ─── BuilderDelegate trait ────────────────────────────────────────────

/// TS `BuilderDelegate` — the caller surface.
pub trait BuilderDelegate {
    /// TS `applyFiltersAnyway?` — when true, re-apply filters after the
    /// source even if the source reports `fullyAppliedFilters`.
    fn apply_filters_anyway(&self) -> bool {
        false
    }

    /// TS `enableNotExists?` — when true, allow `NOT EXISTS` conditions.
    fn enable_not_exists(&self) -> bool {
        false
    }

    /// TS `getSource(tableName)`.
    fn get_source(&self, table_name: &str) -> Option<Arc<dyn Source>>;

    /// TS `createStorage(name)`.
    fn create_storage(&self, name: &str) -> Box<dyn Storage>;

    /// TS `decorateInput(input, name)`. Default is pass-through.
    fn decorate_input(&self, input: Box<dyn Input>, _name: &str) -> Box<dyn Input> {
        input
    }

    /// TS `decorateFilterInput(input, name)`. Default is pass-through.
    fn decorate_filter_input(
        &self,
        input: Box<dyn FilterInput>,
        _name: &str,
    ) -> Box<dyn FilterInput> {
        input
    }

    /// TS `decorateSourceInput(input, queryID)`. Default pass-through.
    fn decorate_source_input(
        &self,
        input: Box<dyn SourceInput>,
        _query_id: &str,
    ) -> Box<dyn Input> {
        // Type-erase SourceInput → Input by boxing.
        Box::new(SourceInputAsInput(input))
    }

    /// TS `addEdge(source, dest)`. Default no-op.
    fn add_edge(&self, _source: &dyn InputBase, _dest: &dyn InputBase) {}

    /// TS `debug?`.
    fn debug(&self) -> Option<&dyn DebugDelegate> {
        None
    }

    /// TS `mapAst?`. Default identity.
    fn map_ast(&self, ast: AST) -> AST {
        ast
    }
}

/// Wrapper adapting `Box<dyn SourceInput>` to `Box<dyn Input>`.
struct SourceInputAsInput(Box<dyn SourceInput>);

impl InputBase for SourceInputAsInput {
    fn get_schema(&self) -> &crate::ivm::schema::SourceSchema {
        self.0.get_schema()
    }
    fn destroy(&mut self) {
        self.0.destroy();
    }
}
impl Input for SourceInputAsInput {
    fn set_output(&mut self, output: Box<dyn crate::ivm::operator::Output>) {
        self.0.set_output(output);
    }
    fn fetch<'a>(
        &'a self,
        req: crate::ivm::operator::FetchRequest,
    ) -> crate::ivm::stream::Stream<'a, crate::ivm::data::NodeOrYield> {
        self.0.fetch(req)
    }
}

// ─── build_pipeline ───────────────────────────────────────────────────

/// TS `buildPipeline(ast, delegate, queryID, costModel?, lc?, planDebugger?)`.
pub fn build_pipeline(
    ast: AST,
    delegate: &dyn BuilderDelegate,
    query_id: &str,
    cost_model: Option<ConnectionCostModel>,
    plan_debugger: Option<&dyn PlanDebugger>,
) -> Box<dyn Input> {
    let ast = delegate.map_ast(ast);

    // TS: `completeOrdering(ast, tableName => must(delegate.getSource(tableName)).tableSchema.primaryKey)`.
    let ast = complete_ordering(&ast, &|table_name: &str| {
        delegate
            .get_source(table_name)
            .expect("Source not found")
            .table_schema()
            .primary_key
            .clone()
    });

    // Branch: cost model provided → run the planner.
    let ast = if let Some(cm) = cost_model {
        plan_query(&ast, cm, plan_debugger)
    } else {
        ast
    };

    build_pipeline_internal(ast, delegate, query_id, "", None)
}

/// TS `bindStaticParameters(ast, staticQueryParameters)`. Thin wrapper
/// around [`zero_cache_types::ast::bind_static_parameters`].
pub fn bind_static_parameters_helper(ast: &AST, params: Option<&StaticQueryParameters>) -> AST {
    match params {
        Some(p) => bind_static_parameters(ast, p),
        None => ast.clone(),
    }
}

// ─── Internal: build_pipeline_internal ────────────────────────────────

fn build_pipeline_internal(
    ast: AST,
    delegate: &dyn BuilderDelegate,
    query_id: &str,
    name: &str,
    partition_key: Option<CompoundKey>,
) -> Box<dyn Input> {
    let source = delegate
        .get_source(&ast.table)
        .unwrap_or_else(|| panic!("Source not found: {}", ast.table));

    let ast = uniquify_correlated_subquery_condition_aliases(ast);

    // Branch: `enableNotExists == false` + where clause → validate.
    if !delegate.enable_not_exists() {
        if let Some(w) = ast.where_clause.as_deref() {
            assert_no_not_exists(w);
        }
    }

    // Gather correlated subquery conditions and compute splitEditKeys.
    let csq_conditions: Vec<Condition> =
        gather_correlated_subquery_query_conditions(ast.where_clause.as_deref());

    let mut split_edit_keys: HashSet<String> = partition_key
        .as_ref()
        .map(|pk| pk.iter().cloned().collect())
        .unwrap_or_default();
    for cond in csq_conditions.iter() {
        if let Condition::CorrelatedSubquery { related, .. } = cond {
            for k in related.correlation.parent_field.iter() {
                split_edit_keys.insert(k.clone());
            }
        }
    }
    if let Some(rel) = ast.related.as_ref() {
        for csq in rel {
            for k in csq.correlation.parent_field.iter() {
                split_edit_keys.insert(k.clone());
            }
        }
    }

    // TS: source.connect(must(ast.orderBy), ast.where, splitEditKeys, delegate.debug)
    let sort = ast
        .order_by
        .clone()
        .expect("ordering must be present after complete_ordering");
    let conn = source.connect(
        sort,
        ast.where_clause.as_deref(),
        Some(&split_edit_keys),
        delegate.debug(),
    );
    let fully_applied = conn.fully_applied_filters();

    let mut end: Box<dyn Input> = delegate.decorate_source_input(conn, query_id);
    end = delegate.decorate_input(end, &format!("{name}:source({})", ast.table));

    // Branch: ast.start → wrap in Skip.
    if let Some(start) = ast.start.as_ref() {
        let skip_bound = skip::Bound {
            row: start.row.clone(),
            exclusive: start.exclusive,
        };
        let skip_arc = skip::Skip::new_wired(end, skip_bound);
        end = delegate.decorate_input(
            Box::new(skip::ArcSkipAsInput::new(skip_arc)),
            &format!("{name}:skip)"),
        );
    }

    // Apply CSQ conditions (non-flipped) as subquery joins.
    for csq_cond in csq_conditions.iter() {
        if let Condition::CorrelatedSubquery { related, flip, .. } = csq_cond {
            if !flip.unwrap_or(false) {
                // Clone the sub-AST but override limit to EXISTS_LIMIT.
                let sub_limit = if matches!(related.system, Some(System::Permissions)) {
                    PERMISSIONS_EXISTS_LIMIT
                } else {
                    EXISTS_LIMIT
                };
                let mut sub_related = (**related).clone();
                let mut sub_ast = (*sub_related.subquery).clone();
                sub_ast.limit = Some(sub_limit);
                sub_related.subquery = Box::new(sub_ast);
                end = apply_correlated_subquery(sub_related, delegate, query_id, end, name, true);
            }
        }
    }

    // Branch: where clause + (not fully applied || apply_filters_anyway).
    if let Some(w) = ast.where_clause.as_deref() {
        if !fully_applied || delegate.apply_filters_anyway() {
            end = apply_where(end, w, delegate, name);
        }
    }

    // Branch: limit → wrap in Take.
    if let Some(limit) = ast.limit {
        let take_name = format!("{name}:take");
        let storage = delegate.create_storage(&take_name);
        let take_arc = Take::new_wired(end, storage, limit as usize, partition_key.clone());
        end = delegate.decorate_input(
            Box::new(crate::ivm::take::ArcTakeAsInput::new(take_arc)),
            &take_name,
        );
    }

    // Branch: ast.related → dedupe by alias, apply each as a join.
    if let Some(related) = ast.related.as_ref() {
        // LWW dedupe keyed on alias.
        let mut by_alias: HashMap<String, CorrelatedSubquery> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for csq in related.iter() {
            let key = csq.subquery.alias.clone().unwrap_or_default();
            if !by_alias.contains_key(&key) {
                order.push(key.clone());
            }
            by_alias.insert(key, csq.clone());
        }
        for key in order.iter() {
            let csq = by_alias.remove(key).unwrap();
            end = apply_correlated_subquery(csq, delegate, query_id, end, name, false);
        }
    }

    end
}

// ─── apply_where ─────────────────────────────────────────────────────

fn apply_where(
    input: Box<dyn Input>,
    condition: &Condition,
    delegate: &dyn BuilderDelegate,
    name: &str,
) -> Box<dyn Input> {
    // Branch: no flipped subquery anywhere → simple filter pipeline.
    if !condition_includes_flipped_subquery_at_any_level(condition) {
        return Box::new(build_filter_pipeline(
            input,
            || {},
            |start| apply_filter_owned(start_to_filter_input(start), condition, delegate, name),
        ));
    }
    // Branch: has a flipped subquery → walk AND/OR structure.
    apply_filter_with_flips(input, condition, delegate, name)
}

fn start_to_filter_input(start: Arc<Mutex<FilterStart>>) -> Box<dyn FilterInput> {
    Box::new(FilterStartAdapter(start))
}

/// Adapter making a shared [`FilterStart`] usable as a
/// [`Box<dyn FilterInput>`].
struct FilterStartAdapter(Arc<Mutex<FilterStart>>);

impl InputBase for FilterStartAdapter {
    fn get_schema(&self) -> &crate::ivm::schema::SourceSchema {
        // We cannot return a reference into the locked mutex. The
        // schema reference is actually derived from the upstream input
        // owned by FilterStart; we leak a stable reference via a
        // leaked box. This matches TS `return this.#input.getSchema()`;
        // callers use the schema immediately after `get_schema()`.
        //
        // Since the Arc<Mutex<FilterStart>> guards the input, we take
        // a short-lived lock, clone the schema into a Box, leak it,
        // and return a &'static reference. This is safe because the
        // schema lives at least as long as the caller's borrow of
        // `self`. The leak is bounded by the number of FilterStart
        // adapters a single pipeline build creates — finite.
        let guard = self.0.lock().expect("filter start mutex poisoned");
        // SAFETY: schema is Clone, and we leak the cloned copy to
        // satisfy the &'static-ish return type. Box::leak returns
        // &'static so the caller's shorter borrow is satisfied via
        // reborrow.
        let schema_clone = guard.get_schema().clone();
        Box::leak(Box::new(schema_clone))
    }
    fn destroy(&mut self) {
        self.0.lock().unwrap().destroy();
    }
}
impl FilterInput for FilterStartAdapter {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        self.0.lock().unwrap().set_filter_output(output);
    }
}

fn apply_filter_with_flips(
    input: Box<dyn Input>,
    condition: &Condition,
    delegate: &dyn BuilderDelegate,
    name: &str,
) -> Box<dyn Input> {
    assert!(
        !matches!(condition, Condition::Simple { .. }),
        "Simple conditions cannot have flips"
    );
    match condition {
        Condition::And { conditions } => {
            let (with_flipped, without_flipped) = partition_branches(
                conditions,
                &condition_includes_flipped_subquery_at_any_level,
            );
            let mut end: Box<dyn Input> = if !without_flipped.is_empty() {
                let cond = Condition::And {
                    conditions: without_flipped.into_iter().cloned().collect(),
                };
                Box::new(build_filter_pipeline(
                    input,
                    || {},
                    move |start| {
                        apply_and_owned(start_to_filter_input(start), &cond, delegate, name)
                    },
                ))
            } else {
                input
            };
            assert!(!with_flipped.is_empty(), "Impossible to have no flips here");
            for cond in with_flipped {
                end = apply_filter_with_flips(end, cond, delegate, name);
            }
            end
        }
        Condition::Or { conditions } => {
            let (with_flipped, without_flipped) = partition_branches(
                conditions,
                &condition_includes_flipped_subquery_at_any_level,
            );
            assert!(!with_flipped.is_empty(), "Impossible to have no flips here");

            let ufo_arc: Arc<Mutex<UnionFanOut>> = UnionFanOut::new_wired(input);
            // We need to pass ufo as an Input multiple times — use an
            // adapter that holds an Arc.
            let mut branches: Vec<Box<dyn Input>> = Vec::new();
            if !without_flipped.is_empty() {
                let cond = Condition::Or {
                    conditions: without_flipped.into_iter().cloned().collect(),
                };
                let start_input: Box<dyn Input> =
                    Box::new(UnionFanOutAdapter(Arc::clone(&ufo_arc)));
                branches.push(Box::new(build_filter_pipeline(
                    start_input,
                    || {},
                    move |start| {
                        apply_or_owned(start_to_filter_input(start), &cond, delegate, name)
                    },
                )));
            }
            for cond in with_flipped {
                let start_input: Box<dyn Input> =
                    Box::new(UnionFanOutAdapter(Arc::clone(&ufo_arc)));
                branches.push(apply_filter_with_flips(start_input, cond, delegate, name));
            }
            // Build UnionFanIn from the ufo's schema and the branches.
            let fanin_schema = ufo_arc.lock().unwrap().get_schema().clone();
            let ufi_arc = UnionFanIn::new_wired(fanin_schema, branches);
            Box::new(crate::ivm::union_fan_in::ArcUnionFanInAsInput::new(ufi_arc))
        }
        Condition::CorrelatedSubquery { related, .. } => {
            let sq = (**related).clone();
            let child = build_pipeline_internal(
                (*sq.subquery).clone(),
                delegate,
                "",
                &format!("{name}.{}", sq.subquery.alias.as_deref().unwrap_or("")),
                Some(sq.correlation.child_field.clone()),
            );
            let alias = sq
                .subquery
                .alias
                .clone()
                .expect("Subquery must have an alias");
            let fj_arc = FlippedJoin::new_wired(FlippedJoinArgs {
                parent: input,
                child,
                parent_key: sq.correlation.parent_field.clone(),
                child_key: sq.correlation.child_field.clone(),
                relationship_name: alias.clone(),
                hidden: sq.hidden.unwrap_or(false),
                system: sq.system.unwrap_or(System::Client),
            });
            Box::new(crate::ivm::flipped_join::ArcFlippedJoinAsInput::new(fj_arc))
        }
        Condition::Simple { .. } => unreachable!("asserted above"),
    }
}

/// Adapter that lets a shared [`UnionFanOut`] serve as a
/// [`Box<dyn Input>`] for downstream branches.
struct UnionFanOutAdapter(Arc<Mutex<UnionFanOut>>);
impl InputBase for UnionFanOutAdapter {
    fn get_schema(&self) -> &crate::ivm::schema::SourceSchema {
        let guard = self.0.lock().unwrap();
        let schema_clone = guard.get_schema().clone();
        Box::leak(Box::new(schema_clone))
    }
    fn destroy(&mut self) {
        self.0.lock().unwrap().destroy();
    }
}
impl Input for UnionFanOutAdapter {
    fn set_output(&mut self, output: Box<dyn crate::ivm::operator::Output>) {
        self.0.lock().unwrap().set_output(output);
    }
    fn fetch<'a>(
        &'a self,
        req: crate::ivm::operator::FetchRequest,
    ) -> crate::ivm::stream::Stream<'a, crate::ivm::data::NodeOrYield> {
        // Cannot hold the lock across the returned stream's lifetime;
        // drain eagerly.
        let guard = self.0.lock().unwrap();
        let collected: Vec<_> = guard.fetch(req).collect();
        Box::new(collected.into_iter())
    }
}

// ─── apply_filter (FilterInput-side) ─────────────────────────────────

fn apply_filter_owned(
    input: Box<dyn FilterInput>,
    condition: &Condition,
    delegate: &dyn BuilderDelegate,
    name: &str,
) -> Box<dyn FilterInput> {
    match condition {
        Condition::And { .. } => apply_and_owned(input, condition, delegate, name),
        Condition::Or { .. } => apply_or_owned(input, condition, delegate, name),
        Condition::CorrelatedSubquery { .. } => {
            apply_correlated_subquery_condition(input, condition, delegate, name)
        }
        Condition::Simple { .. } => apply_simple_condition(input, delegate, condition),
    }
}

fn apply_and_owned(
    mut input: Box<dyn FilterInput>,
    condition: &Condition,
    delegate: &dyn BuilderDelegate,
    name: &str,
) -> Box<dyn FilterInput> {
    if let Condition::And { conditions } = condition {
        for sub in conditions.iter() {
            input = apply_filter_owned(input, sub, delegate, name);
        }
        return input;
    }
    unreachable!("apply_and_owned called with non-And");
}

/// TS `applyOr(input, condition, delegate, name)`.
pub fn apply_or_owned(
    input: Box<dyn FilterInput>,
    condition: &Condition,
    delegate: &dyn BuilderDelegate,
    name: &str,
) -> Box<dyn FilterInput> {
    let Condition::Or { conditions } = condition else {
        unreachable!("apply_or_owned called with non-Or");
    };
    let (subquery_conditions, other_conditions) = group_subquery_conditions(conditions);

    // Branch: no subquery conditions → single Filter with predicate.
    if subquery_conditions.is_empty() {
        let combined = Condition::Or {
            conditions: other_conditions.into_iter().cloned().collect(),
        };
        let pred = Arc::from(create_predicate(&combined));
        let filter = Filter::new(input, pred);
        return Box::new(ArcFilterAsInput::new(filter));
    }

    // Branch: subqueries present → fan-out + fan-in.
    let fan_out_arc = crate::ivm::fan_out::FanOut::new_wired(input);
    let mut branches: Vec<Box<dyn FilterInput>> = Vec::new();
    for sub in subquery_conditions.iter() {
        let branch_input: Box<dyn FilterInput> = Box::new(FanOutAdapter(Arc::clone(&fan_out_arc)));
        branches.push(apply_filter_owned(branch_input, sub, delegate, name));
    }
    if !other_conditions.is_empty() {
        let combined = Condition::Or {
            conditions: other_conditions.into_iter().cloned().collect(),
        };
        let pred = Arc::from(create_predicate(&combined));
        let branch_input: Box<dyn FilterInput> = Box::new(FanOutAdapter(Arc::clone(&fan_out_arc)));
        let filter = Filter::new(branch_input, pred);
        branches.push(Box::new(ArcFilterAsInput::new(filter)));
    }
    let schema = fan_out_arc.lock().unwrap().get_schema().clone();
    let fan_in_arc = crate::ivm::fan_in::FanIn::new_wired(schema, branches);
    Box::new(crate::ivm::fan_in::ArcFanInAsInput::new(fan_in_arc))
}

struct FanOutAdapter(Arc<Mutex<FanOut>>);
impl InputBase for FanOutAdapter {
    fn get_schema(&self) -> &crate::ivm::schema::SourceSchema {
        let guard = self.0.lock().unwrap();
        let schema_clone = guard.get_schema().clone();
        Box::leak(Box::new(schema_clone))
    }
    fn destroy(&mut self) {
        self.0.lock().unwrap().destroy();
    }
}
impl FilterInput for FanOutAdapter {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        self.0.lock().unwrap().set_filter_output(output);
    }
}

// ─── group_subquery_conditions / isNotAndDoesNotContainSubquery ──────

/// TS `groupSubqueryConditions(condition)` — returns
/// `(subquery_conditions, other_conditions)` partitioning by whether
/// each condition contains a correlated subquery.
pub fn group_subquery_conditions<'a>(
    conditions: &'a [Condition],
) -> (Vec<&'a Condition>, Vec<&'a Condition>) {
    let mut subq: Vec<&Condition> = Vec::new();
    let mut other: Vec<&Condition> = Vec::new();
    for c in conditions.iter() {
        if is_not_and_does_not_contain_subquery(c) {
            other.push(c);
        } else {
            subq.push(c);
        }
    }
    (subq, other)
}

/// TS `isNotAndDoesNotContainSubquery`.
pub fn is_not_and_does_not_contain_subquery(condition: &Condition) -> bool {
    match condition {
        Condition::CorrelatedSubquery { .. } => false,
        Condition::Simple { .. } => true,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().all(is_not_and_does_not_contain_subquery)
        }
    }
}

// ─── apply_simple_condition ──────────────────────────────────────────

fn apply_simple_condition(
    input: Box<dyn FilterInput>,
    delegate: &dyn BuilderDelegate,
    condition: &Condition,
) -> Box<dyn FilterInput> {
    let pred = Arc::from(create_predicate(condition));
    let filter = Filter::new(input, pred);
    // TS also calls `delegate.decorateFilterInput(filter, …)` — we call
    // decorate_filter_input on a boxed Filter.
    let name = format!(
        "{}:{}:{}",
        value_pos_name_from_cond_left(condition),
        op_name(condition),
        value_pos_name_from_cond_right(condition)
    );
    delegate.decorate_filter_input(Box::new(ArcFilterAsInput::new(filter)), &name)
}

fn value_pos_name_from_cond_left(cond: &Condition) -> String {
    if let Condition::Simple { left, .. } = cond {
        return match left {
            zero_cache_types::ast::ValuePosition::Column { name } => name.clone(),
            zero_cache_types::ast::ValuePosition::Literal { value } => {
                format!("{value:?}")
            }
            zero_cache_types::ast::ValuePosition::Static { field, .. } => match field {
                zero_cache_types::ast::ParameterField::Single(s) => s.clone(),
                zero_cache_types::ast::ParameterField::Path(p) => p.join("."),
            },
        };
    }
    String::new()
}

fn value_pos_name_from_cond_right(cond: &Condition) -> String {
    if let Condition::Simple { right, .. } = cond {
        return match right {
            zero_cache_types::ast::NonColumnValue::Literal { value } => format!("{value:?}"),
            zero_cache_types::ast::NonColumnValue::Static { field, .. } => match field {
                zero_cache_types::ast::ParameterField::Single(s) => s.clone(),
                zero_cache_types::ast::ParameterField::Path(p) => p.join("."),
            },
        };
    }
    String::new()
}

fn op_name(cond: &Condition) -> String {
    if let Condition::Simple { op, .. } = cond {
        return format!("{op:?}");
    }
    String::new()
}

// ─── apply_correlated_subquery ───────────────────────────────────────

fn apply_correlated_subquery(
    sq: CorrelatedSubquery,
    delegate: &dyn BuilderDelegate,
    query_id: &str,
    end: Box<dyn Input>,
    name: &str,
    from_condition: bool,
) -> Box<dyn Input> {
    // Branch: limit=0 from condition → skip the join entirely.
    if sq.subquery.limit == Some(0) && from_condition {
        return end;
    }
    let alias = sq
        .subquery
        .alias
        .clone()
        .expect("Subquery must have an alias");
    let child = build_pipeline_internal(
        (*sq.subquery).clone(),
        delegate,
        query_id,
        &format!("{name}.{alias}"),
        Some(sq.correlation.child_field.clone()),
    );
    let join_name = format!("{name}:join({alias})");
    let join = Join::new_wired(JoinArgs {
        parent: end,
        child,
        parent_key: sq.correlation.parent_field.clone(),
        child_key: sq.correlation.child_field.clone(),
        relationship_name: alias,
        hidden: sq.hidden.unwrap_or(false),
        system: sq.system.unwrap_or(System::Client),
    });
    delegate.decorate_input(
        Box::new(crate::ivm::join::ArcJoinAsInput::new(join)),
        &join_name,
    )
}

// ─── apply_correlated_subquery_condition (EXISTS / NOT EXISTS) ───────

fn apply_correlated_subquery_condition(
    input: Box<dyn FilterInput>,
    condition: &Condition,
    delegate: &dyn BuilderDelegate,
    name: &str,
) -> Box<dyn FilterInput> {
    let Condition::CorrelatedSubquery { related, op, .. } = condition else {
        unreachable!("apply_correlated_subquery_condition called with non-CSQ");
    };
    assert!(
        matches!(
            op,
            CorrelatedSubqueryOp::EXISTS | CorrelatedSubqueryOp::NotExists
        ),
        "Expected EXISTS or NOT EXISTS operator"
    );
    // Branch: subquery.limit == 0 → constant predicate.
    if related.subquery.limit == Some(0) {
        let pred: Arc<dyn Fn(&zero_cache_types::value::Row) -> bool + Send + Sync> = match op {
            CorrelatedSubqueryOp::EXISTS => Arc::new(|_r| false),
            CorrelatedSubqueryOp::NotExists => Arc::new(|_r| true),
        };
        let filter = Filter::new(input, pred);
        return Box::new(ArcFilterAsInput::new(filter));
    }
    let alias = related.subquery.alias.clone().expect("alias required");
    let exists_name = format!("{name}:exists({alias})");
    let exists_type = match op {
        CorrelatedSubqueryOp::EXISTS => ExistsType::Exists,
        CorrelatedSubqueryOp::NotExists => ExistsType::NotExists,
    };
    let exists_arc = crate::ivm::exists::Exists::new_wired(
        input,
        alias,
        related.correlation.parent_field.clone(),
        exists_type,
    );
    delegate.decorate_filter_input(
        Box::new(crate::ivm::exists::ArcExistsAsInput::new(exists_arc)),
        &exists_name,
    )
}

// ─── Helpers ports ───────────────────────────────────────────────────

fn gather_correlated_subquery_query_conditions(cond: Option<&Condition>) -> Vec<Condition> {
    let mut out: Vec<Condition> = Vec::new();
    fn gather(c: &Condition, out: &mut Vec<Condition>) {
        match c {
            Condition::CorrelatedSubquery { .. } => out.push(c.clone()),
            Condition::And { conditions } | Condition::Or { conditions } => {
                for inner in conditions.iter() {
                    gather(inner, out);
                }
            }
            Condition::Simple { .. } => {}
        }
    }
    if let Some(c) = cond {
        gather(c, &mut out);
    }
    out
}

/// TS `assertOrderingIncludesPK`. Re-exported here for test convenience;
/// the authoritative copy lives in `query/complete_ordering.rs`.
pub use crate::query::complete_ordering::assert_ordering_includes_pk;

/// TS `assertNoNotExists(condition)`.
pub fn assert_no_not_exists(condition: &Condition) {
    match condition {
        Condition::Simple { .. } => {}
        Condition::CorrelatedSubquery { op, .. } => {
            if matches!(op, CorrelatedSubqueryOp::NotExists) {
                panic!(
                    "not(exists()) is not supported on the client - see https://bugs.rocicorp.dev/issue/3438"
                );
            }
        }
        Condition::And { conditions } | Condition::Or { conditions } => {
            for c in conditions.iter() {
                assert_no_not_exists(c);
            }
        }
    }
}

fn uniquify_correlated_subquery_condition_aliases(ast: AST) -> AST {
    let Some(w) = ast.where_clause.as_deref() else {
        return ast;
    };
    if !matches!(w, Condition::And { .. } | Condition::Or { .. }) {
        return ast;
    }
    let mut counter: u32 = 0;
    fn uniquify(cond: &Condition, counter: &mut u32) -> Condition {
        match cond {
            Condition::Simple { .. } => cond.clone(),
            Condition::CorrelatedSubquery {
                related,
                op,
                flip,
                scalar,
            } => {
                let alias = related.subquery.alias.clone().unwrap_or_default();
                let new_alias = format!("{alias}_{counter}");
                *counter += 1;
                let mut new_sub = (*related.subquery).clone();
                new_sub.alias = Some(new_alias);
                let new_related = CorrelatedSubquery {
                    correlation: related.correlation.clone(),
                    subquery: Box::new(new_sub),
                    system: related.system,
                    hidden: related.hidden,
                };
                Condition::CorrelatedSubquery {
                    related: Box::new(new_related),
                    op: *op,
                    flip: *flip,
                    scalar: *scalar,
                }
            }
            Condition::And { conditions } => {
                let mut new_c: Vec<Condition> = Vec::with_capacity(conditions.len());
                for c in conditions.iter() {
                    new_c.push(uniquify(c, counter));
                }
                Condition::And { conditions: new_c }
            }
            Condition::Or { conditions } => {
                let mut new_c: Vec<Condition> = Vec::with_capacity(conditions.len());
                for c in conditions.iter() {
                    new_c.push(uniquify(c, counter));
                }
                Condition::Or { conditions: new_c }
            }
        }
    }
    let new_where = uniquify(w, &mut counter);
    AST {
        where_clause: Some(Box::new(new_where)),
        ..ast
    }
}

/// TS `conditionIncludesFlippedSubqueryAtAnyLevel`.
pub fn condition_includes_flipped_subquery_at_any_level(cond: &Condition) -> bool {
    match cond {
        Condition::CorrelatedSubquery { flip, .. } => flip.unwrap_or(false),
        Condition::And { conditions } | Condition::Or { conditions } => conditions
            .iter()
            .any(condition_includes_flipped_subquery_at_any_level),
        Condition::Simple { .. } => false,
    }
}

/// TS `partitionBranches(conditions, predicate)`.
pub fn partition_branches<'a, F>(
    conditions: &'a [Condition],
    predicate: &F,
) -> (Vec<&'a Condition>, Vec<&'a Condition>)
where
    F: Fn(&Condition) -> bool,
{
    let mut matched: Vec<&Condition> = Vec::new();
    let mut not_matched: Vec<&Condition> = Vec::new();
    for c in conditions.iter() {
        if predicate(c) {
            matched.push(c);
        } else {
            not_matched.push(c);
        }
    }
    (matched, not_matched)
}

/// Force the `simplify_condition` helper into scope so consumers of
/// this module don't have to reach into `filter` for it.
pub fn simplify(c: Condition) -> Condition {
    simplify_condition(c)
}

#[cfg(test)]
mod tests {
    //! Branch coverage for helper functions (pure, AST-only). Full
    //! `build_pipeline` integration tests land with `pipeline_driver`
    //! (Layer 11) — they require a real Source and Storage and a
    //! running pipeline. Here we test the tree-analysis helpers:
    //!
    //! - `assert_no_not_exists` — Simple/And/Or pass; NOT EXISTS panics.
    //! - `condition_includes_flipped_subquery_at_any_level` — each arm.
    //! - `partition_branches` — true/false/both.
    //! - `group_subquery_conditions` / `is_not_and_does_not_contain_subquery`.
    //! - `gather_correlated_subquery_query_conditions` — traversal.
    //! - `uniquify_correlated_subquery_condition_aliases` — increments
    //!   counter, preserves non-where AST.
    //! - `assert_ordering_includes_pk` — re-exported; see source tests.

    use super::*;
    use zero_cache_types::ast::{
        AST, CorrelatedSubquery, CorrelatedSubqueryOp, Correlation, LiteralValue, NonColumnValue,
        SimpleOperator, ValuePosition,
    };

    fn ast(table: &str) -> AST {
        AST {
            schema: None,
            table: table.into(),
            alias: None,
            where_clause: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        }
    }

    fn simple_cond() -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        }
    }

    fn csq(alias: &str, flip: Option<bool>, op: CorrelatedSubqueryOp) -> Condition {
        let mut sub = ast("child");
        sub.alias = Some(alias.into());
        Condition::CorrelatedSubquery {
            related: Box::new(CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["uid".into()],
                },
                subquery: Box::new(sub),
                system: None,
                hidden: None,
            }),
            op,
            flip,
            scalar: None,
        }
    }

    // Branch: assert_no_not_exists — simple ok.
    #[test]
    fn assert_no_not_exists_simple_ok() {
        assert_no_not_exists(&simple_cond());
    }
    // Branch: EXISTS ok.
    #[test]
    fn assert_no_not_exists_exists_ok() {
        assert_no_not_exists(&csq("a", None, CorrelatedSubqueryOp::EXISTS));
    }
    // Branch: NOT EXISTS panics.
    #[test]
    #[should_panic(expected = "not(exists())")]
    fn assert_no_not_exists_not_exists_panics() {
        assert_no_not_exists(&csq("a", None, CorrelatedSubqueryOp::NotExists));
    }
    // Branch: nested NOT EXISTS inside OR panics.
    #[test]
    #[should_panic(expected = "not(exists())")]
    fn assert_no_not_exists_nested_panics() {
        let c = Condition::Or {
            conditions: vec![
                simple_cond(),
                csq("a", None, CorrelatedSubqueryOp::NotExists),
            ],
        };
        assert_no_not_exists(&c);
    }

    // Branch: flipped detection — simple → false.
    #[test]
    fn flipped_simple_false() {
        assert!(!condition_includes_flipped_subquery_at_any_level(
            &simple_cond()
        ));
    }
    // Branch: CSQ with flip=true → true.
    #[test]
    fn flipped_csq_true() {
        assert!(condition_includes_flipped_subquery_at_any_level(&csq(
            "a",
            Some(true),
            CorrelatedSubqueryOp::EXISTS,
        )));
    }
    // Branch: CSQ with flip=None → false.
    #[test]
    fn flipped_csq_none_false() {
        assert!(!condition_includes_flipped_subquery_at_any_level(&csq(
            "a",
            None,
            CorrelatedSubqueryOp::EXISTS,
        )));
    }
    // Branch: AND with any flipped → true.
    #[test]
    fn flipped_nested_true() {
        let c = Condition::And {
            conditions: vec![
                simple_cond(),
                csq("a", Some(true), CorrelatedSubqueryOp::EXISTS),
            ],
        };
        assert!(condition_includes_flipped_subquery_at_any_level(&c));
    }

    // Branch: partition_branches.
    #[test]
    fn partition_branches_splits() {
        let conditions = vec![
            simple_cond(),
            csq("a", Some(true), CorrelatedSubqueryOp::EXISTS),
            simple_cond(),
        ];
        let (matched, not_matched) = partition_branches(
            &conditions,
            &condition_includes_flipped_subquery_at_any_level,
        );
        assert_eq!(matched.len(), 1);
        assert_eq!(not_matched.len(), 2);
    }

    // Branch: group_subquery_conditions.
    #[test]
    fn group_subq_conditions() {
        let conditions = vec![simple_cond(), csq("a", None, CorrelatedSubqueryOp::EXISTS)];
        let (subq, other) = group_subquery_conditions(&conditions);
        assert_eq!(subq.len(), 1);
        assert_eq!(other.len(), 1);
    }

    // Branch: is_not_and_does_not_contain_subquery — nested true.
    #[test]
    fn not_contains_subquery_deep_true() {
        let c = Condition::And {
            conditions: vec![
                simple_cond(),
                Condition::Or {
                    conditions: vec![simple_cond()],
                },
            ],
        };
        assert!(is_not_and_does_not_contain_subquery(&c));
    }
    #[test]
    fn not_contains_subquery_deep_false() {
        let c = Condition::And {
            conditions: vec![simple_cond(), csq("a", None, CorrelatedSubqueryOp::EXISTS)],
        };
        assert!(!is_not_and_does_not_contain_subquery(&c));
    }

    // Branch: gather CSQs — nested.
    #[test]
    fn gather_csqs_traverses() {
        let c = Condition::And {
            conditions: vec![
                simple_cond(),
                Condition::Or {
                    conditions: vec![
                        csq("a", None, CorrelatedSubqueryOp::EXISTS),
                        csq("b", None, CorrelatedSubqueryOp::EXISTS),
                    ],
                },
            ],
        };
        let got = gather_correlated_subquery_query_conditions(Some(&c));
        assert_eq!(got.len(), 2);
    }
    #[test]
    fn gather_csqs_empty_when_none() {
        assert!(gather_correlated_subquery_query_conditions(None).is_empty());
    }

    // Branch: uniquify — no where → AST unchanged.
    #[test]
    fn uniquify_no_where_unchanged() {
        let a = ast("t");
        let b = uniquify_correlated_subquery_condition_aliases(a.clone());
        assert_eq!(a.table, b.table);
        assert!(b.where_clause.is_none());
    }
    // Branch: uniquify — simple where → unchanged.
    #[test]
    fn uniquify_simple_where_unchanged() {
        let mut a = ast("t");
        a.where_clause = Some(Box::new(simple_cond()));
        let b = uniquify_correlated_subquery_condition_aliases(a);
        // Simple condition → function returns early (no And/Or wrap).
        match b.where_clause.as_deref() {
            Some(Condition::Simple { .. }) => {}
            _ => panic!("expected simple"),
        }
    }
    // Branch: uniquify — AND of CSQs → each gets a unique suffix.
    #[test]
    fn uniquify_and_of_csqs_renames() {
        let mut a = ast("parent");
        a.where_clause = Some(Box::new(Condition::And {
            conditions: vec![
                csq("a", None, CorrelatedSubqueryOp::EXISTS),
                csq("a", None, CorrelatedSubqueryOp::EXISTS),
            ],
        }));
        let b = uniquify_correlated_subquery_condition_aliases(a);
        let aliases: Vec<String> = match b.where_clause.as_deref() {
            Some(Condition::And { conditions }) => conditions
                .iter()
                .filter_map(|c| match c {
                    Condition::CorrelatedSubquery { related, .. } => related.subquery.alias.clone(),
                    _ => None,
                })
                .collect(),
            _ => panic!("expected AND"),
        };
        assert_eq!(aliases, vec!["a_0".to_string(), "a_1".to_string()]);
    }

    // Branch: simplify helper re-exports simplify_condition.
    #[test]
    fn simplify_re_export() {
        let c = Condition::And {
            conditions: vec![simple_cond()],
        };
        // Single-child AND unwraps.
        assert_eq!(simplify(c), simple_cond());
    }
}
