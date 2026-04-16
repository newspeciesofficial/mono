//! Port of `packages/zql/src/ivm/fan-in.ts`.
//!
//! Public exports ported:
//!
//! - [`FanIn`] — the `FilterOperator` that merges N upstream filter
//!   streams into one, de-duplicating via the schema comparator. Paired
//!   with a [`super::fan_out::FanOut`] upstream that forks the source.
//!
//! ## Ownership divergence from TS
//!
//! The TS ctor calls `input.setFilterOutput(this)` for every upstream so
//! pushes routed through the branch Filters end up on the fan-in. Porting
//! that verbatim creates an ownership cycle. As elsewhere in the port,
//! the pipeline driver (Layer 9+) is responsible for wiring the
//! back-edges. The in-module tests invoke [`FanIn::push`] directly where
//! the TS test would rely on Filter→FanIn routing.
//!
//! The TS ctor also asserts that every input shares the same schema (by
//! reference equality). In Rust we enforce the weaker check that
//! [`SourceSchema::table_name`], `primary_key`, and `sort` match the
//! fan-out-side schema — which is what downstream consumers actually
//! read. The schema-reference assertion in TS is a sanity check on graph
//! wiring, not a correctness guarantee.

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::data::Node;
use crate::ivm::filter_operators::{FilterInput, FilterOperator, FilterOutput, ThrowFilterOutput};
use crate::ivm::operator::{InputBase, Output};
use crate::ivm::push_accumulated::push_accumulated_changes;
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;
use std::sync::{Arc, Mutex};

/// TS `FanIn` — merges N upstream filter streams into one.
pub struct FanIn {
    inputs: Mutex<Vec<Box<dyn FilterInput>>>,
    schema: SourceSchema,
    output: Mutex<Box<dyn FilterOutput>>,
    accumulated_pushes: Mutex<Vec<Change>>,
}

impl FanIn {
    /// TS `new FanIn(fanOut, inputs)`.
    pub fn new(schema: SourceSchema, inputs: Vec<Box<dyn FilterInput>>) -> Self {
        for input in &inputs {
            let input_schema = input.get_schema();
            assert!(
                schema.table_name == input_schema.table_name,
                "Schema mismatch in fan-in: table_name {} != {}",
                schema.table_name,
                input_schema.table_name
            );
            assert!(
                schema.primary_key == input_schema.primary_key,
                "Schema mismatch in fan-in: primary_key mismatch"
            );
            assert!(
                schema.sort == input_schema.sort,
                "Schema mismatch in fan-in: sort mismatch"
            );
        }

        Self {
            inputs: Mutex::new(inputs),
            schema,
            output: Mutex::new(Box::new(ThrowFilterOutput)),
            accumulated_pushes: Mutex::new(Vec::new()),
        }
    }

    /// Wired variant: matches TS `new FanIn(...)` followed by
    /// `input.setFilterOutput(this)` for every upstream branch.
    /// Returns `Arc<Self>` (no outer Mutex) so back-edges push through
    /// interior `*_arc` helpers without reentrant-lock deadlock.
    pub fn new_wired(schema: SourceSchema, inputs: Vec<Box<dyn FilterInput>>) -> Arc<Self> {
        let arc = Arc::new(Self::new(schema, inputs));
        let n_inputs = arc
            .inputs
            .lock()
            .expect("fan_in inputs mutex poisoned")
            .len();
        for i in 0..n_inputs {
            let back: Box<dyn FilterOutput> = Box::new(FanInPushBackEdge(Arc::clone(&arc)));
            arc.inputs
                .lock()
                .expect("fan_in inputs mutex poisoned")[i]
                .set_filter_output(back);
        }
        arc
    }

    /// &self variant of `set_filter_output`.
    pub fn set_filter_output_arc(&self, output: Box<dyn FilterOutput>) {
        *self.output.lock().expect("fan_in output mutex poisoned") = output;
    }

    /// &self variant of `destroy`.
    pub fn destroy_arc(&self) {
        let mut inputs = self.inputs.lock().expect("fan_in inputs mutex poisoned");
        for input in inputs.iter_mut() {
            input.destroy();
        }
    }

    /// &self variant of `push` — enqueues onto accumulated_pushes.
    pub fn push_arc(&self, change: Change, _pusher: &dyn InputBase) -> Vec<Yield> {
        self.accumulated_pushes
            .lock()
            .expect("fan_in accumulated_pushes mutex poisoned")
            .push(change);
        Vec::new()
    }

    /// &self begin_filter.
    pub fn begin_filter_arc(&self) {
        let mut o = self.output.lock().expect("fan_in output mutex poisoned");
        o.begin_filter();
    }

    /// &self end_filter.
    pub fn end_filter_arc(&self) {
        let mut o = self.output.lock().expect("fan_in output mutex poisoned");
        o.end_filter();
    }

    /// &self filter.
    pub fn filter_arc(&self, node: &Node) -> (Vec<Yield>, bool) {
        let mut o = self.output.lock().expect("fan_in output mutex poisoned");
        let (stream, keep) = o.filter(node);
        let yields: Vec<Yield> = stream.collect();
        (yields, keep)
    }

    /// &self variant of `fan_out_done_pushing_to_all_branches`.
    pub fn fan_out_done_pushing_to_all_branches_arc(
        &self,
        fan_out_change_type: ChangeType,
    ) -> Vec<Yield> {
        let inputs_guard = self.inputs.lock().expect("fan_in inputs mutex poisoned");
        if inputs_guard.is_empty() {
            let accumulated = self
                .accumulated_pushes
                .lock()
                .expect("fan_in accumulated_pushes mutex poisoned");
            assert!(
                accumulated.is_empty(),
                "If there are no inputs then fan-in should not receive any pushes."
            );
            return Vec::new();
        }

        let mut accumulated = self
            .accumulated_pushes
            .lock()
            .expect("fan_in accumulated_pushes mutex poisoned");
        let mut drained: Vec<Change> = accumulated.drain(..).collect();
        drop(accumulated);

        let mut out_guard = self.output.lock().expect("fan_in output mutex poisoned");
        let out: &mut dyn Output = &mut **out_guard;
        let pusher: &dyn InputBase = &**&inputs_guard[0];
        let identity_merge = |_l: Change, r: Change| r;
        let identity_empty = |c: Change| c;
        let collected: Vec<Yield> = push_accumulated_changes(
            &mut drained,
            out,
            pusher,
            fan_out_change_type,
            &identity_merge,
            &identity_empty,
        )
        .collect();
        drop(out_guard);
        drop(inputs_guard);
        collected
    }

    /// Deprecated &mut variant kept for tests.
    pub fn fan_out_done_pushing_to_all_branches<'a>(
        &'a mut self,
        fan_out_change_type: ChangeType,
    ) -> Stream<'a, Yield> {
        let items = self.fan_out_done_pushing_to_all_branches_arc(fan_out_change_type);
        Box::new(items.into_iter())
    }
}

impl InputBase for FanIn {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn destroy(&mut self) {
        self.destroy_arc();
    }
}

impl FilterInput for FanIn {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        self.set_filter_output_arc(output);
    }
}

impl FilterOutput for FanIn {
    fn begin_filter(&mut self) {
        self.begin_filter_arc();
    }

    fn end_filter(&mut self) {
        self.end_filter_arc();
    }

    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        let (yields, keep) = self.filter_arc(node);
        (Box::new(yields.into_iter()), keep)
    }
}

impl Output for FanIn {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let items = self.push_arc(change, pusher);
        Box::new(items.into_iter())
    }
}

impl FilterOperator for FanIn {}

/// Back-edge adapter installed on every [`FanIn::inputs`] entry.
/// Created by [`FanIn::new_wired`]. Holds `Arc<FanIn>` directly (no
/// outer Mutex) and delegates to `*_arc` helpers.
pub struct FanInPushBackEdge(pub Arc<FanIn>);

impl Output for FanInPushBackEdge {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        eprintln!("[TRACE ivm] FanIn::push enter");
        let items = self.0.push_arc(change, pusher);
        eprintln!("[TRACE ivm] FanIn::push exit yields={}", items.len());
        Box::new(items.into_iter())
    }
}

impl FilterOutput for FanInPushBackEdge {
    fn begin_filter(&mut self) {
        self.0.begin_filter_arc();
    }
    fn end_filter(&mut self) {
        self.0.end_filter_arc();
    }
    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        let (yields, keep) = self.0.filter_arc(node);
        (Box::new(yields.into_iter()), keep)
    }
}

/// Adapter to plug a wired [`Arc<FanIn>`] back into the chain
/// as `Box<dyn FilterInput>`. Schema cached at construction.
pub struct ArcFanInAsInput {
    inner: Arc<FanIn>,
    schema: SourceSchema,
}

impl ArcFanInAsInput {
    pub fn new(inner: Arc<FanIn>) -> Self {
        let schema = inner.get_schema().clone();
        Self { inner, schema }
    }
}

impl InputBase for ArcFanInAsInput {
    fn get_schema(&self) -> &SourceSchema { &self.schema }
    fn destroy(&mut self) { self.inner.destroy_arc(); }
}

impl FilterInput for ArcFanInAsInput {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        self.inner.set_filter_output_arc(output);
    }
}

impl Output for ArcFanInAsInput {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let items = self.inner.push_arc(change, pusher);
        Box::new(items.into_iter())
    }
}

impl FilterOutput for ArcFanInAsInput {
    fn begin_filter(&mut self) {
        self.inner.begin_filter_arc();
    }
    fn end_filter(&mut self) {
        self.inner.end_filter_arc();
    }
    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        let (yields, keep) = self.inner.filter_arc(node);
        (Box::new(yields.into_iter()), keep)
    }
}

impl FilterOperator for ArcFanInAsInput {}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!   - `new` with empty inputs — no schema checks run, constructor OK.
    //!   - `new` schema mismatch (table_name) panics.
    //!   - `new` schema mismatch (primary_key) panics.
    //!   - `new` schema mismatch (sort) panics.
    //!   - `get_schema` returns the fan-out schema.
    //!   - `destroy` propagates to every input.
    //!   - `set_filter_output` replaces the default throw sink.
    //!   - `begin_filter` / `end_filter` delegate downstream.
    //!   - `filter` delegates downstream and propagates yields.
    //!   - `push` accumulates and returns empty.
    //!   - `fan_out_done_pushing_to_all_branches` with 0 inputs + 0
    //!     accumulated returns empty (assertion OK).
    //!   - `fan_out_done_pushing_to_all_branches` with 0 inputs + any
    //!     accumulated panics per TS assertion.
    //!   - `fan_out_done_pushing_to_all_branches` with N inputs drains
    //!     accumulated and forwards a single representative downstream.
    //!   - `fan_out_done_pushing_to_all_branches` when accumulated is
    //!     empty returns early (push_accumulated_changes short-circuit).

    use super::*;
    use crate::ivm::change::{AddChange, ChangeType};
    use crate::ivm::data::{Node, make_comparator};
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrdering};
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Row;

    fn make_schema(name: &str, pk_col: &str, sort_col: &str) -> SourceSchema {
        let sort: Ordering = vec![(sort_col.into(), Direction::Asc)];
        SourceSchema {
            table_name: name.into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec![pk_col.into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    fn row_with(id: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r
    }

    fn node_of(row: Row) -> Node {
        Node {
            row,
            relationships: IndexMap::new(),
        }
    }

    struct StubFilterInput {
        schema: SourceSchema,
        destroyed: Arc<AtomicBool>,
    }
    impl InputBase for StubFilterInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {
            self.destroyed.store(true, AtOrdering::SeqCst);
        }
    }
    impl FilterInput for StubFilterInput {
        fn set_filter_output(&mut self, _o: Box<dyn FilterOutput>) {}
    }

    fn stub_with(schema: SourceSchema) -> (Box<dyn FilterInput>, Arc<AtomicBool>) {
        let d = Arc::new(AtomicBool::new(false));
        (
            Box::new(StubFilterInput {
                schema,
                destroyed: Arc::clone(&d),
            }),
            d,
        )
    }

    /// Records calls.
    struct Recorder {
        pushes: Arc<Mutex<Vec<ChangeType>>>,
        filter_calls: Arc<AtomicUsize>,
        filter_result: bool,
        yields_per_filter: usize,
        begins: Arc<AtomicUsize>,
        ends: Arc<AtomicUsize>,
    }
    impl Output for Recorder {
        fn push<'a>(&'a mut self, c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes.lock().unwrap().push(c.change_type());
            Box::new(std::iter::empty())
        }
    }
    impl FilterOutput for Recorder {
        fn begin_filter(&mut self) {
            self.begins.fetch_add(1, AtOrdering::SeqCst);
        }
        fn end_filter(&mut self) {
            self.ends.fetch_add(1, AtOrdering::SeqCst);
        }
        fn filter(&mut self, _n: &Node) -> (Stream<'_, Yield>, bool) {
            self.filter_calls.fetch_add(1, AtOrdering::SeqCst);
            let n = self.yields_per_filter;
            (Box::new((0..n).map(|_| Yield)), self.filter_result)
        }
    }

    // Branch: new with empty inputs — constructor OK.
    #[test]
    fn fan_in_new_empty_inputs_ok() {
        let schema = make_schema("t", "id", "id");
        let fi = FanIn::new(schema, vec![]);
        assert_eq!(fi.get_schema().table_name, "t");
    }

    // Branch: new schema mismatch (table_name) → panic.
    #[test]
    #[should_panic(expected = "Schema mismatch in fan-in: table_name")]
    fn fan_in_new_table_name_mismatch_panics() {
        let fo_schema = make_schema("t", "id", "id");
        let in_schema = make_schema("u", "id", "id");
        let (input, _) = stub_with(in_schema);
        let _ = FanIn::new(fo_schema, vec![input]);
    }

    // Branch: new schema mismatch (primary_key).
    #[test]
    #[should_panic(expected = "primary_key mismatch")]
    fn fan_in_new_primary_key_mismatch_panics() {
        let fo_schema = make_schema("t", "id", "id");
        let in_schema = make_schema("t", "other", "id");
        let (input, _) = stub_with(in_schema);
        let _ = FanIn::new(fo_schema, vec![input]);
    }

    // Branch: new schema mismatch (sort).
    #[test]
    #[should_panic(expected = "sort mismatch")]
    fn fan_in_new_sort_mismatch_panics() {
        let fo_schema = make_schema("t", "id", "id");
        let in_schema = make_schema("t", "id", "other");
        let (input, _) = stub_with(in_schema);
        let _ = FanIn::new(fo_schema, vec![input]);
    }

    // Branch: get_schema returns fan-out schema.
    #[test]
    fn fan_in_get_schema_returns_fan_out_schema() {
        let fi = FanIn::new(make_schema("t", "id", "id"), vec![]);
        assert_eq!(fi.get_schema().table_name, "t");
    }

    // Branch: destroy propagates to every input.
    #[test]
    fn fan_in_destroy_propagates_to_every_input() {
        let (i1, d1) = stub_with(make_schema("t", "id", "id"));
        let (i2, d2) = stub_with(make_schema("t", "id", "id"));
        let mut fi = FanIn::new(make_schema("t", "id", "id"), vec![i1, i2]);
        fi.destroy();
        assert!(d1.load(AtOrdering::SeqCst));
        assert!(d2.load(AtOrdering::SeqCst));
    }

    // Branch: set_filter_output + begin_filter/end_filter delegate.
    #[test]
    fn fan_in_set_filter_output_and_begin_end_delegate() {
        let mut fi = FanIn::new(make_schema("t", "id", "id"), vec![]);
        let begins = Arc::new(AtomicUsize::new(0));
        let ends = Arc::new(AtomicUsize::new(0));
        fi.set_filter_output(Box::new(Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
            filter_calls: Arc::new(AtomicUsize::new(0)),
            filter_result: true,
            yields_per_filter: 0,
            begins: Arc::clone(&begins),
            ends: Arc::clone(&ends),
        }));
        fi.begin_filter();
        fi.end_filter();
        assert_eq!(begins.load(AtOrdering::SeqCst), 1);
        assert_eq!(ends.load(AtOrdering::SeqCst), 1);
    }

    // Branch: filter delegates downstream and forwards yields.
    #[test]
    fn fan_in_filter_delegates_and_propagates_yields() {
        let mut fi = FanIn::new(make_schema("t", "id", "id"), vec![]);
        fi.set_filter_output(Box::new(Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
            filter_calls: Arc::new(AtomicUsize::new(0)),
            filter_result: true,
            yields_per_filter: 3,
            begins: Arc::new(AtomicUsize::new(0)),
            ends: Arc::new(AtomicUsize::new(0)),
        }));
        let (stream, keep) = fi.filter(&node_of(row_with(1)));
        assert!(keep);
        assert_eq!(stream.count(), 3);
    }

    // Branch: push accumulates, returns empty stream.
    #[test]
    fn fan_in_push_accumulates() {
        let (pusher_input, _) = stub_with(make_schema("t", "id", "id"));
        let mut fi = FanIn::new(make_schema("t", "id", "id"), vec![]);
        let out: Vec<_> = fi
            .push(
                Change::Add(AddChange {
                    node: node_of(row_with(1)),
                }),
                &*pusher_input as &dyn InputBase,
            )
            .collect();
        assert_eq!(out.len(), 0);
        assert_eq!(fi.accumulated_pushes.lock().unwrap().len(), 1);
    }

    // Branch: fan_out_done_pushing_to_all_branches — no inputs + no pushes → OK.
    #[test]
    fn fan_in_done_pushing_no_inputs_no_pushes_ok() {
        let mut fi = FanIn::new(make_schema("t", "id", "id"), vec![]);
        let out: Vec<_> = fi
            .fan_out_done_pushing_to_all_branches(ChangeType::Add)
            .collect();
        assert!(out.is_empty());
    }

    // Branch: no inputs + non-empty accumulated → panic per TS assert.
    #[test]
    #[should_panic(expected = "If there are no inputs then fan-in should not receive any pushes.")]
    fn fan_in_done_pushing_no_inputs_with_pushes_panics() {
        let mut fi = FanIn::new(make_schema("t", "id", "id"), vec![]);
        fi.accumulated_pushes
            .lock()
            .unwrap()
            .push(Change::Add(AddChange {
                node: node_of(row_with(1)),
            }));
        let _: Vec<_> = fi
            .fan_out_done_pushing_to_all_branches(ChangeType::Add)
            .collect();
    }

    // Branch: with N inputs + accumulated changes → drains and forwards once.
    #[test]
    fn fan_in_done_pushing_with_inputs_drains_and_forwards() {
        let (input, _) = stub_with(make_schema("t", "id", "id"));
        let mut fi = FanIn::new(make_schema("t", "id", "id"), vec![input]);

        let pushes = Arc::new(Mutex::new(Vec::<ChangeType>::new()));
        fi.set_filter_output(Box::new(Recorder {
            pushes: Arc::clone(&pushes),
            filter_calls: Arc::new(AtomicUsize::new(0)),
            filter_result: true,
            yields_per_filter: 0,
            begins: Arc::new(AtomicUsize::new(0)),
            ends: Arc::new(AtomicUsize::new(0)),
        }));

        // Two adds for the same row — push_accumulated_changes collapses
        // them into one downstream Add.
        fi.accumulated_pushes
            .lock()
            .unwrap()
            .push(Change::Add(AddChange {
                node: node_of(row_with(1)),
            }));
        fi.accumulated_pushes
            .lock()
            .unwrap()
            .push(Change::Add(AddChange {
                node: node_of(row_with(1)),
            }));

        let _: Vec<_> = fi
            .fan_out_done_pushing_to_all_branches(ChangeType::Add)
            .collect();

        // Exactly one Add forwarded.
        let p = pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0], ChangeType::Add);
        // Accumulated buffer has been drained.
        assert_eq!(fi.accumulated_pushes.lock().unwrap().len(), 0);
    }

    // Branch: with N inputs + no accumulated → returns early (empty).
    #[test]
    fn fan_in_done_pushing_with_inputs_empty_accumulated_returns_empty() {
        let (input, _) = stub_with(make_schema("t", "id", "id"));
        let mut fi = FanIn::new(make_schema("t", "id", "id"), vec![input]);

        let pushes = Arc::new(Mutex::new(Vec::<ChangeType>::new()));
        fi.set_filter_output(Box::new(Recorder {
            pushes: Arc::clone(&pushes),
            filter_calls: Arc::new(AtomicUsize::new(0)),
            filter_result: true,
            yields_per_filter: 0,
            begins: Arc::new(AtomicUsize::new(0)),
            ends: Arc::new(AtomicUsize::new(0)),
        }));

        let out: Vec<_> = fi
            .fan_out_done_pushing_to_all_branches(ChangeType::Add)
            .collect();
        assert!(out.is_empty());
        assert_eq!(pushes.lock().unwrap().len(), 0);
    }
}
