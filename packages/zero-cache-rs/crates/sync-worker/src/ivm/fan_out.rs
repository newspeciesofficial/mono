//! Port of `packages/zql/src/ivm/fan-out.ts`.
//!
//! Public exports ported:
//!
//! - [`FanOut`] — the `FilterOperator` that forks one upstream filter
//!   stream into N downstream branches. Paired with a [`super::fan_in::FanIn`]
//!   that merges the branches back.
//!
//! ## Ownership divergence from TS
//!
//! The TS ctor calls `input.setFilterOutput(this)` so upstream pushes land
//! on the newly-constructed `FanOut`. Porting that creates an ownership
//! cycle — upstream owns `this`, `this` owns upstream. As elsewhere in the
//! port, the pipeline driver (Layer 9+) is responsible for wiring
//! upstream → `FanOut::push` explicitly. Within this module `FanOut`'s
//! [`crate::ivm::operator::Output`] and [`FilterOutput`] methods are the
//! downstream entry points.
//!
//! TS stores its FanIn as `#fanIn: FanIn | undefined`, initialised via
//! `setFanIn(...)`. In Rust we model that as
//! [`Option<Arc<FanIn>>`]: shared ownership with no outer Mutex, so
//! back-edges can call helper methods via `&self` without holding a
//! reentrant lock (which caused deadlocks under multi-branch pushes).
//!
//! The outputs list is `Vec<Box<dyn FilterOutput>>` (not shared): each
//! downstream is a unique edge owned by the FanOut exactly like TS.

use std::sync::{Arc, Mutex};

use crate::ivm::change::Change;
use crate::ivm::data::Node;
use crate::ivm::fan_in::FanIn;
use crate::ivm::filter_operators::{FilterInput, FilterOperator, FilterOutput};
use crate::ivm::operator::{InputBase, Output};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

/// TS `FanOut` — forks one upstream filter stream into N downstream
/// branches.
pub struct FanOut {
    input: Mutex<Box<dyn FilterInput>>,
    outputs: Mutex<Vec<Box<dyn FilterOutput>>>,
    fan_in: Mutex<Option<Arc<FanIn>>>,
    destroy_count: Mutex<usize>,
    schema: SourceSchema,
}

impl FanOut {
    /// TS `new FanOut(input)`.
    ///
    /// Does NOT call `input.setFilterOutput(this)`; see module doc.
    pub fn new(input: Box<dyn FilterInput>) -> Self {
        let schema = input.get_schema().clone();
        Self {
            input: Mutex::new(input),
            outputs: Mutex::new(Vec::new()),
            fan_in: Mutex::new(None),
            destroy_count: Mutex::new(0),
            schema,
        }
    }

    /// Wired variant: matches TS `new FanOut(...)` followed by
    /// `input.setFilterOutput(this)`. Returns `Arc<Self>` (no outer
    /// Mutex) so back-edges forward pushes without reentrant-lock
    /// deadlock.
    pub fn new_wired(input: Box<dyn FilterInput>) -> Arc<Self> {
        let arc = Arc::new(Self::new(input));
        let back: Box<dyn FilterOutput> = Box::new(FanOutPushBackEdge(Arc::clone(&arc)));
        arc.input
            .lock()
            .expect("fan_out input mutex poisoned")
            .set_filter_output(back);
        arc
    }

    /// TS `setFanIn(fanIn)`.
    pub fn set_fan_in(&self, fan_in: Arc<FanIn>) {
        let mut guard = self.fan_in.lock().expect("fan_out fan_in mutex poisoned");
        *guard = Some(fan_in);
    }

    /// &self variant of `set_filter_output`.
    pub fn set_filter_output_arc(&self, output: Box<dyn FilterOutput>) {
        self.outputs
            .lock()
            .expect("fan_out outputs mutex poisoned")
            .push(output);
    }

    /// &self variant of destroy (counts calls, destroys upstream once).
    pub fn destroy_arc(&self) {
        let n_outputs = self
            .outputs
            .lock()
            .expect("fan_out outputs mutex poisoned")
            .len();
        let mut count = self
            .destroy_count
            .lock()
            .expect("fan_out destroy_count mutex poisoned");
        if *count < n_outputs {
            *count += 1;
            if *count == n_outputs {
                drop(count);
                self.input
                    .lock()
                    .expect("fan_out input mutex poisoned")
                    .destroy();
            }
        } else {
            panic!("FanOut already destroyed once for each output");
        }
    }

    /// TS `destroy()` — deprecated &mut variant; delegates to `destroy_arc`.
    pub fn destroy_downstream(&mut self) {
        self.destroy_arc();
    }

    /// &self begin_filter — broadcasts to all outputs.
    pub fn begin_filter_arc(&self) {
        let mut outs = self.outputs.lock().expect("fan_out outputs mutex poisoned");
        for o in outs.iter_mut() {
            o.begin_filter();
        }
    }

    /// &self end_filter — broadcasts to all outputs.
    pub fn end_filter_arc(&self) {
        let mut outs = self.outputs.lock().expect("fan_out outputs mutex poisoned");
        for o in outs.iter_mut() {
            o.end_filter();
        }
    }

    /// &self filter — fans out with TS short-circuit semantics.
    pub fn filter_arc(&self, node: &Node) -> (Vec<Yield>, bool) {
        let mut outs = self.outputs.lock().expect("fan_out outputs mutex poisoned");
        let mut yields: Vec<Yield> = Vec::new();
        let mut result = false;
        for out in outs.iter_mut() {
            let (stream, keep) = out.filter(node);
            for y in stream {
                yields.push(y);
            }
            result = keep || result;
            if result {
                return (yields, true);
            }
        }
        (yields, result)
    }

    /// &self push — broadcasts to all outputs then notifies fan_in.
    pub fn push_arc(&self, change: Change, _pusher: &dyn InputBase) -> Vec<Yield> {
        let change_type = change.change_type();
        let mut yields: Vec<Yield> = Vec::new();

        {
            let mut outs = self.outputs.lock().expect("fan_out outputs mutex poisoned");
            for out in outs.iter_mut() {
                for y in out.push(change.clone(), self as &dyn InputBase) {
                    yields.push(y);
                }
            }
        }

        let fan_in = {
            let guard = self.fan_in.lock().expect("fan_out fan_in mutex poisoned");
            guard
                .as_ref()
                .expect("fan-out must have a corresponding fan-in set!")
                .clone()
        };
        for y in fan_in.fan_out_done_pushing_to_all_branches_arc(change_type) {
            yields.push(y);
        }

        yields
    }
}

impl InputBase for FanOut {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn destroy(&mut self) {
        self.destroy_arc();
    }
}

impl FilterInput for FanOut {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        self.set_filter_output_arc(output);
    }
}

impl FilterOutput for FanOut {
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

impl Output for FanOut {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let items = self.push_arc(change, pusher);
        Box::new(items.into_iter())
    }
}

impl FilterOperator for FanOut {}

/// Back-edge adapter installed on [`FanOut::input`] (a FilterInput).
/// Created by [`FanOut::new_wired`]. Holds `Arc<FanOut>` directly —
/// no outer Mutex — so pushes forward through interior `*_arc`
/// helpers and cannot deadlock on reentrant locks.
pub struct FanOutPushBackEdge(pub Arc<FanOut>);

impl Output for FanOutPushBackEdge {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        eprintln!("[TRACE ivm] FanOut::push enter");
        let items = self.0.push_arc(change, pusher);
        eprintln!("[TRACE ivm] FanOut::push exit yields={}", items.len());
        Box::new(items.into_iter())
    }
}

impl FilterOutput for FanOutPushBackEdge {
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

/// Adapter to plug a wired [`Arc<FanOut>`] back into the chain
/// as `Box<dyn FilterInput>`. Schema cached at construction.
pub struct ArcFanOutAsInput {
    pub(crate) inner: Arc<FanOut>,
    schema: SourceSchema,
}

impl ArcFanOutAsInput {
    pub fn new(inner: Arc<FanOut>) -> Self {
        let schema = inner.get_schema().clone();
        Self { inner, schema }
    }
}

impl InputBase for ArcFanOutAsInput {
    fn get_schema(&self) -> &SourceSchema { &self.schema }
    fn destroy(&mut self) { self.inner.destroy_arc(); }
}

impl FilterInput for ArcFanOutAsInput {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        self.inner.set_filter_output_arc(output);
    }
}

impl Output for ArcFanOutAsInput {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let items = self.inner.push_arc(change, pusher);
        Box::new(items.into_iter())
    }
}

impl FilterOutput for ArcFanOutAsInput {
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

impl FilterOperator for ArcFanOutAsInput {}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!   - `new` creates empty outputs list.
    //!   - `set_filter_output` appends output.
    //!   - `set_fan_in` stores the back-reference.
    //!   - `destroy_downstream` increments count; fires upstream destroy
    //!     exactly once when final output calls it.
    //!   - `destroy_downstream` panics on over-call.
    //!   - `destroy_downstream` with zero outputs → panics immediately
    //!     (since `count < 0` is false).
    //!   - `get_schema` delegates.
    //!   - `begin_filter` / `end_filter` fan out to all outputs (covered
    //!     by Recorder observers).
    //!   - `filter` — 0 outputs → returns (empty, false).
    //!   - `filter` — predicate=false on all outputs → returns false.
    //!   - `filter` — first output true → short-circuit, remaining outputs
    //!     NOT called.
    //!   - `filter` — yields from outputs are propagated.
    //!   - `push` — fan_in is None → panics with TS message.
    //!   - `push` — broadcasts change to every output AND calls fan_in's
    //!     `fan_out_done_pushing_to_all_branches` with correct ChangeType.
    //!   - `push` with 0 outputs still signals fan_in.

    use super::*;
    use crate::ivm::change::{AddChange, ChangeType};
    use crate::ivm::data::{Node, make_comparator};
    use crate::ivm::fan_in::FanIn;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrdering};
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Row;

    fn make_schema(name: &str) -> SourceSchema {
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
        SourceSchema {
            table_name: name.into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
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

    // Stub upstream FilterInput.
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

    fn stub_input() -> (Box<dyn FilterInput>, Arc<AtomicBool>) {
        let d = Arc::new(AtomicBool::new(false));
        (
            Box::new(StubFilterInput {
                schema: make_schema("t"),
                destroyed: Arc::clone(&d),
            }),
            d,
        )
    }

    /// Records pushes / filters / begin/end.
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

    fn mk_recorder(
        filter_result: bool,
        yields_per_filter: usize,
    ) -> (Recorder, Arc<Mutex<Vec<ChangeType>>>, Arc<AtomicUsize>) {
        let pushes = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Recorder {
                pushes: Arc::clone(&pushes),
                filter_calls: Arc::clone(&calls),
                filter_result,
                yields_per_filter,
                begins: Arc::new(AtomicUsize::new(0)),
                ends: Arc::new(AtomicUsize::new(0)),
            },
            pushes,
            calls,
        )
    }

    // Branch: construction leaves outputs empty, fan_in unset.
    #[test]
    fn fan_out_new_empty_outputs_and_no_fan_in() {
        let (input, _) = stub_input();
        let fo = FanOut::new(input);
        assert_eq!(fo.outputs.lock().unwrap().len(), 0);
        assert!(fo.fan_in.lock().unwrap().is_none());
    }

    // Branch: set_filter_output appends each new output in order.
    #[test]
    fn fan_out_set_filter_output_appends() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        let (r1, _, _) = mk_recorder(true, 0);
        let (r2, _, _) = mk_recorder(true, 0);
        fo.set_filter_output(Box::new(r1));
        fo.set_filter_output(Box::new(r2));
        assert_eq!(fo.outputs.lock().unwrap().len(), 2);
    }

    // Branch: set_fan_in stores the back-reference (is_some after call).
    #[test]
    fn fan_out_set_fan_in_stores() {
        let (input, _) = stub_input();
        let (input2, _) = stub_input();
        let fo = FanOut::new(input);
        let fi = Arc::new(FanIn::new(make_schema("t"), vec![input2]));
        fo.set_fan_in(fi);
        assert!(fo.fan_in.lock().unwrap().is_some());
    }

    // Branch: get_schema delegates to upstream input.
    #[test]
    fn fan_out_get_schema_delegates() {
        let (input, _) = stub_input();
        let fo = FanOut::new(input);
        assert_eq!(fo.get_schema().table_name, "t");
    }

    // Branch: destroy_downstream — count < n_outputs → increments only.
    #[test]
    fn fan_out_destroy_downstream_increments_before_final() {
        let (input, destroyed) = stub_input();
        let mut fo = FanOut::new(input);
        let (r1, _, _) = mk_recorder(true, 0);
        let (r2, _, _) = mk_recorder(true, 0);
        let (r3, _, _) = mk_recorder(true, 0);
        fo.set_filter_output(Box::new(r1));
        fo.set_filter_output(Box::new(r2));
        fo.set_filter_output(Box::new(r3));
        fo.destroy_downstream();
        fo.destroy_downstream();
        assert!(!destroyed.load(AtOrdering::SeqCst));
    }

    // Branch: destroy_downstream — count reaches n_outputs → fires upstream destroy.
    #[test]
    fn fan_out_destroy_downstream_final_call_destroys_upstream() {
        let (input, destroyed) = stub_input();
        let mut fo = FanOut::new(input);
        let (r1, _, _) = mk_recorder(true, 0);
        let (r2, _, _) = mk_recorder(true, 0);
        fo.set_filter_output(Box::new(r1));
        fo.set_filter_output(Box::new(r2));
        fo.destroy_downstream();
        fo.destroy_downstream();
        assert!(destroyed.load(AtOrdering::SeqCst));
    }

    // Branch: destroy_downstream — called once more than n_outputs → panic.
    #[test]
    #[should_panic(expected = "FanOut already destroyed once for each output")]
    fn fan_out_destroy_downstream_overcall_panics() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        let (r1, _, _) = mk_recorder(true, 0);
        fo.set_filter_output(Box::new(r1));
        fo.destroy_downstream();
        fo.destroy_downstream(); // over-call
    }

    // Branch: destroy_downstream — zero outputs → panics immediately.
    #[test]
    #[should_panic(expected = "FanOut already destroyed once for each output")]
    fn fan_out_destroy_downstream_with_zero_outputs_panics() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        fo.destroy_downstream();
    }

    // Branch: begin_filter / end_filter broadcast to every output.
    #[test]
    fn fan_out_begin_end_filter_broadcast() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        let begins1 = Arc::new(AtomicUsize::new(0));
        let ends1 = Arc::new(AtomicUsize::new(0));
        let begins2 = Arc::new(AtomicUsize::new(0));
        let ends2 = Arc::new(AtomicUsize::new(0));
        let r1 = Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
            filter_calls: Arc::new(AtomicUsize::new(0)),
            filter_result: true,
            yields_per_filter: 0,
            begins: Arc::clone(&begins1),
            ends: Arc::clone(&ends1),
        };
        let r2 = Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
            filter_calls: Arc::new(AtomicUsize::new(0)),
            filter_result: true,
            yields_per_filter: 0,
            begins: Arc::clone(&begins2),
            ends: Arc::clone(&ends2),
        };
        fo.set_filter_output(Box::new(r1));
        fo.set_filter_output(Box::new(r2));
        fo.begin_filter();
        fo.end_filter();
        assert_eq!(begins1.load(AtOrdering::SeqCst), 1);
        assert_eq!(begins2.load(AtOrdering::SeqCst), 1);
        assert_eq!(ends1.load(AtOrdering::SeqCst), 1);
        assert_eq!(ends2.load(AtOrdering::SeqCst), 1);
    }

    // Branch: filter with 0 outputs → (empty, false).
    #[test]
    fn fan_out_filter_no_outputs_returns_false() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        let (stream, keep) = fo.filter(&node_of(row_with(1)));
        assert!(!keep);
        assert_eq!(stream.count(), 0);
    }

    // Branch: filter with all-false outputs → (empty, false).
    #[test]
    fn fan_out_filter_all_false_returns_false() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        let (r1, _, calls1) = mk_recorder(false, 0);
        let (r2, _, calls2) = mk_recorder(false, 0);
        fo.set_filter_output(Box::new(r1));
        fo.set_filter_output(Box::new(r2));
        let (_stream, keep) = fo.filter(&node_of(row_with(1)));
        assert!(!keep);
        // Both outputs were consulted (no short-circuit).
        assert_eq!(calls1.load(AtOrdering::SeqCst), 1);
        assert_eq!(calls2.load(AtOrdering::SeqCst), 1);
    }

    // Branch: filter short-circuits on first true — remaining outputs NOT called.
    #[test]
    fn fan_out_filter_short_circuits_on_first_true() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        let (r1, _, calls1) = mk_recorder(true, 0);
        let (r2, _, calls2) = mk_recorder(true, 0);
        fo.set_filter_output(Box::new(r1));
        fo.set_filter_output(Box::new(r2));
        let (_stream, keep) = fo.filter(&node_of(row_with(1)));
        assert!(keep);
        assert_eq!(calls1.load(AtOrdering::SeqCst), 1);
        // Short-circuit: r2 must not be called.
        assert_eq!(calls2.load(AtOrdering::SeqCst), 0);
    }

    // Branch: filter propagates yields emitted by outputs visited before
    // short-circuit.
    #[test]
    fn fan_out_filter_propagates_yields() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        let (r1, _, _) = mk_recorder(false, 2);
        let (r2, _, _) = mk_recorder(true, 3);
        fo.set_filter_output(Box::new(r1));
        fo.set_filter_output(Box::new(r2));
        let (stream, keep) = fo.filter(&node_of(row_with(1)));
        assert!(keep);
        // 2 yields from r1 + 3 yields from r2 = 5 total.
        assert_eq!(stream.count(), 5);
    }

    // Branch: push without fan_in → panic with TS message.
    #[test]
    #[should_panic(expected = "fan-out must have a corresponding fan-in set!")]
    fn fan_out_push_without_fan_in_panics() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        let (pusher_input, _) = stub_input();
        let _: Vec<_> = fo
            .push(
                Change::Add(AddChange {
                    node: node_of(row_with(1)),
                }),
                &*pusher_input as &dyn InputBase,
            )
            .collect();
    }

    // Branch: push broadcasts to all outputs AND notifies fan_in.
    #[test]
    fn fan_out_push_broadcasts_and_notifies_fan_in() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);

        let (r1, pushes1, _) = mk_recorder(true, 0);
        let (r2, pushes2, _) = mk_recorder(true, 0);
        let (r3, pushes3, _) = mk_recorder(true, 0);
        fo.set_filter_output(Box::new(r1));
        fo.set_filter_output(Box::new(r2));
        fo.set_filter_output(Box::new(r3));

        // Fan_in with zero inputs: TS allows this (see fan-out-fan-in.test.ts
        // "fan-out pushes along all paths" which constructs `new FanIn(fanOut, [])`).
        let fi = Arc::new(FanIn::new(make_schema("t"), vec![]));
        fo.set_fan_in(Arc::clone(&fi));

        let (pusher_input, _) = stub_input();
        let _: Vec<_> = fo
            .push(
                Change::Add(AddChange {
                    node: node_of(row_with(1)),
                }),
                &*pusher_input as &dyn InputBase,
            )
            .collect();

        assert_eq!(pushes1.lock().unwrap().len(), 1);
        assert_eq!(pushes2.lock().unwrap().len(), 1);
        assert_eq!(pushes3.lock().unwrap().len(), 1);
        assert_eq!(pushes1.lock().unwrap()[0], ChangeType::Add);
    }

    // Branch: push with 0 outputs still notifies fan_in (no panic since
    // fan_in is set).
    #[test]
    fn fan_out_push_zero_outputs_still_notifies_fan_in() {
        let (input, _) = stub_input();
        let mut fo = FanOut::new(input);
        let fi = Arc::new(FanIn::new(make_schema("t"), vec![]));
        fo.set_fan_in(Arc::clone(&fi));

        let (pusher_input, _) = stub_input();
        let out: Vec<_> = fo
            .push(
                Change::Add(AddChange {
                    node: node_of(row_with(1)),
                }),
                &*pusher_input as &dyn InputBase,
            )
            .collect();
        // No outputs means no yields accumulated.
        assert_eq!(out.len(), 0);
    }
}
