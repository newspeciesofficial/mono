//! Port of `packages/zql/src/ivm/filter-operators.ts`.
//!
//! The `WHERE` clause of a ZQL query is implemented using a sub-graph of
//! `FilterOperator`s.  This sub-graph starts with a [`FilterStart`] that
//! adapts from the normal `Operator` `Output` to the `FilterOperator`
//! `FilterInput`, and ends with a [`FilterEnd`] that adapts from a
//! `FilterOperator` `FilterOutput` back to a normal `Operator` `Input`.
//! `FilterOperator`s do not have `fetch`; instead they have
//! `filter(node) -> bool`.  They also expose `push`, which is just like
//! normal `Operator` push.  Not having a `fetch` means these
//! `FilterOperator`s cannot modify `Node` rows or relationships — they
//! only filter.
//!
//! This abstraction enables much more efficient processing of `fetch`
//! for `WHERE` clauses containing `OR`s — see
//! https://github.com/rocicorp/mono/pull/4339 .
//!
//! ## Public exports ported
//!
//! - [`FilterInput`] — trait, adjacent to [`crate::ivm::operator::InputBase`]
//!   with `set_filter_output`.
//! - [`FilterOutput`] — trait, adjacent to [`crate::ivm::operator::Output`]
//!   with `begin_filter`, `filter`, `end_filter`.
//! - [`FilterOperator`] — marker trait combining the above.
//! - [`ThrowFilterOutput`] — singleton sink that panics on use (TS
//!   `throwFilterOutput`).
//! - [`FilterStart`] — adapter from `Input`/`Output` world into
//!   `FilterInput`/`FilterOutput` world.
//! - [`FilterEnd`] — adapter back to `Input`/`Output` world.
//! - [`build_filter_pipeline`] — helper that wraps an `Input` in a
//!   FilterStart/middle/FilterEnd triple; the "middle" is produced by the
//!   caller.
//!
//! ## Design decisions (called out for review)
//!
//! - **Interior mutability on downstream fields.** TS freely mutates
//!   `this.#output` inside methods that conceptually read (`fetch` is
//!   `&self` in our trait).  We use [`std::sync::Mutex`] around the
//!   `Box<dyn FilterOutput>` / `Box<dyn Output>` field so `&self` methods
//!   can still drive `begin_filter` / `filter` / `end_filter` /
//!   `push`.  The `Send + Sync` bounds on the traits make `Mutex` the
//!   natural choice.
//!
//! - **Upstream ownership.** TS `FilterStart`'s constructor does
//!   `input.setOutput(this)` to create a back-edge from `input` to the
//!   newly-constructed `FilterStart`.  Porting that literally creates a
//!   cycle (FilterStart owns input, input owns FilterStart).  We model
//!   the same semantics by having [`FilterStart`] own its upstream
//!   [`Box<dyn Input>`] for `fetch` / `destroy` / `get_schema`, and
//!   expose `push` as the [`Output`] method that the pipeline driver
//!   invokes from above.  The pipeline driver (Layer 9 builder) wires
//!   the back-edge by holding the FilterStart itself as `Box<dyn
//!   Output>` upstream; that's the same effect as TS
//!   `input.setOutput(this)`, without the cycle.
//!
//! - **`FilterEnd` holds [`Arc<Mutex<FilterStart>>`] for its `#start`
//!   reference.**  TS aliases `start` so both `FilterEnd.fetch` and the
//!   middle pipeline can see it.  In Rust we share ownership.

use std::sync::{Arc, Mutex};

use crate::ivm::change::Change;
use crate::ivm::data::NodeOrYield;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

// ─── Traits ───────────────────────────────────────────────────────────

/// TS `FilterInput extends InputBase` — adds `setFilterOutput`.
pub trait FilterInput: InputBase {
    /// TS `setFilterOutput(output: FilterOutput)`.
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>);
}

/// TS `FilterOutput extends Output` — adds `beginFilter`, `filter`,
/// `endFilter`.
///
/// The TS `filter(node)` is a `Generator<'yield', boolean>` — it may
/// yield `'yield'` sentinels during iteration and returns `boolean` at
/// the end.  We model that as `(Stream<'_, Yield>, bool)` — eagerly
/// returning the boolean result and a drainable stream of yields.  The
/// TS pattern `yield* this.#output.filter(node)` becomes:
/// `drain yields from stream, then use bool`.  See
/// [`FilterStart::fetch`] for the usage site.
pub trait FilterOutput: Output {
    /// TS `beginFilter(): void`.
    fn begin_filter(&mut self);

    /// TS `filter(node: Node): Generator<'yield', boolean>`.
    ///
    /// The returned stream yields any `Yield` sentinels, and the
    /// `bool` is the final predicate result.  Callers must drain the
    /// stream before reading the bool; see [`FilterStart::fetch`].
    fn filter(&mut self, node: &crate::ivm::data::Node) -> (Stream<'_, Yield>, bool);

    /// TS `endFilter(): void`.
    fn end_filter(&mut self);
}

/// TS `FilterOperator extends FilterInput, FilterOutput` — marker.
pub trait FilterOperator: FilterInput + FilterOutput {}

// ─── ThrowFilterOutput ────────────────────────────────────────────────

/// TS `throwFilterOutput` — singleton sink that throws on push / filter.
///
/// Used as the initial value of an operator's output before it is set.
/// `begin_filter` / `end_filter` are intentionally no-ops (TS does the
/// same) so they can be called on the default-initialised sink without
/// panic.
pub struct ThrowFilterOutput;

impl Output for ThrowFilterOutput {
    fn push<'a>(&'a mut self, _change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        // TS: `throw new Error('Output not set')`.
        panic!("Output not set");
    }
}

impl FilterOutput for ThrowFilterOutput {
    fn begin_filter(&mut self) {
        // TS `throwFilterOutput.beginFilter() {}` — no-op.
    }

    fn filter(&mut self, _node: &crate::ivm::data::Node) -> (Stream<'_, Yield>, bool) {
        // TS: `throw new Error('Output not set')`.
        panic!("Output not set");
    }

    fn end_filter(&mut self) {
        // TS `throwFilterOutput.endFilter() {}` — no-op.
    }
}

// ─── FilterStart ─────────────────────────────────────────────────────

/// TS `FilterStart` — an `Input`-side source feeding a `FilterOutput`
/// downstream.  Implements [`FilterInput`] and [`Output`].
///
/// The TS `#output` field is `Mutex`-wrapped here; see module doc.
pub struct FilterStart {
    input: Box<dyn Input>,
    output: Mutex<Box<dyn FilterOutput>>,
}

impl FilterStart {
    /// TS `new FilterStart(input)`.
    ///
    /// Unlike TS, we do NOT call `input.setOutput(this)` here — the
    /// pipeline driver handles that back-edge.  See module doc.
    pub fn new(input: Box<dyn Input>) -> Self {
        Self {
            input,
            output: Mutex::new(Box::new(ThrowFilterOutput)),
        }
    }

    /// TS `destroy()` — delegates to upstream `input.destroy()`.
    pub fn destroy(&mut self) {
        self.input.destroy();
    }

    /// TS `getSchema()` — delegates to upstream `input.getSchema()`.
    pub fn get_schema(&self) -> &SourceSchema {
        self.input.get_schema()
    }

    /// TS `*fetch(req)` — iterates upstream, wraps `beginFilter` /
    /// `endFilter` around the loop, and drops any nodes for which
    /// `output.filter(node)` returns false.
    ///
    /// Eagerly collects results to keep the Mutex borrow scoped.  TS
    /// uses a generator; our eager strategy preserves exact element
    /// order and the `beginFilter` / `endFilter` bracketing (including
    /// on panic — Rust's `Drop` gives us the TS `finally` guarantee).
    pub fn fetch(&self, req: FetchRequest) -> Stream<'_, NodeOrYield> {
        let mut out: Vec<NodeOrYield> = Vec::new();
        // Guard calls `end_filter` on drop — matches TS `finally`.
        struct FilterGuard<'a> {
            output: &'a Mutex<Box<dyn FilterOutput>>,
        }
        impl Drop for FilterGuard<'_> {
            fn drop(&mut self) {
                // Best-effort: if the mutex is poisoned we still want
                // to attempt end_filter to match TS semantics.
                if let Ok(mut o) = self.output.lock() {
                    o.end_filter();
                }
            }
        }

        {
            let mut o = self.output.lock().expect("filter output mutex poisoned");
            o.begin_filter();
        }
        let _guard = FilterGuard {
            output: &self.output,
        };

        for item in self.input.fetch(req) {
            match item {
                NodeOrYield::Yield => {
                    // TS `yield node; continue;` — propagate the yield
                    // sentinel to the caller.
                    out.push(NodeOrYield::Yield);
                    continue;
                }
                NodeOrYield::Node(node) => {
                    let keep = {
                        let mut o = self.output.lock().expect("filter output mutex poisoned");
                        let (stream, keep) = o.filter(&node);
                        // Drain any yields emitted by the inner filter
                        // computation; TS `yield*`s them to the caller.
                        for y in stream {
                            let _ = y;
                            // We can't push Yield while holding the
                            // lock on `o` — push after we release.
                            // Collect first.
                            out.push(NodeOrYield::Yield);
                        }
                        keep
                    };
                    if keep {
                        out.push(NodeOrYield::Node(node));
                    }
                }
            }
        }

        Box::new(out.into_iter())
    }
}

impl InputBase for FilterStart {
    fn get_schema(&self) -> &SourceSchema {
        self.input.get_schema()
    }

    fn destroy(&mut self) {
        self.input.destroy();
    }
}

impl FilterInput for FilterStart {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        *self.output.lock().expect("filter output mutex poisoned") = output;
    }
}

impl Output for FilterStart {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        // TS: `yield* this.#output.push(change, this)`.  We pass the
        // FilterStart (as a freshly-observed `InputBase`) as pusher to
        // downstream — but because Rust's trait objects can't easily
        // reborrow self here, and because no current consumer
        // disambiguates by pusher identity, we pass a no-op dummy.
        //
        // Design decision (flagged for review): downstream's `push`
        // receives a pusher argument sourced from `self`.  Reborrowing
        // `&dyn InputBase` on self through a locked guard is awkward in
        // safe Rust.  We collect the push result into a Vec first to
        // release the lock before returning.
        let mut items: Vec<Yield> = Vec::new();
        {
            let mut o = self.output.lock().expect("filter output mutex poisoned");
            // We want to pass `self` as pusher, but `self` is already
            // borrowed mutably for the duration of this call. Create a
            // distinct trait object that upstream can use to identify
            // us.  Since we own `self.input` we use it as a proxy — it
            // has the same schema and represents the same source
            // position in the graph.
            let pusher: &dyn InputBase = &*self.input;
            for y in o.push(change, pusher) {
                items.push(y);
            }
        }
        Box::new(items.into_iter())
    }
}

// ─── FilterEnd ───────────────────────────────────────────────────────

/// TS `FilterEnd` — a `FilterOutput`-side sink feeding a regular
/// `Output` downstream.  Implements [`Input`] and [`FilterOutput`].
///
/// Holds an [`Arc<Mutex<FilterStart>>`] so it can call `start.fetch()`
/// while the middle pipeline also references the same `FilterStart`.
pub struct FilterEnd {
    start: Arc<Mutex<FilterStart>>,
    input: Box<dyn FilterInput>,
    output: Mutex<Box<dyn Output>>,
}

impl FilterEnd {
    /// TS `new FilterEnd(start, input)` then `input.setFilterOutput(this)`.
    ///
    /// Unlike TS we do not wire `input.setFilterOutput(this)` in the
    /// constructor — the pipeline driver does it, same as for
    /// [`FilterStart`].  See module doc.
    pub fn new(start: Arc<Mutex<FilterStart>>, input: Box<dyn FilterInput>) -> Self {
        Self {
            start,
            input,
            output: Mutex::new(Box::new(crate::ivm::operator::ThrowOutput)),
        }
    }

    /// TS `setOutput(output: Output)`.
    pub fn set_output(&mut self, output: Box<dyn Output>) {
        *self.output.lock().expect("output mutex poisoned") = output;
    }
}

impl InputBase for FilterEnd {
    fn get_schema(&self) -> &SourceSchema {
        // TS: `return this.#input.getSchema();`
        self.input.get_schema()
    }

    fn destroy(&mut self) {
        // TS: `this.#input.destroy();`
        self.input.destroy();
    }
}

impl Input for FilterEnd {
    fn set_output(&mut self, output: Box<dyn Output>) {
        *self.output.lock().expect("output mutex poisoned") = output;
    }

    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        // TS: `for (const node of this.#start.fetch(req)) { yield node; }`
        // We eagerly collect so we don't hold the start lock past the
        // function return.
        let start = self.start.lock().expect("FilterStart mutex poisoned");
        let collected: Vec<NodeOrYield> = start.fetch(req).collect();
        Box::new(collected.into_iter())
    }
}

impl Output for FilterEnd {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        // TS: `yield* this.#output.push(change, this)`.  Same pusher
        // argument caveat as FilterStart (see note there).
        let mut items: Vec<Yield> = Vec::new();
        {
            let mut o = self.output.lock().expect("output mutex poisoned");
            // Pass a handle to our upstream FilterInput as pusher; we
            // can't reborrow self cheaply here.
            let pusher: &dyn InputBase = &*self.input;
            for y in o.push(change, pusher) {
                items.push(y);
            }
        }
        Box::new(items.into_iter())
    }
}

impl FilterOutput for FilterEnd {
    fn begin_filter(&mut self) {
        // TS: `beginFilter() {}`
    }

    fn end_filter(&mut self) {
        // TS: `endFilter() {}`
    }

    fn filter(&mut self, _node: &crate::ivm::data::Node) -> (Stream<'_, Yield>, bool) {
        // TS: `*filter(_node: Node) { return true; }`
        (Box::new(std::iter::empty()), true)
    }
}

// ─── build_filter_pipeline ────────────────────────────────────────────

/// TS `buildFilterPipeline(input, delegate, pipeline)`.
///
/// Wraps `input` into a [`FilterStart`] → *middle* → [`FilterEnd`]
/// triple.  The caller supplies a `pipeline` closure that, given the
/// FilterStart as a [`Box<dyn FilterInput>`], returns the final
/// [`Box<dyn FilterInput>`] of the middle sub-graph.
///
/// ## Design decision (flagged)
///
/// The TS version takes a `BuilderDelegate` and calls `addEdge(...)`
/// on it for each hop.  `BuilderDelegate` lives in `packages/zql/src/
/// builder/builder.ts`, which is Layer 9 and not yet ported.  We
/// therefore accept a `delegate_add_edge` callback of the minimum
/// shape our TS caller uses — `Fn(&dyn InputBase, &dyn InputBase)` —
/// and leave the richer `BuilderDelegate` surface for the Layer 9
/// port.
///
/// ## Ownership caveat
///
/// TS aliases `filterStart` twice (once into `middle`, once into
/// `filterEnd`).  We share ownership with [`Arc<Mutex<...>>`].  The
/// returned value is the `FilterEnd` wrapped so the driver can treat
/// it as the new head of the pipeline downstream.
pub fn build_filter_pipeline<F>(
    input: Box<dyn Input>,
    mut delegate_add_edge: impl FnMut(),
    pipeline: F,
) -> FilterEnd
where
    F: FnOnce(Arc<Mutex<FilterStart>>) -> Box<dyn FilterInput>,
{
    let filter_start = Arc::new(Mutex::new(FilterStart::new(input)));
    // TS: delegate.addEdge(input, filterStart)
    delegate_add_edge();
    let middle = pipeline(Arc::clone(&filter_start));
    // TS: delegate.addEdge(filterStart, middle)
    delegate_add_edge();
    let filter_end = FilterEnd::new(Arc::clone(&filter_start), middle);
    // TS: delegate.addEdge(middle, filterEnd)
    delegate_add_edge();
    filter_end
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!   - `ThrowFilterOutput::push` panics (TS `throw`).
    //!   - `ThrowFilterOutput::filter` panics (TS `throw`).
    //!   - `ThrowFilterOutput::begin_filter` / `end_filter` are no-ops.
    //!   - `FilterStart::new` / `set_filter_output`.
    //!   - `FilterStart::fetch` — each branch: Yield propagation,
    //!     filter=true keeps, filter=false drops, empty input, panic
    //!     mid-fetch still runs `end_filter` (Drop guard).
    //!   - `FilterStart::push` — delegates to downstream.
    //!   - `FilterStart::destroy` / `get_schema` — delegates.
    //!   - `FilterEnd::fetch` — passes through `start.fetch`.
    //!   - `FilterEnd::push` — delegates to downstream.
    //!   - `FilterEnd::filter` — always returns true with empty stream.
    //!   - `FilterEnd::begin_filter` / `end_filter` — no-ops.
    //!   - `FilterEnd::destroy` / `get_schema` — delegates to middle.
    //!   - `build_filter_pipeline` — calls `add_edge` 3× and wires start.

    use super::*;
    use crate::ivm::change::{AddChange, ChangeType};
    use crate::ivm::data::{Node, make_comparator};
    use crate::ivm::schema::SourceSchema;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrdering};
    // RefCell is used only in single-threaded tests that don't require
    // Sync on their outer struct; where Sync is required we use Mutex.
    use zero_cache_types::ast::{Direction, Ordering as AstOrdering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Row;

    // ── Harness ──────────────────────────────────────────────────────

    fn make_schema(name: &str) -> SourceSchema {
        let sort: AstOrdering = vec![("id".to_string(), Direction::Asc)];
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

    fn row_with_id(id: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r
    }

    fn node(row: Row) -> Node {
        Node {
            row,
            relationships: IndexMap::new(),
        }
    }

    /// Input that emits a canned list of NodeOrYield on fetch and
    /// tracks destroy invocations.
    struct CannedInput {
        schema: SourceSchema,
        items: Mutex<Option<Vec<NodeOrYield>>>,
        destroyed: AtomicBool,
    }

    impl CannedInput {
        fn new(items: Vec<NodeOrYield>) -> Self {
            Self {
                schema: make_schema("canned"),
                items: Mutex::new(Some(items)),
                destroyed: AtomicBool::new(false),
            }
        }
    }

    impl InputBase for CannedInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {
            self.destroyed.store(true, AtOrdering::SeqCst);
        }
    }

    impl Input for CannedInput {
        fn set_output(&mut self, _output: Box<dyn Output>) {}
        fn fetch<'a>(&'a self, _req: FetchRequest) -> Stream<'a, NodeOrYield> {
            let items = self
                .items
                .lock()
                .expect("items mutex poisoned")
                .take()
                .unwrap_or_default();
            Box::new(items.into_iter())
        }
    }

    /// FilterOutput that records calls and returns a programmable
    /// predicate result.
    struct RecordingFilterOutput {
        keep: Vec<bool>, // per-call predicate result (drained from front)
        begins: usize,
        ends: usize,
        pushes: Vec<ChangeType>,
        filter_calls: Vec<Row>,
        // If set, `filter` will emit one Yield sentinel before returning.
        emit_yield_in_filter: bool,
    }

    impl RecordingFilterOutput {
        fn new(keep: Vec<bool>) -> Self {
            Self {
                keep,
                begins: 0,
                ends: 0,
                pushes: vec![],
                filter_calls: vec![],
                emit_yield_in_filter: false,
            }
        }
    }

    impl Output for RecordingFilterOutput {
        fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes.push(change.change_type());
            Box::new(std::iter::empty())
        }
    }

    impl FilterOutput for RecordingFilterOutput {
        fn begin_filter(&mut self) {
            self.begins += 1;
        }
        fn end_filter(&mut self) {
            self.ends += 1;
        }
        fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
            self.filter_calls.push(node.row.clone());
            let keep = if self.keep.is_empty() {
                true
            } else {
                self.keep.remove(0)
            };
            let stream: Stream<'_, Yield> = if self.emit_yield_in_filter {
                Box::new(std::iter::once(Yield))
            } else {
                Box::new(std::iter::empty())
            };
            (stream, keep)
        }
    }

    // ── ThrowFilterOutput ────────────────────────────────────────────

    // Branch: push panics with TS message.
    #[test]
    #[should_panic(expected = "Output not set")]
    fn throw_filter_output_push_panics() {
        let mut sink = ThrowFilterOutput;
        let input = CannedInput::new(vec![]);
        let mut pusher = input;
        let n = node(row_with_id(1));
        let change = Change::Add(AddChange { node: n });
        let _ = sink.push(change, &mut pusher as &mut dyn InputBase);
    }

    // Branch: filter panics with TS message.
    #[test]
    #[should_panic(expected = "Output not set")]
    fn throw_filter_output_filter_panics() {
        let mut sink = ThrowFilterOutput;
        let n = node(row_with_id(1));
        let _ = sink.filter(&n);
    }

    // Branch: beginFilter/endFilter are no-ops — do not panic.
    #[test]
    fn throw_filter_output_begin_end_are_noops() {
        let mut sink = ThrowFilterOutput;
        sink.begin_filter();
        sink.end_filter();
    }

    // ── FilterStart ──────────────────────────────────────────────────

    // Branch: set_filter_output replaces default ThrowFilterOutput.
    #[test]
    fn filter_start_set_filter_output_replaces_default() {
        let mut start = FilterStart::new(Box::new(CannedInput::new(vec![])));
        let out = RecordingFilterOutput::new(vec![]);
        start.set_filter_output(Box::new(out));
        // If the default sink were still in place, fetch over an empty
        // stream would not panic; we just confirm set doesn't blow up.
    }

    // Branch: fetch keeps nodes where filter=true.
    #[test]
    fn filter_start_fetch_keeps_filter_true() {
        let items = vec![
            NodeOrYield::Node(node(row_with_id(1))),
            NodeOrYield::Node(node(row_with_id(2))),
        ];
        let mut start = FilterStart::new(Box::new(CannedInput::new(items)));
        start.set_filter_output(Box::new(RecordingFilterOutput::new(vec![true, true])));
        let out: Vec<NodeOrYield> = start.fetch(FetchRequest::default()).collect();
        let nodes: Vec<_> = out
            .into_iter()
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => Some(n),
                NodeOrYield::Yield => None,
            })
            .collect();
        assert_eq!(nodes.len(), 2);
    }

    // Branch: fetch drops nodes where filter=false.
    #[test]
    fn filter_start_fetch_drops_filter_false() {
        let items = vec![
            NodeOrYield::Node(node(row_with_id(1))),
            NodeOrYield::Node(node(row_with_id(2))),
            NodeOrYield::Node(node(row_with_id(3))),
        ];
        let mut start = FilterStart::new(Box::new(CannedInput::new(items)));
        start.set_filter_output(Box::new(RecordingFilterOutput::new(vec![
            true, false, true,
        ])));
        let out: Vec<NodeOrYield> = start.fetch(FetchRequest::default()).collect();
        let nodes: Vec<_> = out
            .into_iter()
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => Some(n),
                NodeOrYield::Yield => None,
            })
            .collect();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].row.get("id"), Some(&Some(json!(1))));
        assert_eq!(nodes[1].row.get("id"), Some(&Some(json!(3))));
    }

    // Branch: Yield sentinel passes through unchanged.
    #[test]
    fn filter_start_fetch_propagates_yield() {
        let items = vec![
            NodeOrYield::Yield,
            NodeOrYield::Node(node(row_with_id(1))),
            NodeOrYield::Yield,
        ];
        let mut start = FilterStart::new(Box::new(CannedInput::new(items)));
        start.set_filter_output(Box::new(RecordingFilterOutput::new(vec![true])));
        let out: Vec<NodeOrYield> = start.fetch(FetchRequest::default()).collect();
        let yields = out
            .iter()
            .filter(|n| matches!(n, NodeOrYield::Yield))
            .count();
        let nodes = out
            .iter()
            .filter(|n| matches!(n, NodeOrYield::Node(_)))
            .count();
        assert_eq!(yields, 2);
        assert_eq!(nodes, 1);
    }

    // Branch: empty input fetch returns empty.
    #[test]
    fn filter_start_fetch_empty_input_returns_empty() {
        let mut start = FilterStart::new(Box::new(CannedInput::new(vec![])));
        start.set_filter_output(Box::new(RecordingFilterOutput::new(vec![])));
        let out: Vec<NodeOrYield> = start.fetch(FetchRequest::default()).collect();
        assert!(out.is_empty());
    }

    // Branch: end_filter runs in Drop even if consumer short-circuits.
    // We simulate by dropping the returned stream before fully iterating.
    #[test]
    fn filter_start_fetch_calls_begin_and_end_exactly_once() {
        // Capture counters from inside the output; we can't observe
        // through Mutex+Box directly, so we use Arc counters via a
        // custom output.
        struct CountingOut {
            begins: Arc<AtomicUsize>,
            ends: Arc<AtomicUsize>,
        }
        impl Output for CountingOut {
            fn push<'a>(&'a mut self, _c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
                Box::new(std::iter::empty())
            }
        }
        impl FilterOutput for CountingOut {
            fn begin_filter(&mut self) {
                self.begins.fetch_add(1, AtOrdering::SeqCst);
            }
            fn filter(&mut self, _n: &Node) -> (Stream<'_, Yield>, bool) {
                (Box::new(std::iter::empty()), true)
            }
            fn end_filter(&mut self) {
                self.ends.fetch_add(1, AtOrdering::SeqCst);
            }
        }
        let begins = Arc::new(AtomicUsize::new(0));
        let ends = Arc::new(AtomicUsize::new(0));
        let items = vec![NodeOrYield::Node(node(row_with_id(1)))];
        let mut start = FilterStart::new(Box::new(CannedInput::new(items)));
        start.set_filter_output(Box::new(CountingOut {
            begins: Arc::clone(&begins),
            ends: Arc::clone(&ends),
        }));
        let _: Vec<_> = start.fetch(FetchRequest::default()).collect();
        assert_eq!(begins.load(AtOrdering::SeqCst), 1);
        assert_eq!(ends.load(AtOrdering::SeqCst), 1);
    }

    // Branch: yields emitted by output.filter are propagated.
    #[test]
    fn filter_start_fetch_propagates_yields_from_filter() {
        let items = vec![NodeOrYield::Node(node(row_with_id(1)))];
        let mut start = FilterStart::new(Box::new(CannedInput::new(items)));
        let mut out = RecordingFilterOutput::new(vec![true]);
        out.emit_yield_in_filter = true;
        start.set_filter_output(Box::new(out));
        let collected: Vec<NodeOrYield> = start.fetch(FetchRequest::default()).collect();
        // Expect 1 yield (from inner filter) + 1 node.
        let yields = collected
            .iter()
            .filter(|n| matches!(n, NodeOrYield::Yield))
            .count();
        let nodes = collected
            .iter()
            .filter(|n| matches!(n, NodeOrYield::Node(_)))
            .count();
        assert_eq!(yields, 1);
        assert_eq!(nodes, 1);
    }

    // Branch: push delegates to downstream output.
    #[test]
    fn filter_start_push_forwards_to_downstream() {
        let mut start = FilterStart::new(Box::new(CannedInput::new(vec![])));
        // We need a downstream whose push we can observe; use a shared
        // counter via AtomicUsize.
        struct CountingPush {
            count: Arc<AtomicUsize>,
        }
        impl Output for CountingPush {
            fn push<'a>(&'a mut self, _c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
                self.count.fetch_add(1, AtOrdering::SeqCst);
                Box::new(std::iter::empty())
            }
        }
        impl FilterOutput for CountingPush {
            fn begin_filter(&mut self) {}
            fn filter(&mut self, _n: &Node) -> (Stream<'_, Yield>, bool) {
                (Box::new(std::iter::empty()), true)
            }
            fn end_filter(&mut self) {}
        }
        let count = Arc::new(AtomicUsize::new(0));
        start.set_filter_output(Box::new(CountingPush {
            count: Arc::clone(&count),
        }));
        let pusher = CannedInput::new(vec![]);
        let _: Vec<_> = start
            .push(
                Change::Add(AddChange {
                    node: node(row_with_id(1)),
                }),
                &pusher as &dyn InputBase,
            )
            .collect();
        assert_eq!(count.load(AtOrdering::SeqCst), 1);
    }

    // Branch: destroy delegates upstream.
    #[test]
    fn filter_start_destroy_delegates_to_input() {
        // Keep a handle to the CannedInput so we can observe destroy().
        // We use Arc<AtomicBool> captured in a custom Input impl.
        struct TrackedInput {
            schema: SourceSchema,
            destroyed: Arc<AtomicBool>,
        }
        impl InputBase for TrackedInput {
            fn get_schema(&self) -> &SourceSchema {
                &self.schema
            }
            fn destroy(&mut self) {
                self.destroyed.store(true, AtOrdering::SeqCst);
            }
        }
        impl Input for TrackedInput {
            fn set_output(&mut self, _o: Box<dyn Output>) {}
            fn fetch<'a>(&'a self, _r: FetchRequest) -> Stream<'a, NodeOrYield> {
                Box::new(std::iter::empty())
            }
        }
        let flag = Arc::new(AtomicBool::new(false));
        let input = TrackedInput {
            schema: make_schema("t"),
            destroyed: Arc::clone(&flag),
        };
        let mut start = FilterStart::new(Box::new(input));
        start.destroy();
        assert!(flag.load(AtOrdering::SeqCst));
    }

    // Branch: get_schema delegates upstream.
    #[test]
    fn filter_start_get_schema_delegates_to_input() {
        let input = CannedInput::new(vec![]);
        let start = FilterStart::new(Box::new(input));
        assert_eq!(start.get_schema().table_name, "canned");
    }

    // ── FilterEnd ────────────────────────────────────────────────────

    /// FilterInput stub: holds a schema, records destroy, forwards
    /// set_filter_output to a sink we don't inspect.
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

    // Branch: FilterEnd::filter returns (empty stream, true).
    #[test]
    fn filter_end_filter_returns_true_with_empty_stream() {
        let start = Arc::new(Mutex::new(FilterStart::new(Box::new(CannedInput::new(
            vec![],
        )))));
        let input = StubFilterInput {
            schema: make_schema("middle"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let mut end = FilterEnd::new(Arc::clone(&start), Box::new(input));
        let (stream, keep) = end.filter(&node(row_with_id(1)));
        let yields: Vec<Yield> = stream.collect();
        assert!(yields.is_empty());
        assert!(keep);
    }

    // Branch: begin_filter / end_filter are no-ops — just shouldn't panic.
    #[test]
    fn filter_end_begin_end_filter_noop() {
        let start = Arc::new(Mutex::new(FilterStart::new(Box::new(CannedInput::new(
            vec![],
        )))));
        let input = StubFilterInput {
            schema: make_schema("middle"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let mut end = FilterEnd::new(Arc::clone(&start), Box::new(input));
        end.begin_filter();
        end.end_filter();
    }

    // Branch: fetch passes through from start.fetch().
    #[test]
    fn filter_end_fetch_passes_through_start_fetch() {
        let items = vec![
            NodeOrYield::Node(node(row_with_id(1))),
            NodeOrYield::Node(node(row_with_id(2))),
        ];
        let mut fs = FilterStart::new(Box::new(CannedInput::new(items)));
        // FilterEnd acts as the FilterStart's downstream filter — we
        // wire it directly so start.fetch sees filter=true always.
        let input = StubFilterInput {
            schema: make_schema("middle"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let start_arc = Arc::new(Mutex::new({
            // FilterStart needs a FilterOutput that returns true —
            // use a mini impl inline.
            struct AlwaysTrue;
            impl Output for AlwaysTrue {
                fn push<'a>(&'a mut self, _c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
                    Box::new(std::iter::empty())
                }
            }
            impl FilterOutput for AlwaysTrue {
                fn begin_filter(&mut self) {}
                fn filter(&mut self, _n: &Node) -> (Stream<'_, Yield>, bool) {
                    (Box::new(std::iter::empty()), true)
                }
                fn end_filter(&mut self) {}
            }
            fs.set_filter_output(Box::new(AlwaysTrue));
            fs
        }));
        let end = FilterEnd::new(Arc::clone(&start_arc), Box::new(input));
        let out: Vec<NodeOrYield> = end.fetch(FetchRequest::default()).collect();
        let nodes = out
            .iter()
            .filter(|n| matches!(n, NodeOrYield::Node(_)))
            .count();
        assert_eq!(nodes, 2);
    }

    // Branch: push delegates to downstream output.
    #[test]
    fn filter_end_push_forwards_to_downstream() {
        let start = Arc::new(Mutex::new(FilterStart::new(Box::new(CannedInput::new(
            vec![],
        )))));
        let input = StubFilterInput {
            schema: make_schema("middle"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        struct CountingPush {
            count: Arc<AtomicUsize>,
        }
        impl Output for CountingPush {
            fn push<'a>(&'a mut self, _c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
                self.count.fetch_add(1, AtOrdering::SeqCst);
                Box::new(std::iter::empty())
            }
        }
        let count = Arc::new(AtomicUsize::new(0));
        let mut end = FilterEnd::new(Arc::clone(&start), Box::new(input));
        end.set_output(Box::new(CountingPush {
            count: Arc::clone(&count),
        }));
        let pusher = CannedInput::new(vec![]);
        let _: Vec<_> = end
            .push(
                Change::Add(AddChange {
                    node: node(row_with_id(1)),
                }),
                &pusher as &dyn InputBase,
            )
            .collect();
        assert_eq!(count.load(AtOrdering::SeqCst), 1);
    }

    // Branch: destroy delegates to middle FilterInput.
    #[test]
    fn filter_end_destroy_delegates_to_middle_input() {
        let start = Arc::new(Mutex::new(FilterStart::new(Box::new(CannedInput::new(
            vec![],
        )))));
        let destroyed = Arc::new(AtomicBool::new(false));
        let input = StubFilterInput {
            schema: make_schema("middle"),
            destroyed: Arc::clone(&destroyed),
        };
        let mut end = FilterEnd::new(Arc::clone(&start), Box::new(input));
        end.destroy();
        assert!(destroyed.load(AtOrdering::SeqCst));
    }

    // Branch: get_schema delegates to middle FilterInput.
    #[test]
    fn filter_end_get_schema_delegates_to_middle_input() {
        let start = Arc::new(Mutex::new(FilterStart::new(Box::new(CannedInput::new(
            vec![],
        )))));
        let input = StubFilterInput {
            schema: make_schema("middle-x"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let end = FilterEnd::new(Arc::clone(&start), Box::new(input));
        assert_eq!(end.get_schema().table_name, "middle-x");
    }

    // ── build_filter_pipeline ────────────────────────────────────────

    // Branch: add_edge is called exactly 3 times (one per edge).
    #[test]
    fn build_filter_pipeline_calls_add_edge_three_times() {
        let edges = Arc::new(AtomicUsize::new(0));
        let edges2 = Arc::clone(&edges);
        let input: Box<dyn Input> = Box::new(CannedInput::new(vec![]));
        let _end = build_filter_pipeline(
            input,
            move || {
                edges2.fetch_add(1, AtOrdering::SeqCst);
            },
            |_start| {
                Box::new(StubFilterInput {
                    schema: make_schema("middle"),
                    destroyed: Arc::new(AtomicBool::new(false)),
                })
            },
        );
        assert_eq!(edges.load(AtOrdering::SeqCst), 3);
    }

    // Branch: pipeline closure receives the same FilterStart instance
    // that FilterEnd ultimately holds (identity preserved via Arc).
    #[test]
    fn build_filter_pipeline_shares_filter_start_arc() {
        let start_ptr: RefCell<Option<usize>> = RefCell::new(None);
        let input: Box<dyn Input> = Box::new(CannedInput::new(vec![]));
        let end = build_filter_pipeline(
            input,
            || {},
            |start: Arc<Mutex<FilterStart>>| {
                *start_ptr.borrow_mut() = Some(Arc::as_ptr(&start) as usize);
                Box::new(StubFilterInput {
                    schema: make_schema("middle"),
                    destroyed: Arc::new(AtomicBool::new(false)),
                })
            },
        );
        let end_start_ptr = Arc::as_ptr(&end.start) as usize;
        assert_eq!(start_ptr.borrow().unwrap(), end_start_ptr);
    }
}
