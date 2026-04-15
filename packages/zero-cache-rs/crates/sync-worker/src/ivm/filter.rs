//! Port of `packages/zql/src/ivm/filter.ts`.
//!
//! Public export ported:
//!
//! - [`Filter`] — the stateless [`FilterOperator`] that applies a pure
//!   row predicate.  Wraps a [`FilterInput`] upstream and a
//!   [`FilterOutput`] downstream; push/fetch/filter/cleanup all delegate.
//!
//! TS doc block preserved:
//! > The Filter operator filters data through a predicate. It is
//! > stateless.  The predicate must be pure.

use std::sync::{Arc, Mutex};

use crate::ivm::change::Change;
use crate::ivm::data::Node;
use crate::ivm::filter_operators::{FilterInput, FilterOperator, FilterOutput, ThrowFilterOutput};
use crate::ivm::filter_push::filter_push;
use crate::ivm::operator::{InputBase, Output};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

use zero_cache_types::value::Row;

/// Arc-based back-edge for [`Filter`]. Wires the back-pointer that TS
/// `input.setFilterOutput(this)` installs in the constructor, without
/// a Rust ownership cycle by holding a strong `Arc<Mutex<Filter>>` —
/// pipelines are short-lived so the strong cycle (child input → parent
/// Filter) is acceptable (the whole pipeline is destroyed together).
pub struct ArcFilterBackEdge(Arc<Mutex<Filter>>);

impl Output for ArcFilterBackEdge {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let mut guard = self.0.lock().expect("ArcFilterBackEdge mutex poisoned");
        let items: Vec<Yield> = guard.push(change, pusher).collect();
        drop(guard);
        Box::new(items.into_iter())
    }
}

impl FilterOutput for ArcFilterBackEdge {
    fn begin_filter(&mut self) {
        self.0.lock().expect("ArcFilterBackEdge mutex poisoned").begin_filter();
    }
    fn end_filter(&mut self) {
        self.0.lock().expect("ArcFilterBackEdge mutex poisoned").end_filter();
    }
    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        let mut guard = self.0.lock().expect("ArcFilterBackEdge mutex poisoned");
        let (stream, keep) = guard.filter(node);
        let collected: Vec<Yield> = stream.collect();
        drop(guard);
        (Box::new(collected.into_iter()), keep)
    }
}

/// Adapter passing an `Arc<Mutex<Filter>>` as `Box<dyn FilterInput>`
/// up the chain. Caches the schema at construction time so
/// `get_schema(&self) -> &SourceSchema` stays lifetime-clean.
pub struct ArcFilterAsInput {
    inner: Arc<Mutex<Filter>>,
    schema: SourceSchema,
}

impl ArcFilterAsInput {
    pub fn new(inner: Arc<Mutex<Filter>>) -> Self {
        let schema = inner.lock().unwrap().get_schema().clone();
        Self { inner, schema }
    }
}

impl InputBase for ArcFilterAsInput {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.inner.lock().unwrap().destroy();
    }
}

impl FilterInput for ArcFilterAsInput {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        self.inner.lock().unwrap().set_filter_output(output);
    }
}

impl Output for ArcFilterAsInput {
    fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let mut guard = self.inner.lock().unwrap();
        let items: Vec<Yield> = guard.push(change, pusher).collect();
        drop(guard);
        Box::new(items.into_iter())
    }
}

impl FilterOutput for ArcFilterAsInput {
    fn begin_filter(&mut self) {
        self.inner.lock().unwrap().begin_filter();
    }
    fn end_filter(&mut self) {
        self.inner.lock().unwrap().end_filter();
    }
    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        let mut guard = self.inner.lock().unwrap();
        let (stream, keep) = guard.filter(node);
        let collected: Vec<Yield> = stream.collect();
        drop(guard);
        (Box::new(collected.into_iter()), keep)
    }
}

impl FilterOperator for ArcFilterAsInput {}

/// TS `predicate: (row: Row) => boolean`. Shared-ownership boxed
/// closure so the [`Filter`] can hand the same predicate to
/// [`filter_push`] for the `push` path while still applying it in
/// `filter` on the `fetch` path.
pub type Predicate = Arc<dyn Fn(&Row) -> bool + Send + Sync>;

/// TS `Filter` — stateless predicate-based [`FilterOperator`].
pub struct Filter {
    input: Box<dyn FilterInput>,
    predicate: Predicate,
    output: Mutex<Box<dyn FilterOutput>>,
}

impl Filter {
    /// TS `new Filter(input, predicate)`.
    ///
    /// Matches TS which calls `input.setFilterOutput(this)` in the
    /// constructor. We use `Arc<Mutex<Self>>` + a strong back-edge
    /// adapter ([`ArcFilterBackEdge`]) to express the same cycle
    /// without smuggling self-refs out of the constructor. The
    /// resulting `Arc` is typically wrapped in [`ArcFilterAsInput`]
    /// by the builder so it can be passed up the chain as
    /// `Box<dyn FilterInput>`.
    pub fn new(input: Box<dyn FilterInput>, predicate: Predicate) -> Arc<Mutex<Self>> {
        let filter = Arc::new(Mutex::new(Self {
            input,
            predicate,
            output: Mutex::new(Box::new(ThrowFilterOutput)),
        }));
        // Wire the back-edge. Strong-cycle: child input retains an
        // Arc to the parent Filter. Pipelines are destroyed together
        // (driver.reset / remove_query), so the cycle is bounded.
        let back: Box<dyn FilterOutput> = Box::new(ArcFilterBackEdge(Arc::clone(&filter)));
        filter.lock().unwrap().input.set_filter_output(back);
        filter
    }
}

impl InputBase for Filter {
    fn get_schema(&self) -> &SourceSchema {
        // TS: `return this.#input.getSchema();`
        self.input.get_schema()
    }

    fn destroy(&mut self) {
        // TS: `this.#input.destroy();`
        self.input.destroy();
    }
}

impl FilterInput for Filter {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        *self.output.lock().expect("filter output mutex poisoned") = output;
    }
}

impl FilterOutput for Filter {
    fn begin_filter(&mut self) {
        // TS: `this.#output.beginFilter();`
        let mut o = self.output.lock().expect("filter output mutex poisoned");
        o.begin_filter();
    }

    fn end_filter(&mut self) {
        // TS: `this.#output.endFilter();`
        let mut o = self.output.lock().expect("filter output mutex poisoned");
        o.end_filter();
    }

    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        // TS: `return this.#predicate(node.row) && (yield* this.#output.filter(node));`
        //
        // Short-circuit: if our predicate returns false, we return
        // `(empty, false)` immediately — TS `&&` short-circuits and
        // never consults `this.#output.filter`.
        //
        // If our predicate returns true, the result is whatever the
        // downstream filter returns (including any yields it emits).
        if !(self.predicate)(&node.row) {
            return (Box::new(std::iter::empty()), false);
        }
        let mut o = self.output.lock().expect("filter output mutex poisoned");
        // Collect eagerly so the lock is released before returning.
        // This preserves TS `yield*` semantics (all yields from the
        // inner filter are propagated in order).
        let (stream, keep) = o.filter(node);
        let yields: Vec<Yield> = stream.collect();
        (Box::new(yields.into_iter()), keep)
    }
}

impl Output for Filter {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        // TS: `yield* filterPush(change, this.#output, this, this.#predicate);`
        //
        // filter_push needs `&mut dyn Output` for the downstream and
        // `&dyn InputBase` for the pusher identity.  We clone the
        // `Arc<Predicate>` so filter_push owns a reference to the same
        // closure.
        //
        // We collect eagerly; the underlying push sites in
        // filter_push do the same.  The eager collection releases the
        // output mutex before returning the stream.
        let predicate = Arc::clone(&self.predicate);
        let mut out = self.output.lock().expect("filter output mutex poisoned");
        // As pusher we pass the upstream FilterInput — same reasoning
        // as FilterStart: we cannot reborrow `self` as `&dyn InputBase`
        // while `self` is mutably borrowed.
        let pusher: &dyn InputBase = &*self.input;
        let mut items: Vec<Yield> = Vec::new();
        for y in filter_push(change, &mut **out, pusher, Some(predicate.as_ref())) {
            items.push(y);
        }
        Box::new(items.into_iter())
    }
}

impl FilterOperator for Filter {}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!   - `new` installs `ThrowFilterOutput` by default.
    //!   - `set_filter_output` replaces default.
    //!   - `filter` — predicate=false short-circuits (downstream NOT called).
    //!   - `filter` — predicate=true, downstream filter=true.
    //!   - `filter` — predicate=true, downstream filter=false.
    //!   - `filter` — propagates yields emitted by downstream.
    //!   - `begin_filter` / `end_filter` — delegate to downstream.
    //!   - `push` — delegates to [`filter_push`] (verified via a
    //!     recording downstream).
    //!   - `destroy` / `get_schema` — delegate to upstream FilterInput.

    use super::*;
    use crate::ivm::change::{AddChange, ChangeType, EditChange};
    use crate::ivm::data::make_comparator;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrdering};
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;

    fn make_schema(name: &str) -> SourceSchema {
        let sort: Ordering = vec![("id".to_string(), Direction::Asc)];
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

    fn row_id_and_flag(id: i64, flag: bool) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("flag".into(), Some(json!(flag)));
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

    /// Downstream that records every interaction.
    struct Recorder {
        filter_calls: Vec<Row>,
        filter_results: Vec<bool>, // programmable (drained from front)
        yields_in_filter: usize,
        pushes: Vec<ChangeType>,
        begins: usize,
        ends: usize,
    }
    impl Output for Recorder {
        fn push<'a>(&'a mut self, change: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes.push(change.change_type());
            Box::new(std::iter::empty())
        }
    }
    impl FilterOutput for Recorder {
        fn begin_filter(&mut self) {
            self.begins += 1;
        }
        fn end_filter(&mut self) {
            self.ends += 1;
        }
        fn filter(&mut self, n: &Node) -> (Stream<'_, Yield>, bool) {
            self.filter_calls.push(n.row.clone());
            let keep = if self.filter_results.is_empty() {
                true
            } else {
                self.filter_results.remove(0)
            };
            let yields = self.yields_in_filter;
            (Box::new((0..yields).map(|_| Yield)), keep)
        }
    }

    /// Returns a `LockedFilter` helper so existing tests that call
    /// `f.set_filter_output(…)`, `f.filter(…)`, `f.push(…)`, `f.destroy()`,
    /// `f.get_schema()` keep working via `&mut` Deref — we just lock
    /// the Mutex around the returned `Arc<Mutex<Filter>>`.
    fn mk_filter(
        pred: impl Fn(&Row) -> bool + Send + Sync + 'static,
    ) -> (LockedFilter, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        let input = StubFilterInput {
            schema: make_schema("t"),
            destroyed: Arc::clone(&flag),
        };
        let pred: Predicate = Arc::new(pred);
        (LockedFilter(Filter::new(Box::new(input), pred)), flag)
    }

    /// Test-only wrapper that derefs to `Filter` via Mutex so the
    /// existing `(mut f, _) = mk_filter(...)` call shape compiles.
    struct LockedFilter(Arc<Mutex<Filter>>);
    impl std::ops::Deref for LockedFilter {
        type Target = Mutex<Filter>;
        fn deref(&self) -> &Self::Target { &self.0 }
    }
    impl LockedFilter {
        fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
            self.0.lock().unwrap().set_filter_output(output);
        }
        fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
            let mut guard = self.0.lock().unwrap();
            let (stream, keep) = guard.filter(node);
            let items: Vec<Yield> = stream.collect();
            drop(guard);
            (Box::new(items.into_iter()), keep)
        }
        fn push<'a>(&'a mut self, change: Change, pusher: &dyn InputBase) -> Stream<'a, Yield> {
            let mut guard = self.0.lock().unwrap();
            let items: Vec<Yield> = guard.push(change, pusher).collect();
            drop(guard);
            Box::new(items.into_iter())
        }
        fn destroy(&mut self) { self.0.lock().unwrap().destroy(); }
        fn begin_filter(&mut self) { self.0.lock().unwrap().begin_filter(); }
        fn end_filter(&mut self) { self.0.lock().unwrap().end_filter(); }
        fn get_schema(&self) -> SourceSchema {
            self.0.lock().unwrap().get_schema().clone()
        }
    }

    // Branch: predicate=false → short-circuit; downstream.filter NOT called.
    #[test]
    fn filter_filter_predicate_false_short_circuits() {
        let (mut f, _) = mk_filter(|_row| false);
        let rec = Recorder {
            filter_calls: vec![],
            filter_results: vec![true],
            yields_in_filter: 0,
            pushes: vec![],
            begins: 0,
            ends: 0,
        };
        f.set_filter_output(Box::new(rec));
        let (stream, keep) = f.filter(&node_of(row_id_and_flag(1, true)));
        let yields: Vec<Yield> = stream.collect();
        assert!(!keep);
        assert!(yields.is_empty());
        // Unable to observe Recorder through Mutex<Box<dyn ...>> —
        // cover this branch indirectly by: the false result must have
        // come from our predicate, since Recorder would have returned
        // true.
    }

    // Branch: predicate=true, downstream filter=true.
    #[test]
    fn filter_filter_predicate_true_downstream_true() {
        let (mut f, _) = mk_filter(|_row| true);
        let rec = Recorder {
            filter_calls: vec![],
            filter_results: vec![true],
            yields_in_filter: 0,
            pushes: vec![],
            begins: 0,
            ends: 0,
        };
        f.set_filter_output(Box::new(rec));
        let (_stream, keep) = f.filter(&node_of(row_id_and_flag(1, true)));
        assert!(keep);
    }

    // Branch: predicate=true, downstream filter=false.
    #[test]
    fn filter_filter_predicate_true_downstream_false() {
        let (mut f, _) = mk_filter(|_row| true);
        let rec = Recorder {
            filter_calls: vec![],
            filter_results: vec![false],
            yields_in_filter: 0,
            pushes: vec![],
            begins: 0,
            ends: 0,
        };
        f.set_filter_output(Box::new(rec));
        let (_stream, keep) = f.filter(&node_of(row_id_and_flag(1, true)));
        assert!(!keep);
    }

    // Branch: downstream yields propagate when predicate=true.
    #[test]
    fn filter_filter_propagates_downstream_yields() {
        let (mut f, _) = mk_filter(|_row| true);
        let rec = Recorder {
            filter_calls: vec![],
            filter_results: vec![true],
            yields_in_filter: 2,
            pushes: vec![],
            begins: 0,
            ends: 0,
        };
        f.set_filter_output(Box::new(rec));
        let (stream, _keep) = f.filter(&node_of(row_id_and_flag(1, true)));
        assert_eq!(stream.count(), 2);
    }

    // Branch: begin_filter / end_filter delegate downstream. Observable
    // via custom output capturing Arc counters.
    #[test]
    fn filter_begin_end_delegate() {
        struct Counting {
            begins: Arc<AtomicUsize>,
            ends: Arc<AtomicUsize>,
        }
        impl Output for Counting {
            fn push<'a>(&'a mut self, _c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
                Box::new(std::iter::empty())
            }
        }
        impl FilterOutput for Counting {
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
        let (mut f, _) = mk_filter(|_r| true);
        f.set_filter_output(Box::new(Counting {
            begins: Arc::clone(&begins),
            ends: Arc::clone(&ends),
        }));
        f.begin_filter();
        f.end_filter();
        assert_eq!(begins.load(AtOrdering::SeqCst), 1);
        assert_eq!(ends.load(AtOrdering::SeqCst), 1);
    }

    // Branch: push with predicate=true forwards Add.
    #[test]
    fn filter_push_add_forwarded_when_predicate_true() {
        struct CountingPush {
            count: Arc<AtomicUsize>,
            last: Mutex<Option<ChangeType>>,
        }
        impl Output for CountingPush {
            fn push<'a>(&'a mut self, c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
                *self.last.lock().unwrap() = Some(c.change_type());
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
        let (mut f, _) = mk_filter(|_r| true);
        f.set_filter_output(Box::new(CountingPush {
            count: Arc::clone(&count),
            last: Mutex::new(None),
        }));
        let pusher_input = StubFilterInput {
            schema: make_schema("u"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let _: Vec<_> = f
            .push(
                Change::Add(AddChange {
                    node: node_of(row_id_and_flag(1, true)),
                }),
                &pusher_input as &dyn InputBase,
            )
            .collect();
        assert_eq!(count.load(AtOrdering::SeqCst), 1);
    }

    // Branch: push with predicate=false drops Add.
    #[test]
    fn filter_push_add_dropped_when_predicate_false() {
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
        let (mut f, _) = mk_filter(|_r| false);
        f.set_filter_output(Box::new(CountingPush {
            count: Arc::clone(&count),
        }));
        let pusher_input = StubFilterInput {
            schema: make_schema("u"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let _: Vec<_> = f
            .push(
                Change::Add(AddChange {
                    node: node_of(row_id_and_flag(1, false)),
                }),
                &pusher_input as &dyn InputBase,
            )
            .collect();
        assert_eq!(count.load(AtOrdering::SeqCst), 0);
    }

    // Branch: push with Edit — routes through maybe_split_and_push_edit_change
    // via filter_push.  Cover the Edit→Remove branch.
    #[test]
    fn filter_push_edit_old_true_new_false_routes_remove() {
        struct CountingPush {
            seen: Arc<Mutex<Vec<ChangeType>>>,
        }
        impl Output for CountingPush {
            fn push<'a>(&'a mut self, c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
                self.seen.lock().unwrap().push(c.change_type());
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
        let seen = Arc::new(Mutex::new(Vec::<ChangeType>::new()));
        // Predicate: flag == true
        let (mut f, _) =
            mk_filter(|row: &Row| matches!(row.get("flag"), Some(Some(v)) if v == &json!(true)));
        f.set_filter_output(Box::new(CountingPush {
            seen: Arc::clone(&seen),
        }));
        let pusher_input = StubFilterInput {
            schema: make_schema("u"),
            destroyed: Arc::new(AtomicBool::new(false)),
        };
        let _: Vec<_> = f
            .push(
                Change::Edit(EditChange {
                    old_node: node_of(row_id_and_flag(1, true)),
                    node: node_of(row_id_and_flag(1, false)),
                }),
                &pusher_input as &dyn InputBase,
            )
            .collect();
        let s = seen.lock().unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0], ChangeType::Remove);
    }

    // Branch: destroy delegates to upstream FilterInput.
    #[test]
    fn filter_destroy_delegates() {
        let (mut f, flag) = mk_filter(|_r| true);
        f.destroy();
        assert!(flag.load(AtOrdering::SeqCst));
    }

    // Branch: get_schema delegates to upstream FilterInput.
    #[test]
    fn filter_get_schema_delegates() {
        let (f, _) = mk_filter(|_r| true);
        assert_eq!(f.get_schema().table_name, "t");
    }

    // Branch: new() installs ThrowFilterOutput (filter panics if predicate=true).
    #[test]
    #[should_panic(expected = "Output not set")]
    fn filter_new_default_filter_output_throws_on_use() {
        let (mut f, _) = mk_filter(|_r| true);
        // predicate=true path reaches downstream filter which is
        // ThrowFilterOutput → panic.
        let _ = f.filter(&node_of(row_id_and_flag(1, true)));
    }
}
