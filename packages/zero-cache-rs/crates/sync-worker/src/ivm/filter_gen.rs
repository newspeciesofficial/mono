//! genawaiter pilot: same semantics as `filter.rs`, but the push and
//! filter paths are written using `gen!` / `yield_!` so the body shape
//! matches TS `function*` / `yield*` 1:1.
//!
//! ## What this proves
//!
//! - genawaiter integrates cleanly with our existing `Output` /
//!   `FilterOutput` traits — the `GenBoxed<Yield>` returned by a
//!   generator satisfies `Stream<'a, Yield>` (which is
//!   `Box<dyn Iterator<Item=Yield> + 'a>`), because `GenBoxed` implements
//!   `Iterator` and is `'static` from owned/`Arc` captures.
//! - `Arc<Self>` capture works for the operator-as-state pattern.
//! - Drop on early break runs cleanly (smoke-tested in /tmp/genawaiter-smoke).
//!
//! ## What this does NOT prove (and why)
//!
//! Filter is a stateless single-delegate operator. Its push body is just
//! `filter_push(change, output, …)` — there is nothing iterative inside
//! to lazily yield. So the genawaiter version of Filter is essentially
//! a wrapping of the same eager call, with prettier syntax. The real
//! laziness wins show up in **multi-step push** operators (Take's
//! Remove+Add, Join's per-parent ChildChange loop). Use this pilot as
//! integration proof, not as a perf demo.
//!
//! ## Public surface
//!
//! `FilterGen` implements all the same traits as `Filter`
//! (`InputBase`, `FilterInput`, `FilterOutput`, `Output`,
//! `FilterOperator`) and is constructed the same way
//! (`FilterGen::new(input, predicate) -> Arc<Self>`), so it could be
//! drop-in if/when we decide to swap.
//!
//! ## Caveats discovered
//!
//! - `GenBoxed<T>` is `Send` but **not `Sync`** (its inner `dyn Future`
//!   carries only `Send`). For our use this is fine: generators are
//!   consumed sequentially by one downstream and never shared by `&ref`
//!   across threads.
//! - `genawaiter` requires `default-features = false, features =
//!   ["proc_macro"]` to keep the dep tree minimal.

use std::sync::{Arc, Mutex};

use genawaiter::sync::{Gen, GenBoxed};

use crate::ivm::change::Change;
use crate::ivm::data::Node;
use crate::ivm::filter::Predicate;
use crate::ivm::filter_operators::{FilterInput, FilterOperator, FilterOutput, ThrowFilterOutput};
use crate::ivm::filter_push::filter_push;
use crate::ivm::operator::{InputBase, Output};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

/// genawaiter port of `Filter`. Same shape as `filter::Filter`.
pub struct FilterGen {
    input: Mutex<Box<dyn FilterInput>>,
    predicate: Predicate,
    output: Mutex<Box<dyn FilterOutput>>,
    schema: SourceSchema,
}

impl FilterGen {
    pub fn new(input: Box<dyn FilterInput>, predicate: Predicate) -> Arc<Self> {
        let schema = input.get_schema().clone();
        let me = Arc::new(Self {
            input: Mutex::new(input),
            predicate,
            output: Mutex::new(Box::new(ThrowFilterOutput)),
            schema,
        });
        let back: Box<dyn FilterOutput> = Box::new(BackEdge(Arc::clone(&me)));
        me.input
            .lock()
            .expect("filter_gen input mutex poisoned")
            .set_filter_output(back);
        me
    }

    /// TS `*push(change) { yield* filterPush(change, this.#output, this, this.#predicate); }`
    ///
    /// Mechanical translation: the `yield*` becomes `for y in inner { co.yield_(y).await; }`.
    /// We collect from `filter_push` while the output lock is held — the
    /// stream borrows `output` mutably, so we can't release the lock
    /// before draining. This is the same constraint the Vec-based
    /// implementation has; genawaiter doesn't relax it.
    pub fn push_arc(self: Arc<Self>, change: Change) -> GenBoxed<Yield> {
        Gen::new_boxed(move |co| async move {
            let predicate = Arc::clone(&self.predicate);
            // Collect downstream yields under the lock (current trait
            // shape requires it). Hand them out of the generator one
            // at a time. This is identical to the Vec version's perf
            // profile but keeps the body shape readable.
            let collected: Vec<Yield> = {
                let mut output_g = self
                    .output
                    .lock()
                    .expect("filter_gen output mutex poisoned");
                filter_push(
                    change,
                    &mut **output_g,
                    self.as_ref() as &dyn InputBase,
                    Some(predicate.as_ref()),
                )
                .collect()
            };
            for y in collected {
                co.yield_(y).await;
            }
        })
    }

    /// TS `*filter(node) { return predicate(node.row) && (yield* output.filter(node)); }`
    pub fn filter_arc(self: Arc<Self>, node_row: zero_cache_types::value::Row) -> (GenBoxed<Yield>, bool) {
        // Predicate must be evaluated synchronously to know `keep`. Same
        // as the Vec version. The yields come from downstream's `filter`
        // call.
        if !(self.predicate)(&node_row) {
            return (
                Gen::new_boxed(|_co| async move { /* empty */ }),
                false,
            );
        }
        // Build the node we'll pass downstream. We took the row by value
        // because Node is not `Clone` here in our usage pattern; downstream
        // `filter` borrows `&Node`.
        let node = Node {
            row: node_row,
            relationships: indexmap::IndexMap::new(),
        };
        // Collect yields and `keep` under the lock.
        let (yields, keep) = {
            let mut output_g = self
                .output
                .lock()
                .expect("filter_gen output mutex poisoned");
            let (stream, keep) = output_g.filter(&node);
            let yields: Vec<Yield> = stream.collect();
            (yields, keep)
        };
        let g: GenBoxed<Yield> = Gen::new_boxed(move |co| async move {
            for y in yields {
                co.yield_(y).await;
            }
        });
        (g, keep)
    }

    pub fn begin_filter_arc(&self) {
        self.output
            .lock()
            .expect("filter_gen output mutex poisoned")
            .begin_filter();
    }
    pub fn end_filter_arc(&self) {
        self.output
            .lock()
            .expect("filter_gen output mutex poisoned")
            .end_filter();
    }
    pub fn set_filter_output_arc(&self, output: Box<dyn FilterOutput>) {
        *self
            .output
            .lock()
            .expect("filter_gen output mutex poisoned") = output;
    }
    pub fn destroy_arc(&self) {
        self.input
            .lock()
            .expect("filter_gen input mutex poisoned")
            .destroy();
    }
}

impl InputBase for FilterGen {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.input
            .lock()
            .expect("filter_gen input mutex poisoned")
            .destroy();
    }
}

impl FilterInput for FilterGen {
    fn set_filter_output(&mut self, output: Box<dyn FilterOutput>) {
        *self
            .output
            .lock()
            .expect("filter_gen output mutex poisoned") = output;
    }
}

impl Output for FilterGen {
    fn push<'a>(
        &'a mut self,
        change: Change,
        _pusher: &dyn InputBase,
    ) -> Stream<'a, Yield> {
        // The `&mut self` shape can't reach the Arc<Self>-based
        // generator API. Eagerly drain; this trait-based path is for
        // back-compat with non-Arc callers only.
        let predicate = Arc::clone(&self.predicate);
        let mut output_g = self
            .output
            .lock()
            .expect("filter_gen output mutex poisoned");
        let v: Vec<Yield> = filter_push(
            change,
            &mut **output_g,
            self as &dyn InputBase,
            Some(predicate.as_ref()),
        )
        .collect();
        Box::new(v.into_iter())
    }
}

impl FilterOutput for FilterGen {
    fn begin_filter(&mut self) {
        self.output
            .lock()
            .expect("filter_gen output mutex poisoned")
            .begin_filter();
    }
    fn end_filter(&mut self) {
        self.output
            .lock()
            .expect("filter_gen output mutex poisoned")
            .end_filter();
    }
    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        if !(self.predicate)(&node.row) {
            return (Box::new(std::iter::empty()), false);
        }
        let mut output_g = self
            .output
            .lock()
            .expect("filter_gen output mutex poisoned");
        let (stream, keep) = output_g.filter(node);
        let yields: Vec<Yield> = stream.collect();
        (Box::new(yields.into_iter()), keep)
    }
}

impl FilterOperator for FilterGen {}

/// Back-edge from upstream input to FilterGen, mirroring TS
/// `input.setFilterOutput(this)` in the constructor.
struct BackEdge(Arc<FilterGen>);

impl Output for BackEdge {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        let g = Arc::clone(&self.0).push_arc(change);
        Box::new(g.into_iter())
    }
}
impl FilterOutput for BackEdge {
    fn begin_filter(&mut self) {
        self.0.begin_filter_arc();
    }
    fn end_filter(&mut self) {
        self.0.end_filter_arc();
    }
    fn filter(&mut self, node: &Node) -> (Stream<'_, Yield>, bool) {
        let (g, keep) = Arc::clone(&self.0).filter_arc(node.row.clone());
        (Box::new(g.into_iter()), keep)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Mirror of filter.rs's test suite. Goal: prove `FilterGen` is
    //! observably indistinguishable from `Filter`.

    use super::*;
    use crate::ivm::change::{AddChange, ChangeType, EditChange, RemoveChange};
    use crate::ivm::data::make_comparator;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrdering};
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Row;

    fn schema(name: &str) -> SourceSchema {
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

    fn row(id: i64, flag: bool) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r.insert("flag".into(), Some(json!(flag)));
        r
    }

    fn node(r: Row) -> Node {
        Node {
            row: r,
            relationships: IndexMap::new(),
        }
    }

    struct StubInput {
        s: SourceSchema,
        destroyed: Arc<AtomicBool>,
    }
    impl InputBase for StubInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.s
        }
        fn destroy(&mut self) {
            self.destroyed.store(true, AtOrdering::SeqCst);
        }
    }
    impl FilterInput for StubInput {
        fn set_filter_output(&mut self, _o: Box<dyn FilterOutput>) {}
    }

    fn mk(pred: impl Fn(&Row) -> bool + Send + Sync + 'static) -> (Arc<FilterGen>, Arc<AtomicBool>) {
        let dest = Arc::new(AtomicBool::new(false));
        let input = StubInput {
            s: schema("t"),
            destroyed: Arc::clone(&dest),
        };
        let p: Predicate = Arc::new(pred);
        (FilterGen::new(Box::new(input), p), dest)
    }

    struct CountingPush {
        count: Arc<AtomicUsize>,
        seen: Arc<Mutex<Vec<ChangeType>>>,
    }
    impl Output for CountingPush {
        fn push<'a>(&'a mut self, c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.seen.lock().unwrap().push(c.change_type());
            self.count.fetch_add(1, AtOrdering::SeqCst);
            Box::new(std::iter::empty())
        }
    }
    impl FilterOutput for CountingPush {
        fn begin_filter(&mut self) {}
        fn end_filter(&mut self) {}
        fn filter(&mut self, _n: &Node) -> (Stream<'_, Yield>, bool) {
            (Box::new(std::iter::empty()), true)
        }
    }

    // ─── Push branches ────────────────────────────────────────────────

    #[test]
    fn push_add_predicate_true_forwards() {
        let (f, _) = mk(|_r| true);
        let count = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        f.set_filter_output_arc(Box::new(CountingPush {
            count: Arc::clone(&count),
            seen: Arc::clone(&seen),
        }));
        let g = Arc::clone(&f).push_arc(Change::Add(AddChange {
            node: node(row(1, true)),
        }));
        let _: Vec<Yield> = g.into_iter().collect();
        assert_eq!(count.load(AtOrdering::SeqCst), 1);
        assert_eq!(seen.lock().unwrap()[0], ChangeType::Add);
    }

    #[test]
    fn push_add_predicate_false_drops() {
        let (f, _) = mk(|_r| false);
        let count = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        f.set_filter_output_arc(Box::new(CountingPush {
            count: Arc::clone(&count),
            seen: Arc::clone(&seen),
        }));
        let g = Arc::clone(&f).push_arc(Change::Add(AddChange {
            node: node(row(1, false)),
        }));
        let _: Vec<Yield> = g.into_iter().collect();
        assert_eq!(count.load(AtOrdering::SeqCst), 0);
    }

    #[test]
    fn push_remove_predicate_true_forwards() {
        let (f, _) = mk(|_r| true);
        let count = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        f.set_filter_output_arc(Box::new(CountingPush {
            count: Arc::clone(&count),
            seen: Arc::clone(&seen),
        }));
        let g = Arc::clone(&f).push_arc(Change::Remove(RemoveChange {
            node: node(row(1, true)),
        }));
        let _: Vec<Yield> = g.into_iter().collect();
        assert_eq!(count.load(AtOrdering::SeqCst), 1);
        assert_eq!(seen.lock().unwrap()[0], ChangeType::Remove);
    }

    #[test]
    fn push_edit_old_true_new_false_routes_remove() {
        let pred = |r: &Row| matches!(r.get("flag"), Some(Some(v)) if v == &json!(true));
        let (f, _) = mk(pred);
        let count = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        f.set_filter_output_arc(Box::new(CountingPush {
            count: Arc::clone(&count),
            seen: Arc::clone(&seen),
        }));
        let g = Arc::clone(&f).push_arc(Change::Edit(EditChange {
            old_node: node(row(1, true)),
            node: node(row(1, false)),
        }));
        let _: Vec<Yield> = g.into_iter().collect();
        let s = seen.lock().unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0], ChangeType::Remove);
    }

    #[test]
    fn push_edit_old_false_new_true_routes_add() {
        let pred = |r: &Row| matches!(r.get("flag"), Some(Some(v)) if v == &json!(true));
        let (f, _) = mk(pred);
        let count = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        f.set_filter_output_arc(Box::new(CountingPush {
            count: Arc::clone(&count),
            seen: Arc::clone(&seen),
        }));
        let g = Arc::clone(&f).push_arc(Change::Edit(EditChange {
            old_node: node(row(1, false)),
            node: node(row(1, true)),
        }));
        let _: Vec<Yield> = g.into_iter().collect();
        let s = seen.lock().unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0], ChangeType::Add);
    }

    // ─── Filter branches ─────────────────────────────────────────────

    #[test]
    fn filter_predicate_false_short_circuits() {
        let (f, _) = mk(|_r| false);
        let (g, keep) = Arc::clone(&f).filter_arc(row(1, true));
        let v: Vec<Yield> = g.into_iter().collect();
        assert!(!keep);
        assert!(v.is_empty());
    }

    #[test]
    fn filter_predicate_true_downstream_true() {
        let (f, _) = mk(|_r| true);
        let count = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(vec![]));
        f.set_filter_output_arc(Box::new(CountingPush {
            count: Arc::clone(&count),
            seen: Arc::clone(&seen),
        }));
        let (_g, keep) = Arc::clone(&f).filter_arc(row(1, true));
        assert!(keep);
    }

    #[test]
    fn filter_predicate_true_downstream_yields_propagate() {
        struct Yielding;
        impl Output for Yielding {
            fn push<'a>(&'a mut self, _c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
                Box::new(std::iter::empty())
            }
        }
        impl FilterOutput for Yielding {
            fn begin_filter(&mut self) {}
            fn end_filter(&mut self) {}
            fn filter(&mut self, _n: &Node) -> (Stream<'_, Yield>, bool) {
                (Box::new((0..3).map(|_| Yield)), true)
            }
        }
        let (f, _) = mk(|_r| true);
        f.set_filter_output_arc(Box::new(Yielding));
        let (g, keep) = Arc::clone(&f).filter_arc(row(1, true));
        assert!(keep);
        let v: Vec<Yield> = g.into_iter().collect();
        assert_eq!(v.len(), 3);
    }

    // ─── Lifecycle ────────────────────────────────────────────────────

    #[test]
    fn destroy_delegates() {
        let (f, dest) = mk(|_r| true);
        f.destroy_arc();
        assert!(dest.load(AtOrdering::SeqCst));
    }

    #[test]
    fn get_schema_returns_cached() {
        let (f, _) = mk(|_r| true);
        assert_eq!(f.get_schema().table_name, "t");
    }

    #[test]
    #[should_panic(expected = "Output not set")]
    fn new_default_throws_on_filter() {
        let (f, _) = mk(|_r| true);
        // Predicate true → reaches downstream filter (ThrowFilterOutput) → panic.
        let _ = Arc::clone(&f).filter_arc(row(1, true));
    }

    // ─── Begin/end delegation ────────────────────────────────────────

    #[test]
    fn begin_end_delegate() {
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
            fn end_filter(&mut self) {
                self.ends.fetch_add(1, AtOrdering::SeqCst);
            }
            fn filter(&mut self, _n: &Node) -> (Stream<'_, Yield>, bool) {
                (Box::new(std::iter::empty()), true)
            }
        }
        let begins = Arc::new(AtomicUsize::new(0));
        let ends = Arc::new(AtomicUsize::new(0));
        let (f, _) = mk(|_r| true);
        f.set_filter_output_arc(Box::new(Counting {
            begins: Arc::clone(&begins),
            ends: Arc::clone(&ends),
        }));
        f.begin_filter_arc();
        f.end_filter_arc();
        assert_eq!(begins.load(AtOrdering::SeqCst), 1);
        assert_eq!(ends.load(AtOrdering::SeqCst), 1);
    }
}
