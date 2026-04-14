//! Port of `packages/zql/src/ivm/union-fan-out.ts`.
//!
//! Public exports ported:
//!
//! - [`UnionFanOut`] — the `Operator` that forks one upstream into N
//!   downstream branches while preserving set semantics (no duplicates
//!   across the branches). Paired with a [`super::union_fan_in::UnionFanIn`]
//!   that merges the branches.
//!
//! ## Ownership divergence from TS
//!
//! Same pattern as [`super::fan_out::FanOut`]: the TS ctor wires
//! `input.setOutput(this)`; we leave that back-edge to the pipeline
//! driver and expose [`UnionFanOut::push`] / [`UnionFanOut::fetch`] as
//! entry points. The fan-in back-reference is stored as
//! [`Option<Arc<Mutex<UnionFanIn>>>`] so it can also be reached from
//! the branches' back-edges.

use std::sync::{Arc, Mutex};

use crate::ivm::change::Change;
use crate::ivm::data::NodeOrYield;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Operator, Output};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;
use crate::ivm::union_fan_in::UnionFanIn;

/// TS `UnionFanOut`.
pub struct UnionFanOut {
    input: Box<dyn Input>,
    outputs: Mutex<Vec<Box<dyn Output>>>,
    union_fan_in: Mutex<Option<Arc<Mutex<UnionFanIn>>>>,
    destroy_count: Mutex<usize>,
}

impl UnionFanOut {
    /// TS `new UnionFanOut(input)`.
    ///
    /// Does NOT call `input.setOutput(this)`; see module doc.
    pub fn new(input: Box<dyn Input>) -> Self {
        Self {
            input,
            outputs: Mutex::new(Vec::new()),
            union_fan_in: Mutex::new(None),
            destroy_count: Mutex::new(0),
        }
    }

    /// TS `setFanIn(fanIn)`.
    ///
    /// TS asserts that `#unionFanIn` is unset before setting. We enforce
    /// the same invariant.
    pub fn set_fan_in(&self, fan_in: Arc<Mutex<UnionFanIn>>) {
        let mut guard = self
            .union_fan_in
            .lock()
            .expect("union_fan_out fan_in mutex poisoned");
        assert!(guard.is_none(), "FanIn already set for this FanOut");
        *guard = Some(fan_in);
    }

    /// TS `destroy()`. Same count-based semantics as [`super::fan_out::FanOut`].
    pub fn destroy_downstream(&mut self) {
        let n_outputs = self
            .outputs
            .lock()
            .expect("union_fan_out outputs mutex poisoned")
            .len();
        let mut count = self
            .destroy_count
            .lock()
            .expect("union_fan_out destroy_count mutex poisoned");
        if *count < n_outputs {
            *count += 1;
            if *count == n_outputs {
                drop(count);
                self.input.destroy();
            }
        } else {
            // TS: `throw new Error('FanOut already destroyed once for each output')`.
            panic!("FanOut already destroyed once for each output");
        }
    }
}

impl InputBase for UnionFanOut {
    fn get_schema(&self) -> &SourceSchema {
        // TS: `return this.#input.getSchema();`
        self.input.get_schema()
    }

    fn destroy(&mut self) {
        self.destroy_downstream();
    }
}

impl Input for UnionFanOut {
    fn set_output(&mut self, output: Box<dyn Output>) {
        // TS: `this.#outputs.push(output);`
        self.outputs
            .lock()
            .expect("union_fan_out outputs mutex poisoned")
            .push(output);
    }

    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        // TS: `return this.#input.fetch(req);`
        //
        // Eagerly collect to return an owned iterator — the upstream
        // iterator borrows upstream internals that may not outlive this
        // call in Rust.
        let collected: Vec<NodeOrYield> = self.input.fetch(req).collect();
        Box::new(collected.into_iter())
    }
}

impl Output for UnionFanOut {
    fn push<'a>(&'a mut self, change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        // TS:
        //   must(this.#unionFanIn).fanOutStartedPushing();
        //   for (const output of this.#outputs) yield* output.push(change, this);
        //   yield* must(this.#unionFanIn).fanOutDonePushing(change.type);
        let change_type = change.change_type();
        let fan_in = {
            let guard = self
                .union_fan_in
                .lock()
                .expect("union_fan_out fan_in mutex poisoned");
            guard
                .as_ref()
                .expect("UnionFanOut: must(this.#unionFanIn) — fan-in not set")
                .clone()
        };

        // fanOutStartedPushing
        {
            let mut fi = fan_in.lock().expect("union_fan_in mutex poisoned");
            fi.fan_out_started_pushing();
        }

        // broadcast push to every output
        let mut yields: Vec<Yield> = Vec::new();
        {
            let mut outs = self
                .outputs
                .lock()
                .expect("union_fan_out outputs mutex poisoned");
            let pusher: &dyn InputBase = &*self.input;
            for out in outs.iter_mut() {
                for y in out.push(change.clone(), pusher) {
                    yields.push(y);
                }
            }
        }

        // fanOutDonePushing
        {
            let mut fi = fan_in.lock().expect("union_fan_in mutex poisoned");
            for y in fi.fan_out_done_pushing(change_type) {
                yields.push(y);
            }
        }

        Box::new(yields.into_iter())
    }
}

impl Operator for UnionFanOut {}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!   - `new` leaves outputs empty, fan-in unset.
    //!   - `set_output` appends.
    //!   - `set_fan_in` stores the back-reference.
    //!   - `set_fan_in` called twice panics per TS assert.
    //!   - `get_schema` delegates.
    //!   - `destroy_downstream` — partial count: no upstream destroy.
    //!   - `destroy_downstream` — last call: upstream destroyed.
    //!   - `destroy_downstream` — over-call panics.
    //!   - `destroy_downstream` — zero outputs panics immediately.
    //!   - `fetch` delegates to upstream.
    //!   - `push` without fan-in → panics.
    //!   - `push` broadcasts change to every output.
    //!   - `push` notifies fan-in with `fanOutStartedPushing` before
    //!     outputs and `fanOutDonePushing` after.

    use super::*;
    use crate::ivm::change::{AddChange, ChangeType};
    use crate::ivm::data::{Node, make_comparator};
    use crate::ivm::union_fan_in::UnionFanIn;
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

    /// Stub upstream Input backed by a fixed Vec<Row>.
    struct StubInput {
        schema: SourceSchema,
        rows: Vec<Row>,
        destroyed: Arc<AtomicBool>,
    }
    impl InputBase for StubInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {
            self.destroyed.store(true, AtOrdering::SeqCst);
        }
    }
    impl Input for StubInput {
        fn set_output(&mut self, _o: Box<dyn Output>) {}
        fn fetch<'a>(&'a self, _req: FetchRequest) -> Stream<'a, NodeOrYield> {
            let items: Vec<NodeOrYield> = self
                .rows
                .iter()
                .cloned()
                .map(|r| NodeOrYield::Node(node_of(r)))
                .collect();
            Box::new(items.into_iter())
        }
    }

    fn stub_input(rows: Vec<Row>) -> (Box<dyn Input>, Arc<AtomicBool>) {
        let d = Arc::new(AtomicBool::new(false));
        (
            Box::new(StubInput {
                schema: make_schema("t"),
                rows,
                destroyed: Arc::clone(&d),
            }),
            d,
        )
    }

    /// Records each push.
    struct Recorder {
        pushes: Arc<Mutex<Vec<ChangeType>>>,
    }
    impl Output for Recorder {
        fn push<'a>(&'a mut self, c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes.lock().unwrap().push(c.change_type());
            Box::new(std::iter::empty())
        }
    }

    // Branch: new — empty outputs, no fan-in.
    #[test]
    fn union_fan_out_new_empty() {
        let (input, _) = stub_input(vec![]);
        let ufo = UnionFanOut::new(input);
        assert_eq!(ufo.outputs.lock().unwrap().len(), 0);
        assert!(ufo.union_fan_in.lock().unwrap().is_none());
    }

    // Branch: set_output appends.
    #[test]
    fn union_fan_out_set_output_appends() {
        let (input, _) = stub_input(vec![]);
        let mut ufo = UnionFanOut::new(input);
        ufo.set_output(Box::new(Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
        }));
        ufo.set_output(Box::new(Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
        }));
        assert_eq!(ufo.outputs.lock().unwrap().len(), 2);
    }

    // Branch: set_fan_in stores once.
    #[test]
    fn union_fan_out_set_fan_in_stores_once() {
        let (input, _) = stub_input(vec![]);
        let ufo = UnionFanOut::new(input);
        let fi = Arc::new(Mutex::new(UnionFanIn::new(make_schema("t"), vec![])));
        ufo.set_fan_in(Arc::clone(&fi));
        assert!(ufo.union_fan_in.lock().unwrap().is_some());
    }

    // Branch: set_fan_in twice panics per TS assert.
    #[test]
    #[should_panic(expected = "FanIn already set for this FanOut")]
    fn union_fan_out_set_fan_in_twice_panics() {
        let (input, _) = stub_input(vec![]);
        let ufo = UnionFanOut::new(input);
        let fi1 = Arc::new(Mutex::new(UnionFanIn::new(make_schema("t"), vec![])));
        let fi2 = Arc::new(Mutex::new(UnionFanIn::new(make_schema("t"), vec![])));
        ufo.set_fan_in(fi1);
        ufo.set_fan_in(fi2);
    }

    // Branch: get_schema delegates.
    #[test]
    fn union_fan_out_get_schema_delegates() {
        let (input, _) = stub_input(vec![]);
        let ufo = UnionFanOut::new(input);
        assert_eq!(ufo.get_schema().table_name, "t");
    }

    // Branch: destroy_downstream partial.
    #[test]
    fn union_fan_out_destroy_downstream_partial() {
        let (input, destroyed) = stub_input(vec![]);
        let mut ufo = UnionFanOut::new(input);
        ufo.set_output(Box::new(Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
        }));
        ufo.set_output(Box::new(Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
        }));
        ufo.destroy_downstream();
        assert!(!destroyed.load(AtOrdering::SeqCst));
    }

    // Branch: destroy_downstream final call fires upstream.
    #[test]
    fn union_fan_out_destroy_downstream_final() {
        let (input, destroyed) = stub_input(vec![]);
        let mut ufo = UnionFanOut::new(input);
        ufo.set_output(Box::new(Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
        }));
        ufo.destroy_downstream();
        assert!(destroyed.load(AtOrdering::SeqCst));
    }

    // Branch: destroy_downstream over-call panics.
    #[test]
    #[should_panic(expected = "FanOut already destroyed once for each output")]
    fn union_fan_out_destroy_downstream_overcall_panics() {
        let (input, _) = stub_input(vec![]);
        let mut ufo = UnionFanOut::new(input);
        ufo.set_output(Box::new(Recorder {
            pushes: Arc::new(Mutex::new(vec![])),
        }));
        ufo.destroy_downstream();
        ufo.destroy_downstream();
    }

    // Branch: destroy_downstream zero outputs → panics immediately.
    #[test]
    #[should_panic(expected = "FanOut already destroyed once for each output")]
    fn union_fan_out_destroy_downstream_zero_outputs_panics() {
        let (input, _) = stub_input(vec![]);
        let mut ufo = UnionFanOut::new(input);
        ufo.destroy_downstream();
    }

    // Branch: fetch delegates to upstream.
    #[test]
    fn union_fan_out_fetch_delegates() {
        let (input, _) = stub_input(vec![row_with(1), row_with(2)]);
        let ufo = UnionFanOut::new(input);
        let got: Vec<NodeOrYield> = ufo.fetch(FetchRequest::default()).collect();
        assert_eq!(got.len(), 2);
    }

    // Branch: push without fan-in → panics.
    #[test]
    #[should_panic(expected = "UnionFanOut: must(this.#unionFanIn)")]
    fn union_fan_out_push_without_fan_in_panics() {
        let (input, _) = stub_input(vec![]);
        let mut ufo = UnionFanOut::new(input);
        let (pusher_input, _) = stub_input(vec![]);
        let _: Vec<_> = ufo
            .push(
                Change::Add(AddChange {
                    node: node_of(row_with(1)),
                }),
                &*pusher_input as &dyn InputBase,
            )
            .collect();
    }

    // Branch: push broadcasts to all outputs and notifies fan-in both
    // before (started) and after (done).
    #[test]
    fn union_fan_out_push_broadcasts_and_brackets_fan_in() {
        let (input, _) = stub_input(vec![]);
        let mut ufo = UnionFanOut::new(input);

        let p1 = Arc::new(Mutex::new(vec![]));
        let p2 = Arc::new(Mutex::new(vec![]));
        ufo.set_output(Box::new(Recorder {
            pushes: Arc::clone(&p1),
        }));
        ufo.set_output(Box::new(Recorder {
            pushes: Arc::clone(&p2),
        }));

        let fi = Arc::new(Mutex::new(UnionFanIn::new(make_schema("t"), vec![])));
        ufo.set_fan_in(Arc::clone(&fi));

        let (pusher_input, _) = stub_input(vec![]);
        let _: Vec<_> = ufo
            .push(
                Change::Add(AddChange {
                    node: node_of(row_with(1)),
                }),
                &*pusher_input as &dyn InputBase,
            )
            .collect();

        assert_eq!(p1.lock().unwrap().len(), 1);
        assert_eq!(p2.lock().unwrap().len(), 1);
        assert_eq!(p1.lock().unwrap()[0], ChangeType::Add);
        // fan_out_started_pushing was called (and subsequently cleared
        // by fan_out_done_pushing; both would panic if mis-ordered).
    }
}
