//! Port of `packages/zql/src/ivm/union-fan-in.ts`.
//!
//! Public exports ported:
//!
//! - [`UnionFanIn`] — the `Operator` paired with [`super::union_fan_out::UnionFanOut`]
//!   that merges N upstream sub-pipelines while preserving set semantics.
//! - [`merge_fetches`] — merges N already-sorted `Stream<Node | 'yield'>`s
//!   into a single sorted de-duplicated stream.
//!
//! ## Ownership divergence from TS
//!
//! TS's ctor calls `fanOut.setFanIn(this)` *and* `input.setOutput(this)`
//! for every input. Porting those back-edges verbatim would create a
//! cycle. The pipeline driver (Layer 9+) wires the back-edges; in this
//! module we accept the upstream schema and inputs directly, and expose
//! [`UnionFanIn::push`] / [`UnionFanIn::fan_out_started_pushing`] /
//! [`UnionFanIn::fan_out_done_pushing`] as the entry points.
//!
//! The schema is built the same way TS does: start from the
//! union-fan-out schema, then union in each input's relationships that
//! aren't already in the fan-out schema. Assertions on matching
//! tableName/primaryKey/system/sort/compareRows are reproduced. The TS
//! assertion on `compareRows === inputSchema.compareRows` checks
//! function-reference equality; in Rust we check `Arc::ptr_eq` over the
//! comparator Arc.

use std::cmp::Ordering as CmpOrdering;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::constraint::Constraint;
use crate::ivm::data::{Node, NodeOrYield};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Operator, Output, ThrowOutput};
use crate::ivm::push_accumulated::{
    make_add_empty_relationships, merge_relationships, push_accumulated_changes,
};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::Yield;
use crate::ivm::stream::Stream;

/// TS `UnionFanIn`.
pub struct UnionFanIn {
    inputs: Mutex<Vec<Box<dyn Input>>>,
    schema: SourceSchema,
    output: Mutex<Box<dyn Output>>,
    fan_out_push_started: Mutex<bool>,
    accumulated_pushes: Mutex<Vec<Change>>,
}

impl UnionFanIn {
    /// TS `new UnionFanIn(fanOut, inputs)`.
    ///
    /// Takes the fan-out schema directly (instead of a reference to the
    /// fan-out) — same simplification as [`super::fan_in::FanIn::new`].
    /// The pipeline driver is responsible for calling
    /// [`super::union_fan_out::UnionFanOut::set_fan_in`] with a shared
    /// handle to the `UnionFanIn`.
    pub fn new(fan_out_schema: SourceSchema, inputs: Vec<Box<dyn Input>>) -> Self {
        // TS:
        //   const schema: Writable<SourceSchema> = {
        //     tableName, columns, primaryKey, relationships: {...fanOut.relationships},
        //     isHidden, system, compareRows, sort,
        //   };
        let mut relationships = fan_out_schema.relationships.clone();
        let mut relationships_from_branches: indexmap::IndexSet<String> = indexmap::IndexSet::new();

        for input in &inputs {
            let input_schema = input.get_schema();

            // TS asserts.
            assert!(
                fan_out_schema.table_name == input_schema.table_name,
                "Table name mismatch in union fan-in: {} != {}",
                fan_out_schema.table_name,
                input_schema.table_name
            );
            assert!(
                fan_out_schema.primary_key == input_schema.primary_key,
                "Primary key mismatch in union fan-in"
            );
            assert!(
                fan_out_schema.system == input_schema.system,
                "System mismatch in union fan-in: {:?} != {:?}",
                fan_out_schema.system,
                input_schema.system
            );
            // TS: `schema.compareRows === inputSchema.compareRows`.
            // Rust equivalent: Arc::ptr_eq over the Arc<Comparator>.
            assert!(
                Arc::ptr_eq(&fan_out_schema.compare_rows, &input_schema.compare_rows),
                "compareRows mismatch in union fan-in"
            );
            assert!(
                fan_out_schema.sort == input_schema.sort,
                "Sort mismatch in union fan-in"
            );

            for (rel_name, rel_schema) in input_schema.relationships.iter() {
                if fan_out_schema.relationships.contains_key(rel_name) {
                    continue;
                }
                assert!(
                    !relationships_from_branches.contains(rel_name),
                    "Relationship {rel_name} exists in multiple upstream inputs to union fan-in"
                );
                relationships.insert(rel_name.clone(), rel_schema.clone());
                relationships_from_branches.insert(rel_name.clone());
            }
        }

        let schema = SourceSchema {
            table_name: fan_out_schema.table_name.clone(),
            columns: fan_out_schema.columns.clone(),
            primary_key: fan_out_schema.primary_key.clone(),
            relationships,
            is_hidden: fan_out_schema.is_hidden,
            system: fan_out_schema.system,
            compare_rows: Arc::clone(&fan_out_schema.compare_rows),
            sort: fan_out_schema.sort.clone(),
        };

        Self {
            inputs: Mutex::new(inputs),
            schema,
            output: Mutex::new(Box::new(ThrowOutput)),
            fan_out_push_started: Mutex::new(false),
            accumulated_pushes: Mutex::new(Vec::new()),
        }
    }

    /// TS `fanOutStartedPushing()`.
    pub fn fan_out_started_pushing(&mut self) {
        let mut flag = self
            .fan_out_push_started
            .lock()
            .expect("union_fan_in push_started mutex poisoned");
        // TS: `assert(this.#fanOutPushStarted === false, '...')`.
        assert!(
            !*flag,
            "UnionFanIn: fanOutStartedPushing called while already pushing"
        );
        *flag = true;
    }

    /// TS `*fanOutDonePushing(fanOutChangeType)`.
    pub fn fan_out_done_pushing<'a>(
        &'a mut self,
        fan_out_change_type: ChangeType,
    ) -> Stream<'a, Yield> {
        {
            let mut flag = self
                .fan_out_push_started
                .lock()
                .expect("union_fan_in push_started mutex poisoned");
            // TS: `assert(this.#fanOutPushStarted, '...')`.
            assert!(
                *flag,
                "UnionFanIn: fanOutDonePushing called without fanOutStartedPushing"
            );
            *flag = false;
        }

        // TS: `if (this.#inputs.length === 0) return;`
        if self
            .inputs
            .lock()
            .expect("union_fan_in inputs mutex poisoned")
            .is_empty()
        {
            return Box::new(std::iter::empty());
        }

        // TS: `if (this.#accumulatedPushes.length === 0) return;`
        let mut accumulated_guard = self
            .accumulated_pushes
            .lock()
            .expect("union_fan_in accumulated_pushes mutex poisoned");
        if accumulated_guard.is_empty() {
            return Box::new(std::iter::empty());
        }

        // TS:
        //   yield* pushAccumulatedChanges(
        //     #accumulatedPushes, #output, this, fanOutChangeType,
        //     mergeRelationships, makeAddEmptyRelationships(#schema),
        //   );
        let mut drained: Vec<Change> = accumulated_guard.drain(..).collect();
        drop(accumulated_guard);

        let add_empty = make_add_empty_relationships(&self.schema);

        let mut out_guard = self
            .output
            .lock()
            .expect("union_fan_in output mutex poisoned");
        let out: &mut dyn Output = &mut **out_guard;
        let inputs = self
            .inputs
            .lock()
            .expect("union_fan_in inputs mutex poisoned");
        let pusher: &dyn InputBase = &**inputs.first().expect("checked non-empty above");
        let collected: Vec<Yield> = push_accumulated_changes(
            &mut drained,
            out,
            pusher,
            fan_out_change_type,
            &|l, r| merge_relationships(l, r),
            &|c| add_empty(c),
        )
        .collect();
        drop(inputs);
        drop(out_guard);
        Box::new(collected.into_iter())
    }

    /// TS `#pushInternalChange(change, pusher)`.
    ///
    /// Called when a child pipeline pushes *while* fan-out isn't. The TS
    /// pusher argument is an `InputBase`; we take it as a pointer-equals
    /// key — the index of the pusher in `#inputs`, since we cannot
    /// compare arbitrary `&dyn InputBase` trait objects for identity in
    /// safe Rust.
    ///
    /// Most callers will want [`UnionFanIn::push_internal_change_from`] which
    /// takes the index directly.
    fn push_internal_change_from<'a>(
        &'a mut self,
        change: Change,
        pusher_index: usize,
    ) -> Stream<'a, Yield> {
        // TS branch 1: `if (change.type === 'child')` — forward.
        if matches!(change.change_type(), ChangeType::Child) {
            let mut out = self
                .output
                .lock()
                .expect("union_fan_in output mutex poisoned");
            // Pusher identity: upstream at `pusher_index`. We need a
            // lock on inputs to get a reference to it, but the reference
            // must live for the duration of out.push. Eagerly collect.
            let inputs = self
                .inputs
                .lock()
                .expect("union_fan_in inputs mutex poisoned");
            let pusher: &dyn InputBase =
                &**inputs.get(pusher_index).expect("pusher index in range");
            let collected: Vec<Yield> = out.push(change, pusher).collect();
            drop(inputs);
            drop(out);
            return Box::new(collected.into_iter());
        }

        // TS assert: add or remove.
        match change.change_type() {
            ChangeType::Add | ChangeType::Remove => {}
            other => panic!("UnionFanIn: expected add or remove change type, got {other:?}"),
        }

        // TS:
        //   let hadMatch = false;
        //   for (const input of this.#inputs) {
        //     if (input === pusher) { hadMatch = true; continue; }
        //     const constraint = {}; for (const key of primaryKey) constraint[key] = change.node.row[key];
        //     const fetchResult = input.fetch({constraint});
        //     if (first(fetchResult) !== undefined) return;   // another branch has it
        //   }
        //   assert(hadMatch, '...');
        //   yield* this.#output.push(change, this);
        let mut had_match = false;
        {
            let inputs = self
                .inputs
                .lock()
                .expect("union_fan_in inputs mutex poisoned");
            for (i, input) in inputs.iter().enumerate() {
                if i == pusher_index {
                    had_match = true;
                    continue;
                }

                let mut constraint: Constraint = IndexMap::new();
                for key in self.schema.primary_key.columns() {
                    let v = change.node().row.get(key).cloned().unwrap_or(None);
                    constraint.insert(key.clone(), v);
                }

                let req = FetchRequest {
                    constraint: Some(constraint),
                    ..FetchRequest::default()
                };

                // TS `first(fetchResult)` — first non-`yield` element, or
                // undefined. In Rust: first `NodeOrYield::Node`.
                let mut any = false;
                for item in input.fetch(req) {
                    if let NodeOrYield::Node(_) = item {
                        any = true;
                        break;
                    }
                }
                if any {
                    // TS: another branch has the row → return.
                    return Box::new(std::iter::empty());
                }
            }
        }

        assert!(
            had_match,
            "Pusher was not one of the inputs to union-fan-in!"
        );

        // No other branches have the row → forward.
        let mut out = self
            .output
            .lock()
            .expect("union_fan_in output mutex poisoned");
        let inputs = self
            .inputs
            .lock()
            .expect("union_fan_in inputs mutex poisoned");
        let pusher: &dyn InputBase = &**inputs.get(pusher_index).expect("checked had_match above");
        let collected: Vec<Yield> = out.push(change, pusher).collect();
        drop(inputs);
        drop(out);
        Box::new(collected.into_iter())
    }

    /// TS `push(change, pusher)` — entry point for upstream branches.
    ///
    /// The `pusher_index` argument identifies which of `#inputs` sent
    /// the change (a Rust-native substitute for TS `pusher` identity
    /// comparison against each input by reference).
    pub fn push_indexed<'a>(
        &'a mut self,
        change: Change,
        pusher_index: usize,
    ) -> Stream<'a, Yield> {
        // TS:
        //   if (!this.#fanOutPushStarted) {
        //     yield* this.#pushInternalChange(change, pusher);
        //   } else {
        //     this.#accumulatedPushes.push(change);
        //   }
        let started = *self
            .fan_out_push_started
            .lock()
            .expect("union_fan_in push_started mutex poisoned");
        if !started {
            self.push_internal_change_from(change, pusher_index)
        } else {
            self.accumulated_pushes
                .lock()
                .expect("union_fan_in accumulated_pushes mutex poisoned")
                .push(change);
            Box::new(std::iter::empty())
        }
    }
}

impl InputBase for UnionFanIn {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }

    fn destroy(&mut self) {
        // TS: `for (const input of this.#inputs) input.destroy();`
        let mut inputs = self
            .inputs
            .lock()
            .expect("union_fan_in inputs mutex poisoned");
        for input in inputs.iter_mut() {
            input.destroy();
        }
    }
}

impl Input for UnionFanIn {
    fn set_output(&mut self, output: Box<dyn Output>) {
        // TS: `this.#output = output;`
        *self
            .output
            .lock()
            .expect("union_fan_in output mutex poisoned") = output;
    }

    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
        // TS:
        //   const iterables = this.#inputs.map(input => input.fetch(req));
        //   return mergeFetches(iterables, (l, r) => this.#schema.compareRows(l.row, r.row));
        //
        // Collect each input's fetch eagerly to own the items. The
        // comparator is cloned from the shared Arc for the closure.
        let inputs = self
            .inputs
            .lock()
            .expect("union_fan_in inputs mutex poisoned");
        let iterables: Vec<Vec<NodeOrYield>> = inputs
            .iter()
            .map(|inp| {
                // Each fetch request is cloned because FetchRequest is not
                // Copy.
                let cloned = FetchRequest {
                    constraint: req.constraint.clone(),
                    start: req.start.clone(),
                    reverse: req.reverse,
                };
                inp.fetch(cloned).collect()
            })
            .collect();
        drop(inputs);

        let cmp = Arc::clone(&self.schema.compare_rows);
        let merged: Vec<NodeOrYield> =
            merge_fetches(iterables, move |l: &Node, r: &Node| (cmp)(&l.row, &r.row)).collect();
        Box::new(merged.into_iter())
    }
}

impl Output for UnionFanIn {
    /// TS `*push(change, pusher)`.
    ///
    /// NOTE: the trait-level `push` can't identify the pusher by index
    /// since `&dyn InputBase` cannot be compared for trait-object
    /// identity in safe Rust without extra plumbing. This implementation
    /// panics to surface callers that attempt the TS-style
    /// pointer-identity path; use [`UnionFanIn::push_indexed`] instead.
    fn push<'a>(&'a mut self, _change: Change, _pusher: &dyn InputBase) -> Stream<'a, Yield> {
        panic!(
            "UnionFanIn::push — pipeline driver must route through push_indexed; \
             pusher identity cannot be resolved from a `&dyn InputBase` in safe Rust"
        );
    }
}

impl Operator for UnionFanIn {}

// ─── merge_fetches ────────────────────────────────────────────────────

/// TS `mergeFetches(fetches, comparator)`.
///
/// Merges N already-sorted streams (of `NodeOrYield`) into a single
/// sorted stream. Yields are forwarded as they occur on each underlying
/// iterator; `Node` values are emitted in comparator order and deduped
/// against the last-yielded node.
///
/// In TS this is a generator with try/catch/finally cleanup around
/// iter.throw / iter.return; in Rust the drop of the underlying
/// iterators handles cleanup automatically.
pub fn merge_fetches<I, F>(fetches: Vec<I>, comparator: F) -> Box<dyn Iterator<Item = NodeOrYield>>
where
    I: IntoIterator<Item = NodeOrYield> + 'static,
    F: Fn(&Node, &Node) -> CmpOrdering + 'static,
{
    // Eagerly drain yields at the head of each iterator and keep the
    // next Node at the front, buffering yields to emit before the next
    // chosen Node. We produce a Vec<NodeOrYield> so the return type is
    // a simple owned iterator.
    let mut iterators: Vec<Box<dyn Iterator<Item = NodeOrYield>>> = fetches
        .into_iter()
        .map(|it| Box::new(it.into_iter()) as Box<dyn Iterator<Item = NodeOrYield>>)
        .collect();

    let mut out: Vec<NodeOrYield> = Vec::new();
    let mut current: Vec<Option<Node>> = Vec::with_capacity(iterators.len());

    // Initialise: for each iterator, drain leading yields and keep the
    // first Node (or None if exhausted).
    for iter in iterators.iter_mut() {
        let first = advance_past_yields(iter.as_mut(), &mut out);
        current.push(first);
    }

    // Main loop: while any cursor holds a Node, pick the min and yield
    // it (unless it duplicates the last emitted node).
    let mut last_yielded: Option<Node> = None;
    loop {
        // Find index of min non-None node.
        let mut min: Option<(usize, &Node)> = None;
        for (i, c) in current.iter().enumerate() {
            if let Some(node) = c {
                match min {
                    None => min = Some((i, node)),
                    Some((_, m)) => {
                        if comparator(node, m) == CmpOrdering::Less {
                            min = Some((i, node));
                        }
                    }
                }
            }
        }
        let (min_index, _min_node) = match min {
            None => break, // All cursors exhausted.
            Some(x) => x,
        };

        // Take the min node out and advance its iterator.
        let min_node = current[min_index].take().expect("min index had a node");
        let next = advance_past_yields(iterators[min_index].as_mut(), &mut out);
        current[min_index] = next;

        // TS: if (lastNodeYielded !== undefined && comparator(lastNodeYielded, minNode) === 0) continue;
        if let Some(prev) = last_yielded.as_ref() {
            if comparator(prev, &min_node) == CmpOrdering::Equal {
                // Duplicate — drop.
                continue;
            }
        }

        // Emit.
        let emit_clone = min_node.clone();
        out.push(NodeOrYield::Node(min_node));
        last_yielded = Some(emit_clone);
    }

    Box::new(out.into_iter())
}

/// Helper: drain leading `Yield` sentinels from `iter` into `sink`, and
/// return the next `Node` (or `None` if exhausted).
fn advance_past_yields(
    iter: &mut dyn Iterator<Item = NodeOrYield>,
    sink: &mut Vec<NodeOrYield>,
) -> Option<Node> {
    loop {
        match iter.next() {
            None => return None,
            Some(NodeOrYield::Yield) => {
                sink.push(NodeOrYield::Yield);
                continue;
            }
            Some(NodeOrYield::Node(n)) => return Some(n),
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //!
    //! `UnionFanIn::new`:
    //!   - zero inputs — OK, schema mirrors fan-out.
    //!   - table_name mismatch panics.
    //!   - primary_key mismatch panics.
    //!   - system mismatch panics.
    //!   - sort mismatch panics.
    //!   - compareRows Arc mismatch panics.
    //!   - duplicate relationship from branches panics.
    //!   - relationship already on fan-out is skipped (no panic).
    //!   - unique relationships are unioned in.
    //!
    //! `fan_out_started_pushing`:
    //!   - sets flag from false → true.
    //!   - panics if called while already started.
    //!
    //! `fan_out_done_pushing`:
    //!   - panics if called without started.
    //!   - zero inputs → empty.
    //!   - zero accumulated → empty.
    //!   - N inputs + accumulated → drains and forwards once.
    //!
    //! `push_indexed`:
    //!   - fan-out push started → accumulates.
    //!   - fan-out NOT started + child → forwards.
    //!   - fan-out NOT started + add where no other branch has row → forwards.
    //!   - fan-out NOT started + add where another branch has row → drops.
    //!   - fan-out NOT started + remove (same two paths).
    //!   - fan-out NOT started + edit → panics.
    //!   - pusher not in inputs → panics.
    //!
    //! `fetch`:
    //!   - merges and dedupes across N inputs.
    //!   - zero inputs → empty.
    //!
    //! `destroy` propagates.
    //!
    //! `merge_fetches`:
    //!   - empty inputs vec → empty.
    //!   - single iterator passes through.
    //!   - two non-overlapping streams interleave.
    //!   - two overlapping streams dedupe.
    //!   - yields are forwarded.
    //!
    //! `Output::push` (trait-level) panics — pipeline driver must route.

    use super::*;
    use crate::ivm::change::{AddChange, ChildChange, ChildSpec, EditChange, RemoveChange};
    use crate::ivm::constraint::constraint_matches_row;
    use crate::ivm::data::make_comparator;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering as AtOrdering};
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Row;

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

    // All branches of the same test schema share a single comparator Arc
    // so they satisfy the Arc::ptr_eq check.
    fn shared_schema(
        name: &str,
        pk: &str,
        sort_col: &str,
        system: System,
        cmp: Arc<crate::ivm::data::Comparator>,
        relationships: IndexMap<String, SourceSchema>,
    ) -> SourceSchema {
        let sort: Ordering = vec![(sort_col.into(), Direction::Asc)];
        SourceSchema {
            table_name: name.into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec![pk.into()]),
            relationships,
            is_hidden: false,
            system,
            compare_rows: cmp,
            sort,
        }
    }

    fn base_cmp() -> Arc<crate::ivm::data::Comparator> {
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
        Arc::new(make_comparator(sort, false))
    }

    /// Build a schema with the given table name, using the SHARED `cmp`
    /// so `Arc::ptr_eq` succeeds across fan-out and inputs. Most tests
    /// use this via `mk_schemas` which hands out identical Arcs.
    fn make_schema_default(name: &str) -> SourceSchema {
        shared_schema(name, "id", "id", System::Test, base_cmp(), IndexMap::new())
    }

    /// Build `n` schemas that share the same comparator Arc. Returns a
    /// Vec of length `n+1`; index 0 is the fan-out schema, 1..=n are
    /// input schemas. All share the same table_name/pk/sort/system and
    /// the same Arc.
    fn mk_schemas(n: usize) -> Vec<SourceSchema> {
        let cmp = base_cmp();
        (0..=n)
            .map(|_| {
                shared_schema(
                    "t",
                    "id",
                    "id",
                    System::Test,
                    Arc::clone(&cmp),
                    IndexMap::new(),
                )
            })
            .collect()
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
        fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield> {
            let items: Vec<NodeOrYield> = self
                .rows
                .iter()
                .cloned()
                .filter(|r| match &req.constraint {
                    None => true,
                    Some(c) => constraint_matches_row(c, r),
                })
                .map(|r| NodeOrYield::Node(node_of(r)))
                .collect();
            Box::new(items.into_iter())
        }
    }

    fn stub_with(schema: SourceSchema, rows: Vec<Row>) -> (Box<dyn Input>, Arc<AtomicBool>) {
        let d = Arc::new(AtomicBool::new(false));
        (
            Box::new(StubInput {
                schema,
                rows,
                destroyed: Arc::clone(&d),
            }),
            d,
        )
    }

    /// Records pushes.
    struct RecOutput {
        pushes: Arc<Mutex<Vec<ChangeType>>>,
    }
    impl Output for RecOutput {
        fn push<'a>(&'a mut self, c: Change, _p: &dyn InputBase) -> Stream<'a, Yield> {
            self.pushes.lock().unwrap().push(c.change_type());
            Box::new(std::iter::empty())
        }
    }

    // ── new ──

    // Branch: zero inputs → OK, schema mirrors fan-out.
    #[test]
    fn union_fan_in_new_zero_inputs_ok() {
        let s = make_schema_default("t");
        let ufi = UnionFanIn::new(s, vec![]);
        assert_eq!(ufi.get_schema().table_name, "t");
    }

    // Branch: table_name mismatch panics.
    #[test]
    #[should_panic(expected = "Table name mismatch in union fan-in")]
    fn union_fan_in_new_table_name_mismatch_panics() {
        let cmp = base_cmp();
        let fo = shared_schema(
            "t",
            "id",
            "id",
            System::Test,
            Arc::clone(&cmp),
            IndexMap::new(),
        );
        let in_schema = shared_schema("u", "id", "id", System::Test, cmp, IndexMap::new());
        let (input, _) = stub_with(in_schema, vec![]);
        let _ = UnionFanIn::new(fo, vec![input]);
    }

    // Branch: primary_key mismatch.
    #[test]
    #[should_panic(expected = "Primary key mismatch in union fan-in")]
    fn union_fan_in_new_pk_mismatch_panics() {
        let cmp = base_cmp();
        let fo = shared_schema(
            "t",
            "id",
            "id",
            System::Test,
            Arc::clone(&cmp),
            IndexMap::new(),
        );
        let in_schema = shared_schema("t", "other", "id", System::Test, cmp, IndexMap::new());
        let (input, _) = stub_with(in_schema, vec![]);
        let _ = UnionFanIn::new(fo, vec![input]);
    }

    // Branch: system mismatch.
    #[test]
    #[should_panic(expected = "System mismatch in union fan-in")]
    fn union_fan_in_new_system_mismatch_panics() {
        let cmp = base_cmp();
        let fo = shared_schema(
            "t",
            "id",
            "id",
            System::Test,
            Arc::clone(&cmp),
            IndexMap::new(),
        );
        let in_schema = shared_schema("t", "id", "id", System::Client, cmp, IndexMap::new());
        let (input, _) = stub_with(in_schema, vec![]);
        let _ = UnionFanIn::new(fo, vec![input]);
    }

    // Branch: sort mismatch.
    #[test]
    #[should_panic(expected = "Sort mismatch in union fan-in")]
    fn union_fan_in_new_sort_mismatch_panics() {
        let cmp = base_cmp();
        let fo = shared_schema(
            "t",
            "id",
            "id",
            System::Test,
            Arc::clone(&cmp),
            IndexMap::new(),
        );
        let in_schema = shared_schema("t", "id", "other", System::Test, cmp, IndexMap::new());
        let (input, _) = stub_with(in_schema, vec![]);
        let _ = UnionFanIn::new(fo, vec![input]);
    }

    // Branch: compareRows Arc mismatch.
    #[test]
    #[should_panic(expected = "compareRows mismatch in union fan-in")]
    fn union_fan_in_new_compare_rows_mismatch_panics() {
        let fo = make_schema_default("t");
        // Different comparator Arc.
        let in_schema = shared_schema("t", "id", "id", System::Test, base_cmp(), IndexMap::new());
        let (input, _) = stub_with(in_schema, vec![]);
        let _ = UnionFanIn::new(fo, vec![input]);
    }

    // Branch: duplicate relationship across branches panics.
    #[test]
    #[should_panic(expected = "exists in multiple upstream inputs to union fan-in")]
    fn union_fan_in_new_duplicate_relationship_panics() {
        let cmp = base_cmp();
        let fo = shared_schema(
            "t",
            "id",
            "id",
            System::Test,
            Arc::clone(&cmp),
            IndexMap::new(),
        );

        // Two input schemas each declaring a relationship "r".
        let mut rels1 = IndexMap::new();
        rels1.insert(
            "r".into(),
            shared_schema(
                "r",
                "id",
                "id",
                System::Test,
                Arc::clone(&cmp),
                IndexMap::new(),
            ),
        );
        let mut rels2 = IndexMap::new();
        rels2.insert(
            "r".into(),
            shared_schema(
                "r",
                "id",
                "id",
                System::Test,
                Arc::clone(&cmp),
                IndexMap::new(),
            ),
        );
        let s1 = shared_schema("t", "id", "id", System::Test, Arc::clone(&cmp), rels1);
        let s2 = shared_schema("t", "id", "id", System::Test, Arc::clone(&cmp), rels2);

        let (i1, _) = stub_with(s1, vec![]);
        let (i2, _) = stub_with(s2, vec![]);
        let _ = UnionFanIn::new(fo, vec![i1, i2]);
    }

    // Branch: relationship already on fan-out → skipped, no panic even if
    // multiple inputs declare it.
    #[test]
    fn union_fan_in_new_relationship_on_fan_out_skipped() {
        let cmp = base_cmp();
        let mut fo_rels = IndexMap::new();
        fo_rels.insert(
            "r".into(),
            shared_schema(
                "r",
                "id",
                "id",
                System::Test,
                Arc::clone(&cmp),
                IndexMap::new(),
            ),
        );
        let fo = shared_schema("t", "id", "id", System::Test, Arc::clone(&cmp), fo_rels);

        let mut rels1 = IndexMap::new();
        rels1.insert(
            "r".into(),
            shared_schema(
                "r",
                "id",
                "id",
                System::Test,
                Arc::clone(&cmp),
                IndexMap::new(),
            ),
        );
        let s1 = shared_schema("t", "id", "id", System::Test, Arc::clone(&cmp), rels1);
        let (i1, _) = stub_with(s1, vec![]);

        let ufi = UnionFanIn::new(fo, vec![i1]);
        assert!(ufi.get_schema().relationships.contains_key("r"));
    }

    // Branch: unique branch relationship unioned in.
    #[test]
    fn union_fan_in_new_unique_branch_relationships_unioned() {
        let cmp = base_cmp();
        let fo = shared_schema(
            "t",
            "id",
            "id",
            System::Test,
            Arc::clone(&cmp),
            IndexMap::new(),
        );

        let mut rels1 = IndexMap::new();
        rels1.insert(
            "a".into(),
            shared_schema(
                "a",
                "id",
                "id",
                System::Test,
                Arc::clone(&cmp),
                IndexMap::new(),
            ),
        );
        let mut rels2 = IndexMap::new();
        rels2.insert(
            "b".into(),
            shared_schema(
                "b",
                "id",
                "id",
                System::Test,
                Arc::clone(&cmp),
                IndexMap::new(),
            ),
        );
        let s1 = shared_schema("t", "id", "id", System::Test, Arc::clone(&cmp), rels1);
        let s2 = shared_schema("t", "id", "id", System::Test, Arc::clone(&cmp), rels2);
        let (i1, _) = stub_with(s1, vec![]);
        let (i2, _) = stub_with(s2, vec![]);

        let ufi = UnionFanIn::new(fo, vec![i1, i2]);
        assert!(ufi.get_schema().relationships.contains_key("a"));
        assert!(ufi.get_schema().relationships.contains_key("b"));
    }

    // ── started/done pushing ──

    // Branch: started → toggles flag true.
    #[test]
    fn union_fan_in_started_toggles_flag() {
        let mut ufi = UnionFanIn::new(make_schema_default("t"), vec![]);
        ufi.fan_out_started_pushing();
        assert!(*ufi.fan_out_push_started.lock().unwrap());
    }

    // Branch: started twice panics.
    #[test]
    #[should_panic(expected = "fanOutStartedPushing called while already pushing")]
    fn union_fan_in_started_twice_panics() {
        let mut ufi = UnionFanIn::new(make_schema_default("t"), vec![]);
        ufi.fan_out_started_pushing();
        ufi.fan_out_started_pushing();
    }

    // Branch: done without started panics.
    #[test]
    #[should_panic(expected = "fanOutDonePushing called without fanOutStartedPushing")]
    fn union_fan_in_done_without_started_panics() {
        let mut ufi = UnionFanIn::new(make_schema_default("t"), vec![]);
        let _: Vec<_> = ufi.fan_out_done_pushing(ChangeType::Add).collect();
    }

    // Branch: done — zero inputs → empty stream.
    #[test]
    fn union_fan_in_done_zero_inputs_empty() {
        let mut ufi = UnionFanIn::new(make_schema_default("t"), vec![]);
        ufi.fan_out_started_pushing();
        let out: Vec<_> = ufi.fan_out_done_pushing(ChangeType::Add).collect();
        assert!(out.is_empty());
    }

    // Branch: done — N inputs + zero accumulated → empty stream.
    #[test]
    fn union_fan_in_done_empty_accumulated_empty() {
        let mut schemas = mk_schemas(1).into_iter();
        let fo_schema = schemas.next().unwrap();
        let (input, _) = stub_with(schemas.next().unwrap(), vec![]);
        let mut ufi = UnionFanIn::new(fo_schema, vec![input]);
        ufi.fan_out_started_pushing();
        let out: Vec<_> = ufi.fan_out_done_pushing(ChangeType::Add).collect();
        assert!(out.is_empty());
    }

    // Branch: done — accumulated + N inputs → single forward.
    #[test]
    fn union_fan_in_done_forwards_single() {
        let mut schemas = mk_schemas(1).into_iter();
        let fo_schema = schemas.next().unwrap();
        let (input, _) = stub_with(schemas.next().unwrap(), vec![]);
        let mut ufi = UnionFanIn::new(fo_schema, vec![input]);

        let pushes = Arc::new(Mutex::new(vec![]));
        ufi.set_output(Box::new(RecOutput {
            pushes: Arc::clone(&pushes),
        }));

        ufi.fan_out_started_pushing();
        // Two adds for same row, collapsed to one.
        ufi.accumulated_pushes
            .lock()
            .unwrap()
            .push(Change::Add(AddChange {
                node: node_of(row_with(1)),
            }));
        ufi.accumulated_pushes
            .lock()
            .unwrap()
            .push(Change::Add(AddChange {
                node: node_of(row_with(1)),
            }));
        let _: Vec<_> = ufi.fan_out_done_pushing(ChangeType::Add).collect();
        let p = pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0], ChangeType::Add);
    }

    // ── push_indexed ──

    // Branch: fan-out push started → accumulates.
    #[test]
    fn union_fan_in_push_indexed_accumulates_when_started() {
        let mut schemas = mk_schemas(1).into_iter();
        let fo = schemas.next().unwrap();
        let (input, _) = stub_with(schemas.next().unwrap(), vec![]);
        let mut ufi = UnionFanIn::new(fo, vec![input]);
        ufi.fan_out_started_pushing();
        let _: Vec<_> = ufi
            .push_indexed(
                Change::Add(AddChange {
                    node: node_of(row_with(1)),
                }),
                0,
            )
            .collect();
        assert_eq!(ufi.accumulated_pushes.lock().unwrap().len(), 1);
    }

    // Branch: fan-out NOT started, change is child → forwards.
    #[test]
    fn union_fan_in_push_indexed_child_forwards() {
        let mut schemas = mk_schemas(1).into_iter();
        let fo = schemas.next().unwrap();
        let (input, _) = stub_with(schemas.next().unwrap(), vec![]);
        let mut ufi = UnionFanIn::new(fo, vec![input]);

        let pushes = Arc::new(Mutex::new(vec![]));
        ufi.set_output(Box::new(RecOutput {
            pushes: Arc::clone(&pushes),
        }));

        let child_change = Change::Child(ChildChange {
            node: node_of(row_with(1)),
            child: ChildSpec {
                relationship_name: "r".into(),
                change: Box::new(Change::Add(AddChange {
                    node: node_of(row_with(2)),
                })),
            },
        });
        let _: Vec<_> = ufi.push_indexed(child_change, 0).collect();
        let p = pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0], ChangeType::Child);
    }

    // Branch: fan-out NOT started, add, no other branch has row → forwards.
    #[test]
    fn union_fan_in_push_indexed_add_no_match_forwards() {
        // Two inputs; input[1] has nothing for row id=1.
        let mut schemas = mk_schemas(2).into_iter();
        let fo = schemas.next().unwrap();
        let (i1, _) = stub_with(schemas.next().unwrap(), vec![]);
        let (i2, _) = stub_with(schemas.next().unwrap(), vec![]);
        let mut ufi = UnionFanIn::new(fo, vec![i1, i2]);

        let pushes = Arc::new(Mutex::new(vec![]));
        ufi.set_output(Box::new(RecOutput {
            pushes: Arc::clone(&pushes),
        }));

        let _: Vec<_> = ufi
            .push_indexed(
                Change::Add(AddChange {
                    node: node_of(row_with(1)),
                }),
                0,
            )
            .collect();

        let p = pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0], ChangeType::Add);
    }

    // Branch: fan-out NOT started, add, ANOTHER branch has the row → dropped.
    #[test]
    fn union_fan_in_push_indexed_add_other_branch_has_row_drops() {
        // input[0] pushes row id=1; input[1] already has id=1.
        let mut schemas = mk_schemas(2).into_iter();
        let fo = schemas.next().unwrap();
        let (i1, _) = stub_with(schemas.next().unwrap(), vec![]);
        let (i2, _) = stub_with(schemas.next().unwrap(), vec![row_with(1)]);
        let mut ufi = UnionFanIn::new(fo, vec![i1, i2]);

        let pushes = Arc::new(Mutex::new(vec![]));
        ufi.set_output(Box::new(RecOutput {
            pushes: Arc::clone(&pushes),
        }));

        let _: Vec<_> = ufi
            .push_indexed(
                Change::Add(AddChange {
                    node: node_of(row_with(1)),
                }),
                0,
            )
            .collect();

        assert_eq!(pushes.lock().unwrap().len(), 0);
    }

    // Branch: fan-out NOT started, remove, no match → forwards.
    #[test]
    fn union_fan_in_push_indexed_remove_no_match_forwards() {
        let mut schemas = mk_schemas(2).into_iter();
        let fo = schemas.next().unwrap();
        let (i1, _) = stub_with(schemas.next().unwrap(), vec![]);
        let (i2, _) = stub_with(schemas.next().unwrap(), vec![]);
        let mut ufi = UnionFanIn::new(fo, vec![i1, i2]);

        let pushes = Arc::new(Mutex::new(vec![]));
        ufi.set_output(Box::new(RecOutput {
            pushes: Arc::clone(&pushes),
        }));

        let _: Vec<_> = ufi
            .push_indexed(
                Change::Remove(RemoveChange {
                    node: node_of(row_with(1)),
                }),
                0,
            )
            .collect();
        let p = pushes.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0], ChangeType::Remove);
    }

    // Branch: fan-out NOT started, remove, another branch has row → dropped.
    #[test]
    fn union_fan_in_push_indexed_remove_other_branch_has_row_drops() {
        let mut schemas = mk_schemas(2).into_iter();
        let fo = schemas.next().unwrap();
        let (i1, _) = stub_with(schemas.next().unwrap(), vec![]);
        let (i2, _) = stub_with(schemas.next().unwrap(), vec![row_with(1)]);
        let mut ufi = UnionFanIn::new(fo, vec![i1, i2]);

        let pushes = Arc::new(Mutex::new(vec![]));
        ufi.set_output(Box::new(RecOutput {
            pushes: Arc::clone(&pushes),
        }));

        let _: Vec<_> = ufi
            .push_indexed(
                Change::Remove(RemoveChange {
                    node: node_of(row_with(1)),
                }),
                0,
            )
            .collect();
        assert_eq!(pushes.lock().unwrap().len(), 0);
    }

    // Branch: edit change → panics.
    #[test]
    #[should_panic(expected = "expected add or remove change type")]
    fn union_fan_in_push_indexed_edit_panics() {
        let mut schemas = mk_schemas(1).into_iter();
        let fo = schemas.next().unwrap();
        let (i1, _) = stub_with(schemas.next().unwrap(), vec![]);
        let mut ufi = UnionFanIn::new(fo, vec![i1]);
        let _: Vec<_> = ufi
            .push_indexed(
                Change::Edit(EditChange {
                    node: node_of(row_with(1)),
                    old_node: node_of(row_with(1)),
                }),
                0,
            )
            .collect();
    }

    // ── fetch / destroy ──

    // Branch: fetch across N inputs — merges and dedupes.
    #[test]
    fn union_fan_in_fetch_merges_and_dedupes() {
        let mut schemas = mk_schemas(2).into_iter();
        let fo = schemas.next().unwrap();
        let (i1, _) = stub_with(schemas.next().unwrap(), vec![row_with(1), row_with(3)]);
        let (i2, _) = stub_with(schemas.next().unwrap(), vec![row_with(2), row_with(3)]);
        let ufi = UnionFanIn::new(fo, vec![i1, i2]);
        let got: Vec<i64> = ufi
            .fetch(FetchRequest::default())
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => n
                    .row
                    .get("id")
                    .and_then(|v| v.clone())
                    .and_then(|v| v.as_i64()),
                NodeOrYield::Yield => None,
            })
            .collect();
        assert_eq!(got, vec![1, 2, 3]);
    }

    // Branch: fetch with zero inputs → empty.
    #[test]
    fn union_fan_in_fetch_zero_inputs_empty() {
        let ufi = UnionFanIn::new(make_schema_default("t"), vec![]);
        let got: Vec<NodeOrYield> = ufi.fetch(FetchRequest::default()).collect();
        assert!(got.is_empty());
    }

    // Branch: destroy propagates.
    #[test]
    fn union_fan_in_destroy_propagates() {
        let mut schemas = mk_schemas(2).into_iter();
        let fo = schemas.next().unwrap();
        let (i1, d1) = stub_with(schemas.next().unwrap(), vec![]);
        let (i2, d2) = stub_with(schemas.next().unwrap(), vec![]);
        let mut ufi = UnionFanIn::new(fo, vec![i1, i2]);
        ufi.destroy();
        assert!(d1.load(AtOrdering::SeqCst));
        assert!(d2.load(AtOrdering::SeqCst));
    }

    // Branch: Output::push trait-level panics (pipeline driver must route).
    #[test]
    #[should_panic(expected = "pipeline driver must route through push_indexed")]
    fn union_fan_in_output_push_panics() {
        let (pusher_input, _) = stub_with(make_schema_default("t"), vec![]);
        let mut ufi = UnionFanIn::new(make_schema_default("t"), vec![]);
        let _: Vec<_> = Output::push(
            &mut ufi,
            Change::Add(AddChange {
                node: node_of(row_with(1)),
            }),
            &*pusher_input as &dyn InputBase,
        )
        .collect();
    }

    // ── merge_fetches ──

    // Branch: no iterators → empty.
    #[test]
    fn merge_fetches_empty_inputs_empty() {
        let cmp = base_cmp();
        let out: Vec<NodeOrYield> =
            merge_fetches::<Vec<NodeOrYield>, _>(vec![], move |l, r| (cmp)(&l.row, &r.row))
                .collect();
        assert!(out.is_empty());
    }

    // Branch: single iterator → passes through.
    #[test]
    fn merge_fetches_single_iterator_passthrough() {
        let cmp = base_cmp();
        let items: Vec<NodeOrYield> = vec![
            NodeOrYield::Node(node_of(row_with(1))),
            NodeOrYield::Node(node_of(row_with(2))),
        ];
        let out: Vec<i64> = merge_fetches(vec![items], move |l, r| (cmp)(&l.row, &r.row))
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => n
                    .row
                    .get("id")
                    .and_then(|v| v.clone())
                    .and_then(|v| v.as_i64()),
                NodeOrYield::Yield => None,
            })
            .collect();
        assert_eq!(out, vec![1, 2]);
    }

    // Branch: two non-overlapping streams interleave.
    #[test]
    fn merge_fetches_non_overlapping_interleave() {
        let cmp = base_cmp();
        let a: Vec<NodeOrYield> = vec![
            NodeOrYield::Node(node_of(row_with(1))),
            NodeOrYield::Node(node_of(row_with(4))),
        ];
        let b: Vec<NodeOrYield> = vec![
            NodeOrYield::Node(node_of(row_with(2))),
            NodeOrYield::Node(node_of(row_with(3))),
        ];
        let out: Vec<i64> = merge_fetches(vec![a, b], move |l, r| (cmp)(&l.row, &r.row))
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => n
                    .row
                    .get("id")
                    .and_then(|v| v.clone())
                    .and_then(|v| v.as_i64()),
                NodeOrYield::Yield => None,
            })
            .collect();
        assert_eq!(out, vec![1, 2, 3, 4]);
    }

    // Branch: two overlapping streams → dedupe.
    #[test]
    fn merge_fetches_overlapping_dedupe() {
        let cmp = base_cmp();
        let a: Vec<NodeOrYield> = vec![
            NodeOrYield::Node(node_of(row_with(1))),
            NodeOrYield::Node(node_of(row_with(2))),
        ];
        let b: Vec<NodeOrYield> = vec![
            NodeOrYield::Node(node_of(row_with(2))),
            NodeOrYield::Node(node_of(row_with(3))),
        ];
        let out: Vec<i64> = merge_fetches(vec![a, b], move |l, r| (cmp)(&l.row, &r.row))
            .filter_map(|n| match n {
                NodeOrYield::Node(n) => n
                    .row
                    .get("id")
                    .and_then(|v| v.clone())
                    .and_then(|v| v.as_i64()),
                NodeOrYield::Yield => None,
            })
            .collect();
        assert_eq!(out, vec![1, 2, 3]);
    }

    // Branch: yields forwarded.
    #[test]
    fn merge_fetches_yields_forwarded() {
        let cmp = base_cmp();
        let a: Vec<NodeOrYield> = vec![NodeOrYield::Yield, NodeOrYield::Node(node_of(row_with(1)))];
        let out: Vec<NodeOrYield> =
            merge_fetches(vec![a], move |l, r| (cmp)(&l.row, &r.row)).collect();
        // Exactly one Yield and one Node.
        let yields = out
            .iter()
            .filter(|n| matches!(n, NodeOrYield::Yield))
            .count();
        let nodes = out
            .iter()
            .filter(|n| matches!(n, NodeOrYield::Node(_)))
            .count();
        assert_eq!(yields, 1);
        assert_eq!(nodes, 1);
    }
}
