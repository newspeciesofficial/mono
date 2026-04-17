//! FanOut operator — ivm_v2.
//!
//! Forks a stream of changes to N downstream consumers. In ivm_v1, FanOut
//! held `Vec<Box<dyn FilterOutput>>` and pushed to each. Since ivm_v2
//! operators don't hold downstream handles, FanOut simply emits `N`
//! copies of each change; the caller (builder) wires those to the N
//! downstream chains.
//!
//! NOTE: `Change` is not `Clone`-cheap (nested relationship factories
//! aren't Clone). The first-cut behaviour here is that the caller should
//! not rely on cheap multi-way cloning. A future refinement would wrap
//! in `Arc<Change>`.
//!
//! For OR-condition queries (FanOut → N filters → FanIn), coordination
//! with FanIn requires a sibling trait `fan_out_done_pushing_to_all_branches`
//! which isn't yet ported — this cut only handles the push-without-
//! dedup case.

use super::change::{Change, Node, SourceSchema};
use super::operator::{FetchRequest, Input, InputBase, Operator};

pub struct FanOut {
    input: Box<dyn Input>,
    branch_count: usize,
    destroy_count: usize,
    schema: SourceSchema,
}

impl FanOut {
    pub fn new(input: Box<dyn Input>, branch_count: usize) -> Self {
        let schema = input.get_schema().clone();
        Self {
            input,
            branch_count,
            destroy_count: 0,
            schema,
        }
    }
}

impl InputBase for FanOut {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        // TS: only destroy upstream after all N downstreams have called destroy.
        self.destroy_count += 1;
        if self.destroy_count >= self.branch_count && self.branch_count > 0 {
            self.input.destroy();
        }
    }
}

impl Input for FanOut {
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        // FanOut fetch is a straight pass-through. Caller handles
        // dispatching to each branch.
        self.input.fetch(req)
    }
}

impl Operator for FanOut {
    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        // Emit once; caller dispatches to each branch by cloning the
        // iterator result upstream. Since Change isn't Clone, the caller
        // is responsible for choosing how to replicate.
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!("[ivm:rs:fan_out:push] branches={}", self.branch_count);
        }
        Box::new(std::iter::once(change))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::AddChange;
    use crate::ivm::data::make_comparator;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering as AtomOrdering};
    use std::sync::Arc;
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;
    use zero_cache_types::value::Row;

    fn schema() -> SourceSchema {
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
        SourceSchema {
            table_name: "t".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }
    fn node_of(id: i64) -> Node {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        Node {
            row: r,
            relationships: IndexMap::new(),
        }
    }
    struct Stub {
        s: SourceSchema,
        d: Arc<AtomicBool>,
    }
    impl InputBase for Stub {
        fn get_schema(&self) -> &SourceSchema {
            &self.s
        }
        fn destroy(&mut self) {
            self.d.store(true, AtomOrdering::SeqCst);
        }
    }
    impl Input for Stub {
        fn fetch<'a>(&'a mut self, _r: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
            Box::new(std::iter::empty())
        }
    }

    #[test]
    fn destroy_waits_for_all_branches() {
        let d = Arc::new(AtomicBool::new(false));
        let stub = Stub {
            s: schema(),
            d: Arc::clone(&d),
        };
        let mut fanout = FanOut::new(Box::new(stub), 3);
        fanout.destroy(); // 1/3
        assert!(!d.load(AtomOrdering::SeqCst));
        fanout.destroy(); // 2/3
        assert!(!d.load(AtomOrdering::SeqCst));
        fanout.destroy(); // 3/3 → upstream destroy
        assert!(d.load(AtomOrdering::SeqCst));
    }

    #[test]
    fn push_emits_single_change() {
        let d = Arc::new(AtomicBool::new(false));
        let stub = Stub {
            s: schema(),
            d,
        };
        let mut fanout = FanOut::new(Box::new(stub), 2);
        let out: Vec<Change> = fanout
            .push(Change::Add(AddChange { node: node_of(1) }))
            .collect();
        assert_eq!(out.len(), 1);
    }
}
