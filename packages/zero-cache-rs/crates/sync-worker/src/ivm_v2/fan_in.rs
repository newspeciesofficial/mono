//! FanIn operator — ivm_v2.
//!
//! Merges N streams back together, deduplicating rows. In ivm_v1, FanIn
//! tracked per-row received-count and emitted once the count reached N
//! (coordinated via `fan_out_done_pushing_to_all_branches`).
//!
//! In ivm_v2 with iterator-returning operators, deduplication across
//! N push streams requires external coordination: the caller must
//! collect from each branch and call `FanIn::accept_branch(changes)`
//! for each, then `FanIn::drain()` to get the deduplicated output.
//!
//! This is a **scaffold**; the builder integration is deferred.

use std::collections::HashMap;

use super::change::{Change, Node, SourceSchema};
use super::operator::{FetchRequest, Input, InputBase, Operator};

pub struct FanIn {
    input: Box<dyn Input>,
    /// Number of branches expected to push the same change before
    /// dedup is considered complete. Exposed for introspection /
    /// future branch-sync coordination.
    pub branch_count: usize,
    /// Dedup map keyed on change PK (rendered as JSON). Value is the
    /// number of branches that have emitted this change so far.
    dedup: HashMap<String, usize>,
    schema: SourceSchema,
}

impl FanIn {
    pub fn new(input: Box<dyn Input>, branch_count: usize) -> Self {
        let schema = input.get_schema().clone();
        Self {
            input,
            branch_count,
            dedup: HashMap::new(),
            schema,
        }
    }

    pub fn reset_dedup(&mut self) {
        self.dedup.clear();
    }
}

impl InputBase for FanIn {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.input.destroy();
    }
}

impl Input for FanIn {
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        self.input.fetch(req)
    }
}

impl Operator for FanIn {
    /// Push-side dedup: emit the change only the first time it arrives
    /// from any branch for this advance cycle.
    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        let key = change_key(&change);
        let count = self.dedup.entry(key).or_insert(0);
        *count += 1;
        if *count == 1 {
            Box::new(std::iter::once(change))
        } else {
            Box::new(std::iter::empty())
        }
    }
}

fn change_key(c: &Change) -> String {
    // PK-based dedup key. Minimal — real impl would include change type.
    let node = match c {
        Change::Add(a) => &a.node,
        Change::Remove(r) => &r.node,
        Change::Child(c) => &c.node,
        Change::Edit(e) => &e.node,
    };
    serde_json::to_string(&node.row).unwrap_or_default()
}
