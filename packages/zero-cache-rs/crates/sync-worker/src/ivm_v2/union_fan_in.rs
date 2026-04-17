//! UnionFanIn — ivm_v2 scaffold.
//!
//! Like `FanIn` but preserves input order across branches for UNION
//! semantics. First-cut scaffold: pass-through of the first branch's
//! push; de-dup across branches deferred.

use std::collections::HashMap;

use super::change::{Change, Node, SourceSchema};
use super::operator::{FetchRequest, Input, InputBase, Operator};

pub struct UnionFanIn {
    input: Box<dyn Input>,
    dedup: HashMap<String, usize>,
    schema: SourceSchema,
}

impl UnionFanIn {
    pub fn new(input: Box<dyn Input>) -> Self {
        let schema = input.get_schema().clone();
        Self {
            input,
            dedup: HashMap::new(),
            schema,
        }
    }

    pub fn reset_dedup(&mut self) {
        self.dedup.clear();
    }
}

impl InputBase for UnionFanIn {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.input.destroy();
    }
}
impl Input for UnionFanIn {
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        self.input.fetch(req)
    }
}
impl Operator for UnionFanIn {
    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        let key = dedup_key(&change);
        let c = self.dedup.entry(key).or_insert(0);
        *c += 1;
        // mirrors TS union-fan-in.ts:112
        if std::env::var("IVM_PARITY_TRACE").is_ok() {
            eprintln!(
                "[ivm:rs:union-fan-in.ts:111:push type={} fanOutPushStarted={}]",
                match &change { Change::Add(_) => "Add", Change::Remove(_) => "Remove", Change::Child(_) => "Child", Change::Edit(_) => "Edit" },
                *c > 1
            );
        }
        if *c == 1 {
            // mirrors TS union-fan-in.ts:114
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                eprintln!(
                    "[ivm:rs:union-fan-in.ts:113:push-internal-change type={}]",
                    match &change { Change::Add(_) => "Add", Change::Remove(_) => "Remove", Change::Child(_) => "Child", Change::Edit(_) => "Edit" }
                );
            }
            // mirrors TS union-fan-in.ts:181
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                eprintln!(
                    "[ivm:rs:union-fan-in.ts:176:push-internal-no-other-branch-emit type={}]",
                    match &change { Change::Add(_) => "Add", Change::Remove(_) => "Remove", Change::Child(_) => "Child", Change::Edit(_) => "Edit" }
                );
            }
            Box::new(std::iter::once(change))
        } else {
            // mirrors TS union-fan-in.ts:173
            if std::env::var("IVM_PARITY_TRACE").is_ok() {
                eprintln!(
                    "[ivm:rs:union-fan-in.ts:167:push-internal-other-branch-has-row-skip type={}]",
                    match &change { Change::Add(_) => "Add", Change::Remove(_) => "Remove", Change::Child(_) => "Child", Change::Edit(_) => "Edit" }
                );
            }
            Box::new(std::iter::empty())
        }
    }
}

fn dedup_key(c: &Change) -> String {
    let node = match c {
        Change::Add(a) => &a.node,
        Change::Remove(r) => &r.node,
        Change::Child(c) => &c.node,
        Change::Edit(e) => &e.node,
    };
    serde_json::to_string(&node.row).unwrap_or_default()
}
