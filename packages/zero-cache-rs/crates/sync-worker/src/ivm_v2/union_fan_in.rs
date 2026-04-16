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
        if *c == 1 {
            Box::new(std::iter::once(change))
        } else {
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
