//! UnionFanOut operator — ivm_v2 scaffold.
//!
//! Counterpart to `FanOut` for UNION sub-queries. Structurally identical
//! to `FanOut` in this cut. Semantic distinction (union-specific ordering
//! and de-dup) lives in `UnionFanIn`.

use super::change::{Change, Node, SourceSchema};
use super::operator::{FetchRequest, Input, InputBase, Operator};

pub struct UnionFanOut {
    input: Box<dyn Input>,
    branch_count: usize,
    destroy_count: usize,
    schema: SourceSchema,
}

impl UnionFanOut {
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

impl InputBase for UnionFanOut {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.destroy_count += 1;
        if self.destroy_count >= self.branch_count && self.branch_count > 0 {
            self.input.destroy();
        }
    }
}
impl Input for UnionFanOut {
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        self.input.fetch(req)
    }
}
impl Operator for UnionFanOut {
    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        Box::new(std::iter::once(change))
    }
}
