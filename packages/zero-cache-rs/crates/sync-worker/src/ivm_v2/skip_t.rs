//! Skip — Transformer shape.

use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

use zero_cache_types::value::Row;

use super::change::{Change, Node};
use super::filter::maybe_split_edit;
use super::operator::{FetchRequest, Transformer};

#[derive(Debug, Clone)]
pub struct Bound {
    pub row: Row,
    pub exclusive: bool,
}

pub struct SkipT {
    bound: Bound,
    comparator: Arc<crate::ivm::data::Comparator>,
}

impl SkipT {
    pub fn new(
        bound: Bound,
        comparator: Arc<crate::ivm::data::Comparator>,
    ) -> Self {
        Self { bound, comparator }
    }
    fn should_be_present(&self, row: &Row) -> bool {
        let cmp = (self.comparator)(&self.bound.row, row);
        match cmp {
            CmpOrdering::Less => true,
            CmpOrdering::Equal => !self.bound.exclusive,
            CmpOrdering::Greater => false,
        }
    }
}

impl Transformer for SkipT {
    fn fetch_through<'a>(
        &'a mut self,
        upstream: Box<dyn Iterator<Item = Node> + 'a>,
        _req: FetchRequest,
    ) -> Box<dyn Iterator<Item = Node> + 'a> {
        let comparator = Arc::clone(&self.comparator);
        let bound_row = self.bound.row.clone();
        let exclusive = self.bound.exclusive;
        Box::new(upstream.filter(move |node| {
            let cmp = comparator(&bound_row, &node.row);
            match cmp {
                CmpOrdering::Less => true,
                CmpOrdering::Equal => !exclusive,
                CmpOrdering::Greater => false,
            }
        }))
    }

    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        eprintln!("[TRACE ivm_v2] SkipT::push enter");
        if let Change::Edit(edit) = change {
            let comparator = Arc::clone(&self.comparator);
            let bound_row = self.bound.row.clone();
            let exclusive = self.bound.exclusive;
            let predicate = move |row: &Row| -> bool {
                let cmp = comparator(&bound_row, row);
                match cmp {
                    CmpOrdering::Less => true,
                    CmpOrdering::Equal => !exclusive,
                    CmpOrdering::Greater => false,
                }
            };
            return Box::new(maybe_split_edit(edit, &predicate).into_iter());
        }
        let row_ref = match &change {
            Change::Add(a) => &a.node.row,
            Change::Remove(r) => &r.node.row,
            Change::Child(c) => &c.node.row,
            Change::Edit(_) => unreachable!(),
        };
        if self.should_be_present(row_ref) {
            Box::new(std::iter::once(change))
        } else {
            Box::new(std::iter::empty())
        }
    }
}
