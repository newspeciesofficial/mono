//! Skip operator — ivm_v2.
//!
//! Skip sets the start position of the pipeline: no rows before the
//! configured `bound` are emitted. The bound can be inclusive or
//! exclusive.
//!
//! Simplifications vs `ivm/skip.rs`:
//! - No `Arc<Self>`, no `Mutex`, no back-edge adapters.
//! - `push(&mut self, Change) -> Iterator<Change>` — caller drives the
//!   pipeline.
//! - No `'yield'` sentinel in the fetch stream.
//! - Edit splitting reuses `filter::maybe_split_edit`.

use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

use zero_cache_types::value::Row;

use super::change::{Change, Node, SourceSchema};
use super::filter::maybe_split_edit;
use super::operator::{FetchRequest, Input, InputBase, Operator};
use crate::ivm::operator::{Start, StartBasis};

/// TS `type Bound = { row: Row; exclusive: boolean }`.
#[derive(Debug, Clone)]
pub struct Bound {
    pub row: Row,
    pub exclusive: bool,
}

pub struct Skip {
    input: Box<dyn Input>,
    bound: Bound,
    comparator: Arc<dyn Fn(&Row, &Row) -> CmpOrdering + Send + Sync>,
    schema: SourceSchema,
}

impl Skip {
    pub fn new(input: Box<dyn Input>, bound: Bound) -> Self {
        let schema = input.get_schema().clone();
        let comparator = Arc::clone(&schema.compare_rows);
        Self {
            input,
            bound,
            comparator,
            schema,
        }
    }

    /// TS `#shouldBePresent(row)`.
    ///
    /// Row is present iff it's >= bound (or strictly > bound when exclusive).
    fn should_be_present(&self, row: &Row) -> bool {
        let cmp = (self.comparator)(&self.bound.row, row);
        match cmp {
            CmpOrdering::Less => true,
            CmpOrdering::Equal => !self.bound.exclusive,
            CmpOrdering::Greater => false,
        }
    }

    /// TS `#getStart(req)`. Returns `Some(Start)` for the upstream fetch,
    /// `None` if the upstream should be fetched with no bound, or
    /// `Err(())` to signal empty result.
    fn get_start(&self, req: &FetchRequest) -> Result<Option<Start>, ()> {
        let bound_start = Start {
            row: self.bound.row.clone(),
            basis: if self.bound.exclusive {
                StartBasis::After
            } else {
                StartBasis::At
            },
        };

        let Some(req_start) = req.start.as_ref() else {
            if req.reverse.unwrap_or(false) {
                return Ok(None);
            }
            return Ok(Some(bound_start));
        };

        let cmp = (self.comparator)(&self.bound.row, &req_start.row);

        if !req.reverse.unwrap_or(false) {
            if cmp == CmpOrdering::Greater {
                return Ok(Some(bound_start));
            }
            if cmp == CmpOrdering::Equal {
                if self.bound.exclusive || req_start.basis == StartBasis::After {
                    return Ok(Some(Start {
                        row: self.bound.row.clone(),
                        basis: StartBasis::After,
                    }));
                }
                return Ok(Some(bound_start));
            }
            return Ok(Some(req_start.clone()));
        }

        // reverse case
        if cmp == CmpOrdering::Greater {
            return Err(());
        }
        if cmp == CmpOrdering::Equal {
            if !self.bound.exclusive && req_start.basis == StartBasis::At {
                return Ok(Some(bound_start));
            }
            return Err(());
        }
        Ok(Some(req_start.clone()))
    }
}

impl InputBase for Skip {
    fn get_schema(&self) -> &SourceSchema {
        &self.schema
    }
    fn destroy(&mut self) {
        self.input.destroy();
    }
}

impl Input for Skip {
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
        let reverse = req.reverse.unwrap_or(false);
        let start = match self.get_start(&req) {
            Ok(s) => s,
            Err(()) => return Box::new(std::iter::empty()),
        };
        let new_req = FetchRequest {
            constraint: req.constraint,
            start,
            reverse: req.reverse,
        };
        let upstream = self.input.fetch(new_req);
        if !reverse {
            return upstream;
        }
        // Reverse path: stop once we step past the bound.
        let bound_row = self.bound.row.clone();
        let exclusive = self.bound.exclusive;
        let comparator = Arc::clone(&self.comparator);
        Box::new(upstream.take_while(move |node| {
            let cmp = comparator(&bound_row, &node.row);
            match cmp {
                CmpOrdering::Less => true,
                CmpOrdering::Equal => !exclusive,
                CmpOrdering::Greater => false,
            }
        }))
    }
}

impl Operator for Skip {
    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a> {
        eprintln!(
            "[TRACE ivm_v2] Skip::push enter change={}",
            change_name(&change)
        );

        // Edit splits based on presence at bound.
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

        // Add / Remove / Child: forward iff the row is present.
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

fn change_name(c: &Change) -> &'static str {
    match c {
        Change::Add(_) => "Add",
        Change::Remove(_) => "Remove",
        Change::Child(_) => "Child",
        Change::Edit(_) => "Edit",
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Coverage mirrors `ivm::skip::tests`:
    //! - Push: Add/Remove/Child at/before/after bound (inclusive + exclusive).
    //! - Push: Edit split (4 branches via maybe_split_edit).
    //! - Fetch: forward no req.start → starts at bound.
    //! - Fetch: forward with req.start after bound → uses req.start.
    //! - Fetch: reverse without req.start → passes through unbounded,
    //!   take_while stops at bound.
    //! - Fetch: reverse with req.start before bound → empty.
    //! - Destroy / get_schema delegation.
    use super::*;
    use crate::ivm::change::{AddChange, ChangeType, ChildChange, ChildSpec, EditChange, RemoveChange};
    use crate::ivm::data::{make_comparator, Node};
    use crate::ivm::operator::{Start, StartBasis};
    use indexmap::IndexMap;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering as AtomOrdering};
    use std::sync::Arc as StdArc;
    use zero_cache_types::ast::{Direction, Ordering, System};
    use zero_cache_types::primary_key::PrimaryKey;

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
    fn row(id: i64) -> Row {
        let mut r = Row::new();
        r.insert("id".into(), Some(json!(id)));
        r
    }
    fn node_of(r: Row) -> Node {
        Node {
            row: r,
            relationships: IndexMap::new(),
        }
    }

    struct StubInput {
        rows: Vec<Row>,
        schema: SourceSchema,
        destroyed: StdArc<AtomicBool>,
        last_req: StdArc<std::sync::Mutex<Option<FetchRequest>>>,
    }
    impl InputBase for StubInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {
            self.destroyed.store(true, AtomOrdering::SeqCst);
        }
    }
    impl Input for StubInput {
        fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a> {
            *self.last_req.lock().unwrap() = Some(req.clone());
            // Honor start/reverse minimally for fetch assertions.
            let reverse = req.reverse.unwrap_or(false);
            let mut rows = self.rows.clone();
            if reverse {
                rows.reverse();
            }
            Box::new(rows.into_iter().map(node_of))
        }
    }

    struct Harness {
        skip: Skip,
        destroyed: StdArc<AtomicBool>,
        last_req: StdArc<std::sync::Mutex<Option<FetchRequest>>>,
    }
    fn mk(bound: Bound, rows: Vec<Row>) -> Harness {
        let destroyed = StdArc::new(AtomicBool::new(false));
        let last_req = StdArc::new(std::sync::Mutex::new(None));
        let input = StubInput {
            rows,
            schema: schema(),
            destroyed: StdArc::clone(&destroyed),
            last_req: StdArc::clone(&last_req),
        };
        Harness {
            skip: Skip::new(Box::new(input), bound),
            destroyed,
            last_req,
        }
    }
    fn kind(c: &Change) -> ChangeType {
        match c {
            Change::Add(_) => ChangeType::Add,
            Change::Remove(_) => ChangeType::Remove,
            Change::Child(_) => ChangeType::Child,
            Change::Edit(_) => ChangeType::Edit,
        }
    }

    // ── push ──────────────────────────────────────────────────────
    #[test]
    fn add_at_bound_inclusive_forwards() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![],
        );
        let out: Vec<Change> = h
            .skip
            .push(Change::Add(AddChange {
                node: node_of(row(10)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Add);
    }

    #[test]
    fn add_at_bound_exclusive_drops() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: true,
            },
            vec![],
        );
        let out: Vec<Change> = h
            .skip
            .push(Change::Add(AddChange {
                node: node_of(row(10)),
            }))
            .collect();
        assert!(out.is_empty());
    }

    #[test]
    fn add_after_bound_forwards() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: true,
            },
            vec![],
        );
        let out: Vec<Change> = h
            .skip
            .push(Change::Add(AddChange {
                node: node_of(row(11)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn add_before_bound_drops() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![],
        );
        let out: Vec<Change> = h
            .skip
            .push(Change::Add(AddChange {
                node: node_of(row(5)),
            }))
            .collect();
        assert!(out.is_empty());
    }

    #[test]
    fn remove_and_child_gated_same_way() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![],
        );
        let out_remove: Vec<Change> = h
            .skip
            .push(Change::Remove(RemoveChange {
                node: node_of(row(15)),
            }))
            .collect();
        assert_eq!(out_remove.len(), 1);
        assert_eq!(kind(&out_remove[0]), ChangeType::Remove);

        let out_child: Vec<Change> = h
            .skip
            .push(Change::Child(ChildChange {
                node: node_of(row(5)),
                child: ChildSpec {
                    relationship_name: "rel".into(),
                    change: Box::new(Change::Add(AddChange {
                        node: node_of(row(1)),
                    })),
                },
            }))
            .collect();
        assert!(out_child.is_empty());
    }

    // ── edit split ───────────────────────────────────────────────
    #[test]
    fn edit_both_present_forwards_edit() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![],
        );
        let out: Vec<Change> = h
            .skip
            .push(Change::Edit(EditChange {
                old_node: node_of(row(15)),
                node: node_of(row(20)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Edit);
    }

    #[test]
    fn edit_old_present_new_absent_pushes_remove() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![],
        );
        let out: Vec<Change> = h
            .skip
            .push(Change::Edit(EditChange {
                old_node: node_of(row(15)),
                node: node_of(row(5)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Remove);
    }

    #[test]
    fn edit_old_absent_new_present_pushes_add() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![],
        );
        let out: Vec<Change> = h
            .skip
            .push(Change::Edit(EditChange {
                old_node: node_of(row(5)),
                node: node_of(row(15)),
            }))
            .collect();
        assert_eq!(out.len(), 1);
        assert_eq!(kind(&out[0]), ChangeType::Add);
    }

    #[test]
    fn edit_both_absent_nothing() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![],
        );
        let out: Vec<Change> = h
            .skip
            .push(Change::Edit(EditChange {
                old_node: node_of(row(1)),
                node: node_of(row(5)),
            }))
            .collect();
        assert!(out.is_empty());
    }

    // ── fetch ────────────────────────────────────────────────────
    #[test]
    fn fetch_no_req_start_forward_uses_bound() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![row(10), row(11), row(12)],
        );
        let _: Vec<Node> = h.skip.fetch(FetchRequest::default()).collect();
        let req = h.last_req.lock().unwrap().clone().unwrap();
        let s = req.start.unwrap();
        assert_eq!(s.row.get("id"), Some(&Some(json!(10))));
        assert_eq!(s.basis, StartBasis::At);
    }

    #[test]
    fn fetch_req_start_after_bound_wins() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![row(20)],
        );
        let req = FetchRequest {
            start: Some(Start {
                row: row(20),
                basis: StartBasis::At,
            }),
            ..FetchRequest::default()
        };
        let _: Vec<Node> = h.skip.fetch(req).collect();
        let forwarded = h.last_req.lock().unwrap().clone().unwrap();
        let s = forwarded.start.unwrap();
        assert_eq!(s.row.get("id"), Some(&Some(json!(20))));
    }

    #[test]
    fn fetch_reverse_before_bound_is_empty() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![row(15)],
        );
        // reverse + req.start = 5 (before bound) → empty
        let req = FetchRequest {
            start: Some(Start {
                row: row(5),
                basis: StartBasis::At,
            }),
            reverse: Some(true),
            ..FetchRequest::default()
        };
        let out: Vec<Node> = h.skip.fetch(req).collect();
        assert!(out.is_empty());
    }

    // ── lifecycle ───────────────────────────────────────────────
    #[test]
    fn destroy_delegates() {
        let mut h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![],
        );
        h.skip.destroy();
        assert!(h.destroyed.load(AtomOrdering::SeqCst));
    }

    #[test]
    fn get_schema_returns_cached() {
        let h = mk(
            Bound {
                row: row(10),
                exclusive: false,
            },
            vec![],
        );
        assert_eq!(h.skip.get_schema().table_name, "t");
    }
}
