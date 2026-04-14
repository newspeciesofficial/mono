//! Port of `packages/zql/src/ivm/operator.ts`.
//!
//! Public exports ported:
//!
//! - [`InputBase`] — common surface (`getSchema`, `destroy`).
//! - [`Input`] — extends `InputBase` with `setOutput` and `fetch`.
//! - [`FetchRequest`] — parameters to `Input::fetch`.
//! - [`Start`] / [`StartBasis`] — `{row, basis}` position marker for fetch.
//! - [`Output`] — the downstream-receiver interface, with its `push` method.
//! - [`Operator`] — the `Input + Output` composition (marker trait).
//! - [`Storage`] — generic key/value storage surface exposed to operators.
//! - [`ScanOptions`] — argument to `Storage::scan`.
//! - [`ThrowOutput`] — the TS `throwOutput` singleton; panics on push.
//! - [`skip_yields`] — helper that strips the `Yield` sentinel from a
//!   fetch stream.
//! - [`OnDisconnect`] — type alias for the TS `onDisconnect` callback.
//!
//! Trait method shapes follow the TS surface:
//!
//! - `Input::fetch(req) -> Stream<Node | 'yield'>` becomes
//!   `fn fetch(&self, req: FetchRequest) -> Stream<NodeOrYield>`.
//! - `Output::push(change, input)` takes the sender as
//!   `&dyn InputBase` so a downstream with multiple inputs can
//!   disambiguate based on pointer identity.

use serde_json::Value as JsonValue;

use crate::ivm::change::Change;
use crate::ivm::constraint::Constraint;
use crate::ivm::data::NodeOrYield;
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::Stream;

// ─── FetchRequest / Start ─────────────────────────────────────────────

/// TS `Start.basis: 'at' | 'after'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartBasis {
    At,
    After,
}

/// TS `Start = { row: Row; basis: 'at' | 'after' }`.
#[derive(Debug, Clone)]
pub struct Start {
    pub row: zero_cache_types::value::Row,
    pub basis: StartBasis,
}

/// TS `FetchRequest`. All fields optional — defaults mirror TS
/// `undefined`.
#[derive(Debug, Clone, Default)]
pub struct FetchRequest {
    /// TS `constraint?`. If present, limits the fetch.
    pub constraint: Option<Constraint>,
    /// TS `start?`. If present, `start.row` must have previously been
    /// output by `fetch` or `push` on this same input.
    pub start: Option<Start>,
    /// TS `reverse?`. Fetch in reverse of `SourceSchema.sort`.
    pub reverse: Option<bool>,
}

// ─── Traits: InputBase / Input / Output / Operator ────────────────────

/// TS `InputBase`. The lowest-common-denominator input surface.
///
/// `Send + Sync` bounds match the threading model: the operator graph may
/// be touched from the pool worker thread.
pub trait InputBase: Send + Sync {
    /// TS `getSchema(): SourceSchema`.
    ///
    /// Returns a reference; callers that need ownership `.clone()` it.
    /// The returned schema's `compareRows` is `Arc`-shared, so cloning is
    /// cheap.
    fn get_schema(&self) -> &SourceSchema;

    /// TS `destroy(): void`. Releases upstream connections.
    fn destroy(&mut self);
}

/// TS `Input extends InputBase`.
pub trait Input: InputBase {
    /// TS `setOutput(output: Output): void`.
    fn set_output(&mut self, output: Box<dyn Output>);

    /// TS `fetch(req: FetchRequest): Stream<Node | 'yield'>`.
    ///
    /// Returns nodes sorted per `SourceSchema.compareRows`. `'yield'`
    /// sentinels indicate responsiveness yields and must be propagated
    /// to the caller immediately (see module-level doc on yield contract).
    fn fetch<'a>(&'a self, req: FetchRequest) -> Stream<'a, NodeOrYield>;
}

/// TS `Output`. A sink for pushed changes.
///
/// The `pusher` argument identifies the `InputBase` that sent the change,
/// which is how a downstream with multiple inputs disambiguates. In Rust
/// we pass `&dyn InputBase` so consumers can compare pointer identity or
/// downcast via trait-object equality where needed.
pub trait Output: Send + Sync {
    /// TS `push(change: Change, pusher: InputBase): Stream<'yield'>`.
    fn push<'a>(
        &'a mut self,
        change: Change,
        pusher: &dyn InputBase,
    ) -> Stream<'a, crate::ivm::source::Yield>;
}

/// TS `Operator extends Input, Output`. A node in the pipeline that is
/// both an `Input` (upstream side) and an `Output` (downstream side).
/// Marker trait — no additional methods.
pub trait Operator: Input + Output {}

// ─── throwOutput ──────────────────────────────────────────────────────

/// TS `throwOutput` — an `Output` that panics on push. Used as the
/// initial value for an operator's output before it's set.
///
/// Exposed as a unit struct so callers can `Box::new(ThrowOutput)` into
/// `Box<dyn Output>`.
pub struct ThrowOutput;

impl Output for ThrowOutput {
    fn push<'a>(
        &'a mut self,
        _change: Change,
        _pusher: &dyn InputBase,
    ) -> Stream<'a, crate::ivm::source::Yield> {
        // TS throws `new Error('Output not set')`. This matches the TS
        // `throw` exactly: the intended contract is that pushing through
        // a `throwOutput` is a programmer error.
        panic!("Output not set");
    }
}

// ─── skip_yields helper ───────────────────────────────────────────────

/// TS `skipYields(stream)` — yields only concrete `Node`s, dropping the
/// `'yield'` sentinel.
pub fn skip_yields<'a>(stream: Stream<'a, NodeOrYield>) -> Stream<'a, crate::ivm::data::Node> {
    Box::new(stream.filter_map(|n| match n {
        NodeOrYield::Node(node) => Some(node),
        NodeOrYield::Yield => None,
    }))
}

// ─── Storage ──────────────────────────────────────────────────────────

/// TS `Storage.scan({ prefix? })`.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// TS `options.prefix` — scans only entries whose keys start with this.
    pub prefix: Option<String>,
}

/// TS `Storage`. Per-operator key-value state.
///
/// Values are `JsonValue` to match TS `JSONValue`. The `set` method takes
/// the value by value (not reference) because TS assigns ownership; Rust
/// callers commonly build the value on the spot.
pub trait Storage: Send + Sync {
    /// TS `set(key, value)`.
    fn set(&mut self, key: &str, value: JsonValue);

    /// TS `get(key, def?)`. Returns `None` only when the key is absent
    /// *and* `def` is `None`. When `def` is `Some`, TS returns `def`.
    fn get(&self, key: &str, def: Option<&JsonValue>) -> Option<JsonValue>;

    /// TS `scan(options?)`. Iterator over `(key, value)` pairs. If
    /// `options` is `None`, scans all entries.
    fn scan<'a>(&'a self, options: Option<ScanOptions>) -> Stream<'a, (String, JsonValue)>;

    /// TS `del(key)`.
    fn del(&mut self, key: &str);
}

// ─── OnDisconnect callback type ───────────────────────────────────────

/// TS `onDisconnect` callback. Captured lazily and boxed so it can live
/// inside an operator.
///
/// The TS callback is nullary (`() => void`); downstream operators wire
/// a closure that tears down local state when the upstream disconnects.
pub type OnDisconnect = Box<dyn Fn() + Send + Sync>;

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Branch coverage for the data types and helpers defined here:
    //!   - `FetchRequest::default()` — every field starts as `None`.
    //!   - `FetchRequest` construction with each optional field set.
    //!   - `Start` construction with each `StartBasis` variant.
    //!   - `ScanOptions::default()` + with prefix.
    //!   - `skip_yields` — drops `Yield`, forwards `Node`s in order.
    //!   - `skip_yields` over an empty stream.
    //!   - `skip_yields` over a stream that is entirely `Yield` sentinels.
    //!   - `ThrowOutput::push` — TS `throw new Error('Output not set')`.
    //!   - `OnDisconnect` type alias is callable.

    use super::*;
    use crate::ivm::change::{AddChange, Change};
    use crate::ivm::constraint::Constraint;
    use crate::ivm::data::{Node, NodeOrYield};
    use indexmap::IndexMap;
    use serde_json::json;
    use zero_cache_types::value::Row;

    // Dummy InputBase for test push calls.
    struct DummyInput {
        schema: SourceSchema,
    }
    impl InputBase for DummyInput {
        fn get_schema(&self) -> &SourceSchema {
            &self.schema
        }
        fn destroy(&mut self) {}
    }

    fn make_dummy_schema() -> SourceSchema {
        use crate::ivm::data::make_comparator;
        use std::sync::Arc;
        use zero_cache_types::ast::{Direction, Ordering};
        use zero_cache_types::primary_key::PrimaryKey;
        let sort: Ordering = vec![("id".into(), Direction::Asc)];
        SourceSchema {
            table_name: "t".into(),
            columns: IndexMap::new(),
            primary_key: PrimaryKey::new(vec!["id".into()]),
            relationships: IndexMap::new(),
            is_hidden: false,
            system: zero_cache_types::ast::System::Test,
            compare_rows: Arc::new(make_comparator(sort.clone(), false)),
            sort,
        }
    }

    // Branch: FetchRequest default — all three options None.
    #[test]
    fn fetch_request_default_all_none() {
        let req = FetchRequest::default();
        assert!(req.constraint.is_none());
        assert!(req.start.is_none());
        assert!(req.reverse.is_none());
    }

    // Branch: FetchRequest with constraint set.
    #[test]
    fn fetch_request_with_constraint() {
        let mut c: Constraint = IndexMap::new();
        c.insert("id".into(), Some(json!(1)));
        let req = FetchRequest {
            constraint: Some(c),
            ..FetchRequest::default()
        };
        assert!(req.constraint.is_some());
        assert_eq!(req.constraint.as_ref().unwrap().len(), 1);
    }

    // Branch: FetchRequest with start.row/basis = At.
    #[test]
    fn fetch_request_with_start_at() {
        let mut row = Row::new();
        row.insert("id".into(), Some(json!(1)));
        let req = FetchRequest {
            start: Some(Start {
                row,
                basis: StartBasis::At,
            }),
            ..FetchRequest::default()
        };
        let s = req.start.as_ref().unwrap();
        assert_eq!(s.basis, StartBasis::At);
        assert_eq!(s.row.get("id"), Some(&Some(json!(1))));
    }

    // Branch: FetchRequest with start basis = After.
    #[test]
    fn fetch_request_with_start_after() {
        let req = FetchRequest {
            start: Some(Start {
                row: Row::new(),
                basis: StartBasis::After,
            }),
            ..FetchRequest::default()
        };
        assert_eq!(req.start.unwrap().basis, StartBasis::After);
    }

    // Branch: reverse flag both true and false.
    #[test]
    fn fetch_request_reverse_true_and_false() {
        let req_t = FetchRequest {
            reverse: Some(true),
            ..FetchRequest::default()
        };
        assert_eq!(req_t.reverse, Some(true));

        let req_f = FetchRequest {
            reverse: Some(false),
            ..FetchRequest::default()
        };
        assert_eq!(req_f.reverse, Some(false));
    }

    // Branch: ScanOptions::default — prefix None.
    #[test]
    fn scan_options_default() {
        let o = ScanOptions::default();
        assert!(o.prefix.is_none());
    }

    // Branch: ScanOptions with prefix.
    #[test]
    fn scan_options_with_prefix() {
        let o = ScanOptions {
            prefix: Some("foo/".into()),
        };
        assert_eq!(o.prefix.as_deref(), Some("foo/"));
    }

    // Branch: skip_yields drops Yield sentinels, preserves Node order.
    #[test]
    fn skip_yields_drops_yield_keeps_nodes() {
        let items: Vec<NodeOrYield> = vec![
            NodeOrYield::Node(Node {
                row: {
                    let mut r = Row::new();
                    r.insert("id".into(), Some(json!(1)));
                    r
                },
                relationships: IndexMap::new(),
            }),
            NodeOrYield::Yield,
            NodeOrYield::Node(Node {
                row: {
                    let mut r = Row::new();
                    r.insert("id".into(), Some(json!(2)));
                    r
                },
                relationships: IndexMap::new(),
            }),
            NodeOrYield::Yield,
        ];
        let stream: Stream<'_, NodeOrYield> = Box::new(items.into_iter());
        let out: Vec<Node> = skip_yields(stream).collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].row.get("id"), Some(&Some(json!(1))));
        assert_eq!(out[1].row.get("id"), Some(&Some(json!(2))));
    }

    // Branch: skip_yields over empty stream returns empty.
    #[test]
    fn skip_yields_empty_stream() {
        let stream: Stream<'_, NodeOrYield> = Box::new(std::iter::empty());
        let out: Vec<Node> = skip_yields(stream).collect();
        assert!(out.is_empty());
    }

    // Branch: skip_yields over stream of only Yields returns empty.
    #[test]
    fn skip_yields_all_yields_returns_empty() {
        let items: Vec<NodeOrYield> = vec![NodeOrYield::Yield, NodeOrYield::Yield];
        let stream: Stream<'_, NodeOrYield> = Box::new(items.into_iter());
        let out: Vec<Node> = skip_yields(stream).collect();
        assert!(out.is_empty());
    }

    // Branch: ThrowOutput::push panics with TS message.
    #[test]
    #[should_panic(expected = "Output not set")]
    fn throw_output_push_panics() {
        let mut sink = ThrowOutput;
        let mut pusher = DummyInput {
            schema: make_dummy_schema(),
        };
        let mut n_row = Row::new();
        n_row.insert("id".into(), Some(json!(1)));
        let change = Change::Add(AddChange {
            node: Node {
                row: n_row,
                relationships: IndexMap::new(),
            },
        });
        // Consume the returned stream; for ThrowOutput the panic happens
        // inside `push` before returning.
        let _ = sink.push(change, &mut pusher as &mut dyn InputBase);
    }

    // Branch: OnDisconnect callback is callable and observable.
    #[test]
    fn on_disconnect_callable() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::clone(&counter);
        let cb: OnDisconnect = Box::new(move || {
            c2.fetch_add(1, Ordering::SeqCst);
        });
        cb();
        cb();
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
