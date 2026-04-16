//! New operator trait surface — `&mut self`, plain fields, `Send`-only.
//!
//! Key changes from `ivm::operator`:
//!
//! - No `Sync` bound. Each chain lives on one thread. `Send` still
//!   required so the whole chain can be moved onto a rayon worker later.
//! - `push`/`fetch` take `&mut self` directly. Callers split-borrow
//!   `input` / `state` fields as needed. No `Arc<Self>`.
//! - **`push` returns `impl Iterator<Item = Change> + '_`** — the emitted
//!   changes flow back to the caller, which feeds them to downstream.
//!   No `Output` field held inside operators; the caller drives the
//!   pipeline. Stateful operators implement this by returning a
//!   hand-rolled `Iterator` struct that borrows the fields it needs
//!   (see `take::TakePushIter`).
//! - `fetch` returns `impl Iterator<Item = Node> + '_`.
//! - The TS `'yield'` cooperative-scheduling sentinel is dropped
//!   entirely — single-threaded model doesn't need cooperative pauses.
//!
//! ## Pipeline wiring
//!
//! Instead of each operator holding a `Box<dyn Output>` for its
//! downstream, the caller (PipelineDriver or the builder) iterates the
//! stages:
//!
//! ```ignore
//! for c in source.push(input_change) {
//!     for c2 in filter.push(c) {
//!         for c3 in take.push(c2) {
//!             // forward c3 to view-syncer
//!         }
//!     }
//! }
//! ```
//!
//! This is a structural simplification: operators are pure transforms
//! over `(Change, &mut self)` → `Iterator<Change>`. No back-edges, no
//! adapter structs, no `Arc<Mutex<Box<dyn Output>>>`.

use crate::ivm::change::Change;
use crate::ivm::data::Node;
use crate::ivm::schema::SourceSchema;

/// Counterpart of `ivm::InputBase`. Same API, narrower bounds.
pub trait InputBase: Send {
    fn get_schema(&self) -> &SourceSchema;
    fn destroy(&mut self);
    /// Optional downcast hook. Default `None`. `SqliteSource` overrides
    /// so `Chain::build_with_join` can inject the AST's `orderBy` after
    /// source construction without plumbing sort through every factory.
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        None
    }
}

/// An operator capable of providing rows (a `Source` or upstream `Operator`).
///
/// `fetch` returns a lazy iterator of rows. Consumer drops iterator →
/// production stops (rusqlite `.next()` semantics); no channel needed
/// because we're single-threaded per chain.
pub trait Input: InputBase {
    /// TS `input.fetch(req)`. The returned iterator borrows `self` until
    /// it is dropped. The caller decides pacing.
    fn fetch<'a>(&'a mut self, req: FetchRequest) -> Box<dyn Iterator<Item = Node> + 'a>;

    /// Fetch a single row by primary-key equality. The default
    /// implementation is a linear scan via `fetch` — suitable for
    /// test stubs and in-memory sources. `SqliteSource` overrides
    /// this with an indexed `SELECT … WHERE pk_col=? AND …` so
    /// cold-path `get_row` calls stay O(1) regardless of table size.
    fn get_row(&mut self, pk_row: &zero_cache_types::value::Row) -> Option<Node> {
        self.fetch(FetchRequest::default())
            .find(|n| pk_row.iter().all(|(k, v)| n.row.get(k) == Some(v)))
    }
}

/// Operator with a `push` method. The iterator it returns is the stream
/// of changes the operator wants forwarded to its downstream. The caller
/// iterates and forwards each item.
pub trait Operator: Input {
    /// TS `*push(change)`. Returns a lazy iterator of `Change` values.
    ///
    /// Emits 0, 1, or many changes per input change depending on the
    /// operator (e.g. Take at-limit emits 2: Remove + Add).
    fn push<'a>(&'a mut self, change: Change) -> Box<dyn Iterator<Item = Change> + 'a>;
}

/// Pure transformer — no upstream held. Chain provides upstream to
/// `fetch_through` and drives push via `push`. Enables flat composition
/// in Chain without nested ownership.
///
/// **Refetch signalling.** Stateful transformers (Take) sometimes need
/// to read fresh rows from upstream mid-push (e.g. at-limit eviction
/// needs a replacement bound). They set a pending request via
/// `take_pending_refetch`; the Chain honours it by driving the source +
/// preceding transformers with the request and handing the rows back
/// via `ingest_refetch`. Pure transformers (Filter, Skip) never request.
pub trait Transformer: Send {
    fn fetch_through<'a>(
        &'a mut self,
        upstream: Box<dyn Iterator<Item = crate::ivm::data::Node> + 'a>,
        req: FetchRequest,
    ) -> Box<dyn Iterator<Item = crate::ivm::data::Node> + 'a>;

    fn push<'a>(
        &'a mut self,
        change: Change,
    ) -> Box<dyn Iterator<Item = Change> + 'a>;

    /// Called by Chain after each `push`. Returns `Some(FetchRequest)`
    /// if the transformer needs fresh rows before the next push. Default:
    /// no pending refetch.
    fn take_pending_refetch(&mut self) -> Option<FetchRequest> {
        None
    }

    /// Receives rows fulfilling a previously-signalled refetch request.
    /// Default: drop the rows (no-op for transformers that never request).
    fn ingest_refetch(&mut self, _rows: Vec<crate::ivm::data::Node>) {}

    fn destroy(&mut self) {}
}

/// Two-input transformer — used by Join / FlippedJoin. The Chain feeds
/// both upstreams in and receives parent rows decorated with a lazy
/// child-relationship factory. Push is routed by side: `push_parent`
/// for parent-side changes, `push_child` for child-side changes.
pub trait BinaryTransformer: Send {
    /// Hydrate-time: parent rows → decorated parent rows.
    /// `child_snapshot` is a materialised snapshot of all child rows
    /// (Chain collects once per fetch call).
    fn fetch_through<'a>(
        &'a mut self,
        parent_upstream: Box<dyn Iterator<Item = crate::ivm::data::Node> + 'a>,
        child_snapshot: std::sync::Arc<Vec<crate::ivm::data::Node>>,
        req: FetchRequest,
    ) -> Box<dyn Iterator<Item = crate::ivm::data::Node> + 'a>;

    /// Parent-side change. Forwards the change decorated with the
    /// relationship factory.
    fn push_parent<'a>(
        &'a mut self,
        change: Change,
    ) -> Box<dyn Iterator<Item = Change> + 'a>;

    /// Child-side change. For each matching parent in `parent_snapshot`,
    /// emit a `ChildChange`. `parent_snapshot` is materialised by the
    /// Chain from its parent input prior to calling.
    fn push_child<'a>(
        &'a mut self,
        change: Change,
        parent_snapshot: &[crate::ivm::data::Node],
    ) -> Box<dyn Iterator<Item = Change> + 'a>;

    fn destroy(&mut self) {}
}

/// Request object passed to `Input::fetch`. Mirrors `ivm::FetchRequest`.
#[derive(Debug, Clone, Default)]
pub struct FetchRequest {
    pub constraint: Option<crate::ivm::constraint::Constraint>,
    pub start: Option<crate::ivm::operator::Start>,
    pub reverse: Option<bool>,
}
