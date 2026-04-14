//! Port of `packages/zql/src/planner/`.
//!
//! The planner turns an AST into a cost-annotated graph of `PlannerNode`s
//! (Connection, Join, FanOut, FanIn, Terminus), enumerates flip patterns for
//! flippable `EXISTS` joins, picks the best plan by cost, and writes the chosen
//! `flip` flags back into the AST.
//!
//! Layering within this module (bottom-up):
//!
//! 1. [`planner_constraint`] — `PlannerConstraint` alias + `merge_constraints`.
//! 2. [`planner_node`] — `PlannerNode` enum, `CostEstimate`, helpers.
//! 3. [`planner_source`] — `PlannerSource` (table handle → `connect()`).
//! 4. [`planner_terminus`] — final output node.
//! 5. [`planner_fan_out`] — FanOut / UFO node.
//! 6. [`planner_fan_in`] — FanIn / UFI node.
//! 7. [`planner_join`] — semi / flipped Join node.
//! 8. [`planner_connection`] — source-connection node.
//! 9. [`planner_graph`] — owns the nodes, drives planning.
//! 10. [`planner_builder`] — AST → graph; apply plan back onto AST.
//! 11. [`planner_debug`] — structured debug events (diagnostics only).
//!
//! # Design decisions
//!
//! ## Mutual references
//! TS uses class references freely; Rust models each node as `Arc<T>` and
//! `PlannerNode` as a clonable enum over those `Arc`s. Back-edges (`output`,
//! `outputs`) are stored as strong `Arc` via `Mutex<Option<PlannerNode>>`.
//! This creates reference cycles between a parent and its outputs; this is
//! acceptable because a `PlannerGraph` is a short-lived structure that is
//! constructed and discarded per `planQuery()` call. The graph itself is
//! dropped entirely at that point and the cycles are freed with it
//! (provided there is no external `Arc` retained by the caller).
//!
//! ## Interior mutability
//! TS classes mutate fields like `#type`, `#limit`, `#pinned` via method
//! calls on shared references. We use `Mutex<...>` or `AtomicUsize` inside
//! each node so `&self` method signatures suffice, matching the existing
//! filter.rs / take.rs pattern.
//!
//! ## Plan IDs
//! TS attaches `[planIdSymbol]` directly to AST nodes via a Symbol property.
//! Rust's AST is a plain data struct without symbol slots, so the builder
//! assigns plan IDs in DFS order and `apply_plans_to_ast` walks both the
//! source AST and the graph's joins in the same DFS order to pair them up.
//!
//! ## PlanDebugger
//! TS defines rich JSON-typed events. For the Rust port we store events as
//! `serde_json::Value` keyed by `"type"` — match observable shape, not
//! byte-for-byte string output (see `planner_debug` module docs).

pub mod planner_builder;
pub mod planner_connection;
pub mod planner_constraint;
pub mod planner_debug;
pub mod planner_fan_in;
pub mod planner_fan_out;
pub mod planner_graph;
pub mod planner_join;
pub mod planner_node;
pub mod planner_source;
pub mod planner_terminus;
