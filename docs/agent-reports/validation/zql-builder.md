# Validation report: zql-builder

## Scope

- `packages/zql/src/builder/builder.ts` — 796 LOC
- `packages/zql/src/builder/filter.ts` — 204 LOC
- `packages/zql/src/builder/like.ts` — 71 LOC
- `packages/zql/src/builder/debug-delegate.ts` — 103 LOC
- `packages/zql/src/builder/test-builder-delegate.ts` — 87 LOC
- `packages/zql/src/builder/like-test-cases.ts` — 173 LOC (test fixture data only; no logic)
- Files requested but not present: `resolve-scalar-subqueries.ts`, `ast-to-zql.ts` (neither exists under `packages/zql/src/builder/`)
- Test files (excluded per scope rules): `builder.test.ts`, `filter.test.ts`, `like.test.ts`
- Total LOC (non-test): **1,261 LOC** (excluding `like-test-cases.ts` fixtures); **1,434 LOC** including fixtures.

## Scope caveat

The authoritative `docs/rust-translation-guide.md` explicitly states in its
first section: *"This is the translation reference used when porting
`packages/zero-cache/src/**` to Rust. It does not cover zql, zqlite,
zero-protocol, zero-client, replicache, or shared — those are outside scope."*

However, `ts-vs-rs-comparison.md` row 14 (pipeline-driver + zql/ivm) and row 2
(`bindStaticParameters`) identify the zql builder as the **biggest behavioural
gap in the Rust port** (~11K TS LOC, stubbed in Rust with `~5%` coverage). The
orchestrator has assigned this zql file as an explicit exception. All patterns
below are validated against the zero-cache catalogue anyway because the port
will use the same Rust stack (tokio, serde, rusqlite, etc.); cross-package
reuse is the expected outcome.

## Summary

| metric | count |
|---|---|
| TS constructs used | 18 |
| Constructs covered by guide | 16 |
| Constructs NOT covered | 2 |
| Libraries imported | 3 |
| Libraries covered by guide | 3 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 1 |

## Constructs used

| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `interface BuilderDelegate { ... }` with optional method members and readonly fields | `builder.ts:54-99` | A5 | covered |
| `interface DebugDelegate { ... }` | `debug-delegate.ts:17-26` | A5 | covered |
| `class Debug implements DebugDelegate` with `#private` fields | `debug-delegate.ts:28-103` | A1, A23 | covered |
| `class TestBuilderDelegate implements BuilderDelegate` with `#private` fields and readonly | `test-builder-delegate.ts:11-87` | A1, A23 | covered |
| Tagged-union dispatch over `Condition['type']` (`'simple' \| 'and' \| 'or' \| 'correlatedSubquery'`) + exhaustive `switch` + `unreachable(condition)` | `builder.ts:231-253` (`assertNoNotExists`), `builder.ts:484-500` (`applyFilter`), `filter.ts:171-203` (`transformFilters`) | A6 | covered |
| Exhaustive switch over `SimpleOperator` with `operator satisfies never` | `filter.ts:108-150` | A6 | covered |
| Generics with structural callback types (`(c: Condition) => boolean`), default params not present; no `T extends X` constraints | `builder.ts:782-796` | A8 | covered (trivial; no constraint trait needed) |
| Object spread / struct-update `{ ...node, where: ... }` (deep AST rewriting in `bindStaticParameters`) | `builder.ts:149-183` | A13, A20 | covered |
| Optional chaining and nullish coalescing (`sq.subquery.alias ?? ''`, `condition?.conditions`, `sq.hidden ?? false`) | `builder.ts:351, 468, 642`, many | A11, A12, A21 | covered |
| Closures capturing mutable outer variable (`let count = 0` across `uniquify*` closures; `end` / `input` reassignment in `applyOr`, `applyFilterWithFlips`) | `builder.ts:732, 382-481, 535-557` | A18 | covered (single-threaded; pure IVM graph construction) |
| Closure returning closure (curried predicate factories) | `filter.ts:26-94` (`createPredicate`), `like.ts:4-13` (`getLikePredicate`) | A18 | covered |
| `Map<K,V>` / `Set<K>` for dedupe-by-alias and splitEditKeys accumulation | `builder.ts:274-290`, `builder.ts:349-353` | A19 (via `HashMap`/`HashSet`) | covered |
| `Array.prototype.reduce` / `map` / `filter` / `some` / `every` for AST traversal | `builder.ts:213, 152-155, 707-708, 774`, `filter.ts:180-192` | — (stdlib iterator) | covered (Rust: `Iterator::fold/map/filter/any/all`) |
| Dynamic field access `anchor[field]` / `row[left.name]` | `builder.ts:216`, `filter.ts:70, 88` | A19 | covered (`HashMap<String, Value>` lookup) |
| Array destructuring with tuple return type `const [matched, notMatched] = …` | `builder.ts:782-796`, `builder.ts:559-572` | A14 | covered |
| Structural `Record<K,V>` types (`Record<string, Source>`, `Record<string, MemoryStorage>`) | `test-builder-delegate.ts:12-13` | A10 | covered (`HashMap<String, Source>`) |
| Exception throw for protocol-level violations (`throw new Error(...)`) | `builder.ts:238-240, 264, 711-719`, `like.ts:53` | A15, F1 | covered (`thiserror::Error` enum, propagate via `?`) |
| `assert(...)` / `unreachable(...)` from `shared/src/asserts.ts` | `builder.ts:383-404, 625-657`, `filter.ts:53-60, 134-142`, `builder.ts:251` | A6, F1 | covered (Rust: `debug_assert!` / `panic!` or `Result` with typed error — asserts in this codebase are protocol invariants, would become `?`-propagated errors in Rust) |
| `getter` syntax (`get log()`, `get clonedStorage()`) on a class | `test-builder-delegate.ts:76-86` | A1 | covered (plain Rust method returning `&T`) |
| RegExp construction from user pattern + SQL-LIKE escape translation (`new RegExp(...)`) | `like.ts:35-71` | — | **NEW — see `new-patterns/zql-builder-sql-like-regex.md`** |
| `IS NULL` / `IS NOT NULL` short-circuit semantics (NULL propagation in predicates) | `filter.ts:62-93` | — | **NEW — see `new-patterns/zql-builder-null-aware-predicates.md`** |

### Non-constructs worth calling out (for translator awareness)

These are design shapes, not TS language features — no new-pattern file:

- **IVM operator graph wiring.** `buildPipeline` imports concrete operator
  classes from `../ivm/*` (`Filter`, `Join`, `FlippedJoin`, `Take`, `Skip`,
  `Exists`, `FanIn`, `FanOut`, `UnionFanIn`, `UnionFanOut`) and wires them via
  `delegate.addEdge(parent, child)`. Only the **wiring** is in this file's
  scope; the operators themselves live in `packages/zql/src/ivm/*` and are out
  of scope here. The delegate-driven factory pattern (`getSource`,
  `createStorage`, `decorateInput`, `decorateFilterInput`,
  `decorateSourceInput`, `addEdge`) is a standard trait-object pattern in Rust
  (`trait BuilderDelegate { fn get_source(&self, name: &str) -> Option<Arc<dyn Source>>; … }`),
  fully covered by guide A3/A5.
- **Static parameter binding (`bindStaticParameters`).** Already partially
  ported to Rust per `ts-vs-rs-comparison.md` row 2. Pure AST rewrite with
  struct-update syntax (A13), recursive visit functions (A6 + trivial
  recursion), `authData[field]` / `preMutationRow[field]` lookup (A19). No new
  patterns.
- **SQL LIKE/ILIKE semantics.** Translated to JS `RegExp` in TS. Rust would
  use the `regex` crate (`regex::Regex::new(...)`) or a hand-rolled
  character-level matcher; see new-pattern file.
- **NULL-aware comparison (`IS` / `IS NOT` vs `=` / `!=`).** Different
  short-circuit: `IS NULL` matches null, `= NULL` returns false. Captured in a
  new-pattern file because the guide does not document this semantic split.
- **EXISTS subquery rewriting** (`EXISTS_LIMIT = 3`,
  `PERMISSIONS_EXISTS_LIMIT = 1`). Algorithmic rewrite of EXISTS into a join
  with a LIMIT cap; purely local to this file. Direct translation.

## Libraries used

| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `@rocicorp/logger` (`LogContext` type only, passed through to `planQuery`) | `tracing` + `tracing-subscriber` | covered |
| `../../shared/src/asserts.ts` (`assert`, `unreachable`, `assertString`) | built-in `assert!` / `unreachable!()` / explicit `match` | covered (stdlib — not third-party) |
| `../../shared/src/must.ts` (`must(x, msg)` — unwrap-or-throw) | `Option::expect(msg)` / `Result::expect(msg)` | covered (stdlib — not third-party) |
| `../../shared/src/json.ts` (`JSONValue` type) | `serde_json::Value` | covered |

No runtime third-party crates are imported. All dependencies are intra-monorepo
type imports (`zero-protocol/src/ast.ts`, `zero-protocol/src/data.ts`,
`zero-protocol/src/primary-key.ts`, `../ivm/*`, `../planner/*`, `../query/*`,
`./debug-delegate.ts`, `./filter.ts`, `./like.ts`) and local workspace
utilities.

## Risk-flagged items depended on

- **IVM operator graph (out of this file's scope, but this file's sole
  purpose).** The guide does not catalogue any of `Filter`, `Join`,
  `FlippedJoin`, `Take`, `Skip`, `Exists`, `FanIn`, `FanOut`, `UnionFanIn`,
  `UnionFanOut` as ported patterns. `ts-vs-rs-comparison.md` row 14 classifies
  the whole of `zql/src/ivm/*` (~6.4K TS LOC) as **Stub** in Rust with "no
  operator graph at all". Translating `builder.ts` without the operators it
  wires is useless. This is a **Risky / Redesign** dependency, not because any
  single construct in `builder.ts` is novel, but because the target set of
  Rust types it should construct does not yet exist.
- **`planQuery` cost-based planner** (`builder.ts:38, 140`). Imported from
  `../planner/planner-builder.ts`, out of scope here. Classified Risky — it
  rewrites the AST in place and must match behaviour exactly or the Rust
  pipeline will produce different join orders.
- **`completeOrdering`** (`builder.ts:40, 134-137`) — out of scope, appends PK
  fields to `ORDER BY` to make ordering total. Must be ported before
  `buildPipeline` to preserve the `assertOrderingIncludesPK` contract at
  `builder.ts:702-721`.

## Ready-to-port assessment

This file is **mechanically ready to port in isolation**: every TS language
construct it uses is covered by guide sections A1, A5, A6, A8, A10–A21, A23,
and F1. Zero third-party npm dependencies. The only genuinely new patterns
are (1) SQL LIKE-to-regex translation and (2) NULL-aware comparison
semantics; both get a new-pattern file.

However, **translating this file in isolation is not useful**. `buildPipeline`
is a graph constructor over IVM operator types
(`Filter`, `Join`, `FlippedJoin`, `Take`, `Skip`, `Exists`, `FanIn`/`FanOut`,
`UnionFanIn`/`UnionFanOut`) that **do not yet exist in Rust**
(`ts-vs-rs-comparison.md` row 14: "Stub — direct `SELECT * FROM <table>
WHERE …`, no IVM operator graph, no joins, no correlated subqueries"). The
`Source.connect(ordering, where, splitEditKeys, debug)` delegate contract and
the `FilterInput` / `Input` / `Storage` / `SourceInput` trait shapes must also
be defined first.

**Blocked on — must be added to the guide / implemented in Rust before
porting this file:**

1. An IVM operator-graph pattern entry (currently absent from the guide,
   which is scoped to zero-cache). At minimum: the `Input` / `FilterInput` /
   `Output` / `Storage` / `Source` trait shapes, how `push(change, operator)`
   propagates, and how `setOutput` / `addEdge` wire the DAG.
2. Ported Rust types for the nine operator classes listed above.
3. Ported `completeOrdering` + `planQuery`.
4. Ported `simplifyCondition` from `../query/expression.ts` (used by
   `filter.ts:194`).
5. The two new patterns documented here:
   - `new-patterns/zql-builder-sql-like-regex.md`
   - `new-patterns/zql-builder-null-aware-predicates.md`

Once the operator set exists, `builder.ts` + `filter.ts` + `like.ts` + the two
delegate helpers should translate 1:1 into roughly 1,000 Rust LOC in a
`zql-builder` crate (or a module inside a new `zql-rs` crate). Estimate: 2–3
days of mechanical translation after the operators land — the file is pure
AST-walking / graph-wiring with no I/O, no async, and no concurrency.
