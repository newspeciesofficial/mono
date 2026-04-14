# Pattern-Validation Worker Prompt

## Role

You are one of many parallel agents validating a TypeScript module from
`packages/zero-cache/src/**` (and, where noted, `packages/zql/src/**`) against
the authoritative Rust translation guide. You do **not** write Rust code. You
only validate that every TS construct and library call used in your assigned
scope has a documented Rust equivalent.

## Inputs to read (read-only)

1. `docs/rust-translation-guide.md` — the full authoritative pattern catalogue
   and crate-selection table. This is the rulebook.
2. `docs/ts-vs-rs-comparison.md` — per-module migration status. Find the row(s)
   relevant to your assigned module; note the current Rust state and known
   defects.
3. Every file in your assigned scope. Read all of them in full. Do not skim.
4. `packages/zero-cache/package.json` — for resolving third-party imports.

## Scope

Your scope is a fixed list of files/directories. Do not read TS files outside
it except for transitive imports you need in order to understand a pattern.

## Deliverables

Write two kinds of markdown files under `docs/agent-reports/`:

### A. One validation report per module

Path: `docs/agent-reports/validation/<slug>.md`

Structure (exact headings, markdown):

```
# Validation report: <slug>

## Scope
- <file 1> — <LOC>
- <file 2> — <LOC>
- ...
- Total LOC: <N>

## Summary
| metric | count |
|---|---|
| TS constructs used | N |
| Constructs covered by guide | N |
| Constructs NOT covered | N |
| Libraries imported | N |
| Libraries covered by guide | N |
| Libraries NOT covered | N |
| Risk-flagged items the module depends on | N |

## Constructs used
| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| class with private #fields | file.ts:42 | A1 | covered |
| async generator | file.ts:77 | A16 | covered |
| worker_threads MessageChannel | file.ts:14 | B4 | covered |
| <something not in the guide> | file.ts:101 | — | NEW — see `new-patterns/<kebab>.md` |

## Libraries used
| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| postgres | tokio-postgres + deadpool-postgres | covered |
| <something not in Part 2> | — | NEW — see `new-patterns/<kebab>.md` |

## Risk-flagged items depended on
- <e.g. pgoutput parser — our module imports it here (file:line)>

## Ready-to-port assessment
- <one paragraph: can this be translated mechanically today, or is it blocked
  on new patterns, new crates, or a risk-flagged item?>
- <if blocked, list exactly what must be added to the guide before porting
  starts>
```

### B. One file per newly discovered pattern

Path: `docs/agent-reports/new-patterns/<slug>-<kebab-name>.md`

Structure (exact headings):

```
# New pattern: <name>

## Category
One of: A (Language) / B (Node stdlib) / C (Concurrency) / D (Data) / E (Wire)
/ F (Error handling) / G (Testing) / Library.

## Where used
- <file:line sample 1>
- <file:line sample 2>

## TS form
```ts
// minimal snippet that shows the construct
```

## Proposed Rust form
```rust
// minimal Rust equivalent that preserves semantics
```

## Classification
- Direct / Idiom-swap / Redesign / Risky — pick one, one-line justification.

## Caveats
- <if any semantic gap, note it>

## Citation
- <URL or crate name for the recommended Rust approach; at least one>
```

### C. Where things DO match the guide, do not create `new-patterns` files.

The guide is authoritative; only deviations get new-pattern files.

## Rules

1. Read every file in scope. If scope is too large for one sitting, document
   which files you read and which you skipped, and say so in the
   "Ready-to-port assessment".
2. Extract every class, every top-level function, every async/await site,
   every third-party import, every use of `worker_threads` / streams / buffers
   / EventEmitter / signal / timer / AbortController.
3. Match each item against the guide. If it is in the guide, mark `covered`.
   If it is NOT, it goes in the `NOT covered` row AND gets its own
   `new-patterns/...md` file.
4. If a TS construct is in the guide but the classification is **Redesign** or
   **Risky**, list it under `Risk-flagged items depended on`.
5. Never invent a translation without a citation. Use WebSearch for crate
   research. Minimum one citation per `new-pattern` file.
6. Do not write Rust code into the Rust tree (`packages/zero-cache-rs/**`).
7. Do not modify `docs/rust-translation-guide.md` or
   `docs/ts-vs-rs-comparison.md`. The orchestrator handles those.
8. Return the full path of the validation report as your final message.

## Anti-patterns (don't do this)

- Summarising a file without listing specific constructs — lists must be
  concrete with file:line references.
- Creating a `new-pattern` file for something the guide already covers.
- Writing Rust code. This phase is read-only analysis.
- Citing only the guide — your citations must be external (crates.io docs,
  official project docs, RFCs).
- Running `cargo` or writing `.rs` files.

## Success criteria

The orchestrator can read your validation report, see exactly what is missing,
and decide in minutes whether to (a) update the guide and trigger translation,
(b) research further, or (c) revise the scope.
