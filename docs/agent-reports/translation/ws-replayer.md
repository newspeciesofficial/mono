# Translation report: ws-replayer

## Scope

Standalone tool — not a translation of an existing TS module. Task was to
build a Rust binary crate that replays a recorded WebSocket session against
a live Rust server and structurally diffs the Rust responses against the
recorded TS responses, normalizing stateful values (cookies, IDs, timestamps,
LSN watermarks, UUIDs) before compare.

## Files created

- `packages/zero-cache-rs/crates/ws-replayer/Cargo.toml` — binary target
  using workspace deps (`tokio`, `tokio-tungstenite`, `serde_json`, `clap`,
  `indexmap`, `uuid`, `url`, `futures`, `anyhow`, `tracing`) plus crate-local
  `regex = "1"` and `chrono = "0.4"`.
- `packages/zero-cache-rs/crates/ws-replayer/src/main.rs` — clap CLI,
  WebSocket driver (split stream, tokio::spawn reader, mpsc to main loop,
  idle-timeout drain), URL merging (meta path + target host) and fresh-ID
  rewriting.
- `packages/zero-cache-rs/crates/ws-replayer/src/fixture.rs` — `.jsonl`
  loader (`FixtureFrame`, `Direction::{Sent,Recv,Close}`), `.meta.json`
  loader, `meta_path_for` helper.
- `packages/zero-cache-rs/crates/ws-replayer/src/normalize.rs` — per-field
  rules (`pokeID`/`baseCookie`/`cookie` → `<cookie>`, `wsid` → `<wsid>`,
  `timestamp` → `<ts>`), UUID regex, LSN regex (16-hex or `A/B`), row-id
  replacement inside `rowsPatch[*].value.id`, `desiredQueriesPatches`
  rekeyed to `<clientID>`.
- `packages/zero-cache-rs/crates/ws-replayer/src/diff.rs` — `ParsedFrame`
  (tag + body), `diff_frames`, patch lists (`rowsPatch`, `gotQueriesPatch`,
  `mutationsPatch`) compared as multisets of canonical shape strings, row
  counts must match.
- `packages/zero-cache-rs/crates/ws-replayer/src/report.rs` — renders
  markdown (summary table, ordered divergences, Rust-side error list,
  fix-category counters).
- `tools/ws-replayer/fixtures/example-session.jsonl` + `.meta.json` —
  hand-crafted 9-line fixture exercising `initConnection` / `connected` /
  `pokeStart` / `pokePart` (with 2 row puts) / `pokeEnd` /
  `changeDesiredQueries` / `pokeStart` / `pokeEnd` / `close`.

## Workspace changes

- Added `"crates/ws-replayer"` to `members` in
  `packages/zero-cache-rs/Cargo.toml` (only non-scope touch, required so
  the crate builds under `cargo build -p ws-replayer`).

## Structural-diff rules implemented

- Tags compared in order; tag mismatches emit `Divergence::TagMismatch`
  with `prev_tag` hint.
- Bodies normalized before comparison — stateful fields replaced with
  sentinels so `pokeID:"aaa"` vs `pokeID:"bbb"` diffs cleanly.
- `rowsPatch` / `gotQueriesPatch` / `mutationsPatch` compared as
  order-insensitive multisets of shape strings built from sorted keys and
  JSON leaf types (primitive strings collapse to `str`; sentinel strings
  `<cookie>`/`<row-id>`/etc are preserved verbatim because they carry
  structural intent). Row-count mismatch always reported.
- `desiredQueriesPatches` outer clientID key rewritten to `<clientID>`.
- Missing fields reported with full dotted path; extra Rust frames beyond
  the TS tail reported separately; early Rust close reported as
  `RustEndedEarly`.

## Tests (22 unit tests, all passing)

- `normalize::tests` — UUID regex (canonical + rejects non-UUID), LSN
  regex (16-hex + PG `A/B` + rejects), cookie / wsid / timestamp field
  replacement, nested UUID, rowsPatch `id` replacement, desiredQueriesPatches
  rekey.
- `diff::tests` — `identical_poke_ids_normalize_to_same` (pokeID
  `"aaa"` vs `"bbb"` → zero divergences, per-prompt requirement),
  `tag_mismatch_reported`, `missing_rows_patch_entry_flagged` (2 TS rows
  vs 1 Rust row flags RowCountMismatch, per-prompt requirement),
  `rust_ended_early_reported`, `missing_field_on_rust_reported`.
- `fixture::tests` — jsonl round-trip parse, `meta_path_for` stem
  stripping.
- `report::tests` — empty report renders without panicking.
- `tests` (main) — `fresh_ids_rewrites_query_params`,
  `fresh_ids_off_is_identity`, `build_target_url_merges_meta_path`.

## Verification

- `cargo build -p ws-replayer`: ok (finished dev profile, no warnings
  after the unused-import fix).
- `cargo test -p ws-replayer`: 22 passed, 0 failed, 0 ignored.
- `cargo run -p ws-replayer -- --help`: clap help prints as expected.

## Known limitations / notes

- No live WebSocket integration test — deferred pending a real Rust server
  fixture (would need `testcontainers` or a mocked `axum` server; not in
  scope per translator rules).
- Cookie forwarding from `meta.cookies` into the WS handshake headers is
  not yet wired — `tokio-tungstenite::connect_async` takes a bare URL
  here. If the recorded session needs auth, the next iteration should
  switch to `connect_async_with_config` + a custom `Request` carrying a
  `Cookie:` header built from `FixtureMeta::cookies`.
- `regex` and `chrono` were added as crate-local deps (not in workspace
  deps). `chrono` is carried through `FixtureMeta.recorded_at` only as a
  string today; the dep is still listed so the meta struct can grow
  typed timestamps without another Cargo churn.

## Paths

- Crate: `/Users/harsharanga/code/mono-v2/packages/zero-cache-rs/crates/ws-replayer/`
- Fixtures: `/Users/harsharanga/code/mono-v2/tools/ws-replayer/fixtures/`
